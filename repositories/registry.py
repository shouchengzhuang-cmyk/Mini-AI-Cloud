import re
import uuid
from dataclasses import dataclass

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.image_policy import (
    ImagePolicyAction,
    ImagePolicyConfig,
    ImagePolicyDecision,
    ImageRule,
    evaluate_image_policy,
)
from core.rbac import ProjectStatus
from models.identity import Project
from models.registry import ImagePolicy, ImagePolicyRule, RegisteredModel
from repositories.clock import database_utcnow

_MODEL_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


class RegistryNotFoundError(LookupError):
    pass


@dataclass(frozen=True, slots=True)
class StoredImagePolicy:
    policy: ImagePolicy
    rules: list[ImagePolicyRule]


class RegisteredModelRepository:
    @staticmethod
    async def create(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        name: str,
        provider: str,
        source: str,
        revision: str | None,
        size_bytes: int | None,
        gpu_memory_mb: int | None,
        architecture: str | None,
        metadata: dict[str, object],
        created_by_user_id: uuid.UUID | None,
    ) -> RegisteredModel:
        model = RegisteredModel(
            project_id=project_id,
            name=_normalize_model_name(name),
            provider=_normalize_text(provider, "provider", 64),
            source=_normalize_text(source, "source", 1_024),
            revision=_normalize_optional_text(revision, "revision", 255),
            size_bytes=size_bytes,
            gpu_memory_mb=gpu_memory_mb,
            architecture=_normalize_optional_text(architecture, "architecture", 255),
            metadata_json=dict(metadata),
            created_by_user_id=created_by_user_id,
            created_at=await database_utcnow(session),
        )
        session.add(model)
        await session.flush()
        return model

    @staticmethod
    async def get(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        model_id: uuid.UUID,
    ) -> RegisteredModel | None:
        return await session.scalar(
            select(RegisteredModel).where(
                RegisteredModel.id == model_id,
                RegisteredModel.project_id == project_id,
            )
        )

    @staticmethod
    async def list(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        limit: int,
        offset: int,
    ) -> list[RegisteredModel]:
        return list(
            await session.scalars(
                select(RegisteredModel)
                .where(RegisteredModel.project_id == project_id)
                .order_by(RegisteredModel.created_at.desc(), RegisteredModel.id.desc())
                .limit(limit)
                .offset(offset)
            )
        )

    @staticmethod
    async def count(session: AsyncSession, *, project_id: uuid.UUID) -> int:
        return int(
            await session.scalar(
                select(func.count(RegisteredModel.id)).where(
                    RegisteredModel.project_id == project_id
                )
            )
            or 0
        )

    @staticmethod
    async def delete(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        model_id: uuid.UUID,
    ) -> bool:
        model = await RegisteredModelRepository.get(
            session,
            project_id=project_id,
            model_id=model_id,
        )
        if model is None:
            return False
        await session.delete(model)
        return True


class ImagePolicyRepository:
    @staticmethod
    async def get(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        for_update: bool = False,
    ) -> StoredImagePolicy | None:
        query = select(ImagePolicy).where(ImagePolicy.project_id == project_id)
        if for_update:
            query = query.with_for_update()
        policy = await session.scalar(query)
        if policy is None:
            return None
        rules = list(
            await session.scalars(
                select(ImagePolicyRule)
                .where(ImagePolicyRule.project_id == project_id)
                .order_by(ImagePolicyRule.priority, ImagePolicyRule.id)
            )
        )
        return StoredImagePolicy(policy=policy, rules=rules)

    @staticmethod
    async def replace(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        default_action: ImagePolicyAction,
        require_digest: bool,
        rules: list[ImageRule],
    ) -> StoredImagePolicy:
        project = await session.scalar(
            select(Project).where(Project.id == project_id).with_for_update()
        )
        if project is None or project.status != ProjectStatus.ACTIVE:
            raise RegistryNotFoundError("active project does not exist")
        stored = await ImagePolicyRepository.get(
            session,
            project_id=project_id,
            for_update=True,
        )
        now = await database_utcnow(session)
        if stored is None:
            policy = ImagePolicy(
                project_id=project_id,
                default_action=default_action.value,
                require_digest=require_digest,
                updated_at=now,
            )
            session.add(policy)
        else:
            policy = stored.policy
            policy.default_action = default_action.value
            policy.require_digest = require_digest
            policy.updated_at = now
            await session.execute(
                delete(ImagePolicyRule).where(ImagePolicyRule.project_id == project_id)
            )
        stored_rules = [
            ImagePolicyRule(
                id=rule.rule_id or uuid.uuid4(),
                project_id=project_id,
                action=rule.action.value,
                registry_host=rule.registry,
                repository_glob=rule.repository_glob,
                tag_glob=rule.tag_glob,
                digest=rule.digest,
                priority=rule.priority,
            )
            for rule in rules
        ]
        session.add_all(stored_rules)
        await session.flush()
        return StoredImagePolicy(policy=policy, rules=stored_rules)

    @staticmethod
    async def evaluate(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        image: str,
    ) -> ImagePolicyDecision:
        stored = await ImagePolicyRepository.get(session, project_id=project_id)
        if stored is None:
            return evaluate_image_policy(image, ImagePolicyConfig(), [])
        return evaluate_image_policy(
            image,
            ImagePolicyConfig(
                default_action=ImagePolicyAction(stored.policy.default_action),
                require_digest=stored.policy.require_digest,
            ),
            [_rule_from_model(rule) for rule in stored.rules],
        )


def _rule_from_model(rule: ImagePolicyRule) -> ImageRule:
    return ImageRule(
        rule_id=rule.id,
        action=ImagePolicyAction(rule.action),
        registry=rule.registry_host,
        repository_glob=rule.repository_glob,
        tag_glob=rule.tag_glob,
        digest=rule.digest,
        priority=rule.priority,
    )


def _normalize_model_name(value: str) -> str:
    name = value.strip()
    if not _MODEL_NAME.fullmatch(name):
        raise ValueError("model name contains unsupported characters")
    return name


def _normalize_text(value: str, field: str, maximum: int) -> str:
    normalized = value.strip()
    if (
        not normalized
        or len(normalized) > maximum
        or any(character.isspace() or ord(character) < 32 for character in normalized)
    ):
        raise ValueError(f"{field} must be non-empty and contain no whitespace or controls")
    return normalized


def _normalize_optional_text(value: str | None, field: str, maximum: int) -> str | None:
    if value is None:
        return None
    return _normalize_text(value, field, maximum)
