import re
import uuid
from collections.abc import Sequence

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.accelerators import vendor_kind_is_compatible
from core.enums import AcceleratorKind, AcceleratorVendor, ModelAvailabilityStatus
from core.rbac import ProjectStatus
from models.identity import Project
from models.model_variant import LogicalModel, LogicalModelStatusEvent, ModelVariant
from repositories.clock import database_utcnow

_RESOURCE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_PROFILE_ID = re.compile(r"^[a-z][a-z0-9-]{2,63}$")
_SEMANTIC_VERSION = re.compile(r"^[1-9][0-9]*\.[0-9]+\.[0-9]+$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


class LogicalModelNotFoundError(LookupError):
    pass


class ModelVariantNotFoundError(LookupError):
    pass


class ModelVariantInvariantError(ValueError):
    pass


class RuntimeProfileReferencedError(ValueError):
    def __init__(self, profile_id: str, profile_version: str, count: int) -> None:
        super().__init__(
            f"runtime profile {profile_id}@{profile_version} is referenced by {count} variants"
        )
        self.profile_id = profile_id
        self.profile_version = profile_version
        self.count = count


class LogicalModelRepository:
    @staticmethod
    async def create(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        name: str,
        public_name: str,
        description: str | None,
        metadata: dict[str, object],
        created_by_user_id: uuid.UUID | None,
    ) -> LogicalModel:
        project = await session.scalar(
            select(Project).where(Project.id == project_id).with_for_update()
        )
        if project is None or project.status is not ProjectStatus.ACTIVE:
            raise LogicalModelNotFoundError("active project does not exist")
        now = await database_utcnow(session)
        model = LogicalModel(
            project_id=project_id,
            name=_normalize_resource_name(name, "logical model name"),
            public_name=_normalize_display_text(public_name, "public_name", 255),
            description=_normalize_optional_display_text(description, "description", 2_000),
            status=ModelAvailabilityStatus.DISABLED,
            metadata_json=dict(metadata),
            created_by_user_id=created_by_user_id,
            created_at=now,
            updated_at=now,
            version=1,
        )
        session.add(model)
        await session.flush()
        session.add(
            LogicalModelStatusEvent(
                logical_model_id=model.id,
                from_status=None,
                to_status=ModelAvailabilityStatus.DISABLED,
                reason="logical model created",
                model_version=1,
                created_by_user_id=created_by_user_id,
                created_at=now,
            )
        )
        await session.flush()
        return model

    @staticmethod
    async def get(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        logical_model_id: uuid.UUID,
        for_update: bool = False,
    ) -> LogicalModel | None:
        query = select(LogicalModel).where(
            LogicalModel.id == logical_model_id,
            LogicalModel.project_id == project_id,
        )
        if for_update:
            query = query.with_for_update()
        return await session.scalar(query)

    @staticmethod
    async def list(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        limit: int,
        offset: int,
    ) -> list[LogicalModel]:
        return list(
            await session.scalars(
                select(LogicalModel)
                .where(LogicalModel.project_id == project_id)
                .order_by(LogicalModel.created_at.desc(), LogicalModel.id.desc())
                .limit(limit)
                .offset(offset)
            )
        )

    @staticmethod
    async def count(session: AsyncSession, *, project_id: uuid.UUID) -> int:
        return int(
            await session.scalar(
                select(func.count(LogicalModel.id)).where(LogicalModel.project_id == project_id)
            )
            or 0
        )

    @staticmethod
    async def set_status(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        logical_model_id: uuid.UUID,
        status: ModelAvailabilityStatus,
        reason: str,
        created_by_user_id: uuid.UUID | None,
    ) -> LogicalModel:
        model = await LogicalModelRepository.get(
            session,
            project_id=project_id,
            logical_model_id=logical_model_id,
            for_update=True,
        )
        if model is None:
            raise LogicalModelNotFoundError("logical model does not exist")
        if status is ModelAvailabilityStatus.READY:
            ready_count = await ModelVariantRepository.count(
                session,
                logical_model_id=model.id,
                status=ModelAvailabilityStatus.READY,
            )
            if ready_count == 0:
                raise ModelVariantInvariantError(
                    "a ready logical model requires at least one ready variant"
                )
        if model.status is status:
            return model

        previous = model.status
        now = await database_utcnow(session)
        model.status = status
        model.updated_at = now
        model.version += 1
        session.add(
            LogicalModelStatusEvent(
                logical_model_id=model.id,
                from_status=previous,
                to_status=status,
                reason=_normalize_display_text(reason, "reason", 2_000),
                model_version=model.version,
                created_by_user_id=created_by_user_id,
                created_at=now,
            )
        )
        await session.flush()
        return model

    @staticmethod
    async def status_history(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        logical_model_id: uuid.UUID,
        limit: int,
        offset: int,
    ) -> Sequence[LogicalModelStatusEvent]:
        if (
            await LogicalModelRepository.get(
                session,
                project_id=project_id,
                logical_model_id=logical_model_id,
            )
            is None
        ):
            raise LogicalModelNotFoundError("logical model does not exist")
        return list(
            await session.scalars(
                select(LogicalModelStatusEvent)
                .where(LogicalModelStatusEvent.logical_model_id == logical_model_id)
                .order_by(
                    LogicalModelStatusEvent.model_version.desc(),
                    LogicalModelStatusEvent.id.desc(),
                )
                .limit(limit)
                .offset(offset)
            )
        )

    @staticmethod
    async def status_history_count(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        logical_model_id: uuid.UUID,
    ) -> int:
        if (
            await LogicalModelRepository.get(
                session,
                project_id=project_id,
                logical_model_id=logical_model_id,
            )
            is None
        ):
            raise LogicalModelNotFoundError("logical model does not exist")
        return int(
            await session.scalar(
                select(func.count(LogicalModelStatusEvent.id)).where(
                    LogicalModelStatusEvent.logical_model_id == logical_model_id
                )
            )
            or 0
        )

    @staticmethod
    async def delete(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        logical_model_id: uuid.UUID,
    ) -> bool:
        model = await LogicalModelRepository.get(
            session,
            project_id=project_id,
            logical_model_id=logical_model_id,
            for_update=True,
        )
        if model is None:
            return False
        await session.delete(model)
        return True


class ModelVariantRepository:
    @staticmethod
    async def create(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        logical_model_id: uuid.UUID,
        name: str,
        vendor: AcceleratorVendor,
        kind: AcceleratorKind,
        runtime_profile_id: str,
        runtime_profile_version: str,
        runtime_profile_digest: str,
        artifact_source: str,
        artifact_revision: str,
        artifact_digest: str,
        architecture: str,
        dtype: str,
        quantization: str | None,
        status: ModelAvailabilityStatus,
        status_reason: str | None,
        metadata: dict[str, object],
        created_by_user_id: uuid.UUID | None,
    ) -> ModelVariant:
        model = await LogicalModelRepository.get(
            session,
            project_id=project_id,
            logical_model_id=logical_model_id,
            for_update=True,
        )
        if model is None:
            raise LogicalModelNotFoundError("logical model does not exist")
        if not vendor_kind_is_compatible(vendor, kind):
            raise ModelVariantInvariantError(
                f"{vendor.value} is not compatible with kind={kind.value}"
            )
        now = await database_utcnow(session)
        variant = ModelVariant(
            logical_model_id=model.id,
            name=_normalize_resource_name(name, "model variant name"),
            vendor=vendor,
            kind=kind,
            runtime_profile_id=_normalize_profile_id(runtime_profile_id),
            runtime_profile_version=_normalize_profile_version(runtime_profile_version),
            runtime_profile_digest=_normalize_digest(
                runtime_profile_digest, "runtime_profile_digest"
            ),
            artifact_source=_normalize_reference(artifact_source, "artifact_source", 1_024),
            artifact_revision=_normalize_reference(artifact_revision, "artifact_revision", 255),
            artifact_digest=_normalize_digest(artifact_digest, "artifact_digest"),
            architecture=_normalize_reference(architecture, "architecture", 255),
            dtype=_normalize_reference(dtype, "dtype", 64),
            quantization=_normalize_optional_reference(quantization, "quantization", 128),
            status=status,
            status_reason=_normalize_optional_display_text(status_reason, "status_reason", 2_000),
            metadata_json=dict(metadata),
            created_by_user_id=created_by_user_id,
            created_at=now,
            updated_at=now,
            version=1,
        )
        session.add(variant)
        await session.flush()
        return variant

    @staticmethod
    async def get(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        logical_model_id: uuid.UUID,
        variant_id: uuid.UUID,
        for_update: bool = False,
    ) -> ModelVariant | None:
        query = (
            select(ModelVariant)
            .join(LogicalModel, LogicalModel.id == ModelVariant.logical_model_id)
            .where(
                ModelVariant.id == variant_id,
                ModelVariant.logical_model_id == logical_model_id,
                LogicalModel.project_id == project_id,
            )
        )
        if for_update:
            query = query.with_for_update()
        return await session.scalar(query)

    @staticmethod
    async def list(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        logical_model_id: uuid.UUID,
        limit: int,
        offset: int,
    ) -> list[ModelVariant]:
        if (
            await LogicalModelRepository.get(
                session,
                project_id=project_id,
                logical_model_id=logical_model_id,
            )
            is None
        ):
            raise LogicalModelNotFoundError("logical model does not exist")
        return list(
            await session.scalars(
                select(ModelVariant)
                .where(ModelVariant.logical_model_id == logical_model_id)
                .order_by(ModelVariant.created_at.desc(), ModelVariant.id.desc())
                .limit(limit)
                .offset(offset)
            )
        )

    @staticmethod
    async def count(
        session: AsyncSession,
        *,
        logical_model_id: uuid.UUID,
        status: ModelAvailabilityStatus | None = None,
    ) -> int:
        query = select(func.count(ModelVariant.id)).where(
            ModelVariant.logical_model_id == logical_model_id
        )
        if status is not None:
            query = query.where(ModelVariant.status == status)
        return int(await session.scalar(query) or 0)

    @staticmethod
    async def set_status(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        logical_model_id: uuid.UUID,
        variant_id: uuid.UUID,
        status: ModelAvailabilityStatus,
        reason: str,
    ) -> ModelVariant:
        model = await LogicalModelRepository.get(
            session,
            project_id=project_id,
            logical_model_id=logical_model_id,
            for_update=True,
        )
        if model is None:
            raise LogicalModelNotFoundError("logical model does not exist")
        variant = await ModelVariantRepository.get(
            session,
            project_id=project_id,
            logical_model_id=logical_model_id,
            variant_id=variant_id,
            for_update=True,
        )
        if variant is None:
            raise ModelVariantNotFoundError("model variant does not exist")
        if variant.status is status:
            return variant
        if (
            model.status is ModelAvailabilityStatus.READY
            and variant.status is ModelAvailabilityStatus.READY
            and status is not ModelAvailabilityStatus.READY
        ):
            other_ready = int(
                await session.scalar(
                    select(func.count(ModelVariant.id)).where(
                        ModelVariant.logical_model_id == logical_model_id,
                        ModelVariant.id != variant_id,
                        ModelVariant.status == ModelAvailabilityStatus.READY,
                    )
                )
                or 0
            )
            if other_ready == 0:
                raise ModelVariantInvariantError(
                    "a ready logical model must retain at least one ready variant"
                )
        variant.status = status
        variant.status_reason = _normalize_display_text(reason, "reason", 2_000)
        variant.updated_at = await database_utcnow(session)
        variant.version += 1
        await session.flush()
        return variant

    @staticmethod
    async def delete(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        logical_model_id: uuid.UUID,
        variant_id: uuid.UUID,
    ) -> bool:
        model = await LogicalModelRepository.get(
            session,
            project_id=project_id,
            logical_model_id=logical_model_id,
            for_update=True,
        )
        if model is None:
            raise LogicalModelNotFoundError("logical model does not exist")
        variant = await ModelVariantRepository.get(
            session,
            project_id=project_id,
            logical_model_id=logical_model_id,
            variant_id=variant_id,
            for_update=True,
        )
        if variant is None:
            return False
        if (
            model.status is ModelAvailabilityStatus.READY
            and variant.status is ModelAvailabilityStatus.READY
        ):
            ready_count = await ModelVariantRepository.count(
                session,
                logical_model_id=logical_model_id,
                status=ModelAvailabilityStatus.READY,
            )
            if ready_count <= 1:
                raise ModelVariantInvariantError(
                    "a ready logical model must retain at least one ready variant"
                )
        await session.delete(variant)
        return True

    @staticmethod
    async def count_profile_references(
        session: AsyncSession,
        *,
        profile_id: str,
        profile_version: str,
    ) -> int:
        return int(
            await session.scalar(
                select(func.count(ModelVariant.id)).where(
                    ModelVariant.runtime_profile_id == profile_id,
                    ModelVariant.runtime_profile_version == profile_version,
                )
            )
            or 0
        )

    @staticmethod
    async def ensure_runtime_profile_unreferenced(
        session: AsyncSession,
        *,
        profile_id: str,
        profile_version: str,
    ) -> None:
        count = await ModelVariantRepository.count_profile_references(
            session,
            profile_id=profile_id,
            profile_version=profile_version,
        )
        if count:
            raise RuntimeProfileReferencedError(profile_id, profile_version, count)


def _normalize_resource_name(value: str, field: str) -> str:
    normalized = value.strip()
    if not _RESOURCE_NAME.fullmatch(normalized):
        raise ValueError(f"{field} contains unsupported characters")
    return normalized


def _normalize_profile_id(value: str) -> str:
    normalized = value.strip()
    if not _PROFILE_ID.fullmatch(normalized):
        raise ValueError("runtime_profile_id is malformed")
    return normalized


def _normalize_profile_version(value: str) -> str:
    normalized = value.strip()
    if not _SEMANTIC_VERSION.fullmatch(normalized):
        raise ValueError("runtime_profile_version must be a stable semantic version")
    return normalized


def _normalize_digest(value: str, field: str) -> str:
    normalized = value.strip().casefold()
    if not _DIGEST.fullmatch(normalized):
        raise ValueError(f"{field} must be a sha256 digest")
    return normalized


def _normalize_reference(value: str, field: str, maximum: int) -> str:
    normalized = value.strip()
    if (
        not normalized
        or len(normalized) > maximum
        or any(character.isspace() or ord(character) < 32 for character in normalized)
    ):
        raise ValueError(f"{field} must be non-empty and contain no whitespace or controls")
    return normalized


def _normalize_optional_reference(value: str | None, field: str, maximum: int) -> str | None:
    if value is None:
        return None
    return _normalize_reference(value, field, maximum)


def _normalize_display_text(value: str, field: str, maximum: int) -> str:
    normalized = value.strip()
    if (
        not normalized
        or len(normalized) > maximum
        or any(ord(character) < 32 and character != "\t" for character in normalized)
    ):
        raise ValueError(f"{field} must not be blank or contain control characters")
    return normalized


def _normalize_optional_display_text(value: str | None, field: str, maximum: int) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return _normalize_display_text(normalized, field, maximum)
