import base64
import uuid

import pytest
from sqlalchemy import select

from api.errors import ConflictError
from api.schemas.tasks import TaskCreate
from api.services.tasks import TaskService
from core.config import Settings
from core.database import Database
from core.enums import TaskStatus, WorkloadType
from core.image_policy import ImagePolicyAction, ImageRule
from core.rbac import Principal, PrincipalKind
from core.secrets import SecretCipher, SecretKeyRing
from models.identity import Project
from models.registry import TaskSecretBinding
from models.worker import Worker
from repositories.quotas import QuotaRepository
from repositories.registry import ImagePolicyRepository
from repositories.secrets import (
    SecretRepository,
    SecretResolutionError,
    TaskSecretBindingRepository,
)

pytestmark = pytest.mark.integration

PROJECT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
DIGEST = "sha256:" + "a" * 64


def _cipher() -> tuple[SecretCipher, str]:
    encoded = base64.urlsafe_b64encode(b"s" * 32).decode().rstrip("=")
    configured = f"v1:{encoded}"
    return SecretCipher(SecretKeyRing.from_encoded(configured)), configured


async def _configure_policy_and_secret(database: Database) -> tuple[uuid.UUID, SecretCipher, str]:
    cipher, key = _cipher()
    async with database.session() as session, session.begin():
        await ImagePolicyRepository.replace(
            session,
            project_id=PROJECT_ID,
            default_action=ImagePolicyAction.DENY,
            require_digest=True,
            rules=[
                ImageRule(
                    action=ImagePolicyAction.ALLOW,
                    registry="docker.io",
                    repository_glob="library/python",
                    digest=DIGEST,
                )
            ],
        )
        secret = await SecretRepository.create(
            session,
            project_id=PROJECT_ID,
            name="task-token",
            value="ABC123XYZ",
            cipher=cipher,
        )
    return secret.id, cipher, key


async def test_task_creation_persists_only_project_scoped_secret_references(
    database: Database,
) -> None:
    secret_id, _cipher_value, key = await _configure_policy_and_secret(database)
    settings = Settings(
        database_url=str(database.engine.url),
        secret_master_key=key,
        control_plane_enabled=False,
    )
    payload = TaskCreate.model_validate(
        {
            "image": f"python@{DIGEST}",
            "command": ["python", "-c", "print('ok')"],
            "environment": {"PUBLIC": "visible"},
            "secret_bindings": [
                {"secret_id": str(secret_id), "version": 1, "env_name": "TASK_TOKEN"}
            ],
        }
    )

    result = await TaskService(database, settings).create(
        payload,
        idempotency_key=None,
        principal=Principal(kind=PrincipalKind.SYSTEM, project_id=PROJECT_ID),
    )

    async with database.session() as session:
        binding = await session.scalar(
            select(TaskSecretBinding).where(TaskSecretBinding.task_id == result.task.id)
        )
    assert result.task.image == f"docker.io/library/python@{DIGEST}"
    assert result.task.environment == {"PUBLIC": "visible"}
    assert binding is not None
    assert binding.secret_id == secret_id
    assert binding.secret_version == 1
    assert binding.env_name == "TASK_TOKEN"
    assert "ABC123XYZ" not in repr(binding)
    assert "ABC123XYZ" not in repr(result.task)


async def test_task_secret_binding_rejects_cross_project_reference(database: Database) -> None:
    _secret_id, _cipher_value, key = await _configure_policy_and_secret(database)
    other_project_id = uuid.uuid4()
    cipher, _configured = _cipher()
    async with database.session() as session, session.begin():
        session.add(
            Project(id=other_project_id, name="Other", slug=f"other-{other_project_id.hex}")
        )
        await session.flush()
        other_secret = await SecretRepository.create(
            session,
            project_id=other_project_id,
            name="other-token",
            value="other-project-value",
            cipher=cipher,
        )

    payload = TaskCreate.model_validate(
        {
            "image": f"python@{DIGEST}",
            "command": ["python", "-V"],
            "secret_bindings": [
                {
                    "secret_id": str(other_secret.id),
                    "version": 1,
                    "env_name": "TASK_TOKEN",
                }
            ],
        }
    )
    with pytest.raises(ConflictError) as error:
        await TaskService(
            database,
            Settings(
                database_url=str(database.engine.url),
                secret_master_key=key,
                control_plane_enabled=False,
            ),
        ).create(
            payload,
            idempotency_key=None,
            principal=Principal(kind=PrincipalKind.SYSTEM, project_id=PROJECT_ID),
        )
    assert error.value.code == "INVALID_SECRET_BINDING"
    assert str(other_secret.id) not in error.value.message


async def test_authenticated_task_creation_enforces_image_policy(database: Database) -> None:
    _secret_id, _cipher_value, key = await _configure_policy_and_secret(database)
    service = TaskService(
        database,
        Settings(
            database_url=str(database.engine.url),
            secret_master_key=key,
            control_plane_enabled=False,
        ),
    )
    principal = Principal(kind=PrincipalKind.SYSTEM, project_id=PROJECT_ID)

    with pytest.raises(ConflictError) as unpinned:
        await service.create(
            TaskCreate(image="python:3.12", command=["python", "-V"]),
            idempotency_key=None,
            principal=principal,
        )
    assert unpinned.value.code == "IMAGE_POLICY_DENIED"
    assert unpinned.value.details == {"reason": "digest_required"}

    with pytest.raises(ConflictError) as denied:
        await service.create(
            TaskCreate(image=f"busybox@{DIGEST}", command=["true"]),
            idempotency_key=None,
            principal=principal,
        )
    assert denied.value.code == "IMAGE_POLICY_DENIED"


async def test_task_creation_returns_stable_project_quota_error(database: Database) -> None:
    _secret_id, _cipher_value, key = await _configure_policy_and_secret(database)
    async with database.session() as session, session.begin():
        await QuotaRepository.initialize(session, project_id=PROJECT_ID)
        await QuotaRepository.replace(
            session,
            project_id=PROJECT_ID,
            max_queued_tasks=0,
            max_running_tasks=None,
            max_cpu_millicores=None,
            max_memory_mb=None,
            max_gpus=None,
            max_services=None,
            max_service_replicas=None,
            max_artifact_bytes=None,
            daily_cost_limit=None,
        )

    with pytest.raises(ConflictError) as error:
        await TaskService(
            database,
            Settings(
                database_url=str(database.engine.url),
                secret_master_key=key,
                control_plane_enabled=False,
            ),
        ).create(
            TaskCreate(image=f"python@{DIGEST}", command=["python", "-V"]),
            idempotency_key=None,
            principal=Principal(kind=PrincipalKind.SYSTEM, project_id=PROJECT_ID),
        )

    assert error.value.code == "PROJECT_QUOTA_EXCEEDED"
    assert error.value.details == {
        "resource": "queued_tasks",
        "limit": "0",
        "requested": "1",
    }


async def test_task_creation_persists_requested_workload_type(database: Database) -> None:
    _secret_id, _cipher_value, key = await _configure_policy_and_secret(database)

    result = await TaskService(
        database,
        Settings(
            database_url=str(database.engine.url),
            secret_master_key=key,
            control_plane_enabled=False,
        ),
    ).create(
        TaskCreate(
            workload_type=WorkloadType.MODEL_SERVICE,
            image=f"python@{DIGEST}",
            command=["python", "-V"],
        ),
        idempotency_key=None,
        principal=Principal(kind=PrincipalKind.SYSTEM, project_id=PROJECT_ID),
    )

    assert result.task.workload_type == WorkloadType.MODEL_SERVICE


async def test_worker_secret_resolution_is_fenced_and_uses_pinned_version(
    database: Database,
) -> None:
    secret_id, cipher, key = await _configure_policy_and_secret(database)
    settings = Settings(
        database_url=str(database.engine.url),
        secret_master_key=key,
        control_plane_enabled=False,
    )
    payload = TaskCreate.model_validate(
        {
            "image": f"python@{DIGEST}",
            "command": ["python", "-V"],
            "secret_bindings": [
                {"secret_id": str(secret_id), "version": 1, "env_name": "TASK_TOKEN"}
            ],
        }
    )
    result = await TaskService(database, settings).create(
        payload,
        idempotency_key=None,
        principal=Principal(kind=PrincipalKind.SYSTEM, project_id=PROJECT_ID),
    )
    execution_id = uuid.uuid4()
    async with database.session() as session, session.begin():
        session.add(
            Worker(
                id="secret-worker",
                hostname="secret-worker",
                cpu_count=4,
                memory_total_mb=4096,
            )
        )
        task = await session.get(type(result.task), result.task.id, with_for_update=True)
        assert task is not None
        task.status = TaskStatus.PULLING
        task.worker_id = "secret-worker"
        task.execution_id = execution_id
        await SecretRepository.rotate(
            session,
            project_id=PROJECT_ID,
            secret_id=secret_id,
            value="rotated-value",
            cipher=cipher,
        )

    async with database.session() as session, session.begin():
        resolved = await TaskSecretBindingRepository.resolve_for_execution(
            session,
            task_id=result.task.id,
            project_id=PROJECT_ID,
            worker_id="secret-worker",
            execution_id=execution_id,
            cipher=cipher,
        )
    assert dict(resolved.environment) == {"TASK_TOKEN": "ABC123XYZ"}
    assert "ABC123XYZ" not in repr(resolved)
    resolved.clear()

    async with database.session() as session, session.begin():
        with pytest.raises(SecretResolutionError, match="ownership"):
            await TaskSecretBindingRepository.resolve_for_execution(
                session,
                task_id=result.task.id,
                project_id=PROJECT_ID,
                worker_id="secret-worker",
                execution_id=uuid.uuid4(),
                cipher=cipher,
            )
