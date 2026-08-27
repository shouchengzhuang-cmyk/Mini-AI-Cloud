import uuid
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest
import pytest_asyncio
from sqlalchemy import Table, event

from core.database import Database
from core.enums import AcceleratorKind, AcceleratorVendor, ModelAvailabilityStatus
from core.rbac import ProjectStatus
from models.base import Base
from models.identity import Project, User
from models.model_variant import LogicalModel, LogicalModelStatusEvent, ModelVariant
from repositories.model_variants import (
    LogicalModelRepository,
    ModelVariantInvariantError,
    ModelVariantRepository,
    RuntimeProfileReferencedError,
)


@pytest_asyncio.fixture
async def variant_database(tmp_path: Any) -> AsyncIterator[Database]:
    database = Database(f"sqlite+aiosqlite:///{(tmp_path / 'variants.sqlite3').as_posix()}")

    @event.listens_for(database.engine.sync_engine, "connect")
    def configure_sqlite(dbapi_connection: Any, _connection_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with database.engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                cast(Table, User.__table__),
                cast(Table, Project.__table__),
                cast(Table, LogicalModel.__table__),
                cast(Table, ModelVariant.__table__),
                cast(Table, LogicalModelStatusEvent.__table__),
            ],
        )
    try:
        yield database
    finally:
        await database.dispose()


async def _project(database: Database) -> uuid.UUID:
    project_id = uuid.uuid4()
    async with database.session() as session, session.begin():
        session.add(
            Project(
                id=project_id,
                name="Variant Project",
                slug=f"variant-{project_id.hex[:12]}",
                status=ProjectStatus.ACTIVE,
            )
        )
    return project_id


async def _variant(
    database: Database,
    *,
    project_id: uuid.UUID,
    logical_model_id: uuid.UUID,
    name: str,
    vendor: AcceleratorVendor,
    kind: AcceleratorKind,
    profile_id: str,
    artifact_digest: str,
) -> uuid.UUID:
    async with database.session() as session, session.begin():
        variant = await ModelVariantRepository.create(
            session,
            project_id=project_id,
            logical_model_id=logical_model_id,
            name=name,
            vendor=vendor,
            kind=kind,
            runtime_profile_id=profile_id,
            runtime_profile_version="1.0.0",
            runtime_profile_digest="sha256:" + "c" * 64,
            artifact_source=f"modelscope/{name}",
            artifact_revision=f"{name}-revision",
            artifact_digest=artifact_digest,
            architecture="qwen2",
            dtype="bfloat16",
            quantization=None,
            status=ModelAvailabilityStatus.READY,
            status_reason=None,
            metadata={},
            created_by_user_id=None,
        )
        return variant.id


async def test_ready_model_requires_a_ready_variant_and_preserves_status_history(
    variant_database: Database,
) -> None:
    project_id = await _project(variant_database)
    async with variant_database.session() as session, session.begin():
        model = await LogicalModelRepository.create(
            session,
            project_id=project_id,
            name="qwen-small",
            public_name="Qwen Small",
            description=None,
            metadata={},
            created_by_user_id=None,
        )
        model_id = model.id
        with pytest.raises(ModelVariantInvariantError, match="at least one ready variant"):
            await LogicalModelRepository.set_status(
                session,
                project_id=project_id,
                logical_model_id=model_id,
                status=ModelAvailabilityStatus.READY,
                reason="premature activation",
                created_by_user_id=None,
            )

    nvidia_id = await _variant(
        variant_database,
        project_id=project_id,
        logical_model_id=model_id,
        name="qwen-small-nvidia",
        vendor=AcceleratorVendor.NVIDIA,
        kind=AcceleratorKind.GPU,
        profile_id="nvidia-vllm-k8s",
        artifact_digest="sha256:" + "a" * 64,
    )
    async with variant_database.session() as session, session.begin():
        model = await LogicalModelRepository.set_status(
            session,
            project_id=project_id,
            logical_model_id=model_id,
            status=ModelAvailabilityStatus.READY,
            reason="NVIDIA variant reviewed",
            created_by_user_id=None,
        )
        assert model.version == 2

    async with variant_database.session() as session, session.begin():
        with pytest.raises(ModelVariantInvariantError, match="retain at least one"):
            await ModelVariantRepository.set_status(
                session,
                project_id=project_id,
                logical_model_id=model_id,
                variant_id=nvidia_id,
                status=ModelAvailabilityStatus.DEGRADED,
                reason="health probe failed",
            )

    ascend_id = await _variant(
        variant_database,
        project_id=project_id,
        logical_model_id=model_id,
        name="qwen-small-ascend",
        vendor=AcceleratorVendor.HUAWEI_ASCEND,
        kind=AcceleratorKind.NPU,
        profile_id="ascend-vllm-k8s-a2",
        artifact_digest="sha256:" + "b" * 64,
    )
    async with variant_database.session() as session, session.begin():
        await ModelVariantRepository.set_status(
            session,
            project_id=project_id,
            logical_model_id=model_id,
            variant_id=nvidia_id,
            status=ModelAvailabilityStatus.DEGRADED,
            reason="NVIDIA backend degraded",
        )
        history_count = await LogicalModelRepository.status_history_count(
            session,
            project_id=project_id,
            logical_model_id=model_id,
        )
        assert history_count == 2

        with pytest.raises(ModelVariantInvariantError, match="retain at least one"):
            await ModelVariantRepository.delete(
                session,
                project_id=project_id,
                logical_model_id=model_id,
                variant_id=ascend_id,
            )


async def test_profile_deletion_guard_counts_all_variant_references(
    variant_database: Database,
) -> None:
    project_id = await _project(variant_database)
    async with variant_database.session() as session, session.begin():
        model = await LogicalModelRepository.create(
            session,
            project_id=project_id,
            name="profile-reference",
            public_name="Profile Reference",
            description=None,
            metadata={},
            created_by_user_id=None,
        )
        model_id = model.id
    await _variant(
        variant_database,
        project_id=project_id,
        logical_model_id=model_id,
        name="profile-reference-nvidia",
        vendor=AcceleratorVendor.NVIDIA,
        kind=AcceleratorKind.GPU,
        profile_id="nvidia-vllm-k8s",
        artifact_digest="sha256:" + "d" * 64,
    )

    async with variant_database.session() as session:
        with pytest.raises(RuntimeProfileReferencedError) as blocked:
            await ModelVariantRepository.ensure_runtime_profile_unreferenced(
                session,
                profile_id="nvidia-vllm-k8s",
                profile_version="1.0.0",
            )
        assert blocked.value.count == 1
        await ModelVariantRepository.ensure_runtime_profile_unreferenced(
            session,
            profile_id="nvidia-vllm-k8s",
            profile_version="2.0.0",
        )
