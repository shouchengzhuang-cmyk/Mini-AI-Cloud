import uuid
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest
import pytest_asyncio
from sqlalchemy import Table, event

from core.database import Database
from core.enums import (
    AcceleratorKind,
    AcceleratorVendor,
    GatewayRoutingPolicy,
    ModelAvailabilityStatus,
)
from core.rbac import ProjectStatus
from models.base import Base
from models.identity import Project, User
from models.model_variant import LogicalModel, LogicalModelStatusEvent, ModelVariant
from models.registry import RegisteredModel
from models.service import ModelService
from repositories.model_variants import (
    LogicalModelConflictError,
    LogicalModelNotFoundError,
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
                cast(Table, RegisteredModel.__table__),
                cast(Table, ModelService.__table__),
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
    profile_version: str = "1.0.0",
    profile_digest: str = "sha256:" + "c" * 64,
    status: ModelAvailabilityStatus = ModelAvailabilityStatus.READY,
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
            runtime_profile_version=profile_version,
            runtime_profile_digest=profile_digest,
            artifact_source=f"modelscope/{name}",
            artifact_revision=f"{name}-revision",
            artifact_digest=artifact_digest,
            architecture="qwen2",
            dtype="bfloat16",
            quantization=None,
            status=status,
            status_reason=None,
            metadata={},
            created_by_user_id=None,
        )
        return variant.id


async def test_logical_model_create_missing_project_preserves_not_found_precedence(
    variant_database: Database,
) -> None:
    async with variant_database.session() as session, session.begin():
        with pytest.raises(LogicalModelNotFoundError, match="active project does not exist"):
            await LogicalModelRepository.create(
                session,
                project_id=uuid.uuid4(),
                name=" invalid name ",
                public_name=" ",
                description=None,
                metadata={},
                created_by_user_id=None,
            )


async def test_logical_model_routing_policy_updates_without_status_event(
    variant_database: Database,
) -> None:
    project_id = await _project(variant_database)
    async with variant_database.session() as session, session.begin():
        model = await LogicalModelRepository.create(
            session,
            project_id=project_id,
            name="routing-policy",
            public_name="Routing Policy",
            description=None,
            metadata={},
            created_by_user_id=None,
            routing_policy=GatewayRoutingPolicy.STRICT_NVIDIA,
        )
        model_id = model.id
        model.routing_cursor = 7

    async with variant_database.session() as session, session.begin():
        updated = await LogicalModelRepository.set_routing_policy(
            session,
            project_id=project_id,
            logical_model_id=model_id,
            routing_policy=GatewayRoutingPolicy.BALANCED,
        )
        assert updated.routing_policy is GatewayRoutingPolicy.BALANCED
        assert updated.routing_cursor == 0
        assert updated.status is ModelAvailabilityStatus.DISABLED
        assert updated.version == 2
        assert (
            await LogicalModelRepository.status_history_count(
                session,
                project_id=project_id,
                logical_model_id=model_id,
            )
            == 1
        )

    async with variant_database.session() as session, session.begin():
        with pytest.raises(LogicalModelConflictError) as conflict:
            await LogicalModelRepository.create(
                session,
                project_id=project_id,
                name="routing-policy-copy",
                public_name="Routing Policy",
                description=None,
                metadata={},
                created_by_user_id=None,
            )
        assert conflict.value.field == "public_name"


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


async def test_ready_candidates_filter_and_sort_deterministically(
    variant_database: Database,
) -> None:
    project_id = await _project(variant_database)
    async with variant_database.session() as session, session.begin():
        model = await LogicalModelRepository.create(
            session,
            project_id=project_id,
            name="admission-model",
            public_name="Admission Model",
            description=None,
            metadata={},
            created_by_user_id=None,
        )
        model_id = model.id

    await _variant(
        variant_database,
        project_id=project_id,
        logical_model_id=model_id,
        name="z-ascend",
        vendor=AcceleratorVendor.HUAWEI_ASCEND,
        kind=AcceleratorKind.NPU,
        profile_id="ascend-vllm-k8s-a2",
        artifact_digest="sha256:" + "1" * 64,
    )
    await _variant(
        variant_database,
        project_id=project_id,
        logical_model_id=model_id,
        name="z-nvidia",
        vendor=AcceleratorVendor.NVIDIA,
        kind=AcceleratorKind.GPU,
        profile_id="nvidia-vllm-k8s",
        artifact_digest="sha256:" + "2" * 64,
    )
    await _variant(
        variant_database,
        project_id=project_id,
        logical_model_id=model_id,
        name="a-nvidia",
        vendor=AcceleratorVendor.NVIDIA,
        kind=AcceleratorKind.GPU,
        profile_id="nvidia-vllm-k8s",
        artifact_digest="sha256:" + "3" * 64,
    )
    await _variant(
        variant_database,
        project_id=project_id,
        logical_model_id=model_id,
        name="ignored-nvidia",
        vendor=AcceleratorVendor.NVIDIA,
        kind=AcceleratorKind.GPU,
        profile_id="nvidia-vllm-k8s",
        artifact_digest="sha256:" + "4" * 64,
        status=ModelAvailabilityStatus.DEGRADED,
    )

    async with variant_database.session() as session:
        assert (
            await ModelVariantRepository.list_ready_candidates(
                session,
                project_id=project_id,
                logical_model_id=model_id,
                allowed_vendors=(
                    AcceleratorVendor.NVIDIA,
                    AcceleratorVendor.HUAWEI_ASCEND,
                ),
                allowed_kinds=(AcceleratorKind.GPU, AcceleratorKind.NPU),
            )
            == []
        )

    async with variant_database.session() as session, session.begin():
        await LogicalModelRepository.set_status(
            session,
            project_id=project_id,
            logical_model_id=model_id,
            status=ModelAvailabilityStatus.READY,
            reason="admission enabled",
            created_by_user_id=None,
        )
        candidates = await ModelVariantRepository.list_ready_candidates(
            session,
            project_id=project_id,
            logical_model_id=model_id,
            allowed_vendors=(
                AcceleratorVendor.NVIDIA,
                AcceleratorVendor.HUAWEI_ASCEND,
            ),
            allowed_kinds=(AcceleratorKind.GPU, AcceleratorKind.NPU),
            for_update=True,
        )
        assert [candidate.name for candidate in candidates] == [
            "z-ascend",
            "a-nvidia",
            "z-nvidia",
        ]

        nvidia_candidates = await ModelVariantRepository.list_ready_candidates(
            session,
            project_id=project_id,
            logical_model_id=model_id,
            allowed_vendors=(AcceleratorVendor.NVIDIA,),
            allowed_kinds=(AcceleratorKind.GPU,),
            runtime_profile_id="nvidia-vllm-k8s",
        )
        assert [candidate.name for candidate in nvidia_candidates] == [
            "a-nvidia",
            "z-nvidia",
        ]
        assert (
            await ModelVariantRepository.list_ready_candidates(
                session,
                project_id=project_id,
                logical_model_id=model_id,
                allowed_vendors=(AcceleratorVendor.NVIDIA,),
                allowed_kinds=(AcceleratorKind.GPU,),
                runtime_profile_id="nvidia-missing-k8s",
            )
            == []
        )
        assert (
            await ModelVariantRepository.list_ready_candidates(
                session,
                project_id=project_id,
                logical_model_id=model_id,
                allowed_vendors=(AcceleratorVendor.NVIDIA,),
                allowed_kinds=(AcceleratorKind.NPU,),
            )
            == []
        )
        assert (
            await ModelVariantRepository.list_ready_candidates(
                session,
                project_id=project_id,
                logical_model_id=model_id,
                allowed_vendors=(),
                allowed_kinds=(AcceleratorKind.GPU,),
            )
            == []
        )


async def test_reservation_revalidation_requires_exact_ready_snapshot(
    variant_database: Database,
) -> None:
    project_id = await _project(variant_database)
    artifact_digest = "sha256:" + "a" * 64
    profile_digest = "sha256:" + "b" * 64
    async with variant_database.session() as session, session.begin():
        model = await LogicalModelRepository.create(
            session,
            project_id=project_id,
            name="reservation-model",
            public_name="Reservation Model",
            description=None,
            metadata={},
            created_by_user_id=None,
        )
        model_id = model.id

    variant_id = await _variant(
        variant_database,
        project_id=project_id,
        logical_model_id=model_id,
        name="reservation-nvidia",
        vendor=AcceleratorVendor.NVIDIA,
        kind=AcceleratorKind.GPU,
        profile_id="nvidia-vllm-k8s",
        profile_version="2.0.0",
        profile_digest=profile_digest,
        artifact_digest=artifact_digest,
    )
    await _variant(
        variant_database,
        project_id=project_id,
        logical_model_id=model_id,
        name="reservation-ascend",
        vendor=AcceleratorVendor.HUAWEI_ASCEND,
        kind=AcceleratorKind.NPU,
        profile_id="ascend-vllm-k8s-a2",
        artifact_digest="sha256:" + "c" * 64,
    )

    async with variant_database.session() as session, session.begin():
        await LogicalModelRepository.set_status(
            session,
            project_id=project_id,
            logical_model_id=model_id,
            status=ModelAvailabilityStatus.READY,
            reason="reservation enabled",
            created_by_user_id=None,
        )

        async def revalidate(
            *,
            expected_project_id: uuid.UUID = project_id,
            expected_logical_model_id: uuid.UUID = model_id,
            expected_variant_id: uuid.UUID = variant_id,
            expected_vendor: AcceleratorVendor = AcceleratorVendor.NVIDIA,
            expected_kind: AcceleratorKind = AcceleratorKind.GPU,
            expected_runtime_profile_id: str = "nvidia-vllm-k8s",
            expected_runtime_profile_version: str = "2.0.0",
            expected_artifact_digest: str = artifact_digest,
            expected_runtime_profile_digest: str = profile_digest,
            for_update: bool = True,
        ) -> ModelVariant | None:
            return await ModelVariantRepository.revalidate_ready_for_reservation(
                session,
                project_id=expected_project_id,
                logical_model_id=expected_logical_model_id,
                expected_variant_id=expected_variant_id,
                expected_vendor=expected_vendor,
                expected_kind=expected_kind,
                expected_runtime_profile_id=expected_runtime_profile_id,
                expected_runtime_profile_version=expected_runtime_profile_version,
                expected_artifact_digest=expected_artifact_digest,
                expected_runtime_profile_digest=expected_runtime_profile_digest,
                for_update=for_update,
            )

        exact = await revalidate()
        assert exact is not None
        assert exact.id == variant_id

        assert (
            await revalidate(
                expected_artifact_digest="sha256:" + "d" * 64,
                for_update=False,
            )
            is None
        )
        assert await revalidate(expected_runtime_profile_digest="sha256:" + "e" * 64) is None
        assert await revalidate(expected_vendor=AcceleratorVendor.HUAWEI_ASCEND) is None
        assert await revalidate(expected_kind=AcceleratorKind.NPU) is None
        assert await revalidate(expected_runtime_profile_id="ascend-vllm-k8s-a2") is None
        assert await revalidate(expected_runtime_profile_version="1.0.0") is None
        assert await revalidate(expected_variant_id=uuid.uuid4()) is None
        assert await revalidate(expected_project_id=uuid.uuid4()) is None
        assert await revalidate(expected_logical_model_id=uuid.uuid4()) is None

        await LogicalModelRepository.set_status(
            session,
            project_id=project_id,
            logical_model_id=model_id,
            status=ModelAvailabilityStatus.DEGRADED,
            reason="logical model paused",
            created_by_user_id=None,
        )
        assert await revalidate() is None

        await LogicalModelRepository.set_status(
            session,
            project_id=project_id,
            logical_model_id=model_id,
            status=ModelAvailabilityStatus.READY,
            reason="logical model resumed",
            created_by_user_id=None,
        )
        await ModelVariantRepository.set_status(
            session,
            project_id=project_id,
            logical_model_id=model_id,
            variant_id=variant_id,
            status=ModelAvailabilityStatus.DEGRADED,
            reason="variant paused",
        )
        assert await revalidate() is None
