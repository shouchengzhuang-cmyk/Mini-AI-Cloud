from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.artifacts import ArtifactState
from core.rbac import ProjectStatus
from models.artifact import Artifact, Dataset, DatasetVersion
from models.identity import Project
from repositories.clock import database_utcnow


class DatasetNotFoundError(LookupError):
    pass


class DatasetConflictError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class DatasetSummary:
    dataset: Dataset
    current: DatasetVersion


class DatasetRepository:
    @staticmethod
    async def create(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        name: str,
        description: str | None,
        artifact_id: uuid.UUID,
        metadata: dict[str, object],
    ) -> DatasetSummary:
        await _lock_active_project(session, project_id)
        normalized_name = name.strip()
        duplicate = await session.scalar(
            select(Dataset.id).where(
                Dataset.project_id == project_id,
                func.lower(Dataset.name) == normalized_name.casefold(),
            )
        )
        if duplicate is not None:
            raise DatasetConflictError("dataset name already exists in the project")
        artifact = await _lock_ready_artifact(session, project_id, artifact_id)
        now = await database_utcnow(session)
        dataset = Dataset(
            project_id=project_id,
            name=normalized_name,
            description=description,
            current_version=1,
            created_at=now,
        )
        session.add(dataset)
        await session.flush()
        version = DatasetVersion(
            dataset_id=dataset.id,
            version=1,
            artifact_id=artifact.id,
            metadata_json=dict(metadata),
            created_at=now,
        )
        session.add(version)
        await session.flush()
        return DatasetSummary(dataset=dataset, current=version)

    @staticmethod
    async def add_version(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        dataset_id: uuid.UUID,
        artifact_id: uuid.UUID,
        metadata: dict[str, object],
    ) -> DatasetSummary:
        await _lock_active_project(session, project_id)
        dataset = await DatasetRepository.get_dataset(
            session,
            project_id=project_id,
            dataset_id=dataset_id,
            for_update=True,
        )
        if dataset is None:
            raise DatasetNotFoundError("dataset does not exist in the project")
        artifact = await _lock_ready_artifact(session, project_id, artifact_id)
        now = await database_utcnow(session)
        next_version = dataset.current_version + 1
        version = DatasetVersion(
            dataset_id=dataset.id,
            version=next_version,
            artifact_id=artifact.id,
            metadata_json=dict(metadata),
            created_at=now,
        )
        session.add(version)
        dataset.current_version = next_version
        await session.flush()
        return DatasetSummary(dataset=dataset, current=version)

    @staticmethod
    async def get_dataset(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        dataset_id: uuid.UUID,
        for_update: bool = False,
    ) -> Dataset | None:
        query = select(Dataset).where(
            Dataset.id == dataset_id,
            Dataset.project_id == project_id,
        )
        if for_update:
            query = query.with_for_update()
        return await session.scalar(query)

    @staticmethod
    async def get_summary(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        dataset_id: uuid.UUID,
    ) -> DatasetSummary:
        dataset = await DatasetRepository.get_dataset(
            session,
            project_id=project_id,
            dataset_id=dataset_id,
        )
        if dataset is None:
            raise DatasetNotFoundError("dataset does not exist in the project")
        version = await session.get(
            DatasetVersion,
            {"dataset_id": dataset.id, "version": dataset.current_version},
        )
        if version is None:
            raise DatasetConflictError("dataset current version is missing")
        return DatasetSummary(dataset=dataset, current=version)

    @staticmethod
    async def list_summaries(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        limit: int,
        offset: int,
    ) -> list[DatasetSummary]:
        datasets = list(
            await session.scalars(
                select(Dataset)
                .where(Dataset.project_id == project_id)
                .order_by(Dataset.created_at.desc(), Dataset.id.desc())
                .limit(limit)
                .offset(offset)
            )
        )
        if not datasets:
            return []
        versions = list(
            await session.scalars(
                select(DatasetVersion).where(
                    DatasetVersion.dataset_id.in_([item.id for item in datasets])
                )
            )
        )
        by_key = {(item.dataset_id, item.version): item for item in versions}
        summaries: list[DatasetSummary] = []
        for dataset in datasets:
            current = by_key.get((dataset.id, dataset.current_version))
            if current is None:
                raise DatasetConflictError("dataset current version is missing")
            summaries.append(DatasetSummary(dataset=dataset, current=current))
        return summaries

    @staticmethod
    async def count(session: AsyncSession, *, project_id: uuid.UUID) -> int:
        return int(
            await session.scalar(
                select(func.count(Dataset.id)).where(Dataset.project_id == project_id)
            )
            or 0
        )

    @staticmethod
    async def list_versions(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        dataset_id: uuid.UUID,
    ) -> list[DatasetVersion]:
        if (
            await DatasetRepository.get_dataset(
                session,
                project_id=project_id,
                dataset_id=dataset_id,
            )
            is None
        ):
            raise DatasetNotFoundError("dataset does not exist in the project")
        return list(
            await session.scalars(
                select(DatasetVersion)
                .where(DatasetVersion.dataset_id == dataset_id)
                .order_by(DatasetVersion.version.desc())
            )
        )


async def _lock_active_project(session: AsyncSession, project_id: uuid.UUID) -> Project:
    project = await session.scalar(
        select(Project).where(Project.id == project_id).with_for_update()
    )
    if project is None or project.status != ProjectStatus.ACTIVE:
        raise DatasetNotFoundError("active project does not exist")
    return project


async def _lock_ready_artifact(
    session: AsyncSession, project_id: uuid.UUID, artifact_id: uuid.UUID
) -> Artifact:
    artifact = await session.scalar(
        select(Artifact)
        .where(
            Artifact.id == artifact_id,
            Artifact.project_id == project_id,
        )
        .with_for_update()
    )
    if artifact is None:
        raise DatasetNotFoundError("artifact does not exist in the project")
    if artifact.state != ArtifactState.READY.value or artifact.deleted_at is not None:
        raise DatasetConflictError("dataset versions require a ready artifact")
    return artifact
