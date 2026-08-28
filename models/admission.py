import uuid
from datetime import datetime

from sqlalchemy import JSON, CheckConstraint, DateTime, ForeignKey, Index, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, utcnow


class AdmissionEvent(Base):
    """Append-only record of one vendor-aware admission decision."""

    __tablename__ = "admission_events"
    __table_args__ = (
        CheckConstraint(
            "workload_type IN ('batch_job','model_service')",
            name="workload_type",
        ),
        CheckConstraint(
            "policy IN ('any','nvidia-only','ascend-only','prefer-nvidia','prefer-ascend')",
            name="policy",
        ),
        CheckConstraint("length(outcome) > 0", name="outcome"),
        CheckConstraint("length(reason) > 0", name="reason"),
        CheckConstraint(
            "(selected_vendor IS NULL AND selected_kind IS NULL "
            "AND selected_model IS NULL AND runtime_profile_id IS NULL "
            "AND runtime_profile_version IS NULL AND runtime_profile_digest IS NULL "
            "AND model_variant_id IS NULL AND allocation_authority IS NULL) OR "
            "(selected_vendor IS NOT NULL AND selected_kind IS NOT NULL "
            "AND selected_model IS NOT NULL AND allocation_authority IS NOT NULL)",
            name="selected_snapshot",
        ),
        CheckConstraint(
            "(runtime_profile_id IS NULL AND runtime_profile_version IS NULL "
            "AND runtime_profile_digest IS NULL) OR "
            "(runtime_profile_id IS NOT NULL AND runtime_profile_version IS NOT NULL "
            "AND runtime_profile_digest IS NOT NULL)",
            name="selected_profile_snapshot",
        ),
        CheckConstraint(
            "model_variant_id IS NULL OR runtime_profile_id IS NOT NULL",
            name="selected_variant_profile",
        ),
        CheckConstraint(
            "allocation_authority IS NULL "
            "OR allocation_authority != 'kubernetes_device_plugin' "
            "OR runtime_profile_id IS NOT NULL",
            name="selected_profile_authority",
        ),
        CheckConstraint(
            "selected_vendor IS NULL OR "
            "(selected_vendor = 'nvidia' AND selected_kind = 'gpu') OR "
            "(selected_vendor = 'huawei-ascend' AND selected_kind = 'npu')",
            name="selected_vendor_kind",
        ),
        CheckConstraint(
            "runtime_profile_digest IS NULL OR "
            "(length(runtime_profile_digest) = 71 "
            "AND runtime_profile_digest LIKE 'sha256:%')",
            name="profile_digest",
        ),
        CheckConstraint(
            "allocation_authority IS NULL OR allocation_authority IN "
            "('control_plane_exact_device','kubernetes_device_plugin')",
            name="allocation_authority",
        ),
        CheckConstraint(
            "selected_vendor IS NULL OR policy NOT IN ('nvidia-only','ascend-only') OR "
            "(policy = 'nvidia-only' AND selected_vendor = 'nvidia') OR "
            "(policy = 'ascend-only' AND selected_vendor = 'huawei-ascend')",
            name="policy_vendor",
        ),
        CheckConstraint(
            "length(CAST(candidate_summary AS TEXT)) <= 16384",
            name="candidate_summary_size",
        ),
        CheckConstraint(
            "CAST(candidate_summary AS TEXT) NOT LIKE '%\"concrete_device_ids\"%' "
            "AND CAST(candidate_summary AS TEXT) NOT LIKE '%\"device_ids\"%' "
            "AND CAST(candidate_summary AS TEXT) NOT LIKE '%\"device_uuid\"%'",
            name="candidate_summary_devices",
        ),
        Index(
            "ix_admission_events_project_occurred",
            "project_id",
            "occurred_at",
            "id",
        ),
        Index(
            "ix_admission_events_workload_occurred",
            "workload_type",
            "workload_id",
            "occurred_at",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="RESTRICT"), index=True
    )
    workload_type: Mapped[str] = mapped_column(String(32))
    workload_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True))
    execution_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True))
    policy: Mapped[str] = mapped_column(String(32))
    outcome: Mapped[str] = mapped_column(String(32))
    reason: Mapped[str] = mapped_column(String(128))
    selected_vendor: Mapped[str | None] = mapped_column(String(64))
    selected_kind: Mapped[str | None] = mapped_column(String(32))
    selected_model: Mapped[str | None] = mapped_column(String(255))
    runtime_profile_id: Mapped[str | None] = mapped_column(String(128))
    runtime_profile_version: Mapped[str | None] = mapped_column(String(32))
    runtime_profile_digest: Mapped[str | None] = mapped_column(String(71))
    model_variant_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("model_variants.id", ondelete="RESTRICT"), index=True
    )
    allocation_authority: Mapped[str | None] = mapped_column(String(64))
    candidate_summary: Mapped[list[dict[str, object]]] = mapped_column(JSON, default=list)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
