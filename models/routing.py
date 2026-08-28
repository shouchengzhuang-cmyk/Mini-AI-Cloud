import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, utcnow


class VendorCircuitState(Base):
    """Shared circuit state for one logical-model vendor backend."""

    __tablename__ = "vendor_circuit_states"
    __table_args__ = (
        UniqueConstraint(
            "project_id",
            "logical_model_id",
            "vendor",
            name="uq_vendor_circuit_project_model_vendor",
        ),
        CheckConstraint("vendor IN ('nvidia','huawei-ascend')", name="vendor"),
        CheckConstraint("state IN ('closed','open')", name="state"),
        CheckConstraint("failure_count >= 0", name="failure_count"),
        CheckConstraint(
            "(state = 'closed' AND opened_until IS NULL) OR "
            "(state = 'open' AND opened_until IS NOT NULL)",
            name="opened_until",
        ),
        Index("ix_vendor_circuit_open", "state", "opened_until"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), index=True
    )
    logical_model_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("logical_models.id", ondelete="CASCADE"), index=True
    )
    vendor: Mapped[str] = mapped_column(String(64))
    state: Mapped[str] = mapped_column(String(16), default="closed")
    failure_count: Mapped[int] = mapped_column(Integer, default=0)
    opened_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_code: Mapped[str | None] = mapped_column(String(64))
    version: Mapped[int] = mapped_column(Integer, default=1)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )
