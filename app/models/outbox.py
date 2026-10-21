
import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, String, DateTime
from sqlalchemy.dialects.postgresql import UUID, JSONB

from app.database import Base


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )

    event_id = Column(
        String,
        unique=True,
        nullable=False,
        index=True
    )

    payload = Column(JSONB, nullable=False)

    status = Column(
        String,
        nullable=False,
        default="pending"
    )

    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False
    )

    delivered_at = Column(
        DateTime(timezone=True),
        nullable=True
    )
