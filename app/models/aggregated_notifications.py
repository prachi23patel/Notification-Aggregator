from sqlalchemy import Column , String , Integer ,  DateTime , Boolean , func , Index
from sqlalchemy.dialects.postgresql import UUID , JSONB
import uuid
from app.database import Base
from datetime import datetime , timezone

class AggregatedNotification(Base):
    __tablename__ = "aggregated_notifications"

    # id : Same pattern as events and notifications – globally unique, no auto‑increment collisions across shards later.
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(Integer, nullable=False)
    # aggregation key : The string "{user_id}:{event_type}:{entity_id}" – this is what we search for when updating. Indexed for fast lookup.
    aggregation_key = Column(String, nullable=False, index=True)
    # event_count : Starts at 1, incremented atomically. The "3" in "3 comments on doc_55".
    event_count = Column(Integer, nullable=False, default=1)
    # message : Human‑readable summary – e.g., "3 COMMENT_CREATED events on doc_55". We'll improve this in a later task.
    message = Column(String, nullable=False)
    # read : Default false. When the user opens the notification, we set it to true – then it's considered "closed" for aggregation
    read = Column(Boolean, nullable=False, default=False)
    # first_event_at : When the first event in this batch arrived (useful for UI: "started 5 min ago").
    first_event_at = Column(DateTime(timezone=True), server_default=func.now())
    # last_event_at : When the most recent event arrived. Used for the 1‑hour sliding window check (last_event_at > now() - interval '1 hour').
    last_event_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        # Composite index: fast lookup for "unread aggregations for this user"
        Index("ix_agg_user_read", "user_id", "read"),
    )