from sqlalchemy import Column , String , Integer ,  DateTime
from sqlalchemy.dialects.postgresql import UUID , JSONB
from app.database import Base
from datetime import datetime , timezone

class events(Base):
    __tablename__ = "Events"

    id = Column(UUID(as_uuid=True), primary_key=True,  nullable=False)
    event_id = Column( String, unique=True,nullable=False)
    event_type = Column(String , nullable=False)
    user_id = Column(Integer , nullable= False)
    source = Column(String , nullable= False)
    entity_id = Column(String, nullable=True)
    payload = Column(JSONB)
    status = Column(String , nullable= False , default="pending")
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))