from sqlalchemy import Column , String , Integer ,  DateTime , Boolean
from sqlalchemy.dialects.postgresql import UUID , JSONB
from app.database import Base
from datetime import datetime , timezone

class Notify(Base):
    __tablename__ = "Notifications"

    id = Column(UUID(as_uuid=True), primary_key=True,  nullable=False)
    event_id = Column( String, unique=True,nullable=False)
    user_id = Column(Integer , nullable= False)
    message= Column(String)
    read = Column(Boolean , default= False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))