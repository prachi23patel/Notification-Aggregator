from pydantic import BaseModel , EmailStr , ConfigDict , Field 
from uuid import UUID
from typing import Any , Dict, Optional


# 1. Define your data schema using Pydantic
class EventRequest(BaseModel):
    event_id: str
    event_type: str
    user_id: int
    source: str
    payload: Optional[Dict[str, Any]] = None
    entity_id : Optional[str] = None  
