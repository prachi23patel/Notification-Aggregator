from fastapi import APIRouter ,Depends , HTTPException , status 
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime , timezone
import uuid
from app.database import get_db 
from app.redis_client import redis_client
from app.schemas.events import EventRequest
from app.models.events import events
from app.services.redis_queue import RedisQueue
from app.services.logger import setup_logger
from app.routers import worker

router = APIRouter()

redis_queue = RedisQueue(redis_client)
logger = setup_logger("api.events")
work = worker.Worker()

@router.post("/events" ,  status_code=status.HTTP_202_ACCEPTED)
async def create_user(e: EventRequest , db : AsyncSession = Depends(get_db)):
    #  Log the entire received event with a timestamp to the console
    timestamp = datetime.now(timezone.utc).isoformat()
    # .model_dump() converts the validated Pydantic model into a standard Python dict
    logger.info(f"[{timestamp}] Incoming event received: {e.model_dump()}")

    unique_notif_id = f"{uuid.uuid4()}"

    try:
        # ------------------------
        # storing in postgres database
        # ------------------------
        # Create a database record instance from incoming Pydantic validation data
        db_event = events(
            id=unique_notif_id,
            event_id=e.event_id,
            event_type=e.event_type,
            user_id=e.user_id,
            source=e.source,
            entity_id=e.entity_id,
            payload=e.payload
        )
        
        # Add to session and commit to PostgreSQL
        db.add(db_event)
        await db.commit()
        await db.refresh(db_event) # Optional: Reloads data from DB (like created_at timestamp)
        logger.info(f"Successfully saved event {e.event_id} to PostgreSQL database.")

        # ------------------------
        # storing in redis queue
        # ------------------------
        event_dict = e.model_dump()
        event_dict["id"] = unique_notif_id
        await redis_queue.enqueue(event_dict)


    except Exception as ex:
        await db.rollback() # Rollback transaction if database crash happens
        logger.error(f"Database insertion failed for event {e.event_id}. Error: {ex}")
        print(f"Database Error: {ex}")
        raise HTTPException(status_code=500, detail="Internal server error saving data")
    return {
        "notification_id": unique_notif_id,
        "status": "accepted"
    }

# fetches one event from event(main) redis queue
@router.get("/event_queue")
async def event_queue(db : AsyncSession = Depends(get_db)):
    response = await work.process_queue_with_blpop()
    return response
