from fastapi import APIRouter ,Depends , HTTPException , status 
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from datetime import datetime , timezone
import uuid
from app.database import get_db 
from app.redis_client import redis_client
from app.schemas.events import EventRequest
from app.models.events import events
from app.models.outbox import OutboxEvent
from app.services.redis_queue import RedisQueue
from app.services.logger import setup_logger

router = APIRouter()

redis_queue = RedisQueue(redis_client)
logger = setup_logger("api.events")

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
        event_dict = e.model_dump()

        # Add to session and commit to PostgreSQL
        outbox_event = OutboxEvent(
            event_id=e.event_id,
            payload=event_dict,
            status="pending"
        )

        db.add(db_event)
        db.add(outbox_event)

        await db.commit()
        await db.refresh(db_event) # Optional: Reloads data from DB (like created_at timestamp)
        logger.info(f"Successfully saved event {e.event_id} to PostgreSQL database.")

        
    # Preserve intentional HTTP errors such as HTTP 503
    except HTTPException:
        raise

    # IntegrityError can mean a unique-constraint violation, a foreign-key violation, or another database integrity issue. We shouldn't claim every integrity error is definitely a duplicate.
    except IntegrityError as ex:
        await db.rollback()#cancels the current uncommitted database transaction.

        # ex is the SQLAlchemy error.
        # ex.orig is the original database-driver error.
        # ex.orig.diag.constraint_name can tell us which PostgreSQL constraint failed.
        # getattr() safely reads an attribute, returning None if it is unavailable.
        error_message = str(ex.orig)
        constraint_name = getattr(
            getattr(ex.orig, "diag", None),
            "constraint_name",
            None
        )
        if constraint_name == "Events_event_id_key":
            logger.warning(
                f"Duplicate event_id received: {e.event_id}"
            )
            raise HTTPException(
                status_code=409,
                detail="Event ID already exists."
            ) from ex

        # Handle other integrity errors
        logger.exception(
            f"Database integrity error for event {e.event_id}: "
            f"{error_message}"
        )
        raise HTTPException(
            status_code=500,
            detail="Database integrity error."
        ) from ex
    except Exception as ex:
        await db.rollback() # Rollback transaction if database crash happens
        logger.error(f"Database insertion failed for event {e.event_id}. Error: {ex}")
        print(f"Database Error: {ex}")
        raise HTTPException(status_code=500, detail="Internal server error saving data")
    return {
        "notification_id": unique_notif_id,
        "status": "accepted"
    }

