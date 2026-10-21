from fastapi import FastAPI, HTTPException , Depends
from app.database import Base , engine
from app.routers import event
from app.models.aggregated_notifications import AggregatedNotification
from app.models.outbox import OutboxEvent
from app.models.notifications import Notify

api = FastAPI()

@api.on_event("startup")
async def startup_event():
    # Automatically creates tables in your Postgres database when FastAPI starts up
    async with engine.begin() as conn:
        # await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
# It reads all ORM models from Base.metadata and creates corresponding tables in the connected database.

@api.get("/health")
def test_api():
    return {"status": "healthy"}
    return HTTPException(status_code=200 , detail= "healthy")

api.include_router(event.router)

