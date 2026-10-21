
import asyncio
import selectors
from datetime import datetime, timezone

from sqlalchemy import select

from app.database import sessionLocal
from app.models.outbox import OutboxEvent
from app.redis_client import redis_client
from app.services.redis_queue import RedisQueue
from app.services.logger import setup_logger


logger = setup_logger("outbox_worker")

redis_queue = RedisQueue(redis_client)


async def process_pending_outbox_events():
    """Deliver pending outbox records to Redis."""

    async with sessionLocal() as db:
        async with db.begin():

            result = await db.execute(
                select(OutboxEvent)
                .where(OutboxEvent.status == "pending")
                .order_by(OutboxEvent.created_at)
                .limit(20)
                .with_for_update(skip_locked=True)
            )

            outbox_events = result.scalars().all()

            for outbox_event in outbox_events:
                enqueued = await redis_queue.enqueue(
                    outbox_event.payload
                )

                if not enqueued:
                    logger.error(
                        f"Redis delivery failed for event "
                        f"{outbox_event.event_id}. "
                        "It will remain pending."
                    )
                    break

                outbox_event.status = "delivered"
                outbox_event.delivered_at = (
                    datetime.now(timezone.utc)
                )

                logger.info(
                    f"Outbox event {outbox_event.event_id} "
                    "delivered to Redis."
                )


async def run_outbox_worker():
    """Continuously check for pending outbox records."""

    logger.info("Outbox worker started.")

    while True:
        try:
            await process_pending_outbox_events()

        except Exception:
            logger.exception(
                "Outbox worker encountered an error."
            )

        await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(
        run_outbox_worker(),
        loop_factory=lambda: asyncio.SelectorEventLoop(
            selectors.SelectSelector()
        )
    )
