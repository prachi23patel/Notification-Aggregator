import uuid
import os
import asyncio
import selectors

from sqlalchemy import update , select ,func ,text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.events import events
from app.models.notifications import Notify
from app.models.aggregated_notifications import AggregatedNotification
from app.database import sessionLocal
from app.redis_client import redis_client
from app.services.redis_queue import RedisQueue
from app.services.logger import setup_logger


logger = setup_logger("worker")

# Maximum number of retries AFTER the initial attempt
MAX_RETRIES = 3

class EventNotFoundError(Exception):
    pass

class Worker:
    # Production:
    # Worker()
    # ├── postgres DB → production database (session_factory)
    # └── Redis → production Redis (redis)

    # Test:
    # Worker(test_session_factory, test_redis)
    # ├── postgres DB → test database (session_factory)
    # └── Redis → test Redis (redis)

    def __init__(self, session_factory=None , redis= None):
        # Create a RedisQueue object, and give it either the Redis connection supplied by the caller or the application's default Redis connection.
        self.queue = RedisQueue(
            redis if redis is not None else redis_client
        )

        # Production uses sessionLocal.
        # Tests can inject their own session factory.
        self.session_factory = (
            session_factory
            if session_factory is not None
            else sessionLocal
        )
    
        # Unique ID for this worker process
        self.worker_id = os.getpid()

        logger.info(
            f"Worker {self.worker_id} starting..."
        )


    # =========================================================
    # PROCESS ONE EVENT
    # =========================================================
    async def event_process(self, event_data: dict) -> bool:
        """
        Process one event.
        1. Update event status in PostgreSQL.
        2. Create notification.
        3. Both operations happen inside one transaction.

        Returns:
            True  -> processing succeeded
            False -> event was not found
        """

        event_id = event_data["event_id"]
        event_type = event_data["event_type"]
        source = event_data["source"]
        user_id = event_data["user_id"]
        payload = event_data.get("payload") or {}
        entity_id = payload.get("entity_id")


        logger.info(
            f"Worker {self.worker_id} processing event: "
            f"event_id={event_id}, "
            f"event_type={event_type}, "
            f"retry_count={event_data.get('retry_count', 0)}"
        )

        async with self.session_factory() as datab:

            async with datab.begin():
                # =================================================
                # 1. CREATE NOTIFICATION
                # =================================================
                notification_id = str(uuid.uuid4())
                notification_stmt = (
                    pg_insert(Notify)
                    .values(
                        id=notification_id,
                        event_id=event_id,
                        user_id=user_id,
                        message=(
                            f"Event {event_type} "
                            f"received from {source}"
                        ),
                        read=False
                    )
                    .on_conflict_do_nothing(
                        index_elements=[Notify.event_id]
                    )
                    .returning(Notify.id)# That asks PostgreSQL to return the inserted row's ID.
                )
                result = await datab.execute(
                    notification_stmt
                )
                inserted_id = result.scalar_one_or_none()
                # New event: PostgreSQL inserts the notification and returns its ID.
                # Duplicate event: ON CONFLICT DO NOTHING skips the insert, so no ID is returned; scalar_one_or_none() gives None.

                # =================================================
                # 2. UPDATE EVENT STATUS
                # =================================================
                update_stmt = (
                    update(events)
                    .where(events.event_id == event_id)
                    .values(status="processed")
                )

                result = await datab.execute(update_stmt)

                # If the original event doesn't exist,
                # roll back the notification insert too.
                if result.rowcount == 0:
                    raise EventNotFoundError(event_id)
                    logger.error(
                        f"Event not found in database: {event_id}"
                    )
                    return False
                # ================================================
                # STEP 3: Handle duplicate event
                # ================================================
                if inserted_id is None:
                    logger.info(
                        f"Duplicate event skipped: {event_id}"
                    )

                    # Transaction commits on normal exit.
                    return True
                if entity_id is None:
                    logger.info(
                        f"Event {event_id} has no entity_id; "
                        "skipping aggregation."
                    )
                    return True

                aggregation_key = (f"{user_id}:{event_type}:{entity_id}")
                
                # Serialize aggregation operations for this key.
                await datab.execute(
                    select(
                        func.pg_advisory_xact_lock(
                            func.hashtext(aggregation_key)
                        )
                    )
                )

                logger.info(
                    f"Aggregation key for event {event_id}: "
                    f"{aggregation_key}")
                # Field	Value	Purpose
                # event_id	evt_101	Identifies one event occurrence
                # entity_id	doc_55	Identifies the related document
                # aggregation_key	101:COMMENT_CREATED:doc_55	Identifies the group the event belongs to
                
                # STEP 3: Find an existing open aggregation

                aggregation_stmt = (
                    select(AggregatedNotification)
                    .where(
                        AggregatedNotification.aggregation_key
                        == aggregation_key,

                        AggregatedNotification.read.is_(False),

                        AggregatedNotification.last_event_at
                        > func.now() - text("INTERVAL '1 hour'")
                    )
                    .order_by(
                        AggregatedNotification.last_event_at.desc()
                    )
                    .limit(1)
                    .with_for_update()# asks PostgreSQL to lock the matching row against conflicting updates until the transaction ends. This helps when multiple workers update an existing group. We will still need to handle concurrent creation when no matching row exists.
                )

                result = await datab.execute(aggregation_stmt)

                aggregation = result.scalar_one_or_none()

                # STEP 4: Update existing aggregation or create a new one
                if aggregation is not None:
                    # CASE A: A matching aggregation already exists
                    new_count = aggregation.event_count + 1
                    update_stmt = (
                        update(AggregatedNotification)
                        .where(
                            AggregatedNotification.id == aggregation.id
                        )
                        .values(
                            event_count=AggregatedNotification.event_count + 1,
                            last_event_at=func.now(),
                            message=(
                                f"{new_count} {event_type} events on {entity_id}"
                            )
                        )
                    )

                    await datab.execute(update_stmt)

                    logger.info(
                        f"Updated aggregation {aggregation_key}: "
                        f"event_count={new_count}"
                    )
                else:
                    # CASE B: No matching aggregation exists
                    new_aggregation = pg_insert(
                        AggregatedNotification
                    ).values(
                        id=uuid.uuid4(),
                        user_id=user_id,
                        aggregation_key=aggregation_key,
                        event_count=1,
                        message=f"1 {event_type} event on {entity_id}",
                        read=False,
                        first_event_at=func.now(),
                        last_event_at=func.now()
                    )

                    await datab.execute(new_aggregation)

                    logger.info(
                        f"Created new aggregation: {aggregation_key}"
                    )

                # =================================================
                # STEP 4: New event successfully inserted
                # =================================================
                logger.info(
                    f"Raw notification inserted for event "
                    f"{event_id}"
                )
            # Leaving datab.begin() successfully commits.
            # Exception causes automatic rollback.

        logger.info(
            f"Successfully processed event={event_id}"
        )
        return True
        # Situation	--->>> Expected database result
        # New event, matching event record exists	--->>> Insert notification and mark event processed, then commit
        # Duplicate notification, matching event record exists	--->>> Skip notification insertion and mark event processed, then commit
        # Event record does not exist --->>>	Roll back the transaction, including any new notification insert
        # Database operation raises an exception	--->>>	Roll back the transaction

    # =========================================================
    # PROCESS ONE QUEUED EVENT
    # =========================================================
    async def process_one_event(self):
        """
        Dequeue and process exactly ONE event.
        Returns:
            True  -> event successfully processed
            False -> no event / unknown event / processing failure
        """

        # here event comes from redis EVENT_QUEUE but before that it was pushed into PROCESS_QUEUE for processing and after processing it is removed from PROCESS_QUEUE so that if worker crashes in between then we can recover it from PROCESS_QUEUE and put it back into EVENT_QUEUE for processing again
        result = await self.queue.dequeue_blocking_left(
            timeout=1
        )

        # No event available
        if result is None:
            return False

        # dequeue returns:
        # (raw_json, parsed_dict)
        raw_json, task = result

        event_id = task.get("event_id")

        logger.info(
            f"Dequeued event: {event_id}"
        )

        try:

            # =================================================
            # PROCESS EVENT
            # =================================================

            success = await self.event_process(task)

            # -------------------------------------------------
            # Event was not found in PostgreSQL
            # -------------------------------------------------

            if success is False:

                await self.queue.remove_after_processing(
                    raw_json
                )

                logger.error(
                    f"Removing unknown event "
                    f"{event_id} from processing queue."
                )

                return False

            # -------------------------------------------------
            # SUCCESS
            # -------------------------------------------------

            await self.queue.remove_after_processing(
                raw_json
            )

            logger.info(
                f"Event {event_id} removed from "
                f"processing queue."
            )

            return True

        # =====================================================
        # PROCESSING FAILED
        # =====================================================

        except Exception as e:

            logger.exception(
                f"Error while processing event "
                f"{event_id}: {e}"
            )

            # Increment retry count
            retry_count = (
                task.get("retry_count", 0) + 1
            )

            task["retry_count"] = retry_count

            # =================================================
            # MAX RETRIES REACHED
            # =================================================

            if retry_count > MAX_RETRIES:

                logger.critical(
                    f"Event {event_id} exceeded maximum "
                    f"retries ({MAX_RETRIES}). "
                    f"Moving to dead-letter queue."
                )

                await self.queue.move_to_dead_queue(
                    task
                )

            # =================================================
            # SCHEDULE NEXT RETRY
            # =================================================
            else:

                await self.queue.schedule_retry(
                    task
                )

            # Remove old processing copy only after
            # retry/dead-letter scheduling succeeds.
            await self.queue.remove_after_processing(
                raw_json
            )

            return False

    # =========================================================
    # MAIN WORKER LOOP
    # =========================================================
    async def process_queue_with_blpop(self):
        # -----------------------------------------------------
        # Recover unfinished events from previous worker crash
        # -----------------------------------------------------
        logger.info(
            "Checking for unfinished events..."
        )

        await self.queue.recover_processing_queue()

        logger.info(
            "Worker started. Waiting for events..."
        )

        try:

            while True:

                # ---------------------------------------------
                # 1. Check scheduled retries
                # ---------------------------------------------
                await self.queue.process_scheduled_retries()
                # ---------------------------------------------
                # 2. Process ONE event
                # ---------------------------------------------
                await self.process_one_event()

        except KeyboardInterrupt:

            logger.info(
                "Worker shutdown requested."
            )

        except Exception as e:

            logger.exception(
                f"Worker crashed unexpectedly: {e}"
            )

        finally:

            await self.queue.close()

            logger.info(
                "Worker shut down cleanly."
            )


# =========================================================
# START WORKER
# =========================================================

if __name__ == "__main__":
# above line means
# Run the following code only if this Python file is being executed directly."

# if you run : python worker.py ---> pyhton prints : __main__
# threrefore __name__ == "__main__" is true

# if another fie imports it 
# so inside worker.py print(__name__) would print worker
# threrefore __name__ == "__main__" is false

    worker = Worker()

    asyncio.run(

        worker.process_queue_with_blpop(),
        loop_factory=lambda: asyncio.SelectorEventLoop(
            selectors.SelectSelector()
        )
    )



# ------------------------------
# ASYNCIO UNDERSTANDING
# ------------------------------

# asyncio : asyncio is Python's system for doing asynchronous work.
# Instead of making the program completely stuck while waiting, asynchronous code can let other async work run.

# async def : This function is asynchronous and returns a coroutine when called.
# does not immediately execute the function.It creates a coroutine.

# Coroutine : A coroutine is an asynchronous piece of work that can be run by an event loop.
# To execute it, we need an event loop.

# await : Wait for this async operation to finish, but don't block the whole event loop while waiting.
# You can normally use await only inside an async def.

# Event Loop : An event loop is a loop that runs forever, waiting for async work to be ready, and then running it.
# When one task is waiting for I/O, the event loop can let another task run.

# asyncio.run() : This function creates an event loop, runs the given coroutine until it is done, and then closes the loop.

# asyncio.gather() : This function takes multiple coroutines and runs them concurrently, returning their results as a list.