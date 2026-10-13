import json
import time
from typing import Any, Dict
from app.services.logger import setup_logger

EVENT_QUEUE = "queue:events"
# A Redis List containing events waiting to be processed.
PROCESS_QUEUE = "queue:processing"
# A Redis List containing events that have been taken by a worker but not yet confirmed complete.
RETRY_QUEUE = "retry:scheduled"
# A Redis Sorted Set storing failed events alongside the timestamps when they're eligible for retry.
DEAD_QUEUE = "queue:dead"
# A Redis List holding events that have exhausted their retry allowance.


MAX_RETRIES = 3
logger = setup_logger("api.events")


# workings of this class
"""
enqueue : Add events to a queue.
dequeue_blocking_left : Take events out of the queue.
Keep track of events currently being processed.
recover_processing_queue : Recover unfinished events.
schedule_retry : Schedule retries.
process_scheduled_retries : Process events that are due for retry.
move_to_dead_letter_queue : Move permanently failing events to a dead-letter queue.
remove_completed_events : Remove completed events.
"""

class RedisQueue:
    def __init__(self, redis_client_instance):
        self.redis = redis_client_instance # saves the client in the object so every method can use it.

    async def enqueue(self, payload: Dict[str, Any]):
            """Pushes event onto the RIGHT side of the Redis list to work with BLPOP."""
            # serialized_data = json.dumps(payload)
            # await self.redis.rpush(QUEUE_NAME, serialized_data)
            """Attempts to push a task to Redis. 
            If Redis is down, it logs the error and returns False instead of crashing.
            Converts the dictionary into JSON text.
            Appends the JSON text to the right side of queue:events.
            """
            try:
                serialized_data = json.dumps(payload)
                await self.redis.rpush(EVENT_QUEUE, serialized_data)
                logger.info(f"✓ Event {payload.get('event_id')} successfully buffered into Redis queue.")
                return True
            except Exception as e:
                # 🟢 SWALLOW ERROR & LOG IT: Prevents the API request thread from breaking
                logger.critical(
                    f"⚠️ REDIS QUEUE DOWN! Could not enqueue event {payload.get('event_id')}. "
                    f"Data will remain safe in PostgreSQL. Error details: {e}"
                )
                return False

    async def dequeue_blocking_left(self, timeout: int = 2) -> Dict[str, Any] | None:
            """Blocks and extracts an item from the LEFT side of the list.
            If empty for 'timeout' seconds, returns None and exits cleanly.
            """
            """
            1.Uses BLPOP to wait for and remove the leftmost event from queue:events.
            2.Moves the raw JSON to queue:processing.
            3.Parses the JSON into a Python dictionary.
            4.Returns both forms.
            """

            # blpop returns a tuple: (queue_name, item_data)
            # When no event arrives before the timeout, it returns: None
            result = await self.redis.blpop(EVENT_QUEUE, timeout=timeout)
            if result:
                _, item_data = result
                try:
                    await self.redis.rpush(PROCESS_QUEUE, item_data)
                    logger.info(f"✓ Event successfully buffered into process queue.")
                    task = json.loads(item_data)
                    return item_data, task
    
                except Exception as e:
                    # 🟢 SWALLOW ERROR & LOG IT: Prevents the API request thread from breaking
                    logger.critical(
                        f"⚠️ REDIS QUEUE DOWN! Could not enqueue event into process queue. "
                        f"Data will remain safe in PostgreSQL. Error details: {e}"
                    )
                    return None
            
    async def recover_processing_queue(self):
        """Recovers events from the processing queue back to the main event queue.
        This is useful if a worker crashes while processing an event, ensuring that the event isn't lost"""
        while True:
            try:
                result = await self.redis.lpop(PROCESS_QUEUE)

                if result is None:
                    break

                await self.redis.rpush(EVENT_QUEUE, result)

                logger.info(
                    "Recovered event from processing queue."
                )

            except Exception as e:
                logger.error(
                    f"Could not recover event: {e}"
                )
                break

    async def schedule_retry(self, event_data: dict):

        retry_count = event_data.get("retry_count", 0)

        delay = 2 ** retry_count

        retry_at = time.time() + delay

        raw_json = json.dumps(event_data)

        await self.redis.zadd(
            RETRY_QUEUE,
            {
                raw_json: retry_at
            }
        )

        logger.warning(
            f"Event {event_data['event_id']} scheduled for "
            f"retry #{retry_count} in {delay} seconds."
        )

    async def process_scheduled_retries(self):
        """Adds the event back to queue:events.
            Removes it from retry:scheduled."""
        current_time = time.time()

        # Return every member whose score is between 0 and the current timestamp.
        # Nothing has been removed from Redis yet. This operation only retrieves the due items.
        items = await self.redis.zrangebyscore(
            RETRY_QUEUE,
            0,
            current_time
        )

        for raw_json in items:

            # ZREM removes that particular member from the Sorted Set.
            # Why remove it? Because this event is about to leave the scheduled-retry stage and return to the normal queue.
            # returns : 1 — the member was removed.
            # returns : 0 — the member was not present, perhaps because another worker already removed it.


            # -----------
            # ADD it to the project 
            # -----------
            # Worker crashes after ZREM but before RPUSH
            # Redis Lua script that removes the due member and pushes it to queue:events as one operation. That removes the gap between the two commands.
            removed = await self.redis.zrem(
                RETRY_QUEUE,
                raw_json
            )

            # Worker 1                       Worker 2
            #   ├── Finds event A               ├── Finds event A
            #   ├── ZREM event A → 1            ├── ZREM event A → 0
            #   ├── Continue                    └── Skip
            #   └── Requeue event A
            if removed == 0:
                continue

            # Put back into normal queue
            await self.redis.rpush(
                EVENT_QUEUE,
                raw_json
            )

            try:
                # Decode the JSON for logging
                event_data = json.loads(raw_json)

                logger.info(
                    f"Retry is ready. Requeued event "
                    f"{event_data['event_id']}."
                )

            # Handle malformed JSON
            except json.JSONDecodeError:
                logger.error(
                    "Could not decode scheduled retry event."
                )
    async def move_to_dead_queue(self, event_data: dict):
        """Moves an event to the dead-letter queue after exceeding retry attempts."""
        raw_json = json.dumps(event_data)

        await self.redis.rpush(
            DEAD_QUEUE,
            raw_json
        )

        logger.critical(
            f"Event {event_data['event_id']} "
            f"moved to {DEAD_QUEUE}."
        )

    async def remove_after_processing(self , row_json : str ):
            await self.redis.lrem(PROCESS_QUEUE, 1, row_json)
            return None

    async def close(self):
        """frees resources and helps avoid leaving connections open after the worker exits.  """
        await self.redis.close()
