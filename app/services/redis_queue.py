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

    async def dequeue_blocking_left(self, timeout: int = 2) -> tuple[str, Dict[str, Any]] | None:
            """Blocks and extracts an item from the LEFT side of the list."""
            """
            Atomically move an event from the main queue
            to the processing queue, then decode it.
            """

            # Atomically move the oldest event:
            # queue:events -> queue:processing
            # The BLMOVE command blocks for a specified timeout if the source list is empty, waiting for an item to arrive. If an item is available, it moves it to the destination list and returns it. If the timeout expires without an item arriving, it returns None.
            item_data = await self.redis.blmove(
                EVENT_QUEUE,
                PROCESS_QUEUE,
                timeout=timeout,
                src="LEFT",
                dest="RIGHT"
            )

            # No event arrived before the timeout.
            if item_data is None:
                return None

            # Parse the JSON after the atomic move.
            try:
                task = json.loads(item_data)

            except json.JSONDecodeError:
                logger.exception(
                    "Invalid JSON found in processing queue."
                )
                await self.move_raw_to_dead_queue(item_data)
                return None
            logger.info(
                "Event atomically moved to processing queue."
            )

            # Keep the raw JSON for later removal.
            # Return the parsed dictionary for processing.
            return item_data, task

            
    async def recover_processing_queue(self):
        """
        Atomically move all events left in the processing
        queue back to the main queue during worker startup.
        """
        recovered = 0

        while True:
            # LMOVE moves an item immediately,
            # LMOVE, which atomically moves an item from one Redis list to another without blocking.
            item_data = await self.redis.lmove(
                PROCESS_QUEUE,#PROCESS_QUEUE is the source.
                EVENT_QUEUE,#EVENT_QUEUE is the destination.
                "LEFT",#LEFT selects the item from the left side of the processing list.
                "RIGHT"#RIGHT places it at the right side of the main queue.
            )

            if item_data is None:
                break

            recovered += 1

        if recovered:
            logger.warning(
                f"Recovered {recovered} event(s) "
                "from the processing queue."
            )

        return recovered


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
        '''Redis executes the Lua script as one atomic operation:
        - If the event is still scheduled, it removes it and pushes it into the main queue.
        - If another worker already removed it, the script does nothing.
        - If the worker crashes between the removal and insertion steps, that gap no longer exists.
        '''
        """Atomically move due retries back to the main queue."""

        current_time = time.time()

        items = await self.redis.zrangebyscore(
            RETRY_QUEUE,
            0,
            current_time
        )

        # Redis executes this script atomically.
        script = """
        local removed = redis.call('ZREM', KEYS[1], ARGV[1])

        if removed == 1 then
            redis.call('RPUSH', KEYS[2], ARGV[1])
            return 1
        end

        return 0
        """

        for raw_json in items:
            moved = await self.redis.eval(
                script,
                2,
                RETRY_QUEUE,
                EVENT_QUEUE,
                raw_json
            )

            if moved == 0:
                continue

            try:
                event_data = json.loads(raw_json)

                logger.info(
                    f"Retry is ready. Requeued event "
                    f"{event_data['event_id']}."
                )

            except json.JSONDecodeError:
                logger.error(
                    "Could not decode scheduled retry event."
                )
    async def move_raw_to_dead_queue(self, raw_json: str):
        """Atomically move malformed JSON to the dead-letter queue."""

        script = """
        local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])

        if removed == 1 then
            redis.call('RPUSH', KEYS[2], ARGV[1])
            return 1
        end

        return 0
        """

        moved = await self.redis.eval(
            script,
            2,
            PROCESS_QUEUE,
            DEAD_QUEUE,
            raw_json
        )

        if moved:
            logger.error(
                "Malformed event moved from processing "
                "queue to dead-letter queue."
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
