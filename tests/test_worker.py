import asyncio
import uuid

import pytest , json
from sqlalchemy import select, func
from datetime import datetime, timedelta, timezone

from app.models.events import events
from app.models.notifications import Notify
from app.models.aggregated_notifications import AggregatedNotification
from app.routers.worker import Worker , MAX_RETRIES


# ==================================================
# Layer 1: Database fixture
# ==================================================
@pytest.mark.asyncio
async def test_database_fixture(db_session):

    event_id = f"test-{uuid.uuid4()}"

    new_event = events(
        id=uuid.uuid4(),
        event_id=event_id,
        event_type="COMMENT_CREATED",
        user_id=1,
        source="test",
        payload={"message": "hello"},
        status="pending",
    )

    db_session.add(new_event)
    await db_session.commit()

    result = await db_session.execute(
        select(events).where(
            events.event_id == event_id
        )
    )

    event = result.scalar_one()

    assert event.event_id == event_id
    assert event.status == "pending"


# ==================================================
# Layer 2: Concurrent processing of different events
# ==================================================
@pytest.mark.asyncio
async def test_concurrent_processing(session_factory):

    async def process_event(event_id):

        async with session_factory() as session:

            result = await session.execute(
                select(events).where(
                    events.event_id == event_id
                )
            )

            event = result.scalar_one()

            # Simulate processing work
            await asyncio.sleep(0.1)

            event.status = "processed"

            await session.commit()

    # ---------------------------------------------
    # Create 10 events
    # ---------------------------------------------

    event_ids = []

    async with session_factory() as session:

        for i in range(10):

            event_id = f"concurrent-{uuid.uuid4()}"

            event = events(
                id=uuid.uuid4(),
                event_id=event_id,
                event_type="COMMENT_CREATED",
                user_id=1,
                source="test",
                payload={"number": i},
                status="pending",
            )

            session.add(event)
            event_ids.append(event_id)

        await session.commit()

    # ---------------------------------------------
    # Process all events concurrently
    # ---------------------------------------------

    await asyncio.gather(
        *(process_event(event_id) for event_id in event_ids)
    )

    # ---------------------------------------------
    # Verify all events were processed
    # ---------------------------------------------

    async with session_factory() as session:

        result = await session.execute(
            select(events).where(
                events.event_id.in_(event_ids)
            )
        )

        processed_events = result.scalars().all()

        assert len(processed_events) == 10

        for event in processed_events:
            assert event.status == "processed"


# ==================================================
# Layer 3: Real worker + duplicate event
# ==================================================
@pytest.mark.asyncio
async def test_real_worker_concurrent_duplicate_event(
    session_factory
):

    # ---------------------------------------------
    # Create ONE event
    # ---------------------------------------------

    event_id = f"real-worker-{uuid.uuid4()}"

    event_data = {
        "event_id": event_id,
        "event_type": "COMMENT_CREATED",
        "user_id": 1,
        "source": "test",
        "payload": {
            "message": "hello"
        },
    }

    async with session_factory() as session:

        event = events(
            id=uuid.uuid4(),
            event_id=event_id,
            event_type=event_data["event_type"],
            user_id=event_data["user_id"],
            source=event_data["source"],
            payload=event_data["payload"],
            status="pending",
        )

        session.add(event)

        await session.commit()

    # ---------------------------------------------
    # Create TWO real workers
    # ---------------------------------------------

    worker1 = Worker(
        session_factory=session_factory
    )

    worker2 = Worker(
        session_factory=session_factory
    )

    # ---------------------------------------------
    # Process SAME event concurrently
    # ---------------------------------------------

    result1, result2 = await asyncio.gather(
        worker1.event_process(event_data),
        worker2.event_process(event_data),
    )

    assert result1 is True
    assert result2 is True

    # ---------------------------------------------
    # Verify exactly ONE notification
    # ---------------------------------------------

    async with session_factory() as session:

        result = await session.execute(
            select(func.count(Notify.id))
            .where(
                Notify.event_id == event_id
            )
        )

        notification_count = result.scalar_one()

        assert notification_count == 1

    # ---------------------------------------------
    # Verify event is processed
    # ---------------------------------------------

    async with session_factory() as session:

        result = await session.execute(
            select(events)
            .where(
                events.event_id == event_id
            )
        )

        event = result.scalar_one()

        assert event.status == "processed"

@pytest.mark.asyncio
async def test_real_worker_processes_redis_event(
    session_factory,
    redis_client
):

    # --------------------------------------------------
    # 1. Create event in PostgreSQL
    # --------------------------------------------------

    event_id = f"redis-test-{uuid.uuid4()}"

    event_data = {
        "event_id": event_id,
        "event_type": "COMMENT_CREATED",
        "user_id": 1,
        "source": "redis_test",
        "payload": {
            "message": "hello"
        }
    }

    async with session_factory() as session:

        event = events(
            id=uuid.uuid4(),
            event_id=event_id,
            event_type=event_data["event_type"],
            user_id=event_data["user_id"],
            source=event_data["source"],
            payload=event_data["payload"],
            status="pending"
        )

        session.add(event)

        await session.commit()

    # --------------------------------------------------
    # 2. Put event into Redis
    # --------------------------------------------------

    await redis_client.rpush(
        "queue:events",
        json.dumps(event_data)
    )

    # --------------------------------------------------
    # 3. Create REAL worker using TEST resources
    # --------------------------------------------------

    worker = Worker(
        session_factory=session_factory,
        redis=redis_client
    )

    # --------------------------------------------------
    # 4. Process one event from Redis
    # --------------------------------------------------

    success = await worker.process_one_event()

    assert success is True

    # --------------------------------------------------
    # 5. Verify event is processed
    # --------------------------------------------------

    async with session_factory() as session:

        result = await session.execute(
            select(events)
            .where(
                events.event_id == event_id
            )
        )

        event = result.scalar_one()

        assert event.status == "processed"

    # --------------------------------------------------
    # 6. Verify notification exists
    # --------------------------------------------------

    async with session_factory() as session:

        result = await session.execute(
            select(func.count(Notify.id))
            .where(
                Notify.event_id == event_id
            )
        )

        notification_count = result.scalar_one()

        assert notification_count == 1
# =================================================
# Layer 4: Failed event is scheduled for retry
# =================================================
@pytest.mark.asyncio
async def test_failed_event_is_scheduled_for_retry(
    session_factory,
    redis_client
):

    event_id = f"retry-test-{uuid.uuid4()}"

    event_data = {
        "event_id": event_id,
        "event_type": "COMMENT_CREATED",
        "user_id": 1,
        "source": "retry_test",
        "payload": {
            "message": "hello"
        }
    }

    # --------------------------------------------------
    # 1. Create event in PostgreSQL
    # --------------------------------------------------

    async with session_factory() as session:

        event = events(
            id=uuid.uuid4(),
            event_id=event_id,
            event_type=event_data["event_type"],
            user_id=event_data["user_id"],
            source=event_data["source"],
            payload=event_data["payload"],
            status="pending"
        )

        session.add(event)
        await session.commit()

    # --------------------------------------------------
    # 2. Put event into Redis
    # --------------------------------------------------

    await redis_client.rpush(
        "queue:events",
        json.dumps(event_data)
    )

    # --------------------------------------------------
    # 3. Create worker using TEST resources
    # --------------------------------------------------

    worker = Worker(
        session_factory=session_factory,
        redis=redis_client
    )

    # --------------------------------------------------
    # 4. Force event_process() to fail
    # --------------------------------------------------

    async def failing_process(event_data):
        raise RuntimeError("Simulated database failure")

    worker.event_process = failing_process

    # --------------------------------------------------
    # 5. Process event
    # --------------------------------------------------

    result = await worker.process_one_event()

    assert result is False

    # --------------------------------------------------
    # 6. Check retry queue
    # --------------------------------------------------

    retry_count = await redis_client.zcard(
        "retry:scheduled"
    )

    assert retry_count == 1

    # --------------------------------------------------
    # 7. Verify event still exists in PostgreSQL
    # --------------------------------------------------

    async with session_factory() as session:

        result = await session.execute(
            select(events)
            .where(events.event_id == event_id)
        )

        event = result.scalar_one()

        assert event.event_id == event_id

# ================================================
# Layer 5: Aggregation creation
# ================================================
@pytest.mark.asyncio
async def test_creates_new_aggregation(session_factory):

    # 1. Create a unique event
    event_id = f"aggregation-{uuid.uuid4()}"

    event_data = {
        "event_id": event_id,
        "event_type": "COMMENT_CREATED",
        "user_id": 101,
        "source": "document_service",
        "payload": {
            "entity_id": "doc_55"
        }
    }

    # 2. Insert the original event into the test database
    async with session_factory() as session:

        new_event = events(
            id=uuid.uuid4(),
            event_id=event_id,
            event_type=event_data["event_type"],
            user_id=event_data["user_id"],
            source=event_data["source"],
            payload=event_data["payload"],
            status="pending"
        )

        session.add(new_event)
        await session.commit()

    # 3. Create the worker using the test DB
    worker = Worker(
        session_factory=session_factory
    )

    # 4. Process the event through the real function
    success = await worker.event_process(event_data)

    assert success is True

    # 5. Verify the aggregation was created
    async with session_factory() as session:

        result = await session.execute(
            select(AggregatedNotification).where(
                AggregatedNotification.aggregation_key
                == "101:COMMENT_CREATED:doc_55"
            )
        )

        aggregation = result.scalar_one()

        assert aggregation.user_id == 101
        assert aggregation.event_count == 1
        assert aggregation.read is False

        assert aggregation.message == (
            "1 COMMENT_CREATED event on doc_55"
        )

        assert aggregation.first_event_at is not None
        assert aggregation.last_event_at is not None
# ================================================
# Layer 6: Aggregation update
# ================================================
@pytest.mark.asyncio
async def test_updates_existing_aggregation(session_factory):

    worker = Worker(
        session_factory=session_factory
    )

    # Two DIFFERENT events for the same user and document
    event_data_1 = {
        "event_id": f"aggregation-{uuid.uuid4()}",
        "event_type": "COMMENT_CREATED",
        "user_id": 101,
        "source": "document_service",
        "payload": {
            "entity_id": "doc_55"
        }
    }

    event_data_2 = {
        "event_id": f"aggregation-{uuid.uuid4()}",
        "event_type": "COMMENT_CREATED",
        "user_id": 101,
        "source": "document_service",
        "payload": {
            "entity_id": "doc_55"
        }
    }

    # 1. Insert both original events into PostgreSQL
    async with session_factory() as session:

        for event_data in (event_data_1, event_data_2):
            session.add(
                events(
                    id=uuid.uuid4(),
                    event_id=event_data["event_id"],
                    event_type=event_data["event_type"],
                    user_id=event_data["user_id"],
                    source=event_data["source"],
                    payload=event_data["payload"],
                    status="pending"
                )
            )

        await session.commit()

    # 2. Process the first event — creates aggregation
    result_1 = await worker.event_process(event_data_1)
    assert result_1 is True

    # 3. Process the second event — should update aggregation
    result_2 = await worker.event_process(event_data_2)
    assert result_2 is True

    # 4. Verify there is only one aggregation for this key
    aggregation_key = "101:COMMENT_CREATED:doc_55"

    async with session_factory() as session:

        result = await session.execute(
            select(AggregatedNotification).where(
                AggregatedNotification.aggregation_key
                == aggregation_key
            )
        )

        aggregations = result.scalars().all()

        assert len(aggregations) == 1

        aggregation = aggregations[0]

        # The count should be incremented, not a new row created.
        assert aggregation.event_count == 2

        assert aggregation.message == (
            "2 COMMENT_CREATED events on doc_55"
        )
# ===============================================
# Layer 7: Duplicate event does not increment aggregation
# ==============================================
@pytest.mark.asyncio
async def test_duplicate_event_does_not_increment_aggregation(session_factory):
    worker = Worker(session_factory=session_factory)

    event_data = {
        "event_id": f"duplicate-{uuid.uuid4()}",
        "event_type": "COMMENT_CREATED",
        "user_id": 101,
        "source": "document_service",
        "payload": {"entity_id": "doc_55"}
    }

    # Create the original event in PostgreSQL.
    async with session_factory() as session:
        session.add(
            events(
                id=uuid.uuid4(),
                event_id=event_data["event_id"],
                event_type=event_data["event_type"],
                user_id=event_data["user_id"],
                source=event_data["source"],
                payload=event_data["payload"],
                status="pending"
            )
        )
        await session.commit()

    # Process the exact same event twice.
    assert await worker.event_process(event_data) is True
    assert await worker.event_process(event_data) is True

    # Check the aggregation count.
    aggregation_key = "101:COMMENT_CREATED:doc_55"

    async with session_factory() as session:
        result = await session.execute(
            select(AggregatedNotification).where(
                AggregatedNotification.aggregation_key == aggregation_key
            )
        )

        aggregation = result.scalar_one_or_none()

        assert aggregation is not None
        assert aggregation.event_count == 1

# =============================================================
# Layer 8: Read aggregation creates a new group for unread events
# =============================================================
@pytest.mark.asyncio
async def test_read_aggregation_creates_new_group(session_factory):
    worker = Worker(session_factory=session_factory)

    entity_id = f"doc_{uuid.uuid4()}"
    aggregation_key = f"101:COMMENT_CREATED:{entity_id}"
    now = datetime.now(timezone.utc)

    # Create an aggregation that the user has already read.
    async with session_factory() as session:
        session.add(
            AggregatedNotification(
                id=uuid.uuid4(),
                user_id=101,
                aggregation_key=aggregation_key,
                event_count=3,
                message=f"3 COMMENT_CREATED events on {entity_id}",
                read=True,
                first_event_at=now,
                last_event_at=now,
            )
        )

        session.add(
            events(
                id=uuid.uuid4(),
                event_id=f"new-event-{uuid.uuid4()}",
                event_type="COMMENT_CREATED",
                user_id=101,
                source="document_service",
                payload={"entity_id": entity_id},
                status="pending",
            )
        )
        await session.commit()

    event_data = {
        "event_id": None,
        "event_type": "COMMENT_CREATED",
        "user_id": 101,
        "source": "document_service",
        "payload": {"entity_id": entity_id},
    }

    # Use the same event ID saved in the events table.
    async with session_factory() as session:
        result = await session.execute(
            select(events).where(
                events.payload["entity_id"].as_string() == entity_id
            )
        )
        source_event = result.scalar_one()

    event_data["event_id"] = source_event.event_id

    assert await worker.event_process(event_data) is True

    async with session_factory() as session:
        result = await session.execute(
            select(AggregatedNotification).where(
                AggregatedNotification.aggregation_key == aggregation_key
            )
        )
        aggregations = result.scalars().all()

        assert len(aggregations) == 2

        old_group = next(item for item in aggregations if item.read is True)
        new_group = next(item for item in aggregations if item.read is False)

        assert old_group.event_count == 3
        assert new_group.event_count == 1
        assert new_group.message == (
            f"1 COMMENT_CREATED event on {entity_id}"
        )

# =============================================================
# Layer 9: Read aggregation creates a new group for unread events   
# 
@pytest.mark.asyncio
async def test_expired_aggregation_creates_new_group(session_factory):
    worker = Worker(session_factory=session_factory)

    entity_id = f"doc_{uuid.uuid4()}"
    aggregation_key = f"101:COMMENT_CREATED:{entity_id}"
    old_time = datetime.now(timezone.utc) - timedelta(hours=2)

    event_data = {
        "event_id": f"new-event-{uuid.uuid4()}",
        "event_type": "COMMENT_CREATED",
        "user_id": 101,
        "source": "document_service",
        "payload": {"entity_id": entity_id},
    }

    # Create an unread aggregation that is two hours old.
    async with session_factory() as session:
        session.add(
            AggregatedNotification(
                id=uuid.uuid4(),
                user_id=101,
                aggregation_key=aggregation_key,
                event_count=3,
                message=f"3 COMMENT_CREATED events on {entity_id}",
                read=False,
                first_event_at=old_time,
                last_event_at=old_time,
            )
        )

        session.add(
            events(
                id=uuid.uuid4(),
                event_id=event_data["event_id"],
                event_type=event_data["event_type"],
                user_id=event_data["user_id"],
                source=event_data["source"],
                payload=event_data["payload"],
                status="pending",
            )
        )
        await session.commit()

    # Process a new event for the same aggregation key.
    assert await worker.event_process(event_data) is True

    # Verify that a new aggregation was created.
    async with session_factory() as session:
        result = await session.execute(
            select(AggregatedNotification).where(
                AggregatedNotification.aggregation_key == aggregation_key
            )
        )
        aggregations = result.scalars().all()

        assert len(aggregations) == 2

        old_group = next(
            item for item in aggregations if item.event_count == 3
        )
        new_group = next(
            item for item in aggregations if item.event_count == 1
        )

        assert old_group.last_event_at == old_time
        assert old_group.read is False
        assert new_group.read is False
        assert new_group.message == (
            f"1 COMMENT_CREATED event on {entity_id}"
        )

# =============================================================
# Layer 10: Concurrent events with same aggregation key create one aggregation
# =============================================================
@pytest.mark.asyncio
async def test_concurrent_events_create_one_aggregation(session_factory):
    event_count = 10
    entity_id = f"doc_{uuid.uuid4()}"
    aggregation_key = f"101:COMMENT_CREATED:{entity_id}"

    # Create 10 different events with the same aggregation key.
    event_data_list = [
        {
            "event_id": f"concurrent-{uuid.uuid4()}",
            "event_type": "COMMENT_CREATED",
            "user_id": 101,
            "source": "document_service",
            "payload": {"entity_id": entity_id},
        }
        for _ in range(event_count)
    ]

    # Save all source events before processing begins.
    async with session_factory() as session:
        for event_data in event_data_list:
            session.add(
                events(
                    id=uuid.uuid4(),
                    event_id=event_data["event_id"],
                    event_type=event_data["event_type"],
                    user_id=event_data["user_id"],
                    source=event_data["source"],
                    payload=event_data["payload"],
                    status="pending",
                )
            )
        await session.commit()

    # Simulate separate workers processing events concurrently.
    workers = [
        Worker(session_factory=session_factory)
        for _ in event_data_list
    ]

    results = await asyncio.gather(
        *[
            worker.event_process(event_data)
            for worker, event_data in zip(workers, event_data_list)
        ]
    )

    assert all(result is True for result in results)

    # All 10 events should belong to one aggregation.
    async with session_factory() as session:
        result = await session.execute(
            select(AggregatedNotification).where(
                AggregatedNotification.aggregation_key == aggregation_key
            )
        )
        aggregations = result.scalars().all()

        assert len(aggregations) == 1
        assert aggregations[0].event_count == event_count
        assert aggregations[0].message == (
            f"{event_count} COMMENT_CREATED events on {entity_id}"
        )

# =============================================================
# Layer 11: Missing entity_id in payload skips aggregation
# =============================================================
@pytest.mark.asyncio
async def test_missing_entity_id_skips_aggregation(session_factory):
    worker = Worker(session_factory=session_factory)

    event_data = {
        "event_id": f"no-entity-{uuid.uuid4()}",
        "event_type": "SYSTEM_ALERT",
        "user_id": 987654321,
        "source": "system_service",
        "payload": {},
    }

    # Save the source event first.
    async with session_factory() as session:
        session.add(
            events(
                id=uuid.uuid4(),
                event_id=event_data["event_id"],
                event_type=event_data["event_type"],
                user_id=event_data["user_id"],
                source=event_data["source"],
                payload=event_data["payload"],
                status="pending",
            )
        )
        await session.commit()

    # The event should process successfully.
    assert await worker.event_process(event_data) is True

    async with session_factory() as session:
        # No aggregation should be created for this user.
        result = await session.execute(
            select(AggregatedNotification).where(
                AggregatedNotification.user_id == 987654321
            )
        )
        assert result.scalars().all() == []

        # The source event should still be marked processed.
        result = await session.execute(
            select(events.status).where(
                events.event_id == event_data["event_id"]
            )
        )
        # assert result.scalar_one_or_none() is not None
        assert result.scalar_one() == "processed"
        result = await session.execute(
            select(Notify).where(
                Notify.event_id == event_data["event_id"]
            )
        )
        assert result.scalar_one_or_none() is not None

# ==================================================
# Test: Event exceeding maximum retries goes to DLQ
# ==================================================
@pytest.mark.asyncio
async def test_event_exceeding_max_retries_goes_to_dlq(
    session_factory,
    redis_client
):
    event_id = f"dead-test-{uuid.uuid4()}"

    event_data = {
        "event_id": event_id,
        "event_type": "COMMENT_CREATED",
        "user_id": 1,
        "source": "dead_letter_test",
        "payload": {"message": "test"},
        "retry_count": MAX_RETRIES,
    }

    # Put the event into the main queue.
    await redis_client.rpush(
        "queue:events",
        json.dumps(event_data)
    )

    worker = Worker(
        session_factory=session_factory,
        redis=redis_client
    )

    # Simulate another processing failure.
    async def failing_process(event):
        raise RuntimeError("Simulated permanent failure")

    worker.event_process = failing_process

    # Process the event; it should exceed the retry limit.
    result = await worker.process_one_event()

    assert result is False

    # Verify the event reached the dead-letter queue.
    dead_items = await redis_client.lrange(
        "queue:dead", 0, -1
    )
    dead_events = [json.loads(item) for item in dead_items]

    assert any(
        item["event_id"] == event_id
        for item in dead_events
    )

    # Verify it was removed from the processing queue.
    processing_items = await redis_client.lrange(
        "queue:processing", 0, -1
    )
    assert not any(
        json.loads(item)["event_id"] == event_id
        for item in processing_items
    )

    # Verify it was not scheduled for another retry.
    retry_items = await redis_client.zrange(
        "retry:scheduled", 0, -1
    )
    assert not any(
        json.loads(item)["event_id"] == event_id
        for item in retry_items
    )
