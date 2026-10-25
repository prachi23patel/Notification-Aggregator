# Notification Aggregator

A backend application for receiving events, persisting them in PostgreSQL, processing them asynchronously through Redis, and generating individual and aggregated notifications.

The project demonstrates practical backend engineering concepts including the **outbox pattern, producer–consumer processing, Redis queue recovery, retry scheduling, dead-letter handling, database idempotency safeguards, and concurrency control**.

## Table of Contents

- [Problem Statement](#problem-statement)
- [Key Features](#key-features)
- [Technology Stack](#technology-stack)
- [System Architecture](#system-architecture)
- [Event Processing Workflow](#event-processing-workflow)
- [API Routes and Endpoints](#api-routes-and-endpoints)
- [Failure Handling and Reliability](#failure-handling-and-reliability)
- [Database Design](#database-design)
- [Important Engineering Decisions](#important-engineering-decisions)
- [Testing](#testing)
- [Limitations and Future Improvements](#limitations-and-future-improvements)
- [What This Project Demonstrates](#what-this-project-demonstrates)

## Problem Statement

Processing every event completely inside an API request can make request handling slower and makes recovery harder when a dependent service is temporarily unavailable.

Notification Aggregator separates event acceptance from background processing. It first stores an event and its delivery record in PostgreSQL, then uses an outbox worker and Redis queue to pass the event to a background worker. The worker creates an individual notification and, when the event has a suitable entity ID, adds it to an aggregated notification group.

**Scope:** The current application accepts generic event payloads. Direct integrations that fetch notifications from Gmail, WhatsApp, or other external applications are not implemented in the reviewed code.

## Key Features

- **Transactional event persistence:** Stores the event and its outbox record in one PostgreSQL transaction.
- **Outbox-based delivery:** A separate worker retries delivery of pending outbox records to Redis.
- **Asynchronous event processing:** Uses Redis lists to decouple event ingestion from notification processing.
- **Atomic queue transfer:** Uses Redis `BLMOVE` to transfer events from the main queue to the processing queue.
- **Processing recovery:** Moves entries left in the processing queue back to the main queue when the worker starts.
- **Retry scheduling:** Uses exponential backoff with a Redis sorted set to schedule retries.
- **Dead-letter handling:** Sends malformed payloads and events that exceed the retry allowance to a dead-letter queue.
- **Individual notifications:** Creates a notification for an event, with a unique constraint on `event_id` to help avoid duplicate notification rows.
- **Notification aggregation:** Groups eligible unread events by user, event type, and entity ID.
- **Concurrency control:** Uses PostgreSQL advisory locks and row locks while updating aggregation groups.
- **Structured logging:** Records processing activity, retries, delivery failures, and other operational events.

## Technology Stack

| Technology | Role |
|---|---|
| Python | Application and worker implementation |
| FastAPI | HTTP API |
| Pydantic | Incoming event validation |
| PostgreSQL | Durable event, outbox, and notification storage |
| SQLAlchemy Async ORM | Asynchronous database access and transactions |
| Redis | Event queues, processing list, retry schedule, and dead-letter queue |
| Redis Lua scripts | Atomic multi-command Redis transitions |
| Pytest / pytest-asyncio | Asynchronous worker tests |

## System Architecture

```mermaid
flowchart TD
    A[Client submits event] --> B[FastAPI event endpoint]
    B --> C[(PostgreSQL: Events)]
    B --> D[(PostgreSQL: Outbox events)]
    D --> E[Outbox worker]
    E --> F[(Redis: queue:events)]
    F --> G[Background worker]
    G --> H[(PostgreSQL: Notifications)]
    G --> I[(PostgreSQL: Aggregated notifications)]
    G --> J[(Redis: retry:scheduled)]
    J --> F
    G --> K[(Redis: queue:dead)]
```

**Component responsibilities**

- **FastAPI:** Validates the incoming event and persists the event plus outbox record.
- **PostgreSQL:** Stores the durable application records and their processing/delivery status.
- **Outbox worker:** Attempts to deliver pending outbox payloads to Redis.
- **Redis:** Holds queued, processing, retry-scheduled, and dead-letter events.
- **Background worker:** Creates individual notifications, updates event status, and maintains aggregation groups.

## Event Processing Workflow

1. **Receive an event.** The `POST /events` endpoint validates the JSON request using the `EventRequest` Pydantic model.
2. **Persist the event.** The endpoint inserts a record into `Events` and a corresponding record into `outbox_events` within the same PostgreSQL transaction.
3. **Deliver from the outbox.** The outbox worker selects pending records in batches of up to 20. It uses `FOR UPDATE SKIP LOCKED` to help concurrent outbox workers claim different records.
4. **Enqueue in Redis.** The outbox worker pushes the event payload to `queue:events`. When enqueueing succeeds, it marks the outbox record as delivered.
5. **Move to processing.** The background worker uses `BLMOVE` to move an event from `queue:events` to `queue:processing`.
6. **Create the individual notification.** Inside a PostgreSQL transaction, the worker attempts to insert a notification and updates the original event's status to `processed`.
7. **Aggregate when eligible.** If the individual notification is newly inserted and the event has an entity ID, the worker updates a matching unread aggregation or creates a new group.
8. **Acknowledge or recover.** After successful processing, the worker removes the event from the processing list. Failed events are scheduled for retry or moved to the dead-letter queue. Entries left in the processing list can be recovered when the worker starts.

## API Routes and Endpoints

### `POST /events`

Accepts an event, validates the JSON request using the `EventRequest` model, and stores the event and its outbox record in PostgreSQL. The event is processed asynchronously by a background worker.

**Example request**

```json
{
  "event_id": "evt_101",
  "event_type": "COMMENT_CREATED",
  "user_id": 101,
  "source": "document_service",
  "entity_id": "doc_55",
  "payload": {
    "entity_id": "doc_55",
    "comment": "A new comment was added"
  }
}
```

**Success response — `202 Accepted`**

```json
{
  "notification_id": "generated-uuid",
  "status": "accepted"
}
```

The response field is named `notification_id` in the current code, but the UUID is generated when the event is accepted. It does **not** mean that the background worker has already created the individual notification.

**Request fields**

| Field | Type | Required | Description |
|---|---|---|---|
| `event_id` | String | Yes | Identifier for the logical event; must be unique in the events table. |
| `event_type` | String | Yes | Type of event, such as `COMMENT_CREATED`. |
| `user_id` | Integer | Yes | User associated with the event. |
| `source` | String | Yes | System or service that produced the event. |
| `entity_id` | String or null | No | Related entity; used to build the aggregation key. |
| `payload` | Object or null | No | Additional event data. |

**Error responses**

| HTTP status | Meaning |
|---|---|
| `409 Conflict` | The `event_id` already exists in the events table. |
| `500 Internal Server Error` | An unhandled event-persistence error occurs. |
| `422 Unprocessable Entity` | The request does not satisfy FastAPI/Pydantic validation. |

---

## Failure Handling and Reliability

This project uses multiple mechanisms to reduce event loss, recover interrupted work, and handle processing failures. These mechanisms improve reliability, but they do **not** provide exactly-once delivery across PostgreSQL and Redis.

### 1. Outbox Pattern: Keeping Delivery Intent Durable

When an event is accepted, the application writes both the original event and its outbox record in the same PostgreSQL transaction.

The outbox worker then:

1. Selects pending outbox records in batches.
2. Uses `FOR UPDATE SKIP LOCKED` to help concurrent outbox workers claim different records.
3. Attempts to enqueue each payload into Redis.
4. Marks the outbox record as delivered and sets `delivered_at` after a successful enqueue.
5. Leaves the record pending if Redis enqueueing fails, allowing a later attempt.

This means the event and the intent to deliver it are committed together, even if Redis is temporarily unavailable.

**Important failure window:** Redis may accept an event and the outbox worker may then crash before the PostgreSQL transaction commits. The outbox record can remain pending and be sent again, so duplicate queue delivery is possible.

### 2. Main Queue and Processing Queue

The worker uses these Redis keys:

| Redis key | Purpose |
|---|---|
| `queue:events` | Holds events waiting to be processed. |
| `queue:processing` | Holds events moved out of the main queue while processing is underway. |
| `retry:scheduled` | A sorted set containing failed events and their next eligible retry timestamps. |
| `queue:dead` | Stores malformed events and events that exceed the retry allowance. |

The normal processing sequence is:

1. `BLMOVE` moves an event from `queue:events` to `queue:processing` as one Redis operation.
2. The worker attempts to process the event.
3. After successful processing, the worker removes the event from `queue:processing`.
4. If the worker stops unexpectedly while an event remains in the processing list, startup recovery can return that event to the main queue.

Using `BLMOVE` avoids a separate pop-then-push gap during the main-to-processing transfer.

### 3. Recovering Unfinished Events

When the background worker starts, it calls `recover_processing_queue()`.

That method repeatedly uses `LMOVE` to move entries from `queue:processing` back to `queue:events` until the processing list is empty. This provides a recovery path for entries left behind by a previous worker run.

**Limitation:** This recovery runs at worker startup. It is not a continuous visibility-timeout mechanism that detects a stalled event while the same worker process is still running.

### 4. Retry Handling with Exponential Backoff

When processing raises an exception, the worker increments the event's `retry_count` and schedules the next attempt.

The delay is calculated as:

```python
delay = 2 ** retry_count
```

With the current counter logic, the retry schedule is:

| Retry count | Delay before retry |
|---:|---:|
| 1 | 2 seconds |
| 2 | 4 seconds |
| 3 | 8 seconds |

The worker permits **three retries after the initial attempt**. When the retry count exceeds `MAX_RETRIES`, the event is sent to the dead-letter queue.

Retry timestamps are stored in the Redis sorted set `retry:scheduled`. The worker checks for due retries during its processing loop.

### 5. Lua Script: Moving Scheduled Retries Atomically

When a retry becomes due, the worker uses a Redis Lua script to perform two actions:

1. Remove the event from `retry:scheduled` using `ZREM`.
2. Push the event onto `queue:events` using `RPUSH`.

Both actions occur within one Lua script execution, so another client cannot observe an intermediate state between the removal and push. If the sorted-set entry was already removed, the script does not enqueue it again.

This avoids the specific crash window that would exist if the application performed the removal and push as two separate Redis commands.

### 6. Lua Script: Moving Malformed Messages to the Dead-Letter Queue

If the worker cannot decode JSON taken from the processing queue, it calls `move_raw_to_dead_queue()`.

That Lua script:

1. Removes one matching raw message from `queue:processing` with `LREM`.
2. Pushes the raw message onto `queue:dead` with `RPUSH`, only if the removal succeeded.

This makes the malformed-message transfer atomic within Redis, preventing the message from being left between the two lists due to a crash between separate commands.

### 7. Database Transaction and Idempotency Safeguards

The worker performs the individual notification insert, event status update, and aggregation work inside a PostgreSQL transaction.

- If the event record is not found, the worker raises `EventNotFoundError`; the transaction rolls back.
- If a database operation fails before commit, the related database changes in that transaction roll back together.
- The `Notifications.event_id` field has a unique constraint.
- The worker uses PostgreSQL `ON CONFLICT DO NOTHING` and checks whether the insert returned a notification ID.
- If the notification already exists, the worker skips aggregation for that duplicate processing attempt.

These safeguards make repeated processing safer and help prevent duplicate notification rows or duplicate aggregation increments through this worker path.

They do not create a distributed transaction between PostgreSQL and Redis, and they do not guarantee that every event is processed exactly once.

### 8. Dead-Letter Queue Behavior

If a processing attempt fails beyond the retry limit, the worker sends the event to `queue:dead`. Malformed JSON encountered in the processing list is also routed to the dead-letter queue.

The dead-letter queue provides a place to retain events that could not be processed normally instead of retrying them indefinitely.

**Known limitation:** For a normal failed event, `move_to_dead_queue()` uses `RPUSH` and the worker then removes the processing copy using `LREM`. These are separate Redis commands, not one atomic Lua transfer. If the worker crashes between them, the event can exist in both lists.

The reviewed implementation includes dead-letter storage, but not a complete operational workflow for inspecting, replaying, and managing dead-letter events.

### 9. Reliability Boundaries

| Mechanism | What it helps with | What it does not guarantee |
|---|---|---|
| PostgreSQL outbox | Preserves the intent to deliver an accepted event. | Exactly-once delivery from PostgreSQL to Redis. |
| `BLMOVE` | Atomically moves an event from the main list to the processing list. | Automatic recovery from every possible worker failure. |
| Startup `LMOVE` recovery | Returns leftover processing entries to the main queue. | Continuous recovery of stalled work while a worker remains running. |
| Lua retry transfer | Atomically removes a due retry and enqueues it. | A distributed transaction between Redis and PostgreSQL. |
| Lua malformed-message transfer | Atomically moves a malformed processing entry to the dead-letter list. | Atomic dead-letter transfer for normal processing exceptions. |
| Unique notification event ID | Prevents duplicate individual notification rows for the same event ID. | Exactly-once processing across all systems. |
| PostgreSQL transaction | Commits or rolls back related database changes together. | Rollback of Redis operations already completed. |

The project therefore implements several practical reliability techniques—durable outbox records, atomic Redis list movement, Lua-based transitions, retries, dead-letter handling, database transactions, and idempotency safeguards—while retaining known crash windows that should be documented honestly.

---

## Database Design

The project defines four main SQLAlchemy models.

| Model | Table | Responsibility |
|---|---|---|
| `events` | `Events` | Stores original event data and processing status. |
| `OutboxEvent` | `outbox_events` | Stores the event payload and delivery status. |
| `Notify` | `Notifications` | Stores individual notifications. |
| `AggregatedNotification` | `aggregated_notifications` | Stores grouped notification summaries. |

### Important Fields and Constraints

| Model | Important fields / constraints | Why it matters |
|---|---|---|
| `Events` | `event_id` is unique; stores event type, user ID, source, optional entity ID, payload, status, and creation timestamp. | Identifies a logical event and tracks its processing state. |
| `OutboxEvent` | `event_id` is unique and indexed; includes `status` and `delivered_at`. | Tracks pending and completed delivery attempts. |
| `Notify` | `event_id` is unique; stores user ID, message, read status, and creation timestamp. | Helps enforce at most one individual notification per event ID. |
| `AggregatedNotification` | `aggregation_key` is indexed; stores `event_count`, message, read status, and first/last event timestamps. | Supports looking up and updating a grouped notification. |
| All four models | UUID primary keys identify database rows. | Gives each stored row its own identifier. |

**Relationship note:** The models do not declare explicit foreign-key relationships between these four tables. Their event IDs connect records conceptually, but that is not the same as a database-enforced foreign key.

### Aggregation Logic

The aggregation key is constructed from the user ID, event type, and entity ID:

```text
{user_id}:{event_type}:{entity_id}
```

**Example**

```text
101:COMMENT_CREATED:doc_55
```

The worker looks for an aggregation with the same key that is unread and whose `last_event_at` is within the previous hour.

| Condition | Worker behavior |
|---|---|
| A matching unread group exists within the one-hour window. | Increments `event_count` and updates the message and `last_event_at`. |
| No eligible group exists. | Creates a new group with `event_count = 1`. |
| The event has no entity ID in either the top-level field or payload. | Skips aggregation for that event. |
| The individual notification already exists. | Skips aggregation for that duplicate processing attempt. |

The one-hour condition is a **sliding window** based on the latest event time, not a fixed hourly batch.

### Concurrent Aggregation Protection

Before looking up the group, the worker obtains a PostgreSQL transaction-level advisory lock derived from the aggregation key. When it finds an existing group, it also uses `FOR UPDATE` to lock that row while the transaction is active.

These locks coordinate concurrent aggregation work through this worker path. However, `aggregation_key` is indexed rather than unique, so the database does not independently enforce one row per aggregation key.

---

## Important Engineering Decisions

| Decision | Why it is used |
|---|---|
| **PostgreSQL for durable records** | Stores events, outbox state, individual notifications, and aggregated notifications. |
| **Redis for asynchronous work** | Separates queue operations and retry scheduling from the request-handling path. |
| **Outbox pattern** | Commits the event and its delivery intent together before Redis delivery is attempted. |
| **Separate workers** | Keeps outbox delivery and event processing outside the event-acceptance operation. |
| **`BLMOVE` and `LMOVE`** | Atomically move entries between the main and processing lists and support startup recovery. |
| **Lua scripts** | Make the scheduled-retry transfer and malformed-item dead-letter transfer atomic within Redis. |
| **PostgreSQL transactions** | Keep the notification insert, event status update, and aggregation changes in one database transaction. |
| **Unique constraints and conflict-aware inserts** | Help prevent duplicate event and individual-notification records. |
| **Advisory locks and row locks** | Coordinate concurrent aggregation updates. |
| **Explicit reliability boundaries** | Acknowledge that recovery and idempotency safeguards reduce failure risks but do not guarantee exactly-once processing across PostgreSQL and Redis. |

## Testing

The repository includes asynchronous worker tests in `tests/test_worker.py` and test fixtures in `tests/conftest.py`.

The test scenarios include:

- Processing a Redis event and checking the resulting database state.
- Concurrent processing and duplicate-event handling.
- Scheduling a failed event for retry.
- Creating a new aggregation.
- Updating an existing aggregation and incrementing its event count.
- Creating a new group when an earlier aggregation is read or outside the one-hour window.
- Skipping aggregation when an entity ID is missing.
- Moving an event to the dead-letter queue after the retry limit is exceeded.

The repository contains these test cases, but their presence alone does not establish that the entire suite passes in every environment.

## Limitations and Future Improvements

### Current Limitations

- Direct integrations with external notification providers are not implemented; the system currently accepts generic event payloads.
- The normal failed-event dead-letter transfer is not atomic with processing-list removal.
- PostgreSQL and Redis do not share a distributed transaction.
- Aggregation uniqueness is coordinated by the worker but is not enforced by a unique database constraint.
- The aggregation window and grouping fields are fixed in the current implementation.
- A dead-letter queue exists, but a complete dead-letter inspection and replay workflow is not implemented in the reviewed code.

### Possible Improvements

- Make normal dead-letter transfer and processing-list acknowledgement atomic.
- Add metrics for queue depth, retry counts, processing latency, and dead-letter volume.
- Build a controlled dead-letter inspection and replay workflow.
- Add explicit foreign-key relationships where appropriate and evaluate database-enforced aggregation uniqueness.
- Add external notification-source adapters when the relevant APIs and permissions are available.
- Make aggregation rules configurable.
- Expand tests for worker crashes and PostgreSQL/Redis failure scenarios.

## What This Project Demonstrates

Notification Aggregator demonstrates backend concepts beyond basic CRUD operations: durable event persistence, an outbox delivery workflow, Redis-based producer–consumer processing, retry scheduling, dead-letter handling, idempotency safeguards, transactional database updates, and concurrent notification aggregation.

The design focuses on making event processing easier to recover and reason about while clearly documenting the remaining reliability limitations.
