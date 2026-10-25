# Notification Aggregator

A backend project that receives application events, stores them durably,
queues them for asynchronous processing, and creates individual and
grouped notifications. It uses **FastAPI, PostgreSQL, Redis, and
SQLAlchemy's async ORM** to separate event ingestion from background
processing.

The project focuses on backend engineering concepts such as the **outbox
pattern, producer--consumer processing, retry scheduling, dead-letter
handling, idempotency safeguards, and concurrent aggregation**.

## Overview

Applications often generate many events, and processing each event
directly inside the request can make an API slower and harder to recover
when a dependency fails.

Notification Aggregator separates the work into stages:

-   **Persist:** Save the incoming event and an outbox record in the
    same PostgreSQL transaction.
-   **Deliver:** An outbox worker transfers pending events to a Redis
    queue.
-   **Process:** A separate worker consumes queued events and writes
    notification data to PostgreSQL.
-   **Aggregate:** Related unread events can be grouped into a summary
    notification.
-   **Recover:** Failed work can be retried, recovered from the
    processing queue, or moved to a dead-letter queue.

> **Scope note:** The current project accepts generic event payloads. It
> does not itself contain integrations that fetch notifications directly
> from Gmail, WhatsApp, or other external applications.

## Architecture

``` mermaid
flowchart TD
    A[Incoming event] --> B[(PostgreSQL: Events)]
    A --> C[(PostgreSQL: Outbox)]
    C --> D[Outbox worker]
    D --> E[(Redis: queue:events)]
    E --> F[Background worker]
    F --> G[(PostgreSQL: Notifications)]
    F --> H[(PostgreSQL: Aggregated notifications)]
    F --> I[(Redis: retry:scheduled)]
    I --> E
    F --> J[(Redis: queue:dead)]
```

PostgreSQL is the durable store for event and notification records.
Redis is used for queueing, processing-state tracking, scheduled
retries, and dead-letter storage.

## Core Features

### 1. Transactional event persistence and outbox

When an event is accepted, the application creates an `Events` record
and an `outbox_events` record in the same PostgreSQL transaction. This
records both the event and the intent to deliver it to Redis.

The outbox worker:

-   Selects pending records in batches of up to 20.
-   Uses `FOR UPDATE SKIP LOCKED` to help multiple workers claim
    different records.
-   Attempts to enqueue each payload in Redis.
-   Marks a record as `delivered` and sets `delivered_at` after a
    successful enqueue.
-   Leaves a record pending when enqueueing fails so it can be attempted
    again.

This reduces the chance of losing the delivery intent if Redis is
unavailable. It does not guarantee exactly-once delivery between
PostgreSQL and Redis; a crash after enqueueing but before the database
transaction commits can result in a duplicate queue entry.

### 2. Redis producer--consumer queue

The Redis queue uses these keys:

  -----------------------------------------------------------------------
  Redis key                           Purpose
  ----------------------------------- -----------------------------------
  `queue:events`                      Events waiting to be processed

  `queue:processing`                  Events currently being handled by a
                                      worker

  `retry:scheduled`                   Sorted set of failed events with
                                      their next eligible retry time

  `queue:dead`                        Events that cannot be processed
                                      further, including malformed
                                      payloads
  -----------------------------------------------------------------------

Events are serialized as JSON before being pushed to Redis.

### 3. Atomic queue transfer and recovery

The worker uses Redis `BLMOVE` to move an event from `queue:events` to
`queue:processing` as one Redis operation. This avoids a separate
pop-then-push gap during normal dequeueing.

When the worker starts, it checks `queue:processing` and moves
unfinished entries back to the main queue using `LMOVE`. This provides a
recovery path for events left behind after a worker interruption.

Successfully processed events are removed from the processing list.

### 4. Retry scheduling and dead-letter handling

When event processing raises an exception, the worker increments the
event's `retry_count` and schedules a retry. Retry times use exponential
delays calculated as:

``` text
delay_seconds = 2 ** retry_count
```

With the current counter logic, the scheduled delays are 2, 4, and 8
seconds for retry counts 1, 2, and 3. The worker allows up to **three
retries after the initial attempt**; a further failure is moved to
`queue:dead`.

Due retries are stored in a Redis sorted set and checked by the worker
loop. A Lua script atomically removes a due event from `retry:scheduled`
and pushes it to `queue:events`, avoiding a crash gap between those two
Redis operations.

Malformed JSON found in the processing list is moved to the dead-letter
list using a Lua script that removes the malformed item and pushes it to
the dead-letter list in one Redis-side operation.

**Reliability limitation:** Moving an ordinary failed event to
`queue:dead` and removing its processing copy are currently separate
Redis commands. A crash between those commands can leave a copy in both
lists. The dead-letter path is therefore useful failure handling, but
not a fully atomic transfer in every case.

### 5. Individual notifications and idempotency safeguards

The worker creates an individual notification with a message similar to:

``` text
Event COMMENT_CREATED received from document_service
```

The `Notifications.event_id` field is unique. The worker uses PostgreSQL
`INSERT ... ON CONFLICT DO NOTHING` and checks whether an ID was
returned from the insert. If the notification already exists, it skips
aggregation for that duplicate.

This helps prevent retries from creating duplicate individual
notifications or incrementing an aggregation twice through this worker
path.

### 6. Event aggregation

Events can be grouped when they share the same user, event type, and
entity.

The aggregation key is constructed as:

``` text
{user_id}:{event_type}:{entity_id}
```

For example:

``` text
101:COMMENT_CREATED:doc_55
```

The worker looks for an aggregation that has the same key, is unread,
and has received an event within the previous hour.

-   If a matching aggregation exists, its `event_count`,
    `last_event_at`, and summary message are updated.
-   Otherwise, a new aggregation is created with an initial count of
    one.
-   If an event has no entity ID in either its top-level field or
    payload, the worker skips aggregation for it.

The one-hour condition is a **sliding window based on the latest event
time**, not a fixed hourly batch.

### 7. Concurrent aggregation protection

The worker obtains a PostgreSQL transaction-level advisory lock based on
the hash of the aggregation key before searching for a matching group.
It also uses `FOR UPDATE` when selecting an existing aggregation row.

These mechanisms coordinate concurrent updates and reduce the risk of
multiple workers creating or modifying the same group at the same time
through this code path.

The `aggregation_key` has an index but is not declared unique in the
model. The advisory lock is application-level coordination, not a
database-enforced uniqueness constraint.

### 8. Structured logging

The shared logger writes timestamped messages with the log level and
component name to both the console and `app.log`. The worker and outbox
worker log processing activity, failures, retries, and delivery results.

## Data Model

  ----------------------------------------------------------------------------
  Model / table                Responsibility          Important fields
  ---------------------------- ----------------------- -----------------------
  `events` / `Events`          Original event and      `event_id`,
                               processing state        `event_type`,
                                                       `user_id`, `source`,
                                                       `entity_id`, `payload`,
                                                       `status`, `created_at`

  `OutboxEvent` /              Pending delivery intent `event_id`, `payload`,
  `outbox_events`              and delivery state      `status`, `created_at`,
                                                       `delivered_at`

  `Notify` / `Notifications`   Individual notification `event_id`, `user_id`,
                               per event               `message`, `read`,
                                                       `created_at`

  `AggregatedNotification` /   Grouped summary of      `aggregation_key`,
  `aggregated_notifications`   related events          `event_count`,
                                                       `message`, `read`,
                                                       `first_event_at`,
                                                       `last_event_at`
  ----------------------------------------------------------------------------

UUID primary keys identify database rows. Unique constraints on event
IDs in the event, outbox, and individual notification models help
prevent duplicate records. The models do not declare explicit
foreign-key relationships between these tables.

## Engineering Decisions

-   **PostgreSQL for durable state:** Events, delivery status,
    individual notifications, and aggregated notifications remain in a
    relational database.
-   **Redis for queue operations:** Queueing and retry scheduling are
    separated from durable application records.
-   **Outbox pattern:** The event and its delivery intent are committed
    together before Redis delivery is attempted.
-   **Separate workers:** Outbox delivery and event processing happen
    outside the event-ingestion operation.
-   **Redis list movement:** `BLMOVE` and `LMOVE` reduce the risk of
    losing an event between the main and processing lists during
    transfer.
-   **Lua for multi-command Redis transitions:** Due retries and
    malformed processing entries use scripts to perform their
    remove-and-push transitions atomically.
-   **Database transactions:** Notification creation, event status
    updates, and aggregation work are grouped in a PostgreSQL
    transaction.
-   **Idempotency safeguards:** Unique constraints and conflict-aware
    inserts help make repeated processing safer.
-   **Concurrency control:** Advisory locks and row-level locks
    coordinate aggregation work.

## Testing

The repository includes asynchronous tests in `tests/test_worker.py` and
fixtures in `tests/conftest.py`. The test scenarios cover areas
including:

-   Processing an event from Redis and verifying its database status and
    notification.
-   Scheduling a failed event for retry.
-   Creating a new aggregation.
-   Updating an existing aggregation and incrementing its event count.
-   Duplicate-event behavior and Redis queue operations.

The tests use configurable test database and Redis URLs. The existence
of these tests does not by itself establish that the entire suite passes
in every environment; run the suite in a configured test environment to
confirm the current result.

## Known Limitations

-   The project processes generic events; direct integrations with
    external notification providers are not implemented in the current
    code.
-   PostgreSQL and Redis do not share a distributed transaction.
    Duplicate delivery remains possible, so the system relies on
    database safeguards to make repeated processing safer.
-   Dead-lettering a normal failed event and removing its
    processing-list entry are separate operations, leaving a crash
    window.
-   The aggregation key is indexed but not unique at the database level.
-   The current aggregation logic uses a one-hour sliding window and
    unread status; it is not a configurable time-window or
    user-preference engine.
-   A dead-letter queue is implemented, but a complete dead-letter
    inspection, replay, and operational management workflow is not
    present in the reviewed code.

## Future Improvements

-   Make failed-event dead-letter transfer and processing-list
    acknowledgement atomic.
-   Add monitoring and metrics for queue depth, retry counts, processing
    latency, and dead-letter volume.
-   Add a controlled dead-letter inspection and replay workflow.
-   Add explicit foreign-key relationships where appropriate and
    consider database-enforced aggregation uniqueness.
-   Add integration adapters for external notification sources when
    their APIs and permissions are available.
-   Add configurable aggregation rules and user preferences.
-   Expand automated tests for worker crashes, concurrent aggregation
    creation, and PostgreSQL/Redis failure scenarios.

## What This Project Demonstrates

Notification Aggregator is a backend engineering project focused on
asynchronous event processing and practical failure handling. It brings
together relational persistence, Redis queues, an outbox delivery
workflow, retry scheduling, dead-letter handling, idempotency
safeguards, and concurrent notification aggregation.

The design aims to make event processing easier to recover and reason
about while documenting the remaining failure windows rather than
claiming exactly-once delivery.
