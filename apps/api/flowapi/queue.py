import json
import uuid
from datetime import UTC, datetime, timedelta

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import (
    Execution,
    ExecutionStatus,
    NodeExecution,
    NodeExecutionAttempt,
    NodeStatus,
    OutboxEvent,
    Worker,
)

QUEUE_NAME = "flowapi:jobs"


async def dispatch_batch(db: AsyncSession, redis: Redis, batch_size: int = 100) -> int:
    """Publish claimed outbox rows safely across dispatchers.

    A crash after Redis publish but before processed_at produces a duplicate job. Consumers
    claim database state conditionally, making this deliberate at-least-once delivery safe.
    """
    processed = 0
    async with db.begin():
        events = (
            await db.scalars(
                select(OutboxEvent)
                .where(
                    OutboxEvent.processed_at.is_(None),
                    OutboxEvent.available_at <= datetime.now(UTC),
                )
                .order_by(OutboxEvent.created_at)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
        ).all()
        for event in events:
            try:
                await redis.rpush(
                    QUEUE_NAME,
                    json.dumps({"outbox_id": str(event.id), "type": event.event_type, **event.payload}),
                )
            except Exception as exc:
                event.attempts += 1
                event.last_error = str(exc)[:2000]
                event.available_at = datetime.now(UTC) + timedelta(seconds=min(300, 2**event.attempts))
                continue
            event.processed_at = datetime.now(UTC)
            event.attempts += 1
            processed += 1
    return processed


async def recover_stale_nodes(db: AsyncSession, stale_worker_seconds: int = 60) -> int:
    """Return leased work from dead workers to the durable ready queue."""
    now = datetime.now(UTC)
    stale_before = now - timedelta(seconds=stale_worker_seconds)
    recovered = 0
    async with db.begin():
        rows = (
            await db.scalars(
                select(NodeExecution)
                .where(
                    NodeExecution.status == NodeStatus.RUNNING,
                    NodeExecution.lease_expires_at < now,
                )
                .with_for_update(skip_locked=True)
            )
        ).all()
        for row in rows:
            worker = await db.get(Worker, row.worker_id) if row.worker_id else None
            if worker is not None and worker.last_seen_at >= stale_before:
                continue
            attempt = await db.scalar(
                select(NodeExecutionAttempt)
                .where(
                    NodeExecutionAttempt.node_execution_id == row.id,
                    NodeExecutionAttempt.status == "running",
                )
                .order_by(NodeExecutionAttempt.attempt_number.desc())
                .limit(1)
                .with_for_update()
            )
            if attempt is None:
                # State is inconsistent, so fail closed instead of silently inventing history.
                row.status = NodeStatus.FAILED
                execution = await db.get(Execution, row.execution_id, with_for_update=True)
                if execution:
                    execution.status = ExecutionStatus.FAILED
                    execution.error_code = "ATTEMPT_STATE_CORRUPT"
                    execution.error_message = "Running node has no running attempt"
                    execution.finished_at = now
                continue
            attempt.status = "failed"
            attempt.error_code = "WORKER_LOST"
            attempt.error_message = "Worker lease expired before the attempt completed"
            attempt.finished_at = now
            max_attempts = min(max(int(row.configuration_snapshot.get("max_attempts", 1)), 1), 10)
            if attempt.attempt_number >= max_attempts:
                row.status = NodeStatus.FAILED
                row.worker_id = None
                row.lease_expires_at = None
                execution = await db.get(Execution, row.execution_id, with_for_update=True)
                if execution:
                    execution.status = ExecutionStatus.FAILED
                    execution.error_code = "WORKER_LOST"
                    execution.error_message = "Worker disappeared and retry policy was exhausted"
                    execution.finished_at = now
                continue
            row.status = NodeStatus.READY
            row.worker_id = None
            row.lease_expires_at = None
            db.add(
                OutboxEvent(
                    event_type="node.ready",
                    payload={
                        "execution_id": str(row.execution_id),
                        "node_execution_id": str(row.id),
                        "recovered": True,
                    },
                )
            )
            recovered += 1
    return recovered


async def heartbeat(db: AsyncSession, worker_id: uuid.UUID, active_jobs: int) -> None:
    async with db.begin():
        worker = await db.get(Worker, worker_id, with_for_update=True)
        if worker is None:
            raise LookupError("worker not registered")
        worker.last_seen_at = datetime.now(UTC)
        worker.active_jobs = active_jobs
        worker.status = "online"
