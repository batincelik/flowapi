import asyncio
from datetime import UTC, datetime

from sqlalchemy import select

from .config import get_settings
from .database import SessionFactory
from .models import ScheduleOccurrence, ScheduleTrigger, Workflow
from .service import create_execution_in_transaction, next_cron_occurrence


async def enqueue_due_schedules(batch_size: int = 50, now: datetime | None = None) -> int:
    """Claim due schedules and durably create each occurrence exactly once."""
    current = now or datetime.now(UTC)
    async with SessionFactory() as db, db.begin():
        schedules = (
            await db.scalars(
                select(ScheduleTrigger)
                .where(ScheduleTrigger.enabled.is_(True), ScheduleTrigger.next_run_at <= current)
                .order_by(ScheduleTrigger.next_run_at)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
        ).all()
        count = 0
        for schedule in schedules:
            occurrence_time = schedule.next_run_at
            workflow = await db.scalar(
                select(Workflow).where(Workflow.id == schedule.workflow_id).with_for_update(key_share=True)
            )
            if workflow is None or workflow.active_version_id is None:
                schedule.enabled = False
                continue
            execution = await create_execution_in_transaction(
                db,
                workflow,
                "schedule",
                {"schedule_id": str(schedule.id), "occurrence_time": occurrence_time.isoformat()},
            )
            db.add(
                ScheduleOccurrence(
                    schedule_id=schedule.id,
                    occurrence_time=occurrence_time,
                    execution_id=execution.id,
                )
            )
            schedule.last_run_at = occurrence_time
            schedule.next_run_at = next_cron_occurrence(schedule.cron, schedule.timezone, occurrence_time)
            count += 1
        return count


async def run() -> None:
    settings = get_settings()
    while True:
        try:
            await enqueue_due_schedules()
        except Exception:  # pragma: no cover - process supervisor and structured logging handle retry
            await asyncio.sleep(settings.SCHEDULER_POLL_INTERVAL)
        else:
            await asyncio.sleep(settings.SCHEDULER_POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(run())
