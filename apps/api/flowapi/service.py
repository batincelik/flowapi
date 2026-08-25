import secrets
import uuid
from datetime import UTC, datetime
from typing import Any, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter  # type: ignore[import-untyped]
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .graph import Graph, Problem, validate_graph
from .models import (
    Credential,
    Execution,
    ExecutionStatus,
    NodeExecution,
    NodeStatus,
    OutboxEvent,
    ScheduleTrigger,
    WebhookTrigger,
    Workflow,
    WorkflowVersion,
)


class ConflictError(Exception):
    pass


class ValidationError(Exception):
    def __init__(self, problems: list[Problem]) -> None:
        self.problems = problems


def next_cron_occurrence(expression: str, timezone: str, base: datetime | None = None) -> datetime:
    try:
        zone = ZoneInfo(timezone)
        localized = (base or datetime.now(UTC)).astimezone(zone)
        result = croniter(expression, localized).get_next(datetime)
    except (ValueError, KeyError, ZoneInfoNotFoundError) as exc:
        raise ValueError("Invalid cron expression or timezone") from exc
    return cast(datetime, result).astimezone(UTC)


async def update_draft(db: AsyncSession, workflow_id: uuid.UUID, revision: int, graph: Graph) -> Workflow:
    workflow = await db.scalar(select(Workflow).where(Workflow.id == workflow_id).with_for_update())
    if workflow is None:
        raise LookupError("workflow not found")
    if workflow.draft_revision != revision:
        raise ConflictError("draft revision is stale")
    workflow.draft_definition = graph.model_dump(mode="json")
    workflow.draft_revision += 1
    await db.commit()
    return workflow


async def publish(db: AsyncSession, workflow_id: uuid.UUID) -> WorkflowVersion:
    """Validate, create immutable version, and activate in one transaction."""
    async with db.begin():
        workflow = await db.scalar(select(Workflow).where(Workflow.id == workflow_id).with_for_update())
        if workflow is None:
            raise LookupError("workflow not found")
        graph = Graph.model_validate(workflow.draft_definition)
        problems = validate_graph(graph)
        schedule_node = next((node for node in graph.nodes if node.type == "schedule_trigger"), None)
        if schedule_node:
            try:
                next_cron_occurrence(
                    str(schedule_node.configuration.get("cron", "")),
                    str(schedule_node.configuration.get("timezone", "")),
                )
            except ValueError as exc:
                problems.append(
                    Problem(
                        code="INVALID_SCHEDULE",
                        message=str(exc),
                        node_id=schedule_node.id,
                        field="cron",
                    )
                )
        for node in graph.nodes:
            credential_id = node.configuration.get("credential_id")
            if not credential_id:
                continue
            try:
                credential = await db.get(Credential, uuid.UUID(str(credential_id)))
            except ValueError:
                credential = None
            if credential is None or credential.project_id != workflow.project_id:
                problems.append(
                    Problem(
                        code="INVALID_CREDENTIAL_REFERENCE",
                        message="Referenced credential does not exist in this project",
                        node_id=node.id,
                        field="credential_id",
                    )
                )
        if problems:
            raise ValidationError(problems)
        latest = await db.scalar(
            select(func.max(WorkflowVersion.version_number)).where(WorkflowVersion.workflow_id == workflow_id)
        )
        version = WorkflowVersion(
            workflow_id=workflow_id, version_number=(latest or 0) + 1, graph_definition=graph.model_dump(mode="json")
        )
        db.add(version)
        await db.flush()
        workflow.active_version_id = version.id
        workflow.status = "active"
        trigger = next((node for node in graph.nodes if node.type.endswith("_trigger")), None)
        if trigger and trigger.type == "webhook_trigger":
            webhook = await db.scalar(
                select(WebhookTrigger).where(WebhookTrigger.workflow_id == workflow.id).with_for_update()
            )
            if webhook is None:
                webhook = WebhookTrigger(workflow_id=workflow.id, token=secrets.token_urlsafe(32))
                db.add(webhook)
            webhook.method = str(trigger.configuration.get("method", "POST")).upper()
            webhook.auth_type = str(trigger.configuration.get("auth_type", "none"))
            credential_id = trigger.configuration.get("credential_id")
            webhook.credential_id = uuid.UUID(str(credential_id)) if credential_id else None
            webhook.enabled = True
        elif trigger and trigger.type == "schedule_trigger":
            schedule = await db.scalar(
                select(ScheduleTrigger).where(ScheduleTrigger.workflow_id == workflow.id).with_for_update()
            )
            cron = str(trigger.configuration["cron"])
            timezone = str(trigger.configuration["timezone"])
            if schedule is None:
                schedule = ScheduleTrigger(
                    workflow_id=workflow.id,
                    cron=cron,
                    timezone=timezone,
                    next_run_at=next_cron_occurrence(cron, timezone),
                )
                db.add(schedule)
            else:
                schedule.cron = cron
                schedule.timezone = timezone
                schedule.next_run_at = next_cron_occurrence(cron, timezone)
                schedule.enabled = True
        db.add(
            OutboxEvent(
                event_type="workflow.published",
                payload={"workflow_id": str(workflow.id), "workflow_version_id": str(version.id)},
            )
        )
    return version


async def create_execution(
    db: AsyncSession, workflow_id: uuid.UUID, trigger_type: str, trigger_data: dict[str, Any]
) -> Execution:
    """Create all durable states and initial jobs atomically; routes never execute nodes."""
    async with db.begin():
        workflow = await db.scalar(select(Workflow).where(Workflow.id == workflow_id).with_for_update(key_share=True))
        if workflow is None or workflow.active_version_id is None:
            raise LookupError("active workflow not found")
        execution = await create_execution_in_transaction(db, workflow, trigger_type, trigger_data)
    return execution


async def create_execution_in_transaction(
    db: AsyncSession, workflow: Workflow, trigger_type: str, trigger_data: dict[str, Any]
) -> Execution:
    """Create an execution inside the caller's transaction.

    This is intentionally separate so webhook idempotency and schedule occurrence
    records can be committed atomically with execution creation.
    """
    if workflow.active_version_id is None:
        raise LookupError("active workflow not found")
    version = await db.get(WorkflowVersion, workflow.active_version_id)
    if version is None:
        raise RuntimeError("active version invariant violated")
    graph = Graph.model_validate(version.graph_definition)
    execution = Execution(
        workflow_id=workflow.id,
        workflow_version_id=version.id,
        trigger_type=trigger_type,
        trigger_data=trigger_data,
    )
    db.add(execution)
    await db.flush()
    trigger_ids = {node.id for node in graph.nodes if node.type.endswith("_trigger")}
    for node in graph.nodes:
        node_status = NodeStatus.READY if node.id in trigger_ids else NodeStatus.PENDING
        node_execution = NodeExecution(
            execution_id=execution.id,
            node_id=node.id,
            node_type=node.type,
            configuration_snapshot=node.configuration,
            status=node_status,
        )
        db.add(node_execution)
        if node_status is NodeStatus.READY:
            await db.flush()
            db.add(
                OutboxEvent(
                    event_type="node.ready",
                    payload={"execution_id": str(execution.id), "node_execution_id": str(node_execution.id)},
                )
            )
    return execution


async def cancel_execution(db: AsyncSession, execution_id: uuid.UUID) -> Execution:
    async with db.begin():
        execution = await db.scalar(select(Execution).where(Execution.id == execution_id).with_for_update())
        if execution is None:
            raise LookupError("execution not found")
        if execution.status in {ExecutionStatus.COMPLETED, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED}:
            return execution
        execution.cancellation_requested = True
        nodes = (
            await db.scalars(select(NodeExecution).where(NodeExecution.execution_id == execution.id).with_for_update())
        ).all()
        for node in nodes:
            if node.status in {NodeStatus.PENDING, NodeStatus.READY, NodeStatus.WAITING}:
                node.status = NodeStatus.CANCELLED
        if not any(node.status is NodeStatus.RUNNING for node in nodes):
            execution.status = ExecutionStatus.CANCELLED
            execution.finished_at = datetime.now(UTC)
    return execution
