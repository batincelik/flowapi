import json
import uuid
from base64 import b64encode
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import get_settings
from .expressions import ExpressionError, evaluate, interpolate
from .graph import Edge, Graph, Node
from .http_node import HTTPNodeError, request
from .models import (
    Credential,
    Execution,
    ExecutionStatus,
    NodeExecution,
    NodeExecutionAttempt,
    NodeStatus,
    OutboxEvent,
    Workflow,
    WorkflowVersion,
)
from .postgres_node import PostgreSQLNodeError, execute_postgresql
from .security import CredentialCipher


class NodeFailure(Exception):
    def __init__(self, code: str, message: str, transient: bool = False) -> None:
        self.code, self.transient = code, transient
        super().__init__(message)


def _render(value: Any, scope: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return interpolate(value, scope)
    if isinstance(value, list):
        return [_render(item, scope) for item in value]
    if isinstance(value, dict):
        return {key: _render(item, scope) for key, item in value.items()}
    return value


async def execute_builtin(
    node: Node,
    input_data: dict[str, Any],
    scope: dict[str, Any],
    credential_data: dict[str, str] | None = None,
) -> tuple[dict[str, Any], str]:
    if node.type.endswith("_trigger"):
        return scope["trigger"], "success"
    if node.type == "set":
        return _render(node.configuration.get("values", {}), scope), "success"
    if node.type == "condition":
        try:
            selected = bool(evaluate(node.configuration["expression"], scope))
        except ExpressionError as exc:
            raise NodeFailure("INVALID_EXPRESSION", str(exc)) from exc
        return {"result": selected}, "true" if selected else "false"
    if node.type == "stop":
        return {"stopped": True}, "success"
    if node.type == "merge":
        return input_data, "success"
    if node.type == "http_request":
        config = _render(node.configuration, scope)
        headers = dict(config.get("headers", {}))
        secrets: list[str] = []
        if credential_data:
            secrets = [value for key, value in credential_data.items() if key != "kind"]
            if credential_data["kind"] == "bearer_token":
                headers["Authorization"] = f"Bearer {credential_data['token']}"
            elif credential_data["kind"] == "basic_auth":
                encoded = b64encode(f"{credential_data['username']}:{credential_data['password']}".encode()).decode()
                headers["Authorization"] = f"Basic {encoded}"
                secrets.append(encoded)
            elif credential_data["kind"] == "api_key":
                headers[credential_data["header_name"]] = credential_data["value"]
        try:
            result = await request(
                config["method"],
                config["url"],
                headers=headers,
                body=config.get("body"),
                timeout_seconds=float(config.get("timeout_seconds", 30)),
                max_response_bytes=get_settings().MAX_NODE_OUTPUT_BYTES,
                follow_redirects=bool(config.get("follow_redirects", True)),
                allow_private=get_settings().SSRF_ALLOW_PRIVATE_NETWORKS,
            )
        except HTTPNodeError as exc:
            raise NodeFailure(exc.code, str(exc), transient=exc.transient) from exc
        if result.status >= 400:
            raise NodeFailure(
                f"HTTP_{result.status}",
                f"Remote server returned HTTP {result.status}",
                transient=result.status in {408, 425, 429, 502, 503, 504},
            )
        body = result.body
        for secret in secrets:
            if secret:
                body = body.replace(secret, "[REDACTED]")
        return {
            "status": result.status,
            "headers": result.headers,
            "body": body,
            "duration_ms": result.duration_ms,
            "url": result.url,
        }, "success"
    if node.type == "postgresql":
        if not credential_data or credential_data.get("kind") != "postgresql":
            raise NodeFailure("INVALID_CREDENTIAL_TYPE", "PostgreSQL node requires a PostgreSQL credential")
        config = _render(node.configuration, scope)
        parameters = config.get("parameters")
        if not isinstance(parameters, dict):
            raise NodeFailure("INVALID_SQL_PARAMETERS", "PostgreSQL parameters must be an object")
        settings = get_settings()
        try:
            pg_result = await execute_postgresql(
                str(config["query"]),
                parameters,
                credential_data,
                connect_timeout=settings.POSTGRES_CONNECT_TIMEOUT,
                query_timeout=settings.POSTGRES_QUERY_TIMEOUT,
                max_output_bytes=settings.MAX_NODE_OUTPUT_BYTES,
            )
        except PostgreSQLNodeError as exc:
            raise NodeFailure(exc.code, str(exc), transient=exc.transient) from exc
        return pg_result, "success"
    raise NodeFailure("UNSUPPORTED_NODE_TYPE", f"No executor for {node.type}")


async def claim_node(
    db: AsyncSession, node_execution_id: uuid.UUID, worker_id: uuid.UUID, lease_seconds: int = 60
) -> NodeExecution | None:
    async with db.begin():
        row = await db.scalar(
            select(NodeExecution).where(NodeExecution.id == node_execution_id).with_for_update(skip_locked=True)
        )
        if row is None or row.status is not NodeStatus.READY:
            return None
        execution = await db.get(Execution, row.execution_id)
        if execution is None or execution.cancellation_requested:
            row.status = NodeStatus.CANCELLED
            return None
        row.status, row.worker_id = NodeStatus.RUNNING, worker_id
        row.lease_expires_at = datetime.now(UTC) + timedelta(seconds=lease_seconds)
        count = await db.scalar(
            select(func.count())
            .select_from(NodeExecutionAttempt)
            .where(NodeExecutionAttempt.node_execution_id == row.id)
        )
        db.add(NodeExecutionAttempt(node_execution_id=row.id, attempt_number=(count or 0) + 1, status="running"))
        execution.status, execution.started_at = ExecutionStatus.RUNNING, execution.started_at or datetime.now(UTC)
    return row


async def run_claimed_node(db: AsyncSession, node_execution_id: uuid.UUID) -> None:
    row = await db.get(NodeExecution, node_execution_id)
    if row is None or row.status is not NodeStatus.RUNNING:
        return
    execution = await db.get(Execution, row.execution_id)
    version = await db.get(WorkflowVersion, execution.workflow_version_id) if execution else None
    if execution is None or version is None:
        raise RuntimeError("execution invariant violated")
    graph = Graph.model_validate(version.graph_definition)
    by_id = {node.id: node for node in graph.nodes}
    current_node = by_id[row.node_id]
    completed = (
        await db.scalars(
            select(NodeExecution).where(
                NodeExecution.execution_id == execution.id, NodeExecution.status == NodeStatus.COMPLETED
            )
        )
    ).all()
    outputs = {item.node_id: item.output_data for item in completed}
    incoming = [edge for edge in graph.edges if edge.target_node_id == row.node_id]
    input_data: Any = {edge.source_node_id: outputs.get(edge.source_node_id) for edge in incoming}
    if len(input_data) == 1:
        input_data = next(iter(input_data.values())) or {}
    scope = {
        "trigger": execution.trigger_data,
        "input": input_data,
        "nodes": outputs,
        "variables": execution.variable_snapshot,
        "execution": {"id": str(execution.id)},
    }
    credential_data: dict[str, str] | None = None
    credential_id = current_node.configuration.get("credential_id")
    if credential_id:
        workflow = await db.get(Workflow, execution.workflow_id)
        try:
            parsed_credential_id = uuid.UUID(str(credential_id))
        except ValueError:
            await db.rollback()
            await finish_failure(
                db,
                node_execution_id,
                NodeFailure("INVALID_CREDENTIAL_REFERENCE", "Credential ID is invalid"),
                graph,
            )
            return
        credential = await db.get(Credential, parsed_credential_id)
        if credential is None or workflow is None or credential.project_id != workflow.project_id:
            await db.rollback()
            await finish_failure(
                db, node_execution_id, NodeFailure("MISSING_CREDENTIAL", "Credential is unavailable"), graph
            )
            return
        try:
            credential_data = CredentialCipher(get_settings().FLOWAPI_ENCRYPTION_KEY).decrypt_json(
                credential.encrypted_data
            )
        except ValueError:
            await db.rollback()
            await finish_failure(
                db,
                node_execution_id,
                NodeFailure("CREDENTIAL_DECRYPTION_FAILED", "Credential is unavailable"),
                graph,
            )
            return
        credential_data["kind"] = credential.type
    # End the implicit read transaction before the external operation. The running
    # state and lease were committed by claim_node in a separate transaction.
    await db.commit()
    if len(json.dumps(input_data, separators=(",", ":")).encode()) > get_settings().MAX_NODE_OUTPUT_BYTES:
        await finish_failure(
            db,
            node_execution_id,
            NodeFailure("INPUT_TOO_LARGE", "Node input exceeds configured limit"),
            graph,
        )
        return
    async with db.begin():
        attempt = await db.scalar(
            select(NodeExecutionAttempt)
            .where(
                NodeExecutionAttempt.node_execution_id == node_execution_id,
                NodeExecutionAttempt.status == "running",
            )
            .order_by(NodeExecutionAttempt.attempt_number.desc())
            .limit(1)
            .with_for_update()
        )
        if attempt is None:
            raise RuntimeError("running attempt missing before execution")
        attempt.input_data = input_data
    if current_node.type == "delay":
        await begin_delay(db, node_execution_id, current_node, graph)
        return
    try:
        output, _selected_handle = await execute_builtin(current_node, input_data, scope, credential_data)
        if len(json.dumps(output, separators=(",", ":")).encode()) > 1_048_576:
            raise NodeFailure("OUTPUT_TOO_LARGE", "Node output exceeds configured limit")
    except NodeFailure as failure:
        await finish_failure(db, node_execution_id, failure, graph)
        return
    await finish_success(db, node_execution_id, output, graph)


async def begin_delay(db: AsyncSession, node_execution_id: uuid.UUID, node: Node, graph: Graph) -> None:
    seconds = min(max(float(node.configuration.get("seconds", 0)), 0), 31_536_000)
    if seconds <= 0:
        await finish_success(db, node_execution_id, {"delayed_seconds": 0}, graph)
        return
    async with db.begin():
        row = await db.scalar(select(NodeExecution).where(NodeExecution.id == node_execution_id).with_for_update())
        if row is None or row.status is not NodeStatus.RUNNING:
            return
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
            raise RuntimeError("running delay attempt missing")
        wake_at = datetime.now(UTC) + timedelta(seconds=seconds)
        attempt.status = "waiting"
        row.status = NodeStatus.WAITING
        row.available_at = wake_at
        row.worker_id = None
        row.lease_expires_at = None
        execution = await db.get(Execution, row.execution_id, with_for_update=True)
        if execution:
            execution.status = ExecutionStatus.WAITING
        db.add(
            OutboxEvent(
                event_type="node.resume",
                payload={"execution_id": str(row.execution_id), "node_execution_id": str(row.id)},
                available_at=wake_at,
            )
        )


async def resume_delay(db: AsyncSession, node_execution_id: uuid.UUID) -> bool:
    """Complete a persisted delay. Duplicate resume jobs are harmless."""
    row = await db.get(NodeExecution, node_execution_id)
    if row is None or row.status is not NodeStatus.WAITING:
        await db.rollback()
        return False
    execution = await db.get(Execution, row.execution_id)
    version = await db.get(WorkflowVersion, execution.workflow_version_id) if execution else None
    if execution is None or version is None:
        await db.rollback()
        raise RuntimeError("delay execution invariant violated")
    graph = Graph.model_validate(version.graph_definition)
    await db.commit()
    async with db.begin():
        row = await db.scalar(select(NodeExecution).where(NodeExecution.id == node_execution_id).with_for_update())
        if row is None or row.status is not NodeStatus.WAITING:
            return False
        attempt = await db.scalar(
            select(NodeExecutionAttempt)
            .where(
                NodeExecutionAttempt.node_execution_id == row.id,
                NodeExecutionAttempt.status == "waiting",
            )
            .order_by(NodeExecutionAttempt.attempt_number.desc())
            .limit(1)
            .with_for_update()
        )
        if attempt is None:
            raise RuntimeError("waiting delay attempt missing")
        now = datetime.now(UTC)
        seconds = float(row.configuration_snapshot.get("seconds", 0))
        output = {"delayed_seconds": seconds, "resumed_at": now.isoformat()}
        attempt.status = "completed"
        attempt.output_data = output
        attempt.finished_at = now
        row.status = NodeStatus.COMPLETED
        row.output_data = output
        row.official_attempt_id = attempt.id
        row.available_at = None
        execution = await db.get(Execution, row.execution_id, with_for_update=True)
        if execution:
            execution.status = ExecutionStatus.RUNNING
        await _advance(db, row.execution_id, graph, now)
    return True


async def finish_success(db: AsyncSession, node_execution_id: uuid.UUID, output: dict[str, Any], graph: Graph) -> None:
    async with db.begin():
        row = await db.scalar(select(NodeExecution).where(NodeExecution.id == node_execution_id).with_for_update())
        if row is None or row.status is not NodeStatus.RUNNING:
            return
        attempt = await db.scalar(
            select(NodeExecutionAttempt)
            .where(NodeExecutionAttempt.node_execution_id == row.id, NodeExecutionAttempt.status == "running")
            .order_by(NodeExecutionAttempt.attempt_number.desc())
            .limit(1)
            .with_for_update()
        )
        if attempt is None:
            raise RuntimeError("running attempt missing")
        now = datetime.now(UTC)
        attempt.status, attempt.output_data, attempt.finished_at = "completed", output, now
        row.status, row.output_data, row.official_attempt_id = NodeStatus.COMPLETED, output, attempt.id
        row.lease_expires_at, row.worker_id = None, None
        await _advance(db, row.execution_id, graph, now)


async def finish_failure(db: AsyncSession, node_execution_id: uuid.UUID, failure: NodeFailure, graph: Graph) -> None:
    async with db.begin():
        row = await db.scalar(select(NodeExecution).where(NodeExecution.id == node_execution_id).with_for_update())
        if row is None or row.status is not NodeStatus.RUNNING:
            return
        attempt = await db.scalar(
            select(NodeExecutionAttempt)
            .where(NodeExecutionAttempt.node_execution_id == row.id, NodeExecutionAttempt.status == "running")
            .order_by(NodeExecutionAttempt.attempt_number.desc())
            .limit(1)
            .with_for_update()
        )
        if attempt is None:
            raise RuntimeError("running attempt missing")
        attempt.status, attempt.error_code, attempt.error_message, attempt.finished_at = (
            "failed",
            failure.code,
            str(failure),
            datetime.now(UTC),
        )
        max_attempts = min(max(int(row.configuration_snapshot.get("max_attempts", 1)), 1), 10)
        retry_delay = min(max(float(row.configuration_snapshot.get("retry_delay_seconds", 1)), 0), 3600)
        row.lease_expires_at, row.worker_id = None, None
        if failure.transient and attempt.attempt_number < max_attempts:
            row.status = NodeStatus.READY
            db.add(
                OutboxEvent(
                    event_type="node.ready",
                    payload={"execution_id": str(row.execution_id), "node_execution_id": str(row.id)},
                    available_at=datetime.now(UTC) + timedelta(seconds=retry_delay),
                )
            )
            return
        row.status = NodeStatus.FAILED
        execution = await db.get(Execution, row.execution_id, with_for_update=True)
        has_error_branch = any(
            edge.source_node_id == row.node_id and edge.source_handle == "error" for edge in graph.edges
        )
        if execution and has_error_branch:
            await _advance(db, row.execution_id, graph, datetime.now(UTC))
        elif execution:
            execution.status, execution.error_code, execution.error_message, execution.finished_at = (
                ExecutionStatus.FAILED,
                failure.code,
                str(failure),
                datetime.now(UTC),
            )


async def _advance(db: AsyncSession, execution_id: uuid.UUID, graph: Graph, now: datetime) -> None:
    execution = await db.get(Execution, execution_id, with_for_update=True)
    if execution is None:
        raise RuntimeError("execution missing during downstream transition")
    rows = (
        await db.scalars(select(NodeExecution).where(NodeExecution.execution_id == execution_id).with_for_update())
    ).all()
    if execution.cancellation_requested:
        for row in rows:
            if row.status in {NodeStatus.PENDING, NodeStatus.READY, NodeStatus.WAITING}:
                row.status = NodeStatus.CANCELLED
        if not any(row.status is NodeStatus.RUNNING for row in rows):
            execution.status = ExecutionStatus.CANCELLED
            execution.finished_at = now
        return
    states = {row.node_id: row for row in rows}
    while True:
        decisions = downstream_decisions(
            graph,
            {node_id: row.status for node_id, row in states.items()},
            {node_id: row.output_data for node_id, row in states.items()},
        )
        if not decisions:
            break
        for node_id, status in decisions.items():
            row = states[node_id]
            row.status = status
            if status is NodeStatus.READY:
                db.add(
                    OutboxEvent(
                        event_type="node.ready",
                        payload={"execution_id": str(execution_id), "node_execution_id": str(row.id)},
                    )
                )
    terminal = {NodeStatus.COMPLETED, NodeStatus.FAILED, NodeStatus.SKIPPED, NodeStatus.CANCELLED}
    if all(row.status in terminal for row in rows):
        execution.status = ExecutionStatus.CANCELLED if execution.cancellation_requested else ExecutionStatus.COMPLETED
        execution.finished_at = now


def downstream_decisions(
    graph: Graph,
    statuses: dict[str, NodeStatus],
    outputs: dict[str, dict[str, Any] | None],
) -> dict[str, NodeStatus]:
    """Return newly decidable nodes from durable state only.

    Reconciliation can call this after a crash; no selected-edge decision exists only in
    worker memory. Condition selection is reconstructed from its immutable output.
    """
    by_id = {node.id: node for node in graph.nodes}
    incoming: dict[str, list[Edge]] = {node.id: [] for node in graph.nodes}
    for edge in graph.edges:
        incoming[edge.target_node_id].append(edge)
    terminal = {NodeStatus.COMPLETED, NodeStatus.FAILED, NodeStatus.SKIPPED, NodeStatus.CANCELLED}

    def edge_is_active(edge: Edge) -> bool:
        source_status = statuses[edge.source_node_id]
        if source_status is NodeStatus.FAILED:
            return edge.source_handle == "error"
        if source_status is not NodeStatus.COMPLETED:
            return False
        source = by_id[edge.source_node_id]
        if source.type == "condition":
            selected = "true" if bool((outputs.get(source.id) or {}).get("result")) else "false"
            return edge.source_handle == selected
        return edge.source_handle == "success"

    decisions: dict[str, NodeStatus] = {}
    for node_id, status in statuses.items():
        if status is not NodeStatus.PENDING:
            continue
        edges = incoming[node_id]
        if not edges:
            continue
        active_completed = any(edge_is_active(edge) for edge in edges)
        node = by_id[node_id]
        if node.type == "merge" and node.configuration.get("mode", "wait_for_all") == "first_available":
            if active_completed:
                decisions[node_id] = NodeStatus.READY
            continue
        if all(statuses[edge.source_node_id] in terminal for edge in edges):
            decisions[node_id] = NodeStatus.READY if active_completed else NodeStatus.SKIPPED
    return decisions
