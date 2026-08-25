from collections import defaultdict, deque
from typing import Any

from pydantic import BaseModel, Field

from .expressions import ExpressionError, validate_expression, validate_template


class Node(BaseModel):
    id: str = Field(min_length=1, max_length=200)
    type: str
    version: int = 1
    configuration: dict[str, Any] = Field(default_factory=dict)
    position: dict[str, float] = Field(default_factory=dict)


class Edge(BaseModel):
    id: str
    source_node_id: str
    target_node_id: str
    source_handle: str = "success"
    target_handle: str = "input"


class Graph(BaseModel):
    nodes: list[Node]
    edges: list[Edge]


class Problem(BaseModel):
    code: str
    message: str
    node_id: str | None = None
    field: str | None = None


TRIGGERS = {"manual_trigger", "webhook_trigger", "schedule_trigger"}
# Outbound HTTP and durable delay are registered only when their complete executors are available.
# Advertising a non-executable editor node would create a false production capability.
SUPPORTED = TRIGGERS | {"set", "condition", "stop", "merge", "http_request", "delay", "postgresql"}
HANDLES = {
    "condition": {"true", "false", "error"},
    "stop": set(),
}


def validate_graph(graph: Graph, max_nodes: int = 500, max_edges: int = 1000) -> list[Problem]:
    errors: list[Problem] = []
    if len(graph.nodes) > max_nodes:
        errors.append(Problem(code="GRAPH_TOO_LARGE", message="Node limit exceeded"))
    if len(graph.edges) > max_edges:
        errors.append(Problem(code="GRAPH_TOO_LARGE", message="Edge limit exceeded"))
    ids = [node.id for node in graph.nodes]
    known = set(ids)
    if len(known) != len(ids):
        errors.append(Problem(code="DUPLICATE_NODE_ID", message="Node IDs must be unique"))
    triggers = [node for node in graph.nodes if node.type in TRIGGERS]
    if len(triggers) != 1:
        errors.append(Problem(code="INVALID_TRIGGER_COUNT", message="Exactly one trigger is required"))
    by_id = {node.id: node for node in graph.nodes}
    seen_edges: set[tuple[str, str, str, str]] = set()
    adjacency: dict[str, list[str]] = defaultdict(list)
    indegree = dict.fromkeys(known, 0)
    for node in graph.nodes:
        if node.type not in SUPPORTED:
            errors.append(
                Problem(code="UNSUPPORTED_NODE_TYPE", message=f"Unsupported node type: {node.type}", node_id=node.id)
            )
        if node.version != 1:
            errors.append(
                Problem(code="UNSUPPORTED_NODE_VERSION", message="Only node version 1 is supported", node_id=node.id)
            )
        errors.extend(validate_configuration(node))
    for edge in graph.edges:
        key = (edge.source_node_id, edge.source_handle, edge.target_node_id, edge.target_handle)
        if key in seen_edges:
            errors.append(Problem(code="DUPLICATE_EDGE", message="Duplicate edge", node_id=edge.source_node_id))
        seen_edges.add(key)
        if edge.source_node_id not in known or edge.target_node_id not in known:
            errors.append(Problem(code="INVALID_EDGE", message="Edge references a missing node"))
            continue
        source = by_id[edge.source_node_id]
        allowed = HANDLES.get(source.type, {"success", "error"})
        if edge.source_handle not in allowed:
            errors.append(
                Problem(
                    code="INVALID_HANDLE", message=f"Invalid source handle: {edge.source_handle}", node_id=source.id
                )
            )
        adjacency[edge.source_node_id].append(edge.target_node_id)
        indegree[edge.target_node_id] += 1
    queue = deque(node_id for node_id, degree in indegree.items() if degree == 0)
    visited: list[str] = []
    while queue:
        current = queue.popleft()
        visited.append(current)
        for target in adjacency[current]:
            indegree[target] -= 1
            if indegree[target] == 0:
                queue.append(target)
    if len(visited) != len(known):
        errors.append(Problem(code="CYCLE_DETECTED", message="Normal workflow graphs must be acyclic"))
    if triggers:
        reachable: set[str] = set()
        pending = [triggers[0].id]
        while pending:
            current = pending.pop()
            if current not in reachable:
                reachable.add(current)
                pending.extend(adjacency[current])
        for node_id in known - reachable:
            errors.append(Problem(code="UNREACHABLE_NODE", message="Node is unreachable from trigger", node_id=node_id))
    return errors


def validate_configuration(node: Node) -> list[Problem]:
    required = {
        "http_request": ["url", "method"],
        "condition": ["expression"],
        "delay": ["seconds"],
        "postgresql": ["query", "parameters", "credential_id"],
        "schedule_trigger": ["cron", "timezone"],
    }
    problems = [
        Problem(code="MISSING_REQUIRED_FIELD", message=f"{field} is required", node_id=node.id, field=field)
        for field in required.get(node.type, [])
        if node.configuration.get(field) in (None, "")
    ]
    if node.type == "condition" and isinstance(node.configuration.get("expression"), str):
        try:
            validate_expression(node.configuration["expression"])
        except ExpressionError as exc:
            problems.append(Problem(code="INVALID_EXPRESSION", message=str(exc), node_id=node.id, field="expression"))
    for field, value in node.configuration.items():
        if field == "expression" or not isinstance(value, str) or "{{" not in value:
            continue
        try:
            validate_template(value)
        except ExpressionError as exc:
            problems.append(Problem(code="INVALID_EXPRESSION", message=str(exc), node_id=node.id, field=field))
    allowed_methods = {
        "http_request": {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"},
        "webhook_trigger": {"GET", "POST", "PUT", "PATCH", "DELETE"},
    }
    if node.type in allowed_methods:
        method = str(node.configuration.get("method", "POST" if node.type == "webhook_trigger" else "GET")).upper()
        if method not in allowed_methods[node.type]:
            problems.append(
                Problem(
                    code="INVALID_CONFIGURATION", message="Unsupported HTTP method", node_id=node.id, field="method"
                )
            )
    if node.type == "postgresql":
        query = str(node.configuration.get("query", "")).strip()
        first_word = query.split(maxsplit=1)[0].upper() if query else ""
        if first_word not in {"SELECT", "INSERT", "UPDATE"} or ";" in query:
            problems.append(
                Problem(
                    code="INVALID_CONFIGURATION",
                    message="PostgreSQL query must be one parameterized SELECT, INSERT, or UPDATE statement",
                    node_id=node.id,
                    field="query",
                )
            )
        if not isinstance(node.configuration.get("parameters"), dict):
            problems.append(
                Problem(
                    code="INVALID_CONFIGURATION",
                    message="PostgreSQL parameters must be an object",
                    node_id=node.id,
                    field="parameters",
                )
            )
    return problems
