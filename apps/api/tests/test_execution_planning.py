from flowapi.executor import downstream_decisions
from flowapi.graph import Edge, Graph, Node
from flowapi.models import NodeStatus


def condition_graph() -> Graph:
    return Graph(
        nodes=[
            Node(id="trigger", type="manual_trigger"),
            Node(id="condition", type="condition", configuration={"expression": "true"}),
            Node(id="yes", type="set"),
            Node(id="no", type="set"),
            Node(id="join", type="merge"),
        ],
        edges=[
            Edge(id="1", source_node_id="trigger", target_node_id="condition"),
            Edge(id="2", source_node_id="condition", source_handle="true", target_node_id="yes"),
            Edge(id="3", source_node_id="condition", source_handle="false", target_node_id="no"),
            Edge(id="4", source_node_id="yes", target_node_id="join"),
            Edge(id="5", source_node_id="no", target_node_id="join"),
        ],
    )


def test_condition_selects_one_branch_and_skips_other() -> None:
    statuses = {
        "trigger": NodeStatus.COMPLETED,
        "condition": NodeStatus.COMPLETED,
        "yes": NodeStatus.PENDING,
        "no": NodeStatus.PENDING,
        "join": NodeStatus.PENDING,
    }
    decisions = downstream_decisions(condition_graph(), statuses, {"condition": {"result": True}})
    assert decisions == {"yes": NodeStatus.READY, "no": NodeStatus.SKIPPED}


def test_join_runs_after_selected_branch_and_skipped_branch_are_terminal() -> None:
    statuses = {
        "trigger": NodeStatus.COMPLETED,
        "condition": NodeStatus.COMPLETED,
        "yes": NodeStatus.COMPLETED,
        "no": NodeStatus.SKIPPED,
        "join": NodeStatus.PENDING,
    }
    assert downstream_decisions(condition_graph(), statuses, {"condition": {"result": True}}) == {
        "join": NodeStatus.READY
    }


def test_parallel_join_waits_for_every_active_parent() -> None:
    graph = Graph(
        nodes=[
            Node(id="t", type="manual_trigger"),
            Node(id="a", type="set"),
            Node(id="b", type="set"),
            Node(id="join", type="merge"),
        ],
        edges=[
            Edge(id="1", source_node_id="t", target_node_id="a"),
            Edge(id="2", source_node_id="t", target_node_id="b"),
            Edge(id="3", source_node_id="a", target_node_id="join"),
            Edge(id="4", source_node_id="b", target_node_id="join"),
        ],
    )
    statuses = {
        "t": NodeStatus.COMPLETED,
        "a": NodeStatus.COMPLETED,
        "b": NodeStatus.RUNNING,
        "join": NodeStatus.PENDING,
    }
    assert downstream_decisions(graph, statuses, {}) == {}
    statuses["b"] = NodeStatus.COMPLETED
    assert downstream_decisions(graph, statuses, {}) == {"join": NodeStatus.READY}


def test_failed_node_activates_only_error_branch() -> None:
    graph = Graph(
        nodes=[
            Node(id="t", type="manual_trigger"),
            Node(id="request", type="http_request", configuration={"url": "https://example.com", "method": "GET"}),
            Node(id="ok", type="set"),
            Node(id="alert", type="set"),
        ],
        edges=[
            Edge(id="1", source_node_id="t", target_node_id="request"),
            Edge(id="2", source_node_id="request", source_handle="success", target_node_id="ok"),
            Edge(id="3", source_node_id="request", source_handle="error", target_node_id="alert"),
        ],
    )
    statuses = {
        "t": NodeStatus.COMPLETED,
        "request": NodeStatus.FAILED,
        "ok": NodeStatus.PENDING,
        "alert": NodeStatus.PENDING,
    }
    assert downstream_decisions(graph, statuses, {}) == {
        "ok": NodeStatus.SKIPPED,
        "alert": NodeStatus.READY,
    }
