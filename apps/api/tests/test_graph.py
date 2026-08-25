from flowapi.graph import Edge, Graph, Node, validate_graph


def test_valid_parallel_dag() -> None:
    graph = Graph(
        nodes=[Node(id="t", type="manual_trigger"), Node(id="a", type="set"), Node(id="b", type="set")],
        edges=[
            Edge(id="1", source_node_id="t", target_node_id="a"),
            Edge(id="2", source_node_id="t", target_node_id="b"),
        ],
    )
    assert validate_graph(graph) == []


def test_cycle_and_unreachable_are_rejected() -> None:
    graph = Graph(
        nodes=[Node(id="t", type="manual_trigger"), Node(id="a", type="set"), Node(id="b", type="set")],
        edges=[
            Edge(id="1", source_node_id="t", target_node_id="a"),
            Edge(id="2", source_node_id="a", target_node_id="b"),
            Edge(id="3", source_node_id="b", target_node_id="a"),
        ],
    )
    codes = {error.code for error in validate_graph(graph)}
    assert "CYCLE_DETECTED" in codes


def test_invalid_condition_handle_is_rejected() -> None:
    graph = Graph(
        nodes=[
            Node(id="t", type="manual_trigger"),
            Node(id="c", type="condition", configuration={"expression": "true"}),
            Node(id="a", type="set"),
        ],
        edges=[
            Edge(id="1", source_node_id="t", target_node_id="c"),
            Edge(id="2", source_node_id="c", source_handle="maybe", target_node_id="a"),
        ],
    )
    assert "INVALID_HANDLE" in {error.code for error in validate_graph(graph)}


def test_publish_validation_rejects_malicious_expression() -> None:
    graph = Graph(
        nodes=[
            Node(id="t", type="manual_trigger"),
            Node(id="c", type="condition", configuration={"expression": "__import__('os')"}),
        ],
        edges=[Edge(id="1", source_node_id="t", target_node_id="c")],
    )
    assert "INVALID_EXPRESSION" in {error.code for error in validate_graph(graph)}
