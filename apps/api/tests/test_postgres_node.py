import pytest
from flowapi.postgres_node import PostgreSQLNodeError, bind_query, json_value


def test_named_parameters_are_never_interpolated() -> None:
    query, values = bind_query(
        "SELECT * FROM users WHERE id = :user_id OR owner_id = :user_id AND state = :state",
        {"user_id": "1' OR TRUE --", "state": "active"},
    )
    assert query == "SELECT * FROM users WHERE id = $1 OR owner_id = $1 AND state = $2"
    assert values == ["1' OR TRUE --", "active"]
    assert values[0] not in query


def test_missing_and_unused_parameters_are_rejected() -> None:
    with pytest.raises(PostgreSQLNodeError, match="Missing"):
        bind_query("SELECT :missing", {})
    with pytest.raises(PostgreSQLNodeError, match="Unused"):
        bind_query("SELECT 1", {"unexpected": 1})


def test_binary_outputs_are_not_embedded_in_json() -> None:
    assert json_value(b"large binary") == {"type": "binary", "size": 12}
