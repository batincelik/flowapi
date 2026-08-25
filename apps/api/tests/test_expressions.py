import pytest
from flowapi.expressions import ExpressionError, evaluate, interpolate, validate_expression

SCOPE = {
    "trigger": {"body": {"id": 7}},
    "input": {},
    "nodes": {"fetch": {"status": 200}},
    "variables": {"BASE": "https://example.com"},
    "execution": {"id": "x"},
}


def test_restricted_access_arithmetic_and_interpolation() -> None:
    assert evaluate("$trigger.body.id + 2", SCOPE) == 9
    assert evaluate("$nodes.fetch.status >= 200 && $nodes.fetch.status < 300", SCOPE) is True
    assert interpolate("{{$variables.BASE}}/users/{{$trigger.body.id}}", SCOPE) == "https://example.com/users/7"


@pytest.mark.parametrize(
    "attack",
    [
        "__import__('os')",
        "open('/etc/passwd')",
        "constructor.constructor('return process.env')()",
        "process.env",
        "require('fs')",
        "globalThis",
        "$trigger.__class__",
    ],
)
def test_code_execution_attacks_are_rejected(attack: str) -> None:
    with pytest.raises(ExpressionError):
        evaluate(attack, SCOPE)


def test_calls_and_comprehensions_are_not_supported() -> None:
    with pytest.raises(ExpressionError):
        evaluate("len($input)", SCOPE)
    with pytest.raises(ExpressionError):
        evaluate("[x for x in $input]", SCOPE)


def test_static_validation_does_not_require_runtime_values() -> None:
    validate_expression("$nodes.fetch.status >= 200 && $trigger.body.enabled")
    with pytest.raises(ExpressionError):
        validate_expression("$nodes.fetch.constructor()")
