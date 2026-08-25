import ast
import re
from collections.abc import Callable
from typing import Any


class ExpressionError(ValueError):
    code = "EXPRESSION_PARSE_ERROR"


_INTERPOLATION = re.compile(r"\{\{\s*(.*?)\s*\}\}")
_FORBIDDEN = re.compile(
    r"(?:__|\b(?:import|open|eval|exec|require|process|globalThis|constructor|prototype|filesystem|os)\b)"
)
_ROOTS = {"$trigger", "$input", "$nodes", "$variables", "$execution"}


def _normalize(source: str) -> str:
    if _FORBIDDEN.search(source):
        raise ExpressionError("Forbidden expression token")
    for root in _ROOTS:
        source = source.replace(root, root[1:])
    source = source.replace("&&", " and ").replace("||", " or ")
    source = re.sub(r"(?<![=!])!(?!=)", " not ", source)
    source = re.sub(r"\bnull\b", "None", source)
    source = re.sub(r"\btrue\b", "True", source, flags=re.I)
    source = re.sub(r"\bfalse\b", "False", source, flags=re.I)
    return source.strip()


def evaluate(source: str, scope: dict[str, Any]) -> Any:
    try:
        tree = ast.parse(_normalize(source), mode="eval")
    except (SyntaxError, ValueError) as exc:
        raise ExpressionError(str(exc)) from exc
    return _Evaluator(scope).visit(tree.body)


def validate_expression(source: str) -> None:
    """Validate syntax and the AST allowlist without reading runtime data."""
    try:
        tree = ast.parse(_normalize(source), mode="eval")
    except (SyntaxError, ValueError) as exc:
        raise ExpressionError(str(exc)) from exc
    allowed = (
        ast.Expression,
        ast.Name,
        ast.Load,
        ast.Constant,
        ast.Attribute,
        ast.Subscript,
        ast.List,
        ast.Dict,
        ast.BoolOp,
        ast.And,
        ast.Or,
        ast.UnaryOp,
        ast.Not,
        ast.USub,
        ast.BinOp,
        ast.Add,
        ast.Sub,
        ast.Mult,
        ast.Div,
        ast.Mod,
        ast.Compare,
        ast.Eq,
        ast.NotEq,
        ast.Lt,
        ast.LtE,
        ast.Gt,
        ast.GtE,
        ast.In,
        ast.NotIn,
    )
    for node in ast.walk(tree):
        if not isinstance(node, allowed):
            raise ExpressionError(f"Unsupported expression element: {type(node).__name__}")
        if isinstance(node, ast.Name) and node.id not in {
            "trigger",
            "input",
            "nodes",
            "variables",
            "execution",
            "None",
            "True",
            "False",
        }:
            raise ExpressionError("Unknown expression root")
        if isinstance(node, ast.Attribute) and node.attr.startswith("_"):
            raise ExpressionError("Private properties are forbidden")


def validate_template(template: str) -> None:
    matches = list(_INTERPOLATION.finditer(template))
    if "{{" in template and not matches:
        raise ExpressionError("Malformed interpolation")
    for match in matches:
        validate_expression(match.group(1))


def interpolate(template: str, scope: dict[str, Any]) -> Any:
    matches = list(_INTERPOLATION.finditer(template))
    if len(matches) == 1 and matches[0].span() == (0, len(template)):
        return evaluate(matches[0].group(1), scope)
    return _INTERPOLATION.sub(lambda match: str(evaluate(match.group(1), scope) or ""), template)


class _Evaluator(ast.NodeVisitor):
    def __init__(self, scope: dict[str, Any]) -> None:
        self.scope = {
            key.removeprefix("$"): value
            for key, value in scope.items()
            if key.removeprefix("$") in {r[1:] for r in _ROOTS}
        }

    def generic_visit(self, node: ast.AST) -> Any:
        raise ExpressionError(f"Unsupported expression element: {type(node).__name__}")

    def visit_Name(self, node: ast.Name) -> Any:
        if node.id in {"None", "True", "False"}:
            return {"None": None, "True": True, "False": False}[node.id]
        if node.id not in self.scope:
            raise ExpressionError("Unknown expression root")
        return self.scope[node.id]

    def visit_Constant(self, node: ast.Constant) -> Any:
        if isinstance(node.value, (str, int, float, bool)) or node.value is None:
            return node.value
        raise ExpressionError("Unsupported literal")

    def visit_Attribute(self, node: ast.Attribute) -> Any:
        if node.attr.startswith("_"):
            raise ExpressionError("Private properties are forbidden")
        value = self.visit(node.value)
        return value.get(node.attr) if isinstance(value, dict) else None

    def visit_Subscript(self, node: ast.Subscript) -> Any:
        value, key = self.visit(node.value), self.visit(node.slice)
        try:
            return value[key]
        except (KeyError, IndexError, TypeError):
            return None

    def visit_List(self, node: ast.List) -> list[Any]:
        return [self.visit(item) for item in node.elts]

    def visit_Dict(self, node: ast.Dict) -> dict[Any, Any]:
        if any(key is None for key in node.keys):
            raise ExpressionError("Dictionary unpacking is forbidden")
        return {
            self.visit(key): self.visit(value)
            for key, value in zip(node.keys, node.values, strict=True)
            if key is not None
        }

    def visit_BoolOp(self, node: ast.BoolOp) -> bool:
        values = [bool(self.visit(value)) for value in node.values]
        return all(values) if isinstance(node.op, ast.And) else any(values)

    def visit_UnaryOp(self, node: ast.UnaryOp) -> Any:
        value = self.visit(node.operand)
        if isinstance(node.op, ast.Not):
            return not value
        if isinstance(node.op, ast.USub) and isinstance(value, (int, float)):
            return -value
        raise ExpressionError("Unsupported unary operation")

    def visit_BinOp(self, node: ast.BinOp) -> Any:
        left, right = self.visit(node.left), self.visit(node.right)
        operations: dict[type[ast.operator], Callable[[], Any]] = {
            ast.Add: lambda: left + right,
            ast.Sub: lambda: left - right,
            ast.Mult: lambda: left * right,
            ast.Div: lambda: left / right,
            ast.Mod: lambda: left % right,
        }
        operation = operations.get(type(node.op))
        if operation is None:
            raise ExpressionError("Unsupported arithmetic operation")
        try:
            return operation()
        except (TypeError, ZeroDivisionError) as exc:
            raise ExpressionError("Invalid arithmetic operands") from exc

    def visit_Compare(self, node: ast.Compare) -> bool:
        left = self.visit(node.left)
        for operator, comparator in zip(node.ops, node.comparators, strict=True):
            right = self.visit(comparator)
            try:
                if isinstance(operator, ast.Eq):
                    result = left == right
                elif isinstance(operator, ast.NotEq):
                    result = left != right
                elif isinstance(operator, ast.Lt):
                    result = left < right
                elif isinstance(operator, ast.LtE):
                    result = left <= right
                elif isinstance(operator, ast.Gt):
                    result = left > right
                elif isinstance(operator, ast.GtE):
                    result = left >= right
                elif isinstance(operator, ast.In):
                    result = left in right
                elif isinstance(operator, ast.NotIn):
                    result = left not in right
                else:
                    raise ExpressionError("Unsupported comparison")
            except TypeError as exc:
                raise ExpressionError("Incompatible comparison operands") from exc
            if not result:
                return False
            left = right
        return True
