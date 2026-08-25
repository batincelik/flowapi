import asyncio
import json
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import asyncpg  # type: ignore[import-untyped]


class PostgreSQLNodeError(ValueError):
    def __init__(self, code: str, message: str, *, transient: bool = False) -> None:
        self.code = code
        self.transient = transient
        super().__init__(message)


_PARAMETER = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")


def bind_query(query: str, parameters: dict[str, Any]) -> tuple[str, list[Any]]:
    """Convert named parameters to asyncpg positions without interpolating values."""
    names: list[str] = []

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in parameters:
            raise PostgreSQLNodeError("INVALID_SQL_PARAMETERS", f"Missing SQL parameter: {name}")
        if name not in names:
            names.append(name)
        return f"${names.index(name) + 1}"

    bound = _PARAMETER.sub(replace, query)
    unused = set(parameters) - set(names)
    if unused:
        raise PostgreSQLNodeError("INVALID_SQL_PARAMETERS", "Unused SQL parameters are not allowed")
    return bound, [parameters[name] for name in names]


def json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date, Decimal, UUID)):
        return str(value)
    if isinstance(value, bytes):
        return {"type": "binary", "size": len(value)}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): json_value(item) for key, item in value.items()}
    return str(value)


async def execute_postgresql(
    query: str,
    parameters: dict[str, Any],
    credential: dict[str, str],
    *,
    connect_timeout: float,
    query_timeout: float,
    max_output_bytes: int,
) -> dict[str, Any]:
    first_word = query.strip().split(maxsplit=1)[0].upper() if query.strip() else ""
    if first_word not in {"SELECT", "INSERT", "UPDATE"} or ";" in query:
        raise PostgreSQLNodeError("INVALID_SQL", "Only one SELECT, INSERT, or UPDATE statement is allowed")
    bound, values = bind_query(query, parameters)
    connection: Any | None = None
    try:
        connection = await asyncio.wait_for(
            asyncpg.connect(
                host=credential["host"],
                port=int(credential["port"]),
                database=credential["database"],
                user=credential["username"],
                password=credential["password"],
                ssl=credential.get("ssl", "require"),
                command_timeout=query_timeout,
            ),
            timeout=connect_timeout,
        )
        assert connection is not None
        if first_word == "SELECT" or "RETURNING" in query.upper():
            records = await connection.fetch(bound, *values, timeout=query_timeout)
            result: dict[str, Any] = {
                "rows": [json_value(dict(record)) for record in records],
                "row_count": len(records),
            }
        else:
            command = await connection.execute(bound, *values, timeout=query_timeout)
            row_count = int(command.rsplit(" ", 1)[-1]) if command.rsplit(" ", 1)[-1].isdigit() else 0
            result = {"row_count": row_count, "command": first_word}
    except (TimeoutError, asyncpg.TooManyConnectionsError, asyncpg.CannotConnectNowError) as exc:
        raise PostgreSQLNodeError(
            "POSTGRES_TEMPORARY_FAILURE", "PostgreSQL operation temporarily failed", transient=True
        ) from exc
    except (asyncpg.PostgresError, ValueError, KeyError) as exc:
        raise PostgreSQLNodeError("POSTGRES_QUERY_FAILED", "PostgreSQL query failed") from exc
    finally:
        if connection is not None:
            await connection.close(timeout=5)
    if len(json.dumps(result, separators=(",", ":")).encode()) > max_output_bytes:
        raise PostgreSQLNodeError("OUTPUT_TOO_LARGE", "PostgreSQL output exceeds configured limit")
    return result
