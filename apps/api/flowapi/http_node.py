import asyncio
import json
import ssl
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlsplit

import h11

from .security import SSRFBlocked, validate_outbound_url


class HTTPNodeError(Exception):
    def __init__(self, code: str, message: str, *, transient: bool = False) -> None:
        self.code, self.transient = code, transient
        super().__init__(message)


@dataclass(frozen=True)
class HTTPResult:
    status: int
    headers: dict[str, str]
    body: str
    duration_ms: int
    url: str


def _safe_headers(headers: dict[str, Any]) -> list[tuple[bytes, bytes]]:
    output: list[tuple[bytes, bytes]] = []
    for raw_name, raw_value in headers.items():
        name, value = str(raw_name), str(raw_value)
        if "\r" in name or "\n" in name or "\r" in value or "\n" in value:
            raise HTTPNodeError("INVALID_CONFIGURATION", "HTTP headers may not contain newlines")
        output.append((name.encode("ascii"), value.encode("utf-8")))
    return output


async def request(
    method: str,
    url: str,
    *,
    headers: dict[str, Any] | None = None,
    body: Any = None,
    timeout_seconds: float = 30,
    max_response_bytes: int = 5_242_880,
    follow_redirects: bool = True,
    max_redirects: int = 10,
    allow_private: bool = False,
) -> HTTPResult:
    method = method.upper()
    if method not in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"}:
        raise HTTPNodeError("INVALID_CONFIGURATION", f"Unsupported HTTP method: {method}")
    started = time.monotonic()
    current_url = url
    for redirect_count in range(max_redirects + 1):
        result = await _single_request(
            method,
            current_url,
            headers=headers or {},
            body=body,
            timeout_seconds=timeout_seconds,
            max_response_bytes=max_response_bytes,
            allow_private=allow_private,
        )
        location = result.headers.get("location")
        if result.status not in {301, 302, 303, 307, 308} or not location or not follow_redirects:
            return HTTPResult(
                status=result.status,
                headers=result.headers,
                body=result.body,
                duration_ms=int((time.monotonic() - started) * 1000),
                url=current_url,
            )
        if redirect_count == max_redirects:
            raise HTTPNodeError("TOO_MANY_REDIRECTS", "HTTP redirect limit exceeded")
        current_url = urljoin(current_url, location)
        # 303 always becomes GET; common 301/302 POST behavior is made explicit.
        if result.status == 303 or (result.status in {301, 302} and method == "POST"):
            method, body = "GET", None
    raise AssertionError("redirect loop invariant")


async def _single_request(
    method: str,
    url: str,
    *,
    headers: dict[str, Any],
    body: Any,
    timeout_seconds: float,
    max_response_bytes: int,
    allow_private: bool,
) -> HTTPResult:
    try:
        addresses = await validate_outbound_url(url, allow_private=allow_private)
    except SSRFBlocked as exc:
        raise HTTPNodeError("SSRF_BLOCKED", str(exc)) from exc
    parsed = urlsplit(url)
    hostname = parsed.hostname
    if hostname is None:
        raise HTTPNodeError("INVALID_CONFIGURATION", "URL hostname is missing")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    target = parsed.path or "/"
    if parsed.query:
        target += "?" + parsed.query
    request_headers = dict(headers)
    request_headers.setdefault("Host", hostname if parsed.port is None else f"{hostname}:{port}")
    request_headers.setdefault("User-Agent", "FlowAPI/0.1")
    request_headers.setdefault("Accept", "application/json, text/plain, */*")
    if body is None:
        body_bytes = b""
    elif isinstance(body, str):
        body_bytes = body.encode()
    else:
        body_bytes = json.dumps(body, separators=(",", ":")).encode()
        request_headers.setdefault("Content-Type", "application/json")
    request_headers["Content-Length"] = str(len(body_bytes))
    context = ssl.create_default_context() if parsed.scheme == "https" else None
    connection = h11.Connection(h11.CLIENT)
    try:
        async with asyncio.timeout(timeout_seconds):
            # Connect to the already validated address, not the hostname. TLS still verifies
            # and sends SNI for the original public hostname.
            reader, writer = await asyncio.open_connection(
                addresses[0], port, ssl=context, server_hostname=hostname if context else None
            )
            try:
                writer.write(
                    connection.send(
                        h11.Request(
                            method=method.encode(), target=target.encode(), headers=_safe_headers(request_headers)
                        )
                    )
                )
                if body_bytes:
                    writer.write(connection.send(h11.Data(data=body_bytes)))
                writer.write(connection.send(h11.EndOfMessage()))
                await writer.drain()
                status = 0
                response_headers: dict[str, str] = {}
                chunks = bytearray()
                while True:
                    event = connection.next_event()
                    if event is h11.NEED_DATA:
                        data = await reader.read(65_536)
                        connection.receive_data(data)
                        if not data and connection.their_state is not h11.DONE:
                            raise HTTPNodeError(
                                "NETWORK_ERROR", "Remote server closed an incomplete response", transient=True
                            )
                        continue
                    if isinstance(event, h11.Response):
                        status = event.status_code
                        response_headers = {
                            name.decode("latin-1").lower(): value.decode("latin-1") for name, value in event.headers
                        }
                    elif isinstance(event, h11.Data):
                        if len(chunks) + len(event.data) > max_response_bytes:
                            raise HTTPNodeError("RESPONSE_TOO_LARGE", "HTTP response exceeded configured limit")
                        chunks.extend(event.data)
                    elif isinstance(event, h11.EndOfMessage):
                        break
                if status == 0:
                    raise HTTPNodeError("INVALID_RESPONSE", "Remote server returned no HTTP response")
                return HTTPResult(status, response_headers, chunks.decode("utf-8", errors="replace"), 0, url)
            finally:
                writer.close()
                await writer.wait_closed()
    except TimeoutError as exc:
        raise HTTPNodeError("HTTP_TIMEOUT", "HTTP request timed out", transient=True) from exc
    except (OSError, h11.ProtocolError) as exc:
        raise HTTPNodeError("NETWORK_ERROR", "HTTP transport failed", transient=True) from exc
