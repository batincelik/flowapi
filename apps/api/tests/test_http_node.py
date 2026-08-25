import pytest
from flowapi.http_node import HTTPNodeError, request
from flowapi.security import SSRFBlocked


class FakeReader:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    async def read(self, size: int) -> bytes:
        payload, self.payload = self.payload, b""
        return payload


class FakeWriter:
    def write(self, data: bytes) -> None:
        pass

    async def drain(self) -> None:
        pass

    def close(self) -> None:
        pass

    async def wait_closed(self) -> None:
        pass


def install_transport(monkeypatch: pytest.MonkeyPatch, payload: bytes) -> None:
    async def open_connection(*args: object, **kwargs: object) -> tuple[FakeReader, FakeWriter]:
        return FakeReader(payload), FakeWriter()

    async def public_resolution(url: str, **kwargs: object) -> list[str]:
        return ["93.184.216.34"]

    monkeypatch.setattr("flowapi.http_node.asyncio.open_connection", open_connection)
    monkeypatch.setattr("flowapi.http_node.validate_outbound_url", public_resolution)


async def test_http_transport_parses_bounded_response(monkeypatch: pytest.MonkeyPatch) -> None:
    install_transport(monkeypatch, b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\nhello world")
    result = await request("GET", "http://example.com/fixture")
    assert result.status == 200
    assert result.body == "hello world"


async def test_response_limit_fails_without_truncating(monkeypatch: pytest.MonkeyPatch) -> None:
    install_transport(monkeypatch, b"HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\n01234567890123456789")
    with pytest.raises(HTTPNodeError, match="exceeded") as captured:
        await request("GET", "http://example.com/large", max_response_bytes=10)
    assert captured.value.code == "RESPONSE_TOO_LARGE"


async def test_private_target_is_blocked_by_default() -> None:
    with pytest.raises(HTTPNodeError) as captured:
        await request("GET", "http://127.0.0.1/")
    assert captured.value.code == "SSRF_BLOCKED"


async def test_redirect_destination_is_revalidated(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1/private\r\nContent-Length: 0\r\n\r\n"
    install_transport(monkeypatch, payload)
    calls: list[str] = []

    async def controlled_validation(url: str, **kwargs: object) -> list[str]:
        calls.append(url)
        if len(calls) == 1:
            return ["93.184.216.34"]
        raise SSRFBlocked("redirect resolved to private address")

    monkeypatch.setattr("flowapi.http_node.validate_outbound_url", controlled_validation)
    with pytest.raises(HTTPNodeError) as captured:
        await request("GET", "http://public.example/redirect")
    assert captured.value.code == "SSRF_BLOCKED"
    assert calls == ["http://public.example/redirect", "http://127.0.0.1/private"]
