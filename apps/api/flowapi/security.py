import ipaddress
import json
import socket
from collections.abc import Awaitable, Callable
from urllib.parse import urlparse

from cryptography.fernet import Fernet, InvalidToken


class CredentialCipher:
    def __init__(self, key: str) -> None:
        self._fernet = Fernet(key.encode())

    def encrypt(self, plaintext: bytes) -> bytes:
        return self._fernet.encrypt(plaintext)

    def decrypt(self, ciphertext: bytes) -> bytes:
        try:
            return self._fernet.decrypt(ciphertext)
        except InvalidToken as exc:
            raise ValueError("Credential cannot be decrypted") from exc

    def encrypt_json(self, value: dict[str, str]) -> bytes:
        return self.encrypt(json.dumps(value, separators=(",", ":"), sort_keys=True).encode())

    def decrypt_json(self, ciphertext: bytes) -> dict[str, str]:
        value = json.loads(self.decrypt(ciphertext))
        if not isinstance(value, dict) or not all(
            isinstance(key, str) and isinstance(item, str) for key, item in value.items()
        ):
            raise ValueError("Credential payload is invalid")
        return value


class SSRFBlocked(ValueError):
    code = "SSRF_BLOCKED"


def is_forbidden_address(address: str) -> bool:
    ip = ipaddress.ip_address(address)
    return not ip.is_global


async def validate_outbound_url(
    url: str,
    *,
    allow_private: bool = False,
    resolver: Callable[[str, int], Awaitable[list[str]]] | None = None,
) -> list[str]:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise SSRFBlocked("Only absolute HTTP and HTTPS URLs are allowed")
    if parsed.username or parsed.password:
        raise SSRFBlocked("URL userinfo is forbidden")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    addresses = await (resolver or resolve_host)(parsed.hostname, port)
    if not addresses:
        raise SSRFBlocked("Hostname did not resolve")
    if not allow_private and any(is_forbidden_address(address) for address in addresses):
        raise SSRFBlocked("Destination resolves to a non-public address")
    return addresses


async def resolve_host(hostname: str, port: int) -> list[str]:
    import asyncio

    records = await asyncio.get_running_loop().getaddrinfo(
        hostname, port, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM
    )
    return sorted({str(record[4][0]) for record in records})
