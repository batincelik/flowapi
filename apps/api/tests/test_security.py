import pytest
from cryptography.fernet import Fernet
from flowapi.security import CredentialCipher, SSRFBlocked, is_forbidden_address, validate_outbound_url


def test_credential_authenticated_encryption() -> None:
    cipher = CredentialCipher(Fernet.generate_key().decode())
    encrypted = cipher.encrypt(b"very-secret")
    assert b"very-secret" not in encrypted
    assert cipher.decrypt(encrypted) == b"very-secret"
    with pytest.raises(ValueError):
        cipher.decrypt(encrypted[:-2] + b"xx")


@pytest.mark.parametrize(
    "address",
    ["127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.1.1", "169.254.169.254", "::1", "fc00::1", "fe80::1"],
)
def test_private_and_metadata_addresses_are_forbidden(address: str) -> None:
    assert is_forbidden_address(address)


@pytest.mark.parametrize("url", ["file:///etc/passwd", "ftp://example.com/a", "http://user:pass@example.com"])
async def test_unsafe_urls_are_blocked(url: str) -> None:
    with pytest.raises(SSRFBlocked):
        await validate_outbound_url(url)


async def test_dns_to_private_address_is_blocked() -> None:
    async def private_resolver(host: str, port: int) -> list[str]:
        return ["10.0.0.5"]

    with pytest.raises(SSRFBlocked):
        await validate_outbound_url("https://public-looking.example", resolver=private_resolver)


async def test_public_resolution_is_allowed() -> None:
    async def public_resolver(host: str, port: int) -> list[str]:
        return ["93.184.216.34"]

    assert await validate_outbound_url("https://example.com", resolver=public_resolver) == ["93.184.216.34"]
