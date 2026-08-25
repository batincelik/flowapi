from flowapi.auth import hash_password, token_hash, verify_password


def test_passwords_use_argon2id_and_verify() -> None:
    encoded = hash_password("correct horse battery staple")
    assert encoded.startswith("$argon2id$")
    assert "correct horse" not in encoded
    assert verify_password(encoded, "correct horse battery staple")
    assert not verify_password(encoded, "wrong password")


def test_short_password_is_rejected() -> None:
    try:
        hash_password("short")
    except ValueError as exc:
        assert "12 characters" in str(exc)
    else:
        raise AssertionError("short password was accepted")


def test_session_tokens_are_only_stored_as_hashes() -> None:
    raw = "session-secret"
    hashed = token_hash(raw)
    assert raw not in hashed
    assert len(hashed) == 64
