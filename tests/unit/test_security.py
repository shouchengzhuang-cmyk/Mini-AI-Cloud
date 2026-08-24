import re
from collections.abc import Callable

import pytest

from core.security import (
    generate_api_key,
    hash_api_key,
    hash_password,
    hash_password_async,
    is_argon2id_password_hash,
    normalize_email,
    normalize_project_slug,
    normalize_username,
    parse_api_key_prefix,
    password_needs_rehash,
    verify_api_key,
    verify_password,
    verify_password_async,
)


def test_passwords_use_argon2id_and_never_store_plaintext() -> None:
    password = "correct horse battery staple"

    encoded_hash = hash_password(password)

    assert encoded_hash != password
    assert encoded_hash.startswith("$argon2id$")
    assert is_argon2id_password_hash(encoded_hash) is True
    assert verify_password(password, encoded_hash) is True
    assert verify_password("wrong password", encoded_hash) is False
    assert verify_password(password, "not-a-password-hash") is False
    assert password_needs_rehash(encoded_hash) is False


async def test_async_password_helpers_offload_the_same_primitives() -> None:
    encoded_hash = await hash_password_async("a sufficiently long password")

    assert await verify_password_async("a sufficiently long password", encoded_hash) is True
    assert await verify_password_async("incorrect password", encoded_hash) is False


def test_api_key_is_generated_once_and_database_material_is_only_a_digest() -> None:
    hmac_key = b"a" * 32

    material = generate_api_key(hmac_key, hash_key_id="primary-v1")

    assert re.fullmatch(r"mkc_[a-f0-9]{16}_[A-Za-z0-9_-]{43}", material.token)
    assert len(material.token) == 64
    assert material.prefix == parse_api_key_prefix(material.token)
    assert material.secret_hash == hash_api_key(material.token, hmac_key)
    assert material.token.encode() not in material.secret_hash
    assert material.token not in repr(material)
    assert material.secret_hash.hex() not in repr(material)
    assert verify_api_key(material.token, material.secret_hash, hmac_key) is True
    assert verify_api_key(material.token, material.secret_hash, b"b" * 32) is False
    assert verify_api_key("mkc_invalid", material.secret_hash, hmac_key) is False


def test_api_key_hash_requires_a_high_entropy_hmac_key() -> None:
    with pytest.raises(ValueError, match="at least 32 bytes"):
        generate_api_key(b"short")


@pytest.mark.parametrize(
    ("normalizer", "value", "expected"),
    [
        (normalize_username, " Alice.Example ", "alice.example"),
        (normalize_email, " Alice@Example.COM ", "alice@example.com"),
        (normalize_project_slug, " Demo-Project ", "demo-project"),
    ],
)
def test_identity_normalizers_are_stable(
    normalizer: Callable[[str], str],
    value: str,
    expected: str,
) -> None:
    assert callable(normalizer)
    assert normalizer(value) == expected


@pytest.mark.parametrize(
    ("normalizer", "value"),
    [
        (normalize_username, "bad username"),
        (normalize_email, "missing-at.example.com"),
        (normalize_project_slug, "-bad-prefix"),
    ],
)
def test_identity_normalizers_reject_ambiguous_values(
    normalizer: Callable[[str], str], value: str
) -> None:
    with pytest.raises(ValueError):
        normalizer(value)
