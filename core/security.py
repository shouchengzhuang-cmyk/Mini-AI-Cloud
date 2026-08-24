import asyncio
import hashlib
import hmac
import re
import secrets
from dataclasses import dataclass, field

from argon2 import PasswordHasher, extract_parameters
from argon2.exceptions import InvalidHashError, VerificationError
from argon2.low_level import Type

_USERNAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{2,63}$")
_PROJECT_SLUG = re.compile(r"^[a-z0-9][a-z0-9-]{1,62}$")
_API_KEY = re.compile(r"^mkc_(?P<public_id>[a-f0-9]{16})_(?P<secret>[A-Za-z0-9_-]{43})$")
_HASH_KEY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_PASSWORD_MAX_BYTES = 1024
_API_KEY_HMAC_MIN_BYTES = 32

_PASSWORD_HASHER = PasswordHasher()


@dataclass(frozen=True, slots=True)
class ApiKeyMaterial:
    token: str = field(repr=False)
    prefix: str
    secret_hash: bytes = field(repr=False)
    hash_key_id: str


def normalize_username(value: str) -> str:
    username = value.strip()
    if not _USERNAME.fullmatch(username):
        raise ValueError(
            "username must be 3-64 characters and use letters, digits, '.', '_' or '-'"
        )
    return username.casefold()


def normalize_email(value: str) -> str:
    email = value.strip()
    if len(email) > 320 or email.count("@") != 1:
        raise ValueError("email must contain one '@' and be at most 320 characters")
    local, domain = email.rsplit("@", 1)
    if not local or len(local) > 64 or not domain or len(domain) > 255:
        raise ValueError("email local and domain parts must be non-empty and within length limits")
    if any(
        character.isspace() or ord(character) < 32 or ord(character) == 127 for character in email
    ):
        raise ValueError("email must not contain whitespace or control characters")
    if domain.startswith(".") or domain.endswith(".") or ".." in domain:
        raise ValueError("email domain is malformed")
    return email.casefold()


def normalize_project_slug(value: str) -> str:
    slug = value.strip().casefold()
    if not _PROJECT_SLUG.fullmatch(slug):
        raise ValueError(
            "project slug must be 2-63 lowercase letters, digits or '-' and start alphanumeric"
        )
    return slug


def hash_password(password: str) -> str:
    _validate_password_input(password)
    return _PASSWORD_HASHER.hash(password)


def verify_password(password: str, encoded_hash: str) -> bool:
    try:
        _validate_password_input(password)
        return bool(_PASSWORD_HASHER.verify(encoded_hash, password))
    except (InvalidHashError, VerificationError, ValueError):
        return False


def password_needs_rehash(encoded_hash: str) -> bool:
    try:
        return _PASSWORD_HASHER.check_needs_rehash(encoded_hash)
    except (InvalidHashError, ValueError):
        return True


def is_argon2id_password_hash(encoded_hash: str) -> bool:
    try:
        return extract_parameters(encoded_hash).type == Type.ID
    except (InvalidHashError, ValueError):
        return False


async def hash_password_async(password: str) -> str:
    return await asyncio.to_thread(hash_password, password)


async def verify_password_async(password: str, encoded_hash: str) -> bool:
    return await asyncio.to_thread(verify_password, password, encoded_hash)


def generate_api_key(hmac_key: bytes, *, hash_key_id: str = "v1") -> ApiKeyMaterial:
    _validate_hmac_key(hmac_key)
    _validate_hash_key_id(hash_key_id)
    public_id = secrets.token_hex(8)
    token = f"mkc_{public_id}_{secrets.token_urlsafe(32)}"
    prefix = f"mkc_{public_id}"
    return ApiKeyMaterial(
        token=token,
        prefix=prefix,
        secret_hash=_digest_api_key(token, hmac_key),
        hash_key_id=hash_key_id,
    )


def parse_api_key_prefix(token: str) -> str:
    match = _API_KEY.fullmatch(token)
    if match is None:
        raise ValueError("API key must use the mkc_<public-id>_<secret> format")
    return f"mkc_{match.group('public_id')}"


def hash_api_key(token: str, hmac_key: bytes) -> bytes:
    _validate_hmac_key(hmac_key)
    parse_api_key_prefix(token)
    return _digest_api_key(token, hmac_key)


def verify_api_key(token: str, expected_hash: bytes, hmac_key: bytes) -> bool:
    try:
        actual_hash = hash_api_key(token, hmac_key)
    except ValueError:
        return False
    return hmac.compare_digest(actual_hash, expected_hash)


def _digest_api_key(token: str, hmac_key: bytes) -> bytes:
    return hmac.new(hmac_key, token.encode("ascii"), hashlib.sha256).digest()


def _validate_password_input(password: str) -> None:
    encoded = password.encode("utf-8")
    if not encoded:
        raise ValueError("password must not be empty")
    if len(encoded) > _PASSWORD_MAX_BYTES:
        raise ValueError(f"password must be at most {_PASSWORD_MAX_BYTES} encoded bytes")
    if "\x00" in password:
        raise ValueError("password must not contain NUL")


def _validate_hmac_key(hmac_key: bytes) -> None:
    if len(hmac_key) < _API_KEY_HMAC_MIN_BYTES:
        raise ValueError(f"API key HMAC key must be at least {_API_KEY_HMAC_MIN_BYTES} bytes")


def _validate_hash_key_id(hash_key_id: str) -> None:
    if not _HASH_KEY_ID.fullmatch(hash_key_id):
        raise ValueError("hash_key_id must be 1-64 safe identifier characters")
