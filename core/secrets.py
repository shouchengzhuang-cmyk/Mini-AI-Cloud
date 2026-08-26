import base64
import binascii
import re
import secrets
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from core.config import Settings

_KEY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_MAX_SECRET_BYTES = 65_536
_NONCE_BYTES = 12
_AAD_DOMAIN = b"mini-ai-cloud/secret/aes256gcm/v1\x00"


class SecretKeyConfigurationError(ValueError):
    pass


class SecretDecryptionError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class SecretKeyRing:
    active_key_id: str
    _keys: Mapping[str, bytes] = field(repr=False)

    @classmethod
    def from_settings(cls, settings: Settings) -> "SecretKeyRing":
        return cls.from_encoded(settings.secret_master_key)

    @classmethod
    def from_encoded(cls, encoded: str) -> "SecretKeyRing":
        """Decode `key-id:base64-key,...`; the first key is active for writes."""

        if not encoded or not encoded.strip():
            raise SecretKeyConfigurationError("SECRET_MASTER_KEY must configure a key ring")
        parsed: dict[str, bytes] = {}
        active_key_id: str | None = None
        for raw_entry in encoded.split(","):
            key_id, separator, encoded_key = raw_entry.strip().partition(":")
            if not separator or not _KEY_ID.fullmatch(key_id):
                raise SecretKeyConfigurationError(
                    "SECRET_MASTER_KEY entries must use key-id:base64-key"
                )
            if key_id in parsed:
                raise SecretKeyConfigurationError("SECRET_MASTER_KEY contains a duplicate key id")
            key = _decode_key(encoded_key)
            if len(key) != 32:
                raise SecretKeyConfigurationError(
                    "SECRET_MASTER_KEY entries must decode to exactly 32 bytes"
                )
            if active_key_id is None:
                active_key_id = key_id
            parsed[key_id] = key
        if active_key_id is None:
            raise SecretKeyConfigurationError("SECRET_MASTER_KEY must configure a key ring")
        return cls(active_key_id=active_key_id, _keys=MappingProxyType(parsed))

    def active_key(self) -> bytes:
        return self._keys[self.active_key_id]

    def get(self, key_id: str) -> bytes | None:
        return self._keys.get(key_id)


@dataclass(frozen=True, slots=True)
class EncryptedSecret:
    ciphertext: bytes = field(repr=False)
    nonce: bytes = field(repr=False)
    key_id: str


@dataclass(frozen=True, slots=True)
class SecretValue:
    value: str = field(repr=False)


class SecretCipher:
    def __init__(self, key_ring: SecretKeyRing) -> None:
        self._key_ring = key_ring

    @classmethod
    def from_settings(cls, settings: Settings) -> "SecretCipher":
        return cls(SecretKeyRing.from_settings(settings))

    def encrypt(
        self,
        value: str,
        *,
        project_id: uuid.UUID,
        secret_id: uuid.UUID,
        version: int,
    ) -> EncryptedSecret:
        plaintext = _encode_secret(value)
        nonce = secrets.token_bytes(_NONCE_BYTES)
        ciphertext = AESGCM(self._key_ring.active_key()).encrypt(
            nonce,
            plaintext,
            _associated_data(project_id, secret_id, version),
        )
        return EncryptedSecret(
            ciphertext=ciphertext,
            nonce=nonce,
            key_id=self._key_ring.active_key_id,
        )

    def decrypt(
        self,
        encrypted: EncryptedSecret,
        *,
        project_id: uuid.UUID,
        secret_id: uuid.UUID,
        version: int,
    ) -> SecretValue:
        key = self._key_ring.get(encrypted.key_id)
        if key is None or len(encrypted.nonce) != _NONCE_BYTES:
            raise SecretDecryptionError("secret cannot be decrypted")
        try:
            plaintext = AESGCM(key).decrypt(
                encrypted.nonce,
                encrypted.ciphertext,
                _associated_data(project_id, secret_id, version),
            )
            return SecretValue(plaintext.decode("utf-8"))
        except (InvalidTag, UnicodeDecodeError) as exc:
            raise SecretDecryptionError("secret cannot be decrypted") from exc


def _decode_key(encoded: str) -> bytes:
    if not encoded or not re.fullmatch(r"[A-Za-z0-9+/_-]+={0,2}", encoded):
        raise SecretKeyConfigurationError("SECRET_MASTER_KEY contains invalid base64")
    padded = encoded + "=" * (-len(encoded) % 4)
    try:
        return base64.b64decode(padded, altchars=b"-_", validate=True)
    except (binascii.Error, ValueError) as exc:
        raise SecretKeyConfigurationError("SECRET_MASTER_KEY contains invalid base64") from exc


def _encode_secret(value: str) -> bytes:
    if "\x00" in value:
        raise ValueError("secret value must not contain NUL")
    encoded = value.encode("utf-8")
    if not encoded:
        raise ValueError("secret value must not be empty")
    if len(encoded) > _MAX_SECRET_BYTES:
        raise ValueError(f"secret value must be at most {_MAX_SECRET_BYTES} encoded bytes")
    return encoded


def _associated_data(project_id: uuid.UUID, secret_id: uuid.UUID, version: int) -> bytes:
    if version < 1:
        raise ValueError("secret version must be positive")
    return _AAD_DOMAIN + project_id.bytes + secret_id.bytes + version.to_bytes(8, "big")
