import base64
import uuid

import pytest

from core.config import Settings
from core.secrets import (
    EncryptedSecret,
    SecretCipher,
    SecretDecryptionError,
    SecretKeyConfigurationError,
    SecretKeyRing,
)


def _encoded_key(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def test_key_ring_is_decoded_only_from_the_settings_string() -> None:
    encoded = f"active:{_encoded_key(b'a' * 32)},old:{_encoded_key(b'b' * 32)}"
    settings = Settings(secret_master_key=encoded)

    ring = SecretKeyRing.from_settings(settings)

    assert ring.active_key_id == "active"
    assert ring.active_key() == b"a" * 32
    assert "aaaaaaaa" not in repr(ring)


@pytest.mark.parametrize(
    "encoded",
    ["", "missing-separator", "v1:not-base64!", "v1:YQ", "v1:YQ==,v1:Yg=="],
)
def test_key_ring_rejects_missing_malformed_or_non_256_bit_keys(encoded: str) -> None:
    with pytest.raises(SecretKeyConfigurationError):
        SecretKeyRing.from_encoded(encoded)


def test_aes_gcm_round_trip_binds_ciphertext_to_project_secret_and_version() -> None:
    cipher = SecretCipher(SecretKeyRing.from_encoded(f"v1:{_encoded_key(b'k' * 32)}"))
    project_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    plaintext = "database-password-123"

    encrypted = cipher.encrypt(
        plaintext,
        project_id=project_id,
        secret_id=secret_id,
        version=1,
    )

    assert plaintext.encode() not in encrypted.ciphertext
    assert plaintext not in repr(encrypted)
    decrypted = cipher.decrypt(
        encrypted,
        project_id=project_id,
        secret_id=secret_id,
        version=1,
    )
    assert decrypted.value == plaintext
    assert plaintext not in repr(decrypted)

    with pytest.raises(SecretDecryptionError):
        cipher.decrypt(
            encrypted,
            project_id=uuid.uuid4(),
            secret_id=secret_id,
            version=1,
        )


def test_key_rotation_keeps_old_versions_readable_without_reusing_the_old_key() -> None:
    old = SecretCipher(SecretKeyRing.from_encoded(f"v1:{_encoded_key(b'1' * 32)}"))
    project_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    encrypted_old = old.encrypt(
        "old-value",
        project_id=project_id,
        secret_id=secret_id,
        version=1,
    )
    rotated = SecretCipher(
        SecretKeyRing.from_encoded(f"v2:{_encoded_key(b'2' * 32)},v1:{_encoded_key(b'1' * 32)}")
    )
    encrypted_new = rotated.encrypt(
        "new-value",
        project_id=project_id,
        secret_id=secret_id,
        version=2,
    )

    assert encrypted_old.key_id == "v1"
    assert encrypted_new.key_id == "v2"
    assert (
        rotated.decrypt(
            encrypted_old,
            project_id=project_id,
            secret_id=secret_id,
            version=1,
        ).value
        == "old-value"
    )

    tampered = EncryptedSecret(
        ciphertext=encrypted_new.ciphertext[:-1] + bytes([encrypted_new.ciphertext[-1] ^ 1]),
        nonce=encrypted_new.nonce,
        key_id=encrypted_new.key_id,
    )
    with pytest.raises(SecretDecryptionError):
        rotated.decrypt(
            tampered,
            project_id=project_id,
            secret_id=secret_id,
            version=2,
        )
