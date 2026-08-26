import uuid
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from api.schemas.identity import (
    ApiKeyCreate,
    ApiKeyCreated,
    ApiKeyResponse,
    ProjectCreate,
    UserCreate,
)


def test_user_create_hides_password_and_normalizes_visible_identifiers() -> None:
    payload = UserCreate.model_validate(
        {
            "username": " Alice.Example ",
            "email": " Alice@Example.COM ",
            "password": "correct horse battery staple",
        }
    )

    assert payload.username == "Alice.Example"
    assert payload.email == "Alice@Example.COM"
    assert payload.password.get_secret_value() == "correct horse battery staple"
    assert "correct horse battery staple" not in repr(payload)


@pytest.mark.parametrize(
    "payload",
    [
        {"username": "bad name", "email": "a@example.com", "password": "long enough password"},
        {"username": "valid-name", "email": "missing-at", "password": "long enough password"},
        {"username": "valid-name", "email": "a@example.com", "password": "short"},
    ],
)
def test_user_create_rejects_invalid_identity_input(payload: dict[str, str]) -> None:
    with pytest.raises(ValidationError):
        UserCreate.model_validate(payload)


def test_project_create_returns_a_canonical_slug() -> None:
    project = ProjectCreate(name=" Demo Project ", slug=" Demo-Project ")

    assert project.name == "Demo Project"
    assert project.slug == "demo-project"


def test_api_key_create_requires_timezone_aware_expiration() -> None:
    with pytest.raises(ValidationError, match="must include a timezone"):
        ApiKeyCreate(name="automation", expires_at=datetime.now())

    expires_at = datetime.now(UTC) + timedelta(days=1)
    assert ApiKeyCreate(name="automation", expires_at=expires_at).expires_at == expires_at


def test_api_key_secret_exists_only_on_one_time_created_response() -> None:
    now = datetime.now(UTC)
    shared = {
        "id": uuid.uuid4(),
        "project_id": uuid.uuid4(),
        "user_id": uuid.uuid4(),
        "name": "automation",
        "key_prefix": "mkc_0123456789abcdef",
        "created_by_user_id": uuid.uuid4(),
        "created_at": now,
        "expires_at": None,
        "revoked_at": None,
        "last_used_at": None,
        "version": 1,
    }
    token = "mkc_0123456789abcdef_" + "x" * 43

    created = ApiKeyCreated(**shared, api_key=token)
    regular = ApiKeyResponse(**shared)

    assert created.api_key == token
    assert token not in repr(created)
    assert "api_key" not in regular.model_dump()
    assert "secret_hash" not in created.model_dump()
