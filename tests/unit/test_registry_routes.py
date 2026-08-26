import uuid

import pytest
from starlette.requests import Request

from api.errors import APIError, NotFoundError
from api.routes.registry import _authorize
from core.rbac import Permission, Principal, PrincipalKind, ProjectRole


def _request(principal: Principal | None) -> Request:
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    if principal is not None:
        request.state.principal = principal
    return request


def _principal(project_id: uuid.UUID, role: ProjectRole) -> Principal:
    return Principal(
        kind=PrincipalKind.API_KEY,
        project_id=project_id,
        user_id=uuid.uuid4(),
        api_key_id=uuid.uuid4(),
        role=role,
        key_prefix="mkc_0123456789abcdef",
    )


def test_registry_routes_require_authentication_and_hide_cross_project_ids() -> None:
    project_id = uuid.uuid4()
    with pytest.raises(APIError) as missing:
        _authorize(_request(None), project_id, Permission.MODEL_READ)
    assert missing.value.status_code == 401

    with pytest.raises(NotFoundError) as cross_project:
        _authorize(
            _request(_principal(uuid.uuid4(), ProjectRole.OWNER)),
            project_id,
            Permission.MODEL_READ,
        )
    assert cross_project.value.status_code == 404


def test_registry_routes_enforce_the_central_rbac_matrix() -> None:
    project_id = uuid.uuid4()
    viewer = _request(_principal(project_id, ProjectRole.VIEWER))
    admin = _request(_principal(project_id, ProjectRole.ADMIN))

    assert _authorize(viewer, project_id, Permission.MODEL_READ).role == ProjectRole.VIEWER
    with pytest.raises(APIError) as forbidden:
        _authorize(viewer, project_id, Permission.SECRET_MANAGE)
    assert forbidden.value.status_code == 403
    assert _authorize(admin, project_id, Permission.SECRET_MANAGE).role == ProjectRole.ADMIN
