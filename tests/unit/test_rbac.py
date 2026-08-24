import uuid

import pytest

from core.rbac import (
    Permission,
    PermissionDenied,
    Principal,
    PrincipalKind,
    ProjectAccessDenied,
    ProjectRole,
    can_access_project,
    can_cancel_task,
    can_issue_api_key_for_role,
    can_manage_membership_roles,
    has_permission,
    require_permission,
    require_project_access,
)


def _api_key_principal(role: ProjectRole) -> Principal:
    return Principal(
        kind=PrincipalKind.API_KEY,
        project_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        api_key_id=uuid.uuid4(),
        role=role,
        key_prefix="mkc_0123456789abcdef",
    )


@pytest.mark.parametrize(
    ("role", "allowed", "denied"),
    [
        (ProjectRole.VIEWER, Permission.TASK_READ, Permission.TASK_CREATE),
        (ProjectRole.MEMBER, Permission.TASK_CREATE, Permission.API_KEY_MANAGE),
        (ProjectRole.ADMIN, Permission.QUOTA_MANAGE, Permission.MEMBERSHIP_MANAGE),
        (ProjectRole.OWNER, Permission.PROJECT_DELETE, None),
    ],
)
def test_role_permission_matrix(
    role: ProjectRole,
    allowed: Permission,
    denied: Permission | None,
) -> None:
    principal = _api_key_principal(role)

    assert has_permission(principal, allowed) is True
    require_permission(principal, allowed)
    if denied is not None:
        assert has_permission(principal, denied) is False
        with pytest.raises(PermissionDenied):
            require_permission(principal, denied)


def test_project_access_is_explicit_and_system_only_can_cross_projects() -> None:
    principal = _api_key_principal(ProjectRole.ADMIN)
    other_project_id = uuid.uuid4()

    assert principal.project_id is not None
    assert can_access_project(principal, principal.project_id) is True
    assert can_access_project(principal, other_project_id) is False
    with pytest.raises(ProjectAccessDenied):
        require_project_access(principal, other_project_id)

    system = Principal(kind=PrincipalKind.SYSTEM, project_id=None)
    assert can_access_project(system, other_project_id) is True


def test_member_can_cancel_only_own_task_while_admin_can_cancel_any() -> None:
    member = _api_key_principal(ProjectRole.MEMBER)
    admin = _api_key_principal(ProjectRole.ADMIN)

    assert can_cancel_task(member, member.user_id) is True
    assert can_cancel_task(member, uuid.uuid4()) is False
    assert can_cancel_task(admin, uuid.uuid4()) is True


def test_api_key_role_delegation_cannot_exceed_caller_authority() -> None:
    admin = _api_key_principal(ProjectRole.ADMIN)
    owner = _api_key_principal(ProjectRole.OWNER)

    assert can_issue_api_key_for_role(admin, ProjectRole.MEMBER)
    assert can_issue_api_key_for_role(admin, ProjectRole.ADMIN)
    assert not can_issue_api_key_for_role(admin, ProjectRole.OWNER)
    assert can_issue_api_key_for_role(owner, ProjectRole.OWNER)


def test_membership_role_management_cannot_exceed_caller_authority() -> None:
    admin = _api_key_principal(ProjectRole.ADMIN)
    owner = _api_key_principal(ProjectRole.OWNER)

    assert can_manage_membership_roles(
        admin,
        current_role=ProjectRole.MEMBER,
        requested_role=ProjectRole.ADMIN,
    )
    assert not can_manage_membership_roles(admin, current_role=ProjectRole.OWNER)
    assert not can_manage_membership_roles(admin, requested_role=ProjectRole.OWNER)
    assert can_manage_membership_roles(
        owner,
        current_role=ProjectRole.OWNER,
        requested_role=ProjectRole.OWNER,
    )


def test_legacy_principal_has_only_the_legacy_task_surface() -> None:
    legacy = Principal(kind=PrincipalKind.LEGACY, project_id=uuid.uuid4())

    assert has_permission(legacy, Permission.TASK_CREATE) is True
    assert has_permission(legacy, Permission.TASK_CANCEL_ANY) is True
    assert has_permission(legacy, Permission.API_KEY_MANAGE) is False


def test_malformed_api_key_principal_is_rejected() -> None:
    with pytest.raises(ValueError, match="requires project"):
        Principal(kind=PrincipalKind.API_KEY, project_id=None)
