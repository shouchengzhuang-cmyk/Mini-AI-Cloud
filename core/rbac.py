import uuid
from dataclasses import dataclass
from enum import StrEnum

from core.enums import ProjectRole


class UserStatus(StrEnum):
    ACTIVE = "active"
    DISABLED = "disabled"


class ProjectStatus(StrEnum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    DELETED = "deleted"


class MembershipStatus(StrEnum):
    ACTIVE = "active"
    REMOVED = "removed"


class PrincipalKind(StrEnum):
    API_KEY = "api_key"
    LEGACY = "legacy"
    SYSTEM = "system"


class Permission(StrEnum):
    TASK_READ = "task.read"
    TASK_LOG_READ = "task.log.read"
    TASK_CREATE = "task.create"
    TASK_CANCEL_OWN = "task.cancel.own"
    TASK_CANCEL_ANY = "task.cancel.any"
    USAGE_READ = "usage.read"
    COST_READ = "cost.read"
    MODEL_READ = "model.read"
    MODEL_MANAGE = "model.manage"
    SECRET_USE = "secret.use"
    SECRET_MANAGE = "secret.manage"
    API_KEY_MANAGE = "api_key.manage"
    QUOTA_MANAGE = "quota.manage"
    IMAGE_POLICY_MANAGE = "image_policy.manage"
    MEMBERSHIP_MANAGE = "membership.manage"
    PROJECT_DELETE = "project.delete"
    AUDIT_READ = "audit.read"
    WORKER_READ = "worker.read"
    WORKER_MANAGE = "worker.manage"


_VIEWER_PERMISSIONS = frozenset(
    {
        Permission.TASK_READ,
        Permission.TASK_LOG_READ,
        Permission.USAGE_READ,
        Permission.COST_READ,
        Permission.MODEL_READ,
    }
)

_MEMBER_PERMISSIONS = _VIEWER_PERMISSIONS | {
    Permission.TASK_CREATE,
    Permission.TASK_CANCEL_OWN,
    Permission.SECRET_USE,
}

_ADMIN_PERMISSIONS = _MEMBER_PERMISSIONS | {
    Permission.TASK_CANCEL_ANY,
    Permission.MODEL_MANAGE,
    Permission.SECRET_MANAGE,
    Permission.API_KEY_MANAGE,
    Permission.QUOTA_MANAGE,
    Permission.IMAGE_POLICY_MANAGE,
    Permission.AUDIT_READ,
    Permission.WORKER_READ,
    Permission.WORKER_MANAGE,
}

ROLE_PERMISSIONS: dict[ProjectRole, frozenset[Permission]] = {
    ProjectRole.VIEWER: _VIEWER_PERMISSIONS,
    ProjectRole.MEMBER: frozenset(_MEMBER_PERMISSIONS),
    ProjectRole.ADMIN: frozenset(_ADMIN_PERMISSIONS),
    ProjectRole.OWNER: frozenset(Permission),
}

_ROLE_AUTHORITY = {
    ProjectRole.VIEWER: 0,
    ProjectRole.MEMBER: 1,
    ProjectRole.ADMIN: 2,
    ProjectRole.OWNER: 3,
}

LEGACY_PERMISSIONS = frozenset(
    {
        Permission.TASK_READ,
        Permission.TASK_LOG_READ,
        Permission.TASK_CREATE,
        Permission.TASK_CANCEL_ANY,
        Permission.WORKER_READ,
    }
)


@dataclass(frozen=True, slots=True)
class Principal:
    kind: PrincipalKind
    project_id: uuid.UUID | None
    user_id: uuid.UUID | None = None
    api_key_id: uuid.UUID | None = None
    role: ProjectRole | None = None
    key_prefix: str | None = None

    def __post_init__(self) -> None:
        if self.kind == PrincipalKind.API_KEY:
            required = (
                self.project_id,
                self.user_id,
                self.api_key_id,
                self.role,
                self.key_prefix,
            )
            if any(value is None for value in required):
                raise ValueError(
                    "an API key principal requires project, user, key, role and prefix"
                )
        elif self.kind == PrincipalKind.LEGACY:
            if self.project_id is None:
                raise ValueError("a legacy principal requires a project")
            if self.api_key_id is not None or self.role is not None or self.key_prefix is not None:
                raise ValueError("a legacy principal cannot carry API key membership data")
        elif self.kind == PrincipalKind.SYSTEM:
            if self.api_key_id is not None or self.key_prefix is not None:
                raise ValueError("a system principal cannot carry API key data")


class PermissionDenied(PermissionError):
    def __init__(self, permission: Permission) -> None:
        super().__init__(f"principal lacks permission: {permission.value}")
        self.permission = permission


class ProjectAccessDenied(PermissionError):
    def __init__(self) -> None:
        super().__init__("principal does not belong to the requested project")


def permissions_for(principal: Principal) -> frozenset[Permission]:
    if principal.kind == PrincipalKind.SYSTEM:
        return frozenset(Permission)
    if principal.kind == PrincipalKind.LEGACY:
        return LEGACY_PERMISSIONS
    if principal.role is None:
        return frozenset()
    return ROLE_PERMISSIONS[principal.role]


def has_permission(principal: Principal, permission: Permission) -> bool:
    return permission in permissions_for(principal)


def require_permission(principal: Principal, permission: Permission) -> None:
    if not has_permission(principal, permission):
        raise PermissionDenied(permission)


def can_access_project(principal: Principal, project_id: uuid.UUID) -> bool:
    return principal.kind == PrincipalKind.SYSTEM or principal.project_id == project_id


def require_project_access(principal: Principal, project_id: uuid.UUID) -> None:
    if not can_access_project(principal, project_id):
        raise ProjectAccessDenied


def can_issue_api_key_for_role(principal: Principal, target_role: ProjectRole) -> bool:
    """Prevent API-key managers from minting a principal above their own authority."""

    if principal.kind == PrincipalKind.SYSTEM:
        return True
    return (
        principal.kind == PrincipalKind.API_KEY
        and principal.role is not None
        and _ROLE_AUTHORITY[principal.role] >= _ROLE_AUTHORITY[target_role]
    )


def can_manage_membership_roles(
    principal: Principal,
    *,
    current_role: ProjectRole | None = None,
    requested_role: ProjectRole | None = None,
) -> bool:
    """Keep membership mutations at or below the caller's role authority."""

    if principal.kind == PrincipalKind.SYSTEM:
        return True
    if principal.kind != PrincipalKind.API_KEY or principal.role is None:
        return False
    caller_authority = _ROLE_AUTHORITY[principal.role]
    return (current_role is None or caller_authority >= _ROLE_AUTHORITY[current_role]) and (
        requested_role is None or caller_authority >= _ROLE_AUTHORITY[requested_role]
    )


def can_cancel_task(principal: Principal, submitted_by_user_id: uuid.UUID | None) -> bool:
    if has_permission(principal, Permission.TASK_CANCEL_ANY):
        return True
    return (
        principal.user_id is not None
        and principal.user_id == submitted_by_user_id
        and has_permission(principal, Permission.TASK_CANCEL_OWN)
    )
