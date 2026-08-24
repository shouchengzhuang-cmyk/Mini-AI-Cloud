from __future__ import annotations

from dataclasses import dataclass

from fastapi import Request, Response

from core.logging import get_logger
from core.rbac import Principal
from repositories.audit import AuditRepository

LOGGER = get_logger("audit")


@dataclass(frozen=True, slots=True)
class AuditSpec:
    action: str
    resource_type: str
    resource_params: tuple[str, ...] = ()


AUDITED_IDENTITY_WRITES: dict[tuple[str, str], AuditSpec] = {
    ("POST", "/api/v1/bootstrap"): AuditSpec("identity.bootstrap", "platform"),
    ("POST", "/api/v1/users"): AuditSpec("user.create", "user"),
    ("POST", "/api/v1/projects"): AuditSpec("project.create", "project"),
    ("POST", "/api/v1/projects/{project_id}/members"): AuditSpec(
        "membership.create", "membership", ("project_id",)
    ),
    ("PATCH", "/api/v1/projects/{project_id}/members/{user_id}"): AuditSpec(
        "membership.role.update", "membership", ("project_id", "user_id")
    ),
    ("DELETE", "/api/v1/projects/{project_id}/members/{user_id}"): AuditSpec(
        "membership.remove", "membership", ("project_id", "user_id")
    ),
    ("POST", "/api/v1/projects/{project_id}/api-keys"): AuditSpec(
        "api_key.issue", "api_key", ("project_id",)
    ),
    ("DELETE", "/api/v1/projects/{project_id}/api-keys/{api_key_id}"): AuditSpec(
        "api_key.revoke", "api_key", ("api_key_id",)
    ),
}

MUTATING_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
RESOURCE_NAMES = {
    "api-keys": "api_key",
    "artifacts": "artifact",
    "datasets": "dataset",
    "job-groups": "job_group",
    "models": "model",
    "projects": "project",
    "quotas": "quota",
    "registry": "registry",
    "secrets": "secret",
    "services": "service",
    "tasks": "task",
    "users": "user",
}


async def record_authenticated_write(request: Request, response: Response) -> None:
    route = request.scope.get("route")
    route_template = getattr(route, "path", None)
    if not isinstance(route_template, str):
        return
    principal = getattr(request.state, "principal", None)
    if not isinstance(principal, Principal):
        principal = None
    spec = AUDITED_IDENTITY_WRITES.get((request.method, route_template))
    if spec is None:
        if (
            principal is None
            or request.method not in MUTATING_METHODS
            or not route_template.startswith("/api/v1/")
        ):
            return
        resource_type = _resource_type(route_template)
        route_name = getattr(route, "name", "write")
        spec = AuditSpec(
            action=f"{resource_type}.{route_name}",
            resource_type=resource_type,
            resource_params=tuple(request.path_params),
        )
    resource_values = [
        str(request.path_params[name])
        for name in spec.resource_params
        if name in request.path_params
    ]
    if 200 <= response.status_code < 400:
        outcome = "success"
    elif response.status_code in {401, 403}:
        outcome = "denied"
    elif response.status_code < 500:
        outcome = "failure"
    else:
        outcome = "error"

    database = request.app.state.database
    try:
        async with database.session() as session, session.begin():
            await AuditRepository.record(
                session,
                project_id=principal.project_id if principal is not None else None,
                actor_type=principal.kind.value if principal is not None else "anonymous",
                actor_user_id=principal.user_id if principal is not None else None,
                api_key_id=principal.api_key_id if principal is not None else None,
                action=spec.action,
                resource_type=spec.resource_type,
                resource_id=":".join(resource_values) or None,
                outcome=outcome,
                request_id=getattr(request.state, "request_id", None),
                source_ip=request.client.host if request.client is not None else None,
                details={
                    "method": request.method,
                    "route": route_template,
                    "status_code": response.status_code,
                },
            )
    except Exception as exc:
        # The business write has already committed. Preserve its response while
        # surfacing an audit durability failure to operators.
        LOGGER.error(
            "failed to persist authenticated API audit event",
            action=spec.action,
            request_id=getattr(request.state, "request_id", None),
            error=str(exc),
        )


def _resource_type(route_template: str) -> str:
    segments = [segment for segment in route_template.split("/") if segment]
    candidate = segments[2] if len(segments) > 2 else "api"
    return RESOURCE_NAMES.get(candidate, candidate.removesuffix("s") or "api")
