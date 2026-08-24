from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import Response, StreamingResponse

from api.dependencies import get_principal
from api.errors import APIError
from api.schemas.gateway import OpenAIModelList, OpenAIProxyRequest
from api.services.gateway import GatewayForwardResult, GatewayService
from core.rbac import Permission, Principal, PrincipalKind, require_permission

router = APIRouter(tags=["openai-gateway"])


def get_gateway_service(request: Request) -> GatewayService:
    gateway = getattr(request.app.state, "gateway_service", None)
    if not isinstance(gateway, GatewayService):
        raise RuntimeError("gateway service is not configured")
    return gateway


async def require_gateway_principal(
    principal: Annotated[Principal, Depends(get_principal)],
) -> Principal:
    if principal.kind != PrincipalKind.API_KEY:
        raise APIError(
            401,
            "API_KEY_REQUIRED",
            "The OpenAI-compatible gateway requires API key authentication",
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        require_permission(principal, Permission.MODEL_READ)
    except PermissionError as exc:
        raise APIError(403, "PERMISSION_DENIED", str(exc)) from exc
    return principal


@router.get("/v1/models", response_model=OpenAIModelList)
async def list_gateway_models(
    gateway: Annotated[GatewayService, Depends(get_gateway_service)],
    principal: Annotated[Principal, Depends(require_gateway_principal)],
) -> OpenAIModelList:
    assert principal.project_id is not None
    return await gateway.list_models(project_id=principal.project_id)


@router.post("/v1/chat/completions")
async def proxy_chat_completions(
    payload: OpenAIProxyRequest,
    request: Request,
    gateway: Annotated[GatewayService, Depends(get_gateway_service)],
    principal: Annotated[Principal, Depends(require_gateway_principal)],
) -> Response:
    return await _proxy(
        gateway=gateway,
        principal=principal,
        request=request,
        payload=payload,
        path="/v1/chat/completions",
    )


@router.post("/v1/completions")
async def proxy_completions(
    payload: OpenAIProxyRequest,
    request: Request,
    gateway: Annotated[GatewayService, Depends(get_gateway_service)],
    principal: Annotated[Principal, Depends(require_gateway_principal)],
) -> Response:
    return await _proxy(
        gateway=gateway,
        principal=principal,
        request=request,
        payload=payload,
        path="/v1/completions",
    )


async def _proxy(
    *,
    gateway: GatewayService,
    principal: Principal,
    request: Request,
    payload: OpenAIProxyRequest,
    path: str,
) -> Response:
    assert principal.project_id is not None
    result = await gateway.forward(
        project_id=principal.project_id,
        public_model=payload.model,
        path=path,
        payload=payload.model_dump(mode="json"),
        request_headers=request.headers,
        stream_requested=payload.stream,
        client_disconnected=request.is_disconnected,
    )
    return _gateway_response(result)


def _gateway_response(result: GatewayForwardResult) -> Response:
    if result.stream is not None:
        return StreamingResponse(
            result.stream,
            status_code=result.status_code,
            headers=result.headers,
        )
    return Response(
        content=result.body or b"",
        status_code=result.status_code,
        headers=result.headers,
    )
