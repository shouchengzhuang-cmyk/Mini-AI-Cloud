from html import escape
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

WORKBENCH_DIR = Path(__file__).resolve().parent
WORKBENCH_INDEX = WORKBENCH_DIR / "index.html"
WORKBENCH_ASSETS = WORKBENCH_DIR / "assets"
WORKBENCH_ROOT_TOKEN = "__MINI_AI_CLOUD_ROOT_PATH__"
WORKBENCH_INDEX_HTML = WORKBENCH_INDEX.read_text(encoding="utf-8")


def install_workbench(app: FastAPI) -> None:
    """Expose the dependency-free operator workbench without changing API routing."""

    async def workbench_index(request: Request) -> HTMLResponse:
        root_path = str(request.scope.get("root_path", "")).rstrip("/")
        return HTMLResponse(
            WORKBENCH_INDEX_HTML.replace(WORKBENCH_ROOT_TOKEN, escape(root_path, quote=True)),
            headers={
                "Cache-Control": "no-store",
                "Content-Security-Policy": (
                    "default-src 'self'; "
                    "script-src 'self'; "
                    "style-src 'self'; "
                    "img-src 'self' data:; "
                    "connect-src 'self'; "
                    "object-src 'none'; "
                    "base-uri 'none'; "
                    "frame-ancestors 'none'"
                ),
                "Referrer-Policy": "no-referrer",
                "X-Content-Type-Options": "nosniff",
            },
        )

    app.add_api_route(
        "/workbench",
        workbench_index,
        methods=["GET"],
        include_in_schema=False,
        name="workbench",
    )
    app.add_api_route(
        "/workbench/",
        workbench_index,
        methods=["GET"],
        include_in_schema=False,
        name="workbench-trailing-slash",
    )
    app.mount(
        "/workbench/assets",
        StaticFiles(directory=WORKBENCH_ASSETS, check_dir=True),
        name="workbench-assets",
    )
