from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

WORKBENCH_DIR = Path(__file__).resolve().parent
WORKBENCH_INDEX = WORKBENCH_DIR / "index.html"


def install_workbench(app: FastAPI) -> None:
    """Expose the dependency-free operator workbench without changing API routing."""

    async def workbench_index() -> FileResponse:
        return FileResponse(
            WORKBENCH_INDEX,
            media_type="text/html",
            headers={
                "Cache-Control": "no-store",
                "Content-Security-Policy": (
                    "default-src 'self'; "
                    "script-src 'self'; "
                    "style-src 'self'; "
                    "img-src 'self' data:; "
                    "connect-src 'self' http: https:; "
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
        StaticFiles(directory=WORKBENCH_DIR, check_dir=True),
        name="workbench-assets",
    )
