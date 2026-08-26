FROM python:3.12-slim-bookworm AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

RUN python -m venv "$VIRTUAL_ENV" \
    && python -m pip install --upgrade pip

WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.cache/pip python -m pip install . \
    && python -m pip uninstall --yes pip


FROM python:3.12-slim-bookworm AS runtime

LABEL org.opencontainers.image.title="Mini AI Cloud" \
      org.opencontainers.image.description="API and worker runtime for Mini AI Cloud"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

RUN python -m pip uninstall --yes pip \
    && groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --create-home \
        --home-dir /home/app --shell /usr/sbin/nologin app

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY --chown=10001:10001 . .
RUN python -m compileall -q /app

USER 10001:10001
EXPOSE 8000
STOPSIGNAL SIGTERM

CMD ["python", "-m", "api.main"]
