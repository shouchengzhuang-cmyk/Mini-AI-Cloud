SHELL := /bin/sh
.DEFAULT_GOAL := help

UV ?= uv
COMPOSE ?= docker compose
PYTEST ?= $(UV) run pytest

.PHONY: help install format lint typecheck test test-unit test-integration test-docker \
	test-e2e check config build up down ps logs migrate migrate-local run-api run-worker \
	load-test

help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\n"} \
		/^[a-zA-Z_0-9-]+:.*## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Create/update the uv environment with development dependencies.
	$(UV) sync --all-groups

format: ## Format Python sources.
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

lint: ## Run Ruff formatting and lint checks.
	$(UV) run ruff format --check .
	$(UV) run ruff check .

typecheck: ## Run strict static type checking.
	$(UV) run mypy .

test: ## Run the complete pytest suite.
	$(PYTEST)

test-unit: ## Run tests that need no external services or Docker daemon.
	$(PYTEST) -m "not integration and not docker and not e2e"

test-integration: ## Run isolated repository, outbox, Redis, and API integration tests.
	$(PYTEST) -m integration

test-docker: ## Run Docker runtime tests.
	$(PYTEST) -m docker

test-e2e: ## Run Docker Runtime end-to-end tests.
	$(PYTEST) -m e2e

check: lint typecheck test-unit ## Run the fast local quality gate.

config: ## Validate the rendered Compose configuration.
	$(COMPOSE) config --quiet

build: ## Build the API/worker image.
	$(COMPOSE) build

up: ## Build and start the complete stack in the background.
	$(COMPOSE) up --build -d

down: ## Stop the stack while retaining database and Redis volumes.
	$(COMPOSE) down --remove-orphans

ps: ## Show service and health status.
	$(COMPOSE) ps

logs: ## Follow recent service logs.
	$(COMPOSE) logs --follow --tail=200

migrate: ## Upgrade the Compose database to the newest revision.
	$(COMPOSE) run --rm migrate

migrate-local: ## Upgrade DATABASE_URL using the host uv environment.
	$(UV) run alembic upgrade head

run-api: ## Run the API server on the host using API_HOST and API_PORT.
	$(UV) run python -m api.main

run-worker: ## Run one worker on the host.
	$(UV) run python -m worker.main

load-test: ## Run the lightweight API load generator.
	$(UV) run python scripts/load_test.py
