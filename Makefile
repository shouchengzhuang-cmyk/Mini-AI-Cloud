SHELL := /bin/sh
.DEFAULT_GOAL := help

UV ?= uv
COMPOSE ?= docker compose
PYTEST ?= $(UV) run pytest
POWERSHELL ?= pwsh
KIND ?= kind
HELM ?= helm
KUBECTL ?= kubectl
DOCKER ?= docker
KIND_HELM_IMAGE ?= mini-ai-cloud:kind-m7-p1
KIND_CLUSTER_NAME ?= mini-ai-cloud-test
LOCAL_STACK_PROJECT ?= mini-ai-cloud
SIMULATION_OUTPUT_DIR ?= build/scheduler-simulation
BACKUP_OUTPUT_DIR ?= build/backups
RELEASE_IMAGE ?= mini-ai-cloud:release-gate
RELEASE_WHEEL_DIR ?= build/release-wheel
DUAL_BACKEND_OUTPUT ?= build/dual-backend-report.json
P4_EVIDENCE_ROOT ?= build/kind-evidence
P4_POSTGRES_IMAGE ?= docker.io/library/postgres@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685
P4_REDIS_IMAGE ?= docker.io/library/redis@sha256:ff02b58f971e7d7d156a1267e283fcbbeee91773b6aa36c49dac28ecfe28eadf
P4_EVIDENCE_BUNDLE ?=

.PHONY: help install format lint typecheck validate-evidence evidence test test-unit test-integration test-docker \
	test-e2e test-serving check config build up down ps logs migrate migrate-local run-api run-worker \
	load-test dev observability test-chaos test-k8s kind-up kind-down kind-serving-up \
	test-kind-serving kind-serving-down demo-fencing demo-adoption demo-sse-drain demo-all \
	test-nvidia-fake-device-plugin validate-ascend-runtime \
	test-dr test-soak test-release release-validate benchmark benchmark-dual-backend backup restore \
	test-helm-render test-kind-helm test-kind-kubernetes-adaptation test-kind-batch-job \
	test-kind-upgrade-smoke test-kind-cleanup test-evidence-secret-scan

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

validate-evidence: ## Validate claims, invariants, environments, schema, and matrix.
	$(UV) run python scripts/validate_evidence.py

validate-ascend-runtime: ## Validate the pinned Ascend A2 runtime contract.
	$(UV) run python scripts/validate_ascend_runtime.py

evidence: ## Collect a credential-safe evidence bundle bound to the current commit.
	$(UV) run mini-cloud evidence collect

release-validate: ## Validate version, action pins, dependencies, secrets, container, and contracts.
	$(UV) lock --check
	$(UV) run python scripts/release_gate.py validate \
		$(if $(strip $(P4_EVIDENCE_BUNDLE)),--p4-evidence "$(P4_EVIDENCE_BUNDLE)",)

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

test-serving: ## Run Phase III Fake Serving, tenant isolation, and placement tests.
	$(PYTEST) tests/integration/test_fake_serving_e2e.py \
		tests/integration/test_gateway_project_isolation.py \
		tests/integration/test_service_reconcile_concurrency.py \
		tests/integration/test_vllm_tensor_parallel_e2e.py \
		tests/unit/test_serving_scheduler.py

check: lint typecheck validate-evidence test-unit ## Run the fast local quality gate.

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

dev: ## Run the dedicated local development stack in the foreground.
	$(COMPOSE) --project-name $(LOCAL_STACK_PROJECT) up --build

observability: ## Start the local stack with Prometheus and Grafana.
	$(COMPOSE) --project-name $(LOCAL_STACK_PROJECT) --profile observability up --build -d

test-chaos: ## Run destructive fault injection; requires CONFIRM_CHAOS=YES.
	@test "$(CONFIRM_CHAOS)" = "YES" || { echo "CONFIRM_CHAOS=YES is required" >&2; exit 2; }
	$(POWERSHELL) -NoProfile -File scripts/fault_injection.ps1 -Case All '-Confirm:$$false'

test-k8s: ## Run Kubernetes runtime and GPU inventory unit tests.
	$(PYTEST) tests/unit/test_kubernetes_runtime.py tests/unit/test_gpu_inventory.py

test-helm-render: ## Lint and validate the production Helm Chart and negative fixtures.
	$(UV) run python scripts/validate_helm_render.py --helm "$(HELM)"

test-kind-helm: ## Build and run the isolated Helm install/uninstall smoke; requires RUN_ID.
	@test -n "$(RUN_ID)" || { echo "RUN_ID=<unique-id> is required" >&2; exit 2; }
	$(DOCKER) build --file docker/Dockerfile --tag $(KIND_HELM_IMAGE) .
	RUN_ID="$(RUN_ID)" HELM_BIN="$(HELM)" KIND_BIN="$(KIND)" \
		KUBECTL_BIN="$(KUBECTL)" DOCKER_BIN="$(DOCKER)" \
		KIND_HELM_IMAGE="$(KIND_HELM_IMAGE)" bash scripts/helm_kind_smoke.sh

test-kind-kubernetes-adaptation: ## Run the complete isolated P4 Kind evidence harness.
	$(UV) run python scripts/kind_kubernetes_adaptation.py run \
		--evidence-root "$(P4_EVIDENCE_ROOT)" \
		--helm "$(HELM)" --kind "$(KIND)" --kubectl "$(KUBECTL)" \
		--docker "$(DOCKER)" --uv "$(UV)" \
		--postgres-image "$(P4_POSTGRES_IMAGE)" --redis-image "$(P4_REDIS_IMAGE)"

test-kind-batch-job: test-kind-kubernetes-adaptation ## Run full P4; PASS includes real batch Jobs.
	@:

test-kind-upgrade-smoke: test-kind-kubernetes-adaptation ## Run full P4; PASS includes upgrade UID checks.
	@:

test-kind-cleanup: test-kind-kubernetes-adaptation ## Run full P4; PASS includes bounded cleanup.
	@:

test-evidence-secret-scan: ## Validate an explicit completed P4 bundle; never treats NOT_RUN as PASS.
	@test -n "$(P4_EVIDENCE_BUNDLE)" || { echo "P4_EVIDENCE_BUNDLE=<bundle-dir> is required" >&2; exit 2; }
	$(UV) run python scripts/kind_kubernetes_adaptation.py verify-bundle \
		--bundle "$(P4_EVIDENCE_BUNDLE)"

kind-up: ## Create the isolated Kind cluster used for local runtime testing.
	$(KIND) create cluster --name $(KIND_CLUSTER_NAME)

kind-down: ## Delete only the isolated Kind test cluster.
	$(KIND) delete cluster --name $(KIND_CLUSTER_NAME)

kind-serving-up: ## Build and deploy the isolated Phase IV-A Kind serving stack.
	bash scripts/kind_serving.sh up

test-kind-serving: ## Run the mandatory real Kubernetes serving E2E against Kind.
	bash scripts/kind_serving.sh test

test-nvidia-fake-device-plugin: ## Run the fake extended-resource allocation test in Kind.
	bash scripts/nvidia_fake_device_plugin.sh test

kind-serving-down: ## Delete only the Phase IV-A Kind cluster and local credentials.
	bash scripts/kind_serving.sh down

demo-fencing: ## Run the stale worker/execution fencing hero scenario.
	$(UV) run mini-cloud demo fencing

demo-adoption: ## Run controller restart adoption against an isolated Kind cluster.
	$(UV) run mini-cloud demo controller-adoption

demo-sse-drain: ## Run active SSE drain against an isolated Kind cluster.
	$(UV) run mini-cloud demo sse-drain

demo-all: ## Run all hero scenarios and clean the isolated Kind cluster.
	$(UV) run mini-cloud demo all

test-soak: ## Run bounded restart/fencing soak; requires CONFIRM_SOAK=YES.
	@test "$(CONFIRM_SOAK)" = "YES" || { echo "CONFIRM_SOAK=YES is required" >&2; exit 2; }
	$(UV) run python scripts/soak.py --rounds "$${SOAK_ROUNDS:-3}"

benchmark: ## Compare binpack/spread on 100 workers, 4 GPUs each, and 10000 jobs.
	$(UV) run python -m scripts.scheduler_simulation \
		--workers 100 --gpus-per-worker 4 --jobs 10000 \
		--output-dir $(SIMULATION_OUTPUT_DIR)

benchmark-dual-backend: ## Run the configured NVIDIA + Ascend serving benchmark.
	@test -n "$(DUAL_BACKEND_CONFIG)" || { echo "DUAL_BACKEND_CONFIG=/path/to/config.json is required" >&2; exit 2; }
	$(UV) run python -m benchmarks.dual_backend \
		--config "$(DUAL_BACKEND_CONFIG)" --output "$(DUAL_BACKEND_OUTPUT)"

backup: ## Back up local PostgreSQL plus local/MinIO artifact volumes.
	bash scripts/backup.sh --local-stack --project-name $(LOCAL_STACK_PROJECT) \
		--output-dir $(BACKUP_OUTPUT_DIR)

restore: ## Restore BACKUP into the dedicated local stack; requires CONFIRM_RESTORE=YES.
	@test -n "$(BACKUP)" || { echo "BACKUP=/path/to/backup is required" >&2; exit 2; }
	@test "$(CONFIRM_RESTORE)" = "YES" || { echo "CONFIRM_RESTORE=YES is required" >&2; exit 2; }
	bash scripts/restore.sh --local-stack --confirm-overwrite \
		--project-name $(LOCAL_STACK_PROJECT) --backup-dir "$(BACKUP)"

test-dr: ## Run isolated destructive DR rehearsal; requires CONFIRM_DR=YES.
	@test "$(CONFIRM_DR)" = "YES" || { echo "CONFIRM_DR=YES is required" >&2; exit 2; }
	$(UV) run python scripts/dr_rehearsal.py

test-release: release-validate lint typecheck validate-evidence config ## Run the v0.6.0 release gate.
	@test -n "$(P4_EVIDENCE_BUNDLE)" || { \
		echo "P4_EVIDENCE_BUNDLE=<real KIND_K8S_PASS bundle> is required" >&2; exit 2; }
	$(UV) run pytest
	$(UV) build --wheel --out-dir $(RELEASE_WHEEL_DIR)
	$(UV) run python scripts/release_gate.py wheel-smoke --dist-dir $(RELEASE_WHEEL_DIR)
	docker build --file docker/Dockerfile --tag $(RELEASE_IMAGE) .
	docker run --rm $(RELEASE_IMAGE) python -c "import importlib.metadata; assert importlib.metadata.version('mini-ai-cloud') == '0.6.0'"
	$(UV) run mini-cloud evidence collect
	$(UV) run python scripts/release_gate.py prepare \
		--p4-evidence "$(P4_EVIDENCE_BUNDLE)"
