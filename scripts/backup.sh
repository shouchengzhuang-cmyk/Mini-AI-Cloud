#!/usr/bin/env bash
set -euo pipefail

readonly DEFAULT_LOCAL_PROJECT="mini-ai-cloud"
readonly LOCAL_PROJECT_PREFIX="mini-ai-cloud-local"
readonly ARCHIVE_IMAGE="alpine:3.21"

usage() {
  printf '%s\n' \
    "Usage: scripts/backup.sh --output-dir DIR --project-name NAME --local-stack [options]" \
    "" \
    "Back up PostgreSQL plus local and optional MinIO artifact volumes from a dedicated local Compose stack." \
    "The project must be '${DEFAULT_LOCAL_PROJECT}', '${LOCAL_PROJECT_PREFIX}', or a '${LOCAL_PROJECT_PREFIX}-*' stack." \
    "" \
    "Options:" \
    "  --output-dir DIR    Parent directory for a new timestamped backup (required)." \
    "  --project-name NAME Dedicated local Compose project name (required)." \
    "  --compose-file FILE Compose file to use (default: docker-compose.yml)." \
    "  --local-stack       Explicitly acknowledge this is a disposable/local stack." \
    "  -h, --help          Show this help." \
    "" \
    "For a consistent snapshot, postgres must be running while every volume writer is stopped."
}

fail() {
  printf 'backup: %s\n' "$*" >&2
  exit 2
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

archive_compose_volume() {
  local logical_name=$1
  local archive_name=$2
  local -a matching_volumes=()
  local volume_output
  volume_output=$(
    docker volume ls \
      --filter "label=com.docker.compose.project=$project_name" \
      --filter "label=com.docker.compose.volume=$logical_name" \
      --format '{{.Name}}'
  ) || fail "cannot inspect the '$logical_name' volume"
  if [[ -n $volume_output ]]; then
    mapfile -t matching_volumes <<<"$volume_output"
  fi
  ((${#matching_volumes[@]} <= 1)) ||
    fail "multiple '$logical_name' volumes match the local project"
  if ((${#matching_volumes[@]} == 0)); then
    return 1
  fi
  local volume_name=${matching_volumes[0]}
  local destination_owner
  destination_owner=$(stat --format '%u:%g' "$destination") ||
    fail "cannot resolve the backup destination owner"
  [[ $destination_owner =~ ^[0-9]+:[0-9]+$ ]] ||
    fail "backup destination owner is invalid"
  docker run --rm --network none --read-only --cap-drop ALL --cap-add DAC_READ_SEARCH \
    --security-opt no-new-privileges \
    --user "$destination_owner" \
    --tmpfs /tmp:size=16m \
    --mount "type=volume,source=$volume_name,target=/source,readonly" \
    --mount "type=bind,source=$destination,target=/backup" \
    "$ARCHIVE_IMAGE" \
    tar -C /source -czf "/backup/$archive_name" . ||
    fail "cannot archive the '$logical_name' volume"
  [[ -s $destination/$archive_name ]] || fail "volume archive is empty: $archive_name"
}

validate_project_name() {
  local project_name=$1
  [[ $project_name =~ ^[a-z0-9][a-z0-9_-]{0,62}$ ]] ||
    fail "invalid Compose project name: $project_name"
  case "$project_name" in
    "$DEFAULT_LOCAL_PROJECT" | "$LOCAL_PROJECT_PREFIX" | "$LOCAL_PROJECT_PREFIX"-*) ;;
    *) fail "project name is not a dedicated local stack: $project_name" ;;
  esac
}

output_dir=""
project_name=""
compose_file="docker-compose.yml"
local_stack_acknowledged=false

while (($#)); do
  case "$1" in
    --output-dir)
      (($# >= 2)) || fail "--output-dir requires a value"
      output_dir=$2
      shift 2
      ;;
    --project-name)
      (($# >= 2)) || fail "--project-name requires a value"
      project_name=$2
      shift 2
      ;;
    --compose-file)
      (($# >= 2)) || fail "--compose-file requires a value"
      compose_file=$2
      shift 2
      ;;
    --local-stack)
      local_stack_acknowledged=true
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[[ -n $output_dir ]] || fail "--output-dir is required"
[[ -n $project_name ]] || fail "--project-name is required"
[[ $local_stack_acknowledged == true ]] || fail "--local-stack is required"
validate_project_name "$project_name"
[[ -f $compose_file ]] || fail "Compose file not found: $compose_file"
[[ $output_dir != *','* && $output_dir != *$'\n'* ]] ||
  fail "output path cannot contain commas or newlines"

require_command docker
require_command realpath
require_command sha256sum
require_command stat

docker compose version >/dev/null
compose=(docker compose --file "$compose_file" --project-name "$project_name")
"${compose[@]}" config --quiet

running_services=$("${compose[@]}" ps --services --status running)
while IFS= read -r service; do
  case "$service" in
    api | worker | migrate | minio | minio-init | artifact-init)
      fail "stop the '$service' service before taking a consistent backup"
      ;;
  esac
done <<<"$running_services"

postgres_id=$("${compose[@]}" ps --quiet postgres)
[[ -n $postgres_id ]] || fail "the local postgres service is not running"
postgres_project=$(
  docker inspect --format '{{ index .Config.Labels "com.docker.compose.project" }}' "$postgres_id"
)
[[ $postgres_project == "$project_name" ]] || fail "postgres belongs to another Compose project"
postgres_running=$(docker inspect --format '{{.State.Running}}' "$postgres_id")
[[ $postgres_running == true ]] || fail "the local postgres service is not running"

mkdir -p -- "$output_dir"
output_root=$(realpath -- "$output_dir")
timestamp=$(date -u +%Y%m%dT%H%M%SZ)
destination="$output_root/${project_name}-${timestamp}"
[[ ! -e $destination ]] || fail "backup destination already exists: $destination"
mkdir -- "$destination"

dump_tmp="$destination/.postgres.dump.tmp"
# Expand database settings inside the container, not in the host shell.
# shellcheck disable=SC2016
"${compose[@]}" exec -T postgres sh -ec \
  'pg_dump --format=custom --username="$POSTGRES_USER" --dbname="$POSTGRES_DB"' \
  >"$dump_tmp"
[[ -s $dump_tmp ]] || fail "PostgreSQL dump is empty"
mv -- "$dump_tmp" "$destination/postgres.dump"

artifact_data_included=false
if archive_compose_volume artifact-data artifact-data.tar.gz; then
  artifact_data_included=true
fi
minio_included=false
if archive_compose_volume minio-data minio-data.tar.gz; then
  minio_included=true
fi

created_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
printf '{\n  "format_version": 1,\n  "created_at": "%s",\n  "compose_project": "%s",\n  "postgres": true,\n  "local_artifacts": %s,\n  "minio": %s\n}\n' \
  "$created_at" "$project_name" "$artifact_data_included" "$minio_included" \
  >"$destination/manifest.json"

(
  cd -- "$destination"
  checksum_files=(manifest.json postgres.dump)
  if [[ $artifact_data_included == true ]]; then
    checksum_files+=(artifact-data.tar.gz)
  fi
  if [[ $minio_included == true ]]; then
    checksum_files+=(minio-data.tar.gz)
  fi
  sha256sum -- "${checksum_files[@]}" >SHA256SUMS
)

printf 'Backup complete: %s\n' "$destination"
