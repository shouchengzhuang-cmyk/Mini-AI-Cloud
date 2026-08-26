#!/usr/bin/env bash
set -euo pipefail

readonly DEFAULT_LOCAL_PROJECT="mini-docker-cloud"
readonly LOCAL_PROJECT_PREFIX="mini-docker-cloud-local"
readonly ARCHIVE_IMAGE="alpine:3.21"

usage() {
  printf '%s\n' \
    "Usage: scripts/restore.sh --backup-dir DIR --project-name NAME --local-stack --confirm-overwrite [options]" \
    "" \
    "Restore PostgreSQL plus local and optional MinIO artifact data into a stopped, dedicated local Compose stack." \
    "Restore refuses to run without explicit overwrite confirmation and never targets other projects." \
    "" \
    "Options:" \
    "  --backup-dir DIR       Backup directory created by backup.sh (required)." \
    "  --project-name NAME    Dedicated local Compose project name (required)." \
    "  --compose-file FILE    Compose file to use (default: docker-compose.yml)." \
    "  --local-stack          Explicitly acknowledge this is a disposable/local stack." \
    "  --confirm-overwrite    Explicitly allow replacement of local PostgreSQL/MinIO data." \
    "  -h, --help             Show this help."
}

fail() {
  printf 'restore: %s\n' "$*" >&2
  exit 2
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
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

validate_checksum_manifest() {
  local checksum_file=$1
  local hash filename extra
  declare -A seen=()
  while read -r hash filename extra; do
    [[ -z ${extra:-} ]] || fail "invalid SHA256SUMS entry"
    [[ ${#hash} -eq 64 && $hash != *[!0-9a-fA-F]* ]] || fail "invalid checksum"
    filename=${filename#\*}
    case "$filename" in
      manifest.json | postgres.dump | artifact-data.tar.gz | minio-data.tar.gz) ;;
      *) fail "unexpected file in SHA256SUMS: $filename" ;;
    esac
    [[ -z ${seen[$filename]+present} ]] || fail "duplicate SHA256SUMS entry: $filename"
    seen[$filename]=true
  done <"$checksum_file"
  [[ -n ${seen[manifest.json]+present} && -n ${seen[postgres.dump]+present} ]] ||
    fail "SHA256SUMS is incomplete"
  for archive_name in artifact-data.tar.gz minio-data.tar.gz; do
    if [[ -e $backup_root/$archive_name && -z ${seen[$archive_name]+present} ]]; then
      fail "volume archive is not covered by SHA256SUMS: $archive_name"
    fi
  done
}

validate_volume_archive() {
  local archive=$1
  local entry normalized
  tar -tzf "$archive" >/dev/null || fail "volume archive cannot be read: $archive"
  while IFS= read -r entry; do
    normalized=${entry#./}
    [[ -z $normalized || $normalized == . ]] && continue
    [[ $normalized != /* && $normalized != .. && $normalized != ../* ]] ||
      fail "unsafe path in volume archive: $entry"
    [[ $normalized != */../* && $normalized != */.. ]] ||
      fail "unsafe path in volume archive: $entry"
  done < <(tar -tzf "$archive")
  if tar -tvzf "$archive" | awk 'substr($0, 1, 1) ~ /[lh]/ { found=1 } END { exit found ? 0 : 1 }'; then
    fail "volume archive contains links: $archive"
  fi
}

restore_compose_volume() {
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
  local volume_name
  if ((${#matching_volumes[@]} == 0)); then
    volume_name="${project_name}_${logical_name}"
    if docker volume inspect "$volume_name" >/dev/null 2>&1; then
      fail "target volume exists without the expected local Compose labels: $volume_name"
    fi
    volume_name=$(docker volume create \
      --label "com.docker.compose.project=$project_name" \
      --label "com.docker.compose.volume=$logical_name" \
      "$volume_name")
  else
    volume_name=${matching_volumes[0]}
  fi
  docker run --rm --network none --read-only --cap-drop ALL \
    --cap-add CHOWN --cap-add DAC_OVERRIDE --cap-add FOWNER \
    --security-opt no-new-privileges \
    --tmpfs /tmp:size=16m \
    --mount "type=volume,source=$volume_name,target=/target" \
    --mount "type=bind,source=$backup_root,target=/backup,readonly" \
    "$ARCHIVE_IMAGE" sh -ec \
    'find /target -mindepth 1 -maxdepth 1 -exec rm -rf -- {} \; && tar -C /target -xzf "/backup/$1"' \
    restore-volume "$archive_name"
}

backup_dir=""
project_name=""
compose_file="docker-compose.yml"
local_stack_acknowledged=false
overwrite_confirmed=false

while (($#)); do
  case "$1" in
    --backup-dir)
      (($# >= 2)) || fail "--backup-dir requires a value"
      backup_dir=$2
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
    --confirm-overwrite)
      overwrite_confirmed=true
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[[ -n $backup_dir ]] || fail "--backup-dir is required"
[[ -n $project_name ]] || fail "--project-name is required"
[[ $local_stack_acknowledged == true ]] || fail "--local-stack is required"
[[ $overwrite_confirmed == true ]] ||
  fail "refusing to overwrite local data without --confirm-overwrite"
validate_project_name "$project_name"
[[ -d $backup_dir && ! -L $backup_dir ]] || fail "backup directory is missing or is a symlink"
[[ -f $compose_file ]] || fail "Compose file not found: $compose_file"
[[ $backup_dir != *','* && $backup_dir != *$'\n'* ]] ||
  fail "backup path cannot contain commas or newlines"

require_command awk
require_command docker
require_command realpath
require_command sha256sum
require_command tar

backup_root=$(realpath -- "$backup_dir")
for required_file in manifest.json postgres.dump SHA256SUMS; do
  [[ -f $backup_root/$required_file && ! -L $backup_root/$required_file ]] ||
    fail "required backup file is missing or is a symlink: $required_file"
done
validate_checksum_manifest "$backup_root/SHA256SUMS"
(
  cd -- "$backup_root"
  sha256sum --check --strict SHA256SUMS
)
for archive_name in artifact-data.tar.gz minio-data.tar.gz; do
  if [[ -e $backup_root/$archive_name ]]; then
    [[ -f $backup_root/$archive_name && ! -L $backup_root/$archive_name ]] ||
      fail "volume archive is not a regular file: $archive_name"
    validate_volume_archive "$backup_root/$archive_name"
  fi
done

docker compose version >/dev/null
compose=(docker compose --file "$compose_file" --project-name "$project_name")
"${compose[@]}" config --quiet
running_services=$("${compose[@]}" ps --services --status running)
[[ -z $running_services ]] ||
  fail "all services in the target local stack must be stopped before restore"

if [[ -f $backup_root/artifact-data.tar.gz ]]; then
  restore_compose_volume artifact-data artifact-data.tar.gz
fi
if [[ -f $backup_root/minio-data.tar.gz ]]; then
  restore_compose_volume minio-data minio-data.tar.gz
fi

postgres_started=false
stop_postgres() {
  if [[ $postgres_started == true ]]; then
    "${compose[@]}" stop --timeout 10 postgres >/dev/null
  fi
}
trap stop_postgres EXIT

"${compose[@]}" up --detach postgres
postgres_started=true
for _ in {1..60}; do
  # Expand database settings inside the container, not in the host shell.
  # shellcheck disable=SC2016
  if "${compose[@]}" exec -T postgres sh -ec \
    'pg_isready --username="$POSTGRES_USER" --dbname="$POSTGRES_DB"' >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
# shellcheck disable=SC2016
"${compose[@]}" exec -T postgres sh -ec \
  'pg_isready --username="$POSTGRES_USER" --dbname="$POSTGRES_DB"' >/dev/null ||
  fail "PostgreSQL did not become ready"
# shellcheck disable=SC2016
"${compose[@]}" exec -T postgres sh -ec \
  'pg_restore --clean --if-exists --no-owner --no-privileges --exit-on-error --username="$POSTGRES_USER" --dbname="$POSTGRES_DB"' \
  <"$backup_root/postgres.dump"

printf 'Restore complete for dedicated local project: %s\n' "$project_name"
