#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    printf 'error: required command not found: %s\n' "$command_name" >&2
    exit 1
  fi
}

require_commands() {
  local command_name
  for command_name in "$@"; do
    require_command "$command_name"
  done
}

sanitize_source_name() {
  local raw="$1"
  local sanitized
  sanitized="$(printf '%s' "$raw" | tr ':/' '__' | tr -cd 'A-Za-z0-9._-')"
  if [[ -z "$sanitized" ]]; then
    sanitized="source"
  fi
  printf '%s' "$sanitized"
}

json_escape() {
  local raw="$1"
  raw="${raw//\\/\\\\}"
  raw="${raw//\"/\\\"}"
  raw="${raw//$'\n'/\\n}"
  raw="${raw//$'\r'/\\r}"
  raw="${raw//$'\t'/\\t}"
  printf '%s' "$raw"
}

validate_run_name() {
  local candidate="$1"
  if [[ ! "$candidate" =~ ^[A-Za-z0-9._-]+$ ]] || [[ "$candidate" == "." ]] || [[ "$candidate" == ".." ]]; then
    printf 'error: run name must contain only letters, numbers, dot, underscore, or hyphen\n' >&2
    exit 1
  fi
}

validate_profile_version() {
  local candidate="$1"
  if [[ ! "$candidate" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf 'error: profile version must use MAJOR.MINOR.PATCH format\n' >&2
    exit 1
  fi
}

resolve_input_path() {
  local candidate="$1"
  if [[ "$candidate" = /* ]]; then
    printf '%s' "$candidate"
  else
    printf '%s/%s' "$REPO_ROOT" "$candidate"
  fi
}

ensure_supported_block_store_target() {
  local target="$1"
  case "$target" in
    filesystem|overlay)
      return 0
      ;;
    *)
      printf 'error: unsupported block-store target: %s\n' "$target" >&2
      exit 1
      ;;
  esac
}

overlay_memory_cache_max_resident_blocks() {
  local value="${OVERLAY_BLOCK_STORE_MEMORY_CACHE_MAX_RESIDENT_BLOCKS:-4096}"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    printf 'error: OVERLAY_BLOCK_STORE_MEMORY_CACHE_MAX_RESIDENT_BLOCKS must be a positive integer\n' >&2
    exit 1
  fi
  printf '%s' "$value"
}

load_manifest() {
  local manifest_path="$1"
  if [[ ! -f "$manifest_path" ]]; then
    printf 'error: manifest not found: %s\n' "$manifest_path" >&2
    exit 1
  fi

  MANIFEST_CONTAINER_NAME="$(
    python3 - "$manifest_path" <<'PY'
import json
import sys

with open(sys.argv[1], 'r', encoding='utf-8') as handle:
    data = json.load(handle)

container = data.get('container_name')
if not isinstance(container, str) or not container.strip():
    raise SystemExit('manifest must contain non-empty string field "container_name"')

print(container.strip())
PY
  )"

  mapfile -t MANIFEST_SOURCES < <(
    python3 - "$manifest_path" <<'PY'
import json
import sys

with open(sys.argv[1], 'r', encoding='utf-8') as handle:
    data = json.load(handle)

sources = data.get('sources')
if not isinstance(sources, list) or not sources:
    raise SystemExit('manifest must contain non-empty array field "sources"')

for source in sources:
    if not isinstance(source, str) or not source.strip():
        raise SystemExit('manifest sources must be non-empty strings')
    print(source.strip())
PY
  )
}

append_path_to_container_sas_url() {
  local container_sas_url="$1"
  local blob_path="$2"
  python3 - "$container_sas_url" "$blob_path" <<'PY'
import sys
from urllib.parse import urlsplit, urlunsplit

container_sas_url = sys.argv[1]
blob_path = sys.argv[2].strip('/')
segments = []
if blob_path:
    for segment in blob_path.split('/'):
        if not segment:
            raise SystemExit('error: blob path must not contain empty path segments')
        if segment in {'.', '..'}:
            raise SystemExit('error: blob path must not contain "." or ".." path segments')
        if any(ch.isspace() for ch in segment):
            raise SystemExit('error: blob path must not contain whitespace')
        if any(ord(ch) < 32 or ord(ch) == 127 for ch in segment):
            raise SystemExit('error: blob path must not contain control characters')
        if '\\' in segment:
            raise SystemExit('error: blob path must not contain backslashes')
        segments.append(segment)
parts = urlsplit(container_sas_url)
base_path = parts.path.rstrip('/')
full_path = f"{base_path}/{'/'.join(segments)}" if segments else base_path
print(urlunsplit((parts.scheme, parts.netloc, full_path, parts.query, parts.fragment)))
PY
}

blob_path_exists() {
  local container_sas_url="$1"
  local blob_path="$2"
  local blob_url
  blob_url="$(append_path_to_container_sas_url "$container_sas_url" "$blob_path")"
  azcopy list "$blob_url" >/dev/null 2>&1
}

download_blob_tree_if_present() {
  local container_sas_url="$1"
  local blob_path="$2"
  local destination="$3"
  local blob_url

  mkdir -p "$destination"
  if ! blob_path_exists "$container_sas_url" "$blob_path"; then
    return 0
  fi

  blob_url="$(append_path_to_container_sas_url "$container_sas_url" "$blob_path")"
  azcopy copy "$blob_url" "$destination" --recursive=true >/dev/null
}

sync_local_tree_to_blob() {
  local source_dir="$1"
  local container_sas_url="$2"
  local blob_path="$3"
  local blob_url

  if [[ ! -d "$source_dir" ]]; then
    printf 'error: local directory not found for upload: %s\n' "$source_dir" >&2
    exit 1
  fi

  blob_url="$(append_path_to_container_sas_url "$container_sas_url" "$blob_path")"
  azcopy sync "$source_dir" "$blob_url" --recursive=true --delete-destination=false >/dev/null
}

upload_file_to_blob() {
  local source_file="$1"
  local container_sas_url="$2"
  local blob_path="$3"
  local blob_url

  if [[ ! -f "$source_file" ]]; then
    printf 'error: local file not found for upload: %s\n' "$source_file" >&2
    exit 1
  fi

  blob_url="$(append_path_to_container_sas_url "$container_sas_url" "$blob_path")"
  azcopy copy "$source_file" "$blob_url" --overwrite=true >/dev/null
}

mirror_manifest_sources() {
  local run_root="$1"
  shift

  local fetched_dir="${run_root}/fetched"
  local index source_url source_num source_name source_dir rel_to_run filename month mailbox_path

  mkdir -p "$fetched_dir"
  MAILBOX_PATHS=()
  DISCOVERED_MONTHS=()

  for index in "${!MANIFEST_SOURCES[@]}"; do
    source_url="${MANIFEST_SOURCES[$index]}"
    source_num=$((index + 1))
    source_name="$(sanitize_source_name "$source_url")"
    source_dir="${fetched_dir}/$(printf '%02d' "$source_num")-${source_name}"

    mkdir -p "$source_dir"
    printf 'Fetching [%02d/%02d]: %s\n' "$source_num" "${#MANIFEST_SOURCES[@]}" "$source_url"
    rsync -avz --delete -- "${source_url%/}/" "${source_dir}/"

    while IFS= read -r mailbox_path; do
      rel_to_run="${mailbox_path#${run_root}/}"
      MAILBOX_PATHS+=("$rel_to_run")
      filename="$(basename "$mailbox_path")"
      case "$filename" in
        *.mbox)
          month="${filename%.mbox}"
          ;;
        *.mail)
          month="${filename%.mail}"
          ;;
        *)
          printf 'error: discovered mailbox with unsupported extension: %s\n' "$filename" >&2
          exit 1
          ;;
      esac
      DISCOVERED_MONTHS+=("$month")
    done < <(find "$source_dir" -type f \( -name '*.mail' -o -name '*.mbox' \) | LC_ALL=C sort)
  done

  if [[ ${#MAILBOX_PATHS[@]} -eq 0 ]]; then
    printf 'error: no .mail or .mbox files were discovered in fetched rsync mirrors\n' >&2
    exit 1
  fi
}

write_mailbox_request() {
  local request_path="$1"
  local block_store_root="$2"
  local container_sas_url="$3"
  local block_store_blob_path="$4"
  local block_store_target="$5"
  local embedding_base_url="$6"
  local stage="$7"
  local profile_version="${8:-}"
  local include_items="$9"
  local index mailbox_path month overlay_memory_cache_blocks

  if [[ "$block_store_target" == "overlay" ]]; then
    overlay_memory_cache_blocks="$(overlay_memory_cache_max_resident_blocks)"
  fi

  {
    printf '{\n'
    printf '  "environment": {\n'
    if [[ "$block_store_target" == "overlay" ]]; then
      printf '    "kind": "local-overlay",\n'
      printf '    "block_store": {\n'
      printf '      "container_sas_url": "%s",\n' "$(json_escape "$container_sas_url")"
      printf '      "filesystem_cache_root": "%s",\n' "$(json_escape "$block_store_root")"
      printf '      "memory_cache_max_resident_blocks": %s\n' "$overlay_memory_cache_blocks"
      printf '    },\n'
    else
      printf '    "kind": "local",\n'
      printf '    "block_store_root": "%s",\n' "$(json_escape "$block_store_root")"
    fi
    printf '    "embedding": {\n'
    printf '      "base_url": "%s",\n' "$(json_escape "$embedding_base_url")"
    printf '      "model": "all-MiniLM-L6-v2",\n'
    printf '      "request_timeout_secs": 30,\n'
    printf '      "max_retries": 10,\n'
    printf '      "retry_delay_ms": 1000\n'
    printf '    }\n'
    printf '  },\n'
    printf '  "embedding_spec": {\n'
    printf '    "dims": 384,\n'
    printf '    "encoding": "f32le"\n'
    printf '  },\n'
    printf '  "block_size_target": 65536,\n'
    printf '  "stage": "%s"' "$(json_escape "$stage")"
    if [[ -n "$profile_version" ]]; then
      printf ',\n  "profile_version": "%s"' "$(json_escape "$profile_version")"
    fi
    printf ',\n'
    printf '  "items": [\n'

    if [[ "$include_items" == "yes" ]]; then
      for index in "${!MAILBOX_PATHS[@]}"; do
        mailbox_path="${MAILBOX_PATHS[$index]}"
        month="${DISCOVERED_MONTHS[$index]}"
        printf '    {\n'
        printf '      "kind": "mailbox",\n'
        printf '      "path": "%s",\n' "$(json_escape "$mailbox_path")"
        printf '      "metadata": {\n'
        printf '        "month": "%s"\n' "$(json_escape "$month")"
        printf '      }\n'
        if [[ "$index" -eq $((${#MAILBOX_PATHS[@]} - 1)) ]]; then
          printf '    }\n'
        else
          printf '    },\n'
        fi
      done
    fi

    printf '  ]\n'
    printf '}\n'
  } >"$request_path"
}

write_status_json() {
  local status_path="$1"
  local workflow_name="$2"
  local run_name="$3"
  local success_flag="$4"
  local manifest_path="$5"
  local artifact_prefix="$6"
  local extra_json="${7:-{}}"

  python3 - "$status_path" "$workflow_name" "$run_name" "$success_flag" "$manifest_path" "$artifact_prefix" "$extra_json" <<'PY'
import json
import sys

status_path, workflow_name, run_name, success_flag, manifest_path, artifact_prefix, extra_json = sys.argv[1:8]
payload = {
    "workflow": workflow_name,
    "run_name": run_name,
    "success": success_flag == "true",
    "manifest_path": manifest_path,
    "artifact_prefix": artifact_prefix,
}
payload.update(json.loads(extra_json))

with open(status_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}
