#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lexonarchivebuilder-hosted-experiment-common.sh
source "${SCRIPT_DIR}/lexonarchivebuilder-hosted-experiment-common.sh"

usage() {
  cat <<'EOF'
Usage:
  scripts/lexonarchivebuilder-indexing-experiment.sh \
    --manifest PATH \
    --container-sas-url URL \
    --profile-version MAJOR.MINOR.PATCH \
    [--run-name NAME] \
    [--dataset-block-store-prefix PATH] \
    [--dataset-replay-journal-prefix PATH] \
    [--artifact-prefix PATH] \
    [--block-store-target filesystem|overlay]

This script:
  1. reads a checked-in manifest containing rsync URLs and the target container name
  2. downloads the reusable block store and replay journal from Azure Blob Storage
  3. runs clustering-and-block-assembly for one published profile
  4. runs rooted quality over the produced root
  5. uploads the updated block store plus experiment artifacts back to Azure Blob Storage
EOF
}

MANIFEST_PATH=""
CONTAINER_SAS_URL=""
PROFILE_VERSION=""
RUN_NAME=""
DATASET_BLOCK_STORE_PREFIX="datasets/block-store"
DATASET_REPLAY_JOURNAL_PREFIX="datasets/block-store.replay-journal"
ARTIFACT_PREFIX=""
BLOCK_STORE_TARGET="overlay"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --manifest)
      MANIFEST_PATH="$2"
      shift 2
      ;;
    --container-sas-url)
      CONTAINER_SAS_URL="$2"
      shift 2
      ;;
    --profile-version)
      PROFILE_VERSION="$2"
      shift 2
      ;;
    --run-name)
      RUN_NAME="$2"
      shift 2
      ;;
    --dataset-block-store-prefix)
      DATASET_BLOCK_STORE_PREFIX="$2"
      shift 2
      ;;
    --dataset-replay-journal-prefix)
      DATASET_REPLAY_JOURNAL_PREFIX="$2"
      shift 2
      ;;
    --artifact-prefix)
      ARTIFACT_PREFIX="$2"
      shift 2
      ;;
    --block-store-target)
      BLOCK_STORE_TARGET="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      printf 'error: unknown option: %s\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

require_commands azcopy date find lexonarchivebuilder-indexer python3
ensure_supported_block_store_target "$BLOCK_STORE_TARGET"

if [[ -z "$MANIFEST_PATH" || -z "$CONTAINER_SAS_URL" || -z "$PROFILE_VERSION" ]]; then
  usage >&2
  exit 1
fi

MANIFEST_PATH="$(resolve_input_path "$MANIFEST_PATH")"
load_manifest "$MANIFEST_PATH"
validate_profile_version "$PROFILE_VERSION"

if [[ -z "$RUN_NAME" ]]; then
  RUN_NAME="$(date -u '+%Y%m%dT%H%M%SZ')"
else
  validate_run_name "$RUN_NAME"
fi

if [[ -z "$ARTIFACT_PREFIX" ]]; then
  ARTIFACT_PREFIX="artifacts/indexing-experiment/${PROFILE_VERSION}/${RUN_NAME}"
fi

RUN_ROOT="${REPO_ROOT}/examples/local/scale-test/runs/${RUN_NAME}"
BLOCK_STORE_DIR="${RUN_ROOT}/block-store"
REPLAY_JOURNAL_DIR="${RUN_ROOT}/block-store.replay-journal"
REQUEST_PATH="${RUN_ROOT}/request.json"
SUMMARY_PATH="${RUN_ROOT}/summary.json"
QUALITY_PATH="${RUN_ROOT}/quality.json"
STATUS_PATH="${RUN_ROOT}/status.json"
RESULT_PATH="${RUN_ROOT}/result.json"
MANIFEST_COPY_PATH="${RUN_ROOT}/manifest.json"

mkdir -p "$RUN_ROOT" "$BLOCK_STORE_DIR" "$REPLAY_JOURNAL_DIR"
cp "$MANIFEST_PATH" "$MANIFEST_COPY_PATH"

SUCCESS=false
cleanup() {
  local exit_code=$?
  local status_block_store_prefix="$DATASET_BLOCK_STORE_PREFIX"
  local block_store_location="$DATASET_BLOCK_STORE_PREFIX"

  if [[ "$BLOCK_STORE_TARGET" == "overlay" ]]; then
    status_block_store_prefix=""
    block_store_location="container-root"
  fi

  if [[ $exit_code -eq 0 ]]; then
    SUCCESS=true
  fi

  write_status_json \
    "$STATUS_PATH" \
    "indexing-experiment" \
    "$RUN_NAME" \
    "$SUCCESS" \
    "$MANIFEST_PATH" \
    "$ARTIFACT_PREFIX" \
    "{\"profile_version\": \"$(json_escape "$PROFILE_VERSION")\", \"block_store_target\": \"$(json_escape "$BLOCK_STORE_TARGET")\", \"block_store_location\": \"$(json_escape "$block_store_location")\", \"dataset_block_store_prefix\": \"$(json_escape "$status_block_store_prefix")\", \"dataset_replay_journal_prefix\": \"$(json_escape "$DATASET_REPLAY_JOURNAL_PREFIX")\", \"container_name\": \"$(json_escape "$MANIFEST_CONTAINER_NAME")\"}"

  if [[ -f "$REQUEST_PATH" ]]; then
    upload_file_to_blob "$REQUEST_PATH" "$CONTAINER_SAS_URL" "${ARTIFACT_PREFIX}/request.json"
  fi
  if [[ -f "$SUMMARY_PATH" ]]; then
    upload_file_to_blob "$SUMMARY_PATH" "$CONTAINER_SAS_URL" "${ARTIFACT_PREFIX}/summary.json"
  fi
  if [[ -f "$QUALITY_PATH" ]]; then
    upload_file_to_blob "$QUALITY_PATH" "$CONTAINER_SAS_URL" "${ARTIFACT_PREFIX}/quality.json"
  fi
  if [[ -f "$RESULT_PATH" ]]; then
    upload_file_to_blob "$RESULT_PATH" "$CONTAINER_SAS_URL" "${ARTIFACT_PREFIX}/result.json"
  fi
  if [[ -f "$STATUS_PATH" ]]; then
    upload_file_to_blob "$STATUS_PATH" "$CONTAINER_SAS_URL" "${ARTIFACT_PREFIX}/status.json"
  fi
  if [[ -f "$MANIFEST_COPY_PATH" ]]; then
    upload_file_to_blob "$MANIFEST_COPY_PATH" "$CONTAINER_SAS_URL" "${ARTIFACT_PREFIX}/manifest.json"
  fi

  exit $exit_code
}
trap cleanup EXIT

if [[ "$BLOCK_STORE_TARGET" == "filesystem" ]]; then
  download_blob_tree_if_present "$CONTAINER_SAS_URL" "$DATASET_BLOCK_STORE_PREFIX" "$BLOCK_STORE_DIR"
fi
download_blob_tree_if_present "$CONTAINER_SAS_URL" "$DATASET_REPLAY_JOURNAL_PREFIX" "$REPLAY_JOURNAL_DIR"

if [[ "$BLOCK_STORE_TARGET" == "filesystem" ]] && [[ -z "$(find "$BLOCK_STORE_DIR" -type f -print -quit)" ]]; then
  printf 'error: no reusable block-store data was found under blob prefix %s; run the embedding refresh workflow first\n' "$DATASET_BLOCK_STORE_PREFIX" >&2
  exit 1
fi

write_mailbox_request \
  "$REQUEST_PATH" \
  "block-store" \
  "$CONTAINER_SAS_URL" \
  "$DATASET_BLOCK_STORE_PREFIX" \
  "$BLOCK_STORE_TARGET" \
  "http://127.0.0.1:8080" \
  "clustering-and-block-assembly" \
  "$PROFILE_VERSION" \
  "no"

lexonarchivebuilder-indexer run \
  --request "$REQUEST_PATH" \
  --stage clustering-and-block-assembly \
  --profile-version "$PROFILE_VERSION" \
  --summary-out "$SUMMARY_PATH"

ROOT_ID="$(
  python3 - "$SUMMARY_PATH" <<'PY'
import json
import sys

with open(sys.argv[1], 'r', encoding='utf-8') as handle:
    summary = json.load(handle)

root_id = summary.get('root_id')
if not isinstance(root_id, str) or not root_id:
    raise SystemExit('summary.json does not contain a non-empty root_id')

print(root_id)
PY
)"

if [[ "$BLOCK_STORE_TARGET" == "overlay" ]]; then
  OVERLAY_MEMORY_CACHE_MAX_RESIDENT_BLOCKS="$(overlay_memory_cache_max_resident_blocks)"
  lexonarchivebuilder-indexer quality \
    --root-id "$ROOT_ID" \
    --block-store-profile production \
    --block-store-container-sas-url "$CONTAINER_SAS_URL" \
    --block-store-filesystem-cache-root "$BLOCK_STORE_DIR" \
    --block-store-memory-cache-max-resident-blocks "$OVERLAY_MEMORY_CACHE_MAX_RESIDENT_BLOCKS" \
    --json-out "$QUALITY_PATH"
else
  lexonarchivebuilder-indexer quality \
    --root-id "$ROOT_ID" \
    --block-store-root "$BLOCK_STORE_DIR" \
    --json-out "$QUALITY_PATH"
fi

python3 - "$RESULT_PATH" "$PROFILE_VERSION" "$ROOT_ID" "$ARTIFACT_PREFIX" <<'PY'
import json
import sys

result_path, profile_version, root_id, artifact_prefix = sys.argv[1:5]
payload = {
    "profile_version": profile_version,
    "root_id": root_id,
    "quality_report": f"{artifact_prefix}/quality.json",
    "summary_report": f"{artifact_prefix}/summary.json",
}
with open(result_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY

if [[ "$BLOCK_STORE_TARGET" == "filesystem" ]]; then
  sync_local_tree_to_blob "$BLOCK_STORE_DIR" "$CONTAINER_SAS_URL" "$DATASET_BLOCK_STORE_PREFIX"
fi
sync_local_tree_to_blob "$REPLAY_JOURNAL_DIR" "$CONTAINER_SAS_URL" "$DATASET_REPLAY_JOURNAL_PREFIX"
