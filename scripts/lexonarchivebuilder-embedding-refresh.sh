#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LexonArchiveBuilder contributors

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lexonarchivebuilder-hosted-experiment-common.sh
source "${SCRIPT_DIR}/lexonarchivebuilder-hosted-experiment-common.sh"

usage() {
  cat <<'EOF'
Usage:
  scripts/lexonarchivebuilder-embedding-refresh.sh \
    --manifest PATH \
    --container-sas-url URL \
    [--run-name NAME] \
    [--dataset-block-store-prefix PATH] \
    [--dataset-replay-journal-prefix PATH] \
    [--artifact-prefix PATH] \
    [--block-store-target filesystem|overlay] \
    [--embedding-base-url URL]

This script:
  1. reads a checked-in manifest containing rsync URLs and the target container name
  2. downloads any existing filesystem-backed block store and replay journal from Azure Blob Storage
  3. fetches rsync-backed mailbox content and discovers .mail/.mbox files
  4. runs only the ingestion-and-embedding stage
  5. uploads the refreshed block store, replay journal, and run artifacts back to Azure Blob Storage
EOF
}

MANIFEST_PATH=""
CONTAINER_SAS_URL=""
RUN_NAME=""
DATASET_BLOCK_STORE_PREFIX="datasets/block-store"
DATASET_REPLAY_JOURNAL_PREFIX="datasets/block-store.replay-journal"
ARTIFACT_PREFIX=""
BLOCK_STORE_TARGET="overlay"
EMBEDDING_BASE_URL="${SCALE_TEST_EMBEDDING_BASE_URL:-http://stapi:8080}"

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
    --embedding-base-url)
      EMBEDDING_BASE_URL="$2"
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

require_commands azcopy date find lexonarchivebuilder-indexer python3 rsync sort
ensure_supported_block_store_target "$BLOCK_STORE_TARGET"

if [[ -z "$MANIFEST_PATH" || -z "$CONTAINER_SAS_URL" ]]; then
  usage >&2
  exit 1
fi

MANIFEST_PATH="$(resolve_input_path "$MANIFEST_PATH")"
load_manifest "$MANIFEST_PATH"

if [[ -z "$RUN_NAME" ]]; then
  RUN_NAME="$(date -u '+%Y%m%dT%H%M%SZ')"
else
  validate_run_name "$RUN_NAME"
fi

if [[ -z "$ARTIFACT_PREFIX" ]]; then
  ARTIFACT_PREFIX="artifacts/embedding-refresh/${RUN_NAME}"
fi

RUN_ROOT="${REPO_ROOT}/examples/local/scale-test/runs/${RUN_NAME}"
BLOCK_STORE_DIR="${RUN_ROOT}/block-store"
REPLAY_JOURNAL_DIR="${RUN_ROOT}/block-store.replay-journal"
REQUEST_PATH="${RUN_ROOT}/request.json"
SUMMARY_PATH="${RUN_ROOT}/summary.json"
STATUS_PATH="${RUN_ROOT}/status.json"
MANIFEST_COPY_PATH="${RUN_ROOT}/manifest.json"
SOURCES_LOG="${RUN_ROOT}/sources.txt"

mkdir -p "$RUN_ROOT" "$BLOCK_STORE_DIR" "$REPLAY_JOURNAL_DIR"
cp "$MANIFEST_PATH" "$MANIFEST_COPY_PATH"

SUCCESS=false
cleanup() {
  local exit_code=$?
  local summary_blob="${ARTIFACT_PREFIX}/summary.json"
  local request_blob="${ARTIFACT_PREFIX}/request.json"
  local status_blob="${ARTIFACT_PREFIX}/status.json"
  local sources_blob="${ARTIFACT_PREFIX}/sources.txt"
  local manifest_blob="${ARTIFACT_PREFIX}/manifest.json"
  local status_block_store_prefix="$DATASET_BLOCK_STORE_PREFIX"
  local block_store_location="$DATASET_BLOCK_STORE_PREFIX"
  local status_extra_json

  if [[ "$BLOCK_STORE_TARGET" == "overlay" ]]; then
    status_block_store_prefix=""
    block_store_location="container-root"
  fi

  if [[ $exit_code -eq 0 ]]; then
    SUCCESS=true
  fi

  status_extra_json="$(
    python3 - "$BLOCK_STORE_TARGET" "$block_store_location" "$status_block_store_prefix" "$DATASET_REPLAY_JOURNAL_PREFIX" "$MANIFEST_CONTAINER_NAME" <<'PY'
import json
import sys

block_store_target, block_store_location, dataset_block_store_prefix, dataset_replay_journal_prefix, container_name = sys.argv[1:6]
print(json.dumps({
    "phase": "workload",
    "block_store_target": block_store_target,
    "block_store_location": block_store_location,
    "dataset_block_store_prefix": dataset_block_store_prefix,
    "dataset_replay_journal_prefix": dataset_replay_journal_prefix,
    "container_name": container_name,
}, sort_keys=True))
PY
  )"

  write_status_json \
    "$STATUS_PATH" \
    "embedding-refresh" \
    "$RUN_NAME" \
    "$SUCCESS" \
    "$MANIFEST_PATH" \
    "$ARTIFACT_PREFIX" \
    "$status_extra_json"

  if [[ -f "$SUMMARY_PATH" ]]; then
    upload_file_to_blob "$SUMMARY_PATH" "$CONTAINER_SAS_URL" "$summary_blob"
  fi
  if [[ -f "$REQUEST_PATH" ]]; then
    upload_file_to_blob "$REQUEST_PATH" "$CONTAINER_SAS_URL" "$request_blob"
  fi
  if [[ -f "$STATUS_PATH" ]]; then
    upload_file_to_blob "$STATUS_PATH" "$CONTAINER_SAS_URL" "$status_blob"
  fi
  if [[ -f "$SOURCES_LOG" ]]; then
    upload_file_to_blob "$SOURCES_LOG" "$CONTAINER_SAS_URL" "$sources_blob"
  fi
  if [[ -f "$MANIFEST_COPY_PATH" ]]; then
    upload_file_to_blob "$MANIFEST_COPY_PATH" "$CONTAINER_SAS_URL" "$manifest_blob"
  fi

  exit $exit_code
}
trap cleanup EXIT

printf '%s\n' "${MANIFEST_SOURCES[@]}" >"$SOURCES_LOG"

if [[ "$BLOCK_STORE_TARGET" == "filesystem" ]]; then
  download_blob_tree_if_present "$CONTAINER_SAS_URL" "$DATASET_BLOCK_STORE_PREFIX" "$BLOCK_STORE_DIR"
fi
download_blob_tree_if_present "$CONTAINER_SAS_URL" "$DATASET_REPLAY_JOURNAL_PREFIX" "$REPLAY_JOURNAL_DIR"

mirror_manifest_sources "$RUN_ROOT"
write_mailbox_request \
  "$REQUEST_PATH" \
  "block-store" \
  "$CONTAINER_SAS_URL" \
  "$BLOCK_STORE_TARGET" \
  "$EMBEDDING_BASE_URL" \
  "ingestion-and-embedding" \
  "" \
  "yes"

printf 'Discovered %d mailbox files\n' "${#MAILBOX_PATHS[@]}"
printf 'Refreshing reusable embeddings for container %s\n' "$MANIFEST_CONTAINER_NAME"

lexonarchivebuilder-indexer run \
  --request "$REQUEST_PATH" \
  --stage ingestion-and-embedding \
  --summary-out "$SUMMARY_PATH"

if [[ "$BLOCK_STORE_TARGET" == "filesystem" ]]; then
  sync_local_tree_to_blob "$BLOCK_STORE_DIR" "$CONTAINER_SAS_URL" "$DATASET_BLOCK_STORE_PREFIX"
fi
sync_local_tree_to_blob "$REPLAY_JOURNAL_DIR" "$CONTAINER_SAS_URL" "$DATASET_REPLAY_JOURNAL_PREFIX"
