#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LexonArchiveBuilder contributors

set -euo pipefail

source /opt/lexonarchivebuilder/runner/workload.env

WORK_ROOT="/opt/lexonarchivebuilder/work/${RUN_NAME}"
HOSTED_SCRIPTS_DIR="${WORK_ROOT}/hosted-scripts"
mkdir -p "$WORK_ROOT"
BOOTSTRAP_LOG_PATH="${WORK_ROOT}/bootstrap-wrapper.log"
BOOTSTRAP_STATUS_PATH="${WORK_ROOT}/bootstrap-status.json"
BOOTSTRAP_CLOUD_INIT_LOG_PATH="${WORK_ROOT}/bootstrap-cloud-init-output.log"
BOOTSTRAP_STAPI_LOG_PATH="${WORK_ROOT}/bootstrap-stapi.log"

wait_for_tcp_port() {
  local host="$1"
  local port="$2"
  local timeout_secs="$3"
  local start_ts
  start_ts="$(date +%s)"

  while true; do
    if bash -c "</dev/tcp/${host}/${port}" >/dev/null 2>&1; then
      return 0
    fi

    if (( "$(date +%s)" - start_ts >= timeout_secs )); then
      printf 'error: timed out waiting for %s:%s to accept TCP connections\n' "$host" "$port" >&2
      return 1
    fi

    sleep 1
  done
}

blob_url_for() {
  local blob_path="$1"
  local base_url query
  base_url="${CONTAINER_SAS_URL%%\?*}"
  query="${CONTAINER_SAS_URL#*\?}"
  printf '%s/%s?%s' "${base_url%/}" "${blob_path#/}" "$query"
}

upload_blob_file() {
  local source_file="$1"
  local blob_path="$2"
  local blob_url
  blob_url="$(blob_url_for "$blob_path")"
  curl --fail --silent --show-error \
    -X PUT \
    -H 'x-ms-blob-type: BlockBlob' \
    -H 'x-ms-version: 2023-11-03' \
    --upload-file "$source_file" \
    "$blob_url"
}

workload_status_exists() {
  local status_url
  status_url="$(blob_url_for "${ARTIFACT_PREFIX}/status.json")"
  curl --fail --silent --show-error --location "$status_url" --output /dev/null
}

write_bootstrap_status() {
  local exit_code="$1"
  python3 - "$BOOTSTRAP_STATUS_PATH" "$RUN_NAME" "$ARTIFACT_PREFIX" "$exit_code" <<'PY'
import json
import sys

status_path, run_name, artifact_prefix, exit_code = sys.argv[1:5]
payload = {
    "workflow": "embedding-refresh",
    "run_name": run_name,
    "success": False,
    "phase": "bootstrap",
    "artifact_prefix": artifact_prefix,
    "exit_code": int(exit_code),
}

with open(status_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

publish_bootstrap_failure_artifacts() {
  local exit_code="$1"

  if workload_status_exists >/dev/null 2>&1; then
    return 0
  fi

  write_bootstrap_status "$exit_code"

  if [[ -f /var/log/cloud-init-output.log ]]; then
    cp /var/log/cloud-init-output.log "$BOOTSTRAP_CLOUD_INIT_LOG_PATH"
  fi

  if docker ps -a --format '{{.Names}}' | grep -Fx 'lexonarchivebuilder-experiment-stapi' >/dev/null 2>&1; then
    docker logs lexonarchivebuilder-experiment-stapi >"$BOOTSTRAP_STAPI_LOG_PATH" 2>&1 || printf 'warning: failed to capture stapi container logs\n' >&2
  fi

  upload_blob_file "$BOOTSTRAP_STATUS_PATH" "${ARTIFACT_PREFIX}/bootstrap-status.json" || printf 'warning: failed to upload bootstrap-status.json\n' >&2
  upload_blob_file "$BOOTSTRAP_LOG_PATH" "${ARTIFACT_PREFIX}/bootstrap-wrapper.log" || printf 'warning: failed to upload bootstrap-wrapper.log\n' >&2
  if [[ -f "$BOOTSTRAP_CLOUD_INIT_LOG_PATH" ]]; then
    upload_blob_file "$BOOTSTRAP_CLOUD_INIT_LOG_PATH" "${ARTIFACT_PREFIX}/bootstrap-cloud-init-output.log" || printf 'warning: failed to upload bootstrap-cloud-init-output.log\n' >&2
  fi
  if [[ -f "$BOOTSTRAP_STAPI_LOG_PATH" ]]; then
    upload_blob_file "$BOOTSTRAP_STAPI_LOG_PATH" "${ARTIFACT_PREFIX}/bootstrap-stapi.log" || printf 'warning: failed to upload bootstrap-stapi.log\n' >&2
  fi
  if [[ -f "${WORK_ROOT}/manifest.json" ]]; then
    upload_blob_file "${WORK_ROOT}/manifest.json" "${ARTIFACT_PREFIX}/manifest.json" || printf 'warning: failed to upload manifest.json\n' >&2
  fi
}

cleanup() {
  local exit_code=$?
  if [[ $exit_code -ne 0 ]]; then
    publish_bootstrap_failure_artifacts "$exit_code"
  fi
  docker rm -f lexonarchivebuilder-experiment-stapi >/dev/null 2>&1 || true
  exit $exit_code
}
trap cleanup EXIT

exec > >(tee -a "$BOOTSTRAP_LOG_PATH") 2>&1

printf '%s' "$MANIFEST_JSON_B64" | base64 -d > "${WORK_ROOT}/manifest.json"
mkdir -p "$HOSTED_SCRIPTS_DIR"
printf '%s' "$HOSTED_EXPERIMENT_COMMON_SCRIPT_B64" | base64 -d > "${HOSTED_SCRIPTS_DIR}/lexonarchivebuilder-hosted-experiment-common.sh"
printf '%s' "$HOSTED_EXPERIMENT_WORKLOAD_SCRIPT_B64" | base64 -d > "${HOSTED_SCRIPTS_DIR}/lexonarchivebuilder-embedding-refresh.sh"
chmod 0755 "${HOSTED_SCRIPTS_DIR}/lexonarchivebuilder-embedding-refresh.sh"

docker pull "$RUNNER_IMAGE" >/dev/null
docker pull "$STAPI_IMAGE" >/dev/null
docker rm -f lexonarchivebuilder-experiment-stapi >/dev/null 2>&1 || true
docker run -d \
  --name lexonarchivebuilder-experiment-stapi \
  -p 8080:8080 \
  "$STAPI_IMAGE" >/dev/null
wait_for_tcp_port 127.0.0.1 8080 "${STAPI_WAIT_TIMEOUT_SECS:-60}"

docker run --rm \
  --entrypoint bash \
  --add-host host.docker.internal:host-gateway \
  -v "${HOSTED_SCRIPTS_DIR}:/workspace/scripts:ro" \
  -v "${WORK_ROOT}:/workspace/hosted-run" \
  "$RUNNER_IMAGE" \
  /workspace/scripts/lexonarchivebuilder-embedding-refresh.sh \
  --manifest /workspace/hosted-run/manifest.json \
  --container-sas-url "$CONTAINER_SAS_URL" \
  --run-name "$RUN_NAME" \
  --dataset-block-store-prefix "$DATASET_BLOCK_STORE_PREFIX" \
  --dataset-replay-journal-prefix "$DATASET_REPLAY_JOURNAL_PREFIX" \
  --artifact-prefix "$ARTIFACT_PREFIX" \
  --block-store-target "$BLOCK_STORE_TARGET" \
  --embedding-base-url "http://host.docker.internal:8080"
