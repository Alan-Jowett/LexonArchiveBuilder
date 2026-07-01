#!/usr/bin/env bash

set -euo pipefail

source /opt/lexonarchivebuilder/runner/workload.env

WORK_ROOT="/opt/lexonarchivebuilder/work/${RUN_NAME}"
mkdir -p "$WORK_ROOT"

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

cleanup() {
  docker rm -f lexonarchivebuilder-experiment-stapi >/dev/null 2>&1 || true
}
trap cleanup EXIT

printf '%s' "$MANIFEST_JSON_B64" | base64 -d > "${WORK_ROOT}/manifest.json"

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
