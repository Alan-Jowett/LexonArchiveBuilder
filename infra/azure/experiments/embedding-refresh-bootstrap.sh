#!/usr/bin/env bash

set -euo pipefail

source /opt/lexonarchivebuilder/runner/workload.env

WORK_ROOT="/opt/lexonarchivebuilder/work/${RUN_NAME}"
mkdir -p "$WORK_ROOT"

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
