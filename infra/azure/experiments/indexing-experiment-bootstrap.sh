#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LexonArchiveBuilder contributors

set -euo pipefail

source /opt/lexonarchivebuilder/runner/workload.env

WORK_ROOT="/opt/lexonarchivebuilder/work/${RUN_NAME}"
HOSTED_SCRIPTS_DIR="${WORK_ROOT}/hosted-scripts"
mkdir -p "$WORK_ROOT"
printf '%s' "$MANIFEST_JSON_B64" | base64 -d > "${WORK_ROOT}/manifest.json"
mkdir -p "$HOSTED_SCRIPTS_DIR"
printf '%s' "$HOSTED_EXPERIMENT_COMMON_SCRIPT_B64" | base64 -d > "${HOSTED_SCRIPTS_DIR}/lexonarchivebuilder-hosted-experiment-common.sh"
printf '%s' "$HOSTED_EXPERIMENT_WORKLOAD_SCRIPT_B64" | base64 -d > "${HOSTED_SCRIPTS_DIR}/lexonarchivebuilder-indexing-experiment.sh"
chmod 0755 "${HOSTED_SCRIPTS_DIR}/lexonarchivebuilder-indexing-experiment.sh"

docker pull "$RUNNER_IMAGE" >/dev/null

docker run --rm \
  --entrypoint bash \
  -v "${HOSTED_SCRIPTS_DIR}:/workspace/scripts:ro" \
  -v "${WORK_ROOT}:/workspace/hosted-run" \
  "$RUNNER_IMAGE" \
  /workspace/scripts/lexonarchivebuilder-indexing-experiment.sh \
  --manifest /workspace/hosted-run/manifest.json \
  --container-sas-url "$CONTAINER_SAS_URL" \
  --profile-version "$PROFILE_VERSION" \
  --run-name "$RUN_NAME" \
  --dataset-block-store-prefix "$DATASET_BLOCK_STORE_PREFIX" \
  --dataset-replay-journal-prefix "$DATASET_REPLAY_JOURNAL_PREFIX" \
  --artifact-prefix "$ARTIFACT_PREFIX" \
  --block-store-target "$BLOCK_STORE_TARGET"
