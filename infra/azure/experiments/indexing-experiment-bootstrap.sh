#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LexonArchiveBuilder contributors

set -euo pipefail

source /opt/lexonarchivebuilder/runner/workload.env

WORK_ROOT="/opt/lexonarchivebuilder/work/${RUN_NAME}"
mkdir -p "$WORK_ROOT"
printf '%s' "$MANIFEST_JSON_B64" | base64 -d > "${WORK_ROOT}/manifest.json"

docker pull "$RUNNER_IMAGE" >/dev/null

docker run --rm \
  --entrypoint bash \
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
