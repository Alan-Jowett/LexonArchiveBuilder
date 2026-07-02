#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LexonArchiveBuilder contributors

HOSTED_WORKFLOW_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

hosted_workflow_validate_runner_image_tag() {
  local candidate="$1"
  if [[ ! "$candidate" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]; then
    printf 'runner_image_tag must be a valid Docker tag (1-128 chars: letters, digits, underscores, periods, or hyphens)\n' >&2
    exit 1
  fi
}

hosted_workflow_validate_profile_version() {
  local candidate="$1"
  if [[ ! "$candidate" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf 'profile_version must use MAJOR.MINOR.PATCH format\n' >&2
    exit 1
  fi
}

hosted_workflow_normalize_manifest_json() {
  local manifest_path="$1"
  python3 - "$manifest_path" <<'PY'
import json
import re
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    data = json.load(handle)

container_name = data.get("container_name")
if not isinstance(container_name, str) or not container_name.strip():
    raise SystemExit('manifest must contain non-empty string field "container_name"')
container_name = container_name.strip()
if (
    len(container_name) < 3
    or len(container_name) > 63
    or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?", container_name)
):
    raise SystemExit(
        'manifest field "container_name" must be a valid Azure Blob container name '
        "(3-63 lowercase alphanumeric characters or hyphens, starting and ending with an alphanumeric character)"
    )
data["container_name"] = container_name

if not isinstance(data.get("sources"), list) or not data["sources"]:
    raise SystemExit('manifest must contain non-empty array field "sources"')
normalized_sources = []
for source in data["sources"]:
    if not isinstance(source, str) or not source.strip():
        raise SystemExit("manifest sources must be non-empty strings")
    normalized_sources.append(source.strip())
data["sources"] = normalized_sources

print(json.dumps(data, separators=(",", ":"), sort_keys=True))
PY
}

hosted_workflow_extract_container_name() {
  local manifest_path="$1"
  python3 - "$manifest_path" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    print(json.load(handle)["container_name"].strip())
PY
}

hosted_workflow_render_shell_env() {
  python3 "${HOSTED_WORKFLOW_COMMON_DIR}/lexonarchivebuilder-render-shell-env.py" "$@"
}

hosted_workflow_render_embedding_workload_env_file() {
  local run_name="$1"
  local runner_image="$2"
  local stapi_image="$3"
  local manifest_json_b64="$4"
  local hosted_experiment_common_script_b64="$5"
  local hosted_experiment_workload_script_b64="$6"
  local dataset_block_store_prefix="$7"
  local dataset_replay_journal_prefix="$8"
  local artifact_prefix="$9"
  local block_store_target="${10}"
  local debug_retain_failed_vm="${11}"

  hosted_workflow_render_shell_env \
    "RUN_NAME=${run_name}" \
    "RUNNER_IMAGE=${runner_image}" \
    "STAPI_IMAGE=${stapi_image}" \
    "MANIFEST_JSON_B64=${manifest_json_b64}" \
    "HOSTED_EXPERIMENT_COMMON_SCRIPT_B64=${hosted_experiment_common_script_b64}" \
    "HOSTED_EXPERIMENT_WORKLOAD_SCRIPT_B64=${hosted_experiment_workload_script_b64}" \
    "DATASET_BLOCK_STORE_PREFIX=${dataset_block_store_prefix}" \
    "DATASET_REPLAY_JOURNAL_PREFIX=${dataset_replay_journal_prefix}" \
    "ARTIFACT_PREFIX=${artifact_prefix}" \
    "BLOCK_STORE_TARGET=${block_store_target}" \
    "DEBUG_RETAIN_ON_FAILURE=${debug_retain_failed_vm}"
}

hosted_workflow_render_indexing_workload_env_file() {
  local run_name="$1"
  local runner_image="$2"
  local manifest_json_b64="$3"
  local hosted_experiment_common_script_b64="$4"
  local hosted_experiment_workload_script_b64="$5"
  local dataset_block_store_prefix="$6"
  local dataset_replay_journal_prefix="$7"
  local artifact_prefix="$8"
  local block_store_target="$9"
  local debug_retain_failed_vm="${10}"
  local profile_version="${11}"

  hosted_workflow_render_shell_env \
    "RUN_NAME=${run_name}" \
    "RUNNER_IMAGE=${runner_image}" \
    "MANIFEST_JSON_B64=${manifest_json_b64}" \
    "HOSTED_EXPERIMENT_COMMON_SCRIPT_B64=${hosted_experiment_common_script_b64}" \
    "HOSTED_EXPERIMENT_WORKLOAD_SCRIPT_B64=${hosted_experiment_workload_script_b64}" \
    "DATASET_BLOCK_STORE_PREFIX=${dataset_block_store_prefix}" \
    "DATASET_REPLAY_JOURNAL_PREFIX=${dataset_replay_journal_prefix}" \
    "ARTIFACT_PREFIX=${artifact_prefix}" \
    "BLOCK_STORE_TARGET=${block_store_target}" \
    "DEBUG_RETAIN_ON_FAILURE=${debug_retain_failed_vm}" \
    "PROFILE_VERSION=${profile_version}"
}
