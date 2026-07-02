#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LexonArchiveBuilder contributors

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${SCRIPT_DIR}/lexonarchivebuilder-hosted-workflow-common.sh"

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

base64_encode_file() {
  local source_file="$1"
  python3 - "$source_file" <<'PY'
import base64
import pathlib
import sys

print(base64.b64encode(pathlib.Path(sys.argv[1]).read_bytes()).decode('ascii'), end='')
PY
}

base64_encode_text() {
  local source_text="$1"
  python3 - "$source_text" <<'PY'
import base64
import sys

print(base64.b64encode(sys.argv[1].encode('utf-8')).decode('ascii'), end='')
PY
}

base64_decode_to_file() {
  local encoded_text="$1"
  local destination_path="$2"
  python3 - "$encoded_text" "$destination_path" <<'PY'
import base64
import pathlib
import sys

pathlib.Path(sys.argv[2]).write_bytes(base64.b64decode(sys.argv[1].encode('ascii')))
PY
}

assert_file_equals() {
  local expected="$1"
  local actual="$2"
  if ! cmp -s "$expected" "$actual"; then
    printf 'error: files differ: %s != %s\n' "$expected" "$actual" >&2
    exit 1
  fi
}

assert_env_roundtrip() {
  local env_path="$1"
  local expected_run_name="$2"
  local expected_container_sas_url="$3"
  local expected_storage_account_name="$4"
  local expected_container_name="$5"
  local expected_artifact_prefix="$6"
  local expected_block_store_target="$7"
  local expected_workload_script="$8"
  local expected_common_script="$9"
  local expected_debug_retain_on_failure="${10:-}"
  local expected_capture_workload_log="${11:-}"
  local expected_profile_version="${12:-}"

  local decoded_common_script="${env_path}.decoded-common.sh"
  local decoded_workload_script="${env_path}.decoded-workload.sh"
  (
    set -euo pipefail
    source "$env_path"
    [[ "$RUN_NAME" == "$expected_run_name" ]] || { printf 'error: RUN_NAME roundtrip failed\n' >&2; exit 1; }
    [[ "$CONTAINER_SAS_URL" == "$expected_container_sas_url" ]] || { printf 'error: CONTAINER_SAS_URL roundtrip failed\n' >&2; exit 1; }
    [[ "$STORAGE_ACCOUNT_NAME" == "$expected_storage_account_name" ]] || { printf 'error: STORAGE_ACCOUNT_NAME roundtrip failed\n' >&2; exit 1; }
    [[ "$CONTAINER_NAME" == "$expected_container_name" ]] || { printf 'error: CONTAINER_NAME roundtrip failed\n' >&2; exit 1; }
    [[ "$ARTIFACT_PREFIX" == "$expected_artifact_prefix" ]] || { printf 'error: ARTIFACT_PREFIX roundtrip failed\n' >&2; exit 1; }
    [[ "$BLOCK_STORE_TARGET" == "$expected_block_store_target" ]] || { printf 'error: BLOCK_STORE_TARGET roundtrip failed\n' >&2; exit 1; }
    if [[ -n "$expected_debug_retain_on_failure" ]]; then
      [[ "${DEBUG_RETAIN_ON_FAILURE:-}" == "$expected_debug_retain_on_failure" ]] || { printf 'error: DEBUG_RETAIN_ON_FAILURE roundtrip failed\n' >&2; exit 1; }
    fi
    if [[ -n "$expected_capture_workload_log" ]]; then
      [[ "${HOSTED_EXPERIMENT_CAPTURE_WORKLOAD_LOG:-}" == "$expected_capture_workload_log" ]] || { printf 'error: HOSTED_EXPERIMENT_CAPTURE_WORKLOAD_LOG roundtrip failed\n' >&2; exit 1; }
    fi
    if [[ -n "$expected_profile_version" ]]; then
      [[ "$PROFILE_VERSION" == "$expected_profile_version" ]] || { printf 'error: PROFILE_VERSION roundtrip failed\n' >&2; exit 1; }
    fi

    base64_decode_to_file "$HOSTED_EXPERIMENT_COMMON_SCRIPT_B64" "$decoded_common_script"
    base64_decode_to_file "$HOSTED_EXPERIMENT_WORKLOAD_SCRIPT_B64" "$decoded_workload_script"
  )

  assert_file_equals "$expected_common_script" "$decoded_common_script"
  assert_file_equals "$expected_workload_script" "$decoded_workload_script"
}

assert_generated_parameters() {
  local parameters_path="$1"
  local expected_workflow_name="$2"
  local expected_workload_env_file="$3"
  local expected_workload_script_path="$4"
  local expected_container_sas_url="$5"
  local expected_batch_resource_group="$6"

  python3 - "$parameters_path" "$expected_workflow_name" "$expected_workload_env_file" "$expected_workload_script_path" "$expected_container_sas_url" "$expected_batch_resource_group" <<'PY'
import json
import pathlib
import sys

(
    parameters_path,
    expected_workflow_name,
    expected_workload_env_file,
    expected_workload_script_path,
    expected_container_sas_url,
    expected_batch_resource_group,
) = sys.argv[1:7]
payload = json.loads(pathlib.Path(parameters_path).read_text(encoding="utf-8"))
params = payload["parameters"]
tags = params["tags"]["value"]

assert tags["lexon-workflow"] == expected_workflow_name
assert tags["lexon-scope"] == "batch"
assert tags["lexon-batch-resource-group"] == expected_batch_resource_group
assert "lexon-manifest" not in tags
assert params["workloadEnvironmentFile"]["value"] == expected_workload_env_file
assert params["workloadScript"]["value"] == pathlib.Path(expected_workload_script_path).read_text(encoding="utf-8")
assert params["containerSasUrl"]["value"] == expected_container_sas_url
PY
}

assert_workflow_contains() {
local workflow_path="$1"
shift
python3 - "$workflow_path" "$@" <<'PY'
import pathlib
import sys

workflow_path = pathlib.Path(sys.argv[1])
text = workflow_path.read_text(encoding="utf-8")
missing = [snippet for snippet in sys.argv[2:] if snippet not in text]
if missing:
  raise SystemExit(
      f"workflow contract check failed for {workflow_path}: missing snippets: {missing}"
  )
PY
}

assert_file_contains() {
local file_path="$1"
shift
python3 - "$file_path" "$@" <<'PY'
import pathlib
import sys

file_path = pathlib.Path(sys.argv[1])
text = file_path.read_text(encoding="utf-8")
missing = [snippet for snippet in sys.argv[2:] if snippet not in text]
if missing:
  raise SystemExit(
      f"file contract check failed for {file_path}: missing snippets: {missing}"
  )
PY
}

render_final_workload_env() {
  local base_env_path="$1"
  local container_sas_url="$2"
  local storage_account_name="$3"
  local container_name="$4"

  cp "$base_env_path" "${base_env_path}.final"
  printf '\n' >> "${base_env_path}.final"
  python3 "${REPO_ROOT}/infra/azure/experiments/render-workload-storage-env.py" \
    "$container_sas_url" \
    "$storage_account_name" \
    "$container_name" >> "${base_env_path}.final"
  printf '%s' "${base_env_path}.final"
}

require_commands python3 cmp

MANIFEST_PATH="${REPO_ROOT}/examples/local/scale-test/manifests/ietf-mailing-lists.sample.json"
EMBEDDING_BOOTSTRAP_SCRIPT="${REPO_ROOT}/infra/azure/experiments/embedding-refresh-bootstrap.sh"
INDEXING_BOOTSTRAP_SCRIPT="${REPO_ROOT}/infra/azure/experiments/indexing-experiment-bootstrap.sh"
HOSTED_EXPERIMENT_COMMON_SCRIPT="${REPO_ROOT}/scripts/lexonarchivebuilder-hosted-experiment-common.sh"
EMBEDDING_WORKLOAD_SCRIPT="${REPO_ROOT}/scripts/lexonarchivebuilder-embedding-refresh.sh"
INDEXING_WORKLOAD_SCRIPT="${REPO_ROOT}/scripts/lexonarchivebuilder-indexing-experiment.sh"
EMBEDDING_WORKFLOW="${REPO_ROOT}/.github/workflows/run-embedding-refresh.yml"
INDEXING_WORKFLOW="${REPO_ROOT}/.github/workflows/run-indexing-experiment.yml"
EXPERIMENT_MAIN_BICEP="${REPO_ROOT}/infra/azure/experiments/main.bicep"
EXPERIMENT_STORAGE_BICEP="${REPO_ROOT}/infra/azure/experiments/storage.bicep"
EXPERIMENT_VM_RUNNER_BICEP="${REPO_ROOT}/infra/azure/experiments/vm-runner.bicep"

TEMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TEMP_ROOT"' EXIT

manifest_json_minified="$(hosted_workflow_normalize_manifest_json "$MANIFEST_PATH")"
container_name="$(hosted_workflow_extract_container_name "$MANIFEST_PATH")"

runner_image="ghcr.io/example/lexonarchivebuilder-scale-test:main"
stapi_image="ghcr.io/substratusai/stapi:v2.2.2-3"
artifact_prefix_embedding="artifacts/embedding-refresh/embed-preflight"
artifact_prefix_indexing="artifacts/indexing-experiment/0.6.0/index-preflight"
dataset_block_store_prefix="datasets/block-store"
dataset_replay_journal_prefix="datasets/block-store.replay-journal"
container_sas_url="https://example.blob.core.windows.net/${container_name}?sig=quoted'value&sp=racwl&restype=container"
storage_account_name="lexonpreflightsa"
ssh_public_key="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCtest-preflight"
ssh_source_prefixes_json='["203.0.113.10/32"]'
preflight_sas_expiry="2099-01-01T00:00:00Z"

hosted_experiment_common_script_b64="$(base64_encode_file "$HOSTED_EXPERIMENT_COMMON_SCRIPT")"
embedding_workload_script_b64="$(base64_encode_file "$EMBEDDING_WORKLOAD_SCRIPT")"
indexing_workload_script_b64="$(base64_encode_file "$INDEXING_WORKLOAD_SCRIPT")"
manifest_json_b64="$(base64_encode_text "$manifest_json_minified")"

embedding_env_file="$(
  hosted_workflow_render_embedding_workload_env_file \
    "embed-preflight" \
    "$runner_image" \
    "$stapi_image" \
    "$manifest_json_b64" \
    "$hosted_experiment_common_script_b64" \
    "$embedding_workload_script_b64" \
    "$dataset_block_store_prefix" \
    "$dataset_replay_journal_prefix" \
    "$artifact_prefix_embedding" \
    "overlay" \
    "false" \
    "false"
)"
printf '%s\n' "$embedding_env_file" > "${TEMP_ROOT}/embedding.env"
embedding_final_env="$(render_final_workload_env "${TEMP_ROOT}/embedding.env" "$container_sas_url" "$storage_account_name" "$container_name")"

assert_env_roundtrip \
  "$embedding_final_env" \
  "embed-preflight" \
  "$container_sas_url" \
  "$storage_account_name" \
  "$container_name" \
  "$artifact_prefix_embedding" \
  "overlay" \
  "$EMBEDDING_WORKLOAD_SCRIPT" \
  "$HOSTED_EXPERIMENT_COMMON_SCRIPT" \
  "false" \
  "false"

python3 "${REPO_ROOT}/scripts/lexonarchivebuilder-write-deployment-parameters.py" \
  --workflow-name embedding-refresh \
  --location eastus \
  --resource-group lexon-exp-preflight-batch-20260701-123456 \
  --vnet-name lexon-exp-preflight-batch-vnet-123456789-1 \
  --storage-account-name "$storage_account_name" \
  --container-name "$container_name" \
  --container-sas-url "$container_sas_url" \
  --vm-name lexon-exp-embed-preflight \
  --ssh-public-key "$ssh_public_key" \
  --enable-public-ip true \
  --enable-ssh-access true \
  --ssh-source-prefixes-json "$ssh_source_prefixes_json" \
  --workload-environment-file "$embedding_env_file" \
  --workload-script-path "$EMBEDDING_BOOTSTRAP_SCRIPT" \
  --github-run-id 123456789 \
  --github-run-attempt 1 > "${TEMP_ROOT}/embedding.parameters.json"
assert_generated_parameters \
  "${TEMP_ROOT}/embedding.parameters.json" \
  embedding-refresh \
  "$embedding_env_file" \
  "$EMBEDDING_BOOTSTRAP_SCRIPT" \
  "$container_sas_url" \
  lexon-exp-preflight-batch-20260701-123456

indexing_env_file="$(
  hosted_workflow_render_indexing_workload_env_file \
    "index-preflight" \
    "$runner_image" \
    "$manifest_json_b64" \
    "$hosted_experiment_common_script_b64" \
    "$indexing_workload_script_b64" \
    "$dataset_block_store_prefix" \
    "$dataset_replay_journal_prefix" \
    "$artifact_prefix_indexing" \
    "filesystem" \
    "false" \
    "0.6.0"
)"
printf '%s\n' "$indexing_env_file" > "${TEMP_ROOT}/indexing.env"
indexing_final_env="$(render_final_workload_env "${TEMP_ROOT}/indexing.env" "$container_sas_url" "$storage_account_name" "$container_name")"

assert_env_roundtrip \
  "$indexing_final_env" \
  "index-preflight" \
  "$container_sas_url" \
  "$storage_account_name" \
  "$container_name" \
  "$artifact_prefix_indexing" \
  "filesystem" \
  "$INDEXING_WORKLOAD_SCRIPT" \
  "$HOSTED_EXPERIMENT_COMMON_SCRIPT" \
  "false" \
  "" \
  "0.6.0"

python3 "${REPO_ROOT}/scripts/lexonarchivebuilder-write-deployment-parameters.py" \
  --workflow-name indexing-experiment \
  --location eastus \
  --resource-group lexon-exp-preflight-batch-20260701-123457 \
  --vnet-name lexon-exp-preflight-batch-vnet-123456789-1 \
  --storage-account-name "$storage_account_name" \
  --container-name "$container_name" \
  --container-sas-url "$container_sas_url" \
  --vm-name lexon-exp-index-preflight \
  --ssh-public-key "$ssh_public_key" \
  --enable-public-ip false \
  --enable-ssh-access false \
  --ssh-source-prefixes-json '[]' \
  --workload-environment-file "$indexing_env_file" \
  --workload-script-path "$INDEXING_BOOTSTRAP_SCRIPT" \
  --github-run-id 123456789 \
  --github-run-attempt 1 > "${TEMP_ROOT}/indexing.parameters.json"
assert_generated_parameters \
  "${TEMP_ROOT}/indexing.parameters.json" \
  indexing-experiment \
  "$indexing_env_file" \
  "$INDEXING_BOOTSTRAP_SCRIPT" \
  "$container_sas_url" \
  lexon-exp-preflight-batch-20260701-123457

assert_workflow_contains \
  "$EMBEDDING_WORKFLOW" \
  'capture_workload_log:' \
  'long_term_resource_group="lexon-exp-${manifest_hash}"' \
  'batch_resource_group="${long_term_resource_group}-batch-${batch_suffix}"' \
  'tags="$storage_tags_json"' \
  '"lexon-scope": "long-term"' \
  '- name: Generate workflow container SAS' \
  "az storage account keys list \\" \
  "--resource-group '\${{ steps.prepare.outputs.long_term_resource_group }}' \\" \
  "--account-name '\${{ steps.storage.outputs.storageAccountName }}' \\" \
  "--query '[0].value' \\" \
  "if [[ -z \"\$account_key\" ]]; then" \
  "az storage container generate-sas \\" \
  "--account-key \"\$account_key\" \\" \
  "printf '::add-mask::%s\\n' \"\$account_key\"" \
  "printf '::add-mask::%s\\n' \"\$sas_token\"" \
  "printf '::add-mask::%s\\n' \"\$container_sas_url\"" \
  "printf 'CONTAINER_SAS_URL=%s\\n' \"\$container_sas_url\" >>\"\$GITHUB_ENV\"" \
  'DEBUG_RETAIN_FAILED_VM_INPUT: ${{ inputs.debug_retain_failed_vm }}' \
  'CAPTURE_WORKLOAD_LOG_INPUT: ${{ inputs.capture_workload_log }}' \
  'SSH_PUBLIC_KEY_INPUT: ${{ steps.prepare.outputs.ssh_public_key }}' \
  '--ssh-public-key "$SSH_PUBLIC_KEY_INPUT"' \
  "printf 'ssh_public_key=%s\\n' \"\$ssh_public_key\"" \
  '--container-sas-url "$CONTAINER_SAS_URL"' \
  '"$DEBUG_RETAIN_FAILED_VM_INPUT"' \
  '"$CAPTURE_WORKLOAD_LOG_INPUT"' \
  "if [[ '\${{ inputs.capture_workload_log }}' == 'true' ]]; then" \
  '- name: Delete batch resource group' \
  "if: \${{ always() && !(failure() && inputs.debug_retain_failed_vm) }}" \
  "batch_resource_group='\${{ steps.prepare.outputs.batch_resource_group }}'" \
  "if [[ -z \"\$batch_resource_group\" ]]; then" \
  "az group delete \\" \
  '--name "$batch_resource_group"' \
  'echo "| Long-term resource group | \`${{ steps.prepare.outputs.long_term_resource_group }}\` |"' \
  'echo "| Batch resource group | \`${{ steps.prepare.outputs.batch_resource_group }}\` |"'

assert_workflow_contains \
  "$INDEXING_WORKFLOW" \
  'long_term_resource_group="lexon-exp-${manifest_hash}"' \
  'batch_resource_group="${long_term_resource_group}-batch-${batch_suffix}"' \
  'debug_retain_failed_vm:' \
  'tags="$storage_tags_json"' \
  '"lexon-scope": "long-term"' \
  '- name: Generate workflow container SAS' \
  "az storage account keys list \\" \
  "--resource-group '\${{ steps.prepare.outputs.long_term_resource_group }}' \\" \
  "--account-name '\${{ steps.storage.outputs.storageAccountName }}' \\" \
  "--query '[0].value' \\" \
  "if [[ -z \"\$account_key\" ]]; then" \
  "az storage container generate-sas \\" \
  "--account-key \"\$account_key\" \\" \
  "printf '::add-mask::%s\\n' \"\$account_key\"" \
  "printf '::add-mask::%s\\n' \"\$sas_token\"" \
  "printf '::add-mask::%s\\n' \"\$container_sas_url\"" \
  "printf 'CONTAINER_SAS_URL=%s\\n' \"\$container_sas_url\" >>\"\$GITHUB_ENV\"" \
  'DEBUG_RETAIN_FAILED_VM_INPUT: ${{ inputs.debug_retain_failed_vm }}' \
  'SSH_PUBLIC_KEY_INPUT: ${{ steps.prepare.outputs.ssh_public_key }}' \
  '--ssh-public-key "$SSH_PUBLIC_KEY_INPUT"' \
  "printf 'ssh_public_key=%s\\n' \"\$ssh_public_key\"" \
  '--container-sas-url "$CONTAINER_SAS_URL"' \
  '"$DEBUG_RETAIN_FAILED_VM_INPUT"' \
  '- name: Delete batch resource group' \
  "if: \${{ always() && !(failure() && inputs.debug_retain_failed_vm) }}" \
  "batch_resource_group='\${{ steps.prepare.outputs.batch_resource_group }}'" \
  "if [[ -z \"\$batch_resource_group\" ]]; then" \
  "az group delete \\" \
  '--name "$batch_resource_group"' \
  'echo "| Long-term resource group | \`${{ steps.prepare.outputs.long_term_resource_group }}\` |"' \
  'echo "| Batch resource group | \`${{ steps.prepare.outputs.batch_resource_group }}\` |"' \
  'echo "| Debug retain failed batch RG | \`${{ inputs.debug_retain_failed_vm }}\` |"'

assert_file_contains \
  "$EXPERIMENT_MAIN_BICEP" \
  "@secure()" \
  "param containerSasUrl string"

assert_file_contains \
  "$EXPERIMENT_VM_RUNNER_BICEP" \
  "@secure()" \
  "param containerSasUrl string"

assert_file_contains \
  "$EXPERIMENT_STORAGE_BICEP" \
  "output storageAccountName string = storage.name" \
  "output containerName string = container.name" \
  "output blobEndpoint string = storage.properties.primaryEndpoints.blob"

assert_file_contains \
  "${REPO_ROOT}/scripts/lexonarchivebuilder-hosted-workflow-common.sh" \
  "HOSTED_EXPERIMENT_CAPTURE_WORKLOAD_LOG=\${capture_workload_log}"

printf 'Hosted workflow preflight validation passed\n'
