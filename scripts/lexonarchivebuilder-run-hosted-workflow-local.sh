#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LexonArchiveBuilder contributors

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# shellcheck source=scripts/lexonarchivebuilder-hosted-workflow-common.sh
source "${SCRIPT_DIR}/lexonarchivebuilder-hosted-workflow-common.sh"
# shellcheck source=scripts/lexonarchivebuilder-hosted-experiment-common.sh
source "${SCRIPT_DIR}/lexonarchivebuilder-hosted-experiment-common.sh"

usage() {
  cat <<'EOF'
Usage:
  scripts/lexonarchivebuilder-run-hosted-workflow-local.sh \
    --workflow indexing-experiment|embedding-refresh \
    --manifest PATH \
    --location AZURE_REGION \
    [--profile-version MAJOR.MINOR.PATCH] \
    [--runner-image-tag TAG] \
    [--block-store-target filesystem|overlay] \
    [--ssh-public-key KEY] \
    [--ssh-source-prefixes CIDR[,CIDR...]] \
    [--debug-retain-failed-vm true|false] \
    [--repository OWNER/REPO] \
    [--github-run-id VALUE] \
    [--github-run-attempt VALUE] \
    [--artifacts-dir PATH] \
    [--wait-timeout-secs SECONDS] \
    [--poll-interval-secs SECONDS] \
    [--prepare-only]

This script reproduces the Azure deployment flow from the hosted experiment
workflows with a local bash/WSL session:
  1. prepares the same deployment metadata and parameters as the workflow
  2. creates the resource group and deploys infra/azure/experiments/main.bicep
  3. generates a workflow container SAS and waits for the VM-uploaded artifacts
  4. downloads workflow artifacts into a local directory for inspection

Notes:
  - Run this from WSL or another Linux shell with az, git, python3, curl, and
    ssh-keygen available.
  - Authenticate with Azure before running (for example: az login or azd auth login).
  - The indexing workflow requires --profile-version.
EOF
}

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

validate_positive_integer() {
  local label="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    printf 'error: %s must be a positive integer\n' "$label" >&2
    exit 1
  fi
}

validate_bool() {
  local label="$1"
  local value="$2"
  case "$value" in
    true|false)
      ;;
    *)
      printf "error: %s must be 'true' or 'false'\n" "$label" >&2
      exit 1
      ;;
  esac
}

detect_repository_slug() {
  local remote_url
  remote_url="$(git -C "$REPO_ROOT" config --get remote.origin.url || true)"
  if [[ -z "$remote_url" ]]; then
    printf 'error: unable to detect repository slug from git remote origin; pass --repository OWNER/REPO\n' >&2
    exit 1
  fi

  python3 - "$remote_url" <<'PY'
import re
import sys

remote = sys.argv[1].strip()
patterns = [
    r"^git@github\.com:(?P<slug>[^/]+/[^/]+?)(?:\.git)?$",
    r"^https://github\.com/(?P<slug>[^/]+/[^/]+?)(?:\.git)?$",
]
for pattern in patterns:
    match = re.match(pattern, remote)
    if match:
        print(match.group("slug"))
        raise SystemExit(0)
raise SystemExit(f"unable to parse GitHub owner/repo from remote origin URL: {remote}")
PY
}

az_cli_uses_windows_paths() {
  local az_path
  az_path="$(command -v az)"
  [[ "$az_path" == /mnt/* ]]
}

az_cli_path_arg() {
  local path="$1"
  if az_cli_uses_windows_paths; then
    require_command wslpath
    wslpath -w "$path"
  else
    printf '%s' "$path"
  fi
}

normalize_cli_output() {
  printf '%s' "$1" | tr -d '\r'
}

parse_ssh_source_prefixes_json() {
  local raw="$1"
  python3 - "$raw" <<'PY'
import ipaddress
import json
import sys

raw = sys.argv[1]
items = [entry.strip() for entry in raw.split(',') if entry.strip()]
if not items:
    raise SystemExit('ssh_source_prefixes must contain at least one CIDR when SSH access is enabled')

validated = []
for entry in items:
    if '/' not in entry:
        raise SystemExit(f'ssh_source_prefixes entry "{entry}" must use CIDR notation')
    try:
        validated.append(str(ipaddress.ip_network(entry, strict=False)))
    except ValueError as exc:
        raise SystemExit(f'ssh_source_prefixes entry "{entry}" is not a valid CIDR: {exc}')

print(json.dumps(validated))
PY
}

append_blob_path() {
  local container_sas_url="$1"
  local blob_path="$2"
  python3 - "$container_sas_url" "$blob_path" <<'PY'
import sys
from urllib.parse import urlsplit, urlunsplit

container_sas_url = sys.argv[1]
blob_path = sys.argv[2].lstrip('/')
parts = urlsplit(container_sas_url)
path = f"{parts.path.rstrip('/')}/{blob_path}"
print(urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment)))
PY
}

download_blob_if_present() {
  local blob_path="$1"
  local destination="$2"
  local blob_url
  blob_url="$(append_blob_path "$CONTAINER_SAS_URL" "$blob_path")"
  curl --fail --silent --show-error --location "$blob_url" --output "$destination" >/dev/null 2>&1 || return 1
}

wait_for_blob_file() {
  local blob_path="$1"
  local destination="$2"
  local started_at now
  started_at="$(date +%s)"
  while true; do
    if download_blob_if_present "$blob_path" "$destination"; then
      return 0
    fi
    now="$(date +%s)"
    if (( now - started_at >= WAIT_TIMEOUT_SECS )); then
      printf 'error: timed out waiting for blob %s\n' "$blob_path" >&2
      return 1
    fi
    sleep "$POLL_INTERVAL_SECS"
  done
}

write_prepare_summary_json() {
  local destination="$1"
  python3 - "$destination" <<'PY'
import json
import os
import sys

destination = sys.argv[1]
payload = {
    "workflow": os.environ["WORKFLOW"],
    "repository": os.environ["REPOSITORY_SLUG"],
    "manifest_path": os.environ["MANIFEST_PATH"],
    "resource_group": os.environ["RESOURCE_GROUP"],
    "storage_account_name": os.environ["STORAGE_ACCOUNT_NAME"],
    "container_name": os.environ["CONTAINER_NAME"],
    "vnet_name": os.environ["VNET_NAME"],
    "vm_name": os.environ["VM_NAME"],
    "run_name": os.environ["RUN_NAME"],
    "artifact_prefix": os.environ["ARTIFACT_PREFIX"],
    "runner_image": os.environ["RUNNER_IMAGE"],
    "block_store_target": os.environ["BLOCK_STORE_TARGET"],
    "github_run_id": os.environ["GITHUB_RUN_ID"],
    "github_run_attempt": os.environ["GITHUB_RUN_ATTEMPT"],
}
profile_version = os.environ.get("PROFILE_VERSION", "")
if profile_version:
    payload["profile_version"] = profile_version
with open(destination, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

download_indexing_artifacts() {
  mkdir -p "$ARTIFACTS_DIR"
  wait_for_blob_file "${ARTIFACT_PREFIX}/status.json" "${ARTIFACTS_DIR}/status.json"

  download_blob_if_present "${ARTIFACT_PREFIX}/quality.json" "${ARTIFACTS_DIR}/quality.json" || true
  download_blob_if_present "${ARTIFACT_PREFIX}/summary.json" "${ARTIFACTS_DIR}/summary.json" || true
  download_blob_if_present "${ARTIFACT_PREFIX}/result.json" "${ARTIFACTS_DIR}/result.json" || true
  download_blob_if_present "${ARTIFACT_PREFIX}/manifest.json" "${ARTIFACTS_DIR}/manifest.json" || true

  local success_flag
  success_flag="$(
    python3 - "${ARTIFACTS_DIR}/status.json" <<'PY'
import json
import sys

with open(sys.argv[1], 'r', encoding='utf-8') as handle:
    print('true' if json.load(handle).get('success') else 'false')
PY
  )"
  if [[ "$success_flag" != 'true' ]]; then
    printf 'error: indexing experiment reported failure\n' >&2
    return 1
  fi
}

download_embedding_artifacts() {
  mkdir -p "$ARTIFACTS_DIR"

  local phase='unknown'
  local started_at now
  started_at="$(date +%s)"
  while true; do
    if download_blob_if_present "${ARTIFACT_PREFIX}/bootstrap-status.json" "${ARTIFACTS_DIR}/bootstrap-status.json"; then
      cp "${ARTIFACTS_DIR}/bootstrap-status.json" "${ARTIFACTS_DIR}/status.json"
      phase='bootstrap'
      break
    fi
    if download_blob_if_present "${ARTIFACT_PREFIX}/status.json" "${ARTIFACTS_DIR}/status.json"; then
      phase='workload'
      break
    fi
    now="$(date +%s)"
    if (( now - started_at >= WAIT_TIMEOUT_SECS )); then
      printf 'error: timed out waiting for embedding refresh status artifact\n' >&2
      return 1
    fi
    sleep "$POLL_INTERVAL_SECS"
  done

  download_blob_if_present "${ARTIFACT_PREFIX}/summary.json" "${ARTIFACTS_DIR}/summary.json" || true
  download_blob_if_present "${ARTIFACT_PREFIX}/manifest.json" "${ARTIFACTS_DIR}/manifest.json" || true
  download_blob_if_present "${ARTIFACT_PREFIX}/bootstrap-status.json" "${ARTIFACTS_DIR}/bootstrap-status.json" || true
  download_blob_if_present "${ARTIFACT_PREFIX}/bootstrap-wrapper.log" "${ARTIFACTS_DIR}/bootstrap-wrapper.log" || true
  download_blob_if_present "${ARTIFACT_PREFIX}/workload.log" "${ARTIFACTS_DIR}/workload.log" || true
  download_blob_if_present "${ARTIFACT_PREFIX}/bootstrap-cloud-init-output.log" "${ARTIFACTS_DIR}/bootstrap-cloud-init-output.log" || true
  download_blob_if_present "${ARTIFACT_PREFIX}/bootstrap-stapi.log" "${ARTIFACTS_DIR}/bootstrap-stapi.log" || true

  local status_info
  status_info="$(
    python3 - "${ARTIFACTS_DIR}/status.json" "$phase" <<'PY'
import json
import sys

with open(sys.argv[1], 'r', encoding='utf-8') as handle:
    payload = json.load(handle)

print('true' if payload.get('success') else 'false')
print(payload.get('phase') or sys.argv[2])
PY
  )"
  local success_flag actual_phase
  success_flag="$(printf '%s\n' "$status_info" | sed -n '1p')"
  actual_phase="$(printf '%s\n' "$status_info" | sed -n '2p')"
  if [[ "$success_flag" != 'true' ]]; then
    printf 'error: embedding refresh reported %s failure\n' "$actual_phase" >&2
    return 1
  fi
}

WORKFLOW=""
MANIFEST_PATH=""
LOCATION="${AZURE_LOCATION:-}"
RUNNER_IMAGE_TAG="main"
PROFILE_VERSION=""
BLOCK_STORE_TARGET="overlay"
SSH_PUBLIC_KEY=""
SSH_SOURCE_PREFIXES=""
DEBUG_RETAIN_FAILED_VM="false"
REPOSITORY_SLUG=""
GITHUB_RUN_ID="local-$(date -u +%Y%m%d%H%M%S)"
GITHUB_RUN_ATTEMPT="1"
ARTIFACTS_DIR=""
WAIT_TIMEOUT_SECS=$((220 * 60))
POLL_INTERVAL_SECS=60
PREPARE_ONLY="false"
STAPI_IMAGE="${STAPI_IMAGE:-ghcr.io/substratusai/stapi:v2.2.2-3}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workflow)
      WORKFLOW="$2"
      shift 2
      ;;
    --manifest)
      MANIFEST_PATH="$2"
      shift 2
      ;;
    --location)
      LOCATION="$2"
      shift 2
      ;;
    --profile-version)
      PROFILE_VERSION="$2"
      shift 2
      ;;
    --runner-image-tag)
      RUNNER_IMAGE_TAG="$2"
      shift 2
      ;;
    --block-store-target)
      BLOCK_STORE_TARGET="$2"
      shift 2
      ;;
    --ssh-public-key)
      SSH_PUBLIC_KEY="$2"
      shift 2
      ;;
    --ssh-source-prefixes)
      SSH_SOURCE_PREFIXES="$2"
      shift 2
      ;;
    --debug-retain-failed-vm)
      DEBUG_RETAIN_FAILED_VM="$2"
      shift 2
      ;;
    --repository)
      REPOSITORY_SLUG="$2"
      shift 2
      ;;
    --github-run-id)
      GITHUB_RUN_ID="$2"
      shift 2
      ;;
    --github-run-attempt)
      GITHUB_RUN_ATTEMPT="$2"
      shift 2
      ;;
    --artifacts-dir)
      ARTIFACTS_DIR="$2"
      shift 2
      ;;
    --wait-timeout-secs)
      WAIT_TIMEOUT_SECS="$2"
      shift 2
      ;;
    --poll-interval-secs)
      POLL_INTERVAL_SECS="$2"
      shift 2
      ;;
    --prepare-only)
      PREPARE_ONLY="true"
      shift
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

require_commands base64 curl git python3 sha256sum ssh-keygen

case "$WORKFLOW" in
  indexing-experiment|embedding-refresh)
    ;;
  *)
    printf 'error: --workflow must be indexing-experiment or embedding-refresh\n' >&2
    exit 1
    ;;
esac

if [[ -z "$MANIFEST_PATH" || -z "$LOCATION" ]]; then
  usage >&2
  exit 1
fi

if [[ "$WORKFLOW" == "indexing-experiment" && -z "$PROFILE_VERSION" ]]; then
  printf 'error: --profile-version is required for indexing-experiment\n' >&2
  exit 1
fi

validate_positive_integer "--wait-timeout-secs" "$WAIT_TIMEOUT_SECS"
validate_positive_integer "--poll-interval-secs" "$POLL_INTERVAL_SECS"
validate_bool "--debug-retain-failed-vm" "$DEBUG_RETAIN_FAILED_VM"
ensure_supported_block_store_target "$BLOCK_STORE_TARGET"
hosted_workflow_validate_runner_image_tag "$RUNNER_IMAGE_TAG"
if [[ -n "$PROFILE_VERSION" ]]; then
  hosted_workflow_validate_profile_version "$PROFILE_VERSION"
fi

MANIFEST_PATH="$(cd "$REPO_ROOT" && python3 - "$MANIFEST_PATH" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
print(str(path.resolve()))
PY
)"
if [[ ! -f "$MANIFEST_PATH" ]]; then
  printf 'error: manifest not found: %s\n' "$MANIFEST_PATH" >&2
  exit 1
fi

if [[ -z "$REPOSITORY_SLUG" ]]; then
  REPOSITORY_SLUG="$(detect_repository_slug)"
fi
if [[ ! "$REPOSITORY_SLUG" =~ ^[^/]+/[^/]+$ ]]; then
  printf 'error: --repository must use OWNER/REPO format\n' >&2
  exit 1
fi

REPOSITORY_OWNER="${REPOSITORY_SLUG%%/*}"
RUNNER_IMAGE="ghcr.io/${REPOSITORY_OWNER,,}/lexonarchivebuilder-scale-test:${RUNNER_IMAGE_TAG}"

TEMP_ROOT="$(mktemp -d)"
DEPLOYMENT_SUCCEEDED="false"
DEPLOYED_RESOURCE_GROUP=""
DEPLOYED_VM_NAME=""
DEALLOCATE_ON_EXIT="false"
KEEP_FAILED_VM_ALLOCATED="$DEBUG_RETAIN_FAILED_VM"

cleanup() {
  local exit_code=$?
  if [[ "$DEALLOCATE_ON_EXIT" == "true" && "$DEPLOYMENT_SUCCEEDED" == "true" ]]; then
    if [[ ! ( "$KEEP_FAILED_VM_ALLOCATED" == "true" && $exit_code -ne 0 ) ]]; then
      az vm deallocate \
        --resource-group "$DEPLOYED_RESOURCE_GROUP" \
        --name "$DEPLOYED_VM_NAME" \
        --no-wait >/dev/null 2>&1 || true
    fi
  fi
  rm -rf "$TEMP_ROOT"
  exit "$exit_code"
}
trap cleanup EXIT

manifest_json_minified="$(hosted_workflow_normalize_manifest_json "$MANIFEST_PATH")"
manifest_hash="$(printf '%s' "$manifest_json_minified" | sha256sum | cut -c1-10)"
repo_hash="$(printf '%s' "$REPOSITORY_SLUG" | sha256sum | cut -c1-6)"
hosted_experiment_common_script_b64="$(base64 -w0 "${REPO_ROOT}/scripts/lexonarchivebuilder-hosted-experiment-common.sh")"
container_name="$(hosted_workflow_extract_container_name "$MANIFEST_PATH")"
resource_group="lexon-exp-${manifest_hash}"
storage_account_name="lex${repo_hash}${manifest_hash}sa"
vnet_name="lexon-exp-${manifest_hash}-vnet"
dataset_block_store_prefix="datasets/block-store"
dataset_replay_journal_prefix="datasets/block-store.replay-journal"
manifest_json_b64="$(printf '%s' "$manifest_json_minified" | base64 -w0)"
sas_expiry="$(date -u -d '+1 day' '+%Y-%m-%dT%H:%M:%SZ')"

enable_ssh_access=false
ssh_prefixes_json='[]'
if [[ -n "$SSH_SOURCE_PREFIXES" ]]; then
  enable_ssh_access=true
  if [[ -z "$SSH_PUBLIC_KEY" ]]; then
    printf 'error: --ssh-public-key is required when --ssh-source-prefixes enables SSH access\n' >&2
    exit 1
  fi
  ssh_prefixes_json="$(parse_ssh_source_prefixes_json "$SSH_SOURCE_PREFIXES")"
elif [[ -z "$SSH_PUBLIC_KEY" ]]; then
  ssh_key_path="${TEMP_ROOT}/workflow-ssh-key"
  rm -f "$ssh_key_path" "$ssh_key_path.pub"
  ssh-keygen -q -t rsa -b 4096 -N '' -f "$ssh_key_path" >/dev/null
  SSH_PUBLIC_KEY="$(<"${ssh_key_path}.pub")"
fi

case "$WORKFLOW" in
  indexing-experiment)
    hosted_experiment_workload_script_b64="$(base64 -w0 "${REPO_ROOT}/scripts/lexonarchivebuilder-indexing-experiment.sh")"
    vm_name="lexon-exp-index-${manifest_hash}-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
    run_name="index-${manifest_hash}-${PROFILE_VERSION//./-}-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
    artifact_prefix="artifacts/indexing-experiment/${PROFILE_VERSION}/${run_name}"
    workload_script_path="${REPO_ROOT}/infra/azure/experiments/indexing-experiment-bootstrap.sh"
    workload_env_file="$(
      hosted_workflow_render_indexing_workload_env_file \
        "$run_name" \
        "$RUNNER_IMAGE" \
        "$manifest_json_b64" \
        "$hosted_experiment_common_script_b64" \
        "$hosted_experiment_workload_script_b64" \
        "$dataset_block_store_prefix" \
        "$dataset_replay_journal_prefix" \
        "$artifact_prefix" \
        "$BLOCK_STORE_TARGET" \
        "$PROFILE_VERSION"
    )"
    ;;
  embedding-refresh)
    hosted_experiment_workload_script_b64="$(base64 -w0 "${REPO_ROOT}/scripts/lexonarchivebuilder-embedding-refresh.sh")"
    vm_name="lexon-exp-embed-${manifest_hash}-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
    run_name="embed-${manifest_hash}-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
    artifact_prefix="artifacts/embedding-refresh/${run_name}"
    workload_script_path="${REPO_ROOT}/infra/azure/experiments/embedding-refresh-bootstrap.sh"
    workload_env_file="$(
      hosted_workflow_render_embedding_workload_env_file \
        "$run_name" \
        "$RUNNER_IMAGE" \
        "$STAPI_IMAGE" \
        "$manifest_json_b64" \
        "$hosted_experiment_common_script_b64" \
        "$hosted_experiment_workload_script_b64" \
        "$dataset_block_store_prefix" \
        "$dataset_replay_journal_prefix" \
        "$artifact_prefix" \
        "$BLOCK_STORE_TARGET" \
        "$DEBUG_RETAIN_FAILED_VM"
    )"
    ;;
esac

if [[ -z "$ARTIFACTS_DIR" ]]; then
  ARTIFACTS_DIR="${REPO_ROOT}/examples/local/scale-test/workflow-artifacts/${run_name}"
fi
mkdir -p "$ARTIFACTS_DIR"

deployment_parameters_path="${ARTIFACTS_DIR}/deployment.parameters.json"
prepare_summary_path="${ARTIFACTS_DIR}/prepare-summary.json"
deployment_outputs_path="${ARTIFACTS_DIR}/deployment.outputs.json"

python3 "${REPO_ROOT}/scripts/lexonarchivebuilder-write-deployment-parameters.py" \
  --workflow-name "$WORKFLOW" \
  --location "$LOCATION" \
  --resource-group "$resource_group" \
  --vnet-name "$vnet_name" \
  --storage-account-name "$storage_account_name" \
  --container-name "$container_name" \
  --sas-expiry "$sas_expiry" \
  --vm-name "$vm_name" \
  --ssh-public-key "$SSH_PUBLIC_KEY" \
  --enable-public-ip "$enable_ssh_access" \
  --enable-ssh-access "$enable_ssh_access" \
  --ssh-source-prefixes-json "$ssh_prefixes_json" \
  --workload-environment-file "$workload_env_file" \
  --workload-script-path "$workload_script_path" \
  --github-run-id "$GITHUB_RUN_ID" \
  --github-run-attempt "$GITHUB_RUN_ATTEMPT" > "$deployment_parameters_path"

export WORKFLOW REPOSITORY_SLUG MANIFEST_PATH RESOURCE_GROUP="$resource_group" STORAGE_ACCOUNT_NAME="$storage_account_name"
export CONTAINER_NAME="$container_name" VNET_NAME="$vnet_name" VM_NAME="$vm_name" RUN_NAME="$run_name"
export ARTIFACT_PREFIX="$artifact_prefix" RUNNER_IMAGE BLOCK_STORE_TARGET GITHUB_RUN_ID GITHUB_RUN_ATTEMPT PROFILE_VERSION
write_prepare_summary_json "$prepare_summary_path"

printf 'Prepared %s\n' "$WORKFLOW"
printf '  Manifest: %s\n' "$MANIFEST_PATH"
printf '  Resource group: %s\n' "$resource_group"
printf '  VM: %s\n' "$vm_name"
printf '  Run name: %s\n' "$run_name"
printf '  Artifact prefix: %s\n' "$artifact_prefix"
printf '  Local artifacts: %s\n' "$ARTIFACTS_DIR"

if [[ "$PREPARE_ONLY" == "true" ]]; then
  exit 0
fi

require_command az
az account show >/dev/null

az group create \
  --name "$resource_group" \
  --location "$LOCATION" \
  --tags lexon-workflow="$WORKFLOW" github-run-id="$GITHUB_RUN_ID" github-run-attempt="$GITHUB_RUN_ATTEMPT" >/dev/null

outputs_json="$(
  template_file_path="$(az_cli_path_arg "${REPO_ROOT}/infra/azure/experiments/main.bicep")"
  deployment_parameters_arg="$(az_cli_path_arg "$deployment_parameters_path")"
  az deployment group create \
    --resource-group "$resource_group" \
    --template-file "$template_file_path" \
    --parameters @"$deployment_parameters_arg" \
    --query properties.outputs \
    --output json
)"
DEPLOYED_RESOURCE_GROUP="$resource_group"
DEPLOYED_VM_NAME="$vm_name"
DEPLOYMENT_SUCCEEDED="true"
DEALLOCATE_ON_EXIT="true"
printf '%s\n' "$outputs_json" >"$deployment_outputs_path"

readarray -t deployment_outputs < <(
  python3 - "$outputs_json" <<'PY'
import json
import sys

outputs = json.loads(sys.argv[1])
for key in ("blobEndpoint", "storageAccountName", "containerName", "vmName", "vmPublicIpAddress"):
    print(outputs[key]["value"])
PY
)
blob_endpoint="${deployment_outputs[0]}"
deployed_storage_account_name="${deployment_outputs[1]}"
deployed_container_name="${deployment_outputs[2]}"
deployed_vm_name="${deployment_outputs[3]}"
deployed_vm_public_ip_address="${deployment_outputs[4]}"

blob_endpoint="$(normalize_cli_output "$blob_endpoint")"
deployed_storage_account_name="$(normalize_cli_output "$deployed_storage_account_name")"
deployed_container_name="$(normalize_cli_output "$deployed_container_name")"
deployed_vm_name="$(normalize_cli_output "$deployed_vm_name")"
deployed_vm_public_ip_address="$(normalize_cli_output "$deployed_vm_public_ip_address")"

account_key="$(
  az storage account keys list \
    --resource-group "$resource_group" \
    --account-name "$deployed_storage_account_name" \
    --query '[0].value' \
    --output tsv
)"
sas_token="$(
  az storage container generate-sas \
    --account-name "$deployed_storage_account_name" \
    --name "$deployed_container_name" \
    --permissions racwl \
    --expiry "$(date -u -d '+1 day' '+%Y-%m-%dT%H:%M:%SZ')" \
    --https-only \
    --account-key "$account_key" \
    --output tsv
)"
account_key="$(normalize_cli_output "$account_key")"
sas_token="$(normalize_cli_output "$sas_token")"
CONTAINER_SAS_URL="${blob_endpoint}${deployed_container_name}?${sas_token}"
printf '%s\n' "$CONTAINER_SAS_URL" > "${ARTIFACTS_DIR}/container-sas-url.txt"

DEPLOYED_VM_NAME="$deployed_vm_name"

printf 'Deployment succeeded\n'
printf '  Storage account: %s\n' "$deployed_storage_account_name"
printf '  Container: %s\n' "$deployed_container_name"
printf '  VM public IP: %s\n' "$deployed_vm_public_ip_address"

case "$WORKFLOW" in
  indexing-experiment)
    download_indexing_artifacts
    ;;
  embedding-refresh)
    download_embedding_artifacts
    ;;
esac

printf 'Workflow artifacts downloaded to %s\n' "$ARTIFACTS_DIR"
