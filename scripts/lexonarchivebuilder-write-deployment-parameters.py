#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LexonArchiveBuilder contributors

import argparse
import json
from pathlib import Path
import sys
from typing import List


def parse_ssh_source_prefixes(value: str, parser: argparse.ArgumentParser) -> List[str]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        parser.error(f"--ssh-source-prefixes-json must be valid JSON: {exc.msg}")
    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        parser.error("--ssh-source-prefixes-json must be a JSON array of strings")
    return parsed


def read_text_file(path: str, parser: argparse.ArgumentParser) -> str:
    try:
        return Path(path).read_text(encoding="utf-8")
    except FileNotFoundError:
        parser.error(f"--workload-script-path not found: {path}")
    except OSError as exc:
        parser.error(f"--workload-script-path could not be read: {path}: {exc}")


def parse_bool(value: str) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise argparse.ArgumentTypeError("boolean values must be 'true' or 'false'")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow-name", required=True)
    parser.add_argument("--location", required=True)
    parser.add_argument("--resource-group", required=True)
    parser.add_argument("--vnet-name", required=True)
    parser.add_argument("--storage-account-name", required=True)
    parser.add_argument("--container-name", required=True)
    parser.add_argument("--container-sas-url", required=True)
    parser.add_argument("--vm-name", required=True)
    parser.add_argument("--ssh-public-key", required=True)
    parser.add_argument("--enable-public-ip", required=True, type=parse_bool)
    parser.add_argument("--enable-ssh-access", required=True, type=parse_bool)
    parser.add_argument("--ssh-source-prefixes-json", required=True)
    parser.add_argument("--workload-environment-file", required=True)
    parser.add_argument("--workload-script-path", required=True)
    parser.add_argument("--github-run-id", required=True)
    parser.add_argument("--github-run-attempt", required=True)
    args = parser.parse_args()
    ssh_source_prefixes = parse_ssh_source_prefixes(args.ssh_source_prefixes_json, parser)
    workload_script = read_text_file(args.workload_script_path, parser)

    params = {
        "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#",
        "contentVersion": "1.0.0.0",
        "parameters": {
            "location": {"value": args.location},
            "vnetName": {"value": args.vnet_name},
            "storageAccountName": {"value": args.storage_account_name},
            "containerName": {"value": args.container_name},
            "containerSasUrl": {"value": args.container_sas_url},
            "vmName": {"value": args.vm_name},
            "sshPublicKey": {"value": args.ssh_public_key},
            "enablePublicIp": {"value": args.enable_public_ip},
            "enableSshAccess": {"value": args.enable_ssh_access},
            "sshSourcePrefixes": {"value": ssh_source_prefixes},
            "workloadEnvironmentFile": {"value": args.workload_environment_file},
            "workloadScript": {"value": workload_script},
            "tags": {
                "value": {
                    "lexon-workflow": args.workflow_name,
                    "lexon-manifest": args.resource_group,
                    "github-run-id": args.github_run_id,
                    "github-run-attempt": args.github_run_attempt,
                }
            },
        },
    }

    json.dump(params, fp=sys.stdout, indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
