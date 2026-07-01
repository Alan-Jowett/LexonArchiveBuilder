#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LexonArchiveBuilder contributors

import argparse
import json
from pathlib import Path
import sys


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
    parser.add_argument("--sas-expiry", required=True)
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

    params = {
        "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#",
        "contentVersion": "1.0.0.0",
        "parameters": {
            "location": {"value": args.location},
            "vnetName": {"value": args.vnet_name},
            "storageAccountName": {"value": args.storage_account_name},
            "containerName": {"value": args.container_name},
            "sasExpiry": {"value": args.sas_expiry},
            "vmName": {"value": args.vm_name},
            "sshPublicKey": {"value": args.ssh_public_key},
            "enablePublicIp": {"value": args.enable_public_ip},
            "enableSshAccess": {"value": args.enable_ssh_access},
            "sshSourcePrefixes": {"value": json.loads(args.ssh_source_prefixes_json)},
            "workloadEnvironmentFile": {"value": args.workload_environment_file},
            "workloadScript": {"value": Path(args.workload_script_path).read_text(encoding="utf-8")},
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
