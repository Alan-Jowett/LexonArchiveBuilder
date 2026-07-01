#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LexonArchiveBuilder contributors

from typing import Optional
import shlex
import sys


def write_assignments(container_sas_url: str, storage_account_name: str, container_name: str) -> None:
    values = {
        "CONTAINER_SAS_URL": container_sas_url,
        "STORAGE_ACCOUNT_NAME": storage_account_name,
        "CONTAINER_NAME": container_name,
    }
    for key, value in values.items():
        print(f"{key}={shlex.quote(value)}")


def main(
    container_sas_url: Optional[str] = None,
    storage_account_name: Optional[str] = None,
    container_name: Optional[str] = None,
) -> int:
    if container_sas_url is None or storage_account_name is None or container_name is None:
        if len(sys.argv) != 4:
            raise SystemExit(
                "usage: render-workload-storage-env.py <container-sas-url> "
                "<storage-account-name> <container-name>"
            )
        container_sas_url, storage_account_name, container_name = sys.argv[1:4]
    write_assignments(container_sas_url, storage_account_name, container_name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
