#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LexonArchiveBuilder contributors

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
    container_sas_url: str | None = None,
    storage_account_name: str | None = None,
    container_name: str | None = None,
) -> int:
    if container_sas_url is None or storage_account_name is None or container_name is None:
        container_sas_url, storage_account_name, container_name = sys.argv[1:4]
    write_assignments(container_sas_url, storage_account_name, container_name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
