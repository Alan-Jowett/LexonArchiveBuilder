#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LexonArchiveBuilder contributors

import re
import shlex
import sys


KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def main() -> int:
    for assignment in sys.argv[1:]:
        key, separator, value = assignment.partition("=")
        if not separator:
            raise SystemExit(f"invalid assignment (missing '='): {assignment}")
        if not KEY_PATTERN.fullmatch(key):
            raise SystemExit(f"invalid shell variable name: {key}")
        print(f"{key}={shlex.quote(value)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
