#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LexonArchiveBuilder contributors

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SCALE_TEST_SCRIPT="${SCRIPT_DIR}/lexonarchivebuilder-scale-test.sh"
FIXTURE_MAILBOX="${REPO_ROOT}/examples/local/data/mail/2026-01.mbox"

if [[ ! -f "$FIXTURE_MAILBOX" ]]; then
  printf 'error: fixture mailbox not found: %s\n' "$FIXTURE_MAILBOX" >&2
  exit 1
fi

TEMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TEMP_ROOT"' EXIT

SOURCE_ONE="${TEMP_ROOT}/source-one"
SOURCE_TWO="${TEMP_ROOT}/source-two"
RUN_NAME="smoke-$(date -u '+%Y%m%dT%H%M%SZ')"
RUN_DIR="${REPO_ROOT}/examples/local/scale-test/runs/${RUN_NAME}"

mkdir -p "$SOURCE_ONE" "$SOURCE_TWO"
cp "$FIXTURE_MAILBOX" "${SOURCE_ONE}/2026-01.mail"
cp "$FIXTURE_MAILBOX" "${SOURCE_TWO}/2026-02.mbox"

if bash "$SCALE_TEST_SCRIPT" --clustering-algorithm directional-pca "$SOURCE_ONE" >/dev/null 2>&1; then
  printf 'error: retired clustering flag was accepted by scale-test wrapper\n' >&2
  exit 1
fi

bash "$SCALE_TEST_SCRIPT" \
  --run-name "$RUN_NAME" \
  "$SOURCE_ONE" "$SOURCE_TWO"

REQUEST_PATH="${RUN_DIR}/request.json"
SUMMARY_PATH="${RUN_DIR}/summary.json"

[[ -f "$REQUEST_PATH" ]] || { printf 'error: request not generated: %s\n' "$REQUEST_PATH" >&2; exit 1; }
[[ -f "$SUMMARY_PATH" ]] || { printf 'error: summary not generated: %s\n' "$SUMMARY_PATH" >&2; exit 1; }

grep -q '"root_id"' "$SUMMARY_PATH" || { printf 'error: summary missing root_id\n' >&2; exit 1; }
grep -Fq "\"ref_name\": \"${RUN_NAME}\"" "$REQUEST_PATH" || { printf 'error: request missing run-scoped ref_name\n' >&2; exit 1; }
MAILBOX_ITEM_COUNT="$(grep -c '"kind": "mailbox"' "$REQUEST_PATH")"
if [[ "$MAILBOX_ITEM_COUNT" -lt 2 ]]; then
  printf 'error: expected at least 2 mailbox items in generated request, found %s\n' "$MAILBOX_ITEM_COUNT" >&2
  exit 1
fi
grep -q '"month": "2026-01"' "$REQUEST_PATH" || { printf 'error: request missing normalized month for .mail source\n' >&2; exit 1; }
grep -q '"month": "2026-02"' "$REQUEST_PATH" || { printf 'error: request missing normalized month for .mbox source\n' >&2; exit 1; }

printf 'Smoke test passed: %s\n' "${RUN_DIR#${REPO_ROOT}/}"
