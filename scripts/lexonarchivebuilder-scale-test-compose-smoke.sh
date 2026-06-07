#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
FIXTURE_MAILBOX="${REPO_ROOT}/examples/local/data/mail/2026-01.mbox"

if [[ ! -f "$FIXTURE_MAILBOX" ]]; then
  printf 'error: fixture mailbox not found: %s\n' "$FIXTURE_MAILBOX" >&2
  exit 1
fi

RUN_NAME="compose-smoke-$(date -u '+%Y%m%dT%H%M%SZ')"
RUN_DIR="${REPO_ROOT}/examples/local/scale-test/runs/${RUN_NAME}"
FIXTURE_DIR="${RUN_DIR}/fixtures"
CONTAINER_SOURCES_FILE="/workspace/examples/local/scale-test/runs/${RUN_NAME}/fixtures/sources.txt"

mkdir -p "${FIXTURE_DIR}/source-one" "${FIXTURE_DIR}/source-two"
cp "$FIXTURE_MAILBOX" "${FIXTURE_DIR}/source-one/2026-01.mail"
cp "$FIXTURE_MAILBOX" "${FIXTURE_DIR}/source-two/2026-02.mbox"

cat >"${FIXTURE_DIR}/sources.txt" <<EOF
/workspace/examples/local/scale-test/runs/${RUN_NAME}/fixtures/source-one
/workspace/examples/local/scale-test/runs/${RUN_NAME}/fixtures/source-two
EOF

(cd "$REPO_ROOT" && COMPOSE_PROJECT_NAME=lexonarchivebuilder docker compose run --rm scale-test --run-name "$RUN_NAME" --sources-file "$CONTAINER_SOURCES_FILE")

REQUEST_PATH="${RUN_DIR}/request.json"
SUMMARY_PATH="${RUN_DIR}/summary.json"

[[ -f "$REQUEST_PATH" ]] || { printf 'error: request not generated: %s\n' "$REQUEST_PATH" >&2; exit 1; }
[[ -f "$SUMMARY_PATH" ]] || { printf 'error: summary not generated: %s\n' "$SUMMARY_PATH" >&2; exit 1; }

grep -q '"root_id"' "$SUMMARY_PATH" || { printf 'error: summary missing root_id\n' >&2; exit 1; }
MAILBOX_ITEM_COUNT="$(grep -c '"kind": "mailbox"' "$REQUEST_PATH")"
if [[ "$MAILBOX_ITEM_COUNT" -lt 2 ]]; then
  printf 'error: expected at least 2 mailbox items in generated request, found %s\n' "$MAILBOX_ITEM_COUNT" >&2
  exit 1
fi
grep -q '"month": "2026-01"' "$REQUEST_PATH" || { printf 'error: request missing normalized month for .mail source\n' >&2; exit 1; }
grep -q '"month": "2026-02"' "$REQUEST_PATH" || { printf 'error: request missing normalized month for .mbox source\n' >&2; exit 1; }

printf 'Compose smoke test passed: %s\n' "${RUN_DIR#${REPO_ROOT}/}"
