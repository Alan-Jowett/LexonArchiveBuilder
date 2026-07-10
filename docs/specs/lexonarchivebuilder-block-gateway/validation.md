<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder Block Gateway Validation

## Status

Approved specification package with implemented HTTP/3 block-gateway slice for the block-gateway
requirements and design in:

- `docs/specs/lexonarchivebuilder-block-gateway/requirements.md`
- `docs/specs/lexonarchivebuilder-block-gateway/design.md`

## Validation Scope

These validation entries define the expected conformance surface for the
LexonArchiveBuilder-owned `lexonarchivebuilder-block-gateway` boundary.

This package validates the HTTP gateway contract, delegated Azure Table
dependency binding, immutable-cache response behavior, hosting neutrality, and
architectural non-interference. It does not redefine validation already owned
by `lexonarchivebuilder-indexer`, `lexonarchivebuilder-mcp`, or the delegated
LexonGraph block-store contract.

## Validation Entries

### VAL-BGW-001

Inspect the repository specification surface for
`lexonarchivebuilder-block-gateway`.

**Pass condition:** the repository defines `lexonarchivebuilder-block-gateway`
as a separate retrieval-only boundary rather than folding block-serving HTTP
behavior into the existing MCP boundary or indexer semantic boundary.

**Traces to:** RQ-BGW-001, RQ-BGW-010, RQ-BGW-011, DSG-BGW-001

### VAL-BGW-002

Inspect the gateway route contract.

**Pass condition:** the gateway exposes a `/block/<block_id>` retrieval route
and does not add write, delete, list, or search operations in the approved
increment.

**Traces to:** RQ-BGW-003, DSG-BGW-002

### VAL-BGW-003

Inspect the storage-dependency configuration contract.

**Pass condition:** the gateway binds the Azure Storage Table dependency from a
startup-time SAS URL configuration input, and ordinary fetch requests do not
carry backend SAS credentials.

**Traces to:** RQ-BGW-004, RQ-BGW-009, DSG-BGW-003

### VAL-BGW-004

Inspect the delegated block-fetch path for a representative successful lookup.

**Pass condition:** the gateway resolves the block through the delegated
LexonGraph Azure Storage Table v2 block-store path already adopted by the
repository's direct Azure seam, and it does not introduce repository-local
table-entity decoding or payload translation.

**Traces to:** RQ-BGW-005, RQ-BGW-012, DSG-BGW-004

### VAL-BGW-005

Execute or inspect a representative successful block fetch.

**Pass condition:** the gateway returns the stored block bytes exactly as read,
marks the response as `application/octet-stream`, and includes response headers
that denote immutable long-lived caching for the served block content.

**Traces to:** RQ-BGW-006, RQ-BGW-007, DSG-BGW-005

### VAL-BGW-006

Execute or inspect a representative unsuccessful block fetch.

**Pass condition:** the gateway returns HTTP `404` for the externally visible
non-success outcome, including malformed, missing, or otherwise unsuccessful
lookups in the approved current contract.

**Traces to:** RQ-BGW-008, DSG-BGW-006

### VAL-BGW-007

Inspect the gateway contract across supported hosting forms.

**Pass condition:** the gateway preserves one unchanged application contract
across VM-hosted, containerized, and function-hosted realizations, with any
hosting differences limited to lifecycle and packaging rather than route,
startup SAS binding, delegated block-store selection, or response semantics.

**Traces to:** RQ-BGW-002, RQ-BGW-009, DSG-BGW-007

### VAL-BGW-008

Inspect the specification package against repository invariants.

**Pass condition:** the gateway remains retrieval-only, separate from MCP
search serving, free of central-control-plane behavior, and content-type-neutral
despite introducing a narrow server-side immutable-block retrieval exception.

**Traces to:** RQ-BGW-010, RQ-BGW-011, RQ-BGW-013, RQ-BGW-014, DSG-BGW-008
