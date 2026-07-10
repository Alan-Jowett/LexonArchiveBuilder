<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder HTTP/3 Gateway Block-Store Validation

## Status

Approved validation package for the approved repository-owned HTTP/3 QUIC
gateway-backed block-store implementation in
`docs/specs/lexonarchivebuilder-block-store-http3/requirements.md` and
`docs/specs/lexonarchivebuilder-block-store-http3/design.md`.

## Validation Scope

These validation entries define the expected conformance surface for the
LexonArchiveBuilder gateway-backed read-only block-store boundary and its
approved profile integration.

## Validation Entries

### VAL-H3BS-001

Inspect the specification package and the crate boundary it defines.

**Pass condition:** the repository defines a separate
`lexonarchivebuilder-block-store-http3` boundary for a read-only gateway-backed
adapter rather than folding the design into unrelated writable storage profiles
or redefining the upstream `BlockStore` trait.

**Traces to:** RQ-H3BS-001, RQ-H3BS-008, DSG-H3BS-001

### VAL-H3BS-002

Inspect the gateway-addressing contract and any corresponding configuration
surface.

**Pass condition:** the approved operator-facing locator is a gateway DNS host
name, and the design derives HTTPS over QUIC on port `443` from that host
without requiring or claiming an arbitrary scheme, arbitrary port, or arbitrary
base URL contract in the first increment.

**Traces to:** RQ-H3BS-002, DSG-H3BS-002

### VAL-H3BS-003

Inspect the immutable block fetch path mapping and result mapping.

**Pass condition:** block fetches derive the gateway request path from the
immutable block identity using `/block/<block_id>`, preserve returned block
bytes without repository-local translation, map HTTP `404` to missing-block, and
surface transport, protocol, QUIC/TLS/session, or other non-success conditions
as explicit backend failures.

**Traces to:** RQ-H3BS-003, RQ-H3BS-004, DSG-H3BS-003, DSG-H3BS-004

### VAL-H3BS-004

Inspect unsupported-operation handling for the gateway-backed adapter.

**Pass condition:** immutable block writes and whole-store block-ID iteration
fail explicitly rather than silently succeeding, silently no-oping, or implying
writable approval for the `gateway-http3` profile.

**Traces to:** RQ-H3BS-005, DSG-H3BS-005

### VAL-H3BS-005

Inspect the indexer-facing profile integration and representative consuming
surfaces.

**Pass condition:** `gateway-http3` appears as an additive approved profile only
on read-only block-fetching surfaces such as rooted quality, rooted search, and
rooted copy source traversal, while write-bearing or whole-store-enumerating
surfaces continue to require one of the existing writable profiles.

**Traces to:** RQ-H3BS-006, RQ-H3BS-007, DSG-H3BS-006, DSG-H3BS-007

### VAL-H3BS-006

Inspect the updated specification package against repository invariants.

**Pass condition:** the gateway-backed profile remains content-type-neutral,
preserves immutable block identity and payload bytes, stays additive to the
existing writable profile family, and does not redefine indexing, MCP, or
embedding semantics.

**Traces to:** RQ-H3BS-007, RQ-H3BS-008, DSG-H3BS-001, DSG-H3BS-003, DSG-H3BS-006
