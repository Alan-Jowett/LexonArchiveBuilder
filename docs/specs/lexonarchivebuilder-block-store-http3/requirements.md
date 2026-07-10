<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder HTTP/3 Gateway Block-Store Requirements

## Document Status

- **Phase:** Phase 7 - User Review of Implementation
- **Status:** Approved implementation package
- **Scope:** Separate repository-owned read-only `BlockStore`-style crate and
  approved indexer profile for fetching immutable LexonGraph blocks from the
  block gateway over HTTP/3 QUIC

## USER-REQUEST

- **UR-H3BS-1 [KNOWN]:** Create a branch where we will create the
  `lexonarchivebuilder-block-store-http3` crate.
- **UR-H3BS-2 [KNOWN]:** The crate should be LexonGraph-style block storage that
  uses HTTP/3 QUIC to fetch blocks from the gateway.
- **UR-H3BS-3 [KNOWN]:** The crate should accept the gateway DNS name as a
  parameter.
- **UR-H3BS-4 [KNOWN]:** This increment should already wire the new crate in as
  a new approved block-store profile for the indexer and related tools.
- **UR-H3BS-5 [KNOWN]:** The approved operator-facing configuration should be a
  DNS host name only, with implied HTTPS over QUIC on port 443.
- **UR-H3BS-6 [KNOWN]:** The new gateway-backed profile should be read-only and
  limited to block-fetching tool surfaces in this increment.
- **UR-H3BS-7 [KNOWN]:** Unsupported `BlockStore` operations such as writes and
  whole-store iteration should fail explicitly.
- **UR-H3BS-8 [KNOWN]:** Gateway fetches should map HTTP `404` to missing-block
  results, and transport, protocol, or other non-success responses should fail
  explicitly.
- **UR-H3BS-9 [KNOWN]:** Create a dedicated spec package for
  `lexonarchivebuilder-block-store-http3` plus any needed indexer
  cross-references.
- **UR-H3BS-10 [INFERRED]:** The gateway-backed profile should preserve
  immutable block identity and fetch the exact gateway-published block bytes
  rather than introducing repository-local translation of block IDs or payloads.
- **UR-H3BS-11 [INFERRED]:** Because the new profile is read-only, write-bearing
  indexing and publication flows must remain on the existing writable
  block-store profiles instead of silently degrading or partially succeeding.

## Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-H3BS-001 | Add | Introduce a separate repository-owned requirements boundary for the HTTP/3 QUIC gateway-backed block-store crate | UR-H3BS-1, UR-H3BS-2, UR-H3BS-9 |
| CM-H3BS-002 | Add | Define a gateway-addressing contract that accepts only a DNS host name and derives HTTPS-over-QUIC authority from that host on port 443 | UR-H3BS-3, UR-H3BS-5 |
| CM-H3BS-003 | Add | Require immutable block fetches to map block IDs onto the gateway block endpoint without repository-local identity translation | UR-H3BS-2, UR-H3BS-10 |
| CM-H3BS-004 | Add | Define read-only result semantics where `404` means missing block and other transport or protocol failures surface explicitly | UR-H3BS-6, UR-H3BS-7, UR-H3BS-8 |
| CM-H3BS-005 | Add | Require explicit rejection of unsupported write and whole-store-iteration operations | UR-H3BS-6, UR-H3BS-7, UR-H3BS-11 |
| CM-H3BS-006 | Add | Require indexer-facing integration as an approved read-only block-store profile rather than a standalone crate with no repository contract | UR-H3BS-4, UR-H3BS-6, UR-H3BS-9 |
| CM-H3BS-007 | Add | Preserve existing writable storage profiles and operator semantics for flows that require writes or full-store traversal | UR-H3BS-6, UR-H3BS-11 |

## Before / After

### BA-H3BS-001

- **Before [KNOWN]:** The repository's approved block-store profile vocabulary
  covers local filesystem, the production overlay profile, and the direct
  Azure-backed `production-v2` profile, but not a gateway-backed HTTP/3 QUIC
  fetch profile.
- **After [KNOWN]:** The requirements define a separate gateway-backed profile
  boundary for immutable block fetches over HTTP/3 QUIC.

### BA-H3BS-002

- **Before [KNOWN]:** Non-local block access in repository-owned tooling is
  defined in terms of writable Azure-backed profiles rather than a read-only
  gateway contract addressed by DNS host name.
- **After [KNOWN]:** The requirements define a DNS-name-based gateway contract
  with implied HTTPS over QUIC on port 443.

### BA-H3BS-003

- **Before [KNOWN]:** The repository does not yet define how an HTTP gateway
  response should map onto `BlockStore`-style missing-block and backend-failure
  semantics.
- **After [KNOWN]:** The requirements define `404` as missing-block and reserve
  explicit failures for transport, protocol, and other non-success conditions.

### BA-H3BS-004

- **Before [KNOWN]:** The repository's block-store requirements assume approved
  profiles are usable on read/write surfaces unless a tool narrows them
  implicitly.
- **After [KNOWN]:** The requirements explicitly constrain the gateway-backed
  profile to read-only block-fetching surfaces and preserve existing writable
  profiles for indexing, publication, and destination-write flows.

## Requirements

### Functional Requirements

#### RQ-H3BS-001 - Separate gateway-backed block-store boundary

LexonArchiveBuilder SHALL define a separate repository-owned requirements
boundary for `lexonarchivebuilder-block-store-http3`.

- **Boundary [KNOWN]:** This boundary owns the HTTP/3 QUIC gateway-backed
  `BlockStore`-style client contract.
- **Non-goal [KNOWN]:** This boundary does not redefine the upstream
  `lexongraph_block_store::BlockStore` trait contract itself.
- **Traceability:** UR-H3BS-1, UR-H3BS-2, UR-H3BS-9

#### RQ-H3BS-002 - Gateway addressing by DNS host name

The gateway-backed block-store contract SHALL accept the gateway DNS host name
as its operator-facing network locator.

- **Required property [KNOWN]:** The approved first increment derives HTTPS over
  QUIC authority from that DNS host name and uses port `443`.
- **Constraint [KNOWN]:** The first increment does not require a caller-supplied
  scheme, arbitrary port, or arbitrary base URL.
- **Traceability:** UR-H3BS-3, UR-H3BS-5

#### RQ-H3BS-003 - Immutable block fetch contract

The gateway-backed block-store SHALL fetch immutable block bytes by block ID
through the gateway's block endpoint.

- **Required property [KNOWN]:** The request path is derived from the immutable
  block identity and targets the gateway's `/block/<block_id>` contract.
- **Identity boundary [INFERRED]:** The gateway-backed client SHALL NOT
  repository-locally translate block identities or rewrite returned block bytes.
- **Traceability:** UR-H3BS-2, UR-H3BS-10

#### RQ-H3BS-004 - Read-only missing-block and failure semantics

The gateway-backed block-store SHALL expose read-only fetch results that align
with `BlockStore` missing-block and backend-failure behavior.

- **Missing-block mapping [KNOWN]:** HTTP `404` SHALL map to a missing-block
  result rather than an exceptional failure.
- **Failure mapping [KNOWN]:** Transport failures, QUIC/TLS/session failures,
  protocol violations, and non-`404` non-success HTTP responses SHALL surface as
  explicit backend failures.
- **Traceability:** UR-H3BS-6, UR-H3BS-8

#### RQ-H3BS-005 - Explicit rejection of unsupported operations

The gateway-backed block-store SHALL fail explicitly when a caller requests a
write or whole-store-iteration behavior that the read-only gateway profile does
not support.

- **Unsupported operations [KNOWN]:** This includes immutable block writes and
  whole-store block-ID iteration in the first increment.
- **Safety boundary [INFERRED]:** Unsupported operations must fail explicitly
  rather than silently no-op, partially emulate writable behavior, or imply
  repository approval for write-bearing flows on the gateway profile.
- **Traceability:** UR-H3BS-6, UR-H3BS-7, UR-H3BS-11

#### RQ-H3BS-006 - Approved indexer profile integration

LexonArchiveBuilder SHALL integrate the gateway-backed block-store as an
approved repository-owned read-only profile for indexer-owned block-fetching
tool surfaces.

- **Applies to [KNOWN]:** Read-only rooted retrieval, rooted quality analysis,
  rooted copy source traversal, and similar future read-only block-fetching
  surfaces may adopt this profile.
- **Excludes [KNOWN]:** Indexing-time writes, mutable-reference publication,
  replay-journal publication, rooted-copy destination writes, and any flow that
  requires whole-store iteration remain outside this profile in the first
  increment.
- **Traceability:** UR-H3BS-4, UR-H3BS-6, UR-H3BS-9, UR-H3BS-11

### Boundary and Invariant Requirements

#### RQ-H3BS-007 - Writable-profile preservation

The new gateway-backed read-only profile SHALL be additive and SHALL NOT replace
or weaken the existing writable block-store profiles.

- **Preserved profiles [KNOWN]:** Local filesystem, the production overlay
  profile, and the direct Azure-backed `production-v2` profile remain the
  writable repository-approved modes.
- **Boundary [INFERRED]:** Tools that require write capability or authoritative
  whole-store enumeration must continue using an approved writable profile.
- **Traceability:** UR-H3BS-6, UR-H3BS-11

#### RQ-H3BS-008 - Semantic non-interference

The gateway-backed profile SHALL add a transport-backed block fetch option
without redefining indexing, MCP, content-model, or embedding semantics.

- **Boundary [INFERRED]:** This increment changes the storage-access profile
  vocabulary, not the delegated LexonGraph indexing contract or MCP search
  contract.
- **Traceability:** UR-H3BS-4, UR-H3BS-9

## Out of Scope

- redefining the upstream `BlockStore` trait
- introducing repository-owned block-ID or payload translation
- making the gateway-backed profile writable in this increment
- requiring arbitrary scheme, port, or base-URL configuration
- redefining the gateway server contract beyond the existing `/block/<block_id>`
  immutable fetch path
- replacing existing writable storage profiles for indexing or publication flows

## Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | The new crate only adds a block-fetch profile and does not merge indexing and serving roles |
| Local/testing versus production behavior stays behind stable adapters | Preserved with expanded read-only profile set | The repository gains a new read-only remote adapter without collapsing the existing writable profile family |
| The architecture remains extensible to future content types | Preserved | The profile is block-oriented and content-type-neutral rather than email- or document-specific |
| The repository remains subordinate to LexonGraph-owned block semantics | Preserved | The requirements explicitly keep immutable block identity and payload bytes unchanged across the gateway boundary |

## Coverage Notes

- **Covered sources [KNOWN]:**
  - `Cargo.toml:1-45`
  - `crates/lexonarchivebuilder-indexer/src/block_store.rs:1-207`
  - `crates/lexonarchivebuilder-indexer/src/config.rs:153-258`
  - `crates/lexonarchivebuilder-indexer/src/main.rs:143-307`
  - `docs/specs/lexonarchivebuilder-indexer/requirements.md:202-214`
  - `docs/specs/lexonarchivebuilder-indexer/requirements.md:1258-1367`
  - user request in this session: "switch to main, pull, then create a branch
    where we will create the lexongraph style block storage crate that uses
    http/3 QUIC to fetch blocks from the gateway. It should accept the gateway
    dns name as a parameter"
  - user clarification in this session selecting: "Add a new approved
    block-store profile now"
  - user clarification in this session selecting: "DNS host name only with
    implied HTTPS/443 (Recommended)"
  - user clarification in this session selecting: "Read-only profile limited to
    block-fetching surfaces (Recommended)"
  - user clarification in this session selecting: "Fail explicitly for
    unsupported operations (Recommended)"
  - user clarification in this session selecting: "Yes — 404 means missing
    block; everything else fails explicitly (Recommended)"
- **Excluded from this requirements artifact [KNOWN]:**
  - implementation details of the HTTP/3 QUIC client
  - CLI flag naming and config-field naming before the implementation phase
  - design or validation changes before the Phase 2 gate
