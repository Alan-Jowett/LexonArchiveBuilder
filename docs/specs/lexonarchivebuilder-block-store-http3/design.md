<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder HTTP/3 Gateway Block-Store Design

## Status

Approved design realized by the approved repository-owned HTTP/3 QUIC
gateway-backed block-store implementation in
`docs/specs/lexonarchivebuilder-block-store-http3/requirements.md`.

## Scope

This document specifies the LexonArchiveBuilder-owned design for realizing the
read-only `lexonarchivebuilder-block-store-http3` crate and integrating it into
the repository's approved block-store profile vocabulary for read-only
block-fetching tool surfaces.

This document is layered on top of:

- `docs/specs/lexonarchivebuilder-block-store-http3/requirements.md`
- `docs/specs/lexonarchivebuilder-indexer/requirements.md`
- `Cargo.toml`
- `crates/lexonarchivebuilder-indexer/src/block_store.rs`
- `crates/lexonarchivebuilder-indexer/src/config.rs`
- `crates/lexonarchivebuilder-indexer/src/main.rs`

This document does not define writable gateway behavior, whole-store gateway
enumeration, a gateway server implementation, arbitrary scheme/port/base-URL
selection, or upstream `BlockStore` trait changes.

## Impact Map

### Directly affected artifacts

- `docs/specs/lexonarchivebuilder-block-store-http3/requirements.md`
- `docs/specs/lexonarchivebuilder-block-store-http3/design.md`
- `docs/specs/lexonarchivebuilder-block-store-http3/validation.md`
- `docs/specs/lexonarchivebuilder-indexer/requirements.md`
- `docs/specs/lexonarchivebuilder-indexer/design.md`
- `docs/specs/lexonarchivebuilder-indexer/validation.md`

### Indirectly affected artifacts

- workspace membership and dependencies for the new crate
- indexer block-store profile configuration and CLI/profile wiring
- rooted quality, rooted search, and rooted copy source surfaces that consume the
  approved read-only profile vocabulary

### Unaffected artifacts

- MCP search semantics
- embedding-provider selection semantics
- writable local filesystem, overlay, and `production-v2` storage behavior
- gateway server implementation details

## Design Goals

The `lexonarchivebuilder-block-store-http3` design is intended to be:

- read-only
- identity-preserving
- HTTP/3- and QUIC-specific without inventing a parallel block model
- minimal in operator-facing configuration
- explicit about unsupported operations
- additive to the existing writable block-store profile family
- non-invasive to indexing and MCP semantics

## Boundary Design

### DSG-H3BS-001 `Separate gateway-backed block-store boundary`

`lexonarchivebuilder-block-store-http3` owns the repository-local adapter that
consumes the existing immutable block gateway as a read-only `BlockStore`-style
source.

It does not own the upstream `BlockStore` trait, the gateway server
implementation, repository-owned indexing semantics, or MCP semantics.

**Traces to:** RQ-H3BS-001, RQ-H3BS-008

### DSG-H3BS-002 `DNS-derived HTTPS-over-QUIC authority`

The crate accepts a gateway DNS host name and deterministically derives one
network authority from it for the first increment:

- scheme: `https`
- transport: HTTP/3 over QUIC
- port: `443`

The first increment intentionally avoids a broader authority-description surface
such as arbitrary scheme, arbitrary port, or arbitrary base URL. This keeps the
operator-facing contract aligned with the approved gateway-DNS requirement and
avoids prematurely broadening transport configuration.

**Traces to:** RQ-H3BS-002

### DSG-H3BS-003 `Immutable block fetch mapping`

The gateway-backed adapter resolves each immutable block fetch by deriving the
gateway path `/block/<block_id>` from the requested block identity and issuing a
read over the derived HTTPS-over-QUIC authority.

The design preserves upstream block identity by making the block hash itself the
only gateway path input. The returned block bytes flow back through the adapter
unchanged rather than through a repository-owned decode-and-reencode or identity
translation layer.

**Traces to:** RQ-H3BS-003

### DSG-H3BS-004 `Read-only result mapping`

The adapter maps the gateway's response surface onto `BlockStore`-style read
results as follows:

1. HTTP `200` with the immutable block body returns the block bytes.
2. HTTP `404` returns a missing-block result.
3. Transport failures, TLS or QUIC session failures, protocol violations, and
   any other non-success HTTP status return explicit backend failures.

This design keeps missing-block behavior distinguishable from transport or
service failures and avoids silently treating gateway faults as absent content.

**Traces to:** RQ-H3BS-004

### DSG-H3BS-005 `Explicit unsupported-operation rejection`

Because the first gateway-backed profile is read-only, unsupported operations
such as immutable block writes or whole-store block-ID iteration fail
immediately and explicitly at the adapter boundary.

This prevents read-only callers from accidentally relying on silent no-op
behavior and preserves the repository's writable-flow routing onto the existing
writable profiles.

**Traces to:** RQ-H3BS-005, RQ-H3BS-007

### DSG-H3BS-006 `Approved read-only profile integration`

The repository integrates the gateway-backed adapter as an additive
`gateway-http3` profile within the shared block-store targeting vocabulary, but
only on tool surfaces that can operate correctly through rooted read-only block
fetches.

Representative approved surfaces include:

- rooted quality traversal
- rooted CLI search traversal
- rooted block copy on the source side
- future read-only rooted traversal or inspection tools

Representative excluded surfaces include:

- indexing-time writes
- replay-journal publication
- mutable current-root publication
- rooted block copy on the destination side
- any flow that requires authoritative whole-store iteration

This keeps the repository-wide targeting vocabulary coherent while still
preserving the semantic difference between read-only gateway access and writable
storage realizations.

**Traces to:** RQ-H3BS-006, RQ-H3BS-007, RQ-H3BS-008

## Cross-Spec Propagation

### DSG-H3BS-007 `Indexer profile cross-reference`

The indexer specification package treats `gateway-http3` as an additive profile
under the same shared profile vocabulary as the existing writable profiles, but
with explicit source-only or read-only applicability where a tool surface
requires writes or whole-store enumeration.

This propagation point is intentionally narrow: it adjusts profile vocabulary
and applicability rules without redefining the existing local/testing,
production, or `production-v2` adapter behaviors.

**Traces to:** RQ-H3BS-006, RQ-H3BS-007
