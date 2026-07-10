<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder Block Gateway Design

## Status

Approved specification package with implemented Azure Table overlay gateway mode
derived from `docs/specs/lexonarchivebuilder-block-gateway/requirements.md`.

## Scope

This document specifies the LexonArchiveBuilder-owned design for a separate
retrieval-only HTTP/3 block gateway that:

- fronts an approved Azure Table-backed delegated `BlockStore` profile
- serves immutable block bytes from `/block/<block_id>`
- uses startup-time storage-profile configuration
- returns block bytes as `application/octet-stream`
- projects immutable-cache semantics for successful responses
- remains deployable as a VM-hosted process, containerized process, or
  function-hosted process

This document is layered on top of:

- `docs/specs/lexonarchivebuilder-block-gateway/requirements.md`
- `docs/specs/lexonarchivebuilder-indexer/requirements.md`
- `docs/specs/lexonarchivebuilder-indexer/design.md`
- `docs/specs/lexonarchivebuilder-mcp/requirements.md`
- `docs/specs/lexonarchivebuilder-deployment/requirements.md`
- `README.md`

This document does not redefine indexing semantics, MCP search semantics,
content-type-specific retrieval semantics, or the delegated LexonGraph
`BlockStore` contract.

## Impact Map

### Directly affected artifacts

- `docs/specs/lexonarchivebuilder-block-gateway/requirements.md`
- `docs/specs/lexonarchivebuilder-block-gateway/design.md`
- `docs/specs/lexonarchivebuilder-block-gateway/validation.md`

### Indirectly affected artifacts

- future Rust implementation artifacts for the gateway runtime
- future hosting assets for VM, container, or function deployment
- future operator documentation for startup configuration and deployment

### Unaffected artifacts

- indexer replay, chunking, clustering, and publication semantics
- MCP search and named-retrieval semantics
- local/testing filesystem-backed indexing workflows
- deployment requirements unrelated to this additive block-serving seam

## Design Goals

The `lexonarchivebuilder-block-gateway` design is intended to be:

- retrieval-only and block-oriented
- subordinate to delegated LexonGraph storage contracts
- thin and stateless
- explicit about the additive architectural exception it introduces
- deployable without changing its application contract across VM, container, and
  function hosting forms
- safe for immutable caching
- neutral to current and future content types

## Boundary Design

### DSG-BGW-001 `Gateway boundary ownership`

`lexonarchivebuilder-block-gateway` owns the external HTTP block-fetch contract,
startup-time storage configuration, response projection, and hosting-neutral
runtime shape for immutable block retrieval.

`lexonarchivebuilder-block-gateway` does not own indexing, block construction,
MCP result shaping, or repository-local storage-table semantics.

**Traces to:** RQ-BGW-001, RQ-BGW-011, RQ-BGW-012

### DSG-BGW-002 `Single block-fetch route`

The gateway exposes one repository-owned retrieval route at
`/block/<block_id>`.

That route accepts a caller-supplied block identifier and maps it to one
delegated block-store lookup. The route remains retrieval-only and does not add
search, list, write, or mutation behavior.

The approved contract in this increment is the block-fetch route itself.
Whether a later increment adds HEAD or other auxiliary routes remains outside
the approved current surface.

**Traces to:** RQ-BGW-003, RQ-BGW-010, RQ-BGW-014

### DSG-BGW-003 `Startup-configured Azure Table dependency`

The gateway binds its Azure Table-backed storage dependency at process startup.

The startup configuration selects one approved backend profile:

1. direct Azure Storage Table v2
2. additive overlay-backed Azure Table v2

The direct profile binds one Azure Storage Table SAS URL. The overlay-backed
profile binds the Azure Storage Table v2 backing-store configuration together
with one filesystem-cache root and one in-memory cache-capacity setting.

Those inputs may be supplied through direct process arguments or equivalent
startup environment configuration, but they are always bound before the gateway
serves requests.

The external HTTP caller never supplies backend credentials. This preserves one
stable fetch contract across VM, container, and function-hosted realizations,
because hosting-specific request adapters only need to invoke the same
preconfigured application boundary.

**Traces to:** RQ-BGW-004, RQ-BGW-005A, RQ-BGW-005B, RQ-BGW-009

### DSG-BGW-004 `Delegated block-store reuse`

The gateway resolves blocks through the same delegated LexonGraph Azure Table
block-store family already present in the repository's Azure-oriented storage
seams.

The gateway consumes the delegated block-store abstraction as a backend-owned
byte store. It does not reinterpret block bytes, re-encode payloads, or define
repository-local table entities or row schemas.

This keeps the gateway aligned with the repository's shared `BlockStore`
boundary rather than inventing a gateway-specific storage protocol.

**Traces to:** RQ-BGW-005, RQ-BGW-012

### DSG-BGW-004A `Approved gateway storage-profile set`

The gateway's non-local storage boundary is intentionally fixed to one approved
Azure Table-backed profile set rather than to an arbitrary caller-assembled
storage stack.

That profile set contains exactly:

- the existing direct Azure Storage Table v2 profile
- the additive overlay-backed profile composed of:
  - an in-memory cache layer
  - a local filesystem cache layer
  - an Azure Storage Table v2 backing-data layer

This preserves one repository-defined operator vocabulary for the gateway while
keeping the overlay composition narrow and explicit.

**Traces to:** RQ-BGW-005, RQ-BGW-005A, RQ-BGW-005B

### DSG-BGW-004B `Storage-mode-neutral fetch path`

The route handler and HTTP response projection consume only the delegated
`BlockStore` byte-retrieval contract, independent of whether startup
configuration selects the direct or overlay-backed Azure Table profile.

The selected profile may change lookup latency or cache hit behavior, but it
does not change:

- route shape
- block-identifier parsing
- successful payload bytes
- content type
- immutable-cache response semantics
- externally visible non-success normalization

This keeps the overlay-backed mode an internal storage-adapter choice rather
than a new externally visible gateway protocol.

**Traces to:** RQ-BGW-005C, RQ-BGW-006, RQ-BGW-007, RQ-BGW-008

### DSG-BGW-005 `Exact-byte response projection`

When delegated lookup succeeds, the gateway projects the stored block bytes
directly into the HTTP response body and marks the response as
`application/octet-stream`.

Because the served content is immutable and hash-addressed, the response policy
also projects long-lived immutable-cache semantics. The gateway's cache headers
communicate that the bytes may be reused indefinitely without changing the
payload contract.

**Traces to:** RQ-BGW-006, RQ-BGW-007, RQ-BGW-012

### DSG-BGW-006 `External 404 normalization`

The gateway normalizes any non-success delegated block-fetch outcome to external
HTTP `404`.

This design choice keeps the client-visible contract narrow even when the
internal failure cause differs between malformed identifiers, absent rows, or
other unsuccessful fetch outcomes at the delegated boundary.

**Traces to:** RQ-BGW-008

### DSG-BGW-007 `Hosting-form neutrality`

The gateway preserves one application contract independent of whether the final
runtime wrapper is:

1. a long-lived VM-hosted daemon
2. a containerized process
3. a function-hosted process

The hosting wrapper may differ in process lifecycle, startup trigger, and
deployment packaging, but it does not change the route shape, startup-time
storage-profile binding model, approved storage-profile set, delegated
block-store dependency, success payload, cache semantics, or unsuccessful
lookup contract.

**Traces to:** RQ-BGW-002, RQ-BGW-009

### DSG-BGW-008 `Scoped architectural exception`

The gateway is an additive repository-owned retrieval seam that explicitly
carves out a narrow server-side exception to the repository's "no server-side
processing beyond indexing" direction.

The exception is bounded by all of the following:

- immutable block retrieval only
- no search semantics
- no write or mutation semantics
- no central coordination or orchestration role
- no content-type-specific behavior

This preserves the broader repository direction while making the new exception
explicit rather than implicit.

**Traces to:** RQ-BGW-002, RQ-BGW-010, RQ-BGW-011, RQ-BGW-013, RQ-BGW-014
