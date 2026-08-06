<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder MCP Design

## Status

Approved specification baseline for the MVP implementation scope in
`docs/specs/lexonarchivebuilder-mcp/requirements.md`.

## Scope

This document specifies the LexonArchiveBuilder-owned design for realizing the approved
`lexonarchivebuilder-mcp` requirements.

This document is layered on top of:

- `docs/specs/lexonarchivebuilder-mcp/requirements.md`
- `README.md`
- the user request in this session

This document does not redefine delegated LexonGraph search semantics, result
ranking, chunk generation, or delegated dependency contracts. Those remain
owned by LexonGraph and its subordinate crates or APIs.

## Impact Map

### Directly affected artifacts

- `docs/specs/lexonarchivebuilder-mcp/requirements.md`
- `docs/specs/lexonarchivebuilder-mcp/design.md`
- `docs/specs/lexonarchivebuilder-mcp/validation.md`

### Indirectly affected artifacts

- `README.md`, which already describes LexonArchiveBuilder as an MCP server over a
  shared local-versus-production architecture
- future Rust crates, configuration, and test artifacts for `lexonarchivebuilder-mcp`
  that are not yet present in this repository

### Unaffected artifacts

- `docs/specs/lexonarchivebuilder-indexer/*`
- LexonGraph indexing internals
- LexonGraph search internals
- deployment workflow details beyond the existing local/testing and production
  split

## Design Goals

The LexonArchiveBuilder MCP design is intended to be:

- an MCP adaptation layer over delegated LexonGraph search behavior
- explicit about ownership boundaries
- stable across local and production environments
- minimal and fully executable in the local/testing profile first
- extensible to future content types
- consistent about preserving source-name metadata when delegated search
  results provide it

## Boundary Design

### DSG-LFM-001 `Delegated search boundary`

LexonArchiveBuilder owns MCP-facing request and response adaptation, environment-
specific dependency selection, and repository-local wiring to delegated
LexonGraph search APIs.

LexonArchiveBuilder does not own query interpretation, search ranking, chunk
generation, or canonical retrieval semantics internal to the delegated
LexonGraph stack.

**Traces to:** RQ-MCP-001, RQ-MCP-002, RQ-MCP-010, RQ-MCP-011

### DSG-LFM-002 `MCP operation families`

`lexonarchivebuilder-mcp` exposes two operation families at the MCP boundary:

- chunk-returning search operations
- named retrieval operations for email, thread, and document items

The operation families stay content-oriented rather than backend-oriented so
local/testing and production deployments preserve one stable MCP contract.

**Traces to:** RQ-MCP-001, RQ-MCP-003, RQ-MCP-005, RQ-MCP-007

### DSG-LFM-003 `Search result projection`

LexonArchiveBuilder projects delegated LexonGraph search results into MCP responses
without collapsing chunk-oriented output to only top-level item identifiers.

When the delegated result includes the originating source item's name,
LexonArchiveBuilder preserves that name in the MCP response instead of dropping it or
reconstructing a different repository-local name.

**Traces to:** RQ-MCP-003, RQ-MCP-004

### DSG-LFM-004 `Named retrieval projection`

LexonArchiveBuilder exposes retrieval operations for the initially required item
classes of email, thread, and document and forwards the caller-supplied name
selector to the delegated retrieval flow when that delegated contract exists.

The MCP layer preserves class-specific retrieval boundaries and surfaces
delegated unsuccessful lookup outcomes rather than inventing repository-local
fallback behavior.

When the delegated LexonGraph contract does not provide name-based retrieval
for the requested item class, the first MVP returns an explicit unsupported or
unavailable outcome and does not implement repository-local metadata scanning
as a substitute retrieval engine.

**Traces to:** RQ-MCP-005, RQ-MCP-005A, RQ-MCP-011

## Adapter Design

### DSG-LFM-005 `Delegated dependency adapter boundary`

LexonArchiveBuilder provides the concrete trait plugins, adapters, or equivalent
integrations needed by the delegated LexonGraph search APIs to reach
repository-managed dependencies.

- the initial required dependency class is block storage
- the first MVP must make the local/testing dependency path executable against
  filesystem-backed block access
- additional delegated query-time dependencies, if required, are integrated
  behind the same boundary instead of leaking backend-specific details into MCP
  request or response contracts

**Traces to:** RQ-MCP-006, RQ-MCP-007A, RQ-MCP-012

### DSG-LFM-006 `Environment profile selection`

LexonArchiveBuilder selects delegated dependency integrations as an environment
profile:

| Profile | Storage / block access | Query-time embeddings when required by delegated search |
|---|---|---|
| local/testing | direct local filesystem-backed access, or the preserved `local-overlay` storage shape for overlay-backed local testing | local embedding service using the same Docker-containerized embedding engine profile as the indexer |
| production | overlay block store: memory cache + local filesystem cache + Azure Blob SAS-backed access | Azure OpenAI |
| production-v2 | direct Azure-backed LexonGraph block store | Azure OpenAI |

This selection is configuration-driven and preserves one delegated search flow
independent of environment.

The non-local MCP storage family intentionally excludes a plain Azure Blob-only mode
or a caller-assembled arbitrary storage stack. That keeps `search_chunks` and
the named retrieval tools on one shared storage-targeting contract even when a
given retrieval operation is currently specified to return an explicit
unsupported or unavailable outcome.

For the first MVP, only the local/testing family must be executable end to end.
That family includes the direct-local baseline and the preserved
`local-overlay` configuration shape for overlay-backed local testing. The
production and `production-v2` profiles remain preserved adapter and
configuration boundaries rather than executable runtime paths in this
increment.

**Traces to:** RQ-MCP-006, RQ-MCP-007, RQ-MCP-007A, RQ-MCP-012

### DSG-LFM-006A `Local MVP conformance surface`

The first `lexonarchivebuilder-mcp` MVP fixes its executable conformance surface to the
local/testing profile with:

- a local filesystem-backed block-store access path
- a Docker-containerized local embedding service aligned with the indexer's
  local embedding engine profile

This constraint fixes the first executable environment slice without changing
the MCP operation families, response shape, or delegated search ownership
model.

**Traces to:** RQ-MCP-006, RQ-MCP-007, RQ-MCP-007A

### DSG-LFM-007 `Local and production parity boundary`

Local/testing and production environments differ only in adapter realization
and provider configuration, not in the MCP operation families, chunk-oriented
response shape, or delegated search ownership model.

The MCP boundary remains OS-agnostic at the contract level so Linux and
Windows clients consume the same search and retrieval surface regardless of the
host operating system.

The MVP realizes this parity boundary by keeping the MCP contract and adapter
selection model environment-neutral even though only the local/testing profile
is required to execute in the first increment.

Within that parity boundary, all MCP tools share the same approved storage
family: direct local filesystem access, the preserved `local-overlay` testing
shape, the fixed non-local overlay of memory cache plus local filesystem cache
plus Azure Blob SAS-backed storage, and preserved `production-v2`
compatibility. No tool defines a plain Azure Blob-only targeting exception.

**Traces to:** RQ-MCP-007, RQ-MCP-009, RQ-MCP-012

## Invariant Design

### DSG-LFM-008 `Indexing separation`

The `lexonarchivebuilder-mcp` specification package remains separate from indexer
artifacts. No design element in this package changes indexing contracts,
content-resolution behavior, or batch indexing orchestration.

**Traces to:** RQ-MCP-010

### DSG-LFM-009 `Delegated contract subordination`

The design stays subordinate to delegated LexonGraph search and dependency
contracts. The MCP layer adapts them into repository-owned operations but does
not redefine query semantics, result-ranking semantics, or backend-specific
storage rules.

This subordination also applies to named retrieval: the MVP may expose the
operation surface, but it does not invent repository-local retrieval semantics
when the delegated contract is absent.

**Traces to:** RQ-MCP-002, RQ-MCP-005A, RQ-MCP-011

### DSG-LFM-010 `Future content extensibility`

Future content types are added by extending content-type routing and result
projection behind the existing MCP boundary rather than redefining the core
chunk-search contract or the environment-selection contract.

**Traces to:** RQ-MCP-008, RQ-MCP-012

## Verification Realization

### DSG-LFM-011 `Repository verification scope`

LexonArchiveBuilder-owned verification artifacts validate:

- correct delegation from MCP operations to LexonGraph search and retrieval
- preservation of chunk-oriented output and source-name metadata
- correct selection and use of environment-specific dependency integrations
- executable local/testing conformance against filesystem-backed block access
  and the indexer-aligned Docker-containerized embedding profile
- correct preservation of the shared two-mode local-versus-overlay block-store
  targeting contract across `search_chunks` and the named retrieval tools

- explicit unsupported or unavailable named-retrieval outcomes when no
  delegated name-based retrieval contract exists for the requested item class
- preservation of one stable MCP contract across environments

LexonArchiveBuilder-owned verification artifacts do not attempt to revalidate
LexonGraph's own search semantics or dependency-trait contracts beyond proving
that LexonArchiveBuilder consumes them correctly.

**Traces to:** RQ-MCP-005A, RQ-MCP-007A, RQ-MCP-011, RQ-MCP-012

## Incremental Design Patch: Read-only redb access

The MCP runtime uses the shared read-only block-store constructor for local-redb
search and retrieval without changing delegated search semantics or response
projection.

## Incremental Design Patch: Format-neutral rooted MCP targets

### DSG-MCP-FORMAT-001 `Format-neutral MCP target preparation`

For `search_chunks`, the MCP runtime loads the validated root through its
configured block store, requests a logical `f32` query vector through its
existing provider boundary, and passes both values to
`lexongraph_search::prepare_target_embedding`.

The returned upstream target is supplied to the existing default
`lexongraph-search` searcher. The MCP runtime does not create an
`EncodedTargetEmbedding` from the configured root format, branch on encoding
names, parse EBCP descriptors, or encode physical target bytes.

**Traces to:** RQ-MCP-013, RQ-MCP-002, RQ-MCP-011

### DSG-MCP-FORMAT-002 `MCP contract preservation`

Target preparation is an internal runtime substitution. `search_chunks` keeps
the same tool request, top-k and traversal-width rules, storage/provider
selection, result projection, and runtime-error propagation. An upstream
target-preparation error is returned through the existing MCP failure path and
does not trigger a local format fallback.

A leaf root is returned as the same explicit runtime failure. The runtime does
not introduce a leaf-specific encoding or search path.

**Traces to:** RQ-MCP-013, RQ-MCP-003, RQ-MCP-004

## Incremental Design Patch: Gateway-backed MCP search

### DSG-MCP-GATEWAY-001 `MCP-specific gateway configuration`

`McpConfig.environment` SHALL accept either the existing shared
`EnvironmentConfig` shapes or an MCP-specific `gateway-http3` shape. The
gateway shape SHALL not be added to the shared indexer environment enum because
it represents a read-only search-serving transport rather than an indexer
execution environment.

The gateway configuration SHALL contain:

```json
{
  "kind": "gateway-http3",
  "gateway_dns_name": "<operator-provided-gateway-dns-name>",
  "model": "all-MiniLM-L6-v2",
  "request_timeout_secs": 30,
  "max_retries": 5,
  "retry_delay_ms": 1000
}
```

`model`, `request_timeout_secs`, `max_retries`, and `retry_delay_ms` use the
same defaults as the local embedding configuration when omitted. The gateway
shape SHALL reject unknown fields, including a local `base_url`, an embedding
endpoint, or an embedding API-key environment variable, so it cannot appear to
support a separate provider.

The gateway authority is deployment configuration. No Rust default,
repository-tracked configuration template, or documentation example embeds the
tested gateway address.

**Traces to:** RQ-MCP-014, RQ-MCP-015, RQ-MCP-012

### DSG-MCP-GATEWAY-002 `Coupled gateway dependency selection`

The runtime SHALL select the block store and embedding provider from one MCP
environment value:

| Environment shape | Block-store construction | Embedding-provider construction |
|---|---|---|
| Existing shared environment shape | Existing read-only shared-environment constructor | Existing shared-environment constructor |
| `gateway-http3` | `ConfiguredBlockStore::gateway_http3_store(gateway_dns_name)` | `ConfiguredEmbeddingProvider::gateway_http3(gateway_dns_name, model, max_retries, retry_delay_ms, request_timeout_secs)` |

The runtime SHALL use the selected block store to load the root and search
descendants, then use the selected provider to obtain the logical `f32` query
vector. It SHALL keep the existing
`prepare_target_embedding(root, logical_embedding)` flow and existing
`search_with_partial_retry` invocation.

No storage or embedding fallback is permitted when either gateway constructor,
read, or embedding request fails.

**Traces to:** RQ-MCP-014, RQ-MCP-013, RQ-MCP-002

### DSG-MCP-GATEWAY-003 `Gateway example and stdio hosting`

Add a gateway MCP configuration example with:

```json
{
  "environment": {
    "kind": "gateway-http3",
    "gateway_dns_name": "<operator-provided-gateway-dns-name>"
  },
  "embedding_spec": {
    "dims": 384,
    "encoding": "f32le"
  },
  "index": {
    "kind": "root-id",
    "root_id": "adbc431aed97ab541ce73d65ce735552821c6c31f9434a4864333013a278fa78"
  },
  "top_k": 5,
  "traversal_width": 3
}
```

The configured embedding specification remains for compatibility with the
MCP configuration shape. The gateway search path derives the requested logical
vector dimensions from the loaded branch root, as already specified by
DSG-MCP-FORMAT-001.

The existing `serve --config <absolute-config-path>` stdio invocation remains
the Copilot CLI hosting integration. The gateway configuration is supplied by
the deployment operator from the template. It SHALL not require a local summary
file, local block-store path, Docker network alias, or a local
embedding-service process.

**Traces to:** RQ-MCP-015, RQ-MCP-014, RQ-MCP-013

## Incremental Design Patch: Leaf-addressed email retrieval

### DSG-MCP-EMAIL-001 `Leaf-addressed email resolution`

`get_email` SHALL parse its existing `name` parameter as a `BlockHash` and use
the same `configured_block_store` selection path as `search_chunks`. It does
not load an index summary, root, embedding provider, or searcher.

The runtime SHALL retrieve only the named block:

1. A malformed block ID returns the existing invalid-block-ID runtime failure.
2. A missing block returns an explicit missing-email-leaf runtime failure.
3. A branch block returns an explicit non-leaf runtime failure.
4. A leaf block's sole entry is projected only when
   `source_kind == "email"`.
5. A non-email leaf returns an explicit no-email-entries runtime failure.

No root traversal, metadata scan outside the selected leaf, subject comparison,
or Message-ID comparison is allowed.

**Traces to:** RQ-MCP-016, RQ-MCP-005A, RQ-MCP-006

### DSG-MCP-EMAIL-002 `Email result projection`

`get_email` SHALL return an `EmailRetrievalResponse`:

```text
{
  leaf_block_id: String,
  entry: SearchChunkHit
}
```

The entry reuses the existing `SearchChunkHit` projection:

- `position` is `0`, the position of the sole entry within the selected leaf.
- `leaf_block_id` is the requested leaf ID.
- `media_type`, `text`, and `metadata` are taken from the stored entry.
- `source_kind`, `source_path`, and `source_name` are projected by the same
  helpers used by `search_chunks`.

This type is returned only on successful email retrieval. `get_document` and
`get_thread` retain `NamedRetrievalResponse` and their explicit
`unsupported` outcome. MCP tool failures are mapped through the existing
server error path rather than returned as success-shaped
`EmailRetrievalResponse` values.

**Traces to:** RQ-MCP-016, RQ-MCP-003, RQ-MCP-004

### DSG-MCP-EMAIL-003 `Read-only environment reuse`

Email retrieval calls the existing read-only block-store selector exactly once
per request. Therefore local, local-redb, local-overlay, production,
production-v2, and `gateway-http3` use their existing storage integrations.
No embedding request is made by `get_email`.

**Traces to:** RQ-MCP-016, RQ-MCP-007, RQ-MCP-012

## Incremental Design Patch: Corpus-specific MCP tool descriptions

### DSG-MCP-TOOL-DESCRIPTIONS-001 `Description configuration`

Add an optional `tool_descriptions` field to `McpConfig`:

```json
{
  "tool_descriptions": {
    "search_chunks": "Search this corpus for relevant evidence before answering.",
    "get_email": "Retrieve the email entry for a leaf block ID returned by search."
  }
}
```

It maps to a `ToolDescriptionsConfig` with optional string fields
`search_chunks`, `get_document`, `get_email`, and `get_thread`. The field names
match registered MCP tool names exactly. Unknown fields are rejected.

Validation SHALL reject an override that trims to an empty string. It preserves
the configured text otherwise, including leading or trailing whitespace, so
the configured client-facing description is not silently rewritten.

**Traces to:** RQ-MCP-017, RQ-MCP-012

### DSG-MCP-TOOL-DESCRIPTIONS-002 `Resolved per-tool descriptions`

Define stable default constants for each currently registered tool description.
`McpConfig` exposes a resolver for each tool that returns:

1. the configured override when present; otherwise
2. the corresponding stable default.

The MCP server receives the parsed `McpConfig` through the existing runtime
and builds the `ToolRouter` with the resolved descriptions when it is created.
The `#[tool]` attributes continue to bind tool names, schemas, and handlers;
their static descriptions are replaced at router construction with the resolved
values.

`ServerHandler::get_info` retains its existing server-level instructions. No
runtime operation reads description configuration after server initialization.

**Traces to:** RQ-MCP-017, RQ-MCP-001

### DSG-MCP-TOOL-DESCRIPTIONS-003 `Sample configuration`

The local MCP sample includes a `tool_descriptions.search_chunks` example that
is specific to the local corpus. The gateway template remains corpus-neutral
and omits `tool_descriptions`; operators add their own overrides in a copied
deployment configuration.

**Traces to:** RQ-MCP-017

## Incremental Design Patch: MCP response timing metadata

### DSG-MCP-RESPONSE-TIMING-001 `Timed structured tool outcomes`

The MCP server SHALL time each handler immediately after rmcp has decoded its
tool parameters. A shared server helper SHALL retain the start instant while
executing the runtime operation and convert the final duration to a
non-negative whole-millisecond `u64` value.

Successful domain results SHALL be serialized as structured MCP content with
their existing fields flattened alongside a top-level `elapsed_ms` field. The
same JSON value SHALL be used for the caller-visible text content so clients
that do not consume structured content retain access to the complete result.

The handler scope ends when it has constructed this MCP result. It therefore
includes runtime validation, configured storage and embedding work, search,
retrieval, and response projection, but excludes parameter decoding and stdio
transport.

**Traces to:** RQ-MCP-018, RQ-MCP-001, RQ-MCP-007, RQ-MCP-010

### DSG-MCP-RESPONSE-TIMING-002 `Timed tool failures`

For a routed invocation whose runtime operation fails, the shared helper SHALL
return rmcp `CallToolResult::structured_error` rather than propagating the
runtime error through the JSON-RPC error path. Its structured content SHALL
be:

```text
{
  error: String,
  elapsed_ms: u64
}
```

The result's MCP `isError` flag SHALL be true. The `error` value is the
existing runtime error text, and the same structured JSON is exposed in the
text content. This preserves a client-visible MCP tool error while providing
the requested timing metadata.

Unroutable calls and malformed tool-argument payloads remain rmcp/router
protocol failures: they do not reach a decoded handler and are outside the
handler-boundary timing contract.

**Traces to:** RQ-MCP-018, RQ-MCP-016

### DSG-MCP-RESPONSE-TIMING-003 `Uniform handler application`

`search_chunks` and `get_email` SHALL execute through the timed success/error
helper. `get_document` and `get_thread` SHALL execute through the timed
success helper, retaining their existing `unsupported` domain response and
adding only `elapsed_ms`.

No runtime search, retrieval, storage, embedding, configuration, or tool
description code changes behavior to collect timing. The README SHALL
describe `elapsed_ms`, its handler-boundary scope, and that a routed tool
failure is represented by MCP `isError` plus structured `error` and
`elapsed_ms` content.

**Traces to:** RQ-MCP-018, RQ-MCP-005, RQ-MCP-016, RQ-MCP-017

## Incremental Design Patch: Gateway-backed filesystem MCP cache

### DSG-MCP-GATEWAY-FS-CACHE-001 `MCP-specific configuration`

Add a `GatewayHttp3FilesystemCacheMcpEnvironmentConfig` variant selected by
`kind: "gateway-http3-fs-cache"`. It reuses the existing gateway model,
timeout, and retry fields and adds optional `block_cache_root` and
`memory_cache_max_resident_blocks`. Omitted values resolve to
`mcp-block-cache` relative to the request configuration directory and 256
resident blocks. Explicit empty paths and zero capacities are rejected.

**Traces to:** RQ-MCP-019, RQ-MCP-014

### DSG-MCP-GATEWAY-FS-CACHE-002 `Three-layer cache overlay`

`ConfiguredBlockStore` SHALL construct an `OverlayBlockStore` in this order:
`PassiveLayer::cache(MemoryBlockStore)`,
`PassiveLayer::cache(FilesystemBlockStore)`, and
`PassiveLayer::read_only(Http3BlockStore)`. Existing overlay refill behavior
populates higher cache layers after a gateway hit; direct writes do not target
the gateway.

**Traces to:** RQ-MCP-019, RQ-MCP-006, RQ-MCP-012

### DSG-MCP-GATEWAY-FS-CACHE-003 `Coupled runtime selection`

Only `gateway-http3-fs-cache` selects this overlay. It uses the same configured
gateway authority for the existing HTTP/3 embedding provider; direct
`gateway-http3` and shared indexer environments remain unchanged. The gateway
template demonstrates this kind and documents optional cache overrides without
committing deployment-specific values.

**Traces to:** RQ-MCP-019, RQ-MCP-014, RQ-MCP-015

### DSG-MCP-GATEWAY-FS-CACHE-004 `Runtime-owned shared store`

`McpRuntime::new` SHALL construct the selected `ConfiguredBlockStore` once and
retain it as runtime state. Each search and block-backed retrieval operation
SHALL clone or borrow that same configured store instead of selecting and
constructing a store from configuration per request. The overlay store already
uses shared-reference block-store operations; no `BlockStore` trait change is
required. The memory store serializes its in-process state with its internal
mutex. Concurrent misses may still issue duplicate lower-layer reads because
the overlay does not provide single-flight coordination.

**Traces to:** RQ-MCP-019
