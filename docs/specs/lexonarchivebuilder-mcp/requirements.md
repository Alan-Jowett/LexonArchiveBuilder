<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# MCP Requirements

## Document Status

- **Phase:** Phase 4 - User Review of Specifications
- **Status:** Approved specification baseline for the MVP implementation scope
- **Scope:** `lexonarchivebuilder-mcp` search-serving integration boundary and first in-repo MVP slice

## USER-REQUEST

- **UR-MCP-1 [KNOWN]:** Add a spec trifecta for `lexonarchivebuilder-mcp` under `docs/specs/lexonarchivebuilder-mcp/{requirements|design|validation}.md`.
- **UR-MCP-2 [KNOWN]:** `lexonarchivebuilder-mcp` is an MCP server that wraps the LexonGraph search APIs.
- **UR-MCP-3 [KNOWN]:** The MCP server must return content chunks from search.
- **UR-MCP-4 [KNOWN]:** The MCP server must expose APIs that return specific emails, threads, or documents by name.
- **UR-MCP-5 [KNOWN]:** Search results should also return the name of the document, email, or thread the chunk came from when that name is available from the delegated search API.
- **UR-MCP-6 [KNOWN]:** All actual searching belongs to the delegated LexonGraph search APIs rather than to `lexonarchivebuilder-mcp`.
- **UR-MCP-7 [KNOWN]:** `lexonarchivebuilder-mcp` provides the appropriate trait plugins or adapters for block storage and similar delegated dependencies, analogous to `lexonarchivebuilder-indexer`.
- **UR-MCP-8 [KNOWN]:** The architecture must remain extensible to future content types beyond the initial email and document focus.
- **UR-MCP-9 [KNOWN]:** Local and testing operations use local filesystem-backed content plus local embeddings, while production uses Azure Blob Storage plus Azure OpenAI-backed embeddings.
- **UR-MCP-10 [KNOWN]:** LexonArchiveBuilder serves search and retrieval through an MCP server and intends that surface to stay consistent across environments.
- **UR-MCP-11 [KNOWN]:** The MCP server is intended to remain usable from both Linux and Windows environments.
- **UR-MCP-12 [KNOWN]:** Implement the minimal viable product of `lexonarchivebuilder-mcp` using `docs/specs/lexonarchivebuilder-mcp/*` as the source of truth.
- **UR-MCP-13 [KNOWN]:** The first MVP must be testable against a local filesystem-backed block store and a Docker-containerized local embedding service using the same local embedding engine profile as the indexer.
- **UR-MCP-14 [KNOWN]:** Production storage and embedding integrations should remain pluggable through stable trait or adapter boundaries, but do not need an executable production realization in the first MVP.
- **UR-MCP-15 [KNOWN]:** All MCP tools must allow operators to target the
  approved shared environment/profile family, including:
  - local filesystem-backed access
  - the preserved `local-overlay` shape
  - the overlay block store composed of an in-memory cache, a local filesystem
    cache, and an Azure Blob backing store addressed by SAS URL
  - preserved direct-Azure `production-v2` compatibility where the shared
    indexer configuration family already exposes it
- **UR-MCP-16 [KNOWN]:** For this increment, a plain Azure Blob block-store
  target without the required memory-plus-filesystem overlay is not an approved
  MCP tool-targeting mode, and no MCP tool may invent an undocumented plain
  Azure Blob-only exception outside the approved shared family.
- **UR-MCP-17 [INFERRED]:** The same storage-targeting contract should apply consistently across `search_chunks` and the named retrieval tools, even when a specific retrieval tool currently returns an explicit unsupported outcome rather than traversing stored content.

## Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-MCP-001 | Add | Introduce the first structured requirements artifact for the `lexonarchivebuilder-mcp` boundary | UR-MCP-1 |
| CM-MCP-002 | Add | Define `lexonarchivebuilder-mcp` as an MCP adaptation layer over delegated LexonGraph search behavior rather than an in-repo search engine | UR-MCP-2, UR-MCP-6 |
| CM-MCP-003 | Add | Define the required MCP-facing retrieval surface for chunk search and named retrieval of emails, threads, and documents | UR-MCP-3, UR-MCP-4, UR-MCP-5 |
| CM-MCP-004 | Add | Define environment-specific dependency integration for block storage and related delegated search dependencies | UR-MCP-7, UR-MCP-9, UR-MCP-10 |
| CM-MCP-005 | Add | Capture invariants around indexing/search separation, stable contracts, and future content-type extensibility | UR-MCP-6, UR-MCP-8, UR-MCP-10 |
| CM-MCP-006 | Revise | Narrow the first in-repo MVP realization to an end-to-end local/testing profile while preserving production integration seams | UR-MCP-12, UR-MCP-14 |
| CM-MCP-007 | Add | Require local MVP testability against filesystem-backed block access and the same Docker-containerized local embedding engine profile used by the indexer | UR-MCP-12, UR-MCP-13 |
| CM-MCP-008 | Revise | Replace the current local-versus-plain-Azure dependency split with one repository-wide MCP storage family that keeps direct local filesystem, preserved local-overlay, overlay-backed production, and preserved direct-Azure `production-v2` shapes aligned under the same shared configuration boundary | UR-MCP-15, UR-MCP-16, UR-MCP-17 |

## Before / After

### BA-MCP-001

- **Before [KNOWN]:** The repository had no structured requirements artifact for the `lexonarchivebuilder-mcp` boundary.
- **After [KNOWN]:** The repository has an explicit requirements baseline for the MCP search-serving boundary in `docs/specs/lexonarchivebuilder-mcp/requirements.md`.

### BA-MCP-002

- **Before [KNOWN]:** `README.md` described LexonArchiveBuilder as exposing search and retrieval through an MCP server, but it did not define whether `lexonarchivebuilder-mcp` owned search execution or wrapped delegated LexonGraph search APIs.
- **After [KNOWN]:** The requirements define `lexonarchivebuilder-mcp` as an MCP adaptation layer that delegates search execution to LexonGraph while owning repository-local dependency integrations.

### BA-MCP-003

- **Before [KNOWN]:** The repository described a unified search surface at a high level, but did not capture requirements for chunk-returning search or retrieval of emails, threads, and documents by name.
- **After [KNOWN]:** The requirements define an MCP-facing surface for chunk search plus named retrieval of the initially supported content types.

### BA-MCP-004

- **Before [KNOWN]:** Local-versus-production behavior was documented at the architecture level but not translated into MCP-specific requirements for delegated dependency selection.
- **After [KNOWN]:** The requirements define environment-specific integration boundaries so `lexonarchivebuilder-mcp` can consume local/testing and production storage or embedding backends without changing the MCP contract.

### BA-MCP-005

- **Before [KNOWN]:** The requirements identified both local/testing and production environment targets, but did not identify which subset must be executable in the first in-repo MVP.
- **After [KNOWN]:** The requirements define the first MVP as an end-to-end local/testing realization while preserving production storage and embedding integrations as stable extension seams.

### BA-MCP-006

- **Before [KNOWN]:** The requirements described local filesystem-backed content and local embeddings at the environment level, but did not require the MVP to be testable against a local filesystem-backed block store and the same Docker-containerized local embedding engine profile used by the indexer.
- **After [KNOWN]:** The requirements explicitly bind the MVP's local/testing conformance surface to filesystem-backed block access and an indexer-aligned Docker-containerized local embedding service without changing the MCP contract.

### BA-MCP-007

- **Before [KNOWN]:** The requirements allowed MCP dependency integration to vary between local filesystem-backed block access and a plain Azure-backed production boundary, but they did not require every MCP tool to share one explicit configuration family aligned with the reused indexer environment model.
- **After [KNOWN]:** The requirements now constrain all MCP tools to one approved shared configuration family: direct local filesystem access, the preserved local-overlay testing shape, the overlay-backed production shape, and preserved direct-Azure `production-v2` compatibility without introducing an ad hoc plain Azure Blob-only exception path.


## Requirements

### Functional Requirements

#### RQ-MCP-001 - MCP search-serving boundary

LexonArchiveBuilder SHALL provide an MCP server boundary for `lexonarchivebuilder-mcp` that exposes search and retrieval over indexed knowledge.

- **Rationale [KNOWN]:** `README.md` describes LexonArchiveBuilder as serving search and retrieval through an MCP server.
- **Traceability:** UR-MCP-2, UR-MCP-10

#### RQ-MCP-002 - Delegated search execution

`lexonarchivebuilder-mcp` SHALL delegate search execution and result generation to the underlying LexonGraph search APIs.

- **Non-goal [KNOWN]:** `lexonarchivebuilder-mcp` does not define or implement repository-local search, ranking, chunking, or retrieval algorithms in this scope.
- **Traceability:** UR-MCP-2, UR-MCP-6

#### RQ-MCP-003 - Chunk-returning search results

`lexonarchivebuilder-mcp` SHALL surface content chunks returned by the delegated LexonGraph search APIs through its MCP-facing search operations.

- **Constraint [KNOWN]:** The MCP layer must preserve chunk-oriented search behavior rather than collapsing search output to only top-level document, thread, or email identifiers.
- **Traceability:** UR-MCP-3, UR-MCP-6

#### RQ-MCP-004 - Source-name preservation

When the delegated LexonGraph search result includes the originating source item's name, `lexonarchivebuilder-mcp` SHALL preserve and return that name alongside the chunk result.

- **Initial source item classes [KNOWN]:**
  - emails
  - threads
  - documents
- **Constraint [KNOWN]:** This requirement preserves delegated metadata; it does not require `lexonarchivebuilder-mcp` to invent a source name that the delegated search API does not provide.
- **Traceability:** UR-MCP-5, UR-MCP-6

#### RQ-MCP-005 - Named retrieval operations

`lexonarchivebuilder-mcp` SHALL expose retrieval operations that allow callers to request a specific email, thread, or document by name.

- **MVP realization [KNOWN]:** When the delegated LexonGraph contract does not provide name-based retrieval for a requested item class, the first MVP may return an explicit unsupported or unavailable outcome rather than inventing repository-local fallback matching behavior.
- **Clarification gap [UNKNOWN]:** The canonical meaning of "name" for each item class and the expected behavior when multiple items share that name have not yet been specified.
- **Traceability:** UR-MCP-4

#### RQ-MCP-005A - No repository-local named-retrieval fallback

Until a delegated name-based retrieval contract exists for the requested item
class, the first `lexonarchivebuilder-mcp` MVP SHALL surface an explicit unsupported or
unavailable outcome for named retrieval requests rather than implementing
repository-local metadata scanning or other fallback matching semantics.

- **Rationale [KNOWN]:** The approved MCP boundary keeps actual search and retrieval semantics subordinate to delegated LexonGraph contracts.
- **Traceability:** UR-MCP-4, UR-MCP-6, UR-MCP-12

#### RQ-MCP-006 - Delegated dependency integrations

`lexonarchivebuilder-mcp` SHALL provide the concrete trait plugins, adapters, or equivalent integrations required for the delegated LexonGraph search APIs to access repository-managed dependencies.

- **Required initial dependency class [KNOWN]:** block storage
- **Approved tool-targeting modes [KNOWN]:** The MCP dependency integration
  surface SHALL support the approved shared block-store configuration family:
  direct local filesystem access, the preserved `local-overlay` shape for
  overlay-backed local testing, the approved overlay-backed production shape,
  and preserved `production-v2` compatibility where the shared indexer
  configuration family already exposes it.
- **Disallowed mode [KNOWN]:** A plain Azure Blob block-store target without
  the required overlay layers is not an approved MCP-facing storage mode in
  this increment, and no MCP tool may introduce an undocumented plain-Azure-
  only exception.
- **Current increment [KNOWN]:** The existing local/testing realization remains
  required, and this increment additionally requires the same MCP configuration
  family to preserve the approved non-local shapes without introducing per-tool
  storage variants.
- **MVP realization [KNOWN]:** The first in-repo implementation must include repository-local integrations sufficient for an executable local/testing profile using filesystem-backed block access.
- **Extensibility [INFERRED]:** Additional delegated query-time dependencies should be integrated behind the same stable boundary rather than leaking backend-specific details into the MCP contract.
- **Tool-surface consistency [INFERRED]:** `search_chunks` and the named retrieval tools SHALL share the same storage-targeting contract even when a specific tool currently returns an explicit unsupported outcome rather than dereferencing stored content.
- **Traceability:** UR-MCP-6, UR-MCP-7, UR-MCP-12, UR-MCP-13, UR-MCP-15, UR-MCP-16, UR-MCP-17

#### RQ-MCP-007 - Environment-specific adapter selection

`lexonarchivebuilder-mcp` SHALL select its delegated dependency integrations according to environment without changing the MCP-facing search or retrieval contract.

- **Local/testing [KNOWN]:** direct local filesystem-backed content or block
  access, or the preserved `local-overlay` storage shape together with a local
  embedding service using the same Docker-containerized embedding engine
  profile as the indexer where the delegated search APIs require embeddings
- **Production-oriented [KNOWN]:** either the approved overlay block store
  (memory cache + local filesystem cache + Azure Blob SAS-backed storage) plus
  Azure OpenAI-backed embeddings where the delegated search APIs require
  embeddings, or the preserved direct-Azure `production-v2` shape where the
  shared configuration family already exposes it
- **Constraint [KNOWN]:** Every MCP tool SHALL preserve the same approved
  shared block-store configuration family rather than permitting some tools to
  target local filesystem while others target a plain Azure Blob backend
  directly.
- **Constraint [INFERRED]:** Environment-specific wiring must stay behind stable interfaces so clients do not need different MCP contracts per environment.
- **Traceability:** UR-MCP-7, UR-MCP-9, UR-MCP-10, UR-MCP-13, UR-MCP-14, UR-MCP-15, UR-MCP-16, UR-MCP-17

#### RQ-MCP-007A - Local MVP testability

The first `lexonarchivebuilder-mcp` MVP SHALL be testable end to end against a local filesystem-backed block store and a Docker-containerized local embedding service aligned with the indexer's local embedding profile.

- **Constraint [KNOWN]:** This requirement fixes the MVP's executable local/testing conformance surface without changing the MCP-facing search or retrieval contract.
- **Non-goal [KNOWN]:** This requirement does not require a plain Azure-Blob-without-overlay MCP storage mode.
- **Traceability:** UR-MCP-12, UR-MCP-13, UR-MCP-14

#### RQ-MCP-008 - Future content-type extensibility

`lexonarchivebuilder-mcp` SHALL keep its search and retrieval boundary extensible so future content types can be added without redefining the core MCP search contract.

- **Initial focus [KNOWN]:** emails and documents, with thread retrieval explicitly required in the initial MCP surface
- **Traceability:** UR-MCP-4, UR-MCP-8

#### RQ-MCP-009 - Cross-platform MCP usability

The `lexonarchivebuilder-mcp` search-serving boundary SHALL remain usable from both Linux and Windows environments.

- **Rationale [KNOWN]:** The repository README already states that the MCP server should remain usable from Linux and Windows.
- **Traceability:** UR-MCP-10, UR-MCP-11

### Boundary and Invariant Requirements

#### RQ-MCP-010 - Indexing/search separation

The `lexonarchivebuilder-mcp` requirements SHALL remain limited to search-serving orchestration and delegated dependency integrations and SHALL NOT redefine indexing-time behavior.

- **Rationale [KNOWN]:** The repository baseline separates indexing from search serving.
- **Traceability:** UR-MCP-6, UR-MCP-10

#### RQ-MCP-011 - Subordinate external contracts

LexonArchiveBuilder SHALL remain subordinate to the public contracts owned by the delegated LexonGraph search APIs and the delegated dependency traits they consume, and SHALL NOT redefine their search semantics, result-ranking semantics, or storage-contract semantics within this repository.

- **Rationale [KNOWN]:** The user request explicitly assigns actual searching to the delegated LexonGraph search APIs.
- **Traceability:** UR-MCP-2, UR-MCP-6, UR-MCP-7

#### RQ-MCP-012 - Stable abstraction boundary

LexonArchiveBuilder SHALL keep environment-specific storage, embedding, and other delegated dependency variation behind stable integration boundaries so future content types and backend swaps do not require redefinition of the MCP contract.

- **Traceability:** UR-MCP-7, UR-MCP-8, UR-MCP-9, UR-MCP-10

## Out of Scope

- Defining repository-local search, ranking, chunking, or retrieval algorithms
- Defining repository-local metadata-scanning fallback semantics for named retrieval in the first MVP
- Redefining the public contracts owned by LexonGraph search APIs or their delegated dependency traits
- Defining indexing-pipeline behavior already covered by `docs/specs/lexonarchivebuilder-indexer/*`
- Requiring executable Azure production adapters in the first MCP MVP increment
- Finalizing the exact canonical name format or duplicate-name resolution semantics for named retrieval until the user clarifies them
- Finalizing exact deployment workflow details beyond the already documented local/testing and production environment split

## Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | Requirements explicitly constrain `lexonarchivebuilder-mcp` to the MCP search-serving boundary and delegated search integrations |
| Actual search semantics remain owned by LexonGraph | Preserved | Requirements define delegation rather than an in-repo search engine |
| Environment-specific storage and embedding behavior stays behind stable interfaces | Preserved with revised storage contract | Requirements now constrain all MCP tools to the same two-mode local-versus-overlay block-store contract while preserving a stable MCP-facing contract across environments |
| Architecture remains extensible to future content types | Preserved | Requirements keep the surface centered on stable contracts instead of hard-coding only current item classes |
| Local MVP remains aligned with the indexer's local embedding profile | Preserved | Requirements constrain the executable local/testing profile to the same Docker-containerized embedding engine family without coupling the MCP contract to deployment-specific details |

## Open Questions / Discovery Gaps

- **Q-MCP-001 [UNKNOWN]:** What is the canonical "name" for each retrieval class: email, thread, and document?
- **Q-MCP-002 [UNKNOWN]:** What should `lexonarchivebuilder-mcp` do when a caller-provided name matches multiple items of the same class?
- **Q-MCP-003 [UNKNOWN]:** Should named retrieval require exact-match semantics, case-insensitive matching, or delegated matching behavior owned entirely by LexonGraph?
- **Q-MCP-004 [UNKNOWN]:** Beyond block storage, which delegated query-time dependency traits must `lexonarchivebuilder-mcp` wire directly in-repo for the initial scope?
- **Q-MCP-005 [UNKNOWN]:** Must the MCP local/testing profile reuse the indexer's exact Docker Compose topology, or is compatibility with the same Docker-containerized embedding engine profile sufficient for the first MVP?
- **Q-MCP-006 [UNKNOWN]:** Which delegated LexonGraph contract will eventually own name-based retrieval for email, thread, and document items, and what unsuccessful outcome shape should LexonArchiveBuilder preserve until then?

## Coverage Notes

- **Covered sources [KNOWN]:**
  - `README.md:7-12`
  - `README.md:20-27`
  - `README.md:42-49`
  - `README.md:51-59`
  - `README.md:61-80`
  - `README.md:91-134`
  - `docs/specs/lexonarchivebuilder-indexer/requirements.md:20-25`
  - `docs/specs/lexonarchivebuilder-indexer/requirements.md:111-156`
  - `docs/specs/lexonarchivebuilder-indexer/design.md:120-188`
  - `docs/specs/lexonarchivebuilder-indexer/validation.md:30-84`
  - external LexonGraph repository source (not vendored in LexonArchiveBuilder):
    `crates/lexongraph-search/src/lib.rs`
  - user request in this session
- **Excluded for now [KNOWN]:**
  - Rust implementation file paths, crate manifests, and test artifacts for `lexonarchivebuilder-mcp`, because no repository-local crate or implementation files exist yet
  - external LexonGraph crate source for exact search API and trait names, because that source is not vendored in this repository and was not required to state the repository-local requirements boundary

## Incremental Requirements Patch: Read-only redb access

MCP search and retrieval SHALL open local-redb block stores read-only, preserve
the existing MCP contract, and surface mutation attempts explicitly.

## Incremental Requirements Patch: Format-neutral rooted MCP targets

### USER-REQUEST

- **UR-MCP-FORMAT-001 [KNOWN]:** Update the MCP server to support the same
  format-neutral rooted-search target preparation as the indexer CLI.

### Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-MCP-FORMAT-001 | Revise | Delegate MCP `search_chunks` target preparation to the LexonGraph format-neutral API | UR-MCP-FORMAT-001 |
| CM-MCP-FORMAT-002 | Preserve | Keep the MCP tool schema, storage selection, provider transport, result shape, and ranking delegated to existing boundaries | UR-MCP-FORMAT-001 |

### Before / After

#### BA-MCP-FORMAT-001

- **Before [KNOWN]:** `search_chunks` uses the configured physical embedding
  specification to encode the provider response and directly constructs an
  `EncodedTargetEmbedding`.
- **After [KNOWN]:** `search_chunks` loads the root and passes its provider's
  logical `f32` vector plus the validated root to
  `lexongraph_search::prepare_target_embedding`.

### Requirements

#### RQ-MCP-013 - Format-neutral rooted query targets

The MCP `search_chunks` operation SHALL use the format-neutral
`lexongraph_search::prepare_target_embedding` API for every rooted search.

- **Format boundary [KNOWN]:** The MCP crate SHALL not inspect root embedding
  encoding names, parse descriptors, or implement physical embedding codecs.
- **Provider boundary [KNOWN]:** The existing environment-selected embedding
  provider continues to obtain the logical query vector; its request schema
  and configured transport remain unchanged.
- **Contract preservation [KNOWN]:** `SearchChunksRequest`,
  `SearchChunksResponse`, top-k and traversal-width validation, result
  metadata, and named-retrieval behavior remain unchanged.
- **Failure behavior [INFERRED]:** Upstream target-preparation failure SHALL
  be surfaced as an MCP runtime error rather than falling back to a
  repository-local encoding path.
- **Single-leaf roots [KNOWN]:** `search_chunks` SHALL reject a root that is a
  leaf block. Supporting it would require a separate target-preparation path,
  which is out of scope for the format-neutral MCP contract.
- **Traceability:** UR-MCP-FORMAT-001, RQ-MCP-002, RQ-MCP-011

### Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Actual search semantics remain owned by LexonGraph | Preserved | Target preparation and search execution remain upstream-owned |
| MCP tool contract | Preserved | No request or response schema changes |
| Storage and embedding adapters | Preserved | Existing environment/provider selection is unchanged |
| Future format extensibility | Improved | New supported root formats do not require MCP format switches |

### Coverage Notes

- **Covered sources [KNOWN]:**
  - `crates/lexonarchivebuilder-mcp/src/runtime.rs` currently constructs the
    target from configured physical embedding metadata.
  - `lexongraph_search::prepare_target_embedding` is available through the
    workspace's LexonGraph `90ab28948e6c2c5825311b6a3fbc9b2ec34c84e9` pin.
- **Excluded from this phase [KNOWN]:** Rust implementation, tests, design and
  validation artifacts, gateway configuration, and MCP tool-schema changes.

## Incremental Requirements Patch: Gateway-backed MCP search

### USER-REQUEST

- **UR-MCP-GATEWAY-001 [KNOWN]:** Configure `lexonarchivebuilder-mcp` to use
  the operator-provided block proxy previously tested in this session.
- **UR-MCP-GATEWAY-002 [KNOWN]:** Use the selected root block
  `adbc431aed97ab541ce73d65ce735552821c6c31f9434a4864333013a278fa78`.
- **UR-MCP-GATEWAY-003 [INFERRED]:** The MCP runtime must obtain both rooted
  blocks and query embeddings from the gateway rather than a local block store
  or separately configured embedding service.

### Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-MCP-GATEWAY-001 | Add | Define a read-only `gateway-http3` MCP environment profile for gateway block reads and gateway-proxied embeddings | UR-MCP-GATEWAY-001, UR-MCP-GATEWAY-003 |
| CM-MCP-GATEWAY-002 | Add | Define a gateway MCP configuration example targeting the selected root block | UR-MCP-GATEWAY-001, UR-MCP-GATEWAY-002 |
| CM-MCP-GATEWAY-003 | Preserve | Keep MCP tool schemas, format-neutral target preparation, and delegated LexonGraph search semantics unchanged | UR-MCP-GATEWAY-003 |

### Before / After

#### BA-MCP-GATEWAY-001

- **Before [KNOWN]:** `McpConfig.environment` delegates only to the shared
  local, local-redb, local-overlay, production, or production-v2 indexer
  environments. It cannot construct the indexer's read-only HTTP/3 gateway
  block store.
- **After [INFERRED]:** MCP configuration can select a read-only
  `gateway-http3` environment with a gateway DNS authority and use the gateway
  for both block retrieval and query embedding.

#### BA-MCP-GATEWAY-002

- **Before [KNOWN]:** The local MCP sample targets a local summary file and
  local block-store root.
- **After [INFERRED]:** A gateway MCP sample selects the configured root ID
  directly and does not require a local summary file, local block store, or
  separately configured embedding endpoint.

### Requirements

#### RQ-MCP-014 - Gateway HTTP/3 environment profile

`lexonarchivebuilder-mcp` SHALL support a read-only `gateway-http3`
environment profile for `search_chunks`.

- **Block access [KNOWN]:** The profile SHALL construct the shared
  HTTP/3 gateway block-store client from a required gateway DNS authority and
  use it for all root and descendant block reads.
- **Query embedding [KNOWN]:** The profile SHALL obtain query embeddings from
  the gateway's OpenAI-compatible embeddings endpoint through the shared
  gateway embedding-provider path. It SHALL NOT require or accept a separate
  embedding endpoint or embedding API-key environment variable.
- **Search boundary [KNOWN]:** The profile SHALL continue to obtain a logical
  query vector and pass it with the loaded root to
  `lexongraph_search::prepare_target_embedding`; the MCP crate SHALL not
  become aware of the root's physical embedding format.
- **Write boundary [KNOWN]:** The profile is read-only. MCP operations SHALL
  not attempt to write blocks through the gateway.
- **Failure behavior [INFERRED]:** Missing or invalid gateway configuration,
  block-read failures, embedding failures, and upstream target-preparation
  failures SHALL surface through the existing MCP runtime failure path without
  fallback to local storage or an alternate embedding provider.
- **Traceability:** UR-MCP-GATEWAY-001, UR-MCP-GATEWAY-003, RQ-MCP-002,
  RQ-MCP-006, RQ-MCP-011, RQ-MCP-013

#### RQ-MCP-015 - Gateway configuration example

The repository SHALL provide a documented MCP configuration template for an
operator-provided gateway authority and root ID
`adbc431aed97ab541ce73d65ce735552821c6c31f9434a4864333013a278fa78`.

- **Configuration shape [INFERRED]:** The example SHALL select
  `gateway-http3`, require an operator-provided gateway DNS authority, and use
  `index.kind: "root-id"` with the selected root ID.
- **No hardcoded gateway authority [KNOWN]:** The implementation,
  configuration template, and documentation SHALL NOT embed the tested gateway
  authority. Deployment configuration supplies the authority.
- **Deployment boundary [KNOWN]:** The example SHALL be usable by a stdio MCP
  host without Docker network aliases or a local block-store directory.
- **Traceability:** UR-MCP-GATEWAY-001, UR-MCP-GATEWAY-002

### Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | The profile only reads an existing rooted index and embeds search queries |
| Actual search semantics remain owned by LexonGraph | Preserved | Gateway selection changes dependency transport, not search or ranking behavior |
| MCP tool contract | Preserved | `search_chunks` request and response schemas remain unchanged |
| Environment-specific dependencies stay behind stable interfaces | Preserved | Gateway block and embedding providers are selected as one configuration profile |
| Format-neutral rooted search | Preserved | Gateway vectors continue through the existing logical-vector target-preparation path |

### Coverage Notes

- **Covered sources [KNOWN]:**
  - `crates/lexonarchivebuilder-mcp/src/config.rs`, whose shared
    `EnvironmentConfig` currently lacks a gateway profile.
  - `crates/lexonarchivebuilder-mcp/src/runtime.rs`, which obtains both the
    block store and embedding provider through the configured environment.
  - `crates/lexonarchivebuilder-indexer/src/block_store.rs`, which provides
    the existing read-only HTTP/3 gateway block-store implementation.
  - `crates/lexonarchivebuilder-indexer/src/embedding.rs`, which provides the
    existing gateway embedding-provider implementation.
- **Excluded from this phase [KNOWN]:** Rust implementation, tests, design and
  validation artifacts, and README/config-example changes.
