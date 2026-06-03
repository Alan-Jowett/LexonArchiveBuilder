# Indexer Requirements

## Document Status

- **Phase:** Phase 1 - Requirements Discovery
- **Status:** Approved for specification propagation
- **Scope:** LexonFabric indexer integration boundary

## USER-REQUEST

- **UR-1 [KNOWN]:** Create specs under `docs/specs/lexonfabric-indexer/{requirements|design|validation}.md`.
- **UR-2 [KNOWN]:** The first requirement spec is for the indexer.
- **UR-3 [KNOWN]:** LexonFabric does not perform indexing itself. It delegates indexing and index creation to the `lexongraph-indexer` crate and provides concrete implementations for content resolution and block storage integration.
- **UR-4 [KNOWN]:** The indexer runs as a Linux Docker container in batch mode.
- **UR-5 [KNOWN]:** A batch accepts a collection of items to index, such as mailboxes and RFCs.
- **UR-6 [KNOWN]:** The resulting blocks are stored either on the local filesystem or in Azure Blob Storage.
- **UR-7 [KNOWN]:** Embeddings are obtained through an OpenAPI-compatible HTTP embedding API, targeting either a local STAPI container or Azure OpenAI.
- **UR-8 [KNOWN]:** Batch and recovery behavior are owned by the LexonGraph API itself; produced blocks are immutable and hash-addressed, so reruns are idempotent.
- **UR-9 [KNOWN]:** The delegated indexer crate defines `ContentResolver<R>` and consumes `BlockStore` from `lexongraph-block-store` plus `EmbeddingProvider` from `lexongraph-embeddings-trait`.

## Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-INDEXER-001 | Add | Introduce the first structured requirements artifact for the LexonFabric indexer boundary | UR-1, UR-2 |
| CM-INDEXER-002 | Add | Define LexonFabric as an orchestration and adapter layer around `lexongraph-indexer`, not an indexing engine | UR-3 |
| CM-INDEXER-003 | Add | Define batch-container execution, supported initial content inputs, storage targets, and embedding-provider targets | UR-4, UR-5, UR-6, UR-7 |
| CM-INDEXER-004 | Add | Capture invariants around delegated idempotence, immutable blocks, and separation from MCP search-serving behavior | UR-8 |

## Before / After

### BA-INDEXER-001

- **Before [KNOWN]:** The repository had no structured requirements artifact for indexer behavior.
- **After [KNOWN]:** The repository has an explicit requirements baseline for the LexonFabric indexer boundary in `docs/specs/lexonfabric-indexer/requirements.md`.

### BA-INDEXER-002

- **Before [KNOWN]:** `README.md` described LexonFabric as an indexer at a high level, but did not distinguish whether indexing logic lived in-repo or was delegated externally.
- **After [KNOWN]:** The requirements define that LexonFabric delegates indexing and index creation to `lexongraph-indexer` and is responsible for supplying environment-specific integrations around that crate.

### BA-INDEXER-003

- **Before [KNOWN]:** Local-versus-production behavior was described only at the architecture level.
- **After [KNOWN]:** The requirements define initial indexer targets for local filesystem plus STAPI and for Azure Blob Storage plus Azure OpenAI, while keeping those choices behind stable integration boundaries.

### BA-INDEXER-004

- **Before [KNOWN]:** Idempotence and recovery ownership were not captured in repository requirements.
- **After [KNOWN]:** The requirements define rerun idempotence as inherited from LexonGraph API behavior and immutable hash-addressed blocks, rather than re-specifying batch recovery logic inside LexonFabric.

## Requirements

### Functional Requirements

#### RQ-INDEXER-001 - Batch entrypoint

LexonFabric SHALL provide an indexer runtime that executes as a Linux Docker container in batch mode.

- **Rationale [KNOWN]:** This matches the intended local and production execution shape from `README.md` and the user request.
- **Traceability:** UR-2, UR-4

#### RQ-INDEXER-002 - Collection-oriented input

The batch indexer SHALL accept a collection of items to index rather than a single hard-coded content source.

- **Initial supported item classes [KNOWN]:**
  - mailboxes / mail archives
  - document collections such as RFCs
- **Extensibility [INFERRED]:** The accepted collection model should permit future content types without redefining the external batch contract.
- **Traceability:** UR-5

#### RQ-INDEXER-003 - Delegated indexing engine

LexonFabric SHALL delegate indexing and index creation to the `lexongraph-indexer` crate.

- **Non-goal [KNOWN]:** LexonFabric does not define or implement its own indexing algorithm in this scope.
- **Traceability:** UR-3

#### RQ-INDEXER-004 - Content resolution integration

LexonFabric SHALL provide a concrete implementation of `lexongraph_indexer::ContentResolver<R>`.

- **Constraint [KNOWN]:** This integration is responsible for resolving requested source content for the batch's collection items.
- **Traceability:** UR-3, UR-5, UR-9

#### RQ-INDEXER-005 - Block storage integration

LexonFabric SHALL provide a concrete implementation of `lexongraph_block_store::BlockStore` used to persist blocks produced through the delegated indexing flow.

- **Required initial storage targets [KNOWN]:**
  - local filesystem for local/testing operation
  - Azure Blob Storage for production operation
- **Traceability:** UR-3, UR-6, UR-9

#### RQ-INDEXER-006 - Embedding provider integration

LexonFabric SHALL obtain embeddings through a provider that satisfies `lexongraph_embeddings_trait::EmbeddingProvider` and is reached through an OpenAPI-compatible HTTP embedding interface.

- **Required initial embedding targets [KNOWN]:**
  - local STAPI-compatible embedding service
  - Azure OpenAI embedding model
- **Constraint [KNOWN]:** Provider selection varies by environment and must not require changes to the collection-oriented batch contract.
- **Integration note [KNOWN]:** The delegated indexer consumes `EmbeddingInput` and `EmbeddingSpec` through the shared embeddings trait boundary.
- **Traceability:** UR-7, UR-9

#### RQ-INDEXER-007 - Environment-specific adapter selection

LexonFabric SHALL select storage and embedding integrations according to environment without changing the delegated indexing contract or the batch input contract.

- **Local/testing [KNOWN]:** local filesystem + local embedding service
- **Production [KNOWN]:** Azure Blob Storage + Azure OpenAI
- **Traceability:** UR-6, UR-7

#### RQ-INDEXER-008 - Idempotent reruns

LexonFabric SHALL preserve idempotent rerun behavior for repeated indexing of the same source content.

- **Mechanism owner [KNOWN]:** The underlying LexonGraph API owns batch and recovery semantics.
- **Required property [KNOWN]:** Produced blocks are immutable and identified by hash, so reruns must not create distinct logical outputs for unchanged content.
- **Traceability:** UR-8

### Boundary and Invariant Requirements

#### RQ-INDEXER-009 - Search-serving separation

The indexer requirements SHALL remain limited to indexing-time orchestration and adapter responsibilities and SHALL NOT redefine MCP search-serving behavior.

- **Rationale [INFERRED]:** Preserves the repository invariant that indexing remains separate from the MCP server surface.
- **Traceability:** UR-2, README.md

#### RQ-INDEXER-010A - Subordinate external contracts

LexonFabric SHALL remain subordinate to the public contracts owned by `lexongraph-indexer`, `lexongraph-block-store`, and `lexongraph-embeddings-trait` and SHALL NOT redefine their index-construction, block-identity, or embedding-contract semantics within this repository.

- **Rationale [KNOWN]:** Those semantics are already owned by the upstream LexonGraph crates and specifications.
- **Traceability:** UR-3, UR-8, UR-9

#### RQ-INDEXER-010 - Stable abstraction boundary

LexonFabric SHALL keep content resolution, block storage, and embedding-provider variation behind stable integration boundaries so future content types and provider swaps do not require redefinition of the core indexing contract.

- **Traceability:** UR-3, UR-6, UR-7

## Out of Scope

- Defining indexing algorithms internal to `lexongraph-indexer`
- Redefining the public contracts of `ContentResolver<R>`, `BlockStore`, or `EmbeddingProvider`
- Defining MCP query semantics or search ranking behavior
- Re-specifying LexonGraph API batch recovery internals
- Finalizing exact production deployment workflow beyond the batch-container shape already described

## Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | Requirements explicitly constrain scope to indexing-time orchestration and integrations |
| Environment-specific storage and embedding behavior stays behind stable interfaces | Preserved | Requirements capture provider selection without changing the batch contract |
| Architecture remains extensible to future content types | Preserved | Collection-oriented input and stable boundaries avoid locking the design to only email or RFCs |
| Idempotence and recoverability stay aligned with underlying immutable block semantics | Preserved | Requirements adopt LexonGraph API ownership instead of duplicating conflicting logic in LexonFabric |

## Coverage Notes

- **Covered sources [KNOWN]:**
  - `README.md:18-27`
  - `README.md:42-49`
  - `README.md:51-59`
  - `README.md:61-80`
  - external LexonGraph repository source (not vendored in LexonFabric):
    `crates/lexongraph-indexer/src/lib.rs:24-26`
  - external LexonGraph repository source (not vendored in LexonFabric):
    `crates/lexongraph-indexer/src/lib.rs:29-37`
  - external LexonGraph repository source (not vendored in LexonFabric):
    `crates/lexongraph-indexer/src/lib.rs:104-107`
  - external LexonGraph repository source (not vendored in LexonFabric):
    `crates/lexongraph-block-store/src/lib.rs:28-32`
  - external LexonGraph repository source (not vendored in LexonFabric):
    `crates/lexongraph-embeddings-trait/src/lib.rs:20-33`
  - user clarification messages in this session
- **Excluded for now [KNOWN]:**
  - Rust implementation file paths, crate manifests, and test artifacts within this repository, because no such repository files exist yet
