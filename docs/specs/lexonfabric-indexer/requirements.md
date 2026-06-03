# Indexer Requirements

## Document Status

- **Phase:** Phase 4 - User Review of Specifications
- **Status:** Approved specification baseline for the implemented MVP scope
- **Scope:** LexonFabric indexer integration boundary and first in-repo MVP slice

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
- **UR-10 [KNOWN]:** Implement the minimal viable product of the `lexonfabric-indexer` feature using `docs/specs/lexonfabric-indexer/*` as the source of truth.
- **UR-11 [KNOWN]:** The first MVP implementation must support both initial content classes already named by the spec: mailboxes and document collections.
- **UR-12 [KNOWN]:** The first MVP implementation only needs an executable local/testing profile using local filesystem storage and a local embedding service.
- **UR-13 [KNOWN]:** Production storage and embedding integrations should remain pluggable through stable trait and configuration boundaries, but do not need an executable production realization in the first MVP.
- **UR-14 [KNOWN]:** Local/testing should be deployable as a single Docker Compose unit that brings up the indexer runtime and its local dependencies, including volumes/storage and the embedding engine, for integration-style testing.

## Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-INDEXER-001 | Add | Introduce the first structured requirements artifact for the LexonFabric indexer boundary | UR-1, UR-2 |
| CM-INDEXER-002 | Add | Define LexonFabric as an orchestration and adapter layer around `lexongraph-indexer`, not an indexing engine | UR-3 |
| CM-INDEXER-003 | Add | Define batch-container execution, supported initial content inputs, storage targets, and embedding-provider targets | UR-4, UR-5, UR-6, UR-7 |
| CM-INDEXER-004 | Add | Capture invariants around delegated idempotence, immutable blocks, and separation from MCP search-serving behavior | UR-8 |
| CM-INDEXER-005 | Revise | Narrow the first in-repo MVP realization to an end-to-end local/testing profile while preserving production extensibility boundaries | UR-10, UR-12, UR-13 |
| CM-INDEXER-006 | Revise | Require the first MVP implementation to cover both mailbox and document-collection batch items | UR-10, UR-11 |
| CM-INDEXER-007 | Add | Require Docker Compose-based local dependency orchestration for repeatable integration testing of the batch container | UR-12, UR-14 |

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

### BA-INDEXER-005

- **Before [KNOWN]:** The requirements described both local/testing and production environment targets, but did not identify which subset must be executable in the first in-repo MVP.
- **After [KNOWN]:** The requirements define the first MVP as an end-to-end local/testing realization while preserving production storage and embedding integrations as stable extension seams.

### BA-INDEXER-006

- **Before [KNOWN]:** The requirements identified mailbox and document-collection inputs, but did not state whether the first MVP could implement only one of them.
- **After [KNOWN]:** The requirements now state that the first MVP must support both mailbox and document-collection items through the same collection-oriented batch contract.

### BA-INDEXER-007

- **Before [KNOWN]:** The requirements described Linux Docker batch execution, but did not require a repository-local composition layer for exercising dependencies together during testing.
- **After [KNOWN]:** The requirements now require a Docker Compose deployment shape for the local/testing profile so the batch runtime and its local dependencies can be brought up as one integration test unit.

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
- **MVP realization [KNOWN]:** The first in-repo implementation must support both initial item classes rather than deferring either one to a later increment.
- **Extensibility [INFERRED]:** The accepted collection model should permit future content types without redefining the external batch contract.
- **Traceability:** UR-5, UR-11

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

- **Architectural target storage profiles [KNOWN]:**
  - local filesystem for local/testing operation
  - Azure Blob Storage for production operation
- **MVP realization [KNOWN]:** The first in-repo implementation must execute end-to-end against the local filesystem profile. Azure Blob Storage remains a required future profile boundary, but not a required executable realization for the first MVP.
- **Traceability:** UR-3, UR-6, UR-9, UR-12, UR-13

#### RQ-INDEXER-006 - Embedding provider integration

LexonFabric SHALL obtain embeddings through a provider that satisfies `lexongraph_embeddings_trait::EmbeddingProvider` and is reached through an OpenAPI-compatible HTTP embedding interface.

- **Architectural target embedding profiles [KNOWN]:**
  - local STAPI-compatible embedding service
  - Azure OpenAI embedding model
- **Constraint [KNOWN]:** Provider selection varies by environment and must not require changes to the collection-oriented batch contract.
- **MVP realization [KNOWN]:** The first in-repo implementation must execute end-to-end against a local embedding service. Azure OpenAI remains a required future profile boundary, but not a required executable realization for the first MVP.
- **Integration note [KNOWN]:** The delegated indexer consumes `EmbeddingInput` and `EmbeddingSpec` through the shared embeddings trait boundary.
- **Traceability:** UR-7, UR-9, UR-12, UR-13

#### RQ-INDEXER-007 - Environment-specific adapter selection

LexonFabric SHALL select storage and embedding integrations according to environment without changing the delegated indexing contract or the batch input contract.

- **Local/testing [KNOWN]:** local filesystem + local embedding service
- **Production [KNOWN]:** Azure Blob Storage + Azure OpenAI
- **MVP realization [KNOWN]:** Only the local/testing profile is required to be executable in the first MVP. The production profile must remain representable through the same adapter boundary and configuration model without requiring Azure-specific execution support in this increment.
- **Traceability:** UR-6, UR-7, UR-12, UR-13

#### RQ-INDEXER-008 - Idempotent reruns

LexonFabric SHALL preserve idempotent rerun behavior for repeated indexing of the same source content.

- **Mechanism owner [KNOWN]:** The underlying LexonGraph API owns batch and recovery semantics.
- **Required property [KNOWN]:** Produced blocks are immutable and identified by hash, so reruns must not create distinct logical outputs for unchanged content.
- **Traceability:** UR-8

#### RQ-INDEXER-008A - Local integration composition

LexonFabric SHALL provide a Docker Compose topology for the local/testing profile that deploys the batch container and its required local dependencies as one integration-testable unit.

- **Included local dependencies [KNOWN]:** local storage mounts/volumes and the local embedding service
- **Constraint [KNOWN]:** The Compose topology must preserve the Linux batch-container runtime shape rather than introducing a separate long-lived control-plane service for indexing.
- **Traceability:** UR-4, UR-12, UR-14

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

- **MVP implication [KNOWN]:** The first MVP may ship only the local/testing realizations, but it must preserve storage and embedding seams so production adapters can be added without changing the batch contract or content-model abstractions.
- **Traceability:** UR-3, UR-6, UR-7, UR-13

## Out of Scope

- Defining indexing algorithms internal to `lexongraph-indexer`
- Redefining the public contracts of `ContentResolver<R>`, `BlockStore`, or `EmbeddingProvider`
- Defining MCP query semantics or search ranking behavior
- Re-specifying LexonGraph API batch recovery internals
- Finalizing exact production deployment workflow beyond the batch-container shape already described
- Requiring executable Azure production adapters in the first MVP increment

## Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | Requirements explicitly constrain scope to indexing-time orchestration and integrations |
| Environment-specific storage and embedding behavior stays behind stable interfaces | Preserved | MVP scope is narrowed to local/testing execution while production remains preserved behind the same adapter boundaries |
| Architecture remains extensible to future content types | Preserved | Collection-oriented input still covers both mailbox and document collections while keeping future types behind stable boundaries |
| Idempotence and recoverability stay aligned with underlying immutable block semantics | Preserved | Requirements adopt LexonGraph API ownership instead of duplicating conflicting logic in LexonFabric |
| Local development remains self-contained and batch-oriented | Preserved | Docker Compose is constrained to compose local dependencies around the batch container rather than changing the runtime model |

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
  - user clarification messages in this session specifying both mailbox and document-collection MVP coverage
  - user clarification messages in this session specifying local-only executable MVP scope with production left pluggable
  - user clarification messages in this session specifying Docker Compose-based local dependency orchestration
- **Excluded for now [KNOWN]:**
  - Detailed Rust implementation file paths, crate manifests, Docker assets, and test artifacts, because this requirements document captures the semantic contract and leaves implementation realization to downstream design, validation, and code-review artifacts
