# Indexer Requirements

## Document Status

- **Phase:** Phase 2 - Specification Changes
- **Status:** Approved requirements patch being propagated into design and validation
- **Scope:** LexonFabric indexer integration boundary plus incremental email-artifact, chunk-indexing, and local block-store interoperability evolution

## USER-REQUEST

- **UR-1 [KNOWN]:** Create specs under `docs/specs/lexonfabric-indexer/{requirements|design|validation}.md`.
- **UR-2 [KNOWN]:** The first requirement spec is for the indexer.
- **UR-3 [KNOWN]:** LexonFabric does not perform indexing itself. It delegates indexing and index creation to the `lexongraph-indexer` crate and provides concrete implementations for content resolution and block storage integration.
- **UR-4 [KNOWN]:** The indexer runs as a Linux Docker container in batch mode.
- **UR-5 [KNOWN]:** A batch accepts a collection of items to index, such as mailboxes and RFCs.
- **UR-6 [KNOWN]:** The resulting blocks are stored either on the local filesystem or in Azure Blob Storage.
- **UR-7 [KNOWN]:** Embeddings are obtained through an OpenAI-compatible HTTP embedding API, targeting either a local STAPI container or Azure OpenAI.
- **UR-8 [KNOWN]:** Batch and recovery behavior are owned by the LexonGraph API itself; produced blocks are immutable and hash-addressed, so reruns are idempotent.
- **UR-9 [KNOWN]:** The delegated indexer crate defines `ContentResolver<R>` and consumes `BlockStore` from `lexongraph-block-store` plus `EmbeddingProvider` from `lexongraph-embeddings-trait`.
- **UR-10 [KNOWN]:** Implement the minimal viable product of the `lexonfabric-indexer` feature using `docs/specs/lexonfabric-indexer/*` as the source of truth.
- **UR-11 [KNOWN]:** The first MVP implementation must support both initial content classes already named by the spec: mailboxes and document collections.
- **UR-12 [KNOWN]:** The first MVP implementation only needs an executable local/testing profile using local filesystem storage and a local embedding service.
- **UR-13 [KNOWN]:** Production storage and embedding integrations should remain pluggable through stable trait and configuration boundaries, but do not need an executable production realization in the first MVP.
- **UR-14 [KNOWN]:** Local/testing should be deployable as a single Docker Compose unit that brings up the indexer runtime and its local dependencies, including volumes/storage and the embedding engine, for integration-style testing.
- **UR-15 [KNOWN]:** Email indexing should stop embedding whole mailbox files and instead extract and normalize email messages, derive chunk-level retrieval units, and embed those chunks.
- **UR-16 [KNOWN]:** The canonical email artifact identity should be based on the normalized email artifact rather than the raw mailbox bytes.
- **UR-17 [KNOWN]:** Indexed email chunks should carry only minimal search-serving metadata plus a reference to the normalized email artifact so clients can use the chunk directly or retrieve the full normalized email.
- **UR-18 [KNOWN]:** LexonFabric should reuse its hash-addressed storage approach for normalized email artifacts and, when useful, raw mailbox provenance artifacts instead of forcing clients to reconstruct emails from mailbox blobs.
- **UR-19 [KNOWN]:** This change applies to email ingestion now and must not preclude future document-specific chunking and metadata handling.
- **UR-20 [KNOWN]:** Email normalization should derive a meaningful message body for embedding while best-effort excluding common non-semantic content when practical.
- **UR-21 [KNOWN]:** Indexed email chunks should duplicate enough message metadata to satisfy the common retrieval/rendering path without always dereferencing the normalized email artifact.
- **UR-22 [KNOWN]:** Normalized email artifacts and mailbox provenance artifacts should reuse the same environment-selected `BlockStore` abstraction family as indexed LexonGraph blocks rather than introducing a second storage abstraction stack.
- **UR-23 [KNOWN]:** Email provenance should be chainable from indexed chunk to normalized email artifact to source mailbox artifact.
- **UR-24 [KNOWN]:** The first email chunking baseline may be sentence-aware and implementation-simple, but the indexing design must preserve a seam for future tokenizer-driven or more semantic chunking strategies.
- **UR-25 [KNOWN]:** Mailbox artifacts should be retained as first-class provenance artifacts so LexonFabric can support re-normalization, re-chunking, and re-ingestion from the original source material.
- **UR-26 [KNOWN]:** Remove the repository-local `LocalFilesystemBlockStore` and replace it with the LexonGraph `lexongraph-block-store-fs` crate for the local/testing filesystem-backed block-store realization.
- **UR-27 [KNOWN]:** The current repository-local filesystem store breaks `lexongraph-block-inspect` interoperability because it uses a different on-disk naming scheme than LexonGraph's filesystem block-store tools expect.
- **UR-28 [KNOWN]:** It is acceptable for this change to require a fresh or rebuilt local block store; continued read compatibility with blocks written by the superseded custom local layout is not required.
- **UR-29 [KNOWN]:** Mailbox batch inputs must accept mailbox source files ending in `.mail` as well as `.mbox`.
- **UR-30 [KNOWN]:** For this increment, mailbox source compatibility should be limited to exactly `.mail` and `.mbox` rather than broadened to arbitrary mailbox archive extensions.

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
| CM-INDEXER-008 | Revise | Refine email ingestion so mailbox inputs expand into normalized email artifacts and chunk-level embedding units instead of whole-mailbox embeddings | UR-15, UR-19 |
| CM-INDEXER-009 | Add | Require normalized email artifacts to be hash-addressed, retrievable by reference from indexed chunks, and anchored in LexonFabric-owned storage rather than client-side mailbox parsing | UR-16, UR-17, UR-18 |
| CM-INDEXER-010 | Add | Define email-body normalization, common-case chunk metadata duplication, shared storage abstractions, and chained provenance for email indexing artifacts | UR-20, UR-21, UR-22, UR-23 |
| CM-INDEXER-011 | Add | Establish a simple sentence-aware email chunking baseline while requiring retained mailbox provenance and future chunking extensibility | UR-24, UR-25 |
| CM-INDEXER-012 | Revise | Require the local/testing filesystem-backed block-store realization to stay interoperable with LexonGraph filesystem store tooling and naming/layout expectations | UR-26, UR-27 |
| CM-INDEXER-013 | Add | Explicitly allow the local/testing filesystem store transition to require a fresh or rebuilt local store rather than preserving reads from the superseded custom layout | UR-28 |
| CM-INDEXER-014 | Revise | Expand mailbox source compatibility so mailbox batch items may reference `.mail` or `.mbox` files without widening the first increment to arbitrary archive extensions | UR-29, UR-30 |

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

### BA-INDEXER-008

- **Before [KNOWN]:** A mailbox batch item was understood as one embedding unit, which implied embedding the entire `.mbox` body as one vector through the delegated indexer contract.
- **After [KNOWN]:** The requirements define mailbox inputs as ingestion sources that LexonFabric expands into normalized email artifacts and chunk-level embedding units before delegating indexing to `lexongraph-indexer`.

### BA-INDEXER-009

- **Before [KNOWN]:** The requirements did not define a canonical normalized email artifact or a stable retrieval reference from indexed chunks back to full-message content.
- **After [KNOWN]:** The requirements define normalized email artifacts as hash-addressed retrieval targets referenced from indexed chunks, allowing clients to use chunk text directly or follow the artifact reference to the full normalized email without reparsing mailbox blobs.

### BA-INDEXER-010

- **Before [KNOWN]:** The requirements did not define how much email normalization should shape the embedded body, how much metadata should be duplicated onto chunk hits, whether email artifacts should reuse the repository storage abstraction, or how provenance should chain back to the mailbox source.
- **After [KNOWN]:** The requirements define best-effort email-body normalization for embedding, enough duplicated chunk metadata for the common retrieval path, reuse of the environment-selected `BlockStore` abstraction family for email artifacts, and explicit chained provenance from chunk to normalized email artifact to mailbox artifact.

### BA-INDEXER-011

- **Before [KNOWN]:** The requirements did not define whether mailbox provenance retention was mandatory or whether the first email chunking strategy should stay simple while preserving room for more semantic chunking later.
- **After [KNOWN]:** The requirements make mailbox artifact retention mandatory for reprocessing scenarios and define the first email chunking strategy as a simple sentence-aware baseline that preserves a seam for future tokenizer-driven or more semantic chunking policies.

### BA-INDEXER-012

- **Before [KNOWN]:** The requirements allowed a repository-local filesystem `BlockStore` realization without constraining its on-disk naming or layout to remain interoperable with LexonGraph's filesystem inspection tooling.
- **After [KNOWN]:** The requirements now bind the local/testing filesystem-backed block-store realization to LexonGraph's filesystem store layout expectations so `lexongraph-block-inspect` and related filesystem tooling can inspect LexonFabric-produced local stores without repository-specific translation.

### BA-INDEXER-013

- **Before [KNOWN]:** The requirements did not state whether the local filesystem block-store transition had to preserve reads from the superseded custom layout.
- **After [KNOWN]:** The requirements now allow this interoperability fix to require a fresh or rebuilt local store, avoiding a hidden backward-compatibility obligation for the old repository-local layout.

### BA-INDEXER-014

- **Before [KNOWN]:** Mailbox batch-item compatibility implicitly assumed `.mbox` mailbox source files and did not define whether `.mail` files were valid mailbox inputs.
- **After [KNOWN]:** Mailbox batch-item compatibility explicitly accepts source files ending in `.mail` or `.mbox`, while broader mailbox archive extension support remains out of scope for this increment.

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
- **Email ingestion refinement [KNOWN]:** A mailbox item remains a valid batch input, but it is an ingestion source rather than the final embedding unit; LexonFabric expands mailbox content into normalized email artifacts and chunk-level index items before delegated embedding.
- **Mailbox source compatibility [KNOWN]:** In this increment, mailbox batch items may reference source files ending in `.mail` or `.mbox`.
- **Document scope boundary [KNOWN]:** Document collections remain valid batch inputs in this increment, but this change does not require document chunking to match email handling. Future document-specific chunking and metadata handling must remain possible through the same collection-oriented contract.
- **Extensibility [INFERRED]:** The accepted collection model should permit future content types without redefining the external batch contract.
- **Traceability:** UR-5, UR-11, UR-15, UR-19, UR-29, UR-30

#### RQ-INDEXER-003 - Delegated indexing engine

LexonFabric SHALL delegate indexing and index creation to the `lexongraph-indexer` crate.

- **Non-goal [KNOWN]:** LexonFabric does not define or implement its own indexing algorithm in this scope.
- **Traceability:** UR-3

#### RQ-INDEXER-004 - Content resolution integration

LexonFabric SHALL provide a concrete implementation of `lexongraph_indexer::ContentResolver<R>`.

- **Constraint [KNOWN]:** This integration is responsible for resolving requested source content for the batch's collection items.
- **Email refinement [KNOWN]:** For mailbox-driven email indexing, LexonFabric-owned preprocessing may materialize additional logical items such as normalized emails and chunks before the delegated resolver hands final embedding content to `lexongraph-indexer`.
- **Traceability:** UR-3, UR-5, UR-9, UR-15

#### RQ-INDEXER-004A - Normalized email artifact derivation

LexonFabric SHALL extract and normalize individual email messages from mailbox inputs before delegated indexing of email content.

- **Required result [KNOWN]:** The normalization step produces a canonical email artifact suitable for full-message retrieval and for derivation of chunk-level embedding units.
- **Identity rule [KNOWN]:** The canonical identity of the normalized email artifact is based on the normalized artifact content rather than the raw mailbox bytes.
- **Mailbox source compatibility [KNOWN]:** The normalization step SHALL accept mailbox source files ending in `.mail` or `.mbox` and SHALL NOT require broader mailbox extension support in this increment.
- **Body normalization rule [KNOWN]:** The normalization step derives a meaningful email body for embedding while best-effort excluding common non-semantic content when practical.
- **Boundary [KNOWN]:** This requirement applies to email ingestion in this increment and does not require the same normalization shape for document collections.
- **Traceability:** UR-15, UR-16, UR-19, UR-20, UR-29, UR-30

#### RQ-INDEXER-004B - Chunk-level email embedding units

LexonFabric SHALL embed email-derived chunk content rather than whole mailbox files.

- **Required property [KNOWN]:** Each delegated email indexing item must represent a chunk-sized retrieval unit derived from a normalized email artifact.
- **Baseline policy [KNOWN]:** The first email chunking realization may use a sentence-aware baseline strategy, provided the surrounding design preserves room for future tokenizer-driven or more semantic chunking policies.
- **Non-goal [KNOWN]:** This requirement does not redefine LexonGraph's embedding contract or require LexonGraph itself to implement chunking.
- **Traceability:** UR-15, UR-19, UR-24

#### RQ-INDEXER-004C - Chunk-to-email provenance

LexonFabric SHALL preserve a stable reference from each indexed email chunk back to its normalized email artifact.

- **Required property [KNOWN]:** Indexed email chunks must carry enough provenance metadata to support full-message retrieval without requiring clients to reparse raw mailbox blobs.
- **Metadata discipline [KNOWN]:** Search-serving metadata duplicated onto the indexed chunk should remain lean, but it must be sufficient for the common retrieval/rendering path without always dereferencing the normalized email artifact.
- **Traceability:** UR-17, UR-18, UR-21

#### RQ-INDEXER-004D - Chained email provenance

LexonFabric SHALL preserve chained provenance from each indexed email chunk to its normalized email artifact and from that normalized email artifact to its source mailbox artifact.

- **Required property [KNOWN]:** The provenance chain must allow retrieval flows to move from a chunk hit to the full normalized email and then, when needed, to the mailbox-level source artifact.
- **Boundary [KNOWN]:** The provenance chain does not require clients to parse the mailbox artifact for ordinary retrieval.
- **Traceability:** UR-18, UR-23

#### RQ-INDEXER-004E - Stable chunk locator

LexonFabric SHALL assign each delegated email chunk item a stable chunk locator
that makes it possible to determine which chunk is being processed or returned.

- **Required property [KNOWN]:** The chunk locator must be derivable from the
  normalized email artifact reference plus chunk-local identity such as ordinal
  position and remain stable under a stable normalization and chunking policy.
- **Integration boundary [KNOWN]:** Because `lexongraph-indexer` accepts
  `metadata` plus an opaque `content_ref` rather than a first-class item-name
  field, LexonFabric owns how this chunk locator is represented.
- **Traceability:** UR-17, UR-23

#### RQ-INDEXER-005 - Block storage integration

LexonFabric SHALL provide a concrete implementation of `lexongraph_block_store::BlockStore` used to persist blocks produced through the delegated indexing flow.

- **Architectural target storage profiles [KNOWN]:**
  - local filesystem for local/testing operation
  - Azure Blob Storage for production operation
- **MVP realization [KNOWN]:** The first in-repo implementation must execute end-to-end against the local filesystem profile. Azure Blob Storage remains a required future profile boundary, but not a required executable realization for the first MVP.
- **Local filesystem interoperability [KNOWN]:** The local/testing filesystem-backed realization SHALL use the LexonGraph-owned filesystem block-store contract, including its on-disk naming and layout scheme, so LexonGraph filesystem tooling such as `lexongraph-block-inspect` can operate on LexonFabric-produced local stores.
- **Local implementation target [KNOWN]:** The local/testing filesystem-backed realization SHALL use the upstream `lexongraph-block-store-fs` crate rather than a repository-local filesystem naming scheme.
- **Migration boundary [KNOWN]:** This local filesystem interoperability correction may require a fresh or rebuilt local store; continued read compatibility with blocks written by the superseded custom local layout is not required in this increment.
- **Artifact reuse [KNOWN]:** The same environment-selected `BlockStore` abstraction family SHALL also be used for normalized email artifacts and mailbox provenance artifacts, provided indexing contracts and retrieval references remain explicit.
- **Mailbox retention [KNOWN]:** Mailbox provenance artifacts SHALL be retained so the original source material remains available for re-normalization, re-chunking, and re-ingestion flows.
- **Traceability:** UR-3, UR-6, UR-9, UR-12, UR-13, UR-18, UR-22, UR-25, UR-26, UR-27, UR-28

#### RQ-INDEXER-006 - Embedding provider integration

LexonFabric SHALL obtain embeddings through a provider that satisfies `lexongraph_embeddings_trait::EmbeddingProvider` and is reached through an OpenAI-compatible HTTP embedding interface.

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
- **Email artifact implication [INFERRED]:** Repeated normalization of semantically unchanged email content should resolve to the same normalized email artifact identity and the same derived chunk identities under a stable normalization and chunking policy.
- **Traceability:** UR-8, UR-16

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

#### RQ-INDEXER-010B - Local block-store tooling interoperability

For the local/testing filesystem-backed profile, LexonFabric SHALL remain interoperable with LexonGraph-owned filesystem block-store tooling and SHALL NOT publish blocks using a repository-specific local filename or directory scheme under the same `BlockStore` boundary.

- **Rationale [KNOWN]:** Local block inspection and other filesystem-oriented LexonGraph tooling depend on the upstream filesystem block-store layout contract rather than on an arbitrary repository-local naming scheme.
- **Boundary [KNOWN]:** This requirement constrains only the local/testing filesystem-backed profile and does not redefine Azure Blob layout details for the production profile.
- **Traceability:** UR-26, UR-27

#### RQ-INDEXER-010 - Stable abstraction boundary

LexonFabric SHALL keep content resolution, block storage, and embedding-provider variation behind stable integration boundaries so future content types and provider swaps do not require redefinition of the core indexing contract.

- **MVP implication [KNOWN]:** The first MVP may ship only the local/testing realizations, but it must preserve storage and embedding seams so production adapters can be added without changing the batch contract or content-model abstractions.
- **Email evolution implication [KNOWN]:** Email-specific normalization, artifact storage, and chunk derivation must not preclude future document-specific policies, metadata, or artifact shapes.
- **Traceability:** UR-3, UR-6, UR-7, UR-13, UR-19, UR-22

## Out of Scope

- Defining indexing algorithms internal to `lexongraph-indexer`
- Redefining the public contracts of `ContentResolver<R>`, `BlockStore`, or `EmbeddingProvider`
- Defining MCP query semantics or search ranking behavior
- Re-specifying LexonGraph API batch recovery internals
- Finalizing exact production deployment workflow beyond the batch-container shape already described
- Requiring executable Azure production adapters in the first MVP increment
- Requiring document collections to adopt the same normalization or chunking policy as email in this increment
- Broadening mailbox source compatibility beyond the approved `.mail` and `.mbox` extension set in this increment

## Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | Requirements explicitly constrain scope to indexing-time orchestration and integrations |
| Environment-specific storage and embedding behavior stays behind stable interfaces | Preserved | MVP scope is narrowed to local/testing execution while production remains preserved behind the same adapter boundaries |
| Architecture remains extensible to future content types | Preserved | Collection-oriented input still covers both mailbox and document collections while allowing email-specific normalization and future document-specific handling behind the same contract |
| Idempotence and recoverability stay aligned with underlying immutable block semantics | Preserved | Requirements extend hash-addressed identity expectations to normalized email artifacts without redefining LexonGraph recovery ownership |
| Local development remains self-contained and batch-oriented | Preserved | Docker Compose is constrained to compose local dependencies around the batch container rather than changing the runtime model |
| Clients are not forced to parse raw mailbox blobs for ordinary retrieval | Preserved | Indexed chunks must reference normalized email artifacts so retrieval can stay at chunk level or expand to full normalized email through repository-owned artifacts |
| Storage abstraction count stays bounded across environments | Preserved | Requirements now reuse the environment-selected `BlockStore` abstraction family for indexed blocks, normalized email artifacts, and mailbox provenance artifacts rather than introducing a second storage stack |
| Local filesystem block stores remain interoperable with LexonGraph tooling | Preserved | The local/testing profile is now constrained to LexonGraph's filesystem naming/layout contract so inspection tools can consume repository-produced local stores |

## Coverage Notes

- **Covered sources [KNOWN]:**
  - user request in this session: "make it so this can work with .mail as well as .mbox"
  - user clarification in this session selecting: "Exactly `.mail` and `.mbox`"
  - user request in this session: "remove LocalFilesystemBlockStore and replace with the lexongraph-block-store-fs crate from lexongraph. Our custom store is breaking lexongraph-block-inspect because it uses a totally different naming scheme"
  - user clarification in this session selecting: "Fresh/rebuilt local store is acceptable"
  - `README.md:18-27`
  - `README.md:42-49`
  - `README.md:51-59`
  - `README.md:61-80`
  - `crates/lexonfabric-indexer/src/mailbox.rs:24-31`
  - `crates/lexonfabric-indexer/src/mailbox.rs:157-176`
  - external LexonGraph repository source (not vendored in LexonFabric):
    `crates/lexongraph-indexer/src/lib.rs:24-26`
  - external LexonGraph repository source (not vendored in LexonFabric):
    `crates/lexongraph-indexer/src/lib.rs:29-37`
  - external LexonGraph repository source (not vendored in LexonFabric):
    `crates/lexongraph-indexer/src/lib.rs:104-107`
  - external LexonGraph repository source (not vendored in LexonFabric):
    `crates/lexongraph-block-store/src/lib.rs:28-32`
  - external LexonGraph repository source (not vendored in LexonFabric):
    `crates/lexongraph-block-store-fs/src/lib.rs:89-103`
  - external LexonGraph repository source (not vendored in LexonFabric):
    `crates/lexongraph-block-store-fs/src/lib.rs:165-170`
  - external LexonGraph repository source (not vendored in LexonFabric):
    `crates/lexongraph-embeddings-trait/src/lib.rs:20-33`
  - `crates/lexonfabric-indexer/src/block_store.rs:11-31`
  - `crates/lexonfabric-indexer/src/block_store.rs:56-82`
  - user clarification messages in this session specifying both mailbox and document-collection MVP coverage
  - user clarification messages in this session specifying local-only executable MVP scope with production left pluggable
  - user clarification messages in this session specifying Docker Compose-based local dependency orchestration
  - user clarification message in this session: "Lets do email now, but don't preculde docs. Docs will need different handling as they have different meta-data"
  - user discussion in this session specifying normalized email artifacts, chunk-level email embeddings, minimal indexed metadata, and full-email retrieval by artifact reference
  - user clarification message in this session: "I think we have a reasonable understand of what an email body is. The goal is to have something meaningful for embedding while not containing common data (if possible). May be best effort."
  - user clarification message in this session: "We should duplicate enough so that the 80% case can be satisfied with just the block"
  - user clarification message in this session: "I think they should. We don't really want two azure blob store, s3 store, local filesystem, etc, abstractions."
  - user clarification message in this session: "I think we can chain the provenance. Chunk -> mail block -> mbox."
  - user clarification message in this session: "Can we use the text_splitter crate for now, with the option to use huggingface tokenizer later for semantic chunking? Agree to the rest"
- **Excluded for now [KNOWN]:**
  - Detailed Rust implementation file paths, crate manifests, Docker assets, and test artifacts, because this requirements document captures the semantic contract and leaves implementation realization to downstream design, validation, and code-review artifacts
  - Exact normalized email CBOR schema, exact duplicated chunk metadata list, and the specific chunking library choice, because those belong to downstream design and validation artifacts rather than requirements
