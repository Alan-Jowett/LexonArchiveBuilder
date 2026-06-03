# LexonFabric Indexer Design

## Status

Draft specification patch derived from `docs/specs/lexonfabric-indexer/requirements.md`.

## Scope

This document specifies the LexonFabric-owned design for realizing the approved
indexer requirements.

This document is layered on top of:

- `docs/specs/lexonfabric-indexer/requirements.md`
- `README.md`
- external LexonGraph repository source (not vendored in LexonFabric):
  `crates/lexongraph-indexer/src/lib.rs`
- external LexonGraph repository source (not vendored in LexonFabric):
  `crates/lexongraph-block-store/src/lib.rs`
- external LexonGraph repository source (not vendored in LexonFabric):
  `crates/lexongraph-embeddings-trait/src/lib.rs`

This document does not redefine the indexing protocol, block identity rules,
the `BlockStore` contract, or the `EmbeddingProvider` contract. Those remain
owned by LexonGraph and its subordinate crates.

## Impact Map

### Directly affected artifacts

- `docs/specs/lexonfabric-indexer/requirements.md`
- `docs/specs/lexonfabric-indexer/design.md`
- `docs/specs/lexonfabric-indexer/validation.md`

### Indirectly affected artifacts

- `README.md`, which already describes the same local-versus-production split at
  the architecture level
- future Rust crates, configuration, and test artifacts not yet present in this
  repository

### Unaffected artifacts

- MCP server search semantics
- LexonGraph indexing internals
- LexonGraph block encoding and block identity contracts

## Design Goals

The LexonFabric indexer design is intended to be:

- an orchestration layer around `lexongraph-indexer`
- explicit about ownership boundaries
- stable across local and production environments
- extensible to future content types
- compatible with a Linux batch-container runtime

## Boundary Design

### DSG-LFI-001 `Delegated indexing boundary`

LexonFabric owns batch orchestration, environment-specific adapter selection,
and application-defined item modeling.

LexonFabric does not own index construction, canonical block generation, or
batch recovery semantics internal to the delegated LexonGraph stack.

**Traces to:** RQ-INDEXER-001, RQ-INDEXER-003, RQ-INDEXER-008,
RQ-INDEXER-010A

### DSG-LFI-002 `Batch runtime shape`

The indexer runtime is a Linux Docker container that executes one batch over a
caller-supplied collection of items.

At the container boundary, the batch contract is collection-oriented rather than
backend-specific so the same invocation shape can be reused for mail archives,
RFC sets, and future content classes.

**Traces to:** RQ-INDEXER-001, RQ-INDEXER-002

### DSG-LFI-003 `Collection item normalization`

LexonFabric models each batch element as an application-owned indexing item that
can be transformed into a `lexongraph_indexer::IndexItem<R>` with:

- application metadata
- a content reference `R`

The content reference stays opaque to the delegated indexer and is interpreted
only by the LexonFabric `ContentResolver<R>` implementation.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-004, RQ-INDEXER-010

## Adapter Design

### DSG-LFI-004 `Content resolution adapter`

LexonFabric provides a concrete `lexongraph_indexer::ContentResolver<R>`
implementation that resolves a collection item's content reference into the
`Content` value consumed by the delegated indexer.

The resolver owns source-specific retrieval logic for initially supported item
classes such as mailboxes and document collections, while preserving one stable
batch contract at the container boundary.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-004, RQ-INDEXER-010

### DSG-LFI-005 `Block storage adapter boundary`

LexonFabric provides concrete `lexongraph_block_store::BlockStore`
implementations or adapters selected by environment.

- local/testing selects a filesystem-backed block store
- production selects an Azure Blob-backed block store

The rest of the LexonFabric indexing flow consumes only the backend-neutral
`BlockStore` contract and does not depend on filesystem paths or Azure-specific
blob layout details.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-007, RQ-INDEXER-010

### DSG-LFI-006 `Embedding provider adapter boundary`

LexonFabric provides environment-selected implementations or adapters that
satisfy `lexongraph_embeddings_trait::EmbeddingProvider`.

- local/testing targets a local STAPI-compatible HTTP embedding service
- production targets an Azure OpenAI embedding endpoint

Provider-specific HTTP request construction, authentication, and endpoint
selection remain behind this adapter boundary and do not alter the batch input
contract or the delegated indexer contract.

**Traces to:** RQ-INDEXER-006, RQ-INDEXER-007, RQ-INDEXER-010

## Environment Design

### DSG-LFI-007 `Environment selection`

LexonFabric selects the storage adapter and embedding provider as a coupled
environment profile:

| Profile | Block storage | Embedding target |
|---|---|---|
| local/testing | local filesystem | local STAPI-compatible service |
| production | Azure Blob Storage | Azure OpenAI |

This selection is configuration-driven and preserves one stable delegated
indexing flow independent of environment.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-006, RQ-INDEXER-007

### DSG-LFI-008 `Local and production parity boundary`

Local/testing and production environments differ only in adapter realization and
provider configuration, not in the container's batch contract, content item
shape, or the delegated `lexongraph-indexer` orchestration contract.

**Traces to:** RQ-INDEXER-007, RQ-INDEXER-010

## Invariant Design

### DSG-LFI-009 `Search-serving separation`

The indexer package remains separate from MCP server search semantics. No design
element in this package changes retrieval contracts, query semantics, or search
ranking behavior.

**Traces to:** RQ-INDEXER-009

### DSG-LFI-010 `Idempotence ownership`

LexonFabric relies on the underlying immutable, hash-addressed block model for
rerun idempotence and does not introduce repository-local batch-recovery or
duplicate-suppression semantics that could conflict with LexonGraph ownership.

**Traces to:** RQ-INDEXER-008, RQ-INDEXER-010A

## Verification Realization

### DSG-LFI-011 `Repository verification scope`

LexonFabric-owned verification artifacts validate:

- correct delegation to `lexongraph-indexer`
- correct selection and use of content-resolution, block-store, and
  embedding-provider adapters
- preservation of stable batch contracts across environments

LexonFabric-owned verification artifacts do not attempt to revalidate
LexonGraph's own block-store or embedding-trait contracts beyond proving that
LexonFabric consumes them correctly.

**Traces to:** RQ-INDEXER-010A, RQ-INDEXER-010
