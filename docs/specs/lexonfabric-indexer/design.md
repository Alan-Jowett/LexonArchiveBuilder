# LexonFabric Indexer Design

## Status

Phase 2 specification patch for the approved email-artifact, chunk-level
indexing, local filesystem block-store interoperability, incremental
delegated indexing, and batch-progress observability evolution in
`docs/specs/lexonfabric-indexer/requirements.md`.

## Scope

This document specifies the LexonFabric-owned design for realizing the approved
indexer requirements, including the email-ingestion refinement from `.mail` and
`.mbox` mailbox sources to normalized email artifacts and chunk-level embedding
units plus the local filesystem block-store interoperability correction,
incremental delegated indexing adoption, and batch-progress observability for
the local/testing profile.

This document is layered on top of:

- `docs/specs/lexonfabric-indexer/requirements.md`
- `README.md`
- external LexonGraph repository source (not vendored in LexonFabric):
  `crates/lexongraph-indexer/src/lib.rs`
- external LexonGraph repository source (not vendored in LexonFabric):
  `crates/lexongraph-block-store/src/lib.rs`
- external LexonGraph repository source (not vendored in LexonFabric):
  `crates/lexongraph-block-store-fs/src/lib.rs`
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
- Rust implementation, configuration, and test artifacts that realize the
  approved MVP slice in this repository
- Docker Compose, container, and local test-environment artifacts that realize
  the approved MVP slice

### Unaffected artifacts

- MCP server search semantics
- LexonGraph indexing internals
- LexonGraph block encoding and block identity contracts
- document-specific normalization and chunking policy details beyond preserving
  a future extension seam

## Design Goals

The LexonFabric indexer design is intended to be:

- an orchestration layer around `lexongraph-indexer`
- explicit about ownership boundaries
- stable across local and production environments
- minimal and fully executable in the local/testing profile first
- extensible to future content types
- compatible with a Linux batch-container runtime
- interoperable with LexonGraph-owned local block-store tooling
- incremental-friendly at the delegated indexing boundary
- observable during long-running mailbox batches
- chunk-first for email retrieval while preserving full-message and source
  provenance artifacts

## Boundary Design

### DSG-LFI-001 `Delegated indexing boundary`

LexonFabric owns batch orchestration, environment-specific adapter selection,
and application-defined item modeling.

LexonFabric does not own index construction, canonical block generation, or
batch recovery semantics internal to the delegated LexonGraph stack.

**Traces to:** RQ-INDEXER-001, RQ-INDEXER-003, RQ-INDEXER-008,
RQ-INDEXER-010A

### DSG-LFI-001A `Incremental delegated indexing seam`

LexonFabric realizes delegated indexing as a staged handoff to
`lexongraph-indexer`'s incremental construction APIs rather than as a single
terminal indexing call.

LexonFabric still owns mailbox expansion, artifact storage, and item shaping.
The delegated indexer still owns block construction semantics, canonical block
bytes, and branch-shaping behavior.

The design permits LexonFabric to persist delegated leaf and parent blocks as
work becomes available, while leaving final root determination to the upstream
LexonGraph contract.

**Traces to:** RQ-INDEXER-003A, RQ-INDEXER-008, RQ-INDEXER-010A

### DSG-LFI-002 `Batch runtime shape`

The indexer runtime is a Linux Docker container that executes one batch over a
caller-supplied collection of items.

At the container boundary, the batch contract is collection-oriented rather than
backend-specific so the same invocation shape can be reused for mail archives,
RFC sets, and future content classes.

The first MVP realization covers both mailbox and document-collection items
through this one contract rather than splitting the batch surface by content
class.

For email, a mailbox item is a source container that LexonFabric expands into
stored mailbox and normalized email artifacts plus chunk-sized delegated index
items before invoking `lexongraph-indexer`.

Within that runtime shape, mailbox-driven batches may advance mailbox by
mailbox through the incremental delegated indexing seam rather than waiting for
all delegated work to accumulate behind one final indexer call.

**Traces to:** RQ-INDEXER-001, RQ-INDEXER-002, RQ-INDEXER-003A

### DSG-LFI-002A `Batch progress signaling`

The batch runtime emits progress signals on its normal logging/output surface
as mailbox-driven work advances.

The first design baseline reports at least:

- mailbox-processing start or completion boundaries
- delegated indexing progress after additional chunk items or constructed
  blocks have been advanced

This signaling remains part of the short-lived batch runtime and does not
introduce a separate progress API, control-plane service, or MCP-visible
surface.

**Traces to:** RQ-INDEXER-001, RQ-INDEXER-008B

### DSG-LFI-003 `Collection item normalization`

LexonFabric models each batch element as an application-owned indexing item that
can be transformed into a `lexongraph_indexer::IndexItem<R>` with:

- application metadata
- a content reference `R`

The content reference stays opaque to the delegated indexer and is interpreted
only by the LexonFabric `ContentResolver<R>` implementation.

For document collections, this transformation may remain direct from batch item
to delegated item.

For mailbox inputs, LexonFabric first expands one source item into additional
application-owned artifacts and derived delegated items while preserving one
stable collection-oriented batch contract at the container boundary.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-004, RQ-INDEXER-004A, RQ-INDEXER-010

### DSG-LFI-003A `Email ingestion expansion`

LexonFabric realizes mailbox-driven email indexing as a staged pipeline:

1. persist the mailbox source as a mailbox provenance artifact
2. parse the mailbox into individual email messages
3. normalize each message into a canonical CBOR email artifact
4. derive email-core text from the normalized email artifact
5. split the email core into chunk-sized delegated index items
6. delegate chunk indexing to `lexongraph-indexer`

This expansion is LexonFabric-owned orchestration and does not require changes
to LexonGraph public contracts.

The first design baseline accepts mailbox source files ending in `.mail` or
`.mbox` and treats broader mailbox archive extension support as out of scope
for this increment.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-004, RQ-INDEXER-004A,
RQ-INDEXER-004B, RQ-INDEXER-004D, RQ-INDEXER-005

## Adapter Design

### DSG-LFI-004 `Content resolution adapter`

LexonFabric provides a concrete `lexongraph_indexer::ContentResolver<R>`
implementation that resolves a collection item's content reference into the
`Content` value consumed by the delegated indexer.

The resolver owns source-specific retrieval logic for initially supported item
classes such as mailboxes and document collections, while preserving one stable
batch contract at the container boundary.

For document collections, the resolver may continue to read final delegated
content directly from the document source.

For mailbox-driven email indexing, LexonFabric preprocessing materializes final
chunk items before the resolver hands chunk text to the delegated indexer.

Within that mailbox-driven path, LexonFabric treats source files ending in
`.mail` or `.mbox` as equivalent mailbox containers for normalization and
chunk derivation, without widening the first increment to arbitrary mailbox
archive extensions.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-004, RQ-INDEXER-010

### DSG-LFI-004A `Normalized email artifact shape`

The canonical normalized email artifact is a versioned CBOR structure stored as
a first-class hash-addressed artifact.

The artifact carries:

- normalized body material suitable for deriving embedding chunks
- ordered email header name/value pairs so repeated headers and header order are
  preserved
- extracted convenience fields for common access patterns
- provenance to the source mailbox artifact

The canonical artifact identity is derived from the canonical serialized
artifact bytes rather than from raw mailbox bytes.

**Traces to:** RQ-INDEXER-004A, RQ-INDEXER-005, RQ-INDEXER-008

### DSG-LFI-004B `Email core derivation`

LexonFabric derives an email-core text representation from the normalized email
artifact for retrieval and embedding.

The email core is intended to capture the meaningful message body while
best-effort excluding common non-semantic material when practical. The design
does not require perfect suppression of boilerplate or quoted material in the
first realization, but the normalization policy must be explicit and stable.

**Traces to:** RQ-INDEXER-004A, RQ-INDEXER-004B, RQ-INDEXER-008

### DSG-LFI-004C `Email chunk derivation baseline`

The first email chunking realization uses a sentence-aware baseline over the
derived email core. The baseline may be implemented with the `text_splitter`
crate.

The chunking boundary remains a LexonFabric-owned policy seam so later
realizations may adopt tokenizer-driven or more semantic chunking without
changing the batch input contract, the `BlockStore` contract, or the delegated
LexonGraph contract.

**Traces to:** RQ-INDEXER-004B, RQ-INDEXER-010

### DSG-LFI-004D `Chunk metadata duplication`

Each delegated email chunk item carries:

- the chunk text as primary embedded content
- a stable reference to the normalized email artifact
- enough duplicated message metadata to satisfy the common retrieval/rendering
  path without mandatory dereference of the full email artifact

The duplicated metadata is intentionally lean. The first design baseline keeps
message subject plus recipient or list context on the chunk item, while richer
message structure remains on the normalized email artifact.

**Traces to:** RQ-INDEXER-004C, RQ-INDEXER-010

### DSG-LFI-004E `Chained provenance model`

LexonFabric preserves explicit chained provenance:

- chunk item -> normalized email artifact
- normalized email artifact -> mailbox provenance artifact

This chain allows the common search hit path to remain chunk-first while
preserving full-message expansion and source-level reprocessing.

**Traces to:** RQ-INDEXER-004D, RQ-INDEXER-005

### DSG-LFI-004F `Chunk locator representation`

LexonFabric represents chunk identity as a LexonFabric-owned locator rather
than relying on a first-class upstream item-name field.

The locator is attached through the delegated item's `metadata`, `content_ref`,
or both, and is sufficient to tell which chunk is being processed or returned.
The first design baseline composes this locator from:

- the normalized email artifact reference
- chunk-local identity such as ordinal position
- any needed policy/version discriminator when chunk-identity stability depends
  on the active chunking policy

This representation remains internal to LexonFabric's item model and does not
change the public LexonGraph contract.

**Traces to:** RQ-INDEXER-004E, RQ-INDEXER-010

### DSG-LFI-005 `Block storage adapter boundary`

LexonFabric provides concrete `lexongraph_block_store::BlockStore`
implementations or adapters selected by environment.

- local/testing selects a filesystem-backed block store
- production selects an Azure Blob-backed block store

The same environment-selected `BlockStore` abstraction family is reused for:

- delegated LexonGraph index blocks
- normalized email artifacts
- mailbox provenance artifacts

The rest of the LexonFabric indexing flow consumes only the backend-neutral
`BlockStore` contract and does not depend on filesystem paths or Azure-specific
blob layout details.

For the first MVP, only the local/testing block-store realization must be
executable. The production storage profile remains a preserved adapter seam and
configuration target rather than an implemented runtime path in this increment.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-007, RQ-INDEXER-010

### DSG-LFI-005A `Filesystem block-store interoperability`

For the local/testing profile, LexonFabric realizes the filesystem-backed
`BlockStore` through the upstream `lexongraph-block-store-fs` crate rather than
through a repository-local filesystem naming scheme.

This keeps local block publication interoperable with LexonGraph-owned
filesystem tooling by using the upstream on-disk layout contract, including a
sharded block path derived from the block hash rather than a flat
repository-specific filename mapping.

The rest of the LexonFabric indexing flow still consumes only the abstract
`BlockStore` interface, so this interoperability requirement does not leak
filesystem path details into content resolution, batch orchestration, or future
production adapters.

Because the superseded repository-local filesystem layout is not part of the
approved compatibility boundary for this increment, the local/testing
realization may require a fresh or rebuilt local store instead of preserving
reads from the old layout.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-010B

### DSG-LFI-006 `Embedding provider adapter boundary`

LexonFabric provides environment-selected implementations or adapters that
satisfy `lexongraph_embeddings_trait::EmbeddingProvider`.

- local/testing targets a local STAPI-compatible HTTP embedding service
- production targets an Azure OpenAI embedding endpoint

Provider-specific HTTP request construction, authentication, and endpoint
selection remain behind this adapter boundary and do not alter the batch input
contract or the delegated indexer contract.

For mailbox-driven email indexing, the embedding provider consumes chunk-sized
email-core content rather than full mailbox content.

For the first MVP, only the local/testing embedding realization must be
executable. The production embedding profile remains a preserved adapter seam
and configuration target rather than an implemented runtime path in this
increment.

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
indexing flow independent of environment across indexed blocks, normalized email
artifacts, and mailbox provenance artifacts.

For the approved MVP slice, the local/testing profile is the only profile that
must execute end to end. The production profile remains represented at this
design layer so future adapters can plug into the same orchestration contract.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-006, RQ-INDEXER-007

### DSG-LFI-007A `Local compose topology`

The local/testing profile includes a Docker Compose topology that brings up the
batch container and the local dependencies it needs for integration-style
execution as one unit.

This composition layer may provision mounts, volumes, and the local embedding
service, but it does not introduce a separate indexing control plane or alter
the batch-container runtime shape.

**Traces to:** RQ-INDEXER-001, RQ-INDEXER-008A

### DSG-LFI-008 `Local and production parity boundary`

Local/testing and production environments differ only in adapter realization and
provider configuration, not in the container's batch contract, the staged email
artifact model, content item shape, or the delegated
`lexongraph-indexer` orchestration contract.

The MVP realizes this parity boundary by keeping the core orchestration and item
model environment-neutral even though only the local/testing profile executes in
the first increment.

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

Under a stable normalization and chunking policy, unchanged mailbox content is
expected to produce the same mailbox artifact, normalized email artifact, and
derived chunk identities on repeated runs.

**Traces to:** RQ-INDEXER-008, RQ-INDEXER-010A

## Verification Realization

### DSG-LFI-011 `Repository verification scope`

LexonFabric-owned verification artifacts validate:

- correct delegation to `lexongraph-indexer`
- correct use of the incremental delegated indexing seam
- correct selection and use of content-resolution, block-store, and
  embedding-provider adapters
- correct interoperability of the local filesystem-backed block-store profile
  with LexonGraph-owned tooling expectations
- correct mailbox retention, normalized email artifact derivation, and chained
  provenance
- correct shaping of chunk-sized delegated email items
- correct progress visibility during long-running mailbox batches
- preservation of stable batch contracts across environments
- Docker Compose-based realization of the local/testing integration topology

LexonFabric-owned verification artifacts do not attempt to revalidate
LexonGraph's own block-store or embedding-trait contracts beyond proving that
LexonFabric consumes them correctly.

**Traces to:** RQ-INDEXER-003A, RQ-INDEXER-008A, RQ-INDEXER-008B,
RQ-INDEXER-010A, RQ-INDEXER-010B, RQ-INDEXER-010, DSG-LFI-001A,
DSG-LFI-002A, DSG-LFI-007A, DSG-LFI-005A
