# LexonFabric Indexer Validation

## Status

Phase 2 validation patch for the approved email-artifact and chunk-level
indexing evolution in `docs/specs/lexonfabric-indexer/requirements.md` and
`docs/specs/lexonfabric-indexer/design.md`.

## Validation Scope

These validation entries define the expected conformance surface for the
LexonFabric-owned indexer boundary.

This package validates LexonFabric's batch contract, adapter selection, and
delegated use of LexonGraph interfaces. It does not redefine validation already
owned by LexonGraph for `lexongraph-indexer`, `BlockStore`, or
`EmbeddingProvider`.

## Validation Entries

### VAL-LFI-001

Inspect the containerized indexer entrypoint contract.

**Pass condition:** the runtime executes as a Linux batch container and accepts
a collection-oriented indexing request rather than a single hard-coded source.

**Traces to:** RQ-INDEXER-001, RQ-INDEXER-002, DSG-LFI-002

### VAL-LFI-001A

Inspect the local Docker Compose topology for the MVP profile.

**Pass condition:** the Compose topology brings up the batch container together
with the local embedding service and required local storage mounts or volumes
without introducing a separate long-lived indexing control-plane service.

**Traces to:** RQ-INDEXER-001, RQ-INDEXER-008A, DSG-LFI-007A

### VAL-LFI-002

Submit a batch containing representative mailbox and document-collection items.

**Pass condition:** LexonFabric transforms each batch element into an
application-defined content reference and delegates indexing through
`lexongraph-indexer` rather than implementing an in-repo indexing algorithm.
Mailbox inputs are expanded into LexonFabric-owned artifacts and delegated
chunk-sized email items before delegated indexing, while document items remain
compatible with the same collection-oriented batch contract.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-003, RQ-INDEXER-004, DSG-LFI-001,
DSG-LFI-003, DSG-LFI-004

### VAL-LFI-002A

Run mailbox ingestion through the LexonFabric-owned preprocessing pipeline.

**Pass condition:** the original mailbox is retained as a mailbox provenance
artifact, each parsed email produces a canonical normalized email artifact, and
the normalized email artifact identity is derived from the serialized normalized
artifact rather than from raw mailbox bytes.

**Traces to:** RQ-INDEXER-004A, RQ-INDEXER-005, RQ-INDEXER-008, DSG-LFI-003A,
DSG-LFI-004A, DSG-LFI-010

### VAL-LFI-002B

Inspect the derived email-core and chunk-generation pipeline for mailbox-driven
email indexing.

**Pass condition:** LexonFabric derives a meaningful email body representation
for embedding, applies a sentence-aware baseline chunking policy in the first
realization, and keeps the chunking boundary behind a LexonFabric-owned seam so
future tokenizer-driven or more semantic chunking can be introduced without
changing the batch contract or delegated LexonGraph contracts.

**Traces to:** RQ-INDEXER-004A, RQ-INDEXER-004B, RQ-INDEXER-010, DSG-LFI-004B,
DSG-LFI-004C

### VAL-LFI-002C

Inspect the delegated email chunk items produced from a normalized email
artifact.

**Pass condition:** each delegated email item embeds chunk text, carries a
stable normalized email artifact reference, duplicates enough message metadata
for the common retrieval/rendering path, preserves chained provenance from
chunk to normalized email artifact to mailbox provenance artifact, and carries
a stable chunk locator that makes the specific chunk identifiable during
processing and retrieval.

**Traces to:** RQ-INDEXER-004C, RQ-INDEXER-004D, RQ-INDEXER-004E, DSG-LFI-004D,
DSG-LFI-004E, DSG-LFI-004F

### VAL-LFI-003

Exercise the LexonFabric content-resolution adapter with resolvable and
unresolvable content references.

**Pass condition:** successful resolution produces the `Content` shape expected
by `lexongraph_indexer::ContentResolver<R>`, and failures surface through the
delegated indexing error path rather than reporting success.

**Traces to:** RQ-INDEXER-004, DSG-LFI-004

### VAL-LFI-004

Inspect environment-selection wiring for both the executable local/testing
profile and the preserved production profile boundary.

**Pass condition:** the batch contract and delegation flow remain environment
neutral, the local/testing profile is executable end to end, and the production
profile remains representable through the same adapter-selection boundary
without requiring Azure-specific execution in the first MVP. This neutrality
also applies to normalized email artifacts and mailbox provenance artifacts that
share the same `BlockStore` abstraction family.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-006, RQ-INDEXER-007, DSG-LFI-005,
DSG-LFI-006, DSG-LFI-007, DSG-LFI-008

### VAL-LFI-005

Run the local/testing environment profile.

**Pass condition:** LexonFabric selects a filesystem-backed `BlockStore` for
delegated index blocks, normalized email artifacts, and mailbox provenance
artifacts plus a local STAPI-compatible embedding provider without changing the
collection input contract or the delegated indexer contract.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-006, RQ-INDEXER-007, DSG-LFI-005,
DSG-LFI-006, DSG-LFI-007

### VAL-LFI-006

Inspect the preserved production environment profile boundary.

**Pass condition:** production-specific storage and embedding identifiers remain
behind the same `BlockStore` and `EmbeddingProvider` selection boundary as the
local/testing profile, and no local-only assumptions leak into the core batch
contract or content-model abstractions.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-006, RQ-INDEXER-007, DSG-LFI-005,
DSG-LFI-006, DSG-LFI-007

### VAL-LFI-007

Run the same logical batch twice with unchanged source content and deterministic
dependency behavior.

**Pass condition:** the repeated run remains idempotent under the underlying
immutable, hash-addressed block semantics and does not require LexonFabric to
implement separate duplicate-suppression logic. Under a stable normalization
and chunking policy, unchanged mailbox input reproduces the same mailbox
artifact, normalized email artifact, and derived chunk identities.

**Traces to:** RQ-INDEXER-008, DSG-LFI-010

### VAL-LFI-008

Inspect the repository's indexer specification package against MCP server
artifacts.

**Pass condition:** no LexonFabric indexer artifact in this package redefines
search-serving contracts, query semantics, or retrieval behavior owned by the
MCP server surface.

**Traces to:** RQ-INDEXER-009, DSG-LFI-009

### VAL-LFI-009

Inspect the repository's indexer specification package against upstream
LexonGraph contracts.

**Pass condition:** the package remains subordinate to
`lexongraph-indexer`, `lexongraph-block-store`, and
`lexongraph-embeddings-trait`, and does not redefine their public semantics.

**Traces to:** RQ-INDEXER-010A, DSG-LFI-001, DSG-LFI-011

### VAL-LFI-010

Add a new content-reference class beyond the initial mailbox and
document-collection inputs.

**Pass condition:** the new content class can be introduced by extending
LexonFabric item modeling and content resolution without changing the batch
container contract or the environment-selection contract. Existing
email-specific normalization and chunking policies do not preclude
document-specific or future content-specific artifact and chunking policies.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-010, DSG-LFI-003, DSG-LFI-004,
DSG-LFI-008
