# LexonFabric Indexer Validation

## Status

Draft specification patch derived from
`docs/specs/lexonfabric-indexer/requirements.md` and
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

### VAL-LFI-002

Submit a batch containing representative mailbox and document-collection items.

**Pass condition:** LexonFabric transforms each batch element into an
application-defined content reference and delegates indexing through
`lexongraph-indexer` rather than implementing an in-repo indexing algorithm.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-003, RQ-INDEXER-004, DSG-LFI-001,
DSG-LFI-003, DSG-LFI-004

### VAL-LFI-003

Exercise the LexonFabric content-resolution adapter with resolvable and
unresolvable content references.

**Pass condition:** successful resolution produces the `Content` shape expected
by `lexongraph-indexer::ContentResolver<R>`, and failures surface through the
delegated indexing error path rather than reporting success.

**Traces to:** RQ-INDEXER-004, DSG-LFI-004

### VAL-LFI-004

Run the same logical batch once in local/testing mode and once in production
mode using environment-appropriate storage and embedding adapters.

**Pass condition:** the batch contract and delegation flow remain the same while
only the `BlockStore` and `EmbeddingProvider` realizations differ by
environment.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-006, RQ-INDEXER-007, DSG-LFI-005,
DSG-LFI-006, DSG-LFI-007, DSG-LFI-008

### VAL-LFI-005

Run the local/testing environment profile.

**Pass condition:** LexonFabric selects a filesystem-backed `BlockStore` and a
local STAPI-compatible embedding provider without changing the collection input
contract or the delegated indexer contract.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-006, RQ-INDEXER-007, DSG-LFI-005,
DSG-LFI-006, DSG-LFI-007

### VAL-LFI-006

Run the production environment profile.

**Pass condition:** LexonFabric selects an Azure Blob-backed `BlockStore` and an
Azure OpenAI-backed `EmbeddingProvider` without changing the collection input
contract or the delegated indexer contract.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-006, RQ-INDEXER-007, DSG-LFI-005,
DSG-LFI-006, DSG-LFI-007

### VAL-LFI-007

Run the same logical batch twice with unchanged source content and deterministic
dependency behavior.

**Pass condition:** the repeated run remains idempotent under the underlying
immutable, hash-addressed block semantics and does not require LexonFabric to
implement separate duplicate-suppression logic.

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
container contract or the environment-selection contract.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-010, DSG-LFI-003, DSG-LFI-004,
DSG-LFI-008
