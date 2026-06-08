# LexonArchiveBuilder Indexer Validation

## Status

Phase 2 validation patch for the approved email-artifact, chunk-level
indexing, local filesystem block-store interoperability, replay-based
streaming delegated indexing, stage-selectable execution, standalone
clustering input discovery, clustering-algorithm selection, clustering-option
exposure, streaming-status observability, replay-stable fingerprinting, and
layer-parallel
block-construction evolution in
`docs/specs/lexonarchivebuilder-indexer/requirements.md` and
`docs/specs/lexonarchivebuilder-indexer/design.md`.

## Validation Scope

These validation entries define the expected conformance surface for the
LexonArchiveBuilder-owned indexer boundary, including local filesystem
block-store interoperability, replay-based streaming delegated indexing,
stage-selectable execution, standalone clustering input discovery, explicit
delegated clustering-algorithm selection, algorithm-specific clustering-option
exposure, embedding-phase batch-progress observability, streaming-status
observability, replay-stable fingerprinting, and leaf-layer parallel block
scheduling in the local/testing profile.

This package validates LexonArchiveBuilder's batch contract, adapter selection, and
delegated use of LexonGraph interfaces. It does not redefine validation already
owned by LexonGraph for `lexongraph-streaming-indexer`,
`lexongraph-streaming-clustering`, `BlockStore`, or `EmbeddingProvider`.

## Validation Entries

### VAL-LFI-001

Inspect the containerized indexer entrypoint contract.

**Pass condition:** the runtime executes as a Linux batch container and accepts
a collection-oriented indexing request rather than a single hard-coded source,
and the entrypoint preserves one default full-pipeline mode plus the approved
stage-selection surface.

**Traces to:** RQ-INDEXER-001, RQ-INDEXER-002, RQ-INDEXER-003D, DSG-LFI-001D,
DSG-LFI-002

### VAL-LFI-001A

Inspect the local Docker Compose topology for the MVP profile.

**Pass condition:** the Compose topology brings up the batch container together
with the local embedding service and required local storage mounts or volumes
without introducing a separate long-lived indexing control-plane service.

**Traces to:** RQ-INDEXER-001, RQ-INDEXER-008A, DSG-LFI-007A

### VAL-LFI-002

Submit a batch containing representative mailbox and document-collection items.

**Pass condition:** LexonArchiveBuilder transforms each batch element into an
application-defined content reference and delegates indexing through
`lexongraph-streaming-indexer` rather than implementing an in-repo indexing
algorithm.
Mailbox inputs are expanded into LexonArchiveBuilder-owned artifacts and delegated
chunk-sized email items before delegated indexing, while document items remain
compatible with the same collection-oriented batch contract.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-003, RQ-INDEXER-004, DSG-LFI-001,
DSG-LFI-003, DSG-LFI-004

### VAL-LFI-002E

Inspect the delegated indexing orchestration for a representative mailbox
batch.

**Pass condition:** LexonArchiveBuilder uses the upstream replay-based streaming
indexing path, including at least one training pass, explicit training
completion, and final materialization replay, while preserving the approved
repository stage contract rather than exposing raw upstream lifecycle phases.

**Traces to:** RQ-INDEXER-003A, DSG-LFI-001A

### VAL-LFI-002F

Inspect delegated leaf scheduling for a batch that produces more than one ready
leaf work item.

**Pass condition:** LexonArchiveBuilder permits independent leaf work from the same
construction layer to execute concurrently, it does not begin higher-layer
parent construction until that leaf work has completed, and the repository does
not claim in-repo higher-layer concurrency that the delegated upstream surface
does not expose.

**Traces to:** RQ-INDEXER-003B, DSG-LFI-001B, DSG-LFI-001C

### VAL-LFI-002G

Inspect the stage-selection surface on the CLI and `BatchRequest` contract.

**Pass condition:** the same approved stage selector is representable on both
surfaces, omitting it defaults to the full pipeline, a clustering-only request
may use an empty item collection, and stage selection does not introduce a
stage-specific result-schema family distinct from `BatchSummary`.

**Traces to:** RQ-INDEXER-001, RQ-INDEXER-002, RQ-INDEXER-003D, DSG-LFI-001D,
DSG-LFI-007B

### VAL-LFI-002H

Run the ingestion-plus-embedding stage without the clustering-plus-block-
assembly stage for a representative mailbox batch.

**Pass condition:** LexonArchiveBuilder expands mailbox inputs, persists the resulting
artifacts plus replay-safe delegated staging needed for a later streaming
replay, does not require clustering or higher-layer finalization in the same
invocation, and still returns the existing `BatchSummary` shape.

**Traces to:** RQ-INDEXER-003A, RQ-INDEXER-003D, DSG-LFI-001A, DSG-LFI-001D,
DSG-LFI-001F

### VAL-LFI-002I

Run the clustering-plus-block-assembly stage against a configured block store
that already contains representative delegated blocks, replay metadata, and an
empty request item collection.

**Pass condition:** LexonArchiveBuilder iterates all clustering-eligible blocks exposed
by the upstream block-iteration contract for the configured store, excludes
stored artifacts outside that upstream input surface, reconstructs the
deterministic replay input needed by the streaming indexer, and performs
clustering or block assembly without requiring a prior LexonArchiveBuilder
summary manifest.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-003E, RQ-INDEXER-004F,
RQ-INDEXER-010A, DSG-LFI-001E, DSG-LFI-001F

### VAL-LFI-002J

Compare a representative full-pipeline run with an equivalent split-stage run.

**Pass condition:** the split-stage path reconstructs the same logical replay
item order and fingerprint inputs used by the full-pipeline path, so the
streaming indexer accepts both executions without replay-mismatch failures and
both executions remain contract-equivalent at the LexonArchiveBuilder boundary.

**Traces to:** RQ-INDEXER-003A, RQ-INDEXER-004F, DSG-LFI-001A, DSG-LFI-001F

### VAL-LFI-002K

Inspect the clustering-enabled CLI surface for a representative `run`
invocation.

**Pass condition:** the CLI exposes one explicit clustering-algorithm selector,
accepts the supported shared clustering options plus the approved
algorithm-specific option families, rejects option values that do not belong to
the selected algorithm instead of silently ignoring them, and preserves the
existing request-file-driven runtime shape.

**Traces to:** RQ-INDEXER-003F, RQ-INDEXER-003G, DSG-LFI-001G,
DSG-LFI-001H, DSG-LFI-007C

### VAL-LFI-002L

Run clustering-enabled execution for each supported built-in clustering
algorithm once with omitted `cluster_count` and once with the equivalent
explicit derived `cluster_count`.

**Pass condition:** for both `dcbc` and `directional-pca`, LexonArchiveBuilder
resolves the omitted-option invocation to the same effective delegated
clustering configuration as the corresponding explicit derived-count
invocation, so omitted `cluster_count` remains deterministic and does not
create hidden replay drift.

**Traces to:** RQ-INDEXER-003F, RQ-INDEXER-003G, RQ-INDEXER-003H,
RQ-INDEXER-008, DSG-LFI-001G, DSG-LFI-001H, DSG-LFI-010

### VAL-LFI-002M

Run clustering-enabled execution with omitted `cluster_count` under an
embedding specification and block-size target that require auto-sizing to
increase the first-layer cluster count above a trivial fixed default.

**Pass condition:** LexonArchiveBuilder derives a larger effective
`cluster_count` from clustering-input count plus embedding-size-aware
branch-capacity constraints, and first-parent materialization does not fail
solely because the omitted-option path fell back to an unsafe fixed default.

**Traces to:** RQ-INDEXER-003H, DSG-LFI-001H, DSG-LFI-010

### VAL-LFI-002A

Run mailbox ingestion through the LexonArchiveBuilder-owned preprocessing pipeline.

**Pass condition:** the original mailbox is retained as a mailbox provenance
artifact, each parsed email produces a canonical normalized email artifact, and
the normalized email artifact identity is derived from the serialized normalized
artifact rather than from raw mailbox bytes.

**Traces to:** RQ-INDEXER-004A, RQ-INDEXER-005, RQ-INDEXER-008, DSG-LFI-003A,
DSG-LFI-004A, DSG-LFI-010

### VAL-LFI-002B

Inspect the derived email-core and chunk-generation pipeline for mailbox-driven
email indexing.

**Pass condition:** LexonArchiveBuilder derives a meaningful email body representation
for embedding, applies a sentence-aware baseline chunking policy in the first
realization, and keeps the chunking boundary behind a LexonArchiveBuilder-owned seam so
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

### VAL-LFI-002D

Submit representative mailbox batch inputs that reference one `.mail` source
file and one `.mbox` source file.

**Pass condition:** LexonArchiveBuilder accepts both source files as mailbox inputs for
the same normalization and chunk-derivation pipeline, and conformance does not
depend on broader mailbox archive extension support in this increment.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-004A, DSG-LFI-003A, DSG-LFI-004

### VAL-LFI-003

Exercise the LexonArchiveBuilder content-resolution adapter with resolvable and
unresolvable content references.

**Pass condition:** successful resolution produces the `Content` shape expected
by `lexongraph_streaming_indexer::ContentResolver<R>`, successful fingerprinting
produces a stable replay identity for the same logical item, and failures
surface through the delegated indexing error path rather than reporting
success.

**Traces to:** RQ-INDEXER-004, RQ-INDEXER-004F, DSG-LFI-004, DSG-LFI-004G

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

### VAL-LFI-004A

Inspect the batch-request runtime tuning surface for local/testing and the
preserved production profile boundary.

**Pass condition:** both profiles use the same optional `max_concurrency` and
`stage` request fields, an explicit `max_concurrency` value caps same-layer
delegated leaf work, an omitted `max_concurrency` value defaults to one half of
detected physical CPUs with a minimum of one worker slot, and an omitted
`stage` value defaults to the full pipeline. Any fallback used when direct
physical-core detection is not available remains documented and does not change
the request shape.

**Traces to:** RQ-INDEXER-003C, RQ-INDEXER-003D, RQ-INDEXER-007, DSG-LFI-007B,
DSG-LFI-008

### VAL-LFI-005

Run the local/testing environment profile.

**Pass condition:** LexonArchiveBuilder selects a filesystem-backed `BlockStore` for
delegated index blocks, normalized email artifacts, and mailbox provenance
artifacts plus a local STAPI-compatible embedding provider without changing the
collection input contract or the delegated indexer contract.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-006, RQ-INDEXER-007, DSG-LFI-005,
DSG-LFI-006, DSG-LFI-007

### VAL-LFI-005A

Inspect a local/testing block store produced by LexonArchiveBuilder and the
filesystem-backed block-store adapter selected for that profile.

**Pass condition:** the local/testing profile uses the upstream
`lexongraph-block-store-fs` realization, publishes blocks using the upstream
filesystem naming/layout contract rather than a repository-local flat filename
scheme, and yields a local store that LexonGraph filesystem tooling such as
`lexongraph-block-inspect` can consume without repository-specific translation.
Validation may treat the local store as fresh for this increment rather than
requiring reads from the superseded custom filesystem layout.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-010B, DSG-LFI-005, DSG-LFI-005A

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
immutable, hash-addressed block semantics and does not require LexonArchiveBuilder to
implement separate duplicate-suppression logic. Under a stable normalization
and chunking policy, unchanged mailbox input reproduces the same mailbox
artifact, normalized email artifact, derived chunk identities, logical block
set, and final root even when the concurrency budget changes between runs.

**Traces to:** RQ-INDEXER-003B, RQ-INDEXER-003C, RQ-INDEXER-008, DSG-LFI-010

### VAL-LFI-007A

Run a mailbox batch that is large enough to produce multiple observable
mailbox-processing and delegated-indexing steps.

**Pass condition:** the normal batch log stream reports forward progress before
the final summary, including mailbox-processing visibility plus delegated
indexing visibility and observer-driven streaming visibility across training
and finalization when the selected stage includes clustering, so an operator
can distinguish an active run from a hung run without consulting a separate
control-plane service.

**Traces to:** RQ-INDEXER-008B, DSG-LFI-002A

### VAL-LFI-007B

Run the clustering-only stage twice against an unchanged clustering-eligible
block-store snapshot.

**Pass condition:** the same clustering-eligible block set surfaced by the
upstream block-iteration contract produces the same logical clustering result on
repeated standalone clustering runs, without requiring repository-local
duplicate-suppression logic, as long as the effective clustering algorithm and
option values are unchanged.

**Traces to:** RQ-INDEXER-003E, RQ-INDEXER-003F, RQ-INDEXER-003G,
RQ-INDEXER-008, DSG-LFI-001E, DSG-LFI-001G, DSG-LFI-001H, DSG-LFI-010

### VAL-LFI-007C

Run an ingestion-plus-embedding stage with a mailbox batch large enough to keep
local embedding or leaf-materialization work active after delegated-item
preparation has been reported.

**Pass condition:** after mailbox-preparation visibility is emitted for a
non-empty batch, the normal batch log stream continues to report progress by
bounded work units or bounded elapsed time while delegated embedding work
remains outstanding, rather than remaining silent until the first downstream
streaming-status event or the final summary.

**Traces to:** RQ-INDEXER-008B, DSG-LFI-002A, DSG-LFI-002B

### VAL-LFI-008

Inspect the repository's indexer specification package against MCP server
artifacts.

**Pass condition:** no LexonArchiveBuilder indexer artifact in this package redefines
search-serving contracts, query semantics, or retrieval behavior owned by the
MCP server surface.

**Traces to:** RQ-INDEXER-009, DSG-LFI-009

### VAL-LFI-009

Inspect the repository's indexer specification package against upstream
LexonGraph contracts.

**Pass condition:** the package remains subordinate to
`lexongraph-streaming-indexer`, `lexongraph-streaming-clustering`,
`lexongraph-block-store`, and `lexongraph-embeddings-trait`, and does not
redefine their public semantics.

**Traces to:** RQ-INDEXER-010A, DSG-LFI-001, DSG-LFI-011

### VAL-LFI-009A

Inspect the repository's indexer specification package against LexonGraph's
filesystem-backed block-store tooling boundary.

**Pass condition:** the package requires LexonArchiveBuilder to consume the upstream
filesystem-backed block-store layout contract for local/testing operation
without redefining that layout behind a repository-local scheme, while leaving
production storage layout details outside this local-only interoperability
constraint.

**Traces to:** RQ-INDEXER-010B, DSG-LFI-005A, DSG-LFI-011

### VAL-LFI-010

Add a new content-reference class beyond the initial mailbox and
document-collection inputs.

**Pass condition:** the new content class can be introduced by extending
LexonArchiveBuilder item modeling and content resolution without changing the batch
container contract or the environment-selection contract. Existing
email-specific normalization and chunking policies do not preclude
document-specific or future content-specific artifact and chunking policies.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-010, DSG-LFI-003, DSG-LFI-004,
DSG-LFI-008
