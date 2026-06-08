# LexonArchiveBuilder Indexer Design

## Status

Specification patch for the approved email-artifact, chunk-level
indexing, local filesystem block-store interoperability, replay-based
streaming delegated indexing, stage-selectable execution, standalone
clustering input discovery, clustering-algorithm selection, clustering-option
exposure, latest planning-policy and telemetry compatibility, upstream
regression assessment, replay-submission and streaming-status observability,
replay-stable fingerprinting, and layer-parallel block-construction
evolution in
`docs/specs/lexonarchivebuilder-indexer/requirements.md`.

## Scope

This document specifies the LexonArchiveBuilder-owned design for realizing the approved
indexer requirements, including the email-ingestion refinement from `.mail` and
`.mbox` mailbox sources to normalized email artifacts and chunk-level embedding
units plus the local filesystem block-store interoperability correction,
replay-based streaming delegated indexing adoption, stage-selectable execution,
standalone clustering input discovery, delegated clustering-algorithm
selection, algorithm-specific clustering-option exposure, latest
planning-policy and telemetry compatibility, upstream regression
assessment, embedding-phase batch-progress observability,
replay-submission observability, streaming-status observability,
telemetry-count-semantics clarity, replay-stable delegated item
identity, and layer-parallel delegated block construction for the
local/testing profile.

This document is layered on top of:

- `docs/specs/lexonarchivebuilder-indexer/requirements.md`
- `README.md`
- external LexonGraph repository source (not vendored in LexonArchiveBuilder):
  `crates/lexongraph-streaming-indexer/src/lib.rs`
- external LexonGraph repository source (not vendored in LexonArchiveBuilder):
  `crates/lexongraph-streaming-clustering/src/lib.rs`
- external LexonGraph repository source (not vendored in LexonArchiveBuilder):
  `crates/lexongraph-block-store/src/lib.rs`
- external LexonGraph repository source (not vendored in LexonArchiveBuilder):
  `crates/lexongraph-block-store-fs/src/lib.rs`
- external LexonGraph repository source (not vendored in LexonArchiveBuilder):
  `crates/lexongraph-embeddings-trait/src/lib.rs`

This document does not redefine the indexing protocol, block identity rules,
the `BlockStore` contract, or the `EmbeddingProvider` contract. Those remain
owned by LexonGraph and its subordinate crates.

## Impact Map

### Directly affected artifacts

- `docs/specs/lexonarchivebuilder-indexer/requirements.md`
- `docs/specs/lexonarchivebuilder-indexer/design.md`
- `docs/specs/lexonarchivebuilder-indexer/validation.md`

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

The LexonArchiveBuilder indexer design is intended to be:

- an orchestration layer around `lexongraph-streaming-indexer`
- explicit about ownership boundaries
- stable across local and production environments
- minimal and fully executable in the local/testing profile first
- extensible to future content types
- compatible with a Linux batch-container runtime
- interoperable with LexonGraph-owned local block-store tooling
- replay-safe at the delegated indexing boundary
- layer-parallel within one delegated construction layer
- bounded by an administrator-controlled concurrency budget
- stage-selectable at the same batch boundary across CLI and request-file use
- explicit about delegated clustering selection and option defaulting
- observable during long-running mailbox batches, local embedding work,
  clustering-only replay submission, and streaming final materialization or
  block-assembly work
- chunk-first for email retrieval while preserving full-message and source
  provenance artifacts

## Boundary Design

### DSG-LFI-001 `Delegated indexing boundary`

LexonArchiveBuilder owns batch orchestration, environment-specific adapter selection,
and application-defined item modeling.

LexonArchiveBuilder does not own index construction, canonical block generation, or
batch recovery semantics internal to the delegated LexonGraph stack.

**Traces to:** RQ-INDEXER-001, RQ-INDEXER-003, RQ-INDEXER-008,
RQ-INDEXER-010A

### DSG-LFI-001A `Replay-based streaming indexing seam`

LexonArchiveBuilder realizes delegated indexing as a repository-owned replay adapter over
`lexongraph-streaming-indexer` rather than as a single terminal indexing call.

That adapter preserves the approved repository stages while internally driving
the upstream lifecycle in order:

1. establish a deterministic delegated item stream for the selected logical
   input set
2. drive one or more planning passes over that stream
3. mark planning complete
4. drive the final materialization replay

The caller-visible `full pipeline`, `ingestion plus embedding generation only`,
and `clustering plus block assembly only` modes remain repository contracts.
The raw upstream planning and materialization lifecycle is not surfaced directly on
the CLI or `BatchRequest`.

LexonArchiveBuilder still owns mailbox expansion, artifact storage, replay
preparation, item shaping, and stage orchestration. The delegated streaming
indexer still owns block construction semantics, canonical block bytes, replay
validation, and branch-shaping behavior.

The design preserves the existing `BatchSummary` shape for the approved stage
modes rather than introducing a separate stage-specific summary schema.

**Traces to:** RQ-INDEXER-003A, RQ-INDEXER-003D, RQ-INDEXER-008,
RQ-INDEXER-010A

### DSG-LFI-001B `Leaf-layer scheduling discipline`

LexonArchiveBuilder realizes replay-based streaming delegated indexing with a layer-aware
scheduler.

Within the delegated leaf construction layer, ready leaf work items may
execute concurrently. The scheduler treats completion of that leaf layer as the
boundary that must be crossed before higher-layer parent construction begins.

Higher-layer parent construction remains bound to the public higher-layer
materialization behavior exposed by the current upstream streaming API surface.

This preserves the delegated LexonGraph ownership of canonical block bytes,
parent-child structure, and final root determination while allowing LexonArchiveBuilder
to overlap independent leaf work. This entry governs only batch-local leaf
scheduling; standalone clustering input discovery is defined separately.

**Traces to:** RQ-INDEXER-003B, RQ-INDEXER-008, RQ-INDEXER-010A

### DSG-LFI-001C `Concurrency budget application`

LexonArchiveBuilder applies one runtime concurrency budget to the layer-aware scheduler.

That budget limits the number of same-layer delegated leaf tasks that may be in
flight at once.

The budget constrains scheduling only. It does not require CPU pinning, change
the batch contract, or expose internal LexonGraph layering details on the MCP
surface.

The current design does not apply this budget to higher-layer parent
construction because the upstream delegated indexing surface does not expose a
public per-group parent-construction seam. Higher-layer concurrency is tracked
as future work rather than approximated in-repo.

**Traces to:** RQ-INDEXER-003B, RQ-INDEXER-003C, RQ-INDEXER-009

### DSG-LFI-001D `Stage-selectable execution contract`

LexonArchiveBuilder exposes one stage-selection contract across its CLI and
`BatchRequest` surfaces.

The approved stage modes are:

- full pipeline
- ingestion plus embedding generation only
- clustering plus block assembly only

The selector defaults to the full pipeline when omitted. Any stage that
includes ingestion continues to consume the request's collection-oriented items.
A clustering-only invocation may use an empty item collection because its input
set is discovered from the configured block store rather than from the request
payload.

The runtime preserves the existing `BatchSummary` shape for each approved stage
mode so stage selection does not create a second result-schema family.

**Traces to:** RQ-INDEXER-001, RQ-INDEXER-002, RQ-INDEXER-003D

### DSG-LFI-001E `Standalone clustering input discovery`

When the caller selects clustering plus block assembly without a preceding
ingestion phase in the same invocation, LexonArchiveBuilder derives its clustering
candidate set by iterating the configured `BlockStore` through the upstream
LexonGraph block-iteration API.

LexonArchiveBuilder treats the upstream iteration contract as the authority for which
stored blocks are clustering-eligible. Repository-owned artifacts that are not
surfaced by that upstream clustering-input iteration contract remain outside the
standalone clustering input set.

Standalone clustering therefore operates over all clustering-eligible blocks
visible in the configured store at invocation time rather than over a
repository-local per-run manifest.

**Traces to:** RQ-INDEXER-003E, RQ-INDEXER-010A

### DSG-LFI-001F `Replay staging for split-stage execution`

Any stage that includes ingestion persists a replay-safe delegated item record
or equivalent repository-owned staging artifact that captures deterministic item
ordering, content-reference identity, and fingerprint inputs needed for later
streaming replays.

A clustering-only invocation reconstructs its replay batches from stored
clustering-eligible inputs plus that replay metadata rather than from
request-supplied collection items.

This design fixes the replay-safety contract but does not freeze a specific
serialization schema for the staging artifact in the specification layer.

**Traces to:** RQ-INDEXER-003A, RQ-INDEXER-003E, RQ-INDEXER-004F

### DSG-LFI-001G `Delegated planning selection seam`

For any execution stage that includes clustering, LexonArchiveBuilder constructs
one explicit upstream `BuiltInPlanning` selection and wraps it in one
`BuiltInPlanningPolicy` before the first streaming planning pass or standalone
clustering replay begins.

The supported built-in delegated clustering choices in this increment are:

- `dcbc`
- `directional-pca`

LexonArchiveBuilder treats the upstream LexonGraph streaming indexer as the
authority for algorithm execution semantics and only maps repository-owned
configuration onto that explicit upstream planning selection. The selected
algorithm and its effective option values remain fixed for the lifetime of one
batch invocation so replay passes, planning completion, and final
materialization do not observe intra-run clustering-configuration drift.

The design default for omitted clustering configuration is **[ASSUMPTION]** the
`dcbc` built-in clustering path because it is the closest fit to the prior
single-path repository behavior and does not require additional
directional-PCA-specific parameter tuning from operators who do not opt in.

**Traces to:** RQ-INDEXER-003F, RQ-INDEXER-008, RQ-INDEXER-010A

### DSG-LFI-001H `Planning-option normalization`

LexonArchiveBuilder normalizes command-line clustering options into one
algorithm-specific upstream settings object before invoking the delegated
streaming indexer.

Shared clustering inputs are:

- algorithm choice
- `cluster_count`
- `random_seed`

The `dcbc` path additionally accepts the upstream balance-constraint family:

- `min_cluster_occupancy`
- `max_cluster_occupancy`
- `max_cluster_size_ratio`
- `soft_balance_penalty`

The `directional-pca` path additionally accepts the upstream
`DirectionalPcaParams` family:

- `retained_dimension_count`
- `variance_exponent`
- `temperature`
- `min_input_count`
- `min_effective_rank`
- `min_cumulative_variance`

Algorithm-specific options that do not belong to the selected algorithm are
rejected during LexonArchiveBuilder's CLI/configuration validation rather than
being silently ignored.

When a caller selects `directional-pca` but omits one or more directional-PCA-
specific options, LexonArchiveBuilder supplies the following deterministic
repository defaults:

- `retained_dimension_count = 1`
- `variance_exponent = 1.0`
- `temperature = 1.0`
- `min_input_count = 2`
- `min_effective_rank = 1`
- `min_cumulative_variance = 0.0`

When a caller selects either built-in clustering algorithm and omits
`cluster_count`, LexonArchiveBuilder derives the effective cluster count from:

- the number of clustering inputs
- the active embedding dimensions and encoding
- the configured block-size target

The derivation target is the smallest count that keeps first-parent
materialization within branch-capacity limits implied by the active embedding
specification while still satisfying the delegated indexer's minimum child-count
constraints.

When a caller explicitly supplies `cluster_count`, LexonArchiveBuilder forwards
that explicit value unchanged. When a caller selects `dcbc` and omits optional
balance settings or `random_seed`, LexonArchiveBuilder passes `None` for those
optional upstream fields.

**Traces to:** RQ-INDEXER-003F, RQ-INDEXER-003G, RQ-INDEXER-003H,
RQ-INDEXER-008, RQ-INDEXER-010A

### DSG-LFI-001I `Latest-upstream compatibility and regression boundary`

LexonArchiveBuilder treats the latest upstream planning-policy and
status-observer telemetry surfaces as a mechanical adaptation boundary, not as
permission to narrow the approved repository contract.

The design therefore preserves these repository-required capabilities across the
upgrade whenever the latest upstream contract still supports them semantically:

- the external stage contract
- deterministic split-stage replay
- explicit `dcbc` and `directional-pca` selection
- omitted `cluster_count` auto-sizing with explicit override preservation
- repository-owned progress projection over upstream lifecycle events
- projection of richer live hierarchy-stage telemetry and heartbeat events onto
  that same repository-owned progress surface
- unchanged MCP search-serving behavior for already-indexed content

If any of those capabilities proves unavailable on the latest upstream surface,
the implementation must surface that as an explicit compatibility finding or
upstream regression rather than silently deleting the affected repository-owned
behavior.

**Traces to:** RQ-INDEXER-003I, RQ-INDEXER-009, RQ-INDEXER-010A

### DSG-LFI-002 `Batch runtime shape`

The indexer runtime is a Linux Docker container that executes one batch under a
caller-selected stage over a collection-oriented request shape.

At the container boundary, the batch contract is collection-oriented rather than
backend-specific so the same invocation shape can be reused for mail archives,
RFC sets, and future content classes.

The first MVP realization covers both mailbox and document-collection items
through this one contract rather than splitting the batch surface by content
class.

For email, a mailbox item is a source container that LexonArchiveBuilder expands into
stored mailbox and normalized email artifacts plus chunk-sized delegated index
items before invoking `lexongraph-streaming-indexer`.

Within that runtime shape, any stage that includes ingestion may advance mailbox
by mailbox through replay staging and streaming-pass preparation rather than
waiting for all delegated work to accumulate behind one final terminal call. A
clustering-only request may leave the collection empty and instead derive its
input from the configured block store through the separate standalone
clustering-discovery seam plus replay-staging seam.

**Traces to:** RQ-INDEXER-001, RQ-INDEXER-002, RQ-INDEXER-003A,
RQ-INDEXER-003D, RQ-INDEXER-003E

### DSG-LFI-002A `Batch progress signaling`

The batch runtime emits progress signals on its normal logging/output surface as
the selected indexing stage advances.

The first design baseline reports at least:

- mailbox-processing start or completion boundaries
- embedding or leaf-materialization progress after mailbox expansion has
  produced delegated items and before observer-driven streaming status is
  available for downstream work
- delegated indexing progress after additional replay batches, planning passes,
  or constructed blocks have advanced
- clustering or block-assembly progress after upstream observer events indicate
  that additional streaming work has advanced

This signaling remains part of the short-lived batch runtime and does not
introduce a separate progress API, control-plane service, or MCP-visible
surface. For a default full-pipeline run, mailbox or delegated-indexing
progress appears first and observer-driven streaming final materialization or
block-assembly progress follows on the same runtime-visible stream.

For ingestion-plus-embedding execution, the repository-owned runtime must not
leave a non-empty delegated item set silent between mailbox-preparation
visibility and the first downstream streaming-status event. The design therefore
includes either bounded-work-unit or bounded-elapsed-time progress signaling
for local embedding or leaf-materialization work on that same runtime-visible
surface.

For clustering-only replay, the repository-owned runtime already knows the
reconstructed replay-batch count and cumulative delegated-item total before the
first upstream planning-pass heartbeat arrives. The design therefore emits one
repository-owned completion signal after each replay-batch submission that
reports completed batches and cumulative delegated items relative to the known
invocation total, rather than relying only on later observer heartbeats with
phase-level elapsed-time visibility.

When the latest upstream observer surface emits richer live telemetry after
repository-owned submission has handed off control, the runtime keeps those
telemetry events on the same progress stream but does not let them replace the
repository-owned invocation-total context established earlier in the run.

When the last repository-owned replay batch has been submitted and control moves
from local submission into waiting for upstream planning-pass completion, the
same runtime-visible progress stream emits an explicit handoff marker. That
marker distinguishes "all known replay batches submitted; waiting on delegated
planning-pass completion" from the later upstream observer heartbeats so
operators can tell whether LexonArchiveBuilder is still feeding the streaming API
or is already blocked on downstream work.

**Traces to:** RQ-INDEXER-001, RQ-INDEXER-008B

### DSG-LFI-002B `Streaming status signaling`

LexonArchiveBuilder realizes long-running indexing observability by implementing
the upstream streaming status-observer seam and translating observer events
into runtime-visible progress messages.

This keeps planning-pass, planning-completion, final materialization, and
clustering
visibility on the same batch-log surface already used for mailbox and
delegated-indexing progress. It does not introduce a separate progress
transport, metrics backend, or MCP-visible monitoring surface.

Because the upstream observer seam begins only once downstream streaming work is
active, this observer translation complements rather than replaces
repository-owned progress signaling for the earlier local embedding or
leaf-materialization gap covered by `DSG-LFI-002A`.

The observer translation layer therefore preserves the boundary between
repository-owned submission progress and upstream phase progress. It does not
reinterpret upstream `InProgress` heartbeats as proof that additional replay
batches are still being submitted once the repository-owned handoff marker has
been emitted.

For the latest known upstream contract, the translation layer maps planning
passes, hierarchy-planning stages, final materialization replay, bottom-up
assembly updates, and heartbeat-style in-progress telemetry onto
repository-visible progress categories without leaking the raw upstream enum
names into the external CLI or `BatchRequest` contract.

The translation layer also preserves count-semantics clarity when the newest
upstream telemetry mixes multiple count shapes:

- planning-pass counts that remain invocation-total or replay-total
- hierarchy-planning counts that may represent stage-local processed work
- bottom-up assembly counts that may represent layer-local block or group totals

The repository-visible rendering therefore distinguishes local replay totals
from upstream stage-local or layer-local telemetry rather than presenting every
count as if it were the same logical unit. This keeps operator logs
understandable even when upstream observer events intentionally reuse one status
shape across multiple telemetry contexts.


**Traces to:** RQ-INDEXER-008B, RQ-INDEXER-010A

### DSG-LFI-003 `Collection item normalization`

LexonArchiveBuilder models each batch element as an application-owned indexing item that
can be transformed into a `lexongraph_streaming_indexer::IndexItem<R>` with:

- application metadata
- a content reference `R`

The content reference stays opaque to the delegated indexer and is interpreted
only by the LexonArchiveBuilder `ContentResolver<R>` implementation.

For document collections, this transformation may remain direct from batch item
to delegated item.

For mailbox inputs, LexonArchiveBuilder first expands one source item into additional
application-owned artifacts and derived delegated items while preserving one
stable collection-oriented batch contract at the container boundary.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-004, RQ-INDEXER-004A, RQ-INDEXER-010

### DSG-LFI-003A `Email ingestion expansion`

LexonArchiveBuilder realizes mailbox-driven email indexing as a staged pipeline:

1. persist the mailbox source as a mailbox provenance artifact
2. parse the mailbox into individual email messages
3. normalize each message into a canonical CBOR email artifact
4. derive email-core text from the normalized email artifact
5. split the email core into chunk-sized delegated index items
6. delegate chunk indexing through replay-safe `lexongraph-streaming-indexer`
   batches

This expansion is LexonArchiveBuilder-owned orchestration and does not require changes
to LexonGraph public contracts.

The first design baseline accepts mailbox source files ending in `.mail` or
`.mbox` and treats broader mailbox archive extension support as out of scope
for this increment.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-004, RQ-INDEXER-004A,
RQ-INDEXER-004B, RQ-INDEXER-004D, RQ-INDEXER-005

## Adapter Design

### DSG-LFI-004 `Content resolution adapter`

LexonArchiveBuilder provides a concrete `lexongraph_streaming_indexer::ContentResolver<R>`
implementation that resolves a collection item's content reference into the
`Content` value consumed by the delegated indexer and supplies the replay-stable
fingerprint required by the upstream streaming contract.

The resolver owns source-specific retrieval logic for initially supported item
classes such as mailboxes and document collections, while preserving one stable
batch contract at the container boundary.

For document collections, the resolver may continue to read final delegated
content directly from the document source.

For mailbox-driven email indexing, LexonArchiveBuilder preprocessing materializes final
chunk items before the resolver hands chunk text to the delegated indexer.

Within that mailbox-driven path, LexonArchiveBuilder treats source files ending in
`.mail` or `.mbox` as equivalent mailbox containers for normalization and
chunk derivation, without widening the first increment to arbitrary mailbox
archive extensions.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-004, RQ-INDEXER-010

### DSG-LFI-004G `Replay fingerprint derivation`

LexonArchiveBuilder derives replay fingerprints from stable, content-based identity inputs
rather than from transient runtime state.

For document-derived items, the fingerprint is derived from the resolved content
identity exposed by the batch item. For email-derived chunk items, the
fingerprint is derived from the normalized email artifact identity plus the
stable chunk locator.

This design fixes the stable fingerprint inputs but leaves the exact
serialization details to implementation so long as the resulting fingerprint is
deterministic across planning passes, final materialization replay, and reruns.

**Traces to:** RQ-INDEXER-004F, RQ-INDEXER-008

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

LexonArchiveBuilder derives an email-core text representation from the normalized email
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

The chunking boundary remains a LexonArchiveBuilder-owned policy seam so later
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

LexonArchiveBuilder preserves explicit chained provenance:

- chunk item -> normalized email artifact
- normalized email artifact -> mailbox provenance artifact

This chain allows the common search hit path to remain chunk-first while
preserving full-message expansion and source-level reprocessing.

**Traces to:** RQ-INDEXER-004D, RQ-INDEXER-005

### DSG-LFI-004F `Chunk locator representation`

LexonArchiveBuilder represents chunk identity as a LexonArchiveBuilder-owned locator rather
than relying on a first-class upstream item-name field.

The locator is attached through the delegated item's `metadata`, `content_ref`,
or both, and is sufficient to tell which chunk is being processed or returned.
The first design baseline composes this locator from:

- the normalized email artifact reference
- chunk-local identity such as ordinal position
- any needed policy/version discriminator when chunk-identity stability depends
  on the active chunking policy

This representation remains internal to LexonArchiveBuilder's item model and does not
change the public LexonGraph contract.

**Traces to:** RQ-INDEXER-004E, RQ-INDEXER-010

### DSG-LFI-005 `Block storage adapter boundary`

LexonArchiveBuilder provides concrete `lexongraph_block_store::BlockStore`
implementations or adapters selected by environment.

- local/testing selects a filesystem-backed block store
- production selects an Azure Blob-backed block store

The same environment-selected `BlockStore` abstraction family is reused for:

- delegated LexonGraph index blocks
- normalized email artifacts
- mailbox provenance artifacts

The rest of the LexonArchiveBuilder indexing flow consumes only the backend-neutral
`BlockStore` contract and does not depend on filesystem paths or Azure-specific
blob layout details.

For the first MVP, only the local/testing block-store realization must be
executable. The production storage profile remains a preserved adapter seam and
configuration target rather than an implemented runtime path in this increment.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-007, RQ-INDEXER-010

### DSG-LFI-005A `Filesystem block-store interoperability`

For the local/testing profile, LexonArchiveBuilder realizes the filesystem-backed
`BlockStore` through the upstream `lexongraph-block-store-fs` crate rather than
through a repository-local filesystem naming scheme.

This keeps local block publication interoperable with LexonGraph-owned
filesystem tooling by using the upstream on-disk layout contract, including a
sharded block path derived from the block hash rather than a flat
repository-specific filename mapping.

The rest of the LexonArchiveBuilder indexing flow still consumes only the abstract
`BlockStore` interface, so this interoperability requirement does not leak
filesystem path details into content resolution, batch orchestration, or future
production adapters.

Because the superseded repository-local filesystem layout is not part of the
approved compatibility boundary for this increment, the local/testing
realization may require a fresh or rebuilt local store instead of preserving
reads from the old layout.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-010B

### DSG-LFI-006 `Embedding provider adapter boundary`

LexonArchiveBuilder provides environment-selected implementations or adapters that
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

LexonArchiveBuilder selects the storage adapter and embedding provider as a coupled
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

### DSG-LFI-007B `Concurrency configuration surface`

The administrator-defined concurrency budget is supplied on the same
batch-request configuration surface as other runtime tuning inputs.

The design adds optional top-level request fields:

- `max_concurrency`: maximum number of same-layer delegated leaf tasks allowed
  in flight at once
- `stage`: selected execution stage, defaulting to the full pipeline when
  omitted

If `max_concurrency` is omitted, LexonArchiveBuilder derives the runtime budget as:

`max(1, floor(detected_physical_cpu_count / 2))`

For containerized or quota-constrained deployments where direct physical-core
detection is unavailable or unreliable, the runtime may fall back to the best
available host-visible CPU-count signal, provided the default remains bounded,
documented, and never drops below one.

This configuration surface remains environment-neutral: local/testing and the
preserved production profile use the same request shape, stage-selection
contract, and scheduler contract. Higher-layer parent construction remains
serial at the LexonArchiveBuilder layer until the upstream streaming indexing
API exposes a compatible concurrency seam.

**Traces to:** RQ-INDEXER-003C, RQ-INDEXER-003D, RQ-INDEXER-007,
RQ-INDEXER-010

### DSG-LFI-007C `Clustering CLI surface`

LexonArchiveBuilder exposes clustering selection and clustering-option flags on
the `run` command alongside the existing request-file and stage-selection
surface.

The CLI surface is organized as one shared selector plus algorithm-scoped
options:

- `--clustering-algorithm`
- `--clustering-cluster-count`
- `--clustering-random-seed`
- `--clustering-min-cluster-occupancy`
- `--clustering-max-cluster-occupancy`
- `--clustering-max-cluster-size-ratio`
- `--clustering-soft-balance-penalty`
- `--clustering-retained-dimension-count`
- `--clustering-variance-exponent`
- `--clustering-temperature`
- `--clustering-min-input-count`
- `--clustering-min-effective-rank`
- `--clustering-min-cumulative-variance`

LexonArchiveBuilder parses these flags after loading the request file and
resolves them into one effective clustering configuration used only when the
selected execution stage includes clustering. The `ingestion+embedding` stage
ignores clustering execution, but still rejects syntactically invalid
clustering option combinations at argument-validation time so misconfigured
automation fails early and explicitly.

This increment keeps clustering-option exposure on the CLI boundary. The
request-file `BatchRequest` contract remains unchanged unless a later semantic
increment explicitly adds persistent clustering-configuration fields there.

**Traces to:** RQ-INDEXER-003F, RQ-INDEXER-003G, RQ-INDEXER-007,
RQ-INDEXER-010

### DSG-LFI-008 `Local and production parity boundary`

Local/testing and production environments differ only in adapter realization and
provider configuration, not in the container's batch contract, the staged email
artifact model, content item shape, the stage-selection and concurrency-
configuration surfaces, the clustering-selection and clustering-option CLI
surface, or the delegated `lexongraph-streaming-indexer`
orchestration contract.

The MVP realizes this parity boundary by keeping the core orchestration and item
model environment-neutral even though only the local/testing profile executes in
the first increment. Standalone clustering continues to rely on the same
configured `BlockStore` abstraction and the same upstream block-iteration
contract across environments rather than introducing a local-only discovery
mechanism.

**Traces to:** RQ-INDEXER-007, RQ-INDEXER-010, RQ-INDEXER-003D,
RQ-INDEXER-003E, RQ-INDEXER-003G

## Invariant Design

### DSG-LFI-009 `Search-serving separation`

The indexer package remains separate from MCP server search semantics. No design
element in this package changes retrieval contracts, query semantics, or search
ranking behavior.

**Traces to:** RQ-INDEXER-009

### DSG-LFI-010 `Idempotence ownership`

LexonArchiveBuilder relies on the underlying immutable, hash-addressed block model for
rerun idempotence and does not introduce repository-local batch-recovery or
duplicate-suppression semantics that could conflict with LexonGraph ownership.

Under a stable normalization and chunking policy, unchanged mailbox content is
expected to produce the same mailbox artifact, normalized email artifact, and
derived chunk identities on repeated runs.

Replay staging and content fingerprinting are likewise required to be
semantically transparent: repeated planning and final materialization replays
over the same logical item set must not introduce replay mismatches under
unchanged content and metadata semantics.

Leaf-layer scheduling is therefore required to be semantically transparent:
changing the concurrency budget may change throughput, but it does not change
the logical block set or final root produced for unchanged input under the same
delegated LexonGraph contract.

For standalone clustering, the comparable invariant is store-snapshot stability:
repeating the clustering-only stage against the same clustering-eligible block
set surfaced by the upstream iteration contract is expected to produce the same
logical clustering result under unchanged upstream semantics.

The same stability expectation applies to clustering configuration resolution:
repeating a clustering-enabled run with the same explicit flags or the same
omitted-option defaulting path must resolve to the same effective upstream
clustering algorithm and option values.

**Traces to:** RQ-INDEXER-003B, RQ-INDEXER-003C, RQ-INDEXER-008,
RQ-INDEXER-003E, RQ-INDEXER-003F, RQ-INDEXER-003G, RQ-INDEXER-010A

## Verification Realization

### DSG-LFI-011 `Repository verification scope`

LexonArchiveBuilder-owned verification artifacts validate:

- correct delegation to `lexongraph-streaming-indexer`
- correct use of the replay-based streaming indexing seam
- correct stage-selectable execution across CLI and request-file invocation
  without exposing the raw upstream lifecycle
- correct leaf-layer concurrency scheduling with cross-layer barriers
- correct standalone clustering input discovery through the upstream block-
  iteration contract
- correct explicit delegated clustering-algorithm selection and
  algorithm-specific option normalization on the CLI surface
- correct deterministic replay staging and replay-stable content fingerprinting
- correct selection and use of content-resolution, block-store, and
  embedding-provider adapters
- correct interoperability of the local filesystem-backed block-store profile
  with LexonGraph-owned tooling expectations
- correct mailbox retention, normalized email artifact derivation, and chained
  provenance
- correct shaping of chunk-sized delegated email items
- correct progress visibility during long-running mailbox batches, including
  the no-silent-gap requirement between mailbox preparation and local embedding
  progress plus observer-driven final materialization or block-assembly visibility
- correct application and defaulting of the administrator-defined concurrency
  budget
- preservation of stable batch contracts across environments
- explicit preservation of higher-layer parent construction as future work at
  the current upstream API boundary
- Docker Compose-based realization of the local/testing integration topology

LexonArchiveBuilder-owned verification artifacts do not attempt to revalidate
LexonGraph's own block-store or embedding-trait contracts beyond proving that
LexonArchiveBuilder consumes them correctly.

**Traces to:** RQ-INDEXER-003A, RQ-INDEXER-003B, RQ-INDEXER-003C,
RQ-INDEXER-003D, RQ-INDEXER-003E, RQ-INDEXER-003F, RQ-INDEXER-003G,
RQ-INDEXER-004F, RQ-INDEXER-008A, RQ-INDEXER-008B, RQ-INDEXER-010A,
RQ-INDEXER-010B, RQ-INDEXER-010, DSG-LFI-001A, DSG-LFI-001B,
DSG-LFI-001C, DSG-LFI-001D, DSG-LFI-001E, DSG-LFI-001F, DSG-LFI-001G,
DSG-LFI-001H, DSG-LFI-001I, DSG-LFI-002A, DSG-LFI-002B, DSG-LFI-004G,
DSG-LFI-005A, DSG-LFI-007A, DSG-LFI-007B, DSG-LFI-007C
