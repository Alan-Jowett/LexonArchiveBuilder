# LexonArchiveBuilder Indexer Design

## Status

Specification patch for the approved email-artifact, chunk-level
indexing, local filesystem block-store interoperability, replay-based
streaming delegated indexing, stage-selectable execution, standalone
clustering input discovery, published-profile API adoption, published-profile
contract pinning, latest published-profile and telemetry compatibility, upstream
regression assessment, replay-submission and streaming-status observability,
clustering-failure diagnostics, rooted block-tree quality assessment with
rooted TNN-recall diagnostics, rooted CLI search over stored trees,
replay-stable fingerprinting, upstream wgpu-acceleration revision
compatibility, LAB-owned replay-journaled split-stage recovery, and
layer-parallel block-construction evolution in
`docs/specs/lexonarchivebuilder-indexer/requirements.md`.

## Scope

This document specifies the LexonArchiveBuilder-owned design for realizing the approved
indexer requirements, including the email-ingestion refinement from `.mail` and
`.mbox` mailbox sources to normalized email artifacts and chunk-level embedding
units plus the local filesystem block-store interoperability correction,
replay-based streaming delegated indexing adoption, stage-selectable execution,
standalone clustering input discovery, delegated published-profile adoption,
repository-pinned published-profile configuration, latest published-profile and
telemetry compatibility, upstream wgpu-acceleration revision compatibility, upstream regression
assessment, embedding-phase batch-progress observability,
replay-submission observability, streaming-status observability,
telemetry-count-semantics clarity, clustering-failure diagnostics,
rooted block-tree quality assessment with rooted TNN-recall diagnostics,
rooted CLI search over stored trees, replay-stable delegated item identity,
LAB-owned replay-journaled split-stage recovery, and layer-parallel delegated
block construction for the local/testing profile.

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
- CLI parsing, report rendering, and JSON artifact generation for the rooted
  block-tree quality tool
- rooted-corpus sampling, exact-versus-approximate neighbor comparison, and
  recall-report rendering for the rooted quality tool
- CLI parsing, query embedding generation, search execution, and result
  rendering for the rooted CLI search tool
- Docker Compose, container, and local test-environment artifacts that realize
  the approved MVP slice

### Unaffected artifacts

- MCP server search semantics
- LexonGraph indexing internals
- LexonGraph block encoding and block identity contracts
- LexonGraph-owned block validity semantics beyond the repository-owned
  assessment heuristics and structural checks added in this increment
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
  clustering-only replay submission, streaming final materialization or
  block-assembly work, and delegated clustering failures
- able to assess the post-index quality of a rooted stored block tree without
  introducing a second storage abstraction or an MCP-visible operator surface
- able to execute ad hoc rooted CLI search over stored trees without
  redefining MCP search-serving behavior or introducing a second search corpus
  model
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
candidate set from a repository-owned replay-input source that is valid for the
configured store snapshot.

When the selected environment exposes a LAB-managed local filesystem block-store
root, the preferred source is a LAB-owned replay journal emitted during
successful leaf persistence. When that journal is absent, incompatible,
incomplete, or intentionally unavailable because the store was rebuilt without
journal continuity, the runtime falls back to iterating the configured
`BlockStore` through the upstream LexonGraph block-iteration API.

LexonArchiveBuilder treats the approved replay-journal contract plus the upstream
iteration contract as the authorities for which stored inputs are
clustering-eligible. Repository-owned artifacts that are not surfaced by the
approved replay-input surface remain outside the standalone clustering input
set.

Standalone clustering therefore operates over all clustering-eligible blocks
visible in the configured store snapshot at invocation time rather than over a
request-local summary artifact.

**Traces to:** RQ-INDEXER-003E, RQ-INDEXER-003E1, RQ-INDEXER-003E2,
RQ-INDEXER-010A

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

**Traces to:** RQ-INDEXER-003A, RQ-INDEXER-003E, RQ-INDEXER-003E1,
RQ-INDEXER-004F

### DSG-LFI-001F1 `Replay-journal write and growth discipline`

For environments that expose a LAB-managed local filesystem block-store root,
LexonArchiveBuilder realizes the repository-owned replay staging artifact as a
durable replay journal whose committed records are append-only and emitted only
after the corresponding replayable leaf output has been durably persisted
through the approved storage boundary.

The journal is optimized for sequential append and sequential replay rather than
random in-place mutation. Growth is controlled through bounded journal segments
or equivalent rollover units so large corpora do not require one unbounded
monolithic file.

Crash recovery treats a partial trailing record as incomplete local progress and
preserves all earlier committed records as valid replay state.

The specification intentionally leaves the first compact binary encoding choice
open at the design layer boundary, provided the implementation preserves the
append-only, low-overhead, and segmentable requirements approved upstream of
this document.

**Traces to:** RQ-INDEXER-003E1, RQ-INDEXER-003E2, RQ-INDEXER-008

### DSG-LFI-001G `Published-profile planning seam`

For any execution stage that includes clustering, LexonArchiveBuilder resolves
one explicit upstream published indexing profile before the first streaming
planning pass or standalone clustering replay begins.

For this increment, the selected profile is the upstream published profile
version `0.1.0`.

LexonArchiveBuilder treats the upstream LexonGraph streaming indexer as the
authority for the planning, packing, hierarchy, summary, and clustering
semantics bundled into that published profile and does not reconstruct an
equivalent repository-local planning-policy configuration from retired
low-level controls.

The selected published profile version remains fixed for the lifetime of one
batch invocation so replay passes, planning completion, and final
materialization do not observe intra-run clustering-configuration drift.

**[KNOWN]:** The upstream published-profile surface exposes this increment's
approved contract through `PublishedProfileVersion`, the
`PUBLISHED_PROFILE_V0_1_0` constant, and the higher-level
`with_published_profile(...)` construction path.

**Traces to:** RQ-INDEXER-003F, RQ-INDEXER-008, RQ-INDEXER-010A

### DSG-LFI-001H `Published-profile contract normalization`

LexonArchiveBuilder normalizes clustering-enabled invocation state into the
approved published-profile contract before invoking the delegated streaming
indexer.

In this increment, that normalization means:

- clustering-enabled execution resolves to the repository-pinned published
  profile version `0.1.0`
- no repository-local clustering mode, clustering algorithm, `cluster_count`,
  or algorithm-specific tuning values are forwarded as active upstream planning
  inputs
- retired low-level clustering controls are rejected explicitly if supplied by
  stale automation rather than being silently ignored

Because the published profile owns clustering cardinality and related planning
parameters, LexonArchiveBuilder does not perform repository-local omitted-option
auto-sizing or override merging for clustering-enabled execution in this
increment. Any future variation in those values must come from approval of a
different published profile version rather than ad hoc repository-local tuning.

**Traces to:** RQ-INDEXER-003F, RQ-INDEXER-003G, RQ-INDEXER-003H,
RQ-INDEXER-008, RQ-INDEXER-010A

### DSG-LFI-001I `Latest-upstream compatibility and regression boundary`

LexonArchiveBuilder treats the latest upstream published-profile and
status-observer telemetry surfaces as a mechanical adaptation boundary, not as
permission to narrow the approved repository contract.

For this increment, that boundary is the LexonGraph dependency refresh to commit
`70a80a2b51b41759217eec05086cb76586c4f1a5`, which is intended to pick up
upstream wgpu acceleration without changing the repository-visible
published-profile, CLI, request-schema, or MCP-facing contracts.

The design therefore preserves these repository-required capabilities across the
upgrade whenever the latest upstream contract still supports them semantically:

- the external stage contract
- deterministic split-stage replay
- adoption of the published-profile API for clustering-enabled execution
- repository pinning to published profile `0.1.0`
- retirement of the old low-level clustering control family from the approved
  external contract
- repository-owned progress projection over upstream lifecycle events
- projection of richer live hierarchy-stage telemetry and heartbeat events onto
  that same repository-owned progress surface
- unchanged MCP search-serving behavior for already-indexed content
- dependency-pin-only adoption of upstream wgpu acceleration at commit
  `70a80a2b51b41759217eec05086cb76586c4f1a5`

If any of those capabilities proves unavailable on the latest upstream surface,
the implementation must surface that as an explicit compatibility finding or
upstream regression rather than silently deleting the affected repository-owned
behavior.

**Traces to:** RQ-INDEXER-003F, RQ-INDEXER-003I, RQ-INDEXER-009, RQ-INDEXER-010A

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
input from the configured store snapshot through the separate standalone
clustering-discovery seam plus replay-staging seam, which prefers the
repository-owned replay journal and only falls back to whole-store discovery for
compatibility cases.

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

### DSG-LFI-002C `Clustering failure diagnostics`

LexonArchiveBuilder realizes diagnosable clustering failures through one
repository-owned top-level clustering-attempt snapshot plus one narrower
failure-subset snapshot when the upstream failure surface exposes a more precise
failing partition or subproblem.

The top-level clustering-attempt snapshot records:

- the selected execution stage
- the active embedding specification
- the block-size target
- the selected published profile version
- any upstream profile-resolved delegated configuration identifiers needed to
  explain the failed attempt
- the exact repository-visible clustering input set for the attempt using
  stable identifiers such as child block identifiers, replay-item identities,
  or equivalent repository-owned logical node identifiers
- compact embedding-health evidence for the failed attempt, including summary
  statistics and counts that can distinguish zero vectors, repeated vectors,
  non-finite values, or collapsed variance without requiring a full raw-vector
  dump
- a small repository-visible suspicious-input sample tied to that
  embedding-health evidence so operators can inspect representative bad cases

When the upstream failure surface exposes a narrower failing partition or
subproblem, or when LexonArchiveBuilder can otherwise prove a narrower
repository-visible subset was active at the failing step, the same failure
record also carries a failing-subset snapshot that records:

- the exact failing partition or otherwise the narrowest provable
  repository-visible subset active at the failing step
- the embedding-health evidence and suspicious-input sample for that narrower
  subset
- enough linkage to correlate the narrower failing subset back to the enclosing
  top-level clustering attempt

If the upstream failure surface does not expose an exact failing partition,
LexonArchiveBuilder still records the narrowest repository-visible subset it can
prove was active at the failing step rather than dropping back to top-level-only
diagnostics.

On a clustering failure, the runtime renders the same failure record onto the
normal batch log stream and serializes it to one request-adjacent diagnostic
artifact. The artifact location follows one repository-owned output policy: use
the `--summary-out` directory when present, otherwise the `--request` file
directory.

This design keeps failure diagnosability inside the short-lived batch runtime.
It does not introduce a new control plane, metrics surface, or MCP-visible
diagnostic API, and it does not require the same verbose clustering-input
inventory on successful runs.

The design intentionally keeps full raw embedding vectors out of the normal
failure artifact. For this increment, compact embedding-health evidence plus a
small suspicious-input sample is the repository-owned boundary for diagnosing
degenerate-embedding failures without making the artifact unmanageably large.

If writing the request-adjacent artifact fails, the runtime still emits the
clustering-attempt snapshot on the normal log stream together with the original
clustering failure so diagnosability does not depend on artifact persistence.

**Traces to:** RQ-INDEXER-003E, RQ-INDEXER-003F, RQ-INDEXER-008C,
RQ-INDEXER-010

### DSG-LFI-002D `Rooted block-tree quality reporting`

LexonArchiveBuilder realizes post-index block-tree assessment as one CLI-only
operator flow that starts from a caller-supplied root block identifier, walks
the reachable tree through the configured `BlockStore`, and emits one
human-readable summary plus one machine-readable JSON report for that rooted
snapshot.

The reporting flow separates findings into two repository-owned classes:

- structural-correctness findings for hard tree-shape violations such as a
  reachable child whose level is not lower than its parent
- embedding-space quality statistics for advisory signals about cohesion,
  separation, PCA-axis strength, quantile occupancy behavior, and split
  effectiveness across the represented embedding region

The report contains both aggregate rooted-tree evidence and per-block evidence.
That evidence includes quantitative shape or spread measurements, but the
design intentionally does not freeze one metric family in the specification
layer while `Q-INDEXER-065` remains open. The fixed design boundary is that the
same metric outputs appear in the human-readable summary and JSON artifact with
severity labeling that distinguishes hard structural failures from advisory
quality statistics.

The same rooted report family also carries rooted retrieval-quality evidence
through TNN-recall. That evidence is mode-tagged so corpus-based quality
evaluation remains distinguishable from optional user-query diagnostics in both
the human-readable summary and the JSON report.

One required repository-owned heuristic in this increment compares a child's
centroid-distance spread against its parent's corresponding spread. The design
therefore requires the report to preserve enough parent-and-child quantitative
evidence to determine how often children exceed their parent's corresponding
spread, but this evidence is aggregated into split-effectiveness statistics
rather than emitted as per-pair warning findings.

The required repository-owned quality model for this increment includes:

- per-block mean distance from centroid as the base intra-block dispersion input
- per-layer mean and standard deviation of those block-level dispersion values
- per-layer mean and standard deviation of sibling centroid-to-centroid
  distances
- per-block first-principal-component variance fraction with per-layer mean and
  standard deviation aggregation
- per-block quantile-bin occupancy counts plus occupancy variance, empty-bin
  detection, and overfull-bin detection using a repository-defined default bin
  count, where an overfull bin is one whose occupancy exceeds two times the
  expected occupancy for the block's selected quantile partition
- per-parent split-effectiveness statistics covering the percentage of children
  whose dispersion exceeds the parent's plus the mean and maximum increase for
  those children

This design keeps quality interpretation statistical and advisory. Structural
violations remain the only repository-owned hard findings, while the parent- to-
child dispersion heuristic contributes to aggregate quality evidence rather than
to emitted warning records.

This flow is post-index analysis only. It does not change delegated index
construction behavior, redefine LexonGraph validity rules, or introduce an
MCP-visible diagnostics surface.

**Traces to:** RQ-INDEXER-008D, RQ-INDEXER-008D3, RQ-INDEXER-009, RQ-INDEXER-010

### DSG-LFI-002D1 `Rooted corpus TNN-recall flow`

LexonArchiveBuilder realizes corpus-based TNN-recall as a post-index quality flow
over the same rooted snapshot used by the block-tree assessment.

The reachable embedding set under the caller-supplied root is the evaluation
corpus. The flow draws a uniform random sample of query embeddings from that
rooted corpus using a caller-selectable sample size, a reproducible seed, and
a caller-selectable traversal width for the approximate-neighbor path, then
computes Recall@1, Recall@5, and Recall@10 for each sampled query.

For each sampled query, the design compares:

- exact nearest neighbors computed against the rooted corpus itself
- approximate nearest neighbors returned through the repository's approved
  rooted retrieval path over that same rooted snapshot using the
  caller-selected traversal width

Aggregate outputs such as mean recall, recall standard deviation, and recall
histograms are derived only from this corpus-based mode. This keeps automated
quality evaluation statistical, reproducible, and scoped to one rooted tree
rather than to the entire configured block store. The selected traversal width
is carried in the emitted recall artifact so experiment results remain
traceable.

**Traces to:** RQ-INDEXER-008D1, RQ-INDEXER-008D3, RQ-INDEXER-010

### DSG-LFI-002D2 `User-query diagnostic recall mode`

LexonArchiveBuilder may also realize an optional diagnostic recall mode for one
or more user-supplied query embeddings over the same rooted snapshot.

This mode reuses the same exact-neighbor and approximate-neighbor comparison
boundaries as corpus-based recall, but it remains explicitly diagnostic:

- the result is labeled `diagnostic recall`
- exact and approximate neighbors are emitted side by side for operator
  comparison
- the result is excluded from aggregate recall statistics, histograms, and any
  automated quality verdicts

This keeps one-off debugging evidence available without letting ad hoc queries
distort the repository-owned rooted-quality metric.

**Traces to:** RQ-INDEXER-008D2, RQ-INDEXER-008D3

### DSG-LFI-002E `Rooted CLI search flow`

LexonArchiveBuilder realizes rooted operator search as one CLI-only flow that:

1. accepts one caller-provided text query
2. generates one query embedding through a caller-provided embedding endpoint
3. searches one caller-supplied rooted tree through `lexongraph-search`
4. returns the top `k` matching leaf nodes
5. renders one human-readable result summary plus one machine-readable JSON
   result artifact for the same invocation

This flow is additive to the existing MCP server search capability. It exists
for operator convenience and automation over an already-persisted rooted tree;
it does not change the MCP request surface, query semantics, or retrieval
contract.

The design keeps the search algorithm subordinate to `lexongraph-search`.
LexonArchiveBuilder owns CLI orchestration, rooted invocation shaping, output
rendering, and boundary adaptation only.

**Traces to:** RQ-INDEXER-008E, RQ-INDEXER-009, RQ-INDEXER-010A

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

### DSG-LFI-005B `Rooted assessment traversal through BlockStore`

The rooted block-tree quality tool traverses stored tree structure exclusively
through the same environment-selected `BlockStore` abstraction family used by
the indexing pipeline.

The assessment reads the caller-selected root block through that boundary,
discovers reachable descendants by following stored parent-child references, and
limits all findings and aggregates to the reachable rooted snapshot rather than
to every block present in the store.

The same rooted traversal supplies the per-layer grouping needed for cohesion,
separation, PCA-axis-strength, quantile-occupancy, and split-effectiveness
statistics. The same rooted traversal also defines the exact embedding corpus
used by rooted TNN-recall, so repository-owned aggregation stays rooted-
snapshot-local rather than mixing data from unrelated stored trees.

This design keeps assessment logic backend-neutral across local filesystem and
the preserved production storage profile. It also prevents the repository from
introducing a second storage-reader stack with different reachability or
decoding semantics than the indexing path already uses.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-008D, RQ-INDEXER-010

### DSG-LFI-005C `Rooted search boundary through BlockStore`

The rooted CLI search tool scopes search to one caller-supplied root block and
the reachable rooted tree under that block through the same configured
`BlockStore` abstraction family used by the indexing pipeline.

LexonArchiveBuilder therefore does not construct a second repository-local
search corpus description or a parallel manifest of searchable nodes. The
reachable rooted tree is the authority for which stored leaf nodes may appear in
search results for one invocation.

This preserves backend-neutral search orchestration across local filesystem and
the preserved production storage profile while keeping traversal semantics
aligned with the same stored tree boundary used by rooted quality assessment.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-008E, RQ-INDEXER-010

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

### DSG-LFI-006A `Query embedding generation boundary`

For rooted CLI search, LexonArchiveBuilder generates the query embedding through
one caller-provided embedding endpoint that remains compatible with the same
OpenAI-compatible embedding boundary family used elsewhere in the repository.

This design keeps endpoint selection operator-driven at CLI time without
requiring Rust code edits for each query target. The specification layer does
not require the CLI to expose every possible embedding-spec override while
`Q-INDEXER-068` remains open; the fixed boundary is that query-embedding
generation stays subordinate to the repository's existing embedding-provider
family rather than inventing a search-only embedding protocol.

**Traces to:** RQ-INDEXER-006, RQ-INDEXER-008E, RQ-INDEXER-010

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

### DSG-LFI-007C `Published-profile CLI surface`

LexonArchiveBuilder exposes clustering-enabled execution through the existing
`run` command alongside the existing request-file and stage-selection surface,
but this increment retires the prior low-level clustering-configuration flag
family.

The approved operator-facing behavior is:

- callers continue to choose whether clustering runs by selecting the execution
  stage
- any clustering-enabled stage resolves to the repository-pinned published
  profile version `0.1.0`
- the request-file-driven runtime shape remains preserved for batch items,
  environment selection, and stage selection
- any legacy low-level clustering flags that remain in old automation fail
  validation explicitly so operators are not misled into believing they still
  tune active upstream planning behavior

This increment therefore does not require a persistent profile-selection field
in `BatchRequest`; the approved contract is the repository-pinned profile
version rather than a caller-selected profile matrix.

**Traces to:** RQ-INDEXER-003F, RQ-INDEXER-003G, RQ-INDEXER-007,
RQ-INDEXER-010

### DSG-LFI-007D `Block-tree quality CLI surface`

LexonArchiveBuilder exposes the rooted block-tree quality assessment through a
dedicated CLI-only operator surface that accepts:

- the same environment-selected block-store configuration family used by the
  indexer runtime
- one root block identifier to analyze
- optional rooted-corpus recall controls, including sample-size, seed, and
  traversal-width inputs for corpus-based TNN-recall
- optional diagnostic-query inputs when the operator wants one-off
  query-specific recall evidence
- one optional artifact destination for the JSON report when the default output
  location is insufficient

The CLI surface is intentionally separate from the batch `run` request-file
contract because the assessment analyzes an already-persisted rooted tree rather
than orchestrating a new indexing batch. The design does not require MCP
exposure, request-file integration, or a long-lived reporting service in this
increment.

The CLI renders a concise human-readable summary to the operator-facing output
stream and writes the full machine-readable report artifact for downstream
automation or offline analysis. The operator surface does not expose the
quantile-bin count in this increment; that remains a repository-defined default
behind the quality-tool boundary. The same surface must clearly identify whether
reported recall evidence came from rooted-corpus sampling or from optional
diagnostic-query execution.

**Traces to:** RQ-INDEXER-008D, RQ-INDEXER-008D1, RQ-INDEXER-008D2, RQ-INDEXER-008D3, RQ-INDEXER-009

### DSG-LFI-007E `Rooted CLI search surface`

LexonArchiveBuilder exposes the rooted CLI search capability through a dedicated
operator surface that accepts:

- one query text string
- one caller-provided embedding endpoint
- one caller-provided root block identifier
- one `k` value for the number of matching leaf nodes to return
- one optional artifact destination when the default JSON output location is
  insufficient

The CLI surface is intentionally separate from the batch `run` request-file
contract because the tool operates on an already-persisted rooted tree rather
than orchestrating a new indexing batch. The design does not require MCP
exposure or request-file integration in this increment.

The CLI renders one concise human-readable result set for immediate operator use
and writes the full machine-readable JSON result output for downstream
automation.

**Traces to:** RQ-INDEXER-008E, RQ-INDEXER-009

### DSG-LFI-008 `Local and production parity boundary`

Local/testing and production environments differ only in adapter realization and
provider configuration, not in the container's batch contract, the staged email
artifact model, content item shape, the stage-selection and concurrency-
configuration surfaces, the clustering-selection and clustering-option CLI
surface, the rooted block-tree quality CLI surface, or the delegated
`lexongraph-streaming-indexer` orchestration contract. The same parity boundary
also covers the rooted CLI search surface's use of configured storage plus one
operator-supplied embedding endpoint.

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

That separation includes the rooted block-tree quality tool: it remains a CLI
operator capability and does not become an MCP query, retrieval, or reporting
surface in this increment.

The same separation applies to the rooted CLI search tool: it remains additive
to MCP search and does not become the new definition of repository search
semantics.

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
set is expected to produce the same logical clustering result under unchanged
upstream semantics whether replay inputs come from a valid replay journal or
from compatibility-mode whole-store iteration.

The same stability expectation applies to clustering configuration resolution:
repeating a clustering-enabled run with the same approved published profile
version must resolve to the same effective upstream planning behavior.

For rooted block-tree quality assessment, the comparable invariant is rooted
snapshot determinism: repeated assessment over the same reachable rooted block
tree and the same quantitative metric configuration is expected to produce the
same structural findings and the same quantitative quality evidence.

For rooted CLI search, the comparable invariant is rooted-query determinism:
repeating the same query text, rooted tree, embedding endpoint behavior, and
`k` against the same stored snapshot is expected to produce the same ranked leaf
result set under unchanged subordinate `lexongraph-search` semantics.

**Traces to:** RQ-INDEXER-003B, RQ-INDEXER-003C, RQ-INDEXER-008,
RQ-INDEXER-003E, RQ-INDEXER-003F, RQ-INDEXER-003G, RQ-INDEXER-008D,
RQ-INDEXER-008E,
RQ-INDEXER-010A

## Verification Realization

### DSG-LFI-011 `Repository verification scope`

LexonArchiveBuilder-owned verification artifacts validate:

- correct delegation to `lexongraph-streaming-indexer`
- correct use of the replay-based streaming indexing seam
- correct stage-selectable execution across CLI and request-file invocation
  without exposing the raw upstream lifecycle
- correct leaf-layer concurrency scheduling with cross-layer barriers
- correct standalone clustering input discovery through the repository-owned
  replay journal with upstream block-iteration fallback
- correct adoption of the upstream published-profile API with repository pinning
  to profile `0.1.0` and explicit rejection of retired low-level clustering
  controls
- correct deterministic replay staging and replay-stable content fingerprinting
- correct replay-journal append, rollover, and crash-tolerance behavior
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
- correct failure-only clustering diagnostics that identify the failed input
  set and effective delegated clustering configuration on both required
  surfaces
- correct rooted block-tree quality assessment over the shared `BlockStore`
  boundary, including separation of structural findings from advisory
  embedding-space heuristics and emission of both required output surfaces
- correct rooted CLI search over the shared `BlockStore` boundary, including
  query embedding generation through the approved endpoint family, subordinate
  use of `lexongraph-search`, rooted result scoping, and emission of both
  required output surfaces
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
RQ-INDEXER-004F, RQ-INDEXER-008A, RQ-INDEXER-008B, RQ-INDEXER-008C,
RQ-INDEXER-008D, RQ-INDEXER-008E,
RQ-INDEXER-010A, RQ-INDEXER-010B, RQ-INDEXER-010, DSG-LFI-001A,
DSG-LFI-001B, DSG-LFI-001C, DSG-LFI-001D, DSG-LFI-001E, DSG-LFI-001F,
DSG-LFI-001G, DSG-LFI-001H, DSG-LFI-001I, DSG-LFI-002A, DSG-LFI-002B,
DSG-LFI-002C, DSG-LFI-002D, DSG-LFI-004G, DSG-LFI-005A, DSG-LFI-005B,
DSG-LFI-005C, DSG-LFI-006A, DSG-LFI-007A, DSG-LFI-007B, DSG-LFI-007C,
DSG-LFI-007D, DSG-LFI-007E
