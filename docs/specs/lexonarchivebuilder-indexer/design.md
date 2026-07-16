<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder Indexer Design

## Status

Specification patch for the approved email-artifact, chunk-level
indexing, local filesystem block-store interoperability, replay-based
streaming delegated indexing, stage-selectable execution, standalone
clustering input discovery, mutable current-root publication, published-profile API adoption,
published-profile version selection, latest published-profile and
telemetry compatibility, upstream regression assessment,
replay-submission and streaming-status observability,
clustering-failure diagnostics, rooted block-tree quality assessment with
rooted TNN-recall diagnostics, rooted query access-cost reporting, rooted CLI search over stored trees,
replay-stable fingerprinting, temporary upstream `main` tracking for
rapid profile validation, upstream wgpu-acceleration revision
compatibility, 0.6.x published-profile evaluation, local testing sweep
automation, v0.7.0 fixed-budget ladder experiment automation, rooted
block-store copy tooling, upstream embedding-readback API adoption, LAB-owned
replay-journaled split-stage recovery, in-memory replay block-id ordering
simplification, and layer-parallel
block-construction evolution, and v2 custom-block adoption for repository-owned
non-search artifacts in
`docs/specs/lexonarchivebuilder-indexer/requirements.md`.

## Scope

This document specifies the LexonArchiveBuilder-owned design for realizing the approved
indexer requirements, including the email-ingestion refinement from `.mail` and
`.mbox` mailbox sources to normalized email artifacts and chunk-level embedding
units plus the local filesystem block-store interoperability correction,
replay-based streaming delegated indexing adoption, stage-selectable execution,
standalone clustering input discovery, delegated published-profile adoption,
caller-selectable published-profile configuration with default `0.1.0`,
latest published-profile and telemetry compatibility, temporary upstream
`main` tracking for rapid profile validation, upstream
wgpu-acceleration revision compatibility, upstream regression assessment,
0.6.x published-profile evaluation, local testing sweep automation,
v0.7.0 fixed-budget ladder experiment automation, rooted block-store copy tooling,
upstream embedding-readback API adoption, embedding-phase
batch-progress observability,
replay-submission observability, streaming-status observability,
telemetry-count-semantics clarity, clustering-failure diagnostics,
rooted block-tree quality assessment with rooted TNN-recall diagnostics,
rooted query access-cost reporting,
rooted CLI search over stored trees, replay-stable delegated item identity,
LAB-owned replay-journaled split-stage recovery, in-memory replay block-id
ordering for deterministic clustering replay, and layer-parallel delegated
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
- CLI parsing, rooted traversal, copy accounting, and machine-readable result
  emission for the rooted block-copy operator tool
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
- able to copy rooted immutable block graphs between approved block-store
  targets without redefining block identity, mutable-reference publication, or
  MCP behavior
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

### DSG-LFI-001A1 `Fixed-memory replay adapter`

LexonArchiveBuilder realizes the repository-owned replay adapter so resident
memory stays bounded by a caller-configurable budget even when the indexed
corpus is larger than RAM.

The repository-owned runtime therefore avoids corpus-scale in-memory retention
of replay-item inventories, mailbox-or-document expansion state, replay batches,
or stored-embedding maps. Instead, it advances through the approved upstream
lifecycle using streaming or segmented replay shapes whose live working set is
bounded independently of total corpus size.

For replay-journal-driven deterministic ordering, the approved retained state is
the unique raw block-id list plus any fixed-size per-block journal-integrity
digests needed to validate replay metadata against referenced payload blocks.
That retained state remains limited to hash identities and fixed-size digests
rather than decoded blocks, embeddings, or equivalent payload state.

This entry constrains repository-owned orchestration only. It does not redefine
opaque upstream-owned model state, but it does require the adapter layer to
surface any upstream incompatibility with bounded-memory replay as an explicit
compatibility finding rather than normalizing unbounded retention in-repo.

**Traces to:** RQ-INDEXER-003A1, RQ-INDEXER-010A

### DSG-LFI-001A2 `In-memory raw block-id ordering strategy`

LexonArchiveBuilder realizes replay-journal-driven deterministic ordering for
this increment as an in-memory raw block-id list with aligned fixed-size
journal-integrity digests rather than as an externalized ordering catalog.

The runtime walks the immutable replay-audit journal, extracts recorded block
ids, sorts them, dedupes them, and uses that unique block-id order for later
classification and finalization. When replay-metadata validation requires it,
the runtime retains one fixed-size digest per ordered block so later payload
reads can prove the replay-journal record still matches the referenced block.

That replay walk reads replay-audit blocks and their recorded ids only. It does
not dereference referenced payload blocks until later processing needs them,
and it does not introduce SQLite, spill files, or equivalent repository-owned
externalized ordering storage.

**Traces to:** RQ-INDEXER-003A2, RQ-INDEXER-003E, RQ-INDEXER-003E1, RQ-INDEXER-003E3

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

LexonArchiveBuilder treats the repository-owned immutable replay-audit journal as
the authoritative replay-input source for standalone clustering.

The runtime discovers the current journal head through the approved mutable
reference mechanism, traverses the immutable journal chain from that head, and
reconstructs clustering-eligible replay inputs only from entries surfaced by
that chain. Repository-owned artifacts that are not surfaced by the approved
replay-audit input surface remain outside the standalone clustering input set.

Standalone clustering therefore operates over all clustering-eligible blocks
visible through the selected journal head rather than over a request-local
summary artifact or a whole-store block scan.

For this increment, replay discovery remains journal-only: it extracts recorded
block ids from the replay-audit chain, sorts them, dedupes them, and uses that
unique block-id order as the deterministic processing order before any payload
block dereference occurs.

**Traces to:** RQ-INDEXER-003E, RQ-INDEXER-003E1, RQ-INDEXER-003E2,
RQ-INDEXER-003E3, RQ-INDEXER-010A

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
It also does not authorize keeping the entire replay set resident in memory:
the staging shape must support bounded-memory replay preparation and
finalization handoff under large-corpus operation.

**Traces to:** RQ-INDEXER-003A, RQ-INDEXER-003E, RQ-INDEXER-003E1,
RQ-INDEXER-003E4, RQ-INDEXER-004F

### DSG-LFI-001F1 `Immutable replay-audit publication discipline`

LexonArchiveBuilder realizes the repository-owned replay staging artifact as a
shared-`BlockStore` immutable replay-audit journal in both local/testing and
production-oriented environments.

The runtime accumulates bounded completed-work audit entries until the active
journal payload crosses the approved size-oriented threshold, then publishes one
immutable journal block that:

1. contains the grouped audit entries for that completed work chunk
2. identifies the predecessor journal block by hash when a predecessor exists
3. becomes the new authoritative replay point only after the journal block
   itself is durably persisted

Crash recovery treats unpublished in-memory or otherwise incomplete local
progress as non-authoritative and preserves the previously published journal
head plus its immutable predecessor chain as valid replay state.

The specification intentionally leaves the exact compact encoding and the exact
size threshold open at the design layer boundary, provided the implementation
preserves append-only immutable publication, low-overhead operation, and bounded
redo cost.

**Traces to:** RQ-INDEXER-003E1, RQ-INDEXER-003E2, RQ-INDEXER-008

### DSG-LFI-001F2 `Mutable replay-journal head discovery`

LexonArchiveBuilder publishes the latest immutable replay-audit journal head
through a repository-owned mutable reference mechanism.

That reference is the discovery point for later ingestion resume and
clustering-only replay. Updating the head changes which immutable audit chain is
authoritative, but it does not mutate any previously published journal block.

This keeps replay discovery aligned with the repository's mutable
reference pattern for current-root publication instead of relying on request-
local state or block-store scanning heuristics.

The caller supplies the ref name, and the runtime maps that name to one
human-readable JSON ref artifact at `refs/{ref_name}`. That artifact is the
unit of mutable publication; different ref names produce different ref files or
blob paths rather than sharing one repository-global mutable JSON document.

**Traces to:** RQ-INDEXER-003E3, RQ-INDEXER-010

### DSG-LFI-001F3 `Replay-audit entry coverage`

Each immutable replay-audit journal block stores repository-owned audit entries
that are detailed enough to reconstruct what completed work occurred.

For every recorded work entry, the design preserves at least:

- the relevant input item, artifact, or predecessor block identities
- the repository-owned action or step kind that completed
- the generated block identities or equivalent durable output artifacts

This keeps the journal useful both for deterministic replay reconstruction and
for later audit or diagnosis without redefining LexonGraph-owned semantics for
the delegated blocks themselves.

**Traces to:** RQ-INDEXER-003E4, RQ-INDEXER-010A

### DSG-LFI-001F4 `Mutable current-root publication`

When a successful execution stage materializes a new final root, the runtime
publishes that immutable root identity through the same repository-owned
mutable reference mechanism class used for replay-journal head discovery.

The mutable current-root reference is updated only after the new immutable root
is already valid and durable under the selected `BlockStore` boundary. Stages
that do not materialize a new final root leave the existing current-root
reference unchanged.

The `refs/{ref_name}` JSON payload carries the latest replay-journal head block
id, the latest successfully materialized root block id when present, and
publication metadata such as the effective profile version and stage label.

This preserves the existing `BatchSummary` final-root contract while adding a
durable repository-owned discovery surface for later invocations and operator
workflows.

**Traces to:** RQ-INDEXER-003D, RQ-INDEXER-003E5, RQ-INDEXER-010

### DSG-LFI-001G `Published-profile planning seam`

For any execution stage that includes clustering, LexonArchiveBuilder resolves
one explicit upstream published indexing profile before the first streaming
planning pass or standalone clustering replay begins.

The resolved profile version comes from the approved caller-visible
profile-selection contract; when omitted, it defaults to upstream published
profile version `0.1.0`.

LexonArchiveBuilder treats the upstream LexonGraph streaming indexer as the
authority for the planning, packing, hierarchy, summary, and clustering
semantics bundled into that published profile and does not reconstruct an
equivalent repository-local planning-policy configuration from retired
low-level controls.

The selected published profile version remains fixed for the lifetime of one
batch invocation so replay passes, planning completion, and final
materialization do not observe intra-run clustering-configuration drift.

**[KNOWN]:** The upstream published-profile surface exposes this increment's
approved contract through `PublishedProfileVersion`, the current default
constant `PUBLISHED_PROFILE_V0_1_0`, and the higher-level
`with_published_profile(...)` construction path.

When the temporarily tracked upstream `main` branch publishes additional
profile versions in the active `0.6.x` experiment series, LexonArchiveBuilder
refreshes its adopted dependency state so that the same selector surface can
target those versions immediately, without changing the repository default
away from `0.1.0`. Earlier `0.5.x` alignment remains prior comparison context
for evaluation, while `0.4.x` remains historical context for older
experiments, not the current named selector target.

**Traces to:** RQ-INDEXER-003F, RQ-INDEXER-008, RQ-INDEXER-010A

### DSG-LFI-001H `Published-profile contract normalization`

LexonArchiveBuilder normalizes clustering-enabled invocation state into the
approved published-profile contract before invoking the delegated streaming
indexer.

In this increment, that normalization means:

- clustering-enabled execution resolves to one selected published profile
  version, defaulting to `0.1.0` when the caller omits the selector
- refreshing the adopted upstream dependency state may add newly published
  selector targets in the active `0.6.x` series, but does not change
  omitted-selector behavior unless a later approved increment changes the
  default explicitly
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

The one approved exception is the repository-local `0.7.0` fixed-budget ladder
automation surface. That operator aid may realize one deterministic rung table
that pairs a selected beam width with a selected clustering cardinality for
local/testing experiment execution only. This exception remains outside the
ordinary batch contract: it does not redefine `BatchRequest`, does not expose a
general low-level clustering-tuning family, and does not alter production or
MCP-facing behavior.

**Traces to:** RQ-INDEXER-003F, RQ-INDEXER-003G, RQ-INDEXER-003H,
RQ-INDEXER-003J1, RQ-INDEXER-008, RQ-INDEXER-010A

### DSG-LFI-001I `Latest-upstream compatibility and regression boundary`

LexonArchiveBuilder treats the latest upstream published-profile and
status-observer telemetry surfaces as a mechanical adaptation boundary, not as
permission to narrow the approved repository contract.

For this increment, that boundary is temporary explicit tracking of the
LexonGraph `main` branch, which is intended to pick up newly published
profiles and upstream wgpu acceleration quickly without changing the
repository-visible MCP-facing contract or reintroducing low-level clustering
controls.

The design therefore preserves these repository-required capabilities across the
upgrade whenever the latest upstream contract still supports them semantically:

- the external stage contract
- deterministic split-stage replay
- adoption of the published-profile API for clustering-enabled execution
- defaulting to published profile `0.1.0` while permitting explicit selection
  of another upstream-published profile version for evaluation
- refreshing the adopted upstream dependency state so newly published versions
  in the active `0.6.x` series become selectable without redefining the
  repository default, while retaining earlier `0.5.x` alignment as prior
  comparison context and `0.4.x` alignment as historical context
- retirement of the old low-level clustering control family from the approved
  external contract
- repository-local local/testing automation that reuses the approved batch and
  rooted-quality surfaces to sweep the current profile experiment set without
  per-profile code edits
- repository-owned progress projection over upstream lifecycle events
- projection of richer live hierarchy-stage telemetry and heartbeat events onto
  that same repository-owned progress surface
- unchanged MCP search-serving behavior for already-indexed content
- temporary explicit tracking of upstream `main` to pick up new published
  profiles and wgpu acceleration quickly

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
clustering-discovery seam plus replay-staging seam, which uses the repository-
owned immutable replay-audit journal as the authoritative discovery surface.

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

The same rooted report family also carries rooted-query access-cost evidence for
the query workload the quality tool executes. That evidence includes per-query
and aggregate counts of unique touched blocks plus serialized bytes read, both
broken down by block level and summarized as overall totals for the executed
query set.

The same rooted report family also carries rooted retrieval-quality evidence
through TNN-recall. That evidence is mode-tagged so corpus-based quality
evaluation remains distinguishable from optional user-query diagnostics in both
the human-readable summary and the JSON report.

When this flow needs numerical embedding values from stored rooted branch
blocks, it does not decode those payloads through a repository-local
branch-encoding table. Instead, it treats LexonGraph as the authority for
supported branch encodings and reconstructs the logical floating-point vectors
through the upstream embedding readback API. Plain leaf payload decoding for
the currently supported stable encodings remains on the existing local path in
this increment.

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

**Traces to:** RQ-INDEXER-008D, RQ-INDEXER-008D3, RQ-INDEXER-008D4, RQ-INDEXER-009, RQ-INDEXER-010

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

**Traces to:** RQ-INDEXER-008D1, RQ-INDEXER-008D3, RQ-INDEXER-008D4, RQ-INDEXER-008D5, RQ-INDEXER-010

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

**Traces to:** RQ-INDEXER-008D2, RQ-INDEXER-008D3, RQ-INDEXER-008D4, RQ-INDEXER-008D5

### DSG-LFI-002D3 `Rooted-query access accounting and RTT-cost model`

LexonArchiveBuilder realizes rooted-query access-cost reporting by attaching
query-local traversal accounting to the same rooted retrieval path used for
corpus-based TNN-recall and any optional user-query diagnostic recall.

For each rooted query, the design records the set of unique block identities
touched by the approximate-neighbor path, groups those touched blocks by block
level, and derives per-level plus total serialized-byte counts from the encoded
block sizes visible through the shared `BlockStore` boundary. The same
accounting model then rolls those per-query measurements up into aggregate
statistics for the executed query set while preserving recall-mode separation
between corpus-based and optional diagnostic queries.

The report also derives an advisory RTT-style transport-cost estimate for each
query from that per-level byte accounting. For this increment the model is
fixed: each level contributes `ceil(level_bytes / 65536)` RTTs, and the query's
total RTT estimate is the sum of those rounded-up per-level contributions.

This model is intentionally repository-owned and advisory-only. It expresses
logical rooted-query read amplification in RTT units without claiming to predict
cache-hit behavior, retry effects, CPU cost, or wall-clock latency.

**Traces to:** RQ-INDEXER-008D4, RQ-INDEXER-008D5

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

Any repository-owned stored-embedding readback needed by this rooted operator
surface remains subordinate to the same upstream LexonGraph embedding readback
API rather than to a second repository-local decoder path.

**Traces to:** RQ-INDEXER-008E, RQ-INDEXER-009, RQ-INDEXER-010A

### DSG-LFI-002F `Upstream stored-embedding readback seam`

LexonArchiveBuilder treats stored embedding reconstruction as an upstream-owned
boundary whenever repository-owned quality, search, or diagnostic flows need
numerical vectors from persisted blocks.

This seam requires repository-owned consumers to:

- pass stored payloads plus any upstream-owned embedding metadata through the
  upstream LexonGraph readback API
- consume the reconstructed logical embedding values returned by that API for
  downstream distance, centroid, recall, or diagnostic calculations
- avoid maintaining a parallel repository-local matrix of supported stored
  encoding names and reconstruction rules

This keeps new stored encodings, such as EBCP-derived forms, behind one
upstream compatibility surface rather than forcing every downstream repository
tool to replicate format knowledge independently.

**Traces to:** RQ-INDEXER-008D, RQ-INDEXER-008E, RQ-INDEXER-010

### DSG-LFI-002G `Process-wide opt-in SDK diagnostic logging`

LexonArchiveBuilder realizes opt-in Azure SDK and HTTP-client diagnostics by
initializing one standard Rust logger for the entire
`lexonarchivebuilder-indexer` process during startup.

This design keeps diagnostic activation subordinate to the normal Rust logging
environment contract rather than inventing a repository-specific flag. When
`RUST_LOG` or an equivalent standard filter variable is unset, the process does
not emit extra SDK or transport diagnostics. When it is set, repository-owned
commands such as batch indexing, rooted quality, rooted search, and rooted copy
allow underlying Azure SDK and HTTP-client components that already log through
the Rust logging ecosystem to emit their diagnostics on the same short-lived
process output streams.

The logger initialization is process-wide rather than command-specific so the
same diagnostic activation path works across the entire binary. It does not add
a daemon, a second telemetry channel, or an MCP-visible diagnostics surface,
and it does not require LexonArchiveBuilder to wrap every upstream SDK call in
repository-specific tracing statements before operators can observe transport or
retry activity.

**Traces to:** RQ-INDEXER-005C

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

- read-only gateway fetch surfaces may additionally select the additive
  `gateway-http3` profile, which derives HTTPS-over-QUIC access from a gateway
  DNS host name and is approved only where read-only immutable block fetches are
  sufficient
- local/testing selects a filesystem-backed block store
- production-oriented operation selects either:
  - the existing `production` overlay block store composed of an in-memory
    cache layer, a local filesystem cache layer, and an Azure Blob backing
    layer addressed by SAS URL
  - the additive `production-v2` direct Azure-backed LexonGraph block-store
    implementation

The same environment-selected `BlockStore` abstraction family is reused for:

- delegated LexonGraph index blocks
- normalized email artifacts
- mailbox provenance artifacts

The rest of the LexonArchiveBuilder indexing flow consumes only the backend-neutral
`BlockStore` contract and does not depend on filesystem paths or Azure-specific
blob layout details.

The non-local target family is intentionally fixed to one approved
repository-defined profile set rather than a caller-assembled arbitrary stack
of storage adapters. This keeps tool-targeting semantics stable across batch
indexing, standalone clustering, rooted quality assessment, rooted CLI search,
rooted block copy, and future indexer-owned operator tools that traverse the
shared `BlockStore` boundary, while still allowing read-only surfaces to adopt
the additive `gateway-http3` profile without redefining writable profile
semantics.

For the approved increment, the local/testing block-store realization remains
required and executable, and both approved production-oriented storage profiles
remain part of the same preserved adapter seam and configuration family rather
than tool-specific exception paths.

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

### DSG-LFI-005A1 `V2 custom blocks for repository-owned artifacts`

LexonArchiveBuilder adopts LexonGraph v2 custom blocks for the repository-owned
non-search artifacts that it defines itself.

This applies to normalized email artifacts, mailbox provenance artifacts, and
similar repository-owned artifact blocks that are not delegated search tree
nodes.

The design intentionally does not introduce a repository-owned mixed-format
compatibility layer for those artifacts. Operators may rebuild local or
overlay-backed stores and regenerate repository-owned non-search artifacts under
the v2 custom-block contract rather than translating old artifact blocks in
place.

Delegated branch and leaf index blocks remain on the current upstream-owned
contract in this increment so LexonArchiveBuilder does not fork or wrap the
streaming indexer's branch-or-leaf hashing rules.

**Traces to:** RQ-INDEXER-005A, RQ-INDEXER-010A

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

The same `BlockStore`-bounded rooted retrieval path is also the authority for
rooted-query access accounting. Query access-cost reporting therefore reuses the
same reachable block identities, block levels, and encoded block sizes visible
through this boundary rather than inventing a parallel repository-local
transport model.

This design keeps assessment logic backend-neutral across local filesystem and
the approved non-local production storage profiles. It also prevents the
repository from introducing a second storage-reader stack with different
reachability or decoding semantics than the indexing path already uses.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-008D, RQ-INDEXER-008D4, RQ-INDEXER-008D5, RQ-INDEXER-010

### DSG-LFI-005C `Rooted search boundary through BlockStore`

The rooted CLI search tool scopes search to one caller-supplied root block and
the reachable rooted tree under that block through the same configured
`BlockStore` abstraction family used by the indexing pipeline.

LexonArchiveBuilder therefore does not construct a second repository-local
search corpus description or a parallel manifest of searchable nodes. The
reachable rooted tree is the authority for which stored leaf nodes may appear in
search results for one invocation.

This preserves backend-neutral search orchestration across local filesystem and
the approved non-local production storage profiles while keeping traversal
semantics aligned with the same stored tree boundary used by rooted quality
assessment.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-008E, RQ-INDEXER-010

### DSG-LFI-005D `Rooted block-copy traversal through BlockStore`

The rooted block-copy tool traverses immutable rooted block graphs through the
same configured `BlockStore` abstraction family on both its source and
destination sides.

LexonArchiveBuilder reads each caller-selected root from the source boundary,
discovers only the blocks reachable from those roots by following stored
references, and writes raw block bytes to the destination boundary without
re-encoding block payloads or introducing backend-specific transfer logic.

Because the transfer contract is hash-addressed immutable block identity, the
copy workflow treats destination preexistence as a normal condition: it may
check whether a destination block identity is already present and skip that
write while still counting the block as covered by the requested rooted copy.

This design intentionally excludes repository-owned mutable references such as
current-root and replay-journal-head publication. Those references remain
separate operator concerns even when the immutable rooted block content has been
copied successfully.

The same rooted traversal keeps the copy contract content-type-neutral and
backend-neutral across local filesystem plus the approved non-local production
profiles, while preserving the upstream ownership of block bytes and identity
semantics.

**Traces to:** RQ-INDEXER-005B, RQ-INDEXER-010, RQ-INDEXER-010A

### DSG-LFI-005E `Gateway-backed read-only profile applicability`

The additive `gateway-http3` profile realizes read-only immutable block access
through the separate `lexonarchivebuilder-block-store-http3` boundary.

That profile derives one HTTPS-over-QUIC authority from a caller-supplied
gateway DNS host name on port `443`, resolves immutable block fetches through
`/block/<block_id>`, maps gateway `404` responses to missing-block results, and
surfaces transport or other non-success responses as explicit backend failures.

Because the profile is read-only, tool surfaces may adopt it only where rooted
block fetches are sufficient. Rooted quality traversal, rooted CLI search
traversal, and rooted block-copy source traversal are representative approved
uses. Indexing-time writes, replay-journal publication, mutable current-root
publication, rooted-copy destination writes, and any flow that depends on
whole-store iteration remain on the existing writable profiles.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-005B

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
| production | overlay block store: memory cache + local filesystem cache + Azure Blob SAS-backed storage | Azure OpenAI |
| production-v2 | direct Azure-backed LexonGraph block store | Azure OpenAI |

This selection is configuration-driven and preserves one stable delegated
indexing flow independent of environment across indexed blocks, normalized email
artifacts, and mailbox provenance artifacts.

For the approved MVP slice, the local/testing profile is the only profile that
must execute end to end. The production-oriented profiles remain represented at
this design layer so the same orchestration contract can govern direct-local,
overlay-backed, and approved direct-Azure-backed tool targeting without
introducing ad hoc backend-specific operator modes.

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
preserved production-oriented profiles use the same request shape,
stage-selection contract, and scheduler contract. Higher-layer parent
construction remains
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
- callers may also select the published profile version for clustering-enabled
  stages, with omission resolving to the default `0.1.0` profile
- the request-file-driven runtime shape remains preserved for batch items,
  environment selection, stage selection, and profile-version selection
- any legacy low-level clustering flags that remain in old automation fail
  validation explicitly so operators are not misled into believing they still
  tune active upstream planning behavior

This increment therefore requires one persistent profile-version field in
`BatchRequest` and one matching CLI selector for `run`, while still avoiding a
broader repository-local clustering-policy matrix.

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
diagnostic-query execution. It must also surface the rooted-query access
statistics and advisory RTT-cost evidence tied to those query modes without
adding a second operator surface or a transport-specific configuration API.

**Traces to:** RQ-INDEXER-008D, RQ-INDEXER-008D1, RQ-INDEXER-008D2, RQ-INDEXER-008D3, RQ-INDEXER-008D4, RQ-INDEXER-008D5, RQ-INDEXER-009

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

### DSG-LFI-007F `Local published-profile sweep automation surface`

LexonArchiveBuilder preserves one repository-local operator automation surface,
currently the runnable root `test.ps1` script, for evaluating approved
published-profile experiments in the local/testing environment.

That surface is intentionally outside the batch request schema and production
deployment contract. It composes the already-approved `run` and rooted-quality
CLI boundaries rather than defining a testing-only indexing API.

In this increment, the automation surface:

- carries an operator-editable published-profile list whose active named target
  may be the upstream `0.6.x` series for version-sweep evaluation
- may instead execute the approved published-profile `0.7.0` fixed-budget
  ladder by driving the same `run` plus rooted-quality workflow over one
  repository-approved rung table
- may include prior comparison baselines such as `0.5.x` in the same sweep
  without changing the omitted-selector default or widening the runtime
  contract
- emits per-profile run artifacts, per-profile rooted-quality artifacts, and a
  comparable summary output suitable for side-by-side evaluation, with ladder
  runs preserving rung identity plus the selected beam width and clustering
  cardinality in the emitted artifacts
- remains version-series-agnostic so later published-profile series can be
  substituted without reshaping the repository contract

**Traces to:** RQ-INDEXER-003J, RQ-INDEXER-003J1

### DSG-LFI-007F1 `Local fixed-budget ladder execution plan`

The approved `0.7.0` ladder experiment is realized as an execution plan layered
onto the same local/testing automation surface rather than as a second operator
entrypoint.

That execution plan:

1. defines one fixed ladder budget of `1024`, anchored on the prior successful
   `16x64` baseline
2. defines the default rung sequence `4x256`, `8x128`, `16x64`, `32x32`, and
   `64x16`
3. runs preflight validation for each rung before long-running execution
4. executes rungs in deterministic order while preserving one artifact family
   per rung for build output, rooted-quality output, and comparable summaries
5. leaves post-hoc comparison and operator interpretation on repository-local
   artifacts rather than on a new serving or telemetry surface

Because the approved ladder is a repository-local experiment aid, its rung table
is repository-owned and deterministic rather than an open-ended low-level
clustering-control API.

**Traces to:** RQ-INDEXER-003J1

### DSG-LFI-007G `Rooted block-copy CLI surface`

LexonArchiveBuilder exposes the rooted block-copy capability through a
dedicated CLI-only operator surface that accepts:

- one source block-store target using the approved shared profile contract
- one destination block-store target using the same approved contract
- one or more caller-supplied root block identifiers
- one optional artifact destination when the default JSON output location is
  insufficient

The CLI surface is intentionally separate from the batch `run` request-file
contract because the tool operates on already-persisted immutable block graphs
rather than orchestrating a new indexing batch. The design does not require MCP
exposure, request-file integration, elevation into a normal indexing stage, or
definition of any repository-local block-store backend family.

The operator surface renders one concise human-readable transfer summary and
writes one machine-readable artifact reporting rooted transfer outcomes. In the
default read-before-write mode that includes requested roots, copied block
counts, skipped-already-present counts, and failures. In the opt-in blind-write
mode the same surface skips destination existence reads and instead reports
attempted-write plus failure-oriented outcomes without claiming exact
copied-versus-skipped classification. That result contract is about transfer
outcomes only; it does not implicitly publish mutable references or redefine any
upstream block-store backend semantics.

The design keeps the current read-before-write path as the default because it
preserves the strongest destination-state accounting across backends. The
blind-write path is an explicit operator-selected tradeoff for backends where
destination reads hang or are disproportionately expensive: it still traverses
the same rooted immutable graph and still treats duplicate publication as safe,
but it accepts weaker outcome classification in exchange for avoiding
destination presence checks entirely.

When rooted traversal determines that destination publication is required, the
same CLI surface may keep multiple destination writes in flight
asynchronously instead of waiting for each destination write to complete before
issuing the next one. That write pipeline remains bounded by one operator-
selectable CLI concurrency limit, with first approved default `64`, so the
design improves high-latency backend throughput without redefining the shared
`BlockStore` contract or inventing a backend-specific transfer path.

The bounded write pipeline applies to both rooted-copy modes. In the default
read-before-write mode, a block becomes eligible for the asynchronous write
queue only after the destination has already been classified as missing. In the
opt-in blind-write mode, the same bounded queue accepts the direct write
attempts without any preceding destination existence read. Because write
completions may arrive out of traversal order, the design constrains reporting
to remain mode-truthful and rooted-reachability-preserving rather than tying
summary semantics to serialized write completion order.

Because rooted block copy can spend a long time traversing reachable block
graphs or waiting on destination persistence without any intermediate final
artifact to inspect, the same CLI surface also emits basic in-flight liveness on
its normal operator-visible output stream before final completion. That default
liveness remains bounded to the short-lived CLI invocation, does not require a
separate progress API or daemon, and is intentionally lighter-weight than any
future opt-in verbose diagnostic mode. The design leaves the exact cadence and
message schema to implementation so long as ordinary operators can tell that
rooted traversal or transfer work is still advancing.

**Traces to:** RQ-INDEXER-005B, RQ-INDEXER-009

### DSG-LFI-008 `Local and production parity boundary`

Local/testing and production environments differ only in adapter realization and
provider configuration, not in the container's batch contract, the staged email
artifact model, content item shape, the stage-selection and concurrency-
configuration surfaces, the clustering-selection and clustering-option CLI
surface, the rooted block-tree quality CLI surface, or the delegated
rooted block-copy CLI surface, or the delegated `lexongraph-streaming-indexer`
orchestration contract. The same parity boundary
also covers the rooted CLI search surface's use of configured storage plus one
operator-supplied embedding endpoint.

The MVP realizes this parity boundary by keeping the core orchestration and item
model environment-neutral even though only the local/testing profile executes in
the first increment. Standalone clustering continues to rely on the same
configured `BlockStore` abstraction and the same upstream block-iteration
contract across environments rather than introducing a local-only discovery
mechanism.

Within that parity boundary, every indexer-owned tool shares the same approved
storage-profile contract: direct local filesystem, the existing `production`
overlay profile, the additive `production-v2` direct Azure-backed profile, and
for surfaces that can operate through read-only immutable block fetches, the
additive `gateway-http3` profile. No indexer-owned tool defines an ad hoc plain
Azure-only targeting exception outside that shared profile set, and no
write-bearing tool surface treats `gateway-http3` as a writable substitute.

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

The same separation applies to the rooted block-copy tool: it remains a CLI
operator workflow over approved block-store targets and does not become an MCP
mutation, replication, or storage-administration surface in this increment.

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

For standalone clustering, the comparable invariant is journal-head stability:
repeating the clustering-only stage against the same journal head and the same
clustering-eligible block-store snapshot is expected to produce the same
logical clustering result under unchanged upstream semantics.

The same stability expectation applies to clustering configuration resolution:
repeating a clustering-enabled run with the same selected published profile
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
  immutable replay-audit journal without whole-store scan fallback
- correct journal-only replay-list reconstruction through sorted deduped block
  ids with payload reads deferred until later processing
- correct adoption of the upstream published-profile API with defaulted and
  explicit profile-version selection plus explicit rejection of retired
  low-level clustering controls
- correct deterministic replay staging and replay-stable content fingerprinting
- correct replay-audit block publication, mutable head updates, grouped-entry
  coverage, and crash-tolerance behavior
- correct selection and use of content-resolution, block-store, and
  embedding-provider adapters
- correct interoperability of the local filesystem-backed block-store profile
  with LexonGraph-owned tooling expectations
- correct exposure and use of the approved production-oriented block-store
  profile set, including the existing `production` overlay profile, the
  additive `production-v2` direct Azure-backed profile, and the additive
  `gateway-http3` read-only profile where read-only rooted block fetches are
  sufficient
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
- correct opt-in SDK and HTTP-client diagnostic activation for the entire
  indexer process through the standard Rust logging environment path, while
  preserving quiet default runs
- correct rooted block copy over the shared `BlockStore` boundary, including
  reachable-only traversal, identity-preserving transfer, the default
  read-before-write classification path, the opt-in blind-write path with
  reduced copied-versus-skipped accounting, bounded asynchronous destination-
  write concurrency with one operator-selectable limit defaulting to `64`,
  failure reporting, default in-flight liveness on the normal CLI surface, and
  preservation of mutable-reference exclusion
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
RQ-INDEXER-005B, RQ-INDEXER-008D, RQ-INDEXER-008E,
RQ-INDEXER-010A, RQ-INDEXER-010B, RQ-INDEXER-010, DSG-LFI-001A,
DSG-LFI-001B, DSG-LFI-001C, DSG-LFI-001D, DSG-LFI-001E, DSG-LFI-001F,
DSG-LFI-001G, DSG-LFI-001H, DSG-LFI-001I, DSG-LFI-002A, DSG-LFI-002B,
DSG-LFI-002C, DSG-LFI-002D, DSG-LFI-002G, DSG-LFI-004G, DSG-LFI-005A, DSG-LFI-005B,
DSG-LFI-005C, DSG-LFI-005D, DSG-LFI-005E, DSG-LFI-006A, DSG-LFI-007A, DSG-LFI-007B,
DSG-LFI-007C, DSG-LFI-007D, DSG-LFI-007E, DSG-LFI-007G
