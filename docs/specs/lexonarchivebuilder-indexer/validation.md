<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder Indexer Validation

## Status

Validation patch for the approved email-artifact, chunk-level
indexing, local filesystem block-store interoperability, replay-based
streaming delegated indexing, stage-selectable execution, standalone
clustering input discovery, mutable current-root publication, published-profile API adoption,
published-profile version selection, latest published-profile and telemetry
compatibility, upstream regression assessment, replay-submission and
streaming-status observability, pass-end convergence telemetry, v2
intra-pass planning telemetry, user-usable convergence diagnosis, explicit delegated-contract and
effective-profile identity signaling,
clustering-failure diagnostics, rooted
block-tree quality assessment with rooted TNN-recall diagnostics, rooted
query access-cost reporting, rooted
CLI search over stored trees, rooted block-store copy tooling, replay-stable fingerprinting, temporary
upstream `main` tracking for rapid profile validation, upstream
wgpu-acceleration revision compatibility, 0.6.x published-profile
evaluation, local testing sweep automation, v0.7.0 fixed-budget ladder
experiment automation, upstream embedding-readback
API adoption, LAB-owned replay-journaled split-stage recovery, bounded-residency
deterministic replay ordering, efficient replay-order preparation, bounded replay-batch preparation overlap, and
layer-parallel block-construction evolution, v2 custom-block adoption for
repository-owned non-search artifacts, and conditional streaming-indexer v2
adoption with repository-default published profile `0.7.0`, plus derived
planner-state-root support for delegated bounded-residency out-of-core planning
spill, in
`docs/specs/lexonarchivebuilder-indexer/requirements.md`,
`docs/specs/lexonarchivebuilder-indexer/design.md`, and
`docs/specs/lexonarchivebuilder-indexer/validation.md`.

## Validation Scope

These validation entries define the expected conformance surface for the
LexonArchiveBuilder-owned indexer boundary, including local filesystem
block-store interoperability, replay-based streaming delegated indexing,
stage-selectable execution, standalone clustering input discovery,
published-profile API adoption, caller-selectable published-profile
configuration with default `0.7.0`, latest published-profile and telemetry
compatibility, temporary upstream `main` tracking for rapid profile
validation, upstream wgpu-acceleration revision compatibility, 0.6.x
published-profile evaluation, local testing sweep automation, v0.7.0
fixed-budget ladder experiment automation, upstream
embedding-readback API adoption, upstream regression assessment,
embedding-phase batch-progress observability,
replay-submission observability, streaming-status observability,
pass-end convergence telemetry, user-usable convergence diagnosis, explicit delegated-contract and
effective-profile identity signaling, telemetry-count-semantics clarity,
clustering-failure diagnostics, rooted
block-tree quality assessment with rooted TNN-recall diagnostics, rooted
query access-cost reporting, rooted CLI
search over stored trees, rooted block-store copy tooling, replay-stable fingerprinting, LAB-owned replay-journaled
split-stage recovery, bounded-residency deterministic replay ordering for
clustering replay, derived planner-state-root support for delegated
bounded-residency out-of-core planning spill, and leaf-layer parallel block
scheduling
in the local/testing profile.

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
indexing path, including at least one planning pass, explicit planning
completion, and final materialization replay, while preserving the approved
repository stage contract rather than exposing raw upstream lifecycle phases.

**Traces to:** RQ-INDEXER-003A, DSG-LFI-001A

### VAL-LFI-002E1

Run a representative full-pipeline indexing workload whose logical corpus size
exceeds available RAM under an explicit caller-selected memory budget.

**Pass condition:** LexonArchiveBuilder completes or fails explicitly without
repository-owned replay orchestration retaining corpus-scale replay-item,
embedding, or mailbox-expansion state in memory; the observed working set
remains bounded by the approved design strategy rather than growing in
proportion to corpus size, and the caller-visible stage plus summary contracts
remain unchanged.

**Traces to:** RQ-INDEXER-003A1, RQ-INDEXER-003A2, DSG-LFI-001A1,
DSG-LFI-001A2, DSG-LFI-001F

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
replay, emits durable replay-journal state only for successfully persisted
replayable leaf outputs, does not require clustering or higher-layer final
materialization in the same invocation, and still returns the existing
`BatchSummary` shape.

**Traces to:** RQ-INDEXER-003A, RQ-INDEXER-003D, RQ-INDEXER-003E1,
DSG-LFI-001A, DSG-LFI-001D, DSG-LFI-001F, DSG-LFI-001F1

### VAL-LFI-002H1

Interrupt a representative ingestion-plus-embedding run after at least one
replayable leaf output has been durably persisted but before the invocation has
completed.

**Pass condition:** previously committed immutable replay-audit journal blocks
remain readable from the last published journal head, incomplete unpublished
progress does not invalidate earlier committed journal blocks, and subsequent
resume logic can distinguish completed replayable work from unpublished
trailing progress.

**Traces to:** RQ-INDEXER-003E1, RQ-INDEXER-003E2, RQ-INDEXER-008,
DSG-LFI-001F1

### VAL-LFI-002H2

Inspect a representative immutable replay-audit chain after an ingestion run
publishes more than one journal block.

**Pass condition:** each published journal block identifies its predecessor by
hash when a predecessor exists, the latest journal head is discoverable through
the repository-owned mutable reference mechanism at the selected
`refs/{ref_name}` artifact, that artifact remains human-readable JSON, and
representative audit entries record enough input identity, action-kind, and
generated-output detail to reconstruct what completed work occurred.

**Traces to:** RQ-INDEXER-003E2, RQ-INDEXER-003E3, RQ-INDEXER-003E4,
DSG-LFI-001F1, DSG-LFI-001F2, DSG-LFI-001F3

### VAL-LFI-002H3

Run one successful root-materializing execution stage and one later execution
stage that does not materialize a new final root.

**Pass condition:** the root-materializing stage leaves the existing
`BatchSummary` final-root output intact and publishes the same immutable root
identity through the repository-owned mutable current-root reference; the later
non-root-materializing stage does not rewrite that current-root reference, and
the same `refs/{ref_name}` JSON artifact continues to carry the journal head,
published root when present, and publication metadata.

**Traces to:** RQ-INDEXER-003D, RQ-INDEXER-003E5, DSG-LFI-001D,
DSG-LFI-001F4

### VAL-LFI-002I

Run the clustering-plus-block-assembly stage against a configured block store
that already contains representative delegated blocks, replay metadata, a valid
immutable replay-audit journal head, and an empty request item collection.

**Pass condition:** LexonArchiveBuilder reconstructs the deterministic replay
input needed by the streaming indexer from the authoritative immutable replay-
audit journal without rescanning the whole store, reads replay-audit blocks and
their recorded block ids without dereferencing payload blocks during replay-list
generation, sorts and dedupes those ids into the approved deterministic order,
excludes artifacts outside the approved replay-input surface, and performs
clustering or block assembly without requiring a prior LexonArchiveBuilder
summary manifest. Replay submission reconstructs delegated replay content from
the stored replayable leaf blocks rather than by reopening request-era source
files or rerunning normalized-email chunk derivation through resolver-owned
paths.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-003E, RQ-INDEXER-003E1,
RQ-INDEXER-003E3, RQ-INDEXER-004F, RQ-INDEXER-010A, DSG-LFI-001E,
DSG-LFI-001F, DSG-LFI-001F1, DSG-LFI-001F2

### VAL-LFI-002I1

Run the clustering-plus-block-assembly stage against a configured block store
that contains representative delegated blocks but lacks a valid replay-audit
journal head for the selected store snapshot.

**Pass condition:** LexonArchiveBuilder returns an explicit unsuccessful
outcome that identifies the missing or invalid replay-audit journal discovery
state rather than silently rescanning the whole store or producing a
success-shaped clustering result.

**Traces to:** RQ-INDEXER-003E, RQ-INDEXER-003E1, RQ-INDEXER-003E3,
DSG-LFI-001E, DSG-LFI-001F2

### VAL-LFI-002I2

Run the clustering-plus-block-assembly stage against a store whose replay-audit
journal represents a logical corpus larger than available RAM under an explicit
caller-selected memory budget.

**Pass condition:** LexonArchiveBuilder reconstructs and submits the approved
deterministic replay input without whole-store rescans, without repository-
owned orchestration loading corpus-scale replay inventories or stored
embeddings into resident memory at once, and without allowing resident memory
to scale with total replay-input count. Validation evidence shows the runtime
reads replay-audit blocks only during replay-list generation, keeps live memory
bounded to compact ordering windows plus merge buffers, leaves payload fetches
to later classification or finalization processing, uses stored replayable leaf
content rather than resolver-driven source rematerialization during replay
submission, and—when the effective path is v2—keeps any delegated
planner-managed out-of-core files confined to the derived planner-state root
rather than using them as a replay-ordering catalog.

**Traces to:** RQ-INDEXER-003A1, RQ-INDEXER-003A2, RQ-INDEXER-003A3,
RQ-INDEXER-003E, RQ-INDEXER-003E1, RQ-INDEXER-003E3, DSG-LFI-001A1,
DSG-LFI-001A2, DSG-LFI-001A3, DSG-LFI-001E, DSG-LFI-001F, DSG-LFI-001F1,
DSG-LFI-001F2

### VAL-LFI-002I3

Run the clustering-plus-block-assembly stage against a replay-audit journal
large enough to require repository-owned replay-order externalization, with one
case that supplies `--summary-out` and one that omits it.

**Pass condition:** LexonArchiveBuilder derives a run-scoped replay-order
scratch root from the existing request-adjacent artifact policy without adding
any new caller-visible selector, keeps that scratch root separate from the
delegated planner-state root, stores only compact replay-order identities plus
fixed-size validation evidence in the repository-owned scratch files, and drives
later classification or finalization from a file-backed deterministic replay
order without rebuilding a corpus-scale resident vector. If the derived replay-
order scratch root is required but cannot be created or written, the runtime
fails explicitly rather than silently reverting to corpus-scale resident replay
ordering.

**Traces to:** RQ-INDEXER-003A2, DSG-LFI-001A2, DSG-LFI-001A3

### VAL-LFI-002I4

Run a full-pipeline or ingestion-plus-clustering stage that persists both
document-derived and mailbox-derived replayable leaf outputs, then remove or
invalidate the original request-era source files before running the
clustering-plus-block-assembly stage against the resulting replay-journal head.

**Pass condition:** LexonArchiveBuilder successfully reconstructs and submits
delegated replay inputs for both content classes from the stored replayable
leaf blocks plus replay metadata already present in the configured block-store
snapshot. Validation evidence shows clustering-only replay no longer depends on
reopening request-era document paths, no longer requires normalized-email
artifact decode plus rechunking as the replay content transport, and still
preserves provenance plus replay-validation identity needed for diagnostics and
deterministic fingerprints.

**Traces to:** RQ-INDEXER-003E, RQ-INDEXER-003E1, RQ-INDEXER-003E4,
RQ-INDEXER-004F, RQ-INDEXER-010A, DSG-LFI-001E, DSG-LFI-001F

### VAL-LFI-002I5

Run the clustering-plus-block-assembly stage against a replay-audit journal
large enough to require multiple replay batches while instrumenting one future
batch of repository-owned replay preparation to overlap with delegated
`ingest_batch(...)` processing of the current batch.

**Pass condition:** Validation evidence shows the runtime may prepare at most
one tightly bounded successor replay batch while the current batch is inside the
delegated ingestion path, but it never invokes concurrent upstream
`ingest_batch(...)`, `finish_pass()`, `mark_planning_complete()`, or
`finalize(...)` operations on the same delegated run. The active batch continues
to observe the correct replay content and embedding state until it completes,
the next batch is submitted in the same deterministic order that non-overlapped
replay would use, and peak repository-owned resident memory remains bounded to
the current batch plus the approved prefetched successor state rather than an
unbounded replay queue.

**Traces to:** RQ-INDEXER-003A1, RQ-INDEXER-003A4, RQ-INDEXER-004F,
RQ-INDEXER-010A, DSG-LFI-001A1, DSG-LFI-001A4

### VAL-LFI-002I6

Compare the current bounded-residency replay-order preparation baseline against
the optimized replay-order path on a replay-audit journal large enough to force
repository-owned externalization and at least one spill/merge cycle.

**Pass condition:** The optimized path produces the same deduplicated
deterministic replay block-id order and the same replay-validation integrity
outcome as the baseline, while validation evidence shows materially improved
replay-order preparation throughput and/or materially better CPU-disk
utilization. Evidence also shows the optimized path remains bounded-memory,
payload-free during replay-order generation, and compatible with both the
existing request-adjacent scratch-root policy and the unchanged caller-visible
stage contract.

**Traces to:** RQ-INDEXER-003A2, RQ-INDEXER-003A5, DSG-LFI-001A2,
DSG-LFI-001A5

### VAL-LFI-002I7

Compare the current serial replay-batch materialization path against the
optimized bounded-parallel materialization path on a clustering replay workload
large enough to require many replay batches during an `analyze-pca` planning
pass or equivalent replay-heavy delegated phase.

**Pass condition:** Validation evidence shows the optimized path may fetch,
decode, and reconstruct replay entries concurrently while materializing a
single replay batch before handoff, but delegated `ingest_batch(...)` still
receives only fully materialized batches in the exact deterministic
replay-entry order the serial baseline would have produced. The active-batch
embedding-cache state remains aligned with the submitted batch,
replay-validation identity and failure attribution remain deterministic, and
no concurrent delegated lifecycle calls are introduced.

The validation record MUST also show that replay-batch materialization was the
dominant repository-owned waiting seam targeted by the optimization. When a
like-for-like rerun on the updated build is practical, comparative evidence
SHALL show materially reduced repository-owned waiting and/or materially better
CPU-disk utilization during replayed clustering passes. When such a rerun is
not practical, representative profiler evidence from the current seam plus the
deterministic-correctness regressions for the updated implementation are
acceptable in its place.

**Traces to:** RQ-INDEXER-003A4, RQ-INDEXER-003A6, DSG-LFI-001A4,
DSG-LFI-001A6

### VAL-LFI-002J

Compare a representative full-pipeline run with an equivalent split-stage run.

**Pass condition:** the split-stage path reconstructs the same logical replay
item order and fingerprint inputs used by the full-pipeline path, so the
streaming indexer accepts both executions without replay-mismatch failures and
both executions remain contract-equivalent at the LexonArchiveBuilder boundary.

**Traces to:** RQ-INDEXER-003A, RQ-INDEXER-004F, DSG-LFI-001A, DSG-LFI-001F

### VAL-LFI-002K

Inspect the clustering-enabled profile-selection surface for a representative
`run` invocation and request payload.

**Pass condition:** the CLI preserves the existing request-file-driven runtime
shape and stage selector, clustering-enabled execution exposes one
profile-version selector across CLI and `BatchRequest`, omission resolves to
default profile `0.7.0`, explicit selection preserves the same contract shape,
refreshing the adopted upstream dependency state can add newly published
selector targets in the active `0.6.x` series without changing that default
behavior, the effective selected version is resolved from the existing
CLI-override then request-file then default precedence before any old-versus-v2
delegated-surface choice is made, the operator-visible run identity reports
that same effective version together with the delegated contract family chosen
for the run, and
any retired low-level clustering flags or equivalent stale automation inputs
are rejected explicitly instead of being silently ignored.

**Traces to:** RQ-INDEXER-003F, RQ-INDEXER-003G, DSG-LFI-001G,
DSG-LFI-001H, DSG-LFI-007C

### VAL-LFI-002L

Run clustering-enabled execution twice through the same selected
published-profile path against the same representative input snapshot.

**Pass condition:** both invocations resolve to the same selected published
profile version and therefore to the same effective delegated planning
behavior, so the profile-based contract remains deterministic and does not
create hidden replay drift. When the selector is omitted, that resolved version
is `0.7.0`.

**Traces to:** RQ-INDEXER-003F, RQ-INDEXER-003G, RQ-INDEXER-003H,
RQ-INDEXER-008, DSG-LFI-001G, DSG-LFI-001H, DSG-LFI-010

### VAL-LFI-002M

Attempt clustering-enabled execution once through the approved published-profile
path and once with one of the retired low-level clustering controls such as
`cluster_count` supplied explicitly.

**Pass condition:** the normal published-profile invocation succeeds through the
selected published-profile path, and the invocation that supplies a retired
low-level clustering control fails explicitly rather than being merged into or
silently overriding the published profile.

**Traces to:** RQ-INDEXER-003H, DSG-LFI-001H, DSG-LFI-010

### VAL-LFI-002M1

Inspect the approved `0.7.0` fixed-budget ladder specification against the
published-profile clustering-cardinality boundary.

**Pass condition:** the specification preserves the normal rule that ordinary
clustering-enabled runs reject retired low-level clustering controls, while
also defining one scoped local/testing-only ladder mechanism that can realize
the approved rung table without redefining the general batch contract, request
schema, production behavior, or MCP-facing behavior. If the active upstream
`0.7.0` v2 surface still lacks a supported override hook for that scoped
mechanism, the specification must instead require an explicit fail-fast outcome
rather than silent override loss or fallback to the legacy delegated path.

**Traces to:** RQ-INDEXER-003H, RQ-INDEXER-003J1, DSG-LFI-001H, DSG-LFI-007F1, DSG-LFI-010

### VAL-LFI-002N

Inspect the latest LexonGraph upgrade boundary against the repository-required
indexer contract.

**Pass condition:** the upgrade preserves the approved external stage contract,
published-profile API adoption for clustering-enabled execution, default
profile `0.7.0` plus explicit profile-version selection, retirement of the old
low-level clustering control family, deterministic split-stage replay,
repository-owned progress projection, projection of the latest upstream live
telemetry and heartbeat events, unchanged MCP search-serving behavior for
already-indexed content, and temporary explicit tracking of LexonGraph `main`
for rapid profile validation and upstream wgpu acceleration without new
repository-visible low-level controls. This includes refreshing the adopted
dependency state so newly published profile versions in the active `0.6.x`
series become selectable while omitted selectors still resolve
to `0.7.0`, with earlier `0.5.x` alignment retained only as prior comparison
context and `0.4.x` retained as historical context, or else any missing
capability is classified explicitly as an upstream regression or compatibility
finding rather than being silently dropped. For effective profile `0.7.0`,
this also includes preserving the upstream v2 multi-pass lifecycle rather than
assuming repository-local single-pass planning completion, and preserving
additive pass-end convergence telemetry that identifies the effective profile,
delegated contract family, and exposed pass metrics needed to judge
convergence. The same upgrade boundary must also preserve automatic derivation
of the upstream-required planner-state root from existing request-adjacent
artifact/output locations, with no new caller-visible selector and explicit
failure when that derived root is unusable.

**Traces to:** RQ-INDEXER-003I, RQ-INDEXER-003A3, DSG-LFI-001A3, DSG-LFI-001I

### VAL-LFI-002N3

Exercise clustering-enabled execution once with omitted profile selection,
once with explicit `0.7.0`, and once with one explicit supported non-`0.7.0`
profile version, including at least one case where the selected version comes
from the CLI override rather than the request file.

**Pass condition:** omitted selection and explicit `0.7.0` both resolve to the
same effective profile and use the same v2-backed delegated path, while the
explicit non-`0.7.0` run preserves the same caller-visible selector contract
but uses the existing non-v2 delegated path instead. No run silently coerces an
explicit non-`0.7.0` selection back to `0.7.0`, and the delegated-surface
choice is identical whether the effective selected profile came from the CLI,
the request file, or the omitted-selector default. When the effective selected
profile is `0.7.0`, the same v2-backed path must also preserve repeated full
planning replays until planning completion succeeds, rather than failing solely
because one completed v2 planning pass left additional upstream work pending.
The operator-visible run identity must match each resolved effective
profile-plus-contract pairing in all three cases.

**Traces to:** RQ-INDEXER-003F, RQ-INDEXER-003G, RQ-INDEXER-003I,
DSG-LFI-001G, DSG-LFI-001I, DSG-LFI-001I1

### VAL-LFI-002N4

Exercise one effective-`0.7.0` clustering-enabled run whose corpus is large
enough to require more than one upstream v2 planning replay pass before
planning becomes complete.

**Pass condition:** LexonArchiveBuilder keeps replaying full planning passes on
the selected v2 path until `mark_planning_complete()` succeeds or an
upstream/runtime error occurs. A completed first planning pass that leaves v2
partitions pending is not treated as a terminal repository-side lifecycle
failure by itself. The same v2 path derives its planner-state root from the
existing request-adjacent artifact policy and uses no new caller-visible
planner-state-root selector.

**Traces to:** RQ-INDEXER-003I, RQ-INDEXER-003A3, DSG-LFI-001A3, DSG-LFI-001I, DSG-LFI-001I1

### VAL-LFI-002N5

Run one effective-`0.7.0` clustering-enabled invocation with `--summary-out`
present and one without `--summary-out`, keeping the existing request-file-
driven batch surface unchanged in both cases.

**Pass condition:** both runs satisfy the upstream v2 writable-root requirement
without introducing a new CLI flag or `BatchRequest` field for planner-state
selection. When `--summary-out` is present, the delegated planner-state root is
derived beneath that directory; otherwise it is derived beneath the `--request`
file directory. In both cases the derived root is run-scoped and the delegated
out-of-core planner state stays opaque rather than becoming a repository-owned
artifact contract.

**Traces to:** RQ-INDEXER-003A3, DSG-LFI-001A3, DSG-LFI-001I1

### VAL-LFI-002N6

Exercise one effective-`0.7.0` clustering-enabled invocation whose derived
planner-state root cannot be created or written.

**Pass condition:** LexonArchiveBuilder fails explicitly before claiming a
success-shaped v2 clustering result. The failure identifies the unusable
derived planner-state-root condition and does not silently fall back to
unbounded resident planning state, a different delegated contract family, or a
new ad hoc scratch location outside the approved request-adjacent artifact
policy.

**Traces to:** RQ-INDEXER-003A3, DSG-LFI-001A3, DSG-LFI-001I

### VAL-LFI-002N1

Run the repository-local published-profile sweep automation against a
representative local/testing corpus.

**Pass condition:** the runnable `test.ps1` workflow reuses the approved batch
and rooted-quality operator surfaces, can target the active `0.6.x`
published-profile series without per-profile code edits, emits per-profile
artifacts plus comparable summary output, and keeps any optional `0.5.x`
baseline comparison in the same local/testing-only automation surface rather
than changing the batch request contract or MCP behavior.

**Traces to:** RQ-INDEXER-003J, DSG-LFI-001I, DSG-LFI-007F

### VAL-LFI-002N2

Run the approved repository-local `0.7.0` fixed-budget ladder automation
against a representative local/testing corpus.

**Pass condition:** the runnable ladder reuses the approved batch and
rooted-quality operator surfaces, preserves the fixed budget `1024` across the
default rung set `4x256`, `8x128`, `16x64`, `32x32`, and `64x16`, emits
per-rung build and rooted-quality artifacts plus a comparable summary table,
and includes preflight validation plus deterministic rung ordering in the same
local/testing-only automation surface rather than defining a new production or
MCP-visible entrypoint. If the active upstream `0.7.0` v2 surface still lacks
the required override hook, the same automation may satisfy this increment by
failing fast before execution with an explicit operator-facing explanation of
the unsupported ladder gap.

**Traces to:** RQ-INDEXER-003J1, DSG-LFI-001H, DSG-LFI-001I, DSG-LFI-007F, DSG-LFI-007F1

### VAL-LFI-002O

Inspect the rooted block-tree quality operator surface.

**Pass condition:** LexonArchiveBuilder exposes one CLI-only assessment surface
that accepts a configured block-store boundary plus a caller-supplied root block
identifier, does not require request-file batch execution or MCP exposure, and
renders both an operator-readable summary and a machine-readable JSON report for
the rooted analysis result without requiring an operator-visible quantile-bin
configuration surface in this increment. When rooted TNN-recall is enabled, the
same surface keeps corpus-based evaluation controls, including sample size,
seed, and traversal width, distinct from optional diagnostic-query recall
inputs. Any stored branch embeddings consumed by this flow are reconstructed
through the upstream LexonGraph embedding readback API rather than through a
repository-local branch-decoder table, while plain leaf payload decoding for
the currently supported stable encodings remains on the existing local path in
this increment.

**Traces to:** RQ-INDEXER-008D, RQ-INDEXER-008D1, RQ-INDEXER-008D2, RQ-INDEXER-008D3, DSG-LFI-002D, DSG-LFI-002D1, DSG-LFI-002D2, DSG-LFI-002F, DSG-LFI-005B, DSG-LFI-007D

### VAL-LFI-002P

Inspect the rooted CLI search operator surface.

**Pass condition:** LexonArchiveBuilder exposes one CLI-only rooted search
surface that accepts query text, a caller-provided embedding endpoint, a
caller-supplied root block identifier, and `k`, does not require request-file
batch execution or MCP exposure, and emits both operator-readable results and
machine-readable JSON output for one invocation. Any repository-owned
stored-embedding readback required by the rooted search path is delegated to
the upstream LexonGraph embedding readback API instead of a repository-local
decoder.

**Traces to:** RQ-INDEXER-008E, DSG-LFI-002E, DSG-LFI-002F, DSG-LFI-006A, DSG-LFI-007E

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
share the same `BlockStore` abstraction family, and to the rooted block-tree
quality tool when it reads stored trees through that same boundary.
The same neutrality also applies to rooted CLI search over a caller-supplied
tree when it reads through the same `BlockStore` boundary. The non-local
boundary is the approved production profile set: the existing overlay target of
memory cache plus local filesystem cache plus Azure Blob SAS-backed storage, or
the additive `production-v2` direct Azure-backed target, rather than ad hoc
plain Azure-only tool modes.

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

### VAL-LFI-005A1

Inspect the non-local tool-targeting profiles for representative indexer-owned
tool surfaces that traverse the shared `BlockStore` boundary.

**Pass condition:** the approved non-local profile set is specified as the
existing `production` overlay profile, the additive `production-v2` direct
Azure-backed profile, and for read-only tool surfaces only the additive
`gateway-http3` profile; batch indexing, standalone clustering, rooted quality
assessment, rooted CLI search, and rooted block copy all reuse that same
targeting contract with the documented read-only restriction for
`gateway-http3`; and no operator-facing tool introduces a plain Azure-only
block-store mode outside the approved repository-defined profile set.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-005B, RQ-INDEXER-007, DSG-LFI-005,
DSG-LFI-005B, DSG-LFI-005C, DSG-LFI-005D, DSG-LFI-005E, DSG-LFI-007,
DSG-LFI-007G, DSG-LFI-008

### VAL-LFI-005A2

Inspect representative repository-owned non-search artifact surfaces after the
v2 custom-block transition.

**Pass condition:** normalized email artifacts and mailbox provenance artifacts
operate as LexonGraph v2 custom blocks, delegated branch and leaf index blocks
remain on the current upstream-owned contract for this increment, and
validation may rebuild stores instead of requiring reads from pre-v2 v1
artifact blocks.

**Traces to:** RQ-INDEXER-005A, DSG-LFI-005A1

### VAL-LFI-005B

Run the rooted block-tree quality tool against a representative stored tree
whose reachable rooted snapshot contains at least one known structural defect or
boundary case plus enough block variation to exercise per-layer cohesion,
separation, PCA-axis, quantile-occupancy, and split-effectiveness reporting.

**Pass condition:** the assessment traverses only the blocks reachable from the
caller-supplied root, reports hard structural findings separately from advisory
embedding-space quality statistics, identifies the affected block relationships,
and emits both per-block and aggregate quantitative evidence about the rooted
tree's represented embedding-space region in the human-readable summary and the
JSON artifact. That evidence includes per-block mean distance from centroid,
per-layer mean and standard deviation for intra-block dispersion,
per-layer mean and standard deviation for sibling centroid-to-centroid
distance, per-block and per-layer PCA first-axis-strength reporting,
per-block quantile-bin occupancy counts plus occupancy variance with explicit
empty-bin detection and overfull-bin detection for bins whose occupancy exceeds
two times the expected value, and per-parent split-effectiveness statistics.
The required parent-versus-child centroid-distance heuristic is represented as
aggregate split-effectiveness evidence, including the percentage of children
whose dispersion exceeds the parent's plus the mean and maximum increase,
rather than as emitted per-pair warning findings. The same run computes
corpus-based TNN-recall over the rooted reachable embedding set at Recall@1,
Recall@5, and Recall@10 using uniform seeded sampling with configurable sample
size and configurable traversal width, derives mean recall, recall standard
deviation, and recall histograms from that corpus-based mode only, records the
selected traversal width in the emitted recall evidence, and obtains any
numerical embedding values needed for those calculations through the upstream
LexonGraph embedding readback API rather than a repository-local decoder. The
same run also reports rooted-query access statistics for the executed corpus
query set, including unique touched-block counts and serialized bytes read both
per level and as overall totals, plus advisory RTT-cost estimates derived from
the required per-level `ceil(level_bytes / 65536)` model.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-008D, RQ-INDEXER-008D1, RQ-INDEXER-008D4, RQ-INDEXER-008D5, DSG-LFI-002D, DSG-LFI-002D1, DSG-LFI-002D3, DSG-LFI-002F, DSG-LFI-005B

### VAL-LFI-005B1

Run the rooted block-tree quality tool in optional user-query diagnostic recall
mode against a representative stored tree and one or more supplied query
embeddings.

**Pass condition:** when this optional mode is implemented, the report labels
the result as `diagnostic recall`, computes Recall@1, Recall@5, and Recall@10
for each supplied query, emits exact and approximate neighbors for comparison,
excludes those results from aggregate recall statistics and histograms, reports
the per-query touched-block counts, serialized bytes read, and advisory RTT
estimate for each diagnostic query, and uses the upstream LexonGraph embedding
readback API for any rooted stored embedding reconstruction needed by the
comparison.

**Traces to:** RQ-INDEXER-008D2, RQ-INDEXER-008D3, RQ-INDEXER-008D4, RQ-INDEXER-008D5, DSG-LFI-002D2, DSG-LFI-002D3, DSG-LFI-002F, DSG-LFI-007D

### VAL-LFI-005B2

Run the rooted block-tree quality tool twice against the same representative
stored tree with identical corpus-based TNN-recall sample and seed settings but
different traversal-width values.

**Pass condition:** the corpus-based TNN-recall path accepts each traversal
width, preserves the rooted corpus and seeded sampling contract, and records
the selected traversal width in the report so operators can compare measurement
runs across widths without ambiguity while continuing to source any rooted
stored embedding reconstruction through the upstream LexonGraph readback API.

**Traces to:** RQ-INDEXER-008D1, DSG-LFI-002D1, DSG-LFI-002F, DSG-LFI-007D

### VAL-LFI-005B3

Run the rooted block-tree quality tool against a representative stored tree and
query workload whose approximate-neighbor path touches more than one block level
and whose touched block sizes are independently knowable from the shared block
store.

**Pass condition:** for each executed rooted query, the report identifies the
unique touched-block count, serialized bytes read, and per-level breakdowns for
those two measures; the same report includes aggregate totals across the
executed query set; and the advisory RTT estimate for each query equals the sum
of the per-level `ceil(level_bytes / 65536)` contributions under the fixed 64
KiB congestion-window model.

**Traces to:** RQ-INDEXER-008D4, RQ-INDEXER-008D5, DSG-LFI-002D3, DSG-LFI-005B, DSG-LFI-007D

### VAL-LFI-005C

Run the rooted CLI search tool against a representative stored tree whose
reachable rooted snapshot contains more searchable leaf nodes than the selected
`k`.

**Pass condition:** the tool generates a query embedding through the
caller-supplied endpoint, searches only the leaf nodes reachable from the
caller-supplied root through subordinate `lexongraph-search` usage, returns the
top `k` matching leaf nodes, and emits the same rooted result set on the
human-readable and JSON output surfaces.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-006, RQ-INDEXER-008E, DSG-LFI-002E,
DSG-LFI-005C, DSG-LFI-006A, DSG-LFI-007E

### VAL-LFI-005D

Run the rooted block-copy tool from one representative source store to one
representative destination store using one or more caller-supplied root block
identifiers, where the selected source and destination profiles come from the
approved shared target set, at least one exercised non-local side uses either
the approved `production-v2` profile or the approved read-only `gateway-http3`
profile on the source side, the destination already contains at least one
reachable block, the source rooted graph contains at least one unreachable
block, and the invocation encounters at least one copy failure for a reachable
block after the tool has already proven other reachable blocks can be copied or
skipped.

**Pass condition:** the tool traverses only the immutable blocks reachable from
the caller-supplied roots in the source store and copies those blocks to the
destination without re-encoding payload bytes or requiring a backend-specific
transfer path. In the default mode it skips blocks that are already present at
the destination while continuing the rooted transfer successfully and reports
requested-root, copied-block, skipped-already-present, and failed-block
outcomes on both human-readable and machine-readable output surfaces. In the
opt-in blind-write mode it skips destination existence reads, attempts writes
directly, and reports attempted-write plus failure-oriented outcomes without
claiming exact skipped-already-present classification. Both modes must support
one operator-selectable bounded destination-write concurrency limit with first
approved default `64`, and the same limit must allow multiple destination
writes to remain in flight whenever a block has already been classified for
publication. That bounded write pipeline must not cause unreachable blocks to be
written, must not require a backend-specific transfer path, and must not change
the truthfulness of the approved mode-specific reporting contract merely because
write completions arrive out of order. Both modes must emit basic default
in-flight liveness or progress on the normal CLI output surface before final
completion when the rooted copy runs long enough that silence would otherwise
resemble a hang, and both leave mutable references such as current-root and
replay-journal-head unchanged. When the source profile is `gateway-http3`,
gateway `404` responses count as missing-block source reads while transport,
protocol, or other non-success responses remain explicit failures, and the
destination side still uses one of the writable approved profiles.

**Traces to:** RQ-INDEXER-005B, DSG-LFI-005D, DSG-LFI-005E, DSG-LFI-007G

### VAL-LFI-006

Inspect the preserved production environment profile boundary.

**Pass condition:** production-oriented storage and embedding identifiers remain
behind the same `BlockStore` and `EmbeddingProvider` selection boundary as the
local/testing profile, and no local-only assumptions leak into the core batch
contract or content-model abstractions. The preserved non-local storage
identifier family must describe exactly the approved production profile set:
the existing overlay-backed `production` target, the additive direct
Azure-backed `production-v2` target, and for read-only surfaces the additive
`gateway-http3` target, rather than one-off plain Azure-only tool modes or a
write-bearing reinterpretation of the gateway-backed profile.

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
indexing visibility and observer-driven streaming visibility across planning
and final materialization when the selected stage includes clustering, and the
same log surface continues to carry richer live telemetry from newer upstream
observer revisions without requiring a separate control-plane service.

**Traces to:** RQ-INDEXER-008B, DSG-LFI-002A

### VAL-LFI-007B

Run the clustering-only stage twice against an unchanged clustering-eligible
block-store snapshot.

**Pass condition:** the same clustering-eligible block set surfaced by the
approved immutable replay-audit journal produces the same logical clustering
result on repeated standalone clustering runs, without requiring repository-
local duplicate-suppression logic, as long as the selected published profile
version and journal head are unchanged.

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

### VAL-LFI-007D

Run the clustering-only stage against a block-store snapshot large enough to
reconstruct more than one replay batch before the first upstream
planning-pass-completion wait.

**Pass condition:** the normal batch log stream emits one repository-owned
progress update after each replay-batch submission that reports completed
batches and cumulative delegated-item submission relative to the known replay
total for the invocation, so an operator can observe LexonArchiveBuilder-owned
submission progress before any upstream in-phase counts are available.

**Traces to:** RQ-INDEXER-003E, RQ-INDEXER-008B, DSG-LFI-001E, DSG-LFI-002A

### VAL-LFI-007E

Run a clustering-only stage through the point where all replay batches have been
submitted and upstream planning pass completion remains outstanding.

**Pass condition:** the normal batch log stream emits an explicit handoff
message when repository-owned replay submission completes and the runtime begins
waiting for upstream planning-pass completion, and later upstream observer
heartbeats remain distinguishable from that local handoff rather than implying
that additional replay batches are still being submitted.

**Traces to:** RQ-INDEXER-008B, DSG-LFI-002A, DSG-LFI-002B

### VAL-LFI-007F

Run a clustering-enabled stage against a latest-upstream build that emits live
hierarchy-planning telemetry and heartbeat-style in-progress status updates.

**Pass condition:** the normal batch log stream projects those telemetry events
onto the same repository-owned progress surface, preserves distinguishable
rendering for planning-pass, hierarchy-stage, and materialization progress, and
does not require operators to consult a second live telemetry interface, even
though additive completed-pass convergence summaries and intra-pass planning
records may also be mirrored to a dedicated discoverable sink.

**Traces to:** RQ-INDEXER-003I, RQ-INDEXER-008B, DSG-LFI-001I, DSG-LFI-002B

### VAL-LFI-007G

Inspect progress output from a run where upstream observer events report counts
with different semantics across planning-pass, hierarchy-planning, and
bottom-up assembly phases.

**Pass condition:** repository-visible progress messages make it clear when a
count refers to invocation-total delegated items, stage-local processed work,
or layer-local block or group totals, so newer upstream telemetry does not
create misleading operator-visible count interpretations.

**Traces to:** RQ-INDEXER-008B, DSG-LFI-002A, DSG-LFI-002B

### VAL-LFI-007G1

Run a clustering-enabled execution that completes at least two planning passes
and whose delegated pass-completion surface exposes representative planning
summary metrics.

**Pass condition:** the normal batch-progress stream announces the active
dedicated planning-telemetry sink binding when one exists, each completed planning
pass emits one additive convergence summary on the normal progress stream, and
the same logical pass summary is discoverable through the dedicated sink. Each
summary identifies the effective selected published profile version plus the
delegated contract family actually used for the run, and when the delegated API
exposes them it also carries pass number, observed item count, planned versus
terminal partition counts, hierarchy depth, requested versus realized planning
cluster counts, and any exposed planning-quality or planning-balance metrics.

**Traces to:** RQ-INDEXER-003F, RQ-INDEXER-003I, RQ-INDEXER-008B, DSG-LFI-001G, DSG-LFI-001I, DSG-LFI-002A, DSG-LFI-002B1

### VAL-LFI-007G2

Run a clustering-enabled execution on an upstream revision that emits v2
intra-pass planning observer updates before pass completion.

**Pass condition:** the normal batch-progress stream keeps those updates on the
existing repository-owned progress surface, makes it clear which entries are
live within-pass observations rather than completed-pass convergence summaries,
and renders any exposed pass progress, pending partition detail, trainer
subphase summaries, and suspected-stall indicators without inventing stronger
completion claims than the delegated observer emitted. When a dedicated per-run
planning telemetry sink is active, the same invocation writes additive
intra-pass records there rather than creating a second observability artifact.

**Traces to:** RQ-INDEXER-003F, RQ-INDEXER-003I, RQ-INDEXER-008B, DSG-LFI-001I, DSG-LFI-002B, DSG-LFI-002B2

### VAL-LFI-007G3

Run a clustering-enabled execution that emits at least two completed planning
pass summaries and also exposes at least one within-pass status update with
blocked-on detail.

**Pass condition:** the repository-owned convergence-diagnosis surface lets a
user determine, without manually correlating every raw telemetry record,
whether the run appears to be converging and what it is currently or last known
to be blocked on. The surfaced diagnosis distinguishes completed-pass trend
evidence from live or last-known blocked-on evidence and explicitly marks the
result inconclusive when delegated telemetry is insufficient to support a
stronger conclusion.

**Traces to:** RQ-INDEXER-008B1, DSG-LFI-002B1, DSG-LFI-002B2, DSG-LFI-002B3

### VAL-LFI-007G4

Run a clustering-enabled execution that emits planning telemetry and then ends
without confirmed planning completion under a repository-owned non-converged
termination path.

**Pass condition:** the request-adjacent planning-telemetry artifact family
contains deterministic post-run convergence-diagnosis evidence for that run,
including the effective run identity, the latest completed-pass trend evidence,
the latest blocked-on evidence, and an explicit indication when the diagnosis
remains inconclusive. The same behavior remains additive to the existing
runtime-progress and request-adjacent telemetry surfaces and does not require a
new MCP-visible or control-plane surface.

**Traces to:** RQ-INDEXER-008B2, RQ-INDEXER-010A, DSG-LFI-002B3

### VAL-LFI-007H

Run a clustering-enabled execution that reaches the point where the clustering
candidate set and effective delegated clustering configuration are known, and
then fails during delegated clustering or clustering-dependent materialization.

**Pass condition:** the normal batch log stream identifies the exact
repository-visible clustering input set for the failed attempt and the
effective delegated clustering configuration used for that attempt, and the
runtime writes the same failure diagnostics to a request-adjacent artifact in
the `--summary-out` directory when present or otherwise in the `--request`
directory. Those failure diagnostics also include compact embedding-health
evidence plus a small suspicious-input sample sufficient to distinguish
degenerate-embedding cases such as zero vectors, repeated vectors, non-finite
values, or collapsed variance without persisting every raw embedding vector.
When the upstream failure surface exposes a narrower failing partition or
subproblem, the diagnostics also identify that exact failing subset; otherwise
they identify the narrowest repository-visible subset LexonArchiveBuilder can
prove was active at the failing step. If artifact persistence fails, the log
output still contains enough diagnostic detail to identify the failed input
set, effective delegated configuration, failing subset, and embedding-health
failure signature without relying on the artifact.

**Traces to:** RQ-INDEXER-008C, DSG-LFI-002C

### VAL-LFI-007I

Run one representative indexer command that exercises an underlying Azure SDK
or HTTP-client path twice: once with `RUST_LOG` unset and once with a filter
that enables the relevant SDK or transport components.

**Pass condition:** with `RUST_LOG` unset, the command preserves the normal
quiet default operator output apart from already-approved repository messages;
with `RUST_LOG` set, the same process emits underlying SDK or transport
diagnostics on the existing process output streams without requiring a new
repository-specific CLI flag, command-specific switch, daemon, or MCP-visible
diagnostics surface.

**Traces to:** RQ-INDEXER-005C, DSG-LFI-002G

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
redefine their public semantics. If the package enables opt-in SDK diagnostics,
it does so by initializing the standard Rust logging path around those
dependencies rather than by redefining upstream SDK behavior or introducing a
repository-owned parallel diagnostics protocol.

**Traces to:** RQ-INDEXER-005C, RQ-INDEXER-010A, DSG-LFI-001, DSG-LFI-002G, DSG-LFI-011

### VAL-LFI-009B

Inspect the specification package for the rooted block-tree quality increment
against MCP and upstream block-validity boundaries.

**Pass condition:** the package keeps the assessment tool on a CLI-only operator
surface, does not redefine MCP retrieval or search behavior, and does not
reinterpret advisory embedding-space quality heuristics as new upstream
LexonGraph block-validity rules. The package also keeps quantile-bin selection
behind a repository-defined default rather than introducing a new operator
parameter surface in this increment. The package also keeps corpus-based recall
as the only automated quality metric and preserves user-query recall as an
optional diagnostic-only aid.

**Traces to:** RQ-INDEXER-008D, RQ-INDEXER-008D1, RQ-INDEXER-008D2, RQ-INDEXER-008D3, RQ-INDEXER-009, RQ-INDEXER-010A, DSG-LFI-002D, DSG-LFI-002D1, DSG-LFI-002D2, DSG-LFI-009, DSG-LFI-011

### VAL-LFI-009C

Inspect the specification package for the rooted CLI search increment against
MCP and upstream search-boundary constraints.

**Pass condition:** the package keeps rooted CLI search additive to the MCP
search surface, uses subordinate `lexongraph-search` semantics instead of a
repository-local search algorithm, and does not invent a second repository-local
search corpus model outside the approved rooted-tree plus `BlockStore`
boundaries.

**Traces to:** RQ-INDEXER-008E, RQ-INDEXER-009, RQ-INDEXER-010A, DSG-LFI-002E,
DSG-LFI-005C, DSG-LFI-009, DSG-LFI-011

### VAL-LFI-009D

Inspect the specification package for the rooted block-copy increment against
MCP, mutable-reference, and upstream block-store-boundary constraints.

**Pass condition:** the package keeps rooted block copy on a CLI-only operator
surface, reuses existing upstream block-store implementations rather than
specifying a repository-local backend family, limits the increment to
caller-selected roots plus their reachable immutable blocks, and keeps mutable
reference copying plus whole-store replication out of scope for this increment.
The same package must also keep `production-v2` as an additive approved
block-store profile within the shared repository targeting contract rather than
as a copy-only storage exception, while requiring default rooted-copy liveness
on the normal CLI surface rather than making ordinary operators opt into a
verbose-only signal to see that long-running transfer work is still active. The
same package must also preserve read-before-write classification as the default
rooted-copy behavior while allowing an explicit opt-in blind-write mode that
avoids destination reads and accepts reduced copied-versus-skipped accounting.
The same package must also keep bounded asynchronous destination-write
concurrency behind one operator-selectable CLI limit with first approved
default `64`, and apply that bounded write path to both rooted-copy modes only
after the repository has determined that a destination write is actually
required.

**Traces to:** RQ-INDEXER-005B, RQ-INDEXER-009, RQ-INDEXER-010A, DSG-LFI-005D,
DSG-LFI-007G, DSG-LFI-009, DSG-LFI-011

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
