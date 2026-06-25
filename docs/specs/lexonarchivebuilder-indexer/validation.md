# LexonArchiveBuilder Indexer Validation

## Status

Validation patch for the approved email-artifact, chunk-level
indexing, local filesystem block-store interoperability, replay-based
streaming delegated indexing, stage-selectable execution, standalone
clustering input discovery, published-profile API adoption,
published-profile version selection, latest published-profile and telemetry
compatibility, upstream regression assessment, replay-submission and
streaming-status observability, clustering-failure diagnostics, rooted
block-tree quality assessment with rooted TNN-recall diagnostics, rooted
CLI search over stored trees, replay-stable fingerprinting, temporary
upstream `main` tracking for rapid profile validation, upstream
wgpu-acceleration revision compatibility, 0.6.x published-profile
evaluation, local testing sweep automation, upstream embedding-readback
API adoption, LAB-owned replay-journaled split-stage recovery, and
layer-parallel block-construction evolution in
`docs/specs/lexonarchivebuilder-indexer/requirements.md` and
`docs/specs/lexonarchivebuilder-indexer/design.md`.

## Validation Scope

These validation entries define the expected conformance surface for the
LexonArchiveBuilder-owned indexer boundary, including local filesystem
block-store interoperability, replay-based streaming delegated indexing,
stage-selectable execution, standalone clustering input discovery,
published-profile API adoption, caller-selectable published-profile
configuration with default `0.1.0`, latest published-profile and telemetry
compatibility, temporary upstream `main` tracking for rapid profile
validation, upstream wgpu-acceleration revision compatibility, 0.6.x
published-profile evaluation, local testing sweep automation, upstream
embedding-readback API adoption, upstream regression assessment,
embedding-phase batch-progress observability,
replay-submission observability, streaming-status observability,
telemetry-count-semantics clarity, clustering-failure diagnostics, rooted
block-tree quality assessment with rooted TNN-recall diagnostics, rooted CLI
search over stored trees, replay-stable fingerprinting, LAB-owned replay-journaled
split-stage recovery, and leaf-layer parallel block scheduling in the
local/testing profile.

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

**Pass condition:** previously committed replay-journal records remain readable,
an incomplete trailing write does not invalidate earlier committed records, and
subsequent resume logic can distinguish completed replayable work from
uncommitted trailing progress.

**Traces to:** RQ-INDEXER-003E1, RQ-INDEXER-003E2, RQ-INDEXER-008,
DSG-LFI-001F1

### VAL-LFI-002I

Run the clustering-plus-block-assembly stage against a configured local
filesystem-backed block store that already contains representative delegated
blocks, replay metadata, a valid replay journal, and an empty request item
collection.

**Pass condition:** LexonArchiveBuilder prefers the replay journal as the
clustering-only replay-input source, reconstructs the deterministic replay input
needed by the streaming indexer without rescanning the whole store in the common
case, excludes artifacts outside the approved replay-input surface, and performs
clustering or block assembly without requiring a prior LexonArchiveBuilder
summary manifest.

**Traces to:** RQ-INDEXER-002, RQ-INDEXER-003E, RQ-INDEXER-003E1,
RQ-INDEXER-004F, RQ-INDEXER-010A, DSG-LFI-001E, DSG-LFI-001F,
DSG-LFI-001F1

### VAL-LFI-002I1

Run the clustering-plus-block-assembly stage against a configured block store
that contains representative delegated blocks but lacks a valid replay journal
for the selected store snapshot.

**Pass condition:** LexonArchiveBuilder falls back to the upstream block-iteration
contract, reconstructs the deterministic replay input needed by the streaming
indexer from the compatibility path, and preserves the same clustering-only
result contract rather than failing solely because the journal is absent.

**Traces to:** RQ-INDEXER-003E, RQ-INDEXER-008, DSG-LFI-001E, DSG-LFI-001F

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
default profile `0.1.0`, explicit selection preserves the same contract shape,
refreshing the adopted upstream dependency state can add newly published
selector targets in the active `0.6.x` series without changing that default
behavior, and
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
is `0.1.0`.

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

### VAL-LFI-002N

Inspect the latest LexonGraph upgrade boundary against the repository-required
indexer contract.

**Pass condition:** the upgrade preserves the approved external stage contract,
published-profile API adoption for clustering-enabled execution, default
profile `0.1.0` plus explicit profile-version selection, retirement of the old
low-level clustering control family, deterministic split-stage replay,
repository-owned progress projection, projection of the latest upstream live
telemetry and heartbeat events, unchanged MCP search-serving behavior for
already-indexed content, and temporary explicit tracking of LexonGraph `main`
for rapid profile validation and upstream wgpu acceleration without new
repository-visible low-level controls. This includes refreshing the adopted
dependency state so newly published profile versions in the active `0.6.x`
series become selectable while omitted selectors still resolve
to `0.1.0`, with earlier `0.5.x` alignment retained only as prior comparison
context and `0.4.x` retained as historical context, or else any missing
capability is classified explicitly as an upstream regression or compatibility
finding rather than being silently dropped.

**Traces to:** RQ-INDEXER-003I, DSG-LFI-001I

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
tree when it reads through the same `BlockStore` boundary.

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
LexonGraph embedding readback API rather than a repository-local decoder.

**Traces to:** RQ-INDEXER-005, RQ-INDEXER-008D, RQ-INDEXER-008D1, DSG-LFI-002D, DSG-LFI-002D1, DSG-LFI-002F, DSG-LFI-005B

### VAL-LFI-005B1

Run the rooted block-tree quality tool in optional user-query diagnostic recall
mode against a representative stored tree and one or more supplied query
embeddings.

**Pass condition:** when this optional mode is implemented, the report labels
the result as `diagnostic recall`, computes Recall@1, Recall@5, and Recall@10
for each supplied query, emits exact and approximate neighbors for comparison,
excludes those results from aggregate recall statistics and histograms, and
uses the upstream LexonGraph embedding readback API for any rooted stored
embedding reconstruction needed by the comparison.

**Traces to:** RQ-INDEXER-008D2, RQ-INDEXER-008D3, DSG-LFI-002D2, DSG-LFI-002F, DSG-LFI-007D

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
indexing visibility and observer-driven streaming visibility across planning
and final materialization when the selected stage includes clustering, and the
same log surface continues to carry richer live telemetry from newer upstream
observer revisions without requiring a separate control-plane service.

**Traces to:** RQ-INDEXER-008B, DSG-LFI-002A

### VAL-LFI-007B

Run the clustering-only stage twice against an unchanged clustering-eligible
block-store snapshot.

**Pass condition:** the same clustering-eligible block set surfaced by the
approved replay-input source produces the same logical clustering result on
repeated standalone clustering runs, whether discovery uses the replay journal
or the compatibility fallback path, without requiring repository-local
duplicate-suppression logic, as long as the selected published profile version
is unchanged.

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
does not require operators to consult a second telemetry interface.

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
