# Indexer Requirements

## Document Status

- **Phase:** Phase 1 - Requirements Discovery
- **Status:** Approved streaming-indexer migration baseline with incremental requirements patches for latest LexonGraph planning-policy and telemetry compatibility, upstream regression assessment, and clustering-failure diagnostics
- **Scope:** LexonArchiveBuilder indexer integration boundary plus incremental email-artifact, chunk-indexing, local block-store interoperability, replay-based streaming delegated indexing, stage-selectable execution, standalone clustering input discovery, clustering-algorithm selection, clustering-option exposure, latest planning-policy and telemetry compatibility, upstream regression assessment, embedding-phase, replay-submission and streaming-status observability, clustering-failure diagnosability, and layer-parallel block-construction evolution

## USER-REQUEST

- **UR-1 [KNOWN]:** Create specs under `docs/specs/lexonarchivebuilder-indexer/{requirements|design|validation}.md`.
- **UR-2 [KNOWN]:** The first requirement spec is for the indexer.
- **UR-3 [KNOWN]:** LexonArchiveBuilder does not perform indexing itself. It delegates indexing and index creation to LexonGraph indexing crates and provides concrete implementations for content resolution and block storage integration.
- **UR-4 [KNOWN]:** The indexer runs as a Linux Docker container in batch mode.
- **UR-5 [KNOWN]:** A batch accepts a collection of items to index, such as mailboxes and RFCs.
- **UR-6 [KNOWN]:** The resulting blocks are stored either on the local filesystem or in Azure Blob Storage.
- **UR-7 [KNOWN]:** Embeddings are obtained through an OpenAI-compatible HTTP embedding API, targeting either a local STAPI container or Azure OpenAI.
- **UR-8 [KNOWN]:** Batch and recovery behavior are owned by the LexonGraph API itself; produced blocks are immutable and hash-addressed, so reruns are idempotent.
- **UR-9 [KNOWN]:** The delegated streaming indexer crate defines `ContentResolver<R>`, requires deterministic content fingerprints for replay validation, and consumes `BlockStore` from `lexongraph-block-store` plus `EmbeddingProvider` from `lexongraph-embeddings-trait`.
- **UR-10 [KNOWN]:** Implement the minimal viable product of the `lexonarchivebuilder-indexer` feature using `docs/specs/lexonarchivebuilder-indexer/*` as the source of truth.
- **UR-11 [KNOWN]:** The first MVP implementation must support both initial content classes already named by the spec: mailboxes and document collections.
- **UR-12 [KNOWN]:** The first MVP implementation only needs an executable local/testing profile using local filesystem storage and a local embedding service.
- **UR-13 [KNOWN]:** Production storage and embedding integrations should remain pluggable through stable trait and configuration boundaries, but do not need an executable production realization in the first MVP.
- **UR-14 [KNOWN]:** Local/testing should be deployable as a single Docker Compose unit that brings up the indexer runtime and its local dependencies, including volumes/storage and the embedding engine, for integration-style testing.
- **UR-15 [KNOWN]:** Email indexing should stop embedding whole mailbox files and instead extract and normalize email messages, derive chunk-level retrieval units, and embed those chunks.
- **UR-16 [KNOWN]:** The canonical email artifact identity should be based on the normalized email artifact rather than the raw mailbox bytes.
- **UR-17 [KNOWN]:** Indexed email chunks should carry only minimal search-serving metadata plus a reference to the normalized email artifact so clients can use the chunk directly or retrieve the full normalized email.
- **UR-18 [KNOWN]:** LexonArchiveBuilder should reuse its hash-addressed storage approach for normalized email artifacts and, when useful, raw mailbox provenance artifacts instead of forcing clients to reconstruct emails from mailbox blobs.
- **UR-19 [KNOWN]:** This change applies to email ingestion now and must not preclude future document-specific chunking and metadata handling.
- **UR-20 [KNOWN]:** Email normalization should derive a meaningful message body for embedding while best-effort excluding common non-semantic content when practical.
- **UR-21 [KNOWN]:** Indexed email chunks should duplicate enough message metadata to satisfy the common retrieval/rendering path without always dereferencing the normalized email artifact.
- **UR-22 [KNOWN]:** Normalized email artifacts and mailbox provenance artifacts should reuse the same environment-selected `BlockStore` abstraction family as indexed LexonGraph blocks rather than introducing a second storage abstraction stack.
- **UR-23 [KNOWN]:** Email provenance should be chainable from indexed chunk to normalized email artifact to source mailbox artifact.
- **UR-24 [KNOWN]:** The first email chunking baseline may be sentence-aware and implementation-simple, but the indexing design must preserve a seam for future tokenizer-driven or more semantic chunking strategies.
- **UR-25 [KNOWN]:** Mailbox artifacts should be retained as first-class provenance artifacts so LexonArchiveBuilder can support re-normalization, re-chunking, and re-ingestion from the original source material.
- **UR-26 [KNOWN]:** Remove the repository-local `LocalFilesystemBlockStore` and replace it with the LexonGraph `lexongraph-block-store-fs` crate for the local/testing filesystem-backed block-store realization.
- **UR-27 [KNOWN]:** The current repository-local filesystem store breaks `lexongraph-block-inspect` interoperability because it uses a different on-disk naming scheme than LexonGraph's filesystem block-store tools expect.
- **UR-28 [KNOWN]:** It is acceptable for this change to require a fresh or rebuilt local block store; continued read compatibility with blocks written by the superseded custom local layout is not required.
- **UR-29 [KNOWN]:** Mailbox batch inputs must accept mailbox source files ending in `.mail` as well as `.mbox`.
- **UR-30 [KNOWN]:** For this increment, mailbox source compatibility should be limited to exactly `.mail` and `.mbox` rather than broadened to arbitrary mailbox archive extensions.
- **UR-31 [KNOWN]:** LexonGraph indexing APIs have been replaced by a replay-based streaming indexer lifecycle, and LexonArchiveBuilder should switch from the current delegated indexing path to that streaming surface.
- **UR-32 [KNOWN]:** LexonArchiveBuilder should emit visible progress logs while mailboxes are processed and delegated items are indexed so operators can distinguish forward progress from a hung batch.
- **UR-33 [INFERRED]:** Progress reporting should stay on the existing batch-runtime logging surface rather than introducing a separate control-plane or telemetry service for this increment.
- **UR-34 [KNOWN]:** Processing of both leaf and node blocks may occur concurrently within a construction layer; synchronization is only required across layers.
- **UR-35 [KNOWN]:** LexonArchiveBuilder should use up to an administrator-defined number of cores for this work, with the default set to one half of the number of physical CPUs.
- **UR-36 [INFERRED]:** Introducing layer-parallel block processing must preserve the existing indexing contract, including stable logical outputs and search-serving separation.
- **UR-37 [KNOWN]:** Limit the current implementation scope to leaf-layer concurrency for now because that is where the expensive embedding generation occurs; higher-layer concurrency remains future work.
- **UR-38 [KNOWN]:** Provide a command-line option to control which indexing stage runs.
- **UR-39 [KNOWN]:** Allow callers to run only mailbox ingestion plus embedding generation or only clustering and block assembly.
- **UR-40 [KNOWN]:** Standalone clustering should examine all clustering-eligible blocks currently available in the configured block store by using the new LexonGraph block-iteration API.
- **UR-41 [KNOWN]:** LexonGraph streaming indexing now exposes a status-observer seam across training and finalization, and LexonArchiveBuilder should project that visibility onto its runtime progress surface so slow indexing work can be monitored.
- **UR-42 [KNOWN]:** Stage selection should be exposed on both the CLI and the `BatchRequest` contract rather than being CLI-only.
- **UR-43 [KNOWN]:** An ingestion-and-embedding-only run should preserve the existing `BatchSummary` contract rather than introducing a stage-specific partial summary shape.
- **UR-44 [KNOWN]:** Update the LexonGraph Rust crates to the latest version, which contains a significant API change.
- **UR-45 [KNOWN]:** Rebuild the LexonArchiveBuilder indexer code to use the new LexonGraph streaming indexer.
- **UR-46 [KNOWN]:** Preserve the current external stage contract (`full`, `ingestion+embedding`, `clustering+block-assembly`) and adapt it internally to the streaming lifecycle.
- **UR-47 [KNOWN]:** Preserve current MCP search and retrieval behavior for already-indexed content; required changes should stay confined to indexing-time orchestration and its tests.
- **UR-48 [KNOWN]:** The new LexonGraph streaming indexer exposes a caller-visible replay lifecycle: one or more full training passes, explicit training completion, then final materialization replay.
- **UR-49 [INFERRED]:** LexonArchiveBuilder must preserve deterministic delegated item ordering and stable content fingerprints across streaming passes and finalization replay.
- **UR-50 [KNOWN]:** The latest LexonGraph streaming indexer update now requires callers to select a clustering algorithm and provide algorithm-specific options.
- **UR-51 [KNOWN]:** LexonArchiveBuilder should expose clustering algorithm selection and supported clustering options through the command line.
- **UR-52 [KNOWN]:** Reasonable defaults are acceptable for clustering settings the caller does not specify.
- **UR-53 [KNOWN]:** The upstream built-in clustering choices currently exposed by LexonGraph are DCBC and directional PCA, and they do not share the same option set.
- **UR-54 [KNOWN]:** The current builder can report mailbox processing and delegated-item preparation, then remain silent during long-running embedding work even while the local embedding service is actively consuming CPU.
- **UR-55 [INFERRED]:** Progress visibility for ingestion-plus-embedding execution should remain continuous across the gap between delegated-item preparation and the first downstream streaming-status event so operators can distinguish slow embedding work from a hung batch.
- **UR-56 [KNOWN]:** When a clustering-enabled run omits `cluster_count`, LexonArchiveBuilder should auto-size the effective cluster count from the number of blocks being clustered and the embedding size instead of falling back to a small fixed default.
- **UR-57 [KNOWN]:** This auto-sizing rule should apply consistently to both built-in clustering algorithms currently exposed by LexonGraph.
- **UR-58 [KNOWN]:** An explicit caller-supplied `cluster_count` should continue to override auto-sizing; the derived count is only for the omitted-option path.
- **UR-59 [KNOWN]:** During clustering-only replay, LexonArchiveBuilder should report repository-owned replay-batch submission progress using the batch count and cumulative delegated-item count it already knows, so operators can see how much work has been submitted to the streaming API.
- **UR-60 [KNOWN]:** When LexonArchiveBuilder finishes submitting replay batches and begins waiting for upstream planning-pass completion, the runtime progress stream should emit an explicit phase-boundary message so operators can distinguish local submission progress from upstream planning-pass heartbeats.
- **UR-61 [KNOWN]:** Adapt LexonArchiveBuilder to the latest LexonGraph version currently published on the upstream `main` branch.
- **UR-62 [KNOWN]:** The latest LexonGraph streaming indexer replaces the older training-oriented built-in clustering factory surface with a planning-policy surface, including `HierarchicalPlanningPolicy`, `BuiltInPlanningPolicy`, planning passes, explicit planning completion, hierarchy-planning status phases, and bottom-up assembly status phases.
- **UR-63 [KNOWN]:** Preserve the current external stage contract (`full`, `ingestion+embedding`, `clustering+block-assembly`) and existing MCP search or retrieval behavior for already-indexed content while adapting to the latest upstream indexing API.
- **UR-64 [KNOWN]:** Determine whether the latest LexonGraph update regressed any repository-required features or only changed the API shape, so any true upstream regression can be fixed explicitly rather than hidden by narrowing LexonArchiveBuilder behavior.
- **UR-65 [INFERRED]:** LexonArchiveBuilder currently depends on repository-owned behavior layered on top of the upstream indexing crate, including deterministic split-stage replay, explicit built-in algorithm selection for `dcbc` and `directional-pca`, omitted `cluster_count` auto-sizing, and runtime progress projection that hides raw upstream lifecycle details.
- **UR-66 [INFERRED]:** If the latest upstream contract no longer exposes a repository-required capability, LexonArchiveBuilder must surface that incompatibility explicitly in requirements, design, and implementation review rather than silently dropping the affected behavior during adaptation.
- **UR-67 [KNOWN]:** Update LexonArchiveBuilder to the latest LexonGraph `main` revision again because the upstream streaming indexer now exposes richer live telemetry through the status-observer surface.
- **UR-68 [KNOWN]:** LexonArchiveBuilder should project the new upstream telemetry onto the existing runtime progress/log surface.
- **UR-69 [KNOWN]:** The latest upstream observer surface now emits live hierarchy-planning stage telemetry and periodic heartbeat-style in-progress status updates during long-running planning and materialization phases.
- **UR-70 [INFERRED]:** LexonArchiveBuilder should preserve operator-understandable progress semantics when upstream telemetry mixes repository-total counts, stage-local progress counts, and materialization-layer counts rather than exposing those raw count semantics ambiguously.
- **UR-71 [INFERRED]:** This telemetry upgrade must preserve the current external stage contract and unchanged MCP search or retrieval behavior for already-indexed content rather than broadening the user-visible surface beyond runtime progress.
- **UR-72 [KNOWN]:** Current clustering failures do not report which repository-visible clustering inputs were in the failing attempt or which effective delegated clustering parameters LexonArchiveBuilder passed downstream.
- **UR-73 [KNOWN]:** When clustering fails, operators need to know what nodes were being clustered and what parameters were passed so the failure is diagnosable without reproducing the run under a debugger.
- **UR-74 [KNOWN]:** The required clustering-failure diagnostics must be emitted on the runtime log and in a request-adjacent diagnostic artifact rather than on a new control-plane or MCP surface.
- **UR-75 [KNOWN]:** Detailed clustering diagnostics are required only for failure cases in this increment; successful clustering runs do not need the same verbose input inventory.

## Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-INDEXER-001 | Add | Introduce the first structured requirements artifact for the LexonArchiveBuilder indexer boundary | UR-1, UR-2 |
| CM-INDEXER-002 | Add | Define LexonArchiveBuilder as an orchestration and adapter layer around LexonGraph indexing crates, not an indexing engine | UR-3 |
| CM-INDEXER-003 | Add | Define batch-container execution, supported initial content inputs, storage targets, and embedding-provider targets | UR-4, UR-5, UR-6, UR-7 |
| CM-INDEXER-004 | Add | Capture invariants around delegated idempotence, immutable blocks, and separation from MCP search-serving behavior | UR-8 |
| CM-INDEXER-005 | Revise | Narrow the first in-repo MVP realization to an end-to-end local/testing profile while preserving production extensibility boundaries | UR-10, UR-12, UR-13 |
| CM-INDEXER-006 | Revise | Require the first MVP implementation to cover both mailbox and document-collection batch items | UR-10, UR-11 |
| CM-INDEXER-007 | Add | Require Docker Compose-based local dependency orchestration for repeatable integration testing of the batch container | UR-12, UR-14 |
| CM-INDEXER-008 | Revise | Refine email ingestion so mailbox inputs expand into normalized email artifacts and chunk-level embedding units instead of whole-mailbox embeddings | UR-15, UR-19 |
| CM-INDEXER-009 | Add | Require normalized email artifacts to be hash-addressed, retrievable by reference from indexed chunks, and anchored in LexonArchiveBuilder-owned storage rather than client-side mailbox parsing | UR-16, UR-17, UR-18 |
| CM-INDEXER-010 | Add | Define email-body normalization, common-case chunk metadata duplication, shared storage abstractions, and chained provenance for email indexing artifacts | UR-20, UR-21, UR-22, UR-23 |
| CM-INDEXER-011 | Add | Establish a simple sentence-aware email chunking baseline while requiring retained mailbox provenance and future chunking extensibility | UR-24, UR-25 |
| CM-INDEXER-012 | Revise | Require the local/testing filesystem-backed block-store realization to stay interoperable with LexonGraph filesystem store tooling and naming/layout expectations | UR-26, UR-27 |
| CM-INDEXER-013 | Add | Explicitly allow the local/testing filesystem store transition to require a fresh or rebuilt local store rather than preserving reads from the superseded custom layout | UR-28 |
| CM-INDEXER-014 | Revise | Expand mailbox source compatibility so mailbox batch items may reference `.mail` or `.mbox` files without widening the first increment to arbitrary archive extensions | UR-29, UR-30 |
| CM-INDEXER-015 | Revise | Require LexonArchiveBuilder to adopt LexonGraph's replay-based streaming indexing APIs instead of relying on the retired one-shot or pre-streaming delegated indexing surfaces | UR-31, UR-48 |
| CM-INDEXER-016 | Add | Require observable batch-progress logging for mailbox expansion and delegated indexing progress without introducing a new control-plane surface | UR-32, UR-33 |
| CM-INDEXER-017 | Revise | Allow delegated leaf-block work to proceed concurrently within the same construction layer while preserving cross-layer synchronization and recording higher-layer concurrency as future work | UR-34, UR-36, UR-37 |
| CM-INDEXER-018 | Add | Require an administrator-defined concurrency budget for layer-parallel block processing, defaulting to one half of detected physical CPUs with a minimum of one core | UR-35 |
| CM-INDEXER-019 | Add | Introduce stage-selectable execution so callers can run the full pipeline, ingestion plus embedding only, or clustering plus block assembly only | UR-38, UR-39, UR-42 |
| CM-INDEXER-020 | Revise | Extend the batch entrypoint contract to carry stage selection on both the CLI and `BatchRequest` while preserving the existing `BatchSummary` shape | UR-38, UR-42, UR-43 |
| CM-INDEXER-021 | Revise | Permit clustering-only requests to use an empty item collection because standalone clustering discovers its input from the configured block store rather than from request-supplied sources | UR-39, UR-40 |
| CM-INDEXER-022 | Add | Require standalone clustering to iterate all clustering-eligible blocks surfaced by the LexonGraph block-iteration API for the configured block store rather than depending on a prior LexonArchiveBuilder summary manifest | UR-39, UR-40 |
| CM-INDEXER-023 | Revise | Extend observable progress requirements to include streaming lifecycle status updates on the normal runtime progress surface | UR-41, UR-48 |
| CM-INDEXER-024 | Add | Keep stage semantics environment-neutral and content-type-neutral so future content types can participate without reshaping the batch contract | UR-39, UR-42 |
| CM-INDEXER-025 | Revise | Migrate the delegated indexing boundary from the retired `lexongraph-indexer` surface to the replay-based `lexongraph-streaming-indexer` surface while preserving LexonArchiveBuilder's adapter-orchestrator role | UR-44, UR-45, UR-48 |
| CM-INDEXER-026 | Add | Preserve the current external stage contract and MCP search-serving behavior while adapting the internals to the new streaming lifecycle | UR-46, UR-47 |
| CM-INDEXER-027 | Add | Require deterministic replay inputs, including stable delegated item ordering and content fingerprints, so streaming passes and finalization remain valid and repeatable | UR-45, UR-48, UR-49 |
| CM-INDEXER-028 | Revise | Consume upstream streaming status notifications on the existing runtime progress surface instead of relying on the superseded incremental-indexer callback seam | UR-41, UR-45, UR-48 |
| CM-INDEXER-029 | Revise | Require clustering-enabled execution to supply an explicit upstream-compatible clustering algorithm selection rather than depending on one implicit repository-default algorithm path | UR-39, UR-44, UR-50, UR-53 |
| CM-INDEXER-030 | Add | Expose clustering algorithm choice and supported algorithm-specific options on the CLI while permitting repository-owned defaults for omitted settings | UR-50, UR-51, UR-52, UR-53 |
| CM-INDEXER-031 | Add | Preserve replay-safe and environment-neutral clustering behavior by treating the effective clustering algorithm and option set as part of the approved batch orchestration contract | UR-12, UR-13, UR-39, UR-50, UR-52 |
| CM-INDEXER-032 | Revise | Tighten progress observability so ingestion-plus-embedding runs remain visibly active during long-running embedding or leaf-materialization work between mailbox expansion and downstream streaming-status events | UR-32, UR-41, UR-54, UR-55 |
| CM-INDEXER-033 | Revise | Require omitted `cluster_count` to derive from clustering input count and embedding-driven branch capacity for every supported built-in clustering algorithm while preserving explicit caller override behavior | UR-52, UR-53, UR-56, UR-57, UR-58 |
| CM-INDEXER-034 | Revise | Require clustering-only replay to emit repository-owned replay-batch submission progress that reports completed batches and cumulative delegated items relative to the known invocation total | UR-32, UR-39, UR-59 |
| CM-INDEXER-035 | Add | Require an explicit runtime-visible handoff between repository-owned replay submission and upstream planning-pass completion waiting so operator logs disambiguate local submission from upstream heartbeats | UR-41, UR-48, UR-60 |
| CM-INDEXER-036 | Revise | Adapt the delegated indexing requirements from the older training-oriented streaming surface to the latest planning-policy surface published by LexonGraph while preserving the external repository contract | UR-61, UR-62, UR-63 |
| CM-INDEXER-037 | Revise | Reframe built-in clustering configuration as a repository-owned mapping onto upstream built-in planning policy selection rather than the retired built-in clustering factory seam | UR-61, UR-62, UR-65 |
| CM-INDEXER-038 | Add | Require explicit repository-level regression assessment for capabilities relied on by LexonArchiveBuilder before any behavior is narrowed during the upstream upgrade | UR-64, UR-65, UR-66 |
| CM-INDEXER-039 | Revise | Update progress-observability requirements to map latest upstream planning, hierarchy-planning, and bottom-up assembly lifecycle signals onto the existing runtime progress surface without exposing raw upstream terminology directly | UR-62, UR-63, UR-65 |
| CM-INDEXER-040 | Add | Preserve current repository-required capabilities across the latest LexonGraph upgrade, including split-stage replay, explicit algorithm selection, omitted `cluster_count` auto-sizing, and stable MCP search-serving behavior | UR-63, UR-64, UR-65, UR-66 |
| CM-INDEXER-041 | Revise | Adapt the latest-upstream compatibility requirement from planning-policy-only alignment to planning-policy plus telemetry-surface alignment against the newest LexonGraph `main` revision | UR-67, UR-69, UR-71 |
| CM-INDEXER-042 | Revise | Tighten progress-observability requirements so LexonArchiveBuilder projects richer upstream live telemetry and heartbeat events onto the existing runtime progress surface without creating a second telemetry interface | UR-68, UR-69, UR-71 |
| CM-INDEXER-043 | Add | Preserve operator-understandable progress semantics across the telemetry upgrade by distinguishing repository-owned totals, upstream stage-local progress, and materialization-layer counts instead of surfacing ambiguous raw counts | UR-68, UR-69, UR-70 |
| CM-INDEXER-044 | Add | Require failure-only clustering diagnostics that identify the exact failing input set and effective delegated clustering configuration on the runtime log and in a request-adjacent artifact | UR-39, UR-50, UR-72, UR-73, UR-74, UR-75 |

## Before / After

### BA-INDEXER-001

- **Before [KNOWN]:** The repository had no structured requirements artifact for indexer behavior.
- **After [KNOWN]:** The repository has an explicit requirements baseline for the LexonArchiveBuilder indexer boundary in `docs/specs/lexonarchivebuilder-indexer/requirements.md`.

### BA-INDEXER-002

- **Before [KNOWN]:** `README.md` described LexonArchiveBuilder as an indexer at a high level, but did not distinguish whether indexing logic lived in-repo or was delegated externally.
- **After [KNOWN]:** The requirements define that LexonArchiveBuilder delegates indexing and index creation to LexonGraph indexing crates and is responsible for supplying environment-specific integrations around that boundary.

### BA-INDEXER-003

- **Before [KNOWN]:** Local-versus-production behavior was described only at the architecture level.
- **After [KNOWN]:** The requirements define initial indexer targets for local filesystem plus STAPI and for Azure Blob Storage plus Azure OpenAI, while keeping those choices behind stable integration boundaries.

### BA-INDEXER-004

- **Before [KNOWN]:** Idempotence and recovery ownership were not captured in repository requirements.
- **After [KNOWN]:** The requirements define rerun idempotence as inherited from LexonGraph API behavior and immutable hash-addressed blocks, rather than re-specifying batch recovery logic inside LexonArchiveBuilder.

### BA-INDEXER-005

- **Before [KNOWN]:** The requirements described both local/testing and production environment targets, but did not identify which subset must be executable in the first in-repo MVP.
- **After [KNOWN]:** The requirements define the first MVP as an end-to-end local/testing realization while preserving production storage and embedding integrations as stable extension seams.

### BA-INDEXER-006

- **Before [KNOWN]:** The requirements identified mailbox and document-collection inputs, but did not state whether the first MVP could implement only one of them.
- **After [KNOWN]:** The requirements now state that the first MVP must support both mailbox and document-collection items through the same collection-oriented batch contract.

### BA-INDEXER-007

- **Before [KNOWN]:** The requirements described Linux Docker batch execution, but did not require a repository-local composition layer for exercising dependencies together during testing.
- **After [KNOWN]:** The requirements now require a Docker Compose deployment shape for the local/testing profile so the batch runtime and its local dependencies can be brought up as one integration test unit.

### BA-INDEXER-008

- **Before [KNOWN]:** A mailbox batch item was understood as one embedding unit, which implied embedding the entire `.mbox` body as one vector through the delegated indexer contract.
- **After [KNOWN]:** The requirements define mailbox inputs as ingestion sources that LexonArchiveBuilder expands into normalized email artifacts and chunk-level embedding units before delegating indexing to the upstream LexonGraph indexing boundary.

### BA-INDEXER-009

- **Before [KNOWN]:** The requirements did not define a canonical normalized email artifact or a stable retrieval reference from indexed chunks back to full-message content.
- **After [KNOWN]:** The requirements define normalized email artifacts as hash-addressed retrieval targets referenced from indexed chunks, allowing clients to use chunk text directly or follow the artifact reference to the full normalized email without reparsing mailbox blobs.

### BA-INDEXER-010

- **Before [KNOWN]:** The requirements did not define how much email normalization should shape the embedded body, how much metadata should be duplicated onto chunk hits, whether email artifacts should reuse the repository storage abstraction, or how provenance should chain back to the mailbox source.
- **After [KNOWN]:** The requirements define best-effort email-body normalization for embedding, enough duplicated chunk metadata for the common retrieval path, reuse of the environment-selected `BlockStore` abstraction family for email artifacts, and explicit chained provenance from chunk to normalized email artifact to mailbox artifact.

### BA-INDEXER-011

- **Before [KNOWN]:** The requirements did not define whether mailbox provenance retention was mandatory or whether the first email chunking strategy should stay simple while preserving room for more semantic chunking later.
- **After [KNOWN]:** The requirements make mailbox artifact retention mandatory for reprocessing scenarios and define the first email chunking strategy as a simple sentence-aware baseline that preserves a seam for future tokenizer-driven or more semantic chunking policies.

### BA-INDEXER-012

- **Before [KNOWN]:** The requirements allowed a repository-local filesystem `BlockStore` realization without constraining its on-disk naming or layout to remain interoperable with LexonGraph's filesystem inspection tooling.
- **After [KNOWN]:** The requirements now bind the local/testing filesystem-backed block-store realization to LexonGraph's filesystem store layout expectations so `lexongraph-block-inspect` and related filesystem tooling can inspect LexonArchiveBuilder-produced local stores without repository-specific translation.

### BA-INDEXER-013

- **Before [KNOWN]:** The requirements did not state whether the local filesystem block-store transition had to preserve reads from the superseded custom layout.
- **After [KNOWN]:** The requirements now allow this interoperability fix to require a fresh or rebuilt local store, avoiding a hidden backward-compatibility obligation for the old repository-local layout.

### BA-INDEXER-014

- **Before [KNOWN]:** Mailbox batch-item compatibility implicitly assumed `.mbox` mailbox source files and did not define whether `.mail` files were valid mailbox inputs.
- **After [KNOWN]:** Mailbox batch-item compatibility explicitly accepts source files ending in `.mail` or `.mbox`, while broader mailbox archive extension support remains out of scope for this increment.

### BA-INDEXER-015

- **Before [KNOWN]:** The requirements targeted a pre-streaming delegated indexing path and did not account for LexonGraph's newer replay-based streaming lifecycle.
- **After [KNOWN]:** The requirements now define replay-based streaming delegated indexing as the preferred LexonGraph integration path so LexonArchiveBuilder can satisfy the latest upstream APIs while remaining subordinate to upstream indexing contracts.

### BA-INDEXER-016

- **Before [KNOWN]:** Batch visibility was limited to terminal success or failure plus the final summary output, so long-running mailbox expansion and indexing work could appear hung.
- **After [KNOWN]:** The requirements now define runtime-visible progress logging for mailbox processing and delegated indexing progress on the normal batch log surface.

### BA-INDEXER-017

- **Before [KNOWN]:** Incremental delegated indexing was required, but the requirements did not state whether leaf and parent or node blocks within the same construction layer could be processed concurrently.
- **After [KNOWN]:** The requirements now allow same-layer block work to execute concurrently while requiring synchronization only at cross-layer boundaries.

### BA-INDEXER-018

- **Before [KNOWN]:** The requirements did not define an operator-visible concurrency budget or default CPU-allocation rule for delegated block construction work.
- **After [KNOWN]:** The requirements now require an administrator-defined concurrency cap for same-layer block work and define the default as one half of detected physical CPUs, floored at one core.

### BA-INDEXER-019

- **Before [KNOWN]:** The proposed concurrency change treated leaf and higher-layer parent or node block construction as equally in scope for this increment.
- **After [KNOWN]:** The current increment now narrows executable concurrency to the leaf layer, where embedding work is concentrated, and records higher-layer concurrency as future work rather than an approved implementation obligation.

### BA-INDEXER-020

- **Before [KNOWN]:** The batch runtime always executed one end-to-end indexing path, and the repository requirements did not define a caller-selectable stage boundary on either the CLI or `BatchRequest`.
- **After [KNOWN]:** The requirements define one stage-selection surface that is available on both the CLI and `BatchRequest`, defaults to the full pipeline when omitted, and preserves the existing `BatchSummary` contract for every approved stage mode.

### BA-INDEXER-021

- **Before [KNOWN]:** The collection-oriented batch contract implicitly required request-supplied items for every run because all index construction began from the current request payload.
- **After [KNOWN]:** The requirements preserve request-supplied items for any stage that performs ingestion, while permitting a clustering-only run to use an empty item collection because its inputs are discovered from the configured block store.

### BA-INDEXER-022

- **Before [KNOWN]:** Parent and block-assembly work only consumed leaf blocks produced earlier in the same runtime invocation, so the requirements did not define standalone clustering input discovery.
- **After [KNOWN]:** The requirements define standalone clustering to consume all clustering-eligible blocks surfaced by the LexonGraph block-iteration API for the configured block store without depending on a prior LexonArchiveBuilder summary manifest.

### BA-INDEXER-023

- **Before [KNOWN]:** Observable progress covered mailbox processing and delegated indexing progress, but the requirements did not define a streaming status-observer seam or a unified progress stream across full-pipeline runs.
- **After [KNOWN]:** The requirements define streaming lifecycle visibility through the upstream status-observer seam and require those events to appear on the same normal runtime progress surface as mailbox and delegated-indexing progress.

### BA-INDEXER-024

- **Before [KNOWN]:** The requirements did not state whether stage selection should remain generic across content types or whether stage-specific runs would require a new result contract.
- **After [KNOWN]:** The requirements define stage selection in terms of pipeline phases rather than mailbox-specific behavior and preserve the existing `BatchSummary` contract instead of introducing a stage-specific partial schema.

### BA-INDEXER-025

- **Before [KNOWN]:** The requirements targeted the older `lexongraph-indexer` delegated indexing surface and did not account for the new replay-based streaming lifecycle now exposed by LexonGraph.
- **After [KNOWN]:** The requirements target `lexongraph-streaming-indexer` as the delegated indexing boundary and require LexonArchiveBuilder to adapt its orchestration to that replay-based lifecycle without taking ownership of upstream indexing semantics.

### BA-INDEXER-026

- **Before [KNOWN]:** The requirements did not state whether the upstream streaming lifecycle could alter the caller-visible stage contract.
- **After [KNOWN]:** The requirements explicitly preserve the existing external stage contract and keep the streaming lifecycle as an internal adaptation detail.

### BA-INDEXER-027

- **Before [KNOWN]:** The requirements did not define a repository-owned obligation to preserve deterministic delegated item ordering and stable content fingerprints across repeated upstream passes.
- **After [KNOWN]:** The requirements now constrain LexonArchiveBuilder to provide replay-safe delegated inputs so the streaming indexer can validate training and finalization replays without changing the batch contract.

### BA-INDEXER-028

- **Before [KNOWN]:** Observable progress requirements referenced the superseded incremental-indexer and clustering callback seams rather than the newer streaming status-observer surface.
- **After [KNOWN]:** The requirements now define progress visibility in terms of the upstream streaming status observer while preserving one runtime-visible progress stream for local and production-shaped execution.

### BA-INDEXER-029

- **Before [KNOWN]:** The requirements assumed one implicit delegated clustering path and did not capture that the updated LexonGraph streaming indexer now requires callers to choose a clustering algorithm and provide algorithm-specific settings.
- **After [KNOWN]:** The requirements define clustering algorithm selection as an explicit part of the clustering-enabled indexer contract while keeping algorithm execution delegated to LexonGraph.

### BA-INDEXER-030

- **Before [KNOWN]:** The requirements did not define any operator-facing surface for selecting a clustering algorithm or providing supported clustering options.
- **After [KNOWN]:** The requirements define a CLI surface that allows operators to choose a supported delegated clustering algorithm and provide supported option values, with repository-owned defaults for omitted settings.

### BA-INDEXER-031

- **Before [KNOWN]:** The requirements did not state whether clustering defaults and option values were part of the deterministic replay boundary for full-pipeline or clustering-only execution.
- **After [KNOWN]:** The requirements define the effective clustering algorithm and option set as replay-relevant orchestration input so repeated runs can remain explainable and stable under unchanged upstream semantics.

### BA-INDEXER-032

- **Before [KNOWN]:** Observable progress required mailbox-processing visibility and downstream streaming-status visibility, but it did not explicitly forbid a long silent gap while delegated items were being embedded or leaf blocks were being materialized before streaming-status events began.
- **After [KNOWN]:** Observable progress now explicitly requires continued runtime-visible activity during ingestion-plus-embedding work between delegated-item preparation and the first downstream streaming-status event so slow embedding work does not look like a hung batch.

### BA-INDEXER-033

- **Before [KNOWN]:** The requirements allowed omitted clustering settings to resolve to repository defaults, but they did not explicitly require omitted `cluster_count` to auto-size consistently across all supported built-in clustering algorithms.
- **After [KNOWN]:** The requirements now require omitted `cluster_count` to derive from clustering input count plus embedding-size-aware branch capacity for every supported built-in clustering algorithm, while preserving explicit caller-provided `cluster_count` as an override.

### BA-INDEXER-034

- **Before [KNOWN]:** Progress observability required visible mailbox, embedding, training, and finalization activity, but it did not explicitly require clustering-only replay to report repository-owned replay-batch submission progress using the known batch and delegated-item totals.
- **After [KNOWN]:** The requirements now require clustering-only replay to emit repository-owned progress after each replay batch submission, including completed-batch and cumulative delegated-item visibility relative to the known invocation total.

### BA-INDEXER-035

- **Before [KNOWN]:** Runtime progress could transition from repository-owned replay submission into upstream planning-pass heartbeats without an explicit boundary marker, so operators could not tell whether LexonArchiveBuilder was still submitting work or was already waiting for upstream pass completion.
- **After [KNOWN]:** The requirements now require an explicit runtime-visible handoff when repository-owned replay submission completes and the runtime begins waiting for upstream planning-pass completion or an equivalent delegated lifecycle boundary.

### BA-INDEXER-036

- **Before [KNOWN]:** The requirements described the delegated streaming lifecycle in terms of training passes, built-in clustering factories, and training completion because that was the upstream surface previously integrated by LexonArchiveBuilder.
- **After [KNOWN]:** The requirements describe the delegated streaming lifecycle in terms of the latest upstream planning-policy surface while preserving LexonArchiveBuilder's caller-visible stage contract and adapter-orchestrator role.

### BA-INDEXER-037

- **Before [KNOWN]:** The requirements assumed LexonArchiveBuilder would satisfy the upstream built-in clustering contract through the older `BuiltInClustering` and `BuiltInClusteringFactory` seam.
- **After [KNOWN]:** The requirements preserve the same repository-level algorithm choices and option families, but require them to map onto the latest upstream built-in planning-policy seam instead.

### BA-INDEXER-038

- **Before [KNOWN]:** The requirements preserved repository invariants across upstream API changes, but they did not explicitly require distinguishing a true upstream feature regression from a mechanical API rename or lifecycle reshaping.
- **After [KNOWN]:** The requirements explicitly require regression assessment for repository-relied-on capabilities so the upgrade cannot silently narrow behavior.

### BA-INDEXER-039

- **Before [KNOWN]:** Progress observability requirements assumed the older upstream status taxonomy that reported training and materialization phases using the prior names.
- **After [KNOWN]:** The requirements preserve operator-visible progress continuity while allowing LexonArchiveBuilder to remap the latest upstream planning, hierarchy-planning, and bottom-up assembly phases onto the same repository-owned runtime progress surface.

### BA-INDEXER-040

- **Before [KNOWN]:** The requirements preserved stage-selection and MCP invariants during the earlier streaming-indexer migration, but they did not yet enumerate the repository-required capabilities that must survive the newest planning-policy upgrade review.
- **After [KNOWN]:** The requirements explicitly preserve split-stage replay, algorithm selection, omitted `cluster_count` auto-sizing, progress projection, and unchanged MCP search-serving behavior as feature-level obligations for the latest upgrade.

### BA-INDEXER-041

- **Before [KNOWN]:** The current requirements align LexonArchiveBuilder with the latest known planning-policy surface, but they do not yet treat the newly expanded upstream telemetry surface as part of the same compatibility boundary.
- **After [KNOWN]:** The requirements now treat the latest upstream telemetry behavior as part of the upgrade boundary, so the newest LexonGraph revision must be assessed for both planning-policy compatibility and observer-surface compatibility.

### BA-INDEXER-042

- **Before [KNOWN]:** Progress observability required projection of the upstream status-observer surface, but it did not yet explicitly account for richer live hierarchy-stage telemetry and heartbeat-style in-progress events from newer upstream revisions.
- **After [KNOWN]:** The requirements now explicitly require LexonArchiveBuilder to project the richer upstream telemetry onto the same existing runtime progress surface rather than dropping it or introducing a second telemetry interface.

### BA-INDEXER-043

- **Before [KNOWN]:** The requirements assumed upstream observer counts would remain close enough to repository totals that count semantics would stay intuitive without additional clarification.
- **After [KNOWN]:** The requirements now explicitly constrain operator-facing progress to remain understandable when upstream telemetry reports stage-local work counts or layer-local materialization counts that differ from repository-total delegated-item counts.

### BA-INDEXER-044

- **Before [KNOWN]:** When delegated clustering failed, runtime-visible output could report elapsed time and the upstream error text without identifying the exact clustering input set or the effective delegated clustering configuration used for the failing attempt.
- **After [KNOWN]:** The requirements now require failure-only clustering diagnostics that identify the exact repository-visible input set and effective delegated clustering configuration on the runtime log and in a request-adjacent diagnostic artifact.

## Requirements

### Functional Requirements

#### RQ-INDEXER-001 - Batch entrypoint

LexonArchiveBuilder SHALL provide an indexer runtime that executes as a Linux Docker container in batch mode.

- **Stage control [KNOWN]:** The batch entrypoint SHALL accept a caller-selected execution stage on both the CLI surface and the `BatchRequest` contract.
- **Default [KNOWN]:** When the caller omits stage selection, the runtime SHALL execute the full approved pipeline.
- **Summary contract [KNOWN]:** The batch entrypoint SHALL preserve the existing `BatchSummary` shape for the approved stage modes rather than introducing a distinct stage-specific summary schema.
- **Rationale [KNOWN]:** This matches the intended local and production execution shape from `README.md` and the user request.
- **Traceability:** UR-2, UR-4, UR-38, UR-42, UR-43

#### RQ-INDEXER-002 - Collection-oriented input

The batch indexer SHALL accept a collection of items to index rather than a single hard-coded content source.

- **Initial supported item classes [KNOWN]:**
  - mailboxes / mail archives
  - document collections such as RFCs
- **MVP realization [KNOWN]:** The first in-repo implementation must support both initial item classes rather than deferring either one to a later increment.
- **Email ingestion refinement [KNOWN]:** A mailbox item remains a valid batch input, but it is an ingestion source rather than the final embedding unit; LexonArchiveBuilder expands mailbox content into normalized email artifacts and chunk-level index items before delegated embedding.
- **Mailbox source compatibility [KNOWN]:** In this increment, mailbox batch items may reference source files ending in `.mail` or `.mbox`.
- **Document scope boundary [KNOWN]:** Document collections remain valid batch inputs in this increment, but this change does not require document chunking to match email handling. Future document-specific chunking and metadata handling must remain possible through the same collection-oriented contract.
- **Stage-selectable exemption [KNOWN]:** A clustering-and-block-assembly-only run may use an empty item collection because its inputs are discovered from the configured block store rather than from request-supplied sources.
- **Extensibility [INFERRED]:** The accepted collection model should permit future content types without redefining the external batch contract.
- **Traceability:** UR-5, UR-11, UR-15, UR-19, UR-29, UR-30, UR-39, UR-40

#### RQ-INDEXER-003 - Delegated indexing engine

LexonArchiveBuilder SHALL delegate indexing and index creation to the `lexongraph-streaming-indexer` crate.

- **Non-goal [KNOWN]:** LexonArchiveBuilder does not define or implement its own indexing algorithm in this scope.
- **Traceability:** UR-3, UR-44, UR-45

#### RQ-INDEXER-003A - Replay-based streaming delegated indexing

LexonArchiveBuilder SHALL adapt the approved batch contract onto the replay-based streaming indexing APIs exposed by `lexongraph-streaming-indexer`.

- **Required property [KNOWN]:** The delegated indexing flow must support the latest upstream lifecycle of one or more planning passes, explicit planning completion, and final materialization replay rather than depending on the superseded training-oriented or pre-streaming surfaces.
- **External-contract stability [KNOWN]:** LexonArchiveBuilder SHALL preserve the current caller-visible stage contract (`full`, `ingestion+embedding`, `clustering+block-assembly`) and SHALL NOT expose the raw upstream streaming lifecycle directly on the CLI or `BatchRequest`.
- **Replay obligation [KNOWN]:** LexonArchiveBuilder SHALL preserve a deterministic delegated item stream, including stable item ordering and replay identity, anywhere the upstream streaming lifecycle requires caller replay.
- **Boundary [KNOWN]:** LexonArchiveBuilder still does not own index-construction semantics; it consumes upstream streaming APIs rather than reimplementing indexing behavior in-repo.
- **Compatibility note [KNOWN]:** The latest known upstream lifecycle renames the repository's previously consumed training-oriented seam to a planning-oriented seam and introduces hierarchy-planning plus bottom-up-assembly status phases behind the same delegated indexing boundary.
- **Idempotence constraint [INFERRED]:** Adapting to replay-based streaming indexing must preserve the existing immutable, hash-addressed rerun expectations for unchanged content.
- **Traceability:** UR-3, UR-8, UR-31, UR-45, UR-46, UR-48, UR-49, UR-61, UR-62, UR-63

#### RQ-INDEXER-003B - Layer-parallel delegated block processing

LexonArchiveBuilder SHALL permit delegated leaf-block processing to proceed
concurrently within the leaf construction layer.

- **Required property [KNOWN]:** Leaf work items that belong to the same
  delegated construction layer may execute independently up to the configured
  concurrency budget.
- **Synchronization boundary [KNOWN]:** Higher construction layers SHALL NOT
  begin until the leaf layer they depend on has completed the block work needed
  for parent construction.
- **Non-goal [INFERRED]:** This requirement does not redefine LexonGraph's
  block-construction semantics, parent-child relationships, or final root
  determination.
- **Future work [KNOWN]:** Concurrency for higher construction layers remains a
  future enhancement and is not required in the current increment.
- **Traceability:** UR-31, UR-34, UR-36, UR-37

#### RQ-INDEXER-003C - Administrator-defined concurrency budget

LexonArchiveBuilder SHALL expose an administrator-defined maximum concurrency budget for
layer-parallel block processing.

- **Default [KNOWN]:** When the administrator does not supply an explicit cap,
  the runtime default SHALL be `max(1, floor(physical_cpu_count / 2))`.
- **Scope [KNOWN]:** The current increment applies this concurrency budget to
  same-layer leaf work without changing the external batch contract or the
  environment-selection boundary.
- **Execution bound [INFERRED]:** The runtime may use fewer workers than the
  configured cap when a layer has fewer ready block tasks or when upstream
  constraints limit available parallelism.
- **[UNKNOWN: physical CPU detection rule inside containerized deployments and
  CPU-quota-constrained environments]**
- **Future work [KNOWN]:** Reusing or extending this budget for higher-layer
  block construction depends on future upstream API support and is not required
  in the current increment.
- **Traceability:** UR-34, UR-35, UR-37

#### RQ-INDEXER-003D - Stage-selectable execution

LexonArchiveBuilder SHALL expose stage-selectable execution modes that let callers run
the full approved pipeline, only ingestion plus embedding generation, or only
clustering plus block assembly.

- **Required surface [KNOWN]:** The same stage selector must be representable on
  the CLI and on the `BatchRequest` contract.
- **Default [KNOWN]:** Omitting the stage selector SHALL preserve the existing
  full-pipeline behavior.
- **Contract stability [KNOWN]:** Stage selection SHALL preserve the existing
  `BatchSummary` shape rather than introducing a stage-specific partial summary
  contract.
- **Extensibility [INFERRED]:** Stage names should describe generic pipeline
  phases rather than mailbox-specific behavior so future content types can
  participate without reshaping the batch contract.
- **Traceability:** UR-38, UR-39, UR-42, UR-43

#### RQ-INDEXER-003E - Standalone clustering input discovery

When clustering plus block assembly runs without a preceding ingestion stage in
the same invocation, LexonArchiveBuilder SHALL derive clustering inputs by iterating
the configured `BlockStore` through the LexonGraph block-iteration API.

- **Scope [KNOWN]:** Standalone clustering SHALL examine all clustering-eligible
  blocks surfaced by that upstream iteration contract for the configured block
  store rather than only blocks associated with a prior request or summary.
- **Filtering boundary [INFERRED]:** Blocks not surfaced by the upstream
  clustering-input iteration contract, including repository-owned artifact
  classes that are not valid clustering inputs, are outside this requirement's
  input set.
- **Request-shape implication [KNOWN]:** A clustering-only invocation may use an
  empty item collection because input discovery occurs from the configured block
  store rather than from request-supplied sources.
- **Idempotence implication [INFERRED]:** Repeating the clustering-only stage
  against an unchanged clustering-eligible block-store snapshot is expected to
  yield the same logical clustering result under unchanged upstream semantics.
- **Traceability:** UR-39, UR-40

#### RQ-INDEXER-003F - Clustering algorithm selection

For any execution stage that includes clustering plus block assembly,
LexonArchiveBuilder SHALL provide an explicit delegated clustering algorithm
selection that satisfies the updated LexonGraph streaming indexer contract.

- **Upstream contract [KNOWN]:** The delegated streaming indexer now requires the
  caller to choose a built-in planning configuration and pass the corresponding
  algorithm-specific settings rather than relying on one implicit clustering
  realization or the retired built-in clustering-factory seam.
- **Current built-in algorithms [KNOWN]:**
  - `dcbc`
  - `directional-pca`
- **Stage boundary [KNOWN]:** This requirement applies to the `full` and
  `clustering+block-assembly` execution stages and does not affect
  `ingestion+embedding` execution.
- **Delegation boundary [KNOWN]:** LexonArchiveBuilder still delegates all actual
  planning and clustering behavior to LexonGraph and does not define
  repository-local planning or clustering algorithms in this increment.
- **Default policy [KNOWN]:** When the caller omits clustering configuration,
  LexonArchiveBuilder SHALL apply a repository-owned default algorithm and
  default option values that remain compatible with the upstream contract.
- **Compatibility note [KNOWN]:** The latest known upstream built-in planning seam continues to expose `dcbc` and `directional-pca` as the repository-relevant built-in choices, but LexonArchiveBuilder must now bind them through the planning-policy contract rather than through `BuiltInClusteringFactory`.
- **Traceability:** UR-39, UR-44, UR-50, UR-52, UR-53, UR-61, UR-62, UR-65

#### RQ-INDEXER-003G - Algorithm-specific clustering options on the CLI

LexonArchiveBuilder SHALL expose command-line arguments that let operators
select the delegated clustering algorithm and provide supported
algorithm-specific option values for clustering-enabled execution.

- **Required surface [KNOWN]:** The CLI must let the caller choose among the
  supported delegated clustering algorithms and set supported option values
  without modifying Rust code or request fixtures.
- **Algorithm-family boundary [KNOWN]:** LexonArchiveBuilder SHALL expose only the
  options actually supported by the selected delegated clustering algorithm.
  Shared options may be reused across algorithms, but algorithm-specific
  options must remain explicit rather than silently ignored.
- **Validation rule [INFERRED]:** Supplying an option that is unsupported by the
  selected algorithm SHALL fail explicitly rather than being dropped or
  reinterpreted as a different option.
- **Environment-parity implication [INFERRED]:** The same CLI surface must remain
  usable for local/testing and production-shaped batch invocations so
  environment selection does not introduce a separate clustering-configuration
  interface family.
- **[UNKNOWN: whether this increment also requires equivalent request-file
  fields in `BatchRequest` rather than CLI-only exposure]**
- **Traceability:** UR-4, UR-12, UR-13, UR-50, UR-51, UR-52, UR-53

#### RQ-INDEXER-003H - Auto-sized omitted cluster count

When a clustering-enabled execution omits `cluster_count`, LexonArchiveBuilder
SHALL derive the effective cluster count from the number of clustering inputs
and the maximum parent-branch capacity implied by the active embedding
specification and block-size target.

- **Applicability [KNOWN]:** This omitted-option derivation rule SHALL apply to
  every supported built-in clustering algorithm, including `dcbc` and
  `directional-pca`, rather than being limited to one algorithm family.
- **Override rule [KNOWN]:** When the caller explicitly supplies
  `cluster_count`, LexonArchiveBuilder SHALL honor that explicit value instead
  of replacing it with a derived count.
- **Sizing objective [INFERRED]:** The derived count SHALL be large enough that
  the first parent-materialization layer can satisfy the repository's block-size
  target using the active embedding dimensions and encoding, subject to the
  delegated indexer's minimum child-count constraints.
- **Parity implication [INFERRED]:** The same omitted-option auto-sizing rule
  SHALL apply in both `full` and `clustering+block-assembly` execution so
  ingestion-stage participation does not change clustering-count semantics.
- **Failure-safety implication [INFERRED]:** If no valid derived count can
  satisfy the active embedding specification, block-size target, and minimum
  branch constraints, LexonArchiveBuilder SHALL fail explicitly rather than
  silently falling back to an unsafe fixed default.
- **Traceability:** UR-39, UR-43, UR-52, UR-53, UR-56, UR-57, UR-58

#### RQ-INDEXER-003I - Upstream feature-regression containment

When adapting to the latest LexonGraph version, LexonArchiveBuilder SHALL
preserve every repository-required capability that remains semantically
supported by the upstream contract and SHALL classify any missing capability as
an explicit upstream regression instead of silently narrowing repository
behavior.

- **Repository-required capabilities [KNOWN]:**
  - the external stage contract (`full`, `ingestion+embedding`, `clustering+block-assembly`)
  - deterministic split-stage replay acceptance
  - explicit built-in algorithm selection for `dcbc` and `directional-pca`
  - omitted `cluster_count` auto-sizing with explicit override preservation
  - runtime progress projection that keeps raw upstream lifecycle details behind the repository-owned progress surface
  - projection of the latest upstream live telemetry and heartbeat events onto that same repository-owned progress surface
  - unchanged MCP search-serving and retrieval behavior for already-indexed content
- **Regression rule [INFERRED]:** If the latest upstream surface removes or weakens one of those capabilities, LexonArchiveBuilder SHALL treat that as a compatibility finding requiring explicit design and implementation handling, not as permission to drop the affected repository behavior.
- **Boundary [KNOWN]:** This requirement does not force LexonArchiveBuilder to re-implement upstream planning internals in-repo; it constrains adaptation and regression reporting at the repository boundary.
- **Traceability:** UR-47, UR-61, UR-63, UR-64, UR-65, UR-66, UR-67, UR-68, UR-69, UR-71

#### RQ-INDEXER-004 - Content resolution integration

LexonArchiveBuilder SHALL provide a concrete implementation of `lexongraph_streaming_indexer::ContentResolver<R>`.

- **Constraint [KNOWN]:** This integration is responsible for resolving requested source content for the batch's collection items.
- **Email refinement [KNOWN]:** For mailbox-driven email indexing, LexonArchiveBuilder-owned preprocessing may materialize additional logical items such as normalized emails and chunks before the delegated resolver hands final embedding content to `lexongraph-streaming-indexer`.
- **Traceability:** UR-3, UR-5, UR-9, UR-15, UR-45

#### RQ-INDEXER-004A - Normalized email artifact derivation

LexonArchiveBuilder SHALL extract and normalize individual email messages from mailbox inputs before delegated indexing of email content.

- **Required result [KNOWN]:** The normalization step produces a canonical email artifact suitable for full-message retrieval and for derivation of chunk-level embedding units.
- **Identity rule [KNOWN]:** The canonical identity of the normalized email artifact is based on the normalized artifact content rather than the raw mailbox bytes.
- **Mailbox source compatibility [KNOWN]:** The normalization step SHALL accept mailbox source files ending in `.mail` or `.mbox` and SHALL NOT require broader mailbox extension support in this increment.
- **Body normalization rule [KNOWN]:** The normalization step derives a meaningful email body for embedding while best-effort excluding common non-semantic content when practical.
- **Boundary [KNOWN]:** This requirement applies to email ingestion in this increment and does not require the same normalization shape for document collections.
- **Traceability:** UR-15, UR-16, UR-19, UR-20, UR-29, UR-30

#### RQ-INDEXER-004B - Chunk-level email embedding units

LexonArchiveBuilder SHALL embed email-derived chunk content rather than whole mailbox files.

- **Required property [KNOWN]:** Each delegated email indexing item must represent a chunk-sized retrieval unit derived from a normalized email artifact.
- **Baseline policy [KNOWN]:** The first email chunking realization may use a sentence-aware baseline strategy, provided the surrounding design preserves room for future tokenizer-driven or more semantic chunking policies.
- **Non-goal [KNOWN]:** This requirement does not redefine LexonGraph's embedding contract or require LexonGraph itself to implement chunking.
- **Traceability:** UR-15, UR-19, UR-24

#### RQ-INDEXER-004C - Chunk-to-email provenance

LexonArchiveBuilder SHALL preserve a stable reference from each indexed email chunk back to its normalized email artifact.

- **Required property [KNOWN]:** Indexed email chunks must carry enough provenance metadata to support full-message retrieval without requiring clients to reparse raw mailbox blobs.
- **Metadata discipline [KNOWN]:** Search-serving metadata duplicated onto the indexed chunk should remain lean, but it must be sufficient for the common retrieval/rendering path without always dereferencing the normalized email artifact.
- **Traceability:** UR-17, UR-18, UR-21

#### RQ-INDEXER-004D - Chained email provenance

LexonArchiveBuilder SHALL preserve chained provenance from each indexed email chunk to its normalized email artifact and from that normalized email artifact to its source mailbox artifact.

- **Required property [KNOWN]:** The provenance chain must allow retrieval flows to move from a chunk hit to the full normalized email and then, when needed, to the mailbox-level source artifact.
- **Boundary [KNOWN]:** The provenance chain does not require clients to parse the mailbox artifact for ordinary retrieval.
- **Traceability:** UR-18, UR-23

#### RQ-INDEXER-004E - Stable chunk locator

LexonArchiveBuilder SHALL assign each delegated email chunk item a stable chunk locator
that makes it possible to determine which chunk is being processed or returned.

- **Required property [KNOWN]:** The chunk locator must be derivable from the
  normalized email artifact reference plus chunk-local identity such as ordinal
  position and remain stable under a stable normalization and chunking policy.
- **Integration boundary [KNOWN]:** Because `lexongraph-streaming-indexer` accepts
  `metadata` plus an opaque `content_ref` rather than a first-class item-name
  field, LexonArchiveBuilder owns how this chunk locator is represented.
- **Traceability:** UR-17, UR-23

#### RQ-INDEXER-004F - Replay-stable content fingerprints

LexonArchiveBuilder SHALL provide a deterministic content fingerprint for every delegated content reference used with the streaming indexer.

- **Required property [KNOWN]:** The fingerprint for a delegated content reference must remain stable across every training pass and the final materialization replay for the same logical item.
- **Email identity alignment [KNOWN]:** For email-derived chunk items, the fingerprint SHALL remain aligned with the normalized email artifact and stable chunk locator rather than with transient mailbox-processing state.
- **Failure-safety implication [INFERRED]:** Replay or rerun validation failures caused by non-deterministic fingerprinting are specification violations rather than acceptable batch variability.
- **Traceability:** UR-9, UR-16, UR-23, UR-45, UR-48, UR-49

#### RQ-INDEXER-005 - Block storage integration

LexonArchiveBuilder SHALL provide a concrete implementation of `lexongraph_block_store::BlockStore` used to persist blocks produced through the delegated indexing flow.

- **Architectural target storage profiles [KNOWN]:**
  - local filesystem for local/testing operation
  - Azure Blob Storage for production operation
- **MVP realization [KNOWN]:** The first in-repo implementation must execute end-to-end against the local filesystem profile. Azure Blob Storage remains a required future profile boundary, but not a required executable realization for the first MVP.
- **Local filesystem interoperability [KNOWN]:** The local/testing filesystem-backed realization SHALL use the LexonGraph-owned filesystem block-store contract, including its on-disk naming and layout scheme, so LexonGraph filesystem tooling such as `lexongraph-block-inspect` can operate on LexonArchiveBuilder-produced local stores.
- **Local implementation target [KNOWN]:** The local/testing filesystem-backed realization SHALL use the upstream `lexongraph-block-store-fs` crate rather than a repository-local filesystem naming scheme.
- **Migration boundary [KNOWN]:** This local filesystem interoperability correction may require a fresh or rebuilt local store; continued read compatibility with blocks written by the superseded custom local layout is not required in this increment.
- **Artifact reuse [KNOWN]:** The same environment-selected `BlockStore` abstraction family SHALL also be used for normalized email artifacts and mailbox provenance artifacts, provided indexing contracts and retrieval references remain explicit.
- **Mailbox retention [KNOWN]:** Mailbox provenance artifacts SHALL be retained so the original source material remains available for re-normalization, re-chunking, and re-ingestion flows.
- **Traceability:** UR-3, UR-6, UR-9, UR-12, UR-13, UR-18, UR-22, UR-25, UR-26, UR-27, UR-28

#### RQ-INDEXER-006 - Embedding provider integration

LexonArchiveBuilder SHALL obtain embeddings through a provider that satisfies `lexongraph_embeddings_trait::EmbeddingProvider` and is reached through an OpenAI-compatible HTTP embedding interface.

- **Architectural target embedding profiles [KNOWN]:**
  - local STAPI-compatible embedding service
  - Azure OpenAI embedding model
- **Constraint [KNOWN]:** Provider selection varies by environment and must not require changes to the collection-oriented batch contract.
- **MVP realization [KNOWN]:** The first in-repo implementation must execute end-to-end against a local embedding service. Azure OpenAI remains a required future profile boundary, but not a required executable realization for the first MVP.
- **Integration note [KNOWN]:** The delegated indexer consumes `EmbeddingInput` and `EmbeddingSpec` through the shared embeddings trait boundary.
- **Traceability:** UR-7, UR-9, UR-12, UR-13

#### RQ-INDEXER-007 - Environment-specific adapter selection

LexonArchiveBuilder SHALL select storage and embedding integrations according to environment without changing the delegated indexing contract or the batch input contract.

- **Local/testing [KNOWN]:** local filesystem + local embedding service
- **Production [KNOWN]:** Azure Blob Storage + Azure OpenAI
- **MVP realization [KNOWN]:** Only the local/testing profile is required to be executable in the first MVP. The production profile must remain representable through the same adapter boundary and configuration model without requiring Azure-specific execution support in this increment.
- **Traceability:** UR-6, UR-7, UR-12, UR-13

#### RQ-INDEXER-008 - Idempotent reruns

LexonArchiveBuilder SHALL preserve idempotent rerun behavior for repeated indexing of the same source content.

- **Mechanism owner [KNOWN]:** The underlying LexonGraph API owns batch and recovery semantics.
- **Required property [KNOWN]:** Produced blocks are immutable and identified by hash, so reruns must not create distinct logical outputs for unchanged content.
- **Email artifact implication [INFERRED]:** Repeated normalization of semantically unchanged email content should resolve to the same normalized email artifact identity and the same derived chunk identities under a stable normalization and chunking policy.
- **Concurrency implication [INFERRED]:** Same-layer leaf scheduling must not change the logical block set or final root produced for unchanged input relative to the approved delegated indexing contract.
- **Standalone clustering implication [INFERRED]:** Repeating the clustering-only
  stage against the same clustering-eligible block-store snapshot must not
  change the logical clustering result under unchanged upstream semantics.
- **Clustering-configuration implication [INFERRED]:** Repeating a clustering-enabled
  run against unchanged inputs under the same effective clustering algorithm and
  option values must not change the logical clustering result merely because a
  defaulted clustering configuration resolved differently.
- **Traceability:** UR-8, UR-16, UR-36, UR-50, UR-52

#### RQ-INDEXER-008A - Local integration composition

LexonArchiveBuilder SHALL provide a Docker Compose topology for the local/testing profile that deploys the batch container and its required local dependencies as one integration-testable unit.

- **Included local dependencies [KNOWN]:** local storage mounts/volumes and the local embedding service
- **Constraint [KNOWN]:** The Compose topology must preserve the Linux batch-container runtime shape rather than introducing a separate long-lived control-plane service for indexing.
- **Traceability:** UR-4, UR-12, UR-14

#### RQ-INDEXER-008B - Observable indexing progress

LexonArchiveBuilder SHALL emit progress logs during batch execution that make forward
progress visible while mailbox items are processed, delegated indexing work
advances, and clustering or block assembly advances.

- **Minimum visibility [KNOWN]:** Progress output must include mailbox-processing
  visibility, indexed-item visibility, and clustering or block-assembly
  visibility so operators can tell that work is continuing before the final
  summary is emitted.
- **Streaming lifecycle visibility [KNOWN]:** Progress output must remain meaningful across upstream planning passes, planning completion, hierarchy-planning stages, and final materialization or bottom-up assembly without requiring callers to understand raw upstream phase names.
- **Embedding-phase visibility [KNOWN]:** For any execution stage that includes ingestion plus embedding generation, progress output must continue after delegated items have been prepared and while local embedding or leaf-materialization work is still consuming those delegated items.
- **Replay-submission visibility [KNOWN]:** For any execution stage that submits known replay batches to the delegated streaming API, including clustering-only execution reconstructed from stored leaf blocks, progress output must report repository-owned replay-batch submission completion in bounded work units using the known batch count and cumulative delegated-item count for the invocation.
- **Phase-boundary clarity [KNOWN]:** When repository-owned replay-batch submission completes and LexonArchiveBuilder begins waiting for upstream planning-pass completion or an equivalent delegated lifecycle boundary, the runtime progress stream must emit an explicit handoff message so operators can distinguish local submission completion from subsequent upstream observer heartbeats.
- **Gap constraint [INFERRED]:** A non-empty ingestion-plus-embedding run SHALL NOT rely on one mailbox-preparation message and then remain silent until the first downstream streaming-status event or final summary; operators must receive continued liveness or completed-work visibility while delegated embedding work remains outstanding.
- **Cadence boundary [INFERRED]:** The requirements do not fix an exact log-line schema or interval, but the runtime-visible signal must advance by bounded work units or bounded elapsed time rather than only at phase boundaries.
- **Surface [KNOWN]:** Progress output should be emitted on the normal
  batch-runtime log stream so local runs, Compose runs, and containerized
  production-style runs observe the same signal shape.
- **Full-pipeline sequencing [INFERRED]:** When the caller selects the default
  full pipeline, progress remains one unified runtime-visible stream that spans
  the ingestion plus embedding phase and the clustering plus block-assembly
  phase in order.
- **Observer integration [KNOWN]:** LexonArchiveBuilder SHALL implement the upstream
  streaming status-observer seam and translate observer events onto the same
  runtime progress surface used for mailbox and delegated-indexing progress.
- **Telemetry projection [KNOWN]:** When the latest upstream observer surface emits richer live hierarchy-planning telemetry or heartbeat-style in-progress status updates, LexonArchiveBuilder SHALL continue projecting those events onto the same runtime progress stream rather than dropping them or exposing them only through a separate telemetry path.
- **Count-semantics clarity [KNOWN]:** If upstream observer events report phase-local or layer-local work counts that differ from the repository-total delegated-item count for the invocation, LexonArchiveBuilder SHALL render progress messages so operators can tell whether a reported count refers to invocation-total delegated items, stage-local processed work, or materialized block counts.
- **Boundary discipline [INFERRED]:** Repository-owned progress messages SHOULD make clear when they describe local replay submission state versus upstream observer-reported planning, clustering, or materialization state, even when the upstream observer does not expose in-phase processed-versus-remaining counts.
- **Non-goal [KNOWN]:** This requirement does not introduce a separate control-plane, metrics backend, or MCP-surface change.
- **Traceability:** UR-32, UR-33, UR-39, UR-41, UR-45, UR-48, UR-59, UR-60, UR-61, UR-62, UR-63, UR-67, UR-68, UR-69, UR-70, UR-71

#### RQ-INDEXER-008C - Diagnosable clustering failures

When a clustering-enabled execution fails after LexonArchiveBuilder has determined
the clustering candidate set and effective delegated clustering configuration,
LexonArchiveBuilder SHALL emit failure diagnostics that make the failed attempt
reconstructable to an operator.

- **Applicability [KNOWN]:** This requirement applies to the `full` and
  `clustering+block-assembly` execution stages when the failure occurs in or
  because of delegated clustering or clustering-dependent materialization work.
- **Input-set visibility [KNOWN]:** Failure diagnostics must identify the exact
  repository-visible clustering input set for the failed attempt, including
  enough stable identifiers to determine which child blocks, replay items, or
  equivalent repository-owned logical nodes were being clustered.
- **Effective-configuration visibility [KNOWN]:** Failure diagnostics must record
  the effective delegated clustering configuration actually used for the failed
  attempt, including the selected algorithm, the resolved effective
  `cluster_count`, the effective algorithm-specific parameter values, the active
  embedding specification, the block-size target, and the selected execution
  stage.
- **Dual-surface requirement [KNOWN]:** The required diagnostics must be emitted
  on the normal runtime log stream and written to a request-adjacent diagnostic
  artifact so failure analysis does not depend on retaining transient console
  output alone.
- **Failure-only scope [KNOWN]:** This requirement does not obligate
  LexonArchiveBuilder to emit the same detailed clustering-input inventory for
  successful clustering runs in this increment.
- **Environment parity [INFERRED]:** The same diagnostic information must remain
  available for local/testing and preserved production-shaped runs rather than
  existing only in one environment profile.
- **Extensibility [INFERRED]:** The diagnostic shape must not assume mailbox-only
  content; future content types should be representable through the same
  repository-visible input-identification scheme.
- **Failure-path robustness [INFERRED]:** If the request-adjacent diagnostic
  artifact cannot be written, the runtime log output for the original clustering
  failure must still include enough information to identify the failed input set
  and effective delegated configuration rather than silently degrading to the
  current opaque failure shape.
- **Boundary [KNOWN]:** This requirement adds indexing-time diagnosability only
  and does not redefine MCP search semantics, upstream clustering semantics, or
  the external request contract.
- **Artifact-location policy [KNOWN]:** The request-adjacent diagnostic artifact
  SHALL be written in the `--summary-out` directory when that output path is
  present and SHALL otherwise be written in the same directory as the
  `--request` file.
- **Traceability:** UR-39, UR-41, UR-50, UR-72, UR-73, UR-74, UR-75

### Boundary and Invariant Requirements

#### RQ-INDEXER-009 - Search-serving separation

The indexer requirements SHALL remain limited to indexing-time orchestration and adapter responsibilities and SHALL NOT redefine MCP search-serving behavior.

- **Rationale [INFERRED]:** Preserves the repository invariant that indexing remains separate from the MCP server surface.
- **Traceability:** UR-2, README.md

#### RQ-INDEXER-010A - Subordinate external contracts

LexonArchiveBuilder SHALL remain subordinate to the public contracts owned by `lexongraph-streaming-indexer`, `lexongraph-streaming-clustering`, `lexongraph-block-store`, and `lexongraph-embeddings-trait` and SHALL NOT redefine their index-construction, planning-policy, replay-validation, block-identity, or embedding-contract semantics within this repository.

- **Rationale [KNOWN]:** Those semantics are already owned by the upstream LexonGraph crates and specifications.
- **Traceability:** UR-3, UR-8, UR-9, UR-44, UR-45, UR-48, UR-61, UR-62

#### RQ-INDEXER-010B - Local block-store tooling interoperability

For the local/testing filesystem-backed profile, LexonArchiveBuilder SHALL remain interoperable with LexonGraph-owned filesystem block-store tooling and SHALL NOT publish blocks using a repository-specific local filename or directory scheme under the same `BlockStore` boundary.

- **Rationale [KNOWN]:** Local block inspection and other filesystem-oriented LexonGraph tooling depend on the upstream filesystem block-store layout contract rather than on an arbitrary repository-local naming scheme.
- **Boundary [KNOWN]:** This requirement constrains only the local/testing filesystem-backed profile and does not redefine Azure Blob layout details for the production profile.
- **Traceability:** UR-26, UR-27

#### RQ-INDEXER-010 - Stable abstraction boundary

LexonArchiveBuilder SHALL keep content resolution, block storage, and embedding-provider variation behind stable integration boundaries so future content types and provider swaps do not require redefinition of the core indexing contract.

- **MVP implication [KNOWN]:** The first MVP may ship only the local/testing realizations, but it must preserve storage and embedding seams so production adapters can be added without changing the batch contract or content-model abstractions.
- **Email evolution implication [KNOWN]:** Email-specific normalization, artifact storage, and chunk derivation must not preclude future document-specific policies, metadata, or artifact shapes.
- **Stage-semantics implication [KNOWN]:** Stage selection must be expressed in
  terms of generic pipeline phases rather than mailbox-specific behavior so
  future content types can participate without redefining the batch contract.
- **Clustering-configuration implication [INFERRED]:** Clustering algorithm
  selection and supported option values must remain part of the same stable
  batch-orchestration boundary across environments rather than creating a
  separate environment-specific clustering configuration model.
- **Traceability:** UR-3, UR-6, UR-7, UR-13, UR-19, UR-22, UR-42, UR-50, UR-51

## Out of Scope

- Defining indexing algorithms internal to LexonGraph indexing crates
- Exposing the upstream streaming planning or materialization lifecycle directly on the external CLI or `BatchRequest` contract in this increment
- Redefining the public contracts of `ContentResolver<R>`, `BlockStore`, or `EmbeddingProvider`
- Defining MCP query semantics or search ranking behavior
- Re-specifying LexonGraph API batch recovery internals
- Finalizing exact production deployment workflow beyond the batch-container shape already described
- Requiring executable Azure production adapters in the first MVP increment
- Requiring document collections to adopt the same normalization or chunking policy as email in this increment
- Broadening mailbox source compatibility beyond the approved `.mail` and `.mbox` extension set in this increment
- Introducing a dedicated telemetry service, long-lived progress daemon, or MCP-visible progress API for indexing in this increment
- Requiring higher-layer parent or node block concurrency in the current increment before the upstream delegated indexing surface exposes a compatible implementation seam
- Introducing a repository-local per-run clustering manifest or a repository-local block-classification scheme outside the upstream LexonGraph block-iteration contract
- Defining repository-local clustering algorithms or option semantics beyond the supported built-in upstream clustering choices used in this increment
- Requiring detailed clustering-input inventories for successful clustering runs in this increment

## Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | Requirements explicitly constrain scope to indexing-time orchestration and integrations |
| Environment-specific storage and embedding behavior stays behind stable interfaces | Preserved | Stage selection, block-store iteration, and clustering-status reporting are constrained to the same request and adapter boundaries across local/testing and the preserved production profile |
| Architecture remains extensible to future content types | Preserved | Collection-oriented input still covers both mailbox and document collections, and stage selection is defined in generic pipeline terms rather than mailbox-specific behavior |
| Idempotence and recoverability stay aligned with underlying immutable block semantics | Preserved with clarified scope | Requirements extend hash-addressed identity expectations to normalized email artifacts and require clustering-only reruns over the same clustering-eligible block-store snapshot to remain semantically stable under unchanged upstream semantics |
| Local development remains self-contained and batch-oriented | Preserved | Docker Compose is constrained to compose local dependencies around the batch container rather than changing the runtime model |
| Long-running batches remain observable without adding a control plane | Preserved with clarified scope | Progress reporting remains on the existing batch-runtime log surface and now explicitly includes the long-running embedding or leaf-materialization gap between mailbox expansion and downstream streaming-status visibility plus clustering-only replay submission progress, the handoff into upstream planning-pass waiting, and failure-only clustering diagnostics on the runtime log plus a request-adjacent artifact |
| Caller-visible indexing and MCP contracts remain stable across the upstream API migration | Preserved | The streaming lifecycle is constrained to an internal adaptation behind the existing stage surface and unchanged MCP retrieval semantics |
| Clustering configuration remains explicit and replayable | Preserved with clarified scope | Requirements now treat the effective clustering algorithm and option set as part of clustering-enabled orchestration input and constrain defaults to resolve deterministically |
| Omitted clustering-size behavior remains deterministic and safe across algorithms | Preserved with clarified scope | Requirements now constrain omitted `cluster_count` to derive from input count plus embedding-aware branch capacity for every supported built-in algorithm while preserving explicit caller override behavior |
| Required repository capabilities remain distinguishable from upstream regressions during the latest upgrade | Preserved with clarified scope | The requirements now force the upgrade to classify missing capabilities explicitly instead of silently narrowing split-stage replay, planning-policy mapping, progress projection, or MCP-facing behavior |
| Latest upstream telemetry remains subordinate to the existing runtime progress surface | Preserved with clarified scope | Requirements now constrain richer live telemetry and heartbeat events to the same repository-owned log stream rather than a new telemetry interface |
| Operator-visible progress counts remain understandable across upstream telemetry changes | Preserved with clarified scope | Requirements now distinguish invocation-total delegated-item counts from stage-local or layer-local telemetry counts so upstream count-shape changes do not create misleading logs |
| Clients are not forced to parse raw mailbox blobs for ordinary retrieval | Preserved | Indexed chunks must reference normalized email artifacts so retrieval can stay at chunk level or expand to full normalized email through repository-owned artifacts |
| Storage abstraction count stays bounded across environments | Preserved | Requirements now reuse the environment-selected `BlockStore` abstraction family for indexed blocks, normalized email artifacts, and mailbox provenance artifacts rather than introducing a second storage stack |
| Local filesystem block stores remain interoperable with LexonGraph tooling | Preserved | The local/testing profile is now constrained to LexonGraph's filesystem naming/layout contract so inspection tools can consume repository-produced local stores |
| Parallel execution does not weaken deterministic indexing semantics | Preserved | Leaf-layer concurrency is constrained by cross-layer barriers and idempotence requirements so scheduling policy does not become a semantic contract change |

## Open Questions / Discovery Gaps

- **Q-INDEXER-061 [UNKNOWN]:** Does the latest upstream planning-policy surface preserve exactly the same effective option semantics for `dcbc` balance constraints and `directional-pca` parameters that the repository already exposes, or only the same field names?
- **Q-INDEXER-062 [UNKNOWN]:** Does the latest upstream status-observer contract expose enough information for LexonArchiveBuilder to preserve its current replay-submission handoff and long-running liveness messages without weakening operator visibility?
- **Q-INDEXER-063 [UNKNOWN]:** Are any repository-required split-stage replay guarantees now expressed through different upstream lifecycle transitions beyond the observed rename from training completion to planning completion?
- **Q-INDEXER-064 [UNKNOWN]:** Does the newest upstream telemetry contract intend `item_count` to remain invocation-total for planning-pass events while hierarchy-planning and bottom-up assembly events report stage-local or layer-local progress units, or is that count shape still evolving?

## Coverage Notes

- **Covered sources [KNOWN]:**
  - user request in this session: "update the LexonGraph rust crates. The latest version contains a significant api change. Rebuild the indexer code to use the new LexonGraph streaming indexer. Maintain other invariants, update tests. When done, branch, commit, push, pr"
  - user request in this session: "adapt implementation to latest lexongraph version and tell me if lexongraph regressed features we need so I can fix it."
  - user clarification in this session selecting: "Preserve the current external stage contract (Recommended)"
  - user clarification in this session selecting: "Yes, preserve MCP search/retrieval behavior (Recommended)"
  - user request in this session to adopt LexonGraph's incremental indexing APIs and emit visible mailbox/indexing progress during batch execution
  - user request in this session: "make it so this can work with .mail as well as .mbox"
  - user clarification in this session selecting: "Exactly `.mail` and `.mbox`"
  - user request in this session: "remove LocalFilesystemBlockStore and replace with the lexongraph-block-store-fs crate from lexongraph. Our custom store is breaking lexongraph-block-inspect because it uses a totally different naming scheme"
  - user clarification in this session selecting: "Fresh/rebuilt local store is acceptable"
  - user request in this session: "fix this behavior. It should always auto-size based on number of blocks to embededd and the embedding size"
  - user clarification in this session selecting: "Yes — explicit cluster_count overrides; auto-size only when omitted (Recommended)"
  - user request in this session: "ok, can we pull latest lexongraph again? It has new telemtry"
  - user clarification in this session selecting: "Project the new upstream telemetry onto the existing runtime progress/log surface (Recommended)"
  - `docs/specs/lexonarchivebuilder-indexer/design.md:228-315`
  - `docs/specs/lexonarchivebuilder-indexer/validation.md:72-187`
  - `crates/lexonarchivebuilder-indexer/src/runtime.rs:14-340`
  - `Alan-Jowett/LexonGraph` `crates/lexongraph-streaming-indexer/src/lib.rs` on `main`: planning-policy and status-observer surfaces around `HierarchicalPlanningPolicy`, `BuiltInPlanningPolicy`, `StreamingIndexingPhase`, and `mark_planning_complete`
  - `Alan-Jowett/LexonGraph` `crates/lexongraph-streaming-indexer/src/lib.rs` on `main`: telemetry and heartbeat surfaces around `PlanningStageStatusTracker`, `start_status_heartbeat`, and `with_legacy_item_count`
  - `README.md:18-27`
  - `README.md:42-49`
  - `README.md:51-59`
  - `README.md:61-80`
  - `crates/lexonarchivebuilder-indexer/src/mailbox.rs:24-31`
  - `crates/lexonarchivebuilder-indexer/src/mailbox.rs:157-176`
  - external LexonGraph repository source (not vendored in LexonArchiveBuilder):
    `crates/lexongraph-streaming-indexer/src/lib.rs:5-8`
  - external LexonGraph repository source (not vendored in LexonArchiveBuilder):
    `crates/lexongraph-streaming-indexer/src/lib.rs:92-119`
  - external LexonGraph repository source (not vendored in LexonArchiveBuilder):
    `crates/lexongraph-streaming-indexer/src/lib.rs:202-219`
  - external LexonGraph repository source (not vendored in LexonArchiveBuilder):
    `crates/lexongraph-streaming-indexer/src/lib.rs:499-507`
  - external LexonGraph repository source (not vendored in LexonArchiveBuilder):
    `crates/lexongraph-streaming-indexer/src/lib.rs:799-807`
  - external LexonGraph repository source (not vendored in LexonArchiveBuilder):
    `crates/lexongraph-block-store/src/lib.rs:28-32`
  - external LexonGraph repository source (not vendored in LexonArchiveBuilder):
    `crates/lexongraph-block-store-fs/src/lib.rs:89-103`
  - external LexonGraph repository source (not vendored in LexonArchiveBuilder):
    `crates/lexongraph-block-store-fs/src/lib.rs:165-170`
  - external LexonGraph repository source (not vendored in LexonArchiveBuilder):
    `crates/lexongraph-embeddings-trait/src/lib.rs:20-33`
  - `crates/lexonarchivebuilder-indexer/src/block_store.rs:11-31`
  - `crates/lexonarchivebuilder-indexer/src/block_store.rs:56-82`
  - `crates/lexonarchivebuilder-indexer/src/runtime.rs:63-90`
  - `crates/lexonarchivebuilder-indexer/src/mailbox.rs:85-155`
  - `crates/lexonarchivebuilder-indexer/src/main.rs:33-41`
  - user clarification messages in this session specifying both mailbox and document-collection MVP coverage
  - user clarification messages in this session specifying local-only executable MVP scope with production left pluggable
  - user clarification messages in this session specifying Docker Compose-based local dependency orchestration
  - user clarification message in this session: "Lets do email now, but don't preculde docs. Docs will need different handling as they have different meta-data"
  - user discussion in this session specifying normalized email artifacts, chunk-level email embeddings, minimal indexed metadata, and full-email retrieval by artifact reference
  - user clarification message in this session: "I think we have a reasonable understand of what an email body is. The goal is to have something meaningful for embedding while not containing common data (if possible). May be best effort."
  - user clarification message in this session: "We should duplicate enough so that the 80% case can be satisfied with just the block"
  - user clarification message in this session: "I think they should. We don't really want two azure blob store, s3 store, local filesystem, etc, abstractions."
  - user clarification message in this session: "I think we can chain the provenance. Chunk -> mail block -> mbox."
  - user clarification message in this session: "Can we use the text_splitter crate for now, with the option to use huggingface tokenizer later for semantic chunking? Agree to the rest"
  - user request in this session: "Processing of blocks (both leaf and node) can occur concurrently within a layer. Only synchronization required is cross layer."
  - user request in this session: "Can we modify the indexer to use up to a admin defined number of cores, with default being 1/2 the number of physical cpus?"
  - user clarification in this session: "Limit concurrency to the leaf layer for now (it is what is doing the expensive embedding generation anyway). Make note that higher layer concurrency is a future work item."
  - user request in this session: "provide a command line option to control which stage runs. Allow the caller to run only the mailbox ingestion + embedding generation or to run the clustering and block assembly."
  - user clarification in this session selecting: "CLI and BatchRequest"
  - user clarification in this session selecting: "All blocks in the configured block store (Recommended)"
  - user request in this session: "The LexonGraph now has a block iteration API so that the clustering can then examine the list of blocks and then start doing clustering. In addition, the clustering also has a callback trait for status updates. Implement that as well so we can monitor the clustering (which is a slow step)"
  - user clarification in this session selecting: "Keep the existing final-root BatchSummary"
  - user request in this session: "the LexonGraph crate has been updated again. It now requires selection of clustering algorithm and options. Update the latest LexonGraph and expose these options via command line (feel free to pick reasonable defaults for unspecified options)"
  - user request in this session: "the current builder doesn't report progress during them embedding phase: Processed mailbox /workspace/examples/local/scale-test/runs/20260607T204011Z/fetched/01-rsync.ietf.org__mailman-archive_ipsec_/2026-06.mail: 5 message(s), 10 delegated item(s) Prepared 10 delegated item(s) from mailbox /workspace/examples/local/scale-test/runs/20260607T204011Z/fetched/01-rsync.ietf.org__mailman-archive_ipsec_/2026-06.mail it reported this and then nothing. I see the embedding service hitting 8 cpu worth of work, so it's running but doesn't show progress"
  - user request in this session: "but it knows it has submitted batch N/M? It should log after each batch is submitted with N items submitted out of M items total?"
  - `crates/lexonarchivebuilder-indexer/src/runtime.rs:391-418`
  - `crates/lexonarchivebuilder-indexer/src/runtime.rs:457-579`
  - `crates/lexonarchivebuilder-indexer/src/runtime.rs:594-628`
  - `crates/lexonarchivebuilder-indexer/src/runtime.rs:777-913`
  - external LexonGraph repository source (not vendored in LexonArchiveBuilder):
    `crates/lexongraph-streaming-indexer/src/lib.rs:539-573`
  - external LexonGraph repository source (not vendored in LexonArchiveBuilder):
    `crates/lexongraph-streaming-indexer/src/lib.rs:303-327`
- **Excluded for now [KNOWN]:**
  - Detailed Rust implementation file paths, crate manifests, Docker assets, and test artifacts, because this requirements document captures the semantic contract and leaves implementation realization to downstream design, validation, and code-review artifacts
  - Exact normalized email CBOR schema, exact duplicated chunk metadata list, and the specific chunking library choice, because those belong to downstream design and validation artifacts rather than requirements
  - The precise log-line schema, sink configuration, and per-item verbosity throttling policy for progress output, because those belong to downstream design and validation artifacts rather than requirements
  - The exact bounded-work-unit choice or elapsed-time threshold for embedding-phase progress updates, because that belongs to downstream design and validation artifacts so long as the approved requirements-level no-silent-gap contract is preserved
  - The exact mapping from repository stage modes to concrete upstream streaming pass counts, replay batching, and training-completion timing, because those belong to downstream design and validation artifacts rather than requirements
  - The exact configuration surface for the administrator-defined concurrency cap and the exact physical-CPU detection algorithm in containerized or quota-constrained environments, because those belong to downstream design and validation artifacts rather than requirements
  - The precise block-kind predicate used inside the upstream LexonGraph block-iteration API to determine clustering eligibility, because this requirements document constrains LexonArchiveBuilder to the upstream iteration contract without redefining LexonGraph-owned block semantics
  - The exact default clustering algorithm, exact default numeric option values, and whether clustering-option parity with `BatchRequest` is required in this increment, because those choices can be finalized in downstream design and validation artifacts so long as they stay within the approved command-line and determinism constraints
