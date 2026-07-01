<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# Indexer Requirements

## Document Status

- **Phase:** Phase 1 - Requirements Discovery
- **Status:** Approved streaming-indexer migration baseline with incremental requirements patches for LexonGraph published-profile API adoption, published-profile version selection, latest telemetry compatibility, upstream regression assessment, clustering-failure diagnostics, rooted block-tree quality assessment discovery plus quality-metric refinement, rooted TNN-recall diagnostics, rooted CLI search discovery, upstream main-tracking for rapid profile validation, upstream wgpu-acceleration revision compatibility, 0.6.x published-profile evaluation, local testing sweep automation, upstream embedding-readback API adoption, and LAB-owned replay-journaled clustering-only recovery
- **Scope:** LexonArchiveBuilder indexer integration boundary plus incremental email-artifact, chunk-indexing, local block-store interoperability, replay-based streaming delegated indexing, stage-selectable execution, standalone clustering input discovery, LAB-owned replay-journaled split-stage recovery, published-profile-based clustering configuration with caller-selectable profile versions, latest published-profile and telemetry compatibility, upstream regression assessment, embedding-phase, replay-submission and streaming-status observability, clustering-failure diagnosability, rooted block-tree quality assessment with refined per-layer quality metrics and rooted TNN-recall diagnostics, rooted CLI search over stored trees, temporary upstream main-tracking for rapid profile validation, upstream wgpu-acceleration revision compatibility, 0.6.x published-profile evaluation through repository-local testing automation, upstream-owned embedding readback for stored-tree consumers, and layer-parallel block-construction evolution

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
- **UR-50 [KNOWN]:** The earlier low-level LexonGraph clustering-algorithm-and-option surface is superseded for this increment by the published-profile API, so callers are not required to select a clustering algorithm or algorithm-specific options.
- **UR-51 [KNOWN]:** LexonArchiveBuilder should not expose clustering algorithm selection or supported clustering options through the command line while the approved external contract remains pinned to published profile `0.1.0`.
- **UR-52 [KNOWN]:** The approved clustering default for this increment is the repository-pinned published profile rather than repository-local per-option defaults.
- **UR-53 [KNOWN]:** Any upstream built-in algorithm-family details remain internal to the approved published profile and are not part of the current external indexer contract.
- **UR-54 [KNOWN]:** The current builder can report mailbox processing and delegated-item preparation, then remain silent during long-running embedding work even while the local embedding service is actively consuming CPU.
- **UR-55 [INFERRED]:** Progress visibility for ingestion-plus-embedding execution should remain continuous across the gap between delegated-item preparation and the first downstream streaming-status event so operators can distinguish slow embedding work from a hung batch.
- **UR-56 [KNOWN]:** For clustering-enabled execution in this increment, LexonArchiveBuilder should use clustering cardinality owned by the approved published profile rather than repository-local `cluster_count` tuning.
- **UR-57 [KNOWN]:** This profile-owned clustering-cardinality rule should apply consistently across approved invocation shapes.
- **UR-58 [KNOWN]:** Explicit caller-supplied `cluster_count` overrides are retired while the approved external contract remains pinned to published profile `0.1.0`.
- **UR-59 [KNOWN]:** During clustering-only replay, LexonArchiveBuilder should report repository-owned replay-batch submission progress using the batch count and cumulative delegated-item count it already knows, so operators can see how much work has been submitted to the streaming API.
- **UR-60 [KNOWN]:** When LexonArchiveBuilder finishes submitting replay batches and begins waiting for upstream planning-pass completion, the runtime progress stream should emit an explicit phase-boundary message so operators can distinguish local submission progress from upstream planning-pass heartbeats.
- **UR-61 [KNOWN]:** Adapt LexonArchiveBuilder to the latest LexonGraph version currently published on the upstream `main` branch.
- **UR-62 [KNOWN]:** The latest LexonGraph streaming indexer replaces the older training-oriented built-in clustering factory surface with a planning-policy surface, including `HierarchicalPlanningPolicy`, `BuiltInPlanningPolicy`, planning passes, explicit planning completion, hierarchy-planning status phases, and bottom-up assembly status phases.
- **UR-63 [KNOWN]:** Preserve the current external stage contract (`full`, `ingestion+embedding`, `clustering+block-assembly`) and existing MCP search or retrieval behavior for already-indexed content while adapting to the latest upstream indexing API.
- **UR-64 [KNOWN]:** Determine whether the latest LexonGraph update regressed any repository-required features or only changed the API shape, so any true upstream regression can be fixed explicitly rather than hidden by narrowing LexonArchiveBuilder behavior.
- **UR-65 [INFERRED]:** LexonArchiveBuilder currently depends on repository-owned behavior layered on top of the upstream indexing crate, including deterministic split-stage replay, repository pinning to published profile `0.1.0`, retirement of low-level clustering controls, and runtime progress projection that hides raw upstream lifecycle details.
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
- **UR-76 [KNOWN]:** The current clustering-failure artifact can identify the failing input set and effective delegated configuration, but it still cannot explain *why* failures such as directional-PCA rank collapse occurred because it does not include any embedding-level health diagnostics.
- **UR-77 [KNOWN]:** For clustering failures caused by degenerate or suspicious embedding sets, operators need compact embedding-health diagnostics plus a small suspicious-input sample rather than a full dump of every raw embedding vector.
- **UR-78 [KNOWN]:** The current top-level embedding-health diagnostics can show that the full clustering attempt looked broadly healthy while still failing, which is not enough when the real collapse happened inside a smaller upstream partition or subproblem.
- **UR-79 [KNOWN]:** When the upstream failure surface permits it, operators need diagnostics for the exact failing partition or subproblem; when it does not, LexonArchiveBuilder should still report the narrowest repository-visible subset it can prove was active at the failing step.
- **UR-80 [KNOWN]:** We need a tool that, given a block store and a root block, measures the quality and correctness of the resulting block tree.
- **UR-81 [KNOWN]:** The tool should report structural correctness heuristics such as children always having lower level than their parents.
- **UR-82 [KNOWN]:** The tool should report embedding-space quality heuristics such as a child's distance-from-centroid spread being the same or smaller than its parent's corresponding spread, so child blocks represent smaller regions than parents.
- **UR-83 [KNOWN]:** The tool should provide a quantifiable measure of how well the embedding space is divided up, including the shape represented by each block in embedding space.
- **UR-84 [KNOWN]:** In this increment, the block-tree quality tool should be a CLI-only operator tool.
- **UR-85 [KNOWN]:** In this increment, the tool should emit both a human-readable summary and a machine-readable JSON artifact.
- **UR-86 [INFERRED]:** The assessment should operate through the existing `BlockStore` boundary and rooted block graph rather than introducing a second storage abstraction or an MCP-visible quality surface.
- **UR-87 [INFERRED]:** The assessment must distinguish hard structural-correctness findings from advisory embedding-space quality heuristics so operators can tell invariant violations apart from weaker quality signals.
- **UR-88 [KNOWN]:** Add an easy CLI tool that accepts a text string and an embedding endpoint, generates an embedding, searches with the `lexongraph-search` API, and returns the top `k` matching leaf nodes.
- **UR-89 [KNOWN]:** In this increment, the CLI search tool should search a caller-supplied root/tree rather than all searchable content in the configured block store.
- **UR-90 [KNOWN]:** In this increment, the CLI search tool should emit both human-readable results and machine-readable JSON output.
- **UR-91 [INFERRED]:** The CLI search tool should remain additive to the existing MCP server search capability rather than replacing or redefining the MCP search surface.
- **UR-92 [INFERRED]:** The CLI search tool should reuse the existing block-store and rooted-tree boundaries rather than introducing a parallel repository-local search corpus description.
- **UR-93 [KNOWN]:** The current rooted quality tool's per-pair child-spread warnings are probably false positives, so this heuristic should be reported as an aggregate count rather than as emitted warning findings.
- **UR-94 [KNOWN]:** The rooted quality tool should compute mean distance from centroid for each block, then report mean and standard deviation by layer as a rough statistical measure of where blocks fit within the embedding space.
- **UR-95 [KNOWN]:** The rooted quality tool should compute, for every layer, the mean intra-block dispersion and the standard deviation of dispersion across all blocks in that layer so block cohesion and under- or over-splitting become visible.
- **UR-96 [KNOWN]:** The rooted quality tool should compute, for every layer, the mean centroid-to-centroid distance between sibling blocks and the standard deviation of those distances so block separation and overlapping clusters become visible.
- **UR-97 [KNOWN]:** For each block, the rooted quality tool should compute the fraction of total variance explained by the first principal component, aggregate that metric by layer using mean and standard deviation, and use it to detect weak or degenerate PCA axes.
- **UR-98 [KNOWN]:** For each block, the rooted quality tool should measure quantile-bin occupancy counts plus the variance of those occupancies, and detect empty bins plus bins whose occupancy exceeds two times the expected value so quantile failures and misaligned PCA axes become visible.
- **UR-99 [KNOWN]:** For every parent block and its children, the rooted quality tool should compute the percentage of children whose dispersion exceeds the parent's, the mean increase for such cases, and the maximum observed increase so multimodal parents and ineffective splits become visible.
- **UR-100 [KNOWN]:** In this increment, the number of quantile bins should remain a repository-defined default rather than an operator-visible parameter.
- **UR-101 [KNOWN]:** Add a new rooted quality diagnostic for True Nearest Neighbor Recall at Recall@1, Recall@5, and Recall@10.
- **UR-102 [KNOWN]:** The system shall support TNN-Recall using randomly sampled embeddings from the corpus, and this corpus-based mode shall be the default and the source of all aggregate recall metrics.
- **UR-103 [KNOWN]:** Corpus-based TNN-Recall sampling must be uniform over the evaluated embedding set.
- **UR-104 [KNOWN]:** Corpus-based TNN-Recall sampling must be reproducible given a seed.
- **UR-105 [KNOWN]:** Corpus-based TNN-Recall sample size must be configurable.
- **UR-106 [KNOWN]:** Corpus-based TNN-Recall must be the mode used for mean recall, standard-deviation recall, and recall histograms.
- **UR-107 [KNOWN]:** The system may support TNN-Recall evaluation using user-supplied query embeddings as a diagnostic tool only.
- **UR-108 [KNOWN]:** For user-query TNN-Recall, the system shall compute Recall@1, Recall@5, and Recall@10 for the supplied query.
- **UR-109 [KNOWN]:** For user-query TNN-Recall, the system shall report the exact neighbors and approximate neighbors for comparison.
- **UR-110 [KNOWN]:** User-query TNN-Recall results shall be labeled as diagnostic recall.
- **UR-111 [KNOWN]:** The system shall clearly distinguish corpus-based recall as a statistical quality metric from user-query recall as a debugging aid.
- **UR-112 [KNOWN]:** For this rooted quality tool, the corpus-based TNN-Recall evaluation corpus should be the embeddings reachable from the caller-supplied root rather than all embeddings visible in the configured block store.
- **UR-113 [KNOWN]:** Corpus-based rooted TNN-Recall traversal width must be configurable so operators can measure recall as traversal width changes.
- **UR-114 [KNOWN]:** The upstream LexonGraph planning API may support multiple clustering strategies internally, but after published-profile adoption LexonArchiveBuilder no longer owns a repository-local clustering-mode selector for this increment.
- **UR-115 [KNOWN]:** LexonArchiveBuilder should clean up stale requirements and downstream artifacts that still describe indexer-layer clustering-mode selection instead of the approved published-profile path.
- **UR-116 [KNOWN]:** Published profile version `0.1.0` is the approved clustering behavior for this increment; an explicit divisive opt-in is not part of the current external indexer contract.
- **UR-117 [KNOWN]:** This increment should preserve existing MCP search and retrieval behavior exactly; the new control is indexing-time only.
- **UR-118 [KNOWN]:** The published-profile clustering contract should remain content-type-agnostic so it applies uniformly across current and future content types.
- **UR-119 [KNOWN]:** Local/testing and production-shaped indexer invocations should expose the same published-profile clustering contract and default.
- **UR-120 [KNOWN]:** Repository-local clustering mode, algorithm, and tuning controls should remain retired unless a later approved increment reintroduces them through an explicit published-profile-based contract.
- **UR-121 [KNOWN]:** LexonGraph now exposes a simpler higher-level streaming indexing API that groups planning and hierarchy options into a published versioned profile.
- **UR-122 [KNOWN]:** LexonArchiveBuilder should switch from the current lower-level planning-policy configuration path to the published-profile API.
- **UR-123 [KNOWN]:** This increment should use the published profile version `0.1.0`.
- **UR-124 [KNOWN]:** The indexer's external clustering configuration contract should be replaced by a profile-based contract pinned to published profile `0.1.0` rather than preserving the current low-level mode, algorithm, and option controls.
- **UR-125 [INFERRED]:** The approved `0.1.0` profile should apply consistently across local/testing and production-shaped invocations unless a later approved increment expands the profile-version surface.
- **UR-126 [KNOWN]:** LexonGraph has now merged a wgpu acceleration feature, and LexonArchiveBuilder should refresh its pinned upstream LexonGraph revision to include that feature.
- **UR-127 [KNOWN]:** Opting into this upstream wgpu acceleration should not require a LexonArchiveBuilder API, CLI, request-schema, or published-profile contract change for this increment; refreshing the pinned upstream commit should be sufficient.
- **UR-128 [INFERRED]:** The approved published profile version `0.1.0`, existing execution-stage contract, and existing MCP search or retrieval behavior should remain unchanged while the upstream dependency revision advances to pick up wgpu acceleration.
- **UR-129 [KNOWN]:** The target LexonGraph upstream revision for this increment is commit `70a80a2b51b41759217eec05086cb76586c4f1a5`.
- **UR-130 [KNOWN]:** This increment originated as a dependency-pin refresh, but the approved replay-journal capability is also in scope; beyond those two changes, no unrelated requirements, design, validation, API, CLI, or request-schema changes are required unless the target upstream revision proves incompatible during implementation.
- **UR-131 [KNOWN]:** LexonArchiveBuilder should own a durable replay journal for successfully persisted replayable leaf outputs so resumed ingestion and clustering-only replay do not need to rediscover all eligible inputs solely by rescanning the entire configured block store.
- **UR-132 [INFERRED]:** The replay journal should be append-only and should record completion only after the corresponding replayable leaf output has been durably persisted, so crash recovery can distinguish committed replay state from incomplete work.
- **UR-133 [KNOWN]:** Clustering-only execution should prefer LexonArchiveBuilder-owned journaled replay inputs when available and may fall back to block-store iteration for legacy stores, missing journals, or explicitly rebuilt stores.
- **UR-134 [INFERRED]:** The replay-journal contract should remain content-type-agnostic and preserve enough replay metadata to support current and future content types without redefining LexonGraph-owned block or embedding semantics.
- **UR-135 [INFERRED]:** The replay journal should minimize steady-state write and read overhead and support bounded segment rollover for large corpora without requiring in-place mutation of previously committed records.
- **UR-136 [KNOWN]:** The replay journal is a LexonArchiveBuilder-owned orchestration artifact and must not introduce a new MCP-visible surface or a new required LexonGraph contract.
- **UR-137 [KNOWN]:** Clean up dead requirements and downstream specifications that still describe the superseded repository-local clustering-control path instead of the approved published-profile-version path.
- **UR-138 [INFERRED]:** This cleanup should preserve the approved published profile version `0.1.0`, the existing execution-stage contract, and unchanged MCP search and retrieval behavior while removing contradictory references to indexer-layer divisive or algorithm-selectable clustering.
- **UR-139 [KNOWN]:** LexonGraph now exposes a versioned indexing-profile surface, and LexonArchiveBuilder should make the published profile version selectable so the repository can test newer upstream profiles without editing code for each trial.
- **UR-140 [KNOWN]:** For now, LexonArchiveBuilder should track LexonGraph upstream `main` explicitly, with a repository-visible note that this is temporary and is intended to accelerate validation of new upstream published profiles.
- **UR-141 [INFERRED]:** The new profile-version selector should remain an indexing-time control on the existing batch-entry surfaces and must not alter MCP search or retrieval behavior for already-indexed content.
- **UR-142 [INFERRED]:** Profile-version selection should remain environment-neutral and content-type-neutral so local/testing and production-shaped requests, plus current and future content types, use the same published-profile contract.
- **UR-143 [KNOWN]:** Once LexonGraph publishes profile version `0.3.0`, LexonArchiveBuilder should refresh its adopted upstream dependency state so callers can select `0.3.0` immediately while the repository default remains `0.1.0`.
- **UR-144 [KNOWN]:** Once LexonGraph publishes profile version `0.4.0`, LexonArchiveBuilder should refresh its adopted upstream dependency state so callers can select `0.4.0` immediately while the repository default remains `0.1.0`, and repository-owned specifications and docs should treat the earlier `0.3.0` alignment as historical context rather than the current named experiment target.
- **UR-145 [KNOWN]:** LexonGraph now has a published-profile `0.6.x` series, and LexonArchiveBuilder should refresh its adopted upstream dependency state plus repository-owned validation narrative so callers can test those `0.6.x` profiles immediately while the repository default remains `0.1.0`.
- **UR-146 [INFERRED]:** The existing local profile-evaluation workflow should evolve rather than be replaced, so the repository can compare the current `0.6.x` experiments against prior evaluation baselines without changing the external indexing contract.
- **UR-147 [KNOWN]:** Provide or update a repository-local runnable `test.ps1` script so a Windows local/testing workflow can execute the approved profile-evaluation sweep across the target `0.6.x` profiles and emit per-profile artifacts plus comparable summary output.
- **UR-148 [INFERRED]:** This profile-sweep automation is a local/testing operator aid only; it must remain outside the production runtime contract and must not change MCP search or retrieval behavior for already-indexed content.
- **UR-149 [KNOWN]:** Update LexonArchiveBuilder to use the new LexonGraph API for reading back stored embeddings instead of decoding stored embedding payloads inside LexonArchiveBuilder.
- **UR-150 [INFERRED]:** Repository-owned tools that read stored embeddings for quality assessment, rooted search, or diagnostics should rely on the same upstream decode or reconstruction semantics as LexonGraph itself so new embedding encodings do not require duplicated repository-local decoder updates.
- **UR-151 [INFERRED]:** This embedding-readback change should preserve existing CLI and MCP-visible behavior while moving embedding-format awareness behind the upstream LexonGraph API boundary.
- **UR-152 [KNOWN]:** LexonArchiveBuilder should stop treating unsupported stored embedding encodings as a repository-local format table problem when LexonGraph already knows how to reconstruct those embeddings through its shared API.
- **UR-153 [KNOWN]:** All indexer tools that read from or write to the configured block store must allow operators to target either a local filesystem block store or an overlay block store composed of an in-memory cache, a local filesystem cache, and an Azure Blob backing store addressed by SAS URL.
- **UR-154 [KNOWN]:** For this increment, a plain Azure Blob block-store target without the required memory-plus-filesystem overlay is not an approved indexer tool-targeting mode.
- **UR-155 [INFERRED]:** The same block-store targeting contract should apply consistently across batch indexing, standalone clustering discovery, rooted quality assessment, rooted CLI search, and any other indexer-owned operator tool that traverses the shared `BlockStore` boundary.
- **UR-156 [INFERRED]:** The new overlay-capable targeting contract must remain content-type-neutral and preserve the existing shared `BlockStore` abstraction family for indexed blocks, normalized email artifacts, and mailbox provenance artifacts.

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
| CM-INDEXER-029 | Revise | Replace the earlier explicit clustering-algorithm-selection requirement with the repository-pinned published-profile contract for clustering-enabled execution | UR-39, UR-44, UR-50, UR-53, UR-123, UR-124 |
| CM-INDEXER-030 | Revise | Retire operator-facing clustering algorithm and option controls on the CLI in favor of the repository-pinned published profile contract | UR-50, UR-51, UR-52, UR-53, UR-123, UR-124 |
| CM-INDEXER-031 | Revise | Preserve replay-safe and environment-neutral clustering behavior by treating the approved published profile version as the replay-relevant orchestration input | UR-12, UR-13, UR-39, UR-50, UR-52, UR-123, UR-125 |
| CM-INDEXER-032 | Revise | Tighten progress observability so ingestion-plus-embedding runs remain visibly active during long-running embedding or leaf-materialization work between mailbox expansion and downstream streaming-status events | UR-32, UR-41, UR-54, UR-55 |
| CM-INDEXER-033 | Revise | Assign clustering cardinality to the approved published profile rather than repository-local `cluster_count` auto-sizing or explicit override behavior | UR-52, UR-53, UR-56, UR-57, UR-58, UR-123, UR-124 |
| CM-INDEXER-034 | Revise | Require clustering-only replay to emit repository-owned replay-batch submission progress that reports completed batches and cumulative delegated items relative to the known invocation total | UR-32, UR-39, UR-59 |
| CM-INDEXER-035 | Add | Require an explicit runtime-visible handoff between repository-owned replay submission and upstream planning-pass completion waiting so operator logs disambiguate local submission from upstream heartbeats | UR-41, UR-48, UR-60 |
| CM-INDEXER-036 | Revise | Adapt the delegated indexing requirements from the older training-oriented streaming surface to the latest published-profile-compatible upstream surface while preserving the external repository contract | UR-61, UR-62, UR-63, UR-121, UR-122 |
| CM-INDEXER-037 | Revise | Retire repository-owned built-in clustering mapping and rely on the approved published profile instead of upstream built-in planning-policy selection | UR-61, UR-62, UR-65, UR-123, UR-124 |
| CM-INDEXER-038 | Add | Require explicit repository-level regression assessment for capabilities relied on by LexonArchiveBuilder before any behavior is narrowed during the upstream upgrade | UR-64, UR-65, UR-66 |
| CM-INDEXER-039 | Revise | Update progress-observability requirements to map latest upstream planning, hierarchy-planning, and bottom-up assembly lifecycle signals onto the existing runtime progress surface without exposing raw upstream terminology directly | UR-62, UR-63, UR-65 |
| CM-INDEXER-040 | Revise | Preserve current repository-required capabilities across the latest LexonGraph upgrade, including split-stage replay, published-profile pinning, retirement of low-level clustering controls, and stable MCP search-serving behavior | UR-63, UR-64, UR-65, UR-66, UR-123, UR-124 |
| CM-INDEXER-041 | Revise | Adapt the latest-upstream compatibility requirement from planning-policy-only alignment to planning-policy plus telemetry-surface alignment against the newest LexonGraph `main` revision | UR-67, UR-69, UR-71 |
| CM-INDEXER-042 | Revise | Tighten progress-observability requirements so LexonArchiveBuilder projects richer upstream live telemetry and heartbeat events onto the existing runtime progress surface without creating a second telemetry interface | UR-68, UR-69, UR-71 |
| CM-INDEXER-043 | Add | Preserve operator-understandable progress semantics across the telemetry upgrade by distinguishing repository-owned totals, upstream stage-local progress, and materialization-layer counts instead of surfacing ambiguous raw counts | UR-68, UR-69, UR-70 |
| CM-INDEXER-044 | Add | Require failure-only clustering diagnostics that identify the exact failing input set and effective delegated clustering configuration on the runtime log and in a request-adjacent artifact | UR-39, UR-50, UR-72, UR-73, UR-74, UR-75 |
| CM-INDEXER-045 | Revise | Extend clustering-failure diagnostics with compact embedding-health evidence and a small suspicious-input sample so degenerate-embedding failures become diagnosable without dumping all raw vectors | UR-72, UR-73, UR-74, UR-75, UR-76, UR-77 |
| CM-INDEXER-046 | Revise | Extend clustering-failure diagnostics to identify the exact failing partition or otherwise the narrowest provable failing subset, so nested upstream subproblem failures become diagnosable instead of only the top-level attempt | UR-72, UR-73, UR-74, UR-75, UR-78, UR-79 |
| CM-INDEXER-047 | Add | Introduce a rooted block-tree quality assessment tool that traverses a caller-selected root through the existing block-store boundary and reports correctness plus quality findings without changing the MCP surface | UR-80, UR-84, UR-86 |
| CM-INDEXER-048 | Add | Require the assessment to distinguish structural invariants from embedding-space heuristics and to emit quantitative human-readable plus machine-readable quality evidence for each rooted tree | UR-81, UR-82, UR-83, UR-85, UR-87 |
| CM-INDEXER-049 | Add | Introduce a CLI-only rooted search tool that embeds caller-provided text through a caller-provided embedding endpoint, searches a caller-selected rooted tree through `lexongraph-search`, and returns the top `k` matching leaf nodes | UR-88, UR-89 |
| CM-INDEXER-050 | Add | Keep the rooted search tool additive to MCP search while requiring both human-readable and machine-readable result output without introducing a second repository-local search corpus model | UR-90, UR-91, UR-92 |
| CM-INDEXER-051 | Revise | Refine rooted quality assessment so quality reporting covers tree consistency plus per-layer cohesion, separation, PCA-axis, quantile-occupancy, and parent-child split-effectiveness statistics; parent-versus-child spread inversions become aggregate counts rather than per-pair warnings | UR-82, UR-83, UR-93, UR-94, UR-95, UR-96, UR-97, UR-98, UR-99, UR-100 |
| CM-INDEXER-052 | Revise | Extend rooted quality assessment with TNN-Recall at Recall@1, Recall@5, and Recall@10 over the rooted reachable embedding corpus | UR-101, UR-102, UR-112 |
| CM-INDEXER-053 | Add | Require corpus-based TNN-Recall to use uniform, seeded, configurable sampling and to be the only source for aggregate recall metrics and histograms | UR-102, UR-103, UR-104, UR-105, UR-106 |
| CM-INDEXER-054 | Add | Permit optional user-query TNN-Recall as a diagnostic-only mode that reports exact-versus-approximate neighbors and remains separated from automated quality evaluation | UR-107, UR-108, UR-109, UR-110, UR-111 |
| CM-INDEXER-055 | Revise | Extend corpus-based rooted TNN-Recall so the approximate-neighbor path exposes configurable traversal width for measurement sweeps without changing aggregate-mode ownership | UR-102, UR-113 |
| CM-INDEXER-056 | Revise | Retire the stale indexer-layer clustering-mode requirements and keep clustering behavior defined only by the approved published-profile path | UR-114, UR-115, UR-116, UR-123, UR-124 |
| CM-INDEXER-057 | Revise | Preserve indexing-only, environment-neutral, and content-type-neutral clustering behavior through the published-profile contract while keeping repository-local low-level clustering controls retired | UR-117, UR-118, UR-119, UR-120 |
| CM-INDEXER-058 | Revise | Replace the current lower-level planning-policy integration target with the higher-level published-profile streaming API and require `0.1.0` for this increment | UR-121, UR-122, UR-123 |
| CM-INDEXER-059 | Revise | Replace the current external clustering mode, algorithm, and option contract with a profile-based contract pinned to published profile `0.1.0` | UR-121, UR-123, UR-124 |
| CM-INDEXER-060 | Add | Preserve environment-neutral and content-type-neutral indexing behavior while the approved published-profile version remains fixed across invocation shapes in this increment | UR-118, UR-119, UR-123, UR-125 |
| CM-INDEXER-061 | Revise | Refresh the pinned LexonGraph upstream revision to commit `70a80a2b51b41759217eec05086cb76586c4f1a5` to include merged wgpu acceleration while preserving the approved published-profile contract and existing repository-visible behavior | UR-123, UR-126, UR-127, UR-128, UR-129, UR-130 |
| CM-INDEXER-062 | Revise | Change standalone clustering discovery from mandatory whole-store iteration to a journal-preferred replay-input contract with block-store iteration as compatibility fallback | UR-131, UR-133 |
| CM-INDEXER-063 | Add | Require a LAB-owned durable replay journal for successfully persisted replayable leaf outputs so resumed ingestion and clustering-only replay can reuse repository-owned replay state | UR-131, UR-132, UR-136 |
| CM-INDEXER-064 | Add | Constrain the replay journal to remain content-type-agnostic, low-overhead, append-only, and segmentable under large-corpus growth | UR-132, UR-134, UR-135 |
| CM-INDEXER-065 | Revise | Clarify idempotence and recoverability so replay-journaled resume behavior remains subordinate to immutable block semantics rather than redefining LexonGraph recovery ownership | UR-8, UR-131, UR-132, UR-133, UR-136 |
| CM-INDEXER-066 | Revise | Remove contradictory leftover requirements that still describe repository-local clustering controls after published-profile adoption | UR-115, UR-120, UR-137, UR-138 |
| CM-INDEXER-067 | Revise | Expand the published-profile contract from a repository-fixed version to a caller-selectable profile-version surface, while keeping low-level clustering controls retired | UR-139, UR-141, UR-142 |
| CM-INDEXER-068 | Revise | Replace the current fixed LexonGraph revision target with explicit temporary tracking of upstream `main` so new published profiles can be validated quickly | UR-140 |
| CM-INDEXER-069 | Revise | Refresh the adopted upstream dependency state so the published-profile selector can target upstream `0.3.0` immediately while preserving `0.1.0` as the repository default | UR-143 |
| CM-INDEXER-070 | Revise | Refresh the adopted upstream dependency state and repository-owned narrative so the current named experiment target is upstream `0.4.0` while preserving `0.1.0` as the repository default and retaining `0.3.0` only as historical context | UR-144 |
| CM-INDEXER-071 | Revise | Refresh the adopted upstream dependency state and repository-owned narrative so the current named experiment target expands to the upstream `0.6.x` profile series while preserving `0.1.0` as the repository default and retaining `0.5.x` only as prior comparison context | UR-145, UR-146 |
| CM-INDEXER-072 | Add | Require repository-local runnable sweep automation, currently `test.ps1`, for local/testing evaluation of the active published-profile experiment set without changing production or MCP-facing contracts | UR-147, UR-148 |
| CM-INDEXER-073 | Revise | Move stored-embedding readback for repository-owned quality, search, and diagnostic consumers behind the new upstream LexonGraph embedding reconstruction API instead of repository-local decoding logic | UR-149, UR-150, UR-152 |
| CM-INDEXER-074 | Add | Preserve existing CLI and MCP-visible contracts while making upstream LexonGraph the authority for supported stored embedding encodings and reconstruction semantics | UR-150, UR-151, UR-152 |
| CM-INDEXER-075 | Revise | Replace the current local-versus-plain-Azure tool-targeting split with a repository-wide two-mode contract: direct local filesystem or a fixed overlay of memory cache plus local filesystem cache plus Azure Blob SAS-backed storage | UR-153, UR-154, UR-155, UR-156 |

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

- **Before [KNOWN]:** The requirements still reflected an intermediate low-level clustering path in which repository callers would need to choose an explicit delegated clustering algorithm.
- **After [KNOWN]:** The requirements now treat explicit clustering-algorithm selection as retired for this increment and define clustering-enabled execution through the repository-pinned published-profile contract instead.

### BA-INDEXER-030

- **Before [KNOWN]:** The requirements still described an operator-facing CLI surface for selecting a clustering algorithm and supplying supported clustering options.
- **After [KNOWN]:** The requirements now retire that operator-facing low-level clustering surface and keep the CLI aligned to the repository-pinned published profile contract.

### BA-INDEXER-031

- **Before [KNOWN]:** The requirements treated the effective clustering algorithm and option set as the replay-relevant clustering input for repeated runs.
- **After [KNOWN]:** The requirements now define the approved published profile version as the replay-relevant clustering input so repeated runs remain explainable and stable under unchanged upstream semantics.

### BA-INDEXER-032

- **Before [KNOWN]:** Observable progress required mailbox-processing visibility and downstream streaming-status visibility, but it did not explicitly forbid a long silent gap while delegated items were being embedded or leaf blocks were being materialized before streaming-status events began.
- **After [KNOWN]:** Observable progress now explicitly requires continued runtime-visible activity during ingestion-plus-embedding work between delegated-item preparation and the first downstream streaming-status event so slow embedding work does not look like a hung batch.

### BA-INDEXER-033

- **Before [KNOWN]:** The requirements still carried forward repository-local `cluster_count` auto-sizing and override behavior from the older low-level clustering contract.
- **After [KNOWN]:** The requirements now assign clustering cardinality to the approved published profile and retire repository-local `cluster_count` tuning for this increment.

### BA-INDEXER-034

- **Before [KNOWN]:** Progress observability required visible mailbox, embedding, training, and finalization activity, but it did not explicitly require clustering-only replay to report repository-owned replay-batch submission progress using the known batch and delegated-item totals.
- **After [KNOWN]:** The requirements now require clustering-only replay to emit repository-owned progress after each replay batch submission, including completed-batch and cumulative delegated-item visibility relative to the known invocation total.

### BA-INDEXER-035

- **Before [KNOWN]:** Runtime progress could transition from repository-owned replay submission into upstream planning-pass heartbeats without an explicit boundary marker, so operators could not tell whether LexonArchiveBuilder was still submitting work or was already waiting for upstream pass completion.
- **After [KNOWN]:** The requirements now require an explicit runtime-visible handoff when repository-owned replay submission completes and the runtime begins waiting for upstream planning-pass completion or an equivalent delegated lifecycle boundary.

### BA-INDEXER-036

- **Before [KNOWN]:** The requirements described the delegated streaming lifecycle in terms of training passes, built-in clustering factories, and training completion because that was the upstream surface previously integrated by LexonArchiveBuilder.
- **After [KNOWN]:** The requirements describe the delegated streaming lifecycle in terms of the latest upstream published-profile-compatible surface while preserving LexonArchiveBuilder's caller-visible stage contract and adapter-orchestrator role.

### BA-INDEXER-037

- **Before [KNOWN]:** The requirements assumed LexonArchiveBuilder would satisfy the upstream built-in clustering contract through the older `BuiltInClustering` and `BuiltInClusteringFactory` seam.
- **After [KNOWN]:** The requirements retire repository-level algorithm choices and option families for this increment and rely on the approved published profile instead.

### BA-INDEXER-038

- **Before [KNOWN]:** The requirements preserved repository invariants across upstream API changes, but they did not explicitly require distinguishing a true upstream feature regression from a mechanical API rename or lifecycle reshaping.
- **After [KNOWN]:** The requirements explicitly require regression assessment for repository-relied-on capabilities so the upgrade cannot silently narrow behavior.

### BA-INDEXER-039

- **Before [KNOWN]:** Progress observability requirements assumed the older upstream status taxonomy that reported training and materialization phases using the prior names.
- **After [KNOWN]:** The requirements preserve operator-visible progress continuity while allowing LexonArchiveBuilder to remap the latest upstream planning, hierarchy-planning, and bottom-up assembly phases onto the same repository-owned runtime progress surface.

### BA-INDEXER-040

- **Before [KNOWN]:** The requirements preserved stage-selection and MCP invariants during the earlier streaming-indexer migration, but they did not yet enumerate the repository-required capabilities that must survive the newest planning-policy upgrade review.
- **After [KNOWN]:** The requirements explicitly preserve split-stage replay, published-profile pinning, retirement of low-level clustering controls, progress projection, and unchanged MCP search-serving behavior as feature-level obligations for the latest upgrade.

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

### BA-INDEXER-045

- **Before [KNOWN]:** The current clustering-failure diagnostics can identify which repository-visible inputs were clustered and which effective delegated configuration was used, but they still do not expose enough embedding-health evidence to explain why a rank-collapse or similar degenerate-embedding failure occurred.
- **After [KNOWN]:** The requirements now require clustering-failure diagnostics to add compact embedding-health evidence and a small suspicious-input sample so embedding-degeneracy failures become diagnosable without logging or persisting every raw embedding vector.

### BA-INDEXER-046

- **Before [KNOWN]:** The current clustering-failure diagnostics describe the top-level clustering attempt, but they still cannot identify the narrower upstream partition or subproblem that actually triggered a nested rank-collapse failure.
- **After [KNOWN]:** The requirements now require clustering-failure diagnostics to identify the exact failing partition when the upstream failure surface exposes it, or otherwise the narrowest repository-visible subset LexonArchiveBuilder can prove was active at the failing step.

### BA-INDEXER-047

- **Before [KNOWN]:** The requirements describe runtime progress and failure diagnostics, but they do not yet require any post-index tool that can traverse a rooted stored block tree and assess whether the resulting hierarchy looks structurally correct or spatially well-formed.
- **After [KNOWN]:** The requirements now introduce a CLI-only rooted block-tree quality assessment tool that reads through the existing `BlockStore` boundary, starts from a caller-supplied root block, and reports structural-correctness plus embedding-space quality findings without changing MCP behavior.

### BA-INDEXER-048

- **Before [KNOWN]:** The requirements do not distinguish between hard block-tree invariant violations and softer heuristics about how well parent and child blocks partition embedding space, nor do they require quantitative reporting of each block's represented shape.
- **After [KNOWN]:** The requirements now distinguish structural findings from advisory quality heuristics and require both a human-readable summary and a machine-readable JSON artifact containing quantitative evidence about rooted tree quality and each block's represented embedding-space region.

### BA-INDEXER-049

- **Before [KNOWN]:** The requirements preserve existing MCP search behavior for already-indexed content, but they do not require any repository-local CLI surface that lets an operator issue an ad hoc text query directly against a caller-selected rooted tree.
- **After [KNOWN]:** The requirements now add a CLI-only rooted search tool that embeds caller-provided text through a caller-provided embedding endpoint, searches a caller-selected rooted tree through `lexongraph-search`, and returns the top `k` matching leaf nodes without changing MCP behavior.

### BA-INDEXER-050

- **Before [KNOWN]:** The requirements do not define whether such an operator search tool should emit only terminal-friendly output or also a machine-readable representation, and they do not constrain whether the tool may invent a second repository-local search corpus model.
- **After [KNOWN]:** The requirements now require rooted CLI search to emit both human-readable and machine-readable results while remaining additive to the existing MCP search surface and reusing the existing rooted-tree plus block-store boundaries.

### BA-INDEXER-051

- **Before [KNOWN]:** The rooted quality requirements treated parent-versus-child centroid-distance spread as advisory per-pair findings, which can overstate problems when the parent is measured over summarized child representatives while the child is measured over its own members, and they did not yet define a fuller per-layer quality model for cohesion, separation, PCA-axis strength, quantile occupancy behavior, or split effectiveness.
- **After [KNOWN]:** The rooted quality requirements now treat parent-versus-child spread inversions as aggregate heuristic counts only, not emitted warning findings, and require the report to include a refined per-layer quality model covering intra-block dispersion, sibling-centroid separation, PCA-axis strength, quantile-bin occupancy variance, and parent-to-child dispersion deltas, with repository-defined default quantile bins in this increment.

### BA-INDEXER-052

- **Before [KNOWN]:** The rooted quality requirements did not yet require any nearest-neighbor retrieval-quality diagnostic, so operators had no repository-owned Recall@k signal for how well a rooted tree preserved exact-neighbor retrieval.
- **After [KNOWN]:** The rooted quality requirements now add rooted TNN-Recall at Recall@1, Recall@5, and Recall@10 as part of post-index quality assessment over the embeddings reachable from the caller-supplied root.

### BA-INDEXER-053

- **Before [KNOWN]:** Even if a recall diagnostic were added, the requirements did not constrain whether aggregate recall metrics would be computed from rooted-corpus samples, user-supplied debug queries, or a non-reproducible sampling method.
- **After [KNOWN]:** The requirements now define corpus-based TNN-Recall as the default and only aggregate-evaluation mode, with uniform rooted-corpus sampling, configurable sample size, and seed-based reproducibility for mean recall, recall standard deviation, and recall histograms.

### BA-INDEXER-054

- **Before [KNOWN]:** The requirements did not distinguish between a statistical recall metric computed over the rooted corpus and a one-off user-supplied query used to debug approximate-neighbor behavior.
- **After [KNOWN]:** The requirements now separate corpus-based recall from optional user-query diagnostic recall, require user-query output to show exact and approximate neighbors for comparison, and explicitly exclude diagnostic recall from aggregate quality metrics.

### BA-INDEXER-055

- **Before [KNOWN]:** The rooted TNN-recall requirements defined seeded corpus sampling and aggregate-mode ownership but did not specify whether approximate-neighbor traversal width could be tuned for measurement.
- **After [KNOWN]:** The rooted TNN-recall requirements now require configurable traversal width for corpus-based evaluation so operators can measure recall across different approximate-search widths while preserving the rooted-corpus aggregate boundary.

### BA-INDEXER-056

- **Before [KNOWN]:** The requirements still carried forward a transient repository-local clustering-mode story that treated aggregation-versus-divisive selection as part of the approved indexer boundary.
- **After [KNOWN]:** The requirements now treat that mode-selection story as retired and keep clustering behavior defined solely by the approved published-profile path for this increment.

### BA-INDEXER-057

- **Before [KNOWN]:** The requirements still implied that repository-local low-level clustering controls might remain part of a stable cross-environment contract even after published-profile adoption.
- **After [KNOWN]:** The requirements now preserve unchanged MCP behavior, require the same published-profile contract across local/testing and production-shaped invocations, keep that contract generic across current and future content types, and keep repository-local low-level clustering controls retired.

### BA-INDEXER-058

- **Before [KNOWN]:** The requirements targeted the lower-level planning-policy integration surface and did not yet capture the newly published higher-level LexonGraph profile API.
- **After [KNOWN]:** The requirements now target the higher-level published-profile API and require the approved `0.1.0` profile version for this increment.

### BA-INDEXER-059

- **Before [KNOWN]:** The external indexer contract centered clustering configuration on explicit clustering mode, algorithm, and algorithm-specific option controls.
- **After [KNOWN]:** The external indexer contract now centers clustering configuration on the approved published profile version instead of exposing the retired low-level control family.

### BA-INDEXER-060

- **Before [KNOWN]:** The requirements preserved clustering-configuration parity across environments and content types, but they did so through a low-level mode-and-algorithm contract that is now being retired.
- **After [KNOWN]:** The requirements preserve the same parity and extensibility expectations while pinning both invocation shapes to the same approved published profile version in this increment.

### BA-INDEXER-061

- **Before [KNOWN]:** The requirements pinned LexonGraph to the published-profile-compatible upstream revision, but they did not yet capture adoption of the newly merged upstream wgpu acceleration feature or the exact target upstream commit.
- **After [KNOWN]:** The requirements now call for refreshing the pinned LexonGraph revision specifically to commit `70a80a2b51b41759217eec05086cb76586c4f1a5` to include upstream wgpu acceleration while preserving the approved `0.1.0` published profile and all existing repository-visible contracts.

### BA-INDEXER-062

- **Before [KNOWN]:** Standalone clustering input discovery was defined entirely as whole-store iteration through the upstream block-iteration API, so LexonArchiveBuilder had no repository-owned persistent replay catalog for clustering-only replay or resumed ingestion.
- **After [KNOWN]:** The requirements now prefer a LAB-owned durable replay journal as the primary clustering-only replay-input source while preserving upstream block-store iteration as a compatibility fallback for legacy or explicitly rebuilt stores.

### BA-INDEXER-063

- **Before [KNOWN]:** The requirements did not define any repository-owned durable artifact that records successful replayable leaf completion at ingestion time.
- **After [KNOWN]:** The requirements now introduce a LAB-owned replay journal that records successful replayable leaf completion only after durable persistence so later resume or clustering-only execution can reuse repository-owned replay state.

### BA-INDEXER-064

- **Before [KNOWN]:** The requirements did not constrain the persistence shape of any repository-owned replay catalog, so append-only write discipline, large-corpus rollover behavior, and low-overhead sequential replay were unspecified.
- **After [KNOWN]:** The requirements now constrain the replay journal to remain append-only, low-overhead, and segmentable under growth without in-place mutation of committed records.

### BA-INDEXER-065

- **Before [KNOWN]:** Idempotence and recoverability were described only at the immutable-block and upstream-replay-contract level, without a repository-owned requirement for durable partial-progress reuse between split-stage invocations.
- **After [KNOWN]:** The requirements now clarify that LexonArchiveBuilder may reuse repository-owned replay-journal state for resume and clustering-only replay while remaining subordinate to LexonGraph-owned immutable-block and replay-validation semantics.

### BA-INDEXER-066

- **Before [KNOWN]:** The requirements contained contradictory remnants from the earlier clustering-mode-at-this-layer exploration even though the approved implementation path had already moved to the repository-pinned published profile `0.1.0`.
- **After [KNOWN]:** The requirements now consistently describe the profile-version-based clustering contract and treat repository-local clustering mode, algorithm, and tuning controls as retired for this increment.

### BA-INDEXER-067

- **Before [KNOWN]:** The requirements treated published profile version `0.1.0` as a repository-fixed value, so trying a newer upstream published profile required changing repository code or specs first.
- **After [KNOWN]:** The requirements now preserve `0.1.0` as the default while allowing callers to select another published profile version on the approved indexing surfaces for evaluation without reintroducing low-level clustering controls.

### BA-INDEXER-068

- **Before [KNOWN]:** The requirements targeted a fixed upstream LexonGraph revision or commit when adopting new profile behavior.
- **After [KNOWN]:** The requirements now call for explicit temporary tracking of LexonGraph `main`, with a repository-visible note that this is solely to accelerate validation of newly exposed upstream published profiles.

### BA-INDEXER-069

- **Before [KNOWN]:** The requirements allowed caller-selectable published profile versions in general, but they did not explicitly require LexonArchiveBuilder to refresh its adopted upstream dependency state promptly when LexonGraph published version `0.3.0`.
- **After [KNOWN]:** The requirements now call for refreshing the adopted upstream dependency state so callers can select published profile version `0.3.0` immediately, while preserving `0.1.0` as the repository default for omitted selectors.

### BA-INDEXER-070

- **Before [KNOWN]:** The requirements named published profile version `0.3.0` as the current explicit experiment target and did not distinguish that alignment from later published-profile experiments.
- **After [KNOWN]:** The requirements then named published profile version `0.4.0` as the current explicit experiment target, preserved `0.1.0` as the repository default for omitted selectors, and retained the earlier `0.3.0` alignment only as historical context.

### BA-INDEXER-071

- **Before [KNOWN]:** The requirements treated `0.5.x` as the current named published-profile experiment target and did not describe how the newer `0.6.x` series should fit into the existing evaluation narrative.
- **After [KNOWN]:** The requirements now treat the upstream `0.6.x` series as the current named published-profile experiment target, preserve `0.1.0` as the repository default for omitted selectors, and retain `0.5.x` only as prior comparison context for evaluation.

### BA-INDEXER-072

- **Before [KNOWN]:** The requirements did not define an approved repository-local operator automation surface for rerunning published-profile sweeps as the active experiment target changed.
- **After [KNOWN]:** The requirements now call for a runnable repository-local `test.ps1` sweep that exercises the approved local/testing evaluation flow across the active experiment set, emits per-profile artifacts plus comparable summary output, and leaves production plus MCP-facing contracts unchanged.

### BA-INDEXER-073

- **Before [KNOWN]:** LexonArchiveBuilder requirements allowed repository-owned consumers such as rooted quality or diagnostics to decode stored embedding payloads through repository-local format handling, which drifted behind new upstream encodings.
- **After [KNOWN]:** The requirements now move stored-embedding readback behind the upstream LexonGraph embedding reconstruction API so repository-owned consumers reuse upstream-supported encoding semantics instead of maintaining their own decoder table.

### BA-INDEXER-074

- **Before [KNOWN]:** Repository-owned support for new stored embedding encodings depended on duplicating LexonGraph format knowledge inside LexonArchiveBuilder.
- **After [KNOWN]:** The requirements now treat LexonGraph as the authority for stored embedding reconstruction while preserving the existing CLI and MCP-visible surfaces that consume those decoded embeddings.

### BA-INDEXER-075

- **Before [KNOWN]:** The requirements allowed indexer-owned tools to vary between a local filesystem block store and a plain Azure Blob production target, but they did not require a uniform overlay-capable targeting contract across every tool surface that uses the shared `BlockStore` boundary.
- **After [KNOWN]:** The requirements now constrain all indexer-owned block-store-consuming tools to a consistent two-mode target model: direct local filesystem access or a fixed overlay block store composed of memory cache, local filesystem cache, and Azure Blob SAS-backed storage.

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
the same invocation, LexonArchiveBuilder SHALL derive clustering inputs from a
repository-owned replay-input source that is valid for the configured store
snapshot.

- **Preferred source [KNOWN]:** When the selected environment exposes a
  LexonArchiveBuilder-managed local filesystem block-store root and a valid
  LexonArchiveBuilder-owned replay journal is available for that configured
  store snapshot, standalone clustering SHALL reconstruct replay inputs from
  that journal rather than by rescanning the entire configured block store.
- **Compatibility fallback [KNOWN]:** When the replay journal is absent,
  incomplete, incompatible with the configured store snapshot, or intentionally
  unavailable because the store was rebuilt without journal continuity,
  standalone clustering MAY derive clustering inputs by iterating the
  configured `BlockStore` through the LexonGraph block-iteration API.
- **Scope [KNOWN]:** Regardless of discovery source, standalone clustering
  SHALL examine all clustering-eligible replayable leaf inputs visible to the
  selected store snapshot rather than only inputs associated with a prior
  request or summary.
- **Filtering boundary [INFERRED]:** Blocks or artifacts outside the approved
  clustering-input surface, including repository-owned artifact classes that
  are not valid clustering inputs, are outside this requirement's input set.
- **Request-shape implication [KNOWN]:** A clustering-only invocation may use an
  empty item collection because input discovery occurs from the configured block
  store rather than from request-supplied sources.
- **Idempotence implication [INFERRED]:** Repeating the clustering-only stage
  against an unchanged clustering-eligible block-store snapshot is expected to
  yield the same logical clustering result under unchanged upstream semantics
  whether replay inputs are discovered from a valid replay journal or from
  compatibility-mode block-store iteration.
- **Traceability:** UR-39, UR-40, UR-131, UR-133

#### RQ-INDEXER-003E1 - Durable replay journal for split-stage reuse

LexonArchiveBuilder SHALL persist a repository-owned durable replay journal for
successfully persisted replayable leaf outputs when the selected environment
exposes a LexonArchiveBuilder-managed local filesystem block-store root.

- **Write boundary [KNOWN]:** A journal record SHALL become durable only after
  the corresponding replayable leaf output has been durably persisted through
  the approved storage boundary.
- **Reuse scope [KNOWN]:** The journal SHALL support at least:
  1. resumed ingestion that can recognize already completed replayable leaf
     outputs
  2. clustering-only replay that can reconstruct delegated replay inputs
     without requiring whole-store discovery in the common case
- **Environment scope [KNOWN]:** Environments that do not expose a
  LexonArchiveBuilder-managed local filesystem block-store root remain on the
  compatibility fallback path until an equivalent journal location is approved.
- **Ownership boundary [KNOWN]:** The journal is a LexonArchiveBuilder-owned
  orchestration artifact and SHALL NOT redefine LexonGraph-owned block
  identity, embedding, or replay-validation semantics.
- **Content-model constraint [INFERRED]:** The journal contract SHALL remain
  generic enough that future content types can emit replayable completion
  records without reshaping the batch contract around mailbox-only or
  document-only assumptions.
- **Traceability:** UR-131, UR-132, UR-134, UR-136

#### RQ-INDEXER-003E2 - Append-only and segmentable replay-journal behavior

The LexonArchiveBuilder replay journal SHALL optimize for low-overhead
sequential append and replay under large-corpus operation.

- **Mutation constraint [KNOWN]:** Committed journal records SHALL be append-only
  and SHALL NOT require in-place mutation for ordinary progress recording.
- **Growth constraint [KNOWN]:** The journal SHALL support bounded segment
  rollover so one logical indexing corpus does not require unbounded growth of
  a single journal file.
- **Recovery implication [INFERRED]:** Crash recovery MUST tolerate a partial or
  incomplete trailing write without invalidating earlier committed journal
  records.
- **Format boundary [UNKNOWN]:** The compact machine-readable encoding used by
  the journal is not yet fixed at the requirements layer, so downstream design
  must decide whether the first implementation should standardize on CBOR or a
  different binary framing while preserving the above invariants.
- **Traceability:** UR-132, UR-135

#### RQ-INDEXER-003F - Published profile adoption for clustering-enabled execution

For any execution stage that includes clustering plus block assembly,
LexonArchiveBuilder SHALL invoke the delegated LexonGraph streaming indexer
through the published-profile API and SHALL accept a caller-selectable
published profile version, defaulting to `0.1.0`, for this increment.

- **Upstream contract [KNOWN]:** The delegated streaming indexer now exposes a
  published-profile surface in which profile version selects the approved leaf
  algorithm, packing strategy, hierarchy strategy, summary policy, and related
  planning parameters as one versioned configuration.
- **Approved default [KNOWN]:** Published profile version `0.1.0` remains the
  approved default clustering-enabled profile in this increment.
- **Evaluation rule [KNOWN]:** Callers may select a different upstream-published
  profile version for evaluation without reintroducing repository-local
  clustering algorithms, planning policies, or tuning controls.
- **Published-profile availability [KNOWN]:** When temporary upstream `main`
  tracking exposes the current named experiment target in the `0.6.x` series,
  LexonArchiveBuilder SHALL refresh its adopted upstream dependency state so
  callers can select that version without changing the omitted-selector default
  away from `0.1.0`.
- **Compatibility rule [KNOWN]:** LexonArchiveBuilder SHALL adopt that profile
  directly rather than reconstructing an equivalent repository-local
  planning-policy configuration from lower-level clustering controls.
- **Stage boundary [KNOWN]:** This requirement applies to the `full` and
  `clustering+block-assembly` execution stages and does not affect
  `ingestion+embedding` execution.
- **Delegation boundary [KNOWN]:** LexonArchiveBuilder still delegates all actual
  planning and hierarchy construction behavior to LexonGraph and does not
  define repository-local clustering profiles or planning semantics in this
  increment.
- **Traceability:** UR-39, UR-44, UR-61, UR-62, UR-121, UR-122, UR-123, UR-124, UR-139, UR-141, UR-142, UR-143, UR-144, UR-145, UR-146

#### RQ-INDEXER-003G - Profile-based clustering contract on the CLI and `BatchRequest`

LexonArchiveBuilder SHALL express clustering-enabled batch configuration on the
CLI and `BatchRequest` in terms of the published profile contract, including an
explicit profile-version selector, rather than low-level clustering mode,
clustering algorithm, and algorithm-specific option controls.

- **Required surface [KNOWN]:** Clustering-enabled invocations must no longer
  require callers to understand or populate low-level upstream planning-policy
  fields when the approved published profile is sufficient.
- **Selector rule [KNOWN]:** The approved external contract now includes a
  published-profile-version selector on both surfaces so callers can keep the
  default `0.1.0` behavior or choose another upstream-published profile version
  for evaluation.
- **Retirement rule [KNOWN]:** Legacy clustering mode, clustering algorithm, and
  algorithm-specific tuning controls SHALL be removed from the approved
  external contract for this increment rather than silently accepted and
  ignored.
- **Environment-parity implication [INFERRED]:** The same CLI surface must remain
  usable for local/testing and production-shaped batch invocations so
  environment selection does not introduce a separate profile-configuration
  interface family.
- **Traceability:** UR-4, UR-12, UR-13, UR-42, UR-119, UR-123, UR-124, UR-125, UR-139, UR-141, UR-142

#### RQ-INDEXER-003H - Published profile-owned clustering cardinality

For clustering-enabled execution in this increment, LexonArchiveBuilder SHALL
use the clustering cardinality and related planning parameters owned by the
selected published profile version rather than deriving or accepting
repository-local `cluster_count` overrides.

- **Behavior change [KNOWN]:** The previously approved omitted-option auto-sizing
  rule is retired for this increment because the published profile now owns the
  relevant planning cardinality.
- **Override rule [KNOWN]:** Callers SHALL NOT supply repository-local
  `cluster_count` overrides while clustering-enabled execution is governed by a
  selected published profile version.
- **Delegation boundary [KNOWN]:** Any future variation in clustering cardinality
  for this surface must come from an approved published profile version rather
  than from repository-local remapping of profile internals.
- **Traceability:** UR-56, UR-57, UR-58, UR-121, UR-123, UR-124, UR-139

#### RQ-INDEXER-003I - Upstream feature-regression containment

When adapting to the latest LexonGraph version, LexonArchiveBuilder SHALL
preserve every repository-required capability that remains semantically
supported by the upstream contract and SHALL classify any missing capability as
an explicit upstream regression instead of silently narrowing repository
behavior.

- **Repository-required capabilities [KNOWN]:**
  - the external stage contract (`full`, `ingestion+embedding`, `clustering+block-assembly`)
  - deterministic split-stage replay acceptance
  - adoption of the published-profile API for clustering-enabled execution
  - defaulting clustering-enabled execution to published profile `0.1.0` while
    permitting explicit selection of another upstream-published profile version
    for evaluation
  - refreshing the adopted upstream dependency state promptly enough that newly
    published upstream profile versions in the current `0.6.x` experiment series
    become selectable
    without redefining the repository default
  - repository-local local/testing automation that can exercise the current
    published-profile experiment set through the approved batch-plus-quality
    workflow without requiring per-profile code edits
  - removal of the retired low-level clustering mode, algorithm, and numeric tuning controls from the approved external contract
  - runtime progress projection that keeps raw upstream lifecycle details behind the repository-owned progress surface
  - projection of the latest upstream live telemetry and heartbeat events onto that same repository-owned progress surface
  - unchanged MCP search-serving and retrieval behavior for already-indexed content
- **Regression rule [INFERRED]:** If the latest upstream surface removes or weakens one of those capabilities, LexonArchiveBuilder SHALL treat that as a compatibility finding requiring explicit design and implementation handling, not as permission to drop the affected repository behavior.
- **Main-tracking rule [KNOWN]:** While rapid published-profile validation is an
  approved goal, LexonArchiveBuilder SHALL track the LexonGraph dependency set
  against upstream `main` explicitly and SHALL note that this is a temporary
  expedient for testing newly exposed upstream published profiles quickly.
- **Acceleration adoption rule [KNOWN]:** When the latest upstream revision adds
  wgpu acceleration without requiring a caller-surface change, LexonArchiveBuilder
  SHALL pick that up through the same temporary `main` tracking rather than by
  introducing repository-local API or contract changes for this increment.
- **Boundary [KNOWN]:** This requirement does not force LexonArchiveBuilder to re-implement upstream planning internals in-repo; it constrains adaptation and regression reporting at the repository boundary.
- **Traceability:** UR-47, UR-61, UR-63, UR-64, UR-65, UR-66, UR-67, UR-68, UR-69, UR-71, UR-126, UR-127, UR-128, UR-129, UR-130, UR-140, UR-143, UR-144, UR-145, UR-147, UR-148

#### RQ-INDEXER-003J - Local published-profile sweep automation

LexonArchiveBuilder SHALL provide a repository-local runnable operator
automation surface, currently `test.ps1`, that reuses the approved local/testing
indexing and rooted-quality workflow to evaluate the current published-profile
experiment set without changing repository code for each tested profile.

- **Local-only boundary [KNOWN]:** This automation is for local/testing operator
  evaluation and does not define a production runtime entrypoint, request
  schema, or deployment contract.
- **Current experiment target [KNOWN]:** The active named experiment target is
  the upstream `0.6.x` published-profile series, and the automation SHALL allow
  operators to run the approved evaluation flow across that series.
- **Comparison rule [INFERRED]:** The automation should continue to emit
  per-profile artifacts and comparable summary output so `0.6.x` results can be
  compared against earlier baselines such as `0.5.x` when those profiles are
  included in the run.
- **Contract-preservation rule [INFERRED]:** The automation SHALL drive the
  existing batch and rooted-quality tool surfaces instead of introducing a
  testing-only indexing API or changing MCP search semantics.
- **Extensibility rule [INFERRED]:** The operator-edited profile list may change
  as new published profiles land, but the automation surface should remain
  version-series-agnostic so future profile series do not require a new
  repository contract shape solely to update hard-coded experiment labels.
- **Traceability:** UR-12, UR-84, UR-85, UR-139, UR-140, UR-145, UR-146, UR-147, UR-148

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
  - overlay block store for production-oriented operation, composed of memory cache + local filesystem cache + Azure Blob Storage backing addressed by SAS URL
- **Approved tool-targeting modes [KNOWN]:** Every indexer-owned tool that reads from or writes to the shared `BlockStore` boundary SHALL support exactly these two target modes: direct local filesystem or the approved overlay block store.
- **Disallowed mode [KNOWN]:** A plain Azure Blob block-store target without the required overlay layers is not an approved operator-facing mode in this increment.
- **Current increment [KNOWN]:** The existing local/testing realization remains required, and this increment additionally requires the overlay target mode to be usable on the same repository-owned tool surfaces rather than being introduced tool-by-tool.
- **Local filesystem interoperability [KNOWN]:** The local/testing filesystem-backed realization SHALL use the LexonGraph-owned filesystem block-store contract, including its on-disk naming and layout scheme, so LexonGraph filesystem tooling such as `lexongraph-block-inspect` can operate on LexonArchiveBuilder-produced local stores.
- **Local implementation target [KNOWN]:** The local/testing filesystem-backed realization SHALL use the upstream `lexongraph-block-store-fs` crate rather than a repository-local filesystem naming scheme.
- **Migration boundary [KNOWN]:** This local filesystem interoperability correction may require a fresh or rebuilt local store; continued read compatibility with blocks written by the superseded custom local layout is not required in this increment.
- **Overlay shape [KNOWN]:** The non-local target mode SHALL be a fixed ordered overlay containing an in-memory cache layer, a local filesystem cache layer, and an Azure Blob backing layer addressed through a SAS URL rather than a caller-selectable arbitrary stack of storage backends.
- **Artifact reuse [KNOWN]:** The same environment-selected `BlockStore` abstraction family SHALL also be used for normalized email artifacts and mailbox provenance artifacts, provided indexing contracts and retrieval references remain explicit.
- **Tool-surface consistency [INFERRED]:** Batch indexing, standalone clustering discovery, rooted quality assessment, rooted CLI search, and future indexer-owned operator tools SHALL share the same block-store target-selection contract instead of inventing per-tool storage mode variants.
- **Assessment-tool implication [INFERRED]:** Post-index rooted block-tree quality assessment SHALL also read blocks through the same environment-selected `BlockStore` boundary rather than bypassing it with a repository-specific storage reader.
- **Mailbox retention [KNOWN]:** Mailbox provenance artifacts SHALL be retained so the original source material remains available for re-normalization, re-chunking, and re-ingestion flows.
- **Traceability:** UR-3, UR-6, UR-9, UR-12, UR-13, UR-18, UR-22, UR-25, UR-26, UR-27, UR-28, UR-80, UR-86, UR-153, UR-154, UR-155, UR-156

#### RQ-INDEXER-006 - Embedding provider integration

LexonArchiveBuilder SHALL obtain embeddings through a provider that satisfies `lexongraph_embeddings_trait::EmbeddingProvider` and is reached through an OpenAI-compatible HTTP embedding interface.

- **Architectural target embedding profiles [KNOWN]:**
  - local STAPI-compatible embedding service
  - Azure OpenAI embedding model
- **Constraint [KNOWN]:** Provider selection varies by environment and must not require changes to the collection-oriented batch contract.
- **MVP realization [KNOWN]:** The first in-repo implementation must execute end-to-end against a local embedding service. Azure OpenAI remains a required future profile boundary, but not a required executable realization for the first MVP.
- **Integration note [KNOWN]:** The delegated indexer consumes `EmbeddingInput` and `EmbeddingSpec` through the shared embeddings trait boundary.
- **CLI-search implication [INFERRED]:** Any repository-local rooted search tool that generates query embeddings in this increment must remain compatible with the same OpenAI-compatible embedding boundary family, even if the operator supplies the concrete endpoint at CLI time.
- **Traceability:** UR-7, UR-9, UR-12, UR-13, UR-88

#### RQ-INDEXER-007 - Environment-specific adapter selection

LexonArchiveBuilder SHALL select storage and embedding integrations according to environment without changing the delegated indexing contract or the batch input contract.

- **Local/testing [KNOWN]:** local filesystem + local embedding service
- **Production-oriented [KNOWN]:** overlay block store (memory cache + local filesystem cache + Azure Blob SAS-backed storage) + Azure OpenAI
- **Constraint [KNOWN]:** Environment-specific adapter selection for every indexer-owned tool must expose the same two storage-targeting modes rather than allowing some tools to target local filesystem while others target a plain Azure Blob backend directly.
- **Traceability:** UR-6, UR-7, UR-12, UR-13, UR-153, UR-154, UR-155, UR-156

#### RQ-INDEXER-008 - Idempotent reruns

LexonArchiveBuilder SHALL preserve idempotent rerun behavior for repeated indexing of the same source content.

- **Mechanism owner [KNOWN]:** The underlying LexonGraph API owns batch and recovery semantics.
- **Required property [KNOWN]:** Produced blocks are immutable and identified by hash, so reruns must not create distinct logical outputs for unchanged content.
- **Email artifact implication [INFERRED]:** Repeated normalization of semantically unchanged email content should resolve to the same normalized email artifact identity and the same derived chunk identities under a stable normalization and chunking policy.
- **Concurrency implication [INFERRED]:** Same-layer leaf scheduling must not change the logical block set or final root produced for unchanged input relative to the approved delegated indexing contract.
- **Standalone clustering implication [INFERRED]:** Repeating the clustering-only
  stage against the same clustering-eligible block-store snapshot must not
  change the logical clustering result under unchanged upstream semantics.
- **Replay-journal implication [INFERRED]:** Reusing a valid replay journal for
  resumed ingestion or clustering-only replay must not create a distinct
  logical outcome from the same immutable leaf set merely because the runtime
  avoided whole-store rediscovery.
- **Clustering-configuration implication [INFERRED]:** Repeating a clustering-enabled
  run against unchanged inputs under the same approved published profile
  version must not change the logical clustering result merely because profile
  selection or resolution differed across invocations.
- **Traceability:** UR-8, UR-16, UR-36, UR-121, UR-123, UR-124, UR-131, UR-132, UR-133

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
  attempt, including the selected published profile version, the active
  embedding specification, the block-size target, the selected execution
  stage, and any profile-resolved delegated configuration identifiers needed to
  explain the upstream failure.
- **Embedding-health visibility [KNOWN]:** Failure diagnostics must include compact
  embedding-health evidence sufficient to explain embedding-degeneracy failures,
  including summary statistics and counts that let an operator distinguish
  cases such as zero vectors, repeated vectors, non-finite values, or collapsed
  variance without recomputing the run under a debugger.
- **Failing-subset visibility [KNOWN]:** Failure diagnostics must identify the
  exact failing partition or subproblem when the upstream failure surface
  exposes it, and otherwise must identify the narrowest repository-visible
  subset LexonArchiveBuilder can prove was active at the failing step rather
  than reporting only the top-level clustering attempt.
- **Suspicious-sample visibility [KNOWN]:** Failure diagnostics must include a
  small repository-visible sample of suspicious clustering inputs tied to the
  embedding-health evidence so operators can inspect representative bad cases
  without dumping every embedding vector in the failed input set.
- **Dual-surface requirement [KNOWN]:** The required diagnostics must be emitted
  on the normal runtime log stream and written to a request-adjacent diagnostic
  artifact so failure analysis does not depend on retaining transient console
  output alone.
- **Failure-only scope [KNOWN]:** This requirement does not obligate
  LexonArchiveBuilder to emit the same detailed clustering-input inventory for
  successful clustering runs in this increment.
- **Top-level preservation [KNOWN]:** This requirement extends the current
  top-level clustering-attempt diagnostics rather than replacing them, so
  operators can correlate the full attempt with the narrower failing subset in
  the same failure record.
- **Raw-vector boundary [KNOWN]:** This requirement does not obligate
  LexonArchiveBuilder to log or persist the full raw embedding vector for every
  failed clustering input; compact summary evidence plus a small suspicious
  sample is sufficient in this increment.
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
- **Traceability:** UR-39, UR-41, UR-50, UR-72, UR-73, UR-74, UR-75, UR-76, UR-77, UR-78, UR-79

#### RQ-INDEXER-008D - Rooted block-tree quality assessment

LexonArchiveBuilder SHALL provide a rooted block-tree quality assessment tool
that reads a caller-selected root block from the configured block store and
reports structural-correctness, embedding-space quality findings, and rooted
TNN-recall diagnostics for the reachable tree.

- **Invocation scope [KNOWN]:** The assessment takes a configured block store and
  a caller-supplied root block identifier, then traverses the reachable block
  tree rooted at that block rather than depending on a repository-local summary
  manifest or out-of-band tree description.
- **Surface [KNOWN]:** In this increment, the assessment is a CLI-only operator
  tool and SHALL NOT require MCP exposure.
- **Structural-correctness findings [KNOWN]:** The assessment must identify and
  report hard structural violations such as any reachable child whose level is
  not lower than its parent.
- **Embedding-space heuristic findings [KNOWN]:** The assessment must report
  quantitative heuristics about how the tree partitions embedding space,
  including whether a child's centroid-distance spread is less than or equal to
  its parent's corresponding spread so child blocks represent the same or a
  smaller region than their parents, but this heuristic SHALL be reported as an
  aggregate inversion count rather than as emitted per-pair warning findings.
- **Quantification requirement [KNOWN]:** The assessment must emit quantitative
  per-block and aggregate evidence characterizing the size or shape of the
  embedding-space region represented by each block and by the rooted tree as a
  whole. That evidence SHALL include:
  - per-block mean distance from centroid
  - per-layer mean and standard deviation of intra-block dispersion
  - per-layer mean and standard deviation of sibling centroid-to-centroid distance
  - per-block first-principal-component variance fraction plus per-layer mean and standard deviation of that metric
  - per-block quantile-bin occupancy counts, occupancy variance, empty-bin detection, and detection of bins whose occupancy exceeds two times the expected value
  - per-parent split-effectiveness statistics covering the percentage of children whose dispersion exceeds the parent's plus the mean and maximum increase for those cases
- **Severity discipline [INFERRED]:** Structural-correctness violations and
  advisory embedding-space statistics SHALL be reported distinctly so callers
  can separate hard invariant failures from softer quality signals, and the
  parent-versus-child spread heuristic count SHALL remain advisory-only in this
  increment rather than producing per-block warning records.
- **Quantile-bin boundary [KNOWN]:** The number of quantile bins SHALL remain a
  repository-defined default in this increment rather than an operator-visible
  parameter.
- **Output requirement [KNOWN]:** The assessment must emit a human-readable
  summary and a machine-readable JSON artifact for the same analyzed rooted
  tree.
- **TNN-recall extensibility [KNOWN]:** The assessment must support rooted
  TNN-recall diagnostics over the embedding corpus reachable from the supplied
  root without redefining the repository's search-serving surfaces.
- **Embedding-readback boundary [KNOWN]:** When the assessment needs numerical
  embedding values from stored branch blocks, especially for evolving branch
  encodings such as EBCP, it SHALL obtain those values through the upstream
  LexonGraph embedding readback or reconstruction API rather than through a
  repository-local branch-decoder table keyed on embedding-encoding strings.
  Plain leaf payload decoding for the currently supported stable encodings
  remains unchanged in this increment.
- **Environment parity [INFERRED]:** The same assessment contract must remain
  usable against local/testing and preserved production-shaped block stores
  through the shared `BlockStore` boundary.
- **Content-type neutrality [INFERRED]:** The assessment must operate on stored
  block relationships and embeddings without assuming mailbox-only or
  document-only content semantics, so future content types remain representable.
- **Boundary [INFERRED]:** This requirement adds post-index assessment only; it
  does not redefine LexonGraph block validity semantics, change indexing-time
  construction behavior, or alter MCP search-serving behavior.
- **Traceability:** UR-80, UR-81, UR-82, UR-83, UR-84, UR-85, UR-86, UR-87, UR-101, UR-102, UR-111, UR-112

#### RQ-INDEXER-008D1 - Corpus-based rooted TNN-recall

LexonArchiveBuilder SHALL support True Nearest Neighbor Recall evaluation over
the embedding corpus reachable from the caller-supplied root block by sampling
query embeddings from that rooted corpus and computing Recall@1, Recall@5, and
Recall@10.

- **Aggregate-quality default [KNOWN]:** Corpus-based TNN-Recall is the default
  recall-evaluation mode and SHALL be the only mode used for automated or
  aggregate quality evaluation in this increment.
- **Rooted corpus scope [KNOWN]:** For this CLI tool, the evaluated corpus is
  the embedding set reachable from the supplied root rather than every
  embedding visible in the configured block store.
- **Sampling discipline [KNOWN]:** Sampled query embeddings SHALL be selected
  uniformly over the rooted embedding set.
- **Reproducibility [KNOWN]:** Corpus-based sampling SHALL be reproducible for a
  given seed.
- **Sample-size control [KNOWN]:** The number of sampled query embeddings SHALL
  be configurable.
- **Traversal-width control [KNOWN]:** The approximate-neighbor retrieval path
  used for corpus-based TNN-recall SHALL expose configurable traversal width so
  operators can measure recall at different widths.
- **Aggregate outputs [KNOWN]:** Mean recall, recall standard deviation, and
  recall histograms SHALL be computed from this corpus-based mode.
- **Metric family [KNOWN]:** The required recall outputs for this increment are
  Recall@1, Recall@5, and Recall@10.
- **Boundary [INFERRED]:** This requirement adds post-index rooted quality
  evidence only; it does not redefine LexonGraph search semantics or create an
  MCP-visible recall surface.
- **Traceability:** UR-101, UR-102, UR-103, UR-104, UR-105, UR-106, UR-112, UR-113

#### RQ-INDEXER-008D2 - User-query diagnostic recall

LexonArchiveBuilder MAY support TNN-Recall evaluation for one or more
user-supplied query embeddings as a diagnostic-only operator aid over the same
rooted tree.

- **Metric family [KNOWN]:** When this optional mode is supported, it SHALL
  compute Recall@1, Recall@5, and Recall@10 for each supplied query embedding.
- **Comparison evidence [KNOWN]:** The diagnostic output SHALL report the exact
  neighbors and the approximate neighbors for comparison.
- **Labeling [KNOWN]:** This output SHALL be labeled as `diagnostic recall`.
- **Non-aggregate boundary [KNOWN]:** User-query diagnostic recall SHALL NOT
  contribute to mean recall, recall standard deviation, recall histograms, or
  any other aggregate quality metric.
- **Debugging intent [KNOWN]:** This mode is a debugging aid only and does not
  redefine the statistical quality-evaluation contract for rooted trees.
- **Traceability:** UR-107, UR-108, UR-109, UR-110, UR-111

#### RQ-INDEXER-008D3 - TNN-recall mode separation

LexonArchiveBuilder SHALL clearly distinguish corpus-based rooted recall from
user-query diagnostic recall in both the human-readable summary and the
machine-readable report for the rooted quality tool.

- **Mode semantics [KNOWN]:** Corpus-based recall is the repository-owned
  statistical quality metric; user-query recall is a debugging aid.
- **Automated-evaluation boundary [KNOWN]:** Corpus-based recall SHALL remain
  the only mode used for automated quality evaluation.
- **Contract clarity [INFERRED]:** Reported recall artifacts must identify the
  query source so operators cannot mistake one-off diagnostic recall for
  aggregate rooted-corpus quality evidence.
- **Surface boundary [INFERRED]:** This distinction is local to the CLI quality
  tool and does not alter the existing MCP search contract or the separate
  rooted CLI text-search tool.
- **Traceability:** UR-102, UR-106, UR-107, UR-110, UR-111

#### RQ-INDEXER-008E - Rooted CLI search over stored trees

LexonArchiveBuilder SHALL provide a CLI-only operator tool that accepts a text
query, a caller-supplied embedding endpoint, a caller-supplied rooted tree, and
`k`, then generates a query embedding and searches the rooted stored tree
through `lexongraph-search` to return the top `k` matching leaf nodes.

- **Invocation scope [KNOWN]:** The search runs against a caller-supplied root
  block or rooted tree rather than against all searchable content visible in the
  configured block store.
- **Search boundary [KNOWN]:** The tool SHALL use the `lexongraph-search` API for
  the actual rooted search rather than introducing a repository-local search
  algorithm.
- **Embedding-readback boundary [KNOWN]:** Any repository-owned stored-embedding
  readback needed by this rooted-tree operator surface SHALL reuse the upstream
  LexonGraph embedding readback or reconstruction API rather than maintaining a
  separate repository-local decoder.
- **Embedding boundary [KNOWN]:** The tool SHALL accept a caller-supplied
  embedding endpoint for query embedding generation rather than requiring Rust
  code changes for each endpoint choice.
- **Result shape [KNOWN]:** The tool must return the top `k` matching leaf nodes
  for the rooted search invocation.
- **Output requirement [KNOWN]:** The tool must emit both human-readable results
  and machine-readable JSON output for the same invocation.
- **Surface boundary [INFERRED]:** The tool is additive to the existing MCP
  search capability and SHALL NOT replace or redefine the MCP search surface in
  this increment.
- **Storage boundary [INFERRED]:** The tool must reuse the configured block-store
  plus rooted-tree boundaries rather than inventing a second repository-local
  search corpus description.
- **Content-type neutrality [INFERRED]:** The tool must operate on searchable leaf
  nodes reachable from the rooted tree without assuming mailbox-only or
  document-only content semantics.
- **Traceability:** UR-88, UR-89, UR-90, UR-91, UR-92

### Boundary and Invariant Requirements

#### RQ-INDEXER-009 - Search-serving separation

The indexer requirements SHALL remain limited to indexing-time orchestration and adapter responsibilities and SHALL NOT redefine MCP search-serving behavior.

- **Rationale [INFERRED]:** Preserves the repository invariant that indexing remains separate from the MCP server surface.
- **Traceability:** UR-2, README.md

#### RQ-INDEXER-010A - Subordinate external contracts

LexonArchiveBuilder SHALL remain subordinate to the public contracts owned by `lexongraph-streaming-indexer`, `lexongraph-streaming-clustering`, `lexongraph-block-store`, and `lexongraph-embeddings-trait` and SHALL NOT redefine their index-construction, published-profile, replay-validation, block-identity, or embedding-contract semantics within this repository.

- **Rationale [KNOWN]:** Those semantics are already owned by the upstream LexonGraph crates and specifications.
- **Traceability:** UR-3, UR-8, UR-9, UR-44, UR-45, UR-48, UR-61, UR-62, UR-121, UR-122

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
- **Clustering-configuration implication [INFERRED]:** Published profile
  selection and its repository-approved defaults must remain part of the same
  stable batch-orchestration boundary across environments rather than creating
  a separate environment-specific clustering configuration model.
- **Content-type implication [KNOWN]:** The published-profile clustering contract
  must remain generic across current mailbox and document flows so future
  content types do not require a parallel clustering-control family.
- **Embedding-readback implication [KNOWN]:** Stored embedding reconstruction
  semantics and supported on-disk encodings must remain upstream-owned through
  LexonGraph APIs rather than being redefined independently by repository-owned
  quality, search, or diagnostic tools.
- **Traceability:** UR-3, UR-6, UR-7, UR-13, UR-19, UR-22, UR-42, UR-118, UR-119, UR-121, UR-123, UR-125, UR-149, UR-150, UR-151, UR-152

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
- Defining repository-local clustering profiles, clustering modes, clustering algorithms, or option semantics beyond the approved upstream published profile contract used in this increment
- Requiring detailed clustering-input inventories for successful clustering runs in this increment
- Requiring the block-tree quality assessment tool to expose an MCP-visible interface in this increment
- Reinterpreting advisory embedding-space quality heuristics as new LexonGraph-owned block-validity rules in this increment
- Allowing user-query diagnostic recall to contribute to automated or aggregate rooted-quality metrics
- Requiring the rooted CLI search tool to replace or redefine the existing MCP search surface in this increment
- Defining a repository-local search algorithm or a second repository-local search corpus model instead of using `lexongraph-search` over the approved rooted-tree boundary
- Defining or maintaining a repository-local branch-embedding decoding matrix for evolving branch encodings when the upstream LexonGraph embedding readback API already owns the supported branch reconstruction semantics

## Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | Requirements explicitly constrain scope to indexing-time orchestration and integrations |
| Environment-specific storage and embedding behavior stays behind stable interfaces | Preserved with revised storage contract | Stage selection, block-store iteration, clustering-status reporting, and operator-tool traversal now share the same two-mode local-versus-overlay block-store contract instead of splitting between local filesystem and plain Azure Blob targeting |
| Architecture remains extensible to future content types | Preserved | Collection-oriented input still covers both mailbox and document collections, and stage selection is defined in generic pipeline terms rather than mailbox-specific behavior |
| Idempotence and recoverability stay aligned with underlying immutable block semantics | Preserved with clarified scope | Requirements extend hash-addressed identity expectations to normalized email artifacts and require clustering-only reruns over the same clustering-eligible block-store snapshot to remain semantically stable under unchanged upstream semantics |
| Local development remains self-contained and batch-oriented | Preserved | Docker Compose is constrained to compose local dependencies around the batch container rather than changing the runtime model |
| Local published-profile evaluation remains outside production and serving contracts | Preserved with new local/testing aid | Requirements constrain the `0.6.x` profile sweep to repository-local operator automation that reuses existing batch and quality boundaries rather than adding a production entrypoint or MCP-visible test surface |
| Long-running batches remain observable without adding a control plane | Preserved with clarified scope | Progress reporting remains on the existing batch-runtime log surface and now explicitly includes the long-running embedding or leaf-materialization gap between mailbox expansion and downstream streaming-status visibility plus clustering-only replay submission progress, the handoff into upstream planning-pass waiting, and failure-only clustering diagnostics on the runtime log plus a request-adjacent artifact |
| Caller-visible indexing and MCP contracts remain stable across the upstream API migration | Preserved with approved contract change | The stage surface and MCP retrieval semantics remain stable while clustering-enabled indexing adopts a profile-version selector plus defaulted published-profile contract in place of the retired low-level planning controls |
| Clustering configuration remains explicit and replayable | Preserved with revised contract | Requirements now treat the selected published profile version as the replay-relevant clustering input rather than a repository-local mode, algorithm, and option tuple |
| Clustering-size behavior remains deterministic under the selected profile | Preserved with revised ownership | Requirements now assign clustering cardinality to whichever published profile version is selected rather than to repository-local auto-sizing or caller overrides |
| Clustering-only replay does not require whole-store rediscovery in the common case | Revised with LAB-owned recovery artifact | Requirements now permit a LAB-owned replay journal to replace common-case whole-store rescans while preserving block-store iteration as a compatibility fallback |
| Required repository capabilities remain distinguishable from upstream regressions during the latest upgrade | Preserved with clarified scope | The requirements now force the upgrade to classify missing capabilities explicitly instead of silently narrowing split-stage replay, published-profile adoption, progress projection, or MCP-facing behavior |
| Latest upstream telemetry remains subordinate to the existing runtime progress surface | Preserved with clarified scope | Requirements now constrain richer live telemetry and heartbeat events to the same repository-owned log stream rather than a new telemetry interface |
| Operator-visible progress counts remain understandable across upstream telemetry changes | Preserved with clarified scope | Requirements now distinguish invocation-total delegated-item counts from stage-local or layer-local telemetry counts so upstream count-shape changes do not create misleading logs |
| Post-index quality assessment remains subordinate to existing storage and serving boundaries | Preserved with clarified scope | The new assessment is constrained to a CLI-only operator tool that reads through the shared `BlockStore` boundary and does not alter MCP-facing behavior |
| Stored embedding format awareness remains upstream-owned | Preserved with revised ownership | Requirements now place supported stored-embedding encodings and reconstruction semantics behind the upstream LexonGraph readback API instead of a repository-local decoder table |
| Aggregate recall evaluation remains rooted-corpus-based and reproducible | Preserved with clarified scope | TNN-Recall is constrained to uniform seeded sampling over the rooted reachable embedding set for aggregate metrics, while user-query recall remains diagnostic-only |
| Operator CLI search remains additive to MCP search-serving behavior | Preserved with clarified scope | The new rooted CLI search tool is additive, uses the approved rooted-tree boundary plus `lexongraph-search`, and does not replace the MCP surface |
| Clients are not forced to parse raw mailbox blobs for ordinary retrieval | Preserved | Indexed chunks must reference normalized email artifacts so retrieval can stay at chunk level or expand to full normalized email through repository-owned artifacts |
| Storage abstraction count stays bounded across environments | Preserved | Requirements now reuse the environment-selected `BlockStore` abstraction family for indexed blocks, normalized email artifacts, and mailbox provenance artifacts rather than introducing a second storage stack |
| Local filesystem block stores remain interoperable with LexonGraph tooling | Preserved | The local/testing profile is now constrained to LexonGraph's filesystem naming/layout contract so inspection tools can consume repository-produced local stores |
| Parallel execution does not weaken deterministic indexing semantics | Preserved | Leaf-layer concurrency is constrained by cross-layer barriers and idempotence requirements so scheduling policy does not become a semantic contract change |

## Open Questions / Discovery Gaps

- **Q-INDEXER-061 [UNKNOWN]:** Should non-default published profile versions be treated as equally valid for production-shaped runs, or should this first selector increment primarily target local/testing experimentation while `0.1.0` remains the expected production default?
- **Q-INDEXER-062 [UNKNOWN]:** Does the latest upstream status-observer contract expose enough information for LexonArchiveBuilder to preserve its current replay-submission handoff and long-running liveness messages without weakening operator visibility?
- **Q-INDEXER-063 [UNKNOWN]:** Are any repository-required split-stage replay guarantees now expressed through different upstream lifecycle transitions beyond the observed rename from training completion to planning completion?
- **Q-INDEXER-064 [UNKNOWN]:** Does the newest upstream telemetry contract intend `item_count` to remain invocation-total for planning-pass events while hierarchy-planning and bottom-up assembly events report stage-local or layer-local progress units, or is that count shape still evolving?
- **Q-INDEXER-065 [UNKNOWN]:** After the approved intra-block dispersion, sibling-centroid separation, PCA-axis strength, quantile-occupancy, and parent-child dispersion-delta metrics land, which additional quantitative embedding-space shape measures would improve the rooted quality signal in a future increment without overfitting too early?
- **Q-INDEXER-066 [UNKNOWN]:** Future increments may revisit whether any rooted-quality heuristics beyond hard structural violations should influence process exit status, but this increment keeps the heuristic inversion count and layer statistics advisory-only.
- **Q-INDEXER-067 [UNKNOWN]:** Beyond the required query text, embedding endpoint, root, and `k`, does the rooted CLI search tool need repository-approved filters, score thresholds, or output-field selection in this increment?
- **Q-INDEXER-068 [UNKNOWN]:** Should the rooted CLI search tool treat the caller-supplied embedding endpoint as the complete query-embedding configuration, or must it also accept repository-specific embedding-spec inputs such as dimensions or encoding overrides at the CLI boundary?
- **Q-INDEXER-069 [UNKNOWN]:** For corpus-based TNN-recall histograms, should a future increment keep repository-owned default histogram buckets or expose bucket configuration as an operator-visible parameter?
- **Q-INDEXER-070 [UNKNOWN]:** Does published profile `0.1.0` preserve repository-acceptable tree-shape and retrieval-quality behavior across the expected corpus sizes, or will a later increment need additional approved published profiles for materially different workloads?
- **Q-INDEXER-071 [UNKNOWN]:** Should the first replay-journal implementation store only stable replay metadata plus persisted leaf identifiers, or should it also persist raw embedding payloads so clustering-only replay can avoid rereading leaf blocks entirely?
- **Q-INDEXER-072 [UNKNOWN]:** Should the first `0.6.x` evaluation sweep run only the `0.6.x` series, or should the runnable `test.ps1` preserve an in-band `0.5.x` comparison baseline in the same invocation?

## Coverage Notes

- **Covered sources [KNOWN]:**
  - user request in this session: "clean up the dead spec/code that is unrelated to the new profile version based path. It has left over stuff from the previous path where we tried to define it at this layer."
  - user request in this session: "the upstream LexonGraph API has evolved to allow either divisive or aggregation based clustering. We need to expose this as an option at this layer as well"
  - user clarification in this session: "I think it is important to both. Aggregate should be the default with an option to try out divisive (but I suspect that won't be interesting)"
  - user clarification in this session selecting: "Preserve existing MCP/search behavior exactly (Recommended)"
  - user clarification in this session selecting: "Uniform content-type-agnostic control (Recommended)"
  - user clarification in this session selecting: "Yes, keep the same contract and default across environments (Recommended)"
  - user clarification in this session selecting: "Yes, that is the acceptance target (Recommended)"
  - user request in this session: "another requirement: Add an option to allow the user to provide a text string and an embedding endpoint, then generate an embedding, search using the lexongraph-search api, and return the top k matching leaf nodes. The MCP server already does something similar, but I want an easy cli tool to do it as well"
  - user clarification in this session selecting: "Caller-supplied root/tree (Recommended)"
  - user clarification in this session selecting: "Human-readable results plus machine-readable JSON output (Recommended)"
  - user request in this session: "We need a tool that given a block store and root and measure the quality / correctness of the block tree. This would include heuristics like children always have lower level than parents. Distance from centroid of embeddings in parent is the same or bigger than distance from centroid if embeddings in child (i.e. children span a smaller part of the embedding space than their parents). It would also be useful to gain a quantifiable measure of the quality of how the space is divided up (i.e. the shape that each block represents in teh embedding space)."
  - user clarification in this session selecting: "CLI-only operator tool (Recommended)"
  - user clarification in this session selecting: "Human-readable summary plus machine-readable JSON artifact (Recommended)"
  - user request in this session: "yes, that warning is probably a false positive. Report as a count, but don't issue warnings. Can we instead compute mean distance from centroid for each block, then compute mean by layer and stdev by layer? i.e. a rough statistical measure of the where the blocks fit within the embedding space?"
  - user request in this session: "I think we need to refine what we are measuring as quality. It should include tree consistency (like we already have) but also:
1. Intra‑Block Dispersion Statistics (Per Layer)
The system SHALL compute, for every layer of the tree:  
- the mean intra‑block dispersion (mean distance of embeddings to their block centroid), and  
- the standard deviation of dispersion across all blocks in that layer.  
 
These values SHALL be used to assess block cohesion and detect under‑ or over‑splitting.
 
---
 
2. Inter‑Centroid Distance Statistics (Per Layer)
The system SHALL compute, for every layer:  
- the mean centroid‑to‑centroid distance between sibling blocks, and  
- the standard deviation of these distances.  
 
These values SHALL be used to assess block separation and detect overlapping or poorly differentiated clusters.
 
---
 
3. PCA Axis Strength (Per Layer)
For each block at each layer, the system SHALL compute:  
- the fraction of total variance explained by the first principal component (λ₁ / Σλᵢ).  
 
This metric SHALL be aggregated per layer (mean and stdev) and SHALL be used to detect weak or degenerate PCA axes.
 
---
 
4. Quantile Bin Occupancy Variance (Per Layer)
For each block, the system SHALL measure:  
- the occupancy count of each quantile bin, and  
- the variance of these occupancies.  
 
The system SHALL detect and record:  
- empty bins, and  
- bins with occupancy greater than 2× the expected value.  
 
This metric SHALL be used to detect quantile failures and misaligned PCA axes.
 
---
 
5. Parent‑to‑Child Dispersion Delta (Per Split)
For every parent block and its children, the system SHALL compute:  
- the percentage of children whose dispersion exceeds that of the parent,  
- the mean increase in dispersion for such cases, and  
- the maximum observed increase.  
 
This metric SHALL be used to detect multimodal blocks and ineffective splits."
  - user clarification in this session selecting: "Repository-defined default (Recommended)"
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
  - user request in this session: "adding a new diagnostic TNN-Recall (1, 5 and 10 key versions). TNN‑Recall Query Source Requirements 1. Corpus‑Based Evaluation (Required) The system SHALL support True Nearest Neighbor Recall evaluation using randomly sampled embeddings from the corpus. This mode SHALL be the default and SHALL be used for all aggregate recall metrics. - Sampling MUST be uniform over the embedding set. - Sampling MUST be reproducible given a seed. - Sample size MUST be configurable. - This mode SHALL be used for Mean Recall, StdDev Recall, and Recall Histograms. 2. User‑Query Evaluation (Optional) The system MAY support TNN‑Recall evaluation using user‑supplied query embeddings. This mode SHALL be treated as a diagnostic tool only and SHALL NOT contribute to aggregate recall metrics. - The system SHALL compute Recall@k for the user query. - The system SHALL report the exact neighbors and approximate neighbors for comparison. - The system SHALL label this result as “diagnostic recall.” 3. Separation of Modes The system SHALL clearly distinguish between: - Corpus‑based recall (statistical quality metric) - User‑query recall (debugging aid) Corpus‑based recall SHALL be the only mode used for automated quality evaluation."
  - user clarification in this session selecting: "Reachable embeddings under the supplied root (Recommended)"
  - user request in this session: "LexonGraph crate has been updated and now has a simpler higher level API that groups options into a versioned profile. Please switch to this API and use the v0.1.0 profile"
  - user clarification in this session selecting: "Replace the external control surface with profile-based v0.1.0 (Recommended)"
  - user request in this session: "LexonGraph has switched to exposing a versioned indexing profile. Currently we hard-code to v0.1.0 (I think). Make this an option we can test different profiles. Can we also pin to main of LexonGraph for now with an explicit note that this is so we can quickly test new profiles?"
  - user request in this session: "LexonGraph now has a 0.5.x series of profiles to test. Update to allow us to test this and create/update a test.ps1 I can run them"
  - user request in this session: "update lab to use the new api for reading back embeddings rather then decoding them in lab"
  - `test.ps1:1-90`
  - `crates/lexonarchivebuilder-indexer/src/config.rs:45-60`
  - `crates/lexonarchivebuilder-indexer/src/main.rs:287-320`
  - `Cargo.toml:29-37`
  - `crates/lexonarchivebuilder-indexer/src/quality.rs:1105-1179`
  - `crates/lexonarchivebuilder-indexer/src/runtime.rs:720-741`
  - `crates/lexonarchivebuilder-indexer/src/tree_tools.rs:81-114`
  - `docs/specs/lexonarchivebuilder-indexer/design.md:228-315`
  - `docs/specs/lexonarchivebuilder-indexer/validation.md:72-187`
  - `crates/lexonarchivebuilder-indexer/src/runtime.rs:14-340`
  - `Alan-Jowett/LexonGraph` `crates/lexongraph-streaming-indexer/src/lib.rs` on `main`: published-profile surfaces around `PublishedProfileVersion`, `PUBLISHED_PROFILE_V0_1_0`, `PublishedIndexingProfile`, and `with_published_profile`
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
  - The exact field names, serialization shape, and any future operator-visible selector shape for the published-profile contract, because those choices belong to downstream design and validation artifacts so long as they preserve the approved default `0.1.0` behavior and explicit version-selection boundary
  - The exact formulas, thresholds, weighting model, and exit-code policy for quantitative block-tree quality heuristics, because those choices belong to downstream design and validation artifacts so long as the approved distinction between hard structural findings and advisory quality evidence is preserved
  - The exact rooted CLI search flag names, result-field schema, score formatting, and default artifact location, because those choices belong to downstream design and validation artifacts so long as the approved rooted search scope, `lexongraph-search` boundary, and dual-output contract are preserved
