<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# Indexer Requirements

## Document Status

- **Phase:** Phase 1 - Requirements Discovery
- **Status:** Approved streaming-indexer migration baseline with incremental requirements patches for LexonGraph published-profile API adoption, published-profile version selection, latest telemetry compatibility, upstream regression assessment, clustering-failure diagnostics, rooted block-tree quality assessment discovery plus quality-metric refinement, rooted TNN-recall diagnostics, rooted query access-cost reporting, rooted CLI search discovery, upstream main-tracking for rapid profile validation, upstream wgpu-acceleration revision compatibility, 0.6.x published-profile evaluation, local testing sweep automation, v0.7.0 fixed-budget ladder experiment automation, upstream embedding-readback API adoption, immutable block-backed replay-audit journaling, mutable current-root publication, rooted block-store copy tooling, bounded-residency deterministic replay ordering, replay-order preparation efficiency, v2 custom-block adoption for repository-owned non-search artifacts, conditional streaming-indexer v3 adoption with repository-default published profile `0.7.0`, pass-level convergence telemetry with explicit contract/profile identity logging, v3-compatible clustering observability projection, user-usable convergence-diagnosis surfacing, latest-LexonGraph constrained v3 working-root adoption at commit `7c8f375137375709bb608ee2609b38cb80e5422c`, issue-83 replay-order memory decoupling from corpus size, bounded replay-batch preparation overlap exploration for issue #88, issue #93 replay batch-size decoupling from CPU concurrency, issue #95 bounded multi-batch replay-prefetch buffering, and renewed latest-upstream-main compatibility after post-`7c8f375137375709bb608ee2609b38cb80e5422c` breaking LexonGraph changes
- **Scope:** LexonArchiveBuilder indexer integration boundary plus incremental email-artifact, chunk-indexing, local block-store interoperability, replay-based streaming delegated indexing, stage-selectable execution, standalone clustering input discovery, LAB-owned immutable replay-audit journaling for split-stage recovery, repository-owned mutable current-root publication, published-profile-based clustering configuration with caller-selectable profile versions, latest published-profile and telemetry compatibility, upstream regression assessment, embedding-phase, replay-submission and streaming-status observability, pass-level convergence telemetry, v3-compatible clustering telemetry projection, user-usable convergence diagnosis for clustering-enabled runs, contract/profile identity logging for clustering-enabled runs, clustering-failure diagnosability, rooted block-tree quality assessment with refined per-layer quality metrics, rooted TNN-recall diagnostics, rooted query access-cost reporting, rooted CLI search over stored trees, rooted block-store copy between approved storage targets, bounded-residency deterministic replay ordering for deterministic replay submission, efficient replay-order preparation behind the existing replay contract, bounded replay-batch preparation overlap behind the existing replay contract, independent replay batch-sizing versus replay-materialization concurrency control for clustering replay, bounded multi-batch replay-prefetch buffering for clustering replay, temporary upstream main-tracking for rapid profile validation, upstream wgpu-acceleration revision compatibility, 0.6.x published-profile evaluation through repository-local testing automation, v0.7.0 fixed-budget ladder experiments through repository-local testing automation, upstream-owned embedding readback for stored-tree consumers, layer-parallel block-construction evolution, v2 custom-block adoption for repository-owned non-search artifacts, conditional use of the upstream streaming-indexer v3 API when the selected published profile is `0.7.0`, upstream-managed request-adjacent v3 working-root derivation for clustering-enabled v3 execution, and repeatable adaptation to later upstream-main breaking changes without weakening the current external stage or observability contracts

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
- **UR-153 [KNOWN]:** All indexer tools that read from or write to the configured block store must allow operators to target one shared approved profile set: a local filesystem block store, the existing production overlay block store composed of an in-memory cache plus a local filesystem cache plus Azure Blob SAS-backed storage, or a new `production-v2` direct Azure-backed store profile.
- **UR-154 [KNOWN]:** Direct non-local operator targeting remains an approved mode only when it is expressed through one of the repository-defined production profiles; an ad hoc plain Azure Blob backend outside the existing `production` overlay profile or the new `production-v2` profile is not an approved indexer tool-targeting mode.
- **UR-155 [INFERRED]:** The same block-store targeting contract should apply consistently across batch indexing, standalone clustering discovery, rooted quality assessment, rooted CLI search, and any other indexer-owned operator tool that traverses the shared `BlockStore` boundary.
- **UR-156 [INFERRED]:** The new overlay-capable targeting contract must remain content-type-neutral and preserve the existing shared `BlockStore` abstraction family for indexed blocks, normalized email artifacts, and mailbox provenance artifacts.
- **UR-157 [KNOWN]:** LexonGraph now has a v2 block format with custom-block support, and LexonArchiveBuilder should switch its repository-owned non-search artifact blocks from v1-style wrappers to v2 custom blocks.
- **UR-158 [KNOWN]:** It is acceptable for this artifact-block transition to require rebuilt local or production-oriented stores for those repository-owned non-search artifacts; continued read compatibility with pre-v2 v1 artifact blocks is not required in this increment.
- **UR-159 [INFERRED]:** This increment should not introduce a repository-owned branch-or-leaf translation layer for delegated index blocks while the upstream streaming indexer and search flow still own those contracts.
- **UR-160 [KNOWN]:** The current append-only file replay journal no longer matches the repository's immutable block-oriented storage model, so the replay journal should evolve into a full audit journal persisted as immutable blocks.
- **UR-161 [KNOWN]:** After the indexer completes a bounded chunk of embedding or indexing work, it should emit a new immutable journal block that links to the prior journal block by hash so the audit history becomes a hash-addressed Merkle-linked journal.
- **UR-162 [KNOWN]:** This immutable replay-audit journal should live on the same shared `BlockStore` boundary in both local/testing and production-oriented environments rather than remaining local-filesystem-only.
- **UR-163 [KNOWN]:** The immutable replay-audit journal should become the sole authoritative source for replay and resume state; this increment should remove whole-store scanning as a replay-discovery fallback.
- **UR-164 [KNOWN]:** Journal emission should balance write amplification against redo cost by batching multiple completed work entries into one immutable journal block, publishing a new block when the active journal payload crosses an approved size-oriented threshold rather than on every atomic operation.
- **UR-165 [KNOWN]:** The current journal head should be published through the same class of repository-owned mutable reference mechanism used for current-root discovery so later invocations can find the latest immutable journal block without scanning.
- **UR-166 [INFERRED]:** The replay-audit journal should document repository-owned embedding and indexing progress at a content-type-agnostic orchestration boundary without redefining LexonGraph-owned block identity, embedding semantics, or MCP-visible search behavior.
- **UR-167 [KNOWN]:** Each replay-audit entry should be detailed enough to audit what work was performed, including the relevant input identities, the action or step kind, and the generated block identities or equivalent durable outputs.
- **UR-168 [KNOWN]:** When indexing produces a new final root block, LexonArchiveBuilder should publish that current root through a repository-owned mutable reference mechanism so later invocations and operators can discover the latest root without depending on request-local output capture.
- **UR-169 [KNOWN]:** The rooted quality tool should report statistics on blocks touched, broken down by block level and as overall totals, for the query workload it executes.
- **UR-170 [KNOWN]:** For each query executed by the rooted quality tool, the report should include the number of blocks touched and the total size of blocks read.
- **UR-171 [KNOWN]:** Per-query rooted-quality access reporting should break those block-touch and byte-read figures down per layer and summarize them into overall per-query totals.
- **UR-172 [KNOWN]:** The rooted quality tool should report an estimated query time in RTT units assuming a congestion window of 64 KiB.
- **UR-173 [KNOWN]:** The estimated RTT cost for a query should be computed by dividing each layer's bytes read by the 64 KiB congestion window, rounding each layer up to the next whole RTT, and summing those per-layer RTT counts into a total for the query.
- **UR-174 [KNOWN]:** Build a repository-local test script and execution plan for a published-profile `0.7.0` ladder experiment that operators can execute without redesigning the existing local/testing workflow.
- **UR-175 [KNOWN]:** The `0.7.0` ladder should vary beam width and clustering cardinality together while keeping `beam_width * cluster_count` constant across all rungs.
- **UR-176 [KNOWN]:** The first runnable ladder should default to a fixed budget of `1024`, anchored on the prior successful baseline of traversal width `16` and clustering cardinality `64`.
- **UR-177 [KNOWN]:** The first runnable ladder should default to the five-rung set `4x256`, `8x128`, `16x64`, `32x32`, and `64x16`.
- **UR-178 [KNOWN]:** The ladder automation should emit per-rung run artifacts, per-rung rooted-quality artifacts, a comparable summary table, and an operator-facing execution plan that includes preflight validation and rung ordering.
- **UR-179 [INFERRED]:** Because the current published-profile run surface treats clustering cardinality as profile-owned, the ladder may require a scoped repository-local local/testing mechanism to select rung-specific clustering cardinality for experiment execution without redefining the production or MCP-facing clustering contract.
- **UR-180 [KNOWN]:** LexonArchiveBuilder needs a tool that copies immutable blocks between two configured block stores, starting with the operator need to copy from a local filesystem-backed store into the Azure SDK-backed overlay block-store target.
- **UR-181 [KNOWN]:** In this increment, the block-copy capability should be a CLI-only operator tool rather than an MCP-visible surface or a normal indexing-stage behavior.
- **UR-182 [KNOWN]:** The first block-copy tool should copy only caller-selected root blocks and the immutable blocks reachable from those roots, rather than performing whole-store replication by default.
- **UR-183 [INFERRED]:** The block-copy tool should reuse the same approved block-store target modes on both its source and destination sides, so operators can target local filesystem stores and overlay-backed Azure-oriented stores without inventing a separate storage contract for copying.
- **UR-184 [INFERRED]:** Because blocks are immutable and hash-addressed, rerunning a copy into a destination that already contains some or all requested block identities should be safe and should not require repository-local block translation or content reinterpretation.
- **UR-185 [INFERRED]:** The block-copy tool should traverse rooted block graphs and persist block bytes through the shared `BlockStore` abstraction boundary rather than re-encoding block payloads or depending on storage-backend-specific side channels.
- **UR-186 [INFERRED]:** The block-copy tool should emit an operator-readable summary plus a machine-readable artifact describing requested roots, copied block counts, skipped-already-present counts, and any copy failures.
- **UR-187 [INFERRED]:** This increment should copy immutable block content only; repository-owned mutable references such as current-root or replay-journal-head publication remain separate contracts unless a later increment explicitly expands the scope.
- **UR-188 [KNOWN]:** LexonGraph already provides the relevant block-store implementations; this increment should add a repository-owned copy tool layer on top of those existing `BlockStore` implementations rather than introducing a new block-store backend family.
- **UR-189 [KNOWN]:** The repository should add a `production-v2` block-store profile that keeps the current production-oriented targeting shape but uses the alternate Azure-backed LexonGraph block-store implementation rather than the existing Azure Blob-backed overlay path.
- **UR-190 [INFERRED]:** The new `production-v2` profile should be exposed consistently across indexer-owned block-store-consuming tools rather than being introduced for the rooted copy command alone, so repository operator workflows keep one shared storage-profile vocabulary.
- **UR-191 [INFERRED]:** The existing `production` overlay profile remains an approved mode; `production-v2` is additive and should preserve the current CLI-only copy scope, MCP separation, and content-type-neutral `BlockStore` abstraction boundary.
- **UR-192 [KNOWN]:** The rooted block-copy CLI should emit default user-visible liveness or progress output during long-running copy operations so operators can distinguish active traversal or transfer work from a hung invocation.
- **UR-193 [KNOWN]:** Basic rooted-copy liveness should be available without an opt-in verbosity flag; any future verbose mode may add detail, but the default CLI contract must already show that work is continuing.
- **UR-194 [KNOWN]:** LexonArchiveBuilder operators need a supported way to turn on underlying Azure SDK and HTTP-client diagnostic logging when investigating storage or transport hangs in indexer-owned commands.
- **UR-195 [KNOWN]:** That diagnostic logging should be enabled for the entire `lexonarchivebuilder-indexer` binary through the standard `RUST_LOG` environment-variable path rather than through a new repository-specific CLI flag, and it should remain opt-in rather than becoming default operator noise.
- **UR-196 [KNOWN]:** Rooted block copy needs an opt-in mode that skips destination existence reads and instead attempts destination writes directly, so operators can avoid backends where read-before-write presence checks hang or are disproportionately expensive.
- **UR-197 [KNOWN]:** The current read-before-write rooted copy behavior should remain the default; the new blind-write behavior is an explicit operator-selected tradeoff that accepts reduced copy-versus-skip accounting in exchange for avoiding destination reads.
- **UR-198 [KNOWN]:** Rooted block copy should pipeline destination writes asynchronously with multiple writes in flight so high-latency backends such as Azure do not serialize the entire copy through one write at a time.
- **UR-199 [KNOWN]:** The bounded in-flight destination-write limit should be an operator-selectable CLI control rather than a fixed internal constant, and the first increment should default that limit to `64`.
- **UR-200 [KNOWN]:** The bounded asynchronous destination-write policy should apply both to the default read-before-write mode when a block has been classified as missing and to the opt-in blind-write mode.
- **UR-201 [INFERRED]:** Introducing bounded concurrent destination writes must preserve rooted reachability, immutable block identity, and truthful mode-specific reporting semantics even when destination write completions arrive out of traversal order.
- **UR-202 [KNOWN]:** Add a separate repository-owned
  `lexonarchivebuilder-block-store-http3` crate and an approved gateway-backed
  read-only block-store profile that fetches immutable blocks over HTTP/3 QUIC.
- **UR-203 [KNOWN]:** The new gateway-backed profile should accept a gateway DNS
  host name as its operator-facing network parameter and derive HTTPS over QUIC
  on port `443` from that host name.
- **UR-204 [KNOWN]:** This increment should wire the gateway-backed profile into
  the approved indexer block-store profile vocabulary now rather than leaving it
  as an unintegrated standalone crate.
- **UR-205 [KNOWN]:** The gateway-backed profile should be read-only and limited
  to block-fetching tool surfaces in this increment.
- **UR-206 [KNOWN]:** Unsupported `BlockStore` operations such as writes and
  whole-store iteration should fail explicitly for the gateway-backed profile.
- **UR-207 [KNOWN]:** The gateway-backed profile should map HTTP `404` to a
  missing-block result and treat transport, protocol, or other non-success
  responses as explicit backend failures.
- **UR-208 [KNOWN]:** Create a dedicated spec package for
  `lexonarchivebuilder-block-store-http3` plus the necessary cross-references in
  the indexer requirements.
- **UR-209 [INFERRED]:** Because the gateway-backed profile is read-only,
  write-bearing indexing, publication, and copy-destination flows must remain on
  the existing writable profiles rather than silently degrading behavior.
- **UR-210 [KNOWN]:** LexonArchiveBuilder should continue to index corpora whose
  total logical data size exceeds available system memory.
- **UR-211 [KNOWN]:** Both full-pipeline execution and clustering-only replay
  execution should stay within a bounded-memory orchestration contract rather
  than retaining replay inputs, embeddings, or content-expansion state in
  proportion to total corpus size.
- **UR-212 [KNOWN]:** LexonArchiveBuilder should strongly prefer a non-spilling
  realization of that bounded-memory contract; spilling replay inputs,
  embeddings, or equivalent repository-owned staging artifacts to local storage
  or `BlockStore` is acceptable only if downstream design can justify that a
  purely streaming realization is not feasible under the approved upstream
  lifecycle contract.
- **UR-213 [INFERRED]:** Adding bounded-memory behavior must preserve the existing
  stage contract, deterministic replay semantics, immutable-block idempotence,
  and unchanged MCP search-serving behavior for already-indexed content.
- **UR-214 [INFERRED]:** The bounded-memory contract should remain content-type-
  neutral and environment-neutral so mailbox, document, and future content
  sources participate through the same bounded-memory orchestration boundary in
  local/testing and production-oriented profiles.
- **UR-215 [KNOWN]:** The replay-journal-driven ordering path should walk the
  replay list, gather the referenced block ids, sort that result, dedupe it,
  and use the resulting unique block-id order as the deterministic order for
  classification and finalization.
- **UR-216 [KNOWN]:** Actual block state such as embeddings or decoded block
  content should be pulled from the shared `BlockStore` on demand while the
  ordered block-id list is processed rather than being cached as part of
  replay-order preparation.
- **UR-217 [KNOWN]:** SQLite, spill files, and other repository-owned external
  storage are not required for this ordering path; the approved fix for this
  increment is the simplest in-memory raw block-id list that satisfies the
  deterministic replay contract.
- **UR-218 [INFERRED]:** This simplified ordering path must preserve the
  existing stage contract, deterministic replay semantics, and content-type-
  neutral behavior because only hash-addressed block identities become the
  retained ordering state.
- **UR-219 [KNOWN]:** Generating the ordered replay list should read the replay-
  audit journal blocks and their recorded block ids only; it should not fetch
  the referenced payload blocks until classification or finalization actually
  processes those ids.
- **UR-220 [KNOWN]:** LexonGraph now exposes an additive streaming-indexer v2
  API surface specialized to published profile `0.7.0`, and
  LexonArchiveBuilder should prepare to use that v2 surface for clustering-
  enabled indexing when `0.7.0` is the selected profile version.
- **UR-221 [KNOWN]:** Published profile version `0.7.0` should become the
  repository-default clustering behavior for this increment.
- **UR-222 [KNOWN]:** The existing published-profile selector surface on the
  batch request and CLI entrypoints should remain exposed so callers may still
  select a non-default profile version explicitly.
- **UR-223 [KNOWN]:** LexonArchiveBuilder should route clustering-enabled work
  through the upstream streaming-indexer v2 API only when the effective
  selected published profile version is `0.7.0`.
- **UR-224 [INFERRED]:** When callers explicitly select a supported non-`0.7.0`
  profile version, LexonArchiveBuilder should preserve the existing non-v2
  upstream integration path for that run rather than silently coercing the
  selection back to `0.7.0`.
- **UR-225 [INFERRED]:** This conditional v2 migration must preserve the
  existing execution-stage contract, deterministic replay semantics, retired
  low-level clustering controls, and unchanged MCP search or retrieval
  behavior for already-indexed content.
- **UR-226 [KNOWN]:** The upstream streaming-indexer v2 lifecycle may require
  more than one full planning replay pass before planning becomes complete on
  large corpora.
- **UR-227 [KNOWN]:** When effective profile `0.7.0` routes execution through
  the upstream streaming-indexer v2 surface, LexonArchiveBuilder should keep
  replaying full planning passes until planning completion succeeds or an
  upstream/runtime error occurs, rather than assuming one completed pass is
  always sufficient.
- **UR-228 [KNOWN]:** Add operator-visible per-pass telemetry so long-running
  clustering-enabled runs make it possible to tell whether planning is
  converging after `N` passes or simply repeating the same incomplete state.
- **UR-229 [KNOWN]:** Prefer writing per-pass convergence telemetry to a
  separate file or otherwise separate output stream so operators can find pass
  summaries without scanning the entire ordinary progress log.
- **UR-230 [KNOWN]:** The telemetry for clustering-enabled runs should clearly
  identify which delegated indexing contract is active for the run, including
  whether LexonArchiveBuilder is using the legacy/non-v2 path or the upstream
  streaming-indexer v2 path.
- **UR-231 [KNOWN]:** The telemetry for clustering-enabled runs should clearly
  identify the effective selected published profile version `0.x.y` after the
  repository's existing CLI/request/default precedence is applied.
- **UR-232 [KNOWN]:** Pass-end telemetry should surface enough upstream-returned
  planning data to let operators judge convergence, including per-pass topology
  or planning summaries when that data is exposed by the delegated API.
- **UR-233 [INFERRED]:** This convergence telemetry must remain additive to the
  existing batch-runtime logging surface and must not change the approved batch
  contract, stage contract, or MCP search-serving behavior.
- **UR-234 [KNOWN]:** LexonGraph `main` now exposes richer v2 intra-pass planning
  observability, and LexonArchiveBuilder should refresh its adopted upstream
  dependency state to consume that newer observer surface.
- **UR-235 [KNOWN]:** For clustering-enabled runs routed through the upstream
  streaming-indexer v2 surface, LexonArchiveBuilder should project the new
  intra-pass planning telemetry onto operator-visible output, including pass
  progress, pending partition detail, trainer subphase summaries, and
  suspected-stall indicators when the delegated observer exposes them.
- **UR-236 [INFERRED]:** This intra-pass observability should complement, not
  replace, the existing pass-end convergence summaries so operators can see
  both within-pass activity and end-of-pass convergence results for the same
  run.
- **UR-237 [INFERRED]:** When LexonArchiveBuilder writes operator-discoverable
  request-adjacent planning telemetry artifacts, those artifacts should remain
  per-run and should carry intra-pass records as well as pass-end summaries
  whenever the delegated v2 observer exposes enough data to do so without
  inventing repository-local progress semantics.
- **UR-238 [INFERRED]:** Consuming the richer v2 observer surface must preserve
  the approved batch contract, execution-stage contract, replay determinism,
  and unchanged MCP search or retrieval behavior for already-indexed content.
- **UR-239 [KNOWN]:** LexonGraph now exposes a much richer telemetry set whose
  purpose is to help the caller determine whether clustering planning is
  converging and, if not, what the run is blocked on.
- **UR-240 [KNOWN]:** LexonArchiveBuilder should surface that richer delegated
  telemetry in a form a user can actually use to diagnose convergence
  failures.
- **UR-241 [INFERRED]:** User-usable convergence diagnosis should combine
  completed-pass convergence evidence with the latest available within-pass
  blocked-on evidence rather than forcing operators to reconstruct state from
  separate raw telemetry events manually.
- **UR-242 [INFERRED]:** When delegated telemetry does not justify a stronger
  convergence or blocked-on conclusion, LexonArchiveBuilder should report that
  uncertainty explicitly rather than inventing a repository-owned diagnosis.
- **UR-243 [INFERRED]:** This richer convergence-diagnosis surfacing must remain
  additive to the existing runtime progress and request-adjacent planning
  telemetry artifact surfaces and must not introduce a new control plane or
  MCP-visible diagnostics surface.
- **UR-244 [KNOWN]:** Update LexonArchiveBuilder to the latest LexonGraph
  revision represented by commit `858ed455ea0828909aea38a0f2e677cca917ae76`,
  where the upstream streaming-indexer v2 path now supports planner-managed
  out-of-core planning state.
- **UR-245 [KNOWN]:** When LexonArchiveBuilder routes clustering-enabled
  execution through the upstream streaming-indexer v2 surface, it should
  provide a writable planner-state root for that delegated run so the upstream
  planner can externalize planning data beneath that root.
- **UR-246 [KNOWN]:** LexonArchiveBuilder should adopt the upstream bounded-
  residency out-of-core planning behavior for v2 planning data instead of
  treating all dataset-scale planning state as resident-memory-only.
- **UR-247 [INFERRED]:** This planner-state-root adoption should remain
  subordinate to the upstream contract: LexonArchiveBuilder should treat the
  concrete file naming, layout, spill format, and residency mechanics beneath
  that root as upstream-owned opaque state rather than redefining them
  repository-locally.
- **UR-248 [INFERRED]:** The approved replay-ordering simplification remains in
  force for repository-owned deterministic replay orchestration, so adopting
  planner-managed out-of-core planning spill must not be interpreted as
  permission to reintroduce repository-owned spill or SQLite state for replay
  ordering, content expansion, or MCP-serving behavior.
- **UR-249 [INFERRED]:** If the delegated v2 run cannot establish or use the
  required planner-state root, LexonArchiveBuilder should fail explicitly
  rather than silently falling back to unbounded resident planning state,
  silently switching contract families, or claiming successful clustering.
- **UR-250 [KNOWN]:** LexonArchiveBuilder should not add a new caller-visible
  planner-state-root selector for this increment; instead, it should derive the
  delegated v2 planner-state root automatically from the existing request-
  adjacent artifact/output locations.
- **UR-251 [KNOWN]:** Fix issue #83: the first observed post-planner-spill memory
  hotspot is repository-owned replay-order preparation, whose memory use should
  no longer scale with corpus size before delegated planning begins.
- **UR-252 [KNOWN]:** The acceptance target for fixing issue #83 is that memory
  usage should not scale with size of corpora.
- **UR-253 [KNOWN]:** For fixing issue #83, repository-owned replay ordering may
  use bounded externalized state when needed to keep replay-order preparation
  memory independent of corpus size.
- **UR-254 [INFERRED]:** Any issue-83 fix must preserve deterministic replay
  ordering, the existing stage contract, and unchanged MCP search or retrieval
  behavior for already-indexed content.
- **UR-255 [INFERRED]:** Any repository-owned externalized replay-order state
  approved for issue #83 must remain semantically separate from the delegated
  planner-state root and from upstream-owned planner spill internals.
- **UR-256 [KNOWN]:** Issue #88 asks LexonArchiveBuilder to explore whether
  clustering-only replay can overlap bounded repository-owned replay-batch
  preparation with delegated training or ingestion work to hide storage and
  syscall latency.
- **UR-257 [KNOWN]:** Any such overlap must remain subordinate to the upstream
  `StreamingIndexingRunV2` lifecycle and must not require concurrent
  `ingest_batch`, `finish_pass`, `mark_planning_complete`, or `finalize`
  operations on the same delegated run.
- **UR-258 [INFERRED]:** If LexonArchiveBuilder prepares replay batch `N+1`
  while batch `N` is still being ingested upstream, the prepared-next-batch
  content and embedding state must remain isolated until batch `N` completes so
  the current delegated batch never observes replaced replay state.
- **UR-259 [INFERRED]:** Replay-preparation overlap must preserve deterministic
  replay order, replay-validation identity, stable fingerprints, and bounded
  repository-owned memory independently of corpus size.
- **UR-260 [KNOWN]:** The first approved increment for this idea should remain
  an internal runtime optimization and should not add a new caller-visible
  replay-prefetch selector unless the existing upstream/local contract proves
  insufficient.
- **UR-261 [INFERRED]:** Any approved overlap strategy must remain
  content-type-neutral and environment-neutral so future content types and both
  local/testing and production-oriented `BlockStore` profiles participate
  through the same replay boundary.
- **UR-262 [KNOWN]:** Speed up the replay step, specifically the repository-owned
  replay-order preparation and deduplication path that runs before delegated
  clustering replay.
- **UR-263 [KNOWN]:** Improve utilization of available host CPU and disk during
  replay-order preparation when the replay journal is large, rather than leaving
  replay prep substantially underutilized while preserving the current stage
  contract.
- **UR-264 [INFERRED]:** Any replay-step speedup must preserve the existing
  deterministic unique replay order, replay-validation integrity checks, and
  unchanged clustering/finalization behavior for already-indexed content.
- **UR-265 [INFERRED]:** This increment should remain internal to repository-
  owned replay orchestration and should not add a new caller-visible tuning
  selector unless an existing contract proves insufficient.
- **UR-266 [INFERRED]:** Replay-step speedups must preserve the current bounded-
  memory and payload-free replay-ordering boundary: optimization may improve
  preparation concurrency or reduce per-record overhead, but must not turn
  replay-order preparation into a payload cache or require corpus-scale resident
  state.
- **UR-267 [INFERRED]:** Replay-step speedups must remain compatible with both
  local/testing and production-oriented storage profiles through the same
  replay-journal and block-store abstractions rather than introducing
  environment-specific replay-order semantics.
- **UR-268 [KNOWN]:** Speed up repository-owned replay-batch materialization for
  clustering replay by allowing block fetch and replay-item reconstruction work
  within one deterministic batch to proceed in parallel before the delegated
  trainer ingests that batch.
- **UR-269 [KNOWN]:** Any replay-batch materialization speedup must preserve the
  exact deterministic replay batch membership and item order that the current
  serial batch loader would have produced.
- **UR-270 [INFERRED]:** Replay-batch materialization concurrency must remain
  internal to repository-owned orchestration, bounded by existing memory and
  staging constraints, and must not introduce a new caller-visible concurrency
  selector unless an existing contract proves insufficient.
- **UR-271 [INFERRED]:** Parallel replay-batch materialization must preserve
  replay-validation identity, active-batch embedding-cache correctness, and
  deterministic failure diagnosability even when block fetch or decode
  completion order differs from final batch order.
- **UR-272 [KNOWN]:** Issue #93 asks LexonArchiveBuilder to separate
  clustering-replay batch size from CPU / worker concurrency because the
  current `max_concurrency` setting conflates those controls and forces
  operators to widen concurrency semantics just to get more efficient replay
  batch granularity.
- **UR-273 [KNOWN]:** Operators need to tune replay batch amortization
  independently of repository-owned replay-materialization concurrency, so a
  larger deterministic replay batch does not imply proportionally wider worker
  concurrency or broader pipeline concurrency semantics.
- **UR-274 [INFERRED]:** Any decoupling of replay batch size from worker
  concurrency must preserve deterministic replay order, replay-validation
  identity, stable fingerprints, and the existing upstream sequential lifecycle
  contract.
- **UR-275 [INFERRED]:** Independent replay batch sizing must remain subordinate
  to the existing bounded-memory replay orchestration boundary; larger replay
  batches or deeper amortization must not require unbounded prefetched payload
  state or invalidate the active-batch embedding-cache isolation rules.
- **UR-276 [KNOWN]:** The replay tuning surface should provide backward-
  compatible behavior or an explicit migration path for existing request /
  configuration files rather than silently changing the operational meaning of
  `max_concurrency`.
- **UR-277 [INFERRED]:** Replay batch-size and replay-materialization
  concurrency decoupling must remain internal to repository-owned indexing
  orchestration, content-type-neutral, and environment-neutral, and must not
  alter the caller-visible stage contract, `BatchSummary`, or MCP search/
  retrieval behavior.
- **UR-278 [KNOWN]:** Issue #95 asks LexonArchiveBuilder to allow replay
  materialization to prefetch and hold more than one future deterministic
  replay batch ahead of the current consumer batch when bounded capacity
  allows, rather than relying on an effectively single-slot producer/consumer
  handoff.
- **UR-279 [KNOWN]:** Operators need a bounded multi-batch ready queue so
  transient replay-materialization jitter is less likely to become directly
  visible as consumer stalls at replay-batch boundaries during clustering
  replay.
- **UR-280 [INFERRED]:** Deeper replay-prefetch buffering must preserve exact
  deterministic replay batch order and failure attribution; already-prepared
  future batches must drain in the same order the serial replay loader would
  have produced rather than in materialization-completion order.
- **UR-281 [INFERRED]:** Multi-batch replay prefetch must preserve
  active-batch embedding-cache semantics by keeping prepared future-batch state
  isolated until delegated handoff reaches that batch, so deeper buffering does
  not publish the wrong embedding set for the currently ingesting batch.
- **UR-282 [INFERRED]:** Multi-batch replay prefetch must remain bounded by the
  existing bounded-memory replay contract, stay neutral across approved content
  types and storage environments, and avoid introducing a new caller-visible
  tuning surface unless an existing contract proves insufficient.
- **UR-283 [KNOWN]:** Validation for this increment should show whether bounded
  deeper replay-prefetch buffering reduces consumer-visible replay-batch
  handoff stalls on representative clustering replay workloads.
- **UR-284 [KNOWN]:** Switch LexonArchiveBuilder to LexonGraph commit
  `7c8f375137375709bb608ee2609b38cb80e5422c`.
- **UR-285 [KNOWN]:** Switch LexonArchiveBuilder's clustering-enabled `0.7.0`
  path from the current upstream streaming-indexer v2 API to the new upstream
  constrained streaming-indexer v3 API.
- **UR-286 [KNOWN]:** Non-`0.7.0` published-profile selections should remain on
  the existing non-v3 path rather than becoming implicitly unsupported by this
  increment.
- **UR-287 [KNOWN]:** The upstream constrained v3 API for this increment accepts
  an ordered stream of existing replayable leaf block ids, requires a writable
  temporary working root for implementation-owned partition artifacts, and
  completes clustering-enabled execution through a single finalize transition
  over those ingested leaf ids.
- **UR-288 [INFERRED]:** Full-pipeline and clustering-only execution must both
  preserve the existing stage contract while feeding the v3 `0.7.0` path from
  the same deterministic replayable leaf block-id authority rooted in the
  immutable replay-audit journal.
- **UR-289 [INFERRED]:** LexonArchiveBuilder should derive the v3 working root
  from its existing request-adjacent artifact/output policy rather than adding
  a new caller-visible working-root selector.
- **UR-290 [INFERRED]:** Runtime progress and post-run diagnosis must remain
  operator-usable across the v3 migration even though the upstream v3 observer
  surface does not expose the same v2-only pending-partition detail fields.
- **UR-291 [INFERRED]:** If a required repository capability remains unavailable
  on the constrained v3 `0.7.0` path, LexonArchiveBuilder must report that gap
  explicitly rather than silently weakening split-stage replay, published-
  profile selection, or operator-visible progress behavior.
- **UR-292 [KNOWN]:** Switch LexonArchiveBuilder to the latest available
  LexonGraph `main` revision again after commit
  `7c8f375137375709bb608ee2609b38cb80e5422c`.
- **UR-293 [KNOWN]:** Adapt LexonArchiveBuilder to any breaking upstream
  indexing API changes required by that latest LexonGraph `main` revision while
  preserving the current external stage contract and unchanged MCP search or
  retrieval behavior for already-indexed content.
- **UR-294 [INFERRED]:** If the newer upstream `main` revision renames,
  reshapes, or otherwise changes constrained v3 lifecycle or observer surfaces,
  LexonArchiveBuilder should restore compatibility at its adapter boundary
  rather than silently dropping currently approved operator-visible progress,
  telemetry, or diagnosis behavior.
- **UR-295 [INFERRED]:** The refreshed upstream-main adaptation should continue
  to distinguish pure API-shape breakage from true upstream behavior
  regressions, surfacing any repository-required capability gap explicitly
  rather than masking it by narrowing LexonArchiveBuilder behavior.

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
| CM-INDEXER-062 | Revise | Change standalone clustering discovery from whole-store iteration to an authoritative replay-journal contract owned by LexonArchiveBuilder | UR-160, UR-163 |
| CM-INDEXER-063 | Revise | Require a LAB-owned durable immutable replay-audit journal for successfully persisted replayable work so resumed ingestion and clustering-only replay reuse repository-owned audit state | UR-160, UR-161, UR-162, UR-166 |
| CM-INDEXER-064 | Revise | Constrain the replay-audit journal to remain content-type-agnostic, low-overhead, append-only, hash-linked, and size-bounded under large-corpus growth | UR-161, UR-164, UR-166 |
| CM-INDEXER-065 | Revise | Clarify idempotence and recoverability so replay-audit resume behavior remains subordinate to immutable block semantics while using a repository-owned mutable journal-head reference | UR-8, UR-163, UR-165, UR-166 |
| CM-INDEXER-066 | Revise | Remove contradictory leftover requirements that still describe repository-local clustering controls after published-profile adoption | UR-115, UR-120, UR-137, UR-138 |
| CM-INDEXER-067 | Revise | Expand the published-profile contract from a repository-fixed version to a caller-selectable profile-version surface, while keeping low-level clustering controls retired | UR-139, UR-141, UR-142 |
| CM-INDEXER-068 | Revise | Replace the current fixed LexonGraph revision target with explicit temporary tracking of upstream `main` so new published profiles can be validated quickly | UR-140 |
| CM-INDEXER-069 | Revise | Refresh the adopted upstream dependency state so the published-profile selector can target upstream `0.3.0` immediately while preserving `0.1.0` as the repository default | UR-143 |
| CM-INDEXER-070 | Revise | Refresh the adopted upstream dependency state and repository-owned narrative so the current named experiment target is upstream `0.4.0` while preserving `0.1.0` as the repository default and retaining `0.3.0` only as historical context | UR-144 |
| CM-INDEXER-071 | Revise | Refresh the adopted upstream dependency state and repository-owned narrative so the current named experiment target expands to the upstream `0.6.x` profile series while preserving `0.1.0` as the repository default and retaining `0.5.x` only as prior comparison context | UR-145, UR-146 |
| CM-INDEXER-072 | Add | Require repository-local runnable sweep automation, currently `test.ps1`, for local/testing evaluation of the active published-profile experiment set without changing production or MCP-facing contracts | UR-147, UR-148 |
| CM-INDEXER-073 | Revise | Move stored-embedding readback for repository-owned quality, search, and diagnostic consumers behind the new upstream LexonGraph embedding reconstruction API instead of repository-local decoding logic | UR-149, UR-150, UR-152 |
| CM-INDEXER-074 | Add | Preserve existing CLI and MCP-visible contracts while making upstream LexonGraph the authority for supported stored embedding encodings and reconstruction semantics | UR-150, UR-151, UR-152 |
| CM-INDEXER-075 | Revise | Replace the current local-versus-plain-Azure tool-targeting split with a repository-wide approved profile set: direct local filesystem, the existing production overlay, or an additive `production-v2` direct Azure-backed store profile | UR-153, UR-154, UR-155, UR-156, UR-189, UR-190, UR-191 |
| CM-INDEXER-076 | Revise | Adopt LexonGraph v2 custom blocks for repository-owned non-search artifacts while leaving delegated branch and leaf index blocks on the current upstream-owned contract | UR-157, UR-158, UR-159 |
| CM-INDEXER-077 | Revise | Replace the current local-filesystem append-only replay journal with a shared BlockStore-backed immutable replay-audit journal that is authoritative for resume and clustering-only replay in all environments | UR-160, UR-162, UR-163, UR-166 |
| CM-INDEXER-078 | Add | Require each immutable replay-audit journal block to link to its prior journal block by hash so replay history becomes a Merkle-linked audit chain | UR-160, UR-161, UR-166 |
| CM-INDEXER-079 | Add | Publish the latest replay-audit journal head through a repository-owned mutable reference mechanism rather than by segment-file discovery or request-local state | UR-162, UR-165 |
| CM-INDEXER-080 | Revise | Replace the current per-record append-and-rollover journal discipline with bounded work-chunk aggregation into immutable journal blocks, preserving append-only semantics while leaving the exact rollover threshold to downstream design | UR-161, UR-164, UR-166 |
| CM-INDEXER-081 | Revise | Preserve split-stage recoverability and deterministic replay while removing whole-store scan fallback and treating the immutable replay-audit journal as the sole repository-owned replay authority | UR-8, UR-161, UR-163, UR-166 |
| CM-INDEXER-082 | Add | Require replay-audit entries to carry enough detail to reconstruct what inputs were processed, what repository-owned action ran, and which durable block outputs or equivalent artifacts were produced | UR-161, UR-166, UR-167 |
| CM-INDEXER-083 | Add | Publish the latest immutable final root through a repository-owned mutable reference mechanism so current-root discovery no longer depends on request-local output capture alone | UR-168 |
| CM-INDEXER-084 | Revise | Extend rooted quality reporting with query-workload access statistics and advisory RTT-cost estimates derived from per-layer bytes touched through the existing rooted recall path | UR-169, UR-170, UR-171, UR-172, UR-173 |
| CM-INDEXER-085 | Revise | Extend repository-local sweep automation with a runnable published-profile `0.7.0` fixed-budget ladder experiment plus execution plan, while carving out a local/testing-only clustering-cardinality selection exception for the approved ladder | UR-56, UR-57, UR-58, UR-174, UR-175, UR-176, UR-177, UR-178, UR-179 |
| CM-INDEXER-086 | Add | Introduce a CLI-only rooted block-copy operator tool that layers on top of existing LexonGraph block stores and copies immutable blocks reachable from caller-selected roots between approved block-store targets without changing indexing or MCP contracts | UR-180, UR-181, UR-182, UR-183, UR-184, UR-185, UR-186, UR-187, UR-188, UR-189, UR-190, UR-191 |
| CM-INDEXER-087 | Revise | Require the rooted block-copy CLI to emit default long-running liveness or progress visibility on its normal operator-facing output surface rather than staying silent until final summary | UR-180, UR-181, UR-186, UR-192, UR-193 |
| CM-INDEXER-088 | Add | Enable opt-in Azure SDK and HTTP-client diagnostic logging for the entire indexer binary through standard `RUST_LOG` initialization on the existing process output surface rather than through a new repository-specific CLI flag | UR-33, UR-194, UR-195 |
| CM-INDEXER-089 | Revise | Add an opt-in rooted copy blind-write mode that skips destination existence reads, keeps the current read-before-write behavior as the default, and relaxes exact copied-versus-skipped accounting in the blind-write path | UR-184, UR-186, UR-196, UR-197 |
| CM-INDEXER-090 | Revise | Add bounded asynchronous destination-write concurrency to rooted copy, expose an operator-selectable in-flight write limit defaulting to `64`, and apply that limit to both the default and blind-write paths whenever a destination write is required | UR-180, UR-184, UR-186, UR-196, UR-198, UR-199, UR-200, UR-201 |
| CM-INDEXER-091 | Revise | Expand the approved block-store profile vocabulary with an additive `gateway-http3` read-only profile for immutable block fetches, while preserving the existing writable profiles for write-bearing and whole-store-traversal flows | UR-202, UR-203, UR-204, UR-205, UR-206, UR-207, UR-208, UR-209 |
| CM-INDEXER-092 | Revise | Extend replay-based streaming indexing requirements so LexonArchiveBuilder's repository-owned orchestration remains within a bounded-memory contract for both full-pipeline and clustering-only execution over corpora larger than RAM | UR-48, UR-59, UR-160, UR-210, UR-211, UR-213, UR-214 |
| CM-INDEXER-093 | Revise | Realize replay-journal-driven deterministic ordering through an in-memory raw block-id list rather than SQLite or spill-based staging, while leaving block payload state in `BlockStore` until on-demand processing | UR-48, UR-160, UR-210, UR-211, UR-212, UR-213, UR-215, UR-216, UR-217, UR-218 |
| CM-INDEXER-094 | Add | Require standalone clustering replay to derive its deterministic processing order by reading replay-journal block ids only, then sorting and deduping them before classification and finalization | UR-163, UR-215, UR-216, UR-218, UR-219 |
| CM-INDEXER-095 | Revise | Advance the repository-default published profile from `0.1.0` to `0.7.0` while preserving the caller-visible profile selector surface and keeping low-level clustering controls retired | UR-52, UR-58, UR-139, UR-141, UR-142, UR-221, UR-222 |
| CM-INDEXER-096 | Add | Conditionally adopt the upstream streaming-indexer v2 API only for effective profile `0.7.0`, while preserving the existing non-v2 integration path for explicitly selected non-`0.7.0` profiles | UR-220, UR-223, UR-224, UR-225 |
| CM-INDEXER-097 | Revise | Remove the repository-local one-pass assumption from the effective-`0.7.0` v2 orchestration path so full replay passes continue until upstream planning completion succeeds or an upstream/runtime error occurs | UR-223, UR-225, UR-226, UR-227 |
| CM-INDEXER-098 | Revise | Extend clustering-enabled observability with pass-end convergence telemetry, an operator-discoverable dedicated sink preference, and explicit delegated-contract plus effective-profile identity logging | UR-228, UR-229, UR-230, UR-231, UR-232, UR-233 |
| CM-INDEXER-099 | Revise | Refresh the adopted LexonGraph integration target so the repository consumes the newer v2 intra-pass planning observer surface now available on upstream `main` | UR-234, UR-238 |
| CM-INDEXER-100 | Revise | Extend clustering-enabled observability from pass-end-only convergence summaries to operator-visible v2 intra-pass planning telemetry, including pass progress, pending partition detail, trainer subphase summaries, and suspected-stall indicators, while keeping that telemetry additive to the existing runtime progress and dedicated per-run sink surfaces | UR-235, UR-236, UR-237, UR-238 |
| CM-INDEXER-101 | Revise | Extend clustering-enabled observability from raw pass and intra-pass projection to user-usable convergence diagnosis that combines per-pass trend evidence with latest blocked-on state while preserving explicit uncertainty when telemetry is insufficient | UR-239, UR-240, UR-241, UR-242, UR-243 |
| CM-INDEXER-102 | Add | Require request-adjacent post-run convergence-diagnosis evidence for non-converged clustering-enabled runs using the existing per-run planning telemetry artifact family rather than a new control-plane or MCP surface | UR-240, UR-241, UR-243 |
| CM-INDEXER-103 | Revise | Refresh the approved latest-LexonGraph upgrade target to include the upstream streaming-indexer v2 planner-state-root and out-of-core planning-state capability introduced by commit `858ed455ea0828909aea38a0f2e677cca917ae76` | UR-244, UR-246, UR-247 |
| CM-INDEXER-104 | Add | Require clustering-enabled v2 execution to derive a writable delegated planner-state root from existing request-adjacent artifact/output locations, treat the resulting out-of-core planning data as upstream-owned opaque state, and fail explicitly when that root is unusable without adding a new caller-visible selector | UR-245, UR-247, UR-249, UR-250 |
| CM-INDEXER-105 | Revise | Clarify that upstream planner-managed out-of-core planning spill is approved for bounded-residency v2 planning while repository-owned deterministic replay ordering remains the existing in-memory raw block-id path with no new repository-owned spill catalog | UR-212, UR-217, UR-246, UR-248 |
| CM-INDEXER-106 | Revise | Replace the mandatory in-memory-only replay-order catalog with a bounded-residency deterministic replay-ordering contract that may use repository-owned externalized state when needed to keep memory independent of corpus size | UR-251, UR-252, UR-253, UR-254 |
| CM-INDEXER-107 | Add | Require any repository-owned replay-order externalization approved for issue #83 to remain deterministic, journal-driven, payload-free, environment-neutral, and semantically separate from delegated planner-state spill | UR-212, UR-216, UR-219, UR-254, UR-255 |
| CM-INDEXER-108 | Revise | Tighten standalone clustering replay so delegated replay inputs are reconstructed from stored replayable leaf blocks rather than by reopening request-era source references through resolver-owned paths | UR-39, UR-160, UR-163, UR-166 |
| CM-INDEXER-109 | Add | Require clustering-only replay to remain executable from the stored block-store snapshot once replayable leaf outputs are durable, without depending on original request-era source availability | UR-39, UR-160, UR-163, UR-166 |
| CM-INDEXER-110 | Add | Preserve provenance and replay-validation identity metadata for clustering-only replay without making metadata-only refs the execution dependency for replayed content reconstruction | UR-16, UR-23, UR-45, UR-49 |
| CM-INDEXER-111 | Add | Require validation coverage for source-independent document and email-derived clustering-only replay using stored leaf content rather than resolver-driven content rematerialization | UR-39, UR-160, UR-163, UR-166 |
| CM-INDEXER-112 | Add | Permit bounded repository-owned overlap of replay-batch preparation with delegated replay ingestion while preserving the upstream sequential lifecycle | UR-256, UR-257, UR-259 |
| CM-INDEXER-113 | Revise | Extend bounded-memory replay orchestration so any prefetched replay state remains tightly bounded and isolated from the live batch state | UR-214, UR-258, UR-259 |
| CM-INDEXER-114 | Add | Keep replay-preparation overlap internal, content-type-neutral, and environment-neutral rather than broadening the caller-visible batch surface | UR-260, UR-261 |
| CM-INDEXER-115 | Revise | Extend bounded-residency replay-order requirements so replay preparation must pursue materially better throughput and host-utilization behavior, not just bounded memory, while preserving deterministic deduped output | UR-262, UR-263, UR-264, UR-266 |
| CM-INDEXER-116 | Add | Permit internal replay-order preparation optimizations such as bounded overlap between journal scanning and compact-run materialization, or reduced per-record replay-key derivation overhead, so long as replay-order preparation remains payload-free and bounded-memory | UR-262, UR-263, UR-265, UR-266 |
| CM-INDEXER-117 | Add | Require comparative validation evidence that replay-order preparation improves throughput and/or CPU-disk utilization without changing caller-visible behavior or environment parity | UR-263, UR-264, UR-267 |
| CM-INDEXER-118 | Revise | Extend contract-safe replay-batch overlap so the repository-owned next-batch preparation phase may fetch and decode the referenced blocks in bounded parallelism while still handing the delegated trainer only fully materialized batches in deterministic replay order | UR-268, UR-269, UR-270, UR-271 |
| CM-INDEXER-119 | Add | Require deterministic replay-batch materialization to keep completion order internal, reassemble results into the serial baseline batch order, and preserve active-batch embedding-cache and replay-validation identity semantics | UR-269, UR-271 |
| CM-INDEXER-120 | Add | Require validation evidence that deterministic parallel replay-batch materialization targets the dominant repository-owned waiting seam and, where like-for-like reruns are practical, demonstrates reduced waiting and/or better CPU-disk utilization without changing deterministic batch contents or caller-visible lifecycle behavior | UR-268, UR-269, UR-270, UR-271 |
| CM-INDEXER-121 | Add | Separate deterministic replay batch-size tuning from repository-owned replay-materialization concurrency so operators can improve replay amortization without implicitly widening worker concurrency or unrelated pipeline semantics | UR-272, UR-273, UR-274 |
| CM-INDEXER-122 | Add | Require the decoupled replay-tuning surface to preserve bounded-memory replay, deterministic handoff semantics, and active-batch state isolation rather than turning batch-size tuning into an unbounded resident pipeline | UR-274, UR-275 |
| CM-INDEXER-123 | Add | Require backward-compatible behavior or an explicit migration path for existing `max_concurrency`-based request/config usage, while keeping replay-tuning changes internal to repository-owned orchestration and neutral across content types and environments | UR-276, UR-277 |
| CM-INDEXER-124 | Revise | Extend replay-batch preparation overlap from an effectively single-successor handoff to a bounded multi-batch ready queue that may stay more than one deterministic replay batch ahead when repository-owned capacity allows | UR-278, UR-279, UR-280 |
| CM-INDEXER-125 | Add | Require bounded multi-batch replay prefetch to preserve active-batch embedding-cache isolation, deterministic future-batch drain order, and unchanged delegated lifecycle sequencing | UR-280, UR-281 |
| CM-INDEXER-126 | Add | Require deeper replay-prefetch buffering to remain within the existing bounded-memory replay boundary, stay content-type and environment neutral, and produce validation evidence about consumer-visible stall reduction rather than assuming benefit | UR-282, UR-283 |
| CM-INDEXER-127 | Revise | Refresh the approved latest-LexonGraph upgrade target to commit `7c8f375137375709bb608ee2609b38cb80e5422c` and switch the approved `0.7.0` clustering-enabled path from upstream v2 to constrained upstream v3 | UR-284, UR-285, UR-287 |
| CM-INDEXER-128 | Revise | Keep non-`0.7.0` published-profile selections on the existing non-v3 path while the approved `0.7.0` path adopts constrained upstream v3, and require operator-visible run identity to distinguish those delegated contract families | UR-286, UR-291 |
| CM-INDEXER-129 | Revise | Replace the delegated planner-state-root requirement with a request-adjacent delegated v3 working-root requirement for the `0.7.0` path while preserving explicit failure and opaque upstream ownership of temporary partition artifacts | UR-285, UR-287, UR-289 |
| CM-INDEXER-130 | Revise | Preserve standalone clustering and full-pipeline replay semantics by feeding the delegated v3 path from the same deterministic replayable leaf block-id authority rather than reopening request-era source content | UR-287, UR-288 |
| CM-INDEXER-131 | Revise | Preserve operator-usable clustering progress and diagnosis across the v3 migration by projecting the best available v3 hierarchy-planning, partition-load, and bottom-up assembly telemetry without inventing missing v2-only pending-partition detail | UR-290, UR-291 |
| CM-INDEXER-132 | Revise | Refresh the approved latest-LexonGraph integration target from commit `7c8f375137375709bb608ee2609b38cb80e5422c` to the current upstream `main` revision and require LexonArchiveBuilder to absorb any breaking delegated API changes without weakening the approved external stage or clustering-observability contracts | UR-292, UR-293, UR-294, UR-295 |

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
- **After [KNOWN]:** The requirements now require a LAB-owned immutable replay-audit journal as the authoritative clustering-only replay-input source and remove whole-store iteration as an approved replay-discovery path.

### BA-INDEXER-063

- **Before [KNOWN]:** The requirements did not define any repository-owned durable artifact that records successful replayable leaf completion at ingestion time.
- **After [KNOWN]:** The requirements now introduce a LAB-owned immutable replay-audit journal that records successfully persisted replayable work and later supports both replay reuse and audit reconstruction across environments.

### BA-INDEXER-064

- **Before [KNOWN]:** The requirements did not constrain the persistence shape of any repository-owned replay catalog, so append-only write discipline, large-corpus rollover behavior, and low-overhead sequential replay were unspecified.
- **After [KNOWN]:** The requirements now constrain the replay-audit journal to remain append-only, low-overhead, hash-linked, and bounded by a size-oriented publication threshold without in-place mutation of committed blocks.

### BA-INDEXER-065

- **Before [KNOWN]:** Idempotence and recoverability were described only at the immutable-block and upstream-replay-contract level, without a repository-owned requirement for durable partial-progress reuse between split-stage invocations.
- **After [KNOWN]:** The requirements now clarify that LexonArchiveBuilder reuses repository-owned immutable replay-audit state plus a mutable journal-head reference for resume and clustering-only replay while remaining subordinate to LexonGraph-owned immutable-block and replay-validation semantics.

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
- **After [KNOWN]:** The requirements now constrain all indexer-owned block-store-consuming tools to one consistent approved profile set: direct local filesystem access, the existing fixed overlay block store composed of memory cache plus local filesystem cache plus Azure Blob SAS-backed storage, or an additive `production-v2` direct Azure-backed store profile.

### BA-INDEXER-076

- **Before [KNOWN]:** The requirements depended on LexonGraph block persistence but did not state whether repository-owned non-search artifacts should keep using v1-style leaf wrappers or move to the new v2 custom-block envelope.
- **After [KNOWN]:** The requirements now bind repository-owned non-search artifacts to LexonGraph v2 custom blocks, explicitly allow rebuilt artifact stores instead of continued v1 read compatibility for those artifacts, and leave delegated branch and leaf index blocks on the current upstream-owned contract in this increment.

### BA-INDEXER-077

- **Before [KNOWN]:** The requirements described the replay journal as a local-filesystem append-only artifact preferred for clustering-only replay, with whole-store iteration retained as a compatibility fallback and no requirement that the journal itself live on the shared immutable block boundary.
- **After [KNOWN]:** The requirements now treat replay state as a shared-`BlockStore`, immutable replay-audit journal that is authoritative for resume and clustering-only replay across both local/testing and production-oriented environments, with no whole-store scanning fallback.

### BA-INDEXER-078

- **Before [KNOWN]:** The requirements did not state that replay-journal entries themselves should be hash-linked immutable artifacts, so the journal could remain a repository-local file stream without parent-linked audit identity.
- **After [KNOWN]:** The requirements now require each replay-audit journal block to reference its predecessor by hash so the repository-owned audit history forms a Merkle-linked immutable journal chain.

### BA-INDEXER-079

- **Before [KNOWN]:** The requirements did not define how later invocations should discover the current replay-journal tip once journal state moves onto immutable blocks.
- **After [KNOWN]:** The requirements now require a repository-owned mutable reference mechanism for the latest replay-audit journal head, aligned in class with current-root discovery rather than with request-local or segment-file discovery.

### BA-INDEXER-080

- **Before [KNOWN]:** The requirements assumed low-overhead sequential record append and segment rollover, but they did not define a middle-granularity audit-block strategy between one-record-per-operation and one-block-per-full-run.
- **After [KNOWN]:** The requirements now require bounded work-chunk aggregation into immutable journal blocks, with rollover driven by an approved size-oriented threshold so the design can balance write amplification against replay redo cost.

### BA-INDEXER-081

- **Before [KNOWN]:** The requirements said the replay journal should support resume and replay reuse, but they did not explicitly require enough per-entry audit detail to reconstruct which inputs were processed, which repository-owned step ran, and which durable outputs were produced.
- **After [KNOWN]:** The requirements now require replay-audit entries to capture enough input identity, action-kind, and output-block detail for later audit and diagnosis rather than serving only as a minimal replay catalog.

### BA-INDEXER-082

- **Before [KNOWN]:** The requirements preserved `BatchSummary` final-root reporting for successful materialization, but they did not define a repository-owned mutable discovery surface for the current published root itself.
- **After [KNOWN]:** The requirements now require successful final-root materialization to publish the latest immutable root through a repository-owned mutable reference mechanism so current-root discovery is durable and no longer depends only on request-local output capture.

### BA-INDEXER-083

- **Before [KNOWN]:** The rooted quality requirements reported structure, embedding-space quality metrics, and rooted TNN-recall, but they did not require any visibility into how many stored blocks the query workload touched or how many serialized bytes those queries read by layer or in total.
- **After [KNOWN]:** The rooted quality requirements now require the query workload to report block-touch counts and serialized bytes read both per level and as overall totals, including per-query breakdowns plus aggregate totals for the executed query set.

### BA-INDEXER-084

- **Before [KNOWN]:** The rooted quality requirements did not require any repository-owned estimate of rooted-query transport cost, so operators could compare recall quality without any companion RTT-style read-amplification signal.
- **After [KNOWN]:** The rooted quality requirements now require an advisory per-query RTT estimate computed from per-layer bytes read under a fixed 64 KiB congestion-window assumption, with each layer rounded up independently and then summed into one total per query.

### BA-INDEXER-085

- **Before [KNOWN]:** The repository-local testing automation covered version-series profile sweeps such as the `0.6.x` run, but it did not define a fixed-budget ladder experiment for one published profile or any exception to the current profile-owned clustering-cardinality rule.
- **After [KNOWN]:** The requirements now extend the local/testing automation contract to include a runnable published-profile `0.7.0` ladder with fixed `beam_width * cluster_count` budget, an approved default five-rung set around the `16x64` baseline, comparable per-rung artifacts plus summary output, and a scoped local/testing-only mechanism for rung-specific clustering-cardinality selection.

### BA-INDEXER-086

- **Before [KNOWN]:** The requirements let operator tools read from or write to approved block-store targets, but they did not define any repository-owned tool for transferring immutable rooted block content from one configured store to another.
- **After [KNOWN]:** The requirements now introduce a CLI-only rooted block-copy operator tool that reuses the approved source and destination block-store contracts, copies only caller-selected roots and their reachable immutable blocks, preserves hash-addressed identities, reports copy outcomes, and leaves mutable-reference publication outside this increment.

### BA-INDEXER-087

- **Before [KNOWN]:** The requirements treated the non-local operator-facing block-store target as one fixed production overlay profile, so direct Azure-backed writes could not be added without violating the repository-wide shared tool-targeting contract.
- **After [KNOWN]:** The requirements now allow an additive `production-v2` profile alongside the existing production overlay profile so indexer-owned tools can target either the established cache-backed Azure path or the alternate direct Azure-backed LexonGraph store implementation through one shared profile vocabulary.

### BA-INDEXER-088

- **Before [KNOWN]:** The rooted block-copy requirements defined final summary and artifact output, but they did not require any default in-flight liveness signal while a large rooted traversal or transfer was still running.
- **After [KNOWN]:** The requirements now require the rooted block-copy CLI to emit basic default liveness or progress on its normal output surface during long-running copy work so operators can distinguish active traversal or transfer from a hung invocation without opting into a verbose flag.

### BA-INDEXER-089

- **Before [KNOWN]:** The indexer binary did not define any supported repository-level way to activate underlying Azure SDK or HTTP-client diagnostics, so setting `RUST_LOG` alone was not a reliable debugging path for storage or transport hangs.
- **After [KNOWN]:** The requirements now make `RUST_LOG` the approved opt-in diagnostic control for the entire `lexonarchivebuilder-indexer` binary, so repository operators can enable underlying SDK and HTTP-client logging without adding a new repository-specific CLI flag or making verbose diagnostics the default.

### BA-INDEXER-090

- **Before [KNOWN]:** Rooted copy always performed a destination existence read before attempting a write, and the result contract always required exact copied-versus-skipped accounting based on that pre-read behavior.
- **After [KNOWN]:** The requirements now preserve the existing read-before-write behavior as the default while adding an explicit opt-in blind-write mode that skips destination reads, attempts writes directly, and accepts reduced copy-versus-skip accounting in exchange for avoiding destination existence checks.

### BA-INDEXER-091

- **Before [KNOWN]:** Rooted copy traversed and wrote blocks effectively one destination write at a time, so high-latency backends such as Azure could serialize the transfer path even after a block had already been classified for writing.
- **After [KNOWN]:** The requirements now add bounded asynchronous destination-write concurrency, expose an operator-selectable in-flight write limit defaulting to `64`, and apply that bounded write pipeline to both rooted-copy modes whenever a destination write is actually needed, without changing rooted reachability or mode-specific reporting semantics.

### BA-INDEXER-092

- **Before [KNOWN]:** The approved repository block-store profile vocabulary
  covered only writable local or Azure-backed profiles, so repository-owned
  read-only fetch tooling had no approved gateway-backed HTTP/3 QUIC profile and
  no explicit rule separating read-only gateway use from write-bearing flows.
- **After [KNOWN]:** The requirements now add an additive `gateway-http3`
  read-only profile for immutable block fetches by gateway DNS host name while
  explicitly preserving the existing writable profiles for indexing,
  publication, and copy-destination flows.

### BA-INDEXER-093

- **Before [KNOWN]:** The requirements constrained concurrent leaf scheduling and
  replay correctness, but they did not require LexonArchiveBuilder's
  repository-owned replay orchestration to stay within a fixed memory budget
  when corpus size exceeded available RAM.
- **After [KNOWN]:** The requirements now require both full-pipeline and
  clustering-only execution to keep repository-owned staging, replay, and
  embedding-retention behavior within a bounded-memory contract, with public
  tuning remaining on replay batching and concurrency rather than on a
  dedicated memory-budget field, instead of retaining corpus-scale state in
  memory.

### BA-INDEXER-094

- **Before [KNOWN]:** The requirements described replay-based streaming and
  immutable replay-audit journaling, but they did not express whether
  fixed-memory compliance should prefer pure streaming or may freely spill
  staging state to storage.
- **After [KNOWN]:** The requirements now require this replay-ordering path to
  stay entirely in memory as a raw block-id list and no longer approve SQLite,
  spill files, or other repository-owned externalized ordering storage for this
  increment.

### BA-INDEXER-095

- **Before [KNOWN]:** The requirements did not distinguish retaining a
  deterministic ordering surface from retaining full block payload or embedding
  state while preparing clustering-only replay.
- **After [KNOWN]:** The requirements now constrain replay-order preparation to
  retain only unique raw block ids in memory, with embeddings and decoded block
  payloads loaded from `BlockStore` on demand during classification and
  finalization.
- **After [KNOWN]:** The requirements now make replay-list generation itself
  journal-only: it reads replay-audit blocks and recorded ids without fetching
  the referenced payload blocks until later processing needs them.

### BA-INDEXER-096

- **Before [KNOWN]:** The requirements preserved published-profile selection,
  but they still treated `0.1.0` as the repository-default profile for normal
  clustering-enabled execution.
- **After [KNOWN]:** The requirements now make `0.7.0` the repository-default
  published profile while preserving the same request and CLI selector surface
  for callers that explicitly choose a different supported profile.

### BA-INDEXER-097

- **Before [KNOWN]:** The requirements described one upstream streaming-indexer
  integration path for all published profiles and did not constrain when the
  newer upstream v2 surface should be used.
- **After [KNOWN]:** The requirements now route effective profile `0.7.0`
  through the upstream streaming-indexer v2 surface while preserving the
  existing non-v2 path for explicitly selected non-`0.7.0` profiles in this
  increment.

### BA-INDEXER-098

- **Before [KNOWN]:** The requirements required effective profile `0.7.0` to
  use the upstream streaming-indexer v2 surface, but they did not state
  whether LexonArchiveBuilder should continue replaying additional planning
  passes when one completed pass leaves v2 planning incomplete.
- **After [KNOWN]:** The requirements now remove the repository-local
  single-pass assumption: effective-`0.7.0` v2 orchestration must keep
  replaying full planning passes until upstream planning completion succeeds or
  an upstream/runtime error occurs.

### BA-INDEXER-099

- **Before [KNOWN]:** Runtime progress required general liveness across replay,
  planning, hierarchy work, and materialization, but it did not require
  pass-end telemetry that exposed enough delegated planning detail to tell
  whether repeated v2 planning passes were converging, nor did it require a
  clearly discoverable pass-summary sink or explicit run identity for the
  delegated contract/profile combination in use.
- **After [KNOWN]:** The requirements now require clustering-enabled runs to
  emit additive pass-end convergence telemetry that identifies the effective
  delegated contract family plus effective published profile version, and that
  surfaces enough upstream-returned planning summary data to judge whether
  repeated passes are converging; when practical, this telemetry should be
  written to an operator-discoverable dedicated file or separate output stream.

### BA-INDEXER-100

- **Before [KNOWN]:** The requirements required replay-submission visibility,
  richer live heartbeat projection, and pass-end convergence summaries, but
  they did not require LexonArchiveBuilder to consume the newer upstream v2
  intra-pass observer data available while a planning pass is still underway.
- **After [KNOWN]:** The requirements now require LexonArchiveBuilder to refresh
  its adopted upstream dependency state and project the newer v2 intra-pass
  planning observer data when available, including pass progress, pending
  partition detail, trainer subphase summaries, and suspected-stall
  indicators, while preserving operator-understandable repository-owned
  wording.

### BA-INDEXER-101

- **Before [KNOWN]:** The requirements required additive pass-end summaries and
  live intra-pass projection, but they did not require LexonArchiveBuilder to
  turn those separate signals into a user-usable answer to "is this run
  converging?" and "if not, what is it blocked on?".
- **After [KNOWN]:** The requirements now require clustering-enabled
  observability to surface user-usable convergence diagnosis that combines
  per-pass trend evidence with the latest available blocked-on state, and to
  state uncertainty explicitly when the delegated telemetry does not justify a
  stronger conclusion.

### BA-INDEXER-102

- **Before [KNOWN]:** Request-adjacent planning telemetry could contain
  pass-summary and intra-pass records, but the requirements did not require a
  discoverable post-run convergence-diagnosis summary for failed or otherwise
  non-converged runs.
- **After [KNOWN]:** The requirements now require additive request-adjacent
  convergence-diagnosis evidence for non-converged clustering-enabled runs so
  users can diagnose the last known convergence state and blocked-on reason
  without replaying the run or manually correlating every raw telemetry record.

### BA-INDEXER-103

- **Before [KNOWN]:** The requirements tracked the latest LexonGraph `main`
  upgrade for v2 planning telemetry and bounded-memory replay orchestration,
  but they did not account for the newer upstream planner-state-root contract
  or planner-managed out-of-core planning spill introduced in commit
  `858ed455ea0828909aea38a0f2e677cca917ae76`.
- **After [KNOWN]:** The requirements now treat that upstream planner-state-root
  and bounded-residency out-of-core planning-state capability as part of the
  approved latest-LexonGraph upgrade target for clustering-enabled v2 runs.

### BA-INDEXER-104

- **Before [KNOWN]:** The requirements required repository-owned replay
  ordering to remain in-memory and left room to prefer non-spilling behavior,
  but they did not distinguish that repository-owned rule from a delegated
  upstream need to spill planning data out of core beneath an upstream-managed
  root.
- **After [KNOWN]:** The requirements now preserve the existing in-memory raw
  block-id replay-order rule for repository-owned orchestration while
  explicitly approving upstream planner-managed out-of-core planning spill for
  v2 runs under a planner-state root derived from the existing request-adjacent
  artifact policy, with explicit failure when that derived root is unusable and
  no new caller-visible selector.

### BA-INDEXER-105

- **Before [KNOWN]:** The requirements still treated repository-owned replay
  ordering as an in-memory-only raw block-id catalog, so large clustering-only
  runs could satisfy deterministic replay semantics while still retaining
  corpus-scale resident ordering state before delegated planner spill became
  relevant.
- **After [KNOWN]:** The requirements now treat issue #83 as a replay-order
  memory correction: deterministic replay ordering must remain journal-driven
  and payload-free, but repository-owned replay-order preparation may use
  bounded externalized state when needed so resident memory no longer scales
  with corpus size and remains separate from delegated planner-state spill.

### BA-INDEXER-106

- **Before [KNOWN]:** Standalone clustering requirements said replay inputs are
  reconstructed from the authoritative replay journal and stored outputs, but
  they did not explicitly forbid replay from reopening request-era source
  files or rematerializing email chunk content through resolver-owned artifact
  decode and rechunking paths.
- **After [KNOWN]:** The requirements now explicitly constrain clustering-only
  replay to reconstruct delegated replay content from the stored replayable
  leaf blocks themselves, while treating request-era source refs and similar
  metadata as provenance or replay-validation state rather than the primary
  replay content source.

### BA-INDEXER-107

- **Before [KNOWN]:** The requirements did not state whether clustering-only
  replay must continue to function after the original request-era documents,
  mailboxes, or equivalent source containers become unavailable.
- **After [KNOWN]:** The requirements now explicitly require clustering-only
  replay to remain executable from the configured block-store snapshot plus
  replay metadata once replayable leaf outputs are durably stored and surfaced
  by the authoritative replay journal.

### BA-INDEXER-108

- **Before [KNOWN]:** Replay metadata such as source paths, normalized-email
  artifact refs, and chunk locators was required for deterministic identity and
  diagnostics, but the requirements did not clearly separate that role from the
  runtime's responsibility to reconstruct replayed delegated content.
- **After [KNOWN]:** The requirements now explicitly separate provenance and
  replay-validation identity metadata from the stored replay content transport,
  preserving diagnostics and stable fingerprints without allowing metadata-only
  refs to force resolver-driven content rematerialization during clustering-only
  replay.

### BA-INDEXER-109

- **Before [KNOWN]:** Validation covered authoritative replay discovery,
  deterministic replay ordering, and bounded replay-order residency, but it did
  not explicitly require coverage for the regression where clustering-only
  replay reuses stored embeddings while still rematerializing content through
  resolver paths.
- **After [KNOWN]:** Validation now explicitly covers document and email-derived
  clustering-only replay using stored replayable leaf content without source-
  file reopening, normalized-email rechunking, or equivalent resolver-driven
  rematerialization.

### BA-INDEXER-110

- **Before [KNOWN]:** The requirements treated replay submission as deterministic
  and bounded, but they did not explicitly say whether repository-owned
  replay-batch preparation must be fully serialized with delegated
  `ingest_batch(...)` or may overlap safely behind the same upstream lifecycle.
- **After [KNOWN]:** The requirements now explicitly allow bounded replay-batch
  preparation overlap as a repository-owned optimization, provided the delegated
  streaming lifecycle itself remains sequential and unchanged.

### BA-INDEXER-111

- **Before [KNOWN]:** Bounded-memory replay requirements bounded replay-order
  preparation and replay execution generally, but they did not explicitly
  constrain any future prepared-next-batch cache or embedding handoff state.
- **After [KNOWN]:** Bounded-memory replay requirements now explicitly bound any
  prefetched replay state to a tightly capped live working set and require
  prepared-next-batch state to remain isolated from the current batch until
  handoff is safe.

### BA-INDEXER-112

- **Before [KNOWN]:** Stable abstraction requirements preserved replay
  determinism and content-model neutrality, but they did not explicitly state
  that replay-preparation overlap must remain an internal optimization rather
  than a new caller-visible tuning surface.
- **After [KNOWN]:** The requirements now explicitly keep replay-preparation
  overlap behind the existing batch/runtime abstraction, preserving the current
  CLI, `BatchRequest`, and MCP-facing contracts.

### BA-INDEXER-113

- **Before [KNOWN]:** Replay-order requirements constrained memory growth and
  deterministic correctness, but they did not explicitly require repository-
  owned replay preparation to make effective use of available CPU or disk
  bandwidth when processing large replay journals.
- **After [KNOWN]:** The requirements now explicitly call for materially better
  replay-order preparation throughput and host utilization, while keeping
  deterministic output and the existing stage contract unchanged.

### BA-INDEXER-114

- **Before [KNOWN]:** The requirements allowed bounded externalized replay
  ordering but did not explicitly state whether repository-owned replay-order
  preparation could overlap journal scanning with bounded background run
  materialization or reduce avoidable per-record replay-key reconstruction
  overhead.
- **After [KNOWN]:** The requirements now explicitly permit internal replay-
  order preparation optimizations that improve throughput, provided they stay
  bounded-memory, payload-free, deterministic, and hidden behind the existing
  replay abstraction.

### BA-INDEXER-115

- **Before [KNOWN]:** Validation for replay-order externalization focused on
  correctness and bounded residency, without requiring comparative evidence that
  the optimized replay-order path actually improves replay-step throughput or
  resource utilization.
- **After [KNOWN]:** Validation now requires comparative evidence that replay-
  order preparation improves throughput and/or CPU-disk utilization while
  preserving deterministic deduped output, invariant behavior, and environment
  parity.

### BA-INDEXER-116

- **Before [KNOWN]:** The repository-owned replay-batch seam allowed one-batch-
  ahead preparation overlap, but each prefetched batch still loaded its
  referenced blocks and reconstructed replay items through a serial internal
  materialization path before delegated planning could consume that batch.
- **After [KNOWN]:** The requirements now permit bounded internal parallel block
  fetch and replay-item reconstruction within one replay batch, provided the
  completed batch is handed to delegated planning only after deterministic
  baseline order has been fully reassembled.

### BA-INDEXER-117

- **Before [KNOWN]:** The replay-batch overlap contract preserved sequential
  delegated lifecycle calls, but it did not explicitly say whether internal
  parallel block fetch/decode completion order could differ from final replay
  batch order while remaining deterministic.
- **After [KNOWN]:** The requirements now explicitly require internal parallel
  replay-batch materialization to keep completion order hidden, preserve exact
  deterministic batch membership and order, and maintain replay-validation and
  embedding-cache correctness.

### BA-INDEXER-118

- **Before [KNOWN]:** Validation and profiling evidence could show that replayed
  clustering spent substantial time waiting for next-batch materialization, but
  the requirements did not yet call for comparative evidence that repository-
  owned batch-load waiting was reduced without changing lifecycle behavior.
- **After [KNOWN]:** Validation now requires evidence that the optimized replay-
  batch materialization path targets the dominant repository-owned waiting seam
  and, where a like-for-like rerun is practical, demonstrates reduced waiting
  and/or improved CPU-disk utilization while preserving deterministic batch
  handoff semantics and the existing caller-visible lifecycle.

### BA-INDEXER-119

- **Before [KNOWN]:** The replay requirements allowed bounded overlap and bounded
  internal materialization parallelism, but they did not distinguish replay
  batch granularity from repository-owned worker concurrency, leaving
  `max_concurrency` free to conflate amortization and CPU-parallelism concerns.
- **After [KNOWN]:** The requirements now treat deterministic replay batch size
  and replay-materialization concurrency as separate tuning concerns, so larger
  batches may improve replay amortization without implicitly widening worker
  concurrency or unrelated pipeline semantics.

### BA-INDEXER-120

- **Before [KNOWN]:** The requirements did not define what compatibility promise
  applied when replay batch-size tuning was separated from the existing
  `max_concurrency`-shaped operational contract.
- **After [KNOWN]:** The requirements now demand backward-compatible behavior or
  an explicit migration path for existing request/config usage, while still
  preserving bounded-memory replay, deterministic handoff semantics, and
  unchanged caller-visible stage and MCP-serving contracts.

### BA-INDEXER-121

- **Before [KNOWN]:** The replay-batch overlap contract allowed bounded
  preparation ahead of the current delegated batch, but the approved
  requirements still fit an effectively single-successor prefetch handoff and
  did not explicitly permit a deeper bounded ready queue.
- **After [KNOWN]:** The requirements now allow repository-owned replay
  materialization to stay more than one deterministic batch ahead when bounded
  capacity allows, provided prepared future batches remain queued in exact
  replay order for later delegated handoff.

### BA-INDEXER-122

- **Before [KNOWN]:** Active-batch isolation requirements explicitly protected
  the current batch from one prepared successor, but they did not yet state how
  embedding-cache publication and ready-queue drain order should behave once
  multiple future batches could be resident.
- **After [KNOWN]:** The requirements now explicitly require deeper
  multi-batch replay prefetch to keep future-batch state isolated until its own
  handoff point, while preserving deterministic drain order and unchanged
  delegated lifecycle sequencing.

### BA-INDEXER-123

- **Before [KNOWN]:** Replay-prefetch requirements constrained memory growth and
  lifecycle safety, but they did not explicitly require validation evidence
  about whether a deeper bounded ready queue actually reduces consumer-visible
  replay-batch boundary stalls.
- **After [KNOWN]:** The requirements now require issue #95 validation to show
  whether bounded multi-batch replay prefetch improves consumer-visible replay
  handoff behavior on representative clustering workloads without breaking
  bounded-memory or deterministic replay invariants.

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

- **Required property [KNOWN]:** The delegated indexing flow must support the
  latest upstream lifecycle exposed by the selected delegated surface rather
  than depending on superseded training-oriented or pre-streaming surfaces.
  For the existing non-v3 path, that remains one or more planning passes,
  explicit planning completion, and final materialization replay. For the
  approved effective-`0.7.0` constrained v3 path, that is ordered leaf-block-id
  ingestion followed by one delegated finalize transition.
- **External-contract stability [KNOWN]:** LexonArchiveBuilder SHALL preserve the current caller-visible stage contract (`full`, `ingestion+embedding`, `clustering+block-assembly`) and SHALL NOT expose the raw upstream streaming lifecycle directly on the CLI or `BatchRequest`.
- **Replay obligation [KNOWN]:** LexonArchiveBuilder SHALL preserve a deterministic delegated item stream, including stable item ordering and replay identity, anywhere the upstream streaming lifecycle requires caller replay.
- **Boundary [KNOWN]:** LexonArchiveBuilder still does not own index-construction semantics; it consumes upstream streaming APIs rather than reimplementing indexing behavior in-repo.
- **Compatibility note [KNOWN]:** The latest known upstream lifecycle renames the repository's previously consumed training-oriented seam to a planning-oriented seam and introduces hierarchy-planning plus bottom-up-assembly status phases behind the same delegated indexing boundary.
- **Constrained-v3 note [KNOWN]:** For the approved effective published profile
  `0.7.0`, LexonArchiveBuilder SHALL adapt to the upstream constrained v3
  boundary that ingests ordered replayable leaf block ids into a temporary
  partition-working store and completes clustering plus block assembly through a
  single finalize transition.
- **Compatibility carveout [KNOWN]:** Published-profile selections other than
  `0.7.0` remain on the existing non-v3 delegated path until a later increment
  explicitly revises that compatibility boundary.
- **Idempotence constraint [INFERRED]:** Adapting to replay-based streaming indexing must preserve the existing immutable, hash-addressed rerun expectations for unchanged content.
- **Traceability:** UR-3, UR-8, UR-31, UR-45, UR-46, UR-48, UR-49, UR-61, UR-62, UR-63, UR-285, UR-286, UR-287

#### RQ-INDEXER-003A1 - Bounded-memory replay orchestration

LexonArchiveBuilder SHALL keep its repository-owned replay orchestration
bounded with respect to corpus size even when the indexed corpus is larger
than available system memory.

- **Execution scope [KNOWN]:** This requirement applies to both full-pipeline
  execution and clustering-only replay execution.
- **Retention boundary [KNOWN]:** Repository-owned staging structures such as
  replay-item inventories, mailbox-or-document expansion state, replay batches,
  and stored-embedding retention SHALL NOT require resident memory that scales
  linearly with total corpus size.
- **Permitted retained state [KNOWN]:** For replay-journal-driven deterministic
  ordering, LexonArchiveBuilder MAY retain the unique raw block-id list in
  memory when that retained state is limited to hash identities rather than
  decoded blocks, embeddings, or equivalent per-block payload state.
- **Boundedness semantics [INFERRED]:** The approved bounded-memory contract
  constrains repository-owned orchestration behavior rather than redefining
  opaque upstream-owned model state, but LexonArchiveBuilder SHALL treat any
  upstream incompatibility with this contract as an explicit adaptation
  finding rather than silently accepting unbounded growth.
- **Prefetch boundary [KNOWN]:** If repository-owned replay preparation overlaps
  delegated batch ingestion or training, the live resident working set SHALL
  remain limited to the current replay batch plus a tightly bounded amount of
  prepared future state rather than an unbounded queue of prefetched batches.
- **Surface boundary [KNOWN]:** This increment does not add a dedicated memory-
  budget field on the CLI or `BatchRequest`; the current caller-visible tuning
  controls remain `max_concurrency` and `replay_batch_size`, while spill
  thresholds and equivalent replay-order residency controls remain
  repository-owned implementation details.
- **Isolation boundary [KNOWN]:** Prepared future-batch replay state SHALL NOT
  replace or invalidate the content or embedding state needed by the currently
  active delegated batch before that delegated batch has completed ingestion.
- **Stage-contract boundary [KNOWN]:** Satisfying this requirement SHALL NOT
  change the caller-visible stage contract, `BatchSummary` contract, or MCP
  search-serving behavior for already-indexed content.
- **Extensibility boundary [INFERRED]:** Future content types must participate
  through the same bounded-memory orchestration boundary rather than adding
  content-type-specific corpus-scale retention exceptions.
- **Traceability:** UR-47, UR-48, UR-59, UR-160, UR-210, UR-211, UR-213, UR-214, UR-258, UR-259

#### RQ-INDEXER-003A2 - Bounded-residency deterministic replay ordering

LexonArchiveBuilder SHALL realize replay-journal-driven deterministic ordering
through a bounded-residency strategy whose live resident memory does not scale
with total replay-input corpus size.

- **Ordering algorithm [KNOWN]:** The runtime SHALL walk the replay list,
  gather referenced block ids, sort that list, dedupe it, and use the
  resulting unique block-id order as the deterministic order for classification
  and finalization.
- **Residency rule [KNOWN]:** When corpora exceed available RAM, replay-order
  preparation SHALL keep repository-owned resident memory bounded
  independently of total replay-input count rather than retaining a
  corpus-scale in-memory ordering catalog.
- **Payload boundary [KNOWN]:** Actual block state such as embeddings, decoded
  block bytes, or equivalent derived payload state SHALL be loaded from the
  shared `BlockStore` on demand while that ordered block-id list is processed
  rather than being cached as part of replay-order preparation.
- **Integrity boundary [KNOWN]:** Additional retained replay-order state, when
  present, SHALL remain limited to raw block identities, fixed-size journal-
  integrity digests, or equivalent compact validation evidence rather than
  decoded payload blocks, embeddings, or other corpus-scale variable-size
  state.
- **Replay-walk boundary [KNOWN]:** Building that ordered block-id list SHALL
  read replay-audit journal blocks and recorded block ids only; it SHALL NOT
  fetch referenced payload blocks during replay-list generation.
- **Externalization rule [KNOWN]:** Repository-owned externalized ordering state
  is approved when needed to satisfy the bounded-residency contract, provided
  that the externalized representation remains deterministic, derives only from
  replay-audit ordering inputs, and does not become a payload cache or a new
  MCP-facing artifact family.
- **Performance objective [KNOWN]:** Replay-order preparation SHALL materially
  improve repository-owned replay-step throughput versus the current bounded-
  residency external sort baseline when processing large replay journals, while
  preserving the same deterministic unique block-id order.
- **Utilization objective [KNOWN]:** Where replay-order preparation is limited
  by repository-owned orchestration rather than upstream planner work, the
  runtime SHOULD make materially better use of available CPU and/or storage
  throughput than a fully serialized journal-scan-plus-spill path, subject to
  the existing bounded-memory contract.
- **Optimization boundary [INFERRED]:** Approved optimizations MAY include
  bounded overlap between replay-journal scanning and compact run sorting/
  writing, earlier duplicate elimination within bounded replay-order scratch
  stages, or lower-overhead replay-key/digest derivation from replay-journal
  inputs, provided those changes do not alter replay-order semantics.
- **Validation boundary [INFERRED]:** Replay-order performance optimizations
  SHALL be justified by comparative validation evidence rather than by assumed
  benefit alone.
- **Separation rule [INFERRED]:** Any repository-owned externalized replay-order
  state SHALL remain semantically separate from the delegated planner-state
  root and SHALL NOT depend on upstream-owned planner spill formats,
  filenames, or lifecycle assumptions.
- **Parity rule [INFERRED]:** This ordering rule SHALL apply consistently
  across local/testing and production-oriented profiles rather than creating
  environment-specific replay-order behavior.
- **Traceability:** UR-48, UR-160, UR-210, UR-211, UR-212, UR-213, UR-214, UR-215, UR-216, UR-218, UR-219, UR-251, UR-252, UR-253, UR-254, UR-255, UR-262, UR-263, UR-264, UR-266

#### RQ-INDEXER-003A3 - Delegated v3 working-root derivation

When LexonArchiveBuilder uses the upstream constrained streaming-indexer v3
surface for a clustering-enabled run, it SHALL resolve a writable temporary
working root for that delegated run and SHALL permit the upstream v3
implementation to own partition-working artifacts beneath that root.

- **Applicability [KNOWN]:** This requirement applies to the effective
  clustering-enabled v3 path only, currently the approved effective published
  profile `0.7.0` path, and does not by itself widen v3 adoption to other
  profiles.
- **Ownership boundary [KNOWN]:** LexonArchiveBuilder SHALL treat concrete file
  names, directory layout, temporary partition-artifact formats, cleanup
  behavior, and equivalent mechanics beneath the delegated v3 working root as
  upstream-owned opaque behavior rather than redefining those internals
  repository-locally.
- **Surface rule [KNOWN]:** This increment SHALL NOT add a new working-root
  field on the CLI or `BatchRequest`. Instead, LexonArchiveBuilder SHALL derive
  the delegated v3 working root automatically from the existing request-
  adjacent artifact/output policy.
- **Replay-order boundary [KNOWN]:** This delegated working-root approval does
  not relax `RQ-INDEXER-003A2`; repository-owned deterministic replay ordering
  remains a separate repository-owned bounded-residency concern and SHALL NOT
  be conflated with the delegated v3 temporary partition-artifact family.
- **Failure rule [INFERRED]:** If the delegated v3 run cannot establish or use
  the required working root, LexonArchiveBuilder SHALL fail explicitly rather
  than silently switching delegated contract families or reporting a
  success-shaped clustering result.
- **Cleanup boundary [KNOWN]:** The delegated v3 working subtree is a temporary
  implementation-owned artifact family rather than a durable caller-visible
  output contract.
- **Environment boundary [INFERRED]:** The delegated v3 working-root contract
  SHALL remain compatible with both local/testing and production-oriented
  writable profiles, while preserving the existing stage contract and unchanged
  MCP search-serving or retrieval behavior for already-indexed content.
- **Traceability:** UR-63, UR-220, UR-223, UR-224, UR-225, UR-244, UR-245, UR-246, UR-247, UR-248, UR-249, UR-250, UR-287, UR-289

#### RQ-INDEXER-003A4 - Contract-safe replay-batch preparation overlap

LexonArchiveBuilder MAY overlap repository-owned replay-batch preparation for a
later deterministic replay batch with delegated ingestion or training of the
current batch when doing so remains subordinate to the approved upstream
lifecycle.

- **Lifecycle rule [KNOWN]:** The runtime SHALL NOT invoke concurrent
  `ingest_batch`, `finish_pass`, `mark_planning_complete`, or `finalize`
  operations on one delegated streaming run.
- **Ordering rule [KNOWN]:** Overlap SHALL NOT change deterministic replay
  submission order, pass boundaries, or finalization replay semantics.
- **Batch-handoff rule [KNOWN]:** LexonArchiveBuilder SHALL invoke delegated
  `ingest_batch(...)` only with a fully materialized replay batch whose item
  membership, order, and embedding-cache state are complete for that batch.
- **Identity rule [KNOWN]:** Overlap SHALL preserve replay-validation identity,
  stable content fingerprints, and clustering-failure diagnosability for
  replayed items.
- **Abstraction rule [KNOWN]:** This optimization remains internal to
  repository-owned orchestration and SHALL NOT add a new caller-visible stage,
  request shape, or MCP surface in this increment.
- **Extensibility rule [INFERRED]:** The overlap boundary SHALL remain generic
  across approved content types and environment-selected storage profiles.
- **Traceability:** UR-48, UR-49, UR-160, UR-214, UR-256, UR-257, UR-258, UR-259, UR-260, UR-261, UR-268, UR-269, UR-271

#### RQ-INDEXER-003A6 - Deterministic parallel replay-batch materialization

LexonArchiveBuilder MAY realize repository-owned replay-batch materialization
through bounded internal parallelism, provided that delegated planning still
sees the same deterministic completed batch the serial baseline would have
produced.

- **Applicability rule [KNOWN]:** This requirement applies only to the
  repository-owned step that reads a deterministic replay-order window, fetches
  the referenced stored blocks, reconstructs replay items, and prepares the
  active-batch embedding-cache state before delegated `ingest_batch(...)`.
- **Order-preservation rule [KNOWN]:** Internal block fetch, decode, and replay-
  item reconstruction MAY complete in a different order, but the finished batch
  SHALL be reassembled into the exact replay-entry order that the current serial
  implementation would have emitted.
- **Lifecycle rule [KNOWN]:** Internal materialization parallelism SHALL NOT
  introduce concurrent delegated `ingest_batch`, `finish_pass`,
  `mark_planning_complete`, or `finalize` calls, and SHALL NOT allow delegated
  ingestion to observe a partially materialized batch.
- **Cache-correctness rule [KNOWN]:** Any active-batch embedding-cache or
  equivalent repository-owned lookup state derived during batch materialization
  SHALL remain aligned with the final deterministic item order and SHALL NOT be
  published for a later batch before handoff.
- **Boundedness rule [INFERRED]:** This optimization SHALL remain bounded by the
  existing replay-batch and memory-residency contract rather than turning replay
  preparation into an unbounded payload cache or multi-batch resident pipeline.
- **Parity rule [INFERRED]:** The parallel materialization path SHALL remain
  compatible with both local/testing and production-oriented storage profiles
  through the same `BlockStore` and replay abstraction boundaries.
- **Traceability:** UR-268, UR-269, UR-270, UR-271

#### RQ-INDEXER-003A7 - Decoupled replay batch-size and worker-concurrency tuning

LexonArchiveBuilder SHALL treat deterministic clustering-replay batch size and
repository-owned replay-materialization worker concurrency as separate tuning
concerns.

- **Decoupling rule [KNOWN]:** Changing replay batch size to improve replay
  amortization SHALL NOT by itself require proportionally changing
  repository-owned worker concurrency or unrelated pipeline concurrency
  semantics.
- **Tuning rule [KNOWN]:** The repository-owned clustering replay boundary SHALL
  provide a backward-compatible way, or an explicit migration path, for
  operators to obtain independent effective control over replay batch
  granularity versus replay-materialization concurrency.
- **Lifecycle rule [INFERRED]:** Decoupled tuning SHALL remain subordinate to the
  approved upstream sequential lifecycle and SHALL NOT introduce concurrent
  delegated `ingest_batch`, `finish_pass`, `mark_planning_complete`, or
  `finalize` calls on one streaming run.
- **Determinism rule [INFERRED]:** Decoupled tuning SHALL preserve exact replay
  batch membership, deterministic batch order, replay-validation identity,
  stable fingerprints, and deterministic failure attribution.
- **Boundedness rule [INFERRED]:** Larger replay batches, or independent batch-
  size tuning generally, SHALL remain inside the existing bounded-memory replay
  and prepared-future-state limits rather than becoming an unbounded payload
  cache or resident multi-batch queue.
- **Boundary rule [INFERRED]:** This decoupling SHALL remain internal to
  repository-owned orchestration, generic across approved content types and
  environment-selected storage profiles, and SHALL NOT alter the caller-visible
  stage contract, `BatchSummary`, or MCP search/retrieval behavior.
- **Traceability:** UR-272, UR-273, UR-274, UR-275, UR-276, UR-277

#### RQ-INDEXER-003A8 - Bounded multi-batch replay-prefetch buffering

LexonArchiveBuilder MAY keep a bounded deterministic ready queue of more than
one fully materialized future replay batch ahead of the currently ingesting
batch when doing so reduces repository-owned replay handoff stalls without
violating the approved replay contract.

- **Queue-depth rule [KNOWN]:** Repository-owned replay prefetch MAY stay more
  than one batch ahead of the consumer when capacity allows, but the number of
  prepared future batches SHALL remain explicitly bounded rather than growing
  without limit.
- **Drain-order rule [KNOWN]:** Prepared future batches SHALL be handed to the
  delegated streaming lifecycle only in the exact deterministic replay order
  that the serial replay loader would have produced, regardless of internal
  materialization completion timing.
- **Active-batch rule [KNOWN]:** The runtime SHALL continue to publish
  active-batch embedding-cache state only for the batch currently being handed
  to delegated `ingest_batch(...)`; prepared future-batch state SHALL remain
  isolated until its own handoff point.
- **Lifecycle rule [INFERRED]:** Deeper buffering SHALL remain subordinate to
  the approved sequential delegated lifecycle and SHALL NOT introduce
  concurrent delegated `ingest_batch`, `finish_pass`, `mark_planning_complete`,
  or `finalize` calls on one streaming run.
- **Boundedness rule [INFERRED]:** A deeper ready queue SHALL remain inside the
  existing bounded-memory replay boundary rather than becoming an unbounded
  resident payload pipeline or weakening prepared-future-state isolation.
- **Parity rule [INFERRED]:** This buffering strategy SHALL remain compatible
  with both local/testing and production-oriented storage profiles through the
  same replay and block-store abstraction boundaries, and SHALL NOT require a
  new caller-visible tuning surface unless an existing contract proves
  insufficient.
- **Validation rule [KNOWN]:** The increment SHALL require validation evidence
  about whether bounded deeper replay-prefetch buffering reduces
  consumer-visible replay-batch boundary stalls on representative clustering
  workloads.
- **Traceability:** UR-278, UR-279, UR-280, UR-281, UR-282, UR-283

#### RQ-INDEXER-003A5 - Efficient replay-order preparation

LexonArchiveBuilder SHALL realize replay-order preparation as an efficient
repository-owned preprocessing stage whose optimizations remain subordinate to
the existing deterministic replay contract.

- **Internality rule [KNOWN]:** This increment SHALL NOT add a new caller-
  visible replay-order performance selector on the CLI, `BatchRequest`, or MCP
  surface.
- **Correctness rule [KNOWN]:** Optimizations to replay-order preparation SHALL
  preserve the same deduplicated deterministic replay order and the same
  replay-validation integrity outcomes that the non-optimized bounded-residency
  path would produce.
- **Payload rule [KNOWN]:** Replay-order preparation optimizations SHALL
  continue to operate on replay-journal metadata, compact ordering identities,
  and fixed-size validation evidence only; they SHALL NOT require replay
  payload dereference during replay-order generation.
- **Concurrency rule [INFERRED]:** If replay-order preparation overlaps journal
  scanning with background sort, spill, or merge work, the overlap SHALL remain
  tightly bounded and SHALL NOT introduce environment-specific correctness
  differences.
- **Parity rule [INFERRED]:** The optimized replay-order path SHALL remain
  compatible with both local/testing and production-oriented storage profiles
  through the same repository-owned abstraction boundary.
- **Traceability:** UR-262, UR-263, UR-264, UR-265, UR-266, UR-267

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

- **Replay authority [KNOWN]:** Standalone clustering SHALL reconstruct replay
  inputs from the repository-owned immutable replay-audit journal for the
  configured store snapshot rather than by rescanning the configured block
  store.
- **Scope [KNOWN]:** Standalone clustering SHALL examine all clustering-
  eligible replayable leaf inputs visible through the selected journal head
  rather than only inputs associated with a prior request or summary.
- **Ordering rule [KNOWN]:** After replay discovery identifies the eligible
  replayable leaf block ids, standalone clustering SHALL derive its
  deterministic processing order by sorting and deduping those block ids before
  classification and finalization.
- **Replay-read boundary [KNOWN]:** That discovery step SHALL read the
  repository-owned replay-audit journal blocks and their recorded ids without
  dereferencing the referenced payload blocks until later classification or
  finalization processing.
- **Replay execution boundary [KNOWN]:** After standalone clustering derives the
  deterministic replayable leaf block-id order, later replay submission SHALL
  reconstruct delegated replay inputs from the stored replayable leaf blocks
  themselves rather than by reopening request-era source files or rerunning
  source-artifact normalization or chunk derivation through resolver-owned
  paths.
- **V3-input compatibility [KNOWN]:** On the approved `0.7.0` v3 path, that
  replay authority SHALL be sufficient to feed the delegated API directly as an
  ordered stream of replayable leaf block ids without requiring repository-
  owned rematerialization of content references or inline payloads for the v3
  boundary itself.
- **Filtering boundary [INFERRED]:** Blocks or artifacts outside the approved
  clustering-input surface, including repository-owned artifact classes that
  are not valid clustering inputs, are outside this requirement's input set.
- **Request-shape implication [KNOWN]:** A clustering-only invocation may use an
  empty item collection because input discovery occurs from the configured block
  store rather than from request-supplied sources.
- **Idempotence implication [INFERRED]:** Repeating the clustering-only stage
  against an unchanged journal head and unchanged clustering-eligible block
  store snapshot is expected to yield the same logical clustering result under
  unchanged upstream semantics.
- **Traceability:** UR-39, UR-40, UR-160, UR-163

#### RQ-INDEXER-003E1 - Durable replay journal for split-stage reuse

LexonArchiveBuilder SHALL persist a repository-owned durable immutable
replay-audit journal for successfully persisted replayable work through the
shared `BlockStore` boundary in both local/testing and production-oriented
environments.

- **Write boundary [KNOWN]:** A journal record SHALL become durable only after
  the corresponding replayable work has been durably persisted through the
  approved storage boundary.
- **Reuse scope [KNOWN]:** The journal SHALL support at least:
  1. resumed ingestion that can recognize already completed replayable leaf
     outputs
  2. clustering-only replay that can reconstruct delegated replay inputs
     without requiring whole-store discovery
- **Authority boundary [KNOWN]:** The immutable replay-audit journal SHALL be
  the sole repository-owned authority for replay discovery in this increment;
  whole-store scan fallback is not an approved recovery path.
- **Source-independence implication [KNOWN]:** Once replayable leaf outputs
  have been durably persisted and surfaced by the selected replay-journal head,
  clustering-only replay SHALL remain executable from the configured
  block-store snapshot plus replay metadata without requiring the original
  request-era documents, mailboxes, or equivalent source containers to remain
  present or readable.
- **Environment scope [KNOWN]:** The same journal contract SHALL apply through
  the shared `BlockStore` boundary in both local/testing and production-
  oriented environments rather than diverging by environment.
- **Ownership boundary [KNOWN]:** The journal is a LexonArchiveBuilder-owned
  orchestration artifact and SHALL NOT redefine LexonGraph-owned block
  identity, embedding, or replay-validation semantics.
- **Metadata boundary [KNOWN]:** Replay metadata may preserve source paths,
  normalized-email artifact refs, chunk locators, and equivalent provenance or
  replay-validation identity, but clustering-only replay SHALL NOT require
  those metadata-only refs to act as the primary replay content transport when
  stored replayable leaf content is already present through the shared
  `BlockStore`.
- **Content-model constraint [INFERRED]:** The journal contract SHALL remain
  generic enough that future content types can emit replayable completion
  records without reshaping the batch contract around mailbox-only or
  document-only assumptions.
- **Traceability:** UR-160, UR-162, UR-163, UR-166

#### RQ-INDEXER-003E2 - Append-only and segmentable replay-journal behavior

The LexonArchiveBuilder replay-audit journal SHALL optimize for low-overhead
immutable block publication and deterministic replay under large-corpus
operation.

- **Mutation constraint [KNOWN]:** Committed journal records SHALL be append-only
  and SHALL NOT require in-place mutation of previously published journal
  blocks for ordinary progress recording.
- **Growth constraint [KNOWN]:** The journal SHALL support bounded work-chunk
  aggregation so one logical indexing corpus does not require a one-block-per-
  atomic-operation emission policy or a single monolithic journal block.
- **Recovery implication [INFERRED]:** Crash recovery MUST tolerate a partial or
  incomplete trailing write without invalidating earlier committed journal
  blocks.
- **Linkage constraint [KNOWN]:** Each committed replay-audit journal block
  SHALL identify its predecessor by hash so later replay can verify a single
  immutable history from the current journal head.
- **Rollover boundary [UNKNOWN]:** The exact size-oriented threshold or entry
  budget that triggers publication of the next journal block is not yet fixed
  at the requirements layer, but downstream design SHALL treat that threshold
  as an approved tuning point rather than emitting one block per atomic step.
- **Traceability:** UR-161, UR-164, UR-166

#### RQ-INDEXER-003E3 - Mutable replay-journal head discovery

LexonArchiveBuilder SHALL publish the latest immutable replay-audit journal head
through a repository-owned mutable reference mechanism.

- **Discovery boundary [KNOWN]:** Later invocations MUST be able to discover
  the current journal head without scanning the whole block store or traversing
  unrelated immutable artifacts.
- **Alignment boundary [KNOWN]:** This mutable reference mechanism SHALL be the
  same class of repository-owned discovery surface used for current-root
  publication rather than a request-local file or a local-only segment
  directory.
- **Artifact shape [KNOWN]:** The caller SHALL provide a ref name, and the
  runtime SHALL publish one human-readable JSON ref artifact at
  `refs/{ref_name}` for that name rather than coalescing multiple logical refs
  into one shared mutable file or blob.
- **Ownership boundary [INFERRED]:** The mutable reference points to immutable
  audit blocks by hash; it does not make the journal blocks themselves mutable.
- **Traceability:** UR-162, UR-165

#### RQ-INDEXER-003E4 - Bounded audit coverage of embedding and indexing work

LexonArchiveBuilder SHALL record embedding and indexing progress as repository-
owned audit entries grouped into bounded immutable replay-audit journal blocks.

- **Audit scope [KNOWN]:** The journal SHALL document completed work across the
  repository-owned embedding and indexing orchestration boundary rather than
  only final leaf identifiers.
- **Audit detail [KNOWN]:** Each replay-audit entry SHALL record enough detail
  to identify:
  1. the relevant input item, artifact, or prior block identities being acted on
  2. the repository-owned action or step kind that completed
  3. the generated block identities or equivalent durable output artifacts
- **Granularity boundary [KNOWN]:** The journal SHALL aggregate multiple
  completed work entries into one immutable block so redo cost is bounded
  without forcing a one-block-per-atomic-operation design.
- **Extensibility boundary [INFERRED]:** The grouped audit-entry model SHALL
  remain content-type-agnostic so future content types can emit equivalent
  progress entries without redefining the batch contract.
- **Traceability:** UR-161, UR-164, UR-166, UR-167

#### RQ-INDEXER-003E5 - Mutable current-root publication

For any successful execution stage that materializes a new final root,
LexonArchiveBuilder SHALL publish the latest immutable root block through a
repository-owned mutable reference mechanism.

- **Discovery boundary [KNOWN]:** Later invocations and operator tools MUST be
  able to discover the current published root without scanning unrelated block-
  store contents or depending on request-local output capture alone.
- **Stage boundary [KNOWN]:** Stages that do not materialize a new final root
  SHALL NOT rewrite the current-root reference.
- **Ref payload boundary [KNOWN]:** The `refs/{ref_name}` JSON payload SHALL
  carry the latest replay-journal head block id, the latest root block id when
  a root-materializing stage completed successfully, and publication metadata
  such as the effective profile version.
- **Ownership boundary [INFERRED]:** The mutable reference points to an
  immutable root block identity by hash; it does not make the root block
  mutable.
- **Alignment boundary [KNOWN]:** Replay-journal head publication SHALL use the
  same class of repository-owned mutable reference mechanism as current-root
  publication.
- **Traceability:** UR-168

#### RQ-INDEXER-003F - Published profile adoption for clustering-enabled execution

For any execution stage that includes clustering plus block assembly,
LexonArchiveBuilder SHALL invoke the delegated LexonGraph streaming indexer
through the published-profile API and SHALL accept a caller-selectable
published profile version, defaulting to `0.7.0`, for this increment.

- **Upstream contract [KNOWN]:** The delegated streaming indexer now exposes a
  published-profile surface in which profile version selects the approved leaf
  algorithm, packing strategy, hierarchy strategy, summary policy, and related
  planning parameters as one versioned configuration.
- **Approved default [KNOWN]:** Published profile version `0.7.0` remains the
  approved default clustering-enabled profile in this increment.
- **Evaluation rule [KNOWN]:** Callers may select a different upstream-published
  profile version for evaluation without reintroducing repository-local
  clustering algorithms, planning policies, or tuning controls.
- **Conditional-v3 rule [KNOWN]:** When the effective selected published profile
  version is `0.7.0`, LexonArchiveBuilder SHALL use the upstream constrained
  streaming-indexer v3 surface rather than the existing non-v3 path.
- **Non-v3 compatibility rule [KNOWN]:** When the effective selected published
  profile version is not `0.7.0`, LexonArchiveBuilder SHALL keep using the
  existing non-v3 delegated path until a later increment approves broader v3
  adoption.
- **Selector-source rule [KNOWN]:** The effective selected published profile
  version is the one that remains after applying the existing selector
  precedence across CLI override, request-file value, and repository default;
  the non-v3-versus-v3 delegated-surface choice SHALL follow that effective
  version irrespective of which selector source supplied it.
- **Run-identity visibility [KNOWN]:** For every clustering-enabled run,
  LexonArchiveBuilder SHALL make the effective selected published profile
  version together with the delegated contract family actually used for that
  run operator-visible, so pass-level telemetry can be interpreted without
  inferring non-v3-versus-v3 routing from source code or omitted defaults.
- **Published-profile availability [KNOWN]:** When temporary upstream `main`
  tracking exposes the current named experiment target in the `0.6.x` series,
  LexonArchiveBuilder SHALL refresh its adopted upstream dependency state so
  callers can select that version without changing the omitted-selector default
  away from `0.7.0`.
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
- **Traceability:** UR-39, UR-44, UR-61, UR-62, UR-121, UR-122, UR-123, UR-124, UR-139, UR-141, UR-142, UR-143, UR-144, UR-145, UR-146, UR-220, UR-221, UR-223, UR-230, UR-231, UR-285, UR-286

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
  default `0.7.0` behavior or choose another upstream-published profile version
  for evaluation.
- **Retirement rule [KNOWN]:** Legacy clustering mode, clustering algorithm, and
  algorithm-specific tuning controls SHALL be removed from the approved
  external contract for this increment rather than silently accepted and
  ignored.
- **Environment-parity implication [INFERRED]:** The same CLI surface must remain
  usable for local/testing and production-shaped batch invocations so
  environment selection does not introduce a separate profile-configuration
  interface family.
- **Traceability:** UR-4, UR-12, UR-13, UR-42, UR-119, UR-123, UR-124, UR-125, UR-139, UR-141, UR-142, UR-221, UR-222

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
- **Local-testing ladder exception [KNOWN]:** The approved repository-local
  `0.7.0` fixed-budget ladder may select rung-specific clustering cardinality as
  part of local/testing operator automation, but that exception is limited to
  the approved ladder surface and does not broaden the normal batch contract.
  While effective profile `0.7.0` is required to use the upstream constrained
  streaming-indexer v3 path and that path exposes only published-profile
  construction, the repository may fail this ladder surface explicitly rather than
  silently ignoring rung-specific clustering-cardinality selection.
- **Delegation boundary [KNOWN]:** Any future variation in clustering cardinality
  for this surface must come from an approved published profile version rather
  than from repository-local remapping of profile internals, except for the
  scoped local/testing ladder aid approved in this increment.
- **Traceability:** UR-56, UR-57, UR-58, UR-121, UR-123, UR-124, UR-139, UR-174, UR-175, UR-179, UR-221

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
  - defaulting clustering-enabled execution to published profile `0.7.0` while
    permitting explicit selection of another upstream-published profile version
    for evaluation
  - using the upstream constrained streaming-indexer v3 surface only when the effective
    selected profile version is `0.7.0`
  - preserving non-`0.7.0` published-profile evaluation through the existing
    non-v3 delegated path until broader v3 adoption is explicitly approved
  - feeding the effective-`0.7.0` v3 path from deterministic replayable leaf
    block ids rooted in the immutable replay-audit journal, rather than
    reopening request-era source content
  - deriving and using a request-adjacent writable delegated v3 working root
    without adding a new caller-visible selector
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
  - additive pass-end telemetry that identifies the effective delegated
    contract family plus effective published profile version and surfaces enough
    planning summary detail to judge whether repeated passes are converging
  - resolving a writable planner-state root for each delegated v2 run and
    allowing the latest upstream planner-managed bounded-residency out-of-core
    planning state beneath that root
  - explicit failure when the delegated v2 planner-state root is unusable,
    instead of silently dropping back to a weaker or unbounded planning path
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
- **Traceability:** UR-47, UR-61, UR-63, UR-64, UR-65, UR-66, UR-67, UR-68, UR-69, UR-71, UR-126, UR-127, UR-128, UR-129, UR-130, UR-140, UR-143, UR-144, UR-145, UR-147, UR-148, UR-220, UR-221, UR-223, UR-224, UR-225, UR-226, UR-227, UR-228, UR-230, UR-231, UR-232, UR-233, UR-244, UR-245, UR-246, UR-247, UR-249

#### RQ-INDEXER-003J - Local published-profile sweep automation

LexonArchiveBuilder SHALL provide a repository-local runnable operator
automation surface, currently `test.ps1`, that reuses the approved local/testing
indexing and rooted-quality workflow to evaluate the current published-profile
experiment set without changing repository code for each tested profile.

- **Local-only boundary [KNOWN]:** This automation is for local/testing operator
  evaluation and does not define a production runtime entrypoint, request
  schema, or deployment contract.
- **Current experiment target [KNOWN]:** The automation SHALL support approved
  experiment targets on the same repository-local surface, including the earlier
  upstream `0.6.x` published-profile series and the approved `0.7.0`
  fixed-budget ladder defined in `RQ-INDEXER-003J1`.
- **Comparison rule [INFERRED]:** The automation should continue to emit
  per-target artifacts and comparable summary output so version-series sweeps
  remain comparable to earlier baselines such as `0.5.x` when included, and
  ladder runs remain comparable across their approved rung set.
- **Contract-preservation rule [INFERRED]:** The automation SHALL drive the
  existing batch and rooted-quality tool surfaces instead of introducing a
  testing-only indexing API or changing MCP search semantics.
- **Extensibility rule [INFERRED]:** The operator-edited profile list may change
  as new published profiles land, but the automation surface should remain
  version-series-agnostic so future profile series do not require a new
  repository contract shape solely to update hard-coded experiment labels.
- **Traceability:** UR-12, UR-84, UR-85, UR-139, UR-140, UR-145, UR-146, UR-147, UR-148, UR-174

#### RQ-INDEXER-003J1 - Local fixed-budget ladder experiment automation

LexonArchiveBuilder SHALL provide a repository-local runnable operator
automation surface, evolving the same local/testing evaluation path currently
used by `test.ps1`, that can execute an approved fixed-budget ladder for
published profile `0.7.0` without changing production or MCP-facing contracts.

- **Ladder rule [KNOWN]:** Each rung SHALL pair one beam width (the rooted
  quality traversal width used for the evaluation) with one clustering
  cardinality such that `beam_width * cluster_count` remains constant across the
  ladder.
- **Default budget [KNOWN]:** The first runnable ladder SHALL default to budget
  `1024`, anchored on traversal width `16` and clustering cardinality `64`.
- **Default rung set [KNOWN]:** The first runnable ladder SHALL default to the
  five-rung sequence `4x256`, `8x128`, `16x64`, `32x32`, and `64x16`.
- **Artifact rule [KNOWN]:** The automation SHALL emit per-rung build artifacts,
  per-rung rooted-quality artifacts, and a comparable machine-readable summary
  table that preserves rung identity plus the selected beam width and clustering
  cardinality.
- **Execution-plan rule [KNOWN]:** The same operator aid SHALL define an
  executable plan covering preflight validation, rung ordering, artifact
  locations, and post-run comparison steps for the approved ladder.
- **Local-only boundary [KNOWN]:** This ladder automation is a local/testing aid
  only and SHALL NOT redefine the production runtime contract, request schema,
  or MCP-visible search and retrieval behavior.
- **Scoped-selection boundary [INFERRED]:** Any rung-specific clustering
  cardinality selection introduced for this ladder must remain scoped to the
  repository-local experiment surface and SHALL NOT become a general low-level
  clustering-control family for ordinary batch runs.
- **Temporary upstream-gap rule [KNOWN]:** If the upstream `0.7.0`
  streaming-indexer v2 surface still lacks a supported resolved-profile or
  equivalent local/testing override hook, this operator aid MAY fail fast with
  an explicit operator-facing error instead of silently dropping the rung's
  clustering-cardinality selection. Such failure remains a temporary upstream
  compatibility gap to be tracked outside this repository rather than a reason
  to route effective profile `0.7.0` back through the legacy delegated path.
- **Traceability:** UR-12, UR-84, UR-85, UR-139, UR-147, UR-148, UR-174, UR-175, UR-176, UR-177, UR-178, UR-179

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
  - gateway-backed `gateway-http3` read-only immutable-block fetch profile addressed by DNS host name with implied HTTPS-over-QUIC authority on port `443`
  - local filesystem for local/testing operation
  - overlay block store for production-oriented operation, composed of memory cache + local filesystem cache + Azure Blob Storage backing addressed by SAS URL
  - additive `production-v2` direct Azure-backed store profile for production-oriented operation
- **Approved tool-targeting modes [KNOWN]:** Every indexer-owned tool that reads
  from or writes to the shared `BlockStore` boundary SHALL support one approved
  shared profile vocabulary: direct local filesystem, the existing
  `production` overlay profile, the additive `production-v2` direct
  Azure-backed profile, and where the tool surface can operate correctly through
  read-only immutable block fetches, the additive `gateway-http3` profile.
- **Disallowed mode [KNOWN]:** A direct non-local backend that is not expressed
  through one of the approved repository-defined profiles is not an approved
  operator-facing mode in this increment.
- **Current increment [KNOWN]:** The existing local/testing realization remains required, and this increment additionally requires both approved non-local target profiles to be usable on the same repository-owned tool surfaces rather than being introduced tool-by-tool.
- **Gateway addressing [KNOWN]:** The additive `gateway-http3` profile accepts a
  gateway DNS host name and derives HTTPS-over-QUIC authority from that host on
  port `443` rather than accepting an arbitrary scheme or base URL in this
  increment.
- **Gateway applicability [KNOWN]:** The additive `gateway-http3` profile is
  read-only and is approved only for tool surfaces that can operate correctly
  without block writes or whole-store block-ID iteration.
- **Local filesystem interoperability [KNOWN]:** The local/testing filesystem-backed realization SHALL use the LexonGraph-owned filesystem block-store contract, including its on-disk naming and layout scheme, so LexonGraph filesystem tooling such as `lexongraph-block-inspect` can operate on LexonArchiveBuilder-produced local stores.
- **Local implementation target [KNOWN]:** The local/testing filesystem-backed realization SHALL use the upstream `lexongraph-block-store-fs` crate rather than a repository-local filesystem naming scheme.
- **Migration boundary [KNOWN]:** This local filesystem interoperability correction may require a fresh or rebuilt local store; continued read compatibility with blocks written by the superseded custom local layout is not required in this increment.
- **Overlay shape [KNOWN]:** The non-local target mode SHALL be a fixed ordered overlay containing an in-memory cache layer, a local filesystem cache layer, and an Azure Blob backing layer addressed through a SAS URL rather than a caller-selectable arbitrary stack of storage backends.
- **Direct-profile addition [KNOWN]:** The additive `production-v2` profile SHALL target the alternate direct Azure-backed LexonGraph `BlockStore` implementation without introducing a repository-owned translation layer around block identities or payload bytes.
- **Gateway-profile addition [KNOWN]:** The additive `gateway-http3` profile
  SHALL fetch immutable block bytes through the gateway's `/block/<block_id>`
  contract without introducing repository-owned translation of block identities
  or payload bytes.
- **Artifact reuse [KNOWN]:** The same environment-selected `BlockStore` abstraction family SHALL also be used for normalized email artifacts and mailbox provenance artifacts, provided indexing contracts and retrieval references remain explicit.
- **Tool-surface consistency [INFERRED]:** Batch indexing, standalone clustering discovery, rooted quality assessment, rooted CLI search, and future indexer-owned operator tools SHALL share the same block-store target-selection contract instead of inventing per-tool storage mode variants.
- **Assessment-tool implication [INFERRED]:** Post-index rooted block-tree quality assessment SHALL also read blocks through the same environment-selected `BlockStore` boundary rather than bypassing it with a repository-specific storage reader.
- **Mailbox retention [KNOWN]:** Mailbox provenance artifacts SHALL be retained so the original source material remains available for re-normalization, re-chunking, and re-ingestion flows.
- **Traceability:** UR-3, UR-6, UR-9, UR-12, UR-13, UR-18, UR-22, UR-25, UR-26, UR-27, UR-28, UR-80, UR-86, UR-153, UR-154, UR-155, UR-156, UR-189, UR-190, UR-191, UR-202, UR-203, UR-204, UR-205, UR-206, UR-207, UR-209

#### RQ-INDEXER-005A - LexonGraph v2 custom-block adoption for repository-owned artifacts

LexonArchiveBuilder SHALL read and write its repository-owned non-search
artifact blocks using LexonGraph v2 custom blocks.

- **Included artifact families [KNOWN]:** This applies to normalized email artifacts, mailbox provenance artifacts, and similar repository-owned non-search artifacts that LexonArchiveBuilder itself defines and persists through the shared `BlockStore` boundary.
- **Migration boundary [KNOWN]:** This transition may require rebuilt local filesystem stores, rebuilt overlay-backed stores, and regenerated repository-owned non-search artifacts; continued read compatibility with pre-v2 v1 artifact blocks is not required in this increment.
- **Upstream boundary [KNOWN]:** Delegated branch and leaf index blocks remain on the current upstream-owned contract for this increment; LexonArchiveBuilder does not introduce a repository-owned branch-or-leaf format translation layer around the delegated streaming indexer.
- **Contract stability [INFERRED]:** Operator-facing batch, CLI, and MCP semantics remain unchanged apart from the repository-owned artifact-format change.
- **Traceability:** UR-157, UR-158, UR-159

#### RQ-INDEXER-005B - Rooted block-store copy between approved targets

LexonArchiveBuilder SHALL provide a CLI-only operator tool that copies
caller-selected immutable rooted block graphs from one configured block store
to another configured block store.

- **Invocation scope [KNOWN]:** The tool SHALL accept one or more caller-supplied
  root block identities and copy only those root blocks plus the immutable
  blocks reachable from them, rather than performing whole-store replication by
  default.
- **Source and destination targeting [KNOWN]:** The tool SHALL reuse the same
  approved block-store target profiles on both source and destination sides:
  direct local filesystem, the existing `production` overlay block store
  composed of memory cache + local filesystem cache + Azure Blob
  SAS-backed storage, or the additive `production-v2` direct Azure-backed
  store profile; the additive `gateway-http3` profile is additionally approved
  on the source side only because it is read-only in this increment.
- **Identity preservation [KNOWN]:** Copied blocks SHALL retain their existing
  hash-addressed identities; the tool SHALL NOT reinterpret, rewrite, or
  repository-locally translate delegated or repository-owned block payloads.
- **Idempotence boundary [INFERRED]:** Re-running the same copy into a
  destination that already contains some or all requested block identities
  SHALL be treated as safe operator behavior rather than as a duplicate-write
  error contract.
- **Mode boundary [KNOWN]:** The default rooted-copy mode SHALL preserve the
  current destination read-before-write behavior, but the CLI SHALL also allow
  an explicit operator-selected blind-write mode that skips destination
  existence reads and attempts writes directly.
- **Write-concurrency requirement [KNOWN]:** When the tool has determined that a
  destination write must occur, it SHALL allow multiple destination writes to
  remain in flight asynchronously instead of serializing the entire transfer
  through one completed destination write at a time.
- **Write-concurrency control [KNOWN]:** The CLI SHALL expose an operator-
  selectable maximum in-flight destination-write limit, and the first
  repository-approved default for that limit is `64`.
- **Mode applicability [KNOWN]:** The bounded asynchronous destination-write
  limit applies to the opt-in blind-write path and also to the default
  read-before-write path for blocks that have already been classified as absent
  and therefore require publication.
- **Boundary [INFERRED]:** The tool SHALL traverse and persist blocks through
  the shared `BlockStore` abstraction boundary rather than through a separate
  storage-backend-specific transfer path.
- **Implementation boundary [KNOWN]:** This increment SHALL layer on top of the
  existing LexonGraph block-store implementations already adopted by the
  repository rather than defining a new repository-owned block-store backend
  family for copying.
- **Gateway-source boundary [KNOWN]:** When rooted copy uses the additive
  `gateway-http3` source profile, missing blocks map from gateway `404`
  responses and any transport, protocol, or other non-success responses remain
  explicit failures rather than becoming synthetic skip outcomes.
- **Output requirement [KNOWN]:** The tool SHALL emit both a human-readable
  summary and a machine-readable artifact that reports requested roots, copied
  block counts, skipped-already-present counts, and copy failures.
- **Blind-write reporting boundary [KNOWN]:** In the opt-in blind-write mode,
  the tool MAY relax exact copied-versus-skipped accounting and instead report
  attempted-write and failure outcomes, because that mode is explicitly chosen
  to avoid destination reads rather than to preserve pre-read destination-state
  classification.
- **Concurrent-reporting boundary [INFERRED]:** Bounded asynchronous destination
  writes SHALL NOT weaken the truthfulness of the existing mode-specific report
  contract; out-of-order write completion may change throughput, but it SHALL
  NOT redefine rooted reachability, immutable identity, or the meaning of
  copied, skipped, attempted, and failed outcomes on the approved reporting
  surface.
- **Liveness requirement [KNOWN]:** During long-running rooted traversals or
  block transfer work, the tool SHALL emit basic default operator-visible
  liveness or progress on its normal CLI output surface before final completion
  so a large copy does not appear hung while work is still advancing.
- **Mutable-reference exclusion [KNOWN]:** This increment copies immutable block
  content only; repository-owned mutable references such as current-root or
  replay-journal-head publication remain out of scope unless a later approved
  increment expands the contract explicitly.
- **Content-type neutrality [INFERRED]:** The rooted copy contract SHALL remain
  agnostic to mailbox, document, and future content types because it operates
  on block identities and rooted reachability rather than content-model
  semantics.
- **Surface boundary [KNOWN]:** The tool is additive to existing indexing,
  quality, search, and MCP surfaces and SHALL NOT become an indexing stage, a
  `BatchRequest` feature, or an MCP-visible API in this increment.
- **Traceability:** UR-153, UR-154, UR-155, UR-156, UR-180, UR-181, UR-182, UR-183, UR-184, UR-185, UR-186, UR-187, UR-188, UR-189, UR-190, UR-191, UR-192, UR-193, UR-196, UR-197, UR-202, UR-205, UR-206, UR-207, UR-209

#### RQ-INDEXER-005C - Opt-in SDK diagnostic logging on existing CLI surfaces

LexonArchiveBuilder SHALL support opt-in underlying SDK and HTTP-client
diagnostic logging for the entire `lexonarchivebuilder-indexer` binary through
the standard Rust environment-driven logging path.

- **Activation contract [KNOWN]:** Operators SHALL be able to activate
  repository-visible Azure SDK and HTTP-client diagnostics by setting
  `RUST_LOG` or an equivalent standard Rust log-filter environment variable
  recognized by the process, without requiring a repository-specific CLI flag.
- **Scope [KNOWN]:** This activation contract SHALL apply across the indexer
  binary rather than being limited to one subcommand such as rooted block copy.
- **Opt-in boundary [KNOWN]:** Diagnostic logging SHALL remain disabled by
  default so ordinary indexing, quality, search, and copy workflows do not gain
  unsolicited SDK or transport noise.
- **Surface boundary [INFERRED]:** The diagnostic output SHALL remain on the
  existing short-lived process output streams rather than introducing a separate
  daemon, control plane, or MCP-visible diagnostics surface.
- **Traceability:** UR-33, UR-194, UR-195

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

- **Local/testing [KNOWN]:** either direct local filesystem + local embedding
  service, or the preserved `local-overlay` shape that reuses the approved
  overlay-backed storage profile together with a local embedding service
- **Production-oriented [KNOWN]:** either the existing production overlay block
  store (memory cache + local filesystem cache + Azure Blob SAS-backed
  storage) + Azure OpenAI, or the additive `production-v2` direct Azure-backed
  store profile + Azure OpenAI
- **Constraint [KNOWN]:** Environment-specific adapter selection for every indexer-owned tool must expose the same approved storage-profile set rather than allowing some tools to invent one-off direct production backends outside the repository-defined `production` and `production-v2` profiles.
- **Traceability:** UR-6, UR-7, UR-12, UR-13, UR-153, UR-154, UR-155, UR-156, UR-189, UR-190, UR-191

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
- **Streaming lifecycle visibility [KNOWN]:** Progress output must remain
  meaningful across the delegated lifecycle selected for the run without
  requiring callers to understand raw upstream phase names. On non-v3 paths
  this includes planning passes, planning completion, and final materialization;
  on the approved v3 path this includes ordered replay submission,
  hierarchy-planning, partition-load, bottom-up assembly, and final
  persistence.
- **Embedding-phase visibility [KNOWN]:** For any execution stage that includes ingestion plus embedding generation, progress output must continue after delegated items have been prepared and while local embedding or leaf-materialization work is still consuming those delegated items.
- **Replay-submission visibility [KNOWN]:** For any execution stage that submits known replay batches to the delegated streaming API, including clustering-only execution reconstructed from stored leaf blocks, progress output must report repository-owned replay-batch submission completion in bounded work units using the known batch count and cumulative delegated-item count for the invocation.
- **Phase-boundary clarity [KNOWN]:** When repository-owned replay-batch
  submission completes and LexonArchiveBuilder begins waiting for further
  delegated clustering work, the runtime progress stream must emit an explicit
  handoff message so operators can distinguish local submission completion from
  subsequent upstream observer activity.
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
- **V3 telemetry projection [KNOWN]:** When the effective selected published
  profile version routes clustering-enabled execution through the upstream
  constrained streaming-indexer v3 surface, LexonArchiveBuilder SHALL project
  the delegated hierarchy-planning, partition-load, and bottom-up assembly
  observer data it receives onto operator-visible output, including explicit
  stage identity, phase-local totals or progress, and active partition or layer
  identifiers when those fields are exposed.
- **Observer-fidelity rule [KNOWN]:** On the v3 path, LexonArchiveBuilder SHALL
  NOT invent v2-only pending-partition detail, trainer-subphase summaries, or
  suspected-stall diagnoses that the delegated observer did not expose.
- **Intra-phase semantics discipline [INFERRED]:** LexonArchiveBuilder SHOULD
  render delegated live clustering telemetry in repository-owned language that
  makes clear whether a record is a live in-phase observation, a layer-local
  completion, or a terminal convergence summary.
- **Per-pass convergence visibility [KNOWN]:** When the delegated API for the
  selected path exposes completed planning-pass summaries, operator-visible
  telemetry SHALL surface enough pass-end evidence to tell whether planning is
  converging after `N` passes rather than merely reporting that another pass was
  required. When such summaries are not exposed on the selected path, the
  runtime SHALL NOT invent synthetic pass boundaries or convergence metrics and
  SHALL instead surface the best available live phase evidence with explicit
  uncertainty about trend-to-convergence.
- **Completed-pass summary fields [KNOWN]:** When the delegated API exposes
  completed pass summaries, the surfaced record SHALL include pass number,
  planned-partition count, terminal-partition count, hierarchy depth,
  requested-versus-realized planning cluster counts, and equivalent topology or
  planning indicators needed to judge convergence when those fields are
  available.
- **Dedicated-sink preference [KNOWN]:** When the runtime can do so without
  changing the approved batch contract, it SHOULD write pass-end convergence
  telemetry to an operator-discoverable dedicated file or separate output
  stream in addition to the ordinary runtime progress log so operators can find
  pass summaries without scanning the full progress transcript.
- **Dedicated-sink scope [INFERRED]:** When the runtime already writes a
  request-adjacent per-run planning telemetry artifact, it SHOULD extend that
  same additive sink with the live clustering records available on the selected
  delegated path rather than creating a second repository-owned observability
  file for the same run.
- **Additive-surface rule [INFERRED]:** Any dedicated pass-end telemetry sink
  remains additive to the normal runtime progress stream and SHALL NOT become a
  separate control plane, metrics backend, or MCP-visible surface.
- **Non-goal [KNOWN]:** This requirement does not introduce a separate control-plane, metrics backend, or MCP-surface change.
- **Traceability:** UR-32, UR-33, UR-39, UR-41, UR-45, UR-48, UR-59, UR-60, UR-61, UR-62, UR-63, UR-67, UR-68, UR-69, UR-70, UR-71, UR-228, UR-229, UR-230, UR-231, UR-232, UR-233, UR-234, UR-235, UR-236, UR-237, UR-238, UR-287, UR-290

#### RQ-INDEXER-008B1 - User-usable convergence diagnosis

For clustering-enabled execution, LexonArchiveBuilder SHALL surface delegated
planning telemetry in a user-usable diagnostic form that lets operators
understand the latest available delegated progress evidence, including whether
convergence can be assessed from the selected path's telemetry and what the run
is currently or last known to be waiting on.

- **Evidence-composition rule [KNOWN]:** The surfaced diagnosis must stay keyed
  to one effective run identity and must combine only the evidence the selected
  delegated path actually exposed. On paths that expose completed-pass
  summaries, the diagnosis should relate those summaries across passes. On paths
  that do not expose completed-pass summaries, the diagnosis should surface the
  latest live phase evidence instead. In either case it must also surface the
  latest available waiting-state evidence, such as pending partitions,
  trainer subphase, suspected-stall indicators, active hierarchy-planning
  partition identity, partition-load phase, bottom-up assembly layer identity,
  or equivalent delegated waiting state when those fields are exposed.
- **Provenance rule [INFERRED]:** Repository-owned messaging SHOULD make clear
  which parts of the diagnosis came from completed-pass convergence summaries,
  which came from live or last-known in-phase observations, and when a stronger
  convergence judgment is unavailable because the selected path does not expose
  comparable pass-boundary evidence.
- **User-interpretation rule [INFERRED]:** The surfaced diagnosis SHOULD reduce
  the amount of manual correlation a user must perform across raw progress
  lines or JSONL records in order to understand convergence behavior.
- **Honesty rule [KNOWN]:** When delegated telemetry does not justify a stronger
  conclusion, LexonArchiveBuilder SHALL report that the convergence or
  blocked-on diagnosis is uncertain or incomplete rather than inventing a
  repository-owned conclusion unsupported by the telemetry.
- **Surface boundary [KNOWN]:** This requirement is additive to the existing
  runtime progress and request-adjacent planning telemetry artifact surfaces
  and does not introduce a separate control plane or MCP-visible diagnostics
  surface.
- **Traceability:** UR-239, UR-240, UR-241, UR-242, UR-243

#### RQ-INDEXER-008B2 - Post-run convergence diagnosis persistence

When clustering-enabled execution ends without confirmed successful delegated
completion after clustering telemetry has been emitted, LexonArchiveBuilder
SHALL preserve deterministic request-adjacent diagnosis evidence for post-run
analysis.

- **Applicability [INFERRED]:** This requirement applies to repository-owned
  termination paths such as upstream/runtime failure or other non-converged
  completion paths that occur after planning telemetry has become available; it
  does not claim recovery from abrupt external process termination that bypasses
  repository-controlled shutdown logic.
- **Minimum content [KNOWN]:** The preserved evidence must identify the
  effective run identity, the latest completed-pass convergence evidence when
  that class of evidence was available on the selected path, the latest
  available live-phase or blocked-on evidence used for diagnosis, and an
  explicit indication when convergence trend evidence was unavailable.
- **Artifact-family boundary [INFERRED]:** The first approved realization should
  reuse the existing request-adjacent planning telemetry artifact family,
  whether by an end-of-run summary record or a sibling summary artifact, rather
  than introducing a second unrelated observability channel.
- **Non-goal [KNOWN]:** This requirement does not require MCP clients to query
  convergence-diagnosis state through the search-serving surface.
- **Traceability:** UR-240, UR-241, UR-243

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
- **Query-workload access accounting [KNOWN]:** The assessment must report, for
  the rooted query workload it executes, how many unique stored block
  identities are touched and how many serialized block bytes are read, both
  broken down by block level and summarized as overall totals.
- **Query-workload boundary [INFERRED]:** This access accounting applies to
  rooted queries executed by the quality tool itself, including corpus-based
  TNN-recall queries and any optional user-query diagnostic recall queries that
  are enabled for the invocation, rather than to unrelated MCP searches or
  index-construction-time I/O.
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
- **Traceability:** UR-80, UR-81, UR-82, UR-83, UR-84, UR-85, UR-86, UR-87, UR-101, UR-102, UR-111, UR-112, UR-169

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

#### RQ-INDEXER-008D4 - Rooted-query access accounting

LexonArchiveBuilder SHALL report rooted-query access statistics for each query
the rooted quality tool executes and for the overall executed query set.

- **Per-query outputs [KNOWN]:** For each rooted query, the report SHALL include
  the total count of unique block identities touched, the total serialized bytes
  read, and the same two measures broken down by block level.
- **Aggregate outputs [KNOWN]:** The report SHALL also include the same
  block-touch and serialized-byte-read measures aggregated across the full
  executed query set, while preserving the distinction between corpus-based and
  optional user-query diagnostic recall modes when both are present.
- **Byte-count rule [INFERRED]:** Serialized-byte reporting is based on the
  encoded block size read through the shared `BlockStore` boundary for the
  touched block identities, not on a repository-local reinterpretation of block
  payload structure.
- **Accounting boundary [INFERRED]:** This accounting models the logical rooted
  query traversal performed by the quality tool and does not require reporting
  cache-hit effects, retry behavior, or unrelated block-store reads outside the
  query path.
- **Surface boundary [INFERRED]:** This requirement extends only the CLI rooted
  quality tool's reporting contract and does not alter MCP search-serving
  behavior or the separate rooted CLI text-search surface.
- **Traceability:** UR-169, UR-170, UR-171

#### RQ-INDEXER-008D5 - Advisory RTT-cost estimate for rooted queries

LexonArchiveBuilder SHALL report an advisory RTT-style transport-cost estimate
for each rooted query executed by the quality tool.

- **Fixed model [KNOWN]:** For this increment, the estimate SHALL assume a
  congestion window of 64 KiB.
- **Per-layer formula [KNOWN]:** For one rooted query, the RTT contribution for
  a block level SHALL be `ceil(bytes_read_at_that_level / 65536)`.
- **Per-query total [KNOWN]:** The total RTT estimate for one rooted query SHALL
  be the sum of those rounded-up per-level RTT contributions.
- **Aggregate reporting [INFERRED]:** When the report includes aggregate query
  statistics over a query set, it SHOULD also include the corresponding rolled-
  up RTT-cost totals or summaries derived from the per-query RTT estimates.
- **Advisory boundary [KNOWN]:** This estimate is a transport-style diagnostic
  expressed in RTT units only; it SHALL NOT be presented as a wall-clock latency
  prediction or as a substitute for measured end-to-end runtime.
- **Traceability:** UR-170, UR-171, UR-172, UR-173

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
- Broadening the scoped local/testing-only ladder cardinality selector into a general-purpose production or MCP-visible clustering-tuning surface
- Requiring detailed clustering-input inventories for successful clustering runs in this increment
- Requiring the block-tree quality assessment tool to expose an MCP-visible interface in this increment
- Reinterpreting advisory embedding-space quality heuristics as new LexonGraph-owned block-validity rules in this increment
- Allowing user-query diagnostic recall to contribute to automated or aggregate rooted-quality metrics
- Requiring the rooted CLI search tool to replace or redefine the existing MCP search surface in this increment
- Defining a repository-local search algorithm or a second repository-local search corpus model instead of using `lexongraph-search` over the approved rooted-tree boundary
- Performing whole-store block replication by default when the approved first increment is rooted-copy-only
- Copying repository-owned mutable references such as current-root or replay-journal-head publication as part of the first immutable block-copy increment
- Defining or maintaining a repository-local branch-embedding decoding matrix for evolving branch encodings when the upstream LexonGraph embedding readback API already owns the supported branch reconstruction semantics
- Preserving mixed-format or pre-v2 v1 compatibility for repository-owned non-search artifact blocks after the approved v2 custom-block transition
- Preserving the current append-only filesystem replay-journal segment layout or whole-store replay-discovery fallback after the immutable block-backed replay-audit journal is adopted

## Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | Requirements explicitly constrain scope to indexing-time orchestration and integrations |
| Environment-specific storage and embedding behavior stays behind stable interfaces | Preserved with revised storage contract | Stage selection, block-store iteration, clustering-status reporting, and operator-tool traversal now share the same approved storage-profile contract: local filesystem, the existing production overlay, or additive `production-v2`, instead of splitting between local filesystem and ad hoc plain Azure targeting |
| Architecture remains extensible to future content types | Preserved | Collection-oriented input still covers both mailbox and document collections, and stage selection is defined in generic pipeline terms rather than mailbox-specific behavior |
| Idempotence and recoverability stay aligned with underlying immutable block semantics | Preserved with clarified scope | Requirements extend hash-addressed identity expectations to normalized email artifacts and require clustering-only reruns over the same clustering-eligible block-store snapshot to remain semantically stable under unchanged upstream semantics |
| Local development remains self-contained and batch-oriented | Preserved | Docker Compose is constrained to compose local dependencies around the batch container rather than changing the runtime model |
| Local published-profile evaluation remains outside production and serving contracts | Preserved with expanded local/testing aid | Requirements constrain both the earlier `0.6.x` sweep and the new `0.7.0` fixed-budget ladder to repository-local operator automation that reuses existing batch and quality boundaries rather than adding a production entrypoint or MCP-visible test surface |
| Long-running batches remain observable without adding a control plane | Preserved with clarified scope | Progress reporting remains on the existing batch-runtime log surface and now explicitly includes the long-running embedding or leaf-materialization gap between mailbox expansion and downstream streaming-status visibility plus clustering-only replay submission progress, the handoff into upstream planning-pass waiting, and failure-only clustering diagnostics on the runtime log plus a request-adjacent artifact |
| Caller-visible indexing and MCP contracts remain stable across the upstream API migration | Preserved with approved contract change | The stage surface and MCP retrieval semantics remain stable while clustering-enabled indexing keeps the existing profile-version selector, advances the repository default to `0.7.0`, and conditionally adopts the upstream v2 surface only for that effective profile |
| Immutable block identity remains the transfer contract across storage targets | Preserved with expanded operator tooling | The rooted copy tool is constrained to copy hash-addressed immutable blocks through the shared `BlockStore` boundary without redefining block payload semantics, mutable-reference publication, or MCP behavior |
| Long-running operator tools remain observable without adding a control plane | Preserved with expanded scope | Requirements now extend the existing no-silent-gap observability principle to the rooted block-copy CLI, requiring default operator-visible liveness on the normal CLI surface during long-running rooted traversal or transfer work rather than only at final summary time |
| Operator diagnostics remain opt-in and stay on standard process output | Preserved with expanded diagnostic path | Requirements now approve `RUST_LOG`-controlled SDK and HTTP-client diagnostics for the entire indexer binary, but keep them disabled by default and constrained to the existing process output streams rather than introducing a new flag-specific or service-style observability channel |
| Copy idempotence remains subordinate to immutable block semantics | Preserved with explicit operator-selected tradeoff | Requirements keep read-before-write classification as the default rooted-copy path, allow an opt-in blind-write mode that still treats duplicate publication as safe operator behavior, and now permit bounded asynchronous destination writes without changing rooted reachability or truthful mode-specific outcome semantics |
| Clustering configuration remains explicit and replayable | Preserved with revised contract | Requirements now treat the selected published profile version as the replay-relevant clustering input, make `0.7.0` the repository default, and constrain the chosen upstream integration surface to follow that effective profile selection rather than a repository-local mode, algorithm, and option tuple |
| Clustering-size behavior remains deterministic under the selected profile | Preserved with scoped local/testing exception | Normal batch behavior still assigns clustering cardinality to the selected published profile version, while the approved `0.7.0` ladder adds one repository-local deterministic rung table for local/testing evaluation only |
| Large-corpus indexing remains memory-bounded at the repository orchestration layer | Revised with issue-83 replay-order correction | Requirements now permit repository-owned replay ordering to use bounded externalized state when needed so resident memory no longer scales with corpus size, while still keeping delegated v2 planner-managed out-of-core planning state separate beneath the writable planner-state root |
| Clustering-only replay does not require whole-store rediscovery | Revised with authoritative immutable audit artifact | Requirements now require a shared-BlockStore immutable replay-audit journal as the sole repository-owned replay authority and remove whole-store scan fallback |
| Repository-owned progress artifacts stay aligned with immutable storage principles | Preserved with stronger alignment | Requirements now move replay and audit state onto immutable hash-addressed blocks plus a mutable head reference, matching the repository's broader storage model instead of retaining a special append-only file journal |
| Required repository capabilities remain distinguishable from upstream regressions during the latest upgrade | Preserved with clarified scope | The requirements now force the upgrade to classify missing capabilities explicitly instead of silently narrowing split-stage replay, published-profile adoption, progress projection, or MCP-facing behavior |
| Latest upstream telemetry remains subordinate to the existing runtime progress surface | Preserved with clarified scope | Requirements now constrain richer live telemetry and heartbeat events to the same repository-owned log stream rather than a new telemetry interface |
| Operator-visible progress counts remain understandable across upstream telemetry changes | Preserved with clarified scope | Requirements now distinguish invocation-total delegated-item counts from stage-local or layer-local telemetry counts so upstream count-shape changes do not create misleading logs |
| Clustering-enabled run identity remains operator-visible and replay-explainable | Preserved with stronger observability | Requirements now require telemetry to identify both the effective selected published profile version and the delegated non-v3-versus-v3 contract family actually used for the run |
| Repeated planning passes remain user-diagnosable for convergence without a new control plane | Preserved with expanded observability | Requirements now require additive pass-end convergence telemetry plus user-usable convergence diagnosis that relates per-pass evidence across the same run identity and can be preserved for post-run analysis without scanning every raw record manually |
| Clustering telemetry remains operator-visible without redefining repository surfaces | Preserved with revised upstream shape | Requirements now project the best available delegated v3 hierarchy-planning, partition-load, and bottom-up assembly telemetry onto the same runtime progress plus per-run additive telemetry surfaces without inventing missing v2-only fields |
| Post-index quality assessment remains subordinate to existing storage and serving boundaries | Preserved with clarified scope | The new assessment is constrained to a CLI-only operator tool that reads through the shared `BlockStore` boundary and does not alter MCP-facing behavior |
| Rooted-quality access-cost reporting remains advisory and repository-local | Preserved with clarified scope | Query access statistics and RTT estimates are constrained to CLI quality diagnostics over the existing rooted `BlockStore` boundary and do not redefine MCP search latency, transport, or serving contracts |
| Stored embedding format awareness remains upstream-owned | Preserved with revised ownership | Requirements now place supported stored-embedding encodings and reconstruction semantics behind the upstream LexonGraph readback API instead of a repository-local decoder table |
| Aggregate recall evaluation remains rooted-corpus-based and reproducible | Preserved with clarified scope | TNN-Recall is constrained to uniform seeded sampling over the rooted reachable embedding set for aggregate metrics, while user-query recall remains diagnostic-only |
| Operator CLI search remains additive to MCP search-serving behavior | Preserved with clarified scope | The new rooted CLI search tool is additive, uses the approved rooted-tree boundary plus `lexongraph-search`, and does not replace the MCP surface |
| Clients are not forced to parse raw mailbox blobs for ordinary retrieval | Preserved | Indexed chunks must reference normalized email artifacts so retrieval can stay at chunk level or expand to full normalized email through repository-owned artifacts |
| Storage abstraction count stays bounded across environments | Preserved | Requirements now reuse the environment-selected `BlockStore` abstraction family for indexed blocks, normalized email artifacts, and mailbox provenance artifacts rather than introducing a second storage stack |
| Local filesystem block stores remain interoperable with LexonGraph tooling | Preserved | The local/testing profile is now constrained to LexonGraph's filesystem naming/layout contract so inspection tools can consume repository-produced local stores |
| Parallel execution does not weaken deterministic indexing semantics | Preserved | Leaf-layer concurrency is constrained by cross-layer barriers and idempotence requirements so scheduling policy does not become a semantic contract change |

## Open Questions / Discovery Gaps

- **Q-INDEXER-061 [UNKNOWN]:** After `0.7.0` becomes the only approved v3-backed
  profile in this increment, should future increments widen the v3 surface to
  additional published profiles, or keep non-`0.7.0` selections on the
  existing non-v3 path until each profile is explicitly approved?
- **Q-INDEXER-062 [UNKNOWN]:** Does the latest upstream v3 status-observer
  contract expose enough information for LexonArchiveBuilder to preserve its
  current replay-submission handoff and long-running liveness messages without
  weakening operator visibility?
- **Q-INDEXER-063 [UNKNOWN]:** Are any repository-required split-stage replay
  guarantees materially constrained by the v3 leaf-block-id ingestion boundary,
  or does the existing immutable replay-audit journal contract remain
  sufficient for all approved execution stages?
- **Q-INDEXER-064 [UNKNOWN]:** Does the newest upstream v3 telemetry contract
  intend `item_count` to remain invocation-total for all relevant events, or
  should LexonArchiveBuilder expect phase-local and layer-local count semantics
  to remain distinct across hierarchy-planning, partition-load, and bottom-up
  assembly updates?
- **Q-INDEXER-065 [UNKNOWN]:** After the approved intra-block dispersion, sibling-centroid separation, PCA-axis strength, quantile-occupancy, and parent-child dispersion-delta metrics land, which additional quantitative embedding-space shape measures would improve the rooted quality signal in a future increment without overfitting too early?
- **Q-INDEXER-066 [UNKNOWN]:** Future increments may revisit whether any rooted-quality heuristics beyond hard structural violations should influence process exit status, but this increment keeps the heuristic inversion count and layer statistics advisory-only.
- **Q-INDEXER-067 [UNKNOWN]:** Beyond the required query text, embedding endpoint, root, and `k`, does the rooted CLI search tool need repository-approved filters, score thresholds, or output-field selection in this increment?
- **Q-INDEXER-068 [UNKNOWN]:** Should the rooted CLI search tool treat the caller-supplied embedding endpoint as the complete query-embedding configuration, or must it also accept repository-specific embedding-spec inputs such as dimensions or encoding overrides at the CLI boundary?
- **Q-INDEXER-069 [UNKNOWN]:** For corpus-based TNN-recall histograms, should a future increment keep repository-owned default histogram buckets or expose bucket configuration as an operator-visible parameter?
- **Q-INDEXER-070 [UNKNOWN]:** Does published profile `0.7.0` on the constrained
  v3 path preserve repository-acceptable tree-shape, retrieval-quality, and
  bounded-memory behavior across the expected corpus sizes, or will a later
  increment need additional approved published profiles or broader v3 support
  for materially different workloads?
- **Q-INDEXER-071 [UNKNOWN]:** What size-oriented threshold or equivalent entry budget should trigger publication of the next immutable replay-audit journal block?
- **Q-INDEXER-073 [UNKNOWN]:** Should current-root publication and replay-journal head publication share one already approved repository-wide reference-store artifact family, or must this increment define separate concrete mutable-reference artifacts under the same design class?
- **Q-INDEXER-072 [UNKNOWN]:** Should the first `0.6.x` evaluation sweep run only the `0.6.x` series, or should the runnable `test.ps1` preserve an in-band `0.5.x` comparison baseline in the same invocation?
- **Q-INDEXER-074 [UNKNOWN]:** Should a future increment make the rooted-query RTT-cost model configurable for different assumed congestion windows or transport regimes, or is the fixed 64 KiB model sufficient for the repository-owned diagnostic surface?
- **Q-INDEXER-075 [UNKNOWN]:** If future block-store implementations add stronger client-side caching or prefetch semantics, should rooted-query access accounting remain a logical uncached traversal measure, or should a later increment add a second cache-aware metric family?
- **Q-INDEXER-076 [UNKNOWN]:** After the first `0.7.0` ladder lands with default budget `1024`, should future ladders keep one repository-approved default rung table or expose budget-and-rung selection as an operator-editable input on the same local/testing automation surface?
- **Q-INDEXER-077 [UNKNOWN]:** Should a future block-copy increment also move repository-owned mutable references such as current-root and replay-journal-head publication, or should that remain a separate explicit operator workflow even after immutable rooted-block copying exists?
- **Q-INDEXER-078 [UNKNOWN]:** If a later increment adds `--verbose` or equivalent richer diagnostics to rooted block copy, what additional per-block or per-phase detail would be useful without overwhelming ordinary operator workflows?
- **Q-INDEXER-079 [UNKNOWN]:** Should a future increment add repository-documented recommended `RUST_LOG` filter presets for common debugging cases such as Azure Table transport, retry behavior, or HTTP wire visibility, or is raw operator-selected filtering sufficient?
- **Q-INDEXER-080 [UNKNOWN]:** Should a future rooted-copy increment expose backend-specific blind-write optimizations more granularly, or is one repository-wide opt-in mode sufficient as long as the default preserves exact copied-versus-skipped accounting?
- **Q-INDEXER-081 [UNKNOWN]:** After the first bounded-write increment lands with default limit `64`, should a future increment keep one shared repository-wide destination-write default across block-store backends, or allow backend-specific recommended defaults while preserving the same CLI surface?
- **Q-INDEXER-082 [UNKNOWN]:** After approving bounded repository-owned
  replay-order externalization for issue #83, should a future increment keep
  the strategy fully automatic under the existing memory-budget contract, or
  expose an operator-visible threshold or mode selector?
- **Q-INDEXER-083 [UNKNOWN]:** For the preferred dedicated pass-end convergence
  telemetry sink, should the first approved realization default to a sibling
  file beside the request or summary artifacts, a distinct process output
  stream, or allow either as long as operators can discover it deterministically?
- **Q-INDEXER-084 [UNKNOWN]:** Should the first approved user-usable
  convergence-diagnosis surface emit a repository-owned discrete verdict such
  as `converging`, `not-converging`, or `inconclusive`, or should it stop at
  surfacing comparative evidence plus blocked-on state for human judgment?
- **Q-INDEXER-085 [UNKNOWN]:** For post-run diagnosis of non-converged runs, is
  it preferable to append one terminal diagnosis record into the existing
  planning-telemetry artifact, or to emit a separate sibling convergence
  summary artifact as long as it stays deterministic and request-adjacent?
## Coverage Notes

- **Covered sources [KNOWN]:**
  - user request in this session: "why are we reading the target first? Just attempt a write."
  - user request in this session: "add a mode that skips the read and the better stats (just copies everything)"
  - user clarification in this session selecting: "Keep the current read-before-write behavior as default, and add an opt-in blind-write mode (Recommended)"
  - user clarification in this session selecting: "Report only attempted writes and failures; drop exact skipped-already-present accounting in that mode (Recommended)"
  - user request in this session: "can we modify the copy to be async and keep say 64 (or some number) of azure writes in flight?"
  - user clarification in this session selecting: "Expose a CLI flag with default 64 (Recommended)"
  - user clarification in this session selecting: "Apply to both modes when a destination write is needed (Recommended)"
  - user request in this session: "does lexongraph / azure sdk have any rust tracing we can enable to see why it's happening?"
  - user request in this session: "ok, can we modify lexonarchivebuilder-indexer to make this work?"
  - user clarification in this session selecting: "Respect `RUST_LOG` automatically with no new CLI flag (Recommended)"
  - user request in this session: "switch to lexongraph at 7c8f375137375709bb608ee2609b38cb80e5422c and switch to the new v3 indexing api."
  - user clarification in this session selecting: "Keep non-`0.7.0` profiles on the existing non-v3 path (Recommended)"
  - `Alan-Jowett/LexonGraph` `crates/lexongraph-streaming-indexer/src/v3.rs` at `7c8f375137375709bb608ee2609b38cb80e5422c`: constrained v3 run construction, leaf-block-id ingestion, temporary working-root contract, hierarchy-planning telemetry, partition-load telemetry, and bottom-up assembly telemetry
  - `Alan-Jowett/LexonGraph` `docs/specs/rust-streaming-indexer-crate/requirements.md` at `7c8f375137375709bb608ee2609b38cb80e5422c`: constrained v3 working-root and leaf-block-id input requirements
  - user clarification in this session selecting: "Enable it for the entire indexer binary (Recommended)"
  - user request in this session: "can we ammend the tool to print some indication that it is working, maybe a --verbose mode or somethign?"
  - user clarification in this session selecting: "Always show basic progress/liveness (Recommended)"
  - user request in this session: "clean up the dead spec/code that is unrelated to the new profile version based path. It has left over stuff from the previous path where we tried to define it at this layer."
  - user request in this session: "the upstream LexonGraph API has evolved to allow either divisive or aggregation based clustering. We need to expose this as an option at this layer as well"
  - user clarification in this session: "I think it is important to both. Aggregate should be the default with an option to try out divisive (but I suspect that won't be interesting)"
  - user request in this session: "fix LexonArchiveBuilder to work with fixed memory budget when data size exceeds memory"
  - user clarification in this session selecting: "Both full-pipeline and clustering-only must stay within a caller-configurable memory budget, and spilling/replay to local disk or BlockStore is acceptable (Recommended)"
  - user clarification in this session: "both, but prefer not to spill to storage unless we can prove its unavoidable. Try realy not to spill"
  - user clarification in this session: "hold on. If it's only 10 million block ids and they 32 bytes each, we can easily keep it in memory, no need to spill that."
  - user clarification in this session: "I think the correct behavior is this:
    1) Walk the replay list gathering all the block ids
    2) Sort the result list of block ids and dedupe
    3) Use this as our deterministic order in which we classify and finalize block

    actual block state (embedding etc) is pulled from the block store on demand as we process them and is not cached

    No need for sqlite / spilling / external storage at all.

    This is the simplest fix"
  - user clarification in this session selecting: "Preserve existing MCP/search behavior exactly (Recommended)"
  - user clarification in this session selecting: "Uniform content-type-agnostic control (Recommended)"
  - user clarification in this session selecting: "Yes, keep the same contract and default across environments (Recommended)"
  - user clarification in this session selecting: "Yes, that is the acceptance target (Recommended)"
  - user request in this session: "lexongraph now has a v2 of the block format. Switch over to using that instead of the v1 format."
  - user clarification in this session selecting: "Require rebuilding stores and support only v2 blocks"
  - user request in this session: "currently the indexing uses an append only file journal of changes, but this doesn't really match the overall way in which this project works. Instead, the replay journal should be a full audit journal documenting each step in the embedding and indexing process, but as a series of immutable blocks."
  - user request in this session: "The block then form a tree immutable journal/audit entries. Because each block is identified by hash and points to the parent journal entry by hash it forms a merkle tree."
  - user clarification in this session selecting: "Use the immutable block-backed journal in both local/testing and production-oriented environments, with the same shared BlockStore boundary (Recommended)"
  - user clarification in this session: "The goal is to get rid of scanning entirely. It should solely use the replay / audit journal"
  - user clarification in this session: "I think we can probably make it block size based? Once the current journal block has more than X entries, create and publish it. The goal is a tradeoff between creating too many blocks and redoing to much work."
  - user clarification in this session: "I beleive we already had some sort of mutable ref store planned for root blocks? Same deal for replay journal"
  - user feedback in this session: "Is there requirement that the journal is detailed enough to audit what actions where performed (i.e. inputs, action, generated blocks)?"
  - user request in this session: "do any of the specs already cover a mutable ref for the root block produced by indexing? This builds on that, but if it's not present we need to add it as well"
  - user request in this session: "another requirement: Add an option to allow the user to provide a text string and an embedding endpoint, then generate an embedding, search using the lexongraph-search api, and return the top k matching leaf nodes. The MCP server already does something similar, but I want an easy cli tool to do it as well"
  - user clarification in this session selecting: "Caller-supplied root/tree (Recommended)"
  - user clarification in this session selecting: "Human-readable results plus machine-readable JSON output (Recommended)"
  - user request in this session: "We need a tool that given a block store and root and measure the quality / correctness of the block tree. This would include heuristics like children always have lower level than parents. Distance from centroid of embeddings in parent is the same or bigger than distance from centroid if embeddings in child (i.e. children span a smaller part of the embedding space than their parents). It would also be useful to gain a quantifiable measure of the quality of how the space is divided up (i.e. the shape that each block represents in teh embedding space)."
  - user clarification in this session selecting: "CLI-only operator tool (Recommended)"
  - user clarification in this session selecting: "Human-readable summary plus machine-readable JSON artifact (Recommended)"
  - user request in this session: "I need a tool to allow us to copy blocks between two block stores. i.e. from file system -> azure storage table sdk block store"
  - user clarification in this session selecting: "CLI-only operator tool (Recommended)"
  - user clarification in this session selecting: "Caller-selected root(s) and reachable blocks only (Recommended)"
  - user clarification in this session: "just to clarify, lexongraph already has these block stores, this would just be a layer on top of that"
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
  - user request in this session: "we need to improve the quality tool. I want it to report: stats on blocks touched per level and total, per query it should include number of blocks and size of blocks read it should also give an \"estimated\" query time (in rtt), it assumes a cwnd of 64k, with query time as: per layer data per layer / cwnd (rounded up) summarized into a total per query?"
  - user clarification in this session selecting: "Reachable embeddings under the supplied root (Recommended)"
  - user request in this session: "we now need to design a ladder experiment that tries a combination of beam width and cluster size, using v0.7.0. profile, with the constraint that beam width * cluster size remains constant"
  - user request in this session: "Use the skill tool to invoke the \"evolve\" skill, then follow the skill's instructions to help with: build this as a test script/plan we can execute."
  - user clarification in this session: "it's not block_size_target that determines block size, but embeddings per cluster. I think it was 64 in the last run?"
  - user clarification in this session selecting: "Yes, use 1024 as the default ladder budget (Recommended)"
  - user clarification in this session selecting: "Yes, use that 5-rung default ladder (Recommended)"
  - user request in this session: "LexonGraph crate has been updated and now has a simpler higher level API that groups options into a versioned profile. Please switch to this API and use the v0.1.0 profile"
  - user clarification in this session selecting: "Replace the external control surface with profile-based v0.1.0 (Recommended)"
  - user request in this session: "LexonGraph has switched to exposing a versioned indexing profile. Currently we hard-code to v0.1.0 (I think). Make this an option we can test different profiles. Can we also pin to main of LexonGraph for now with an explicit note that this is so we can quickly test new profiles?"
  - user request in this session: "can we start prepareing this repo to work with the new v2 api and default to v0.7.0 profile (the only one supported by the new v2 api)"
  - user clarification in this session selecting: "make v0.7.0 the default. Use v2 only if v0.7.0 is selected."
  - user request in this session: "Use the skill tool to invoke the \"evolve\" skill, then follow the skill's instructions to help with: add logging for per-pass telemetry, preferably to a seperate file / stream to make it easily findable. I want to be able to tell that after N passes we are converging or not? It should also clearly log the contract and profile version we are using (i.e. api version 1 or 2 and profile version 0.x.y)."
  - user request in this session: "fix this bug"
  - user clarification in this session selecting: "Replay until planning completes or an upstream/runtime error occurs (Recommended)"
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
  - `Alan-Jowett/LexonGraph` compare `00760dd5121466b7f089bd22d3a26d8d23aa61b6...75c936d2d5bf3c8ae7afa6df598e433a731e0c3c`: upstream change "Add v2 intra-pass telemetry (#179)" covering pass progress, pending partition detail, trainer subphase summaries, and suspected-stall indicators
  - user request in this session: "LexonGraph now exposes a much richer set of telemetry with the goal of helping the caller determine if it is converging or not and if not what it is blocked on. This project needs to surface this in a way a user can use to diagnose convergence failures."
  - `README.md:18-27`
  - `README.md:42-49`
  - `README.md:51-59`
  - `docs/specs/lexonarchivebuilder-indexer/design.md:670-782`
  - `docs/specs/lexonarchivebuilder-indexer/validation.md:877-934`
  - `crates/lexonarchivebuilder-indexer/src/runtime.rs:1112-1367`
  - `crates/lexonarchivebuilder-indexer/src/runtime.rs:1418-1452`
  - `crates/lexonarchivebuilder-indexer/src/runtime.rs:5189-5218`
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
  - user request in this session: "update to latest lexongraph and adopt it's
    new feature for out of core spilling of planning data"
  - user clarification in this session: "see commit
    858ed455ea0828909aea38a0f2e677cca917ae76"
  - user clarification in this session selecting: "Derive it automatically from existing request/output paths"
  - user request in this session: "fix #83. Goal is that memory usage should not scale with size of corpora"
  - user clarification in this session selecting: "Allow bounded repository-owned externalized state when needed (Recommended)"
- **Excluded for now [KNOWN]:**
  - Detailed Rust implementation file paths, crate manifests, Docker assets, and test artifacts, because this requirements document captures the semantic contract and leaves implementation realization to downstream design, validation, and code-review artifacts
  - Exact normalized email CBOR schema, exact duplicated chunk metadata list, and the specific chunking library choice, because those belong to downstream design and validation artifacts rather than requirements
  - The precise log-line schema, exact artifact filename or stream handle, and
    per-item verbosity throttling policy for progress output, because those
    belong to downstream design and validation artifacts rather than
    requirements once the additive dedicated-sink preference itself is fixed
  - The exact bounded-work-unit choice or elapsed-time threshold for embedding-phase progress updates, because that belongs to downstream design and validation artifacts so long as the approved requirements-level no-silent-gap contract is preserved
  - The exact mapping from repository stage modes to concrete upstream streaming pass counts, replay batching, and training-completion timing, because those belong to downstream design and validation artifacts rather than requirements
  - The exact configuration surface for the administrator-defined concurrency cap and the exact physical-CPU detection algorithm in containerized or quota-constrained environments, because those belong to downstream design and validation artifacts rather than requirements
  - The precise block-kind predicate used inside the upstream LexonGraph block-iteration API to determine clustering eligibility, because this requirements document constrains LexonArchiveBuilder to the upstream iteration contract without redefining LexonGraph-owned block semantics
  - The exact field names, serialization shape, and any future operator-visible selector shape for the published-profile contract, because those choices belong to downstream design and validation artifacts so long as they preserve the approved default `0.7.0` behavior and explicit version-selection boundary
  - The exact formulas, thresholds, weighting model, and exit-code policy for quantitative block-tree quality heuristics, because those choices belong to downstream design and validation artifacts so long as the approved distinction between hard structural findings and advisory quality evidence is preserved
  - The exact rooted CLI search flag names, result-field schema, score formatting, and default artifact location, because those choices belong to downstream design and validation artifacts so long as the approved rooted search scope, `lexongraph-search` boundary, and dual-output contract are preserved

### BA-INDEXER-132

- **Before [KNOWN]:** The requirements pinned the approved latest-LexonGraph
  integration target to commit `7c8f375137375709bb608ee2609b38cb80e5422c` and
  did not require another immediate refresh if later upstream `main` changes
  broke the constrained v3 integration boundary again.
- **After [KNOWN]:** The requirements now refresh that approved upstream target
  to the current LexonGraph `main` revision and require LexonArchiveBuilder to
  adapt any resulting breaking delegated indexing API changes while preserving
  the approved external stage contract, unchanged MCP search or retrieval
  behavior for already-indexed content, and the current operator-visible v3
  progress, telemetry, and diagnosis semantics unless a true upstream
  capability regression must be surfaced explicitly.
