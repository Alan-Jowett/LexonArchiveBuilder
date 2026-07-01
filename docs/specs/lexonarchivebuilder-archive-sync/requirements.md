<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder Archive Sync Requirements

## Document Status

- **Phase:** Phase 2 - Specification Changes
- **Status:** Approved requirements revision being propagated into design and validation for Azure-backed rsync snapshot acquisition through the updated LexonGraph Azure Blob-backed block storage, plus v2 custom-block adoption for source-snapshot artifacts
- **Scope:** `lexonarchivebuilder-archive-sync` as a separate production workflow layered on top of existing LexonArchiveBuilder indexing and storage boundaries, including source-snapshot acquisition and v2 custom-block alignment for source-snapshot artifacts

## USER-REQUEST

- **UR-ARCHIVE-1 [KNOWN]:** Create a spec trifecta for a new workflow under a new tool boundary.
- **UR-ARCHIVE-2 [KNOWN]:** The workflow should be called `lexonarchivebuilder-archive-sync`.
- **UR-ARCHIVE-3 [KNOWN]:** The requested operator entrypoint is a Docker Compose YAML.
- **UR-ARCHIVE-4 [KNOWN]:** The workflow must start automatically on machine boot.
- **UR-ARCHIVE-5 [KNOWN]:** The workflow must run `rsync` over `rsync.ietf.org::mailman-archive/` into Azure Blob Storage.
- **UR-ARCHIVE-6 [KNOWN]:** The workflow must insert each mailbox as a block through a `BlockStore` trait implementation when that block is not already present.
- **UR-ARCHIVE-7 [KNOWN]:** For each new mailbox block, the workflow must chunk the mailbox, generate any new chunks, and store those chunks through the same `BlockStore` trait family.
- **UR-ARCHIVE-8 [KNOWN]:** For each new chunk, the workflow must generate an embedding.
- **UR-ARCHIVE-9 [KNOWN]:** The workflow must recompute the index tree.
- **UR-ARCHIVE-10 [KNOWN]:** The workflow must append each new root block to a JSON file stored in Azure Blob Storage.
- **UR-ARCHIVE-11 [KNOWN]:** The workflow must shut the VM down.
- **UR-ARCHIVE-12 [KNOWN]:** The workflow must be resumable by using a journal that records the current step and the blocks that still require chunking, embedding, or indexing.
- **UR-ARCHIVE-13 [KNOWN]:** Index recomputation should trigger only after all required blocks are embedded.
- **UR-ARCHIVE-14 [KNOWN]:** The workflow must be compatible with spot-instance VMs, so forced shutdown and later restart can resume from a checkpoint.
- **UR-ARCHIVE-15 [KNOWN]:** This should be a new tool or workflow, but it may leverage the existing `lexonarchivebuilder-indexer`, `rsync`, and other tools as needed.
- **UR-ARCHIVE-16 [KNOWN]:** Changes to `lexonarchivebuilder-indexer` are acceptable when needed to support this workflow.
- **UR-ARCHIVE-17 [KNOWN]:** For now, only the spec trifecta is requested.
- **UR-ARCHIVE-18 [KNOWN]:** The first specification increment should treat this as a production-only workflow rather than requiring a local/testing entrypoint.
- **UR-ARCHIVE-19 [KNOWN]:** On non-recoverable failure, the workflow should still shut the VM down rather than leaving it running.
- **UR-ARCHIVE-20 [INFERRED]:** MCP search and retrieval semantics should remain unchanged; this workflow is about production ingestion and publication, not search-serving changes.
- **UR-ARCHIVE-21 [INFERRED]:** Production execution should remain aligned with the repository's Azure Blob Storage plus Azure OpenAI direction rather than introducing a second production storage or embedding model for this increment.
- **UR-ARCHIVE-22 [KNOWN]:** The workflow journal should serve not only resumption but also auditing.
- **UR-ARCHIVE-23 [KNOWN]:** A repeated workflow run should either reproduce the same root block or provide enough audit evidence to explain why the root changed.
- **UR-ARCHIVE-24 [KNOWN]:** The workflow needs a formal source snapshot identity so repeated runs can prove what source corpus was indexed.
- **UR-ARCHIVE-25 [KNOWN]:** Root-history entries must contain provenance metadata rather than only the root identifier.
- **UR-ARCHIVE-26 [KNOWN]:** The requirements should avoid hard-coding `recompute index tree` as the only acceptable publication strategy.
- **UR-ARCHIVE-27 [KNOWN]:** The term `work set` should be formally defined.
- **UR-ARCHIVE-28 [KNOWN]:** Each workflow execution should have a stable generation identifier recorded across journal, publication, and failure artifacts.
- **UR-ARCHIVE-29 [KNOWN]:** Checkpoint semantics should guarantee that successfully checkpointed mailbox, chunk, embedding, and publication work does not need to be re-executed.
- **UR-ARCHIVE-30 [KNOWN]:** The workflow should explicitly preserve immutable mailbox blocks, chunk blocks, embeddings, index blocks, and root artifacts.
- **UR-ARCHIVE-31 [KNOWN]:** Every successful generation should append a root-history entry even when the root repeats, so periods of stability remain visible in the audit trail.
- **UR-ARCHIVE-32 [KNOWN]:** Source snapshot identity should be reproducibly bound to the effective source corpus rather than acting only as an execution identifier.
- **UR-ARCHIVE-33 [KNOWN]:** The specification should define effective indexing configuration identity rather than referring to it implicitly.
- **UR-ARCHIVE-34 [KNOWN]:** Root-history publication needs explicit durability or repair semantics when history append and publication are not one physical storage operation.
- **UR-ARCHIVE-35 [KNOWN]:** The archive-sync journal should be authoritative at workflow-stage boundaries relative to any subordinate indexer replay journals.
- **UR-ARCHIVE-36 [KNOWN]:** The work set participating in one publication generation should be fixed before published-root generation begins.
- **UR-ARCHIVE-37 [KNOWN]:** Source snapshot provenance should preserve enough information to identify the mirrored corpus, not merely a generated snapshot identifier.
- **UR-ARCHIVE-38 [KNOWN]:** `lexonarchivebuilder-archive-sync` now needs Azure-backed rsync snapshot acquisition.
- **UR-ARCHIVE-39 [KNOWN]:** LexonGraph has an updated block storage implementation that works over Azure Blob Storage.
- **UR-ARCHIVE-40 [KNOWN]:** The rsync snapshot payloads and manifests should use the updated Azure-backed LexonGraph `BlockStore` realization rather than a separate workflow-specific Azure Blob storage path.
- **UR-ARCHIVE-41 [KNOWN]:** This increment should remain production-only and should not change MCP behavior.
- **UR-ARCHIVE-42 [KNOWN]:** The source-snapshot boundary should stay reusable for future content types and future source types rather than remaining mailbox-source-specific.
- **UR-ARCHIVE-43 [KNOWN]:** LexonGraph now has a v2 block format with custom-block support, and `lexonarchivebuilder-archive-sync` should use it for source-snapshot payload and manifest blocks.
- **UR-ARCHIVE-44 [KNOWN]:** It is acceptable for this transition to require rebuilt source-snapshot stores; continued read compatibility with pre-v2 v1 source-snapshot blocks is not required in this increment.
- **UR-ARCHIVE-45 [INFERRED]:** The workflow's resumability, auditability, and publication contracts should be preserved across the source-snapshot v2 transition while downstream mailbox, chunk, embedding, and index flows remain on their existing delegated contracts in this increment.

## Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-ARCHIVE-001 | Add | Introduce a new production workflow specification package for `lexonarchivebuilder-archive-sync` as a boundary separate from the indexer and MCP server | UR-ARCHIVE-1, UR-ARCHIVE-2, UR-ARCHIVE-15, UR-ARCHIVE-17 |
| CM-ARCHIVE-002 | Add | Define Docker Compose as the workflow entrypoint and require boot-time automatic start compatibility | UR-ARCHIVE-3, UR-ARCHIVE-4 |
| CM-ARCHIVE-003 | Add | Define the first production source as `rsync.ietf.org::mailman-archive/` acquired as an Azure Blob-backed durable source snapshot | UR-ARCHIVE-5 |
| CM-ARCHIVE-003A | Add | Require a formal source snapshot identity for the effective source corpus used by each publication generation | UR-ARCHIVE-24 |
| CM-ARCHIVE-003B | Add | Require source snapshot identities to be reproducibly bound to the effective mirrored corpus and backed by explicit provenance | UR-ARCHIVE-32, UR-ARCHIVE-37 |
| CM-ARCHIVE-003C | Revise | Require rsync snapshot payloads and manifests to be durably acquired through the updated Azure-backed LexonGraph `BlockStore` realization instead of a separate workflow-specific Azure Blob storage path | UR-ARCHIVE-38, UR-ARCHIVE-39, UR-ARCHIVE-40 |
| CM-ARCHIVE-003D | Revise | Preserve the source-snapshot boundary as a reusable acquisition contract for future source and content types while keeping the first increment focused on IETF mailman mailboxes | UR-ARCHIVE-42 |
| CM-ARCHIVE-004 | Add | Require idempotent mailbox block persistence through the existing `BlockStore` abstraction family | UR-ARCHIVE-6, UR-ARCHIVE-15 |
| CM-ARCHIVE-005 | Add | Require chunk derivation and chunk persistence only for newly admitted mailbox blocks | UR-ARCHIVE-7 |
| CM-ARCHIVE-006 | Add | Require embedding generation only for newly admitted chunks | UR-ARCHIVE-8 |
| CM-ARCHIVE-007 | Revise | Gate published-root generation on completion of all required embedding work without fixing one implementation strategy | UR-ARCHIVE-9, UR-ARCHIVE-13, UR-ARCHIVE-26, UR-ARCHIVE-27 |
| CM-ARCHIVE-007A | Add | Require each publication generation's work set to be fixed before published-root generation begins | UR-ARCHIVE-27, UR-ARCHIVE-36 |
| CM-ARCHIVE-008 | Revise | Require append-only root publication to a JSON artifact in Azure Blob Storage with generation and provenance metadata per successful generation | UR-ARCHIVE-10, UR-ARCHIVE-25, UR-ARCHIVE-31 |
| CM-ARCHIVE-008A | Add | Require effective indexing configuration identity and explicit history-durability semantics for successful publication generations | UR-ARCHIVE-33, UR-ARCHIVE-34 |
| CM-ARCHIVE-009 | Add | Require a durable resume journal that records stage, work inventories, and checkpoint-safe progress | UR-ARCHIVE-12, UR-ARCHIVE-14 |
| CM-ARCHIVE-009A | Add | Require the journal to double as an audit artifact that explains root reproducibility or root drift across repeated runs | UR-ARCHIVE-22, UR-ARCHIVE-23 |
| CM-ARCHIVE-009B | Add | Require a stable generation identifier and durable checkpoint granularity across workflow artifacts | UR-ARCHIVE-28, UR-ARCHIVE-29 |
| CM-ARCHIVE-009C | Add | Require the workflow journal to remain authoritative at workflow-stage boundaries even when subordinate indexer journals exist | UR-ARCHIVE-35 |
| CM-ARCHIVE-010 | Add | Require VM shutdown on terminal success and terminal non-recoverable failure | UR-ARCHIVE-11, UR-ARCHIVE-19 |
| CM-ARCHIVE-011 | Add | Preserve compatibility with spot-instance eviction by requiring resume from durable checkpoints instead of in-memory state | UR-ARCHIVE-12, UR-ARCHIVE-14 |
| CM-ARCHIVE-012 | Add | Preserve indexer reuse and allow targeted `lexonarchivebuilder-indexer` evolution without moving this workflow into the indexer boundary | UR-ARCHIVE-15, UR-ARCHIVE-16 |
| CM-ARCHIVE-013 | Add | Constrain the first increment to production-only Azure-oriented execution while leaving local/testing concerns out of scope | UR-ARCHIVE-18, UR-ARCHIVE-21 |
| CM-ARCHIVE-014 | Add | Define the first production runtime shape as a VM-hosted, boot-triggered, Compose-launched workflow compatible with spot-instance semantics | UR-ARCHIVE-3, UR-ARCHIVE-4, UR-ARCHIVE-11, UR-ARCHIVE-14, UR-ARCHIVE-18 |
| CM-ARCHIVE-015 | Revise | Preserve search-serving separation, immutable artifact behavior, and future content extensibility while introducing mailbox-focused workflow stages now | UR-ARCHIVE-20, UR-ARCHIVE-21, UR-ARCHIVE-30 |
| CM-ARCHIVE-016 | Revise | Adopt LexonGraph v2 custom blocks for source-snapshot payloads and manifests while leaving downstream mailbox, chunk, embedding, and index flows on their existing delegated contracts | UR-ARCHIVE-43, UR-ARCHIVE-44, UR-ARCHIVE-45 |

## Before / After

### BA-ARCHIVE-001

- **Before [KNOWN]:** The repository has indexer and local scale-test specifications, but no production workflow specification for rsync-driven mailbox mirroring, resumable block admission, and root publication on Azure-backed spot VMs.
- **After [KNOWN]:** The repository has an explicit requirements baseline for `lexonarchivebuilder-archive-sync` under `docs/specs/lexonarchivebuilder-archive-sync/requirements.md`.

### BA-ARCHIVE-002

- **Before [KNOWN]:** Production direction mentions Azure Blob Storage and Azure OpenAI at the architecture level, but does not define an automated archive-sync workflow that starts on VM boot.
- **After [KNOWN]:** The requirements define a production-only Docker Compose workflow that is compatible with boot-triggered execution on a VM.

### BA-ARCHIVE-003

- **Before [KNOWN]:** Rsync-backed mailbox acquisition is currently described only as a local scale-test wrapper concern.
- **After [KNOWN]:** The first production workflow increment explicitly acquires `rsync.ietf.org::mailman-archive/` as an Azure Blob-backed durable source snapshot before downstream mailbox admission and indexing work begins.

### BA-ARCHIVE-004

- **Before [KNOWN]:** The repository does not yet specify a workflow-level requirement that raw mailbox artifacts themselves be admitted into the shared `BlockStore` family before chunking and embedding decisions are made.
- **After [KNOWN]:** The workflow must admit mailbox artifacts as blocks when absent, then derive chunk and embedding work only from newly admitted mailbox blocks.

### BA-ARCHIVE-005

- **Before [KNOWN]:** Existing requirements describe split-stage indexing and replay journaling for local/testing indexer execution, but not a workflow journal that coordinates rsync mirroring, mailbox admission, chunking, embedding, index publication, and restart after spot eviction.
- **After [KNOWN]:** The workflow requirements define a durable journal and checkpoint contract across all production stages.

### BA-ARCHIVE-006

- **Before [KNOWN]:** The repository does not specify a workflow-level barrier preventing published-root generation until all newly required embeddings are durably present.
- **After [KNOWN]:** Published-root generation is explicitly gated on embedding completeness for the pending work set.

### BA-ARCHIVE-007

- **Before [KNOWN]:** Root publication for new production indexing runs is not defined as an append-only JSON history in Azure Blob Storage.
- **After [KNOWN]:** Each successful publication generation must append a new root entry to a JSON artifact stored in Azure Blob Storage.

### BA-ARCHIVE-008

- **Before [KNOWN]:** VM shutdown behavior after workflow completion or terminal failure is not specified at the repository workflow layer.
- **After [KNOWN]:** The workflow must shut the VM down after terminal success and after terminal non-recoverable failure.

### BA-ARCHIVE-009

- **Before [KNOWN]:** Resume state was the only explicit journal concern, so repeated runs could lack repository-defined evidence for why the published root matched or changed.
- **After [KNOWN]:** The workflow journal is also an audit artifact that must support root reproducibility checks and explain root drift across repeated runs.

### BA-ARCHIVE-010

- **Before [KNOWN]:** The workflow requirements did not formally define source snapshot identity, generation identity, or work-set identity boundaries for repeated-run auditability.
- **After [KNOWN]:** The requirements explicitly define source snapshot identity, generation identity, and work set semantics so published roots and journal records are tied to a specific effective corpus and execution generation.

### BA-ARCHIVE-011

- **Before [KNOWN]:** Root publication could have been interpreted as a bare root identifier append with no required provenance and no requirement to record repeated stable generations.
- **After [KNOWN]:** Every successful generation appends a provenance-rich root-history entry that identifies the source snapshot, generation, effective indexing configuration, publication timestamp, and audit linkage even when the root repeats.

### BA-ARCHIVE-012

- **Before [KNOWN]:** Checkpoint durability and immutability expectations were implicit rather than stated as workflow requirements.
- **After [KNOWN]:** Successfully checkpointed committed work must not require re-execution, and previously published mailbox, chunk, embedding, index, and root artifacts remain immutable.

### BA-ARCHIVE-013

- **Before [KNOWN]:** The requirements required Azure-backed rsync mirroring but left room for a workflow-specific Azure Blob storage path separate from the production `BlockStore` realization used for downstream immutable artifacts.
- **After [KNOWN]:** The requirements now direct rsync snapshot payloads and manifests through the updated Azure-backed LexonGraph `BlockStore` realization so source acquisition shares the same stable production storage boundary as downstream artifact persistence.

### BA-ARCHIVE-014

- **Before [KNOWN]:** The source-snapshot boundary preserved future extensibility in general terms, but it did not explicitly state that the acquisition contract itself should remain reusable beyond the first mailbox source.
- **After [KNOWN]:** The requirements explicitly preserve a reusable source-snapshot acquisition boundary for future content and source types while keeping the first increment mailbox-focused and production-only.

### BA-ARCHIVE-015

- **Before [KNOWN]:** The requirements bound source-snapshot persistence to the shared `BlockStore` family but did not state whether source-snapshot payloads and manifests should continue using v1-style wrappers or move to v2 custom blocks.
- **After [KNOWN]:** The requirements now bind source-snapshot payloads and manifests to LexonGraph v2 custom blocks, explicitly allow rebuilt source-snapshot stores instead of continued read compatibility with pre-v2 v1 snapshot blocks, and leave downstream mailbox, chunk, embedding, and index flows on their current delegated contracts in this increment.

## Glossary

### Source Snapshot Identity

The durable identity reproducibly bound to one effective source corpus produced
by a source-acquisition operation.

When the effective source corpus is unchanged, the same source snapshot identity
is required.

### Generation Identity

The stable identity assigned to one workflow execution or publication
generation.

### Effective Indexing Configuration Identity

The durable identity of the effective chunking, embedding, delegated published-
root generation, and other root-affecting indexing inputs that participate in
one publication generation.

### Work Set

The complete set of source artifacts, chunk artifacts, embeddings, and index
updates that participate in one publication generation.

### Source Artifact

The normalized workflow input artifact family admitted into the publication
pipeline. In the first increment, mailbox artifacts are the only required source
artifact class, but future source artifacts may include RFCs, Internet Drafts,
Datatracker metadata, and Working Group metadata.

## Requirements

### Functional Requirements

#### RQ-ARCHIVE-001 - Workflow boundary

LexonArchiveBuilder SHALL provide a separate production workflow named
`lexonarchivebuilder-archive-sync`.

- **Boundary [KNOWN]:** This workflow is not the MCP server and is not itself the `lexonarchivebuilder-indexer` crate.
- **Reuse intent [KNOWN]:** The workflow may orchestrate existing repository tools, including `lexonarchivebuilder-indexer`, `rsync`, and shared storage or embedding adapters.
- **Traceability:** UR-ARCHIVE-1, UR-ARCHIVE-2, UR-ARCHIVE-15, UR-ARCHIVE-16

#### RQ-ARCHIVE-002 - Docker Compose operator entrypoint

`lexonarchivebuilder-archive-sync` SHALL provide a Docker Compose-based operator
entrypoint for the production workflow.

- **Operator surface [KNOWN]:** The user explicitly requested a Docker Compose YAML rather than a repository-local bespoke control plane.
- **Boundary [UNKNOWN]:** The exact host-side boot integration mechanism that launches Docker Compose on startup is not yet fixed in this phase.
- **Traceability:** UR-ARCHIVE-3, UR-ARCHIVE-4

#### RQ-ARCHIVE-002A - Boot-triggered execution compatibility

The Docker Compose entrypoint SHALL be compatible with automatic execution when
the VM boots.

- **Compatibility boundary [INFERRED]:** The workflow contract must not require interactive operator input to begin a normal production run after boot.
- **Non-goal [KNOWN]:** This requirement does not yet choose between host mechanisms such as `systemd`, cloud-init, or Azure VM extensions.
- **Traceability:** UR-ARCHIVE-3, UR-ARCHIVE-4

#### RQ-ARCHIVE-003 - Production-only execution scope

The first `lexonarchivebuilder-archive-sync` increment SHALL be specified as a
production-only workflow.

- **Environment direction [KNOWN]:** The workflow is intended for Azure-backed production execution rather than for the local/testing profile used elsewhere in the repository.
- **Boundary [KNOWN]:** A local/testing archive-sync entrypoint is out of scope for this increment.
- **Traceability:** UR-ARCHIVE-18, UR-ARCHIVE-21

#### RQ-ARCHIVE-003A - VM-hosted production runtime shape

The first `lexonarchivebuilder-archive-sync` increment SHALL target a VM-hosted
production runtime shape that is compatible with machine boot, shutdown, and
spot-instance interruption semantics.

- **Operator shape [KNOWN]:** The user requested machine-boot startup and VM shutdown as first-class workflow behavior.
- **Direction change [INFERRED]:** This increment fixes the previously TBD production workflow shape to a VM-hosted batch workflow for this tool rather than to a generic container-app-only shape.
- **Traceability:** UR-ARCHIVE-3, UR-ARCHIVE-4, UR-ARCHIVE-11, UR-ARCHIVE-14, UR-ARCHIVE-18

#### RQ-ARCHIVE-004 - Rsync source acquisition

`lexonarchivebuilder-archive-sync` SHALL acquire
`rsync.ietf.org::mailman-archive/` as a durable source snapshot whose payloads
and manifests are stored through the production Azure-backed LexonGraph
`BlockStore` realization as the first workflow stage.

- **Source baseline [KNOWN]:** The first increment targets the IETF Mailman archive source explicitly named by the user.
- **Storage target [KNOWN]:** The mirrored archive content, including resumable snapshot-manifest state, must be durably persisted through the updated Azure-backed LexonGraph `BlockStore` realization before mailbox-admission decisions are made.
- **Extensibility [INFERRED]:** The workflow boundary should leave room for future additional archive sources without redefining downstream mailbox-processing contracts or replacing the source-snapshot acquisition contract.
- **Traceability:** UR-ARCHIVE-5, UR-ARCHIVE-21, UR-ARCHIVE-38, UR-ARCHIVE-39, UR-ARCHIVE-40, UR-ARCHIVE-42

#### RQ-ARCHIVE-004A - Rsync snapshot durability

The workflow SHALL durably record enough source-mirror progress to resume after
interruption without requiring the entire mirrored archive to be re-fetched from
scratch when the already-copied snapshot is still valid.

- **Spot-instance rationale [INFERRED]:** Resume safety must include the source-acquisition stage because spot eviction can occur before mailbox admission begins.
- **Boundary [UNKNOWN]:** The exact mirrored-snapshot manifest format and block-addressing or container-layout policy are not yet fixed in this phase.
- **Traceability:** UR-ARCHIVE-5, UR-ARCHIVE-12, UR-ARCHIVE-14, UR-ARCHIVE-40

#### RQ-ARCHIVE-004B - Source snapshot identity

The workflow SHALL assign a source snapshot identity reproducibly bound to the
effective source corpus for each source-acquisition operation.

- **Required recording [KNOWN]:** The source snapshot identity must be recorded in the workflow journal, the published root-history artifact, and any workflow-owned audit artifacts used for root reproducibility.
- **Identity meaning [KNOWN]:** The source snapshot identity must uniquely identify the effective source corpus used for one publication generation.
- **Determinism intent [KNOWN]:** When the effective source corpus is unchanged, the workflow must derive the same source snapshot identity.
- **Traceability:** UR-ARCHIVE-24, UR-ARCHIVE-32

#### RQ-ARCHIVE-004C - Source snapshot provenance

The workflow SHALL preserve sufficient source-snapshot provenance to identify
the effective mirrored corpus for each source snapshot identity.

- **Minimum provenance [INFERRED]:** This provenance should include the rsync source URI plus acquisition evidence sufficient to distinguish complete versus partial or changed mirror states.
- **Audit use [KNOWN]:** Source-snapshot provenance must be available to workflow audit and root reproducibility analysis rather than remaining transient acquisition-only state.
- **Traceability:** UR-ARCHIVE-24, UR-ARCHIVE-37, UR-ARCHIVE-42

#### RQ-ARCHIVE-004D - Source snapshot storage boundary reuse

The workflow SHALL store source-snapshot payloads and manifests through the
same stable production `BlockStore` abstraction family used for immutable
workflow artifacts when that family provides the required Azure-backed
realization.

- **Reuse intent [KNOWN]:** The updated LexonGraph Azure Blob-backed block storage is now the preferred durable production storage boundary for source-snapshot acquisition as well as for downstream immutable artifact persistence.
- **Boundary [INFERRED]:** Source-specific discovery metadata may still evolve independently, but higher workflow stages must not depend on raw Azure Blob API call shapes.
- **Extensibility [KNOWN]:** This storage-boundary reuse must remain applicable to future source and content types that participate in the same workflow family.
- **Traceability:** UR-ARCHIVE-15, UR-ARCHIVE-39, UR-ARCHIVE-40, UR-ARCHIVE-42

#### RQ-ARCHIVE-004E - LexonGraph v2 custom-block adoption for source snapshots

The workflow SHALL persist and consume source-snapshot payloads and manifests
using LexonGraph v2 custom blocks.

- **Included artifacts [KNOWN]:** This applies to source-snapshot payload and manifest blocks owned by `lexonarchivebuilder-archive-sync`.
- **Migration boundary [KNOWN]:** Rebuilt source-snapshot stores are acceptable; continued read compatibility with pre-v2 v1 source-snapshot blocks is not required in this increment.
- **Downstream boundary [KNOWN]:** Downstream mailbox, chunk, embedding, and index flows remain on their existing delegated contracts in this increment; archive-sync does not introduce a repository-owned branch-or-leaf format translation layer around those stages.
- **Recoverability boundary [INFERRED]:** Resume and audit guarantees apply to source-snapshot artifacts created under the v2 custom-block contract for a given generation rather than requiring mixed-format resume across a v1-to-v2 snapshot transition.
- **Traceability:** UR-ARCHIVE-43, UR-ARCHIVE-44, UR-ARCHIVE-45

#### RQ-ARCHIVE-005 - Mailbox block admission

For each mirrored mailbox artifact, `lexonarchivebuilder-archive-sync` SHALL
attempt to admit the mailbox into the configured `BlockStore` abstraction family
and SHALL skip re-admission when the mailbox block is already present.

- **Idempotence [KNOWN]:** Existing mailbox blocks must not be duplicated.
- **Storage boundary [INFERRED]:** Mailbox artifacts should use the same stable block-store abstraction family as downstream chunk and index artifacts rather than a second storage abstraction stack.
- **Traceability:** UR-ARCHIVE-6, UR-ARCHIVE-15

#### RQ-ARCHIVE-006 - New-mailbox chunk derivation

For each mailbox block newly admitted during the current workflow run,
`lexonarchivebuilder-archive-sync` SHALL derive mailbox chunks, persist any new
chunk blocks, and record which chunk work remains incomplete.

- **Delta discipline [KNOWN]:** Mailboxes already present in the block store do not create new chunking work merely because the workflow restarts.
- **Recovery boundary [KNOWN]:** Chunking progress must be represented in durable workflow state rather than inferred only from in-memory execution.
- **Traceability:** UR-ARCHIVE-7, UR-ARCHIVE-12

#### RQ-ARCHIVE-007 - New-chunk embedding generation

For each chunk block newly admitted during the current workflow run,
`lexonarchivebuilder-archive-sync` SHALL generate an embedding and durably
record embedding completion.

- **Delta discipline [KNOWN]:** Existing chunk blocks that already have the required embedding material should not force duplicate embedding work on resume.
- **Provider direction [INFERRED]:** The production embedding path should remain compatible with the repository's Azure OpenAI direction through the existing embedding-provider boundary.
- **Traceability:** UR-ARCHIVE-8, UR-ARCHIVE-12, UR-ARCHIVE-21

#### RQ-ARCHIVE-008 - Embedding-complete indexing barrier

`lexonarchivebuilder-archive-sync` SHALL NOT trigger published-root generation for the
current work set until all required embeddings for that work set are durably
complete.

- **Ordering constraint [KNOWN]:** Published-root generation is gated on embedding completeness, not on mailbox discovery alone.
- **Recovery implication [INFERRED]:** Resume logic must be able to distinguish `pending embedding` from `ready to publish`.
- **Traceability:** UR-ARCHIVE-9, UR-ARCHIVE-12, UR-ARCHIVE-13, UR-ARCHIVE-26, UR-ARCHIVE-27

#### RQ-ARCHIVE-008A - Work-set freeze before publication

The work set participating in one publication generation SHALL be fixed before
published-root generation begins.

- **Generation boundary [KNOWN]:** Source artifacts or derived artifacts discovered after this boundary participate in a later generation rather than modifying the in-flight generation.
- **Audit rationale [INFERRED]:** Fixing the work set before publication keeps generation boundaries explicit for reproducibility analysis.
- **Traceability:** UR-ARCHIVE-27, UR-ARCHIVE-36

#### RQ-ARCHIVE-009 - Published root generation

Once the current work set reaches the embedding-complete state,
`lexonarchivebuilder-archive-sync` SHALL produce a valid published root that
incorporates the effective work set.

- **Reuse intent [KNOWN]:** The workflow may delegate this stage to existing `lexonarchivebuilder-indexer` capabilities or to approved indexer extensions rather than inventing a second repository-local indexing algorithm.
- **Strategy boundary [KNOWN]:** This requirement does not freeze one implementation strategy such as full recomputation, incremental subtree regeneration, merge-based publishing, or other valid root-construction approaches.
- **Boundary [KNOWN]:** This requirement changes orchestration expectations, not MCP search semantics.
- **Traceability:** UR-ARCHIVE-9, UR-ARCHIVE-15, UR-ARCHIVE-16, UR-ARCHIVE-20, UR-ARCHIVE-26, UR-ARCHIVE-27

#### RQ-ARCHIVE-010 - Root-history publication

After each successful publication generation, `lexonarchivebuilder-archive-sync`
SHALL append one new root-history entry to a JSON artifact stored in Azure Blob
Storage.

- **Publication discipline [KNOWN]:** The root-history artifact is an append-only workflow audit log rather than a last-write-wins replacement record in this increment.
- **Immutability boundary [KNOWN]:** The append-only root-history artifact is not itself a published root artifact for purposes of immutable root preservation.
- **Stability visibility [KNOWN]:** Every successful generation appends an entry even when the published root repeats, so periods of stability remain visible in the audit trail.
- **Boundary [UNKNOWN]:** The exact field names and serialization schema for the root-history artifact are not yet fixed in this phase beyond the required provenance semantics.
- **Traceability:** UR-ARCHIVE-10, UR-ARCHIVE-25, UR-ARCHIVE-31

#### RQ-ARCHIVE-010A - Root publication provenance

Every published root-history entry SHALL contain enough metadata to identify:

1. the source snapshot
2. the workflow generation
3. the effective indexing configuration
4. the publication timestamp

- **Audit linkage [INFERRED]:** The entry should also carry a journal identity or equivalent workflow-owned audit link so later diagnosis can connect the published root to its durable workflow record.
- **Schema boundary [KNOWN]:** This requirement fixes required provenance semantics without freezing exact JSON field names.
- **Traceability:** UR-ARCHIVE-25, UR-ARCHIVE-28, UR-ARCHIVE-31

#### RQ-ARCHIVE-010B - Effective indexing configuration identity

Each publication generation SHALL derive and record an effective indexing
configuration identity.

- **Identity meaning [KNOWN]:** The effective indexing configuration identity must cover the root-affecting indexing inputs used for that generation.
- **Minimum scope [KNOWN]:** This identity SHALL include at minimum chunking policy identity, embedding-provider or model identity, delegated published-root generation configuration, and any other workflow-owned or delegated indexing inputs that can change the published root.
- **Recording requirement [KNOWN]:** The effective indexing configuration identity must be recorded in the workflow journal and each root-history entry.
- **Traceability:** UR-ARCHIVE-33

#### RQ-ARCHIVE-010C - Root-history durability or repair

A successful published root generation SHALL NOT be considered complete unless
either:

1. the corresponding root-history entry has been durably recorded, or
2. the workflow journal durably preserves enough information to complete or repair the missing root-history append on resume without regenerating the root

- **Failure-window rationale [KNOWN]:** This requirement closes the gap between successful root generation and later durable history append.
- **Traceability:** UR-ARCHIVE-34

#### RQ-ARCHIVE-011 - Durable workflow journal

`lexonarchivebuilder-archive-sync` SHALL maintain a durable journal that records:

1. the current workflow stage
2. the source snapshot identity
3. the workflow generation identity
4. mailbox blocks pending admission, chunking, embedding, indexing, or publication as applicable
5. completion checkpoints sufficient for safe restart after interruption
6. audit evidence sufficient to explain the effective work set and published-root outcome for a run

- **Required use [KNOWN]:** The journal is the workflow authority for resume decisions.
- **Audit use [KNOWN]:** The journal is also a workflow-owned audit artifact rather than a resume-only checkpoint file.
- **Boundary [UNKNOWN]:** The exact journal serialization format is not yet fixed in this phase.
- **Traceability:** UR-ARCHIVE-12, UR-ARCHIVE-14, UR-ARCHIVE-22, UR-ARCHIVE-23, UR-ARCHIVE-24, UR-ARCHIVE-28

#### RQ-ARCHIVE-011A - Spot-instance checkpoint compatibility

The workflow journal and any related persisted state SHALL be sufficient for
restart after spot-instance eviction or host shutdown without requiring the
workflow to repeat already committed work.

- **Checkpoint boundary [KNOWN]:** Resume must rely on durable persisted state, not on process-local memory.
- **Failure model [INFERRED]:** Restart may occur after abrupt termination in the middle of rsync, mailbox admission, chunking, embedding, indexing, or publication.
- **Traceability:** UR-ARCHIVE-12, UR-ARCHIVE-14, UR-ARCHIVE-29

#### RQ-ARCHIVE-011B - Idempotent resume behavior

On restart, `lexonarchivebuilder-archive-sync` SHALL reconcile journal state
with durable storage state so that previously committed mailbox blocks, chunk
blocks, embeddings, and published roots are not duplicated.

- **Consistency intent [INFERRED]:** Resume logic must tolerate interruption between adjacent stage commits without turning one logical update into duplicate downstream artifacts.
- **Boundary [UNKNOWN]:** The exact reconciliation precedence between journal state and storage-observed state is not yet fixed in this phase.
- **Traceability:** UR-ARCHIVE-6, UR-ARCHIVE-7, UR-ARCHIVE-8, UR-ARCHIVE-10, UR-ARCHIVE-12, UR-ARCHIVE-14

#### RQ-ARCHIVE-011C - Root reproducibility and drift auditability

For repeated runs over the same logical source snapshot and effective indexing
configuration, `lexonarchivebuilder-archive-sync` SHALL either:

1. reproduce the same published root block, or
2. preserve enough audit evidence to explain why the published root changed

- **Audit minimum [KNOWN]:** The workflow must make root drift diagnosable rather than leaving operators to infer differences indirectly from storage side effects alone.
- **Determinism intent [INFERRED]:** When the effective source snapshot and effective indexing configuration are unchanged, unchanged roots are the expected baseline.
- **Boundary [KNOWN]:** The minimum required audit evidence includes source snapshot identity, generation identity, effective indexing configuration identity, publication timestamp, and workflow-owned audit linkage; additional diagnostic fields may evolve without changing this requirement.
- **Traceability:** UR-ARCHIVE-22, UR-ARCHIVE-23, UR-ARCHIVE-24, UR-ARCHIVE-25, UR-ARCHIVE-28

#### RQ-ARCHIVE-011D - Generation identity

Each workflow execution SHALL be assigned a stable generation identifier.

- **Required recording [KNOWN]:** The generation identifier must be recorded in journal artifacts, root-history entries, failure artifacts, and publication records.
- **Audit rationale [KNOWN]:** Generation identity is required so operators can distinguish one successful or failed publication attempt from another even when they target the same source snapshot.
- **Traceability:** UR-ARCHIVE-28

#### RQ-ARCHIVE-011E - Checkpoint granularity

No committed mailbox, chunk, embedding, or publication operation may require
re-execution after a successful checkpoint has been recorded.

- **Granularity boundary [KNOWN]:** This requirement constrains correctness of checkpoint boundaries without mandating a specific timer-based checkpoint cadence.
- **Spot-instance rationale [KNOWN]:** This requirement preserves compatibility with abrupt eviction by ensuring committed work does not drift backward after restart.
- **Traceability:** UR-ARCHIVE-29

#### RQ-ARCHIVE-011F - Workflow journal authority

At workflow-stage boundaries, the `lexonarchivebuilder-archive-sync` journal
SHALL remain the authoritative source for workflow resume decisions.

- **Subordinate journal boundary [KNOWN]:** Any downstream `lexonarchivebuilder-indexer` replay journal or equivalent delegated journal remains a subordinate implementation artifact rather than a peer workflow authority.
- **Design implication [INFERRED]:** When journal states disagree, workflow-stage control must be resolved through the archive-sync journal's authority plus any required reconciliation logic.
- **Traceability:** UR-ARCHIVE-35

#### RQ-ARCHIVE-012 - Terminal VM shutdown

After a terminal workflow outcome, `lexonarchivebuilder-archive-sync` SHALL
trigger VM shutdown.

- **Terminal outcomes [KNOWN]:** This includes terminal success and terminal non-recoverable failure.
- **Boundary [UNKNOWN]:** The exact shutdown invocation surface is not yet fixed in this phase.
- **Traceability:** UR-ARCHIVE-11, UR-ARCHIVE-19

#### RQ-ARCHIVE-013 - Failure-state preservation before shutdown

If the workflow reaches a terminal non-recoverable failure,
`lexonarchivebuilder-archive-sync` SHALL durably preserve failure-adjacent
journal state before triggering VM shutdown.

- **Operator need [INFERRED]:** Postmortem diagnosis depends on keeping the final known stage and pending work set, even though the VM will still shut down.
- **Boundary [UNKNOWN]:** The exact failure artifact family beyond the journal is not yet fixed in this phase.
- **Traceability:** UR-ARCHIVE-12, UR-ARCHIVE-19

#### RQ-ARCHIVE-014 - Stable downstream contract reuse

`lexonarchivebuilder-archive-sync` SHALL reuse existing
LexonArchiveBuilder-compatible storage, embedding, and indexing contracts where
practical instead of inventing a parallel protocol.

- **Authority boundary [KNOWN]:** `BlockStore`, embedding-provider, and delegated indexing semantics remain subordinate to their existing owning crates and repository interfaces.
- **Allowed evolution [KNOWN]:** Targeted changes to `lexonarchivebuilder-indexer` are permitted when necessary to support the approved workflow.
- **Updated storage direction [KNOWN]:** The updated Azure-backed LexonGraph `BlockStore` realization is now also an allowed production source-acquisition storage boundary for rsync snapshot payloads and manifests.
- **Traceability:** UR-ARCHIVE-15, UR-ARCHIVE-16, UR-ARCHIVE-21, UR-ARCHIVE-39, UR-ARCHIVE-40

#### RQ-ARCHIVE-015 - Future content extensibility

The `lexonarchivebuilder-archive-sync` workflow SHALL preserve a stable
orchestration boundary that can be extended to future content types without
redefining the core workflow contract.

- **Initial focus [KNOWN]:** The first increment is mailbox-focused.
- **Extensibility [INFERRED]:** Future content-specific derivation logic should fit behind the same journaled orchestration boundary rather than forcing a second workflow family.
- **Future source-artifact examples [KNOWN]:** Future source artifacts may include RFCs, Internet Drafts, Datatracker metadata, and Working Group metadata.
- **Acquisition extensibility [KNOWN]:** The reusable source-snapshot acquisition boundary should remain applicable when future content or source families are added.
- **Traceability:** UR-ARCHIVE-15, UR-ARCHIVE-21, UR-ARCHIVE-42

### Boundary and Invariant Requirements

#### RQ-ARCHIVE-016 - Indexing/search-serving separation

`lexonarchivebuilder-archive-sync` SHALL remain limited to production ingestion,
artifact persistence, embedding, and root publication orchestration and SHALL
NOT redefine MCP server behavior or search semantics.

- **Rationale [KNOWN]:** The user requested a new workflow, not MCP-surface changes.
- **Traceability:** UR-ARCHIVE-15, UR-ARCHIVE-20, UR-ARCHIVE-41

#### RQ-ARCHIVE-017 - No central control-plane expansion

The first `lexonarchivebuilder-archive-sync` increment SHALL remain a
boot-triggered batch workflow and SHALL NOT require a new long-lived repository
control plane beyond the workflow runtime itself.

- **Architecture alignment [INFERRED]:** This preserves the repository's intended CDN-backed, indexing-oriented shape without adding unrelated server-side processing layers.
- **Traceability:** UR-ARCHIVE-3, UR-ARCHIVE-4, UR-ARCHIVE-15

#### RQ-ARCHIVE-018 - Stable Azure-oriented adapter boundary

Production storage and embedding choices for `lexonarchivebuilder-archive-sync`
SHALL remain behind stable repository or upstream adapter boundaries so the
workflow contract does not depend on Azure-specific call shapes at every stage.

- **Environment direction [KNOWN]:** The first production path targets Azure Blob Storage and the repository's Azure-oriented embedding direction.
- **Boundary [INFERRED]:** Azure-specific realizations should stay behind stable storage and embedding seams rather than leaking into every higher-level workflow stage contract.
- **Updated storage boundary [KNOWN]:** The production rsync snapshot path should consume Azure Blob Storage through the updated LexonGraph `BlockStore` seam instead of a workflow-specific Azure Blob API surface.
- **Traceability:** UR-ARCHIVE-15, UR-ARCHIVE-21, UR-ARCHIVE-39, UR-ARCHIVE-40, UR-ARCHIVE-41

#### RQ-ARCHIVE-019 - Immutable artifact preservation

The workflow SHALL NOT modify previously published mailbox blocks, chunk
blocks, embeddings, index blocks, or root artifacts.

New information SHALL be represented by new immutable artifacts.

- **Invariant alignment [KNOWN]:** This preserves the repository and LexonGraph assumption that published artifacts are immutable and auditability is achieved by adding new durable artifacts rather than rewriting old ones.
- **Traceability:** UR-ARCHIVE-30

## Out of Scope

- Defining the exact host boot mechanism that launches Docker Compose on VM startup
- Defining the exact Azure VM shutdown command or IAM plumbing
- Introducing a local/testing `lexonarchivebuilder-archive-sync` entrypoint in this increment
- Redefining MCP server behavior, search ranking, or retrieval semantics
- Inventing a repository-local block-store or embedding abstraction separate from the existing trait families
- Inventing a second repository-local indexing algorithm instead of reusing or extending the approved indexer path
- Finalizing exact field names or serialization details for the root-history artifact beyond the required provenance metadata
- Finalizing the exact block-addressing, container-layout, or naming scheme for source snapshots, journals, or failure artifacts
- Preserving mixed-format or pre-v2 v1 compatibility for source-snapshot payloads and manifests after the approved v2 custom-block transition
- Generalizing the first source beyond `rsync.ietf.org::mailman-archive/` in this increment
- Defining non-mailbox content derivation rules for the first increment

## Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | The workflow is constrained to ingestion, persistence, embedding, indexing orchestration, and root publication |
| Environment-specific behavior stays behind stable interfaces | Preserved with clarified storage path | Azure Blob Storage access for both source snapshots and downstream immutable artifacts now remains behind the updated LexonGraph `BlockStore` seam plus the existing embedding seam |
| Architecture remains extensible to future content types | Preserved with clarified acquisition scope | Mailbox-specific stages remain the first increment within a reusable orchestration boundary that now explicitly includes source-snapshot acquisition |
| Idempotence and recoverability remain aligned with immutable block semantics | Preserved with clarified scope | The workflow now requires journal-driven resume, durable source snapshot and generation identities, checkpoint-safe committed work, and duplicate-safe reconciliation across mailbox, chunk, embedding, indexing, and publication stages |
| Repeated runs remain auditable and explainable | Preserved with clarified scope | The journal now serves both restart safety and root reproducibility or drift explanation across repeated runs |
| Published artifacts remain immutable | Preserved with clarified scope | The workflow may publish new artifacts and root-history entries but must not mutate previously published mailbox, chunk, embedding, index, or root artifacts |
| Production execution remains batch-oriented rather than control-plane-driven | Preserved | The workflow is boot-triggered and Compose-launched without introducing a new long-lived control plane |
| Production runtime shape is explicit for this workflow | Revised with approved direction change | The new workflow fixes this increment to a VM-hosted, boot-triggered, shutdown-capable batch runtime compatible with spot interruption |
| Existing indexer and MCP contracts remain stable | Preserved | The workflow may reuse or extend indexer internals while leaving MCP behavior unchanged |

## Discovery Gaps and Resolution Notes

- **Q-ARCHIVE-001 [KNOWN]:** Resolved in Phase 2 revision. Each root-history entry must include provenance metadata for source snapshot identity, generation identity, effective indexing configuration, and publication timestamp.
- **Q-ARCHIVE-002 [UNKNOWN]:** Given the archive-sync journal's workflow-stage authority, what exact reconciliation data needs to flow between it and any downstream `lexonarchivebuilder-indexer` replay journal when both exist for the same run?
- **Q-ARCHIVE-003 [UNKNOWN]:** Within the Azure-backed `BlockStore` snapshot boundary, must the logical rsync directory layout be reproducibly preserved, or is normalized manifest indirection acceptable so long as resume and provenance remain correct?
- **Q-ARCHIVE-004 [UNKNOWN]:** What criteria classify a failure as terminal and non-recoverable versus restartable on the next boot?
- **Q-ARCHIVE-005 [KNOWN]:** Resolved in Phase 2 revision. Every successful generation appends a root-history entry even when the published root repeats.
- **Q-ARCHIVE-006 [UNKNOWN]:** Does the workflow need an explicit operator-visible artifact summarizing pending work counts by stage, or is the durable journal alone sufficient in the first increment?
- **Q-ARCHIVE-007 [KNOWN]:** Resolved in Phase 2 revision. The workflow must retain source snapshot identity, generation identity, effective indexing configuration identity, publication timestamp, and workflow-owned audit linkage sufficient to explain a changed root.
- **Q-ARCHIVE-008 [UNKNOWN]:** Does the updated Azure-backed LexonGraph `BlockStore` already expose the manifest-addressable semantics needed for restart-safe rsync snapshot acquisition, or will archive-sync need a repository-owned manifest convention layered on that storage seam?

## Coverage Notes

- **Covered sources [KNOWN]:**
  - user request in this session: "I need a Docker compose yaml that does the following: 1) Starts on machine boot 2) Run rsync over rsync.ietf.org::mailman-archive/ to an azure storage blob. 3) Insert each mailbox as a block in an block-store trait implementation if the block is not already present 4) For all new blocks, chunk the mailbox and generate new chunks and store them in block-store trait. 5) For each new chunk, generate an embedding. 6) Recompute index tree. 7) Append new root block to json file stored in azure storage blob 8) Shutdown VM"
  - user request in this session: "It should resumable, using a journal that tells it what step it what blocks need chunking/embedding/indexing. Only trigger indexing when all blocks are embedded. It should be compatible with spot-instance VMs (so that if it gets shutdown it can resume via a checkpoint)."
  - user request in this session: "for now, just a spec trifecta. We can then start mapping out what still needs to be built."
  - user clarification in this session selecting: `lexonarchivebuilder-archive-sync`
  - user clarification in this session selecting: `Production-only workflow`
  - user clarification in this session selecting: `Always shut down, even on failure`
  - user clarification in this session: "The journal should not just be for resumption, but also auditing. I.e. I should be able to run the workflow again and either get the same root block or know why it's different"
  - user review in this session: "Add source snapshot identity"
  - user review in this session: "Every published root-history entry SHALL contain enough metadata to identify: 1. the source snapshot 2. the workflow generation 3. the effective indexing configuration 4. the publication timestamp"
  - user review in this session: "Replace with: SHALL produce a valid published root that incorporates the effective work set."
  - user review in this session: "Each workflow execution SHALL be assigned a stable generation identifier."
  - user review in this session: "No committed mailbox, chunk, embedding, or publication operation may require re-execution after a successful checkpoint has been recorded."
  - user review in this session: "The workflow SHALL NOT modify previously published mailbox blocks, chunk blocks, embeddings, index blocks, or root artifacts."
  - user feedback in this session: "The workflow SHALL assign a source snapshot identity derived from or uniquely bound to the effective source corpus. Identical effective source corpora SHOULD produce the same source snapshot identity."
  - user feedback in this session: "The specification should define Effective Indexing Configuration Identity."
  - user feedback in this session: "Publication is not considered complete until the history entry is durably recorded." / "History append failures must be recoverable from journal state on resume."
  - user feedback in this session: "The archive-sync journal is authoritative at workflow-stage boundaries; indexer journals are subordinate implementation artifacts."
  - user feedback in this session: "The work set participating in a publication generation SHALL be fixed before published-root generation begins."
  - user request in this session: "Azure-backed rsync snapshot acquisition for lexonarchivebuilder-archive-sync. LexonGraph has an updated block storage that works over an azure blob store"
  - user clarification in this session selecting: `Use the updated Azure-backed LexonGraph BlockStore for the rsync snapshot payloads and manifests`
  - user clarification in this session selecting: `Keep it production-only and leave MCP behavior unchanged`
  - user clarification in this session selecting: `Establish a reusable source-snapshot boundary for future content types and sources`
  - user request in this session: "lexongraph now has a v2 of the block format. Switch over to using that instead of the v1 format."
  - user clarification in this session selecting: "Require rebuilding stores and support only v2 blocks"
  - `README.md:7-13`
  - `README.md:20-28`
  - `README.md:42-49`
  - `README.md:72-80`
  - `docs/specs/lexonarchivebuilder-indexer/requirements.md:11-18`
  - `docs/specs/lexonarchivebuilder-indexer/requirements.md:22-24`
  - `docs/specs/lexonarchivebuilder-indexer/requirements.md:41-57`
  - `docs/specs/lexonarchivebuilder-indexer/requirements.md:1207-1245`
  - `docs/specs/lexonarchivebuilder-scale-test/requirements.md:11-18`
  - `docs/specs/lexonarchivebuilder-scale-test/requirements.md:142-183`
- **Excluded for now [KNOWN]:**
  - Design-level journal schema, checkpoint serialization, and retry policy
  - Validation-level pass conditions and test assets
  - Rust implementation files, Docker Compose runtime assets, Azure deployment plumbing, and operational secrets
