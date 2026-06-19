# LexonArchiveBuilder Archive Sync Requirements

## Document Status

- **Phase:** Phase 2 - Specification Changes
- **Status:** Approved requirements baseline being propagated into design and validation for a production-only `lexonarchivebuilder-archive-sync` workflow covering boot-triggered rsync mirroring into Azure Blob Storage, mailbox-to-block persistence, resumable chunking and embedding, index recomputation gating, root-history publication, and terminal VM shutdown
- **Scope:** `lexonarchivebuilder-archive-sync` as a separate production workflow layered on top of existing LexonArchiveBuilder indexing and storage boundaries

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

## Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-ARCHIVE-001 | Add | Introduce a new production workflow specification package for `lexonarchivebuilder-archive-sync` as a boundary separate from the indexer and MCP server | UR-ARCHIVE-1, UR-ARCHIVE-2, UR-ARCHIVE-15, UR-ARCHIVE-17 |
| CM-ARCHIVE-002 | Add | Define Docker Compose as the workflow entrypoint and require boot-time automatic start compatibility | UR-ARCHIVE-3, UR-ARCHIVE-4 |
| CM-ARCHIVE-003 | Add | Define the first production source as `rsync.ietf.org::mailman-archive/` mirrored into Azure Blob Storage | UR-ARCHIVE-5 |
| CM-ARCHIVE-004 | Add | Require idempotent mailbox block persistence through the existing `BlockStore` abstraction family | UR-ARCHIVE-6, UR-ARCHIVE-15 |
| CM-ARCHIVE-005 | Add | Require chunk derivation and chunk persistence only for newly admitted mailbox blocks | UR-ARCHIVE-7 |
| CM-ARCHIVE-006 | Add | Require embedding generation only for newly admitted chunks | UR-ARCHIVE-8 |
| CM-ARCHIVE-007 | Add | Gate index recomputation on completion of all required embedding work | UR-ARCHIVE-9, UR-ARCHIVE-13 |
| CM-ARCHIVE-008 | Add | Require append-only root publication to a JSON artifact in Azure Blob Storage | UR-ARCHIVE-10 |
| CM-ARCHIVE-009 | Add | Require a durable resume journal that records stage, work inventories, and checkpoint-safe progress | UR-ARCHIVE-12, UR-ARCHIVE-14 |
| CM-ARCHIVE-009A | Add | Require the journal to double as an audit artifact that explains root reproducibility or root drift across repeated runs | UR-ARCHIVE-22, UR-ARCHIVE-23 |
| CM-ARCHIVE-010 | Add | Require VM shutdown on terminal success and terminal non-recoverable failure | UR-ARCHIVE-11, UR-ARCHIVE-19 |
| CM-ARCHIVE-011 | Add | Preserve compatibility with spot-instance eviction by requiring resume from durable checkpoints instead of in-memory state | UR-ARCHIVE-12, UR-ARCHIVE-14 |
| CM-ARCHIVE-012 | Add | Preserve indexer reuse and allow targeted `lexonarchivebuilder-indexer` evolution without moving this workflow into the indexer boundary | UR-ARCHIVE-15, UR-ARCHIVE-16 |
| CM-ARCHIVE-013 | Add | Constrain the first increment to production-only Azure-oriented execution while leaving local/testing concerns out of scope | UR-ARCHIVE-18, UR-ARCHIVE-21 |
| CM-ARCHIVE-014 | Add | Define the first production runtime shape as a VM-hosted, boot-triggered, Compose-launched workflow compatible with spot-instance semantics | UR-ARCHIVE-3, UR-ARCHIVE-4, UR-ARCHIVE-11, UR-ARCHIVE-14, UR-ARCHIVE-18 |
| CM-ARCHIVE-015 | Add | Preserve search-serving separation and future content extensibility while introducing mailbox-focused workflow stages now | UR-ARCHIVE-20, UR-ARCHIVE-21 |

## Before / After

### BA-ARCHIVE-001

- **Before [KNOWN]:** The repository has indexer and local scale-test specifications, but no production workflow specification for rsync-driven mailbox mirroring, resumable block admission, and root publication on Azure-backed spot VMs.
- **After [KNOWN]:** The repository has an explicit requirements baseline for `lexonarchivebuilder-archive-sync` under `docs/specs/lexonarchivebuilder-archive-sync/requirements.md`.

### BA-ARCHIVE-002

- **Before [KNOWN]:** Production direction mentions Azure Blob Storage and Azure OpenAI at the architecture level, but does not define an automated archive-sync workflow that starts on VM boot.
- **After [KNOWN]:** The requirements define a production-only Docker Compose workflow that is compatible with boot-triggered execution on a VM.

### BA-ARCHIVE-003

- **Before [KNOWN]:** Rsync-backed mailbox acquisition is currently described only as a local scale-test wrapper concern.
- **After [KNOWN]:** The first production workflow increment explicitly mirrors `rsync.ietf.org::mailman-archive/` into Azure Blob Storage before downstream mailbox admission and indexing work begins.

### BA-ARCHIVE-004

- **Before [KNOWN]:** The repository does not yet specify a workflow-level requirement that raw mailbox artifacts themselves be admitted into the shared `BlockStore` family before chunking and embedding decisions are made.
- **After [KNOWN]:** The workflow must admit mailbox artifacts as blocks when absent, then derive chunk and embedding work only from newly admitted mailbox blocks.

### BA-ARCHIVE-005

- **Before [KNOWN]:** Existing requirements describe split-stage indexing and replay journaling for local/testing indexer execution, but not a workflow journal that coordinates rsync mirroring, mailbox admission, chunking, embedding, index publication, and restart after spot eviction.
- **After [KNOWN]:** The workflow requirements define a durable journal and checkpoint contract across all production stages.

### BA-ARCHIVE-006

- **Before [KNOWN]:** The repository does not specify a workflow-level barrier preventing index recomputation until all newly required embeddings are durably present.
- **After [KNOWN]:** Index recomputation is explicitly gated on embedding completeness for the pending work set.

### BA-ARCHIVE-007

- **Before [KNOWN]:** Root publication for new production indexing runs is not defined as an append-only JSON history in Azure Blob Storage.
- **After [KNOWN]:** Each successful index recomputation must append a new root entry to a JSON artifact stored in Azure Blob Storage.

### BA-ARCHIVE-008

- **Before [KNOWN]:** VM shutdown behavior after workflow completion or terminal failure is not specified at the repository workflow layer.
- **After [KNOWN]:** The workflow must shut the VM down after terminal success and after terminal non-recoverable failure.

### BA-ARCHIVE-009

- **Before [KNOWN]:** Resume state was the only explicit journal concern, so repeated runs could lack repository-defined evidence for why the published root matched or changed.
- **After [KNOWN]:** The workflow journal is also an audit artifact that must support root reproducibility checks and explain root drift across repeated runs.

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

`lexonarchivebuilder-archive-sync` SHALL mirror
`rsync.ietf.org::mailman-archive/` into Azure Blob Storage as the first
workflow stage.

- **Source baseline [KNOWN]:** The first increment targets the IETF Mailman archive source explicitly named by the user.
- **Storage target [KNOWN]:** The mirrored archive content must land in Azure Blob Storage before mailbox-admission decisions are made.
- **Extensibility [INFERRED]:** The workflow boundary should leave room for future additional archive sources without redefining downstream mailbox-processing contracts.
- **Traceability:** UR-ARCHIVE-5, UR-ARCHIVE-21

#### RQ-ARCHIVE-004A - Rsync snapshot durability

The workflow SHALL durably record enough source-mirror progress to resume after
interruption without requiring the entire mirrored archive to be re-fetched from
scratch when the already-copied snapshot is still valid.

- **Spot-instance rationale [INFERRED]:** Resume safety must include the source-acquisition stage because spot eviction can occur before mailbox admission begins.
- **Boundary [UNKNOWN]:** The exact mirrored-snapshot manifest format and Azure Blob naming policy are not yet fixed in this phase.
- **Traceability:** UR-ARCHIVE-5, UR-ARCHIVE-12, UR-ARCHIVE-14

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

`lexonarchivebuilder-archive-sync` SHALL NOT trigger index recomputation for the
current work set until all required embeddings for that work set are durably
complete.

- **Ordering constraint [KNOWN]:** Indexing is gated on embedding completeness, not on mailbox discovery alone.
- **Recovery implication [INFERRED]:** Resume logic must be able to distinguish `pending embedding` from `ready to index`.
- **Traceability:** UR-ARCHIVE-9, UR-ARCHIVE-12, UR-ARCHIVE-13

#### RQ-ARCHIVE-009 - Index recomputation

Once the current work set reaches the embedding-complete state,
`lexonarchivebuilder-archive-sync` SHALL recompute the index tree.

- **Reuse intent [KNOWN]:** The workflow may delegate this stage to existing `lexonarchivebuilder-indexer` capabilities or to approved indexer extensions rather than inventing a second repository-local indexing algorithm.
- **Boundary [KNOWN]:** This requirement changes orchestration expectations, not MCP search semantics.
- **Traceability:** UR-ARCHIVE-9, UR-ARCHIVE-15, UR-ARCHIVE-16, UR-ARCHIVE-20

#### RQ-ARCHIVE-010 - Root-history publication

After a successful index recomputation, `lexonarchivebuilder-archive-sync`
SHALL append the newly produced root block identifier to a JSON artifact stored
in Azure Blob Storage.

- **Publication discipline [KNOWN]:** The root artifact is append-oriented rather than last-write-wins replacement in this increment.
- **Boundary [UNKNOWN]:** The exact JSON schema for the root-history artifact is not yet fixed in this phase.
- **Traceability:** UR-ARCHIVE-10

#### RQ-ARCHIVE-011 - Durable workflow journal

`lexonarchivebuilder-archive-sync` SHALL maintain a durable journal that records:

1. the current workflow stage
2. mailbox blocks pending admission, chunking, embedding, indexing, or publication as applicable
3. completion checkpoints sufficient for safe restart after interruption
4. audit evidence sufficient to explain the effective work set and published-root outcome for a run

- **Required use [KNOWN]:** The journal is the workflow authority for resume decisions.
- **Audit use [KNOWN]:** The journal is also a workflow-owned audit artifact rather than a resume-only checkpoint file.
- **Boundary [UNKNOWN]:** The exact journal serialization format is not yet fixed in this phase.
- **Traceability:** UR-ARCHIVE-12, UR-ARCHIVE-14, UR-ARCHIVE-22, UR-ARCHIVE-23

#### RQ-ARCHIVE-011A - Spot-instance checkpoint compatibility

The workflow journal and any related persisted state SHALL be sufficient for
restart after spot-instance eviction or host shutdown without requiring the
workflow to repeat already committed work.

- **Checkpoint boundary [KNOWN]:** Resume must rely on durable persisted state, not on process-local memory.
- **Failure model [INFERRED]:** Restart may occur after abrupt termination in the middle of rsync, mailbox admission, chunking, embedding, indexing, or publication.
- **Traceability:** UR-ARCHIVE-12, UR-ARCHIVE-14

#### RQ-ARCHIVE-011B - Idempotent resume behavior

On restart, `lexonarchivebuilder-archive-sync` SHALL reconcile journal state
with durable storage state so that previously committed mailbox blocks, chunk
blocks, embeddings, and published roots are not duplicated.

- **Consistency intent [INFERRED]:** Resume logic must tolerate interruption between adjacent stage commits without turning one logical update into duplicate downstream artifacts.
- **Boundary [UNKNOWN]:** The exact reconciliation precedence between journal state and storage-observed state is not yet fixed in this phase.
- **Traceability:** UR-ARCHIVE-6, UR-ARCHIVE-7, UR-ARCHIVE-8, UR-ARCHIVE-10, UR-ARCHIVE-12, UR-ARCHIVE-14

#### RQ-ARCHIVE-011C - Root reproducibility and drift auditability

For repeated runs over the same logical source snapshot and effective workflow
configuration, `lexonarchivebuilder-archive-sync` SHALL either:

1. reproduce the same published root block, or
2. preserve enough audit evidence to explain why the published root changed

- **Audit minimum [KNOWN]:** The workflow must make root drift diagnosable rather than leaving operators to infer differences indirectly from storage side effects alone.
- **Determinism intent [INFERRED]:** When the effective source snapshot and effective workflow inputs are unchanged, unchanged roots are the expected baseline.
- **Boundary [UNKNOWN]:** The exact set of audit fields required to explain root drift is not yet fixed in this phase.
- **Traceability:** UR-ARCHIVE-22, UR-ARCHIVE-23

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
- **Traceability:** UR-ARCHIVE-15, UR-ARCHIVE-16, UR-ARCHIVE-21

#### RQ-ARCHIVE-015 - Future content extensibility

The `lexonarchivebuilder-archive-sync` workflow SHALL preserve a stable
orchestration boundary that can be extended to future content types without
redefining the core workflow contract.

- **Initial focus [KNOWN]:** The first increment is mailbox-focused.
- **Extensibility [INFERRED]:** Future content-specific derivation logic should fit behind the same journaled orchestration boundary rather than forcing a second workflow family.
- **Traceability:** UR-ARCHIVE-15, UR-ARCHIVE-21

### Boundary and Invariant Requirements

#### RQ-ARCHIVE-016 - Indexing/search-serving separation

`lexonarchivebuilder-archive-sync` SHALL remain limited to production ingestion,
artifact persistence, embedding, and root publication orchestration and SHALL
NOT redefine MCP server behavior or search semantics.

- **Rationale [KNOWN]:** The user requested a new workflow, not MCP-surface changes.
- **Traceability:** UR-ARCHIVE-15, UR-ARCHIVE-20

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
- **Traceability:** UR-ARCHIVE-15, UR-ARCHIVE-21

## Out of Scope

- Defining the exact host boot mechanism that launches Docker Compose on VM startup
- Defining the exact Azure VM shutdown command or IAM plumbing
- Introducing a local/testing `lexonarchivebuilder-archive-sync` entrypoint in this increment
- Redefining MCP server behavior, search ranking, or retrieval semantics
- Inventing a repository-local block-store or embedding abstraction separate from the existing trait families
- Inventing a second repository-local indexing algorithm instead of reusing or extending the approved indexer path
- Finalizing the exact JSON schema for the root-history artifact
- Finalizing the exact on-blob layout or naming scheme for mirrored rsync snapshots, journals, or failure artifacts
- Generalizing the first source beyond `rsync.ietf.org::mailman-archive/` in this increment
- Defining non-mailbox content derivation rules for the first increment

## Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | The workflow is constrained to ingestion, persistence, embedding, indexing orchestration, and root publication |
| Environment-specific behavior stays behind stable interfaces | Preserved | Azure Blob Storage and production embeddings remain adapter concerns rather than workflow-wide special cases |
| Architecture remains extensible to future content types | Preserved | Mailbox-specific stages are defined as the first increment within a reusable orchestration boundary |
| Idempotence and recoverability remain aligned with immutable block semantics | Preserved with clarified scope | The workflow now requires journal-driven resume and duplicate-safe reconciliation across mailbox, chunk, embedding, indexing, and publication stages |
| Repeated runs remain auditable and explainable | Preserved with clarified scope | The journal now serves both restart safety and root reproducibility or drift explanation across repeated runs |
| Production execution remains batch-oriented rather than control-plane-driven | Preserved | The workflow is boot-triggered and Compose-launched without introducing a new long-lived control plane |
| Production runtime shape is explicit for this workflow | Revised with approved direction change | The new workflow fixes this increment to a VM-hosted, boot-triggered, shutdown-capable batch runtime compatible with spot interruption |
| Existing indexer and MCP contracts remain stable | Preserved | The workflow may reuse or extend indexer internals while leaving MCP behavior unchanged |

## Open Questions / Discovery Gaps

- **Q-ARCHIVE-001 [UNKNOWN]:** Should the root-history JSON artifact contain only appended root identifiers, or must each entry also include source snapshot metadata, timestamps, and journal provenance?
- **Q-ARCHIVE-002 [UNKNOWN]:** What is the exact authority boundary between the workflow journal and any downstream `lexonarchivebuilder-indexer` replay journal when both exist for the same run?
- **Q-ARCHIVE-003 [UNKNOWN]:** Should source mirroring preserve the raw rsync directory layout byte-for-byte in Azure Blob Storage, or is a normalized blob layout acceptable so long as resume and provenance remain correct?
- **Q-ARCHIVE-004 [UNKNOWN]:** What criteria classify a failure as terminal and non-recoverable versus restartable on the next boot?
- **Q-ARCHIVE-005 [UNKNOWN]:** Must the workflow publish a new root-history entry only when the recomputed root differs from the previously published root, or should every completed run append an entry even if the root repeats?
- **Q-ARCHIVE-006 [UNKNOWN]:** Does the workflow need an explicit operator-visible artifact summarizing pending work counts by stage, or is the durable journal alone sufficient in the first increment?
- **Q-ARCHIVE-007 [UNKNOWN]:** What exact source-snapshot identity, effective-configuration identity, and per-stage evidence must be retained to explain a changed root conclusively?

## Coverage Notes

- **Covered sources [KNOWN]:**
  - user request in this session: "I need a Docker compose yaml that does the following: 1) Starts on machine boot 2) Run rsync over rsync.ietf.org::mailman-archive/ to an azure storage blob. 3) Insert each mailbox as a block in an block-store trait implementation if the block is not already present 4) For all new blocks, chunk the mailbox and generate new chunks and store them in block-store trait. 5) For each new chunk, generate an embedding. 6) Recompute index tree. 7) Append new root block to json file stored in azure storage blob 8) Shutdown VM"
  - user request in this session: "It should resumable, using a journal that tells it what step it what blocks need chunking/embedding/indexing. Only trigger indexing when all blocks are embedded. It should be compatible with spot-instance VMs (so that if it gets shutdown it can resume via a checkpoint)."
  - user request in this session: "for now, just a spec trifecta. We can then start mapping out what still needs to be built."
  - user clarification in this session selecting: `lexonarchivebuilder-archive-sync`
  - user clarification in this session selecting: `Production-only workflow`
  - user clarification in this session selecting: `Always shut down, even on failure`
  - user clarification in this session: "The journal should not just be for resumption, but also auditing. I.e. I should be able to run the workflow again and either get the same root block or know why it's different"
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
