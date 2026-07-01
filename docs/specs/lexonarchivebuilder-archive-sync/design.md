<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder Archive Sync Design

## Status

Phase 2 specification patch for the approved production-only
`lexonarchivebuilder-archive-sync` workflow in
`docs/specs/lexonarchivebuilder-archive-sync/requirements.md`, including the
Azure-backed rsync source-snapshot acquisition revision that reuses the updated
LexonGraph Azure Blob-backed `BlockStore` realization plus v2 custom-block
adoption for source-snapshot artifacts.

## Scope

This document specifies the LexonArchiveBuilder-owned design for realizing
`lexonarchivebuilder-archive-sync` as a VM-hosted, boot-triggered, Docker
Compose-launched production workflow that mirrors
`rsync.ietf.org::mailman-archive/` as a durable Azure-backed source snapshot
through the shared `BlockStore` boundary, admits mailbox and chunk artifacts
through that same storage family, generates embeddings for newly admitted
chunks, gates published-root generation on embedding completion,
publishes append-only provenance-rich root history, preserves restart-safe and audit-safe
journal state, and shuts the VM down on terminal success or terminal
non-recoverable failure.

This document is layered on top of:

- `docs/specs/lexonarchivebuilder-archive-sync/requirements.md`
- `docs/specs/lexonarchivebuilder-indexer/requirements.md`
- `docs/specs/lexonarchivebuilder-indexer/design.md`
- `README.md`

This document does not redefine `BlockStore`, embedding-provider, or delegated
index-construction semantics, and it does not redefine MCP server behavior.
Those remain owned by existing LexonArchiveBuilder and LexonGraph boundaries.

## Impact Map

### Directly affected artifacts

- `docs/specs/lexonarchivebuilder-archive-sync/requirements.md`
- `docs/specs/lexonarchivebuilder-archive-sync/design.md`
- `docs/specs/lexonarchivebuilder-archive-sync/validation.md`

### Indirectly affected artifacts

- Docker Compose workflow assets and container entrypoint wiring for the new
  production workflow
- Azure-backed `BlockStore` snapshot, journal, and root-history publication assets
- `lexonarchivebuilder-indexer` orchestration seams or helper surfaces needed to
  support chunking, embedding, replay, and index recomputation in the approved
  workflow order
- operator documentation for VM boot, restart, and shutdown behavior

### Unaffected artifacts

- `docs/specs/lexonarchivebuilder-mcp/*`
- MCP request/response semantics and search ranking behavior
- local/testing wrapper behavior under `docs/specs/lexonarchivebuilder-scale-test/*`
- the upstream `BlockStore` and embedding-provider contracts themselves

## Design Goals

The `lexonarchivebuilder-archive-sync` design is intended to be:

- a workflow layer above existing indexing and storage contracts
- explicit about stage ownership and audit boundaries
- VM-hosted and non-interactive at normal boot
- durable across abrupt spot-instance interruption
- delta-oriented so already committed work is not repeated
- auditable enough to explain repeated-root stability or root drift
- batch-oriented without introducing a long-lived control plane
- aligned with Azure Blob Storage through shared adapter seams plus Azure-oriented production embeddings
- extensible to future content types and future source types

## Boundary Design

### DSG-LAS-001 `Delegated archive-sync boundary`

`lexonarchivebuilder-archive-sync` owns source mirroring, journal management,
delta work discovery, workflow-stage transitions, root-history publication, and
terminal shutdown orchestration.

`lexonarchivebuilder-archive-sync` does not own MCP search semantics, the
canonical `BlockStore` contract, the embedding-provider contract, or the
delegated index-construction algorithm itself.

**Traces to:** RQ-ARCHIVE-001, RQ-ARCHIVE-014, RQ-ARCHIVE-016, RQ-ARCHIVE-018

### DSG-LAS-002 `VM-hosted Compose runtime`

The first executable realization is a VM-hosted batch workflow whose user-facing
runtime entrypoint is Docker Compose.

The host boot mechanism is intentionally outside this spec package, but the
Compose entrypoint must be invocable without interactive prompts and must
support one normal production run after boot.

This keeps `lexonarchivebuilder-archive-sync` aligned with the approved
VM-startup and VM-shutdown lifecycle without requiring a new repository-local
control plane.

**Traces to:** RQ-ARCHIVE-002, RQ-ARCHIVE-002A, RQ-ARCHIVE-003A,
RQ-ARCHIVE-017

### DSG-LAS-003 `Ordered workflow state machine`

The workflow realizes one run as a stage-ordered state machine:

1. acquire or resume the rsync source snapshot through the Azure-backed `BlockStore` boundary
2. discover mailbox artifacts from that snapshot
3. admit missing mailbox blocks through the shared `BlockStore` boundary
4. derive and persist missing chunk blocks for newly admitted mailboxes
5. generate and durably record missing embeddings for newly admitted chunks
6. produce a valid published root for the active work set only after embedding work is complete
7. append a root-history entry for the successful generation
8. persist final audit state and trigger terminal VM shutdown

The journal records the active state-machine position, source snapshot identity,
generation identity, and the pending or completed work inventory needed to
resume safely after interruption.

**Traces to:** RQ-ARCHIVE-004, RQ-ARCHIVE-005, RQ-ARCHIVE-006,
RQ-ARCHIVE-007, RQ-ARCHIVE-008, RQ-ARCHIVE-009, RQ-ARCHIVE-010,
RQ-ARCHIVE-011, RQ-ARCHIVE-012

## Source and Artifact Design

### DSG-LAS-004 `Azure-backed BlockStore rsync source snapshot`

The rsync stage materializes the first approved source
`rsync.ietf.org::mailman-archive/` as an Azure Blob-backed source snapshot
whose payloads and manifests are persisted through the updated LexonGraph
`BlockStore` realization and are suitable for later mailbox discovery and
restart-safe continuation.

The workflow treats this as a source-acquisition concern rather than as an
indexer concern. The design expects the journal to bind downstream work to a
specific logical source snapshot identity so later audit and resume decisions
can reason about one mirrored snapshot rather than about an implicit live rsync
view.

**Traces to:** RQ-ARCHIVE-004, RQ-ARCHIVE-004A, RQ-ARCHIVE-011C

### DSG-LAS-004A `Source snapshot identity binding`

Each completed or resumable source-acquisition operation produces one source
snapshot identity for the effective mirrored corpus.

The design binds downstream mailbox admission, audit evidence, and root
publication to that source snapshot identity so repeated runs can prove whether
they operated on the same effective source corpus.

When the effective mirrored corpus is unchanged, the design requires the same
source snapshot identity rather than a fresh execution-local identifier.

**Traces to:** RQ-ARCHIVE-004B, RQ-ARCHIVE-011C

### DSG-LAS-004B `Source snapshot provenance`

The source-acquisition stage preserves workflow-owned provenance sufficient to
identify the mirrored corpus behind each source snapshot identity.

That provenance is expected to include the rsync source URI and acquisition
evidence sufficient to distinguish complete, partial, and changed mirror states
without requiring operators to infer corpus identity from a bare snapshot token.

**Traces to:** RQ-ARCHIVE-004C

### DSG-LAS-004C `Shared source-snapshot storage seam`

Source-snapshot payloads and manifests are stored through the same stable
production `BlockStore` abstraction family used for immutable workflow
artifacts when the required Azure-backed realization is available.

This keeps source acquisition on the same production storage seam as mailbox,
chunk, and later immutable publication artifacts while preventing higher-level
workflow stages from depending on raw Azure Blob API call shapes.

If the updated upstream `BlockStore` surface does not directly expose the
manifest-addressable semantics archive-sync needs, archive-sync may layer a
repository-owned manifest convention on that seam without redefining the seam
itself.

**Traces to:** RQ-ARCHIVE-004D, RQ-ARCHIVE-014, RQ-ARCHIVE-018

### DSG-LAS-004D `V2 custom blocks for source snapshots`

`lexonarchivebuilder-archive-sync` uses LexonGraph v2 custom blocks for
source-snapshot payload and manifest blocks.

The workflow does not add a repository-owned bridge between v1 and v2 for those
source-snapshot artifacts. If the format transition invalidates an existing
source-snapshot store, operators may rebuild that store and rerun source
acquisition so resume and audit guarantees continue within the v2 custom-block
contract rather than across mixed-format snapshot state.

Downstream mailbox, chunk, embedding, and index flows remain on their current
delegated contracts in this increment.

**Traces to:** RQ-ARCHIVE-004E, RQ-ARCHIVE-011, RQ-ARCHIVE-011C

### DSG-LAS-005 `Mailbox admission and delta derivation`

After source-snapshot acquisition, the workflow discovers mailbox artifacts
from the acquired snapshot and attempts mailbox-block admission through the shared
`BlockStore` abstraction family.

Mailbox artifacts already present in the block store are treated as committed
prior work for this workflow snapshot. Only newly admitted mailbox blocks create
new chunk-derivation work, and only newly admitted chunk blocks create new
embedding work.

This keeps stage advancement delta-oriented and restart-safe without requiring a
parallel repository-local storage abstraction.

**Traces to:** RQ-ARCHIVE-005, RQ-ARCHIVE-006, RQ-ARCHIVE-007,
RQ-ARCHIVE-011B

### DSG-LAS-006 `Journal as checkpoint and audit ledger`

The workflow maintains one durable journal that serves both as the restart
checkpoint ledger and as the audit ledger for a run.

At minimum, the journal design must preserve:

- current workflow stage
- source snapshot identity
- generation identity
- effective indexing configuration identity
- pending and completed mailbox, chunk, embedding, indexing, and publication
  work inventories
- enough provenance to connect a published root back to the mirrored source
  snapshot and effective run configuration
- terminal outcome status

The requirements intentionally leave exact serialization open, but the design
fixes the journal as the workflow-owned authority for both resume and audit.

**Traces to:** RQ-ARCHIVE-011, RQ-ARCHIVE-011A, RQ-ARCHIVE-011B,
RQ-ARCHIVE-011C, RQ-ARCHIVE-011D, RQ-ARCHIVE-013

### DSG-LAS-006A `Root reproducibility evidence`

The audit portion of the journal must preserve enough evidence to support the
approved reproducibility rule:

- when the logical source snapshot and effective indexing configuration are unchanged,
  repeated runs are expected to reproduce the same published root
- when a repeated run produces a different root, the recorded evidence must be
  sufficient to explain the difference

The design therefore ties each published root to the effective source snapshot,
generation identity, and effective indexing configuration rather than treating
the root as an unexplained terminal value.

**Traces to:** RQ-ARCHIVE-010, RQ-ARCHIVE-010A, RQ-ARCHIVE-011, RQ-ARCHIVE-011C,
RQ-ARCHIVE-011D

### DSG-LAS-006B `Checkpoint granularity`

Workflow checkpoints are placed at committed mailbox, chunk, embedding, and
publication boundaries so a successfully checkpointed committed operation does
not need to be re-executed after restart.

This design constrains correctness of checkpoint boundaries without fixing a
timer-based checkpoint cadence.

**Traces to:** RQ-ARCHIVE-011A, RQ-ARCHIVE-011B, RQ-ARCHIVE-011E

### DSG-LAS-006C `Workflow journal authority`

The archive-sync journal is authoritative for workflow-stage control and resume
decisions.

Any downstream `lexonarchivebuilder-indexer` replay journal remains subordinate
to that authority and exists to support delegated indexing behavior below the
workflow-stage boundary rather than to redefine stage ownership.

The design therefore requires explicit reconciliation logic between these
journal layers instead of treating them as independent peer authorities.

**Traces to:** RQ-ARCHIVE-011F

## Integration Design

### DSG-LAS-007 `Embedding-complete published-root barrier`

Published-root generation is unlocked only when the current journal state contains no
remaining required embedding work for the active work set.

This barrier is workflow-owned. The design does not rely on a best-effort or
time-based guess that embeddings have probably finished; instead, the workflow
advances to published-root generation only from a durable `embedding complete
for this work set` state.

**Traces to:** RQ-ARCHIVE-008, RQ-ARCHIVE-009, RQ-ARCHIVE-011

### DSG-LAS-007A `Work-set freeze boundary`

Before published-root generation begins, the workflow freezes the active work
set for that generation.

Any newly discovered source or derived artifacts after that boundary are queued
for a later generation rather than modifying the generation already progressing
toward publication.

**Traces to:** RQ-ARCHIVE-008A

### DSG-LAS-008 `Delegated index recomputation seam`

When the workflow reaches the embedding-complete state, the current design
realizes published-root generation by delegating index recomputation through
existing `lexonarchivebuilder-indexer` capabilities or through approved
extensions to those capabilities rather than defining a second repository-local
indexing implementation.

The archive-sync workflow owns orchestration and gating. The delegated indexer
surface remains authoritative for index-construction internals and any
repository-owned replay details below that seam.

The workflow records one effective indexing configuration identity for the
root-affecting inputs that participate in this delegated realization seam.

**Traces to:** RQ-ARCHIVE-009, RQ-ARCHIVE-010B, RQ-ARCHIVE-014, RQ-ARCHIVE-018

### DSG-LAS-009 `Append-only root-history publication`

After each successful generation, the workflow appends one new root-history
record to an Azure Blob JSON artifact.

The design expects that record to remain linkable to the corresponding journaled
generation identity and source snapshot identity so later audit can explain why
a root was reproduced or changed.

At minimum, the design expects each entry to carry provenance sufficient to
identify:

- the published root
- the source snapshot identity
- the generation identity
- the effective indexing configuration identity
- the publication timestamp
- a workflow-owned audit linkage such as a journal identifier

This publication step occurs only after a successful published-root result is
available, occurs once per successful generation even when the root repeats, and
is itself journaled so restart logic can avoid duplicate publication.

Publication is not considered fully complete until the root-history entry is
durably recorded or the journal durably preserves enough information to repair
that append on resume without regenerating the root.

The append-only root-history log is treated as a workflow audit artifact rather
than as a mutable published root artifact.

**Traces to:** RQ-ARCHIVE-010, RQ-ARCHIVE-010A, RQ-ARCHIVE-010B,
RQ-ARCHIVE-010C, RQ-ARCHIVE-011B, RQ-ARCHIVE-011C, RQ-ARCHIVE-011D

### DSG-LAS-010 `Terminal failure preservation and shutdown`

On terminal non-recoverable failure, the workflow first persists failure-adjacent
journal state, including the final known stage and pending work inventory, and
then triggers VM shutdown.

On successful completion, the workflow persists final success state after root
publication and then also triggers VM shutdown.

Abrupt spot interruption is not modeled as a graceful terminal failure inside
the workflow. Instead, restart after the next boot is handled through the
journaled checkpoint path.

**Traces to:** RQ-ARCHIVE-011A, RQ-ARCHIVE-012, RQ-ARCHIVE-013

### DSG-LAS-010A `Immutable publication model`

The workflow never mutates previously published mailbox blocks, chunk blocks,
embeddings, index blocks, or published root artifacts in place.

New information is represented by new immutable artifacts and by appended
publication-history entries.

**Traces to:** RQ-ARCHIVE-019

## Extensibility and Invariant Design

### DSG-LAS-011 `Future content-type extensibility`

The workflow stage machine is defined in terms of generic source acquisition,
artifact admission, chunk derivation, embedding, index recomputation, and root
publication stages.

Mailbox-specific logic is the first concrete realization inside those stages.
Future content types and source artifacts such as RFCs, Internet Drafts,
Datatracker metadata, and Working Group metadata should be addable by extending
stage-local derivation logic and work-item identity rules without redefining the
workflow boundary, journal contract, or root-publication contract.

**Traces to:** RQ-ARCHIVE-015, RQ-ARCHIVE-018

### DSG-LAS-012 `No new search-serving or control-plane surface`

The workflow remains a batch ingestion boundary. It neither introduces a new MCP
surface nor a new long-lived repository control plane.

Search-serving behavior remains unchanged, and operational visibility for this
increment is carried by the workflow journal and its related durable artifacts
rather than by a new continuously running service layer.

**Traces to:** RQ-ARCHIVE-016, RQ-ARCHIVE-017

## Implementation Work Remaining

This section records the currently known implementation breakdown for the
approved `lexonarchivebuilder-archive-sync` design. It distinguishes upstream
dependencies from repository-owned follow-on work so future implementation
planning can preserve the approved ownership boundaries.

### Upstream LexonGraph dependencies

1. Azure-oriented production embedding support suitable for the approved
   embedding-provider boundary

This item is expected to be built in LexonGraph rather than reimplemented in
LexonArchiveBuilder. Until it exists at the required contract level,
`lexonarchivebuilder-archive-sync` can finalize its repository-owned
orchestration but cannot complete an end-to-end production realization.

### Repository-owned follow-on work

1. Add a new `lexonarchivebuilder-archive-sync` runtime entrypoint and Docker
   Compose workflow distinct from the existing local scale-test wrapper
2. Implement Azure-backed rsync snapshot acquisition and restart-safe source
   progress tracking for `rsync.ietf.org::mailman-archive/` through the updated
   LexonGraph `BlockStore` seam, adding a repository-owned manifest convention
   only if the upstream seam does not already expose the required semantics
3. Implement the workflow-level journal as both a restart checkpoint ledger and
   an audit ledger
4. Implement delta-oriented mailbox admission, chunk derivation, and embedding
   work discovery so previously committed work is not repeated
5. Integrate the embedding-complete barrier and work-set freeze with delegated
   published-root generation through existing `lexonarchivebuilder-indexer`
   seams or approved extensions
6. Define and persist effective indexing configuration identity for each
   successful or failed generation
7. Implement append-only root-history publication in Azure Blob Storage with
   duplicate-safe resume behavior and durable history-repair semantics
8. Implement terminal success and terminal non-recoverable failure shutdown
   handling that preserves audit state before VM shutdown
9. Document the boot-launch assumptions and operational handoff needed to run
   the Compose workflow automatically on VM startup

### Ownership notes

- The existing mailbox normalization, chunk derivation, and split-stage replay
  surfaces in `lexonarchivebuilder-indexer` are expected reuse points rather
  than greenfield replacements.
- The workflow journal defined here is broader than the current
  `lexonarchivebuilder-indexer` replay journal because it must cover source
  acquisition, audit evidence, root publication, and terminal shutdown state in
  addition to replayable indexing inputs.
- If upstream LexonGraph delivery changes the exact production adapter surface,
  this section should be revised before implementation begins so repository work
  remains aligned to the approved boundary design.
