# LexonArchiveBuilder Archive Sync Design

## Status

Phase 2 specification patch for the approved production-only
`lexonarchivebuilder-archive-sync` workflow in
`docs/specs/lexonarchivebuilder-archive-sync/requirements.md`.

## Scope

This document specifies the LexonArchiveBuilder-owned design for realizing
`lexonarchivebuilder-archive-sync` as a VM-hosted, boot-triggered, Docker
Compose-launched production workflow that mirrors
`rsync.ietf.org::mailman-archive/` into Azure Blob Storage, admits mailbox and
chunk artifacts through the shared `BlockStore` boundary, generates embeddings
for newly admitted chunks, gates index recomputation on embedding completion,
publishes append-only root history, preserves restart-safe and audit-safe
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
- Azure-backed storage, journal, and root-history publication assets
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
- aligned with Azure Blob Storage plus Azure-oriented production embeddings
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

1. acquire or resume the rsync mirror snapshot in Azure Blob Storage
2. discover mailbox artifacts from that snapshot
3. admit missing mailbox blocks through the shared `BlockStore` boundary
4. derive and persist missing chunk blocks for newly admitted mailboxes
5. generate and durably record missing embeddings for newly admitted chunks
6. recompute the index tree only after embedding work is complete
7. append a root-history entry for the successful recomputation
8. persist final audit state and trigger terminal VM shutdown

The journal records the active state-machine position and the pending or
completed work inventory needed to resume safely after interruption.

**Traces to:** RQ-ARCHIVE-004, RQ-ARCHIVE-005, RQ-ARCHIVE-006,
RQ-ARCHIVE-007, RQ-ARCHIVE-008, RQ-ARCHIVE-009, RQ-ARCHIVE-010,
RQ-ARCHIVE-011, RQ-ARCHIVE-012

## Source and Artifact Design

### DSG-LAS-004 `Azure-backed rsync mirror snapshot`

The rsync stage materializes the first approved source
`rsync.ietf.org::mailman-archive/` into an Azure Blob-backed mirror snapshot
that is suitable for later mailbox discovery and restart-safe continuation.

The workflow treats this as a source-acquisition concern rather than as an
indexer concern. The design expects the journal to bind downstream work to a
specific logical source snapshot identity so later audit and resume decisions
can reason about one mirrored snapshot rather than about an implicit live rsync
view.

**Traces to:** RQ-ARCHIVE-004, RQ-ARCHIVE-004A, RQ-ARCHIVE-011C

### DSG-LAS-005 `Mailbox admission and delta derivation`

After source mirroring, the workflow discovers mailbox artifacts from the
mirrored snapshot and attempts mailbox-block admission through the shared
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
- effective workflow configuration identity
- pending and completed mailbox, chunk, embedding, indexing, and publication
  work inventories
- enough provenance to connect a published root back to the mirrored source
  snapshot and effective run configuration
- terminal outcome status

The requirements intentionally leave exact serialization open, but the design
fixes the journal as the workflow-owned authority for both resume and audit.

**Traces to:** RQ-ARCHIVE-011, RQ-ARCHIVE-011A, RQ-ARCHIVE-011B,
RQ-ARCHIVE-011C, RQ-ARCHIVE-013

### DSG-LAS-006A `Root reproducibility evidence`

The audit portion of the journal must preserve enough evidence to support the
approved reproducibility rule:

- when the logical source snapshot and effective configuration are unchanged,
  repeated runs are expected to reproduce the same published root
- when a repeated run produces a different root, the recorded evidence must be
  sufficient to explain the difference

The design therefore ties each published root to the effective source snapshot
and effective workflow configuration rather than treating the root as an
unexplained terminal value.

**Traces to:** RQ-ARCHIVE-010, RQ-ARCHIVE-011, RQ-ARCHIVE-011C

## Integration Design

### DSG-LAS-007 `Embedding-complete indexing barrier`

Index recomputation is unlocked only when the current journal state contains no
remaining required embedding work for the active work set.

This barrier is workflow-owned. The design does not rely on a best-effort or
time-based guess that embeddings have probably finished; instead, the workflow
advances to index recomputation only from a durable `embedding complete for this
work set` state.

**Traces to:** RQ-ARCHIVE-008, RQ-ARCHIVE-009, RQ-ARCHIVE-011

### DSG-LAS-008 `Delegated index recomputation seam`

When the workflow reaches the embedding-complete state, it delegates index
recomputation through existing `lexonarchivebuilder-indexer` capabilities or
through approved extensions to those capabilities rather than defining a second
repository-local indexing implementation.

The archive-sync workflow owns orchestration and gating. The delegated indexer
surface remains authoritative for index-construction internals and any
repository-owned replay details below that seam.

**Traces to:** RQ-ARCHIVE-009, RQ-ARCHIVE-014, RQ-ARCHIVE-018

### DSG-LAS-009 `Append-only root-history publication`

After successful delegated index recomputation, the workflow appends one new
root-history record to an Azure Blob JSON artifact.

The design expects that record to remain linkable to the corresponding journaled
run identity and source snapshot identity so later audit can explain why a root
was reproduced or changed.

This publication step occurs only after a successful recomputation result is
available and is itself journaled so restart logic can avoid duplicate
publication.

**Traces to:** RQ-ARCHIVE-010, RQ-ARCHIVE-011B, RQ-ARCHIVE-011C

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

## Extensibility and Invariant Design

### DSG-LAS-011 `Future content-type extensibility`

The workflow stage machine is defined in terms of generic source acquisition,
artifact admission, chunk derivation, embedding, index recomputation, and root
publication stages.

Mailbox-specific logic is the first concrete realization inside those stages.
Future content types should be addable by extending stage-local derivation logic
and work-item identity rules without redefining the workflow boundary, journal
contract, or root-publication contract.

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

1. Azure Blob-backed `BlockStore` support suitable for the production workflow's
   mailbox, chunk, and root-publication artifact persistence path
2. Azure-oriented production embedding support suitable for the approved
   embedding-provider boundary

These items are expected to be built in LexonGraph rather than reimplemented in
LexonArchiveBuilder. Until they exist at the required contract level,
`lexonarchivebuilder-archive-sync` can finalize its repository-owned
orchestration but cannot complete an end-to-end production realization.

### Repository-owned follow-on work

1. Add a new `lexonarchivebuilder-archive-sync` runtime entrypoint and Docker
   Compose workflow distinct from the existing local scale-test wrapper
2. Implement Azure-backed rsync snapshot acquisition and restart-safe source
   progress tracking for `rsync.ietf.org::mailman-archive/`
3. Implement the workflow-level journal as both a restart checkpoint ledger and
   an audit ledger
4. Implement delta-oriented mailbox admission, chunk derivation, and embedding
   work discovery so previously committed work is not repeated
5. Integrate the embedding-complete barrier with delegated index recomputation
   through existing `lexonarchivebuilder-indexer` seams or approved extensions
6. Implement append-only root-history publication in Azure Blob Storage with
   duplicate-safe resume behavior
7. Implement terminal success and terminal non-recoverable failure shutdown
   handling that preserves audit state before VM shutdown
8. Document the boot-launch assumptions and operational handoff needed to run
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
