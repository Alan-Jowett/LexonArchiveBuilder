# LexonArchiveBuilder Archive Sync Validation

## Status

Phase 2 validation patch for the approved production-only
`lexonarchivebuilder-archive-sync` workflow in
`docs/specs/lexonarchivebuilder-archive-sync/requirements.md` and
`docs/specs/lexonarchivebuilder-archive-sync/design.md`.

## Validation Scope

These validation entries define the expected conformance surface for the
LexonArchiveBuilder-owned `lexonarchivebuilder-archive-sync` workflow boundary.

This package validates workflow-owned orchestration, journal durability,
auditability, delegated indexer integration, root-history publication, and VM
shutdown behavior. It does not redefine validation already owned by
`lexonarchivebuilder-indexer`, LexonGraph, or `lexonarchivebuilder-mcp`.

## Validation Entries

### VAL-LAS-001

Inspect the repository surface for `lexonarchivebuilder-archive-sync`.

**Pass condition:** the workflow is specified as a separate production workflow
boundary above existing indexer and MCP surfaces rather than as part of
`lexonarchivebuilder-indexer` or `lexonarchivebuilder-mcp`.

**Traces to:** RQ-ARCHIVE-001, RQ-ARCHIVE-016, DSG-LAS-001, DSG-LAS-012

### VAL-LAS-002

Inspect the first executable runtime shape for `lexonarchivebuilder-archive-sync`.

**Pass condition:** the workflow is realizable as a VM-hosted, Docker
Compose-launched, non-interactive batch workflow compatible with machine boot
and VM shutdown, remains production-only for this increment, and does not
introduce a repository-local long-lived control plane or require a local/testing
archive-sync entrypoint.

**Traces to:** RQ-ARCHIVE-002, RQ-ARCHIVE-002A, RQ-ARCHIVE-003,
RQ-ARCHIVE-003A,
RQ-ARCHIVE-017, DSG-LAS-002, DSG-LAS-012

### VAL-LAS-003

Execute or inspect a representative production run that starts from an empty
workflow state.

**Pass condition:** the workflow realizes the approved stage order of rsync
mirror acquisition, mailbox discovery or admission, chunk derivation, embedding
generation, embedding-gated index recomputation, root-history publication, and
terminal shutdown.

**Traces to:** RQ-ARCHIVE-004, RQ-ARCHIVE-005, RQ-ARCHIVE-006,
RQ-ARCHIVE-007, RQ-ARCHIVE-008, RQ-ARCHIVE-009, RQ-ARCHIVE-010,
RQ-ARCHIVE-012, DSG-LAS-003

### VAL-LAS-003A

Inspect the Azure-backed source mirror state across an interrupted and resumed
run.

**Pass condition:** the workflow can resume source acquisition from a durable
mirror snapshot or equivalent recorded progress without requiring a full re-fetch
when the already copied snapshot remains valid.

**Traces to:** RQ-ARCHIVE-004A, RQ-ARCHIVE-011A, DSG-LAS-004, DSG-LAS-006

### VAL-LAS-004

Run the workflow against a snapshot containing a mix of already present and
previously unseen mailbox artifacts.

**Pass condition:** mailbox artifacts already present in the block store are not
re-admitted, only newly admitted mailbox blocks create chunk-derivation work,
and only newly admitted chunk blocks create embedding work.

**Traces to:** RQ-ARCHIVE-005, RQ-ARCHIVE-006, RQ-ARCHIVE-007,
RQ-ARCHIVE-011B, DSG-LAS-005

### VAL-LAS-005

Interrupt the workflow after at least one mailbox admission, chunk persistence,
or embedding completion has been durably committed, then restart it.

**Pass condition:** the journaled checkpoint path resumes from durable state
without duplicating previously committed mailbox blocks, chunk blocks,
embeddings, or root publications.

**Traces to:** RQ-ARCHIVE-011, RQ-ARCHIVE-011A, RQ-ARCHIVE-011B,
DSG-LAS-006, DSG-LAS-009, DSG-LAS-010

### VAL-LAS-005A

Inspect the journal artifact for a completed run.

**Pass condition:** the journal records the active or final workflow stage, the
effective source snapshot identity, the effective workflow-configuration
identity, the pending or completed work inventories needed for resume, and the
published-root outcome needed for audit.

**Traces to:** RQ-ARCHIVE-011, RQ-ARCHIVE-011C, DSG-LAS-006,
DSG-LAS-006A

### VAL-LAS-005B

Repeat the workflow against the same logical source snapshot and effective
configuration, then compare outcomes.

**Pass condition:** the repeated run either reproduces the same published root
or emits enough journaled evidence to explain why the root changed.

**Traces to:** RQ-ARCHIVE-011C, DSG-LAS-006A, DSG-LAS-009

### VAL-LAS-006

Interrupt the workflow after chunk persistence but before all required
embeddings are complete, then restart it.

**Pass condition:** the resumed run does not trigger index recomputation until
all required embeddings for the active work set are durably complete.

**Traces to:** RQ-ARCHIVE-008, RQ-ARCHIVE-011, DSG-LAS-007

### VAL-LAS-007

Inspect the delegated indexing seam for a completed indexing stage.

**Pass condition:** the workflow delegates index recomputation through existing
`lexonarchivebuilder-indexer` capabilities or approved indexer extensions rather
than introducing a second repository-local indexing implementation.

**Traces to:** RQ-ARCHIVE-009, RQ-ARCHIVE-014, DSG-LAS-008

### VAL-LAS-007A

Inspect the production storage and embedding integration seams used by
`lexonarchivebuilder-archive-sync`.

**Pass condition:** Azure Blob Storage and production embedding realizations stay
behind stable workflow integration boundaries, so stage contracts are not
rewritten around Azure-specific call shapes at each step.

**Traces to:** RQ-ARCHIVE-014, RQ-ARCHIVE-018, DSG-LAS-001, DSG-LAS-008

### VAL-LAS-008

Inspect the root-history publication artifact after one successful run and after
a later resumed or repeated run.

**Pass condition:** successful index recomputation appends a new root-history
record in Azure Blob Storage, the publication step is durable across restart,
and repeated publication does not silently duplicate one logical publish event.

**Traces to:** RQ-ARCHIVE-010, RQ-ARCHIVE-011B, RQ-ARCHIVE-011C,
DSG-LAS-009

### VAL-LAS-009

Force an abrupt stop equivalent to spot-instance interruption during any
non-terminal workflow stage, then restart on the next boot.

**Pass condition:** the workflow resumes from durable journal state, does not
require in-memory recovery, and can continue toward the same terminal outcome
without redoing already committed work.

**Traces to:** RQ-ARCHIVE-011A, RQ-ARCHIVE-011B, DSG-LAS-006, DSG-LAS-010

### VAL-LAS-010

Trigger a terminal non-recoverable failure after some durable work has already
been committed.

**Pass condition:** the workflow persists failure-adjacent journal state before
initiating VM shutdown, so the final known stage and pending work inventory
remain available for audit after the VM is stopped.

**Traces to:** RQ-ARCHIVE-012, RQ-ARCHIVE-013, DSG-LAS-010

### VAL-LAS-011

Inspect the workflow scope against MCP and search-serving artifacts.

**Pass condition:** `lexonarchivebuilder-archive-sync` does not redefine MCP
request or response contracts, does not change search semantics, and remains an
ingestion or publication workflow only.

**Traces to:** RQ-ARCHIVE-016, DSG-LAS-001, DSG-LAS-012

### VAL-LAS-012

Add a future non-mailbox content type to the workflow design hypothetically.

**Pass condition:** the new content type can fit by extending source or
stage-local derivation logic behind the existing journaled workflow stages
without redefining the top-level workflow boundary or the root-publication
contract.

**Traces to:** RQ-ARCHIVE-015, RQ-ARCHIVE-018, DSG-LAS-011
