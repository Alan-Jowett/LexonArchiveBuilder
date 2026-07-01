<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder Experiment Workflow Design

## Status

Phase 2 specification patch for the approved hosted staged experiment-workflow
boundary in
`docs/specs/lexonarchivebuilder-experiment-workflow/requirements.md`.

## Scope

This document specifies the LexonArchiveBuilder-owned design for realizing a
hosted GitHub Actions workflow family that:

1. refreshes a reusable embedding dataset for a checked-in working-group
   manifest
2. runs an indexing experiment against that reusable dataset for one selected
   published-profile version
3. retrieves operator-relevant outputs from Azure Blob Storage and republishes
   them as workflow artifacts

This document is layered on top of:

- `docs/specs/lexonarchivebuilder-experiment-workflow/requirements.md`
- `docs/specs/lexonarchivebuilder-indexer/requirements.md`
- `docs/specs/lexonarchivebuilder-indexer/design.md`
- `docs/specs/lexonarchivebuilder-deployment/requirements.md`
- `docs/specs/lexonarchivebuilder-image-publishing/requirements.md`
- `test.ps1`
- `README.md`

This document does not redefine indexer semantics, replay-journal semantics
already owned by the indexer boundary, MCP semantics, or production-serving
CDN retrieval behavior.

## Impact Map

### Directly affected artifacts

- `docs/specs/lexonarchivebuilder-experiment-workflow/requirements.md`
- `docs/specs/lexonarchivebuilder-experiment-workflow/design.md`
- `docs/specs/lexonarchivebuilder-experiment-workflow/validation.md`

### Indirectly affected artifacts

- hosted GitHub Actions workflow definitions under `.github/workflows/`
- experiment-specific IaC and VM bootstrap assets
- checked-in manifest assets that define reusable embedding datasets
- published lab images consumed by the hosted workflows
- Azure operator guidance for post-run inspection and manual cleanup

### Unaffected artifacts

- `docs/specs/lexonarchivebuilder-mcp/*`
- MCP request/response semantics
- container-image publication semantics beyond being consumed as an input
- production-serving CDN publication and retrieval semantics
- local-only wrapper semantics outside the shared rsync-source and staged-indexer
  boundaries they already own

## Design Goals

The `lexonarchivebuilder-experiment-workflow` design is intended to be:

- staged
- reproducible
- explicit about reuse of expensive embedding work
- aligned with existing stage-selectable indexer behavior
- explicit about hosted-versus-break-glass operator paths
- minimal in new semantic surface area
- Azure-backed without requiring a local control machine
- compatible with manual post-run inspection
- non-invasive to existing indexer, scale-test, MCP, and production-serving
  contracts

## Boundary Design

### DSG-EXP-001 `Hosted workflow-family boundary`

`lexonarchivebuilder-experiment-workflow` owns hosted orchestration,
checked-in reusable-dataset intent, Azure run coordination, artifact retrieval,
and workflow-visible pass/fail reporting.

It does not own mailbox parsing semantics, embedding semantics, replay-journal
format semantics, block-construction semantics, MCP-serving semantics, or CDN
publication semantics.

**Traces to:** RQ-EXP-001, RQ-EXP-020, RQ-EXP-021

### DSG-EXP-002 `Two-workflow staged realization`

The repository realizes this boundary as two hosted workflow entrypoints rather
than one monolithic workflow:

1. an embedding-refresh workflow for reusable dataset preparation
2. an indexing-experiment workflow for profile evaluation over that dataset

This separation makes the expensive embedding step durable and reusable across
multiple experiment runs, instead of coupling every experiment rerun to fresh
embedding work.

**Traces to:** RQ-EXP-002, RQ-EXP-008

### DSG-EXP-003 `Checked-in reusable-dataset manifest`

Each reusable embedding dataset is described by one checked-in manifest asset
that owns:

1. the full rsync URL list for the working-group corpus
2. the Azure Blob container name that stores the reusable embedding dataset

The manifest is repository-owned intent rather than workflow-run-local pasted
state. That design keeps the reusable corpus definition reviewable, versioned,
and shareable across embedding refresh and downstream indexing experiments.

The first specification baseline does not freeze a specific schema encoding for
the manifest so long as it remains a checked-in file with those required fields.

**Traces to:** RQ-EXP-005, RQ-EXP-006

### DSG-EXP-003A `Hosted block-store target contract`

The hosted workflow family preserves a caller-visible block-store selection
contract for the experiment path instead of hard-coding one storage realization
forever.

The design baseline distinguishes:

1. the currently available regular filesystem block-store path
2. the approved overlay block-store path, which layers memory, local
   filesystem, and Azure Blob Storage so
   writes persist to Azure while reads are satisfied by the first layer that has
   the requested data

Both hosted workflows use the same two-target contract, and the default
selection is `overlay` whenever the caller does not explicitly choose a target.

The design therefore treats overlay as an executable hosted mode rather than a
future-only seam while preserving explicit filesystem selection for comparison,
fallback, and local-parity investigations.

**Traces to:** RQ-EXP-008A, RQ-EXP-008B, RQ-EXP-020

### DSG-EXP-004 `Embedding-refresh as stage-selectable ingestion`

The embedding-refresh workflow reuses the existing indexer stage-selectable
execution boundary and maps its VM-side work to the approved
`ingestion-and-embedding` class of behavior.

Within that staged design:

1. the workflow resolves the selected checked-in manifest
2. the VM-side run acquires the manifest-defined rsync-backed content
3. the VM-side run performs only the ingestion-plus-embedding stage needed to
   persist reusable embedding-side state
4. the run persists the reusable embedding dataset and replay journal to Azure
   Blob Storage

The embedding-refresh workflow therefore remains subordinate to the indexer
boundary's existing stage model rather than inventing a second workflow-local
embedding protocol.

**Traces to:** RQ-EXP-007, RQ-EXP-012, RQ-EXP-013

### DSG-EXP-005 `Incremental reusable-dataset refresh`

The reusable embedding dataset is refreshed incrementally against the
manifest-selected corpus and container rather than rebuilt from scratch on every
workflow invocation.

The design baseline is:

1. the checked-in manifest identifies the stable reusable-dataset identity
2. Azure Blob Storage holds the current persisted embedding dataset and replay
   journal for that identity
3. a refresh invocation extends that persisted state only for newly required
   embeddings when the source corpus has grown or changed

This design intentionally relies on the repository-owned replay-journaled
split-stage seam already defined by the indexer boundary, because that seam is
the repository-approved mechanism for resumable ingestion and clustering-only
reuse.

The specification layer does not freeze one freshness-detection algorithm here;
it only constrains the observable contract to incremental extension rather than
whole-dataset recomputation in the normal case.

**Traces to:** RQ-EXP-007, RQ-EXP-020

### DSG-EXP-006 `Indexing experiment as replay-backed clustering`

The indexing-experiment workflow consumes the reusable embedding dataset
identified by the selected checked-in manifest and maps its VM-side work to the
approved `clustering-and-block-assembly` class of behavior.

Within that staged design:

1. the workflow resolves the selected checked-in manifest
2. the VM-side run opens the corresponding reusable embedding dataset and replay
   journal from Azure Blob Storage
3. the run executes the indexing experiment for one caller-selected
   published-profile version
4. the run writes experiment outputs back to Azure Blob Storage for later
   retrieval

This preserves the indexer boundary's approved split-stage model in which
clustering-plus-assembly reuses persisted replay-safe state instead of
requiring a preceding ingestion phase in the same invocation.

**Traces to:** RQ-EXP-008, RQ-EXP-009, RQ-EXP-014

### DSG-EXP-006A `Rooted quality/report completion`

The indexing-experiment workflow does not stop at clustering-plus-block-assembly.
To remain comparable in nature to `test.ps1`, the hosted workflow continues from
the produced rooted output into the existing rooted quality/report step and
publishes that report family as the primary experiment result.

The design therefore mirrors the existing local evaluation shape:

1. staged clustering-and-block-assembly over reusable persisted inputs
2. rooted quality/report generation over the produced root
3. operator-visible experiment result publication

This keeps the hosted workflow aligned with the repository's existing profile
evaluation narrative instead of inventing a weaker "index-only" experiment
definition.

**Traces to:** RQ-EXP-014, RQ-EXP-014A, RQ-EXP-015

### DSG-EXP-007 `Published-image selection contract`

Both hosted workflows consume lab-published container images rather than
building repository-local images as part of the normal workflow path.

The design baseline provides:

1. one default runner-image tag aligned to the lab pipeline's published `main`
   tag
2. one caller-visible override path for a specific published tag

This keeps hosted experiment execution aligned with the repository's existing
image-publication boundary and makes workflow runs reproducible against known
published images.

**Traces to:** RQ-EXP-010

### DSG-EXP-008 `Hosted-orchestration versus break-glass access`

The hosted workflows are designed to complete their normal success path without
interactive SSH use.

The SSH public key input exists only to seed a break-glass debugging path on
the VM when a run fails or behaves unexpectedly. Operator investigation is
therefore subordinate to the hosted workflow boundary rather than part of the
expected execution contract.

**Traces to:** RQ-EXP-003, RQ-EXP-011

### DSG-EXP-009 `Federated Azure orchestration path`

The hosted workflows authenticate to Azure through a repository-owned GitHub
Actions federation path and then orchestrate the Azure run from the workflow
runner.

The orchestration path remains distinct from:

- developer-local Azure login state
- long-lived repository secrets that impersonate a local operator session
- a repository-owned always-on control plane

This preserves hosted repeatability while keeping the workflow family inside the
existing no-control-plane architectural direction.

**Traces to:** RQ-EXP-003, RQ-EXP-004, RQ-EXP-021

### DSG-EXP-010 `Minimal Azure execution shape`

Each hosted workflow invocation provisions or selects only the Azure resources
needed for one VM-hosted run plus artifact persistence and inspection.

The common execution shape is:

1. one VM that runs the selected workflow's container workload
2. one Blob container reachable through an approved SAS-backed access path
3. surfaced deployment identifiers including resource-group and storage-account
   names for manual inspection

The first design baseline leaves the exact Bicep module factoring open but
constrains the hosted workflow to remain a minimal experiment-orchestration
surface rather than a production-serving deployment expansion.

**Traces to:** RQ-EXP-012, RQ-EXP-017, RQ-EXP-021

### DSG-EXP-010A `Hosted overlay-target execution wiring`

The hosted workflow family wires the approved two-target block-store contract
through workflow inputs, VM bootstrap inputs, and downstream container
invocation without changing the high-level two-workflow contract.

Within that wiring:

1. both workflows accept explicit caller selection of `filesystem` or `overlay`
2. both workflows default the omitted selection to `overlay`
3. the workflow, IaC, and bootstrap layers pass the selected target through to
   the existing runtime surfaces without inventing a third storage mode

This preserves Azure-hosted execution parity across embedding refresh and
indexing experiments while avoiding any redesign of the upstream overlay
implementation itself.

**Traces to:** RQ-EXP-008A, RQ-EXP-008B, RQ-EXP-012, RQ-EXP-020

### DSG-EXP-011 `Blob-backed artifact handoff`

Both hosted workflows use Azure Blob Storage as the durable handoff boundary
between VM-side execution and GitHub-side artifact publication.

The design distinguishes two artifact families:

1. **Reusable dataset artifacts** owned by the embedding-refresh workflow:
   embeddings and replay journal persisted for later reuse
2. **Experiment report artifacts** owned by the indexing-experiment workflow:
   rooted quality/report output and related operator-visible outputs for the
   selected profile run

GitHub Actions artifacts are derived from the Blob-stored outputs after the VM
run concludes. Blob Storage remains the durable source of truth for reusable
state; workflow artifacts remain the convenient retrieval surface for run
consumers.

**Traces to:** RQ-EXP-015, RQ-EXP-016, RQ-EXP-014A

### DSG-EXP-012 `Outcome and cleanup contract`

Each hosted workflow concludes by surfacing:

1. pass/fail outcome
2. resource-group name
3. storage-account name

and by always attempting VM deallocation regardless of success or failure.

Resource-group deletion is intentionally excluded from the workflow contract so
operators can inspect Azure state manually after failures or surprising runs.

This design combines guest-side success-path shutdown with workflow-side cleanup
fallback instead of trusting only one cleanup mechanism.

**Traces to:** RQ-EXP-017, RQ-EXP-018, RQ-EXP-019

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-EXP-001 | RQ-EXP-001, RQ-EXP-020, RQ-EXP-021 |
| DSG-EXP-002 | RQ-EXP-002, RQ-EXP-008 |
| DSG-EXP-003 | RQ-EXP-005, RQ-EXP-006 |
| DSG-EXP-003A | RQ-EXP-008A, RQ-EXP-008B, RQ-EXP-020 |
| DSG-EXP-004 | RQ-EXP-007, RQ-EXP-012, RQ-EXP-013 |
| DSG-EXP-005 | RQ-EXP-007, RQ-EXP-020 |
| DSG-EXP-006 | RQ-EXP-008, RQ-EXP-009, RQ-EXP-014 |
| DSG-EXP-006A | RQ-EXP-014, RQ-EXP-014A, RQ-EXP-015 |
| DSG-EXP-007 | RQ-EXP-010 |
| DSG-EXP-008 | RQ-EXP-003, RQ-EXP-011 |
| DSG-EXP-009 | RQ-EXP-003, RQ-EXP-004, RQ-EXP-021 |
| DSG-EXP-010 | RQ-EXP-012, RQ-EXP-017, RQ-EXP-021 |
| DSG-EXP-010A | RQ-EXP-008A, RQ-EXP-008B, RQ-EXP-012, RQ-EXP-020 |
| DSG-EXP-011 | RQ-EXP-015, RQ-EXP-016, RQ-EXP-014A |
| DSG-EXP-012 | RQ-EXP-017, RQ-EXP-018, RQ-EXP-019 |
