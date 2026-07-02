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
- repository-owned CI and local smoke-validation surfaces that can exercise hosted workflow seams without Azure deployment

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

1. one predictable long-term storage resource group that owns the durable
   workflow storage account and Blob container surface
2. one per-run batch resource group that owns the VM-hosted execution
   environment and other reclaimable run-local infrastructure
3. one Blob container reachable through an approved SAS-backed access path
4. surfaced deployment identifiers including long-term resource-group, batch
   resource-group, and storage-account names for manual inspection

The first design baseline leaves the exact Bicep module factoring open but
constrains the hosted workflow to remain a minimal experiment-orchestration
surface rather than a production-serving deployment expansion.

**Traces to:** RQ-EXP-012, RQ-EXP-017, RQ-EXP-021

### DSG-EXP-010B `Stable long-term naming with related batch suffixing`

The Azure naming contract distinguishes durable dataset identity from per-run
execution identity.

Within that contract:

1. the long-term storage resource group and workflow storage account are derived
   from a predictable repository-owned naming rule
2. repeated runs against the same reusable dataset contract resolve to that same
   long-term storage scope
3. each batch resource group reuses the same naming family but appends a
   uniqueness suffix so operators can relate it to the long-term scope while
   avoiding run-to-run collisions

The design baseline intentionally leaves the exact stable key and suffix recipe
open so long as the resulting names preserve operator recognizability and
support deterministic durable-state targeting plus distinct reclaimable runs.

**Traces to:** RQ-EXP-012A, RQ-EXP-012B

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

### DSG-EXP-011A `Bootstrap-owned failure diagnostics`

The embedding-refresh workflow adds a bootstrap-owned diagnostic publication
path that exists before control reaches the inner
`lexonarchivebuilder-embedding-refresh.sh` cleanup trap.

That design separates two failure phases:

1. **bootstrap failure** before the inner workload-owned status/upload path is
   active
2. **workload failure** after the inner workload-owned status/upload path is
   active

For the bootstrap-failure phase, the VM bootstrap/wrapper layer owns:

1. writing a machine-readable bootstrap status artifact
2. collecting best-effort early diagnostics from cloud-init, wrapper execution,
   and available service/container failure evidence
3. uploading those artifacts to the same Blob-backed run prefix used by the
   workflow retrieval path

This preserves the existing inner-script artifact contract while closing the
observability gap that appears when failure occurs before that contract is live.

**Traces to:** RQ-EXP-015A, RQ-EXP-017A

### DSG-EXP-011B `Failure-phase-aware workflow retrieval`

The GitHub-side embedding-refresh workflow treats bootstrap diagnostics as a
first-class retrieval surface rather than assuming `status.json` only exists in
the workload-owned shape.

The design therefore requires the workflow wait/retrieval path to:

1. recognize the bootstrap-owned machine-readable status artifact
2. classify failure as bootstrap-phase or workload-phase
3. retrieve the corresponding diagnostic files from Blob Storage
4. republish that diagnostic bundle as workflow artifacts

This keeps the operator-facing workflow surface usable without requiring
immediate break-glass Azure inspection for early failures.

**Traces to:** RQ-EXP-016A, RQ-EXP-017A

### DSG-EXP-012 `Outcome and cleanup contract`

Each hosted workflow concludes by surfacing:

1. pass/fail outcome
2. long-term storage resource-group name
3. batch resource-group name
4. storage-account name

and by reclaiming the batch execution environment in the normal path regardless
of success or failure.

The normal reclaim action is deletion of the batch resource group, which
reclaims the VM together with the other per-run infrastructure while preserving
the long-term storage resource group that owns the durable Blob-backed state.

This design combines guest-side success-path shutdown with workflow-side cleanup
fallback instead of trusting only one cleanup mechanism.

**Traces to:** RQ-EXP-017, RQ-EXP-018, RQ-EXP-019

### DSG-EXP-012A `Failure-path diagnostics before cleanup`

Failure-path cleanup remains subordinate to diagnostic publication.

The design requires the embedding-refresh bootstrap/wrapper path to attempt
diagnostic capture and Blob publication before the workflow's default
batch-cleanup behavior removes the easiest source of failure evidence.

This is a best-effort ordering guarantee, not a promise that every possible
guest failure can always produce every diagnostic file before batch-resource-group
reclamation occurs.

**Traces to:** RQ-EXP-015A, RQ-EXP-016A, RQ-EXP-018

### DSG-EXP-012B `Opt-in debug retention`

The hosted workflow family preserves automatic batch-resource-group deletion as
the normal success and failure contract, but exposes an explicit
debug-retention mode for manual investigation of failed runs.

Within that mode:

1. retention is caller-selected rather than automatic
2. retention affects batch-resource-group deletion timing only for failures
3. the long-term storage resource group remains preserved in all cases

This keeps cost and cleanup expectations stable for normal runs while giving
operators an approved path for deeper investigation when automated diagnostics
are insufficient.

**Traces to:** RQ-EXP-018A, RQ-EXP-019

### DSG-EXP-013 `Preflight validation boundary`

The hosted experiment-workflow boundary adds a repository-owned preflight
validation layer for workflow-owned regressions that are preventable without a
live Azure run.

This layer is intentionally subordinate to, not a replacement for, the live
Azure integration layer. Its purpose is to catch repository-owned defects in
rendered workflow/bootstrap/workload seams before deployment, VM startup, and
Blob-backed execution consume most of the debugging cost.

**Traces to:** RQ-EXP-018B, RQ-EXP-018F

### DSG-EXP-013A `Rendered-artifact validation strategy`

The design targets the workflow family's generated handoff artifacts rather than
only the checked-in source text.

The preflight strategy therefore validates repository-owned artifacts such as:

1. rendered workload environment-file content
2. rendered bootstrap/workload script handoff content
3. workflow-side status and artifact path inputs
4. repository-owned step and process invocation seams

This design choice is required because the covered regressions are not limited
to source-file syntax errors; they include defects introduced by composition of
workflow values, bootstrap inputs, and shell-owned handoff files.

**Traces to:** RQ-EXP-018C, RQ-EXP-018E

### DSG-EXP-013B `Repository-local execution path`

The preflight layer executes through repository-owned local or normal-CI paths
that do not require Azure deployment for the covered regression class.

Allowed realization shapes include deterministic rendering, fixture-driven
shell execution, smoke tests, and other repository-local checks that exercise
the hosted workflow family's owned seams deeply enough to detect malformed
generated artifacts.

This keeps the validation contract aligned with the repository's existing
pattern of smoke-style shell validation while avoiding any requirement to
emulate the full Azure runtime locally.

**Traces to:** RQ-EXP-018D, RQ-EXP-018E

### DSG-EXP-013C `Preventable-failure regression scope`

The preflight layer explicitly covers the class of repository-owned failures
that can stop the hosted workflows before Azure integration behavior becomes the
dominant risk.

The minimum covered class includes:

1. malformed env-file concatenation or missing separators
2. incorrect shell quoting for sourced assignments
3. broken bootstrap-to-workload invocation wiring
4. malformed repository-owned status or artifact handoff inputs

This scope is intentionally narrower than all cloud failures and broader than
static linting alone.

**Traces to:** RQ-EXP-018C

### DSG-EXP-013D `Layered confidence model`

Hosted workflow confidence is established in two layers:

1. preflight validation for repository-owned, locally preventable workflow regressions
2. live Azure confirmation for full integration behavior involving cloud identity, deployed infrastructure, external images, and runtime services

The design explicitly rejects using live Azure runs as the primary discovery
mechanism for the first layer of defects.

**Traces to:** RQ-EXP-018B, RQ-EXP-018D, RQ-EXP-018F

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
| DSG-EXP-011A | RQ-EXP-015A, RQ-EXP-017A |
| DSG-EXP-011B | RQ-EXP-016A, RQ-EXP-017A |
| DSG-EXP-012 | RQ-EXP-017, RQ-EXP-018, RQ-EXP-019 |
| DSG-EXP-012A | RQ-EXP-015A, RQ-EXP-016A, RQ-EXP-018 |
| DSG-EXP-012B | RQ-EXP-018A, RQ-EXP-019 |
| DSG-EXP-013 | RQ-EXP-018B, RQ-EXP-018F |
| DSG-EXP-013A | RQ-EXP-018C, RQ-EXP-018E |
| DSG-EXP-013B | RQ-EXP-018D, RQ-EXP-018E |
| DSG-EXP-013C | RQ-EXP-018C |
| DSG-EXP-013D | RQ-EXP-018B, RQ-EXP-018D, RQ-EXP-018F |
