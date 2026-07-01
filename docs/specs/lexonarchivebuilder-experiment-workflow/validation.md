<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder Experiment Workflow Validation

## Status

Phase 2 validation patch for the approved hosted staged experiment-workflow
boundary in
`docs/specs/lexonarchivebuilder-experiment-workflow/requirements.md` and
`docs/specs/lexonarchivebuilder-experiment-workflow/design.md`.

## Validation Scope

These validation entries define the expected conformance surface for the
LexonArchiveBuilder-owned `lexonarchivebuilder-experiment-workflow` boundary.

This package validates hosted workflow separation, checked-in reusable-dataset
manifest ownership, staged reuse of embeddings and replay journal state,
published-image selection, Azure artifact retrieval, and cleanup behavior. It
does not redefine validation already owned by `lexonarchivebuilder-indexer`,
`lexonarchivebuilder-mcp`, the image-publication boundary, or the production
deployment boundary.

## Validation Entries

### VAL-EXP-001

Inspect the repository workflow surface for
`lexonarchivebuilder-experiment-workflow`.

**Pass condition:** the repository defines a separate hosted workflow boundary
for experiment automation rather than relying solely on a local-machine script
surface.

**Traces to:** RQ-EXP-001, RQ-EXP-003, DSG-EXP-001, DSG-EXP-009

### VAL-EXP-002

Inspect the hosted workflow family definition.

**Pass condition:** the repository defines two separate workflow entrypoints:
one for embedding refresh and one for indexing experiments over reusable
embeddings.

**Traces to:** RQ-EXP-002, RQ-EXP-008, DSG-EXP-002

### VAL-EXP-003

Inspect the checked-in reusable-dataset manifest surface.

**Pass condition:** a checked-in manifest exists for the hosted workflow family,
and its contract includes full rsync URLs plus the Azure Blob container name for
the reusable embedding dataset.

**Traces to:** RQ-EXP-005, RQ-EXP-006, DSG-EXP-003

### VAL-EXP-003A

Inspect the experiment-workflow specification and downstream implementation seam
for block-store targeting.

**Pass condition:** the experiment path preserves compatibility with the
existing filesystem block-store route, exposes the approved overlay
block-store route as an executable hosted mode in both workflows, and defaults
an omitted caller selection to `overlay` without removing explicit filesystem
selection.

**Traces to:** RQ-EXP-008A, RQ-EXP-008B, RQ-EXP-020, DSG-EXP-003A, DSG-EXP-010A

### VAL-EXP-004

Inspect the embedding-refresh workflow contract and its VM-side staged
invocation.

**Pass condition:** the embedding workflow runs only the embedding-oriented
split-stage path, persists embeddings plus replay journal state to Azure Blob
Storage, and does not collapse back into a full-pipeline rerun.

**Traces to:** RQ-EXP-007, RQ-EXP-012, RQ-EXP-013, DSG-EXP-004, DSG-EXP-005

### VAL-EXP-005

Inspect the embedding-refresh workflow against a reusable dataset that already
has persisted state.

**Pass condition:** a refresh extends the dataset incrementally for newly needed
embedding work rather than recomputing all prior embeddings in the normal case.

**Traces to:** RQ-EXP-007, DSG-EXP-005

### VAL-EXP-006

Inspect the indexing-experiment workflow contract and its VM-side staged
invocation.

**Pass condition:** the indexing workflow consumes the existing manifest-defined
reusable embedding dataset and replay journal, runs one caller-selected
published-profile experiment, and does not require a same-run embedding phase in
the normal path.

**Traces to:** RQ-EXP-008, RQ-EXP-009, RQ-EXP-014, DSG-EXP-002, DSG-EXP-006

### VAL-EXP-006A

Inspect the indexing-experiment workflow against the existing `test.ps1`
evaluation shape.

**Pass condition:** after the hosted indexing run completes clustering and block
assembly, it also performs the rooted quality/report step and publishes that
quality/report artifact family as the experiment result rather than stopping at
root construction alone.

**Traces to:** RQ-EXP-014A, RQ-EXP-015, DSG-EXP-006A, DSG-EXP-011

### VAL-EXP-007

Inspect the hosted workflow input contract for image selection and break-glass
access.

**Pass condition:** callers can rely on the lab pipeline's published `main` tag
by default, override that default with a specific published tag, and provide an
SSH public key without making SSH part of the normal success-path workflow.

**Traces to:** RQ-EXP-010, RQ-EXP-011, DSG-EXP-007, DSG-EXP-008

### VAL-EXP-008

Inspect the Azure authentication and orchestration contract for the hosted
workflow family.

**Pass condition:** Azure access is obtained through a repository-owned GitHub
Actions federation path rather than through a developer-local Azure session or a
new always-on control plane.

**Traces to:** RQ-EXP-003, RQ-EXP-004, RQ-EXP-021, DSG-EXP-009

### VAL-EXP-009

Inspect the minimal Azure execution surface for each hosted workflow.

**Pass condition:** each workflow uses only the VM-plus-Blob execution shape
needed for one run, surfaces the resource-group and storage-account identifiers,
and does not expand into a production-serving deployment contract.

**Traces to:** RQ-EXP-012, RQ-EXP-017, RQ-EXP-021, DSG-EXP-010, DSG-EXP-012

### VAL-EXP-010

Inspect the artifact handoff path for both hosted workflows.

**Pass condition:** operator-relevant outputs are written to Azure Blob Storage,
retrieved after VM-side execution, and republished as GitHub workflow artifacts,
while Blob Storage remains the durable source of truth for reusable embedding
state.

**Traces to:** RQ-EXP-015, RQ-EXP-016, DSG-EXP-011

### VAL-EXP-010A

Inspect the embedding-refresh failure path before the inner workload-owned
status/upload contract becomes active.

**Pass condition:** when bootstrap fails before the inner embedding script can
publish its normal `status.json`, the VM/bootstrap layer still publishes a
machine-readable status artifact plus a Blob-backed diagnostic bundle that
distinguishes bootstrap failure from later workload failure.

**Traces to:** RQ-EXP-015A, RQ-EXP-017A, DSG-EXP-011A

### VAL-EXP-010B

Inspect the GitHub-side embedding-refresh retrieval path for early failures.

**Pass condition:** the workflow can retrieve bootstrap-owned failure artifacts
from Blob Storage and republish them as GitHub workflow artifacts without
requiring manual Azure guest inspection as the primary diagnostic path.

**Traces to:** RQ-EXP-016A, RQ-EXP-017A, DSG-EXP-011B

### VAL-EXP-011

Inspect workflow cleanup behavior on both success and failure paths.

**Pass condition:** each hosted workflow always attempts VM deallocation, and
neither workflow deletes the Azure resource group automatically.

**Traces to:** RQ-EXP-018, RQ-EXP-019, DSG-EXP-012

### VAL-EXP-011A

Inspect embedding-refresh failure cleanup behavior with and without the
debug-retention mode enabled.

**Pass condition:** the default failure path attempts diagnostic publication
before deallocation and still deallocates automatically, while the explicit
debug-retention mode preserves or delays VM deallocation for failed runs without
introducing automatic resource-group deletion.

**Traces to:** RQ-EXP-015A, RQ-EXP-016A, RQ-EXP-018A, RQ-EXP-019, DSG-EXP-012A, DSG-EXP-012B

### VAL-EXP-012

Inspect the hosted workflow family against repository semantic boundaries.

**Pass condition:** the hosted workflows orchestrate existing indexer and
deployment surfaces without redefining indexer semantics, MCP semantics,
production-serving semantics, or the separate block-storage-abstraction effort
beyond consuming the approved hosted overlay-selection contract.

**Traces to:** RQ-EXP-001, RQ-EXP-020, RQ-EXP-021, DSG-EXP-001, DSG-EXP-012

### VAL-EXP-013

Inspect the repository validation surface for the hosted workflow family.

**Pass condition:** the repository defines a preflight validation layer for
hosted workflow regressions that is separate from the live Azure confirmation
run.

**Traces to:** RQ-EXP-018B, RQ-EXP-018F, DSG-EXP-013, DSG-EXP-013D

### VAL-EXP-013A

Exercise the hosted workflow preflight layer against rendered workflow-owned
artifacts.

**Pass condition:** the validation surface checks generated env-file content,
generated bootstrap/workload handoff content, and repository-owned invocation
inputs rather than relying solely on static source inspection.

**Traces to:** RQ-EXP-018C, RQ-EXP-018E, DSG-EXP-013A

### VAL-EXP-013B

Exercise the hosted workflow preflight layer without deploying Azure
infrastructure.

**Pass condition:** the covered regression class can be validated through
repository-local or normal-CI execution paths without requiring a successful
live Azure deployment for those checks.

**Traces to:** RQ-EXP-018D, DSG-EXP-013B, DSG-EXP-013D

### VAL-EXP-013C

Seed the hosted workflow preflight layer with representative repository-owned
failure fixtures from the currently observed regression class.

**Pass condition:** malformed env-file separation, bad sourced-value quoting,
broken bootstrap-to-workload wiring, and malformed repository-owned status or
artifact handoff inputs are rejected before live Azure confirmation is
required.

**Traces to:** RQ-EXP-018C, RQ-EXP-018E, DSG-EXP-013A, DSG-EXP-013C

### VAL-EXP-013D

Compare the hosted workflow validation strategy against the live Azure workflow
role.

**Pass condition:** live Azure runs remain the integration-confirmation layer,
but repository-owned preventable regressions are no longer expected to be
discovered there first.

**Traces to:** RQ-EXP-018F, DSG-EXP-013, DSG-EXP-013D
