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
existing filesystem block-store route, reserves compatibility with the future
overlay block-store route, and leaves overlay-specific integration points
clearly marked as TODOs rather than pretending the separate pull request has
already landed.

**Traces to:** RQ-EXP-008A, RQ-EXP-020, DSG-EXP-003A, DSG-EXP-010A

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

### VAL-EXP-011

Inspect workflow cleanup behavior on both success and failure paths.

**Pass condition:** each hosted workflow always attempts VM deallocation, and
neither workflow deletes the Azure resource group automatically.

**Traces to:** RQ-EXP-018, RQ-EXP-019, DSG-EXP-012

### VAL-EXP-012

Inspect the hosted workflow family against repository semantic boundaries.

**Pass condition:** the hosted workflows orchestrate existing indexer and
deployment surfaces without redefining indexer semantics, MCP semantics,
production-serving semantics, or the separate block-storage-abstraction effort
beyond clearly marked TODO seams for the future overlay block-store path.

**Traces to:** RQ-EXP-001, RQ-EXP-020, RQ-EXP-021, DSG-EXP-001, DSG-EXP-012
