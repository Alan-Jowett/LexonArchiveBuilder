# LexonArchiveBuilder Image Publishing Validation

## Status

Phase 2 validation patch for the approved repository-owned Docker image
publication workflow in
`docs/specs/lexonarchivebuilder-image-publishing/requirements.md` and
`docs/specs/lexonarchivebuilder-image-publishing/design.md`.

## Validation Scope

These validation entries define the expected conformance surface for the
LexonArchiveBuilder image-publication boundary.

## Validation Entries

### VAL-IMG-001

Inspect the repository workflow definitions.

**Pass condition:** the repository defines a dedicated hosted workflow for image
publication rather than folding publish behavior into the verification-only CI
workflow.

**Traces to:** RQ-IMG-001, RQ-IMG-003, DSG-IMG-001, DSG-IMG-002

### VAL-IMG-002

Inspect the workflow definition and its build matrix.

**Pass condition:** it builds and publishes the approved image set for
`lexonarchivebuilder-indexer`, `lexonarchivebuilder-scale-test`, and
`lexonarchivebuilder-mcp`.

**Traces to:** RQ-IMG-002, RQ-IMG-003, DSG-IMG-003, DSG-IMG-008

### VAL-IMG-003

Inspect the workflow definition and publication metadata.

**Pass condition:** it publishes to GHCR and emits reproducible image
identifiers for downstream selection.

**Traces to:** RQ-IMG-006, RQ-IMG-008, DSG-IMG-004, DSG-IMG-005

### VAL-IMG-004

Inspect each runtime image definition and resulting image filesystem.

**Pass condition:** each image contains the repository-owned files required by
its documented Linux Docker entrypoint, and the image does not rely on a source
checkout merely to provide those in-image runtime assets.

**Traces to:** RQ-IMG-004, RQ-IMG-007, DSG-IMG-006

### VAL-IMG-005

Inspect the published `lexonarchivebuilder-scale-test` image definition and run
surface.

**Pass condition:** the image can execute the approved Linux Docker scale-test
entrypoint without a bind-mounted repository checkout for wrapper-owned runtime
files such as its entry script or documented default source-list asset.

**Traces to:** RQ-IMG-005, DSG-IMG-007

### VAL-IMG-006

Inspect the repository's semantic boundaries against the workflow and image
changes.

**Pass condition:** image publication remains a packaging concern and does not
redefine indexer semantics, MCP semantics, storage semantics, embedding
semantics, or local-versus-production semantic boundaries.

**Traces to:** RQ-IMG-009, RQ-IMG-010, DSG-IMG-001, DSG-IMG-008

### VAL-IMG-007

Inspect the workflow contract for platform scope.

**Pass condition:** the first increment targets Linux Docker consumption without
claiming a multi-architecture publication contract that the repository does not
yet validate.

**Traces to:** RQ-IMG-007, DSG-IMG-009
