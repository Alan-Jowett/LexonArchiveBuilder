<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder Image Publishing Design

## Status

Approved specification package with implemented block-gateway image-publication
increment for the repository-owned Docker image publication workflow in
`docs/specs/lexonarchivebuilder-image-publishing/requirements.md`.

## Scope

This document specifies the LexonArchiveBuilder-owned design for realizing a
hosted workflow that publishes self-contained Linux Docker images for the
repository's current container entrypoints, including the additive
`lexonarchivebuilder-block-gateway` runtime image.

This document is layered on top of:

- `docs/specs/lexonarchivebuilder-image-publishing/requirements.md`
- `docs/specs/lexonarchivebuilder-deployment/requirements.md`
- `README.md`
- `docker-compose.yml`
- `crates/lexonarchivebuilder-indexer/Dockerfile`
- `crates/lexonarchivebuilder-indexer/Dockerfile.scale-test`
- `crates/lexonarchivebuilder-mcp/Dockerfile`
- `docs/specs/lexonarchivebuilder-block-gateway/requirements.md`
- `docs/specs/lexonarchivebuilder-block-gateway/design.md`

This document does not define release notes, GitHub Releases, non-GHCR
distribution, multi-architecture publication, or deployment orchestration.

## Impact Map

### Directly affected artifacts

- `docs/specs/lexonarchivebuilder-image-publishing/requirements.md`
- `docs/specs/lexonarchivebuilder-image-publishing/design.md`
- `docs/specs/lexonarchivebuilder-image-publishing/validation.md`

### Indirectly affected artifacts

- `.github/workflows/` image-publication workflow definitions
- `crates/lexonarchivebuilder-block-gateway/Dockerfile`
- `crates/lexonarchivebuilder-indexer/Dockerfile`
- `crates/lexonarchivebuilder-indexer/Dockerfile.scale-test`
- `crates/lexonarchivebuilder-mcp/Dockerfile`
- README operator guidance for pulling and running published images
- downstream deployment inputs that consume GHCR image tags

### Unaffected artifacts

- `docs/specs/lexonarchivebuilder-indexer/*`
- `docs/specs/lexonarchivebuilder-mcp/*`
- `docs/specs/lexonarchivebuilder-scale-test/*`
- indexer request semantics
- MCP search semantics
- storage-adapter and embedding-adapter semantics

## Design Goals

The `lexonarchivebuilder-image-publishing` design is intended to be:

- minimal
- reproducible
- Linux-Docker-oriented
- explicit about runtime asset completeness
- aligned with existing repository Dockerfile boundaries
- reusable by downstream deployment and operator workflows
- non-invasive to existing semantic contracts

## Boundary Design

### DSG-IMG-001 `Publishing boundary ownership`

`lexonarchivebuilder-image-publishing` owns hosted build-and-publish workflow
behavior, published image naming, and repository-owned runtime-asset inclusion
for the published images.

It does not own indexer semantics, MCP semantics, storage selection semantics,
embedding selection semantics, or deployment orchestration semantics.

**Traces to:** RQ-IMG-001, RQ-IMG-009, RQ-IMG-010

### DSG-IMG-002 `Workflow realization`

The repository realizes this boundary through one GitHub Actions workflow file
dedicated to image publication.

The workflow is separate from verification-only CI so release-like artifact
publication does not blur the existing CI boundary that intentionally excludes
publish automation.

**Traces to:** RQ-IMG-003

### DSG-IMG-003 `Approved image matrix`

The workflow builds and publishes one image per currently approved repository
entrypoint:

1. `lexonarchivebuilder-indexer`
2. `lexonarchivebuilder-scale-test`
3. `lexonarchivebuilder-mcp`
4. `lexonarchivebuilder-block-gateway`

Each matrix entry maps to the existing repository Dockerfile that owns that
entrypoint.

**Traces to:** RQ-IMG-002, RQ-IMG-003

### DSG-IMG-004 `GHCR publication target`

The workflow publishes images to GHCR under repository-owned names so the same
publication surface can satisfy both operator pull scenarios and deployment
inputs that already name GHCR image tags.

The first increment uses one registry target rather than introducing parallel
distribution policy across multiple registries.

**Traces to:** RQ-IMG-006

### DSG-IMG-005 `Reusable tag family`

The workflow emits reproducible image identifiers for each published build.

The minimum design baseline is:

- an immutable source-derived identifier such as a commit SHA tag
- one workflow-owned stable alias appropriate for the selected publication
  source, such as a branch-aligned tag for the mainline build

This design satisfies reproducible downstream selection without requiring a full
release-tag governance model in the first increment.

**Traces to:** RQ-IMG-008

### DSG-IMG-006 `Self-contained runtime asset rule`

Each published image includes the repository-owned files required by its
documented Linux Docker entrypoint.

The design distinguishes:

- binaries or compiled entrypoints
- repository-owned scripts
- repository-owned example or default input files that the documented container
  contract expects inside the image

The design does not require bundling unrelated repository material solely
because it exists in the source tree.

**Traces to:** RQ-IMG-004, RQ-IMG-007

### DSG-IMG-006A `Block-gateway runtime packaging`

The published `lexonarchivebuilder-block-gateway` image packages the
repository-owned runtime surface needed to launch the gateway, including the
gateway binary and its documented container entrypoint, while keeping the SAS
URL plus certificate and private-key material external to the image.

This preserves the already approved gateway startup contract, where those
values are provided at runtime through flags, environment, or mounted files,
rather than redefining packaging as a secret-distribution mechanism.

**Traces to:** RQ-IMG-004A, RQ-IMG-009, RQ-BGW-004, RQ-BGW-009

### DSG-IMG-007 `Scale-test completeness`

The published `lexonarchivebuilder-scale-test` image embeds its wrapper-owned
runtime files so the image can execute the approved Linux Docker scale-test flow
without relying on a bind-mounted source checkout for those wrapper-owned files.

This design preserves the wrapper's existing semantic stages while changing only
how the container obtains its repository-owned runtime assets.

**Traces to:** RQ-IMG-005, RQ-IMG-009

### DSG-IMG-008 `Entry-point-preserving image updates`

The design preserves the current image-to-entrypoint mapping rather than
collapsing the indexer, scale-test, and MCP surfaces into one polymorphic
runtime image.

This keeps packaging aligned with the repository's existing runtime boundaries.

**Traces to:** RQ-IMG-002, RQ-IMG-009

### DSG-IMG-009 `Linux-only first publication contract`

The first publication design targets Linux Docker hosts and does not add a
multi-architecture manifest requirement.

Future expansion to additional architectures can layer on top of the same image
matrix boundary if later requirements approve it.

**Traces to:** RQ-IMG-007

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-IMG-001 | RQ-IMG-001, RQ-IMG-009, RQ-IMG-010 |
| DSG-IMG-002 | RQ-IMG-003 |
| DSG-IMG-003, DSG-IMG-008 | RQ-IMG-002 |
| DSG-IMG-004 | RQ-IMG-006 |
| DSG-IMG-005 | RQ-IMG-008 |
| DSG-IMG-006 | RQ-IMG-004, RQ-IMG-007 |
| DSG-IMG-006A | RQ-IMG-004A |
| DSG-IMG-007 | RQ-IMG-005, RQ-IMG-009 |
| DSG-IMG-009 | RQ-IMG-007 |
