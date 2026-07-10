<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder Image Publishing Requirements

## Document Status

- **Phase:** Phase 8 - Create Deliverable
- **Status:** Approved specification package with implemented block-gateway image-publication increment
- **Scope:** Repository-owned container-image publishing workflow for Linux Docker consumers of LexonArchiveBuilder runtime and test images, including the additive `lexonarchivebuilder-block-gateway` runtime image

## USER-REQUEST

- **UR-IMG-1 [KNOWN]:** Add a workflow to publish Docker images.
- **UR-IMG-2 [KNOWN]:** The published images must contain the files required to run the tests on a Linux machine with Docker.
- **UR-IMG-3 [KNOWN]:** Publish all of the repository's current Docker-image entrypoints, not only the scale-test image.
- **UR-IMG-4 [KNOWN]:** The repository already defines Docker build entrypoints for `lexonarchivebuilder-indexer`, `lexonarchivebuilder-scale-test`, and `lexonarchivebuilder-mcp`.
- **UR-IMG-5 [KNOWN]:** Existing deployment requirements already refer to GHCR image tags as deployment inputs.
- **UR-IMG-6 [INFERRED]:** This change is about repository artifact publication and reuse, not about changing indexer semantics, MCP search semantics, storage-adapter semantics, or embedding-adapter semantics.
- **UR-IMG-7 [INFERRED]:** Linux Docker consumers should be able to pull the published images and use the documented runtime/test entrypoints without first cloning the repository to obtain missing runtime files.
- **UR-IMG-8 [ASSUMPTION]:** The first publishing increment should target GitHub Container Registry because the repository already names GHCR image tags in its deployment boundary and the source repository is hosted on GitHub.
- **UR-IMG-9 [ASSUMPTION]:** The first publishing increment should optimize for Linux-hosted Docker use and does not need to introduce a multi-architecture publication contract unless a later request makes that necessary.
- **UR-IMG-10 [UNKNOWN]:** The long-term release-tag policy for published images is not yet specified beyond the need for a reusable publication workflow.
- **UR-IMG-11 [KNOWN]:** Add a Docker container for `lexonarchivebuilder-block-gateway` and add CI publication for it.
- **UR-IMG-12 [KNOWN]:** This increment should extend the existing image-publication boundary rather than redesigning the broader publication workflow.
- **UR-IMG-13 [KNOWN]:** The published `lexonarchivebuilder-block-gateway` image should keep the SAS URL plus certificate and private-key material external at runtime rather than baking those secrets into the image.
- **UR-IMG-14 [INFERRED]:** Because the gateway runtime already depends on operator-supplied network address, SAS URL, and certificate files, the image contract should package the runtime binary and its repository-owned launch surface while preserving runtime secret injection through flags, environment, or mounted files.

## Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-IMG-001 | Add | Introduce a separate repository-owned requirements boundary for Docker image publication | UR-IMG-1, UR-IMG-6 |
| CM-IMG-002 | Add | Define one hosted workflow that publishes all current repository image entrypoints | UR-IMG-1, UR-IMG-3, UR-IMG-4 |
| CM-IMG-003 | Add | Require published images to remain self-contained for their documented Linux Docker entrypoints, including non-binary runtime files when needed | UR-IMG-2, UR-IMG-7 |
| CM-IMG-004 | Add | Align the first publication target to GHCR so downstream deployment and operator workflows can consume repository-owned image tags consistently | UR-IMG-5, UR-IMG-8 |
| CM-IMG-005 | Add | Preserve architecture and environment invariants by keeping image publication separate from indexing, MCP, storage, and embedding semantics | UR-IMG-6 |
| CM-IMG-006 | Add | Constrain the first increment to Linux Docker consumption without requiring a new multi-architecture contract | UR-IMG-2, UR-IMG-9 |
| CM-IMG-007 | Add | Record the unresolved long-term release-tag policy so the first workflow does not silently over-commit to a release process that was not requested | UR-IMG-10 |
| CM-IMG-008 | Revise | Expand the approved published image set so the existing workflow also publishes `lexonarchivebuilder-block-gateway` without redesigning the broader image-publication boundary | UR-IMG-11, UR-IMG-12 |
| CM-IMG-009 | Add | Require the published block-gateway image to package its repository-owned runtime surface while keeping SAS and certificate material external at runtime | UR-IMG-13, UR-IMG-14 |

## Before / After

### BA-IMG-001

- **Before [KNOWN]:** The repository has Dockerfiles for its current runtime entrypoints, but no approved repository-owned workflow that publishes those images for reuse.
- **After [KNOWN]:** The repository has an explicit requirements baseline for publishing LexonArchiveBuilder container images.

### BA-IMG-002

- **Before [KNOWN]:** Linux Docker consumers can build images locally from the repository, but they are not guaranteed a prebuilt published image set.
- **After [KNOWN]:** The repository defines a hosted publication workflow for the current image entrypoints.

### BA-IMG-003

- **Before [KNOWN]:** At least one current image boundary, `lexonarchivebuilder-scale-test`, depends on repository-owned runtime files outside the compiled binary and therefore risks requiring a source checkout at runtime.
- **After [KNOWN]:** Published images are required to contain the repository-owned runtime files needed for their documented Linux Docker entrypoints.

### BA-IMG-004

- **Before [KNOWN]:** Existing deployment requirements refer to GHCR image tags, but the repository does not yet define the publication workflow that would supply those tags.
- **After [KNOWN]:** The repository defines image-publication requirements aligned with GHCR-backed downstream consumption.

### BA-IMG-005

- **Before [KNOWN]:** The approved image-publication boundary covered `lexonarchivebuilder-indexer`, `lexonarchivebuilder-scale-test`, and `lexonarchivebuilder-mcp`, but not the newly added `lexonarchivebuilder-block-gateway` runtime.
- **After [KNOWN]:** The requirements expand the approved image set so the existing publication workflow also covers `lexonarchivebuilder-block-gateway`.

### BA-IMG-006

- **Before [KNOWN]:** The image-publication requirements defined self-contained runtime assets generically, but they did not spell out how the new block-gateway image should separate packaged runtime files from runtime-injected secrets and certificates.
- **After [KNOWN]:** The requirements now state that the block-gateway image packages its repository-owned runtime surface while leaving the SAS URL plus certificate/private-key material external at runtime.

## Requirements

### Functional Requirements

#### RQ-IMG-001 - Image-publication boundary

LexonArchiveBuilder SHALL provide a separate repository-owned automation boundary for publishing container images.

- **Boundary [KNOWN]:** This boundary owns hosted image-build and image-publication behavior.
- **Non-goal [KNOWN]:** This boundary does not redefine indexing, MCP, content-model, storage, or embedding semantics.
- **Traceability:** UR-IMG-1, UR-IMG-6

#### RQ-IMG-002 - Published image set

The first publishing increment SHALL publish all current repository-owned image entrypoints:

1. `lexonarchivebuilder-indexer`
2. `lexonarchivebuilder-scale-test`
3. `lexonarchivebuilder-mcp`
4. `lexonarchivebuilder-block-gateway`

- **Boundary [KNOWN]:** This requirement applies to the current repository image surfaces and does not by itself require future image families.
- **Traceability:** UR-IMG-3, UR-IMG-4, UR-IMG-11, UR-IMG-12

#### RQ-IMG-003 - Hosted publication workflow

The repository SHALL define one hosted workflow that builds and publishes the approved image set.

- **Constraint [INFERRED]:** The workflow should remain repository-owned automation rather than a manual-only operator recipe.
- **Traceability:** UR-IMG-1, UR-IMG-3, UR-IMG-6

#### RQ-IMG-004 - Self-contained Linux Docker runtime assets

Each published image SHALL contain the repository-owned files required by its documented Linux Docker runtime or test entrypoint.

- **Required property [KNOWN]:** Callers should not need a repository checkout at runtime merely to supply files that the image's own documented entrypoint expects to exist inside the image.
- **Boundary [INFERRED]:** Images only need to embed repository-owned runtime assets that are required by their documented entrypoints; they do not need to embed unrelated repository content.
- **Traceability:** UR-IMG-2, UR-IMG-7, UR-IMG-14

#### RQ-IMG-004A - Block-gateway runtime secret externalization

The published `lexonarchivebuilder-block-gateway` image SHALL package the gateway's repository-owned runtime surface without embedding the SAS URL, certificate, or private-key material required for a production invocation.

- **Required property [KNOWN]:** The image remains usable only when operators provide those values through runtime configuration or mounted secret files rather than through image-baked credentials.
- **Boundary [INFERRED]:** This requirement constrains packaging and secret placement only; it does not redefine the gateway's approved startup-time configuration contract.
- **Traceability:** UR-IMG-13, UR-IMG-14

#### RQ-IMG-005 - Scale-test runtime completeness

The published `lexonarchivebuilder-scale-test` image SHALL remain runnable for the approved Linux Docker scale-test flow without requiring bind-mounted repository scripts or sample source-list files solely to satisfy the image's own wrapper-owned stages.

- **Boundary [KNOWN]:** This requirement covers wrapper-owned assets such as the scale-test entry script and any repository-owned default files that the documented scale-test container contract expects.
- **Traceability:** UR-IMG-2, UR-IMG-4, UR-IMG-7

#### RQ-IMG-006 - Registry alignment

The first publishing increment SHALL publish the approved image set to GitHub Container Registry.

- **Rationale [KNOWN]:** Existing deployment requirements already name GHCR image tags as deployment inputs.
- **Boundary [ASSUMPTION]:** This increment does not define a second registry target unless later requirements add one.
- **Traceability:** UR-IMG-5, UR-IMG-8

#### RQ-IMG-007 - Linux-hosted consumption baseline

The published image contract SHALL target Linux-hosted Docker consumption.

- **Constraint [ASSUMPTION]:** The first increment does not require a multi-architecture manifest contract.
- **Traceability:** UR-IMG-2, UR-IMG-9

#### RQ-IMG-008 - Reusable image identification

The hosted workflow SHALL publish image tags or identifiers that allow downstream operators or automation to select published image builds reproducibly.

- **Boundary [UNKNOWN]:** The long-term stable-tag policy beyond the first increment remains open.
- **Traceability:** UR-IMG-10

### Boundary and Invariant Requirements

#### RQ-IMG-009 - Semantic non-interference

The image-publication workflow SHALL package and publish existing repository entrypoints without redefining:

1. indexer request or execution semantics
2. MCP request/response semantics
3. storage-adapter behavior
4. embedding-adapter behavior
5. content-type abstraction boundaries

- **Traceability:** UR-IMG-6

#### RQ-IMG-010 - Local-versus-production boundary preservation

The image-publication workflow SHALL remain a packaging surface and SHALL NOT collapse the repository's local/testing and production semantic boundaries into one new runtime contract.

- **Rationale [INFERRED]:** The repository keeps local/testing workflows and production deployment concerns as separate semantic boundaries even when both consume Docker images.
- **Traceability:** UR-IMG-6, UR-IMG-8

## Out of Scope

- changing indexer runtime behavior
- changing MCP runtime behavior
- changing storage-provider or embedding-provider selection semantics
- defining a release-management process beyond the image-publication workflow itself
- defining non-GitHub registry publication in this increment
- requiring a multi-architecture image contract in this increment
- redefining deployment IaC or VM orchestration semantics

## Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | Image publication packages existing entrypoints but does not merge their runtime roles |
| Local/testing versus production behavior stays behind stable adapters | Preserved | The workflow publishes reusable artifacts without redefining environment-specific storage or embedding contracts |
| The architecture remains extensible to future content types | Preserved | The packaging boundary remains image-entrypoint-oriented rather than hard-coded to only today's content types |
| The repository remains subordinate to LexonGraph-owned indexing and search contracts | Preserved | Publishing prebuilt images does not redefine delegated upstream APIs or behaviors |
| Runtime secrets stay outside published image artifacts when the runtime contract already expects operator-supplied credentials | Preserved with clarified packaging rule | The block-gateway image may package its runtime binary and launch surface, but the SAS URL and certificate material remain external at runtime |

## Coverage Notes

- **Covered sources [KNOWN]:**
  - `README.md:136-220`
  - `docker-compose.yml:1-67`
  - `crates/lexonarchivebuilder-indexer/Dockerfile:1-16`
  - `crates/lexonarchivebuilder-indexer/Dockerfile.scale-test:1-16`
  - `crates/lexonarchivebuilder-mcp/Dockerfile:1-16`
  - `.github/workflows/publish-images.yml:1-75`
  - `docs/specs/lexonarchivebuilder-block-gateway/requirements.md:8-183`
  - `crates/lexonarchivebuilder-block-gateway/Cargo.toml:1-32`
  - `crates/lexonarchivebuilder-block-gateway/src/main.rs:1-57`
  - `docs/specs/lexonarchivebuilder-deployment/requirements.md:11-29`
  - user request in this session: "add a workflow to publish docker images that contain the files required to run the tests on a Linux machine with docker"
  - user clarification in this session: "publish all of them"
  - user request in this session: "create a docker container and add a ci step to publish it"
  - user clarification in this session selecting `Yes — add only \`lexonarchivebuilder-block-gateway\` to the existing publication boundary (Recommended)`
  - user clarification in this session selecting `Yes — keep credentials and certificates external at runtime (Recommended)`
- **Excluded from this requirements artifact [KNOWN]:**
  - implementation details of the workflow file
  - Dockerfile edits before the implementation phase
  - release-tag policy details beyond the reusable-publication requirement
