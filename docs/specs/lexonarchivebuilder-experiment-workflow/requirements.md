<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder Experiment Workflow Requirements

## Document Status

- **Phase:** Phase 2 - Specification Changes
- **Status:** Approved requirements patch being propagated into design and validation
- **Scope:** `lexonarchivebuilder-experiment-workflow` hosted GitHub workflow boundary for Azure-backed staged experiment automation that separates embedding refresh from indexing experiments and publishes retrieved reports as workflow artifacts

## USER-REQUEST

- **UR-EXP-1 [KNOWN]:** Build hosted GitHub workflow automation for experiments similar in nature to what `test.ps1` currently does.
- **UR-EXP-2 [KNOWN]:** The hosted automation should avoid the normal dependency on the operator's local machine.
- **UR-EXP-3 [KNOWN]:** The original single-workflow concept has been revised to two workflows because embedding is much more expensive than running the later experiment steps.
- **UR-EXP-4 [KNOWN]:** The first workflow should run only the embedding step over a set of working groups and store the resulting embeddings in Azure Blob Storage together with the replay journal.
- **UR-EXP-5 [KNOWN]:** The second workflow should run the indexing experiment over an existing set of embeddings rather than recomputing embeddings every time.
- **UR-EXP-6 [KNOWN]:** The expensive embedding stage should be reusable so rerunning the indexing experiment does not always repeat embedding work or introduce avoidable uncertainty.
- **UR-EXP-7 [KNOWN]:** Working-group inputs should be expressed as full rsync URLs rather than bare working-group names.
- **UR-EXP-8 [KNOWN]:** The embeddings used by the indexing workflow should derive from a checked-in file.
- **UR-EXP-9 [KNOWN]:** The checked-in embeddings file should contain the list of working groups and the container name to store them in.
- **UR-EXP-10 [KNOWN]:** The embedding workflow should be incrementally updateable so that if working-group content changes, only new embeddings are added.
- **UR-EXP-11 [KNOWN]:** The indexing workflow input must include a single published-profile version to run.
- **UR-EXP-12 [KNOWN]:** The workflow input surface should include a Docker image tag override while defaulting to the lab pipeline's published `main` tag when no custom tag is supplied.
- **UR-EXP-13 [KNOWN]:** The workflow input surface should include a public key for SSH access to the VM as a last-resort investigation path.
- **UR-EXP-14 [KNOWN]:** Workflow results should be uploaded as GitHub workflow artifacts after being downloaded from Azure Blob Storage.
- **UR-EXP-15 [KNOWN]:** The workflows must always deallocate the VM when the experiment concludes.
- **UR-EXP-16 [KNOWN]:** The workflows must not delete the resource group automatically because cleanup remains manual.
- **UR-EXP-17 [KNOWN]:** The operator still needs the resource-group name and storage-account name surfaced for manual inspection.
- **UR-EXP-18 [KNOWN]:** The design may assume the relevant container accepts the working-group list, the profile selector where applicable, and a SAS token for an Azure Blob container.
- **UR-EXP-19 [KNOWN]:** Do not solve the separate LAB block-storage-abstraction effort here.
- **UR-EXP-20 [INFERRED]:** The hosted workflows should authenticate to Azure through repository-owned GitHub Actions federation rather than a developer-local Azure session.
- **UR-EXP-21 [INFERRED]:** The new hosted automation boundary should orchestrate existing repository experiment/runtime surfaces without redefining indexer semantics, scale-test semantics, MCP semantics, or production-serving semantics.
- **UR-EXP-22 [KNOWN]:** The approved overlay block-store shape is memory cache plus local filesystem cache plus Azure Blob Storage backing, with writes persisting to Azure and reads served from the first layer that has the data.
- **UR-EXP-23 [KNOWN]:** All relevant repository components for this experiment path should be able to target either the existing regular filesystem block store or the approved overlay block store.
- **UR-EXP-24 [KNOWN]:** This increment should consume the landed overlay block-store implementation rather than redesigning or replacing it.
- **UR-EXP-25 [KNOWN]:** Hosted workflow changes should remove stale TODO-only overlay seams and instead use the approved executable overlay contract where that contract is now available.
- **UR-EXP-26 [KNOWN]:** The indexing workflow should remain comparable to `test.ps1`, including the post-index quality/report step rather than only the clustering-and-block-assembly step.
- **UR-EXP-27 [KNOWN]:** Enable the hosted workflow family to execute the overlay block-store path now that the overlay implementation is available.
- **UR-EXP-28 [KNOWN]:** Keep both hosted block-store targets selectable, but make `overlay` the default target for hosted workflow runs.

## Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-EXP-001 | Add | Introduce a separate hosted experiment-workflow requirements boundary for staged Azure-backed experiment automation | UR-EXP-1, UR-EXP-2, UR-EXP-21 |
| CM-EXP-002 | Add | Define two repository-owned hosted workflows: one for reusable embedding refresh and one for indexing experiments over precomputed embeddings | UR-EXP-3, UR-EXP-4, UR-EXP-5, UR-EXP-6 |
| CM-EXP-003 | Add | Define a checked-in embeddings-manifest contract that owns rsync source URLs and the Azure Blob container selection for reusable embedding datasets | UR-EXP-7, UR-EXP-8, UR-EXP-9 |
| CM-EXP-004 | Add | Require incremental embedding refresh so changed source content adds only new embeddings and preserves prior reusable embedding state | UR-EXP-4, UR-EXP-6, UR-EXP-10 |
| CM-EXP-005 | Add | Define hosted caller inputs for single-profile experiment selection, runner-image tag override, and break-glass SSH public-key injection | UR-EXP-11, UR-EXP-12, UR-EXP-13 |
| CM-EXP-006 | Add | Define Azure-backed execution and Blob-backed artifact retrieval for both workflows while leaving block-store abstraction redesign out of scope | UR-EXP-14, UR-EXP-17, UR-EXP-18, UR-EXP-19 |
| CM-EXP-007 | Add | Require unconditional VM deallocation while preserving the resource group for manual post-run inspection | UR-EXP-15, UR-EXP-16, UR-EXP-17 |
| CM-EXP-008 | Add | Align default runner-image selection with the lab pipeline's published `main` tag while preserving explicit tag override support | UR-EXP-12 |
| CM-EXP-009 | Add | Preserve repository semantic boundaries by keeping hosted experiment automation separate from scale-test, indexer, MCP, and production-serving contracts | UR-EXP-19, UR-EXP-20, UR-EXP-21 |
| CM-EXP-010 | Revise | Replace the deferred overlay TODO seam with an executable hosted block-store target contract that keeps filesystem available while making overlay the default | UR-EXP-22, UR-EXP-23, UR-EXP-27, UR-EXP-28 |
| CM-EXP-011 | Revise | Require the indexing-experiment workflow to include the rooted quality/report step needed to stay comparable to `test.ps1` | UR-EXP-14, UR-EXP-26 |

## Before / After

### BA-EXP-001

- **Before [KNOWN]:** The repository provides local experiment automation such as `test.ps1`, but it does not define a repository-owned hosted GitHub workflow boundary for Azure-backed experiments.
- **After [KNOWN]:** The repository will have an explicit requirements baseline for hosted staged experiment automation.

### BA-EXP-002

- **Before [KNOWN]:** The previously discussed hosted experiment concept treated embedding and later experiment execution as one workflow-shaped operation.
- **After [KNOWN]:** The hosted experiment boundary is split into two workflows so reusable embeddings can outlive individual indexing experiments.

### BA-EXP-003

- **Before [KNOWN]:** There is no approved repository-owned hosted contract for a checked-in artifact that identifies the reusable embedding dataset by rsync source list and Azure container destination.
- **After [KNOWN]:** The hosted workflow boundary owns a checked-in embeddings-manifest contract that names the full rsync URLs and target container for a reusable embedding dataset.

### BA-EXP-004

- **Before [KNOWN]:** The local experiment path can recompute expensive embedding work during repeated evaluation runs.
- **After [KNOWN]:** The hosted boundary requires embedding refresh and indexing experiments to be decoupled so repeated profile experiments can reuse an existing embedding dataset.

### BA-EXP-005

- **Before [KNOWN]:** Existing Azure VM lifecycle requirements do not define a hosted-workflow fallback that always deallocates the VM after either staged workflow concludes.
- **After [KNOWN]:** Both hosted workflows must always attempt VM deallocation while leaving the resource group intact for manual cleanup and inspection.

### BA-EXP-006

- **Before [KNOWN]:** The repository does not define a hosted report-handoff contract that downloads workflow outputs from Azure Blob Storage and republishes them as GitHub workflow artifacts.
- **After [KNOWN]:** The hosted workflow boundary owns a Blob-to-GitHub-artifact handoff for the embedding refresh outputs and the indexing experiment outputs that matter to operators.

### BA-EXP-007

- **Before [KNOWN]:** The hosted experiment spec preserved a filesystem-versus-overlay selection seam, but treated overlay execution as a deferred integration path guarded by TODO markers rather than an approved executable hosted mode.
- **After [KNOWN]:** The requirements now make overlay an approved executable hosted mode for both workflows, keep filesystem targeting available, and require the caller-visible default to be `overlay`.

### BA-EXP-008

- **Before [KNOWN]:** The hosted indexing-experiment requirements named an experiment report output but did not explicitly require the post-index quality/report step that makes the workflow comparable to `test.ps1`.
- **After [KNOWN]:** The hosted indexing-experiment requirements explicitly include rooted quality/report generation as part of the experiment workflow.

## Requirements

### Functional Requirements

#### RQ-EXP-001 - Experiment-workflow boundary

LexonArchiveBuilder SHALL provide a separate repository-owned automation boundary named `lexonarchivebuilder-experiment-workflow`.

- **Boundary [KNOWN]:** This boundary owns hosted orchestration for reusable embedding refresh and downstream indexing experiments over those embeddings.
- **Non-goal [KNOWN]:** This boundary does not redefine indexer execution semantics, scale-test semantics, MCP search semantics, or production-serving semantics.
- **Traceability:** UR-EXP-1, UR-EXP-21

#### RQ-EXP-002 - Hosted workflow family

`lexonarchivebuilder-experiment-workflow` SHALL define two repository-owned GitHub Actions workflow entrypoints:

1. an embedding-refresh workflow
2. an indexing-experiment workflow

- **Required property [KNOWN]:** The workflows are separate so expensive embedding work can be reused across multiple indexing experiments.
- **Traceability:** UR-EXP-3, UR-EXP-4, UR-EXP-5, UR-EXP-6

#### RQ-EXP-003 - Local-machine independence

The hosted workflow family SHALL be runnable without requiring the operator's local machine to perform deployment, waiting, result download, or artifact publication during the normal success path.

- **Constraint [INFERRED]:** Break-glass SSH access may exist for investigation, but it must not be a prerequisite for the expected workflow.
- **Traceability:** UR-EXP-2, UR-EXP-13

#### RQ-EXP-004 - Federated Azure authentication

The hosted workflow family SHALL authenticate to Azure through a repository-owned GitHub Actions federation path.

- **Constraint [INFERRED]:** The normal workflow contract should not depend on a developer-local Azure session.
- **Boundary [UNKNOWN]:** Subscription scoping, service-principal naming, and repository/environment secret placement are not yet specified in this phase.
- **Traceability:** UR-EXP-2, UR-EXP-20

#### RQ-EXP-005 - Checked-in embeddings manifest

The reusable embedding dataset for this workflow family SHALL be identified by a checked-in repository file.

- **Required manifest contents [KNOWN]:**
  1. one-or-more full rsync URLs
  2. the Azure Blob container name that stores the corresponding reusable embedding dataset
- **Boundary [KNOWN]:** Bare working-group names are not the approved contract for this increment.
- **Boundary [UNKNOWN]:** The exact file format, schema location, and naming convention are not yet specified in this phase.
- **Traceability:** UR-EXP-7, UR-EXP-8, UR-EXP-9

#### RQ-EXP-006 - Embedding-refresh workflow inputs

The embedding-refresh workflow SHALL accept a caller-visible selection of the checked-in embeddings manifest to refresh, plus any required runner-image and break-glass SSH inputs.

- **Required property [INFERRED]:** The workflow input should point at repository-owned reusable embedding intent rather than requiring callers to paste the rsync URL list inline for every run.
- **Traceability:** UR-EXP-8, UR-EXP-12, UR-EXP-13

#### RQ-EXP-007 - Incremental embedding refresh

The embedding-refresh workflow SHALL update the reusable embedding dataset incrementally so that source-content changes append only the newly required embeddings rather than recomputing all prior embeddings for the same manifest-defined dataset.

- **Required persisted outputs [KNOWN]:**
  1. the resulting embeddings in Azure Blob Storage
  2. the replay journal corresponding to that reusable embedding dataset
- **Rationale [KNOWN]:** Repeating expensive embedding work unnecessarily increases cost and uncertainty.
- **Boundary [UNKNOWN]:** The exact freshness-detection rule for deciding what counts as "new" content is not yet specified in this phase.
- **Traceability:** UR-EXP-4, UR-EXP-6, UR-EXP-10

#### RQ-EXP-008 - Embedding dataset reuse

The indexing-experiment workflow SHALL consume an existing reusable embedding dataset produced for a checked-in embeddings manifest rather than re-running the embedding stage as part of the normal experiment path.

- **Constraint [KNOWN]:** The indexing workflow is downstream of the reusable embedding dataset and should not treat embedding as an unconditional prerequisite step.
- **Traceability:** UR-EXP-5, UR-EXP-6, UR-EXP-8

#### RQ-EXP-008A - Hosted block-store target contract

The hosted workflow family and its supporting experiment-orchestration surfaces
SHALL support exactly two caller-selectable block-store targets:

1. the existing regular filesystem block-store path
2. the approved overlay block-store path composed of memory cache, local filesystem cache, and Azure Blob Storage backing

- **Required property [KNOWN]:** The overlay target is an approved executable hosted mode rather than a deferred TODO-only integration seam.
- **Required property [KNOWN]:** Both the embedding-refresh workflow and the indexing-experiment workflow use the same two-target contract.
- **Boundary [KNOWN]:** The caller-visible selection contract keeps both targets available rather than removing filesystem support in this increment.
- **Traceability:** UR-EXP-22, UR-EXP-23, UR-EXP-27, UR-EXP-28

#### RQ-EXP-008B - Hosted block-store target default

Whenever a hosted workflow caller does not explicitly choose a block-store
target, the workflow family SHALL default that selection to `overlay`.

- **Rationale [KNOWN]:** The hosted Azure-backed execution path should prefer the production-oriented overlay realization while preserving filesystem as an explicit fallback and comparison mode.
- **Constraint [KNOWN]:** Defaulting to `overlay` must not remove the caller's ability to choose filesystem explicitly.
- **Traceability:** UR-EXP-27, UR-EXP-28

#### RQ-EXP-009 - Indexing-experiment profile input

Each indexing-experiment workflow run SHALL target exactly one caller-selected published-profile version.

- **Rationale [KNOWN]:** The requested hosted indexing contract is per-profile rather than a local multi-profile sweep in one invocation.
- **Extensibility [INFERRED]:** Future workflow fan-out across multiple profiles may be added later without redefining the single-run contract approved here.
- **Traceability:** UR-EXP-11

#### RQ-EXP-010 - Runner image selection

The hosted workflow family SHALL allow callers to select the published runner-image tag used for the VM-side container execution.

- **Default [KNOWN]:** If the caller does not provide an explicit custom tag, the workflow uses the lab pipeline's published `main` tag for the approved runner image.
- **Required property [KNOWN]:** Callers may override the default with a specific published tag for reproducible targeting.
- **Boundary [KNOWN]:** This increment reuses lab-published images rather than requiring repository-local image builds inside the experiment workflows.
- **Traceability:** UR-EXP-12

#### RQ-EXP-011 - Break-glass SSH input

The hosted workflow family SHALL accept an SSH public key that can be injected into the experiment VM for last-resort investigation.

- **Operational intent [KNOWN]:** SSH exists as a break-glass debugging path when automated execution fails or behaves unexpectedly.
- **Constraint [KNOWN]:** SSH access must not become part of the primary success-path workflow contract.
- **Traceability:** UR-EXP-13

#### RQ-EXP-012 - Minimal Azure execution environment

The hosted workflow family SHALL deploy the minimal Azure environment needed to execute one embedding refresh or one indexing experiment and retrieve the relevant artifacts.

- **Required capabilities [KNOWN]:**
  1. one VM capable of running the approved container for the selected workflow
  2. one Azure Blob container reachable through a SAS credential path accepted by the relevant container
  3. deployment outputs sufficient for post-run operator inspection, including the resource-group name and storage-account name
- **Boundary [KNOWN]:** This increment may assume the relevant container accepts the working-group list, the profile selector where applicable, and the Blob-container SAS input.
- **Boundary [UNKNOWN]:** The exact IaC module/package reuse versus new experiment-specific IaC shape is not yet chosen in this phase.
- **Traceability:** UR-EXP-14, UR-EXP-17, UR-EXP-18, UR-EXP-19

#### RQ-EXP-013 - Embedding-refresh execution

The embedding-refresh workflow SHALL arrange for the deployed VM to run only the embedding-oriented stage needed to refresh the manifest-selected reusable embedding dataset.

- **Required effect [KNOWN]:** The workflow must store both the refreshed embeddings and the replay journal in Azure Blob Storage for later reuse.
- **Constraint [KNOWN]:** The normal success path is automated and does not rely on SSH.
- **Traceability:** UR-EXP-4, UR-EXP-10, UR-EXP-13, UR-EXP-18

#### RQ-EXP-014 - Indexing-experiment execution

The indexing-experiment workflow SHALL arrange for the deployed VM to run the indexing experiment over the selected reusable embedding dataset using the caller-selected published-profile version.

- **Constraint [KNOWN]:** The normal success path is automated and does not rely on SSH.
- **Boundary [KNOWN]:** This requirements patch treats the container's accepted input contract as an upstream dependency rather than redefining container CLI semantics here.
- **Traceability:** UR-EXP-5, UR-EXP-11, UR-EXP-13, UR-EXP-18

#### RQ-EXP-014A - Rooted quality/report step

The indexing-experiment workflow SHALL include the post-index rooted
quality/report step needed to make the hosted experiment comparable in nature to
the current `test.ps1` profile-evaluation workflow.

- **Required property [KNOWN]:** Producing the experiment result requires more than clustering-and-block-assembly alone; the workflow must also emit the corresponding quality/report artifact family for the selected profile run.
- **Boundary [UNKNOWN]:** The exact report bundle shape beyond the required rooted quality/report artifact is not yet specified in this phase.
- **Traceability:** UR-EXP-14, UR-EXP-26

#### RQ-EXP-015 - Blob-backed artifact handoff

Each hosted workflow SHALL write its operator-relevant output artifacts to Azure Blob Storage so the GitHub workflow can retrieve them after VM-side execution completes.

- **Embedding workflow minimum [KNOWN]:** The reusable embedding dataset and replay journal must be durably available in Blob Storage after the run.
- **Indexing workflow minimum [KNOWN]:** The rooted quality/report artifact family for the selected profile run must be durably available in Blob Storage after the run.
- **Boundary [UNKNOWN]:** The exact manifest, report, and supplemental log bundle naming conventions are not yet specified in this phase.
- **Traceability:** UR-EXP-4, UR-EXP-14, UR-EXP-18, UR-EXP-26

#### RQ-EXP-016 - Workflow artifact publication

After each workflow concludes, the hosted workflow SHALL upload the retrieved operator-relevant output artifact set as GitHub Actions workflow artifacts.

- **Required property [KNOWN]:** Workflow consumers should be able to obtain the relevant run outputs directly from the workflow run without separately browsing Azure storage.
- **Boundary [INFERRED]:** The embedding-refresh workflow may publish a smaller artifact family than the indexing workflow so long as the durable reusable dataset remains in Blob Storage.
- **Traceability:** UR-EXP-14

#### RQ-EXP-017 - Workflow outcome surface

Each hosted workflow SHALL surface whether the run passed or failed and SHALL also surface the resource-group name and storage-account name associated with the run.

- **Rationale [KNOWN]:** Operators need a quick run verdict plus stable identifiers for manual Azure inspection.
- **Boundary [UNKNOWN]:** Whether these values are emitted as job outputs, workflow summary entries, or both is not yet specified in this phase.
- **Traceability:** UR-EXP-14, UR-EXP-17

#### RQ-EXP-018 - Always-on VM deallocation

Each hosted workflow SHALL always attempt to deallocate the experiment VM after VM-side execution concludes, regardless of whether the run succeeded or failed.

- **Required property [KNOWN]:** Workflow-level cleanup must provide a fallback even when VM-side self-deallocation does not occur.
- **Constraint [KNOWN]:** This cleanup requirement applies to failure paths as well as success paths.
- **Traceability:** UR-EXP-15

#### RQ-EXP-019 - Resource-group preservation

The hosted workflow family SHALL NOT delete the experiment resource group automatically as part of normal or failure-path cleanup.

- **Operational intent [KNOWN]:** Resource-group cleanup remains a manual operator step so failed runs can still be inspected in Azure after the workflow completes.
- **Traceability:** UR-EXP-16, UR-EXP-17

### Boundary and Invariant Requirements

#### RQ-EXP-020 - Semantic non-interference

The hosted workflow family SHALL orchestrate existing repository-owned experiment and deployment surfaces without redefining:

1. indexer request or execution semantics
2. scale-test content-discovery semantics outside the manifest-owned source-list contract
3. MCP request/response semantics
4. the separately developed overlay block-store implementation beyond consuming its approved hosted caller contract
5. production-serving CDN and retrieval semantics

- **Traceability:** UR-EXP-19, UR-EXP-21, UR-EXP-24, UR-EXP-25

#### RQ-EXP-021 - Local/prod boundary preservation

The hosted workflow family SHALL remain an operator-automation and experiment-orchestration boundary rather than a new production-serving runtime contract.

- **Rationale [INFERRED]:** The repository already separates local/testing workflows, hosted packaging workflows, and production deployment concerns even when they share images or Azure resources.
- **Traceability:** UR-EXP-21

## Out of Scope

- redefining the experiment containers' internal CLI or API beyond the accepted input assumptions
- solving the separate LAB block-storage-abstraction effort
- deleting the Azure resource group automatically after the workflow run
- replacing the existing local experiment workflows for all use cases
- changing MCP server behavior
- changing production CDN publication behavior
- requiring SSH for the normal success path
- broadening the manifest contract to bare working-group names in this increment
- redesigning or reimplementing the upstream overlay block-store in this change set

## Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | The hosted workflows orchestrate staged experiment execution without changing MCP-serving behavior |
| Local/testing versus production semantics remain distinct | Preserved | The hosted workflows now prefer the production-oriented overlay target by default while preserving explicit filesystem selection for comparison and fallback without redefining the production-serving boundary |
| The architecture remains extensible to future content types | Preserved | The reusable manifest and staged workflow boundary focus on orchestration and dataset reuse rather than hard-coding new content-model semantics into indexer or MCP layers |
| Repository automation remains traceable and reusable | Preserved | The workflows reuse published images, Azure deployment primitives, checked-in manifest intent, and workflow-visible artifacts instead of relying on ad hoc local steps |

## Coverage Notes

- **Covered sources [KNOWN]:**
  - `test.ps1:1-98`
  - `README.md:174-233`
  - `docs/specs/lexonarchivebuilder-scale-test/requirements.md:11-31,125-260`
  - `docs/specs/lexonarchivebuilder-deployment/requirements.md:11-35,90-152`
  - `docs/specs/lexonarchivebuilder-image-publishing/requirements.md:11-21,60-121`
  - `.github/workflows/publish-images.yml:1-70`
  - `.github/workflows/run-embedding-refresh.yml:27-35,82-107`
  - `.github/workflows/run-indexing-experiment.yml:32-39,87-118`
  - `crates/lexonarchivebuilder-indexer/src/block_store.rs:36-64`
  - `crates/lexonarchivebuilder-indexer/src/config.rs:22-39,72-89`
  - `docs/specs/lexonarchivebuilder-indexer/requirements.md:141-144,186-188,230-232,552-568,703-779,1445`
  - user request in this session: "build a full github workflow to allow running experiments similar in nature to what test.ps1 currently does"
  - user revision in this session: "two workflows: 1) run just the embedding step over a set of working groups and store it in a azure blob store along with the replay journal 2) run the indexing experiment over a set of embeddings"
  - user clarification in this session: "Make the embeddings derive from a checked in file. Checked in file will then contain: 1) the list of work groups. 2) container name to store them in. It should also be incrementally updateable so if the content of the working groups changes, only new embeddings are added."
  - user request in this session: "enable overlay and make it the default"
  - user clarification in this session: "Keep both targets and default to `overlay`"
- **Excluded from this requirements artifact [KNOWN]:**
  - workflow YAML structure and job graph details
  - Bicep module changes
  - VM bootstrap implementation details
  - manifest file format details
  - Blob report naming conventions
  - test and verification implementation details
