<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder Deployment Requirements

## Document Status

- **Phase:** Phase 2 - Specification Changes
- **Status:** Revised requirements baseline being propagated into design and validation for alignment with the current Azure CDN Standard (Akamai) MVP implementation
- **Scope:** `lexonarchivebuilder-deployment` production deployment boundary for CDN-backed blob publication, VM-hosted indexing, VM-hosted embedding service, and supporting Azure network and secret-management assets layered above existing LexonArchiveBuilder feature boundaries

## USER-REQUEST

- **UR-DEPLOY-1 [KNOWN]:** Create a new deployment specification boundary owned by a new `lexonarchivebuilder-deployment` spec package.
- **UR-DEPLOY-2 [KNOWN]:** The MVP deployment uses one parameterized Azure resource group.
- **UR-DEPLOY-3 [KNOWN]:** The storage layer uses one standard general-purpose v2 storage account with hierarchical namespace off.
- **UR-DEPLOY-4 [KNOWN]:** The original request required public network access to the storage account to be disabled, but the later approved Azure CDN Standard (Akamai) hidden-origin path superseded that with an enabled public endpoint plus default-deny firewall allowlisting.
- **UR-DEPLOY-5 [KNOWN]:** The storage account must expose a private blob container with a parameterized name.
- **UR-DEPLOY-6 [KNOWN]:** The deployment must use an ARM-generated container SAS token with read permission, optional list permission, and long expiry.
- **UR-DEPLOY-7 [KNOWN]:** The SAS token must be stored in Key Vault or configured directly on the CDN origin, and it must never be exposed to clients.
- **UR-DEPLOY-8 [KNOWN]:** CDN origin access must be the only approved caller path to the storage account, with storage firewall and private-network controls enforcing that restriction.
- **UR-DEPLOY-9 [KNOWN]:** The CDN layer must include a parameterized public endpoint, origin authentication, URL rewrite, and caching rules.
- **UR-DEPLOY-10 [KNOWN]:** The deployment must include an indexing VM on Ubuntu LTS sized `F1s`.
- **UR-DEPLOY-11 [KNOWN]:** The indexing VM bootstrap must install Docker and Docker Compose, pull GHCR images, run the indexing compose stack, execute a boot -> index -> shutdown workflow, and shut the VM down on completion.
- **UR-DEPLOY-12 [KNOWN]:** The deployment must include an embedding VM on Ubuntu LTS sized `B1s`.
- **UR-DEPLOY-13 [KNOWN]:** The embedding VM bootstrap must install Docker and Docker Compose, pull a GHCR embedding-service image, start the embedding API container, and configure restart policies.
- **UR-DEPLOY-14 [KNOWN]:** The network layer uses one VNet with a VM subnet and a private-endpoint subnet, plus a private DNS zone link for `privatelink.blob.core.windows.net`.
- **UR-DEPLOY-15 [KNOWN]:** The deployment must define NSGs for the VM subnet and the private-endpoint subnet.
- **UR-DEPLOY-16 [KNOWN]:** Key Vault is optional but recommended for storing SAS tokens and CDN origin secrets.
- **UR-DEPLOY-17 [KNOWN]:** The deployment orchestration surface is a Bicep or ARM template package with modules `storage.bicep`, `cdn.bicep`, `network.bicep`, `vm-indexer.bicep`, `vm-embedder.bicep`, `keyvault.bicep`, and `main.bicep`.
- **UR-DEPLOY-18 [KNOWN]:** The main template parameter surface must include resource-group, storage-account, container, SAS-expiry, CDN endpoint, VM sizes, GHCR image tags, and VNet address ranges, and it must output the CDN public URL, embedding VM public URL when applicable, and optionally the SAS token.
- **UR-DEPLOY-19 [KNOWN]:** The indexing VM must run indexing once, shut down, and remain restartable for reindexing.
- **UR-DEPLOY-20 [KNOWN]:** The embedding VM must run continuously and auto-restart on failure.
- **UR-DEPLOY-21 [KNOWN]:** The CDN must continue serving cached content after SAS expiry and must prevent direct blob access.
- **UR-DEPLOY-22 [KNOWN]:** VMs should be isolated in private subnets, SSH and public IPs are optional, and SSH may be disabled after bootstrap.
- **UR-DEPLOY-23 [INFERRED]:** This deployment boundary is production-oriented and must not redefine the repository's local/testing workflows.
- **UR-DEPLOY-24 [INFERRED]:** The deployment should preserve LexonArchiveBuilder's intended CDN-backed RAG shape with no central control plane or extra server-side processing beyond indexing.
- **UR-DEPLOY-25 [INFERRED]:** The requested deployment boundary spans existing indexer, MCP, and production workflow seams, so it should remain separate from `lexonarchivebuilder-indexer` and `lexonarchivebuilder-archive-sync`.
- **UR-DEPLOY-26 [UNKNOWN]:** The authoritative MVP production embedding direction is not yet reconciled with the repository baseline that currently names Azure OpenAI for production while this request introduces a self-hosted embedding VM.

## Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-DEPLOY-001 | Add | Introduce a new repository-owned production deployment boundary under `docs/specs/lexonarchivebuilder-deployment/requirements.md` | UR-DEPLOY-1, UR-DEPLOY-25 |
| CM-DEPLOY-002 | Add | Define one parameterized Azure resource-group and module-oriented IaC package for the MVP deployment | UR-DEPLOY-2, UR-DEPLOY-17, UR-DEPLOY-18 |
| CM-DEPLOY-003 | Add | Define a hidden Azure Blob publication origin with parameterized container, origin secret generation, and no client-visible SAS exposure | UR-DEPLOY-3, UR-DEPLOY-4, UR-DEPLOY-5, UR-DEPLOY-6, UR-DEPLOY-7 |
| CM-DEPLOY-004 | Add | Define a CDN edge boundary that exposes the public hostname while isolating direct blob access and preserving origin-only credentials | UR-DEPLOY-8, UR-DEPLOY-9, UR-DEPLOY-21 |
| CM-DEPLOY-005 | Add | Define an explicit VM-hosted indexing runtime for one-shot batch execution and restartable reindex runs | UR-DEPLOY-10, UR-DEPLOY-11, UR-DEPLOY-19 |
| CM-DEPLOY-006 | Add | Define an explicit VM-hosted embedding-service runtime for continuous API availability with restart policy | UR-DEPLOY-12, UR-DEPLOY-13, UR-DEPLOY-20 |
| CM-DEPLOY-007 | Add | Define the Azure network, private endpoint, private DNS, and NSG topology required to isolate storage and VM traffic | UR-DEPLOY-8, UR-DEPLOY-14, UR-DEPLOY-15, UR-DEPLOY-22 |
| CM-DEPLOY-008 | Add | Define optional Key Vault integration for deployment-owned origin secrets | UR-DEPLOY-7, UR-DEPLOY-16 |
| CM-DEPLOY-009 | Revise | Replace the README's still-TBD production deployment shape with an explicit MVP deployment requirements baseline for this new boundary | UR-DEPLOY-1, UR-DEPLOY-17, UR-DEPLOY-24 |
| CM-DEPLOY-010 | Add | Preserve repository invariants around indexing/search separation, local-versus-production split, and no central control plane while adding Azure deployment requirements | UR-DEPLOY-23, UR-DEPLOY-24, UR-DEPLOY-25 |
| CM-DEPLOY-011 | Add | Record the unresolved production-embedding direction conflict introduced by the requested embedding VM | UR-DEPLOY-12, UR-DEPLOY-26 |

## Before / After

### BA-DEPLOY-001

- **Before [KNOWN]:** `README.md` described the repository's production direction only at a high level as Azure Blob Storage plus Azure OpenAI with a planned or TBD batch-oriented Azure runtime shape.
- **After [KNOWN]:** The repository has an explicit requirements baseline for a production deployment boundary in `docs/specs/lexonarchivebuilder-deployment/requirements.md`.

### BA-DEPLOY-002

- **Before [KNOWN]:** The repository had production-workflow requirements for `lexonarchivebuilder-archive-sync`, but no separate cross-cutting Azure deployment topology covering CDN, network, storage, and both long-lived and one-shot VMs.
- **After [KNOWN]:** The deployment requirements define a repository-owned Azure infrastructure boundary layered above the existing feature boundaries.

### BA-DEPLOY-003

- **Before [KNOWN]:** The production direction named Azure Blob Storage, but it did not define how public content would be served through a CDN while preventing direct blob access and keeping origin credentials off the client surface.
- **After [KNOWN]:** The deployment requirements define a CDN-backed hidden blob-origin model with origin-only secret handling and direct-blob denial as first-class requirements.

### BA-DEPLOY-004

- **Before [KNOWN]:** The repository did not define a deployment-owned VM lifecycle for one-shot indexing completion versus continuous embedding-service availability.
- **After [KNOWN]:** The deployment requirements separate a restartable batch indexing VM from a continuously running embedding VM.

### BA-DEPLOY-005

- **Before [KNOWN]:** The repository baseline described production embeddings as Azure OpenAI and did not define a self-hosted embedding-service VM in production.
- **After [UNKNOWN]:** This patch records the requested embedding VM as part of the proposed MVP deployment boundary, but it leaves final reconciliation with the existing Azure OpenAI production direction open pending user confirmation.

### BA-DEPLOY-006

- **Before [KNOWN]:** Local/testing and production boundaries were described at the architecture level, but the repository did not define a Bicep module structure for a production MVP deployment package.
- **After [KNOWN]:** The requirements define a module-oriented IaC surface with explicit parameter and output expectations.

## Requirements

### Functional Requirements

#### RQ-DEPLOY-001 - Deployment boundary

LexonArchiveBuilder SHALL provide a separate production deployment boundary named `lexonarchivebuilder-deployment`.

- **Boundary [KNOWN]:** This boundary owns deployment topology, secret-placement choices, network isolation, and runtime-hosting requirements.
- **Non-goal [KNOWN]:** This boundary does not redefine indexing semantics, MCP search semantics, or content-model semantics already owned elsewhere.
- **Traceability:** UR-DEPLOY-1, UR-DEPLOY-25

#### RQ-DEPLOY-002 - Production-oriented scope

`lexonarchivebuilder-deployment` SHALL remain limited to the production MVP deployment shape and SHALL NOT redefine the repository's local/testing workflows.

- **Rationale [INFERRED]:** The repository already treats local/testing as a fully local profile separate from production.
- **Traceability:** UR-DEPLOY-23, UR-DEPLOY-25

#### RQ-DEPLOY-003 - Single resource group

The MVP deployment SHALL place all required Azure resources in one parameterized resource group.

- **Constraint [KNOWN]:** The selected resource group must be caller-supplied through the deployment invocation boundary, whether by deployment scope selection or by an equivalent top-level orchestration input.
- **Traceability:** UR-DEPLOY-2, UR-DEPLOY-18

#### RQ-DEPLOY-004 - Module-oriented IaC package

The MVP deployment SHALL be specified as a module-oriented Bicep or ARM package with at least:

1. `storage.bicep`
2. `cdn.bicep`
3. `network.bicep`
4. `vm-indexer.bicep`
5. `vm-embedder.bicep`
6. `keyvault.bicep`
7. `main.bicep`

- **Constraint [KNOWN]:** `main.bicep` acts as the top-level orchestration entrypoint for the package.
- **Traceability:** UR-DEPLOY-17

#### RQ-DEPLOY-005 - Main deployment parameter and output surface

The top-level deployment package SHALL expose a parameter surface that includes:

1. storage-account name
2. blob-container name
3. SAS expiry
4. CDN endpoint name
5. VM sizes
6. GHCR image tags
7. VNet address ranges
8. index-output path
9. storage-access mode for VM workloads, including SAS-backed and managed-identity-capable deployment modes
10. embedding API port
11. embedder storage-access configuration when the embedding runtime requires storage access

- **Constraint [KNOWN]:** Resource-group selection may be carried by the deployment scope rather than a template parameter when `main.bicep` targets an existing resource group directly.

The top-level deployment package SHALL output:

1. the CDN public URL
2. the embedding VM public URL when that VM is reachable through a public endpoint
3. the SAS token only when the approved deployment mode explicitly requires that output

- **Boundary [INFERRED]:** Sensitive outputs should remain optional so the default deployment surface does not force secret disclosure.
- **Traceability:** UR-DEPLOY-17, UR-DEPLOY-18

#### RQ-DEPLOY-006 - Hidden-origin storage-account baseline

The MVP deployment SHALL provision one standard general-purpose v2 Azure Storage
account with hierarchical namespace disabled and no anonymous blob access.

- **Akamai compromise [KNOWN]:** For the approved Azure CDN Standard (Akamai)
  implementation path, public network access remains enabled so the CDN can
  reach the blob origin, but the storage firewall defaults to deny and permits
  only the approved CDN POP IP ranges and explicitly approved operator or VM
  access paths.
- **Constraint [KNOWN]:** Lifecycle rules are optional in this increment.
- **Traceability:** UR-DEPLOY-3, UR-DEPLOY-4

#### RQ-DEPLOY-007 - Private blob container

The storage account SHALL contain one parameterized blob container whose access level is private.

- **Traceability:** UR-DEPLOY-5

#### RQ-DEPLOY-008 - Origin-credential support

The deployment SHALL support blob-origin authentication through a
deployment-owned container-scoped SAS credential path.

- **Constraint [KNOWN]:** The deployment-owned origin credential path must remain
  hidden from clients.
- **Boundary [KNOWN]:** The current executable MVP package does not require or
  validate a CDN-origin path that consumes a storage-account key directly.
- **Traceability:** UR-DEPLOY-6, UR-DEPLOY-7, UR-DEPLOY-9

#### RQ-DEPLOY-008A - Container SAS generation

The deployment SHALL provision an ARM-generated container-scoped SAS credential for origin access with:

1. read permission
2. optional list permission
3. a long-lived expiry suitable for the MVP, such as one year

- **Boundary [UNKNOWN]:** Whether list permission is required by the final origin implementation remains open in this phase.
- **Traceability:** UR-DEPLOY-6

#### RQ-DEPLOY-009 - Origin secret placement

The deployment SHALL make the origin credential available either through Azure
Key Vault or through a deployment-owned post-deploy handoff artifact used to
attach the credential to the CDN origin configuration.

- **Constraint [KNOWN]:** The selected secret-distribution mode is
  deployment-configurable in this increment.
- **Traceability:** UR-DEPLOY-7, UR-DEPLOY-16

#### RQ-DEPLOY-009A - Post-deploy origin-query-string handoff

When the selected CDN provider path cannot attach the origin credential fully
through the deployed template surface, the deployment SHALL emit a
deployment-owned post-deploy handoff describing the required origin host, origin
path, and the origin-only credential attachment source or value needed for the
post-deploy query-string step.

- **Constraint [KNOWN]:** This handoff is an operator or automation step at the
  CDN origin boundary only and SHALL NOT expose the credential to clients.
- **Constraint [KNOWN]:** The handoff may carry either the query-string value
  itself or a deployment-owned source reference, depending on whether secret
  output is enabled.
- **Traceability:** UR-DEPLOY-7, UR-DEPLOY-9

#### RQ-DEPLOY-010 - Storage origin isolation

The storage account SHALL be reachable only through the approved CDN origin
access path and explicitly approved operator or VM access paths.

- **Required controls [KNOWN]:**
  - storage firewall restriction
  - private blob container access level
  - denial of direct client blob access
- **Approved Akamai path [KNOWN]:** The current implementation path constrains
  the storage public endpoint to the approved CDN POP IP ranges plus explicitly
  approved VM or operator access ranges and does not rely on a private-link
  origin path.
- **Traceability:** UR-DEPLOY-8, UR-DEPLOY-21, UR-DEPLOY-22

#### RQ-DEPLOY-011 - CDN public edge boundary

The MVP deployment SHALL provide one parameterized public CDN hostname whose origin serves content from the private storage boundary.

- **Constraint [KNOWN]:** The public edge must not forward origin credentials to clients.
- **Traceability:** UR-DEPLOY-8, UR-DEPLOY-9

#### RQ-DEPLOY-012 - CDN rule behavior

The CDN layer SHALL support:

1. URL rewrite from public paths to blob paths
2. caching with a default TTL
3. cache-policy override of blob headers when required by the approved deployment policy
4. an origin-only query-string credential attachment path that is never exposed
   to clients

- **Current implementation direction [KNOWN]:** The approved implementation path
  targets Azure CDN Standard (Akamai).
- **Traceability:** UR-DEPLOY-9

#### RQ-DEPLOY-013 - Cached-content continuity

The CDN configuration SHALL preserve the ability to serve already cached content even after the active origin SAS expires, subject to the normal limits of cached-object retention.

- **Constraint [KNOWN]:** This requirement does not permit exposing the SAS to clients as a workaround.
- **Traceability:** UR-DEPLOY-21

#### RQ-DEPLOY-014 - Indexing VM baseline

The MVP deployment SHALL provision one Ubuntu LTS indexing VM sized `F1s`.

- **Optional access [KNOWN]:** A public IP is optional in this increment.
- **Identity [KNOWN]:** Managed identity must be enabled.
- **Traceability:** UR-DEPLOY-10, UR-DEPLOY-22

#### RQ-DEPLOY-015 - Indexing VM bootstrap and batch lifecycle

The indexing VM bootstrap SHALL:

1. install Docker and Docker Compose
2. pull the approved GHCR images
3. run the indexing compose stack
4. execute the boot -> index -> shutdown workflow
5. shut the VM down after terminal completion

- **Lifecycle [KNOWN]:** The indexing runtime is one-shot per activation rather than continuously serving traffic.
- **Traceability:** UR-DEPLOY-11, UR-DEPLOY-19

#### RQ-DEPLOY-016 - Restartable reindex execution

The indexing VM deployment contract SHALL allow a later restart to execute reindexing again without requiring a second deployment boundary.

- **Boundary [KNOWN]:** This requirement covers operational restartability, not application-level replay semantics already owned by the indexer or archive-sync workflows.
- **Traceability:** UR-DEPLOY-19

#### RQ-DEPLOY-017 - Embedding VM baseline

The MVP deployment SHALL provision one Ubuntu LTS embedding VM sized `B1s`.

- **Optional access [KNOWN]:** A public IP is optional in this increment.
- **Traffic intent [KNOWN]:** The embedding API is expected to listen on a deployment-configured port such as `8080`.
- **Traceability:** UR-DEPLOY-12, UR-DEPLOY-22

#### RQ-DEPLOY-018 - Embedding VM bootstrap and continuous-service lifecycle

The embedding VM bootstrap SHALL:

1. install Docker and Docker Compose
2. pull the approved GHCR embedding-service image
3. start the embedding API container
4. configure restart policy for continuous service availability

- **Lifecycle [KNOWN]:** The embedding runtime is continuous and auto-restarting rather than one-shot.
- **Traceability:** UR-DEPLOY-13, UR-DEPLOY-20

#### RQ-DEPLOY-018A - Runtime-specific deployment parameters

The deployment parameter surface SHALL preserve the runtime-specific operator
inputs required by the approved MVP shape, including:

1. index-output path for the indexing runtime
2. SAS-backed or managed-identity-capable storage access selection for indexing
   runtime access
3. embedding API port
4. embedder storage-access configuration when required

- **Boundary [KNOWN]:** These parameters shape deployment wiring and bootstrap
  behavior but do not redefine application semantics owned by the hosted
  runtimes.
- **Traceability:** UR-DEPLOY-11, UR-DEPLOY-13, UR-DEPLOY-18

#### RQ-DEPLOY-019 - Embedding-provider direction preservation

The deployment boundary SHALL make the MVP embedding-hosting choice explicit without silently redefining the repository's broader production embedding direction.

- **Conflict [KNOWN]:** The current repository baseline names Azure OpenAI for production, while this request introduces a self-hosted embedding VM.
- **Resolution gap [UNKNOWN]:** The authoritative production direction for this MVP remains open after Phase 1 approval.
- **Traceability:** UR-DEPLOY-12, UR-DEPLOY-20, UR-DEPLOY-26

#### RQ-DEPLOY-020 - Shared network topology

The MVP deployment SHALL provision one VNet with at least:

1. one VM subnet
2. one private-endpoint subnet

- **Traceability:** UR-DEPLOY-14

#### RQ-DEPLOY-021 - Optional private storage endpoint and DNS

The deployment SHALL preserve support for an optional storage private endpoint
and a private DNS zone link for `privatelink.blob.core.windows.net`.

- **Current implementation direction [KNOWN]:** The approved Akamai hidden-origin
  path does not require the private endpoint for CDN-to-origin reachability, but
  the deployment package keeps the private-endpoint subnet and optional private
  endpoint as a VM-side extension seam.
- **Traceability:** UR-DEPLOY-14

#### RQ-DEPLOY-022 - Network security groups

The deployment SHALL provision one NSG for the VM subnet and one NSG for the private-endpoint subnet.

- **Required baseline [KNOWN]:**
  - VM-subnet rules must allow required outbound internet access
  - indexing-VM rules may allow SSH only when enabled by deployment parameters
  - embedding-VM rules may allow the embedding API port only when that endpoint is intentionally exposed
  - private-endpoint subnet rules must remain locked down
- **Traceability:** UR-DEPLOY-15, UR-DEPLOY-22

#### RQ-DEPLOY-023 - Optional Key Vault integration

The MVP deployment SHALL support an optional Azure Key Vault for origin secrets such as SAS credentials and CDN-origin secrets.

- **Recommended path [KNOWN]:** Key Vault is recommended but not mandatory in this increment.
- **Traceability:** UR-DEPLOY-7, UR-DEPLOY-16

#### RQ-DEPLOY-024 - No client-visible origin credentials

The deployment SHALL prevent clients from observing SAS tokens, storage-account keys, or equivalent origin credentials through public URLs, client configuration, or required request headers.

- **Traceability:** UR-DEPLOY-7, UR-DEPLOY-9, UR-DEPLOY-21

#### RQ-DEPLOY-024A - Optional custom domain and operator-managed certificate

The MVP deployment SHALL preserve support for an optional CDN custom domain and
an operator-managed bring-your-own-certificate TLS path.

- **Boundary [KNOWN]:** DNS automation and certificate automation remain
  optional in this increment.
- **Constraint [KNOWN]:** The current Akamai MVP path treats certificate
  attachment as an operator-managed post-deploy step rather than a fully
  automated managed-certificate flow.
- **Traceability:** UR-DEPLOY-9

### Boundary and Invariant Requirements

#### RQ-DEPLOY-025 - Indexing/search separation

`lexonarchivebuilder-deployment` SHALL preserve the repository's separation between indexing-time processing and search-serving behavior.

- **Rationale [KNOWN]:** The repository baseline keeps indexing separate from the MCP search surface.
- **Traceability:** UR-DEPLOY-24, UR-DEPLOY-25

#### RQ-DEPLOY-026 - No central control plane

The MVP deployment SHALL remain compatible with the repository's intended shape of no central control plane or server-side processing layer beyond indexing and the explicitly approved embedding-service runtime.

- **Constraint [INFERRED]:** The deployment must realize hosting and connectivity without introducing a repository-local orchestration service as a new application boundary.
- **Traceability:** UR-DEPLOY-24

#### RQ-DEPLOY-027 - Stable application boundaries

The deployment requirements SHALL remain subordinate to the existing application boundaries owned by `lexonarchivebuilder-indexer`, `lexonarchivebuilder-mcp`, and `lexonarchivebuilder-archive-sync`.

- **Non-goal [KNOWN]:** This deployment package does not redefine content ingestion order, replay semantics, or MCP result semantics.
- **Traceability:** UR-DEPLOY-23, UR-DEPLOY-25

#### RQ-DEPLOY-028 - Future content-type extensibility

The deployment boundary SHALL not hard-code infrastructure assumptions that prevent future content types from using the same storage, indexing, and retrieval deployment seams.

- **Constraint [INFERRED]:** Blob layout, CDN pathing, and VM wiring must remain generic enough for content families beyond the current email and document focus.
- **Traceability:** UR-DEPLOY-24

## Out of Scope

- Redefining indexer request, replay, chunking, or block-publication semantics
- Redefining MCP search, retrieval, ranking, or result-shaping behavior
- Finalizing the exact host-side boot integration mechanism for the VMs
- Re-automating the manual CDN origin-query-string attachment step
- Finalizing whether Key Vault is mandatory or optional in the executable MVP
- Finalizing whether SSH or public-IP exposure is enabled by default
- Redefining the repository's local/testing workflows

## Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | The deployment boundary hosts existing runtimes but does not redefine their contracts |
| Repository shape remains CDN-backed and avoids a new central control plane | Preserved | The requirements use CDN plus VMs and do not introduce a repository-local orchestration service |
| Local/testing stays distinct from production | Preserved | This package is explicitly production-oriented and leaves local/testing behavior untouched |
| Environment-specific storage and embedding behavior stays behind stable seams | Preserved with open question | Storage remains deployment-configured, but the production embedding direction still needs explicit reconciliation |
| Future content-type extensibility remains possible | Preserved | The requirements focus on shared hosting and storage seams rather than content-type-specific infrastructure |
| Production runtime shape is explicit rather than TBD | Revised with approved direction change | This patch introduces a dedicated production deployment boundary instead of leaving deployment shape only in README-level direction |

## Open Questions / Discovery Gaps

- **Q-DEPLOY-001 [UNKNOWN]:** Should the MVP deployment spec treat the requested embedding VM as the authoritative production embedding path, or as an MVP-only exception while Azure OpenAI remains the long-term production direction?
- **Q-DEPLOY-002 [UNKNOWN]:** Is Azure Key Vault merely recommended, or should it be required whenever the deployment uses a SAS or other origin credential?
- **Q-DEPLOY-003 [UNKNOWN]:** Should a future increment add a private-link-capable origin path, or does the approved Akamai hidden-origin allowlist model remain sufficient beyond this MVP?
- **Q-DEPLOY-003A [UNKNOWN]:** How will the approved CDN POP IP allowlist be sourced and rotated operationally for the Akamai hidden-origin path?
- **Q-DEPLOY-004 [UNKNOWN]:** Is blob-container `list` permission actually required by the chosen CDN-origin implementation, or should the SAS remain read-only?
- **Q-DEPLOY-005 [UNKNOWN]:** Should SSH and public IP exposure default to disabled for both VMs, with only parameterized opt-in for operator access?
- **Q-DEPLOY-006 [UNKNOWN]:** Should the top-level deployment ever output the SAS token directly, or should secret outputs be prohibited once Key Vault integration is enabled?
- **Q-DEPLOY-007 [UNKNOWN]:** If a future increment adds storage-account-key-backed CDN origin authentication, should that secret be publishable only through Key Vault or another deployment-owned handoff with no direct operator output?

## Coverage Notes

- **Covered sources [KNOWN]:**
  - `README.md:20-28`
  - `README.md:44-49`
  - `README.md:72-80`
  - `docs/specs/lexonarchivebuilder-archive-sync/requirements.md:202-219`
  - `docs/specs/lexonarchivebuilder-archive-sync/design.md:11-19`
  - `docs/specs/lexonarchivebuilder-scale-test/requirements.md:20-21`
  - user request in this session beginning `MVP Deployment Requirements (LLM-Ready Specification)`
  - user clarification in this session selecting `New \`lexonarchivebuilder-deployment\` spec package (Recommended)`

- **Sampled claim re-checks [KNOWN]:**
  - The repository baseline still describes production only at a high level in `README.md:72-80`.
  - The repository baseline still describes the overall architecture as a CDN-backed RAG system in `README.md:20-28`.
  - `lexonarchivebuilder-archive-sync` already owns a VM-hosted workflow boundary, but not the full shared deployment topology, in `docs/specs/lexonarchivebuilder-archive-sync/design.md:11-19`.

- **Excluded from this phase [KNOWN]:**
  - design and validation implementation beyond the specification artifacts
  - Rust implementation, Bicep files, bootstrapping scripts, and deployment automation assets
