<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder Deployment Design

## Status

Phase 2 specification patch for the approved Azure MVP deployment requirements in
`docs/specs/lexonarchivebuilder-deployment/requirements.md`.

## Scope

This document specifies the LexonArchiveBuilder-owned design for realizing
`lexonarchivebuilder-deployment` as a production Azure deployment boundary that
provides:

- a hidden Blob Storage origin
- a public CDN edge
- one restartable one-shot indexing VM
- one continuous embedding-service VM
- one shared private network and secret-placement model
- one module-oriented Bicep or ARM deployment package

This document is layered on top of:

- `docs/specs/lexonarchivebuilder-deployment/requirements.md`
- `docs/specs/lexonarchivebuilder-archive-sync/requirements.md`
- `docs/specs/lexonarchivebuilder-archive-sync/design.md`
- `docs/specs/lexonarchivebuilder-indexer/requirements.md`
- `docs/specs/lexonarchivebuilder-mcp/requirements.md`
- `README.md`

This document does not redefine indexing semantics, MCP search semantics,
archive-sync workflow-stage semantics, or the application-owned storage and
embedding contracts themselves. Those remain owned by the existing repository
boundaries.

## Impact Map

### Directly affected artifacts

- `docs/specs/lexonarchivebuilder-deployment/requirements.md`
- `docs/specs/lexonarchivebuilder-deployment/design.md`
- `docs/specs/lexonarchivebuilder-deployment/validation.md`

### Indirectly affected artifacts

- future Bicep or ARM deployment modules under the new deployment boundary
- VM bootstrap assets for the indexing and embedding runtimes
- Docker Compose runtime assets referenced by the VM bootstrap steps
- deployment-facing secret and managed-identity wiring
- operator documentation for deployment, reindex activation, and secret rotation
- README production-direction wording once the deployment implementation exists

### Unaffected artifacts

- local/testing workflows under `docs/specs/lexonarchivebuilder-scale-test/*`
- MCP contract semantics under `docs/specs/lexonarchivebuilder-mcp/*`
- indexer request, replay, and chunking semantics under
  `docs/specs/lexonarchivebuilder-indexer/*`
- archive-sync source-acquisition and publication semantics under
  `docs/specs/lexonarchivebuilder-archive-sync/*`

## Design Goals

The `lexonarchivebuilder-deployment` design is intended to be:

- production-oriented without altering local/testing behavior
- CDN-backed and hidden-origin at the storage boundary
- explicit about origin-secret ownership and client-secret exclusion
- module-oriented so deployment concerns remain composable
- consistent with the repository's no-central-control-plane direction
- separated between one-shot indexing work and continuous embedding service
- generic enough for future content types to reuse the same deployment seams
- explicit about open design gaps rather than silently resolving them

## Boundary Design

### DSG-LFD-001 `Deployment boundary ownership`

`lexonarchivebuilder-deployment` owns Azure resource topology, deployment-time
parameterization, network isolation, VM-hosting shape, origin-secret placement,
and deployment outputs.

`lexonarchivebuilder-deployment` does not own index construction semantics, MCP
response semantics, content normalization, or archive-sync work ordering.

**Traces to:** RQ-DEPLOY-001, RQ-DEPLOY-002, RQ-DEPLOY-025, RQ-DEPLOY-027

### DSG-LFD-002 `Top-level module composition`

The deployment package is realized as one top-level composition module,
`main.bicep`, that orchestrates subordinate modules for storage, CDN, network,
indexer VM, embedder VM, and optional Key Vault.

The subordinate modules preserve one deployment-owned boundary per major Azure
concern rather than merging unrelated resource families into a single template.

The current design fixes the required module set to:

1. `storage.bicep`
2. `cdn.bicep`
3. `network.bicep`
4. `vm-indexer.bicep`
5. `vm-embedder.bicep`
6. `keyvault.bicep`
7. `main.bicep`

**Traces to:** RQ-DEPLOY-003, RQ-DEPLOY-004, RQ-DEPLOY-005

### DSG-LFD-002A `Deployment parameter families`

`main.bicep` preserves one deployment-owned parameter surface that includes both
shared infrastructure inputs and runtime-specific inputs.

The shared infrastructure inputs include deployment names, network ranges, and
origin-expiry controls. The runtime-specific inputs include:

- indexing-runtime image tags
- embedding-runtime image tags
- index-output path
- storage-access mode for VM workloads, including SAS-backed and
  managed-identity-capable deployment modes
- embedding API port
- embedder storage-access configuration when required

This preserves the caller-visible deployment contract requested for the MVP
without pushing runtime-wiring choices into undocumented module-local defaults.

**Traces to:** RQ-DEPLOY-005, RQ-DEPLOY-018A

### DSG-LFD-003 `Shared private network topology`

The deployment realizes one VNet with:

1. one VM subnet for the indexer and embedder VMs
2. one private-endpoint subnet for storage private-endpoint resources

The VM subnet carries the deployment-owned NSG rules for workload ingress and
egress. The private-endpoint subnet carries a separate locked-down NSG and
preserves a later private-endpoint extension seam.

When the optional private-endpoint path is enabled, the storage account is
resolved by deployment-owned workloads through the private DNS zone link for
`privatelink.blob.core.windows.net`. The current Akamai hidden-origin path does
not require that private endpoint for CDN-to-origin reachability.

**Traces to:** RQ-DEPLOY-020, RQ-DEPLOY-021, RQ-DEPLOY-022

### DSG-LFD-004 `Hidden blob-origin model`

The deployment uses one standard general-purpose v2 storage account and one
private blob container as the origin artifact store.

For the approved Azure CDN Standard (Akamai) path, public network access stays
enabled on the storage account so the CDN can reach the blob origin, but the
firewall defaults to deny and permits only the approved CDN POP IP ranges plus
explicitly approved VM or operator access ranges.

The private container is the only required public-content origin described by
this package. Public content is therefore served through the CDN edge rather
than from directly browsable blob URLs, and anonymous blob access remains
disabled.

Lifecycle management remains optional and is not required to satisfy the MVP
deployment contract.

**Traces to:** RQ-DEPLOY-006, RQ-DEPLOY-007, RQ-DEPLOY-010

### DSG-LFD-004A `Origin-credential placement`

The current executable deployment package supports one deployment-owned
blob-origin credential family:

1. container-scoped SAS

That origin credential is made available either:

1. in Azure Key Vault for later origin configuration consumption, or
2. in a deployment-owned post-deploy handoff artifact consumed by an operator or
   later automation step

The deployment-owned secret is never projected onto the public client surface.

The package may preserve storage-account-key material in Key Vault as an
operator-owned future extension seam, but that key is not the approved CDN
origin-authentication path for this increment.

**Traces to:** RQ-DEPLOY-008, RQ-DEPLOY-009, RQ-DEPLOY-023, RQ-DEPLOY-024

### DSG-LFD-004B `Container SAS realization`

When the deployment selects the container-SAS origin path, it uses an
ARM-generated container-scoped SAS with the approved permission and expiry
shape.

This preserves the requested long-lived read-oriented origin credential path
without making it the only admissible origin-authentication model in the
specification.

**Traces to:** RQ-DEPLOY-008A, RQ-DEPLOY-009

### DSG-LFD-004C `Manual Akamai origin-query-string attachment`

The current Akamai implementation path generates the container SAS inside the
deployment boundary but does not fully attach that query string to the CDN
origin through the deployed template surface.

Instead, the deployment emits a deployment-owned handoff describing the origin
host, origin path, and the origin-only query-string value or source reference
that must be used for the post-deploy attachment step. This keeps the SAS off
the client surface while acknowledging the current provider-specific limitation
in the executable package.

**Traces to:** RQ-DEPLOY-009A, RQ-DEPLOY-012, RQ-DEPLOY-024

### DSG-LFD-005 `CDN edge projection`

The CDN boundary exposes one parameterized public hostname whose origin maps to
the hidden blob origin through deployment-owned origin wiring plus a post-deploy
origin-configuration handoff.

The CDN layer owns:

- public path to blob-path rewrite
- cache behavior and default TTL
- optional blob-header override behavior when deployment policy requires it

The CDN layer does not expose SAS credentials or other origin credentials to
clients.

When enabled, the CDN layer also carries the optional custom-domain and
operator-managed bring-your-own-certificate attachment without changing the
underlying hidden-origin model.

**Traces to:** RQ-DEPLOY-011, RQ-DEPLOY-012, RQ-DEPLOY-013, RQ-DEPLOY-024, RQ-DEPLOY-024A

### DSG-LFD-005A `Blob-origin isolation enforcement`

The deployment enforces the CDN-backed origin model by combining:

- private blob container access
- default-deny storage firewall rules
- allowlisting of approved CDN POP IP ranges
- explicit allowlisting of any approved VM subnet or operator public IP ranges
- storage-firewall or equivalent origin restriction so direct public callers are
  not accepted as blob clients

This preserves the invariant that the CDN-origin network identity is the only
approved public-edge caller class, even though the storage public endpoint
remains enabled for the hidden-origin Akamai path.

**Traces to:** RQ-DEPLOY-010, RQ-DEPLOY-011, RQ-DEPLOY-024

### DSG-LFD-006 `Indexer VM runtime`

The indexing runtime is hosted on one Ubuntu LTS `Standard_DS1_v2` VM with
managed identity enabled.

Its bootstrap sequence is deployment-owned and performs the approved runtime
bring-up steps:

1. install Docker and Docker Compose
2. acquire the approved GHCR image set
3. start the indexing compose stack
4. run one boot -> index -> shutdown activation
5. trigger VM shutdown at terminal completion

This VM remains batch-oriented and restartable for later reindex activation,
without becoming a continuously serving application tier.

The approved sizing rationale is the approximately 3.5 GiB RAM capacity of
`Standard_DS1_v2`, replacing the earlier `F1s` baseline that no longer matches
current workload expectations. Any quoted hourly cost remains an operator-facing
pricing note rather than a design invariant.

**Traces to:** RQ-DEPLOY-014, RQ-DEPLOY-015, RQ-DEPLOY-016

### DSG-LFD-006A `Indexer VM network policy`

The indexer VM shares the deployment VM subnet and consumes only the minimum
deployment-owned network surface needed for:

- outbound internet access required for package installation or GHCR pulls
- approved access to the storage origin through either firewall-allowlisted
  public-endpoint access or the optional private-endpoint path
- optional SSH only when enabled by deployment parameters

The design does not require a public IP for normal indexing operation.

**Traces to:** RQ-DEPLOY-014, RQ-DEPLOY-022

### DSG-LFD-007 `Embedding VM runtime`

The embedding runtime is hosted on one Ubuntu LTS `B1s` VM whose bootstrap
installs Docker and Docker Compose, pulls the approved GHCR embedding-service
image, starts the embedding API container, and configures restart policy for
continuous availability.

Unlike the indexer VM, the embedding VM is modeled as a continuously running
service endpoint rather than as a one-shot batch worker.

Its runtime wiring remains parameterized so the embedding API port and any
required storage-access configuration are deployment-controlled rather than
hard-coded in the template family.

**Traces to:** RQ-DEPLOY-017, RQ-DEPLOY-018, RQ-DEPLOY-018A

### DSG-LFD-007A `Embedding VM exposure policy`

The embedding VM may expose its service port through deployment-controlled NSG
rules only when the approved access model requires that exposure.

Public IP assignment and direct public reachability therefore remain optional
deployment choices rather than baseline requirements of the service contract.

**Traces to:** RQ-DEPLOY-017, RQ-DEPLOY-022

### DSG-LFD-008 `Secret and identity boundary`

Managed identity is enabled at least on the indexing VM, and the deployment
reserves Key Vault as the preferred secret-placement seam when deployment-owned
origin credentials must be stored for later operator-owned consumption.

This design keeps secret retrieval and secret placement inside deployment-owned
infrastructure seams rather than placing long-lived origin credentials in client
configuration or public URLs. When the current Akamai path requires a manual
origin-query-string attachment step, the handoff remains deployment-owned and
operator-facing rather than client-facing, and may carry either the credential
value itself or a source reference to Key Vault or an explicitly enabled secret
output.

**Traces to:** RQ-DEPLOY-009, RQ-DEPLOY-014, RQ-DEPLOY-023, RQ-DEPLOY-024

### DSG-LFD-009 `Deployment parameter and output model`

`main.bicep` owns one caller-facing parameter surface for deployment identity,
network ranges, runtime image tags, secret expiry, and naming inputs.

`main.bicep` also owns the approved output surface for:

1. the CDN public URL
2. the embedding VM public URL when the chosen access model creates one
3. optional secret outputs only when explicitly approved by the deployment mode

This keeps deployment discovery and operator handoff centralized at the
top-level module without forcing every child module to publish user-facing
outputs independently.

**Traces to:** RQ-DEPLOY-003, RQ-DEPLOY-005, RQ-DEPLOY-018A

### DSG-LFD-010 `Production embedding-direction preservation seam`

The approved requirements preserve an open conflict between:

- the repository baseline, which currently names Azure OpenAI as the production
  embedding direction
- the current deployment request, which introduces a self-hosted embedding VM

The design therefore makes the embedding VM an explicit MVP deployment artifact
without claiming that this specification has resolved the broader repository
production-direction question.

This preserves traceability and avoids silently rewriting the baseline
production embedding narrative in another document.

**Traces to:** RQ-DEPLOY-019

## Invariant Design

### DSG-LFD-011 `Indexing and search separation`

The deployment package hosts existing runtime boundaries but does not merge
indexing and search-serving concerns into one new application contract.

The indexing VM remains a batch-oriented indexing surface. The CDN edge remains
a publication and retrieval surface. The deployment package does not redefine
MCP semantics or introduce a central application server that combines those
roles.

**Traces to:** RQ-DEPLOY-025, RQ-DEPLOY-026, RQ-DEPLOY-027

### DSG-LFD-012 `Future content-type neutrality`

The deployment topology is defined around shared hosting, networking, storage,
and publication seams rather than around email-specific or document-specific
infrastructure.

That neutrality preserves the repository's future-content extensibility without
requiring a second deployment boundary for later content classes.

**Traces to:** RQ-DEPLOY-028

## Design Notes

- **[KNOWN: current CDN SKU choice]** The aligned executable MVP package targets
  Azure CDN Standard (Akamai).
- **[UNKNOWN: final Key Vault policy]** The specification preserves Key Vault as
  optional but recommended and does not yet upgrade it to mandatory.
- **[KNOWN: current custom-domain operating mode]** The specification preserves
  optional custom-domain support with operator-managed BYOC TLS and does not
  require DNS or certificate automation in this increment.
- **[UNKNOWN: final CDN POP allowlist source]** The specification does not yet
  fix the operational source or refresh workflow for the Akamai POP IP ranges.
- **[UNKNOWN: final SSH and public-IP defaults]** Operator-access defaults
  remain open pending later review.

## Coverage Notes

- **Covered sources [KNOWN]:**
  - `docs/specs/lexonarchivebuilder-deployment/requirements.md`
  - `README.md:20-28`
  - `README.md:44-49`
  - `README.md:72-80`
  - `docs/specs/lexonarchivebuilder-archive-sync/design.md:11-19`
  - `docs/specs/lexonarchivebuilder-mcp/design.md:23-45`
- **Excluded from this phase [KNOWN]:**
  - implementation files, Bicep modules, VM bootstrap scripts, and tests
