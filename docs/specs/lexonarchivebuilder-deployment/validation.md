<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder Deployment Validation

## Status

Phase 2 validation patch for the approved Azure MVP deployment requirements and
design in `docs/specs/lexonarchivebuilder-deployment/requirements.md` and
`docs/specs/lexonarchivebuilder-deployment/design.md`.

## Validation Scope

These validation entries define the expected conformance surface for the
LexonArchiveBuilder-owned `lexonarchivebuilder-deployment` boundary.

This package validates deployment-owned topology, origin isolation, VM runtime
hosting shape, parameterization, and invariant preservation. It does not
redefine validation already owned by `lexonarchivebuilder-indexer`,
`lexonarchivebuilder-mcp`, `lexonarchivebuilder-archive-sync`, or the delegated
Azure service contracts themselves.

## Validation Entries

### VAL-LFD-001

Inspect the repository specification surface for `lexonarchivebuilder-deployment`.

**Pass condition:** the repository defines `lexonarchivebuilder-deployment` as a
separate production deployment boundary rather than folding Azure deployment
topology into the indexer, MCP, or archive-sync semantic boundaries.

**Traces to:** RQ-DEPLOY-001, RQ-DEPLOY-002, RQ-DEPLOY-027, DSG-LFD-001

### VAL-LFD-002

Inspect the deployment package structure.

**Pass condition:** the package defines one top-level deployment entrypoint and
the approved subordinate module set for storage, CDN, network, indexer VM,
embedder VM, and optional Key Vault.

**Traces to:** RQ-DEPLOY-003, RQ-DEPLOY-004, DSG-LFD-002

### VAL-LFD-002A

Inspect the top-level deployment parameter and output contract.

**Pass condition:** the top-level package exposes the approved parameter families
for names, ranges, VM sizes, image tags, and SAS expiry; it outputs the CDN
public URL, emits an embedding VM public URL only when applicable, and does not
force secret output in the default case.

**Traces to:** RQ-DEPLOY-005, DSG-LFD-002, DSG-LFD-009

### VAL-LFD-002B

Inspect the runtime-specific deployment parameter surface.

**Pass condition:** the top-level package preserves the approved runtime-specific
deployment inputs for index-output path, VM storage-access mode, embedding API
port, and embedder storage-access configuration when required.

**Traces to:** RQ-DEPLOY-018A, DSG-LFD-002A, DSG-LFD-009

### VAL-LFD-003

Inspect the storage-account baseline.

**Pass condition:** the deployment provisions one standard general-purpose v2
storage account with hierarchical namespace disabled, no anonymous blob access,
and a private container as the origin artifact store; for the approved Akamai
path, the storage firewall defaults to deny while the storage public endpoint
remains enabled only for allowlisted origin callers.

**Traces to:** RQ-DEPLOY-006, RQ-DEPLOY-007, DSG-LFD-004

### VAL-LFD-003A

Inspect origin-credential generation and placement.

**Pass condition:** the deployment supports the approved container-scoped SAS
origin path; that credential is made available either through Key Vault or a
deployment-owned post-deploy handoff artifact, and it is not exposed to
clients. Any stored storage-account key material is treated only as an auxiliary
future-extension secret rather than as the validated current CDN origin path.

**Traces to:** RQ-DEPLOY-008, RQ-DEPLOY-009, RQ-DEPLOY-023, RQ-DEPLOY-024,
DSG-LFD-004A, DSG-LFD-008

### VAL-LFD-003A1

Inspect the container-SAS-specific origin path.

**Pass condition:** when the deployment selects the SAS-backed origin path, it
uses an ARM-generated container-scoped SAS with read permission, optional list
permission, and long-lived expiry.

**Traces to:** RQ-DEPLOY-008A, DSG-LFD-004B

### VAL-LFD-003A2

Inspect the Akamai post-deploy origin-query-string handoff.

**Pass condition:** when the provider path requires a manual origin-query-string
attachment step, the deployment package emits a deployment-owned handoff that
describes the required origin host, origin path, and the origin-only
query-string attachment value or source reference without exposing that
credential to clients.

**Traces to:** RQ-DEPLOY-009A, DSG-LFD-004C, DSG-LFD-008

### VAL-LFD-003B

Inspect the storage-origin isolation model.

**Pass condition:** the storage account accepts only approved CDN POP IP ranges
and explicitly approved VM or operator access paths, keeps the blob container
private, and does not provide a direct anonymous or credential-forwarding client
blob path.

**Traces to:** RQ-DEPLOY-010, RQ-DEPLOY-024, DSG-LFD-005A

### VAL-LFD-004

Inspect the CDN public-edge configuration surface.

**Pass condition:** the CDN layer exposes one parameterized public hostname over
the hidden blob origin, supports public-path rewrite to blob paths, supports
default caching behavior, and does not place origin credentials on the client
surface.

**Traces to:** RQ-DEPLOY-011, RQ-DEPLOY-012, RQ-DEPLOY-024, DSG-LFD-005

### VAL-LFD-004A

Inspect the cached-content continuity rule.

**Pass condition:** the deployment contract preserves CDN service of already
cached content after origin SAS expiry without using client-visible SAS
distribution as a workaround.

**Traces to:** RQ-DEPLOY-013, RQ-DEPLOY-024, DSG-LFD-005

### VAL-LFD-004B

Inspect the optional CDN custom-domain capability.

**Pass condition:** the deployment package preserves an optional custom-domain
and operator-managed BYOC certificate attachment path without making DNS or
certificate automation mandatory for the MVP baseline.

**Traces to:** RQ-DEPLOY-024A, DSG-LFD-005

### VAL-LFD-005

Inspect the shared network topology.

**Pass condition:** the deployment provisions one VNet with a VM subnet, a
private-endpoint subnet, and distinct NSG policy for the VM and private-endpoint
subnets; when the optional private-endpoint path is enabled, the deployment also
provisions the storage private endpoint and private DNS zone link for
`privatelink.blob.core.windows.net`.

**Traces to:** RQ-DEPLOY-020, RQ-DEPLOY-021, RQ-DEPLOY-022, DSG-LFD-003

### VAL-LFD-005A

Inspect VM network policy for the indexing and embedding runtimes.

**Pass condition:** the indexing VM and embedding VM receive only the approved
minimum ingress or egress surface, including optional SSH or embedding-port
exposure only when enabled by deployment parameters, while retaining required
outbound access and approved storage-origin reachability through the selected
public-endpoint or optional private-endpoint path. Embedding ingress, when
enabled, is scoped to the embedding VM rather than to every NIC on the shared VM
subnet.

**Traces to:** RQ-DEPLOY-014, RQ-DEPLOY-017, RQ-DEPLOY-022, DSG-LFD-006A,
DSG-LFD-007A

### VAL-LFD-006

Inspect the indexing VM runtime contract.

**Pass condition:** the deployment provisions one Ubuntu LTS
`Standard_DS1_v2` VM with managed identity, bootstraps Docker and Docker
Compose, acquires the approved GHCR images, runs the indexing compose stack as
one boot -> index -> shutdown activation, and remains restartable for later
reindex execution.

**Traces to:** RQ-DEPLOY-014, RQ-DEPLOY-015, RQ-DEPLOY-016, DSG-LFD-006

### VAL-LFD-007

Inspect the embedding VM runtime contract.

**Pass condition:** the deployment provisions one Ubuntu LTS `B1s` VM whose
bootstrap installs Docker and Docker Compose, acquires the approved GHCR
embedding-service image, starts the embedding API container, and configures the
service for continuous restartable operation.

**Traces to:** RQ-DEPLOY-017, RQ-DEPLOY-018, DSG-LFD-007

### VAL-LFD-008

Inspect the deployment secret and identity boundary.

**Pass condition:** deployment-owned secrets stay inside Key Vault or a
deployment-owned handoff path, managed identity is enabled where required by
the specification, and public outputs or URLs do not disclose origin
credentials unless a secret output mode was explicitly enabled for operator use.

**Traces to:** RQ-DEPLOY-009, RQ-DEPLOY-014, RQ-DEPLOY-023, RQ-DEPLOY-024,
DSG-LFD-008, DSG-LFD-009

### VAL-LFD-009

Inspect the deployment package against repository invariants.

**Pass condition:** the package preserves indexing and search separation, does
not introduce a new central control plane, and does not redefine local/testing,
indexer, MCP, or archive-sync semantic contracts.

**Traces to:** RQ-DEPLOY-002, RQ-DEPLOY-025, RQ-DEPLOY-026, RQ-DEPLOY-027,
DSG-LFD-011

### VAL-LFD-010

Inspect the deployment topology for future content-type neutrality.

**Pass condition:** the deployment package defines shared hosting, storage, CDN,
and network seams that are not hard-coded only for the current email and
document content families.

**Traces to:** RQ-DEPLOY-028, DSG-LFD-012

### VAL-LFD-011

Inspect the deployment package's production embedding-direction statement.

**Pass condition:** the specification makes the embedding VM explicit as an MVP
deployment artifact while also preserving the unresolved repository-level
production-direction conflict with the existing Azure OpenAI baseline instead of
silently deleting or rewriting that baseline.

**Traces to:** RQ-DEPLOY-019, DSG-LFD-010

## Coverage Notes

- **Covered sources [KNOWN]:**
  - `docs/specs/lexonarchivebuilder-deployment/requirements.md`
  - `docs/specs/lexonarchivebuilder-deployment/design.md`
  - `README.md:20-28`
  - `README.md:44-49`
  - `README.md:72-80`
- **Excluded from this phase [KNOWN]:**
  - implementation code, Bicep files, deployment scripts, and executable tests
