<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder Block Gateway Requirements

## Document Status

- **Phase:** Phase 8 - Create Deliverable
- **Status:** Approved specification package with implemented Azure Table overlay gateway mode
- **Scope:** `lexonarchivebuilder-block-gateway` retrieval-only boundary for serving immutable block bytes through a low-cost HTTP daemon deployable on a VM, in a container, or as a function, using either the direct LexonGraph Azure Storage Table v2 block-store implementation or an additive overlay composed of in-memory cache, local filesystem cache, and Azure Storage Table v2 backing data

## USER-REQUEST

- **UR-BGW-1 [KNOWN]:** Add a new separate spec package named `lexonarchivebuilder-block-gateway`.
- **UR-BGW-2 [KNOWN]:** The new capability is a low-cost daemon that acts as a front-end to Azure Storage Table.
- **UR-BGW-3 [KNOWN]:** The daemon must expose a REST-like endpoint at `/block/<block_id>`.
- **UR-BGW-4 [KNOWN]:** The daemon must accept a SAS URL as a startup configuration input.
- **UR-BGW-5 [KNOWN]:** The SAS URL should be provided as process configuration at startup rather than per request.
- **UR-BGW-6 [KNOWN]:** Under the covers, the daemon should use the LexonGraph Azure Storage Table v2 block-store API to fetch the block.
- **UR-BGW-7 [KNOWN]:** The daemon must return the fetched block as `application/octet-stream`.
- **UR-BGW-8 [KNOWN]:** The daemon must emit headers denoting that returned block content is cacheable forever.
- **UR-BGW-9 [KNOWN]:** The deployment target should remain open so the same gateway contract can later be hosted on an Azure VM, in a container, or as a function.
- **UR-BGW-10 [KNOWN]:** The user will decide the final hosting form later.
- **UR-BGW-11 [KNOWN]:** The HTTP surface should remain a separate daemon boundary rather than being folded into the existing MCP server surface.
- **UR-BGW-12 [KNOWN]:** Any non-success lookup outcome should return `404`.
- **UR-BGW-13 [INFERRED]:** The gateway should remain retrieval-only and must not redefine indexing behavior, MCP search semantics, or content-type-specific retrieval semantics.
- **UR-BGW-14 [INFERRED]:** Because LexonArchiveBuilder blocks are immutable and hash-addressed, long-lived cache semantics are safe only when the gateway preserves block-identity fidelity rather than rewriting payload bytes.
- **UR-BGW-15 [INFERRED]:** The gateway should stay deployment-neutral and stateless so the same contract can fit VM, container, and function hosting shapes without a separate control-plane service.
- **UR-BGW-16 [INFERRED]:** The request targets the existing production-oriented Azure Table block-store seam already present in the repository rather than introducing a new repository-owned table backend.
- **UR-BGW-17 [KNOWN]:** Update the gateway so it can optionally use an overlay block store instead of only the raw Azure Storage Table block store.
- **UR-BGW-18 [KNOWN]:** The overlay mode should be composed of an in-memory cache, a filesystem cache, and Azure Storage Table v2 as the backing data store.
- **UR-BGW-19 [INFERRED]:** The existing direct Azure Storage Table v2 mode should remain available as an approved gateway backend mode rather than being replaced outright.
- **UR-BGW-20 [INFERRED]:** The external HTTP contract should remain stable regardless of whether the gateway is configured for direct Azure Table access or the additive overlay-backed mode.

## Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-BGW-001 | Add | Introduce a new separate requirements boundary for `lexonarchivebuilder-block-gateway` | UR-BGW-1, UR-BGW-11 |
| CM-BGW-002 | Revise | Define a low-cost retrieval-only HTTP daemon over an approved Azure Table-backed block-store profile set instead of a single direct Azure Table seam | UR-BGW-2, UR-BGW-6, UR-BGW-13, UR-BGW-16, UR-BGW-17, UR-BGW-18, UR-BGW-19 |
| CM-BGW-003 | Add | Define a stable `GET /block/<block_id>` block-fetch endpoint that returns raw block bytes | UR-BGW-3, UR-BGW-7 |
| CM-BGW-004 | Revise | Require startup-time storage-profile configuration rather than per-request storage credentials | UR-BGW-4, UR-BGW-5, UR-BGW-17, UR-BGW-18 |
| CM-BGW-005 | Add | Require immutable-cache response semantics for successful block fetches | UR-BGW-8, UR-BGW-14 |
| CM-BGW-006 | Add | Preserve deployment neutrality across VM, container, and function hosting forms | UR-BGW-9, UR-BGW-10, UR-BGW-15 |
| CM-BGW-007 | Add | Keep the gateway outside the MCP server boundary and avoid search-contract changes | UR-BGW-11, UR-BGW-13 |
| CM-BGW-008 | Add | Normalize unsuccessful block lookups to `404` | UR-BGW-12 |
| CM-BGW-009 | Add | Introduce an additive overlay-backed Azure Table mode composed of memory cache, filesystem cache, and Azure Table v2 backing data | UR-BGW-17, UR-BGW-18 |
| CM-BGW-010 | Add | Preserve the existing direct Azure Table mode as an approved alternative backend profile | UR-BGW-19 |
| CM-BGW-011 | Add | Preserve one unchanged external HTTP contract across the approved direct and overlay-backed gateway storage profiles | UR-BGW-20 |

## Before / After

### BA-BGW-001

- **Before [KNOWN]:** The repository had no separate specification boundary for a block-serving HTTP gateway.
- **After [KNOWN]:** The repository has a proposed requirements baseline for `docs/specs/lexonarchivebuilder-block-gateway/requirements.md`.

### BA-BGW-002

- **Before [KNOWN]:** Production-oriented non-local block access in the repository was specified through shared `BlockStore` seams for indexer- and MCP-owned tooling, but not through a dedicated HTTP block gateway.
- **After [KNOWN]:** The requirements define an additive retrieval-only HTTP gateway layered on the existing Azure Table `BlockStore` seam.

### BA-BGW-003

- **Before [KNOWN]:** The repository described search-serving through MCP and deployment/publication through separate boundaries, but it did not define a direct immutable-block HTTP fetch endpoint.
- **After [KNOWN]:** The requirements define a dedicated `/block/<block_id>` endpoint that returns raw immutable block bytes with long-lived cache semantics.

### BA-BGW-004

- **Before [KNOWN]:** The user-facing hosting choice for a thin block-serving surface was unresolved.
- **After [KNOWN]:** The requirements preserve one deployment-neutral gateway contract that can later be hosted on a VM, in a container, or as a function without changing the HTTP contract.

### BA-BGW-005

- **Before [KNOWN]:** The repository's intended shape emphasized no server-side processing beyond indexing and did not explicitly carve out a dedicated immutable-block retrieval daemon.
- **After [KNOWN]:** The requirements add a scoped retrieval exception: a stateless immutable-block gateway that remains separate from MCP and avoids introducing a central control plane.

### BA-BGW-006

- **Before [KNOWN]:** The gateway requirements constrained the runtime to one direct Azure Storage Table v2 block-store dependency configured from a startup-time SAS URL.
- **After [KNOWN]:** The gateway requirements constrain the runtime to an approved Azure Table-backed profile set with two startup-selected options: the existing direct Azure Storage Table v2 path and an additive overlay composed of in-memory cache, local filesystem cache, and Azure Storage Table v2 backing data.

### BA-BGW-007

- **Before [KNOWN]:** The requirements treated the gateway's backend choice as a single direct Azure Table seam, so cache layering above Azure Table would have been a contract change rather than an approved operator option.
- **After [KNOWN]:** The requirements make the overlay-backed Azure Table mode additive, preserve the existing direct mode, and keep both modes behind the same retrieval-only HTTP contract.

## Requirements

### Functional Requirements

#### RQ-BGW-001 - Block-gateway boundary

LexonArchiveBuilder SHALL provide a separate retrieval-only boundary named `lexonarchivebuilder-block-gateway`.

- **Boundary [KNOWN]:** This boundary owns HTTP block-fetch serving behavior and storage-gateway configuration for immutable block retrieval.
- **Non-goal [KNOWN]:** This boundary does not own indexing, chunking, clustering, MCP search, MCP named retrieval, or content normalization semantics.
- **Traceability:** UR-BGW-1, UR-BGW-11, UR-BGW-13

#### RQ-BGW-002 - Low-cost daemon shape

`lexonarchivebuilder-block-gateway` SHALL be specified as a low-cost, stateless daemon-style HTTP surface rather than as a full control-plane service.

- **Constraint [INFERRED]:** The gateway contract must stay minimal enough to fit a VM-hosted process, a containerized process, or a function-hosted process without changing its external behavior.
- **Traceability:** UR-BGW-2, UR-BGW-9, UR-BGW-10, UR-BGW-15

#### RQ-BGW-003 - Block fetch endpoint

The gateway SHALL expose an HTTP endpoint at `/block/<block_id>` for immutable block retrieval.

- **Constraint [KNOWN]:** The endpoint contract in this increment is retrieval-only.
- **Non-goal [KNOWN]:** This increment does not add write, delete, list, or search operations.
- **Traceability:** UR-BGW-3, UR-BGW-13

#### RQ-BGW-004 - Startup-time storage-profile configuration

The gateway SHALL obtain its Azure Table-backed storage dependency from process configuration at startup.

- **Direct mode [KNOWN]:** the direct Azure Storage Table v2 profile requires a startup-time SAS URL.
- **Overlay mode [KNOWN]:** the additive overlay-backed Azure Table profile requires startup-time configuration for the Azure Storage Table v2 backing store plus the filesystem cache root and in-memory cache capacity.
- **Constraint [KNOWN]:** Callers do not supply storage credentials or cache-selection inputs on each fetch request.
- **Traceability:** UR-BGW-4, UR-BGW-5, UR-BGW-17, UR-BGW-18

#### RQ-BGW-005 - Approved delegated Azure Table block-store profiles

The gateway SHALL resolve requested blocks through the LexonGraph-owned Azure Storage Table v2 block-store API using one approved startup-selected backend profile.

- **Approved profiles [KNOWN]:** direct Azure Storage Table v2, or an additive overlay composed of in-memory cache, local filesystem cache, and Azure Storage Table v2 backing data.
- **Constraint [INFERRED]:** The gateway must reuse the existing delegated `BlockStore` contract family rather than inventing a repository-local table-storage protocol.
- **Constraint [INFERRED]:** The overlay-backed profile is fixed to the repository-approved cache stack rather than an arbitrary caller-assembled storage graph.
- **Traceability:** UR-BGW-6, UR-BGW-16, UR-BGW-17, UR-BGW-18, UR-BGW-19

#### RQ-BGW-005A - Direct Azure Table mode preservation

The existing direct Azure Storage Table v2 gateway mode SHALL remain an approved optional backend profile in this increment.

- **Constraint [INFERRED]:** Adding the overlay-backed mode must not silently remove the current direct Azure Table operating mode.
- **Traceability:** UR-BGW-17, UR-BGW-19

#### RQ-BGW-005B - Overlay-backed Azure Table mode

The additive overlay-backed gateway mode SHALL layer an in-memory cache and a local filesystem cache in front of Azure Storage Table v2 backing data.

- **Constraint [KNOWN]:** The overlay-backed mode uses Azure Storage Table v2 for durable block bytes rather than substituting Azure Blob backing storage.
- **Constraint [INFERRED]:** The cache layers are internal to the delegated storage dependency and do not redefine block identity or payload bytes.
- **Traceability:** UR-BGW-17, UR-BGW-18, UR-BGW-20

#### RQ-BGW-005C - Storage-mode-neutral HTTP contract

The gateway's externally visible HTTP behavior SHALL remain unchanged across the approved direct and overlay-backed Azure Table profiles.

- **Constraint [INFERRED]:** Route shape, successful response payload bytes, content type, cache semantics, and unsuccessful lookup normalization remain governed by the same gateway requirements regardless of selected backend profile.
- **Traceability:** UR-BGW-17, UR-BGW-19, UR-BGW-20

#### RQ-BGW-006 - Raw block-byte response

When a block is found, the gateway SHALL return the stored block bytes as the HTTP response body with content type `application/octet-stream`.

- **Constraint [INFERRED]:** The gateway must preserve block payload bytes exactly as stored.
- **Traceability:** UR-BGW-7, UR-BGW-14

#### RQ-BGW-007 - Immutable-cache response semantics

Successful block-fetch responses SHALL carry headers that denote the returned block content is cacheable as immutable content for effectively permanent reuse.

- **Rationale [INFERRED]:** Immutable hash-addressed block identity makes long-lived caching compatible with correctness when payload bytes are served without reinterpretation.
- **Clarification gap [UNKNOWN]:** The exact header set and TTL expression for "cacheable forever" remain intentionally unconstrained by the approved current specification package.
- **Traceability:** UR-BGW-8, UR-BGW-14

#### RQ-BGW-008 - Unsuccessful lookup behavior

Any non-success block lookup outcome in this increment SHALL return HTTP `404`.

- **Constraint [KNOWN]:** This includes malformed, missing, or otherwise unsuccessful block fetch attempts at the external HTTP contract.
- **Traceability:** UR-BGW-12

#### RQ-BGW-009 - Hosting-form neutrality

The gateway requirements SHALL preserve one hosting-neutral application contract usable from an Azure VM, a container runtime, or a function-hosting shape.

- **Constraint [KNOWN]:** The user may decide the final hosting form later without changing the gateway endpoint or storage-dependency contract.
- **Traceability:** UR-BGW-9, UR-BGW-10, UR-BGW-15

#### RQ-BGW-010 - Separate from MCP search serving

The gateway SHALL remain outside the existing MCP server boundary.

- **Constraint [KNOWN]:** The gateway does not become a new MCP tool surface in this increment.
- **Traceability:** UR-BGW-11, UR-BGW-13

### Boundary and Invariant Requirements

#### RQ-BGW-011 - Indexing and search-semantic non-interference

`lexonarchivebuilder-block-gateway` SHALL NOT redefine indexer behavior, replay behavior, MCP search semantics, or content-type-specific retrieval semantics already owned by existing repository boundaries.

- **Traceability:** UR-BGW-11, UR-BGW-13

#### RQ-BGW-012 - Stable delegated storage boundary

The gateway SHALL stay subordinate to the delegated LexonGraph block-store contracts and SHALL NOT introduce a repository-owned translation layer that changes block identity or payload semantics.

- **Traceability:** UR-BGW-6, UR-BGW-14, UR-BGW-16

#### RQ-BGW-013 - No central control-plane expansion

The gateway SHALL remain a thin stateless retrieval adapter and SHALL NOT require a new repository-owned central control plane.

- **Traceability:** UR-BGW-2, UR-BGW-15

#### RQ-BGW-014 - Future content-type neutrality

The gateway SHALL remain block-oriented and content-type-neutral so future content types can reuse the same immutable block-fetch surface without redefining the endpoint contract.

- **Traceability:** UR-BGW-13, UR-BGW-15

## Out of Scope

- Redefining MCP search or retrieval semantics
- Redefining indexer replay, chunking, clustering, or publication behavior
- Introducing write, delete, list, or search operations on the gateway
- Requiring Azure Front Door, a load balancer, or another higher-cost traffic-management layer in this increment
- Defining a separate repository-owned storage-table protocol or backend outside the delegated LexonGraph Azure Table v2 block-store API
- Introducing a plain Azure Blob-backed gateway mode or another non-Azure-Table durable backing store for this gateway increment
- Finalizing the exact hosting choice among VM, container, or function
- Finalizing the exact cache-header syntax for "cacheable forever"
- Adding a local filesystem or local/testing block-store mode for this gateway in the first increment

## Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | The gateway is retrieval-only and does not redefine indexing or MCP search semantics |
| Existing MCP server contract remains stable | Preserved | The gateway is a separate boundary rather than an MCP-surface change |
| Environment-specific adapters stay behind stable seams | Preserved with revised approved storage-profile set | The gateway now supports either the existing direct Azure Table profile or the additive overlay-backed Azure Table profile while still keeping backend selection behind startup-time configuration |
| Repository avoids a new central control plane | Preserved | The requirements constrain the gateway to a thin stateless daemon rather than a coordinating service |
| Repository shape avoids broad new server-side processing beyond indexing | Revised with scoped exception | This patch adds a dedicated immutable-block retrieval surface, but limits it to simple block serving with no search, orchestration, or mutation semantics |
| Future content-type extensibility remains possible | Preserved | The gateway is block-oriented and does not encode email- or document-specific behavior |

## Open Questions / Discovery Gaps

- **Q-BGW-001 [UNKNOWN]:** Should the HTTP surface require any client-facing authentication or network restriction beyond the backend SAS-configured storage dependency?
- **Q-BGW-002 [UNKNOWN]:** Should the first executable realization require support for HEAD requests, or is GET-only sufficient in the initial contract?
- **Q-BGW-003 [UNKNOWN]:** Should the gateway preserve any repository-owned diagnostics or health endpoint beyond the block-fetch surface, or should the first increment remain endpoint-minimal?
- **Q-BGW-004 [UNKNOWN]:** Should a future local/testing realization emulate the Azure Table dependency for development, or remain production-oriented only?
- **Q-BGW-005 [UNKNOWN]:** Should legacy direct-Azure startup inputs continue to imply the direct profile by default, or should operators be required to select direct versus overlay mode explicitly?
- **Q-BGW-006 [UNKNOWN]:** Should the overlay-backed mode reuse the indexer's existing cache-configuration vocabulary exactly, or may the gateway define a narrower gateway-specific configuration surface over the same underlying profile?

## Coverage Notes

- **Covered sources [KNOWN]:**
  - `docs/specs/lexonarchivebuilder-block-gateway/requirements.md:8-194`
  - `docs/specs/lexonarchivebuilder-block-gateway/design.md:14-188`
  - `docs/specs/lexonarchivebuilder-block-gateway/validation.md:16-108`
  - `docs/specs/lexonarchivebuilder-indexer/requirements.md:166-167`
  - `docs/specs/lexonarchivebuilder-indexer/requirements.md:202-204`
  - `docs/specs/lexonarchivebuilder-indexer/design.md:1065-1068`
  - `crates/lexonarchivebuilder-block-gateway/src/lib.rs:42-58`
  - `crates/lexonarchivebuilder-block-gateway/src/lib.rs:110-159`
  - `crates/lexonarchivebuilder-block-gateway/src/main.rs:20-56`
  - `crates/lexonarchivebuilder-indexer/src/block_store.rs:21-25`
  - `crates/lexonarchivebuilder-indexer/src/block_store.rs:47-83`
  - `crates/lexonarchivebuilder-indexer/src/config.rs:118-136`
  - `crates/lexonarchivebuilder-indexer/src/config.rs:380-429`
  - user request in this session: "update the gateway to optionally add a mode where it use an overaly block store instead of the raw azure storage table block store. Overlay should be memory cache + file system cache + azure table block store v2 (for data)"

- **Sampled claim re-checks [KNOWN]:**
  - `crates/lexonarchivebuilder-block-gateway/src/lib.rs:42-58` and `:110-124` now define `GatewayStorageProfile` and select either the overlay-backed or direct Azure Table profile in `build_store`, confirming the gateway runtime is profile-driven rather than single-profile.
  - `crates/lexonarchivebuilder-indexer/src/block_store.rs:47-83` still shows existing repository prior art for both an overlay-backed store and a direct Azure Table v2 store under one `ConfiguredBlockStore` boundary.
  - `crates/lexonarchivebuilder-indexer/src/config.rs:380-429` still enforces distinct validation rules for overlay-backed versus direct Azure Table-backed production profiles, confirming the repository already distinguishes those profile shapes.

- **Excluded from this phase [KNOWN]:**
  - Rust implementation, host packaging, deployment automation, and tests

## Incremental Requirements Patch: Read-only redb serving

The local-redb gateway profile SHALL open its immutable block store read-only
and SHALL not repair, compact, or persist blocks in the served database.

## Incremental Requirements Patch: Debian Docker Compose gateway runtime

### USER-REQUEST

- **UR-BGW-COMPOSE-1 [KNOWN]:** Provide a Docker Compose deployment for a
  Debian host that runs only
  `ghcr.io/alan-jowett/lexonarchivebuilder-block-gateway:main` and
  `ghcr.io/substratusai/stapi:v2.2.2-3`.
- **UR-BGW-COMPOSE-2 [KNOWN]:** The gateway TLS certificate is available at
  `/etc/letsencrypt/live/ietf-block-proxy.westus2.cloudapp.azure.com/fullchain.pem`.
- **UR-BGW-COMPOSE-3 [KNOWN]:** The corresponding private key is available at
  `/etc/letsencrypt/live/ietf-block-proxy.westus2.cloudapp.azure.com/privkey.pem`.
- **UR-BGW-COMPOSE-4 [INFERRED]:** The Compose realization must preserve the
  published image contract by supplying the Azure Table SAS URL externally at
  runtime rather than embedding it in the Compose file or image.

### Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-BGW-COMPOSE-001 | Add | Define a Debian-hosted Docker Compose runtime containing exactly the published block-gateway and STAPI images | UR-BGW-COMPOSE-1 |
| CM-BGW-COMPOSE-002 | Add | Mount the specified certificate and private key read-only into the gateway container without copying either secret into the repository or image | UR-BGW-COMPOSE-2, UR-BGW-COMPOSE-3 |
| CM-BGW-COMPOSE-003 | Add | Require runtime injection of the Azure Table SAS URL through the gateway's existing environment-variable contract | UR-BGW-COMPOSE-4, RQ-BGW-004, RQ-IMG-004A |
| CM-BGW-COMPOSE-004 | Add | Preserve the gateway's UDP 443 listener and avoid adding application services, reverse proxies, indexers, or MCP servers | UR-BGW-COMPOSE-1, RQ-BGW-002, RQ-BGW-009 |
| CM-BGW-COMPOSE-005 | Add | Keep STAPI limited to the requested model service without introducing a gateway-to-STAPI dependency absent from the gateway contract | UR-BGW-COMPOSE-1, RQ-BGW-011 |

### Requirements

#### RQ-BGW-COMPOSE-001 - Minimal Debian Compose service set

The repository SHALL provide one Docker Compose file suitable for Docker Compose
v2 on a Debian host that declares exactly these application services:

1. a gateway service using
   `ghcr.io/alan-jowett/lexonarchivebuilder-block-gateway:main`
2. an embedding service using `ghcr.io/substratusai/stapi:v2.2.2-3`

- **Constraint [KNOWN]:** The file SHALL not build images locally.
- **Constraint [KNOWN]:** The file SHALL not add an indexer, MCP server,
  reverse proxy, database, or other application service.
- **Traceability:** UR-BGW-COMPOSE-1

#### RQ-BGW-COMPOSE-002 - Gateway TLS material

The gateway service SHALL receive an operator-supplied host certificate and
private key.

- **Constraint [INFERRED]:** Both host paths SHALL be mounted read-only.
- **Constraint [KNOWN]:** The gateway command SHALL refer to the mounted
  container paths through its existing `--certificate` and `--private-key`
  arguments.
- **Constraint [KNOWN]:** The Compose file does not fix the host paths; it
  obtains them through `BLOCK_GATEWAY_CERTIFICATE_PATH` and
  `BLOCK_GATEWAY_PRIVATE_KEY_PATH`.
- **Traceability:** UR-BGW-COMPOSE-2, UR-BGW-COMPOSE-3

#### RQ-BGW-COMPOSE-003 - Gateway public listener

The Compose gateway service SHALL publish the gateway's existing QUIC listener
on host UDP port `443` to container UDP port `443`.

- **Rationale [KNOWN]:** The published gateway image exposes only `443/udp`,
  and the gateway defaults its listener to `0.0.0.0:443`.
- **Traceability:** UR-BGW-COMPOSE-1, RQ-BGW-003, RQ-BGW-009

#### RQ-BGW-COMPOSE-004 - External SAS configuration

The Compose gateway service SHALL supply
`LEXONARCHIVEBUILDER_BLOCK_GATEWAY_SAS_URL` from an operator-provided
runtime environment source.

- **Constraint [KNOWN]:** The SAS URL SHALL not be committed to the Compose
  file, image, or repository.
- **Constraint [INFERRED]:** The Compose file should require the value at
  startup so a missing credential fails configuration rather than creating a
  runnable-looking gateway with no storage access.
- **Traceability:** UR-BGW-COMPOSE-4, RQ-BGW-004, RQ-IMG-004A

#### RQ-BGW-COMPOSE-005 - Direct Azure Table v2 profile

The Compose gateway service SHALL select the existing `production-v2` gateway
storage profile unless a later approved requirement requests an overlay or
local-redb deployment.

- **Rationale [INFERRED]:** The requested runtime supplies the gateway image
  and Azure Table SAS configuration but does not request cache-root,
  cache-capacity, or local-redb configuration.
- **Traceability:** UR-BGW-COMPOSE-1, RQ-BGW-005, RQ-BGW-005A

#### RQ-BGW-COMPOSE-006 - STAPI isolation

The Compose stack SHALL run the requested STAPI image without configuring the
gateway to depend on, invoke, or expose an embedding-specific contract.

- **Constraint [KNOWN]:** The current gateway is a block retrieval service and
  has no embedding-service startup configuration.
- **Traceability:** UR-BGW-COMPOSE-1, RQ-BGW-011

### Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Gateway remains retrieval-only | Preserved | The Compose runtime only configures the existing gateway image and does not add routes or behavior |
| Runtime secrets stay outside images and source | Preserved | The certificate, private key, and SAS URL remain host- or operator-provided runtime inputs |
| MCP and indexing remain separate | Preserved | Neither the MCP server nor an indexer is included in the service set |
| No central control plane is introduced | Preserved | The stack contains only the existing gateway and requested embedding model |
| Future content-type neutrality | Preserved | The Compose runtime configures block serving and does not encode content-family behavior |

## Incremental Requirements Patch: Configurable TLS mount sources

### USER-REQUEST

- **UR-BGW-COMPOSE-5 [KNOWN]:** The Docker Compose file must not hard-code
  certificate or private-key host paths; operators must supply them through
  environment variables, including a Compose `.env` file.

### Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-BGW-COMPOSE-006 | Revise | Replace literal certificate and private-key bind-mount sources with required Compose-interpolated environment variables | UR-BGW-COMPOSE-5 |

### Requirements

#### RQ-BGW-COMPOSE-007 - Configurable TLS mount sources

The gateway certificate and private-key bind-mount source paths SHALL be
provided by required Compose variables named
`BLOCK_GATEWAY_CERTIFICATE_PATH` and `BLOCK_GATEWAY_PRIVATE_KEY_PATH`.

- **Constraint [KNOWN]:** Docker Compose `.env` files are an approved source
  for these variables.
- **Constraint [KNOWN]:** The Compose file SHALL fail configuration when either
  variable is absent.
- **Constraint [KNOWN]:** The container-side mount targets remain fixed and
  read-only so the gateway command is unchanged.
- **Traceability:** UR-BGW-COMPOSE-5, RQ-BGW-COMPOSE-002
