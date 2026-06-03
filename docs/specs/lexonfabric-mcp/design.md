# LexonFabric MCP Design

## Status

Approved specification baseline for the MVP implementation scope in
`docs/specs/lexonfabric-mcp/requirements.md`.

## Scope

This document specifies the LexonFabric-owned design for realizing the approved
`lexonfabric-mcp` requirements.

This document is layered on top of:

- `docs/specs/lexonfabric-mcp/requirements.md`
- `README.md`
- the user request in this session

This document does not redefine delegated LexonGraph search semantics, result
ranking, chunk generation, or delegated dependency contracts. Those remain
owned by LexonGraph and its subordinate crates or APIs.

## Impact Map

### Directly affected artifacts

- `docs/specs/lexonfabric-mcp/requirements.md`
- `docs/specs/lexonfabric-mcp/design.md`
- `docs/specs/lexonfabric-mcp/validation.md`

### Indirectly affected artifacts

- `README.md`, which already describes LexonFabric as an MCP server over a
  shared local-versus-production architecture
- future Rust crates, configuration, and test artifacts for `lexonfabric-mcp`
  that are not yet present in this repository

### Unaffected artifacts

- `docs/specs/lexonfabric-indexer/*`
- LexonGraph indexing internals
- LexonGraph search internals
- deployment workflow details beyond the existing local/testing and production
  split

## Design Goals

The LexonFabric MCP design is intended to be:

- an MCP adaptation layer over delegated LexonGraph search behavior
- explicit about ownership boundaries
- stable across local and production environments
- minimal and fully executable in the local/testing profile first
- extensible to future content types
- consistent about preserving source-name metadata when delegated search
  results provide it

## Boundary Design

### DSG-LFM-001 `Delegated search boundary`

LexonFabric owns MCP-facing request and response adaptation, environment-
specific dependency selection, and repository-local wiring to delegated
LexonGraph search APIs.

LexonFabric does not own query interpretation, search ranking, chunk
generation, or canonical retrieval semantics internal to the delegated
LexonGraph stack.

**Traces to:** RQ-MCP-001, RQ-MCP-002, RQ-MCP-010, RQ-MCP-011

### DSG-LFM-002 `MCP operation families`

`lexonfabric-mcp` exposes two operation families at the MCP boundary:

- chunk-returning search operations
- named retrieval operations for email, thread, and document items

The operation families stay content-oriented rather than backend-oriented so
local/testing and production deployments preserve one stable MCP contract.

**Traces to:** RQ-MCP-001, RQ-MCP-003, RQ-MCP-005, RQ-MCP-007

### DSG-LFM-003 `Search result projection`

LexonFabric projects delegated LexonGraph search results into MCP responses
without collapsing chunk-oriented output to only top-level item identifiers.

When the delegated result includes the originating source item's name,
LexonFabric preserves that name in the MCP response instead of dropping it or
reconstructing a different repository-local name.

**Traces to:** RQ-MCP-003, RQ-MCP-004

### DSG-LFM-004 `Named retrieval projection`

LexonFabric exposes retrieval operations for the initially required item
classes of email, thread, and document and forwards the caller-supplied name
selector to the delegated retrieval flow when that delegated contract exists.

The MCP layer preserves class-specific retrieval boundaries and surfaces
delegated unsuccessful lookup outcomes rather than inventing repository-local
fallback behavior.

When the delegated LexonGraph contract does not provide name-based retrieval
for the requested item class, the first MVP returns an explicit unsupported or
unavailable outcome and does not implement repository-local metadata scanning
as a substitute retrieval engine.

**Traces to:** RQ-MCP-005, RQ-MCP-005A, RQ-MCP-011

## Adapter Design

### DSG-LFM-005 `Delegated dependency adapter boundary`

LexonFabric provides the concrete trait plugins, adapters, or equivalent
integrations needed by the delegated LexonGraph search APIs to reach
repository-managed dependencies.

- the initial required dependency class is block storage
- the first MVP must make the local/testing dependency path executable against
  filesystem-backed block access
- additional delegated query-time dependencies, if required, are integrated
  behind the same boundary instead of leaking backend-specific details into MCP
  request or response contracts

**Traces to:** RQ-MCP-006, RQ-MCP-007A, RQ-MCP-012

### DSG-LFM-006 `Environment profile selection`

LexonFabric selects delegated dependency integrations as an environment profile:

| Profile | Storage / block access | Query-time embeddings when required by delegated search |
|---|---|---|
| local/testing | local filesystem-backed access | local embedding service using the same Docker-containerized embedding engine profile as the indexer |
| production | Azure Blob Storage-backed access | Azure OpenAI |

This selection is configuration-driven and preserves one delegated search flow
independent of environment.

For the first MVP, only the local/testing profile must be executable end to
end. The production profile remains a preserved adapter and configuration
boundary rather than an executable runtime path in this increment.

**Traces to:** RQ-MCP-006, RQ-MCP-007, RQ-MCP-007A, RQ-MCP-012

### DSG-LFM-006A `Local MVP conformance surface`

The first `lexonfabric-mcp` MVP fixes its executable conformance surface to the
local/testing profile with:

- a local filesystem-backed block-store access path
- a Docker-containerized local embedding service aligned with the indexer's
  local embedding engine profile

This constraint fixes the first executable environment slice without changing
the MCP operation families, response shape, or delegated search ownership
model.

**Traces to:** RQ-MCP-006, RQ-MCP-007, RQ-MCP-007A

### DSG-LFM-007 `Local and production parity boundary`

Local/testing and production environments differ only in adapter realization
and provider configuration, not in the MCP operation families, chunk-oriented
response shape, or delegated search ownership model.

The MCP boundary remains OS-agnostic at the contract level so Linux and
Windows clients consume the same search and retrieval surface regardless of the
host operating system.

The MVP realizes this parity boundary by keeping the MCP contract and adapter
selection model environment-neutral even though only the local/testing profile
is required to execute in the first increment.

**Traces to:** RQ-MCP-007, RQ-MCP-009, RQ-MCP-012

## Invariant Design

### DSG-LFM-008 `Indexing separation`

The `lexonfabric-mcp` specification package remains separate from indexer
artifacts. No design element in this package changes indexing contracts,
content-resolution behavior, or batch indexing orchestration.

**Traces to:** RQ-MCP-010

### DSG-LFM-009 `Delegated contract subordination`

The design stays subordinate to delegated LexonGraph search and dependency
contracts. The MCP layer adapts them into repository-owned operations but does
not redefine query semantics, result-ranking semantics, or backend-specific
storage rules.

This subordination also applies to named retrieval: the MVP may expose the
operation surface, but it does not invent repository-local retrieval semantics
when the delegated contract is absent.

**Traces to:** RQ-MCP-002, RQ-MCP-005A, RQ-MCP-011

### DSG-LFM-010 `Future content extensibility`

Future content types are added by extending content-type routing and result
projection behind the existing MCP boundary rather than redefining the core
chunk-search contract or the environment-selection contract.

**Traces to:** RQ-MCP-008, RQ-MCP-012

## Verification Realization

### DSG-LFM-011 `Repository verification scope`

LexonFabric-owned verification artifacts validate:

- correct delegation from MCP operations to LexonGraph search and retrieval
- preservation of chunk-oriented output and source-name metadata
- correct selection and use of environment-specific dependency integrations
- executable local/testing conformance against filesystem-backed block access
  and the indexer-aligned Docker-containerized embedding profile
- explicit unsupported or unavailable named-retrieval outcomes when no
  delegated name-based retrieval contract exists for the requested item class
- preservation of one stable MCP contract across environments

LexonFabric-owned verification artifacts do not attempt to revalidate
LexonGraph's own search semantics or dependency-trait contracts beyond proving
that LexonFabric consumes them correctly.

**Traces to:** RQ-MCP-005A, RQ-MCP-007A, RQ-MCP-011, RQ-MCP-012
