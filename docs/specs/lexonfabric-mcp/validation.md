# LexonFabric MCP Validation

## Status

Approved specification baseline for the MVP implementation scope in
`docs/specs/lexonfabric-mcp/requirements.md` and
`docs/specs/lexonfabric-mcp/design.md`.

## Validation Scope

These validation entries define the expected conformance surface for the
LexonFabric-owned `lexonfabric-mcp` boundary.

This package validates LexonFabric's MCP contract, delegated search and
retrieval wiring, source-name preservation, and environment-specific dependency
selection. It does not redefine validation already owned by LexonGraph for
search semantics or delegated dependency traits.

## Validation Entries

### VAL-LFM-001

Inspect the MCP server surface for `lexonfabric-mcp`.

**Pass condition:** the server exposes chunk-returning search operations and
named retrieval operations for email, thread, and document items.

**Traces to:** RQ-MCP-001, RQ-MCP-003, RQ-MCP-005, DSG-LFM-002

### VAL-LFM-002

Execute a representative search through the MCP surface.

**Pass condition:** the MCP response returns content chunks delegated from the
underlying LexonGraph search flow rather than only top-level item identifiers.

**Traces to:** RQ-MCP-002, RQ-MCP-003, DSG-LFM-001, DSG-LFM-003

### VAL-LFM-003

Execute a representative search whose delegated result includes source-item
names.

**Pass condition:** `lexonfabric-mcp` preserves the delegated source name in
the MCP response for email, thread, or document-backed chunk results when that
metadata is present upstream.

**Traces to:** RQ-MCP-004, DSG-LFM-003

### VAL-LFM-004

Execute named retrieval requests for representative email, thread, and document
items.

**Pass condition:** each operation delegates retrieval for its item class and
returns the requested item when the delegated lookup succeeds. When no
delegated name-based retrieval contract exists for the requested item class in
the first MVP, the operation returns an explicit unsupported or unavailable
outcome rather than a success-shaped fallback result.

**Traces to:** RQ-MCP-005, RQ-MCP-005A, DSG-LFM-004

### VAL-LFM-005

Execute named retrieval requests that do not resolve successfully through the
delegated retrieval flow.

**Pass condition:** `lexonfabric-mcp` surfaces the delegated unsuccessful
lookup outcome, or an explicit unsupported or unavailable outcome when no
delegated name-based retrieval contract exists, rather than returning a
success-shaped response or inventing a repository-local fallback result.

**Traces to:** RQ-MCP-005, RQ-MCP-005A, RQ-MCP-011, DSG-LFM-004, DSG-LFM-009

### VAL-LFM-006

Run the local/testing environment profile.

**Pass condition:** `lexonfabric-mcp` selects local filesystem-backed storage
or block access and the same Docker-containerized local embedding engine
profile used by the indexer when the delegated search flow requires embeddings,
without changing the MCP contract.

**Traces to:** RQ-MCP-006, RQ-MCP-007, DSG-LFM-005, DSG-LFM-006, DSG-LFM-006A,
DSG-LFM-007

### VAL-LFM-006A

Exercise the first-MVP local/testing conformance surface.

**Pass condition:** `lexonfabric-mcp` is testable end to end against a local
filesystem-backed block store and a Docker-containerized local embedding
service aligned with the indexer's local embedding profile, without requiring
an executable Azure-backed production setup.

**Traces to:** RQ-MCP-007A, DSG-LFM-006A, DSG-LFM-011

### VAL-LFM-007

Inspect the preserved production environment profile boundary.

**Pass condition:** production-specific storage and embedding identifiers remain
behind the same adapter-selection boundary as the executable local/testing
profile, and no local-only assumptions leak into the MCP contract or delegated
search orchestration boundary.

**Traces to:** RQ-MCP-006, RQ-MCP-007, DSG-LFM-005, DSG-LFM-006, DSG-LFM-007

### VAL-LFM-008

Inspect the executable local/testing profile against the preserved production
profile boundary for the same logical MCP interactions.

**Pass condition:** the operation families, response categories, and delegated
search ownership model remain the same while only environment-specific adapter
realizations differ, even though only the local/testing profile is executable
in the first MVP.

**Traces to:** RQ-MCP-007, RQ-MCP-012, DSG-LFM-006, DSG-LFM-007

### VAL-LFM-009

Inspect the `lexonfabric-mcp` specification package against indexer artifacts.

**Pass condition:** no MCP artifact in this package redefines indexing
contracts, indexing-time orchestration, or content-resolution behavior owned by
the indexer boundary.

**Traces to:** RQ-MCP-010, DSG-LFM-008

### VAL-LFM-010

Inspect the `lexonfabric-mcp` specification package against delegated
LexonGraph contracts.

**Pass condition:** the package remains subordinate to delegated LexonGraph
search and dependency contracts and does not redefine their search semantics,
ranking semantics, or storage semantics.

**Traces to:** RQ-MCP-002, RQ-MCP-011, DSG-LFM-001, DSG-LFM-009, DSG-LFM-011

### VAL-LFM-011

Add a new content type beyond the initial email, thread, and document surface.

**Pass condition:** the new content type can be introduced by extending
delegated routing and result projection without redefining the core chunk-search
contract or the environment-selection contract.

**Traces to:** RQ-MCP-008, RQ-MCP-012, DSG-LFM-010

### VAL-LFM-012

Exercise representative `lexonfabric-mcp` interactions from both Linux and
Windows client environments.

**Pass condition:** both environments consume the same MCP operation families
and response contracts without requiring OS-specific request semantics or
response interpretation.

**Traces to:** RQ-MCP-009, DSG-LFM-007
