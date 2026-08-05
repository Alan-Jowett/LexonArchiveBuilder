<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder MCP Validation

## Status

Approved specification baseline for the MVP implementation scope in
`docs/specs/lexonarchivebuilder-mcp/requirements.md` and
`docs/specs/lexonarchivebuilder-mcp/design.md`.

## Validation Scope

These validation entries define the expected conformance surface for the
LexonArchiveBuilder-owned `lexonarchivebuilder-mcp` boundary.

This package validates LexonArchiveBuilder's MCP contract, delegated search and
retrieval wiring, source-name preservation, and environment-specific dependency
selection. It does not redefine validation already owned by LexonGraph for
search semantics or delegated dependency traits.

## Validation Entries

### VAL-LFM-001

Inspect the MCP server surface for `lexonarchivebuilder-mcp`.

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

**Pass condition:** `lexonarchivebuilder-mcp` preserves the delegated source name in
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

**Pass condition:** `lexonarchivebuilder-mcp` surfaces the delegated unsuccessful
lookup outcome, or an explicit unsupported or unavailable outcome when no
delegated name-based retrieval contract exists, rather than returning a
success-shaped response or inventing a repository-local fallback result.

**Traces to:** RQ-MCP-005, RQ-MCP-005A, RQ-MCP-011, DSG-LFM-004, DSG-LFM-009

### VAL-LFM-006

Run the local/testing environment profile.

**Pass condition:** `lexonarchivebuilder-mcp` selects local filesystem-backed storage
or block access and the same Docker-containerized local embedding engine
profile used by the indexer when the delegated search flow requires embeddings,
without changing the MCP contract.

**Traces to:** RQ-MCP-006, RQ-MCP-007, DSG-LFM-005, DSG-LFM-006, DSG-LFM-006A,
DSG-LFM-007

### VAL-LFM-006A

Exercise the first-MVP local/testing conformance surface.

**Pass condition:** `lexonarchivebuilder-mcp` is testable end to end against a local
filesystem-backed block store and a Docker-containerized local embedding
service aligned with the indexer's local embedding profile, without requiring
an executable Azure-backed production setup.

**Traces to:** RQ-MCP-007A, DSG-LFM-006A, DSG-LFM-011

### VAL-LFM-007

Inspect the preserved production environment profile boundary.

**Pass condition:** production-specific storage and embedding identifiers remain
behind the same adapter-selection boundary as the executable local/testing
profile, and no local-only assumptions leak into the MCP contract or delegated
search orchestration boundary. The preserved shared storage family must
describe the approved overlay target of memory cache plus local filesystem
cache plus Azure Blob SAS-backed access, while also remaining consistent with
the preserved `local-overlay` and `production-v2` configuration shapes exposed
through the shared indexer environment model rather than inventing a plain
Azure Blob-only mode.

**Traces to:** RQ-MCP-006, RQ-MCP-007, DSG-LFM-005, DSG-LFM-006, DSG-LFM-007

### VAL-LFM-007A

Inspect the MCP tool surface against the approved non-local storage-targeting
contract.

**Pass condition:** `search_chunks` and the named retrieval tools share one
approved storage-targeting family: direct local filesystem access, the
preserved `local-overlay` testing shape, the fixed overlay of memory cache
plus local filesystem cache plus Azure Blob SAS-backed access, and preserved
`production-v2` compatibility where the shared configuration family already
exposes it. No MCP tool introduces a plain Azure Blob-only block-store targeting

mode outside that approved family.

**Traces to:** RQ-MCP-006, RQ-MCP-007, DSG-LFM-006, DSG-LFM-007, DSG-LFM-011

### VAL-LFM-008

Inspect the executable local/testing profile against the preserved production
profile boundary for the same logical MCP interactions.

**Pass condition:** the operation families, response categories, and delegated
search ownership model remain the same while only environment-specific adapter
realizations differ, even though only the local/testing profile is executable
in the first MVP.

**Traces to:** RQ-MCP-007, RQ-MCP-012, DSG-LFM-006, DSG-LFM-007

### VAL-LFM-009

Inspect the `lexonarchivebuilder-mcp` specification package against indexer artifacts.

**Pass condition:** no MCP artifact in this package redefines indexing
contracts, indexing-time orchestration, or content-resolution behavior owned by
the indexer boundary.

**Traces to:** RQ-MCP-010, DSG-LFM-008

### VAL-LFM-010

Inspect the `lexonarchivebuilder-mcp` specification package against delegated
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

Exercise representative `lexonarchivebuilder-mcp` interactions from both Linux and
Windows client environments.

**Pass condition:** both environments consume the same MCP operation families
and response contracts without requiring OS-specific request semantics or
response interpretation.

**Traces to:** RQ-MCP-009, DSG-LFM-007

## Incremental Validation Patch: Read-only redb access

Run local-redb MCP search against a populated store.

**Pass condition:** indexed content is returned through the existing MCP
response shape while the runtime uses read-only redb access.

## Incremental Validation Patch: Format-neutral rooted MCP targets

### VAL-MCP-FORMAT-001

Inspect `search_chunks` target preparation.

**Pass condition:** the runtime loads the root and supplies the provider's
logical vector to `lexongraph_search::prepare_target_embedding`; no MCP-owned
root encoding-name branch, descriptor parser, or physical target codec exists.

**Traces to:** RQ-MCP-013, DSG-MCP-FORMAT-001

### VAL-MCP-FORMAT-002

Run MCP `search_chunks` against a rooted fixture whose physical root format is
resolved by the LexonGraph target-preparation API.

**Pass condition:** search returns the existing chunk result shape without an
MCP `UnsupportedEncoding` failure. An upstream target-preparation failure is
returned as a runtime error without fallback.

**Traces to:** RQ-MCP-013, DSG-MCP-FORMAT-001, DSG-MCP-FORMAT-002

### VAL-MCP-FORMAT-003

Run `search_chunks` with an index root that resolves to a leaf block.

**Pass condition:** the operation returns an explicit runtime error and does
not use a leaf-specific local encoding or search fallback.

**Traces to:** RQ-MCP-013, DSG-MCP-FORMAT-002

## Incremental Validation Patch: Gateway-backed MCP search

### VAL-MCP-GATEWAY-001

Parse the gateway MCP configuration example.

**Pass condition:** it selects the MCP-only `gateway-http3` environment,
preserves the configured gateway authority and root ID, and accepts the
documented optional provider defaults.

**Traces to:** RQ-MCP-014, RQ-MCP-015, DSG-MCP-GATEWAY-001,
DSG-MCP-GATEWAY-003

### VAL-MCP-GATEWAY-002

Parse gateway configuration with each disallowed independent embedding field:
`base_url`, an embedding endpoint, and an embedding API-key environment
variable.

**Pass condition:** each configuration fails validation or deserialization;
no field is silently ignored and no alternate provider is constructed.

**Traces to:** RQ-MCP-014, DSG-MCP-GATEWAY-001

### VAL-MCP-GATEWAY-003

Exercise `search_chunks` using a gateway environment with a captured or
test-double HTTP/3 gateway transport.

**Pass condition:** root and descendant reads use the configured gateway
authority; the query embedding request is a `POST /v1/embeddings` to the same
authority; the provider uses its configured model and retry settings; and the
existing result projection is unchanged.

**Traces to:** RQ-MCP-014, RQ-MCP-013, DSG-MCP-GATEWAY-002

### VAL-MCP-GATEWAY-004

Run the stdio MCP server through an MCP client using an operator-supplied
gateway authority and root ID
`adbc431aed97ab541ce73d65ce735552821c6c31f9434a4864333013a278fa78`.

**Pass condition:** `search_chunks` returns the existing chunk-result schema
without requiring a local block store, local summary file, separate embedding
endpoint, or local embedding service. Gateway connectivity or service errors
are returned as MCP operation failures rather than triggering a local fallback.
The server, template, and documentation do not embed the deployed gateway
authority.

**Traces to:** RQ-MCP-014, RQ-MCP-015, DSG-MCP-GATEWAY-002,
DSG-MCP-GATEWAY-003

## Incremental Validation Patch: Leaf-addressed email retrieval

### VAL-MCP-EMAIL-001

Invoke `get_email` with the `leaf_block_id` returned by a representative
`search_chunks` result whose sole leaf entry is an email.

**Pass condition:** the response is an `EmailRetrievalResponse` containing the
requested leaf ID and its sole email entry. The entry preserves the existing
search chunk projection fields.

**Traces to:** RQ-MCP-016, DSG-MCP-EMAIL-001, DSG-MCP-EMAIL-002

### VAL-MCP-EMAIL-002

Invoke `get_email` with malformed and missing block IDs, a branch block ID,
and a leaf whose sole entry is not an email.

**Pass condition:** each case returns the documented MCP error path and never
an `unsupported` response, a successful empty entry list, or a lookup outside
the requested block.

**Traces to:** RQ-MCP-016, DSG-MCP-EMAIL-001, DSG-MCP-EMAIL-002

### VAL-MCP-EMAIL-003

Invoke `get_email` against local and gateway HTTP/3 MCP configurations.

**Pass condition:** both paths use their selected read-only block-store
integration, return the same response shape for the same leaf content, and
make no embedding request.

**Traces to:** RQ-MCP-016, DSG-MCP-EMAIL-003

## Incremental Validation Patch: Corpus-specific MCP tool descriptions

### VAL-MCP-TOOL-DESCRIPTIONS-001

Parse an MCP configuration with an override for `search_chunks` and no
overrides for the other tools.

**Pass condition:** the `search_chunks` resolver returns the configured text;
each other resolver returns its documented default description.

**Traces to:** RQ-MCP-017, DSG-MCP-TOOL-DESCRIPTIONS-001,
DSG-MCP-TOOL-DESCRIPTIONS-002

### VAL-MCP-TOOL-DESCRIPTIONS-002

Parse MCP configurations with an unknown `tool_descriptions` field and with
whitespace-only description values.

**Pass condition:** each configuration fails parsing or validation; no invalid
description is advertised to MCP clients.

**Traces to:** RQ-MCP-017, DSG-MCP-TOOL-DESCRIPTIONS-001

### VAL-MCP-TOOL-DESCRIPTIONS-003

Initialize the MCP server using a configuration with a custom
`search_chunks` description and inspect `tools/list`.

**Pass condition:** the advertised `search_chunks` description is the exact
configured override, while tool names, schemas, other default descriptions,
and server-level instructions retain their existing behavior.

**Traces to:** RQ-MCP-017, DSG-MCP-TOOL-DESCRIPTIONS-002
