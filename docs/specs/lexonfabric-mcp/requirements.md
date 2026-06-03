# MCP Requirements

## Document Status

- **Phase:** Phase 1 - Requirements Discovery
- **Status:** Approved for specification propagation
- **Scope:** `lexonfabric-mcp` search-serving integration boundary

## USER-REQUEST

- **UR-MCP-1 [KNOWN]:** Add a spec trifecta for `lexonfabric-mcp` under `docs/specs/lexonfabric-mcp/{requirements|design|validation}.md`.
- **UR-MCP-2 [KNOWN]:** `lexonfabric-mcp` is an MCP server that wraps the LexonGraph search APIs.
- **UR-MCP-3 [KNOWN]:** The MCP server must return content chunks from search.
- **UR-MCP-4 [KNOWN]:** The MCP server must expose APIs that return specific emails, threads, or documents by name.
- **UR-MCP-5 [KNOWN]:** Search results should also return the name of the document, email, or thread the chunk came from when that name is available from the delegated search API.
- **UR-MCP-6 [KNOWN]:** All actual searching belongs to the underlying `lexongraph` crate rather than to `lexonfabric-mcp`.
- **UR-MCP-7 [KNOWN]:** `lexonfabric-mcp` provides the appropriate trait plugins or adapters for block storage and similar delegated dependencies, analogous to `lexonfabric-indexer`.
- **UR-MCP-8 [KNOWN]:** The architecture must remain extensible to future content types beyond the initial email and document focus.
- **UR-MCP-9 [KNOWN]:** Local and testing operation use local filesystem-backed content plus local embeddings, while production uses Azure Blob Storage plus Azure OpenAI-backed embeddings.
- **UR-MCP-10 [KNOWN]:** LexonFabric serves search and retrieval through an MCP server and intends that surface to stay consistent across environments.
- **UR-MCP-11 [KNOWN]:** The MCP server is intended to remain usable from both Linux and Windows environments.

## Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-MCP-001 | Add | Introduce the first structured requirements artifact for the `lexonfabric-mcp` boundary | UR-MCP-1 |
| CM-MCP-002 | Add | Define `lexonfabric-mcp` as an MCP adaptation layer over delegated LexonGraph search behavior rather than an in-repo search engine | UR-MCP-2, UR-MCP-6 |
| CM-MCP-003 | Add | Define the required MCP-facing retrieval surface for chunk search and named retrieval of emails, threads, and documents | UR-MCP-3, UR-MCP-4, UR-MCP-5 |
| CM-MCP-004 | Add | Define environment-specific dependency integration for block storage and related delegated search dependencies | UR-MCP-7, UR-MCP-9, UR-MCP-10 |
| CM-MCP-005 | Add | Capture invariants around indexing/search separation, stable contracts, and future content-type extensibility | UR-MCP-6, UR-MCP-8, UR-MCP-10 |

## Before / After

### BA-MCP-001

- **Before [KNOWN]:** The repository had no structured requirements artifact for the `lexonfabric-mcp` boundary.
- **After [KNOWN]:** The repository has an explicit requirements baseline for the MCP search-serving boundary in `docs/specs/lexonfabric-mcp/requirements.md`.

### BA-MCP-002

- **Before [KNOWN]:** `README.md` described LexonFabric as exposing search and retrieval through an MCP server, but it did not define whether `lexonfabric-mcp` owned search execution or wrapped delegated LexonGraph search APIs.
- **After [KNOWN]:** The requirements define `lexonfabric-mcp` as an MCP adaptation layer that delegates search execution to LexonGraph while owning repository-local dependency integrations.

### BA-MCP-003

- **Before [KNOWN]:** The repository described a unified search surface at a high level, but did not capture requirements for chunk-returning search or retrieval of emails, threads, and documents by name.
- **After [KNOWN]:** The requirements define an MCP-facing surface for chunk search plus named retrieval of the initially supported content types.

### BA-MCP-004

- **Before [KNOWN]:** Local-versus-production behavior was documented at the architecture level but not translated into MCP-specific requirements for delegated dependency selection.
- **After [KNOWN]:** The requirements define environment-specific integration boundaries so `lexonfabric-mcp` can consume local/testing and production storage or embedding backends without changing the MCP contract.

## Requirements

### Functional Requirements

#### RQ-MCP-001 - MCP search-serving boundary

LexonFabric SHALL provide an MCP server boundary for `lexonfabric-mcp` that exposes search and retrieval over indexed knowledge.

- **Rationale [KNOWN]:** `README.md` describes LexonFabric as serving search and retrieval through an MCP server.
- **Traceability:** UR-MCP-2, UR-MCP-10

#### RQ-MCP-002 - Delegated search execution

`lexonfabric-mcp` SHALL delegate search execution and result generation to the underlying LexonGraph search APIs.

- **Non-goal [KNOWN]:** `lexonfabric-mcp` does not define or implement repository-local search, ranking, chunking, or retrieval algorithms in this scope.
- **Traceability:** UR-MCP-2, UR-MCP-6

#### RQ-MCP-003 - Chunk-returning search results

`lexonfabric-mcp` SHALL surface content chunks returned by the delegated LexonGraph search APIs through its MCP-facing search operations.

- **Constraint [KNOWN]:** The MCP layer must preserve chunk-oriented search behavior rather than collapsing search output to only top-level document, thread, or email identifiers.
- **Traceability:** UR-MCP-3, UR-MCP-6

#### RQ-MCP-004 - Source-name preservation

When the delegated LexonGraph search result includes the originating source item's name, `lexonfabric-mcp` SHALL preserve and return that name alongside the chunk result.

- **Initial source item classes [KNOWN]:**
  - emails
  - threads
  - documents
- **Constraint [KNOWN]:** This requirement preserves delegated metadata; it does not require `lexonfabric-mcp` to invent a source name that the delegated search API does not provide.
- **Traceability:** UR-MCP-5, UR-MCP-6

#### RQ-MCP-005 - Named retrieval operations

`lexonfabric-mcp` SHALL expose retrieval operations that allow callers to request a specific email, thread, or document by name.

- **Clarification gap [UNKNOWN]:** The canonical meaning of "name" for each item class and the expected behavior when multiple items share that name have not yet been specified.
- **Traceability:** UR-MCP-4

#### RQ-MCP-006 - Delegated dependency integrations

`lexonfabric-mcp` SHALL provide the concrete trait plugins, adapters, or equivalent integrations required for the delegated LexonGraph search APIs to access repository-managed dependencies.

- **Required initial dependency class [KNOWN]:** block storage
- **Extensibility [INFERRED]:** Additional delegated query-time dependencies should be integrated behind the same stable boundary rather than leaking backend-specific details into the MCP contract.
- **Traceability:** UR-MCP-6, UR-MCP-7

#### RQ-MCP-007 - Environment-specific adapter selection

`lexonfabric-mcp` SHALL select its delegated dependency integrations according to environment without changing the MCP-facing search or retrieval contract.

- **Local/testing [KNOWN]:** local filesystem-backed content or block access, local embedding service where the delegated search APIs require embeddings
- **Production [KNOWN]:** Azure Blob Storage-backed content or block access, Azure OpenAI-backed embeddings where the delegated search APIs require embeddings
- **Constraint [INFERRED]:** Environment-specific wiring must stay behind stable interfaces so clients do not need different MCP contracts per environment.
- **Traceability:** UR-MCP-7, UR-MCP-9, UR-MCP-10

#### RQ-MCP-008 - Future content-type extensibility

`lexonfabric-mcp` SHALL keep its search and retrieval boundary extensible so future content types can be added without redefining the core MCP search contract.

- **Initial focus [KNOWN]:** emails and documents, with thread retrieval explicitly required in the initial MCP surface
- **Traceability:** UR-MCP-4, UR-MCP-8

#### RQ-MCP-009 - Cross-platform MCP usability

The `lexonfabric-mcp` search-serving boundary SHALL remain usable from both Linux and Windows environments.

- **Rationale [KNOWN]:** The repository README already states that the MCP server should remain usable from Linux and Windows.
- **Traceability:** UR-MCP-10, UR-MCP-11

### Boundary and Invariant Requirements

#### RQ-MCP-010 - Indexing/search separation

The `lexonfabric-mcp` requirements SHALL remain limited to search-serving orchestration and delegated dependency integrations and SHALL NOT redefine indexing-time behavior.

- **Rationale [KNOWN]:** The repository baseline separates indexing from search serving.
- **Traceability:** UR-MCP-6, UR-MCP-10

#### RQ-MCP-011 - Subordinate external contracts

LexonFabric SHALL remain subordinate to the public contracts owned by the delegated LexonGraph search APIs and the delegated dependency traits they consume, and SHALL NOT redefine their search semantics, result-ranking semantics, or storage-contract semantics within this repository.

- **Rationale [KNOWN]:** The user request explicitly assigns actual searching to the underlying `lexongraph` crate.
- **Traceability:** UR-MCP-2, UR-MCP-6, UR-MCP-7

#### RQ-MCP-012 - Stable abstraction boundary

LexonFabric SHALL keep environment-specific storage, embedding, and other delegated dependency variation behind stable integration boundaries so future content types and backend swaps do not require redefinition of the MCP contract.

- **Traceability:** UR-MCP-7, UR-MCP-8, UR-MCP-9, UR-MCP-10

## Out of Scope

- Defining repository-local search, ranking, chunking, or retrieval algorithms
- Redefining the public contracts owned by LexonGraph search APIs or their delegated dependency traits
- Defining indexing-pipeline behavior already covered by `docs/specs/lexonfabric-indexer/*`
- Finalizing the exact canonical name format or duplicate-name resolution semantics for named retrieval until the user clarifies them
- Finalizing exact deployment workflow details beyond the already documented local/testing and production environment split

## Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | Requirements explicitly constrain `lexonfabric-mcp` to the MCP search-serving boundary and delegated search integrations |
| Actual search semantics remain owned by LexonGraph | Preserved | Requirements define delegation rather than an in-repo search engine |
| Environment-specific storage and embedding behavior stays behind stable interfaces | Preserved | Requirements capture environment selection without changing the MCP contract |
| Architecture remains extensible to future content types | Preserved | Requirements keep the surface centered on stable contracts instead of hard-coding only current item classes |

## Open Questions / Discovery Gaps

- **Q-MCP-001 [UNKNOWN]:** What is the canonical "name" for each retrieval class: email, thread, and document?
- **Q-MCP-002 [UNKNOWN]:** What should `lexonfabric-mcp` do when a caller-provided name matches multiple items of the same class?
- **Q-MCP-003 [UNKNOWN]:** Should named retrieval require exact-match semantics, case-insensitive matching, or delegated matching behavior owned entirely by LexonGraph?
- **Q-MCP-004 [UNKNOWN]:** Beyond block storage, which delegated query-time dependency traits must `lexonfabric-mcp` wire directly in-repo for the initial scope?

## Coverage Notes

- **Covered sources [KNOWN]:**
  - `README.md:7-12`
  - `README.md:20-27`
  - `README.md:42-49`
  - `README.md:51-59`
  - `README.md:61-80`
  - user request in this session
- **Excluded for now [KNOWN]:**
  - Rust implementation file paths, crate manifests, and test artifacts for `lexonfabric-mcp`, because no repository-local crate or implementation files exist yet
  - external LexonGraph crate source for exact search API and trait names, because that source is not vendored in this repository and was not required to state the repository-local requirements boundary
