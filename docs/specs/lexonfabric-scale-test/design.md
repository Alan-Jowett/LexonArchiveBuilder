# LexonFabric Scale Test Design

## Status

Phase 2 specification patch for the approved local rsync-driven stress-test
wrapper in `docs/specs/lexonfabric-scale-test/requirements.md`.

## Scope

This document specifies the LexonFabric-owned design for realizing
`lexonfabric-scale-test` as a lightweight local wrapper that fetches mailbox
archives from rsync, discovers `.mail` and `.mbox` mailbox inputs, generates an
indexer-compatible request/config artifact, and delegates block-tree generation
to existing LexonFabric indexing behavior through one shared local workflow
with both direct shell and Docker Compose entrypoints.

This document is layered on top of:

- `docs/specs/lexonfabric-scale-test/requirements.md`
- `docs/specs/lexonfabric-indexer/requirements.md`
- `docs/specs/lexonfabric-indexer/design.md`
- `README.md`

This document does not redefine indexer semantics, parser behavior, block-store
contracts, embedding-provider contracts, or MCP semantics. Those remain owned
by the existing LexonFabric and LexonGraph boundaries.

## Impact Map

### Directly affected artifacts

- `docs/specs/lexonfabric-scale-test/requirements.md`
- `docs/specs/lexonfabric-scale-test/design.md`
- `docs/specs/lexonfabric-scale-test/validation.md`

### Indirectly affected artifacts

- local operator assets such as a bash script and Docker Compose workflow wiring
- example request/output locations reused by the wrapper
- the existing `lexonfabric-indexer` runtime and its local execution profile

### Unaffected artifacts

- `docs/specs/lexonfabric-mcp/*`
- LexonFabric MCP request/response semantics
- LexonFabric indexer parsing, normalization, chunking, and embedding semantics
- production ARM/Bicep and Azure Functions orchestration details

## Design Goals

The `lexonfabric-scale-test` design is intended to be:

- a wrapper over existing LexonFabric indexing behavior
- explicit about ownership boundaries
- lightweight in first realization
- executable in the local/testing profile only
- Windows-friendly through Docker Compose while preserving Linux bash use
- suitable for large-scale parser stress testing
- deterministic enough for repeatable local runs
- extensible to future content-discovery classes

## Boundary Design

### DSG-LST-001 `Delegated stress-test boundary`

`lexonfabric-scale-test` owns local orchestration, mailbox acquisition, mailbox
discovery, generated request/config materialization, and run artifact assembly.

`lexonfabric-scale-test` does not own parser semantics, delegated indexing
semantics, block construction, embedding semantics, or MCP-serving semantics.

**Traces to:** RQ-SCALE-001, RQ-SCALE-004, RQ-SCALE-012, RQ-SCALE-013

### DSG-LST-002 `Minimal operator realization`

The first `lexonfabric-scale-test` realization supports two lightweight local
entrypoints:

- a direct Linux-local operator form such as a bash script
- a Docker Compose user entrypoint suitable for Linux or Windows hosts

The realization remains lightweight rather than becoming a dedicated Rust crate
or long-lived service.

The realization stays intentionally simple so long as it preserves:

- the ordered workflow
- generated request/config artifacts
- delegated execution of the downstream batch runtime
- machine-consumable root handoff output

**Traces to:** RQ-SCALE-003A, RQ-SCALE-003B, RQ-SCALE-003C, RQ-SCALE-009A,
RQ-SCALE-009B, RQ-SCALE-009C

### DSG-LST-002A `Compose user entrypoint`

The Docker Compose entrypoint is a first-class user-facing launch mode for
`lexonfabric-scale-test`, especially for Windows-hosted local development where
host bash is unavailable.

The Compose entrypoint remains subordinate to the same wrapper-owned workflow
boundary and may wrap the Linux execution shape inside containers rather than
requiring Windows-native rsync or bash support on the host.

**Traces to:** RQ-SCALE-003B, RQ-SCALE-003C, RQ-SCALE-003D, RQ-SCALE-009B

### DSG-LST-003 `Ordered workflow pipeline`

The wrapper realizes one run as a staged pipeline:

1. acquire rsync-backed mailbox content
2. discover mailbox files from the fetched mirror set
3. generate an indexer-compatible request/config artifact
4. invoke the existing LexonFabric batch/indexer entrypoint
5. capture and publish root handoff output for the resulting block tree

The wrapper remains batch-oriented and does not introduce a long-lived control
plane. The same staged pipeline is preserved regardless of whether the user
launches it through direct bash or Docker Compose.

**Traces to:** RQ-SCALE-003, RQ-SCALE-003D, RQ-SCALE-004, RQ-SCALE-006,
RQ-SCALE-007

## Input and Artifact Design

### DSG-LST-004 `Rsync acquisition stage`

The wrapper accepts one or more rsync URLs and materializes their fetched
content into a local working area suitable for discovery.

The rsync stage is a source-acquisition concern owned by the wrapper rather
than by `lexonfabric-indexer`.

The first design baseline assumes the rsync stage mirrors mailbox content into
a local directory tree without requiring the indexer to understand rsync URLs
directly.

**Traces to:** RQ-SCALE-002, RQ-SCALE-003, RQ-SCALE-004

### DSG-LST-005 `Mailbox discovery and deterministic request generation`

After rsync acquisition, the wrapper walks the fetched local mirror set,
discovers mailbox files ending in `.mail` or `.mbox`, and translates them into
an indexer-compatible request artifact.

The design keeps discovery and request generation wrapper-owned so downstream
indexer contracts continue to receive ordinary mailbox items rather than a new
rsync-specific input mode.

The first design baseline constrains mailbox discovery compatibility to the
explicit `.mail` and `.mbox` extension allowlist. This keeps the wrapper's
behavior deterministic and aligned with the approved mailbox contract without
requiring broader extension heuristics or content sniffing in this increment.

For repeatable local stress-test runs, the first design baseline expects the
wrapper to generate mailbox items in a deterministic order when the discovered
mailbox set is unchanged. That determinism applies across both user-facing
entrypoints so the same logical run yields the same request shape and artifact
family independent of launch mode.

**Traces to:** RQ-SCALE-003, RQ-SCALE-003D, RQ-SCALE-005, RQ-SCALE-010,
RQ-SCALE-011

### DSG-LST-005A `Combined run output model`

When multiple rsync URLs are supplied for one run, the wrapper merges their
discovered mailbox items into one logical generated request artifact and one
logical block-tree output set for that run.

This model preserves one run-scoped stress-test result rather than one
independent index tree per rsync source in the first increment.

**Traces to:** RQ-SCALE-011

## Downstream Integration Design

### DSG-LST-006 `Delegated indexer invocation`

The wrapper invokes the existing LexonFabric batch/indexer runtime using the
generated request/config artifact rather than calling parser internals
directly.

The generated artifact is compatible with the existing local indexer contract
so the wrapper exercises the same parser and block-generation path as ordinary
local indexing.

This preserves the wrapper as a stress-test harness over existing behavior
instead of creating a second indexing surface.

**Traces to:** RQ-SCALE-004, RQ-SCALE-010, RQ-SCALE-012, RQ-SCALE-013

### DSG-LST-007 `Root handoff artifact`

The wrapper publishes a machine-consumable root handoff artifact that identifies
the resulting block-tree root from the delegated indexing run.

The first design baseline reuses the existing summary/root-style output family
already produced by the downstream local indexing flow when practical rather
than introducing a second root-reporting schema.

**Traces to:** RQ-SCALE-006, RQ-SCALE-007, RQ-SCALE-010

## Environment and Scope Design

### DSG-LST-008 `Local-only execution boundary`

The wrapper's executable conformance surface is limited to local/testing.

Its design remains compatible with the repository's container-oriented local
profile and may use Docker Compose plus a Linux bash script to coordinate local
dependencies and runs.

Host operating system does not redefine the wrapper contract:

- Linux hosts may use either the bash or Docker Compose entrypoint
- Windows hosts use the Docker Compose entrypoint to reach the same Linux-
  oriented execution shape

The design explicitly does not define the production orchestration shape, which
remains a separate ARM/Bicep plus Azure Functions concern.

**Traces to:** RQ-SCALE-003, RQ-SCALE-003B, RQ-SCALE-003C, RQ-SCALE-009,
RQ-SCALE-009A, RQ-SCALE-009B, RQ-SCALE-009C

### DSG-LST-009 `No MCP-serving responsibilities`

`lexonfabric-scale-test` stops at delegated block-tree generation and root
handoff output.

It does not generate MCP config artifacts and does not extend or reinterpret
MCP contracts.

**Traces to:** RQ-SCALE-007, RQ-SCALE-008, RQ-SCALE-012

## Invariant Design

### DSG-LST-010 `Stable contract reuse`

The wrapper composes existing LexonFabric request and output families where
practical rather than inventing a new rsync-specific indexing protocol.

This design keeps the stress-test harness subordinate to existing boundaries
and minimizes the surface area that future changes must keep in sync.

**Traces to:** RQ-SCALE-003D, RQ-SCALE-010, RQ-SCALE-013

### DSG-LST-011 `Future discovery extensibility`

The wrapper keeps its top-level contract centered on source acquisition,
discovery, generated request materialization, delegated execution, and run
artifact publication so future stress-test modes can extend discovery policy
without redefining the wrapper boundary.

The first focus remains rsync-backed mailbox acquisition, with mailbox
discovery compatibility limited to `.mail` and `.mbox` in this increment.
Future document-oriented, broader mailbox-oriented, or other content-oriented
discovery flows may be added behind the same wrapper-owned stages.

**Traces to:** RQ-SCALE-005, RQ-SCALE-014

## Verification Realization

### DSG-LST-012 `Repository verification scope`

LexonFabric-owned verification artifacts validate:

- wrapper-owned rsync acquisition and mailbox discovery behavior
- equivalent workflow semantics across the bash and Docker Compose entrypoints
- generated request/config compatibility with the downstream indexer contract
- delegated execution of the existing parser/indexer path
- production of a machine-consumable root handoff artifact
- local-only execution plus the approved Linux and Docker Compose entrypoints
- absence of MCP-config generation requirements in this increment

LexonFabric-owned verification artifacts do not attempt to revalidate parser,
indexing, block-store, embedding, or MCP semantics already covered by other
repository or upstream boundaries.

**Traces to:** RQ-SCALE-003A, RQ-SCALE-003B, RQ-SCALE-003C, RQ-SCALE-003D,
RQ-SCALE-007, RQ-SCALE-008, RQ-SCALE-012, RQ-SCALE-013
