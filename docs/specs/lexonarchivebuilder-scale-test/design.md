# LexonArchiveBuilder Scale Test Design

## Status

Phase 2 specification patch for the approved local rsync-driven stress-test
wrapper with caller-selectable delegated clustering configuration in
`docs/specs/lexonarchivebuilder-scale-test/requirements.md`.

## Scope

This document specifies the LexonArchiveBuilder-owned design for realizing
`lexonarchivebuilder-scale-test` as a lightweight local wrapper that fetches
mailbox archives from rsync, discovers `.mail` and `.mbox` mailbox inputs,
generates an indexer-compatible request/config artifact, derives one explicit
delegated clustering configuration from caller-selected wrapper inputs when
needed, and delegates block-tree generation to existing
LexonArchiveBuilder indexing behavior through one shared local workflow with both
direct shell and Docker Compose entrypoints.

This document is layered on top of:

- `docs/specs/lexonarchivebuilder-scale-test/requirements.md`
- `docs/specs/lexonarchivebuilder-indexer/requirements.md`
- `docs/specs/lexonarchivebuilder-indexer/design.md`
- `README.md`

This document does not redefine indexer semantics, parser behavior, block-store
contracts, embedding-provider contracts, or MCP semantics. Those remain owned
by the existing LexonArchiveBuilder and LexonGraph boundaries.

## Impact Map

### Directly affected artifacts

- `docs/specs/lexonarchivebuilder-scale-test/requirements.md`
- `docs/specs/lexonarchivebuilder-scale-test/design.md`
- `docs/specs/lexonarchivebuilder-scale-test/validation.md`

### Indirectly affected artifacts

- local operator assets such as a bash script and Docker Compose workflow wiring
- example request/output locations reused by the wrapper
- the existing `lexonarchivebuilder-indexer` runtime and its local execution profile

### Unaffected artifacts

- `docs/specs/lexonarchivebuilder-mcp/*`
- LexonArchiveBuilder MCP request/response semantics
- LexonArchiveBuilder indexer parsing, normalization, chunking, and embedding semantics
- production ARM/Bicep and Azure Functions orchestration details

## Design Goals

The `lexonarchivebuilder-scale-test` design is intended to be:

- a wrapper over existing LexonArchiveBuilder indexing behavior
- explicit about ownership boundaries
- lightweight in first realization
- executable in the local/testing profile only
- Windows-friendly through Docker Compose while preserving Linux bash use
- suitable for large-scale parser stress testing
- deterministic enough for repeatable local runs
- aligned with the delegated indexer clustering contract
- extensible to future content-discovery classes

## Boundary Design

### DSG-LST-001 `Delegated stress-test boundary`

`lexonarchivebuilder-scale-test` owns local orchestration, mailbox acquisition, mailbox
discovery, generated request/config materialization, and run artifact assembly.

`lexonarchivebuilder-scale-test` does not own parser semantics, delegated indexing
semantics, block construction, embedding semantics, or MCP-serving semantics.

**Traces to:** RQ-SCALE-001, RQ-SCALE-004, RQ-SCALE-012, RQ-SCALE-013

### DSG-LST-002 `Minimal operator realization`

The first `lexonarchivebuilder-scale-test` realization supports two lightweight local
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
`lexonarchivebuilder-scale-test`, especially for Windows-hosted local development where
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
4. invoke the existing LexonArchiveBuilder batch/indexer entrypoint with any
   caller-selected delegated clustering configuration
5. capture and publish root handoff output for the resulting block tree

The wrapper remains batch-oriented and does not introduce a long-lived control
plane. The same staged pipeline is preserved regardless of whether the user
launches it through direct bash or Docker Compose.

**Traces to:** RQ-SCALE-003, RQ-SCALE-003D, RQ-SCALE-003E, RQ-SCALE-004,
RQ-SCALE-006, RQ-SCALE-007

## Input and Artifact Design

### DSG-LST-004 `Rsync acquisition stage`

The wrapper accepts one or more rsync URLs and materializes their fetched
content into a local working area suitable for discovery.

The rsync stage is a source-acquisition concern owned by the wrapper rather
than by `lexonarchivebuilder-indexer`.

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

### DSG-LST-005B `Wrapper-owned delegated clustering control surface`

`lexonarchivebuilder-scale-test` exposes one first-class wrapper input family
for delegated clustering configuration rather than a generic opaque downstream-
argument passthrough.

The wrapper-owned delegated clustering control surface is aligned to the
existing delegated indexer clustering surface and is limited to:

- one delegated clustering algorithm selector using the downstream-supported
  algorithm names
- the shared delegated clustering controls supported by the downstream indexer
- the approved algorithm-specific delegated clustering option families

The generated request artifact remains focused on discovered mailbox items,
environment configuration, and other existing indexer-request concerns. This
increment does not define a second wrapper-local clustering schema inside the
generated request artifact.

**Traces to:** RQ-SCALE-003E, RQ-SCALE-003F, RQ-SCALE-004A, RQ-SCALE-010A

## Downstream Integration Design

### DSG-LST-006 `Delegated indexer invocation`

The wrapper invokes the existing LexonArchiveBuilder batch/indexer runtime using the
generated request/config artifact rather than calling parser internals
directly.

The generated artifact is compatible with the existing local indexer contract
so the wrapper exercises the same parser and block-generation path as ordinary
local indexing.

This preserves the wrapper as a stress-test harness over existing behavior
instead of creating a second indexing surface.

**Traces to:** RQ-SCALE-004, RQ-SCALE-004A, RQ-SCALE-010, RQ-SCALE-012,
RQ-SCALE-013

### DSG-LST-006A `Explicit delegated clustering forwarding`

When the caller supplies delegated clustering selections, the wrapper forwards
those selections to the existing LexonArchiveBuilder indexer entrypoint using the
same algorithm names and option meanings already owned by the downstream
indexer contract.

The wrapper does not reinterpret algorithm-specific settings, synthesize a new
wrapper-local clustering policy, or redefine downstream validation rules for
unsupported option combinations. Downstream indexer validation and defaulting
remain authoritative.

For one logical wrapper run, the same effective delegated clustering
configuration is forwarded regardless of whether the user launches through the
direct shell entrypoint or the Docker Compose entrypoint.

**Traces to:** RQ-SCALE-003D, RQ-SCALE-003E, RQ-SCALE-003F, RQ-SCALE-004A,
RQ-SCALE-010A

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

`lexonarchivebuilder-scale-test` stops at delegated block-tree generation and root
handoff output.

It does not generate MCP config artifacts and does not extend or reinterpret
MCP contracts.

**Traces to:** RQ-SCALE-007, RQ-SCALE-008, RQ-SCALE-012

## Invariant Design

### DSG-LST-010 `Stable contract reuse`

The wrapper composes existing LexonArchiveBuilder request and output families where
practical rather than inventing a new rsync-specific indexing protocol.

This design keeps the stress-test harness subordinate to existing boundaries
and minimizes the surface area that future changes must keep in sync.

**Traces to:** RQ-SCALE-003D, RQ-SCALE-010, RQ-SCALE-013

### DSG-LST-010A `No wrapper-local clustering protocol`

The wrapper reuses the delegated indexer's clustering-control vocabulary and
effective defaulting behavior rather than introducing a scale-test-specific
clustering protocol or a generic extra-argument escape hatch.

This keeps the wrapper subordinate to the delegated indexer contract and
preserves reproducible stress-test behavior across the approved local
entrypoints.

**Traces to:** RQ-SCALE-003E, RQ-SCALE-003F, RQ-SCALE-004A, RQ-SCALE-010A,
RQ-SCALE-013

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

LexonArchiveBuilder-owned verification artifacts validate:

- wrapper-owned rsync acquisition and mailbox discovery behavior
- equivalent workflow semantics across the bash and Docker Compose entrypoints
- generated request/config compatibility with the downstream indexer contract
- caller-selectable delegated clustering controls and their forwarding contract
- delegated execution of the existing parser/indexer path
- production of a machine-consumable root handoff artifact
- local-only execution plus the approved Linux and Docker Compose entrypoints
- absence of MCP-config generation requirements in this increment

LexonArchiveBuilder-owned verification artifacts do not attempt to revalidate parser,
indexing, block-store, embedding, or MCP semantics already covered by other
repository or upstream boundaries.

**Traces to:** RQ-SCALE-003A, RQ-SCALE-003B, RQ-SCALE-003C, RQ-SCALE-003D,
RQ-SCALE-003E, RQ-SCALE-003F, RQ-SCALE-007, RQ-SCALE-008, RQ-SCALE-012,
RQ-SCALE-013
