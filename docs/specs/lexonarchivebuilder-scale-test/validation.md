# LexonArchiveBuilder Scale Test Validation

## Status

Phase 2 validation patch for the approved local rsync-driven stress-test
wrapper and caller-selectable delegated clustering mode and configuration in
`docs/specs/lexonarchivebuilder-scale-test/requirements.md` and
`docs/specs/lexonarchivebuilder-scale-test/design.md`.

## Validation Scope

These validation entries define the expected conformance surface for the
LexonArchiveBuilder-owned `lexonarchivebuilder-scale-test` boundary.

This package validates wrapper-owned orchestration, generated request
compatibility, delegated clustering-control forwarding, delegated execution,
and root handoff output. It does not redefine validation already owned by
`lexonarchivebuilder-indexer`, LexonGraph, or `lexonarchivebuilder-mcp`.

## Validation Entries

### VAL-LST-001

Inspect the repository surface for `lexonarchivebuilder-scale-test`.

**Pass condition:** the tool is specified as a separate wrapper/test boundary
above the existing indexer flow rather than as part of `lexonarchivebuilder-indexer`.

**Traces to:** RQ-SCALE-001, RQ-SCALE-012, DSG-LST-001

### VAL-LST-002

Inspect the first executable realization shape for `lexonarchivebuilder-scale-test`.

**Pass condition:** the tool is realizable as a lightweight Linux-local
operator form such as a bash script and does not require a long-lived service
or dedicated Rust crate in the first increment.

**Traces to:** RQ-SCALE-003A, RQ-SCALE-009A, DSG-LST-002, DSG-LST-008

### VAL-LST-002A

Inspect the Docker Compose entrypoint for `lexonarchivebuilder-scale-test`.

**Pass condition:** the tool exposes a supported Docker Compose user entrypoint
for the approved local workflow so Windows-hosted local development does not
depend on host bash availability.

**Traces to:** RQ-SCALE-003B, RQ-SCALE-003C, RQ-SCALE-009B, DSG-LST-002,
DSG-LST-002A, DSG-LST-008

### VAL-LST-002B

Inspect the supported bash and Docker Compose entrypoints against the wrapper
contract.

**Pass condition:** both entrypoints preserve the same ordered workflow
semantics, output artifact family, and downstream indexer contract rather than
creating divergent `lexonarchivebuilder-scale-test` behaviors.

**Traces to:** RQ-SCALE-003C, RQ-SCALE-003D, RQ-SCALE-010, DSG-LST-003,
DSG-LST-005, DSG-LST-010

### VAL-LST-002C

Inspect the caller-facing delegated clustering control surface for
`lexonarchivebuilder-scale-test`.

**Pass condition:** the wrapper exposes one explicit delegated clustering-mode
selector with aggregation as the default and divisive as an explicit opt-in,
one explicit delegated clustering-algorithm selector, plus the approved shared
and algorithm-specific delegated clustering option families, aligned to the
downstream indexer contract, and does not rely on a generic opaque extra-
argument passthrough surface.

**Traces to:** RQ-SCALE-003E, RQ-SCALE-003F, DSG-LST-005B, DSG-LST-010A

### VAL-LST-002D

Inspect the delegated clustering control surface across the supported bash and
Docker Compose entrypoints.

**Pass condition:** both entrypoints preserve the same delegated clustering-mode
selector, delegated clustering-algorithm selector, and delegated option family,
with the same downstream meaning for one logical run, rather than introducing
entrypoint-specific clustering behavior.

**Traces to:** RQ-SCALE-003D, RQ-SCALE-003E, RQ-SCALE-003F, RQ-SCALE-010A,
DSG-LST-005B, DSG-LST-006A, DSG-LST-010A

### VAL-LST-003

Execute a representative local run with one rsync URL.

**Pass condition:** the wrapper fetches mailbox content from the rsync source,
discovers mailbox files from the fetched mirror, generates an indexer-compatible
request/config artifact, invokes the downstream parser/indexer flow, and
produces root handoff output in the approved stage order.

**Traces to:** RQ-SCALE-002, RQ-SCALE-003, RQ-SCALE-004, RQ-SCALE-005,
RQ-SCALE-006, RQ-SCALE-007, DSG-LST-003, DSG-LST-004, DSG-LST-005,
DSG-LST-006, DSG-LST-007

### VAL-LST-003A

Execute a representative local run with one rsync URL through the Docker
Compose entrypoint.

**Pass condition:** the Compose-launched run exercises the same rsync ->
discovery -> generated request/config -> delegated parser/indexer -> root
handoff stages without requiring host bash on Windows.

**Traces to:** RQ-SCALE-003B, RQ-SCALE-003D, RQ-SCALE-009B, DSG-LST-002A,
DSG-LST-003, DSG-LST-008

### VAL-LST-003B

Inspect mailbox discovery against representative fetched mirror content that
contains mailbox files ending in `.mail` and `.mbox`.

**Pass condition:** the wrapper treats both `.mail` and `.mbox` files as
eligible mailbox discoveries for generated request materialization and does not
require broader mailbox extension support in this increment.

**Traces to:** RQ-SCALE-005, DSG-LST-005, DSG-LST-011

### VAL-LST-003C

Execute a representative local run with explicit delegated clustering
selection.

**Pass condition:** the wrapper accepts the selected delegated clustering
algorithm and supported delegated clustering options while preserving the
approved rsync -> discovery -> generated request/config -> delegated
parser/indexer -> root handoff stage order.

**Traces to:** RQ-SCALE-003E, RQ-SCALE-003F, RQ-SCALE-004A, DSG-LST-003,
DSG-LST-005B, DSG-LST-006A

### VAL-LST-004

Inspect the generated request/config artifact produced from discovered mailbox
files.

**Pass condition:** the artifact is compatible with the existing local indexer
contract and does not require the downstream indexer to understand rsync URLs
directly.

**Traces to:** RQ-SCALE-004, RQ-SCALE-005, RQ-SCALE-010, DSG-LST-005,
DSG-LST-006, DSG-LST-010

### VAL-LST-004A

Inspect wrapper-owned request materialization and delegated invocation when
delegated clustering inputs are supplied.

**Pass condition:** the generated request artifact remains compatible with the
existing local indexer request contract, while the selected delegated
clustering configuration is forwarded through the downstream invocation rather
than being serialized into a wrapper-local clustering protocol.

**Traces to:** RQ-SCALE-003F, RQ-SCALE-004A, RQ-SCALE-010A, DSG-LST-005B,
DSG-LST-006A, DSG-LST-010A

### VAL-LST-005

Execute a representative local run with multiple rsync URLs.

**Pass condition:** the wrapper combines the discovered mailbox set into one
logical generated request artifact and one logical run output set rather than
producing disconnected per-source result contracts in the first increment.

**Traces to:** RQ-SCALE-011, DSG-LST-005A

### VAL-LST-006

Inspect the root handoff artifact from a completed run.

**Pass condition:** the wrapper emits a machine-consumable artifact that
identifies the produced block-tree root and remains compatible with the
existing summary/root-style output family when practical.

**Traces to:** RQ-SCALE-006, RQ-SCALE-007, RQ-SCALE-010, DSG-LST-007

### VAL-LST-007

Inspect the wrapper scope against MCP artifacts.

**Pass condition:** `lexonarchivebuilder-scale-test` does not generate MCP config
artifacts and does not redefine MCP-serving semantics.

**Traces to:** RQ-SCALE-008, RQ-SCALE-012, DSG-LST-009

### VAL-LST-008

Inspect the wrapper scope against indexer semantics.

**Pass condition:** the wrapper delegates parsing and block-tree generation
through the existing downstream batch/indexer path rather than introducing a
repository-local indexing or parser contract.

**Traces to:** RQ-SCALE-004, RQ-SCALE-012, RQ-SCALE-013, DSG-LST-001,
DSG-LST-006, DSG-LST-010

### VAL-LST-009

Inspect the executable scope against the production boundary.

**Pass condition:** the wrapper is executable for local/testing only, remains
compatible with the repository's container-oriented local profile, remains
usable from Windows-hosted local development through Docker Compose, and does
not attempt to specify the production ARM/Bicep plus Azure Functions workflow.

**Traces to:** RQ-SCALE-009, RQ-SCALE-009A, RQ-SCALE-009B, RQ-SCALE-009C,
DSG-LST-008

### VAL-LST-010

Add a future discovery mode beyond rsync-backed mailbox acquisition.

**Pass condition:** the new mode can be introduced by extending source
acquisition or discovery policy behind the existing wrapper stages without
redefining the top-level wrapper boundary or downstream indexer contract.

**Traces to:** RQ-SCALE-014, DSG-LST-011
