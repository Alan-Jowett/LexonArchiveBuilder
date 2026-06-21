# LexonArchiveBuilder Scale Test Requirements

## Document Status

- **Phase:** Phase 2 - Specification Changes
- **Status:** Approved requirements patch being propagated into design and validation
- **Scope:** `lexonarchivebuilder-scale-test` local stress-test wrapper for rsync-backed mailbox acquisition, repository-pinned published-profile delegated indexing, and delegated block-tree generation

## USER-REQUEST

- **UR-SCALE-1 [KNOWN]:** Add a spec trifecta for a separate local wrapper/test tool under `docs/specs/lexonarchivebuilder-scale-test/{requirements|design|validation}.md`.
- **UR-SCALE-2 [KNOWN]:** The tool is not part of `lexonarchivebuilder-indexer`; it is built on top of existing LexonArchiveBuilder components.
- **UR-SCALE-3 [KNOWN]:** The tool may be named `lexonarchivebuilder-scale-test`.
- **UR-SCALE-4 [KNOWN]:** The tool accepts one or more rsync URLs as input.
- **UR-SCALE-5 [KNOWN]:** The tool runs a Docker Compose-style local workflow for large-scale testing.
- **UR-SCALE-6 [KNOWN]:** The workflow stages are: fetch mailbox archives from rsync, discover mailboxes, generate an indexer request/config from the discovered mailboxes, then run the existing indexer/parser flow.
- **UR-SCALE-7 [KNOWN]:** The purpose is to stress test the LexonArchiveBuilder parser and produce a block tree.
- **UR-SCALE-8 [KNOWN]:** The output must include a root block or root-id handoff artifact representing the produced block tree.
- **UR-SCALE-9 [KNOWN]:** MCP config generation is out of scope for this increment.
- **UR-SCALE-10 [KNOWN]:** This increment is for large-scale local testing only.
- **UR-SCALE-11 [KNOWN]:** Production orchestration will use ARM/Bicep plus Azure Functions and is out of scope here.
- **UR-SCALE-12 [KNOWN]:** This tool could be as simple as a Linux bash script.
- **UR-SCALE-13 [INFERRED]:** `lexonarchivebuilder-scale-test` should reuse existing LexonArchiveBuilder request and output contracts where practical rather than inventing a second indexing protocol.
- **UR-SCALE-14 [ASSUMPTION]:** When multiple rsync URLs are provided in one run, the tool produces one combined run output and one root handoff artifact for that run.
- **UR-SCALE-15 [KNOWN]:** `lexonarchivebuilder-scale-test` should also be wrapped in a Docker Compose workflow.
- **UR-SCALE-16 [KNOWN]:** The motivation for the Compose wrapper is Windows developer usability because the development box does not support bash.
- **UR-SCALE-17 [KNOWN]:** Linux users should be able to use either the direct bash entrypoint or the Docker Compose entrypoint, while Windows users should use Docker Compose.
- **UR-SCALE-18 [INFERRED]:** Docker Compose must be a supported user-facing entrypoint for the same local stress-test workflow rather than a second divergent workflow.
- **UR-SCALE-19 [KNOWN]:** Mailbox discovery for fetched rsync mirrors must work when the mirrored archive exposes mailbox files with the `.mail` extension as well as the `.mbox` extension.
- **UR-SCALE-20 [KNOWN]:** For this increment, mailbox discovery compatibility should be limited to exactly `.mail` and `.mbox` rather than broadened to arbitrary mailbox archive extensions.
- **UR-SCALE-21 [KNOWN]:** Update the scale test so delegated clustering uses the downstream repository-pinned published-profile contract instead of caller-selected low-level clustering controls.
- **UR-SCALE-22 [KNOWN]:** Update the scale test so it does not accept low-level delegated clustering tuning flags when the repository-pinned published profile is sufficient.
- **UR-SCALE-23 [KNOWN]:** The scale-test caller contract should align with the existing indexer published-profile contract rather than re-exposing retired clustering controls as first-class wrapper flags.
- **UR-SCALE-24 [INFERRED]:** The scale-test wrapper should reuse the delegated indexer's approved published-profile behavior rather than inventing wrapper-local clustering semantics.
- **UR-SCALE-25 [KNOWN]:** Generic arbitrary extra indexer argument passthrough is not the approved caller contract for this increment.
- **UR-SCALE-26 [KNOWN]:** The upstream LexonGraph clustering surface may support multiple clustering strategies internally, but the scale-test wrapper should not expose repository-local clustering-mode selection while the downstream contract is pinned to one approved published profile.
- **UR-SCALE-27 [KNOWN]:** The repository-pinned published profile version `0.1.0` is the approved wrapper clustering behavior for this increment.
- **UR-SCALE-28 [KNOWN]:** The same published-profile clustering contract should apply across both the direct shell and Docker Compose entrypoints.
- **UR-SCALE-29 [KNOWN]:** Retired delegated clustering mode, algorithm, and option flags should remain unexposed by the wrapper and should be rejected clearly if supplied.
- **UR-SCALE-30 [KNOWN]:** The wrapper should continue to mirror the downstream indexer clustering contract as a content-type-agnostic published-profile surface rather than adding content-type-specific clustering logic.
- **UR-SCALE-31 [KNOWN]:** Clean up dead wrapper requirements and downstream specifications that still describe the superseded caller-selectable clustering-control path instead of the approved published-profile path.

## Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-SCALE-001 | Add | Introduce a new structured requirements artifact for `lexonarchivebuilder-scale-test` as a separate local wrapper/test tool | UR-SCALE-1, UR-SCALE-2, UR-SCALE-3 |
| CM-SCALE-002 | Add | Define one-or-more rsync URL inputs for large-scale local mailbox acquisition | UR-SCALE-4, UR-SCALE-5 |
| CM-SCALE-003 | Add | Define the ordered local workflow: rsync fetch, mailbox discovery, generated request/config, delegated parser/indexer run, root handoff | UR-SCALE-5, UR-SCALE-6, UR-SCALE-7, UR-SCALE-8 |
| CM-SCALE-004 | Add | Define the tool purpose as parser stress testing and block-tree production rather than MCP-serving preparation | UR-SCALE-7, UR-SCALE-9 |
| CM-SCALE-005 | Add | Constrain the first realization to a lightweight Linux-local operator form such as a bash script | UR-SCALE-10, UR-SCALE-12 |
| CM-SCALE-006 | Add | Preserve local-only scope while keeping production orchestration out of this increment | UR-SCALE-10, UR-SCALE-11 |
| CM-SCALE-007 | Add | Require stable contract reuse so the wrapper composes existing LexonArchiveBuilder request and output shapes where practical | UR-SCALE-13, UR-SCALE-14 |
| CM-SCALE-008 | Revise | Expand the first realization from bash-capable local operation to dual entrypoint local operation supporting both direct shell use and Docker Compose | UR-SCALE-12, UR-SCALE-15, UR-SCALE-17 |
| CM-SCALE-009 | Add | Require a Docker Compose user entrypoint suitable for Windows-hosted local usage | UR-SCALE-15, UR-SCALE-16, UR-SCALE-17 |
| CM-SCALE-010 | Add | Preserve one shared workflow and artifact model across the bash and Docker Compose entrypoints | UR-SCALE-17, UR-SCALE-18 |
| CM-SCALE-011 | Revise | Expand mailbox discovery compatibility so fetched rsync mirrors may contribute mailbox files ending in `.mail` or `.mbox` without widening the first increment beyond those two extensions | UR-SCALE-19, UR-SCALE-20 |
| CM-SCALE-012 | Revise | Require delegated clustering runs to use the downstream repository-pinned published-profile contract rather than caller-selected clustering algorithms | UR-SCALE-21, UR-SCALE-23, UR-SCALE-24 |
| CM-SCALE-013 | Revise | Retire first-class wrapper exposure of low-level delegated clustering options and reject unsupported tuning flags rather than passing them through | UR-SCALE-22, UR-SCALE-23, UR-SCALE-25, UR-SCALE-29 |
| CM-SCALE-014 | Revise | Preserve one published-profile-based delegated clustering contract across the bash and Docker Compose entrypoints for reproducible local stress-test runs | UR-SCALE-21, UR-SCALE-23, UR-SCALE-24, UR-SCALE-28, UR-SCALE-30 |
| CM-SCALE-015 | Revise | Remove stale mirrored clustering-mode requirements and keep the wrapper aligned to the repository-pinned published-profile path | UR-SCALE-26, UR-SCALE-27, UR-SCALE-28, UR-SCALE-29, UR-SCALE-30, UR-SCALE-31 |

## Before / After

### BA-SCALE-001

- **Before [KNOWN]:** The repository had no structured requirements artifact for a local rsync-driven stress-test wrapper layered above existing LexonArchiveBuilder indexing behavior.
- **After [KNOWN]:** The repository has an explicit requirements baseline for `lexonarchivebuilder-scale-test` in `docs/specs/lexonarchivebuilder-scale-test/requirements.md`.

### BA-SCALE-002

- **Before [KNOWN]:** Local examples assumed hand-authored request/config files and direct indexer invocation.
- **After [KNOWN]:** `lexonarchivebuilder-scale-test` may generate the request/config inputs from rsync-discovered mailbox files before invoking the existing downstream indexing flow.

### BA-SCALE-003

- **Before [KNOWN]:** The rsync-driven workflow had not been separated architecturally from `lexonarchivebuilder-indexer`.
- **After [KNOWN]:** The rsync-driven workflow is explicitly defined as a separate local wrapper/test tool rather than part of the indexer feature boundary.

### BA-SCALE-004

- **Before [KNOWN]:** The wrapper implementation vehicle was open across crate, service, or script options.
- **After [KNOWN]:** The first realization may be a simple Linux bash script, provided it preserves the approved ordered workflow and required artifacts.

### BA-SCALE-005

- **Before [KNOWN]:** MCP-serving preparation was still a plausible wrapper output.
- **After [KNOWN]:** MCP config generation is explicitly out of scope; the required output is the block tree and root handoff artifact.

### BA-SCALE-006

- **Before [KNOWN]:** The first realization allowed a lightweight Linux bash script, but did not define a Windows-friendly user entrypoint when host bash is unavailable.
- **After [KNOWN]:** The first realization must also expose a Docker Compose user entrypoint so the same local stress-test workflow remains usable from Windows hosts.

### BA-SCALE-007

- **Before [KNOWN]:** The wrapper requirements described one local workflow but did not distinguish between workflow semantics and the user-facing entrypoint used to launch that workflow.
- **After [KNOWN]:** The requirements define one wrapper workflow with dual supported local entrypoints: direct bash on Linux and Docker Compose on Linux or Windows.

### BA-SCALE-008

- **Before [KNOWN]:** Mailbox discovery compatibility was not explicit, so the documented rsync-driven workflow could implicitly assume only `.mbox` mailbox files even when fetched mirrors exposed `.mail` files.
- **After [KNOWN]:** Mailbox discovery compatibility is explicit: the first increment accepts mailbox files ending in `.mail` or `.mbox` from fetched rsync mirrors, while broader extension support remains out of scope.

### BA-SCALE-009

- **Before [KNOWN]:** The wrapper caller contract was still being evaluated as a possible place to expose delegated clustering-algorithm selection.
- **After [KNOWN]:** The wrapper caller contract now relies on the downstream repository-pinned published profile instead of exposing delegated clustering-algorithm selection.

### BA-SCALE-010

- **Before [KNOWN]:** The wrapper requirements still entertained a first-class surface for delegated clustering tuning options.
- **After [KNOWN]:** The wrapper requirements now retire low-level delegated clustering tuning inputs and keep the request shape fixed to the downstream published-profile contract.

### BA-SCALE-011

- **Before [KNOWN]:** The wrapper could have grown a generic opaque downstream-argument passthrough that diverged between direct shell and Docker Compose launch paths.
- **After [KNOWN]:** The wrapper requirements constrain this increment to one explicit published-profile-based delegated clustering contract that remains consistent across both supported entrypoints.

### BA-SCALE-012

- **Before [KNOWN]:** The wrapper requirements still carried forward a transient caller-selectable clustering surface, including mode, algorithm, and option exposure, from an earlier exploration path.
- **After [KNOWN]:** The wrapper requirements now align entirely to the repository-pinned published-profile path, preserve parity across shell and Docker Compose entrypoints, and treat low-level delegated clustering flags as retired.

## Requirements

### Functional Requirements

#### RQ-SCALE-001 - Wrapper boundary

LexonArchiveBuilder SHALL provide a separate local wrapper/test tool named `lexonarchivebuilder-scale-test`.

- **Boundary [KNOWN]:** This tool is not part of `lexonarchivebuilder-indexer`.
- **Non-goal [KNOWN]:** This tool does not define repository-local indexing semantics or MCP semantics.
- **Traceability:** UR-SCALE-1, UR-SCALE-2, UR-SCALE-3

#### RQ-SCALE-002 - Rsync source inputs

`lexonarchivebuilder-scale-test` SHALL accept one or more rsync URLs as workflow inputs.

- **Traceability:** UR-SCALE-4

#### RQ-SCALE-003 - Ordered local stress-test workflow

`lexonarchivebuilder-scale-test` SHALL orchestrate the local workflow in ordered stages:

1. fetch rsync sources
2. discover mailbox files
3. generate an indexer-compatible request/config from the discovered mailbox set
4. invoke the existing LexonArchiveBuilder parser/indexer flow
5. emit block-tree handoff artifacts

- **Constraint [KNOWN]:** The workflow is local/testing-only in this increment.
- **Traceability:** UR-SCALE-5, UR-SCALE-6, UR-SCALE-7, UR-SCALE-8, UR-SCALE-10

#### RQ-SCALE-003A - Minimal operator realization

The first `lexonarchivebuilder-scale-test` realization SHALL be allowed to use a simple Linux operator form such as a bash script.

- **Constraint [KNOWN]:** The implementation vehicle may stay lightweight so long as it preserves the approved ordered workflow and output artifacts.
- **Non-goal [KNOWN]:** This increment does not require a dedicated Rust crate, long-lived service, or control-plane component.
- **Traceability:** UR-SCALE-12

#### RQ-SCALE-003B - Docker Compose user entrypoint

`lexonarchivebuilder-scale-test` SHALL provide a Docker Compose-based user entrypoint for the approved local stress-test workflow.

- **Required property [KNOWN]:** The Compose entrypoint must execute the same rsync -> discovery -> generated request/config -> delegated parser/indexer -> root handoff flow.
- **Traceability:** UR-SCALE-15, UR-SCALE-16, UR-SCALE-17

#### RQ-SCALE-003C - Dual local entrypoint support

For the first local/testing increment, `lexonarchivebuilder-scale-test` SHALL support both:

1. a direct Linux-oriented shell entrypoint
2. a Docker Compose entrypoint

- **Platform intent [KNOWN]:** Linux users may use either entrypoint; Windows users rely on Docker Compose.
- **Traceability:** UR-SCALE-16, UR-SCALE-17

#### RQ-SCALE-003D - Shared workflow semantics across entrypoints

The bash and Docker Compose entrypoints SHALL preserve the same wrapper-owned workflow semantics, output artifact family, and downstream indexer contract.

- **Constraint [INFERRED]:** Docker Compose must not introduce a second, divergent `lexonarchivebuilder-scale-test` contract.
- **Traceability:** UR-SCALE-17, UR-SCALE-18

#### RQ-SCALE-003E - Repository-pinned published profile for delegated clustering

For any `lexonarchivebuilder-scale-test` run that includes delegated clustering,
the wrapper SHALL use the downstream repository-pinned published-profile
contract rather than allowing the caller to select delegated clustering mode or
delegated clustering algorithm.

- **Approved profile [KNOWN]:** For this increment, the wrapper relies on the
  downstream repository-pinned published profile version `0.1.0`.
- **Entry-point parity [KNOWN]:** The same published-profile contract applies to
  both the direct shell and Docker Compose entrypoints.
- **Boundary [KNOWN]:** The wrapper does not define wrapper-local clustering
  semantics or reconstruct lower-level delegated planning choices.
- **Traceability:** UR-SCALE-21, UR-SCALE-23, UR-SCALE-24, UR-SCALE-26, UR-SCALE-27, UR-SCALE-28, UR-SCALE-30

#### RQ-SCALE-003F - Retired low-level clustering flag rejection

`lexonarchivebuilder-scale-test` SHALL NOT expose low-level delegated
clustering mode, algorithm, or tuning options as approved wrapper inputs.

- **Approved caller contract [KNOWN]:** The wrapper's approved clustering
  contract is the downstream repository-pinned published profile rather than a
  wrapper-visible family of low-level clustering flags.
- **Rejection rule [KNOWN]:** If callers supply retired low-level clustering
  flags, the wrapper should fail clearly instead of silently ignoring or
  forwarding them.
- **Boundary [INFERRED]:** Future expansion, if any, should come from an
  approved published-profile-based contract rather than ad hoc wrapper-local
  tuning inputs.
- **Traceability:** UR-SCALE-22, UR-SCALE-23, UR-SCALE-25, UR-SCALE-26, UR-SCALE-29, UR-SCALE-31

#### RQ-SCALE-004 - Delegated parser/indexer use

`lexonarchivebuilder-scale-test` SHALL invoke the existing LexonArchiveBuilder batch contract as a downstream dependency rather than moving this workflow into `lexonarchivebuilder-indexer`.

- **Required property [KNOWN]:** The tool stress-tests existing parser/indexer behavior through generated inputs.
- **Traceability:** UR-SCALE-2, UR-SCALE-6, UR-SCALE-7, UR-SCALE-13

#### RQ-SCALE-004A - Delegated clustering contract preservation

When `lexonarchivebuilder-scale-test` runs delegated clustering, it SHALL rely
on the existing LexonArchiveBuilder indexer entrypoint's repository-pinned
published-profile contract without redefining clustering semantics in the
wrapper.

- **Authority boundary [KNOWN]:** The downstream indexer remains the authority
  for clustering validation, defaulting, and execution semantics.
- **Constraint [INFERRED]:** The wrapper remains responsible only for generating
  compatible requests and invoking the downstream indexer consistently across
  its supported entrypoints.
- **Traceability:** UR-SCALE-21, UR-SCALE-23, UR-SCALE-24, UR-SCALE-27, UR-SCALE-28, UR-SCALE-30, UR-SCALE-31

#### RQ-SCALE-005 - Mailbox discovery expansion

For fetched rsync mirrors, `lexonarchivebuilder-scale-test` SHALL discover mailbox files ending in `.mail` or `.mbox` and translate the discovered set into indexer-compatible mailbox batch items.

- **Accepted compatibility set [KNOWN]:** The first increment accepts exactly `.mail` and `.mbox` mailbox files.
- **Extensibility [KNOWN]:** This must not preclude future wrapper support for documents or other content classes with different metadata handling.
- **Boundary [KNOWN]:** This increment does not require broader mailbox extension support or content sniffing beyond the approved `.mail` and `.mbox` compatibility set.
- **Residual gap [UNKNOWN]:** Include/exclude rules beyond the explicit `.mail` and `.mbox` extension allowlist are not yet specified.
- **Traceability:** UR-SCALE-6, UR-SCALE-19, UR-SCALE-20

#### RQ-SCALE-006 - Block-tree output

`lexonarchivebuilder-scale-test` SHALL produce a block tree from the generated run.

- **Current baseline [KNOWN]:** The downstream LexonArchiveBuilder flow already emits summary/root information sufficient to identify the produced tree root.
- **Traceability:** UR-SCALE-7, UR-SCALE-8

#### RQ-SCALE-007 - Root handoff artifact

`lexonarchivebuilder-scale-test` SHALL emit a machine-consumable handoff artifact containing the root identifier or reference for the produced block tree.

- **Boundary [KNOWN]:** This handoff is for inspection or downstream use; MCP config generation is not required in this increment.
- **Traceability:** UR-SCALE-8, UR-SCALE-9

#### RQ-SCALE-008 - No MCP-config generation in scope

The first `lexonarchivebuilder-scale-test` increment SHALL NOT require generation of MCP server configuration artifacts.

- **Rationale [KNOWN]:** The approved scope is stress testing and block-tree production only.
- **Traceability:** UR-SCALE-9

#### RQ-SCALE-009 - Local-only execution scope

The first `lexonarchivebuilder-scale-test` increment SHALL be executable for large-scale local testing and SHALL NOT define the production orchestration workflow.

- **Preserved seam [KNOWN]:** Production remains a separate ARM/Bicep plus Azure Functions concern.
- **Traceability:** UR-SCALE-10, UR-SCALE-11

#### RQ-SCALE-009A - Linux-local execution shape

The first executable `lexonarchivebuilder-scale-test` realization SHALL target a Linux-oriented local execution environment compatible with the existing containerized local workflow.

- **Rationale [INFERRED]:** A bash-script realization implies a Linux-shaped operator environment and aligns with the repository's existing container-oriented local profile.
- **Compatibility note [KNOWN]:** This Linux-oriented execution shape may still be launched from a Windows host through Docker Compose.
- **Traceability:** UR-SCALE-5, UR-SCALE-10, UR-SCALE-12, UR-SCALE-16

#### RQ-SCALE-009B - Windows-friendly local usability

The first `lexonarchivebuilder-scale-test` increment SHALL remain executable for Windows-hosted local development through Docker Compose even when bash is unavailable on the host.

- **Boundary [KNOWN]:** This is a local/testing usability requirement, not a production orchestration requirement.
- **Traceability:** UR-SCALE-15, UR-SCALE-16, UR-SCALE-17

#### RQ-SCALE-009C - Linux optionality preservation

The addition of Docker Compose support SHALL NOT remove the direct Linux shell entrypoint for `lexonarchivebuilder-scale-test`.

- **Rationale [KNOWN]:** Linux users should retain either launch mode.
- **Traceability:** UR-SCALE-17

#### RQ-SCALE-010 - Stable contract reuse

`lexonarchivebuilder-scale-test` SHALL reuse existing LexonArchiveBuilder-compatible request and output contracts where practical.

- **Constraint [INFERRED]:** The wrapper should compose existing shapes rather than inventing a parallel protocol without need.
- **Traceability:** UR-SCALE-13

#### RQ-SCALE-010A - Reproducible delegated clustering contract

`lexonarchivebuilder-scale-test` SHALL preserve one explicit effective
delegated clustering configuration per run across its supported local
entrypoints.

- **Rationale [INFERRED]:** Comparable stress-test runs require the approved
  published profile behavior to remain stable for the lifetime of one wrapper
  invocation.
- **Boundary [KNOWN]:** This requirement constrains wrapper contract
  consistency; it does not redefine how the downstream indexer serializes or
  internally applies published-profile settings.
- **Traceability:** UR-SCALE-21, UR-SCALE-23, UR-SCALE-24, UR-SCALE-27, UR-SCALE-28, UR-SCALE-30

#### RQ-SCALE-011 - Combined run output

When multiple rsync URLs are provided in one run, `lexonarchivebuilder-scale-test` SHALL produce one coherent output set for that run.

- **Assumption [ASSUMPTION]:** One run yields one combined root handoff artifact.
- **Traceability:** UR-SCALE-4, UR-SCALE-8, UR-SCALE-14

### Boundary and Invariant Requirements

#### RQ-SCALE-012 - Indexer/MCP separation

`lexonarchivebuilder-scale-test` SHALL remain limited to local orchestration, input materialization, and delegated batch execution and SHALL NOT redefine indexer semantics or MCP-serving behavior.

- **Rationale [KNOWN]:** The user explicitly scoped this tool as a wrapper/test tool rather than an indexer or MCP feature.
- **Traceability:** UR-SCALE-2, UR-SCALE-9, UR-SCALE-13

#### RQ-SCALE-013 - Subordinate downstream contracts

`lexonarchivebuilder-scale-test` SHALL remain subordinate to the public contracts already owned by downstream LexonArchiveBuilder entrypoints and their delegated LexonGraph dependencies and SHALL NOT redefine block-construction, parser, or embedding semantics within this repository.

- **Rationale [INFERRED]:** The wrapper exists to stress-test existing behavior, not to invent an alternative indexing protocol.
- **Traceability:** UR-SCALE-2, UR-SCALE-7, UR-SCALE-13

#### RQ-SCALE-014 - Future content extensibility

`lexonarchivebuilder-scale-test` SHALL preserve room for future non-mailbox stress-test inputs without redefining the core wrapper contract.

- **Initial focus [KNOWN]:** rsync-backed mailbox acquisition
- **Constraint [INFERRED]:** Future document-specific or other content-specific discovery policies should fit behind the same wrapper boundary.
- **Traceability:** UR-SCALE-6, UR-SCALE-13

## Out of Scope

- Moving this workflow into `lexonarchivebuilder-indexer`
- Defining repository-local indexing, parser, block-construction, or embedding algorithms
- Inventing wrapper-local clustering semantics or algorithm names distinct from the delegated indexer contract
- Providing generic arbitrary downstream indexer argument passthrough beyond the approved first-class delegated clustering inputs
- Defining MCP server behavior or generating MCP configuration artifacts
- Defining the production ARM/Bicep plus Azure Functions workflow
- Requiring a dedicated Rust crate or service for the first wrapper realization
- Broadening mailbox discovery beyond the approved `.mail` and `.mbox` compatibility set in this increment
- Finalizing include/exclude filtering policy beyond the approved `.mail` and `.mbox` extension allowlist

## Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | The wrapper is limited to local orchestration and delegated execution |
| `lexonarchivebuilder-indexer` remains focused on indexing contracts | Preserved | The rsync stress-test flow is explicitly outside the indexer boundary |
| Local/testing remains self-contained and batch-oriented | Preserved | The wrapper remains stage-ordered and container-oriented while supporting both direct Linux shell use and Docker Compose launch |
| Entry-point parity remains intact | Preserved | The repository-pinned published-profile clustering contract is required to stay consistent across shell and Docker Compose launch modes |
| Production seams remain open | Preserved | Production orchestration remains a separate future workflow |
| Future content extensibility remains intact | Preserved | The wrapper adds mailbox stress testing now without closing off later document handling |
| LexonArchiveBuilder remains subordinate to LexonGraph contracts | Preserved | The wrapper drives existing downstream flows rather than redefining block construction |

## Open Questions / Discovery Gaps

- **Q-SCALE-001 [UNKNOWN]:** Beyond the approved `.mail` and `.mbox` compatibility set, what additional mailbox file patterns, if any, should future increments qualify for discovery from fetched rsync mirrors?
- **Q-SCALE-002 [UNKNOWN]:** Should the generated request/config artifact be persisted as a durable fixture, ephemeral run output, or both?
- **Q-SCALE-003 [UNKNOWN]:** What metadata from the rsync source URL, if any, must be preserved in generated mailbox batch items?
- **Q-SCALE-004 [UNKNOWN]:** If one rsync source is unreachable during a multi-source run, should the wrapper fail the whole run or allow partial stress-test completion?
- **Q-SCALE-005 [UNKNOWN]:** Should the Docker Compose entrypoint wrap the existing bash implementation inside a Linux container, or should Compose invoke a dedicated container command path that reproduces the same workflow semantics without host bash?
- **Q-SCALE-006 [UNKNOWN]:** How should rsync source inputs be passed into the Docker Compose entrypoint for Windows users: a mounted sources file, inline environment variables, or both?
- **Q-SCALE-007 [UNKNOWN]:** If future increments approve more than one published profile version, should the effective profile version be persisted in a dedicated machine-consumable run artifact in addition to the existing generated request and summary outputs?
- **Q-SCALE-008 [UNKNOWN]:** If future increments approve additional published profiles, should the wrapper expose caller-visible profile selection or remain repository-pinned until workload segmentation demands it?

## Coverage Notes

- **Covered sources [KNOWN]:**
  - user request in this session: "clean up the dead spec/code that is unrelated to the new profile version based path. It has left over stuff from the previous path where we tried to define it at this layer."
  - user request in this session: "the upstream LexonGraph API has evolved to allow either divisive or aggregation based clustering. We need to expose this as an option at this layer as well"
  - user clarification in this session: "I think it is important to both. Aggregate should be the default with an option to try out divisive (but I suspect that won't be interesting)"
  - `README.md:91-168`
  - `README.md:183-203`
  - `docker-compose.yml:1-45`
  - `examples/local/request.sample.json:1-34`
  - `examples/local/scale-test/rsync.sources.sample.txt:1-4`
  - `crates/lexonarchivebuilder-indexer/src/main.rs:16-27`
  - `crates/lexonarchivebuilder-indexer/src/main.rs:79-124`
  - `crates/lexonarchivebuilder-indexer/src/config.rs:50-100`
  - `crates/lexonarchivebuilder-indexer/src/config.rs:102-216`
  - `scripts/lexonarchivebuilder-scale-test.sh:223-235`
  - `scripts/lexonarchivebuilder-scale-test.sh:293-303`
  - user clarification in this session: "I don't want this to be part of the lexonfabric indexer. This is a wrapper / test tool built on top of it"
  - user clarification in this session: "This tool is just a way to run a local stress test on the LexonFabric parser and produce a block tree. Feel free to name it lexonfabric-scale-test or something along those lines."
  - user clarification in this session selecting block tree/root handoff only rather than MCP config output
  - user clarification in this session: "This could be as simple as a Linux bash script, no need for anything fancy here."
  - user clarification in this session: "Support both. When running on Linux people can use either. On Windows they use docker compose"
  - user clarification in this session selecting: "Exactly `.mail` and `.mbox`"
  - user clarification in this session selecting: "First-class clustering flags passed through to the indexer (Recommended)"
- **Excluded for now [KNOWN]:**
  - Exact generated file locations and directory layout
  - Specific script flag spellings, shell ergonomics, and Docker Compose command lines
  - Rust implementation files, tests, and operational assets
