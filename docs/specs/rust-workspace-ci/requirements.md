# Rust Workspace CI Requirements

## Document Status

- **Phase:** Phase 2 - Design and Validation
- **Status:** Approved requirements patch being propagated into design and validation
- **Scope:** Repository-level GitHub Actions quality gates for the current Rust workspace in LexonArchiveBuilder, using LexonGraph's `docs/specs/rust-workspace-ci` package as a guide rather than as a verbatim template

## USER-REQUEST

- **UR-CI-1 [KNOWN]:** Build a CI workflow for this repository.
- **UR-CI-2 [KNOWN]:** Use `https://github.com/Alan-Jowett/LexonGraph/tree/main/docs/specs/rust-workspace-ci` as a guide.
- **UR-CI-3 [KNOWN]:** LexonArchiveBuilder is a Rust workspace rooted at `Cargo.toml` with the member crates `crates/lexonarchivebuilder-indexer` and `crates/lexonarchivebuilder-mcp`.
- **UR-CI-4 [KNOWN]:** The repository currently has no `.github/workflows/*.yml` workflow files.
- **UR-CI-5 [KNOWN]:** The repository already documents local Rust entrypoints and Compose-backed local workflows in `README.md`.
- **UR-CI-6 [INFERRED]:** The requested change is about repository verification for the existing Rust workspace rather than release, publishing, or deployment automation.
- **UR-CI-7 [INFERRED]:** The guide repository's broader CI package includes SPDX header enforcement, local Git hooks, coverage publication, and README badges, but the user asked specifically for a CI workflow rather than for all adjacent repository-governance surfaces.
- **UR-CI-8 [INFERRED]:** The workflow should preserve LexonArchiveBuilder's current architecture split by verifying repository quality without redefining indexer behavior, MCP behavior, storage selection, or embedding selection semantics.

## Change Manifest

| ID | Type | Summary | Traceability |
|---|---|---|---|
| CM-CI-001 | Add | Introduce the first structured requirements artifact for a LexonArchiveBuilder Rust workspace CI workflow | UR-CI-1, UR-CI-2 |
| CM-CI-002 | Add | Define a GitHub Actions workflow that verifies the current Rust workspace on `main` pushes and `main` pull requests | UR-CI-1, UR-CI-3, UR-CI-4 |
| CM-CI-003 | Add | Define the core repository quality gates as formatting, linting, and test execution over the Rust workspace | UR-CI-1, UR-CI-3, UR-CI-6 |
| CM-CI-004 | Add | Require practical hosted-CI behavior including path-aware pull-request triggering, cancellation of superseded runs, least-privilege permissions, and Rust-aware caching | UR-CI-1, UR-CI-4, UR-CI-6 |
| CM-CI-005 | Add | Constrain this increment to repository verification only and exclude release, publish, and deployment automation | UR-CI-1, UR-CI-6 |
| CM-CI-006 | Add | Record guide-driven discovery gaps around optional coverage publication, SPDX/header enforcement, and README badge surfacing so they are not silently assumed into scope | UR-CI-2, UR-CI-7 |

## Before / After

### BA-CI-001

- **Before [KNOWN]:** LexonArchiveBuilder had no structured repository requirements for hosted CI workflow behavior.
- **After [KNOWN]:** LexonArchiveBuilder has an explicit Phase 1 requirements baseline for repository CI in `docs/specs/rust-workspace-ci/requirements.md`.

### BA-CI-002

- **Before [KNOWN]:** The repository had no GitHub Actions workflow file under `.github/workflows/`.
- **After [KNOWN]:** The requirements define that the repository will gain a GitHub Actions workflow dedicated to Rust workspace quality verification.

### BA-CI-003

- **Before [KNOWN]:** Repository quality checks for formatting, linting, and tests were runnable locally through Cargo, but not defined as hosted CI requirements.
- **After [KNOWN]:** The requirements define hosted CI quality gates around `cargo fmt`, `cargo clippy`, and `cargo test` for the current Rust workspace.

### BA-CI-004

- **Before [KNOWN]:** The guide repository demonstrates a broader CI/governance package that includes SPDX policy, hooks, coverage publication, and README badges, but LexonArchiveBuilder had no approved statement about whether those adjunct surfaces belong in this increment.
- **After [KNOWN]:** The requirements explicitly keep this increment centered on the GitHub Actions workflow and classify the broader guide-inspired surfaces as out of scope for this increment rather than silently expanding scope.

## Requirements

### Functional Requirements

#### RQ-CI-001 - Hosted CI workflow

LexonArchiveBuilder SHALL define a GitHub Actions workflow for repository quality verification.

- **Execution scope [KNOWN]:** The workflow is repository-level and validates the current Rust workspace rooted at `Cargo.toml`.
- **Traceability:** UR-CI-1, UR-CI-3, UR-CI-4

#### RQ-CI-002 - Trigger surface

The workflow SHALL run on pushes to `main` and on pull requests targeting `main`.

- **Pull request filtering [INFERRED]:** Pull request triggers should be limited to repository-quality-relevant paths so unrelated changes do not trigger the workflow unnecessarily.
- **Minimum governed paths [INFERRED]:**
  - `Cargo.toml`
  - `Cargo.lock`
  - `crates/**`
  - `docs/**`
  - `README.md`
  - `.github/workflows/ci.yml`
- **Approved boundary [KNOWN]:** This increment intentionally stops at the minimum Rust/docs/workflow surface above rather than expanding to scripts, Compose files, or broader repository-governance paths.
- **Traceability:** UR-CI-1, UR-CI-3, UR-CI-4, UR-CI-6

#### RQ-CI-003 - Formatting gate

The workflow SHALL enforce Rust formatting across the workspace with:

`cargo fmt --check --all`

- **Traceability:** UR-CI-1, UR-CI-3, UR-CI-6

#### RQ-CI-004 - Lint gate

The workflow SHALL enforce Clippy across the workspace and fail on warnings treated as errors.

- **Baseline command [INFERRED]:** `cargo clippy --workspace --all-targets --locked -- -D warnings`
- **Traceability:** UR-CI-1, UR-CI-3, UR-CI-6

#### RQ-CI-005 - Test gate

The workflow SHALL execute the Rust workspace test suite.

- **Baseline command [KNOWN]:** `cargo test` is already the repository's current workspace test entrypoint from the repository root.
- **Locked-resolution preference [INFERRED]:** Hosted CI should prefer locked dependency resolution when compatible with the current workspace commands.
- **Traceability:** UR-CI-1, UR-CI-3, UR-CI-6

#### RQ-CI-006 - Hosted CI efficiency

The workflow SHALL use practical GitHub Actions optimizations appropriate for routine repository development.

- **Required behaviors [INFERRED]:**
  - cancel superseded runs for the same workflow and branch or pull request
  - use least-privilege workflow permissions
  - use Rust-aware dependency/build caching
- **Traceability:** UR-CI-1, UR-CI-6

### Boundary and Invariant Requirements

#### RQ-CI-007 - Verification-only scope

This CI increment SHALL remain limited to repository verification and SHALL NOT implement release creation, crate publishing, artifact distribution, or deployment automation.

- **Rationale [INFERRED]:** The user asked for a CI workflow, not for release or operational automation.
- **Traceability:** UR-CI-1, UR-CI-6

#### RQ-CI-008 - Architectural non-interference

The CI requirements SHALL verify repository quality without redefining LexonArchiveBuilder's indexer contracts, MCP search-serving contracts, storage adapters, embedding adapters, or local-versus-production runtime semantics.

- **Rationale [INFERRED]:** CI is a repository-quality surface and must stay subordinate to the existing LexonArchiveBuilder semantic baseline.
- **Traceability:** UR-CI-5, UR-CI-8

#### RQ-CI-009 - Current-repository alignment

The workflow SHALL align with the current LexonArchiveBuilder repository structure and existing Cargo-based verification commands rather than assuming a different workspace layout from the guide repository.

- **Rationale [KNOWN]:** The guide is a reference point, but LexonArchiveBuilder has its own repository shape, crates, and documented local workflows.
- **Traceability:** UR-CI-2, UR-CI-3, UR-CI-5

## Out of Scope

- crate publishing
- GitHub release automation
- binary artifact packaging
- deployment automation for Docker Compose or Azure
- changes to indexer or MCP runtime behavior
- mandatory SPDX/header policy enforcement in this increment
- contributor-local Git hook installation in this increment
- coverage publication and README badge surfacing in this increment

## Invariant Impact Assessment

| Invariant | Impact | Assessment |
|---|---|---|
| Indexing remains separate from search serving | Preserved | The CI requirements verify repository quality only and do not change either semantic boundary |
| Local/testing versus production behavior stays behind stable adapters | Preserved | The workflow validates code quality without selecting or redefining runtime adapters |
| The architecture remains extensible to future content types | Preserved | The CI surface is repository-level and content-type-agnostic |
| The repository remains subordinate to LexonGraph-owned indexing and search contracts | Preserved | The workflow validates LexonArchiveBuilder's codebase without redefining delegated upstream semantics |

## Coverage Notes

- **Covered sources [KNOWN]:**
  - `README.md:1-89`
  - `Cargo.toml:1-30`
  - repository filesystem state showing no `.github/workflows/*.yml`
  - external guide package:
    `Alan-Jowett/LexonGraph/docs/specs/rust-workspace-ci/{requirements,design,validation}.md`
- **Approved discovery outcomes [KNOWN]:**
  - keep this increment limited to the hosted GitHub Actions workflow
  - stop the first workflow at formatting, linting, and tests
  - keep SPDX/header enforcement, hooks, coverage publication, and README badges out of scope
  - keep pull-request path filtering limited to the minimum Rust/docs/workflow surface listed in `RQ-CI-002`
- **Excluded from this requirements artifact [KNOWN]:**
  - design and validation material before the Phase 2 specification pass
  - implementation files before the later implementation phases
