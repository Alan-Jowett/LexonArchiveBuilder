# Rust Workspace CI Design

## Status

Phase 2 specification patch for the approved repository-level GitHub Actions
workflow that verifies the current LexonFabric Rust workspace.

## Scope

This document specifies the LexonFabric-owned design for realizing the approved
`rust-workspace-ci` requirements.

This document is layered on top of:

- `docs/specs/rust-workspace-ci/requirements.md`
- `README.md`
- `Cargo.toml`
- the current repository filesystem state showing no existing
  `.github/workflows/*.yml`
- external guide package:
  `Alan-Jowett/LexonGraph/docs/specs/rust-workspace-ci/{requirements,design,validation}.md`

This document does not define SPDX/header policy enforcement, local Git hooks,
coverage publication, README badges, release automation, or deployment
automation in this increment.

## Impact Map

### Directly affected artifacts

- `docs/specs/rust-workspace-ci/requirements.md`
- `docs/specs/rust-workspace-ci/design.md`
- `docs/specs/rust-workspace-ci/validation.md`

### Indirectly affected artifacts

- `.github/workflows/ci.yml`
- repository contributor expectations around hosted CI behavior

### Unaffected artifacts

- `docs/specs/lexonfabric-indexer/*`
- `docs/specs/lexonfabric-mcp/*`
- indexer runtime behavior
- MCP runtime behavior
- Docker Compose and Azure deployment behavior

## Design Goals

The workflow design is intended to be:

- minimal
- deterministic
- aligned with the current LexonFabric repository structure
- efficient for routine pull requests
- explicit about non-goals
- subordinate to the existing indexer and MCP semantic boundaries

## Workflow Boundary

The repository quality gates own:

- GitHub Actions triggering for repository-quality-relevant Rust/docs/workflow
  changes
- formatting verification
- lint verification
- Rust workspace test execution
- hosted CI cancellation and caching behavior

The repository quality gates do not own:

- release automation
- package publishing
- artifact distribution
- coverage publication
- README badge surfacing
- SPDX/header policy enforcement
- contributor-local Git hook management

## Workflow Shape

### DSG-CI-001 `Workflow file`

The repository defines the workflow at `.github/workflows/ci.yml`.

**Traces to:** RQ-CI-001, RQ-CI-009

### DSG-CI-002 `Triggers`

The workflow triggers on:

- `push` to `main`
- `pull_request` targeting `main`

Pull request triggers are limited to the approved minimum repository-quality
surface:

- `Cargo.toml`
- `Cargo.lock`
- `crates/**`
- `docs/**`
- `README.md`
- `.github/workflows/ci.yml`

**Traces to:** RQ-CI-002, RQ-CI-009

### DSG-CI-003 `Concurrency`

The workflow uses GitHub Actions concurrency to cancel superseded runs for the
same workflow and pull request when a pull request number is available, and for
the same workflow and Git ref otherwise.

**Traces to:** RQ-CI-006

### DSG-CI-004 `Execution environment`

The workflow runs on `ubuntu-latest` and uses the stable Rust toolchain for all
Rust verification jobs.

**Traces to:** RQ-CI-001, RQ-CI-003, RQ-CI-004, RQ-CI-005

### DSG-CI-005 `Permissions`

The workflow uses least-privilege permissions sufficient for repository
checkout and CI execution.

**Traces to:** RQ-CI-006

### DSG-CI-006 `Formatting job`

The workflow contains a formatting job that installs `rustfmt` and runs:

`cargo fmt --check --all`

**Traces to:** RQ-CI-003

### DSG-CI-007 `Lint job`

The workflow contains a lint job that installs `clippy`, restores Rust-aware
cache state, and runs:

`cargo clippy --workspace --all-targets --locked -- -D warnings`

**Traces to:** RQ-CI-004, RQ-CI-006

### DSG-CI-008 `Test job`

The workflow contains a test job that restores Rust-aware cache state and runs:

`cargo test --workspace --locked`

**Traces to:** RQ-CI-005, RQ-CI-006

### DSG-CI-009 `Caching`

The workflow uses Rust-aware dependency and build caching suitable for GitHub
Actions rather than repository-local assumptions about prewarmed build state.

**Traces to:** RQ-CI-006

### DSG-CI-010 `Verification-only boundary`

The workflow contains only repository verification jobs and does not add
release creation, crate publishing, artifact packaging, deployment automation,
coverage publication, README badge updates, SPDX/header enforcement, or local
hook execution.

**Traces to:** RQ-CI-007

### DSG-CI-011 `Semantic non-interference`

The workflow validates the repository without redefining or branching on
LexonFabric's indexer contracts, MCP contracts, storage adapters, embedding
adapters, or content-type abstractions.

**Traces to:** RQ-CI-008

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-CI-001 | RQ-CI-001, RQ-CI-009 |
| DSG-CI-002 | RQ-CI-002, RQ-CI-009 |
| DSG-CI-003, DSG-CI-005, DSG-CI-009 | RQ-CI-006 |
| DSG-CI-004, DSG-CI-006 | RQ-CI-003 |
| DSG-CI-004, DSG-CI-007 | RQ-CI-004 |
| DSG-CI-004, DSG-CI-008 | RQ-CI-005 |
| DSG-CI-010 | RQ-CI-007 |
| DSG-CI-011 | RQ-CI-008 |
