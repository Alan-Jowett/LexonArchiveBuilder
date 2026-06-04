# Rust Workspace CI Validation

## Status

Phase 2 validation patch for the approved repository-level GitHub Actions
workflow that verifies the current LexonFabric Rust workspace.

## Validation Scope

These validation entries define the expected verification surface for the
LexonFabric repository CI workflow.

## Validation Entries

### VAL-CI-001

Open a pull request that changes a repository-quality-relevant path.

**Pass condition:** the CI workflow is triggered.

**Traces to:** RQ-CI-001, RQ-CI-002, DSG-CI-001, DSG-CI-002

### VAL-CI-002

Open a pull request that changes only paths outside the approved minimum
repository-quality path filter.

**Pass condition:** the CI workflow is not triggered solely by that change.

**Traces to:** RQ-CI-002, DSG-CI-002

### VAL-CI-003

Introduce a formatting violation in Rust source.

**Pass condition:** the formatting job fails.

**Traces to:** RQ-CI-003, DSG-CI-006

### VAL-CI-004

Introduce a Clippy warning in the Rust workspace.

**Pass condition:** the lint job fails because warnings are treated as errors.

**Traces to:** RQ-CI-004, DSG-CI-007

### VAL-CI-005

Introduce or expose a failing Rust test.

**Pass condition:** the test job fails.

**Traces to:** RQ-CI-005, DSG-CI-008

### VAL-CI-006

Push multiple updates rapidly to the same branch or pull request.

**Pass condition:** superseded runs for the same workflow are canceled and the
newest run remains authoritative.

**Traces to:** RQ-CI-006, DSG-CI-003

### VAL-CI-007

Inspect the workflow definition.

**Pass condition:** it uses `ubuntu-latest`, the stable Rust toolchain,
least-privilege permissions, Rust-aware caching, and only repository
verification jobs for formatting, linting, and tests.

**Traces to:** RQ-CI-001, RQ-CI-003, RQ-CI-004, RQ-CI-005, RQ-CI-006,
RQ-CI-007, DSG-CI-004, DSG-CI-005, DSG-CI-006, DSG-CI-007, DSG-CI-008,
DSG-CI-009, DSG-CI-010

### VAL-CI-008

Inspect the workflow definition and related specification package against the
LexonFabric semantic baseline.

**Pass condition:** the workflow verifies repository quality without redefining
indexer contracts, MCP contracts, storage adapters, embedding adapters, or
content-type behavior.

**Traces to:** RQ-CI-008, RQ-CI-009, DSG-CI-010, DSG-CI-011
