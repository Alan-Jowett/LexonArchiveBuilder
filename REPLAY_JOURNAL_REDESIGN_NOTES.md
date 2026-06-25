# Replay Journal Redesign Pause Notes

## Status

This repository has an **approved specification package** for redesigning the
indexer replay journal, but the **implementation is intentionally paused** until
the underlying block-store and block-model support is updated to handle the
required arbitrary journal blocks cleanly.

Approved spec files:

- `docs/specs/lexonarchivebuilder-indexer/requirements.md`
- `docs/specs/lexonarchivebuilder-indexer/design.md`
- `docs/specs/lexonarchivebuilder-indexer/validation.md`

No implementation changes are intentionally left in the Rust code at this
pause point.

## Why work paused

We decided the next implementation attempt should happen **after** the block
store / LexonGraph block model is updated so the indexer can store replay
journal data as ordinary immutable blocks without forcing an awkward local-only
filesystem journal format.

The immediate prerequisite is:

1. **Fix the block store / block model so it can support arbitrary journal
   blocks cleanly.**

This likely means a small upstream LexonGraph change, because the current block
crate does not appear to expose a dedicated free-form or journal block shape.

## Architecture decisions we reached

### 1. Replay journal ownership

- The **replay journal is indexer-owned**, not LexonGraph-owned.
- LexonGraph should remain authoritative for:
  - generic block storage
  - overlay behavior
  - durability mechanics
  - block-store adapters
- LexonArchiveBuilder should remain authoritative for:
  - replay semantics
  - replay item identity
  - replay snapshot selection
  - resume behavior

## 2. Remove block-store iteration as the replay source

We concluded that the indexer should **stop depending on whole-store
`iter_block_ids()`** for clustering replay discovery.

Clustering-only replay should instead load inputs from an **explicit selected
replay-journal snapshot**.

Iteration existed only as a fallback because:

1. production did not have a journal location
2. legacy stores might not have a journal
3. stale or invalid journals needed a fallback path

The approved redesign removes that fallback from the repository contract.

## 3. Immutable block-addressed replay journal

The replay journal should become:

- **immutable**
- **block-addressed**
- **stored through the approved `BlockStore` boundary**

Instead of mutable append-only segment files on local disk, an ingestion or
later stage should publish a new immutable replay-journal snapshot.

## 4. Replay journal as a DAG

We agreed the replay journal should be modeled as a **DAG of immutable blocks**.

Desired behavior:

1. a run performs work
2. it creates a new replay-journal block/root
3. that new snapshot can link to one or more previous replay-journal roots
4. a ref is advanced to the new root

This gives:

- immutable history
- incremental publication
- content-addressed replay state
- explicit snapshot selection for experiments and resume flows

## 5. Git-style mutable refs

Replay-journal snapshots should be selected through **Git-style mutable refs**
that point at immutable replay-journal roots.

The chosen implementation direction for the first cut was:

- store refs as **ordinary block-store blocks**
- use a **small well-known pointer** to publish the current ref target

We also decided runs should choose the replay ref through an:

- **explicit `replay_ref` input in the request/CLI**

rather than using an implicit global default.

## 6. Resumability expectations

### Ingestion plus embedding

Approved target:

- ingestion should be resumable from the selected replay-journal snapshot
- completed replayable leaf outputs should be recognized and skipped

### Clustering plus block assembly

For the **first implementation cut**, we explicitly narrowed scope to:

- publish a **new replay snapshot only after successful
  clustering/block-assembly completion**
- **do not** implement mid-stage clustering resume yet

That means the first clustering resumability cut is publication-oriented rather
than partial-progress recovery-oriented.

## 7. Environment model

We agreed local/testing and production-shaped execution should share the **same
replay snapshot and replay ref semantics**, even if the concrete publication
mechanics differ by environment.

The approved specs therefore removed the earlier assumption that replay journal
support depends on a LAB-managed local filesystem block-store root.

## 8. Relationship to overlay storage

We also concluded:

- the **LexonGraph overlay crate should be authoritative**
- this repository should **not duplicate overlay logic**

The replay-journal redesign should therefore sit **above** the generic block
store abstraction and work equally with:

- filesystem-backed storage
- Azure-backed storage
- future overlay-backed storage

## Implementation choices already decided

If implementation resumes after the upstream block support lands, use these
choices unless they are deliberately revised:

1. **Replay journal remains indexer-owned**
2. **Replay refs are explicit request/CLI inputs**
3. **Replay refs are stored as ordinary block-store blocks plus a small
   well-known pointer**
4. **Standalone clustering loads only from the selected replay snapshot**
5. **No whole-store iteration fallback**
6. **First clustering resume cut publishes a new snapshot only after successful
   stage completion**

## What was changed in specs

The approved spec package now says:

- standalone clustering is rooted in an **explicit replay-journal snapshot**
- replay snapshots are **immutable**
- replay history may form a **DAG**
- replay refs are **mutable selector objects**
- replay semantics stay **indexer-owned**
- local and production-shaped execution share the same replay semantics

## Open questions intentionally left for later

These were not resolved and should be revisited when implementation resumes:

1. What exact upstream LexonGraph block change is best:
   - new free-form block kind
   - dedicated journal/artifact block kind
   - reuse of existing artifact-like leaf blocks
2. What exact replay-journal payload schema should be used?
3. How should the small well-known ref pointer be represented?
4. Should the first DAG increment allow **multi-parent** snapshots, or should
   it start as a single-parent append chain?
5. Should replay snapshots carry only replay metadata plus leaf ids, or also
   enough extra data to avoid rereading some stored leaf blocks?

## Suggested resume order

When work resumes:

1. Update LexonGraph / block-store support so arbitrary journal blocks are a
   clean supported use case.
2. Re-check whether existing artifact-like leaf blocks are now sufficient, or
   whether a dedicated upstream block kind exists.
3. Add explicit `replay_ref` to the request and CLI.
4. Replace the local filesystem replay-journal segment implementation with
   block-store-backed immutable replay snapshots.
5. Replace clustering-only replay discovery with replay-ref resolution.
6. Add ingestion resume using the selected replay snapshot.
7. Add successful-clustering snapshot publication.
8. Run the validation scenarios already captured in the approved validation
   spec.

## Notes about current repository state

- The approved replay-journal redesign currently exists only in the
  **requirements/design/validation docs**.
- A partial implementation was started and then intentionally **backed out**
  before this handoff so the branch stays coherent.
- This note is meant to be the quick restart point after the block-store update
  is available.
