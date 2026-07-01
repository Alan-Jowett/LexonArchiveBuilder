<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonArchiveBuilder contributors -->

# LexonArchiveBuilder

> LexonArchiveBuilder weaves mail archives, RFCs, and metadata into a unified, queryable knowledge layer atop LexonGraph, structuring threads, messages, and chunks into a coherent semantic fabric accessible through an MCP server.

LexonArchiveBuilder is the application layer built on top of LexonGraph. It exists to prove that LexonGraph works on real data and to provide a practical system for indexing and semantically searching mail archives and technical document collections.

At a high level, LexonArchiveBuilder has two jobs:

1. **Index content** into a structured semantic graph.
2. **Serve search and retrieval** through an MCP server.

The initial content types are **email** and **documents**, but the architecture is intended to extend to additional content types over time without changing the core search contract.

## Why it exists

LexonArchiveBuilder is meant to validate LexonGraph under realistic ingestion and retrieval workloads while also being directly useful as an application in its own right. The project focuses on turning fragmented archives, messages, and documents into a coherent knowledge layer that can support semantic lookup, retrieval, and downstream RAG-style workflows.

## Architecture overview

LexonArchiveBuilder is designed as a CDN-backed RAG system with:

- **LexonGraph** as the underlying graph and knowledge substrate
- An **indexer** that ingests source content, extracts structure, and emits searchable graph-aligned artifacts
- An **MCP server** that exposes search and retrieval over that indexed knowledge
- **Environment-specific adapters** for storage and embeddings

The system is intentionally split so that indexing remains separate from search serving. Outside of indexing, the intended shape is to avoid a central control plane or other server-side processing layers.

## Content model

The current focus is on content that naturally decomposes into:

- collections
- threads
- messages
- documents
- chunks
- metadata

That model supports archives of discussion content alongside long-form technical documents while keeping the design open to future content types.

## Environment model

| Environment | Storage | Embeddings | Runtime shape |
|---|---|---|---|
| Local / testing | Local filesystem | Local embedding server such as `ghcr.io/substratusai/stapi` | 100% local, with indexing running inside Linux Docker containers |
| Production | Azure Blob Storage | Azure OpenAI | Planned/TBD batch-oriented Azure container application shape |

This split is intentional: local development should be self-contained and easy to run without cloud dependencies, while production should scale against cloud-native storage and embedding services.

## MCP server

The MCP server is the search-facing surface of LexonArchiveBuilder. It is intended to:

- expose the indexed knowledge layer through a stable MCP interface
- remain compatible with both **Linux** and **Windows**
- support content backed by either the **local filesystem** or **Azure Blob Storage**

The goal is for clients to interact with a consistent semantic search surface even as the underlying storage and embedding providers vary by environment.

## Local development and testing

Local and testing workflows are designed to be fully local:

- source content lives on the local filesystem
- indexing runs inside Linux Docker containers
- embeddings come from a local embedding service, currently expected to be something like `ghcr.io/substratusai/stapi`
- the MCP server should remain usable from Linux or Windows environments

This keeps local development fast, reproducible, and independent of cloud services.

## Repository policy

Tracked source, script, workflow, infrastructure, and Markdown files are expected to carry the repository SPDX/license header. To check staged changes locally, install the Git hook with:

```bash
git config core.hooksPath hooks
```

## Production direction

Production is intended to use:

- Azure Blob Storage for persisted content and artifacts
- Azure OpenAI for embeddings
- a batch-oriented Azure container app deployment model

Some production details are still **planned/TBD**, especially the exact batch shape and surrounding operational workflow. This README describes the intended direction without claiming those deployment details are finalized.

## Using LexonArchiveBuilder

Conceptually, LexonArchiveBuilder is used in two stages:

1. **Indexing:** ingest mail archives and technical document collections, normalize them into the LexonGraph-backed fabric, and generate searchable semantic artifacts.
2. **Querying:** connect through the MCP server to search threads, messages, documents, chunks, and related metadata through a unified knowledge layer.

Concrete commands and operational examples can be added as the executable surface of the repository stabilizes.

## Indexer MVP

The first `lexonarchivebuilder-indexer` MVP is now implemented as a Rust batch runtime in
`crates/lexonarchivebuilder-indexer`.

### Request contract

The MVP request format is JSON and remains collection-oriented across both
supported content classes:

- **mailbox** items point at per-month `.mbox` or `.mail` files
- **document** items point at `.txt` files

See `examples/local/request.sample.json` for a complete local request that
indexes one mailbox file and one document file.

### Running locally

Build and run the batch directly:

```powershell
cargo run -p lexonarchivebuilder-indexer -- run --request examples\local\request.sample.json
```

The sample request assumes the embedding endpoint is available at
`http://stapi:8080`. For manual testing against an already-running STAPI
container, update the request file's `base_url` to match that endpoint.

### Running with Docker Compose

The repository includes a local integration stack:

```powershell
docker compose up --build indexer
```

That stack starts:

- `stapi` at `http://localhost:8080`
- the `lexonarchivebuilder-indexer` batch container
- a named Docker volume mounted into the batch container at `examples/local/block-store`

After the batch completes, the summary output is written to
`examples/local/output/summary.json`.

### Local scale-test workflow

For large local mailbox stress tests, the repository also includes a lightweight
wrapper script that fetches one or more rsync-backed mailbox archives, discovers
`.mail` and `.mbox` files, generates an indexer request, and runs the existing
`lexonarchivebuilder-indexer` batch to produce a block tree and summary/root handoff
artifact.

The wrapper is intentionally simple. It is designed as a local stress-test
harness over the existing indexer rather than a new indexer subsystem.

**Prerequisites**

- Direct Linux script entrypoint: `rsync` on the host plus Docker with `docker compose`
- Docker Compose entrypoint on Linux or Windows: Docker with `docker compose`

Run it with one or more rsync URLs:

```bash
scripts/lexonarchivebuilder-scale-test.sh \
  rsync.ietf.org::mailman-archive/ipsec/
```

Or from a sources file:

```bash
scripts/lexonarchivebuilder-scale-test.sh \
  --sources-file examples/local/scale-test/rsync.sources.sample.txt
```

The wrapper now uses the repository-pinned LexonGraph published profile
`0.1.0` automatically, so no clustering-tuning flags are required:

```bash
scripts/lexonarchivebuilder-scale-test.sh \
  rsync.ietf.org::mailman-archive/ipsec/
```

To evaluate the current named experiment target after refreshing the
LexonGraph dependency state, pass `--profile-version 0.6.0` to the indexer or
set `"profile_version": "0.6.0"` in the request JSON. Earlier `0.5.x`
alignment remains comparison context for prior experiments, and `0.4.x`
remains historical context.

On Windows, run `.\test.ps1` from the repository root to sweep the current
`0.6.x` series through the clustering-plus-quality workflow.
Build the release indexer first, and override `-RequestPath`, `-BlockStoreRoot`,
or `-OutputRoot` when your local inputs do not live under the default `C:\data3`
layout.

On Linux, you can also launch the same workflow through Docker Compose:

```bash
docker compose run --rm scale-test
```

On Windows, use the Docker Compose entrypoint:

```powershell
docker compose run --rm scale-test
```

To point the Compose entrypoint at a different sources file in the repository:

```powershell
docker compose run --rm scale-test --sources-file /workspace/examples/local/scale-test/rsync.sources.sample.txt
```

The Docker Compose entrypoint uses the same pinned published profile:

```powershell
docker compose run --rm scale-test `
  --sources-file /workspace/examples/local/scale-test/rsync.sources.sample.txt
```

Each run writes its generated request, fetched mailbox mirror, block store, and
summary output under:

```text
examples/local/scale-test/runs/<run-id>/
```

The generated `request.json` is compatible with the existing local indexer
contract, and `summary.json` contains the resulting `root_id` for the produced
block tree.

To exercise the wrapper end to end against local mailbox fixtures, run:

```bash
scripts/lexonarchivebuilder-scale-test-smoke.sh
```

To exercise the Docker Compose entrypoint end to end against local mailbox
fixtures, run:

```bash
scripts/lexonarchivebuilder-scale-test-compose-smoke.sh
```

### Hosted experiment workflows

The repository also includes two hosted GitHub Actions workflows for Azure-backed
experiment automation:

1. **Run embedding refresh** refreshes a reusable embedding dataset and replay
   journal for a checked-in manifest.
2. **Run indexing experiment** reuses that persisted dataset, runs one
   published-profile experiment, and uploads the rooted quality report as a
   workflow artifact.

Both workflows are dispatched manually from GitHub and default to the published
`main` tag of `ghcr.io/<owner>/lexonarchivebuilder-scale-test`, with an input to
override the tag for a specific published image.

The checked-in manifest format is currently JSON with:

```json
{
  "container_name": "ietf-mailing-lists-sample",
  "sources": [
    "rsync.ietf.org::mailman-archive/ipsec/"
  ]
}
```

See `examples/local/scale-test/manifests/ietf-mailing-lists.sample.json` for a
repository sample.

Before running the hosted workflows, configure these repository variables for
GitHub Actions OIDC Azure login:

- `AZURE_LOCATION`
- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_SUBSCRIPTION_ID`

To create the required Azure Entra identity for these workflows, use:

```powershell
$repo = 'Alan-Jowett/LexonArchiveBuilder'
$appName = 'lexonarchivebuilder-experiments-gha'
$subscriptionId = (az account show --query id -o tsv).Trim()
$tenantId = (az account show --query tenantId -o tsv).Trim()
$scope = "/subscriptions/$subscriptionId"

$app = az ad app create `
  --display-name $appName `
  --sign-in-audience AzureADMyOrg `
  --query '{appId:appId,id:id}' `
  -o json | ConvertFrom-Json

az ad sp create --id $app.appId | Out-Null

foreach ($role in @('Contributor', 'User Access Administrator')) {
  az role assignment create `
    --assignee $app.appId `
    --role $role `
    --scope $scope `
    --output none
}

@(
  @{ name = 'github-main'; subject = "repo:${repo}:ref:refs/heads/main" }
) | ForEach-Object {
  $body = @{
    name = $_.name
    issuer = 'https://token.actions.githubusercontent.com'
    subject = $_.subject
    audiences = @('api://AzureADTokenExchange')
  } | ConvertTo-Json -Depth 5

  $tmp = New-TemporaryFile
  Set-Content -Path $tmp -Value $body -NoNewline
  az ad app federated-credential create --id $app.id --parameters @$tmp | Out-Null
  Remove-Item $tmp -Force
}

Write-Host "AZURE_CLIENT_ID=$($app.appId)"
Write-Host "AZURE_TENANT_ID=$tenantId"
Write-Host "AZURE_SUBSCRIPTION_ID=$subscriptionId"
```

This identity needs:

1. **Contributor** on the subscription because the workflows create resource
   groups and deploy the experiment infrastructure.
2. **User Access Administrator** on the subscription because the Bicep package
   creates a VM-scoped role assignment so the one-shot runner can deallocate
   itself after completion.

The federated credential above allows GitHub OIDC access from:

1. `repo:Alan-Jowett/LexonArchiveBuilder:ref:refs/heads/main`

Add additional branch-specific subjects only when you need to run the hosted
workflows from non-`main` refs.

The hosted workflows always attempt to deallocate the experiment VM when the run
concludes, but they intentionally leave the Azure resource group in place for
manual inspection and cleanup.

The hosted experiment workflows now support both `filesystem` and `overlay`
block-store targets, and default to `overlay` while preserving explicit
filesystem selection for comparison or fallback runs.

## MCP MVP

The first `lexonarchivebuilder-mcp` MVP is now implemented as a Rust stdio MCP server
in `crates/lexonarchivebuilder-mcp`.

### Request contract

The local MVP server reads a JSON config file that identifies:

- the local filesystem-backed block store to search
- the local OpenAI-compatible embedding endpoint to use for query embeddings
- the index summary file that provides the current `root_id`
- default `top_k` and traversal-width settings for chunk search

See `examples/local/mcp.request.sample.json` for a complete local config.

### Running locally

First generate the local block store and summary:

```powershell
cargo run -p lexonarchivebuilder-indexer -- run --request examples\local\request.sample.json --summary-out examples\local\output\summary.json
```

The sample MCP config uses `http://stapi:8080` for the Docker Compose network.
For a host-side `cargo run`, update `examples\local\mcp.request.sample.json` to
use a host-reachable endpoint such as `http://localhost:8080` before starting
the server.

Then start the MCP server over stdio:

```powershell
cargo run -p lexonarchivebuilder-mcp -- serve --config examples\local\mcp.request.sample.json
```

The MVP exposes four MCP tools:

- `search_chunks`
- `get_document`
- `get_email`
- `get_thread`

The search tool is executable end to end in the local profile. The named
retrieval tools are present in the MVP surface and currently return an explicit
`unsupported` outcome until LexonGraph exposes a delegated retrieval-by-name
contract for those item classes.

### Running with Docker Compose

Use the local integration stack in three steps so STAPI stays available in the
background, the one-shot indexer finishes writing
`examples/local/output/summary.json`, and then the MCP server starts:

```powershell
docker compose up -d stapi
docker compose run --rm --build --no-deps indexer
docker compose run --rm -i --no-deps mcp
```

That workflow uses:

- `stapi` published to the host at `http://localhost:8080` and reached from the
  Compose network as `http://stapi:8080`
- the `lexonarchivebuilder-indexer` batch container
- the `lexonarchivebuilder-mcp` stdio server container
- a named Docker volume mounted into both containers at
  `examples/local/block-store`

## License

This project is licensed under the MIT License. See [`LICENSE`](LICENSE).
