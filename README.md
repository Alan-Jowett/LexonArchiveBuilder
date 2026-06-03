# LexonFabric

> LexonFabric weaves mail archives, RFCs, and metadata into a unified, queryable knowledge layer atop LexonGraph, structuring threads, messages, and chunks into a coherent semantic fabric accessible through an MCP server.

LexonFabric is the application layer built on top of LexonGraph. It exists to prove that LexonGraph works on real data and to provide a practical system for indexing and semantically searching mail archives and technical document collections.

At a high level, LexonFabric has two jobs:

1. **Index content** into a structured semantic graph.
2. **Serve search and retrieval** through an MCP server.

The initial content types are **email** and **documents**, but the architecture is intended to extend to additional content types over time without changing the core search contract.

## Why it exists

LexonFabric is meant to validate LexonGraph under realistic ingestion and retrieval workloads while also being directly useful as an application in its own right. The project focuses on turning fragmented archives, messages, and documents into a coherent knowledge layer that can support semantic lookup, retrieval, and downstream RAG-style workflows.

## Architecture overview

LexonFabric is designed as a CDN-backed RAG system with:

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

The MCP server is the search-facing surface of LexonFabric. It is intended to:

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

## Production direction

Production is intended to use:

- Azure Blob Storage for persisted content and artifacts
- Azure OpenAI for embeddings
- a batch-oriented Azure container app deployment model

Some production details are still **planned/TBD**, especially the exact batch shape and surrounding operational workflow. This README describes the intended direction without claiming those deployment details are finalized.

## Using LexonFabric

Conceptually, LexonFabric is used in two stages:

1. **Indexing:** ingest mail archives and technical document collections, normalize them into the LexonGraph-backed fabric, and generate searchable semantic artifacts.
2. **Querying:** connect through the MCP server to search threads, messages, documents, chunks, and related metadata through a unified knowledge layer.

Concrete commands and operational examples can be added as the executable surface of the repository stabilizes.

## Indexer MVP

The first `lexonfabric-indexer` MVP is now implemented as a Rust batch runtime in
`crates/lexonfabric-indexer`.

### Request contract

The MVP request format is JSON and remains collection-oriented across both
supported content classes:

- **mailbox** items point at per-month `.mbox` files
- **document** items point at `.txt` files

See `examples/local/request.sample.json` for a complete local request that
indexes one mailbox file and one document file.

### Running locally

Build and run the batch directly:

```powershell
cargo run -p lexonfabric-indexer -- run --request examples\local\request.sample.json
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
- the `lexonfabric-indexer` batch container
- a named Docker volume mounted into the batch container at `examples/local/block-store`

After the batch completes, the summary output is written to
`examples/local/output/summary.json`.

## License

This project is licensed under the MIT License. See [`LICENSE`](LICENSE).
