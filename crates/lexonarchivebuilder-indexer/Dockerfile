FROM rust:1.94-bookworm AS builder

WORKDIR /workspace
COPY . .
RUN cargo build --release -p lexonarchivebuilder-indexer

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY --from=builder /workspace/target/release/lexonarchivebuilder-indexer /usr/local/bin/lexonarchivebuilder-indexer
COPY examples/local /workspace/examples/local

ENTRYPOINT ["lexonarchivebuilder-indexer"]
