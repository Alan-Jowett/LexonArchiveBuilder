FROM rust:1.94-bookworm AS builder

WORKDIR /workspace
COPY . .
RUN cargo build --locked --release -p lexonarchivebuilder-block-gateway

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY --from=builder /workspace/target/release/lexonarchivebuilder-block-gateway /usr/local/bin/lexonarchivebuilder-block-gateway

EXPOSE 443/udp

ENTRYPOINT ["lexonarchivebuilder-block-gateway", "serve", "--listen-addr", "0.0.0.0:443"]
