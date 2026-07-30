# md_worker.Dockerfile
#
# Standalone market-data collector for v0.1.0 deployment.
# Builds the aetelier-sdk `md_worker` binary; runs with a TOML manifest
# mounted at /configs/manifest.toml, writes parquet to /data.
#
# NOTE: the binary moved from `aetelier-connect` to `aetelier-sdk` so it can
# wire `aetelier_io::ParquetSnapshotFlusher` into the per-worker sink set.
# Without that flusher, `OutputSinkConfig::Parquet { dir = ... }` entries
# in the manifest are silently dropped at sink-build time and nothing is
# written to disk. See `aetelier-sdk/src/bin/md_worker.rs` and
# `aetelier-connect::workers::output::build_sinks`.
#
# Build context: the parent of this repository
#   docker build -t aetelier-md-worker:v0.1.0 \
#                -f aetelier-sdk/aetelier-sdk/serve/md_worker.Dockerfile .

# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM rust:1.88-bookworm AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*


# Workspace manifests for layer caching (manifests first, sources later).
COPY aetelier-sdk/Cargo.toml   aetelier-sdk/Cargo.lock        aetelier-sdk/

COPY aetelier-sdk/aetelier-sdk/Cargo.toml        aetelier-sdk/aetelier-sdk/
COPY aetelier-sdk/aetelier-connect/Cargo.toml    aetelier-sdk/aetelier-connect/
COPY aetelier-sdk/aetelier-io/Cargo.toml         aetelier-sdk/aetelier-io/
COPY aetelier-sdk/aetelier-types/Cargo.toml      aetelier-sdk/aetelier-types/
COPY aetelier-sdk/aetelier-telemetry/Cargo.toml  aetelier-sdk/aetelier-telemetry/

# Stub layout for cargo dep cache.
RUN mkdir -p aetelier-sdk/aetelier-sdk/src \
             aetelier-sdk/aetelier-sdk/src/bin \
             aetelier-sdk/aetelier-connect/src \
             aetelier-sdk/aetelier-io/src \
             aetelier-sdk/aetelier-types/src \
             aetelier-sdk/aetelier-telemetry/src \
    && touch aetelier-sdk/aetelier-sdk/src/lib.rs \
             aetelier-sdk/aetelier-connect/src/lib.rs \
             aetelier-sdk/aetelier-io/src/lib.rs \
             aetelier-sdk/aetelier-types/src/lib.rs \
             aetelier-sdk/aetelier-telemetry/src/lib.rs \
    && echo 'fn main(){}' > aetelier-sdk/aetelier-sdk/src/bin/md_worker.rs

RUN cd aetelier-sdk && cargo build --release \
        -p aetelier-sdk --bin md_worker --features parquet 2>/dev/null || true

# Real source.
COPY aetelier-sdk/    aetelier-sdk/

# Bust cargo's mtime fingerprint (cargo reuses stale fingerprints otherwise).
RUN find aetelier-sdk -type f \
        \( -name "*.rs" -o -name "Cargo.toml" \) -exec touch {} +

RUN cd aetelier-sdk && cargo build --release \
        -p aetelier-sdk --bin md_worker --features parquet \
    && strip target/release/md_worker

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime
LABEL aetelier.lifecycle="legacy" aetelier.component="aetelier-md-worker"

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /bin/bash --uid 1000 collector
USER collector
WORKDIR /home/collector

COPY --from=builder /app/aetelier-sdk/target/release/md_worker /usr/local/bin/md_worker

ENV RUST_LOG=aetelier_connect=info,aetelier_sdk=info,md_worker=info
ENV TZ=UTC

# Manifest mounted at /configs/manifest.toml (read-only); data at /data.
ENTRYPOINT ["md_worker", "--config", "/configs/manifest.toml"]
