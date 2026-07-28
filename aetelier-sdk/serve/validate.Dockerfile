# aetelier-infra/dockerfiles/validate.Dockerfile
#
# Post-hoc parquet integrity & invariant checker for the `md_worker`
# data lake. Builds the `aetelier-sdk` `validate` binary and runs it
# against the same `/data` mount that `md_worker` writes into,
# persisting cumulative statistics under `/state` so subsequent runs
# can compute deltas across runs.
#
# Build context: the siblings parent dir (parent of aetelier-sdk)
#   docker build -t iteralabs/aetelier-validate:v0.1.0 \
#                -f aetelier-sdk/aetelier-sdk/serve/validate.Dockerfile .
#
# Run on demand:
#   docker run --rm \
#     -v /var/lib/aetelier/datasets:/data:ro \
#     -v /var/lib/aetelier/validate-state:/state \
#     iteralabs/aetelier-validate:v0.1.0 \
#     --flush-threshold 3600 --grid-period-ms 100 --verbose
#
# Notes
# -----
# * `/data` is mounted read-only — the validator never mutates the lake.
# * `/state` is read-write and holds `validate_state.json` (cumulative)
#   plus `last_report.json` (per-run snapshot).
# * Exit code 0 = all 10 tests passed, 1 = at least one failed,
#   2 = fatal error (state file unreadable, etc.). Wire this directly
#   into a cron-driven monitor or alertmanager probe.

# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM rust:1.88-bookworm AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*


# Workspace manifests for layer caching (mirror md_worker.Dockerfile so the
# build cache is reusable across both images).
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
    && echo 'fn main(){}' > aetelier-sdk/aetelier-sdk/src/bin/validate.rs

RUN cd aetelier-sdk && cargo build --release \
        -p aetelier-sdk --bin validate --features parquet 2>/dev/null || true

# Real source.
COPY aetelier-sdk/    aetelier-sdk/

# Bust cargo's mtime fingerprint (same fix as md_worker.Dockerfile).
RUN find aetelier-sdk -type f \
        \( -name "*.rs" -o -name "Cargo.toml" \) -exec touch {} +

RUN cd aetelier-sdk && cargo build --release \
        -p aetelier-sdk --bin validate --features parquet \
    && strip target/release/validate

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime
LABEL aetelier.lifecycle="legacy" aetelier.component="aetelier-validate"

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

# UID 1000 matches the `collector` user from md_worker.Dockerfile, so a
# bind-mounted `/state` directory created by either image is writable
# by the other without `chown` gymnastics.
RUN useradd --create-home --shell /bin/bash --uid 1000 validator \
    && mkdir -p /data /state \
    && chown -R validator:validator /state

USER validator
WORKDIR /home/validator

COPY --from=builder /app/aetelier-sdk/target/release/validate /usr/local/bin/validate

ENV RUST_LOG=aetelier_sdk=info,validate=info
ENV TZ=UTC

# Default command: read /data, persist state under /state, surface
# tests + cumulative summary to stdout. Override flags at `docker run`.
ENTRYPOINT ["validate"]
CMD ["--data-dir", "/data", \
     "--state-file", "/state/validate_state.json", \
     "--report-out", "/state/last_report.json", \
     "--flush-threshold", "3600", \
     "--grid-period-ms", "100", \
     "--verbose"]
