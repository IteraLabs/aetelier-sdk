# aetelier-telemetry

Metrics and log surfaces for aetelier collectors: OpenTelemetry meter and tracing-subscriber wiring shared by the workers and binaries.

## Design

The crate owns the boundary between the engine's counters and the operator's observability stack. Collector counters (messages, gaps, resyncs, trade loss, recovery) live in `aetelier-connect`'s `SourceMetrics`; this crate exports the meters and exporters that surface them, plus the structured-log initialization every binary shares.

## Module map

- `exporters` — OTLP/stdout exporter construction and endpoint configuration.
- `meters` — meter registration for collector gauges and counters.
- `lib` — `init` entry points for binaries: tracing subscriber, log filtering, exporter lifecycle.

## Usage

Binaries initialize telemetry once at startup; workers then record through their shared handles. The `framework_live` example runs with plain stdout logging; the `md_worker` binary in `aetelier-sdk` wires the full exporter path.

## Tests

Unit tests cover exporter configuration parsing and meter registration; the workspace CI runs them on every push.
