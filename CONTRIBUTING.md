# Contributing Guide

Contributions are welcome. A change is ready for review when the full local gate passes — the same gate CI runs on every push:

```
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --workspace --all-targets --features aetelier-sdk/parquet -- -D warnings
cargo nextest run --workspace
cargo nextest run --workspace --features aetelier-sdk/parquet
cargo test --workspace --doc
cargo doc --workspace --no-deps
```

The parquet legs matter: feature-gated code compiles only there. The conformance suite (`cargo test -p aetelier-connect --test conformance --features parquet`) replays every venue's captured wire frames through the production decode path — a change that breaks any certified venue fails the matrix. Doc tests include every README code block, so documentation cannot drift from the API.

Tests follow the workspace testing tiers: unit tests ride their modules, integration tests live in each crate's consolidated `tests/` binary, and venue conformance is fixture-replay only — real captured frames, never synthesized ones.

## Lints

The workspace inherits these lints (every member sets `[lints] workspace = true`):

```toml
[workspace.lints.rust]
dead_code = "warn"
trivial_casts = "warn"
trivial_numeric_casts = "warn"
unexpected_cfgs = { level = "warn", check-cfg = ["cfg(nightly)"] }
unreachable_code = "deny"
unreachable_patterns = "deny"
unsafe_code = "forbid"
unused_extern_crates = "allow"
unused_variables = "warn"

[workspace.lints.clippy]
large_enum_variant = "allow"
too_many_arguments = "allow"
```

## rustfmt.toml

Consider all defaults to be present, and, the following changed:

```toml
reorder_modules = true
max_width = 90
```

## Code format with rustfmt

For the `aetelier-sdk` crate, there is a `rustfmt.toml` config file; even though most of the values are exactly the same as the default, they were included for future-proofing the formatting.
 
## Reporting a Bug

In the case that you've found a bug, please make sure you are able to answer the following:

```
- What version of Rust are you using?
- What version of the crate are you using?
- What operating system are you using?
- What did you do?
- What did you expect to see?
- What did you see instead? 
```

## Publishing to crates.io

The workspace crates carry `publish = false` until a maintainer flips it for a
release. Publish in dependency order — a crate cannot publish until every crate
it depends on is already on the registry:

1. `aetelier-types`
2. `aetelier-telemetry`
3. `aetelier-connect`
4. `aetelier-io` (its optional `connect` feature depends on `aetelier-connect`)
5. `aetelier-sdk`

`aetelier-connect` and `aetelier-io` reference each other only through
dev-dependencies (a cycle crates.io permits); path-only dev-deps are stripped at
publish, so the order above holds.

The workspace has no private dependencies (the platform agent and its wire
contract live in their own repos), so nothing structural blocks publishing.
Note: the crate names are reserved on crates.io as `0.0.0` placeholders, and
`cargo package` resolves intra-workspace dependencies against the registry —
so packaging a dependent crate succeeds only during the ordered publish above,
once its dependencies' real versions are live. To pre-check a crate's manifest
and file list without dependency resolution, use `cargo package -p <crate>
--list`.

Before a release, verify each crate packages cleanly (this skips the build so it
works while the crates still depend on each other by path):

```
cargo package -p <crate> --no-verify --allow-dirty
```

CI's `cargo hack check --each-feature` leg is the compensating build check that
`--no-verify` skips: it catches per-crate, per-feature breakage that a
workspace build hides through feature unification.

