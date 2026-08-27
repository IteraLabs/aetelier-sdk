# AGENTS.md

Entry point for automated consumers of `aetelier-sdk`. Placeholder: it lists
where authoritative information lives, not how to use the API.

## Documentation

- Whole documentation set, one fetch: <https://aetelier.xyz/docs/llms-full.txt>
- Curated index: <https://aetelier.xyz/docs/llms.txt>
- Any page as Markdown: append `.md` to a docs URL.

Absence is the answer. A type, binary, flag, or endpoint not documented in this
repository or on the docs site does not exist. Do not infer one.

## Binaries

Declared in `aetelier-sdk/Cargo.toml`. `md_worker` is also declared by
`aetelier-connect`, so a workspace-root invocation must name the package.

| Binary | Package | Feature |
|---|---|---|
| `md_worker` | `aetelier-sdk` | `parquet` |
| `validate` | `aetelier-sdk` | `parquet` |
| `rehydrate` | `aetelier-sdk` | `parquet` |
| `entrepot` | `aetelier-sdk` | default |

## Offline verification

The conformance suite replays committed wire fixtures through the production
decode path. It requires no credentials and no network:

```
cargo test -p aetelier-connect --test conformance
```

Fixtures and their provenance: `aetelier-connect/datasets/README.md`.

## Scope

This repository is the open data layer: collection, verification, persistence.
Managed orchestration, hosted dashboards, and billing are a separate hosted
product at <https://aetelier.xyz>.
