# Contributing

## Getting started

```bash
git clone https://github.com/YOUR_USER/anigraph
cd anigraph
cp .env.example .env   # see docs/environment.md
cargo build --release
cargo test
```

## Project structure

```
src/                        # CLI binary (main.rs)
crates/
  model/                    # Data types — zero I/O, serde-only
  providers/                # External API clients (AniList)
  pipeline/                 # Pipeline orchestration + enrichment phases
docs/
  research/                 # API research notes (gitignored)
```

## Making changes

1. Run `cargo test` before and after
2. Keep phases idempotent — checkpointing handles resume
3. Add tests for new model types or mapping functions
4. Run `cargo clippy` if available

## Code style

- Rust 2024 edition
- `camelCase` for serde serialization
- `snake_case` for Rust identifiers
- Follow existing patterns for phase structure, error handling, rate limiting

## Adding a new provider

1. Add the phase type in `crates/pipeline/src/<name>.rs`
2. Implement the `Phase` trait
3. Wire it into `runner.rs`
4. Add stats types to `stats.rs`
5. Add CLI flags to `src/main.rs` if needed
