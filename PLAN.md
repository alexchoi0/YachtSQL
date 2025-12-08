# Worker 1: PostgreSQL Functions

## Objective
Implement ignored tests in PostgreSQL function modules.

## Files to Work On
1. `tests/postgresql/functions/aggregate.rs` (45 ignored tests)
2. `tests/postgresql/functions/system.rs` (37 ignored tests)
3. `tests/postgresql/functions/network.rs` (35 ignored tests)

## Total: ~117 ignored tests

## Instructions
1. For each ignored test, remove the `#[ignore = "Implement me!"]` attribute
2. Run the test to see what's missing
3. Implement the missing functionality in the executor
4. Ensure the test passes before moving to the next one

## Key Areas
- **Aggregate functions**: Advanced aggregates like `PERCENTILE_CONT`, `PERCENTILE_DISC`, `MODE`, ordered-set aggregates
- **System functions**: `pg_*` system catalog functions, session info functions
- **Network functions**: `inet`, `cidr` operations, network address manipulation

## Running Tests
```bash
cargo test --test postgresql functions::aggregate
cargo test --test postgresql functions::system
cargo test --test postgresql functions::network
```

## Notes
- Do NOT simply add `#[ignore]` to failing tests
- Implement the missing features rather than skipping tests
- Check existing implementations in `crates/executor/src/` for patterns
