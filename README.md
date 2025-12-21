# YachtSQL

An in-memory test database for Rust programs. YachtSQL emulates BigQuery SQL features—handy for unit tests and prototyping without spinning up real database servers.

## Quick Start

```rust
use yachtsql::YachtSQLEngine;

fn main() -> yachtsql::Result<()> {
    let mut engine = YachtSQLEngine::new();

    engine.execute_sql("
        CREATE TABLE users (
            id INT64,
            name STRING NOT NULL,
            email STRING
        )
    ")?;

    engine.execute_sql("
        INSERT INTO users (id, name, email) VALUES
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com')
    ")?;

    let results = engine.execute_sql("SELECT * FROM users")?;
    println!("{:?}", results);
    Ok(())
}
```

## Installation

```toml
[dependencies]
yachtsql = { git = "https://github.com/alexchoi0/yachtSQL" }
```

Requires Rust nightly (edition 2024).

## What's Supported

YachtSQL covers a good chunk of SQL:

- **Queries**: Joins (inner, left, right, full, cross, lateral), subqueries, CTEs, window functions, aggregations, set operations
- **DDL**: CREATE/DROP/ALTER for tables, views, indexes
- **DML**: INSERT, UPDATE, DELETE, MERGE, TRUNCATE
- **Types**: The usual suspects (integers, floats, strings, dates) plus arrays, structs, JSON, and more

BigQuery-specific features like `STRUCT()`, `ARRAY_AGG()`, `SAFE_DIVIDE()`, and `INT64`/`FLOAT64`/`STRING` types are supported.

## Project Structure

```
yachtsql/
├── crates/
│   ├── core/           # Types, errors, values
│   ├── storage/        # Columnar storage, schemas, indexes, MVCC
│   ├── parser/         # SQL parsing
│   ├── ir/             # Intermediate representation (logical plan)
│   ├── optimizer/      # Query optimization rules
│   ├── executor/       # Physical execution engine
│   ├── functions/      # SQL function implementations
│   ├── capability/     # SQL feature registry
│   └── test-utils/     # Testing utilities and macros
├── tests/              # Integration tests
│   └── bigquery/
└── benches/            # Performance benchmarks
```

## Building & Testing

```bash
cargo build
cargo nextest run
cargo bench
```

## License

MIT OR Apache-2.0
