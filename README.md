# YachtSQL

An in-memory test database for Rust programs. YachtSQL emulates SQL features for PostgreSQL, BigQuery, and ClickHouse—handy for unit tests and prototyping without spinning up real database servers.

## Quick Start

```rust
use yachtsql::{QueryExecutor, DialectType};

fn main() -> yachtsql::Result<()> {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor.execute_sql("
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT
        )
    ")?;

    executor.execute_sql("
        INSERT INTO users (name, email) VALUES
            ('Alice', 'alice@example.com'),
            ('Bob', 'bob@example.com')
    ")?;

    let results = executor.execute_sql("SELECT * FROM users")?;
    println!("{:?}", results);
    Ok(())
}
```

Switch dialects as needed:

```rust
// BigQuery
let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

// ClickHouse
let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);
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

Each dialect has its own quirks implemented—ClickHouse gets MergeTree engines and functions like `toStartOfMonth()`, BigQuery gets `STRUCT()` and `ARRAY_AGG()`, PostgreSQL gets range types and `SERIAL`.

## Project Structure

```
yachtsql/
├── crates/
│   ├── core/           # Types, errors, values
│   ├── storage/        # Columnar storage, schemas, indexes, MVCC
│   ├── parser/         # Multi-dialect SQL parsing
│   ├── ir/             # Intermediate representation (logical plan)
│   ├── optimizer/      # Query optimization rules
│   ├── executor/       # Physical execution engine
│   ├── functions/      # SQL function implementations
│   ├── capability/     # SQL feature registry
│   ├── dialects/       # Dialect-specific behavior
│   └── test-utils/     # Testing utilities and macros
├── tests/              # Integration tests
│   ├── bigquery/
│   ├── clickhouse/
│   └── postgresql/
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
