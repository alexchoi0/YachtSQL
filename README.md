# YachtSQL

A lightweight, in-memory SQL database with columnar storage for Rust programs. YachtSQL currently supports PostgreSQL,
Bigquery, and Clickhouse SQL features

## Features

- **SQL:2023 Compliance** - Modern SQL standard support
- **Multi-Dialect** - PostgreSQL, BigQuery, and ClickHouse dialects
- **Columnar Storage** - SIMD-optimized columnar engine with optional row storage
- **MVCC Transactions** - Full transaction support with multiple isolation levels
- **Query Optimization** - Multi-phase rule-based optimizer with cost model
- **Advanced SQL** - CTEs, window functions, lateral joins, MATCH RECOGNIZE, temporal queries

## Requirements

- Rust nightly (1.91.0+)

## Getting Started

Add YachtSQL to your `Cargo.toml`:

```toml
[dependencies]
yachtsql = { path = "path/to/yachtsql" }
```

### Basic Usage

```rust
use yachtsql::{QueryExecutor, Result};

fn main() -> Result<()> {
    let mut executor = QueryExecutor::new();

    executor.execute_sql("CREATE TABLE users (id INT, name TEXT)")?;
    executor.execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")?;

    let results = executor.execute_sql("SELECT * FROM users WHERE id = 1")?;
    println!("{:?}", results);

    Ok(())
}
```

## Building

```bash
# Build all crates
cargo build

# Build release
cargo build --release
```

## Testing

```bash
# Run all tests
cargo test

# Run tests for a specific crate
cargo test -p yachtsql-executor

# Run with output
cargo test -- --nocapture
```

## Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench --bench basic_operations
```

## Project Structure

```
yachtsql/
├── crates/
│   ├── core/           # Core types and error handling
│   ├── parser/         # SQL parsing (multi-dialect)
│   ├── ir/             # Intermediate representation
│   ├── storage/        # Columnar/row storage, MVCC, indexes
│   ├── executor/       # Query execution engine
│   ├── optimizer/      # Query optimization
│   ├── functions/      # SQL function implementations
│   ├── capability/     # SQL:2023 feature registry
│   ├── dialects/       # Dialect-specific implementations
│   └── test-utils/     # Testing utilities
├── tests/              # Integration tests
└── benches/            # Performance benchmarks
```

## Supported Data Types

| Category | Types                           |
|----------|---------------------------------|
| Numeric  | INT64, FLOAT64, DECIMAL         |
| Text     | STRING, TEXT                    |
| Boolean  | BOOLEAN                         |
| Temporal | DATE, TIME, TIMESTAMP, INTERVAL |
| Complex  | ARRAY, JSON                     |
| Other    | UUID, ENUM, composite types     |

## License

MIT OR Apache-2.0
