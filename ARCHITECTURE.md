# YachtSQL Architecture

## Overview

YachtSQL is a BigQuery-compatible SQL engine written in Rust. It provides a complete SQL execution pipeline from parsing to execution, with support for:

- DDL (CREATE/DROP/ALTER TABLE, VIEW, SCHEMA, FUNCTION)
- DML (SELECT, INSERT, UPDATE, DELETE, MERGE)
- Analytical queries (window functions, CTEs, set operations)
- Scripting (DECLARE, IF/ELSE, WHILE, LOOP, FOR)
- User-defined functions (JavaScript UDFs)

## Crate Structure

```
crates/
├── common/         # Shared types (DataType, Value, Error)
├── storage/        # In-memory table storage (Table, Column, Record, Schema)
├── ir/             # Intermediate representation (LogicalPlan, Expr)
├── parser/         # SQL parsing and logical planning (using sqlparser-rs)
├── optimizer/      # Physical plan generation (PhysicalPlan)
├── executor/       # Query execution engine (PlanExecutor, Catalog, Session)
├── functions/      # Built-in function implementations
└── test-utils/     # Testing utilities and assertion helpers
```

## Three-Tier Plan Structure

YachtSQL uses a three-tier plan architecture:

```
SQL Text
    │
    ▼
┌─────────────┐
│   Parser    │  (sqlparser-rs BigQueryDialect)
└─────────────┘
    │
    ▼
┌─────────────┐
│  Planner    │  (yachtsql_parser)
└─────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│ LogicalPlan (crates/ir/src/plan/mod.rs)                         │
│ - Declarative representation of the query                       │
│ - Contains: Scan, Filter, Project, Aggregate, Join, Sort, etc.  │
│ - Preserves original query semantics                            │
└─────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│ PhysicalPlan (crates/optimizer/src/physical_plan.rs)            │
│ - Physical operators (e.g., NestedLoopJoin, HashAggregate)      │
│ - TopN optimization (Sort + Limit → TopN)                       │
│ - CrossJoin for cartesian products                              │
└─────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│ ExecutorPlan (crates/executor/src/plan.rs)                      │
│ - Executor-specific plan representation                         │
│ - 1:1 mapping from PhysicalPlan (converted via from_physical)   │
│ - Enables future executor-specific optimizations                │
└─────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────┐
│ PlanExecutor│
└─────────────┘
    │
    ▼
  Table (result)
```

### LogicalPlan Nodes

Key logical plan nodes (defined in `crates/ir/src/plan/mod.rs`):

| Node | Description |
|------|-------------|
| `Scan` | Read from a table |
| `Filter` | Apply predicate to filter rows |
| `Project` | Compute expressions, select columns |
| `Aggregate` | GROUP BY with aggregate functions |
| `Join` | Join two inputs with condition |
| `Sort` | Order by expressions |
| `Limit` | LIMIT and OFFSET |
| `Distinct` | Remove duplicate rows |
| `Window` | Window function computation |
| `WithCte` | Common Table Expression |
| `Unnest` | Flatten arrays |
| `SetOperation` | UNION, INTERSECT, EXCEPT |
| `Values` | Inline row values |
| `Qualify` | Filter on window function results |

### PhysicalPlan Optimizations

The physical planner applies optimizations:

- **TopN**: `Sort + Limit` → `TopN` (partial sort)
- **CrossJoin**: Separates cross joins from conditional joins
- **HashAggregate**: All aggregates use hash-based implementation

## Expression System

Expressions are defined in `crates/ir/src/expr/mod.rs`:

```rust
pub enum Expr {
    Literal(Literal),
    Column { table, name, index },
    BinaryOp { left, op, right },
    UnaryOp { op, expr },
    ScalarFunction { name, args },
    Aggregate { func, args, distinct, filter, order_by, limit, ignore_nulls },
    Window { func, args, partition_by, order_by, frame },
    Case { operand, when_clauses, else_result },
    Cast { expr, data_type, safe },
    // ... 20+ more variants
}
```

### Expression Evaluation

The `IrEvaluator` (in `crates/executor/src/ir_evaluator.rs`) evaluates expressions:

1. Takes a `Record` (row of values) and an `Expr`
2. Recursively evaluates the expression tree
3. Returns a `Value`

Key evaluation patterns:

- **Columns**: Resolved by name or index against the record
- **Functions**: Dispatched via large match statements
- **Aggregates/Windows**: Handled separately by specialized executors

## Catalog Structure

The `Catalog` (in `crates/executor/src/catalog.rs`) manages metadata:

```rust
pub struct Catalog {
    tables: HashMap<String, Table>,
    table_defaults: HashMap<String, Vec<ColumnDefault>>,
    functions: HashMap<String, UserFunction>,
    procedures: HashMap<String, UserProcedure>,
    views: HashMap<String, ViewDef>,
    schemas: HashSet<String>,
    schema_metadata: HashMap<String, SchemaMetadata>,
    search_path: Vec<String>,
}
```

Features:

- **Case-insensitive**: All names normalized to uppercase
- **Schema support**: Qualified names (schema.table)
- **Search path**: Unqualified name resolution
- **Views**: Stored as query strings, expanded at planning time

## Session Management

The `Session` (in `crates/executor/src/session.rs`) holds:

```rust
pub struct Session {
    variables: HashMap<String, Value>,       // Script variables
    system_variables: HashMap<String, Value>, // @@time_zone, etc.
    current_schema: Option<String>,
}
```

Sessions are per-query context for:

- Script variable storage (DECLARE/SET)
- System variable access
- Current schema tracking

## Data Types

Core types in `crates/common/src/types/mod.rs`:

| Type | Rust Type | Description |
|------|-----------|-------------|
| `Bool` | `bool` | Boolean |
| `Int64` | `i64` | 64-bit integer |
| `Float64` | `OrderedFloat<f64>` | 64-bit float (NaN-safe) |
| `Numeric` | `rust_decimal::Decimal` | Arbitrary precision decimal |
| `String` | `String` | UTF-8 string |
| `Bytes` | `Vec<u8>` | Binary data |
| `Date` | `NaiveDate` | Calendar date |
| `Time` | `NaiveTime` | Time of day |
| `DateTime` | `NaiveDateTime` | Date and time (no timezone) |
| `Timestamp` | `DateTime<Utc>` | Date and time (with timezone) |
| `Array` | `Vec<Value>` | Homogeneous array |
| `Struct` | `Vec<(String, Value)>` | Named tuple |
| `Json` | `serde_json::Value` | JSON document |
| `Geography` | `String` | GeoJSON (WKT format) |
| `Interval` | `IntervalValue` | Duration (months, days, nanos) |
| `Range` | `RangeValue` | Value range with bounds |

## Storage Layer

In-memory columnar storage (in `crates/storage/src/`):

### Table

```rust
pub struct Table {
    schema: Schema,
    columns: Vec<Column>,
}
```

- Columnar storage with nullable values
- Row-based insert/update API
- Row iteration and column access

### Schema

```rust
pub struct Schema {
    fields: Vec<Field>,
}

pub struct Field {
    name: String,
    data_type: DataType,
    mode: FieldMode,  // Required, Nullable, Repeated
}
```

## Executor Components

The `PlanExecutor` delegates to specialized sub-executors:

| Module | Responsibility |
|--------|----------------|
| `scan.rs` | Table scans, view expansion |
| `filter.rs` | Row filtering with predicates |
| `project.rs` | Expression projection |
| `aggregate.rs` | Hash aggregation |
| `join.rs` | Nested loop and cross joins |
| `window.rs` | Window function computation |
| `sort.rs` | Sorting with multi-key support |
| `limit.rs` | LIMIT/OFFSET |
| `distinct.rs` | Duplicate elimination |
| `set_ops.rs` | UNION, INTERSECT, EXCEPT |
| `cte.rs` | CTE materialization |
| `unnest.rs` | Array flattening |
| `qualify.rs` | Window function result filtering |
| `dml.rs` | INSERT, UPDATE, DELETE, MERGE |
| `ddl.rs` | CREATE/DROP/ALTER operations |
| `scripting.rs` | IF, WHILE, LOOP, FOR, DECLARE |

## Built-in Functions

Functions are implemented in two places:

1. **IrEvaluator** (`crates/executor/src/ir_evaluator.rs`): Inline evaluation
2. **functions crate** (`crates/functions/src/`): Reusable implementations

### Scalar Functions

Categories: String, Math, Date/Time, Conversion, Conditional, Array, Struct, JSON, Regex, Hash, Net, Bit

### Aggregate Functions

- `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- `ARRAY_AGG`, `STRING_AGG`
- `ANY_VALUE`
- Statistical: `STDDEV`, `VARIANCE`, `CORR`
- Approximate: `APPROX_COUNT_DISTINCT`

### Window Functions

- Ranking: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`
- Navigation: `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`
- Aggregate windows: Any aggregate with `OVER` clause

## Extension Points

### Adding a New Scalar Function

1. Add variant to `ScalarFunction` enum (`crates/ir/src/expr/functions.rs`)
2. Add parsing in planner (`crates/parser/src/expr_planner.rs`)
3. Implement evaluation in `IrEvaluator` (`crates/executor/src/ir_evaluator.rs`)

### Adding a New Aggregate Function

1. Add variant to `AggregateFunction` enum (`crates/ir/src/expr/functions.rs`)
2. Implement `AggregateState` trait (`crates/functions/src/aggregate.rs`)
3. Add handling in aggregate executor (`crates/executor/src/executor/aggregate.rs`)

### Adding a New Plan Node

1. Add variant to `LogicalPlan` (`crates/ir/src/plan/mod.rs`)
2. Add variant to `PhysicalPlan` (`crates/optimizer/src/physical_plan.rs`)
3. Add variant to `ExecutorPlan` (`crates/executor/src/plan.rs`)
4. Add planning logic (`crates/parser/src/planner.rs`)
5. Add physical planning (`crates/optimizer/src/planner.rs`)
6. Add execution logic in appropriate executor module

## Common Patterns

### Error Handling

Use `yachtsql_common::error::Result<T>` throughout:

```rust
use yachtsql_common::error::{Error, Result};

fn my_function() -> Result<Value> {
    Err(Error::InvalidQuery("reason".into()))
}
```

Error types:

- `InvalidQuery`: User-facing SQL errors
- `TypeMismatch`: Type-related errors
- `TableNotFound`: Missing table/view
- `DivisionByZero`: Arithmetic error
- `UnsupportedFeature`: Not-yet-implemented

### Adding Tests

Tests go in `tests/bigquery/`. Use the test utilities:

```rust
use crate::common::*;

#[test]
fn my_test() {
    let mut exec = create_executor();
    exec_ok(&mut exec, "CREATE TABLE t (id INT64)");
    let result = query(&mut exec, "SELECT * FROM t");
    assert_batch_values(&result, vec![]);
}
```

Key test helpers:

- `create_executor()`: Fresh QueryExecutor
- `exec_ok(exec, sql)`: Execute, panic on error
- `query(exec, sql)`: Execute and return result
- `assert_batch_values(result, expected)`: Compare results
- `assert_error_contains(result, keywords)`: Verify error messages

## Performance Considerations

- **No query optimization**: Plans execute as specified
- **No indexes**: Full table scans only
- **In-memory only**: No disk persistence
- **Single-threaded**: No parallel execution
- **Hash aggregation**: O(n) grouping
- **Nested loop joins**: O(n*m) complexity

This is designed for testing and prototyping, not production workloads.
