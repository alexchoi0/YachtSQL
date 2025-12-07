# Test Utilities

This module provides utilities for testing Table results.

## Usage

Add to your test file:

```rust
mod common;
use common::assertions::{assert_batch_eq, build_batch};
```

## Example

### Before (Shallow Assertion):

```rust
#[test]
fn test_query() {
    let result = execute_query("SELECT id, name FROM users");
    assert_eq!(result.num_rows(), 2, "Should return 2 rows");
    // ❌ Doesn't verify actual values!
}
```

### After (Deep Assertion):

```rust
mod common;
use common::assertions::{assert_batch_eq, build_batch};
use yachtsql_storage::{Field, Schema};
use yachtsql_core::types::{DataType, Value};

#[test]
fn test_query() {
    let result = execute_query("SELECT id, name FROM users");

    let expected = build_batch(
        result.schema().clone(),
        vec![
            vec![Value::int64(1), Value::string("Alice".into())],
            vec![Value::int64(2), Value::string("Bob".into())],
        ]
    );

    assert_batch_eq(&result, &expected);
    // ✅ Verifies schema, row count, and every single value!
}
```

## API

### `assert_batch_eq(actual: &Table, expected: &Table)`

Asserts two RecordBatches are equal:

- Compares schemas
- Compares row counts
- Compares every value in every row
- Provides detailed error messages on mismatch

### `build_batch(schema: Schema, values: Vec<Vec<Value>>) -> Table`

Builds a Table from schema and values for testing.

## Examples

### Basic Usage

```rust
use common::assertions::{assert_batch_eq, build_batch};
use yachtsql_storage::{Field, Schema};
use yachtsql_core::types::{DataType, Value};

let schema = Schema::from_fields(vec![
    Field::required("id", DataType::Int64),
    Field::required("name", DataType::String),
]);

let batch = build_batch(
schema,
vec![
    vec![Value::int64(1), Value::string("Alice".into())],
    vec![Value::int64(2), Value::string("Bob".into())],
]
);

assert_eq!(batch.num_rows(), 2);
assert_eq!(batch.num_columns(), 2);
```

### With NULL Values

```rust
let schema = Schema::from_fields(vec![
    Field::nullable("id", DataType::Int64),
    Field::nullable("name", DataType::String),
]);

let batch = build_batch(
schema,
vec![
    vec![Value::int64(1), Value::string("Alice".into())],
    vec![Value::null(), Value::string("Bob".into())],
    vec![Value::int64(3), Value::null()],
]
);
```

### With Different Types

```rust
let schema = Schema::from_fields(vec![
    Field::required("id", DataType::Int64),
    Field::required("value", DataType::Float64),
    Field::required("active", DataType::Bool),
]);

let batch = build_batch(
schema,
vec![
    vec![Value::int64(1), Value::float64(3.14), Value::bool_val(true)],
    vec![Value::int64(2), Value::float64(2.71), Value::bool_val(false)],
]
);
```

## Benefits

- **Deep Validation**: Verifies every single value, not just row counts
- **Clear Error Messages**: When tests fail, you get detailed information about what differed
- **Schema Validation**: Ensures the structure matches expectations
- **Type Safety**: Compile-time guarantees about the data you're testing
