#![allow(clippy::approx_constant)]
mod common;
use common::assertions::{assert_batch_eq, build_batch};
use yachtsql_common::types::{DataType, Value};
use yachtsql_storage::{Field, Schema};

#[test]
fn test_assert_batch_eq_identical_batches() {
    let schema = Schema::from_fields(vec![
        Field::required("id", DataType::Int64),
        Field::required("name", DataType::String),
    ]);

    let batch1 = build_batch(
        schema.clone(),
        vec![
            vec![Value::int64(1), Value::string("Alice".into())],
            vec![Value::int64(2), Value::string("Bob".into())],
            vec![Value::int64(3), Value::string("Charlie".into())],
        ],
    );

    let batch2 = build_batch(
        schema,
        vec![
            vec![Value::int64(1), Value::string("Alice".into())],
            vec![Value::int64(2), Value::string("Bob".into())],
            vec![Value::int64(3), Value::string("Charlie".into())],
        ],
    );

    assert_batch_eq(&batch1, &batch2);
}

#[test]
fn test_assert_batch_eq_with_different_types() {
    let schema = Schema::from_fields(vec![
        Field::required("id", DataType::Int64),
        Field::required("value", DataType::Float64),
        Field::required("active", DataType::Bool),
    ]);

    let batch1 = build_batch(
        schema.clone(),
        vec![
            vec![Value::int64(1), Value::float64(3.14), Value::bool_val(true)],
            vec![
                Value::int64(2),
                Value::float64(2.71),
                Value::bool_val(false),
            ],
        ],
    );

    let batch2 = build_batch(
        schema,
        vec![
            vec![Value::int64(1), Value::float64(3.14), Value::bool_val(true)],
            vec![
                Value::int64(2),
                Value::float64(2.71),
                Value::bool_val(false),
            ],
        ],
    );

    assert_batch_eq(&batch1, &batch2);
}

#[test]
#[should_panic(expected = "VALUE MISMATCHES")]
fn test_assert_batch_eq_detects_value_mismatch() {
    let schema = Schema::from_fields(vec![
        Field::required("id", DataType::Int64),
        Field::required("name", DataType::String),
    ]);

    let batch1 = build_batch(
        schema.clone(),
        vec![
            vec![Value::int64(1), Value::string("Alice".into())],
            vec![Value::int64(2), Value::string("Bob".into())],
        ],
    );

    let batch2 = build_batch(
        schema,
        vec![
            vec![Value::int64(1), Value::string("Alice".into())],
            vec![Value::int64(2), Value::string("Bobby".into())],
        ],
    );

    assert_batch_eq(&batch1, &batch2);
}

#[test]
#[should_panic(expected = "ROW COUNT MISMATCH")]
fn test_assert_batch_eq_detects_row_count_mismatch() {
    let schema = Schema::from_fields(vec![Field::required("id", DataType::Int64)]);

    let batch1 = build_batch(
        schema.clone(),
        vec![
            vec![Value::int64(1)],
            vec![Value::int64(2)],
            vec![Value::int64(3)],
        ],
    );

    let batch2 = build_batch(schema, vec![vec![Value::int64(1)], vec![Value::int64(2)]]);

    assert_batch_eq(&batch1, &batch2);
}

#[test]
fn test_build_batch_creates_valid_batch() {
    let schema = Schema::from_fields(vec![
        Field::required("id", DataType::Int64),
        Field::required("value", DataType::Float64),
    ]);

    let batch = build_batch(
        schema,
        vec![
            vec![Value::int64(1), Value::float64(1.5)],
            vec![Value::int64(2), Value::float64(2.5)],
            vec![Value::int64(3), Value::float64(3.5)],
        ],
    );

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);
}

#[test]
fn test_build_batch_empty() {
    let schema = Schema::from_fields(vec![Field::required("id", DataType::Int64)]);

    let batch = build_batch(schema, vec![]);

    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.num_columns(), 1);
}

#[test]
fn test_build_batch_with_nulls() {
    let schema = Schema::from_fields(vec![
        Field::nullable("id", DataType::Int64),
        Field::nullable("name", DataType::String),
    ]);

    let batch1 = build_batch(
        schema.clone(),
        vec![
            vec![Value::int64(1), Value::string("Alice".into())],
            vec![Value::null(), Value::string("Bob".into())],
            vec![Value::int64(3), Value::null()],
        ],
    );

    let batch2 = build_batch(
        schema,
        vec![
            vec![Value::int64(1), Value::string("Alice".into())],
            vec![Value::null(), Value::string("Bob".into())],
            vec![Value::int64(3), Value::null()],
        ],
    );

    assert_batch_eq(&batch1, &batch2);
}
