//! Testing utilities for YachtSQL.

#![allow(dead_code)]
#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![warn(rustdoc::broken_intra_doc_links)]
#![allow(missing_docs)]

use yachtsql::{QueryExecutor, Result, Schema, Table, Value};
use yachtsql_parser::DialectType;
use yachtsql_storage::Row;

pub fn setup_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::PostgreSQL)
}

pub fn assert_batch_empty(batch: &yachtsql::Table) {
    assert_eq!(
        batch.num_rows(),
        0,
        "expected zero rows but found {}",
        batch.num_rows()
    );
    assert!(
        batch.schema().fields().is_empty(),
        "expected zero columns but schema has {} fields",
        batch.schema().fields().len()
    );
    if let Some(cols) = batch.columns() {
        assert!(
            cols.is_empty(),
            "expected zero column buffers but found {}",
            cols.len()
        );
    }
}

pub fn assert_float_eq(actual: f64, expected: f64, epsilon: f64) {
    let diff = (actual - expected).abs();
    assert!(
        diff < epsilon,
        "Float values not equal within epsilon: actual={}, expected={}, diff={}, epsilon={}",
        actual,
        expected,
        diff,
        epsilon
    );
}

pub fn assert_error_contains<T>(result: Result<T>, keywords: &[&str]) {
    match result {
        Ok(_) => panic!("Expected error but got Ok result"),
        Err(e) => {
            let error_msg = e.to_string().to_lowercase();
            let found = keywords
                .iter()
                .any(|keyword| error_msg.contains(&keyword.to_lowercase()));
            assert!(
                found,
                "Error message '{}' does not contain any of the expected keywords: {:?}",
                e, keywords
            );
        }
    }
}

pub fn create_table_with_schema(
    executor: &mut QueryExecutor,
    table_name: &str,
    columns: &[(&str, &str)],
) -> Result<()> {
    let columns_def = columns
        .iter()
        .map(|(name, type_)| format!("{} {}", name, type_))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("CREATE TABLE {} ({})", table_name, columns_def);
    executor.execute_sql(&sql)?;
    Ok(())
}

pub fn insert_rows(
    executor: &mut QueryExecutor,
    table_name: &str,
    rows: Vec<Vec<&str>>,
) -> Result<()> {
    for row in rows {
        let values = row.join(", ");
        let sql = format!("INSERT INTO {} VALUES ({})", table_name, values);
        executor.execute_sql(&sql)?;
    }
    Ok(())
}

pub fn setup_table_with_id_values(
    executor: &mut QueryExecutor,
    table_name: &str,
    data: &[(i64, f64)],
) -> Result<()> {
    executor.execute_sql(&format!(
        "CREATE TABLE {} (id INT64, value FLOAT64)",
        table_name
    ))?;
    for (id, value) in data {
        executor.execute_sql(&format!(
            "INSERT INTO {} VALUES ({}, {})",
            table_name, id, value
        ))?;
    }
    Ok(())
}

pub fn assert_query_float_eq(executor: &mut QueryExecutor, sql: &str, expected: f64, epsilon: f64) {
    let batch = executor
        .execute_sql(sql)
        .expect("Query execution should succeed");
    assert_eq!(batch.num_rows(), 1);
    let column = batch.column(0).expect("Column 0 not found");
    let value = column.get(0).expect("Row 0 should exist");
    if let Some(f) = value.as_f64() {
        assert_float_eq(f, expected, epsilon);
    } else {
        panic!("Expected Float64, got {:?}", value);
    }
}

pub fn get_string_value(batch: &yachtsql::Table, row: usize, col: usize) -> Option<String> {
    if let Some(column) = batch.column(col)
        && let Ok(value) = column.get(row)
        && let Some(s) = value.as_str()
    {
        return Some(s.to_string());
    }
    None
}

pub fn build_repeated_expression(expr: &str, count: usize, separator: &str) -> String {
    vec![expr; count].join(separator)
}

pub fn build_nested_expression(wrapper: &str, inner: &str, depth: usize) -> String {
    let mut result = inner.to_string();
    for _ in 0..depth {
        result = format!("{} ({})", wrapper, result);
    }
    result
}

pub fn setup_bool_table() -> QueryExecutor {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    executor
        .execute_sql("CREATE TABLE bools (id INT64, val BOOL)")
        .expect("CREATE TABLE should succeed");
    executor
}

pub fn insert_bool(executor: &mut QueryExecutor, id: i64, val: Option<bool>) {
    let val_str = match val {
        Some(true) => "TRUE",
        Some(false) => "FALSE",
        None => "NULL",
    };
    executor
        .execute_sql(&format!("INSERT INTO bools VALUES ({}, {})", id, val_str))
        .expect("INSERT should succeed");
}

pub fn assert_query_bool(executor: &mut QueryExecutor, sql: &str, expected: Option<bool>) {
    let batch = executor
        .execute_sql(sql)
        .expect("Query execution should succeed");
    assert_eq!(batch.num_rows(), 1);
    let column = batch.column(0).expect("Column 0 not found");
    let value = column.get(0).expect("Row 0 should exist");
    match expected {
        Some(exp_bool) => {
            if let Some(b) = value.as_bool() {
                assert_eq!(b, exp_bool);
            } else {
                panic!("Expected Bool({:?}), got {:?}", exp_bool, value);
            }
        }
        None => {
            if !value.is_null() {
                panic!("Expected NULL, got {:?}", value);
            }
        }
    }
}

pub fn setup_table_with_float_values(
    executor: &mut QueryExecutor,
    table_name: &str,
    values: &[f64],
) -> Result<()> {
    executor.execute_sql(&format!("CREATE TABLE {} (value FLOAT64)", table_name))?;
    for value in values {
        executor.execute_sql(&format!("INSERT INTO {} VALUES ({})", table_name, value))?;
    }
    Ok(())
}

pub fn assert_query_null(executor: &mut QueryExecutor, sql: &str) {
    let batch = executor
        .execute_sql(sql)
        .expect("Query execution should succeed");
    assert_eq!(batch.num_rows(), 1);
    let column = batch.column(0).expect("Column 0 not found");
    let value = column.get(0).expect("Row 0 should exist");
    assert!(value.is_null(), "Expected NULL, got {:?}", value);
}

pub fn assert_query_null_simple(executor: &mut QueryExecutor, sql: &str) {
    assert_query_null(executor, sql);
}

pub fn assert_query_error(executor: &mut QueryExecutor, sql: &str, keywords: &[&str]) {
    assert_error_contains(executor.execute_sql(sql), keywords);
}

pub fn assert_row_count(batch: &yachtsql::Table, expected_count: usize) {
    assert_eq!(
        batch.num_rows(),
        expected_count,
        "Expected {} rows, but got {}",
        expected_count,
        batch.num_rows()
    );
}

pub fn new_executor() -> QueryExecutor {
    setup_executor()
}

pub fn table_exists(executor: &mut QueryExecutor, table_name: &str) -> bool {
    let query = format!("SELECT COUNT(*) FROM {}", table_name);
    executor.execute_sql(&query).is_ok()
}

pub fn assert_error_contains_with_context<T>(result: Result<T>, keywords: &[&str], context: &str) {
    match result {
        Ok(_) => panic!("{}: Expected error but got Ok result", context),
        Err(e) => {
            let error_msg = e.to_string().to_lowercase();
            let found = keywords
                .iter()
                .any(|keyword| error_msg.contains(&keyword.to_lowercase()));
            assert!(
                found,
                "{}: Error '{}' does not contain any of the expected keywords: {:?}",
                context, e, keywords
            );
        }
    }
}

pub fn get_i64(result: &yachtsql::Table, col: usize, row: usize) -> i64 {
    result
        .column(col)
        .unwrap_or_else(|| panic!("Column {} not found", col))
        .get(row)
        .unwrap_or_else(|e| panic!("Failed to get row {} in column {}: {}", row, col, e))
        .as_i64()
        .unwrap_or_else(|| panic!("Value at row {} col {} is not INT64", row, col))
}

pub fn get_string(result: &yachtsql::Table, col: usize, row: usize) -> String {
    let value = result
        .column(col)
        .unwrap_or_else(|| panic!("Column {} not found", col))
        .get(row)
        .unwrap_or_else(|e| panic!("Failed to get row {} in column {}: {}", row, col, e));

    if let Some(b) = value.as_bool() {
        return if b {
            "YES".to_string()
        } else {
            "NO".to_string()
        };
    }

    if let Some(s) = value.as_str() {
        return s.to_string();
    }

    panic!("Expected STRING or BOOL value, got {:?}", value)
}

pub fn get_f64(result: &yachtsql::Table, col: usize, row: usize) -> f64 {
    result
        .column(col)
        .unwrap_or_else(|| panic!("Column {} not found", col))
        .get(row)
        .unwrap_or_else(|e| panic!("Failed to get row {} in column {}: {}", row, col, e))
        .as_f64()
        .unwrap_or_else(|| panic!("Value at row {} col {} is not FLOAT64", row, col))
}

pub fn get_bool(result: &yachtsql::Table, col: usize, row: usize) -> bool {
    result
        .column(col)
        .unwrap_or_else(|| panic!("Column {} not found", col))
        .get(row)
        .unwrap_or_else(|e| panic!("Failed to get row {} in column {}: {}", row, col, e))
        .as_bool()
        .unwrap_or_else(|| panic!("Value at row {} col {} is not BOOL", row, col))
}

pub fn get_numeric(result: &yachtsql::Table, col: usize, row: usize) -> rust_decimal::Decimal {
    let value = result
        .column(col)
        .unwrap_or_else(|| panic!("Column {} not found", col))
        .get(row)
        .unwrap_or_else(|e| panic!("Failed to get row {} in column {}: {}", row, col, e));

    if let Some(d) = value.as_numeric() {
        return d;
    }

    if let Some(i) = value.as_i64() {
        return rust_decimal::Decimal::from(i);
    }

    panic!(
        "Expected NUMERIC value at row {} col {}, got {:?}",
        row, col, value
    )
}

pub fn assert_numeric_eq(result: &yachtsql::Table, col: usize, row: usize, expected: &str) {
    use std::str::FromStr;
    let actual = get_numeric(result, col, row);
    let expected_decimal = rust_decimal::Decimal::from_str(expected)
        .unwrap_or_else(|e| panic!("Invalid expected decimal '{}': {}", expected, e));
    assert_eq!(
        actual, expected_decimal,
        "Numeric value mismatch at row {} col {}: expected '{}', got '{}'",
        row, col, expected, actual
    );
}

pub fn is_null(result: &yachtsql::Table, col: usize, row: usize) -> bool {
    result
        .column(col)
        .unwrap_or_else(|| panic!("Column {} not found", col))
        .get(row)
        .unwrap_or_else(|e| panic!("Failed to get row {} in column {}: {}", row, col, e))
        .is_null()
}

fn find_column_index(result: &yachtsql::Table, col_name: &str) -> usize {
    result
        .schema()
        .fields()
        .iter()
        .position(|f| f.name == col_name)
        .unwrap_or_else(|| panic!("Column '{}' not found in schema", col_name))
}

pub fn get_i64_by_name(result: &yachtsql::Table, row: usize, col_name: &str) -> i64 {
    let col = find_column_index(result, col_name);
    get_i64(result, col, row)
}

pub fn get_string_by_name(result: &yachtsql::Table, row: usize, col_name: &str) -> String {
    let col = find_column_index(result, col_name);
    get_string(result, col, row)
}

pub fn get_f64_by_name(result: &yachtsql::Table, row: usize, col_name: &str) -> f64 {
    let col = find_column_index(result, col_name);
    get_f64(result, col, row)
}

pub fn get_bool_by_name(result: &yachtsql::Table, row: usize, col_name: &str) -> bool {
    let col = find_column_index(result, col_name);
    get_bool(result, col, row)
}

pub fn is_null_by_name(result: &yachtsql::Table, row: usize, col_name: &str) -> bool {
    let col = find_column_index(result, col_name);
    is_null(result, col, row)
}

pub fn assert_error_with_sqlstate<T: std::fmt::Debug>(
    result: Result<T>,
    expected_sqlstate: &str,
    context: &str,
) {
    assert!(result.is_err(), "Expected error for: {}", context);
    let err = result.unwrap_err();
    let err_msg = err.to_string();

    let has_sqlstate = err_msg.contains(expected_sqlstate)
        || err_msg.contains(&format!("[{}]", expected_sqlstate))
        || err_msg.contains(&format!("SQLSTATE[{}]", expected_sqlstate));

    assert!(
        has_sqlstate,
        "Expected SQLSTATE {} in error '{}' (context: {})",
        expected_sqlstate, err_msg, context
    );
}

pub fn assert_result_equals(
    result: yachtsql::Result<yachtsql::Table>,
    expected_rows: usize,
    context: &str,
) {
    let batch = result.unwrap_or_else(|e| panic!("Query failed for {}: {}", context, e));
    assert_eq!(
        batch.num_rows(),
        expected_rows,
        "Expected {} rows for: {}",
        expected_rows,
        context
    );
}

pub fn column_strings(batch: &yachtsql::Table, index: usize) -> Vec<String> {
    let column = batch.column(index).expect("missing column");
    (0..batch.num_rows())
        .map(|row| {
            column
                .get(row)
                .unwrap_or_else(|e| panic!("failed to read row {row} from column {index}: {e}"))
                .as_str()
                .unwrap_or_else(|| panic!("Value at row {row} col {index} is not STRING"))
                .to_string()
        })
        .collect()
}

pub fn column_i64(batch: &yachtsql::Table, index: usize) -> Vec<i64> {
    let column = batch.column(index).expect("missing column");
    (0..batch.num_rows())
        .map(|row| {
            column
                .get(row)
                .unwrap_or_else(|e| panic!("failed to read row {row} from column {index}: {e}"))
                .as_i64()
                .unwrap_or_else(|| panic!("Value at row {row} col {index} is not INT64"))
        })
        .collect()
}

pub fn column_nullable_i64(batch: &yachtsql::Table, index: usize) -> Vec<Option<i64>> {
    let column = batch.column(index).expect("missing column");
    (0..batch.num_rows())
        .map(|row| {
            let val = column
                .get(row)
                .unwrap_or_else(|e| panic!("failed to read row {row} from column {index}: {e}"));
            if val.is_null() {
                None
            } else {
                Some(
                    val.as_i64()
                        .unwrap_or_else(|| panic!("Value at row {row} col {index} is not INT64")),
                )
            }
        })
        .collect()
}

pub fn scalar_value(batch: &yachtsql::Table, index: usize) -> Value {
    if batch.num_rows() == 0 {
        Value::null()
    } else {
        batch
            .column(index)
            .expect("missing column")
            .get(0)
            .unwrap_or_else(|e| panic!("failed to read scalar column {index}: {e}"))
    }
}

pub fn exec_ok(executor: &mut QueryExecutor, sql: &str) {
    executor
        .execute_sql(sql)
        .unwrap_or_else(|e| panic!("SQL execution failed for '{}': {}", sql, e));
}

pub fn query(executor: &mut QueryExecutor, sql: &str) -> yachtsql::Table {
    executor
        .execute_sql(sql)
        .unwrap_or_else(|e| panic!("Query execution failed for '{}': {}", sql, e))
}

pub fn get_rowcount(executor: &mut QueryExecutor) -> i64 {
    let batch = executor
        .execute_sql("GET DIAGNOSTICS :rowcount = ROW_COUNT")
        .expect("GET DIAGNOSTICS should succeed");
    get_i64_by_name(&batch, 0, "rowcount")
}

pub fn get_rowcount_opt(executor: &mut QueryExecutor) -> Option<i64> {
    let batch = executor
        .execute_sql("GET DIAGNOSTICS :rowcount = ROW_COUNT")
        .expect("GET DIAGNOSTICS should succeed");
    if is_null_by_name(&batch, 0, "rowcount") {
        None
    } else {
        Some(get_i64_by_name(&batch, 0, "rowcount"))
    }
}

pub fn get_exception_diag(executor: &mut QueryExecutor, projection: &str) -> yachtsql::Table {
    executor
        .execute_sql(&format!("GET DIAGNOSTICS EXCEPTION 1 {}", projection))
        .expect("GET DIAGNOSTICS EXCEPTION should succeed")
}

pub fn assert_no_exception(executor: &mut QueryExecutor) {
    let err = executor.execute_sql("GET DIAGNOSTICS EXCEPTION 1 :sqlstate = RETURNED_SQLSTATE");
    assert_error_contains(err, &["no exception", "diagnostic"]);
}

pub fn assert_exception_message_contains(diag: &yachtsql::Table, fragments: &[&str]) {
    let message = get_string_by_name(diag, 0, "message").to_lowercase();
    for fragment in fragments {
        assert!(
            message.contains(&fragment.to_lowercase()),
            "Expected diagnostic message to contain '{fragment}', got '{message}'",
        );
    }
}

pub fn build_batch(schema: Schema, rows: Vec<Vec<Value>>) -> Table {
    let row_objs: Vec<Row> = rows.into_iter().map(Row::from_values).collect();
    Table::from_rows(schema, row_objs).expect("Failed to build Table")
}

fn values_equal(actual: &Value, expected: &Value, epsilon: f64) -> bool {
    if actual.is_null() && expected.is_null() {
        return true;
    }
    if actual.is_null() || expected.is_null() {
        return false;
    }

    if let (Some(a), Some(e)) = (actual.as_f64(), expected.as_f64()) {
        return (a - e).abs() < epsilon;
    }

    actual == expected
}

pub fn assert_batch_values(actual: &yachtsql::Table, expected: Vec<Vec<Value>>) {
    let expected_rows = expected.len();
    assert_eq!(
        actual.num_rows(),
        expected_rows,
        "Row count mismatch: expected {}, got {}",
        expected_rows,
        actual.num_rows()
    );

    if expected_rows == 0 {
        return;
    }

    let expected_cols = expected[0].len();
    let actual_cols = actual.schema().fields().len();
    assert_eq!(
        actual_cols, expected_cols,
        "Column count mismatch: expected {}, got {}",
        expected_cols, actual_cols
    );

    for (row_idx, expected_row) in expected.iter().enumerate() {
        assert_eq!(
            expected_row.len(),
            expected_cols,
            "Row {} has inconsistent column count: expected {}, got {}",
            row_idx,
            expected_cols,
            expected_row.len()
        );

        for (col_idx, expected_val) in expected_row.iter().enumerate() {
            let actual_val = actual
                .column(col_idx)
                .unwrap_or_else(|| panic!("Column {} not found", col_idx))
                .get(row_idx)
                .unwrap_or_else(|e| {
                    panic!(
                        "Failed to get value at row {} col {}: {}",
                        row_idx, col_idx, e
                    )
                });

            assert_values_equal(&actual_val, expected_val, row_idx, col_idx);
        }
    }
}

fn assert_values_equal(actual: &Value, expected: &Value, row: usize, col: usize) {
    if expected.is_null() {
        assert!(
            actual.is_null(),
            "Value mismatch at row {} col {}: expected NULL, got {:?}",
            row,
            col,
            actual
        );
        return;
    }

    if actual.is_null() {
        panic!(
            "Value mismatch at row {} col {}: expected {:?}, got NULL",
            row, col, expected
        );
    }

    match (actual.data_type(), expected.data_type()) {
        (yachtsql::DataType::Float64, yachtsql::DataType::Float64) => {
            let a = actual
                .as_f64()
                .unwrap_or_else(|| panic!("Expected Float64 value at row {} col {}", row, col));
            let e = expected.as_f64().unwrap_or_else(|| {
                panic!(
                    "Expected Float64 value for comparison at row {} col {}",
                    row, col
                )
            });
            if a.is_nan() && e.is_nan() {
                return;
            }
            if a.is_infinite() && e.is_infinite() && a.signum() == e.signum() {
                return;
            }
            let epsilon = 1e-9;
            let diff = (a - e).abs();
            assert!(
                diff < epsilon || diff / e.abs().max(1.0) < epsilon,
                "Float value mismatch at row {} col {}: expected {}, got {} (diff: {})",
                row,
                col,
                e,
                a,
                diff
            );
        }

        _ => {
            assert_eq!(
                actual, expected,
                "Value mismatch at row {} col {}: expected {:?}, got {:?}",
                row, col, expected, actual
            );
        }
    }
}

pub fn assert_batch_eq(actual: &Table, expected: &Table) {
    assert_batch_eq_with_epsilon(actual, expected, 1e-10);
}

pub fn assert_batch_eq_with_epsilon(actual: &Table, expected: &Table, epsilon: f64) {
    assert_eq!(
        actual.num_rows(),
        expected.num_rows(),
        "Row count mismatch: actual={}, expected={}",
        actual.num_rows(),
        expected.num_rows()
    );

    let actual_cols = actual.num_columns();
    let expected_cols = expected.num_columns();
    assert_eq!(
        actual_cols, expected_cols,
        "Column count mismatch: actual={}, expected={}",
        actual_cols, expected_cols
    );

    let actual_rows = actual.rows().expect("Failed to get actual rows");
    let expected_rows = expected.rows().expect("Failed to get expected rows");

    for (row_idx, (actual_row, expected_row)) in
        actual_rows.iter().zip(expected_rows.iter()).enumerate()
    {
        let actual_values = actual_row.values();
        let expected_values = expected_row.values();

        for (col_idx, (actual_val, expected_val)) in
            actual_values.iter().zip(expected_values.iter()).enumerate()
        {
            if !values_equal(actual_val, expected_val, epsilon) {
                let col_name = actual
                    .schema()
                    .fields()
                    .get(col_idx)
                    .map(|f| f.name.as_str())
                    .unwrap_or("?");
                panic!(
                    "Value mismatch at row {}, column {} ('{}'): actual={:?}, expected={:?}",
                    row_idx, col_idx, col_name, actual_val, expected_val
                );
            }
        }
    }
}

pub fn get_value(batch: &yachtsql::Table, row: usize, col: usize) -> Value {
    batch
        .column(col)
        .unwrap_or_else(|| panic!("Column {} not found", col))
        .get(row)
        .unwrap_or_else(|e| panic!("Failed to get value at row {} col {}: {}", row, col, e))
}

pub fn assert_scalar(batch: &yachtsql::Table, expected: Value) {
    assert_eq!(
        batch.num_rows(),
        1,
        "Expected 1 row, got {}",
        batch.num_rows()
    );
    assert_eq!(
        batch.schema().fields().len(),
        1,
        "Expected 1 column, got {}",
        batch.schema().fields().len()
    );
    assert_batch_values(batch, vec![vec![expected]]);
}

pub fn assert_single_row(batch: &yachtsql::Table, expected: Vec<Value>) {
    assert_eq!(
        batch.num_rows(),
        1,
        "Expected 1 row, got {}",
        batch.num_rows()
    );
    assert_batch_values(batch, vec![expected]);
}

pub fn assert_null_at(batch: &yachtsql::Table, row: usize, col: usize) {
    let value = get_value(batch, row, col);
    assert!(
        value.is_null(),
        "Expected NULL at row {} col {}, got {:?}",
        row,
        col,
        value
    );
}

pub fn assert_value_at(batch: &yachtsql::Table, row: usize, col: usize, expected: Value) {
    let actual = get_value(batch, row, col);
    assert_values_equal(&actual, &expected, row, col);
}

pub fn assert_query_rows(executor: &mut QueryExecutor, sql: &str, expected_rows: Vec<Vec<Value>>) {
    let result = executor
        .execute_sql(sql)
        .unwrap_or_else(|e| panic!("Query execution failed for '{}': {}", sql, e));
    let expected = build_batch(result.schema().clone(), expected_rows);
    assert_batch_eq(&result, &expected);
}
