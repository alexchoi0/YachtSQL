use yachtsql::Table;
use yachtsql_core::types::Value;
use yachtsql_storage::{Row, Schema};

pub fn assert_batch_eq(actual: &Table, expected: &Table) {
    let mut errors = Vec::new();

    let actual_schema = actual.schema();
    let expected_schema = expected.schema();

    if actual_schema != expected_schema {
        errors.push(format!(
            "SCHEMA MISMATCH:\n\
             ┌─────────────────────────────────────────────────────────────────┐\n\
             │ Expected Schema ({} columns):                                   │\n\
             └─────────────────────────────────────────────────────────────────┘\n\
             {}\n\
             ┌─────────────────────────────────────────────────────────────────┐\n\
             │ Actual Schema ({} columns):                                     │\n\
             └─────────────────────────────────────────────────────────────────┘\n\
             {}",
            expected_schema.fields().len(),
            format_schema(expected_schema),
            actual_schema.fields().len(),
            format_schema(actual_schema)
        ));
    }

    let actual_rows_count = actual.num_rows();
    let expected_rows_count = expected.num_rows();

    if actual_rows_count != expected_rows_count {
        errors.push(format!(
            "ROW COUNT MISMATCH:\n\
             Expected: {} rows\n\
             Actual:   {} rows",
            expected_rows_count, actual_rows_count
        ));
    }

    let actual_rows = actual.rows().expect("Failed to get actual rows");
    let expected_rows = expected.rows().expect("Failed to get expected rows");

    let mut value_mismatches = Vec::new();

    let min_rows = actual_rows.len().min(expected_rows.len());
    for row_idx in 0..min_rows {
        let actual_row = &actual_rows[row_idx];
        let expected_row = &expected_rows[row_idx];
        let actual_values = actual_row.values();
        let expected_values = expected_row.values();

        let min_cols = actual_values.len().min(expected_values.len());
        for col_idx in 0..min_cols {
            let actual_val = &actual_values[col_idx];
            let expected_val = &expected_values[col_idx];

            if actual_val != expected_val {
                let col_name = expected_schema
                    .fields()
                    .get(col_idx)
                    .map(|f| f.name.as_str())
                    .unwrap_or("<unknown>");

                value_mismatches.push(format!(
                    "  Row {}, Column {} ({}):\n\
                     \x20   Expected: {:?}\n\
                     \x20   Actual:   {:?}",
                    row_idx, col_idx, col_name, expected_val, actual_val
                ));
            }
        }

        if actual_values.len() != expected_values.len() {
            value_mismatches.push(format!(
                "  Row {}: Column count mismatch - Expected {} columns, got {}",
                row_idx,
                expected_values.len(),
                actual_values.len()
            ));
        }
    }

    if !value_mismatches.is_empty() {
        errors.push(format!(
            "VALUE MISMATCHES ({} differences found):\n{}",
            value_mismatches.len(),
            value_mismatches.join("\n\n")
        ));
    }

    if !errors.is_empty() {
        let separator = "═".repeat(70);
        panic!(
            "\n\n{separator}\n\
             ╔══════════════════════════════════════════════════════════════════════╗\n\
             ║                    RECORD BATCH ASSERTION FAILED                     ║\n\
             ╚══════════════════════════════════════════════════════════════════════╝\n\
             {separator}\n\n\
             {}\n\n\
             {separator}\n\
             EXPECTED BATCH ({} rows, {} columns):\n\
             {separator}\n\
             {}\n\n\
             {separator}\n\
             ACTUAL BATCH ({} rows, {} columns):\n\
             {separator}\n\
             {}\n\
             {separator}\n",
            errors.join("\n\n"),
            expected.num_rows(),
            expected.num_columns(),
            pretty_print_batch(expected),
            actual.num_rows(),
            actual.num_columns(),
            pretty_print_batch(actual)
        );
    }
}

fn format_schema(schema: &Schema) -> String {
    let mut s = String::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        let nullable = if field.is_nullable() {
            "NULL"
        } else {
            "NOT NULL"
        };
        s.push_str(&format!(
            "  [{:2}] {:20} {:15} {}\n",
            idx,
            field.name,
            format!("{:?}", field.data_type),
            nullable
        ));
    }
    s
}

pub fn build_batch(schema: Schema, values: Vec<Vec<Value>>) -> Table {
    let rows = values.into_iter().map(Row::from_values).collect();
    Table::from_rows(schema, rows).expect("Failed to build Table")
}

fn pretty_print_batch(batch: &Table) -> String {
    let schema = batch.schema();
    let fields = schema.fields();

    if fields.is_empty() {
        return "  (empty schema)".to_string();
    }

    let mut col_widths: Vec<usize> = fields.iter().map(|f| f.name.len().max(10)).collect();

    if let Ok(rows) = batch.rows() {
        for row in &rows {
            for (col_idx, value) in row.values().iter().enumerate() {
                if col_idx < col_widths.len() {
                    let val_str = format_value(value);
                    col_widths[col_idx] = col_widths[col_idx].max(val_str.len()).min(40);
                }
            }
        }
    }

    let mut s = String::new();

    s.push_str("  ");
    s.push_str(&format!("{:>4} │ ", "#"));

    for (idx, field) in fields.iter().enumerate() {
        let width = col_widths[idx];
        s.push_str(&format!("{:width$} │ ", field.name, width = width));
    }
    s.push('\n');

    s.push_str("  ");
    s.push_str(&format!("{:─>4}─┼─", ""));
    for (idx, _) in fields.iter().enumerate() {
        let width = col_widths[idx];
        s.push_str(&format!("{:─>width$}─┼─", "", width = width));
    }
    s.push('\n');

    if let Ok(rows) = batch.rows() {
        if rows.is_empty() {
            s.push_str("  (no rows)\n");
        } else {
            for (row_idx, row) in rows.iter().enumerate() {
                s.push_str("  ");
                s.push_str(&format!("{:>4} │ ", row_idx));

                for (col_idx, value) in row.values().iter().enumerate() {
                    let width = col_widths.get(col_idx).copied().unwrap_or(10);
                    let val_str = format_value(value);

                    let display_str = if val_str.len() > width {
                        format!("{}…", &val_str[..width - 1])
                    } else {
                        val_str
                    };
                    s.push_str(&format!("{:width$} │ ", display_str, width = width));
                }
                s.push('\n');
            }
        }
    }

    s
}

fn format_value(value: &Value) -> String {
    if value.is_null() {
        "NULL".to_string()
    } else if let Some(s) = value.as_str() {
        format!("'{}'", s)
    } else if let Some(i) = value.as_i64() {
        i.to_string()
    } else if let Some(f) = value.as_f64() {
        format!("{:.6}", f)
    } else if let Some(b) = value.as_bool() {
        b.to_string()
    } else {
        format!("{:?}", value)
    }
}

pub fn assert_batch_rows(batch: &Table, expected_rows: usize) {
    assert_eq!(
        batch.num_rows(),
        expected_rows,
        "\nRow count mismatch!\n\
         Expected: {} rows\n\
         Actual:   {} rows\n\n\
         Batch contents:\n{}",
        expected_rows,
        batch.num_rows(),
        pretty_print_batch(batch)
    );
}

#[allow(dead_code)]
pub fn assert_batch_columns(batch: &Table, expected_cols: usize) {
    assert_eq!(
        batch.num_columns(),
        expected_cols,
        "\nColumn count mismatch!\n\
         Expected: {} columns\n\
         Actual:   {} columns\n\n\
         Schema:\n{}",
        expected_cols,
        batch.num_columns(),
        format_schema(batch.schema())
    );
}

pub fn assert_batch_empty(batch: &Table) {
    assert_eq!(
        batch.num_rows(),
        0,
        "\nExpected empty batch but got {} rows!\n\n\
         Batch contents:\n{}",
        batch.num_rows(),
        pretty_print_batch(batch)
    );
}

pub fn assert_batch_value(batch: &Table, row: usize, col: usize, expected: &Value) {
    let rows = batch.rows().expect("Failed to get rows");
    assert!(
        row < rows.len(),
        "Row {} out of bounds (batch has {} rows)",
        row,
        rows.len()
    );

    let values = rows[row].values();
    assert!(
        col < values.len(),
        "Column {} out of bounds (row has {} columns)",
        col,
        values.len()
    );

    let actual = &values[col];
    let col_name = batch
        .schema()
        .fields()
        .get(col)
        .map(|f| f.name.as_str())
        .unwrap_or("<unknown>");

    assert_eq!(
        actual,
        expected,
        "\nValue mismatch at row {}, column {} ({})!\n\
         Expected: {:?}\n\
         Actual:   {:?}\n\n\
         Full batch:\n{}",
        row,
        col,
        col_name,
        expected,
        actual,
        pretty_print_batch(batch)
    );
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::DataType;
    use yachtsql_storage::Field;

    use super::*;

    #[test]
    fn test_assert_batch_eq_simple() {
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
                vec![Value::int64(2), Value::string("Bob".into())],
            ],
        );

        assert_batch_eq(&batch1, &batch2);
    }

    #[test]
    #[should_panic(expected = "VALUE MISMATCHES")]
    fn test_assert_batch_eq_value_mismatch() {
        let schema = Schema::from_fields(vec![Field::required("id", DataType::Int64)]);

        let batch1 = build_batch(schema.clone(), vec![vec![Value::int64(1)]]);

        let batch2 = build_batch(schema, vec![vec![Value::int64(2)]]);

        assert_batch_eq(&batch1, &batch2);
    }

    #[test]
    #[should_panic(expected = "ROW COUNT MISMATCH")]
    fn test_assert_batch_eq_row_count_mismatch() {
        let schema = Schema::from_fields(vec![Field::required("id", DataType::Int64)]);

        let batch1 = build_batch(
            schema.clone(),
            vec![vec![Value::int64(1)], vec![Value::int64(2)]],
        );

        let batch2 = build_batch(schema, vec![vec![Value::int64(1)]]);

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
            ],
        );

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_assert_batch_rows() {
        let schema = Schema::from_fields(vec![Field::required("id", DataType::Int64)]);
        let batch = build_batch(schema, vec![vec![Value::int64(1)], vec![Value::int64(2)]]);
        assert_batch_rows(&batch, 2);
    }

    #[test]
    fn test_assert_batch_empty() {
        let schema = Schema::from_fields(vec![Field::required("id", DataType::Int64)]);
        let batch = build_batch(schema, vec![]);
        assert_batch_empty(&batch);
    }

    #[test]
    fn test_assert_batch_value() {
        let schema = Schema::from_fields(vec![
            Field::required("id", DataType::Int64),
            Field::required("name", DataType::String),
        ]);
        let batch = build_batch(
            schema,
            vec![vec![Value::int64(42), Value::string("test".into())]],
        );
        assert_batch_value(&batch, 0, 0, &Value::int64(42));
        assert_batch_value(&batch, 0, 1, &Value::string("test".into()));
    }
}
