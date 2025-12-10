#![allow(clippy::wildcard_enum_match_arm)]

use chrono::Utc;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;

use crate::{DefaultValue, Row, Schema};

#[inline]
fn get_column_value(schema: &Schema, row: &Row, column_name: &str) -> Value {
    row.get_by_name(schema, column_name)
        .cloned()
        .unwrap_or(Value::null())
}

#[inline]
fn is_null(value: &Value) -> bool {
    value.is_null()
}

pub fn validate_row_constraints(schema: &Schema, row: &Row, existing_rows: &[Row]) -> Result<()> {
    validate_not_null_constraints(schema, row)?;

    validate_primary_key_constraints(schema, row, existing_rows)?;

    validate_unique_constraints(schema, row, existing_rows)?;

    validate_check_constraints(schema, row)?;

    Ok(())
}

fn validate_not_null_constraints(schema: &Schema, row: &Row) -> Result<()> {
    for field in schema.fields() {
        if !field.is_nullable() {
            let value = get_column_value(schema, row, &field.name);
            if is_null(&value) {
                return Err(Error::NotNullViolation {
                    column: field.name.clone(),
                });
            }
        }
    }
    Ok(())
}

fn validate_primary_key_constraints(
    schema: &Schema,
    row: &Row,
    existing_rows: &[Row],
) -> Result<()> {
    let Some(pk_columns) = schema.primary_key() else {
        return Ok(());
    };

    for col_name in pk_columns {
        let value = get_column_value(schema, row, col_name);
        if is_null(&value) {
            return Err(Error::NotNullViolation {
                column: col_name.clone(),
            });
        }
    }

    if existing_rows
        .iter()
        .any(|existing_row| columns_match(schema, pk_columns, row, existing_row))
    {
        return Err(Error::UniqueConstraintViolation(format!(
            "PRIMARY KEY constraint violation: duplicate key value violates unique constraint on columns: {}",
            pk_columns.join(", ")
        )));
    }

    Ok(())
}

fn columns_match(schema: &Schema, columns: &[String], row1: &Row, row2: &Row) -> bool {
    columns.iter().all(|col| {
        let v1 = get_column_value(schema, row1, col);
        let v2 = get_column_value(schema, row2, col);
        values_equal(&v1, &v2)
    })
}

fn validate_unique_constraints(schema: &Schema, row: &Row, existing_rows: &[Row]) -> Result<()> {
    validate_column_unique_constraints(schema, row, existing_rows)?;

    validate_table_unique_constraints(schema, row, existing_rows)?;

    Ok(())
}

fn validate_column_unique_constraints(
    schema: &Schema,
    row: &Row,
    existing_rows: &[Row],
) -> Result<()> {
    for field in schema.fields() {
        if !field.is_unique {
            continue;
        }

        let value = get_column_value(schema, row, &field.name);

        if is_null(&value) {
            continue;
        }

        let collation = field.collation.as_deref();
        if existing_rows.iter().any(|existing_row| {
            let other_value = get_column_value(schema, existing_row, &field.name);
            values_equal_with_collation(&value, &other_value, collation)
        }) {
            return Err(Error::UniqueConstraintViolation(format!(
                "UNIQUE constraint violation: duplicate value in column '{}'",
                field.name
            )));
        }
    }
    Ok(())
}

fn validate_table_unique_constraints(
    schema: &Schema,
    row: &Row,
    existing_rows: &[Row],
) -> Result<()> {
    for unique_constraint in schema.unique_constraints() {
        if !unique_constraint.enforced {
            continue;
        }

        let unique_cols = &unique_constraint.columns;
        let nulls_distinct = unique_constraint.nulls_distinct;

        if nulls_distinct && columns_contain_null(schema, unique_cols, row) {
            continue;
        }

        let matcher: fn(&Schema, &[String], &Row, &Row) -> bool = if nulls_distinct {
            columns_match_non_null
        } else {
            columns_match_with_null_equal
        };

        if existing_rows
            .iter()
            .any(|existing_row| matcher(schema, unique_cols, row, existing_row))
        {
            return Err(Error::UniqueConstraintViolation(format!(
                "UNIQUE constraint violation: duplicate key value on columns: {}",
                unique_cols.join(", ")
            )));
        }
    }
    Ok(())
}

fn columns_match_with_null_equal(
    schema: &Schema,
    columns: &[String],
    row1: &Row,
    row2: &Row,
) -> bool {
    columns.iter().all(|col| {
        let v1 = get_column_value(schema, row1, col);
        let v2 = get_column_value(schema, row2, col);
        if is_null(&v1) && is_null(&v2) {
            return true;
        }
        if is_null(&v1) || is_null(&v2) {
            return false;
        }
        values_equal(&v1, &v2)
    })
}

fn columns_contain_null(schema: &Schema, columns: &[String], row: &Row) -> bool {
    columns
        .iter()
        .any(|col| is_null(&get_column_value(schema, row, col)))
}

fn columns_match_non_null(schema: &Schema, columns: &[String], row1: &Row, row2: &Row) -> bool {
    columns.iter().all(|col| {
        let v1 = get_column_value(schema, row1, col);
        let v2 = get_column_value(schema, row2, col);

        if is_null(&v1) || is_null(&v2) {
            return false;
        }

        values_equal(&v1, &v2)
    })
}

pub fn validate_check_constraints(schema: &Schema, row: &Row) -> Result<()> {
    let Some(evaluator) = schema.check_evaluator() else {
        return Ok(());
    };

    for constraint in schema.check_constraints() {
        if !constraint.enforced {
            continue;
        }

        let is_satisfied = evaluator(schema, row, &constraint.expression)?;

        if !is_satisfied {
            let constraint_name = constraint.name.clone();
            let expression = constraint.expression.clone();

            return Err(Error::CheckConstraintViolation {
                message: format!(
                    "CHECK constraint '{}' violated: {}",
                    constraint_name.as_deref().unwrap_or("<unnamed>"),
                    expression
                ),
                constraint: constraint_name,
                expression,
            });
        }
    }

    Ok(())
}

pub fn apply_default_values(schema: &Schema, row: &mut Row) -> Result<()> {
    for field in schema.fields() {
        let has_column = row.contains_column(schema, &field.name);
        let is_explicit_default = has_column
            && row
                .get_by_name(schema, &field.name)
                .map(|v| v.is_default())
                .unwrap_or(false);

        if has_column && !is_explicit_default {
            continue;
        }

        if let Some(default) = &field.default_value {
            let value = match default {
                DefaultValue::Literal(v) => v.clone(),
                DefaultValue::CurrentTimestamp => Value::timestamp(Utc::now()),
                DefaultValue::CurrentDate => Value::date(Utc::now().date_naive()),
                DefaultValue::GenRandomUuid => Value::uuid(uuid::Uuid::new_v4()),
            };
            row.set_by_name(schema, &field.name, value)?;
        } else {
            row.set_by_name(schema, &field.name, Value::null())?;
        }
    }
    Ok(())
}

#[inline]
fn compare_strings_with_collation(a: &str, b: &str, collation: Option<&str>) -> bool {
    match collation {
        Some("en_US") | Some("en_us") => a.to_lowercase() == b.to_lowercase(),
        Some("C") | None => a == b,
        _ => a == b,
    }
}

fn values_equal(v1: &Value, v2: &Value) -> bool {
    if v1.is_null() || v2.is_null() {
        return false;
    }

    if let (Some(a), Some(b)) = (v1.as_bool(), v2.as_bool()) {
        return a == b;
    }

    if let (Some(a), Some(b)) = (v1.as_i64(), v2.as_i64()) {
        return a == b;
    }

    if let (Some(a), Some(b)) = (v1.as_f64(), v2.as_f64()) {
        if a.is_nan() && b.is_nan() {
            return true;
        }
        if a.is_nan() || b.is_nan() {
            return false;
        }

        if a.is_infinite() || b.is_infinite() {
            return a == b;
        }

        let abs_a = a.abs();
        let abs_b = b.abs();
        let diff = (a - b).abs();

        if abs_a < f64::EPSILON && abs_b < f64::EPSILON {
            return a == b;
        }

        let largest = abs_a.max(abs_b);
        return diff <= largest * f64::EPSILON;
    }

    if let (Some(a), Some(b)) = (v1.as_str(), v2.as_str()) {
        return a == b;
    }

    if let (Some(a), Some(b)) = (v1.as_uuid(), v2.as_uuid()) {
        return a == b;
    }

    false
}

fn values_equal_with_collation(v1: &Value, v2: &Value, collation: Option<&str>) -> bool {
    if v1.is_null() || v2.is_null() {
        return false;
    }

    if let (Some(a), Some(b)) = (v1.as_bool(), v2.as_bool()) {
        return a == b;
    }

    if let (Some(a), Some(b)) = (v1.as_i64(), v2.as_i64()) {
        return a == b;
    }

    if let (Some(a), Some(b)) = (v1.as_f64(), v2.as_f64()) {
        if a.is_nan() && b.is_nan() {
            return true;
        }
        if a.is_nan() || b.is_nan() {
            return false;
        }

        if a.is_infinite() || b.is_infinite() {
            return a == b;
        }

        let abs_a = a.abs();
        let abs_b = b.abs();
        let diff = (a - b).abs();

        if abs_a < f64::EPSILON && abs_b < f64::EPSILON {
            return a == b;
        }

        let largest = abs_a.max(abs_b);
        return diff <= largest * f64::EPSILON;
    }

    if let (Some(a), Some(b)) = (v1.as_str(), v2.as_str()) {
        return compare_strings_with_collation(a, b, collation);
    }

    if let (Some(a), Some(b)) = (v1.as_uuid(), v2.as_uuid()) {
        return a == b;
    }

    false
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};

    use super::*;
    use crate::{CheckConstraint, CheckEvaluator, Field, Schema};

    fn row_with_values(schema: &Schema, entries: Vec<(&str, Value)>) -> Row {
        let mut row = Row::for_schema(schema);
        for (name, value) in entries {
            row.set_by_name(schema, name, value).unwrap();
        }
        row
    }

    #[test]
    fn test_float_comparison_nan() {
        let nan1 = Value::float64(f64::NAN);
        let nan2 = Value::float64(f64::NAN);
        assert!(values_equal(&nan1, &nan2), "NaN should equal NaN");

        let finite = Value::float64(1.0);
        assert!(!values_equal(&nan1, &finite), "NaN should not equal finite");
    }

    #[test]
    fn test_float_comparison_infinity() {
        let pos_inf = Value::float64(f64::INFINITY);
        let neg_inf = Value::float64(f64::NEG_INFINITY);
        let finite = Value::float64(1.0);

        assert!(values_equal(&pos_inf, &pos_inf), "+Inf should equal +Inf");

        assert!(values_equal(&neg_inf, &neg_inf), "-Inf should equal -Inf");

        assert!(
            !values_equal(&pos_inf, &neg_inf),
            "+Inf should not equal -Inf"
        );

        assert!(
            !values_equal(&pos_inf, &finite),
            "Infinity should not equal finite"
        );
    }

    #[test]
    fn test_float_comparison_exact() {
        let a = Value::float64(42.0);
        let b = Value::float64(42.0);
        assert!(values_equal(&a, &b), "Exact same floats should be equal");
    }

    #[test]
    fn test_float_comparison_very_small() {
        let a = Value::float64(1e-308);
        let b = Value::float64(1e-308);
        assert!(values_equal(&a, &b), "Very small floats should be equal");

        let tiny1 = Value::float64(f64::EPSILON / 2.0);
        let tiny2 = Value::float64(f64::EPSILON / 2.0);
        assert!(
            values_equal(&tiny1, &tiny2),
            "Sub-epsilon floats should be equal"
        );
    }

    #[test]
    fn test_float_comparison_different() {
        let a = Value::float64(1.0);
        let b = Value::float64(2.0);
        assert!(
            !values_equal(&a, &b),
            "Different floats should not be equal"
        );

        let c = Value::float64(1.0);
        let d = Value::float64(1.0 + 1e-10);
        assert!(
            !values_equal(&c, &d),
            "Floats outside epsilon should not be equal"
        );
    }

    #[test]
    fn test_float_comparison_zero() {
        let zero = Value::float64(0.0);
        let neg_zero = Value::float64(-0.0);
        let tiny = Value::float64(1e-320);

        assert!(values_equal(&zero, &neg_zero), "0.0 should equal -0.0");

        assert!(values_equal(&zero, &zero), "0.0 should equal 0.0");

        assert!(
            !values_equal(&zero, &tiny),
            "0.0 should not equal tiny non-zero"
        );
    }

    #[test]
    fn test_null_comparison() {
        let null1 = Value::null();
        let null2 = Value::null();
        let not_null = Value::float64(1.0);

        assert!(!values_equal(&null1, &null2), "NULL should not equal NULL");

        assert!(
            !values_equal(&null1, &not_null),
            "NULL should not equal non-NULL"
        );
    }

    #[test]
    fn test_collation_binary() {
        assert!(
            compare_strings_with_collation("foo", "foo", Some("C")),
            "Binary collation: 'foo' == 'foo'"
        );
        assert!(
            !compare_strings_with_collation("foo", "Foo", Some("C")),
            "Binary collation: 'foo' != 'Foo'"
        );
        assert!(
            !compare_strings_with_collation("foo", "FOO", Some("C")),
            "Binary collation: 'foo' != 'FOO'"
        );
    }

    #[test]
    fn test_collation_case_insensitive() {
        assert!(
            compare_strings_with_collation("foo", "foo", Some("en_US")),
            "en_US collation: 'foo' == 'foo'"
        );
        assert!(
            compare_strings_with_collation("foo", "Foo", Some("en_US")),
            "en_US collation: 'foo' == 'Foo'"
        );
        assert!(
            compare_strings_with_collation("foo", "FOO", Some("en_US")),
            "en_US collation: 'foo' == 'FOO'"
        );
        assert!(
            compare_strings_with_collation("foo", "fOo", Some("en_US")),
            "en_US collation: 'foo' == 'fOo'"
        );
    }

    #[test]
    fn test_collation_default() {
        assert!(
            compare_strings_with_collation("foo", "foo", None),
            "Default collation: 'foo' == 'foo'"
        );
        assert!(
            !compare_strings_with_collation("foo", "Foo", None),
            "Default collation: 'foo' != 'Foo'"
        );
    }

    #[test]
    fn test_collation_unknown() {
        assert!(
            compare_strings_with_collation("foo", "foo", Some("unknown")),
            "Unknown collation: 'foo' == 'foo'"
        );
        assert!(
            !compare_strings_with_collation("foo", "Foo", Some("unknown")),
            "Unknown collation: 'foo' != 'Foo'"
        );
    }

    #[test]
    fn test_values_equal_with_collation_strings() {
        let foo_lower = Value::string("foo".to_string());
        let foo_upper = Value::string("FOO".to_string());

        assert!(
            !values_equal_with_collation(&foo_lower, &foo_upper, Some("C")),
            "Binary: 'foo' != 'FOO'"
        );

        assert!(
            values_equal_with_collation(&foo_lower, &foo_upper, Some("en_US")),
            "en_US: 'foo' == 'FOO'"
        );
    }

    #[test]
    fn test_values_equal_with_collation_non_strings() {
        let int1 = Value::int64(42);
        let int2 = Value::int64(42);
        let int3 = Value::int64(43);

        assert!(
            values_equal_with_collation(&int1, &int2, Some("en_US")),
            "Integers equal regardless of collation"
        );
        assert!(
            !values_equal_with_collation(&int1, &int3, Some("en_US")),
            "Different integers not equal"
        );

        let f1 = Value::float64(1.0);
        let f2 = Value::float64(1.0);
        assert!(
            values_equal_with_collation(&f1, &f2, Some("en_US")),
            "Floats equal regardless of collation"
        );
    }

    #[test]
    fn test_check_constraint_callback() {
        use std::rc::Rc;

        let mut schema = Schema::new();
        let field = Field::required("age", DataType::Int64);
        schema.add_field(field);

        schema.add_check_constraint(CheckConstraint {
            name: Some("age_positive".to_string()),
            expression: "age > 0".to_string(),
            enforced: true,
        });

        let evaluator: CheckEvaluator = Rc::new(|schema: &Schema, row: &Row, expr: &str| {
            if expr == "age > 0" {
                if let Some(age_val) = row.get_by_name(schema, "age") {
                    if let Some(age) = age_val.as_i64() {
                        Ok(age > 0)
                    } else {
                        Ok(false)
                    }
                } else {
                    Ok(false)
                }
            } else {
                Ok(true)
            }
        });

        schema.set_check_evaluator(evaluator);

        let valid_row = row_with_values(&schema, vec![("age", Value::int64(25))]);
        assert!(
            validate_check_constraints(&schema, &valid_row).is_ok(),
            "Valid age should pass CHECK constraint"
        );

        let invalid_row = row_with_values(&schema, vec![("age", Value::int64(-5))]);
        let result = validate_check_constraints(&schema, &invalid_row);
        assert!(result.is_err(), "Negative age should fail CHECK constraint");
        if let Err(Error::CheckConstraintViolation { message, .. }) = result {
            assert!(
                message.contains("age_positive"),
                "Error message should mention constraint name"
            );
        } else {
            panic!("Expected CheckConstraintViolation error");
        }
    }

    #[test]
    fn test_check_constraint_without_evaluator() {
        let mut schema = Schema::new();
        let field = Field::required("price", DataType::Float64);
        schema.add_field(field);

        schema.add_check_constraint(CheckConstraint {
            name: Some("price_positive".to_string()),
            expression: "price > 0".to_string(),
            enforced: true,
        });

        let row = row_with_values(&schema, vec![("price", Value::float64(-10.0))]);

        assert!(
            validate_check_constraints(&schema, &row).is_ok(),
            "Without evaluator, CHECK constraints are skipped"
        );
    }

    #[test]
    fn test_check_constraint_3vl_null_passes() {
        use std::rc::Rc;

        let mut schema = Schema::new();
        let field = Field::nullable("age", DataType::Int64);
        schema.add_field(field);

        schema.add_check_constraint(CheckConstraint {
            name: Some("age_adult".to_string()),
            expression: "age >= 18".to_string(),
            enforced: true,
        });

        let evaluator: CheckEvaluator = Rc::new(|schema: &Schema, row: &Row, expr: &str| {
            if expr == "age >= 18" {
                match row.get_by_name(schema, "age") {
                    Some(age_val) if age_val.as_i64().is_some() => {
                        Ok(age_val.as_i64().unwrap() >= 18)
                    }
                    Some(age_val) if age_val.is_null() => Ok(true),
                    None => Ok(true),
                    _ => Ok(false),
                }
            } else {
                Ok(true)
            }
        });

        schema.set_check_evaluator(evaluator);

        let valid_row = row_with_values(&schema, vec![("age", Value::int64(25))]);
        assert!(
            validate_check_constraints(&schema, &valid_row).is_ok(),
            "Age 25 should pass (TRUE)"
        );

        let invalid_row = row_with_values(&schema, vec![("age", Value::int64(10))]);
        assert!(
            validate_check_constraints(&schema, &invalid_row).is_err(),
            "Age 10 should fail (FALSE)"
        );

        let null_row = row_with_values(&schema, vec![("age", Value::null())]);
        assert!(
            validate_check_constraints(&schema, &null_row).is_ok(),
            "NULL age should pass CHECK constraint (3VL UNKNOWN → passes)"
        );
    }

    #[test]
    fn test_check_constraint_3vl_complex_expression() {
        use std::rc::Rc;

        let mut schema = Schema::new();
        let field = Field::nullable("salary", DataType::Float64);
        schema.add_field(field);
        let field = Field::nullable("bonus", DataType::Float64);
        schema.add_field(field);

        schema.add_check_constraint(CheckConstraint {
            name: Some("salary_bonus_ratio".to_string()),
            expression: "bonus < salary * 0.5".to_string(),
            enforced: true,
        });

        let evaluator: CheckEvaluator = Rc::new(|schema: &Schema, row: &Row, expr: &str| {
            if expr == "bonus < salary * 0.5" {
                let salary = row.get_by_name(schema, "salary");
                let bonus = row.get_by_name(schema, "bonus");

                match (salary, bonus) {
                    (Some(s_val), Some(b_val))
                        if s_val.as_f64().is_some() && b_val.as_f64().is_some() =>
                    {
                        let s = s_val.as_f64().unwrap();
                        let b = b_val.as_f64().unwrap();
                        Ok(b < s * 0.5)
                    }
                    (Some(s_val), _) if s_val.is_null() => Ok(true),
                    (_, Some(b_val)) if b_val.is_null() => Ok(true),
                    _ => Ok(true),
                }
            } else {
                Ok(true)
            }
        });

        schema.set_check_evaluator(evaluator);

        let null_salary_row = row_with_values(
            &schema,
            vec![("salary", Value::null()), ("bonus", Value::float64(1000.0))],
        );
        assert!(
            validate_check_constraints(&schema, &null_salary_row).is_ok(),
            "NULL salary in CHECK should pass (3VL UNKNOWN)"
        );

        let null_bonus_row = row_with_values(
            &schema,
            vec![
                ("salary", Value::float64(10000.0)),
                ("bonus", Value::null()),
            ],
        );
        assert!(
            validate_check_constraints(&schema, &null_bonus_row).is_ok(),
            "NULL bonus in CHECK should pass (3VL UNKNOWN)"
        );
    }
}
