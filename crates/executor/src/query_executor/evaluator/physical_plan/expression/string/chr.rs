use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_chr(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("CHR", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if val.is_null() {
            return Ok(Value::null());
        }

        if let Some(code) = val.as_i64() {
            if code < 0 {
                return Err(crate::error::Error::invalid_query(
                    "CHR code cannot be negative".to_string(),
                ));
            } else if code > 0x10FFFF {
                return Err(crate::error::Error::invalid_query(
                    "CHR code out of valid Unicode range".to_string(),
                ));
            } else {
                match char::from_u32(code as u32) {
                    Some(c) => return Ok(Value::string(c.to_string())),
                    None => {
                        return Err(crate::error::Error::invalid_query(
                            "Invalid Unicode code point".to_string(),
                        ));
                    }
                }
            }
        }

        Err(crate::error::Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: val.data_type().to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;
    use crate::tests::support::assert_error_contains;

    #[test]
    fn converts_ascii_code_to_character() {
        let schema = Schema::from_fields(vec![Field::nullable("code", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(65)]]);
        let args = vec![Expr::column("code")];
        let result = ProjectionWithExprExec::evaluate_chr(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("A".into()));
    }

    #[test]
    fn converts_lowercase_ascii_code() {
        let schema = Schema::from_fields(vec![Field::nullable("code", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(97)]]);
        let args = vec![Expr::column("code")];
        let result = ProjectionWithExprExec::evaluate_chr(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("a".into()));
    }

    #[test]
    fn converts_unicode_code_point() {
        let schema = Schema::from_fields(vec![Field::nullable("code", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(233)]]);
        let args = vec![Expr::column("code")];
        let result = ProjectionWithExprExec::evaluate_chr(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("é".into()));
    }

    #[test]
    fn errors_on_negative_code() {
        let schema = Schema::from_fields(vec![Field::nullable("code", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(-1)]]);
        let args = vec![Expr::column("code")];
        let err = ProjectionWithExprExec::evaluate_chr(&args, &batch, 0).expect_err("negative");
        assert_error_contains(&err, "negative");
    }

    #[test]
    fn errors_on_out_of_range_code() {
        let schema = Schema::from_fields(vec![Field::nullable("code", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(0x110000)]]);
        let args = vec![Expr::column("code")];
        let err = ProjectionWithExprExec::evaluate_chr(&args, &batch, 0).expect_err("out of range");
        assert_error_contains(&err, "Unicode range");
    }

    #[test]
    fn propagates_null() {
        let schema = Schema::from_fields(vec![Field::nullable("code", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("code")];
        let result = ProjectionWithExprExec::evaluate_chr(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::evaluate_chr(&[], &batch, 0).expect_err("no args");
        assert_error_contains(&err, "CHR");
    }
}
