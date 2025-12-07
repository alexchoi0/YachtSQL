use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_regexp_contains(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        use regex::Regex;
        if args.len() != 2 {
            return Err(crate::error::Error::invalid_query(
                "REGEXP_CONTAINS requires exactly 2 arguments (string, pattern)".to_string(),
            ));
        }
        let text_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let pattern_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if text_val.is_null() || pattern_val.is_null() {
            return Ok(Value::null());
        }

        if let (Some(text), Some(pattern)) = (text_val.as_str(), pattern_val.as_str()) {
            match Regex::new(pattern) {
                Ok(re) => Ok(Value::bool_val(re.is_match(text))),
                Err(e) => Err(crate::error::Error::invalid_query(format!(
                    "Invalid regex pattern: {}",
                    e
                ))),
            }
        } else {
            Err(crate::error::Error::TypeMismatch {
                expected: "STRING, STRING".to_string(),
                actual: format!("{}, {}", text_val.data_type(), pattern_val.data_type()),
            })
        }
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

    fn schema_with_two_strings() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("text", DataType::String),
            Field::nullable("pattern", DataType::String),
        ])
    }

    #[test]
    fn returns_true_when_pattern_matches() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![
                Value::string("hello123world".into()),
                Value::string(r"\d+".into()),
            ]],
        );
        let args = vec![Expr::column("text"), Expr::column("pattern")];
        let result =
            ProjectionWithExprExec::evaluate_regexp_contains(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::bool_val(true));
    }

    #[test]
    fn returns_false_when_pattern_does_not_match() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![
                Value::string("hello world".into()),
                Value::string(r"\d+".into()),
            ]],
        );
        let args = vec![Expr::column("text"), Expr::column("pattern")];
        let result =
            ProjectionWithExprExec::evaluate_regexp_contains(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::bool_val(false));
    }

    #[test]
    fn supports_complex_regex_patterns() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![
                Value::string("test@example.com".into()),
                Value::string(r"^[a-z]+@[a-z]+\.[a-z]+$".into()),
            ]],
        );
        let args = vec![Expr::column("text"), Expr::column("pattern")];
        let result =
            ProjectionWithExprExec::evaluate_regexp_contains(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::bool_val(true));
    }

    #[test]
    fn errors_on_invalid_regex_pattern() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![
                Value::string("hello".into()),
                Value::string("[invalid(".into()),
            ]],
        );
        let args = vec![Expr::column("text"), Expr::column("pattern")];
        let err = ProjectionWithExprExec::evaluate_regexp_contains(&args, &batch, 0)
            .expect_err("invalid pattern");
        assert_error_contains(&err, "Invalid regex pattern");
    }

    #[test]
    fn propagates_null_text() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![Value::null(), Value::string(r"\d+".into())]],
        );
        let args = vec![Expr::column("text"), Expr::column("pattern")];
        let result =
            ProjectionWithExprExec::evaluate_regexp_contains(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_pattern() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![Value::string("hello".into()), Value::null()]],
        );
        let args = vec![Expr::column("text"), Expr::column("pattern")];
        let result =
            ProjectionWithExprExec::evaluate_regexp_contains(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("text", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hello".into())]]);
        let err =
            ProjectionWithExprExec::evaluate_regexp_contains(&[Expr::column("text")], &batch, 0)
                .expect_err("missing argument");
        assert_error_contains(&err, "REGEXP_CONTAINS");
    }
}
