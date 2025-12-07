use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_regexp_replace(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        use regex::Regex;
        if args.len() != 3 {
            return Err(crate::error::Error::invalid_query(
                "REGEXP_REPLACE requires exactly 3 arguments (string, pattern, replacement)"
                    .to_string(),
            ));
        }
        let text_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let pattern_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let replacement_val = Self::evaluate_expr(&args[2], batch, row_idx)?;

        if let (Some(text), Some(pattern), Some(replacement)) = (
            text_val.as_str(),
            pattern_val.as_str(),
            replacement_val.as_str(),
        ) {
            match Regex::new(pattern) {
                Ok(re) => {
                    let result = re.replace_all(text, replacement).to_string();
                    Ok(Value::string(result))
                }
                Err(e) => Err(crate::error::Error::invalid_query(format!(
                    "Invalid regex pattern: {}",
                    e
                ))),
            }
        } else if text_val.is_null() || pattern_val.is_null() || replacement_val.is_null() {
            Ok(Value::null())
        } else {
            Err(crate::error::Error::TypeMismatch {
                expected: "STRING, STRING, STRING".to_string(),
                actual: format!(
                    "{}, {}, {}",
                    text_val.data_type(),
                    pattern_val.data_type(),
                    replacement_val.data_type()
                ),
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

    fn schema_with_three_strings() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("text", DataType::String),
            Field::nullable("pattern", DataType::String),
            Field::nullable("replacement", DataType::String),
        ])
    }

    #[test]
    fn replaces_all_matches() {
        let batch = create_batch(
            schema_with_three_strings(),
            vec![vec![
                Value::string("hello123world456".into()),
                Value::string(r"\d+".into()),
                Value::string("X".into()),
            ]],
        );
        let args = vec![
            Expr::column("text"),
            Expr::column("pattern"),
            Expr::column("replacement"),
        ];
        let result =
            ProjectionWithExprExec::evaluate_regexp_replace(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("helloXworldX".into()));
    }

    #[test]
    fn replaces_with_empty_string() {
        let batch = create_batch(
            schema_with_three_strings(),
            vec![vec![
                Value::string("hello123world".into()),
                Value::string(r"\d+".into()),
                Value::string("".into()),
            ]],
        );
        let args = vec![
            Expr::column("text"),
            Expr::column("pattern"),
            Expr::column("replacement"),
        ];
        let result =
            ProjectionWithExprExec::evaluate_regexp_replace(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("helloworld".into()));
    }

    #[test]
    fn returns_original_when_no_match() {
        let batch = create_batch(
            schema_with_three_strings(),
            vec![vec![
                Value::string("hello world".into()),
                Value::string(r"\d+".into()),
                Value::string("X".into()),
            ]],
        );
        let args = vec![
            Expr::column("text"),
            Expr::column("pattern"),
            Expr::column("replacement"),
        ];
        let result =
            ProjectionWithExprExec::evaluate_regexp_replace(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("hello world".into()));
    }

    #[test]
    fn errors_on_invalid_regex_pattern() {
        let batch = create_batch(
            schema_with_three_strings(),
            vec![vec![
                Value::string("hello".into()),
                Value::string("[invalid(".into()),
                Value::string("X".into()),
            ]],
        );
        let args = vec![
            Expr::column("text"),
            Expr::column("pattern"),
            Expr::column("replacement"),
        ];
        let err = ProjectionWithExprExec::evaluate_regexp_replace(&args, &batch, 0)
            .expect_err("invalid pattern");
        assert_error_contains(&err, "Invalid regex pattern");
    }

    #[test]
    fn propagates_null_text() {
        let batch = create_batch(
            schema_with_three_strings(),
            vec![vec![
                Value::null(),
                Value::string(r"\d+".into()),
                Value::string("X".into()),
            ]],
        );
        let args = vec![
            Expr::column("text"),
            Expr::column("pattern"),
            Expr::column("replacement"),
        ];
        let result =
            ProjectionWithExprExec::evaluate_regexp_replace(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_pattern() {
        let batch = create_batch(
            schema_with_three_strings(),
            vec![vec![
                Value::string("hello123".into()),
                Value::null(),
                Value::string("X".into()),
            ]],
        );
        let args = vec![
            Expr::column("text"),
            Expr::column("pattern"),
            Expr::column("replacement"),
        ];
        let result =
            ProjectionWithExprExec::evaluate_regexp_replace(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_replacement() {
        let batch = create_batch(
            schema_with_three_strings(),
            vec![vec![
                Value::string("hello123".into()),
                Value::string(r"\d+".into()),
                Value::null(),
            ]],
        );
        let args = vec![
            Expr::column("text"),
            Expr::column("pattern"),
            Expr::column("replacement"),
        ];
        let result =
            ProjectionWithExprExec::evaluate_regexp_replace(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("text", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hello".into())]]);
        let err =
            ProjectionWithExprExec::evaluate_regexp_replace(&[Expr::column("text")], &batch, 0)
                .expect_err("missing arguments");
        assert_error_contains(&err, "REGEXP_REPLACE");
    }
}
