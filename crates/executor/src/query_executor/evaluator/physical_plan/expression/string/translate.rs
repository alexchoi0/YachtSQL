use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_translate(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 3 {
            return Err(crate::error::Error::invalid_query(
                "TRANSLATE requires exactly 3 arguments (string, from, to)".to_string(),
            ));
        }

        let string_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let from_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let to_val = Self::evaluate_expr(&args[2], batch, row_idx)?;

        if let (Some(s), Some(from), Some(to)) =
            (string_val.as_str(), from_val.as_str(), to_val.as_str())
        {
            let mut result = String::new();
            let from_chars: Vec<char> = from.chars().collect();
            let to_chars: Vec<char> = to.chars().collect();

            for ch in s.chars() {
                if let Some(pos) = from_chars.iter().position(|&c| c == ch) {
                    if pos < to_chars.len() {
                        result.push(to_chars[pos]);
                    }
                } else {
                    result.push(ch);
                }
            }

            Ok(Value::string(result))
        } else if string_val.is_null() || from_val.is_null() || to_val.is_null() {
            Ok(Value::null())
        } else {
            Err(crate::error::Error::TypeMismatch {
                expected: "STRING, STRING, STRING".to_string(),
                actual: format!(
                    "{}, {}, {}",
                    string_val.data_type(),
                    from_val.data_type(),
                    to_val.data_type()
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
            Field::nullable("str", DataType::String),
            Field::nullable("from", DataType::String),
            Field::nullable("to", DataType::String),
        ])
    }

    #[test]
    fn translates_characters() {
        let batch = create_batch(
            schema_with_three_strings(),
            vec![vec![
                Value::string("hello".into()),
                Value::string("el".into()),
                Value::string("ip".into()),
            ]],
        );
        let args = vec![
            Expr::column("str"),
            Expr::column("from"),
            Expr::column("to"),
        ];
        let result = ProjectionWithExprExec::evaluate_translate(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("hippo".into()));
    }

    #[test]
    fn removes_characters_when_to_is_shorter() {
        let batch = create_batch(
            schema_with_three_strings(),
            vec![vec![
                Value::string("hello".into()),
                Value::string("lo".into()),
                Value::string("x".into()),
            ]],
        );
        let args = vec![
            Expr::column("str"),
            Expr::column("from"),
            Expr::column("to"),
        ];
        let result = ProjectionWithExprExec::evaluate_translate(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("hexx".into()));
    }

    #[test]
    fn preserves_unmapped_characters() {
        let batch = create_batch(
            schema_with_three_strings(),
            vec![vec![
                Value::string("abc123".into()),
                Value::string("abc".into()),
                Value::string("xyz".into()),
            ]],
        );
        let args = vec![
            Expr::column("str"),
            Expr::column("from"),
            Expr::column("to"),
        ];
        let result = ProjectionWithExprExec::evaluate_translate(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("xyz123".into()));
    }

    #[test]
    fn propagates_null_string() {
        let batch = create_batch(
            schema_with_three_strings(),
            vec![vec![
                Value::null(),
                Value::string("a".into()),
                Value::string("b".into()),
            ]],
        );
        let args = vec![
            Expr::column("str"),
            Expr::column("from"),
            Expr::column("to"),
        ];
        let result = ProjectionWithExprExec::evaluate_translate(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_from() {
        let batch = create_batch(
            schema_with_three_strings(),
            vec![vec![
                Value::string("hello".into()),
                Value::null(),
                Value::string("b".into()),
            ]],
        );
        let args = vec![
            Expr::column("str"),
            Expr::column("from"),
            Expr::column("to"),
        ];
        let result = ProjectionWithExprExec::evaluate_translate(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_to() {
        let batch = create_batch(
            schema_with_three_strings(),
            vec![vec![
                Value::string("hello".into()),
                Value::string("a".into()),
                Value::null(),
            ]],
        );
        let args = vec![
            Expr::column("str"),
            Expr::column("from"),
            Expr::column("to"),
        ];
        let result = ProjectionWithExprExec::evaluate_translate(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("str", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hello".into())]]);
        let err = ProjectionWithExprExec::evaluate_translate(&[Expr::column("str")], &batch, 0)
            .expect_err("missing arguments");
        assert_error_contains(&err, "TRANSLATE");
    }
}
