use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_concat(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let mut result_bytes: Vec<u8> = Vec::new();
        let mut has_fixed_string = false;

        for arg in args {
            let val = Self::evaluate_expr(arg, batch, row_idx)?;

            if val.is_null() {
                continue;
            }

            if let Some(fs) = val.as_fixed_string() {
                has_fixed_string = true;
                result_bytes.extend_from_slice(&fs.data);
            } else if let Some(s) = val.as_str() {
                result_bytes.extend_from_slice(s.as_bytes());
            } else if let Some(i) = val.as_i64() {
                result_bytes.extend_from_slice(i.to_string().as_bytes());
            } else if let Some(f) = val.as_f64() {
                result_bytes.extend_from_slice(f.to_string().as_bytes());
            } else if let Some(b) = val.as_bool() {
                result_bytes.extend_from_slice(b.to_string().as_bytes());
            }
        }

        if has_fixed_string {
            Ok(Value::string(
                String::from_utf8_lossy(&result_bytes).to_string(),
            ))
        } else {
            Ok(Value::string(
                String::from_utf8_lossy(&result_bytes).to_string(),
            ))
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

    #[test]
    fn concatenates_strings() {
        let schema = Schema::from_fields(vec![
            Field::nullable("a", DataType::String),
            Field::nullable("b", DataType::String),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::string("Hello".into()),
                Value::string(" World".into()),
            ]],
        );
        let args = vec![Expr::column("a"), Expr::column("b")];
        let result = ProjectionWithExprExec::evaluate_concat(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("Hello World".into()));
    }

    #[test]
    fn concatenates_with_integer() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("num", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("Number: ".into()), Value::int64(42)]],
        );
        let args = vec![Expr::column("str"), Expr::column("num")];
        let result = ProjectionWithExprExec::evaluate_concat(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("Number: 42".into()));
    }

    #[test]
    fn concatenates_with_float() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("num", DataType::Float64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("Pi: ".into()), Value::float64(3.14)]],
        );
        let args = vec![Expr::column("str"), Expr::column("num")];
        let result = ProjectionWithExprExec::evaluate_concat(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("Pi: 3.14".into()));
    }

    #[test]
    fn skips_null_values() {
        let schema = Schema::from_fields(vec![
            Field::nullable("a", DataType::String),
            Field::nullable("b", DataType::String),
            Field::nullable("c", DataType::String),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::string("Hello".into()),
                Value::null(),
                Value::string("World".into()),
            ]],
        );
        let args = vec![Expr::column("a"), Expr::column("b"), Expr::column("c")];
        let result = ProjectionWithExprExec::evaluate_concat(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("HelloWorld".into()));
    }

    #[test]
    fn returns_empty_string_for_no_args() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let result = ProjectionWithExprExec::evaluate_concat(&[], &batch, 0).expect("success");
        assert_eq!(result, Value::string("".into()));
    }
}
