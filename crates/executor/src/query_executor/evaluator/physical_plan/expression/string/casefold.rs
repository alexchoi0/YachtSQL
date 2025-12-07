use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_casefold(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("CASEFOLD", args, 1)?;
        Self::apply_string_unary("CASEFOLD", &args[0], batch, row_idx, |s| {
            let mut result = String::with_capacity(s.len());
            for c in s.chars() {
                match c {
                    'ß' => result.push_str("ss"),
                    'ẞ' => result.push_str("ss"),
                    'ﬁ' => result.push_str("fi"),
                    'ﬂ' => result.push_str("fl"),
                    'ﬀ' => result.push_str("ff"),
                    'ﬃ' => result.push_str("ffi"),
                    'ﬄ' => result.push_str("ffl"),
                    'ﬅ' => result.push_str("st"),
                    'ﬆ' => result.push_str("st"),
                    _ => {
                        for lc in c.to_lowercase() {
                            result.push(lc);
                        }
                    }
                }
            }
            result
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

    #[test]
    fn test_casefold_uppercase() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("HELLO".into())]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_casefold(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("hello".into()));
    }

    #[test]
    fn test_casefold_mixed() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("HeLLo WoRLD".into())]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_casefold(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("hello world".into()));
    }

    #[test]
    fn test_casefold_null() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_casefold(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }
}
