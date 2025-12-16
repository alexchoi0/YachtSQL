use indexmap::IndexMap;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_ir::FunctionName;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_tuple_function(
        name: &FunctionName,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            FunctionName::Tuple => Self::eval_tuple(args, batch, row_idx),
            FunctionName::TupleElement => Self::eval_tuple_element(args, batch, row_idx),
            FunctionName::Untuple => Self::eval_untuple(args, batch, row_idx),
            _ => Err(Error::unsupported_feature(format!(
                "Unknown tuple function: {}",
                name.as_str()
            ))),
        }
    }

    fn eval_tuple(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        let mut result_map = IndexMap::new();
        for (i, arg) in args.iter().enumerate() {
            let value = Self::evaluate_expr(arg, batch, row_idx)?;
            let field_name = (i + 1).to_string();
            result_map.insert(field_name, value);
        }
        Ok(Value::struct_val(result_map))
    }

    fn eval_tuple_element(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "tupleElement requires 2 arguments".to_string(),
            ));
        }
        let tuple_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let index_or_name = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if tuple_val.is_null() {
            return Ok(Value::null());
        }

        let struct_map = tuple_val.as_struct().ok_or_else(|| Error::TypeMismatch {
            expected: "TUPLE/STRUCT".to_string(),
            actual: tuple_val.data_type().to_string(),
        })?;

        if let Some(idx) = index_or_name.as_i64() {
            let idx = idx as usize;
            if idx == 0 || idx > struct_map.len() {
                return Err(Error::invalid_query(format!(
                    "Tuple index {} out of bounds (tuple has {} elements)",
                    idx,
                    struct_map.len()
                )));
            }
            Ok(struct_map
                .get_index(idx - 1)
                .map(|(_, v)| v.clone())
                .unwrap_or(Value::null()))
        } else if let Some(name) = index_or_name.as_str() {
            if let Some(value) = struct_map.get(name) {
                Ok(value.clone())
            } else if let Some((_, value)) = struct_map
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
            {
                Ok(value.clone())
            } else {
                Err(Error::invalid_query(format!(
                    "Tuple does not have field '{}'",
                    name
                )))
            }
        } else {
            Err(Error::InvalidQuery(
                "tupleElement second argument must be an integer index or string field name"
                    .to_string(),
            ))
        }
    }

    fn eval_untuple(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "untuple requires 1 argument".to_string(),
            ));
        }
        Self::evaluate_expr(&args[0], batch, row_idx)
    }
}
