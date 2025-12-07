use std::rc::Rc;

use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

#[allow(dead_code)]
impl ProjectionWithExprExec {
    pub(crate) fn evaluate_between_expression(
        value: &crate::types::Value,
        low: &crate::types::Value,
        high: &crate::types::Value,
        negated: bool,
    ) -> Result<crate::types::Value> {
        use yachtsql_optimizer::expr::BinaryOp;

        let ge_low = Self::evaluate_binary_op(value, &BinaryOp::GreaterThanOrEqual, low)?;
        let le_high = Self::evaluate_binary_op(value, &BinaryOp::LessThanOrEqual, high)?;

        let between = ge_low.as_bool() == Some(true) && le_high.as_bool() == Some(true);
        let result = if negated { !between } else { between };
        Ok(Value::bool_val(result))
    }

    pub(crate) fn evaluate_in_list_expression(
        value: &crate::types::Value,
        list: &[crate::types::Value],
        negated: bool,
    ) -> Result<crate::types::Value> {
        let mut found = false;
        let mut has_null = false;

        for item_value in list {
            if item_value.is_null() {
                has_null = true;
                continue;
            }

            if Self::values_equal(value, item_value) {
                found = true;
                break;
            }
        }

        if found {
            Ok(Value::bool_val(!negated))
        } else if has_null {
            Ok(Value::null())
        } else {
            Ok(Value::bool_val(negated))
        }
    }

    pub(super) fn evaluate_scalar_in_predicate(
        expr: &Rc<Expr>,
        list: &[Expr],
        negated: bool,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let value = Self::evaluate_expr(expr, batch, row_idx)?;
        let list_values: Result<Vec<_>> = list
            .iter()
            .map(|item_expr| Self::evaluate_expr(item_expr, batch, row_idx))
            .collect();
        Self::evaluate_in_list_expression(&value, &list_values?, negated)
    }

    pub(super) fn evaluate_tuple_in_predicate(
        expr: &Rc<Expr>,
        list: &[Expr],
        negated: bool,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let left_tuple = Self::evaluate_tuple_values(expr, batch, row_idx)?;

        let right_tuples: Result<Vec<Vec<Value>>> = list
            .iter()
            .map(|item_expr| Self::evaluate_tuple_values(item_expr, batch, row_idx))
            .collect();

        Self::evaluate_tuple_in_expression(&left_tuple, &right_tuples?, negated)
    }

    pub(super) fn evaluate_tuple_in_list_with_coercion(
        tuple: &[Expr],
        list: &[Vec<Expr>],
        negated: bool,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let left_values: Result<Vec<Value>> = tuple
            .iter()
            .map(|e| Self::evaluate_expr(e, batch, row_idx))
            .collect();

        let right_tuples: Result<Vec<Vec<Value>>> = list
            .iter()
            .map(|tuple_exprs| {
                tuple_exprs
                    .iter()
                    .map(|e| Self::evaluate_expr(e, batch, row_idx))
                    .collect()
            })
            .collect();

        crate::types::tuple_ops::evaluate_tuple_in(&left_values?, &right_tuples?, negated)
    }

    pub(super) fn evaluate_tuple_in_subquery_with_coercion(
        _tuple: &[Expr],
        _plan: &Rc<crate::optimizer::plan::PlanNode>,
        _negated: bool,
        _batch: &Table,
        _row_idx: usize,
    ) -> Result<Value> {
        Err(crate::error::Error::InvalidOperation(
            "Tuple IN subquery must be evaluated through QueryExecutor context".to_string(),
        ))
    }

    pub(super) fn evaluate_tuple_values(
        expr: &Expr,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Vec<Value>> {
        match expr {
            Expr::Tuple(exprs) => exprs
                .iter()
                .map(|e| Self::evaluate_expr(e, batch, row_idx))
                .collect(),
            _ => {
                let val = Self::evaluate_expr(expr, batch, row_idx)?;
                Ok(vec![val])
            }
        }
    }

    pub(super) fn evaluate_tuple_as_struct(
        exprs: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let values: Result<Vec<_>> = exprs
            .iter()
            .map(|e| Self::evaluate_expr(e, batch, row_idx))
            .collect();
        let values = values?;

        let fields: Vec<(String, Value)> = values
            .into_iter()
            .enumerate()
            .map(|(idx, val)| (format!("field_{}", idx), val))
            .collect();

        Ok(Value::struct_val(fields.into_iter().collect()))
    }

    pub(super) fn evaluate_tuple_in_expression(
        left_tuple: &[Value],
        right_tuples: &[Vec<Value>],
        negated: bool,
    ) -> Result<Value> {
        Self::validate_tuple_arity(left_tuple.len(), right_tuples)?;

        let match_result = Self::find_matching_tuple(left_tuple, right_tuples);

        Self::apply_three_valued_in_logic(match_result, negated)
    }

    pub(super) fn validate_tuple_arity(expected_arity: usize, tuples: &[Vec<Value>]) -> Result<()> {
        for tuple in tuples {
            if tuple.len() != expected_arity {
                return Err(crate::error::Error::invalid_query(format!(
                    "Tuple arity mismatch in IN predicate: expected {} elements, got {}",
                    expected_arity,
                    tuple.len()
                )));
            }
        }
        Ok(())
    }

    pub(super) fn find_matching_tuple(left: &[Value], candidates: &[Vec<Value>]) -> Option<bool> {
        let mut has_unknown = false;

        for right in candidates {
            match Self::compare_tuples_with_nulls(left, right) {
                Some(true) => return Some(true),
                Some(false) => continue,
                None => has_unknown = true,
            }
        }

        if has_unknown { None } else { Some(false) }
    }

    pub(super) fn apply_three_valued_in_logic(
        match_result: Option<bool>,
        negated: bool,
    ) -> Result<Value> {
        match (match_result, negated) {
            (Some(true), false) | (Some(false), true) => Ok(Value::bool_val(true)),
            (Some(false), false) | (Some(true), true) => Ok(Value::bool_val(false)),
            (None, _) => Ok(Value::null()),
        }
    }

    pub(super) fn compare_tuples_with_nulls(left: &[Value], right: &[Value]) -> Option<bool> {
        let mut has_null = false;

        for (l, r) in left.iter().zip(right.iter()) {
            if l.is_null() || r.is_null() {
                has_null = true;
                continue;
            }

            if !Self::values_equal(l, r) {
                return Some(false);
            }
        }

        if has_null { None } else { Some(true) }
    }

    pub(super) fn extract_array_from_value_expr(
        right: &crate::optimizer::expr::Expr,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Vec<crate::types::Value>> {
        let value = Self::evaluate_expr(right, batch, row_idx)?;
        if value.is_null() {
            return Ok(Vec::new());
        }
        if let Some(arr) = value.as_array() {
            return Ok(arr.to_vec());
        }
        Err(crate::error::Error::invalid_query(format!(
            "ANY/ALL requires array, got {:?}",
            value
        )))
    }

    pub(super) fn extract_array_for_quantified_op(
        right: &crate::optimizer::expr::Expr,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Vec<crate::types::Value>> {
        use yachtsql_optimizer::expr::Expr;

        match right {
            Expr::Subquery { plan: _ } => {
                todo!("Subquery support in physical plan ANY/ALL not yet implemented")
            }

            Expr::Literal(crate::optimizer::expr::LiteralValue::Array(elements)) => {
                let mut values = Vec::with_capacity(elements.len());
                for elem in elements {
                    values.push(Self::evaluate_expr(elem, batch, row_idx)?);
                }
                Ok(values)
            }

            Expr::Column { .. }
            | Expr::Function { .. }
            | Expr::Aggregate { .. }
            | Expr::Cast { .. } => Self::extract_array_from_value_expr(right, batch, row_idx),

            _ => Err(crate::error::Error::invalid_query(format!(
                "ANY/ALL requires array or subquery, got {:?}",
                right
            ))),
        }
    }

    pub(super) fn evaluate_quantified_comparison_array(
        lhs_value: &crate::types::Value,
        compare_op: &crate::optimizer::expr::BinaryOp,
        rhs_values: &[crate::types::Value],
        is_any: bool,
    ) -> Result<crate::types::Value> {
        if lhs_value == &Value::null() {
            return Ok(Value::null());
        }

        let mut has_null_in_rhs = false;

        for rhs_value in rhs_values {
            if rhs_value == &Value::null() {
                has_null_in_rhs = true;
                continue;
            }

            let comparison_result = Self::evaluate_binary_op(lhs_value, compare_op, rhs_value)?;

            if is_any && comparison_result.as_bool() == Some(true) {
                return Ok(Value::bool_val(true));
            }
            if !is_any && comparison_result.as_bool() == Some(false) {
                return Ok(Value::bool_val(false));
            }
        }

        match has_null_in_rhs {
            true => Ok(Value::null()),
            false => Ok(Value::bool_val(!is_any)),
        }
    }
}
