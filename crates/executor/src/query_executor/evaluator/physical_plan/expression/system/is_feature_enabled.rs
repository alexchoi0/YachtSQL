use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::{FEATURE_REGISTRY_CONTEXT, ProjectionWithExprExec};
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_is_feature_enabled(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("yachtsql.is_feature_enabled", args, 1)?;
        let feature_value = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if feature_value.is_null() {
            return Ok(Value::null());
        }

        let feature_str = if let Some(s) = feature_value.as_str() {
            s.trim().to_string()
        } else {
            return Err(Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: feature_value.data_type().to_string(),
            });
        };

        let registry = FEATURE_REGISTRY_CONTEXT
            .with(|ctx| ctx.borrow().clone())
            .ok_or_else(|| {
                Error::InternalError(
                    "Feature registry context missing for yachtsql.is_feature_enabled".to_string(),
                )
            })?;

        let feature_id = registry
            .all_features()
            .find(|feature| {
                feature
                    .id
                    .as_str()
                    .eq_ignore_ascii_case(feature_str.as_str())
            })
            .map(|feature| feature.id)
            .ok_or_else(|| {
                Error::unsupported_feature(format!("Unknown feature id '{}'", feature_str))
            })?;

        let enabled = registry.is_enabled(feature_id);
        Ok(Value::bool_val(enabled))
    }
}
