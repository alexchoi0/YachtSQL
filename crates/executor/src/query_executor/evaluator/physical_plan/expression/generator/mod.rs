mod generate_uuid;
mod generate_uuid_array;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_generator_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "GENERATE_UUID" | "GEN_RANDOM_UUID" | "UUID_GENERATE_V4" | "UUIDV4" => {
                Self::eval_generate_uuid(args)
            }
            "UUID_GENERATE_V1" => Self::eval_generate_uuid(args),
            "GENERATE_UUID_ARRAY" => Self::eval_generate_uuid_array(args, batch, row_idx),
            "UUIDV7" => Self::eval_generate_uuidv7(args),
            "SESSION_USER" => Self::eval_session_user(args),
            "CURRENT_USER" | "CURRENTUSER" | "USER" => Self::eval_current_user(args),
            _ => Err(Error::unsupported_feature(format!(
                "Unknown generator function: {}",
                name
            ))),
        }
    }

    fn eval_session_user(args: &[Expr]) -> Result<Value> {
        Self::validate_zero_args("SESSION_USER", args)?;
        Ok(Value::string("default_user".to_string()))
    }

    fn eval_current_user(args: &[Expr]) -> Result<Value> {
        Self::validate_zero_args("CURRENT_USER", args)?;
        Ok(Value::string("default_user".to_string()))
    }
}
