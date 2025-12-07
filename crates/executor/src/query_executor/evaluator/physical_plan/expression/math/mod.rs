mod abs;
mod acos;
mod asin;
mod atan;
mod atan2;
mod ceil;
mod cos;
mod degrees;
mod exp;
mod floor;
mod gamma;
mod ln;
mod log;
mod log10;
mod modulo;
mod pi;
mod power;
mod radians;
mod random;
mod round;
mod safe_add;
mod safe_divide;
mod safe_multiply;
mod safe_negate;
mod safe_subtract;
mod sign;
mod sin;
mod sqrt;
mod tan;
mod trunc;

use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_math_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "SIGN" => Self::eval_sign(args, batch, row_idx),
            "ABS" => Self::eval_abs(args, batch, row_idx),
            "CEIL" | "CEILING" => Self::eval_ceil(args, batch, row_idx),
            "FLOOR" => Self::eval_floor(args, batch, row_idx),
            "ROUND" => Self::eval_round(args, batch, row_idx),
            "TRUNC" | "TRUNCATE" => Self::eval_trunc(args, batch, row_idx),
            "MOD" => Self::eval_mod(args, batch, row_idx),

            "POWER" | "POW" => Self::eval_power(args, batch, row_idx),
            "SQRT" => Self::eval_sqrt(args, batch, row_idx),
            "EXP" => Self::eval_exp(args, batch, row_idx),

            "LN" => Self::eval_ln(args, batch, row_idx),
            "LOG" => Self::eval_log(args, batch, row_idx),
            "LOG10" => Self::eval_log10(args, batch, row_idx),

            "SIN" => Self::eval_sin(args, batch, row_idx),
            "COS" => Self::eval_cos(args, batch, row_idx),
            "TAN" => Self::eval_tan(args, batch, row_idx),
            "ASIN" => Self::eval_asin(args, batch, row_idx),
            "ACOS" => Self::eval_acos(args, batch, row_idx),
            "ATAN" => Self::eval_atan(args, batch, row_idx),
            "ATAN2" => Self::eval_atan2(args, batch, row_idx),
            "DEGREES" => Self::eval_degrees(args, batch, row_idx),
            "RADIANS" => Self::eval_radians(args, batch, row_idx),

            "PI" => Self::eval_pi(args),

            "RANDOM" | "RAND" => Self::eval_random(name, args),

            "SAFE_DIVIDE" => Self::eval_safe_divide(args, batch, row_idx),
            "SAFE_MULTIPLY" => Self::eval_safe_multiply(args, batch, row_idx),
            "SAFE_ADD" => Self::eval_safe_add(args, batch, row_idx),
            "SAFE_SUBTRACT" => Self::eval_safe_subtract(args, batch, row_idx),
            "SAFE_NEGATE" => Self::eval_safe_negate(args, batch, row_idx),

            "GAMMA" => Self::eval_gamma(args, batch, row_idx),
            "LGAMMA" => Self::eval_lgamma(args, batch, row_idx),

            _ => Err(crate::error::Error::invalid_query(format!(
                "Unknown mathematical function: {}",
                name
            ))),
        }
    }
}
