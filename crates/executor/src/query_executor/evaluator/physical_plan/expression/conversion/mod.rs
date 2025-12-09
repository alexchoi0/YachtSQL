mod clickhouse;
mod to_char;
mod to_number;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_conversion_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "TO_NUMBER" => Self::eval_to_number(args, batch, row_idx),
            "TO_CHAR" => Self::eval_to_char(args, batch, row_idx),
            "TOINT8" => Self::eval_to_int8(args, batch, row_idx),
            "TOINT16" => Self::eval_to_int16(args, batch, row_idx),
            "TOINT32" => Self::eval_to_int32(args, batch, row_idx),
            "TOINT64" => Self::eval_to_int64(args, batch, row_idx),
            "TOUINT8" => Self::eval_to_uint8(args, batch, row_idx),
            "TOUINT16" => Self::eval_to_uint16(args, batch, row_idx),
            "TOUINT32" => Self::eval_to_uint32(args, batch, row_idx),
            "TOUINT64" => Self::eval_to_uint64(args, batch, row_idx),
            "TOFLOAT32" => Self::eval_to_float32(args, batch, row_idx),
            "TOFLOAT64" => Self::eval_to_float64(args, batch, row_idx),
            "TOSTRING" => Self::eval_to_string(args, batch, row_idx),
            "TOFIXEDSTRING" => Self::eval_to_fixed_string(args, batch, row_idx),
            "TODATETIME" => Self::eval_ch_to_datetime(args, batch, row_idx),
            "TODATETIME64" => Self::eval_to_datetime64(args, batch, row_idx),
            "TODECIMAL32" => Self::eval_to_decimal32(args, batch, row_idx),
            "TODECIMAL64" => Self::eval_to_decimal64(args, batch, row_idx),
            "TODECIMAL128" => Self::eval_to_decimal128(args, batch, row_idx),
            "TOINT64ORNULL" => Self::eval_to_int64_or_null(args, batch, row_idx),
            "TOINT64ORZERO" => Self::eval_to_int64_or_zero(args, batch, row_idx),
            "TOFLOAT64ORNULL" => Self::eval_to_float64_or_null(args, batch, row_idx),
            "TOFLOAT64ORZERO" => Self::eval_to_float64_or_zero(args, batch, row_idx),
            "TODATEORNULL" => Self::eval_to_date_or_null(args, batch, row_idx),
            "TODATETIMEORNULL" => Self::eval_to_datetime_or_null(args, batch, row_idx),
            "REINTERPRETASINT64" => Self::eval_reinterpret_as_int64(args, batch, row_idx),
            "REINTERPRETASSTRING" => Self::eval_reinterpret_as_string(args, batch, row_idx),
            "ACCURATECAST" => Self::eval_accurate_cast(args, batch, row_idx),
            "ACCURATECASTORNULL" => Self::eval_accurate_cast_or_null(args, batch, row_idx),
            "PARSEDATETIME" => Self::eval_parse_datetime(args, batch, row_idx),
            "PARSEDATETIMEBESTEFFORT" => {
                Self::eval_parse_datetime_best_effort(args, batch, row_idx)
            }
            "PARSEDATETIMEBESTEFFORTORNULL" => {
                Self::eval_parse_datetime_best_effort_or_null(args, batch, row_idx)
            }
            _ => Err(Error::unsupported_feature(format!(
                "Unknown conversion function: {}",
                name
            ))),
        }
    }
}
