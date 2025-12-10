use std::str::FromStr;

use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
use rust_decimal::Decimal;
#[allow(unused_imports)]
use rust_decimal::prelude::FromPrimitive;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_int8(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::eval_to_int_impl::<i8>(args, batch, row_idx, "toInt8")
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_int16(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::eval_to_int_impl::<i16>(args, batch, row_idx, "toInt16")
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_int32(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::eval_to_int_impl::<i32>(args, batch, row_idx, "toInt32")
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_int64(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::eval_to_int_impl::<i64>(args, batch, row_idx, "toInt64")
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_uint8(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::eval_to_uint_impl::<u8>(args, batch, row_idx, "toUInt8")
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_uint16(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::eval_to_uint_impl::<u16>(args, batch, row_idx, "toUInt16")
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_uint32(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::eval_to_uint_impl::<u32>(args, batch, row_idx, "toUInt32")
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_uint64(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("toUInt64 requires 1 argument"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let n: u64 = convert_to_u64(&val, "toUInt64")?;
        Ok(Value::int64(n as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_float32(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("toFloat32 requires 1 argument"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f: f64 = convert_to_float(&val, "toFloat32")?;
        Ok(Value::float64(f as f32 as f64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_float64(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("toFloat64 requires 1 argument"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f: f64 = convert_to_float(&val, "toFloat64")?;
        Ok(Value::float64(f))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_string(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("toString requires 1 argument"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        Ok(Value::string(val.to_string()))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_fixed_string(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query("toFixedString requires 2 arguments"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let length_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let length = length_val.as_i64().ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: length_val.data_type().to_string(),
        })? as usize;

        if let Some(s) = val.as_str() {
            return Ok(Value::fixed_string_from_str(s, length));
        }
        if let Some(bytes) = val.as_bytes() {
            return Ok(Value::fixed_string_from_bytes(bytes.to_vec(), length));
        }
        if let Some(fs) = val.as_fixed_string() {
            return Ok(Value::fixed_string_from_bytes(fs.data.clone(), length));
        }
        Err(Error::TypeMismatch {
            expected: "STRING or BYTES".to_string(),
            actual: val.data_type().to_string(),
        })
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_ch_to_datetime(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() || args.len() > 2 {
            return Err(Error::invalid_query("toDateTime requires 1 or 2 arguments"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(dt) = val.as_datetime() {
            return Ok(Value::datetime(dt));
        }
        if let Some(ts) = val.as_timestamp() {
            return Ok(Value::datetime(ts));
        }
        if let Some(d) = val.as_date() {
            let dt = d.and_hms_opt(0, 0, 0).unwrap();
            return Ok(Value::datetime(Utc.from_utc_datetime(&dt)));
        }
        if let Some(s) = val.as_str() {
            let dt = parse_datetime_string(s)?;
            return Ok(Value::datetime(dt));
        }
        Err(Error::TypeMismatch {
            expected: "DATETIME, TIMESTAMP, DATE, or STRING".to_string(),
            actual: val.data_type().to_string(),
        })
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_datetime64(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() || args.len() > 3 {
            return Err(Error::invalid_query(
                "toDateTime64 requires 1 to 3 arguments",
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(ts) = val.as_timestamp() {
            return Ok(Value::timestamp(ts));
        }
        if let Some(s) = val.as_str() {
            let dt = parse_datetime_string(s)?;
            return Ok(Value::timestamp(dt));
        }
        Err(Error::TypeMismatch {
            expected: "TIMESTAMP or STRING".to_string(),
            actual: val.data_type().to_string(),
        })
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_decimal32(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::eval_to_decimal_impl(args, batch, row_idx, "toDecimal32")
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_decimal64(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::eval_to_decimal_impl(args, batch, row_idx, "toDecimal64")
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_decimal128(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::eval_to_decimal_impl(args, batch, row_idx, "toDecimal128")
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_int64_or_null(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("toInt64OrNull requires 1 argument"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        match try_convert_to_int64(&val) {
            Ok(n) => Ok(Value::int64(n)),
            Err(_) => Ok(Value::null()),
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_int64_or_zero(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("toInt64OrZero requires 1 argument"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::int64(0));
        }
        match try_convert_to_int64(&val) {
            Ok(n) => Ok(Value::int64(n)),
            Err(_) => Ok(Value::int64(0)),
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_float64_or_null(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("toFloat64OrNull requires 1 argument"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        match try_convert_to_float64(&val) {
            Ok(n) => Ok(Value::float64(n)),
            Err(_) => Ok(Value::null()),
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_float64_or_zero(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("toFloat64OrZero requires 1 argument"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::float64(0.0));
        }
        match try_convert_to_float64(&val) {
            Ok(n) => Ok(Value::float64(n)),
            Err(_) => Ok(Value::float64(0.0)),
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_date_or_null(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("toDateOrNull requires 1 argument"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        match try_parse_date(&val) {
            Ok(d) => Ok(Value::date(d)),
            Err(_) => Ok(Value::null()),
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_datetime_or_null(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("toDateTimeOrNull requires 1 argument"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        match try_parse_datetime(&val) {
            Ok(dt) => Ok(Value::timestamp(dt)),
            Err(_) => Ok(Value::null()),
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_reinterpret_as_int64(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "reinterpretAsInt64 requires 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(s) = val.as_str() {
            let bytes = s.as_bytes();
            let mut buf = [0u8; 8];
            let len = bytes.len().min(8);
            buf[..len].copy_from_slice(&bytes[..len]);
            return Ok(Value::int64(i64::from_le_bytes(buf)));
        }
        if let Some(bytes) = val.as_bytes() {
            let mut buf = [0u8; 8];
            let len = bytes.len().min(8);
            buf[..len].copy_from_slice(&bytes[..len]);
            return Ok(Value::int64(i64::from_le_bytes(buf)));
        }
        if let Some(n) = val.as_i64() {
            return Ok(Value::int64(n));
        }
        Err(Error::TypeMismatch {
            expected: "STRING or BYTES".to_string(),
            actual: val.data_type().to_string(),
        })
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_reinterpret_as_string(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "reinterpretAsString requires 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(n) = val.as_i64() {
            let bytes = n.to_le_bytes();
            let end = bytes.iter().position(|&b| b == 0).unwrap_or(8);
            let s = String::from_utf8_lossy(&bytes[..end]).to_string();
            return Ok(Value::string(s));
        }
        if let Some(s) = val.as_str() {
            return Ok(Value::string(s.to_string()));
        }
        if let Some(bytes) = val.as_bytes() {
            let s = String::from_utf8_lossy(bytes).to_string();
            return Ok(Value::string(s));
        }
        Err(Error::TypeMismatch {
            expected: "INT64, STRING or BYTES".to_string(),
            actual: val.data_type().to_string(),
        })
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_accurate_cast(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query("accurateCast requires 2 arguments"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let type_arg = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let target_type = type_arg.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: type_arg.data_type().to_string(),
        })?;
        accurate_cast_impl(&val, target_type)
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_accurate_cast_or_null(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "accurateCastOrNull requires 2 arguments",
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let type_arg = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let target_type = type_arg.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: type_arg.data_type().to_string(),
        })?;
        match accurate_cast_impl(&val, target_type) {
            Ok(v) => Ok(v),
            Err(_) => Ok(Value::null()),
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_parse_datetime(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query("parseDateTime requires 2 arguments"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let format = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let s = val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: val.data_type().to_string(),
        })?;
        let fmt = format.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: format.data_type().to_string(),
        })?;
        let rust_fmt = clickhouse_to_rust_format(fmt);
        let dt = NaiveDateTime::parse_from_str(s, &rust_fmt).map_err(|_| {
            Error::invalid_query(format!("Cannot parse '{}' with format '{}'", s, fmt))
        })?;
        Ok(Value::timestamp(Utc.from_utc_datetime(&dt)))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_parse_datetime_best_effort(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "parseDateTimeBestEffort requires 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let s = val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: val.data_type().to_string(),
        })?;
        let dt = parse_datetime_best_effort(s)?;
        Ok(Value::timestamp(dt))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_parse_datetime_best_effort_or_null(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "parseDateTimeBestEffortOrNull requires 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let s = match val.as_str() {
            Some(s) => s,
            None => return Ok(Value::null()),
        };
        match parse_datetime_best_effort(s) {
            Ok(dt) => Ok(Value::timestamp(dt)),
            Err(_) => Ok(Value::null()),
        }
    }

    fn eval_to_int_impl<T>(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        func_name: &str,
    ) -> Result<Value>
    where
        T: TryFrom<i64> + Into<i64>,
        <T as TryFrom<i64>>::Error: std::fmt::Debug,
    {
        if args.len() != 1 {
            return Err(Error::invalid_query(format!(
                "{} requires 1 argument",
                func_name
            )));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let n: i64 = convert_to_i64(&val, func_name)?;
        let bounded: T = T::try_from(n).map_err(|_| {
            Error::invalid_query(format!("{}: value {} out of range", func_name, n))
        })?;
        Ok(Value::int64(bounded.into()))
    }

    fn eval_to_uint_impl<T>(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        func_name: &str,
    ) -> Result<Value>
    where
        T: TryFrom<u64> + Into<i64>,
        <T as TryFrom<u64>>::Error: std::fmt::Debug,
    {
        if args.len() != 1 {
            return Err(Error::invalid_query(format!(
                "{} requires 1 argument",
                func_name
            )));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let n: u64 = convert_to_u64(&val, func_name)?;
        let bounded: T = T::try_from(n).map_err(|_| {
            Error::invalid_query(format!("{}: value {} out of range", func_name, n))
        })?;
        Ok(Value::int64(bounded.into()))
    }

    fn eval_to_decimal_impl(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        func_name: &str,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(format!(
                "{} requires 2 arguments",
                func_name
            )));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let _scale = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(d) = val.as_numeric() {
            return Ok(Value::numeric(d));
        }
        if let Some(n) = val.as_i64() {
            return Ok(Value::numeric(Decimal::from(n)));
        }
        if let Some(f) = val.as_f64() {
            let d = Decimal::from_f64_retain(f).ok_or_else(|| {
                Error::invalid_query(format!("{}: cannot convert {} to decimal", func_name, f))
            })?;
            return Ok(Value::numeric(d));
        }
        if let Some(s) = val.as_str() {
            let d = Decimal::from_str(s).map_err(|_| {
                Error::invalid_query(format!("{}: cannot parse '{}' as decimal", func_name, s))
            })?;
            return Ok(Value::numeric(d));
        }
        Err(Error::TypeMismatch {
            expected: "NUMERIC, INT64, FLOAT64, or STRING".to_string(),
            actual: val.data_type().to_string(),
        })
    }
}

fn convert_to_i64(val: &Value, func_name: &str) -> Result<i64> {
    if let Some(n) = val.as_i64() {
        return Ok(n);
    }
    if let Some(f) = val.as_f64() {
        return Ok(f as i64);
    }
    if let Some(d) = val.as_numeric() {
        return d.to_string().parse::<i64>().map_err(|_| {
            Error::invalid_query(format!("{}: cannot convert {} to int64", func_name, d))
        });
    }
    if let Some(s) = val.as_str() {
        return s.parse::<i64>().map_err(|_| {
            Error::invalid_query(format!("{}: cannot parse '{}' as int64", func_name, s))
        });
    }
    if let Some(b) = val.as_bool() {
        return Ok(if b { 1 } else { 0 });
    }
    Err(Error::TypeMismatch {
        expected: "INT64, FLOAT64, NUMERIC, STRING, or BOOLEAN".to_string(),
        actual: val.data_type().to_string(),
    })
}

fn convert_to_u64(val: &Value, func_name: &str) -> Result<u64> {
    if let Some(n) = val.as_i64() {
        if n < 0 {
            return Err(Error::invalid_query(format!(
                "{}: cannot convert negative value {} to unsigned",
                func_name, n
            )));
        }
        return Ok(n as u64);
    }
    if let Some(f) = val.as_f64() {
        if f < 0.0 {
            return Err(Error::invalid_query(format!(
                "{}: cannot convert negative value {} to unsigned",
                func_name, f
            )));
        }
        return Ok(f as u64);
    }
    if let Some(d) = val.as_numeric() {
        let n: i64 = d.to_string().parse().map_err(|_| {
            Error::invalid_query(format!("{}: cannot convert {} to u64", func_name, d))
        })?;
        if n < 0 {
            return Err(Error::invalid_query(format!(
                "{}: cannot convert negative value {} to unsigned",
                func_name, n
            )));
        }
        return Ok(n as u64);
    }
    if let Some(s) = val.as_str() {
        return s.parse::<u64>().map_err(|_| {
            Error::invalid_query(format!("{}: cannot parse '{}' as u64", func_name, s))
        });
    }
    if let Some(b) = val.as_bool() {
        return Ok(if b { 1 } else { 0 });
    }
    Err(Error::TypeMismatch {
        expected: "INT64, FLOAT64, NUMERIC, STRING, or BOOLEAN".to_string(),
        actual: val.data_type().to_string(),
    })
}

fn convert_to_float(val: &Value, func_name: &str) -> Result<f64> {
    if let Some(n) = val.as_i64() {
        return Ok(n as f64);
    }
    if let Some(f) = val.as_f64() {
        return Ok(f);
    }
    if let Some(d) = val.as_numeric() {
        let f: f64 = d.to_string().parse().map_err(|_| {
            Error::invalid_query(format!("{}: cannot convert {} to float", func_name, d))
        })?;
        return Ok(f);
    }
    if let Some(s) = val.as_str() {
        let f: f64 = s.parse().map_err(|_| {
            Error::invalid_query(format!("{}: cannot parse '{}' as float", func_name, s))
        })?;
        return Ok(f);
    }
    if let Some(b) = val.as_bool() {
        return Ok(if b { 1.0 } else { 0.0 });
    }
    Err(Error::TypeMismatch {
        expected: "INT64, FLOAT64, NUMERIC, STRING, or BOOLEAN".to_string(),
        actual: val.data_type().to_string(),
    })
}

fn try_convert_to_int64(val: &Value) -> Result<i64> {
    if let Some(n) = val.as_i64() {
        return Ok(n);
    }
    if let Some(f) = val.as_f64() {
        return Ok(f as i64);
    }
    if let Some(d) = val.as_numeric() {
        return d
            .to_string()
            .parse::<i64>()
            .map_err(|_| Error::invalid_query("conversion failed"));
    }
    if let Some(s) = val.as_str() {
        return s
            .parse::<i64>()
            .map_err(|_| Error::invalid_query("conversion failed"));
    }
    if let Some(b) = val.as_bool() {
        return Ok(if b { 1 } else { 0 });
    }
    Err(Error::invalid_query("conversion failed"))
}

fn try_convert_to_float64(val: &Value) -> Result<f64> {
    if let Some(n) = val.as_i64() {
        return Ok(n as f64);
    }
    if let Some(f) = val.as_f64() {
        return Ok(f);
    }
    if let Some(d) = val.as_numeric() {
        return d
            .to_string()
            .parse::<f64>()
            .map_err(|_| Error::invalid_query("conversion failed"));
    }
    if let Some(s) = val.as_str() {
        return s
            .parse::<f64>()
            .map_err(|_| Error::invalid_query("conversion failed"));
    }
    if let Some(b) = val.as_bool() {
        return Ok(if b { 1.0 } else { 0.0 });
    }
    Err(Error::invalid_query("conversion failed"))
}

fn try_parse_date(val: &Value) -> Result<NaiveDate> {
    if let Some(d) = val.as_date() {
        return Ok(d);
    }
    if let Some(ts) = val.as_timestamp() {
        return Ok(ts.date_naive());
    }
    if let Some(s) = val.as_str() {
        return NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|_| Error::invalid_query("date parse failed"));
    }
    Err(Error::invalid_query("cannot convert to date"))
}

fn try_parse_datetime(val: &Value) -> Result<chrono::DateTime<Utc>> {
    if let Some(ts) = val.as_timestamp() {
        return Ok(ts);
    }
    if let Some(d) = val.as_date() {
        let dt = d.and_hms_opt(0, 0, 0).unwrap();
        return Ok(Utc.from_utc_datetime(&dt));
    }
    if let Some(s) = val.as_str() {
        return parse_datetime_string(s);
    }
    Err(Error::invalid_query("cannot convert to datetime"))
}

fn parse_datetime_string(s: &str) -> Result<chrono::DateTime<Utc>> {
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ];
    for fmt in &formats {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(Utc.from_utc_datetime(&dt));
        }
        if let Ok(d) = NaiveDate::parse_from_str(s, fmt) {
            let dt = d.and_hms_opt(0, 0, 0).unwrap();
            return Ok(Utc.from_utc_datetime(&dt));
        }
    }
    Err(Error::invalid_query(format!(
        "Cannot parse '{}' as datetime",
        s
    )))
}

fn accurate_cast_impl(val: &Value, target_type: &str) -> Result<Value> {
    match target_type.to_uppercase().as_str() {
        "INT8" => {
            let n = convert_to_i64(val, "accurateCast")?;
            if n < i8::MIN as i64 || n > i8::MAX as i64 {
                return Err(Error::invalid_query(format!(
                    "accurateCast: {} out of Int8 range",
                    n
                )));
            }
            Ok(Value::int64(n))
        }
        "INT16" => {
            let n = convert_to_i64(val, "accurateCast")?;
            if n < i16::MIN as i64 || n > i16::MAX as i64 {
                return Err(Error::invalid_query(format!(
                    "accurateCast: {} out of Int16 range",
                    n
                )));
            }
            Ok(Value::int64(n))
        }
        "INT32" => {
            let n = convert_to_i64(val, "accurateCast")?;
            if n < i32::MIN as i64 || n > i32::MAX as i64 {
                return Err(Error::invalid_query(format!(
                    "accurateCast: {} out of Int32 range",
                    n
                )));
            }
            Ok(Value::int64(n))
        }
        "INT64" => {
            let n = convert_to_i64(val, "accurateCast")?;
            Ok(Value::int64(n))
        }
        "FLOAT32" | "FLOAT64" => {
            let f: f64 = convert_to_float(val, "accurateCast")?;
            Ok(Value::float64(f))
        }
        "STRING" => Ok(Value::string(val.to_string())),
        _ => Err(Error::invalid_query(format!(
            "accurateCast: unsupported target type '{}'",
            target_type
        ))),
    }
}

fn clickhouse_to_rust_format(fmt: &str) -> String {
    fmt.replace("%i", "%M").replace("%s", "%S")
}

fn parse_datetime_best_effort(s: &str) -> Result<chrono::DateTime<Utc>> {
    let formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%b %d, %Y %I:%M %p",
        "%b %d, %Y %H:%M",
        "%B %d, %Y %I:%M %p",
        "%B %d, %Y",
        "%d %b %Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%Y%m%d%H%M%S",
        "%Y%m%d",
    ];
    for fmt in &formats {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(Utc.from_utc_datetime(&dt));
        }
        if let Ok(d) = NaiveDate::parse_from_str(s, fmt) {
            let dt = d.and_hms_opt(0, 0, 0).unwrap();
            return Ok(Utc.from_utc_datetime(&dt));
        }
    }
    Err(Error::invalid_query(format!(
        "parseDateTimeBestEffort: cannot parse '{}'",
        s
    )))
}

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_low_cardinality(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("toLowCardinality requires 1 argument"));
        }
        Self::evaluate_expr(&args[0], batch, row_idx)
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_low_cardinality_indices(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "lowCardinalityIndices requires 1 argument",
            ));
        }
        Self::evaluate_expr(&args[0], batch, row_idx)?;
        Ok(Value::int64(0))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_low_cardinality_keys(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "lowCardinalityKeys requires 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        Ok(Value::array(vec![val]))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_uuid(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("toUUID requires 1 argument"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(s) = val.as_str() {
            return yachtsql_core::types::parse_uuid_strict(s);
        }
        Err(Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: val.data_type().to_string(),
        })
    }
}
