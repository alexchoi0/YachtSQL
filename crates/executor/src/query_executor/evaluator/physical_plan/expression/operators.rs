use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::{BinaryOp, Expr};

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_and(
        left: &Expr,
        right: &Expr,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::evaluate_and_internal(left, right, batch, row_idx, crate::DialectType::PostgreSQL)
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_and_internal(
        left: &Expr,
        right: &Expr,
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        let left_val = Self::evaluate_expr_internal(left, batch, row_idx, dialect)?;

        if left_val.is_null() {
            let right_val = Self::evaluate_expr_internal(right, batch, row_idx, dialect)?;
            if let Some(false) = right_val.as_bool() {
                return Ok(Value::bool_val(false));
            }
            return Ok(Value::null());
        }

        if let Some(b) = left_val.as_bool() {
            if !b {
                return Ok(Value::bool_val(false));
            }
            return Self::evaluate_expr_internal(right, batch, row_idx, dialect);
        }

        Err(Error::TypeMismatch {
            expected: "BOOL".to_string(),
            actual: left_val.data_type().to_string(),
        })
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_or(
        left: &Expr,
        right: &Expr,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::evaluate_or_internal(left, right, batch, row_idx, crate::DialectType::PostgreSQL)
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_or_internal(
        left: &Expr,
        right: &Expr,
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        let left_val = Self::evaluate_expr_internal(left, batch, row_idx, dialect)?;

        if left_val.is_null() {
            let right_val = Self::evaluate_expr_internal(right, batch, row_idx, dialect)?;
            if let Some(true) = right_val.as_bool() {
                return Ok(Value::bool_val(true));
            }
            return Ok(Value::null());
        }

        if let Some(b) = left_val.as_bool() {
            if b {
                return Ok(Value::bool_val(true));
            }
            return Self::evaluate_expr_internal(right, batch, row_idx, dialect);
        }

        Err(Error::TypeMismatch {
            expected: "BOOL".to_string(),
            actual: left_val.data_type().to_string(),
        })
    }

    pub(super) fn int64_arithmetic(
        op: &crate::optimizer::expr::BinaryOp,
        l: i64,
        r: i64,
    ) -> Result<crate::types::Value> {
        match op {
            BinaryOp::Add => l.checked_add(r).map(Value::int64).ok_or_else(|| {
                crate::error::Error::ExecutionError("INT64 overflow in addition".to_string())
            }),
            BinaryOp::Subtract => l.checked_sub(r).map(Value::int64).ok_or_else(|| {
                crate::error::Error::ExecutionError("INT64 overflow in subtraction".to_string())
            }),
            BinaryOp::Multiply => l.checked_mul(r).map(Value::int64).ok_or_else(|| {
                crate::error::Error::ExecutionError("INT64 overflow in multiplication".to_string())
            }),
            BinaryOp::Divide if r != 0 => Ok(Value::int64(l / r)),
            BinaryOp::Divide => Err(crate::error::Error::ExecutionError(
                "Division by zero".to_string(),
            )),
            BinaryOp::Modulo if r != 0 => Ok(Value::int64(l % r)),
            BinaryOp::Modulo => Err(crate::error::Error::ExecutionError(
                "Modulo by zero".to_string(),
            )),
            BinaryOp::BitwiseAnd => Ok(Value::int64(l & r)),
            BinaryOp::BitwiseOr => Ok(Value::int64(l | r)),
            BinaryOp::BitwiseXor => Ok(Value::int64(l ^ r)),
            BinaryOp::ShiftLeft => {
                let shift = r as u32;
                Ok(Value::int64(l.wrapping_shl(shift)))
            }
            BinaryOp::ShiftRight => {
                let shift = r as u32;
                Ok(Value::int64(l.wrapping_shr(shift)))
            }
            _ => Err(crate::error::Error::unsupported_feature(format!(
                "Operator {:?} not supported for Int64 arithmetic",
                op
            ))),
        }
    }

    pub(super) fn float64_arithmetic(
        op: &crate::optimizer::expr::BinaryOp,
        l: f64,
        r: f64,
    ) -> Result<crate::types::Value> {
        match op {
            BinaryOp::Add => Ok(Value::float64(l + r)),
            BinaryOp::Subtract => Ok(Value::float64(l - r)),
            BinaryOp::Multiply => Ok(Value::float64(l * r)),
            BinaryOp::Divide => Ok(Value::float64(l / r)),
            _ => Err(crate::error::Error::unsupported_feature(format!(
                "Operator {:?} not supported for Float64 arithmetic",
                op
            ))),
        }
    }

    pub(super) fn numeric_comparison<T: PartialOrd>(
        op: &crate::optimizer::expr::BinaryOp,
        l: T,
        r: T,
    ) -> Result<bool> {
        match op {
            BinaryOp::Equal => Ok(l == r),
            BinaryOp::NotEqual => Ok(l != r),
            BinaryOp::LessThan => Ok(l < r),
            BinaryOp::LessThanOrEqual => Ok(l <= r),
            BinaryOp::GreaterThan => Ok(l > r),
            BinaryOp::GreaterThanOrEqual => Ok(l >= r),
            _ => Err(crate::error::Error::unsupported_feature(format!(
                "Operator {:?} not a comparison operator",
                op
            ))),
        }
    }

    pub(crate) fn evaluate_binary_op(
        left: &crate::types::Value,
        op: &crate::optimizer::expr::BinaryOp,
        right: &crate::types::Value,
    ) -> Result<crate::types::Value> {
        if left.is_null() || right.is_null() {
            return Ok(Value::null());
        }

        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            return match op {
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::LessThan
                | BinaryOp::LessThanOrEqual
                | BinaryOp::GreaterThan
                | BinaryOp::GreaterThanOrEqual => {
                    Self::numeric_comparison(op, l, r).map(Value::bool_val)
                }

                _ => Self::int64_arithmetic(op, l, r),
            };
        }

        if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
            return match op {
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::LessThan
                | BinaryOp::LessThanOrEqual
                | BinaryOp::GreaterThan
                | BinaryOp::GreaterThanOrEqual => {
                    Self::numeric_comparison(op, l, r).map(Value::bool_val)
                }

                _ => Self::float64_arithmetic(op, l, r),
            };
        }

        if let (Some(l), Some(r)) = (left.as_f64(), right.as_i64()) {
            return match op {
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::LessThan
                | BinaryOp::LessThanOrEqual
                | BinaryOp::GreaterThan
                | BinaryOp::GreaterThanOrEqual => {
                    Self::numeric_comparison(op, l, r as f64).map(Value::bool_val)
                }

                _ => Self::float64_arithmetic(op, l, r as f64),
            };
        }

        if let (Some(l), Some(r)) = (left.as_i64(), right.as_f64()) {
            return match op {
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::LessThan
                | BinaryOp::LessThanOrEqual
                | BinaryOp::GreaterThan
                | BinaryOp::GreaterThanOrEqual => {
                    Self::numeric_comparison(op, l as f64, r).map(Value::bool_val)
                }

                _ => Self::float64_arithmetic(op, l as f64, r),
            };
        }

        if let (Some(fs_l), Some(fs_r)) = (left.as_fixed_string(), right.as_fixed_string()) {
            return match op {
                BinaryOp::Equal => Ok(Value::bool_val(fs_l.data == fs_r.data)),
                BinaryOp::NotEqual => Ok(Value::bool_val(fs_l.data != fs_r.data)),
                BinaryOp::LessThan => Ok(Value::bool_val(fs_l.data < fs_r.data)),
                BinaryOp::LessThanOrEqual => Ok(Value::bool_val(fs_l.data <= fs_r.data)),
                BinaryOp::GreaterThan => Ok(Value::bool_val(fs_l.data > fs_r.data)),
                BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(fs_l.data >= fs_r.data)),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for FixedString",
                    op
                ))),
            };
        }

        if let (Some(fs), Some(s)) = (left.as_fixed_string(), right.as_str()) {
            let l = fs.to_string_lossy();
            return match op {
                BinaryOp::Equal => Ok(Value::bool_val(l.eq_ignore_ascii_case(s))),
                BinaryOp::NotEqual => Ok(Value::bool_val(!l.eq_ignore_ascii_case(s))),
                BinaryOp::LessThan => Ok(Value::bool_val(l.as_str() < s)),
                BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l.as_str() <= s)),
                BinaryOp::GreaterThan => Ok(Value::bool_val(l.as_str() > s)),
                BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l.as_str() >= s)),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for FixedString - STRING",
                    op
                ))),
            };
        }

        if let (Some(s), Some(fs)) = (left.as_str(), right.as_fixed_string()) {
            let r = fs.to_string_lossy();
            return match op {
                BinaryOp::Equal => Ok(Value::bool_val(s.eq_ignore_ascii_case(&r))),
                BinaryOp::NotEqual => Ok(Value::bool_val(!s.eq_ignore_ascii_case(&r))),
                BinaryOp::LessThan => Ok(Value::bool_val(s < r.as_str())),
                BinaryOp::LessThanOrEqual => Ok(Value::bool_val(s <= r.as_str())),
                BinaryOp::GreaterThan => Ok(Value::bool_val(s > r.as_str())),
                BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(s >= r.as_str())),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for STRING - FixedString",
                    op
                ))),
            };
        }

        if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
            return match op {
                BinaryOp::Equal => Ok(Value::bool_val(l.eq_ignore_ascii_case(r))),
                BinaryOp::NotEqual => Ok(Value::bool_val(!l.eq_ignore_ascii_case(r))),
                BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                BinaryOp::Concat => {
                    let mut result = l.to_string();
                    result.push_str(r);
                    Ok(Value::string(result))
                }
                BinaryOp::Like => Ok(Value::bool_val(crate::pattern_matching::matches_pattern(
                    l, r,
                ))),
                BinaryOp::NotLike => Ok(Value::bool_val(
                    !crate::pattern_matching::matches_pattern(l, r),
                )),
                BinaryOp::ILike => Ok(Value::bool_val(
                    crate::pattern_matching::matches_pattern_case_insensitive(l, r),
                )),
                BinaryOp::NotILike => Ok(Value::bool_val(
                    !crate::pattern_matching::matches_pattern_case_insensitive(l, r),
                )),
                BinaryOp::SimilarTo | BinaryOp::NotSimilarTo => {
                    let matches =
                        crate::pattern_matching::matches_similar_to(l, r).map_err(|e| {
                            crate::error::Error::invalid_query(format!(
                                "Invalid SIMILAR TO pattern: {}",
                                e
                            ))
                        })?;
                    let result = matches!(op, BinaryOp::SimilarTo) == matches;
                    Ok(Value::bool_val(result))
                }
                BinaryOp::RegexMatch | BinaryOp::RegexNotMatch => {
                    let matches = crate::pattern_matching::matches_regex(l, r)
                        .map_err(crate::error::Error::InvalidQuery)?;
                    let result = matches!(op, BinaryOp::RegexMatch) == matches;
                    Ok(Value::bool_val(result))
                }
                BinaryOp::RegexMatchI | BinaryOp::RegexNotMatchI => {
                    let matches = crate::pattern_matching::matches_regex_case_insensitive(l, r)
                        .map_err(crate::error::Error::InvalidQuery)?;
                    let result = matches!(op, BinaryOp::RegexMatchI) == matches;
                    Ok(Value::bool_val(result))
                }
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for String",
                    op
                ))),
            };
        }

        if let (Some(l), Some(r)) = (left.as_bool(), right.as_bool()) {
            return match op {
                BinaryOp::And => Ok(Value::bool_val(l && r)),
                BinaryOp::Or => Ok(Value::bool_val(l || r)),
                BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for Bool",
                    op
                ))),
            };
        }

        if let (Some(l), Some(r)) = (left.as_bytes(), right.as_bytes()) {
            return match op {
                BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                BinaryOp::Concat => {
                    let mut result = l.to_vec();
                    result.extend_from_slice(r);
                    Ok(Value::bytes(result))
                }
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for Bytes",
                    op
                ))),
            };
        }

        if let (Some(l), Some(r)) = (left.as_numeric(), right.as_numeric()) {
            return crate::query_executor::execution::evaluate_numeric_op(&l, op, &r);
        }

        if let (Some(l), Some(r_i64)) = (left.as_numeric(), right.as_i64()) {
            use rust_decimal::Decimal;
            let r_dec = Decimal::from(r_i64);
            return crate::query_executor::execution::evaluate_numeric_op(&l, op, &r_dec);
        }
        if let (Some(l_i64), Some(r)) = (left.as_i64(), right.as_numeric()) {
            use rust_decimal::Decimal;
            let l_dec = Decimal::from(l_i64);
            return crate::query_executor::execution::evaluate_numeric_op(&l_dec, op, &r);
        }

        if let (Some(l), Some(r_f64)) = (left.as_numeric(), right.as_f64()) {
            use rust_decimal::prelude::ToPrimitive;
            let l_f64 = l.to_f64().unwrap_or(0.0);
            return Self::float64_arithmetic(op, l_f64, r_f64);
        }
        if let (Some(l_f64), Some(r)) = (left.as_f64(), right.as_numeric()) {
            use rust_decimal::prelude::ToPrimitive;
            let r_f64 = r.to_f64().unwrap_or(0.0);
            return Self::float64_arithmetic(op, l_f64, r_f64);
        }

        if let (Some(l_struct), Some(r_struct)) = (left.as_struct(), right.as_struct()) {
            return match op {
                BinaryOp::Equal => {
                    if l_struct.len() != r_struct.len() {
                        return Ok(Value::bool_val(false));
                    }
                    for (l_val, r_val) in l_struct.values().zip(r_struct.values()) {
                        let cmp = Self::evaluate_binary_op(l_val, &BinaryOp::Equal, r_val)?;
                        match cmp.as_bool() {
                            Some(false) => return Ok(Value::bool_val(false)),
                            None => return Ok(Value::null()),
                            _ => {}
                        }
                    }
                    Ok(Value::bool_val(true))
                }
                BinaryOp::NotEqual => {
                    if l_struct.len() != r_struct.len() {
                        return Ok(Value::bool_val(true));
                    }
                    for (l_val, r_val) in l_struct.values().zip(r_struct.values()) {
                        let cmp = Self::evaluate_binary_op(l_val, &BinaryOp::Equal, r_val)?;
                        match cmp.as_bool() {
                            Some(false) => return Ok(Value::bool_val(true)),
                            None => return Ok(Value::null()),
                            _ => {}
                        }
                    }
                    Ok(Value::bool_val(false))
                }
                BinaryOp::LessThan => {
                    for (l_val, r_val) in l_struct.values().zip(r_struct.values()) {
                        let lt = Self::evaluate_binary_op(l_val, &BinaryOp::LessThan, r_val)?;
                        if let Some(true) = lt.as_bool() {
                            return Ok(Value::bool_val(true));
                        }
                        let eq = Self::evaluate_binary_op(l_val, &BinaryOp::Equal, r_val)?;
                        match eq.as_bool() {
                            Some(true) => continue,
                            None => return Ok(Value::null()),
                            _ => return Ok(Value::bool_val(false)),
                        }
                    }
                    Ok(Value::bool_val(false))
                }
                BinaryOp::LessThanOrEqual => {
                    for (l_val, r_val) in l_struct.values().zip(r_struct.values()) {
                        let lt = Self::evaluate_binary_op(l_val, &BinaryOp::LessThan, r_val)?;
                        if let Some(true) = lt.as_bool() {
                            return Ok(Value::bool_val(true));
                        }
                        let eq = Self::evaluate_binary_op(l_val, &BinaryOp::Equal, r_val)?;
                        match eq.as_bool() {
                            Some(true) => continue,
                            None => return Ok(Value::null()),
                            _ => return Ok(Value::bool_val(false)),
                        }
                    }
                    Ok(Value::bool_val(true))
                }
                BinaryOp::GreaterThan => {
                    for (l_val, r_val) in l_struct.values().zip(r_struct.values()) {
                        let gt = Self::evaluate_binary_op(l_val, &BinaryOp::GreaterThan, r_val)?;
                        if let Some(true) = gt.as_bool() {
                            return Ok(Value::bool_val(true));
                        }
                        let eq = Self::evaluate_binary_op(l_val, &BinaryOp::Equal, r_val)?;
                        match eq.as_bool() {
                            Some(true) => continue,
                            None => return Ok(Value::null()),
                            _ => return Ok(Value::bool_val(false)),
                        }
                    }
                    Ok(Value::bool_val(false))
                }
                BinaryOp::GreaterThanOrEqual => {
                    for (l_val, r_val) in l_struct.values().zip(r_struct.values()) {
                        let gt = Self::evaluate_binary_op(l_val, &BinaryOp::GreaterThan, r_val)?;
                        if let Some(true) = gt.as_bool() {
                            return Ok(Value::bool_val(true));
                        }
                        let eq = Self::evaluate_binary_op(l_val, &BinaryOp::Equal, r_val)?;
                        match eq.as_bool() {
                            Some(true) => continue,
                            None => return Ok(Value::null()),
                            _ => return Ok(Value::bool_val(false)),
                        }
                    }
                    Ok(Value::bool_val(true))
                }
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for STRUCT",
                    op
                ))),
            };
        }

        if let (Some(l), Some(r)) = (left.as_interval(), right.as_interval()) {
            return match op {
                BinaryOp::Add => yachtsql_functions::interval::interval_add(left, right),
                BinaryOp::Subtract => yachtsql_functions::interval::interval_subtract(left, right),
                BinaryOp::Equal => {
                    let l_micros = Self::interval_to_total_micros(l);
                    let r_micros = Self::interval_to_total_micros(r);
                    Ok(Value::bool_val(l_micros == r_micros))
                }
                BinaryOp::NotEqual => {
                    let l_micros = Self::interval_to_total_micros(l);
                    let r_micros = Self::interval_to_total_micros(r);
                    Ok(Value::bool_val(l_micros != r_micros))
                }
                BinaryOp::LessThan => {
                    let l_micros = Self::interval_to_total_micros(l);
                    let r_micros = Self::interval_to_total_micros(r);
                    Ok(Value::bool_val(l_micros < r_micros))
                }
                BinaryOp::LessThanOrEqual => {
                    let l_micros = Self::interval_to_total_micros(l);
                    let r_micros = Self::interval_to_total_micros(r);
                    Ok(Value::bool_val(l_micros <= r_micros))
                }
                BinaryOp::GreaterThan => {
                    let l_micros = Self::interval_to_total_micros(l);
                    let r_micros = Self::interval_to_total_micros(r);
                    Ok(Value::bool_val(l_micros > r_micros))
                }
                BinaryOp::GreaterThanOrEqual => {
                    let l_micros = Self::interval_to_total_micros(l);
                    let r_micros = Self::interval_to_total_micros(r);
                    Ok(Value::bool_val(l_micros >= r_micros))
                }
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for INTERVAL",
                    op
                ))),
            };
        }

        if left.as_interval().is_some() && (right.as_i64().is_some() || right.as_f64().is_some()) {
            let factor = if let Some(i) = right.as_i64() {
                Value::float64(i as f64)
            } else {
                right.clone()
            };
            return match op {
                BinaryOp::Multiply => {
                    yachtsql_functions::interval::interval_multiply(left, &factor)
                }
                BinaryOp::Divide => yachtsql_functions::interval::interval_divide(left, &factor),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for INTERVAL * scalar",
                    op
                ))),
            };
        }

        if (left.as_i64().is_some() || left.as_f64().is_some()) && right.as_interval().is_some() {
            let factor = if let Some(i) = left.as_i64() {
                Value::float64(i as f64)
            } else {
                left.clone()
            };
            return match op {
                BinaryOp::Multiply => {
                    yachtsql_functions::interval::interval_multiply(right, &factor)
                }
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for scalar * INTERVAL",
                    op
                ))),
            };
        }

        if let (Some(ts), Some(interval)) = (left.as_timestamp(), right.as_interval()) {
            return match op {
                BinaryOp::Add => Self::add_interval_to_timestamp(ts, interval),
                BinaryOp::Subtract => Self::subtract_interval_from_timestamp(ts, interval),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for TIMESTAMP +/- INTERVAL",
                    op
                ))),
            };
        }

        if let (Some(interval), Some(ts)) = (left.as_interval(), right.as_timestamp()) {
            return match op {
                BinaryOp::Add => Self::add_interval_to_timestamp(ts, interval),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for INTERVAL + TIMESTAMP",
                    op
                ))),
            };
        }

        if let (Some(l), Some(r)) = (left.as_timestamp(), right.as_timestamp()) {
            return match op {
                BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                BinaryOp::Subtract => {
                    let duration = l.signed_duration_since(r);
                    let micros = duration.num_microseconds().unwrap_or(0);
                    Ok(Value::interval(yachtsql_core::types::Interval::new(
                        0, 0, micros,
                    )))
                }
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for TIMESTAMP",
                    op
                ))),
            };
        }

        if let (Some(l), Some(r_str)) = (left.as_timestamp(), right.as_str()) {
            use chrono::{NaiveDateTime, TimeZone, Utc};
            if let Ok(dt) = NaiveDateTime::parse_from_str(r_str.trim(), "%Y-%m-%d %H:%M:%S%.f") {
                let r = Utc.from_utc_datetime(&dt);
                return match op {
                    BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                    BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                    BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                    BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                    BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                    BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                    _ => Err(crate::error::Error::unsupported_feature(format!(
                        "Operator {:?} not supported for TIMESTAMP vs STRING",
                        op
                    ))),
                };
            }
            if let Ok(dt) = NaiveDateTime::parse_from_str(r_str.trim(), "%Y-%m-%d %H:%M:%S") {
                let r = Utc.from_utc_datetime(&dt);
                return match op {
                    BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                    BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                    BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                    BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                    BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                    BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                    _ => Err(crate::error::Error::unsupported_feature(format!(
                        "Operator {:?} not supported for TIMESTAMP vs STRING",
                        op
                    ))),
                };
            }
        }

        if let (Some(l_str), Some(r)) = (left.as_str(), right.as_timestamp()) {
            use chrono::{NaiveDateTime, TimeZone, Utc};
            if let Ok(dt) = NaiveDateTime::parse_from_str(l_str.trim(), "%Y-%m-%d %H:%M:%S%.f") {
                let l = Utc.from_utc_datetime(&dt);
                return match op {
                    BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                    BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                    BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                    BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                    BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                    BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                    _ => Err(crate::error::Error::unsupported_feature(format!(
                        "Operator {:?} not supported for STRING vs TIMESTAMP",
                        op
                    ))),
                };
            }
            if let Ok(dt) = NaiveDateTime::parse_from_str(l_str.trim(), "%Y-%m-%d %H:%M:%S") {
                let l = Utc.from_utc_datetime(&dt);
                return match op {
                    BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                    BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                    BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                    BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                    BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                    BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                    _ => Err(crate::error::Error::unsupported_feature(format!(
                        "Operator {:?} not supported for STRING vs TIMESTAMP",
                        op
                    ))),
                };
            }
        }

        if let (Some(l), Some(r)) = (left.as_datetime(), right.as_datetime()) {
            return match op {
                BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for DATETIME",
                    op
                ))),
            };
        }

        if let (Some(l), Some(r_str)) = (left.as_datetime(), right.as_str()) {
            use chrono::{NaiveDateTime, TimeZone, Utc};
            if let Ok(dt) = NaiveDateTime::parse_from_str(r_str.trim(), "%Y-%m-%d %H:%M:%S%.f") {
                let r = Utc.from_utc_datetime(&dt);
                return match op {
                    BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                    BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                    BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                    BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                    BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                    BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                    _ => Err(crate::error::Error::unsupported_feature(format!(
                        "Operator {:?} not supported for DATETIME vs STRING",
                        op
                    ))),
                };
            }
            if let Ok(dt) = NaiveDateTime::parse_from_str(r_str.trim(), "%Y-%m-%d %H:%M:%S") {
                let r = Utc.from_utc_datetime(&dt);
                return match op {
                    BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                    BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                    BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                    BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                    BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                    BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                    _ => Err(crate::error::Error::unsupported_feature(format!(
                        "Operator {:?} not supported for DATETIME vs STRING",
                        op
                    ))),
                };
            }
        }

        if let (Some(l_str), Some(r)) = (left.as_str(), right.as_datetime()) {
            use chrono::{NaiveDateTime, TimeZone, Utc};
            if let Ok(dt) = NaiveDateTime::parse_from_str(l_str.trim(), "%Y-%m-%d %H:%M:%S%.f") {
                let l = Utc.from_utc_datetime(&dt);
                return match op {
                    BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                    BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                    BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                    BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                    BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                    BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                    _ => Err(crate::error::Error::unsupported_feature(format!(
                        "Operator {:?} not supported for STRING vs DATETIME",
                        op
                    ))),
                };
            }
            if let Ok(dt) = NaiveDateTime::parse_from_str(l_str.trim(), "%Y-%m-%d %H:%M:%S") {
                let l = Utc.from_utc_datetime(&dt);
                return match op {
                    BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                    BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                    BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                    BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                    BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                    BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                    _ => Err(crate::error::Error::unsupported_feature(format!(
                        "Operator {:?} not supported for STRING vs DATETIME",
                        op
                    ))),
                };
            }
        }

        if let (Some(l), Some(r)) = (left.as_date(), right.as_date()) {
            return match op {
                BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for DATE",
                    op
                ))),
            };
        }

        if let (Some(date), Some(interval)) = (left.as_date(), right.as_interval()) {
            return match op {
                BinaryOp::Add => Self::add_interval_to_date(date, interval),
                BinaryOp::Subtract => Self::subtract_interval_from_date(date, interval),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for DATE +/- INTERVAL",
                    op
                ))),
            };
        }

        if let (Some(interval), Some(date)) = (left.as_interval(), right.as_date()) {
            return match op {
                BinaryOp::Add => Self::add_interval_to_date(date, interval),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for INTERVAL + DATE",
                    op
                ))),
            };
        }

        if let (Some(l), Some(r)) = (left.as_time(), right.as_time()) {
            return match op {
                BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for TIME",
                    op
                ))),
            };
        }

        if let (Some(_l), Some(_r)) = (left.as_json(), right.as_json()) {
            return match op {
                BinaryOp::ArrayContains => yachtsql_functions::json::jsonb_contains(left, right),
                BinaryOp::ArrayContainedBy => yachtsql_functions::json::jsonb_contains(right, left),
                BinaryOp::Concat => yachtsql_functions::json::jsonb_concat(left, right),
                BinaryOp::Equal => {
                    let left_val = left.as_json();
                    let right_val = right.as_json();
                    Ok(Value::bool_val(left_val == right_val))
                }
                BinaryOp::NotEqual => {
                    let left_val = left.as_json();
                    let right_val = right.as_json();
                    Ok(Value::bool_val(left_val != right_val))
                }
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for JSON",
                    op
                ))),
            };
        }

        if left.as_json().is_some() && (right.as_str().is_some() || right.as_i64().is_some()) {
            return match op {
                BinaryOp::Subtract => yachtsql_functions::json::jsonb_delete(left, right),
                BinaryOp::HashMinus => yachtsql_functions::json::jsonb_delete_path(left, right),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for JSON - STRING/INT",
                    op
                ))),
            };
        }

        if left.as_json().is_some() && right.as_array().is_some() {
            return match op {
                BinaryOp::HashMinus => yachtsql_functions::json::jsonb_delete_path(left, right),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for JSON - ARRAY",
                    op
                ))),
            };
        }

        if let (Some(_l), Some(_r)) = (left.as_hstore(), right.as_hstore()) {
            return match op {
                BinaryOp::Concat => yachtsql_functions::hstore::hstore_concat(left, right),
                BinaryOp::Subtract => yachtsql_functions::hstore::hstore_delete_hstore(left, right),
                BinaryOp::ArrayContains => yachtsql_functions::hstore::hstore_contains(left, right),
                BinaryOp::ArrayContainedBy => {
                    yachtsql_functions::hstore::hstore_contained_by(left, right)
                }
                BinaryOp::Equal => yachtsql_functions::hstore::hstore_equal(left, right),
                BinaryOp::NotEqual => {
                    let eq = yachtsql_functions::hstore::hstore_equal(left, right)?;
                    match eq.as_bool() {
                        Some(b) => Ok(Value::bool_val(!b)),
                        None => Ok(Value::null()),
                    }
                }
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for HSTORE",
                    op
                ))),
            };
        }

        if left.as_hstore().is_some() && right.as_str().is_some() {
            return match op {
                BinaryOp::Subtract => yachtsql_functions::hstore::hstore_delete_key(left, right),
                BinaryOp::ArrayContains => {
                    let right_hstore = yachtsql_functions::hstore::hstore_from_text(right)?;
                    yachtsql_functions::hstore::hstore_contains(left, &right_hstore)
                }
                BinaryOp::ArrayContainedBy => {
                    let right_hstore = yachtsql_functions::hstore::hstore_from_text(right)?;
                    yachtsql_functions::hstore::hstore_contained_by(left, &right_hstore)
                }
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for HSTORE - STRING",
                    op
                ))),
            };
        }

        if left.as_hstore().is_some() && right.as_array().is_some() {
            return match op {
                BinaryOp::Subtract => yachtsql_functions::hstore::hstore_delete_keys(left, right),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for HSTORE - ARRAY",
                    op
                ))),
            };
        }

        if let (Some(l), Some(r)) = (left.as_macaddr(), right.as_macaddr()) {
            return match op {
                BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for MACADDR",
                    op
                ))),
            };
        }

        if let (Some(l), Some(r)) = (left.as_macaddr8(), right.as_macaddr8()) {
            return match op {
                BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for MACADDR8",
                    op
                ))),
            };
        }

        if let (Some(l), Some(r)) = (left.as_ipv4(), right.as_ipv4()) {
            return match op {
                BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for IPv4",
                    op
                ))),
            };
        }

        if let (Some(l), Some(r)) = (left.as_ipv6(), right.as_ipv6()) {
            return match op {
                BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for IPv6",
                    op
                ))),
            };
        }

        if let (Some(l), Some(r)) = (left.as_date32(), right.as_date32()) {
            return match op {
                BinaryOp::Equal => Ok(Value::bool_val(l.0 == r.0)),
                BinaryOp::NotEqual => Ok(Value::bool_val(l.0 != r.0)),
                BinaryOp::LessThan => Ok(Value::bool_val(l.0 < r.0)),
                BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l.0 <= r.0)),
                BinaryOp::GreaterThan => Ok(Value::bool_val(l.0 > r.0)),
                BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l.0 >= r.0)),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for Date32",
                    op
                ))),
            };
        }

        if left.as_date32().is_some() && right.as_str().is_some() {
            if let (Some(l), Some(r_str)) = (left.as_date32(), right.as_str()) {
                if let Some(r) = yachtsql_core::types::Date32Value::parse(r_str) {
                    return match op {
                        BinaryOp::Equal => Ok(Value::bool_val(l.0 == r.0)),
                        BinaryOp::NotEqual => Ok(Value::bool_val(l.0 != r.0)),
                        BinaryOp::LessThan => Ok(Value::bool_val(l.0 < r.0)),
                        BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l.0 <= r.0)),
                        BinaryOp::GreaterThan => Ok(Value::bool_val(l.0 > r.0)),
                        BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l.0 >= r.0)),
                        _ => Err(crate::error::Error::unsupported_feature(format!(
                            "Operator {:?} not supported for Date32",
                            op
                        ))),
                    };
                }
            }
        }

        if left.as_str().is_some() && right.as_date32().is_some() {
            if let (Some(l_str), Some(r)) = (left.as_str(), right.as_date32()) {
                if let Some(l) = yachtsql_core::types::Date32Value::parse(l_str) {
                    return match op {
                        BinaryOp::Equal => Ok(Value::bool_val(l.0 == r.0)),
                        BinaryOp::NotEqual => Ok(Value::bool_val(l.0 != r.0)),
                        BinaryOp::LessThan => Ok(Value::bool_val(l.0 < r.0)),
                        BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l.0 <= r.0)),
                        BinaryOp::GreaterThan => Ok(Value::bool_val(l.0 > r.0)),
                        BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l.0 >= r.0)),
                        _ => Err(crate::error::Error::unsupported_feature(format!(
                            "Operator {:?} not supported for Date32",
                            op
                        ))),
                    };
                }
            }
        }

        if left.as_date32().is_some() && right.as_i64().is_some() {
            if let (Some(l), Some(r)) = (left.as_date32(), right.as_i64()) {
                return match op {
                    BinaryOp::Add => Ok(Value::date32(yachtsql_core::types::Date32Value(
                        l.0 + r as i32,
                    ))),
                    BinaryOp::Subtract => Ok(Value::date32(yachtsql_core::types::Date32Value(
                        l.0 - r as i32,
                    ))),
                    _ => Err(crate::error::Error::unsupported_feature(format!(
                        "Operator {:?} not supported for Date32 and INT64",
                        op
                    ))),
                };
            }
        }

        if let (Some(l), Some(r)) = (left.as_inet(), right.as_inet()) {
            return match op {
                BinaryOp::Equal => Ok(Value::bool_val(
                    l.addr == r.addr && l.prefix_len == r.prefix_len,
                )),
                BinaryOp::NotEqual => Ok(Value::bool_val(
                    l.addr != r.addr || l.prefix_len != r.prefix_len,
                )),
                BinaryOp::LessThan => {
                    let cmp = Self::inet_compare(l, r);
                    Ok(Value::bool_val(cmp == std::cmp::Ordering::Less))
                }
                BinaryOp::LessThanOrEqual => {
                    let cmp = Self::inet_compare(l, r);
                    Ok(Value::bool_val(cmp != std::cmp::Ordering::Greater))
                }
                BinaryOp::GreaterThan => {
                    let cmp = Self::inet_compare(l, r);
                    Ok(Value::bool_val(cmp == std::cmp::Ordering::Greater))
                }
                BinaryOp::GreaterThanOrEqual => {
                    let cmp = Self::inet_compare(l, r);
                    Ok(Value::bool_val(cmp != std::cmp::Ordering::Less))
                }
                BinaryOp::InetContainedBy | BinaryOp::ShiftLeft => {
                    Ok(Value::bool_val(Self::inet_is_contained_by(l, r)))
                }
                BinaryOp::InetContains | BinaryOp::ShiftRight => {
                    Ok(Value::bool_val(Self::inet_is_contained_by(r, l)))
                }
                BinaryOp::InetContainedByOrEqual => {
                    Ok(Value::bool_val(Self::inet_is_contained_by_or_equal(l, r)))
                }
                BinaryOp::InetContainsOrEqual => {
                    Ok(Value::bool_val(Self::inet_is_contained_by_or_equal(r, l)))
                }
                BinaryOp::InetOverlap | BinaryOp::ArrayOverlap => {
                    Ok(Value::bool_val(Self::inet_overlaps(l, r)))
                }
                BinaryOp::BitwiseAnd => Self::inet_bitwise_and(l, r),
                BinaryOp::BitwiseOr => Self::inet_bitwise_or(l, r),
                BinaryOp::Add => Self::inet_add_int(l, &0i64),
                BinaryOp::Subtract => Self::inet_subtract_inet(l, r),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for INET",
                    op
                ))),
            };
        }

        if left.as_inet().is_some() && right.as_i64().is_some() {
            let inet = left.as_inet().unwrap();
            let offset = right.as_i64().unwrap();
            return match op {
                BinaryOp::Add => Self::inet_add_int(inet, &offset),
                BinaryOp::Subtract => Self::inet_add_int(inet, &(-offset)),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for INET + INT",
                    op
                ))),
            };
        }

        if let (Some(l), Some(r)) = (left.as_uuid(), right.as_uuid()) {
            return match op {
                BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                BinaryOp::LessThan => Ok(Value::bool_val(l < r)),
                BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l <= r)),
                BinaryOp::GreaterThan => Ok(Value::bool_val(l > r)),
                BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l >= r)),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for UUID",
                    op
                ))),
            };
        }

        if let (Some(l), Some(r)) = (left.as_range(), right.as_range()) {
            return match op {
                BinaryOp::Equal => Ok(Value::bool_val(l == r)),
                BinaryOp::NotEqual => Ok(Value::bool_val(l != r)),
                BinaryOp::ArrayContains => {
                    yachtsql_functions::range::range_contains_range(left, right)
                }
                BinaryOp::ArrayContainedBy => {
                    yachtsql_functions::range::range_contains_range(right, left)
                }
                BinaryOp::ArrayOverlap => yachtsql_functions::range::range_overlaps(left, right),
                BinaryOp::RangeAdjacent => yachtsql_functions::range::range_adjacent(left, right),
                BinaryOp::RangeStrictlyLeft | BinaryOp::ShiftLeft => {
                    yachtsql_functions::range::range_strictly_left(left, right)
                }
                BinaryOp::RangeStrictlyRight | BinaryOp::ShiftRight => {
                    yachtsql_functions::range::range_strictly_right(left, right)
                }
                BinaryOp::Add => yachtsql_functions::range::range_union(left, right),
                BinaryOp::Multiply => yachtsql_functions::range::range_intersection(left, right),
                BinaryOp::Subtract => yachtsql_functions::range::range_difference(left, right),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for RANGE",
                    op
                ))),
            };
        }

        if left.as_range().is_some() && right.as_range().is_none() {
            return match op {
                BinaryOp::ArrayContains => {
                    yachtsql_functions::range::range_contains_elem(left, right)
                }
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for RANGE @> element",
                    op
                ))),
            };
        }

        if left.as_range().is_none() && right.as_range().is_some() {
            return match op {
                BinaryOp::ArrayContainedBy => {
                    yachtsql_functions::range::range_contains_elem(right, left)
                }
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for element <@ RANGE",
                    op
                ))),
            };
        }

        let is_geometric_left = left.as_point().is_some()
            || left.as_circle().is_some()
            || matches!(left.data_type(), yachtsql_core::types::DataType::PgBox)
            || matches!(left.data_type(), yachtsql_core::types::DataType::Point)
            || matches!(left.data_type(), yachtsql_core::types::DataType::Circle);

        let is_geometric_right = right.as_point().is_some()
            || right.as_circle().is_some()
            || matches!(right.data_type(), yachtsql_core::types::DataType::PgBox)
            || matches!(right.data_type(), yachtsql_core::types::DataType::Point)
            || matches!(right.data_type(), yachtsql_core::types::DataType::Circle);

        if is_geometric_left || is_geometric_right {
            return match op {
                BinaryOp::ArrayContains | BinaryOp::GeometricContains => {
                    yachtsql_functions::geometric::contains(left, right)
                }
                BinaryOp::ArrayContainedBy | BinaryOp::GeometricContainedBy => {
                    yachtsql_functions::geometric::contained_by(left, right)
                }
                BinaryOp::ArrayOverlap | BinaryOp::GeometricOverlap => {
                    yachtsql_functions::geometric::overlaps(left, right)
                }
                BinaryOp::GeometricDistance | BinaryOp::VectorL2Distance => {
                    yachtsql_functions::geometric::distance(left, right)
                }
                BinaryOp::Add => yachtsql_functions::geometric::point_add(left, right),
                BinaryOp::Subtract => {
                    if left.as_point().is_some() && right.as_point().is_some() {
                        yachtsql_functions::geometric::point_subtract(left, right)
                    } else {
                        yachtsql_functions::geometric::distance(left, right)
                    }
                }
                BinaryOp::Multiply => yachtsql_functions::geometric::point_multiply(left, right),
                BinaryOp::Divide => yachtsql_functions::geometric::point_divide(left, right),
                _ => Err(crate::error::Error::unsupported_feature(format!(
                    "Operator {:?} not supported for geometric types",
                    op
                ))),
            };
        }

        match op {
            BinaryOp::VectorL2Distance => {
                crate::query_executor::execution::evaluate_vector_l2_distance(left, right)
            }
            BinaryOp::VectorInnerProduct => {
                crate::query_executor::execution::evaluate_vector_inner_product(left, right)
            }
            BinaryOp::VectorCosineDistance => {
                crate::query_executor::execution::evaluate_vector_cosine_distance(left, right)
            }

            BinaryOp::ArrayContains => yachtsql_functions::array::array_contains_array(left, right),
            BinaryOp::ArrayContainedBy => {
                yachtsql_functions::array::array_contained_by(left, right)
            }
            BinaryOp::ArrayOverlap => yachtsql_functions::array::array_overlap(left, right),

            BinaryOp::GeometricDistance => yachtsql_functions::geometric::distance(left, right),
            BinaryOp::GeometricContains => yachtsql_functions::geometric::contains(left, right),
            BinaryOp::GeometricContainedBy => {
                yachtsql_functions::geometric::contained_by(left, right)
            }
            BinaryOp::GeometricOverlap => yachtsql_functions::geometric::overlaps(left, right),
            _ => Err(crate::error::Error::TypeMismatch {
                expected: left.data_type().to_string(),
                actual: right.data_type().to_string(),
            }),
        }
    }

    pub(crate) fn evaluate_unary_op(
        op: &crate::optimizer::expr::UnaryOp,
        operand: &crate::types::Value,
    ) -> Result<crate::types::Value> {
        use yachtsql_optimizer::expr::UnaryOp;

        match op {
            UnaryOp::IsNull => Ok(Value::bool_val(operand.is_null())),
            UnaryOp::IsNotNull => Ok(Value::bool_val(!operand.is_null())),
            UnaryOp::Not => {
                if operand.is_null() {
                    return Ok(Value::null());
                }
                if let Some(b) = operand.as_bool() {
                    return Ok(Value::bool_val(!b));
                }
                Err(crate::error::Error::TypeMismatch {
                    expected: "BOOL".to_string(),
                    actual: operand.data_type().to_string(),
                })
            }
            UnaryOp::Negate => {
                if operand.is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = operand.as_i64() {
                    return i.checked_neg().map(Value::int64).ok_or_else(|| {
                        crate::error::Error::ExecutionError(format!(
                            "INT64 overflow: cannot negate {} (INT64::MIN)",
                            i
                        ))
                    });
                }
                if let Some(f) = operand.as_f64() {
                    return Ok(Value::float64(-f));
                }
                if let Some(n) = operand.as_numeric() {
                    return Ok(Value::numeric(-n));
                }
                Err(crate::error::Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    actual: operand.data_type().to_string(),
                })
            }
            UnaryOp::Plus => {
                if operand.is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = operand.as_i64() {
                    return Ok(Value::int64(i));
                }
                if let Some(f) = operand.as_f64() {
                    return Ok(Value::float64(f));
                }
                if let Some(n) = operand.as_numeric() {
                    return Ok(Value::numeric(n));
                }
                Err(crate::error::Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    actual: operand.data_type().to_string(),
                })
            }
            UnaryOp::BitwiseNot => {
                if operand.is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = operand.as_i64() {
                    return Ok(Value::int64(!i));
                }
                if let Some(inet) = operand.as_inet() {
                    return Self::inet_bitwise_not(inet);
                }
                Err(crate::error::Error::TypeMismatch {
                    expected: "integer or INET".to_string(),
                    actual: operand.data_type().to_string(),
                })
            }
        }
    }

    fn interval_to_total_micros(interval: &yachtsql_core::types::Interval) -> i128 {
        const MICROS_PER_DAY: i128 = 24 * 60 * 60 * 1_000_000;
        const MICROS_PER_MONTH: i128 = 30 * MICROS_PER_DAY;

        (interval.months as i128) * MICROS_PER_MONTH
            + (interval.days as i128) * MICROS_PER_DAY
            + (interval.micros as i128)
    }

    fn add_interval_to_timestamp(
        ts: chrono::DateTime<chrono::Utc>,
        interval: &yachtsql_core::types::Interval,
    ) -> Result<Value> {
        use chrono::{Datelike, Duration, Months, TimeZone, Utc};

        let mut result = ts;

        if interval.months != 0 {
            if interval.months > 0 {
                result = result
                    .checked_add_months(Months::new(interval.months as u32))
                    .ok_or_else(|| {
                        crate::error::Error::ExecutionError(
                            "Timestamp overflow when adding months".to_string(),
                        )
                    })?;
            } else {
                result = result
                    .checked_sub_months(Months::new((-interval.months) as u32))
                    .ok_or_else(|| {
                        crate::error::Error::ExecutionError(
                            "Timestamp underflow when subtracting months".to_string(),
                        )
                    })?;
            }
        }

        if interval.days != 0 {
            result = result
                .checked_add_signed(Duration::days(interval.days as i64))
                .ok_or_else(|| {
                    crate::error::Error::ExecutionError(
                        "Timestamp overflow when adding days".to_string(),
                    )
                })?;
        }

        if interval.micros != 0 {
            result = result
                .checked_add_signed(Duration::microseconds(interval.micros))
                .ok_or_else(|| {
                    crate::error::Error::ExecutionError(
                        "Timestamp overflow when adding microseconds".to_string(),
                    )
                })?;
        }

        Ok(Value::timestamp(result))
    }

    fn subtract_interval_from_timestamp(
        ts: chrono::DateTime<chrono::Utc>,
        interval: &yachtsql_core::types::Interval,
    ) -> Result<Value> {
        let negated = yachtsql_core::types::Interval {
            months: -interval.months,
            days: -interval.days,
            micros: -interval.micros,
        };
        Self::add_interval_to_timestamp(ts, &negated)
    }

    fn add_interval_to_date(
        date: chrono::NaiveDate,
        interval: &yachtsql_core::types::Interval,
    ) -> Result<Value> {
        use chrono::{Datelike, Duration, Months, TimeZone, Utc};

        let mut result_date = date;

        if interval.months != 0 {
            if interval.months > 0 {
                result_date = result_date
                    .checked_add_months(Months::new(interval.months as u32))
                    .ok_or_else(|| {
                        crate::error::Error::ExecutionError(
                            "Date overflow when adding months".to_string(),
                        )
                    })?;
            } else {
                result_date = result_date
                    .checked_sub_months(Months::new((-interval.months) as u32))
                    .ok_or_else(|| {
                        crate::error::Error::ExecutionError(
                            "Date underflow when subtracting months".to_string(),
                        )
                    })?;
            }
        }

        if interval.days != 0 {
            result_date = result_date
                .checked_add_signed(Duration::days(interval.days as i64))
                .ok_or_else(|| {
                    crate::error::Error::ExecutionError(
                        "Date overflow when adding days".to_string(),
                    )
                })?;
        }

        if interval.micros != 0 {
            let ts = Utc
                .from_utc_datetime(&result_date.and_hms_opt(0, 0, 0).unwrap())
                .checked_add_signed(Duration::microseconds(interval.micros))
                .ok_or_else(|| {
                    crate::error::Error::ExecutionError(
                        "Timestamp overflow when adding microseconds".to_string(),
                    )
                })?;
            return Ok(Value::timestamp(ts));
        }

        Ok(Value::date(result_date))
    }

    fn subtract_interval_from_date(
        date: chrono::NaiveDate,
        interval: &yachtsql_core::types::Interval,
    ) -> Result<Value> {
        let negated = yachtsql_core::types::Interval {
            months: -interval.months,
            days: -interval.days,
            micros: -interval.micros,
        };
        Self::add_interval_to_date(date, &negated)
    }

    pub(crate) fn evaluate_binary_op_with_enum(
        left: &crate::types::Value,
        op: &crate::optimizer::expr::BinaryOp,
        right: &crate::types::Value,
        enum_labels: Option<&[String]>,
    ) -> Result<crate::types::Value> {
        if let Some(labels) = enum_labels {
            if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
                let l_pos = labels.iter().position(|label| label == l);
                let r_pos = labels.iter().position(|label| label == r);

                if let (Some(l_idx), Some(r_idx)) = (l_pos, r_pos) {
                    return match op {
                        BinaryOp::Equal => Ok(Value::bool_val(l_idx == r_idx)),
                        BinaryOp::NotEqual => Ok(Value::bool_val(l_idx != r_idx)),
                        BinaryOp::LessThan => Ok(Value::bool_val(l_idx < r_idx)),
                        BinaryOp::LessThanOrEqual => Ok(Value::bool_val(l_idx <= r_idx)),
                        BinaryOp::GreaterThan => Ok(Value::bool_val(l_idx > r_idx)),
                        BinaryOp::GreaterThanOrEqual => Ok(Value::bool_val(l_idx >= r_idx)),
                        _ => Self::evaluate_binary_op(left, op, right),
                    };
                }
            }
        }

        Self::evaluate_binary_op(left, op, right)
    }

    fn inet_compare(
        l: &yachtsql_core::types::network::InetAddr,
        r: &yachtsql_core::types::network::InetAddr,
    ) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        use std::net::IpAddr;

        let family_cmp = l.family().cmp(&r.family());
        if family_cmp != Ordering::Equal {
            return family_cmp;
        }

        match (&l.addr, &r.addr) {
            (IpAddr::V4(l_ip), IpAddr::V4(r_ip)) => {
                let l_bits = u32::from_be_bytes(l_ip.octets());
                let r_bits = u32::from_be_bytes(r_ip.octets());
                let addr_cmp = l_bits.cmp(&r_bits);
                if addr_cmp != Ordering::Equal {
                    return addr_cmp;
                }
                l.prefix_len.unwrap_or(32).cmp(&r.prefix_len.unwrap_or(32))
            }
            (IpAddr::V6(l_ip), IpAddr::V6(r_ip)) => {
                let l_bits = u128::from_be_bytes(l_ip.octets());
                let r_bits = u128::from_be_bytes(r_ip.octets());
                let addr_cmp = l_bits.cmp(&r_bits);
                if addr_cmp != Ordering::Equal {
                    return addr_cmp;
                }
                l.prefix_len
                    .unwrap_or(128)
                    .cmp(&r.prefix_len.unwrap_or(128))
            }
            _ => Ordering::Equal,
        }
    }

    fn inet_is_contained_by(
        inner: &yachtsql_core::types::network::InetAddr,
        outer: &yachtsql_core::types::network::InetAddr,
    ) -> bool {
        use std::net::IpAddr;

        if inner.is_ipv4() != outer.is_ipv4() {
            return false;
        }

        let outer_prefix = outer.prefix_len.unwrap_or(outer.max_prefix_len());
        let inner_prefix = inner.prefix_len.unwrap_or(inner.max_prefix_len());

        if inner_prefix < outer_prefix {
            return false;
        }

        match (&inner.addr, &outer.addr) {
            (IpAddr::V4(inner_ip), IpAddr::V4(outer_ip)) => {
                let mask = if outer_prefix == 0 {
                    0u32
                } else {
                    !0u32 << (32 - outer_prefix)
                };
                let inner_bits = u32::from_be_bytes(inner_ip.octets());
                let outer_bits = u32::from_be_bytes(outer_ip.octets());
                (inner_bits & mask) == (outer_bits & mask)
            }
            (IpAddr::V6(inner_ip), IpAddr::V6(outer_ip)) => {
                let mask = if outer_prefix == 0 {
                    0u128
                } else {
                    !0u128 << (128 - outer_prefix)
                };
                let inner_bits = u128::from_be_bytes(inner_ip.octets());
                let outer_bits = u128::from_be_bytes(outer_ip.octets());
                (inner_bits & mask) == (outer_bits & mask)
            }
            _ => false,
        }
    }

    fn inet_is_contained_by_or_equal(
        inner: &yachtsql_core::types::network::InetAddr,
        outer: &yachtsql_core::types::network::InetAddr,
    ) -> bool {
        use std::net::IpAddr;

        if inner.is_ipv4() != outer.is_ipv4() {
            return false;
        }

        let outer_prefix = outer.prefix_len.unwrap_or(outer.max_prefix_len());
        let inner_prefix = inner.prefix_len.unwrap_or(inner.max_prefix_len());

        if inner_prefix < outer_prefix {
            return false;
        }

        match (&inner.addr, &outer.addr) {
            (IpAddr::V4(inner_ip), IpAddr::V4(outer_ip)) => {
                let mask = if outer_prefix == 0 {
                    0u32
                } else {
                    !0u32 << (32 - outer_prefix)
                };
                let inner_bits = u32::from_be_bytes(inner_ip.octets());
                let outer_bits = u32::from_be_bytes(outer_ip.octets());
                (inner_bits & mask) == (outer_bits & mask)
            }
            (IpAddr::V6(inner_ip), IpAddr::V6(outer_ip)) => {
                let mask = if outer_prefix == 0 {
                    0u128
                } else {
                    !0u128 << (128 - outer_prefix)
                };
                let inner_bits = u128::from_be_bytes(inner_ip.octets());
                let outer_bits = u128::from_be_bytes(outer_ip.octets());
                (inner_bits & mask) == (outer_bits & mask)
            }
            _ => false,
        }
    }

    fn inet_overlaps(
        a: &yachtsql_core::types::network::InetAddr,
        b: &yachtsql_core::types::network::InetAddr,
    ) -> bool {
        Self::inet_is_contained_by_or_equal(a, b) || Self::inet_is_contained_by_or_equal(b, a)
    }

    fn inet_bitwise_not(inet: &yachtsql_core::types::network::InetAddr) -> Result<Value> {
        use std::net::IpAddr;

        use yachtsql_core::types::network::InetAddr;

        let result_addr = match &inet.addr {
            IpAddr::V4(ip) => {
                let bits = u32::from_be_bytes(ip.octets());
                IpAddr::V4(std::net::Ipv4Addr::from((!bits).to_be_bytes()))
            }
            IpAddr::V6(ip) => {
                let bits = u128::from_be_bytes(ip.octets());
                IpAddr::V6(std::net::Ipv6Addr::from((!bits).to_be_bytes()))
            }
        };

        Ok(Value::inet(InetAddr::new(result_addr)))
    }

    fn inet_bitwise_and(
        l: &yachtsql_core::types::network::InetAddr,
        r: &yachtsql_core::types::network::InetAddr,
    ) -> Result<Value> {
        use std::net::IpAddr;

        use yachtsql_core::types::network::InetAddr;

        if l.is_ipv4() != r.is_ipv4() {
            return Err(Error::InvalidOperation(
                "Cannot perform bitwise AND on different IP families".to_string(),
            ));
        }

        let result_addr = match (&l.addr, &r.addr) {
            (IpAddr::V4(l_ip), IpAddr::V4(r_ip)) => {
                let l_bits = u32::from_be_bytes(l_ip.octets());
                let r_bits = u32::from_be_bytes(r_ip.octets());
                IpAddr::V4(std::net::Ipv4Addr::from((l_bits & r_bits).to_be_bytes()))
            }
            (IpAddr::V6(l_ip), IpAddr::V6(r_ip)) => {
                let l_bits = u128::from_be_bytes(l_ip.octets());
                let r_bits = u128::from_be_bytes(r_ip.octets());
                IpAddr::V6(std::net::Ipv6Addr::from((l_bits & r_bits).to_be_bytes()))
            }
            _ => unreachable!(),
        };

        Ok(Value::inet(InetAddr::new(result_addr)))
    }

    fn inet_bitwise_or(
        l: &yachtsql_core::types::network::InetAddr,
        r: &yachtsql_core::types::network::InetAddr,
    ) -> Result<Value> {
        use std::net::IpAddr;

        use yachtsql_core::types::network::InetAddr;

        if l.is_ipv4() != r.is_ipv4() {
            return Err(Error::InvalidOperation(
                "Cannot perform bitwise OR on different IP families".to_string(),
            ));
        }

        let result_addr = match (&l.addr, &r.addr) {
            (IpAddr::V4(l_ip), IpAddr::V4(r_ip)) => {
                let l_bits = u32::from_be_bytes(l_ip.octets());
                let r_bits = u32::from_be_bytes(r_ip.octets());
                IpAddr::V4(std::net::Ipv4Addr::from((l_bits | r_bits).to_be_bytes()))
            }
            (IpAddr::V6(l_ip), IpAddr::V6(r_ip)) => {
                let l_bits = u128::from_be_bytes(l_ip.octets());
                let r_bits = u128::from_be_bytes(r_ip.octets());
                IpAddr::V6(std::net::Ipv6Addr::from((l_bits | r_bits).to_be_bytes()))
            }
            _ => unreachable!(),
        };

        Ok(Value::inet(InetAddr::new(result_addr)))
    }

    fn inet_add_int(inet: &yachtsql_core::types::network::InetAddr, offset: &i64) -> Result<Value> {
        use std::net::IpAddr;

        use yachtsql_core::types::network::InetAddr;

        let result_addr = match &inet.addr {
            IpAddr::V4(ip) => {
                let bits = u32::from_be_bytes(ip.octets());
                let new_bits = if *offset >= 0 {
                    bits.wrapping_add(*offset as u32)
                } else {
                    bits.wrapping_sub((-*offset) as u32)
                };
                IpAddr::V4(std::net::Ipv4Addr::from(new_bits.to_be_bytes()))
            }
            IpAddr::V6(ip) => {
                let bits = u128::from_be_bytes(ip.octets());
                let new_bits = if *offset >= 0 {
                    bits.wrapping_add(*offset as u128)
                } else {
                    bits.wrapping_sub((-*offset) as u128)
                };
                IpAddr::V6(std::net::Ipv6Addr::from(new_bits.to_be_bytes()))
            }
        };

        let result = if let Some(prefix) = inet.prefix_len {
            InetAddr {
                addr: result_addr,
                prefix_len: Some(prefix),
            }
        } else {
            InetAddr::new(result_addr)
        };

        Ok(Value::inet(result))
    }

    fn inet_subtract_inet(
        l: &yachtsql_core::types::network::InetAddr,
        r: &yachtsql_core::types::network::InetAddr,
    ) -> Result<Value> {
        use std::net::IpAddr;

        if l.is_ipv4() != r.is_ipv4() {
            return Err(Error::InvalidOperation(
                "Cannot subtract addresses from different IP families".to_string(),
            ));
        }

        let diff = match (&l.addr, &r.addr) {
            (IpAddr::V4(l_ip), IpAddr::V4(r_ip)) => {
                let l_bits = u32::from_be_bytes(l_ip.octets()) as i64;
                let r_bits = u32::from_be_bytes(r_ip.octets()) as i64;
                l_bits - r_bits
            }
            (IpAddr::V6(l_ip), IpAddr::V6(r_ip)) => {
                let l_bits = u128::from_be_bytes(l_ip.octets()) as i128;
                let r_bits = u128::from_be_bytes(r_ip.octets()) as i128;
                (l_bits - r_bits) as i64
            }
            _ => unreachable!(),
        };

        Ok(Value::int64(diff))
    }
}
