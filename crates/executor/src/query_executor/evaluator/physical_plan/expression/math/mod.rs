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

            "SINH" => Self::eval_sinh(args, batch, row_idx),
            "COSH" => Self::eval_cosh(args, batch, row_idx),
            "TANH" => Self::eval_tanh(args, batch, row_idx),
            "ASINH" => Self::eval_asinh(args, batch, row_idx),
            "ACOSH" => Self::eval_acosh(args, batch, row_idx),
            "ATANH" => Self::eval_atanh(args, batch, row_idx),
            "COT" => Self::eval_cot(args, batch, row_idx),
            "SIND" => Self::eval_sind(args, batch, row_idx),
            "COSD" => Self::eval_cosd(args, batch, row_idx),
            "TAND" => Self::eval_tand(args, batch, row_idx),
            "ASIND" => Self::eval_asind(args, batch, row_idx),
            "ACOSD" => Self::eval_acosd(args, batch, row_idx),
            "ATAND" => Self::eval_atand(args, batch, row_idx),
            "ATAN2D" => Self::eval_atan2d(args, batch, row_idx),
            "COTD" => Self::eval_cotd(args, batch, row_idx),
            "CBRT" => Self::eval_cbrt(args, batch, row_idx),
            "FACTORIAL" => Self::eval_factorial(args, batch, row_idx),
            "GCD" => Self::eval_gcd(args, batch, row_idx),
            "LCM" => Self::eval_lcm(args, batch, row_idx),
            "DIV" => Self::eval_div(args, batch, row_idx),
            "SCALE" => Self::eval_scale(args, batch, row_idx),
            "MIN_SCALE" => Self::eval_min_scale(args, batch, row_idx),
            "TRIM_SCALE" => Self::eval_trim_scale(args, batch, row_idx),
            "WIDTH_BUCKET" => Self::eval_width_bucket(args, batch, row_idx),
            "SETSEED" => Self::eval_setseed(args, batch, row_idx),

            _ => Err(crate::error::Error::invalid_query(format!(
                "Unknown mathematical function: {}",
                name
            ))),
        }
    }

    fn eval_sinh(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("SINH", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = Self::extract_f64(&val, "SINH")?;
        Ok(Value::float64(f.sinh()))
    }

    fn eval_cosh(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("COSH", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = Self::extract_f64(&val, "COSH")?;
        Ok(Value::float64(f.cosh()))
    }

    fn eval_tanh(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("TANH", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = Self::extract_f64(&val, "TANH")?;
        Ok(Value::float64(f.tanh()))
    }

    fn eval_asinh(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("ASINH", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = Self::extract_f64(&val, "ASINH")?;
        Ok(Value::float64(f.asinh()))
    }

    fn eval_acosh(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("ACOSH", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = Self::extract_f64(&val, "ACOSH")?;
        if f < 1.0 {
            return Err(crate::error::Error::invalid_query(
                "ACOSH input must be >= 1".to_string(),
            ));
        }
        Ok(Value::float64(f.acosh()))
    }

    fn eval_atanh(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("ATANH", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = Self::extract_f64(&val, "ATANH")?;
        if !(-1.0..1.0).contains(&f) {
            return Err(crate::error::Error::invalid_query(
                "ATANH input must be in range (-1, 1)".to_string(),
            ));
        }
        Ok(Value::float64(f.atanh()))
    }

    fn eval_cot(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("COT", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = Self::extract_f64(&val, "COT")?;
        Ok(Value::float64(1.0 / f.tan()))
    }

    fn eval_sind(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("SIND", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = Self::extract_f64(&val, "SIND")?;
        Ok(Value::float64(f.to_radians().sin()))
    }

    fn eval_cosd(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("COSD", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = Self::extract_f64(&val, "COSD")?;
        Ok(Value::float64(f.to_radians().cos()))
    }

    fn eval_tand(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("TAND", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = Self::extract_f64(&val, "TAND")?;
        Ok(Value::float64(f.to_radians().tan()))
    }

    fn eval_asind(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("ASIND", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = Self::extract_f64(&val, "ASIND")?;
        if !(-1.0..=1.0).contains(&f) {
            return Err(crate::error::Error::invalid_query(
                "ASIND input must be in range [-1, 1]".to_string(),
            ));
        }
        Ok(Value::float64(f.asin().to_degrees()))
    }

    fn eval_acosd(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("ACOSD", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = Self::extract_f64(&val, "ACOSD")?;
        if !(-1.0..=1.0).contains(&f) {
            return Err(crate::error::Error::invalid_query(
                "ACOSD input must be in range [-1, 1]".to_string(),
            ));
        }
        Ok(Value::float64(f.acos().to_degrees()))
    }

    fn eval_atand(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("ATAND", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = Self::extract_f64(&val, "ATAND")?;
        Ok(Value::float64(f.atan().to_degrees()))
    }

    fn eval_atan2d(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("ATAN2D", args, 2)?;
        let y_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let x_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if y_val.is_null() || x_val.is_null() {
            return Ok(Value::null());
        }
        let y = Self::extract_f64(&y_val, "ATAN2D")?;
        let x = Self::extract_f64(&x_val, "ATAN2D")?;
        Ok(Value::float64(y.atan2(x).to_degrees()))
    }

    fn eval_cotd(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("COTD", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = Self::extract_f64(&val, "COTD")?;
        let radians = f.to_radians();
        Ok(Value::float64(1.0 / radians.tan()))
    }

    fn eval_cbrt(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("CBRT", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = Self::extract_f64(&val, "CBRT")?;
        Ok(Value::float64(f.cbrt()))
    }

    fn eval_factorial(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("FACTORIAL", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let n = match val.as_i64() {
            Some(i) => i,
            None => {
                return Err(crate::error::Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: val.data_type().to_string(),
                });
            }
        };
        if n < 0 {
            return Err(crate::error::Error::invalid_query(
                "FACTORIAL requires a non-negative integer".to_string(),
            ));
        }
        if n > 20 {
            return Err(crate::error::Error::invalid_query(
                "FACTORIAL argument too large (max 20)".to_string(),
            ));
        }
        let mut result: i64 = 1;
        for i in 2..=n {
            result = result.saturating_mul(i);
        }
        Ok(Value::int64(result))
    }

    fn eval_gcd(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("GCD", args, 2)?;
        let a_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let b_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if a_val.is_null() || b_val.is_null() {
            return Ok(Value::null());
        }
        let a = match a_val.as_i64() {
            Some(i) => i.abs(),
            None => {
                return Err(crate::error::Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: a_val.data_type().to_string(),
                });
            }
        };
        let b = match b_val.as_i64() {
            Some(i) => i.abs(),
            None => {
                return Err(crate::error::Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: b_val.data_type().to_string(),
                });
            }
        };
        fn gcd(mut a: i64, mut b: i64) -> i64 {
            while b != 0 {
                let t = b;
                b = a % b;
                a = t;
            }
            a
        }
        Ok(Value::int64(gcd(a, b)))
    }

    fn eval_lcm(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("LCM", args, 2)?;
        let a_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let b_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if a_val.is_null() || b_val.is_null() {
            return Ok(Value::null());
        }
        let a = match a_val.as_i64() {
            Some(i) => i.abs(),
            None => {
                return Err(crate::error::Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: a_val.data_type().to_string(),
                });
            }
        };
        let b = match b_val.as_i64() {
            Some(i) => i.abs(),
            None => {
                return Err(crate::error::Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: b_val.data_type().to_string(),
                });
            }
        };
        if a == 0 || b == 0 {
            return Ok(Value::int64(0));
        }
        fn gcd(mut a: i64, mut b: i64) -> i64 {
            while b != 0 {
                let t = b;
                b = a % b;
                a = t;
            }
            a
        }
        Ok(Value::int64((a / gcd(a, b)) * b))
    }

    fn eval_div(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("DIV", args, 2)?;
        let a_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let b_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if a_val.is_null() || b_val.is_null() {
            return Ok(Value::null());
        }
        let a = Self::extract_f64(&a_val, "DIV")?;
        let b = Self::extract_f64(&b_val, "DIV")?;
        if b == 0.0 {
            return Err(crate::error::Error::invalid_query(
                "Division by zero in DIV".to_string(),
            ));
        }
        Ok(Value::int64((a / b).trunc() as i64))
    }

    fn eval_scale(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("SCALE", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(d) = val.as_numeric() {
            return Ok(Value::int64(d.scale() as i64));
        }
        if let Some(f) = val.as_f64() {
            let s = format!("{}", f);
            if let Some(pos) = s.find('.') {
                return Ok(Value::int64((s.len() - pos - 1) as i64));
            }
            return Ok(Value::int64(0));
        }
        Err(crate::error::Error::TypeMismatch {
            expected: "NUMERIC".to_string(),
            actual: val.data_type().to_string(),
        })
    }

    fn eval_min_scale(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("MIN_SCALE", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(d) = val.as_numeric() {
            let s = d.to_string();
            if let Some(pos) = s.find('.') {
                let decimal_part = &s[pos + 1..];
                let trimmed = decimal_part.trim_end_matches('0');
                return Ok(Value::int64(trimmed.len() as i64));
            }
            return Ok(Value::int64(0));
        }
        if let Some(f) = val.as_f64() {
            let s = format!("{}", f);
            if let Some(pos) = s.find('.') {
                let decimal_part = &s[pos + 1..];
                let trimmed = decimal_part.trim_end_matches('0');
                return Ok(Value::int64(trimmed.len() as i64));
            }
            return Ok(Value::int64(0));
        }
        Err(crate::error::Error::TypeMismatch {
            expected: "NUMERIC".to_string(),
            actual: val.data_type().to_string(),
        })
    }

    fn eval_trim_scale(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("TRIM_SCALE", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(d) = val.as_numeric() {
            return Ok(Value::numeric(d.normalize()));
        }
        if let Some(f) = val.as_f64() {
            return Ok(Value::float64(f));
        }
        Err(crate::error::Error::TypeMismatch {
            expected: "NUMERIC".to_string(),
            actual: val.data_type().to_string(),
        })
    }

    fn eval_width_bucket(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("WIDTH_BUCKET", args, 4)?;
        let value_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let low_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let high_val = Self::evaluate_expr(&args[2], batch, row_idx)?;
        let buckets_val = Self::evaluate_expr(&args[3], batch, row_idx)?;
        if value_val.is_null() || low_val.is_null() || high_val.is_null() || buckets_val.is_null() {
            return Ok(Value::null());
        }
        let value = Self::extract_f64(&value_val, "WIDTH_BUCKET")?;
        let low = Self::extract_f64(&low_val, "WIDTH_BUCKET")?;
        let high = Self::extract_f64(&high_val, "WIDTH_BUCKET")?;
        let buckets = match buckets_val.as_i64() {
            Some(b) => b,
            None => {
                return Err(crate::error::Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: buckets_val.data_type().to_string(),
                });
            }
        };
        if buckets <= 0 {
            return Err(crate::error::Error::invalid_query(
                "WIDTH_BUCKET buckets must be positive".to_string(),
            ));
        }
        if low >= high {
            return Err(crate::error::Error::invalid_query(
                "WIDTH_BUCKET low must be less than high".to_string(),
            ));
        }
        if value < low {
            return Ok(Value::int64(0));
        }
        if value >= high {
            return Ok(Value::int64(buckets + 1));
        }
        let bucket_width = (high - low) / buckets as f64;
        let bucket = ((value - low) / bucket_width).floor() as i64 + 1;
        Ok(Value::int64(bucket))
    }

    fn eval_setseed(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("SETSEED", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let seed = Self::extract_f64(&val, "SETSEED")?;
        if !(-1.0..=1.0).contains(&seed) {
            return Err(crate::error::Error::invalid_query(
                "SETSEED argument must be between -1 and 1".to_string(),
            ));
        }
        Ok(Value::null())
    }

    fn extract_f64(val: &Value, func_name: &str) -> Result<f64> {
        if let Some(i) = val.as_i64() {
            return Ok(i as f64);
        }
        if let Some(f) = val.as_f64() {
            return Ok(f);
        }
        if let Some(d) = val.as_numeric() {
            use rust_decimal::prelude::ToPrimitive;
            return Ok(d.to_f64().unwrap_or(0.0));
        }
        Err(crate::error::Error::TypeMismatch {
            expected: format!("NUMERIC for {}", func_name),
            actual: val.data_type().to_string(),
        })
    }
}
