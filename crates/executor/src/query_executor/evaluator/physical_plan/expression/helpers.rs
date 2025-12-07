use yachtsql_core::error::Result;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn validate_arg_count(
        func_name: &str,
        args: &[crate::optimizer::expr::Expr],
        expected: usize,
    ) -> Result<()> {
        if args.len() != expected {
            return Err(crate::error::Error::invalid_query(format!(
                "{} requires exactly {} argument(s), got {}",
                func_name,
                expected,
                args.len()
            )));
        }
        Ok(())
    }

    pub(super) fn validate_min_arg_count(
        func_name: &str,
        args: &[crate::optimizer::expr::Expr],
        min: usize,
    ) -> Result<()> {
        if args.len() < min {
            return Err(crate::error::Error::invalid_query(format!(
                "{} requires at least {} argument(s), got {}",
                func_name,
                min,
                args.len()
            )));
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(super) fn evaluate_hash_function<F>(
        dialect: crate::DialectType,
        func_name: &str,
        args: &[crate::optimizer::expr::Expr],
        batch: &Table,
        row_idx: usize,
        eval_fn: F,
    ) -> Result<crate::types::Value>
    where
        F: FnOnce(&crate::types::Value, bool) -> Result<crate::types::Value>,
    {
        Self::validate_arg_count(func_name, args, 1)?;
        let val = Self::evaluate_expr_internal(&args[0], batch, row_idx, dialect)?;
        let return_hex = matches!(dialect, crate::DialectType::PostgreSQL);
        eval_fn(&val, return_hex)
    }

    #[allow(dead_code)]
    pub(super) fn evaluate_unary_function_with_dialect<F>(
        dialect: crate::DialectType,
        func_name: &str,
        args: &[crate::optimizer::expr::Expr],
        batch: &Table,
        row_idx: usize,
        eval_fn: F,
    ) -> Result<crate::types::Value>
    where
        F: FnOnce(&crate::types::Value) -> Result<crate::types::Value>,
    {
        Self::validate_arg_count(func_name, args, 1)?;
        let val = Self::evaluate_expr_internal(&args[0], batch, row_idx, dialect)?;
        eval_fn(&val)
    }

    pub(super) fn validate_zero_args(
        func_name: &str,
        args: &[crate::optimizer::expr::Expr],
    ) -> Result<()> {
        if !args.is_empty() {
            return Err(crate::error::Error::invalid_query(format!(
                "{} takes no arguments, got {}",
                func_name,
                args.len()
            )));
        }
        Ok(())
    }

    pub(super) fn apply_string_unary<F>(
        _func_name: &str,
        arg: &crate::optimizer::expr::Expr,
        batch: &Table,
        row_idx: usize,
        f: F,
    ) -> Result<crate::types::Value>
    where
        F: FnOnce(&str) -> String,
    {
        use yachtsql_core::types::Value;

        let val = Self::evaluate_expr(arg, batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(s) = val.as_str() {
            Ok(Value::string(f(s)))
        } else {
            Err(crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: val.data_type().to_string(),
            })
        }
    }

    pub(super) fn evaluate_args(
        args: &[crate::optimizer::expr::Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Vec<crate::types::Value>> {
        args.iter()
            .map(|arg| Self::evaluate_expr(arg, batch, row_idx))
            .collect()
    }

    pub(super) fn find_substring_position(haystack: &str, needle: &str) -> i64 {
        match haystack.find(needle) {
            Some(byte_pos) => (haystack[..byte_pos].chars().count() + 1) as i64,
            None => 0,
        }
    }

    pub(super) fn apply_string_int_binary<F>(
        func_name: &str,
        args: &[crate::optimizer::expr::Expr],
        batch: &Table,
        row_idx: usize,
        f: F,
    ) -> Result<crate::types::Value>
    where
        F: FnOnce(&str, i64) -> String,
    {
        use yachtsql_core::types::Value;

        Self::validate_arg_count(func_name, args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;

        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        if let (Some(s), Some(n)) = (values[0].as_str(), values[1].as_i64()) {
            Ok(Value::string(f(s, n)))
        } else {
            Err(crate::error::Error::TypeMismatch {
                expected: "STRING, INT64".to_string(),
                actual: format!("{}, {}", values[0].data_type(), values[1].data_type()),
            })
        }
    }

    pub(super) fn pad_string(
        s: &str,
        target_len: i64,
        fill_str: &str,
        pad_left: bool,
    ) -> Result<String> {
        if target_len < 0 {
            return Err(crate::error::Error::invalid_query(format!(
                "{} length cannot be negative",
                if pad_left { "LPAD" } else { "RPAD" }
            )));
        }

        if fill_str.is_empty() {
            return Err(crate::error::Error::invalid_query(format!(
                "{} fill string cannot be empty",
                if pad_left { "LPAD" } else { "RPAD" }
            )));
        }

        let target_len = target_len as usize;
        let chars: Vec<char> = s.chars().collect();
        let current_len = chars.len();

        if current_len >= target_len {
            Ok(chars[..target_len].iter().collect())
        } else {
            let pad_len = target_len - current_len;
            let fill_chars: Vec<char> = fill_str.chars().collect();
            let mut result = String::new();

            if pad_left {
                for i in 0..pad_len {
                    result.push(fill_chars[i % fill_chars.len()]);
                }
                result.push_str(s);
            } else {
                result.push_str(s);
                for i in 0..pad_len {
                    result.push(fill_chars[i % fill_chars.len()]);
                }
            }

            Ok(result)
        }
    }

    pub(super) fn value_to_f64(val: &crate::types::Value) -> Result<Option<f64>> {
        if val.is_null() {
            return Ok(None);
        }

        if let Some(i) = val.as_i64() {
            Ok(Some(i as f64))
        } else if let Some(f) = val.as_f64() {
            Ok(Some(f))
        } else if let Some(d) = val.as_numeric() {
            use rust_decimal::prelude::ToPrimitive;
            Ok(d.to_f64())
        } else {
            Err(crate::error::Error::TypeMismatch {
                expected: "NUMERIC".to_string(),
                actual: val.data_type().to_string(),
            })
        }
    }

    pub(super) fn validate_positive(value: f64, context: &str) -> Result<()> {
        if value <= 0.0 {
            return Err(crate::error::Error::InvalidOperation(format!(
                "{} must be greater than 0",
                context
            )));
        }
        Ok(())
    }

    pub(super) fn calculate_natural_log(val: crate::types::Value) -> Result<crate::types::Value> {
        use yachtsql_core::types::Value;

        match Self::value_to_f64(&val)? {
            Some(f) => {
                Self::validate_positive(f, "logarithm value")?;
                Ok(Value::float64(f.ln()))
            }
            None => Ok(Value::null()),
        }
    }

    pub(super) fn apply_format_string(
        format_str: &str,
        args: &[crate::types::Value],
    ) -> Result<crate::types::Value> {
        use yachtsql_core::types::Value;

        let mut result = String::new();
        let mut chars = format_str.chars().peekable();
        let mut arg_idx = 0;

        while let Some(ch) = chars.next() {
            if ch == '%' {
                if let Some(&next_ch) = chars.peek()
                    && next_ch == '%'
                {
                    chars.next();
                    result.push('%');
                    continue;
                }

                let mut width: Option<usize> = None;
                let mut precision: Option<usize> = None;
                let mut num_buf = String::new();

                while let Some(&next_ch) = chars.peek() {
                    if next_ch.is_ascii_digit() {
                        num_buf.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                if !num_buf.is_empty() {
                    width = num_buf.parse().ok();
                    num_buf.clear();
                }

                if let Some(&'.') = chars.peek() {
                    chars.next();
                    while let Some(&next_ch) = chars.peek() {
                        if next_ch.is_ascii_digit() {
                            num_buf.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }
                    if !num_buf.is_empty() {
                        precision = num_buf.parse().ok();
                    }
                }

                if let Some(spec) = chars.next() {
                    if arg_idx >= args.len() {
                        return Err(crate::error::Error::invalid_query(format!(
                            "FORMAT: not enough arguments for format string (expected at least {})",
                            arg_idx + 1
                        )));
                    }

                    let arg = &args[arg_idx];
                    arg_idx += 1;

                    let formatted = match spec {
                        's' => {
                            if arg.is_null() {
                                "NULL".to_string()
                            } else if let Some(s) = arg.as_str() {
                                s.to_string()
                            } else if let Some(i) = arg.as_i64() {
                                i.to_string()
                            } else if let Some(f) = arg.as_f64() {
                                f.to_string()
                            } else if let Some(b) = arg.as_bool() {
                                b.to_string()
                            } else if let Some(d) = arg.as_date() {
                                format!("{}", d)
                            } else if let Some(t) = arg.as_timestamp() {
                                format!("{}", t)
                            } else {
                                arg.to_string()
                            }
                        }
                        'd' | 'i' => {
                            if arg.is_null() {
                                "NULL".to_string()
                            } else if let Some(i) = arg.as_i64() {
                                i.to_string()
                            } else {
                                return Err(crate::error::Error::TypeMismatch {
                                    expected: "INT64".to_string(),
                                    actual: arg.data_type().to_string(),
                                });
                            }
                        }
                        'l' => {
                            if let Some(&'d') = chars.peek() {
                                chars.next();
                                if arg.is_null() {
                                    "NULL".to_string()
                                } else if let Some(i) = arg.as_i64() {
                                    i.to_string()
                                } else {
                                    return Err(crate::error::Error::TypeMismatch {
                                        expected: "INT64".to_string(),
                                        actual: arg.data_type().to_string(),
                                    });
                                }
                            } else {
                                return Err(crate::error::Error::invalid_query(
                                    "FORMAT: invalid format specifier %l (expected %ld)"
                                        .to_string(),
                                ));
                            }
                        }
                        'f' | 'e' | 'E' | 'g' | 'G' => {
                            if arg.is_null() {
                                "NULL".to_string()
                            } else if let Some(f) = arg.as_f64() {
                                if let Some(prec) = precision {
                                    format!("{:.prec$}", f, prec = prec)
                                } else {
                                    f.to_string()
                                }
                            } else if let Some(i) = arg.as_i64() {
                                let f = i as f64;
                                if let Some(prec) = precision {
                                    format!("{:.prec$}", f, prec = prec)
                                } else {
                                    f.to_string()
                                }
                            } else {
                                return Err(crate::error::Error::TypeMismatch {
                                    expected: "FLOAT64".to_string(),
                                    actual: arg.data_type().to_string(),
                                });
                            }
                        }
                        _ => {
                            return Err(crate::error::Error::invalid_query(format!(
                                "FORMAT: unsupported format specifier '%{}'",
                                spec
                            )));
                        }
                    };

                    if let Some(w) = width {
                        result.push_str(&format!("{:>width$}", formatted, width = w));
                    } else {
                        result.push_str(&formatted);
                    }
                } else {
                    result.push('%');
                }
            } else {
                result.push(ch);
            }
        }

        Ok(Value::string(result))
    }
}
