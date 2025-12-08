use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{Interval, Value};
use yachtsql_ir::expr::LiteralValue;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_interval_literal(
        args: &[Expr],
        _batch: &Table,
        _row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "INTERVAL_LITERAL requires exactly 2 arguments",
            ));
        }

        let value = match &args[0] {
            Expr::Literal(LiteralValue::Int64(v)) => *v,
            _ => {
                return Err(Error::invalid_query(
                    "INTERVAL_LITERAL first argument must be an integer",
                ));
            }
        };

        let unit = match &args[1] {
            Expr::Literal(LiteralValue::String(s)) => s.to_uppercase(),
            _ => {
                return Err(Error::invalid_query(
                    "INTERVAL_LITERAL second argument must be a string",
                ));
            }
        };

        let interval = Self::create_interval_from_value_and_unit(value, &unit)?;
        Ok(Value::interval(interval))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_interval_parse(
        args: &[Expr],
        _batch: &Table,
        _row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "INTERVAL_PARSE requires exactly 2 arguments",
            ));
        }

        let value_str = match &args[0] {
            Expr::Literal(LiteralValue::String(s)) => s.clone(),
            _ => {
                return Err(Error::invalid_query(
                    "INTERVAL_PARSE first argument must be a string",
                ));
            }
        };

        let unit_hint = match &args[1] {
            Expr::Literal(LiteralValue::String(s)) => s.to_uppercase(),
            _ => String::new(),
        };

        let interval = Self::parse_interval_string(&value_str, &unit_hint)?;
        Ok(Value::interval(interval))
    }

    fn create_interval_from_value_and_unit(value: i64, unit: &str) -> Result<Interval> {
        match unit {
            "YEAR" | "YEARS" => Ok(Interval::new((value * 12) as i32, 0, 0)),
            "MONTH" | "MONTHS" => Ok(Interval::new(value as i32, 0, 0)),
            "DAY" | "DAYS" => Ok(Interval::new(0, value as i32, 0)),
            "HOUR" | "HOURS" => Ok(Interval::new(0, 0, value * Interval::MICROS_PER_HOUR)),
            "MINUTE" | "MINUTES" => Ok(Interval::new(0, 0, value * Interval::MICROS_PER_MINUTE)),
            "SECOND" | "SECONDS" => Ok(Interval::new(0, 0, value * Interval::MICROS_PER_SECOND)),
            "WEEK" | "WEEKS" => Ok(Interval::new(0, (value * 7) as i32, 0)),
            _ => Err(Error::unsupported_feature(format!(
                "Unsupported interval unit: {}",
                unit
            ))),
        }
    }

    fn create_interval_from_float_and_unit(value: f64, unit: &str) -> Result<Interval> {
        match unit {
            "YEAR" | "YEARS" => Ok(Interval::new((value * 12.0) as i32, 0, 0)),
            "MONTH" | "MONTHS" => Ok(Interval::new(value as i32, 0, 0)),
            "DAY" | "DAYS" => {
                let days = value.trunc() as i32;
                let frac_micros = (value.fract() * Interval::MICROS_PER_DAY as f64) as i64;
                Ok(Interval::new(0, days, frac_micros))
            }
            "HOUR" | "HOURS" => Ok(Interval::new(
                0,
                0,
                (value * Interval::MICROS_PER_HOUR as f64) as i64,
            )),
            "MINUTE" | "MINUTES" => Ok(Interval::new(
                0,
                0,
                (value * Interval::MICROS_PER_MINUTE as f64) as i64,
            )),
            "SECOND" | "SECONDS" => Ok(Interval::new(
                0,
                0,
                (value * Interval::MICROS_PER_SECOND as f64) as i64,
            )),
            "WEEK" | "WEEKS" => Ok(Interval::new(0, (value * 7.0) as i32, 0)),
            _ => Err(Error::unsupported_feature(format!(
                "Unsupported interval unit: {}",
                unit
            ))),
        }
    }

    fn parse_interval_string(value_str: &str, unit_hint: &str) -> Result<Interval> {
        let trimmed = value_str.trim();

        if trimmed.starts_with('P') || trimmed.starts_with('p') {
            return Self::parse_iso8601_interval(trimmed);
        }

        if let Ok(int_val) = trimmed.parse::<i64>() {
            if unit_hint.is_empty() {
                return Err(Error::invalid_query(
                    "INTERVAL requires a unit when using a numeric value",
                ));
            }
            return Self::create_interval_from_value_and_unit(int_val, unit_hint);
        }

        if let Ok(float_val) = trimmed.parse::<f64>() {
            if unit_hint.is_empty() {
                return Err(Error::invalid_query(
                    "INTERVAL requires a unit when using a numeric value",
                ));
            }
            return Self::create_interval_from_float_and_unit(float_val, unit_hint);
        }

        let mut months = 0i32;
        let mut days = 0i32;
        let mut micros = 0i64;

        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        let mut i = 0;
        while i < parts.len() {
            let num_str = parts[i];
            let num: f64 = num_str.parse().map_err(|_| {
                Error::invalid_query(format!("Invalid interval number: {}", num_str))
            })?;

            if i + 1 >= parts.len() {
                return Err(Error::invalid_query(format!(
                    "Missing unit after number {} in interval",
                    num_str
                )));
            }

            let unit = parts[i + 1].to_uppercase();
            match unit.as_str() {
                "YEAR" | "YEARS" => months += (num * 12.0) as i32,
                "MONTH" | "MONTHS" => months += num as i32,
                "WEEK" | "WEEKS" => days += (num * 7.0) as i32,
                "DAY" | "DAYS" => days += num as i32,
                "HOUR" | "HOURS" => micros += (num * Interval::MICROS_PER_HOUR as f64) as i64,
                "MINUTE" | "MINUTES" => micros += (num * Interval::MICROS_PER_MINUTE as f64) as i64,
                "SECOND" | "SECONDS" => micros += (num * Interval::MICROS_PER_SECOND as f64) as i64,
                _ => {
                    return Err(Error::unsupported_feature(format!(
                        "Unsupported interval unit: {}",
                        unit
                    )));
                }
            }
            i += 2;
        }

        Ok(Interval::new(months, days, micros))
    }

    fn parse_iso8601_interval(s: &str) -> Result<Interval> {
        let mut months = 0i32;
        let mut days = 0i32;
        let mut micros = 0i64;

        let s = if s.starts_with('P') || s.starts_with('p') {
            &s[1..]
        } else {
            s
        };

        let (date_part, time_part) = if let Some(t_pos) = s.find(['T', 't']) {
            (&s[..t_pos], Some(&s[t_pos + 1..]))
        } else {
            (s, None)
        };

        let mut num_buf = String::new();
        for c in date_part.chars() {
            match c {
                '0'..='9' | '.' | '-' => num_buf.push(c),
                'Y' | 'y' => {
                    if !num_buf.is_empty() {
                        let years: f64 = num_buf.parse().map_err(|_| {
                            Error::invalid_query(format!("Invalid year value: {}", num_buf))
                        })?;
                        months += (years * 12.0) as i32;
                        num_buf.clear();
                    }
                }
                'M' | 'm' => {
                    if !num_buf.is_empty() {
                        let m: f64 = num_buf.parse().map_err(|_| {
                            Error::invalid_query(format!("Invalid month value: {}", num_buf))
                        })?;
                        months += m as i32;
                        num_buf.clear();
                    }
                }
                'W' | 'w' => {
                    if !num_buf.is_empty() {
                        let weeks: f64 = num_buf.parse().map_err(|_| {
                            Error::invalid_query(format!("Invalid week value: {}", num_buf))
                        })?;
                        days += (weeks * 7.0) as i32;
                        num_buf.clear();
                    }
                }
                'D' | 'd' => {
                    if !num_buf.is_empty() {
                        let d: f64 = num_buf.parse().map_err(|_| {
                            Error::invalid_query(format!("Invalid day value: {}", num_buf))
                        })?;
                        days += d as i32;
                        num_buf.clear();
                    }
                }
                _ => {}
            }
        }

        if let Some(time_str) = time_part {
            num_buf.clear();
            for c in time_str.chars() {
                match c {
                    '0'..='9' | '.' | '-' => num_buf.push(c),
                    'H' | 'h' => {
                        if !num_buf.is_empty() {
                            let hours: f64 = num_buf.parse().map_err(|_| {
                                Error::invalid_query(format!("Invalid hour value: {}", num_buf))
                            })?;
                            micros += (hours * Interval::MICROS_PER_HOUR as f64) as i64;
                            num_buf.clear();
                        }
                    }
                    'M' | 'm' => {
                        if !num_buf.is_empty() {
                            let minutes: f64 = num_buf.parse().map_err(|_| {
                                Error::invalid_query(format!("Invalid minute value: {}", num_buf))
                            })?;
                            micros += (minutes * Interval::MICROS_PER_MINUTE as f64) as i64;
                            num_buf.clear();
                        }
                    }
                    'S' | 's' => {
                        if !num_buf.is_empty() {
                            let seconds: f64 = num_buf.parse().map_err(|_| {
                                Error::invalid_query(format!("Invalid second value: {}", num_buf))
                            })?;
                            micros += (seconds * Interval::MICROS_PER_SECOND as f64) as i64;
                            num_buf.clear();
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(Interval::new(months, days, micros))
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{Interval, Value};
    use yachtsql_ir::expr::LiteralValue;
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::Schema;

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    #[test]
    fn test_interval_days() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let args = vec![
            Expr::Literal(LiteralValue::Int64(5)),
            Expr::Literal(LiteralValue::String("DAY".to_string())),
        ];
        let result =
            ProjectionWithExprExec::eval_interval_literal(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::interval(Interval::new(0, 5, 0)));
    }

    #[test]
    fn test_interval_months() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let args = vec![
            Expr::Literal(LiteralValue::Int64(3)),
            Expr::Literal(LiteralValue::String("MONTH".to_string())),
        ];
        let result =
            ProjectionWithExprExec::eval_interval_literal(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::interval(Interval::new(3, 0, 0)));
    }

    #[test]
    fn test_interval_years() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let args = vec![
            Expr::Literal(LiteralValue::Int64(2)),
            Expr::Literal(LiteralValue::String("YEAR".to_string())),
        ];
        let result =
            ProjectionWithExprExec::eval_interval_literal(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::interval(Interval::new(24, 0, 0)));
    }

    #[test]
    fn test_interval_hours() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let args = vec![
            Expr::Literal(LiteralValue::Int64(12)),
            Expr::Literal(LiteralValue::String("HOUR".to_string())),
        ];
        let result =
            ProjectionWithExprExec::eval_interval_literal(&args, &batch, 0).expect("success");
        assert_eq!(
            result,
            Value::interval(Interval::new(0, 0, 12 * Interval::MICROS_PER_HOUR))
        );
    }

    #[test]
    fn test_error_with_no_args() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let result = ProjectionWithExprExec::eval_interval_literal(&[], &batch, 0);
        assert!(result.is_err());
    }
}
