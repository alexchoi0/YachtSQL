use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn eval_gamma(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("GAMMA requires exactly 1 argument"));
        }

        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }

        let x = if let Some(i) = val.as_i64() {
            i as f64
        } else if let Some(f) = val.as_f64() {
            f
        } else if let Some(n) = val.as_numeric() {
            n.to_string().parse::<f64>().unwrap_or(f64::NAN)
        } else {
            return Err(Error::invalid_query(
                "GAMMA argument must be a numeric value",
            ));
        };

        let result = gamma(x);
        Ok(Value::float64(result))
    }

    pub(super) fn eval_lgamma(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("LGAMMA requires exactly 1 argument"));
        }

        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }

        let x = if let Some(i) = val.as_i64() {
            i as f64
        } else if let Some(f) = val.as_f64() {
            f
        } else if let Some(n) = val.as_numeric() {
            n.to_string().parse::<f64>().unwrap_or(f64::NAN)
        } else {
            return Err(Error::invalid_query(
                "LGAMMA argument must be a numeric value",
            ));
        };

        let result = lgamma(x);
        Ok(Value::float64(result))
    }
}

fn gamma(x: f64) -> f64 {
    if x.is_nan() || x.is_infinite() {
        return x;
    }

    if x <= 0.0 && x.floor() == x {
        return f64::NAN;
    }

    #[allow(clippy::excessive_precision)]
    const COEFFS: [f64; 9] = [
        0.99999999999980993,
        676.5203681218851,
        -1259.1392167224028,
        771.32342877765313,
        -176.61502916214059,
        12.507343278686905,
        -0.13857109526572012,
        9.9843695780195716e-6,
        1.5056327351493116e-7,
    ];
    let g = 7;

    if x < 0.5 {
        std::f64::consts::PI / (f64::sin(std::f64::consts::PI * x) * gamma(1.0 - x))
    } else {
        let z = x - 1.0;
        let mut sum = COEFFS[0];
        for i in 1..g + 2 {
            sum += COEFFS[i] / (z + i as f64);
        }
        let t = z + g as f64 + 0.5;
        (2.0 * std::f64::consts::PI).sqrt() * t.powf(z + 0.5) * (-t).exp() * sum
    }
}

fn lgamma(x: f64) -> f64 {
    if x.is_nan() {
        return x;
    }
    if x <= 0.0 && x.floor() == x {
        return f64::INFINITY;
    }
    if x < 0.5 {
        let sin_pi_x = (std::f64::consts::PI * x).sin();
        if sin_pi_x.abs() < 1e-15 {
            return f64::INFINITY;
        }
        std::f64::consts::PI.ln() - sin_pi_x.abs().ln() - lgamma(1.0 - x)
    } else {
        #[allow(clippy::excessive_precision)]
        const COEFFS: [f64; 9] = [
            0.99999999999980993,
            676.5203681218851,
            -1259.1392167224028,
            771.32342877765313,
            -176.61502916214059,
            12.507343278686905,
            -0.13857109526572012,
            9.9843695780195716e-6,
            1.5056327351493116e-7,
        ];
        let g = 7;

        let z = x - 1.0;
        let mut sum = COEFFS[0];
        for i in 1..g + 2 {
            sum += COEFFS[i] / (z + i as f64);
        }
        let t = z + g as f64 + 0.5;
        0.5 * (2.0 * std::f64::consts::PI).ln() + (z + 0.5) * t.ln() - t + sum.ln()
    }
}
