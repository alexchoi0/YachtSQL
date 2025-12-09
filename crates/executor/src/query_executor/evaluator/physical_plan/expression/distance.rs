use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_distance_function(
        func_name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match func_name {
            "L1NORM" => Self::eval_l1_norm(args, batch, row_idx),
            "L2NORM" => Self::eval_l2_norm(args, batch, row_idx),
            "LINFNORM" => Self::eval_linf_norm(args, batch, row_idx),
            "LPNORM" => Self::eval_lp_norm(args, batch, row_idx),
            "L1DISTANCE" => Self::eval_l1_distance(args, batch, row_idx),
            "L2DISTANCE" => Self::eval_l2_distance(args, batch, row_idx),
            "LINFDISTANCE" => Self::eval_linf_distance(args, batch, row_idx),
            "LPDISTANCE" => Self::eval_lp_distance(args, batch, row_idx),
            "L1NORMALIZE" => Self::eval_l1_normalize(args, batch, row_idx),
            "L2NORMALIZE" => Self::eval_l2_normalize(args, batch, row_idx),
            "LINFNORMALIZE" => Self::eval_linf_normalize(args, batch, row_idx),
            "LPNORMALIZE" => Self::eval_lp_normalize(args, batch, row_idx),
            "COSINEDISTANCE" => Self::eval_cosine_distance(args, batch, row_idx),
            "DOTPRODUCT" => Self::eval_dot_product(args, batch, row_idx),
            "L2SQUAREDDISTANCE" => Self::eval_l2_squared_distance(args, batch, row_idx),
            _ => Err(Error::unsupported_feature(format!(
                "Distance function {} not implemented",
                func_name
            ))),
        }
    }

    fn extract_vector(val: &Value) -> Result<Vec<f64>> {
        let arr = val.as_array().ok_or_else(|| {
            Error::invalid_query("Distance function argument must be an array".to_string())
        })?;
        arr.iter()
            .map(|v| {
                v.as_f64()
                    .or_else(|| v.as_i64().map(|i| i as f64))
                    .ok_or_else(|| Error::invalid_query("Array must contain numbers".to_string()))
            })
            .collect()
    }

    fn eval_l1_norm(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("L1Norm", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let vec = Self::extract_vector(&val)?;
        let norm: f64 = vec.iter().map(|x| x.abs()).sum();
        Ok(Value::float64(norm))
    }

    fn eval_l2_norm(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("L2Norm", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let vec = Self::extract_vector(&val)?;
        let norm: f64 = vec.iter().map(|x| x * x).sum::<f64>().sqrt();
        Ok(Value::float64(norm))
    }

    fn eval_linf_norm(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("LinfNorm", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let vec = Self::extract_vector(&val)?;
        let norm = vec
            .iter()
            .map(|x| x.abs())
            .fold(f64::NEG_INFINITY, f64::max);
        Ok(Value::float64(norm))
    }

    fn eval_lp_norm(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("LpNorm", args, 2)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let p_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if val.is_null() || p_val.is_null() {
            return Ok(Value::null());
        }
        let vec = Self::extract_vector(&val)?;
        let p = p_val
            .as_f64()
            .or_else(|| p_val.as_i64().map(|i| i as f64))
            .ok_or_else(|| Error::invalid_query("p must be a number".to_string()))?;
        let norm: f64 = vec
            .iter()
            .map(|x| x.abs().powf(p))
            .sum::<f64>()
            .powf(1.0 / p);
        Ok(Value::float64(norm))
    }

    fn eval_l1_distance(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("L1Distance", args, 2)?;
        let val1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let val2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if val1.is_null() || val2.is_null() {
            return Ok(Value::null());
        }
        let vec1 = Self::extract_vector(&val1)?;
        let vec2 = Self::extract_vector(&val2)?;
        if vec1.len() != vec2.len() {
            return Err(Error::invalid_query(
                "Vectors must have the same length".to_string(),
            ));
        }
        let dist: f64 = vec1
            .iter()
            .zip(vec2.iter())
            .map(|(a, b)| (a - b).abs())
            .sum();
        Ok(Value::float64(dist))
    }

    fn eval_l2_distance(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("L2Distance", args, 2)?;
        let val1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let val2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if val1.is_null() || val2.is_null() {
            return Ok(Value::null());
        }
        let vec1 = Self::extract_vector(&val1)?;
        let vec2 = Self::extract_vector(&val2)?;
        if vec1.len() != vec2.len() {
            return Err(Error::invalid_query(
                "Vectors must have the same length".to_string(),
            ));
        }
        let dist: f64 = vec1
            .iter()
            .zip(vec2.iter())
            .map(|(a, b)| (a - b) * (a - b))
            .sum::<f64>()
            .sqrt();
        Ok(Value::float64(dist))
    }

    fn eval_linf_distance(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("LinfDistance", args, 2)?;
        let val1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let val2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if val1.is_null() || val2.is_null() {
            return Ok(Value::null());
        }
        let vec1 = Self::extract_vector(&val1)?;
        let vec2 = Self::extract_vector(&val2)?;
        if vec1.len() != vec2.len() {
            return Err(Error::invalid_query(
                "Vectors must have the same length".to_string(),
            ));
        }
        let dist = vec1
            .iter()
            .zip(vec2.iter())
            .map(|(a, b)| (a - b).abs())
            .fold(f64::NEG_INFINITY, f64::max);
        Ok(Value::float64(dist))
    }

    fn eval_lp_distance(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("LpDistance", args, 3)?;
        let val1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let val2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let p_val = Self::evaluate_expr(&args[2], batch, row_idx)?;
        if val1.is_null() || val2.is_null() || p_val.is_null() {
            return Ok(Value::null());
        }
        let vec1 = Self::extract_vector(&val1)?;
        let vec2 = Self::extract_vector(&val2)?;
        if vec1.len() != vec2.len() {
            return Err(Error::invalid_query(
                "Vectors must have the same length".to_string(),
            ));
        }
        let p = p_val
            .as_f64()
            .or_else(|| p_val.as_i64().map(|i| i as f64))
            .ok_or_else(|| Error::invalid_query("p must be a number".to_string()))?;
        let dist: f64 = vec1
            .iter()
            .zip(vec2.iter())
            .map(|(a, b)| (a - b).abs().powf(p))
            .sum::<f64>()
            .powf(1.0 / p);
        Ok(Value::float64(dist))
    }

    fn eval_l1_normalize(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("L1Normalize", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let vec = Self::extract_vector(&val)?;
        let norm: f64 = vec.iter().map(|x| x.abs()).sum();
        if norm == 0.0 {
            return Ok(Value::array(vec.into_iter().map(Value::float64).collect()));
        }
        let normalized: Vec<Value> = vec.into_iter().map(|x| Value::float64(x / norm)).collect();
        Ok(Value::array(normalized))
    }

    fn eval_l2_normalize(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("L2Normalize", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let vec = Self::extract_vector(&val)?;
        let norm: f64 = vec.iter().map(|x| x * x).sum::<f64>().sqrt();
        if norm == 0.0 {
            return Ok(Value::array(vec.into_iter().map(Value::float64).collect()));
        }
        let normalized: Vec<Value> = vec.into_iter().map(|x| Value::float64(x / norm)).collect();
        Ok(Value::array(normalized))
    }

    fn eval_linf_normalize(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("LinfNormalize", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let vec = Self::extract_vector(&val)?;
        let norm = vec
            .iter()
            .map(|x| x.abs())
            .fold(f64::NEG_INFINITY, f64::max);
        if norm == 0.0 {
            return Ok(Value::array(vec.into_iter().map(Value::float64).collect()));
        }
        let normalized: Vec<Value> = vec.into_iter().map(|x| Value::float64(x / norm)).collect();
        Ok(Value::array(normalized))
    }

    fn eval_lp_normalize(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("LpNormalize", args, 2)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let p_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if val.is_null() || p_val.is_null() {
            return Ok(Value::null());
        }
        let vec = Self::extract_vector(&val)?;
        let p = p_val
            .as_f64()
            .or_else(|| p_val.as_i64().map(|i| i as f64))
            .ok_or_else(|| Error::invalid_query("p must be a number".to_string()))?;
        let norm: f64 = vec
            .iter()
            .map(|x| x.abs().powf(p))
            .sum::<f64>()
            .powf(1.0 / p);
        if norm == 0.0 {
            return Ok(Value::array(vec.into_iter().map(Value::float64).collect()));
        }
        let normalized: Vec<Value> = vec.into_iter().map(|x| Value::float64(x / norm)).collect();
        Ok(Value::array(normalized))
    }

    fn eval_cosine_distance(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("cosineDistance", args, 2)?;
        let val1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let val2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if val1.is_null() || val2.is_null() {
            return Ok(Value::null());
        }
        let vec1 = Self::extract_vector(&val1)?;
        let vec2 = Self::extract_vector(&val2)?;
        if vec1.len() != vec2.len() {
            return Err(Error::invalid_query(
                "Vectors must have the same length".to_string(),
            ));
        }
        let dot: f64 = vec1.iter().zip(vec2.iter()).map(|(a, b)| a * b).sum();
        let norm1: f64 = vec1.iter().map(|x| x * x).sum::<f64>().sqrt();
        let norm2: f64 = vec2.iter().map(|x| x * x).sum::<f64>().sqrt();
        if norm1 == 0.0 || norm2 == 0.0 {
            return Ok(Value::float64(1.0));
        }
        let cosine_similarity = dot / (norm1 * norm2);
        Ok(Value::float64(1.0 - cosine_similarity))
    }

    fn eval_dot_product(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("dotProduct", args, 2)?;
        let val1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let val2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if val1.is_null() || val2.is_null() {
            return Ok(Value::null());
        }
        let vec1 = Self::extract_vector(&val1)?;
        let vec2 = Self::extract_vector(&val2)?;
        if vec1.len() != vec2.len() {
            return Err(Error::invalid_query(
                "Vectors must have the same length".to_string(),
            ));
        }
        let dot: f64 = vec1.iter().zip(vec2.iter()).map(|(a, b)| a * b).sum();
        Ok(Value::float64(dot))
    }

    fn eval_l2_squared_distance(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("L2SquaredDistance", args, 2)?;
        let val1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let val2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if val1.is_null() || val2.is_null() {
            return Ok(Value::null());
        }
        let vec1 = Self::extract_vector(&val1)?;
        let vec2 = Self::extract_vector(&val2)?;
        if vec1.len() != vec2.len() {
            return Err(Error::invalid_query(
                "Vectors must have the same length".to_string(),
            ));
        }
        let dist: f64 = vec1
            .iter()
            .zip(vec2.iter())
            .map(|(a, b)| (a - b) * (a - b))
            .sum();
        Ok(Value::float64(dist))
    }
}
