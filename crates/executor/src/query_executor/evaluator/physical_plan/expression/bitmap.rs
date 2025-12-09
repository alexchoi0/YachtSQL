use roaring::RoaringBitmap;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_ir::FunctionName;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

fn value_to_bitmap(value: &Value) -> Result<RoaringBitmap> {
    if let Some(arr) = value.as_array() {
        let mut bitmap = RoaringBitmap::new();
        for v in arr {
            if let Some(n) = v.as_i64() {
                if n >= 0 && n <= u32::MAX as i64 {
                    bitmap.insert(n as u32);
                }
            }
        }
        Ok(bitmap)
    } else {
        Err(Error::type_mismatch("ARRAY", "other"))
    }
}

fn bitmap_to_value(bitmap: &RoaringBitmap) -> Value {
    let arr: Vec<Value> = bitmap.iter().map(|n| Value::int64(n as i64)).collect();
    Value::array(arr)
}

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_build(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query("bitmapBuild requires 1 argument"));
        }
        let arr_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let bitmap = value_to_bitmap(&arr_val)?;
        Ok(bitmap_to_value(&bitmap))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_to_array(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query("bitmapToArray requires 1 argument"));
        }
        let bitmap_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let bitmap = value_to_bitmap(&bitmap_val)?;
        Ok(bitmap_to_value(&bitmap))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_cardinality(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "bitmapCardinality requires 1 argument",
            ));
        }
        let bitmap_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let bitmap = value_to_bitmap(&bitmap_val)?;
        Ok(Value::int64(bitmap.len() as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_and(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query("bitmapAnd requires 2 arguments"));
        }
        let bitmap1 = value_to_bitmap(&Self::evaluate_expr(&args[0], batch, row_idx)?)?;
        let bitmap2 = value_to_bitmap(&Self::evaluate_expr(&args[1], batch, row_idx)?)?;
        let result = &bitmap1 & &bitmap2;
        Ok(bitmap_to_value(&result))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_or(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query("bitmapOr requires 2 arguments"));
        }
        let bitmap1 = value_to_bitmap(&Self::evaluate_expr(&args[0], batch, row_idx)?)?;
        let bitmap2 = value_to_bitmap(&Self::evaluate_expr(&args[1], batch, row_idx)?)?;
        let result = &bitmap1 | &bitmap2;
        Ok(bitmap_to_value(&result))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_xor(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query("bitmapXor requires 2 arguments"));
        }
        let bitmap1 = value_to_bitmap(&Self::evaluate_expr(&args[0], batch, row_idx)?)?;
        let bitmap2 = value_to_bitmap(&Self::evaluate_expr(&args[1], batch, row_idx)?)?;
        let result = &bitmap1 ^ &bitmap2;
        Ok(bitmap_to_value(&result))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_andnot(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query("bitmapAndnot requires 2 arguments"));
        }
        let bitmap1 = value_to_bitmap(&Self::evaluate_expr(&args[0], batch, row_idx)?)?;
        let bitmap2 = value_to_bitmap(&Self::evaluate_expr(&args[1], batch, row_idx)?)?;
        let result = &bitmap1 - &bitmap2;
        Ok(bitmap_to_value(&result))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_contains(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query("bitmapContains requires 2 arguments"));
        }
        let bitmap = value_to_bitmap(&Self::evaluate_expr(&args[0], batch, row_idx)?)?;
        let value = Self::evaluate_expr(&args[1], batch, row_idx)?
            .as_i64()
            .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
        let contains = value >= 0 && value <= u32::MAX as i64 && bitmap.contains(value as u32);
        Ok(Value::int64(if contains { 1 } else { 0 }))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_has_any(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query("bitmapHasAny requires 2 arguments"));
        }
        let bitmap1 = value_to_bitmap(&Self::evaluate_expr(&args[0], batch, row_idx)?)?;
        let bitmap2 = value_to_bitmap(&Self::evaluate_expr(&args[1], batch, row_idx)?)?;
        let has_any = !bitmap1.is_disjoint(&bitmap2);
        Ok(Value::int64(if has_any { 1 } else { 0 }))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_has_all(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query("bitmapHasAll requires 2 arguments"));
        }
        let bitmap1 = value_to_bitmap(&Self::evaluate_expr(&args[0], batch, row_idx)?)?;
        let bitmap2 = value_to_bitmap(&Self::evaluate_expr(&args[1], batch, row_idx)?)?;
        let has_all = bitmap2.is_subset(&bitmap1);
        Ok(Value::int64(if has_all { 1 } else { 0 }))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_and_cardinality(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "bitmapAndCardinality requires 2 arguments",
            ));
        }
        let bitmap1 = value_to_bitmap(&Self::evaluate_expr(&args[0], batch, row_idx)?)?;
        let bitmap2 = value_to_bitmap(&Self::evaluate_expr(&args[1], batch, row_idx)?)?;
        let count = bitmap1.intersection_len(&bitmap2);
        Ok(Value::int64(count as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_or_cardinality(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "bitmapOrCardinality requires 2 arguments",
            ));
        }
        let bitmap1 = value_to_bitmap(&Self::evaluate_expr(&args[0], batch, row_idx)?)?;
        let bitmap2 = value_to_bitmap(&Self::evaluate_expr(&args[1], batch, row_idx)?)?;
        let count = bitmap1.union_len(&bitmap2);
        Ok(Value::int64(count as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_xor_cardinality(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "bitmapXorCardinality requires 2 arguments",
            ));
        }
        let bitmap1 = value_to_bitmap(&Self::evaluate_expr(&args[0], batch, row_idx)?)?;
        let bitmap2 = value_to_bitmap(&Self::evaluate_expr(&args[1], batch, row_idx)?)?;
        let count = bitmap1.symmetric_difference_len(&bitmap2);
        Ok(Value::int64(count as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_andnot_cardinality(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "bitmapAndnotCardinality requires 2 arguments",
            ));
        }
        let bitmap1 = value_to_bitmap(&Self::evaluate_expr(&args[0], batch, row_idx)?)?;
        let bitmap2 = value_to_bitmap(&Self::evaluate_expr(&args[1], batch, row_idx)?)?;
        let count = bitmap1.difference_len(&bitmap2);
        Ok(Value::int64(count as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_min(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query("bitmapMin requires 1 argument"));
        }
        let bitmap = value_to_bitmap(&Self::evaluate_expr(&args[0], batch, row_idx)?)?;
        match bitmap.min() {
            Some(min) => Ok(Value::int64(min as i64)),
            None => Ok(Value::null()),
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_max(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query("bitmapMax requires 1 argument"));
        }
        let bitmap = value_to_bitmap(&Self::evaluate_expr(&args[0], batch, row_idx)?)?;
        match bitmap.max() {
            Some(max) => Ok(Value::int64(max as i64)),
            None => Ok(Value::null()),
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_subset_in_range(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "bitmapSubsetInRange requires 3 arguments: bitmap, range_start, range_end",
            ));
        }
        let bitmap = value_to_bitmap(&Self::evaluate_expr(&args[0], batch, row_idx)?)?;
        let start = Self::evaluate_expr(&args[1], batch, row_idx)?
            .as_i64()
            .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
        let end = Self::evaluate_expr(&args[2], batch, row_idx)?
            .as_i64()
            .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;

        let start_u32 = start.max(0) as u32;
        let end_u32 = end.max(0) as u32;

        let mut result = RoaringBitmap::new();
        for v in bitmap.iter() {
            if v >= start_u32 && v < end_u32 {
                result.insert(v);
            }
        }
        Ok(bitmap_to_value(&result))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_subset_limit(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "bitmapSubsetLimit requires 3 arguments: bitmap, range_start, limit",
            ));
        }
        let bitmap = value_to_bitmap(&Self::evaluate_expr(&args[0], batch, row_idx)?)?;
        let start = Self::evaluate_expr(&args[1], batch, row_idx)?
            .as_i64()
            .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
        let limit = Self::evaluate_expr(&args[2], batch, row_idx)?
            .as_i64()
            .ok_or_else(|| Error::type_mismatch("INT64", "other"))? as usize;

        let start_u32 = start.max(0) as u32;

        let mut result = RoaringBitmap::new();
        let mut count = 0;
        for v in bitmap.iter() {
            if v >= start_u32 {
                result.insert(v);
                count += 1;
                if count >= limit {
                    break;
                }
            }
        }
        Ok(bitmap_to_value(&result))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_bitmap_transform(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "bitmapTransform requires 3 arguments: bitmap, from_array, to_array",
            ));
        }
        let bitmap = value_to_bitmap(&Self::evaluate_expr(&args[0], batch, row_idx)?)?;
        let from_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let to_val = Self::evaluate_expr(&args[2], batch, row_idx)?;

        let from_arr = from_val
            .as_array()
            .ok_or_else(|| Error::type_mismatch("ARRAY", "other"))?;
        let to_arr = to_val
            .as_array()
            .ok_or_else(|| Error::type_mismatch("ARRAY", "other"))?;

        let mut mapping = std::collections::HashMap::new();
        for (f, t) in from_arr.iter().zip(to_arr.iter()) {
            if let (Some(from), Some(to)) = (f.as_i64(), t.as_i64()) {
                if from >= 0 && from <= u32::MAX as i64 && to >= 0 && to <= u32::MAX as i64 {
                    mapping.insert(from as u32, to as u32);
                }
            }
        }

        let mut result = RoaringBitmap::new();
        for v in bitmap.iter() {
            result.insert(*mapping.get(&v).unwrap_or(&v));
        }
        Ok(bitmap_to_value(&result))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_sub_bitmap(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "subBitmap requires 3 arguments: bitmap, offset, limit",
            ));
        }
        let bitmap = value_to_bitmap(&Self::evaluate_expr(&args[0], batch, row_idx)?)?;
        let offset = Self::evaluate_expr(&args[1], batch, row_idx)?
            .as_i64()
            .ok_or_else(|| Error::type_mismatch("INT64", "other"))? as usize;
        let limit = Self::evaluate_expr(&args[2], batch, row_idx)?
            .as_i64()
            .ok_or_else(|| Error::type_mismatch("INT64", "other"))? as usize;

        let result: RoaringBitmap = bitmap.iter().skip(offset).take(limit).collect();
        Ok(bitmap_to_value(&result))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_group_bitmap_state(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query("groupBitmapState requires 1 argument"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if let Some(n) = val.as_i64() {
            let mut bitmap = RoaringBitmap::new();
            if n >= 0 && n <= u32::MAX as i64 {
                bitmap.insert(n as u32);
            }
            Ok(bitmap_to_value(&bitmap))
        } else {
            let bitmap = value_to_bitmap(&val)?;
            Ok(bitmap_to_value(&bitmap))
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_bitmap_function(
        name: &FunctionName,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            FunctionName::BitmapBuild => Self::eval_bitmap_build(args, batch, row_idx),
            FunctionName::BitmapToArray => Self::eval_bitmap_to_array(args, batch, row_idx),
            FunctionName::BitmapCardinality => Self::eval_bitmap_cardinality(args, batch, row_idx),
            FunctionName::BitmapAnd => Self::eval_bitmap_and(args, batch, row_idx),
            FunctionName::BitmapOr => Self::eval_bitmap_or(args, batch, row_idx),
            FunctionName::BitmapXor => Self::eval_bitmap_xor(args, batch, row_idx),
            FunctionName::BitmapAndnot => Self::eval_bitmap_andnot(args, batch, row_idx),
            FunctionName::BitmapContains => Self::eval_bitmap_contains(args, batch, row_idx),
            FunctionName::BitmapHasAny => Self::eval_bitmap_has_any(args, batch, row_idx),
            FunctionName::BitmapHasAll => Self::eval_bitmap_has_all(args, batch, row_idx),
            FunctionName::BitmapAndCardinality => {
                Self::eval_bitmap_and_cardinality(args, batch, row_idx)
            }
            FunctionName::BitmapOrCardinality => {
                Self::eval_bitmap_or_cardinality(args, batch, row_idx)
            }
            FunctionName::BitmapXorCardinality => {
                Self::eval_bitmap_xor_cardinality(args, batch, row_idx)
            }
            FunctionName::BitmapAndnotCardinality => {
                Self::eval_bitmap_andnot_cardinality(args, batch, row_idx)
            }
            FunctionName::BitmapMin => Self::eval_bitmap_min(args, batch, row_idx),
            FunctionName::BitmapMax => Self::eval_bitmap_max(args, batch, row_idx),
            FunctionName::BitmapSubsetInRange => {
                Self::eval_bitmap_subset_in_range(args, batch, row_idx)
            }
            FunctionName::BitmapSubsetLimit => Self::eval_bitmap_subset_limit(args, batch, row_idx),
            FunctionName::BitmapTransform => Self::eval_bitmap_transform(args, batch, row_idx),
            FunctionName::SubBitmap => Self::eval_sub_bitmap(args, batch, row_idx),
            _ => Err(Error::unsupported_feature(format!(
                "Unknown bitmap function: {}",
                name.as_str()
            ))),
        }
    }
}
