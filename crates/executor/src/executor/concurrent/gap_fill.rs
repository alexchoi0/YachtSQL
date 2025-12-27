use std::collections::BTreeMap;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_ir::{Expr, GapFillColumn, GapFillStrategy, PlanSchema};
use yachtsql_storage::{Record, Table};

use super::{ConcurrentPlanExecutor, plan_schema_to_schema};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl ConcurrentPlanExecutor<'_> {
    pub(crate) async fn execute_gap_fill(
        &self,
        input: &PhysicalPlan,
        ts_column: &str,
        bucket_width: &Expr,
        value_columns: &[GapFillColumn],
        partitioning_columns: &[String],
        origin: Option<&Expr>,
        input_schema: &PlanSchema,
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input).await?;
        let storage_schema = input_table.schema();

        let ts_idx = input_schema
            .fields
            .iter()
            .position(|f| f.name.to_uppercase() == ts_column.to_uppercase())
            .ok_or_else(|| Error::invalid_query(format!("Column not found: {}", ts_column)))?;

        let partition_indices: Vec<usize> = partitioning_columns
            .iter()
            .filter_map(|p| {
                input_schema
                    .fields
                    .iter()
                    .position(|f| f.name.to_uppercase() == p.to_uppercase())
            })
            .collect();

        let value_col_indices: Vec<(usize, GapFillStrategy)> = value_columns
            .iter()
            .filter_map(|vc| {
                input_schema
                    .fields
                    .iter()
                    .position(|f| f.name.to_uppercase() == vc.column_name.to_uppercase())
                    .map(|idx| (idx, vc.strategy))
            })
            .collect();

        let vars = self.get_variables();
        let sys_vars = self.get_system_variables();
        let udf = self.get_user_functions();
        let evaluator = IrEvaluator::new(storage_schema)
            .with_variables(&vars)
            .with_system_variables(&sys_vars)
            .with_user_functions(&udf);
        let empty_record = Record::new();

        let bucket_interval = evaluator.evaluate(bucket_width, &empty_record)?;
        let bucket_millis = match bucket_interval {
            Value::Interval(ref interval) => {
                (interval.months as i64 * 30 * 24 * 60 * 60 * 1000)
                    + (interval.days as i64 * 24 * 60 * 60 * 1000)
                    + (interval.nanos / 1_000_000)
            }
            _ => {
                return Err(Error::invalid_query("bucket_width must be an interval"));
            }
        };

        let origin_offset = if let Some(origin_expr) = origin {
            let origin_val = evaluator.evaluate(origin_expr, &empty_record)?;
            match origin_val {
                Value::DateTime(d) => d.and_utc().timestamp_millis() % bucket_millis,
                Value::Timestamp(d) => d.timestamp_millis() % bucket_millis,
                _ => 0,
            }
        } else {
            0
        };

        let mut partitions: BTreeMap<Vec<Value>, Vec<(i64, Vec<Value>)>> = BTreeMap::new();

        for row in input_table.rows()? {
            let row_values = row.values().to_vec();
            let ts_val = &row_values[ts_idx];

            let ts_millis = match ts_val {
                Value::DateTime(d) => d.and_utc().timestamp_millis(),
                Value::Timestamp(d) => d.timestamp_millis(),
                Value::Date(d) => d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis(),
                _ => continue,
            };

            let partition_key: Vec<Value> = partition_indices
                .iter()
                .map(|&idx| row_values[idx].clone())
                .collect();

            let values_for_row: Vec<Value> = value_col_indices
                .iter()
                .map(|(idx, _)| row_values[*idx].clone())
                .collect();

            partitions
                .entry(partition_key)
                .or_default()
                .push((ts_millis, values_for_row));
        }

        let output_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(output_schema.clone());

        for (partition_key, mut entries) in partitions {
            entries.sort_by_key(|(ts, _)| *ts);

            if entries.is_empty() {
                continue;
            }

            let min_original_ts = entries.first().map(|(ts, _)| *ts).unwrap();
            let max_original_ts = entries.last().map(|(ts, _)| *ts).unwrap();

            let min_bucket = {
                let floored = ((min_original_ts - origin_offset) / bucket_millis) * bucket_millis
                    + origin_offset;
                if floored < min_original_ts {
                    floored + bucket_millis
                } else {
                    floored
                }
            };
            let max_bucket =
                ((max_original_ts - origin_offset) / bucket_millis) * bucket_millis + origin_offset;

            let mut exact_match_map: BTreeMap<i64, Vec<Value>> = BTreeMap::new();
            for (ts, values) in &entries {
                let bucket_ts =
                    ((*ts - origin_offset) / bucket_millis) * bucket_millis + origin_offset;
                if *ts == bucket_ts {
                    exact_match_map.insert(bucket_ts, values.clone());
                }
            }

            let mut last_values: Vec<Option<Value>> = vec![None; value_col_indices.len()];

            let mut bucket = min_bucket;
            while bucket <= max_bucket {
                for (ts, values) in &entries {
                    if *ts <= bucket {
                        for (i, val) in values.iter().enumerate() {
                            if !val.is_null() {
                                last_values[i] = Some(val.clone());
                            }
                        }
                    }
                }

                let row_values = if let Some(existing) = exact_match_map.get(&bucket) {
                    existing.clone()
                } else {
                    value_col_indices
                        .iter()
                        .enumerate()
                        .map(|(i, (_, strategy))| match strategy {
                            GapFillStrategy::Null => Value::null(),
                            GapFillStrategy::Locf => {
                                last_values[i].clone().unwrap_or(Value::null())
                            }
                            GapFillStrategy::Linear => {
                                let prev_entry =
                                    entries.iter().filter(|(ts, _)| *ts < bucket).next_back();
                                let next_entry = entries.iter().find(|(ts, _)| *ts > bucket);

                                match (prev_entry, next_entry) {
                                    (Some((prev_ts, prev_vals)), Some((next_ts, next_vals))) => {
                                        interpolate_value(
                                            &prev_vals[i],
                                            &next_vals[i],
                                            *prev_ts,
                                            *next_ts,
                                            bucket,
                                        )
                                    }
                                    _ => Value::null(),
                                }
                            }
                        })
                        .collect()
                };

                let ts_value = match &input_schema.fields[ts_idx].data_type {
                    DataType::DateTime => Value::DateTime(
                        chrono::DateTime::from_timestamp_millis(bucket)
                            .unwrap()
                            .naive_utc(),
                    ),
                    DataType::Timestamp => {
                        Value::Timestamp(chrono::DateTime::from_timestamp_millis(bucket).unwrap())
                    }
                    _ => Value::DateTime(
                        chrono::DateTime::from_timestamp_millis(bucket)
                            .unwrap()
                            .naive_utc(),
                    ),
                };

                let mut record_values = vec![ts_value];
                record_values.extend(partition_key.clone());
                record_values.extend(row_values);

                result.push_row(record_values)?;

                bucket += bucket_millis;
            }
        }

        Ok(result)
    }
}

fn interpolate_value(
    prev: &Value,
    next: &Value,
    prev_ts: i64,
    next_ts: i64,
    current_ts: i64,
) -> Value {
    use ordered_float::OrderedFloat;
    let ratio = (current_ts - prev_ts) as f64 / (next_ts - prev_ts) as f64;

    match (prev, next) {
        (Value::Int64(p), Value::Int64(n)) => {
            let interpolated = *p as f64 + (*n as f64 - *p as f64) * ratio;
            Value::Int64(interpolated.round() as i64)
        }
        (Value::Float64(p), Value::Float64(n)) => {
            let p_val: f64 = (*p).into();
            let n_val: f64 = (*n).into();
            Value::Float64(OrderedFloat(p_val + (n_val - p_val) * ratio))
        }
        (Value::Int64(p), Value::Float64(n)) => {
            let n_val: f64 = (*n).into();
            Value::Float64(OrderedFloat(*p as f64 + (n_val - *p as f64) * ratio))
        }
        (Value::Float64(p), Value::Int64(n)) => {
            let p_val: f64 = (*p).into();
            Value::Float64(OrderedFloat(p_val + (*n as f64 - p_val) * ratio))
        }
        _ => Value::null(),
    }
}

#[allow(dead_code)]
fn default_value_for_type(data_type: &DataType) -> Value {
    match data_type {
        DataType::Int64 => Value::Int64(0),
        DataType::Float64 => Value::Float64(ordered_float::OrderedFloat(0.0)),
        DataType::Bool => Value::Bool(false),
        DataType::String => Value::String(String::new()),
        _ => Value::Null,
    }
}
