use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{IntervalValue, Value};
use yachtsql_ir::{Expr, PlanSchema};
use yachtsql_storage::{Record, Table};

use super::PlanExecutor;
use crate::ir_evaluator::IrEvaluator;
use crate::plan::ExecutorPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_gap_fill(
        &mut self,
        input: &ExecutorPlan,
        ts_column: &str,
        bucket_width: &Expr,
        partition_columns: &[String],
        origin: Option<&Expr>,
        value_columns: &[String],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let source = self.execute_plan(input)?;
        let source_records = source.to_records()?;
        let source_schema = source.schema();

        let ts_col_idx = source_schema
            .fields()
            .iter()
            .position(|f| f.name.to_uppercase() == ts_column.to_uppercase())
            .ok_or_else(|| Error::InvalidQuery(format!("Column '{}' not found", ts_column)))?;

        let partition_indices: Vec<usize> = partition_columns
            .iter()
            .map(|col| {
                source_schema
                    .fields()
                    .iter()
                    .position(|f| f.name.to_uppercase() == col.to_uppercase())
                    .ok_or_else(|| {
                        Error::InvalidQuery(format!("Partition column '{}' not found", col))
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        let value_indices: Vec<usize> = if value_columns.is_empty() {
            (0..source_schema.field_count())
                .filter(|i| *i != ts_col_idx && !partition_indices.contains(i))
                .collect()
        } else {
            value_columns
                .iter()
                .map(|col| {
                    source_schema
                        .fields()
                        .iter()
                        .position(|f| f.name.to_uppercase() == col.to_uppercase())
                        .ok_or_else(|| {
                            Error::InvalidQuery(format!("Value column '{}' not found", col))
                        })
                })
                .collect::<Result<Vec<_>>>()?
        };

        let empty_record = Record::from_values(vec![]);
        let empty_schema = yachtsql_storage::Schema::new();
        let evaluator =
            IrEvaluator::new(&empty_schema).with_user_functions(&self.user_function_defs);

        let interval = evaluator.evaluate(bucket_width, &empty_record)?;
        let bucket_width_interval = match interval {
            Value::Interval(iv) => iv,
            _ => {
                return Err(Error::InvalidQuery(
                    "bucket_width must be an INTERVAL".to_string(),
                ));
            }
        };

        let origin_value = if let Some(origin_expr) = origin {
            Some(evaluator.evaluate(origin_expr, &empty_record)?)
        } else {
            None
        };

        let mut partitions: HashMap<Vec<Value>, Vec<&Record>> = HashMap::new();
        for row in &source_records {
            let partition_key: Vec<Value> = partition_indices
                .iter()
                .map(|&i| row.values()[i].clone())
                .collect();
            partitions.entry(partition_key).or_default().push(row);
        }

        let mut result_rows: Vec<Vec<Value>> = Vec::new();

        for (partition_key, partition_rows) in partitions {
            let mut timestamps: Vec<i64> = Vec::new();
            let mut is_date = false;

            for row in &partition_rows {
                let ts_val = &row.values()[ts_col_idx];
                match ts_val {
                    Value::Timestamp(ts) => {
                        timestamps.push(ts.timestamp_micros());
                    }
                    Value::Date(d) => {
                        is_date = true;
                        let ts = d.and_hms_opt(0, 0, 0).unwrap();
                        timestamps.push(
                            DateTime::<Utc>::from_naive_utc_and_offset(ts, Utc).timestamp_micros(),
                        );
                    }
                    Value::DateTime(dt) => {
                        timestamps.push(
                            DateTime::<Utc>::from_naive_utc_and_offset(*dt, Utc).timestamp_micros(),
                        );
                    }
                    _ => continue,
                }
            }

            if timestamps.is_empty() {
                continue;
            }

            let min_ts = *timestamps.iter().min().unwrap();
            let max_ts = *timestamps.iter().max().unwrap();

            let origin_micros = match &origin_value {
                Some(Value::Timestamp(ts)) => ts.timestamp_micros(),
                Some(Value::Date(d)) => {
                    let ts = d.and_hms_opt(0, 0, 0).unwrap();
                    DateTime::<Utc>::from_naive_utc_and_offset(ts, Utc).timestamp_micros()
                }
                Some(Value::DateTime(dt)) => {
                    DateTime::<Utc>::from_naive_utc_and_offset(dt.clone(), Utc).timestamp_micros()
                }
                _ => min_ts,
            };

            let bucket_micros = interval_to_micros(&bucket_width_interval);

            if bucket_micros <= 0 {
                return Err(Error::InvalidQuery(
                    "bucket_width must be positive".to_string(),
                ));
            }

            let start_bucket =
                origin_micros + ((min_ts - origin_micros) / bucket_micros) * bucket_micros;

            let mut row_map: HashMap<i64, &Record> = HashMap::new();
            for row in &partition_rows {
                let ts_val = &row.values()[ts_col_idx];
                let ts_micros = match ts_val {
                    Value::Timestamp(ts) => ts.timestamp_micros(),
                    Value::Date(d) => {
                        let ts = d.and_hms_opt(0, 0, 0).unwrap();
                        DateTime::<Utc>::from_naive_utc_and_offset(ts, Utc).timestamp_micros()
                    }
                    Value::DateTime(dt) => {
                        DateTime::<Utc>::from_naive_utc_and_offset(*dt, Utc).timestamp_micros()
                    }
                    _ => continue,
                };
                let bucket =
                    origin_micros + ((ts_micros - origin_micros) / bucket_micros) * bucket_micros;
                row_map.insert(bucket, row);
            }

            let mut current_bucket = start_bucket;
            while current_bucket <= max_ts {
                let mut row_values: Vec<Value> = vec![Value::Null; source_schema.field_count()];

                for (i, pk_val) in partition_key.iter().enumerate() {
                    row_values[partition_indices[i]] = pk_val.clone();
                }

                if let Some(existing_row) = row_map.get(&current_bucket) {
                    for (i, val) in existing_row.values().iter().enumerate() {
                        row_values[i] = val.clone();
                    }
                } else {
                    let ts_val = if is_date {
                        let dt = DateTime::from_timestamp_micros(current_bucket).unwrap();
                        Value::date(dt.date_naive())
                    } else {
                        Value::timestamp(DateTime::from_timestamp_micros(current_bucket).unwrap())
                    };
                    row_values[ts_col_idx] = ts_val;
                }

                result_rows.push(row_values);
                current_bucket += bucket_micros;
            }
        }

        let result_schema = super::plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);
        for row in result_rows {
            result.push_row(row)?;
        }

        Ok(result)
    }
}

fn interval_to_micros(interval: &IntervalValue) -> i64 {
    let month_micros = (interval.months as i64) * 30 * 24 * 60 * 60 * 1_000_000;
    let day_micros = (interval.days as i64) * 24 * 60 * 60 * 1_000_000;
    let nano_micros = interval.nanos / IntervalValue::NANOS_PER_MICRO;
    month_micros + day_micros + nano_micros
}
