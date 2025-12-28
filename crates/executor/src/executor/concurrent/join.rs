use std::collections::{HashMap, HashSet};

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, JoinType, PlanSchema};
use yachtsql_storage::{Record, Schema, Table};

use super::{ConcurrentPlanExecutor, plan_schema_to_schema};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl ConcurrentPlanExecutor {
    pub(crate) async fn execute_nested_loop_join(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        join_type: &JoinType,
        condition: Option<&Expr>,
        schema: &PlanSchema,
        parallel: bool,
    ) -> Result<Table> {
        let (left_table, right_table) = if parallel {
            let executor_l = self.clone();
            let executor_r = self.clone();
            let left_plan = left.clone();
            let right_plan = right.clone();
            let (l, r) = tokio::join!(
                tokio::spawn(async move { executor_l.execute_plan(&left_plan).await }),
                tokio::spawn(async move { executor_r.execute_plan(&right_plan).await })
            );
            (
                l.map_err(|e| Error::Internal(e.to_string()))??,
                r.map_err(|e| Error::Internal(e.to_string()))??,
            )
        } else {
            (
                self.execute_plan(left).await?,
                self.execute_plan(right).await?,
            )
        };
        let left_schema = left_table.schema().clone();
        let right_schema = right_table.schema().clone();
        let result_schema = plan_schema_to_schema(schema);

        let mut combined_schema = Schema::new();
        for field in left_schema.fields() {
            combined_schema.add_field(field.clone());
        }
        for field in right_schema.fields() {
            combined_schema.add_field(field.clone());
        }

        let vars = self.get_variables();
        let sys_vars = self.get_system_variables();
        let udf = self.get_user_functions();
        let evaluator = IrEvaluator::new(&combined_schema)
            .with_variables(&vars)
            .with_system_variables(&sys_vars)
            .with_user_functions(&udf);

        let mut result = Table::empty(result_schema.clone());
        let left_rows = left_table.rows()?;
        let right_rows = right_table.rows()?;
        let left_width = left_schema.field_count();
        let right_width = right_schema.field_count();

        match join_type {
            JoinType::Inner => {
                for left_record in &left_rows {
                    for right_record in &right_rows {
                        let mut combined = left_record.values().to_vec();
                        combined.extend(right_record.values().to_vec());
                        let combined_record = Record::from_values(combined.clone());

                        let matches = condition
                            .map(|c| evaluator.evaluate(c, &combined_record))
                            .transpose()?
                            .map(|v| v.as_bool().unwrap_or(false))
                            .unwrap_or(true);

                        if matches {
                            result.push_row(combined)?;
                        }
                    }
                }
            }
            JoinType::Left => {
                for left_record in &left_rows {
                    let mut found_match = false;
                    for right_record in &right_rows {
                        let mut combined = left_record.values().to_vec();
                        combined.extend(right_record.values().to_vec());
                        let combined_record = Record::from_values(combined.clone());

                        let matches = condition
                            .map(|c| evaluator.evaluate(c, &combined_record))
                            .transpose()?
                            .map(|v| v.as_bool().unwrap_or(false))
                            .unwrap_or(true);

                        if matches {
                            found_match = true;
                            result.push_row(combined)?;
                        }
                    }
                    if !found_match {
                        let mut combined = left_record.values().to_vec();
                        combined.extend(vec![Value::Null; right_width]);
                        result.push_row(combined)?;
                    }
                }
            }
            JoinType::Right => {
                for right_record in &right_rows {
                    let mut found_match = false;
                    for left_record in &left_rows {
                        let mut combined = left_record.values().to_vec();
                        combined.extend(right_record.values().to_vec());
                        let combined_record = Record::from_values(combined.clone());

                        let matches = condition
                            .map(|c| evaluator.evaluate(c, &combined_record))
                            .transpose()?
                            .map(|v| v.as_bool().unwrap_or(false))
                            .unwrap_or(true);

                        if matches {
                            found_match = true;
                            result.push_row(combined)?;
                        }
                    }
                    if !found_match {
                        let mut combined = vec![Value::Null; left_width];
                        combined.extend(right_record.values().to_vec());
                        result.push_row(combined)?;
                    }
                }
            }
            JoinType::Full => {
                let mut matched_right: HashSet<usize> = HashSet::new();
                for left_record in &left_rows {
                    let mut found_match = false;
                    for (ri, right_record) in right_rows.iter().enumerate() {
                        let mut combined = left_record.values().to_vec();
                        combined.extend(right_record.values().to_vec());
                        let combined_record = Record::from_values(combined.clone());

                        let matches = condition
                            .map(|c| evaluator.evaluate(c, &combined_record))
                            .transpose()?
                            .map(|v| v.as_bool().unwrap_or(false))
                            .unwrap_or(true);

                        if matches {
                            found_match = true;
                            matched_right.insert(ri);
                            result.push_row(combined)?;
                        }
                    }
                    if !found_match {
                        let mut combined = left_record.values().to_vec();
                        combined.extend(vec![Value::Null; right_width]);
                        result.push_row(combined)?;
                    }
                }
                for (ri, right_record) in right_rows.iter().enumerate() {
                    if !matched_right.contains(&ri) {
                        let mut combined = vec![Value::Null; left_width];
                        combined.extend(right_record.values().to_vec());
                        result.push_row(combined)?;
                    }
                }
            }
            JoinType::Cross => {
                for left_record in &left_rows {
                    for right_record in &right_rows {
                        let mut combined = left_record.values().to_vec();
                        combined.extend(right_record.values().to_vec());
                        result.push_row(combined)?;
                    }
                }
            }
        }

        Ok(result)
    }

    pub(crate) async fn execute_cross_join(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        schema: &PlanSchema,
        parallel: bool,
    ) -> Result<Table> {
        self.execute_nested_loop_join(left, right, &JoinType::Cross, None, schema, parallel)
            .await
    }

    pub(crate) async fn execute_hash_join(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        join_type: &JoinType,
        left_keys: &[Expr],
        right_keys: &[Expr],
        schema: &PlanSchema,
        parallel: bool,
    ) -> Result<Table> {
        let (left_table, right_table) = if parallel {
            let executor_l = self.clone();
            let executor_r = self.clone();
            let left_plan = left.clone();
            let right_plan = right.clone();
            let (l, r) = tokio::join!(
                tokio::spawn(async move { executor_l.execute_plan(&left_plan).await }),
                tokio::spawn(async move { executor_r.execute_plan(&right_plan).await })
            );
            (
                l.map_err(|e| Error::Internal(e.to_string()))??,
                r.map_err(|e| Error::Internal(e.to_string()))??,
            )
        } else {
            (
                self.execute_plan(left).await?,
                self.execute_plan(right).await?,
            )
        };
        let left_schema = left_table.schema().clone();
        let right_schema = right_table.schema().clone();
        let result_schema = plan_schema_to_schema(schema);

        match join_type {
            JoinType::Inner => {
                let left_rows: Vec<Record> = left_table.rows()?;
                let right_rows: Vec<Record> = right_table.rows()?;

                let build_on_right = right_rows.len() <= left_rows.len();

                let (build_rows, probe_rows, build_schema, probe_schema, build_keys, probe_keys) =
                    if build_on_right {
                        (
                            right_rows,
                            left_rows,
                            &right_schema,
                            &left_schema,
                            right_keys,
                            left_keys,
                        )
                    } else {
                        (
                            left_rows,
                            right_rows,
                            &left_schema,
                            &right_schema,
                            left_keys,
                            right_keys,
                        )
                    };

                let vars = self.get_variables();
                let sys_vars = self.get_system_variables();
                let udf = self.get_user_functions();
                let build_evaluator = IrEvaluator::new(build_schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);

                let mut hash_table: HashMap<Vec<Value>, Vec<Record>> = HashMap::new();
                for build_record in &build_rows {
                    let key_values: Vec<Value> = build_keys
                        .iter()
                        .map(|expr| build_evaluator.evaluate(expr, build_record))
                        .collect::<Result<Vec<_>>>()?;

                    if key_values.iter().any(|v| matches!(v, Value::Null)) {
                        continue;
                    }

                    hash_table
                        .entry(key_values)
                        .or_default()
                        .push(build_record.clone());
                }

                let combine_row = |probe_rec: &Record, build_rec: &Record| -> Vec<Value> {
                    if build_on_right {
                        let mut combined = probe_rec.values().to_vec();
                        combined.extend(build_rec.values().to_vec());
                        combined
                    } else {
                        let mut combined = build_rec.values().to_vec();
                        combined.extend(probe_rec.values().to_vec());
                        combined
                    }
                };

                if parallel && probe_rows.len() >= 10000 {
                    let num_threads = std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(4);
                    let chunk_size = probe_rows.len().div_ceil(num_threads);

                    let chunk_results: Vec<Result<Vec<Vec<Value>>>> = std::thread::scope(|s| {
                        let handles: Vec<_> = probe_rows
                            .chunks(chunk_size)
                            .map(|chunk| {
                                let hash_table = &hash_table;
                                let vars = &vars;
                                let sys_vars = &sys_vars;
                                let udf = &udf;
                                let combine_row = &combine_row;
                                s.spawn(move || {
                                    let probe_evaluator = IrEvaluator::new(probe_schema)
                                        .with_variables(vars)
                                        .with_system_variables(sys_vars)
                                        .with_user_functions(udf);
                                    let mut rows = Vec::new();
                                    for probe_record in chunk {
                                        let key_values: Vec<Value> = probe_keys
                                            .iter()
                                            .map(|expr| {
                                                probe_evaluator.evaluate(expr, probe_record)
                                            })
                                            .collect::<Result<Vec<_>>>()?;

                                        if key_values.iter().any(|v| matches!(v, Value::Null)) {
                                            continue;
                                        }

                                        if let Some(matching_rows) = hash_table.get(&key_values) {
                                            for build_record in matching_rows {
                                                rows.push(combine_row(probe_record, build_record));
                                            }
                                        }
                                    }
                                    Ok(rows)
                                })
                            })
                            .collect();
                        handles.into_iter().map(|h| h.join().unwrap()).collect()
                    });

                    let mut result = Table::empty(result_schema);
                    for chunk_result in chunk_results {
                        for row in chunk_result? {
                            result.push_row(row)?;
                        }
                    }
                    Ok(result)
                } else {
                    let probe_evaluator = IrEvaluator::new(probe_schema)
                        .with_variables(&vars)
                        .with_system_variables(&sys_vars)
                        .with_user_functions(&udf);
                    let mut result = Table::empty(result_schema);
                    for probe_record in &probe_rows {
                        let key_values: Vec<Value> = probe_keys
                            .iter()
                            .map(|expr| probe_evaluator.evaluate(expr, probe_record))
                            .collect::<Result<Vec<_>>>()?;

                        if key_values.iter().any(|v| matches!(v, Value::Null)) {
                            continue;
                        }

                        if let Some(matching_rows) = hash_table.get(&key_values) {
                            for build_record in matching_rows {
                                result.push_row(combine_row(probe_record, build_record))?;
                            }
                        }
                    }
                    Ok(result)
                }
            }
            _ => {
                panic!("HashJoin only supports Inner join type currently");
            }
        }
    }
}
