use std::collections::{HashMap, HashSet};

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, JoinType, PlanSchema};
use yachtsql_storage::{Record, Schema, Table};

use super::{ConcurrentPlanExecutor, plan_schema_to_schema};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl ConcurrentPlanExecutor<'_> {
    pub(crate) async fn execute_nested_loop_join(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        join_type: &JoinType,
        condition: Option<&Expr>,
        schema: &PlanSchema,
        parallel: bool,
    ) -> Result<Table> {
        let use_parallel = parallel && self.is_parallel_execution_enabled();

        let (left_table, right_table) = if use_parallel {
            let rt = tokio::runtime::Handle::current();
            let (l, r) = std::thread::scope(|s| {
                let left_handle = s.spawn(|| rt.block_on(self.execute_plan(left)));
                let right_handle = s.spawn(|| rt.block_on(self.execute_plan(right)));
                (left_handle.join().unwrap(), right_handle.join().unwrap())
            });
            (l?, r?)
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
        let use_parallel = parallel && self.is_parallel_execution_enabled();

        let (left_table, right_table) = if use_parallel {
            let rt = tokio::runtime::Handle::current();
            let (l, r) = std::thread::scope(|s| {
                let left_handle = s.spawn(|| rt.block_on(self.execute_plan(left)));
                let right_handle = s.spawn(|| rt.block_on(self.execute_plan(right)));
                (left_handle.join().unwrap(), right_handle.join().unwrap())
            });
            (l?, r?)
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
                let vars = self.get_variables();
                let sys_vars = self.get_system_variables();
                let udf = self.get_user_functions();
                let left_evaluator = IrEvaluator::new(&left_schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);
                let right_evaluator = IrEvaluator::new(&right_schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);

                let right_rows = right_table.rows()?;
                let mut hash_table: HashMap<Vec<Value>, Vec<Record>> = HashMap::new();

                for right_record in right_rows {
                    let key_values: Vec<Value> = right_keys
                        .iter()
                        .map(|expr| right_evaluator.evaluate(expr, &right_record))
                        .collect::<Result<Vec<_>>>()?;

                    let has_null = key_values.iter().any(|v| matches!(v, Value::Null));
                    if has_null {
                        continue;
                    }

                    hash_table.entry(key_values).or_default().push(right_record);
                }

                let mut result = Table::empty(result_schema);
                for left_record in left_table.rows()? {
                    let key_values: Vec<Value> = left_keys
                        .iter()
                        .map(|expr| left_evaluator.evaluate(expr, &left_record))
                        .collect::<Result<Vec<_>>>()?;

                    let has_null = key_values.iter().any(|v| matches!(v, Value::Null));
                    if has_null {
                        continue;
                    }

                    if let Some(matching_rows) = hash_table.get(&key_values) {
                        for right_record in matching_rows {
                            let mut combined = left_record.values().to_vec();
                            combined.extend(right_record.values().to_vec());
                            result.push_row(combined)?;
                        }
                    }
                }

                Ok(result)
            }
            _ => {
                panic!("HashJoin only supports Inner join type currently");
            }
        }
    }
}
