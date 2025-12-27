use std::collections::HashSet;

use rand::Rng;
use rand::seq::SliceRandom;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, PlanSchema, SortExpr};
use yachtsql_optimizer::SampleType;
use yachtsql_storage::{Field, FieldMode, Record, Schema, Table};

use super::{ConcurrentPlanExecutor, compare_values_for_sort};
use crate::executor::plan_schema_to_schema;
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl ConcurrentPlanExecutor<'_> {
    pub(crate) async fn execute_scan(
        &self,
        table_name: &str,
        planned_schema: &PlanSchema,
    ) -> Result<Table> {
        if let Some(cte_table) = self.cte_results.read().unwrap().get(table_name) {
            return Ok(self.apply_planned_schema(cte_table, planned_schema));
        }
        let table_name_upper = table_name.to_uppercase();
        if let Some(cte_table) = self.cte_results.read().unwrap().get(&table_name_upper) {
            return Ok(self.apply_planned_schema(cte_table, planned_schema));
        }
        let table_name_lower = table_name.to_lowercase();
        if let Some(cte_table) = self.cte_results.read().unwrap().get(&table_name_lower) {
            return Ok(self.apply_planned_schema(cte_table, planned_schema));
        }

        if let Some(table) = self.tables.get_table(table_name) {
            return Ok(self.apply_planned_schema(&table, planned_schema));
        }

        if let Some(handle) = self.catalog.get_table_handle(table_name) {
            let guard = handle.read();
            return Ok(self.apply_planned_schema(&guard, planned_schema));
        }

        Err(Error::TableNotFound(table_name.to_string()))
    }

    pub(crate) fn apply_planned_schema(
        &self,
        source_table: &Table,
        planned_schema: &PlanSchema,
    ) -> Table {
        if planned_schema.fields.is_empty() {
            return source_table.clone();
        }

        let mut new_schema = Schema::new();
        let mut column_indices = Vec::new();
        for plan_field in &planned_schema.fields {
            let mode = if plan_field.nullable {
                FieldMode::Nullable
            } else {
                FieldMode::Required
            };
            let mut field = Field::new(&plan_field.name, plan_field.data_type.clone(), mode);
            if let Some(ref table) = plan_field.table {
                field = field.with_source_table(table.clone());
            }
            let source_field_idx = source_table
                .schema()
                .fields()
                .iter()
                .position(|f| f.name.eq_ignore_ascii_case(&plan_field.name));
            if let Some(idx) = source_field_idx {
                if let Some(ref collation) = source_table.schema().fields()[idx].collation {
                    field.collation = Some(collation.clone());
                }
                column_indices.push(idx);
            }
            new_schema.add_field(field);
        }
        source_table.with_reordered_schema(new_schema, &column_indices)
    }

    pub(crate) async fn execute_filter(
        &self,
        input: &PhysicalPlan,
        predicate: &Expr,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input).await?;
        let schema = input_table.schema().clone();
        let has_subquery = Self::expr_contains_subquery(predicate);
        let mut result = Table::empty(schema.clone());

        if has_subquery {
            for record in input_table.rows()? {
                let val = self
                    .eval_expr_with_subqueries(predicate, &schema, &record)
                    .await?;
                if val.as_bool().unwrap_or(false) {
                    result.push_row(record.values().to_vec())?;
                }
            }
        } else {
            let vars = self.get_variables();
            let sys_vars = self.get_system_variables();
            let udf = self.get_user_functions();
            let evaluator = IrEvaluator::new(&schema)
                .with_variables(&vars)
                .with_system_variables(&sys_vars)
                .with_user_functions(&udf);

            for record in input_table.rows()? {
                let val = evaluator.evaluate(predicate, &record)?;
                if val.as_bool().unwrap_or(false) {
                    result.push_row(record.values().to_vec())?;
                }
            }
        }

        Ok(result)
    }

    pub(crate) async fn execute_project(
        &self,
        input: &PhysicalPlan,
        expressions: &[Expr],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input).await?;
        let input_schema = input_table.schema().clone();
        let result_schema = plan_schema_to_schema(schema);
        let has_subqueries = expressions.iter().any(Self::expr_contains_subquery);

        let mut result = Table::empty(result_schema);

        if has_subqueries {
            for record in input_table.rows()? {
                let mut new_row = Vec::with_capacity(expressions.len());
                for expr in expressions {
                    let val = self
                        .eval_expr_with_subqueries(expr, &input_schema, &record)
                        .await?;
                    new_row.push(val);
                }
                result.push_row(new_row)?;
            }
        } else {
            let vars = self.get_variables();
            let sys_vars = self.get_system_variables();
            let udf = self.get_user_functions();
            let evaluator = IrEvaluator::new(&input_schema)
                .with_variables(&vars)
                .with_system_variables(&sys_vars)
                .with_user_functions(&udf);

            for record in input_table.rows()? {
                let mut new_row = Vec::with_capacity(expressions.len());
                for expr in expressions {
                    let val = evaluator.evaluate(expr, &record)?;
                    new_row.push(val);
                }
                result.push_row(new_row)?;
            }
        }

        Ok(result)
    }

    pub(crate) async fn execute_sample(
        &self,
        input: &PhysicalPlan,
        sample_type: &SampleType,
        sample_value: i64,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input).await?;
        let schema = input_table.schema().clone();
        let rows = input_table.rows()?;
        let mut result = Table::empty(schema);

        match sample_type {
            SampleType::Rows => {
                let n = sample_value as usize;
                let mut rng = rand::thread_rng();
                let sampled: Vec<_> = rows.choose_multiple(&mut rng, n.min(rows.len())).collect();
                for record in sampled {
                    result.push_row(record.values().to_vec())?;
                }
            }
            SampleType::Percent => {
                let pct = sample_value as f64 / 100.0;
                let mut rng = rand::thread_rng();
                for record in rows {
                    if rng.r#gen::<f64>() < pct {
                        result.push_row(record.values().to_vec())?;
                    }
                }
            }
        }

        Ok(result)
    }

    pub(crate) async fn execute_sort(
        &self,
        input: &PhysicalPlan,
        sort_exprs: &[SortExpr],
    ) -> Result<Table> {
        let input_table = self.execute_plan(input).await?;
        let schema = input_table.schema().clone();
        let vars = self.get_variables();
        let sys_vars = self.get_system_variables();
        let udf = self.get_user_functions();
        let evaluator = IrEvaluator::new(&schema)
            .with_variables(&vars)
            .with_system_variables(&sys_vars)
            .with_user_functions(&udf);

        let mut rows: Vec<Record> = input_table.rows()?;

        rows.sort_by(|a, b| {
            for sort_expr in sort_exprs {
                let val_a = evaluator
                    .evaluate(&sort_expr.expr, a)
                    .unwrap_or(Value::Null);
                let val_b = evaluator
                    .evaluate(&sort_expr.expr, b)
                    .unwrap_or(Value::Null);

                let ordering = compare_values_for_sort(&val_a, &val_b);
                let ordering = if !sort_expr.asc {
                    ordering.reverse()
                } else {
                    ordering
                };

                match (val_a.is_null(), val_b.is_null()) {
                    (true, true) => {}
                    (true, false) => {
                        return if sort_expr.nulls_first {
                            std::cmp::Ordering::Less
                        } else {
                            std::cmp::Ordering::Greater
                        };
                    }
                    (false, true) => {
                        return if sort_expr.nulls_first {
                            std::cmp::Ordering::Greater
                        } else {
                            std::cmp::Ordering::Less
                        };
                    }
                    (false, false) => {}
                }

                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });

        let mut result = Table::empty(schema);
        for record in rows {
            result.push_row(record.values().to_vec())?;
        }

        Ok(result)
    }

    pub(crate) async fn execute_limit(
        &self,
        input: &PhysicalPlan,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input).await?;
        let schema = input_table.schema().clone();
        let mut result = Table::empty(schema);

        let offset = offset.unwrap_or(0);
        let limit = limit.unwrap_or(usize::MAX);

        for (i, record) in input_table.rows()?.into_iter().enumerate() {
            if i >= offset && i < offset + limit {
                result.push_row(record.values().to_vec())?;
            }
            if i >= offset + limit {
                break;
            }
        }

        Ok(result)
    }

    pub(crate) async fn execute_topn(
        &self,
        input: &PhysicalPlan,
        sort_exprs: &[SortExpr],
        limit: usize,
    ) -> Result<Table> {
        let sorted = self.execute_sort(input, sort_exprs).await?;
        let schema = sorted.schema().clone();
        let mut result = Table::empty(schema);

        for (i, record) in sorted.rows()?.into_iter().enumerate() {
            if i >= limit {
                break;
            }
            result.push_row(record.values().to_vec())?;
        }

        Ok(result)
    }

    pub(crate) async fn execute_distinct(&self, input: &PhysicalPlan) -> Result<Table> {
        let input_table = self.execute_plan(input).await?;
        let schema = input_table.schema().clone();
        let mut result = Table::empty(schema);
        let mut seen: HashSet<Vec<Value>> = HashSet::new();

        for record in input_table.rows()? {
            let values = record.values().to_vec();
            if seen.insert(values.clone()) {
                result.push_row(values)?;
            }
        }

        Ok(result)
    }

    pub(crate) async fn execute_aggregate(
        &self,
        input: &PhysicalPlan,
        group_by: &[Expr],
        aggregates: &[Expr],
        schema: &PlanSchema,
        grouping_sets: Option<&Vec<Vec<usize>>>,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input).await?;
        let vars = self.get_variables();
        let udf = self.get_user_functions();
        crate::executor::compute_aggregate(
            &input_table,
            group_by,
            aggregates,
            schema,
            grouping_sets,
            &vars,
            &udf,
        )
    }

    pub(crate) async fn execute_window(
        &self,
        input: &PhysicalPlan,
        window_exprs: &[Expr],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input).await?;
        let vars = self.get_variables();
        let udf = self.get_user_functions();
        crate::executor::compute_window(&input_table, window_exprs, schema, &vars, &udf)
    }

    pub(crate) async fn execute_values(
        &self,
        values: &[Vec<Expr>],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let result_schema = plan_schema_to_schema(schema);
        let empty_schema = Schema::new();
        let vars = self.get_variables();
        let sys_vars = self.get_system_variables();
        let udf = self.get_user_functions();
        let evaluator = IrEvaluator::new(&empty_schema)
            .with_variables(&vars)
            .with_system_variables(&sys_vars)
            .with_user_functions(&udf);
        let empty_record = Record::new();
        let mut result = Table::empty(result_schema);

        for row_exprs in values {
            let mut row = Vec::new();
            for expr in row_exprs {
                let val = evaluator.evaluate(expr, &empty_record)?;
                row.push(val);
            }
            result.push_row(row)?;
        }

        Ok(result)
    }
}
