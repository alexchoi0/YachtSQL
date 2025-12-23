use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, JoinType, PlanSchema};
use yachtsql_storage::{Record, Schema, Table};

use super::{PlanExecutor, plan_schema_to_schema};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_nested_loop_join(
        &mut self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        join_type: &JoinType,
        condition: Option<&Expr>,
        schema: &PlanSchema,
    ) -> Result<Table> {
        let left_table = self.execute_plan(left)?;
        let right_table = self.execute_plan(right)?;

        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema.clone());

        let combined_schema = combine_schemas(left_table.schema(), right_table.schema());

        match join_type {
            JoinType::Inner => {
                self.inner_join(
                    &left_table,
                    &right_table,
                    condition,
                    &combined_schema,
                    &mut result,
                )?;
            }
            JoinType::Left => {
                self.left_join(
                    &left_table,
                    &right_table,
                    condition,
                    &combined_schema,
                    &mut result,
                )?;
            }
            JoinType::Right => {
                self.right_join(
                    &left_table,
                    &right_table,
                    condition,
                    &combined_schema,
                    &mut result,
                )?;
            }
            JoinType::Full => {
                self.full_join(
                    &left_table,
                    &right_table,
                    condition,
                    &combined_schema,
                    &mut result,
                )?;
            }
            JoinType::Cross => {
                self.cross_join_inner(&left_table, &right_table, &mut result)?;
            }
        }

        Ok(result)
    }

    pub fn execute_cross_join(
        &mut self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        schema: &PlanSchema,
    ) -> Result<Table> {
        let left_table = self.execute_plan(left)?;
        let right_table = self.execute_plan(right)?;

        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        self.cross_join_inner(&left_table, &right_table, &mut result)?;

        Ok(result)
    }

    fn inner_join(
        &self,
        left: &Table,
        right: &Table,
        condition: Option<&Expr>,
        combined_schema: &Schema,
        result: &mut Table,
    ) -> Result<()> {
        let evaluator = IrEvaluator::new(combined_schema);

        for left_record in left.rows()? {
            for right_record in right.rows()? {
                let combined_values = combine_records(&left_record, &right_record);
                let combined_record = Record::from_values(combined_values.clone());

                let matches = match condition {
                    Some(expr) => evaluator
                        .evaluate(expr, &combined_record)?
                        .as_bool()
                        .unwrap_or(false),
                    None => true,
                };

                if matches {
                    result.push_row(combined_values)?;
                }
            }
        }

        Ok(())
    }

    fn left_join(
        &self,
        left: &Table,
        right: &Table,
        condition: Option<&Expr>,
        combined_schema: &Schema,
        result: &mut Table,
    ) -> Result<()> {
        let evaluator = IrEvaluator::new(combined_schema);
        let right_null_row: Vec<Value> = (0..right.schema().field_count())
            .map(|_| Value::Null)
            .collect();

        for left_record in left.rows()? {
            let mut had_match = false;

            for right_record in right.rows()? {
                let combined_values = combine_records(&left_record, &right_record);
                let combined_record = Record::from_values(combined_values.clone());

                let matches = match condition {
                    Some(expr) => evaluator
                        .evaluate(expr, &combined_record)?
                        .as_bool()
                        .unwrap_or(false),
                    None => true,
                };

                if matches {
                    had_match = true;
                    result.push_row(combined_values)?;
                }
            }

            if !had_match {
                let mut row = left_record.values().to_vec();
                row.extend(right_null_row.clone());
                result.push_row(row)?;
            }
        }

        Ok(())
    }

    fn right_join(
        &self,
        left: &Table,
        right: &Table,
        condition: Option<&Expr>,
        combined_schema: &Schema,
        result: &mut Table,
    ) -> Result<()> {
        let evaluator = IrEvaluator::new(combined_schema);
        let left_null_row: Vec<Value> = (0..left.schema().field_count())
            .map(|_| Value::Null)
            .collect();

        for right_record in right.rows()? {
            let mut had_match = false;

            for left_record in left.rows()? {
                let combined_values = combine_records(&left_record, &right_record);
                let combined_record = Record::from_values(combined_values.clone());

                let matches = match condition {
                    Some(expr) => evaluator
                        .evaluate(expr, &combined_record)?
                        .as_bool()
                        .unwrap_or(false),
                    None => true,
                };

                if matches {
                    had_match = true;
                    result.push_row(combined_values)?;
                }
            }

            if !had_match {
                let mut row = left_null_row.clone();
                row.extend(right_record.values().to_vec());
                result.push_row(row)?;
            }
        }

        Ok(())
    }

    fn full_join(
        &self,
        left: &Table,
        right: &Table,
        condition: Option<&Expr>,
        combined_schema: &Schema,
        result: &mut Table,
    ) -> Result<()> {
        let evaluator = IrEvaluator::new(combined_schema);
        let left_null_row: Vec<Value> = (0..left.schema().field_count())
            .map(|_| Value::Null)
            .collect();
        let right_null_row: Vec<Value> = (0..right.schema().field_count())
            .map(|_| Value::Null)
            .collect();

        let left_rows = left.rows()?;
        let right_rows = right.rows()?;
        let mut right_matched: Vec<bool> = vec![false; right_rows.len()];

        for left_record in &left_rows {
            let mut had_match = false;

            for (right_idx, right_record) in right_rows.iter().enumerate() {
                let combined_values = combine_records(left_record, right_record);
                let combined_record = Record::from_values(combined_values.clone());

                let matches = match condition {
                    Some(expr) => evaluator
                        .evaluate(expr, &combined_record)?
                        .as_bool()
                        .unwrap_or(false),
                    None => true,
                };

                if matches {
                    had_match = true;
                    right_matched[right_idx] = true;
                    result.push_row(combined_values)?;
                }
            }

            if !had_match {
                let mut row = left_record.values().to_vec();
                row.extend(right_null_row.clone());
                result.push_row(row)?;
            }
        }

        for (right_idx, right_record) in right_rows.iter().enumerate() {
            if !right_matched[right_idx] {
                let mut row = left_null_row.clone();
                row.extend(right_record.values().to_vec());
                result.push_row(row)?;
            }
        }

        Ok(())
    }

    fn cross_join_inner(&self, left: &Table, right: &Table, result: &mut Table) -> Result<()> {
        for left_record in left.rows()? {
            for right_record in right.rows()? {
                let combined_values = combine_records(&left_record, &right_record);
                result.push_row(combined_values)?;
            }
        }
        Ok(())
    }
}

fn combine_schemas(left: &Schema, right: &Schema) -> Schema {
    let mut schema = Schema::new();
    for field in left.fields() {
        schema.add_field(field.clone());
    }
    for field in right.fields() {
        schema.add_field(field.clone());
    }
    schema
}

fn combine_records(left: &Record, right: &Record) -> Vec<Value> {
    let mut values = left.values().to_vec();
    values.extend(right.values().to_vec());
    values
}
