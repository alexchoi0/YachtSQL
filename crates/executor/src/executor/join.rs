use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, JoinType, PlanSchema};
use yachtsql_storage::{Record, Schema, Table};

use super::{PlanExecutor, plan_schema_to_schema};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

#[derive(Clone, PartialEq)]
struct HashKey(Vec<Value>);

impl Eq for HashKey {}

impl Hash for HashKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for val in &self.0 {
            match val {
                Value::Null => 0u8.hash(state),
                Value::Bool(b) => {
                    1u8.hash(state);
                    b.hash(state);
                }
                Value::Int64(i) => {
                    2u8.hash(state);
                    i.hash(state);
                }
                Value::Float64(f) => {
                    3u8.hash(state);
                    f.to_bits().hash(state);
                }
                Value::String(s) => {
                    4u8.hash(state);
                    s.hash(state);
                }
                Value::Bytes(b) => {
                    5u8.hash(state);
                    b.hash(state);
                }
                Value::Date(d) => {
                    6u8.hash(state);
                    d.hash(state);
                }
                Value::DateTime(dt) => {
                    7u8.hash(state);
                    dt.hash(state);
                }
                Value::Time(t) => {
                    8u8.hash(state);
                    t.hash(state);
                }
                Value::Timestamp(ts) => {
                    9u8.hash(state);
                    ts.hash(state);
                }
                Value::Interval(interval) => {
                    10u8.hash(state);
                    interval.months.hash(state);
                    interval.days.hash(state);
                    interval.nanos.hash(state);
                }
                Value::Numeric(n) => {
                    11u8.hash(state);
                    n.to_string().hash(state);
                }
                Value::Array(arr) => {
                    12u8.hash(state);
                    arr.len().hash(state);
                }
                Value::Struct(fields) => {
                    13u8.hash(state);
                    fields.len().hash(state);
                }
                Value::Json(j) => {
                    14u8.hash(state);
                    j.to_string().hash(state);
                }
                Value::Geography(_) => {
                    15u8.hash(state);
                }
                Value::Range { .. } => {
                    16u8.hash(state);
                }
                Value::BigNumeric(n) => {
                    17u8.hash(state);
                    n.to_string().hash(state);
                }
                Value::Default => {
                    18u8.hash(state);
                }
            }
        }
    }
}

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

    pub fn execute_hash_join(
        &mut self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        join_type: &JoinType,
        left_keys: &[Expr],
        right_keys: &[Expr],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let left_table = self.execute_plan(left)?;
        let right_table = self.execute_plan(right)?;

        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        match join_type {
            JoinType::Inner => {
                self.hash_inner_join(
                    &left_table,
                    &right_table,
                    left_keys,
                    right_keys,
                    &mut result,
                )?;
            }
            _ => {
                panic!("HashJoin only supports Inner join type currently");
            }
        }

        Ok(result)
    }

    fn hash_inner_join(
        &self,
        left: &Table,
        right: &Table,
        left_keys: &[Expr],
        right_keys: &[Expr],
        result: &mut Table,
    ) -> Result<()> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        let left_evaluator = IrEvaluator::new(left_schema);
        let right_evaluator = IrEvaluator::new(right_schema);

        let right_rows = right.rows()?;
        let mut hash_table: HashMap<HashKey, Vec<&Record>> = HashMap::new();

        for right_record in &right_rows {
            let key_values: Vec<Value> = right_keys
                .iter()
                .map(|expr| right_evaluator.evaluate(expr, right_record))
                .collect::<Result<Vec<_>>>()?;

            let has_null = key_values.iter().any(|v| matches!(v, Value::Null));
            if has_null {
                continue;
            }

            let key = HashKey(key_values);
            hash_table.entry(key).or_default().push(right_record);
        }

        for left_record in left.rows()? {
            let key_values: Vec<Value> = left_keys
                .iter()
                .map(|expr| left_evaluator.evaluate(expr, &left_record))
                .collect::<Result<Vec<_>>>()?;

            let has_null = key_values.iter().any(|v| matches!(v, Value::Null));
            if has_null {
                continue;
            }

            let key = HashKey(key_values);

            if let Some(matching_rows) = hash_table.get(&key) {
                for right_record in matching_rows {
                    let combined_values = combine_records(&left_record, right_record);
                    result.push_row(combined_values)?;
                }
            }
        }

        Ok(())
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
