use std::collections::{HashMap, HashSet};

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::PlanSchema;
use yachtsql_storage::Table;

use super::{PlanExecutor, plan_schema_to_schema};
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_union(
        &mut self,
        inputs: &[PhysicalPlan],
        all: bool,
        schema: &PlanSchema,
    ) -> Result<Table> {
        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        if all {
            for input in inputs {
                let table = self.execute_plan(input)?;
                for record in table.rows()? {
                    result.push_row(record.values().to_vec())?;
                }
            }
        } else {
            let mut seen: HashSet<Vec<RowKey>> = HashSet::new();
            for input in inputs {
                let table = self.execute_plan(input)?;
                for record in table.rows()? {
                    let key = row_to_key(record.values());
                    if seen.insert(key) {
                        result.push_row(record.values().to_vec())?;
                    }
                }
            }
        }

        Ok(result)
    }

    pub fn execute_intersect(
        &mut self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        all: bool,
        schema: &PlanSchema,
    ) -> Result<Table> {
        let left_table = self.execute_plan(left)?;
        let right_table = self.execute_plan(right)?;

        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        if all {
            let mut right_counts: HashMap<Vec<RowKey>, usize> = HashMap::new();
            for record in right_table.rows()? {
                let key = row_to_key(record.values());
                *right_counts.entry(key).or_insert(0) += 1;
            }

            for record in left_table.rows()? {
                let key = row_to_key(record.values());
                if let Some(count) = right_counts.get_mut(&key)
                    && *count > 0
                {
                    result.push_row(record.values().to_vec())?;
                    *count -= 1;
                }
            }
        } else {
            let right_rows: HashSet<Vec<RowKey>> = right_table
                .rows()?
                .iter()
                .map(|r| row_to_key(r.values()))
                .collect();

            let mut seen: HashSet<Vec<RowKey>> = HashSet::new();
            for record in left_table.rows()? {
                let key = row_to_key(record.values());
                if right_rows.contains(&key) && seen.insert(key) {
                    result.push_row(record.values().to_vec())?;
                }
            }
        }

        Ok(result)
    }

    pub fn execute_except(
        &mut self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        all: bool,
        schema: &PlanSchema,
    ) -> Result<Table> {
        let left_table = self.execute_plan(left)?;
        let right_table = self.execute_plan(right)?;

        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        if all {
            let mut right_counts: HashMap<Vec<RowKey>, usize> = HashMap::new();
            for record in right_table.rows()? {
                let key = row_to_key(record.values());
                *right_counts.entry(key).or_insert(0) += 1;
            }

            for record in left_table.rows()? {
                let key = row_to_key(record.values());
                if let Some(count) = right_counts.get_mut(&key)
                    && *count > 0
                {
                    *count -= 1;
                    continue;
                }
                result.push_row(record.values().to_vec())?;
            }
        } else {
            let right_rows: HashSet<Vec<RowKey>> = right_table
                .rows()?
                .iter()
                .map(|r| row_to_key(r.values()))
                .collect();

            let mut seen: HashSet<Vec<RowKey>> = HashSet::new();
            for record in left_table.rows()? {
                let key = row_to_key(record.values());
                if !right_rows.contains(&key) && seen.insert(key) {
                    result.push_row(record.values().to_vec())?;
                }
            }
        }

        Ok(result)
    }
}

type RowKey = String;

fn row_to_key(values: &[Value]) -> Vec<RowKey> {
    values.iter().map(|v| format!("{:?}", v)).collect()
}
