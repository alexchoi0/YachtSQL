use std::collections::{HashMap, HashSet};

use futures::future::{join, join_all};
use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::PlanSchema;
use yachtsql_storage::Table;

use super::ConcurrentPlanExecutor;
use crate::executor::plan_schema_to_schema;
use crate::plan::PhysicalPlan;

impl ConcurrentPlanExecutor<'_> {
    pub(crate) async fn execute_union(
        &self,
        inputs: &[PhysicalPlan],
        all: bool,
        schema: &PlanSchema,
        parallel: bool,
    ) -> Result<Table> {
        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);
        let mut seen: HashSet<Vec<Value>> = HashSet::new();

        let use_parallel = parallel && self.is_parallel_execution_enabled();
        let tables: Vec<Table> = if use_parallel && inputs.len() > 1 {
            let futures: Vec<_> = inputs
                .iter()
                .map(|input| self.execute_plan(input))
                .collect();
            let results = join_all(futures).await;
            results.into_iter().collect::<Result<Vec<_>>>()?
        } else {
            let mut tables = Vec::with_capacity(inputs.len());
            for input in inputs {
                tables.push(self.execute_plan(input).await?);
            }
            tables
        };

        for table in tables {
            for record in table.rows()? {
                let values = record.values().to_vec();
                if all || seen.insert(values.clone()) {
                    result.push_row(values)?;
                }
            }
        }

        Ok(result)
    }

    pub(crate) async fn execute_intersect(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        all: bool,
        schema: &PlanSchema,
        parallel: bool,
    ) -> Result<Table> {
        let use_parallel = parallel && self.is_parallel_execution_enabled();
        let (left_table, right_table) = if use_parallel {
            let (l, r) = join(self.execute_plan(left), self.execute_plan(right)).await;
            (l?, r?)
        } else {
            (
                self.execute_plan(left).await?,
                self.execute_plan(right).await?,
            )
        };
        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        let mut right_set: HashMap<Vec<Value>, usize> = HashMap::new();
        for record in right_table.rows()? {
            *right_set.entry(record.values().to_vec()).or_insert(0) += 1;
        }

        let mut seen: HashSet<Vec<Value>> = HashSet::new();
        for record in left_table.rows()? {
            let values = record.values().to_vec();
            if let Some(count) = right_set.get_mut(&values)
                && *count > 0
                && (all || seen.insert(values.clone()))
            {
                result.push_row(values)?;
                if all {
                    *count -= 1;
                }
            }
        }

        Ok(result)
    }

    pub(crate) async fn execute_except(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        all: bool,
        schema: &PlanSchema,
        parallel: bool,
    ) -> Result<Table> {
        let use_parallel = parallel && self.is_parallel_execution_enabled();
        let (left_table, right_table) = if use_parallel {
            let (l, r) = join(self.execute_plan(left), self.execute_plan(right)).await;
            (l?, r?)
        } else {
            (
                self.execute_plan(left).await?,
                self.execute_plan(right).await?,
            )
        };
        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        let mut right_set: HashMap<Vec<Value>, usize> = HashMap::new();
        for record in right_table.rows()? {
            *right_set.entry(record.values().to_vec()).or_insert(0) += 1;
        }

        let mut seen: HashSet<Vec<Value>> = HashSet::new();
        for record in left_table.rows()? {
            let values = record.values().to_vec();
            let in_right = right_set.get_mut(&values).map(|c| {
                if *c > 0 {
                    *c -= 1;
                    true
                } else {
                    false
                }
            });

            if !in_right.unwrap_or(false) && (all || seen.insert(values.clone())) {
                result.push_row(values)?;
            }
        }

        Ok(result)
    }
}
