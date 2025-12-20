use yachtsql_common::error::Result;
use yachtsql_ir::SortExpr;
use yachtsql_storage::Table;

use super::PlanExecutor;
use crate::plan::ExecutorPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_limit(
        &mut self,
        input: &ExecutorPlan,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();
        let mut result = Table::empty(schema);

        let offset = offset.unwrap_or(0);
        let limit = limit.unwrap_or(usize::MAX);

        for (i, record) in input_table.rows()?.into_iter().enumerate() {
            if i < offset {
                continue;
            }
            if i >= offset + limit {
                break;
            }
            result.push_row(record.values().to_vec())?;
        }

        Ok(result)
    }

    pub fn execute_topn(
        &mut self,
        input: &ExecutorPlan,
        sort_exprs: &[SortExpr],
        limit: usize,
    ) -> Result<Table> {
        let sorted = self.execute_sort(input, sort_exprs)?;
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
}
