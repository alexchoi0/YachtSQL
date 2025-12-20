use yachtsql_common::error::Result;
use yachtsql_ir::CteDefinition;
use yachtsql_storage::Table;

use super::PlanExecutor;
use crate::plan::ExecutorPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_cte(&mut self, ctes: &[CteDefinition], body: &ExecutorPlan) -> Result<Table> {
        for cte in ctes {
            let physical_cte = yachtsql_optimizer::optimize(&cte.query)?;
            let cte_result = self.execute(&physical_cte)?;
            self.cte_results.insert(cte.name.clone(), cte_result);
        }

        let result = self.execute_plan(body)?;

        for cte in ctes {
            self.cte_results.remove(&cte.name);
        }

        Ok(result)
    }
}
