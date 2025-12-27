mod subquery;

use yachtsql_common::error::Result;
use yachtsql_ir::Expr;
use yachtsql_storage::Table;

use super::PlanExecutor;
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_filter(&mut self, input: &PhysicalPlan, predicate: &Expr) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();

        if Self::expr_contains_subquery(predicate) {
            self.execute_filter_with_subquery(&input_table, predicate)
        } else {
            let evaluator = IrEvaluator::new(&schema)
                .with_variables(&self.variables)
                .with_system_variables(self.session.system_variables())
                .with_user_functions(&self.user_function_defs);
            let mut result = Table::empty(schema.clone());

            for record in input_table.rows()? {
                let val = evaluator.evaluate(predicate, &record)?;
                if val.as_bool().unwrap_or(false) {
                    result.push_row(record.values().to_vec())?;
                }
            }

            Ok(result)
        }
    }

    pub fn expr_contains_subquery(expr: &Expr) -> bool {
        match expr {
            Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::Subquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::ArraySubquery(_) => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_contains_subquery(left) || Self::expr_contains_subquery(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::ScalarFunction { args, .. } => args.iter().any(Self::expr_contains_subquery),
            _ => false,
        }
    }
}
