use debug_print::debug_eprintln;
use yachtsql_common::error::Result;
use yachtsql_ir::{CteDefinition, LogicalPlan, SetOperationType};
use yachtsql_storage::Table;

use super::{PlanExecutor, plan_schema_to_schema};
use crate::plan::PhysicalPlan;

const MAX_RECURSION_DEPTH: usize = 500;

impl<'a> PlanExecutor<'a> {
    pub fn execute_cte(&mut self, ctes: &[CteDefinition], body: &PhysicalPlan) -> Result<Table> {
        for cte in ctes {
            if cte.recursive {
                self.execute_recursive_cte(cte)?;
            } else {
                let physical_cte = yachtsql_optimizer::optimize(&cte.query)?;
                let cte_result = self.execute(&physical_cte)?;
                self.cte_results.insert(cte.name.clone(), cte_result);
            }
        }

        let result = self.execute_plan(body)?;

        for cte in ctes {
            self.cte_results.remove(&cte.name);
        }

        Ok(result)
    }

    fn execute_recursive_cte(&mut self, cte: &CteDefinition) -> Result<()> {
        debug_eprintln!("[executor::cte] Executing recursive CTE: {}", cte.name);

        let (anchor_terms, recursive_terms) = split_recursive_cte(&cte.query, &cte.name);

        debug_eprintln!(
            "[executor::cte] Found {} anchor terms and {} recursive terms",
            anchor_terms.len(),
            recursive_terms.len()
        );

        let mut all_results = Vec::new();
        for anchor in &anchor_terms {
            let physical = yachtsql_optimizer::optimize(anchor)?;
            let result = self.execute(&physical)?;
            for row in result.rows()? {
                all_results.push(row.values().to_vec());
            }
        }

        let schema = plan_schema_to_schema(cte.query.schema());
        let mut accumulated = Table::from_values(schema.clone(), all_results.clone())?;
        self.cte_results
            .insert(cte.name.clone(), accumulated.clone());

        let mut working_set = accumulated.clone();
        let mut iteration = 0;

        while !working_set.is_empty() && iteration < MAX_RECURSION_DEPTH {
            iteration += 1;
            debug_eprintln!(
                "[executor::cte] Iteration {} with {} rows in working set",
                iteration,
                working_set.row_count()
            );

            self.cte_results.insert(cte.name.clone(), working_set);

            let mut new_rows = Vec::new();
            for recursive_term in &recursive_terms {
                let physical = yachtsql_optimizer::optimize(recursive_term)?;
                let result = self.execute(&physical)?;
                for row in result.rows()? {
                    new_rows.push(row.values().to_vec());
                }
            }

            if new_rows.is_empty() {
                debug_eprintln!("[executor::cte] No new rows, terminating recursion");
                break;
            }

            for row in &new_rows {
                all_results.push(row.clone());
            }

            working_set = Table::from_values(schema.clone(), new_rows)?;
            accumulated = Table::from_values(schema.clone(), all_results.clone())?;
        }

        self.cte_results.insert(cte.name.clone(), accumulated);

        debug_eprintln!(
            "[executor::cte] Recursive CTE {} completed after {} iterations with {} total rows",
            cte.name,
            iteration,
            all_results.len()
        );

        Ok(())
    }
}

fn split_recursive_cte(
    query: &LogicalPlan,
    cte_name: &str,
) -> (Vec<LogicalPlan>, Vec<LogicalPlan>) {
    let mut anchors = Vec::new();
    let mut recursives = Vec::new();

    collect_union_terms(query, cte_name, &mut anchors, &mut recursives);

    if anchors.is_empty() {
        anchors.push(query.clone());
    }

    (anchors, recursives)
}

fn collect_union_terms(
    plan: &LogicalPlan,
    cte_name: &str,
    anchors: &mut Vec<LogicalPlan>,
    recursives: &mut Vec<LogicalPlan>,
) {
    match plan {
        LogicalPlan::SetOperation {
            left,
            right,
            op: SetOperationType::Union,
            all: true,
            ..
        } => {
            collect_union_terms(left, cte_name, anchors, recursives);
            collect_union_terms(right, cte_name, anchors, recursives);
        }
        LogicalPlan::Scan { .. }
        | LogicalPlan::Filter { .. }
        | LogicalPlan::Project { .. }
        | LogicalPlan::Aggregate { .. }
        | LogicalPlan::Join { .. }
        | LogicalPlan::Sort { .. }
        | LogicalPlan::Limit { .. }
        | LogicalPlan::Distinct { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::SetOperation { .. }
        | LogicalPlan::Window { .. }
        | LogicalPlan::WithCte { .. }
        | LogicalPlan::Unnest { .. }
        | LogicalPlan::Qualify { .. }
        | LogicalPlan::Insert { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::Delete { .. }
        | LogicalPlan::Merge { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateView { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::CreateSchema { .. }
        | LogicalPlan::DropSchema { .. }
        | LogicalPlan::CreateFunction { .. }
        | LogicalPlan::DropFunction { .. }
        | LogicalPlan::Call { .. }
        | LogicalPlan::ExportData { .. }
        | LogicalPlan::Declare { .. }
        | LogicalPlan::SetVariable { .. }
        | LogicalPlan::If { .. }
        | LogicalPlan::While { .. }
        | LogicalPlan::Loop { .. }
        | LogicalPlan::For { .. }
        | LogicalPlan::Return { .. }
        | LogicalPlan::Raise { .. }
        | LogicalPlan::Break
        | LogicalPlan::Continue
        | LogicalPlan::AlterSchema { .. }
        | LogicalPlan::CreateProcedure { .. }
        | LogicalPlan::DropProcedure { .. }
        | LogicalPlan::LoadData { .. }
        | LogicalPlan::Repeat { .. }
        | LogicalPlan::CreateSnapshot { .. }
        | LogicalPlan::DropSnapshot { .. }
        | LogicalPlan::Sample { .. }
        | LogicalPlan::Assert { .. }
        | LogicalPlan::Grant { .. }
        | LogicalPlan::Revoke { .. } => {
            if references_table(plan, cte_name) {
                recursives.push(plan.clone());
            } else {
                anchors.push(plan.clone());
            }
        }
    }
}

fn references_table(plan: &LogicalPlan, table_name: &str) -> bool {
    match plan {
        LogicalPlan::Scan {
            table_name: name, ..
        } => name.eq_ignore_ascii_case(table_name),
        LogicalPlan::Filter { input, .. } => references_table(input, table_name),
        LogicalPlan::Project { input, .. } => references_table(input, table_name),
        LogicalPlan::Aggregate { input, .. } => references_table(input, table_name),
        LogicalPlan::Join { left, right, .. } => {
            references_table(left, table_name) || references_table(right, table_name)
        }
        LogicalPlan::Sort { input, .. } => references_table(input, table_name),
        LogicalPlan::Limit { input, .. } => references_table(input, table_name),
        LogicalPlan::Distinct { input, .. } => references_table(input, table_name),
        LogicalPlan::SetOperation { left, right, .. } => {
            references_table(left, table_name) || references_table(right, table_name)
        }
        LogicalPlan::Window { input, .. } => references_table(input, table_name),
        LogicalPlan::WithCte { body, .. } => references_table(body, table_name),
        LogicalPlan::Unnest { input, .. } => references_table(input, table_name),
        LogicalPlan::Qualify { input, .. } => references_table(input, table_name),
        LogicalPlan::Values { .. } => false,
        LogicalPlan::Empty { .. } => false,
        LogicalPlan::Insert { source, .. } => references_table(source, table_name),
        LogicalPlan::Update { .. } => false,
        LogicalPlan::Delete { .. } => false,
        LogicalPlan::Merge { source, .. } => references_table(source, table_name),
        LogicalPlan::CreateTable { .. } => false,
        LogicalPlan::DropTable { .. } => false,
        LogicalPlan::AlterTable { .. } => false,
        LogicalPlan::Truncate { .. } => false,
        LogicalPlan::CreateView { .. } => false,
        LogicalPlan::DropView { .. } => false,
        LogicalPlan::CreateSchema { .. } => false,
        LogicalPlan::DropSchema { .. } => false,
        LogicalPlan::CreateFunction { .. } => false,
        LogicalPlan::DropFunction { .. } => false,
        LogicalPlan::Call { .. } => false,
        LogicalPlan::ExportData { query, .. } => references_table(query, table_name),
        LogicalPlan::Declare { .. } => false,
        LogicalPlan::SetVariable { .. } => false,
        LogicalPlan::If {
            then_branch,
            else_branch,
            ..
        } => {
            then_branch.iter().any(|p| references_table(p, table_name))
                || else_branch
                    .as_ref()
                    .is_some_and(|b| b.iter().any(|p| references_table(p, table_name)))
        }
        LogicalPlan::While { body, .. } => body.iter().any(|p| references_table(p, table_name)),
        LogicalPlan::Loop { body, .. } => body.iter().any(|p| references_table(p, table_name)),
        LogicalPlan::For { query, body, .. } => {
            references_table(query, table_name)
                || body.iter().any(|p| references_table(p, table_name))
        }
        LogicalPlan::Return { .. } => false,
        LogicalPlan::Raise { .. } => false,
        LogicalPlan::Break => false,
        LogicalPlan::Continue => false,
        LogicalPlan::AlterSchema { .. } => false,
        LogicalPlan::CreateProcedure { .. } => false,
        LogicalPlan::DropProcedure { .. } => false,
        LogicalPlan::LoadData { .. } => false,
        LogicalPlan::Repeat { body, .. } => body.iter().any(|p| references_table(p, table_name)),
        LogicalPlan::CreateSnapshot { .. } => false,
        LogicalPlan::DropSnapshot { .. } => false,
        LogicalPlan::Sample { input, .. } => references_table(input, table_name),
        LogicalPlan::Assert { .. } => false,
        LogicalPlan::Grant { .. } => false,
        LogicalPlan::Revoke { .. } => false,
    }
}
