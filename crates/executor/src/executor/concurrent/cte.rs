use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{CteDefinition, LogicalPlan, SetOperationType};
use yachtsql_storage::{Field, Schema, Table};

use super::ConcurrentPlanExecutor;
use crate::executor::plan_schema_to_schema;
use crate::plan::PhysicalPlan;

impl ConcurrentPlanExecutor<'_> {
    pub(crate) async fn execute_cte(
        &self,
        ctes: &[CteDefinition],
        body: &PhysicalPlan,
        _parallel_ctes: &[usize],
    ) -> Result<Table> {
        for cte in ctes {
            if cte.recursive {
                self.execute_recursive_cte(cte).await?;
            } else {
                let physical_cte = yachtsql_optimizer::optimize(&cte.query)?;
                let cte_plan = PhysicalPlan::from_physical(&physical_cte);
                let mut cte_result = self.execute_plan(&cte_plan).await?;

                if let Some(ref columns) = cte.columns {
                    cte_result = self.apply_cte_column_aliases(&cte_result, columns)?;
                }

                self.cte_results
                    .write()
                    .unwrap()
                    .insert(cte.name.to_uppercase(), cte_result);
            }
        }
        self.execute_plan(body).await
    }

    pub(crate) fn apply_cte_column_aliases(
        &self,
        table: &Table,
        columns: &[String],
    ) -> Result<Table> {
        let mut new_schema = Schema::new();
        for (i, alias) in columns.iter().enumerate() {
            if let Some(old_field) = table.schema().fields().get(i) {
                let mut new_field = Field::new(alias, old_field.data_type.clone(), old_field.mode);
                if let Some(ref src) = old_field.source_table {
                    new_field = new_field.with_source_table(src.clone());
                }
                new_schema.add_field(new_field);
            }
        }
        let rows: Vec<Vec<Value>> = table
            .rows()?
            .into_iter()
            .map(|r| r.values().to_vec())
            .collect();
        let mut result = Table::empty(new_schema);
        for row in rows {
            result.push_row(row)?;
        }
        Ok(result)
    }

    async fn execute_recursive_cte(&self, cte: &CteDefinition) -> Result<()> {
        const MAX_RECURSION_DEPTH: usize = 500;

        let (anchor_terms, recursive_terms) = Self::split_recursive_cte(&cte.query, &cte.name);

        let mut all_results = Vec::new();
        for anchor in &anchor_terms {
            let physical = yachtsql_optimizer::optimize(anchor)?;
            let anchor_plan = PhysicalPlan::from_physical(&physical);
            let result = self.execute_plan(&anchor_plan).await?;
            for row in result.rows()? {
                all_results.push(row.values().to_vec());
            }
        }

        let schema = plan_schema_to_schema(cte.query.schema());
        let mut accumulated = Table::from_values(schema.clone(), all_results.clone())?;

        if let Some(ref columns) = cte.columns {
            accumulated = self.apply_cte_column_aliases(&accumulated, columns)?;
        }

        self.cte_results
            .write()
            .unwrap()
            .insert(cte.name.to_uppercase(), accumulated.clone());

        let mut working_set = accumulated.clone();
        let mut iteration = 0;

        while !working_set.is_empty() && iteration < MAX_RECURSION_DEPTH {
            iteration += 1;

            self.cte_results
                .write()
                .unwrap()
                .insert(cte.name.to_uppercase(), working_set);

            let mut new_rows = Vec::new();
            for recursive_term in &recursive_terms {
                let physical = yachtsql_optimizer::optimize(recursive_term)?;
                let rec_plan = PhysicalPlan::from_physical(&physical);
                let result = self.execute_plan(&rec_plan).await?;
                for row in result.rows()? {
                    new_rows.push(row.values().to_vec());
                }
            }

            if new_rows.is_empty() {
                break;
            }

            for row in &new_rows {
                all_results.push(row.clone());
            }

            working_set = Table::from_values(schema.clone(), new_rows)?;
            if let Some(ref columns) = cte.columns {
                working_set = self.apply_cte_column_aliases(&working_set, columns)?;
            }
            accumulated = Table::from_values(schema.clone(), all_results.clone())?;
            if let Some(ref columns) = cte.columns {
                accumulated = self.apply_cte_column_aliases(&accumulated, columns)?;
            }
        }

        self.cte_results
            .write()
            .unwrap()
            .insert(cte.name.to_uppercase(), accumulated);
        Ok(())
    }

    fn split_recursive_cte(
        query: &LogicalPlan,
        cte_name: &str,
    ) -> (Vec<LogicalPlan>, Vec<LogicalPlan>) {
        let mut anchors = Vec::new();
        let mut recursives = Vec::new();

        Self::collect_union_terms(query, cte_name, &mut anchors, &mut recursives);

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
                Self::collect_union_terms(left, cte_name, anchors, recursives);
                Self::collect_union_terms(right, cte_name, anchors, recursives);
            }
            _ => {
                if Self::references_table(plan, cte_name) {
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
            LogicalPlan::Filter { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Project { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Aggregate { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Join { left, right, .. } => {
                Self::references_table(left, table_name)
                    || Self::references_table(right, table_name)
            }
            LogicalPlan::Sort { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Limit { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Distinct { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::SetOperation { left, right, .. } => {
                Self::references_table(left, table_name)
                    || Self::references_table(right, table_name)
            }
            LogicalPlan::Window { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::WithCte { body, .. } => Self::references_table(body, table_name),
            LogicalPlan::Unnest { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Qualify { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Sample { input, .. } => Self::references_table(input, table_name),
            _ => false,
        }
    }
}
