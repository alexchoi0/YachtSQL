use std::collections::{HashMap, HashSet};

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{CteDefinition, LogicalPlan, SetOperationType};
use yachtsql_storage::{Field, Schema, Table};

use super::ConcurrentPlanExecutor;
use crate::executor::plan_schema_to_schema;
use crate::plan::PhysicalPlan;

impl ConcurrentPlanExecutor {
    pub(crate) async fn execute_cte(
        &self,
        ctes: &[CteDefinition],
        body: &PhysicalPlan,
        parallel_ctes: &[usize],
    ) -> Result<Table> {
        if ctes.is_empty() {
            return self.execute_plan(body).await;
        }

        let cte_names: Vec<String> = ctes.iter().map(|c| c.name.to_uppercase()).collect();
        let deps = Self::build_cte_dependencies(ctes, &cte_names);
        let waves = Self::topological_waves(ctes.len(), &deps);

        for wave in waves {
            let wave_ctes: Vec<(usize, &CteDefinition)> =
                wave.iter().map(|&i| (i, &ctes[i])).collect();

            let has_recursive = wave_ctes.iter().any(|(_, c)| c.recursive);
            let can_parallel = !has_recursive
                && wave_ctes.len() > 1
                && wave_ctes.iter().any(|(i, _)| parallel_ctes.contains(i));

            if can_parallel {
                let handles: Vec<_> = wave_ctes
                    .iter()
                    .map(|(_, cte)| {
                        let executor = self.clone();
                        let query = cte.query.clone();
                        let name = cte.name.to_uppercase();
                        let columns = cte.columns.clone();
                        tokio::spawn(async move {
                            let physical_cte = yachtsql_optimizer::optimize(&query)?;
                            let cte_plan = PhysicalPlan::from_physical(&physical_cte);
                            let cte_result = executor.execute_plan(&cte_plan).await?;
                            Ok((name, cte_result, columns))
                        })
                    })
                    .collect();
                let results: Vec<
                    std::result::Result<Result<(String, Table, Option<Vec<String>>)>, _>,
                > = futures::future::join_all(handles).await;

                for result in results {
                    let (name, mut table, columns) =
                        result.map_err(|e| Error::Internal(e.to_string()))??;
                    if let Some(ref cols) = columns {
                        table = self.apply_cte_column_aliases(&table, cols)?;
                    }
                    self.cte_results.write().unwrap().insert(name, table);
                }
            } else {
                for (_, cte) in wave_ctes {
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
            }
        }
        self.execute_plan(body).await
    }

    fn build_cte_dependencies(
        ctes: &[CteDefinition],
        cte_names: &[String],
    ) -> HashMap<usize, HashSet<usize>> {
        let mut deps: HashMap<usize, HashSet<usize>> = HashMap::new();
        for (i, cte) in ctes.iter().enumerate() {
            let mut cte_deps = HashSet::new();
            for (j, name) in cte_names.iter().enumerate() {
                if i != j && Self::references_table(&cte.query, name) {
                    cte_deps.insert(j);
                }
            }
            deps.insert(i, cte_deps);
        }
        deps
    }

    fn topological_waves(n: usize, deps: &HashMap<usize, HashSet<usize>>) -> Vec<Vec<usize>> {
        let mut waves = Vec::new();
        let mut executed: HashSet<usize> = HashSet::new();
        let mut remaining: HashSet<usize> = (0..n).collect();

        while !remaining.is_empty() {
            let ready: Vec<usize> = remaining
                .iter()
                .filter(|&i| {
                    deps.get(i)
                        .map(|d| d.iter().all(|dep| executed.contains(dep)))
                        .unwrap_or(true)
                })
                .copied()
                .collect();

            if ready.is_empty() {
                let fallback: Vec<usize> = remaining.iter().copied().collect();
                waves.push(fallback.clone());
                for i in fallback {
                    executed.insert(i);
                    remaining.remove(&i);
                }
            } else {
                waves.push(ready.clone());
                for i in ready {
                    executed.insert(i);
                    remaining.remove(&i);
                }
            }
        }
        waves
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
