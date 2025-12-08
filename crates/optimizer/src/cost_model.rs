use crate::optimizer::{LogicalPlan, PlanNode};
use crate::ordering::OrderingProperty;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cost {
    pub rows: usize,
    pub cpu: f64,
    pub io: f64,
    pub memory: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinStrategy {
    HashJoin,
    MergeJoin,
    NestedLoopJoin,
    IndexNestedLoopJoin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateStrategy {
    HashAggregate,
    SortAggregate,
}

impl Cost {
    pub fn new(rows: usize, cpu: f64, io: f64, memory: usize) -> Self {
        Self {
            rows,
            cpu,
            io,
            memory,
        }
    }

    pub fn zero() -> Self {
        Self {
            rows: 0,
            cpu: 0.0,
            io: 0.0,
            memory: 0,
        }
    }

    pub fn total(&self) -> f64 {
        const CPU_WEIGHT: f64 = 1.0;
        const IO_WEIGHT: f64 = 2.0;
        const MEMORY_WEIGHT: f64 = 0.001;

        self.cpu * CPU_WEIGHT + self.io * IO_WEIGHT + (self.memory as f64) * MEMORY_WEIGHT
    }

    pub fn add(&self, other: &Cost) -> Cost {
        Cost {
            rows: self.rows + other.rows,
            cpu: self.cpu + other.cpu,
            io: self.io + other.io,
            memory: self.memory + other.memory,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CostModel {
    table_stats: std::collections::HashMap<String, TableStats>,
}

#[derive(Debug, Clone, Copy)]
pub struct TableStats {
    pub row_count: usize,
    pub avg_row_size: usize,
    pub column_count: usize,
}

impl CostModel {
    pub fn new() -> Self {
        Self {
            table_stats: std::collections::HashMap::new(),
        }
    }

    pub fn with_table_stats(mut self, table_name: String, stats: TableStats) -> Self {
        self.table_stats.insert(table_name, stats);
        self
    }

    pub fn estimate_cost(&self, plan: &LogicalPlan) -> Cost {
        self.estimate_node_cost(plan.root())
    }

    fn estimate_node_cost(&self, node: &PlanNode) -> Cost {
        match node {
            PlanNode::Scan { table_name, .. } | PlanNode::IndexScan { table_name, .. } => {
                self.estimate_scan_cost(table_name)
            }
            PlanNode::Filter { input, .. } => {
                let input_cost = self.estimate_node_cost(input);
                self.estimate_filter_cost(&input_cost)
            }
            PlanNode::Projection {
                input, expressions, ..
            } => {
                let input_cost = self.estimate_node_cost(input);
                self.estimate_projection_cost(&input_cost, expressions.len())
            }
            PlanNode::Join { left, right, .. } => {
                let left_cost = self.estimate_node_cost(left);
                let right_cost = self.estimate_node_cost(right);
                self.estimate_join_cost(&left_cost, &right_cost)
            }
            PlanNode::LateralJoin { left, right, .. } => {
                let left_cost = self.estimate_node_cost(left);
                let right_cost = self.estimate_node_cost(right);
                self.estimate_join_cost(&left_cost, &right_cost)
            }
            PlanNode::Aggregate { input, .. } => {
                let input_cost = self.estimate_node_cost(input);
                self.estimate_aggregate_cost(&input_cost)
            }
            PlanNode::Sort { input, .. } => {
                let input_cost = self.estimate_node_cost(input);
                self.estimate_sort_cost(&input_cost)
            }
            PlanNode::Limit { input, limit, .. } => {
                let input_cost = self.estimate_node_cost(input);
                self.estimate_limit_cost(&input_cost, *limit)
            }
            PlanNode::Distinct { input } => {
                let input_cost = self.estimate_node_cost(input);
                self.estimate_distinct_cost(&input_cost)
            }
            PlanNode::SubqueryScan { subquery, .. } => self.estimate_node_cost(subquery),
            PlanNode::Union {
                left, right, all, ..
            } => {
                let left_cost = self.estimate_node_cost(left);
                let right_cost = self.estimate_node_cost(right);

                let total_rows = left_cost.rows + right_cost.rows;
                let dedup_cost = if *all { 0.0 } else { total_rows as f64 * 0.5 };
                Cost::new(
                    total_rows,
                    left_cost.cpu + right_cost.cpu + dedup_cost,
                    left_cost.io + right_cost.io,
                    left_cost.memory + right_cost.memory,
                )
            }
            PlanNode::Intersect { left, right, .. } => {
                let left_cost = self.estimate_node_cost(left);
                let right_cost = self.estimate_node_cost(right);

                let result_rows = std::cmp::min(left_cost.rows, right_cost.rows) / 2;
                Cost::new(
                    result_rows,
                    left_cost.cpu + right_cost.cpu + result_rows as f64 * 0.5,
                    left_cost.io + right_cost.io,
                    left_cost.memory + right_cost.memory,
                )
            }
            PlanNode::Except { left, right, .. } => {
                let left_cost = self.estimate_node_cost(left);
                let right_cost = self.estimate_node_cost(right);

                let result_rows = left_cost.rows / 2;
                Cost::new(
                    result_rows,
                    left_cost.cpu + right_cost.cpu + result_rows as f64 * 0.5,
                    left_cost.io + right_cost.io,
                    left_cost.memory + right_cost.memory,
                )
            }
            PlanNode::Cte {
                cte_plan, input, ..
            } => {
                let cte_cost = self.estimate_node_cost(cte_plan);
                let input_cost = self.estimate_node_cost(input);

                Cost::new(
                    input_cost.rows,
                    cte_cost.cpu + input_cost.cpu,
                    cte_cost.io + input_cost.io,
                    cte_cost.memory + input_cost.memory + (cte_cost.rows * 100),
                )
            }
            PlanNode::Update { table_name, .. } => {
                let scan_cost = self.estimate_scan_cost(table_name);
                Cost::new(
                    scan_cost.rows,
                    scan_cost.cpu + scan_cost.rows as f64 * 1.5,
                    scan_cost.io + scan_cost.rows as f64 * 200.0,
                    scan_cost.memory,
                )
            }
            PlanNode::Delete { table_name, .. } => {
                let scan_cost = self.estimate_scan_cost(table_name);
                Cost::new(
                    scan_cost.rows,
                    scan_cost.cpu + scan_cost.rows as f64 * 1.0,
                    scan_cost.io + scan_cost.rows as f64 * 100.0,
                    scan_cost.memory,
                )
            }
            PlanNode::Truncate { .. } => Cost::new(0, 10.0, 100.0, 0),
            PlanNode::Unnest { .. } => Cost::new(100, 50.0, 0.0, 1000),
            PlanNode::TableValuedFunction { .. } => Cost::new(100, 100.0, 0.0, 1000),
            PlanNode::Window {
                input,
                window_exprs,
            } => {
                let input_cost = self.estimate_node_cost(input);
                let sort_cost = input_cost.rows as f64 * (input_cost.rows as f64).log2();
                Cost::new(
                    input_cost.rows,
                    input_cost.cpu + sort_cost * window_exprs.len() as f64,
                    input_cost.io,
                    input_cost.memory + (input_cost.rows * 100),
                )
            }
            PlanNode::AlterTable { .. } => Cost::new(0, 10.0, 100.0, 0),
            PlanNode::EmptyRelation => Cost::new(1, 1.0, 0.0, 0),
            PlanNode::Values { rows } => Cost::new(rows.len(), 1.0, 0.0, rows.len() * 50),
            PlanNode::DistinctOn { input, .. } => {
                let input_cost = self.estimate_node_cost(input);
                self.estimate_distinct_cost(&input_cost)
            }
            PlanNode::InsertOnConflict { values, .. } => {
                Cost::new(values.len(), 10.0, 1.0, values.len() * 100)
            }
            PlanNode::Insert { values, source, .. } => {
                if let Some(rows) = values {
                    Cost::new(
                        rows.len(),
                        rows.len() as f64 * 5.0,
                        rows.len() as f64 * 100.0,
                        rows.len() * 100,
                    )
                } else if let Some(src) = source {
                    self.estimate_node_cost(src)
                } else {
                    Cost::zero()
                }
            }
            PlanNode::Merge { source, .. } => self.estimate_node_cost(source),
            PlanNode::ArrayJoin { input, .. } => {
                let input_cost = self.estimate_node_cost(input);
                Cost::new(
                    input_cost.rows * 10,
                    input_cost.cpu + input_cost.rows as f64 * 5.0,
                    input_cost.io,
                    input_cost.memory + (input_cost.rows * 100),
                )
            }
            PlanNode::TableSample { input, size, .. } => {
                let input_cost = self.estimate_node_cost(input);
                let sample_factor = match size {
                    yachtsql_ir::plan::SampleSize::Percent(pct) => *pct / 100.0,
                    yachtsql_ir::plan::SampleSize::Rows(n) => {
                        (*n as f64) / (input_cost.rows as f64)
                    }
                };
                Cost::new(
                    (input_cost.rows as f64 * sample_factor) as usize,
                    input_cost.cpu * sample_factor,
                    input_cost.io * sample_factor,
                    (input_cost.memory as f64 * sample_factor) as usize,
                )
            }
            PlanNode::Pivot { input, .. } => {
                let input_cost = self.estimate_node_cost(input);
                Cost::new(
                    input_cost.rows / 2,
                    input_cost.cpu + input_cost.rows as f64 * 2.0,
                    input_cost.io,
                    input_cost.memory + (input_cost.rows * 50),
                )
            }
            PlanNode::Unpivot {
                input,
                unpivot_columns,
                ..
            } => {
                let input_cost = self.estimate_node_cost(input);
                let expansion_factor = unpivot_columns.len();
                Cost::new(
                    input_cost.rows * expansion_factor,
                    input_cost.cpu + input_cost.rows as f64 * expansion_factor as f64,
                    input_cost.io,
                    input_cost.memory + (input_cost.rows * expansion_factor * 50),
                )
            }
        }
    }

    fn estimate_scan_cost(&self, table_name: &str) -> Cost {
        if let Some(stats) = self.table_stats.get(table_name) {
            Cost::new(
                stats.row_count,
                stats.row_count as f64 * 0.1,
                stats.row_count as f64 * stats.avg_row_size as f64,
                stats.row_count * stats.avg_row_size,
            )
        } else {
            Cost::new(1000, 100.0, 1000.0, 100_000)
        }
    }

    fn estimate_filter_cost(&self, input_cost: &Cost) -> Cost {
        let selectivity = 0.5;
        Cost::new(
            (input_cost.rows as f64 * selectivity) as usize,
            input_cost.cpu + input_cost.rows as f64 * 0.5,
            input_cost.io,
            input_cost.memory,
        )
    }

    fn estimate_projection_cost(&self, input_cost: &Cost, _expr_count: usize) -> Cost {
        Cost::new(
            input_cost.rows,
            input_cost.cpu + input_cost.rows as f64 * 0.2,
            input_cost.io,
            input_cost.memory,
        )
    }

    fn estimate_join_cost(&self, left_cost: &Cost, right_cost: &Cost) -> Cost {
        let output_rows = left_cost.rows * right_cost.rows / 10;
        Cost::new(
            output_rows,
            left_cost.cpu + right_cost.cpu + (left_cost.rows * right_cost.rows) as f64 * 0.1,
            left_cost.io + right_cost.io,
            left_cost.memory + right_cost.memory + output_rows * 100,
        )
    }

    fn estimate_aggregate_cost(&self, input_cost: &Cost) -> Cost {
        let output_rows = input_cost.rows / 10;
        Cost::new(
            output_rows,
            input_cost.cpu + input_cost.rows as f64 * 1.0,
            input_cost.io,
            input_cost.memory + output_rows * 50,
        )
    }

    fn estimate_sort_cost(&self, input_cost: &Cost) -> Cost {
        let sort_cost = (input_cost.rows as f64 * (input_cost.rows as f64).log2()).max(0.0);
        Cost::new(
            input_cost.rows,
            input_cost.cpu + sort_cost,
            input_cost.io,
            input_cost.memory + input_cost.rows * 100,
        )
    }

    fn estimate_limit_cost(&self, input_cost: &Cost, limit: usize) -> Cost {
        Cost::new(
            limit.min(input_cost.rows),
            input_cost.cpu * 0.1,
            input_cost.io * 0.1,
            limit * 100,
        )
    }

    fn estimate_distinct_cost(&self, input_cost: &Cost) -> Cost {
        let unique_ratio = 0.5;
        let output_rows = (input_cost.rows as f64 * unique_ratio) as usize;

        let hash_cost = input_cost.rows as f64 * 1.0;
        let memory_for_hash = input_cost.rows * 50;

        Cost::new(
            output_rows,
            input_cost.cpu + hash_cost,
            input_cost.io,
            input_cost.memory + memory_for_hash,
        )
    }

    pub fn estimate_sort_cost_with_ordering(
        &self,
        input_cost: &Cost,
        input_ordering: &OrderingProperty,
        required_ordering: &OrderingProperty,
    ) -> Cost {
        if input_ordering.satisfies(required_ordering) {
            Cost::new(
                input_cost.rows,
                input_cost.cpu,
                input_cost.io,
                input_cost.memory,
            )
        } else {
            self.estimate_sort_cost(input_cost)
        }
    }

    pub fn estimate_join_cost_with_strategy(
        &self,
        left_cost: &Cost,
        right_cost: &Cost,
        strategy: JoinStrategy,
    ) -> Cost {
        let output_rows = left_cost.rows * right_cost.rows / 10;
        let base_memory = left_cost.memory + right_cost.memory;

        match strategy {
            JoinStrategy::HashJoin => {
                let build_cost = right_cost.rows as f64 * 1.5;
                let probe_cost = left_cost.rows as f64 * 0.5;
                let hash_memory = right_cost.rows * 100;

                Cost::new(
                    output_rows,
                    left_cost.cpu + right_cost.cpu + build_cost + probe_cost,
                    left_cost.io + right_cost.io,
                    base_memory + hash_memory,
                )
            }
            JoinStrategy::MergeJoin => {
                let merge_cost = (left_cost.rows + right_cost.rows) as f64 * 0.3;

                Cost::new(
                    output_rows,
                    left_cost.cpu + right_cost.cpu + merge_cost,
                    left_cost.io + right_cost.io,
                    base_memory,
                )
            }
            JoinStrategy::NestedLoopJoin => {
                let nested_cost = (left_cost.rows * right_cost.rows) as f64 * 0.2;

                Cost::new(
                    output_rows,
                    left_cost.cpu + right_cost.cpu + nested_cost,
                    left_cost.io + right_cost.io,
                    base_memory,
                )
            }
            JoinStrategy::IndexNestedLoopJoin => {
                let index_lookup_cost = left_cost.rows as f64 * (right_cost.rows as f64).log2();

                Cost::new(
                    output_rows,
                    left_cost.cpu + right_cost.cpu + index_lookup_cost,
                    left_cost.io + right_cost.io + left_cost.rows as f64 * 10.0,
                    base_memory,
                )
            }
        }
    }

    pub fn estimate_aggregate_cost_with_strategy(
        &self,
        input_cost: &Cost,
        strategy: AggregateStrategy,
        num_groups: Option<usize>,
    ) -> Cost {
        let estimated_groups = num_groups.unwrap_or(input_cost.rows / 10);
        let output_rows = estimated_groups.max(1);

        match strategy {
            AggregateStrategy::HashAggregate => {
                let hash_cost = input_cost.rows as f64 * 1.2;
                let hash_memory = estimated_groups * 80;

                Cost::new(
                    output_rows,
                    input_cost.cpu + hash_cost,
                    input_cost.io,
                    input_cost.memory + hash_memory,
                )
            }
            AggregateStrategy::SortAggregate => {
                let agg_cost = input_cost.rows as f64 * 0.8;

                Cost::new(
                    output_rows,
                    input_cost.cpu + agg_cost,
                    input_cost.io,
                    input_cost.memory + output_rows * 50,
                )
            }
        }
    }

    pub fn choose_join_strategy(
        &self,
        left_cost: &Cost,
        right_cost: &Cost,
        left_ordering: &OrderingProperty,
        right_ordering: &OrderingProperty,
        join_keys: &[(String, String)],
        has_index_on_right: bool,
    ) -> JoinStrategy {
        if join_keys.is_empty() {
            return JoinStrategy::NestedLoopJoin;
        }

        let left_cols: Vec<String> = join_keys.iter().map(|(l, _)| l.clone()).collect();
        let right_cols: Vec<String> = join_keys.iter().map(|(_, r)| r.clone()).collect();
        let join_ordering = OrderingProperty::from_column_names(&left_cols);
        let right_join_ordering = OrderingProperty::from_column_names(&right_cols);

        let both_sorted = left_ordering.satisfies(&join_ordering)
            && right_ordering.satisfies(&right_join_ordering);

        if both_sorted {
            return JoinStrategy::MergeJoin;
        }

        if has_index_on_right && left_cost.rows < right_cost.rows / 10 {
            return JoinStrategy::IndexNestedLoopJoin;
        }

        JoinStrategy::HashJoin
    }

    pub fn choose_aggregate_strategy(
        &self,
        input_cost: &Cost,
        input_ordering: &OrderingProperty,
        group_by_columns: &[String],
    ) -> AggregateStrategy {
        if group_by_columns.is_empty() {
            return AggregateStrategy::HashAggregate;
        }

        let required_ordering = OrderingProperty::from_column_names(group_by_columns);

        if input_ordering.satisfies(&required_ordering) {
            return AggregateStrategy::SortAggregate;
        }

        let sort_cost = self.estimate_sort_cost(input_cost);
        let hash_agg_cost = self.estimate_aggregate_cost_with_strategy(
            input_cost,
            AggregateStrategy::HashAggregate,
            None,
        );
        let sort_then_agg_cost = self.estimate_aggregate_cost_with_strategy(
            &sort_cost,
            AggregateStrategy::SortAggregate,
            None,
        );

        if sort_then_agg_cost.total() < hash_agg_cost.total() {
            AggregateStrategy::SortAggregate
        } else {
            AggregateStrategy::HashAggregate
        }
    }

    pub fn estimate_total_cost_with_ordering(
        &self,
        base_cost: &Cost,
        provided_ordering: &OrderingProperty,
        required_ordering: &OrderingProperty,
    ) -> Cost {
        if required_ordering.is_empty() || provided_ordering.satisfies(required_ordering) {
            *base_cost
        } else {
            self.estimate_sort_cost(base_cost)
        }
    }
}

impl Default for CostModel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_cost_when_already_sorted() {
        let model = CostModel::new();
        let input = Cost::new(1000, 100.0, 500.0, 10000);
        let cols = vec!["a".to_string()];
        let ordering = OrderingProperty::from_column_names(&cols);

        let cost = model.estimate_sort_cost_with_ordering(&input, &ordering, &ordering);

        assert_eq!(cost.rows, input.rows);
        assert_eq!(cost.cpu, input.cpu);
    }

    #[test]
    fn test_sort_cost_when_not_sorted() {
        let model = CostModel::new();
        let input = Cost::new(1000, 100.0, 500.0, 10000);
        let no_ordering = OrderingProperty::empty();
        let cols = vec!["a".to_string()];
        let required = OrderingProperty::from_column_names(&cols);

        let cost = model.estimate_sort_cost_with_ordering(&input, &no_ordering, &required);

        assert!(cost.cpu > input.cpu);
    }

    #[test]
    fn test_merge_join_cheaper_when_both_sorted() {
        let model = CostModel::new();
        let left = Cost::new(10000, 1000.0, 5000.0, 100000);
        let right = Cost::new(10000, 1000.0, 5000.0, 100000);

        let hash_cost =
            model.estimate_join_cost_with_strategy(&left, &right, JoinStrategy::HashJoin);
        let merge_cost =
            model.estimate_join_cost_with_strategy(&left, &right, JoinStrategy::MergeJoin);

        assert!(merge_cost.total() < hash_cost.total());
    }

    #[test]
    fn test_choose_merge_join_when_sorted() {
        let model = CostModel::new();
        let left = Cost::new(10000, 1000.0, 5000.0, 100000);
        let right = Cost::new(10000, 1000.0, 5000.0, 100000);
        let left_cols = vec!["id".to_string()];
        let right_cols = vec!["user_id".to_string()];
        let left_ordering = OrderingProperty::from_column_names(&left_cols);
        let right_ordering = OrderingProperty::from_column_names(&right_cols);
        let join_keys = vec![("id".to_string(), "user_id".to_string())];

        let strategy = model.choose_join_strategy(
            &left,
            &right,
            &left_ordering,
            &right_ordering,
            &join_keys,
            false,
        );

        assert_eq!(strategy, JoinStrategy::MergeJoin);
    }

    #[test]
    fn test_choose_hash_join_when_not_sorted() {
        let model = CostModel::new();
        let left = Cost::new(10000, 1000.0, 5000.0, 100000);
        let right = Cost::new(10000, 1000.0, 5000.0, 100000);
        let no_ordering = OrderingProperty::empty();
        let join_keys = vec![("id".to_string(), "user_id".to_string())];

        let strategy = model.choose_join_strategy(
            &left,
            &right,
            &no_ordering,
            &no_ordering,
            &join_keys,
            false,
        );

        assert_eq!(strategy, JoinStrategy::HashJoin);
    }

    #[test]
    fn test_sort_aggregate_when_presorted() {
        let model = CostModel::new();
        let input = Cost::new(10000, 1000.0, 5000.0, 100000);
        let cols = vec!["category".to_string()];
        let ordering = OrderingProperty::from_column_names(&cols);
        let group_by = vec!["category".to_string()];

        let strategy = model.choose_aggregate_strategy(&input, &ordering, &group_by);

        assert_eq!(strategy, AggregateStrategy::SortAggregate);
    }

    #[test]
    fn test_hash_aggregate_when_not_sorted() {
        let model = CostModel::new();
        let input = Cost::new(10000, 1000.0, 5000.0, 100000);
        let no_ordering = OrderingProperty::empty();
        let group_by = vec!["category".to_string()];

        let strategy = model.choose_aggregate_strategy(&input, &no_ordering, &group_by);

        assert_eq!(strategy, AggregateStrategy::HashAggregate);
    }
}
