use std::collections::HashMap;

use yachtsql_ir::expr::{BinaryOp, Expr, LiteralValue};

#[derive(Debug, Clone)]
pub struct TableStatistics {
    pub table_name: String,

    pub row_count: usize,

    pub total_bytes: u64,

    pub column_stats: HashMap<String, ColumnStatistics>,
}

impl TableStatistics {
    pub fn new(table_name: String, row_count: usize) -> Self {
        Self {
            table_name,
            row_count,
            total_bytes: 0,
            column_stats: HashMap::new(),
        }
    }

    pub fn with_column_stats(mut self, column: String, stats: ColumnStatistics) -> Self {
        self.column_stats.insert(column, stats);
        self
    }

    pub fn get_column_stats(&self, column: &str) -> Option<&ColumnStatistics> {
        self.column_stats.get(column)
    }
}

#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub column_name: String,
    pub distinct_count: usize,
    pub null_count: usize,
    pub min_value: Option<LiteralValue>,
    pub max_value: Option<LiteralValue>,
    pub histogram: Option<Histogram>,
}

impl ColumnStatistics {
    pub fn new(column_name: String, distinct_count: usize, null_count: usize) -> Self {
        Self {
            column_name,
            distinct_count,
            null_count,
            min_value: None,
            max_value: None,
            histogram: None,
        }
    }

    pub fn with_min_max(mut self, min: LiteralValue, max: LiteralValue) -> Self {
        self.min_value = Some(min);
        self.max_value = Some(max);
        self
    }

    pub fn with_histogram(mut self, histogram: Histogram) -> Self {
        self.histogram = Some(histogram);
        self
    }

    pub fn estimate_selectivity(&self, op: &BinaryOp, value: &LiteralValue) -> f64 {
        if let Some(hist) = &self.histogram {
            return hist.estimate_selectivity(op, value);
        }

        match op {
            BinaryOp::Equal => {
                if self.distinct_count == 0 {
                    0.0
                } else {
                    1.0 / self.distinct_count as f64
                }
            }
            BinaryOp::NotEqual => {
                if self.distinct_count == 0 {
                    1.0
                } else {
                    1.0 - (1.0 / self.distinct_count as f64)
                }
            }
            BinaryOp::LessThan
            | BinaryOp::LessThanOrEqual
            | BinaryOp::GreaterThan
            | BinaryOp::GreaterThanOrEqual => 0.3,
            BinaryOp::And | BinaryOp::Or => 0.5,
            _ => 0.5,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Histogram {
    pub buckets: Vec<HistogramBucket>,
    pub total_count: usize,
}

impl Histogram {
    pub fn new(buckets: Vec<HistogramBucket>) -> Self {
        let total_count = buckets.iter().map(|b| b.count).sum();
        Self {
            buckets,
            total_count,
        }
    }

    pub fn equi_width(
        min: LiteralValue,
        max: LiteralValue,
        data: &[LiteralValue],
        num_buckets: usize,
    ) -> Self {
        let mut buckets = Vec::new();

        if let (LiteralValue::Int64(min_val), LiteralValue::Int64(max_val)) = (min, max) {
            let width = (max_val - min_val) as f64 / num_buckets as f64;

            for i in 0..num_buckets {
                let lower = min_val + (i as f64 * width) as i64;
                let upper = if i == num_buckets - 1 {
                    max_val
                } else {
                    min_val + ((i + 1) as f64 * width) as i64
                };

                let count = data
                    .iter()
                    .filter(|v| {
                        if let LiteralValue::Int64(val) = v {
                            *val >= lower && *val < upper
                        } else {
                            false
                        }
                    })
                    .count();

                let distinct = data
                    .iter()
                    .filter_map(|v| {
                        if let LiteralValue::Int64(val) = v {
                            if *val >= lower && *val < upper {
                                Some(*val)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect::<std::collections::HashSet<_>>()
                    .len();

                buckets.push(HistogramBucket {
                    lower_bound: LiteralValue::Int64(lower),
                    upper_bound: LiteralValue::Int64(upper),
                    count,
                    distinct_count: distinct,
                });
            }
        }

        Self::new(buckets)
    }

    pub fn estimate_selectivity(&self, op: &BinaryOp, value: &LiteralValue) -> f64 {
        if self.total_count == 0 {
            return 0.0;
        }

        match op {
            BinaryOp::Equal => self.estimate_equality(value),
            BinaryOp::NotEqual => 1.0 - self.estimate_equality(value),
            BinaryOp::LessThan => self.estimate_less_than(value),
            BinaryOp::LessThanOrEqual => self.estimate_less_than_or_equal(value),
            BinaryOp::GreaterThan => 1.0 - self.estimate_less_than_or_equal(value),
            BinaryOp::GreaterThanOrEqual => 1.0 - self.estimate_less_than(value),
            _ => 0.5,
        }
    }

    fn estimate_equality(&self, value: &LiteralValue) -> f64 {
        for bucket in &self.buckets {
            if bucket.contains(value) {
                if bucket.distinct_count == 0 {
                    return 0.0;
                }

                let bucket_selectivity = bucket.count as f64 / self.total_count as f64;
                return bucket_selectivity / bucket.distinct_count as f64;
            }
        }
        0.0
    }

    fn estimate_less_than(&self, value: &LiteralValue) -> f64 {
        let mut count = 0;

        for bucket in &self.buckets {
            if bucket.is_entirely_less_than(value) {
                count += bucket.count;
            } else if bucket.contains(value) {
                count += bucket.estimate_less_than(value);
            }
        }

        count as f64 / self.total_count as f64
    }

    fn estimate_less_than_or_equal(&self, value: &LiteralValue) -> f64 {
        let mut count = 0;

        for bucket in &self.buckets {
            if bucket.is_entirely_less_than_or_equal(value) {
                count += bucket.count;
            } else if bucket.contains(value) {
                count += bucket.estimate_less_than_or_equal(value);
            }
        }

        count as f64 / self.total_count as f64
    }
}

#[derive(Debug, Clone)]
pub struct HistogramBucket {
    pub lower_bound: LiteralValue,
    pub upper_bound: LiteralValue,
    pub count: usize,
    pub distinct_count: usize,
}

impl HistogramBucket {
    fn compare_literals(a: &LiteralValue, b: &LiteralValue) -> Option<std::cmp::Ordering> {
        match (a, b) {
            (LiteralValue::Int64(x), LiteralValue::Int64(y)) => Some(x.cmp(y)),
            (LiteralValue::Float64(x), LiteralValue::Float64(y)) => x.partial_cmp(y),
            (LiteralValue::Numeric(x), LiteralValue::Numeric(y)) => Some(x.cmp(y)),
            _ => None,
        }
    }

    pub fn contains(&self, value: &LiteralValue) -> bool {
        matches!(
            (
                Self::compare_literals(value, &self.lower_bound),
                Self::compare_literals(value, &self.upper_bound)
            ),
            (
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal),
                Some(std::cmp::Ordering::Less)
            )
        )
    }

    pub fn is_entirely_less_than(&self, value: &LiteralValue) -> bool {
        matches!(
            Self::compare_literals(&self.upper_bound, value),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        )
    }

    pub fn is_entirely_less_than_or_equal(&self, value: &LiteralValue) -> bool {
        matches!(
            Self::compare_literals(&self.upper_bound, value),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        )
    }

    pub fn estimate_less_than(&self, value: &LiteralValue) -> usize {
        if let (LiteralValue::Int64(lower), LiteralValue::Int64(upper), LiteralValue::Int64(val)) =
            (&self.lower_bound, &self.upper_bound, value)
        {
            if upper == lower {
                return 0;
            }
            let fraction = (*val - *lower) as f64 / (*upper - *lower) as f64;
            (self.count as f64 * fraction) as usize
        } else {
            self.count / 2
        }
    }

    pub fn estimate_less_than_or_equal(&self, value: &LiteralValue) -> usize {
        self.estimate_less_than(value)
    }
}

#[derive(Debug, Clone, Default)]
pub struct StatisticsRegistry {
    tables: HashMap<String, TableStatistics>,
}

impl StatisticsRegistry {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, stats: TableStatistics) {
        self.tables.insert(stats.table_name.clone(), stats);
    }

    pub fn add_table_statistics(&mut self, stats: TableStatistics) {
        self.add_table(stats);
    }

    pub fn get_table(&self, table_name: &str) -> Option<&TableStatistics> {
        self.tables.get(table_name)
    }

    pub fn get_table_statistics(&self, table_name: &str) -> Option<&TableStatistics> {
        self.get_table(table_name)
    }

    pub fn estimate_predicate_selectivity(&self, predicate: &Expr, table: &str) -> f64 {
        match predicate {
            Expr::BinaryOp {
                left,
                op,
                right: right_expr,
            } => {
                if let (Expr::Column { name, .. }, Expr::Literal(value)) =
                    (left.as_ref(), right_expr.as_ref())
                    && let Some(table_stats) = self.get_table(table)
                    && let Some(col_stats) = table_stats.get_column_stats(name)
                {
                    return col_stats.estimate_selectivity(op, value);
                }

                match op {
                    BinaryOp::Equal => 0.1,
                    BinaryOp::NotEqual => 0.9,
                    BinaryOp::LessThan
                    | BinaryOp::LessThanOrEqual
                    | BinaryOp::GreaterThan
                    | BinaryOp::GreaterThanOrEqual => 0.3,
                    BinaryOp::And => {
                        if let (Expr::BinaryOp { .. }, Expr::BinaryOp { .. }) =
                            (left.as_ref(), right_expr.as_ref())
                        {
                            let left_sel = self.estimate_predicate_selectivity(left, table);
                            let right_sel = self.estimate_predicate_selectivity(right_expr, table);
                            return left_sel * right_sel;
                        }
                        0.5
                    }
                    BinaryOp::Or => {
                        if let (Expr::BinaryOp { .. }, Expr::BinaryOp { .. }) =
                            (left.as_ref(), right_expr.as_ref())
                        {
                            let left_sel = self.estimate_predicate_selectivity(left, table);
                            let right_sel = self.estimate_predicate_selectivity(right_expr, table);
                            return left_sel + right_sel - (left_sel * right_sel);
                        }
                        0.5
                    }
                    _ => 0.5,
                }
            }
            Expr::UnaryOp { op, expr } => {
                use yachtsql_ir::expr::UnaryOp;
                match op {
                    UnaryOp::Not => 1.0 - self.estimate_predicate_selectivity(expr, table),
                    _ => 0.5,
                }
            }
            _ => 0.5,
        }
    }

    pub fn estimate_cardinality(&self, table: &str, predicates: &[Expr]) -> usize {
        if let Some(table_stats) = self.get_table(table) {
            let mut selectivity = 1.0;

            for pred in predicates {
                selectivity *= self.estimate_predicate_selectivity(pred, table);
            }

            (table_stats.row_count as f64 * selectivity) as usize
        } else {
            let default_selectivity = 0.1_f64.powi(predicates.len() as i32);
            (10000.0 * default_selectivity) as usize
        }
    }
}
