use yachtsql_common::types::Value;

#[derive(Debug, Clone)]
pub struct TableStatistics {
    pub row_count: usize,
    pub total_bytes: usize,
    pub column_stats: Vec<ColumnStatistics>,
}

#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub null_count: usize,
    pub distinct_count: Option<usize>,
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
}
