use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionType {
    Date { column: String },
    TimestampTrunc { column: String, unit: String },
    RangeBucket { column: String, buckets: Vec<i64> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionSpec {
    pub partition_type: PartitionType,

    pub expression_sql: String,
}

impl PartitionSpec {
    pub fn new(partition_type: PartitionType, expression_sql: String) -> Self {
        Self {
            partition_type,
            expression_sql,
        }
    }

    pub fn column_name(&self) -> &str {
        match &self.partition_type {
            PartitionType::Date { column } => column,
            PartitionType::TimestampTrunc { column, .. } => column,
            PartitionType::RangeBucket { column, .. } => column,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TablePartitionStrategy {
    Range { columns: Vec<String> },
    List { columns: Vec<String> },
    Hash { columns: Vec<String> },
}

impl TablePartitionStrategy {
    pub fn columns(&self) -> &[String] {
        match self {
            TablePartitionStrategy::Range { columns } => columns,
            TablePartitionStrategy::List { columns } => columns,
            TablePartitionStrategy::Hash { columns } => columns,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TablePartitionBound {
    Range { from: Vec<String>, to: Vec<String> },
    List { values: Vec<String> },
    Hash { modulus: i64, remainder: i64 },
    Default,
}

impl TablePartitionBound {
    pub fn is_default(&self) -> bool {
        matches!(self, TablePartitionBound::Default)
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct TablePartitionInfo {
    pub parent_table: Option<String>,
    pub bound: Option<TablePartitionBound>,
    pub strategy: Option<TablePartitionStrategy>,
    pub child_partitions: Vec<String>,
    pub row_movement_enabled: bool,
}
