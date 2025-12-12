use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use sqlparser::ast::Expr;
use yachtsql_common::error::{Error, Result};

use crate::table::Table;

#[derive(Debug, Clone, PartialEq)]
pub struct IndexMetadata {
    pub index_name: String,
    pub table_name: String,
    pub columns: Vec<IndexColumn>,
    pub index_type: IndexType,
    pub is_unique: bool,
    pub is_primary: bool,
    pub where_clause: Option<Expr>,
    pub options: HashMap<String, String>,
    pub tablespace: Option<String>,
}

impl IndexMetadata {
    pub fn new(index_name: String, table_name: String, columns: Vec<IndexColumn>) -> Self {
        Self {
            index_name,
            table_name,
            columns,
            index_type: IndexType::default(),
            is_unique: false,
            is_primary: false,
            where_clause: None,
            options: HashMap::new(),
            tablespace: None,
        }
    }

    pub fn with_unique(mut self, is_unique: bool) -> Self {
        self.is_unique = is_unique;
        self
    }

    pub fn with_index_type(mut self, index_type: IndexType) -> Self {
        self.index_type = index_type;
        self
    }

    pub fn with_where_clause(mut self, where_clause: Expr) -> Self {
        self.where_clause = Some(where_clause);
        self
    }

    pub fn validate(&self, table: &Table) -> Result<()> {
        self.validate_has_columns()?;
        self.validate_columns_exist(table)?;
        Ok(())
    }

    fn validate_has_columns(&self) -> Result<()> {
        if self.columns.is_empty() {
            return Err(Error::InvalidOperation(
                "Index must have at least one column".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_columns_exist(&self, table: &Table) -> Result<()> {
        for col in &self.columns {
            if let Some(column_name) = &col.column_name
                && table.schema().field(column_name).is_none()
            {
                return Err(Error::column_not_found(format!(
                    "{} in table {}",
                    column_name, self.table_name
                )));
            }
        }
        Ok(())
    }

    pub fn columns_display(&self) -> String {
        self.columns
            .iter()
            .map(IndexColumn::display_name)
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn is_single_column(&self) -> bool {
        self.columns.len() == 1
    }

    pub fn has_expressions(&self) -> bool {
        self.columns.iter().any(|col| col.expression.is_some())
    }

    pub fn provides_ordering(&self) -> bool {
        match self.index_type {
            IndexType::BTree => true,
            IndexType::Hash
            | IndexType::GiST
            | IndexType::GIN
            | IndexType::BRIN
            | IndexType::SPGiST
            | IndexType::IVFFlat
            | IndexType::HNSW => false,
        }
    }

    pub fn order_provided(&self) -> Option<Vec<(String, bool)>> {
        if !self.provides_ordering() {
            return None;
        }

        if self.has_expressions() {
            return None;
        }

        let ordering: Vec<(String, bool)> = self
            .columns
            .iter()
            .filter_map(|col| {
                col.column_name.as_ref().map(|name| {
                    let ascending = col.order == ColumnOrder::Asc;
                    (name.clone(), ascending)
                })
            })
            .collect();

        if ordering.is_empty() {
            None
        } else {
            Some(ordering)
        }
    }

    pub fn column_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .filter_map(|col| col.column_name.clone())
            .collect()
    }
}

impl fmt::Display for IndexMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "INDEX {} ON {} ({})",
            self.index_name,
            self.table_name,
            self.columns_display()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexColumn {
    pub column_name: Option<String>,
    pub expression: Option<Expr>,
    pub order: ColumnOrder,
    pub nulls_order: NullsOrder,
}

impl IndexColumn {
    pub fn simple(column_name: String) -> Self {
        Self {
            column_name: Some(column_name),
            expression: None,
            order: ColumnOrder::Asc,
            nulls_order: NullsOrder::Default,
        }
    }

    pub fn expression(expr: Expr) -> Self {
        Self {
            column_name: None,
            expression: Some(expr),
            order: ColumnOrder::Asc,
            nulls_order: NullsOrder::Default,
        }
    }

    fn display_name(&self) -> String {
        self.column_name
            .clone()
            .unwrap_or_else(|| "<expression>".to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[allow(clippy::upper_case_acronyms)]
pub enum IndexType {
    #[default]
    BTree,
    Hash,
    GiST,
    GIN,
    BRIN,
    SPGiST,
    IVFFlat,
    HNSW,
}

impl fmt::Display for IndexType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            IndexType::BTree => "BTREE",
            IndexType::Hash => "HASH",
            IndexType::GiST => "GIST",
            IndexType::GIN => "GIN",
            IndexType::BRIN => "BRIN",
            IndexType::SPGiST => "SPGIST",
            IndexType::IVFFlat => "IVFFLAT",
            IndexType::HNSW => "HNSW",
        };
        write!(f, "{}", s)
    }
}

impl IndexType {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "BTREE" => Some(IndexType::BTree),
            "HASH" => Some(IndexType::Hash),
            "GIST" => Some(IndexType::GiST),
            "GIN" => Some(IndexType::GIN),
            "BRIN" => Some(IndexType::BRIN),
            "SPGIST" => Some(IndexType::SPGiST),
            "IVFFLAT" => Some(IndexType::IVFFlat),
            "HNSW" => Some(IndexType::HNSW),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnOrder {
    Asc,
    Desc,
}

impl fmt::Display for ColumnOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ColumnOrder::Asc => "ASC",
            ColumnOrder::Desc => "DESC",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NullsOrder {
    First,
    Last,
    Default,
}

impl fmt::Display for NullsOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            NullsOrder::First => "NULLS FIRST",
            NullsOrder::Last => "NULLS LAST",
            NullsOrder::Default => "",
        };
        write!(f, "{}", s)
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::DataType;

    use super::*;

    #[test]
    fn test_index_metadata_creation() {
        let id_col = String::from("id");
        let columns = vec![IndexColumn::simple(id_col)];
        let idx_name = String::from("idx_test");
        let tbl_name = String::from("test");
        let metadata = IndexMetadata::new(idx_name.clone(), tbl_name.clone(), columns);

        assert_eq!(metadata.index_name, idx_name);
        assert_eq!(metadata.table_name, tbl_name);
        assert_eq!(metadata.columns.len(), 1);
        assert_eq!(metadata.index_type, IndexType::BTree);
        assert!(!metadata.is_unique);
    }

    #[test]
    fn test_index_type_from_str() {
        assert_eq!(IndexType::from_str("BTREE"), Some(IndexType::BTree));
        assert_eq!(IndexType::from_str("btree"), Some(IndexType::BTree));
        assert_eq!(IndexType::from_str("HASH"), Some(IndexType::Hash));
        assert_eq!(IndexType::from_str("GIST"), Some(IndexType::GiST));
        assert_eq!(IndexType::from_str("GIN"), Some(IndexType::GIN));
        assert_eq!(IndexType::from_str("BRIN"), Some(IndexType::BRIN));
        assert_eq!(IndexType::from_str("invalid"), None);
    }

    #[test]
    fn test_columns_display() {
        let col1 = String::from("col1");
        let col2 = String::from("col2");
        let columns = vec![IndexColumn::simple(col1), IndexColumn::simple(col2)];
        let test_name = String::from("test");
        let idx_name = String::from("idx_test");
        let metadata = IndexMetadata::new(idx_name, test_name, columns);

        let expected = String::from("col1, col2");
        assert_eq!(metadata.columns_display(), expected);
    }

    #[test]
    fn test_validate_empty_columns() {
        let idx_name = String::from("idx_test");
        let tbl_name = String::from("test");
        let metadata = IndexMetadata::new(idx_name, tbl_name, vec![]);

        let id_str = String::from("id");
        let schema =
            crate::Schema::from_fields(vec![crate::Field::required(id_str, DataType::Int64)]);
        let table = Table::new(schema);

        let result = metadata.validate(&table);
        assert!(result.is_err());
    }

    #[test]
    fn test_btree_provides_ordering() {
        let columns = vec![IndexColumn::simple("id".to_string())];
        let metadata = IndexMetadata::new("idx".to_string(), "t".to_string(), columns)
            .with_index_type(IndexType::BTree);

        assert!(metadata.provides_ordering());
        let order = metadata.order_provided().unwrap();
        assert_eq!(order.len(), 1);
        assert_eq!(order[0].0, "id");
        assert!(order[0].1);
    }

    #[test]
    fn test_hash_does_not_provide_ordering() {
        let columns = vec![IndexColumn::simple("id".to_string())];
        let metadata = IndexMetadata::new("idx".to_string(), "t".to_string(), columns)
            .with_index_type(IndexType::Hash);

        assert!(!metadata.provides_ordering());
        assert!(metadata.order_provided().is_none());
    }

    #[test]
    fn test_composite_btree_ordering() {
        let mut col_b = IndexColumn::simple("b".to_string());
        col_b.order = ColumnOrder::Desc;

        let columns = vec![IndexColumn::simple("a".to_string()), col_b];
        let metadata = IndexMetadata::new("idx".to_string(), "t".to_string(), columns)
            .with_index_type(IndexType::BTree);

        let order = metadata.order_provided().unwrap();
        assert_eq!(order.len(), 2);
        assert_eq!(order[0].0, "a");
        assert!(order[0].1);
        assert_eq!(order[1].0, "b");
        assert!(!order[1].1);
    }

    #[test]
    fn test_column_names() {
        let columns = vec![
            IndexColumn::simple("a".to_string()),
            IndexColumn::simple("b".to_string()),
        ];
        let metadata = IndexMetadata::new("idx".to_string(), "t".to_string(), columns);

        assert_eq!(metadata.column_names(), vec!["a", "b"]);
    }
}
