use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexType {
    #[default]
    BTree,
    Hash,
    GiST,
    GIN,
    BRIN,
}

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub index_name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub index_type: IndexType,
    pub is_unique: bool,
}

impl IndexInfo {
    pub fn new(index_name: String, table_name: String, columns: Vec<String>) -> Self {
        Self {
            index_name,
            table_name,
            columns,
            index_type: IndexType::default(),
            is_unique: false,
        }
    }

    pub fn with_type(mut self, index_type: IndexType) -> Self {
        self.index_type = index_type;
        self
    }

    pub fn with_unique(mut self, is_unique: bool) -> Self {
        self.is_unique = is_unique;
        self
    }

    pub fn is_single_column(&self) -> bool {
        self.columns.len() == 1
    }

    pub fn supports_equality(&self) -> bool {
        matches!(self.index_type, IndexType::BTree | IndexType::Hash)
    }

    pub fn supports_range(&self) -> bool {
        matches!(self.index_type, IndexType::BTree)
    }
}

pub trait IndexCatalog: Send + Sync {
    fn get_indexes_for_table(&self, table_name: &str) -> Vec<IndexInfo>;

    fn get_index(&self, index_name: &str) -> Option<IndexInfo>;

    fn find_index_for_column(&self, table_name: &str, column_name: &str) -> Option<IndexInfo> {
        self.get_indexes_for_table(table_name)
            .into_iter()
            .find(|idx| idx.is_single_column() && idx.columns[0] == column_name)
    }

    fn find_covering_index(&self, table_name: &str, columns: &[String]) -> Option<IndexInfo> {
        if columns.is_empty() {
            return None;
        }

        self.get_indexes_for_table(table_name)
            .into_iter()
            .filter(|idx| columns.iter().all(|col| idx.columns.contains(col)))
            .min_by_key(|idx| idx.columns.len())
    }
}

#[derive(Default)]
pub struct EmptyCatalog;

impl IndexCatalog for EmptyCatalog {
    fn get_indexes_for_table(&self, _table_name: &str) -> Vec<IndexInfo> {
        Vec::new()
    }

    fn get_index(&self, _index_name: &str) -> Option<IndexInfo> {
        None
    }
}

pub type CatalogRef = Arc<dyn IndexCatalog>;

#[cfg(test)]
pub mod mock {
    use std::collections::HashMap;

    use super::*;

    pub struct MockCatalog {
        indexes: HashMap<String, IndexInfo>,
        table_indexes: HashMap<String, Vec<String>>,
    }

    impl MockCatalog {
        pub fn new() -> Self {
            Self {
                indexes: HashMap::new(),
                table_indexes: HashMap::new(),
            }
        }

        pub fn add_index(&mut self, info: IndexInfo) {
            let table_name = info.table_name.clone();
            let index_name = info.index_name.clone();

            self.indexes.insert(index_name.clone(), info);
            self.table_indexes
                .entry(table_name)
                .or_default()
                .push(index_name);
        }
    }

    impl Default for MockCatalog {
        fn default() -> Self {
            Self::new()
        }
    }

    impl IndexCatalog for MockCatalog {
        fn get_indexes_for_table(&self, table_name: &str) -> Vec<IndexInfo> {
            self.table_indexes
                .get(table_name)
                .map(|names| {
                    names
                        .iter()
                        .filter_map(|name| self.indexes.get(name).cloned())
                        .collect()
                })
                .unwrap_or_default()
        }

        fn get_index(&self, index_name: &str) -> Option<IndexInfo> {
            self.indexes.get(index_name).cloned()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_info_creation() {
        let info = IndexInfo::new(
            "idx_users_email".to_string(),
            "users".to_string(),
            vec!["email".to_string()],
        );

        assert_eq!(info.index_name, "idx_users_email");
        assert_eq!(info.table_name, "users");
        assert!(info.is_single_column());
        assert!(info.supports_equality());
        assert!(info.supports_range());
        assert!(!info.is_unique);
    }

    #[test]
    fn test_hash_index_no_range() {
        let info = IndexInfo::new(
            "idx_users_id".to_string(),
            "users".to_string(),
            vec!["id".to_string()],
        )
        .with_type(IndexType::Hash);

        assert!(info.supports_equality());
        assert!(!info.supports_range());
    }

    #[test]
    fn test_mock_catalog() {
        use mock::MockCatalog;

        let mut catalog = MockCatalog::new();
        catalog.add_index(IndexInfo::new(
            "idx_users_id".to_string(),
            "users".to_string(),
            vec!["id".to_string()],
        ));
        catalog.add_index(IndexInfo::new(
            "idx_users_email".to_string(),
            "users".to_string(),
            vec!["email".to_string()],
        ));

        let indexes = catalog.get_indexes_for_table("users");
        assert_eq!(indexes.len(), 2);

        let idx = catalog.find_index_for_column("users", "id");
        assert!(idx.is_some());
        assert_eq!(idx.unwrap().index_name, "idx_users_id");

        let idx = catalog.find_index_for_column("users", "name");
        assert!(idx.is_none());
    }

    #[test]
    fn test_empty_catalog() {
        let catalog = EmptyCatalog;
        assert!(catalog.get_indexes_for_table("users").is_empty());
        assert!(catalog.get_index("any").is_none());
    }

    #[test]
    fn test_find_covering_index() {
        use mock::MockCatalog;

        let mut catalog = MockCatalog::new();
        catalog.add_index(IndexInfo::new(
            "idx_composite".to_string(),
            "orders".to_string(),
            vec!["user_id".to_string(), "order_date".to_string()],
        ));
        catalog.add_index(IndexInfo::new(
            "idx_user_id".to_string(),
            "orders".to_string(),
            vec!["user_id".to_string()],
        ));

        let idx = catalog.find_covering_index("orders", &["user_id".to_string()]);
        assert!(idx.is_some());
        assert_eq!(idx.unwrap().index_name, "idx_user_id");

        let idx = catalog
            .find_covering_index("orders", &["user_id".to_string(), "order_date".to_string()]);
        assert!(idx.is_some());
        assert_eq!(idx.unwrap().index_name, "idx_composite");
    }
}
