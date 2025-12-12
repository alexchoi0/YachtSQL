pub mod btree_index;
pub mod hash_index;
pub mod index_key;

pub use btree_index::BPlusTreeIndex;
pub use hash_index::HashIndex;
pub use index_key::{IndexKey, RangeBound, RangeQuery};
use yachtsql_common::error::Result;
use yachtsql_common::types::Value;

pub trait TableIndex: Send + Sync {
    fn insert(&mut self, key: IndexKey, row_index: usize) -> Result<()>;

    fn delete(&mut self, key: &IndexKey, row_index: usize) -> Result<()>;

    fn update(&mut self, old_key: &IndexKey, new_key: IndexKey, row_index: usize) -> Result<()> {
        self.delete(old_key, row_index)?;
        self.insert(new_key, row_index)
    }

    fn lookup_exact(&self, key: &IndexKey) -> Vec<usize>;

    fn lookup_range(&self, range: &RangeQuery) -> Vec<usize>;

    fn is_ordered(&self) -> bool;

    fn is_unique(&self) -> bool;

    fn entry_count(&self) -> usize;

    fn estimate_size_bytes(&self) -> usize;

    fn clear(&mut self);
}

pub fn extract_index_key(row_values: &[Value], column_indices: &[usize]) -> IndexKey {
    let key_values: Vec<Value> = column_indices
        .iter()
        .map(|&idx| row_values.get(idx).cloned().unwrap_or(Value::null()))
        .collect();

    IndexKey::new(key_values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_index_key_single_column() {
        let row = vec![
            Value::int64(1),
            Value::string("Alice".to_string()),
            Value::int64(30),
        ];
        let key = extract_index_key(&row, &[0]);

        assert_eq!(key.len(), 1);
        assert_eq!(key.values()[0], Value::int64(1));
    }

    #[test]
    fn test_extract_index_key_composite() {
        let row = vec![
            Value::int64(1),
            Value::string("Alice".to_string()),
            Value::int64(30),
        ];
        let key = extract_index_key(&row, &[0, 1]);

        assert_eq!(key.len(), 2);
        assert_eq!(key.values()[0], Value::int64(1));
        assert_eq!(key.values()[1], Value::string("Alice".to_string()));
    }

    #[test]
    fn test_extract_index_key_out_of_bounds() {
        let row = vec![Value::int64(1), Value::string("Alice".to_string())];
        let key = extract_index_key(&row, &[5]);

        assert_eq!(key.len(), 1);
        assert!(key.values()[0].is_null());
    }
}
