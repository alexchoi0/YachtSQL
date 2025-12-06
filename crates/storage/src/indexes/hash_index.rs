use std::collections::HashMap;

use yachtsql_core::error::{Error, Result};

use super::{IndexKey, RangeQuery, TableIndex};

#[derive(Debug, Clone)]
pub struct HashIndex {
    map: HashMap<IndexKey, Vec<usize>>,

    column_indices: Vec<usize>,

    is_unique: bool,

    entry_count: usize,
}

impl HashIndex {
    pub fn new(column_indices: Vec<usize>, is_unique: bool) -> Self {
        Self {
            map: HashMap::new(),
            column_indices,
            is_unique,
            entry_count: 0,
        }
    }

    pub fn column_indices(&self) -> &[usize] {
        &self.column_indices
    }

    pub fn contains_key(&self, key: &IndexKey) -> bool {
        self.map.contains_key(key)
    }

    pub fn get(&self, key: &IndexKey) -> Option<&Vec<usize>> {
        self.map.get(key)
    }

    #[cfg(test)]
    pub fn inner_map(&self) -> &HashMap<IndexKey, Vec<usize>> {
        &self.map
    }
}

impl TableIndex for HashIndex {
    fn insert(&mut self, key: IndexKey, row_index: usize) -> Result<()> {
        if self.is_unique && !key.contains_null() && self.map.contains_key(&key) {
            return Err(Error::InvalidOperation(format!(
                "UNIQUE constraint violation: key {:?} already exists",
                key.values()
            )));
        }

        self.map.entry(key).or_default().push(row_index);
        self.entry_count += 1;

        Ok(())
    }

    fn delete(&mut self, key: &IndexKey, row_index: usize) -> Result<()> {
        if let Some(indices) = self.map.get_mut(key)
            && let Some(pos) = indices.iter().position(|&idx| idx == row_index)
        {
            indices.remove(pos);
            self.entry_count -= 1;

            if indices.is_empty() {
                self.map.remove(key);
            }

            return Ok(());
        }

        Ok(())
    }

    fn lookup_exact(&self, key: &IndexKey) -> Vec<usize> {
        self.map.get(key).cloned().unwrap_or_default()
    }

    fn lookup_range(&self, _range: &RangeQuery) -> Vec<usize> {
        Vec::new()
    }

    fn is_ordered(&self) -> bool {
        false
    }

    fn is_unique(&self) -> bool {
        self.is_unique
    }

    fn entry_count(&self) -> usize {
        self.entry_count
    }

    fn estimate_size_bytes(&self) -> usize {
        let avg_rows_per_key = if self.map.is_empty() {
            1
        } else {
            self.entry_count / self.map.len()
        };

        self.map.len() * (16 + 32 + 24 + avg_rows_per_key * 8)
    }

    fn clear(&mut self) {
        self.map.clear();
        self.entry_count = 0;
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::Value;

    use super::*;

    #[test]
    fn test_hash_index_new() {
        let index = HashIndex::new(vec![0], false);
        assert_eq!(index.entry_count(), 0);
        assert!(!index.is_unique());
        assert!(!index.is_ordered());
    }

    #[test]
    fn test_hash_index_insert_and_lookup() {
        let mut index = HashIndex::new(vec![0], false);

        let key1 = IndexKey::single(Value::int64(42));
        let key2 = IndexKey::single(Value::int64(100));

        index
            .insert(key1.clone(), 0)
            .expect("insert key1 should succeed");
        index
            .insert(key2.clone(), 1)
            .expect("insert key2 should succeed");

        assert_eq!(index.entry_count(), 2);
        assert_eq!(index.lookup_exact(&key1), vec![0]);
        assert_eq!(index.lookup_exact(&key2), vec![1]);
    }

    #[test]
    fn test_hash_index_non_unique_allows_duplicates() {
        let mut index = HashIndex::new(vec![0], false);

        let key = IndexKey::single(Value::string("test".to_string()));

        index
            .insert(key.clone(), 0)
            .expect("insert row 0 should succeed");
        index
            .insert(key.clone(), 1)
            .expect("insert row 1 should succeed");
        index
            .insert(key.clone(), 2)
            .expect("insert row 2 should succeed");

        assert_eq!(index.entry_count(), 3);

        let result = index.lookup_exact(&key);
        assert_eq!(result.len(), 3);
        assert!(result.contains(&0));
        assert!(result.contains(&1));
        assert!(result.contains(&2));
    }

    #[test]
    fn test_hash_index_unique_prevents_duplicates() {
        let mut index = HashIndex::new(vec![0], true);

        let key = IndexKey::single(Value::int64(42));

        assert!(index.insert(key.clone(), 0).is_ok());

        let result = index.insert(key.clone(), 1);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("UNIQUE constraint violation")
        );
    }

    #[test]
    fn test_hash_index_delete() {
        let mut index = HashIndex::new(vec![0], false);

        let key1 = IndexKey::single(Value::int64(42));
        let key2 = IndexKey::single(Value::int64(100));

        index
            .insert(key1.clone(), 0)
            .expect("insert key1 should succeed");
        index
            .insert(key2.clone(), 1)
            .expect("insert key2 should succeed");

        assert_eq!(index.entry_count(), 2);

        index.delete(&key1, 0).expect("delete key1 should succeed");
        assert_eq!(index.entry_count(), 1);
        assert!(index.lookup_exact(&key1).is_empty());
        assert_eq!(index.lookup_exact(&key2), vec![1]);
    }

    #[test]
    fn test_hash_index_delete_from_non_unique() {
        let mut index = HashIndex::new(vec![0], false);

        let key = IndexKey::single(Value::int64(42));

        index
            .insert(key.clone(), 0)
            .expect("insert row 0 should succeed");
        index
            .insert(key.clone(), 1)
            .expect("insert row 1 should succeed");
        index
            .insert(key.clone(), 2)
            .expect("insert row 2 should succeed");

        index.delete(&key, 1).expect("delete row 1 should succeed");

        let result = index.lookup_exact(&key);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&0));
        assert!(result.contains(&2));
        assert!(!result.contains(&1));
    }

    #[test]
    fn test_hash_index_delete_nonexistent() {
        let mut index = HashIndex::new(vec![0], false);

        let key = IndexKey::single(Value::int64(42));

        assert!(index.delete(&key, 0).is_ok());
    }

    #[test]
    fn test_hash_index_lookup_nonexistent() {
        let index = HashIndex::new(vec![0], false);

        let key = IndexKey::single(Value::int64(999));
        assert!(index.lookup_exact(&key).is_empty());
    }

    #[test]
    fn test_hash_index_no_range_support() {
        let mut index = HashIndex::new(vec![0], false);

        let key1 = IndexKey::single(Value::int64(10));
        let key2 = IndexKey::single(Value::int64(20));
        let key3 = IndexKey::single(Value::int64(30));

        index
            .insert(key1.clone(), 0)
            .expect("insert key1 should succeed");
        index.insert(key2, 1).expect("insert key2 should succeed");
        index
            .insert(key3.clone(), 2)
            .expect("insert key3 should succeed");

        let range = RangeQuery::between(key1, key3);
        assert!(index.lookup_range(&range).is_empty());
    }

    #[test]
    fn test_hash_index_composite_key() {
        let mut index = HashIndex::new(vec![0, 1], false);

        let key1 = IndexKey::new(vec![
            Value::string("John".to_string()),
            Value::string("Doe".to_string()),
        ]);
        let key2 = IndexKey::new(vec![
            Value::string("Jane".to_string()),
            Value::string("Smith".to_string()),
        ]);

        index
            .insert(key1.clone(), 0)
            .expect("insert key1 should succeed");
        index
            .insert(key2.clone(), 1)
            .expect("insert key2 should succeed");

        assert_eq!(index.lookup_exact(&key1), vec![0]);
        assert_eq!(index.lookup_exact(&key2), vec![1]);

        let key3 = IndexKey::new(vec![
            Value::string("John".to_string()),
            Value::string("Smith".to_string()),
        ]);
        assert!(index.lookup_exact(&key3).is_empty());
    }

    #[test]
    fn test_hash_index_null_handling() {
        let mut index = HashIndex::new(vec![0], false);

        let null_key = IndexKey::single(Value::null());
        let int_key = IndexKey::single(Value::int64(42));

        index
            .insert(null_key.clone(), 0)
            .expect("insert null key should succeed");
        index
            .insert(int_key.clone(), 1)
            .expect("insert int key should succeed");

        assert_eq!(index.lookup_exact(&null_key), vec![0]);
        assert_eq!(index.lookup_exact(&int_key), vec![1]);
    }

    #[test]
    fn test_hash_index_update() {
        let mut index = HashIndex::new(vec![0], false);

        let old_key = IndexKey::single(Value::int64(42));
        let new_key = IndexKey::single(Value::int64(100));

        index
            .insert(old_key.clone(), 0)
            .expect("insert should succeed");
        assert_eq!(index.lookup_exact(&old_key), vec![0]);

        index
            .update(&old_key, new_key.clone(), 0)
            .expect("update should succeed");

        assert!(index.lookup_exact(&old_key).is_empty());
        assert_eq!(index.lookup_exact(&new_key), vec![0]);
        assert_eq!(index.entry_count(), 1);
    }

    #[test]
    fn test_hash_index_contains_key() {
        let mut index = HashIndex::new(vec![0], false);

        let key = IndexKey::single(Value::int64(42));

        assert!(!index.contains_key(&key));

        index.insert(key.clone(), 0).expect("insert should succeed");
        assert!(index.contains_key(&key));

        index.delete(&key, 0).expect("delete should succeed");
        assert!(!index.contains_key(&key));
    }

    #[test]
    fn test_hash_index_estimate_size() {
        let mut index = HashIndex::new(vec![0], false);

        let initial_size = index.estimate_size_bytes();
        assert_eq!(initial_size, 0);

        for i in 0..100 {
            index
                .insert(IndexKey::single(Value::int64(i)), i as usize)
                .expect("insert should succeed");
        }

        let size_with_data = index.estimate_size_bytes();
        assert!(size_with_data > 0);

        assert!(size_with_data > 1000);
    }
}
