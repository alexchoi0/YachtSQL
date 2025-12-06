use std::collections::BTreeMap;

use yachtsql_core::error::{Error, Result};

use super::{IndexKey, RangeBound, RangeQuery, TableIndex};

#[derive(Debug, Clone)]
pub struct BPlusTreeIndex {
    tree: BTreeMap<IndexKey, Vec<usize>>,

    column_indices: Vec<usize>,

    is_unique: bool,

    entry_count: usize,
}

impl BPlusTreeIndex {
    pub fn new(column_indices: Vec<usize>, is_unique: bool) -> Self {
        Self {
            tree: BTreeMap::new(),
            column_indices,
            is_unique,
            entry_count: 0,
        }
    }

    pub fn column_indices(&self) -> &[usize] {
        &self.column_indices
    }

    pub fn contains_key(&self, key: &IndexKey) -> bool {
        self.tree.contains_key(key)
    }

    pub fn get(&self, key: &IndexKey) -> Option<&Vec<usize>> {
        self.tree.get(key)
    }

    pub fn estimated_height(&self) -> usize {
        if self.tree.is_empty() {
            0
        } else {
            let n = self.tree.len() as f64;
            (n.log2() / 6.64).ceil() as usize
        }
    }

    #[cfg(test)]
    pub fn keys_in_order(&self) -> Vec<IndexKey> {
        self.tree.keys().cloned().collect()
    }
}

impl TableIndex for BPlusTreeIndex {
    fn insert(&mut self, key: IndexKey, row_index: usize) -> Result<()> {
        if self.is_unique && !key.contains_null() && self.tree.contains_key(&key) {
            return Err(Error::InvalidOperation(format!(
                "UNIQUE constraint violation: key {:?} already exists",
                key.values()
            )));
        }

        self.tree.entry(key).or_default().push(row_index);
        self.entry_count += 1;

        Ok(())
    }

    fn delete(&mut self, key: &IndexKey, row_index: usize) -> Result<()> {
        if let Some(indices) = self.tree.get_mut(key)
            && let Some(pos) = indices.iter().position(|&idx| idx == row_index)
        {
            indices.remove(pos);
            self.entry_count -= 1;

            if indices.is_empty() {
                self.tree.remove(key);
            }

            return Ok(());
        }

        Ok(())
    }

    fn lookup_exact(&self, key: &IndexKey) -> Vec<usize> {
        self.tree.get(key).cloned().unwrap_or_default()
    }

    fn lookup_range(&self, range: &RangeQuery) -> Vec<usize> {
        let mut result = Vec::new();

        let iter: Box<dyn Iterator<Item = (&IndexKey, &Vec<usize>)>> =
            match (&range.lower, &range.upper) {
                (Some(lower), Some(upper)) => {
                    let start_key = lower.key();
                    let end_key = upper.key();

                    match (lower, upper) {
                        (RangeBound::Inclusive(_), RangeBound::Inclusive(_)) => {
                            Box::new(self.tree.range(start_key..=end_key))
                        }
                        (RangeBound::Inclusive(_), RangeBound::Exclusive(_)) => {
                            Box::new(self.tree.range(start_key..end_key))
                        }
                        (RangeBound::Exclusive(start), RangeBound::Inclusive(_)) => Box::new(
                            self.tree
                                .range(start_key..=end_key)
                                .filter(move |(k, _)| *k != start),
                        ),
                        (RangeBound::Exclusive(start), RangeBound::Exclusive(_)) => Box::new(
                            self.tree
                                .range(start_key..end_key)
                                .filter(move |(k, _)| *k != start),
                        ),
                    }
                }

                (Some(lower), None) => {
                    let start_key = lower.key();
                    match lower {
                        RangeBound::Inclusive(_) => Box::new(self.tree.range(start_key..)),
                        RangeBound::Exclusive(start) => Box::new(
                            self.tree
                                .range(start_key..)
                                .filter(move |(k, _)| *k != start),
                        ),
                    }
                }

                (None, Some(upper)) => {
                    let end_key = upper.key();
                    match upper {
                        RangeBound::Inclusive(_) => Box::new(self.tree.range(..=end_key)),
                        RangeBound::Exclusive(_) => Box::new(self.tree.range(..end_key)),
                    }
                }

                (None, None) => Box::new(self.tree.iter()),
            };

        for (_key, indices) in iter {
            result.extend_from_slice(indices);
        }

        result
    }

    fn is_ordered(&self) -> bool {
        true
    }

    fn is_unique(&self) -> bool {
        self.is_unique
    }

    fn entry_count(&self) -> usize {
        self.entry_count
    }

    fn estimate_size_bytes(&self) -> usize {
        let avg_rows_per_key = if self.tree.is_empty() {
            1
        } else {
            self.entry_count / self.tree.len()
        };

        self.tree.len() * (24 + 32 + 24 + avg_rows_per_key * 8)
    }

    fn clear(&mut self) {
        self.tree.clear();
        self.entry_count = 0;
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::Value;

    use super::*;

    #[test]
    fn test_btree_index_new() {
        let index = BPlusTreeIndex::new(vec![0], false);
        assert_eq!(index.entry_count(), 0);
        assert!(!index.is_unique());
        assert!(index.is_ordered());
    }

    #[test]
    fn test_btree_index_insert_and_lookup() {
        let mut index = BPlusTreeIndex::new(vec![0], false);

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
    fn test_btree_index_maintains_order() {
        let mut index = BPlusTreeIndex::new(vec![0], false);

        index
            .insert(IndexKey::single(Value::int64(50)), 2)
            .expect("insert 50 should succeed");
        index
            .insert(IndexKey::single(Value::int64(10)), 0)
            .expect("insert 10 should succeed");
        index
            .insert(IndexKey::single(Value::int64(30)), 1)
            .expect("insert 30 should succeed");
        index
            .insert(IndexKey::single(Value::int64(70)), 3)
            .expect("insert 70 should succeed");

        let keys = index.keys_in_order();
        assert_eq!(keys.len(), 4);
        assert_eq!(keys[0], IndexKey::single(Value::int64(10)));
        assert_eq!(keys[1], IndexKey::single(Value::int64(30)));
        assert_eq!(keys[2], IndexKey::single(Value::int64(50)));
        assert_eq!(keys[3], IndexKey::single(Value::int64(70)));
    }

    #[test]
    fn test_btree_index_range_query_between() {
        let mut index = BPlusTreeIndex::new(vec![0], false);

        for i in 0..10 {
            index
                .insert(IndexKey::single(Value::int64(i * 10)), i as usize)
                .expect("insert should succeed");
        }

        let range = RangeQuery::between(
            IndexKey::single(Value::int64(20)),
            IndexKey::single(Value::int64(60)),
        );

        let result = index.lookup_range(&range);

        assert_eq!(result.len(), 5);
        assert!(result.contains(&2));
        assert!(result.contains(&3));
        assert!(result.contains(&4));
        assert!(result.contains(&5));
        assert!(result.contains(&6));
    }

    #[test]
    fn test_btree_index_range_query_greater_than() {
        let mut index = BPlusTreeIndex::new(vec![0], false);

        for i in 0..5 {
            index
                .insert(IndexKey::single(Value::int64(i * 10)), i as usize)
                .expect("insert should succeed");
        }

        let range = RangeQuery::greater_than(IndexKey::single(Value::int64(20)));

        let result = index.lookup_range(&range);

        assert_eq!(result.len(), 2);
        assert!(result.contains(&3));
        assert!(result.contains(&4));
        assert!(!result.contains(&2));
    }

    #[test]
    fn test_btree_index_range_query_less_than_or_equal() {
        let mut index = BPlusTreeIndex::new(vec![0], false);

        for i in 0..5 {
            index
                .insert(IndexKey::single(Value::int64(i * 10)), i as usize)
                .expect("insert should succeed");
        }

        let range = RangeQuery::less_than_or_equal(IndexKey::single(Value::int64(20)));

        let result = index.lookup_range(&range);

        assert_eq!(result.len(), 3);
        assert!(result.contains(&0));
        assert!(result.contains(&1));
        assert!(result.contains(&2));
    }

    #[test]
    fn test_btree_index_non_unique_allows_duplicates() {
        let mut index = BPlusTreeIndex::new(vec![0], false);

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

        let result = index.lookup_exact(&key);
        assert_eq!(result.len(), 3);
        assert!(result.contains(&0));
        assert!(result.contains(&1));
        assert!(result.contains(&2));
    }

    #[test]
    fn test_btree_index_unique_prevents_duplicates() {
        let mut index = BPlusTreeIndex::new(vec![0], true);

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
    fn test_btree_index_delete() {
        let mut index = BPlusTreeIndex::new(vec![0], false);

        let key1 = IndexKey::single(Value::int64(10));
        let key2 = IndexKey::single(Value::int64(20));
        let key3 = IndexKey::single(Value::int64(30));

        index
            .insert(key1.clone(), 0)
            .expect("insert key1 should succeed");
        index
            .insert(key2.clone(), 1)
            .expect("insert key2 should succeed");
        index
            .insert(key3.clone(), 2)
            .expect("insert key3 should succeed");

        index.delete(&key2, 1).expect("delete key2 should succeed");

        assert_eq!(index.entry_count(), 2);
        assert_eq!(index.lookup_exact(&key1), vec![0]);
        assert!(index.lookup_exact(&key2).is_empty());
        assert_eq!(index.lookup_exact(&key3), vec![2]);
    }

    #[test]
    fn test_btree_index_delete_from_non_unique() {
        let mut index = BPlusTreeIndex::new(vec![0], false);

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
    }

    #[test]
    fn test_btree_index_update() {
        let mut index = BPlusTreeIndex::new(vec![0], false);

        let old_key = IndexKey::single(Value::int64(42));
        let new_key = IndexKey::single(Value::int64(100));

        index
            .insert(old_key.clone(), 0)
            .expect("insert should succeed");

        index
            .update(&old_key, new_key.clone(), 0)
            .expect("update should succeed");

        assert!(index.lookup_exact(&old_key).is_empty());
        assert_eq!(index.lookup_exact(&new_key), vec![0]);
        assert_eq!(index.entry_count(), 1);
    }

    #[test]
    fn test_btree_index_null_handling() {
        let mut index = BPlusTreeIndex::new(vec![0], false);

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

        let keys = index.keys_in_order();
        assert_eq!(keys[0], null_key);
        assert_eq!(keys[1], int_key);
    }

    #[test]
    fn test_btree_index_composite_key() {
        let mut index = BPlusTreeIndex::new(vec![0, 1], false);

        let key1 = IndexKey::new(vec![Value::int64(1), Value::string("A".to_string())]);
        let key2 = IndexKey::new(vec![Value::int64(1), Value::string("B".to_string())]);
        let key3 = IndexKey::new(vec![Value::int64(2), Value::string("A".to_string())]);

        index
            .insert(key1.clone(), 0)
            .expect("insert key1 should succeed");
        index
            .insert(key2.clone(), 1)
            .expect("insert key2 should succeed");
        index
            .insert(key3.clone(), 2)
            .expect("insert key3 should succeed");

        let keys = index.keys_in_order();
        assert_eq!(keys[0], key1);
        assert_eq!(keys[1], key2);
        assert_eq!(keys[2], key3);
    }

    #[test]
    fn test_btree_index_estimated_height() {
        let mut index = BPlusTreeIndex::new(vec![0], false);

        assert_eq!(index.estimated_height(), 0);

        for i in 0..1000 {
            index
                .insert(IndexKey::single(Value::int64(i)), i as usize)
                .expect("insert should succeed");
        }

        let height = index.estimated_height();

        assert!((2..=4).contains(&height));
    }

    #[test]
    fn test_btree_index_string_ordering() {
        let mut index = BPlusTreeIndex::new(vec![0], false);

        index
            .insert(IndexKey::single(Value::string("zebra".to_string())), 3)
            .expect("insert zebra should succeed");
        index
            .insert(IndexKey::single(Value::string("apple".to_string())), 0)
            .expect("insert apple should succeed");
        index
            .insert(IndexKey::single(Value::string("mango".to_string())), 1)
            .expect("insert mango should succeed");
        index
            .insert(IndexKey::single(Value::string("banana".to_string())), 2)
            .expect("insert banana should succeed");

        let keys = index.keys_in_order();
        assert_eq!(keys.len(), 4);
        assert_eq!(
            keys[0],
            IndexKey::single(Value::string("apple".to_string()))
        );
        assert_eq!(
            keys[1],
            IndexKey::single(Value::string("banana".to_string()))
        );
        assert_eq!(
            keys[2],
            IndexKey::single(Value::string("mango".to_string()))
        );
        assert_eq!(
            keys[3],
            IndexKey::single(Value::string("zebra".to_string()))
        );
    }

    #[test]
    fn test_btree_index_range_empty_result() {
        let mut index = BPlusTreeIndex::new(vec![0], false);

        for i in 0..5 {
            index
                .insert(IndexKey::single(Value::int64(i * 10)), i as usize)
                .expect("insert should succeed");
        }

        let range = RangeQuery::between(
            IndexKey::single(Value::int64(100)),
            IndexKey::single(Value::int64(200)),
        );

        let result = index.lookup_range(&range);
        assert!(result.is_empty());
    }
}
