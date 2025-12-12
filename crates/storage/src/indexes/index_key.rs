use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use yachtsql_common::types::Value;

#[derive(Debug, Clone)]
pub struct IndexKey {
    values: Vec<Value>,
}

impl IndexKey {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn single(value: Value) -> Self {
        Self {
            values: vec![value],
        }
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn contains_null(&self) -> bool {
        self.values.iter().any(|v| v.is_null())
    }

    pub fn to_hash_string(&self) -> String {
        self.values
            .iter()
            .map(|v| format!("{:?}", v))
            .collect::<Vec<_>>()
            .join("|")
    }
}

impl PartialEq for IndexKey {
    fn eq(&self, other: &Self) -> bool {
        if self.values.len() != other.values.len() {
            return false;
        }

        self.values
            .iter()
            .zip(other.values.iter())
            .all(|(a, b)| a == b)
    }
}

impl Eq for IndexKey {}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexKey {
    fn cmp(&self, other: &Self) -> Ordering {
        for (a, b) in self.values.iter().zip(other.values.iter()) {
            match compare_values(a, b) {
                Ordering::Equal => continue,
                other => return other,
            }
        }

        self.values.len().cmp(&other.values.len())
    }
}

impl Hash for IndexKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_hash_string().hash(state);
    }
}

fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Less,
        (false, true) => return Ordering::Greater,
        (false, false) => {}
    }

    if let (Some(a_val), Some(b_val)) = (a.as_bool(), b.as_bool()) {
        return a_val.cmp(&b_val);
    }

    if let (Some(a_val), Some(b_val)) = (a.as_i64(), b.as_i64()) {
        return a_val.cmp(&b_val);
    }

    if let (Some(a_val), Some(b_val)) = (a.as_f64(), b.as_f64()) {
        return a_val.partial_cmp(&b_val).unwrap_or(Ordering::Equal);
    }

    if let (Some(a_val), Some(b_val)) = (a.as_str(), b.as_str()) {
        return a_val.cmp(b_val);
    }

    if let (Some(a_val), Some(b_val)) = (a.as_bytes(), b.as_bytes()) {
        return a_val.cmp(b_val);
    }

    if let (Some(a_val), Some(b_val)) = (a.as_date(), b.as_date()) {
        return a_val.cmp(&b_val);
    }

    if let (Some(a_val), Some(b_val)) = (a.as_datetime(), b.as_datetime()) {
        return a_val.cmp(&b_val);
    }

    if let (Some(a_val), Some(b_val)) = (a.as_time(), b.as_time()) {
        return a_val.cmp(&b_val);
    }

    if let (Some(a_val), Some(b_val)) = (a.as_timestamp(), b.as_timestamp()) {
        return a_val.cmp(&b_val);
    }

    if let (Some(a_val), Some(b_val)) = (a.as_uuid(), b.as_uuid()) {
        return a_val.cmp(b_val);
    }

    if let (Some(a_val), Some(b_val)) = (a.as_numeric(), b.as_numeric()) {
        return a_val.cmp(&b_val);
    }

    if let (Some(a_arr), Some(b_arr)) = (a.as_array(), b.as_array()) {
        for (a_elem, b_elem) in a_arr.iter().zip(b_arr.iter()) {
            match compare_values(a_elem, b_elem) {
                Ordering::Equal => continue,
                other => return other,
            }
        }
        return a_arr.len().cmp(&b_arr.len());
    }

    Ordering::Equal
}

#[derive(Debug, Clone)]
pub struct RangeQuery {
    pub lower: Option<RangeBound>,

    pub upper: Option<RangeBound>,
}

impl RangeQuery {
    pub fn new(lower: Option<RangeBound>, upper: Option<RangeBound>) -> Self {
        Self { lower, upper }
    }

    pub fn exact(key: IndexKey) -> Self {
        Self {
            lower: Some(RangeBound::Inclusive(key.clone())),
            upper: Some(RangeBound::Inclusive(key)),
        }
    }

    pub fn greater_than(key: IndexKey) -> Self {
        Self {
            lower: Some(RangeBound::Exclusive(key)),
            upper: None,
        }
    }

    pub fn greater_than_or_equal(key: IndexKey) -> Self {
        Self {
            lower: Some(RangeBound::Inclusive(key)),
            upper: None,
        }
    }

    pub fn less_than(key: IndexKey) -> Self {
        Self {
            lower: None,
            upper: Some(RangeBound::Exclusive(key)),
        }
    }

    pub fn less_than_or_equal(key: IndexKey) -> Self {
        Self {
            lower: None,
            upper: Some(RangeBound::Inclusive(key)),
        }
    }

    pub fn between(lower: IndexKey, upper: IndexKey) -> Self {
        Self {
            lower: Some(RangeBound::Inclusive(lower)),
            upper: Some(RangeBound::Inclusive(upper)),
        }
    }

    pub fn contains(&self, key: &IndexKey) -> bool {
        if let Some(lower) = &self.lower {
            let lower_satisfied = match lower {
                RangeBound::Inclusive(bound) => key >= bound,
                RangeBound::Exclusive(bound) => key > bound,
            };
            if !lower_satisfied {
                return false;
            }
        }

        if let Some(upper) = &self.upper {
            let upper_satisfied = match upper {
                RangeBound::Inclusive(bound) => key <= bound,
                RangeBound::Exclusive(bound) => key < bound,
            };
            if !upper_satisfied {
                return false;
            }
        }

        true
    }
}

#[derive(Debug, Clone)]
pub enum RangeBound {
    Inclusive(IndexKey),
    Exclusive(IndexKey),
}

impl RangeBound {
    pub fn key(&self) -> &IndexKey {
        match self {
            RangeBound::Inclusive(k) | RangeBound::Exclusive(k) => k,
        }
    }

    pub fn is_inclusive(&self) -> bool {
        matches!(self, RangeBound::Inclusive(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_key_equality() {
        let key1 = IndexKey::single(Value::int64(42));
        let key2 = IndexKey::single(Value::int64(42));
        let key3 = IndexKey::single(Value::int64(43));

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_index_key_ordering() {
        let key1 = IndexKey::single(Value::int64(10));
        let key2 = IndexKey::single(Value::int64(20));
        let key3 = IndexKey::single(Value::int64(15));

        assert!(key1 < key2);
        assert!(key3 > key1);
        assert!(key3 < key2);
    }

    #[test]
    fn test_composite_key_ordering() {
        let key1 = IndexKey::new(vec![Value::int64(1), Value::string("A".to_string())]);
        let key2 = IndexKey::new(vec![Value::int64(1), Value::string("B".to_string())]);
        let key3 = IndexKey::new(vec![Value::int64(2), Value::string("A".to_string())]);

        assert!(key1 < key2);
        assert!(key1 < key3);
        assert!(key2 < key3);
    }

    #[test]
    fn test_null_ordering() {
        let null_key = IndexKey::single(Value::null());
        let int_key = IndexKey::single(Value::int64(10));

        assert!(null_key < int_key);
        assert!(int_key >= null_key);
    }

    #[test]
    fn test_index_key_hashing() {
        use std::collections::HashSet;

        let key1 = IndexKey::single(Value::int64(42));
        let key2 = IndexKey::single(Value::int64(42));
        let key3 = IndexKey::single(Value::int64(43));

        let mut set = HashSet::new();
        set.insert(key1);
        assert!(set.contains(&key2));
        assert!(!set.contains(&key3));
    }

    #[test]
    fn test_range_query_exact() {
        let key = IndexKey::single(Value::int64(10));
        let range = RangeQuery::exact(key.clone());

        assert!(range.contains(&key));
        assert!(!range.contains(&IndexKey::single(Value::int64(9))));
        assert!(!range.contains(&IndexKey::single(Value::int64(11))));
    }

    #[test]
    fn test_range_query_greater_than() {
        let bound = IndexKey::single(Value::int64(10));
        let range = RangeQuery::greater_than(bound);

        assert!(range.contains(&IndexKey::single(Value::int64(11))));
        assert!(range.contains(&IndexKey::single(Value::int64(100))));
        assert!(!range.contains(&IndexKey::single(Value::int64(10))));
        assert!(!range.contains(&IndexKey::single(Value::int64(9))));
    }

    #[test]
    fn test_range_query_between() {
        let lower = IndexKey::single(Value::int64(10));
        let upper = IndexKey::single(Value::int64(20));
        let range = RangeQuery::between(lower, upper);

        assert!(range.contains(&IndexKey::single(Value::int64(10))));
        assert!(range.contains(&IndexKey::single(Value::int64(15))));
        assert!(range.contains(&IndexKey::single(Value::int64(20))));
        assert!(!range.contains(&IndexKey::single(Value::int64(9))));
        assert!(!range.contains(&IndexKey::single(Value::int64(21))));
    }
}
