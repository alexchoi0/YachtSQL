use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Multiset {
    elements: Vec<(Value, usize)>,
}

impl Multiset {
    pub fn new() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    pub fn from_vec(values: Vec<Value>) -> Self {
        let mut multiset = Self::new();
        for value in values {
            multiset.add(value);
        }
        multiset
    }

    pub fn add(&mut self, value: Value) {
        if let Some(pos) = self.elements.iter().position(|(v, _)| v == &value) {
            self.elements[pos].1 += 1;
        } else {
            self.elements.push((value, 1));
        }
    }

    pub fn add_with_multiplicity(&mut self, value: Value, count: usize) {
        if let Some(pos) = self.elements.iter().position(|(v, _)| v == &value) {
            self.elements[pos].1 += count;
        } else {
            self.elements.push((value, count));
        }
    }

    pub fn multiplicity(&self, value: &Value) -> usize {
        self.elements
            .iter()
            .find(|(v, _)| v == value)
            .map(|(_, count)| *count)
            .unwrap_or(0)
    }

    pub fn cardinality(&self) -> usize {
        self.elements.iter().map(|(_, count)| count).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    pub fn to_vec(&self) -> Vec<Value> {
        let mut result = Vec::new();
        for (value, count) in &self.elements {
            for _ in 0..*count {
                result.push(value.clone());
            }
        }
        result
    }

    pub fn to_set(&self) -> Vec<Value> {
        self.elements.iter().map(|(v, _)| v.clone()).collect()
    }

    pub fn union(&self, other: &Multiset) -> Multiset {
        let mut result = self.clone();
        for (value, count) in &other.elements {
            result.add_with_multiplicity(value.clone(), *count);
        }
        result
    }

    pub fn union_distinct(&self, other: &Multiset) -> Multiset {
        let mut result = Multiset::new();
        for value in self.to_set() {
            result.add(value);
        }
        for value in other.to_set() {
            if result.multiplicity(&value) == 0 {
                result.add(value);
            }
        }
        result
    }

    pub fn except(&self, other: &Multiset) -> Multiset {
        let mut result = Multiset::new();
        for (value, self_count) in &self.elements {
            let other_count = other.multiplicity(value);
            if *self_count > other_count {
                result.add_with_multiplicity(value.clone(), self_count - other_count);
            }
        }
        result
    }

    pub fn except_distinct(&self, other: &Multiset) -> Multiset {
        let mut result = Multiset::new();
        for value in self.to_set() {
            if other.multiplicity(&value) == 0 {
                result.add(value);
            }
        }
        result
    }

    pub fn intersect(&self, other: &Multiset) -> Multiset {
        let mut result = Multiset::new();
        for (value, self_count) in &self.elements {
            let other_count = other.multiplicity(value);
            if other_count > 0 {
                result.add_with_multiplicity(value.clone(), (*self_count).min(other_count));
            }
        }
        result
    }

    pub fn intersect_distinct(&self, other: &Multiset) -> Multiset {
        let mut result = Multiset::new();
        for value in self.to_set() {
            if other.multiplicity(&value) > 0 {
                result.add(value);
            }
        }
        result
    }

    pub fn element(&self) -> Result<Value> {
        if self.cardinality() != 1 {
            return Err(Error::invalid_query(format!(
                "ELEMENT requires exactly one element, found {}",
                self.cardinality()
            )));
        }

        Ok(self.to_vec()[0].clone())
    }

    pub fn is_subset_of(&self, other: &Multiset) -> bool {
        for (value, self_count) in &self.elements {
            if other.multiplicity(value) < *self_count {
                return false;
            }
        }
        true
    }

    pub fn is_proper_subset_of(&self, other: &Multiset) -> bool {
        self.is_subset_of(other) && self != other
    }

    pub fn contains(&self, value: &Value) -> bool {
        self.multiplicity(value) > 0
    }
}

impl Default for Multiset {
    fn default() -> Self {
        Self::new()
    }
}

pub mod aggregates {
    use super::*;

    pub fn collect(values: &[Value]) -> Multiset {
        Multiset::from_vec(values.to_vec())
    }

    pub fn fusion(multisets: &[Multiset]) -> Multiset {
        let mut result = Multiset::new();
        for ms in multisets {
            result = result.union(ms);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multiset_creation() {
        let ms = Multiset::from_vec(vec![
            Value::int64(1),
            Value::int64(2),
            Value::int64(2),
            Value::int64(3),
        ]);

        assert_eq!(ms.cardinality(), 4);
        assert_eq!(ms.multiplicity(&Value::int64(1)), 1);
        assert_eq!(ms.multiplicity(&Value::int64(2)), 2);
        assert_eq!(ms.multiplicity(&Value::int64(3)), 1);
    }

    #[test]
    fn test_multiset_union() {
        let ms1 = Multiset::from_vec(vec![Value::int64(1), Value::int64(2), Value::int64(2)]);

        let ms2 = Multiset::from_vec(vec![Value::int64(2), Value::int64(3)]);

        let result = ms1.union(&ms2);

        assert_eq!(result.cardinality(), 5);
        assert_eq!(result.multiplicity(&Value::int64(1)), 1);
        assert_eq!(result.multiplicity(&Value::int64(2)), 3);
        assert_eq!(result.multiplicity(&Value::int64(3)), 1);
    }

    #[test]
    fn test_multiset_union_distinct() {
        let ms1 = Multiset::from_vec(vec![Value::int64(1), Value::int64(2), Value::int64(2)]);

        let ms2 = Multiset::from_vec(vec![Value::int64(2), Value::int64(3)]);

        let result = ms1.union_distinct(&ms2);

        assert_eq!(result.cardinality(), 3);
        assert_eq!(result.multiplicity(&Value::int64(1)), 1);
        assert_eq!(result.multiplicity(&Value::int64(2)), 1);
        assert_eq!(result.multiplicity(&Value::int64(3)), 1);
    }

    #[test]
    fn test_multiset_except() {
        let ms1 = Multiset::from_vec(vec![
            Value::int64(1),
            Value::int64(2),
            Value::int64(2),
            Value::int64(2),
        ]);

        let ms2 = Multiset::from_vec(vec![Value::int64(2), Value::int64(3)]);

        let result = ms1.except(&ms2);

        assert_eq!(result.cardinality(), 3);
        assert_eq!(result.multiplicity(&Value::int64(1)), 1);
        assert_eq!(result.multiplicity(&Value::int64(2)), 2);
        assert_eq!(result.multiplicity(&Value::int64(3)), 0);
    }

    #[test]
    fn test_multiset_intersect() {
        let ms1 = Multiset::from_vec(vec![
            Value::int64(1),
            Value::int64(2),
            Value::int64(2),
            Value::int64(2),
        ]);

        let ms2 = Multiset::from_vec(vec![Value::int64(2), Value::int64(2), Value::int64(3)]);

        let result = ms1.intersect(&ms2);

        assert_eq!(result.cardinality(), 2);
        assert_eq!(result.multiplicity(&Value::int64(1)), 0);
        assert_eq!(result.multiplicity(&Value::int64(2)), 2);
        assert_eq!(result.multiplicity(&Value::int64(3)), 0);
    }

    #[test]
    fn test_element_single() {
        let ms = Multiset::from_vec(vec![Value::int64(42)]);

        let result = ms.element().unwrap();
        assert_eq!(result, Value::int64(42));
    }

    #[test]
    fn test_element_multiple_error() {
        let ms = Multiset::from_vec(vec![Value::int64(1), Value::int64(2)]);

        let result = ms.element();
        assert!(result.is_err());
    }

    #[test]
    fn test_is_subset_of() {
        let ms1 = Multiset::from_vec(vec![Value::int64(1), Value::int64(2)]);

        let ms2 = Multiset::from_vec(vec![
            Value::int64(1),
            Value::int64(2),
            Value::int64(2),
            Value::int64(3),
        ]);

        assert!(ms1.is_subset_of(&ms2));
        assert!(!ms2.is_subset_of(&ms1));
    }

    #[test]
    fn test_contains() {
        let ms = Multiset::from_vec(vec![Value::int64(1), Value::int64(2), Value::int64(2)]);

        assert!(ms.contains(&Value::int64(1)));
        assert!(ms.contains(&Value::int64(2)));
        assert!(!ms.contains(&Value::int64(3)));
    }

    #[test]
    fn test_to_set() {
        let ms = Multiset::from_vec(vec![
            Value::int64(1),
            Value::int64(2),
            Value::int64(2),
            Value::int64(3),
        ]);

        let set = ms.to_set();
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn test_collect_aggregate() {
        let values = vec![
            Value::int64(1),
            Value::int64(2),
            Value::int64(2),
            Value::int64(3),
        ];

        let ms = aggregates::collect(&values);
        assert_eq!(ms.cardinality(), 4);
        assert_eq!(ms.multiplicity(&Value::int64(2)), 2);
    }

    #[test]
    fn test_fusion_aggregate() {
        let ms1 = Multiset::from_vec(vec![Value::int64(1), Value::int64(2)]);
        let ms2 = Multiset::from_vec(vec![Value::int64(2), Value::int64(3)]);

        let result = aggregates::fusion(&[ms1, ms2]);

        assert_eq!(result.cardinality(), 4);
        assert_eq!(result.multiplicity(&Value::int64(2)), 2);
    }
}
