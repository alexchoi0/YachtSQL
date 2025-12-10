use std::collections::HashSet;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone)]
pub struct HyperLogLogPlusPlus {
    seen_hashes: HashSet<u64>,
}

impl HyperLogLogPlusPlus {
    pub fn new() -> Self {
        Self {
            seen_hashes: HashSet::new(),
        }
    }

    pub fn add<T: Hash>(&mut self, value: &T) {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        value.hash(&mut hasher);
        let hash = hasher.finish();
        self.seen_hashes.insert(hash);
    }

    pub fn cardinality(&self) -> u64 {
        self.seen_hashes.len() as u64
    }

    pub fn estimate(&self) -> u64 {
        self.cardinality()
    }

    pub fn merge(&mut self, other: &HyperLogLogPlusPlus) {
        self.seen_hashes.extend(other.seen_hashes.iter().cloned());
    }
}

impl Default for HyperLogLogPlusPlus {
    fn default() -> Self {
        Self::new()
    }
}
