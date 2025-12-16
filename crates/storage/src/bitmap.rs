use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NullBitmap {
    data: Vec<bool>,
}

impl NullBitmap {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn new_valid(len: usize) -> Self {
        Self {
            data: vec![false; len],
        }
    }

    pub fn new_null(len: usize) -> Self {
        Self {
            data: vec![true; len],
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[inline]
    pub fn is_null(&self, index: usize) -> bool {
        self.data.get(index).copied().unwrap_or(true)
    }

    #[inline]
    pub fn is_valid(&self, index: usize) -> bool {
        !self.is_null(index)
    }

    #[inline]
    pub fn set(&mut self, index: usize, is_null: bool) {
        if index < self.data.len() {
            self.data[index] = is_null;
        }
    }

    #[inline]
    pub fn set_valid(&mut self, index: usize) {
        self.set(index, false);
    }

    #[inline]
    pub fn set_null(&mut self, index: usize) {
        self.set(index, true);
    }

    pub fn push(&mut self, is_null: bool) {
        self.data.push(is_null);
    }

    pub fn remove(&mut self, index: usize) {
        if index < self.data.len() {
            self.data.remove(index);
        }
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn count_null(&self) -> usize {
        self.data.iter().filter(|&&b| b).count()
    }

    pub fn count_valid(&self) -> usize {
        self.len() - self.count_null()
    }
}

impl Default for NullBitmap {
    fn default() -> Self {
        Self::new()
    }
}
