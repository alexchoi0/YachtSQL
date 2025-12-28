use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NullBitmap {
    data: Vec<u64>,
    len: usize,
}

impl NullBitmap {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            len: 0,
        }
    }

    pub fn new_valid(len: usize) -> Self {
        let num_words = len.div_ceil(64);
        Self {
            data: vec![0; num_words],
            len,
        }
    }

    pub fn new_null(len: usize) -> Self {
        let num_words = len.div_ceil(64);
        Self {
            data: vec![u64::MAX; num_words],
            len,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn is_null(&self, index: usize) -> bool {
        if index >= self.len {
            return true;
        }
        let word = index / 64;
        let bit = index % 64;
        (self.data[word] >> bit) & 1 == 1
    }

    #[inline]
    pub fn is_valid(&self, index: usize) -> bool {
        !self.is_null(index)
    }

    #[inline]
    pub fn set(&mut self, index: usize, is_null: bool) {
        if index >= self.len {
            return;
        }
        let word = index / 64;
        let bit = index % 64;
        if is_null {
            self.data[word] |= 1 << bit;
        } else {
            self.data[word] &= !(1 << bit);
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
        let word = self.len / 64;
        let bit = self.len % 64;
        if word >= self.data.len() {
            self.data.push(0);
        }
        if is_null {
            self.data[word] |= 1 << bit;
        }
        self.len += 1;
    }

    pub fn remove(&mut self, index: usize) {
        if index >= self.len {
            return;
        }
        for i in index..self.len - 1 {
            let next_null = self.is_null(i + 1);
            self.set(i, next_null);
        }
        self.len -= 1;
        let num_words = self.len.div_ceil(64);
        self.data.truncate(num_words.max(1));
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.len = 0;
    }

    pub fn count_null(&self) -> usize {
        if self.len == 0 {
            return 0;
        }
        let full_words = self.len / 64;
        let remaining_bits = self.len % 64;
        let mut count: usize = self.data[..full_words]
            .iter()
            .map(|w| w.count_ones() as usize)
            .sum();
        if remaining_bits > 0 && full_words < self.data.len() {
            let mask = (1u64 << remaining_bits) - 1;
            count += (self.data[full_words] & mask).count_ones() as usize;
        }
        count
    }

    pub fn count_valid(&self) -> usize {
        self.len - self.count_null()
    }

    pub fn words(&self) -> &[u64] {
        &self.data
    }
}

impl Default for NullBitmap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn test_new_valid() {
        let bitmap = NullBitmap::new_valid(100);
        assert_eq!(bitmap.len(), 100);
        for i in 0..100 {
            assert!(bitmap.is_valid(i));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_new_null() {
        let bitmap = NullBitmap::new_null(100);
        assert_eq!(bitmap.len(), 100);
        for i in 0..100 {
            assert!(bitmap.is_null(i));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_push_and_check() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(false);
        bitmap.push(true);
        bitmap.push(false);
        assert_eq!(bitmap.len(), 3);
        assert!(bitmap.is_valid(0));
        assert!(bitmap.is_null(1));
        assert!(bitmap.is_valid(2));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_set() {
        let mut bitmap = NullBitmap::new_valid(10);
        bitmap.set(5, true);
        assert!(bitmap.is_null(5));
        bitmap.set(5, false);
        assert!(bitmap.is_valid(5));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(false);
        bitmap.push(true);
        bitmap.push(false);
        bitmap.remove(1);
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.is_valid(0));
        assert!(bitmap.is_valid(1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_count() {
        let mut bitmap = NullBitmap::new();
        for i in 0..100 {
            bitmap.push(i % 3 == 0);
        }
        assert_eq!(bitmap.count_null(), 34);
        assert_eq!(bitmap.count_valid(), 66);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_across_word_boundary() {
        let mut bitmap = NullBitmap::new();
        for i in 0..130 {
            bitmap.push(i % 2 == 0);
        }
        assert_eq!(bitmap.len(), 130);
        for i in 0..130 {
            assert_eq!(bitmap.is_null(i), i % 2 == 0);
        }
    }
}
