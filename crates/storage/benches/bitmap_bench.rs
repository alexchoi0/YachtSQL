#![allow(clippy::new_without_default, clippy::len_without_is_empty)]

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};

#[derive(Debug, Clone, PartialEq)]
pub struct NullBitmapVecBool {
    data: Vec<bool>,
}

impl NullBitmapVecBool {
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

    pub fn push(&mut self, is_null: bool) {
        self.data.push(is_null);
    }

    pub fn count_null(&self) -> usize {
        self.data.iter().filter(|&&b| b).count()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NullBitmapBitPacked {
    data: Vec<u64>,
    len: usize,
}

impl NullBitmapBitPacked {
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
}

fn bench_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("bitmap_creation");

    for size in [100, 1_000, 10_000, 100_000] {
        group.bench_with_input(BenchmarkId::new("vec_bool", size), &size, |b, &size| {
            b.iter(|| NullBitmapVecBool::new_valid(black_box(size)))
        });

        group.bench_with_input(BenchmarkId::new("bit_packed", size), &size, |b, &size| {
            b.iter(|| NullBitmapBitPacked::new_valid(black_box(size)))
        });
    }

    group.finish();
}

fn bench_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("bitmap_push");

    for size in [100, 1_000, 10_000] {
        group.bench_with_input(BenchmarkId::new("vec_bool", size), &size, |b, &size| {
            b.iter(|| {
                let mut bitmap = NullBitmapVecBool::new();
                for i in 0..size {
                    bitmap.push(i % 3 == 0);
                }
                bitmap
            })
        });

        group.bench_with_input(BenchmarkId::new("bit_packed", size), &size, |b, &size| {
            b.iter(|| {
                let mut bitmap = NullBitmapBitPacked::new();
                for i in 0..size {
                    bitmap.push(i % 3 == 0);
                }
                bitmap
            })
        });
    }

    group.finish();
}

fn bench_is_null(c: &mut Criterion) {
    let mut group = c.benchmark_group("bitmap_is_null");

    for size in [100, 1_000, 10_000, 100_000] {
        let vec_bool = {
            let mut b = NullBitmapVecBool::new();
            for i in 0..size {
                b.push(i % 3 == 0);
            }
            b
        };
        let bit_packed = {
            let mut b = NullBitmapBitPacked::new();
            for i in 0..size {
                b.push(i % 3 == 0);
            }
            b
        };

        group.bench_with_input(BenchmarkId::new("vec_bool", size), &size, |b, &size| {
            b.iter(|| {
                let mut count = 0;
                for i in 0..size {
                    if vec_bool.is_null(i) {
                        count += 1;
                    }
                }
                count
            })
        });

        group.bench_with_input(BenchmarkId::new("bit_packed", size), &size, |b, &size| {
            b.iter(|| {
                let mut count = 0;
                for i in 0..size {
                    if bit_packed.is_null(i) {
                        count += 1;
                    }
                }
                count
            })
        });
    }

    group.finish();
}

fn bench_count_null(c: &mut Criterion) {
    let mut group = c.benchmark_group("bitmap_count_null");

    for size in [100, 1_000, 10_000, 100_000, 1_000_000] {
        let vec_bool = {
            let mut b = NullBitmapVecBool::new();
            for i in 0..size {
                b.push(i % 3 == 0);
            }
            b
        };
        let bit_packed = {
            let mut b = NullBitmapBitPacked::new();
            for i in 0..size {
                b.push(i % 3 == 0);
            }
            b
        };

        group.bench_with_input(
            BenchmarkId::new("vec_bool", size),
            &vec_bool,
            |b, bitmap| b.iter(|| bitmap.count_null()),
        );

        group.bench_with_input(
            BenchmarkId::new("bit_packed", size),
            &bit_packed,
            |b, bitmap| b.iter(|| bitmap.count_null()),
        );
    }

    group.finish();
}

fn bench_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("bitmap_set");

    for size in [100, 1_000, 10_000] {
        group.bench_with_input(BenchmarkId::new("vec_bool", size), &size, |b, &size| {
            let mut bitmap = NullBitmapVecBool::new_valid(size);
            b.iter(|| {
                for i in 0..size {
                    bitmap.set(i, i % 2 == 0);
                }
            })
        });

        group.bench_with_input(BenchmarkId::new("bit_packed", size), &size, |b, &size| {
            let mut bitmap = NullBitmapBitPacked::new_valid(size);
            b.iter(|| {
                for i in 0..size {
                    bitmap.set(i, i % 2 == 0);
                }
            })
        });
    }

    group.finish();
}

fn bench_memory_size(c: &mut Criterion) {
    let group = c.benchmark_group("bitmap_memory");

    for size in [1_000, 10_000, 100_000, 1_000_000] {
        let vec_bool = NullBitmapVecBool::new_valid(size);
        let bit_packed = NullBitmapBitPacked::new_valid(size);

        let vec_bool_bytes = std::mem::size_of_val(&vec_bool) + vec_bool.data.capacity();
        let bit_packed_bytes = std::mem::size_of_val(&bit_packed) + bit_packed.data.capacity() * 8;

        println!(
            "Size {}: vec_bool = {} bytes, bit_packed = {} bytes, ratio = {:.1}x",
            size,
            vec_bool_bytes,
            bit_packed_bytes,
            vec_bool_bytes as f64 / bit_packed_bytes as f64
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_creation,
    bench_push,
    bench_is_null,
    bench_count_null,
    bench_set,
    bench_memory_size,
);
criterion_main!(benches);
