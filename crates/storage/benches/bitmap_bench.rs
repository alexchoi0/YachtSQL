#![allow(clippy::new_without_default, clippy::len_without_is_empty)]

use aligned_vec::{AVec, ConstAlign};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};

type A64 = ConstAlign<64>;

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

fn sum_original(data: &[i64], nulls: &NullBitmapBitPacked) -> Option<f64> {
    let mut sum: i64 = 0;
    let mut has_value = false;
    for (i, &val) in data.iter().enumerate() {
        if !nulls.is_null(i) {
            sum += val;
            has_value = true;
        }
    }
    if has_value { Some(sum as f64) } else { None }
}

fn sum_option2_auto_vectorize(data: &[i64], nulls: &NullBitmapBitPacked) -> Option<f64> {
    let null_count = nulls.count_null();
    if null_count == data.len() {
        None
    } else if null_count == 0 {
        Some(data.iter().sum::<i64>() as f64)
    } else {
        let sum: i64 = data
            .iter()
            .enumerate()
            .filter(|(i, _)| !nulls.is_null(*i))
            .map(|(_, &v)| v)
            .sum();
        Some(sum as f64)
    }
}

fn sum_option3_bitmap_aware(data: &[i64], nulls: &NullBitmapBitPacked) -> Option<f64> {
    let mut sum: i64 = 0;
    let mut has_value = false;

    for (word_idx, &bitmap_word) in nulls.data.iter().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(data.len());

        if bitmap_word == 0 {
            sum += data[start..end].iter().sum::<i64>();
            has_value = true;
        } else if bitmap_word != u64::MAX {
            let mut valid_mask = !bitmap_word;
            while valid_mask != 0 {
                let bit = valid_mask.trailing_zeros() as usize;
                if start + bit < end {
                    sum += data[start + bit];
                    has_value = true;
                }
                valid_mask &= valid_mask - 1;
            }
        }
    }

    if has_value { Some(sum as f64) } else { None }
}

fn sum_option3b_bitmap_aware_chunks(data: &[i64], nulls: &NullBitmapBitPacked) -> Option<f64> {
    let mut sum: i64 = 0;
    let mut has_value = false;
    let chunks = data.chunks_exact(64);
    let remainder = chunks.remainder();

    for (bitmap_word, chunk) in nulls.data.iter().zip(chunks) {
        if *bitmap_word == 0 {
            sum += chunk.iter().sum::<i64>();
            has_value = true;
        } else if *bitmap_word != u64::MAX {
            let mut valid_mask = !*bitmap_word;
            while valid_mask != 0 {
                let bit = valid_mask.trailing_zeros() as usize;
                sum += chunk[bit];
                has_value = true;
                valid_mask &= valid_mask - 1;
            }
        }
    }

    if !remainder.is_empty() {
        let last_word = nulls.data.last().copied().unwrap_or(0);
        if last_word == 0 {
            sum += remainder.iter().sum::<i64>();
            has_value = true;
        } else {
            for (i, &val) in remainder.iter().enumerate() {
                if (last_word >> i) & 1 == 0 {
                    sum += val;
                    has_value = true;
                }
            }
        }
    }

    if has_value { Some(sum as f64) } else { None }
}

fn sum_option3c_hybrid(data: &[i64], nulls: &NullBitmapBitPacked) -> Option<f64> {
    let null_count = nulls.count_null();
    if null_count == data.len() {
        return None;
    }
    if null_count == 0 {
        return Some(data.iter().sum::<i64>() as f64);
    }

    let mut sum: i64 = 0;
    let chunks = data.chunks_exact(64);
    let remainder = chunks.remainder();

    for (bitmap_word, chunk) in nulls.data.iter().zip(chunks) {
        if *bitmap_word == 0 {
            sum += chunk.iter().sum::<i64>();
        } else if *bitmap_word != u64::MAX {
            let mut valid_mask = !*bitmap_word;
            while valid_mask != 0 {
                let bit = valid_mask.trailing_zeros() as usize;
                sum += chunk[bit];
                valid_mask &= valid_mask - 1;
            }
        }
    }

    if !remainder.is_empty() {
        let last_word = nulls.data.last().copied().unwrap_or(0);
        if last_word == 0 {
            sum += remainder.iter().sum::<i64>();
        } else {
            for (i, &val) in remainder.iter().enumerate() {
                if (last_word >> i) & 1 == 0 {
                    sum += val;
                }
            }
        }
    }

    Some(sum as f64)
}

fn sum_f64_original(data: &[f64], nulls: &NullBitmapBitPacked) -> Option<f64> {
    let mut sum: f64 = 0.0;
    let mut has_value = false;
    for (i, &val) in data.iter().enumerate() {
        if !nulls.is_null(i) {
            sum += val;
            has_value = true;
        }
    }
    if has_value { Some(sum) } else { None }
}

fn sum_f64_auto_vectorize(data: &[f64], nulls: &NullBitmapBitPacked) -> Option<f64> {
    let null_count = nulls.count_null();
    if null_count == data.len() {
        None
    } else if null_count == 0 {
        Some(data.iter().sum())
    } else {
        let sum: f64 = data
            .iter()
            .enumerate()
            .filter(|(i, _)| !nulls.is_null(*i))
            .map(|(_, &v)| v)
            .sum();
        Some(sum)
    }
}

fn sum_f64_bitmap_chunks(data: &[f64], nulls: &NullBitmapBitPacked) -> Option<f64> {
    let mut sum: f64 = 0.0;
    let mut has_value = false;
    let chunks = data.chunks_exact(64);
    let remainder = chunks.remainder();

    for (bitmap_word, chunk) in nulls.data.iter().zip(chunks) {
        if *bitmap_word == 0 {
            sum += chunk.iter().sum::<f64>();
            has_value = true;
        } else if *bitmap_word != u64::MAX {
            let mut valid_mask = !*bitmap_word;
            while valid_mask != 0 {
                let bit = valid_mask.trailing_zeros() as usize;
                sum += chunk[bit];
                has_value = true;
                valid_mask &= valid_mask - 1;
            }
        }
    }

    if !remainder.is_empty() {
        let last_word = nulls.data.last().copied().unwrap_or(0);
        if last_word == 0 {
            sum += remainder.iter().sum::<f64>();
            has_value = true;
        } else {
            for (i, &val) in remainder.iter().enumerate() {
                if (last_word >> i) & 1 == 0 {
                    sum += val;
                    has_value = true;
                }
            }
        }
    }

    if has_value { Some(sum) } else { None }
}

fn sum_i64_avec_bitmap_chunks(data: &AVec<i64, A64>, nulls: &NullBitmapBitPacked) -> Option<f64> {
    let mut sum: i64 = 0;
    let mut has_value = false;
    let chunks = data.chunks_exact(64);
    let remainder = chunks.remainder();

    for (&bitmap_word, chunk) in nulls.data.iter().zip(chunks) {
        if bitmap_word == 0 {
            sum += chunk.iter().sum::<i64>();
            has_value = true;
        } else if bitmap_word != u64::MAX {
            let mut valid_mask = !bitmap_word;
            while valid_mask != 0 {
                let bit = valid_mask.trailing_zeros() as usize;
                sum += chunk[bit];
                has_value = true;
                valid_mask &= valid_mask - 1;
            }
        }
    }

    if !remainder.is_empty() {
        let last_word = nulls.data.last().copied().unwrap_or(0);
        if last_word == 0 {
            sum += remainder.iter().sum::<i64>();
            has_value = true;
        } else {
            for (i, &val) in remainder.iter().enumerate() {
                if (last_word >> i) & 1 == 0 {
                    sum += val;
                    has_value = true;
                }
            }
        }
    }

    if has_value { Some(sum as f64) } else { None }
}

fn sum_f64_avec_bitmap_chunks(data: &AVec<f64, A64>, nulls: &NullBitmapBitPacked) -> Option<f64> {
    let mut sum: f64 = 0.0;
    let mut has_value = false;
    let chunks = data.chunks_exact(64);
    let remainder = chunks.remainder();

    for (&bitmap_word, chunk) in nulls.data.iter().zip(chunks) {
        if bitmap_word == 0 {
            sum += chunk.iter().sum::<f64>();
            has_value = true;
        } else if bitmap_word != u64::MAX {
            let mut valid_mask = !bitmap_word;
            while valid_mask != 0 {
                let bit = valid_mask.trailing_zeros() as usize;
                sum += chunk[bit];
                has_value = true;
                valid_mask &= valid_mask - 1;
            }
        }
    }

    if !remainder.is_empty() {
        let last_word = nulls.data.last().copied().unwrap_or(0);
        if last_word == 0 {
            sum += remainder.iter().sum::<f64>();
            has_value = true;
        } else {
            for (i, &val) in remainder.iter().enumerate() {
                if (last_word >> i) & 1 == 0 {
                    sum += val;
                    has_value = true;
                }
            }
        }
    }

    if has_value { Some(sum) } else { None }
}

fn bench_sum(c: &mut Criterion) {
    let mut group = c.benchmark_group("sum_i64");

    for (name, null_ratio) in [
        ("no_nulls", 0.0),
        ("10pct_nulls", 0.1),
        ("50pct_nulls", 0.5),
    ] {
        let size = 100_000;
        let data: Vec<i64> = (0..size as i64).collect();
        let data_avec: AVec<i64, A64> = AVec::from_iter(64, 0..size as i64);
        let mut nulls = NullBitmapBitPacked::new_valid(size);

        let null_count = (size as f64 * null_ratio) as usize;
        for i in 0..null_count {
            nulls.set(i * (size / null_count.max(1)), true);
        }

        group.bench_with_input(
            BenchmarkId::new("original", name),
            &(&data, &nulls),
            |b, (data, nulls)| b.iter(|| black_box(sum_original(data, nulls))),
        );

        group.bench_with_input(
            BenchmarkId::new("auto_vectorize", name),
            &(&data, &nulls),
            |b, (data, nulls)| b.iter(|| black_box(sum_option2_auto_vectorize(data, nulls))),
        );

        group.bench_with_input(
            BenchmarkId::new("bitmap_aware", name),
            &(&data, &nulls),
            |b, (data, nulls)| b.iter(|| black_box(sum_option3_bitmap_aware(data, nulls))),
        );

        group.bench_with_input(
            BenchmarkId::new("bitmap_chunks", name),
            &(&data, &nulls),
            |b, (data, nulls)| b.iter(|| black_box(sum_option3b_bitmap_aware_chunks(data, nulls))),
        );

        group.bench_with_input(
            BenchmarkId::new("hybrid", name),
            &(&data, &nulls),
            |b, (data, nulls)| b.iter(|| black_box(sum_option3c_hybrid(data, nulls))),
        );

        group.bench_with_input(
            BenchmarkId::new("avec_bitmap_chunks", name),
            &(&data_avec, &nulls),
            |b, (data, nulls)| b.iter(|| black_box(sum_i64_avec_bitmap_chunks(data, nulls))),
        );
    }

    group.finish();
}

fn bench_sum_f64(c: &mut Criterion) {
    let mut group = c.benchmark_group("sum_f64");

    for (name, null_ratio) in [
        ("no_nulls", 0.0),
        ("10pct_nulls", 0.1),
        ("50pct_nulls", 0.5),
    ] {
        let size = 100_000;
        let data: Vec<f64> = (0..size).map(|i| i as f64 * 1.1).collect();
        let data_avec: AVec<f64, A64> = AVec::from_iter(64, (0..size).map(|i| i as f64 * 1.1));
        let mut nulls = NullBitmapBitPacked::new_valid(size);

        let null_count = (size as f64 * null_ratio) as usize;
        for i in 0..null_count {
            nulls.set(i * (size / null_count.max(1)), true);
        }

        group.bench_with_input(
            BenchmarkId::new("original", name),
            &(&data, &nulls),
            |b, (data, nulls)| b.iter(|| black_box(sum_f64_original(data, nulls))),
        );

        group.bench_with_input(
            BenchmarkId::new("auto_vectorize", name),
            &(&data, &nulls),
            |b, (data, nulls)| b.iter(|| black_box(sum_f64_auto_vectorize(data, nulls))),
        );

        group.bench_with_input(
            BenchmarkId::new("bitmap_chunks", name),
            &(&data, &nulls),
            |b, (data, nulls)| b.iter(|| black_box(sum_f64_bitmap_chunks(data, nulls))),
        );

        group.bench_with_input(
            BenchmarkId::new("avec_bitmap_chunks", name),
            &(&data_avec, &nulls),
            |b, (data, nulls)| b.iter(|| black_box(sum_f64_avec_bitmap_chunks(data, nulls))),
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
    bench_sum,
    bench_sum_f64,
);
criterion_main!(benches);
