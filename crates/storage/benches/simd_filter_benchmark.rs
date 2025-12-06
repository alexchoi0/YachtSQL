use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
#[cfg(target_arch = "aarch64")]
use yachtsql_storage::simd::{
    avg_f64, avg_i64, count_nonzero_i64, filter_eq_i64, filter_gt_f64, filter_gt_i64,
    filter_lt_i64, max_i64, min_i64, sum_f64, sum_f64_masked, sum_i64, sum_i64_masked,
};

fn sum_i64_scalar(data: &[i64]) -> i64 {
    data.iter().sum()
}

fn sum_f64_scalar(data: &[f64]) -> f64 {
    data.iter().sum()
}

fn sum_i64_masked_scalar(data: &[i64], nulls: &[u8]) -> i64 {
    data.iter()
        .zip(nulls.iter())
        .filter(|&(_, &n)| n != 0)
        .map(|(&d, _)| d)
        .sum()
}

fn sum_f64_masked_scalar(data: &[f64], nulls: &[u8]) -> f64 {
    data.iter()
        .zip(nulls.iter())
        .filter(|&(_, &n)| n != 0)
        .map(|(&d, _)| d)
        .sum()
}

fn count_nonzero_i64_scalar(data: &[i64]) -> usize {
    data.iter().filter(|&&x| x != 0).count()
}

fn min_i64_scalar(data: &[i64]) -> Option<i64> {
    data.iter().copied().min()
}

fn max_i64_scalar(data: &[i64]) -> Option<i64> {
    data.iter().copied().max()
}

fn filter_gt_i64_scalar(data: &[i64], threshold: i64) -> Vec<usize> {
    let mut indices = Vec::with_capacity(data.len());
    for (i, &v) in data.iter().enumerate() {
        if v > threshold {
            indices.push(i);
        }
    }
    indices
}

fn filter_gt_f64_scalar(data: &[f64], threshold: f64) -> Vec<usize> {
    let mut indices = Vec::with_capacity(data.len());
    for (i, &v) in data.iter().enumerate() {
        if v > threshold {
            indices.push(i);
        }
    }
    indices
}

fn filter_lt_i64_scalar(data: &[i64], threshold: i64) -> Vec<usize> {
    let mut indices = Vec::with_capacity(data.len());
    for (i, &v) in data.iter().enumerate() {
        if v < threshold {
            indices.push(i);
        }
    }
    indices
}

fn filter_eq_i64_scalar(data: &[i64], value: i64) -> Vec<usize> {
    let mut indices = Vec::with_capacity(data.len());
    for (i, &v) in data.iter().enumerate() {
        if v == value {
            indices.push(i);
        }
    }
    indices
}

fn avg_i64_scalar(data: &[i64]) -> Option<f64> {
    if data.is_empty() {
        None
    } else {
        Some(data.iter().sum::<i64>() as f64 / data.len() as f64)
    }
}

fn avg_f64_scalar(data: &[f64]) -> Option<f64> {
    if data.is_empty() {
        None
    } else {
        Some(data.iter().sum::<f64>() / data.len() as f64)
    }
}

fn benchmark_sum_i64(c: &mut Criterion) {
    let mut group = c.benchmark_group("sum_i64");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let data: Vec<i64> = (0..*size as i64).collect();

        #[cfg(target_arch = "aarch64")]
        group.bench_with_input(BenchmarkId::new("simd", size), size, |b, _| {
            b.iter(|| sum_i64(black_box(&data)))
        });

        group.bench_with_input(BenchmarkId::new("scalar", size), size, |b, _| {
            b.iter(|| sum_i64_scalar(black_box(&data)))
        });
    }
    group.finish();
}

fn benchmark_sum_f64(c: &mut Criterion) {
    let mut group = c.benchmark_group("sum_f64");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let data: Vec<f64> = (0..*size).map(|i| i as f64).collect();

        #[cfg(target_arch = "aarch64")]
        group.bench_with_input(BenchmarkId::new("simd", size), size, |b, _| {
            b.iter(|| sum_f64(black_box(&data)))
        });

        group.bench_with_input(BenchmarkId::new("scalar", size), size, |b, _| {
            b.iter(|| sum_f64_scalar(black_box(&data)))
        });
    }
    group.finish();
}

fn benchmark_sum_i64_masked(c: &mut Criterion) {
    let mut group = c.benchmark_group("sum_i64_masked");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let data: Vec<i64> = (0..*size as i64).collect();
        let nulls: Vec<u8> = (0..*size).map(|i| if i % 3 == 0 { 0 } else { 1 }).collect();

        #[cfg(target_arch = "aarch64")]
        group.bench_with_input(BenchmarkId::new("simd", size), size, |b, _| {
            b.iter(|| sum_i64_masked(black_box(&data), black_box(&nulls)))
        });

        group.bench_with_input(BenchmarkId::new("scalar", size), size, |b, _| {
            b.iter(|| sum_i64_masked_scalar(black_box(&data), black_box(&nulls)))
        });
    }
    group.finish();
}

fn benchmark_sum_f64_masked(c: &mut Criterion) {
    let mut group = c.benchmark_group("sum_f64_masked");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let data: Vec<f64> = (0..*size).map(|i| i as f64).collect();
        let nulls: Vec<u8> = (0..*size).map(|i| if i % 3 == 0 { 0 } else { 1 }).collect();

        #[cfg(target_arch = "aarch64")]
        group.bench_with_input(BenchmarkId::new("simd", size), size, |b, _| {
            b.iter(|| sum_f64_masked(black_box(&data), black_box(&nulls)))
        });

        group.bench_with_input(BenchmarkId::new("scalar", size), size, |b, _| {
            b.iter(|| sum_f64_masked_scalar(black_box(&data), black_box(&nulls)))
        });
    }
    group.finish();
}

fn benchmark_count_nonzero(c: &mut Criterion) {
    let mut group = c.benchmark_group("count_nonzero_i64");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let data: Vec<i64> = (0..*size as i64)
            .map(|i| if i % 5 == 0 { 0 } else { i })
            .collect();

        #[cfg(target_arch = "aarch64")]
        group.bench_with_input(BenchmarkId::new("simd", size), size, |b, _| {
            b.iter(|| count_nonzero_i64(black_box(&data)))
        });

        group.bench_with_input(BenchmarkId::new("scalar", size), size, |b, _| {
            b.iter(|| count_nonzero_i64_scalar(black_box(&data)))
        });
    }
    group.finish();
}

fn benchmark_min_max(c: &mut Criterion) {
    let mut group = c.benchmark_group("min_max_i64");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let data: Vec<i64> = (0..*size as i64).collect();

        #[cfg(target_arch = "aarch64")]
        {
            group.bench_with_input(BenchmarkId::new("simd_min", size), size, |b, _| {
                b.iter(|| min_i64(black_box(&data)))
            });
            group.bench_with_input(BenchmarkId::new("simd_max", size), size, |b, _| {
                b.iter(|| max_i64(black_box(&data)))
            });
        }

        group.bench_with_input(BenchmarkId::new("scalar_min", size), size, |b, _| {
            b.iter(|| min_i64_scalar(black_box(&data)))
        });
        group.bench_with_input(BenchmarkId::new("scalar_max", size), size, |b, _| {
            b.iter(|| max_i64_scalar(black_box(&data)))
        });
    }
    group.finish();
}

fn benchmark_filter_gt_i64(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_gt_i64");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let data: Vec<i64> = (0..*size as i64).collect();
        let threshold = (*size as i64) / 2;

        #[cfg(target_arch = "aarch64")]
        group.bench_with_input(BenchmarkId::new("simd", size), size, |b, _| {
            b.iter(|| filter_gt_i64(black_box(&data), black_box(threshold)))
        });

        group.bench_with_input(BenchmarkId::new("scalar", size), size, |b, _| {
            b.iter(|| filter_gt_i64_scalar(black_box(&data), black_box(threshold)))
        });
    }
    group.finish();
}

fn benchmark_filter_gt_f64(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_gt_f64");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let data: Vec<f64> = (0..*size).map(|i| i as f64).collect();
        let threshold = (*size as f64) / 2.0;

        #[cfg(target_arch = "aarch64")]
        group.bench_with_input(BenchmarkId::new("simd", size), size, |b, _| {
            b.iter(|| filter_gt_f64(black_box(&data), black_box(threshold)))
        });

        group.bench_with_input(BenchmarkId::new("scalar", size), size, |b, _| {
            b.iter(|| filter_gt_f64_scalar(black_box(&data), black_box(threshold)))
        });
    }
    group.finish();
}

fn benchmark_filter_lt_i64(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_lt_i64");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let data: Vec<i64> = (0..*size as i64).collect();
        let threshold = (*size as i64) / 2;

        #[cfg(target_arch = "aarch64")]
        group.bench_with_input(BenchmarkId::new("simd", size), size, |b, _| {
            b.iter(|| filter_lt_i64(black_box(&data), black_box(threshold)))
        });

        group.bench_with_input(BenchmarkId::new("scalar", size), size, |b, _| {
            b.iter(|| filter_lt_i64_scalar(black_box(&data), black_box(threshold)))
        });
    }
    group.finish();
}

fn benchmark_filter_eq_i64(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_eq_i64");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let data: Vec<i64> = (0..*size as i64).collect();
        let value = (*size as i64) / 2;

        #[cfg(target_arch = "aarch64")]
        group.bench_with_input(BenchmarkId::new("simd", size), size, |b, _| {
            b.iter(|| filter_eq_i64(black_box(&data), black_box(value)))
        });

        group.bench_with_input(BenchmarkId::new("scalar", size), size, |b, _| {
            b.iter(|| filter_eq_i64_scalar(black_box(&data), black_box(value)))
        });
    }
    group.finish();
}

fn benchmark_avg(c: &mut Criterion) {
    let mut group = c.benchmark_group("avg");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let data_i64: Vec<i64> = (0..*size as i64).collect();
        let data_f64: Vec<f64> = (0..*size).map(|i| i as f64).collect();

        #[cfg(target_arch = "aarch64")]
        {
            group.bench_with_input(BenchmarkId::new("simd_i64", size), size, |b, _| {
                b.iter(|| avg_i64(black_box(&data_i64)))
            });
            group.bench_with_input(BenchmarkId::new("simd_f64", size), size, |b, _| {
                b.iter(|| avg_f64(black_box(&data_f64)))
            });
        }

        group.bench_with_input(BenchmarkId::new("scalar_i64", size), size, |b, _| {
            b.iter(|| avg_i64_scalar(black_box(&data_i64)))
        });
        group.bench_with_input(BenchmarkId::new("scalar_f64", size), size, |b, _| {
            b.iter(|| avg_f64_scalar(black_box(&data_f64)))
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    benchmark_sum_i64,
    benchmark_sum_f64,
    benchmark_sum_i64_masked,
    benchmark_sum_f64_masked,
    benchmark_count_nonzero,
    benchmark_min_max,
    benchmark_filter_gt_i64,
    benchmark_filter_gt_f64,
    benchmark_filter_lt_i64,
    benchmark_filter_eq_i64,
    benchmark_avg,
);
criterion_main!(benches);
