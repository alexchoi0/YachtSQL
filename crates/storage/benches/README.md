# SIMD Benchmark Results (ARM64 NEON)

Benchmark results comparing manual SIMD implementations vs scalar (auto-vectorized) implementations on Apple Silicon.

## Results at 100k elements

| Function | SIMD | Scalar | Winner | Speedup |
|----------|------|--------|--------|---------|
| sum_i64 | 24.0µs | 7.3µs | **Scalar** | 3.3x |
| sum_f64 | 30.0µs | 51.9µs | **SIMD** | 1.7x |
| sum_i64_masked | 25.5µs | 12.5µs | **Scalar** | 2.0x |
| sum_f64_masked | 35.6µs | 121.9µs | **SIMD** | 3.4x |
| count_nonzero_i64 | 28.9µs | 7.5µs | **Scalar** | 3.9x |
| min_i64 | 50.6µs | 12.8µs | **Scalar** | 4.0x |
| max_i64 | 50.6µs | 12.9µs | **Scalar** | 3.9x |
| filter_gt_i64 | 37.6µs | 71.2µs | **SIMD** | 1.9x |
| filter_gt_f64 | 37.1µs | ~71µs | **SIMD** | ~1.9x |
| filter_lt_i64 | ~37µs | ~66µs | **SIMD** | ~1.8x |
| filter_eq_i64 | ~37µs | ~45µs | **SIMD** | ~1.2x |
| avg_i64 | ~24µs | ~7µs | **Scalar** | ~3.4x |
| avg_f64 | ~30µs | ~52µs | **SIMD** | ~1.7x |

## NeonBackend Configuration

Based on benchmarks, `NeonBackend` uses the optimal implementation for each function:

**Using SIMD (neon.rs):**
- `sum_f64` - 1.7x faster than scalar
- `sum_f64_masked` - 3.4x faster than scalar
- `filter_gt_i64` - 1.9x faster than scalar
- `filter_gt_f64` - 1.9x faster than scalar
- `filter_lt_i64` - 1.8x faster than scalar
- `filter_eq_i64` - 1.2x faster than scalar
- `filter_gte_i64`, `filter_lte_i64`, `filter_ne_i64` - SIMD
- `avg_f64`, `avg_f64_masked` - uses SIMD sum_f64

**Using Scalar (scalar.rs):**
- `sum_i64` - scalar 3.3x faster
- `sum_i64_masked` - scalar 2.0x faster
- `count_nonzero_i64` - scalar 3.9x faster
- `min_i64` - scalar 4.0x faster
- `max_i64` - scalar 3.9x faster
- `avg_i64`, `avg_i64_masked` - uses scalar sum_i64

## Analysis

The pattern is clear: **i64 operations** are generally slower with manual SIMD because the Rust compiler auto-vectorizes them very effectively. **f64 operations** benefit from manual SIMD, likely because floating-point reductions require explicit SIMD to avoid strict IEEE ordering constraints that prevent auto-vectorization.

## Running Benchmarks

```bash
cargo bench --bench simd_filter_benchmark -p yachtsql-storage
```
