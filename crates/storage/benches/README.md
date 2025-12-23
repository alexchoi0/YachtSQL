# Storage Benchmarks

## NullBitmap Implementation

Comparing `Vec<bool>` vs bit-packed `Vec<u64>` for null bitmaps.

### Memory Usage

| Size | Vec<bool> | Bit-packed | Savings |
|------|-----------|------------|---------|
| 1k   | 1,024 B   | 160 B      | 6.4x    |
| 10k  | 10,024 B  | 1,288 B    | 7.8x    |
| 100k | 100,024 B | 12,536 B   | 8x      |
| 1M   | 1,000,024 B | 125,032 B | 8x    |

### count_null Performance

| Size | Vec<bool> | Bit-packed | Winner |
|------|-----------|------------|--------|
| 100  | 5.6 ns    | 0.77 ns    | bit-packed 7x faster |
| 100k | 4.7 µs    | 153 ns     | bit-packed 31x faster |
| 1M   | 47 µs     | 1.5 µs     | bit-packed 31x faster |

Bit-packed uses CPU POPCNT instruction for O(n/64) null counting.

## Sum with Null Handling

Comparing approaches for summing column values while skipping nulls.

### i64 Sum (100k values)

| Scenario | Original | Auto-vectorize | Bitmap-chunks |
|----------|----------|----------------|---------------|
| No nulls | 51 µs    | 7.4 µs         | **7.4 µs**    |
| 10% nulls| 53 µs    | 40 µs          | **43 µs**     |
| 50% nulls| 53 µs    | 38 µs          | **18.5 µs**   |

### f64 Sum (100k values)

| Scenario | Original | Auto-vectorize | Bitmap-chunks |
|----------|----------|----------------|---------------|
| No nulls | 116 µs   | 50 µs          | **10.3 µs**   |
| 10% nulls| 126 µs   | 124 µs         | **53 µs**     |
| 50% nulls| 124 µs   | 124 µs         | **29 µs**     |

### Implementation Details

**Original**: Iterate all values, check `is_null(i)` for each.

**Auto-vectorize**: Fast path for no-nulls, fallback to filtered iterator.

**Bitmap-chunks**: Process 64 elements at a time using bitmap words.
```rust
for (&bitmap_word, chunk) in nulls.words().iter().zip(data.chunks_exact(64)) {
    if bitmap_word == 0 {
        sum += chunk.iter().sum();  // LLVM auto-vectorizes
    } else if bitmap_word != u64::MAX {
        // Extract valid indices with bit manipulation
        let mut valid_mask = !bitmap_word;
        while valid_mask != 0 {
            let bit = valid_mask.trailing_zeros() as usize;
            sum += chunk[bit];
            valid_mask &= valid_mask - 1;
        }
    }
}
```

### Why Bitmap-chunks Wins

1. **No nulls**: `chunks_exact(64)` enables LLVM SIMD vectorization
2. **Sparse nulls**: Processes 64 elements per bitmap word lookup
3. **Dense nulls**: Skips entire 64-element chunks that are all null
4. **f64 advantage**: 11x faster because LLVM vectorizes chunk sums better than filtered iterators

---

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
