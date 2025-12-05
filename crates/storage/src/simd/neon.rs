#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

#[cfg(target_arch = "aarch64")]
pub fn sum_i64(data: &[i64]) -> i64 {
    if data.is_empty() {
        return 0;
    }

    unsafe {
        let mut sum_vec = vdupq_n_s64(0);
        let chunks = data.chunks_exact(2);
        let remainder = chunks.remainder();

        for chunk in chunks {
            let v = vld1q_s64(chunk.as_ptr());
            sum_vec = vaddq_s64(sum_vec, v);
        }

        let sum = vaddvq_s64(sum_vec);

        let remainder_sum: i64 = remainder.iter().sum();
        sum + remainder_sum
    }
}

#[cfg(target_arch = "aarch64")]
pub fn sum_f64(data: &[f64]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }

    unsafe {
        let mut sum_vec = vdupq_n_f64(0.0);
        let chunks = data.chunks_exact(2);
        let remainder = chunks.remainder();

        for chunk in chunks {
            let v = vld1q_f64(chunk.as_ptr());
            sum_vec = vaddq_f64(sum_vec, v);
        }

        let sum = vaddvq_f64(sum_vec);

        let remainder_sum: f64 = remainder.iter().sum();
        sum + remainder_sum
    }
}

#[cfg(target_arch = "aarch64")]
pub fn sum_i64_masked(data: &[i64], nulls: &[u8]) -> i64 {
    assert_eq!(data.len(), nulls.len());
    if data.is_empty() {
        return 0;
    }

    unsafe {
        let mut sum_vec = vdupq_n_s64(0);
        let zero_vec = vdupq_n_s64(0);
        let chunks_data = data.chunks_exact(2);
        let chunks_nulls = nulls.chunks_exact(2);
        let remainder_data = chunks_data.remainder();
        let remainder_nulls = chunks_nulls.remainder();

        for (chunk_data, chunk_nulls) in chunks_data.zip(chunks_nulls) {
            let data_vec = vld1q_s64(chunk_data.as_ptr());

            let mask0: u64 = if chunk_nulls[0] != 0 { !0 } else { 0 };
            let mask1: u64 = if chunk_nulls[1] != 0 { !0 } else { 0 };
            let mask_vec = vld1q_u64([mask0, mask1].as_ptr());

            let masked = vbslq_s64(mask_vec, data_vec, zero_vec);
            sum_vec = vaddq_s64(sum_vec, masked);
        }

        let sum = vaddvq_s64(sum_vec);
        let remainder_sum: i64 = remainder_data
            .iter()
            .zip(remainder_nulls.iter())
            .filter(|&(_, n)| *n != 0)
            .map(|(&d, _)| d)
            .sum();

        sum + remainder_sum
    }
}

#[cfg(target_arch = "aarch64")]
pub fn sum_f64_masked(data: &[f64], nulls: &[u8]) -> f64 {
    assert_eq!(data.len(), nulls.len());

    if data.is_empty() {
        return 0.0;
    }

    unsafe {
        let mut sum_vec = vdupq_n_f64(0.0);
        let chunks_data = data.chunks_exact(2);
        let chunks_nulls = nulls.chunks_exact(2);
        let remainder_data = chunks_data.remainder();
        let remainder_nulls = chunks_nulls.remainder();

        for (chunk_data, chunk_nulls) in chunks_data.zip(chunks_nulls) {
            let data_vec = vld1q_f64(chunk_data.as_ptr());

            let mask_0 = chunk_nulls[0] as f64;
            let mask_1 = chunk_nulls[1] as f64;
            let mask_vec = vld1q_f64([mask_0, mask_1].as_ptr());

            let masked = vmulq_f64(data_vec, mask_vec);

            sum_vec = vaddq_f64(sum_vec, masked);
        }

        let sum = vaddvq_f64(sum_vec);

        let remainder_sum: f64 = remainder_data
            .iter()
            .zip(remainder_nulls.iter())
            .map(|(&d, &n)| d * (n as f64))
            .sum();

        sum + remainder_sum
    }
}

#[cfg(target_arch = "aarch64")]
pub fn count_nonzero_i64(data: &[i64]) -> usize {
    if data.is_empty() {
        return 0;
    }

    unsafe {
        let zero_vec = vdupq_n_s64(0);
        let mut count = 0usize;
        let chunks = data.chunks_exact(2);
        let remainder = chunks.remainder();

        for chunk in chunks {
            let v = vld1q_s64(chunk.as_ptr());

            let cmp = vceqq_s64(v, zero_vec);

            let lane0 = vgetq_lane_u64(cmp, 0);
            let lane1 = vgetq_lane_u64(cmp, 1);

            if lane0 != 0xFFFFFFFFFFFFFFFF {
                count += 1;
            }
            if lane1 != 0xFFFFFFFFFFFFFFFF {
                count += 1;
            }
        }

        count += remainder.iter().filter(|&&x| x != 0).count();
        count
    }
}

#[cfg(target_arch = "aarch64")]
pub fn filter_gt_i64(data: &[i64], threshold: i64) -> Vec<usize> {
    if data.is_empty() {
        return Vec::new();
    }

    let mut indices = Vec::with_capacity(data.len());

    unsafe {
        let threshold_vec = vdupq_n_s64(threshold);
        let chunks = data.chunks_exact(2);
        let remainder = chunks.remainder();
        let mut idx = 0;

        for chunk in chunks {
            let v = vld1q_s64(chunk.as_ptr());

            let cmp = vcgtq_s64(v, threshold_vec);

            let lane0 = vgetq_lane_u64(cmp, 0);
            let lane1 = vgetq_lane_u64(cmp, 1);

            if lane0 != 0 {
                indices.push(idx);
            }
            if lane1 != 0 {
                indices.push(idx + 1);
            }

            idx += 2;
        }

        for (i, &value) in remainder.iter().enumerate() {
            if value > threshold {
                indices.push(idx + i);
            }
        }
    }

    indices
}

#[cfg(target_arch = "aarch64")]
pub fn filter_gt_i64_with_nulls(data: &[i64], nulls: &[u8], threshold: i64) -> Vec<usize> {
    assert_eq!(data.len(), nulls.len());
    let mut indices = Vec::with_capacity(data.len());
    for (i, (&v, &n)) in data.iter().zip(nulls.iter()).enumerate() {
        if v > threshold && n != 0 {
            indices.push(i);
        }
    }
    indices
}

#[cfg(target_arch = "aarch64")]
pub fn filter_gt_f64(data: &[f64], threshold: f64) -> Vec<usize> {
    if data.is_empty() {
        return Vec::new();
    }

    let mut indices = Vec::with_capacity(data.len());

    unsafe {
        let threshold_vec = vdupq_n_f64(threshold);
        let chunks = data.chunks_exact(2);
        let remainder = chunks.remainder();
        let mut idx = 0;

        for chunk in chunks {
            let v = vld1q_f64(chunk.as_ptr());
            let cmp = vcgtq_f64(v, threshold_vec);

            let lane0 = vgetq_lane_u64(cmp, 0);
            let lane1 = vgetq_lane_u64(cmp, 1);

            if lane0 != 0 {
                indices.push(idx);
            }
            if lane1 != 0 {
                indices.push(idx + 1);
            }

            idx += 2;
        }

        for (i, &value) in remainder.iter().enumerate() {
            if value > threshold {
                indices.push(idx + i);
            }
        }
    }

    indices
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn vmaxq_s64_emulated(a: int64x2_t, b: int64x2_t) -> int64x2_t {
    unsafe {
        let mask = vcgtq_s64(a, b);
        vbslq_s64(mask, a, b)
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn vminq_s64_emulated(a: int64x2_t, b: int64x2_t) -> int64x2_t {
    unsafe {
        let mask = vcltq_s64(a, b);
        vbslq_s64(mask, a, b)
    }
}

#[cfg(target_arch = "aarch64")]
pub fn min_i64(data: &[i64]) -> Option<i64> {
    if data.is_empty() {
        return None;
    }

    unsafe {
        let len = data.len();
        let ptr = data.as_ptr();
        let mut min_vec = vdupq_n_s64(i64::MAX);

        let chunks = len / 2;
        for i in 0..chunks {
            let vec = vld1q_s64(ptr.add(i * 2));
            min_vec = vminq_s64_emulated(min_vec, vec);
        }

        let val0 = vgetq_lane_s64(min_vec, 0);
        let val1 = vgetq_lane_s64(min_vec, 1);
        let mut result = val0.min(val1);

        for i in (chunks * 2)..len {
            result = result.min(*ptr.add(i));
        }

        Some(result)
    }
}

#[cfg(target_arch = "aarch64")]
pub fn max_i64(data: &[i64]) -> Option<i64> {
    if data.is_empty() {
        return None;
    }

    unsafe {
        let len = data.len();
        let ptr = data.as_ptr();
        let mut max_vec = vdupq_n_s64(i64::MIN);

        let chunks = len / 2;
        for i in 0..chunks {
            let vec = vld1q_s64(ptr.add(i * 2));
            max_vec = vmaxq_s64_emulated(max_vec, vec);
        }

        let val0 = vgetq_lane_s64(max_vec, 0);
        let val1 = vgetq_lane_s64(max_vec, 1);
        let mut result = val0.max(val1);

        for i in (chunks * 2)..len {
            result = result.max(*ptr.add(i));
        }

        Some(result)
    }
}

#[cfg(target_arch = "aarch64")]
pub fn filter_eq_i64(data: &[i64], value: i64) -> Vec<usize> {
    if data.is_empty() {
        return Vec::new();
    }

    let mut indices = Vec::with_capacity(data.len());

    unsafe {
        let value_vec = vdupq_n_s64(value);
        let chunks = data.chunks_exact(2);
        let remainder = chunks.remainder();
        let mut idx = 0;

        for chunk in chunks {
            let v = vld1q_s64(chunk.as_ptr());
            let cmp = vceqq_s64(v, value_vec);

            let lane0 = vgetq_lane_u64(cmp, 0);
            let lane1 = vgetq_lane_u64(cmp, 1);

            if lane0 != 0 {
                indices.push(idx);
            }
            if lane1 != 0 {
                indices.push(idx + 1);
            }
            idx += 2;
        }

        for (i, &v) in remainder.iter().enumerate() {
            if v == value {
                indices.push(idx + i);
            }
        }
    }

    indices
}

#[cfg(target_arch = "aarch64")]
pub fn filter_lt_i64(data: &[i64], threshold: i64) -> Vec<usize> {
    if data.is_empty() {
        return Vec::new();
    }

    let mut indices = Vec::with_capacity(data.len());

    unsafe {
        let threshold_vec = vdupq_n_s64(threshold);
        let chunks = data.chunks_exact(2);
        let remainder = chunks.remainder();
        let mut idx = 0;

        for chunk in chunks {
            let v = vld1q_s64(chunk.as_ptr());
            let cmp = vcltq_s64(v, threshold_vec);

            let lane0 = vgetq_lane_u64(cmp, 0);
            let lane1 = vgetq_lane_u64(cmp, 1);

            if lane0 != 0 {
                indices.push(idx);
            }
            if lane1 != 0 {
                indices.push(idx + 1);
            }
            idx += 2;
        }

        for (i, &value) in remainder.iter().enumerate() {
            if value < threshold {
                indices.push(idx + i);
            }
        }
    }

    indices
}

#[cfg(target_arch = "aarch64")]
pub fn filter_gte_i64(data: &[i64], threshold: i64) -> Vec<usize> {
    if data.is_empty() {
        return Vec::new();
    }

    let mut indices = Vec::with_capacity(data.len());

    unsafe {
        let threshold_vec = vdupq_n_s64(threshold);
        let chunks = data.chunks_exact(2);
        let remainder = chunks.remainder();
        let mut idx = 0;

        for chunk in chunks {
            let v = vld1q_s64(chunk.as_ptr());
            let cmp = vcgeq_s64(v, threshold_vec);

            let lane0 = vgetq_lane_u64(cmp, 0);
            let lane1 = vgetq_lane_u64(cmp, 1);

            if lane0 != 0 {
                indices.push(idx);
            }
            if lane1 != 0 {
                indices.push(idx + 1);
            }
            idx += 2;
        }

        for (i, &value) in remainder.iter().enumerate() {
            if value >= threshold {
                indices.push(idx + i);
            }
        }
    }

    indices
}

#[cfg(target_arch = "aarch64")]
pub fn filter_lte_i64(data: &[i64], threshold: i64) -> Vec<usize> {
    if data.is_empty() {
        return Vec::new();
    }

    let mut indices = Vec::with_capacity(data.len());

    unsafe {
        let threshold_vec = vdupq_n_s64(threshold);
        let chunks = data.chunks_exact(2);
        let remainder = chunks.remainder();
        let mut idx = 0;

        for chunk in chunks {
            let v = vld1q_s64(chunk.as_ptr());
            let cmp = vcleq_s64(v, threshold_vec);

            let lane0 = vgetq_lane_u64(cmp, 0);
            let lane1 = vgetq_lane_u64(cmp, 1);

            if lane0 != 0 {
                indices.push(idx);
            }
            if lane1 != 0 {
                indices.push(idx + 1);
            }
            idx += 2;
        }

        for (i, &value) in remainder.iter().enumerate() {
            if value <= threshold {
                indices.push(idx + i);
            }
        }
    }

    indices
}

#[cfg(target_arch = "aarch64")]
pub fn filter_ne_i64(data: &[i64], value: i64) -> Vec<usize> {
    if data.is_empty() {
        return Vec::new();
    }

    let mut indices = Vec::with_capacity(data.len());

    unsafe {
        let value_vec = vdupq_n_s64(value);
        let chunks = data.chunks_exact(2);
        let remainder = chunks.remainder();
        let mut idx = 0;

        for chunk in chunks {
            let v = vld1q_s64(chunk.as_ptr());
            let cmp = vceqq_s64(v, value_vec);

            let lane0 = vgetq_lane_u64(cmp, 0);
            let lane1 = vgetq_lane_u64(cmp, 1);

            if lane0 == 0 {
                indices.push(idx);
            }
            if lane1 == 0 {
                indices.push(idx + 1);
            }
            idx += 2;
        }

        for (i, &v) in remainder.iter().enumerate() {
            if v != value {
                indices.push(idx + i);
            }
        }
    }

    indices
}

#[cfg(target_arch = "aarch64")]
pub fn avg_i64(data: &[i64]) -> Option<f64> {
    if data.is_empty() {
        return None;
    }
    let sum = sum_i64(data);
    Some(sum as f64 / data.len() as f64)
}

#[cfg(target_arch = "aarch64")]
pub fn avg_f64(data: &[f64]) -> Option<f64> {
    if data.is_empty() {
        return None;
    }
    let sum = sum_f64(data);
    Some(sum / data.len() as f64)
}

#[cfg(target_arch = "aarch64")]
pub fn avg_i64_masked(data: &[i64], nulls: &[u8]) -> Option<f64> {
    assert_eq!(data.len(), nulls.len());
    let sum = sum_i64_masked(data, nulls);
    let count = nulls.iter().filter(|&&n| n != 0).count();
    if count == 0 {
        None
    } else {
        Some(sum as f64 / count as f64)
    }
}

#[cfg(target_arch = "aarch64")]
pub fn avg_f64_masked(data: &[f64], nulls: &[u8]) -> Option<f64> {
    assert_eq!(data.len(), nulls.len());
    let sum = sum_f64_masked(data, nulls);
    let count = nulls.iter().filter(|&&n| n != 0).count();
    if count == 0 {
        None
    } else {
        Some(sum / count as f64)
    }
}
