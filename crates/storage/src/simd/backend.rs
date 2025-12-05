pub trait SimdBackend {
    const NAME: &'static str;

    const VECTOR_WIDTH: usize;

    fn sum_i64(data: &[i64]) -> i64;
    fn sum_f64(data: &[f64]) -> f64;
    fn sum_i64_masked(data: &[i64], nulls: &[u8]) -> i64;
    fn sum_f64_masked(data: &[f64], nulls: &[u8]) -> f64;

    fn count_nonzero_i64(data: &[i64]) -> usize;
    fn min_i64(data: &[i64]) -> Option<i64>;
    fn max_i64(data: &[i64]) -> Option<i64>;

    fn filter_gt_i64(data: &[i64], threshold: i64) -> Vec<usize>;
    fn filter_gt_i64_with_nulls(data: &[i64], nulls: &[u8], threshold: i64) -> Vec<usize>;
    fn filter_gt_f64(data: &[f64], threshold: f64) -> Vec<usize>;

    fn filter_eq_i64(data: &[i64], value: i64) -> Vec<usize>;
    fn filter_lt_i64(data: &[i64], threshold: i64) -> Vec<usize>;
    fn filter_gte_i64(data: &[i64], threshold: i64) -> Vec<usize>;
    fn filter_lte_i64(data: &[i64], threshold: i64) -> Vec<usize>;
    fn filter_ne_i64(data: &[i64], value: i64) -> Vec<usize>;

    fn avg_i64(data: &[i64]) -> Option<f64>;
    fn avg_f64(data: &[f64]) -> Option<f64>;
    fn avg_i64_masked(data: &[i64], nulls: &[u8]) -> Option<f64>;
    fn avg_f64_masked(data: &[f64], nulls: &[u8]) -> Option<f64>;
}

#[cfg(target_arch = "aarch64")]
pub struct NeonBackend;

#[cfg(target_arch = "aarch64")]
impl SimdBackend for NeonBackend {
    const NAME: &'static str = "ARM NEON";
    const VECTOR_WIDTH: usize = 16;

    #[inline]
    fn sum_i64(data: &[i64]) -> i64 {
        super::scalar::sum_i64(data)
    }

    #[inline]
    fn sum_f64(data: &[f64]) -> f64 {
        super::neon::sum_f64(data)
    }

    #[inline]
    fn sum_i64_masked(data: &[i64], nulls: &[u8]) -> i64 {
        super::scalar::sum_i64_masked(data, nulls)
    }

    #[inline]
    fn sum_f64_masked(data: &[f64], nulls: &[u8]) -> f64 {
        super::neon::sum_f64_masked(data, nulls)
    }

    #[inline]
    fn count_nonzero_i64(data: &[i64]) -> usize {
        super::scalar::count_nonzero_i64(data)
    }

    #[inline]
    fn min_i64(data: &[i64]) -> Option<i64> {
        super::scalar::min_i64(data)
    }

    #[inline]
    fn max_i64(data: &[i64]) -> Option<i64> {
        super::scalar::max_i64(data)
    }

    #[inline]
    fn filter_gt_i64(data: &[i64], threshold: i64) -> Vec<usize> {
        super::neon::filter_gt_i64(data, threshold)
    }

    #[inline]
    fn filter_gt_i64_with_nulls(data: &[i64], nulls: &[u8], threshold: i64) -> Vec<usize> {
        super::neon::filter_gt_i64_with_nulls(data, nulls, threshold)
    }

    #[inline]
    fn filter_gt_f64(data: &[f64], threshold: f64) -> Vec<usize> {
        super::neon::filter_gt_f64(data, threshold)
    }

    #[inline]
    fn filter_eq_i64(data: &[i64], value: i64) -> Vec<usize> {
        super::neon::filter_eq_i64(data, value)
    }

    #[inline]
    fn filter_lt_i64(data: &[i64], threshold: i64) -> Vec<usize> {
        super::neon::filter_lt_i64(data, threshold)
    }

    #[inline]
    fn filter_gte_i64(data: &[i64], threshold: i64) -> Vec<usize> {
        super::neon::filter_gte_i64(data, threshold)
    }

    #[inline]
    fn filter_lte_i64(data: &[i64], threshold: i64) -> Vec<usize> {
        super::neon::filter_lte_i64(data, threshold)
    }

    #[inline]
    fn filter_ne_i64(data: &[i64], value: i64) -> Vec<usize> {
        super::neon::filter_ne_i64(data, value)
    }

    #[inline]
    fn avg_i64(data: &[i64]) -> Option<f64> {
        super::scalar::avg_i64(data)
    }

    #[inline]
    fn avg_f64(data: &[f64]) -> Option<f64> {
        super::neon::avg_f64(data)
    }

    #[inline]
    fn avg_i64_masked(data: &[i64], nulls: &[u8]) -> Option<f64> {
        super::scalar::avg_i64_masked(data, nulls)
    }

    #[inline]
    fn avg_f64_masked(data: &[f64], nulls: &[u8]) -> Option<f64> {
        super::neon::avg_f64_masked(data, nulls)
    }
}

#[cfg(target_arch = "x86_64")]
pub struct Avx2Backend;

#[cfg(target_arch = "x86_64")]
impl SimdBackend for Avx2Backend {
    const NAME: &'static str = "x86_64 AVX2";
    const VECTOR_WIDTH: usize = 32;

    #[inline]
    fn sum_i64(data: &[i64]) -> i64 {
        super::avx::sum_i64(data)
    }

    #[inline]
    fn sum_f64(data: &[f64]) -> f64 {
        super::avx::sum_f64(data)
    }

    #[inline]
    fn sum_i64_masked(data: &[i64], nulls: &[u8]) -> i64 {
        super::avx::sum_i64_masked(data, nulls)
    }

    #[inline]
    fn sum_f64_masked(data: &[f64], nulls: &[u8]) -> f64 {
        super::avx::sum_f64_masked(data, nulls)
    }

    #[inline]
    fn count_nonzero_i64(data: &[i64]) -> usize {
        super::avx::count_nonzero_i64(data)
    }

    #[inline]
    fn min_i64(data: &[i64]) -> Option<i64> {
        super::avx::min_i64(data)
    }

    #[inline]
    fn max_i64(data: &[i64]) -> Option<i64> {
        super::avx::max_i64(data)
    }

    #[inline]
    fn filter_gt_i64(data: &[i64], threshold: i64) -> Vec<usize> {
        super::avx::filter_gt_i64(data, threshold)
    }

    #[inline]
    fn filter_gt_i64_with_nulls(data: &[i64], nulls: &[u8], threshold: i64) -> Vec<usize> {
        super::avx::filter_gt_i64_with_nulls(data, nulls, threshold)
    }

    #[inline]
    fn filter_gt_f64(data: &[f64], threshold: f64) -> Vec<usize> {
        super::avx::filter_gt_f64(data, threshold)
    }

    #[inline]
    fn filter_eq_i64(data: &[i64], value: i64) -> Vec<usize> {
        super::avx::filter_eq_i64(data, value)
    }

    #[inline]
    fn filter_lt_i64(data: &[i64], threshold: i64) -> Vec<usize> {
        super::avx::filter_lt_i64(data, threshold)
    }

    #[inline]
    fn filter_gte_i64(data: &[i64], threshold: i64) -> Vec<usize> {
        super::avx::filter_gte_i64(data, threshold)
    }

    #[inline]
    fn filter_lte_i64(data: &[i64], threshold: i64) -> Vec<usize> {
        super::avx::filter_lte_i64(data, threshold)
    }

    #[inline]
    fn filter_ne_i64(data: &[i64], value: i64) -> Vec<usize> {
        super::avx::filter_ne_i64(data, value)
    }

    #[inline]
    fn avg_i64(data: &[i64]) -> Option<f64> {
        super::avx::avg_i64(data)
    }

    #[inline]
    fn avg_f64(data: &[f64]) -> Option<f64> {
        super::avx::avg_f64(data)
    }

    #[inline]
    fn avg_i64_masked(data: &[i64], nulls: &[u8]) -> Option<f64> {
        super::avx::avg_i64_masked(data, nulls)
    }

    #[inline]
    fn avg_f64_masked(data: &[f64], nulls: &[u8]) -> Option<f64> {
        super::avx::avg_f64_masked(data, nulls)
    }
}

pub struct ScalarBackend;

impl SimdBackend for ScalarBackend {
    const NAME: &'static str = "Scalar (no SIMD)";
    const VECTOR_WIDTH: usize = 8;

    #[inline]
    fn sum_i64(data: &[i64]) -> i64 {
        super::scalar::sum_i64(data)
    }

    #[inline]
    fn sum_f64(data: &[f64]) -> f64 {
        super::scalar::sum_f64(data)
    }

    #[inline]
    fn sum_i64_masked(data: &[i64], nulls: &[u8]) -> i64 {
        super::scalar::sum_i64_masked(data, nulls)
    }

    #[inline]
    fn sum_f64_masked(data: &[f64], nulls: &[u8]) -> f64 {
        super::scalar::sum_f64_masked(data, nulls)
    }

    #[inline]
    fn count_nonzero_i64(data: &[i64]) -> usize {
        super::scalar::count_nonzero_i64(data)
    }

    #[inline]
    fn min_i64(data: &[i64]) -> Option<i64> {
        super::scalar::min_i64(data)
    }

    #[inline]
    fn max_i64(data: &[i64]) -> Option<i64> {
        super::scalar::max_i64(data)
    }

    #[inline]
    fn filter_gt_i64(data: &[i64], threshold: i64) -> Vec<usize> {
        super::scalar::filter_gt_i64(data, threshold)
    }

    #[inline]
    fn filter_gt_i64_with_nulls(data: &[i64], nulls: &[u8], threshold: i64) -> Vec<usize> {
        super::scalar::filter_gt_i64_with_nulls(data, nulls, threshold)
    }

    #[inline]
    fn filter_gt_f64(data: &[f64], threshold: f64) -> Vec<usize> {
        super::scalar::filter_gt_f64(data, threshold)
    }

    #[inline]
    fn filter_eq_i64(data: &[i64], value: i64) -> Vec<usize> {
        super::scalar::filter_eq_i64(data, value)
    }

    #[inline]
    fn filter_lt_i64(data: &[i64], threshold: i64) -> Vec<usize> {
        super::scalar::filter_lt_i64(data, threshold)
    }

    #[inline]
    fn filter_gte_i64(data: &[i64], threshold: i64) -> Vec<usize> {
        super::scalar::filter_gte_i64(data, threshold)
    }

    #[inline]
    fn filter_lte_i64(data: &[i64], threshold: i64) -> Vec<usize> {
        super::scalar::filter_lte_i64(data, threshold)
    }

    #[inline]
    fn filter_ne_i64(data: &[i64], value: i64) -> Vec<usize> {
        super::scalar::filter_ne_i64(data, value)
    }

    #[inline]
    fn avg_i64(data: &[i64]) -> Option<f64> {
        super::scalar::avg_i64(data)
    }

    #[inline]
    fn avg_f64(data: &[f64]) -> Option<f64> {
        super::scalar::avg_f64(data)
    }

    #[inline]
    fn avg_i64_masked(data: &[i64], nulls: &[u8]) -> Option<f64> {
        super::scalar::avg_i64_masked(data, nulls)
    }

    #[inline]
    fn avg_f64_masked(data: &[f64], nulls: &[u8]) -> Option<f64> {
        super::scalar::avg_f64_masked(data, nulls)
    }
}

#[cfg(target_arch = "aarch64")]
pub type DefaultBackend = NeonBackend;

#[cfg(target_arch = "x86_64")]
pub type DefaultBackend = Avx2Backend;

#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
pub type DefaultBackend = ScalarBackend;

#[inline]
pub fn sum_i64(data: &[i64]) -> i64 {
    DefaultBackend::sum_i64(data)
}

#[inline]
pub fn sum_f64(data: &[f64]) -> f64 {
    DefaultBackend::sum_f64(data)
}

#[inline]
pub fn sum_i64_masked(data: &[i64], nulls: &[u8]) -> i64 {
    DefaultBackend::sum_i64_masked(data, nulls)
}

#[inline]
pub fn sum_f64_masked(data: &[f64], nulls: &[u8]) -> f64 {
    DefaultBackend::sum_f64_masked(data, nulls)
}

#[inline]
pub fn count_nonzero_i64(data: &[i64]) -> usize {
    DefaultBackend::count_nonzero_i64(data)
}

#[inline]
pub fn min_i64(data: &[i64]) -> Option<i64> {
    DefaultBackend::min_i64(data)
}

#[inline]
pub fn max_i64(data: &[i64]) -> Option<i64> {
    DefaultBackend::max_i64(data)
}

#[inline]
pub fn filter_gt_i64(data: &[i64], threshold: i64) -> Vec<usize> {
    DefaultBackend::filter_gt_i64(data, threshold)
}

#[inline]
pub fn filter_gt_i64_with_nulls(data: &[i64], nulls: &[u8], threshold: i64) -> Vec<usize> {
    DefaultBackend::filter_gt_i64_with_nulls(data, nulls, threshold)
}

#[inline]
pub fn filter_gt_f64(data: &[f64], threshold: f64) -> Vec<usize> {
    DefaultBackend::filter_gt_f64(data, threshold)
}

pub fn backend_info() -> (&'static str, usize) {
    (DefaultBackend::NAME, DefaultBackend::VECTOR_WIDTH)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_info() {
        let (name, width) = backend_info();
        println!("SIMD Backend: {}", name);
        println!("Vector Width: {} bytes", width);

        #[cfg(target_arch = "aarch64")]
        {
            assert_eq!(name, "ARM NEON");
            assert_eq!(width, 16);
        }

        #[cfg(target_arch = "x86_64")]
        {
            assert_eq!(name, "x86_64 AVX2");
            assert_eq!(width, 32);
        }
    }

    #[test]
    fn test_trait_dispatch() {
        let data = vec![1i64, 2, 3, 4, 5];

        let sum = DefaultBackend::sum_i64(&data);
        assert_eq!(sum, 15);

        let max = DefaultBackend::max_i64(&data);
        assert_eq!(max, Some(5));

        let min = DefaultBackend::min_i64(&data);
        assert_eq!(min, Some(1));
    }

    #[test]
    fn test_all_backends_produce_same_results() {
        let data = vec![10i64, 20, 30, 40, 50];
        let nulls = vec![1u8, 0, 1, 0, 1];

        let scalar_sum = ScalarBackend::sum_i64(&data);
        let default_sum = DefaultBackend::sum_i64(&data);
        assert_eq!(scalar_sum, default_sum);

        let scalar_masked = ScalarBackend::sum_i64_masked(&data, &nulls);
        let default_masked = DefaultBackend::sum_i64_masked(&data, &nulls);
        assert_eq!(scalar_masked, default_masked);
        assert_eq!(scalar_masked, 90);
    }
}
