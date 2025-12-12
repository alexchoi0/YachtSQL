use yachtsql_common::error::{Error, Result};

use crate::{Column, simd};

impl Column {
    pub fn sum_i64(&self) -> Result<i64> {
        match self {
            Column::Int64 { data, nulls } => {
                if data.is_empty() {
                    return Err(Error::invalid_operation(
                        "Cannot compute sum of empty column",
                    ));
                }

                let null_count = nulls.count_null();

                if null_count == nulls.len() {
                    return Ok(0);
                }

                if null_count == 0 {
                    return Ok(simd::sum_i64(data.as_slice()));
                }

                let mut sum = 0i64;
                let null_data = nulls.as_slice();
                for i in 0..data.len() {
                    sum += data[i] * (null_data[i] as i64);
                }
                Ok(sum)
            }
            _ => Err(Error::type_mismatch("Int64", self.data_type())),
        }
    }

    pub fn sum_f64(&self) -> Result<f64> {
        match self {
            Column::Float64 { data, nulls } => {
                if data.is_empty() {
                    return Err(Error::invalid_operation(
                        "Cannot compute sum of empty column",
                    ));
                }

                let null_count = nulls.count_null();

                if null_count == nulls.len() {
                    return Ok(0.0);
                }

                if null_count == 0 {
                    return Ok(simd::sum_f64(data.as_slice()));
                }

                Ok(simd::sum_f64_masked(data.as_slice(), nulls.as_slice()))
            }
            _ => Err(Error::type_mismatch("Float64", self.data_type())),
        }
    }

    pub fn min_i64(&self) -> Result<Option<i64>> {
        match self {
            Column::Int64 { data, nulls } => {
                let null_count = nulls.count_null();

                if null_count == nulls.len() {
                    return Ok(None);
                }

                if null_count == 0 {
                    return Ok(simd::min_i64(data.as_slice()));
                }

                let mut min = i64::MAX;
                for i in 0..data.len() {
                    if !nulls.is_null(i) {
                        min = min.min(data[i]);
                    }
                }
                Ok(Some(min))
            }
            _ => Err(Error::type_mismatch("Int64", self.data_type())),
        }
    }

    pub fn max_i64(&self) -> Result<Option<i64>> {
        match self {
            Column::Int64 { data, nulls } => {
                let null_count = nulls.count_null();

                if null_count == nulls.len() {
                    return Ok(None);
                }

                if null_count == 0 {
                    return Ok(simd::max_i64(data.as_slice()));
                }

                let mut max = i64::MIN;
                for i in 0..data.len() {
                    if !nulls.is_null(i) {
                        max = max.max(data[i]);
                    }
                }
                Ok(Some(max))
            }
            _ => Err(Error::type_mismatch("Int64", self.data_type())),
        }
    }

    pub fn count_nonzero_i64(&self) -> Result<usize> {
        match self {
            Column::Int64 { data, nulls } => {
                let null_count = nulls.count_null();

                if null_count == nulls.len() {
                    return Ok(0);
                }

                if null_count == 0 {
                    return Ok(simd::count_nonzero_i64(data.as_slice()));
                }

                let mut count = 0usize;
                let null_data = nulls.as_slice();
                for i in 0..data.len() {
                    count += ((data[i] != 0) as usize) * (null_data[i] as usize);
                }

                Ok(count)
            }
            _ => Err(Error::type_mismatch("Int64", self.data_type())),
        }
    }

    pub fn filter_gt_i64(&self, threshold: i64) -> Result<Vec<usize>> {
        match self {
            Column::Int64 { data, nulls } => {
                let null_count = nulls.count_null();

                if null_count == nulls.len() {
                    return Ok(Vec::new());
                }

                if null_count == 0 {
                    return Ok(simd::filter_gt_i64(data.as_slice(), threshold));
                }

                Ok(simd::filter_gt_i64_with_nulls(
                    data.as_slice(),
                    nulls.as_slice(),
                    threshold,
                ))
            }
            _ => Err(Error::type_mismatch("Int64", self.data_type())),
        }
    }

    pub fn filter_gt_f64(&self, threshold: f64) -> Result<Vec<usize>> {
        match self {
            Column::Float64 { data, nulls } => {
                let null_count = nulls.count_null();

                if null_count == nulls.len() {
                    return Ok(Vec::new());
                }

                if null_count == 0 {
                    return Ok(simd::filter_gt_f64(data.as_slice(), threshold));
                }

                let simd_indices = simd::filter_gt_f64(data.as_slice(), threshold);
                let filtered: Vec<usize> = simd_indices
                    .into_iter()
                    .filter(|&i| !nulls.is_null(i))
                    .collect();

                Ok(filtered)
            }
            _ => Err(Error::type_mismatch("Float64", self.data_type())),
        }
    }

    pub fn filter_eq_i64(&self, value: i64) -> Result<Vec<usize>> {
        match self {
            Column::Int64 { data, nulls } => {
                let null_count = nulls.count_null();

                if null_count == nulls.len() {
                    return Ok(Vec::new());
                }

                if null_count == 0 {
                    return Ok(simd::filter_eq_i64(data.as_slice(), value));
                }

                let simd_indices = simd::filter_eq_i64(data.as_slice(), value);
                let filtered: Vec<usize> = simd_indices
                    .into_iter()
                    .filter(|&i| !nulls.is_null(i))
                    .collect();

                Ok(filtered)
            }
            _ => Err(Error::type_mismatch("Int64", self.data_type())),
        }
    }

    pub fn filter_lt_i64(&self, threshold: i64) -> Result<Vec<usize>> {
        match self {
            Column::Int64 { data, nulls } => {
                let null_count = nulls.count_null();

                if null_count == nulls.len() {
                    return Ok(Vec::new());
                }

                if null_count == 0 {
                    return Ok(simd::filter_lt_i64(data.as_slice(), threshold));
                }

                let simd_indices = simd::filter_lt_i64(data.as_slice(), threshold);
                let filtered: Vec<usize> = simd_indices
                    .into_iter()
                    .filter(|&i| !nulls.is_null(i))
                    .collect();

                Ok(filtered)
            }
            _ => Err(Error::type_mismatch("Int64", self.data_type())),
        }
    }

    pub fn filter_gte_i64(&self, threshold: i64) -> Result<Vec<usize>> {
        match self {
            Column::Int64 { data, nulls } => {
                let null_count = nulls.count_null();

                if null_count == nulls.len() {
                    return Ok(Vec::new());
                }

                if null_count == 0 {
                    return Ok(simd::filter_gte_i64(data.as_slice(), threshold));
                }

                let simd_indices = simd::filter_gte_i64(data.as_slice(), threshold);
                let filtered: Vec<usize> = simd_indices
                    .into_iter()
                    .filter(|&i| !nulls.is_null(i))
                    .collect();

                Ok(filtered)
            }
            _ => Err(Error::type_mismatch("Int64", self.data_type())),
        }
    }

    pub fn filter_lte_i64(&self, threshold: i64) -> Result<Vec<usize>> {
        match self {
            Column::Int64 { data, nulls } => {
                let null_count = nulls.count_null();

                if null_count == nulls.len() {
                    return Ok(Vec::new());
                }

                if null_count == 0 {
                    return Ok(simd::filter_lte_i64(data.as_slice(), threshold));
                }

                let simd_indices = simd::filter_lte_i64(data.as_slice(), threshold);
                let filtered: Vec<usize> = simd_indices
                    .into_iter()
                    .filter(|&i| !nulls.is_null(i))
                    .collect();

                Ok(filtered)
            }
            _ => Err(Error::type_mismatch("Int64", self.data_type())),
        }
    }

    pub fn filter_ne_i64(&self, value: i64) -> Result<Vec<usize>> {
        match self {
            Column::Int64 { data, nulls } => {
                let null_count = nulls.count_null();

                if null_count == nulls.len() {
                    return Ok(Vec::new());
                }

                if null_count == 0 {
                    return Ok(simd::filter_ne_i64(data.as_slice(), value));
                }

                let simd_indices = simd::filter_ne_i64(data.as_slice(), value);
                let filtered: Vec<usize> = simd_indices
                    .into_iter()
                    .filter(|&i| !nulls.is_null(i))
                    .collect();

                Ok(filtered)
            }
            _ => Err(Error::type_mismatch("Int64", self.data_type())),
        }
    }

    pub fn avg_i64(&self) -> Result<Option<f64>> {
        match self {
            Column::Int64 { data, nulls } => {
                let null_count = nulls.count_null();

                if null_count == nulls.len() {
                    return Ok(None);
                }

                if null_count == 0 {
                    return Ok(simd::avg_i64(data.as_slice()));
                }

                Ok(simd::avg_i64_masked(data.as_slice(), nulls.as_slice()))
            }
            _ => Err(Error::type_mismatch("Int64", self.data_type())),
        }
    }

    pub fn avg_f64(&self) -> Result<Option<f64>> {
        match self {
            Column::Float64 { data, nulls } => {
                let null_count = nulls.count_null();

                if null_count == nulls.len() {
                    return Ok(None);
                }

                if null_count == 0 {
                    return Ok(simd::avg_f64(data.as_slice()));
                }

                Ok(simd::avg_f64_masked(data.as_slice(), nulls.as_slice()))
            }
            _ => Err(Error::type_mismatch("Float64", self.data_type())),
        }
    }

    pub fn variance_pop_f64(&self) -> Result<Option<f64>> {
        match self {
            Column::Float64 { data, nulls } => {
                let mean = match self.avg_f64()? {
                    Some(m) => m,
                    None => return Ok(None),
                };

                let null_count = nulls.count_null();

                let sum_sq_diff = if null_count == 0 {
                    let mut sum = 0.0;
                    for &val in data.as_slice() {
                        let diff = val - mean;
                        sum += diff * diff;
                    }
                    sum
                } else {
                    let mut sum = 0.0;
                    let null_data = nulls.as_slice();
                    for i in 0..data.len() {
                        if null_data[i] != 0 {
                            let diff = data[i] - mean;
                            sum += diff * diff;
                        }
                    }
                    sum
                };

                let count = (data.len() - null_count) as f64;
                Ok(Some(sum_sq_diff / count))
            }
            _ => Err(Error::type_mismatch("Float64", self.data_type())),
        }
    }

    pub fn variance_samp_f64(&self) -> Result<Option<f64>> {
        match self {
            Column::Float64 { data, nulls } => {
                let null_count = nulls.count_null();
                let count = data.len() - null_count;

                if count <= 1 {
                    return Ok(None);
                }

                let mean = match self.avg_f64()? {
                    Some(m) => m,
                    None => return Ok(None),
                };

                let sum_sq_diff = if null_count == 0 {
                    let mut sum = 0.0;
                    for &val in data.as_slice() {
                        let diff = val - mean;
                        sum += diff * diff;
                    }
                    sum
                } else {
                    let mut sum = 0.0;
                    let null_data = nulls.as_slice();
                    for i in 0..data.len() {
                        if null_data[i] != 0 {
                            let diff = data[i] - mean;
                            sum += diff * diff;
                        }
                    }
                    sum
                };

                Ok(Some(sum_sq_diff / (count - 1) as f64))
            }
            _ => Err(Error::type_mismatch("Float64", self.data_type())),
        }
    }

    pub fn stddev_pop_f64(&self) -> Result<Option<f64>> {
        match self.variance_pop_f64()? {
            Some(var) => Ok(Some(var.sqrt())),
            None => Ok(None),
        }
    }

    pub fn stddev_samp_f64(&self) -> Result<Option<f64>> {
        match self.variance_samp_f64()? {
            Some(var) => Ok(Some(var.sqrt())),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use aligned_vec::AVec;

    use super::*;
    use crate::NullBitmap;

    #[test]
    fn test_sum_i64_no_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[1, 2, 3, 4, 5]);
        let column = Column::Int64 {
            data,
            nulls: NullBitmap::new_valid(5),
        };

        assert_eq!(column.sum_i64().unwrap(), 15);
    }

    #[test]
    fn test_sum_i64_with_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[1, 2, 3, 4, 5]);
        let mut nulls = NullBitmap::new_valid(5);
        nulls.set_null(1);

        let column = Column::Int64 { data, nulls };

        assert_eq!(column.sum_i64().unwrap(), 13);
    }

    #[test]
    fn test_sum_f64_no_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let column = Column::Float64 {
            data,
            nulls: NullBitmap::new_valid(5),
        };

        assert_eq!(column.sum_f64().unwrap(), 15.0);
    }

    #[test]
    fn test_filter_gt_i64_no_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[1, 2, 3, 4, 5]);
        let column = Column::Int64 {
            data,
            nulls: NullBitmap::new_valid(5),
        };

        let indices = column.filter_gt_i64(2).unwrap();
        assert_eq!(indices, vec![2, 3, 4]);
    }

    #[test]
    fn test_filter_gt_i64_with_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[1, 2, 3, 4, 5]);
        let mut nulls = NullBitmap::new_valid(5);
        nulls.set_null(3);

        let column = Column::Int64 { data, nulls };

        let indices = column.filter_gt_i64(2).unwrap();
        assert_eq!(indices, vec![2, 4]);
    }

    #[test]
    fn test_min_max_i64() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[5, 2, 8, 1, 3]);
        let column = Column::Int64 {
            data,
            nulls: NullBitmap::new_valid(5),
        };

        assert_eq!(column.min_i64().unwrap(), Some(1));
        assert_eq!(column.max_i64().unwrap(), Some(8));
    }

    #[test]
    fn test_count_nonzero_i64() {
        let mut data = AVec::with_capacity(64, 6);
        data.extend_from_slice(&[0, 1, 0, 2, 3, 0]);
        let column = Column::Int64 {
            data,
            nulls: NullBitmap::new_valid(6),
        };

        assert_eq!(column.count_nonzero_i64().unwrap(), 3);
    }

    #[test]
    fn test_filter_eq_i64_no_nulls() {
        let mut data = AVec::with_capacity(64, 6);
        data.extend_from_slice(&[1, 2, 3, 2, 5, 2]);
        let column = Column::Int64 {
            data,
            nulls: NullBitmap::new_valid(6),
        };

        let indices = column.filter_eq_i64(2).unwrap();
        assert_eq!(indices, vec![1, 3, 5]);
    }

    #[test]
    fn test_filter_eq_i64_with_nulls() {
        let mut data = AVec::with_capacity(64, 6);
        data.extend_from_slice(&[1, 2, 3, 2, 5, 2]);
        let mut nulls = NullBitmap::new_valid(6);
        nulls.set_null(1);

        let column = Column::Int64 { data, nulls };

        let indices = column.filter_eq_i64(2).unwrap();
        assert_eq!(indices, vec![3, 5]);
    }

    #[test]
    fn test_filter_lt_i64_no_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[1, 2, 3, 4, 5]);
        let column = Column::Int64 {
            data,
            nulls: NullBitmap::new_valid(5),
        };

        let indices = column.filter_lt_i64(3).unwrap();
        assert_eq!(indices, vec![0, 1]);
    }

    #[test]
    fn test_filter_lt_i64_with_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[1, 2, 3, 4, 5]);
        let mut nulls = NullBitmap::new_valid(5);
        nulls.set_null(0);

        let column = Column::Int64 { data, nulls };

        let indices = column.filter_lt_i64(3).unwrap();
        assert_eq!(indices, vec![1]);
    }

    #[test]
    fn test_filter_gte_i64_no_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[1, 2, 3, 4, 5]);
        let column = Column::Int64 {
            data,
            nulls: NullBitmap::new_valid(5),
        };

        let indices = column.filter_gte_i64(3).unwrap();
        assert_eq!(indices, vec![2, 3, 4]);
    }

    #[test]
    fn test_filter_gte_i64_with_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[1, 2, 3, 4, 5]);
        let mut nulls = NullBitmap::new_valid(5);
        nulls.set_null(2);

        let column = Column::Int64 { data, nulls };

        let indices = column.filter_gte_i64(3).unwrap();
        assert_eq!(indices, vec![3, 4]);
    }

    #[test]
    fn test_filter_lte_i64_no_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[1, 2, 3, 4, 5]);
        let column = Column::Int64 {
            data,
            nulls: NullBitmap::new_valid(5),
        };

        let indices = column.filter_lte_i64(3).unwrap();
        assert_eq!(indices, vec![0, 1, 2]);
    }

    #[test]
    fn test_filter_lte_i64_with_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[1, 2, 3, 4, 5]);
        let mut nulls = NullBitmap::new_valid(5);
        nulls.set_null(1);

        let column = Column::Int64 { data, nulls };

        let indices = column.filter_lte_i64(3).unwrap();
        assert_eq!(indices, vec![0, 2]);
    }

    #[test]
    fn test_filter_ne_i64_no_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[1, 2, 3, 2, 5]);
        let column = Column::Int64 {
            data,
            nulls: NullBitmap::new_valid(5),
        };

        let indices = column.filter_ne_i64(2).unwrap();
        assert_eq!(indices, vec![0, 2, 4]);
    }

    #[test]
    fn test_filter_ne_i64_with_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[1, 2, 3, 2, 5]);
        let mut nulls = NullBitmap::new_valid(5);
        nulls.set_null(0);

        let column = Column::Int64 { data, nulls };

        let indices = column.filter_ne_i64(2).unwrap();
        assert_eq!(indices, vec![2, 4]);
    }

    #[test]
    fn test_avg_i64_no_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[1, 2, 3, 4, 5]);
        let column = Column::Int64 {
            data,
            nulls: NullBitmap::new_valid(5),
        };

        assert_eq!(column.avg_i64().unwrap(), Some(3.0));
    }

    #[test]
    fn test_avg_i64_with_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[1, 2, 3, 4, 5]);
        let mut nulls = NullBitmap::new_valid(5);
        nulls.set_null(1);

        let column = Column::Int64 { data, nulls };

        assert_eq!(column.avg_i64().unwrap(), Some(3.25));
    }

    #[test]
    fn test_avg_i64_all_nulls() {
        let mut data = AVec::with_capacity(64, 3);
        data.extend_from_slice(&[1, 2, 3]);
        let mut nulls = NullBitmap::new_valid(3);
        nulls.set_null(0);
        nulls.set_null(1);
        nulls.set_null(2);

        let column = Column::Int64 { data, nulls };

        assert_eq!(column.avg_i64().unwrap(), None);
    }

    #[test]
    fn test_avg_f64_no_nulls() {
        let mut data = AVec::with_capacity(64, 4);
        data.extend_from_slice(&[1.0, 2.0, 3.0, 4.0]);
        let column = Column::Float64 {
            data,
            nulls: NullBitmap::new_valid(4),
        };

        assert_eq!(column.avg_f64().unwrap(), Some(2.5));
    }

    #[test]
    fn test_avg_f64_with_nulls() {
        let mut data = AVec::with_capacity(64, 4);
        data.extend_from_slice(&[1.0, 2.0, 3.0, 4.0]);
        let mut nulls = NullBitmap::new_valid(4);
        nulls.set_null(0);

        let column = Column::Float64 { data, nulls };

        assert_eq!(column.avg_f64().unwrap(), Some(3.0));
    }

    #[test]
    fn test_variance_pop_f64_no_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        let column = Column::Float64 {
            data,
            nulls: NullBitmap::new_valid(8),
        };

        let var = column.variance_pop_f64().unwrap().unwrap();
        assert!((var - 4.0).abs() < 0.0001);
    }

    #[test]
    fn test_variance_samp_f64_no_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        let column = Column::Float64 {
            data,
            nulls: NullBitmap::new_valid(8),
        };

        let var = column.variance_samp_f64().unwrap().unwrap();
        assert!((var - 4.571428).abs() < 0.0001);
    }

    #[test]
    fn test_stddev_pop_f64_no_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        let column = Column::Float64 {
            data,
            nulls: NullBitmap::new_valid(8),
        };

        let stddev = column.stddev_pop_f64().unwrap().unwrap();
        assert!((stddev - 2.0).abs() < 0.0001);
    }

    #[test]
    fn test_stddev_samp_f64_no_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        let column = Column::Float64 {
            data,
            nulls: NullBitmap::new_valid(8),
        };

        let stddev = column.stddev_samp_f64().unwrap().unwrap();
        assert!((stddev - 2.138089).abs() < 0.0001);
    }

    #[test]
    fn test_variance_f64_with_nulls() {
        let mut data = AVec::with_capacity(64, 5);
        data.extend_from_slice(&[2.0, 4.0, 4.0, 4.0, 5.0]);
        let mut nulls = NullBitmap::new_valid(5);
        nulls.set_null(0);

        let column = Column::Float64 { data, nulls };

        let var = column.variance_pop_f64().unwrap().unwrap();
        assert!((var - 0.1875).abs() < 0.0001);
    }

    #[test]
    fn test_variance_samp_f64_one_value() {
        let mut data = AVec::with_capacity(64, 1);
        data.extend_from_slice(&[5.0]);
        let column = Column::Float64 {
            data,
            nulls: NullBitmap::new_valid(1),
        };

        assert_eq!(column.variance_samp_f64().unwrap(), None);
    }

    #[test]
    fn test_stddev_f64_all_nulls() {
        let mut data = AVec::with_capacity(64, 3);
        data.extend_from_slice(&[1.0, 2.0, 3.0]);
        let mut nulls = NullBitmap::new_valid(3);
        nulls.set_null(0);
        nulls.set_null(1);
        nulls.set_null(2);

        let column = Column::Float64 { data, nulls };

        assert_eq!(column.stddev_pop_f64().unwrap(), None);
    }
}
