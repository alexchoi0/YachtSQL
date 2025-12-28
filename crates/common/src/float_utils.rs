use std::cmp::Ordering;

pub const DEFAULT_EPSILON: f64 = 1e-10;

#[inline]
pub fn float_eq(a: f64, b: f64, epsilon: Option<f64>) -> bool {
    let eps = epsilon.unwrap_or(DEFAULT_EPSILON);

    if a.is_nan() || b.is_nan() {
        return false;
    }

    if a.is_infinite() && b.is_infinite() {
        return a.signum() == b.signum();
    }

    if a.is_infinite() || b.is_infinite() {
        return false;
    }

    if a == 0.0 && b == 0.0 {
        return true;
    }

    (a - b).abs() <= eps
}

#[inline]
pub fn float_cmp(a: &f64, b: &f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Less,
        (false, true) => return Ordering::Greater,
        _ => {}
    }

    match (a.is_infinite(), b.is_infinite()) {
        (true, true) => {
            return a.partial_cmp(b).unwrap_or(Ordering::Equal);
        }
        (true, false) => {
            return if *a > 0.0 {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        (false, true) => {
            return if *b > 0.0 {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        _ => {}
    }

    a.partial_cmp(b).unwrap_or(Ordering::Equal)
}

#[inline]
pub fn is_zero(value: f64, epsilon: Option<f64>) -> bool {
    let eps = epsilon.unwrap_or(DEFAULT_EPSILON);
    value.abs() <= eps
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn test_float_eq_basic() {
        assert!(float_eq(1.0, 1.0, None));
        assert!(float_eq(0.0, 0.0, None));

        assert!(!float_eq(1.0, 2.0, None));
        assert!(!float_eq(0.0, 1.0, None));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_float_eq_precision() {
        assert!(float_eq(0.1 + 0.2, 0.3, None));

        assert!(float_eq(1e308, 1e308 + 1.0, None));

        assert!(float_eq(1.0, 1.0 + 1e-11, None));
        assert!(!float_eq(1.0, 1.0 + 1e-9, None));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_float_eq_signed_zero() {
        assert!(float_eq(0.0, -0.0, None));
        assert!(float_eq(-0.0, 0.0, None));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_float_eq_infinity() {
        assert!(float_eq(f64::INFINITY, f64::INFINITY, None));
        assert!(float_eq(f64::NEG_INFINITY, f64::NEG_INFINITY, None));

        assert!(!float_eq(f64::INFINITY, f64::NEG_INFINITY, None));

        assert!(!float_eq(f64::INFINITY, 1e308, None));
        assert!(!float_eq(f64::NEG_INFINITY, -1e308, None));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_float_eq_nan() {
        assert!(!float_eq(f64::NAN, f64::NAN, None));

        assert!(!float_eq(f64::NAN, 0.0, None));
        assert!(!float_eq(f64::NAN, f64::INFINITY, None));
        assert!(!float_eq(0.0, f64::NAN, None));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_float_eq_custom_epsilon() {
        assert!(float_eq(1.0, 1.5, Some(1.0)));
        assert!(!float_eq(1.0, 2.5, Some(1.0)));

        assert!(!float_eq(1.0, 1.0 + 1e-15, Some(1e-20)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_float_cmp_basic() {
        assert_eq!(float_cmp(&1.0, &2.0), Ordering::Less);
        assert_eq!(float_cmp(&2.0, &1.0), Ordering::Greater);
        assert_eq!(float_cmp(&1.0, &1.0), Ordering::Equal);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_float_cmp_signed_zero() {
        assert_eq!(float_cmp(&0.0, &-0.0), Ordering::Equal);
        assert_eq!(float_cmp(&-0.0, &0.0), Ordering::Equal);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_float_cmp_infinity() {
        assert_eq!(float_cmp(&f64::INFINITY, &1.0), Ordering::Greater);
        assert_eq!(float_cmp(&1.0, &f64::INFINITY), Ordering::Less);
        assert_eq!(float_cmp(&f64::INFINITY, &f64::INFINITY), Ordering::Equal);

        assert_eq!(float_cmp(&f64::NEG_INFINITY, &1.0), Ordering::Less);
        assert_eq!(float_cmp(&1.0, &f64::NEG_INFINITY), Ordering::Greater);
        assert_eq!(
            float_cmp(&f64::NEG_INFINITY, &f64::NEG_INFINITY),
            Ordering::Equal
        );

        assert_eq!(
            float_cmp(&f64::NEG_INFINITY, &f64::INFINITY),
            Ordering::Less
        );
        assert_eq!(
            float_cmp(&f64::INFINITY, &f64::NEG_INFINITY),
            Ordering::Greater
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_float_cmp_nan() {
        assert_eq!(float_cmp(&f64::NAN, &f64::NAN), Ordering::Equal);

        assert_eq!(float_cmp(&f64::NAN, &0.0), Ordering::Less);
        assert_eq!(float_cmp(&f64::NAN, &1.0), Ordering::Less);
        assert_eq!(float_cmp(&f64::NAN, &f64::INFINITY), Ordering::Less);
        assert_eq!(float_cmp(&f64::NAN, &f64::NEG_INFINITY), Ordering::Less);

        assert_eq!(float_cmp(&0.0, &f64::NAN), Ordering::Greater);
        assert_eq!(float_cmp(&1.0, &f64::NAN), Ordering::Greater);
        assert_eq!(float_cmp(&f64::INFINITY, &f64::NAN), Ordering::Greater);
        assert_eq!(float_cmp(&f64::NEG_INFINITY, &f64::NAN), Ordering::Greater);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_is_zero() {
        assert!(is_zero(0.0, None));
        assert!(is_zero(-0.0, None));

        assert!(is_zero(1e-15, None));
        assert!(is_zero(-1e-15, None));

        assert!(!is_zero(1.0, None));
        assert!(!is_zero(0.1, None));

        assert!(is_zero(0.5, Some(1.0)));
        assert!(!is_zero(1.5, Some(1.0)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_float_arithmetic_precision() {
        let result = 0.1 + 0.2;
        assert!(float_eq(result, 0.3, None));

        let large = 1e308;
        let result = large + 1.0;
        assert!(float_eq(result, large, None));

        let mut sum = 0.0;
        for _ in 0..10 {
            sum += 0.1;
        }
        assert!(float_eq(sum, 1.0, None));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_float_transitivity() {
        let a = 1.0;
        let b = 1.0 + 0.5e-10;
        let c = 1.0 + 0.9e-10;

        assert!(float_eq(a, b, None));
        assert!(float_eq(b, c, None));
    }
}
