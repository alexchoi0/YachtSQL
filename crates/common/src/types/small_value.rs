use std::fmt;

use serde::{Deserialize, Serialize};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ValueTag {
    Null = 0,

    Bool = 1,

    Int64 = 2,

    Float64 = 3,

    Date = 4,

    Time = 5,

    DateTime = 6,

    Timestamp = 7,

    SmallString = 8,
}

#[repr(C)]
#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct SmallValue {
    pub(crate) tag: ValueTag,

    pub(crate) data: [u8; 15],
}

impl SmallValue {
    #[inline]
    pub const fn null() -> Self {
        Self {
            tag: ValueTag::Null,
            data: [0; 15],
        }
    }

    #[inline]
    pub const fn bool(value: bool) -> Self {
        let mut data = [0u8; 15];
        data[0] = value as u8;
        Self {
            tag: ValueTag::Bool,
            data,
        }
    }

    #[inline]
    pub fn int64(value: i64) -> Self {
        let mut data = [0u8; 15];
        data[0..8].copy_from_slice(&value.to_le_bytes());
        Self {
            tag: ValueTag::Int64,
            data,
        }
    }

    #[inline]
    pub fn float64(value: f64) -> Self {
        let mut data = [0u8; 15];
        data[0..8].copy_from_slice(&value.to_le_bytes());
        Self {
            tag: ValueTag::Float64,
            data,
        }
    }

    #[inline]
    pub fn small_string(s: &str) -> Option<Self> {
        let bytes = s.as_bytes();
        if bytes.len() > 14 {
            return None;
        }

        let mut data = [0u8; 15];
        data[0] = bytes.len() as u8;
        data[1..1 + bytes.len()].copy_from_slice(bytes);

        Some(Self {
            tag: ValueTag::SmallString,
            data,
        })
    }

    #[inline]
    pub fn date(days_since_epoch: i32) -> Self {
        let mut data = [0u8; 15];
        data[0..4].copy_from_slice(&days_since_epoch.to_le_bytes());
        Self {
            tag: ValueTag::Date,
            data,
        }
    }

    #[inline]
    pub fn time(nanos: i64) -> Self {
        let mut data = [0u8; 15];
        data[0..8].copy_from_slice(&nanos.to_le_bytes());
        Self {
            tag: ValueTag::Time,
            data,
        }
    }

    #[inline]
    pub fn datetime(micros: i64) -> Self {
        let mut data = [0u8; 15];
        data[0..8].copy_from_slice(&micros.to_le_bytes());
        Self {
            tag: ValueTag::DateTime,
            data,
        }
    }

    #[inline]
    pub fn timestamp(micros: i64) -> Self {
        let mut data = [0u8; 15];
        data[0..8].copy_from_slice(&micros.to_le_bytes());
        Self {
            tag: ValueTag::Timestamp,
            data,
        }
    }

    #[inline]
    pub const fn tag(&self) -> ValueTag {
        self.tag
    }

    #[inline]
    pub const fn is_null(&self) -> bool {
        matches!(self.tag, ValueTag::Null)
    }

    #[inline]
    pub const fn as_bool(&self) -> Option<bool> {
        if matches!(self.tag, ValueTag::Bool) {
            Some(self.data[0] != 0)
        } else {
            None
        }
    }

    #[inline]
    pub fn as_int64(&self) -> Option<i64> {
        if matches!(self.tag, ValueTag::Int64) {
            let bytes: [u8; 8] = self.data[0..8].try_into().ok()?;
            Some(i64::from_le_bytes(bytes))
        } else {
            None
        }
    }

    #[inline]
    pub fn as_float64(&self) -> Option<f64> {
        if matches!(self.tag, ValueTag::Float64) {
            let bytes: [u8; 8] = self.data[0..8].try_into().ok()?;
            Some(f64::from_le_bytes(bytes))
        } else {
            None
        }
    }

    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        if matches!(self.tag, ValueTag::SmallString) {
            let len = self.data[0] as usize;
            if len > 14 {
                return None;
            }
            std::str::from_utf8(&self.data[1..1 + len]).ok()
        } else {
            None
        }
    }

    #[inline]
    pub fn as_date(&self) -> Option<i32> {
        if matches!(self.tag, ValueTag::Date) {
            let bytes: [u8; 4] = self.data[0..4].try_into().ok()?;
            Some(i32::from_le_bytes(bytes))
        } else {
            None
        }
    }

    #[inline]
    pub fn as_time(&self) -> Option<i64> {
        if matches!(self.tag, ValueTag::Time) {
            let bytes: [u8; 8] = self.data[0..8].try_into().ok()?;
            Some(i64::from_le_bytes(bytes))
        } else {
            None
        }
    }

    #[inline]
    pub fn as_datetime(&self) -> Option<i64> {
        if matches!(self.tag, ValueTag::DateTime) {
            let bytes: [u8; 8] = self.data[0..8].try_into().ok()?;
            Some(i64::from_le_bytes(bytes))
        } else {
            None
        }
    }

    #[inline]
    pub fn as_timestamp(&self) -> Option<i64> {
        if matches!(self.tag, ValueTag::Timestamp) {
            let bytes: [u8; 8] = self.data[0..8].try_into().ok()?;
            Some(i64::from_le_bytes(bytes))
        } else {
            None
        }
    }
}

impl fmt::Debug for SmallValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.tag {
            ValueTag::Null => write!(f, "NULL"),
            ValueTag::Bool => write!(f, "Bool({})", self.as_bool().expect("tag is Bool")),
            ValueTag::Int64 => write!(f, "Int64({})", self.as_int64().expect("tag is Int64")),
            ValueTag::Float64 => {
                write!(f, "Float64({})", self.as_float64().expect("tag is Float64"))
            }
            ValueTag::SmallString => {
                write!(
                    f,
                    "String({:?})",
                    self.as_str().expect("tag is SmallString")
                )
            }
            ValueTag::Date => write!(f, "Date({})", self.as_date().expect("tag is Date")),
            _ => write!(f, "SmallValue({:?})", self.tag),
        }
    }
}

impl PartialEq for SmallValue {
    fn eq(&self, other: &Self) -> bool {
        if self.tag != other.tag {
            return false;
        }

        match self.tag {
            ValueTag::Null => true,
            ValueTag::Bool => self.as_bool() == other.as_bool(),
            ValueTag::Int64 => self.as_int64() == other.as_int64(),
            ValueTag::Float64 => {
                let a = self.as_float64();
                let b = other.as_float64();
                match (a, b) {
                    (Some(a_val), Some(b_val)) => crate::float_utils::float_eq(a_val, b_val, None),
                    _ => false,
                }
            }
            ValueTag::SmallString => self.as_str() == other.as_str(),
            ValueTag::Date => self.as_date() == other.as_date(),
            _ => self.data == other.data,
        }
    }
}

impl Eq for SmallValue {}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use std::mem;

    use super::*;

    #[test]
    fn test_size() {
        assert_eq!(mem::size_of::<SmallValue>(), 16);
        assert_eq!(mem::align_of::<SmallValue>(), 1);
    }

    #[test]
    fn test_null() {
        let v = SmallValue::null();
        assert!(v.is_null());
        assert_eq!(v.tag(), ValueTag::Null);
    }

    #[test]
    fn test_bool() {
        let v_true = SmallValue::bool(true);
        let v_false = SmallValue::bool(false);

        assert_eq!(v_true.as_bool(), Some(true));
        assert_eq!(v_false.as_bool(), Some(false));
        assert_eq!(v_true.tag(), ValueTag::Bool);
    }

    #[test]
    fn test_int64() {
        let v = SmallValue::int64(42);
        assert_eq!(v.as_int64(), Some(42));
        assert_eq!(v.tag(), ValueTag::Int64);

        let v_neg = SmallValue::int64(-100);
        assert_eq!(v_neg.as_int64(), Some(-100));

        let v_max = SmallValue::int64(i64::MAX);
        assert_eq!(v_max.as_int64(), Some(i64::MAX));

        let v_min = SmallValue::int64(i64::MIN);
        assert_eq!(v_min.as_int64(), Some(i64::MIN));
    }

    #[test]
    fn test_float64() {
        let v = SmallValue::float64(3.14);
        assert_eq!(v.as_float64(), Some(3.14));
        assert_eq!(v.tag(), ValueTag::Float64);

        let v_neg = SmallValue::float64(-2.5);
        assert_eq!(v_neg.as_float64(), Some(-2.5));

        let v_zero = SmallValue::float64(0.0);
        assert_eq!(v_zero.as_float64(), Some(0.0));
    }

    #[test]
    fn test_small_string() {
        let v = SmallValue::small_string("hello").unwrap();
        assert_eq!(v.as_str(), Some("hello"));
        assert_eq!(v.tag(), ValueTag::SmallString);

        let v_empty = SmallValue::small_string("").unwrap();
        assert_eq!(v_empty.as_str(), Some(""));

        let v_max = SmallValue::small_string("12345678901234").unwrap();
        assert_eq!(v_max.as_str(), Some("12345678901234"));

        let v_too_large = SmallValue::small_string("123456789012345");
        assert!(v_too_large.is_none());
    }

    #[test]
    fn test_date() {
        let v = SmallValue::date(19000);
        assert_eq!(v.as_date(), Some(19000));
        assert_eq!(v.tag(), ValueTag::Date);

        let v_neg = SmallValue::date(-365);
        assert_eq!(v_neg.as_date(), Some(-365));
    }

    #[test]
    fn test_equality() {
        let v1 = SmallValue::int64(42);
        let v2 = SmallValue::int64(42);
        let v3 = SmallValue::int64(43);

        assert_eq!(v1, v2);
        assert_ne!(v1, v3);

        let s1 = SmallValue::small_string("hello").unwrap();
        let s2 = SmallValue::small_string("hello").unwrap();
        let s3 = SmallValue::small_string("world").unwrap();

        assert_eq!(s1, s2);
        assert_ne!(s1, s3);
    }

    #[test]
    fn test_type_mismatch() {
        let v_int = SmallValue::int64(42);
        let v_float = SmallValue::float64(42.0);

        assert_ne!(v_int, v_float);
        assert_eq!(v_int.as_float64(), None);
        assert_eq!(v_float.as_int64(), None);
    }

    #[test]
    fn test_copy() {
        let v1 = SmallValue::int64(42);
        let v2 = v1;

        assert_eq!(v1, v2);
        assert_eq!(v1.as_int64(), v2.as_int64());
    }

    #[test]
    fn test_debug() {
        let v_null = SmallValue::null();
        assert_eq!(format!("{:?}", v_null), "NULL");

        let v_bool = SmallValue::bool(true);
        assert_eq!(format!("{:?}", v_bool), "Bool(true)");

        let v_int = SmallValue::int64(42);
        assert_eq!(format!("{:?}", v_int), "Int64(42)");

        let v_str = SmallValue::small_string("hello").unwrap();
        assert_eq!(format!("{:?}", v_str), "String(\"hello\")");
    }

    #[test]
    fn test_memory_layout() {
        let v = SmallValue::int64(0x0102030405060708);

        assert_eq!(v.tag as u8, 2);

        assert_eq!(v.data[0], 0x08);
        assert_eq!(v.data[1], 0x07);
        assert_eq!(v.data[7], 0x01);
    }

    #[test]
    fn test_small_string_utf8() {
        let v = SmallValue::small_string("hello").unwrap();
        assert_eq!(v.as_str(), Some("hello"));

        let v_emoji = SmallValue::small_string("👋").unwrap();
        assert_eq!(v_emoji.as_str(), Some("👋"));

        let long_emoji = "👋👋👋👋👋";
        let result = SmallValue::small_string(long_emoji);

        if long_emoji.len() > 14 {
            assert!(result.is_none());
        }
    }
}
