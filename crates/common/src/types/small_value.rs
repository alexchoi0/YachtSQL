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
