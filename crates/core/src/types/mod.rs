pub mod coercion;
pub mod collation;
pub mod conversion;
pub mod network;
pub mod tuple_ops;

pub mod small_value;

use std::fmt;
use std::rc::Rc;

use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use indexmap::IndexMap;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Unknown,
    Bool,
    Int64,
    Float64,
    Numeric(Option<(u8, u8)>),
    BigNumeric,
    String,
    Bytes,
    Date,
    DateTime,
    Time,
    Timestamp,
    TimestampTz,
    Geography,
    Json,
    Hstore,
    Struct(Vec<StructField>),
    Array(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
    Uuid,
    Serial,
    BigSerial,
    Vector(usize),
    Interval,
    Range(RangeType),
    Inet,
    Cidr,
    Point,
    PgBox,
    Circle,
    MacAddr,
    MacAddr8,
    Enum {
        type_name: String,
        labels: Vec<String>,
    },
    Custom(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RangeType {
    Int4Range,
    Int8Range,
    NumRange,
    TsRange,
    TsTzRange,
    DateRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StructField {
    pub name: String,

    pub data_type: DataType,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Bool => write!(f, "BOOL"),
            DataType::Int64 => write!(f, "INT64"),
            DataType::Float64 => write!(f, "FLOAT64"),
            DataType::Unknown => write!(f, "UNKNOWN"),
            DataType::Numeric(None) => write!(f, "NUMERIC"),
            DataType::Numeric(Some((precision, scale))) => {
                write!(f, "NUMERIC({}, {})", precision, scale)
            }
            DataType::BigNumeric => write!(f, "BIGNUMERIC"),
            DataType::String => write!(f, "STRING"),
            DataType::Bytes => write!(f, "BYTES"),
            DataType::Date => write!(f, "DATE"),
            DataType::DateTime => write!(f, "DATETIME"),
            DataType::Time => write!(f, "TIME"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::TimestampTz => write!(f, "TIMESTAMPTZ"),
            DataType::Geography => write!(f, "GEOGRAPHY"),
            DataType::Json => write!(f, "JSON"),
            DataType::Hstore => write!(f, "HSTORE"),
            DataType::Struct(fields) => {
                write!(f, "STRUCT<")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{} {}", field.name, field.data_type)?;
                }
                write!(f, ">")
            }
            DataType::Array(inner) => write!(f, "ARRAY<{}>", inner),
            DataType::Map(key_type, value_type) => {
                write!(f, "MAP<{}, {}>", key_type, value_type)
            }
            DataType::Uuid => write!(f, "UUID"),
            DataType::Serial => write!(f, "SERIAL"),
            DataType::BigSerial => write!(f, "BIGSERIAL"),
            DataType::Vector(dims) => write!(f, "VECTOR({})", dims),
            DataType::Interval => write!(f, "INTERVAL"),
            DataType::Range(range_type) => match range_type {
                RangeType::Int4Range => write!(f, "INT4RANGE"),
                RangeType::Int8Range => write!(f, "INT8RANGE"),
                RangeType::NumRange => write!(f, "NUMRANGE"),
                RangeType::TsRange => write!(f, "TSRANGE"),
                RangeType::TsTzRange => write!(f, "TSTZRANGE"),
                RangeType::DateRange => write!(f, "DATERANGE"),
            },
            DataType::Inet => write!(f, "INET"),
            DataType::Cidr => write!(f, "CIDR"),
            DataType::Point => write!(f, "POINT"),
            DataType::PgBox => write!(f, "BOX"),
            DataType::Circle => write!(f, "CIRCLE"),
            DataType::MacAddr => write!(f, "MACADDR"),
            DataType::MacAddr8 => write!(f, "MACADDR8"),
            DataType::Enum { type_name, .. } => write!(f, "{}", type_name),
            DataType::Custom(name) => write!(f, "{}", name),
        }
    }
}

const TAG_HEAP_START: u8 = 128;
const TAG_LARGE_STRING: u8 = 128;
const TAG_NUMERIC: u8 = 129;
const TAG_ARRAY: u8 = 130;
const TAG_BYTES: u8 = 131;
const TAG_JSON: u8 = 132;
const TAG_GEOGRAPHY: u8 = 133;
const TAG_STRUCT: u8 = 134;
const TAG_UUID: u8 = 135;
const TAG_DEFAULT: u8 = 136;
const TAG_VECTOR: u8 = 137;
const TAG_INTERVAL: u8 = 138;
const TAG_RANGE: u8 = 139;
const TAG_INET: u8 = 140;
const TAG_CIDR: u8 = 141;
const TAG_POINT: u8 = 142;
const TAG_PGBOX: u8 = 143;
const TAG_CIRCLE: u8 = 144;
const TAG_HSTORE: u8 = 145;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PgPoint {
    pub x: f64,

    pub y: f64,
}

impl PgPoint {
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();

        let s = s.strip_prefix('(')?.strip_suffix(')')?;
        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() != 2 {
            return None;
        }
        let x = parts[0].trim().parse::<f64>().ok()?;
        let y = parts[1].trim().parse::<f64>().ok()?;
        Some(Self::new(x, y))
    }

    pub fn distance(&self, other: &PgPoint) -> f64 {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        (dx * dx + dy * dy).sqrt()
    }
}

impl Eq for PgPoint {}

impl std::hash::Hash for PgPoint {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.x.to_bits().hash(state);
        self.y.to_bits().hash(state);
    }
}

impl fmt::Display for PgPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({},{})", self.x, self.y)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PgBox {
    pub high: PgPoint,

    pub low: PgPoint,
}

impl PgBox {
    pub fn new(p1: PgPoint, p2: PgPoint) -> Self {
        let high = PgPoint::new(p1.x.max(p2.x), p1.y.max(p2.y));
        let low = PgPoint::new(p1.x.min(p2.x), p1.y.min(p2.y));
        Self { high, low }
    }

    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();

        let s = if s.starts_with('(') && s.ends_with(')') {
            &s[1..s.len() - 1]
        } else {
            s
        };

        let split_pos = s.find("),(")?;
        let p1_str = &s[..split_pos + 1];
        let p2_str = &s[split_pos + 2..];

        let p1 = PgPoint::parse(p1_str)?;
        let p2 = PgPoint::parse(p2_str)?;

        Some(Self::new(p1, p2))
    }

    pub fn area(&self) -> f64 {
        (self.high.x - self.low.x).abs() * (self.high.y - self.low.y).abs()
    }

    pub fn width(&self) -> f64 {
        (self.high.x - self.low.x).abs()
    }

    pub fn height(&self) -> f64 {
        (self.high.y - self.low.y).abs()
    }

    pub fn center(&self) -> PgPoint {
        PgPoint::new(
            (self.high.x + self.low.x) / 2.0,
            (self.high.y + self.low.y) / 2.0,
        )
    }

    pub fn contains_point(&self, p: &PgPoint) -> bool {
        p.x >= self.low.x && p.x <= self.high.x && p.y >= self.low.y && p.y <= self.high.y
    }

    pub fn overlaps(&self, other: &PgBox) -> bool {
        self.low.x <= other.high.x
            && self.high.x >= other.low.x
            && self.low.y <= other.high.y
            && self.high.y >= other.low.y
    }
}

impl Eq for PgBox {}

impl std::hash::Hash for PgBox {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.high.hash(state);
        self.low.hash(state);
    }
}

impl fmt::Display for PgBox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({},{})", self.high, self.low)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PgCircle {
    pub center: PgPoint,

    pub radius: f64,
}

impl PgCircle {
    pub fn new(center: PgPoint, radius: f64) -> Self {
        Self {
            center,
            radius: radius.abs(),
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();

        let s = s.strip_prefix('<')?.strip_suffix('>')?;

        let center_end = s.find(')')?;
        let center_str = &s[..center_end + 1];
        let center = PgPoint::parse(center_str)?;

        let radius_str = &s[center_end + 1..];
        let radius_str = radius_str.trim_start_matches(',').trim();
        let radius = radius_str.parse::<f64>().ok()?;

        Some(Self::new(center, radius))
    }

    pub fn area(&self) -> f64 {
        std::f64::consts::PI * self.radius * self.radius
    }

    pub fn diameter(&self) -> f64 {
        2.0 * self.radius
    }

    pub fn contains_point(&self, p: &PgPoint) -> bool {
        self.center.distance(p) <= self.radius
    }

    pub fn overlaps(&self, other: &PgCircle) -> bool {
        self.center.distance(&other.center) <= self.radius + other.radius
    }

    pub fn distance_to_point(&self, p: &PgPoint) -> f64 {
        let dist_to_center = self.center.distance(p);
        if dist_to_center <= self.radius {
            0.0
        } else {
            dist_to_center - self.radius
        }
    }

    pub fn distance_to_circle(&self, other: &PgCircle) -> f64 {
        let center_dist = self.center.distance(&other.center);
        let edge_dist = center_dist - self.radius - other.radius;
        if edge_dist < 0.0 { 0.0 } else { edge_dist }
    }
}

impl Eq for PgCircle {}

impl std::hash::Hash for PgCircle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.center.hash(state);
        self.radius.to_bits().hash(state);
    }
}

impl fmt::Display for PgCircle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{},{}>", self.center, self.radius)
    }
}

fn parse_hstore_text(s: &str) -> Result<IndexMap<String, Option<String>>, String> {
    let mut map = IndexMap::new();
    let s = s.trim();

    if s.is_empty() {
        return Ok(map);
    }

    let mut pos = 0;
    let chars: Vec<char> = s.chars().collect();

    while pos < chars.len() {
        while pos < chars.len() && chars[pos].is_whitespace() {
            pos += 1;
        }

        if pos >= chars.len() {
            break;
        }

        let key = if chars[pos] == '"' {
            pos += 1;
            let start = pos;
            while pos < chars.len() && chars[pos] != '"' {
                if chars[pos] == '\\' && pos + 1 < chars.len() {
                    pos += 2;
                } else {
                    pos += 1;
                }
            }
            if pos >= chars.len() {
                return Err("Unterminated quoted key".to_string());
            }
            let key_str: String = chars[start..pos].iter().collect();
            pos += 1;
            unescape_hstore(&key_str)
        } else {
            let start = pos;
            while pos < chars.len() && chars[pos] != '=' && !chars[pos].is_whitespace() {
                pos += 1;
            }
            chars[start..pos].iter().collect()
        };

        while pos < chars.len() && chars[pos].is_whitespace() {
            pos += 1;
        }

        if pos + 1 >= chars.len() || chars[pos] != '=' || chars[pos + 1] != '>' {
            return Err(format!(
                "Expected '=>' after key '{}', found '{}'",
                key,
                if pos < chars.len() {
                    chars[pos].to_string()
                } else {
                    "EOF".to_string()
                }
            ));
        }
        pos += 2;

        while pos < chars.len() && chars[pos].is_whitespace() {
            pos += 1;
        }

        let value = if pos >= chars.len() {
            return Err(format!("Missing value for key '{}'", key));
        } else if chars[pos] == '"' {
            pos += 1;
            let start = pos;
            while pos < chars.len() && chars[pos] != '"' {
                if chars[pos] == '\\' && pos + 1 < chars.len() {
                    pos += 2;
                } else {
                    pos += 1;
                }
            }
            if pos >= chars.len() {
                return Err("Unterminated quoted value".to_string());
            }
            let val_str: String = chars[start..pos].iter().collect();
            pos += 1;
            Some(unescape_hstore(&val_str))
        } else {
            let start = pos;
            while pos < chars.len() && chars[pos] != ',' && !chars[pos].is_whitespace() {
                pos += 1;
            }
            let val_str: String = chars[start..pos].iter().collect();
            if val_str.to_uppercase() == "NULL" {
                None
            } else {
                Some(val_str)
            }
        };

        map.insert(key, value);

        while pos < chars.len() && chars[pos].is_whitespace() {
            pos += 1;
        }
        if pos < chars.len() && chars[pos] == ',' {
            pos += 1;
        }
    }

    Ok(map)
}

fn unescape_hstore(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(&next) = chars.peek() {
                chars.next();
                result.push(next);
            } else {
                result.push(c);
            }
        } else {
            result.push(c);
        }
    }

    result
}
const TAG_MACADDR: u8 = 146;
const TAG_MACADDR8: u8 = 147;
const TAG_MAP: u8 = 148;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct Interval {
    pub months: i32,

    pub days: i32,

    pub micros: i64,
}

impl Interval {
    pub const MICROS_PER_SECOND: i64 = 1_000_000;

    pub const MICROS_PER_MINUTE: i64 = 60 * Self::MICROS_PER_SECOND;

    pub const MICROS_PER_HOUR: i64 = 60 * Self::MICROS_PER_MINUTE;

    pub const MICROS_PER_DAY: i64 = 24 * Self::MICROS_PER_HOUR;

    pub const DAYS_PER_MONTH: i32 = 30;

    pub fn new(months: i32, days: i32, micros: i64) -> Self {
        Self {
            months,
            days,
            micros,
        }
    }

    pub fn from_hours(hours: i64) -> Self {
        Self {
            months: 0,
            days: 0,
            micros: hours * Self::MICROS_PER_HOUR,
        }
    }

    pub fn from_days(days: i32) -> Self {
        Self {
            months: 0,
            days,
            micros: 0,
        }
    }

    pub fn from_months(months: i32) -> Self {
        Self {
            months,
            days: 0,
            micros: 0,
        }
    }

    pub fn add(&self, other: &Interval) -> Interval {
        Interval {
            months: self.months + other.months,
            days: self.days + other.days,
            micros: self.micros + other.micros,
        }
    }

    pub fn sub(&self, other: &Interval) -> Interval {
        Interval {
            months: self.months - other.months,
            days: self.days - other.days,
            micros: self.micros - other.micros,
        }
    }

    pub fn neg(&self) -> Interval {
        Interval {
            months: -self.months,
            days: -self.days,
            micros: -self.micros,
        }
    }

    pub fn mul(&self, scalar: i64) -> Interval {
        Interval {
            months: (self.months as i64 * scalar) as i32,
            days: (self.days as i64 * scalar) as i32,
            micros: self.micros * scalar,
        }
    }

    pub fn div(&self, scalar: i64) -> Option<Interval> {
        if scalar == 0 {
            return None;
        }
        Some(Interval {
            months: self.months / scalar as i32,
            days: self.days / scalar as i32,
            micros: self.micros / scalar,
        })
    }

    pub fn to_total_micros(&self) -> i64 {
        let month_micros = self.months as i64 * Self::DAYS_PER_MONTH as i64 * Self::MICROS_PER_DAY;
        let day_micros = self.days as i64 * Self::MICROS_PER_DAY;
        month_micros + day_micros + self.micros
    }

    pub fn add_to_timestamp(&self, timestamp: DateTime<Utc>) -> Option<DateTime<Utc>> {
        use chrono::{Duration, Months};

        let mut result = timestamp;

        if self.months != 0 {
            if self.months > 0 {
                result = result.checked_add_months(Months::new(self.months as u32))?;
            } else {
                result = result.checked_sub_months(Months::new((-self.months) as u32))?;
            }
        }

        if self.days != 0 {
            result = result.checked_add_signed(Duration::days(self.days as i64))?;
        }

        if self.micros != 0 {
            result = result.checked_add_signed(Duration::microseconds(self.micros))?;
        }

        Some(result)
    }

    pub fn sub_from_timestamp(&self, timestamp: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.neg().add_to_timestamp(timestamp)
    }
}

impl PartialOrd for Interval {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Interval {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.to_total_micros().cmp(&other.to_total_micros())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Range {
    pub range_type: RangeType,

    pub lower: Option<Value>,

    pub upper: Option<Value>,

    pub lower_inclusive: bool,

    pub upper_inclusive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MacAddress {
    pub octets: [u8; 8],

    pub is_eui64: bool,
}

impl MacAddress {
    pub fn new_macaddr(octets: [u8; 6]) -> Self {
        Self {
            octets: [
                octets[0], octets[1], octets[2], octets[3], octets[4], octets[5], 0, 0,
            ],
            is_eui64: false,
        }
    }

    pub fn new_macaddr8(octets: [u8; 8]) -> Self {
        Self {
            octets,
            is_eui64: true,
        }
    }

    pub fn to_eui64(&self) -> Self {
        if self.is_eui64 {
            self.clone()
        } else {
            Self {
                octets: [
                    self.octets[0],
                    self.octets[1],
                    self.octets[2],
                    0xff,
                    0xfe,
                    self.octets[3],
                    self.octets[4],
                    self.octets[5],
                ],
                is_eui64: true,
            }
        }
    }

    pub fn trunc(&self) -> Self {
        if self.is_eui64 {
            Self {
                octets: [
                    self.octets[0],
                    self.octets[1],
                    self.octets[2],
                    0,
                    0,
                    0,
                    0,
                    0,
                ],
                is_eui64: true,
            }
        } else {
            Self {
                octets: [
                    self.octets[0],
                    self.octets[1],
                    self.octets[2],
                    0,
                    0,
                    0,
                    0,
                    0,
                ],
                is_eui64: false,
            }
        }
    }

    pub fn len(&self) -> usize {
        if self.is_eui64 { 8 } else { 6 }
    }

    pub fn is_empty(&self) -> bool {
        if self.is_eui64 {
            self.octets == [0; 8]
        } else {
            self.octets[..6] == [0; 6]
        }
    }

    pub fn parse(s: &str, is_eui64: bool) -> Option<Self> {
        let s = s.trim();
        let expected_bytes = if is_eui64 { 8 } else { 6 };

        if s.contains(':') {
            let parts: Vec<&str> = s.split(':').collect();
            if parts.len() == expected_bytes {
                let mut octets = [0u8; 8];
                for (i, part) in parts.iter().enumerate() {
                    octets[i] = u8::from_str_radix(part, 16).ok()?;
                }
                return Some(if is_eui64 {
                    Self::new_macaddr8(octets)
                } else {
                    Self::new_macaddr([
                        octets[0], octets[1], octets[2], octets[3], octets[4], octets[5],
                    ])
                });
            }
        }

        if s.contains('-') {
            let parts: Vec<&str> = s.split('-').collect();
            if parts.len() == expected_bytes {
                let mut octets = [0u8; 8];
                for (i, part) in parts.iter().enumerate() {
                    octets[i] = u8::from_str_radix(part, 16).ok()?;
                }
                return Some(if is_eui64 {
                    Self::new_macaddr8(octets)
                } else {
                    Self::new_macaddr([
                        octets[0], octets[1], octets[2], octets[3], octets[4], octets[5],
                    ])
                });
            }
        }

        if s.contains('.') {
            let parts: Vec<&str> = s.split('.').collect();
            let expected_groups = if is_eui64 { 4 } else { 3 };
            if parts.len() == expected_groups {
                let mut octets = [0u8; 8];
                let mut idx = 0;
                for part in parts {
                    if part.len() != 4 {
                        return None;
                    }
                    let high = u8::from_str_radix(&part[0..2], 16).ok()?;
                    let low = u8::from_str_radix(&part[2..4], 16).ok()?;
                    octets[idx] = high;
                    octets[idx + 1] = low;
                    idx += 2;
                }
                return Some(if is_eui64 {
                    Self::new_macaddr8(octets)
                } else {
                    Self::new_macaddr([
                        octets[0], octets[1], octets[2], octets[3], octets[4], octets[5],
                    ])
                });
            }
        }

        None
    }
}

impl fmt::Display for MacAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_eui64 {
            write!(
                f,
                "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                self.octets[0],
                self.octets[1],
                self.octets[2],
                self.octets[3],
                self.octets[4],
                self.octets[5],
                self.octets[6],
                self.octets[7]
            )
        } else {
            write!(
                f,
                "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                self.octets[0],
                self.octets[1],
                self.octets[2],
                self.octets[3],
                self.octets[4],
                self.octets[5]
            )
        }
    }
}

impl PartialOrd for MacAddress {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MacAddress {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_len = self.len();
        let other_len = other.len();

        for i in 0..std::cmp::min(self_len, other_len) {
            match self.octets[i].cmp(&other.octets[i]) {
                std::cmp::Ordering::Equal => continue,
                ord => return ord,
            }
        }
        self_len.cmp(&other_len)
    }
}

#[repr(C)]
struct HeapValue {
    tag: u8,
    _pad: [u8; 7],
    ptr: *mut u8,
}

const _: () = assert!(std::mem::size_of::<HeapValue>() == 16);

#[repr(C)]
union ValueInner {
    inline: small_value::SmallValue,
    heap: std::mem::ManuallyDrop<HeapValue>,
}

const _: () = assert!(std::mem::size_of::<ValueInner>() == 16);

#[repr(C, align(16))]
pub struct Value {
    inner: ValueInner,
}

const _: () = assert!(std::mem::size_of::<Value>() == 16);
const _: () = assert!(std::mem::align_of::<Value>() == 16);

unsafe impl Send for Value {}

unsafe impl Sync for Value {}

impl Default for Value {
    fn default() -> Self {
        Self::null()
    }
}

impl Value {
    #[inline(always)]
    fn tag(&self) -> u8 {
        unsafe { *(self as *const Value as *const u8) }
    }

    #[inline(always)]
    fn is_heap(&self) -> bool {
        self.tag() >= TAG_HEAP_START
    }

    #[inline(always)]
    fn is_inline(&self) -> bool {
        self.tag() < TAG_HEAP_START
    }

    #[inline(always)]
    unsafe fn as_inline(&self) -> &small_value::SmallValue {
        debug_assert!(self.is_inline());
        unsafe { &self.inner.inline }
    }

    #[inline(always)]
    unsafe fn as_heap(&self) -> &HeapValue {
        debug_assert!(self.is_heap());
        unsafe { &self.inner.heap }
    }
}

impl Value {
    #[inline]
    pub const fn null() -> Self {
        Self {
            inner: ValueInner {
                inline: small_value::SmallValue::null(),
            },
        }
    }

    #[inline]
    pub const fn bool_val(value: bool) -> Self {
        Self {
            inner: ValueInner {
                inline: small_value::SmallValue::bool(value),
            },
        }
    }

    #[inline]
    pub fn int64(value: i64) -> Self {
        Self {
            inner: ValueInner {
                inline: small_value::SmallValue::int64(value),
            },
        }
    }

    #[inline]
    pub fn float64(value: f64) -> Self {
        Self {
            inner: ValueInner {
                inline: small_value::SmallValue::float64(value),
            },
        }
    }

    #[inline]
    pub fn string(s: String) -> Self {
        if s.len() <= 14
            && let Some(small) = small_value::SmallValue::small_string(&s)
        {
            return Self {
                inner: ValueInner { inline: small },
            };
        }

        let rc = Rc::new(s);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_LARGE_STRING,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    #[inline]
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        Self::string(s.to_string())
    }

    #[inline]
    pub fn numeric(value: Decimal) -> Self {
        let rc = Rc::new(value);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_NUMERIC,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    #[inline]
    pub fn bytes(value: Vec<u8>) -> Self {
        let rc = Rc::new(value);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_BYTES,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    #[inline]
    pub fn date(value: NaiveDate) -> Self {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is a valid date");
        let days = value.signed_duration_since(epoch).num_days() as i32;
        Self {
            inner: ValueInner {
                inline: small_value::SmallValue::date(days),
            },
        }
    }

    #[inline]
    pub fn datetime(value: DateTime<Utc>) -> Self {
        let micros = value.timestamp_micros();
        Self {
            inner: ValueInner {
                inline: small_value::SmallValue::datetime(micros),
            },
        }
    }

    #[inline]
    pub fn time(value: NaiveTime) -> Self {
        let midnight = NaiveTime::from_hms_opt(0, 0, 0).expect("00:00:00 is a valid time");
        let nanos = value
            .signed_duration_since(midnight)
            .num_nanoseconds()
            .unwrap_or(0);
        Self {
            inner: ValueInner {
                inline: small_value::SmallValue::time(nanos),
            },
        }
    }

    #[inline]
    pub fn timestamp(value: DateTime<Utc>) -> Self {
        let micros = value.timestamp_micros();
        Self {
            inner: ValueInner {
                inline: small_value::SmallValue::timestamp(micros),
            },
        }
    }

    #[inline]
    pub fn geography(wkt: String) -> Self {
        let rc = Rc::new(wkt);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_GEOGRAPHY,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    #[inline]
    pub fn struct_val(map: IndexMap<String, Value>) -> Self {
        let rc = Rc::new(map);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_STRUCT,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    #[inline]
    pub fn array(values: Vec<Value>) -> Self {
        let rc = Rc::new(values);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_ARRAY,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    #[inline]
    pub fn json(value: serde_json::Value) -> Self {
        let rc = Rc::new(value);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_JSON,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    #[inline]
    pub fn uuid(value: Uuid) -> Self {
        let rc = Rc::new(value);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_UUID,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    #[inline]
    pub fn vector(values: Vec<f64>) -> Self {
        let rc = Rc::new(values);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_VECTOR,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    #[inline]
    pub fn interval(interval: Interval) -> Self {
        let rc = Rc::new(interval);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_INTERVAL,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    #[inline]
    pub fn range(range: Range) -> Self {
        let rc = Rc::new(range);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_RANGE,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    #[inline]
    pub fn hstore(map: IndexMap<String, Option<String>>) -> Self {
        let rc = Rc::new(map);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_HSTORE,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    pub fn hstore_from_str(s: &str) -> Result<Self, String> {
        let map = parse_hstore_text(s)?;
        Ok(Self::hstore(map))
    }

    pub fn default_value() -> Self {
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_DEFAULT,
                    _pad: [0; 7],
                    ptr: std::ptr::null_mut(),
                }),
            },
        }
    }

    #[inline]
    pub fn map(entries: Vec<(Value, Value)>) -> Self {
        let rc = Rc::new(entries);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_MAP,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    #[inline]
    pub fn inet(value: network::InetAddr) -> Self {
        let rc = Rc::new(value);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_INET,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    #[inline]
    pub fn macaddr(addr: MacAddress) -> Self {
        let rc = Rc::new(addr);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_MACADDR,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    pub fn inet_from_str(s: &str) -> Result<Self, String> {
        let addr = s.parse::<network::InetAddr>()?;
        Ok(Self::inet(addr))
    }

    #[inline]
    pub fn cidr(value: network::CidrAddr) -> Self {
        let rc = Rc::new(value);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_CIDR,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    #[inline]
    pub fn macaddr8(addr: MacAddress) -> Self {
        let rc = Rc::new(addr);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_MACADDR8,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    pub fn cidr_from_str(s: &str) -> Result<Self, String> {
        let addr = s.parse::<network::CidrAddr>()?;
        Ok(Self::cidr(addr))
    }

    #[inline]
    pub fn point(p: PgPoint) -> Self {
        let rc = Rc::new(p);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_POINT,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    #[inline]
    pub fn pgbox(b: PgBox) -> Self {
        let rc = Rc::new(b);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_PGBOX,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }

    #[inline]
    pub fn circle(c: PgCircle) -> Self {
        let rc = Rc::new(c);
        let ptr = Rc::into_raw(rc) as *mut u8;
        Self {
            inner: ValueInner {
                heap: std::mem::ManuallyDrop::new(HeapValue {
                    tag: TAG_CIRCLE,
                    _pad: [0; 7],
                    ptr,
                }),
            },
        }
    }
}

impl Value {
    #[inline]
    pub fn is_null(&self) -> bool {
        unsafe {
            if self.is_inline() {
                self.as_inline().is_null()
            } else {
                false
            }
        }
    }

    #[inline]
    pub fn is_default(&self) -> bool {
        !self.is_inline() && self.tag() == TAG_DEFAULT
    }

    #[inline]
    pub fn is_bool(&self) -> bool {
        unsafe { self.is_inline() && matches!(self.as_inline().tag(), small_value::ValueTag::Bool) }
    }

    #[inline]
    pub fn is_int64(&self) -> bool {
        unsafe {
            self.is_inline() && matches!(self.as_inline().tag(), small_value::ValueTag::Int64)
        }
    }

    #[inline]
    pub fn is_float64(&self) -> bool {
        unsafe {
            self.is_inline() && matches!(self.as_inline().tag(), small_value::ValueTag::Float64)
        }
    }

    #[inline]
    pub fn is_string(&self) -> bool {
        unsafe {
            match self.tag() {
                tag if tag < TAG_HEAP_START => {
                    matches!(self.as_inline().tag(), small_value::ValueTag::SmallString)
                }
                TAG_LARGE_STRING => true,
                _ => false,
            }
        }
    }

    #[inline]
    pub fn is_numeric(&self) -> bool {
        self.tag() == TAG_NUMERIC
    }

    pub fn data_type(&self) -> DataType {
        unsafe {
            let tag = self.tag();
            if tag < TAG_HEAP_START {
                use small_value::ValueTag;
                match self.as_inline().tag() {
                    ValueTag::Null => DataType::Unknown,
                    ValueTag::Bool => DataType::Bool,
                    ValueTag::Int64 => DataType::Int64,
                    ValueTag::Float64 => DataType::Float64,
                    ValueTag::Date => DataType::Date,
                    ValueTag::Time => DataType::Time,
                    ValueTag::DateTime => DataType::DateTime,
                    ValueTag::Timestamp => DataType::Timestamp,
                    ValueTag::SmallString => DataType::String,
                }
            } else {
                match tag {
                    TAG_LARGE_STRING => DataType::String,
                    TAG_NUMERIC => DataType::Numeric(None),
                    TAG_BYTES => DataType::Bytes,
                    TAG_GEOGRAPHY => DataType::Geography,
                    TAG_STRUCT => {
                        let heap = self.as_heap();
                        let map_ptr = heap.ptr as *const IndexMap<String, Value>;
                        let map = &*map_ptr;
                        let fields = map
                            .iter()
                            .map(|(name, value)| StructField {
                                name: name.clone(),
                                data_type: value.data_type(),
                            })
                            .collect();
                        DataType::Struct(fields)
                    }
                    TAG_ARRAY => {
                        let heap = self.as_heap();
                        let arr_ptr = heap.ptr as *const Vec<Value>;
                        let arr = &*arr_ptr;
                        let mut inferred: Option<DataType> = None;
                        for value in arr.iter() {
                            let ty = value.data_type();
                            if ty == DataType::Unknown {
                                continue;
                            }
                            match &mut inferred {
                                None => inferred = Some(ty),
                                Some(existing) if *existing == ty => {}
                                Some(_) => {
                                    inferred = Some(DataType::Unknown);
                                    break;
                                }
                            }
                        }
                        DataType::Array(Box::new(inferred.unwrap_or(DataType::Unknown)))
                    }
                    TAG_JSON => DataType::Json,
                    TAG_HSTORE => DataType::Hstore,
                    TAG_MAP => {
                        let heap = self.as_heap();
                        let map_ptr = heap.ptr as *const Vec<(Value, Value)>;
                        let entries = &*map_ptr;
                        if entries.is_empty() {
                            DataType::Map(Box::new(DataType::Unknown), Box::new(DataType::Unknown))
                        } else {
                            let key_type = entries[0].0.data_type();
                            let value_type = entries[0].1.data_type();
                            DataType::Map(Box::new(key_type), Box::new(value_type))
                        }
                    }
                    TAG_UUID => DataType::Uuid,
                    TAG_VECTOR => {
                        let heap = self.as_heap();
                        let vec_ptr = heap.ptr as *const Vec<f64>;
                        let vec = &*vec_ptr;
                        DataType::Vector(vec.len())
                    }
                    TAG_INTERVAL => DataType::Interval,
                    TAG_RANGE => {
                        let heap = self.as_heap();
                        let range_ptr = heap.ptr as *const Range;
                        let range = &*range_ptr;
                        DataType::Range(range.range_type.clone())
                    }
                    TAG_MACADDR => DataType::MacAddr,
                    TAG_MACADDR8 => DataType::MacAddr8,
                    TAG_DEFAULT => DataType::Unknown,
                    TAG_INET => DataType::Inet,
                    TAG_CIDR => DataType::Cidr,
                    TAG_POINT => DataType::Point,
                    TAG_PGBOX => DataType::PgBox,
                    TAG_CIRCLE => DataType::Circle,
                    _ => DataType::Unknown,
                }
            }
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        unsafe {
            let tag = self.tag();
            if tag < TAG_HEAP_START {
                self.as_inline().as_bool()
            } else if tag == TAG_JSON {
                let heap = self.as_heap();
                let json_ptr = heap.ptr as *const serde_json::Value;
                match &*json_ptr {
                    serde_json::Value::Bool(b) => Some(*b),
                    _ => None,
                }
            } else {
                None
            }
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        unsafe {
            let tag = self.tag();
            if tag < TAG_HEAP_START {
                self.as_inline().as_int64()
            } else if tag == TAG_JSON {
                let heap = self.as_heap();
                let json_ptr = heap.ptr as *const serde_json::Value;
                match &*json_ptr {
                    serde_json::Value::Number(n) => n.as_i64(),
                    _ => None,
                }
            } else {
                None
            }
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        unsafe {
            let tag = self.tag();
            if tag < TAG_HEAP_START {
                let small = self.as_inline();
                if let Some(f) = small.as_float64() {
                    Some(f)
                } else {
                    small.as_int64().map(|i| i as f64)
                }
            } else if tag == TAG_NUMERIC {
                let heap = self.as_heap();
                let d_ptr = heap.ptr as *const Decimal;
                (*d_ptr).to_string().parse::<f64>().ok()
            } else if tag == TAG_JSON {
                let heap = self.as_heap();
                let json_ptr = heap.ptr as *const serde_json::Value;
                match &*json_ptr {
                    serde_json::Value::Number(n) => n.as_f64(),
                    _ => None,
                }
            } else {
                None
            }
        }
    }

    pub fn as_numeric(&self) -> Option<Decimal> {
        unsafe {
            let tag = self.tag();
            if tag == TAG_NUMERIC {
                let heap = self.as_heap();
                let d_ptr = heap.ptr as *const Decimal;
                Some((*d_ptr).normalize())
            } else if tag < TAG_HEAP_START {
                let small = self.as_inline();
                if let Some(i) = small.as_int64() {
                    Some(Decimal::from(i))
                } else if let Some(f) = small.as_float64() {
                    Decimal::try_from(f).ok()
                } else {
                    None
                }
            } else {
                None
            }
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        unsafe {
            let tag = self.tag();
            if tag < TAG_HEAP_START {
                self.as_inline().as_str()
            } else {
                let heap = self.as_heap();
                match tag {
                    TAG_LARGE_STRING | TAG_GEOGRAPHY => {
                        let s_ptr = heap.ptr as *const String;
                        Some((*s_ptr).as_str())
                    }
                    TAG_JSON => {
                        let json_ptr = heap.ptr as *const serde_json::Value;
                        match &*json_ptr {
                            serde_json::Value::String(s) => Some(s.as_str()),
                            _ => None,
                        }
                    }
                    _ => None,
                }
            }
        }
    }

    pub fn is_array(&self) -> bool {
        self.tag() == TAG_ARRAY
    }

    pub fn as_array(&self) -> Option<&Vec<Value>> {
        unsafe {
            if self.tag() == TAG_ARRAY {
                let heap = self.as_heap();
                let arr_ptr = heap.ptr as *const Vec<Value>;
                Some(&*arr_ptr)
            } else {
                None
            }
        }
    }

    pub fn is_json(&self) -> bool {
        self.tag() == TAG_JSON
    }

    pub fn as_json_string(&self) -> Option<String> {
        unsafe {
            if self.tag() == TAG_JSON {
                let heap = self.as_heap();
                let json_ptr = heap.ptr as *const serde_json::Value;
                Some((*json_ptr).to_string())
            } else {
                None
            }
        }
    }

    pub fn as_uuid(&self) -> Option<&Uuid> {
        unsafe {
            if self.tag() == TAG_UUID {
                let heap = self.as_heap();
                let uuid_ptr = heap.ptr as *const Uuid;
                Some(&*uuid_ptr)
            } else {
                None
            }
        }
    }

    pub fn as_uuid_string(&self) -> Option<String> {
        self.as_uuid().map(format_uuid_string)
    }

    pub fn as_date(&self) -> Option<NaiveDate> {
        unsafe {
            if self.is_inline() {
                let small = self.as_inline();
                if let Some(days) = small.as_date() {
                    let epoch =
                        NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is a valid date");
                    Some(epoch + chrono::Duration::days(days as i64))
                } else {
                    None
                }
            } else {
                None
            }
        }
    }

    pub fn as_time(&self) -> Option<NaiveTime> {
        unsafe {
            if self.is_inline() {
                let small = self.as_inline();
                if let Some(nanos) = small.as_time() {
                    let time =
                        NaiveTime::from_hms_opt(0, 0, 0)? + chrono::Duration::nanoseconds(nanos);
                    Some(time)
                } else {
                    None
                }
            } else {
                None
            }
        }
    }

    pub fn as_datetime(&self) -> Option<DateTime<Utc>> {
        unsafe {
            if self.is_inline() {
                let small = self.as_inline();
                if let Some(micros) = small.as_datetime() {
                    DateTime::from_timestamp_micros(micros)
                } else {
                    None
                }
            } else {
                None
            }
        }
    }

    pub fn as_timestamp(&self) -> Option<DateTime<Utc>> {
        unsafe {
            if self.is_inline() {
                let small = self.as_inline();
                if let Some(micros) = small.as_timestamp() {
                    DateTime::from_timestamp_micros(micros)
                } else {
                    None
                }
            } else {
                None
            }
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        unsafe {
            if self.tag() == TAG_BYTES {
                let heap = self.as_heap();
                let bytes_ptr = heap.ptr as *const Vec<u8>;
                Some((*bytes_ptr).as_slice())
            } else {
                None
            }
        }
    }

    pub fn as_struct(&self) -> Option<&IndexMap<String, Value>> {
        unsafe {
            if self.tag() == TAG_STRUCT {
                let heap = self.as_heap();
                let map_ptr = heap.ptr as *const IndexMap<String, Value>;
                Some(&*map_ptr)
            } else {
                None
            }
        }
    }

    pub fn as_geography(&self) -> Option<&str> {
        unsafe {
            if self.tag() == TAG_GEOGRAPHY {
                let heap = self.as_heap();
                let wkt_ptr = heap.ptr as *const String;
                Some((*wkt_ptr).as_str())
            } else {
                None
            }
        }
    }

    pub fn as_json(&self) -> Option<&serde_json::Value> {
        unsafe {
            if self.tag() == TAG_JSON {
                let heap = self.as_heap();
                let json_ptr = heap.ptr as *const serde_json::Value;
                Some(&*json_ptr)
            } else {
                None
            }
        }
    }

    pub fn is_hstore(&self) -> bool {
        self.tag() == TAG_HSTORE
    }

    pub fn as_hstore(&self) -> Option<&IndexMap<String, Option<String>>> {
        unsafe {
            if self.tag() == TAG_HSTORE {
                let heap = self.as_heap();
                let hstore_ptr = heap.ptr as *const IndexMap<String, Option<String>>;
                Some(&*hstore_ptr)
            } else {
                None
            }
        }
    }

    pub fn is_map(&self) -> bool {
        self.tag() == TAG_MAP
    }

    pub fn as_map(&self) -> Option<&Vec<(Value, Value)>> {
        unsafe {
            if self.tag() == TAG_MAP {
                let heap = self.as_heap();
                let map_ptr = heap.ptr as *const Vec<(Value, Value)>;
                Some(&*map_ptr)
            } else {
                None
            }
        }
    }

    pub fn as_vector(&self) -> Option<&Vec<f64>> {
        unsafe {
            if self.tag() == TAG_VECTOR {
                let heap = self.as_heap();
                let vec_ptr = heap.ptr as *const Vec<f64>;
                Some(&*vec_ptr)
            } else {
                None
            }
        }
    }

    pub fn as_interval(&self) -> Option<&Interval> {
        unsafe {
            if self.tag() == TAG_INTERVAL {
                let heap = self.as_heap();
                let interval_ptr = heap.ptr as *const Interval;
                Some(&*interval_ptr)
            } else {
                None
            }
        }
    }

    pub fn as_range(&self) -> Option<&Range> {
        unsafe {
            if self.tag() == TAG_RANGE {
                let heap = self.as_heap();
                let range_ptr = heap.ptr as *const Range;
                Some(&*range_ptr)
            } else {
                None
            }
        }
    }

    pub fn as_inet(&self) -> Option<&network::InetAddr> {
        unsafe {
            if self.tag() == TAG_INET {
                let heap = self.as_heap();
                let inet_ptr = heap.ptr as *const network::InetAddr;
                Some(&*inet_ptr)
            } else {
                None
            }
        }
    }

    #[inline]
    pub fn is_macaddr(&self) -> bool {
        self.tag() == TAG_MACADDR
    }

    #[inline]
    pub fn is_macaddr8(&self) -> bool {
        self.tag() == TAG_MACADDR8
    }

    pub fn as_macaddr(&self) -> Option<&MacAddress> {
        unsafe {
            if self.tag() == TAG_MACADDR {
                let heap = self.as_heap();
                let mac_ptr = heap.ptr as *const MacAddress;
                Some(&*mac_ptr)
            } else {
                None
            }
        }
    }

    pub fn as_cidr(&self) -> Option<&network::CidrAddr> {
        unsafe {
            if self.tag() == TAG_CIDR {
                let heap = self.as_heap();
                let cidr_ptr = heap.ptr as *const network::CidrAddr;
                Some(&*cidr_ptr)
            } else {
                None
            }
        }
    }

    pub fn as_point(&self) -> Option<&PgPoint> {
        unsafe {
            if self.tag() == TAG_POINT {
                let heap = self.as_heap();
                let point_ptr = heap.ptr as *const PgPoint;
                Some(&*point_ptr)
            } else {
                None
            }
        }
    }

    pub fn as_pgbox(&self) -> Option<&PgBox> {
        unsafe {
            if self.tag() == TAG_PGBOX {
                let heap = self.as_heap();
                let box_ptr = heap.ptr as *const PgBox;
                Some(&*box_ptr)
            } else {
                None
            }
        }
    }

    pub fn as_circle(&self) -> Option<&PgCircle> {
        unsafe {
            if self.tag() == TAG_CIRCLE {
                let heap = self.as_heap();
                let circle_ptr = heap.ptr as *const PgCircle;
                Some(&*circle_ptr)
            } else {
                None
            }
        }
    }

    pub fn as_macaddr8(&self) -> Option<&MacAddress> {
        unsafe {
            if self.tag() == TAG_MACADDR8 {
                let heap = self.as_heap();
                let mac_ptr = heap.ptr as *const MacAddress;
                Some(&*mac_ptr)
            } else {
                None
            }
        }
    }
}

impl Drop for Value {
    fn drop(&mut self) {
        unsafe {
            let tag = self.tag();
            if tag >= TAG_HEAP_START {
                let heap = &*self.inner.heap;

                match tag {
                    TAG_LARGE_STRING | TAG_GEOGRAPHY => {
                        let _ = Rc::from_raw(heap.ptr as *const String);
                    }
                    TAG_NUMERIC => {
                        let _ = Rc::from_raw(heap.ptr as *const Decimal);
                    }
                    TAG_ARRAY => {
                        let _ = Rc::from_raw(heap.ptr as *const Vec<Value>);
                    }
                    TAG_BYTES => {
                        let _ = Rc::from_raw(heap.ptr as *const Vec<u8>);
                    }
                    TAG_JSON => {
                        let _ = Rc::from_raw(heap.ptr as *const serde_json::Value);
                    }
                    TAG_STRUCT => {
                        let _ = Rc::from_raw(heap.ptr as *const IndexMap<String, Value>);
                    }
                    TAG_UUID => {
                        let _ = Rc::from_raw(heap.ptr as *const Uuid);
                    }
                    TAG_VECTOR => {
                        let _ = Rc::from_raw(heap.ptr as *const Vec<f64>);
                    }
                    TAG_INTERVAL => {
                        let _ = Rc::from_raw(heap.ptr as *const Interval);
                    }
                    TAG_RANGE => {
                        let _ = Rc::from_raw(heap.ptr as *const Range);
                    }
                    TAG_INET => {
                        let _ = Rc::from_raw(heap.ptr as *const network::InetAddr);
                    }
                    TAG_CIDR => {
                        let _ = Rc::from_raw(heap.ptr as *const network::CidrAddr);
                    }
                    TAG_POINT => {
                        let _ = Rc::from_raw(heap.ptr as *const PgPoint);
                    }
                    TAG_PGBOX => {
                        let _ = Rc::from_raw(heap.ptr as *const PgBox);
                    }
                    TAG_CIRCLE => {
                        let _ = Rc::from_raw(heap.ptr as *const PgCircle);
                    }
                    TAG_HSTORE => {
                        let _ = Rc::from_raw(heap.ptr as *const IndexMap<String, Option<String>>);
                    }
                    TAG_MAP => {
                        let _ = Rc::from_raw(heap.ptr as *const Vec<(Value, Value)>);
                    }
                    TAG_MACADDR | TAG_MACADDR8 => {
                        let _ = Rc::from_raw(heap.ptr as *const MacAddress);
                    }
                    TAG_DEFAULT => {}
                    _ => {}
                }
            }
        }
    }
}

impl Clone for Value {
    fn clone(&self) -> Self {
        unsafe {
            let tag = self.tag();
            if tag < TAG_HEAP_START {
                Self {
                    inner: ValueInner {
                        inline: self.inner.inline,
                    },
                }
            } else {
                let heap = self.as_heap();
                match tag {
                    TAG_LARGE_STRING | TAG_GEOGRAPHY => {
                        let arc_ptr = heap.ptr as *const String;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);

                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_NUMERIC => {
                        let arc_ptr = heap.ptr as *const Decimal;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);
                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag: TAG_NUMERIC,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_ARRAY => {
                        let arc_ptr = heap.ptr as *const Vec<Value>;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);
                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag: TAG_ARRAY,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_BYTES => {
                        let arc_ptr = heap.ptr as *const Vec<u8>;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);
                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag: TAG_BYTES,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_JSON => {
                        let arc_ptr = heap.ptr as *const serde_json::Value;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);
                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag: TAG_JSON,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_STRUCT => {
                        let arc_ptr = heap.ptr as *const IndexMap<String, Value>;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);
                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag: TAG_STRUCT,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_UUID => {
                        let arc_ptr = heap.ptr as *const Uuid;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);
                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag: TAG_UUID,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_VECTOR => {
                        let arc_ptr = heap.ptr as *const Vec<f64>;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);
                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag: TAG_VECTOR,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_INTERVAL => {
                        let arc_ptr = heap.ptr as *const Interval;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);
                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag: TAG_INTERVAL,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_RANGE => {
                        let arc_ptr = heap.ptr as *const Range;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);
                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag: TAG_RANGE,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_INET => {
                        let arc_ptr = heap.ptr as *const network::InetAddr;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);
                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag: TAG_INET,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_MACADDR | TAG_MACADDR8 => {
                        let arc_ptr = heap.ptr as *const MacAddress;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);
                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        let tag = self.tag();
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_CIDR => {
                        let arc_ptr = heap.ptr as *const network::CidrAddr;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);
                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag: TAG_CIDR,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_POINT => {
                        let arc_ptr = heap.ptr as *const PgPoint;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);
                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag: TAG_POINT,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_PGBOX => {
                        let arc_ptr = heap.ptr as *const PgBox;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);
                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag: TAG_PGBOX,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_CIRCLE => {
                        let arc_ptr = heap.ptr as *const PgCircle;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);
                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag: TAG_CIRCLE,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_HSTORE => {
                        let arc_ptr = heap.ptr as *const IndexMap<String, Option<String>>;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);
                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag: TAG_HSTORE,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_MAP => {
                        let arc_ptr = heap.ptr as *const Vec<(Value, Value)>;
                        let rc = Rc::from_raw(arc_ptr);
                        let cloned_arc = Rc::clone(&rc);
                        let _ = Rc::into_raw(rc);
                        let ptr = Rc::into_raw(cloned_arc) as *mut u8;
                        Self {
                            inner: ValueInner {
                                heap: std::mem::ManuallyDrop::new(HeapValue {
                                    tag: TAG_MAP,
                                    _pad: [0; 7],
                                    ptr,
                                }),
                            },
                        }
                    }
                    TAG_DEFAULT => Self::default_value(),
                    _ => Self::null(),
                }
            }
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            let self_tag = self.tag();
            let other_tag = other.tag();

            if self_tag != other_tag {
                return false;
            }

            if self_tag < TAG_HEAP_START {
                self.as_inline() == other.as_inline()
            } else {
                let self_heap = self.as_heap();
                let other_heap = other.as_heap();

                match self_tag {
                    TAG_LARGE_STRING | TAG_GEOGRAPHY => {
                        let self_s = &*(self_heap.ptr as *const String);
                        let other_s = &*(other_heap.ptr as *const String);
                        self_s == other_s
                    }
                    TAG_NUMERIC => {
                        let self_d = &*(self_heap.ptr as *const Decimal);
                        let other_d = &*(other_heap.ptr as *const Decimal);
                        self_d == other_d
                    }
                    TAG_ARRAY => {
                        let self_arr = &*(self_heap.ptr as *const Vec<Value>);
                        let other_arr = &*(other_heap.ptr as *const Vec<Value>);
                        self_arr == other_arr
                    }
                    TAG_BYTES => {
                        let self_bytes = &*(self_heap.ptr as *const Vec<u8>);
                        let other_bytes = &*(other_heap.ptr as *const Vec<u8>);
                        self_bytes == other_bytes
                    }
                    TAG_JSON => {
                        let self_json = &*(self_heap.ptr as *const serde_json::Value);
                        let other_json = &*(other_heap.ptr as *const serde_json::Value);
                        self_json == other_json
                    }
                    TAG_STRUCT => {
                        let self_map = &*(self_heap.ptr as *const IndexMap<String, Value>);
                        let other_map = &*(other_heap.ptr as *const IndexMap<String, Value>);

                        if self_map.len() != other_map.len() {
                            return false;
                        }
                        self_map
                            .values()
                            .zip(other_map.values())
                            .all(|(a, b)| a == b)
                    }
                    TAG_UUID => {
                        let self_uuid = &*(self_heap.ptr as *const Uuid);
                        let other_uuid = &*(other_heap.ptr as *const Uuid);
                        self_uuid == other_uuid
                    }
                    TAG_VECTOR => {
                        let self_vec = &*(self_heap.ptr as *const Vec<f64>);
                        let other_vec = &*(other_heap.ptr as *const Vec<f64>);
                        self_vec == other_vec
                    }
                    TAG_INTERVAL => {
                        let self_interval = &*(self_heap.ptr as *const Interval);
                        let other_interval = &*(other_heap.ptr as *const Interval);
                        self_interval == other_interval
                    }
                    TAG_RANGE => {
                        let self_range = &*(self_heap.ptr as *const Range);
                        let other_range = &*(other_heap.ptr as *const Range);
                        self_range == other_range
                    }
                    TAG_INET => {
                        let self_inet = &*(self_heap.ptr as *const network::InetAddr);
                        let other_inet = &*(other_heap.ptr as *const network::InetAddr);
                        self_inet == other_inet
                    }
                    TAG_CIDR => {
                        let self_cidr = &*(self_heap.ptr as *const network::CidrAddr);
                        let other_cidr = &*(other_heap.ptr as *const network::CidrAddr);
                        self_cidr == other_cidr
                    }
                    TAG_POINT => {
                        let self_point = &*(self_heap.ptr as *const PgPoint);
                        let other_point = &*(other_heap.ptr as *const PgPoint);
                        self_point == other_point
                    }
                    TAG_PGBOX => {
                        let self_box = &*(self_heap.ptr as *const PgBox);
                        let other_box = &*(other_heap.ptr as *const PgBox);
                        self_box == other_box
                    }
                    TAG_CIRCLE => {
                        let self_circle = &*(self_heap.ptr as *const PgCircle);
                        let other_circle = &*(other_heap.ptr as *const PgCircle);
                        self_circle == other_circle
                    }
                    TAG_MAP => {
                        let self_map = &*(self_heap.ptr as *const Vec<(Value, Value)>);
                        let other_map = &*(other_heap.ptr as *const Vec<(Value, Value)>);
                        self_map == other_map
                    }
                    TAG_DEFAULT => true,

                    TAG_MACADDR | TAG_MACADDR8 => {
                        let self_mac = &*(self_heap.ptr as *const MacAddress);
                        let other_mac = &*(other_heap.ptr as *const MacAddress);
                        self_mac == other_mac
                    }
                    _ => false,
                }
            }
        }
    }
}

impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        unsafe {
            let tag = self.tag();

            tag.hash(state);

            if tag < TAG_HEAP_START {
                use small_value::ValueTag;
                match self.as_inline().tag() {
                    ValueTag::Null => {}
                    ValueTag::Bool => {
                        if let Some(b) = self.as_inline().as_bool() {
                            b.hash(state);
                        }
                    }
                    ValueTag::Int64 => {
                        if let Some(i) = self.as_inline().as_int64() {
                            i.hash(state);
                        }
                    }
                    ValueTag::Float64 => {
                        if let Some(f) = self.as_inline().as_float64() {
                            f.to_bits().hash(state);
                        }
                    }
                    ValueTag::SmallString => {
                        if let Some(s) = self.as_inline().as_str() {
                            s.hash(state);
                        }
                    }
                    ValueTag::Date => {
                        if let Some(days) = self.as_inline().as_date() {
                            days.hash(state);
                        }
                    }
                    ValueTag::Time => {
                        if let Some(nanos) = self.as_inline().as_time() {
                            nanos.hash(state);
                        }
                    }
                    ValueTag::DateTime => {
                        if let Some(micros) = self.as_inline().as_datetime() {
                            micros.hash(state);
                        }
                    }
                    ValueTag::Timestamp => {
                        if let Some(micros) = self.as_inline().as_timestamp() {
                            micros.hash(state);
                        }
                    }
                }
            } else {
                let _heap = self.as_heap();
                match tag {
                    TAG_LARGE_STRING | TAG_GEOGRAPHY => {
                        if let Some(s) = self.as_str() {
                            s.hash(state);
                        }
                    }
                    TAG_NUMERIC => {
                        if let Some(d) = self.as_numeric() {
                            d.mantissa().hash(state);
                            d.scale().hash(state);
                        }
                    }
                    TAG_ARRAY => {
                        if let Some(arr) = self.as_array() {
                            arr.len().hash(state);
                            for val in arr {
                                val.hash(state);
                            }
                        }
                    }
                    TAG_BYTES => {
                        if let Some(bytes) = self.as_bytes() {
                            bytes.hash(state);
                        }
                    }
                    TAG_JSON => {
                        if let Some(json_str) = self.as_json_string() {
                            json_str.hash(state);
                        }
                    }
                    TAG_STRUCT => {
                        if let Some(map) = self.as_struct() {
                            map.len().hash(state);
                            for (key, val) in map {
                                key.hash(state);
                                val.hash(state);
                            }
                        }
                    }
                    TAG_UUID => {
                        if let Some(uuid) = self.as_uuid() {
                            uuid.hash(state);
                        }
                    }
                    TAG_VECTOR => {
                        if let Some(vec) = self.as_vector() {
                            vec.len().hash(state);
                            for &f in vec {
                                f.to_bits().hash(state);
                            }
                        }
                    }
                    TAG_INTERVAL => {
                        if let Some(interval) = self.as_interval() {
                            interval.hash(state);
                        }
                    }
                    TAG_RANGE => {
                        if let Some(range) = self.as_range() {
                            range.hash(state);
                        }
                    }
                    TAG_INET => {
                        if let Some(inet) = self.as_inet() {
                            inet.hash(state);
                        }
                    }
                    TAG_CIDR => {
                        if let Some(cidr) = self.as_cidr() {
                            cidr.hash(state);
                        }
                    }
                    TAG_POINT => {
                        if let Some(point) = self.as_point() {
                            point.hash(state);
                        }
                    }
                    TAG_PGBOX => {
                        if let Some(b) = self.as_pgbox() {
                            b.hash(state);
                        }
                    }
                    TAG_CIRCLE => {
                        if let Some(c) = self.as_circle() {
                            c.hash(state);
                        }
                    }
                    TAG_MAP => {
                        if let Some(entries) = self.as_map() {
                            entries.len().hash(state);
                            for (k, v) in entries {
                                k.hash(state);
                                v.hash(state);
                            }
                        }
                    }
                    TAG_DEFAULT => {}
                    TAG_MACADDR | TAG_MACADDR8 => {
                        if let Some(mac) = self.as_macaddr().or_else(|| self.as_macaddr8()) {
                            mac.hash(state);
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe {
            let tag = self.tag();
            if tag < TAG_HEAP_START {
                write!(f, "Value::Inline({:?})", self.as_inline())
            } else {
                let heap = self.as_heap();
                match tag {
                    TAG_LARGE_STRING => {
                        let s = &*(heap.ptr as *const String);
                        write!(f, "Value::LargeString({:?})", s)
                    }
                    TAG_NUMERIC => {
                        let d = &*(heap.ptr as *const Decimal);
                        write!(f, "Value::numeric({:?})", d)
                    }
                    TAG_ARRAY => {
                        let arr = &*(heap.ptr as *const Vec<Value>);
                        write!(f, "Value::array({:?})", arr)
                    }
                    TAG_BYTES => {
                        let bytes = &*(heap.ptr as *const Vec<u8>);
                        write!(f, "Value::bytes({:?})", bytes)
                    }
                    TAG_JSON => {
                        let json = &*(heap.ptr as *const serde_json::Value);
                        write!(f, "Value::json({:?})", json)
                    }
                    TAG_GEOGRAPHY => {
                        let wkt = &*(heap.ptr as *const String);
                        write!(f, "Value::geography({:?})", wkt)
                    }
                    TAG_STRUCT => {
                        let map = &*(heap.ptr as *const IndexMap<String, Value>);
                        write!(f, "Value::struct_val({:?})", map)
                    }
                    TAG_MAP => {
                        let entries = &*(heap.ptr as *const Vec<(Value, Value)>);
                        write!(f, "Value::map({:?})", entries)
                    }
                    TAG_UUID => {
                        let uuid = &*(heap.ptr as *const Uuid);
                        write!(f, "Value::uuid({:?})", uuid)
                    }
                    TAG_VECTOR => {
                        let vec = &*(heap.ptr as *const Vec<f64>);
                        write!(f, "Value::vector({:?})", vec)
                    }
                    TAG_INTERVAL => {
                        let interval = &*(heap.ptr as *const Interval);
                        write!(f, "Value::interval({:?})", interval)
                    }
                    TAG_RANGE => {
                        let range = &*(heap.ptr as *const Range);
                        write!(f, "Value::range({:?})", range)
                    }
                    TAG_INET => {
                        let inet = &*(heap.ptr as *const network::InetAddr);
                        write!(f, "Value::inet({:?})", inet)
                    }
                    TAG_CIDR => {
                        let cidr = &*(heap.ptr as *const network::CidrAddr);
                        write!(f, "Value::cidr({:?})", cidr)
                    }
                    TAG_POINT => {
                        let point = &*(heap.ptr as *const PgPoint);
                        write!(f, "Value::point({:?})", point)
                    }
                    TAG_PGBOX => {
                        let b = &*(heap.ptr as *const PgBox);
                        write!(f, "Value::pgbox({:?})", b)
                    }
                    TAG_CIRCLE => {
                        let c = &*(heap.ptr as *const PgCircle);
                        write!(f, "Value::circle({:?})", c)
                    }
                    TAG_DEFAULT => write!(f, "Value::Default"),

                    TAG_MACADDR => {
                        let mac = &*(heap.ptr as *const MacAddress);
                        write!(f, "Value::macaddr({:?})", mac)
                    }
                    TAG_MACADDR8 => {
                        let mac = &*(heap.ptr as *const MacAddress);
                        write!(f, "Value::macaddr8({:?})", mac)
                    }
                    _ => write!(f, "Value::Unknown(tag={})", tag),
                }
            }
        }
    }
}

impl serde::Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_json().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let json = serde_json::Value::deserialize(deserializer)?;

        Ok(Value::json(json))
    }
}

pub fn parse_timestamp_to_utc(timestamp_str: &str) -> Option<DateTime<Utc>> {
    use chrono::NaiveDateTime;

    if let Ok(dt) = DateTime::parse_from_rfc3339(timestamp_str) {
        return Some(dt.with_timezone(&Utc));
    }

    if let Ok(dt) = DateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%:z")
        .or_else(|_| DateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%.f%:z"))
        .or_else(|_| DateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%z"))
        .or_else(|_| DateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%.f%z"))
    {
        return Some(dt.with_timezone(&Utc));
    }

    if let Ok(dt) = DateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S%:z")
        .or_else(|_| DateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S%.f%:z"))
        .or_else(|_| DateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S%z"))
        .or_else(|_| DateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S%.f%z"))
    {
        return Some(dt.with_timezone(&Utc));
    }

    if let Some(sign_pos) = timestamp_str.rfind(|c| ['+', '-'].contains(&c)) {
        let offset = &timestamp_str[sign_pos + 1..];
        if offset.len() == 2 && offset.chars().all(|c| c.is_ascii_digit()) {
            let mut normalized = String::with_capacity(timestamp_str.len() + 3);
            normalized.push_str(&timestamp_str[..sign_pos + 1]);
            normalized.push_str(offset);
            normalized.push_str(":00");
            if let Ok(dt) = DateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%:z")
                .or_else(|_| DateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%.f%:z"))
            {
                return Some(dt.with_timezone(&Utc));
            }
        }
    }

    if let Some(result) = parse_timestamp_with_named_timezone(timestamp_str) {
        return Some(result);
    }

    if let Ok(naive) = NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%.f"))
    {
        return Some(Utc.from_utc_datetime(&naive));
    }

    None
}

fn parse_timestamp_with_named_timezone(timestamp_str: &str) -> Option<DateTime<Utc>> {
    use chrono::NaiveDateTime;
    use chrono_tz::Tz;

    let trimmed = timestamp_str.trim();

    if let Some(last_space_pos) = trimmed.rfind(' ') {
        let potential_tz = &trimmed[last_space_pos + 1..];
        let datetime_part = &trimmed[..last_space_pos];

        if potential_tz.contains(':') {
            return None;
        }

        if let Ok(tz) = potential_tz.parse::<Tz>()
            && let Ok(naive) = NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%d %H:%M:%S%.f"))
                .or_else(|_| NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%dT%H:%M:%S"))
                .or_else(|_| NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%dT%H:%M:%S%.f"))
            && let Some(local_dt) = tz.from_local_datetime(&naive).earliest()
        {
            return Some(local_dt.with_timezone(&Utc));
        }

        let tz_offset = match potential_tz.to_uppercase().as_str() {
            "UTC" | "GMT" | "Z" => Some(0),
            "EST" => Some(-5 * 3600),
            "EDT" => Some(-4 * 3600),
            "CST" => Some(-6 * 3600),
            "CDT" => Some(-5 * 3600),
            "MST" => Some(-7 * 3600),
            "MDT" => Some(-6 * 3600),
            "PST" => Some(-8 * 3600),
            "PDT" => Some(-7 * 3600),
            _ => None,
        };

        use chrono::FixedOffset;
        if let Some(offset_seconds) = tz_offset
            && let Ok(naive) = NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%d %H:%M:%S%.f"))
                .or_else(|_| NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%dT%H:%M:%S"))
                .or_else(|_| NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%dT%H:%M:%S%.f"))
            && let Some(fixed_offset) = FixedOffset::east_opt(offset_seconds)
            && let Some(local_dt) = fixed_offset.from_local_datetime(&naive).earliest()
        {
            return Some(local_dt.with_timezone(&Utc));
        }
    }

    None
}

#[inline]
pub fn format_uuid_string(uuid: &Uuid) -> String {
    uuid.hyphenated().to_string().to_lowercase()
}

#[inline]
pub fn parse_uuid_literal(s: &str) -> Value {
    match Uuid::parse_str(s) {
        Ok(uuid) => Value::uuid(uuid),
        Err(_) => Value::null(),
    }
}

#[inline]
pub fn parse_point_literal(s: &str) -> Value {
    let s = s.trim();

    if !s.starts_with('(') || !s.ends_with(')') {
        return Value::null();
    }

    let content = &s[1..s.len() - 1];
    let parts: Vec<&str> = content.split(',').collect();

    if parts.len() != 2 {
        return Value::null();
    }

    let x: f64 = match parts[0].trim().parse() {
        Ok(v) => v,
        Err(_) => return Value::null(),
    };

    let y: f64 = match parts[1].trim().parse() {
        Ok(v) => v,
        Err(_) => return Value::null(),
    };

    Value::point(PgPoint::new(x, y))
}

#[inline]
pub fn parse_pgbox_literal(s: &str) -> Value {
    let s = s.trim();

    if !s.starts_with("((") || !s.ends_with("))") {
        return Value::null();
    }

    let content = &s[1..s.len() - 1];

    let mut paren_depth = 0;
    let mut split_pos = None;
    for (i, c) in content.chars().enumerate() {
        match c {
            '(' => paren_depth += 1,
            ')' => paren_depth -= 1,
            ',' if paren_depth == 0 => {
                split_pos = Some(i);
                break;
            }
            _ => {}
        }
    }

    let split_pos = match split_pos {
        Some(pos) => pos,
        None => return Value::null(),
    };

    let point1_str = content[..split_pos].trim();
    let point2_str = content[split_pos + 1..].trim();

    let p1 = parse_point_literal(point1_str);
    if p1.is_null() {
        return Value::null();
    }

    let p2 = parse_point_literal(point2_str);
    if p2.is_null() {
        return Value::null();
    }

    let (x1, y1) = match p1.as_point() {
        Some(point) => (point.x, point.y),
        None => return Value::null(),
    };

    let (x2, y2) = match p2.as_point() {
        Some(point) => (point.x, point.y),
        None => return Value::null(),
    };

    Value::pgbox(PgBox::new(PgPoint::new(x1, y1), PgPoint::new(x2, y2)))
}

#[inline]
pub fn parse_circle_literal(s: &str) -> Value {
    let s = s.trim();

    if !s.starts_with('<') || !s.ends_with('>') {
        return Value::null();
    }

    let content = &s[1..s.len() - 1];

    let paren_end = match content.find(')') {
        Some(pos) => pos,
        None => return Value::null(),
    };

    let point_str = &content[..=paren_end];
    let remainder = &content[paren_end + 1..].trim_start();

    if !remainder.starts_with(',') {
        return Value::null();
    }

    let radius_str = remainder[1..].trim();

    let center = parse_point_literal(point_str);
    if center.is_null() {
        return Value::null();
    }

    let radius: f64 = match radius_str.parse() {
        Ok(v) => v,
        Err(_) => return Value::null(),
    };

    let (x, y) = match center.as_point() {
        Some(point) => (point.x, point.y),
        None => return Value::null(),
    };

    Value::circle(PgCircle::new(PgPoint::new(x, y), radius))
}

#[inline]
pub fn parse_uuid_strict(s: &str) -> crate::error::Result<Value> {
    Uuid::parse_str(s).map(Value::uuid).map_err(|e| {
        crate::error::Error::invalid_query(format!("Invalid UUID format '{}': {}", s, e))
    })
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe {
            let tag = self.tag();
            if tag < TAG_HEAP_START {
                let small = self.as_inline();
                use small_value::ValueTag;
                match small.tag() {
                    ValueTag::Null => write!(f, "NULL"),
                    ValueTag::Bool => write!(f, "{}", small.as_bool().expect("tag is Bool")),
                    ValueTag::Int64 => write!(f, "{}", small.as_int64().expect("tag is Int64")),
                    ValueTag::Float64 => {
                        write!(f, "{}", small.as_float64().expect("tag is Float64"))
                    }
                    ValueTag::SmallString => {
                        write!(f, "'{}'", small.as_str().expect("tag is SmallString"))
                    }
                    ValueTag::Date => {
                        let days = small.as_date().expect("tag is Date");
                        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                            .expect("1970-01-01 is a valid date");
                        let date = epoch + chrono::Duration::days(days as i64);
                        write!(f, "{}", date)
                    }
                    ValueTag::Time => {
                        let nanos = small.as_time().expect("tag is Time");
                        let time = NaiveTime::from_hms_opt(0, 0, 0)
                            .expect("00:00:00 is a valid time")
                            + chrono::Duration::nanoseconds(nanos);
                        write!(f, "{}", time)
                    }
                    ValueTag::DateTime => {
                        let micros = small.as_datetime().expect("tag is DateTime");
                        let dt = DateTime::from_timestamp_micros(micros)
                            .expect("valid timestamp micros from internal storage");
                        write!(f, "{}", dt)
                    }
                    ValueTag::Timestamp => {
                        let micros = small.as_timestamp().expect("tag is Timestamp");
                        let dt = DateTime::from_timestamp_micros(micros)
                            .expect("valid timestamp micros from internal storage");
                        write!(f, "{}", dt)
                    }
                }
            } else {
                let heap = self.as_heap();
                match tag {
                    TAG_LARGE_STRING => {
                        let s = &*(heap.ptr as *const String);
                        write!(f, "'{}'", s)
                    }
                    TAG_NUMERIC => {
                        let d = &*(heap.ptr as *const Decimal);
                        write!(f, "{}", d.normalize())
                    }
                    TAG_BYTES => {
                        let b = &*(heap.ptr as *const Vec<u8>);
                        write!(f, "b'{:?}'", b)
                    }
                    TAG_GEOGRAPHY => {
                        let wkt = &*(heap.ptr as *const String);
                        write!(f, "{}", wkt)
                    }
                    TAG_STRUCT => write!(f, "<STRUCT>"),
                    TAG_ARRAY => {
                        let arr = &*(heap.ptr as *const Vec<Value>);
                        write!(f, "[")?;
                        for (i, v) in arr.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{}", v)?;
                        }
                        write!(f, "]")
                    }
                    TAG_JSON => {
                        let j = &*(heap.ptr as *const serde_json::Value);
                        write!(f, "{}", j)
                    }
                    TAG_UUID => {
                        let u = &*(heap.ptr as *const Uuid);
                        write!(f, "{}", format_uuid_string(u))
                    }
                    TAG_VECTOR => {
                        let vec = &*(heap.ptr as *const Vec<f64>);
                        write!(f, "[")?;
                        for (i, &v) in vec.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{}", v)?;
                        }
                        write!(f, "]")
                    }
                    TAG_INTERVAL => {
                        let interval = &*(heap.ptr as *const Interval);
                        write!(
                            f,
                            "INTERVAL '{} months {} days {} microseconds'",
                            interval.months, interval.days, interval.micros
                        )
                    }
                    TAG_RANGE => {
                        let range = &*(heap.ptr as *const Range);
                        let lower_bound = if range.lower_inclusive { "[" } else { "(" };
                        let upper_bound = if range.upper_inclusive { "]" } else { ")" };
                        let lower_val = range
                            .lower
                            .as_ref()
                            .map(|v| format!("{}", v))
                            .unwrap_or_default();
                        let upper_val = range
                            .upper
                            .as_ref()
                            .map(|v| format!("{}", v))
                            .unwrap_or_default();
                        write!(
                            f,
                            "{}{},{}{}",
                            lower_bound, lower_val, upper_val, upper_bound
                        )
                    }
                    TAG_INET => {
                        let inet = &*(heap.ptr as *const network::InetAddr);
                        write!(f, "{}", inet)
                    }
                    TAG_CIDR => {
                        let cidr = &*(heap.ptr as *const network::CidrAddr);
                        write!(f, "{}", cidr)
                    }
                    TAG_POINT => {
                        let point = &*(heap.ptr as *const PgPoint);
                        write!(f, "{}", point)
                    }
                    TAG_PGBOX => {
                        let b = &*(heap.ptr as *const PgBox);
                        write!(f, "{}", b)
                    }
                    TAG_CIRCLE => {
                        let c = &*(heap.ptr as *const PgCircle);
                        write!(f, "{}", c)
                    }
                    TAG_HSTORE => {
                        let h = &*(heap.ptr as *const IndexMap<String, Option<String>>);
                        write!(f, "'")?;
                        for (i, (k, v)) in h.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            match v {
                                Some(val) => write!(f, "\"{}\"=>\"{}\"", k, val)?,
                                None => write!(f, "\"{}\"=>NULL", k)?,
                            }
                        }
                        write!(f, "'")
                    }
                    TAG_MAP => {
                        let entries = &*(heap.ptr as *const Vec<(Value, Value)>);
                        write!(f, "{{")?;
                        for (i, (k, v)) in entries.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{}:{}", k, v)?;
                        }
                        write!(f, "}}")
                    }
                    TAG_DEFAULT => write!(f, "DEFAULT"),
                    TAG_MACADDR | TAG_MACADDR8 => {
                        let mac = &*(heap.ptr as *const MacAddress);
                        write!(f, "{}", mac)
                    }
                    0..=127 => unreachable!("inline tags handled above"),
                    _ => write!(f, "<UNKNOWN_HEAP_TYPE:{}>", tag),
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;

    #[test]
    fn test_value_is_null() {
        assert!(Value::null().is_null());
        assert!(!Value::bool_val(true).is_null());
        assert!(!Value::int64(42).is_null());
        assert!(!Value::string("hello".to_string()).is_null());
    }

    #[test]
    fn test_value_default() {
        let default_value = Value::default();
        assert_eq!(default_value, Value::null());
        assert!(default_value.is_null());
    }

    #[test]
    fn test_value_data_type() {
        assert_eq!(Value::bool_val(true).data_type(), DataType::Bool);
        assert_eq!(Value::int64(42).data_type(), DataType::Int64);
        assert_eq!(Value::float64(3.14).data_type(), DataType::Float64);
        assert_eq!(
            Value::string("test".to_string()).data_type(),
            DataType::String
        );
        assert_eq!(Value::bytes(vec![1, 2, 3]).data_type(), DataType::Bytes);
        assert_eq!(Value::null().data_type(), DataType::Unknown);
    }

    #[test]
    fn test_value_as_bool() {
        assert_eq!(Value::bool_val(true).as_bool(), Some(true));
        assert_eq!(Value::bool_val(false).as_bool(), Some(false));
        assert_eq!(Value::int64(42).as_bool(), None);
        assert_eq!(Value::null().as_bool(), None);
    }

    #[test]
    fn test_value_as_i64() {
        assert_eq!(Value::int64(42).as_i64(), Some(42));
        assert_eq!(Value::int64(-100).as_i64(), Some(-100));
        assert_eq!(Value::int64(0).as_i64(), Some(0));
        assert_eq!(Value::float64(3.14).as_i64(), None);
        assert_eq!(Value::null().as_i64(), None);
    }

    #[test]
    fn test_value_as_f64() {
        assert_eq!(Value::float64(3.14).as_f64(), Some(3.14));
        assert_eq!(Value::float64(0.0).as_f64(), Some(0.0));
        assert_eq!(Value::int64(42).as_f64(), Some(42.0));
        assert_eq!(Value::string("test".to_string()).as_f64(), None);
        assert_eq!(Value::null().as_f64(), None);
    }

    #[test]
    fn test_value_as_str() {
        let s = Value::string("hello".to_string());
        assert_eq!(s.as_str(), Some("hello"));

        assert_eq!(Value::int64(42).as_str(), None);
        assert_eq!(Value::null().as_str(), None);
    }

    #[test]
    fn test_value_array_data_type() {
        let arr = Value::array(vec![Value::int64(1), Value::int64(2), Value::int64(3)]);
        assert_eq!(arr.data_type(), DataType::Array(Box::new(DataType::Int64)));

        let empty_arr = Value::array(vec![]);
        assert_eq!(
            empty_arr.data_type(),
            DataType::Array(Box::new(DataType::Unknown))
        );

        let mixed_arr = Value::array(vec![Value::int64(1), Value::null()]);
        assert_eq!(
            mixed_arr.data_type(),
            DataType::Array(Box::new(DataType::Int64))
        );

        let conflicting_arr = Value::array(vec![Value::int64(1), Value::string("x".into())]);
        assert_eq!(
            conflicting_arr.data_type(),
            DataType::Array(Box::new(DataType::Unknown))
        );
    }

    #[test]
    fn test_value_struct_data_type() {
        let mut map = IndexMap::new();
        map.insert("name".to_string(), Value::string("Alice".to_string()));
        map.insert("age".to_string(), Value::int64(30));

        let struct_value = Value::struct_val(map);
        let data_type = struct_value.data_type();

        match data_type {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
            }
            _ => panic!("Expected Struct type"),
        }
    }

    #[test]
    fn test_data_type_display() {
        assert_eq!(format!("{}", DataType::Bool), "BOOL");
        assert_eq!(format!("{}", DataType::Int64), "INT64");
        assert_eq!(format!("{}", DataType::Float64), "FLOAT64");
        assert_eq!(format!("{}", DataType::Unknown), "UNKNOWN");
        assert_eq!(format!("{}", DataType::String), "STRING");
        assert_eq!(format!("{}", DataType::Bytes), "BYTES");
        assert_eq!(format!("{}", DataType::Date), "DATE");
        assert_eq!(format!("{}", DataType::DateTime), "DATETIME");
        assert_eq!(format!("{}", DataType::Time), "TIME");
        assert_eq!(format!("{}", DataType::Timestamp), "TIMESTAMP");
        assert_eq!(format!("{}", DataType::Numeric(None)), "NUMERIC");
        assert_eq!(format!("{}", DataType::BigNumeric), "BIGNUMERIC");
        assert_eq!(format!("{}", DataType::Geography), "GEOGRAPHY");
        assert_eq!(format!("{}", DataType::Json), "JSON");
    }

    #[test]
    fn test_data_type_array_display() {
        let arr_type = DataType::Array(Box::new(DataType::Int64));
        assert_eq!(format!("{}", arr_type), "ARRAY<INT64>");

        let nested_arr = DataType::Array(Box::new(DataType::Array(Box::new(DataType::String))));
        assert_eq!(format!("{}", nested_arr), "ARRAY<ARRAY<STRING>>");
    }

    #[test]
    fn test_data_type_struct_display() {
        let struct_type = DataType::Struct(vec![
            StructField {
                name: "id".to_string(),
                data_type: DataType::Int64,
            },
            StructField {
                name: "name".to_string(),
                data_type: DataType::String,
            },
        ]);
        assert_eq!(format!("{}", struct_type), "STRUCT<id INT64, name STRING>");
    }

    #[test]
    fn test_value_display() {
        assert_eq!(format!("{}", Value::null()), "NULL");
        assert_eq!(format!("{}", Value::bool_val(true)), "true");
        assert_eq!(format!("{}", Value::bool_val(false)), "false");
        assert_eq!(format!("{}", Value::int64(42)), "42");
        assert_eq!(format!("{}", Value::float64(3.14)), "3.14");
        assert_eq!(format!("{}", Value::string("hello".to_string())), "'hello'");
    }

    #[test]
    fn test_value_array_display() {
        let arr = Value::array(vec![Value::int64(1), Value::int64(2), Value::int64(3)]);
        assert_eq!(format!("{}", arr), "[1, 2, 3]");

        let empty_arr = Value::array(vec![]);
        assert_eq!(format!("{}", empty_arr), "[]");
    }

    #[test]
    fn test_value_clone() {
        let original = Value::int64(42);
        let cloned = original.clone();
        assert_eq!(original, cloned);

        let arr = Value::array(vec![Value::int64(1), Value::int64(2)]);
        let arr_cloned = arr.clone();
        assert_eq!(arr, arr_cloned);
    }

    #[test]
    fn test_value_equality() {
        assert_eq!(Value::int64(42), Value::int64(42));
        assert_ne!(Value::int64(42), Value::int64(43));

        assert_eq!(
            Value::string("hello".to_string()),
            Value::string("hello".to_string())
        );
        assert_ne!(
            Value::string("hello".to_string()),
            Value::string("world".to_string())
        );

        assert_eq!(Value::null(), Value::null());
    }

    #[test]
    fn test_data_type_equality() {
        assert_eq!(DataType::Int64, DataType::Int64);
        assert_ne!(DataType::Int64, DataType::Float64);

        let arr1 = DataType::Array(Box::new(DataType::Int64));
        let arr2 = DataType::Array(Box::new(DataType::Int64));
        assert_eq!(arr1, arr2);

        let arr3 = DataType::Array(Box::new(DataType::String));
        assert_ne!(arr1, arr3);
    }

    #[test]
    fn test_struct_field() {
        let field = StructField {
            name: "test_field".to_string(),
            data_type: DataType::Int64,
        };

        assert_eq!(field.name, "test_field");
        assert_eq!(field.data_type, DataType::Int64);
    }

    #[test]
    fn test_value_bytes() {
        let bytes = Value::bytes(vec![1, 2, 3, 4, 5]);
        assert_eq!(bytes.data_type(), DataType::Bytes);
        assert!(!bytes.is_null());
    }

    #[test]
    fn test_numeric_display_normalization() {
        use std::str::FromStr;

        let d1 = Value::numeric(Decimal::from_str("100.00").unwrap());
        assert_eq!(format!("{}", d1), "100");

        let d2 = Value::numeric(Decimal::from_str("0.10").unwrap());
        assert_eq!(format!("{}", d2), "0.1");

        let d3 = Value::numeric(Decimal::from_str("50.50").unwrap());
        assert_eq!(format!("{}", d3), "50.5");

        let d4 = Value::numeric(Decimal::from_str("85").unwrap());
        assert_eq!(format!("{}", d4), "85");
    }

    #[test]
    fn test_value_size() {
        use std::mem;
        let value_size = mem::size_of::<Value>();
        let small_value_size = mem::size_of::<crate::types::small_value::SmallValue>();

        eprintln!(
            "[core::types] Current Value enum size: {} bytes",
            value_size
        );
        eprintln!("[core::types] SmallValue size: {} bytes", small_value_size);
        eprintln!(
            "[core::types] Potential savings: {} bytes ({:.1}%)",
            value_size.saturating_sub(small_value_size),
            (value_size.saturating_sub(small_value_size) as f64 / value_size as f64) * 100.0
        );

        assert_eq!(small_value_size, 16);
    }

    #[test]
    fn test_hash_consistency_basic_types() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_value(v: &Value) -> u64 {
            let mut hasher = DefaultHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }

        assert_eq!(hash_value(&Value::int64(42)), hash_value(&Value::int64(42)));
        assert_eq!(
            hash_value(&Value::bool_val(true)),
            hash_value(&Value::bool_val(true))
        );
        assert_eq!(
            hash_value(&Value::string("test".to_string())),
            hash_value(&Value::string("test".to_string()))
        );
        assert_eq!(hash_value(&Value::null()), hash_value(&Value::null()));

        assert_ne!(hash_value(&Value::int64(42)), hash_value(&Value::int64(43)));
        assert_ne!(
            hash_value(&Value::bool_val(true)),
            hash_value(&Value::bool_val(false))
        );
        assert_ne!(
            hash_value(&Value::string("test".to_string())),
            hash_value(&Value::string("other".to_string()))
        );
    }

    #[test]
    fn test_hash_consistency_floats() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_value(v: &Value) -> u64 {
            let mut hasher = DefaultHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }

        assert_eq!(
            hash_value(&Value::float64(3.14)),
            hash_value(&Value::float64(3.14))
        );
        assert_eq!(
            hash_value(&Value::float64(0.0)),
            hash_value(&Value::float64(0.0))
        );

        assert_ne!(
            hash_value(&Value::float64(3.14)),
            hash_value(&Value::float64(2.71))
        );

        let nan1 = Value::float64(f64::NAN);
        let nan2 = Value::float64(f64::NAN);
        assert_eq!(hash_value(&nan1), hash_value(&nan2));
    }

    #[test]
    fn test_hash_consistency_arrays() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_value(v: &Value) -> u64 {
            let mut hasher = DefaultHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }

        let arr1 = Value::array(vec![Value::int64(1), Value::int64(2), Value::int64(3)]);
        let arr2 = Value::array(vec![Value::int64(1), Value::int64(2), Value::int64(3)]);
        let arr3 = Value::array(vec![Value::int64(1), Value::int64(2), Value::int64(4)]);

        assert_eq!(hash_value(&arr1), hash_value(&arr2));

        assert_ne!(hash_value(&arr1), hash_value(&arr3));
    }

    #[test]
    fn test_hash_consistency_strings() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_value(v: &Value) -> u64 {
            let mut hasher = DefaultHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }

        let small1 = Value::string("short".to_string());
        let small2 = Value::string("short".to_string());
        assert_eq!(hash_value(&small1), hash_value(&small2));

        let large1 = Value::string("a very long string that exceeds the inline limit".to_string());
        let large2 = Value::string("a very long string that exceeds the inline limit".to_string());
        assert_eq!(hash_value(&large1), hash_value(&large2));

        let short1 = Value::string("abc".to_string());
        let short2 = Value::string("abc".to_string());
        assert_eq!(hash_value(&short1), hash_value(&short2));
    }

    #[test]
    fn test_hash_consistency_numerics() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        use std::str::FromStr;

        fn hash_value(v: &Value) -> u64 {
            let mut hasher = DefaultHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }

        let d1 = Value::numeric(Decimal::from_str("100.00").unwrap());
        let d2 = Value::numeric(Decimal::from_str("100.00").unwrap());
        assert_eq!(hash_value(&d1), hash_value(&d2));

        let d3 = Value::numeric(Decimal::from_str("100.01").unwrap());
        assert_ne!(hash_value(&d1), hash_value(&d3));
    }

    #[test]
    fn test_hash_type_discrimination() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_value(v: &Value) -> u64 {
            let mut hasher = DefaultHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }

        assert_ne!(hash_value(&Value::int64(0)), hash_value(&Value::null()));
        assert_ne!(
            hash_value(&Value::int64(1)),
            hash_value(&Value::bool_val(true))
        );
    }
}
