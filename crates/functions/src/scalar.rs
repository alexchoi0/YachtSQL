use std::fmt::Debug;

use md5;
use rust_decimal::prelude::ToPrimitive;
use sha1::Sha1;
use sha2::{Digest, Sha256, Sha512};
use yachtsql_core::error::Result;
use yachtsql_core::types::{DataType, Value};

pub trait ScalarFunction: Debug + Send + Sync {
    fn name(&self) -> &str;

    fn arg_types(&self) -> &[DataType];

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;

    fn is_variadic(&self) -> bool {
        false
    }

    fn evaluate(&self, args: &[Value]) -> Result<Value>;
}

#[derive(Debug)]
pub struct ScalarFunctionImpl {
    pub name: String,
    pub arg_types: Vec<DataType>,
    pub return_type: DataType,
    pub variadic: bool,
    pub evaluator: fn(&[Value]) -> Result<Value>,
}

impl ScalarFunctionImpl {
    pub fn new(
        name: String,
        arg_types: Vec<DataType>,
        return_type: DataType,
        variadic: bool,
        evaluator: fn(&[Value]) -> Result<Value>,
    ) -> Self {
        Self {
            name,
            arg_types,
            return_type,
            variadic,
            evaluator,
        }
    }
}

impl ScalarFunction for ScalarFunctionImpl {
    fn name(&self) -> &str {
        &self.name
    }

    fn arg_types(&self) -> &[DataType] {
        &self.arg_types
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn is_variadic(&self) -> bool {
        self.variadic
    }

    fn evaluate(&self, args: &[Value]) -> Result<Value> {
        (self.evaluator)(args)
    }
}

pub fn eval_to_number(value: &Value) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(s) = value.as_str() {
        let trimmed = s.trim();

        if trimmed.is_empty() {
            return Err(yachtsql_core::error::Error::invalid_query(
                "TO_NUMBER: empty string cannot be converted to number".to_string(),
            ));
        }

        match trimmed.parse::<f64>() {
            Ok(num) => {
                if num.is_infinite() || num.is_nan() {
                    return Err(yachtsql_core::error::Error::invalid_query(format!(
                        "TO_NUMBER: invalid numeric value: {}",
                        trimmed
                    )));
                }
                Ok(Value::float64(num))
            }
            Err(_) => Err(yachtsql_core::error::Error::invalid_query(format!(
                "TO_NUMBER: invalid numeric string: '{}'",
                s
            ))),
        }
    } else {
        Err(yachtsql_core::error::Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: value.data_type().to_string(),
        })
    }
}

pub fn eval_to_char(value: &Value) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(n) = value.as_i64() {
        return Ok(Value::string(n.to_string()));
    }

    if let Some(f) = value.as_f64() {
        return Ok(Value::string(format_float(f)));
    }

    Err(yachtsql_core::error::Error::TypeMismatch {
        expected: "INT64 or FLOAT64".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn format_float(f: f64) -> String {
    if f.is_infinite() {
        if f.is_sign_positive() {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        }
    } else if f.is_nan() {
        "NaN".to_string()
    } else {
        f.to_string()
    }
}

pub fn eval_length(value: &Value) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(s) = value.as_str() {
        let char_count = s.chars().count() as i64;
        return Ok(Value::int64(char_count));
    }

    if let Some(b) = value.as_bytes() {
        return Ok(Value::int64(b.len() as i64));
    }

    Err(yachtsql_core::error::Error::TypeMismatch {
        expected: "STRING or BYTES".to_string(),
        actual: value.data_type().to_string(),
    })
}

pub fn eval_octet_length(value: &Value) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(s) = value.as_str() {
        return Ok(Value::int64(s.len() as i64));
    }

    if let Some(b) = value.as_bytes() {
        return Ok(Value::int64(b.len() as i64));
    }

    Err(yachtsql_core::error::Error::TypeMismatch {
        expected: "STRING or BYTES".to_string(),
        actual: value.data_type().to_string(),
    })
}

pub fn eval_bit_count(value: &Value) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(i) = value.as_i64() {
        return Ok(Value::int64(i.count_ones() as i64));
    }

    if let Some(b) = value.as_bytes() {
        let count: u32 = b.iter().map(|byte| byte.count_ones()).sum();
        return Ok(Value::int64(count as i64));
    }

    Err(yachtsql_core::error::Error::TypeMismatch {
        expected: "INTEGER or BYTES".to_string(),
        actual: value.data_type().to_string(),
    })
}

pub fn eval_get_bit(value: &Value, position: i64) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(b) = value.as_bytes() {
        let bit_len = b.len() * 8;
        if position < 0 || position >= bit_len as i64 {
            return Err(yachtsql_core::error::Error::invalid_query(format!(
                "GET_BIT: bit index {} out of range (0..{})",
                position, bit_len
            )));
        }
        let byte_idx = position as usize / 8;
        let bit_idx = 7 - (position as usize % 8);
        let bit = (b[byte_idx] >> bit_idx) & 1;
        return Ok(Value::int64(bit as i64));
    }

    Err(yachtsql_core::error::Error::TypeMismatch {
        expected: "BYTES".to_string(),
        actual: value.data_type().to_string(),
    })
}

pub fn eval_set_bit(value: &Value, position: i64, new_value: i64) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if new_value != 0 && new_value != 1 {
        return Err(yachtsql_core::error::Error::invalid_query(format!(
            "SET_BIT: new value must be 0 or 1, got {}",
            new_value
        )));
    }

    if let Some(b) = value.as_bytes() {
        let bit_len = b.len() * 8;
        if position < 0 || position >= bit_len as i64 {
            return Err(yachtsql_core::error::Error::invalid_query(format!(
                "SET_BIT: bit index {} out of range (0..{})",
                position, bit_len
            )));
        }
        let byte_idx = position as usize / 8;
        let bit_idx = 7 - (position as usize % 8);
        let mut result = b.to_vec();
        if new_value == 1 {
            result[byte_idx] |= 1 << bit_idx;
        } else {
            result[byte_idx] &= !(1 << bit_idx);
        }
        return Ok(Value::bytes(result));
    }

    Err(yachtsql_core::error::Error::TypeMismatch {
        expected: "BYTES".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn value_to_string_for_hashing(value: &Value) -> Result<String> {
    if value.is_null() {
        return Ok(String::new());
    }

    if let Some(s) = value.as_str() {
        return Ok(s.to_string());
    }

    if let Some(i) = value.as_i64() {
        return Ok(i.to_string());
    }
    if let Some(f) = value.as_f64() {
        return Ok(f.to_string());
    }
    if let Some(n) = value.as_numeric() {
        return Ok(n.to_string());
    }

    if let Some(b) = value.as_bool() {
        return Ok(b.to_string());
    }

    if let Some(d) = value.as_date() {
        return Ok(d.to_string());
    }
    if let Some(ts) = value.as_timestamp() {
        return Ok(ts.to_string());
    }

    if value.as_array().is_some() {
        return Err(yachtsql_core::error::Error::invalid_query(
            "Cannot hash ARRAY type directly. Use ARRAY_TO_STRING or hash individual elements"
                .to_string(),
        ));
    }

    if value.as_struct().is_some() {
        return Err(yachtsql_core::error::Error::invalid_query(
            "Cannot hash STRUCT type directly. Hash individual fields instead".to_string(),
        ));
    }

    Err(yachtsql_core::error::Error::invalid_query(format!(
        "Cannot hash type {:?}",
        value.data_type()
    )))
}

fn compute_hash<F>(value: &Value, hash_fn: F, _func_name: &str, return_hex: bool) -> Result<Value>
where
    F: FnOnce(&[u8]) -> Vec<u8>,
{
    let format_hash_output = |hash_bytes: Vec<u8>| -> Value {
        if return_hex {
            Value::string(hex::encode(&hash_bytes))
        } else {
            Value::bytes(hash_bytes)
        }
    };

    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(s) = value.as_str() {
        let hash_bytes = hash_fn(s.as_bytes());
        return Ok(format_hash_output(hash_bytes));
    }

    if let Some(b) = value.as_bytes() {
        let hash_bytes = hash_fn(b);
        return Ok(format_hash_output(hash_bytes));
    }

    let s = value_to_string_for_hashing(value)?;
    let hash_bytes = hash_fn(s.as_bytes());
    Ok(format_hash_output(hash_bytes))
}

pub fn eval_md5(value: &Value, return_hex: bool) -> Result<Value> {
    compute_hash(
        value,
        |bytes| md5::compute(bytes).to_vec(),
        "MD5",
        return_hex,
    )
}

pub fn eval_sha1(value: &Value, return_hex: bool) -> Result<Value> {
    compute_hash(
        value,
        |bytes| {
            let mut hasher = Sha1::new();
            hasher.update(bytes);
            hasher.finalize().to_vec()
        },
        "SHA1",
        return_hex,
    )
}

pub fn eval_sha256(value: &Value, return_hex: bool) -> Result<Value> {
    compute_hash(
        value,
        |bytes| {
            let mut hasher = Sha256::new();
            hasher.update(bytes);
            hasher.finalize().to_vec()
        },
        "SHA256",
        return_hex,
    )
}

pub fn eval_sha512(value: &Value, return_hex: bool) -> Result<Value> {
    compute_hash(
        value,
        |bytes| {
            let mut hasher = Sha512::new();
            hasher.update(bytes);
            hasher.finalize().to_vec()
        },
        "SHA512",
        return_hex,
    )
}

pub fn eval_farm_fingerprint(value: &Value) -> Result<Value> {
    let compute_fingerprint = |bytes: &[u8]| -> Value {
        let hash = farmhash::hash64(bytes);

        Value::int64(hash as i64)
    };

    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(s) = value.as_str() {
        return Ok(compute_fingerprint(s.as_bytes()));
    }

    if let Some(b) = value.as_bytes() {
        return Ok(compute_fingerprint(b));
    }

    let s = value_to_string_for_hashing(value)?;
    Ok(compute_fingerprint(s.as_bytes()))
}

pub fn eval_to_hex(value: &Value) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(b) = value.as_bytes() {
        return Ok(Value::string(hex::encode(b)));
    }

    Err(yachtsql_core::error::Error::TypeMismatch {
        expected: "TO_HEX requires BYTES".to_string(),
        actual: value.data_type().to_string(),
    })
}

pub fn eval_from_hex(value: &Value) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(s) = value.as_str() {
        let bytes = hex::decode(s).map_err(|e| {
            yachtsql_core::error::Error::invalid_query(format!(
                "FROM_HEX: invalid hex string: {}",
                e
            ))
        })?;
        return Ok(Value::bytes(bytes));
    }

    Err(yachtsql_core::error::Error::TypeMismatch {
        expected: "FROM_HEX requires STRING".to_string(),
        actual: value.data_type().to_string(),
    })
}

pub fn eval_to_base64(value: &Value) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(b) = value.as_bytes() {
        use base64::Engine as _;
        use base64::engine::general_purpose::STANDARD;
        return Ok(Value::string(STANDARD.encode(b)));
    }

    Err(yachtsql_core::error::Error::TypeMismatch {
        expected: "TO_BASE64 requires BYTES".to_string(),
        actual: value.data_type().to_string(),
    })
}

pub fn eval_from_base64(value: &Value) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(s) = value.as_str() {
        use base64::Engine as _;
        use base64::engine::general_purpose::STANDARD;
        let bytes = STANDARD.decode(s).map_err(|e| {
            yachtsql_core::error::Error::invalid_query(format!(
                "FROM_BASE64: invalid base64 string: {}",
                e
            ))
        })?;
        return Ok(Value::bytes(bytes));
    }

    Err(yachtsql_core::error::Error::TypeMismatch {
        expected: "FROM_BASE64 requires STRING".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn checked_int64_arithmetic_for_numeric<F>(
    a: &rust_decimal::Decimal,
    b: &rust_decimal::Decimal,
    operation: F,
) -> Option<Option<rust_decimal::Decimal>>
where
    F: FnOnce(i64, i64) -> Option<i64>,
{
    if a.is_integer()
        && b.is_integer()
        && let (Some(a_i64), Some(b_i64)) = (a.to_i64(), b.to_i64())
    {
        return Some(operation(a_i64, b_i64).map(rust_decimal::Decimal::from));
    }
    None
}

fn checked_int64_unary_for_numeric<F>(
    a: &rust_decimal::Decimal,
    operation: F,
) -> Option<Option<rust_decimal::Decimal>>
where
    F: FnOnce(i64) -> Option<i64>,
{
    if a.is_integer()
        && let Some(a_i64) = a.to_i64()
    {
        return Some(operation(a_i64).map(rust_decimal::Decimal::from));
    }
    None
}

fn safe_binary_operation<F, G, H>(
    left: &Value,
    right: &Value,
    int64_op: F,
    float64_op: G,
    numeric_op: H,
    op_name: &str,
    op_symbol: &str,
) -> Result<Value>
where
    F: FnOnce(i64, i64) -> Option<i64>,
    G: Fn(f64, f64) -> f64,
    H: Fn(&rust_decimal::Decimal, &rust_decimal::Decimal) -> Option<rust_decimal::Decimal>,
{
    if left.is_null() || right.is_null() {
        return Ok(Value::null());
    }

    if let (Some(a), Some(b)) = (left.as_i64(), right.as_i64()) {
        return Ok(int64_op(a, b).map(Value::int64).unwrap_or(Value::null()));
    }

    if let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) {
        let result = float64_op(a, b);
        return if result.is_finite() {
            Ok(Value::float64(result))
        } else {
            Ok(Value::null())
        };
    }

    if let (Some(a), Some(b)) = (left.as_i64(), right.as_f64()) {
        return safe_binary_operation(
            &Value::float64(a as f64),
            &Value::float64(b),
            int64_op,
            float64_op,
            numeric_op,
            op_name,
            op_symbol,
        );
    }
    if let (Some(a), Some(b)) = (left.as_f64(), right.as_i64()) {
        return safe_binary_operation(
            &Value::float64(a),
            &Value::float64(b as f64),
            int64_op,
            float64_op,
            numeric_op,
            op_name,
            op_symbol,
        );
    }

    if let (Some(a), Some(b)) = (left.as_numeric(), right.as_numeric()) {
        return match checked_int64_arithmetic_for_numeric(&a, &b, int64_op) {
            Some(Some(result)) => Ok(Value::numeric(result)),
            Some(None) => Ok(Value::null()),
            None => Ok(numeric_op(&a, &b)
                .map(Value::numeric)
                .unwrap_or(Value::null())),
        };
    }

    if let (Some(a), Some(b)) = (left.as_i64(), right.as_numeric()) {
        let a_numeric = rust_decimal::Decimal::from(a);
        return safe_binary_operation(
            &Value::numeric(a_numeric),
            &Value::numeric(b),
            int64_op,
            float64_op,
            numeric_op,
            op_name,
            op_symbol,
        );
    }
    if let (Some(a), Some(b)) = (left.as_numeric(), right.as_i64()) {
        let b_numeric = rust_decimal::Decimal::from(b);
        return safe_binary_operation(
            &Value::numeric(a),
            &Value::numeric(b_numeric),
            int64_op,
            float64_op,
            numeric_op,
            op_name,
            op_symbol,
        );
    }

    Err(yachtsql_core::error::Error::TypeMismatch {
        expected: format!(
            "{} requires numeric types (INT64, FLOAT64, NUMERIC)",
            op_name
        ),
        actual: format!("{} {} {}", left.data_type(), op_symbol, right.data_type()),
    })
}

pub fn eval_safe_add(left: &Value, right: &Value) -> Result<Value> {
    safe_binary_operation(
        left,
        right,
        |a, b| a.checked_add(b),
        |a, b| a + b,
        |a, b| a.checked_add(*b),
        "SAFE_ADD",
        "+",
    )
}

pub fn eval_safe_subtract(left: &Value, right: &Value) -> Result<Value> {
    safe_binary_operation(
        left,
        right,
        |a, b| a.checked_sub(b),
        |a, b| a - b,
        |a, b| a.checked_sub(*b),
        "SAFE_SUBTRACT",
        "-",
    )
}

pub fn eval_safe_multiply(left: &Value, right: &Value) -> Result<Value> {
    safe_binary_operation(
        left,
        right,
        |a, b| a.checked_mul(b),
        |a, b| a * b,
        |a, b| a.checked_mul(*b),
        "SAFE_MULTIPLY",
        "*",
    )
}

pub fn eval_safe_divide(left: &Value, right: &Value) -> Result<Value> {
    if left.is_null() || right.is_null() {
        return Ok(Value::null());
    }

    if let (Some(a), Some(b)) = (left.as_i64(), right.as_i64()) {
        if b == 0 {
            return Ok(Value::null());
        } else {
            return match a.checked_div(b) {
                Some(result) => Ok(Value::int64(result)),
                None => Ok(Value::null()),
            };
        }
    }

    if let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) {
        if b == 0.0 {
            return Ok(Value::null());
        } else {
            let result = a / b;
            return if result.is_finite() {
                Ok(Value::float64(result))
            } else {
                Ok(Value::null())
            };
        }
    }

    if let (Some(a), Some(b)) = (left.as_i64(), right.as_f64()) {
        return eval_safe_divide(&Value::float64(a as f64), &Value::float64(b));
    }
    if let (Some(a), Some(b)) = (left.as_f64(), right.as_i64()) {
        return eval_safe_divide(&Value::float64(a), &Value::float64(b as f64));
    }

    if let (Some(a), Some(b)) = (left.as_numeric(), right.as_numeric()) {
        if b.is_zero() {
            return Ok(Value::null());
        } else {
            return match a.checked_div(b) {
                Some(result) => Ok(Value::numeric(result)),
                None => Ok(Value::null()),
            };
        }
    }

    if let (Some(a), Some(b)) = (left.as_i64(), right.as_numeric()) {
        let a_numeric = rust_decimal::Decimal::from(a);
        return eval_safe_divide(&Value::numeric(a_numeric), &Value::numeric(b));
    }
    if let (Some(a), Some(b)) = (left.as_numeric(), right.as_i64()) {
        let b_numeric = rust_decimal::Decimal::from(b);
        return eval_safe_divide(&Value::numeric(a), &Value::numeric(b_numeric));
    }

    Err(yachtsql_core::error::Error::TypeMismatch {
        expected: "SAFE_DIVIDE requires numeric types".to_string(),
        actual: format!("{} / {}", left.data_type(), right.data_type()),
    })
}

pub fn eval_safe_negate(value: &Value) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(a) = value.as_i64() {
        return Ok(a.checked_neg().map(Value::int64).unwrap_or(Value::null()));
    }

    if let Some(a) = value.as_f64() {
        return Ok(Value::float64(-a));
    }

    if let Some(a) = value.as_numeric() {
        return match checked_int64_unary_for_numeric(&a, |x| x.checked_neg()) {
            Some(Some(result)) => Ok(Value::numeric(result)),
            Some(None) => Ok(Value::null()),
            None => Ok(Value::numeric(-a)),
        };
    }

    Err(yachtsql_core::error::Error::TypeMismatch {
        expected: "SAFE_NEGATE requires numeric type".to_string(),
        actual: value.data_type().to_string(),
    })
}
