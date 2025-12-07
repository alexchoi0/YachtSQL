use std::rc::Rc;

use yachtsql_core::error::Result;
use yachtsql_core::types::{DataType, Value};

use crate::scalar::{self, ScalarFunctionImpl};

pub(super) fn register(registry: &mut super::FunctionRegistry) {
    register_uuid_functions(registry);
    register_crypto_functions(registry);
}

fn register_uuid_functions(registry: &mut super::FunctionRegistry) {
    registry.register_scalar(
        "GEN_RANDOM_UUID".to_string(),
        Rc::new(ScalarFunctionImpl::new(
            "gen_random_uuid".to_string(),
            vec![],
            DataType::String,
            false,
            eval_gen_random_uuid,
        )),
    );

    registry.register_scalar(
        "UUID_GENERATE_V4".to_string(),
        Rc::new(ScalarFunctionImpl::new(
            "uuid_generate_v4".to_string(),
            vec![],
            DataType::String,
            false,
            eval_gen_random_uuid,
        )),
    );

    registry.register_scalar(
        "UUID_NIL".to_string(),
        Rc::new(ScalarFunctionImpl::new(
            "uuid_nil".to_string(),
            vec![],
            DataType::String,
            false,
            eval_uuid_nil,
        )),
    );

    registry.register_scalar(
        "UUIDV7".to_string(),
        Rc::new(ScalarFunctionImpl::new(
            "uuidv7".to_string(),
            vec![],
            DataType::String,
            false,
            eval_uuidv7,
        )),
    );

    registry.register_scalar(
        "UUIDV4".to_string(),
        Rc::new(ScalarFunctionImpl::new(
            "uuidv4".to_string(),
            vec![],
            DataType::String,
            false,
            eval_gen_random_uuid,
        )),
    );
}

fn register_crypto_functions(registry: &mut super::FunctionRegistry) {
    registry.register_scalar(
        "GEN_RANDOM_BYTES".to_string(),
        Rc::new(ScalarFunctionImpl::new(
            "gen_random_bytes".to_string(),
            vec![DataType::Int64],
            DataType::Bytes,
            false,
            eval_gen_random_bytes,
        )),
    );

    registry.register_scalar(
        "MD5".to_string(),
        Rc::new(ScalarFunctionImpl::new(
            "md5".to_string(),
            vec![DataType::String],
            DataType::Bytes,
            false,
            eval_md5,
        )),
    );

    registry.register_scalar(
        "SHA256".to_string(),
        Rc::new(ScalarFunctionImpl::new(
            "sha256".to_string(),
            vec![DataType::String],
            DataType::Bytes,
            false,
            eval_sha256,
        )),
    );

    registry.register_scalar(
        "SHA512".to_string(),
        Rc::new(ScalarFunctionImpl::new(
            "sha512".to_string(),
            vec![DataType::String],
            DataType::Bytes,
            false,
            eval_sha512,
        )),
    );

    registry.register_scalar(
        "DIGEST".to_string(),
        Rc::new(ScalarFunctionImpl::new(
            "digest".to_string(),
            vec![DataType::String, DataType::String],
            DataType::Bytes,
            false,
            eval_digest,
        )),
    );

    registry.register_scalar(
        "ENCODE".to_string(),
        Rc::new(ScalarFunctionImpl::new(
            "encode".to_string(),
            vec![DataType::Bytes, DataType::String],
            DataType::String,
            false,
            eval_encode,
        )),
    );

    registry.register_scalar(
        "DECODE".to_string(),
        Rc::new(ScalarFunctionImpl::new(
            "decode".to_string(),
            vec![DataType::String, DataType::String],
            DataType::Bytes,
            false,
            eval_decode,
        )),
    );
}

fn eval_gen_random_uuid(_args: &[Value]) -> Result<Value> {
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes);

    bytes[6] = (bytes[6] & 0x0f) | 0x40;

    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    let uuid = format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    );

    Ok(Value::string(uuid))
}

fn eval_uuid_nil(_args: &[Value]) -> Result<Value> {
    Ok(Value::string(
        "00000000-0000-0000-0000-000000000000".to_string(),
    ))
}

fn eval_uuidv7(_args: &[Value]) -> Result<Value> {
    use std::time::{SystemTime, UNIX_EPOCH};

    use rand::Rng;

    let mut rng = rand::thread_rng();
    let mut bytes = [0u8; 16];

    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    bytes[0] = ((timestamp_ms >> 40) & 0xFF) as u8;
    bytes[1] = ((timestamp_ms >> 32) & 0xFF) as u8;
    bytes[2] = ((timestamp_ms >> 24) & 0xFF) as u8;
    bytes[3] = ((timestamp_ms >> 16) & 0xFF) as u8;
    bytes[4] = ((timestamp_ms >> 8) & 0xFF) as u8;
    bytes[5] = (timestamp_ms & 0xFF) as u8;

    rng.fill(&mut bytes[6..]);

    bytes[6] = (bytes[6] & 0x0f) | 0x70;

    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    let uuid = format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    );

    Ok(Value::string(uuid))
}

fn eval_md5(args: &[Value]) -> Result<Value> {
    scalar::eval_md5(&args[0], false)
}

fn eval_sha256(args: &[Value]) -> Result<Value> {
    scalar::eval_sha256(&args[0], false)
}

fn eval_sha512(args: &[Value]) -> Result<Value> {
    scalar::eval_sha512(&args[0], false)
}

fn eval_gen_random_bytes(args: &[Value]) -> Result<Value> {
    use rand::Rng;
    use yachtsql_core::error::Error;

    if args.is_empty() {
        return Err(Error::invalid_query(
            "gen_random_bytes requires a length argument".to_string(),
        ));
    }

    if args[0].is_null() {
        return Ok(Value::null());
    }

    let length = args[0].as_i64().ok_or_else(|| Error::TypeMismatch {
        expected: "INT64".to_string(),
        actual: args[0].data_type().to_string(),
    })?;

    if length < 0 || length > 1024 * 1024 {
        return Err(Error::invalid_query(format!(
            "gen_random_bytes: length must be between 0 and 1048576, got {}",
            length
        )));
    }

    let mut rng = rand::thread_rng();
    let bytes: Vec<u8> = (0..length).map(|_| rng.r#gen()).collect();

    Ok(Value::bytes(bytes))
}

fn eval_digest(args: &[Value]) -> Result<Value> {
    use sha2::{Digest, Sha256, Sha512};
    use yachtsql_core::error::Error;

    if args.len() < 2 {
        return Err(Error::invalid_query(
            "digest requires two arguments: data and algorithm".to_string(),
        ));
    }

    if args[0].is_null() || args[1].is_null() {
        return Ok(Value::null());
    }

    let data = if let Some(s) = args[0].as_str() {
        s.as_bytes().to_vec()
    } else if let Some(b) = args[0].as_bytes() {
        b.to_vec()
    } else {
        return Err(Error::TypeMismatch {
            expected: "STRING or BYTES".to_string(),
            actual: args[0].data_type().to_string(),
        });
    };

    let algorithm = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
        expected: "STRING".to_string(),
        actual: args[1].data_type().to_string(),
    })?;

    let hash_bytes = match algorithm.to_lowercase().as_str() {
        "md5" => md5::compute(&data).to_vec(),
        "sha1" => {
            use sha1::{Digest as Sha1Digest, Sha1};
            let mut hasher = Sha1::new();
            hasher.update(&data);
            hasher.finalize().to_vec()
        }
        "sha256" => {
            let mut hasher = Sha256::new();
            hasher.update(&data);
            hasher.finalize().to_vec()
        }
        "sha512" => {
            let mut hasher = Sha512::new();
            hasher.update(&data);
            hasher.finalize().to_vec()
        }
        _ => {
            return Err(Error::invalid_query(format!(
                "digest: unknown algorithm '{}'. Supported: md5, sha1, sha256, sha512",
                algorithm
            )));
        }
    };

    Ok(Value::bytes(hash_bytes))
}

fn eval_encode(args: &[Value]) -> Result<Value> {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use yachtsql_core::error::Error;

    if args.len() < 2 {
        return Err(Error::invalid_query(
            "encode requires two arguments: data and format".to_string(),
        ));
    }

    if args[0].is_null() || args[1].is_null() {
        return Ok(Value::null());
    }

    let data = args[0].as_bytes().ok_or_else(|| Error::TypeMismatch {
        expected: "BYTES".to_string(),
        actual: args[0].data_type().to_string(),
    })?;

    let format = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
        expected: "STRING".to_string(),
        actual: args[1].data_type().to_string(),
    })?;

    let encoded = match format.to_lowercase().as_str() {
        "hex" => hex::encode(data),
        "base64" => STANDARD.encode(data),
        "escape" => {
            let mut result = String::new();
            for &byte in data {
                if byte >= 32 && byte < 127 && byte != b'\\' {
                    result.push(byte as char);
                } else {
                    result.push_str(&format!("\\{:03o}", byte));
                }
            }
            result
        }
        _ => {
            return Err(Error::invalid_query(format!(
                "encode: unknown format '{}'. Supported: hex, base64, escape",
                format
            )));
        }
    };

    Ok(Value::string(encoded))
}

fn eval_decode(args: &[Value]) -> Result<Value> {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use yachtsql_core::error::Error;

    if args.len() < 2 {
        return Err(Error::invalid_query(
            "decode requires two arguments: text and format".to_string(),
        ));
    }

    if args[0].is_null() || args[1].is_null() {
        return Ok(Value::null());
    }

    let text = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
        expected: "STRING".to_string(),
        actual: args[0].data_type().to_string(),
    })?;

    let format = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
        expected: "STRING".to_string(),
        actual: args[1].data_type().to_string(),
    })?;

    let decoded = match format.to_lowercase().as_str() {
        "hex" => hex::decode(text)
            .map_err(|e| Error::invalid_query(format!("decode: invalid hex string: {}", e)))?,
        "base64" => STANDARD
            .decode(text)
            .map_err(|e| Error::invalid_query(format!("decode: invalid base64 string: {}", e)))?,
        "escape" => {
            let mut result = Vec::new();
            let mut chars = text.chars().peekable();
            while let Some(c) = chars.next() {
                if c == '\\' {
                    let mut octal = String::new();
                    for _ in 0..3 {
                        if let Some(&next) = chars.peek()
                            && next >= '0'
                            && next <= '7'
                        {
                            octal.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }
                    if !octal.is_empty() {
                        let byte = u8::from_str_radix(&octal, 8).map_err(|_| {
                            Error::invalid_query(format!(
                                "decode: invalid escape sequence \\{}",
                                octal
                            ))
                        })?;
                        result.push(byte);
                    } else if let Some(&next) = chars.peek() {
                        if next == '\\' {
                            chars.next();
                            result.push(b'\\');
                        } else {
                            result.push(b'\\');
                        }
                    } else {
                        result.push(b'\\');
                    }
                } else {
                    result.push(c as u8);
                }
            }
            result
        }
        _ => {
            return Err(Error::invalid_query(format!(
                "decode: unknown format '{}'. Supported: hex, base64, escape",
                format
            )));
        }
    };

    Ok(Value::bytes(decoded))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gen_random_uuid() {
        let result = eval_gen_random_uuid(&[]).unwrap();
        let uuid = result.as_str().unwrap();

        assert_eq!(uuid.len(), 36);
        assert_eq!(uuid.chars().filter(|c| *c == '-').count(), 4);

        assert_eq!(uuid.chars().nth(14).unwrap(), '4');
    }

    #[test]
    fn test_uuid_nil() {
        let result = eval_uuid_nil(&[]).unwrap();
        assert_eq!(
            result.as_str().unwrap(),
            "00000000-0000-0000-0000-000000000000"
        );
    }

    #[test]
    fn test_gen_random_bytes() {
        let result = eval_gen_random_bytes(&[Value::int64(16)]).unwrap();
        let bytes = result.as_bytes().unwrap();
        assert_eq!(bytes.len(), 16);
    }

    #[test]
    fn test_digest_sha256() {
        let result = eval_digest(&[
            Value::string("hello".to_string()),
            Value::string("sha256".to_string()),
        ])
        .unwrap();
        let bytes = result.as_bytes().unwrap();
        assert_eq!(bytes.len(), 32);
    }

    #[test]
    fn test_encode_hex() {
        let result = eval_encode(&[
            Value::bytes(vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]),
            Value::string("hex".to_string()),
        ])
        .unwrap();
        assert_eq!(result.as_str().unwrap(), "48656c6c6f");
    }

    #[test]
    fn test_decode_hex() {
        let result = eval_decode(&[
            Value::string("48656c6c6f".to_string()),
            Value::string("hex".to_string()),
        ])
        .unwrap();
        assert_eq!(result.as_bytes().unwrap(), &[0x48, 0x65, 0x6c, 0x6c, 0x6f]);
    }
}
