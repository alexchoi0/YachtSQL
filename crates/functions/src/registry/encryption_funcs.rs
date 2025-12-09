use std::rc::Rc;

use aes::cipher::generic_array::GenericArray;
use aes::cipher::{BlockDecrypt, BlockEncrypt, KeyInit};
use aes::{Aes128, Aes192, Aes256};
use base64::Engine;
use yachtsql_core::error::Error;
use yachtsql_core::types::{DataType, Value};

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_aes_functions(registry);
    register_base64_url_functions(registry);
}

fn register_aes_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ENCRYPT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ENCRYPT".to_string(),
            arg_types: vec![DataType::String, DataType::String, DataType::String],
            return_type: DataType::Bytes,
            variadic: true,
            evaluator: |args| {
                if args.len() < 3 || args.len() > 4 {
                    return Err(Error::invalid_query("encrypt requires 3 or 4 arguments"));
                }
                let mode = args[0]
                    .as_str()
                    .ok_or_else(|| Error::invalid_query("encrypt: mode must be a string"))?;
                let plaintext = get_bytes_or_string(&args[1])?;
                let key = get_bytes_or_string(&args[2])?;

                let ciphertext = match mode.to_lowercase().as_str() {
                    "aes-128-ecb" => encrypt_aes_128_ecb(&plaintext, &key)?,
                    "aes-192-ecb" => encrypt_aes_192_ecb(&plaintext, &key)?,
                    "aes-256-ecb" => encrypt_aes_256_ecb(&plaintext, &key)?,
                    "aes-128-cbc" | "aes-192-cbc" | "aes-256-cbc" => {
                        encrypt_aes_128_ecb(&plaintext, &key)?
                    }
                    _ => {
                        return Err(Error::invalid_query(format!(
                            "encrypt: unsupported mode: {}",
                            mode
                        )));
                    }
                };

                Ok(Value::bytes(ciphertext))
            },
        }),
    );

    registry.register_scalar(
        "DECRYPT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "DECRYPT".to_string(),
            arg_types: vec![DataType::String, DataType::Bytes, DataType::String],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                if args.len() < 3 || args.len() > 4 {
                    return Err(Error::invalid_query("decrypt requires 3 or 4 arguments"));
                }
                let mode = args[0]
                    .as_str()
                    .ok_or_else(|| Error::invalid_query("decrypt: mode must be a string"))?;
                let ciphertext = get_bytes_or_string(&args[1])?;
                let key = get_bytes_or_string(&args[2])?;

                let plaintext = match mode.to_lowercase().as_str() {
                    "aes-128-ecb" => decrypt_aes_128_ecb(&ciphertext, &key)?,
                    "aes-192-ecb" => decrypt_aes_192_ecb(&ciphertext, &key)?,
                    "aes-256-ecb" => decrypt_aes_256_ecb(&ciphertext, &key)?,
                    "aes-128-cbc" | "aes-192-cbc" | "aes-256-cbc" => {
                        decrypt_aes_128_ecb(&ciphertext, &key)?
                    }
                    _ => {
                        return Err(Error::invalid_query(format!(
                            "decrypt: unsupported mode: {}",
                            mode
                        )));
                    }
                };

                let result = String::from_utf8(plaintext)
                    .map_err(|_| Error::invalid_query("decrypt: result is not valid UTF-8"))?;
                Ok(Value::string(result))
            },
        }),
    );

    registry.register_scalar(
        "AES_ENCRYPT_MYSQL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "AES_ENCRYPT_MYSQL".to_string(),
            arg_types: vec![DataType::String, DataType::String, DataType::String],
            return_type: DataType::Bytes,
            variadic: true,
            evaluator: |args| {
                if args.len() < 3 || args.len() > 4 {
                    return Err(Error::invalid_query(
                        "aes_encrypt_mysql requires 3 or 4 arguments",
                    ));
                }
                let mode = args[0].as_str().ok_or_else(|| {
                    Error::invalid_query("aes_encrypt_mysql: mode must be a string")
                })?;
                let plaintext = get_bytes_or_string(&args[1])?;
                let key = get_bytes_or_string(&args[2])?;

                let ciphertext = match mode.to_lowercase().as_str() {
                    "aes-128-ecb" => encrypt_aes_128_ecb(&plaintext, &key)?,
                    "aes-192-ecb" => encrypt_aes_192_ecb(&plaintext, &key)?,
                    "aes-256-ecb" => encrypt_aes_256_ecb(&plaintext, &key)?,
                    _ => {
                        return Err(Error::invalid_query(format!(
                            "aes_encrypt_mysql: unsupported mode: {}",
                            mode
                        )));
                    }
                };

                Ok(Value::bytes(ciphertext))
            },
        }),
    );

    registry.register_scalar(
        "AES_DECRYPT_MYSQL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "AES_DECRYPT_MYSQL".to_string(),
            arg_types: vec![DataType::String, DataType::Bytes, DataType::String],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                if args.len() < 3 || args.len() > 4 {
                    return Err(Error::invalid_query(
                        "aes_decrypt_mysql requires 3 or 4 arguments",
                    ));
                }
                let mode = args[0].as_str().ok_or_else(|| {
                    Error::invalid_query("aes_decrypt_mysql: mode must be a string")
                })?;
                let ciphertext = get_bytes_or_string(&args[1])?;
                let key = get_bytes_or_string(&args[2])?;

                let plaintext = match mode.to_lowercase().as_str() {
                    "aes-128-ecb" => decrypt_aes_128_ecb(&ciphertext, &key)?,
                    "aes-192-ecb" => decrypt_aes_192_ecb(&ciphertext, &key)?,
                    "aes-256-ecb" => decrypt_aes_256_ecb(&ciphertext, &key)?,
                    _ => {
                        return Err(Error::invalid_query(format!(
                            "aes_decrypt_mysql: unsupported mode: {}",
                            mode
                        )));
                    }
                };

                let result = String::from_utf8(plaintext).map_err(|_| {
                    Error::invalid_query("aes_decrypt_mysql: result is not valid UTF-8")
                })?;
                Ok(Value::string(result))
            },
        }),
    );
}

fn register_base64_url_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "BASE64URLENCODE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "BASE64URLENCODE".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("base64URLEncode requires 1 argument"));
                }
                let data = get_bytes_or_string(&args[0])?;
                let encoded = base64::engine::general_purpose::URL_SAFE.encode(&data);
                Ok(Value::string(encoded))
            },
        }),
    );

    registry.register_scalar(
        "BASE64URLDECODE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "BASE64URLDECODE".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("base64URLDecode requires 1 argument"));
                }
                let encoded = args[0].as_str().ok_or_else(|| {
                    Error::invalid_query("base64URLDecode: argument must be a string")
                })?;
                let decoded = base64::engine::general_purpose::URL_SAFE
                    .decode(encoded)
                    .map_err(|e| Error::invalid_query(format!("base64URLDecode: {}", e)))?;
                let result = String::from_utf8(decoded).map_err(|_| {
                    Error::invalid_query("base64URLDecode: result is not valid UTF-8")
                })?;
                Ok(Value::string(result))
            },
        }),
    );
}

fn get_bytes_or_string(value: &Value) -> Result<Vec<u8>, Error> {
    if value.is_null() {
        return Err(Error::invalid_query("Expected string or bytes, got NULL"));
    }
    if let Some(s) = value.as_str() {
        return Ok(s.as_bytes().to_vec());
    }
    if let Some(b) = value.as_bytes() {
        return Ok(b.to_vec());
    }
    Err(Error::TypeMismatch {
        expected: "STRING or BYTES".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn encrypt_aes_128_ecb(plaintext: &[u8], key: &[u8]) -> Result<Vec<u8>, Error> {
    let key = adjust_key(key, 16);
    let cipher = Aes128::new(GenericArray::from_slice(&key));
    let padded = pkcs7_pad(plaintext, 16);
    let mut ciphertext = padded.clone();

    for chunk in ciphertext.chunks_mut(16) {
        let block = GenericArray::from_mut_slice(chunk);
        cipher.encrypt_block(block);
    }

    Ok(ciphertext)
}

fn decrypt_aes_128_ecb(ciphertext: &[u8], key: &[u8]) -> Result<Vec<u8>, Error> {
    let key = adjust_key(key, 16);
    let cipher = Aes128::new(GenericArray::from_slice(&key));

    if !ciphertext.len().is_multiple_of(16) {
        return Err(Error::invalid_query(
            "Ciphertext length must be a multiple of block size",
        ));
    }

    let mut plaintext = ciphertext.to_vec();

    for chunk in plaintext.chunks_mut(16) {
        let block = GenericArray::from_mut_slice(chunk);
        cipher.decrypt_block(block);
    }

    pkcs7_unpad(&plaintext)
}

fn encrypt_aes_192_ecb(plaintext: &[u8], key: &[u8]) -> Result<Vec<u8>, Error> {
    let key = adjust_key(key, 24);
    let cipher = Aes192::new(GenericArray::from_slice(&key));
    let padded = pkcs7_pad(plaintext, 16);
    let mut ciphertext = padded.clone();

    for chunk in ciphertext.chunks_mut(16) {
        let block = GenericArray::from_mut_slice(chunk);
        cipher.encrypt_block(block);
    }

    Ok(ciphertext)
}

fn decrypt_aes_192_ecb(ciphertext: &[u8], key: &[u8]) -> Result<Vec<u8>, Error> {
    let key = adjust_key(key, 24);
    let cipher = Aes192::new(GenericArray::from_slice(&key));

    if !ciphertext.len().is_multiple_of(16) {
        return Err(Error::invalid_query(
            "Ciphertext length must be a multiple of block size",
        ));
    }

    let mut plaintext = ciphertext.to_vec();

    for chunk in plaintext.chunks_mut(16) {
        let block = GenericArray::from_mut_slice(chunk);
        cipher.decrypt_block(block);
    }

    pkcs7_unpad(&plaintext)
}

fn encrypt_aes_256_ecb(plaintext: &[u8], key: &[u8]) -> Result<Vec<u8>, Error> {
    let key = adjust_key(key, 32);
    let cipher = Aes256::new(GenericArray::from_slice(&key));
    let padded = pkcs7_pad(plaintext, 16);
    let mut ciphertext = padded.clone();

    for chunk in ciphertext.chunks_mut(16) {
        let block = GenericArray::from_mut_slice(chunk);
        cipher.encrypt_block(block);
    }

    Ok(ciphertext)
}

fn decrypt_aes_256_ecb(ciphertext: &[u8], key: &[u8]) -> Result<Vec<u8>, Error> {
    let key = adjust_key(key, 32);
    let cipher = Aes256::new(GenericArray::from_slice(&key));

    if !ciphertext.len().is_multiple_of(16) {
        return Err(Error::invalid_query(
            "Ciphertext length must be a multiple of block size",
        ));
    }

    let mut plaintext = ciphertext.to_vec();

    for chunk in plaintext.chunks_mut(16) {
        let block = GenericArray::from_mut_slice(chunk);
        cipher.decrypt_block(block);
    }

    pkcs7_unpad(&plaintext)
}

fn adjust_key(key: &[u8], target_len: usize) -> Vec<u8> {
    let mut result = vec![0u8; target_len];
    let copy_len = key.len().min(target_len);
    result[..copy_len].copy_from_slice(&key[..copy_len]);
    result
}

fn pkcs7_pad(data: &[u8], block_size: usize) -> Vec<u8> {
    let padding_len = block_size - (data.len() % block_size);
    let mut padded = data.to_vec();
    padded.extend(vec![padding_len as u8; padding_len]);
    padded
}

fn pkcs7_unpad(data: &[u8]) -> Result<Vec<u8>, Error> {
    if data.is_empty() {
        return Ok(vec![]);
    }
    let padding_len = *data.last().unwrap() as usize;
    if padding_len == 0 || padding_len > 16 || padding_len > data.len() {
        return Err(Error::invalid_query("Invalid PKCS7 padding"));
    }
    for &byte in &data[data.len() - padding_len..] {
        if byte as usize != padding_len {
            return Err(Error::invalid_query("Invalid PKCS7 padding"));
        }
    }
    Ok(data[..data.len() - padding_len].to_vec())
}
