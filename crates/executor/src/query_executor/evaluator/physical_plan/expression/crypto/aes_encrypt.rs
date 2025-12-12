use aes::cipher::block_padding::Pkcs7;
use aes::cipher::{BlockDecryptMut, BlockEncryptMut, KeyInit, KeyIvInit};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

type Aes128EcbEnc = ecb::Encryptor<aes::Aes128>;
type Aes128EcbDec = ecb::Decryptor<aes::Aes128>;
type Aes256EcbEnc = ecb::Encryptor<aes::Aes256>;
type Aes256EcbDec = ecb::Decryptor<aes::Aes256>;
type Aes128CbcEnc = cbc::Encryptor<aes::Aes128>;
type Aes128CbcDec = cbc::Decryptor<aes::Aes128>;
type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;
type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;

fn get_key_bytes(key: &str, required_len: usize) -> Result<Vec<u8>> {
    let key_bytes = key.as_bytes();
    if key_bytes.len() < required_len {
        let mut padded = key_bytes.to_vec();
        padded.resize(required_len, 0);
        Ok(padded)
    } else {
        Ok(key_bytes[..required_len].to_vec())
    }
}

fn get_iv_bytes(iv: Option<&str>) -> Result<[u8; 16]> {
    match iv {
        Some(iv_str) => {
            let iv_bytes = iv_str.as_bytes();
            if iv_bytes.len() < 16 {
                let mut padded = [0u8; 16];
                padded[..iv_bytes.len()].copy_from_slice(iv_bytes);
                Ok(padded)
            } else {
                let mut arr = [0u8; 16];
                arr.copy_from_slice(&iv_bytes[..16]);
                Ok(arr)
            }
        }
        None => Ok([0u8; 16]),
    }
}

fn aes_encrypt_impl(mode: &str, plaintext: &[u8], key: &str, iv: Option<&str>) -> Result<Value> {
    let mode_lower = mode.to_lowercase();

    match mode_lower.as_str() {
        "aes-128-ecb" => {
            let key_bytes = get_key_bytes(key, 16)?;
            let key_arr: [u8; 16] = key_bytes.try_into().unwrap();
            let cipher = Aes128EcbEnc::new(&key_arr.into());
            let block_size = 16;
            let padding_needed = block_size - (plaintext.len() % block_size);
            let padded_len = plaintext.len() + padding_needed;
            let mut buf = vec![0u8; padded_len];
            buf[..plaintext.len()].copy_from_slice(plaintext);
            let ciphertext = cipher
                .encrypt_padded_b2b_mut::<Pkcs7>(plaintext, &mut buf)
                .map_err(|_| Error::invalid_query("Encryption failed"))?;
            Ok(Value::bytes(ciphertext.to_vec()))
        }
        "aes-256-ecb" => {
            let key_bytes = get_key_bytes(key, 32)?;
            let key_arr: [u8; 32] = key_bytes.try_into().unwrap();
            let cipher = Aes256EcbEnc::new(&key_arr.into());
            let block_size = 16;
            let padding_needed = block_size - (plaintext.len() % block_size);
            let padded_len = plaintext.len() + padding_needed;
            let mut buf = vec![0u8; padded_len];
            buf[..plaintext.len()].copy_from_slice(plaintext);
            let ciphertext = cipher
                .encrypt_padded_b2b_mut::<Pkcs7>(plaintext, &mut buf)
                .map_err(|_| Error::invalid_query("Encryption failed"))?;
            Ok(Value::bytes(ciphertext.to_vec()))
        }
        "aes-128-cbc" => {
            let key_bytes = get_key_bytes(key, 16)?;
            let key_arr: [u8; 16] = key_bytes.try_into().unwrap();
            let iv_arr = get_iv_bytes(iv)?;
            let cipher = Aes128CbcEnc::new(&key_arr.into(), &iv_arr.into());
            let block_size = 16;
            let padding_needed = block_size - (plaintext.len() % block_size);
            let padded_len = plaintext.len() + padding_needed;
            let mut buf = vec![0u8; padded_len];
            let ciphertext = cipher
                .encrypt_padded_b2b_mut::<Pkcs7>(plaintext, &mut buf)
                .map_err(|_| Error::invalid_query("Encryption failed"))?;
            Ok(Value::bytes(ciphertext.to_vec()))
        }
        "aes-256-cbc" => {
            let key_bytes = get_key_bytes(key, 32)?;
            let key_arr: [u8; 32] = key_bytes.try_into().unwrap();
            let iv_arr = get_iv_bytes(iv)?;
            let cipher = Aes256CbcEnc::new(&key_arr.into(), &iv_arr.into());
            let block_size = 16;
            let padding_needed = block_size - (plaintext.len() % block_size);
            let padded_len = plaintext.len() + padding_needed;
            let mut buf = vec![0u8; padded_len];
            let ciphertext = cipher
                .encrypt_padded_b2b_mut::<Pkcs7>(plaintext, &mut buf)
                .map_err(|_| Error::invalid_query("Encryption failed"))?;
            Ok(Value::bytes(ciphertext.to_vec()))
        }
        _ => Err(Error::invalid_query(format!(
            "Unsupported encryption mode: {}",
            mode
        ))),
    }
}

fn aes_decrypt_impl(mode: &str, ciphertext: &[u8], key: &str, iv: Option<&str>) -> Result<Value> {
    let mode_lower = mode.to_lowercase();

    match mode_lower.as_str() {
        "aes-128-ecb" => {
            let key_bytes = get_key_bytes(key, 16)?;
            let key_arr: [u8; 16] = key_bytes.try_into().unwrap();
            let cipher = Aes128EcbDec::new(&key_arr.into());
            let mut buf = ciphertext.to_vec();
            let plaintext = cipher.decrypt_padded_mut::<Pkcs7>(&mut buf).map_err(|_| {
                Error::invalid_query("Decryption failed - invalid key or corrupted data")
            })?;
            match String::from_utf8(plaintext.to_vec()) {
                Ok(s) => Ok(Value::string(s)),
                Err(_) => Ok(Value::bytes(plaintext.to_vec())),
            }
        }
        "aes-256-ecb" => {
            let key_bytes = get_key_bytes(key, 32)?;
            let key_arr: [u8; 32] = key_bytes.try_into().unwrap();
            let cipher = Aes256EcbDec::new(&key_arr.into());
            let mut buf = ciphertext.to_vec();
            let plaintext = cipher.decrypt_padded_mut::<Pkcs7>(&mut buf).map_err(|_| {
                Error::invalid_query("Decryption failed - invalid key or corrupted data")
            })?;
            match String::from_utf8(plaintext.to_vec()) {
                Ok(s) => Ok(Value::string(s)),
                Err(_) => Ok(Value::bytes(plaintext.to_vec())),
            }
        }
        "aes-128-cbc" => {
            let key_bytes = get_key_bytes(key, 16)?;
            let key_arr: [u8; 16] = key_bytes.try_into().unwrap();
            let iv_arr = get_iv_bytes(iv)?;
            let cipher = Aes128CbcDec::new(&key_arr.into(), &iv_arr.into());
            let mut buf = ciphertext.to_vec();
            let plaintext = cipher.decrypt_padded_mut::<Pkcs7>(&mut buf).map_err(|_| {
                Error::invalid_query("Decryption failed - invalid key or corrupted data")
            })?;
            match String::from_utf8(plaintext.to_vec()) {
                Ok(s) => Ok(Value::string(s)),
                Err(_) => Ok(Value::bytes(plaintext.to_vec())),
            }
        }
        "aes-256-cbc" => {
            let key_bytes = get_key_bytes(key, 32)?;
            let key_arr: [u8; 32] = key_bytes.try_into().unwrap();
            let iv_arr = get_iv_bytes(iv)?;
            let cipher = Aes256CbcDec::new(&key_arr.into(), &iv_arr.into());
            let mut buf = ciphertext.to_vec();
            let plaintext = cipher.decrypt_padded_mut::<Pkcs7>(&mut buf).map_err(|_| {
                Error::invalid_query("Decryption failed - invalid key or corrupted data")
            })?;
            match String::from_utf8(plaintext.to_vec()) {
                Ok(s) => Ok(Value::string(s)),
                Err(_) => Ok(Value::bytes(plaintext.to_vec())),
            }
        }
        _ => Err(Error::invalid_query(format!(
            "Unsupported encryption mode: {}",
            mode
        ))),
    }
}

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_encrypt(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "encrypt requires at least 3 arguments: mode, plaintext, key".to_string(),
            ));
        }

        let mode = Self::evaluate_expr(&args[0], batch, row_idx)?
            .as_str()
            .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
            .to_string();

        let plaintext_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let plaintext = if let Some(s) = plaintext_val.as_str() {
            s.as_bytes().to_vec()
        } else if let Some(b) = plaintext_val.as_bytes() {
            b.to_vec()
        } else {
            return Err(Error::type_mismatch("STRING or BYTES", "other"));
        };

        let key = Self::evaluate_expr(&args[2], batch, row_idx)?
            .as_str()
            .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
            .to_string();

        let iv = if args.len() > 3 {
            Some(
                Self::evaluate_expr(&args[3], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string(),
            )
        } else {
            None
        };

        aes_encrypt_impl(&mode, &plaintext, &key, iv.as_deref())
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_decrypt(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "decrypt requires at least 3 arguments: mode, ciphertext, key".to_string(),
            ));
        }

        let mode = Self::evaluate_expr(&args[0], batch, row_idx)?
            .as_str()
            .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
            .to_string();

        let ciphertext_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let ciphertext = if let Some(b) = ciphertext_val.as_bytes() {
            b.to_vec()
        } else if let Some(s) = ciphertext_val.as_str() {
            s.as_bytes().to_vec()
        } else {
            return Err(Error::type_mismatch("STRING or BYTES", "other"));
        };

        let key = Self::evaluate_expr(&args[2], batch, row_idx)?
            .as_str()
            .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
            .to_string();

        let iv = if args.len() > 3 {
            Some(
                Self::evaluate_expr(&args[3], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string(),
            )
        } else {
            None
        };

        aes_decrypt_impl(&mode, &ciphertext, &key, iv.as_deref())
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_aes_encrypt_mysql(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "aes_encrypt_mysql requires at least 3 arguments: mode, plaintext, key".to_string(),
            ));
        }
        Self::eval_encrypt(args, batch, row_idx)
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_aes_decrypt_mysql(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "aes_decrypt_mysql requires at least 3 arguments: mode, ciphertext, key"
                    .to_string(),
            ));
        }
        Self::eval_decrypt(args, batch, row_idx)
    }
}
