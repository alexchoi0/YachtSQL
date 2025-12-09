mod aes_encrypt;
mod blake3;
mod crc32;
mod farm_fingerprint;
mod from_base64;
mod from_hex;
mod md5;
mod net_functions;
mod non_crypto_hash;
mod sha1;
mod sha224;
mod sha256;
mod sha384;
mod sha512;
mod to_base64;
mod to_hex;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_crypto_hash_network_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        match name {
            "MD5" => Self::eval_md5_with_dialect(args, batch, row_idx, dialect),
            "SHA1" => Self::eval_sha1_with_dialect(args, batch, row_idx, dialect),
            "SHA224" => Self::eval_sha224_with_dialect(args, batch, row_idx, dialect),
            "SHA256" => Self::eval_sha256_with_dialect(args, batch, row_idx, dialect),
            "SHA384" => Self::eval_sha384_with_dialect(args, batch, row_idx, dialect),
            "SHA512" => Self::eval_sha512_with_dialect(args, batch, row_idx, dialect),
            "BLAKE3" => Self::eval_blake3_with_dialect(args, batch, row_idx, dialect),
            "FARM_FINGERPRINT" => Self::eval_farm_fingerprint(args, batch, row_idx),
            "TO_HEX" => Self::eval_to_hex(args, batch, row_idx),
            "FROM_HEX" => Self::eval_from_hex(args, batch, row_idx),
            "TO_BASE64" => Self::eval_to_base64(args, batch, row_idx),
            "FROM_BASE64" => Self::eval_from_base64(args, batch, row_idx),
            "GEN_RANDOM_BYTES" => Self::eval_gen_random_bytes(args, batch, row_idx),
            "DIGEST" => Self::eval_digest(args, batch, row_idx),
            "ENCODE" => Self::eval_pgcrypto_encode(args, batch, row_idx),
            "DECODE" => Self::eval_pgcrypto_decode(args, batch, row_idx),
            "CRC32" => Self::eval_crc32(args, batch, row_idx),
            "CRC32C" => Self::eval_crc32c(args, batch, row_idx),
            "XXHASH32" => Self::eval_xxhash32(args, batch, row_idx),
            "XXHASH64" => Self::eval_xxhash64(args, batch, row_idx),
            "CITYHASH64" => Self::eval_cityhash64(args, batch, row_idx),
            "SIPHASH64" => Self::eval_siphash64(args, batch, row_idx),
            "MURMURHASH2_32" => Self::eval_murmurhash2_32(args, batch, row_idx),
            "MURMURHASH2_64" => Self::eval_murmurhash2_64(args, batch, row_idx),
            "MURMURHASH3_32" => Self::eval_murmurhash3_32(args, batch, row_idx),
            "MURMURHASH3_64" => Self::eval_murmurhash3_64(args, batch, row_idx),
            "MURMURHASH3_128" => Self::eval_murmurhash3_128(args, batch, row_idx),
            "JAVAHASH" => Self::eval_javahash(args, batch, row_idx),
            "HALFMD5" => Self::eval_halfmd5(args, batch, row_idx),
            "FARMHASH64" => Self::eval_farmhash64(args, batch, row_idx),
            "METROHASH64" => Self::eval_metrohash64(args, batch, row_idx),
            name if name.starts_with("NET.") => Self::eval_net_function(name, args, batch, row_idx),
            _ => Err(Error::unsupported_feature(format!(
                "Unknown crypto/hash/network function: {}",
                name
            ))),
        }
    }

    fn eval_gen_random_bytes(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("GEN_RANDOM_BYTES requires 1 argument"));
        }
        let len_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let len = len_val
            .as_i64()
            .ok_or_else(|| Error::invalid_query("GEN_RANDOM_BYTES length must be an integer"))?;
        if len < 0 || len > 1024 {
            return Err(Error::invalid_query(
                "GEN_RANDOM_BYTES length must be between 0 and 1024",
            ));
        }
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let bytes: Vec<u8> = (0..len as usize).map(|_| rng.r#gen::<u8>()).collect();
        Ok(Value::bytes(bytes))
    }

    fn eval_digest(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query("DIGEST requires 2 arguments"));
        }
        let data_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let algo_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        let algo = algo_val
            .as_str()
            .ok_or_else(|| Error::invalid_query("DIGEST algorithm must be a string"))?;

        match algo.to_lowercase().as_str() {
            "sha256" => yachtsql_functions::scalar::eval_sha256(&data_val, false),
            "sha512" => yachtsql_functions::scalar::eval_sha512(&data_val, false),
            "md5" => yachtsql_functions::scalar::eval_md5(&data_val, false),
            "sha1" => yachtsql_functions::scalar::eval_sha1(&data_val, false),
            _ => Err(Error::invalid_query(format!(
                "Unsupported digest algorithm: {}",
                algo
            ))),
        }
    }

    fn eval_pgcrypto_encode(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query("ENCODE requires 2 arguments"));
        }
        let data_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let format_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        let data_bytes = data_val
            .as_bytes()
            .ok_or_else(|| Error::invalid_query("ENCODE data must be bytes"))?;

        let format = format_val
            .as_str()
            .ok_or_else(|| Error::invalid_query("ENCODE format must be a string"))?;

        let result = match format.to_lowercase().as_str() {
            "hex" => hex::encode(data_bytes),
            "base64" => {
                base64::Engine::encode(&base64::engine::general_purpose::STANDARD, data_bytes)
            }
            "escape" => String::from_utf8_lossy(data_bytes).to_string(),
            _ => {
                return Err(Error::invalid_query(format!(
                    "Unsupported encoding format: {}",
                    format
                )));
            }
        };
        Ok(Value::string(result))
    }

    fn eval_pgcrypto_decode(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query("DECODE requires 2 arguments"));
        }
        let data_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let format_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        let data_str = data_val
            .as_str()
            .ok_or_else(|| Error::invalid_query("DECODE data must be a string"))?;

        let format = format_val
            .as_str()
            .ok_or_else(|| Error::invalid_query("DECODE format must be a string"))?;

        let result = match format.to_lowercase().as_str() {
            "hex" => hex::decode(data_str)
                .map_err(|e| Error::invalid_query(format!("Invalid hex string: {}", e)))?,
            "base64" => {
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data_str)
                    .map_err(|e| Error::invalid_query(format!("Invalid base64 string: {}", e)))?
            }
            "escape" => data_str.as_bytes().to_vec(),
            _ => {
                return Err(Error::invalid_query(format!(
                    "Unsupported decoding format: {}",
                    format
                )));
            }
        };
        Ok(Value::bytes(result))
    }
}
