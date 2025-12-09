use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    fn get_bytes_for_hash(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Option<Vec<u8>>> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "Hash function requires at least 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(None);
        }
        if let Some(s) = val.as_str() {
            return Ok(Some(s.as_bytes().to_vec()));
        }
        if let Some(b) = val.as_bytes() {
            return Ok(Some(b.to_vec()));
        }
        if let Some(i) = val.as_i64() {
            return Ok(Some(i.to_string().as_bytes().to_vec()));
        }
        Err(Error::invalid_query(
            "Hash function argument must be a string, bytes, or integer",
        ))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_xxhash32(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let bytes = match Self::get_bytes_for_hash(args, batch, row_idx)? {
            Some(b) => b,
            None => return Ok(Value::null()),
        };
        let hash = xxhash_rust::xxh32::xxh32(&bytes, 0);
        Ok(Value::int64(hash as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_xxhash64(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let bytes = match Self::get_bytes_for_hash(args, batch, row_idx)? {
            Some(b) => b,
            None => return Ok(Value::null()),
        };
        let hash = xxhash_rust::xxh64::xxh64(&bytes, 0);
        Ok(Value::int64(hash as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_cityhash64(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let bytes = match Self::get_bytes_for_hash(args, batch, row_idx)? {
            Some(b) => b,
            None => return Ok(Value::null()),
        };
        use std::hash::Hasher;
        let mut hasher = cityhasher::CityHasher::new();
        hasher.write(&bytes);
        let hash = hasher.finish();
        Ok(Value::int64(hash as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_siphash64(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let bytes = match Self::get_bytes_for_hash(args, batch, row_idx)? {
            Some(b) => b,
            None => return Ok(Value::null()),
        };
        use std::hash::Hasher;

        use siphasher::sip::SipHasher;
        let mut hasher = SipHasher::new();
        hasher.write(&bytes);
        let hash = hasher.finish();
        Ok(Value::int64(hash as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_murmurhash2_32(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let bytes = match Self::get_bytes_for_hash(args, batch, row_idx)? {
            Some(b) => b,
            None => return Ok(Value::null()),
        };
        let hash = murmur2_hash32(&bytes, 0);
        Ok(Value::int64(hash as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_murmurhash2_64(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let bytes = match Self::get_bytes_for_hash(args, batch, row_idx)? {
            Some(b) => b,
            None => return Ok(Value::null()),
        };
        let hash = murmur2_hash64(&bytes, 0);
        Ok(Value::int64(hash as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_murmurhash3_32(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let bytes = match Self::get_bytes_for_hash(args, batch, row_idx)? {
            Some(b) => b,
            None => return Ok(Value::null()),
        };
        let hash = murmur3::murmur3_32(&mut std::io::Cursor::new(&bytes), 0)
            .expect("murmur3_32 should not fail");
        Ok(Value::int64(hash as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_murmurhash3_64(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let bytes = match Self::get_bytes_for_hash(args, batch, row_idx)? {
            Some(b) => b,
            None => return Ok(Value::null()),
        };
        let hash = murmur3::murmur3_x64_128(&mut std::io::Cursor::new(&bytes), 0)
            .expect("murmur3_x64_128 should not fail");
        Ok(Value::int64(hash as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_murmurhash3_128(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let bytes = match Self::get_bytes_for_hash(args, batch, row_idx)? {
            Some(b) => b,
            None => return Ok(Value::null()),
        };
        let hash = murmur3::murmur3_x64_128(&mut std::io::Cursor::new(&bytes), 0)
            .expect("murmur3_x64_128 should not fail");
        Ok(Value::string(format!("{:032x}", hash)))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_javahash(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let bytes = match Self::get_bytes_for_hash(args, batch, row_idx)? {
            Some(b) => b,
            None => return Ok(Value::null()),
        };
        let mut hash: i32 = 0;
        for &b in &bytes {
            hash = hash.wrapping_mul(31).wrapping_add(b as i32);
        }
        Ok(Value::int64(hash as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_halfmd5(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let bytes = match Self::get_bytes_for_hash(args, batch, row_idx)? {
            Some(b) => b,
            None => return Ok(Value::null()),
        };
        let digest = md5::compute(&bytes);
        let first_8_bytes: [u8; 8] = digest[0..8].try_into().unwrap();
        let hash = u64::from_be_bytes(first_8_bytes);
        Ok(Value::int64(hash as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_farmhash64(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let bytes = match Self::get_bytes_for_hash(args, batch, row_idx)? {
            Some(b) => b,
            None => return Ok(Value::null()),
        };
        let hash = farmhash::hash64(&bytes);
        Ok(Value::int64(hash as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_metrohash64(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let bytes = match Self::get_bytes_for_hash(args, batch, row_idx)? {
            Some(b) => b,
            None => return Ok(Value::null()),
        };
        use std::hash::Hasher;

        use metrohash::MetroHash64;
        let mut hasher = MetroHash64::new();
        hasher.write(&bytes);
        let hash = hasher.finish();
        Ok(Value::int64(hash as i64))
    }
}

fn murmur2_hash32(data: &[u8], seed: u32) -> u32 {
    const M: u32 = 0x5bd1e995;
    const R: u32 = 24;

    let len = data.len();
    let mut h: u32 = seed ^ (len as u32);
    let mut idx = 0;

    while idx + 4 <= len {
        let mut k = u32::from_le_bytes([data[idx], data[idx + 1], data[idx + 2], data[idx + 3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
        idx += 4;
    }

    match len - idx {
        3 => {
            h ^= (data[idx + 2] as u32) << 16;
            h ^= (data[idx + 1] as u32) << 8;
            h ^= data[idx] as u32;
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= (data[idx + 1] as u32) << 8;
            h ^= data[idx] as u32;
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= data[idx] as u32;
            h = h.wrapping_mul(M);
        }
        _ => {}
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;

    h
}

fn murmur2_hash64(data: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;

    let len = data.len();
    let mut h: u64 = seed ^ ((len as u64).wrapping_mul(M));
    let mut idx = 0;

    while idx + 8 <= len {
        let mut k = u64::from_le_bytes([
            data[idx],
            data[idx + 1],
            data[idx + 2],
            data[idx + 3],
            data[idx + 4],
            data[idx + 5],
            data[idx + 6],
            data[idx + 7],
        ]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
        idx += 8;
    }

    let remaining = len - idx;
    if remaining > 0 {
        let mut k: u64 = 0;
        for i in (0..remaining).rev() {
            k = (k << 8) | (data[idx + i] as u64);
        }
        h ^= k;
        h = h.wrapping_mul(M);
    }

    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;

    h
}
