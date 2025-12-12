use rand::Rng;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_keys_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let func_name = name.strip_prefix("KEYS.").unwrap_or(name);
        match func_name {
            "NEW_KEYSET" => Self::eval_keys_new_keyset(args, batch, row_idx),
            "ADD_KEY_FROM_RAW_BYTES" => {
                Self::eval_keys_add_key_from_raw_bytes(args, batch, row_idx)
            }
            "KEYSET_CHAIN" => Self::eval_keys_keyset_chain(args, batch, row_idx),
            "KEYSET_FROM_JSON" => Self::eval_keys_keyset_from_json(args, batch, row_idx),
            "KEYSET_TO_JSON" => Self::eval_keys_keyset_to_json(args, batch, row_idx),
            "ROTATE_KEYSET" => Self::eval_keys_rotate_keyset(args, batch, row_idx),
            "KEYSET_LENGTH" => Self::eval_keys_keyset_length(args, batch, row_idx),
            _ => Err(Error::unsupported_feature(format!(
                "Unknown KEYS function: {}",
                name
            ))),
        }
    }

    fn eval_keys_new_keyset(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "KEYS.NEW_KEYSET requires 1 argument (algorithm)".to_string(),
            ));
        }
        let algorithm = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let algo_str = algorithm.as_str().ok_or_else(|| {
            Error::invalid_query("KEYS.NEW_KEYSET algorithm must be a string".to_string())
        })?;

        let key_size = match algo_str.to_uppercase().as_str() {
            "AEAD_AES_GCM_256" => 32,
            "DETERMINISTIC_AEAD_AES_SIV_CMAC_256" => 64,
            _ => 32,
        };

        let mut rng = rand::thread_rng();
        let keyset: Vec<u8> = (0..key_size).map(|_| rng.r#gen()).collect();
        Ok(Value::bytes(keyset))
    }

    fn eval_keys_add_key_from_raw_bytes(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "KEYS.ADD_KEY_FROM_RAW_BYTES requires 3 arguments".to_string(),
            ));
        }
        let keyset = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let _key_type = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let raw_key = Self::evaluate_expr(&args[2], batch, row_idx)?;

        let mut existing = keyset.as_bytes().unwrap_or(&[]).to_vec();
        if let Some(raw) = raw_key.as_bytes() {
            existing.extend_from_slice(raw);
        }
        Ok(Value::bytes(existing))
    }

    fn eval_keys_keyset_chain(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "KEYS.KEYSET_CHAIN requires 2 arguments".to_string(),
            ));
        }
        let _kms_key = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let keyset = Self::evaluate_expr(&args[1], batch, row_idx)?;
        Ok(keyset)
    }

    fn eval_keys_keyset_from_json(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "KEYS.KEYSET_FROM_JSON requires 1 argument".to_string(),
            ));
        }
        let json_str = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let s = json_str.as_str().unwrap_or("{}");
        Ok(Value::bytes(s.as_bytes().to_vec()))
    }

    fn eval_keys_keyset_to_json(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "KEYS.KEYSET_TO_JSON requires 1 argument".to_string(),
            ));
        }
        let keyset = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let bytes = keyset.as_bytes().unwrap_or(&[]);
        let json_str = format!(
            "{{\"primaryKeyId\":{},\"key\":[]}}",
            bytes.first().copied().unwrap_or(0) as i32
        );
        Ok(Value::string(json_str))
    }

    fn eval_keys_rotate_keyset(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "KEYS.ROTATE_KEYSET requires 2 arguments".to_string(),
            ));
        }
        let keyset = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let _algorithm = Self::evaluate_expr(&args[1], batch, row_idx)?;

        let mut existing = keyset.as_bytes().unwrap_or(&[]).to_vec();
        let mut rng = rand::thread_rng();
        let new_key: Vec<u8> = (0..32).map(|_| rng.r#gen()).collect();
        existing.extend_from_slice(&new_key);
        Ok(Value::bytes(existing))
    }

    fn eval_keys_keyset_length(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "KEYS.KEYSET_LENGTH requires 1 argument".to_string(),
            ));
        }
        let _keyset = Self::evaluate_expr(&args[0], batch, row_idx)?;
        Ok(Value::int64(1))
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    #[test]
    fn test_new_keyset_returns_bytes() {
        let schema = Schema::from_fields(vec![Field::nullable("algo", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("AEAD_AES_GCM_256".into())]]);
        let args = vec![Expr::column("algo")];
        let result =
            ProjectionWithExprExec::evaluate_keys_function("KEYS.NEW_KEYSET", &args, &batch, 0)
                .unwrap();
        assert!(result.as_bytes().is_some());
        assert_eq!(result.as_bytes().unwrap().len(), 32);
    }

    #[test]
    fn test_keyset_length_returns_one() {
        let schema = Schema::from_fields(vec![Field::nullable("keyset", DataType::Bytes)]);
        let batch = create_batch(schema, vec![vec![Value::bytes(vec![0, 1, 2, 3])]]);
        let args = vec![Expr::column("keyset")];
        let result =
            ProjectionWithExprExec::evaluate_keys_function("KEYS.KEYSET_LENGTH", &args, &batch, 0)
                .unwrap();
        assert_eq!(result.as_i64(), Some(1));
    }
}
