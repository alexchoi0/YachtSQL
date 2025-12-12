use rand::Rng;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
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
        match name {
            "KEYS.NEW_KEYSET" => Self::eval_keys_new_keyset(args, batch, row_idx),
            "KEYS.ADD_KEY_FROM_RAW_BYTES" => {
                Self::eval_keys_add_key_from_raw_bytes(args, batch, row_idx)
            }
            "KEYS.KEYSET_CHAIN" => Self::eval_keys_keyset_chain(args, batch, row_idx),
            "KEYS.KEYSET_FROM_JSON" => Self::eval_keys_keyset_from_json(args, batch, row_idx),
            "KEYS.KEYSET_TO_JSON" => Self::eval_keys_keyset_to_json(args, batch, row_idx),
            "KEYS.ROTATE_KEYSET" => Self::eval_keys_rotate_keyset(args, batch, row_idx),
            "KEYS.KEYSET_LENGTH" => Self::eval_keys_keyset_length(args, batch, row_idx),
            _ => Err(Error::unsupported_feature(format!(
                "Unknown KEYS function: {}",
                name
            ))),
        }
    }

    fn eval_keys_new_keyset(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "KEYS.NEW_KEYSET requires exactly 1 argument (algorithm)".to_string(),
            ));
        }

        let algorithm = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let algo_str = algorithm.as_str().ok_or_else(|| {
            Error::invalid_query("KEYS.NEW_KEYSET: algorithm must be a string".to_string())
        })?;

        let key_len = match algo_str {
            "AEAD_AES_GCM_256" => 32,
            "DETERMINISTIC_AEAD_AES_SIV_CMAC_256" => 64,
            _ => {
                return Err(Error::invalid_query(format!(
                    "KEYS.NEW_KEYSET: unsupported algorithm '{}'",
                    algo_str
                )));
            }
        };

        let mut rng = rand::thread_rng();
        let keyset: Vec<u8> = (0..key_len).map(|_| rng.r#gen()).collect();
        Ok(Value::bytes(keyset))
    }

    fn eval_keys_add_key_from_raw_bytes(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 3 {
            return Err(Error::invalid_query(
                "KEYS.ADD_KEY_FROM_RAW_BYTES requires exactly 3 arguments".to_string(),
            ));
        }

        let keyset = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let _key_type = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let raw_bytes = Self::evaluate_expr(&args[2], batch, row_idx)?;

        let mut keyset_bytes = keyset.as_bytes().unwrap_or(&[]).to_vec();
        let raw = raw_bytes.as_bytes().unwrap_or(&[]);
        keyset_bytes.extend_from_slice(raw);

        Ok(Value::bytes(keyset_bytes))
    }

    fn eval_keys_keyset_chain(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "KEYS.KEYSET_CHAIN requires exactly 2 arguments".to_string(),
            ));
        }

        let _kms_key = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let keyset = Self::evaluate_expr(&args[1], batch, row_idx)?;

        Ok(keyset)
    }

    fn eval_keys_keyset_from_json(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "KEYS.KEYSET_FROM_JSON requires exactly 1 argument".to_string(),
            ));
        }

        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let json_str = json_val.as_str().unwrap_or("{}");

        Ok(Value::bytes(json_str.as_bytes().to_vec()))
    }

    fn eval_keys_keyset_to_json(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "KEYS.KEYSET_TO_JSON requires exactly 1 argument".to_string(),
            ));
        }

        let _keyset = Self::evaluate_expr(&args[0], batch, row_idx)?;
        Ok(Value::string(
            "{\"primaryKeyId\": 1, \"key\": []}".to_string(),
        ))
    }

    fn eval_keys_rotate_keyset(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "KEYS.ROTATE_KEYSET requires exactly 2 arguments".to_string(),
            ));
        }

        let keyset = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let _algorithm = Self::evaluate_expr(&args[1], batch, row_idx)?;

        let mut keyset_bytes = keyset.as_bytes().unwrap_or(&[]).to_vec();
        let mut rng = rand::thread_rng();
        let new_key: Vec<u8> = (0..32).map(|_| rng.r#gen()).collect();
        keyset_bytes.extend_from_slice(&new_key);

        Ok(Value::bytes(keyset_bytes))
    }

    fn eval_keys_keyset_length(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "KEYS.KEYSET_LENGTH requires exactly 1 argument".to_string(),
            ));
        }

        let keyset = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let len = keyset.as_bytes().map(|b| b.len()).unwrap_or(0);

        Ok(Value::int64(len as i64))
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::LiteralValue;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    #[test]
    fn test_keys_new_keyset_returns_bytes() {
        let schema = Schema::from_fields(vec![]);
        let batch = create_batch(schema, vec![vec![]]);
        let args = vec![Expr::Literal(LiteralValue::String(
            "AEAD_AES_GCM_256".to_string(),
        ))];
        let result =
            ProjectionWithExprExec::evaluate_keys_function("KEYS.NEW_KEYSET", &args, &batch, 0)
                .expect("should succeed");
        assert!(result.as_bytes().is_some());
        assert_eq!(result.as_bytes().unwrap().len(), 32);
    }

    #[test]
    fn test_keys_keyset_length() {
        let schema = Schema::from_fields(vec![Field::nullable("keyset", DataType::Bytes)]);
        let batch = create_batch(schema, vec![vec![Value::bytes(vec![0, 1, 2, 3])]]);
        let args = vec![Expr::column("keyset")];
        let result =
            ProjectionWithExprExec::evaluate_keys_function("KEYS.KEYSET_LENGTH", &args, &batch, 0)
                .expect("should succeed");
        assert_eq!(result.as_i64(), Some(4));
    }
}
