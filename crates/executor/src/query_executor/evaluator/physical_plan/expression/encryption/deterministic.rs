use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_deterministic_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "DETERMINISTIC_ENCRYPT" => Self::eval_deterministic_encrypt(args, batch, row_idx),
            "DETERMINISTIC_DECRYPT_STRING" => {
                Self::eval_deterministic_decrypt_string(args, batch, row_idx)
            }
            "DETERMINISTIC_DECRYPT_BYTES" => {
                Self::eval_deterministic_decrypt_bytes(args, batch, row_idx)
            }
            _ => Err(Error::unsupported_feature(format!(
                "Unknown DETERMINISTIC function: {}",
                name
            ))),
        }
    }

    fn eval_deterministic_encrypt(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "DETERMINISTIC_ENCRYPT requires 3 arguments (keyset, plaintext, additional_data)"
                    .to_string(),
            ));
        }
        let _keyset = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let plaintext = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let _additional_data = Self::evaluate_expr(&args[2], batch, row_idx)?;

        let plaintext_bytes = if let Some(bytes) = plaintext.as_bytes() {
            bytes.to_vec()
        } else if let Some(s) = plaintext.as_str() {
            s.as_bytes().to_vec()
        } else {
            return Err(Error::invalid_query(
                "DETERMINISTIC_ENCRYPT plaintext must be BYTES or STRING".to_string(),
            ));
        };

        let mut encrypted = vec![0xDE, 0x7E, 0x01, 0x00];
        encrypted.extend_from_slice(&plaintext_bytes);
        Ok(Value::bytes(encrypted))
    }

    fn eval_deterministic_decrypt_string(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "DETERMINISTIC_DECRYPT_STRING requires 3 arguments".to_string(),
            ));
        }
        let _keyset = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let ciphertext = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let _additional_data = Self::evaluate_expr(&args[2], batch, row_idx)?;

        let ciphertext_bytes = ciphertext.as_bytes().ok_or_else(|| {
            Error::invalid_query(
                "DETERMINISTIC_DECRYPT_STRING ciphertext must be BYTES".to_string(),
            )
        })?;

        if ciphertext_bytes.len() >= 4 {
            let plaintext = &ciphertext_bytes[4..];
            match std::str::from_utf8(plaintext) {
                Ok(s) => Ok(Value::string(s.to_string())),
                Err(_) => Ok(Value::null()),
            }
        } else {
            Ok(Value::null())
        }
    }

    fn eval_deterministic_decrypt_bytes(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "DETERMINISTIC_DECRYPT_BYTES requires 3 arguments".to_string(),
            ));
        }
        let _keyset = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let ciphertext = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let _additional_data = Self::evaluate_expr(&args[2], batch, row_idx)?;

        let ciphertext_bytes = ciphertext.as_bytes().ok_or_else(|| {
            Error::invalid_query("DETERMINISTIC_DECRYPT_BYTES ciphertext must be BYTES".to_string())
        })?;

        if ciphertext_bytes.len() >= 4 {
            let plaintext = &ciphertext_bytes[4..];
            Ok(Value::bytes(plaintext.to_vec()))
        } else {
            Ok(Value::null())
        }
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
    fn test_deterministic_encrypt_returns_bytes() {
        let schema = Schema::from_fields(vec![
            Field::nullable("keyset", DataType::Bytes),
            Field::nullable("plaintext", DataType::Bytes),
            Field::nullable("aad", DataType::Bytes),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::bytes(vec![0, 1, 2, 3]),
                Value::bytes(b"secret".to_vec()),
                Value::bytes(b"aad".to_vec()),
            ]],
        );
        let args = vec![
            Expr::column("keyset"),
            Expr::column("plaintext"),
            Expr::column("aad"),
        ];
        let result = ProjectionWithExprExec::evaluate_deterministic_function(
            "DETERMINISTIC_ENCRYPT",
            &args,
            &batch,
            0,
        )
        .unwrap();
        assert!(result.as_bytes().is_some());
    }
}
