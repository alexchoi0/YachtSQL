use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_generate_uuid(
        args: &[Expr],
    ) -> Result<Value> {
        Self::validate_zero_args("GENERATE_UUID", args)?;
        crate::functions::generator::generate_uuid()
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_generate_uuidv7(
        args: &[Expr],
    ) -> Result<Value> {
        Self::validate_zero_args("UUIDV7", args)?;
        use std::time::{SystemTime, UNIX_EPOCH};

        use rand::Rng;

        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let mut rng = rand::thread_rng();
        let random_a: u16 = rng.r#gen();
        let random_b: u64 = rng.r#gen();

        let time_high = ((timestamp_ms >> 28) & 0xFFFF_FFFF) as u32;
        let time_mid = ((timestamp_ms >> 12) & 0xFFFF) as u16;
        let time_low_and_version = ((timestamp_ms & 0xFFF) as u16) | 0x7000;
        let clock_seq_and_variant = (random_a & 0x3FFF) | 0x8000;
        let node = random_b & 0xFFFF_FFFF_FFFF;

        let uuid_str = format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            time_high, time_mid, time_low_and_version, clock_seq_and_variant, node
        );
        Ok(Value::string(uuid_str))
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;
    use yachtsql_core::types::Value;
    use yachtsql_optimizer::expr::Expr;

    use super::*;
    use crate::tests::support::assert_error_contains;

    #[test]
    fn generates_uuid_value() {
        let result = ProjectionWithExprExec::eval_generate_uuid(&[]).expect("success");
        if let Some(s) = result.as_str() {
            Uuid::parse_str(&s).expect("valid UUID string");
        } else {
            panic!("Expected String")
        }
    }

    #[test]
    fn generates_different_uuids_on_each_call() {
        let uuid1 = ProjectionWithExprExec::eval_generate_uuid(&[]).expect("success");
        let uuid2 = ProjectionWithExprExec::eval_generate_uuid(&[]).expect("success");
        assert_ne!(uuid1, uuid2, "UUIDs should be unique");
    }

    #[test]
    fn validates_zero_arguments() {
        use yachtsql_optimizer::expr::LiteralValue;
        let args = vec![Expr::literal(LiteralValue::Int64(1))];
        let err = ProjectionWithExprExec::eval_generate_uuid(&args).expect_err("extra argument");
        assert_error_contains(&err, "GENERATE_UUID");
    }
}
