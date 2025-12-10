use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_net_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "NET.IP_FROM_STRING" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "NET.IP_FROM_STRING requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                let s = val
                    .as_str()
                    .ok_or_else(|| Error::invalid_query("Expected string argument".to_string()))?;
                yachtsql_functions::network::ip_from_string(s)
            }
            "NET.SAFE_IP_FROM_STRING" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "NET.SAFE_IP_FROM_STRING requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                let s = val
                    .as_str()
                    .ok_or_else(|| Error::invalid_query("Expected string argument".to_string()))?;
                yachtsql_functions::network::safe_ip_from_string(s)
            }
            "NET.IP_TO_STRING" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "NET.IP_TO_STRING requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                let bytes = val
                    .as_bytes()
                    .ok_or_else(|| Error::invalid_query("Expected bytes argument".to_string()))?;
                yachtsql_functions::network::ip_to_string(bytes)
            }
            "NET.IPV4_FROM_INT64" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "NET.IPV4_FROM_INT64 requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                let i = val
                    .as_i64()
                    .ok_or_else(|| Error::invalid_query("Expected INT64 argument".to_string()))?;
                yachtsql_functions::network::ipv4_from_int64(i)
            }
            "NET.IPV4_TO_INT64" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "NET.IPV4_TO_INT64 requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                let bytes = val
                    .as_bytes()
                    .ok_or_else(|| Error::invalid_query("Expected bytes argument".to_string()))?;
                yachtsql_functions::network::ipv4_to_int64(bytes)
            }
            "NET.IP_NET_MASK" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "NET.IP_NET_MASK requires 2 arguments".to_string(),
                    ));
                }
                let family = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let prefix = Self::evaluate_expr(&args[1], batch, row_idx)?;
                if family.is_null() || prefix.is_null() {
                    return Ok(Value::null());
                }
                let family_val = family
                    .as_i64()
                    .ok_or_else(|| Error::invalid_query("Expected INT64 for family".to_string()))?;
                let prefix_val = prefix
                    .as_i64()
                    .ok_or_else(|| Error::invalid_query("Expected INT64 for prefix".to_string()))?;
                yachtsql_functions::network::ip_net_mask(prefix_val, family_val)
            }
            "NET.IP_TRUNC" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "NET.IP_TRUNC requires 2 arguments".to_string(),
                    ));
                }
                let addr = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let prefix = Self::evaluate_expr(&args[1], batch, row_idx)?;
                if addr.is_null() || prefix.is_null() {
                    return Ok(Value::null());
                }
                let addr_bytes = addr.as_bytes().ok_or_else(|| {
                    Error::invalid_query("Expected bytes for address".to_string())
                })?;
                let prefix_val = prefix
                    .as_i64()
                    .ok_or_else(|| Error::invalid_query("Expected INT64 for prefix".to_string()))?;
                yachtsql_functions::network::ip_trunc(addr_bytes, prefix_val)
            }
            "NET.HOST" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "NET.HOST requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                let s = val
                    .as_str()
                    .ok_or_else(|| Error::invalid_query("Expected string argument".to_string()))?;
                yachtsql_functions::network::host(s)
            }
            "NET.PUBLIC_SUFFIX" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "NET.PUBLIC_SUFFIX requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                let s = val
                    .as_str()
                    .ok_or_else(|| Error::invalid_query("Expected string argument".to_string()))?;
                yachtsql_functions::network::public_suffix(s)
            }
            "NET.REG_DOMAIN" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "NET.REG_DOMAIN requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                let s = val
                    .as_str()
                    .ok_or_else(|| Error::invalid_query("Expected string argument".to_string()))?;
                yachtsql_functions::network::reg_domain(s)
            }
            "NET.IP_IN_NET" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "NET.IP_IN_NET requires 2 arguments".to_string(),
                    ));
                }
                let addr = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let cidr = Self::evaluate_expr(&args[1], batch, row_idx)?;
                if addr.is_null() || cidr.is_null() {
                    return Ok(Value::null());
                }
                let addr_bytes = addr.as_bytes().ok_or_else(|| {
                    Error::invalid_query("Expected bytes for address".to_string())
                })?;
                let cidr_str = cidr
                    .as_str()
                    .ok_or_else(|| Error::invalid_query("Expected string for CIDR".to_string()))?;
                yachtsql_functions::network::ip_in_net(addr_bytes, cidr_str)
            }
            "NET.MAKE_NET" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "NET.MAKE_NET requires 2 arguments".to_string(),
                    ));
                }
                let addr = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let prefix = Self::evaluate_expr(&args[1], batch, row_idx)?;
                if addr.is_null() || prefix.is_null() {
                    return Ok(Value::null());
                }
                let addr_bytes = addr.as_bytes().ok_or_else(|| {
                    Error::invalid_query("Expected bytes for address".to_string())
                })?;
                let prefix_val = prefix
                    .as_i64()
                    .ok_or_else(|| Error::invalid_query("Expected INT64 for prefix".to_string()))?;
                yachtsql_functions::network::make_net(addr_bytes, prefix_val)
            }
            "NET.IP_IS_PRIVATE" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "NET.IP_IS_PRIVATE requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                let addr_bytes = val.as_bytes().ok_or_else(|| {
                    Error::invalid_query("Expected bytes for address".to_string())
                })?;
                yachtsql_functions::network::ip_is_private(addr_bytes)
            }
            _ => Err(Error::unsupported_feature(format!(
                "Unknown NET function: {}",
                name
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    #[test]
    fn test_ip_from_string() {
        let schema = Schema::from_fields(vec![Field::nullable("ip_str", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("192.168.1.1".into())]]);
        let args = vec![Expr::column("ip_str")];
        let result =
            ProjectionWithExprExec::eval_net_function("NET.IP_FROM_STRING", &args, &batch, 0)
                .unwrap();
        assert_eq!(result.as_bytes().unwrap(), &[192, 168, 1, 1]);
    }

    #[test]
    fn test_ipv4_from_int64() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(167772161)]]);
        let args = vec![Expr::column("val")];
        let result =
            ProjectionWithExprExec::eval_net_function("NET.IPV4_FROM_INT64", &args, &batch, 0)
                .unwrap();
        assert_eq!(result.as_bytes().unwrap(), &[10, 0, 0, 1]);
    }
}
