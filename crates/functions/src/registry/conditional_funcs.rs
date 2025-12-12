use std::rc::Rc;

use yachtsql_common::error::Error;
use yachtsql_common::types::{DataType, Value};

use super::{FunctionRegistry, eval_boolean_condition};
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_coalesce_variants(registry);
    register_if_variants(registry);
    register_extrema_ignore_nulls(registry);
}

fn register_coalesce_variants(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "COALESCE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "COALESCE".to_string(),
            arg_types: vec![],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                for arg in args {
                    if !arg.is_null() {
                        return Ok(arg.clone());
                    }
                }
                Ok(Value::null())
            },
        }),
    );

    registry.register_scalar(
        "IFNULL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "IFNULL".to_string(),
            arg_types: vec![],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "IFNULL requires exactly 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    Ok(args[1].clone())
                } else {
                    Ok(args[0].clone())
                }
            },
        }),
    );

    registry.register_scalar(
        "NULLIF".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "NULLIF".to_string(),
            arg_types: vec![],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "NULLIF requires exactly 2 arguments".to_string(),
                    ));
                }
                if args[0] == args[1] {
                    Ok(Value::null())
                } else {
                    Ok(args[0].clone())
                }
            },
        }),
    );
}

fn register_if_variants(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "IF".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "IF".to_string(),
            arg_types: vec![],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() != 3 {
                    return Err(Error::invalid_query(
                        "IF requires exactly 3 arguments: IF(condition, true_value, false_value)"
                            .to_string(),
                    ));
                }
                Ok(if eval_boolean_condition(&args[0])? {
                    args[1].clone()
                } else {
                    args[2].clone()
                })
            },
        }),
    );

    registry.register_scalar(
        "IIF".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "IIF".to_string(),
            arg_types: vec![],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() != 3 {
                    return Err(Error::invalid_query(
                        "IIF requires exactly 3 arguments: IIF(condition, true_value, false_value)"
                            .to_string(),
                    ));
                }
                Ok(if eval_boolean_condition(&args[0])? {
                    args[1].clone()
                } else {
                    args[2].clone()
                })
            },
        }),
    );

    registry.register_scalar(
        "DECODE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "DECODE".to_string(),
            arg_types: vec![],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                if args.len() < 3 {
                    return Err(Error::invalid_query(
                        "DECODE requires at least 3 arguments: DECODE(expr, search1, result1, ..., default)".to_string(),
                    ));
                }

                let expr = &args[0];
                let pair_count = (args.len() - 1) / 2;

                for i in 0..pair_count {
                    let search_idx = 1 + i * 2;
                    let result_idx = search_idx + 1;
                    if result_idx >= args.len() {
                        break;
                    }

                    let search = &args[search_idx];
                    let matches = if expr.is_null() && search.is_null() {
                        true
                    } else {
                        expr == search
                    };

                    if matches {
                        return Ok(args[result_idx].clone());
                    }
                }

                if args.len() % 2 == 0 {
                    Ok(args[args.len() - 1].clone())
                } else {
                    Ok(Value::null())
                }
            },
        }),
    );
}

fn register_extrema_ignore_nulls(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "GREATEST_IGNORE_NULLS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "GREATEST_IGNORE_NULLS".to_string(),
            arg_types: vec![],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "GREATEST_IGNORE_NULLS requires at least one argument".to_string(),
                    ));
                }

                let non_null: Vec<&Value> = args.iter().filter(|value| !value.is_null()).collect();

                if non_null.is_empty() {
                    return Ok(Value::null());
                }

                let mut best = non_null[0].clone();
                for value in &non_null[1..] {
                    let order = compare_values(&best, value)?;
                    if matches!(order, std::cmp::Ordering::Less) {
                        best = (*value).clone();
                    }
                }

                Ok(best.clone())
            },
        }),
    );

    registry.register_scalar(
        "LEAST_IGNORE_NULLS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "LEAST_IGNORE_NULLS".to_string(),
            arg_types: vec![],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "LEAST_IGNORE_NULLS requires at least one argument".to_string(),
                    ));
                }

                let non_null: Vec<&Value> = args.iter().filter(|value| !value.is_null()).collect();

                if non_null.is_empty() {
                    return Ok(Value::null());
                }

                let mut best = non_null[0].clone();
                for value in &non_null[1..] {
                    let order = compare_values(&best, value)?;
                    if matches!(order, std::cmp::Ordering::Greater) {
                        best = (*value).clone();
                    }
                }

                Ok(best.clone())
            },
        }),
    );
}

fn compare_values<'a>(
    lhs: &'a Value,
    rhs: &'a Value,
) -> yachtsql_common::error::Result<std::cmp::Ordering> {
    if let (Some(a), Some(b)) = (lhs.as_i64(), rhs.as_i64()) {
        return Ok(a.cmp(&b));
    }

    if let (Some(a), Some(b)) = (lhs.as_f64(), rhs.as_f64()) {
        return Ok(a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal));
    }

    if let (Some(a), Some(b)) = (lhs.as_i64(), rhs.as_f64()) {
        return Ok((a as f64)
            .partial_cmp(&b)
            .unwrap_or(std::cmp::Ordering::Equal));
    }

    if let (Some(a), Some(b)) = (lhs.as_f64(), rhs.as_i64()) {
        return Ok(a
            .partial_cmp(&(b as f64))
            .unwrap_or(std::cmp::Ordering::Equal));
    }

    if let (Some(a), Some(b)) = (lhs.as_str(), rhs.as_str()) {
        return Ok(a.cmp(b));
    }

    Err(Error::TypeMismatch {
        expected: "Comparable numeric or string types".to_string(),
        actual: format!("{}, {}", lhs.data_type(), rhs.data_type()),
    })
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_values() {
        assert_eq!(
            compare_values(&Value::int64(5), &Value::int64(10)).unwrap(),
            std::cmp::Ordering::Less
        );

        assert_eq!(
            compare_values(&Value::float64(3.14), &Value::float64(2.71)).unwrap(),
            std::cmp::Ordering::Greater
        );

        assert_eq!(
            compare_values(&Value::int64(5), &Value::float64(5.0)).unwrap(),
            std::cmp::Ordering::Equal
        );

        assert_eq!(
            compare_values(
                &Value::string("abc".to_string()),
                &Value::string("xyz".to_string())
            )
            .unwrap(),
            std::cmp::Ordering::Less
        );
    }
}
