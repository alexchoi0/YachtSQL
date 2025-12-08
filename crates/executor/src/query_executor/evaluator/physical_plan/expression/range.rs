use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_range_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "LOWER" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("LOWER requires 1 argument"));
                }
                let range = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if range.as_range().is_some() {
                    yachtsql_functions::range::range_lower(&range)
                } else if let Some(s) = range.as_str() {
                    Ok(Value::string(s.to_lowercase()))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "RANGE or STRING".to_string(),
                        actual: format!("{:?}", range.data_type()),
                    })
                }
            }
            "UPPER" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("UPPER requires 1 argument"));
                }
                let range = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if range.as_range().is_some() {
                    yachtsql_functions::range::range_upper(&range)
                } else if let Some(s) = range.as_str() {
                    Ok(Value::string(s.to_uppercase()))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "RANGE or STRING".to_string(),
                        actual: format!("{:?}", range.data_type()),
                    })
                }
            }
            "LOWER_INC" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("LOWER_INC requires 1 argument"));
                }
                let range = Self::evaluate_expr(&args[0], batch, row_idx)?;
                yachtsql_functions::range::range_lower_inc(&range)
            }
            "UPPER_INC" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("UPPER_INC requires 1 argument"));
                }
                let range = Self::evaluate_expr(&args[0], batch, row_idx)?;
                yachtsql_functions::range::range_upper_inc(&range)
            }
            "LOWER_INF" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("LOWER_INF requires 1 argument"));
                }
                let range = Self::evaluate_expr(&args[0], batch, row_idx)?;
                yachtsql_functions::range::range_lower_inf(&range)
            }
            "UPPER_INF" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("UPPER_INF requires 1 argument"));
                }
                let range = Self::evaluate_expr(&args[0], batch, row_idx)?;
                yachtsql_functions::range::range_upper_inf(&range)
            }
            "ISEMPTY" | "RANGE_ISEMPTY" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("ISEMPTY requires 1 argument"));
                }
                let range = Self::evaluate_expr(&args[0], batch, row_idx)?;
                yachtsql_functions::range::range_is_empty(&range)
            }
            "RANGE_MERGE" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("RANGE_MERGE requires 2 arguments"));
                }
                let range1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let range2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::range::range_merge(&range1, &range2)
            }
            "RANGE_CONTAINS" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("RANGE_CONTAINS requires 2 arguments"));
                }
                let range1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let range2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::range::range_contains_range(&range1, &range2)
            }
            "RANGE_CONTAINS_ELEM" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "RANGE_CONTAINS_ELEM requires 2 arguments",
                    ));
                }
                let range = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let elem = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::range::range_contains(&range, &elem)
            }
            "RANGE_OVERLAPS" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("RANGE_OVERLAPS requires 2 arguments"));
                }
                let range1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let range2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::range::range_overlaps(&range1, &range2)
            }
            "RANGE_UNION" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("RANGE_UNION requires 2 arguments"));
                }
                let range1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let range2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::range::range_union(&range1, &range2)
            }
            "RANGE_INTERSECTION" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "RANGE_INTERSECTION requires 2 arguments",
                    ));
                }
                let range1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let range2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::range::range_intersection(&range1, &range2)
            }
            "RANGE_ADJACENT" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("RANGE_ADJACENT requires 2 arguments"));
                }
                let range1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let range2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::range::range_adjacent(&range1, &range2)
            }
            "RANGE_STRICTLY_LEFT" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "RANGE_STRICTLY_LEFT requires 2 arguments",
                    ));
                }
                let range1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let range2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::range::range_strictly_left(&range1, &range2)
            }
            "RANGE_STRICTLY_RIGHT" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "RANGE_STRICTLY_RIGHT requires 2 arguments",
                    ));
                }
                let range1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let range2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::range::range_strictly_right(&range1, &range2)
            }
            "RANGE_DIFFERENCE" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "RANGE_DIFFERENCE requires 2 arguments",
                    ));
                }
                let range1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let range2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::range::range_difference(&range1, &range2)
            }
            _ => Err(Error::unsupported_feature(format!(
                "Unknown range function: {}",
                name
            ))),
        }
    }
}
