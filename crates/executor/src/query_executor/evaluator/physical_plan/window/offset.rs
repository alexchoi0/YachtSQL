use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::{Expr, UnaryOp};

use super::WindowExec;
use crate::Table;

#[derive(Clone, Copy)]
pub(super) enum OffsetDirection {
    Backward,
    Forward,
}

impl WindowExec {
    pub(super) fn compute_offset_function(
        indices: &[usize],
        args: &[Expr],
        batch: &Table,
        results: &mut [Value],
        direction: OffsetDirection,
        null_treatment: Option<yachtsql_optimizer::expr::NullTreatment>,
    ) {
        use yachtsql_optimizer::expr::NullTreatment;

        let offset = Self::extract_offset_arg(args);
        let default_value = Self::extract_default_value_arg(args);
        let value_expr = args.first();

        let ignore_nulls = matches!(null_treatment, Some(NullTreatment::IgnoreNulls));

        for (position, &original_idx) in indices.iter().enumerate() {
            let offset_idx = if ignore_nulls {
                Self::find_offset_idx_ignore_nulls(
                    indices, position, offset, direction, value_expr, batch,
                )
            } else {
                match direction {
                    OffsetDirection::Backward => {
                        if position < offset {
                            None
                        } else {
                            Some(indices[position - offset])
                        }
                    }
                    OffsetDirection::Forward => {
                        if position + offset >= indices.len() {
                            None
                        } else {
                            Some(indices[position + offset])
                        }
                    }
                }
            };

            results[original_idx] = match (offset_idx, value_expr) {
                (Some(idx), Some(expr)) => {
                    Self::evaluate_expr(expr, batch, idx).unwrap_or(Value::null())
                }
                _ => default_value.clone(),
            };
        }
    }

    fn find_offset_idx_ignore_nulls(
        indices: &[usize],
        position: usize,
        offset: usize,
        direction: OffsetDirection,
        value_expr: Option<&Expr>,
        batch: &Table,
    ) -> Option<usize> {
        let Some(expr) = value_expr else {
            return None;
        };

        let mut non_null_count = 0;
        let range: Box<dyn Iterator<Item = usize>> = match direction {
            OffsetDirection::Backward => Box::new((0..position).rev()),
            OffsetDirection::Forward => Box::new((position + 1)..indices.len()),
        };

        for pos in range {
            let idx = indices[pos];
            let val = Self::evaluate_expr(expr, batch, idx).unwrap_or(Value::null());

            if !val.is_null() {
                non_null_count += 1;
                if non_null_count == offset {
                    return Some(idx);
                }
            }
        }

        None
    }

    pub(super) fn extract_offset_arg(args: &[Expr]) -> usize {
        if args.len() >= 2 {
            match &args[1] {
                Expr::Literal(crate::optimizer::expr::LiteralValue::Int64(n)) => *n as usize,
                _ => 1,
            }
        } else {
            1
        }
    }

    pub(super) fn extract_default_value_arg(args: &[Expr]) -> Value {
        let Some(default_expr) = args.get(2) else {
            return Value::null();
        };

        match default_expr {
            Expr::Literal(lit) => lit.to_value(),
            Expr::UnaryOp {
                op: UnaryOp::Negate,
                expr,
            } => Self::negate_literal_value(expr.as_ref()),
            _ => Value::null(),
        }
    }

    pub(super) fn negate_literal_value(expr: &Expr) -> Value {
        if let Expr::Literal(lit) = expr {
            let val = lit.to_value();
            if let Some(i) = val.as_i64() {
                Value::int64(-i)
            } else if let Some(f) = val.as_f64() {
                Value::float64(-f)
            } else if let Some(n) = val.as_numeric() {
                Value::numeric(-n)
            } else {
                Value::null()
            }
        } else {
            Value::null()
        }
    }
}
