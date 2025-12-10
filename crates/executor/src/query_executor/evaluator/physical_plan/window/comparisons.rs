use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::WindowExec;
use crate::Table;

impl WindowExec {
    pub(super) fn evaluate_expr(expr: &Expr, batch: &Table, row_idx: usize) -> Result<Value> {
        match expr {
            Expr::Column { name, .. } => {
                let schema = batch.schema();
                let col_idx = schema
                    .field_index(name)
                    .ok_or_else(|| crate::error::Error::column_not_found(name.clone()))?;
                let column = batch
                    .column(col_idx)
                    .ok_or_else(|| crate::error::Error::column_not_found(name.clone()))?;
                column.get(row_idx)
            }
            _ => Err(crate::error::Error::unsupported_feature(
                "Complex expressions in window functions not yet supported".to_string(),
            )),
        }
    }

    pub(super) fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (a.is_null(), b.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => Self::compare_non_null_values(a, b),
        }
    }

    pub(super) fn compare_non_null_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        if let (Some(a_val), Some(b_val)) = (a.as_i64(), b.as_i64()) {
            return a_val.cmp(&b_val);
        }

        if let (Some(a_val), Some(b_val)) = (a.as_f64(), b.as_f64()) {
            return a_val.partial_cmp(&b_val).unwrap_or(Ordering::Equal);
        }

        if let (Some(a_val), Some(b_val)) = (a.as_str(), b.as_str()) {
            return a_val.cmp(b_val);
        }

        if let (Some(fs_a), Some(fs_b)) = (a.as_fixed_string(), b.as_fixed_string()) {
            return fs_a.data.cmp(&fs_b.data);
        }

        if let (Some(fs), Some(s)) = (a.as_fixed_string(), b.as_str()) {
            return fs.to_string_lossy().cmp(&s.to_string());
        }

        if let (Some(s), Some(fs)) = (a.as_str(), b.as_fixed_string()) {
            return s.to_string().cmp(&fs.to_string_lossy());
        }

        if let (Some(a_val), Some(b_val)) = (a.as_bool(), b.as_bool()) {
            return a_val.cmp(&b_val);
        }

        if let (Some(a_val), Some(b_val)) = (a.as_date(), b.as_date()) {
            return a_val.cmp(&b_val);
        }

        if let (Some(a_val), Some(b_val)) = (a.as_timestamp(), b.as_timestamp()) {
            return a_val.cmp(&b_val);
        }

        if let (Some(a_val), Some(b_val)) = (a.as_datetime(), b.as_datetime()) {
            return a_val.cmp(&b_val);
        }

        if let (Some(a_val), Some(b_val)) = (a.as_time(), b.as_time()) {
            return a_val.cmp(&b_val);
        }

        if let (Some(a_val), Some(b_val)) = (a.as_numeric(), b.as_numeric()) {
            return a_val.cmp(&b_val);
        }

        Ordering::Equal
    }

    pub(super) fn apply_sort_direction(
        ordering: std::cmp::Ordering,
        asc: bool,
    ) -> std::cmp::Ordering {
        if asc { ordering } else { ordering.reverse() }
    }

    pub(super) fn compare_values_with_nulls(
        a: &Value,
        b: &Value,
        asc: bool,
        nulls_first: Option<bool>,
    ) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        let nulls_first = nulls_first.unwrap_or(!asc);

        match (a.is_null(), b.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let cmp = Self::compare_non_null_values(a, b);
                Self::apply_sort_direction(cmp, asc)
            }
        }
    }

    #[allow(dead_code)]
    pub(super) fn values_equal_for_array(a: &Value, b: &Value) -> bool {
        if a.is_null() && b.is_null() {
            return true;
        }

        if a.is_null() || b.is_null() {
            return false;
        }

        if let (Some(x), Some(y)) = (a.as_bool(), b.as_bool()) {
            return x == y;
        }

        if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
            return x == y;
        }

        if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
            return (x - y).abs() < f64::EPSILON;
        }

        if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
            return x == y;
        }

        if let (Some(x), Some(y)) = (a.as_array(), b.as_array()) {
            return x.len() == y.len()
                && x.iter()
                    .zip(y.iter())
                    .all(|(a, b)| Self::values_equal_for_array(a, b));
        }

        false
    }

    pub(super) fn values_equal(a: &[Value], b: &[Value]) -> bool {
        if a.len() != b.len() {
            return false;
        }

        a.iter().zip(b.iter()).all(|(val_a, val_b)| {
            use std::cmp::Ordering;
            Self::compare_values(val_a, val_b) == Ordering::Equal
        })
    }
}
