use std::cmp::Ordering;

use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_core::types::collation::CollationRegistry;

use crate::Table;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullOrdering {
    NullsFirst,
    NullsLast,
    Default,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey {
    pub column: String,
    pub direction: SortDirection,
    pub null_ordering: NullOrdering,
    pub collation: Option<String>,
}

pub struct SortOperator {
    sort_keys: Vec<SortKey>,
}

impl SortOperator {
    pub fn new(sort_keys: Vec<SortKey>) -> Self {
        Self { sort_keys }
    }

    pub fn execute(&self, input: Table) -> Result<Table> {
        if input.num_rows() == 0 {
            return Ok(input);
        }

        let mut indices: Vec<usize> = (0..input.num_rows()).collect();

        indices.sort_by(|&a, &b| {
            for sort_key in &self.sort_keys {
                let col = match input.column_by_name(&sort_key.column) {
                    Some(c) => c,
                    None => continue,
                };

                let cmp = if col.is_enum() {
                    let ordinal_cmp = compare_enum_ordinals(col, a, b);

                    let effective_null_ordering =
                        resolve_null_ordering(sort_key.null_ordering, sort_key.direction);
                    let val_a = col.get(a).unwrap_or(Value::null());
                    let val_b = col.get(b).unwrap_or(Value::null());

                    if val_a.is_null() && val_b.is_null() {
                        Ordering::Equal
                    } else if val_a.is_null() {
                        let null_cmp = null_comes_first(effective_null_ordering);
                        match sort_key.direction {
                            SortDirection::Ascending => null_cmp,
                            SortDirection::Descending => null_cmp.reverse(),
                        }
                    } else if val_b.is_null() {
                        let null_cmp = null_comes_first(effective_null_ordering).reverse();
                        match sort_key.direction {
                            SortDirection::Ascending => null_cmp,
                            SortDirection::Descending => null_cmp.reverse(),
                        }
                    } else {
                        match sort_key.direction {
                            SortDirection::Ascending => ordinal_cmp,
                            SortDirection::Descending => ordinal_cmp.reverse(),
                        }
                    }
                } else {
                    let val_a = col.get(a).unwrap_or(Value::null());
                    let val_b = col.get(b).unwrap_or(Value::null());
                    compare_values_with_null_ordering(
                        &val_a,
                        &val_b,
                        sort_key.direction,
                        sort_key.null_ordering,
                        sort_key.collation.as_deref(),
                    )
                };

                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
        });

        let mut new_columns = Vec::new();
        for col in input.expect_columns() {
            let new_col = col.gather(&indices)?;
            new_columns.push(new_col);
        }

        Table::new(input.schema().clone(), new_columns)
    }

    pub fn sort_keys(&self) -> &[SortKey] {
        &self.sort_keys
    }
}

fn compare_values_with_null_ordering(
    a: &Value,
    b: &Value,
    direction: SortDirection,
    null_ordering: NullOrdering,
    collation: Option<&str>,
) -> Ordering {
    let effective_null_ordering = resolve_null_ordering(null_ordering, direction);

    let cmp = if a.is_null() && b.is_null() {
        Ordering::Equal
    } else if a.is_null() {
        null_comes_first(effective_null_ordering)
    } else if b.is_null() {
        null_comes_first(effective_null_ordering).reverse()
    } else {
        compare_non_null_values(a, b, collation)
    };

    match direction {
        SortDirection::Ascending => cmp,
        SortDirection::Descending => cmp.reverse(),
    }
}

#[inline]
fn resolve_null_ordering(null_ordering: NullOrdering, direction: SortDirection) -> NullOrdering {
    match null_ordering {
        NullOrdering::Default => match direction {
            SortDirection::Ascending => NullOrdering::NullsLast,
            SortDirection::Descending => NullOrdering::NullsFirst,
        },
        other => other,
    }
}

#[inline]
fn null_comes_first(null_ordering: NullOrdering) -> Ordering {
    match null_ordering {
        NullOrdering::NullsFirst => Ordering::Less,
        NullOrdering::NullsLast => Ordering::Greater,
        NullOrdering::Default => unreachable!("Default should be resolved"),
    }
}

pub fn compare_enum_ordinals(
    col: &yachtsql_storage::Column,
    idx_a: usize,
    idx_b: usize,
) -> Ordering {
    match (col.enum_ordinal(idx_a), col.enum_ordinal(idx_b)) {
        (Some(a), Some(b)) => a.cmp(&b),
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
    }
}

fn compare_non_null_values(a: &Value, b: &Value, collation: Option<&str>) -> Ordering {
    if let (Some(a_i64), Some(b_i64)) = (a.as_i64(), b.as_i64()) {
        return a_i64.cmp(&b_i64);
    }

    if let (Some(a_f64), Some(b_f64)) = (a.as_f64(), b.as_f64()) {
        return compare_floats(a_f64, b_f64);
    }

    if let (Some(a_str), Some(b_str)) = (a.as_str(), b.as_str()) {
        return compare_strings(a_str, b_str, collation);
    }

    if let (Some(a_bool), Some(b_bool)) = (a.as_bool(), b.as_bool()) {
        return a_bool.cmp(&b_bool);
    }

    if let (Some(a_date), Some(b_date)) = (a.as_date(), b.as_date()) {
        return a_date.cmp(&b_date);
    }

    if let (Some(a_ts), Some(b_ts)) = (a.as_timestamp(), b.as_timestamp()) {
        return a_ts.cmp(&b_ts);
    }

    if let (Some(a_num), Some(b_num)) = (a.as_numeric(), b.as_numeric()) {
        return a_num.cmp(&b_num);
    }

    if let Some(a_i64) = a.as_i64() {
        if let Some(b_f64) = b.as_f64() {
            return compare_floats(a_i64 as f64, b_f64);
        }
    }
    if let Some(a_f64) = a.as_f64() {
        if let Some(b_i64) = b.as_i64() {
            return compare_floats(a_f64, b_i64 as f64);
        }
    }

    compare_as_debug_strings(a, b)
}

#[inline]
fn compare_floats(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
    }
}

#[inline]
fn compare_strings(a: &str, b: &str, collation: Option<&str>) -> Ordering {
    match collation {
        Some(collation_name) => CollationRegistry::global()
            .get(collation_name)
            .map(|c| c.compare(a, b))
            .unwrap_or_else(|| a.cmp(b)),
        None => a.cmp(b),
    }
}

#[inline]
fn compare_as_debug_strings(a: &Value, b: &Value) -> Ordering {
    format!("{:?}", a).cmp(&format!("{:?}", b))
}
