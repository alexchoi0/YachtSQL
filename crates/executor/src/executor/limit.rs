use std::cmp::Ordering;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, SortExpr};
use yachtsql_storage::{Column, NullBitmap, Table};

use super::PlanExecutor;
use crate::plan::PhysicalPlan;

enum SortColumnRef<'a> {
    Bool(&'a [bool], &'a NullBitmap),
    Int64(&'a [i64], &'a NullBitmap),
    Float64(&'a [f64], &'a NullBitmap),
    Numeric(&'a [Decimal], &'a NullBitmap),
    String(&'a [String], &'a NullBitmap),
    Bytes(&'a [Vec<u8>], &'a NullBitmap),
    Date(&'a [NaiveDate], &'a NullBitmap),
    Time(&'a [NaiveTime], &'a NullBitmap),
    DateTime(&'a [NaiveDateTime], &'a NullBitmap),
    Timestamp(&'a [DateTime<Utc>], &'a NullBitmap),
}

impl<'a> SortColumnRef<'a> {
    fn from_column(column: &'a Column) -> Option<Self> {
        match column {
            Column::Bool { data, nulls } => Some(SortColumnRef::Bool(data, nulls)),
            Column::Int64 { data, nulls } => Some(SortColumnRef::Int64(data.as_slice(), nulls)),
            Column::Float64 { data, nulls } => Some(SortColumnRef::Float64(data.as_slice(), nulls)),
            Column::Numeric { data, nulls } => Some(SortColumnRef::Numeric(data, nulls)),
            Column::String { data, nulls } => Some(SortColumnRef::String(data, nulls)),
            Column::Bytes { data, nulls } => Some(SortColumnRef::Bytes(data, nulls)),
            Column::Date { data, nulls } => Some(SortColumnRef::Date(data, nulls)),
            Column::Time { data, nulls } => Some(SortColumnRef::Time(data, nulls)),
            Column::DateTime { data, nulls } => Some(SortColumnRef::DateTime(data, nulls)),
            Column::Timestamp { data, nulls } => Some(SortColumnRef::Timestamp(data, nulls)),
            Column::Geography { data, nulls } => Some(SortColumnRef::String(data, nulls)),
            Column::Json { .. }
            | Column::Array { .. }
            | Column::Struct { .. }
            | Column::Interval { .. }
            | Column::Range { .. } => None,
        }
    }

    #[inline]
    fn is_null(&self, idx: usize) -> bool {
        match self {
            SortColumnRef::Bool(_, nulls)
            | SortColumnRef::Int64(_, nulls)
            | SortColumnRef::Float64(_, nulls)
            | SortColumnRef::Numeric(_, nulls)
            | SortColumnRef::String(_, nulls)
            | SortColumnRef::Bytes(_, nulls)
            | SortColumnRef::Date(_, nulls)
            | SortColumnRef::Time(_, nulls)
            | SortColumnRef::DateTime(_, nulls)
            | SortColumnRef::Timestamp(_, nulls) => nulls.is_null(idx),
        }
    }

    #[inline]
    fn compare(&self, a: usize, b: usize) -> Ordering {
        match self {
            SortColumnRef::Bool(data, _) => data[a].cmp(&data[b]),
            SortColumnRef::Int64(data, _) => data[a].cmp(&data[b]),
            SortColumnRef::Float64(data, _) => {
                data[a].partial_cmp(&data[b]).unwrap_or(Ordering::Equal)
            }
            SortColumnRef::Numeric(data, _) => data[a].cmp(&data[b]),
            SortColumnRef::String(data, _) => data[a].cmp(&data[b]),
            SortColumnRef::Bytes(data, _) => data[a].cmp(&data[b]),
            SortColumnRef::Date(data, _) => data[a].cmp(&data[b]),
            SortColumnRef::Time(data, _) => data[a].cmp(&data[b]),
            SortColumnRef::DateTime(data, _) => data[a].cmp(&data[b]),
            SortColumnRef::Timestamp(data, _) => data[a].cmp(&data[b]),
        }
    }
}

fn try_resolve_column_index(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Column { index, .. } => *index,
        _ => None,
    }
}

struct ColumnarSortKeys<'a> {
    columns: Vec<SortColumnRef<'a>>,
    asc: Vec<bool>,
    nulls_first: Vec<bool>,
}

impl<'a> ColumnarSortKeys<'a> {
    fn new(table: &'a Table, sort_exprs: &[SortExpr]) -> Option<Self> {
        let mut columns = Vec::with_capacity(sort_exprs.len());

        for sort_expr in sort_exprs {
            let col_idx = try_resolve_column_index(&sort_expr.expr)?;
            let column = table.column(col_idx)?;
            let sort_col_ref = SortColumnRef::from_column(column)?;
            columns.push(sort_col_ref);
        }

        Some(ColumnarSortKeys {
            columns,
            asc: sort_exprs.iter().map(|e| e.asc).collect(),
            nulls_first: sort_exprs.iter().map(|e| e.nulls_first).collect(),
        })
    }

    #[inline]
    fn compare(&self, a: usize, b: usize) -> Ordering {
        for (i, col) in self.columns.iter().enumerate() {
            let a_null = col.is_null(a);
            let b_null = col.is_null(b);

            match (a_null, b_null) {
                (true, true) => continue,
                (true, false) => {
                    return if self.nulls_first[i] {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    };
                }
                (false, true) => {
                    return if self.nulls_first[i] {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    };
                }
                (false, false) => {}
            }

            let ordering = col.compare(a, b);
            let ordering = if self.asc[i] {
                ordering
            } else {
                ordering.reverse()
            };

            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    }
}

impl<'a> PlanExecutor<'a> {
    pub fn execute_limit(
        &mut self,
        input: &PhysicalPlan,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();
        let n = input_table.row_count();

        let offset = offset.unwrap_or(0);
        let limit = limit.unwrap_or(usize::MAX);

        let start = offset.min(n);
        let end = (offset + limit).min(n);

        if start >= end {
            return Ok(Table::empty(schema));
        }

        let mut result = Table::empty(schema);
        for i in start..end {
            let row = input_table.get_row(i)?;
            result.push_row(row.into_values())?;
        }

        Ok(result)
    }

    pub fn execute_topn(
        &mut self,
        input: &PhysicalPlan,
        sort_exprs: &[SortExpr],
        limit: usize,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();
        let n = input_table.row_count();

        if n == 0 || limit == 0 {
            return Ok(Table::empty(schema));
        }

        let sort_keys = match ColumnarSortKeys::new(&input_table, sort_exprs) {
            Some(keys) => keys,
            None => {
                return self.execute_topn_fallback(input_table, sort_exprs, limit);
            }
        };

        let mut indices: Vec<usize> = (0..n).collect();
        let actual_limit = limit.min(n);

        if actual_limit < n {
            indices.select_nth_unstable_by(actual_limit, |&a, &b| sort_keys.compare(a, b));
            indices.truncate(actual_limit);
        }

        indices.sort_unstable_by(|&a, &b| sort_keys.compare(a, b));

        let mut result = Table::empty(schema);
        for idx in indices {
            let row = input_table.get_row(idx)?;
            result.push_row(row.into_values())?;
        }

        Ok(result)
    }

    fn execute_topn_fallback(
        &self,
        input_table: Table,
        sort_exprs: &[SortExpr],
        limit: usize,
    ) -> Result<Table> {
        use crate::ir_evaluator::IrEvaluator;

        let schema = input_table.schema().clone();
        let records = input_table.rows()?;
        let n = records.len();

        if n == 0 || limit == 0 {
            return Ok(Table::empty(schema));
        }

        let evaluator = IrEvaluator::new(&schema);

        let mut sort_key_columns: Vec<Vec<Value>> = (0..sort_exprs.len())
            .map(|_| Vec::with_capacity(n))
            .collect();

        for record in &records {
            for (i, sort_expr) in sort_exprs.iter().enumerate() {
                let val = evaluator
                    .evaluate(&sort_expr.expr, record)
                    .unwrap_or(Value::Null);
                sort_key_columns[i].push(val);
            }
        }

        let asc: Vec<bool> = sort_exprs.iter().map(|e| e.asc).collect();
        let nulls_first: Vec<bool> = sort_exprs.iter().map(|e| e.nulls_first).collect();

        let compare = |a: usize, b: usize| -> Ordering {
            for (i, col) in sort_key_columns.iter().enumerate() {
                let a_val = &col[a];
                let b_val = &col[b];

                match (a_val.is_null(), b_val.is_null()) {
                    (true, true) => continue,
                    (true, false) => {
                        return if nulls_first[i] {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        };
                    }
                    (false, true) => {
                        return if nulls_first[i] {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        };
                    }
                    (false, false) => {}
                }

                let ordering = compare_values(a_val, b_val);
                let ordering = if asc[i] { ordering } else { ordering.reverse() };

                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            Ordering::Equal
        };

        let mut indices: Vec<usize> = (0..n).collect();
        let actual_limit = limit.min(n);

        if actual_limit < n {
            indices.select_nth_unstable_by(actual_limit, |&a, &b| compare(a, b));
            indices.truncate(actual_limit);
        }

        indices.sort_unstable_by(|&a, &b| compare(a, b));

        let mut result = Table::empty(schema);
        for idx in indices {
            result.push_row(records[idx].values().to_vec())?;
        }

        Ok(result)
    }
}

#[inline]
fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
        (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::Int64(a), Value::Float64(b)) => {
            (*a as f64).partial_cmp(&b.0).unwrap_or(Ordering::Equal)
        }
        (Value::Float64(a), Value::Int64(b)) => {
            a.0.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
        }
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
        (Value::DateTime(a), Value::DateTime(b)) => a.cmp(b),
        (Value::Time(a), Value::Time(b)) => a.cmp(b),
        (Value::Numeric(a), Value::Numeric(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}
