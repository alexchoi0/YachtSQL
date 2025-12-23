use std::cmp::Ordering;

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::SortExpr;
use yachtsql_storage::{Record, Table};

use super::PlanExecutor;
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_sort(&mut self, input: &PhysicalPlan, sort_exprs: &[SortExpr]) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();
        let evaluator = IrEvaluator::new(&schema);

        let mut records: Vec<Record> = input_table.rows()?;

        records.sort_by(|a, b| {
            for sort_expr in sort_exprs {
                let a_val = evaluator
                    .evaluate(&sort_expr.expr, a)
                    .unwrap_or(Value::Null);
                let b_val = evaluator
                    .evaluate(&sort_expr.expr, b)
                    .unwrap_or(Value::Null);

                let ordering = compare_values(&a_val, &b_val);
                let ordering = if !sort_expr.asc {
                    ordering.reverse()
                } else {
                    ordering
                };

                match (a_val.is_null(), b_val.is_null()) {
                    (true, true) => {}
                    (true, false) => {
                        return if sort_expr.nulls_first {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        };
                    }
                    (false, true) => {
                        return if sort_expr.nulls_first {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        };
                    }
                    (false, false) => {}
                }

                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            Ordering::Equal
        });

        let mut result = Table::empty(schema);
        for record in records {
            result.push_row(record.values().to_vec())?;
        }

        Ok(result)
    }
}

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
