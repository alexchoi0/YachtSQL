use yachtsql_core::error::Result;
use yachtsql_core::types::DataType;
use yachtsql_optimizer::expr::{Expr, OrderByExpr};
use yachtsql_storage::Schema;

use super::WindowExec;
use crate::Table;

impl WindowExec {
    pub(super) fn sort_by_order_by(indices: &mut [usize], order_by: &[OrderByExpr], batch: &Table) {
        use yachtsql_core::types::Value;

        if order_by.is_empty() {
            return;
        }

        indices.sort_by(|&a, &b| {
            for order_expr in order_by {
                let a_val =
                    Self::evaluate_expr(&order_expr.expr, batch, a).unwrap_or(Value::null());
                let b_val =
                    Self::evaluate_expr(&order_expr.expr, batch, b).unwrap_or(Value::null());

                let cmp = Self::compare_values_with_nulls(
                    &a_val,
                    &b_val,
                    order_expr.asc != Some(false),
                    order_expr.nulls_first,
                );

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    pub(super) fn build_partition_key(
        partition_by: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<String> {
        const PARTITION_SEPARATOR: &str = "|";
        let mut partition_key = String::new();

        for part_expr in partition_by {
            let val = Self::evaluate_expr(part_expr, batch, row_idx)?;
            partition_key.push_str(&format!("{:?}", val));
            partition_key.push_str(PARTITION_SEPARATOR);
        }

        Ok(partition_key)
    }

    pub(super) fn extract_column_type(expr: &Expr, schema: &Schema) -> Option<DataType> {
        if let Expr::Column { name: col_name, .. } = expr {
            schema.field(col_name).map(|field| field.data_type.clone())
        } else {
            None
        }
    }

    pub(super) fn infer_avg_return_type(input_type: &DataType) -> DataType {
        match input_type {
            DataType::Numeric(_) | DataType::Int64 => DataType::Numeric(None),
            _ => DataType::Float64,
        }
    }

    pub(super) fn get_window_function_return_type(
        name: &yachtsql_ir::FunctionName,
        args: &[Expr],
        schema: &Schema,
    ) -> DataType {
        use yachtsql_ir::FunctionName;
        match name {
            FunctionName::PercentRank
            | FunctionName::Percentrank
            | FunctionName::CumeDist
            | FunctionName::Cumedist => DataType::Float64,
            FunctionName::Avg | FunctionName::Average => {
                if let Some(first_arg) = args.first()
                    && let Some(input_type) = Self::extract_column_type(first_arg, schema)
                {
                    return Self::infer_avg_return_type(&input_type);
                }
                DataType::Float64
            }
            FunctionName::Sum
            | FunctionName::Min
            | FunctionName::Minimum
            | FunctionName::Max
            | FunctionName::Maximum => {
                if let Some(first_arg) = args.first()
                    && let Some(col_type) = Self::extract_column_type(first_arg, schema)
                {
                    return col_type;
                }
                DataType::Int64
            }
            _ => DataType::Int64,
        }
    }

    pub(super) fn get_window_function_return_type_with_registry(
        name: &yachtsql_ir::FunctionName,
        args: &[Expr],
        schema: &Schema,
        registry: &std::rc::Rc<crate::functions::FunctionRegistry>,
    ) -> DataType {
        use yachtsql_ir::FunctionName;

        match name {
            FunctionName::RowNumber
            | FunctionName::Rownumber
            | FunctionName::Rank
            | FunctionName::Ranking
            | FunctionName::DenseRank
            | FunctionName::Denserank
            | FunctionName::Ntile => return DataType::Int64,
            FunctionName::PercentRank
            | FunctionName::Percentrank
            | FunctionName::CumeDist
            | FunctionName::Cumedist => return DataType::Float64,
            FunctionName::Lag
            | FunctionName::Lead
            | FunctionName::FirstValue
            | FunctionName::First
            | FunctionName::LastValue
            | FunctionName::Last
            | FunctionName::NthValue => {
                if let Some(first_arg) = args.first() {
                    if let Some(col_type) = Self::extract_column_type(first_arg, schema) {
                        return col_type;
                    }
                }

                return DataType::Float64;
            }
            _ => {}
        }

        let func_name_upper = name.as_str().to_uppercase();
        if let Some(agg_func) = registry.get_aggregate(&func_name_upper) {
            let arg_types: Vec<DataType> = args
                .iter()
                .filter_map(|arg| Self::extract_column_type(arg, schema))
                .collect();

            if let Ok(return_type) = agg_func.return_type(&arg_types) {
                if let DataType::Array(elem_type) = return_type {
                    return *elem_type;
                }
                return return_type;
            }
        }

        Self::get_window_function_return_type(name, args, schema)
    }
}
