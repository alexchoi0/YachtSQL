mod aggregate_window;
mod comparisons;
mod distribution;
mod frame_values;
mod groups_frame;
mod offset;
mod peer_groups;
mod range_frame;
mod ranking;
mod rows_frame;
mod schema;
mod utils;

use std::rc::Rc;

use offset::OffsetDirection;
use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;
use yachtsql_storage::Schema;

use super::ExecutionPlan;
use crate::Table;

#[derive(Debug)]
pub struct WindowExec {
    input: Rc<dyn ExecutionPlan>,
    schema: Schema,
    window_exprs: Vec<(Expr, Option<String>)>,
    function_registry: Rc<crate::functions::FunctionRegistry>,
}

impl WindowExec {
    pub fn new(
        input: Rc<dyn ExecutionPlan>,
        window_exprs: Vec<(Expr, Option<String>)>,
    ) -> Result<Self> {
        Self::new_with_registry(
            input,
            window_exprs,
            Rc::new(crate::functions::FunctionRegistry::new()),
        )
    }

    pub fn new_with_registry(
        input: Rc<dyn ExecutionPlan>,
        window_exprs: Vec<(Expr, Option<String>)>,
        function_registry: Rc<crate::functions::FunctionRegistry>,
    ) -> Result<Self> {
        let input_schema = input.schema();

        let mut fields: Vec<crate::storage::Field> = input_schema.fields().to_vec();

        for (expr, alias) in &window_exprs {
            let (field_name, data_type) = match expr {
                Expr::WindowFunction { name, args, .. } | Expr::Aggregate { name, args, .. } => {
                    let fname = alias
                        .clone()
                        .unwrap_or_else(|| format!("{}(...)", name.as_str()));
                    let dtype = Self::get_window_function_return_type_with_registry(
                        name,
                        args,
                        input_schema,
                        &function_registry,
                    );
                    (fname, dtype)
                }
                _ => {
                    let fname = alias.clone().unwrap_or_else(|| "window_result".to_string());

                    (fname, crate::types::DataType::Float64)
                }
            };
            fields.push(crate::storage::Field::nullable(field_name, data_type));
        }

        let schema = Schema::from_fields(fields);

        Ok(Self {
            input,
            schema,
            window_exprs,
            function_registry,
        })
    }

    fn compute_window_results(
        window_fn: &Expr,
        mut indices: Vec<usize>,
        batch: &Table,
        window_results: &mut Vec<Value>,
        registry: &Rc<crate::functions::FunctionRegistry>,
    ) {
        let (
            name,
            args,
            order_by,
            frame_units,
            frame_start_offset,
            frame_end_offset,
            exclude,
            null_treatment,
        ) = match window_fn {
            Expr::WindowFunction {
                name,
                args,
                partition_by: _,
                order_by,
                frame_units,
                frame_start_offset,
                frame_end_offset,
                exclude,
                null_treatment,
            } => (
                name.as_str(),
                args.as_slice(),
                order_by.as_slice(),
                *frame_units,
                *frame_start_offset,
                *frame_end_offset,
                *exclude,
                *null_treatment,
            ),
            _ => {
                return;
            }
        };

        let results = window_results.as_mut_slice();

        Self::sort_by_order_by(&mut indices, order_by, batch);

        use yachtsql_optimizer::expr::WindowFrameUnits;

        if frame_units == Some(WindowFrameUnits::Groups) {
            Self::compute_groups_frame_window(
                name,
                args,
                &indices,
                order_by,
                batch,
                results,
                frame_start_offset,
                frame_end_offset,
                exclude,
                registry,
            );
            return;
        }

        if frame_units == Some(WindowFrameUnits::Range) {
            let func_name_upper = name.to_uppercase();

            if registry.has_aggregate(&func_name_upper)
                || func_name_upper == "FIRST_VALUE"
                || func_name_upper == "LAST_VALUE"
                || func_name_upper == "NTH_VALUE"
            {
                Self::compute_range_frame_window(
                    name,
                    args,
                    &indices,
                    order_by,
                    batch,
                    results,
                    frame_start_offset,
                    frame_end_offset,
                    exclude,
                    registry,
                );
                return;
            }
        }

        if frame_units == Some(WindowFrameUnits::Rows) {
            let func_name_upper = name.to_uppercase();

            if registry.has_aggregate(&func_name_upper)
                || func_name_upper == "FIRST_VALUE"
                || func_name_upper == "LAST_VALUE"
                || func_name_upper == "NTH_VALUE"
            {
                Self::compute_rows_frame_window(
                    name,
                    args,
                    &indices,
                    order_by,
                    batch,
                    results,
                    frame_start_offset,
                    frame_end_offset,
                    exclude,
                    registry,
                );
                return;
            }
        }

        if frame_units.is_none() && !order_by.is_empty() {
            let func_name_upper = name.to_uppercase();

            let is_ranking_function = matches!(
                func_name_upper.as_str(),
                "ROW_NUMBER"
                    | "RANK"
                    | "DENSE_RANK"
                    | "NTILE"
                    | "PERCENT_RANK"
                    | "CUME_DIST"
                    | "LAG"
                    | "LEAD"
                    | "FIRST_VALUE"
                    | "LAST_VALUE"
                    | "NTH_VALUE"
            );

            if !is_ranking_function && registry.has_aggregate(&func_name_upper) {
                Self::compute_range_frame_window(
                    name,
                    args,
                    &indices,
                    order_by,
                    batch,
                    results,
                    None,
                    Some(0),
                    exclude,
                    registry,
                );
                return;
            }
        }

        match name.to_uppercase().as_str() {
            "ROW_NUMBER" => Self::compute_row_number(&indices, results),
            "RANK" => Self::compute_rank(&indices, order_by, batch, results, false),
            "DENSE_RANK" => Self::compute_rank(&indices, order_by, batch, results, true),
            "LAG" => Self::compute_offset_function(
                &indices,
                args,
                batch,
                results,
                OffsetDirection::Backward,
                null_treatment,
            ),
            "LEAD" => Self::compute_offset_function(
                &indices,
                args,
                batch,
                results,
                OffsetDirection::Forward,
                null_treatment,
            ),
            "FIRST_VALUE" => Self::compute_first_value(
                &indices,
                args,
                batch,
                results,
                order_by,
                exclude,
                null_treatment,
            ),
            "LAST_VALUE" => Self::compute_last_value(
                &indices,
                args,
                batch,
                results,
                order_by,
                exclude,
                null_treatment,
            ),
            "NTH_VALUE" => Self::compute_nth_value(
                &indices,
                args,
                batch,
                results,
                order_by,
                exclude,
                null_treatment,
            ),
            "PERCENT_RANK" => Self::compute_percent_rank(&indices, order_by, batch, results),
            "CUME_DIST" => Self::compute_cume_dist(&indices, order_by, batch, results),
            "NTILE" => Self::compute_ntile(&indices, args, results),
            func_name => {
                if registry.has_aggregate(func_name) {
                    Self::compute_aggregate_window_function(
                        &indices, args, order_by, batch, results, exclude, registry, func_name,
                    );
                } else {
                    Self::compute_unknown_function(&indices, results);
                }
            }
        }
    }
}

impl ExecutionPlan for WindowExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        use yachtsql_core::types::Value;
        use yachtsql_storage::Column;

        use crate::Table;

        let input_batches = self.input.execute()?;

        if input_batches.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let mut result_batches = Vec::new();

        for input_batch in &input_batches {
            let num_rows = input_batch.num_rows();

            let mut output_columns: Vec<Column> = Vec::new();

            for i in 0..input_batch.schema().field_count() {
                if let Some(col) = input_batch.column(i) {
                    output_columns.push(col.clone());
                }
            }

            for (expr, _alias) in &self.window_exprs {
                let (name, args, partition_by) = match expr {
                    Expr::WindowFunction {
                        name,
                        args,
                        partition_by,
                        ..
                    } => (name, args, partition_by.as_slice()),
                    Expr::Aggregate { name, args, .. } => (name, args, &[][..]),
                    _ => {
                        return Err(crate::error::Error::unsupported_feature(format!(
                            "Non-window expression in Window node: {:?}",
                            expr
                        )));
                    }
                };

                match expr {
                    Expr::WindowFunction { .. } | Expr::Aggregate { .. } => {
                        let mut window_results = vec![Value::null(); num_rows];

                        if !partition_by.is_empty() {
                            let mut partitions: std::collections::HashMap<String, Vec<usize>> =
                                std::collections::HashMap::new();

                            for row_idx in 0..num_rows {
                                let partition_key =
                                    Self::build_partition_key(partition_by, input_batch, row_idx)?;
                                partitions.entry(partition_key).or_default().push(row_idx);
                            }

                            for (_partition_key, row_indices) in partitions {
                                Self::compute_window_results(
                                    expr,
                                    row_indices,
                                    input_batch,
                                    &mut window_results,
                                    &self.function_registry,
                                );
                            }
                        } else {
                            let all_indices: Vec<usize> = (0..num_rows).collect();
                            Self::compute_window_results(
                                expr,
                                all_indices,
                                input_batch,
                                &mut window_results,
                                &self.function_registry,
                            );
                        }

                        let data_type = Self::get_window_function_return_type_with_registry(
                            name,
                            args,
                            input_batch.schema(),
                            &self.function_registry,
                        );
                        let mut window_column = Column::new(&data_type, num_rows);
                        for value in window_results {
                            window_column.push(value)?;
                        }
                        output_columns.push(window_column);
                    }
                    _ => {
                        return Err(crate::error::Error::unsupported_feature(format!(
                            "Non-window expression in Window node: {:?}",
                            expr
                        )));
                    }
                }
            }

            result_batches.push(Table::new(self.schema.clone(), output_columns)?);
        }

        Ok(result_batches)
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn describe(&self) -> String {
        format!("Window ({} functions)", self.window_exprs.len())
    }
}
