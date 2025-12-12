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
use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
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

        let normalized_window_exprs =
            Self::normalize_nested_aggregates(&window_exprs, input_schema);

        let mut fields: Vec<crate::storage::Field> = input_schema.fields().to_vec();

        for (expr, alias) in &normalized_window_exprs {
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
            window_exprs: normalized_window_exprs,
            function_registry,
        })
    }

    fn normalize_nested_aggregates(
        window_exprs: &[(Expr, Option<String>)],
        input_schema: &Schema,
    ) -> Vec<(Expr, Option<String>)> {
        window_exprs
            .iter()
            .map(|(expr, alias)| {
                let normalized = Self::normalize_window_expr(expr, input_schema);
                (normalized, alias.clone())
            })
            .collect()
    }

    fn normalize_window_expr(expr: &Expr, input_schema: &Schema) -> Expr {
        match expr {
            Expr::WindowFunction {
                name,
                args,
                partition_by,
                order_by,
                frame_units,
                frame_start_offset,
                frame_end_offset,
                exclude,
                null_treatment,
            } => {
                let normalized_args: Vec<Expr> = args
                    .iter()
                    .map(|arg| Self::replace_nested_aggregate(arg, input_schema))
                    .collect();

                let normalized_order_by: Vec<yachtsql_optimizer::expr::OrderByExpr> = order_by
                    .iter()
                    .map(|ob| yachtsql_optimizer::expr::OrderByExpr {
                        expr: Self::normalize_expr_for_input(&ob.expr, input_schema),
                        asc: ob.asc,
                        nulls_first: ob.nulls_first,
                        collation: ob.collation.clone(),
                        with_fill: ob.with_fill.clone(),
                    })
                    .collect();

                let normalized_partition_by: Vec<Expr> = partition_by
                    .iter()
                    .map(|pb| Self::normalize_expr_for_input(pb, input_schema))
                    .collect();

                Expr::WindowFunction {
                    name: name.clone(),
                    args: normalized_args,
                    partition_by: normalized_partition_by,
                    order_by: normalized_order_by,
                    frame_units: *frame_units,
                    frame_start_offset: *frame_start_offset,
                    frame_end_offset: *frame_end_offset,
                    exclude: *exclude,
                    null_treatment: *null_treatment,
                }
            }
            Expr::Aggregate {
                name,
                args,
                distinct,
                order_by,
                filter,
            } => {
                let normalized_args: Vec<Expr> = args
                    .iter()
                    .map(|arg| Self::replace_nested_aggregate(arg, input_schema))
                    .collect();

                Expr::Aggregate {
                    name: name.clone(),
                    args: normalized_args,
                    distinct: *distinct,
                    order_by: order_by.clone(),
                    filter: filter.clone(),
                }
            }
            _ => expr.clone(),
        }
    }

    fn normalize_expr_for_input(expr: &Expr, input_schema: &Schema) -> Expr {
        let fields = input_schema.fields();

        match expr {
            Expr::Column { name, .. } => {
                if fields.iter().any(|f| f.name == *name) {
                    return expr.clone();
                }
                expr.clone()
            }
            Expr::Function {
                name: func_name,
                args,
            } => {
                let func_str = Self::function_to_string(func_name, args);
                if let Some(field) = fields.iter().find(|f| f.name == func_str) {
                    return Expr::Column {
                        name: field.name.clone(),
                        table: None,
                    };
                }

                let func_name_str = func_name.as_str().to_uppercase();
                if func_name_str == "DATE_TRUNC" || func_name_str == "TIMESTAMP_TRUNC" {
                    if let Some(field) = fields.iter().find(|f| {
                        matches!(
                            f.data_type,
                            crate::types::DataType::Date
                                | crate::types::DataType::Timestamp
                                | crate::types::DataType::DateTime
                        )
                    }) {
                        return Expr::Column {
                            name: field.name.clone(),
                            table: None,
                        };
                    }
                }

                for field in fields {
                    if Self::expr_might_match_field(expr, &field.name, &field.data_type) {
                        return Expr::Column {
                            name: field.name.clone(),
                            table: None,
                        };
                    }
                }
                expr.clone()
            }
            Expr::Aggregate { .. } => Self::replace_nested_aggregate(expr, input_schema),
            _ => expr.clone(),
        }
    }

    fn function_to_string(name: &yachtsql_ir::function::FunctionName, args: &[Expr]) -> String {
        let arg_str = args
            .iter()
            .map(|a| match a {
                Expr::Column { name, .. } => name.clone(),
                Expr::Wildcard => "*".to_string(),
                _ => "_".to_string(),
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("{}({})", name.as_str(), arg_str)
    }

    fn expr_might_match_field(
        expr: &Expr,
        field_name: &str,
        _data_type: &crate::types::DataType,
    ) -> bool {
        match expr {
            Expr::Function { name, args } => {
                let func_name = name.as_str().to_lowercase();
                let field_lower = field_name.to_lowercase();

                if field_lower.starts_with(&func_name) || field_lower.contains(&func_name) {
                    return true;
                }

                if let Some(Expr::Column { name: col_name, .. }) = args.first() {
                    if field_lower.contains(&col_name.to_lowercase()) {
                        return true;
                    }
                }
                false
            }
            _ => false,
        }
    }

    fn replace_nested_aggregate(expr: &Expr, input_schema: &Schema) -> Expr {
        match expr {
            Expr::Aggregate {
                name,
                args,
                distinct,
                ..
            } => {
                let func_name = name.as_str();
                let fields = input_schema.fields();

                if let Some(field) = fields.iter().find(|f| f.name == func_name) {
                    return Expr::Column {
                        name: field.name.clone(),
                        table: None,
                    };
                }

                let arg_str = args
                    .iter()
                    .map(|a| match a {
                        Expr::Column { name, .. } => name.clone(),
                        Expr::Wildcard => "*".to_string(),
                        _ => "*".to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(", ");

                let full_name = format!("{}({})", func_name, arg_str);
                if let Some(field) = fields.iter().find(|f| f.name == full_name) {
                    return Expr::Column {
                        name: field.name.clone(),
                        table: None,
                    };
                }

                let distinct_prefix = if *distinct { "DISTINCT " } else { "" };
                let full_name_distinct = format!("{}({}{})", func_name, distinct_prefix, arg_str);
                if let Some(field) = fields.iter().find(|f| f.name == full_name_distinct) {
                    return Expr::Column {
                        name: field.name.clone(),
                        table: None,
                    };
                }

                let expected_type = Self::infer_aggregate_result_type(func_name);
                if let Some(field) = fields.iter().find(|f| {
                    Self::is_compatible_type(&f.data_type, expected_type) && !f.name.contains("__")
                }) {
                    return Expr::Column {
                        name: field.name.clone(),
                        table: None,
                    };
                }

                expr.clone()
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::replace_nested_aggregate(left, input_schema)),
                op: op.clone(),
                right: Box::new(Self::replace_nested_aggregate(right, input_schema)),
            },
            _ => expr.clone(),
        }
    }

    fn infer_aggregate_result_type(func_name: &str) -> &'static str {
        match func_name.to_uppercase().as_str() {
            "COUNT" => "int64",
            "SUM" => "numeric",
            "AVG" => "float64",
            "MIN" | "MAX" => "any",
            _ => "any",
        }
    }

    fn is_compatible_type(data_type: &crate::types::DataType, expected: &str) -> bool {
        match expected {
            "int64" => matches!(data_type, crate::types::DataType::Int64),
            "float64" => matches!(data_type, crate::types::DataType::Float64),
            "numeric" => matches!(
                data_type,
                crate::types::DataType::Int64
                    | crate::types::DataType::Float64
                    | crate::types::DataType::Numeric { .. }
            ),
            _ => true,
        }
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
        use yachtsql_common::types::Value;
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
