use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;
use yachtsql_optimizer::plan::JoinType;
use yachtsql_storage::{Column, Schema};

use super::ExecutionPlan;
use crate::Table;

#[derive(Debug)]
pub struct HashJoinExec {
    left: Rc<dyn ExecutionPlan>,
    right: Rc<dyn ExecutionPlan>,
    schema: Schema,

    join_type: JoinType,

    on_conditions: Vec<(Expr, Expr)>,
}

impl HashJoinExec {
    pub fn new(
        left: Rc<dyn ExecutionPlan>,
        right: Rc<dyn ExecutionPlan>,
        join_type: JoinType,
        on_conditions: Vec<(Expr, Expr)>,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        let mut fields = Vec::new();
        for field in left_schema.fields() {
            fields.push(field.clone());
        }

        if !matches!(join_type, JoinType::Semi | JoinType::Anti) {
            for field in right_schema.fields() {
                fields.push(field.clone());
            }
        }

        let schema = Schema::from_fields(fields);

        Ok(Self {
            left,
            right,
            schema,
            join_type,
            on_conditions,
        })
    }

    fn build_hash_table(
        &self,
        batch: &Table,
        is_left: bool,
    ) -> Result<HashMap<Vec<u8>, Vec<Vec<Value>>>> {
        let mut hash_table: HashMap<Vec<u8>, Vec<Vec<Value>>> = HashMap::new();

        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            let mut join_key = Vec::new();
            let exprs = if is_left {
                self.on_conditions
                    .iter()
                    .map(|(l, _)| l)
                    .collect::<Vec<_>>()
            } else {
                self.on_conditions
                    .iter()
                    .map(|(_, r)| r)
                    .collect::<Vec<_>>()
            };

            let mut has_null_key = false;
            for expr in exprs {
                let value = self.evaluate_expr(expr, batch, row_idx)?;
                if value.is_null() {
                    has_null_key = true;
                }
                join_key.push(value);
            }

            if has_null_key {
                continue;
            }

            let key_bytes = serialize_key(&join_key);

            let mut row_data = Vec::new();
            for col in batch.expect_columns() {
                row_data.push(col.get(row_idx)?);
            }

            hash_table.entry(key_bytes).or_default().push(row_data);
        }

        Ok(hash_table)
    }

    fn evaluate_expr(&self, expr: &Expr, batch: &Table, row_idx: usize) -> Result<Value> {
        super::ProjectionWithExprExec::evaluate_expr(expr, batch, row_idx)
    }

    fn build_result_batch(&self, result_rows: &[Vec<Value>]) -> Result<Vec<Table>> {
        if result_rows.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let num_output_rows = result_rows.len();
        let num_cols = self.schema.fields().len();
        let mut columns = Vec::new();

        for col_idx in 0..num_cols {
            let field = &self.schema.fields()[col_idx];
            let mut column = Column::new(&field.data_type, num_output_rows);

            for row in result_rows {
                column.push(row[col_idx].clone())?;
            }

            columns.push(column);
        }

        Ok(vec![Table::new(self.schema.clone(), columns)?])
    }
}

impl ExecutionPlan for HashJoinExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let left_batches: Vec<Table> = self
            .left
            .execute()?
            .into_iter()
            .map(|b| b.to_column_format())
            .collect::<Result<Vec<_>>>()?;
        let right_batches: Vec<Table> = self
            .right
            .execute()?
            .into_iter()
            .map(|b| b.to_column_format())
            .collect::<Result<Vec<_>>>()?;

        if left_batches.is_empty() || right_batches.is_empty() {
            match self.join_type {
                JoinType::Inner | JoinType::Right | JoinType::Semi => {
                    return Ok(vec![Table::empty(self.schema.clone())]);
                }
                JoinType::Anti => {
                    if right_batches.is_empty() && !left_batches.is_empty() {
                        let mut result_rows: Vec<Vec<Value>> = Vec::new();
                        for left_batch in &left_batches {
                            for row_idx in 0..left_batch.num_rows() {
                                let mut left_row = Vec::new();
                                for col in left_batch.expect_columns() {
                                    left_row.push(col.get(row_idx)?);
                                }
                                result_rows.push(left_row);
                            }
                        }
                        return self.build_result_batch(&result_rows);
                    }
                    return Ok(vec![Table::empty(self.schema.clone())]);
                }
                _ => {}
            }
        }

        let mut right_hash_table: HashMap<Vec<u8>, Vec<Vec<Value>>> = HashMap::new();
        for right_batch in &right_batches {
            let batch_table = self.build_hash_table(right_batch, false)?;
            for (key, rows) in batch_table {
                right_hash_table.entry(key).or_default().extend(rows);
            }
        }

        let mut result_rows: Vec<Vec<Value>> = Vec::new();
        let mut matched_right_keys: HashMap<Vec<u8>, bool> = HashMap::new();

        for left_batch in &left_batches {
            let num_rows = left_batch.num_rows();

            for row_idx in 0..num_rows {
                let mut join_key = Vec::new();
                let mut has_null_key = false;
                for (left_expr, _) in &self.on_conditions {
                    let value = self.evaluate_expr(left_expr, left_batch, row_idx)?;
                    if value.is_null() {
                        has_null_key = true;
                    }
                    join_key.push(value);
                }

                let mut left_row = Vec::new();
                for col in left_batch.expect_columns() {
                    left_row.push(col.get(row_idx)?);
                }

                if has_null_key {
                    match self.join_type {
                        JoinType::Semi => {}
                        JoinType::Anti => {
                            result_rows.push(left_row);
                        }
                        JoinType::Left | JoinType::Full => {
                            let mut combined_row = left_row;
                            let right_null_cols = self.right.schema().fields().len();
                            combined_row.extend(vec![Value::null(); right_null_cols]);
                            result_rows.push(combined_row);
                        }
                        _ => {}
                    }
                    continue;
                }

                let key_bytes = serialize_key(&join_key);
                let has_match = right_hash_table.contains_key(&key_bytes);

                match self.join_type {
                    JoinType::Semi => {
                        if has_match {
                            result_rows.push(left_row);
                        }
                    }
                    JoinType::Anti => {
                        if !has_match {
                            result_rows.push(left_row);
                        }
                    }
                    _ => {
                        if let Some(right_rows) = right_hash_table.get(&key_bytes) {
                            matched_right_keys.insert(key_bytes.clone(), true);

                            for right_row in right_rows {
                                let mut combined_row = left_row.clone();
                                combined_row.extend(right_row.clone());
                                result_rows.push(combined_row);
                            }
                        } else {
                            match self.join_type {
                                JoinType::Left | JoinType::Full => {
                                    let mut combined_row = left_row.clone();
                                    let right_null_cols = self.right.schema().fields().len();
                                    combined_row.extend(vec![Value::null(); right_null_cols]);
                                    result_rows.push(combined_row);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            for (key_bytes, right_rows) in &right_hash_table {
                if !matched_right_keys.contains_key(key_bytes) {
                    for right_row in right_rows {
                        let left_null_cols = self.left.schema().fields().len();
                        let mut combined_row = vec![Value::null(); left_null_cols];
                        combined_row.extend(right_row.clone());
                        result_rows.push(combined_row);
                    }
                }
            }
        }

        self.build_result_batch(&result_rows)
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn describe(&self) -> String {
        format!(
            "HashJoin [{:?}, conditions: {}]",
            self.join_type,
            self.on_conditions.len()
        )
    }
}

#[derive(Debug)]
pub struct NestedLoopJoinExec {
    left: Rc<dyn ExecutionPlan>,
    right: Rc<dyn ExecutionPlan>,
    schema: Schema,
    join_type: JoinType,

    predicate: Option<Expr>,
}

impl NestedLoopJoinExec {
    pub fn new(
        left: Rc<dyn ExecutionPlan>,
        right: Rc<dyn ExecutionPlan>,
        join_type: JoinType,
        predicate: Option<Expr>,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        let mut fields = Vec::new();
        for field in left_schema.fields() {
            fields.push(field.clone());
        }
        for field in right_schema.fields() {
            fields.push(field.clone());
        }

        let schema = Schema::from_fields(fields);

        Ok(Self {
            left,
            right,
            schema,
            join_type,
            predicate,
        })
    }

    fn evaluate_predicate(&self, left_row: &[Value], right_row: &[Value]) -> Result<bool> {
        match &self.predicate {
            None => Ok(true),
            Some(pred) => {
                let mut columns = Vec::new();
                for (idx, field) in self.schema.fields().iter().enumerate() {
                    let mut col = Column::new(&field.data_type, 1);
                    if idx < left_row.len() {
                        col.push(left_row[idx].clone())?;
                    } else {
                        col.push(right_row[idx - left_row.len()].clone())?;
                    }
                    columns.push(col);
                }
                let batch = Table::new(self.schema.clone(), columns)?;
                let result = super::ProjectionWithExprExec::evaluate_expr(pred, &batch, 0)?;
                Ok(result.as_bool().unwrap_or(false))
            }
        }
    }
}

impl ExecutionPlan for NestedLoopJoinExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let left_batches: Vec<Table> = self
            .left
            .execute()?
            .into_iter()
            .map(|b| b.to_column_format())
            .collect::<Result<Vec<_>>>()?;
        let right_batches: Vec<Table> = self
            .right
            .execute()?
            .into_iter()
            .map(|b| b.to_column_format())
            .collect::<Result<Vec<_>>>()?;

        if left_batches.is_empty() || right_batches.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let mut result_rows: Vec<Vec<Value>> = Vec::new();

        for left_batch in &left_batches {
            for left_row_idx in 0..left_batch.num_rows() {
                let mut left_row = Vec::new();
                for col in left_batch.expect_columns() {
                    left_row.push(col.get(left_row_idx)?);
                }

                let mut found_match = false;

                for right_batch in &right_batches {
                    for right_row_idx in 0..right_batch.num_rows() {
                        let mut right_row = Vec::new();
                        for col in right_batch.expect_columns() {
                            right_row.push(col.get(right_row_idx)?);
                        }

                        if self.evaluate_predicate(&left_row, &right_row)? {
                            found_match = true;
                            let mut combined_row = left_row.clone();
                            combined_row.extend(right_row);
                            result_rows.push(combined_row);
                        }
                    }
                }

                if !found_match && matches!(self.join_type, JoinType::Left | JoinType::Full) {
                    let mut combined_row = left_row.clone();
                    let right_null_cols = self.right.schema().fields().len();
                    combined_row.extend(vec![Value::null(); right_null_cols]);
                    result_rows.push(combined_row);
                }
            }
        }

        if result_rows.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let num_output_rows = result_rows.len();
        let num_cols = self.schema.fields().len();
        let mut columns = Vec::new();

        for col_idx in 0..num_cols {
            let field = &self.schema.fields()[col_idx];
            let mut column = Column::new(&field.data_type, num_output_rows);

            for row in &result_rows {
                column.push(row[col_idx].clone())?;
            }

            columns.push(column);
        }

        Ok(vec![Table::new(self.schema.clone(), columns)?])
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn describe(&self) -> String {
        format!("NestedLoopJoin [{:?}]", self.join_type)
    }
}

#[derive(Debug)]
pub struct LateralJoinExec {
    left: Rc<dyn ExecutionPlan>,

    right_subquery: yachtsql_ir::plan::PlanNode,
    schema: Schema,

    join_type: JoinType,

    storage: Rc<RefCell<yachtsql_storage::Storage>>,
}

impl LateralJoinExec {
    pub fn new(
        left: Rc<dyn ExecutionPlan>,
        right_subquery: yachtsql_ir::plan::PlanNode,
        join_type: JoinType,
        storage: Rc<RefCell<yachtsql_storage::Storage>>,
    ) -> Result<Self> {
        let left_schema = left.schema();

        let right_schema = Self::infer_subquery_schema(&right_subquery, &storage)?;

        let mut fields = Vec::new();
        for field in left_schema.fields() {
            fields.push(field.clone());
        }
        for field in right_schema.fields() {
            fields.push(field.clone());
        }

        let schema = Schema::from_fields(fields);

        Ok(Self {
            left,
            right_subquery,
            schema,
            join_type,
            storage,
        })
    }

    fn infer_subquery_schema(
        node: &yachtsql_ir::plan::PlanNode,
        storage: &Rc<RefCell<yachtsql_storage::Storage>>,
    ) -> Result<Schema> {
        use yachtsql_ir::plan::PlanNode;
        use yachtsql_storage::Field;

        match node {
            PlanNode::Projection { expressions, input } => {
                let input_schema = Self::infer_subquery_schema(input, storage)?;

                let mut fields = Vec::with_capacity(expressions.len());
                for (idx, (expr, alias)) in expressions.iter().enumerate() {
                    let data_type = super::ProjectionWithExprExec::infer_expr_type_with_schema(
                        expr,
                        &input_schema,
                    )
                    .unwrap_or(yachtsql_core::types::DataType::String);

                    let field_name = if let Some(alias) = alias {
                        alias.clone()
                    } else {
                        match expr {
                            yachtsql_ir::expr::Expr::Column { name, .. } => name.clone(),
                            _ => format!("expr_{}", idx),
                        }
                    };

                    fields.push(Field::nullable(field_name, data_type));
                }

                Ok(Schema::from_fields(fields))
            }

            PlanNode::Scan {
                table_name,
                projection,
                ..
            } => {
                let storage = storage.borrow_mut();
                let table = storage
                    .get_table(table_name)
                    .ok_or_else(|| Error::table_not_found(table_name.clone()))?;
                let table_schema = table.schema();

                if let Some(proj_cols) = projection {
                    let fields: Vec<_> = proj_cols
                        .iter()
                        .filter_map(|col_name| {
                            table_schema
                                .fields()
                                .iter()
                                .find(|f| &f.name == col_name)
                                .cloned()
                        })
                        .collect();
                    Ok(Schema::from_fields(fields))
                } else {
                    Ok(table_schema.clone())
                }
            }

            PlanNode::SubqueryScan { subquery, alias } => {
                let mut schema = Self::infer_subquery_schema(subquery, storage)?;

                for field in schema.fields_mut() {
                    field.source_table = Some(alias.clone());
                }
                Ok(schema)
            }

            PlanNode::Filter { input, .. }
            | PlanNode::Sort { input, .. }
            | PlanNode::Limit { input, .. }
            | PlanNode::Distinct { input } => Self::infer_subquery_schema(input, storage),

            PlanNode::Aggregate {
                group_by,
                aggregates,
                input,
                ..
            } => {
                let input_schema = Self::infer_subquery_schema(input, storage)?;
                let mut fields = Vec::new();

                for (idx, expr) in group_by.iter().enumerate() {
                    let data_type = super::ProjectionWithExprExec::infer_expr_type_with_schema(
                        expr,
                        &input_schema,
                    )
                    .unwrap_or(yachtsql_core::types::DataType::String);
                    let name = match expr {
                        yachtsql_ir::expr::Expr::Column { name, .. } => name.clone(),
                        _ => format!("group_{}", idx),
                    };
                    fields.push(Field::nullable(name, data_type));
                }

                for (idx, expr) in aggregates.iter().enumerate() {
                    let (name, data_type) = match expr {
                        yachtsql_ir::expr::Expr::Aggregate {
                            name: agg_name,
                            args,
                            ..
                        } => {
                            use yachtsql_ir::function::FunctionName;
                            let dt = match agg_name {
                                FunctionName::Count => yachtsql_core::types::DataType::Int64,
                                FunctionName::Avg | FunctionName::Sum => {
                                    yachtsql_core::types::DataType::Float64
                                }
                                FunctionName::Min | FunctionName::Max => {
                                    super::ProjectionWithExprExec::infer_expr_type_with_schema(
                                        expr,
                                        &input_schema,
                                    )
                                    .unwrap_or(yachtsql_core::types::DataType::Float64)
                                }
                                _ => yachtsql_core::types::DataType::Float64,
                            };

                            let arg_str = if args.is_empty() {
                                "*".to_string()
                            } else {
                                args.iter()
                                    .map(|a| match a {
                                        yachtsql_ir::expr::Expr::Column { name, .. } => {
                                            name.clone()
                                        }
                                        yachtsql_ir::expr::Expr::Wildcard => "*".to_string(),
                                        _ => format!("{:?}", a),
                                    })
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            };
                            (format!("{}({})", agg_name.as_str(), arg_str), dt)
                        }
                        _ => {
                            let dt = super::ProjectionWithExprExec::infer_expr_type_with_schema(
                                expr,
                                &input_schema,
                            )
                            .unwrap_or(yachtsql_core::types::DataType::Float64);
                            (format!("agg_{}", idx), dt)
                        }
                    };
                    fields.push(Field::nullable(name, data_type));
                }

                Ok(Schema::from_fields(fields))
            }

            PlanNode::TableValuedFunction {
                function_name,
                alias,
                ..
            } => {
                let fn_upper = function_name.to_uppercase();
                let mut schema = match fn_upper.as_str() {
                    "JSON_EACH" | "JSONB_EACH" => Schema::from_fields(vec![
                        Field::nullable("key", yachtsql_core::types::DataType::String),
                        Field::nullable("value", yachtsql_core::types::DataType::Json),
                    ]),
                    "JSON_EACH_TEXT" | "JSONB_EACH_TEXT" => Schema::from_fields(vec![
                        Field::nullable("key", yachtsql_core::types::DataType::String),
                        Field::nullable("value", yachtsql_core::types::DataType::String),
                    ]),
                    "UNNEST" => Schema::from_fields(vec![Field::nullable(
                        "unnest",
                        yachtsql_core::types::DataType::String,
                    )]),
                    "GENERATE_SERIES" => Schema::from_fields(vec![Field::nullable(
                        "generate_series",
                        yachtsql_core::types::DataType::Int64,
                    )]),
                    _ => Schema::from_fields(vec![]),
                };
                if let Some(table_alias) = alias {
                    for field in schema.fields_mut() {
                        field.source_table = Some(table_alias.clone());
                    }
                }
                Ok(schema)
            }

            PlanNode::Unnest {
                alias,
                column_alias,
                ..
            } => {
                let element_name = column_alias
                    .clone()
                    .or_else(|| alias.clone())
                    .unwrap_or_else(|| "value".to_string());

                let mut field =
                    Field::nullable(element_name, yachtsql_core::types::DataType::String);
                if let Some(table_alias) = alias {
                    field = field.with_source_table(table_alias.clone());
                }
                Ok(Schema::from_fields(vec![field]))
            }

            _ => Ok(Schema::from_fields(vec![])),
        }
    }

    fn execute_correlated_subquery(
        &self,
        left_row: &[Value],
        left_schema: &Schema,
    ) -> Result<(Vec<Vec<Value>>, Schema)> {
        use yachtsql_ir::plan::LogicalPlan;

        use crate::query_executor::logical_to_physical::LogicalToPhysicalPlanner;

        let substituted_plan =
            self.substitute_correlations(&self.right_subquery, left_row, left_schema)?;

        let logical_plan = LogicalPlan::new(substituted_plan);
        let planner = LogicalToPhysicalPlanner::new(Rc::clone(&self.storage));
        let physical_plan = planner.create_physical_plan(&logical_plan)?;
        let right_schema = physical_plan.schema().clone();
        let batches = physical_plan.execute()?;

        let mut result_rows = Vec::new();
        for batch in batches {
            let num_rows = batch.num_rows();
            for row_idx in 0..num_rows {
                let mut row = Vec::new();
                for col in batch.expect_columns() {
                    row.push(col.get(row_idx)?);
                }
                result_rows.push(row);
            }
        }

        Ok((result_rows, right_schema))
    }

    fn substitute_correlations(
        &self,
        node: &yachtsql_ir::plan::PlanNode,
        left_row: &[Value],
        left_schema: &Schema,
    ) -> Result<yachtsql_ir::plan::PlanNode> {
        use yachtsql_ir::plan::PlanNode;

        match node {
            PlanNode::Filter { predicate, input } => {
                let substituted_predicate =
                    self.substitute_expr(predicate, left_row, left_schema)?;
                let substituted_input =
                    self.substitute_correlations(input, left_row, left_schema)?;
                Ok(PlanNode::Filter {
                    predicate: substituted_predicate,
                    input: Box::new(substituted_input),
                })
            }
            PlanNode::Projection { expressions, input } => {
                let substituted_exprs: Vec<_> = expressions
                    .iter()
                    .map(|(expr, alias)| {
                        Ok((
                            self.substitute_expr(expr, left_row, left_schema)?,
                            alias.clone(),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let substituted_input =
                    self.substitute_correlations(input, left_row, left_schema)?;
                Ok(PlanNode::Projection {
                    expressions: substituted_exprs,
                    input: Box::new(substituted_input),
                })
            }
            PlanNode::Limit {
                limit,
                offset,
                input,
            } => {
                let substituted_input =
                    self.substitute_correlations(input, left_row, left_schema)?;
                Ok(PlanNode::Limit {
                    limit: *limit,
                    offset: *offset,
                    input: Box::new(substituted_input),
                })
            }
            PlanNode::Sort { order_by, input } => {
                let substituted_input =
                    self.substitute_correlations(input, left_row, left_schema)?;
                Ok(PlanNode::Sort {
                    order_by: order_by.clone(),
                    input: Box::new(substituted_input),
                })
            }
            PlanNode::SubqueryScan { subquery, alias } => {
                let substituted_subquery =
                    self.substitute_correlations(subquery, left_row, left_schema)?;
                Ok(PlanNode::SubqueryScan {
                    subquery: Box::new(substituted_subquery),
                    alias: alias.clone(),
                })
            }

            PlanNode::TableValuedFunction {
                function_name,
                args,
                alias,
            } => {
                let substituted_args: Vec<_> = args
                    .iter()
                    .map(|arg| self.substitute_expr(arg, left_row, left_schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok(PlanNode::TableValuedFunction {
                    function_name: function_name.clone(),
                    args: substituted_args,
                    alias: alias.clone(),
                })
            }

            PlanNode::Aggregate {
                group_by,
                aggregates,
                input,
                grouping_metadata,
            } => {
                let substituted_input =
                    self.substitute_correlations(input, left_row, left_schema)?;
                let substituted_group_by: Vec<_> = group_by
                    .iter()
                    .map(|expr| self.substitute_expr(expr, left_row, left_schema))
                    .collect::<Result<Vec<_>>>()?;
                let substituted_aggregates: Vec<_> = aggregates
                    .iter()
                    .map(|expr| self.substitute_expr(expr, left_row, left_schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok(PlanNode::Aggregate {
                    group_by: substituted_group_by,
                    aggregates: substituted_aggregates,
                    input: Box::new(substituted_input),
                    grouping_metadata: grouping_metadata.clone(),
                })
            }

            PlanNode::Unnest {
                array_expr,
                alias,
                column_alias,
                with_offset,
                offset_alias,
            } => {
                let substituted_array_expr =
                    self.substitute_expr(array_expr, left_row, left_schema)?;
                Ok(PlanNode::Unnest {
                    array_expr: substituted_array_expr,
                    alias: alias.clone(),
                    column_alias: column_alias.clone(),
                    with_offset: *with_offset,
                    offset_alias: offset_alias.clone(),
                })
            }

            PlanNode::Scan { .. } | PlanNode::Distinct { .. } => Ok(node.clone()),

            _ => Ok(node.clone()),
        }
    }

    fn value_to_literal(&self, value: &Value) -> Result<yachtsql_ir::expr::LiteralValue> {
        use yachtsql_ir::expr::LiteralValue;

        if value.is_null() {
            return Ok(LiteralValue::Null);
        }

        if let Some(b) = value.as_bool() {
            return Ok(LiteralValue::Boolean(b));
        }

        if let Some(i) = value.as_i64() {
            return Ok(LiteralValue::Int64(i));
        }

        if let Some(f) = value.as_f64() {
            return Ok(LiteralValue::Float64(f));
        }

        if let Some(s) = value.as_str() {
            return Ok(LiteralValue::String(s.to_string()));
        }

        if let Some(json) = value.as_json() {
            return Ok(LiteralValue::Json(json.to_string()));
        }

        Ok(LiteralValue::String(format!("{}", value)))
    }

    fn substitute_expr(
        &self,
        expr: &yachtsql_ir::expr::Expr,
        left_row: &[Value],
        left_schema: &Schema,
    ) -> Result<yachtsql_ir::expr::Expr> {
        use yachtsql_ir::expr::{BinaryOp, Expr, LiteralValue};

        match expr {
            Expr::Column { name, table } => {
                if let Some(tbl) = table {
                    let col_idx = left_schema.fields().iter().position(|f| {
                        f.source_table.as_deref() == Some(tbl.as_str()) && &f.name == name
                    });

                    if let Some(idx) = col_idx {
                        let value = &left_row[idx];
                        let literal = self.value_to_literal(value)?;
                        return Ok(Expr::Literal(literal));
                    }
                }

                Ok(expr.clone())
            }
            Expr::BinaryOp { left, op, right } => {
                let substituted_left = self.substitute_expr(left, left_row, left_schema)?;
                let substituted_right = self.substitute_expr(right, left_row, left_schema)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(substituted_left),
                    op: *op,
                    right: Box::new(substituted_right),
                })
            }
            Expr::UnaryOp { op, expr: inner } => {
                let substituted_inner = self.substitute_expr(inner, left_row, left_schema)?;
                Ok(Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(substituted_inner),
                })
            }
            Expr::Function { name, args } => {
                let substituted_args: Vec<_> = args
                    .iter()
                    .map(|arg| self.substitute_expr(arg, left_row, left_schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::Function {
                    name: name.clone(),
                    args: substituted_args,
                })
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                let substituted_operand = operand
                    .as_ref()
                    .map(|op| self.substitute_expr(op, left_row, left_schema))
                    .transpose()?
                    .map(Box::new);
                let substituted_when_then: Vec<_> = when_then
                    .iter()
                    .map(|(when, then)| {
                        Ok((
                            self.substitute_expr(when, left_row, left_schema)?,
                            self.substitute_expr(then, left_row, left_schema)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let substituted_else = else_expr
                    .as_ref()
                    .map(|e| self.substitute_expr(e, left_row, left_schema))
                    .transpose()?
                    .map(Box::new);
                Ok(Expr::Case {
                    operand: substituted_operand,
                    when_then: substituted_when_then,
                    else_expr: substituted_else,
                })
            }

            _ => Ok(expr.clone()),
        }
    }
}

impl ExecutionPlan for LateralJoinExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let left_batches: Vec<Table> = self
            .left
            .execute()?
            .into_iter()
            .map(|b| b.to_column_format())
            .collect::<Result<Vec<_>>>()?;

        if left_batches.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let left_schema = self.left.schema().clone();
        let mut result_rows: Vec<Vec<Value>> = Vec::new();
        let mut right_schema: Option<Schema> = None;

        for left_batch in &left_batches {
            let num_rows = left_batch.num_rows();

            for row_idx in 0..num_rows {
                let mut left_row = Vec::new();
                for col in left_batch.expect_columns() {
                    left_row.push(col.get(row_idx)?);
                }

                let (right_rows, subquery_schema) =
                    self.execute_correlated_subquery(&left_row, &left_schema)?;

                if right_schema.is_none() {
                    right_schema = Some(subquery_schema);
                }

                if right_rows.is_empty() {
                    match self.join_type {
                        JoinType::Left | JoinType::Full => {
                            let right_col_count =
                                right_schema.as_ref().map(|s| s.fields().len()).unwrap_or(0);
                            let mut combined_row = left_row.clone();
                            for _ in 0..right_col_count {
                                combined_row.push(Value::null());
                            }
                            result_rows.push(combined_row);
                        }
                        JoinType::Cross | JoinType::Inner => {}
                        _ => {}
                    }
                } else {
                    for right_row in right_rows {
                        let mut combined_row = left_row.clone();
                        combined_row.extend(right_row);
                        result_rows.push(combined_row);
                    }
                }
            }
        }

        let mut output_fields = left_schema.fields().to_vec();
        if let Some(ref rs) = right_schema {
            output_fields.extend(rs.fields().iter().cloned());
        }
        let output_schema = Schema::from_fields(output_fields);

        if result_rows.is_empty() {
            return Ok(vec![Table::empty(output_schema)]);
        }

        let num_output_rows = result_rows.len();
        let num_cols = output_schema.fields().len();
        let mut columns = Vec::new();

        for col_idx in 0..num_cols {
            let field = &output_schema.fields()[col_idx];
            let mut column = Column::new(&field.data_type, num_output_rows);

            for row in &result_rows {
                if col_idx < row.len() {
                    column.push(row[col_idx].clone())?;
                } else {
                    column.push(Value::null())?;
                }
            }

            columns.push(column);
        }

        Ok(vec![Table::new(output_schema, columns)?])
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.left.clone()]
    }

    fn describe(&self) -> String {
        format!("LateralJoin [{:?}]", self.join_type)
    }
}

fn serialize_key(key: &[Value]) -> Vec<u8> {
    let serialized = serde_json::to_string(key).unwrap_or_default();
    serialized.into_bytes()
}

#[derive(Debug)]
pub struct MergeJoinExec {
    left: Rc<dyn ExecutionPlan>,
    right: Rc<dyn ExecutionPlan>,
    schema: Schema,
    join_type: JoinType,
    on_conditions: Vec<(Expr, Expr)>,
}

impl MergeJoinExec {
    pub fn new(
        left: Rc<dyn ExecutionPlan>,
        right: Rc<dyn ExecutionPlan>,
        join_type: JoinType,
        on_conditions: Vec<(Expr, Expr)>,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        let mut fields = Vec::new();
        for field in left_schema.fields() {
            fields.push(field.clone());
        }

        if !matches!(join_type, JoinType::Semi | JoinType::Anti) {
            for field in right_schema.fields() {
                fields.push(field.clone());
            }
        }

        let schema = Schema::from_fields(fields);

        Ok(Self {
            left,
            right,
            schema,
            join_type,
            on_conditions,
        })
    }

    fn extract_join_key(&self, batch: &Table, row_idx: usize, is_left: bool) -> Result<Vec<Value>> {
        let mut key = Vec::new();
        let exprs: Vec<&Expr> = if is_left {
            self.on_conditions.iter().map(|(l, _)| l).collect()
        } else {
            self.on_conditions.iter().map(|(_, r)| r).collect()
        };

        for expr in exprs {
            let value = self.evaluate_expr(expr, batch, row_idx)?;
            key.push(value);
        }
        Ok(key)
    }

    fn evaluate_expr(&self, expr: &Expr, batch: &Table, row_idx: usize) -> Result<Value> {
        super::ProjectionWithExprExec::evaluate_expr(expr, batch, row_idx)
    }

    fn compare_keys(&self, left_key: &[Value], right_key: &[Value]) -> std::cmp::Ordering {
        for (l, r) in left_key.iter().zip(right_key.iter()) {
            let cmp = compare_join_values(l, r);
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    }

    fn extract_row(&self, batch: &Table, row_idx: usize) -> Result<Vec<Value>> {
        let mut row = Vec::new();
        for col in batch.expect_columns() {
            row.push(col.get(row_idx)?);
        }
        Ok(row)
    }

    fn build_result_batch(&self, result_rows: &[Vec<Value>]) -> Result<Vec<Table>> {
        if result_rows.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let num_output_rows = result_rows.len();
        let num_cols = self.schema.fields().len();
        let mut columns = Vec::new();

        for col_idx in 0..num_cols {
            let field = &self.schema.fields()[col_idx];
            let mut column = Column::new(&field.data_type, num_output_rows);

            for row in result_rows {
                column.push(row[col_idx].clone())?;
            }

            columns.push(column);
        }

        Ok(vec![Table::new(self.schema.clone(), columns)?])
    }
}

impl ExecutionPlan for MergeJoinExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let left_batches: Vec<Table> = self
            .left
            .execute()?
            .into_iter()
            .map(|b| b.to_column_format())
            .collect::<Result<Vec<_>>>()?;
        let right_batches: Vec<Table> = self
            .right
            .execute()?
            .into_iter()
            .map(|b| b.to_column_format())
            .collect::<Result<Vec<_>>>()?;

        let left_rows: Vec<(Vec<Value>, Vec<Value>)> = left_batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).filter_map(|idx| {
                    let key = self.extract_join_key(batch, idx, true).ok()?;
                    let row = self.extract_row(batch, idx).ok()?;
                    Some((key, row))
                })
            })
            .collect();

        let right_rows: Vec<(Vec<Value>, Vec<Value>)> = right_batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).filter_map(|idx| {
                    let key = self.extract_join_key(batch, idx, false).ok()?;
                    let row = self.extract_row(batch, idx).ok()?;
                    Some((key, row))
                })
            })
            .collect();

        if left_rows.is_empty() || right_rows.is_empty() {
            match self.join_type {
                JoinType::Inner | JoinType::Semi => {
                    return Ok(vec![Table::empty(self.schema.clone())]);
                }
                JoinType::Anti => {
                    if right_rows.is_empty() && !left_rows.is_empty() {
                        let result_rows: Vec<Vec<Value>> =
                            left_rows.into_iter().map(|(_, row)| row).collect();
                        return self.build_result_batch(&result_rows);
                    }
                    return Ok(vec![Table::empty(self.schema.clone())]);
                }
                JoinType::Left => {
                    if right_rows.is_empty() {
                        let right_null_cols = self.right.schema().fields().len();
                        let result_rows: Vec<Vec<Value>> = left_rows
                            .into_iter()
                            .map(|(_, mut row)| {
                                row.extend(vec![Value::null(); right_null_cols]);
                                row
                            })
                            .collect();
                        return self.build_result_batch(&result_rows);
                    }
                }
                JoinType::Right => {
                    if left_rows.is_empty() {
                        let left_null_cols = self.left.schema().fields().len();
                        let result_rows: Vec<Vec<Value>> = right_rows
                            .into_iter()
                            .map(|(_, row)| {
                                let mut combined = vec![Value::null(); left_null_cols];
                                combined.extend(row);
                                combined
                            })
                            .collect();
                        return self.build_result_batch(&result_rows);
                    }
                }
                JoinType::Full => {
                    let right_null_cols = self.right.schema().fields().len();
                    let left_null_cols = self.left.schema().fields().len();
                    let mut result_rows: Vec<Vec<Value>> = Vec::new();

                    for (_, mut row) in left_rows {
                        row.extend(vec![Value::null(); right_null_cols]);
                        result_rows.push(row);
                    }
                    for (_, row) in right_rows {
                        let mut combined = vec![Value::null(); left_null_cols];
                        combined.extend(row);
                        result_rows.push(combined);
                    }
                    return self.build_result_batch(&result_rows);
                }
                JoinType::Cross => {}
            }
        }

        let mut result_rows: Vec<Vec<Value>> = Vec::new();
        let mut left_idx = 0;
        let mut right_idx = 0;
        let mut left_matched = vec![false; left_rows.len()];
        let mut right_matched = vec![false; right_rows.len()];

        while left_idx < left_rows.len() && right_idx < right_rows.len() {
            let (left_key, left_row) = &left_rows[left_idx];
            let (right_key, right_row) = &right_rows[right_idx];

            match self.compare_keys(left_key, right_key) {
                std::cmp::Ordering::Less => {
                    left_idx += 1;
                }
                std::cmp::Ordering::Greater => {
                    right_idx += 1;
                }
                std::cmp::Ordering::Equal => {
                    let mut left_group_end = left_idx;
                    while left_group_end < left_rows.len()
                        && self.compare_keys(&left_rows[left_group_end].0, left_key)
                            == std::cmp::Ordering::Equal
                    {
                        left_group_end += 1;
                    }

                    let mut right_group_end = right_idx;
                    while right_group_end < right_rows.len()
                        && self.compare_keys(&right_rows[right_group_end].0, right_key)
                            == std::cmp::Ordering::Equal
                    {
                        right_group_end += 1;
                    }

                    for li in left_idx..left_group_end {
                        left_matched[li] = true;
                        for ri in right_idx..right_group_end {
                            right_matched[ri] = true;
                            match self.join_type {
                                JoinType::Semi => {
                                    result_rows.push(left_rows[li].1.clone());
                                    break;
                                }
                                JoinType::Anti => {}
                                _ => {
                                    let mut combined = left_rows[li].1.clone();
                                    combined.extend(right_rows[ri].1.clone());
                                    result_rows.push(combined);
                                }
                            }
                        }
                    }

                    left_idx = left_group_end;
                    right_idx = right_group_end;
                }
            }
        }

        match self.join_type {
            JoinType::Anti => {
                for (idx, matched) in left_matched.iter().enumerate() {
                    if !matched {
                        result_rows.push(left_rows[idx].1.clone());
                    }
                }
            }
            JoinType::Left | JoinType::Full => {
                let right_null_cols = self.right.schema().fields().len();
                for (idx, matched) in left_matched.iter().enumerate() {
                    if !matched {
                        let mut combined = left_rows[idx].1.clone();
                        combined.extend(vec![Value::null(); right_null_cols]);
                        result_rows.push(combined);
                    }
                }
            }
            _ => {}
        }

        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            let left_null_cols = self.left.schema().fields().len();
            for (idx, matched) in right_matched.iter().enumerate() {
                if !matched {
                    let mut combined = vec![Value::null(); left_null_cols];
                    combined.extend(right_rows[idx].1.clone());
                    result_rows.push(combined);
                }
            }
        }

        self.build_result_batch(&result_rows)
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn describe(&self) -> String {
        format!(
            "MergeJoin [{:?}, conditions: {}]",
            self.join_type,
            self.on_conditions.len()
        )
    }
}

fn compare_join_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    if a.is_null() && b.is_null() {
        return std::cmp::Ordering::Equal;
    }
    if a.is_null() {
        return std::cmp::Ordering::Less;
    }
    if b.is_null() {
        return std::cmp::Ordering::Greater;
    }

    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return x.cmp(&y);
    }
    if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
        return x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal);
    }
    if let Some(x) = a.as_i64() {
        if let Some(y) = b.as_f64() {
            return (x as f64)
                .partial_cmp(&y)
                .unwrap_or(std::cmp::Ordering::Equal);
        }
    }
    if let Some(x) = a.as_f64() {
        if let Some(y) = b.as_i64() {
            return x
                .partial_cmp(&(y as f64))
                .unwrap_or(std::cmp::Ordering::Equal);
        }
    }
    if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
        return x.cmp(y);
    }
    if let (Some(x), Some(y)) = (a.as_bool(), b.as_bool()) {
        return x.cmp(&y);
    }
    if let (Some(x), Some(y)) = (a.as_date(), b.as_date()) {
        return x.cmp(&y);
    }
    if let (Some(x), Some(y)) = (a.as_timestamp(), b.as_timestamp()) {
        return x.cmp(&y);
    }
    if let (Some(x_struct), Some(y_struct)) = (a.as_struct(), b.as_struct()) {
        for (x_val, y_val) in x_struct.values().zip(y_struct.values()) {
            let cmp = compare_join_values(x_val, y_val);
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        return x_struct.len().cmp(&y_struct.len());
    }
    std::cmp::Ordering::Equal
}

#[derive(Debug)]
pub struct IndexNestedLoopJoinExec {
    left: Rc<dyn ExecutionPlan>,
    right_table_name: String,
    right_index_name: String,
    right_schema: Schema,
    storage: Rc<RefCell<yachtsql_storage::Storage>>,
    schema: Schema,
    join_type: JoinType,
    left_key_expr: Expr,
    #[allow(dead_code)]
    right_key_column: String,
}

impl IndexNestedLoopJoinExec {
    pub fn new(
        left: Rc<dyn ExecutionPlan>,
        right_table_name: String,
        right_index_name: String,
        right_schema: Schema,
        storage: Rc<RefCell<yachtsql_storage::Storage>>,
        join_type: JoinType,
        left_key_expr: Expr,
        right_key_column: String,
    ) -> Result<Self> {
        let left_schema = left.schema();

        let mut fields = Vec::new();
        for field in left_schema.fields() {
            fields.push(field.clone());
        }

        if !matches!(join_type, JoinType::Semi | JoinType::Anti) {
            for field in right_schema.fields() {
                fields.push(field.clone());
            }
        }

        let schema = Schema::from_fields(fields);

        Ok(Self {
            left,
            right_table_name,
            right_index_name,
            right_schema,
            storage,
            schema,
            join_type,
            left_key_expr,
            right_key_column,
        })
    }

    fn evaluate_expr(&self, expr: &Expr, batch: &Table, row_idx: usize) -> Result<Value> {
        match expr {
            Expr::Column { name, table } => {
                let col_idx = if let Some(table_alias) = table {
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .position(|f| {
                            f.source_table.as_deref() == Some(table_alias.as_str())
                                && &f.name == name
                        })
                        .or_else(|| batch.schema().fields().iter().position(|f| &f.name == name))
                } else {
                    batch.schema().fields().iter().position(|f| &f.name == name)
                };

                let col_idx = col_idx.ok_or_else(|| Error::column_not_found(name.clone()))?;
                batch.expect_columns()[col_idx].get(row_idx)
            }
            _ => Err(Error::unsupported_feature(
                "Complex expressions in JOIN key not yet fully supported".to_string(),
            )),
        }
    }

    fn extract_row(&self, batch: &Table, row_idx: usize) -> Result<Vec<Value>> {
        let mut row = Vec::new();
        for col in batch.expect_columns() {
            row.push(col.get(row_idx)?);
        }
        Ok(row)
    }

    fn lookup_by_index(&self, key_value: &Value) -> Result<Vec<Vec<Value>>> {
        use yachtsql_storage::TableIndexOps;
        use yachtsql_storage::indexes::IndexKey;

        let storage = self.storage.borrow();

        let (dataset_name, table_id) = if let Some(dot_pos) = self.right_table_name.find('.') {
            let dataset = &self.right_table_name[..dot_pos];
            let table = &self.right_table_name[dot_pos + 1..];
            (dataset, table)
        } else {
            ("default", self.right_table_name.as_str())
        };

        let dataset = storage.get_dataset(dataset_name).ok_or_else(|| {
            Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_name))
        })?;

        let table = dataset
            .get_table(table_id)
            .ok_or_else(|| Error::TableNotFound(format!("Table '{}' not found", table_id)))?;

        let index_key = IndexKey::single(key_value.clone());
        let row_indices = table.index_lookup(&self.right_index_name, &index_key);

        let rows: Vec<Vec<Value>> = row_indices
            .into_iter()
            .filter_map(|row_idx| table.get_row(row_idx).ok().map(|r| r.into_values()))
            .collect();

        Ok(rows)
    }

    fn build_result_batch(&self, result_rows: &[Vec<Value>]) -> Result<Vec<Table>> {
        if result_rows.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let num_output_rows = result_rows.len();
        let num_cols = self.schema.fields().len();
        let mut columns = Vec::new();

        for col_idx in 0..num_cols {
            let field = &self.schema.fields()[col_idx];
            let mut column = Column::new(&field.data_type, num_output_rows);

            for row in result_rows {
                column.push(row[col_idx].clone())?;
            }

            columns.push(column);
        }

        Ok(vec![Table::new(self.schema.clone(), columns)?])
    }
}

impl ExecutionPlan for IndexNestedLoopJoinExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let left_batches: Vec<Table> = self
            .left
            .execute()?
            .into_iter()
            .map(|b| b.to_column_format())
            .collect::<Result<Vec<_>>>()?;

        let mut result_rows: Vec<Vec<Value>> = Vec::new();
        let right_null_cols = self.right_schema.fields().len();

        for left_batch in &left_batches {
            let num_left_rows = left_batch.num_rows();

            for left_row_idx in 0..num_left_rows {
                let left_key_value =
                    self.evaluate_expr(&self.left_key_expr, left_batch, left_row_idx)?;
                let left_row = self.extract_row(left_batch, left_row_idx)?;

                if left_key_value.is_null() {
                    match self.join_type {
                        JoinType::Left | JoinType::Full => {
                            let mut combined = left_row;
                            combined.extend(vec![Value::null(); right_null_cols]);
                            result_rows.push(combined);
                        }
                        JoinType::Anti => {
                            result_rows.push(left_row);
                        }
                        JoinType::Inner | JoinType::Right | JoinType::Cross | JoinType::Semi => {}
                    }
                    continue;
                }

                let right_matches = self.lookup_by_index(&left_key_value)?;

                if right_matches.is_empty() {
                    match self.join_type {
                        JoinType::Left | JoinType::Full => {
                            let mut combined = left_row;
                            combined.extend(vec![Value::null(); right_null_cols]);
                            result_rows.push(combined);
                        }
                        JoinType::Anti => {
                            result_rows.push(left_row);
                        }
                        JoinType::Inner | JoinType::Right | JoinType::Cross | JoinType::Semi => {}
                    }
                } else {
                    match self.join_type {
                        JoinType::Semi => {
                            result_rows.push(left_row);
                        }
                        JoinType::Anti => {}
                        JoinType::Inner
                        | JoinType::Left
                        | JoinType::Right
                        | JoinType::Full
                        | JoinType::Cross => {
                            for right_row in &right_matches {
                                let mut combined = left_row.clone();
                                combined.extend(right_row.clone());
                                result_rows.push(combined);
                            }
                        }
                    }
                }
            }
        }

        self.build_result_batch(&result_rows)
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.left.clone()]
    }

    fn describe(&self) -> String {
        format!(
            "IndexNestedLoopJoin [{:?}, table={}, index={}]",
            self.join_type, self.right_table_name, self.right_index_name
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_executor::evaluator::physical_plan::TableScanExec;

    #[test]
    fn test_serialize_key() {
        let key = vec![Value::int64(1), Value::string("test".to_string())];
        let bytes = serialize_key(&key);
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_serialize_key_consistency() {
        let key1 = vec![Value::int64(100), Value::string("abc".to_string())];
        let key2 = vec![Value::int64(100), Value::string("abc".to_string())];
        assert_eq!(serialize_key(&key1), serialize_key(&key2));
    }

    #[test]
    fn test_hash_join_creation() {
        let left_schema = yachtsql_storage::Schema::from_fields(vec![
            yachtsql_storage::Field::required(
                "id".to_string(),
                yachtsql_core::types::DataType::Int64,
            ),
            yachtsql_storage::Field::required(
                "name".to_string(),
                yachtsql_core::types::DataType::String,
            ),
        ]);

        let right_schema = yachtsql_storage::Schema::from_fields(vec![
            yachtsql_storage::Field::required(
                "id".to_string(),
                yachtsql_core::types::DataType::Int64,
            ),
            yachtsql_storage::Field::required(
                "value".to_string(),
                yachtsql_core::types::DataType::Int64,
            ),
        ]);

        let left_exec = Rc::new(TableScanExec::new(
            left_schema.clone(),
            "left_table".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let right_exec = Rc::new(TableScanExec::new(
            right_schema.clone(),
            "right_table".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let on_conditions = vec![(
            yachtsql_optimizer::expr::Expr::Column {
                name: "id".to_string(),
                table: None,
            },
            yachtsql_optimizer::expr::Expr::Column {
                name: "id".to_string(),
                table: None,
            },
        )];

        let hash_join = HashJoinExec::new(
            left_exec,
            right_exec,
            yachtsql_optimizer::plan::JoinType::Inner,
            on_conditions,
        );

        assert!(hash_join.is_ok());
        let join = hash_join.unwrap();
        assert_eq!(join.schema().fields().len(), 4);
    }

    #[test]
    fn test_nested_loop_join_creation() {
        let left_schema =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "a".to_string(),
                yachtsql_core::types::DataType::Int64,
            )]);

        let right_schema =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "b".to_string(),
                yachtsql_core::types::DataType::Int64,
            )]);

        let left_exec = Rc::new(TableScanExec::new(
            left_schema.clone(),
            "left".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let right_exec = Rc::new(TableScanExec::new(
            right_schema.clone(),
            "right".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let nested_loop = NestedLoopJoinExec::new(
            left_exec,
            right_exec,
            yachtsql_optimizer::plan::JoinType::Inner,
            None,
        );

        assert!(nested_loop.is_ok());
        let join = nested_loop.unwrap();
        assert_eq!(join.schema().fields().len(), 2);
    }

    #[test]
    fn test_join_types() {
        let schema =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "x".to_string(),
                yachtsql_core::types::DataType::Int64,
            )]);

        let left_exec = Rc::new(TableScanExec::new(
            schema.clone(),
            "t1".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let right_exec = Rc::new(TableScanExec::new(
            schema.clone(),
            "t2".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        assert!(
            NestedLoopJoinExec::new(
                left_exec.clone(),
                right_exec.clone(),
                yachtsql_optimizer::plan::JoinType::Inner,
                None
            )
            .is_ok()
        );

        assert!(
            NestedLoopJoinExec::new(
                left_exec.clone(),
                right_exec.clone(),
                yachtsql_optimizer::plan::JoinType::Left,
                None
            )
            .is_ok()
        );

        assert!(
            NestedLoopJoinExec::new(
                left_exec.clone(),
                right_exec.clone(),
                yachtsql_optimizer::plan::JoinType::Right,
                None
            )
            .is_ok()
        );

        assert!(
            NestedLoopJoinExec::new(
                left_exec,
                right_exec,
                yachtsql_optimizer::plan::JoinType::Full,
                None
            )
            .is_ok()
        );
    }
}
