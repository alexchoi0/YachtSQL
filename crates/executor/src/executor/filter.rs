use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{BinaryOp, Expr, Literal, LogicalPlan, SortExpr, UnnestColumn};
use yachtsql_optimizer::optimize;
use yachtsql_storage::{Record, Schema, Table};

use super::PlanExecutor;
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_filter(&mut self, input: &PhysicalPlan, predicate: &Expr) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();

        if Self::expr_contains_subquery(predicate) {
            self.execute_filter_with_subquery(&input_table, predicate)
        } else {
            let evaluator = IrEvaluator::new(&schema)
                .with_variables(&self.variables)
                .with_system_variables(self.session.system_variables())
                .with_user_functions(&self.user_function_defs);
            let mut result = Table::empty(schema.clone());

            for record in input_table.rows()? {
                let val = evaluator.evaluate(predicate, &record)?;
                if val.as_bool().unwrap_or(false) {
                    result.push_row(record.values().to_vec())?;
                }
            }

            Ok(result)
        }
    }

    fn execute_filter_with_subquery(&mut self, input: &Table, predicate: &Expr) -> Result<Table> {
        let schema = input.schema().clone();
        let mut result = Table::empty(schema.clone());

        let outer_col_indices = Self::collect_outer_column_indices_from_expr(predicate, &schema);
        let mut subquery_cache: std::collections::HashMap<Vec<Value>, Value> =
            std::collections::HashMap::new();

        for record in input.rows()? {
            let cache_key: Vec<Value> = outer_col_indices
                .iter()
                .map(|&idx| record.values().get(idx).cloned().unwrap_or(Value::Null))
                .collect();

            let val = if let Some(cached) = subquery_cache.get(&cache_key) {
                cached.clone()
            } else {
                let computed = self.eval_expr_with_subquery(predicate, &schema, &record)?;
                subquery_cache.insert(cache_key, computed.clone());
                computed
            };

            if val.as_bool().unwrap_or(false) {
                result.push_row(record.values().to_vec())?;
            }
        }

        Ok(result)
    }

    fn collect_outer_column_indices_from_expr(expr: &Expr, schema: &Schema) -> Vec<usize> {
        let mut indices = Vec::new();
        Self::collect_column_indices_recursive(expr, schema, &mut indices);
        indices.sort_unstable();
        indices.dedup();
        indices
    }

    fn collect_column_indices_recursive(expr: &Expr, schema: &Schema, indices: &mut Vec<usize>) {
        match expr {
            Expr::Column { name, index, .. } => {
                if let Some(idx) = index {
                    indices.push(*idx);
                } else if let Some(idx) = schema.field_index(name) {
                    indices.push(idx);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_column_indices_recursive(left, schema, indices);
                Self::collect_column_indices_recursive(right, schema, indices);
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_column_indices_recursive(expr, schema, indices);
            }
            Expr::ScalarFunction { args, .. } => {
                for arg in args {
                    Self::collect_column_indices_recursive(arg, schema, indices);
                }
            }
            Expr::Exists { subquery, .. } => {
                Self::collect_column_indices_from_plan(subquery, schema, indices);
            }
            Expr::InSubquery { expr, subquery, .. } => {
                Self::collect_column_indices_recursive(expr, schema, indices);
                Self::collect_column_indices_from_plan(subquery, schema, indices);
            }
            Expr::Subquery(plan) | Expr::ScalarSubquery(plan) | Expr::ArraySubquery(plan) => {
                Self::collect_column_indices_from_plan(plan, schema, indices);
            }
            _ => {}
        }
    }

    fn collect_column_indices_from_plan(
        plan: &LogicalPlan,
        outer_schema: &Schema,
        indices: &mut Vec<usize>,
    ) {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                Self::collect_outer_refs_from_expr(predicate, outer_schema, indices);
                Self::collect_column_indices_from_plan(input, outer_schema, indices);
            }
            LogicalPlan::Project {
                input, expressions, ..
            } => {
                for expr in expressions {
                    Self::collect_outer_refs_from_expr(expr, outer_schema, indices);
                }
                Self::collect_column_indices_from_plan(input, outer_schema, indices);
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                ..
            } => {
                for expr in group_by {
                    Self::collect_outer_refs_from_expr(expr, outer_schema, indices);
                }
                for expr in aggregates {
                    Self::collect_outer_refs_from_expr(expr, outer_schema, indices);
                }
                Self::collect_column_indices_from_plan(input, outer_schema, indices);
            }
            LogicalPlan::Join {
                left,
                right,
                condition,
                ..
            } => {
                if let Some(cond) = condition {
                    Self::collect_outer_refs_from_expr(cond, outer_schema, indices);
                }
                Self::collect_column_indices_from_plan(left, outer_schema, indices);
                Self::collect_column_indices_from_plan(right, outer_schema, indices);
            }
            LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input, .. } => {
                Self::collect_column_indices_from_plan(input, outer_schema, indices);
            }
            _ => {}
        }
    }

    fn collect_outer_refs_from_expr(expr: &Expr, outer_schema: &Schema, indices: &mut Vec<usize>) {
        match expr {
            Expr::Column { name, .. } => {
                if let Some(idx) = outer_schema.field_index(name) {
                    indices.push(idx);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_outer_refs_from_expr(left, outer_schema, indices);
                Self::collect_outer_refs_from_expr(right, outer_schema, indices);
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_outer_refs_from_expr(expr, outer_schema, indices);
            }
            Expr::ScalarFunction { args, .. } => {
                for arg in args {
                    Self::collect_outer_refs_from_expr(arg, outer_schema, indices);
                }
            }
            Expr::Aggregate { args, filter, .. } => {
                for arg in args {
                    Self::collect_outer_refs_from_expr(arg, outer_schema, indices);
                }
                if let Some(f) = filter {
                    Self::collect_outer_refs_from_expr(f, outer_schema, indices);
                }
            }
            Expr::Cast { expr, .. } => {
                Self::collect_outer_refs_from_expr(expr, outer_schema, indices);
            }
            Expr::IsNull { expr, .. } => {
                Self::collect_outer_refs_from_expr(expr, outer_schema, indices);
            }
            _ => {}
        }
    }

    pub fn eval_expr_with_subquery(
        &mut self,
        expr: &Expr,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        match expr {
            Expr::Exists { subquery, negated } => {
                let has_rows = self.eval_exists(subquery, outer_schema, outer_record)?;
                Ok(Value::Bool(if *negated { !has_rows } else { has_rows }))
            }
            Expr::InSubquery {
                expr: value_expr,
                subquery,
                negated,
            } => {
                let value = self.eval_expr_with_subquery(value_expr, outer_schema, outer_record)?;
                let in_list = self.eval_in_subquery(subquery, outer_schema, outer_record)?;
                let is_in = in_list.contains(&value);
                Ok(Value::Bool(if *negated { !is_in } else { is_in }))
            }
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOp::And => {
                    let left_val =
                        self.eval_expr_with_subquery(left, outer_schema, outer_record)?;
                    if !left_val.as_bool().unwrap_or(false) {
                        return Ok(Value::Bool(false));
                    }
                    let right_val =
                        self.eval_expr_with_subquery(right, outer_schema, outer_record)?;
                    Ok(Value::Bool(right_val.as_bool().unwrap_or(false)))
                }
                BinaryOp::Or => {
                    let left_val =
                        self.eval_expr_with_subquery(left, outer_schema, outer_record)?;
                    if left_val.as_bool().unwrap_or(false) {
                        return Ok(Value::Bool(true));
                    }
                    let right_val =
                        self.eval_expr_with_subquery(right, outer_schema, outer_record)?;
                    Ok(Value::Bool(right_val.as_bool().unwrap_or(false)))
                }
                _ => {
                    let left_val =
                        self.eval_expr_with_subquery(left, outer_schema, outer_record)?;
                    let right_val =
                        self.eval_expr_with_subquery(right, outer_schema, outer_record)?;
                    match op {
                        BinaryOp::Eq => Ok(Value::Bool(Self::values_equal(&left_val, &right_val))),
                        BinaryOp::NotEq => {
                            Ok(Value::Bool(!Self::values_equal(&left_val, &right_val)))
                        }
                        BinaryOp::Lt => Ok(Value::Bool(
                            Self::compare_values(&left_val, &right_val) == std::cmp::Ordering::Less,
                        )),
                        BinaryOp::LtEq => Ok(Value::Bool(matches!(
                            Self::compare_values(&left_val, &right_val),
                            std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                        ))),
                        BinaryOp::Gt => Ok(Value::Bool(
                            Self::compare_values(&left_val, &right_val)
                                == std::cmp::Ordering::Greater,
                        )),
                        BinaryOp::GtEq => Ok(Value::Bool(matches!(
                            Self::compare_values(&left_val, &right_val),
                            std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                        ))),
                        BinaryOp::Add => Self::arithmetic_op(&left_val, &right_val, |a, b| a + b),
                        BinaryOp::Sub => Self::arithmetic_op(&left_val, &right_val, |a, b| a - b),
                        BinaryOp::Mul => Self::arithmetic_op(&left_val, &right_val, |a, b| a * b),
                        BinaryOp::Div => Self::arithmetic_op(&left_val, &right_val, |a, b| a / b),
                        _ => {
                            let new_left = Self::value_to_literal(left_val);
                            let new_right = Self::value_to_literal(right_val);
                            let simplified_expr = Expr::BinaryOp {
                                left: Box::new(Expr::Literal(new_left)),
                                op: *op,
                                right: Box::new(Expr::Literal(new_right)),
                            };
                            let evaluator = IrEvaluator::new(outer_schema)
                                .with_variables(&self.variables)
                                .with_system_variables(self.session.system_variables())
                                .with_user_functions(&self.user_function_defs);
                            evaluator.evaluate(&simplified_expr, outer_record)
                        }
                    }
                }
            },
            Expr::UnaryOp {
                op: yachtsql_ir::UnaryOp::Not,
                expr: inner,
            } => {
                let val = self.eval_expr_with_subquery(inner, outer_schema, outer_record)?;
                Ok(Value::Bool(!val.as_bool().unwrap_or(false)))
            }
            Expr::Subquery(subquery) | Expr::ScalarSubquery(subquery) => {
                if Self::plan_contains_outer_refs(subquery, outer_schema) {
                    self.evaluate_scalar_subquery_with_outer(subquery, outer_schema, outer_record)
                } else {
                    self.evaluate_scalar_subquery(subquery)
                }
            }
            Expr::ArraySubquery(subquery) => {
                self.evaluate_array_subquery(subquery, outer_schema, outer_record)
            }
            Expr::ScalarFunction { name, args } => {
                let arg_vals: Vec<Value> = args
                    .iter()
                    .map(|a| self.eval_expr_with_subquery(a, outer_schema, outer_record))
                    .collect::<Result<_>>()?;
                let evaluator = IrEvaluator::new(outer_schema)
                    .with_variables(&self.variables)
                    .with_system_variables(self.session.system_variables())
                    .with_user_functions(&self.user_function_defs);
                evaluator.eval_scalar_function_with_values(name, &arg_vals)
            }
            _ => {
                let evaluator = IrEvaluator::new(outer_schema)
                    .with_variables(&self.variables)
                    .with_system_variables(self.session.system_variables())
                    .with_user_functions(&self.user_function_defs);
                evaluator.evaluate(expr, outer_record)
            }
        }
    }

    fn eval_exists(
        &mut self,
        subquery: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<bool> {
        let mut inner_tables = std::collections::HashSet::new();
        Self::collect_plan_tables(subquery, &mut inner_tables);
        let substituted = self.substitute_outer_refs_in_plan_with_inner_tables(
            subquery,
            outer_schema,
            outer_record,
            &inner_tables,
        )?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;
        Ok(!result_table.is_empty())
    }

    fn eval_in_subquery(
        &mut self,
        subquery: &LogicalPlan,
        _outer_schema: &Schema,
        _outer_record: &Record,
    ) -> Result<Vec<Value>> {
        let physical = optimize(subquery)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

        let mut values = Vec::new();
        for record in result_table.rows()? {
            let row_values = record.values();
            if !row_values.is_empty() {
                values.push(row_values[0].clone());
            }
        }
        Ok(values)
    }

    fn evaluate_scalar_subquery(&mut self, subquery: &LogicalPlan) -> Result<Value> {
        let physical = optimize(subquery)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

        for record in result_table.rows()? {
            let row_values = record.values();
            if !row_values.is_empty() {
                return Ok(row_values[0].clone());
            }
        }
        Ok(Value::Null)
    }

    fn evaluate_scalar_subquery_with_outer(
        &mut self,
        subquery: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        let mut inner_tables = std::collections::HashSet::new();
        Self::collect_plan_tables(subquery, &mut inner_tables);
        let substituted = self.substitute_outer_refs_in_plan_with_inner_tables(
            subquery,
            outer_schema,
            outer_record,
            &inner_tables,
        )?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

        for record in result_table.rows()? {
            let row_values = record.values();
            if !row_values.is_empty() {
                return Ok(row_values[0].clone());
            }
        }
        Ok(Value::Null)
    }

    fn evaluate_array_subquery(
        &mut self,
        subquery: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        let mut inner_tables = std::collections::HashSet::new();
        Self::collect_plan_tables(subquery, &mut inner_tables);
        let substituted = self.substitute_outer_refs_in_plan_with_inner_tables(
            subquery,
            outer_schema,
            outer_record,
            &inner_tables,
        )?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

        let result_schema = result_table.schema();
        let num_fields = result_schema.field_count();

        let mut array_values = Vec::new();
        for record in result_table.rows()? {
            let values = record.values();
            if num_fields == 1 {
                array_values.push(values[0].clone());
            } else {
                let fields: Vec<(String, Value)> = result_schema
                    .fields()
                    .iter()
                    .zip(values.iter())
                    .map(|(f, v)| (f.name.clone(), v.clone()))
                    .collect();
                array_values.push(Value::Struct(fields));
            }
        }

        Ok(Value::Array(array_values))
    }

    pub fn substitute_outer_refs_in_plan(
        &self,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<LogicalPlan> {
        let mut inner_tables = std::collections::HashSet::new();
        Self::collect_plan_tables(plan, &mut inner_tables);
        self.substitute_outer_refs_in_plan_with_inner_tables(
            plan,
            outer_schema,
            outer_record,
            &inner_tables,
        )
    }

    fn substitute_outer_refs_in_expr(
        &self,
        expr: &Expr,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Expr> {
        match expr {
            Expr::Column { table, name, index } => {
                let should_substitute = if let Some(tbl) = table {
                    outer_schema.fields().iter().any(|f| {
                        f.source_table
                            .as_ref()
                            .is_some_and(|src| src.eq_ignore_ascii_case(tbl))
                    }) || outer_schema
                        .fields()
                        .iter()
                        .any(|f| f.name.eq_ignore_ascii_case(name) && f.source_table.is_none())
                } else {
                    outer_schema.field_index(name).is_some()
                };

                if should_substitute && let Some(idx) = outer_schema.field_index(name) {
                    let value = outer_record
                        .values()
                        .get(idx)
                        .cloned()
                        .unwrap_or(Value::Null);
                    return Ok(Expr::Literal(Self::value_to_literal(value)));
                }
                Ok(Expr::Column {
                    table: table.clone(),
                    name: name.clone(),
                    index: *index,
                })
            }
            Expr::BinaryOp { left, op, right } => {
                let new_left =
                    self.substitute_outer_refs_in_expr(left, outer_schema, outer_record)?;
                let new_right =
                    self.substitute_outer_refs_in_expr(right, outer_schema, outer_record)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(new_left),
                    op: *op,
                    right: Box::new(new_right),
                })
            }
            Expr::IsNull {
                expr: inner,
                negated,
            } => {
                let new_inner =
                    self.substitute_outer_refs_in_expr(inner, outer_schema, outer_record)?;
                Ok(Expr::IsNull {
                    expr: Box::new(new_inner),
                    negated: *negated,
                })
            }
            Expr::ScalarFunction { name, args } => {
                let new_args = args
                    .iter()
                    .map(|a| self.substitute_outer_refs_in_expr(a, outer_schema, outer_record))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::ScalarFunction {
                    name: name.clone(),
                    args: new_args,
                })
            }
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => {
                let new_inner =
                    self.substitute_outer_refs_in_expr(inner, outer_schema, outer_record)?;
                Ok(Expr::Cast {
                    expr: Box::new(new_inner),
                    data_type: data_type.clone(),
                    safe: *safe,
                })
            }
            _ => Ok(expr.clone()),
        }
    }

    fn substitute_outer_refs_in_plan_with_inner_tables(
        &self,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
        inner_tables: &std::collections::HashSet<String>,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Scan {
                table_name,
                schema,
                projection,
            } => Ok(LogicalPlan::Scan {
                table_name: table_name.clone(),
                schema: schema.clone(),
                projection: projection.clone(),
            }),
            LogicalPlan::Filter { input, predicate } => {
                let new_input = self.substitute_outer_refs_in_plan_with_inner_tables(
                    input,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_predicate = self.substitute_outer_refs_in_expr_with_inner_tables(
                    predicate,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                Ok(LogicalPlan::Filter {
                    input: Box::new(new_input),
                    predicate: new_predicate,
                })
            }
            LogicalPlan::Project {
                input,
                expressions,
                schema,
            } => {
                let new_input = self.substitute_outer_refs_in_plan_with_inner_tables(
                    input,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_expressions = expressions
                    .iter()
                    .map(|e| {
                        self.substitute_outer_refs_in_expr_with_inner_tables(
                            e,
                            outer_schema,
                            outer_record,
                            inner_tables,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::Project {
                    input: Box::new(new_input),
                    expressions: new_expressions,
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Unnest {
                input,
                columns,
                schema,
            } => {
                let new_input = self.substitute_outer_refs_in_plan_with_inner_tables(
                    input,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_columns = columns
                    .iter()
                    .map(|c| {
                        let new_expr = self.substitute_outer_refs_in_expr_with_inner_tables(
                            &c.expr,
                            outer_schema,
                            outer_record,
                            inner_tables,
                        )?;
                        Ok(UnnestColumn {
                            expr: new_expr,
                            alias: c.alias.clone(),
                            with_offset: c.with_offset,
                            offset_alias: c.offset_alias.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::Unnest {
                    input: Box::new(new_input),
                    columns: new_columns,
                    schema: schema.clone(),
                })
            }
            LogicalPlan::SetOperation {
                left,
                right,
                op,
                all,
                schema,
            } => {
                let new_left = self.substitute_outer_refs_in_plan_with_inner_tables(
                    left,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_right = self.substitute_outer_refs_in_plan_with_inner_tables(
                    right,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                Ok(LogicalPlan::SetOperation {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    op: *op,
                    all: *all,
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                schema,
            } => {
                let new_left = self.substitute_outer_refs_in_plan_with_inner_tables(
                    left,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_right = self.substitute_outer_refs_in_plan_with_inner_tables(
                    right,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_condition = condition
                    .as_ref()
                    .map(|c| {
                        self.substitute_outer_refs_in_expr_with_inner_tables(
                            c,
                            outer_schema,
                            outer_record,
                            inner_tables,
                        )
                    })
                    .transpose()?;
                Ok(LogicalPlan::Join {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    join_type: *join_type,
                    condition: new_condition,
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let new_input = self.substitute_outer_refs_in_plan_with_inner_tables(
                    input,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                Ok(LogicalPlan::Limit {
                    input: Box::new(new_input),
                    limit: *limit,
                    offset: *offset,
                })
            }
            LogicalPlan::Sort { input, sort_exprs } => {
                let new_input = self.substitute_outer_refs_in_plan_with_inner_tables(
                    input,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_sort_exprs = sort_exprs
                    .iter()
                    .map(|se| {
                        let new_expr = self.substitute_outer_refs_in_expr_with_inner_tables(
                            &se.expr,
                            outer_schema,
                            outer_record,
                            inner_tables,
                        )?;
                        Ok(SortExpr {
                            expr: new_expr,
                            asc: se.asc,
                            nulls_first: se.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::Sort {
                    input: Box::new(new_input),
                    sort_exprs: new_sort_exprs,
                })
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
            } => {
                let new_input = self.substitute_outer_refs_in_plan_with_inner_tables(
                    input,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_group_by = group_by
                    .iter()
                    .map(|e| {
                        self.substitute_outer_refs_in_expr_with_inner_tables(
                            e,
                            outer_schema,
                            outer_record,
                            inner_tables,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let new_aggregates = aggregates
                    .iter()
                    .map(|e| {
                        self.substitute_outer_refs_in_expr_with_inner_tables(
                            e,
                            outer_schema,
                            outer_record,
                            inner_tables,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::Aggregate {
                    input: Box::new(new_input),
                    group_by: new_group_by,
                    aggregates: new_aggregates,
                    schema: schema.clone(),
                    grouping_sets: grouping_sets.clone(),
                })
            }
            other => Ok(other.clone()),
        }
    }

    fn substitute_outer_refs_in_expr_with_inner_tables(
        &self,
        expr: &Expr,
        outer_schema: &Schema,
        outer_record: &Record,
        inner_tables: &std::collections::HashSet<String>,
    ) -> Result<Expr> {
        match expr {
            Expr::Column { table, name, index } => {
                let should_substitute = if let Some(tbl) = table {
                    if inner_tables.contains(&tbl.to_lowercase()) {
                        false
                    } else {
                        outer_schema.fields().iter().any(|f| {
                            f.source_table
                                .as_ref()
                                .is_some_and(|src| src.eq_ignore_ascii_case(tbl))
                        }) || outer_schema
                            .fields()
                            .iter()
                            .any(|f| f.name.eq_ignore_ascii_case(name) && f.source_table.is_none())
                    }
                } else {
                    let inner_table_is_only_alias = inner_tables
                        .iter()
                        .all(|t| self.catalog.get_table(t).is_none());
                    inner_table_is_only_alias
                        && !inner_tables.contains(&name.to_lowercase())
                        && outer_schema.field_index(name).is_some()
                };

                if should_substitute && let Some(idx) = outer_schema.field_index(name) {
                    let value = outer_record
                        .values()
                        .get(idx)
                        .cloned()
                        .unwrap_or(Value::Null);
                    return Ok(Expr::Literal(Self::value_to_literal(value)));
                }
                Ok(Expr::Column {
                    table: table.clone(),
                    name: name.clone(),
                    index: *index,
                })
            }
            Expr::BinaryOp { left, op, right } => {
                let new_left = self.substitute_outer_refs_in_expr_with_inner_tables(
                    left,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_right = self.substitute_outer_refs_in_expr_with_inner_tables(
                    right,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                Ok(Expr::BinaryOp {
                    left: Box::new(new_left),
                    op: *op,
                    right: Box::new(new_right),
                })
            }
            Expr::ScalarFunction { name, args } => {
                let new_args = args
                    .iter()
                    .map(|a| {
                        self.substitute_outer_refs_in_expr_with_inner_tables(
                            a,
                            outer_schema,
                            outer_record,
                            inner_tables,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::ScalarFunction {
                    name: name.clone(),
                    args: new_args,
                })
            }
            Expr::Struct { fields } => {
                let new_fields = fields
                    .iter()
                    .map(|(name, e)| {
                        let new_e = self.substitute_outer_refs_in_expr_with_inner_tables(
                            e,
                            outer_schema,
                            outer_record,
                            inner_tables,
                        )?;
                        Ok((name.clone(), new_e))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::Struct { fields: new_fields })
            }
            Expr::StructAccess { expr, field } => {
                let new_expr = self.substitute_outer_refs_in_expr_with_inner_tables(
                    expr,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                Ok(Expr::StructAccess {
                    expr: Box::new(new_expr),
                    field: field.clone(),
                })
            }
            Expr::IsNull { expr, negated } => {
                let new_expr = self.substitute_outer_refs_in_expr_with_inner_tables(
                    expr,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                Ok(Expr::IsNull {
                    expr: Box::new(new_expr),
                    negated: *negated,
                })
            }
            Expr::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
            } => {
                let new_expr = self.substitute_outer_refs_in_expr_with_inner_tables(
                    expr,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_pattern = self.substitute_outer_refs_in_expr_with_inner_tables(
                    pattern,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                Ok(Expr::Like {
                    expr: Box::new(new_expr),
                    pattern: Box::new(new_pattern),
                    negated: *negated,
                    case_insensitive: *case_insensitive,
                })
            }
            Expr::UnaryOp { op, expr } => {
                let new_expr = self.substitute_outer_refs_in_expr_with_inner_tables(
                    expr,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                Ok(Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(new_expr),
                })
            }
            _ => Ok(expr.clone()),
        }
    }

    pub fn expr_contains_subquery(expr: &Expr) -> bool {
        match expr {
            Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::Subquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::ArraySubquery(_) => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_contains_subquery(left) || Self::expr_contains_subquery(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::ScalarFunction { args, .. } => args.iter().any(Self::expr_contains_subquery),
            _ => false,
        }
    }

    fn plan_contains_outer_refs(plan: &LogicalPlan, outer_schema: &Schema) -> bool {
        let mut inner_tables = std::collections::HashSet::new();
        Self::collect_plan_tables(plan, &mut inner_tables);
        Self::plan_has_outer_refs(plan, outer_schema, &inner_tables)
    }

    fn collect_plan_tables(plan: &LogicalPlan, tables: &mut std::collections::HashSet<String>) {
        match plan {
            LogicalPlan::Scan {
                table_name, schema, ..
            } => {
                tables.insert(table_name.to_lowercase());
                for field in &schema.fields {
                    if let Some(ref tbl) = field.table {
                        tables.insert(tbl.to_lowercase());
                    }
                }
            }
            LogicalPlan::Filter { input, .. } => Self::collect_plan_tables(input, tables),
            LogicalPlan::Project { input, schema, .. } => {
                Self::collect_plan_tables(input, tables);
                for field in &schema.fields {
                    if let Some(ref tbl) = field.table {
                        tables.insert(tbl.to_lowercase());
                    }
                }
            }
            LogicalPlan::Aggregate { input, schema, .. } => {
                Self::collect_plan_tables(input, tables);
                for field in &schema.fields {
                    if let Some(ref tbl) = field.table {
                        tables.insert(tbl.to_lowercase());
                    }
                }
            }
            LogicalPlan::Join {
                left,
                right,
                schema,
                ..
            } => {
                Self::collect_plan_tables(left, tables);
                Self::collect_plan_tables(right, tables);
                for field in &schema.fields {
                    if let Some(ref tbl) = field.table {
                        tables.insert(tbl.to_lowercase());
                    }
                }
            }
            LogicalPlan::Unnest {
                input,
                columns,
                schema,
            } => {
                Self::collect_plan_tables(input, tables);
                for col in columns {
                    if let Some(alias) = &col.alias {
                        tables.insert(alias.to_lowercase());
                    }
                }
                for field in &schema.fields {
                    if let Some(ref tbl) = field.table {
                        tables.insert(tbl.to_lowercase());
                    }
                }
            }
            LogicalPlan::Sort { input, .. } => Self::collect_plan_tables(input, tables),
            LogicalPlan::Limit { input, .. } => Self::collect_plan_tables(input, tables),
            LogicalPlan::Distinct { input, .. } => Self::collect_plan_tables(input, tables),
            _ => {}
        }
    }

    fn plan_has_outer_refs(
        plan: &LogicalPlan,
        outer_schema: &Schema,
        inner_tables: &std::collections::HashSet<String>,
    ) -> bool {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                Self::expr_has_outer_refs(predicate, outer_schema, inner_tables)
                    || Self::plan_has_outer_refs(input, outer_schema, inner_tables)
            }
            LogicalPlan::Project {
                input, expressions, ..
            } => {
                expressions
                    .iter()
                    .any(|e| Self::expr_has_outer_refs(e, outer_schema, inner_tables))
                    || Self::plan_has_outer_refs(input, outer_schema, inner_tables)
            }
            LogicalPlan::Join {
                left,
                right,
                condition,
                ..
            } => {
                condition
                    .as_ref()
                    .is_some_and(|c| Self::expr_has_outer_refs(c, outer_schema, inner_tables))
                    || Self::plan_has_outer_refs(left, outer_schema, inner_tables)
                    || Self::plan_has_outer_refs(right, outer_schema, inner_tables)
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                ..
            } => {
                group_by
                    .iter()
                    .any(|e| Self::expr_has_outer_refs(e, outer_schema, inner_tables))
                    || aggregates
                        .iter()
                        .any(|e| Self::expr_has_outer_refs(e, outer_schema, inner_tables))
                    || Self::plan_has_outer_refs(input, outer_schema, inner_tables)
            }
            _ => false,
        }
    }

    fn expr_has_outer_refs(
        expr: &Expr,
        outer_schema: &Schema,
        inner_tables: &std::collections::HashSet<String>,
    ) -> bool {
        match expr {
            Expr::Column {
                table: Some(tbl),
                name,
                ..
            } => {
                if inner_tables.contains(&tbl.to_lowercase()) {
                    return false;
                }
                outer_schema.fields().iter().any(|f| {
                    f.source_table
                        .as_ref()
                        .is_some_and(|src| src.eq_ignore_ascii_case(tbl))
                        || f.name.eq_ignore_ascii_case(name) && f.source_table.is_none()
                })
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_has_outer_refs(left, outer_schema, inner_tables)
                    || Self::expr_has_outer_refs(right, outer_schema, inner_tables)
            }
            Expr::UnaryOp { expr, .. } => {
                Self::expr_has_outer_refs(expr, outer_schema, inner_tables)
            }
            Expr::ScalarFunction { args, .. } => args
                .iter()
                .any(|a| Self::expr_has_outer_refs(a, outer_schema, inner_tables)),
            Expr::Aggregate { args, filter, .. } => {
                args.iter()
                    .any(|a| Self::expr_has_outer_refs(a, outer_schema, inner_tables))
                    || filter
                        .as_ref()
                        .is_some_and(|f| Self::expr_has_outer_refs(f, outer_schema, inner_tables))
            }
            _ => false,
        }
    }

    fn value_to_literal(value: Value) -> Literal {
        use chrono::{NaiveDate, Timelike};
        const UNIX_EPOCH_DATE: NaiveDate = match NaiveDate::from_ymd_opt(1970, 1, 1) {
            Some(d) => d,
            None => panic!("Invalid date"),
        };
        match value {
            Value::Null => Literal::Null,
            Value::Bool(b) => Literal::Bool(b),
            Value::Int64(n) => Literal::Int64(n),
            Value::Float64(f) => Literal::Float64(f),
            Value::Numeric(d) => Literal::Numeric(d),
            Value::BigNumeric(d) => Literal::BigNumeric(d),
            Value::String(s) => Literal::String(s),
            Value::Bytes(b) => Literal::Bytes(b),
            Value::Date(d) => {
                let days = d.signed_duration_since(UNIX_EPOCH_DATE).num_days() as i32;
                Literal::Date(days)
            }
            Value::Time(t) => {
                let nanos =
                    t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64;
                Literal::Time(nanos)
            }
            Value::Timestamp(ts) => {
                let micros = ts.timestamp_micros();
                Literal::Timestamp(micros)
            }
            Value::DateTime(dt) => {
                let micros = dt.and_utc().timestamp_micros();
                Literal::Datetime(micros)
            }
            Value::Interval(iv) => Literal::Interval {
                months: iv.months,
                days: iv.days,
                nanos: iv.nanos,
            },
            Value::Array(arr) => {
                Literal::Array(arr.into_iter().map(Self::value_to_literal).collect())
            }
            Value::Struct(fields) => Literal::Struct(
                fields
                    .into_iter()
                    .map(|(k, v)| (k, Self::value_to_literal(v)))
                    .collect(),
            ),
            Value::Json(j) => Literal::Json(j),
            Value::Geography(_) => Literal::Null,
            Value::Range(_) => Literal::Null,
            Value::Default => Literal::Null,
        }
    }

    fn arithmetic_op<F>(left: &Value, right: &Value, op: F) -> Result<Value>
    where
        F: Fn(f64, f64) -> f64,
    {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(op(*a as f64, *b as f64) as i64)),
            (Value::Float64(a), Value::Float64(b)) => {
                Ok(Value::Float64(ordered_float::OrderedFloat(op(a.0, b.0))))
            }
            (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(
                ordered_float::OrderedFloat(op(*a as f64, b.0)),
            )),
            (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(
                ordered_float::OrderedFloat(op(a.0, *b as f64)),
            )),
            _ => Err(Error::InvalidQuery(format!(
                "Cannot perform arithmetic on {:?} and {:?}",
                left, right
            ))),
        }
    }

    fn values_equal(left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Null, Value::Null) => true,
            (Value::Null, _) | (_, Value::Null) => false,
            (Value::Int64(a), Value::Float64(b)) => (*a as f64) == b.0,
            (Value::Float64(a), Value::Int64(b)) => a.0 == (*b as f64),
            _ => left == right,
        }
    }

    fn compare_values(left: &Value, right: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (left, right) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Greater,
            (_, Value::Null) => Ordering::Less,
            (Value::Int64(a), Value::Float64(b)) => {
                let a_f64 = *a as f64;
                a_f64.partial_cmp(&b.0).unwrap_or(Ordering::Equal)
            }
            (Value::Float64(a), Value::Int64(b)) => {
                let b_f64 = *b as f64;
                a.0.partial_cmp(&b_f64).unwrap_or(Ordering::Equal)
            }
            _ => left.cmp(right),
        }
    }
}
