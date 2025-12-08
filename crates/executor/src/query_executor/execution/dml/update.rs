use debug_print::debug_eprintln;
use indexmap::IndexMap;
use sqlparser::ast::{Assignment, Expr as SqlExpr, Statement, Value as SqlValue};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};
use yachtsql_storage::{Row, Schema, TableIndexOps, TableSchemaOps};

use super::super::super::QueryExecutor;
use super::super::super::expression_evaluator::ExpressionEvaluator;
use super::super::DdlExecutor;
use super::super::query::QueryExecutorTrait;
use crate::Table;
use crate::query_executor::returning::DmlRowContext;

pub trait DmlUpdateExecutor {
    fn execute_update(&mut self, stmt: &Statement, _original_sql: &str) -> Result<Table>;

    fn parse_update_assignments(
        &mut self,
        assignments: &[Assignment],
        schema: &Schema,
    ) -> Result<Vec<(String, Value)>>;

    fn apply_update_to_row(
        &mut self,
        row: &Row,
        assignments: &[(String, Value)],
        schema: &Schema,
    ) -> Result<IndexMap<String, Value>>;
}

impl DmlUpdateExecutor for QueryExecutor {
    fn execute_update(&mut self, stmt: &Statement, _original_sql: &str) -> Result<Table> {
        let Statement::Update {
            table,
            assignments,
            selection,
            returning,
            ..
        } = stmt
        else {
            return Err(Error::InternalError("Not an UPDATE statement".to_string()));
        };

        let table_name_str = table.to_string();
        let (dataset_id, table_id) = self.parse_ddl_table_name(&table_name_str)?;

        let is_view = {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
            })?;
            dataset.views().exists(&table_id)
        };

        let schema = if is_view {
            let view_sql = {
                let storage = self.storage.borrow_mut();
                let dataset = storage.get_dataset(&dataset_id).unwrap();
                dataset.views().get_view(&table_id).unwrap().sql.clone()
            };
            let view_result = self.execute_sql(&view_sql)?;
            view_result.schema().clone()
        } else {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
            })?;
            let table = dataset.get_table(&table_id).ok_or_else(|| {
                Error::TableNotFound(format!("Table '{}.{}' not found", dataset_id, table_id))
            })?;
            table.schema().clone()
        };

        let returning_spec = if let Some(ret) = returning {
            self.parse_returning_clause(ret, &schema)?
        } else {
            crate::query_executor::returning::ReturningSpec::None
        };

        let capture_returning = !matches!(
            returning_spec,
            crate::query_executor::returning::ReturningSpec::None
        );

        let parsed_assignments_with_exprs =
            self.parse_update_assignments_with_exprs(assignments, &schema)?;

        let all_rows = if is_view {
            let view_sql = {
                let storage = self.storage.borrow_mut();
                let dataset = storage.get_dataset(&dataset_id).unwrap();
                dataset.views().get_view(&table_id).unwrap().sql.clone()
            };
            let view_result = self.execute_sql(&view_sql)?;
            view_result.rows().map(|r| r.to_vec()).unwrap_or_default()
        } else {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
            })?;
            let table = dataset.get_table(&table_id).ok_or_else(|| {
                Error::TableNotFound(format!("Table '{}.{}' not found", dataset_id, table_id))
            })?;
            table.get_all_rows()
        };

        let mut updated_rows = Vec::with_capacity(all_rows.len());
        let mut trigger_updates: Vec<(Row, Row)> = Vec::new();
        let mut returning_contexts: Vec<DmlRowContext> = Vec::new();
        let mut changed_row_indices: Vec<usize> = Vec::new();

        if let Some(where_expr) = selection {
            let processed_where = self.preprocess_subqueries_in_update_expr(where_expr)?;
            let evaluator = ExpressionEvaluator::new(&schema);
            for (row_idx, row) in all_rows.iter().enumerate() {
                if evaluator
                    .evaluate_where(&processed_where, &row)
                    .unwrap_or(false)
                {
                    let updated_row_map = self.apply_update_to_row_with_exprs(
                        &row,
                        &parsed_assignments_with_exprs,
                        &schema,
                    )?;
                    let new_row = Row::from_named_values(updated_row_map.clone());

                    self.validate_view_check_option_update(
                        &dataset_id,
                        &table_id,
                        &new_row,
                        &schema,
                    )?;

                    trigger_updates.push((row.clone(), new_row.clone()));

                    changed_row_indices.push(row_idx);

                    if capture_returning {
                        returning_contexts.push(DmlRowContext::for_update(
                            row.values().to_vec(),
                            new_row.values().to_vec(),
                        ));
                    }

                    updated_rows.push(updated_row_map);
                } else {
                    let mut unchanged_map = IndexMap::new();
                    for (idx, field) in schema.fields().iter().enumerate() {
                        unchanged_map.insert(field.name.clone(), row.values()[idx].clone());
                    }
                    updated_rows.push(unchanged_map);
                }
            }
        } else {
            for (row_idx, row) in all_rows.iter().enumerate() {
                let updated_row_map = self.apply_update_to_row_with_exprs(
                    &row,
                    &parsed_assignments_with_exprs,
                    &schema,
                )?;
                let new_row = Row::from_named_values(updated_row_map.clone());

                self.validate_view_check_option_update(&dataset_id, &table_id, &new_row, &schema)?;

                trigger_updates.push((row.clone(), new_row.clone()));

                changed_row_indices.push(row_idx);

                if capture_returning {
                    returning_contexts.push(DmlRowContext::for_update(
                        row.values().to_vec(),
                        new_row.values().to_vec(),
                    ));
                }

                updated_rows.push(updated_row_map);
            }
        }

        let mut instead_of_count = 0;
        let mut instead_of_updates: Vec<(Row, Row)> = Vec::new();
        let mut normal_updates: Vec<(Row, Row)> = Vec::new();

        for (old_row, new_row) in trigger_updates {
            let instead_of_result = self.fire_instead_of_update_triggers(
                &dataset_id,
                &table_id,
                &old_row,
                &new_row,
                &schema,
            )?;

            if instead_of_result.triggers_fired > 0 {
                instead_of_count += 1;
                instead_of_updates.push((old_row, new_row));
            } else {
                normal_updates.push((old_row, new_row));
            }
        }

        if instead_of_count > 0 && normal_updates.is_empty() {
            debug_eprintln!(
                "[executor::dml::update] Updated {} rows via INSTEAD OF triggers",
                instead_of_count
            );
            return Self::empty_result();
        }

        let mut final_updates: Vec<(Row, Row)> = Vec::new();
        for (old_row, new_row) in normal_updates {
            let before_result = self.fire_before_update_triggers(
                &dataset_id,
                &table_id,
                &old_row,
                &new_row,
                &schema,
            )?;
            if !before_result.skip_operation {
                final_updates.push((old_row, new_row));
            }
        }

        let update_count = final_updates.len();

        {
            let mut storage = self.storage.borrow_mut();
            let enforcer = crate::query_executor::enforcement::ForeignKeyEnforcer::new();
            let table_full_name = format!("{}.{}", dataset_id, table_id);

            for (old_row, new_row) in &final_updates {
                enforcer.cascade_update(&table_full_name, old_row, new_row, &mut storage)?;
            }
        }

        self.replace_table_rows(
            &dataset_id,
            &table_id,
            &schema,
            updated_rows,
            &changed_row_indices,
        )?;

        for (old_row, new_row) in final_updates {
            self.fire_after_update_triggers(&dataset_id, &table_id, &old_row, &new_row, &schema)?;
        }

        debug_eprintln!(
            "[executor::dml::update] Updated {} rows (INSTEAD OF: {}, normal: {})",
            instead_of_count + update_count,
            instead_of_count,
            update_count
        );

        if capture_returning {
            crate::query_executor::returning::build_returning_batch_with_old_new(
                &returning_spec,
                &returning_contexts,
                &schema,
            )
        } else {
            Self::empty_result()
        }
    }

    fn parse_update_assignments(
        &mut self,
        assignments: &[Assignment],
        schema: &Schema,
    ) -> Result<Vec<(String, Value)>> {
        let mut parsed = Vec::with_capacity(assignments.len());

        for assignment in assignments {
            let col_name = match &assignment.target {
                sqlparser::ast::AssignmentTarget::ColumnName(ident) => ident.to_string(),
                sqlparser::ast::AssignmentTarget::Tuple(_) => {
                    return Err(Error::UnsupportedFeature(
                        "Tuple assignments not supported".to_string(),
                    ));
                }
            };

            let field = schema
                .field(&col_name)
                .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;

            if field.is_generated() {
                return Err(Error::InvalidOperation(format!(
                    "Cannot update generated column '{}'",
                    col_name
                )));
            }

            let value = self.evaluate_update_expression(&assignment.value)?;
            use yachtsql_core::types::coercion::CoercionRules;
            let coerced_value = CoercionRules::coerce_value(value, &field.data_type)?;

            parsed.push((col_name, coerced_value));
        }

        Ok(parsed)
    }

    fn apply_update_to_row(
        &mut self,
        row: &Row,
        assignments: &[(String, Value)],
        schema: &Schema,
    ) -> Result<IndexMap<String, Value>> {
        let mut updated_map = IndexMap::new();
        for (idx, field) in schema.fields().iter().enumerate() {
            updated_map.insert(field.name.clone(), row.values()[idx].clone());
        }

        for (col_name, new_value) in assignments {
            updated_map.insert(col_name.clone(), new_value.clone());
        }

        self.recompute_generated_columns(schema, &mut updated_map)?;

        Ok(updated_map)
    }
}

impl QueryExecutor {
    fn parse_update_assignments_with_exprs(
        &self,
        assignments: &[Assignment],
        schema: &Schema,
    ) -> Result<Vec<(String, SqlExpr, DataType, Option<String>)>> {
        let mut parsed = Vec::with_capacity(assignments.len());

        for assignment in assignments {
            let col_name_full = match &assignment.target {
                sqlparser::ast::AssignmentTarget::ColumnName(ident) => ident.to_string(),
                sqlparser::ast::AssignmentTarget::Tuple(_) => {
                    return Err(Error::UnsupportedFeature(
                        "Tuple assignments not supported".to_string(),
                    ));
                }
            };

            let (base_col_name, composite_field) = if col_name_full.contains('.') {
                let parts: Vec<&str> = col_name_full.splitn(2, '.').collect();
                (parts[0].to_string(), Some(parts[1].to_string()))
            } else {
                (col_name_full, None)
            };

            let field = schema
                .field(&base_col_name)
                .ok_or_else(|| Error::ColumnNotFound(base_col_name.clone()))?;

            if field.is_generated() {
                return Err(Error::InvalidOperation(format!(
                    "Cannot update generated column '{}'",
                    base_col_name
                )));
            }

            let target_type = if composite_field.is_some() {
                DataType::Int64
            } else {
                field.data_type.clone()
            };

            parsed.push((
                base_col_name,
                assignment.value.clone(),
                target_type,
                composite_field,
            ));
        }

        Ok(parsed)
    }

    fn apply_update_to_row_with_exprs(
        &mut self,
        row: &Row,
        assignments: &[(String, SqlExpr, DataType, Option<String>)],
        schema: &Schema,
    ) -> Result<IndexMap<String, Value>> {
        let mut updated_map = IndexMap::new();
        for (idx, field) in schema.fields().iter().enumerate() {
            updated_map.insert(field.name.clone(), row.values()[idx].clone());
        }

        let type_registry = {
            let storage = self.storage.borrow();
            storage.get_dataset("default").map(|d| d.types().clone())
        };

        let evaluator = if let Some(registry) = type_registry {
            ExpressionEvaluator::new(schema).with_owned_type_registry(registry)
        } else {
            ExpressionEvaluator::new(schema)
        };

        for (col_name, expr, target_type, composite_field) in assignments {
            let computed_value = evaluator.evaluate_expr(expr, row)?;

            if let Some(field_name) = composite_field {
                let current_value = updated_map
                    .get(col_name)
                    .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;

                let current_struct = current_value.as_struct().ok_or_else(|| {
                    Error::InvalidOperation(format!(
                        "Column '{}' is not a composite type, cannot update field '{}'",
                        col_name, field_name
                    ))
                })?;

                let mut new_struct = current_struct.clone();

                let found_key = new_struct
                    .keys()
                    .find(|k| k.eq_ignore_ascii_case(field_name))
                    .cloned();

                if let Some(key) = found_key {
                    new_struct.insert(key, computed_value);
                } else {
                    return Err(Error::InvalidQuery(format!(
                        "Field '{}' not found in composite type column '{}'",
                        field_name, col_name
                    )));
                }

                updated_map.insert(col_name.clone(), Value::struct_val(new_struct));
            } else {
                use yachtsql_core::types::coercion::CoercionRules;
                let coerced_value = CoercionRules::coerce_value(computed_value, target_type)?;
                updated_map.insert(col_name.clone(), coerced_value);
            }
        }

        self.recompute_generated_columns(schema, &mut updated_map)?;

        Ok(updated_map)
    }

    fn recompute_generated_columns(
        &mut self,
        schema: &Schema,
        row_map: &mut IndexMap<String, Value>,
    ) -> Result<()> {
        for field in schema.fields() {
            if let Some(gen_expr) = &field.generated_expression {
                let expr_sql = &gen_expr.expression_sql;

                let dummy_query = format!("SELECT {} FROM dummy", expr_sql);
                let parser = yachtsql_parser::Parser::with_dialect(self.dialect());
                let statements = parser.parse_sql(&dummy_query).map_err(|e| {
                    Error::InvalidOperation(format!(
                        "Failed to parse generated column expression '{}': {}",
                        expr_sql, e
                    ))
                })?;

                let expr = if let Some(yachtsql_parser::Statement::Standard(std_stmt)) =
                    statements.first()
                {
                    use sqlparser::ast::{SelectItem, SetExpr, Statement as SqlStatement};
                    if let SqlStatement::Query(query) = std_stmt.ast() {
                        if let SetExpr::Select(select) = query.body.as_ref() {
                            if let Some(SelectItem::UnnamedExpr(expr)) = select.projection.first() {
                                expr
                            } else {
                                return Err(Error::InvalidOperation(format!(
                                    "Invalid generated column expression: {}",
                                    expr_sql
                                )));
                            }
                        } else {
                            return Err(Error::InvalidOperation(format!(
                                "Invalid generated column expression: {}",
                                expr_sql
                            )));
                        }
                    } else {
                        return Err(Error::InvalidOperation(format!(
                            "Failed to parse generated column expression: {}",
                            expr_sql
                        )));
                    }
                } else {
                    return Err(Error::InvalidOperation(format!(
                        "Failed to parse generated column expression: {}",
                        expr_sql
                    )));
                };

                let temp_row = Row::from_named_values(row_map.clone());

                let evaluator = ExpressionEvaluator::new(schema);
                let computed_value = evaluator.evaluate_expr(expr, &temp_row)?;

                row_map.insert(field.name.clone(), computed_value);
            }
        }

        Ok(())
    }

    fn evaluate_update_expression(&mut self, expr: &SqlExpr) -> Result<Value> {
        use sqlparser::ast::{Value as SqlValue, ValueWithSpan as SqlValueWithSpan};

        match expr {
            SqlExpr::Value(SqlValueWithSpan {
                value: SqlValue::Number(n, _),
                ..
            }) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::int64(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Value::float64(f))
                } else {
                    Err(Error::InvalidQuery(format!("Invalid number: {}", n)))
                }
            }
            SqlExpr::Value(SqlValueWithSpan {
                value: SqlValue::SingleQuotedString(s),
                ..
            })
            | SqlExpr::Value(SqlValueWithSpan {
                value: SqlValue::DoubleQuotedString(s),
                ..
            }) => Ok(Value::string(s.clone())),
            SqlExpr::Value(SqlValueWithSpan {
                value: SqlValue::Boolean(b),
                ..
            }) => Ok(Value::bool_val(*b)),
            SqlExpr::Value(SqlValueWithSpan {
                value: SqlValue::Null,
                ..
            }) => Ok(Value::null()),
            _ => Err(Error::UnsupportedFeature(format!(
                "Complex expressions in UPDATE not yet supported: {}",
                expr
            ))),
        }
    }

    fn replace_table_rows(
        &mut self,
        dataset_id: &str,
        table_id: &str,
        schema: &Schema,
        rows: Vec<IndexMap<String, Value>>,
        changed_row_indices: &[usize],
    ) -> Result<()> {
        let table_full_name = format!("{}.{}", dataset_id, table_id);

        let mut tm = self.transaction_manager.borrow_mut();
        if let Some(_txn) = tm.get_active_transaction_mut() {
            drop(tm);

            let original_rows = {
                let storage = self.storage.borrow_mut();
                let dataset = storage.get_dataset(dataset_id).ok_or_else(|| {
                    Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
                })?;
                let table = dataset.get_table(table_id).ok_or_else(|| {
                    Error::TableNotFound(format!("Table '{}.{}' not found", dataset_id, table_id))
                })?;

                changed_row_indices
                    .iter()
                    .map(|&idx| table.get_row(idx))
                    .collect::<Result<Vec<_>>>()?
            };

            let mut tm = self.transaction_manager.borrow_mut();
            if let Some(txn) = tm.get_active_transaction_mut() {
                for (i, &row_idx) in changed_row_indices.iter().enumerate() {
                    let new_row_map = &rows[row_idx];
                    let new_row = Row::from_named_values(new_row_map.clone());
                    let original_row = &original_rows[i];

                    if original_row != &new_row {
                        txn.track_update(&table_full_name, row_idx, new_row);
                    }
                }
            }
        } else {
            drop(tm);

            let mut storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset_mut(dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
            })?;

            let table = dataset.get_table_mut(table_id).ok_or_else(|| {
                Error::TableNotFound(format!("Table '{}.{}' not found", dataset_id, table_id))
            })?;

            {
                let schema = table.schema_mut();
                if schema.check_evaluator().is_none() {
                    let evaluator = super::build_check_evaluator();
                    schema.set_check_evaluator(evaluator);
                }
            }

            let original_rows: Vec<Vec<yachtsql_core::types::Value>> = changed_row_indices
                .iter()
                .map(|&idx| table.get_row(idx).map(|row| row.values().to_vec()))
                .collect::<Result<Vec<_>>>()?;

            for (i, &row_idx) in changed_row_indices.iter().enumerate() {
                let new_row_map = &rows[row_idx];

                for (col_name, new_value) in new_row_map {
                    table.update_cell_at_index(row_idx, col_name, new_value.clone())?;
                }

                let new_row = table.get_row(row_idx)?;
                let new_row_values = new_row.values().to_vec();
                table.update_indexes_on_update(&original_rows[i], &new_row_values, row_idx)?;
            }
        }

        Ok(())
    }

    fn validate_view_check_option_update(
        &self,
        dataset_id: &str,
        table_id: &str,
        updated_row: &Row,
        schema: &Schema,
    ) -> Result<()> {
        use yachtsql_storage::WithCheckOption;

        let storage = self.storage.borrow_mut();
        let dataset = storage
            .get_dataset(dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        if !dataset.views().exists(table_id) {
            return Ok(());
        }

        let view = dataset.views().get_view(table_id).ok_or_else(|| {
            Error::InvalidQuery(format!("View '{}.{}' not found", dataset_id, table_id))
        })?;

        if view.with_check_option == WithCheckOption::None {
            return Ok(());
        }

        let where_clause_sql = match &view.where_clause {
            Some(clause) => clause,
            None => return Ok(()),
        };

        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let dialect = GenericDialect {};

        let test_query = format!("SELECT * FROM dummy WHERE {}", where_clause_sql);
        let parsed_query = Parser::parse_sql(&dialect, &test_query)
            .map_err(|e| Error::InternalError(format!("Failed to parse WHERE clause: {}", e)))?;

        let where_expr = if let Some(sqlparser::ast::Statement::Query(query)) = parsed_query.first()
        {
            use sqlparser::ast::SetExpr;
            if let SetExpr::Select(select) = query.body.as_ref() {
                select
                    .selection
                    .clone()
                    .ok_or_else(|| Error::InternalError("No WHERE clause found".to_string()))?
            } else {
                return Err(Error::InternalError("Invalid query structure".to_string()));
            }
        } else {
            return Err(Error::InternalError("Failed to parse query".to_string()));
        };

        use super::super::super::expression_evaluator::ExpressionEvaluator;
        let evaluator = ExpressionEvaluator::new(schema);
        let satisfies_where = evaluator
            .evaluate_where(&where_expr, updated_row)
            .unwrap_or(false);

        if !satisfies_where {
            return Err(Error::InvalidQuery(format!(
                "WITH CHECK OPTION violation: updated row does not satisfy view WHERE clause ({})",
                where_clause_sql
            )));
        }

        Ok(())
    }

    fn preprocess_subqueries_in_update_expr(&mut self, expr: &SqlExpr) -> Result<SqlExpr> {
        match expr {
            SqlExpr::Subquery(query) => {
                let sql = query.to_string();
                let result = self.execute_sql(&sql)?;

                if result.num_rows() == 0 {
                    return Ok(SqlExpr::Value(SqlValue::Null.into()));
                }

                if result.num_rows() > 1 {
                    return Err(Error::InvalidQuery(
                        "Scalar subquery returned more than one row".to_string(),
                    ));
                }

                if result.num_columns() != 1 {
                    return Err(Error::InvalidQuery(format!(
                        "Scalar subquery must return exactly one column, got {}",
                        result.num_columns()
                    )));
                }

                let column = result.column(0).ok_or_else(|| {
                    Error::InternalError("Subquery result has no column".to_string())
                })?;
                let value = column.get(0)?;
                let sql_value = Self::value_to_sql_value_for_update(&value)?;
                Ok(SqlExpr::Value(sql_value.into()))
            }

            SqlExpr::InSubquery {
                expr: left_expr,
                subquery,
                negated,
            } => {
                let sql = subquery.to_string();
                let result = self.execute_sql(&sql)?;

                if result.num_columns() != 1 {
                    return Err(Error::InvalidQuery(
                        "IN subquery must return exactly one column".to_string(),
                    ));
                }

                let column = result.column(0).ok_or_else(|| {
                    Error::InternalError("IN subquery result has no column".to_string())
                })?;

                let mut list_values = Vec::new();
                for i in 0..result.num_rows() {
                    let value = column.get(i)?;
                    let sql_value = Self::value_to_sql_value_for_update(&value)?;
                    list_values.push(SqlExpr::Value(sql_value.into()));
                }

                let processed_left = self.preprocess_subqueries_in_update_expr(left_expr)?;

                Ok(SqlExpr::InList {
                    expr: Box::new(processed_left),
                    list: list_values,
                    negated: *negated,
                })
            }

            SqlExpr::Exists { subquery, negated } => {
                let sql = subquery.to_string();
                let result = self.execute_sql(&sql)?;

                let exists = result.num_rows() > 0;
                let bool_result = if *negated { !exists } else { exists };

                Ok(SqlExpr::Value(SqlValue::Boolean(bool_result).into()))
            }

            SqlExpr::BinaryOp { left, op, right } => {
                let processed_left = self.preprocess_subqueries_in_update_expr(left)?;
                let processed_right = self.preprocess_subqueries_in_update_expr(right)?;
                Ok(SqlExpr::BinaryOp {
                    left: Box::new(processed_left),
                    op: op.clone(),
                    right: Box::new(processed_right),
                })
            }

            SqlExpr::UnaryOp { op, expr: inner } => {
                let processed = self.preprocess_subqueries_in_update_expr(inner)?;
                Ok(SqlExpr::UnaryOp {
                    op: op.clone(),
                    expr: Box::new(processed),
                })
            }

            SqlExpr::Nested(inner) => {
                let processed = self.preprocess_subqueries_in_update_expr(inner)?;
                Ok(SqlExpr::Nested(Box::new(processed)))
            }

            _ => Ok(expr.clone()),
        }
    }

    fn value_to_sql_value_for_update(value: &Value) -> Result<SqlValue> {
        if value.is_null() {
            return Ok(SqlValue::Null);
        }

        if let Some(b) = value.as_bool() {
            return Ok(SqlValue::Boolean(b));
        }

        if let Some(i) = value.as_i64() {
            return Ok(SqlValue::Number(i.to_string(), false));
        }

        if let Some(f) = value.as_f64() {
            return Ok(SqlValue::Number(f.to_string(), false));
        }

        if let Some(s) = value.as_str() {
            return Ok(SqlValue::SingleQuotedString(s.to_string()));
        }

        Ok(SqlValue::SingleQuotedString(format!("{:?}", value)))
    }
}
