use debug_print::debug_eprintln;
use indexmap::IndexMap;
use yachtsql_capability::feature_ids::F311_SCHEMA_DEFINITION;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};
use yachtsql_storage::{Row, Schema, TableSchemaOps};

use super::super::super::QueryExecutor;
use super::super::DdlExecutor;
use super::super::query::QueryExecutorTrait;
use crate::Table;
use crate::query_executor::returning::DmlRowContext;

pub trait DmlInsertExecutor {
    fn execute_insert(
        &mut self,
        stmt: &sqlparser::ast::Statement,
        _original_sql: &str,
    ) -> Result<Table>;

    fn resolve_insert_columns(
        &self,
        columns: &[sqlparser::ast::Ident],
        schema: &Schema,
    ) -> Result<Vec<String>>;

    fn parse_insert_values(
        &mut self,
        schema: &Schema,
        column_names: &[String],
        rows: &[Vec<sqlparser::ast::Expr>],
    ) -> Result<Vec<IndexMap<String, Value>>>;

    fn evaluate_insert_expression(&mut self, expr: &sqlparser::ast::Expr) -> Result<Value>;
}

impl DmlInsertExecutor for QueryExecutor {
    fn execute_insert(
        &mut self,
        stmt: &sqlparser::ast::Statement,
        _original_sql: &str,
    ) -> Result<Table> {
        use sqlparser::ast::{SetExpr, Statement};

        let Statement::Insert(insert) = stmt else {
            return Err(Error::InternalError("Not an INSERT statement".to_string()));
        };

        let on_conflict = insert.on.as_ref();

        let table_name_str = resolve_insert_table_name(insert)?;
        let (dataset_id, table_id) = self.parse_ddl_table_name(&table_name_str)?;

        let (dataset_id, table_id) = {
            let storage = self.storage.borrow();
            let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
            })?;
            if let Some(table) = dataset.get_table(&table_id) {
                match table.engine() {
                    yachtsql_storage::TableEngine::Distributed {
                        database,
                        table: target_table,
                        ..
                    } => {
                        let target_db = if database.is_empty() {
                            dataset_id.clone()
                        } else {
                            database.clone()
                        };
                        (target_db, target_table.clone())
                    }
                    yachtsql_storage::TableEngine::Buffer {
                        database,
                        table: target_table,
                    } => {
                        let target_db = if database.is_empty() {
                            dataset_id.clone()
                        } else {
                            database.clone()
                        };
                        (target_db, target_table.clone())
                    }
                    _ => (dataset_id.clone(), table_id.clone()),
                }
            } else {
                (dataset_id.clone(), table_id.clone())
            }
        };

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
            self.get_table_schema(&dataset_id, &table_id)?
        };

        let column_names = self.resolve_insert_columns(&insert.columns, &schema)?;

        let returning_spec = if let Some(ref returning) = insert.returning {
            self.parse_returning_clause(returning, &schema)?
        } else {
            crate::query_executor::returning::ReturningSpec::None
        };

        let capture_returning = !matches!(
            returning_spec,
            crate::query_executor::returning::ReturningSpec::None
        );

        let (row_count, inserted_rows) = if let Some(query) = insert.source.as_ref() {
            match query.body.as_ref() {
                SetExpr::Values(values) => {
                    let rows = self.parse_insert_values(&schema, &column_names, &values.rows)?;

                    let rows =
                        self.compute_generated_columns_for_insert(&schema, &column_names, rows)?;

                    let row_objects: Vec<Row> = rows
                        .iter()
                        .map(|row_map| {
                            let values: Vec<Value> = schema
                                .fields()
                                .iter()
                                .map(|field| {
                                    row_map.get(&field.name).cloned().unwrap_or(Value::null())
                                })
                                .collect();
                            Row::from_values(values)
                        })
                        .collect();

                    let count = if on_conflict.is_some() {
                        self.insert_rows_batch_with_conflict(
                            &dataset_id,
                            &table_id,
                            &schema,
                            rows,
                            on_conflict,
                        )?
                    } else {
                        self.insert_rows_batch(&dataset_id, &table_id, &schema, rows)?
                    };

                    if capture_returning {
                        (count, row_objects)
                    } else {
                        (count, vec![])
                    }
                }
                SetExpr::Select(_) | SetExpr::Query(_) | SetExpr::SetOperation { .. } => {
                    let select_stmt = Statement::Query(query.clone());
                    let select_result = self.execute_select(&select_stmt, "")?;

                    let select_schema = select_result.schema();
                    if select_schema.fields().len() != column_names.len() {
                        return Err(Error::InvalidQuery(format!(
                            "INSERT ... SELECT column count mismatch: target has {} columns, SELECT has {} columns",
                            column_names.len(),
                            select_schema.fields().len()
                        )));
                    }

                    let select_rows = select_result.rows().map(|r| r.to_vec()).unwrap_or_default();

                    let mut rows_to_insert = Vec::new();
                    for row_values in &select_rows {
                        let mut row_map = IndexMap::new();

                        for (idx, col_name) in column_names.iter().enumerate() {
                            row_map.insert(col_name.clone(), row_values[idx].clone());
                        }

                        rows_to_insert.push(row_map);
                    }

                    let rows_to_insert = self.compute_generated_columns_for_insert(
                        &schema,
                        &column_names,
                        rows_to_insert,
                    )?;

                    let row_objects: Vec<Row> = rows_to_insert
                        .iter()
                        .map(|row_map| {
                            let values: Vec<Value> = schema
                                .fields()
                                .iter()
                                .map(|field| {
                                    row_map.get(&field.name).cloned().unwrap_or(Value::null())
                                })
                                .collect();
                            Row::from_values(values)
                        })
                        .collect();

                    let count = if on_conflict.is_some() {
                        self.insert_rows_batch_with_conflict(
                            &dataset_id,
                            &table_id,
                            &schema,
                            rows_to_insert,
                            on_conflict,
                        )?
                    } else {
                        self.insert_rows_batch(&dataset_id, &table_id, &schema, rows_to_insert)?
                    };

                    if capture_returning {
                        (count, row_objects)
                    } else {
                        (count, vec![])
                    }
                }
                _ => {
                    return Err(Error::UnsupportedFeature(
                        "Unsupported SetExpr variant in INSERT statement".to_string(),
                    ));
                }
            }
        } else {
            let mut row_map = IndexMap::new();

            for field in schema.fields() {
                row_map.insert(field.name.clone(), Value::default_value());
            }

            let rows =
                self.compute_generated_columns_for_insert(&schema, &column_names, vec![row_map])?;

            let row_objects: Vec<Row> = rows
                .iter()
                .map(|row_map| {
                    let values: Vec<Value> = schema
                        .fields()
                        .iter()
                        .map(|field| row_map.get(&field.name).cloned().unwrap_or(Value::null()))
                        .collect();
                    Row::from_values(values)
                })
                .collect();

            let count = if on_conflict.is_some() {
                self.insert_rows_batch_with_conflict(
                    &dataset_id,
                    &table_id,
                    &schema,
                    rows,
                    on_conflict,
                )?
            } else {
                self.insert_rows_batch(&dataset_id, &table_id, &schema, rows)?
            };

            if capture_returning {
                (count, row_objects)
            } else {
                (count, vec![])
            }
        };

        if capture_returning {
            let returning_contexts: Vec<DmlRowContext> = inserted_rows
                .iter()
                .map(|row| DmlRowContext::for_insert(row.values().to_vec()))
                .collect();

            crate::query_executor::returning::build_returning_batch_with_old_new(
                &returning_spec,
                &returning_contexts,
                &schema,
            )
        } else {
            Self::empty_result()
        }
    }

    fn resolve_insert_columns(
        &self,
        columns: &[sqlparser::ast::Ident],
        schema: &Schema,
    ) -> Result<Vec<String>> {
        if !columns.is_empty() {
            let column_names: Vec<String> = columns.iter().map(|c| c.value.clone()).collect();

            for col_name in &column_names {
                if schema.field(col_name).is_none() {
                    return Err(Error::ColumnNotFound(col_name.clone()));
                }
            }

            return Ok(column_names);
        }

        Ok(schema.fields().iter().map(|f| f.name.clone()).collect())
    }

    fn parse_insert_values(
        &mut self,
        schema: &Schema,
        column_names: &[String],
        rows: &[Vec<sqlparser::ast::Expr>],
    ) -> Result<Vec<IndexMap<String, Value>>> {
        let mut row_data = Vec::new();

        for row_exprs in rows {
            if row_exprs.len() != column_names.len() {
                return Err(Error::InvalidQuery(format!(
                    "column count mismatch: expected {} values, got {}",
                    column_names.len(),
                    row_exprs.len()
                )));
            }

            let mut row_map = IndexMap::new();
            for (idx, expr) in row_exprs.iter().enumerate() {
                let column_name = &column_names[idx];

                let mut value = self.evaluate_insert_expression(expr)?;

                if let Some(field) = schema.field(column_name) {
                    if !value.is_default() {
                        value = self.coerce_value_to_data_type(value, &field.data_type)?;
                    }
                }

                row_map.insert(column_name.clone(), value);
            }

            row_data.push(row_map);
        }

        Ok(row_data)
    }

    fn evaluate_insert_expression(&mut self, expr: &sqlparser::ast::Expr) -> Result<Value> {
        use sqlparser::ast::{
            Expr as SqlExpr, Value as SqlValue, ValueWithSpan as SqlValueWithSpan,
        };

        if Self::contains_subquery(expr) {
            self.require_feature(F311_SCHEMA_DEFINITION, "scalar subquery in VALUES")?;
        }

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
            }) => {
                if let Some(hex_str) = s.strip_prefix("\\x") {
                    Self::parse_hex_literal(hex_str)
                } else {
                    Ok(Value::string(s.clone()))
                }
            }
            SqlExpr::Value(SqlValueWithSpan {
                value: SqlValue::SingleQuotedByteStringLiteral(s),
                ..
            })
            | SqlExpr::Value(SqlValueWithSpan {
                value: SqlValue::DoubleQuotedByteStringLiteral(s),
                ..
            })
            | SqlExpr::Value(SqlValueWithSpan {
                value: SqlValue::TripleSingleQuotedByteStringLiteral(s),
                ..
            })
            | SqlExpr::Value(SqlValueWithSpan {
                value: SqlValue::TripleDoubleQuotedByteStringLiteral(s),
                ..
            }) => {
                if let Some(hex_str) = s.strip_prefix("\\x") {
                    Self::parse_hex_literal(hex_str)
                } else {
                    Ok(Value::bytes(s.as_bytes().to_vec()))
                }
            }
            SqlExpr::Value(SqlValueWithSpan {
                value: SqlValue::HexStringLiteral(s),
                ..
            }) => Self::parse_hex_literal(s),
            SqlExpr::Value(SqlValueWithSpan {
                value: SqlValue::Boolean(b),
                ..
            }) => Ok(Value::bool_val(*b)),
            SqlExpr::Value(SqlValueWithSpan {
                value: SqlValue::Null,
                ..
            }) => Ok(Value::null()),
            SqlExpr::Identifier(ident) if ident.value.eq_ignore_ascii_case("DEFAULT") => {
                Ok(Value::default_value())
            }
            SqlExpr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                if matches!(
                    func_name.as_str(),
                    "NEXTVAL" | "CURRVAL" | "SETVAL" | "LASTVAL"
                ) {
                    return self.evaluate_sequence_function(&func_name, func);
                }
                self.evaluate_constant_expression(expr)
            }
            _ => self.evaluate_constant_expression(expr),
        }
    }
}

fn resolve_insert_table_name(insert: &sqlparser::ast::Insert) -> Result<String> {
    match &insert.table {
        sqlparser::ast::TableObject::TableName(name) => Ok(name.to_string()),
        other => Err(Error::unsupported_feature(format!(
            "INSERT target '{other}' is not supported"
        ))),
    }
}

impl QueryExecutor {
    fn compute_generated_columns_for_insert(
        &mut self,
        schema: &Schema,
        user_provided_columns: &[String],
        mut rows: Vec<IndexMap<String, Value>>,
    ) -> Result<Vec<IndexMap<String, Value>>> {
        for col_name in user_provided_columns {
            if let Some(field) = schema.field(col_name)
                && field.is_generated()
            {
                return Err(Error::InvalidOperation(format!(
                    "Cannot insert into generated column '{}'",
                    col_name
                )));
            }
        }

        for row_map in &rows {
            for col_name in user_provided_columns {
                if let Some(field) = schema.field(col_name)
                    && field.is_identity_always()
                {
                    if let Some(value) = row_map.get(col_name) {
                        if !value.is_null() && !value.is_default() {
                            return Err(Error::InvalidOperation(format!(
                                "Cannot insert explicit value into IDENTITY column '{}' with GENERATED ALWAYS",
                                col_name
                            )));
                        }
                    }
                }
            }
        }

        for row_map in &mut rows {
            for field in schema.fields() {
                if field.is_identity() {
                    let has_non_null_value = row_map
                        .get(&field.name)
                        .map(|v| !v.is_null())
                        .unwrap_or(false);

                    let is_default = row_map
                        .get(&field.name)
                        .map(|v| v.is_default())
                        .unwrap_or(false);

                    if has_non_null_value && !is_default {
                        continue;
                    }

                    if let Some(seq_name) = &field.identity_sequence_name {
                        let (dataset_id, seq_id) =
                            if let Some((schema, table)) = seq_name.split_once('.') {
                                (schema.to_string(), table.to_string())
                            } else {
                                ("default".to_string(), seq_name.to_string())
                            };

                        let next_value = {
                            let mut storage = self.storage.borrow_mut();
                            let dataset =
                                storage.get_dataset_mut(&dataset_id).ok_or_else(|| {
                                    Error::invalid_query(format!(
                                        "Sequence '{}' does not exist",
                                        seq_name
                                    ))
                                })?;

                            dataset.sequences_mut().nextval(&seq_id)?
                        };

                        row_map.insert(field.name.clone(), Value::int64(next_value));
                    } else {
                        return Err(Error::InternalError(format!(
                            "IDENTITY column '{}' has no associated sequence",
                            field.name
                        )));
                    }
                }
            }
        }

        for row_map in &mut rows {
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
                                if let Some(SelectItem::UnnamedExpr(expr)) =
                                    select.projection.first()
                                {
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

                    use crate::query_executor::expression_evaluator::ExpressionEvaluator;
                    let evaluator = ExpressionEvaluator::new(schema);
                    let computed_value = evaluator.evaluate_expr(expr, &temp_row)?;

                    row_map.insert(field.name.clone(), computed_value);
                }
            }
        }

        Ok(rows)
    }

    pub(crate) fn evaluate_sequence_function(
        &mut self,
        func_name: &str,
        func: &sqlparser::ast::Function,
    ) -> Result<Value> {
        use sqlparser::ast::{FunctionArg, FunctionArguments};

        let extract_seq_name = |arg: &FunctionArg| -> Result<String> {
            match arg {
                FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                    sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                        value: sqlparser::ast::Value::SingleQuotedString(s),
                        ..
                    })
                    | sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                        value: sqlparser::ast::Value::DoubleQuotedString(s),
                        ..
                    }),
                )) => Ok(s.clone()),
                _ => Err(Error::InvalidOperation(format!(
                    "{} expects a string literal sequence name",
                    func_name
                ))),
            }
        };

        let parse_table_name = |name: &str| -> (String, String) {
            if let Some((schema, table)) = name.split_once('.') {
                (schema.to_string(), table.to_string())
            } else {
                ("default".to_string(), name.to_string())
            }
        };

        let args = match &func.args {
            FunctionArguments::List(list) => &list.args,
            _ => {
                return Err(Error::InvalidOperation(format!(
                    "{} requires argument list",
                    func_name
                )));
            }
        };

        match func_name {
            "NEXTVAL" => {
                if args.len() != 1 {
                    return Err(Error::InvalidOperation(format!(
                        "NEXTVAL expects exactly 1 argument, got {}",
                        args.len()
                    )));
                }

                let seq_name = extract_seq_name(&args[0])?;
                let (dataset_id, seq_id) = parse_table_name(&seq_name);

                let mut storage = self.storage.borrow_mut();
                let dataset = storage.get_dataset_mut(&dataset_id).ok_or_else(|| {
                    Error::invalid_query(format!("Sequence '{}' does not exist", seq_name))
                })?;

                let next_value = dataset.sequences_mut().nextval(&seq_id)?;
                Ok(Value::int64(next_value))
            }

            "CURRVAL" => {
                if args.len() != 1 {
                    return Err(Error::InvalidOperation(format!(
                        "CURRVAL expects exactly 1 argument, got {}",
                        args.len()
                    )));
                }

                let seq_name = extract_seq_name(&args[0])?;
                let (dataset_id, seq_id) = parse_table_name(&seq_name);

                let storage = self.storage.borrow_mut();
                let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                    Error::invalid_query(format!("Sequence '{}' does not exist", seq_name))
                })?;

                let curr_value = dataset.sequences().currval(&seq_id)?;
                Ok(Value::int64(curr_value))
            }

            "SETVAL" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidOperation(format!(
                        "SETVAL expects 2 or 3 arguments, got {}",
                        args.len()
                    )));
                }

                let seq_name = extract_seq_name(&args[0])?;
                let (dataset_id, seq_id) = parse_table_name(&seq_name);

                let value = match &args[1] {
                    FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                        sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                            value: sqlparser::ast::Value::Number(n, _),
                            ..
                        }),
                    )) => n.parse::<i64>().map_err(|_| {
                        Error::InvalidOperation("SETVAL value must be an integer".to_string())
                    })?,
                    _ => {
                        return Err(Error::InvalidOperation(
                            "SETVAL value must be a number".to_string(),
                        ));
                    }
                };

                let is_called = if args.len() == 3 {
                    match &args[2] {
                        FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                            sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                                value: sqlparser::ast::Value::Boolean(b),
                                ..
                            }),
                        )) => *b,
                        _ => {
                            return Err(Error::InvalidOperation(
                                "SETVAL is_called must be a boolean".to_string(),
                            ));
                        }
                    }
                } else {
                    true
                };

                let mut storage = self.storage.borrow_mut();
                let dataset = storage.get_dataset_mut(&dataset_id).ok_or_else(|| {
                    Error::invalid_query(format!("Sequence '{}' does not exist", seq_name))
                })?;

                let result_value = dataset.sequences_mut().setval(&seq_id, value, is_called)?;
                Ok(Value::int64(result_value))
            }

            "LASTVAL" => {
                if !args.is_empty() {
                    return Err(Error::InvalidOperation(format!(
                        "LASTVAL expects no arguments, got {}",
                        args.len()
                    )));
                }

                let storage = self.storage.borrow_mut();
                let dataset = storage.get_dataset("default").ok_or_else(|| {
                    Error::InvalidOperation(
                        "LASTVAL: no sequences have been accessed in this session".to_string(),
                    )
                })?;

                let last_value = dataset.sequences().lastval()?;
                Ok(Value::int64(last_value))
            }

            _ => Err(Error::UnsupportedFeature(format!(
                "Sequence function {} not supported",
                func_name
            ))),
        }
    }

    fn evaluate_constant_expression(&mut self, expr: &sqlparser::ast::Expr) -> Result<Value> {
        let sql = format!("SELECT {}", expr);
        let result = self.execute_sql(&sql)?;

        if result.num_rows() == 0 {
            return Err(Error::InternalError(
                "Constant expression returned no rows".to_string(),
            ));
        }

        let column = result.column(0).ok_or_else(|| {
            Error::InternalError("Constant expression returned no columns".to_string())
        })?;

        column.get(0)
    }

    fn get_table_schema(&self, dataset_id: &str, table_id: &str) -> Result<Schema> {
        let storage = self.storage.borrow_mut();
        let dataset = storage
            .get_dataset(dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;
        let table = dataset.get_table(table_id).ok_or_else(|| {
            Error::TableNotFound(format!("Table '{}.{}' not found", dataset_id, table_id))
        })?;
        let mut schema = table.schema().clone();
        if schema.check_evaluator().is_none() {
            let evaluator = super::build_check_evaluator();
            schema.set_check_evaluator(evaluator);
        }
        Ok(schema)
    }

    fn coerce_value_to_data_type(&self, value: Value, target_type: &DataType) -> Result<Value> {
        use yachtsql_core::types::coercion::CoercionRules;
        CoercionRules::coerce_value(value, target_type)
    }

    fn insert_rows_batch(
        &mut self,
        dataset_id: &str,
        table_id: &str,
        schema: &Schema,
        rows: Vec<IndexMap<String, Value>>,
    ) -> Result<usize> {
        self.insert_rows_batch_with_conflict(dataset_id, table_id, schema, rows, None)
    }

    fn insert_rows_batch_with_conflict(
        &mut self,
        dataset_id: &str,
        table_id: &str,
        schema: &Schema,
        rows: Vec<IndexMap<String, Value>>,
        on_conflict: Option<&sqlparser::ast::OnInsert>,
    ) -> Result<usize> {
        let mut inserted_count = 0;

        for row_map in rows {
            let mut row = Row::for_schema(schema);
            for (column, value) in &row_map {
                row.set_by_name(schema, column, value.clone())?;
            }

            self.validate_view_check_option_insert(dataset_id, table_id, &row, schema)?;

            let instead_of_result =
                self.fire_instead_of_insert_triggers(dataset_id, table_id, &row, schema)?;

            if instead_of_result.triggers_fired > 0 {
                inserted_count += 1;
                continue;
            }

            let before_result =
                self.fire_before_insert_triggers(dataset_id, table_id, &row, schema)?;

            if before_result.skip_operation {
                continue;
            }

            if let Some(conflict) = on_conflict {
                let conflict_detected = {
                    let mut storage = self.storage.borrow_mut();
                    let dataset = storage.get_dataset_mut(dataset_id).ok_or_else(|| {
                        Error::dataset_not_found(format!("Dataset '{}' not found", dataset_id))
                    })?;
                    let table = dataset.get_table_mut(table_id).ok_or_else(|| {
                        Error::table_not_found(format!(
                            "Table '{}.{}' not found",
                            dataset_id, table_id
                        ))
                    })?;

                    let all_rows = table.get_all_rows();
                    self.check_unique_conflict(schema, &row, &all_rows)
                };

                use sqlparser::ast::{OnConflictAction, OnInsert};
                match conflict {
                    OnInsert::DuplicateKeyUpdate(assignments) => {
                        if conflict_detected {
                            self.handle_conflict_update(
                                dataset_id,
                                table_id,
                                schema,
                                &row,
                                assignments,
                                None,
                            )?;
                            inserted_count += 1;
                            continue;
                        }
                    }
                    OnInsert::OnConflict(on_conflict) => {
                        use sqlparser::ast::ConflictTarget;
                        match &on_conflict.conflict_target {
                            Some(ConflictTarget::Columns(cols)) => {
                                for col in cols {
                                    let col_name = col.to_string();
                                    if schema.field_index(&col_name).is_none() {
                                        return Err(Error::InvalidQuery(format!(
                                            "ON CONFLICT column '{}' not found in table",
                                            col_name
                                        )));
                                    }
                                }
                            }
                            Some(ConflictTarget::OnConstraint(_)) => {}
                            None => {
                                let has_pk = schema.primary_key().is_some();
                                let has_unique = !schema.unique_constraints().is_empty()
                                    || schema.fields().iter().any(|f| f.is_unique);
                                if !has_pk && !has_unique {
                                    return Err(Error::InvalidQuery(
                                        "ON CONFLICT requires a conflict target (specify columns) or the table must have a PRIMARY KEY or UNIQUE constraint".to_string(),
                                    ));
                                }
                            }
                        }

                        match &on_conflict.action {
                            OnConflictAction::DoNothing => {
                                if conflict_detected {
                                    continue;
                                }
                            }
                            OnConflictAction::DoUpdate(do_update) => {
                                if conflict_detected {
                                    self.handle_conflict_update(
                                        dataset_id,
                                        table_id,
                                        schema,
                                        &row,
                                        &do_update.assignments,
                                        do_update.selection.as_ref(),
                                    )?;
                                    inserted_count += 1;
                                    continue;
                                }
                            }
                        }
                    }

                    _ => {
                        return Err(Error::UnsupportedFeature(
                            "Unsupported ON CONFLICT variant".to_string(),
                        ));
                    }
                }
            }

            {
                let storage = self.storage.borrow_mut();
                let enforcer = crate::query_executor::enforcement::ForeignKeyEnforcer::new();
                let table_full_name = format!("{}.{}", dataset_id, table_id);

                let deferred_state = {
                    let tm = self.transaction_manager.borrow();
                    tm.get_active_transaction()
                        .map(|txn| txn.deferred_fk_state().clone())
                };

                let deferred_checks = enforcer.validate_insert_with_deferral(
                    &table_full_name,
                    &row,
                    &storage,
                    deferred_state.as_ref(),
                )?;

                if !deferred_checks.is_empty() {
                    drop(storage);
                    let mut tm = self.transaction_manager.borrow_mut();
                    if let Some(txn) = tm.get_active_transaction_mut() {
                        for check in deferred_checks {
                            txn.deferred_fk_state_mut().defer_check(check);
                        }
                    }
                }
            }

            {
                let mut tm = self.transaction_manager.borrow_mut();
                if let Some(txn) = tm.get_active_transaction_mut() {
                    let table_full_name = format!("{}.{}", dataset_id, table_id);
                    txn.track_insert(&table_full_name, row.clone());
                } else {
                    drop(tm);
                    let mut storage = self.storage.borrow_mut();
                    let dataset = storage.get_dataset_mut(dataset_id).ok_or_else(|| {
                        Error::dataset_not_found(format!("Dataset '{}' not found", dataset_id))
                    })?;
                    let table = dataset.get_table_mut(table_id).ok_or_else(|| {
                        Error::table_not_found(format!(
                            "Table '{}.{}' not found",
                            dataset_id, table_id
                        ))
                    })?;
                    {
                        let schema = table.schema_mut();
                        if schema.check_evaluator().is_none() {
                            let evaluator = super::build_check_evaluator();
                            schema.set_check_evaluator(evaluator);
                        }
                    }
                    table.insert_row(row.clone())?;
                }
            }

            self.fire_after_insert_triggers(dataset_id, table_id, &row, schema)?;

            inserted_count += 1;
        }

        Ok(inserted_count)
    }

    fn check_unique_conflict(&self, schema: &Schema, new_row: &Row, existing_rows: &[Row]) -> bool {
        if let Some(pk_cols) = schema.primary_key() {
            let new_pk_values: Vec<&Value> = pk_cols
                .iter()
                .filter_map(|col| schema.field_index(col).map(|idx| &new_row.values()[idx]))
                .collect();

            for existing_row in existing_rows {
                let existing_pk_values: Vec<&Value> = pk_cols
                    .iter()
                    .filter_map(|col| {
                        schema
                            .field_index(col)
                            .map(|idx| &existing_row.values()[idx])
                    })
                    .collect();

                if new_pk_values == existing_pk_values && !new_pk_values.is_empty() {
                    return true;
                }
            }
        }

        for constraint in schema.unique_constraints() {
            if !constraint.enforced {
                continue;
            }
            let new_unique_values: Vec<&Value> = constraint
                .columns
                .iter()
                .filter_map(|col| schema.field_index(col).map(|idx| &new_row.values()[idx]))
                .collect();

            for existing_row in existing_rows {
                let existing_unique_values: Vec<&Value> = constraint
                    .columns
                    .iter()
                    .filter_map(|col| {
                        schema
                            .field_index(col)
                            .map(|idx| &existing_row.values()[idx])
                    })
                    .collect();

                if new_unique_values == existing_unique_values && !new_unique_values.is_empty() {
                    return true;
                }
            }
        }

        for (idx, field) in schema.fields().iter().enumerate() {
            if field.is_unique {
                let new_value = &new_row.values()[idx];

                if new_value.is_null() {
                    continue;
                }

                for existing_row in existing_rows {
                    let existing_value = &existing_row.values()[idx];
                    if !existing_value.is_null() && new_value == existing_value {
                        return true;
                    }
                }
            }
        }

        false
    }

    fn handle_conflict_update(
        &mut self,
        dataset_id: &str,
        table_id: &str,
        schema: &Schema,
        new_row: &Row,
        assignments: &[sqlparser::ast::Assignment],
        where_clause: Option<&sqlparser::ast::Expr>,
    ) -> Result<()> {
        use crate::query_executor::expression_evaluator::ExpressionEvaluator;

        let mut storage = self.storage.borrow_mut();
        let dataset = storage.get_dataset_mut(dataset_id).ok_or_else(|| {
            Error::dataset_not_found(format!("Dataset '{}' not found", dataset_id))
        })?;
        let table = dataset.get_table_mut(table_id).ok_or_else(|| {
            Error::table_not_found(format!("Table '{}.{}' not found", dataset_id, table_id))
        })?;

        let all_rows = table.get_all_rows();
        let mut updated_rows = Vec::new();
        let mut found_conflict = false;

        for existing_row in all_rows {
            let is_conflict = self.is_row_conflict(schema, new_row, &existing_row);

            if is_conflict && !found_conflict {
                if let Some(where_expr) = where_clause {
                    let evaluator = ExcludedAwareEvaluator::new(schema, &existing_row, new_row);
                    let where_result = evaluator.evaluate_where(where_expr)?;

                    if !where_result {
                        updated_rows.push(existing_row);
                        found_conflict = true;
                        continue;
                    }
                }

                let mut updated_values = existing_row.values().to_vec();

                for assignment in assignments {
                    let col_name = match &assignment.target {
                        sqlparser::ast::AssignmentTarget::ColumnName(object_name) => {
                            object_name.to_string()
                        }
                        _ => continue,
                    };
                    let col_idx = schema.field_index(&col_name).ok_or_else(|| {
                        Error::InvalidQuery(format!("Column '{}' not found", col_name))
                    })?;

                    let evaluator = ExcludedAwareEvaluator::new(schema, &existing_row, new_row);
                    let new_value = evaluator.evaluate_expr(&assignment.value)?;
                    updated_values[col_idx] = new_value;
                }

                updated_rows.push(Row::from_values(updated_values));
                found_conflict = true;
            } else {
                updated_rows.push(existing_row);
            }
        }

        table.clear_rows()?;
        for row in updated_rows {
            table.insert_row(row)?;
        }

        Ok(())
    }

    fn is_row_conflict(&self, schema: &Schema, new_row: &Row, existing_row: &Row) -> bool {
        if let Some(pk_cols) = schema.primary_key() {
            let new_pk_values: Vec<&Value> = pk_cols
                .iter()
                .filter_map(|col| schema.field_index(col).map(|idx| &new_row.values()[idx]))
                .collect();
            let existing_pk_values: Vec<&Value> = pk_cols
                .iter()
                .filter_map(|col| {
                    schema
                        .field_index(col)
                        .map(|idx| &existing_row.values()[idx])
                })
                .collect();

            if !new_pk_values.is_empty()
                && !new_pk_values.iter().any(|v| v.is_null())
                && new_pk_values == existing_pk_values
            {
                return true;
            }
        }

        for constraint in schema.unique_constraints() {
            if !constraint.enforced {
                continue;
            }
            let new_unique_values: Vec<&Value> = constraint
                .columns
                .iter()
                .filter_map(|col| schema.field_index(col).map(|idx| &new_row.values()[idx]))
                .collect();
            let existing_unique_values: Vec<&Value> = constraint
                .columns
                .iter()
                .filter_map(|col| {
                    schema
                        .field_index(col)
                        .map(|idx| &existing_row.values()[idx])
                })
                .collect();

            if !new_unique_values.is_empty()
                && !new_unique_values.iter().any(|v| v.is_null())
                && !existing_unique_values.iter().any(|v| v.is_null())
                && new_unique_values == existing_unique_values
            {
                return true;
            }
        }

        for (idx, field) in schema.fields().iter().enumerate() {
            if field.is_unique {
                let new_value = &new_row.values()[idx];
                let existing_value = &existing_row.values()[idx];

                if !new_value.is_null() && !existing_value.is_null() && new_value == existing_value
                {
                    return true;
                }
            }
        }

        false
    }

    fn validate_view_check_option_insert(
        &self,
        dataset_id: &str,
        table_id: &str,
        row: &Row,
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

        debug_eprintln!(
            "[executor::dml::insert] validate_view_check_option_insert: WHERE clause = '{}'",
            where_clause_sql
        );

        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let dialect = GenericDialect {};

        debug_eprintln!("[executor::dml::insert] Creating dummy SELECT query");
        let test_query = format!("SELECT * FROM dummy WHERE {}", where_clause_sql);
        debug_eprintln!("[executor::dml::insert] Test query: {}", test_query);

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
        let satisfies_where = evaluator.evaluate_where(&where_expr, row).unwrap_or(false);

        if !satisfies_where {
            return Err(Error::InvalidQuery(format!(
                "WITH CHECK OPTION violation: new row does not satisfy view WHERE clause ({})",
                where_clause_sql
            )));
        }

        Ok(())
    }

    fn contains_subquery(expr: &sqlparser::ast::Expr) -> bool {
        use sqlparser::ast::Expr;

        match expr {
            Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::contains_subquery(left) || Self::contains_subquery(right)
            }
            Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } | Expr::Nested(expr) => {
                Self::contains_subquery(expr)
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand
                    && Self::contains_subquery(op)
                {
                    return true;
                }
                if conditions.iter().any(|case_when| {
                    Self::contains_subquery(&case_when.condition)
                        || Self::contains_subquery(&case_when.result)
                }) {
                    return true;
                }
                if let Some(else_res) = else_result
                    && Self::contains_subquery(else_res)
                {
                    return true;
                }
                false
            }
            Expr::Function(func) => {
                use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
                if let FunctionArguments::List(list) = &func.args {
                    for arg in &list.args {
                        match arg {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                            | FunctionArg::Named {
                                arg: FunctionArgExpr::Expr(e),
                                ..
                            }
                            | FunctionArg::ExprNamed {
                                arg: FunctionArgExpr::Expr(e),
                                ..
                            } => {
                                if Self::contains_subquery(e) {
                                    return true;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                matches!(func.args, FunctionArguments::Subquery(_))
            }
            _ => false,
        }
    }

    fn parse_hex_literal(hex_str: &str) -> Result<Value> {
        if !hex_str.len().is_multiple_of(2) {
            return Err(Error::InvalidQuery(format!(
                "Hex literal must have even number of characters, got {}",
                hex_str.len()
            )));
        }

        let mut bytes = Vec::with_capacity(hex_str.len() / 2);
        for i in (0..hex_str.len()).step_by(2) {
            let byte_str = &hex_str[i..i + 2];
            match u8::from_str_radix(byte_str, 16) {
                Ok(byte) => bytes.push(byte),
                Err(_) => {
                    return Err(Error::InvalidQuery(format!(
                        "Invalid hex characters in literal: '{}'",
                        byte_str
                    )));
                }
            }
        }
        Ok(Value::bytes(bytes))
    }
}

struct ExcludedAwareEvaluator<'a> {
    schema: &'a Schema,
    existing_row: &'a Row,
    excluded_row: &'a Row,
}

impl<'a> ExcludedAwareEvaluator<'a> {
    fn new(schema: &'a Schema, existing_row: &'a Row, excluded_row: &'a Row) -> Self {
        Self {
            schema,
            existing_row,
            excluded_row,
        }
    }

    fn evaluate_expr(&self, expr: &sqlparser::ast::Expr) -> Result<Value> {
        use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator};

        match expr {
            Expr::CompoundIdentifier(parts) => {
                if parts.len() == 2 {
                    let table_or_keyword = parts[0].value.to_uppercase();
                    let column = &parts[1].value;

                    if table_or_keyword == "EXCLUDED" {
                        let col_idx = self.schema.field_index(column).ok_or_else(|| {
                            Error::InvalidQuery(format!("Column '{}' not found", column))
                        })?;
                        return Ok(self.excluded_row.values()[col_idx].clone());
                    } else {
                        let col_idx = self.schema.field_index(column).ok_or_else(|| {
                            Error::InvalidQuery(format!("Column '{}' not found", column))
                        })?;
                        return Ok(self.existing_row.values()[col_idx].clone());
                    }
                }
                Err(Error::InvalidQuery(format!(
                    "Unsupported compound identifier: {:?}",
                    parts
                )))
            }

            Expr::Identifier(ident) => {
                let col_idx = self.schema.field_index(&ident.value).ok_or_else(|| {
                    Error::InvalidQuery(format!("Column '{}' not found", ident.value))
                })?;
                Ok(self.existing_row.values()[col_idx].clone())
            }

            Expr::Value(v) => {
                use sqlparser::ast::{Value as SqlValue, ValueWithSpan};
                match v {
                    ValueWithSpan {
                        value: SqlValue::Number(n, _),
                        ..
                    } => {
                        if let Ok(i) = n.parse::<i64>() {
                            Ok(Value::int64(i))
                        } else if let Ok(f) = n.parse::<f64>() {
                            Ok(Value::float64(f))
                        } else {
                            Err(Error::InvalidQuery(format!("Invalid number: {}", n)))
                        }
                    }
                    ValueWithSpan {
                        value: SqlValue::SingleQuotedString(s),
                        ..
                    }
                    | ValueWithSpan {
                        value: SqlValue::DoubleQuotedString(s),
                        ..
                    } => Ok(Value::string(s.clone())),
                    ValueWithSpan {
                        value: SqlValue::Boolean(b),
                        ..
                    } => Ok(Value::bool_val(*b)),
                    ValueWithSpan {
                        value: SqlValue::Null,
                        ..
                    } => Ok(Value::null()),
                    _ => Err(Error::InvalidQuery(format!(
                        "Unsupported value type: {:?}",
                        v
                    ))),
                }
            }

            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expr(left)?;
                let right_val = self.evaluate_expr(right)?;

                match op {
                    BinaryOperator::Plus => self.add_values(&left_val, &right_val),
                    BinaryOperator::Minus => self.subtract_values(&left_val, &right_val),
                    BinaryOperator::Multiply => self.multiply_values(&left_val, &right_val),
                    BinaryOperator::Divide => self.divide_values(&left_val, &right_val),
                    BinaryOperator::Lt => Ok(Value::bool_val(
                        self.compare_values(&left_val, &right_val)? < 0,
                    )),
                    BinaryOperator::LtEq => Ok(Value::bool_val(
                        self.compare_values(&left_val, &right_val)? <= 0,
                    )),
                    BinaryOperator::Gt => Ok(Value::bool_val(
                        self.compare_values(&left_val, &right_val)? > 0,
                    )),
                    BinaryOperator::GtEq => Ok(Value::bool_val(
                        self.compare_values(&left_val, &right_val)? >= 0,
                    )),
                    BinaryOperator::Eq => Ok(Value::bool_val(left_val == right_val)),
                    BinaryOperator::NotEq => Ok(Value::bool_val(left_val != right_val)),
                    BinaryOperator::And => {
                        let l = left_val.as_bool().unwrap_or(false);
                        let r = right_val.as_bool().unwrap_or(false);
                        Ok(Value::bool_val(l && r))
                    }
                    BinaryOperator::Or => {
                        let l = left_val.as_bool().unwrap_or(false);
                        let r = right_val.as_bool().unwrap_or(false);
                        Ok(Value::bool_val(l || r))
                    }
                    _ => Err(Error::InvalidQuery(format!(
                        "Unsupported binary operator: {:?}",
                        op
                    ))),
                }
            }

            Expr::UnaryOp { op, expr } => {
                let val = self.evaluate_expr(expr)?;
                match op {
                    UnaryOperator::Minus => {
                        if let Some(i) = val.as_i64() {
                            Ok(Value::int64(-i))
                        } else if let Some(f) = val.as_f64() {
                            Ok(Value::float64(-f))
                        } else {
                            Err(Error::InvalidQuery(
                                "Cannot negate non-numeric value".to_string(),
                            ))
                        }
                    }
                    UnaryOperator::Not => {
                        let b = val.as_bool().unwrap_or(false);
                        Ok(Value::bool_val(!b))
                    }
                    _ => Err(Error::InvalidQuery(format!(
                        "Unsupported unary operator: {:?}",
                        op
                    ))),
                }
            }

            Expr::Nested(inner) => self.evaluate_expr(inner),

            Expr::Cast {
                expr, data_type, ..
            } => {
                let val = self.evaluate_expr(expr)?;

                match data_type {
                    sqlparser::ast::DataType::Int64 | sqlparser::ast::DataType::BigInt(_) => {
                        if let Some(i) = val.as_i64() {
                            Ok(Value::int64(i))
                        } else if let Some(f) = val.as_f64() {
                            Ok(Value::int64(f as i64))
                        } else {
                            Err(Error::InvalidQuery("Cannot cast to INT64".to_string()))
                        }
                    }
                    sqlparser::ast::DataType::Float64 | sqlparser::ast::DataType::Double(..) => {
                        if let Some(f) = val.as_f64() {
                            Ok(Value::float64(f))
                        } else if let Some(i) = val.as_i64() {
                            Ok(Value::float64(i as f64))
                        } else {
                            Err(Error::InvalidQuery("Cannot cast to FLOAT64".to_string()))
                        }
                    }
                    _ => Ok(val),
                }
            }

            _ => Err(Error::InvalidQuery(format!(
                "Unsupported expression in ON CONFLICT: {:?}",
                expr
            ))),
        }
    }

    fn evaluate_where(&self, expr: &sqlparser::ast::Expr) -> Result<bool> {
        let val = self.evaluate_expr(expr)?;
        Ok(val.as_bool().unwrap_or(false))
    }

    fn add_values(&self, left: &Value, right: &Value) -> Result<Value> {
        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            Ok(Value::int64(l + r))
        } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
            Ok(Value::float64(l + r))
        } else if let (Some(l), Some(r)) = (left.as_i64(), right.as_f64()) {
            Ok(Value::float64(l as f64 + r))
        } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_i64()) {
            Ok(Value::float64(l + r as f64))
        } else {
            Err(Error::InvalidQuery(
                "Cannot add non-numeric values".to_string(),
            ))
        }
    }

    fn subtract_values(&self, left: &Value, right: &Value) -> Result<Value> {
        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            Ok(Value::int64(l - r))
        } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
            Ok(Value::float64(l - r))
        } else {
            Err(Error::InvalidQuery(
                "Cannot subtract non-numeric values".to_string(),
            ))
        }
    }

    fn multiply_values(&self, left: &Value, right: &Value) -> Result<Value> {
        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            Ok(Value::int64(l * r))
        } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
            Ok(Value::float64(l * r))
        } else {
            Err(Error::InvalidQuery(
                "Cannot multiply non-numeric values".to_string(),
            ))
        }
    }

    fn divide_values(&self, left: &Value, right: &Value) -> Result<Value> {
        if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
            if r == 0.0 {
                return Err(Error::InvalidQuery("Division by zero".to_string()));
            }
            Ok(Value::float64(l / r))
        } else if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            if r == 0 {
                return Err(Error::InvalidQuery("Division by zero".to_string()));
            }
            Ok(Value::int64(l / r))
        } else {
            Err(Error::InvalidQuery(
                "Cannot divide non-numeric values".to_string(),
            ))
        }
    }

    fn compare_values(&self, left: &Value, right: &Value) -> Result<i32> {
        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            Ok(l.cmp(&r) as i32)
        } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
            Ok(l.partial_cmp(&r).map(|o| o as i32).unwrap_or(0))
        } else if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
            Ok(l.cmp(r) as i32)
        } else {
            Err(Error::InvalidQuery(
                "Cannot compare values of different types".to_string(),
            ))
        }
    }
}
