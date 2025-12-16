//! Query executor - parses and executes SQL statements.

use std::collections::HashMap;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value, StructField};
use yachtsql_parser::DialectType;
use yachtsql_storage::{Column, Field, Row, Schema};
use sqlparser::ast::{
    self, Expr, Ident, ObjectName, Query, Select, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins, OrderByExpr, Value as SqlValue, LimitClause, OrderBy,
    OrderByKind,
};
use sqlparser::dialect::BigQueryDialect;
use sqlparser::parser::Parser;

use crate::catalog::{Catalog, TableData};
use crate::evaluator::Evaluator;
use crate::table::Table;

pub struct QueryExecutor {
    dialect: DialectType,
    catalog: Catalog,
}

impl QueryExecutor {
    pub fn new() -> Self {
        Self {
            dialect: DialectType::BigQuery,
            catalog: Catalog::new(),
        }
    }

    pub fn with_dialect(dialect: DialectType) -> Self {
        Self {
            dialect,
            catalog: Catalog::new(),
        }
    }

    pub fn execute_sql(&mut self, sql: &str) -> Result<Table> {
        let dialect = BigQueryDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| Error::ParseError(e.to_string()))?;

        if statements.is_empty() {
            return Err(Error::ParseError("Empty SQL statement".to_string()));
        }

        self.execute_statement(&statements[0])
    }

    fn execute_statement(&mut self, stmt: &Statement) -> Result<Table> {
        match stmt {
            Statement::Query(query) => self.execute_query(query),
            Statement::CreateTable(create) => self.execute_create_table(create),
            Statement::Drop { object_type, names, if_exists, .. } => {
                self.execute_drop(object_type, names, *if_exists)
            }
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Update { table, assignments, selection, .. } => {
                self.execute_update(table, assignments, selection.as_ref())
            }
            Statement::Delete(delete) => self.execute_delete(delete),
            Statement::Truncate { table_names, .. } => {
                self.execute_truncate(table_names)
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Statement type not yet supported: {:?}",
                stmt
            ))),
        }
    }

    fn execute_query(&self, query: &Query) -> Result<Table> {
        match query.body.as_ref() {
            SetExpr::Select(select) => self.execute_select(select, &query.order_by, &query.limit_clause),
            SetExpr::Values(values) => self.execute_values(values),
            _ => Err(Error::UnsupportedFeature(format!(
                "Query type not yet supported: {:?}",
                query.body
            ))),
        }
    }

    fn execute_select(
        &self,
        select: &Select,
        order_by: &Option<OrderBy>,
        limit_clause: &Option<LimitClause>,
    ) -> Result<Table> {
        let (schema, mut rows) = if select.from.is_empty() {
            self.evaluate_select_without_from(select)?
        } else {
            self.evaluate_select_with_from(select)?
        };

        if let Some(order_by) = order_by {
            self.sort_rows(&schema, &mut rows, order_by)?;
        }

        if let Some(limit_clause) = limit_clause {
            match limit_clause {
                LimitClause::LimitOffset { limit, offset, .. } => {
                    if let Some(offset_expr) = offset {
                        let offset_val = self.evaluate_literal_expr(&offset_expr.value)?;
                        let offset_num = offset_val.as_i64().ok_or_else(|| {
                            Error::InvalidQuery("OFFSET must be an integer".to_string())
                        })? as usize;
                        if offset_num < rows.len() {
                            rows = rows.into_iter().skip(offset_num).collect();
                        } else {
                            rows.clear();
                        }
                    }
                    if let Some(limit_expr) = limit {
                        let limit_val = self.evaluate_literal_expr(limit_expr)?;
                        let limit_num = limit_val.as_i64().ok_or_else(|| {
                            Error::InvalidQuery("LIMIT must be an integer".to_string())
                        })? as usize;
                        rows.truncate(limit_num);
                    }
                }
                LimitClause::OffsetCommaLimit { offset, limit } => {
                    let offset_val = self.evaluate_literal_expr(offset)?;
                    let offset_num = offset_val.as_i64().ok_or_else(|| {
                        Error::InvalidQuery("OFFSET must be an integer".to_string())
                    })? as usize;
                    if offset_num < rows.len() {
                        rows = rows.into_iter().skip(offset_num).collect();
                    } else {
                        rows.clear();
                    }
                    let limit_val = self.evaluate_literal_expr(limit)?;
                    let limit_num = limit_val.as_i64().ok_or_else(|| {
                        Error::InvalidQuery("LIMIT must be an integer".to_string())
                    })? as usize;
                    rows.truncate(limit_num);
                }
            }
        }

        Table::from_rows(schema, rows)
    }

    fn evaluate_select_without_from(&self, select: &Select) -> Result<(Schema, Vec<Row>)> {
        let empty_schema = Schema::new();
        let empty_row = Row::from_values(vec![]);
        let evaluator = Evaluator::new(&empty_schema);

        let mut result_values = Vec::new();
        let mut field_names = Vec::new();

        for (idx, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let val = evaluator.evaluate(expr, &empty_row)?;
                    result_values.push(val.clone());
                    field_names.push(self.expr_to_alias(expr, idx));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let val = evaluator.evaluate(expr, &empty_row)?;
                    result_values.push(val.clone());
                    field_names.push(alias.value.clone());
                }
                _ => return Err(Error::UnsupportedFeature(
                    "Unsupported projection item".to_string(),
                )),
            }
        }

        let fields: Vec<Field> = result_values.iter().zip(field_names.iter())
            .map(|(val, name)| Field::nullable(name.clone(), val.data_type()))
            .collect();

        let schema = Schema::from_fields(fields);
        let row = Row::from_values(result_values);

        Ok((schema, vec![row]))
    }

    fn evaluate_select_with_from(&self, select: &Select) -> Result<(Schema, Vec<Row>)> {
        let table_name = self.extract_table_name(&select.from)?;
        let table_data = self.catalog.get_table(&table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

        let input_schema = &table_data.schema;
        let input_rows = &table_data.rows;
        let evaluator = Evaluator::new(input_schema);

        let mut filtered_rows: Vec<Row> = if let Some(selection) = &select.selection {
            input_rows.iter()
                .filter(|row| evaluator.evaluate_to_bool(selection, row).unwrap_or(false))
                .cloned()
                .collect()
        } else {
            input_rows.clone()
        };

        if select.distinct.is_some() {
            let mut seen = std::collections::HashSet::new();
            filtered_rows.retain(|row| {
                let key = format!("{:?}", row.values());
                seen.insert(key)
            });
        }

        let (output_schema, output_rows) = self.project_rows(
            input_schema,
            &filtered_rows,
            &select.projection,
        )?;

        Ok((output_schema, output_rows))
    }

    fn project_rows(
        &self,
        input_schema: &Schema,
        rows: &[Row],
        projection: &[SelectItem],
    ) -> Result<(Schema, Vec<Row>)> {
        let evaluator = Evaluator::new(input_schema);

        let mut all_cols: Vec<(String, DataType)> = Vec::new();
        let mut has_star = false;

        for (idx, item) in projection.iter().enumerate() {
            match item {
                SelectItem::Wildcard(_) => {
                    has_star = true;
                    for field in input_schema.fields() {
                        all_cols.push((field.name.clone(), field.data_type.clone()));
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    let sample_row = rows.first().cloned().unwrap_or_else(|| {
                        Row::from_values(vec![Value::null(); input_schema.field_count()])
                    });
                    let val = evaluator.evaluate(expr, &sample_row).unwrap_or(Value::null());
                    let name = self.expr_to_alias(expr, idx);
                    all_cols.push((name, val.data_type()));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let sample_row = rows.first().cloned().unwrap_or_else(|| {
                        Row::from_values(vec![Value::null(); input_schema.field_count()])
                    });
                    let val = evaluator.evaluate(expr, &sample_row).unwrap_or(Value::null());
                    all_cols.push((alias.value.clone(), val.data_type()));
                }
                _ => return Err(Error::UnsupportedFeature(
                    "Unsupported projection item".to_string(),
                )),
            }
        }

        let fields: Vec<Field> = all_cols.iter()
            .map(|(name, dt)| Field::nullable(name.clone(), dt.clone()))
            .collect();
        let output_schema = Schema::from_fields(fields);

        let mut output_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let mut values = Vec::new();
            for item in projection {
                match item {
                    SelectItem::Wildcard(_) => {
                        values.extend(row.values().iter().cloned());
                    }
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        let val = evaluator.evaluate(expr, row)?;
                        values.push(val);
                    }
                    _ => {}
                }
            }
            output_rows.push(Row::from_values(values));
        }

        Ok((output_schema, output_rows))
    }

    fn sort_rows(&self, schema: &Schema, rows: &mut Vec<Row>, order_by: &OrderBy) -> Result<()> {
        let evaluator = Evaluator::new(schema);

        let exprs: &[OrderByExpr] = match &order_by.kind {
            OrderByKind::Expressions(exprs) => exprs,
            OrderByKind::All(_) => return Ok(()),
        };

        rows.sort_by(|a, b| {
            for order_expr in exprs {
                let a_val = evaluator.evaluate(&order_expr.expr, a).unwrap_or(Value::null());
                let b_val = evaluator.evaluate(&order_expr.expr, b).unwrap_or(Value::null());

                let ordering = self.compare_values(&a_val, &b_val);
                let ordering = if order_expr.options.asc.unwrap_or(true) {
                    ordering
                } else {
                    ordering.reverse()
                };

                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(())
    }

    fn compare_values(&self, a: &Value, b: &Value) -> std::cmp::Ordering {
        if a.is_null() && b.is_null() {
            return std::cmp::Ordering::Equal;
        }
        if a.is_null() {
            return std::cmp::Ordering::Greater;
        }
        if b.is_null() {
            return std::cmp::Ordering::Less;
        }

        if let (Some(a_i), Some(b_i)) = (a.as_i64(), b.as_i64()) {
            return a_i.cmp(&b_i);
        }
        if let (Some(a_f), Some(b_f)) = (a.as_f64(), b.as_f64()) {
            return a_f.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal);
        }
        if let (Some(a_s), Some(b_s)) = (a.as_str(), b.as_str()) {
            return a_s.cmp(b_s);
        }
        if let (Some(a_b), Some(b_b)) = (a.as_bool(), b.as_bool()) {
            return a_b.cmp(&b_b);
        }

        std::cmp::Ordering::Equal
    }

    fn execute_values(&self, values: &ast::Values) -> Result<Table> {
        if values.rows.is_empty() {
            return Ok(Table::empty(Schema::new()));
        }

        let first_row = &values.rows[0];
        let num_cols = first_row.len();

        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        for row_exprs in &values.rows {
            if row_exprs.len() != num_cols {
                return Err(Error::InvalidQuery(
                    "All rows must have the same number of columns".to_string(),
                ));
            }
            let mut row_values = Vec::new();
            for expr in row_exprs {
                let val = self.evaluate_literal_expr(expr)?;
                row_values.push(val);
            }
            all_rows.push(row_values);
        }

        let fields: Vec<Field> = (0..num_cols)
            .map(|i| {
                let dt = all_rows.iter()
                    .find_map(|row| {
                        let dt = row[i].data_type();
                        if dt != DataType::Unknown { Some(dt) } else { None }
                    })
                    .unwrap_or(DataType::String);
                Field::nullable(format!("column{}", i + 1), dt)
            })
            .collect();

        let schema = Schema::from_fields(fields);
        let rows: Vec<Row> = all_rows.into_iter()
            .map(Row::from_values)
            .collect();

        Table::from_rows(schema, rows)
    }

    fn execute_create_table(&mut self, create: &ast::CreateTable) -> Result<Table> {
        let table_name = create.name.to_string();

        if create.or_replace {
            let _ = self.catalog.drop_table(&table_name);
        } else if self.catalog.table_exists(&table_name) && !create.if_not_exists {
            return Err(Error::invalid_query(format!("Table already exists: {}", table_name)));
        }

        if self.catalog.table_exists(&table_name) {
            return Ok(Table::empty(Schema::new()));
        }

        let fields: Vec<Field> = create.columns.iter()
            .map(|col| {
                let data_type = self.sql_type_to_data_type(&col.data_type)?;
                let nullable = !col.options.iter().any(|opt| {
                    matches!(opt.option, ast::ColumnOption::NotNull)
                });
                if nullable {
                    Ok(Field::nullable(col.name.value.clone(), data_type))
                } else {
                    Ok(Field::required(col.name.value.clone(), data_type))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let schema = Schema::from_fields(fields);
        self.catalog.create_table(&table_name, schema)?;

        Ok(Table::empty(Schema::new()))
    }

    fn execute_drop(
        &mut self,
        object_type: &ast::ObjectType,
        names: &[ObjectName],
        if_exists: bool,
    ) -> Result<Table> {
        match object_type {
            ast::ObjectType::Table => {
                for name in names {
                    let table_name = name.to_string();
                    if if_exists && !self.catalog.table_exists(&table_name) {
                        continue;
                    }
                    self.catalog.drop_table(&table_name)?;
                }
                Ok(Table::empty(Schema::new()))
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "DROP {:?} not yet supported",
                object_type
            ))),
        }
    }

    fn execute_insert(&mut self, insert: &ast::Insert) -> Result<Table> {
        let table_name = insert.table.to_string();
        let table_data = self.catalog.get_table_mut(&table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

        let schema = table_data.schema.clone();

        let column_indices: Vec<usize> = if insert.columns.is_empty() {
            (0..schema.field_count()).collect()
        } else {
            insert.columns.iter()
                .map(|col| {
                    schema.fields().iter()
                        .position(|f| f.name.to_uppercase() == col.value.to_uppercase())
                        .ok_or_else(|| Error::ColumnNotFound(col.value.clone()))
                })
                .collect::<Result<Vec<_>>>()?
        };

        let source = insert.source.as_ref()
            .ok_or_else(|| Error::InvalidQuery("INSERT requires VALUES or SELECT".to_string()))?;

        match source.body.as_ref() {
            SetExpr::Values(values) => {
                for row_exprs in &values.rows {
                    if row_exprs.len() != column_indices.len() {
                        return Err(Error::InvalidQuery(format!(
                            "Expected {} values, got {}",
                            column_indices.len(),
                            row_exprs.len()
                        )));
                    }

                    let mut row_values = vec![Value::null(); schema.field_count()];
                    for (expr_idx, &col_idx) in column_indices.iter().enumerate() {
                        let val = self.evaluate_literal_expr(&row_exprs[expr_idx])?;
                        row_values[col_idx] = val;
                    }

                    let table_data = self.catalog.get_table_mut(&table_name).unwrap();
                    table_data.rows.push(Row::from_values(row_values));
                }
            }
            SetExpr::Select(select) => {
                let (_, rows) = self.evaluate_select_with_from(select)?;
                let table_data = self.catalog.get_table_mut(&table_name).unwrap();
                for row in rows {
                    let values = row.values();
                    if values.len() != column_indices.len() {
                        return Err(Error::InvalidQuery(format!(
                            "Expected {} values, got {}",
                            column_indices.len(),
                            values.len()
                        )));
                    }

                    let mut row_values = vec![Value::null(); schema.field_count()];
                    for (val_idx, &col_idx) in column_indices.iter().enumerate() {
                        row_values[col_idx] = values[val_idx].clone();
                    }
                    table_data.rows.push(Row::from_values(row_values));
                }
            }
            _ => return Err(Error::UnsupportedFeature(
                "INSERT source type not supported".to_string(),
            )),
        }

        Ok(Table::empty(Schema::new()))
    }

    fn execute_update(
        &mut self,
        table: &ast::TableWithJoins,
        assignments: &[ast::Assignment],
        selection: Option<&Expr>,
    ) -> Result<Table> {
        let table_name = self.extract_single_table_name(table)?;
        let table_data = self.catalog.get_table_mut(&table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

        let schema = table_data.schema.clone();
        let evaluator = Evaluator::new(&schema);

        let assignment_indices: Vec<(usize, &Expr)> = assignments.iter()
            .map(|a| {
                let col_name = match &a.target {
                    ast::AssignmentTarget::ColumnName(obj_name) => obj_name.to_string(),
                    ast::AssignmentTarget::Tuple(_) => {
                        return Err(Error::UnsupportedFeature("Tuple assignment not supported".to_string()));
                    }
                };
                let idx = schema.fields().iter()
                    .position(|f| f.name.to_uppercase() == col_name.to_uppercase())
                    .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;
                Ok((idx, &a.value))
            })
            .collect::<Result<Vec<_>>>()?;

        for row in &mut table_data.rows {
            let should_update = match selection {
                Some(sel) => evaluator.evaluate_to_bool(sel, row)?,
                None => true,
            };

            if should_update {
                let mut values = row.values().to_vec();
                for (col_idx, expr) in &assignment_indices {
                    let new_val = evaluator.evaluate(expr, row)?;
                    values[*col_idx] = new_val;
                }
                *row = Row::from_values(values);
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    fn execute_delete(&mut self, delete: &ast::Delete) -> Result<Table> {
        let table_name = self.extract_delete_table_name(delete)?;
        let table_data = self.catalog.get_table_mut(&table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

        let schema = table_data.schema.clone();
        let evaluator = Evaluator::new(&schema);

        match &delete.selection {
            Some(selection) => {
                table_data.rows.retain(|row| {
                    !evaluator.evaluate_to_bool(selection, row).unwrap_or(false)
                });
            }
            None => {
                table_data.rows.clear();
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    fn execute_truncate(&mut self, table_names: &[ast::TruncateTableTarget]) -> Result<Table> {
        for target in table_names {
            let table_name = target.name.to_string();
            if let Some(table_data) = self.catalog.get_table_mut(&table_name) {
                table_data.rows.clear();
            } else {
                return Err(Error::TableNotFound(table_name));
            }
        }
        Ok(Table::empty(Schema::new()))
    }

    fn extract_table_name(&self, from: &[TableWithJoins]) -> Result<String> {
        if from.is_empty() {
            return Err(Error::InvalidQuery("FROM clause is empty".to_string()));
        }

        match &from[0].relation {
            TableFactor::Table { name, .. } => Ok(name.to_string()),
            _ => Err(Error::UnsupportedFeature(
                "Only simple table references supported".to_string(),
            )),
        }
    }

    fn extract_single_table_name(&self, table: &ast::TableWithJoins) -> Result<String> {
        match &table.relation {
            TableFactor::Table { name, .. } => Ok(name.to_string()),
            _ => Err(Error::UnsupportedFeature(
                "Only simple table references supported".to_string(),
            )),
        }
    }

    fn extract_delete_table_name(&self, delete: &ast::Delete) -> Result<String> {
        let tables = match &delete.from {
            ast::FromTable::WithFromKeyword(tables) | ast::FromTable::WithoutKeyword(tables) => tables,
        };
        if let Some(from) = tables.first() {
            match &from.relation {
                TableFactor::Table { name, .. } => Ok(name.to_string()),
                _ => Err(Error::UnsupportedFeature(
                    "Only simple table references supported".to_string(),
                )),
            }
        } else {
            Err(Error::InvalidQuery("DELETE requires FROM clause".to_string()))
        }
    }

    fn expr_to_alias(&self, expr: &Expr, idx: usize) -> String {
        match expr {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(parts) => {
                parts.last().map(|i| i.value.clone()).unwrap_or_else(|| format!("_col{}", idx))
            }
            _ => format!("_col{}", idx),
        }
    }

    fn evaluate_literal_expr(&self, expr: &Expr) -> Result<Value> {
        match expr {
            Expr::Value(val) => self.sql_value_to_value(&val.value),
            Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr } => {
                let val = self.evaluate_literal_expr(expr)?;
                if let Some(i) = val.as_i64() {
                    return Ok(Value::int64(-i));
                }
                if let Some(f) = val.as_f64() {
                    return Ok(Value::float64(-f));
                }
                Err(Error::InvalidQuery("Cannot negate non-numeric value".to_string()))
            }
            Expr::Identifier(ident) if ident.value.to_uppercase() == "NULL" => {
                Ok(Value::null())
            }
            Expr::Array(arr) => {
                let mut values = Vec::with_capacity(arr.elem.len());
                for elem in &arr.elem {
                    values.push(self.evaluate_literal_expr(elem)?);
                }
                Ok(Value::array(values))
            }
            Expr::Nested(inner) => self.evaluate_literal_expr(inner),
            Expr::Tuple(exprs) => {
                let mut values = Vec::with_capacity(exprs.len());
                for e in exprs {
                    values.push(self.evaluate_literal_expr(e)?);
                }
                Ok(Value::array(values))
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Expression not supported in this context: {:?}",
                expr
            ))),
        }
    }

    fn sql_value_to_value(&self, val: &SqlValue) -> Result<Value> {
        match val {
            SqlValue::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::int64(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Value::float64(f))
                } else {
                    Err(Error::ParseError(format!("Invalid number: {}", n)))
                }
            }
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                Ok(Value::string(s.clone()))
            }
            SqlValue::Boolean(b) => Ok(Value::bool_val(*b)),
            SqlValue::Null => Ok(Value::null()),
            _ => Err(Error::UnsupportedFeature(format!(
                "SQL value type not yet supported: {:?}",
                val
            ))),
        }
    }

    fn sql_type_to_data_type(&self, sql_type: &ast::DataType) -> Result<DataType> {
        match sql_type {
            ast::DataType::Int64 | ast::DataType::BigInt(_) | ast::DataType::Integer(_) => {
                Ok(DataType::Int64)
            }
            ast::DataType::Float64 | ast::DataType::Double(_) | ast::DataType::DoublePrecision => {
                Ok(DataType::Float64)
            }
            ast::DataType::Boolean | ast::DataType::Bool => Ok(DataType::Bool),
            ast::DataType::String(_) | ast::DataType::Varchar(_) | ast::DataType::Text => {
                Ok(DataType::String)
            }
            ast::DataType::Bytes(_) | ast::DataType::Binary(_) | ast::DataType::Bytea => {
                Ok(DataType::Bytes)
            }
            ast::DataType::Date => Ok(DataType::Date),
            ast::DataType::Time(_, _) => Ok(DataType::Time),
            ast::DataType::Timestamp(_, _) => Ok(DataType::Timestamp),
            ast::DataType::Datetime(_) => Ok(DataType::Timestamp),
            ast::DataType::Numeric(_) | ast::DataType::Decimal(_) => {
                Ok(DataType::Numeric(None))
            }
            ast::DataType::JSON => Ok(DataType::Json),
            ast::DataType::Array(inner) => {
                let element_type = match inner {
                    ast::ArrayElemTypeDef::None => DataType::Unknown,
                    ast::ArrayElemTypeDef::AngleBracket(dt)
                    | ast::ArrayElemTypeDef::SquareBracket(dt, _)
                    | ast::ArrayElemTypeDef::Parenthesis(dt) => {
                        self.sql_type_to_data_type(dt)?
                    }
                };
                Ok(DataType::Array(Box::new(element_type)))
            }
            ast::DataType::Struct(fields, _) => {
                let struct_fields: Vec<StructField> = fields.iter()
                    .map(|f| {
                        let dt = self.sql_type_to_data_type(&f.field_type)?;
                        let name = f.field_name.as_ref()
                            .map(|n| n.value.clone())
                            .unwrap_or_default();
                        Ok(StructField { name, data_type: dt })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(DataType::Struct(struct_fields))
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Data type not yet supported: {:?}",
                sql_type
            ))),
        }
    }
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}
