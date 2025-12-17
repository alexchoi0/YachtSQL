#![allow(clippy::wildcard_enum_match_arm)]

use std::collections::HashMap;
use std::sync::LazyLock;

use regex::Regex;
use sqlparser::ast::{
    self, Expr, LimitClause, ObjectName, OrderBy, OrderByExpr, OrderByKind, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value as SqlValue,
};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_storage::{Column, Field, Record, Schema, Table, TableSchemaOps};

use crate::catalog::Catalog;
use crate::evaluator::Evaluator;

#[derive(Debug, Clone)]
pub struct ArrayJoinClause {
    pub is_left: bool,
    pub expressions: Vec<ArrayJoinExpr>,
}

#[derive(Debug, Clone)]
pub struct ArrayJoinExpr {
    pub expr: String,
    pub alias: Option<String>,
}

pub struct ClickHouseExecutor {
    catalog: Catalog,
}

impl ClickHouseExecutor {
    pub fn new() -> Self {
        Self {
            catalog: Catalog::new(),
        }
    }

    pub fn execute_sql(&mut self, sql: &str) -> Result<Table> {
        let (processed_sql, array_join_clause, original_sql) = self.extract_array_join(sql);

        let dialect = ClickHouseDialect {};
        let statements = Parser::parse_sql(&dialect, &processed_sql)
            .map_err(|e| Error::ParseError(e.to_string()))?;

        if statements.is_empty() {
            return Err(Error::ParseError("Empty SQL statement".to_string()));
        }

        let result = self.execute_statement(&statements[0])?;

        if let Some(array_join) = array_join_clause {
            let expanded = self.apply_array_join(result, &array_join)?;
            if let Some(orig_sql) = original_sql {
                self.apply_post_array_join_processing(expanded, &orig_sql)
            } else {
                Ok(expanded)
            }
        } else {
            Ok(result)
        }
    }

    fn apply_post_array_join_processing(&self, table: Table, original_sql: &str) -> Result<Table> {
        let re = Regex::new(r"(?i)^SELECT\s+(.+?)\s+FROM").unwrap();
        let projection_cols: Vec<String> = if let Some(caps) = re.captures(original_sql) {
            let projection_str = caps.get(1).map(|m| m.as_str()).unwrap_or("*");
            projection_str
                .split(',')
                .map(|s| s.trim().to_uppercase())
                .collect()
        } else {
            vec!["*".to_string()]
        };

        let order_re = Regex::new(r"(?i)\bORDER\s+BY\s+(.+?)(?:\s+LIMIT|\s*$)").unwrap();
        let order_cols: Vec<(String, bool)> = if let Some(caps) = order_re.captures(original_sql) {
            let order_str = caps.get(1).map(|m| m.as_str()).unwrap_or("");
            order_str
                .split(',')
                .map(|s| {
                    let s = s.trim();
                    let upper = s.to_uppercase();
                    if upper.ends_with(" DESC") {
                        (s[..s.len() - 5].trim().to_uppercase(), false)
                    } else if upper.ends_with(" ASC") {
                        (s[..s.len() - 4].trim().to_uppercase(), true)
                    } else {
                        (upper, true)
                    }
                })
                .collect()
        } else {
            vec![]
        };

        let schema = table.schema();
        let mut rows = table.to_records()?;

        if !order_cols.is_empty() {
            rows.sort_by(|a, b| {
                for (col_name, asc) in &order_cols {
                    let idx = schema
                        .fields()
                        .iter()
                        .position(|f| f.name.to_uppercase() == *col_name);
                    if let Some(idx) = idx {
                        let a_val = a.values().get(idx).cloned().unwrap_or(Value::null());
                        let b_val = b.values().get(idx).cloned().unwrap_or(Value::null());
                        let cmp = self.compare_values(&a_val, &b_val);
                        if cmp != std::cmp::Ordering::Equal {
                            return if *asc { cmp } else { cmp.reverse() };
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        if projection_cols == vec!["*".to_string()] {
            let values: Vec<Vec<Value>> = rows.into_iter().map(|r| r.into_values()).collect();
            return Table::from_values(schema.clone(), values);
        }

        let mut projected_fields = Vec::new();
        let mut col_indices = Vec::new();
        for col in &projection_cols {
            if let Some(idx) = schema
                .fields()
                .iter()
                .position(|f| f.name.to_uppercase() == *col)
            {
                projected_fields.push(schema.fields()[idx].clone());
                col_indices.push(idx);
            } else {
                projected_fields.push(Field::nullable(col.clone(), DataType::Unknown));
                col_indices.push(usize::MAX);
            }
        }

        let projected_rows: Vec<Vec<Value>> = rows
            .into_iter()
            .map(|row| {
                col_indices
                    .iter()
                    .map(|&idx| {
                        if idx == usize::MAX {
                            Value::null()
                        } else {
                            row.values().get(idx).cloned().unwrap_or(Value::null())
                        }
                    })
                    .collect()
            })
            .collect();

        let new_schema = Schema::from_fields(projected_fields);
        Table::from_values(new_schema, projected_rows)
    }

    fn extract_array_join(&self, sql: &str) -> (String, Option<ArrayJoinClause>, Option<String>) {
        let left_array_join_re = Regex::new(r"(?i)\bLEFT\s+ARRAY\s+JOIN\s+").unwrap();
        let array_join_re = Regex::new(r"(?i)\bARRAY\s+JOIN\s+").unwrap();

        let is_left = left_array_join_re.is_match(sql);

        let sql_upper = sql.to_uppercase();
        let array_join_pos = if is_left {
            sql_upper.find("LEFT ARRAY JOIN")
        } else {
            sql_upper.find("ARRAY JOIN")
        };

        if array_join_pos.is_none() {
            return (sql.to_string(), None, None);
        }

        let pos = array_join_pos.unwrap();
        let before = &sql[..pos];

        let after_keyword = if is_left {
            let skip = "LEFT ARRAY JOIN".len();
            &sql[pos + skip..]
        } else {
            let skip = "ARRAY JOIN".len();
            &sql[pos + skip..]
        };

        let clause_end_keywords = [
            "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT", "SETTINGS", "FORMAT",
        ];
        let mut clause_end = after_keyword.len();

        for keyword in &clause_end_keywords {
            if let Some(kw_pos) = after_keyword.to_uppercase().find(keyword)
                && kw_pos < clause_end
            {
                clause_end = kw_pos;
            }
        }

        let array_join_exprs_str = after_keyword[..clause_end].trim();
        let after_clause = &after_keyword[clause_end..];

        let expressions = self.parse_array_join_expressions(array_join_exprs_str);

        let from_subquery_re = Regex::new(r"(?i)\bFROM\s*\(").unwrap();
        let processed_sql = if from_subquery_re.is_match(before) {
            let from_pos = before.to_uppercase().find("FROM").unwrap_or(0);
            format!("SELECT * {}", &before[from_pos..].trim())
        } else {
            let from_re = Regex::new(r"(?i)\bFROM\s+(\w+)").unwrap();
            if let Some(caps) = from_re.captures(before) {
                let table_name = caps.get(1).map(|m| m.as_str()).unwrap_or("");
                format!("SELECT * FROM {}", table_name)
            } else {
                before.trim().to_string()
            }
        };

        let original_sql = sql.to_string();

        (
            processed_sql,
            Some(ArrayJoinClause {
                is_left,
                expressions,
            }),
            Some(original_sql),
        )
    }

    fn parse_array_join_expressions(&self, exprs_str: &str) -> Vec<ArrayJoinExpr> {
        static AS_RE: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"(?i)\s+AS\s+(\w+)\s*$").unwrap());

        let mut expressions = Vec::new();

        for expr_str in exprs_str.split(',') {
            let expr_str = expr_str.trim();
            if expr_str.is_empty() {
                continue;
            }

            if let Some(caps) = AS_RE.captures(expr_str) {
                let alias = caps.get(1).map(|m| m.as_str().to_string());
                let expr = AS_RE.replace(expr_str, "").to_string();
                expressions.push(ArrayJoinExpr { expr, alias });
            } else {
                expressions.push(ArrayJoinExpr {
                    expr: expr_str.to_string(),
                    alias: None,
                });
            }
        }

        expressions
    }

    fn apply_array_join(&self, table: Table, clause: &ArrayJoinClause) -> Result<Table> {
        let rows = table.to_records()?;
        let schema = table.schema();
        let evaluator = Evaluator::new(schema);

        let mut result_rows = Vec::new();
        let mut new_fields: Vec<Field> = schema.fields().to_vec();
        let mut element_types: Vec<DataType> = vec![DataType::Unknown; clause.expressions.len()];

        for expr in &clause.expressions {
            let alias = expr.alias.as_ref().unwrap_or(&expr.expr);
            new_fields.push(Field::nullable(alias.clone(), DataType::Unknown));
        }

        for row in &rows {
            let mut arrays: Vec<Vec<Value>> = Vec::new();
            let mut max_len = 0;

            for (expr_idx, expr) in clause.expressions.iter().enumerate() {
                let array_val = self.evaluate_array_expr(&expr.expr, schema, row, &evaluator)?;
                if let Some(arr) = array_val.as_array() {
                    if arr.len() > max_len {
                        max_len = arr.len();
                    }
                    if !arr.is_empty() && element_types[expr_idx] == DataType::Unknown {
                        element_types[expr_idx] = arr[0].data_type();
                    }
                    arrays.push(arr.to_vec());
                } else if array_val.is_null() {
                    arrays.push(vec![]);
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "ARRAY".to_string(),
                        actual: array_val.data_type().to_string(),
                    });
                }
            }

            if max_len == 0 {
                if clause.is_left {
                    let mut new_row_values = row.values().to_vec();
                    for _ in &clause.expressions {
                        new_row_values.push(Value::null());
                    }
                    result_rows.push(Record::from_values(new_row_values));
                }
                continue;
            }

            for i in 0..max_len {
                let mut new_row_values = row.values().to_vec();
                for arr in &arrays {
                    let val = arr.get(i).cloned().unwrap_or_else(Value::null);
                    new_row_values.push(val);
                }
                result_rows.push(Record::from_values(new_row_values));
            }
        }

        for (i, element_type) in element_types.iter().enumerate() {
            let field_idx = schema.fields().len() + i;
            if field_idx < new_fields.len() {
                new_fields[field_idx] =
                    Field::nullable(new_fields[field_idx].name.clone(), element_type.clone());
            }
        }

        let new_schema = Schema::from_fields(new_fields);
        let values: Vec<Vec<Value>> = result_rows.into_iter().map(|r| r.into_values()).collect();
        Table::from_values(new_schema, values)
    }

    fn evaluate_array_expr(
        &self,
        expr_str: &str,
        schema: &Schema,
        row: &Record,
        evaluator: &Evaluator,
    ) -> Result<Value> {
        let col_name = expr_str.trim().to_uppercase();
        if let Some(idx) = schema
            .fields()
            .iter()
            .position(|f| f.name.to_uppercase() == col_name)
        {
            return Ok(row.values().get(idx).cloned().unwrap_or(Value::null()));
        }

        if let Some(idx) = schema
            .fields()
            .iter()
            .position(|f| f.name.to_uppercase().ends_with(&format!(".{}", col_name)))
        {
            return Ok(row.values().get(idx).cloned().unwrap_or(Value::null()));
        }

        let dialect = ClickHouseDialect {};
        let parsed = Parser::parse_sql(&dialect, &format!("SELECT {}", expr_str));
        if let Ok(stmts) = parsed
            && let Some(Statement::Query(query)) = stmts.into_iter().next()
            && let SetExpr::Select(select) = *query.body
            && let Some(SelectItem::UnnamedExpr(expr)) = select.projection.into_iter().next()
        {
            return evaluator.evaluate(&expr, row);
        }

        Err(Error::ColumnNotFound(expr_str.to_string()))
    }

    fn execute_statement(&mut self, stmt: &Statement) -> Result<Table> {
        match stmt {
            Statement::Query(query) => self.execute_query(query),
            Statement::CreateTable(create) => self.execute_create_table(create),
            Statement::Drop {
                object_type,
                names,
                if_exists,
                ..
            } => self.execute_drop(object_type, names, *if_exists),
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => self.execute_update(table, assignments, selection.as_ref()),
            Statement::Delete(delete) => self.execute_delete(delete),
            Statement::Truncate { table_names, .. } => self.execute_truncate(table_names),
            _ => Err(Error::UnsupportedFeature(format!(
                "Statement type not yet supported: {:?}",
                stmt
            ))),
        }
    }

    fn execute_query(&self, query: &Query) -> Result<Table> {
        let cte_tables = self.materialize_ctes(&query.with)?;
        self.execute_query_with_ctes(query, &cte_tables)
    }

    fn materialize_ctes(&self, with_clause: &Option<ast::With>) -> Result<HashMap<String, Table>> {
        let mut cte_tables = HashMap::new();
        if let Some(with) = with_clause {
            for cte in &with.cte_tables {
                let name = cte.alias.name.value.to_uppercase();
                let cte_result = self.execute_query_with_ctes(&cte.query, &cte_tables)?;
                cte_tables.insert(name, cte_result);
            }
        }
        Ok(cte_tables)
    }

    fn execute_query_with_ctes(
        &self,
        query: &Query,
        cte_tables: &HashMap<String, Table>,
    ) -> Result<Table> {
        match query.body.as_ref() {
            SetExpr::Select(select) => self.execute_select_with_ctes(
                select,
                &query.order_by,
                &query.limit_clause,
                cte_tables,
            ),
            SetExpr::Values(values) => self.execute_values(values),
            _ => Err(Error::UnsupportedFeature(format!(
                "Query type not yet supported: {:?}",
                query.body
            ))),
        }
    }

    fn execute_select_with_ctes(
        &self,
        select: &Select,
        order_by: &Option<OrderBy>,
        limit_clause: &Option<LimitClause>,
        cte_tables: &HashMap<String, Table>,
    ) -> Result<Table> {
        let (input_schema, input_rows) = self.get_from_data_ctes(&select.from, cte_tables)?;
        let evaluator = Evaluator::new(&input_schema);

        let filtered_rows: Vec<Record> = if let Some(selection) = &select.selection {
            input_rows
                .iter()
                .filter(|row| evaluator.evaluate_to_bool(selection, row).unwrap_or(false))
                .cloned()
                .collect()
        } else {
            input_rows.clone()
        };

        let (output_schema, output_rows) =
            self.project_rows(&input_schema, &filtered_rows, &select.projection)?;

        let mut rows = output_rows;

        if let Some(order_by) = order_by {
            self.sort_rows(&output_schema, &mut rows, order_by)?;
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

        let values: Vec<Vec<Value>> = rows.into_iter().map(|r| r.into_values()).collect();
        Table::from_values(output_schema, values)
    }

    fn get_from_data_ctes(
        &self,
        from: &[TableWithJoins],
        cte_tables: &HashMap<String, Table>,
    ) -> Result<(Schema, Vec<Record>)> {
        if from.is_empty() {
            return Ok((Schema::new(), vec![Record::from_values(vec![])]));
        }

        let first_table = &from[0];
        let (mut schema, mut rows) =
            self.get_table_factor_data_ctes(&first_table.relation, cte_tables)?;

        for join in &first_table.joins {
            let (right_schema, right_rows) =
                self.get_table_factor_data_ctes(&join.relation, cte_tables)?;
            (schema, rows) = self.execute_join(
                &schema,
                &rows,
                &right_schema,
                &right_rows,
                &join.join_operator,
            )?;
        }

        Ok((schema, rows))
    }

    fn get_table_factor_data_ctes(
        &self,
        table_factor: &TableFactor,
        cte_tables: &HashMap<String, Table>,
    ) -> Result<(Schema, Vec<Record>)> {
        match table_factor {
            TableFactor::Table {
                name, alias, args, ..
            } => {
                let table_name = name.to_string();
                let table_name_upper = table_name.to_uppercase();

                if table_name_upper == "NUMBERS"
                    && let Some(table_args) = args
                {
                    let empty_schema = Schema::new();
                    let empty_record = Record::from_values(vec![]);
                    let evaluator = Evaluator::new(&empty_schema);

                    let n =
                        if let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr))) =
                            table_args.args.first()
                        {
                            evaluator
                                .evaluate(expr, &empty_record)?
                                .as_i64()
                                .unwrap_or(0)
                        } else {
                            0
                        };

                    let schema = Schema::from_fields(vec![Field::nullable(
                        "number".to_string(),
                        DataType::Int64,
                    )]);
                    let rows: Vec<Record> = (0..n)
                        .map(|i| Record::from_values(vec![Value::int64(i)]))
                        .collect();
                    return Ok((schema, rows));
                }

                let table_data = cte_tables
                    .get(&table_name_upper)
                    .cloned()
                    .or_else(|| self.catalog.get_table(&table_name).cloned())
                    .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

                let schema = if let Some(alias) = alias {
                    let prefix = &alias.name.value;
                    Schema::from_fields(
                        table_data
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| {
                                Field::nullable(
                                    format!("{}.{}", prefix, f.name),
                                    f.data_type.clone(),
                                )
                            })
                            .collect(),
                    )
                } else {
                    table_data.schema().clone()
                };

                Ok((schema, table_data.to_records()?))
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let result = self.execute_query(subquery)?;
                let rows = result.rows()?;
                let schema = if let Some(alias) = alias {
                    Schema::from_fields(
                        result
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| {
                                Field::nullable(
                                    format!("{}.{}", alias.name.value, f.name),
                                    f.data_type.clone(),
                                )
                            })
                            .collect(),
                    )
                } else {
                    result.schema().clone()
                };
                Ok((schema, rows))
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Table factor not yet supported: {:?}",
                table_factor
            ))),
        }
    }

    fn execute_join(
        &self,
        left_schema: &Schema,
        left_rows: &[Record],
        right_schema: &Schema,
        right_rows: &[Record],
        join_op: &ast::JoinOperator,
    ) -> Result<(Schema, Vec<Record>)> {
        let combined_schema = Schema::from_fields(
            left_schema
                .fields()
                .iter()
                .chain(right_schema.fields().iter())
                .cloned()
                .collect(),
        );

        match join_op {
            ast::JoinOperator::Inner(constraint) | ast::JoinOperator::Join(constraint) => {
                self.execute_inner_join(&combined_schema, left_rows, right_rows, constraint)
            }
            ast::JoinOperator::CrossJoin(_) => {
                self.execute_cross_join(&combined_schema, left_rows, right_rows)
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Join type not yet supported: {:?}",
                join_op
            ))),
        }
    }

    fn execute_inner_join(
        &self,
        combined_schema: &Schema,
        left_rows: &[Record],
        right_rows: &[Record],
        constraint: &ast::JoinConstraint,
    ) -> Result<(Schema, Vec<Record>)> {
        let evaluator = Evaluator::new(combined_schema);
        let mut result_rows = Vec::new();

        for left_record in left_rows {
            for right_record in right_rows {
                let combined_values: Vec<Value> = left_record
                    .values()
                    .iter()
                    .chain(right_record.values().iter())
                    .cloned()
                    .collect();
                let combined_record = Record::from_values(combined_values);

                let matches = match constraint {
                    ast::JoinConstraint::On(expr) => {
                        evaluator.evaluate_to_bool(expr, &combined_record)?
                    }
                    ast::JoinConstraint::None => true,
                    _ => {
                        return Err(Error::UnsupportedFeature(
                            "Join constraint not supported".to_string(),
                        ));
                    }
                };

                if matches {
                    result_rows.push(combined_record);
                }
            }
        }

        Ok((combined_schema.clone(), result_rows))
    }

    fn execute_cross_join(
        &self,
        combined_schema: &Schema,
        left_rows: &[Record],
        right_rows: &[Record],
    ) -> Result<(Schema, Vec<Record>)> {
        let mut result_rows = Vec::new();

        for left_record in left_rows {
            for right_record in right_rows {
                let combined_values: Vec<Value> = left_record
                    .values()
                    .iter()
                    .chain(right_record.values().iter())
                    .cloned()
                    .collect();
                result_rows.push(Record::from_values(combined_values));
            }
        }

        Ok((combined_schema.clone(), result_rows))
    }

    fn project_rows(
        &self,
        input_schema: &Schema,
        rows: &[Record],
        items: &[SelectItem],
    ) -> Result<(Schema, Vec<Record>)> {
        let evaluator = Evaluator::new(input_schema);
        let mut result_rows = Vec::new();
        let mut output_fields = Vec::new();
        let mut first_row = true;

        for row in rows {
            let mut result_values = Vec::new();

            for (idx, item) in items.iter().enumerate() {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        let val = evaluator.evaluate(expr, row)?;
                        if first_row {
                            let name = self.expr_to_alias(expr, idx);
                            output_fields.push(Field::nullable(name, val.data_type()));
                        }
                        result_values.push(val);
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let val = evaluator.evaluate(expr, row)?;
                        if first_row {
                            output_fields
                                .push(Field::nullable(alias.value.clone(), val.data_type()));
                        }
                        result_values.push(val);
                    }
                    SelectItem::Wildcard(_) => {
                        for (i, field) in input_schema.fields().iter().enumerate() {
                            let val = row.values().get(i).cloned().unwrap_or(Value::null());
                            if first_row {
                                output_fields.push(field.clone());
                            }
                            result_values.push(val);
                        }
                    }
                    _ => {}
                }
            }

            first_row = false;
            result_rows.push(Record::from_values(result_values));
        }

        let output_schema = Schema::from_fields(output_fields);
        Ok((output_schema, result_rows))
    }

    fn sort_rows(&self, schema: &Schema, rows: &mut [Record], order_by: &OrderBy) -> Result<()> {
        let evaluator = Evaluator::new(schema);

        let exprs = match &order_by.kind {
            OrderByKind::Expressions(exprs) => exprs,
            OrderByKind::All(_) => return Ok(()),
        };

        rows.sort_by(|a, b| {
            for order_expr in exprs {
                let a_val = evaluator
                    .evaluate(&order_expr.expr, a)
                    .unwrap_or(Value::null());
                let b_val = evaluator
                    .evaluate(&order_expr.expr, b)
                    .unwrap_or(Value::null());

                let asc = order_expr.options.asc.unwrap_or(true);
                let cmp = self.compare_values(&a_val, &b_val);

                if cmp != std::cmp::Ordering::Equal {
                    return if asc { cmp } else { cmp.reverse() };
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

        if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
            return ai.cmp(&bi);
        }
        if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
            return af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal);
        }
        if let (Some(as_), Some(bs)) = (a.as_str(), b.as_str()) {
            return as_.cmp(bs);
        }

        std::cmp::Ordering::Equal
    }

    fn expr_to_alias(&self, expr: &Expr, idx: usize) -> String {
        match expr {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(parts) => parts
                .last()
                .map(|p| p.value.clone())
                .unwrap_or_else(|| format!("col{}", idx)),
            Expr::Function(func) => func.name.to_string(),
            _ => format!("col{}", idx),
        }
    }

    fn evaluate_literal_expr(&self, expr: &Expr) -> Result<Value> {
        match expr {
            Expr::Value(v) => match &v.value {
                SqlValue::Number(n, _) => {
                    if let Ok(i) = n.parse::<i64>() {
                        Ok(Value::int64(i))
                    } else if let Ok(f) = n.parse::<f64>() {
                        Ok(Value::float64(f))
                    } else {
                        Ok(Value::string(n.clone()))
                    }
                }
                SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                    Ok(Value::string(s.clone()))
                }
                SqlValue::Boolean(b) => Ok(Value::bool_val(*b)),
                SqlValue::Null => Ok(Value::null()),
                _ => Ok(Value::null()),
            },
            _ => Ok(Value::null()),
        }
    }

    fn execute_values(&self, values: &ast::Values) -> Result<Table> {
        let empty_schema = Schema::new();
        let empty_record = Record::from_values(vec![]);
        let evaluator = Evaluator::new(&empty_schema);

        let mut all_rows = Vec::new();
        let mut fields: Option<Vec<Field>> = None;

        for row in &values.rows {
            let mut row_values = Vec::new();
            for (i, expr) in row.iter().enumerate() {
                let val = evaluator.evaluate(expr, &empty_record)?;
                if fields.is_none() {
                    let mut new_fields = Vec::new();
                    for _ in 0..row.len() {
                        new_fields.push(Field::nullable(
                            format!("column{}", i + 1),
                            DataType::Unknown,
                        ));
                    }
                    fields = Some(new_fields);
                }
                row_values.push(val);
            }
            all_rows.push(row_values);
        }

        let schema = Schema::from_fields(fields.unwrap_or_default());
        Table::from_values(schema, all_rows)
    }

    fn execute_create_table(&mut self, create: &ast::CreateTable) -> Result<Table> {
        let table_name = create.name.to_string();
        let fields: Vec<Field> = create
            .columns
            .iter()
            .map(|col| {
                let data_type = self.sql_type_to_data_type(&col.data_type);
                Field::nullable(col.name.value.clone(), data_type)
            })
            .collect();

        let schema = Schema::from_fields(fields);

        if create.if_not_exists && self.catalog.get_table(&table_name).is_some() {
            return Table::from_values(Schema::new(), vec![]);
        }

        self.catalog.create_table(&table_name, schema)?;
        Table::from_values(Schema::new(), vec![])
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
                    if if_exists && self.catalog.get_table(&table_name).is_none() {
                        continue;
                    }
                    self.catalog.drop_table(&table_name)?;
                }
                Table::from_values(Schema::new(), vec![])
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "DROP not supported for {:?}",
                object_type
            ))),
        }
    }

    fn execute_insert(&mut self, insert: &ast::Insert) -> Result<Table> {
        let table_name = insert.table.to_string();
        let table = self
            .catalog
            .get_table(&table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?
            .clone();

        let schema = table.schema().clone();

        let column_indices: Vec<usize> = if insert.columns.is_empty() {
            (0..schema.field_count()).collect()
        } else {
            insert
                .columns
                .iter()
                .map(|col| {
                    schema
                        .fields()
                        .iter()
                        .position(|f| f.name.to_uppercase() == col.value.to_uppercase())
                        .ok_or_else(|| Error::ColumnNotFound(col.value.clone()))
                })
                .collect::<Result<Vec<_>>>()?
        };

        let source = insert
            .source
            .as_ref()
            .ok_or_else(|| Error::ParseError("INSERT requires a source".to_string()))?;

        let mut new_rows: Vec<Vec<Value>> = table
            .to_records()?
            .into_iter()
            .map(|r| r.into_values())
            .collect();

        match source.body.as_ref() {
            SetExpr::Values(values) => {
                let empty_schema = Schema::new();
                let empty_record = Record::from_values(vec![]);
                let evaluator = Evaluator::new(&empty_schema);

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
                        let val = evaluator.evaluate(&row_exprs[expr_idx], &empty_record)?;
                        row_values[col_idx] = val;
                    }
                    new_rows.push(row_values);
                }
            }
            _ => {
                let source_table = self.execute_query(source)?;
                let source_rows = source_table.to_records()?;
                for row in source_rows {
                    new_rows.push(row.into_values());
                }
            }
        }

        let new_table = Table::from_values(schema, new_rows)?;
        self.catalog.replace_table(&table_name, new_table)?;

        Table::from_values(Schema::new(), vec![])
    }

    fn execute_update(
        &mut self,
        _table: &TableWithJoins,
        _assignments: &[ast::Assignment],
        _selection: Option<&Expr>,
    ) -> Result<Table> {
        Err(Error::UnsupportedFeature(
            "UPDATE not yet implemented".to_string(),
        ))
    }

    fn execute_delete(&mut self, _delete: &ast::Delete) -> Result<Table> {
        Err(Error::UnsupportedFeature(
            "DELETE not yet implemented".to_string(),
        ))
    }

    fn execute_truncate(&mut self, table_names: &[ast::TruncateTableTarget]) -> Result<Table> {
        for target in table_names {
            let table_name = target.name.to_string();
            let table = self
                .catalog
                .get_table(&table_name)
                .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

            let empty_table = Table::from_values(table.schema().clone(), vec![])?;
            self.catalog.replace_table(&table_name, empty_table)?;
        }
        Table::from_values(Schema::new(), vec![])
    }

    fn sql_type_to_data_type(&self, sql_type: &ast::DataType) -> DataType {
        sql_type_to_data_type(sql_type)
    }
}

fn sql_type_to_data_type(sql_type: &ast::DataType) -> DataType {
    match sql_type {
        ast::DataType::Boolean | ast::DataType::Bool => DataType::Bool,
        ast::DataType::Int64 | ast::DataType::BigInt(_) => DataType::Int64,
        ast::DataType::Int32 | ast::DataType::Int(_) | ast::DataType::Integer(_) => DataType::Int64,
        ast::DataType::Float64 | ast::DataType::Double(_) => DataType::Float64,
        ast::DataType::String(_) | ast::DataType::Varchar(_) | ast::DataType::Text => {
            DataType::String
        }
        ast::DataType::Array(inner) => {
            let element_type = match inner {
                ast::ArrayElemTypeDef::AngleBracket(dt) => sql_type_to_data_type(dt),
                ast::ArrayElemTypeDef::SquareBracket(dt, _) => sql_type_to_data_type(dt),
                ast::ArrayElemTypeDef::Parenthesis(dt) => sql_type_to_data_type(dt),
                ast::ArrayElemTypeDef::None => DataType::Unknown,
            };
            DataType::Array(Box::new(element_type))
        }
        _ => DataType::Unknown,
    }
}

impl Default for ClickHouseExecutor {
    fn default() -> Self {
        Self::new()
    }
}
