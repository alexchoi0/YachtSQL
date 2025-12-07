use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};
use yachtsql_optimizer::expr::Expr;
use yachtsql_storage::{Row, Schema};

use crate::Table;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReturningColumnOrigin {
    Target,
    Source,
    Table(String),
    Expression,
    Old,
    New,
}

#[derive(Debug, Clone)]
pub struct ReturningColumn {
    pub source_name: String,
    pub output_name: String,
    pub origin: ReturningColumnOrigin,
}

#[derive(Debug, Clone)]
pub struct ReturningExpressionItem {
    pub expr: Expr,
    pub output_name: Option<String>,
    pub data_type: DataType,
    pub origin: ReturningColumnOrigin,
}

#[derive(Debug, Clone)]
pub struct ReturningAstItem {
    pub item: sqlparser::ast::SelectItem,
    pub output_name: String,
}

#[derive(Debug, Clone)]
pub enum ReturningSpec {
    None,
    AllColumns,
    Columns(Vec<ReturningColumn>),
    Expressions(Vec<ReturningExpressionItem>),
    AstItems(Vec<ReturningAstItem>),
}

pub fn build_returning_batch_from_rows(
    spec: &ReturningSpec,
    rows: &[Vec<Value>],
    table_schema: &Schema,
) -> Result<Table> {
    match spec {
        ReturningSpec::None => Ok(Table::empty(Schema::from_fields(vec![]))),
        ReturningSpec::AllColumns => {
            if rows.is_empty() {
                return Ok(Table::empty(table_schema.clone()));
            }
            Table::from_values(table_schema.clone(), rows.to_vec())
        }
        ReturningSpec::Columns(columns) => {
            let output_schema = returning_spec_output_schema(spec, table_schema, None)?;

            if rows.is_empty() {
                return Ok(Table::empty(output_schema));
            }

            let mut output_rows = Vec::with_capacity(rows.len());
            for row in rows {
                let mut output_values = Vec::with_capacity(columns.len());
                for column in columns {
                    let value = if let Some(idx) = table_schema.field_index(&column.source_name) {
                        row.get(idx).cloned().unwrap_or(Value::null())
                    } else {
                        Value::null()
                    };
                    output_values.push(value);
                }
                output_rows.push(output_values);
            }

            Table::from_values(output_schema, output_rows)
        }
        ReturningSpec::Expressions(_items) => {
            let output_schema = returning_spec_output_schema(spec, table_schema, None)?;

            if rows.is_empty() {
                return Ok(Table::empty(output_schema));
            }

            Err(Error::unsupported_feature(
                "RETURNING with expressions requires evaluation context - use dedicated expression evaluator".to_string(),
            ))
        }
        ReturningSpec::AstItems(_) => {
            Err(Error::InternalError(
                "RETURNING with AST items requires evaluation context - use build_returning_batch_with_ast_eval".to_string(),
            ))
        }
    }
}

pub fn returning_spec_output_schema(
    spec: &ReturningSpec,
    table_schema: &Schema,
    source_schema: Option<&Schema>,
) -> Result<Schema> {
    match spec {
        ReturningSpec::None => Ok(Schema::from_fields(vec![])),
        ReturningSpec::AllColumns => Ok(table_schema.clone()),
        ReturningSpec::Columns(cols) => {
            let mut fields = vec![];
            for col in cols {
                let source_schema = match col.origin {
                    ReturningColumnOrigin::Target => table_schema,
                    ReturningColumnOrigin::Source => source_schema.ok_or_else(|| {
                        Error::InternalError(
                            "Source schema required for MERGE RETURNING".to_string(),
                        )
                    })?,
                    _ => table_schema,
                };

                if let Some(field) = source_schema.field(&col.source_name) {
                    let mut output_field = field.clone();

                    if col.output_name != col.source_name {
                        output_field.name = col.output_name.clone();
                    }
                    fields.push(output_field);
                } else {
                    return Err(Error::column_not_found(col.source_name.clone()));
                }
            }
            Ok(Schema::from_fields(fields))
        }
        ReturningSpec::Expressions(exprs) => {
            let fields = exprs
                .iter()
                .enumerate()
                .map(|(idx, item)| {
                    let name = item
                        .output_name
                        .clone()
                        .unwrap_or_else(|| format!("column{}", idx + 1));
                    yachtsql_storage::Field::nullable(name, item.data_type.clone())
                })
                .collect();
            Ok(Schema::from_fields(fields))
        }
        ReturningSpec::AstItems(items) => {
            let fields = items
                .iter()
                .map(|item| {
                    yachtsql_storage::Field::nullable(item.output_name.clone(), DataType::String)
                })
                .collect();
            Ok(Schema::from_fields(fields))
        }
    }
}

pub fn build_returning_batch_with_ast_eval<E>(
    spec: &ReturningSpec,
    rows: &[Vec<Value>],
    table_schema: &Schema,
    evaluator: &E,
) -> Result<Table>
where
    E: AstExprEvaluator,
{
    match spec {
        ReturningSpec::AstItems(items) => {
            if rows.is_empty() {
                let fields: Vec<_> = items
                    .iter()
                    .map(|item| {
                        yachtsql_storage::Field::nullable(
                            item.output_name.clone(),
                            DataType::String,
                        )
                    })
                    .collect();
                return Ok(Table::empty(Schema::from_fields(fields)));
            }

            let mut output_rows = Vec::with_capacity(rows.len());
            let mut inferred_types: Vec<Option<DataType>> = vec![None; items.len()];

            for row_values in rows {
                let row = Row::from_values(row_values.clone());
                let mut output_values = Vec::with_capacity(items.len());

                for (idx, ast_item) in items.iter().enumerate() {
                    let expr = match &ast_item.item {
                        sqlparser::ast::SelectItem::UnnamedExpr(e) => e,
                        sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => expr,
                        _ => {
                            return Err(Error::InternalError(
                                "Unexpected SelectItem type in RETURNING".to_string(),
                            ));
                        }
                    };

                    let value = evaluator.evaluate_ast_expr(expr, &row, table_schema)?;

                    if inferred_types[idx].is_none() && !value.is_null() {
                        inferred_types[idx] = Some(value.data_type());
                    }

                    output_values.push(value);
                }
                output_rows.push(output_values);
            }

            let fields: Vec<_> = items
                .iter()
                .enumerate()
                .map(|(idx, item)| {
                    let data_type = inferred_types[idx].clone().unwrap_or(DataType::String);
                    yachtsql_storage::Field::nullable(item.output_name.clone(), data_type)
                })
                .collect();

            Table::from_values(Schema::from_fields(fields), output_rows)
        }

        _ => build_returning_batch_from_rows(spec, rows, table_schema),
    }
}

pub trait AstExprEvaluator {
    fn evaluate_ast_expr(
        &self,
        expr: &sqlparser::ast::Expr,
        row: &Row,
        schema: &Schema,
    ) -> Result<Value>;
}

pub struct ReturningExprEvaluator<'a> {
    pub schema: &'a Schema,
}

impl<'a> ReturningExprEvaluator<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self { schema }
    }
}

impl<'a> AstExprEvaluator for ReturningExprEvaluator<'a> {
    fn evaluate_ast_expr(
        &self,
        expr: &sqlparser::ast::Expr,
        row: &Row,
        _schema: &Schema,
    ) -> Result<Value> {
        use crate::query_executor::expression_evaluator::ExpressionEvaluator;
        let evaluator = ExpressionEvaluator::new(self.schema);
        evaluator.evaluate_expr(expr, row)
    }
}

#[derive(Debug, Clone)]
pub struct DmlRowContext {
    pub old_row: Option<Vec<Value>>,

    pub new_row: Option<Vec<Value>>,
}

impl DmlRowContext {
    pub fn for_insert(new_row: Vec<Value>) -> Self {
        Self {
            old_row: None,
            new_row: Some(new_row),
        }
    }

    pub fn for_update(old_row: Vec<Value>, new_row: Vec<Value>) -> Self {
        Self {
            old_row: Some(old_row),
            new_row: Some(new_row),
        }
    }

    pub fn for_delete(old_row: Vec<Value>) -> Self {
        Self {
            old_row: Some(old_row),
            new_row: None,
        }
    }
}

pub struct OldNewExprEvaluator<'a> {
    pub schema: &'a Schema,
}

impl<'a> OldNewExprEvaluator<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self { schema }
    }

    pub fn evaluate_with_context(
        &self,
        expr: &sqlparser::ast::Expr,
        context: &DmlRowContext,
    ) -> Result<Value> {
        use sqlparser::ast::Expr as SqlExpr;

        use crate::query_executor::expression_evaluator::ExpressionEvaluator;

        match expr {
            SqlExpr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let qualifier = parts[0].value.to_uppercase();
                let column = &parts[1].value;

                if qualifier == "OLD" {
                    return self.get_column_value(column, &context.old_row, "OLD");
                } else if qualifier == "NEW" {
                    return self.get_column_value(column, &context.new_row, "NEW");
                }
            }

            SqlExpr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_with_context(left, context)?;
                let right_val = self.evaluate_with_context(right, context)?;

                use sqlparser::ast::BinaryOperator;
                match op {
                    BinaryOperator::Plus => {
                        if let (Some(l), Some(r)) = (left_val.as_i64(), right_val.as_i64()) {
                            return Ok(Value::int64(l + r));
                        }
                        if let (Some(l), Some(r)) = (left_val.as_f64(), right_val.as_f64()) {
                            return Ok(Value::float64(l + r));
                        }
                    }
                    BinaryOperator::Minus => {
                        if let (Some(l), Some(r)) = (left_val.as_i64(), right_val.as_i64()) {
                            return Ok(Value::int64(l - r));
                        }
                        if let (Some(l), Some(r)) = (left_val.as_f64(), right_val.as_f64()) {
                            return Ok(Value::float64(l - r));
                        }
                    }
                    BinaryOperator::Multiply => {
                        if let (Some(l), Some(r)) = (left_val.as_i64(), right_val.as_i64()) {
                            return Ok(Value::int64(l * r));
                        }
                        if let (Some(l), Some(r)) = (left_val.as_f64(), right_val.as_f64()) {
                            return Ok(Value::float64(l * r));
                        }
                    }
                    BinaryOperator::Divide => {
                        if let (Some(l), Some(r)) = (left_val.as_f64(), right_val.as_f64()) {
                            if r != 0.0 {
                                return Ok(Value::float64(l / r));
                            }
                        }
                    }
                    _ => {}
                }

                let row_values = context
                    .new_row
                    .as_ref()
                    .or(context.old_row.as_ref())
                    .ok_or_else(|| Error::InternalError("No row data available".to_string()))?;
                let row = Row::from_values(row_values.clone());
                let evaluator = ExpressionEvaluator::new(self.schema);
                return evaluator.evaluate_expr(expr, &row);
            }

            SqlExpr::Nested(inner) => {
                return self.evaluate_with_context(inner, context);
            }
            _ => {}
        }

        let row_values = context
            .new_row
            .as_ref()
            .or(context.old_row.as_ref())
            .ok_or_else(|| Error::InternalError("No row data available".to_string()))?;

        let row = Row::from_values(row_values.clone());
        let evaluator = ExpressionEvaluator::new(self.schema);
        evaluator.evaluate_expr(expr, &row)
    }

    fn get_column_value(
        &self,
        column: &str,
        row_opt: &Option<Vec<Value>>,
        context_name: &str,
    ) -> Result<Value> {
        let row_values = row_opt.as_ref().ok_or_else(|| {
            Error::InvalidQuery(format!(
                "{} row not available in this DML operation",
                context_name
            ))
        })?;

        let idx = self
            .schema
            .field_index(column)
            .ok_or_else(|| Error::column_not_found(column.to_string()))?;

        Ok(row_values.get(idx).cloned().unwrap_or(Value::null()))
    }
}

pub fn build_returning_batch_with_old_new(
    spec: &ReturningSpec,
    contexts: &[DmlRowContext],
    table_schema: &Schema,
) -> Result<Table> {
    match spec {
        ReturningSpec::None => Ok(Table::empty(Schema::from_fields(vec![]))),

        ReturningSpec::AllColumns => {
            if contexts.is_empty() {
                return Ok(Table::empty(table_schema.clone()));
            }

            let rows: Vec<Vec<Value>> = contexts
                .iter()
                .map(|ctx| {
                    ctx.new_row
                        .clone()
                        .or_else(|| ctx.old_row.clone())
                        .unwrap_or_default()
                })
                .collect();

            Table::from_values(table_schema.clone(), rows)
        }

        ReturningSpec::AstItems(items) => {
            if contexts.is_empty() {
                let fields: Vec<_> = items
                    .iter()
                    .map(|item| {
                        yachtsql_storage::Field::nullable(
                            item.output_name.clone(),
                            DataType::String,
                        )
                    })
                    .collect();
                return Ok(Table::empty(Schema::from_fields(fields)));
            }

            let evaluator = OldNewExprEvaluator::new(table_schema);
            let mut output_rows = Vec::with_capacity(contexts.len());
            let mut inferred_types: Vec<Option<DataType>> = vec![None; items.len()];

            for ctx in contexts {
                let mut output_values = Vec::with_capacity(items.len());

                for (idx, ast_item) in items.iter().enumerate() {
                    let expr = match &ast_item.item {
                        sqlparser::ast::SelectItem::UnnamedExpr(e) => e,
                        sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => expr,
                        sqlparser::ast::SelectItem::QualifiedWildcard(kind, _) => {
                            let qualifier = match kind {
                                sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(
                                    name,
                                ) => name.to_string().to_uppercase(),
                                _ => {
                                    return Err(Error::unsupported_feature(
                                        "Expression-qualified wildcard not supported".to_string(),
                                    ));
                                }
                            };

                            let row_values = if qualifier == "OLD" {
                                ctx.old_row.as_ref().ok_or_else(|| {
                                    Error::InvalidQuery(
                                        "OLD row not available in this DML operation".to_string(),
                                    )
                                })?
                            } else if qualifier == "NEW" {
                                ctx.new_row.as_ref().ok_or_else(|| {
                                    Error::InvalidQuery(
                                        "NEW row not available in this DML operation".to_string(),
                                    )
                                })?
                            } else {
                                ctx.new_row
                                    .as_ref()
                                    .or(ctx.old_row.as_ref())
                                    .ok_or_else(|| {
                                        Error::InternalError("No row data available".to_string())
                                    })?
                            };

                            for (field_idx, _field) in table_schema.fields().iter().enumerate() {
                                let value =
                                    row_values.get(field_idx).cloned().unwrap_or(Value::null());
                                output_values.push(value.clone());
                                if inferred_types.len() <= idx + field_idx {
                                    inferred_types.push(Some(value.data_type()));
                                } else if inferred_types[idx + field_idx].is_none()
                                    && !value.is_null()
                                {
                                    inferred_types[idx + field_idx] = Some(value.data_type());
                                }
                            }
                            continue;
                        }
                        _ => {
                            return Err(Error::unsupported_feature(
                                "Unsupported RETURNING item".to_string(),
                            ));
                        }
                    };

                    let value = evaluator.evaluate_with_context(expr, ctx)?;

                    if inferred_types[idx].is_none() && !value.is_null() {
                        inferred_types[idx] = Some(value.data_type());
                    }

                    output_values.push(value);
                }
                output_rows.push(output_values);
            }

            let fields: Vec<_> = items
                .iter()
                .enumerate()
                .flat_map(|(idx, item)| match &item.item {
                    sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => table_schema
                        .fields()
                        .iter()
                        .map(|f| {
                            yachtsql_storage::Field::nullable(f.name.clone(), f.data_type.clone())
                        })
                        .collect::<Vec<_>>(),
                    _ => {
                        let data_type = inferred_types
                            .get(idx)
                            .cloned()
                            .flatten()
                            .unwrap_or(DataType::String);
                        vec![yachtsql_storage::Field::nullable(
                            item.output_name.clone(),
                            data_type,
                        )]
                    }
                })
                .collect();

            Table::from_values(Schema::from_fields(fields), output_rows)
        }

        ReturningSpec::Columns(columns) => {
            let output_schema = returning_spec_output_schema(spec, table_schema, None)?;

            if contexts.is_empty() {
                return Ok(Table::empty(output_schema));
            }

            let mut output_rows = Vec::with_capacity(contexts.len());
            for ctx in contexts {
                let mut output_values = Vec::with_capacity(columns.len());
                for column in columns {
                    let row_values = match column.origin {
                        ReturningColumnOrigin::Old => ctx.old_row.as_ref().ok_or_else(|| {
                            Error::InvalidQuery(
                                "OLD row not available in this DML operation".to_string(),
                            )
                        })?,
                        ReturningColumnOrigin::New => ctx.new_row.as_ref().ok_or_else(|| {
                            Error::InvalidQuery(
                                "NEW row not available in this DML operation".to_string(),
                            )
                        })?,
                        _ => ctx
                            .new_row
                            .as_ref()
                            .or(ctx.old_row.as_ref())
                            .ok_or_else(|| {
                                Error::InternalError("No row data available".to_string())
                            })?,
                    };

                    let value = if let Some(idx) = table_schema.field_index(&column.source_name) {
                        row_values.get(idx).cloned().unwrap_or(Value::null())
                    } else {
                        Value::null()
                    };
                    output_values.push(value);
                }
                output_rows.push(output_values);
            }

            Table::from_values(output_schema, output_rows)
        }

        _ => {
            let rows: Vec<Vec<Value>> = contexts
                .iter()
                .filter_map(|ctx| ctx.new_row.clone().or_else(|| ctx.old_row.clone()))
                .collect();
            build_returning_batch_from_rows(spec, &rows, table_schema)
        }
    }
}
