use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;

use chrono::{Datelike, Timelike};
use debug_print::debug_eprintln;
use sqlparser::ast::{
    Expr, Fetch, GroupByExpr, JoinConstraint, JoinOperator, LimitClause, OrderBy, OrderByExpr,
    OrderByKind, Select, SelectItem, SetExpr, SetOperator, SetQuantifier, Statement, TableFactor,
    TableWithJoins,
};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};
use yachtsql_functions::FunctionRegistry;
use yachtsql_storage::{Column, Field, Row, Schema, TableIndexOps};

use super::super::QueryExecutor;
use super::super::aggregator::{AggregateSpec, Aggregator};
use super::super::expression_evaluator::ExpressionEvaluator;
use super::super::window_functions::{WindowFunction, WindowFunctionType};
use super::DdlExecutor;
use crate::Table;
use crate::information_schema::{InformationSchemaProvider, InformationSchemaTable};

pub trait QueryExecutorTrait {
    fn execute_select(&mut self, stmt: &Statement, _original_sql: &str) -> Result<Table>;

    fn scan_table(&self, dataset_id: &str, table_id: &str) -> Result<Vec<Row>>;

    fn project_rows(
        &self,
        rows: Vec<Row>,
        schema: &Schema,
        select_items: &[SelectItem],
    ) -> Result<(Schema, Vec<Row>)>;
}

pub(crate) trait ScanTableHelper {
    fn scan_table_helper(&self, dataset_id: &str, table_id: &str) -> Result<Vec<Row>>;
}

impl QueryExecutorTrait for QueryExecutor {
    fn execute_select(&mut self, stmt: &Statement, original_sql: &str) -> Result<Table> {
        let Statement::Query(query) = stmt else {
            return Err(Error::InternalError("Not a SELECT statement".to_string()));
        };

        self.execute_select_with_optimizer(query, original_sql)
    }

    fn scan_table(&self, dataset_id: &str, table_id: &str) -> Result<Vec<Row>> {
        let is_view = {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
            })?;
            dataset.views().exists(table_id)
        };

        if is_view {
            return Err(Error::UnsupportedFeature(format!(
                "View '{}' cannot be scanned directly. View expansion in subqueries not yet implemented.",
                table_id
            )));
        }

        let storage = self.storage.borrow_mut();
        let dataset = storage
            .get_dataset(dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;
        let table = dataset.get_table(table_id).ok_or_else(|| {
            Error::TableNotFound(format!("Table '{}.{}' not found", dataset_id, table_id))
        })?;

        Ok(table.get_all_rows())
    }

    fn project_rows(
        &self,
        rows: Vec<Row>,
        schema: &Schema,
        select_items: &[SelectItem],
    ) -> Result<(Schema, Vec<Row>)> {
        use yachtsql_core::types::DataType;

        use crate::query_executor::expression_evaluator::ExpressionEvaluator;

        let mut projected_fields: Vec<Field> = Vec::new();
        let mut projection_exprs: Vec<(Option<String>, Expr)> = Vec::new();

        for item in select_items {
            match item {
                SelectItem::Wildcard(_) => {
                    for field in schema.fields() {
                        projected_fields.push(field.clone());
                        projection_exprs.push((
                            Some(field.name.clone()),
                            Expr::Identifier(sqlparser::ast::Ident::new(&field.name)),
                        ));
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    let col_name = self.infer_column_name(expr);

                    let data_type = Self::infer_expression_type(expr, schema)?;
                    projected_fields.push(Field::nullable(col_name.clone(), data_type));
                    projection_exprs.push((Some(col_name), expr.clone()));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let col_name = alias.value.clone();

                    let data_type = Self::infer_expression_type(expr, schema)?;
                    projected_fields.push(Field::nullable(col_name.clone(), data_type));
                    projection_exprs.push((Some(col_name), expr.clone()));
                }
                SelectItem::QualifiedWildcard(kind, _) => {
                    use sqlparser::ast::SelectItemQualifiedWildcardKind;
                    let table_qualifier = match kind {
                        SelectItemQualifiedWildcardKind::ObjectName(name) => name.to_string(),
                        SelectItemQualifiedWildcardKind::Expr(expr) => expr.to_string(),
                    };

                    let has_source_tables =
                        schema.fields().iter().any(|f| f.source_table.is_some());

                    for field in schema.fields() {
                        let should_include = if has_source_tables {
                            field.source_table.as_ref() == Some(&table_qualifier)
                        } else {
                            true
                        };

                        if should_include {
                            projected_fields.push(field.clone());
                            projection_exprs.push((
                                Some(field.name.clone()),
                                Expr::Identifier(sqlparser::ast::Ident::new(&field.name)),
                            ));
                        }
                    }
                }
            }
        }

        let storage = self.storage.borrow_mut();

        let type_registry = storage
            .get_dataset("default")
            .map(|dataset| dataset.types());

        let mut evaluator = ExpressionEvaluator::new(schema).with_dialect(self.dialect());
        if let Some(tr) = type_registry {
            evaluator = evaluator.with_type_registry(tr);
        }

        let mut projected_rows: Vec<Row> = Vec::new();
        let mut first_row_types: Option<Vec<DataType>> = None;

        for row in rows {
            let mut values: Vec<Value> = Vec::new();
            for (col_idx, (_col_name, expr)) in projection_exprs.iter().enumerate() {
                let mut value = evaluator.evaluate_expr(expr, &row)?;

                let expected_type = &projected_fields[col_idx].data_type;
                if matches!(expected_type, DataType::Float64) {
                    if let Some(i) = value.as_i64() {
                        value = Value::float64(i as f64);
                    }
                }

                values.push(value);
            }

            if first_row_types.is_none() {
                first_row_types = Some(values.iter().map(|v| v.data_type()).collect());
            }

            projected_rows.push(Row::from_values(values));
        }

        if let Some(types) = first_row_types {
            for (idx, field) in projected_fields.iter_mut().enumerate() {
                if matches!(field.data_type, DataType::Unknown) {
                    field.data_type = types[idx].clone();
                }
            }
        }

        let projected_schema = Schema::from_fields(projected_fields);

        Ok((projected_schema, projected_rows))
    }
}

type WindowSelectItems = (
    Vec<SelectItem>,
    Vec<(String, super::super::window_functions::WindowFunction)>,
);

struct NestedLoopJoinParams<'a> {
    left_schema: &'a Schema,
    left_rows: Vec<Row>,
    right_schema: &'a Schema,
    right_rows: Vec<Row>,
    join_type: JoinType,
    join_condition: &'a Expr,
    left_table_name: &'a str,
    right_table_name: &'a str,
}

impl QueryExecutor {
    fn query_contains_subquery(query: &Box<sqlparser::ast::Query>) -> bool {
        match query.body.as_ref() {
            SetExpr::Select(select) => {
                for table_with_joins in &select.from {
                    if Self::table_factor_contains_subquery(&table_with_joins.relation) {
                        return true;
                    }

                    for join in &table_with_joins.joins {
                        if Self::table_factor_contains_subquery(&join.relation) {
                            return true;
                        }
                    }
                }

                for item in &select.projection {
                    if Self::select_item_contains_subquery(item) {
                        return true;
                    }
                }

                if let Some(ref selection) = select.selection {
                    if Self::expr_contains_subquery(selection) {
                        return true;
                    }
                }

                if let Some(ref having) = select.having {
                    if Self::expr_contains_subquery(having) {
                        return true;
                    }
                }

                false
            }
            SetExpr::Query(_) | SetExpr::SetOperation { .. } => true,
            _ => false,
        }
    }

    fn query_contains_grouping_extensions(query: &Box<sqlparser::ast::Query>) -> bool {
        match query.body.as_ref() {
            SetExpr::Select(select) => match &select.group_by {
                GroupByExpr::Expressions(exprs, _) => exprs
                    .iter()
                    .any(|expr| Self::expr_is_grouping_extension(expr)),
                GroupByExpr::All(_) => false,
            },
            _ => false,
        }
    }

    fn expr_is_grouping_extension(expr: &Expr) -> bool {
        match expr {
            Expr::Rollup(_) | Expr::Cube(_) | Expr::GroupingSets(_) => true,

            Expr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                matches!(func_name.as_str(), "ROLLUP" | "CUBE" | "GROUPING")
            }
            _ => false,
        }
    }

    fn query_has_distinct_on(query: &Box<sqlparser::ast::Query>) -> bool {
        match query.body.as_ref() {
            SetExpr::Select(select) => {
                matches!(select.distinct, Some(sqlparser::ast::Distinct::On(_)))
            }
            _ => false,
        }
    }

    fn query_contains_join(query: &Box<sqlparser::ast::Query>) -> bool {
        match query.body.as_ref() {
            SetExpr::Select(select) => {
                select
                    .from
                    .iter()
                    .any(|table_with_joins| !table_with_joins.joins.is_empty())
                    || select.from.len() > 1
            }
            _ => false,
        }
    }

    fn query_contains_tvf(query: &Box<sqlparser::ast::Query>) -> bool {
        match query.body.as_ref() {
            SetExpr::Select(select) => select.from.iter().any(|table_with_joins| {
                Self::table_factor_is_tvf(&table_with_joins.relation)
                    || table_with_joins
                        .joins
                        .iter()
                        .any(|join| Self::table_factor_is_tvf(&join.relation))
            }),
            _ => false,
        }
    }

    fn table_factor_is_tvf(table_factor: &TableFactor) -> bool {
        matches!(table_factor, TableFactor::Table { args: Some(_), .. })
    }

    fn table_factor_contains_subquery(table_factor: &TableFactor) -> bool {
        matches!(
            table_factor,
            TableFactor::Derived { .. } | TableFactor::UNNEST { .. }
        )
    }

    fn select_item_contains_subquery(item: &SelectItem) -> bool {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                Self::expr_contains_subquery(expr)
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => false,
        }
    }

    fn expr_contains_subquery(expr: &Expr) -> bool {
        match expr {
            Expr::Subquery(_) => true,
            Expr::Exists { .. } => true,
            Expr::InSubquery { .. } => true,
            Expr::AnyOp { right, .. } | Expr::AllOp { right, .. } => {
                Self::expr_contains_subquery(right)
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_contains_subquery(left) || Self::expr_contains_subquery(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::Nested(inner) => Self::expr_contains_subquery(inner),
            Expr::IsNull(inner)
            | Expr::IsNotNull(inner)
            | Expr::IsTrue(inner)
            | Expr::IsFalse(inner) => Self::expr_contains_subquery(inner),
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::expr_contains_subquery(expr)
                    || Self::expr_contains_subquery(low)
                    || Self::expr_contains_subquery(high)
            }
            Expr::InList { expr, list, .. } => {
                Self::expr_contains_subquery(expr)
                    || list.iter().any(|e| Self::expr_contains_subquery(e))
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                operand
                    .as_ref()
                    .is_some_and(|o| Self::expr_contains_subquery(o))
                    || conditions.iter().any(|c| {
                        Self::expr_contains_subquery(&c.condition)
                            || Self::expr_contains_subquery(&c.result)
                    })
                    || else_result
                        .as_ref()
                        .is_some_and(|e| Self::expr_contains_subquery(e))
            }
            Expr::Cast { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::Function(func) => {
                if let sqlparser::ast::FunctionArguments::List(args) = &func.args {
                    args.args.iter().any(|arg| match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        )
                        | sqlparser::ast::FunctionArg::Named {
                            arg: sqlparser::ast::FunctionArgExpr::Expr(e),
                            ..
                        } => Self::expr_contains_subquery(e),
                        _ => false,
                    })
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    fn execute_select_with_optimizer(
        &mut self,
        query: &Box<sqlparser::ast::Query>,
        original_sql: &str,
    ) -> Result<Table> {
        let resource_tracker =
            crate::resource_limits::ResourceTracker::new(self.resource_limits.clone());

        let (cte_cleanup, query_for_plan) = if let Some(ref with_clause) = query.with {
            let cte_names = self.process_ctes(with_clause)?;
            let mut modified_query = query.as_ref().clone();
            modified_query.with = None;
            for cte_name in &cte_names {
                let cte_table = format!("__cte_{}", cte_name);
                Self::rename_table_references(&mut modified_query.body, cte_name, &cte_table);
            }
            (Some(cte_names), Box::new(modified_query))
        } else {
            (None, query.clone())
        };

        let sql_hash = crate::sql_normalizer::hash_sql(original_sql, self.dialect());
        let cache_key = crate::plan_cache::PlanCacheKey::new(sql_hash, self.dialect());

        resource_tracker.check_timeout()?;

        let optimized_plan = {
            let session_vars = self.session.variables();
            let session_udfs = self.session.udfs_for_parser();
            let has_variables = !session_vars.is_empty();
            let has_udfs = !session_udfs.is_empty();

            let mut cache = self.plan_cache.borrow_mut();
            if !has_variables && !has_udfs {
                if let Some(cached) = cache.get(&cache_key) {
                    (*cached.plan).clone()
                } else {
                    drop(cache);

                    let plan_builder = yachtsql_parser::LogicalPlanBuilder::new()
                        .with_storage(Rc::clone(&self.storage))
                        .with_dialect(self.dialect());
                    let logical_plan = plan_builder.query_to_plan(&query_for_plan)?;

                    resource_tracker.check_timeout()?;

                    let optimized = self.optimizer.optimize(logical_plan)?;

                    resource_tracker.check_timeout()?;

                    let cached_plan = crate::plan_cache::CachedPlan::new(
                        optimized.root().clone(),
                        original_sql.to_string(),
                    );
                    self.plan_cache.borrow_mut().insert(cache_key, cached_plan);

                    optimized.root().clone()
                }
            } else {
                drop(cache);

                let parser_vars: std::collections::HashMap<
                    String,
                    yachtsql_parser::SessionVariable,
                > = session_vars
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            yachtsql_parser::SessionVariable {
                                data_type: v.data_type.clone(),
                                value: v.value.clone(),
                            },
                        )
                    })
                    .collect();

                let plan_builder = yachtsql_parser::LogicalPlanBuilder::new()
                    .with_storage(Rc::clone(&self.storage))
                    .with_dialect(self.dialect())
                    .with_variables(parser_vars)
                    .with_udfs(session_udfs);
                let logical_plan = plan_builder.query_to_plan(&query_for_plan)?;

                resource_tracker.check_timeout()?;

                let optimized = self.optimizer.optimize(logical_plan)?;

                resource_tracker.check_timeout()?;

                optimized.root().clone()
            }
        };

        let planner = crate::query_executor::logical_to_physical::LogicalToPhysicalPlanner::new(
            Rc::clone(&self.storage),
        )
        .with_dialect(self.dialect());

        let logical_plan_for_conversion = yachtsql_ir::plan::LogicalPlan::new(optimized_plan);
        let physical_plan = planner.create_physical_plan(&logical_plan_for_conversion)?;

        resource_tracker.check_timeout()?;

        let sequence_executor = Rc::new(RefCell::new(super::StorageSequenceExecutor::new(
            Rc::clone(&self.storage),
        )));
        let _sequence_guard =
            crate::query_executor::evaluator::physical_plan::SequenceExecutorContextGuard::set(
                sequence_executor,
            );

        let batches = physical_plan.execute()?;

        for batch in &batches {
            resource_tracker.record_batch(batch.num_rows());

            let estimated_batch_size = batch.num_rows() * batch.schema().fields().len() * 8;
            resource_tracker.allocate(estimated_batch_size)?;
        }

        resource_tracker.check_timeout()?;

        let result = if batches.is_empty() {
            Ok(Table::empty(physical_plan.schema().clone()))
        } else if batches.len() == 1 {
            Ok(batches.into_iter().next().expect("checked len == 1"))
        } else {
            Self::merge_batches(batches)
        };

        result
    }

    fn merge_batches(batches: Vec<Table>) -> Result<Table> {
        if batches.is_empty() {
            return Err(Error::InternalError(
                "Cannot merge empty batches".to_string(),
            ));
        }

        let schema = batches[0].schema().clone();

        let mut all_rows: Vec<yachtsql_storage::Row> = Vec::new();
        for batch in batches {
            if let Ok(rows) = batch.rows() {
                all_rows.extend(rows);
            }
        }

        let mut columns = Vec::new();
        for field in schema.fields() {
            let mut column = yachtsql_storage::Column::new(&field.data_type, all_rows.len());
            for row in &all_rows {
                let value = row
                    .get_by_name(&schema, &field.name)
                    .cloned()
                    .unwrap_or_else(Value::null);
                column.push(value)?;
            }
            columns.push(column);
        }

        Table::new(schema, columns)
    }

    fn infer_column_name(&self, expr: &Expr) -> String {
        match expr {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(parts) => parts
                .last()
                .map(|p| p.value.clone())
                .unwrap_or_else(|| "column".to_string()),
            Expr::Function(func) => {
                format!("{}()", func.name)
            }
            Expr::BinaryOp { .. } => "expr".to_string(),
            _ => panic!("expr_to_column_name: unhandled expression type: {:?}", expr),
        }
    }

    fn try_index_scan(
        &self,
        dataset_id: &str,
        table_id: &str,
        where_expr: &Expr,
        _schema: &Schema,
    ) -> Option<Vec<Row>> {
        use yachtsql_storage::indexes::IndexKey;

        let (column_name, value) = self.extract_equality_predicate(where_expr)?;

        let storage = self.storage.borrow_mut();
        let dataset = storage.get_dataset(dataset_id)?;
        let table = dataset.get_table(table_id)?;

        let (index_name, index_metadata) = table.indexes().keys().find_map(|index_name| {
            if let Some(index_metadata) = dataset.get_index(index_name) {
                if index_metadata
                    .columns
                    .first()
                    .and_then(|col| col.column_name.as_ref())
                    .map(|name| name == &column_name)
                    .unwrap_or(false)
                {
                    return Some((index_name.clone(), index_metadata.clone()));
                }
            }
            None
        })?;

        if index_metadata.columns.len() > 1 {
            return None;
        }

        let index_key = IndexKey::single(value.clone());

        let row_indices = table.index_lookup(&index_name, &index_key);

        let rows: Vec<Row> = row_indices
            .into_iter()
            .filter_map(|row_idx| table.get_row(row_idx).ok())
            .collect();

        debug_eprintln!(
            "[executor::query] Index scan on {} using index '{}' found {} rows",
            table_id,
            index_name,
            rows.len()
        );

        Some(rows)
    }

    fn extract_equality_predicate(&self, expr: &Expr) -> Option<(String, Value)> {
        use sqlparser::ast::{BinaryOperator, Expr, Value as SqlValue, ValueWithSpan};

        match expr {
            Expr::BinaryOp { left, op, right } if matches!(op, BinaryOperator::Eq) => {
                if let Expr::Identifier(ident) = left.as_ref() {
                    let column_name = ident.value.clone();

                    if let Expr::Value(ValueWithSpan { value, .. }) = right.as_ref() {
                        let value = match value {
                            SqlValue::Number(n, _) => {
                                if let Ok(i) = n.parse::<i64>() {
                                    Value::int64(i)
                                } else if let Ok(f) = n.parse::<f64>() {
                                    Value::float64(f)
                                } else {
                                    return None;
                                }
                            }
                            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                                Value::string(s.clone())
                            }
                            SqlValue::Boolean(b) => Value::bool_val(*b),
                            SqlValue::Null => Value::null(),
                            _ => return None,
                        };

                        return Some((column_name, value));
                    }
                }

                if let Expr::Identifier(ident) = right.as_ref() {
                    let column_name = ident.value.clone();

                    if let Expr::Value(ValueWithSpan { value, .. }) = left.as_ref() {
                        let value = match value {
                            SqlValue::Number(n, _) => {
                                if let Ok(i) = n.parse::<i64>() {
                                    Value::int64(i)
                                } else if let Ok(f) = n.parse::<f64>() {
                                    Value::float64(f)
                                } else {
                                    return None;
                                }
                            }
                            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                                Value::string(s.clone())
                            }
                            SqlValue::Boolean(b) => Value::bool_val(*b),
                            SqlValue::Null => Value::null(),
                            _ => return None,
                        };

                        return Some((column_name, value));
                    }
                }

                None
            }
            _ => None,
        }
    }

    fn sql_type_to_data_type(sql_type: &sqlparser::ast::DataType) -> Result<DataType> {
        use sqlparser::ast::DataType as SqlDataType;
        match sql_type {
            SqlDataType::Int64
            | SqlDataType::Int(_)
            | SqlDataType::Integer(_)
            | SqlDataType::BigInt(_)
            | SqlDataType::TinyInt(_)
            | SqlDataType::SmallInt(_) => Ok(DataType::Int64),
            SqlDataType::Float64
            | SqlDataType::Float(_)
            | SqlDataType::Real
            | SqlDataType::Double(_)
            | SqlDataType::DoublePrecision => Ok(DataType::Float64),
            SqlDataType::Boolean | SqlDataType::Bool => Ok(DataType::Bool),
            SqlDataType::String(_)
            | SqlDataType::Varchar(_)
            | SqlDataType::Char(_)
            | SqlDataType::Text => Ok(DataType::String),
            SqlDataType::Bytea | SqlDataType::Bytes(_) => Ok(DataType::Bytes),
            SqlDataType::Date => Ok(DataType::Date),
            SqlDataType::Timestamp(_, _) => Ok(DataType::Timestamp),
            SqlDataType::Time(_, _) => Ok(DataType::Time),
            SqlDataType::Datetime(_) => Ok(DataType::DateTime),
            SqlDataType::Decimal(info) | SqlDataType::Numeric(info) => {
                use sqlparser::ast::ExactNumberInfo;
                let precision_scale = match info {
                    ExactNumberInfo::PrecisionAndScale(p, s) => Some((*p as u8, *s as u8)),
                    ExactNumberInfo::Precision(p) => Some((*p as u8, 0)),
                    ExactNumberInfo::None => None,
                };
                Ok(DataType::Numeric(precision_scale))
            }
            SqlDataType::Array(inner_def) => {
                let inner_sql_type = match inner_def {
                    sqlparser::ast::ArrayElemTypeDef::AngleBracket(inner_type) => &**inner_type,
                    sqlparser::ast::ArrayElemTypeDef::SquareBracket(inner_type, _) => &**inner_type,
                    sqlparser::ast::ArrayElemTypeDef::Parenthesis(inner_type) => &**inner_type,
                    sqlparser::ast::ArrayElemTypeDef::None => {
                        return Ok(DataType::Array(Box::new(DataType::Unknown)));
                    }
                };

                let inner_data_type = Self::sql_type_to_data_type(inner_sql_type)?;
                Ok(DataType::Array(Box::new(inner_data_type)))
            }
            SqlDataType::Custom(name, _) => {
                let type_name = name.to_string();
                let type_upper = type_name.to_uppercase();
                match type_upper.as_str() {
                    "POINT" => Ok(DataType::Point),
                    "BOX" | "PGBOX" => Ok(DataType::PgBox),
                    "CIRCLE" => Ok(DataType::Circle),
                    "INET" => Ok(DataType::Inet),
                    "CIDR" => Ok(DataType::Cidr),
                    "MACADDR" => Ok(DataType::MacAddr),
                    "MACADDR8" => Ok(DataType::MacAddr8),
                    "HSTORE" => Ok(DataType::Hstore),
                    "GEOGRAPHY" => Ok(DataType::Geography),

                    _ => Ok(DataType::Custom(type_name)),
                }
            }
            SqlDataType::GeometricType(kind) => {
                use sqlparser::ast::GeometricTypeKind;
                match kind {
                    GeometricTypeKind::Point => Ok(DataType::Point),
                    GeometricTypeKind::GeometricBox => Ok(DataType::PgBox),
                    GeometricTypeKind::Circle => Ok(DataType::Circle),
                    _ => panic!(
                        "sql_data_type_to_data_type: unhandled GeometricTypeKind: {:?}",
                        kind
                    ),
                }
            }
            _ => panic!(
                "sql_data_type_to_data_type: unhandled SqlDataType: {:?}",
                sql_type
            ),
        }
    }

    fn infer_expression_type(expr: &Expr, schema: &Schema) -> Result<DataType> {
        match expr {
            Expr::Identifier(ident) => {
                let matching_fields: Vec<_> = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| f.name == ident.value)
                    .collect();

                match matching_fields.len() {
                    0 => Err(Error::ColumnNotFound(ident.value.clone())),
                    1 => Ok(matching_fields[0].1.data_type.clone()),
                    _ => {
                        let source_tables: std::collections::HashSet<_> = matching_fields
                            .iter()
                            .filter_map(|(_, f)| f.source_table.as_ref())
                            .collect();
                        if source_tables.len() > 1 {
                            Err(Error::InvalidQuery(format!(
                                "Column reference '{}' is ambiguous - it exists in multiple tables. Use table.column syntax to disambiguate.",
                                ident.value
                            )))
                        } else {
                            Ok(matching_fields[0].1.data_type.clone())
                        }
                    }
                }
            }
            Expr::CompoundIdentifier(parts) => {
                if parts.len() == 2 {
                    let first_part = &parts[0].value;
                    let second_part = &parts[1].value;

                    if let Some(field) = schema.fields().iter().find(|f| f.name == *first_part) {
                        match &field.data_type {
                            DataType::Struct(struct_fields) => {
                                let struct_field = struct_fields
                                    .iter()
                                    .find(|sf| sf.name == *second_part)
                                    .ok_or_else(|| {
                                        Error::ColumnNotFound(format!(
                                            "{}.{}",
                                            first_part, second_part
                                        ))
                                    })?;
                                return Ok(struct_field.data_type.clone());
                            }
                            DataType::Custom(type_name) => {
                                return Ok(DataType::Unknown);
                            }
                            _ => {}
                        }
                    }

                    let field = schema
                        .fields()
                        .iter()
                        .find(|f| f.name == *second_part)
                        .ok_or_else(|| Error::ColumnNotFound(second_part.to_string()))?;
                    Ok(field.data_type.clone())
                } else {
                    let col_name = parts.last().map(|p| p.value.as_str()).unwrap_or("");
                    let field = schema
                        .fields()
                        .iter()
                        .find(|f| f.name == col_name)
                        .ok_or_else(|| Error::ColumnNotFound(col_name.to_string()))?;
                    Ok(field.data_type.clone())
                }
            }
            Expr::Value(sql_value) => {
                use sqlparser::ast::Value as SqlValue;
                match &sql_value.value {
                    SqlValue::Number(n, _) => {
                        if n.contains('.') {
                            Ok(DataType::Float64)
                        } else {
                            Ok(DataType::Int64)
                        }
                    }
                    SqlValue::SingleQuotedString(_) | SqlValue::DoubleQuotedString(_) => {
                        Ok(DataType::String)
                    }
                    SqlValue::Boolean(_) => Ok(DataType::Bool),
                    SqlValue::Null => Ok(DataType::String),
                    _ => Ok(DataType::String),
                }
            }
            Expr::BinaryOp { left, op, right } => {
                use sqlparser::ast::BinaryOperator;
                match op {
                    BinaryOperator::Plus => {
                        let left_type = Self::infer_expression_type(left, schema)?;
                        let right_type = Self::infer_expression_type(right, schema)?;

                        if (matches!(left_type, DataType::Timestamp | DataType::TimestampTz)
                            && matches!(right_type, DataType::Interval))
                            || (matches!(left_type, DataType::Interval)
                                && matches!(
                                    right_type,
                                    DataType::Timestamp | DataType::TimestampTz
                                ))
                        {
                            Ok(DataType::Timestamp)
                        } else if (matches!(left_type, DataType::Date)
                            && matches!(right_type, DataType::Interval))
                            || (matches!(left_type, DataType::Interval)
                                && matches!(right_type, DataType::Date))
                        {
                            Ok(DataType::Date)
                        } else if matches!(left_type, DataType::Float64)
                            || matches!(right_type, DataType::Float64)
                        {
                            Ok(DataType::Float64)
                        } else {
                            Ok(left_type)
                        }
                    }
                    BinaryOperator::Minus => {
                        let left_type = Self::infer_expression_type(left, schema)?;
                        let right_type = Self::infer_expression_type(right, schema)?;

                        if matches!(left_type, DataType::Timestamp | DataType::TimestampTz)
                            && matches!(right_type, DataType::Timestamp | DataType::TimestampTz)
                        {
                            Ok(DataType::Interval)
                        } else if matches!(left_type, DataType::Timestamp | DataType::TimestampTz)
                            && matches!(right_type, DataType::Interval)
                        {
                            Ok(DataType::Timestamp)
                        } else if matches!(left_type, DataType::Date)
                            && matches!(right_type, DataType::Interval)
                        {
                            Ok(DataType::Date)
                        } else if matches!(left_type, DataType::Float64)
                            || matches!(right_type, DataType::Float64)
                        {
                            Ok(DataType::Float64)
                        } else {
                            Ok(left_type)
                        }
                    }
                    BinaryOperator::Multiply | BinaryOperator::Divide | BinaryOperator::Modulo => {
                        let left_type = Self::infer_expression_type(left, schema)?;
                        let right_type = Self::infer_expression_type(right, schema)?;

                        if matches!(left_type, DataType::Float64)
                            || matches!(right_type, DataType::Float64)
                        {
                            Ok(DataType::Float64)
                        } else {
                            Ok(left_type)
                        }
                    }

                    BinaryOperator::StringConcat => {
                        let left_type = Self::infer_expression_type(left, schema)?;
                        if matches!(left_type, DataType::Hstore) {
                            Ok(DataType::Hstore)
                        } else {
                            Ok(DataType::String)
                        }
                    }

                    BinaryOperator::Gt
                    | BinaryOperator::Lt
                    | BinaryOperator::GtEq
                    | BinaryOperator::LtEq
                    | BinaryOperator::Eq
                    | BinaryOperator::NotEq => Ok(DataType::Bool),

                    BinaryOperator::And | BinaryOperator::Or => Ok(DataType::Bool),

                    BinaryOperator::Question
                    | BinaryOperator::QuestionAnd
                    | BinaryOperator::QuestionPipe
                    | BinaryOperator::AtArrow
                    | BinaryOperator::ArrowAt => Ok(DataType::Bool),

                    BinaryOperator::Arrow => {
                        let left_type = Self::infer_expression_type(left, schema)?;
                        if matches!(left_type, DataType::Hstore) {
                            Ok(DataType::String)
                        } else if matches!(left_type, DataType::Json) {
                            Ok(DataType::Json)
                        } else {
                            Ok(DataType::String)
                        }
                    }
                    _ => Ok(DataType::String),
                }
            }
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                match func_name.as_str() {
                    "UPPER" | "LOWER" | "CONCAT" | "SUBSTRING" | "SUBSTR" | "TRIM" | "LTRIM"
                    | "RTRIM" | "REPLACE" | "LEFT" | "RIGHT" | "LPAD" | "RPAD" | "REPEAT"
                    | "FORMAT" | "CHR" | "INITCAP" | "TO_CHAR" | "TO_HEX" | "MD5" | "SHA256"
                    | "SHA512" | "BASE64_ENCODE" | "BASE64_DECODE" => Ok(DataType::String),

                    "REVERSE" => {
                        if let sqlparser::ast::FunctionArguments::List(args) = &func.args {
                            if let Some(first_arg) = args.args.first() {
                                if let sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(arg_expr),
                                ) = first_arg
                                {
                                    let arg_type = Self::infer_expression_type(arg_expr, schema)?;
                                    if matches!(arg_type, DataType::Bytes) {
                                        return Ok(DataType::Bytes);
                                    }
                                }
                            }
                        }
                        Ok(DataType::String)
                    }

                    "LENGTH" | "LEN" | "CHAR_LENGTH" | "CHARACTER_LENGTH" | "OCTET_LENGTH"
                    | "ASCII" | "POSITION" | "STRPOS" | "INSTR" | "BIT_COUNT" | "ARRAY_LENGTH"
                    | "CARDINALITY" | "ARRAY_POSITION" => Ok(DataType::Int64),

                    "CURRENT_DATE" | "DATE" | "DATE_TRUNC" => Ok(DataType::Date),
                    "CURRENT_TIMESTAMP" | "NOW" | "TIMESTAMP" => Ok(DataType::Timestamp),
                    "CURRENT_TIME" => Ok(DataType::Time),

                    "COALESCE" | "NULLIF" | "IFNULL" | "NVL" => {
                        if let sqlparser::ast::FunctionArguments::List(args) = &func.args {
                            if let Some(first_arg) = args.args.first() {
                                if let sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(expr),
                                ) = first_arg
                                {
                                    return Self::infer_expression_type(expr, schema);
                                }
                            }
                        }
                        Ok(DataType::String)
                    }

                    "IF" => {
                        if let sqlparser::ast::FunctionArguments::List(args) = &func.args {
                            if args.args.len() != 3 {
                                return Err(Error::InvalidQuery(
                                    "IF() requires exactly 3 arguments".to_string(),
                                ));
                            }
                            if let Some(second_arg) = args.args.get(1) {
                                if let sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(expr),
                                ) = second_arg
                                {
                                    return Self::infer_expression_type(expr, schema);
                                }
                            }
                        }
                        Ok(DataType::String)
                    }

                    "TO_NUMBER" => {
                        if let sqlparser::ast::FunctionArguments::List(args) = &func.args {
                            if args.args.len() == 2 {
                                if let Some(second_arg) = args.args.get(1) {
                                    if let sqlparser::ast::FunctionArg::Unnamed(
                                        sqlparser::ast::FunctionArgExpr::Expr(Expr::Value(val)),
                                    ) = second_arg
                                    {
                                        if let sqlparser::ast::Value::SingleQuotedString(s) =
                                            &val.value
                                        {
                                            if s.eq_ignore_ascii_case("RN") {
                                                return Ok(DataType::Int64);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(DataType::Float64)
                    }

                    "ABS" | "CEIL" | "CEILING" | "FLOOR" | "ROUND" | "TRUNC" | "TRUNCATE"
                    | "SQRT" | "EXP" | "LN" | "LOG" | "LOG10" | "LOG2" | "SIN" | "COS" | "TAN"
                    | "ASIN" | "ACOS" | "ATAN" | "ATAN2" | "DEGREES" | "RADIANS" | "PI"
                    | "POWER" | "POW" | "MOD" | "RANDOM" | "RAND" | "SIGN" | "GREATEST"
                    | "LEAST" => Ok(DataType::Float64),

                    "ISNULL" | "ISNOTNULL" | "REGEXP_LIKE" | "LIKE" | "ILIKE" => Ok(DataType::Bool),

                    "POINT" => Ok(DataType::Point),
                    "BOX" => Ok(DataType::PgBox),
                    "CIRCLE" => Ok(DataType::Circle),

                    "AREA" | "DIAMETER" | "RADIUS" | "WIDTH" | "HEIGHT" => Ok(DataType::Float64),

                    "CENTER" => Ok(DataType::Point),

                    "COUNT" | "COUNT_IF" => Ok(DataType::Int64),

                    "SUM" | "AVG" | "STDDEV" | "VARIANCE" | "VAR_POP" | "VAR_SAMP"
                    | "STDDEV_POP" | "STDDEV_SAMP" => Ok(DataType::Float64),

                    "MIN" | "MAX" => {
                        if let sqlparser::ast::FunctionArguments::List(args) = &func.args {
                            if let Some(first_arg) = args.args.first() {
                                if let sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(expr),
                                ) = first_arg
                                {
                                    return Self::infer_expression_type(expr, schema);
                                }
                            }
                        }
                        Ok(DataType::String)
                    }

                    "TO_TSVECTOR"
                    | "TO_TSQUERY"
                    | "PLAINTO_TSQUERY"
                    | "PHRASETO_TSQUERY"
                    | "WEBSEARCH_TO_TSQUERY"
                    | "TS_HEADLINE"
                    | "STRIP"
                    | "SETWEIGHT"
                    | "TSVECTOR_CONCAT"
                    | "TSQUERY_AND"
                    | "TSQUERY_OR"
                    | "TSQUERY_NOT" => Ok(DataType::String),
                    "TS_RANK" | "TS_RANK_CD" => Ok(DataType::Float64),
                    "TS_MATCH" => Ok(DataType::Bool),
                    "TSVECTOR_LENGTH" => Ok(DataType::Int64),

                    _ => Ok(DataType::String),
                }
            }
            Expr::Case {
                conditions,
                else_result,
                ..
            } => {
                let mut has_float = false;
                let mut result_type = DataType::String;

                for case_when in conditions {
                    let branch_type = Self::infer_expression_type(&case_when.result, schema)?;
                    if matches!(branch_type, DataType::Float64) {
                        has_float = true;
                    }
                    if !matches!(result_type, DataType::Float64) {
                        result_type = branch_type;
                    }
                }

                if let Some(else_expr) = else_result {
                    let else_type = Self::infer_expression_type(else_expr, schema)?;
                    if matches!(else_type, DataType::Float64) {
                        has_float = true;
                    }
                    if !matches!(result_type, DataType::Float64) {
                        result_type = else_type;
                    }
                }

                if has_float {
                    Ok(DataType::Float64)
                } else {
                    Ok(result_type)
                }
            }
            Expr::Cast { data_type, .. } => Self::sql_type_to_data_type(data_type),

            Expr::Ceil { .. } | Expr::Floor { .. } => Ok(DataType::Float64),

            Expr::IsNull(_) | Expr::IsNotNull(_) => Ok(DataType::Bool),

            Expr::UnaryOp { op, expr } => {
                use sqlparser::ast::UnaryOperator;
                match op {
                    UnaryOperator::Minus | UnaryOperator::Plus => {
                        Self::infer_expression_type(expr, schema)
                    }
                    UnaryOperator::Not => Ok(DataType::Bool),
                    _ => Self::infer_expression_type(expr, schema),
                }
            }

            Expr::Nested(inner) => Self::infer_expression_type(inner, schema),

            Expr::CompoundFieldAccess { root, access_chain } => {
                let root_type = Self::infer_expression_type(root, schema)?;

                let mut result_type = root_type;
                for operation in access_chain {
                    match operation {
                        sqlparser::ast::AccessExpr::Subscript(subscript) => match subscript {
                            sqlparser::ast::Subscript::Index { .. } => {
                                if let DataType::Array(elem_type) = result_type {
                                    result_type = *elem_type;
                                } else {
                                    return Ok(result_type);
                                }
                            }
                            sqlparser::ast::Subscript::Slice { .. } => {}
                        },
                        sqlparser::ast::AccessExpr::Dot(_) => {}
                    }
                }
                Ok(result_type)
            }

            Expr::Interval(_) => Ok(DataType::Interval),
            _ => panic!(
                "infer_expression_type: unhandled expression type: {:?}",
                expr
            ),
        }
    }

    fn execute_simple_select(
        &mut self,
        select: &Select,
        order_by: Option<&OrderBy>,
    ) -> Result<Table> {
        if let Some(ref where_clause) = select.selection {
            self.validate_where_clause(where_clause)?;
        }

        let has_group_by = match &select.group_by {
            GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
            GroupByExpr::All(_) => true,
        };

        if select.having.is_some() && !has_group_by {
            return Err(Error::InvalidQuery(
                "HAVING clause requires GROUP BY".to_string(),
            ));
        }

        let has_aggregates = self.has_aggregate_functions(&select.projection);

        if has_group_by || has_aggregates {
            return self.execute_group_by_select(select);
        }

        if self.has_window_functions(&select.projection) {
            return self.execute_window_select_base(select);
        }

        if select.qualify.is_some() {
            return Err(Error::UnsupportedFeature(
                "QUALIFY can only be used with window functions".to_string(),
            ));
        }

        if select.from.is_empty() {
            return self.execute_select_without_from(select);
        }

        if select.from.len() > 1 {
            return self.execute_cartesian_product_query(select);
        }

        let table_with_joins = &select.from[0];

        if !table_with_joins.joins.is_empty() {
            return self.execute_join_query(select);
        }

        let table_reference = &table_with_joins.relation;

        if let TableFactor::Table {
            name,
            args: Some(func_args),
            ..
        } = table_reference
        {
            let (tvf_schema, tvf_rows) = self.execute_table_valued_function(name, func_args)?;
            return self.execute_tvf_select(select, tvf_schema, tvf_rows);
        }

        let table_name = self.extract_table_name(table_reference)?;

        let (dataset_id, table_id) = self.parse_ddl_table_name(&table_name)?;
        let actual_table_id = self.resolve_table_name(&dataset_id, &table_id);

        if dataset_id.eq_ignore_ascii_case("information_schema") {
            debug_eprintln!(
                "[executor::query] Handling information_schema query for table '{}'",
                table_id
            );
            let info_table = InformationSchemaTable::from_str(&table_id)?;
            let provider = InformationSchemaProvider::new(Rc::clone(&self.storage));
            let (schema, rows) = provider.query(info_table)?;

            let filtered_rows = if let Some(ref where_expr) = select.selection {
                let evaluator = ExpressionEvaluator::new(&schema);
                rows.into_iter()
                    .filter(|row| evaluator.evaluate_where(where_expr, row).unwrap_or(false))
                    .collect()
            } else {
                rows
            };

            let order_by_columns: Vec<String> = if let Some(order_by_clause) = order_by {
                let expressions = match &order_by_clause.kind {
                    OrderByKind::Expressions(exprs) => exprs,
                    _ => &vec![],
                };
                let mut columns = Vec::new();
                for order_expr in expressions {
                    if let Ok(col_name) = self.extract_column_name(&order_expr.expr) {
                        columns.push(col_name);
                    } else {
                        collect_column_refs_from_expr(&order_expr.expr, &mut columns);
                    }
                }
                columns
            } else {
                vec![]
            };

            let (result_schema, result_rows) = self.project_rows_with_order_by(
                filtered_rows,
                &schema,
                &select.projection,
                &order_by_columns,
            )?;

            return self.rows_to_record_batch(result_schema, result_rows);
        }

        let view_info = {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
            })?;

            dataset.views().get_view(&actual_table_id).map(|v| {
                (
                    v.is_materialized(),
                    v.sql.clone(),
                    v.get_materialized_data().map(|d| d.to_vec()),
                    v.get_materialized_schema().cloned(),
                )
            })
        };

        let (schema, rows) = if let Some((is_materialized, view_sql, mat_data, mat_schema)) =
            view_info
        {
            if is_materialized {
                debug_eprintln!(
                    "[executor::query] Reading from materialized view '{}'",
                    table_id
                );
                let schema = mat_schema.ok_or_else(|| {
                    Error::InternalError(format!("Materialized view '{}' has no schema", table_id))
                })?;
                let rows = mat_data.ok_or_else(|| {
                    Error::InternalError(format!("Materialized view '{}' has no data", table_id))
                })?;
                (schema, rows)
            } else {
                debug_eprintln!(
                    "[executor::query] Expanding view '{}' with SQL: {}",
                    table_id,
                    view_sql
                );
                let view_result = self.execute_sql(&view_sql)?;
                let view_schema = view_result.schema().clone();
                let view_rows = view_result.rows().map(|r| r.to_vec()).unwrap_or_default();
                (view_schema, view_rows)
            }
        } else {
            let schema = {
                let storage = self.storage.borrow_mut();
                let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                    Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
                })?;
                let table = dataset.get_table(&actual_table_id).ok_or_else(|| {
                    Error::TableNotFound(format!(
                        "Table '{}.{}' not found",
                        dataset_id, actual_table_id
                    ))
                })?;
                table.schema().clone()
            };
            let rows = self.scan_table(&dataset_id, &actual_table_id)?;
            (schema, rows)
        };

        let filtered_rows = if let Some(ref where_expr) = select.selection {
            if let Some(index_rows) =
                self.try_index_scan(&dataset_id, &actual_table_id, where_expr, &schema)
            {
                index_rows
            } else {
                let type_registry = {
                    let storage = self.storage.borrow();
                    storage.get_dataset("default").map(|d| d.types().clone())
                };
                let evaluator = if let Some(registry) = type_registry {
                    ExpressionEvaluator::new(&schema).with_owned_type_registry(registry)
                } else {
                    ExpressionEvaluator::new(&schema)
                };
                rows.into_iter()
                    .filter(|row| evaluator.evaluate_where(where_expr, row).unwrap_or(false))
                    .collect()
            }
        } else {
            rows
        };

        let order_by_columns: Vec<String> = if let Some(order_by_clause) = order_by {
            let expressions = match &order_by_clause.kind {
                OrderByKind::Expressions(exprs) => exprs,
                _ => &vec![],
            };
            let mut columns = Vec::new();
            for order_expr in expressions {
                if let Ok(col_name) = self.extract_column_name(&order_expr.expr) {
                    columns.push(col_name);
                } else {
                    collect_column_refs_from_expr(&order_expr.expr, &mut columns);
                }
            }
            columns
        } else {
            vec![]
        };

        let (result_schema, result_rows) = self.project_rows_with_order_by(
            filtered_rows,
            &schema,
            &select.projection,
            &order_by_columns,
        )?;

        self.rows_to_record_batch(result_schema, result_rows)
    }

    fn execute_group_by_select(&mut self, select: &Select) -> Result<Table> {
        if select.from.is_empty() {
            return Err(Error::InvalidQuery(
                "GROUP BY requires a FROM clause".to_string(),
            ));
        }

        let (schema, filtered_rows) = if select.from.len() > 1 {
            let cart_result = self.execute_cartesian_product_base(select)?;
            let cart_schema = cart_result.0;
            let cart_rows = cart_result.1;
            (cart_schema, cart_rows)
        } else {
            let table_with_joins = &select.from[0];

            if !table_with_joins.joins.is_empty() {
                let (join_schema, join_rows) = self.execute_join_base(select)?;

                (join_schema, join_rows)
            } else {
                let table_reference = &table_with_joins.relation;
                let table_name = self.extract_table_name(table_reference)?;
                let (dataset_id, table_id_orig) = self.parse_ddl_table_name(&table_name)?;
                let table_id = self.resolve_table_name(&dataset_id, &table_id_orig);

                let view_sql = {
                    let storage = self.storage.borrow_mut();
                    let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                        Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
                    })?;

                    dataset.views().get_view(&table_id).map(|v| v.sql.clone())
                };

                let (schema, rows) = if let Some(view_sql) = view_sql {
                    let view_result = self.execute_sql(&view_sql)?;
                    let view_schema = view_result.schema().clone();
                    let view_rows = view_result.rows().unwrap_or_default();
                    (view_schema, view_rows)
                } else {
                    let schema = {
                        let storage = self.storage.borrow_mut();
                        let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                            Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
                        })?;
                        let table = dataset.get_table(&table_id).ok_or_else(|| {
                            Error::TableNotFound(format!(
                                "Table '{}.{}' not found",
                                dataset_id, table_id
                            ))
                        })?;
                        table.schema().clone()
                    };
                    let rows = self.scan_table(&dataset_id, &table_id)?;
                    (schema, rows)
                };

                let filtered_rows = if let Some(ref where_expr) = select.selection {
                    let evaluator = ExpressionEvaluator::new(&schema);
                    rows.into_iter()
                        .filter(|row| evaluator.evaluate_where(where_expr, row).unwrap_or(false))
                        .collect()
                } else {
                    rows
                };

                (schema, filtered_rows)
            }
        };

        let func_registry = self.session.function_registry().clone();

        let group_by_exprs = match &select.group_by {
            GroupByExpr::Expressions(exprs, _) => exprs.clone(),
            GroupByExpr::All(_) => {
                let mut non_agg_exprs = Vec::new();
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => {
                            if AggregateSpec::from_expr(expr, &func_registry)?.is_none() {
                                non_agg_exprs.push(expr.clone());
                            }
                        }
                        SelectItem::ExprWithAlias { expr, .. } => {
                            if AggregateSpec::from_expr(expr, &func_registry)?.is_none() {
                                non_agg_exprs.push(expr.clone());
                            }
                        }
                        _ => {}
                    }
                }
                non_agg_exprs
            }
        };

        let mut aggregate_specs = Vec::new();
        let mut select_col_names = Vec::new();
        let mut select_exprs = Vec::new();

        let mut col_type = Vec::new();

        let mut wrapped_agg_inner_idx = Vec::new();

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    if let Some(agg_spec) = AggregateSpec::from_expr(expr, &func_registry)? {
                        let col_name = format!(
                            "{}({})",
                            agg_spec.function_name,
                            if agg_spec.is_count_star {
                                "*".to_string()
                            } else if agg_spec.argument_exprs.is_empty() {
                                "".to_string()
                            } else {
                                agg_spec
                                    .argument_exprs
                                    .iter()
                                    .map(|e| {
                                        self.extract_column_name(e)
                                            .unwrap_or_else(|_| "?".to_string())
                                    })
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            }
                        );
                        select_col_names.push(col_name);
                        select_exprs.push(expr.clone());
                        aggregate_specs.push(agg_spec);
                        col_type.push(1);
                        wrapped_agg_inner_idx.push(-1);
                    } else if self.contains_aggregate_function(expr) {
                        let col_name = self
                            .extract_column_name(expr)
                            .unwrap_or_else(|_| format!("{}", expr));
                        select_col_names.push(col_name);
                        select_exprs.push(expr.clone());

                        if let Some(inner_agg_expr) = self.extract_inner_aggregate(expr) {
                            if let Some(inner_agg_spec) =
                                AggregateSpec::from_expr(&inner_agg_expr, &func_registry)?
                            {
                                let inner_idx = aggregate_specs.len() as i32;
                                aggregate_specs.push(inner_agg_spec);
                                col_type.push(2);
                                wrapped_agg_inner_idx.push(inner_idx);
                            } else {
                                return Err(Error::InternalError(
                                    "Failed to parse inner aggregate from wrapped expression"
                                        .to_string(),
                                ));
                            }
                        } else {
                            return Err(Error::InternalError(
                                "Failed to extract inner aggregate from wrapped expression"
                                    .to_string(),
                            ));
                        }
                    } else {
                        let col_name = self
                            .extract_column_name(expr)
                            .unwrap_or_else(|_| format!("{}", expr));
                        select_col_names.push(col_name);
                        select_exprs.push(expr.clone());
                        col_type.push(0);
                        wrapped_agg_inner_idx.push(-1);
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    if let Some(agg_spec) = AggregateSpec::from_expr(expr, &func_registry)? {
                        select_col_names.push(alias.value.clone());
                        select_exprs.push(expr.clone());
                        aggregate_specs.push(agg_spec);
                        col_type.push(1);
                        wrapped_agg_inner_idx.push(-1);
                    } else if self.contains_aggregate_function(expr) {
                        select_col_names.push(alias.value.clone());
                        select_exprs.push(expr.clone());

                        if let Some(inner_agg_expr) = self.extract_inner_aggregate(expr) {
                            if let Some(inner_agg_spec) =
                                AggregateSpec::from_expr(&inner_agg_expr, &func_registry)?
                            {
                                let inner_idx = aggregate_specs.len() as i32;
                                aggregate_specs.push(inner_agg_spec);
                                col_type.push(2);
                                wrapped_agg_inner_idx.push(inner_idx);
                            } else {
                                return Err(Error::InternalError(
                                    "Failed to parse inner aggregate from wrapped expression"
                                        .to_string(),
                                ));
                            }
                        } else {
                            return Err(Error::InternalError(
                                "Failed to extract inner aggregate from wrapped expression"
                                    .to_string(),
                            ));
                        }
                    } else {
                        select_col_names.push(alias.value.clone());
                        select_exprs.push(expr.clone());
                        col_type.push(0);
                        wrapped_agg_inner_idx.push(-1);
                    }
                }
                _ => {
                    return Err(Error::UnsupportedFeature(
                        "Wildcard not supported in GROUP BY SELECT".to_string(),
                    ));
                }
            }
        }

        let is_aggregate_col: Vec<bool> = col_type.iter().map(|&t| t == 1).collect();

        let num_select_aggregates = aggregate_specs.len();

        let mut having_agg_indices: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        if let Some(ref having_expr) = select.having {
            self.extract_having_aggregates(
                having_expr,
                &func_registry,
                &select_exprs,
                &mut aggregate_specs,
                &mut having_agg_indices,
            )?;
        }

        for (expr, is_agg) in select_exprs.iter().zip(is_aggregate_col.iter()) {
            if !is_agg {
                if self.contains_aggregate_function(expr) {
                    continue;
                }

                let expr_str = format!("{}", expr);
                let in_group_by = group_by_exprs
                    .iter()
                    .any(|group_expr| format!("{}", group_expr) == expr_str);

                if !in_group_by {
                    return Err(Error::InvalidQuery(format!(
                        "Column '{}' must appear in GROUP BY clause or be used in an aggregate function",
                        expr_str
                    )));
                }
            }
        }

        let mut aggregator = Aggregator::new(
            group_by_exprs.clone(),
            aggregate_specs.clone(),
            func_registry.clone(),
        );

        for row in &filtered_rows {
            aggregator.process_row(row, &schema)?;
        }

        let mut aggregated = aggregator.finalize()?;

        let mut result_fields = Vec::new();
        let mut direct_agg_idx = 0;

        for (col_idx, (col_name, expr)) in
            select_col_names.iter().zip(select_exprs.iter()).enumerate()
        {
            match col_type[col_idx] {
                1 => {
                    let agg_spec = &aggregate_specs[direct_agg_idx];
                    direct_agg_idx += 1;
                    let agg_func = func_registry
                        .get_aggregate(&agg_spec.function_name)
                        .ok_or_else(|| {
                            Error::UnsupportedFeature(format!(
                                "Unknown aggregate function: {}",
                                agg_spec.function_name
                            ))
                        })?;

                    let arg_types: Vec<DataType> = agg_spec
                        .argument_exprs
                        .iter()
                        .filter_map(|expr| match expr {
                            Expr::Identifier(ident) => schema
                                .fields()
                                .iter()
                                .find(|f| f.name == ident.value)
                                .map(|f| f.data_type.clone()),
                            Expr::CompoundIdentifier(parts) if !parts.is_empty() => {
                                let col_name =
                                    parts.last().expect("checked non-empty").value.clone();
                                schema
                                    .fields()
                                    .iter()
                                    .find(|f| f.name == col_name)
                                    .map(|f| f.data_type.clone())
                            }

                            _ => Self::infer_expression_type(expr, &schema).ok(),
                        })
                        .collect();

                    let return_type = agg_func.return_type(&arg_types)?;
                    result_fields.push(Field::nullable(col_name.clone(), return_type));
                }
                2 => {
                    let data_type =
                        Self::infer_expression_type(expr, &schema).unwrap_or(DataType::Int64);
                    result_fields.push(Field::nullable(col_name.clone(), data_type));
                }
                _ => {
                    let data_type = match expr {
                        Expr::Identifier(ident) => schema
                            .fields()
                            .iter()
                            .find(|f| f.name == ident.value)
                            .map(|f| f.data_type.clone())
                            .unwrap_or(DataType::Unknown),
                        Expr::CompoundIdentifier(parts) if !parts.is_empty() => {
                            let col_name = parts.last().expect("checked non-empty").value.clone();
                            schema
                                .fields()
                                .iter()
                                .find(|f| f.name == col_name)
                                .map(|f| f.data_type.clone())
                                .unwrap_or(DataType::Unknown)
                        }

                        Expr::Substring { .. } => DataType::String,

                        Expr::Function(func)
                            if func.name.to_string().to_uppercase() == "SUBSTRING" =>
                        {
                            DataType::String
                        }

                        _ => Self::infer_expression_type(expr, &schema).unwrap_or(DataType::String),
                    };
                    result_fields.push(Field::nullable(col_name.clone(), data_type));
                }
            }
        }
        let result_schema = Schema::from_fields(result_fields);

        let mut agg_expr_to_col_idx: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();

        let num_group_by_cols = group_by_exprs.len();

        let mut agg_idx = 0;
        for (col_idx, (expr, is_agg)) in
            select_exprs.iter().zip(is_aggregate_col.iter()).enumerate()
        {
            if *is_agg {
                let agg_key = self.aggregate_function_to_column_name_from_expr(expr)?;

                agg_expr_to_col_idx.insert(agg_key, num_group_by_cols + agg_idx);
                agg_idx += 1;
            }
        }

        for (agg_key, agg_spec_idx) in &having_agg_indices {
            agg_expr_to_col_idx.insert(agg_key.clone(), num_group_by_cols + *agg_spec_idx);
        }

        if let Some(ref having_expr) = select.having {
            aggregated = self.filter_having(
                aggregated,
                having_expr,
                &result_schema,
                &agg_expr_to_col_idx,
            )?;
        }

        let result_rows: Result<Vec<Row>> = aggregated
            .into_iter()
            .map(|(group_values, agg_values)| {
                let mut row_values = Vec::new();
                let mut group_idx = 0;
                let mut agg_idx = 0;

                for (col_idx, &ctype) in col_type.iter().enumerate() {
                    match ctype {
                        0 => {
                            row_values.push(group_values[group_idx].clone());
                            group_idx += 1;
                        }
                        1 => {
                            row_values.push(agg_values[agg_idx].clone());
                            agg_idx += 1;
                        }
                        2 => {
                            let inner_idx = wrapped_agg_inner_idx[col_idx] as usize;
                            let inner_agg_value = &agg_values[inner_idx];
                            let wrapper_expr = &select_exprs[col_idx];

                            if let Some(inner_agg_expr) = self.extract_inner_aggregate(wrapper_expr)
                            {
                                let result = self.evaluate_wrapped_aggregate(
                                    wrapper_expr,
                                    &inner_agg_expr,
                                    inner_agg_value,
                                )?;
                                row_values.push(result);
                            } else {
                                row_values.push(Value::null());
                            }

                            agg_idx += 1;
                        }
                        _ => {
                            row_values.push(Value::null());
                        }
                    }
                }

                Ok(Row::from_values(row_values))
            })
            .collect();

        let result_rows = result_rows?;

        self.rows_to_record_batch(result_schema, result_rows)
    }

    pub(crate) fn extract_table_name(&self, table_ref: &TableFactor) -> Result<String> {
        match table_ref {
            TableFactor::Table { name, .. } => Ok(name.to_string()),
            TableFactor::Derived { alias, .. } => {
                if alias.is_none() {
                    return Err(Error::InvalidQuery(
                        "Subquery in FROM clause must have an alias".to_string(),
                    ));
                }
                Err(Error::UnsupportedFeature(
                    "Subqueries in FROM clause are not yet supported".to_string(),
                ))
            }
            _ => Err(Error::UnsupportedFeature(
                "Only simple table references supported (no subqueries, LATERAL, etc.)".to_string(),
            )),
        }
    }

    pub(crate) fn extract_table_alias(&self, table_ref: &TableFactor) -> Result<String> {
        match table_ref {
            TableFactor::Table { name, alias, .. } => {
                if let Some(alias) = alias {
                    Ok(alias.name.value.clone())
                } else {
                    Ok(name.to_string())
                }
            }
            TableFactor::Derived { alias, .. } => {
                if let Some(alias) = alias {
                    Ok(alias.name.value.clone())
                } else {
                    Err(Error::InvalidQuery(
                        "Subquery in FROM clause must have an alias".to_string(),
                    ))
                }
            }
            _ => Err(Error::UnsupportedFeature(
                "Only simple table references supported (no subqueries, LATERAL, etc.)".to_string(),
            )),
        }
    }

    pub(crate) fn get_table_data(&mut self, table_ref: &TableFactor) -> Result<(Schema, Vec<Row>)> {
        match table_ref {
            TableFactor::Table { name, args, .. } => {
                if let Some(func_args) = args {
                    return self.execute_table_valued_function(name, func_args);
                }

                let table_name = name.to_string();
                let (dataset, table_orig) = self.parse_ddl_table_name(&table_name)?;
                let table = self.resolve_table_name(&dataset, &table_orig);

                let schema = {
                    let storage = self.storage.borrow_mut();
                    let ds = storage.get_dataset(&dataset).ok_or_else(|| {
                        Error::DatasetNotFound(format!("Dataset '{}' not found", dataset))
                    })?;
                    let tbl = ds.get_table(&table).ok_or_else(|| {
                        Error::TableNotFound(format!("Table '{}.{}' not found", dataset, table))
                    })?;
                    tbl.schema().clone()
                };

                let rows = self.scan_table(&dataset, &table)?;
                Ok((schema, rows))
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                if alias.is_none() {
                    return Err(Error::InvalidQuery(
                        "Subquery in FROM clause must have an alias".to_string(),
                    ));
                }

                let result_batch = self.execute_set_expr(&subquery.body)?;
                let schema = result_batch.schema().clone();
                let rows: Vec<Row> = (0..result_batch.num_rows())
                    .map(|i| {
                        let values: Vec<Value> = (0..result_batch.num_columns())
                            .map(|j| {
                                result_batch
                                    .column(j)
                                    .and_then(|col| col.get(i).ok())
                                    .unwrap_or(Value::null())
                            })
                            .collect();
                        Row::from_values(values)
                    })
                    .collect();
                Ok((schema, rows))
            }
            _ => Err(Error::UnsupportedFeature(
                "Only table and derived table references are supported".to_string(),
            )),
        }
    }

    fn execute_table_valued_function(
        &mut self,
        name: &sqlparser::ast::ObjectName,
        args: &sqlparser::ast::TableFunctionArgs,
    ) -> Result<(Schema, Vec<Row>)> {
        let func_name = name.to_string().to_uppercase();

        let empty_schema = Schema::from_fields(vec![]);
        let evaluator = ExpressionEvaluator::new(&empty_schema);
        let empty_row = Row::from_values(vec![]);

        let evaluated_args: Vec<Value> = args
            .args
            .iter()
            .map(|arg| match arg {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                    expr,
                )) => evaluator.evaluate_expr(expr, &empty_row),
                _ => Err(Error::InvalidQuery(
                    "Unsupported argument type in table-valued function".to_string(),
                )),
            })
            .collect::<Result<Vec<_>>>()?;

        match func_name.as_str() {
            "EACH" => {
                if evaluated_args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "each() requires exactly 1 argument".to_string(),
                    ));
                }
                let hstore_val = &evaluated_args[0];
                let hstore_map = hstore_val.as_hstore().ok_or_else(|| Error::TypeMismatch {
                    expected: "HSTORE".to_string(),
                    actual: hstore_val.data_type().to_string(),
                })?;

                let schema = Schema::from_fields(vec![
                    Field::nullable("key", DataType::String),
                    Field::nullable("value", DataType::String),
                ]);

                let rows: Vec<Row> = hstore_map
                    .iter()
                    .map(|(k, v)| {
                        Row::from_values(vec![
                            Value::string(k.clone()),
                            v.as_ref()
                                .map(|s| Value::string(s.clone()))
                                .unwrap_or(Value::null()),
                        ])
                    })
                    .collect();

                Ok((schema, rows))
            }
            "SKEYS" => {
                if evaluated_args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "skeys() requires exactly 1 argument".to_string(),
                    ));
                }
                let hstore_val = &evaluated_args[0];
                let hstore_map = hstore_val.as_hstore().ok_or_else(|| Error::TypeMismatch {
                    expected: "HSTORE".to_string(),
                    actual: hstore_val.data_type().to_string(),
                })?;

                let schema = Schema::from_fields(vec![Field::nullable("skeys", DataType::String)]);

                let rows: Vec<Row> = hstore_map
                    .keys()
                    .map(|k| Row::from_values(vec![Value::string(k.clone())]))
                    .collect();

                Ok((schema, rows))
            }
            "SVALS" => {
                if evaluated_args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "svals() requires exactly 1 argument".to_string(),
                    ));
                }
                let hstore_val = &evaluated_args[0];
                let hstore_map = hstore_val.as_hstore().ok_or_else(|| Error::TypeMismatch {
                    expected: "HSTORE".to_string(),
                    actual: hstore_val.data_type().to_string(),
                })?;

                let schema = Schema::from_fields(vec![Field::nullable("svals", DataType::String)]);

                let rows: Vec<Row> = hstore_map
                    .values()
                    .map(|v| {
                        Row::from_values(vec![
                            v.as_ref()
                                .map(|s| Value::string(s.clone()))
                                .unwrap_or(Value::null()),
                        ])
                    })
                    .collect();

                Ok((schema, rows))
            }
            "POPULATE_RECORD" => {
                if args.args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "populate_record() requires exactly 2 arguments".to_string(),
                    ));
                }

                let type_name = if let Some(sqlparser::ast::FunctionArg::Unnamed(
                    sqlparser::ast::FunctionArgExpr::Expr(expr),
                )) = args.args.first()
                {
                    match expr {
                        sqlparser::ast::Expr::Cast { data_type, .. } => match data_type {
                            sqlparser::ast::DataType::Custom(obj_name, _) => {
                                obj_name.to_string().to_lowercase()
                            }
                            _ => {
                                return Err(Error::InvalidQuery(
                                    "populate_record first argument must be cast to a record type"
                                        .to_string(),
                                ));
                            }
                        },
                        _ => {
                            return Err(Error::InvalidQuery(
                                "populate_record first argument must be NULL::record_type"
                                    .to_string(),
                            ));
                        }
                    }
                } else {
                    return Err(Error::InvalidQuery(
                        "populate_record first argument must be NULL::record_type".to_string(),
                    ));
                };

                let hstore_val = &evaluated_args[1];
                let hstore_map = hstore_val.as_hstore().ok_or_else(|| Error::TypeMismatch {
                    expected: "HSTORE".to_string(),
                    actual: hstore_val.data_type().to_string(),
                })?;

                let (schema, field_names) = {
                    let storage = self.storage.borrow();
                    let dataset = storage.get_dataset("default").ok_or_else(|| {
                        Error::DatasetNotFound("Dataset 'default' not found".to_string())
                    })?;

                    if let Some(table) = dataset.tables().get(&type_name) {
                        let fields: Vec<Field> = table
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| Field::nullable(&f.name, f.data_type.clone()))
                            .collect();
                        let names: Vec<String> = table
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| f.name.clone())
                            .collect();
                        (Schema::from_fields(fields), names)
                    } else {
                        return Err(Error::TableNotFound(format!(
                            "Record type '{}' not found (no table with this name)",
                            type_name
                        )));
                    }
                };

                let values: Vec<Value> = field_names
                    .iter()
                    .map(|col_name| {
                        hstore_map
                            .get(col_name)
                            .and_then(|v| v.as_ref())
                            .map(|s| Value::string(s.clone()))
                            .unwrap_or(Value::null())
                    })
                    .collect();

                let row = Row::from_values(values);
                Ok((schema, vec![row]))
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Table-valued function '{}' is not supported",
                func_name
            ))),
        }
    }

    fn execute_tvf_select(
        &mut self,
        select: &Select,
        schema: Schema,
        rows: Vec<Row>,
    ) -> Result<Table> {
        let is_select_star = select.projection.len() == 1
            && matches!(&select.projection[0], SelectItem::Wildcard(_));

        if is_select_star {
            let mut columns: Vec<yachtsql_storage::Column> = schema
                .fields()
                .iter()
                .map(|f| yachtsql_storage::Column::new(&f.data_type, rows.len()))
                .collect();

            for row in rows {
                for (i, value) in row.values().iter().enumerate() {
                    if i < columns.len() {
                        columns[i].push(value.clone())?;
                    }
                }
            }

            return Table::new(schema, columns);
        }

        let evaluator = ExpressionEvaluator::new(&schema);
        let mut result_columns: Vec<yachtsql_storage::Column> = Vec::new();
        let mut result_schema_fields: Vec<Field> = Vec::new();

        for item in select.projection.iter() {
            let (col_name, data_type) = match item {
                SelectItem::UnnamedExpr(expr) => {
                    let name = self.infer_column_name(expr);
                    let dt = Self::infer_expression_type(expr, &schema).unwrap_or(DataType::String);
                    (name, dt)
                }
                SelectItem::ExprWithAlias { alias, expr } => {
                    let dt = Self::infer_expression_type(expr, &schema).unwrap_or(DataType::String);
                    (alias.value.clone(), dt)
                }
                SelectItem::Wildcard(_) => {
                    continue;
                }
                SelectItem::QualifiedWildcard(_, _) => {
                    continue;
                }
            };
            result_columns.push(yachtsql_storage::Column::new(&data_type, rows.len()));
            result_schema_fields.push(Field::nullable(&col_name, data_type));
        }

        for row in rows {
            for (idx, item) in select.projection.iter().enumerate() {
                let value = match item {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        evaluator.evaluate_expr(expr, &row)?
                    }
                    _ => continue,
                };
                if idx < result_columns.len() {
                    result_columns[idx].push(value)?;
                }
            }
        }

        let result_schema = Schema::from_fields(result_schema_fields);
        Table::new(result_schema, result_columns)
    }

    #[allow(dead_code)]
    fn resolve_select_columns(
        &self,
        schema: &Schema,
        select_items: &[SelectItem],
    ) -> Result<Vec<usize>> {
        let mut indices = Vec::new();

        for item in select_items {
            match item {
                SelectItem::Wildcard(_) => {
                    for i in 0..schema.fields().len() {
                        indices.push(i);
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    let col_name = self.extract_column_name(expr)?;
                    let idx = schema
                        .fields()
                        .iter()
                        .position(|f| f.name == col_name)
                        .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;
                    indices.push(idx);
                }
                SelectItem::ExprWithAlias { expr, .. } => {
                    let col_name = self.extract_column_name(expr)?;
                    let idx = schema
                        .fields()
                        .iter()
                        .position(|f| f.name == col_name)
                        .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;
                    indices.push(idx);
                }
                SelectItem::QualifiedWildcard(_object_name, _) => {
                    for i in 0..schema.fields().len() {
                        indices.push(i);
                    }
                }
            }
        }

        if indices.is_empty() {
            return Err(Error::InvalidQuery(
                "SELECT must specify at least one column".to_string(),
            ));
        }

        Ok(indices)
    }

    fn extract_column_name(&self, expr: &Expr) -> Result<String> {
        match expr {
            Expr::Identifier(ident) => Ok(ident.value.clone()),
            Expr::CompoundIdentifier(parts) => {
                if parts.len() == 1 {
                    Ok(parts[0].value.clone())
                } else if parts.len() == 2 {
                    Ok(parts[1].value.clone())
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Invalid column reference: {}",
                        parts
                            .iter()
                            .map(|p| p.value.as_str())
                            .collect::<Vec<_>>()
                            .join(".")
                    )))
                }
            }
            _ => Err(Error::UnsupportedFeature(
                "Only simple column references supported in SELECT".to_string(),
            )),
        }
    }

    fn extract_qualified_column_name(&self, expr: &Expr) -> Result<(Option<String>, String)> {
        match expr {
            Expr::Identifier(ident) => Ok((None, ident.value.clone())),
            Expr::CompoundIdentifier(parts) => {
                if parts.len() == 1 {
                    Ok((None, parts[0].value.clone()))
                } else if parts.len() == 2 {
                    Ok((Some(parts[0].value.clone()), parts[1].value.clone()))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Invalid column reference: {}",
                        parts
                            .iter()
                            .map(|p| p.value.as_str())
                            .collect::<Vec<_>>()
                            .join(".")
                    )))
                }
            }
            _ => Err(Error::UnsupportedFeature(
                "Only simple column references supported in ORDER BY".to_string(),
            )),
        }
    }

    fn rows_to_record_batch(&self, schema: Schema, rows: Vec<Row>) -> Result<Table> {
        if rows.is_empty() {
            return Ok(Table::empty(schema));
        }

        let values: Vec<Vec<Value>> = rows.into_iter().map(|row| row.values().to_vec()).collect();

        Table::from_values(schema, values)
    }

    fn execute_multi_join_query(
        &mut self,
        select: &Select,
        table_with_joins: &TableWithJoins,
    ) -> Result<Table> {
        let (mut current_schema, mut current_rows) =
            self.get_table_data(&table_with_joins.relation)?;
        let mut current_alias = self.extract_table_alias(&table_with_joins.relation)?;

        for join in &table_with_joins.joins {
            let (right_schema, right_rows) = self.get_table_data(&join.relation)?;
            let right_table_alias = self.extract_table_alias(&join.relation)?;

            let join_type = match &join.join_operator {
                JoinOperator::Inner(_) | JoinOperator::Join(_) => JoinType::Inner,
                JoinOperator::LeftOuter(_) | JoinOperator::Left(_) => JoinType::Left,
                JoinOperator::RightOuter(_) | JoinOperator::Right(_) => JoinType::Right,
                JoinOperator::FullOuter(_) => JoinType::Full,
                JoinOperator::CrossJoin(_) => JoinType::Cross,
                other => {
                    return Err(Error::UnsupportedFeature(format!(
                        "Unsupported join operator: {:?}",
                        other
                    )));
                }
            };

            let cross_join_condition = sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                value: sqlparser::ast::Value::Boolean(true),
                span: sqlparser::tokenizer::Span::empty(),
            });
            let join_condition: &sqlparser::ast::Expr = match &join.join_operator {
                JoinOperator::Inner(JoinConstraint::On(expr))
                | JoinOperator::Join(JoinConstraint::On(expr))
                | JoinOperator::LeftOuter(JoinConstraint::On(expr))
                | JoinOperator::Left(JoinConstraint::On(expr))
                | JoinOperator::RightOuter(JoinConstraint::On(expr))
                | JoinOperator::Right(JoinConstraint::On(expr))
                | JoinOperator::FullOuter(JoinConstraint::On(expr)) => expr,
                JoinOperator::CrossJoin(_) => &cross_join_condition,
                _ => {
                    return Err(Error::UnsupportedFeature(
                        "Only ON clause supported in multi-join".to_string(),
                    ));
                }
            };

            let (joined_schema, joined_rows) = self.nested_loop_join(NestedLoopJoinParams {
                left_schema: &current_schema,
                left_rows: current_rows,
                right_schema: &right_schema,
                right_rows,
                join_type,
                join_condition,
                left_table_name: &current_alias,
                right_table_name: &right_table_alias,
            })?;

            current_schema = joined_schema;
            current_rows = joined_rows;
            current_alias = format!("{}_{}", current_alias, right_table_alias);
        }

        let filtered_rows = if let Some(ref where_expr) = select.selection {
            let evaluator = ExpressionEvaluator::new(&current_schema);
            current_rows
                .into_iter()
                .filter(|row| evaluator.evaluate_where(where_expr, row).unwrap_or(false))
                .collect()
        } else {
            current_rows
        };

        let (result_schema, result_rows) =
            self.project_rows(filtered_rows, &current_schema, &select.projection)?;

        self.rows_to_record_batch(result_schema, result_rows)
    }

    fn execute_join_query(&mut self, select: &Select) -> Result<Table> {
        let table_with_joins = &select.from[0];

        if table_with_joins.joins.is_empty() {
            return Err(Error::UnsupportedFeature(
                "No JOIN clause found".to_string(),
            ));
        }

        if table_with_joins.joins.len() > 1 {
            return self.execute_multi_join_query(select, table_with_joins);
        }

        let left_table_name = self.extract_table_name(&table_with_joins.relation)?;
        let (left_dataset, left_table_orig) = self.parse_ddl_table_name(&left_table_name)?;
        let left_table = self.resolve_table_name(&left_dataset, &left_table_orig);
        let left_table_alias = self.extract_table_alias(&table_with_joins.relation)?;

        let left_schema = {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&left_dataset).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", left_dataset))
            })?;
            let table = dataset.get_table(&left_table).ok_or_else(|| {
                Error::TableNotFound(format!("Table '{}.{}' not found", left_dataset, left_table))
            })?;
            table.schema().clone()
        };

        let left_rows = self.scan_table(&left_dataset, &left_table)?;

        let join = &table_with_joins.joins[0];
        let right_table_name = self.extract_table_name(&join.relation)?;
        let (right_dataset, right_table_orig) = self.parse_ddl_table_name(&right_table_name)?;
        let right_table = self.resolve_table_name(&right_dataset, &right_table_orig);
        let right_table_alias = self.extract_table_alias(&join.relation)?;

        let right_schema = {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&right_dataset).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", right_dataset))
            })?;
            let table = dataset.get_table(&right_table).ok_or_else(|| {
                Error::TableNotFound(format!(
                    "Table '{}.{}' not found",
                    right_dataset, right_table
                ))
            })?;
            table.schema().clone()
        };

        let right_rows = self.scan_table(&right_dataset, &right_table)?;

        let join_type = match &join.join_operator {
            JoinOperator::Inner(_) | JoinOperator::Join(_) => JoinType::Inner,
            JoinOperator::LeftOuter(_) | JoinOperator::Left(_) => JoinType::Left,
            JoinOperator::RightOuter(_) | JoinOperator::Right(_) => JoinType::Right,
            JoinOperator::FullOuter(_) => JoinType::Full,
            JoinOperator::CrossJoin(_) => JoinType::Cross,
            other => {
                return Err(Error::UnsupportedFeature(format!(
                    "Unsupported join operator: {:?}",
                    other
                )));
            }
        };

        let cross_join_condition = sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
            value: sqlparser::ast::Value::Boolean(true),
            span: sqlparser::tokenizer::Span::empty(),
        });
        let join_condition: &sqlparser::ast::Expr = match &join.join_operator {
            JoinOperator::Inner(JoinConstraint::On(expr))
            | JoinOperator::Join(JoinConstraint::On(expr))
            | JoinOperator::LeftOuter(JoinConstraint::On(expr))
            | JoinOperator::Left(JoinConstraint::On(expr))
            | JoinOperator::RightOuter(JoinConstraint::On(expr))
            | JoinOperator::Right(JoinConstraint::On(expr))
            | JoinOperator::FullOuter(JoinConstraint::On(expr)) => expr,
            JoinOperator::CrossJoin(_) => &cross_join_condition,
            JoinOperator::Inner(JoinConstraint::Using(cols))
            | JoinOperator::Join(JoinConstraint::Using(cols))
            | JoinOperator::LeftOuter(JoinConstraint::Using(cols))
            | JoinOperator::Left(JoinConstraint::Using(cols))
            | JoinOperator::RightOuter(JoinConstraint::Using(cols))
            | JoinOperator::Right(JoinConstraint::Using(cols))
            | JoinOperator::FullOuter(JoinConstraint::Using(cols)) => {
                return self.execute_join_with_using(
                    select,
                    &left_table_alias,
                    &right_table_alias,
                    &left_schema,
                    &right_schema,
                    left_rows,
                    right_rows,
                    cols,
                    join_type,
                );
            }
            JoinOperator::Inner(JoinConstraint::Natural)
            | JoinOperator::Join(JoinConstraint::Natural)
            | JoinOperator::LeftOuter(JoinConstraint::Natural)
            | JoinOperator::Left(JoinConstraint::Natural)
            | JoinOperator::RightOuter(JoinConstraint::Natural)
            | JoinOperator::Right(JoinConstraint::Natural)
            | JoinOperator::FullOuter(JoinConstraint::Natural) => {
                return self.execute_natural_join(
                    select,
                    &left_table_alias,
                    &right_table_alias,
                    &left_schema,
                    &right_schema,
                    left_rows,
                    right_rows,
                    join_type,
                );
            }
            _ => {
                return Err(Error::UnsupportedFeature(
                    "Unsupported JOIN constraint".to_string(),
                ));
            }
        };

        let mut combined_fields = Vec::new();
        for field in left_schema.fields() {
            let mut f = field.clone();
            f.source_table = Some(left_table_alias.clone());
            combined_fields.push(f);
        }
        for field in right_schema.fields() {
            let mut f = field.clone();
            f.source_table = Some(right_table_alias.clone());
            combined_fields.push(f);
        }
        let combined_schema = Schema::from_fields(combined_fields);

        self.validate_join_condition_types(join_condition, &combined_schema)?;

        let (joined_schema, joined_rows) = self.nested_loop_join(NestedLoopJoinParams {
            left_schema: &left_schema,
            left_rows,
            right_schema: &right_schema,
            right_rows,
            join_type,
            join_condition,
            left_table_name: &left_table_alias,
            right_table_name: &right_table_alias,
        })?;

        let filtered_rows = if let Some(ref where_expr) = select.selection {
            let evaluator = ExpressionEvaluator::new(&joined_schema);
            joined_rows
                .into_iter()
                .filter(|row| evaluator.evaluate_where(where_expr, row).unwrap_or(false))
                .collect()
        } else {
            joined_rows
        };

        let (result_schema, result_rows) =
            self.project_rows(filtered_rows, &joined_schema, &select.projection)?;

        self.rows_to_record_batch(result_schema, result_rows)
    }

    fn execute_join_base(&mut self, select: &Select) -> Result<(Schema, Vec<Row>)> {
        let table_with_joins = &select.from[0];

        if table_with_joins.joins.len() > 1 {
            return self.execute_multi_join_base(select, table_with_joins);
        }

        let left_table_name = self.extract_table_name(&table_with_joins.relation)?;
        let (left_dataset, left_table_orig) = self.parse_ddl_table_name(&left_table_name)?;
        let left_table = self.resolve_table_name(&left_dataset, &left_table_orig);
        let left_table_alias = self.extract_table_alias(&table_with_joins.relation)?;

        let left_schema = {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&left_dataset).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", left_dataset))
            })?;
            let table = dataset.get_table(&left_table).ok_or_else(|| {
                Error::TableNotFound(format!("Table '{}.{}' not found", left_dataset, left_table))
            })?;
            table.schema().clone()
        };

        let left_rows = self.scan_table(&left_dataset, &left_table)?;

        let join = &table_with_joins.joins[0];
        let right_table_name = self.extract_table_name(&join.relation)?;
        let (right_dataset, right_table_orig) = self.parse_ddl_table_name(&right_table_name)?;
        let right_table = self.resolve_table_name(&right_dataset, &right_table_orig);
        let right_table_alias = self.extract_table_alias(&join.relation)?;

        let right_schema = {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&right_dataset).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", right_dataset))
            })?;
            let table = dataset.get_table(&right_table).ok_or_else(|| {
                Error::TableNotFound(format!(
                    "Table '{}.{}' not found",
                    right_dataset, right_table
                ))
            })?;
            table.schema().clone()
        };

        let right_rows = self.scan_table(&right_dataset, &right_table)?;

        let join_type = match &join.join_operator {
            JoinOperator::Inner(_) | JoinOperator::Join(_) => JoinType::Inner,
            JoinOperator::LeftOuter(_) | JoinOperator::Left(_) => JoinType::Left,
            JoinOperator::RightOuter(_) | JoinOperator::Right(_) => JoinType::Right,
            JoinOperator::FullOuter(_) => JoinType::Full,
            JoinOperator::CrossJoin(_) => JoinType::Cross,
            other => {
                return Err(Error::UnsupportedFeature(format!(
                    "Unsupported join operator: {:?}",
                    other
                )));
            }
        };

        let cross_join_condition = sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
            value: sqlparser::ast::Value::Boolean(true),
            span: sqlparser::tokenizer::Span::empty(),
        });

        let join_condition: &sqlparser::ast::Expr = match &join.join_operator {
            JoinOperator::Inner(JoinConstraint::On(expr))
            | JoinOperator::Join(JoinConstraint::On(expr))
            | JoinOperator::LeftOuter(JoinConstraint::On(expr))
            | JoinOperator::Left(JoinConstraint::On(expr))
            | JoinOperator::RightOuter(JoinConstraint::On(expr))
            | JoinOperator::Right(JoinConstraint::On(expr))
            | JoinOperator::FullOuter(JoinConstraint::On(expr)) => expr,
            JoinOperator::CrossJoin(_) => &cross_join_condition,
            JoinOperator::Inner(JoinConstraint::Using(cols))
            | JoinOperator::Join(JoinConstraint::Using(cols))
            | JoinOperator::LeftOuter(JoinConstraint::Using(cols))
            | JoinOperator::Left(JoinConstraint::Using(cols))
            | JoinOperator::RightOuter(JoinConstraint::Using(cols))
            | JoinOperator::Right(JoinConstraint::Using(cols))
            | JoinOperator::FullOuter(JoinConstraint::Using(cols)) => {
                return self.execute_join_with_using_base(
                    select,
                    &left_table_alias,
                    &right_table_alias,
                    &left_schema,
                    &right_schema,
                    left_rows,
                    right_rows,
                    cols,
                    join_type,
                );
            }
            JoinOperator::Inner(JoinConstraint::Natural)
            | JoinOperator::Join(JoinConstraint::Natural)
            | JoinOperator::LeftOuter(JoinConstraint::Natural)
            | JoinOperator::Left(JoinConstraint::Natural)
            | JoinOperator::RightOuter(JoinConstraint::Natural)
            | JoinOperator::Right(JoinConstraint::Natural)
            | JoinOperator::FullOuter(JoinConstraint::Natural) => {
                return self.execute_natural_join_base(
                    select,
                    &left_table_alias,
                    &right_table_alias,
                    &left_schema,
                    &right_schema,
                    left_rows,
                    right_rows,
                    join_type,
                );
            }
            _ => {
                return Err(Error::UnsupportedFeature(
                    "Unsupported JOIN constraint".to_string(),
                ));
            }
        };

        let (joined_schema, joined_rows) = self.nested_loop_join(NestedLoopJoinParams {
            left_schema: &left_schema,
            left_rows,
            right_schema: &right_schema,
            right_rows,
            join_type,
            join_condition,
            left_table_name: &left_table_alias,
            right_table_name: &right_table_alias,
        })?;

        let filtered_rows = if let Some(ref where_expr) = select.selection {
            let evaluator = ExpressionEvaluator::new(&joined_schema);
            joined_rows
                .into_iter()
                .filter(|row| evaluator.evaluate_where(where_expr, row).unwrap_or(false))
                .collect()
        } else {
            joined_rows
        };

        Ok((joined_schema, filtered_rows))
    }

    fn execute_multi_join_base(
        &mut self,
        select: &Select,
        table_with_joins: &TableWithJoins,
    ) -> Result<(Schema, Vec<Row>)> {
        let (mut current_schema, mut current_rows) =
            self.get_table_data(&table_with_joins.relation)?;
        let mut current_alias = self.extract_table_alias(&table_with_joins.relation)?;

        for join in &table_with_joins.joins {
            let (right_schema, right_rows) = self.get_table_data(&join.relation)?;
            let right_table_alias = self.extract_table_alias(&join.relation)?;

            let join_type = match &join.join_operator {
                JoinOperator::Inner(_) | JoinOperator::Join(_) => JoinType::Inner,
                JoinOperator::LeftOuter(_) | JoinOperator::Left(_) => JoinType::Left,
                JoinOperator::RightOuter(_) | JoinOperator::Right(_) => JoinType::Right,
                JoinOperator::FullOuter(_) => JoinType::Full,
                JoinOperator::CrossJoin(_) => JoinType::Cross,
                other => {
                    return Err(Error::UnsupportedFeature(format!(
                        "Unsupported join operator: {:?}",
                        other
                    )));
                }
            };

            let cross_join_condition = sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                value: sqlparser::ast::Value::Boolean(true),
                span: sqlparser::tokenizer::Span::empty(),
            });
            let join_condition: &sqlparser::ast::Expr = match &join.join_operator {
                JoinOperator::Inner(JoinConstraint::On(expr))
                | JoinOperator::Join(JoinConstraint::On(expr))
                | JoinOperator::LeftOuter(JoinConstraint::On(expr))
                | JoinOperator::Left(JoinConstraint::On(expr))
                | JoinOperator::RightOuter(JoinConstraint::On(expr))
                | JoinOperator::Right(JoinConstraint::On(expr))
                | JoinOperator::FullOuter(JoinConstraint::On(expr)) => expr,
                JoinOperator::CrossJoin(_) => &cross_join_condition,
                _ => {
                    return Err(Error::UnsupportedFeature(
                        "Only ON clause supported in multi-join".to_string(),
                    ));
                }
            };

            let (joined_schema, joined_rows) = self.nested_loop_join(NestedLoopJoinParams {
                left_schema: &current_schema,
                left_rows: current_rows,
                right_schema: &right_schema,
                right_rows,
                join_type,
                join_condition,
                left_table_name: &current_alias,
                right_table_name: &right_table_alias,
            })?;

            current_schema = joined_schema;
            current_rows = joined_rows;
            current_alias = format!("{}_{}", current_alias, right_table_alias);
        }

        let filtered_rows = if let Some(ref where_expr) = select.selection {
            let evaluator = ExpressionEvaluator::new(&current_schema);
            current_rows
                .into_iter()
                .filter(|row| evaluator.evaluate_where(where_expr, row).unwrap_or(false))
                .collect()
        } else {
            current_rows
        };

        Ok((current_schema, filtered_rows))
    }

    fn execute_join_with_using_base(
        &mut self,
        select: &Select,
        left_table_alias: &str,
        right_table_alias: &str,
        left_schema: &Schema,
        right_schema: &Schema,
        left_rows: Vec<Row>,
        right_rows: Vec<Row>,
        using_cols: &[sqlparser::ast::ObjectName],
        join_type: JoinType,
    ) -> Result<(Schema, Vec<Row>)> {
        let col_names: Vec<String> = using_cols
            .iter()
            .filter_map(|obj_name| {
                obj_name
                    .0
                    .last()
                    .and_then(|part| part.as_ident())
                    .map(|ident| ident.value.clone())
            })
            .collect();
        let condition =
            self.build_using_condition_from_names(&col_names, left_table_alias, right_table_alias)?;

        let (joined_schema, joined_rows) = self.nested_loop_join(NestedLoopJoinParams {
            left_schema,
            left_rows,
            right_schema,
            right_rows,
            join_type,
            join_condition: &condition,
            left_table_name: left_table_alias,
            right_table_name: right_table_alias,
        })?;

        self.finalize_join_result_base(select, joined_schema, joined_rows)
    }

    fn execute_natural_join_base(
        &mut self,
        select: &Select,
        left_table_alias: &str,
        right_table_alias: &str,
        left_schema: &Schema,
        right_schema: &Schema,
        left_rows: Vec<Row>,
        right_rows: Vec<Row>,
        join_type: JoinType,
    ) -> Result<(Schema, Vec<Row>)> {
        let left_col_names: std::collections::HashSet<_> = left_schema
            .fields()
            .iter()
            .map(|f| f.name.to_lowercase())
            .collect();
        let right_col_names: Vec<_> = right_schema
            .fields()
            .iter()
            .map(|f| f.name.to_lowercase())
            .collect();

        let common_cols: Vec<_> = right_col_names
            .into_iter()
            .filter(|name| left_col_names.contains(name))
            .collect();

        let condition = if common_cols.is_empty() {
            sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                value: sqlparser::ast::Value::Boolean(true),
                span: sqlparser::tokenizer::Span::empty(),
            })
        } else {
            self.build_using_condition_from_names(
                &common_cols,
                left_table_alias,
                right_table_alias,
            )?
        };

        let (joined_schema, joined_rows) = self.nested_loop_join(NestedLoopJoinParams {
            left_schema,
            left_rows,
            right_schema,
            right_rows,
            join_type,
            join_condition: &condition,
            left_table_name: left_table_alias,
            right_table_name: right_table_alias,
        })?;

        self.finalize_join_result_base(select, joined_schema, joined_rows)
    }

    fn execute_cartesian_product_query(&mut self, select: &Select) -> Result<Table> {
        let (schema, rows) = self.execute_cartesian_product_base(select)?;

        let filtered_rows = if let Some(ref where_expr) = select.selection {
            let evaluator = ExpressionEvaluator::new(&schema);
            rows.into_iter()
                .filter(|row| evaluator.evaluate_where(where_expr, row).unwrap_or(false))
                .collect()
        } else {
            rows
        };

        let (result_schema, result_rows) =
            self.project_rows(filtered_rows, &schema, &select.projection)?;

        self.rows_to_record_batch(result_schema, result_rows)
    }

    fn execute_cartesian_product_base(&mut self, select: &Select) -> Result<(Schema, Vec<Row>)> {
        let mut all_schemas: Vec<Schema> = Vec::new();
        let mut all_rows_sets: Vec<Vec<Row>> = Vec::new();

        for table_with_joins in &select.from {
            let table_reference = &table_with_joins.relation;
            let table_name = self.extract_table_name(table_reference)?;
            let (dataset_id, table_id) = self.parse_ddl_table_name(&table_name)?;
            let actual_table_id = self.resolve_table_name(&dataset_id, &table_id);

            let schema = {
                let storage = self.storage.borrow_mut();
                let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                    Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
                })?;
                let table = dataset.get_table(&actual_table_id).ok_or_else(|| {
                    Error::TableNotFound(format!(
                        "Table '{}.{}' not found",
                        dataset_id, actual_table_id
                    ))
                })?;
                table.schema().clone()
            };

            let rows = self.scan_table(&dataset_id, &actual_table_id)?;

            all_schemas.push(schema);
            all_rows_sets.push(rows);
        }

        let (product_schema, product_rows) =
            self.compute_cartesian_product(all_schemas, all_rows_sets)?;

        let filtered_rows = if let Some(ref where_expr) = select.selection {
            let evaluator = ExpressionEvaluator::new(&product_schema);
            product_rows
                .into_iter()
                .filter(|row| evaluator.evaluate_where(where_expr, row).unwrap_or(false))
                .collect()
        } else {
            product_rows
        };

        Ok((product_schema, filtered_rows))
    }

    fn compute_cartesian_product(
        &self,
        schemas: Vec<Schema>,
        row_sets: Vec<Vec<Row>>,
    ) -> Result<(Schema, Vec<Row>)> {
        if schemas.is_empty() {
            return Err(Error::InternalError(
                "No schemas provided for Cartesian product".to_string(),
            ));
        }

        let mut combined_fields = Vec::new();
        for schema in &schemas {
            for field in schema.fields() {
                combined_fields.push(field.clone());
            }
        }
        let combined_schema = Schema::from_fields(combined_fields);

        let mut result_rows = vec![Row::from_values(vec![])];

        for rows in row_sets {
            let mut new_result = Vec::new();
            for existing_row in &result_rows {
                for new_row in &rows {
                    let mut combined_values = existing_row.values().to_vec();
                    combined_values.extend_from_slice(new_row.values());
                    new_result.push(Row::from_values(combined_values));
                }
            }
            result_rows = new_result;
        }

        Ok((combined_schema, result_rows))
    }

    fn nested_loop_join(&self, params: NestedLoopJoinParams) -> Result<(Schema, Vec<Row>)> {
        let NestedLoopJoinParams {
            left_schema,
            left_rows,
            right_schema,
            right_rows,
            join_type,
            join_condition,
            left_table_name,
            right_table_name,
        } = params;

        let mut combined_fields = Vec::new();
        let _left_col_count = left_schema.fields().len();

        for field in left_schema.fields() {
            let mut f = field.clone();
            f.source_table = Some(left_table_name.to_string());
            combined_fields.push(f);
        }
        for field in right_schema.fields() {
            let mut f = field.clone();
            f.source_table = Some(right_table_name.to_string());
            combined_fields.push(f);
        }
        let combined_schema = Schema::from_fields(combined_fields);

        let mut result_rows = Vec::new();

        let mut right_matched = vec![false; right_rows.len()];

        for left_row in &left_rows {
            let mut left_matched = false;

            for (right_idx, right_row) in right_rows.iter().enumerate() {
                let mut combined_values = left_row.values().to_vec();
                combined_values.extend_from_slice(right_row.values());
                let combined_row = Row::from_values(combined_values);

                let evaluator = ExpressionEvaluator::new_for_join(
                    &combined_schema,
                    left_schema,
                    right_schema,
                    left_table_name,
                    right_table_name,
                );
                if evaluator
                    .evaluate_where(join_condition, &combined_row)
                    .unwrap_or(false)
                {
                    result_rows.push(combined_row);
                    left_matched = true;
                    right_matched[right_idx] = true;
                }
            }

            if !left_matched && (join_type == JoinType::Left || join_type == JoinType::Full) {
                let mut combined_values = left_row.values().to_vec();

                for _ in 0..right_schema.fields().len() {
                    combined_values.push(Value::null());
                }
                result_rows.push(Row::from_values(combined_values));
            }
        }

        if join_type == JoinType::Right || join_type == JoinType::Full {
            for (right_idx, right_row) in right_rows.iter().enumerate() {
                if !right_matched[right_idx] {
                    let mut combined_values = Vec::new();

                    for _ in 0..left_schema.fields().len() {
                        combined_values.push(Value::null());
                    }
                    combined_values.extend_from_slice(right_row.values());
                    result_rows.push(Row::from_values(combined_values));
                }
            }
        }

        Ok((combined_schema, result_rows))
    }

    fn execute_join_with_using(
        &mut self,
        select: &Select,
        left_table_alias: &str,
        right_table_alias: &str,
        left_schema: &Schema,
        right_schema: &Schema,
        left_rows: Vec<Row>,
        right_rows: Vec<Row>,
        using_cols: &[sqlparser::ast::ObjectName],
        join_type: JoinType,
    ) -> Result<Table> {
        let col_names: Vec<String> = using_cols
            .iter()
            .filter_map(|obj_name| {
                obj_name
                    .0
                    .last()
                    .and_then(|part| part.as_ident())
                    .map(|ident| ident.value.clone())
            })
            .collect();

        let condition =
            self.build_using_condition_from_names(&col_names, left_table_alias, right_table_alias)?;

        let (joined_schema, joined_rows) = self.nested_loop_join(NestedLoopJoinParams {
            left_schema,
            left_rows,
            right_schema,
            right_rows,
            join_type,
            join_condition: &condition,
            left_table_name: left_table_alias,
            right_table_name: right_table_alias,
        })?;

        self.finalize_join_result(select, joined_schema, joined_rows)
    }

    fn execute_natural_join(
        &mut self,
        select: &Select,
        left_table_alias: &str,
        right_table_alias: &str,
        left_schema: &Schema,
        right_schema: &Schema,
        left_rows: Vec<Row>,
        right_rows: Vec<Row>,
        join_type: JoinType,
    ) -> Result<Table> {
        let left_col_names: std::collections::HashSet<_> = left_schema
            .fields()
            .iter()
            .map(|f| f.name.to_lowercase())
            .collect();
        let right_col_names: Vec<_> = right_schema
            .fields()
            .iter()
            .map(|f| f.name.to_lowercase())
            .collect();

        let common_cols: Vec<_> = right_col_names
            .into_iter()
            .filter(|name| left_col_names.contains(name))
            .collect();

        let condition = if common_cols.is_empty() {
            sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                value: sqlparser::ast::Value::Boolean(true),
                span: sqlparser::tokenizer::Span::empty(),
            })
        } else {
            self.build_using_condition_from_names(
                &common_cols,
                left_table_alias,
                right_table_alias,
            )?
        };

        let (joined_schema, joined_rows) = self.nested_loop_join(NestedLoopJoinParams {
            left_schema,
            left_rows,
            right_schema,
            right_rows,
            join_type,
            join_condition: &condition,
            left_table_name: left_table_alias,
            right_table_name: right_table_alias,
        })?;

        self.finalize_join_result(select, joined_schema, joined_rows)
    }

    fn build_using_condition(
        &self,
        using_cols: &[sqlparser::ast::Ident],
        left_alias: &str,
        right_alias: &str,
    ) -> Result<sqlparser::ast::Expr> {
        let col_names: Vec<String> = using_cols.iter().map(|c| c.value.clone()).collect();
        self.build_using_condition_from_names(&col_names, left_alias, right_alias)
    }

    fn build_using_condition_from_names(
        &self,
        col_names: &[String],
        left_alias: &str,
        right_alias: &str,
    ) -> Result<sqlparser::ast::Expr> {
        use sqlparser::ast::{BinaryOperator, Expr, Ident, ObjectName};

        if col_names.is_empty() {
            return Ok(Expr::Value(sqlparser::ast::ValueWithSpan {
                value: sqlparser::ast::Value::Boolean(true),
                span: sqlparser::tokenizer::Span::empty(),
            }));
        }

        let mut conditions: Vec<Expr> = Vec::new();

        for col_name in col_names {
            let left_col = Expr::CompoundIdentifier(vec![
                Ident::new(left_alias),
                Ident::new(col_name.clone()),
            ]);
            let right_col = Expr::CompoundIdentifier(vec![
                Ident::new(right_alias),
                Ident::new(col_name.clone()),
            ]);

            conditions.push(Expr::BinaryOp {
                left: Box::new(left_col),
                op: BinaryOperator::Eq,
                right: Box::new(right_col),
            });
        }

        let mut result = conditions.remove(0);
        for cond in conditions {
            result = Expr::BinaryOp {
                left: Box::new(result),
                op: BinaryOperator::And,
                right: Box::new(cond),
            };
        }

        Ok(result)
    }

    fn finalize_join_result_base(
        &self,
        select: &Select,
        joined_schema: Schema,
        joined_rows: Vec<Row>,
    ) -> Result<(Schema, Vec<Row>)> {
        let filtered_rows = if let Some(ref where_expr) = select.selection {
            let evaluator = ExpressionEvaluator::new(&joined_schema);
            joined_rows
                .into_iter()
                .filter(|row| evaluator.evaluate_where(where_expr, row).unwrap_or(false))
                .collect()
        } else {
            joined_rows
        };

        Ok((joined_schema, filtered_rows))
    }

    fn finalize_join_result(
        &mut self,
        select: &Select,
        joined_schema: Schema,
        joined_rows: Vec<Row>,
    ) -> Result<Table> {
        let (schema, filtered_rows) =
            self.finalize_join_result_base(select, joined_schema, joined_rows)?;

        let (result_schema, result_rows) =
            self.project_rows(filtered_rows, &schema, &select.projection)?;

        self.rows_to_record_batch(result_schema, result_rows)
    }

    fn apply_distinct_order_limit_offset(
        &self,
        batch: Table,
        distinct: bool,
        order_by: Option<&OrderBy>,
        limit_clause: Option<&LimitClause>,
        fetch: Option<&Fetch>,
        select_projection: Option<&[SelectItem]>,
        top_clause: Option<&sqlparser::ast::Top>,
    ) -> Result<Table> {
        let schema = batch.schema().clone();
        let mut rows: Vec<Row> = batch.rows()?;

        let projected_column_count = if let Some(projection) = select_projection {
            let has_wildcard = projection.iter().any(|item| {
                matches!(
                    item,
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
                )
            });
            if has_wildcard {
                schema.fields().len()
            } else {
                projection.len()
            }
        } else {
            schema.fields().len()
        };

        if distinct {
            rows = self.deduplicate_rows(rows);
        }

        if let Some(order_by_clause) = order_by {
            let expressions = match &order_by_clause.kind {
                OrderByKind::Expressions(exprs) => exprs,
                OrderByKind::All(_) => {
                    return Err(Error::UnsupportedFeature(
                        "ORDER BY ALL is not supported".to_string(),
                    ));
                }
            };
            self.sort_rows(&mut rows, &schema, expressions)?;
        }

        let fetch_limit_value = if let Some(fetch_clause) = fetch {
            match fetch_clause.quantity {
                Some(ref expr) => Some(self.extract_limit_offset_value(expr)?),
                None => Some(1),
            }
        } else {
            None
        };

        let top_limit_value = if let Some(top) = top_clause {
            if top.percent {
                return Err(Error::UnsupportedFeature(
                    "TOP ... PERCENT is not yet supported".to_string(),
                ));
            }
            if top.with_ties {
                return Err(Error::UnsupportedFeature(
                    "TOP ... WITH TIES is not yet supported".to_string(),
                ));
            }
            if let Some(ref quantity) = top.quantity {
                use sqlparser::ast::TopQuantity;
                match quantity {
                    TopQuantity::Constant(n) => Some(*n as usize),
                    TopQuantity::Expr(expr) => Some(self.extract_limit_offset_value(expr)?),
                }
            } else {
                None
            }
        } else {
            None
        };

        let (limit_expr, offset_expr) = match limit_clause {
            Some(LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            }) => {
                if !limit_by.is_empty() {
                    return Err(Error::UnsupportedFeature(
                        "LIMIT ... BY is not supported".to_string(),
                    ));
                }
                (
                    limit.as_ref(),
                    offset.as_ref().map(|offset_clause| &offset_clause.value),
                )
            }
            Some(LimitClause::OffsetCommaLimit { offset, limit }) => (Some(limit), Some(offset)),
            None => (None, None),
        };

        let offset_value = if let Some(offset_expr) = offset_expr {
            self.extract_limit_offset_value(offset_expr)?
        } else {
            0
        };

        let limit_value = if let Some(limit_expr) = limit_expr {
            Some(self.extract_limit_offset_value(limit_expr)?)
        } else {
            None
        };

        let final_limit = {
            let mut limits = Vec::new();
            if let Some(top) = top_limit_value {
                limits.push(top);
            }
            if let Some(limit) = limit_value {
                limits.push(limit);
            }
            if let Some(fetch) = fetch_limit_value {
                limits.push(fetch);
            }
            if limits.is_empty() {
                None
            } else {
                Some(*limits.iter().min().expect("checked non-empty"))
            }
        };

        let start = offset_value.min(rows.len());
        let end = if let Some(limit) = final_limit {
            (start + limit).min(rows.len())
        } else {
            rows.len()
        };

        let sliced_rows = rows[start..end].to_vec();

        if schema.fields().len() > projected_column_count {
            let final_fields = schema.fields()[..projected_column_count].to_vec();
            let final_schema = Schema::from_fields(final_fields);

            let final_rows: Vec<Row> = sliced_rows
                .iter()
                .map(|row| {
                    let trimmed_values = row.values()[..projected_column_count].to_vec();
                    Row::from_values(trimmed_values)
                })
                .collect();

            self.rows_to_record_batch(final_schema, final_rows)
        } else {
            self.rows_to_record_batch(schema, sliced_rows)
        }
    }

    pub(crate) fn extract_limit_offset_value(&self, expr: &Expr) -> Result<usize> {
        match expr {
            Expr::Value(value_with_span) => match &value_with_span.value {
                sqlparser::ast::Value::Number(n, _) => {
                    if n.starts_with('-') {
                        return Err(Error::InvalidQuery(
                            "LIMIT and OFFSET must not be negative".to_string(),
                        ));
                    }
                    n.parse::<usize>().map_err(|_| {
                        Error::InvalidQuery(format!("Invalid LIMIT/OFFSET value: {}", n))
                    })
                }
                _ => Err(Error::InvalidQuery(
                    "LIMIT/OFFSET must be a literal integer".to_string(),
                )),
            },
            _ => Err(Error::InvalidQuery(
                "LIMIT/OFFSET must be a literal integer".to_string(),
            )),
        }
    }

    pub(crate) fn sort_rows(
        &self,
        rows: &mut [Row],
        schema: &Schema,
        order_exprs: &[OrderByExpr],
    ) -> Result<()> {
        let mut sort_specs: Vec<(SortSpec, bool, Option<bool>, Option<Vec<String>>)> = Vec::new();

        for order_expr in order_exprs {
            let (sort_spec, enum_labels) = if let Ok((table_qualifier, col_name)) =
                self.extract_qualified_column_name(&order_expr.expr)
            {
                let col_idx = if let Some(ref table) = table_qualifier {
                    schema
                        .fields()
                        .iter()
                        .position(|f| {
                            f.source_table.as_deref() == Some(table.as_str()) && f.name == col_name
                        })
                        .or_else(|| {
                            let qualified_name = format!("{}.{}", table, col_name);
                            schema
                                .fields()
                                .iter()
                                .position(|f| f.name == qualified_name)
                        })
                        .or_else(|| schema.fields().iter().position(|f| f.name == col_name))
                } else {
                    schema.fields().iter().position(|f| f.name == col_name)
                };

                if let Some(idx) = col_idx {
                    let labels = match &schema.fields()[idx].data_type {
                        DataType::Enum { labels, .. } => Some(labels.clone()),
                        _ => None,
                    };
                    (SortSpec::Column(idx), labels)
                } else {
                    return Err(Error::ColumnNotFound(col_name.clone()));
                }
            } else {
                (
                    SortSpec::Expression(Box::new(order_expr.expr.clone())),
                    None,
                )
            };

            let is_ascending = order_expr.options.asc.unwrap_or(true);
            let nulls_first = order_expr.options.nulls_first;
            sort_specs.push((sort_spec, is_ascending, nulls_first, enum_labels));
        }

        let evaluator = ExpressionEvaluator::new(schema);

        rows.sort_by(|a, b| {
            for (spec, is_ascending, nulls_first, enum_labels) in &sort_specs {
                let (val_a, val_b) = match spec {
                    SortSpec::Column(col_idx) => {
                        (a.values()[*col_idx].clone(), b.values()[*col_idx].clone())
                    }
                    SortSpec::Expression(expr) => {
                        let eval_a = evaluator
                            .evaluate_expr(expr, a)
                            .unwrap_or_else(|_| Value::null());
                        let eval_b = evaluator
                            .evaluate_expr(expr, b)
                            .unwrap_or_else(|_| Value::null());
                        (eval_a, eval_b)
                    }
                };

                let cmp = if let Some(labels) = enum_labels {
                    compare_enum_values_with_nulls_for_sort(
                        &val_a,
                        &val_b,
                        labels,
                        *is_ascending,
                        *nulls_first,
                    )
                } else {
                    compare_values_with_nulls_for_sort(&val_a, &val_b, *is_ascending, *nulls_first)
                };

                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
        });

        Ok(())
    }

    fn project_rows_with_order_by(
        &self,
        rows: Vec<Row>,
        schema: &Schema,
        select_items: &[SelectItem],
        order_by_columns: &[String],
    ) -> Result<(Schema, Vec<Row>)> {
        let (projected_schema, projected_rows) =
            self.project_rows(rows.clone(), schema, select_items)?;

        let missing_order_by_cols: Vec<&String> = order_by_columns
            .iter()
            .filter(|col| !projected_schema.fields().iter().any(|f| &f.name == *col))
            .collect();

        if missing_order_by_cols.is_empty() {
            return Ok((projected_schema, projected_rows));
        }

        let mut new_fields = projected_schema.fields().to_vec();
        let mut additional_col_indices = Vec::new();

        for col_name in &missing_order_by_cols {
            if let Some(field) = schema.fields().iter().find(|f| &f.name == *col_name) {
                new_fields.push(field.clone());

                let orig_idx = schema
                    .fields()
                    .iter()
                    .position(|f| &f.name == *col_name)
                    .ok_or_else(|| Error::ColumnNotFound((*col_name).clone()))?;
                additional_col_indices.push(orig_idx);
            }
        }

        let new_schema = Schema::from_fields(new_fields);

        let mut extended_rows = Vec::new();
        for (orig_row, proj_row) in rows.iter().zip(projected_rows.iter()) {
            let mut extended_values = proj_row.values().to_vec();

            for &col_idx in &additional_col_indices {
                extended_values.push(orig_row.values()[col_idx].clone());
            }

            extended_rows.push(Row::from_values(extended_values));
        }

        Ok((new_schema, extended_rows))
    }

    fn deduplicate_rows(&self, rows: Vec<Row>) -> Vec<Row> {
        use std::collections::HashSet;

        let mut seen = HashSet::new();
        let mut unique_rows = Vec::new();

        for row in rows {
            let key = self.row_to_key(&row);

            if seen.insert(key) {
                unique_rows.push(row);
            }
        }

        unique_rows
    }

    fn filter_having(
        &self,
        aggregated: Vec<(Vec<Value>, Vec<Value>)>,
        having_expr: &Expr,
        result_schema: &Schema,
        agg_expr_to_col_idx: &std::collections::HashMap<String, usize>,
    ) -> Result<Vec<(Vec<Value>, Vec<Value>)>> {
        let mut filtered = Vec::new();

        for (group_values, agg_values) in &aggregated {
            let mut combined_values = group_values.clone();
            combined_values.extend(agg_values.clone());
            let combined_row = Row::from_values(combined_values);

            if self.evaluate_having_expression(
                having_expr,
                &combined_row,
                result_schema,
                agg_expr_to_col_idx,
            )? {
                filtered.push((group_values.clone(), agg_values.clone()));
            }
        }

        Ok(filtered)
    }

    fn evaluate_having_expression(
        &self,
        expr: &Expr,
        row: &Row,
        schema: &Schema,
        agg_expr_to_col_idx: &std::collections::HashMap<String, usize>,
    ) -> Result<bool> {
        use sqlparser::ast::BinaryOperator;

        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_value =
                    self.evaluate_having_value(left, row, schema, agg_expr_to_col_idx)?;
                let right_value =
                    self.evaluate_having_value(right, row, schema, agg_expr_to_col_idx)?;

                match op {
                    BinaryOperator::Gt => Ok(self.compare_values(&left_value, &right_value)? > 0),
                    BinaryOperator::Lt => Ok(self.compare_values(&left_value, &right_value)? < 0),
                    BinaryOperator::GtEq => {
                        Ok(self.compare_values(&left_value, &right_value)? >= 0)
                    }
                    BinaryOperator::LtEq => {
                        Ok(self.compare_values(&left_value, &right_value)? <= 0)
                    }
                    BinaryOperator::Eq => Ok(left_value == right_value),
                    BinaryOperator::NotEq => Ok(left_value != right_value),
                    BinaryOperator::And => {
                        let left_bool = self.value_to_bool(&left_value)?;
                        let right_bool = self.value_to_bool(&right_value)?;
                        Ok(left_bool && right_bool)
                    }
                    BinaryOperator::Or => {
                        let left_bool = self.value_to_bool(&left_value)?;
                        let right_bool = self.value_to_bool(&right_value)?;
                        Ok(left_bool || right_bool)
                    }
                    _ => Err(Error::UnsupportedFeature(format!(
                        "Operator {:?} not supported in HAVING",
                        op
                    ))),
                }
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Expression {:?} not supported in HAVING",
                expr
            ))),
        }
    }

    fn evaluate_having_value(
        &self,
        expr: &Expr,
        row: &Row,
        schema: &Schema,
        agg_expr_to_col_idx: &std::collections::HashMap<String, usize>,
    ) -> Result<Value> {
        match expr {
            Expr::Function(func) => {
                let agg_key = self.aggregate_function_to_column_name(func)?;

                if let Some(&col_idx) = agg_expr_to_col_idx.get(&agg_key) {
                    return Ok(row.values()[col_idx].clone());
                }

                if let Some(col_idx) = schema.fields().iter().position(|f| f.name == agg_key) {
                    Ok(row.values()[col_idx].clone())
                } else {
                    Err(Error::ColumnNotFound(format!(
                        "Aggregate function {} not found in GROUP BY result",
                        agg_key
                    )))
                }
            }

            Expr::Value(val) => match &val.value {
                sqlparser::ast::Value::Number(n, _) => {
                    if let Ok(i) = n.parse::<i64>() {
                        Ok(Value::int64(i))
                    } else if let Ok(f) = n.parse::<f64>() {
                        Ok(Value::float64(f))
                    } else {
                        Err(Error::InvalidQuery(format!("Invalid number: {}", n)))
                    }
                }
                sqlparser::ast::Value::SingleQuotedString(s)
                | sqlparser::ast::Value::DoubleQuotedString(s) => Ok(Value::string(s.clone())),
                sqlparser::ast::Value::Boolean(b) => Ok(Value::bool_val(*b)),
                sqlparser::ast::Value::Null => Ok(Value::null()),
                _ => Err(Error::UnsupportedFeature(format!(
                    "Value {:?} not supported in HAVING",
                    val.value
                ))),
            },

            Expr::Identifier(ident) => {
                let col_name = ident.value.clone();
                if let Some(col_idx) = schema.fields().iter().position(|f| f.name == col_name) {
                    Ok(row.values()[col_idx].clone())
                } else {
                    Err(Error::ColumnNotFound(col_name))
                }
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Expression {:?} not supported in HAVING value",
                expr
            ))),
        }
    }

    fn aggregate_function_to_column_name(&self, func: &sqlparser::ast::Function) -> Result<String> {
        use sqlparser::ast::{FunctionArgumentList, FunctionArguments};

        let func_name = func.name.to_string().to_uppercase();

        let arg_str = match &func.args {
            FunctionArguments::List(FunctionArgumentList { args, .. }) => {
                if args.is_empty() {
                    "*".to_string()
                } else {
                    let arg_strs: Vec<String> = args
                        .iter()
                        .map(|arg| match arg {
                            sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Wildcard,
                            ) => "*".to_string(),
                            sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Expr(e),
                            ) => match e {
                                Expr::Identifier(ident) => ident.value.clone(),
                                _ => format!("{}", e),
                            },
                            sqlparser::ast::FunctionArg::Named { name, arg, .. } => {
                                let arg_val = match arg {
                                    sqlparser::ast::FunctionArgExpr::Expr(e) => format!("{}", e),
                                    sqlparser::ast::FunctionArgExpr::Wildcard => "*".to_string(),
                                    _ => "?".to_string(),
                                };
                                format!("{} => {}", name, arg_val)
                            }
                            _ => "?".to_string(),
                        })
                        .collect();
                    arg_strs.join(", ")
                }
            }
            FunctionArguments::None => "*".to_string(),
            FunctionArguments::Subquery(_) => {
                return Err(Error::UnsupportedFeature(
                    "Subquery function arguments not supported".to_string(),
                ));
            }
        };

        Ok(format!("{}({})", func_name, arg_str))
    }

    fn aggregate_function_to_column_name_from_expr(&self, expr: &Expr) -> Result<String> {
        match expr {
            Expr::Function(func) => self.aggregate_function_to_column_name(func),
            _ => Err(Error::InvalidQuery(format!(
                "Expected aggregate function expression, got {:?}",
                expr
            ))),
        }
    }

    fn compare_values(&self, a: &Value, b: &Value) -> Result<i32> {
        if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
            return Ok(x.cmp(&y) as i32);
        }

        if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
            return if x < y {
                Ok(-1)
            } else if x > y {
                Ok(1)
            } else {
                Ok(0)
            };
        }

        if let (Some(x), Some(y)) = (a.as_i64(), b.as_f64()) {
            let x_f = x as f64;
            return if x_f < y {
                Ok(-1)
            } else if x_f > y {
                Ok(1)
            } else {
                Ok(0)
            };
        }

        if let (Some(x), Some(y)) = (a.as_f64(), b.as_i64()) {
            let y_f = y as f64;
            return if x < y_f {
                Ok(-1)
            } else if x > y_f {
                Ok(1)
            } else {
                Ok(0)
            };
        }

        if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
            return Ok(x.cmp(y) as i32);
        }

        Err(Error::InvalidQuery(format!(
            "Cannot compare {:?} and {:?}",
            a, b
        )))
    }

    fn value_to_bool(&self, val: &Value) -> Result<bool> {
        if let Some(b) = val.as_bool() {
            Ok(b)
        } else {
            Err(Error::InvalidQuery(format!(
                "Cannot convert {:?} to boolean",
                val
            )))
        }
    }

    fn has_window_functions(&self, projection: &[SelectItem]) -> bool {
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    if self.is_window_function_expr(expr) {
                        return true;
                    }
                }
                _ => {}
            }
        }
        false
    }

    fn is_window_function_expr(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function(func) => func.over.is_some(),
            _ => false,
        }
    }

    fn has_aggregate_functions(&self, projection: &[SelectItem]) -> bool {
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    if let Expr::Function(func) = expr {
                        if self.is_aggregate_function_expr(expr) && func.over.is_none() {
                            return true;
                        }
                    }

                    if self.contains_aggregate_function(expr) {
                        return true;
                    }
                }
                _ => {}
            }
        }
        false
    }

    fn is_aggregate_function_expr(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                matches!(
                    func_name.as_str(),
                    "COUNT"
                        | "SUM"
                        | "AVG"
                        | "MIN"
                        | "MAX"
                        | "STRING_AGG"
                        | "ARRAY_AGG"
                        | "STDDEV"
                        | "STDDEV_POP"
                        | "STDDEV_SAMP"
                        | "VARIANCE"
                        | "VAR_POP"
                        | "VAR_SAMP"
                        | "CORR"
                        | "COVAR_POP"
                        | "COVAR_SAMP"
                        | "MEDIAN"
                        | "MODE"
                        | "PERCENTILE_CONT"
                        | "PERCENTILE_DISC"
                        | "REGR_SLOPE"
                        | "REGR_INTERCEPT"
                        | "REGR_R2"
                        | "REGR_AVGX"
                        | "REGR_AVGY"
                        | "REGR_COUNT"
                        | "REGR_SXX"
                        | "REGR_SYY"
                        | "REGR_SXY"
                        | "BOOL_AND"
                        | "BOOL_OR"
                        | "EVERY"
                        | "BIT_AND"
                        | "BIT_OR"
                        | "BIT_XOR"
                        | "APPROX_COUNT_DISTINCT"
                        | "APPROX_QUANTILES"
                        | "APPROX_TOP_COUNT"
                        | "APPROX_TOP_SUM"
                        | "LISTAGG"
                        | "COUNTIF"
                        | "LOGICAL_AND"
                        | "LOGICAL_OR"
                        | "ANY_VALUE"
                        | "ARRAY_AGG_DISTINCT"
                        | "ARRAY_CONCAT_AGG"
                        | "STRING_AGG_DISTINCT"
                        | "HLL_COUNT_INIT"
                        | "HLL_COUNT_MERGE"
                        | "HLL_COUNT_EXTRACT"
                        | "JSON_AGG"
                        | "JSONB_AGG"
                        | "JSON_OBJECT_AGG"
                        | "JSONB_OBJECT_AGG"
                        | "FIRST_VALUE"
                        | "LAST_VALUE"
                        | "NTH_VALUE"
                        | "LAG"
                        | "LEAD"
                        | "ROW_NUMBER"
                        | "RANK"
                        | "DENSE_RANK"
                        | "NTILE"
                        | "PERCENT_RANK"
                        | "CUME_DIST"
                        | "UNIQ"
                        | "UNIQ_EXACT"
                        | "UNIQ_COMBINED"
                        | "UNIQ_COMBINED_64"
                        | "UNIQ_HLL12"
                        | "UNIQ_THETA_SKETCH"
                        | "UNIQ_UPTO"
                        | "TOP_K"
                        | "TOP_K_WEIGHTED"
                        | "QUANTILE"
                        | "QUANTILE_EXACT"
                        | "QUANTILE_EXACT_WEIGHTED"
                        | "QUANTILE_TIMING"
                        | "QUANTILE_TIMING_WEIGHTED"
                        | "QUANTILE_TDIGEST"
                        | "QUANTILE_TDIGEST_WEIGHTED"
                        | "QUANTILES"
                        | "QUANTILES_EXACT"
                        | "QUANTILES_TIMING"
                        | "QUANTILES_TDIGEST"
                        | "QUANTILE_DETERMINISTIC"
                        | "QUANTILE_BFLOAT16"
                        | "GROUP_ARRAY"
                        | "GROUP_ARRAY_INSERTAT"
                        | "GROUP_ARRAY_MOVING_AVG"
                        | "GROUP_ARRAY_MOVING_SUM"
                        | "GROUP_ARRAY_SAMPLE"
                        | "GROUP_UNIQ_ARRAY"
                        | "GROUP_BIT_AND"
                        | "GROUP_BIT_OR"
                        | "GROUP_BIT_XOR"
                        | "GROUP_BITMAP"
                        | "GROUP_BITMAP_AND"
                        | "GROUP_BITMAP_OR"
                        | "GROUP_BITMAP_XOR"
                        | "GROUP_CONCAT"
                        | "SUM_WITH_OVERFLOW"
                        | "SUM_MAP"
                        | "MIN_MAP"
                        | "MAX_MAP"
                        | "ARG_MIN"
                        | "ARG_MAX"
                        | "SUM_IF"
                        | "AVG_IF"
                        | "MIN_IF"
                        | "MAX_IF"
                        | "COUNT_EQUAL"
                        | "RANK_CORR"
                        | "EXPONENTIAL_MOVING_AVERAGE"
                        | "SIMPLE_LINEAR_REGRESSION"
                        | "BOUNDING_RATIO"
                        | "CONTINGENCY"
                        | "CRAMERS_V"
                        | "ENTROPY"
                        | "THEIL_U"
                        | "CATEGORICAL_INFORMATION_VALUE"
                        | "MANN_WHITNEY_U_TEST"
                        | "STUDENT_T_TEST"
                        | "WELCH_T_TEST"
                        | "RETENTION"
                        | "WINDOW_FUNNEL"
                        | "INTERVAL_LENGTH_SUM"
                        | "SEQUENCE_MATCH"
                        | "SEQUENCE_COUNT"
                        | "DELTA_SUM"
                        | "DELTA_SUM_TIMESTAMP"
                        | "ANY"
                        | "ANY_HEAVY"
                        | "ANY_LAST"
                        | "ARRAY_FLATTEN"
                        | "ARRAY_REDUCE"
                        | "ARRAY_MAP"
                        | "ARRAY_FILTER"
                        | "SUM_ARRAY"
                        | "AVG_ARRAY"
                        | "MIN_ARRAY"
                        | "MAX_ARRAY"
                        | "BITMAP_CARDINALITY"
                        | "BITMAP_AND_CARDINALITY"
                        | "BITMAP_OR_CARDINALITY"
                )
            }
            _ => false,
        }
    }

    fn contains_window_function(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function(func) if func.over.is_some() => true,
            Expr::BinaryOp { left, right, .. } => {
                self.contains_window_function(left) || self.contains_window_function(right)
            }
            Expr::UnaryOp { expr, .. } => self.contains_window_function(expr),
            Expr::Nested(inner) => self.contains_window_function(inner),
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                operand
                    .as_ref()
                    .map(|e| self.contains_window_function(e))
                    .unwrap_or(false)
                    || conditions.iter().any(|c| {
                        self.contains_window_function(&c.condition)
                            || self.contains_window_function(&c.result)
                    })
                    || else_result
                        .as_ref()
                        .map(|e| self.contains_window_function(e))
                        .unwrap_or(false)
            }
            _ => false,
        }
    }

    fn contains_aggregate_function(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function(func)
                if self.is_aggregate_function_expr(expr) && func.over.is_none() =>
            {
                true
            }

            Expr::Function(func) => {
                if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                    arg_list.args.iter().any(|arg| {
                        if let sqlparser::ast::FunctionArg::Unnamed(arg_expr) = arg {
                            match arg_expr {
                                sqlparser::ast::FunctionArgExpr::Expr(e) => {
                                    self.contains_aggregate_function(e)
                                }
                                sqlparser::ast::FunctionArgExpr::Wildcard => false,
                                sqlparser::ast::FunctionArgExpr::QualifiedWildcard(_) => false,
                            }
                        } else {
                            false
                        }
                    })
                } else {
                    false
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.contains_aggregate_function(left) || self.contains_aggregate_function(right)
            }
            Expr::UnaryOp { expr, .. } => self.contains_aggregate_function(expr),
            Expr::Nested(inner) => self.contains_aggregate_function(inner),
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                operand
                    .as_ref()
                    .map(|e| self.contains_aggregate_function(e))
                    .unwrap_or(false)
                    || conditions.iter().any(|c| {
                        self.contains_aggregate_function(&c.condition)
                            || self.contains_aggregate_function(&c.result)
                    })
                    || else_result
                        .as_ref()
                        .map(|e| self.contains_aggregate_function(e))
                        .unwrap_or(false)
            }
            _ => false,
        }
    }

    fn extract_inner_aggregate(&self, expr: &Expr) -> Option<Expr> {
        match expr {
            Expr::Function(func)
                if self.is_aggregate_function_expr(expr) && func.over.is_none() =>
            {
                Some(expr.clone())
            }

            Expr::Function(func) => {
                if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                    for arg in &arg_list.args {
                        if let sqlparser::ast::FunctionArg::Unnamed(arg_expr) = arg {
                            if let sqlparser::ast::FunctionArgExpr::Expr(e) = arg_expr {
                                if let Some(found) = self.extract_inner_aggregate(e) {
                                    return Some(found);
                                }
                            }
                        }
                    }
                }
                None
            }

            Expr::BinaryOp { left, right, .. } => self
                .extract_inner_aggregate(left)
                .or_else(|| self.extract_inner_aggregate(right)),

            Expr::UnaryOp { expr: inner, .. } => self.extract_inner_aggregate(inner),

            Expr::Nested(inner) => self.extract_inner_aggregate(inner),

            _ => None,
        }
    }

    fn extract_having_aggregates(
        &self,
        expr: &Expr,
        func_registry: &FunctionRegistry,
        _select_exprs: &[Expr],
        aggregate_specs: &mut Vec<AggregateSpec>,
        having_agg_indices: &mut std::collections::HashMap<String, usize>,
    ) -> Result<()> {
        match expr {
            Expr::Function(func)
                if self.is_aggregate_function_expr(expr) && func.over.is_none() =>
            {
                let agg_key = self.aggregate_function_to_column_name(func)?;

                if let std::collections::hash_map::Entry::Vacant(e) =
                    having_agg_indices.entry(agg_key)
                {
                    if let Some(agg_spec) = AggregateSpec::from_expr(expr, func_registry)? {
                        let idx = aggregate_specs.len();
                        aggregate_specs.push(agg_spec);
                        e.insert(idx);
                    }
                }
                Ok(())
            }

            Expr::BinaryOp { left, right, .. } => {
                self.extract_having_aggregates(
                    left,
                    func_registry,
                    _select_exprs,
                    aggregate_specs,
                    having_agg_indices,
                )?;
                self.extract_having_aggregates(
                    right,
                    func_registry,
                    _select_exprs,
                    aggregate_specs,
                    having_agg_indices,
                )?;
                Ok(())
            }

            Expr::UnaryOp { expr: inner, .. } => {
                self.extract_having_aggregates(
                    inner,
                    func_registry,
                    _select_exprs,
                    aggregate_specs,
                    having_agg_indices,
                )?;
                Ok(())
            }

            Expr::Nested(inner) => {
                self.extract_having_aggregates(
                    inner,
                    func_registry,
                    _select_exprs,
                    aggregate_specs,
                    having_agg_indices,
                )?;
                Ok(())
            }

            Expr::Function(func) => {
                if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                    for arg in &arg_list.args {
                        if let sqlparser::ast::FunctionArg::Unnamed(arg_expr) = arg {
                            if let sqlparser::ast::FunctionArgExpr::Expr(e) = arg_expr {
                                self.extract_having_aggregates(
                                    e,
                                    func_registry,
                                    _select_exprs,
                                    aggregate_specs,
                                    having_agg_indices,
                                )?;
                            }
                        }
                    }
                }
                Ok(())
            }

            _ => Ok(()),
        }
    }

    fn evaluate_wrapped_aggregate(
        &self,
        wrapper_expr: &Expr,
        inner_agg_expr: &Expr,
        agg_value: &Value,
    ) -> Result<Value> {
        let agg_col_name = format!("{}", inner_agg_expr);
        let agg_data_type = agg_value.data_type();
        let synthetic_schema =
            Schema::from_fields(vec![Field::nullable(agg_col_name.clone(), agg_data_type)]);
        let synthetic_row = Row::from_values(vec![agg_value.clone()]);

        let substituted_expr =
            self.substitute_aggregate_with_column(wrapper_expr, inner_agg_expr, &agg_col_name);

        let evaluator = ExpressionEvaluator::new(&synthetic_schema);
        evaluator.evaluate_expr(&substituted_expr, &synthetic_row)
    }

    fn substitute_aggregate_with_column(
        &self,
        expr: &Expr,
        inner_agg_expr: &Expr,
        col_name: &str,
    ) -> Expr {
        if format!("{}", expr) == format!("{}", inner_agg_expr) {
            return Expr::Identifier(sqlparser::ast::Ident::new(col_name));
        }

        match expr {
            Expr::Function(func) => {
                if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                    let new_args: Vec<_> = arg_list
                        .args
                        .iter()
                        .map(|arg| match arg {
                            sqlparser::ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                                sqlparser::ast::FunctionArgExpr::Expr(e) => {
                                    sqlparser::ast::FunctionArg::Unnamed(
                                        sqlparser::ast::FunctionArgExpr::Expr(
                                            self.substitute_aggregate_with_column(
                                                e,
                                                inner_agg_expr,
                                                col_name,
                                            ),
                                        ),
                                    )
                                }
                                other => sqlparser::ast::FunctionArg::Unnamed(other.clone()),
                            },
                            other => other.clone(),
                        })
                        .collect();
                    Expr::Function(sqlparser::ast::Function {
                        args: sqlparser::ast::FunctionArguments::List(
                            sqlparser::ast::FunctionArgumentList {
                                duplicate_treatment: arg_list.duplicate_treatment.clone(),
                                args: new_args,
                                clauses: arg_list.clauses.clone(),
                            },
                        ),
                        ..func.clone()
                    })
                } else {
                    expr.clone()
                }
            }
            Expr::BinaryOp { left, right, op } => Expr::BinaryOp {
                left: Box::new(self.substitute_aggregate_with_column(
                    left,
                    inner_agg_expr,
                    col_name,
                )),
                op: op.clone(),
                right: Box::new(self.substitute_aggregate_with_column(
                    right,
                    inner_agg_expr,
                    col_name,
                )),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op: op.clone(),
                expr: Box::new(self.substitute_aggregate_with_column(
                    inner,
                    inner_agg_expr,
                    col_name,
                )),
            },
            Expr::Nested(inner) => Expr::Nested(Box::new(self.substitute_aggregate_with_column(
                inner,
                inner_agg_expr,
                col_name,
            ))),

            other => other.clone(),
        }
    }

    fn validate_where_clause(&self, where_clause: &Expr) -> Result<()> {
        if self.contains_aggregate_function(where_clause) {
            return Err(Error::InvalidQuery(
                "Aggregate functions are not allowed in WHERE clause. Use HAVING instead."
                    .to_string(),
            ));
        }
        if self.contains_window_function(where_clause) {
            return Err(Error::InvalidQuery(
                "Window functions are not allowed in WHERE clause".to_string(),
            ));
        }
        Ok(())
    }

    fn execute_window_select_base(&mut self, select: &Select) -> Result<Table> {
        use super::super::window_functions::WindowExecutor;

        if select.from.is_empty() {
            return Err(Error::InvalidQuery(
                "Window functions require a FROM clause".to_string(),
            ));
        }

        let (schema, rows) = if select.from.len() > 1 {
            let cart_result = self.execute_cartesian_product_base(select)?;
            (cart_result.0, cart_result.1)
        } else {
            let table_with_joins = &select.from[0];

            if !table_with_joins.joins.is_empty() {
                let join_result = self.execute_join_query(select)?;
                let join_schema = join_result.schema().clone();
                let join_rows: Vec<Row> = join_result.rows().unwrap_or_default();
                (join_schema, join_rows)
            } else {
                let table_reference = &table_with_joins.relation;
                let table_name = self.extract_table_name(table_reference)?;
                let (dataset_id, table_id) = self.parse_ddl_table_name(&table_name)?;

                let schema = {
                    let storage = self.storage.borrow_mut();
                    let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                        Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
                    })?;
                    let table = dataset.get_table(&table_id).ok_or_else(|| {
                        Error::TableNotFound(format!(
                            "Table '{}.{}' not found",
                            dataset_id, table_id
                        ))
                    })?;
                    table.schema().clone()
                };

                let rows = self.scan_table(&dataset_id, &table_id)?;

                let filtered_rows = if let Some(ref where_expr) = select.selection {
                    let evaluator = ExpressionEvaluator::new(&schema);
                    rows.into_iter()
                        .filter(|row| evaluator.evaluate_where(where_expr, row).unwrap_or(false))
                        .collect()
                } else {
                    rows
                };

                (schema, filtered_rows)
            }
        };

        let (_base_items, window_specs) =
            self.parse_window_select_items(&select.projection, &schema, &select.named_window)?;

        let mut window_results: Vec<Vec<Value>> = Vec::new();
        let mut window_col_info: Vec<(String, super::super::window_functions::WindowFunction)> =
            Vec::new();

        for (col_name, window_fn) in window_specs {
            let executor = WindowExecutor::new(&schema, rows.clone());
            let results = executor.execute(&window_fn)?;
            window_results.push(results);
            window_col_info.push((col_name, window_fn));
        }

        let final_schema = self.build_window_result_schema(&schema, &window_col_info)?;
        let window_col_names: Vec<String> = window_col_info
            .iter()
            .map(|(name, _)| name.clone())
            .collect();
        let mut final_rows = self.combine_base_and_window_results(
            &rows,
            &window_results,
            &schema,
            &window_col_names,
        )?;

        if let Some(ref qualify_expr) = select.qualify {
            let evaluator = ExpressionEvaluator::new(&final_schema);
            final_rows.retain(|row| evaluator.evaluate_where(qualify_expr, row).unwrap_or(false));
        }

        self.rows_to_record_batch(final_schema, final_rows)
    }

    fn apply_window_projection(&self, batch: Table, projection: &[SelectItem]) -> Result<Table> {
        let schema = batch.schema();
        let rows = batch.rows()?;

        let mut output_fields = Vec::new();
        let mut column_indices = Vec::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    if self.is_window_function_expr(expr) {
                        if let Expr::Function(func) = expr {
                            let func_name = func.name.to_string().to_uppercase();
                            let col_name = format!("{}()", func_name);
                            if let Some(idx) =
                                schema.fields().iter().position(|f| f.name == col_name)
                            {
                                output_fields.push(schema.fields()[idx].clone());
                                column_indices.push(idx);
                            } else {
                                return Err(Error::ColumnNotFound(col_name));
                            }
                        }
                    } else {
                        let col_name = self.extract_column_name_from_expr(expr)?;
                        if let Some(idx) = schema.fields().iter().position(|f| f.name == col_name) {
                            output_fields.push(schema.fields()[idx].clone());
                            column_indices.push(idx);
                        } else {
                            return Err(Error::ColumnNotFound(col_name));
                        }
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let col_name = &alias.value;
                    if let Some(idx) = schema.fields().iter().position(|f| &f.name == col_name) {
                        output_fields.push(schema.fields()[idx].clone());
                        column_indices.push(idx);
                    } else {
                        return Err(Error::ColumnNotFound(col_name.clone()));
                    }
                }
                SelectItem::Wildcard(_) => {
                    for (idx, field) in schema.fields().iter().enumerate() {
                        output_fields.push(field.clone());
                        column_indices.push(idx);
                    }
                }
                _ => {
                    return Err(Error::UnsupportedFeature(
                        "Unsupported SELECT item in window query".to_string(),
                    ));
                }
            }
        }

        let output_schema = Schema::from_fields(output_fields);
        let output_rows: Vec<Row> = rows
            .iter()
            .map(|row| {
                let values: Vec<Value> = column_indices
                    .iter()
                    .map(|&idx| row.values()[idx].clone())
                    .collect();
                Row::from_values(values)
            })
            .collect();

        self.rows_to_record_batch(output_schema, output_rows)
    }

    fn extract_column_name_from_expr(&self, expr: &Expr) -> Result<String> {
        match expr {
            Expr::Identifier(ident) => Ok(ident.value.clone()),
            Expr::CompoundIdentifier(parts) => {
                if parts.len() == 1 {
                    Ok(parts[0].value.clone())
                } else if parts.len() == 2 {
                    Ok(parts[1].value.clone())
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Invalid column reference: {}",
                        parts
                            .iter()
                            .map(|p| p.value.as_str())
                            .collect::<Vec<_>>()
                            .join(".")
                    )))
                }
            }
            _ => Err(Error::UnsupportedFeature(
                "Complex expressions in window SELECT projection not yet supported".to_string(),
            )),
        }
    }

    fn parse_window_select_items(
        &self,
        projection: &[SelectItem],
        schema: &Schema,
        named_windows: &[sqlparser::ast::NamedWindowDefinition],
    ) -> Result<WindowSelectItems> {
        let mut base_items = Vec::new();
        let mut window_specs = Vec::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    if self.is_window_function_expr(expr) {
                        let (col_name, window_fn) =
                            self.parse_window_function_with_named(expr, schema, named_windows)?;
                        window_specs.push((col_name, window_fn));
                    } else {
                        base_items.push(item.clone());
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    if self.is_window_function_expr(expr) {
                        let (_, window_fn) =
                            self.parse_window_function_with_named(expr, schema, named_windows)?;
                        window_specs.push((alias.value.clone(), window_fn));
                    } else {
                        base_items.push(item.clone());
                    }
                }
                SelectItem::Wildcard(_) => {
                    base_items.push(item.clone());
                }
                _ => {
                    return Err(Error::UnsupportedFeature(
                        "Unsupported SELECT item with window functions".to_string(),
                    ));
                }
            }
        }

        Ok((base_items, window_specs))
    }

    fn parse_window_function_with_named(
        &self,
        expr: &Expr,
        schema: &Schema,
        named_windows: &[sqlparser::ast::NamedWindowDefinition],
    ) -> Result<(String, super::super::window_functions::WindowFunction)> {
        let Expr::Function(func) = expr else {
            return Err(Error::InternalError("Not a window function".to_string()));
        };

        let Some(ref over) = func.over else {
            return Err(Error::InternalError("Missing OVER clause".to_string()));
        };

        let func_name = func.name.to_string().to_uppercase();
        let function_type = match func_name.as_str() {
            "ROW_NUMBER" => WindowFunctionType::RowNumber,
            "RANK" => WindowFunctionType::Rank,
            "DENSE_RANK" => WindowFunctionType::DenseRank,
            "PERCENT_RANK" => WindowFunctionType::PercentRank,
            "CUME_DIST" => WindowFunctionType::CumeDist,
            "NTILE" => {
                let buckets = self.extract_ntile_buckets(func)?;
                WindowFunctionType::Ntile { buckets }
            }
            "LAG" => {
                let (expr, offset, default) = self.extract_offset_function_args(func, 1)?;
                WindowFunctionType::Lag {
                    expr,
                    offset,
                    default,
                }
            }
            "LEAD" => {
                let (expr, offset, default) = self.extract_offset_function_args(func, 1)?;
                WindowFunctionType::Lead {
                    expr,
                    offset,
                    default,
                }
            }
            "FIRST_VALUE" => {
                let expr = self.extract_single_arg(func)?;
                WindowFunctionType::FirstValue { expr }
            }
            "LAST_VALUE" => {
                let expr = self.extract_single_arg(func)?;
                WindowFunctionType::LastValue { expr }
            }
            "NTH_VALUE" => {
                let (expr, n) = self.extract_nth_value_args(func)?;
                WindowFunctionType::NthValue { expr, n }
            }

            "SUM" | "AVG" | "MIN" | "MAX" | "COUNT" | "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP"
            | "VARIANCE" | "VAR_POP" | "VAR_SAMP" => {
                use super::super::window_functions::AggregateType;
                let agg_type = match func_name.as_str() {
                    "SUM" => AggregateType::Sum,
                    "AVG" => AggregateType::Avg,
                    "MIN" => AggregateType::Min,
                    "MAX" => AggregateType::Max,
                    "COUNT" => AggregateType::Count,
                    "STDDEV" => AggregateType::Stddev,
                    "STDDEV_POP" => AggregateType::StddevPop,
                    "STDDEV_SAMP" => AggregateType::StddevSamp,
                    "VARIANCE" => AggregateType::Variance,
                    "VAR_POP" => AggregateType::VarPop,
                    "VAR_SAMP" => AggregateType::VarSamp,
                    _ => unreachable!(),
                };
                let expr = if func_name == "COUNT" {
                    None
                } else {
                    Some(self.extract_single_arg(func)?)
                };
                WindowFunctionType::Aggregate { agg_type, expr }
            }

            "PERCENTILE_CONT" | "PERCENTILE_DISC" => {
                let (expr, percentile) = self.extract_percentile_args(func)?;
                if func_name == "PERCENTILE_CONT" {
                    WindowFunctionType::PercentileCont { expr, percentile }
                } else {
                    WindowFunctionType::PercentileDisc { expr, percentile }
                }
            }
            _ => {
                return Err(Error::UnsupportedFeature(format!(
                    "Window function {} not yet supported",
                    func_name
                )));
            }
        };

        let window_spec = self.parse_window_spec_with_named(over, schema, named_windows)?;

        let col_name = format!("{}()", func_name);

        Ok((
            col_name,
            WindowFunction {
                function_type,
                spec: window_spec,
            },
        ))
    }

    fn extract_ntile_buckets(&self, func: &sqlparser::ast::Function) -> Result<i64> {
        use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};

        let FunctionArguments::List(ref arg_list) = func.args else {
            return Err(Error::InvalidQuery("NTILE requires arguments".to_string()));
        };

        if arg_list.args.len() != 1 {
            return Err(Error::InvalidQuery(
                "NTILE requires exactly one argument".to_string(),
            ));
        }

        let arg = &arg_list.args[0];
        let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg else {
            return Err(Error::InvalidQuery("Invalid NTILE argument".to_string()));
        };

        match expr {
            Expr::Value(value_with_span) => match &value_with_span.value {
                sqlparser::ast::Value::Number(n, _) => n
                    .parse::<i64>()
                    .map_err(|_| Error::InvalidQuery(format!("Invalid NTILE bucket count: {}", n))),
                _ => Err(Error::InvalidQuery(
                    "NTILE argument must be a literal integer".to_string(),
                )),
            },
            _ => Err(Error::InvalidQuery(
                "NTILE argument must be a literal integer".to_string(),
            )),
        }
    }

    fn extract_offset_function_args(
        &self,
        func: &sqlparser::ast::Function,
        default_offset: i64,
    ) -> Result<(sqlparser::ast::Expr, i64, Option<Value>)> {
        use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};

        let FunctionArguments::List(ref arg_list) = func.args else {
            return Err(Error::InvalidQuery(
                "LAG/LEAD requires arguments".to_string(),
            ));
        };

        if arg_list.args.is_empty() || arg_list.args.len() > 3 {
            return Err(Error::InvalidQuery(
                "LAG/LEAD requires 1-3 arguments".to_string(),
            ));
        }

        let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &arg_list.args[0] else {
            return Err(Error::InvalidQuery(
                "Invalid LAG/LEAD expression argument".to_string(),
            ));
        };

        let offset = if arg_list.args.len() >= 2 {
            let FunctionArg::Unnamed(FunctionArgExpr::Expr(offset_expr)) = &arg_list.args[1] else {
                return Err(Error::InvalidQuery(
                    "Invalid LAG/LEAD offset argument".to_string(),
                ));
            };
            self.extract_limit_offset_value(offset_expr)? as i64
        } else {
            default_offset
        };

        let default = if arg_list.args.len() >= 3 {
            let FunctionArg::Unnamed(FunctionArgExpr::Expr(default_expr)) = &arg_list.args[2]
            else {
                return Err(Error::InvalidQuery(
                    "Invalid LAG/LEAD default argument".to_string(),
                ));
            };
            Some(QueryExecutor::extract_literal_value(default_expr)?)
        } else {
            None
        };

        Ok((expr.clone(), offset, default))
    }

    fn extract_single_arg(&self, func: &sqlparser::ast::Function) -> Result<sqlparser::ast::Expr> {
        use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};

        let FunctionArguments::List(ref arg_list) = func.args else {
            return Err(Error::InvalidQuery(
                "Function requires arguments".to_string(),
            ));
        };

        if arg_list.args.len() != 1 {
            return Err(Error::InvalidQuery(
                "Function requires exactly one argument".to_string(),
            ));
        }

        let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &arg_list.args[0] else {
            return Err(Error::InvalidQuery("Invalid function argument".to_string()));
        };

        Ok(expr.clone())
    }

    fn extract_nth_value_args(
        &self,
        func: &sqlparser::ast::Function,
    ) -> Result<(sqlparser::ast::Expr, i64)> {
        use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};

        let FunctionArguments::List(ref arg_list) = func.args else {
            return Err(Error::InvalidQuery(
                "NTH_VALUE requires arguments".to_string(),
            ));
        };

        if arg_list.args.len() != 2 {
            return Err(Error::InvalidQuery(
                "NTH_VALUE requires exactly two arguments".to_string(),
            ));
        }

        let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &arg_list.args[0] else {
            return Err(Error::InvalidQuery(
                "Invalid NTH_VALUE expression argument".to_string(),
            ));
        };

        let FunctionArg::Unnamed(FunctionArgExpr::Expr(n_expr)) = &arg_list.args[1] else {
            return Err(Error::InvalidQuery(
                "Invalid NTH_VALUE N argument".to_string(),
            ));
        };

        let n = self.extract_limit_offset_value(n_expr)? as i64;

        Ok((expr.clone(), n))
    }

    fn extract_percentile_args(
        &self,
        func: &sqlparser::ast::Function,
    ) -> Result<(sqlparser::ast::Expr, f64)> {
        use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};

        let FunctionArguments::List(ref arg_list) = func.args else {
            return Err(Error::InvalidQuery(
                "PERCENTILE_CONT/PERCENTILE_DISC requires arguments".to_string(),
            ));
        };

        let extract_percentile = |expr: &sqlparser::ast::Expr| -> Result<f64> {
            match expr {
                sqlparser::ast::Expr::Value(value_with_span) => match &value_with_span.value {
                    sqlparser::ast::Value::Number(n, _) => n.parse::<f64>().map_err(|_| {
                        Error::InvalidQuery(format!("Invalid percentile value: {}", n))
                    }),
                    _ => Err(Error::InvalidQuery(
                        "Percentile value must be a number".to_string(),
                    )),
                },
                _ => Err(Error::InvalidQuery(
                    "Percentile value must be a literal number".to_string(),
                )),
            }
        };

        let (expr, percentile) = match arg_list.args.len() {
            1 => {
                let FunctionArg::Unnamed(FunctionArgExpr::Expr(percentile_expr)) =
                    &arg_list.args[0]
                else {
                    return Err(Error::InvalidQuery(
                        "Invalid PERCENTILE_CONT/PERCENTILE_DISC percentile argument".to_string(),
                    ));
                };

                let percentile = extract_percentile(percentile_expr)?;

                if func.within_group.is_empty() {
                    return Err(Error::InvalidQuery(
                        "PERCENTILE_CONT/PERCENTILE_DISC with one argument requires WITHIN GROUP (ORDER BY expr)".to_string(),
                    ));
                }

                let order_expr = &func.within_group[0].expr;
                (order_expr.clone(), percentile)
            }

            2 => {
                let FunctionArg::Unnamed(FunctionArgExpr::Expr(value_expr)) = &arg_list.args[0]
                else {
                    return Err(Error::InvalidQuery(
                        "Invalid PERCENTILE_CONT/PERCENTILE_DISC expression argument".to_string(),
                    ));
                };

                let FunctionArg::Unnamed(FunctionArgExpr::Expr(percentile_expr)) =
                    &arg_list.args[1]
                else {
                    return Err(Error::InvalidQuery(
                        "Invalid PERCENTILE_CONT/PERCENTILE_DISC percentile argument".to_string(),
                    ));
                };

                let percentile = extract_percentile(percentile_expr)?;
                (value_expr.clone(), percentile)
            }
            _ => {
                return Err(Error::InvalidQuery(
                    "PERCENTILE_CONT/PERCENTILE_DISC requires 1 or 2 arguments".to_string(),
                ));
            }
        };

        if !(0.0..=1.0).contains(&percentile) {
            return Err(Error::InvalidQuery(format!(
                "Percentile value must be between 0 and 1, got {}",
                percentile
            )));
        }

        Ok((expr, percentile))
    }

    fn extract_literal_value(expr: &sqlparser::ast::Expr) -> Result<Value> {
        use sqlparser::ast::Value as SqlValue;

        match expr {
            sqlparser::ast::Expr::Value(value_with_span) => match &value_with_span.value {
                SqlValue::Number(n, _) => {
                    if let Ok(i) = n.parse::<i64>() {
                        Ok(Value::int64(i))
                    } else if let Ok(f) = n.parse::<f64>() {
                        Ok(Value::float64(f))
                    } else {
                        Err(Error::InvalidQuery(format!("Invalid number: {}", n)))
                    }
                }
                SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                    Ok(Value::string(s.clone()))
                }
                SqlValue::Null => Ok(Value::null()),
                _ => Err(Error::InvalidQuery(
                    "Unsupported literal value type".to_string(),
                )),
            },

            sqlparser::ast::Expr::UnaryOp { op, expr } => {
                if matches!(op, sqlparser::ast::UnaryOperator::Minus) {
                    let val = Self::extract_literal_value(expr)?;
                    if let Some(i) = val.as_i64() {
                        if i == i64::MIN {
                            return Err(Error::ArithmeticOverflow {
                                operation: "negation".to_string(),
                                left: i.to_string(),
                                right: "0".to_string(),
                            });
                        }
                        Ok(Value::int64(-i))
                    } else if let Some(f) = val.as_f64() {
                        if f == 9223372036854775808.0 {
                            Ok(Value::int64(i64::MIN))
                        } else {
                            Ok(Value::float64(-f))
                        }
                    } else {
                        Err(Error::InvalidQuery(
                            "Unary minus can only be applied to numbers".to_string(),
                        ))
                    }
                } else {
                    Err(Error::InvalidQuery(
                        "Only unary minus is supported in default values".to_string(),
                    ))
                }
            }
            _ => Err(Error::InvalidQuery(
                "Default value must be a literal".to_string(),
            )),
        }
    }

    fn parse_window_spec_with_named(
        &self,
        over: &sqlparser::ast::WindowType,
        schema: &Schema,
        named_windows: &[sqlparser::ast::NamedWindowDefinition],
    ) -> Result<super::super::window_functions::WindowSpecification> {
        use sqlparser::ast::{NamedWindowExpr, WindowType};

        let spec = match over {
            WindowType::WindowSpec(spec) => spec,
            WindowType::NamedWindow(name) => {
                let window_def = named_windows
                    .iter()
                    .find(|w| w.0.value == name.value)
                    .ok_or_else(|| {
                        Error::InvalidQuery(format!("Named window '{}' not found", name.value))
                    })?;

                match &window_def.1 {
                    NamedWindowExpr::WindowSpec(spec) => spec,
                    NamedWindowExpr::NamedWindow(ref_name) => {
                        return Err(Error::UnsupportedFeature(format!(
                            "Named window references (WINDOW {} AS {}) not yet supported",
                            name.value, ref_name.value
                        )));
                    }
                }
            }
        };

        let mut partition_by = Vec::new();
        for part_expr in &spec.partition_by {
            let col_name = self.extract_column_name(part_expr)?;
            let col_idx = schema
                .fields()
                .iter()
                .position(|f| f.name == col_name)
                .ok_or_else(|| Error::ColumnNotFound(col_name))?;
            partition_by.push(col_idx);
        }

        let mut order_by = Vec::new();
        for order_expr in &spec.order_by {
            let col_name = self.extract_column_name(&order_expr.expr)?;
            let col_idx = schema
                .fields()
                .iter()
                .position(|f| f.name == col_name)
                .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;
            let is_asc = order_expr.options.asc.unwrap_or(true);
            let nulls_first = order_expr.options.nulls_first;
            order_by.push((col_idx, is_asc, nulls_first));
        }

        let frame = spec
            .window_frame
            .as_ref()
            .map(|wf| self.parse_window_frame(wf))
            .transpose()?;

        Ok(super::super::window_functions::WindowSpecification::new(
            partition_by,
            order_by,
            frame,
        ))
    }

    fn parse_window_frame(
        &self,
        window_frame: &sqlparser::ast::WindowFrame,
    ) -> Result<super::super::window_functions::FrameSpecification> {
        use sqlparser::ast::WindowFrameUnits;

        use super::super::window_functions::{FrameBound, FrameSpecification, FrameType};

        let frame_type = match window_frame.units {
            WindowFrameUnits::Rows => FrameType::Rows,
            WindowFrameUnits::Range => FrameType::Range,
            WindowFrameUnits::Groups => {
                return Err(Error::UnsupportedFeature(
                    "GROUPS window frame type not yet supported".to_string(),
                ));
            }
        };

        let start_bound = self.parse_frame_bound(&window_frame.start_bound)?;

        let end_bound = if let Some(ref end) = window_frame.end_bound {
            self.parse_frame_bound(end)?
        } else {
            FrameBound::CurrentRow
        };

        Ok(FrameSpecification {
            frame_type,
            start_bound,
            end_bound,
        })
    }

    fn parse_frame_bound(
        &self,
        bound: &sqlparser::ast::WindowFrameBound,
    ) -> Result<super::super::window_functions::FrameBound> {
        use sqlparser::ast::WindowFrameBound as SqlFrameBound;

        use super::super::window_functions::FrameBound;

        match bound {
            SqlFrameBound::CurrentRow => Ok(FrameBound::CurrentRow),
            SqlFrameBound::Preceding(Some(expr)) => {
                let offset = self.extract_limit_offset_value(expr)?;
                Ok(FrameBound::Preceding(offset))
            }
            SqlFrameBound::Preceding(None) => Ok(FrameBound::UnboundedPreceding),
            SqlFrameBound::Following(Some(expr)) => {
                let offset = self.extract_limit_offset_value(expr)?;
                Ok(FrameBound::Following(offset))
            }
            SqlFrameBound::Following(None) => Ok(FrameBound::UnboundedFollowing),
        }
    }

    fn build_window_result_schema(
        &self,
        base_schema: &Schema,
        window_col_info: &[(String, WindowFunction)],
    ) -> Result<Schema> {
        use sqlparser::ast::Expr as SqlExpr;

        use super::super::window_functions::AggregateType;

        let mut fields = base_schema.fields().to_vec();

        let get_expr_type = |expr: &SqlExpr| -> Option<DataType> {
            if let SqlExpr::Identifier(ident) = expr {
                for field in base_schema.fields() {
                    if field.name.eq_ignore_ascii_case(&ident.value) {
                        return Some(field.data_type.clone());
                    }
                }
            }
            None
        };

        for (col_name, window_fn) in window_col_info {
            let data_type = match &window_fn.function_type {
                WindowFunctionType::RowNumber
                | WindowFunctionType::Rank
                | WindowFunctionType::DenseRank
                | WindowFunctionType::Ntile { .. } => DataType::Int64,

                WindowFunctionType::PercentRank | WindowFunctionType::CumeDist => DataType::Float64,

                WindowFunctionType::PercentileCont { .. }
                | WindowFunctionType::PercentileDisc { .. } => DataType::Float64,

                WindowFunctionType::Lag { expr, .. }
                | WindowFunctionType::Lead { expr, .. }
                | WindowFunctionType::FirstValue { expr }
                | WindowFunctionType::LastValue { expr }
                | WindowFunctionType::NthValue { expr, .. } => {
                    get_expr_type(expr).unwrap_or(DataType::String)
                }

                WindowFunctionType::Aggregate { agg_type, expr } => {
                    let input_type = expr.as_ref().and_then(|e| get_expr_type(e));

                    match agg_type {
                        AggregateType::Count => DataType::Int64,
                        AggregateType::Avg | AggregateType::Sum => match input_type {
                            Some(DataType::Numeric(prec_scale)) => DataType::Numeric(prec_scale),
                            _ => DataType::Float64,
                        },
                        AggregateType::Min | AggregateType::Max => {
                            input_type.unwrap_or(DataType::Float64)
                        }
                        AggregateType::Stddev
                        | AggregateType::StddevPop
                        | AggregateType::StddevSamp
                        | AggregateType::Variance
                        | AggregateType::VarPop
                        | AggregateType::VarSamp => DataType::Float64,
                    }
                }
            };

            fields.push(Field::nullable(col_name.clone(), data_type));
        }

        Ok(Schema::from_fields(fields))
    }

    fn combine_base_and_window_results(
        &self,
        base_rows: &[Row],
        window_results: &[Vec<Value>],
        _base_schema: &Schema,
        _window_col_names: &[String],
    ) -> Result<Vec<Row>> {
        let mut combined_rows = Vec::new();

        for (row_idx, base_row) in base_rows.iter().enumerate() {
            let mut combined_values = base_row.values().to_vec();

            for window_col_results in window_results {
                if row_idx < window_col_results.len() {
                    combined_values.push(window_col_results[row_idx].clone());
                } else {
                    combined_values.push(Value::null());
                }
            }

            combined_rows.push(Row::from_values(combined_values));
        }

        Ok(combined_rows)
    }

    fn execute_select_without_from(&mut self, select: &Select) -> Result<Table> {
        use crate::query_executor::expression_evaluator::ExpressionEvaluator;

        for item in &select.projection {
            if matches!(item, SelectItem::Wildcard(_)) {
                return Err(Error::InvalidQuery(
                    "SELECT * requires a FROM clause".to_string(),
                ));
            }
        }

        if select.selection.is_some() {
            return Err(Error::InvalidOperation(
                "WHERE clause requires a FROM clause".to_string(),
            ));
        }

        if select.having.is_some() {
            return Err(Error::InvalidQuery(
                "HAVING clause requires GROUP BY".to_string(),
            ));
        }

        let empty_schema = Schema::from_fields(vec![]);
        let dummy_row = Row::from_values(vec![]);

        let mut projected_fields: Vec<Field> = Vec::new();
        let mut result_values: Vec<Value> = Vec::new();

        let type_registry = {
            let storage = self.storage.borrow();
            storage.get_dataset("default").map(|d| d.types().clone())
        };

        let evaluator = if let Some(registry) = type_registry {
            ExpressionEvaluator::new(&empty_schema).with_owned_type_registry(registry)
        } else {
            ExpressionEvaluator::new(&empty_schema)
        };

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, alias: _ } => {
                    let col_name = if let SelectItem::ExprWithAlias { alias, .. } = item {
                        alias.value.clone()
                    } else {
                        self.infer_column_name(expr)
                    };

                    let (value, is_srf) = if let Expr::Function(func) = expr {
                        let func_name = func.name.to_string().to_uppercase();
                        if matches!(
                            func_name.as_str(),
                            "NEXTVAL" | "CURRVAL" | "SETVAL" | "LASTVAL"
                        ) {
                            (self.evaluate_sequence_function(&func_name, func)?, false)
                        } else if matches!(
                            func_name.as_str(),
                            "SKEYS" | "SVALS" | "JSON_OBJECT_KEYS" | "JSONB_OBJECT_KEYS"
                        ) {
                            (evaluator.evaluate_expr(expr, &dummy_row)?, true)
                        } else {
                            (evaluator.evaluate_expr(expr, &dummy_row)?, false)
                        }
                    } else {
                        (evaluator.evaluate_expr(expr, &dummy_row)?, false)
                    };

                    if is_srf {
                        let (element_type, elements) = if let Some(arr) = value.as_array() {
                            let elem_type = if let DataType::Array(inner) = value.data_type() {
                                (*inner).clone()
                            } else {
                                DataType::String
                            };
                            (elem_type, arr.clone())
                        } else {
                            let data_type = Self::infer_data_type_from_value(&value);
                            projected_fields.push(Field::nullable(col_name, data_type));
                            result_values.push(value);
                            continue;
                        };

                        let srf_schema =
                            Schema::from_fields(vec![Field::nullable(&col_name, element_type)]);

                        let rows: Vec<Vec<Value>> = elements.into_iter().map(|v| vec![v]).collect();

                        return Table::from_values(srf_schema, rows);
                    }

                    let data_type = Self::infer_data_type_from_value(&value);

                    projected_fields.push(Field::nullable(col_name, data_type));
                    result_values.push(value);
                }
                SelectItem::Wildcard(_) => {
                    return Err(Error::InvalidOperation(
                        "SELECT * requires a FROM clause".to_string(),
                    ));
                }
                SelectItem::QualifiedWildcard(_, _) => {
                    return Err(Error::InvalidOperation(
                        "SELECT table.* requires a FROM clause".to_string(),
                    ));
                }
            }
        }

        let result_schema = Schema::from_fields(projected_fields);

        Table::from_values(result_schema, vec![result_values])
    }

    fn infer_data_type_from_value(value: &Value) -> DataType {
        if value.is_null() {
            return DataType::Unknown;
        }

        if value.as_bool().is_some() {
            return DataType::Bool;
        }
        if value.as_i64().is_some() {
            return DataType::Int64;
        }
        if value.as_f64().is_some() {
            return DataType::Float64;
        }
        if value.as_str().is_some() {
            return DataType::String;
        }
        if value.as_date().is_some() {
            return DataType::Date;
        }
        if value.as_datetime().is_some() {
            return DataType::DateTime;
        }
        if value.as_timestamp().is_some() {
            return DataType::Timestamp;
        }
        if value.as_time().is_some() {
            return DataType::Time;
        }

        if value.as_bytes().is_some() {
            return DataType::Bytes;
        }
        if value.is_json() {
            return DataType::Json;
        }
        if value.as_uuid().is_some() {
            return DataType::String;
        }
        if let Some(arr) = value.as_array() {
            if let Some(first) = arr.iter().next() {
                return DataType::Array(Box::new(Self::infer_data_type_from_value(first)));
            } else {
                return DataType::Array(Box::new(DataType::Unknown));
            }
        }
        if let Some(fields) = value.as_struct() {
            use yachtsql_core::types::StructField;
            let struct_fields: Vec<StructField> = fields
                .iter()
                .map(|(name, val)| StructField {
                    name: name.clone(),
                    data_type: Self::infer_data_type_from_value(val),
                })
                .collect();
            return DataType::Struct(struct_fields);
        }
        if value.as_numeric().is_some() {
            return DataType::Numeric(None);
        }
        if value.as_geography().is_some() {
            return DataType::Geography;
        }
        if value.as_interval().is_some() {
            return DataType::Interval;
        }
        if value.as_hstore().is_some() {
            return DataType::Hstore;
        }
        if value.as_macaddr().is_some() {
            return DataType::MacAddr;
        }
        if value.as_macaddr8().is_some() {
            return DataType::MacAddr8;
        }
        if value.as_point().is_some() {
            return DataType::Point;
        }
        if value.as_pgbox().is_some() {
            return DataType::PgBox;
        }
        if value.as_circle().is_some() {
            return DataType::Circle;
        }

        DataType::Unknown
    }

    fn execute_set_operation(
        &mut self,
        op: &SetOperator,
        set_quantifier: &SetQuantifier,
        left: &SetExpr,
        right: &SetExpr,
    ) -> Result<Table> {
        let left_result = self.execute_set_expr(left)?;
        let right_result = self.execute_set_expr(right)?;

        self.validate_set_operation_schemas(left_result.schema(), right_result.schema())?;

        let result = match op {
            SetOperator::Union => self.union_results(left_result, right_result, set_quantifier)?,
            SetOperator::Intersect => {
                self.intersect_results(left_result, right_result, set_quantifier)?
            }
            SetOperator::Except => {
                self.except_results(left_result, right_result, set_quantifier)?
            }
            SetOperator::Minus => self.except_results(left_result, right_result, set_quantifier)?,
        };

        Ok(result)
    }

    fn execute_set_expr(&mut self, set_expr: &SetExpr) -> Result<Table> {
        match set_expr {
            SetExpr::Select(select) => self.execute_simple_select(select.as_ref(), None),
            SetExpr::Query(query) => self.execute_select(&Statement::Query(query.clone()), ""),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => self.execute_set_operation(op, set_quantifier, left, right),
            _ => Err(Error::UnsupportedFeature(
                "Unsupported SetExpr variant".to_string(),
            )),
        }
    }

    fn validate_set_operation_schemas(&self, left: &Schema, right: &Schema) -> Result<()> {
        if left.fields().len() != right.fields().len() {
            return Err(Error::InvalidQuery(format!(
                "Set operation requires same number of columns: left has {}, right has {}",
                left.fields().len(),
                right.fields().len()
            )));
        }

        for (i, (left_field, right_field)) in
            left.fields().iter().zip(right.fields().iter()).enumerate()
        {
            if !self.types_compatible(&left_field.data_type, &right_field.data_type) {
                return Err(Error::InvalidQuery(format!(
                    "Set operation column {} type mismatch: left is {:?}, right is {:?}",
                    i + 1,
                    left_field.data_type,
                    right_field.data_type
                )));
            }
        }

        Ok(())
    }

    fn types_compatible(&self, left: &DataType, right: &DataType) -> bool {
        if left == right {
            return true;
        }

        matches!(
            (left, right),
            (DataType::Int64, DataType::Float64)
                | (DataType::Float64, DataType::Int64)
                | (DataType::Int64, DataType::Numeric(_))
                | (DataType::Numeric(_), DataType::Int64)
                | (DataType::Float64, DataType::Numeric(_))
                | (DataType::Numeric(_), DataType::Float64)
        )
    }

    fn validate_join_condition_types(
        &self,
        join_condition: &Expr,
        combined_schema: &Schema,
    ) -> Result<()> {
        self.validate_join_condition_types_recursive(join_condition, combined_schema)
    }

    fn validate_join_condition_types_recursive(&self, expr: &Expr, schema: &Schema) -> Result<()> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    sqlparser::ast::BinaryOperator::Eq
                    | sqlparser::ast::BinaryOperator::NotEq
                    | sqlparser::ast::BinaryOperator::Lt
                    | sqlparser::ast::BinaryOperator::LtEq
                    | sqlparser::ast::BinaryOperator::Gt
                    | sqlparser::ast::BinaryOperator::GtEq => {
                        let left_type = Self::infer_expression_type(left, schema)?;
                        let right_type = Self::infer_expression_type(right, schema)?;
                        if !self.types_compatible(&left_type, &right_type) {
                            return Err(Error::InvalidQuery(format!(
                                "JOIN condition type mismatch: cannot compare {:?} with {:?}",
                                left_type, right_type
                            )));
                        }
                    }
                    sqlparser::ast::BinaryOperator::And | sqlparser::ast::BinaryOperator::Or => {
                        self.validate_join_condition_types_recursive(left, schema)?;
                        self.validate_join_condition_types_recursive(right, schema)?;
                    }
                    _ => {}
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn union_results(
        &self,
        left: Table,
        right: Table,
        set_quantifier: &SetQuantifier,
    ) -> Result<Table> {
        let schema = left.schema().clone();
        let mut rows = Vec::new();

        rows.extend(left.rows()?);

        rows.extend(right.rows()?);

        if !matches!(
            set_quantifier,
            SetQuantifier::All | SetQuantifier::AllByName
        ) {
            rows = self.deduplicate_rows(rows);
        }

        self.rows_to_record_batch(schema, rows)
    }

    fn intersect_results(
        &self,
        left: Table,
        right: Table,
        set_quantifier: &SetQuantifier,
    ) -> Result<Table> {
        let schema = left.schema().clone();
        let left_rows = left.rows()?;
        let right_rows = right.rows()?;

        let use_bag_semantics = matches!(
            set_quantifier,
            SetQuantifier::All | SetQuantifier::AllByName
        );

        if use_bag_semantics {
            let mut left_counts: std::collections::HashMap<RowKey, (usize, Row)> =
                std::collections::HashMap::new();
            for row in left_rows {
                let row_key = self.row_to_key(&row);
                left_counts.entry(row_key).or_insert((0, row.clone())).0 += 1;
            }

            let mut right_counts: std::collections::HashMap<RowKey, usize> =
                std::collections::HashMap::new();
            for row in right_rows {
                let row_key = self.row_to_key(&row);
                *right_counts.entry(row_key).or_insert(0) += 1;
            }

            let mut result_rows = Vec::new();
            for (row_key, (left_count, row)) in left_counts {
                if let Some(&right_count) = right_counts.get(&row_key) {
                    let min_count = left_count.min(right_count);
                    for _ in 0..min_count {
                        result_rows.push(row.clone());
                    }
                }
            }

            self.rows_to_record_batch(schema, result_rows)
        } else {
            let right_set: std::collections::HashSet<RowKey> =
                right_rows.iter().map(|row| self.row_to_key(row)).collect();

            let mut result_rows = Vec::new();
            for row in left_rows {
                let row_key = self.row_to_key(&row);
                if right_set.contains(&row_key) {
                    result_rows.push(row);
                }
            }

            result_rows = self.deduplicate_rows(result_rows);

            self.rows_to_record_batch(schema, result_rows)
        }
    }

    fn except_results(
        &self,
        left: Table,
        right: Table,
        set_quantifier: &SetQuantifier,
    ) -> Result<Table> {
        let schema = left.schema().clone();
        let left_rows = left.rows()?;
        let right_rows = right.rows()?;

        let use_bag_semantics = matches!(
            set_quantifier,
            SetQuantifier::All | SetQuantifier::AllByName
        );

        if use_bag_semantics {
            let mut left_counts: std::collections::HashMap<RowKey, (usize, Row)> =
                std::collections::HashMap::new();
            for row in left_rows {
                let row_key = self.row_to_key(&row);
                left_counts.entry(row_key).or_insert((0, row.clone())).0 += 1;
            }

            let mut right_counts: std::collections::HashMap<RowKey, usize> =
                std::collections::HashMap::new();
            for row in right_rows {
                let row_key = self.row_to_key(&row);
                *right_counts.entry(row_key).or_insert(0) += 1;
            }

            let mut result_rows = Vec::new();
            for (row_key, (left_count, row)) in left_counts {
                let right_count = right_counts.get(&row_key).copied().unwrap_or(0);
                let diff_count = left_count.saturating_sub(right_count);
                for _ in 0..diff_count {
                    result_rows.push(row.clone());
                }
            }

            self.rows_to_record_batch(schema, result_rows)
        } else {
            let right_set: std::collections::HashSet<RowKey> =
                right_rows.iter().map(|row| self.row_to_key(row)).collect();

            let mut result_rows = Vec::new();
            for row in left_rows {
                let row_key = self.row_to_key(&row);
                if !right_set.contains(&row_key) {
                    result_rows.push(row);
                }
            }

            result_rows = self.deduplicate_rows(result_rows);

            self.rows_to_record_batch(schema, result_rows)
        }
    }

    fn row_to_key(&self, row: &Row) -> RowKey {
        RowKey(row.values().to_vec())
    }

    fn process_ctes(&mut self, with_clause: &sqlparser::ast::With) -> Result<Vec<String>> {
        let mut cte_names = Vec::new();

        for cte in &with_clause.cte_tables {
            let cte_name = cte.alias.name.value.clone();
            cte_names.push(cte_name.clone());

            let temp_table_name = format!("__cte_{}", cte_name);

            let is_recursive =
                with_clause.recursive && Self::is_cte_recursive(&cte.query, &cte_name);

            let (schema, all_rows) = if is_recursive {
                self.execute_recursive_cte(&cte_name, &cte.query)?
            } else {
                let mut modified_cte_query = (*cte.query).clone();
                for prev_cte_name in &cte_names[..cte_names.len() - 1] {
                    let cte_table = format!("__cte_{}", prev_cte_name);
                    Self::rename_table_references(
                        &mut modified_cte_query.body,
                        prev_cte_name,
                        &cte_table,
                    );
                }
                let cte_query = Statement::Query(Box::new(modified_cte_query.clone()));
                let cte_query_sql = cte_query.to_string();
                let cte_result = self.execute_select(&cte_query, &cte_query_sql)?;
                let schema = cte_result.schema().clone();
                let rows = cte_result.rows().unwrap_or_default();
                (schema, rows)
            };

            let mut storage = self.storage.borrow_mut();

            if storage.get_dataset("default").is_none() {
                storage.create_dataset("default".to_string())?;
            }

            let dataset = storage
                .get_dataset_mut("default")
                .ok_or_else(|| Error::DatasetNotFound("Dataset 'default' not found".to_string()))?;

            dataset.create_table(temp_table_name.clone(), schema)?;

            let table = dataset
                .get_table_mut(&temp_table_name)
                .ok_or_else(|| Error::table_not_found(temp_table_name.clone()))?;

            for row in all_rows {
                table.insert_row(row)?;
            }
        }

        Ok(cte_names)
    }

    fn cleanup_ctes(&mut self, cte_names: &[String]) -> Result<()> {
        let mut storage = self.storage.borrow_mut();

        if let Some(dataset) = storage.get_dataset_mut("default") {
            for cte_name in cte_names {
                let temp_table_name = format!("__cte_{}", cte_name);
                let _ = dataset.delete_table(&temp_table_name);
            }
        }

        Ok(())
    }

    fn is_cte_recursive(query: &sqlparser::ast::Query, cte_name: &str) -> bool {
        use sqlparser::ast::{SetExpr, TableFactor};

        fn check_set_expr(expr: &SetExpr, cte_name: &str) -> bool {
            match expr {
                SetExpr::Select(select) => {
                    for table_with_joins in &select.from {
                        if check_table_factor(&table_with_joins.relation, cte_name) {
                            return true;
                        }

                        for join in &table_with_joins.joins {
                            if check_table_factor(&join.relation, cte_name) {
                                return true;
                            }
                        }
                    }
                    false
                }
                SetExpr::SetOperation { left, right, .. } => {
                    check_set_expr(left, cte_name) || check_set_expr(right, cte_name)
                }
                SetExpr::Query(q) => check_set_expr(&q.body, cte_name),
                _ => false,
            }
        }

        fn check_table_factor(table: &TableFactor, cte_name: &str) -> bool {
            match table {
                TableFactor::Table { name, .. } => name.to_string() == cte_name,
                TableFactor::Derived { subquery, .. } => check_set_expr(&subquery.body, cte_name),
                TableFactor::NestedJoin {
                    table_with_joins, ..
                } => check_table_factor(&table_with_joins.relation, cte_name),
                _ => false,
            }
        }

        check_set_expr(&query.body, cte_name)
    }

    fn execute_recursive_cte(
        &mut self,
        cte_name: &str,
        query: &sqlparser::ast::Query,
    ) -> Result<(yachtsql_storage::Schema, Vec<yachtsql_storage::Row>)> {
        use sqlparser::ast::{SetExpr, SetOperator, SetQuantifier};

        const MAX_RECURSION_DEPTH: usize = 1000;

        let SetExpr::SetOperation {
            left,
            right,
            op,
            set_quantifier,
        } = &*query.body
        else {
            return Err(Error::InvalidQuery(
                "Recursive CTE must be a UNION operation".to_string(),
            ));
        };

        if !matches!(op, SetOperator::Union) {
            return Err(Error::InvalidQuery(
                "Recursive CTE must use UNION or UNION ALL".to_string(),
            ));
        }

        let use_distinct = matches!(
            set_quantifier,
            SetQuantifier::Distinct | SetQuantifier::None
        );

        let base_query = Statement::Query(Box::new(sqlparser::ast::Query {
            with: None,
            body: left.clone(),
            order_by: None,
            limit_clause: None,
            fetch: None,
            locks: Vec::new(),
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: Vec::new(),
        }));

        let base_result = self.execute_select(&base_query, "")?;
        let schema = base_result.schema().clone();
        let mut all_rows = base_result.rows().unwrap_or_default();

        let mut seen_rows = if use_distinct {
            let mut set = std::collections::HashSet::new();
            for row in &all_rows {
                set.insert(RowKey(row.values().to_vec()));
            }
            set
        } else {
            std::collections::HashSet::new()
        };

        let mut working_rows = all_rows.clone();
        let mut iteration = 0;

        while !working_rows.is_empty() && iteration < MAX_RECURSION_DEPTH {
            iteration += 1;

            let working_table_name = format!("__cte_{}_working", cte_name);

            {
                let mut storage = self.storage.borrow_mut();

                if storage.get_dataset("default").is_none() {
                    storage.create_dataset("default".to_string())?;
                }

                let dataset = storage.get_dataset_mut("default").ok_or_else(|| {
                    Error::DatasetNotFound("Dataset 'default' not found".to_string())
                })?;

                let _ = dataset.delete_table(&working_table_name);

                dataset.create_table(working_table_name.clone(), schema.clone())?;

                let table = dataset
                    .get_table_mut(&working_table_name)
                    .ok_or_else(|| Error::table_not_found(working_table_name.clone()))?;

                for row in &working_rows {
                    table.insert_row(row.clone())?;
                }
            }

            let temp_cte_name = format!("__cte_{}", cte_name);

            let mut recursive_part = right.clone();
            Self::rename_table_references(&mut recursive_part, cte_name, &temp_cte_name);

            {
                let mut storage = self.storage.borrow_mut();

                if storage.get_dataset("default").is_none() {
                    storage.create_dataset("default".to_string())?;
                }

                let dataset = storage.get_dataset_mut("default").ok_or_else(|| {
                    Error::DatasetNotFound("Dataset 'default' not found".to_string())
                })?;

                let _ = dataset.delete_table(&temp_cte_name);

                dataset.create_table(temp_cte_name.clone(), schema.clone())?;

                let table = dataset
                    .get_table_mut(&temp_cte_name)
                    .ok_or_else(|| Error::table_not_found(temp_cte_name.clone()))?;

                debug_eprintln!(
                    "[executor::query] Created CTE table '{}' with {} fields: {:?}",
                    temp_cte_name,
                    schema.fields().len(),
                    schema.fields().iter().map(|f| &f.name).collect::<Vec<_>>()
                );

                for (idx, row) in working_rows.iter().enumerate() {
                    debug_eprintln!(
                        "[executor::query] Inserting row {} into '{}': {:?}",
                        idx,
                        temp_cte_name,
                        row.values()
                    );
                    table.insert_row(row.clone())?;
                }
            }

            let recursive_query = Statement::Query(Box::new(sqlparser::ast::Query {
                with: None,
                body: recursive_part.clone(),
                order_by: None,
                limit_clause: None,
                fetch: None,
                locks: Vec::new(),
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: Vec::new(),
            }));

            let recursive_query_sql = format!("{}_{}", recursive_query, iteration);
            debug_eprintln!(
                "[executor::query] Executing recursive query: {}",
                recursive_query_sql
            );

            let recursive_result = self.execute_select(&recursive_query, &recursive_query_sql)?;
            let new_rows = recursive_result.rows().unwrap_or_default();

            debug_eprintln!(
                "[executor::query] Recursive part returned {} rows (working had {} rows)",
                new_rows.len(),
                working_rows.len()
            );
            for (idx, row) in new_rows.iter().enumerate() {
                debug_eprintln!(
                    "[executor::query] Recursive result row {}: {:?}",
                    idx,
                    row.values()
                );
            }

            {
                let mut storage = self.storage.borrow_mut();

                if storage.get_dataset("default").is_none() {
                    storage.create_dataset("default".to_string())?;
                }

                let dataset = storage.get_dataset_mut("default").ok_or_else(|| {
                    Error::DatasetNotFound("Dataset 'default' not found".to_string())
                })?;
                let _ = dataset.delete_table(&working_table_name);
                let _ = dataset.delete_table(&temp_cte_name);
            }

            working_rows = if use_distinct {
                new_rows
                    .into_iter()
                    .filter(|row| seen_rows.insert(RowKey(row.values().to_vec())))
                    .collect()
            } else {
                new_rows
            };

            all_rows.extend(working_rows.clone());

            debug_eprintln!(
                "[executor::query] Recursive CTE '{}' iteration {}: {} new rows, {} total rows",
                cte_name,
                iteration,
                working_rows.len(),
                all_rows.len()
            );
        }

        if iteration >= MAX_RECURSION_DEPTH {
            debug_eprintln!(
                "[executor::query] WARNING: Recursive CTE '{}' reached maximum recursion depth of {}",
                cte_name,
                MAX_RECURSION_DEPTH
            );
        }

        Ok((schema, all_rows))
    }

    fn rename_table_references(expr: &mut Box<SetExpr>, old_name: &str, new_name: &str) {
        use sqlparser::ast::{SetExpr, TableFactor};

        fn rename_in_set_expr(expr: &mut SetExpr, old_name: &str, new_name: &str) {
            match expr {
                SetExpr::Select(select) => {
                    for table_with_joins in &mut select.from {
                        rename_in_table_factor(&mut table_with_joins.relation, old_name, new_name);

                        for join in &mut table_with_joins.joins {
                            rename_in_table_factor(&mut join.relation, old_name, new_name);
                        }
                    }
                }
                SetExpr::SetOperation { left, right, .. } => {
                    rename_in_set_expr(left, old_name, new_name);
                    rename_in_set_expr(right, old_name, new_name);
                }
                SetExpr::Query(q) => {
                    rename_in_set_expr(&mut q.body, old_name, new_name);
                }
                _ => {}
            }
        }

        fn rename_in_table_factor(table: &mut TableFactor, old_name: &str, new_name: &str) {
            match table {
                TableFactor::Table { name, .. } => {
                    if name.to_string() == old_name {
                        *name = sqlparser::ast::ObjectName(vec![
                            sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(
                                new_name,
                            )),
                        ]);
                    }
                }
                TableFactor::Derived { subquery, .. } => {
                    rename_in_set_expr(&mut subquery.body, old_name, new_name);
                }
                TableFactor::NestedJoin {
                    table_with_joins, ..
                } => {
                    rename_in_table_factor(&mut table_with_joins.relation, old_name, new_name);
                }
                _ => {}
            }
        }

        rename_in_set_expr(expr, old_name, new_name);
    }

    pub(crate) fn resolve_table_name(&self, dataset_id: &str, table_id: &str) -> String {
        let cte_table_name = format!("__cte_{}", table_id);
        let storage = self.storage.borrow_mut();

        if let Some(dataset) = storage.get_dataset(dataset_id)
            && dataset.get_table(&cte_table_name).is_some()
        {
            return cte_table_name;
        }

        table_id.to_string()
    }
}

#[derive(Clone)]
struct RowKey(Vec<Value>);

impl std::hash::Hash for RowKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.len().hash(state);

        for value in &self.0 {
            value.data_type().hash(state);

            if value.is_null() {
                continue;
            }

            if let Some(b) = value.as_bool() {
                b.hash(state);
                continue;
            }

            if let Some(i) = value.as_i64() {
                i.hash(state);
                continue;
            }

            if let Some(f) = value.as_f64() {
                f.to_bits().hash(state);
                continue;
            }

            if let Some(s) = value.as_str() {
                s.hash(state);
                continue;
            }

            if let Some(d) = value.as_date() {
                Datelike::num_days_from_ce(&d).hash(state);
                continue;
            }

            if let Some(dt) = value.as_datetime() {
                dt.timestamp().hash(state);
                dt.timestamp_subsec_nanos().hash(state);
                continue;
            }

            if let Some(ts) = value.as_timestamp() {
                ts.timestamp().hash(state);
                ts.timestamp_subsec_nanos().hash(state);
                continue;
            }

            if let Some(t) = value.as_time() {
                Timelike::num_seconds_from_midnight(&t).hash(state);
                Timelike::nanosecond(&t).hash(state);
                continue;
            }

            if let Some(n) = value.as_numeric() {
                n.to_string().hash(state);
                continue;
            }
            if let Some(b) = value.as_bytes() {
                b.hash(state);
                continue;
            }
            if let Some(arr) = value.as_array() {
                arr.len().hash(state);
                for item in arr.iter() {
                    RowKey(vec![item.clone()]).hash(state);
                }
                continue;
            }
            if let Some(fields) = value.as_struct() {
                fields.len().hash(state);
                for (k, v) in fields.iter() {
                    k.hash(state);
                    RowKey(vec![v.clone()]).hash(state);
                }
                continue;
            }
            if let Some(wkt) = value.as_geography() {
                wkt.hash(state);
                continue;
            }
            if let Some(j) = value.as_json() {
                j.to_string().hash(state);
                continue;
            }
            if let Some(u) = value.as_uuid() {
                u.hash(state);
                continue;
            }
            if let Some(s) = value.as_str() {
                s.hash(state);
                continue;
            }
        }
    }
}

impl PartialEq for RowKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for RowKey {}

fn compare_values_for_sort(a: &Value, b: &Value) -> Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Greater,
        (false, true) => return Ordering::Less,
        (false, false) => {}
    }

    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return x.cmp(&y);
    }

    if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
        return x.partial_cmp(&y).unwrap_or(Ordering::Equal);
    }

    if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
        return x.cmp(y);
    }

    if let (Some(x), Some(y)) = (a.as_bool(), b.as_bool()) {
        return x.cmp(&y);
    }

    if let (Some(x), Some(y)) = (a.as_i64(), b.as_f64()) {
        return (x as f64).partial_cmp(&y).unwrap_or(Ordering::Equal);
    }

    if let (Some(x), Some(y)) = (a.as_f64(), b.as_i64()) {
        return x.partial_cmp(&(y as f64)).unwrap_or(Ordering::Equal);
    }

    if let (Some(x), Some(y)) = (a.as_bytes(), b.as_bytes()) {
        return x.cmp(y);
    }

    Ordering::Equal
}

fn compare_values_with_nulls_for_sort(
    a: &Value,
    b: &Value,
    is_asc: bool,
    nulls_first: Option<bool>,
) -> Ordering {
    let nulls_first = nulls_first.unwrap_or(!is_asc);

    match (a.is_null(), b.is_null()) {
        (true, true) => return Ordering::Equal,
        (true, false) => {
            return if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        (false, true) => {
            return if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        (false, false) => {
            let cmp = compare_values_for_sort_non_null(a, b);
            return if is_asc { cmp } else { cmp.reverse() };
        }
    }
}

fn compare_enum_values_with_nulls_for_sort(
    a: &Value,
    b: &Value,
    labels: &[String],
    is_asc: bool,
    nulls_first: Option<bool>,
) -> Ordering {
    let nulls_first = nulls_first.unwrap_or(!is_asc);

    match (a.is_null(), b.is_null()) {
        (true, true) => return Ordering::Equal,
        (true, false) => {
            return if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        (false, true) => {
            return if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        (false, false) => {
            let pos_a = a.as_str().and_then(|s| labels.iter().position(|l| l == s));
            let pos_b = b.as_str().and_then(|s| labels.iter().position(|l| l == s));

            let cmp = match (pos_a, pos_b) {
                (Some(a), Some(b)) => a.cmp(&b),

                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
                (None, None) => Ordering::Equal,
            };

            return if is_asc { cmp } else { cmp.reverse() };
        }
    }
}

fn compare_values_for_sort_non_null(a: &Value, b: &Value) -> Ordering {
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return x.cmp(&y);
    }

    if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
        return x.partial_cmp(&y).unwrap_or(Ordering::Equal);
    }

    if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
        return x.cmp(y);
    }

    if let (Some(x), Some(y)) = (a.as_bool(), b.as_bool()) {
        return x.cmp(&y);
    }

    if let (Some(x), Some(y)) = (a.as_timestamp(), b.as_timestamp()) {
        return x.cmp(&y);
    }

    if let (Some(x), Some(y)) = (a.as_i64(), b.as_f64()) {
        return (x as f64).partial_cmp(&y).unwrap_or(Ordering::Equal);
    }

    if let (Some(x), Some(y)) = (a.as_f64(), b.as_i64()) {
        return x.partial_cmp(&(y as f64)).unwrap_or(Ordering::Equal);
    }

    if let (Some(x), Some(y)) = (a.as_bytes(), b.as_bytes()) {
        return x.cmp(y);
    }

    if let (Some(left_map), Some(right_map)) = (a.as_struct(), b.as_struct()) {
        let left_vals: Vec<&Value> = left_map.values().collect();
        let right_vals: Vec<&Value> = right_map.values().collect();

        if left_vals.len() != right_vals.len() {
            return left_vals.len().cmp(&right_vals.len());
        }

        for (l, r) in left_vals.iter().zip(right_vals.iter()) {
            if l.is_null() && r.is_null() {
                continue;
            }
            if l.is_null() {
                return Ordering::Greater;
            }
            if r.is_null() {
                return Ordering::Less;
            }

            let cmp = compare_values_for_sort_non_null(l, r);
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        return Ordering::Equal;
    }

    Ordering::Equal
}

fn collect_column_refs_from_expr(expr: &Expr, columns: &mut Vec<String>) {
    match expr {
        Expr::Identifier(ident) => {
            columns.push(ident.value.clone());
        }
        Expr::CompoundIdentifier(parts) => {
            if let Some(last) = parts.last() {
                columns.push(last.value.clone());
            }
        }
        Expr::Function(func) => {
            if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                for arg in &arg_list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) = arg
                    {
                        collect_column_refs_from_expr(e, columns);
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_column_refs_from_expr(left, columns);
            collect_column_refs_from_expr(right, columns);
        }
        Expr::UnaryOp { expr: inner, .. } => {
            collect_column_refs_from_expr(inner, columns);
        }
        Expr::Nested(inner) => {
            collect_column_refs_from_expr(inner, columns);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_column_refs_from_expr(op, columns);
            }
            for case_when in conditions {
                collect_column_refs_from_expr(&case_when.condition, columns);
                collect_column_refs_from_expr(&case_when.result, columns);
            }
            if let Some(else_e) = else_result {
                collect_column_refs_from_expr(else_e, columns);
            }
        }
        Expr::Cast { expr: inner, .. } => {
            collect_column_refs_from_expr(inner, columns);
        }

        _ => {}
    }
}

enum SortSpec {
    Column(usize),
    Expression(Box<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}
