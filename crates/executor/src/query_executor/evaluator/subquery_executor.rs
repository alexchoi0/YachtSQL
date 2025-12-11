use std::cell::RefCell;
use std::rc::Rc;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};
use yachtsql_optimizer::expr::{BinaryOp, Expr, LiteralValue, UnaryOp};
use yachtsql_optimizer::plan::{JoinType, PlanNode};
use yachtsql_storage::Column;

use super::physical_plan::{
    CORRELATION_CONTEXT, CachedSubqueryResult, SubqueryExecutor, UNCORRELATED_SUBQUERY_CACHE,
    hash_plan,
};
use crate::DialectType;
use crate::correlation::{bind_correlation, plan_has_correlation};

#[derive(Clone)]
pub struct SubqueryExecutorImpl {
    _storage: Rc<RefCell<yachtsql_storage::Storage>>,
    _transaction_manager: Rc<RefCell<yachtsql_storage::TransactionManager>>,
    _temporary_storage: Rc<RefCell<yachtsql_storage::TempStorage>>,
    _feature_registry: Rc<yachtsql_capability::FeatureRegistry>,
    _dialect: DialectType,
}

impl SubqueryExecutorImpl {
    pub fn new(
        storage: Rc<RefCell<yachtsql_storage::Storage>>,
        transaction_manager: Rc<RefCell<yachtsql_storage::TransactionManager>>,
        temporary_storage: Rc<RefCell<yachtsql_storage::TempStorage>>,
        feature_registry: Rc<yachtsql_capability::FeatureRegistry>,
        dialect: DialectType,
    ) -> Self {
        Self {
            _storage: storage,
            _transaction_manager: transaction_manager,
            _temporary_storage: temporary_storage,
            _feature_registry: feature_registry,
            _dialect: dialect,
        }
    }

    pub fn execute_subquery(&self, sql: &str) -> Result<crate::Table> {
        use yachtsql_parser::{LogicalPlanBuilder, Parser, Statement};

        let parser = Parser::with_dialect(self._dialect);
        let statements = parser
            .parse_sql(sql)
            .map_err(|e| Error::parse_error(format!("Failed to parse subquery: {}", e)))?;

        if statements.is_empty() {
            return Err(Error::parse_error("Empty subquery".to_string()));
        }

        let statement = &statements[0];

        let plan_builder = LogicalPlanBuilder::new()
            .with_storage(Rc::clone(&self._storage))
            .with_dialect(self._dialect);

        let logical_plan = match statement {
            Statement::Standard(std_stmt) => match std_stmt.ast() {
                sqlparser::ast::Statement::Query(query) => plan_builder.query_to_plan(query),
                _ => Err(Error::invalid_query(
                    "SET subquery must be a SELECT statement".to_string(),
                )),
            },
            Statement::Custom(_) => Err(Error::invalid_query(
                "SET subquery must be a SELECT statement".to_string(),
            )),
        }?;

        self.execute_plan(logical_plan.root())
    }

    fn execute_plan(&self, plan: &PlanNode) -> Result<crate::Table> {
        use yachtsql_storage::Column;

        use crate::Table;

        match plan {
            PlanNode::Scan {
                table_name,
                alias: _,
                projection: _,
                ..
            } => {
                use super::physical_plan::cte::get_cte_from_context;

                let cte_table_name = format!("__cte_{}", table_name);
                if let Some(cte_tables) = get_cte_from_context(&cte_table_name) {
                    if let Some(first_table) = cte_tables.into_iter().next() {
                        return first_table.to_column_format();
                    }
                }

                let storage = self._storage.borrow();

                let table = storage
                    .get_table(table_name)
                    .or_else(|| storage.get_table(&cte_table_name))
                    .ok_or_else(|| {
                        Error::TableNotFound(format!("Table '{}' not found", table_name))
                    })?;

                let schema = table.schema().clone();

                let rows: Vec<_> = table.scan().collect::<Result<Vec<_>>>()?;
                let num_rows = rows.len();

                let mut columns = Vec::new();

                for field in schema.fields() {
                    let mut column = Column::new(&field.data_type, num_rows);
                    for row in &rows {
                        let value = row
                            .get_by_name(&schema, &field.name)
                            .unwrap_or(&Value::null())
                            .clone();
                        column.push(value)?;
                    }
                    columns.push(column);
                }

                Ok(Table::new(schema, columns)?)
            }

            PlanNode::Unnest {
                array_expr,
                alias,
                column_alias: _,
                with_offset,
                offset_alias: _,
            } => {
                use super::physical_plan::ProjectionWithExprExec;

                let empty_batch = Table::empty(yachtsql_storage::Schema::new());
                let array_val = ProjectionWithExprExec::evaluate_expr(array_expr, &empty_batch, 0)?;

                if array_val.is_null() {
                    let col_name = alias.clone().unwrap_or_else(|| "value".to_string());
                    let field = yachtsql_storage::Field::nullable(col_name, DataType::Int64);
                    let schema = yachtsql_storage::Schema::from_fields(vec![field]);
                    return Ok(Table::empty(schema));
                }

                let arr = array_val.as_array().ok_or_else(|| Error::TypeMismatch {
                    expected: "ARRAY".to_string(),
                    actual: array_val.data_type().to_string(),
                })?;

                let num_elements = arr.len();
                let element_type = arr
                    .iter()
                    .find(|v| !v.is_null())
                    .map(|v| v.data_type())
                    .unwrap_or(DataType::Int64);

                let col_name = alias.clone().unwrap_or_else(|| "value".to_string());
                let mut column = Column::new(&element_type, num_elements);
                for element in arr.iter() {
                    column.push(element.clone())?;
                }

                let mut fields = vec![yachtsql_storage::Field::nullable(col_name, element_type)];
                let mut columns = vec![column];

                if *with_offset {
                    let mut offset_col = Column::new(&DataType::Int64, num_elements);
                    for idx in 0..num_elements {
                        offset_col.push(Value::int64(idx as i64))?;
                    }
                    fields.push(yachtsql_storage::Field::nullable("offset", DataType::Int64));
                    columns.push(offset_col);
                }

                let schema = yachtsql_storage::Schema::from_fields(fields);
                Ok(Table::new(schema, columns)?)
            }

            PlanNode::Projection { expressions, input } => {
                use super::physical_plan::ProjectionWithExprExec;

                let input_batch = self.execute_plan(input)?;

                let mut result_columns = Vec::new();
                let mut result_fields = Vec::new();

                let num_rows = input_batch.num_rows();

                for (expr_idx, (expr, alias)) in expressions.iter().enumerate() {
                    let field_name = alias.clone().unwrap_or_else(|| {
                        if let yachtsql_optimizer::expr::Expr::Column { name, .. } = expr {
                            name.clone()
                        } else {
                            format!("expr_{}", expr_idx)
                        }
                    });
                    let mut values = Vec::new();

                    for row_idx in 0..num_rows {
                        let value =
                            ProjectionWithExprExec::evaluate_expr(expr, &input_batch, row_idx)?;
                        values.push(value);
                    }

                    let data_type = values
                        .iter()
                        .find(|v| !v.is_null())
                        .map(|v| v.data_type())
                        .unwrap_or(DataType::String);

                    let mut column = Column::new(&data_type, values.len());
                    for value in values {
                        column.push(value)?;
                    }
                    result_columns.push(column);
                    result_fields.push(yachtsql_storage::Field::nullable(field_name, data_type));
                }

                let result_schema = yachtsql_storage::Schema::from_fields(result_fields);
                Ok(Table::new(result_schema, result_columns)?)
            }

            PlanNode::Filter { predicate, input } => {
                let batch = self.execute_plan(input)?;

                if batch.is_empty() {
                    return Ok(batch);
                }

                let mut passing_rows = Vec::new();
                for row_idx in 0..batch.num_rows() {
                    let result = self.evaluate_predicate(predicate, &batch, row_idx)?;
                    if let Some(true) = result.as_bool() {
                        passing_rows.push(row_idx);
                    }
                }

                if passing_rows.is_empty() {
                    return Ok(Table::empty(batch.schema().clone()));
                }

                if batch.schema().fields().is_empty() {
                    return Ok(Table::empty_with_rows(
                        batch.schema().clone(),
                        passing_rows.len(),
                    ));
                }

                let mut new_columns = Vec::new();
                for col_idx in 0..batch.schema().fields().len() {
                    let input_col = batch.column(col_idx).ok_or_else(|| {
                        Error::InternalError(format!("Column {} not found", col_idx))
                    })?;
                    let field = &batch.schema().fields()[col_idx];
                    let mut new_col = Column::new(&field.data_type, passing_rows.len());
                    for &row_idx in &passing_rows {
                        new_col.push(input_col.get(row_idx)?)?;
                    }
                    new_columns.push(new_col);
                }

                Ok(Table::new(batch.schema().clone(), new_columns)?)
            }

            PlanNode::Aggregate {
                group_by,
                aggregates,
                input,
                grouping_metadata: _,
            } => {
                use super::physical_plan::ProjectionWithExprExec;

                let batch = self.execute_plan(input)?;

                if group_by.is_empty() {
                    let mut result_columns = Vec::new();
                    let mut result_fields = Vec::new();

                    for agg_expr in aggregates.iter() {
                        let (agg_value, agg_type) = self.evaluate_aggregate(agg_expr, &batch)?;

                        let field_name = Self::aggregate_field_name(agg_expr);
                        let mut col = Column::new(&agg_type, 1);
                        col.push(agg_value)?;
                        result_columns.push(col);
                        result_fields.push(yachtsql_storage::Field::nullable(field_name, agg_type));
                    }

                    let result_schema = yachtsql_storage::Schema::from_fields(result_fields);
                    return Table::new(result_schema, result_columns);
                }

                let mut groups: std::collections::HashMap<Vec<String>, Vec<usize>> =
                    std::collections::HashMap::new();

                for row_idx in 0..batch.num_rows() {
                    let mut key = Vec::with_capacity(group_by.len());
                    for group_expr in group_by {
                        let val =
                            ProjectionWithExprExec::evaluate_expr(group_expr, &batch, row_idx)?;
                        key.push(format!("{:?}", val));
                    }
                    groups.entry(key).or_default().push(row_idx);
                }

                let num_groups = groups.len();
                let mut result_fields = Vec::new();
                let mut result_columns = Vec::new();

                for (gb_idx, group_expr) in group_by.iter().enumerate() {
                    let field_name = match group_expr {
                        Expr::Column { name, .. } => name.clone(),
                        _ => format!("group_{}", gb_idx),
                    };

                    let first_row = groups
                        .values()
                        .next()
                        .and_then(|rows| rows.first())
                        .copied();
                    let data_type = if let Some(row_idx) = first_row {
                        ProjectionWithExprExec::evaluate_expr(group_expr, &batch, row_idx)?
                            .data_type()
                    } else {
                        DataType::String
                    };

                    result_fields.push(yachtsql_storage::Field::nullable(
                        field_name,
                        data_type.clone(),
                    ));
                    result_columns.push(Column::new(&data_type, num_groups));
                }

                for (agg_idx, agg_expr) in aggregates.iter().enumerate() {
                    let field_name = match agg_expr {
                        Expr::Aggregate { name, .. } => format!("{:?}", name).to_lowercase(),
                        _ => format!("agg_{}", agg_idx),
                    };

                    let first_group_rows = groups.values().next();
                    let (_, data_type) = if let Some(rows) = first_group_rows {
                        let group_batch = self.create_group_batch(&batch, rows)?;
                        self.evaluate_aggregate(agg_expr, &group_batch)?
                    } else {
                        (Value::null(), DataType::Int64)
                    };

                    result_fields.push(yachtsql_storage::Field::nullable(
                        field_name,
                        data_type.clone(),
                    ));
                    result_columns.push(Column::new(&data_type, num_groups));
                }

                for (group_idx, (_key, row_indices)) in groups.iter().enumerate() {
                    let representative_row = row_indices[0];

                    for (gb_idx, group_expr) in group_by.iter().enumerate() {
                        let val = ProjectionWithExprExec::evaluate_expr(
                            group_expr,
                            &batch,
                            representative_row,
                        )?;
                        result_columns[gb_idx].push(val)?;
                    }

                    let group_batch = self.create_group_batch(&batch, row_indices)?;

                    for (agg_idx, agg_expr) in aggregates.iter().enumerate() {
                        let (agg_val, _) = self.evaluate_aggregate(agg_expr, &group_batch)?;
                        result_columns[group_by.len() + agg_idx].push(agg_val)?;
                    }
                }

                let result_schema = yachtsql_storage::Schema::from_fields(result_fields);
                Ok(Table::new(result_schema, result_columns)?)
            }

            PlanNode::Limit {
                limit,
                offset,
                input,
            } => {
                let batch = self.execute_plan(input)?.to_column_format()?;

                let start = *offset;
                let end = start + limit;

                if start >= batch.num_rows() {
                    return Ok(Table::empty(batch.schema().clone()));
                }

                let actual_end = end.min(batch.num_rows());
                let result_rows = actual_end - start;

                let columns = batch.columns().ok_or_else(|| {
                    Error::InternalError("Expected column-format table".to_string())
                })?;

                let mut new_columns = Vec::new();
                for col in columns {
                    let mut new_col = Column::new(&col.data_type(), result_rows);
                    for i in start..actual_end {
                        new_col.push(col.get(i)?)?;
                    }
                    new_columns.push(new_col);
                }

                Ok(Table::new(batch.schema().clone(), new_columns)?)
            }

            PlanNode::EmptyRelation => {
                let schema = yachtsql_storage::Schema::from_fields(vec![]);
                Ok(Table::empty_with_rows(schema, 1))
            }

            PlanNode::SubqueryScan { subquery, alias: _ } => self.execute_plan(subquery),

            PlanNode::Sort { order_by, input } => {
                use super::physical_plan::ProjectionWithExprExec;

                let batch = self.execute_plan(input)?.to_column_format()?;

                if batch.is_empty() {
                    return Ok(batch);
                }

                let mut row_indices: Vec<usize> = (0..batch.num_rows()).collect();

                row_indices.sort_by(|&a, &b| {
                    for order_expr in order_by {
                        let a_val =
                            ProjectionWithExprExec::evaluate_expr(&order_expr.expr, &batch, a)
                                .unwrap_or_else(|_| Value::null());
                        let b_val =
                            ProjectionWithExprExec::evaluate_expr(&order_expr.expr, &batch, b)
                                .unwrap_or_else(|_| Value::null());

                        let cmp = self.compare_values(&a_val, &b_val);

                        let asc = order_expr.asc.unwrap_or(true);
                        let ordering = if asc { cmp } else { cmp.reverse() };

                        if ordering != std::cmp::Ordering::Equal {
                            return ordering;
                        }
                    }
                    std::cmp::Ordering::Equal
                });

                let columns = batch.columns().ok_or_else(|| {
                    Error::InternalError("Expected column-format table".to_string())
                })?;

                let mut new_columns = Vec::new();
                for col in columns {
                    let mut new_col = Column::new(&col.data_type(), row_indices.len());
                    for &row_idx in &row_indices {
                        new_col.push(col.get(row_idx)?)?;
                    }
                    new_columns.push(new_col);
                }

                Ok(Table::new(batch.schema().clone(), new_columns)?)
            }

            PlanNode::Distinct { input } => {
                let batch = self.execute_plan(input)?.to_column_format()?;

                if batch.is_empty() {
                    return Ok(batch);
                }

                let columns = batch.columns().ok_or_else(|| {
                    Error::InternalError("Expected column-format table".to_string())
                })?;

                let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
                let mut unique_rows: Vec<usize> = Vec::new();

                for row_idx in 0..batch.num_rows() {
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    use std::hash::Hash;

                    for col in columns {
                        let val = col.get(row_idx)?;
                        format!("{:?}", val).hash(&mut hasher);
                    }

                    use std::hash::Hasher;
                    let hash_bytes = hasher.finish().to_le_bytes().to_vec();

                    if seen.insert(hash_bytes) {
                        unique_rows.push(row_idx);
                    }
                }

                let mut new_columns = Vec::new();
                for col in columns {
                    let mut new_col = Column::new(&col.data_type(), unique_rows.len());
                    for &row_idx in &unique_rows {
                        new_col.push(col.get(row_idx)?)?;
                    }
                    new_columns.push(new_col);
                }

                Ok(Table::new(batch.schema().clone(), new_columns)?)
            }

            PlanNode::Cte {
                name,
                cte_plan,
                input,
                column_aliases,
                ..
            } => {
                let cte_result = self.execute_plan(cte_plan)?;
                let cte_table = cte_result.to_column_format()?;

                let mut schema = cte_table.schema().clone();
                if let Some(aliases) = column_aliases {
                    let renamed_fields: Vec<yachtsql_storage::Field> = schema
                        .fields()
                        .iter()
                        .zip(aliases.iter())
                        .map(|(field, alias)| {
                            let mut new_field = field.clone();
                            new_field.name = alias.clone();
                            new_field
                        })
                        .collect();
                    schema = yachtsql_storage::Schema::from_fields(renamed_fields);
                }

                let temp_table_name = format!("__cte_{}", name);

                {
                    let mut storage = self._storage.borrow_mut();
                    if storage.get_dataset("default").is_none() {
                        storage.create_dataset("default".to_string())?;
                    }
                    let dataset = storage
                        .get_dataset_mut("default")
                        .ok_or_else(|| Error::DatasetNotFound("default".to_string()))?;

                    dataset.create_table(temp_table_name.clone(), schema)?;
                    let table = dataset
                        .get_table_mut(&temp_table_name)
                        .ok_or_else(|| Error::table_not_found(temp_table_name.clone()))?;

                    for row_idx in 0..cte_table.num_rows() {
                        let row = cte_table.row(row_idx)?;
                        table.insert_row(row)?;
                    }
                }

                let result = self.execute_plan(input);

                {
                    let mut storage = self._storage.borrow_mut();
                    if let Some(dataset) = storage.get_dataset_mut("default") {
                        let _ = dataset.delete_table(&temp_table_name);
                    }
                }

                result
            }

            PlanNode::Union { left, right, all } => {
                let left_batch = self.execute_plan(left)?.to_column_format()?;
                let right_batch = self.execute_plan(right)?.to_column_format()?;

                if left_batch.schema().fields().len() != right_batch.schema().fields().len() {
                    return Err(Error::InvalidQuery(
                        "UNION operands must have the same number of columns".to_string(),
                    ));
                }

                let num_cols = left_batch.schema().fields().len();
                let total_rows = left_batch.num_rows() + right_batch.num_rows();

                let mut new_columns = Vec::new();
                for col_idx in 0..num_cols {
                    let left_col = left_batch.column(col_idx).ok_or_else(|| {
                        Error::InternalError(format!("Column {} not found in left batch", col_idx))
                    })?;
                    let right_col = right_batch.column(col_idx).ok_or_else(|| {
                        Error::InternalError(format!("Column {} not found in right batch", col_idx))
                    })?;

                    let mut new_col = Column::new(&left_col.data_type(), total_rows);

                    for i in 0..left_batch.num_rows() {
                        new_col.push(left_col.get(i)?)?;
                    }
                    for i in 0..right_batch.num_rows() {
                        new_col.push(right_col.get(i)?)?;
                    }

                    new_columns.push(new_col);
                }

                let combined = Table::new(left_batch.schema().clone(), new_columns)?;

                if *all {
                    Ok(combined)
                } else {
                    let combined_cols = combined.columns().ok_or_else(|| {
                        Error::InternalError("Expected column-format table".to_string())
                    })?;

                    let mut seen: std::collections::HashSet<Vec<u8>> =
                        std::collections::HashSet::new();
                    let mut unique_rows: Vec<usize> = Vec::new();

                    for row_idx in 0..combined.num_rows() {
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        use std::hash::Hash;

                        for col in combined_cols {
                            let val = col.get(row_idx)?;
                            format!("{:?}", val).hash(&mut hasher);
                        }

                        use std::hash::Hasher;
                        let hash_bytes = hasher.finish().to_le_bytes().to_vec();

                        if seen.insert(hash_bytes) {
                            unique_rows.push(row_idx);
                        }
                    }

                    let mut unique_columns = Vec::new();
                    for col in combined_cols {
                        let mut new_col = Column::new(&col.data_type(), unique_rows.len());
                        for &row_idx in &unique_rows {
                            new_col.push(col.get(row_idx)?)?;
                        }
                        unique_columns.push(new_col);
                    }

                    Ok(Table::new(combined.schema().clone(), unique_columns)?)
                }
            }

            PlanNode::Join {
                left,
                right,
                on,
                join_type,
                using_columns: _,
            } => {
                let left_batch = self.execute_plan(left)?.to_column_format()?;
                let right_batch = self.execute_plan(right)?.to_column_format()?;

                let left_fields = left_batch.schema().fields().to_vec();
                let right_fields = right_batch.schema().fields().to_vec();

                let mut combined_fields = left_fields.clone();
                combined_fields.extend(right_fields.clone());
                let combined_schema = yachtsql_storage::Schema::from_fields(combined_fields);

                let left_cols = left_batch.columns().ok_or_else(|| {
                    Error::InternalError("Expected column-format table for left".to_string())
                })?;
                let right_cols = right_batch.columns().ok_or_else(|| {
                    Error::InternalError("Expected column-format table for right".to_string())
                })?;

                let mut result_rows: Vec<(usize, usize)> = Vec::new();
                let mut left_matched: std::collections::HashSet<usize> =
                    std::collections::HashSet::new();
                let mut right_matched: std::collections::HashSet<usize> =
                    std::collections::HashSet::new();

                for left_idx in 0..left_batch.num_rows() {
                    for right_idx in 0..right_batch.num_rows() {
                        let combined_row = self.build_combined_row(
                            left_cols,
                            &left_fields,
                            left_idx,
                            right_cols,
                            &right_fields,
                            right_idx,
                        )?;

                        let matched = self.evaluate_join_condition(on, &combined_row)?;

                        if matched {
                            result_rows.push((left_idx, right_idx));
                            left_matched.insert(left_idx);
                            right_matched.insert(right_idx);
                        }
                    }
                }

                match join_type {
                    JoinType::Inner | JoinType::Cross => {}
                    JoinType::Left => {
                        for left_idx in 0..left_batch.num_rows() {
                            if !left_matched.contains(&left_idx) {
                                result_rows.push((left_idx, usize::MAX));
                            }
                        }
                    }
                    JoinType::Right => {
                        for right_idx in 0..right_batch.num_rows() {
                            if !right_matched.contains(&right_idx) {
                                result_rows.push((usize::MAX, right_idx));
                            }
                        }
                    }
                    JoinType::Full => {
                        for left_idx in 0..left_batch.num_rows() {
                            if !left_matched.contains(&left_idx) {
                                result_rows.push((left_idx, usize::MAX));
                            }
                        }
                        for right_idx in 0..right_batch.num_rows() {
                            if !right_matched.contains(&right_idx) {
                                result_rows.push((usize::MAX, right_idx));
                            }
                        }
                    }
                    JoinType::Semi => {
                        let mut seen = std::collections::HashSet::new();
                        result_rows.retain(|(left_idx, _)| seen.insert(*left_idx));
                    }
                    JoinType::Anti => {
                        result_rows.clear();
                        for left_idx in 0..left_batch.num_rows() {
                            if !left_matched.contains(&left_idx) {
                                result_rows.push((left_idx, usize::MAX));
                            }
                        }
                    }
                    _ => {}
                }

                let num_cols = combined_schema.fields().len();
                let num_left_cols = left_fields.len();
                let mut new_columns = Vec::with_capacity(num_cols);

                for col_idx in 0..num_cols {
                    let field = &combined_schema.fields()[col_idx];
                    let mut new_col = Column::new(&field.data_type, result_rows.len());

                    for &(left_idx, right_idx) in &result_rows {
                        if col_idx < num_left_cols {
                            if left_idx == usize::MAX {
                                new_col.push(Value::null())?;
                            } else {
                                new_col.push(left_cols[col_idx].get(left_idx)?)?;
                            }
                        } else {
                            let right_col_idx = col_idx - num_left_cols;
                            if right_idx == usize::MAX {
                                new_col.push(Value::null())?;
                            } else {
                                new_col.push(right_cols[right_col_idx].get(right_idx)?)?;
                            }
                        }
                    }
                    new_columns.push(new_col);
                }

                Ok(Table::new(combined_schema, new_columns)?)
            }

            _ => Err(Error::UnsupportedFeature(format!(
                "Subquery execution for {:?} not yet implemented",
                plan
            ))),
        }
    }

    fn build_combined_row(
        &self,
        left_cols: &[Column],
        left_fields: &[yachtsql_storage::Field],
        left_idx: usize,
        right_cols: &[Column],
        right_fields: &[yachtsql_storage::Field],
        right_idx: usize,
    ) -> Result<std::collections::HashMap<String, Value>> {
        let mut row = std::collections::HashMap::new();

        for (col_idx, field) in left_fields.iter().enumerate() {
            let value = left_cols[col_idx].get(left_idx)?;
            row.insert(field.name.clone(), value.clone());
            if let Some(table) = &field.source_table {
                row.insert(format!("{}.{}", table, field.name), value);
            }
        }

        for (col_idx, field) in right_fields.iter().enumerate() {
            let value = right_cols[col_idx].get(right_idx)?;
            row.insert(field.name.clone(), value.clone());
            if let Some(table) = &field.source_table {
                row.insert(format!("{}.{}", table, field.name), value);
            }
        }

        Ok(row)
    }

    fn evaluate_join_condition(
        &self,
        condition: &Expr,
        row: &std::collections::HashMap<String, Value>,
    ) -> Result<bool> {
        match condition {
            Expr::Literal(LiteralValue::Boolean(true)) => Ok(true),
            Expr::Literal(LiteralValue::Boolean(false)) => Ok(false),

            Expr::BinaryOp { left, op, right } => match op {
                BinaryOp::And => {
                    let left_result = self.evaluate_join_condition(left, row)?;
                    if !left_result {
                        return Ok(false);
                    }
                    self.evaluate_join_condition(right, row)
                }
                BinaryOp::Or => {
                    let left_result = self.evaluate_join_condition(left, row)?;
                    if left_result {
                        return Ok(true);
                    }
                    self.evaluate_join_condition(right, row)
                }
                _ => {
                    let left_val = self.evaluate_join_expr(left, row)?;
                    let right_val = self.evaluate_join_expr(right, row)?;
                    self.apply_binary_op_bool(&left_val, op, &right_val)
                }
            },

            Expr::Column { name, table } => {
                if let Some(t) = table {
                    let key = format!("{}.{}", t, name);
                    if let Some(v) = row.get(&key) {
                        return Ok(v.as_bool().unwrap_or(false));
                    }
                }
                if let Some(v) = row.get(name) {
                    return Ok(v.as_bool().unwrap_or(false));
                }
                Ok(false)
            }

            _ => Ok(true),
        }
    }

    fn evaluate_join_expr(
        &self,
        expr: &Expr,
        row: &std::collections::HashMap<String, Value>,
    ) -> Result<Value> {
        match expr {
            Expr::Column { name, table } => {
                if let Some(t) = table {
                    let key = format!("{}.{}", t, name);
                    if let Some(v) = row.get(&key) {
                        return Ok(v.clone());
                    }
                }
                if let Some(v) = row.get(name) {
                    return Ok(v.clone());
                }
                Ok(Value::null())
            }
            Expr::Literal(lit) => Ok(self.literal_to_value(lit)),
            _ => Ok(Value::null()),
        }
    }

    fn apply_binary_op_bool(&self, left: &Value, op: &BinaryOp, right: &Value) -> Result<bool> {
        if left.is_null() || right.is_null() {
            return Ok(false);
        }

        match op {
            BinaryOp::Equal => Ok(left == right),
            BinaryOp::NotEqual => Ok(left != right),
            BinaryOp::LessThan => Ok(self.compare_values(left, right) == std::cmp::Ordering::Less),
            BinaryOp::LessThanOrEqual => {
                Ok(self.compare_values(left, right) != std::cmp::Ordering::Greater)
            }
            BinaryOp::GreaterThan => {
                Ok(self.compare_values(left, right) == std::cmp::Ordering::Greater)
            }
            BinaryOp::GreaterThanOrEqual => {
                Ok(self.compare_values(left, right) != std::cmp::Ordering::Less)
            }
            _ => Ok(false),
        }
    }

    fn aggregate_field_name(agg_expr: &Expr) -> String {
        match agg_expr {
            Expr::Aggregate { name, args, .. } => {
                let args_str = args
                    .iter()
                    .map(|arg| match arg {
                        Expr::Column { name, .. } => name.clone(),
                        Expr::Literal(lit) => format!("{:?}", lit),
                        _ => "expr".to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({})", name.as_str(), args_str)
            }
            _ => "agg".to_string(),
        }
    }

    fn compare_values(&self, a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        if a.is_null() && b.is_null() {
            return Ordering::Equal;
        }
        if a.is_null() {
            return Ordering::Greater;
        }
        if b.is_null() {
            return Ordering::Less;
        }

        if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
            return ai.cmp(&bi);
        }
        if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
            return af.partial_cmp(&bf).unwrap_or(Ordering::Equal);
        }
        if let (Some(a_str), Some(b_str)) = (a.as_str(), b.as_str()) {
            return a_str.cmp(b_str);
        }
        if let (Some(ad), Some(bd)) = (a.as_numeric(), b.as_numeric()) {
            return ad.cmp(&bd);
        }

        Ordering::Equal
    }

    fn create_group_batch(
        &self,
        batch: &crate::Table,
        row_indices: &[usize],
    ) -> Result<crate::Table> {
        use yachtsql_storage::Column;

        use crate::Table;

        let columns = batch
            .columns()
            .ok_or_else(|| Error::InternalError("Expected column-format table".to_string()));

        let columns = match columns {
            Ok(cols) => cols,
            Err(_) => {
                let converted = batch.to_column_format()?;
                return self.create_group_batch(&converted, row_indices);
            }
        };

        let mut new_columns = Vec::new();
        for col in columns {
            let mut new_col = Column::new(&col.data_type(), row_indices.len());
            for &row_idx in row_indices {
                new_col.push(col.get(row_idx)?)?;
            }
            new_columns.push(new_col);
        }

        Table::new(batch.schema().clone(), new_columns)
    }

    fn evaluate_predicate(
        &self,
        predicate: &Expr,
        batch: &crate::Table,
        row_idx: usize,
    ) -> Result<Value> {
        match predicate {
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_predicate(left, batch, row_idx)?;
                let right_val = self.evaluate_predicate(right, batch, row_idx)?;
                self.apply_binary_op(&left_val, op, &right_val)
            }
            Expr::Column { name, table: _ } => {
                for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                    if field.name.eq_ignore_ascii_case(name) {
                        return batch
                            .column(col_idx)
                            .ok_or_else(|| {
                                Error::InternalError(format!("Column {} not found", col_idx))
                            })?
                            .get(row_idx);
                    }
                }
                Err(Error::InvalidQuery(format!("Column '{}' not found", name)))
            }
            Expr::Literal(lit) => Ok(self.literal_to_value(lit)),

            Expr::UnaryOp { op, expr } => {
                let val = self.evaluate_predicate(expr, batch, row_idx)?;
                match op {
                    UnaryOp::Not => {
                        if val.is_null() {
                            Ok(Value::null())
                        } else {
                            match val.as_bool() {
                                Some(b) => Ok(Value::bool_val(!b)),
                                None => Ok(Value::null()),
                            }
                        }
                    }
                    UnaryOp::IsNull => Ok(Value::bool_val(val.is_null())),
                    UnaryOp::IsNotNull => Ok(Value::bool_val(!val.is_null())),
                    UnaryOp::Negate => {
                        if val.is_null() {
                            Ok(Value::null())
                        } else if let Some(i) = val.as_i64() {
                            Ok(Value::int64(-i))
                        } else if let Some(f) = val.as_f64() {
                            Ok(Value::float64(-f))
                        } else if let Some(d) = val.as_numeric() {
                            Ok(Value::numeric(-d))
                        } else {
                            Ok(Value::null())
                        }
                    }
                    UnaryOp::Plus => Ok(val),
                    UnaryOp::BitwiseNot => {
                        if val.is_null() {
                            Ok(Value::null())
                        } else if let Some(i) = val.as_i64() {
                            Ok(Value::int64(!i))
                        } else {
                            Ok(Value::null())
                        }
                    }
                    UnaryOp::TSQueryNot => {
                        if val.is_null() {
                            Ok(Value::null())
                        } else if let Some(s) = val.as_str() {
                            match yachtsql_functions::fulltext::tsquery_negate(s) {
                                Ok(result) => Ok(Value::string(result)),
                                Err(_) => Ok(Value::null()),
                            }
                        } else {
                            Ok(Value::null())
                        }
                    }
                }
            }

            Expr::InSubquery {
                expr,
                plan,
                negated,
            } => {
                let value = self.evaluate_predicate(expr, batch, row_idx)?;
                let subquery_values = self.execute_in_subquery(plan)?;

                if subquery_values.is_empty() {
                    return Ok(Value::bool_val(*negated));
                }

                for subquery_val in &subquery_values {
                    if !subquery_val.is_null() && !value.is_null() && value == *subquery_val {
                        return Ok(Value::bool_val(!*negated));
                    }
                }
                Ok(Value::bool_val(*negated))
            }

            _ => Err(Error::UnsupportedFeature(format!(
                "Predicate expression {:?} not supported in subquery",
                predicate
            ))),
        }
    }

    fn apply_binary_op(&self, left: &Value, op: &BinaryOp, right: &Value) -> Result<Value> {
        match op {
            BinaryOp::Equal => {
                if left.is_null() || right.is_null() {
                    Ok(Value::null())
                } else {
                    Ok(Value::bool_val(left == right))
                }
            }
            BinaryOp::NotEqual => {
                if left.is_null() || right.is_null() {
                    Ok(Value::null())
                } else {
                    Ok(Value::bool_val(left != right))
                }
            }
            BinaryOp::And => {
                let l = left.as_bool();
                let r = right.as_bool();
                match (l, r) {
                    (Some(false), _) | (_, Some(false)) => Ok(Value::bool_val(false)),
                    (Some(true), Some(true)) => Ok(Value::bool_val(true)),
                    _ => Ok(Value::null()),
                }
            }
            BinaryOp::Or => {
                let l = left.as_bool();
                let r = right.as_bool();
                match (l, r) {
                    (Some(true), _) | (_, Some(true)) => Ok(Value::bool_val(true)),
                    (Some(false), Some(false)) => Ok(Value::bool_val(false)),
                    _ => Ok(Value::null()),
                }
            }
            BinaryOp::LessThan => {
                if left.is_null() || right.is_null() {
                    Ok(Value::null())
                } else if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    Ok(Value::bool_val(l < r))
                } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    Ok(Value::bool_val(l < r))
                } else if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
                    Ok(Value::bool_val(l < r))
                } else {
                    Ok(Value::null())
                }
            }
            BinaryOp::LessThanOrEqual => {
                if left.is_null() || right.is_null() {
                    Ok(Value::null())
                } else if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    Ok(Value::bool_val(l <= r))
                } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    Ok(Value::bool_val(l <= r))
                } else if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
                    Ok(Value::bool_val(l <= r))
                } else {
                    Ok(Value::null())
                }
            }
            BinaryOp::GreaterThan => {
                if left.is_null() || right.is_null() {
                    Ok(Value::null())
                } else if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    Ok(Value::bool_val(l > r))
                } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    Ok(Value::bool_val(l > r))
                } else if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
                    Ok(Value::bool_val(l > r))
                } else {
                    Ok(Value::null())
                }
            }
            BinaryOp::GreaterThanOrEqual => {
                if left.is_null() || right.is_null() {
                    Ok(Value::null())
                } else if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    Ok(Value::bool_val(l >= r))
                } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    Ok(Value::bool_val(l >= r))
                } else if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
                    Ok(Value::bool_val(l >= r))
                } else {
                    Ok(Value::null())
                }
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Binary operator {:?} not supported in subquery predicate",
                op
            ))),
        }
    }

    fn literal_to_value(&self, lit: &LiteralValue) -> Value {
        match lit {
            LiteralValue::Null => Value::null(),
            LiteralValue::Boolean(b) => Value::bool_val(*b),
            LiteralValue::Int64(i) => Value::int64(*i),
            LiteralValue::Float64(f) => Value::float64(*f),
            LiteralValue::Numeric(d) => Value::numeric(*d),
            LiteralValue::String(s) => Value::string(s.clone()),
            _ => Value::null(),
        }
    }

    fn evaluate_aggregate(
        &self,
        agg_expr: &Expr,
        batch: &crate::Table,
    ) -> Result<(Value, DataType)> {
        match agg_expr {
            Expr::Aggregate {
                name,
                args,
                distinct: _,
                filter: _,
                order_by: _,
            } => {
                let agg_name = format!("{:?}", name).to_uppercase();

                let col_values: Vec<Value> = if args.is_empty() {
                    vec![]
                } else {
                    let arg = &args[0];
                    self.get_column_values(arg, batch)?
                };

                match agg_name.as_str() {
                    name if name.contains("SUM") => {
                        let result = self.compute_sum(&col_values)?;
                        let data_type = if col_values.iter().any(|v| v.is_numeric()) {
                            DataType::Numeric(None)
                        } else {
                            DataType::Float64
                        };
                        Ok((result, data_type))
                    }
                    name if name.contains("AVG") => {
                        let result = self.compute_avg(&col_values)?;
                        let data_type = if col_values.iter().any(|v| v.is_numeric()) {
                            DataType::Numeric(None)
                        } else {
                            DataType::Float64
                        };
                        Ok((result, data_type))
                    }
                    name if name.contains("COUNT") => {
                        let count = if args.is_empty() {
                            batch.num_rows() as i64
                        } else {
                            col_values.iter().filter(|v| !v.is_null()).count() as i64
                        };
                        Ok((Value::int64(count), DataType::Int64))
                    }
                    name if name.contains("MAX") => {
                        let result = self.compute_max(&col_values)?;
                        let data_type = col_values
                            .first()
                            .map(|v| v.data_type())
                            .unwrap_or(DataType::Float64);
                        Ok((result, data_type))
                    }
                    name if name.contains("MIN") => {
                        let result = self.compute_min(&col_values)?;
                        let data_type = col_values
                            .first()
                            .map(|v| v.data_type())
                            .unwrap_or(DataType::Float64);
                        Ok((result, data_type))
                    }
                    name if name.contains("STDDEV_POP") || name.contains("STDDEVPOP") => {
                        let result = self.compute_stddev_pop(&col_values)?;
                        Ok((result, DataType::Float64))
                    }
                    name if name.contains("STDDEV_SAMP")
                        || name.contains("STDDEVSAMP")
                        || name.contains("STDDEV") =>
                    {
                        let result = self.compute_stddev_samp(&col_values)?;
                        Ok((result, DataType::Float64))
                    }
                    name if name.contains("VAR_POP") || name.contains("VARPOP") => {
                        let result = self.compute_var_pop(&col_values)?;
                        Ok((result, DataType::Float64))
                    }
                    name if name.contains("VAR_SAMP")
                        || name.contains("VARSAMP")
                        || name.contains("VARIANCE") =>
                    {
                        let result = self.compute_var_samp(&col_values)?;
                        Ok((result, DataType::Float64))
                    }
                    _ => Err(Error::UnsupportedFeature(format!(
                        "Aggregate function {} not supported in subquery",
                        agg_name
                    ))),
                }
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Non-aggregate expression {:?} in aggregate position",
                agg_expr
            ))),
        }
    }

    fn get_column_values(&self, expr: &Expr, batch: &crate::Table) -> Result<Vec<Value>> {
        use super::physical_plan::ProjectionWithExprExec;

        match expr {
            Expr::Wildcard => {
                let mut values = Vec::with_capacity(batch.num_rows());
                for _ in 0..batch.num_rows() {
                    values.push(Value::int64(1));
                }
                Ok(values)
            }

            Expr::Column { name, table: _ } => {
                for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                    if field.name.eq_ignore_ascii_case(name) {
                        let col = batch.column(col_idx).ok_or_else(|| {
                            Error::InternalError(format!("Column {} not found", col_idx))
                        })?;
                        let mut values = Vec::with_capacity(batch.num_rows());
                        for i in 0..batch.num_rows() {
                            values.push(col.get(i)?);
                        }
                        return Ok(values);
                    }
                }
                Err(Error::InvalidQuery(format!("Column '{}' not found", name)))
            }

            Expr::Function { .. }
            | Expr::BinaryOp { .. }
            | Expr::UnaryOp { .. }
            | Expr::Cast { .. }
            | Expr::Case { .. }
            | Expr::Literal(_) => {
                let mut values = Vec::with_capacity(batch.num_rows());
                for i in 0..batch.num_rows() {
                    let value = ProjectionWithExprExec::evaluate_expr(expr, batch, i)?;
                    values.push(value);
                }
                Ok(values)
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Expression {:?} not supported as aggregate argument",
                expr
            ))),
        }
    }

    fn compute_sum(&self, values: &[Value]) -> Result<Value> {
        use rust_decimal::Decimal;

        let mut sum = Decimal::ZERO;
        let mut has_numeric = false;
        let mut count = 0;

        for v in values {
            if v.is_null() {
                continue;
            }
            count += 1;
            if let Some(d) = v.as_numeric() {
                sum += d;
                has_numeric = true;
            } else if let Some(i) = v.as_i64() {
                sum += Decimal::from(i);
            } else if let Some(f) = v.as_f64() {
                sum += Decimal::try_from(f).unwrap_or(Decimal::ZERO);
            }
        }

        if count == 0 {
            return Ok(Value::null());
        }

        if has_numeric {
            Ok(Value::numeric(sum))
        } else {
            Ok(Value::float64(
                sum.to_string().parse::<f64>().unwrap_or(0.0),
            ))
        }
    }

    fn compute_avg(&self, values: &[Value]) -> Result<Value> {
        use rust_decimal::Decimal;

        let mut sum = Decimal::ZERO;
        let mut has_numeric = false;
        let mut count = 0;

        for v in values {
            if v.is_null() {
                continue;
            }
            count += 1;
            if let Some(d) = v.as_numeric() {
                sum += d;
                has_numeric = true;
            } else if let Some(i) = v.as_i64() {
                sum += Decimal::from(i);
            } else if let Some(f) = v.as_f64() {
                sum += Decimal::try_from(f).unwrap_or(Decimal::ZERO);
            }
        }

        if count == 0 {
            return Ok(Value::null());
        }

        let avg = sum / Decimal::from(count);

        if has_numeric {
            Ok(Value::numeric(avg))
        } else {
            Ok(Value::float64(
                avg.to_string().parse::<f64>().unwrap_or(0.0),
            ))
        }
    }

    fn compute_max(&self, values: &[Value]) -> Result<Value> {
        let non_null: Vec<_> = values.iter().filter(|v| !v.is_null()).collect();
        if non_null.is_empty() {
            return Ok(Value::null());
        }

        let mut max = non_null[0].clone();
        for v in non_null.iter().skip(1) {
            if self.value_greater_than(v, &max) {
                max = (*v).clone();
            }
        }
        Ok(max)
    }

    fn compute_min(&self, values: &[Value]) -> Result<Value> {
        let non_null: Vec<_> = values.iter().filter(|v| !v.is_null()).collect();
        if non_null.is_empty() {
            return Ok(Value::null());
        }

        let mut min = non_null[0].clone();
        for v in non_null.iter().skip(1) {
            if self.value_less_than(v, &min) {
                min = (*v).clone();
            }
        }
        Ok(min)
    }

    fn value_greater_than(&self, v1: &Value, v2: &Value) -> bool {
        if let (Some(d1), Some(d2)) = (v1.as_numeric(), v2.as_numeric()) {
            return d1 > d2;
        }
        if let (Some(i1), Some(i2)) = (v1.as_i64(), v2.as_i64()) {
            return i1 > i2;
        }
        if let (Some(f1), Some(f2)) = (v1.as_f64(), v2.as_f64()) {
            return f1 > f2;
        }
        if let (Some(s1), Some(s2)) = (v1.as_str(), v2.as_str()) {
            return s1 > s2;
        }
        if let (Some(d1), Some(d2)) = (v1.as_date(), v2.as_date()) {
            return d1 > d2;
        }
        if let (Some(dt1), Some(dt2)) = (v1.as_datetime(), v2.as_datetime()) {
            return dt1 > dt2;
        }
        if let (Some(ts1), Some(ts2)) = (v1.as_timestamp(), v2.as_timestamp()) {
            return ts1 > ts2;
        }
        if let (Some(t1), Some(t2)) = (v1.as_time(), v2.as_time()) {
            return t1 > t2;
        }
        false
    }

    fn value_less_than(&self, v1: &Value, v2: &Value) -> bool {
        if let (Some(d1), Some(d2)) = (v1.as_numeric(), v2.as_numeric()) {
            return d1 < d2;
        }
        if let (Some(i1), Some(i2)) = (v1.as_i64(), v2.as_i64()) {
            return i1 < i2;
        }
        if let (Some(f1), Some(f2)) = (v1.as_f64(), v2.as_f64()) {
            return f1 < f2;
        }
        if let (Some(s1), Some(s2)) = (v1.as_str(), v2.as_str()) {
            return s1 < s2;
        }
        if let (Some(d1), Some(d2)) = (v1.as_date(), v2.as_date()) {
            return d1 < d2;
        }
        if let (Some(dt1), Some(dt2)) = (v1.as_datetime(), v2.as_datetime()) {
            return dt1 < dt2;
        }
        if let (Some(ts1), Some(ts2)) = (v1.as_timestamp(), v2.as_timestamp()) {
            return ts1 < ts2;
        }
        if let (Some(t1), Some(t2)) = (v1.as_time(), v2.as_time()) {
            return t1 < t2;
        }
        false
    }

    fn values_to_f64(&self, values: &[Value]) -> Vec<f64> {
        use rust_decimal::prelude::ToPrimitive;

        values
            .iter()
            .filter(|v| !v.is_null())
            .filter_map(|v| {
                if let Some(d) = v.as_numeric() {
                    d.to_f64()
                } else if let Some(i) = v.as_i64() {
                    Some(i as f64)
                } else {
                    v.as_f64()
                }
            })
            .collect()
    }

    fn compute_stddev_pop(&self, values: &[Value]) -> Result<Value> {
        let floats = self.values_to_f64(values);
        if floats.is_empty() {
            return Ok(Value::null());
        }

        let n = floats.len() as f64;
        let mean: f64 = floats.iter().sum::<f64>() / n;
        let variance: f64 = floats.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
        Ok(Value::float64(variance.sqrt()))
    }

    fn compute_stddev_samp(&self, values: &[Value]) -> Result<Value> {
        let floats = self.values_to_f64(values);
        if floats.len() < 2 {
            return Ok(Value::null());
        }

        let n = floats.len() as f64;
        let mean: f64 = floats.iter().sum::<f64>() / n;
        let variance: f64 = floats.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
        Ok(Value::float64(variance.sqrt()))
    }

    fn compute_var_pop(&self, values: &[Value]) -> Result<Value> {
        let floats = self.values_to_f64(values);
        if floats.is_empty() {
            return Ok(Value::null());
        }

        let n = floats.len() as f64;
        let mean: f64 = floats.iter().sum::<f64>() / n;
        let variance: f64 = floats.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
        Ok(Value::float64(variance))
    }

    fn compute_var_samp(&self, values: &[Value]) -> Result<Value> {
        let floats = self.values_to_f64(values);
        if floats.len() < 2 {
            return Ok(Value::null());
        }

        let n = floats.len() as f64;
        let mean: f64 = floats.iter().sum::<f64>() / n;
        let variance: f64 = floats.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
        Ok(Value::float64(variance))
    }
}

impl SubqueryExecutor for SubqueryExecutorImpl {
    fn execute_scalar_subquery(&self, plan: &PlanNode) -> Result<Value> {
        let is_uncorrelated = !plan_has_correlation(plan);

        if is_uncorrelated {
            let plan_hash = hash_plan(plan);
            let cached = UNCORRELATED_SUBQUERY_CACHE.with(|cache| {
                cache.borrow().get(&plan_hash).map(|result| match result {
                    CachedSubqueryResult::Scalar(v) => Ok(v.clone()),
                    _ => Err(Error::InternalError(
                        "Cache type mismatch for scalar subquery".to_string(),
                    )),
                })
            });

            if let Some(result) = cached {
                return result;
            }
        }

        let bound_plan = CORRELATION_CONTEXT.with(|ctx| {
            if let Some(ref correlation_ctx) = *ctx.borrow() {
                bind_correlation(plan.clone(), correlation_ctx)
            } else {
                Ok(plan.clone())
            }
        })?;

        let batch = self.execute_plan(&bound_plan)?;

        if batch.num_rows() == 0 {
            let result = Value::null();
            if is_uncorrelated {
                let plan_hash = hash_plan(plan);
                UNCORRELATED_SUBQUERY_CACHE.with(|cache| {
                    cache
                        .borrow_mut()
                        .insert(plan_hash, CachedSubqueryResult::Scalar(result.clone()));
                });
            }
            return Ok(result);
        }

        if batch.num_rows() > 1 {
            return Err(Error::InvalidQuery(
                "Scalar subquery returned more than one row".to_string(),
            ));
        }

        if batch.schema().fields().len() != 1 {
            return Err(Error::InvalidQuery(format!(
                "Scalar subquery must return exactly one column, got {}",
                batch.schema().fields().len()
            )));
        }

        let column = &batch.expect_columns()[0];
        let result = column.get(0)?;

        if is_uncorrelated {
            let plan_hash = hash_plan(plan);
            UNCORRELATED_SUBQUERY_CACHE.with(|cache| {
                cache
                    .borrow_mut()
                    .insert(plan_hash, CachedSubqueryResult::Scalar(result.clone()));
            });
        }

        Ok(result)
    }

    fn execute_exists_subquery(&self, plan: &PlanNode) -> Result<bool> {
        let is_uncorrelated = !plan_has_correlation(plan);

        if is_uncorrelated {
            let plan_hash = hash_plan(plan);
            let cached = UNCORRELATED_SUBQUERY_CACHE.with(|cache| {
                cache.borrow().get(&plan_hash).map(|result| match result {
                    CachedSubqueryResult::Exists(b) => Ok(*b),
                    _ => Err(Error::InternalError(
                        "Cache type mismatch for exists subquery".to_string(),
                    )),
                })
            });

            if let Some(result) = cached {
                return result;
            }
        }

        let batch = self.execute_plan(plan)?;
        let result = batch.num_rows() > 0;

        if is_uncorrelated {
            let plan_hash = hash_plan(plan);
            UNCORRELATED_SUBQUERY_CACHE.with(|cache| {
                cache
                    .borrow_mut()
                    .insert(plan_hash, CachedSubqueryResult::Exists(result));
            });
        }

        Ok(result)
    }

    fn execute_in_subquery(&self, plan: &PlanNode) -> Result<Vec<Value>> {
        let is_uncorrelated = !plan_has_correlation(plan);

        if is_uncorrelated {
            let plan_hash = hash_plan(plan);
            let cached = UNCORRELATED_SUBQUERY_CACHE.with(|cache| {
                cache.borrow().get(&plan_hash).map(|result| match result {
                    CachedSubqueryResult::InList(v) => Ok(v.clone()),
                    _ => Err(Error::InternalError(
                        "Cache type mismatch for in subquery".to_string(),
                    )),
                })
            });

            if let Some(result) = cached {
                return result;
            }
        }

        let batch = self.execute_plan(plan)?;

        if batch.schema().fields().is_empty() {
            return Err(Error::InvalidQuery(
                "IN subquery must return at least one column".to_string(),
            ));
        }

        let column = &batch.expect_columns()[0];
        let mut values = Vec::with_capacity(batch.num_rows());

        for i in 0..batch.num_rows() {
            values.push(column.get(i)?);
        }

        if is_uncorrelated {
            let plan_hash = hash_plan(plan);
            UNCORRELATED_SUBQUERY_CACHE.with(|cache| {
                cache
                    .borrow_mut()
                    .insert(plan_hash, CachedSubqueryResult::InList(values.clone()));
            });
        }

        Ok(values)
    }

    fn execute_tuple_in_subquery(&self, plan: &PlanNode) -> Result<Vec<Vec<Value>>> {
        let batch = self.execute_plan(plan)?.to_column_format()?;

        if batch.schema().fields().is_empty() {
            return Err(Error::InvalidQuery(
                "Tuple IN subquery must return at least one column".to_string(),
            ));
        }

        let columns = batch
            .columns()
            .ok_or_else(|| Error::InternalError("Expected column-format table".to_string()))?;

        let num_cols = columns.len();
        let mut tuples = Vec::with_capacity(batch.num_rows());

        for row_idx in 0..batch.num_rows() {
            let mut tuple = Vec::with_capacity(num_cols);
            for col in columns {
                tuple.push(col.get(row_idx)?);
            }
            tuples.push(tuple);
        }

        Ok(tuples)
    }
}
