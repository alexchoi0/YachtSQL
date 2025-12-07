use std::cell::RefCell;
use std::rc::Rc;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::{BinaryOp, Expr, LiteralValue};
use yachtsql_storage::indexes::IndexKey;
use yachtsql_storage::{Column, Schema, TableIndexOps};

use super::{ExecutionPlan, ExecutionStatistics};
use crate::Table;

#[derive(Debug)]
pub struct IndexScanExec {
    schema: Schema,

    table_name: String,

    index_name: String,

    predicate: Expr,

    projection: Option<Vec<usize>>,

    statistics: ExecutionStatistics,

    storage: Rc<RefCell<yachtsql_storage::Storage>>,
}

impl IndexScanExec {
    pub fn new(
        schema: Schema,
        table_name: String,
        index_name: String,
        predicate: Expr,
        storage: Rc<RefCell<yachtsql_storage::Storage>>,
    ) -> Self {
        Self {
            schema,
            table_name,
            index_name,
            predicate,
            projection: None,
            statistics: ExecutionStatistics {
                num_rows: Some(100),
                memory_usage: None,
                is_sorted: false,
                sort_columns: None,
            },
            storage,
        }
    }

    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    pub fn with_statistics(mut self, statistics: ExecutionStatistics) -> Self {
        self.statistics = statistics;
        self
    }

    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    pub fn predicate(&self) -> &Expr {
        &self.predicate
    }

    fn extract_index_key(&self, predicate: &Expr) -> Result<IndexKey> {
        match predicate {
            Expr::BinaryOp { left, op, right } if matches!(op, BinaryOp::Equal) => {
                if let (Expr::Column { .. }, Expr::Literal(lit)) = (left.as_ref(), right.as_ref()) {
                    let value = lit.to_value();
                    return Ok(IndexKey::single(value));
                }

                if let (Expr::Literal(lit), Expr::Column { .. }) = (left.as_ref(), right.as_ref()) {
                    let value = lit.to_value();
                    return Ok(IndexKey::single(value));
                }

                Err(Error::InternalError(format!(
                    "Cannot extract index key from predicate: {:?}",
                    predicate
                )))
            }
            _ => Err(Error::InternalError(format!(
                "Unsupported predicate for index scan: {:?}",
                predicate
            ))),
        }
    }
}

impl ExecutionPlan for IndexScanExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let index_key = self.extract_index_key(&self.predicate)?;

        let storage = self.storage.borrow();

        let (dataset_name, table_id) = if let Some(dot_pos) = self.table_name.find('.') {
            let dataset = &self.table_name[..dot_pos];
            let table = &self.table_name[dot_pos + 1..];
            (dataset, table)
        } else {
            ("default", self.table_name.as_str())
        };

        let dataset = storage.get_dataset(dataset_name).ok_or_else(|| {
            Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_name))
        })?;

        let table = dataset
            .get_table(table_id)
            .ok_or_else(|| Error::TableNotFound(format!("Table '{}' not found", table_id)))?;

        let row_indices = table.index_lookup(&self.index_name, &index_key);

        let rows: Vec<_> = row_indices
            .into_iter()
            .filter_map(|row_idx| table.get_row(row_idx).ok())
            .collect();

        if rows.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let num_rows = rows.len();
        let num_cols = self.schema.fields().len();
        let mut columns: Vec<Column> = Vec::with_capacity(num_cols);

        for col_idx in 0..num_cols {
            let field = &self.schema.fields()[col_idx];
            let mut column = Column::new(&field.data_type, num_rows);

            for row in &rows {
                let value = match row.get(col_idx) {
                    Some(v) => v.clone(),
                    None => Value::null(),
                };
                column.push(value)?;
            }

            columns.push(column);
        }

        Ok(vec![Table::new(self.schema.clone(), columns)?])
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![]
    }

    fn statistics(&self) -> ExecutionStatistics {
        self.statistics.clone()
    }

    fn describe(&self) -> String {
        match &self.projection {
            Some(proj) => format!(
                "IndexScan: {} index={} predicate={:?} projection={:?}",
                self.table_name, self.index_name, self.predicate, proj
            ),
            None => format!(
                "IndexScan: {} index={} predicate={:?}",
                self.table_name, self.index_name, self.predicate
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::DataType;
    use yachtsql_optimizer::expr::LiteralValue;
    use yachtsql_storage::Field;

    use super::*;

    fn create_test_schema() -> Schema {
        Schema::from_fields(vec![
            Field::required("id".to_string(), DataType::Int64),
            Field::nullable("name".to_string(), DataType::String),
        ])
    }

    fn create_equality_predicate() -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "id".to_string(),
                table: None,
            }),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Int64(42))),
        }
    }

    fn create_test_storage() -> Rc<RefCell<yachtsql_storage::Storage>> {
        std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new()))
    }

    #[test]
    fn test_index_scan_creation() {
        let schema = create_test_schema();
        let predicate = create_equality_predicate();
        let storage = create_test_storage();
        let index_scan = IndexScanExec::new(
            schema.clone(),
            "users".to_string(),
            "idx_users_id".to_string(),
            predicate,
            storage,
        );

        assert_eq!(index_scan.table_name, "users");
        assert_eq!(index_scan.index_name, "idx_users_id");
        assert_eq!(index_scan.schema(), &schema);
    }

    #[test]
    fn test_index_scan_with_projection() {
        let schema = create_test_schema();
        let predicate = create_equality_predicate();
        let storage = create_test_storage();
        let projection = vec![0];

        let index_scan = IndexScanExec::new(
            schema,
            "users".to_string(),
            "idx_users_id".to_string(),
            predicate,
            storage,
        )
        .with_projection(projection.clone());

        assert_eq!(index_scan.projection, Some(projection));
    }

    #[test]
    fn test_index_scan_describe() {
        let schema = create_test_schema();
        let predicate = create_equality_predicate();
        let storage = create_test_storage();
        let index_scan = IndexScanExec::new(
            schema,
            "users".to_string(),
            "idx_users_id".to_string(),
            predicate,
            storage,
        );

        let description = index_scan.describe();
        assert!(description.contains("IndexScan"));
        assert!(description.contains("users"));
        assert!(description.contains("idx_users_id"));
    }

    #[test]
    fn test_index_scan_statistics() {
        let schema = create_test_schema();
        let predicate = create_equality_predicate();
        let storage = create_test_storage();
        let index_scan = IndexScanExec::new(
            schema,
            "users".to_string(),
            "idx_users_id".to_string(),
            predicate,
            storage,
        );

        let stats = index_scan.statistics();

        assert_eq!(stats.num_rows, Some(100));
    }

    #[test]
    fn test_index_scan_is_leaf_node() {
        let schema = create_test_schema();
        let predicate = create_equality_predicate();
        let storage = create_test_storage();
        let index_scan = IndexScanExec::new(
            schema,
            "users".to_string(),
            "idx_users_id".to_string(),
            predicate,
            storage,
        );

        assert_eq!(index_scan.children().len(), 0);
    }

    #[test]
    fn test_extract_index_key_column_eq_literal() {
        let schema = create_test_schema();
        let storage = create_test_storage();
        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "id".to_string(),
                table: None,
            }),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Int64(42))),
        };

        let index_scan = IndexScanExec::new(
            schema,
            "users".to_string(),
            "idx_users_id".to_string(),
            predicate.clone(),
            storage,
        );

        let key = index_scan.extract_index_key(&predicate).unwrap();
        assert_eq!(key, IndexKey::single(Value::int64(42)));
    }

    #[test]
    fn test_extract_index_key_literal_eq_column() {
        let schema = create_test_schema();
        let storage = create_test_storage();

        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int64(42))),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Column {
                name: "id".to_string(),
                table: None,
            }),
        };

        let index_scan = IndexScanExec::new(
            schema,
            "users".to_string(),
            "idx_users_id".to_string(),
            predicate.clone(),
            storage,
        );

        let key = index_scan.extract_index_key(&predicate).unwrap();
        assert_eq!(key, IndexKey::single(Value::int64(42)));
    }

    #[test]
    fn test_extract_index_key_string_value() {
        let schema = create_test_schema();
        let storage = create_test_storage();
        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "name".to_string(),
                table: None,
            }),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Literal(LiteralValue::String("Alice".to_string()))),
        };

        let index_scan = IndexScanExec::new(
            schema,
            "users".to_string(),
            "idx_users_name".to_string(),
            predicate.clone(),
            storage,
        );

        let key = index_scan.extract_index_key(&predicate).unwrap();
        assert_eq!(key, IndexKey::single(Value::string("Alice".to_string())));
    }
}
