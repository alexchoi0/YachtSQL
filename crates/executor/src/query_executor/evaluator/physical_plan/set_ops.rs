use std::collections::HashSet;
use std::rc::Rc;

use yachtsql_core::error::{Error, Result};
use yachtsql_storage::{Column, Schema};

use super::ExecutionPlan;
use crate::Table;

#[derive(Debug)]
pub struct UnionExec {
    left: Rc<dyn ExecutionPlan>,
    right: Rc<dyn ExecutionPlan>,
    schema: Schema,
    all: bool,
}

impl UnionExec {
    pub fn new(
        left: Rc<dyn ExecutionPlan>,
        right: Rc<dyn ExecutionPlan>,
        all: bool,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        if left_schema.fields().len() != right_schema.fields().len() {
            return Err(Error::InternalError(format!(
                "UNION schemas have different number of columns: {} vs {}",
                left_schema.fields().len(),
                right_schema.fields().len()
            )));
        }

        let mut merged_fields = Vec::with_capacity(left_schema.fields().len());
        for (i, (left_field, right_field)) in left_schema
            .fields()
            .iter()
            .zip(right_schema.fields().iter())
            .enumerate()
        {
            if !Self::types_compatible(&left_field.data_type, &right_field.data_type) {
                return Err(Error::InvalidQuery(format!(
                    "UNION column {} type mismatch: {:?} vs {:?}",
                    i + 1,
                    left_field.data_type,
                    right_field.data_type
                )));
            }
            let merged_type =
                Self::pick_more_specific_type(&left_field.data_type, &right_field.data_type);
            let mut merged_field = left_field.clone();
            merged_field.data_type = merged_type;
            merged_fields.push(merged_field);
        }

        let schema = Schema::from_fields(merged_fields);

        Ok(Self {
            left,
            right,
            schema,
            all,
        })
    }

    fn types_compatible(
        left: &yachtsql_core::types::DataType,
        right: &yachtsql_core::types::DataType,
    ) -> bool {
        use yachtsql_core::types::DataType;

        if left == right {
            return true;
        }

        if matches!(left, DataType::Unknown) || matches!(right, DataType::Unknown) {
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

    fn pick_more_specific_type(
        left: &yachtsql_core::types::DataType,
        right: &yachtsql_core::types::DataType,
    ) -> yachtsql_core::types::DataType {
        use yachtsql_core::types::DataType;

        if left == right {
            return left.clone();
        }

        match (left, right) {
            (DataType::Unknown, other) | (other, DataType::Unknown) => other.clone(),
            (DataType::Float64, DataType::Int64) | (DataType::Int64, DataType::Float64) => {
                DataType::Float64
            }
            (DataType::Numeric(p), _) | (_, DataType::Numeric(p)) => DataType::Numeric(*p),
            _ => left.clone(),
        }
    }
}

impl ExecutionPlan for UnionExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let left_batches = self.left.execute()?;
        let right_batches = self.right.execute()?;

        if self.all {
            let mut result = left_batches;
            result.extend(right_batches);
            Ok(result)
        } else {
            let mut all_rows = Vec::new();

            for batch in &left_batches {
                for row_idx in 0..batch.num_rows() {
                    let mut row = Vec::new();
                    for col in batch.expect_columns() {
                        row.push(col.get(row_idx)?);
                    }
                    all_rows.push(row);
                }
            }

            for batch in &right_batches {
                for row_idx in 0..batch.num_rows() {
                    let mut row = Vec::new();
                    for col in batch.expect_columns() {
                        row.push(col.get(row_idx)?);
                    }
                    all_rows.push(row);
                }
            }

            let mut seen = HashSet::new();
            let mut distinct_rows = Vec::new();

            for row in all_rows {
                let key = format!("{:?}", row);
                if seen.insert(key) {
                    distinct_rows.push(row);
                }
            }

            if distinct_rows.is_empty() {
                return Ok(vec![Table::empty(self.schema.clone())]);
            }

            let num_rows = distinct_rows.len();
            let num_cols = self.schema.fields().len();
            let mut columns = Vec::new();

            for col_idx in 0..num_cols {
                let field = &self.schema.fields()[col_idx];
                let mut column = Column::new(&field.data_type, num_rows);
                for row in &distinct_rows {
                    column.push(row[col_idx].clone())?;
                }
                columns.push(column);
            }

            Ok(vec![Table::new(self.schema.clone(), columns)?])
        }
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn describe(&self) -> String {
        if self.all {
            "UNION ALL".to_string()
        } else {
            "UNION".to_string()
        }
    }
}

#[derive(Debug)]
pub struct IntersectExec {
    left: Rc<dyn ExecutionPlan>,
    right: Rc<dyn ExecutionPlan>,
    schema: Schema,
    all: bool,
}

impl IntersectExec {
    pub fn new(
        left: Rc<dyn ExecutionPlan>,
        right: Rc<dyn ExecutionPlan>,
        all: bool,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        if left_schema.fields().len() != right_schema.fields().len() {
            return Err(Error::InternalError(format!(
                "INTERSECT schemas have different number of columns: {} vs {}",
                left_schema.fields().len(),
                right_schema.fields().len()
            )));
        }

        let schema = left_schema.clone();

        Ok(Self {
            left,
            right,
            schema,
            all,
        })
    }
}

impl ExecutionPlan for IntersectExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let left_batches = self.left.execute()?;
        let right_batches = self.right.execute()?;

        let mut left_rows = Vec::new();
        for batch in &left_batches {
            for row_idx in 0..batch.num_rows() {
                let mut row = Vec::new();
                for col in batch.expect_columns() {
                    row.push(col.get(row_idx)?);
                }
                left_rows.push(row);
            }
        }

        let mut right_set = HashSet::new();
        for batch in &right_batches {
            for row_idx in 0..batch.num_rows() {
                let mut row = Vec::new();
                for col in batch.expect_columns() {
                    row.push(col.get(row_idx)?);
                }
                let key = format!("{:?}", row);
                right_set.insert(key);
            }
        }

        let mut result_rows = Vec::new();
        if self.all {
            for row in left_rows {
                let key = format!("{:?}", row);
                if right_set.contains(&key) {
                    result_rows.push(row);
                }
            }
        } else {
            let mut seen = HashSet::new();
            for row in left_rows {
                let key = format!("{:?}", row);
                if right_set.contains(&key) && seen.insert(key) {
                    result_rows.push(row);
                }
            }
        }

        if result_rows.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let num_rows = result_rows.len();
        let num_cols = self.schema.fields().len();
        let mut columns = Vec::new();

        for col_idx in 0..num_cols {
            let field = &self.schema.fields()[col_idx];
            let mut column = Column::new(&field.data_type, num_rows);
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
        if self.all {
            "INTERSECT ALL".to_string()
        } else {
            "INTERSECT".to_string()
        }
    }
}

#[derive(Debug)]
pub struct ExceptExec {
    left: Rc<dyn ExecutionPlan>,
    right: Rc<dyn ExecutionPlan>,
    schema: Schema,
    all: bool,
}

impl ExceptExec {
    pub fn new(
        left: Rc<dyn ExecutionPlan>,
        right: Rc<dyn ExecutionPlan>,
        all: bool,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        if left_schema.fields().len() != right_schema.fields().len() {
            return Err(Error::InternalError(format!(
                "EXCEPT schemas have different number of columns: {} vs {}",
                left_schema.fields().len(),
                right_schema.fields().len()
            )));
        }

        let schema = left_schema.clone();

        Ok(Self {
            left,
            right,
            schema,
            all,
        })
    }
}

impl ExecutionPlan for ExceptExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let left_batches = self.left.execute()?;
        let right_batches = self.right.execute()?;

        let mut left_rows = Vec::new();
        for batch in &left_batches {
            for row_idx in 0..batch.num_rows() {
                let mut row = Vec::new();
                for col in batch.expect_columns() {
                    row.push(col.get(row_idx)?);
                }
                left_rows.push(row);
            }
        }

        let mut right_set = HashSet::new();
        for batch in &right_batches {
            for row_idx in 0..batch.num_rows() {
                let mut row = Vec::new();
                for col in batch.expect_columns() {
                    row.push(col.get(row_idx)?);
                }
                let key = format!("{:?}", row);
                right_set.insert(key);
            }
        }

        let mut result_rows = Vec::new();
        if self.all {
            for row in left_rows {
                let key = format!("{:?}", row);
                if !right_set.contains(&key) {
                    result_rows.push(row);
                }
            }
        } else {
            let mut seen = HashSet::new();
            for row in left_rows {
                let key = format!("{:?}", row);
                if !right_set.contains(&key) && seen.insert(key.clone()) {
                    result_rows.push(row);
                }
            }
        }

        if result_rows.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let num_rows = result_rows.len();
        let num_cols = self.schema.fields().len();
        let mut columns = Vec::new();

        for col_idx in 0..num_cols {
            let field = &self.schema.fields()[col_idx];
            let mut column = Column::new(&field.data_type, num_rows);
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
        if self.all {
            "EXCEPT ALL".to_string()
        } else {
            "EXCEPT".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_executor::evaluator::physical_plan::TableScanExec;

    #[test]
    fn test_union_all() {
        let schema =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "id".to_string(),
                yachtsql_core::types::DataType::Int64,
            )]);

        let left = Rc::new(TableScanExec::new(
            schema.clone(),
            "left".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));
        let right = Rc::new(TableScanExec::new(
            schema.clone(),
            "right".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let union = UnionExec::new(left, right, true);
        assert!(union.is_ok());

        let union_exec = union.unwrap();
        assert_eq!(union_exec.schema().fields().len(), 1);
        assert_eq!(union_exec.describe(), "UNION ALL");
    }

    #[test]
    fn test_union_distinct() {
        let schema =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "value".to_string(),
                yachtsql_core::types::DataType::String,
            )]);

        let left = Rc::new(TableScanExec::new(
            schema.clone(),
            "left".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));
        let right = Rc::new(TableScanExec::new(
            schema.clone(),
            "right".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let union = UnionExec::new(left, right, false);
        assert!(union.is_ok());

        let union_exec = union.unwrap();
        assert_eq!(union_exec.describe(), "UNION");
    }

    #[test]
    fn test_intersect_all() {
        let schema =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "id".to_string(),
                yachtsql_core::types::DataType::Int64,
            )]);

        let left = Rc::new(TableScanExec::new(
            schema.clone(),
            "left".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));
        let right = Rc::new(TableScanExec::new(
            schema.clone(),
            "right".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let intersect = IntersectExec::new(left, right, true);
        assert!(intersect.is_ok());

        let intersect_exec = intersect.unwrap();
        assert_eq!(intersect_exec.describe(), "INTERSECT ALL");
    }

    #[test]
    fn test_intersect_distinct() {
        let schema =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "value".to_string(),
                yachtsql_core::types::DataType::String,
            )]);

        let left = Rc::new(TableScanExec::new(
            schema.clone(),
            "left".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));
        let right = Rc::new(TableScanExec::new(
            schema.clone(),
            "right".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let intersect = IntersectExec::new(left, right, false);
        assert!(intersect.is_ok());

        let intersect_exec = intersect.unwrap();
        assert_eq!(intersect_exec.describe(), "INTERSECT");
    }

    #[test]
    fn test_except_all() {
        let schema =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "id".to_string(),
                yachtsql_core::types::DataType::Int64,
            )]);

        let left = Rc::new(TableScanExec::new(
            schema.clone(),
            "left".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));
        let right = Rc::new(TableScanExec::new(
            schema.clone(),
            "right".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let except = ExceptExec::new(left, right, true);
        assert!(except.is_ok());

        let except_exec = except.unwrap();
        assert_eq!(except_exec.describe(), "EXCEPT ALL");
    }

    #[test]
    fn test_except_distinct() {
        let schema =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "value".to_string(),
                yachtsql_core::types::DataType::String,
            )]);

        let left = Rc::new(TableScanExec::new(
            schema.clone(),
            "left".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));
        let right = Rc::new(TableScanExec::new(
            schema.clone(),
            "right".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let except = ExceptExec::new(left, right, false);
        assert!(except.is_ok());

        let except_exec = except.unwrap();
        assert_eq!(except_exec.describe(), "EXCEPT");
    }

    #[test]
    fn test_schema_mismatch() {
        let schema1 =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "id".to_string(),
                yachtsql_core::types::DataType::Int64,
            )]);

        let schema2 = yachtsql_storage::Schema::from_fields(vec![
            yachtsql_storage::Field::required(
                "id".to_string(),
                yachtsql_core::types::DataType::Int64,
            ),
            yachtsql_storage::Field::required(
                "name".to_string(),
                yachtsql_core::types::DataType::String,
            ),
        ]);

        let left = Rc::new(TableScanExec::new(
            schema1,
            "left".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));
        let right = Rc::new(TableScanExec::new(
            schema2,
            "right".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let union = UnionExec::new(left.clone(), right.clone(), true);
        assert!(union.is_err());

        let intersect = IntersectExec::new(left.clone(), right.clone(), true);
        assert!(intersect.is_err());

        let except = ExceptExec::new(left, right, true);
        assert!(except.is_err());
    }
}
