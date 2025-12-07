use std::rc::Rc;

use yachtsql_core::error::Result;
use yachtsql_storage::Schema;

use super::ExecutionPlan;
use crate::Table;

#[derive(Debug)]
pub struct LimitExec {
    input: Rc<dyn ExecutionPlan>,
    schema: Schema,
    offset: usize,
    limit: usize,
}

impl LimitExec {
    pub fn new(input: Rc<dyn ExecutionPlan>, limit: usize, offset: usize) -> Self {
        let schema = input.schema().clone();
        Self {
            input,
            schema,
            offset,
            limit,
        }
    }

    pub fn limit_only(input: Rc<dyn ExecutionPlan>, limit: usize) -> Self {
        Self::new(input, limit, 0)
    }

    pub fn offset_only(input: Rc<dyn ExecutionPlan>, offset: usize) -> Self {
        Self::new(input, usize::MAX, offset)
    }
}

impl ExecutionPlan for LimitExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let input_batches = self.input.execute()?;
        let mut output_batches = Vec::new();
        let mut skipped = 0;
        let mut remaining = self.limit;

        for batch in input_batches {
            let batch_rows = batch.num_rows();

            if skipped < self.offset {
                let to_skip = (self.offset - skipped).min(batch_rows);
                skipped += to_skip;

                if to_skip == batch_rows {
                    continue;
                } else {
                    let remaining_batch = batch.slice(to_skip, batch_rows - to_skip)?;
                    let remaining_rows = remaining_batch.num_rows();

                    if remaining_rows <= remaining {
                        remaining -= remaining_rows;
                        output_batches.push(remaining_batch);
                    } else {
                        output_batches.push(remaining_batch.slice(0, remaining)?);
                        break;
                    }
                }
            } else {
                if remaining == 0 {
                    break;
                }

                if batch_rows <= remaining {
                    remaining -= batch_rows;
                    output_batches.push(batch);
                } else {
                    output_batches.push(batch.slice(0, remaining)?);
                    break;
                }
            }
        }

        Ok(output_batches)
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn describe(&self) -> String {
        if self.offset > 0 && self.limit != usize::MAX {
            format!("Limit: {} Offset: {}", self.limit, self.offset)
        } else if self.offset > 0 {
            format!("Offset: {}", self.offset)
        } else {
            format!("Limit: {}", self.limit)
        }
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_storage::{Column, Field};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::TableScanExec;

    #[derive(Debug)]
    struct MockDataExec {
        schema: Schema,
        data: Vec<Table>,
    }

    impl MockDataExec {
        fn new(schema: Schema, data: Vec<Table>) -> Self {
            Self { schema, data }
        }
    }

    impl ExecutionPlan for MockDataExec {
        fn schema(&self) -> &Schema {
            &self.schema
        }

        fn execute(&self) -> Result<Vec<Table>> {
            Ok(self.data.clone())
        }

        fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
            vec![]
        }

        fn describe(&self) -> String {
            "MockData".to_string()
        }
    }

    fn create_test_batch(schema: Schema, values: Vec<i64>) -> Table {
        let num_rows = values.len();
        let mut column = Column::new(&DataType::Int64, num_rows);

        for &value in &values {
            column.push(Value::int64(value)).unwrap();
        }

        Table::new(schema, vec![column]).unwrap()
    }

    #[test]
    fn test_limit_only() {
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        let batch = create_test_batch(schema.clone(), vec![1, 2, 3, 4, 5]);
        let input = Rc::new(MockDataExec::new(schema.clone(), vec![batch]));

        let limit_exec = LimitExec::new(input, 3, 0);
        let result = limit_exec.execute().unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_offset_only() {
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        let batch = create_test_batch(schema.clone(), vec![1, 2, 3, 4, 5]);
        let input = Rc::new(MockDataExec::new(schema.clone(), vec![batch]));

        let limit_exec = LimitExec::new(input, usize::MAX, 2);
        let result = limit_exec.execute().unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_limit_and_offset() {
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        let batch = create_test_batch(schema.clone(), vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let input = Rc::new(MockDataExec::new(schema.clone(), vec![batch]));

        let limit_exec = LimitExec::new(input, 3, 5);
        let result = limit_exec.execute().unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_limit_larger_than_data() {
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        let batch = create_test_batch(schema.clone(), vec![1, 2, 3]);
        let input = Rc::new(MockDataExec::new(schema.clone(), vec![batch]));

        let limit_exec = LimitExec::new(input, 100, 0);
        let result = limit_exec.execute().unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_offset_larger_than_data() {
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        let batch = create_test_batch(schema.clone(), vec![1, 2, 3]);
        let input = Rc::new(MockDataExec::new(schema.clone(), vec![batch]));

        let limit_exec = LimitExec::new(input, 10, 100);
        let result = limit_exec.execute().unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    #[test]
    fn test_zero_limit() {
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        let batch = create_test_batch(schema.clone(), vec![1, 2, 3]);
        let input = Rc::new(MockDataExec::new(schema.clone(), vec![batch]));

        let limit_exec = LimitExec::new(input, 0, 0);
        let result = limit_exec.execute().unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    #[test]
    fn test_multiple_batches() {
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        let batch1 = create_test_batch(schema.clone(), vec![1, 2, 3]);
        let batch2 = create_test_batch(schema.clone(), vec![4, 5, 6]);
        let input = Rc::new(MockDataExec::new(schema.clone(), vec![batch1, batch2]));

        let limit_exec = LimitExec::new(input, 3, 2);
        let result = limit_exec.execute().unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_describe() {
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);
        let input = Rc::new(TableScanExec::new(
            schema.clone(),
            "test".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let limit_only = LimitExec::new(input.clone(), 10, 0);
        assert_eq!(limit_only.describe(), "Limit: 10");

        let offset_only = LimitExec::new(input.clone(), usize::MAX, 5);
        assert_eq!(offset_only.describe(), "Offset: 5");

        let both = LimitExec::new(input, 10, 5);
        assert_eq!(both.describe(), "Limit: 10 Offset: 5");
    }
}
