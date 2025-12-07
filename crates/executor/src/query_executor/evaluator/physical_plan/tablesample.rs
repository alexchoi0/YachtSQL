use std::rc::Rc;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use yachtsql_core::error::{Error, Result};
use yachtsql_storage::Schema;

use super::ExecutionPlan;
use crate::Table;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SamplingMethod {
    Bernoulli,

    System,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SampleSize {
    Percent(f64),

    Rows(usize),
}

#[derive(Debug)]
pub struct TableSampleExec {
    input: Rc<dyn ExecutionPlan>,

    schema: Schema,

    method: SamplingMethod,

    size: SampleSize,

    seed: Option<u64>,
}

impl TableSampleExec {
    pub fn new(
        input: Rc<dyn ExecutionPlan>,
        method: SamplingMethod,
        size: SampleSize,
        seed: Option<u64>,
    ) -> Result<Self> {
        match &size {
            SampleSize::Percent(p) => {
                if *p < 0.0 || *p > 100.0 {
                    return Err(Error::invalid_query(format!(
                        "TABLESAMPLE percentage must be between 0 and 100, got: {}",
                        p
                    )));
                }
            }
            SampleSize::Rows(r) => {
                if *r == 0 {
                    return Err(Error::invalid_query(
                        "TABLESAMPLE row count must be greater than 0".to_string(),
                    ));
                }
            }
        }

        let schema = input.schema().clone();

        Ok(Self {
            input,
            schema,
            method,
            size,
            seed,
        })
    }

    pub fn bernoulli_percent(
        input: Rc<dyn ExecutionPlan>,
        percent: f64,
        seed: Option<u64>,
    ) -> Result<Self> {
        Self::new(
            input,
            SamplingMethod::Bernoulli,
            SampleSize::Percent(percent),
            seed,
        )
    }

    pub fn system_percent(
        input: Rc<dyn ExecutionPlan>,
        percent: f64,
        seed: Option<u64>,
    ) -> Result<Self> {
        Self::new(
            input,
            SamplingMethod::System,
            SampleSize::Percent(percent),
            seed,
        )
    }

    pub fn bernoulli_rows(
        input: Rc<dyn ExecutionPlan>,
        rows: usize,
        seed: Option<u64>,
    ) -> Result<Self> {
        Self::new(
            input,
            SamplingMethod::Bernoulli,
            SampleSize::Rows(rows),
            seed,
        )
    }

    fn sample_bernoulli(
        &self,
        batches: Vec<Table>,
        probability: f64,
        rng: &mut StdRng,
    ) -> Result<Vec<Table>> {
        use yachtsql_storage::Column;

        let mut result_batches = Vec::new();

        for batch in batches {
            if batch.is_empty() {
                continue;
            }

            let num_rows = batch.num_rows();
            let mut selected_indices = Vec::new();

            for row_idx in 0..num_rows {
                if rng.r#gen::<f64>() < probability {
                    selected_indices.push(row_idx);
                }
            }

            if selected_indices.is_empty() {
                continue;
            }

            let sampled_columns: Vec<Column> = batch
                .expect_columns()
                .iter()
                .map(|col| col.gather(&selected_indices))
                .collect::<Result<Vec<_>>>()?;

            result_batches.push(Table::new(batch.schema().clone(), sampled_columns)?);
        }

        if result_batches.is_empty() {
            Ok(vec![Table::empty(self.schema.clone())])
        } else {
            Ok(result_batches)
        }
    }

    fn sample_system(
        &self,
        batches: Vec<Table>,
        probability: f64,
        rng: &mut StdRng,
    ) -> Result<Vec<Table>> {
        let mut result_batches = Vec::new();

        for batch in batches {
            if batch.is_empty() {
                continue;
            }

            if rng.r#gen::<f64>() < probability {
                result_batches.push(batch);
            }
        }

        if result_batches.is_empty() {
            Ok(vec![Table::empty(self.schema.clone())])
        } else {
            Ok(result_batches)
        }
    }

    fn sample_n_rows(&self, batches: Vec<Table>, n: usize, rng: &mut StdRng) -> Result<Vec<Table>> {
        use yachtsql_storage::Column;

        let merged_batch = if batches.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        } else if batches.len() == 1 {
            batches
                .into_iter()
                .next()
                .ok_or_else(|| Error::internal("Expected single batch but found none"))?
        } else {
            Table::concat(&batches)?
        };

        let total_rows = merged_batch.num_rows();
        if total_rows == 0 {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let sample_count = n.min(total_rows);

        let mut selected_indices: Vec<usize> = (0..sample_count).collect();

        for i in sample_count..total_rows {
            let j = rng.gen_range(0..=i);
            if j < sample_count {
                selected_indices[j] = i;
            }
        }

        selected_indices.sort_unstable();

        let sampled_columns: Vec<Column> = merged_batch
            .expect_columns()
            .iter()
            .map(|col| col.gather(&selected_indices))
            .collect::<Result<Vec<_>>>()?;

        Ok(vec![Table::new(
            merged_batch.schema().clone(),
            sampled_columns,
        )?])
    }
}

impl ExecutionPlan for TableSampleExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let input_batches = self.input.execute()?;

        if input_batches.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let mut rng = match self.seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::from_entropy(),
        };

        match &self.size {
            SampleSize::Percent(percent) => {
                let probability = percent / 100.0;

                match self.method {
                    SamplingMethod::Bernoulli => {
                        self.sample_bernoulli(input_batches, probability, &mut rng)
                    }
                    SamplingMethod::System => {
                        self.sample_system(input_batches, probability, &mut rng)
                    }
                }
            }
            SampleSize::Rows(n) => self.sample_n_rows(input_batches, *n, &mut rng),
        }
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn describe(&self) -> String {
        let method_str = match self.method {
            SamplingMethod::Bernoulli => "BERNOULLI",
            SamplingMethod::System => "SYSTEM",
        };

        let size_str = match &self.size {
            SampleSize::Percent(p) => format!("{:.1}%", p),
            SampleSize::Rows(r) => format!("{} ROWS", r),
        };

        let seed_str = match self.seed {
            Some(s) => format!(" REPEATABLE ({})", s),
            None => String::new(),
        };

        format!("TableSample: {} ({}){}", method_str, size_str, seed_str)
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
    fn test_bernoulli_percent_with_seed() {
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        let values: Vec<i64> = (1..=100).collect();
        let batch = create_test_batch(schema.clone(), values);
        let input = Rc::new(MockDataExec::new(schema.clone(), vec![batch]));

        let sample_exec =
            TableSampleExec::bernoulli_percent(input.clone(), 10.0, Some(42)).unwrap();

        let result1 = sample_exec.execute().unwrap();
        let result2 = sample_exec.execute().unwrap();

        let count1: usize = result1.iter().map(|b| b.num_rows()).sum();
        let count2: usize = result2.iter().map(|b| b.num_rows()).sum();

        assert_eq!(count1, count2);
        assert!(count1 > 0);
        assert!(count1 < 100);
    }

    #[test]
    fn test_system_sampling() {
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        let batch = create_test_batch(schema.clone(), vec![1, 2, 3, 4, 5]);
        let input = Rc::new(MockDataExec::new(schema.clone(), vec![batch]));

        let sample_exec = TableSampleExec::system_percent(input, 50.0, Some(42)).unwrap();

        let result = sample_exec.execute().unwrap();
        assert!(!result.is_empty());
    }

    #[test]
    fn test_sample_n_rows() {
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        let values: Vec<i64> = (1..=100).collect();
        let batch = create_test_batch(schema.clone(), values);
        let input = Rc::new(MockDataExec::new(schema.clone(), vec![batch]));

        let sample_exec = TableSampleExec::bernoulli_rows(input, 10, Some(42)).unwrap();

        let result = sample_exec.execute().unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();

        assert_eq!(total_rows, 10);
    }

    #[test]
    fn test_sample_more_than_available() {
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        let batch = create_test_batch(schema.clone(), vec![1, 2, 3, 4, 5]);
        let input = Rc::new(MockDataExec::new(schema.clone(), vec![batch]));

        let sample_exec = TableSampleExec::bernoulli_rows(input, 100, Some(42)).unwrap();

        let result = sample_exec.execute().unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();

        assert_eq!(total_rows, 5);
    }

    #[test]
    fn test_invalid_percentage() {
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        let input = Rc::new(TableScanExec::new(
            schema,
            "test".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let result = TableSampleExec::bernoulli_percent(input.clone(), 150.0, None);
        assert!(result.is_err());

        let result = TableSampleExec::bernoulli_percent(input, -10.0, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_zero_rows_invalid() {
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        let input = Rc::new(TableScanExec::new(
            schema,
            "test".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let result = TableSampleExec::bernoulli_rows(input, 0, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_input() {
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        let input = Rc::new(MockDataExec::new(schema.clone(), vec![]));

        let sample_exec = TableSampleExec::bernoulli_percent(input, 50.0, None).unwrap();

        let result = sample_exec.execute().unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].is_empty());
    }

    #[test]
    fn test_describe() {
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        let input = Rc::new(TableScanExec::new(
            schema.clone(),
            "test".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let sample1 = TableSampleExec::bernoulli_percent(input.clone(), 10.0, None).unwrap();
        assert_eq!(sample1.describe(), "TableSample: BERNOULLI (10.0%)");

        let sample2 = TableSampleExec::system_percent(input.clone(), 25.0, Some(42)).unwrap();
        assert_eq!(
            sample2.describe(),
            "TableSample: SYSTEM (25.0%) REPEATABLE (42)"
        );

        let sample3 = TableSampleExec::bernoulli_rows(input, 100, None).unwrap();
        assert_eq!(sample3.describe(), "TableSample: BERNOULLI (100 ROWS)");
    }

    #[test]
    fn test_deterministic_with_seed() {
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        let values: Vec<i64> = (1..=100).collect();
        let batch = create_test_batch(schema.clone(), values);
        let input = Rc::new(MockDataExec::new(schema.clone(), vec![batch]));

        let sample_exec = TableSampleExec::bernoulli_percent(input, 20.0, Some(12345)).unwrap();

        let result1 = sample_exec.execute().unwrap();
        let result2 = sample_exec.execute().unwrap();

        let rows1: Vec<i64> = result1
            .iter()
            .flat_map(|b| {
                (0..b.num_rows())
                    .map(|i| {
                        b.expect_columns()[0]
                            .get(i)
                            .unwrap()
                            .as_i64()
                            .expect("expected int64")
                    })
                    .collect::<Vec<_>>()
            })
            .collect();

        let rows2: Vec<i64> = result2
            .iter()
            .flat_map(|b| {
                (0..b.num_rows())
                    .map(|i| {
                        b.expect_columns()[0]
                            .get(i)
                            .unwrap()
                            .as_i64()
                            .expect("expected int64")
                    })
                    .collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(rows1, rows2);
    }
}
