use std::collections::HashMap;
use std::rc::Rc;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;
use yachtsql_storage::{Column, Field, Schema};

use super::ExecutionPlan;
use crate::Table;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PivotAggregateFunction {
    Sum,
    Avg,
    Count,
    Min,
    Max,
    First,
    Last,
}

impl PivotAggregateFunction {
    pub fn from_name(name: &str) -> Result<Self> {
        match name.to_uppercase().as_str() {
            "SUM" => Ok(Self::Sum),
            "AVG" => Ok(Self::Avg),
            "COUNT" => Ok(Self::Count),
            "MIN" => Ok(Self::Min),
            "MAX" => Ok(Self::Max),
            "FIRST" | "FIRST_VALUE" => Ok(Self::First),
            "LAST" | "LAST_VALUE" => Ok(Self::Last),
            _ => Err(Error::unsupported_feature(format!(
                "Unsupported PIVOT aggregate function: {}",
                name
            ))),
        }
    }

    fn aggregate(&self, values: &[Value]) -> Value {
        if values.is_empty() {
            return Value::null();
        }

        match self {
            Self::Count => Value::int64(values.len() as i64),
            Self::Sum => {
                let mut sum = 0.0;
                for val in values {
                    if let Some(i) = val.as_i64() {
                        sum += i as f64;
                    } else if let Some(f) = val.as_f64() {
                        sum += f;
                    }
                }
                Value::float64(sum)
            }
            Self::Avg => {
                let mut sum = 0.0;
                let mut count = 0;
                for val in values {
                    if let Some(i) = val.as_i64() {
                        sum += i as f64;
                        count += 1;
                    } else if let Some(f) = val.as_f64() {
                        sum += f;
                        count += 1;
                    }
                }
                if count > 0 {
                    Value::float64(sum / count as f64)
                } else {
                    Value::null()
                }
            }
            Self::Min => {
                let mut min: Option<Value> = None;
                for val in values {
                    if val.is_null() {
                        continue;
                    }
                    min = Some(match &min {
                        None => val.clone(),
                        Some(current) => {
                            if Self::compare_values(val, current) == std::cmp::Ordering::Less {
                                val.clone()
                            } else {
                                current.clone()
                            }
                        }
                    });
                }
                min.unwrap_or(Value::null())
            }
            Self::Max => {
                let mut max: Option<Value> = None;
                for val in values {
                    if val.is_null() {
                        continue;
                    }
                    max = Some(match &max {
                        None => val.clone(),
                        Some(current) => {
                            if Self::compare_values(val, current) == std::cmp::Ordering::Greater {
                                val.clone()
                            } else {
                                current.clone()
                            }
                        }
                    });
                }
                max.unwrap_or(Value::null())
            }
            Self::First => values.first().cloned().unwrap_or(Value::null()),
            Self::Last => values.last().cloned().unwrap_or(Value::null()),
        }
    }

    fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
            return x.cmp(&y);
        }
        if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
            return x.partial_cmp(&y).unwrap_or(Ordering::Equal);
        }
        if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
            return x.cmp(y);
        }
        Ordering::Equal
    }
}

#[derive(Debug)]
pub struct PivotExec {
    input: Rc<dyn ExecutionPlan>,

    schema: Schema,

    aggregate_expr: Expr,

    aggregate_fn: PivotAggregateFunction,

    pivot_column: String,

    pivot_values: Vec<Value>,

    group_by_columns: Vec<String>,
}

impl PivotExec {
    pub fn new(
        input: Rc<dyn ExecutionPlan>,
        aggregate_expr: Expr,
        aggregate_fn: PivotAggregateFunction,
        pivot_column: String,
        pivot_values: Vec<Value>,
        group_by_columns: Vec<String>,
    ) -> Result<Self> {
        let input_schema = input.schema();

        let mut fields = Vec::new();

        for col_name in &group_by_columns {
            let field = input_schema
                .fields()
                .iter()
                .find(|f| &f.name == col_name)
                .ok_or_else(|| Error::column_not_found(col_name.clone()))?;
            fields.push(field.clone());
        }

        for pivot_val in &pivot_values {
            let col_name = format!("{:?}", pivot_val).trim_matches('"').to_string();
            fields.push(Field::nullable(
                col_name,
                yachtsql_core::types::DataType::Float64,
            ));
        }

        let schema = Schema::from_fields(fields);

        Ok(Self {
            input,
            schema,
            aggregate_expr,
            aggregate_fn,
            pivot_column,
            pivot_values,
            group_by_columns,
        })
    }

    fn evaluate_expr(&self, expr: &Expr, batch: &Table, row_idx: usize) -> Result<Value> {
        match expr {
            Expr::Column { name, .. } => {
                let col_idx = batch
                    .schema()
                    .fields()
                    .iter()
                    .position(|f| &f.name == name)
                    .ok_or_else(|| Error::column_not_found(name.clone()))?;
                batch.expect_columns()[col_idx].get(row_idx)
            }
            Expr::Literal(lit) => Ok(lit.to_value()),
            _ => Err(Error::unsupported_feature(
                "Complex expressions not yet supported in PIVOT".to_string(),
            )),
        }
    }

    fn compute_group_key(&self, batch: &Table, row_idx: usize) -> Result<Vec<Value>> {
        let mut key = Vec::new();
        for col_name in &self.group_by_columns {
            let col_idx = batch
                .schema()
                .fields()
                .iter()
                .position(|f| &f.name == col_name)
                .ok_or_else(|| Error::column_not_found(col_name.clone()))?;
            key.push(batch.expect_columns()[col_idx].get(row_idx)?);
        }
        Ok(key)
    }
}

impl ExecutionPlan for PivotExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let input_batches = self.input.execute()?;

        if input_batches.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let merged_batch = if input_batches.len() == 1 {
            input_batches
                .into_iter()
                .next()
                .ok_or_else(|| Error::internal("Expected single batch but found none"))?
        } else {
            Table::concat(&input_batches)?
        };

        if merged_batch.num_rows() == 0 {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let pivot_col_idx = merged_batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name == self.pivot_column)
            .ok_or_else(|| Error::column_not_found(self.pivot_column.clone()))?;

        let mut groups: HashMap<Vec<u8>, HashMap<String, Vec<Value>>> = HashMap::new();
        let mut group_keys_order: Vec<Vec<Value>> = Vec::new();

        for row_idx in 0..merged_batch.num_rows() {
            let group_key = self.compute_group_key(&merged_batch, row_idx)?;
            let group_key_bytes = serde_json::to_vec(&group_key).unwrap_or_default();

            let pivot_value = merged_batch.expect_columns()[pivot_col_idx].get(row_idx)?;
            let pivot_value_str = format!("{:?}", pivot_value);

            let agg_value = self.evaluate_expr(&self.aggregate_expr, &merged_batch, row_idx)?;

            let group_entry = groups.entry(group_key_bytes.clone()).or_insert_with(|| {
                group_keys_order.push(group_key.clone());
                HashMap::new()
            });

            group_entry
                .entry(pivot_value_str)
                .or_insert_with(Vec::new)
                .push(agg_value);
        }

        let num_output_rows = groups.len();
        let mut output_columns = Vec::new();

        for (col_idx, _col_name) in self.group_by_columns.iter().enumerate() {
            let field = self.schema.fields()[col_idx].clone();
            let mut column = Column::new(&field.data_type, num_output_rows);

            for group_key in &group_keys_order {
                column.push(group_key[col_idx].clone())?;
            }

            output_columns.push(column);
        }

        for (pivot_idx, pivot_value) in self.pivot_values.iter().enumerate() {
            let field_idx = self.group_by_columns.len() + pivot_idx;
            let field = &self.schema.fields()[field_idx];
            let mut column = Column::new(&field.data_type, num_output_rows);

            let pivot_value_str = format!("{:?}", pivot_value);

            for group_key in &group_keys_order {
                let group_key_bytes = serde_json::to_vec(group_key).unwrap_or_default();

                let value = groups
                    .get(&group_key_bytes)
                    .and_then(|pivot_map| pivot_map.get(&pivot_value_str))
                    .map(|values| self.aggregate_fn.aggregate(values))
                    .unwrap_or(Value::null());

                column.push(value)?;
            }

            output_columns.push(column);
        }

        Ok(vec![Table::new(self.schema.clone(), output_columns)?])
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn describe(&self) -> String {
        format!(
            "Pivot [pivot_col: {}, values: {}, agg: {:?}]",
            self.pivot_column,
            self.pivot_values.len(),
            self.aggregate_fn
        )
    }
}

#[derive(Debug)]
pub struct UnpivotExec {
    input: Rc<dyn ExecutionPlan>,

    schema: Schema,

    value_column_name: String,

    name_column_name: String,

    unpivot_columns: Vec<String>,

    passthrough_columns: Vec<String>,
}

impl UnpivotExec {
    pub fn new(
        input: Rc<dyn ExecutionPlan>,
        value_column_name: String,
        name_column_name: String,
        unpivot_columns: Vec<String>,
    ) -> Result<Self> {
        let input_schema = input.schema();

        let passthrough_columns: Vec<String> = input_schema
            .fields()
            .iter()
            .filter(|f| !unpivot_columns.contains(&f.name))
            .map(|f| f.name.clone())
            .collect();

        let mut fields = Vec::new();

        for col_name in &passthrough_columns {
            let field = input_schema
                .fields()
                .iter()
                .find(|f| &f.name == col_name)
                .ok_or_else(|| Error::column_not_found(col_name.clone()))?;
            fields.push(field.clone());
        }

        fields.push(Field::nullable(
            name_column_name.clone(),
            yachtsql_core::types::DataType::String,
        ));

        fields.push(Field::nullable(
            value_column_name.clone(),
            yachtsql_core::types::DataType::Int64,
        ));

        let schema = Schema::from_fields(fields);

        Ok(Self {
            input,
            schema,
            value_column_name,
            name_column_name,
            unpivot_columns,
            passthrough_columns,
        })
    }
}

impl ExecutionPlan for UnpivotExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let input_batches = self.input.execute()?;

        if input_batches.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let merged_batch = if input_batches.len() == 1 {
            input_batches
                .into_iter()
                .next()
                .ok_or_else(|| Error::internal("Expected single batch but found none"))?
        } else {
            Table::concat(&input_batches)?
        };

        if merged_batch.num_rows() == 0 {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let input_num_rows = merged_batch.num_rows();
        let num_unpivot_cols = self.unpivot_columns.len();
        let output_num_rows = input_num_rows * num_unpivot_cols;

        let passthrough_indices: Vec<usize> = self
            .passthrough_columns
            .iter()
            .map(|name| {
                merged_batch
                    .schema()
                    .fields()
                    .iter()
                    .position(|f| &f.name == name)
                    .ok_or_else(|| Error::column_not_found(name.clone()))
            })
            .collect::<Result<Vec<_>>>()?;

        let unpivot_indices: Vec<usize> = self
            .unpivot_columns
            .iter()
            .map(|name| {
                merged_batch
                    .schema()
                    .fields()
                    .iter()
                    .position(|f| &f.name == name)
                    .ok_or_else(|| Error::column_not_found(name.clone()))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut output_columns = Vec::new();

        for &col_idx in &passthrough_indices {
            let input_col = &merged_batch.expect_columns()[col_idx];
            let field = &merged_batch.schema().fields()[col_idx];
            let mut output_col = Column::new(&field.data_type, output_num_rows);

            for row_idx in 0..input_num_rows {
                let value = input_col.get(row_idx)?;

                for _ in 0..num_unpivot_cols {
                    output_col.push(value.clone())?;
                }
            }

            output_columns.push(output_col);
        }

        let mut name_column = Column::new(&yachtsql_core::types::DataType::String, output_num_rows);
        for _ in 0..input_num_rows {
            for col_name in &self.unpivot_columns {
                name_column.push(Value::string(col_name.clone()))?;
            }
        }
        output_columns.push(name_column);

        let mut value_column = Column::new(&yachtsql_core::types::DataType::Int64, output_num_rows);
        for row_idx in 0..input_num_rows {
            for &col_idx in &unpivot_indices {
                let value = merged_batch.expect_columns()[col_idx].get(row_idx)?;
                value_column.push(value)?;
            }
        }
        output_columns.push(value_column);

        Ok(vec![Table::new(self.schema.clone(), output_columns)?])
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn describe(&self) -> String {
        format!(
            "Unpivot [columns: {}, value_col: {}, name_col: {}]",
            self.unpivot_columns.len(),
            self.value_column_name,
            self.name_column_name
        )
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::DataType;
    use yachtsql_optimizer::expr::LiteralValue;

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

    #[test]
    fn test_pivot_basic() {
        let schema = Schema::from_fields(vec![
            Field::required("region".to_string(), DataType::String),
            Field::required("quarter".to_string(), DataType::String),
            Field::required("sales".to_string(), DataType::Int64),
        ]);

        let batch = Table::from_values(
            schema.clone(),
            vec![
                vec![
                    Value::string("North".to_string()),
                    Value::string("Q1".to_string()),
                    Value::int64(100),
                ],
                vec![
                    Value::string("North".to_string()),
                    Value::string("Q2".to_string()),
                    Value::int64(150),
                ],
                vec![
                    Value::string("South".to_string()),
                    Value::string("Q1".to_string()),
                    Value::int64(200),
                ],
            ],
        )
        .unwrap();

        let input = Rc::new(MockDataExec::new(schema, vec![batch]));

        let pivot = PivotExec::new(
            input,
            Expr::Column {
                name: "sales".to_string(),
                table: None,
            },
            PivotAggregateFunction::Sum,
            "quarter".to_string(),
            vec![
                Value::string("Q1".to_string()),
                Value::string("Q2".to_string()),
            ],
            vec!["region".to_string()],
        )
        .unwrap();

        let result = pivot.execute().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
        assert_eq!(result[0].schema().fields().len(), 3);
    }

    #[test]
    fn test_unpivot_basic() {
        let schema = Schema::from_fields(vec![
            Field::required("region".to_string(), DataType::String),
            Field::required("Q1".to_string(), DataType::Int64),
            Field::required("Q2".to_string(), DataType::Int64),
            Field::required("Q3".to_string(), DataType::Int64),
        ]);

        let batch = Table::from_values(
            schema.clone(),
            vec![
                vec![
                    Value::string("North".to_string()),
                    Value::int64(100),
                    Value::int64(150),
                    Value::int64(200),
                ],
                vec![
                    Value::string("South".to_string()),
                    Value::int64(250),
                    Value::int64(300),
                    Value::int64(350),
                ],
            ],
        )
        .unwrap();

        let input = Rc::new(MockDataExec::new(schema, vec![batch]));

        let unpivot = UnpivotExec::new(
            input,
            "sales".to_string(),
            "quarter".to_string(),
            vec!["Q1".to_string(), "Q2".to_string(), "Q3".to_string()],
        )
        .unwrap();

        let result = unpivot.execute().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 6);
        assert_eq!(result[0].schema().fields().len(), 3);
    }

    #[test]
    fn test_pivot_aggregate_functions() {
        assert_eq!(
            PivotAggregateFunction::from_name("SUM").unwrap(),
            PivotAggregateFunction::Sum
        );
        assert_eq!(
            PivotAggregateFunction::from_name("count").unwrap(),
            PivotAggregateFunction::Count
        );
        assert!(PivotAggregateFunction::from_name("INVALID").is_err());
    }

    #[test]
    fn test_pivot_aggregate_sum() {
        let values = vec![Value::int64(10), Value::int64(20), Value::int64(30)];
        let result = PivotAggregateFunction::Sum.aggregate(&values);
        assert_eq!(result, Value::float64(60.0));
    }

    #[test]
    fn test_pivot_aggregate_avg() {
        let values = vec![Value::int64(10), Value::int64(20), Value::int64(30)];
        let result = PivotAggregateFunction::Avg.aggregate(&values);
        assert_eq!(result, Value::float64(20.0));
    }

    #[test]
    fn test_pivot_aggregate_count() {
        let values = vec![Value::int64(10), Value::null(), Value::int64(30)];
        let result = PivotAggregateFunction::Count.aggregate(&values);
        assert_eq!(result, Value::int64(3));
    }
}
