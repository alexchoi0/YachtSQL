use std::collections::HashMap;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};

#[derive(Debug, Clone)]
pub enum PTFArgument {
    Scalar(Value),

    Table {
        schema: Vec<(String, DataType)>,
        rows: Vec<Vec<Value>>,
    },

    Descriptor {
        columns: Vec<String>,
        types: Vec<DataType>,
    },
}

#[derive(Debug, Clone)]
pub struct PTFSchema {
    pub columns: Vec<String>,

    pub types: Vec<DataType>,
}

pub trait PolymorphicTableFunction: Send + Sync {
    fn name(&self) -> &str;

    fn infer_schema(&self, args: &[PTFArgument]) -> Result<PTFSchema>;

    fn execute(&self, args: &[PTFArgument]) -> Result<Vec<Vec<Value>>>;

    fn supports_row_semantics(&self) -> bool {
        false
    }

    fn supports_set_semantics(&self) -> bool {
        true
    }
}

pub struct UnnestFunction;

impl PolymorphicTableFunction for UnnestFunction {
    fn name(&self) -> &str {
        "UNNEST"
    }

    fn infer_schema(&self, args: &[PTFArgument]) -> Result<PTFSchema> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "UNNEST requires at least 1 argument".to_string(),
            ));
        }

        let mut columns = Vec::new();
        let mut types = Vec::new();

        for (i, arg) in args.iter().enumerate() {
            if let PTFArgument::Scalar(val) = arg {
                if val.as_array().is_some() {
                    columns.push(format!("col{}", i + 1));
                    types.push(DataType::Unknown);
                } else if let Some(mr) = val.as_multirange() {
                    columns.push(format!("col{}", i + 1));
                    types.push(DataType::Range(mr.multirange_type.element_range_type()));
                } else {
                    return Err(Error::InvalidQuery(
                        "UNNEST requires array or multirange arguments".to_string(),
                    ));
                }
            } else {
                return Err(Error::InvalidQuery(
                    "UNNEST requires array or multirange arguments".to_string(),
                ));
            }
        }

        Ok(PTFSchema { columns, types })
    }

    fn execute(&self, args: &[PTFArgument]) -> Result<Vec<Vec<Value>>> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "UNNEST requires at least 1 argument".to_string(),
            ));
        }

        let mut arrays: Vec<Vec<Value>> = Vec::new();

        for arg in args {
            if let PTFArgument::Scalar(val) = arg {
                if let Some(arr) = val.as_array() {
                    arrays.push(arr.to_vec());
                } else if let Some(mr) = val.as_multirange() {
                    let range_values: Vec<Value> =
                        mr.ranges.iter().map(|r| Value::range(r.clone())).collect();
                    arrays.push(range_values);
                } else {
                    return Err(Error::InvalidQuery(
                        "UNNEST requires array or multirange arguments".to_string(),
                    ));
                }
            } else {
                return Err(Error::InvalidQuery(
                    "UNNEST requires array or multirange arguments".to_string(),
                ));
            }
        }

        let max_len = arrays.iter().map(|a| a.len()).max().unwrap_or(0);

        let mut result = Vec::new();
        for i in 0..max_len {
            let mut row = Vec::new();
            for array in &arrays {
                if i < array.len() {
                    row.push(array[i].clone());
                } else {
                    row.push(Value::null());
                }
            }
            result.push(row);
        }

        Ok(result)
    }

    fn supports_row_semantics(&self) -> bool {
        false
    }

    fn supports_set_semantics(&self) -> bool {
        true
    }
}

pub struct JsonTableFunction;

impl PolymorphicTableFunction for JsonTableFunction {
    fn name(&self) -> &str {
        "JSON_TABLE"
    }

    fn infer_schema(&self, args: &[PTFArgument]) -> Result<PTFSchema> {
        for arg in args {
            if let PTFArgument::Descriptor { columns, types } = arg {
                return Ok(PTFSchema {
                    columns: columns.clone(),
                    types: types.clone(),
                });
            }
        }

        Err(Error::InvalidQuery(
            "JSON_TABLE requires a descriptor argument".to_string(),
        ))
    }

    fn execute(&self, args: &[PTFArgument]) -> Result<Vec<Vec<Value>>> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "JSON_TABLE requires arguments".to_string(),
            ));
        }

        Ok(Vec::new())
    }
}

pub struct GenerateSeriesFunction;

impl PolymorphicTableFunction for GenerateSeriesFunction {
    fn name(&self) -> &str {
        "GENERATE_SERIES"
    }

    fn infer_schema(&self, _args: &[PTFArgument]) -> Result<PTFSchema> {
        Ok(PTFSchema {
            columns: vec!["value".to_string()],
            types: vec![DataType::Int64],
        })
    }

    fn execute(&self, args: &[PTFArgument]) -> Result<Vec<Vec<Value>>> {
        if args.len() < 2 || args.len() > 3 {
            return Err(Error::InvalidQuery(
                "GENERATE_SERIES requires 2 or 3 arguments".to_string(),
            ));
        }

        let start = match &args[0] {
            PTFArgument::Scalar(v) => v.as_i64().ok_or_else(|| {
                Error::InvalidQuery("GENERATE_SERIES start must be INT64".to_string())
            })?,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_SERIES start must be INT64".to_string(),
                ));
            }
        };

        let end = match &args[1] {
            PTFArgument::Scalar(v) => v.as_i64().ok_or_else(|| {
                Error::InvalidQuery("GENERATE_SERIES end must be INT64".to_string())
            })?,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_SERIES end must be INT64".to_string(),
                ));
            }
        };

        let step = if args.len() == 3 {
            match &args[2] {
                PTFArgument::Scalar(v) => v.as_i64().ok_or_else(|| {
                    Error::InvalidQuery("GENERATE_SERIES step must be INT64".to_string())
                })?,
                _ => {
                    return Err(Error::InvalidQuery(
                        "GENERATE_SERIES step must be INT64".to_string(),
                    ));
                }
            }
        } else {
            1
        };

        if step == 0 {
            return Err(Error::InvalidQuery(
                "GENERATE_SERIES step cannot be zero".to_string(),
            ));
        }

        let mut result = Vec::new();
        let mut current = start;

        if step > 0 {
            while current <= end {
                result.push(vec![Value::int64(current)]);
                current += step;
            }
        } else {
            while current >= end {
                result.push(vec![Value::int64(current)]);
                current += step;
            }
        }

        Ok(result)
    }
}

#[derive(Default)]
pub struct PTFRegistry {
    functions: HashMap<String, Box<dyn PolymorphicTableFunction>>,
}

impl PTFRegistry {
    pub fn new() -> Self {
        let mut registry = Self::default();
        registry.register_builtins();
        registry
    }

    fn register_builtins(&mut self) {
        self.register("UNNEST".to_string(), Box::new(UnnestFunction));
        self.register("JSON_TABLE".to_string(), Box::new(JsonTableFunction));
        self.register(
            "GENERATE_SERIES".to_string(),
            Box::new(GenerateSeriesFunction),
        );
    }

    pub fn register(&mut self, name: String, func: Box<dyn PolymorphicTableFunction>) {
        self.functions.insert(name.to_uppercase(), func);
    }

    pub fn get(&self, name: &str) -> Option<&dyn PolymorphicTableFunction> {
        self.functions.get(&name.to_uppercase()).map(|f| &**f)
    }

    pub fn exists(&self, name: &str) -> bool {
        self.functions.contains_key(&name.to_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unnest_single_array() {
        let func = UnnestFunction;

        let args = vec![PTFArgument::Scalar(Value::array(vec![
            Value::int64(1),
            Value::int64(2),
            Value::int64(3),
        ]))];

        let schema = func.infer_schema(&args).unwrap();
        assert_eq!(schema.columns.len(), 1);
        assert_eq!(schema.columns[0], "col1");

        let result = func.execute(&args).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], Value::int64(1));
        assert_eq!(result[1][0], Value::int64(2));
        assert_eq!(result[2][0], Value::int64(3));
    }

    #[test]
    fn test_unnest_multiple_arrays() {
        let func = UnnestFunction;

        let args = vec![
            PTFArgument::Scalar(Value::array(vec![Value::int64(1), Value::int64(2)])),
            PTFArgument::Scalar(Value::array(vec![
                Value::string("a".to_string()),
                Value::string("b".to_string()),
                Value::string("c".to_string()),
            ])),
        ];

        let schema = func.infer_schema(&args).unwrap();
        assert_eq!(schema.columns.len(), 2);

        let result = func.execute(&args).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], Value::int64(1));
        assert_eq!(result[0][1], Value::string("a".to_string()));
        assert_eq!(result[2][0], Value::null());
        assert_eq!(result[2][1], Value::string("c".to_string()));
    }

    #[test]
    fn test_generate_series_basic() {
        let func = GenerateSeriesFunction;

        let args = vec![
            PTFArgument::Scalar(Value::int64(1)),
            PTFArgument::Scalar(Value::int64(5)),
        ];

        let schema = func.infer_schema(&args).unwrap();
        assert_eq!(schema.columns[0], "value");
        assert_eq!(schema.types[0], DataType::Int64);

        let result = func.execute(&args).unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(result[0][0], Value::int64(1));
        assert_eq!(result[4][0], Value::int64(5));
    }

    #[test]
    fn test_generate_series_with_step() {
        let func = GenerateSeriesFunction;

        let args = vec![
            PTFArgument::Scalar(Value::int64(0)),
            PTFArgument::Scalar(Value::int64(10)),
            PTFArgument::Scalar(Value::int64(2)),
        ];

        let result = func.execute(&args).unwrap();
        assert_eq!(result.len(), 6);
        assert_eq!(result[0][0], Value::int64(0));
        assert_eq!(result[1][0], Value::int64(2));
        assert_eq!(result[5][0], Value::int64(10));
    }

    #[test]
    fn test_generate_series_descending() {
        let func = GenerateSeriesFunction;

        let args = vec![
            PTFArgument::Scalar(Value::int64(5)),
            PTFArgument::Scalar(Value::int64(1)),
            PTFArgument::Scalar(Value::int64(-1)),
        ];

        let result = func.execute(&args).unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(result[0][0], Value::int64(5));
        assert_eq!(result[4][0], Value::int64(1));
    }

    #[test]
    fn test_ptf_registry() {
        let registry = PTFRegistry::new();

        assert!(registry.exists("UNNEST"));
        assert!(registry.exists("GENERATE_SERIES"));
        assert!(registry.exists("JSON_TABLE"));

        let func = registry.get("GENERATE_SERIES").unwrap();
        assert_eq!(func.name(), "GENERATE_SERIES");
    }
}
