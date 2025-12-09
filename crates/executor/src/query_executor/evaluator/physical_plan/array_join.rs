use std::rc::Rc;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;
use yachtsql_storage::{Column, Schema};

use super::ExecutionPlan;
use crate::Table;

#[derive(Debug)]
pub struct ArrayJoinExec {
    input: Rc<dyn ExecutionPlan>,
    schema: Schema,
    arrays: Vec<(Expr, Option<String>)>,
    is_left: bool,
    is_unaligned: bool,
}

impl ArrayJoinExec {
    pub fn new(
        input: Rc<dyn ExecutionPlan>,
        arrays: Vec<(Expr, Option<String>)>,
        is_left: bool,
        is_unaligned: bool,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let mut fields = Vec::new();

        let array_col_names: std::collections::HashSet<String> = arrays
            .iter()
            .filter(|(_, alias)| alias.is_none())
            .filter_map(|(expr, _)| {
                if let Expr::Column { name, .. } = expr {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();

        for field in input_schema.fields() {
            if array_col_names.contains(&field.name) {
                let element_type =
                    if let yachtsql_core::types::DataType::Array(inner) = &field.data_type {
                        inner.as_ref().clone()
                    } else {
                        yachtsql_core::types::DataType::String
                    };
                fields.push(yachtsql_storage::Field::nullable(
                    field.name.clone(),
                    element_type,
                ));
            } else {
                fields.push(field.clone());
            }
        }

        for (expr, alias) in &arrays {
            if let Some(alias_name) = alias {
                let field_name = alias_name.clone();
                let element_type = if let Expr::Column { name, .. } = expr {
                    input_schema
                        .fields()
                        .iter()
                        .find(|f| &f.name == name)
                        .map(|f| {
                            if let yachtsql_core::types::DataType::Array(inner) = &f.data_type {
                                inner.as_ref().clone()
                            } else {
                                yachtsql_core::types::DataType::String
                            }
                        })
                        .unwrap_or(yachtsql_core::types::DataType::String)
                } else {
                    yachtsql_core::types::DataType::String
                };

                fields.push(yachtsql_storage::Field::nullable(field_name, element_type));
            }
        }

        let schema = Schema::from_fields(fields);

        Ok(Self {
            input,
            schema,
            arrays,
            is_left,
            is_unaligned,
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
            _ => Err(Error::unsupported_feature(
                "Complex expressions in ARRAY JOIN not yet supported".to_string(),
            )),
        }
    }

    fn extract_arrays(&self, batch: &Table, row_idx: usize) -> Result<Vec<Vec<Value>>> {
        let mut result = Vec::new();

        for (expr, _) in &self.arrays {
            let value = self.evaluate_expr(expr, batch, row_idx)?;

            if let Some(arr) = value.as_array() {
                result.push(arr.clone());
            } else if value.is_null() {
                result.push(vec![]);
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        }

        Ok(result)
    }

    fn get_array_col_map(&self) -> std::collections::HashMap<String, usize> {
        let mut map = std::collections::HashMap::new();
        for (idx, (expr, alias)) in self.arrays.iter().enumerate() {
            if alias.is_none() {
                if let Expr::Column { name, .. } = expr {
                    map.insert(name.clone(), idx);
                }
            }
        }
        map
    }

    fn generate_aligned_rows(
        &self,
        batch: &Table,
        row_idx: usize,
        arrays: Vec<Vec<Value>>,
    ) -> Result<Vec<Vec<Value>>> {
        if arrays.is_empty() {
            return Ok(vec![]);
        }

        let max_len = arrays.iter().map(|arr| arr.len()).max().unwrap_or(0);
        let array_col_map = self.get_array_col_map();

        if max_len == 0 && self.is_left {
            let mut row = Vec::new();
            let input_cols = batch.expect_columns();
            let input_fields = batch.schema().fields();

            for (col_idx, col) in input_cols.iter().enumerate() {
                if let Some(arr_idx) = array_col_map.get(&input_fields[col_idx].name) {
                    row.push(Value::null());
                    let _ = arr_idx;
                } else {
                    row.push(col.get(row_idx)?);
                }
            }

            for (_, alias) in &self.arrays {
                if alias.is_some() {
                    row.push(Value::null());
                }
            }

            return Ok(vec![row]);
        }

        if max_len == 0 {
            return Ok(vec![]);
        }

        let mut result_rows = Vec::new();
        let input_cols = batch.expect_columns();
        let input_fields = batch.schema().fields();

        for i in 0..max_len {
            let mut row = Vec::new();

            for (col_idx, col) in input_cols.iter().enumerate() {
                if let Some(arr_idx) = array_col_map.get(&input_fields[col_idx].name) {
                    let val = if i < arrays[*arr_idx].len() {
                        arrays[*arr_idx][i].clone()
                    } else {
                        Value::null()
                    };
                    row.push(val);
                } else {
                    row.push(col.get(row_idx)?);
                }
            }

            for (arr_idx, (_, alias)) in self.arrays.iter().enumerate() {
                if alias.is_some() {
                    let val = if i < arrays[arr_idx].len() {
                        arrays[arr_idx][i].clone()
                    } else {
                        Value::null()
                    };
                    row.push(val);
                }
            }

            result_rows.push(row);
        }

        Ok(result_rows)
    }

    fn generate_unaligned_rows(
        &self,
        batch: &Table,
        row_idx: usize,
        arrays: Vec<Vec<Value>>,
    ) -> Result<Vec<Vec<Value>>> {
        if arrays.is_empty() {
            return Ok(vec![]);
        }

        let total_combinations = arrays.iter().map(|arr| arr.len().max(1)).product::<usize>();

        if total_combinations == 0 {
            return Ok(vec![]);
        }

        let array_col_map = self.get_array_col_map();
        let mut result_rows = Vec::new();
        let input_cols = batch.expect_columns();
        let input_fields = batch.schema().fields();

        for combo_idx in 0..total_combinations {
            let mut row = Vec::new();

            let mut indices: Vec<usize> = Vec::new();
            let mut divisor = total_combinations;
            for arr in &arrays {
                let arr_len = arr.len().max(1);
                divisor /= arr_len;
                let idx = (combo_idx / divisor) % arr_len;
                indices.push(idx);
            }

            for (col_idx, col) in input_cols.iter().enumerate() {
                if let Some(arr_idx) = array_col_map.get(&input_fields[col_idx].name) {
                    let idx = indices[*arr_idx];
                    let val = if !arrays[*arr_idx].is_empty() {
                        arrays[*arr_idx][idx].clone()
                    } else {
                        Value::null()
                    };
                    row.push(val);
                } else {
                    row.push(col.get(row_idx)?);
                }
            }

            for (arr_idx, (_, alias)) in self.arrays.iter().enumerate() {
                if alias.is_some() {
                    let idx = indices[arr_idx];
                    let val = if !arrays[arr_idx].is_empty() {
                        arrays[arr_idx][idx].clone()
                    } else {
                        Value::null()
                    };
                    row.push(val);
                }
            }

            result_rows.push(row);
        }

        Ok(result_rows)
    }
}

impl ExecutionPlan for ArrayJoinExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let input_batches = self.input.execute()?;

        if input_batches.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let mut result_batches = Vec::new();

        for input_batch in input_batches {
            let num_rows = input_batch.num_rows();
            let mut all_rows: Vec<Vec<Value>> = Vec::new();

            for row_idx in 0..num_rows {
                let arrays = self.extract_arrays(&input_batch, row_idx)?;

                let rows = if self.is_unaligned {
                    self.generate_unaligned_rows(&input_batch, row_idx, arrays)?
                } else {
                    self.generate_aligned_rows(&input_batch, row_idx, arrays)?
                };

                all_rows.extend(rows);
            }

            if all_rows.is_empty() {
                continue;
            }

            let num_output_rows = all_rows.len();
            let num_cols = self.schema.fields().len();
            let mut columns = Vec::new();

            for col_idx in 0..num_cols {
                let field = &self.schema.fields()[col_idx];
                let mut column = Column::new(&field.data_type, num_output_rows);

                for row in &all_rows {
                    column.push(row[col_idx].clone())?;
                }

                columns.push(column);
            }

            result_batches.push(Table::new(self.schema.clone(), columns)?);
        }

        if result_batches.is_empty() {
            Ok(vec![Table::empty(self.schema.clone())])
        } else {
            Ok(result_batches)
        }
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn describe(&self) -> String {
        let variant = if self.is_left {
            "LEFT ARRAY JOIN"
        } else if self.is_unaligned {
            "UNALIGNED ARRAY JOIN"
        } else {
            "ARRAY JOIN"
        };
        format!("{} ({} arrays)", variant, self.arrays.len())
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_storage::Field;

    use super::*;

    #[derive(Debug)]
    struct MockScan {
        schema: Schema,
        data: Vec<Vec<Value>>,
    }

    impl ExecutionPlan for MockScan {
        fn schema(&self) -> &Schema {
            &self.schema
        }

        fn execute(&self) -> Result<Vec<Table>> {
            let num_rows = self.data.len();
            let num_cols = self.schema.fields().len();

            let mut columns = Vec::new();
            for col_idx in 0..num_cols {
                let field = &self.schema.fields()[col_idx];
                let mut column = Column::new(&field.data_type, num_rows);

                for row in &self.data {
                    column.push(row[col_idx].clone())?;
                }

                columns.push(column);
            }

            Ok(vec![Table::new(self.schema.clone(), columns)?])
        }

        fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
            vec![]
        }

        fn describe(&self) -> String {
            "MockScan".to_string()
        }
    }

    #[test]
    fn test_array_join_basic() {
        let schema = Schema::from_fields(vec![
            Field::required("id".to_string(), yachtsql_core::types::DataType::Int64),
            Field::nullable(
                "hobbies".to_string(),
                yachtsql_core::types::DataType::Array(Box::new(
                    yachtsql_core::types::DataType::String,
                )),
            ),
        ]);

        let data = vec![
            vec![
                Value::int64(1),
                Value::array(vec![
                    Value::string("reading".to_string()),
                    Value::string("gaming".to_string()),
                ]),
            ],
            vec![
                Value::int64(2),
                Value::array(vec![Value::string("cooking".to_string())]),
            ],
        ];

        let input = Rc::new(MockScan {
            schema: schema.clone(),
            data,
        });

        let arrays = vec![(
            Expr::Column {
                name: "hobbies".to_string(),
                table: None,
            },
            Some("hobby".to_string()),
        )];

        let array_join = ArrayJoinExec::new(input, arrays, false, false).unwrap();
        let results = array_join.execute().unwrap();

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_left_array_join() {
        let schema = Schema::from_fields(vec![
            Field::required("id".to_string(), yachtsql_core::types::DataType::Int64),
            Field::nullable(
                "hobbies".to_string(),
                yachtsql_core::types::DataType::Array(Box::new(
                    yachtsql_core::types::DataType::String,
                )),
            ),
        ]);

        let data = vec![
            vec![Value::int64(1), Value::array(vec![])],
            vec![Value::int64(2), Value::null()],
        ];

        let input = Rc::new(MockScan {
            schema: schema.clone(),
            data,
        });

        let arrays = vec![(
            Expr::Column {
                name: "hobbies".to_string(),
                table: None,
            },
            Some("hobby".to_string()),
        )];

        let array_join = ArrayJoinExec::new(input, arrays, true, false).unwrap();
        let results = array_join.execute().unwrap();

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 2);
    }
}
