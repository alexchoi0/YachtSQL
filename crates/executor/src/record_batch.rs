use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::coercion::CoercionRules;
use yachtsql_core::types::{DataType, Value};
use yachtsql_storage::{Column, Row, Schema};

#[inline]
fn are_types_compatible(col_type: &DataType, schema_type: &DataType) -> bool {
    if col_type == schema_type {
        return true;
    }

    if matches!(col_type, DataType::Unknown) || matches!(schema_type, DataType::Unknown) {
        return true;
    }

    if CoercionRules::can_implicitly_coerce(col_type, schema_type) {
        return true;
    }

    if matches!(
        (col_type, schema_type),
        (DataType::Struct(_), DataType::Custom(_))
            | (DataType::Custom(_), DataType::Struct(_))
            | (DataType::Struct(_), DataType::Struct(_))
    ) {
        return true;
    }

    matches!(
        (col_type, schema_type),
        (DataType::Float64, DataType::Int64) | (DataType::Float64, DataType::BigNumeric)
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFormat {
    Row,
    Column,
}

#[derive(Debug, Clone)]
enum StorageData {
    Rows(Vec<Row>),
    Columns(Vec<Column>),
}

#[derive(Debug, Clone)]
pub struct Table {
    schema: Schema,
    storage: StorageData,
    num_rows: usize,
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        if self.num_rows != other.num_rows || self.schema != other.schema {
            return false;
        }

        for row_idx in 0..self.num_rows {
            let self_row = match self.row(row_idx) {
                Ok(r) => r,
                Err(_) => return false,
            };
            let other_row = match other.row(row_idx) {
                Ok(r) => r,
                Err(_) => return false,
            };
            if self_row.values() != other_row.values() {
                return false;
            }
        }
        true
    }
}

impl Table {
    pub fn new(schema: Schema, columns: Vec<Column>) -> Result<Self> {
        if columns.len() != schema.field_count() {
            return Err(Error::schema_mismatch(format!(
                "Expected {} columns, got {}",
                schema.field_count(),
                columns.len()
            )));
        }

        let num_rows = columns.first().map(|c| c.len()).unwrap_or(0);
        for (i, column) in columns.iter().enumerate() {
            if column.len() != num_rows {
                return Err(Error::schema_mismatch(format!(
                    "Column {} has {} rows, expected {}",
                    i,
                    column.len(),
                    num_rows
                )));
            }
        }

        for (column, field) in columns.iter().zip(schema.fields()) {
            let col_type = column.data_type();
            let schema_type = &field.data_type;

            if !are_types_compatible(&col_type, schema_type) {
                return Err(Error::TypeMismatch {
                    expected: schema_type.to_string(),
                    actual: col_type.to_string(),
                });
            }
        }

        Ok(Self {
            schema,
            storage: StorageData::Columns(columns),
            num_rows,
        })
    }

    pub fn from_rows(schema: Schema, rows: Vec<Row>) -> Result<Self> {
        let num_rows = rows.len();

        for (i, row) in rows.iter().enumerate() {
            if row.values().len() != schema.field_count() {
                return Err(Error::schema_mismatch(format!(
                    "Row {} has {} values, expected {}",
                    i,
                    row.values().len(),
                    schema.field_count()
                )));
            }
        }

        Ok(Self {
            schema,
            storage: StorageData::Rows(rows),
            num_rows,
        })
    }

    pub fn empty(schema: Schema) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|field| Column::new(&field.data_type, 0))
            .collect();

        Self {
            schema,
            storage: StorageData::Columns(columns),
            num_rows: 0,
        }
    }

    pub fn empty_rows(schema: Schema) -> Self {
        Self {
            schema,
            storage: StorageData::Rows(vec![]),
            num_rows: 0,
        }
    }

    pub fn empty_with_rows(schema: Schema, num_rows: usize) -> Self {
        let rows = (0..num_rows).map(|_| Row::from_values(vec![])).collect();
        Self {
            schema,
            storage: StorageData::Rows(rows),
            num_rows,
        }
    }

    pub fn storage_format(&self) -> StorageFormat {
        match &self.storage {
            StorageData::Rows(_) => StorageFormat::Row,
            StorageData::Columns(_) => StorageFormat::Column,
        }
    }

    pub fn to_row_format(&self) -> Result<Self> {
        match &self.storage {
            StorageData::Rows(_) => Ok(self.clone()),
            StorageData::Columns(columns) => {
                let mut rows = Vec::with_capacity(self.num_rows);

                for row_idx in 0..self.num_rows {
                    let mut values = Vec::with_capacity(columns.len());
                    for column in columns {
                        values.push(column.get(row_idx)?);
                    }
                    rows.push(Row::from_values(values));
                }

                Ok(Self {
                    schema: self.schema.clone(),
                    storage: StorageData::Rows(rows),
                    num_rows: self.num_rows,
                })
            }
        }
    }

    pub fn to_column_format(&self) -> Result<Self> {
        match &self.storage {
            StorageData::Columns(_) => Ok(self.clone()),
            StorageData::Rows(rows) => {
                if rows.is_empty() {
                    return Ok(Self::empty(self.schema.clone()));
                }

                let num_cols = self.schema.field_count();
                let mut columns: Vec<Column> = self
                    .schema
                    .fields()
                    .iter()
                    .map(|field| Column::new(&field.data_type, self.num_rows))
                    .collect();

                for row in rows {
                    let values = row.values();
                    for (col_idx, value) in values.iter().enumerate() {
                        if col_idx < num_cols {
                            columns[col_idx].push(value.clone())?;
                        }
                    }
                }

                Ok(Self {
                    schema: self.schema.clone(),
                    storage: StorageData::Columns(columns),
                    num_rows: self.num_rows,
                })
            }
        }
    }

    pub fn single_empty_row(schema: Schema) -> Self {
        debug_assert_eq!(
            schema.field_count(),
            0,
            "single_empty_row requires empty schema"
        );

        Self {
            schema,
            storage: StorageData::Columns(vec![]),
            num_rows: 1,
        }
    }

    pub fn from_values(schema: Schema, values: Vec<Vec<Value>>) -> Result<Self> {
        if values.is_empty() {
            return Ok(Self::empty(schema));
        }

        let num_rows = values.len();
        let num_cols = schema.field_count();

        if values.iter().any(|row| row.len() != num_cols) {
            return Err(Error::schema_mismatch(
                "All rows must have the same number of columns".to_string(),
            ));
        }

        let mut columns: Vec<Column> = schema
            .fields()
            .iter()
            .map(|field| Column::new(&field.data_type, num_rows))
            .collect();

        for row in values {
            for (col_idx, value) in row.into_iter().enumerate() {
                columns[col_idx].push(value)?;
            }
        }

        Self::new(schema, columns)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn num_columns(&self) -> usize {
        self.schema.field_count()
    }

    pub fn column(&self, index: usize) -> Option<&Column> {
        match &self.storage {
            StorageData::Columns(columns) => columns.get(index),
            StorageData::Rows(_) => None,
        }
    }

    pub fn columns(&self) -> Option<&[Column]> {
        match &self.storage {
            StorageData::Columns(columns) => Some(columns.as_slice()),
            StorageData::Rows(_) => None,
        }
    }

    pub(crate) fn expect_columns(&self) -> &[Column] {
        match &self.storage {
            StorageData::Columns(columns) => columns.as_slice(),
            StorageData::Rows(_) => panic!(
                "Expected column-formatted Table, but got row format. \
                 Convert to column format first using to_column_format()"
            ),
        }
    }

    pub fn column_by_name(&self, name: &str) -> Option<&Column> {
        match &self.storage {
            StorageData::Columns(columns) => self
                .schema
                .fields()
                .iter()
                .position(|f| f.name == name)
                .and_then(|idx| columns.get(idx)),
            StorageData::Rows(_) => None,
        }
    }

    pub fn rows_slice(&self) -> Option<&[Row]> {
        match &self.storage {
            StorageData::Rows(rows) => Some(rows.as_slice()),
            StorageData::Columns(_) => None,
        }
    }

    pub fn row(&self, index: usize) -> Result<Row> {
        if index >= self.num_rows {
            return Err(Error::InvalidOperation(format!(
                "Row index {} out of bounds (num_rows: {})",
                index, self.num_rows
            )));
        }

        match &self.storage {
            StorageData::Rows(rows) => Ok(rows[index].clone()),
            StorageData::Columns(columns) => {
                let mut values = Vec::with_capacity(self.schema.field_count());
                for column in columns {
                    values.push(column.get(index)?);
                }
                Ok(Row::from_values(values))
            }
        }
    }

    pub fn rows(&self) -> Result<Vec<Row>> {
        match &self.storage {
            StorageData::Rows(rows) => Ok(rows.clone()),
            StorageData::Columns(_) => {
                let mut result = Vec::with_capacity(self.num_rows);
                for i in 0..self.num_rows {
                    result.push(self.row(i)?);
                }
                Ok(result)
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    pub fn slice(&self, offset: usize, length: usize) -> Result<Self> {
        if offset + length > self.num_rows {
            return Err(Error::InvalidOperation(format!(
                "Slice range {}..{} exceeds batch size {}",
                offset,
                offset + length,
                self.num_rows
            )));
        }

        let storage = match &self.storage {
            StorageData::Columns(columns) => {
                let sliced_columns = columns
                    .iter()
                    .map(|col| col.slice(offset, length))
                    .collect::<Result<Vec<_>>>()?;
                StorageData::Columns(sliced_columns)
            }
            StorageData::Rows(rows) => {
                let sliced_rows = rows[offset..offset + length].to_vec();
                StorageData::Rows(sliced_rows)
            }
        };

        Ok(Self {
            schema: self.schema.clone(),
            storage,
            num_rows: length,
        })
    }

    pub fn project(&self, indices: &[usize]) -> Result<Self> {
        let field_names: Vec<String> = indices
            .iter()
            .map(|&i| {
                self.schema
                    .fields()
                    .get(i)
                    .map(|f| f.name.clone())
                    .ok_or_else(|| {
                        Error::InvalidOperation(format!("Column index {} out of bounds", i))
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        let projected_schema = self.schema.project(&field_names)?;

        let storage = match &self.storage {
            StorageData::Columns(columns) => {
                let mut projected_columns = Vec::with_capacity(indices.len());
                for &index in indices {
                    let column = columns.get(index).ok_or_else(|| {
                        Error::InvalidOperation(format!("Column index {} out of bounds", index))
                    })?;
                    projected_columns.push(column.clone());
                }
                StorageData::Columns(projected_columns)
            }
            StorageData::Rows(rows) => {
                let projected_rows: Vec<Row> = rows
                    .iter()
                    .map(|row| {
                        let values: Vec<Value> = indices
                            .iter()
                            .map(|&idx| row.values().get(idx).cloned().unwrap_or(Value::null()))
                            .collect();
                        Row::from_values(values)
                    })
                    .collect();
                StorageData::Rows(projected_rows)
            }
        };

        Ok(Self {
            schema: projected_schema,
            storage,
            num_rows: self.num_rows,
        })
    }

    pub fn concat(batches: &[Table]) -> Result<Self> {
        if batches.is_empty() {
            return Err(Error::InvalidOperation(
                "Cannot concatenate empty batch list".to_string(),
            ));
        }

        let schema = batches[0].schema.clone();
        let format = batches[0].storage_format();

        for batch in &batches[1..] {
            if !batch.schema.is_compatible_with(&schema) {
                return Err(Error::schema_mismatch(
                    "All batches must have the same schema".to_string(),
                ));
            }
            if batch.storage_format() != format {
                return Err(Error::InvalidOperation(
                    "All batches must have the same storage format. Convert batches first."
                        .to_string(),
                ));
            }
        }

        let total_rows: usize = batches.iter().map(|b| b.num_rows).sum();

        let storage = match format {
            StorageFormat::Column => {
                let num_cols = schema.field_count();
                let mut concatenated_columns = Vec::with_capacity(num_cols);

                for col_idx in 0..num_cols {
                    let first_batch_columns = batches[0].columns().ok_or_else(|| {
                        Error::InternalError("Expected column storage".to_string())
                    })?;
                    let mut result_col = first_batch_columns[col_idx].clone();

                    for batch in &batches[1..] {
                        let batch_columns = batch.columns().ok_or_else(|| {
                            Error::InternalError("Expected column storage".to_string())
                        })?;
                        result_col.append(&batch_columns[col_idx])?;
                    }
                    concatenated_columns.push(result_col);
                }
                StorageData::Columns(concatenated_columns)
            }
            StorageFormat::Row => {
                let mut concatenated_rows = Vec::with_capacity(total_rows);
                for batch in batches {
                    let batch_rows = batch
                        .rows_slice()
                        .ok_or_else(|| Error::InternalError("Expected row storage".to_string()))?;
                    concatenated_rows.extend_from_slice(batch_rows);
                }
                StorageData::Rows(concatenated_rows)
            }
        };

        Ok(Self {
            schema,
            storage,
            num_rows: total_rows,
        })
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::DataType;
    use yachtsql_storage::Field;

    use super::*;

    #[test]
    fn test_empty_record_batch() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);

        let batch = Table::empty(schema.clone());
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_record_batch_creation() {
        let schema = Schema::from_fields(vec![Field::nullable("id", DataType::Int64)]);

        let mut column = Column::new(&DataType::Int64, 3);
        column.push(Value::int64(1)).unwrap();
        column.push(Value::int64(2)).unwrap();
        column.push(Value::int64(3)).unwrap();

        let batch = Table::new(schema, vec![column]).unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 1);
        assert!(!batch.is_empty());
    }

    #[test]
    fn test_schema_mismatch() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);

        let mut column = Column::new(&DataType::Int64, 1);
        column.push(Value::int64(1)).unwrap();

        let result = Table::new(schema, vec![column]);
        assert!(result.is_err());
    }

    #[test]
    fn test_row_format_creation() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);

        let rows = vec![
            Row::from_values(vec![Value::int64(1), Value::string("Alice".to_string())]),
            Row::from_values(vec![Value::int64(2), Value::string("Bob".to_string())]),
            Row::from_values(vec![Value::int64(3), Value::string("Charlie".to_string())]),
        ];

        let batch = Table::from_rows(schema.clone(), rows).unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.storage_format(), StorageFormat::Row);
        assert!(batch.rows_slice().is_some());
        assert!(batch.columns().is_none());
    }

    #[test]
    fn test_column_format_creation() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);

        let mut id_col = Column::new(&DataType::Int64, 3);
        id_col.push(Value::int64(1)).unwrap();
        id_col.push(Value::int64(2)).unwrap();
        id_col.push(Value::int64(3)).unwrap();

        let mut name_col = Column::new(&DataType::String, 3);
        name_col.push(Value::string("Alice".to_string())).unwrap();
        name_col.push(Value::string("Bob".to_string())).unwrap();
        name_col.push(Value::string("Charlie".to_string())).unwrap();

        let batch = Table::new(schema, vec![id_col, name_col]).unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.storage_format(), StorageFormat::Column);
        assert!(batch.columns().is_some());
        assert!(batch.rows_slice().is_none());
    }

    #[test]
    fn test_row_to_column_conversion() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("value", DataType::Float64),
        ]);

        let rows = vec![
            Row::from_values(vec![Value::int64(1), Value::float64(1.5)]),
            Row::from_values(vec![Value::int64(2), Value::float64(2.5)]),
            Row::from_values(vec![Value::int64(3), Value::float64(3.5)]),
        ];

        let row_batch = Table::from_rows(schema.clone(), rows).unwrap();
        assert_eq!(row_batch.storage_format(), StorageFormat::Row);

        let col_batch = row_batch.to_column_format().unwrap();
        assert_eq!(col_batch.storage_format(), StorageFormat::Column);
        assert_eq!(col_batch.num_rows(), 3);
        assert_eq!(col_batch.num_columns(), 2);

        let columns = col_batch.columns().unwrap();
        assert_eq!(columns[0].get(0).unwrap(), Value::int64(1));
        assert_eq!(columns[0].get(1).unwrap(), Value::int64(2));
        assert_eq!(columns[0].get(2).unwrap(), Value::int64(3));
        assert_eq!(columns[1].get(0).unwrap(), Value::float64(1.5));
        assert_eq!(columns[1].get(1).unwrap(), Value::float64(2.5));
        assert_eq!(columns[1].get(2).unwrap(), Value::float64(3.5));
    }

    #[test]
    fn test_column_to_row_conversion() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("value", DataType::Float64),
        ]);

        let mut id_col = Column::new(&DataType::Int64, 3);
        id_col.push(Value::int64(1)).unwrap();
        id_col.push(Value::int64(2)).unwrap();
        id_col.push(Value::int64(3)).unwrap();

        let mut val_col = Column::new(&DataType::Float64, 3);
        val_col.push(Value::float64(1.5)).unwrap();
        val_col.push(Value::float64(2.5)).unwrap();
        val_col.push(Value::float64(3.5)).unwrap();

        let col_batch = Table::new(schema.clone(), vec![id_col, val_col]).unwrap();
        assert_eq!(col_batch.storage_format(), StorageFormat::Column);

        let row_batch = col_batch.to_row_format().unwrap();
        assert_eq!(row_batch.storage_format(), StorageFormat::Row);
        assert_eq!(row_batch.num_rows(), 3);
        assert_eq!(row_batch.num_columns(), 2);

        let rows = row_batch.rows_slice().unwrap();
        assert_eq!(rows[0].values()[0], Value::int64(1));
        assert_eq!(rows[0].values()[1], Value::float64(1.5));
        assert_eq!(rows[1].values()[0], Value::int64(2));
        assert_eq!(rows[1].values()[1], Value::float64(2.5));
        assert_eq!(rows[2].values()[0], Value::int64(3));
        assert_eq!(rows[2].values()[1], Value::float64(3.5));
    }

    #[test]
    fn test_row_accessor_for_both_formats() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);

        let mut id_col = Column::new(&DataType::Int64, 2);
        id_col.push(Value::int64(1)).unwrap();
        id_col.push(Value::int64(2)).unwrap();

        let mut name_col = Column::new(&DataType::String, 2);
        name_col.push(Value::string("Alice".to_string())).unwrap();
        name_col.push(Value::string("Bob".to_string())).unwrap();

        let col_batch = Table::new(schema.clone(), vec![id_col, name_col]).unwrap();
        let row = col_batch.row(1).unwrap();
        assert_eq!(row.values()[0], Value::int64(2));
        assert_eq!(row.values()[1], Value::string("Bob".to_string()));

        let rows = vec![
            Row::from_values(vec![Value::int64(1), Value::string("Alice".to_string())]),
            Row::from_values(vec![Value::int64(2), Value::string("Bob".to_string())]),
        ];
        let row_batch = Table::from_rows(schema, rows).unwrap();
        let row = row_batch.row(1).unwrap();
        assert_eq!(row.values()[0], Value::int64(2));
        assert_eq!(row.values()[1], Value::string("Bob".to_string()));
    }

    #[test]
    fn test_slice_preserves_format() {
        let schema = Schema::from_fields(vec![Field::nullable("id", DataType::Int64)]);

        let rows = vec![
            Row::from_values(vec![Value::int64(1)]),
            Row::from_values(vec![Value::int64(2)]),
            Row::from_values(vec![Value::int64(3)]),
            Row::from_values(vec![Value::int64(4)]),
        ];
        let row_batch = Table::from_rows(schema.clone(), rows).unwrap();
        let sliced = row_batch.slice(1, 2).unwrap();
        assert_eq!(sliced.storage_format(), StorageFormat::Row);
        assert_eq!(sliced.num_rows(), 2);

        let mut col = Column::new(&DataType::Int64, 4);
        col.push(Value::int64(1)).unwrap();
        col.push(Value::int64(2)).unwrap();
        col.push(Value::int64(3)).unwrap();
        col.push(Value::int64(4)).unwrap();
        let col_batch = Table::new(schema, vec![col]).unwrap();
        let sliced = col_batch.slice(1, 2).unwrap();
        assert_eq!(sliced.storage_format(), StorageFormat::Column);
        assert_eq!(sliced.num_rows(), 2);
    }

    #[test]
    fn test_project_preserves_format() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
            Field::nullable("age", DataType::Int64),
        ]);

        let rows = vec![
            Row::from_values(vec![
                Value::int64(1),
                Value::string("Alice".to_string()),
                Value::int64(30),
            ]),
            Row::from_values(vec![
                Value::int64(2),
                Value::string("Bob".to_string()),
                Value::int64(25),
            ]),
        ];
        let row_batch = Table::from_rows(schema.clone(), rows).unwrap();
        let projected = row_batch.project(&[0, 2]).unwrap();
        assert_eq!(projected.storage_format(), StorageFormat::Row);
        assert_eq!(projected.num_columns(), 2);

        let mut id_col = Column::new(&DataType::Int64, 2);
        id_col.push(Value::int64(1)).unwrap();
        id_col.push(Value::int64(2)).unwrap();

        let mut name_col = Column::new(&DataType::String, 2);
        name_col.push(Value::string("Alice".to_string())).unwrap();
        name_col.push(Value::string("Bob".to_string())).unwrap();

        let mut age_col = Column::new(&DataType::Int64, 2);
        age_col.push(Value::int64(30)).unwrap();
        age_col.push(Value::int64(25)).unwrap();

        let col_batch = Table::new(schema, vec![id_col, name_col, age_col]).unwrap();
        let projected = col_batch.project(&[0, 2]).unwrap();
        assert_eq!(projected.storage_format(), StorageFormat::Column);
        assert_eq!(projected.num_columns(), 2);
    }

    #[test]
    fn test_concat_row_format() {
        let schema = Schema::from_fields(vec![Field::nullable("id", DataType::Int64)]);

        let batch1 = Table::from_rows(
            schema.clone(),
            vec![
                Row::from_values(vec![Value::int64(1)]),
                Row::from_values(vec![Value::int64(2)]),
            ],
        )
        .unwrap();

        let batch2 = Table::from_rows(
            schema.clone(),
            vec![
                Row::from_values(vec![Value::int64(3)]),
                Row::from_values(vec![Value::int64(4)]),
            ],
        )
        .unwrap();

        let concatenated = Table::concat(&[batch1, batch2]).unwrap();
        assert_eq!(concatenated.storage_format(), StorageFormat::Row);
        assert_eq!(concatenated.num_rows(), 4);
    }

    #[test]
    fn test_concat_column_format() {
        let schema = Schema::from_fields(vec![Field::nullable("id", DataType::Int64)]);

        let mut col1 = Column::new(&DataType::Int64, 2);
        col1.push(Value::int64(1)).unwrap();
        col1.push(Value::int64(2)).unwrap();
        let batch1 = Table::new(schema.clone(), vec![col1]).unwrap();

        let mut col2 = Column::new(&DataType::Int64, 2);
        col2.push(Value::int64(3)).unwrap();
        col2.push(Value::int64(4)).unwrap();
        let batch2 = Table::new(schema, vec![col2]).unwrap();

        let concatenated = Table::concat(&[batch1, batch2]).unwrap();
        assert_eq!(concatenated.storage_format(), StorageFormat::Column);
        assert_eq!(concatenated.num_rows(), 4);
    }

    #[test]
    fn test_concat_mixed_formats_fails() {
        let schema = Schema::from_fields(vec![Field::nullable("id", DataType::Int64)]);

        let batch1 = Table::from_rows(
            schema.clone(),
            vec![Row::from_values(vec![Value::int64(1)])],
        )
        .unwrap();

        let mut col = Column::new(&DataType::Int64, 1);
        col.push(Value::int64(2)).unwrap();
        let batch2 = Table::new(schema, vec![col]).unwrap();

        let result = Table::concat(&[batch1, batch2]);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_rows() {
        let schema = Schema::from_fields(vec![Field::nullable("id", DataType::Int64)]);
        let batch = Table::empty_rows(schema);
        assert_eq!(batch.storage_format(), StorageFormat::Row);
        assert_eq!(batch.num_rows(), 0);
        assert!(batch.is_empty());
    }
}
