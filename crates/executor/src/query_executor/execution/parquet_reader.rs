use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, ListArray, MapArray, StringArray, StructArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, NaiveTime};
use indexmap::IndexMap;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::Decimal;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, StructField, Value};
use yachtsql_storage::{Field, FieldMode, Row, Schema};

pub fn read_parquet_file(path: &str) -> Result<(Schema, Vec<Row>)> {
    let normalized_path = normalize_uri_to_path(path)?;
    let file = File::open(&normalized_path)
        .map_err(|e| Error::internal(format!("Failed to open parquet file '{}': {}", path, e)))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::internal(format!("Failed to read parquet file '{}': {}", path, e)))?;

    let arrow_schema = builder.schema().clone();
    let schema = arrow_schema_to_schema(&arrow_schema)?;

    let reader = builder.build().map_err(|e| {
        Error::internal(format!(
            "Failed to build parquet reader for '{}': {}",
            path, e
        ))
    })?;

    let mut all_rows = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| {
            Error::internal(format!(
                "Failed to read record batch from '{}': {}",
                path, e
            ))
        })?;
        let rows = convert_record_batch_to_rows(&batch, &schema)?;
        all_rows.extend(rows);
    }

    Ok((schema, all_rows))
}

pub fn load_parquet_into_table(path: &str, target_schema: &Schema) -> Result<Vec<Row>> {
    let normalized_path = normalize_uri_to_path(path)?;
    let file = File::open(&normalized_path)
        .map_err(|e| Error::internal(format!("Failed to open parquet file '{}': {}", path, e)))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::internal(format!("Failed to read parquet file '{}': {}", path, e)))?;

    let arrow_schema = builder.schema().clone();
    let column_mapping = build_column_mapping(&arrow_schema, target_schema)?;

    let reader = builder.build().map_err(|e| {
        Error::internal(format!(
            "Failed to build parquet reader for '{}': {}",
            path, e
        ))
    })?;

    let mut all_rows = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| {
            Error::internal(format!(
                "Failed to read record batch from '{}': {}",
                path, e
            ))
        })?;
        let rows = convert_batch_with_mapping(&batch, target_schema, &column_mapping)?;
        all_rows.extend(rows);
    }

    Ok(all_rows)
}

fn normalize_uri_to_path(uri: &str) -> Result<String> {
    if uri.starts_with("file://") {
        Ok(uri.strip_prefix("file://").unwrap().to_string())
    } else if uri.starts_with("gs://") || uri.starts_with("s3://") || uri.starts_with("http") {
        Err(Error::unsupported_feature(format!(
            "Remote URIs are not yet supported: {}",
            uri
        )))
    } else {
        Ok(uri.to_string())
    }
}

fn arrow_schema_to_schema(arrow_schema: &arrow::datatypes::Schema) -> Result<Schema> {
    let fields: Vec<Field> = arrow_schema
        .fields()
        .iter()
        .map(|f| arrow_field_to_field(f))
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::from_fields(fields))
}

fn arrow_field_to_field(arrow_field: &ArrowField) -> Result<Field> {
    let data_type = arrow_type_to_datatype(arrow_field.data_type())?;
    if arrow_field.is_nullable() {
        Ok(Field::nullable(arrow_field.name().clone(), data_type))
    } else {
        Ok(Field::required(arrow_field.name().clone(), data_type))
    }
}

fn arrow_type_to_datatype(arrow_type: &ArrowDataType) -> Result<DataType> {
    match arrow_type {
        ArrowDataType::Boolean => Ok(DataType::Bool),
        ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::Int64
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64 => Ok(DataType::Int64),
        ArrowDataType::Float16 | ArrowDataType::Float32 => Ok(DataType::Float32),
        ArrowDataType::Float64 => Ok(DataType::Float64),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Ok(DataType::String),
        ArrowDataType::Binary | ArrowDataType::LargeBinary => Ok(DataType::Bytes),
        ArrowDataType::Date32 | ArrowDataType::Date64 => Ok(DataType::Date),
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => Ok(DataType::Time),
        ArrowDataType::Timestamp(_, None) => Ok(DataType::DateTime),
        ArrowDataType::Timestamp(_, Some(_)) => Ok(DataType::TimestampTz),
        ArrowDataType::Decimal128(precision, scale) => {
            Ok(DataType::Numeric(Some((*precision, *scale as u8))))
        }
        ArrowDataType::Decimal256(precision, scale) => {
            Ok(DataType::Numeric(Some((*precision, *scale as u8))))
        }
        ArrowDataType::List(field) | ArrowDataType::LargeList(field) => {
            let element_type = arrow_type_to_datatype(field.data_type())?;
            Ok(DataType::Array(Box::new(element_type)))
        }
        ArrowDataType::Struct(fields) => {
            let struct_fields: Vec<StructField> = fields
                .iter()
                .map(|f| {
                    let dt = arrow_type_to_datatype(f.data_type())?;
                    Ok(StructField {
                        name: f.name().clone(),
                        data_type: dt,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(DataType::Struct(struct_fields))
        }
        ArrowDataType::Map(field, _) => {
            if let ArrowDataType::Struct(inner_fields) = field.data_type() {
                if inner_fields.len() == 2 {
                    let key_type = arrow_type_to_datatype(inner_fields[0].data_type())?;
                    let value_type = arrow_type_to_datatype(inner_fields[1].data_type())?;
                    return Ok(DataType::Map(Box::new(key_type), Box::new(value_type)));
                }
            }
            Err(Error::unsupported_feature(format!(
                "Unsupported Map structure: {:?}",
                arrow_type
            )))
        }
        ArrowDataType::Null => Ok(DataType::Unknown),
        _ => Err(Error::unsupported_feature(format!(
            "Unsupported Arrow type: {:?}",
            arrow_type
        ))),
    }
}

#[derive(Clone)]
struct ColumnMapping {
    parquet_idx: usize,
    #[allow(dead_code)]
    target_idx: usize,
    target_type: DataType,
}

fn build_column_mapping(
    arrow_schema: &arrow::datatypes::Schema,
    target_schema: &Schema,
) -> Result<Vec<Option<ColumnMapping>>> {
    let mut mappings: Vec<Option<ColumnMapping>> = Vec::with_capacity(target_schema.fields().len());

    for (target_idx, target_field) in target_schema.fields().iter().enumerate() {
        let target_name_lower = target_field.name.to_lowercase();
        let parquet_idx = arrow_schema
            .fields()
            .iter()
            .position(|f| f.name().to_lowercase() == target_name_lower);

        match parquet_idx {
            Some(idx) => {
                let arrow_type = arrow_schema.field(idx).data_type();
                validate_type_compatibility(arrow_type, &target_field.data_type)?;
                mappings.push(Some(ColumnMapping {
                    parquet_idx: idx,
                    target_idx,
                    target_type: target_field.data_type.clone(),
                }));
            }
            None => {
                if target_field.mode == FieldMode::Required {
                    return Err(Error::invalid_query(format!(
                        "Required column '{}' not found in parquet file",
                        target_field.name
                    )));
                }
                mappings.push(None);
            }
        }
    }

    Ok(mappings)
}

fn validate_type_compatibility(arrow_type: &ArrowDataType, target_type: &DataType) -> Result<()> {
    let can_coerce = matches!(
        (arrow_type, target_type),
        (ArrowDataType::Boolean, DataType::Bool)
            | (
                ArrowDataType::Int8
                    | ArrowDataType::Int16
                    | ArrowDataType::Int32
                    | ArrowDataType::Int64
                    | ArrowDataType::UInt8
                    | ArrowDataType::UInt16
                    | ArrowDataType::UInt32
                    | ArrowDataType::UInt64,
                DataType::Int64,
            )
            | (
                ArrowDataType::Int8
                    | ArrowDataType::Int16
                    | ArrowDataType::Int32
                    | ArrowDataType::Int64,
                DataType::Float64,
            )
            | (
                ArrowDataType::Float16 | ArrowDataType::Float32,
                DataType::Float32 | DataType::Float64,
            )
            | (ArrowDataType::Float64, DataType::Float64)
            | (
                ArrowDataType::Utf8 | ArrowDataType::LargeUtf8,
                DataType::String
            )
            | (
                ArrowDataType::Binary | ArrowDataType::LargeBinary,
                DataType::Bytes
            )
            | (
                ArrowDataType::Date32 | ArrowDataType::Date64,
                DataType::Date
            )
            | (
                ArrowDataType::Time32(_) | ArrowDataType::Time64(_),
                DataType::Time
            )
            | (
                ArrowDataType::Timestamp(_, _),
                DataType::DateTime | DataType::TimestampTz
            )
            | (
                ArrowDataType::Decimal128(_, _) | ArrowDataType::Decimal256(_, _),
                DataType::Numeric(_),
            )
            | (
                ArrowDataType::List(_) | ArrowDataType::LargeList(_),
                DataType::Array(_)
            )
            | (ArrowDataType::Struct(_), DataType::Struct(_))
            | (ArrowDataType::Map(_, _), DataType::Map(_, _))
    );

    if can_coerce {
        Ok(())
    } else {
        Err(Error::invalid_query(format!(
            "Cannot coerce Arrow type {:?} to {:?}",
            arrow_type, target_type
        )))
    }
}

fn convert_record_batch_to_rows(batch: &RecordBatch, schema: &Schema) -> Result<Vec<Row>> {
    let num_rows = batch.num_rows();
    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut values = Vec::with_capacity(schema.fields().len());
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let array = batch.column(col_idx);
            let value = extract_value_from_array(array, row_idx, &field.data_type)?;
            values.push(value);
        }
        rows.push(Row::from_values(values));
    }

    Ok(rows)
}

fn convert_batch_with_mapping(
    batch: &RecordBatch,
    target_schema: &Schema,
    column_mapping: &[Option<ColumnMapping>],
) -> Result<Vec<Row>> {
    let num_rows = batch.num_rows();
    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut values = Vec::with_capacity(target_schema.fields().len());
        for mapping in column_mapping.iter() {
            let value = match mapping {
                Some(m) => {
                    let array = batch.column(m.parquet_idx);
                    extract_value_from_array(array, row_idx, &m.target_type)?
                }
                None => Value::null(),
            };
            values.push(value);
        }
        rows.push(Row::from_values(values));
    }

    Ok(rows)
}

fn extract_value_from_array(array: &ArrayRef, idx: usize, target_type: &DataType) -> Result<Value> {
    if array.is_null(idx) {
        return Ok(Value::null());
    }

    match target_type {
        DataType::Bool => extract_bool(array, idx),
        DataType::Int64 => extract_int64(array, idx),
        DataType::Float32 => extract_float32(array, idx),
        DataType::Float64 => extract_float64(array, idx),
        DataType::String => extract_string(array, idx),
        DataType::Bytes => extract_bytes(array, idx),
        DataType::Date => extract_date(array, idx),
        DataType::Time => extract_time(array, idx),
        DataType::DateTime | DataType::TimestampTz => extract_datetime(array, idx),
        DataType::Numeric(_) => extract_numeric(array, idx),
        DataType::Array(element_type) => extract_array(array, idx, element_type),
        DataType::Struct(fields) => extract_struct(array, idx, fields),
        DataType::Map(key_type, value_type) => extract_map(array, idx, key_type, value_type),
        _ => Err(Error::unsupported_feature(format!(
            "Extraction for type {:?} not implemented",
            target_type
        ))),
    }
}

fn extract_bool(array: &ArrayRef, idx: usize) -> Result<Value> {
    let arr = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| Error::invalid_query("Expected BooleanArray".to_string()))?;
    Ok(Value::bool_val(arr.value(idx)))
}

fn extract_int64(array: &ArrayRef, idx: usize) -> Result<Value> {
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(Value::int64(arr.value(idx)));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(Value::int64(arr.value(idx) as i64));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int16Array>() {
        return Ok(Value::int64(arr.value(idx) as i64));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int8Array>() {
        return Ok(Value::int64(arr.value(idx) as i64));
    }
    if let Some(arr) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(Value::int64(arr.value(idx) as i64));
    }
    if let Some(arr) = array.as_any().downcast_ref::<UInt32Array>() {
        return Ok(Value::int64(arr.value(idx) as i64));
    }
    if let Some(arr) = array.as_any().downcast_ref::<UInt16Array>() {
        return Ok(Value::int64(arr.value(idx) as i64));
    }
    if let Some(arr) = array.as_any().downcast_ref::<UInt8Array>() {
        return Ok(Value::int64(arr.value(idx) as i64));
    }
    Err(Error::invalid_query(format!(
        "Expected integer array, got {:?}",
        array.data_type()
    )))
}

fn extract_float32(array: &ArrayRef, idx: usize) -> Result<Value> {
    if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(Value::float64(arr.value(idx) as f64));
    }
    Err(Error::invalid_query(format!(
        "Expected Float32Array, got {:?}",
        array.data_type()
    )))
}

fn extract_float64(array: &ArrayRef, idx: usize) -> Result<Value> {
    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(Value::float64(arr.value(idx)));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(Value::float64(arr.value(idx) as f64));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(Value::float64(arr.value(idx) as f64));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(Value::float64(arr.value(idx) as f64));
    }
    Err(Error::invalid_query(format!(
        "Expected float array, got {:?}",
        array.data_type()
    )))
}

fn extract_string(array: &ArrayRef, idx: usize) -> Result<Value> {
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Value::string(arr.value(idx).to_string()));
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Value::string(arr.value(idx).to_string()));
    }
    Err(Error::invalid_query(format!(
        "Expected string array, got {:?}",
        array.data_type()
    )))
}

fn extract_bytes(array: &ArrayRef, idx: usize) -> Result<Value> {
    if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(Value::bytes(arr.value(idx).to_vec()));
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(Value::bytes(arr.value(idx).to_vec()));
    }
    Err(Error::invalid_query(format!(
        "Expected binary array, got {:?}",
        array.data_type()
    )))
}

fn extract_date(array: &ArrayRef, idx: usize) -> Result<Value> {
    if let Some(arr) = array.as_any().downcast_ref::<Date32Array>() {
        let days = arr.value(idx);
        let date = NaiveDate::from_num_days_from_ce_opt(days + 719163)
            .ok_or_else(|| Error::invalid_query(format!("Invalid date value: {}", days)))?;
        return Ok(Value::date(date));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Date64Array>() {
        let millis = arr.value(idx);
        let days = (millis / 86400000) as i32;
        let date = NaiveDate::from_num_days_from_ce_opt(days + 719163)
            .ok_or_else(|| Error::invalid_query(format!("Invalid date value: {}", millis)))?;
        return Ok(Value::date(date));
    }
    Err(Error::invalid_query(format!(
        "Expected date array, got {:?}",
        array.data_type()
    )))
}

fn extract_time(array: &ArrayRef, idx: usize) -> Result<Value> {
    if let Some(arr) = array.as_any().downcast_ref::<Time64NanosecondArray>() {
        let nanos = arr.value(idx);
        let secs = (nanos / 1_000_000_000) as u32;
        let nano = (nanos % 1_000_000_000) as u32;
        let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
            .ok_or_else(|| Error::invalid_query(format!("Invalid time value: {}", nanos)))?;
        return Ok(Value::time(time));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Time64MicrosecondArray>() {
        let micros = arr.value(idx);
        let secs = (micros / 1_000_000) as u32;
        let nano = ((micros % 1_000_000) * 1000) as u32;
        let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
            .ok_or_else(|| Error::invalid_query(format!("Invalid time value: {}", micros)))?;
        return Ok(Value::time(time));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Time32MillisecondArray>() {
        let millis = arr.value(idx);
        let secs = (millis / 1000) as u32;
        let nano = ((millis % 1000) * 1_000_000) as u32;
        let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
            .ok_or_else(|| Error::invalid_query(format!("Invalid time value: {}", millis)))?;
        return Ok(Value::time(time));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Time32SecondArray>() {
        let secs = arr.value(idx) as u32;
        let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, 0)
            .ok_or_else(|| Error::invalid_query(format!("Invalid time value: {}", secs)))?;
        return Ok(Value::time(time));
    }
    Err(Error::invalid_query(format!(
        "Expected time array, got {:?}",
        array.data_type()
    )))
}

fn extract_datetime(array: &ArrayRef, idx: usize) -> Result<Value> {
    use chrono::{DateTime, Utc};

    let timestamp_micros =
        if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
            arr.value(idx) / 1000
        } else if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
            arr.value(idx)
        } else if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
            arr.value(idx) * 1000
        } else if let Some(arr) = array.as_any().downcast_ref::<TimestampSecondArray>() {
            arr.value(idx) * 1_000_000
        } else {
            return Err(Error::invalid_query(format!(
                "Expected timestamp array, got {:?}",
                array.data_type()
            )));
        };

    let secs = timestamp_micros / 1_000_000;
    let nsecs = ((timestamp_micros % 1_000_000) * 1000) as u32;
    let dt: DateTime<Utc> = DateTime::from_timestamp(secs, nsecs)
        .ok_or_else(|| Error::invalid_query(format!("Invalid timestamp: {}", timestamp_micros)))?;
    Ok(Value::datetime(dt))
}

fn extract_numeric(array: &ArrayRef, idx: usize) -> Result<Value> {
    if let Some(arr) = array.as_any().downcast_ref::<Decimal128Array>() {
        let value = arr.value(idx);
        let scale = arr.scale() as u32;
        let decimal = Decimal::from_i128_with_scale(value, scale);
        return Ok(Value::numeric(decimal));
    }
    Err(Error::invalid_query(format!(
        "Expected decimal array, got {:?}",
        array.data_type()
    )))
}

fn extract_array(array: &ArrayRef, idx: usize, element_type: &DataType) -> Result<Value> {
    let list_array = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| Error::invalid_query("Expected ListArray".to_string()))?;

    let values_array = list_array.value(idx);
    let mut elements = Vec::with_capacity(values_array.len());

    for i in 0..values_array.len() {
        let value = extract_value_from_array(&values_array, i, element_type)?;
        elements.push(value);
    }

    Ok(Value::array(elements))
}

fn extract_struct(array: &ArrayRef, idx: usize, fields: &[StructField]) -> Result<Value> {
    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::invalid_query("Expected StructArray".to_string()))?;

    let mut struct_values = IndexMap::new();
    for (col_idx, field) in fields.iter().enumerate() {
        let col_array = struct_array.column(col_idx);
        let value = extract_value_from_array(col_array, idx, &field.data_type)?;
        struct_values.insert(field.name.clone(), value);
    }

    Ok(Value::struct_val(struct_values))
}

fn extract_map(
    array: &ArrayRef,
    idx: usize,
    key_type: &DataType,
    value_type: &DataType,
) -> Result<Value> {
    let map_array = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| Error::invalid_query("Expected MapArray".to_string()))?;

    let entries = map_array.value(idx);
    let keys = entries.column(0);
    let values = entries.column(1);

    let mut map_entries = Vec::with_capacity(entries.len());
    for i in 0..entries.len() {
        let key = extract_value_from_array(keys, i, key_type)?;
        let val = extract_value_from_array(values, i, value_type)?;
        map_entries.push((key, val));
    }

    Ok(Value::map(map_entries))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use arrow::array::{Int32Builder, StringBuilder};
    use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
    use parquet::arrow::arrow_writer::ArrowWriter;
    use tempfile::NamedTempFile;

    use super::*;

    fn create_test_parquet_file() -> NamedTempFile {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));

        let mut id_builder = Int32Builder::new();
        let mut name_builder = StringBuilder::new();

        id_builder.append_value(1);
        name_builder.append_value("Alice");

        id_builder.append_value(2);
        name_builder.append_value("Bob");

        id_builder.append_value(3);
        name_builder.append_null();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(name_builder.finish()),
            ],
        )
        .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        {
            let file = temp_file.reopen().unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        temp_file
    }

    #[test]
    fn test_read_parquet_file() {
        let temp_file = create_test_parquet_file();
        let path = temp_file.path().to_str().unwrap();

        let (schema, rows) = read_parquet_file(path).unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.fields()[0].name, "id");
        assert_eq!(schema.fields()[1].name, "name");

        assert_eq!(rows.len(), 3);

        assert_eq!(rows[0].values()[0], Value::int64(1));
        assert_eq!(rows[0].values()[1], Value::string("Alice".to_string()));

        assert_eq!(rows[1].values()[0], Value::int64(2));
        assert_eq!(rows[1].values()[1], Value::string("Bob".to_string()));

        assert_eq!(rows[2].values()[0], Value::int64(3));
        assert!(rows[2].values()[1].is_null());
    }

    #[test]
    fn test_arrow_type_mapping() {
        assert_eq!(
            arrow_type_to_datatype(&ArrowDataType::Boolean).unwrap(),
            DataType::Bool
        );
        assert_eq!(
            arrow_type_to_datatype(&ArrowDataType::Int32).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            arrow_type_to_datatype(&ArrowDataType::Float64).unwrap(),
            DataType::Float64
        );
        assert_eq!(
            arrow_type_to_datatype(&ArrowDataType::Utf8).unwrap(),
            DataType::String
        );
        assert_eq!(
            arrow_type_to_datatype(&ArrowDataType::Date32).unwrap(),
            DataType::Date
        );
    }

    #[test]
    fn test_normalize_uri() {
        assert_eq!(
            normalize_uri_to_path("file:///tmp/test.parquet").unwrap(),
            "/tmp/test.parquet"
        );
        assert_eq!(
            normalize_uri_to_path("/tmp/test.parquet").unwrap(),
            "/tmp/test.parquet"
        );
        assert_eq!(
            normalize_uri_to_path("./test.parquet").unwrap(),
            "./test.parquet"
        );
        assert!(normalize_uri_to_path("gs://bucket/file.parquet").is_err());
        assert!(normalize_uri_to_path("s3://bucket/file.parquet").is_err());
    }
}
