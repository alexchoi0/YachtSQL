use std::fs::File;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int64Builder, ListBuilder, MapBuilder, StringBuilder, StructBuilder, Time64NanosecondBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Fields, Schema as ArrowSchema, TimeUnit,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_storage::Schema;

use crate::Table;

pub fn write_parquet_file(path: &str, table: &Table, compression: Option<&str>) -> Result<usize> {
    let path = normalize_uri_to_path(path)?;

    let arrow_schema = Arc::new(schema_to_arrow(table.schema())?);
    let record_batch = table_to_record_batch(table, &arrow_schema)?;
    let num_rows = record_batch.num_rows();

    let file = File::create(&path)
        .map_err(|e| Error::internal(format!("Failed to create file {}: {}", path, e)))?;

    let compression = match compression {
        Some("SNAPPY") | Some("snappy") => Compression::SNAPPY,
        Some("GZIP") | Some("gzip") => Compression::GZIP(Default::default()),
        Some("LZ4") | Some("lz4") => Compression::LZ4,
        Some("ZSTD") | Some("zstd") => Compression::ZSTD(Default::default()),
        Some("NONE") | Some("none") | None => Compression::UNCOMPRESSED,
        Some(other) => {
            return Err(Error::invalid_query(format!(
                "Unsupported compression: {}. Supported: SNAPPY, GZIP, LZ4, ZSTD, NONE",
                other
            )));
        }
    };

    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();

    let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props))
        .map_err(|e| Error::internal(format!("Failed to create parquet writer: {}", e)))?;

    writer
        .write(&record_batch)
        .map_err(|e| Error::internal(format!("Failed to write parquet data: {}", e)))?;

    writer
        .close()
        .map_err(|e| Error::internal(format!("Failed to close parquet writer: {}", e)))?;

    Ok(num_rows)
}

pub fn normalize_uri_to_path(uri: &str) -> Result<String> {
    if uri.starts_with("gs://") || uri.starts_with("s3://") || uri.starts_with("az://") {
        return Err(Error::unsupported_feature(
            "Cloud storage URIs (gs://, s3://, az://) are not yet supported. Use local file paths.",
        ));
    }

    if let Some(path) = uri.strip_prefix("file://") {
        Ok(path.to_string())
    } else {
        Ok(uri.to_string())
    }
}

fn schema_to_arrow(schema: &Schema) -> Result<ArrowSchema> {
    let fields: Vec<ArrowField> = schema
        .fields()
        .iter()
        .map(|f| {
            let arrow_type = datatype_to_arrow(&f.data_type)?;
            let nullable = f.mode == yachtsql_storage::FieldMode::Nullable;
            Ok(ArrowField::new(&f.name, arrow_type, nullable))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ArrowSchema::new(fields))
}

fn datatype_to_arrow(dt: &DataType) -> Result<ArrowDataType> {
    match dt {
        DataType::Bool => Ok(ArrowDataType::Boolean),
        DataType::Int64 => Ok(ArrowDataType::Int64),
        DataType::Float32 => Ok(ArrowDataType::Float32),
        DataType::Float64 => Ok(ArrowDataType::Float64),
        DataType::String => Ok(ArrowDataType::Utf8),
        DataType::Bytes => Ok(ArrowDataType::Binary),
        DataType::Date => Ok(ArrowDataType::Date32),
        DataType::Time => Ok(ArrowDataType::Time64(TimeUnit::Nanosecond)),
        DataType::DateTime => Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, None)),
        DataType::TimestampTz => Ok(ArrowDataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        DataType::Numeric(params) => {
            let (precision, scale) = params.map(|(p, s)| (p, s as i8)).unwrap_or((38, 9));
            Ok(ArrowDataType::Decimal128(precision, scale))
        }
        DataType::Array(element_type) => {
            let element_arrow = datatype_to_arrow(element_type)?;
            Ok(ArrowDataType::List(Arc::new(ArrowField::new(
                "item",
                element_arrow,
                true,
            ))))
        }
        DataType::Struct(fields) => {
            let arrow_fields: Vec<ArrowField> = fields
                .iter()
                .map(|f| {
                    let arrow_type = datatype_to_arrow(&f.data_type)?;
                    Ok(ArrowField::new(&f.name, arrow_type, true))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ArrowDataType::Struct(Fields::from(arrow_fields)))
        }
        DataType::Map(key_type, value_type) => {
            let key_arrow = datatype_to_arrow(key_type)?;
            let value_arrow = datatype_to_arrow(value_type)?;
            Ok(ArrowDataType::Map(
                Arc::new(ArrowField::new(
                    "entries",
                    ArrowDataType::Struct(Fields::from(vec![
                        ArrowField::new("key", key_arrow, false),
                        ArrowField::new("value", value_arrow, true),
                    ])),
                    false,
                )),
                false,
            ))
        }
        DataType::Json => Ok(ArrowDataType::Utf8),
        DataType::Uuid => Ok(ArrowDataType::Utf8),
        DataType::Interval => Ok(ArrowDataType::Utf8),
        _ => Err(Error::unsupported_feature(format!(
            "Cannot convert {:?} to Arrow type",
            dt
        ))),
    }
}

fn table_to_record_batch(table: &Table, arrow_schema: &Arc<ArrowSchema>) -> Result<RecordBatch> {
    let num_rows = table.num_rows();
    let num_cols = table.schema().fields().len();

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let field = &table.schema().fields()[col_idx];
        let arrow_field = arrow_schema.field(col_idx);
        let array =
            build_array_for_column(table, col_idx, &field.data_type, arrow_field, num_rows)?;
        arrays.push(array);
    }

    RecordBatch::try_new(arrow_schema.clone(), arrays)
        .map_err(|e| Error::internal(format!("Failed to create record batch: {}", e)))
}

fn build_array_for_column(
    table: &Table,
    col_idx: usize,
    data_type: &DataType,
    _arrow_field: &ArrowField,
    num_rows: usize,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Bool => build_bool_array(table, col_idx, num_rows),
        DataType::Int64 => build_int64_array(table, col_idx, num_rows),
        DataType::Float32 => build_float32_array(table, col_idx, num_rows),
        DataType::Float64 => build_float64_array(table, col_idx, num_rows),
        DataType::String | DataType::Json | DataType::Uuid | DataType::Interval => {
            build_string_array(table, col_idx, num_rows)
        }
        DataType::Bytes => build_bytes_array(table, col_idx, num_rows),
        DataType::Date => build_date_array(table, col_idx, num_rows),
        DataType::Time => build_time_array(table, col_idx, num_rows),
        DataType::DateTime | DataType::TimestampTz => {
            build_timestamp_array(table, col_idx, num_rows)
        }
        DataType::Numeric(_) => build_decimal_array(table, col_idx, num_rows),
        DataType::Array(element_type) => build_list_array(table, col_idx, element_type, num_rows),
        DataType::Struct(fields) => build_struct_array(table, col_idx, fields, num_rows),
        DataType::Map(key_type, value_type) => {
            build_map_array(table, col_idx, key_type, value_type, num_rows)
        }
        _ => Err(Error::unsupported_feature(format!(
            "Cannot export {:?} to parquet",
            data_type
        ))),
    }
}

fn build_bool_array(table: &Table, col_idx: usize, num_rows: usize) -> Result<ArrayRef> {
    let mut builder = BooleanBuilder::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let row = table.row(row_idx)?;
        let value = &row.values()[col_idx];
        if value.is_null() {
            builder.append_null();
        } else {
            builder.append_value(value.as_bool().unwrap_or(false));
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_int64_array(table: &Table, col_idx: usize, num_rows: usize) -> Result<ArrayRef> {
    let mut builder = Int64Builder::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let row = table.row(row_idx)?;
        let value = &row.values()[col_idx];
        if value.is_null() {
            builder.append_null();
        } else {
            builder.append_value(value.as_i64().unwrap_or(0));
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_float32_array(table: &Table, col_idx: usize, num_rows: usize) -> Result<ArrayRef> {
    let mut builder = Float32Builder::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let row = table.row(row_idx)?;
        let value = &row.values()[col_idx];
        if value.is_null() {
            builder.append_null();
        } else {
            builder.append_value(value.as_f64().unwrap_or(0.0) as f32);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_float64_array(table: &Table, col_idx: usize, num_rows: usize) -> Result<ArrayRef> {
    let mut builder = Float64Builder::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let row = table.row(row_idx)?;
        let value = &row.values()[col_idx];
        if value.is_null() {
            builder.append_null();
        } else {
            builder.append_value(value.as_f64().unwrap_or(0.0));
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_string_array(table: &Table, col_idx: usize, num_rows: usize) -> Result<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
    for row_idx in 0..num_rows {
        let row = table.row(row_idx)?;
        let value = &row.values()[col_idx];
        if value.is_null() {
            builder.append_null();
        } else {
            let s = value.as_str().unwrap_or("");
            builder.append_value(s);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_bytes_array(table: &Table, col_idx: usize, num_rows: usize) -> Result<ArrayRef> {
    let mut builder = BinaryBuilder::with_capacity(num_rows, num_rows * 32);
    for row_idx in 0..num_rows {
        let row = table.row(row_idx)?;
        let value = &row.values()[col_idx];
        if value.is_null() {
            builder.append_null();
        } else {
            let bytes = value.as_bytes().unwrap_or(&[]);
            builder.append_value(bytes);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_date_array(table: &Table, col_idx: usize, num_rows: usize) -> Result<ArrayRef> {
    use chrono::Datelike;
    let mut builder = Date32Builder::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let row = table.row(row_idx)?;
        let value = &row.values()[col_idx];
        if value.is_null() {
            builder.append_null();
        } else if let Some(date) = value.as_date() {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let days = (date - epoch).num_days() as i32;
            builder.append_value(days);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_time_array(table: &Table, col_idx: usize, num_rows: usize) -> Result<ArrayRef> {
    use chrono::Timelike;
    let mut builder = Time64NanosecondBuilder::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let row = table.row(row_idx)?;
        let value = &row.values()[col_idx];
        if value.is_null() {
            builder.append_null();
        } else if let Some(time) = value.as_time() {
            let nanos =
                time.num_seconds_from_midnight() as i64 * 1_000_000_000 + time.nanosecond() as i64;
            builder.append_value(nanos);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_timestamp_array(table: &Table, col_idx: usize, num_rows: usize) -> Result<ArrayRef> {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let row = table.row(row_idx)?;
        let value = &row.values()[col_idx];
        if value.is_null() {
            builder.append_null();
        } else if let Some(dt) = value.as_datetime() {
            let micros = dt.timestamp_micros();
            builder.append_value(micros);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_decimal_array(table: &Table, col_idx: usize, num_rows: usize) -> Result<ArrayRef> {
    use arrow::array::Decimal128Builder;

    let mut builder = Decimal128Builder::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let row = table.row(row_idx)?;
        let value = &row.values()[col_idx];
        if value.is_null() {
            builder.append_null();
        } else if let Some(dec) = value.as_numeric() {
            let mantissa = dec.mantissa();
            builder.append_value(mantissa);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(
        builder
            .finish()
            .with_precision_and_scale(38, 9)
            .map_err(|e| Error::internal(format!("Failed to set decimal precision: {}", e)))?,
    ))
}

fn build_list_array(
    table: &Table,
    col_idx: usize,
    element_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    match element_type {
        DataType::Int64 => {
            let mut builder = ListBuilder::new(Int64Builder::new());
            for row_idx in 0..num_rows {
                let row = table.row(row_idx)?;
                let value = &row.values()[col_idx];
                if value.is_null() {
                    builder.append_null();
                } else if let Some(arr) = value.as_array() {
                    for elem in arr {
                        if elem.is_null() {
                            builder.values().append_null();
                        } else {
                            builder.values().append_value(elem.as_i64().unwrap_or(0));
                        }
                    }
                    builder.append(true);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::String => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            for row_idx in 0..num_rows {
                let row = table.row(row_idx)?;
                let value = &row.values()[col_idx];
                if value.is_null() {
                    builder.append_null();
                } else if let Some(arr) = value.as_array() {
                    for elem in arr {
                        if elem.is_null() {
                            builder.values().append_null();
                        } else {
                            builder.values().append_value(elem.as_str().unwrap_or(""));
                        }
                    }
                    builder.append(true);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = ListBuilder::new(Float64Builder::new());
            for row_idx in 0..num_rows {
                let row = table.row(row_idx)?;
                let value = &row.values()[col_idx];
                if value.is_null() {
                    builder.append_null();
                } else if let Some(arr) = value.as_array() {
                    for elem in arr {
                        if elem.is_null() {
                            builder.values().append_null();
                        } else {
                            builder.values().append_value(elem.as_f64().unwrap_or(0.0));
                        }
                    }
                    builder.append(true);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(Error::unsupported_feature(format!(
            "Array element type {:?} not yet supported for parquet export",
            element_type
        ))),
    }
}

fn build_struct_array(
    table: &Table,
    col_idx: usize,
    fields: &[yachtsql_common::types::StructField],
    num_rows: usize,
) -> Result<ArrayRef> {
    let arrow_fields: Vec<ArrowField> = fields
        .iter()
        .map(|f| {
            let arrow_type = datatype_to_arrow(&f.data_type)?;
            Ok(ArrowField::new(&f.name, arrow_type, true))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut field_builders: Vec<Box<dyn arrow::array::ArrayBuilder>> = Vec::new();
    for field in fields {
        let builder: Box<dyn arrow::array::ArrayBuilder> = match &field.data_type {
            DataType::Int64 => Box::new(Int64Builder::new()),
            DataType::Float64 => Box::new(Float64Builder::new()),
            DataType::String => Box::new(StringBuilder::new()),
            DataType::Bool => Box::new(BooleanBuilder::new()),
            _ => {
                return Err(Error::unsupported_feature(format!(
                    "Struct field type {:?} not yet supported for parquet export",
                    field.data_type
                )));
            }
        };
        field_builders.push(builder);
    }

    let mut struct_builder = StructBuilder::new(Fields::from(arrow_fields), field_builders);

    for row_idx in 0..num_rows {
        let row = table.row(row_idx)?;
        let value = &row.values()[col_idx];

        if value.is_null() {
            for field_idx in 0..fields.len() {
                match &fields[field_idx].data_type {
                    DataType::Int64 => struct_builder
                        .field_builder::<Int64Builder>(field_idx)
                        .unwrap()
                        .append_null(),
                    DataType::Float64 => struct_builder
                        .field_builder::<Float64Builder>(field_idx)
                        .unwrap()
                        .append_null(),
                    DataType::String => struct_builder
                        .field_builder::<StringBuilder>(field_idx)
                        .unwrap()
                        .append_null(),
                    DataType::Bool => struct_builder
                        .field_builder::<BooleanBuilder>(field_idx)
                        .unwrap()
                        .append_null(),
                    _ => {}
                }
            }
            struct_builder.append_null();
        } else if let Some(struct_map) = value.as_struct() {
            for (field_idx, field) in fields.iter().enumerate() {
                let field_value = struct_map.get(&field.name);
                match &field.data_type {
                    DataType::Int64 => {
                        let builder = struct_builder
                            .field_builder::<Int64Builder>(field_idx)
                            .unwrap();
                        if let Some(v) = field_value {
                            if v.is_null() {
                                builder.append_null();
                            } else {
                                builder.append_value(v.as_i64().unwrap_or(0));
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    DataType::Float64 => {
                        let builder = struct_builder
                            .field_builder::<Float64Builder>(field_idx)
                            .unwrap();
                        if let Some(v) = field_value {
                            if v.is_null() {
                                builder.append_null();
                            } else {
                                builder.append_value(v.as_f64().unwrap_or(0.0));
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    DataType::String => {
                        let builder = struct_builder
                            .field_builder::<StringBuilder>(field_idx)
                            .unwrap();
                        if let Some(v) = field_value {
                            if v.is_null() {
                                builder.append_null();
                            } else {
                                builder.append_value(v.as_str().unwrap_or(""));
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    DataType::Bool => {
                        let builder = struct_builder
                            .field_builder::<BooleanBuilder>(field_idx)
                            .unwrap();
                        if let Some(v) = field_value {
                            if v.is_null() {
                                builder.append_null();
                            } else {
                                builder.append_value(v.as_bool().unwrap_or(false));
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => {}
                }
            }
            struct_builder.append(true);
        } else {
            struct_builder.append_null();
        }
    }

    Ok(Arc::new(struct_builder.finish()))
}

fn build_map_array(
    table: &Table,
    col_idx: usize,
    key_type: &DataType,
    value_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    match (key_type, value_type) {
        (DataType::String, DataType::String) => {
            let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
            for row_idx in 0..num_rows {
                let row = table.row(row_idx)?;
                let value = &row.values()[col_idx];
                if value.is_null() {
                    builder
                        .append(false)
                        .map_err(|e| Error::internal(format!("Map builder error: {}", e)))?;
                } else if let Some(map) = value.as_map() {
                    for (k, v) in map {
                        builder.keys().append_value(k.as_str().unwrap_or(""));
                        if v.is_null() {
                            builder.values().append_null();
                        } else {
                            builder.values().append_value(v.as_str().unwrap_or(""));
                        }
                    }
                    builder
                        .append(true)
                        .map_err(|e| Error::internal(format!("Map builder error: {}", e)))?;
                } else {
                    builder
                        .append(false)
                        .map_err(|e| Error::internal(format!("Map builder error: {}", e)))?;
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (DataType::String, DataType::Int64) => {
            let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
            for row_idx in 0..num_rows {
                let row = table.row(row_idx)?;
                let value = &row.values()[col_idx];
                if value.is_null() {
                    builder
                        .append(false)
                        .map_err(|e| Error::internal(format!("Map builder error: {}", e)))?;
                } else if let Some(map) = value.as_map() {
                    for (k, v) in map {
                        builder.keys().append_value(k.as_str().unwrap_or(""));
                        if v.is_null() {
                            builder.values().append_null();
                        } else {
                            builder.values().append_value(v.as_i64().unwrap_or(0));
                        }
                    }
                    builder
                        .append(true)
                        .map_err(|e| Error::internal(format!("Map builder error: {}", e)))?;
                } else {
                    builder
                        .append(false)
                        .map_err(|e| Error::internal(format!("Map builder error: {}", e)))?;
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(Error::unsupported_feature(format!(
            "Map<{:?}, {:?}> not yet supported for parquet export",
            key_type, value_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;
    use yachtsql_storage::{Field, FieldMode, Row};

    use super::*;
    use crate::query_executor::execution::parquet_reader;

    #[test]
    fn test_write_and_read_parquet() {
        let schema = Schema::from_fields(vec![
            Field::required("id".to_string(), DataType::Int64),
            Field::nullable("name".to_string(), DataType::String),
            Field::nullable("value".to_string(), DataType::Float64),
        ]);

        let rows = vec![
            Row::from_values(vec![
                Value::int64(1),
                Value::string("Alice".to_string()),
                Value::float64(100.5),
            ]),
            Row::from_values(vec![
                Value::int64(2),
                Value::string("Bob".to_string()),
                Value::float64(200.75),
            ]),
            Row::from_values(vec![Value::int64(3), Value::null(), Value::float64(300.0)]),
        ];

        let table = Table::from_rows(schema, rows).unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let rows_written = write_parquet_file(path, &table, None).unwrap();
        assert_eq!(rows_written, 3);

        let (read_schema, read_rows) = parquet_reader::read_parquet_file(path).unwrap();
        assert_eq!(read_schema.fields().len(), 3);
        assert_eq!(read_rows.len(), 3);

        assert_eq!(read_rows[0].values()[0], Value::int64(1));
        assert_eq!(read_rows[0].values()[1], Value::string("Alice".to_string()));
        assert_eq!(read_rows[0].values()[2], Value::float64(100.5));

        assert_eq!(read_rows[2].values()[0], Value::int64(3));
        assert!(read_rows[2].values()[1].is_null());
    }

    #[test]
    fn test_roundtrip_with_dates() {
        use chrono::NaiveDate;

        let schema = Schema::from_fields(vec![
            Field::required("id".to_string(), DataType::Int64),
            Field::nullable("date".to_string(), DataType::Date),
        ]);

        let rows = vec![
            Row::from_values(vec![
                Value::int64(1),
                Value::date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()),
            ]),
            Row::from_values(vec![
                Value::int64(2),
                Value::date(NaiveDate::from_ymd_opt(2024, 6, 30).unwrap()),
            ]),
        ];

        let table = Table::from_rows(schema, rows).unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        write_parquet_file(path, &table, None).unwrap();

        let (_, read_rows) = parquet_reader::read_parquet_file(path).unwrap();
        assert_eq!(read_rows.len(), 2);

        let date1 = read_rows[0].values()[1].as_date().unwrap();
        assert_eq!(date1, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
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
        assert!(normalize_uri_to_path("gs://bucket/file.parquet").is_err());
        assert!(normalize_uri_to_path("s3://bucket/file.parquet").is_err());
    }
}
