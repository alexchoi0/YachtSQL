use std::fs::File;
use std::io::{BufWriter, Write};

use chrono::Timelike;
use serde_json::{Map, Value as JsonValue};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_storage::Schema;

use crate::Table;

pub fn write_json_file(path: &str, table: &Table) -> Result<usize> {
    let path = normalize_uri_to_path(path)?;
    let file = File::create(&path)
        .map_err(|e| Error::internal(format!("Failed to create file {}: {}", path, e)))?;

    let mut writer = BufWriter::new(file);
    let num_rows = table.num_rows();
    let schema = table.schema();

    for row_idx in 0..num_rows {
        let row = table.row(row_idx)?;
        let json_obj = row_to_json_object(row.values(), schema)?;
        let json_line = serde_json::to_string(&json_obj)
            .map_err(|e| Error::internal(format!("Failed to serialize row {}: {}", row_idx, e)))?;
        writeln!(writer, "{}", json_line)
            .map_err(|e| Error::internal(format!("Failed to write row {}: {}", row_idx, e)))?;
    }

    writer
        .flush()
        .map_err(|e| Error::internal(format!("Failed to flush writer: {}", e)))?;

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

fn row_to_json_object(values: &[Value], schema: &Schema) -> Result<JsonValue> {
    let mut obj = Map::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        let value = &values[idx];
        let json_value = value_to_json(value, &field.data_type)?;
        obj.insert(field.name.clone(), json_value);
    }
    Ok(JsonValue::Object(obj))
}

fn value_to_json(value: &Value, data_type: &DataType) -> Result<JsonValue> {
    if value.is_null() {
        return Ok(JsonValue::Null);
    }

    match data_type {
        DataType::Bool => {
            let b = value.as_bool().unwrap_or(false);
            Ok(JsonValue::Bool(b))
        }
        DataType::Int64 => {
            let i = value.as_i64().unwrap_or(0);
            Ok(JsonValue::Number(serde_json::Number::from(i)))
        }
        DataType::Float32 | DataType::Float64 => {
            let f = value.as_f64().unwrap_or(0.0);
            if f.is_nan() || f.is_infinite() {
                Ok(JsonValue::String(f.to_string()))
            } else {
                Ok(serde_json::Number::from_f64(f)
                    .map(JsonValue::Number)
                    .unwrap_or_else(|| JsonValue::String(f.to_string())))
            }
        }
        DataType::String => {
            let s = value.as_str().unwrap_or("");
            Ok(JsonValue::String(s.to_string()))
        }
        DataType::Bytes => {
            if let Some(bytes) = value.as_bytes() {
                let encoded =
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes);
                Ok(JsonValue::String(encoded))
            } else {
                Ok(JsonValue::Null)
            }
        }
        DataType::Date => {
            if let Some(date) = value.as_date() {
                Ok(JsonValue::String(date.format("%Y-%m-%d").to_string()))
            } else {
                Ok(JsonValue::Null)
            }
        }
        DataType::Time => {
            if let Some(time) = value.as_time() {
                let formatted = if time.nanosecond() > 0 {
                    time.format("%H:%M:%S%.f").to_string()
                } else {
                    time.format("%H:%M:%S").to_string()
                };
                Ok(JsonValue::String(formatted))
            } else {
                Ok(JsonValue::Null)
            }
        }
        DataType::DateTime => {
            if let Some(dt) = value.as_datetime() {
                Ok(JsonValue::String(
                    dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
                ))
            } else {
                Ok(JsonValue::Null)
            }
        }
        DataType::TimestampTz => {
            if let Some(dt) = value.as_datetime() {
                Ok(JsonValue::String(dt.to_rfc3339()))
            } else {
                Ok(JsonValue::Null)
            }
        }
        DataType::Numeric(_) => {
            if let Some(dec) = value.as_numeric() {
                Ok(JsonValue::String(dec.to_string()))
            } else {
                Ok(JsonValue::Null)
            }
        }
        DataType::Array(element_type) => {
            if let Some(arr) = value.as_array() {
                let json_elements: Vec<JsonValue> = arr
                    .iter()
                    .map(|elem| value_to_json(elem, element_type))
                    .collect::<Result<Vec<_>>>()?;
                Ok(JsonValue::Array(json_elements))
            } else {
                Ok(JsonValue::Null)
            }
        }
        DataType::Struct(fields) => {
            if let Some(struct_map) = value.as_struct() {
                let mut obj = Map::new();
                for field in fields {
                    let field_value = struct_map
                        .get(&field.name)
                        .cloned()
                        .unwrap_or(Value::null());
                    let json_value = value_to_json(&field_value, &field.data_type)?;
                    obj.insert(field.name.clone(), json_value);
                }
                Ok(JsonValue::Object(obj))
            } else {
                Ok(JsonValue::Null)
            }
        }
        DataType::Map(key_type, value_type) => {
            if let Some(map) = value.as_map() {
                let mut obj = Map::new();
                for (k, v) in map {
                    let key_str = match key_type.as_ref() {
                        DataType::String => k.as_str().unwrap_or("").to_string(),
                        DataType::Int64 => k.as_i64().map(|i| i.to_string()).unwrap_or_default(),
                        _ => k.to_string(),
                    };
                    let json_value = value_to_json(v, value_type)?;
                    obj.insert(key_str, json_value);
                }
                Ok(JsonValue::Object(obj))
            } else {
                Ok(JsonValue::Null)
            }
        }
        DataType::Json => {
            if let Some(json) = value.as_json() {
                Ok(json.clone())
            } else {
                Ok(JsonValue::Null)
            }
        }
        DataType::Uuid => {
            if let Some(uuid) = value.as_uuid() {
                Ok(JsonValue::String(uuid.to_string()))
            } else {
                Ok(JsonValue::Null)
            }
        }
        DataType::Interval => {
            if let Some(interval) = value.as_interval() {
                Ok(JsonValue::String(format!("{:?}", interval)))
            } else {
                Ok(JsonValue::Null)
            }
        }
        _ => Err(Error::unsupported_feature(format!(
            "Cannot export {:?} to JSON",
            data_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;
    use yachtsql_storage::{Field, Row};

    use super::*;
    use crate::query_executor::execution::json_reader;

    #[test]
    fn test_write_and_read_json() {
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

        let rows_written = write_json_file(path, &table).unwrap();
        assert_eq!(rows_written, 3);

        let (read_schema, read_rows) = json_reader::read_json_file(path).unwrap();
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

        let table = Table::from_rows(schema.clone(), rows).unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        write_json_file(path, &table).unwrap();

        let read_rows = json_reader::load_json_into_table(path, &schema).unwrap();
        assert_eq!(read_rows.len(), 2);

        let date1 = read_rows[0].values()[1].as_date().unwrap();
        assert_eq!(date1, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
    }

    #[test]
    fn test_write_with_arrays() {
        let schema = Schema::from_fields(vec![
            Field::required("id".to_string(), DataType::Int64),
            Field::nullable(
                "tags".to_string(),
                DataType::Array(Box::new(DataType::String)),
            ),
        ]);

        let rows = vec![Row::from_values(vec![
            Value::int64(1),
            Value::array(vec![
                Value::string("a".to_string()),
                Value::string("b".to_string()),
            ]),
        ])];

        let table = Table::from_rows(schema, rows).unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let rows_written = write_json_file(path, &table).unwrap();
        assert_eq!(rows_written, 1);

        let content = std::fs::read_to_string(path).unwrap();
        assert!(content.contains("\"tags\":[\"a\",\"b\"]"));
    }

    #[test]
    fn test_normalize_uri() {
        assert_eq!(
            normalize_uri_to_path("file:///tmp/test.json").unwrap(),
            "/tmp/test.json"
        );
        assert_eq!(
            normalize_uri_to_path("/tmp/test.json").unwrap(),
            "/tmp/test.json"
        );
        assert!(normalize_uri_to_path("gs://bucket/file.json").is_err());
        assert!(normalize_uri_to_path("s3://bucket/file.json").is_err());
    }
}
