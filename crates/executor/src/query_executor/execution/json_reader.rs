use std::fs::File;
use std::io::{BufRead, BufReader};

use indexmap::IndexMap;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, StructField, Value};
use yachtsql_storage::{Field, FieldMode, Row, Schema};

pub fn read_json_file(path: &str) -> Result<(Schema, Vec<Row>)> {
    let normalized_path = normalize_uri_to_path(path)?;
    let file = File::open(&normalized_path)
        .map_err(|e| Error::internal(format!("Failed to open JSON file '{}': {}", path, e)))?;

    let reader = BufReader::new(file);
    let mut rows = Vec::new();
    let mut schema: Option<Schema> = None;

    for (line_num, line_result) in reader.lines().enumerate() {
        let line = line_result.map_err(|e| {
            Error::internal(format!(
                "Failed to read line {} from '{}': {}",
                line_num + 1,
                path,
                e
            ))
        })?;

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let json_value: serde_json::Value = serde_json::from_str(trimmed).map_err(|e| {
            Error::internal(format!(
                "Failed to parse JSON at line {} in '{}': {}",
                line_num + 1,
                path,
                e
            ))
        })?;

        let obj = json_value.as_object().ok_or_else(|| {
            Error::invalid_query(format!(
                "Expected JSON object at line {} in '{}', got {:?}",
                line_num + 1,
                path,
                json_value
            ))
        })?;

        if schema.is_none() {
            schema = Some(infer_schema_from_json(obj)?);
        }

        let row = json_object_to_row(obj, schema.as_ref().unwrap())?;
        rows.push(row);
    }

    let schema = schema.unwrap_or_else(|| Schema::from_fields(vec![]));
    Ok((schema, rows))
}

pub fn load_json_into_table(path: &str, target_schema: &Schema) -> Result<Vec<Row>> {
    let normalized_path = normalize_uri_to_path(path)?;
    let file = File::open(&normalized_path)
        .map_err(|e| Error::internal(format!("Failed to open JSON file '{}': {}", path, e)))?;

    let reader = BufReader::new(file);
    let mut rows = Vec::new();

    for (line_num, line_result) in reader.lines().enumerate() {
        let line = line_result.map_err(|e| {
            Error::internal(format!(
                "Failed to read line {} from '{}': {}",
                line_num + 1,
                path,
                e
            ))
        })?;

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let json_value: serde_json::Value = serde_json::from_str(trimmed).map_err(|e| {
            Error::internal(format!(
                "Failed to parse JSON at line {} in '{}': {}",
                line_num + 1,
                path,
                e
            ))
        })?;

        let obj = json_value.as_object().ok_or_else(|| {
            Error::invalid_query(format!(
                "Expected JSON object at line {} in '{}', got {:?}",
                line_num + 1,
                path,
                json_value
            ))
        })?;

        let row = json_object_to_row_with_schema(obj, target_schema)?;
        rows.push(row);
    }

    Ok(rows)
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

fn infer_schema_from_json(obj: &serde_json::Map<String, serde_json::Value>) -> Result<Schema> {
    let fields: Vec<Field> = obj
        .iter()
        .map(|(key, value)| {
            let data_type = infer_type_from_json_value(value);
            Field::nullable(key.clone(), data_type)
        })
        .collect();
    Ok(Schema::from_fields(fields))
}

fn infer_type_from_json_value(value: &serde_json::Value) -> DataType {
    match value {
        serde_json::Value::Null => DataType::String,
        serde_json::Value::Bool(_) => DataType::Bool,
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                DataType::Int64
            } else {
                DataType::Float64
            }
        }
        serde_json::Value::String(_) => DataType::String,
        serde_json::Value::Array(arr) => {
            if arr.is_empty() {
                DataType::Array(Box::new(DataType::String))
            } else {
                let element_type = infer_type_from_json_value(&arr[0]);
                DataType::Array(Box::new(element_type))
            }
        }
        serde_json::Value::Object(obj) => {
            let fields: Vec<StructField> = obj
                .iter()
                .map(|(k, v)| StructField {
                    name: k.clone(),
                    data_type: infer_type_from_json_value(v),
                })
                .collect();
            DataType::Struct(fields)
        }
    }
}

fn json_object_to_row(
    obj: &serde_json::Map<String, serde_json::Value>,
    schema: &Schema,
) -> Result<Row> {
    let mut values = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let value = if let Some(json_val) = obj.get(&field.name) {
            json_value_to_value(json_val, &field.data_type)?
        } else {
            Value::null()
        };
        values.push(value);
    }
    Ok(Row::from_values(values))
}

fn json_object_to_row_with_schema(
    obj: &serde_json::Map<String, serde_json::Value>,
    schema: &Schema,
) -> Result<Row> {
    let mut values = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let field_name_lower = field.name.to_lowercase();
        let json_val = obj
            .iter()
            .find(|(k, _)| k.to_lowercase() == field_name_lower)
            .map(|(_, v)| v);

        let value = if let Some(jv) = json_val {
            json_value_to_value(jv, &field.data_type)?
        } else if field.mode == FieldMode::Required {
            return Err(Error::invalid_query(format!(
                "Required field '{}' not found in JSON object",
                field.name
            )));
        } else {
            Value::null()
        };
        values.push(value);
    }
    Ok(Row::from_values(values))
}

fn json_value_to_value(json_val: &serde_json::Value, target_type: &DataType) -> Result<Value> {
    if json_val.is_null() {
        return Ok(Value::null());
    }

    match target_type {
        DataType::Bool => {
            let b = json_val.as_bool().ok_or_else(|| {
                Error::invalid_query(format!("Expected boolean, got {:?}", json_val))
            })?;
            Ok(Value::bool_val(b))
        }
        DataType::Int64 => {
            if let Some(i) = json_val.as_i64() {
                Ok(Value::int64(i))
            } else if let Some(f) = json_val.as_f64() {
                Ok(Value::int64(f as i64))
            } else if let Some(s) = json_val.as_str() {
                let i: i64 = s
                    .parse()
                    .map_err(|_| Error::invalid_query(format!("Cannot parse '{}' as INT64", s)))?;
                Ok(Value::int64(i))
            } else {
                Err(Error::invalid_query(format!(
                    "Expected integer, got {:?}",
                    json_val
                )))
            }
        }
        DataType::Float32 | DataType::Float64 => {
            if let Some(f) = json_val.as_f64() {
                Ok(Value::float64(f))
            } else if let Some(i) = json_val.as_i64() {
                Ok(Value::float64(i as f64))
            } else if let Some(s) = json_val.as_str() {
                let f: f64 = s.parse().map_err(|_| {
                    Error::invalid_query(format!("Cannot parse '{}' as FLOAT64", s))
                })?;
                Ok(Value::float64(f))
            } else {
                Err(Error::invalid_query(format!(
                    "Expected number, got {:?}",
                    json_val
                )))
            }
        }
        DataType::String => {
            if let Some(s) = json_val.as_str() {
                Ok(Value::string(s.to_string()))
            } else {
                Ok(Value::string(json_val.to_string()))
            }
        }
        DataType::Bytes => {
            if let Some(s) = json_val.as_str() {
                let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                    .map_err(|e| {
                    Error::invalid_query(format!("Invalid base64 for BYTES: {}", e))
                })?;
                Ok(Value::bytes(bytes))
            } else {
                Err(Error::invalid_query(format!(
                    "Expected base64 string for BYTES, got {:?}",
                    json_val
                )))
            }
        }
        DataType::Date => {
            if let Some(s) = json_val.as_str() {
                let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|e| {
                    Error::invalid_query(format!("Invalid date format '{}': {}", s, e))
                })?;
                Ok(Value::date(date))
            } else {
                Err(Error::invalid_query(format!(
                    "Expected date string, got {:?}",
                    json_val
                )))
            }
        }
        DataType::Time => {
            if let Some(s) = json_val.as_str() {
                let time = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
                    .or_else(|_| chrono::NaiveTime::parse_from_str(s, "%H:%M:%S"))
                    .map_err(|e| {
                        Error::invalid_query(format!("Invalid time format '{}': {}", s, e))
                    })?;
                Ok(Value::time(time))
            } else {
                Err(Error::invalid_query(format!(
                    "Expected time string, got {:?}",
                    json_val
                )))
            }
        }
        DataType::DateTime => {
            if let Some(s) = json_val.as_str() {
                let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
                    .map_err(|e| {
                        Error::invalid_query(format!("Invalid datetime format '{}': {}", s, e))
                    })?;
                let utc_dt =
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(dt, chrono::Utc);
                Ok(Value::datetime(utc_dt))
            } else {
                Err(Error::invalid_query(format!(
                    "Expected datetime string, got {:?}",
                    json_val
                )))
            }
        }
        DataType::TimestampTz => {
            if let Some(s) = json_val.as_str() {
                let dt = chrono::DateTime::parse_from_rfc3339(s)
                    .map(|d| d.with_timezone(&chrono::Utc))
                    .or_else(|_| {
                        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                            .or_else(|_| {
                                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                            })
                            .map(|ndt| {
                                chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
                                    ndt,
                                    chrono::Utc,
                                )
                            })
                    })
                    .map_err(|e| {
                        Error::invalid_query(format!("Invalid timestamp format '{}': {}", s, e))
                    })?;
                Ok(Value::datetime(dt))
            } else {
                Err(Error::invalid_query(format!(
                    "Expected timestamp string, got {:?}",
                    json_val
                )))
            }
        }
        DataType::Numeric(_) => {
            if let Some(s) = json_val.as_str() {
                let dec: rust_decimal::Decimal = s
                    .parse()
                    .map_err(|e| Error::invalid_query(format!("Invalid decimal '{}': {}", s, e)))?;
                Ok(Value::numeric(dec))
            } else if let Some(n) = json_val.as_i64() {
                Ok(Value::numeric(rust_decimal::Decimal::from(n)))
            } else if let Some(n) = json_val.as_f64() {
                let dec = rust_decimal::Decimal::try_from(n).map_err(|e| {
                    Error::invalid_query(format!("Cannot convert {} to decimal: {}", n, e))
                })?;
                Ok(Value::numeric(dec))
            } else {
                Err(Error::invalid_query(format!(
                    "Expected numeric value, got {:?}",
                    json_val
                )))
            }
        }
        DataType::Array(element_type) => {
            let arr = json_val.as_array().ok_or_else(|| {
                Error::invalid_query(format!("Expected array, got {:?}", json_val))
            })?;
            let elements: Vec<Value> = arr
                .iter()
                .map(|elem| json_value_to_value(elem, element_type))
                .collect::<Result<Vec<_>>>()?;
            Ok(Value::array(elements))
        }
        DataType::Struct(fields) => {
            let obj = json_val.as_object().ok_or_else(|| {
                Error::invalid_query(format!("Expected object for struct, got {:?}", json_val))
            })?;
            let mut struct_values = IndexMap::new();
            for field in fields {
                let field_name_lower = field.name.to_lowercase();
                let field_val = obj
                    .iter()
                    .find(|(k, _)| k.to_lowercase() == field_name_lower)
                    .map(|(_, v)| v);
                let value = if let Some(fv) = field_val {
                    json_value_to_value(fv, &field.data_type)?
                } else {
                    Value::null()
                };
                struct_values.insert(field.name.clone(), value);
            }
            Ok(Value::struct_val(struct_values))
        }
        DataType::Map(key_type, value_type) => {
            let obj = json_val.as_object().ok_or_else(|| {
                Error::invalid_query(format!("Expected object for map, got {:?}", json_val))
            })?;
            let mut entries = Vec::new();
            for (k, v) in obj {
                let key = json_value_to_value(&serde_json::Value::String(k.clone()), key_type)?;
                let val = json_value_to_value(v, value_type)?;
                entries.push((key, val));
            }
            Ok(Value::map(entries))
        }
        DataType::Json => Ok(Value::json(json_val.clone())),
        _ => Err(Error::unsupported_feature(format!(
            "JSON to {:?} conversion not implemented",
            target_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    fn create_test_json_file(content: &str) -> NamedTempFile {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(content.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        temp_file
    }

    #[test]
    fn test_read_json_file_basic() {
        let content = r#"{"id": 1, "name": "Alice", "score": 95.5}
{"id": 2, "name": "Bob", "score": 88.0}
{"id": 3, "name": null, "score": 77.5}"#;
        let temp_file = create_test_json_file(content);
        let path = temp_file.path().to_str().unwrap();

        let (schema, rows) = read_json_file(path).unwrap();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(rows.len(), 3);

        assert_eq!(rows[0].values()[0], Value::int64(1));
        assert_eq!(rows[0].values()[1], Value::string("Alice".to_string()));

        assert_eq!(rows[1].values()[0], Value::int64(2));
        assert_eq!(rows[1].values()[1], Value::string("Bob".to_string()));

        assert_eq!(rows[2].values()[0], Value::int64(3));
        assert!(rows[2].values()[1].is_null());
    }

    #[test]
    fn test_load_json_with_schema() {
        let content = r#"{"id": 1, "NAME": "Alice"}
{"id": 2, "NAME": "Bob"}"#;
        let temp_file = create_test_json_file(content);
        let path = temp_file.path().to_str().unwrap();

        let schema = Schema::from_fields(vec![
            Field::required("id".to_string(), DataType::Int64),
            Field::nullable("name".to_string(), DataType::String),
        ]);

        let rows = load_json_into_table(path, &schema).unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], Value::int64(1));
        assert_eq!(rows[0].values()[1], Value::string("Alice".to_string()));
    }

    #[test]
    fn test_json_with_nested_types() {
        let content = r#"{"id": 1, "tags": ["a", "b"], "meta": {"key": "value"}}"#;
        let temp_file = create_test_json_file(content);
        let path = temp_file.path().to_str().unwrap();

        let (schema, rows) = read_json_file(path).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(schema.fields().len(), 3);

        let tags_idx = schema
            .fields()
            .iter()
            .position(|f| f.name == "tags")
            .unwrap();
        let tags = rows[0].values()[tags_idx].as_array().unwrap();
        assert_eq!(tags.len(), 2);
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
    }
}
