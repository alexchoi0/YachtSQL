use yachtsql_core::error::Result;

use super::super::ProjectionWithExprExec;

impl ProjectionWithExprExec {
    pub(crate) fn cast_value(
        value: crate::types::Value,
        target_type: &crate::optimizer::expr::CastDataType,
    ) -> Result<crate::types::Value> {
        use yachtsql_core::error::Error;
        use yachtsql_core::types::Value;
        use yachtsql_optimizer::expr::CastDataType;

        let is_basic_type = value.as_i64().is_some()
            || value.as_f64().is_some()
            || value.as_numeric().is_some()
            || value.as_str().is_some()
            || value.as_bool().is_some()
            || value.is_null()
            || value.as_bytes().is_some()
            || value.as_array().is_some()
            || value.as_interval().is_some()
            || value.as_json().is_some();

        if is_basic_type {
            match target_type {
                CastDataType::Int64
                | CastDataType::Float64
                | CastDataType::Numeric(_)
                | CastDataType::String
                | CastDataType::Bytes
                | CastDataType::Bool
                | CastDataType::Json
                | CastDataType::Array(_)
                | CastDataType::MacAddr
                | CastDataType::MacAddr8
                | CastDataType::Inet
                | CastDataType::Cidr
                | CastDataType::Hstore
                | CastDataType::Uuid
                | CastDataType::Interval
                | CastDataType::Int4Range
                | CastDataType::Int8Range
                | CastDataType::NumRange
                | CastDataType::TsRange
                | CastDataType::TsTzRange
                | CastDataType::DateRange
                | CastDataType::Point
                | CastDataType::PgBox
                | CastDataType::Circle
                | CastDataType::Vector(_) => {
                    return crate::query_executor::execution::perform_cast(&value, target_type);
                }
                _ => {}
            }
        }

        if value.is_null() {
            return Ok(Value::null());
        }

        if matches!(target_type, CastDataType::String) {
            if let Some(d) = value.as_date() {
                return Ok(Value::string(d.to_string()));
            }
            if let Some(dt) = value.as_datetime() {
                return Ok(Value::string(dt.to_string()));
            }
            if let Some(t) = value.as_time() {
                return Ok(Value::string(t.to_string()));
            }
            if let Some(ts) = value.as_timestamp() {
                return Ok(Value::string(ts.to_string()));
            }
            if let Some(p) = value.as_point() {
                return Ok(Value::string(p.to_string()));
            }
            if let Some(b) = value.as_pgbox() {
                return Ok(Value::string(b.to_string()));
            }
            if let Some(c) = value.as_circle() {
                return Ok(Value::string(c.to_string()));
            }
        }

        if matches!(target_type, CastDataType::Date) {
            if let Some(d) = value.as_date() {
                return Ok(Value::date(d));
            }
            if let Some(s) = value.as_str() {
                use chrono::NaiveDate;
                return NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
                    .map(Value::date)
                    .map_err(|_| Error::InvalidOperation(format!("Cannot cast '{}' to DATE", s)));
            }
            if let Some(ts) = value.as_timestamp() {
                return Ok(Value::date(ts.date_naive()));
            }
            if let Some(dt) = value.as_datetime() {
                return Ok(Value::date(dt.date_naive()));
            }
        }

        if matches!(target_type, CastDataType::Timestamp) {
            if let Some(ts) = value.as_timestamp() {
                return Ok(Value::timestamp(ts));
            }
            if let Some(s) = value.as_str() {
                return crate::types::parse_timestamp_to_utc(s.trim())
                    .map(Value::timestamp)
                    .ok_or_else(|| {
                        Error::InvalidOperation(format!("Cannot cast '{}' to TIMESTAMP", s))
                    });
            }
            if let Some(d) = value.as_date() {
                use chrono::NaiveTime;
                let ndt = d.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                return Ok(Value::timestamp(
                    chrono::DateTime::from_naive_utc_and_offset(ndt, chrono::Utc),
                ));
            }
            if let Some(dt) = value.as_datetime() {
                return Ok(Value::timestamp(dt));
            }
        }

        if matches!(target_type, CastDataType::DateTime) {
            if let Some(dt) = value.as_datetime() {
                return Ok(Value::datetime(dt));
            }
            if let Some(ts) = value.as_timestamp() {
                return Ok(Value::datetime(ts));
            }
            if let Some(d) = value.as_date() {
                use chrono::NaiveTime;
                let ndt = d.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                return Ok(Value::datetime(
                    chrono::DateTime::from_naive_utc_and_offset(ndt, chrono::Utc),
                ));
            }
            if let Some(s) = value.as_str() {
                return crate::types::parse_timestamp_to_utc(s.trim())
                    .map(Value::datetime)
                    .ok_or_else(|| {
                        Error::InvalidOperation(format!("Cannot cast '{}' to DATETIME", s))
                    });
            }
        }

        if matches!(target_type, CastDataType::Time) {
            if let Some(t) = value.as_time() {
                return Ok(Value::time(t));
            }
            if let Some(s) = value.as_str() {
                use chrono::NaiveTime;
                return NaiveTime::parse_from_str(s.trim(), "%H:%M:%S")
                    .map(Value::time)
                    .map_err(|_| Error::InvalidOperation(format!("Cannot cast '{}' to TIME", s)));
            }
            if let Some(ts) = value.as_timestamp() {
                return Ok(Value::time(ts.time()));
            }
            if let Some(dt) = value.as_datetime() {
                return Ok(Value::time(dt.time()));
            }
        }

        if let CastDataType::Custom(type_name, struct_fields) = target_type {
            debug_print::debug_eprintln!(
                "[executor::cast] casting to Custom type '{}' with fields {:?}",
                type_name,
                struct_fields.iter().map(|f| &f.name).collect::<Vec<_>>()
            );
            if struct_fields.is_empty() {
                let resolved_type = match type_name.to_uppercase().as_str() {
                    "HSTORE" => CastDataType::Hstore,
                    "MACADDR" => CastDataType::MacAddr,
                    "MACADDR8" => CastDataType::MacAddr8,
                    "INET" => CastDataType::Inet,
                    "CIDR" => CastDataType::Cidr,
                    "INTERVAL" => CastDataType::Interval,
                    "UUID" => CastDataType::Uuid,
                    "INT4RANGE" => CastDataType::Int4Range,
                    "INT8RANGE" => CastDataType::Int8Range,
                    "NUMRANGE" => CastDataType::NumRange,
                    "TSRANGE" => CastDataType::TsRange,
                    "TSTZRANGE" => CastDataType::TsTzRange,
                    "DATERANGE" => CastDataType::DateRange,
                    "POINT" => CastDataType::Point,
                    "BOX" => CastDataType::PgBox,
                    "CIRCLE" => CastDataType::Circle,
                    _ => {
                        if let Some(s) = value.as_str() {
                            return Ok(Value::string(s.to_string()));
                        }
                        return Ok(value);
                    }
                };
                return crate::query_executor::execution::perform_cast(&value, &resolved_type);
            }
            if let Some(struct_val) = value.as_struct() {
                let old_values: Vec<_> = struct_val.values().cloned().collect();
                if old_values.len() != struct_fields.len() {
                    return Err(Error::type_mismatch(
                        format!("Struct with {} fields", struct_fields.len()),
                        format!("Struct with {} fields", old_values.len()),
                    ));
                }
                let new_fields: Vec<_> = struct_fields
                    .iter()
                    .zip(old_values)
                    .map(|(field, val)| (field.name.clone(), val))
                    .collect();
                debug_print::debug_eprintln!(
                    "[executor::cast] renamed fields to {:?}",
                    new_fields.iter().map(|(n, _)| n).collect::<Vec<_>>()
                );
                return Ok(Value::struct_val(new_fields.into_iter().collect()));
            }
        }

        Err(Error::unsupported_feature(format!(
            "Cannot cast {:?} to {:?}",
            value.data_type(),
            target_type
        )))
    }

    pub(crate) fn try_cast_value(
        value: crate::types::Value,
        target_type: &crate::optimizer::expr::CastDataType,
    ) -> crate::types::Value {
        use yachtsql_core::types::Value;

        match Self::cast_value(value, target_type) {
            Ok(v) => v,
            Err(_) => Value::null(),
        }
    }

    pub(crate) fn values_equal(a: &crate::types::Value, b: &crate::types::Value) -> bool {
        if a.is_null() && b.is_null() {
            return true;
        }

        if let (Some(a_val), Some(b_val)) = (a.as_bool(), b.as_bool()) {
            return a_val == b_val;
        }

        if let (Some(a_val), Some(b_val)) = (a.as_i64(), b.as_i64()) {
            return a_val == b_val;
        }

        if let (Some(a_val), Some(b_val)) = (a.as_f64(), b.as_f64()) {
            return a_val == b_val;
        }

        if let (Some(a_val), Some(b_val)) = (a.as_str(), b.as_str()) {
            return a_val == b_val;
        }

        if let (Some(a_struct), Some(b_struct)) = (a.as_struct(), b.as_struct()) {
            if a_struct.len() != b_struct.len() {
                return false;
            }
            return a_struct
                .values()
                .zip(b_struct.values())
                .all(|(av, bv)| Self::values_equal(av, bv));
        }

        false
    }

    pub(crate) fn values_are_distinct(a: &crate::types::Value, b: &crate::types::Value) -> bool {
        match (a.is_null(), b.is_null()) {
            (true, true) => false,
            (true, false) | (false, true) => true,
            (false, false) => {
                if let (Some(a_val), Some(b_val)) = (a.as_bool(), b.as_bool()) {
                    return a_val != b_val;
                }

                if let (Some(a_val), Some(b_val)) = (a.as_i64(), b.as_i64()) {
                    return a_val != b_val;
                }

                if let (Some(a_val), Some(b_val)) = (a.as_f64(), b.as_f64()) {
                    return a_val != b_val;
                }

                if let (Some(a_val), Some(b_val)) = (a.as_str(), b.as_str()) {
                    return a_val != b_val;
                }

                if let (Some(a_struct), Some(b_struct)) = (a.as_struct(), b.as_struct()) {
                    if a_struct.len() != b_struct.len() {
                        return true;
                    }
                    return a_struct
                        .values()
                        .zip(b_struct.values())
                        .any(|(av, bv)| Self::values_are_distinct(av, bv));
                }

                true
            }
        }
    }
}
