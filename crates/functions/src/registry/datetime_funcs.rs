use std::rc::Rc;

use chrono::{DateTime, Datelike, Duration, NaiveDate, Timelike, Utc};
use yachtsql_common::error::Error;
use yachtsql_common::types::{DataType, Value};

use super::{FunctionRegistry, month_abbr, month_name, weekday_abbr, weekday_name};
use crate::datetime;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_current_timestamp_funcs(registry);
    register_timestamp_helpers(registry);
    register_date_helpers(registry);
    register_additional_core_functions(registry);
}

pub(super) fn register_arithmetic(registry: &mut FunctionRegistry) {
    register_date_math(registry);
    register_extract(registry);
    register_extraction_wrappers(registry);
}

pub(super) fn register_formatters(registry: &mut FunctionRegistry) {
    register_date_formatters(registry);
}

fn register_current_timestamp_funcs(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "CURRENT_DATE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "CURRENT_DATE".to_string(),
            arg_types: vec![],
            return_type: DataType::Date,
            variadic: false,
            evaluator: |_| Ok(Value::date(Utc::now().naive_utc().date())),
        }),
    );

    registry.register_scalar(
        "CURRENT_TIMESTAMP".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "CURRENT_TIMESTAMP".to_string(),
            arg_types: vec![],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |_| Ok(Value::timestamp(Utc::now())),
        }),
    );

    registry.register_scalar(
        "NOW".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "NOW".to_string(),
            arg_types: vec![],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |_| Ok(Value::timestamp(Utc::now())),
        }),
    );
}

fn register_timestamp_helpers(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "TIMESTAMP_DIFF".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TIMESTAMP_DIFF".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Timestamp, DataType::String],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| datetime::eval_timestamp_diff(&args[0], &args[1], &args[2]),
        }),
    );

    registry.register_scalar(
        "TIMESTAMP_TRUNC".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TIMESTAMP_TRUNC".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::String],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| datetime::eval_timestamp_trunc(&args[0], &args[1]),
        }),
    );
}

fn register_date_helpers(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "DATE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "DATE".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Date,
            variadic: false,
            evaluator: |args| datetime::date(&args[0]),
        }),
    );
}

fn register_date_math(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "DATE_ADD".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "DATE_ADD".to_string(),
            arg_types: vec![DataType::Date, DataType::Int64],
            return_type: DataType::Date,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                match (args[0].as_date(), args[1].as_i64()) {
                    (Some(d), Some(days)) => d
                        .checked_add_signed(Duration::days(days))
                        .map(Value::date)
                        .ok_or_else(|| Error::invalid_query("Date overflow in DATE_ADD")),
                    _ => Err(Error::TypeMismatch {
                        expected: "DATE, INT64".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );

    registry.register_scalar(
        "DATE_DIFF".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "DATE_DIFF".to_string(),
            arg_types: vec![DataType::Date, DataType::Date],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                match (args[0].as_date(), args[1].as_date()) {
                    (Some(d1), Some(d2)) => {
                        Ok(Value::int64(d1.signed_duration_since(d2).num_days()))
                    }
                    _ => Err(Error::TypeMismatch {
                        expected: "DATE, DATE".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );
}

fn register_extract(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "EXTRACT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "EXTRACT".to_string(),
            arg_types: vec![DataType::String, DataType::Date],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                if let Some(part) = args[0].as_str() {
                    if let Some(d) = args[1].as_date() {
                        return extract_from_date(part, &d);
                    } else if let Some(ts) = args[1].as_timestamp() {
                        return extract_from_timestamp(part, &ts);
                    }
                }

                Err(Error::TypeMismatch {
                    expected: "STRING, DATE or STRING, TIMESTAMP".to_string(),
                    actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );
}

fn extract_from_date(
    part: &str,
    date: &chrono::NaiveDate,
) -> yachtsql_common::error::Result<Value> {
    match part.to_uppercase().as_str() {
        "YEAR" => Ok(Value::int64(date.year() as i64)),
        "MONTH" => Ok(Value::int64(date.month() as i64)),
        "DAY" => Ok(Value::int64(date.day() as i64)),
        "DOW" | "DAYOFWEEK" => Ok(Value::int64(date.weekday().num_days_from_sunday() as i64)),
        "DOY" | "DAYOFYEAR" => Ok(Value::int64(date.ordinal() as i64)),
        "QUARTER" => {
            let quarter = (date.month() - 1) / 3 + 1;
            Ok(Value::int64(quarter as i64))
        }
        "WEEK" => Ok(Value::int64(date.iso_week().week() as i64)),
        "ISODOW" => Ok(Value::int64(date.weekday().number_from_monday() as i64)),
        "EPOCH" => {
            let epoch_start = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .ok_or_else(|| Error::internal("Failed to create Unix epoch date"))?;
            let days = date.signed_duration_since(epoch_start).num_days();
            Ok(Value::int64(days * 86400))
        }
        _ => Err(Error::invalid_query(format!(
            "Unknown date part for EXTRACT: {}",
            part
        ))),
    }
}

fn extract_from_timestamp(
    part: &str,
    ts: &chrono::DateTime<Utc>,
) -> yachtsql_common::error::Result<Value> {
    match part.to_uppercase().as_str() {
        "YEAR" => Ok(Value::int64(ts.year() as i64)),
        "MONTH" => Ok(Value::int64(ts.month() as i64)),
        "DAY" => Ok(Value::int64(ts.day() as i64)),
        "HOUR" => Ok(Value::int64(ts.hour() as i64)),
        "MINUTE" => Ok(Value::int64(ts.minute() as i64)),
        "SECOND" => Ok(Value::int64(ts.second() as i64)),
        "DOW" | "DAYOFWEEK" => Ok(Value::int64(ts.weekday().num_days_from_sunday() as i64)),
        "DOY" | "DAYOFYEAR" => Ok(Value::int64(ts.ordinal() as i64)),
        "QUARTER" => {
            let quarter = (ts.month() - 1) / 3 + 1;
            Ok(Value::int64(quarter as i64))
        }
        "WEEK" => Ok(Value::int64(ts.iso_week().week() as i64)),
        "ISODOW" => Ok(Value::int64(ts.weekday().number_from_monday() as i64)),
        "EPOCH" => Ok(Value::int64(ts.timestamp())),
        "MILLISECOND" => Ok(Value::int64(ts.timestamp_subsec_millis() as i64)),
        "MICROSECOND" => Ok(Value::int64(ts.timestamp_subsec_micros() as i64)),
        _ => Err(Error::invalid_query(format!(
            "Unknown timestamp part for EXTRACT: {}",
            part
        ))),
    }
}

fn register_date_formatters(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "DATE_TRUNC".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "DATE_TRUNC".to_string(),
            arg_types: vec![DataType::String, DataType::Date],
            return_type: DataType::Date,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                if let Some(precision) = args[0].as_str() {
                    if let Some(date) = args[1].as_date() {
                        return truncate_date(precision, date).map(Value::date);
                    } else if let Some(ts) = args[1].as_timestamp() {
                        return datetime::timestamp_trunc(&ts, precision).map(Value::timestamp);
                    }
                }

                Err(Error::TypeMismatch {
                    expected: "STRING, DATE or STRING, TIMESTAMP".to_string(),
                    actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "FORMAT_DATE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "FORMAT_DATE".to_string(),
            arg_types: vec![DataType::String, DataType::Date],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                match (args[0].as_str(), args[1].as_date()) {
                    (Some(format), Some(date)) => Ok(Value::string(format_date(format, &date))),
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, DATE".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );

    registry.register_scalar(
        "FORMAT_TIMESTAMP".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "FORMAT_TIMESTAMP".to_string(),
            arg_types: vec![DataType::String, DataType::Timestamp],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                match (args[0].as_str(), args[1].as_timestamp()) {
                    (Some(format), Some(ts)) => Ok(Value::string(format_timestamp(format, &ts))),
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, TIMESTAMP".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );

    registry.register_scalar(
        "PARSE_DATE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "PARSE_DATE".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Date,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                match (args[0].as_str(), args[1].as_str()) {
                    (Some(format), Some(date_str)) => {
                        chrono::NaiveDate::parse_from_str(date_str, format)
                            .map(Value::date)
                            .map_err(|e| {
                                Error::invalid_query(format!(
                                    "Failed to parse date '{}' with format '{}': {}",
                                    date_str, format, e
                                ))
                            })
                    }
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, STRING".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );

    registry.register_scalar(
        "PARSE_TIMESTAMP".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "PARSE_TIMESTAMP".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                match (args[0].as_str(), args[1].as_str()) {
                    (Some(format), Some(ts)) => chrono::NaiveDateTime::parse_from_str(ts, format)
                        .map(|ndt| {
                            Value::timestamp(chrono::DateTime::from_naive_utc_and_offset(ndt, Utc))
                        })
                        .map_err(|e| {
                            Error::invalid_query(format!(
                                "Failed to parse timestamp '{}' with format '{}': {}",
                                ts, format, e
                            ))
                        }),
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, STRING".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );
}

fn truncate_date(
    precision: &str,
    date: chrono::NaiveDate,
) -> yachtsql_common::error::Result<chrono::NaiveDate> {
    match precision.to_uppercase().as_str() {
        "YEAR" => date
            .with_month(1)
            .and_then(|d| d.with_day(1))
            .ok_or_else(|| Error::invalid_query("Failed to truncate date to YEAR")),
        "MONTH" => date
            .with_day(1)
            .ok_or_else(|| Error::invalid_query("Failed to truncate date to MONTH")),
        "DAY" => Ok(date),
        "WEEK" => {
            let diff = date.weekday().num_days_from_monday();
            Ok(date - Duration::days(diff as i64))
        }
        "QUARTER" => {
            let start_month = ((date.month() - 1) / 3) * 3 + 1;
            date.with_month(start_month)
                .and_then(|d| d.with_day(1))
                .ok_or_else(|| Error::invalid_query("Failed to truncate date to QUARTER"))
        }
        _ => Err(Error::invalid_query(format!(
            "Unknown precision for DATE_TRUNC: {}",
            precision
        ))),
    }
}

fn format_date(pattern: &str, date: &chrono::NaiveDate) -> String {
    pattern
        .replace("%Y", &format!("{:04}", date.year()))
        .replace("%y", &format!("{:02}", date.year() % 100))
        .replace("%m", &format!("{:02}", date.month()))
        .replace("%B", month_name(date.month()))
        .replace("%b", month_abbr(date.month()))
        .replace("%d", &format!("{:02}", date.day()))
        .replace("%e", &format!("{:2}", date.day()))
        .replace("%A", weekday_name(date.weekday()))
        .replace("%a", weekday_abbr(date.weekday()))
        .replace("%j", &format!("{:03}", date.ordinal()))
}

fn format_timestamp(pattern: &str, ts: &chrono::DateTime<Utc>) -> String {
    let hour_12 = match ts.hour() {
        0 => 12,
        h if h > 12 => h - 12,
        h => h,
    };

    pattern
        .replace("%Y", &format!("{:04}", ts.year()))
        .replace("%y", &format!("{:02}", ts.year() % 100))
        .replace("%m", &format!("{:02}", ts.month()))
        .replace("%B", month_name(ts.month()))
        .replace("%b", month_abbr(ts.month()))
        .replace("%d", &format!("{:02}", ts.day()))
        .replace("%e", &format!("{:2}", ts.day()))
        .replace("%A", weekday_name(ts.weekday()))
        .replace("%a", weekday_abbr(ts.weekday()))
        .replace("%j", &format!("{:03}", ts.ordinal()))
        .replace("%H", &format!("{:02}", ts.hour()))
        .replace("%I", &format!("{:02}", hour_12))
        .replace("%M", &format!("{:02}", ts.minute()))
        .replace("%S", &format!("{:02}", ts.second()))
        .replace("%p", if ts.hour() < 12 { "AM" } else { "PM" })
        .replace("%f", &format!("{:06}", ts.timestamp_subsec_micros()))
}

fn register_extraction_wrappers(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "YEAR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "YEAR".to_string(),
            arg_types: vec![DataType::Date],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64(d.year() as i64));
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.year() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "DATE or TIMESTAMP".to_string(),
                    actual: format!("{}", args[0].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "MONTH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "MONTH".to_string(),
            arg_types: vec![DataType::Date],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64(d.month() as i64));
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.month() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "DATE or TIMESTAMP".to_string(),
                    actual: format!("{}", args[0].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "DAY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "DAY".to_string(),
            arg_types: vec![DataType::Date],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64(d.day() as i64));
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.day() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "DATE or TIMESTAMP".to_string(),
                    actual: format!("{}", args[0].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "HOUR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HOUR".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.hour() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: format!("{}", args[0].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "MINUTE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "MINUTE".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.minute() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: format!("{}", args[0].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "SECOND".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SECOND".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.second() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: format!("{}", args[0].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "DAYOFWEEK".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "DAYOFWEEK".to_string(),
            arg_types: vec![DataType::Date],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64(d.weekday().num_days_from_sunday() as i64 + 1));
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.weekday().num_days_from_sunday() as i64 + 1));
                }
                Err(Error::TypeMismatch {
                    expected: "DATE or TIMESTAMP".to_string(),
                    actual: format!("{}", args[0].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "DAYOFYEAR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "DAYOFYEAR".to_string(),
            arg_types: vec![DataType::Date],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64(d.ordinal() as i64));
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.ordinal() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "DATE or TIMESTAMP".to_string(),
                    actual: format!("{}", args[0].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "DAYOFMONTH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "DAYOFMONTH".to_string(),
            arg_types: vec![DataType::Date],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64(d.day() as i64));
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.day() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "DATE or TIMESTAMP".to_string(),
                    actual: format!("{}", args[0].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "WEEKDAY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "WEEKDAY".to_string(),
            arg_types: vec![DataType::Date],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64(d.weekday().num_days_from_monday() as i64));
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.weekday().num_days_from_monday() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "DATE or TIMESTAMP".to_string(),
                    actual: format!("{}", args[0].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "WEEK".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "WEEK".to_string(),
            arg_types: vec![DataType::Date],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64(d.iso_week().week() as i64));
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.iso_week().week() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "DATE or TIMESTAMP".to_string(),
                    actual: format!("{}", args[0].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "ISOWEEK".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ISOWEEK".to_string(),
            arg_types: vec![DataType::Date],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64(d.iso_week().week() as i64));
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.iso_week().week() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "DATE or TIMESTAMP".to_string(),
                    actual: format!("{}", args[0].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "QUARTER".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "QUARTER".to_string(),
            arg_types: vec![DataType::Date],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(d) = args[0].as_date() {
                    let quarter = (d.month() - 1) / 3 + 1;
                    return Ok(Value::int64(quarter as i64));
                }
                if let Some(ts) = args[0].as_timestamp() {
                    let quarter = (ts.month() - 1) / 3 + 1;
                    return Ok(Value::int64(quarter as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "DATE or TIMESTAMP".to_string(),
                    actual: format!("{}", args[0].data_type()),
                })
            },
        }),
    );
}

fn register_additional_core_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "CURRENT_TIME".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "CURRENT_TIME".to_string(),
            arg_types: vec![],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |_| Ok(Value::timestamp(Utc::now())),
        }),
    );

    registry.register_scalar(
        "DATE_SUB".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "DATE_SUB".to_string(),
            arg_types: vec![DataType::Date, DataType::Int64],
            return_type: DataType::Date,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                match (args[0].as_date(), args[1].as_i64()) {
                    (Some(d), Some(days)) => d
                        .checked_sub_signed(Duration::days(days))
                        .map(Value::date)
                        .ok_or_else(|| Error::invalid_query("Date overflow in DATE_SUB")),
                    _ => Err(Error::TypeMismatch {
                        expected: "DATE, INT64".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );

    registry.register_scalar(
        "LAST_DAY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "LAST_DAY".to_string(),
            arg_types: vec![DataType::Date],
            return_type: DataType::Date,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }

                if let Some(date) = args[0].as_date() {
                    let next_month = if date.month() == 12 {
                        chrono::NaiveDate::from_ymd_opt(date.year() + 1, 1, 1)
                    } else {
                        chrono::NaiveDate::from_ymd_opt(date.year(), date.month() + 1, 1)
                    };

                    return next_month
                        .and_then(|nm| nm.pred_opt())
                        .map(Value::date)
                        .ok_or_else(|| {
                            Error::invalid_query("Failed to calculate last day of month")
                        });
                }

                Err(Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: format!("{}", args[0].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "TIME".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TIME".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Time,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }

                if let Some(ts) = args[0].as_timestamp() {
                    let time = ts.time();
                    return Ok(Value::time(time));
                }

                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: format!("{}", args[0].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "AGE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "AGE".to_string(),
            arg_types: vec![DataType::Date, DataType::Date],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                match (args[0].as_date(), args[1].as_date()) {
                    (Some(d1), Some(d2)) => {
                        Ok(Value::int64(d1.signed_duration_since(d2).num_days()))
                    }
                    _ => Err(Error::TypeMismatch {
                        expected: "DATE, DATE".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );

    registry.register_scalar(
        "MAKE_DATE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "MAKE_DATE".to_string(),
            arg_types: vec![DataType::Int64, DataType::Int64, DataType::Int64],
            return_type: DataType::Date,
            variadic: false,
            evaluator: |args| {
                if args.len() != 3 {
                    return Err(Error::invalid_query(
                        "MAKE_DATE requires exactly 3 arguments (year, month, day)".to_string(),
                    ));
                }

                if args[0].is_null() || args[1].is_null() || args[2].is_null() {
                    return Ok(Value::null());
                }

                match (args[0].as_i64(), args[1].as_i64(), args[2].as_i64()) {
                    (Some(year), Some(month), Some(day)) => {
                        match NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32) {
                            Some(date) => Ok(Value::date(date)),
                            None => Err(Error::invalid_query(format!(
                                "Invalid date: year={}, month={}, day={}",
                                year, month, day
                            ))),
                        }
                    }
                    _ => Err(Error::TypeMismatch {
                        expected: "INT64, INT64, INT64".to_string(),
                        actual: format!(
                            "{}, {}, {}",
                            args[0].data_type(),
                            args[1].data_type(),
                            args[2].data_type()
                        ),
                    }),
                }
            },
        }),
    );

    registry.register_scalar(
        "MAKE_TIMESTAMP".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "MAKE_TIMESTAMP".to_string(),
            arg_types: vec![
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
            ],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args.len() != 6 {
                    return Err(Error::invalid_query(
                        "MAKE_TIMESTAMP requires exactly 6 arguments (year, month, day, hour, minute, second)".to_string(),
                    ));
                }

                if args.iter().any(|arg| arg.is_null()) {
                    return Ok(Value::null());
                }

                match (
                    args[0].as_i64(),
                    args[1].as_i64(),
                    args[2].as_i64(),
                    args[3].as_i64(),
                    args[4].as_i64(),
                    args[5].as_i64(),
                ) {
                    (Some(year), Some(month), Some(day), Some(hour), Some(minute), Some(second)) => {
                        match NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32) {
                            Some(date) => {
                                match date.and_hms_opt(hour as u32, minute as u32, second as u32) {
                                    Some(naive_datetime) => {
                                        let timestamp = DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc);
                                        Ok(Value::timestamp(timestamp))
                                    }
                                    None => Err(Error::invalid_query(format!(
                                        "Invalid time: {}:{:02}:{:02}",
                                        hour, minute, second
                                    ))),
                                }
                            }
                            None => Err(Error::invalid_query(format!(
                                "Invalid date: year={}, month={}, day={}",
                                year, month, day
                            ))),
                        }
                    }
                    _ => Err(Error::TypeMismatch {
                        expected: "INT64, INT64, INT64, INT64, INT64, INT64".to_string(),
                        actual: format!(
                            "{}, {}, {}, {}, {}, {}",
                            args[0].data_type(),
                            args[1].data_type(),
                            args[2].data_type(),
                            args[3].data_type(),
                            args[4].data_type(),
                            args[5].data_type()
                        ),
                    }),
                }
            },
        }),
    );
}
