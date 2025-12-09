use std::rc::Rc;

use chrono::{DateTime, Datelike, Duration, NaiveDate, Timelike, Utc};
use yachtsql_core::error::Error;
use yachtsql_core::types::{DataType, Value};

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_extraction_functions(registry);
    register_truncation_functions(registry);
    register_arithmetic_functions(registry);
    register_subtract_functions(registry);
    register_now64_function(registry);
    register_formatting_functions(registry);
    register_relative_functions(registry);
    register_todate_function(registry);
}

fn register_extraction_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "TOYEAR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOYEAR".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.year() as i64));
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64(d.year() as i64));
                }
                if let Some(d32) = args[0].as_date32() {
                    if let Some(date) = d32.to_naive_date() {
                        return Ok(Value::int64(date.year() as i64));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP, DATE, or DATE32".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TOMONTH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOMONTH".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.month() as i64));
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64(d.month() as i64));
                }
                if let Some(d32) = args[0].as_date32() {
                    if let Some(date) = d32.to_naive_date() {
                        return Ok(Value::int64(date.month() as i64));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP, DATE, or DATE32".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TODAYOFMONTH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TODAYOFMONTH".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.day() as i64));
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64(d.day() as i64));
                }
                if let Some(d32) = args[0].as_date32() {
                    if let Some(date) = d32.to_naive_date() {
                        return Ok(Value::int64(date.day() as i64));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP, DATE, or DATE32".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TODAYOFWEEK".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TODAYOFWEEK".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.weekday().number_from_monday() as i64));
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64(d.weekday().number_from_monday() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP or DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TODAYOFYEAR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TODAYOFYEAR".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.ordinal() as i64));
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64(d.ordinal() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP or DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TOHOUR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOHOUR".to_string(),
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
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TOMINUTE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOMINUTE".to_string(),
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
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TOSECOND".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOSECOND".to_string(),
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
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );
}

fn register_truncation_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "TOSTARTOFYEAR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOSTARTOFYEAR".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Date,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    let date = ts.date_naive();
                    return date
                        .with_month(1)
                        .and_then(|d| d.with_day(1))
                        .map(Value::date)
                        .ok_or_else(|| {
                            Error::invalid_query("Failed to truncate to start of year")
                        });
                }
                if let Some(d) = args[0].as_date() {
                    return d
                        .with_month(1)
                        .and_then(|dt| dt.with_day(1))
                        .map(Value::date)
                        .ok_or_else(|| {
                            Error::invalid_query("Failed to truncate to start of year")
                        });
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP or DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TOSTARTOFQUARTER".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOSTARTOFQUARTER".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Date,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    let date = ts.date_naive();
                    let start_month = ((date.month() - 1) / 3) * 3 + 1;
                    return date
                        .with_month(start_month)
                        .and_then(|d| d.with_day(1))
                        .map(Value::date)
                        .ok_or_else(|| {
                            Error::invalid_query("Failed to truncate to start of quarter")
                        });
                }
                if let Some(d) = args[0].as_date() {
                    let start_month = ((d.month() - 1) / 3) * 3 + 1;
                    return d
                        .with_month(start_month)
                        .and_then(|dt| dt.with_day(1))
                        .map(Value::date)
                        .ok_or_else(|| {
                            Error::invalid_query("Failed to truncate to start of quarter")
                        });
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP or DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TOSTARTOFMONTH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOSTARTOFMONTH".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Date,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return ts.date_naive().with_day(1).map(Value::date).ok_or_else(|| {
                        Error::invalid_query("Failed to truncate to start of month")
                    });
                }
                if let Some(d) = args[0].as_date() {
                    return d.with_day(1).map(Value::date).ok_or_else(|| {
                        Error::invalid_query("Failed to truncate to start of month")
                    });
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP or DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TOSTARTOFWEEK".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOSTARTOFWEEK".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Date,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    let date = ts.date_naive();
                    let diff = date.weekday().num_days_from_monday();
                    return Ok(Value::date(date - Duration::days(diff as i64)));
                }
                if let Some(d) = args[0].as_date() {
                    let diff = d.weekday().num_days_from_monday();
                    return Ok(Value::date(d - Duration::days(diff as i64)));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP or DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TOMONDAY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOMONDAY".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Date,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    let date = ts.date_naive();
                    let diff = date.weekday().num_days_from_monday();
                    return Ok(Value::date(date - Duration::days(diff as i64)));
                }
                if let Some(d) = args[0].as_date() {
                    let diff = d.weekday().num_days_from_monday();
                    return Ok(Value::date(d - Duration::days(diff as i64)));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP or DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TOSTARTOFDAY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOSTARTOFDAY".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Date,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::date(ts.date_naive()));
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::date(d));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP or DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );
}

fn register_arithmetic_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ADDYEARS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ADDYEARS".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Int64],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    if let Some(years) = args[1].as_i64() {
                        let new_year = ts.year() + years as i32;
                        return ts
                            .with_year(new_year)
                            .map(Value::timestamp)
                            .ok_or_else(|| Error::invalid_query("Date overflow in ADDYEARS"));
                    }
                }
                if let Some(d) = args[0].as_date() {
                    if let Some(years) = args[1].as_i64() {
                        let new_year = d.year() + years as i32;
                        return d
                            .with_year(new_year)
                            .map(Value::date)
                            .ok_or_else(|| Error::invalid_query("Date overflow in ADDYEARS"));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP/DATE, INT64".to_string(),
                    actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "ADDMONTHS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ADDMONTHS".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Int64],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    if let Some(months) = args[1].as_i64() {
                        return add_months_to_timestamp(&ts, months).map(Value::timestamp);
                    }
                }
                if let Some(d) = args[0].as_date() {
                    if let Some(months) = args[1].as_i64() {
                        return add_months_to_date(&d, months).map(Value::date);
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP/DATE, INT64".to_string(),
                    actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "ADDWEEKS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ADDWEEKS".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Int64],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    if let Some(weeks) = args[1].as_i64() {
                        return ts
                            .checked_add_signed(Duration::weeks(weeks))
                            .map(Value::timestamp)
                            .ok_or_else(|| Error::invalid_query("Date overflow in ADDWEEKS"));
                    }
                }
                if let Some(d) = args[0].as_date() {
                    if let Some(weeks) = args[1].as_i64() {
                        return d
                            .checked_add_signed(Duration::weeks(weeks))
                            .map(Value::date)
                            .ok_or_else(|| Error::invalid_query("Date overflow in ADDWEEKS"));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP/DATE, INT64".to_string(),
                    actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "ADDDAYS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ADDDAYS".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Int64],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    if let Some(days) = args[1].as_i64() {
                        return ts
                            .checked_add_signed(Duration::days(days))
                            .map(Value::timestamp)
                            .ok_or_else(|| Error::invalid_query("Date overflow in ADDDAYS"));
                    }
                }
                if let Some(d) = args[0].as_date() {
                    if let Some(days) = args[1].as_i64() {
                        return d
                            .checked_add_signed(Duration::days(days))
                            .map(Value::date)
                            .ok_or_else(|| Error::invalid_query("Date overflow in ADDDAYS"));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP/DATE, INT64".to_string(),
                    actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "ADDHOURS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ADDHOURS".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Int64],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    if let Some(hours) = args[1].as_i64() {
                        return ts
                            .checked_add_signed(Duration::hours(hours))
                            .map(Value::timestamp)
                            .ok_or_else(|| Error::invalid_query("Date overflow in ADDHOURS"));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP, INT64".to_string(),
                    actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "ADDMINUTES".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ADDMINUTES".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Int64],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    if let Some(minutes) = args[1].as_i64() {
                        return ts
                            .checked_add_signed(Duration::minutes(minutes))
                            .map(Value::timestamp)
                            .ok_or_else(|| Error::invalid_query("Date overflow in ADDMINUTES"));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP, INT64".to_string(),
                    actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "ADDSECONDS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ADDSECONDS".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Int64],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    if let Some(seconds) = args[1].as_i64() {
                        return ts
                            .checked_add_signed(Duration::seconds(seconds))
                            .map(Value::timestamp)
                            .ok_or_else(|| Error::invalid_query("Date overflow in ADDSECONDS"));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP, INT64".to_string(),
                    actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );
}

fn register_formatting_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "TOYYYYMM".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOYYYYMM".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64((ts.year() as i64) * 100 + ts.month() as i64));
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64((d.year() as i64) * 100 + d.month() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP or DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TOYYYYMMDD".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOYYYYMMDD".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(
                        (ts.year() as i64) * 10000 + (ts.month() as i64) * 100 + ts.day() as i64,
                    ));
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64(
                        (d.year() as i64) * 10000 + (d.month() as i64) * 100 + d.day() as i64,
                    ));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP or DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TOYYYYMMDDHHMMSS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOYYYYMMDDHHMMSS".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(
                        (ts.year() as i64) * 10000000000
                            + (ts.month() as i64) * 100000000
                            + (ts.day() as i64) * 1000000
                            + (ts.hour() as i64) * 10000
                            + (ts.minute() as i64) * 100
                            + ts.second() as i64,
                    ));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );
}

fn register_relative_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "TORELATIVEYEARNUM".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TORELATIVEYEARNUM".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.year() as i64));
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64(d.year() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP or DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TORELATIVEMONTHNUM".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TORELATIVEMONTHNUM".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::int64(ts.year() as i64 * 12 + ts.month() as i64 - 1));
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::int64(d.year() as i64 * 12 + d.month() as i64 - 1));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP or DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TORELATIVEWEEKNUM".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TORELATIVEWEEKNUM".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    let epoch = DateTime::from_timestamp(0, 0)
                        .ok_or_else(|| Error::internal("Failed to create Unix epoch timestamp"))?;
                    let duration = ts.signed_duration_since(epoch);
                    return Ok(Value::int64(duration.num_weeks()));
                }
                if let Some(d) = args[0].as_date() {
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                        .ok_or_else(|| Error::internal("Failed to create Unix epoch date"))?;
                    let duration = d.signed_duration_since(epoch);
                    return Ok(Value::int64(duration.num_weeks()));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP or DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TORELATIVEDAYNUM".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TORELATIVEDAYNUM".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    let epoch = DateTime::from_timestamp(0, 0)
                        .ok_or_else(|| Error::internal("Failed to create Unix epoch timestamp"))?;
                    let duration = ts.signed_duration_since(epoch);
                    return Ok(Value::int64(duration.num_days()));
                }
                if let Some(d) = args[0].as_date() {
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                        .ok_or_else(|| Error::internal("Failed to create Unix epoch date"))?;
                    let duration = d.signed_duration_since(epoch);
                    return Ok(Value::int64(duration.num_days()));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP or DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );

    registry.register_scalar(
        "TORELATIVEHOURNUM".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TORELATIVEHOURNUM".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    let epoch = DateTime::from_timestamp(0, 0)
                        .ok_or_else(|| Error::internal("Failed to create Unix epoch timestamp"))?;
                    let duration = ts.signed_duration_since(epoch);
                    return Ok(Value::int64(duration.num_hours()));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );
}

fn register_subtract_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "SUBTRACTYEARS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SUBTRACTYEARS".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Int64],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    if let Some(years) = args[1].as_i64() {
                        let new_year = ts.year() - years as i32;
                        return ts
                            .with_year(new_year)
                            .map(Value::timestamp)
                            .ok_or_else(|| Error::invalid_query("Date overflow in SUBTRACTYEARS"));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP, INT64".to_string(),
                    actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "SUBTRACTMONTHS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SUBTRACTMONTHS".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Int64],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    if let Some(months) = args[1].as_i64() {
                        return add_months_to_timestamp(&ts, -months).map(Value::timestamp);
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP, INT64".to_string(),
                    actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "SUBTRACTWEEKS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SUBTRACTWEEKS".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Int64],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    if let Some(weeks) = args[1].as_i64() {
                        return ts
                            .checked_sub_signed(Duration::weeks(weeks))
                            .map(Value::timestamp)
                            .ok_or_else(|| Error::invalid_query("Date overflow in SUBTRACTWEEKS"));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP, INT64".to_string(),
                    actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "SUBTRACTDAYS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SUBTRACTDAYS".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Int64],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    if let Some(days) = args[1].as_i64() {
                        return ts
                            .checked_sub_signed(Duration::days(days))
                            .map(Value::timestamp)
                            .ok_or_else(|| Error::invalid_query("Date overflow in SUBTRACTDAYS"));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP, INT64".to_string(),
                    actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "SUBTRACTHOURS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SUBTRACTHOURS".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Int64],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    if let Some(hours) = args[1].as_i64() {
                        return ts
                            .checked_sub_signed(Duration::hours(hours))
                            .map(Value::timestamp)
                            .ok_or_else(|| Error::invalid_query("Date overflow in SUBTRACTHOURS"));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP, INT64".to_string(),
                    actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "SUBTRACTMINUTES".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SUBTRACTMINUTES".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Int64],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    if let Some(minutes) = args[1].as_i64() {
                        return ts
                            .checked_sub_signed(Duration::minutes(minutes))
                            .map(Value::timestamp)
                            .ok_or_else(|| {
                                Error::invalid_query("Date overflow in SUBTRACTMINUTES")
                            });
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP, INT64".to_string(),
                    actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );

    registry.register_scalar(
        "SUBTRACTSECONDS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SUBTRACTSECONDS".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Int64],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    if let Some(seconds) = args[1].as_i64() {
                        return ts
                            .checked_sub_signed(Duration::seconds(seconds))
                            .map(Value::timestamp)
                            .ok_or_else(|| {
                                Error::invalid_query("Date overflow in SUBTRACTSECONDS")
                            });
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP, INT64".to_string(),
                    actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );
}

fn register_now64_function(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "NOW64".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "NOW64".to_string(),
            arg_types: vec![DataType::Int64],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |_args| Ok(Value::timestamp(Utc::now())),
        }),
    );
}

fn register_todate_function(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "TODATE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TODATE".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Date,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = args[0].as_timestamp() {
                    return Ok(Value::date(ts.date_naive()));
                }
                if let Some(d) = args[0].as_date() {
                    return Ok(Value::date(d));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP or DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );
}

fn add_months_to_timestamp(
    ts: &DateTime<Utc>,
    months: i64,
) -> yachtsql_core::error::Result<DateTime<Utc>> {
    let total_months = ts.year() as i64 * 12 + ts.month() as i64 - 1 + months;
    let new_year = (total_months / 12) as i32;
    let new_month = (total_months % 12 + 1) as u32;

    ts.with_year(new_year)
        .and_then(|t| t.with_month(new_month))
        .ok_or_else(|| Error::invalid_query("Date overflow in ADDMONTHS"))
}

fn add_months_to_date(d: &NaiveDate, months: i64) -> yachtsql_core::error::Result<NaiveDate> {
    let total_months = d.year() as i64 * 12 + d.month() as i64 - 1 + months;
    let new_year = (total_months / 12) as i32;
    let new_month = (total_months % 12 + 1) as u32;

    d.with_year(new_year)
        .and_then(|dt| dt.with_month(new_month))
        .ok_or_else(|| Error::invalid_query("Date overflow in ADDMONTHS"))
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, Weekday};

    use super::*;

    #[test]
    fn test_truncation_functions() {
        let ts = DateTime::from_timestamp(1609459200, 0).unwrap();
        let date = ts.date_naive();

        let diff = date.weekday().num_days_from_monday();
        let monday = date - Duration::days(diff as i64);

        assert_eq!(monday.weekday(), Weekday::Mon);
    }

    #[test]
    fn test_add_months() {
        let date = NaiveDate::from_ymd_opt(2023, 1, 15).unwrap();
        let result = add_months_to_date(&date, 2).unwrap();
        assert_eq!(result, NaiveDate::from_ymd_opt(2023, 3, 15).unwrap());

        let date = NaiveDate::from_ymd_opt(2023, 11, 15).unwrap();
        let result = add_months_to_date(&date, 3).unwrap();
        assert_eq!(result, NaiveDate::from_ymd_opt(2024, 2, 15).unwrap());
    }

    #[test]
    fn test_formatting_functions() {
        let date = NaiveDate::from_ymd_opt(2023, 3, 15).unwrap();
        let yyyymmdd =
            (date.year() as i64) * 10000 + (date.month() as i64) * 100 + date.day() as i64;
        assert_eq!(yyyymmdd, 20230315);
    }

    #[test]
    fn test_relative_functions() {
        let date = NaiveDate::from_ymd_opt(2023, 3, 15).unwrap();
        let relative_month = date.year() as i64 * 12 + date.month() as i64 - 1;
        assert_eq!(relative_month, 24278);
    }
}
