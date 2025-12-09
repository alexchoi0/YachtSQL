use std::rc::Rc;

use chrono::{DateTime, Duration, Utc};
use yachtsql_core::error::Error;
use yachtsql_core::types::{DataType, Value};

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_tumble_functions(registry);
    register_hop_functions(registry);
    register_time_slot_functions(registry);
    register_date_bin_functions(registry);
}

fn register_tumble_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "TUMBLE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TUMBLE".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Interval],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query("tumble requires 2 arguments"));
                }
                let ts = extract_timestamp(&args[0])?;
                let interval_secs = extract_interval_seconds(&args[1])?;
                let window_start = compute_tumble_start(ts, interval_secs);
                Ok(Value::timestamp(window_start))
            },
        }),
    );

    registry.register_scalar(
        "TUMBLESTART".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TUMBLESTART".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Interval],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query("tumbleStart requires 2 arguments"));
                }
                let ts = extract_timestamp(&args[0])?;
                let interval_secs = extract_interval_seconds(&args[1])?;
                let window_start = compute_tumble_start(ts, interval_secs);
                Ok(Value::timestamp(window_start))
            },
        }),
    );

    registry.register_scalar(
        "TUMBLEEND".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TUMBLEEND".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Interval],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query("tumbleEnd requires 2 arguments"));
                }
                let ts = extract_timestamp(&args[0])?;
                let interval_secs = extract_interval_seconds(&args[1])?;
                let window_start = compute_tumble_start(ts, interval_secs);
                let window_end = window_start + Duration::seconds(interval_secs);
                Ok(Value::timestamp(window_end))
            },
        }),
    );
}

fn register_hop_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "HOP".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HOP".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Interval, DataType::Interval],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args.len() != 3 {
                    return Err(Error::invalid_query("hop requires 3 arguments"));
                }
                let ts = extract_timestamp(&args[0])?;
                let hop_secs = extract_interval_seconds(&args[1])?;
                let _window_secs = extract_interval_seconds(&args[2])?;
                let hop_start = compute_hop_start(ts, hop_secs);
                Ok(Value::timestamp(hop_start))
            },
        }),
    );

    registry.register_scalar(
        "HOPSTART".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HOPSTART".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Interval, DataType::Interval],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args.len() != 3 {
                    return Err(Error::invalid_query("hopStart requires 3 arguments"));
                }
                let ts = extract_timestamp(&args[0])?;
                let hop_secs = extract_interval_seconds(&args[1])?;
                let _window_secs = extract_interval_seconds(&args[2])?;
                let hop_start = compute_hop_start(ts, hop_secs);
                Ok(Value::timestamp(hop_start))
            },
        }),
    );

    registry.register_scalar(
        "HOPEND".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HOPEND".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Interval, DataType::Interval],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args.len() != 3 {
                    return Err(Error::invalid_query("hopEnd requires 3 arguments"));
                }
                let ts = extract_timestamp(&args[0])?;
                let hop_secs = extract_interval_seconds(&args[1])?;
                let window_secs = extract_interval_seconds(&args[2])?;
                let hop_start = compute_hop_start(ts, hop_secs);
                let hop_end = hop_start + Duration::seconds(window_secs);
                Ok(Value::timestamp(hop_end))
            },
        }),
    );
}

fn register_time_slot_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "TIMESLOT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TIMESLOT".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("timeSlot requires 1 argument"));
                }
                let ts = extract_timestamp(&args[0])?;
                let slot_start = compute_tumble_start(ts, 1800);
                Ok(Value::timestamp(slot_start))
            },
        }),
    );

    registry.register_scalar(
        "TIMESLOTS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TIMESLOTS".to_string(),
            arg_types: vec![DataType::Timestamp, DataType::Int64],
            return_type: DataType::Array(Box::new(DataType::Timestamp)),
            variadic: true,
            evaluator: |args| {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::invalid_query("timeSlots requires 2 or 3 arguments"));
                }
                let start_ts = extract_timestamp(&args[0])?;
                let duration_secs = extract_i64(&args[1])?;
                let slot_size_secs = if args.len() == 3 {
                    extract_i64(&args[2])?
                } else {
                    1800
                };

                if slot_size_secs <= 0 {
                    return Err(Error::invalid_query(
                        "timeSlots: slot size must be positive",
                    ));
                }

                let slot_start = compute_tumble_start(start_ts, slot_size_secs);
                let end_ts = start_ts + Duration::seconds(duration_secs);

                let mut slots = Vec::new();
                let mut current = slot_start;
                while current < end_ts {
                    slots.push(Value::timestamp(current));
                    current += Duration::seconds(slot_size_secs);
                }

                Ok(Value::array(slots))
            },
        }),
    );

    registry.register_scalar(
        "WINDOWID".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "WINDOWID".to_string(),
            arg_types: vec![DataType::Timestamp],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("windowID requires 1 argument"));
                }
                let ts = extract_timestamp(&args[0])?;
                let window_id = ts.timestamp();
                Ok(Value::int64(window_id))
            },
        }),
    );
}

fn register_date_bin_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "DATE_BIN".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "DATE_BIN".to_string(),
            arg_types: vec![DataType::Interval, DataType::Timestamp],
            return_type: DataType::Timestamp,
            variadic: true,
            evaluator: |args| {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::invalid_query("date_bin requires 2 or 3 arguments"));
                }
                let interval_secs = extract_interval_seconds(&args[0])?;
                let ts = extract_timestamp(&args[1])?;
                let origin = if args.len() == 3 {
                    extract_timestamp(&args[2])?
                } else {
                    DateTime::from_timestamp(0, 0).unwrap()
                };

                let ts_secs = ts.timestamp();
                let origin_secs = origin.timestamp();
                let offset = ts_secs - origin_secs;
                let bin_num = offset / interval_secs;
                let bin_start_secs = origin_secs + bin_num * interval_secs;

                let result = DateTime::from_timestamp(bin_start_secs, 0)
                    .ok_or_else(|| Error::invalid_query("date_bin: invalid result timestamp"))?;
                Ok(Value::timestamp(result))
            },
        }),
    );
}

fn extract_timestamp(value: &Value) -> Result<DateTime<Utc>, Error> {
    if value.is_null() {
        return Err(Error::invalid_query("Expected timestamp value, got NULL"));
    }
    if let Some(ts) = value.as_timestamp() {
        return Ok(ts);
    }
    Err(Error::TypeMismatch {
        expected: "TIMESTAMP".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn extract_interval_seconds(value: &Value) -> Result<i64, Error> {
    if value.is_null() {
        return Err(Error::invalid_query("Expected interval value, got NULL"));
    }
    if let Some(interval) = value.as_interval() {
        let months = interval.months;
        let days = interval.days;
        let micros = interval.micros;
        let total_secs =
            (months as i64) * 30 * 24 * 3600 + (days as i64) * 24 * 3600 + micros / 1_000_000;
        return Ok(total_secs);
    }
    if let Some(i) = value.as_i64() {
        return Ok(i);
    }
    Err(Error::TypeMismatch {
        expected: "INTERVAL".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn extract_i64(value: &Value) -> Result<i64, Error> {
    if value.is_null() {
        return Err(Error::invalid_query("Expected integer value, got NULL"));
    }
    if let Some(i) = value.as_i64() {
        return Ok(i);
    }
    Err(Error::TypeMismatch {
        expected: "Int64".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn compute_tumble_start(ts: DateTime<Utc>, interval_secs: i64) -> DateTime<Utc> {
    let ts_secs = ts.timestamp();
    let window_num = ts_secs / interval_secs;
    let window_start_secs = window_num * interval_secs;
    DateTime::from_timestamp(window_start_secs, 0).unwrap()
}

fn compute_hop_start(ts: DateTime<Utc>, hop_secs: i64) -> DateTime<Utc> {
    let ts_secs = ts.timestamp();
    let hop_num = ts_secs / hop_secs;
    let hop_start_secs = hop_num * hop_secs;
    DateTime::from_timestamp(hop_start_secs, 0).unwrap()
}
