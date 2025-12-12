use std::rc::Rc;

use regex::Regex;
use yachtsql_common::error::Error;
use yachtsql_common::types::{DataType, Value};

use super::FunctionRegistry;
use crate::scalar::{self, ScalarFunctionImpl};

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_core_string_funcs(registry);
    register_string_predicates(registry);
    register_string_mutators(registry);
    register_string_extraction(registry);
    register_string_padding(registry);
    register_string_transformation(registry);
    register_string_search(registry);
    register_regex_functions(registry);
    register_character_functions(registry);
    register_formatting_functions(registry);
    register_postgres_text_functions(registry);
    register_conversion_functions(registry);
}

fn register_core_string_funcs(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "CONCAT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "CONCAT".to_string(),
            arg_types: vec![],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                let mut result = String::new();
                for arg in args {
                    if arg.is_null() {
                        continue;
                    }
                    if let Some(s) = arg.as_str() {
                        result.push_str(s);
                    } else if let Some(i) = arg.as_i64() {
                        result.push_str(&i.to_string());
                    } else if let Some(f) = arg.as_f64() {
                        result.push_str(&f.to_string());
                    } else if let Some(d) = arg.as_numeric() {
                        result.push_str(&d.to_string());
                    } else if let Some(b) = arg.as_bool() {
                        result.push_str(&b.to_string());
                    } else if let Some(d) = arg.as_date() {
                        result.push_str(&d.to_string());
                    } else if let Some(dt) = arg.as_datetime() {
                        result.push_str(&dt.to_string());
                    } else if let Some(t) = arg.as_time() {
                        result.push_str(&t.to_string());
                    } else if let Some(ts) = arg.as_timestamp() {
                        result.push_str(&ts.to_string());
                    } else if let Some(b) = arg.as_bytes() {
                        result.push_str(&format!("{:?}", b));
                    } else if let Some(wkt) = arg.as_geography() {
                        result.push_str(wkt);
                    } else if arg.is_array() {
                        result.push_str("[array]");
                    } else if arg.as_struct().is_some() {
                        result.push_str("{struct}");
                    } else if let Some(j) = arg.as_json() {
                        result.push_str(&j.to_string());
                    } else if let Some(u) = arg.as_uuid() {
                        result.push_str(&u.hyphenated().to_string().to_lowercase());
                    }
                }
                Ok(Value::string(result))
            },
        }),
    );

    registry.register_scalar(
        "UPPER".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "UPPER".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    Ok(Value::string(s.to_uppercase()))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "LOWER".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "LOWER".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    Ok(Value::string(s.to_lowercase()))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "LENGTH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "LENGTH".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| scalar::eval_length(&args[0]),
        }),
    );

    registry.register_scalar(
        "SUBSTR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SUBSTR".to_string(),
            arg_types: vec![DataType::String, DataType::Int64, DataType::Int64],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() || args[2].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_str(), args[1].as_i64(), args[2].as_i64()) {
                    (Some(s), Some(start), Some(length)) => {
                        let start_idx = (start - 1).max(0) as usize;
                        let end_idx = (start_idx + length as usize).min(s.len());
                        if start_idx >= s.len() {
                            Ok(Value::string(String::new()))
                        } else {
                            Ok(Value::string(s[start_idx..end_idx].to_string()))
                        }
                    }
                    _ => Err(Error::invalid_query(
                        "SUBSTR requires (STRING, INT64, INT64)".to_string(),
                    )),
                }
            },
        }),
    );

    registry.register_scalar(
        "REPLACE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "REPLACE".to_string(),
            arg_types: vec![DataType::String, DataType::String, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() || args[2].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_str(), args[1].as_str(), args[2].as_str()) {
                    (Some(s), Some(from), Some(to)) => Ok(Value::string(s.replace(from, to))),
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, STRING, STRING".to_string(),
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
        "SPLIT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SPLIT".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Array(Box::new(DataType::String)),
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_str(), args[1].as_str()) {
                    (Some(s), Some(delimiter)) => {
                        if delimiter.is_empty() {
                            let chars = s
                                .chars()
                                .map(|c| Value::string(c.to_string()))
                                .collect::<Vec<_>>();
                            Ok(Value::array(chars))
                        } else {
                            let parts = s
                                .split(delimiter)
                                .map(|part| Value::string(part.to_string()))
                                .collect::<Vec<_>>();
                            Ok(Value::array(parts))
                        }
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
        "STRING_TO_ARRAY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "STRING_TO_ARRAY".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Array(Box::new(DataType::String)),
            variadic: true,
            evaluator: |args| {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::invalid_query(
                        "STRING_TO_ARRAY requires 2 or 3 arguments".to_string(),
                    ));
                }

                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                match (args[0].as_str(), args[1].as_str()) {
                    (Some(s), Some(delimiter)) => {
                        let null_string = if args.len() == 3 && !args[2].is_null() {
                            args[2].as_str()
                        } else {
                            None
                        };

                        if delimiter.is_empty() {
                            let chars = s
                                .chars()
                                .map(|c| {
                                    let char_str = c.to_string();
                                    if Some(char_str.as_str()) == null_string {
                                        Value::null()
                                    } else {
                                        Value::string(char_str)
                                    }
                                })
                                .collect::<Vec<_>>();
                            Ok(Value::array(chars))
                        } else {
                            let parts = s
                                .split(delimiter)
                                .map(|part| {
                                    if Some(part) == null_string {
                                        Value::null()
                                    } else {
                                        Value::string(part.to_string())
                                    }
                                })
                                .collect::<Vec<_>>();
                            Ok(Value::array(parts))
                        }
                    }
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, STRING".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );
}

fn register_string_predicates(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "STARTS_WITH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "STARTS_WITH".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_str(), args[1].as_str()) {
                    (Some(s), Some(prefix)) => Ok(Value::bool_val(s.starts_with(prefix))),
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, STRING".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );

    registry.register_scalar(
        "ENDS_WITH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ENDS_WITH".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_str(), args[1].as_str()) {
                    (Some(s), Some(suffix)) => Ok(Value::bool_val(s.ends_with(suffix))),
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, STRING".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );
}

fn register_string_mutators(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "TRIM".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TRIM".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    Ok(Value::string(s.trim().to_string()))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "LTRIM".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "LTRIM".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    Ok(Value::string(s.trim_start().to_string()))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "RTRIM".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RTRIM".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    Ok(Value::string(s.trim_end().to_string()))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "TRIM_CHARS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TRIM_CHARS".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_str(), args[1].as_str()) {
                    (Some(s), Some(chars)) => {
                        let result = s.trim_matches(|c: char| chars.contains(c));
                        Ok(Value::string(result.to_string()))
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
        "LTRIM_CHARS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "LTRIM_CHARS".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_str(), args[1].as_str()) {
                    (Some(s), Some(chars)) => {
                        let result = s.trim_start_matches(|c: char| chars.contains(c));
                        Ok(Value::string(result.to_string()))
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
        "RTRIM_CHARS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RTRIM_CHARS".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_str(), args[1].as_str()) {
                    (Some(s), Some(chars)) => {
                        let result = s.trim_end_matches(|c: char| chars.contains(c));
                        Ok(Value::string(result.to_string()))
                    }
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, STRING".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );
}

fn register_string_extraction(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "LEFT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "LEFT".to_string(),
            arg_types: vec![DataType::String, DataType::Int64],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_str(), args[1].as_i64()) {
                    (Some(s), Some(n)) => {
                        let n = n.max(0) as usize;
                        Ok(Value::string(s.chars().take(n).collect()))
                    }
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, INT64".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );

    registry.register_scalar(
        "RIGHT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RIGHT".to_string(),
            arg_types: vec![DataType::String, DataType::Int64],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_str(), args[1].as_i64()) {
                    (Some(s), Some(n)) => {
                        let n = n.max(0) as usize;
                        let chars: Vec<char> = s.chars().collect();
                        let start = chars.len().saturating_sub(n);
                        Ok(Value::string(chars[start..].iter().collect()))
                    }
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, INT64".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );

    registry.register_scalar(
        "SUBSTRING".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SUBSTRING".to_string(),
            arg_types: vec![DataType::String, DataType::Int64],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::invalid_query(
                        "SUBSTRING requires 2 or 3 arguments".to_string(),
                    ));
                }

                if args[0].is_null() {
                    return Ok(Value::null());
                }

                if let Some(s) = args[0].as_str() {
                    let start = if args[1].is_null() {
                        return Ok(Value::null());
                    } else if let Some(n) = args[1].as_i64() {
                        (n - 1).max(0) as usize
                    } else {
                        return Err(Error::TypeMismatch {
                            expected: "INT64".to_string(),
                            actual: args[1].data_type().to_string(),
                        });
                    };

                    let chars: Vec<char> = s.chars().collect();
                    if start >= chars.len() {
                        return Ok(Value::string(String::new()));
                    }

                    if args.len() == 3 {
                        let length = if args[2].is_null() {
                            return Ok(Value::null());
                        } else if let Some(n) = args[2].as_i64() {
                            n.max(0) as usize
                        } else {
                            return Err(Error::TypeMismatch {
                                expected: "INT64".to_string(),
                                actual: args[2].data_type().to_string(),
                            });
                        };
                        let end = (start + length).min(chars.len());
                        Ok(Value::string(chars[start..end].iter().collect()))
                    } else {
                        Ok(Value::string(chars[start..].iter().collect()))
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "SPLIT_PART".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SPLIT_PART".to_string(),
            arg_types: vec![DataType::String, DataType::String, DataType::Int64],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() || args[2].is_null() {
                    return Ok(Value::null());
                }

                match (args[0].as_str(), args[1].as_str(), args[2].as_i64()) {
                    (Some(string), Some(delimiter), Some(field)) => {
                        if field == 0 {
                            return Err(Error::invalid_query(
                                "SPLIT_PART field position must be non-zero".to_string(),
                            ));
                        }

                        let parts: Vec<&str> = if delimiter.is_empty() {
                            string.chars().map(|_| "").collect::<Vec<_>>()
                        } else {
                            string.split(delimiter).collect()
                        };

                        let idx = if field < 0 {
                            let abs_field = (-field) as usize;
                            if abs_field > parts.len() {
                                return Ok(Value::string(String::new()));
                            }
                            parts.len() - abs_field
                        } else {
                            let field_idx = (field - 1) as usize;
                            if field_idx >= parts.len() {
                                return Ok(Value::string(String::new()));
                            }
                            field_idx
                        };

                        Ok(Value::string(parts[idx].to_string()))
                    }
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, STRING, INT64".to_string(),
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
}

fn register_string_padding(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "LPAD".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "LPAD".to_string(),
            arg_types: vec![DataType::String, DataType::Int64],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::invalid_query(
                        "LPAD requires 2 or 3 arguments".to_string(),
                    ));
                }

                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                let s = match args[0].as_str() {
                    Some(s) => s,
                    None => {
                        return Err(Error::TypeMismatch {
                            expected: "STRING".to_string(),
                            actual: args[0].data_type().to_string(),
                        });
                    }
                };

                let length = match args[1].as_i64() {
                    Some(n) => n.max(0) as usize,
                    None => {
                        return Err(Error::TypeMismatch {
                            expected: "INT64".to_string(),
                            actual: args[1].data_type().to_string(),
                        });
                    }
                };

                let pad = if args.len() == 3 {
                    if args[2].is_null() {
                        return Ok(Value::null());
                    }
                    match args[2].as_str() {
                        Some(p) => p,
                        None => {
                            return Err(Error::TypeMismatch {
                                expected: "STRING".to_string(),
                                actual: args[2].data_type().to_string(),
                            });
                        }
                    }
                } else {
                    " "
                };

                let s_len = s.chars().count();

                if s_len >= length {
                    Ok(Value::string(s.chars().take(length).collect()))
                } else if pad.is_empty() {
                    Err(Error::invalid_query(
                        "LPAD: pad string cannot be empty".to_string(),
                    ))
                } else {
                    let pad_len = length - s_len;
                    let pad_chars: Vec<char> = pad.chars().collect();
                    let mut result = String::new();

                    for i in 0..pad_len {
                        result.push(pad_chars[i % pad_chars.len()]);
                    }
                    result.push_str(s);
                    Ok(Value::string(result))
                }
            },
        }),
    );

    registry.register_scalar(
        "RPAD".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RPAD".to_string(),
            arg_types: vec![DataType::String, DataType::Int64],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::invalid_query(
                        "RPAD requires 2 or 3 arguments".to_string(),
                    ));
                }

                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                let s = match args[0].as_str() {
                    Some(s) => s,
                    None => {
                        return Err(Error::TypeMismatch {
                            expected: "STRING".to_string(),
                            actual: args[0].data_type().to_string(),
                        });
                    }
                };

                let length = match args[1].as_i64() {
                    Some(n) => n.max(0) as usize,
                    None => {
                        return Err(Error::TypeMismatch {
                            expected: "INT64".to_string(),
                            actual: args[1].data_type().to_string(),
                        });
                    }
                };

                let pad = if args.len() == 3 {
                    if args[2].is_null() {
                        return Ok(Value::null());
                    }
                    match args[2].as_str() {
                        Some(p) => p,
                        None => {
                            return Err(Error::TypeMismatch {
                                expected: "STRING".to_string(),
                                actual: args[2].data_type().to_string(),
                            });
                        }
                    }
                } else {
                    " "
                };

                let s_len = s.chars().count();

                if s_len >= length {
                    Ok(Value::string(s.chars().take(length).collect()))
                } else if pad.is_empty() {
                    Err(Error::invalid_query(
                        "RPAD: pad string cannot be empty".to_string(),
                    ))
                } else {
                    let pad_len = length - s_len;
                    let pad_chars: Vec<char> = pad.chars().collect();
                    let mut result = s.to_string();

                    for i in 0..pad_len {
                        result.push(pad_chars[i % pad_chars.len()]);
                    }
                    Ok(Value::string(result))
                }
            },
        }),
    );
}

fn register_string_transformation(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "REVERSE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "REVERSE".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    Ok(Value::string(s.chars().rev().collect()))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "REPEAT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "REPEAT".to_string(),
            arg_types: vec![DataType::String, DataType::Int64],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_str(), args[1].as_i64()) {
                    (Some(s), Some(n)) => {
                        if n < 0 {
                            Err(Error::invalid_query(
                                "REPEAT count must be non-negative".to_string(),
                            ))
                        } else if n > 10000 {
                            Err(Error::invalid_query(
                                "REPEAT count too large (max 10000)".to_string(),
                            ))
                        } else {
                            Ok(Value::string(s.repeat(n as usize)))
                        }
                    }
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, INT64".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );

    registry.register_scalar(
        "INITCAP".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "INITCAP".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    let mut result = String::new();
                    let mut capitalize_next = true;

                    for ch in s.chars() {
                        if ch.is_alphabetic() {
                            if capitalize_next {
                                result.extend(ch.to_uppercase());
                                capitalize_next = false;
                            } else {
                                result.extend(ch.to_lowercase());
                            }
                        } else {
                            result.push(ch);
                            capitalize_next = ch.is_whitespace();
                        }
                    }
                    Ok(Value::string(result))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "TRANSLATE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TRANSLATE".to_string(),
            arg_types: vec![DataType::String, DataType::String, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() || args[2].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_str(), args[1].as_str(), args[2].as_str()) {
                    (Some(s), Some(from), Some(to)) => {
                        let from_chars: Vec<char> = from.chars().collect();
                        let to_chars: Vec<char> = to.chars().collect();

                        let result: String = s
                            .chars()
                            .filter_map(|ch| {
                                if let Some(pos) = from_chars.iter().position(|&c| c == ch) {
                                    to_chars.get(pos).copied()
                                } else {
                                    Some(ch)
                                }
                            })
                            .collect();

                        Ok(Value::string(result))
                    }
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, STRING, STRING".to_string(),
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
}

fn register_string_search(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "POSITION".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "POSITION".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_str(), args[1].as_str()) {
                    (Some(substring), Some(string)) => {
                        if let Some(pos) = string.find(substring) {
                            Ok(Value::int64((pos + 1) as i64))
                        } else {
                            Ok(Value::int64(0))
                        }
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
        "INSTR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "INSTR".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_str(), args[1].as_str()) {
                    (Some(string), Some(substring)) => {
                        if let Some(pos) = string.find(substring) {
                            Ok(Value::int64((pos + 1) as i64))
                        } else {
                            Ok(Value::int64(0))
                        }
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
        "STRPOS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "STRPOS".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_str(), args[1].as_str()) {
                    (Some(string), Some(substring)) => {
                        if let Some(pos) = string.find(substring) {
                            Ok(Value::int64((pos + 1) as i64))
                        } else {
                            Ok(Value::int64(0))
                        }
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
        "LOCATE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "LOCATE".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Int64,
            variadic: true,
            evaluator: |args| {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::invalid_query(
                        "LOCATE requires 2 or 3 arguments".to_string(),
                    ));
                }

                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                match (args[0].as_str(), args[1].as_str()) {
                    (Some(substring), Some(string)) => {
                        let start_pos = if args.len() == 3 {
                            if args[2].is_null() {
                                return Ok(Value::null());
                            } else if let Some(pos) = args[2].as_i64() {
                                (pos - 1).max(0) as usize
                            } else {
                                return Err(Error::TypeMismatch {
                                    expected: "INT64".to_string(),
                                    actual: args[2].data_type().to_string(),
                                });
                            }
                        } else {
                            0
                        };

                        if start_pos >= string.len() {
                            return Ok(Value::int64(0));
                        }

                        if let Some(pos) = string[start_pos..].find(substring) {
                            Ok(Value::int64((start_pos + pos + 1) as i64))
                        } else {
                            Ok(Value::int64(0))
                        }
                    }
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, STRING".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );
}

fn register_regex_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "REGEXP_SUBSTR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "REGEXP_SUBSTR".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                match (args[0].as_str(), args[1].as_str()) {
                    (Some(string), Some(pattern)) => match Regex::new(pattern) {
                        Ok(re) => {
                            if let Some(mat) = re.find(string) {
                                Ok(Value::string(mat.as_str().to_string()))
                            } else {
                                Ok(Value::null())
                            }
                        }
                        Err(e) => Err(Error::invalid_query(format!(
                            "Invalid regex pattern: {}",
                            e
                        ))),
                    },
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, STRING".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );

    registry.register_scalar(
        "REGEXP_REPLACE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "REGEXP_REPLACE".to_string(),
            arg_types: vec![DataType::String, DataType::String, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() || args[2].is_null() {
                    return Ok(Value::null());
                }

                match (args[0].as_str(), args[1].as_str(), args[2].as_str()) {
                    (Some(string), Some(pattern), Some(replacement)) => match Regex::new(pattern) {
                        Ok(re) => {
                            let result = re.replace_all(string, replacement).to_string();
                            Ok(Value::string(result))
                        }
                        Err(e) => Err(Error::invalid_query(format!(
                            "Invalid regex pattern: {}",
                            e
                        ))),
                    },
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, STRING, STRING".to_string(),
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
}

fn register_character_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ASCII".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ASCII".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    if s.is_empty() {
                        Ok(Value::int64(0))
                    } else {
                        let first_char = s
                            .chars()
                            .next()
                            .ok_or_else(|| Error::internal("Non-empty string has no characters"))?;
                        Ok(Value::int64(first_char as i64))
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "CHR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "CHR".to_string(),
            arg_types: vec![DataType::Int64],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(code) = args[0].as_i64() {
                    if code < 0 || code > 1_114_111 {
                        Err(Error::invalid_query(format!(
                            "Invalid character code: {}",
                            code
                        )))
                    } else if let Some(ch) = char::from_u32(code as u32) {
                        Ok(Value::string(ch.to_string()))
                    } else {
                        Err(Error::invalid_query(format!(
                            "Invalid Unicode code point: {}",
                            code
                        )))
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "INT64".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "CHAR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "CHAR".to_string(),
            arg_types: vec![DataType::Int64],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(code) = args[0].as_i64() {
                    if code < 0 || code > 1_114_111 {
                        Err(Error::invalid_query(format!(
                            "Invalid character code: {}",
                            code
                        )))
                    } else if let Some(ch) = char::from_u32(code as u32) {
                        Ok(Value::string(ch.to_string()))
                    } else {
                        Err(Error::invalid_query(format!(
                            "Invalid Unicode code point: {}",
                            code
                        )))
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "INT64".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "CHAR_LENGTH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "CHAR_LENGTH".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    Ok(Value::int64(s.chars().count() as i64))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "CHARACTER_LENGTH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "CHARACTER_LENGTH".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    Ok(Value::int64(s.chars().count() as i64))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "OCTET_LENGTH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "OCTET_LENGTH".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    Ok(Value::int64(s.len() as i64))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "BIT_LENGTH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "BIT_LENGTH".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    Ok(Value::int64((s.len() * 8) as i64))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );
}

fn format_value_as_string(v: &Value) -> String {
    if v.is_null() {
        "NULL".to_string()
    } else if let Some(s) = v.as_str() {
        s.to_string()
    } else {
        v.to_string()
    }
}

fn format_value_as_identifier(v: &Value) -> String {
    if v.is_null() {
        "NULL".to_string()
    } else {
        let s = if let Some(s) = v.as_str() {
            s.to_string()
        } else {
            v.to_string()
        };
        let escaped = s.replace('"', "\"\"");
        format!("\"{}\"", escaped)
    }
}

fn format_value_as_literal(v: &Value) -> String {
    if v.is_null() {
        "NULL".to_string()
    } else {
        let s = if let Some(s) = v.as_str() {
            s.to_string()
        } else {
            v.to_string()
        };
        let escaped = s.replace('\'', "''");
        format!("'{}'", escaped)
    }
}

fn register_formatting_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "FORMAT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "FORMAT".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "FORMAT requires at least 1 argument".to_string(),
                    ));
                }

                if args[0].is_null() {
                    return Ok(Value::null());
                }

                let format_str = match args[0].as_str() {
                    Some(s) => s,
                    None => {
                        return Err(Error::TypeMismatch {
                            expected: "STRING".to_string(),
                            actual: args[0].data_type().to_string(),
                        });
                    }
                };

                let mut result = String::new();
                let mut chars = format_str.chars().peekable();
                let mut arg_idx = 1;

                while let Some(ch) = chars.next() {
                    if ch == '%' {
                        if let Some(&next_ch) = chars.peek() {
                            if next_ch == '%' {
                                chars.next();
                                result.push('%');
                                continue;
                            }

                            let mut positional_arg: Option<usize> = None;
                            let mut num_str = String::new();

                            while let Some(&c) = chars.peek() {
                                if c.is_ascii_digit() {
                                    num_str.push(c);
                                    chars.next();
                                } else if c == '$' && !num_str.is_empty() {
                                    chars.next();
                                    positional_arg = num_str.parse::<usize>().ok();
                                    num_str.clear();
                                    break;
                                } else {
                                    break;
                                }
                            }

                            while let Some(&c) = chars.peek() {
                                if c == '.'
                                    || c == '-'
                                    || c == '+'
                                    || c == ' '
                                    || c.is_ascii_digit()
                                {
                                    chars.next();
                                } else {
                                    break;
                                }
                            }

                            if let Some(fmt_type) = chars.next() {
                                let actual_arg_idx = match positional_arg {
                                    Some(pos) => pos,
                                    None => {
                                        let idx = arg_idx;
                                        arg_idx += 1;
                                        idx
                                    }
                                };

                                if actual_arg_idx >= args.len() || actual_arg_idx == 0 {
                                    return Err(Error::invalid_query(
                                        "FORMAT: not enough arguments for format string"
                                            .to_string(),
                                    ));
                                }

                                let arg = &args[actual_arg_idx];

                                let formatted = match fmt_type {
                                    's' => format_value_as_string(arg),
                                    'I' => format_value_as_identifier(arg),
                                    'L' => format_value_as_literal(arg),
                                    'd' | 'i' => {
                                        if arg.is_null() {
                                            "NULL".to_string()
                                        } else if let Some(i) = arg.as_i64() {
                                            i.to_string()
                                        } else {
                                            return Err(Error::TypeMismatch {
                                                expected: "INT64".to_string(),
                                                actual: arg.data_type().to_string(),
                                            });
                                        }
                                    }
                                    'f' => {
                                        if arg.is_null() {
                                            "NULL".to_string()
                                        } else if let Some(f) = arg.as_f64() {
                                            f.to_string()
                                        } else {
                                            return Err(Error::TypeMismatch {
                                                expected: "FLOAT64".to_string(),
                                                actual: arg.data_type().to_string(),
                                            });
                                        }
                                    }
                                    _ => {
                                        if positional_arg.is_none() {
                                            arg_idx -= 1;
                                        }
                                        result.push('%');
                                        result.push(fmt_type);
                                        continue;
                                    }
                                };

                                result.push_str(&formatted);
                            }
                        } else {
                            result.push(ch);
                        }
                    } else {
                        result.push(ch);
                    }
                }

                Ok(Value::string(result))
            },
        }),
    );

    registry.register_scalar(
        "QUOTE_IDENT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "QUOTE_IDENT".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    let escaped = s.replace('"', "\"\"");
                    Ok(Value::string(format!("\"{}\"", escaped)))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "QUOTE_LITERAL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "QUOTE_LITERAL".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    let escaped = s.replace('\'', "''");
                    Ok(Value::string(format!("'{}'", escaped)))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "QUOTE_NULLABLE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "QUOTE_NULLABLE".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::string("NULL".to_string()));
                }
                if let Some(s) = args[0].as_str() {
                    let escaped = s.replace('\'', "''");
                    Ok(Value::string(format!("'{}'", escaped)))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );
}

fn register_postgres_text_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "BTRIM".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "BTRIM".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::invalid_query(
                        "BTRIM requires 1 or 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = match args[0].as_str() {
                    Some(s) => s,
                    None => {
                        return Err(Error::TypeMismatch {
                            expected: "STRING".to_string(),
                            actual: args[0].data_type().to_string(),
                        });
                    }
                };
                let chars_to_trim = if args.len() == 2 {
                    if args[1].is_null() {
                        return Ok(Value::null());
                    }
                    match args[1].as_str() {
                        Some(c) => c,
                        None => {
                            return Err(Error::TypeMismatch {
                                expected: "STRING".to_string(),
                                actual: args[1].data_type().to_string(),
                            });
                        }
                    }
                } else {
                    " "
                };
                let result = s.trim_matches(|c: char| chars_to_trim.contains(c));
                Ok(Value::string(result.to_string()))
            },
        }),
    );

    registry.register_scalar(
        "CONCAT_WS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "CONCAT_WS".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "CONCAT_WS requires at least 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let separator = match args[0].as_str() {
                    Some(s) => s,
                    None => {
                        return Err(Error::TypeMismatch {
                            expected: "STRING".to_string(),
                            actual: args[0].data_type().to_string(),
                        });
                    }
                };
                let parts: Vec<String> = args[1..]
                    .iter()
                    .filter(|arg| !arg.is_null())
                    .map(|arg| {
                        if let Some(s) = arg.as_str() {
                            s.to_string()
                        } else {
                            arg.to_string()
                        }
                    })
                    .collect();
                Ok(Value::string(parts.join(separator)))
            },
        }),
    );

    registry.register_scalar(
        "TO_HEX".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TO_HEX".to_string(),
            arg_types: vec![DataType::Int64],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(n) = args[0].as_i64() {
                    Ok(Value::string(format!("{:x}", n)))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "INT64".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "PARSE_IDENT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "PARSE_IDENT".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Array(Box::new(DataType::String)),
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    let parts: Vec<Value> = s
                        .split('.')
                        .map(|part| {
                            let trimmed = part.trim();
                            let unquoted = if trimmed.starts_with('"')
                                && trimmed.ends_with('"')
                                && trimmed.len() >= 2
                            {
                                trimmed[1..trimmed.len() - 1].replace("\"\"", "\"")
                            } else {
                                trimmed.to_lowercase()
                            };
                            Value::string(unquoted)
                        })
                        .collect();
                    Ok(Value::array(parts))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "NORMALIZE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "NORMALIZE".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    Ok(Value::string(s.to_string()))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "IS_NORMALIZED".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "IS_NORMALIZED".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if args[0].as_str().is_some() {
                    Ok(Value::bool_val(true))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "REGEXP_COUNT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "REGEXP_COUNT".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_str(), args[1].as_str()) {
                    (Some(string), Some(pattern)) => match Regex::new(pattern) {
                        Ok(re) => {
                            let count = re.find_iter(string).count();
                            Ok(Value::int64(count as i64))
                        }
                        Err(e) => Err(Error::invalid_query(format!(
                            "Invalid regex pattern: {}",
                            e
                        ))),
                    },
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, STRING".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );

    registry.register_scalar(
        "REGEXP_INSTR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "REGEXP_INSTR".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_str(), args[1].as_str()) {
                    (Some(string), Some(pattern)) => match Regex::new(pattern) {
                        Ok(re) => {
                            if let Some(mat) = re.find(string) {
                                Ok(Value::int64((mat.start() + 1) as i64))
                            } else {
                                Ok(Value::int64(0))
                            }
                        }
                        Err(e) => Err(Error::invalid_query(format!(
                            "Invalid regex pattern: {}",
                            e
                        ))),
                    },
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, STRING".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );

    registry.register_scalar(
        "OVERLAY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "OVERLAY".to_string(),
            arg_types: vec![DataType::String, DataType::String, DataType::Int64],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                if args.len() < 3 || args.len() > 4 {
                    return Err(Error::invalid_query(
                        "OVERLAY requires 3 or 4 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() || args[2].is_null() {
                    return Ok(Value::null());
                }
                let string = match args[0].as_str() {
                    Some(s) => s,
                    None => {
                        return Err(Error::TypeMismatch {
                            expected: "STRING".to_string(),
                            actual: args[0].data_type().to_string(),
                        });
                    }
                };
                let replacement = match args[1].as_str() {
                    Some(s) => s,
                    None => {
                        return Err(Error::TypeMismatch {
                            expected: "STRING".to_string(),
                            actual: args[1].data_type().to_string(),
                        });
                    }
                };
                let start = match args[2].as_i64() {
                    Some(n) => n,
                    None => {
                        return Err(Error::TypeMismatch {
                            expected: "INT64".to_string(),
                            actual: args[2].data_type().to_string(),
                        });
                    }
                };
                let length = if args.len() == 4 {
                    if args[3].is_null() {
                        return Ok(Value::null());
                    }
                    match args[3].as_i64() {
                        Some(n) => n as usize,
                        None => {
                            return Err(Error::TypeMismatch {
                                expected: "INT64".to_string(),
                                actual: args[3].data_type().to_string(),
                            });
                        }
                    }
                } else {
                    replacement.chars().count()
                };

                let chars: Vec<char> = string.chars().collect();
                let start_idx = (start - 1).max(0) as usize;

                let mut result = String::new();
                for (i, ch) in chars.iter().enumerate() {
                    if i < start_idx {
                        result.push(*ch);
                    }
                }
                result.push_str(replacement);
                let skip_end = start_idx + length;
                for (i, ch) in chars.iter().enumerate() {
                    if i >= skip_end {
                        result.push(*ch);
                    }
                }

                Ok(Value::string(result))
            },
        }),
    );
}

fn register_conversion_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "TO_NUMBER".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TO_NUMBER".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }

                if let Some(s) = args[0].as_str() {
                    let trimmed = s.trim();

                    if trimmed.is_empty() {
                        return Ok(Value::null());
                    }

                    match trimmed.parse::<f64>() {
                        Ok(num) => {
                            if num.is_infinite() || num.is_nan() {
                                Ok(Value::null())
                            } else {
                                Ok(Value::float64(num))
                            }
                        }
                        Err(_) => Ok(Value::null()),
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "TO_CHAR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TO_CHAR".to_string(),
            arg_types: vec![DataType::Float64],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }

                let s = args[0].to_string();
                Ok(Value::string(s))
            },
        }),
    );
}
