use std::rc::Rc;

use yachtsql_core::error::Error;
use yachtsql_core::types::{DataType, Value};

use super::FunctionRegistry;
use crate::fulltext::{
    self, HeadlineOptions, Weight, parse_tsvector, tsquery_to_string, tsvector_to_string,
};
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_conversion_functions(registry);
    register_query_functions(registry);
    register_ranking_functions(registry);
    register_utility_functions(registry);
    register_operators(registry);
    register_advanced_functions(registry);
}

fn register_conversion_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "TO_TSVECTOR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TO_TSVECTOR".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "TO_TSVECTOR requires at least 1 argument".to_string(),
                    ));
                }

                if args[0].is_null() {
                    return Ok(Value::null());
                }

                let text = if args.len() == 1 {
                    args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?
                } else {
                    if args[1].is_null() {
                        return Ok(Value::null());
                    }
                    args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?
                };

                let vector = fulltext::to_tsvector(text);
                Ok(Value::string(tsvector_to_string(&vector)))
            },
        }),
    );

    registry.register_scalar(
        "TO_TSQUERY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TO_TSQUERY".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "TO_TSQUERY requires at least 1 argument".to_string(),
                    ));
                }

                if args[0].is_null() {
                    return Ok(Value::null());
                }

                let text = if args.len() == 1 {
                    args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?
                } else {
                    if args[1].is_null() {
                        return Ok(Value::null());
                    }
                    args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?
                };

                let query = fulltext::to_tsquery(text)?;
                Ok(Value::string(tsquery_to_string(&query)))
            },
        }),
    );
}

fn register_query_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "PLAINTO_TSQUERY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "PLAINTO_TSQUERY".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "PLAINTO_TSQUERY requires at least 1 argument".to_string(),
                    ));
                }

                if args[0].is_null() {
                    return Ok(Value::null());
                }

                let text = if args.len() == 1 {
                    args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?
                } else {
                    if args[1].is_null() {
                        return Ok(Value::null());
                    }
                    args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?
                };

                let query = fulltext::plainto_tsquery(text);
                Ok(Value::string(tsquery_to_string(&query)))
            },
        }),
    );

    registry.register_scalar(
        "PHRASETO_TSQUERY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "PHRASETO_TSQUERY".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "PHRASETO_TSQUERY requires at least 1 argument".to_string(),
                    ));
                }

                if args[0].is_null() {
                    return Ok(Value::null());
                }

                let text = if args.len() == 1 {
                    args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?
                } else {
                    if args[1].is_null() {
                        return Ok(Value::null());
                    }
                    args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?
                };

                let query = fulltext::phraseto_tsquery(text);
                Ok(Value::string(tsquery_to_string(&query)))
            },
        }),
    );

    registry.register_scalar(
        "WEBSEARCH_TO_TSQUERY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "WEBSEARCH_TO_TSQUERY".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "WEBSEARCH_TO_TSQUERY requires at least 1 argument".to_string(),
                    ));
                }

                if args[0].is_null() {
                    return Ok(Value::null());
                }

                let text = if args.len() == 1 {
                    args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?
                } else {
                    if args[1].is_null() {
                        return Ok(Value::null());
                    }
                    args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?
                };

                let query = fulltext::websearch_to_tsquery(text);
                Ok(Value::string(tsquery_to_string(&query)))
            },
        }),
    );
}

fn register_ranking_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "TS_RANK".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TS_RANK".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "TS_RANK requires at least 2 arguments".to_string(),
                    ));
                }

                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                let vector_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsvector)".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let query_str = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsquery)".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;

                let vector = parse_tsvector(vector_str)?;
                let query = fulltext::to_tsquery(query_str)?;

                let score = fulltext::ts_rank(&vector, &query);
                Ok(Value::float64(score))
            },
        }),
    );

    registry.register_scalar(
        "TS_RANK_CD".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TS_RANK_CD".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "TS_RANK_CD requires at least 2 arguments".to_string(),
                    ));
                }

                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                let vector_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsvector)".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let query_str = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsquery)".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;

                let vector = parse_tsvector(vector_str)?;
                let query = fulltext::to_tsquery(query_str)?;

                let score = fulltext::ts_rank_cd(&vector, &query);
                Ok(Value::float64(score))
            },
        }),
    );

    registry.register_scalar(
        "TS_HEADLINE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TS_HEADLINE".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "TS_HEADLINE requires at least 2 arguments".to_string(),
                    ));
                }

                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                let document = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let query_str = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsquery)".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;

                let query = fulltext::to_tsquery(query_str)?;
                let options = HeadlineOptions::default();

                let headline = fulltext::ts_headline(document, &query, &options);
                Ok(Value::string(headline))
            },
        }),
    );
}

fn register_utility_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "TSVECTOR_LENGTH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TSVECTOR_LENGTH".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "TSVECTOR_LENGTH requires 1 argument".to_string(),
                    ));
                }

                if args[0].is_null() {
                    return Ok(Value::null());
                }

                let vector_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsvector)".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let vector = parse_tsvector(vector_str)?;
                Ok(Value::int64(fulltext::tsvector_length(&vector)))
            },
        }),
    );

    registry.register_scalar(
        "STRIP".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "STRIP".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "STRIP requires 1 argument".to_string(),
                    ));
                }

                if args[0].is_null() {
                    return Ok(Value::null());
                }

                let vector_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsvector)".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let vector = parse_tsvector(vector_str)?;
                let stripped = fulltext::tsvector_strip(&vector);
                Ok(Value::string(tsvector_to_string(&stripped)))
            },
        }),
    );

    registry.register_scalar(
        "SETWEIGHT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SETWEIGHT".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "SETWEIGHT requires 2 arguments".to_string(),
                    ));
                }

                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                let vector_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsvector)".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let weight_str = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;

                let weight = weight_str
                    .chars()
                    .next()
                    .and_then(Weight::from_char)
                    .ok_or_else(|| {
                        Error::invalid_query(format!(
                            "Invalid weight '{}'. Must be A, B, C, or D",
                            weight_str
                        ))
                    })?;

                let vector = parse_tsvector(vector_str)?;
                let weighted = fulltext::tsvector_setweight(&vector, weight);
                Ok(Value::string(tsvector_to_string(&weighted)))
            },
        }),
    );

    registry.register_scalar(
        "TSVECTOR_CONCAT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TSVECTOR_CONCAT".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "TSVECTOR_CONCAT requires 2 arguments".to_string(),
                    ));
                }

                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                let a_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsvector)".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let b_str = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsvector)".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;

                let a = parse_tsvector(a_str)?;
                let b = parse_tsvector(b_str)?;
                let result = fulltext::tsvector_concat(&a, &b);
                Ok(Value::string(tsvector_to_string(&result)))
            },
        }),
    );
}

fn register_operators(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "TS_MATCH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TS_MATCH".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "TS_MATCH requires 2 arguments".to_string(),
                    ));
                }

                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                let vector_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsvector)".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let query_str = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsquery)".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;

                let vector = parse_tsvector(vector_str)?;
                let query = fulltext::to_tsquery(query_str)?;

                Ok(Value::bool_val(query.matches(&vector)))
            },
        }),
    );

    registry.register_scalar(
        "TSQUERY_AND".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TSQUERY_AND".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "TSQUERY_AND requires 2 arguments".to_string(),
                    ));
                }

                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                let a_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsquery)".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let b_str = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsquery)".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;

                let a = fulltext::to_tsquery(a_str)?;
                let b = fulltext::to_tsquery(b_str)?;
                let result = a.and(b);
                Ok(Value::string(tsquery_to_string(&result)))
            },
        }),
    );

    registry.register_scalar(
        "TSQUERY_OR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TSQUERY_OR".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "TSQUERY_OR requires 2 arguments".to_string(),
                    ));
                }

                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                let a_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsquery)".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let b_str = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsquery)".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;

                let a = fulltext::to_tsquery(a_str)?;
                let b = fulltext::to_tsquery(b_str)?;
                let result = a.or(b);
                Ok(Value::string(tsquery_to_string(&result)))
            },
        }),
    );

    registry.register_scalar(
        "TSQUERY_NOT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TSQUERY_NOT".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "TSQUERY_NOT requires 1 argument".to_string(),
                    ));
                }

                if args[0].is_null() {
                    return Ok(Value::null());
                }

                let query_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsquery)".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let query = fulltext::to_tsquery(query_str)?;
                let result = query.negate();
                Ok(Value::string(tsquery_to_string(&result)))
            },
        }),
    );
}

fn register_advanced_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "NUMNODE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "NUMNODE".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "NUMNODE requires 1 argument".to_string(),
                    ));
                }

                if args[0].is_null() {
                    return Ok(Value::null());
                }

                let query_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsquery)".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let query = fulltext::to_tsquery(query_str)?;
                Ok(Value::int64(fulltext::numnode(&query)))
            },
        }),
    );

    registry.register_scalar(
        "QUERYTREE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "QUERYTREE".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "QUERYTREE requires 1 argument".to_string(),
                    ));
                }

                if args[0].is_null() {
                    return Ok(Value::null());
                }

                let query_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsquery)".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let query = fulltext::to_tsquery(query_str)?;
                Ok(Value::string(fulltext::querytree(&query)))
            },
        }),
    );

    registry.register_scalar(
        "TS_REWRITE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TS_REWRITE".to_string(),
            arg_types: vec![DataType::String, DataType::String, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() < 3 {
                    return Err(Error::invalid_query(
                        "TS_REWRITE requires 3 arguments".to_string(),
                    ));
                }

                if args[0].is_null() || args[1].is_null() || args[2].is_null() {
                    return Ok(Value::null());
                }

                let query_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsquery)".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let old_str = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsquery)".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;

                let new_str = args[2].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsquery)".to_string(),
                    actual: args[2].data_type().to_string(),
                })?;

                let query = fulltext::to_tsquery(query_str)?;
                let old_query = fulltext::to_tsquery(old_str)?;
                let new_query = fulltext::to_tsquery(new_str)?;
                let result = fulltext::ts_rewrite(&query, &old_query, &new_query);
                Ok(Value::string(tsquery_to_string(&result)))
            },
        }),
    );

    registry.register_scalar(
        "TS_DELETE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TS_DELETE".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "TS_DELETE requires at least 2 arguments".to_string(),
                    ));
                }

                if args[0].is_null() {
                    return Ok(Value::null());
                }

                let vector_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsvector)".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let vector = parse_tsvector(vector_str)?;

                let mut lexemes_to_delete = Vec::new();
                for arg in &args[1..] {
                    if let Some(s) = arg.as_str() {
                        lexemes_to_delete.push(s.to_string());
                    } else if let Some(arr) = arg.as_array() {
                        for elem in arr {
                            if let Some(s) = elem.as_str() {
                                lexemes_to_delete.push(s.to_string());
                            }
                        }
                    }
                }

                let result = fulltext::ts_delete(&vector, &lexemes_to_delete);
                Ok(Value::string(tsvector_to_string(&result)))
            },
        }),
    );

    registry.register_scalar(
        "TS_FILTER".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TS_FILTER".to_string(),
            arg_types: vec![
                DataType::String,
                DataType::Array(Box::new(DataType::String)),
            ],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "TS_FILTER requires 2 arguments".to_string(),
                    ));
                }

                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                let vector_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsvector)".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let vector = parse_tsvector(vector_str)?;

                let weight_chars = args[1].as_array().ok_or_else(|| Error::TypeMismatch {
                    expected: "ARRAY of STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;

                let mut weights = Vec::new();
                for elem in weight_chars {
                    if let Some(s) = elem.as_str() {
                        if let Some(w) = s.chars().next().and_then(Weight::from_char) {
                            weights.push(w);
                        }
                    }
                }

                let result = fulltext::ts_filter(&vector, &weights);
                Ok(Value::string(tsvector_to_string(&result)))
            },
        }),
    );

    registry.register_scalar(
        "ARRAY_TO_TSVECTOR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAY_TO_TSVECTOR".to_string(),
            arg_types: vec![DataType::Array(Box::new(DataType::String))],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "ARRAY_TO_TSVECTOR requires 1 argument".to_string(),
                    ));
                }

                if args[0].is_null() {
                    return Ok(Value::null());
                }

                let lexemes = args[0].as_array().ok_or_else(|| Error::TypeMismatch {
                    expected: "ARRAY of STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let lexeme_strings: Vec<String> = lexemes
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();

                let result = fulltext::array_to_tsvector(&lexeme_strings);
                Ok(Value::string(tsvector_to_string(&result)))
            },
        }),
    );

    registry.register_scalar(
        "GET_CURRENT_TS_CONFIG".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "GET_CURRENT_TS_CONFIG".to_string(),
            arg_types: vec![],
            return_type: DataType::String,
            variadic: false,
            evaluator: |_args| Ok(Value::string(fulltext::get_current_ts_config())),
        }),
    );

    registry.register_scalar(
        "TSVECTOR_TO_ARRAY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TSVECTOR_TO_ARRAY".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Array(Box::new(DataType::String)),
            variadic: false,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "TSVECTOR_TO_ARRAY requires 1 argument".to_string(),
                    ));
                }

                if args[0].is_null() {
                    return Ok(Value::null());
                }

                let vector_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING (tsvector)".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;

                let vector = parse_tsvector(vector_str)?;
                let lexemes = fulltext::tsvector_to_array(&vector);
                let values: Vec<Value> = lexemes.into_iter().map(Value::string).collect();
                Ok(Value::array(values))
            },
        }),
    );
}
