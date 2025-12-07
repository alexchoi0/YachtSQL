use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_functions::fulltext::{
    self, HeadlineOptions, Weight, parse_tsvector, tsquery_to_string, tsvector_to_string,
};
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_fulltext_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "TO_TSVECTOR" => Self::eval_to_tsvector(args, batch, row_idx),
            "TO_TSQUERY" => Self::eval_to_tsquery(args, batch, row_idx),
            "PLAINTO_TSQUERY" => Self::eval_plainto_tsquery(args, batch, row_idx),
            "PHRASETO_TSQUERY" => Self::eval_phraseto_tsquery(args, batch, row_idx),
            "WEBSEARCH_TO_TSQUERY" => Self::eval_websearch_to_tsquery(args, batch, row_idx),
            "TS_MATCH" | "TS_MATCH_VQ" | "TS_MATCH_QV" => Self::eval_ts_match(args, batch, row_idx),
            "TS_RANK" => Self::eval_ts_rank(args, batch, row_idx),
            "TS_RANK_CD" => Self::eval_ts_rank_cd(args, batch, row_idx),
            "TSVECTOR_CONCAT" => Self::eval_tsvector_concat(args, batch, row_idx),
            "TS_HEADLINE" => Self::eval_ts_headline(args, batch, row_idx),
            "SETWEIGHT" => Self::eval_setweight(args, batch, row_idx),
            "STRIP" => Self::eval_strip(args, batch, row_idx),
            "TSVECTOR_LENGTH" => Self::eval_tsvector_length(args, batch, row_idx),
            "NUMNODE" => Self::eval_numnode(args, batch, row_idx),
            "QUERYTREE" => Self::eval_querytree(args, batch, row_idx),
            "TSQUERY_AND" => Self::eval_tsquery_and(args, batch, row_idx),
            "TSQUERY_OR" => Self::eval_tsquery_or(args, batch, row_idx),
            "TSQUERY_NOT" => Self::eval_tsquery_not(args, batch, row_idx),
            _ => Err(crate::error::Error::invalid_query(format!(
                "Unknown fulltext function: {}",
                name
            ))),
        }
    }

    fn eval_to_tsvector(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(crate::error::Error::invalid_query(
                "TO_TSVECTOR requires at least 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[args.len() - 1], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let text = val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: val.data_type().to_string(),
            })?;
        let vector = fulltext::to_tsvector(text);
        Ok(Value::string(tsvector_to_string(&vector)))
    }

    fn eval_to_tsquery(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(crate::error::Error::invalid_query(
                "TO_TSQUERY requires at least 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[args.len() - 1], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let text = val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: val.data_type().to_string(),
            })?;
        let query = fulltext::to_tsquery(text)?;
        Ok(Value::string(tsquery_to_string(&query)))
    }

    fn eval_plainto_tsquery(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(crate::error::Error::invalid_query(
                "PLAINTO_TSQUERY requires at least 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[args.len() - 1], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let text = val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: val.data_type().to_string(),
            })?;
        let query = fulltext::plainto_tsquery(text);
        Ok(Value::string(tsquery_to_string(&query)))
    }

    fn eval_phraseto_tsquery(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(crate::error::Error::invalid_query(
                "PHRASETO_TSQUERY requires at least 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[args.len() - 1], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let text = val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: val.data_type().to_string(),
            })?;
        let query = fulltext::phraseto_tsquery(text);
        Ok(Value::string(tsquery_to_string(&query)))
    }

    fn eval_websearch_to_tsquery(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(crate::error::Error::invalid_query(
                "WEBSEARCH_TO_TSQUERY requires at least 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[args.len() - 1], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let text = val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: val.data_type().to_string(),
            })?;
        let query = fulltext::websearch_to_tsquery(text);
        Ok(Value::string(tsquery_to_string(&query)))
    }

    fn eval_ts_match(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(crate::error::Error::invalid_query(
                "TS_MATCH requires 2 arguments",
            ));
        }
        let vector_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let query_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if vector_val.is_null() || query_val.is_null() {
            return Ok(Value::null());
        }
        let vector_str = vector_val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: vector_val.data_type().to_string(),
            })?;
        let query_str = query_val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: query_val.data_type().to_string(),
            })?;
        let vector = parse_tsvector(vector_str)?;
        let query = fulltext::to_tsquery(query_str)?;
        Ok(Value::bool_val(query.matches(&vector)))
    }

    fn eval_ts_rank(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(crate::error::Error::invalid_query(
                "TS_RANK requires at least 2 arguments",
            ));
        }
        let vector_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let query_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if vector_val.is_null() || query_val.is_null() {
            return Ok(Value::null());
        }
        let vector_str = vector_val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: vector_val.data_type().to_string(),
            })?;
        let query_str = query_val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: query_val.data_type().to_string(),
            })?;
        let vector = parse_tsvector(vector_str)?;
        let query = fulltext::to_tsquery(query_str)?;
        let score = fulltext::ts_rank(&vector, &query);
        Ok(Value::float64(score))
    }

    fn eval_ts_rank_cd(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(crate::error::Error::invalid_query(
                "TS_RANK_CD requires at least 2 arguments",
            ));
        }
        let vector_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let query_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if vector_val.is_null() || query_val.is_null() {
            return Ok(Value::null());
        }
        let vector_str = vector_val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: vector_val.data_type().to_string(),
            })?;
        let query_str = query_val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: query_val.data_type().to_string(),
            })?;
        let vector = parse_tsvector(vector_str)?;
        let query = fulltext::to_tsquery(query_str)?;
        let score = fulltext::ts_rank_cd(&vector, &query);
        Ok(Value::float64(score))
    }

    fn eval_tsvector_concat(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(crate::error::Error::invalid_query(
                "TSVECTOR_CONCAT requires 2 arguments",
            ));
        }
        let a_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let b_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if a_val.is_null() || b_val.is_null() {
            return Ok(Value::null());
        }
        let a_str = a_val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: a_val.data_type().to_string(),
            })?;
        let b_str = b_val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: b_val.data_type().to_string(),
            })?;
        let a = parse_tsvector(a_str)?;
        let b = parse_tsvector(b_str)?;
        let result = fulltext::tsvector_concat(&a, &b);
        Ok(Value::string(tsvector_to_string(&result)))
    }

    fn eval_ts_headline(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(crate::error::Error::invalid_query(
                "TS_HEADLINE requires at least 2 arguments",
            ));
        }
        let doc_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let query_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if doc_val.is_null() || query_val.is_null() {
            return Ok(Value::null());
        }
        let document = doc_val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: doc_val.data_type().to_string(),
            })?;
        let query_str = query_val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: query_val.data_type().to_string(),
            })?;
        let query = fulltext::to_tsquery(query_str)?;
        let options = HeadlineOptions::default();
        let headline = fulltext::ts_headline(document, &query, &options);
        Ok(Value::string(headline))
    }

    fn eval_setweight(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(crate::error::Error::invalid_query(
                "SETWEIGHT requires 2 arguments",
            ));
        }
        let vector_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let weight_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if vector_val.is_null() || weight_val.is_null() {
            return Ok(Value::null());
        }
        let vector_str = vector_val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: vector_val.data_type().to_string(),
            })?;
        let weight_str = weight_val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: weight_val.data_type().to_string(),
            })?;
        let weight = weight_str
            .chars()
            .next()
            .and_then(Weight::from_char)
            .ok_or_else(|| {
                crate::error::Error::invalid_query(format!(
                    "Invalid weight '{}'. Must be A, B, C, or D",
                    weight_str
                ))
            })?;
        let vector = parse_tsvector(vector_str)?;
        let weighted = fulltext::tsvector_setweight(&vector, weight);
        Ok(Value::string(tsvector_to_string(&weighted)))
    }

    fn eval_strip(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(crate::error::Error::invalid_query(
                "STRIP requires 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let vector_str = val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: val.data_type().to_string(),
            })?;
        let vector = parse_tsvector(vector_str)?;
        let stripped = fulltext::tsvector_strip(&vector);
        Ok(Value::string(tsvector_to_string(&stripped)))
    }

    fn eval_tsvector_length(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(crate::error::Error::invalid_query(
                "LENGTH requires 1 argument for tsvector",
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let vector_str = val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: val.data_type().to_string(),
            })?;
        let vector = parse_tsvector(vector_str)?;
        Ok(Value::int64(fulltext::tsvector_length(&vector)))
    }

    fn eval_numnode(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(crate::error::Error::invalid_query(
                "NUMNODE requires 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let query_str = val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: val.data_type().to_string(),
            })?;
        let _query = fulltext::to_tsquery(query_str)?;
        Ok(Value::int64(1))
    }

    fn eval_querytree(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(crate::error::Error::invalid_query(
                "QUERYTREE requires 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let query_str = val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: val.data_type().to_string(),
            })?;
        let query = fulltext::to_tsquery(query_str)?;
        Ok(Value::string(tsquery_to_string(&query)))
    }

    fn eval_tsquery_and(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(crate::error::Error::invalid_query(
                "TSQUERY_AND requires 2 arguments",
            ));
        }
        let a_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let b_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if a_val.is_null() || b_val.is_null() {
            return Ok(Value::null());
        }
        let a_str = a_val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: a_val.data_type().to_string(),
            })?;
        let b_str = b_val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: b_val.data_type().to_string(),
            })?;
        let a = fulltext::to_tsquery(a_str)?;
        let b = fulltext::to_tsquery(b_str)?;
        let result = a.and(b);
        Ok(Value::string(tsquery_to_string(&result)))
    }

    fn eval_tsquery_or(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(crate::error::Error::invalid_query(
                "TSQUERY_OR requires 2 arguments",
            ));
        }
        let a_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let b_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if a_val.is_null() || b_val.is_null() {
            return Ok(Value::null());
        }
        let a_str = a_val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: a_val.data_type().to_string(),
            })?;
        let b_str = b_val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: b_val.data_type().to_string(),
            })?;
        let a = fulltext::to_tsquery(a_str)?;
        let b = fulltext::to_tsquery(b_str)?;
        let result = a.or(b);
        Ok(Value::string(tsquery_to_string(&result)))
    }

    fn eval_tsquery_not(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(crate::error::Error::invalid_query(
                "TSQUERY_NOT requires 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let query_str = val
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: val.data_type().to_string(),
            })?;
        let query = fulltext::to_tsquery(query_str)?;
        let result = query.negate();
        Ok(Value::string(tsquery_to_string(&result)))
    }
}
