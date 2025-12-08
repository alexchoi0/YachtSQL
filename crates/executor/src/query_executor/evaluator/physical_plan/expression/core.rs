use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::BinaryOp;
use yachtsql_optimizer::expr::Expr;
use yachtsql_storage::Schema;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(crate) fn compute_column_occurrence_indices(
        expressions: &[(Expr, Option<String>)],
    ) -> Vec<usize> {
        let mut occurrence_tracker: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();

        expressions
            .iter()
            .map(|(expr, _)| {
                if let Expr::Column { name, .. } = expr {
                    let idx = *occurrence_tracker.get(name).unwrap_or(&0);
                    occurrence_tracker.insert(name.clone(), idx + 1);
                    idx
                } else {
                    0
                }
            })
            .collect()
    }

    fn find_column_by_occurrence(
        schema: &Schema,
        col_name: &str,
        occurrence_index: usize,
    ) -> Result<usize> {
        let mut count = 0;
        for (idx, field) in schema.fields().iter().enumerate() {
            if field.name == col_name {
                if count == occurrence_index {
                    return Ok(idx);
                }
                count += 1;
            }
        }
        Err(Error::column_not_found(format!(
            "Column '{}' (occurrence {}) not found",
            col_name, occurrence_index
        )))
    }

    pub(crate) fn evaluate_expr_with_occurrence(
        expr: &Expr,
        batch: &Table,
        row_idx: usize,
        occurrence_index: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        let Expr::Column { name, table } = expr else {
            return Self::evaluate_expr_internal(expr, batch, row_idx, dialect);
        };

        let Some(table_name) = table else {
            return Self::get_column_by_occurrence(batch, name, row_idx, occurrence_index);
        };

        let schema = batch.schema();

        if schema.field_index(name).is_some() {
            return Self::get_column_by_occurrence(batch, name, row_idx, occurrence_index);
        }

        let Some(col_idx) = schema.field_index(table_name) else {
            return Self::get_column_by_occurrence(batch, name, row_idx, occurrence_index);
        };

        let field = &schema.fields()[col_idx];
        let is_struct = matches!(
            field.data_type,
            yachtsql_core::types::DataType::Struct(_) | yachtsql_core::types::DataType::Custom(_)
        );
        if !is_struct {
            return Err(Error::column_not_found(name.clone()));
        }

        let struct_value = batch
            .column(col_idx)
            .ok_or_else(|| Error::column_not_found(table_name.clone()))?
            .get(row_idx)?;

        if struct_value.is_null() {
            return Ok(Value::null());
        }

        let Some(map) = struct_value.as_struct() else {
            return Err(Error::column_not_found(name.clone()));
        };

        if let Some(value) = map.get(name) {
            return Ok(value.clone());
        }

        if let Some((_, value)) = map.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)) {
            return Ok(value.clone());
        }

        Err(Error::column_not_found(format!("{}.{}", table_name, name)))
    }

    fn get_column_by_occurrence(
        batch: &Table,
        name: &str,
        row_idx: usize,
        occurrence_index: usize,
    ) -> Result<Value> {
        let col_idx = Self::find_column_by_occurrence(batch.schema(), name, occurrence_index)?;
        batch
            .column(col_idx)
            .ok_or_else(|| Error::column_not_found(name.to_string()))?
            .get(row_idx)
    }

    pub(crate) fn evaluate_expr(expr: &Expr, batch: &Table, row_idx: usize) -> Result<Value> {
        Self::evaluate_expr_internal(expr, batch, row_idx, crate::DialectType::BigQuery)
    }

    pub(super) fn evaluate_expr_internal(
        expr: &Expr,
        batch: &Table,
        row_idx: usize,
        _dialect: crate::DialectType,
    ) -> Result<Value> {
        match expr {
            Expr::Column { name, table } => {
                let Some(table_name) = table else {
                    return Self::evaluate_column(name, batch, row_idx);
                };

                let schema = batch.schema();

                if let Some(col_idx) = schema.field_index_qualified(name, Some(table_name)) {
                    return batch
                        .column(col_idx)
                        .ok_or_else(|| Error::column_not_found(format!("{}.{}", table_name, name)))?
                        .get(row_idx);
                }

                if schema.field_index(name).is_some() {
                    return Self::evaluate_column(name, batch, row_idx);
                }

                let Some(col_idx) = schema.field_index(table_name) else {
                    return Self::evaluate_column(name, batch, row_idx);
                };

                let field = &schema.fields()[col_idx];
                let is_struct = matches!(
                    field.data_type,
                    yachtsql_core::types::DataType::Struct(_)
                        | yachtsql_core::types::DataType::Custom(_)
                );
                if !is_struct {
                    return Err(Error::column_not_found(name.clone()));
                }

                let struct_value = batch
                    .column(col_idx)
                    .ok_or_else(|| Error::column_not_found(table_name.clone()))?
                    .get(row_idx)?;

                if struct_value.is_null() {
                    return Ok(Value::null());
                }

                let Some(map) = struct_value.as_struct() else {
                    return Err(Error::column_not_found(name.clone()));
                };

                if let Some(value) = map.get(name) {
                    return Ok(value.clone());
                }

                if let Some((_, value)) = map.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)) {
                    return Ok(value.clone());
                }

                Err(Error::column_not_found(format!("{}.{}", table_name, name)))
            }

            Expr::Literal(lit) => Self::evaluate_literal(lit, batch, row_idx),

            Expr::Wildcard => Ok(Value::int64(1)),

            Expr::BinaryOp { left, op, right } => match op {
                BinaryOp::And => Self::evaluate_and_internal(left, right, batch, row_idx, _dialect),
                BinaryOp::Or => Self::evaluate_or_internal(left, right, batch, row_idx, _dialect),
                _ => {
                    if let (
                        Expr::Tuple(left_exprs),
                        Expr::Subquery { plan } | Expr::ScalarSubquery { subquery: plan },
                    ) = (left.as_ref(), right.as_ref())
                    {
                        if matches!(op, BinaryOp::Equal | BinaryOp::NotEqual) {
                            return Self::evaluate_row_comparison(
                                left_exprs, op, plan, batch, row_idx,
                            );
                        }
                    }
                    let left_val = Self::evaluate_expr_internal(left, batch, row_idx, _dialect)?;
                    let right_val = Self::evaluate_expr_internal(right, batch, row_idx, _dialect)?;
                    let enum_labels = Self::get_enum_labels_for_expr(left, batch.schema())
                        .or_else(|| Self::get_enum_labels_for_expr(right, batch.schema()));
                    Self::evaluate_binary_op_with_enum(
                        &left_val,
                        op,
                        &right_val,
                        enum_labels.as_deref(),
                    )
                }
            },

            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => Self::evaluate_case_internal(
                operand, when_then, else_expr, batch, row_idx, _dialect,
            ),

            Expr::Cast { expr, data_type } => {
                let value = Self::evaluate_expr_internal(expr, batch, row_idx, _dialect)?;
                Self::cast_value(value, data_type)
            }

            Expr::TryCast { expr, data_type } => {
                let value = Self::evaluate_expr_internal(expr, batch, row_idx, _dialect)?;
                Ok(Self::try_cast_value(value, data_type))
            }

            Expr::UnaryOp { op, expr } => {
                let operand = Self::evaluate_expr_internal(expr, batch, row_idx, _dialect)?;
                Self::evaluate_unary_op(op, &operand)
            }

            Expr::Function { name, args } => {
                Self::evaluate_function_by_category(name, args, batch, row_idx, _dialect)
            }

            Expr::Aggregate { name, args, .. } => {
                let func_name = name.as_str();
                let fields = batch.schema().fields();

                let col_idx = fields.iter().position(|f| f.name == func_name).or_else(|| {
                    let full_name = format!(
                        "{}({})",
                        func_name,
                        args.iter()
                            .map(|a| match a {
                                Expr::Column { name, .. } => name.clone(),
                                Expr::Literal(lit) => format!("{:?}", lit),
                                _ => "*".to_string(),
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                    fields.iter().position(|f| f.name == full_name)
                });

                match col_idx {
                    Some(idx) => batch
                        .column(idx)
                        .ok_or_else(|| Error::column_not_found(func_name))?
                        .get(row_idx),
                    None if args.len() == 1 => {
                        Self::compute_aggregate_over_batch(name, &args[0], batch)
                    }
                    None => Err(Error::unsupported_feature(format!(
                        "Aggregate expression {} requires pre-computed values",
                        func_name
                    ))),
                }
            }

            Expr::ArrayIndex {
                array,
                index,
                safe: _,
            } => Self::evaluate_array_index(array, index, batch, row_idx),

            Expr::ArraySlice { array, start, end } => {
                Self::evaluate_array_slice(array, start, end, batch, row_idx)
            }

            Expr::Tuple(exprs) => Self::evaluate_tuple_as_struct(exprs, batch, row_idx),

            Expr::StructLiteral { fields } => Self::evaluate_struct_literal(fields, batch, row_idx),

            Expr::StructFieldAccess { expr, field } => {
                Self::evaluate_struct_field_access(expr, field, batch, row_idx)
            }

            Expr::Grouping { column: _ } => Self::evaluate_grouping(),

            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let val = Self::evaluate_expr(expr, batch, row_idx)?;
                let low_val = Self::evaluate_expr(low, batch, row_idx)?;
                let high_val = Self::evaluate_expr(high, batch, row_idx)?;

                let in_range = Self::check_between(&val, &low_val, &high_val, batch.schema(), expr);
                let result = match (in_range, *negated) {
                    (None, _) => Value::null(),
                    (Some(b), false) => Value::bool_val(b),
                    (Some(b), true) => Value::bool_val(!b),
                };
                Ok(result)
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let val = Self::evaluate_expr(expr, batch, row_idx)?;

                let mut found = false;
                let mut has_null = false;

                for item in list {
                    let item_val = Self::evaluate_expr(item, batch, row_idx)?;
                    if item_val.is_null() {
                        has_null = true;
                        continue;
                    }
                    if val == item_val {
                        found = true;
                        break;
                    }
                }

                let in_list = match (found, has_null) {
                    (true, _) => Some(true),
                    (false, true) => None,
                    (false, false) => Some(false),
                };

                let result = match (in_list, *negated) {
                    (None, _) => Value::null(),
                    (Some(b), false) => Value::bool_val(b),
                    (Some(b), true) => Value::bool_val(!b),
                };
                Ok(result)
            }

            Expr::Subquery { plan } => Self::evaluate_scalar_subquery_expr(plan),

            Expr::ScalarSubquery { subquery } => Self::evaluate_scalar_subquery_expr(subquery),

            Expr::Exists { plan, negated } => Self::evaluate_exists_subquery_expr(plan, *negated),

            Expr::InSubquery {
                expr,
                plan,
                negated,
            } => Self::evaluate_in_subquery_expr(expr, plan, *negated, batch, row_idx),

            Expr::TupleInList {
                tuple,
                list,
                negated,
            } => Self::evaluate_tuple_in_list_with_coercion(tuple, list, *negated, batch, row_idx),

            Expr::TupleInSubquery {
                tuple,
                plan,
                negated,
            } => Self::evaluate_tuple_in_subquery_expr(tuple, plan, *negated, batch, row_idx),

            Expr::AnyOp {
                left,
                compare_op,
                right,
            } => Self::evaluate_any_op_expr(left, compare_op, right, batch, row_idx),

            Expr::AllOp {
                left,
                compare_op,
                right,
            } => Self::evaluate_all_op_expr(left, compare_op, right, batch, row_idx),

            Expr::IsDistinctFrom {
                left,
                right,
                negated,
            } => {
                let left_val = Self::evaluate_expr(left, batch, row_idx)?;
                let right_val = Self::evaluate_expr(right, batch, row_idx)?;

                let is_distinct = Self::values_are_distinct(&left_val, &right_val);

                Ok(Value::bool_val(if *negated {
                    !is_distinct
                } else {
                    is_distinct
                }))
            }

            Expr::Lambda { .. } => Err(Error::invalid_query(
                "Lambda expressions can only be used as arguments to higher-order functions"
                    .to_string(),
            )),

            _ => Err(Error::unsupported_feature(format!(
                "Expression evaluation not yet implemented for: {:?}",
                expr
            ))),
        }
    }

    fn evaluate_function_by_category(
        name: &yachtsql_ir::FunctionName,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        use yachtsql_ir::FunctionName;

        let func_name = name.as_str();

        if matches!(
            name,
            FunctionName::Concat
                | FunctionName::Concatenate
                | FunctionName::Trim
                | FunctionName::Btrim
                | FunctionName::Ltrim
                | FunctionName::TrimLeft
                | FunctionName::Rtrim
                | FunctionName::TrimRight
                | FunctionName::Upper
                | FunctionName::Ucase
                | FunctionName::Lower
                | FunctionName::Lcase
                | FunctionName::Replace
                | FunctionName::StrReplace
                | FunctionName::Substr
                | FunctionName::Substring
                | FunctionName::Mid
                | FunctionName::Length
                | FunctionName::Len
                | FunctionName::CharLength
                | FunctionName::CharacterLength
                | FunctionName::Split
                | FunctionName::SplitPart
                | FunctionName::StringSplit
                | FunctionName::Position
                | FunctionName::Strpos
                | FunctionName::Instr
                | FunctionName::Locate
                | FunctionName::Left
                | FunctionName::Right
                | FunctionName::Repeat
                | FunctionName::Replicate
                | FunctionName::Reverse
                | FunctionName::Strrev
                | FunctionName::Lpad
                | FunctionName::LeftPad
                | FunctionName::Rpad
                | FunctionName::RightPad
                | FunctionName::Ascii
                | FunctionName::Ord
                | FunctionName::Chr
                | FunctionName::Char
                | FunctionName::Initcap
                | FunctionName::Proper
                | FunctionName::Translate
        ) || matches!(
            name,
            FunctionName::Custom(s) if matches!(s.as_str(),
                "TRIM_CHARS"
                | "LTRIM_CHARS"
                | "RTRIM_CHARS"
                | "STRING_TO_ARRAY"
                | "STARTS_WITH"
                | "STARTSWITH"
                | "ENDS_WITH"
                | "ENDSWITH"
                | "REGEXP_CONTAINS"
                | "REGEXP_REPLACE"
                | "REPLACEREGEXPALL"
                | "REPLACEREGEXPONE"
                | "REGEXP_EXTRACT"
                | "FORMAT"
                | "QUOTE_IDENT"
                | "QUOTE_LITERAL"
                | "CASEFOLD"
                | "SPLITBYCHAR"
                | "SPLITBYSTRING"
            )
        ) {
            return Self::evaluate_string_function(func_name, args, batch, row_idx);
        }

        if matches!(
            name,
            FunctionName::ArrayLength
                | FunctionName::Cardinality
                | FunctionName::ArrayConcat
                | FunctionName::ArrayCat
        ) || matches!(
            name,
            FunctionName::Custom(s) if matches!(s.as_str(),
                "ARRAY_REVERSE"
                | "ARRAY_APPEND"
                | "ARRAY_PREPEND"
                | "ARRAY_POSITION"
                | "ARRAY_CONTAINS"
                | "ARRAY_REMOVE"
                | "ARRAY_REPLACE"
                | "ARRAY_SORT"
                | "ARRAY_DISTINCT"
                | "GENERATE_ARRAY"
                | "GENERATE_DATE_ARRAY"
                | "GENERATE_TIMESTAMP_ARRAY"
            )
        ) {
            return Self::evaluate_array_function(func_name, args, batch, row_idx);
        }

        if matches!(
            name,
            FunctionName::Sign
                | FunctionName::Signum
                | FunctionName::Abs
                | FunctionName::Absolute
                | FunctionName::Ceil
                | FunctionName::Ceiling
                | FunctionName::Floor
                | FunctionName::Round
                | FunctionName::Rnd
                | FunctionName::Trunc
                | FunctionName::Truncate
                | FunctionName::Mod
                | FunctionName::Modulo
                | FunctionName::Power
                | FunctionName::Pow
                | FunctionName::Sqrt
                | FunctionName::Sqr
                | FunctionName::Exp
                | FunctionName::Exponent
                | FunctionName::Ln
                | FunctionName::Loge
                | FunctionName::Log
                | FunctionName::Logarithm
                | FunctionName::Log10
                | FunctionName::Log2
                | FunctionName::Sin
                | FunctionName::Sine
                | FunctionName::Cos
                | FunctionName::Cosine
                | FunctionName::Tan
                | FunctionName::Tangent
                | FunctionName::Asin
                | FunctionName::Arcsine
                | FunctionName::Acos
                | FunctionName::Arccosine
                | FunctionName::Atan
                | FunctionName::Arctangent
                | FunctionName::Atan2
                | FunctionName::Arctangent2
                | FunctionName::Pi
                | FunctionName::Random
                | FunctionName::Rand
        ) || matches!(
            name,
            FunctionName::Custom(s) if matches!(s.as_str(),
                "DEGREES"
                | "RADIANS"
                | "SAFE_DIVIDE"
                | "SAFE_MULTIPLY"
                | "SAFE_ADD"
                | "SAFE_SUBTRACT"
                | "SAFE_NEGATE"
                | "GAMMA"
                | "LGAMMA"
            )
        ) {
            return Self::evaluate_math_function(func_name, args, batch, row_idx);
        }

        if matches!(
            name,
            FunctionName::CurrentDate
                | FunctionName::Curdate
                | FunctionName::Today
                | FunctionName::CurrentTimestamp
                | FunctionName::Getdate
                | FunctionName::Sysdate
                | FunctionName::Systimestamp
                | FunctionName::Now
                | FunctionName::CurrentTime
                | FunctionName::Curtime
                | FunctionName::DateAdd
                | FunctionName::Dateadd
                | FunctionName::Adddate
                | FunctionName::DateSub
                | FunctionName::Datesub
                | FunctionName::Subdate
                | FunctionName::DateDiff
                | FunctionName::Datediff
                | FunctionName::Extract
                | FunctionName::DatePart
                | FunctionName::Datepart
                | FunctionName::DateTrunc
                | FunctionName::TruncDate
                | FunctionName::FormatTimestamp
                | FunctionName::DateFormat
                | FunctionName::StrToDate
                | FunctionName::ParseDatetime
                | FunctionName::Age
                | FunctionName::Date
                | FunctionName::CastDate
                | FunctionName::ToDate
                | FunctionName::Timestampdiff
        ) || matches!(
            name,
            FunctionName::Custom(s) if matches!(s.as_str(),
                "TIMESTAMP_TRUNC"
                | "FORMAT_DATE"
                | "PARSE_DATE"
                | "PARSE_TIMESTAMP"
                | "MAKE_DATE"
                | "MAKE_TIMESTAMP"
                | "TIMESTAMP_DIFF"
                | "INTERVAL_LITERAL"
                | "INTERVAL_PARSE"
                | "YEAR"
                | "MONTH"
                | "DAY"
                | "HOUR"
                | "MINUTE"
                | "SECOND"
                | "QUARTER"
                | "WEEK"
                | "ISOWEEK"
                | "DAYOFWEEK"
                | "DAYOFYEAR"
                | "DAYOFMONTH"
                | "WEEKDAY"
                | "LAST_DAY"
                | "AT_TIME_ZONE"
            )
        ) {
            return Self::evaluate_datetime_function(func_name, args, batch, row_idx);
        }

        {
            let s = name.as_str();
            if s.starts_with("JSON") || s.starts_with("IS_JSON") || s.starts_with("IS_NOT_JSON") {
                return Self::evaluate_json_function(func_name, args, batch, row_idx);
            }
        }

        if matches!(
            name,
            FunctionName::Md5
                | FunctionName::Md5Hash
                | FunctionName::Sha256
                | FunctionName::Sha2
                | FunctionName::Encode
        ) || matches!(
            name,
            FunctionName::Custom(s) if matches!(s.as_str(),
                "SHA1"
                | "SHA512"
                | "FARM_FINGERPRINT"
                | "TO_HEX"
                | "FROM_HEX"
                | "GEN_RANDOM_BYTES"
                | "DIGEST"
                | "CRC32"
                | "CRC32C"
            )
        ) || name.as_str().starts_with("NET.")
        {
            return Self::evaluate_crypto_hash_network_function(
                func_name, args, batch, row_idx, dialect,
            );
        }

        {
            let s = name.as_str();
            if s.starts_with("AEAD.") || s.starts_with("DETERMINISTIC_") || s.starts_with("KEYS.") {
                return Self::evaluate_encryption_function(func_name, args, batch, row_idx);
            }
        }

        if matches!(
            name,
            FunctionName::Coalesce
                | FunctionName::Ifnull
                | FunctionName::Nvl
                | FunctionName::Isnull
                | FunctionName::Nullif
                | FunctionName::If
                | FunctionName::Iif
                | FunctionName::Decode
                | FunctionName::Greatest
                | FunctionName::MaxValue
                | FunctionName::Least
                | FunctionName::MinValue
        ) {
            return Self::evaluate_conditional_function(func_name, args, batch, row_idx);
        }

        if matches!(
            name,
            FunctionName::GenerateUuid
                | FunctionName::Uuid
                | FunctionName::GenRandomUuid
                | FunctionName::Newid
                | FunctionName::UuidGenerateV4
        ) || matches!(
            name,
            FunctionName::Custom(s) if matches!(s.as_str(),
                "GENERATE_UUID_ARRAY" | "UUID_GENERATE_V1" | "UUIDV4" | "UUIDV7"
            )
        ) {
            return Self::evaluate_generator_function(func_name, args, batch, row_idx);
        }

        if matches!(name, FunctionName::ToChar)
            || matches!(name, FunctionName::Custom(s) if s == "TO_NUMBER")
        {
            return Self::evaluate_conversion_function(func_name, args, batch, row_idx);
        }

        if matches!(
            name,
            FunctionName::Custom(s) if matches!(s.as_str(),
                "POINT"
                | "BOX"
                | "CIRCLE"
                | "AREA"
                | "CENTER"
                | "DIAMETER"
                | "RADIUS"
                | "WIDTH"
                | "HEIGHT"
                | "DISTANCE"
                | "CONTAINS"
                | "CONTAINED_BY"
                | "OVERLAPS"
            )
        ) {
            return Self::evaluate_geometric_function(func_name, args, batch, row_idx);
        }

        if matches!(
            name,
            FunctionName::Custom(s) if matches!(s.as_str(),
                "ST_GEOGPOINT"
                | "ST_GEOGFROMTEXT"
                | "ST_GEOGFROMGEOJSON"
                | "ST_ASTEXT"
                | "ST_ASGEOJSON"
                | "ST_ASBINARY"
                | "ST_X"
                | "ST_Y"
                | "ST_GEOMETRYTYPE"
                | "ST_ISEMPTY"
                | "ST_ISCLOSED"
                | "ST_ISCOLLECTION"
                | "ST_DIMENSION"
                | "ST_NUMPOINTS"
                | "ST_NPOINTS"
                | "ST_POINTN"
                | "ST_STARTPOINT"
                | "ST_ENDPOINT"
                | "ST_MAKELINE"
                | "ST_MAKEPOLYGON"
                | "ST_DISTANCE"
                | "ST_LENGTH"
                | "ST_AREA"
                | "ST_PERIMETER"
                | "ST_MAXDISTANCE"
                | "ST_AZIMUTH"
                | "ST_CENTROID"
                | "ST_CONTAINS"
                | "ST_COVERS"
                | "ST_COVEREDBY"
                | "ST_DISJOINT"
                | "ST_DWITHIN"
                | "ST_EQUALS"
                | "ST_INTERSECTS"
                | "ST_TOUCHES"
                | "ST_WITHIN"
                | "ST_BOUNDARY"
                | "ST_BUFFER"
                | "ST_BUFFERWITHTOLERANCE"
                | "ST_CLOSESTPOINT"
                | "ST_CONVEXHULL"
                | "ST_DIFFERENCE"
                | "ST_INTERSECTION"
                | "ST_SIMPLIFY"
                | "ST_SNAPTOGRID"
                | "ST_UNION"
                | "ST_BOUNDINGBOX"
                | "ST_GEOHASH"
                | "ST_GEOGPOINTFROMGEOHASH"
            )
        ) {
            return Self::evaluate_geography_function(func_name, args, batch, row_idx);
        }

        if matches!(
            name,
            FunctionName::Custom(s) if matches!(s.as_str(),
                "TO_TSVECTOR"
                | "TO_TSQUERY"
                | "PLAINTO_TSQUERY"
                | "PHRASETO_TSQUERY"
                | "WEBSEARCH_TO_TSQUERY"
                | "TS_MATCH"
                | "TS_MATCH_VQ"
                | "TS_MATCH_QV"
                | "TS_RANK"
                | "TS_RANK_CD"
                | "TSVECTOR_CONCAT"
                | "TS_HEADLINE"
                | "SETWEIGHT"
                | "STRIP"
                | "TSVECTOR_LENGTH"
                | "NUMNODE"
                | "QUERYTREE"
                | "TSQUERY_AND"
                | "TSQUERY_OR"
                | "TSQUERY_NOT"
            )
        ) {
            return Self::evaluate_fulltext_function(func_name, args, batch, row_idx);
        }

        if name.as_str().starts_with("YACHTSQL.") {
            return Self::evaluate_system_function(func_name, args, batch, row_idx);
        }

        if matches!(
            name,
            FunctionName::Custom(s) if matches!(s.as_str(),
                "HSTORE_EXISTS"
                | "HSTORE_EXISTS_ALL"
                | "HSTORE_EXISTS_ANY"
                | "EXIST"
                | "HSTORE_CONCAT"
                | "HSTORE_DELETE"
                | "HSTORE_DELETE_KEY"
                | "HSTORE_DELETE_KEYS"
                | "HSTORE_DELETE_HSTORE"
                | "DELETE"
                | "HSTORE_CONTAINS"
                | "HSTORE_CONTAINED_BY"
                | "HSTORE_AKEYS"
                | "AKEYS"
                | "SKEYS"
                | "HSTORE_AVALS"
                | "AVALS"
                | "SVALS"
                | "HSTORE_DEFINED"
                | "DEFINED"
                | "HSTORE_TO_JSON"
                | "HSTORE_TO_JSONB"
                | "HSTORE_TO_ARRAY"
                | "HSTORE_TO_MATRIX"
                | "HSTORE_SLICE"
                | "SLICE"
                | "HSTORE"
                | "HSTORE_GET"
                | "HSTORE_GET_VALUES"
            )
        ) {
            return Self::evaluate_hstore_function(func_name, args, batch, row_idx);
        }

        if matches!(
            name,
            FunctionName::Custom(s) if matches!(s.as_str(),
                "ARRAYMAP"
                | "ARRAYFILTER"
                | "ARRAYEXISTS"
                | "ARRAYALL"
                | "ARRAYFIRST"
                | "ARRAYLAST"
                | "ARRAYFIRSTINDEX"
                | "ARRAYLASTINDEX"
                | "ARRAYCOUNT"
                | "ARRAYSUM"
                | "ARRAYAVG"
                | "ARRAYMIN"
                | "ARRAYMAX"
                | "ARRAYSORT"
                | "ARRAYREVERSESORT"
                | "ARRAYFOLD"
                | "ARRAYREDUCE"
                | "ARRAYREDUCEINRANGES"
                | "ARRAYCUMSUM"
                | "ARRAYCUMSUMNONNEGATIVE"
                | "ARRAYDIFFERENCE"
                | "ARRAYSPLIT"
                | "ARRAYREVERSESPLIT"
                | "ARRAYCOMPACT"
                | "ARRAYZIP"
                | "ARRAYAUC"
            )
        ) {
            return Self::evaluate_higher_order_function(func_name, args, batch, row_idx, dialect);
        }

        Err(Error::unsupported_feature(format!(
            "Unknown function: {}",
            func_name
        )))
    }

    fn compute_aggregate_over_batch(
        agg_name: &yachtsql_ir::FunctionName,
        arg: &Expr,
        batch: &Table,
    ) -> Result<Value> {
        use yachtsql_ir::FunctionName;

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(match agg_name {
                FunctionName::Count => Value::int64(0),
                _ => Value::null(),
            });
        }

        let mut values = Vec::with_capacity(num_rows);
        for row_idx in 0..num_rows {
            let val = Self::evaluate_expr(arg, batch, row_idx)?;
            values.push(val);
        }

        match agg_name {
            FunctionName::Count => {
                let count = values.iter().filter(|v| !v.is_null()).count();
                Ok(Value::int64(count as i64))
            }
            FunctionName::Sum => {
                let mut sum_int: i64 = 0;
                let mut sum_float: f64 = 0.0;
                let mut has_float = false;
                let mut has_value = false;

                for v in &values {
                    if !v.is_null() {
                        has_value = true;
                        if let Some(i) = v.as_i64() {
                            sum_int += i;
                            sum_float += i as f64;
                        } else if let Some(f) = v.as_f64() {
                            has_float = true;
                            sum_float += f;
                        }
                    }
                }

                if !has_value {
                    Ok(Value::null())
                } else if has_float {
                    Ok(Value::float64(sum_float))
                } else {
                    Ok(Value::int64(sum_int))
                }
            }
            FunctionName::Avg | FunctionName::Average => {
                let mut sum: f64 = 0.0;
                let mut count: usize = 0;

                for v in &values {
                    if !v.is_null() {
                        if let Some(n) = v.as_f64() {
                            sum += n;
                            count += 1;
                        } else if let Some(i) = v.as_i64() {
                            sum += i as f64;
                            count += 1;
                        }
                    }
                }

                if count == 0 {
                    Ok(Value::null())
                } else {
                    Ok(Value::float64(sum / count as f64))
                }
            }
            FunctionName::Min | FunctionName::Minimum => {
                let mut min_val: Option<Value> = None;
                for v in values {
                    if !v.is_null() {
                        min_val = Some(match min_val {
                            None => v,
                            Some(cur) => {
                                let cur_f = cur.as_f64().or_else(|| cur.as_i64().map(|i| i as f64));
                                let v_f = v.as_f64().or_else(|| v.as_i64().map(|i| i as f64));
                                match (cur_f, v_f) {
                                    (Some(c), Some(vv)) if vv < c => v,
                                    _ => cur,
                                }
                            }
                        });
                    }
                }
                Ok(min_val.unwrap_or_else(Value::null))
            }
            FunctionName::Max | FunctionName::Maximum => {
                let mut max_val: Option<Value> = None;
                for v in values {
                    if !v.is_null() {
                        max_val = Some(match max_val {
                            None => v,
                            Some(cur) => {
                                let cur_f = cur.as_f64().or_else(|| cur.as_i64().map(|i| i as f64));
                                let v_f = v.as_f64().or_else(|| v.as_i64().map(|i| i as f64));
                                match (cur_f, v_f) {
                                    (Some(c), Some(vv)) if vv > c => v,
                                    _ => cur,
                                }
                            }
                        });
                    }
                }
                Ok(max_val.unwrap_or_else(Value::null))
            }
            FunctionName::ArrayAgg => {
                let non_null_values: Vec<Value> =
                    values.into_iter().filter(|v| !v.is_null()).collect();
                Ok(Value::array(non_null_values))
            }
            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "UNIQ"
                        | "UNIQ_EXACT"
                        | "UNIQ_HLL12"
                        | "UNIQ_COMBINED"
                        | "UNIQ_COMBINED_64"
                        | "UNIQ_THETA_SKETCH"
                ) =>
            {
                let mut unique_values = std::collections::HashSet::new();
                for val in &values {
                    if !val.is_null() {
                        let key = format!("{:?}", val);
                        unique_values.insert(key);
                    }
                }
                Ok(Value::int64(unique_values.len() as i64))
            }
            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "QUANTILE" | "QUANTILE_EXACT" | "QUANTILE_TIMING" | "QUANTILE_TDIGEST"
                ) =>
            {
                let mut numeric_values: Vec<f64> = values
                    .iter()
                    .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                    .collect();
                if numeric_values.is_empty() {
                    Ok(Value::null())
                } else {
                    numeric_values
                        .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    let median_idx = numeric_values.len() / 2;
                    Ok(Value::float64(numeric_values[median_idx]))
                }
            }
            FunctionName::Custom(s) if s == "GROUP_ARRAY" => {
                let non_null_values: Vec<Value> =
                    values.into_iter().filter(|v| !v.is_null()).collect();
                Ok(Value::array(non_null_values))
            }
            FunctionName::ArgMin => Ok(Value::null()),
            FunctionName::Custom(s) if s == "ARGMIN" => Ok(Value::null()),
            FunctionName::ArgMax => Ok(Value::null()),
            FunctionName::Custom(s) if s == "ARGMAX" => Ok(Value::null()),
            FunctionName::Custom(s) if matches!(s.as_str(), "TOP_K" | "TOPK") => {
                let non_null_values: Vec<Value> =
                    values.into_iter().filter(|v| !v.is_null()).collect();
                Ok(Value::array(non_null_values))
            }
            FunctionName::Any => values
                .into_iter()
                .find(|v| !v.is_null())
                .map_or(Ok(Value::null()), Ok),
            FunctionName::Custom(s) if matches!(s.as_str(), "ANY_LAST" | "ANY_HEAVY") => values
                .into_iter()
                .find(|v| !v.is_null())
                .map_or(Ok(Value::null()), Ok),
            FunctionName::Custom(s) if s == "GROUP_UNIQ_ARRAY" => {
                let mut unique_values = std::collections::HashSet::new();
                let mut result = Vec::new();
                for val in values {
                    if !val.is_null() {
                        let key = format!("{:?}", val);
                        if unique_values.insert(key) {
                            result.push(val);
                        }
                    }
                }
                Ok(Value::array(result))
            }
            _ => Err(Error::unsupported_feature(format!(
                "Aggregate function {} not supported in expression context",
                agg_name.as_str()
            ))),
        }
    }

    pub(super) fn get_enum_labels_for_expr(expr: &Expr, schema: &Schema) -> Option<Vec<String>> {
        match expr {
            Expr::Column { name, .. } => {
                for field in schema.fields() {
                    if field.name == *name {
                        if let yachtsql_core::types::DataType::Enum { labels, .. } =
                            &field.data_type
                        {
                            return Some(labels.clone());
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }

    fn check_between(
        val: &Value,
        low: &Value,
        high: &Value,
        schema: &Schema,
        expr: &Expr,
    ) -> Option<bool> {
        if val.is_null() || low.is_null() || high.is_null() {
            return None;
        }

        if let (Some(v), Some(l), Some(h)) = (val.as_i64(), low.as_i64(), high.as_i64()) {
            return Some(v >= l && v <= h);
        }

        if let (Some(v), Some(l), Some(h)) = (val.as_f64(), low.as_f64(), high.as_f64()) {
            return Some(v >= l && v <= h);
        }

        if let (Some(v), Some(l), Some(h)) = (val.as_str(), low.as_str(), high.as_str()) {
            let enum_labels = Self::get_enum_labels_for_expr(expr, schema);
            if let Some(labels) = &enum_labels {
                let v_pos = labels.iter().position(|lbl| lbl == v);
                let l_pos = labels.iter().position(|lbl| lbl == l);
                let h_pos = labels.iter().position(|lbl| lbl == h);
                if let (Some(v_idx), Some(l_idx), Some(h_idx)) = (v_pos, l_pos, h_pos) {
                    return Some(v_idx >= l_idx && v_idx <= h_idx);
                }
            }
            return Some(v >= l && v <= h);
        }

        if let (Some(v), Some(l), Some(h)) = (val.as_date(), low.as_date(), high.as_date()) {
            return Some(v >= l && v <= h);
        }

        if let (Some(v), Some(l), Some(h)) =
            (val.as_timestamp(), low.as_timestamp(), high.as_timestamp())
        {
            return Some(v >= l && v <= h);
        }

        None
    }
}
