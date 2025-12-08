use std::rc::Rc;

use yachtsql_core::types::Value;
use yachtsql_functions::json::JsonValueEvalOptions;
use yachtsql_storage::Schema;

use super::{
    FeatureRegistryContextGuard, ProjectionWithExprExec, SubqueryExecutor,
    SubqueryExecutorContextGuard,
};

#[allow(dead_code)]
impl ProjectionWithExprExec {
    pub(crate) fn enter_feature_registry_context(
        registry: Rc<yachtsql_capability::FeatureRegistry>,
    ) -> FeatureRegistryContextGuard {
        FeatureRegistryContextGuard::set(registry)
    }

    pub(crate) fn enter_subquery_executor_context(
        executor: Rc<dyn SubqueryExecutor>,
    ) -> SubqueryExecutorContextGuard {
        SubqueryExecutorContextGuard::set(executor)
    }

    fn infer_case_type(
        when_then: &[(crate::optimizer::expr::Expr, crate::optimizer::expr::Expr)],
        else_expr: Option<&crate::optimizer::expr::Expr>,
        schema: &Schema,
    ) -> crate::types::DataType {
        use yachtsql_core::types::DataType;

        for (when_cond, then_expr) in when_then {
            let _ = Self::infer_expr_type_with_schema(when_cond, schema);
            let _ = Self::infer_expr_type_with_schema(then_expr, schema);
        }

        if let Some(else_e) = else_expr
            && let Some(data_type) = Self::infer_expr_type_with_schema(else_e, schema)
        {
            return data_type;
        }

        if let Some((_, then_expr)) = when_then.first()
            && let Some(data_type) = Self::infer_expr_type_with_schema(then_expr, schema)
        {
            return data_type;
        }

        DataType::String
    }

    fn is_empty_array_literal(expr: &crate::optimizer::expr::Expr) -> bool {
        matches!(expr,
            crate::optimizer::expr::Expr::Literal(
                crate::optimizer::expr::LiteralValue::Array(elements)
            ) if elements.is_empty()
        )
    }

    fn infer_array_type_from_first_arg(
        args: &[crate::optimizer::expr::Expr],
        schema: &Schema,
    ) -> Option<crate::types::DataType> {
        use yachtsql_core::types::DataType;

        args.first().and_then(|first_arg| {
            Self::infer_expr_type_with_schema(first_arg, schema).and_then(|data_type| {
                match data_type {
                    DataType::Array(element_type) => Some(DataType::Array(element_type)),
                    _ => None,
                }
            })
        })
    }

    fn infer_array_type_with_element_fallback(
        array_arg_idx: usize,
        element_arg_idx: usize,
        args: &[crate::optimizer::expr::Expr],
        schema: &Schema,
    ) -> Option<crate::types::DataType> {
        use yachtsql_core::types::DataType;

        if args.len() <= array_arg_idx.max(element_arg_idx) {
            return None;
        }

        if Self::is_empty_array_literal(&args[array_arg_idx])
            && let Some(element_type) =
                Self::infer_expr_type_with_schema(&args[element_arg_idx], schema)
        {
            return Some(DataType::Array(Box::new(element_type)));
        }

        Self::infer_expr_type_with_schema(&args[array_arg_idx], schema).and_then(|data_type| {
            match data_type {
                DataType::Array(element_type) => Some(DataType::Array(element_type)),
                _ => None,
            }
        })
    }

    fn infer_array_type_from_non_empty_array(
        args: &[crate::optimizer::expr::Expr],
        schema: &Schema,
    ) -> Option<crate::types::DataType> {
        use yachtsql_core::types::DataType;

        for arg in args {
            if !Self::is_empty_array_literal(arg)
                && let Some(DataType::Array(element_type)) =
                    Self::infer_expr_type_with_schema(arg, schema)
            {
                return Some(DataType::Array(element_type));
            }
        }

        Self::infer_array_type_from_first_arg(args, schema)
    }

    fn cast_type_to_data_type(
        cast_type: &crate::optimizer::expr::CastDataType,
    ) -> crate::types::DataType {
        use yachtsql_core::types::DataType;
        use yachtsql_optimizer::expr::CastDataType;

        match cast_type {
            CastDataType::Int64 => DataType::Int64,
            CastDataType::Float64 => DataType::Float64,
            CastDataType::Numeric(precision_scale) => DataType::Numeric(*precision_scale),
            CastDataType::String => DataType::String,
            CastDataType::Bytes => DataType::Bytes,
            CastDataType::Bool => DataType::Bool,
            CastDataType::Date => DataType::Date,
            CastDataType::DateTime => DataType::DateTime,
            CastDataType::Time => DataType::Time,
            CastDataType::Timestamp => DataType::Timestamp,
            CastDataType::TimestampTz => DataType::TimestampTz,
            CastDataType::Geography => DataType::Geography,
            CastDataType::Json => DataType::Json,
            CastDataType::Array(inner) => {
                DataType::Array(Box::new(Self::cast_type_to_data_type(inner.as_ref())))
            }
            CastDataType::Vector(dims) => DataType::Vector(*dims),
            CastDataType::Interval => DataType::Interval,
            CastDataType::Uuid => DataType::Uuid,
            CastDataType::Hstore => DataType::Hstore,
            CastDataType::MacAddr => DataType::MacAddr,
            CastDataType::MacAddr8 => DataType::MacAddr8,
            CastDataType::Int4Range => DataType::Range(yachtsql_core::types::RangeType::Int4Range),
            CastDataType::Int8Range => DataType::Range(yachtsql_core::types::RangeType::Int8Range),
            CastDataType::NumRange => DataType::Range(yachtsql_core::types::RangeType::NumRange),
            CastDataType::TsRange => DataType::Range(yachtsql_core::types::RangeType::TsRange),
            CastDataType::TsTzRange => DataType::Range(yachtsql_core::types::RangeType::TsTzRange),
            CastDataType::DateRange => DataType::Range(yachtsql_core::types::RangeType::DateRange),
            CastDataType::Custom(name, struct_fields) => {
                if struct_fields.is_empty() {
                    DataType::Custom(name.clone())
                } else {
                    DataType::Struct(struct_fields.clone())
                }
            }
        }
    }

    fn infer_binary_op_type(
        op: &crate::optimizer::expr::BinaryOp,
        left_type: Option<crate::types::DataType>,
        right_type: Option<crate::types::DataType>,
    ) -> Option<crate::types::DataType> {
        use yachtsql_core::types::DataType;
        use yachtsql_optimizer::expr::BinaryOp;

        match op {
            BinaryOp::Equal
            | BinaryOp::NotEqual
            | BinaryOp::LessThan
            | BinaryOp::LessThanOrEqual
            | BinaryOp::GreaterThan
            | BinaryOp::GreaterThanOrEqual
            | BinaryOp::And
            | BinaryOp::Or
            | BinaryOp::RangeAdjacent
            | BinaryOp::RangeStrictlyLeft
            | BinaryOp::RangeStrictlyRight => Some(DataType::Bool),

            BinaryOp::Add => match (&left_type, &right_type) {
                (Some(DataType::Range(rt)), Some(DataType::Range(_))) => {
                    Some(DataType::Range(rt.clone()))
                }
                (Some(DataType::Timestamp), Some(DataType::Interval))
                | (Some(DataType::Interval), Some(DataType::Timestamp))
                | (Some(DataType::TimestampTz), Some(DataType::Interval))
                | (Some(DataType::Interval), Some(DataType::TimestampTz)) => {
                    Some(DataType::Timestamp)
                }

                (Some(DataType::Date), Some(DataType::Interval))
                | (Some(DataType::Interval), Some(DataType::Date)) => Some(DataType::Date),

                (Some(DataType::Interval), Some(DataType::Interval)) => Some(DataType::Interval),

                (Some(DataType::Numeric(_)), _) | (_, Some(DataType::Numeric(_))) => {
                    Some(DataType::Numeric(None))
                }
                (Some(DataType::Float64), _) | (_, Some(DataType::Float64)) => {
                    Some(DataType::Float64)
                }
                (Some(DataType::Int64), Some(DataType::Int64)) => Some(DataType::Int64),
                _ => None,
            },
            BinaryOp::Subtract => match (&left_type, &right_type) {
                (Some(DataType::Json), Some(DataType::String))
                | (Some(DataType::Json), Some(DataType::Int64)) => Some(DataType::Json),

                (Some(DataType::Range(rt)), Some(DataType::Range(_))) => {
                    Some(DataType::Range(rt.clone()))
                }
                (Some(DataType::Timestamp), Some(DataType::Interval))
                | (Some(DataType::TimestampTz), Some(DataType::Interval)) => {
                    Some(DataType::Timestamp)
                }

                (Some(DataType::Timestamp), Some(DataType::Timestamp))
                | (Some(DataType::TimestampTz), Some(DataType::TimestampTz))
                | (Some(DataType::Timestamp), Some(DataType::TimestampTz))
                | (Some(DataType::TimestampTz), Some(DataType::Timestamp)) => {
                    Some(DataType::Interval)
                }

                (Some(DataType::Date), Some(DataType::Interval)) => Some(DataType::Date),

                (Some(DataType::Interval), Some(DataType::Interval)) => Some(DataType::Interval),

                (Some(DataType::Numeric(_)), _) | (_, Some(DataType::Numeric(_))) => {
                    Some(DataType::Numeric(None))
                }
                (Some(DataType::Float64), _) | (_, Some(DataType::Float64)) => {
                    Some(DataType::Float64)
                }
                (Some(DataType::Int64), Some(DataType::Int64)) => Some(DataType::Int64),
                _ => None,
            },
            BinaryOp::Multiply => match (&left_type, &right_type) {
                (Some(DataType::Range(rt)), Some(DataType::Range(_))) => {
                    Some(DataType::Range(rt.clone()))
                }
                (Some(DataType::Interval), Some(DataType::Int64))
                | (Some(DataType::Interval), Some(DataType::Float64))
                | (Some(DataType::Int64), Some(DataType::Interval))
                | (Some(DataType::Float64), Some(DataType::Interval)) => Some(DataType::Interval),
                (Some(DataType::Numeric(_)), _) | (_, Some(DataType::Numeric(_))) => {
                    Some(DataType::Numeric(None))
                }
                (Some(DataType::Float64), _) | (_, Some(DataType::Float64)) => {
                    Some(DataType::Float64)
                }
                (Some(DataType::Int64), Some(DataType::Int64)) => Some(DataType::Int64),
                _ => None,
            },
            BinaryOp::Divide => match (&left_type, &right_type) {
                (Some(DataType::Interval), Some(DataType::Int64))
                | (Some(DataType::Interval), Some(DataType::Float64)) => Some(DataType::Interval),
                (Some(DataType::Numeric(_)), _) | (_, Some(DataType::Numeric(_))) => {
                    Some(DataType::Numeric(None))
                }
                (Some(DataType::Float64), _) | (_, Some(DataType::Float64)) => {
                    Some(DataType::Float64)
                }
                (Some(DataType::Int64), Some(DataType::Int64)) => Some(DataType::Int64),
                _ => None,
            },
            BinaryOp::Modulo => match (left_type, right_type) {
                (Some(DataType::Numeric(_)), _) | (_, Some(DataType::Numeric(_))) => {
                    Some(DataType::Numeric(None))
                }
                (Some(DataType::Float64), _) | (_, Some(DataType::Float64)) => {
                    Some(DataType::Float64)
                }
                (Some(DataType::Int64), Some(DataType::Int64)) => Some(DataType::Int64),
                _ => None,
            },

            BinaryOp::VectorL2Distance
            | BinaryOp::VectorInnerProduct
            | BinaryOp::VectorCosineDistance => Some(DataType::Float64),

            BinaryOp::ArrayContains | BinaryOp::ArrayContainedBy | BinaryOp::ArrayOverlap => {
                Some(DataType::Bool)
            }

            BinaryOp::Like
            | BinaryOp::NotLike
            | BinaryOp::ILike
            | BinaryOp::NotILike
            | BinaryOp::SimilarTo
            | BinaryOp::NotSimilarTo
            | BinaryOp::RegexMatch
            | BinaryOp::RegexNotMatch
            | BinaryOp::RegexMatchI
            | BinaryOp::RegexNotMatchI => Some(DataType::Bool),

            BinaryOp::Concat => match (&left_type, &right_type) {
                (Some(DataType::Json), Some(DataType::Json)) => Some(DataType::Json),
                (Some(DataType::Hstore), Some(DataType::Hstore)) => Some(DataType::Hstore),
                (Some(DataType::String), _) | (_, Some(DataType::String)) => Some(DataType::String),
                (Some(DataType::Bytes), Some(DataType::Bytes)) => Some(DataType::Bytes),
                _ => left_type.or(right_type),
            },

            BinaryOp::HashMinus => match (&left_type, &right_type) {
                (Some(DataType::Json), _) => Some(DataType::Json),
                _ => None,
            },

            BinaryOp::BitwiseAnd | BinaryOp::BitwiseOr | BinaryOp::BitwiseXor => {
                Some(DataType::Int64)
            }

            BinaryOp::ShiftLeft | BinaryOp::ShiftRight => match (&left_type, &right_type) {
                (Some(DataType::Range(_)), Some(DataType::Range(_))) => Some(DataType::Bool),
                _ => Some(DataType::Int64),
            },

            _ => None,
        }
    }

    fn infer_unary_op_type(
        op: &crate::optimizer::expr::UnaryOp,
        operand_type: Option<crate::types::DataType>,
    ) -> Option<crate::types::DataType> {
        use yachtsql_core::types::DataType;
        use yachtsql_optimizer::expr::UnaryOp;

        match op {
            UnaryOp::IsNull | UnaryOp::IsNotNull | UnaryOp::Not => Some(DataType::Bool),
            UnaryOp::Negate | UnaryOp::Plus => operand_type,
            UnaryOp::BitwiseNot => Some(DataType::Int64),
        }
    }

    fn infer_literal_type(
        lit: &crate::optimizer::expr::LiteralValue,
    ) -> Option<crate::types::DataType> {
        use yachtsql_core::types::DataType;
        use yachtsql_optimizer::expr::LiteralValue;

        match lit {
            LiteralValue::Null => None,
            LiteralValue::Boolean(_) => Some(DataType::Bool),
            LiteralValue::Int64(_) => Some(DataType::Int64),
            LiteralValue::Float64(_) => Some(DataType::Float64),
            LiteralValue::Numeric(_) => Some(DataType::Numeric(None)),
            LiteralValue::String(_) => Some(DataType::String),
            LiteralValue::Bytes(_) => Some(DataType::Bytes),
            LiteralValue::Date(_) => Some(DataType::Date),
            LiteralValue::Timestamp(_) => Some(DataType::Timestamp),
            LiteralValue::Json(_) => Some(DataType::Json),
            LiteralValue::Array(elements) => {
                let element_type = elements
                    .first()
                    .and_then(Self::infer_expr_type)
                    .unwrap_or(DataType::String);
                Some(DataType::Array(Box::new(element_type)))
            }
            LiteralValue::Uuid(_) => Some(DataType::Uuid),
            LiteralValue::Vector(vec) => Some(DataType::Vector(vec.len())),
            LiteralValue::Interval(_) => Some(DataType::Interval),
            LiteralValue::Range(_) => {
                Some(DataType::Range(yachtsql_core::types::RangeType::Int4Range))
            }
            LiteralValue::Point(_) => Some(DataType::Point),
            LiteralValue::PgBox(_) => Some(DataType::PgBox),
            LiteralValue::Circle(_) => Some(DataType::Circle),
            LiteralValue::MacAddr(_) => Some(DataType::MacAddr),
            LiteralValue::MacAddr8(_) => Some(DataType::MacAddr8),
        }
    }

    fn infer_coalesce_type(
        args: &[crate::optimizer::expr::Expr],
        schema: &Schema,
    ) -> Option<crate::types::DataType> {
        for arg in args {
            if let Some(arg_type) = Self::infer_expr_type_with_schema(arg, schema) {
                return Some(arg_type);
            }
        }
        None
    }

    fn infer_first_arg_type(
        args: &[crate::optimizer::expr::Expr],
        schema: &Schema,
    ) -> Option<crate::types::DataType> {
        args.first()
            .and_then(|arg| Self::infer_expr_type_with_schema(arg, schema))
    }

    #[inline]
    fn infer_comparison_operator_type() -> Option<crate::types::DataType> {
        use yachtsql_core::types::DataType;
        Some(DataType::Bool)
    }

    fn infer_function_type(
        name: &yachtsql_ir::FunctionName,
        args: &[crate::optimizer::expr::Expr],
        schema: &Schema,
    ) -> Option<crate::types::DataType> {
        use yachtsql_core::types::DataType;
        use yachtsql_ir::FunctionName;

        match name {
            FunctionName::Custom(s) if s == "YACHTSQL.IS_FEATURE_ENABLED" => Some(DataType::Bool),

            FunctionName::Abs
            | FunctionName::Absolute
            | FunctionName::Ceil
            | FunctionName::Ceiling
            | FunctionName::Floor
            | FunctionName::Round
            | FunctionName::Rnd
            | FunctionName::Trunc
            | FunctionName::Truncate
            | FunctionName::Mod
            | FunctionName::Modulo => {
                if !args.is_empty() {
                    let arg_type = Self::infer_expr_type_with_schema(&args[0], schema);
                    match arg_type {
                        Some(DataType::Int64) => Some(DataType::Int64),
                        Some(DataType::Numeric(p)) => Some(DataType::Numeric(p)),
                        Some(DataType::MacAddr) => Some(DataType::MacAddr),
                        Some(DataType::MacAddr8) => Some(DataType::MacAddr8),
                        _ => Some(DataType::Float64),
                    }
                } else {
                    Some(DataType::Float64)
                }
            }

            FunctionName::Sign | FunctionName::Signum => Some(DataType::Int64),

            FunctionName::Sqrt
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
            | FunctionName::Pow
            | FunctionName::Power
            | FunctionName::Random
            | FunctionName::Rand => Some(DataType::Float64),

            FunctionName::Custom(s)
                if s == "DEGREES"
                    || s == "RADIANS"
                    || s == "GAMMA"
                    || s == "LGAMMA"
                    || s == "TO_NUMBER"
                    || s == "SINH"
                    || s == "COSH"
                    || s == "TANH"
                    || s == "ASINH"
                    || s == "ACOSH"
                    || s == "ATANH"
                    || s == "COT"
                    || s == "SIND"
                    || s == "COSD"
                    || s == "TAND"
                    || s == "ASIND"
                    || s == "ACOSD"
                    || s == "ATAND"
                    || s == "ATAN2D"
                    || s == "COTD"
                    || s == "CBRT" =>
            {
                if s == "TO_NUMBER" && args.len() == 2 {
                    if let yachtsql_optimizer::Expr::Literal(
                        yachtsql_optimizer::expr::LiteralValue::String(fmt),
                    ) = &args[1]
                    {
                        if fmt.eq_ignore_ascii_case("RN") {
                            return Some(DataType::Int64);
                        }
                    }
                }
                Some(DataType::Float64)
            }

            FunctionName::Div => Some(DataType::Int64),

            FunctionName::Custom(s)
                if s == "FACTORIAL"
                    || s == "GCD"
                    || s == "LCM"
                    || s == "DIV"
                    || s == "SCALE"
                    || s == "MIN_SCALE"
                    || s == "WIDTH_BUCKET" =>
            {
                Some(DataType::Int64)
            }

            FunctionName::Custom(s) if s == "TRIM_SCALE" => Some(DataType::Numeric(None)),

            FunctionName::Custom(s) if s == "SETSEED" => Some(DataType::Unknown),

            FunctionName::Custom(s)
                if s == "SAFE_ADD"
                    || s == "SAFE_SUBTRACT"
                    || s == "SAFE_MULTIPLY"
                    || s == "SAFE_DIVIDE" =>
            {
                if args.len() >= 2 {
                    let left_type = Self::infer_expr_type_with_schema(&args[0], schema);
                    let right_type = Self::infer_expr_type_with_schema(&args[1], schema);

                    match (left_type, right_type) {
                        (Some(DataType::Float64), _) | (_, Some(DataType::Float64)) => {
                            Some(DataType::Float64)
                        }
                        (Some(DataType::Numeric(_)), _) | (_, Some(DataType::Numeric(_))) => {
                            Some(DataType::Numeric(None))
                        }
                        (Some(DataType::Int64), Some(DataType::Int64)) => Some(DataType::Int64),
                        _ => Some(DataType::Int64),
                    }
                } else {
                    Some(DataType::Int64)
                }
            }

            FunctionName::Custom(s) if s == "SAFE_NEGATE" => {
                if !args.is_empty() {
                    let arg_type = Self::infer_expr_type_with_schema(&args[0], schema);
                    match arg_type {
                        Some(DataType::Float64) => Some(DataType::Float64),
                        Some(DataType::Numeric(_)) => Some(DataType::Numeric(None)),
                        _ => Some(DataType::Int64),
                    }
                } else {
                    Some(DataType::Int64)
                }
            }

            FunctionName::Concat
            | FunctionName::Concatenate
            | FunctionName::Trim
            | FunctionName::Btrim
            | FunctionName::Ltrim
            | FunctionName::TrimLeft
            | FunctionName::Rtrim
            | FunctionName::TrimRight
            | FunctionName::Replace
            | FunctionName::StrReplace
            | FunctionName::Ucase
            | FunctionName::Lcase
            | FunctionName::Mid
            | FunctionName::Left
            | FunctionName::Right
            | FunctionName::Repeat
            | FunctionName::Replicate
            | FunctionName::Lpad
            | FunctionName::LeftPad
            | FunctionName::Rpad
            | FunctionName::RightPad
            | FunctionName::Chr
            | FunctionName::Char
            | FunctionName::Initcap
            | FunctionName::Proper
            | FunctionName::ToChar
            | FunctionName::Translate => Some(DataType::String),

            FunctionName::Custom(s)
                if s == "FORMAT"
                    || s == "QUOTE_IDENT"
                    || s == "QUOTE_LITERAL"
                    || s == "REGEXP_EXTRACT"
                    || s == "REGEXP_REPLACE" =>
            {
                Some(DataType::String)
            }

            FunctionName::Reverse | FunctionName::Strrev => {
                if !args.is_empty() {
                    match Self::infer_expr_type_with_schema(&args[0], schema) {
                        Some(DataType::Bytes) => Some(DataType::Bytes),
                        _ => Some(DataType::String),
                    }
                } else {
                    Some(DataType::String)
                }
            }

            FunctionName::Substr | FunctionName::Substring => {
                if !args.is_empty() {
                    match Self::infer_expr_type_with_schema(&args[0], schema) {
                        Some(DataType::Bytes) => Some(DataType::Bytes),
                        _ => Some(DataType::String),
                    }
                } else {
                    Some(DataType::String)
                }
            }

            FunctionName::Lower | FunctionName::Upper => {
                if !args.is_empty() {
                    match Self::infer_expr_type_with_schema(&args[0], schema) {
                        Some(DataType::Range(range_type)) => match range_type {
                            yachtsql_core::types::RangeType::Int4Range
                            | yachtsql_core::types::RangeType::Int8Range => Some(DataType::Int64),
                            yachtsql_core::types::RangeType::NumRange => Some(DataType::Float64),
                            yachtsql_core::types::RangeType::DateRange => Some(DataType::Date),
                            yachtsql_core::types::RangeType::TsRange
                            | yachtsql_core::types::RangeType::TsTzRange => {
                                Some(DataType::Timestamp)
                            }
                        },
                        _ => Some(DataType::String),
                    }
                } else {
                    Some(DataType::String)
                }
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "LOWER_INC"
                        | "UPPER_INC"
                        | "LOWER_INF"
                        | "UPPER_INF"
                        | "ISEMPTY"
                        | "RANGE_ISEMPTY"
                        | "RANGE_CONTAINS"
                        | "RANGE_CONTAINS_ELEM"
                        | "RANGE_OVERLAPS"
                        | "RANGE_ADJACENT"
                        | "RANGE_STRICTLY_LEFT"
                        | "RANGE_STRICTLY_RIGHT"
                ) =>
            {
                Some(DataType::Bool)
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "RANGE_MERGE" | "RANGE_UNION" | "RANGE_INTERSECTION" | "RANGE_DIFFERENCE"
                ) =>
            {
                if !args.is_empty() {
                    Self::infer_expr_type_with_schema(&args[0], schema)
                } else {
                    None
                }
            }

            FunctionName::Length
            | FunctionName::Len
            | FunctionName::CharLength
            | FunctionName::CharacterLength
            | FunctionName::OctetLength
            | FunctionName::Lengthb
            | FunctionName::Position
            | FunctionName::Strpos
            | FunctionName::Instr
            | FunctionName::Locate
            | FunctionName::Ascii
            | FunctionName::Ord => Some(DataType::Int64),

            FunctionName::Custom(s)
                if s == "STARTS_WITH" || s == "ENDS_WITH" || s == "REGEXP_CONTAINS" =>
            {
                Some(DataType::Bool)
            }

            FunctionName::Md5
            | FunctionName::Md5Hash
            | FunctionName::Sha256
            | FunctionName::Sha2 => Some(DataType::String),

            FunctionName::Custom(s) if s == "SHA1" || s == "SHA512" => Some(DataType::String),

            FunctionName::Custom(s) if s == "FARM_FINGERPRINT" || s == "CRC32" || s == "CRC32C" => {
                Some(DataType::Int64)
            }

            FunctionName::Custom(s) if s == "TO_HEX" => Some(DataType::String),
            FunctionName::Custom(s) if s == "FROM_HEX" => Some(DataType::Bytes),

            FunctionName::CurrentDate | FunctionName::Curdate | FunctionName::Today => {
                Some(DataType::Date)
            }
            FunctionName::CurrentTimestamp
            | FunctionName::Getdate
            | FunctionName::Sysdate
            | FunctionName::Systimestamp
            | FunctionName::Now => Some(DataType::Timestamp),

            FunctionName::Date
            | FunctionName::CastDate
            | FunctionName::ToDate
            | FunctionName::DateAdd
            | FunctionName::Dateadd
            | FunctionName::Adddate
            | FunctionName::DateSub
            | FunctionName::Datesub
            | FunctionName::Subdate
            | FunctionName::DateTrunc
            | FunctionName::TruncDate => Some(DataType::Date),

            FunctionName::Custom(s) if s == "TIMESTAMP_DIFF" => Some(DataType::Int64),
            FunctionName::Custom(s) if s == "TIMESTAMP_TRUNC" => Some(DataType::Timestamp),

            FunctionName::Custom(s) if s == "PARSE_DATE" => Some(DataType::Date),
            FunctionName::StrToDate | FunctionName::ParseDatetime => Some(DataType::Date),
            FunctionName::Custom(s) if s == "PARSE_TIMESTAMP" || s == "AT_TIME_ZONE" => {
                Some(DataType::Timestamp)
            }

            FunctionName::Custom(s) if s == "MAKE_DATE" => Some(DataType::Date),
            FunctionName::Custom(s) if s == "MAKE_TIMESTAMP" => Some(DataType::Timestamp),
            FunctionName::CurrentTime
            | FunctionName::Curtime
            | FunctionName::Localtime
            | FunctionName::Localtimestamp => Some(DataType::Time),

            FunctionName::Extract
            | FunctionName::DatePart
            | FunctionName::Datepart
            | FunctionName::DateDiff
            | FunctionName::Datediff
            | FunctionName::Timestampdiff => Some(DataType::Int64),

            FunctionName::Age => Some(DataType::Interval),

            FunctionName::Custom(s)
                if s == "JUSTIFY_DAYS" || s == "JUSTIFY_HOURS" || s == "JUSTIFY_INTERVAL" =>
            {
                Some(DataType::Interval)
            }

            FunctionName::Custom(s)
                if s == "YEAR"
                    || s == "MONTH"
                    || s == "DAY"
                    || s == "HOUR"
                    || s == "MINUTE"
                    || s == "SECOND"
                    || s == "QUARTER"
                    || s == "WEEK"
                    || s == "DOW"
                    || s == "DOY"
                    || s == "DAYOFWEEK"
                    || s == "DAYOFYEAR" =>
            {
                Some(DataType::Int64)
            }

            FunctionName::FormatTimestamp | FunctionName::DateFormat => Some(DataType::String),
            FunctionName::Custom(s) if s == "FORMAT_DATE" || s == "FORMAT_TIMESTAMP" => {
                Some(DataType::String)
            }

            FunctionName::Custom(s)
                if s == "NEXTVAL" || s == "CURRVAL" || s == "SETVAL" || s == "LASTVAL" =>
            {
                Some(DataType::Int64)
            }

            FunctionName::ArrayLength | FunctionName::Cardinality => Some(DataType::Int64),
            FunctionName::Custom(s) if s == "ARRAY_POSITION" => Some(DataType::Int64),
            FunctionName::ArrayContains | FunctionName::ArrayContainsAll => Some(DataType::Bool),
            FunctionName::Split | FunctionName::StringSplit => {
                Some(DataType::Array(Box::new(DataType::String)))
            }
            FunctionName::SplitPart => Some(DataType::String),
            FunctionName::Custom(s) if s == "STRING_TO_ARRAY" => {
                Some(DataType::Array(Box::new(DataType::String)))
            }

            FunctionName::Custom(s) if s == "GENERATE_ARRAY" => {
                Some(DataType::Array(Box::new(DataType::Int64)))
            }
            FunctionName::Custom(s) if s == "GENERATE_DATE_ARRAY" => {
                Some(DataType::Array(Box::new(DataType::Date)))
            }
            FunctionName::Custom(s) if s == "GENERATE_TIMESTAMP_ARRAY" => {
                Some(DataType::Array(Box::new(DataType::Timestamp)))
            }
            FunctionName::GenerateUuid
            | FunctionName::Uuid
            | FunctionName::GenRandomUuid
            | FunctionName::Newid
            | FunctionName::UuidGenerateV4 => Some(DataType::String),
            FunctionName::Custom(s)
                if s == "UUID_GENERATE_V1" || s == "UUIDV4" || s == "UUIDV7" =>
            {
                Some(DataType::String)
            }
            FunctionName::Custom(s) if s == "GENERATE_UUID_ARRAY" => {
                Some(DataType::Array(Box::new(DataType::String)))
            }

            FunctionName::Custom(s) if s == "GEN_RANDOM_BYTES" || s == "DIGEST" => {
                Some(DataType::Bytes)
            }
            FunctionName::Encode => Some(DataType::String),
            FunctionName::Decode => Some(DataType::Bytes),

            FunctionName::Custom(s)
                if s == "HSTORE_EXISTS"
                    || s == "HSTORE_EXISTS_ALL"
                    || s == "HSTORE_EXISTS_ANY"
                    || s == "HSTORE_CONTAINS"
                    || s == "HSTORE_CONTAINED_BY"
                    || s == "HSTORE_DEFINED"
                    || s == "DEFINED"
                    || s == "EXIST" =>
            {
                Some(DataType::Bool)
            }
            FunctionName::Custom(s)
                if s == "HSTORE_CONCAT"
                    || s == "HSTORE_DELETE"
                    || s == "HSTORE_DELETE_KEY"
                    || s == "HSTORE_DELETE_KEYS"
                    || s == "HSTORE_DELETE_HSTORE"
                    || s == "HSTORE_SLICE"
                    || s == "SLICE"
                    || s == "DELETE"
                    || s == "HSTORE" =>
            {
                Some(DataType::Hstore)
            }
            FunctionName::Custom(s) if s == "HSTORE_AKEYS" || s == "AKEYS" => {
                Some(DataType::Array(Box::new(DataType::String)))
            }
            FunctionName::Custom(s) if s == "SKEYS" || s == "SVALS" => Some(DataType::String),
            FunctionName::Custom(s) if s == "HSTORE_AVALS" || s == "AVALS" => {
                Some(DataType::Array(Box::new(DataType::String)))
            }
            FunctionName::Custom(s) if s == "HSTORE_TO_JSON" || s == "HSTORE_TO_JSONB" => {
                Some(DataType::Json)
            }
            FunctionName::Custom(s) if s == "HSTORE_TO_ARRAY" => {
                Some(DataType::Array(Box::new(DataType::String)))
            }
            FunctionName::Custom(s) if s == "HSTORE_TO_MATRIX" => Some(DataType::Array(Box::new(
                DataType::Array(Box::new(DataType::String)),
            ))),
            FunctionName::Custom(s) if s == "HSTORE_GET" => Some(DataType::String),
            FunctionName::Custom(s) if s == "HSTORE_GET_VALUES" => {
                Some(DataType::Array(Box::new(DataType::String)))
            }

            FunctionName::Custom(s)
                if s == "ARRAY_REVERSE"
                    || s == "ARRAY_SORT"
                    || s == "ARRAY_DISTINCT"
                    || s == "ARRAY_REPLACE" =>
            {
                Self::infer_array_type_from_first_arg(args, schema)
            }

            FunctionName::Custom(s) if s == "ARRAY_APPEND" || s == "ARRAY_REMOVE" => {
                Self::infer_array_type_with_element_fallback(0, 1, args, schema)
            }

            FunctionName::Custom(s) if s == "ARRAY_PREPEND" => {
                Self::infer_array_type_with_element_fallback(1, 0, args, schema)
            }

            FunctionName::ArrayConcat | FunctionName::ArrayCat => {
                Self::infer_array_type_from_non_empty_array(args, schema)
            }

            FunctionName::Custom(s)
                if s == "ARRAYMAP"
                    || s == "ARRAYFILTER"
                    || s == "ARRAYSORT"
                    || s == "ARRAYREVERSESORT" =>
            {
                if args.len() >= 2 {
                    Self::infer_expr_type_with_schema(&args[1], schema)
                } else {
                    None
                }
            }

            FunctionName::Custom(s)
                if s == "ARRAYEXISTS"
                    || s == "ARRAYALL"
                    || s == "ARRAYCOUNT"
                    || s == "ARRAYFIRSTINDEX"
                    || s == "ARRAYLASTINDEX" =>
            {
                Some(DataType::Int64)
            }

            FunctionName::Custom(s) if s == "ARRAYFIRST" || s == "ARRAYLAST" => {
                if args.len() >= 2 {
                    if let Some(DataType::Array(elem_type)) =
                        Self::infer_expr_type_with_schema(&args[1], schema)
                    {
                        Some(*elem_type)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }

            FunctionName::Custom(s) if s == "ARRAYSUM" || s == "ARRAYMIN" || s == "ARRAYMAX" => {
                if args.len() >= 2 && matches!(&args[0], yachtsql_optimizer::Expr::Lambda { .. }) {
                    Some(DataType::Int64)
                } else if !args.is_empty() {
                    if let Some(DataType::Array(elem_type)) =
                        Self::infer_expr_type_with_schema(&args[0], schema)
                    {
                        Some(*elem_type)
                    } else {
                        Some(DataType::Int64)
                    }
                } else {
                    Some(DataType::Int64)
                }
            }

            FunctionName::Custom(s) if s == "ARRAYAVG" => Some(DataType::Float64),

            FunctionName::Custom(s) if s == "ARRAYFOLD" => {
                if args.len() >= 3 {
                    Self::infer_expr_type_with_schema(&args[2], schema)
                } else {
                    None
                }
            }

            FunctionName::Custom(s) if s == "ARRAYREDUCE" => {
                if args.len() >= 2 {
                    if let Some(DataType::Array(elem_type)) =
                        Self::infer_expr_type_with_schema(&args[1], schema)
                    {
                        Some(*elem_type)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }

            FunctionName::Custom(s) if s == "ARRAYREDUCEINRANGES" => {
                Some(DataType::Array(Box::new(DataType::Unknown)))
            }

            FunctionName::Custom(s)
                if s == "ARRAYCUMSUM"
                    || s == "ARRAYCUMSUMNONNEGATIVE"
                    || s == "ARRAYDIFFERENCE" =>
            {
                Some(DataType::Array(Box::new(DataType::Int64)))
            }

            FunctionName::Custom(s) if s == "ARRAYSPLIT" || s == "ARRAYREVERSESPLIT" => {
                if args.len() >= 2 {
                    if let Some(arr_type) = Self::infer_expr_type_with_schema(&args[1], schema) {
                        Some(DataType::Array(Box::new(arr_type)))
                    } else {
                        Some(DataType::Array(Box::new(DataType::Array(Box::new(
                            DataType::Unknown,
                        )))))
                    }
                } else {
                    Some(DataType::Array(Box::new(DataType::Array(Box::new(
                        DataType::Unknown,
                    )))))
                }
            }

            FunctionName::Custom(s) if s == "ARRAYCOMPACT" => {
                if !args.is_empty() {
                    Self::infer_expr_type_with_schema(&args[0], schema)
                } else {
                    None
                }
            }

            FunctionName::Custom(s) if s == "ARRAYZIP" => {
                Some(DataType::Array(Box::new(DataType::Struct(vec![]))))
            }

            FunctionName::Custom(s) if s == "ARRAYAUC" => Some(DataType::Float64),

            FunctionName::Greatest
            | FunctionName::MaxValue
            | FunctionName::Least
            | FunctionName::MinValue => {
                let mut has_float = false;
                let mut has_int = false;
                let mut has_string = false;
                let mut result_type: Option<DataType> = None;

                for arg in args {
                    if let Some(arg_type) = Self::infer_expr_type_with_schema(arg, schema) {
                        match arg_type {
                            DataType::Float64 => has_float = true,
                            DataType::Int64 => has_int = true,
                            DataType::String => has_string = true,
                            _ => {}
                        }
                        if result_type.is_none() {
                            result_type = Some(arg_type);
                        }
                    }
                }

                if has_float && has_int {
                    Some(DataType::Float64)
                } else if has_string {
                    Some(DataType::String)
                } else {
                    result_type
                }
            }

            FunctionName::Coalesce => Self::infer_coalesce_type(args, schema),
            FunctionName::Ifnull | FunctionName::Nvl | FunctionName::Isnull => {
                Self::infer_first_arg_type(args, schema)
            }
            FunctionName::Nullif => Self::infer_first_arg_type(args, schema),

            FunctionName::If | FunctionName::Iif => args
                .get(1)
                .and_then(|arg| Self::infer_expr_type_with_schema(arg, schema)),

            FunctionName::Count => Some(DataType::Int64),
            FunctionName::Sum
            | FunctionName::Avg
            | FunctionName::Average
            | FunctionName::Min
            | FunctionName::Minimum
            | FunctionName::Max
            | FunctionName::Maximum
            | FunctionName::Mode => None,

            FunctionName::Stddev
            | FunctionName::Stdev
            | FunctionName::StandardDeviation
            | FunctionName::StddevPop
            | FunctionName::Stddevp
            | FunctionName::StddevSamp
            | FunctionName::Stddevs
            | FunctionName::Variance
            | FunctionName::Var
            | FunctionName::VarPop
            | FunctionName::Varp
            | FunctionName::VarSamp
            | FunctionName::Vars
            | FunctionName::Median
            | FunctionName::PercentileCont
            | FunctionName::PercentileDisc
            | FunctionName::Corr
            | FunctionName::CovarPop
            | FunctionName::CovarSamp => Some(DataType::Float64),

            FunctionName::Custom(s)
                if s == "JSON_AGG"
                    || s == "JSONB_AGG"
                    || s == "JSON_OBJECT_AGG"
                    || s == "JSONB_OBJECT_AGG" =>
            {
                Some(DataType::Json)
            }

            FunctionName::JsonExtract => {
                if let Some(first_arg) = args.first() {
                    if let Some(arg_type) = Self::infer_expr_type_with_schema(first_arg, schema) {
                        if matches!(arg_type, DataType::Hstore) {
                            return Some(DataType::String);
                        }
                    }
                }
                Some(DataType::Json)
            }
            FunctionName::Custom(s) if s == "JSON_EXTRACT_JSON" => {
                if let Some(first_arg) = args.first() {
                    if let Some(arg_type) = Self::infer_expr_type_with_schema(first_arg, schema) {
                        if matches!(arg_type, DataType::Hstore) {
                            return Some(DataType::String);
                        }
                    }
                }
                Some(DataType::Json)
            }
            FunctionName::Custom(s)
                if s == "JSON_ARRAY"
                    || s == "JSON_OBJECT"
                    || s == "PARSE_JSON"
                    || s == "TO_JSON"
                    || s == "TO_JSONB"
                    || s == "JSON_BUILD_ARRAY"
                    || s == "JSONB_BUILD_ARRAY"
                    || s == "JSON_BUILD_OBJECT"
                    || s == "JSONB_BUILD_OBJECT"
                    || s == "JSON_STRIP_NULLS"
                    || s == "JSONB_STRIP_NULLS"
                    || s == "JSONB_INSERT" =>
            {
                Some(DataType::Json)
            }

            FunctionName::Custom(s) if s == "TO_JSON_STRING" => Some(DataType::String),
            FunctionName::Custom(s) if s == "JSONB_PRETTY" => Some(DataType::String),
            FunctionName::Custom(s) if s == "JSON_OBJECT_KEYS" || s == "JSONB_OBJECT_KEYS" => {
                Some(DataType::String)
            }

            FunctionName::JsonQuery => Some(DataType::Json),
            FunctionName::JsonType | FunctionName::JsonTypeof => Some(DataType::String),
            FunctionName::Custom(s) if s == "JSON_EXISTS" => Some(DataType::Bool),
            FunctionName::Custom(s) if s == "JSONB_PATH_EXISTS" || s == "JSONB_PATH_MATCH" => {
                Some(DataType::Bool)
            }
            FunctionName::Custom(s) if s == "JSONB_PATH_QUERY_FIRST" => Some(DataType::Json),
            FunctionName::Custom(s) if s == "JSONB_CONTAINS" => Some(DataType::Bool),
            FunctionName::Custom(s)
                if s == "JSONB_CONCAT"
                    || s == "JSONB_DELETE"
                    || s == "JSONB_DELETE_PATH"
                    || s == "JSONB_SET" =>
            {
                Some(DataType::Json)
            }
            FunctionName::JsonLength | FunctionName::JsonArrayLength => Some(DataType::Int64),
            FunctionName::Custom(s) if s == "JSON_KEYS" => Some(DataType::Json),

            FunctionName::Custom(s)
                if s == "IS_JSON_VALUE"
                    || s == "IS_JSON_ARRAY"
                    || s == "IS_JSON_OBJECT"
                    || s == "IS_JSON_SCALAR" =>
            {
                Some(DataType::Bool)
            }
            FunctionName::Custom(s)
                if s == "IS_NOT_JSON_VALUE"
                    || s == "IS_NOT_JSON_ARRAY"
                    || s == "IS_NOT_JSON_OBJECT"
                    || s == "IS_NOT_JSON_SCALAR" =>
            {
                Some(DataType::Bool)
            }

            FunctionName::JsonValue | FunctionName::JsonExtractScalar => args
                .get(2)
                .and_then(|arg| {
                    if let crate::optimizer::expr::Expr::Literal(
                        crate::optimizer::expr::LiteralValue::String(s),
                    ) = arg
                    {
                        JsonValueEvalOptions::from_literal(&Value::string(s.clone()))
                            .map(|options| options.inferred_return_type())
                            .ok()
                    } else {
                        None
                    }
                })
                .or(Some(DataType::String)),
            FunctionName::JsonExtractString => Some(DataType::String),
            FunctionName::Custom(s) if s == "JSON_VALUE_TEXT" => Some(DataType::String),
            FunctionName::Custom(s) if s == "JSON_EXTRACT_PATH_ARRAY" => Some(DataType::Json),
            FunctionName::Custom(s) if s == "JSON_EXTRACT_PATH_ARRAY_TEXT" => {
                Some(DataType::String)
            }

            FunctionName::Custom(s) if s == "BIT_AND" || s == "BIT_OR" || s == "BIT_XOR" => {
                Some(DataType::Int64)
            }

            FunctionName::Custom(s)
                if s == "BIT_COUNT" || s == "GET_BIT" || s == "OCTET_LENGTH" =>
            {
                Some(DataType::Int64)
            }

            FunctionName::Custom(s) if s == "SET_BIT" => Some(DataType::Bytes),

            FunctionName::Custom(s) if s == "BOOL_AND" || s == "BOOL_OR" || s == "EVERY" => {
                Some(DataType::Bool)
            }

            FunctionName::ApproxCountDistinct
            | FunctionName::ApproxDistinct
            | FunctionName::Ndv => Some(DataType::Int64),
            FunctionName::ApproxQuantiles => Some(DataType::Array(Box::new(DataType::Float64))),
            FunctionName::ApproxTopCount | FunctionName::ApproxTopSum => {
                Some(DataType::Array(Box::new(DataType::String)))
            }

            FunctionName::Custom(s) if s == "INTERVAL_LITERAL" || s == "INTERVAL_PARSE" => {
                Some(DataType::Interval)
            }

            FunctionName::Custom(s)
                if s == "NET.IP_FROM_STRING"
                    || s == "NET.SAFE_IP_FROM_STRING"
                    || s == "NET.IPV4_FROM_INT64"
                    || s == "NET.IP_NET_MASK"
                    || s == "NET.IP_TRUNC" =>
            {
                Some(DataType::Bytes)
            }
            FunctionName::Custom(s) if s == "NET.IPV4_TO_INT64" => Some(DataType::Int64),
            FunctionName::Custom(s)
                if s == "NET.IP_TO_STRING"
                    || s == "NET.HOST"
                    || s == "NET.PUBLIC_SUFFIX"
                    || s == "NET.REG_DOMAIN" =>
            {
                Some(DataType::String)
            }

            FunctionName::Custom(s)
                if s == "KEYS.KEYSET_CHAIN"
                    || s == "AEAD.ENCRYPT"
                    || s == "DETERMINISTIC_ENCRYPT" =>
            {
                Some(DataType::Bytes)
            }
            FunctionName::Custom(s)
                if s == "AEAD.DECRYPT_BYTES" || s == "DETERMINISTIC_DECRYPT_BYTES" =>
            {
                Some(DataType::Bytes)
            }
            FunctionName::Custom(s)
                if s == "AEAD.DECRYPT_STRING" || s == "DETERMINISTIC_DECRYPT_STRING" =>
            {
                Some(DataType::String)
            }

            FunctionName::Custom(s)
                if s == "TO_TSVECTOR"
                    || s == "TO_TSQUERY"
                    || s == "PLAINTO_TSQUERY"
                    || s == "PHRASETO_TSQUERY"
                    || s == "WEBSEARCH_TO_TSQUERY"
                    || s == "TS_HEADLINE"
                    || s == "STRIP"
                    || s == "SETWEIGHT"
                    || s == "TSVECTOR_CONCAT"
                    || s == "TSQUERY_AND"
                    || s == "TSQUERY_OR"
                    || s == "TSQUERY_NOT" =>
            {
                Some(DataType::String)
            }
            FunctionName::Custom(s) if s == "TS_RANK" || s == "TS_RANK_CD" => {
                Some(DataType::Float64)
            }
            FunctionName::Custom(s) if s == "TS_MATCH" => Some(DataType::Bool),
            FunctionName::Custom(s) if s == "TSVECTOR_LENGTH" => Some(DataType::Int64),

            FunctionName::Custom(s) if s == "POINT" => Some(DataType::Point),
            FunctionName::Custom(s) if s == "BOX" => Some(DataType::PgBox),
            FunctionName::Custom(s) if s == "CIRCLE" => Some(DataType::Circle),

            FunctionName::Custom(s)
                if s == "AREA"
                    || s == "WIDTH"
                    || s == "HEIGHT"
                    || s == "DIAGONAL"
                    || s == "RADIUS"
                    || s == "DIAMETER"
                    || s == "CENTER"
                    || s == "DISTANCE" =>
            {
                Some(DataType::Float64)
            }

            FunctionName::Custom(s) if s == "POINT_X" || s == "POINT_Y" => Some(DataType::Float64),

            FunctionName::Custom(s)
                if s == "ST_GEOGPOINT"
                    || s == "ST_GEOGFROMTEXT"
                    || s == "ST_GEOGFROMGEOJSON"
                    || s == "ST_MAKELINE"
                    || s == "ST_MAKEPOLYGON"
                    || s == "ST_CENTROID"
                    || s == "ST_STARTPOINT"
                    || s == "ST_ENDPOINT"
                    || s == "ST_POINTN"
                    || s == "ST_BOUNDARY"
                    || s == "ST_BUFFER"
                    || s == "ST_BUFFERWITHTOLERANCE"
                    || s == "ST_CLOSESTPOINT"
                    || s == "ST_CONVEXHULL"
                    || s == "ST_DIFFERENCE"
                    || s == "ST_INTERSECTION"
                    || s == "ST_SIMPLIFY"
                    || s == "ST_SNAPTOGRID"
                    || s == "ST_UNION"
                    || s == "ST_BOUNDINGBOX"
                    || s == "ST_GEOGPOINTFROMGEOHASH" =>
            {
                Some(DataType::Geography)
            }

            FunctionName::Custom(s)
                if s == "ST_X"
                    || s == "ST_Y"
                    || s == "ST_DISTANCE"
                    || s == "ST_LENGTH"
                    || s == "ST_AREA"
                    || s == "ST_PERIMETER"
                    || s == "ST_MAXDISTANCE"
                    || s == "ST_AZIMUTH" =>
            {
                Some(DataType::Float64)
            }

            FunctionName::Custom(s)
                if s == "ST_CONTAINS"
                    || s == "ST_COVERS"
                    || s == "ST_COVEREDBY"
                    || s == "ST_DISJOINT"
                    || s == "ST_DWITHIN"
                    || s == "ST_EQUALS"
                    || s == "ST_INTERSECTS"
                    || s == "ST_TOUCHES"
                    || s == "ST_WITHIN"
                    || s == "ST_ISEMPTY"
                    || s == "ST_ISCLOSED"
                    || s == "ST_ISCOLLECTION" =>
            {
                Some(DataType::Bool)
            }

            FunctionName::Custom(s)
                if s == "ST_NUMPOINTS" || s == "ST_NPOINTS" || s == "ST_DIMENSION" =>
            {
                Some(DataType::Int64)
            }

            FunctionName::Custom(s)
                if s == "ST_ASTEXT"
                    || s == "ST_ASGEOJSON"
                    || s == "ST_GEOMETRYTYPE"
                    || s == "ST_GEOHASH" =>
            {
                Some(DataType::String)
            }

            FunctionName::Custom(s) if s == "ST_ASBINARY" => Some(DataType::Bytes),

            FunctionName::Map => {
                if args.len() >= 2 {
                    let key_type = Self::infer_expr_type_with_schema(&args[0], schema)
                        .unwrap_or(DataType::String);
                    let value_type = Self::infer_expr_type_with_schema(&args[1], schema)
                        .unwrap_or(DataType::String);
                    Some(DataType::Map(Box::new(key_type), Box::new(value_type)))
                } else {
                    Some(DataType::Map(
                        Box::new(DataType::String),
                        Box::new(DataType::String),
                    ))
                }
            }
            FunctionName::MapFromArrays => {
                if args.len() >= 2 {
                    let key_type = Self::infer_expr_type_with_schema(&args[0], schema)
                        .and_then(|dt| match dt {
                            DataType::Array(elem) => Some(*elem),
                            _ => None,
                        })
                        .unwrap_or(DataType::String);
                    let value_type = Self::infer_expr_type_with_schema(&args[1], schema)
                        .and_then(|dt| match dt {
                            DataType::Array(elem) => Some(*elem),
                            _ => None,
                        })
                        .unwrap_or(DataType::String);
                    Some(DataType::Map(Box::new(key_type), Box::new(value_type)))
                } else {
                    Some(DataType::Map(
                        Box::new(DataType::String),
                        Box::new(DataType::String),
                    ))
                }
            }
            FunctionName::MapKeys => {
                if !args.is_empty() {
                    if let Some(DataType::Map(key_type, _)) =
                        Self::infer_expr_type_with_schema(&args[0], schema)
                    {
                        Some(DataType::Array(key_type))
                    } else {
                        Some(DataType::Array(Box::new(DataType::String)))
                    }
                } else {
                    Some(DataType::Array(Box::new(DataType::String)))
                }
            }
            FunctionName::MapValues => {
                if !args.is_empty() {
                    if let Some(DataType::Map(_, value_type)) =
                        Self::infer_expr_type_with_schema(&args[0], schema)
                    {
                        Some(DataType::Array(value_type))
                    } else {
                        Some(DataType::Array(Box::new(DataType::String)))
                    }
                } else {
                    Some(DataType::Array(Box::new(DataType::String)))
                }
            }
            FunctionName::MapContains => Some(DataType::Bool),
            FunctionName::MapAdd
            | FunctionName::MapSubtract
            | FunctionName::MapUpdate
            | FunctionName::MapConcat
            | FunctionName::MapPopulateSeries
            | FunctionName::MapFilter
            | FunctionName::MapApply
            | FunctionName::MapSort
            | FunctionName::MapReverseSort
            | FunctionName::MapPartialSort => {
                if !args.is_empty() {
                    let first_map_arg =
                        if matches!(&args[0], yachtsql_optimizer::Expr::Lambda { .. }) {
                            args.get(1)
                        } else {
                            args.first()
                        };
                    if let Some(arg) = first_map_arg {
                        if let Some(map_type @ DataType::Map(_, _)) =
                            Self::infer_expr_type_with_schema(arg, schema)
                        {
                            return Some(map_type);
                        }
                    }
                }
                Some(DataType::Map(
                    Box::new(DataType::String),
                    Box::new(DataType::Int64),
                ))
            }
            FunctionName::MapExists | FunctionName::MapAll => Some(DataType::Bool),

            _ => None,
        }
    }

    pub fn infer_expr_type_with_schema(
        expr: &crate::optimizer::expr::Expr,
        schema: &Schema,
    ) -> Option<crate::types::DataType> {
        use yachtsql_core::types::{DataType, StructField};
        use yachtsql_optimizer::expr::Expr;

        match expr {
            Expr::Column { name, table } => {
                if schema.field(name).is_some() {
                    schema.field(name).map(|f| f.data_type.clone())
                } else if let Some(table_name) = table {
                    if let Some(field) = schema.field(table_name) {
                        match &field.data_type {
                            DataType::Struct(fields) => fields
                                .iter()
                                .find(|f| f.name.eq_ignore_ascii_case(name))
                                .map(|f| f.data_type.clone()),
                            DataType::Custom(_) => None,
                            _ => None,
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            Expr::Literal(lit) => Self::infer_literal_type(lit),
            Expr::BinaryOp { left, op, right } => {
                let left_type = Self::infer_expr_type_with_schema(left, schema);
                let right_type = Self::infer_expr_type_with_schema(right, schema);
                Self::infer_binary_op_type(op, left_type, right_type)
            }
            Expr::UnaryOp { op, expr } => {
                let operand_type = Self::infer_expr_type_with_schema(expr, schema);
                Self::infer_unary_op_type(op, operand_type)
            }
            Expr::Function { name, args } => Self::infer_function_type(name, args, schema),
            Expr::Cast { data_type, .. } => Some(Self::cast_type_to_data_type(data_type)),
            Expr::TryCast { data_type, .. } => Some(Self::cast_type_to_data_type(data_type)),

            Expr::Between { .. }
            | Expr::InList { .. }
            | Expr::TupleInList { .. }
            | Expr::TupleInSubquery { .. }
            | Expr::IsDistinctFrom { .. } => Self::infer_comparison_operator_type(),
            Expr::StructLiteral { fields } => {
                let mut struct_fields = Vec::with_capacity(fields.len());
                for field in fields {
                    let field_type = field
                        .declared_type
                        .clone()
                        .or_else(|| Self::infer_expr_type_with_schema(&field.expr, schema))?;
                    struct_fields.push(StructField {
                        name: field.name.clone(),
                        data_type: field_type,
                    });
                }
                Some(DataType::Struct(struct_fields))
            }
            Expr::StructFieldAccess { expr, field } => {
                let inner_type = Self::infer_expr_type_with_schema(expr, schema);
                debug_print::debug_eprintln!(
                    "[type_inference] StructFieldAccess for field '{}', inner type: {:?}",
                    field,
                    inner_type
                );
                match inner_type? {
                    DataType::Struct(fields) => {
                        let field_names: Vec<_> = fields.iter().map(|f| &f.name).collect();
                        debug_print::debug_eprintln!(
                            "[type_inference] struct has fields: {:?}",
                            field_names
                        );
                        fields
                            .into_iter()
                            .find(|f| f.name.eq_ignore_ascii_case(field))
                            .map(|f| f.data_type)
                    }
                    DataType::Custom(type_name) => {
                        debug_print::debug_eprintln!(
                            "[type_inference] Custom type '{}', need to look up fields",
                            type_name
                        );
                        None
                    }
                    _ => None,
                }
            }
            Expr::ScalarSubquery { subquery } => {
                crate::query_executor::execution::infer_scalar_subquery_type_static(
                    subquery,
                    Some(schema),
                )
            }
            Expr::ArrayIndex { array, .. } => {
                match Self::infer_expr_type_with_schema(array, schema)? {
                    DataType::Array(elem_type) => Some(*elem_type),
                    DataType::Map(_, value_type) => Some(*value_type),
                    _ => None,
                }
            }
            Expr::ArraySlice { array, .. } => Self::infer_expr_type_with_schema(array, schema),
            Expr::Aggregate { name, args, .. } => Self::infer_function_type(name, args, schema),
            Expr::Case {
                when_then,
                else_expr,
                ..
            } => {
                let mut types = Vec::new();
                for (_, then_expr) in when_then {
                    if let Some(t) = Self::infer_expr_type_with_schema(then_expr, schema) {
                        types.push(t);
                    }
                }
                if let Some(else_e) = else_expr {
                    if let Some(t) = Self::infer_expr_type_with_schema(else_e, schema) {
                        types.push(t);
                    }
                }
                if types.is_empty() {
                    None
                } else if types.len() == 1 {
                    Some(types[0].clone())
                } else {
                    yachtsql_core::types::coercion::CoercionRules::find_common_type(&types).ok()
                }
            }
            _ => None,
        }
    }

    pub(super) fn infer_expr_type(
        expr: &crate::optimizer::expr::Expr,
    ) -> Option<crate::types::DataType> {
        use yachtsql_core::types::{DataType, StructField};
        use yachtsql_optimizer::expr::Expr;

        match expr {
            Expr::Literal(lit) => Self::infer_literal_type(lit),
            Expr::BinaryOp { left, op, right } => {
                let left_type = Self::infer_expr_type(left);
                let right_type = Self::infer_expr_type(right);
                Self::infer_binary_op_type(op, left_type, right_type)
            }
            Expr::UnaryOp { op, expr } => {
                let operand_type = Self::infer_expr_type(expr);
                Self::infer_unary_op_type(op, operand_type)
            }
            Expr::Function { name, args } => {
                let empty_schema = Schema::new();
                Self::infer_function_type(name, args, &empty_schema)
            }
            Expr::Cast { data_type, .. } => Some(Self::cast_type_to_data_type(data_type)),
            Expr::TryCast { data_type, .. } => Some(Self::cast_type_to_data_type(data_type)),

            Expr::Between { .. }
            | Expr::InList { .. }
            | Expr::TupleInList { .. }
            | Expr::TupleInSubquery { .. }
            | Expr::IsDistinctFrom { .. } => Self::infer_comparison_operator_type(),
            Expr::StructLiteral { fields } => {
                let mut struct_fields = Vec::with_capacity(fields.len());
                for field in fields {
                    let field_type = field
                        .declared_type
                        .clone()
                        .or_else(|| Self::infer_expr_type(&field.expr))?;
                    struct_fields.push(StructField {
                        name: field.name.clone(),
                        data_type: field_type,
                    });
                }
                Some(DataType::Struct(struct_fields))
            }
            Expr::StructFieldAccess { expr, field } => match Self::infer_expr_type(expr)? {
                DataType::Struct(fields) => fields
                    .into_iter()
                    .find(|f| f.name == *field)
                    .map(|f| f.data_type),
                _ => None,
            },
            Expr::ArrayIndex { array, .. } => match Self::infer_expr_type(array)? {
                DataType::Array(elem_type) => Some(*elem_type),
                DataType::Map(_, value_type) => Some(*value_type),
                _ => None,
            },
            Expr::ArraySlice { array, .. } => Self::infer_expr_type(array),
            Expr::Aggregate { name, args, .. } => {
                let empty_schema = Schema::new();
                Self::infer_function_type(name, args, &empty_schema)
            }
            Expr::Case {
                when_then,
                else_expr,
                ..
            } => {
                let mut types = Vec::new();
                for (_, then_expr) in when_then {
                    if let Some(t) = Self::infer_expr_type(then_expr) {
                        types.push(t);
                    }
                }
                if let Some(else_e) = else_expr {
                    if let Some(t) = Self::infer_expr_type(else_e) {
                        types.push(t);
                    }
                }
                if types.is_empty() {
                    None
                } else if types.len() == 1 {
                    Some(types[0].clone())
                } else {
                    yachtsql_core::types::coercion::CoercionRules::find_common_type(&types).ok()
                }
            }
            _ => None,
        }
    }
}
