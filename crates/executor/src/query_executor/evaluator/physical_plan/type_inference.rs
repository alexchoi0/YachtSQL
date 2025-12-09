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
            CastDataType::Inet => DataType::Inet,
            CastDataType::Cidr => DataType::Cidr,
            CastDataType::Int4Range => DataType::Range(yachtsql_core::types::RangeType::Int4Range),
            CastDataType::Int8Range => DataType::Range(yachtsql_core::types::RangeType::Int8Range),
            CastDataType::NumRange => DataType::Range(yachtsql_core::types::RangeType::NumRange),
            CastDataType::TsRange => DataType::Range(yachtsql_core::types::RangeType::TsRange),
            CastDataType::TsTzRange => DataType::Range(yachtsql_core::types::RangeType::TsTzRange),
            CastDataType::DateRange => DataType::Range(yachtsql_core::types::RangeType::DateRange),
            CastDataType::Point => DataType::Point,
            CastDataType::PgBox => DataType::PgBox,
            CastDataType::Circle => DataType::Circle,
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
                (Some(DataType::Point), Some(DataType::Point)) => Some(DataType::Point),
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

                (Some(DataType::Inet), Some(DataType::Int64))
                | (Some(DataType::Int64), Some(DataType::Inet))
                | (Some(DataType::Inet), Some(DataType::Inet)) => Some(DataType::Inet),

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
                (Some(DataType::Point), Some(DataType::Point)) => Some(DataType::Point),
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

                (Some(DataType::Inet), Some(DataType::Int64)) => Some(DataType::Inet),
                (Some(DataType::Inet), Some(DataType::Inet)) => Some(DataType::Int64),

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
                (Some(DataType::Point), Some(DataType::Point)) => Some(DataType::Point),
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
                (Some(DataType::Point), Some(DataType::Point)) => Some(DataType::Point),
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
                match (&left_type, &right_type) {
                    (Some(DataType::Inet), Some(DataType::Inet)) => Some(DataType::Inet),
                    _ => Some(DataType::Int64),
                }
            }

            BinaryOp::InetContains
            | BinaryOp::InetContainedBy
            | BinaryOp::InetContainsOrEqual
            | BinaryOp::InetContainedByOrEqual
            | BinaryOp::InetOverlap => Some(DataType::Bool),

            BinaryOp::ShiftLeft | BinaryOp::ShiftRight => match (&left_type, &right_type) {
                (Some(DataType::Range(_)), Some(DataType::Range(_))) => Some(DataType::Bool),
                (Some(DataType::Inet), Some(DataType::Inet)) => Some(DataType::Bool),
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
            UnaryOp::BitwiseNot => match operand_type {
                Some(DataType::Inet) => Some(DataType::Inet),
                _ => Some(DataType::Int64),
            },
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
            LiteralValue::Time(_) => Some(DataType::Time),
            LiteralValue::DateTime(_) => Some(DataType::DateTime),
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

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "GREATCIRCLEDISTANCE"
                        | "GEODISTANCE"
                        | "H3EDGELENGTHM"
                        | "H3EDGEANGLE"
                        | "H3HEXAREAKM2"
                        | "H3CELLAREAM2"
                        | "H3CELLAREARADS2"
                ) =>
            {
                Some(DataType::Float64)
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "POINTINELLIPSES"
                        | "POINTINPOLYGON"
                        | "H3ISVALID"
                        | "H3ISPENTAGON"
                        | "H3ISRESCLASSIII"
                ) =>
            {
                Some(DataType::Bool)
            }

            FunctionName::Custom(s) if s == "GEOHASHENCODE" => Some(DataType::String),

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "H3GETRESOLUTION"
                        | "GEOTOH3"
                        | "H3GETBASECELL"
                        | "H3TOPARENT"
                        | "H3DISTANCE"
                        | "LONGLATTOS2CELLID"
                ) =>
            {
                Some(DataType::Int64)
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "GEOHASHDECODE" | "H3TOGEO" | "S2CELLIDTOLONGLAT"
                ) =>
            {
                Some(DataType::Struct(vec![
                    yachtsql_core::types::StructField {
                        name: "lat".to_string(),
                        data_type: DataType::Float64,
                    },
                    yachtsql_core::types::StructField {
                        name: "lon".to_string(),
                        data_type: DataType::Float64,
                    },
                ]))
            }

            FunctionName::Custom(s) if s == "GEOHASHESINBOX" => {
                Some(DataType::Array(Box::new(DataType::String)))
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "H3KRING" | "H3TOCHILDREN" | "H3LINE" | "H3GETFACES"
                ) =>
            {
                Some(DataType::Array(Box::new(DataType::Int64)))
            }

            FunctionName::Custom(s) if s == "H3TOGEOBOUNDARY" => {
                Some(DataType::Array(Box::new(DataType::Struct(vec![
                    yachtsql_core::types::StructField {
                        name: "lat".to_string(),
                        data_type: DataType::Float64,
                    },
                    yachtsql_core::types::StructField {
                        name: "lon".to_string(),
                        data_type: DataType::Float64,
                    },
                ]))))
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "PROTOCOL"
                        | "DOMAIN"
                        | "DOMAINWITHOUTWWW"
                        | "TOPLEVELDOMAIN"
                        | "FIRSTSIGNIFICANTSUBDOMAIN"
                        | "PATH"
                        | "PATHFULL"
                        | "QUERYSTRING"
                        | "FRAGMENT"
                        | "QUERYSTRINGANDFRAGMENT"
                        | "EXTRACTURLPARAMETER"
                        | "DECODEURLCOMPONENT"
                        | "ENCODEURLCOMPONENT"
                        | "ENCODEURLFORMCOMPONENT"
                        | "DECODEURLFORMCOMPONENT"
                        | "NETLOC"
                        | "CUTTOFIRSTSIGNIFICANTSUBDOMAIN"
                        | "CUTWWW"
                        | "CUTQUERYSTRING"
                        | "CUTFRAGMENT"
                        | "CUTQUERYSTRINGANDFRAGMENT"
                        | "CUTURLPARAMETER"
                ) =>
            {
                Some(DataType::String)
            }

            FunctionName::Custom(s) if s == "PORT" => Some(DataType::Int64),

            FunctionName::Custom(s) if matches!(s.as_str(), "MASKLEN" | "FAMILY") => {
                Some(DataType::Int64)
            }

            FunctionName::Custom(s)
                if matches!(s.as_str(), "NETMASK" | "HOSTMASK" | "BROADCAST") =>
            {
                Some(DataType::Inet)
            }

            FunctionName::Custom(s) if matches!(s.as_str(), "NETWORK" | "INET_MERGE") => {
                Some(DataType::Cidr)
            }

            FunctionName::Custom(s) if matches!(s.as_str(), "HOST" | "ABBREV" | "TEXT") => {
                Some(DataType::String)
            }

            FunctionName::Custom(s) if s == "INET_SAME_FAMILY" => Some(DataType::Bool),

            FunctionName::Custom(s) if s == "SET_MASKLEN" => {
                Self::infer_first_arg_type(args, schema).or(Some(DataType::Inet))
            }

            FunctionName::Custom(s) if s == "TRUNC" => Some(DataType::MacAddr),

            FunctionName::Custom(s) if s == "MACADDR8_SET7BIT" => Some(DataType::MacAddr8),

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "EXTRACTURLPARAMETERS"
                        | "EXTRACTURLPARAMETERNAMES"
                        | "URLHIERARCHY"
                        | "URLPATHHIERARCHY"
                ) =>
            {
                Some(DataType::Array(Box::new(DataType::String)))
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "HOSTNAME"
                        | "FQDN"
                        | "VERSION"
                        | "TIMEZONE"
                        | "CURRENTDATABASE"
                        | "CURRENTUSER"
                        | "BAR"
                        | "FORMATREADABLESIZE"
                        | "FORMATREADABLEQUANTITY"
                        | "FORMATREADABLETIMEDELTA"
                        | "GETSETTING"
                ) =>
            {
                Some(DataType::String)
            }

            FunctionName::Custom(s)
                if matches!(s.as_str(), "UPTIME" | "SLEEP" | "THROWIF" | "IGNORE") =>
            {
                Some(DataType::Int64)
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "ISCONSTANT" | "ISFINITE" | "ISINFINITE" | "ISNAN" | "HASCOLUMNINTABLE"
                ) =>
            {
                Some(DataType::Bool)
            }

            FunctionName::Custom(s) if s == "MODELEVALUATE" => Some(DataType::Float64),

            FunctionName::Custom(s) if matches!(s.as_str(), "MATERIALIZE" | "IDENTITY") => {
                if !args.is_empty() {
                    Self::infer_expr_type_with_schema(&args[0], schema)
                } else {
                    None
                }
            }

            FunctionName::Custom(s) if s == "TRANSFORM" => {
                if args.len() >= 3 {
                    Self::infer_expr_type_with_schema(&args[2], schema)
                } else {
                    None
                }
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "HEX"
                        | "UNHEX"
                        | "BASE64ENCODE"
                        | "BASE64DECODE"
                        | "TRYBASE64DECODE"
                        | "BASE64URLENCODE"
                        | "BASE64URLDECODE"
                        | "TRYBASE64URLDECODE"
                        | "BASE58ENCODE"
                        | "BASE58DECODE"
                        | "BIN"
                        | "CHAR"
                ) =>
            {
                Some(DataType::String)
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "UNBIN"
                        | "BITSHIFTLEFT"
                        | "BITSHIFTRIGHT"
                        | "BITAND"
                        | "BITOR"
                        | "BITXOR"
                        | "BITNOT"
                        | "BITCOUNT"
                        | "BITTEST"
                        | "BITTESTALL"
                        | "BITTESTANY"
                        | "BITROTATELEFT"
                        | "BITROTATERIGHT"
                        | "BITHAMMINGDISTANCE"
                        | "BITSLICE"
                        | "BYTESWAP"
                ) =>
            {
                Some(DataType::Int64)
            }

            FunctionName::Custom(s) if s == "BITPOSITIONSTOARRAY" => {
                Some(DataType::Array(Box::new(DataType::Int64)))
            }

            FunctionName::Custom(s)
                if matches!(s.as_str(), "ISNULL" | "ISNOTNULL" | "ISZEROORNULL") =>
            {
                Some(DataType::Bool)
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "IPV4NUMTOSTRING"
                        | "IPV4NUMTOSTRINGCLASSC"
                        | "IPV4TOIPV6"
                        | "TOIPV4"
                        | "TOIPV6"
                        | "TOIPV4ORNULL"
                        | "TOIPV6ORNULL"
                        | "IPV4CIDRTORANGE"
                        | "IPV6CIDRTORANGE"
                        | "MACNUMTOSTRING"
                ) =>
            {
                Some(DataType::String)
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "IPV4STRINGTONUM" | "MACSTRINGTONUM" | "MACSTRINGTOOUI"
                ) =>
            {
                Some(DataType::Int64)
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "ISIPV4STRING" | "ISIPV6STRING" | "ISIPADDRESSINRANGE"
                ) =>
            {
                Some(DataType::Bool)
            }

            FunctionName::Custom(s) if matches!(s.as_str(), "IPV6NUMTOSTRING") => {
                Some(DataType::String)
            }

            FunctionName::Custom(s) if matches!(s.as_str(), "IPV6STRINGTONUM") => {
                Some(DataType::Bytes)
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "TOINT8"
                        | "TOINT16"
                        | "TOINT32"
                        | "TOINT64"
                        | "TOUINT8"
                        | "TOUINT16"
                        | "TOUINT32"
                        | "TOUINT64"
                        | "TOINT64ORNULL"
                        | "TOINT64ORZERO"
                        | "REINTERPRETASINT64"
                        | "RAND"
                        | "RAND32"
                        | "RAND64"
                        | "RANDCONSTANT"
                        | "RANDBERNOULLI"
                        | "RANDBINOMIAL"
                        | "RANDNEGATIVEBINOMIAL"
                        | "RANDPOISSON"
                ) =>
            {
                Some(DataType::Int64)
            }

            FunctionName::Custom(s) if matches!(s.as_str(), "ENCRYPT" | "AES_ENCRYPT_MYSQL") => {
                Some(DataType::Bytes)
            }

            FunctionName::Custom(s) if matches!(s.as_str(), "DECRYPT" | "AES_DECRYPT_MYSQL") => {
                Some(DataType::String)
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "TOFLOAT32"
                        | "TOFLOAT64"
                        | "TOFLOAT64ORNULL"
                        | "TOFLOAT64ORZERO"
                        | "RANDUNIFORM"
                        | "RANDNORMAL"
                        | "RANDLOGNORMAL"
                        | "RANDEXPONENTIAL"
                        | "RANDCHISQUARED"
                        | "RANDSTUDENTT"
                        | "RANDFISHERF"
                ) =>
            {
                Some(DataType::Float64)
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "TOSTRING"
                        | "TOFIXEDSTRING"
                        | "REINTERPRETASSTRING"
                        | "TOTYPENAME"
                        | "GENERATEUUIDV4"
                        | "RANDOMPRINTABLEASCII"
                        | "RANDOMSTRINGUTF8"
                        | "FAKEDATA"
                ) =>
            {
                Some(DataType::String)
            }

            FunctionName::Custom(s) if matches!(s.as_str(), "TODATE" | "TODATEORNULL") => {
                Some(DataType::Date)
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "TODATETIME"
                        | "TODATETIME64"
                        | "TODATETIMEORNULL"
                        | "PARSEDATETIME"
                        | "PARSEDATETIMEBESTEFFORT"
                        | "PARSEDATETIMEBESTEFFORTORNULL"
                ) =>
            {
                Some(DataType::Timestamp)
            }

            FunctionName::Custom(s)
                if matches!(s.as_str(), "TODECIMAL32" | "TODECIMAL64" | "TODECIMAL128") =>
            {
                Some(DataType::Numeric(None))
            }

            FunctionName::Custom(s)
                if matches!(s.as_str(), "ACCURATECAST" | "ACCURATECASTORNULL") =>
            {
                Some(DataType::Int64)
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "POSITION"
                        | "POSITIONCASEINSENSITIVE"
                        | "POSITIONUTF8"
                        | "POSITIONCASEINSENSITIVEUTF8"
                        | "LOCATE"
                        | "COUNTSUBSTRINGS"
                        | "COUNTSUBSTRINGSCASEINSENSITIVE"
                        | "MULTISEARCHFIRSTINDEX"
                        | "MULTIMATCHANYINDEX"
                        | "MULTISEARCHFIRSTPOSITION"
                        | "COUNTMATCHES"
                ) =>
            {
                Some(DataType::Int64)
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "MATCH"
                        | "MULTISEARCHANY"
                        | "MULTIMATCHANY"
                        | "HASTOKEN"
                        | "HASTOKENCASEINSENSITIVE"
                ) =>
            {
                Some(DataType::Bool)
            }

            FunctionName::Custom(s)
                if matches!(
                    s.as_str(),
                    "MULTISEARCHALLPOSITIONS" | "MULTIMATCHALLINDICES"
                ) =>
            {
                Some(DataType::Array(Box::new(DataType::Int64)))
            }

            FunctionName::Custom(s) if matches!(s.as_str(), "EXTRACTGROUPS") => {
                Some(DataType::Array(Box::new(DataType::String)))
            }

            FunctionName::Custom(s) if matches!(s.as_str(), "NGRAMDISTANCE" | "NGRAMSEARCH") => {
                Some(DataType::Float64)
            }
            FunctionName::Custom(s)
                if matches!(s.as_str(), "RANDOMSTRING" | "RANDOMFIXEDSTRING") =>
            {
                Some(DataType::Bytes)
            }

            FunctionName::BitmapBuild
            | FunctionName::BitmapToArray
            | FunctionName::BitmapAnd
            | FunctionName::BitmapOr
            | FunctionName::BitmapXor
            | FunctionName::BitmapAndnot
            | FunctionName::BitmapSubsetInRange
            | FunctionName::BitmapSubsetLimit
            | FunctionName::BitmapTransform
            | FunctionName::SubBitmap
            | FunctionName::GroupBitmapState => Some(DataType::Array(Box::new(DataType::Int64))),

            FunctionName::BitmapCardinality
            | FunctionName::BitmapContains
            | FunctionName::BitmapHasAny
            | FunctionName::BitmapHasAll
            | FunctionName::BitmapAndCardinality
            | FunctionName::BitmapOrCardinality
            | FunctionName::BitmapXorCardinality
            | FunctionName::BitmapAndnotCardinality
            | FunctionName::BitmapMin
            | FunctionName::BitmapMax => Some(DataType::Int64),

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

            FunctionName::Degrees
            | FunctionName::Radians
            | FunctionName::Gamma
            | FunctionName::Lgamma
            | FunctionName::Sinh
            | FunctionName::Cosh
            | FunctionName::Tanh
            | FunctionName::Asinh
            | FunctionName::Acosh
            | FunctionName::Atanh
            | FunctionName::Cot
            | FunctionName::Sind
            | FunctionName::Cosd
            | FunctionName::Tand
            | FunctionName::Asind
            | FunctionName::Acosd
            | FunctionName::Atand
            | FunctionName::Atan2d
            | FunctionName::Cotd
            | FunctionName::Cbrt => Some(DataType::Float64),

            FunctionName::ToNumber => {
                if args.len() == 2 {
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

            FunctionName::Factorial
            | FunctionName::Gcd
            | FunctionName::Lcm
            | FunctionName::Scale
            | FunctionName::MinScale
            | FunctionName::WidthBucket => Some(DataType::Int64),

            FunctionName::TrimScale => Some(DataType::Numeric(None)),

            FunctionName::Setseed => Some(DataType::Unknown),

            FunctionName::SafeAdd
            | FunctionName::SafeSubtract
            | FunctionName::SafeMultiply
            | FunctionName::SafeDivide => {
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

            FunctionName::SafeNegate => {
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

            FunctionName::Format
            | FunctionName::QuoteIdent
            | FunctionName::QuoteLiteral
            | FunctionName::RegexpExtract
            | FunctionName::RegexpReplace
            | FunctionName::ReplaceRegexpAll
            | FunctionName::ReplaceRegexpOne => Some(DataType::String),

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

            FunctionName::LowerInc
            | FunctionName::UpperInc
            | FunctionName::LowerInf
            | FunctionName::UpperInf
            | FunctionName::Isempty
            | FunctionName::RangeIsempty
            | FunctionName::RangeContains
            | FunctionName::RangeContainsElem
            | FunctionName::RangeOverlaps
            | FunctionName::RangeAdjacent
            | FunctionName::RangeStrictlyLeft
            | FunctionName::RangeStrictlyRight => Some(DataType::Bool),

            FunctionName::Range => {
                if !args.is_empty() {
                    match Self::infer_expr_type_with_schema(&args[0], schema) {
                        Some(DataType::Date) => {
                            Some(DataType::Range(yachtsql_core::types::RangeType::DateRange))
                        }
                        Some(DataType::Timestamp) | Some(DataType::DateTime) => {
                            Some(DataType::Range(yachtsql_core::types::RangeType::TsRange))
                        }
                        Some(DataType::Int64) => {
                            Some(DataType::Range(yachtsql_core::types::RangeType::Int8Range))
                        }
                        Some(DataType::Float64) => {
                            Some(DataType::Range(yachtsql_core::types::RangeType::NumRange))
                        }
                        _ => Some(DataType::Range(yachtsql_core::types::RangeType::DateRange)),
                    }
                } else {
                    Some(DataType::Range(yachtsql_core::types::RangeType::DateRange))
                }
            }

            FunctionName::RangeMerge
            | FunctionName::RangeUnion
            | FunctionName::RangeIntersection
            | FunctionName::RangeDifference => {
                if !args.is_empty() {
                    Self::infer_expr_type_with_schema(&args[0], schema)
                } else {
                    None
                }
            }

            FunctionName::RangeStart | FunctionName::RangeEnd => {
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
                        _ => None,
                    }
                } else {
                    None
                }
            }

            FunctionName::Length
            | FunctionName::Len
            | FunctionName::CharLength
            | FunctionName::CharacterLength
            | FunctionName::OctetLength
            | FunctionName::ByteLength
            | FunctionName::Lengthb
            | FunctionName::Position
            | FunctionName::Strpos
            | FunctionName::Instr
            | FunctionName::Locate
            | FunctionName::Ascii
            | FunctionName::Ord => Some(DataType::Int64),

            FunctionName::StartsWith | FunctionName::EndsWith | FunctionName::RegexpContains => {
                Some(DataType::Bool)
            }

            FunctionName::Md5
            | FunctionName::Md5Hash
            | FunctionName::Sha256
            | FunctionName::Sha2
            | FunctionName::Sha1
            | FunctionName::Sha224
            | FunctionName::Sha384
            | FunctionName::Sha512
            | FunctionName::Blake3 => Some(DataType::String),

            FunctionName::FarmFingerprint | FunctionName::Crc32 | FunctionName::Crc32c => {
                Some(DataType::Int64)
            }

            FunctionName::ToHex => Some(DataType::String),
            FunctionName::FromHex => Some(DataType::Bytes),

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

            FunctionName::TimestampDiff => Some(DataType::Int64),
            FunctionName::TimestampTrunc => Some(DataType::Timestamp),

            FunctionName::ParseDate => Some(DataType::Date),
            FunctionName::StrToDate | FunctionName::ParseDatetime => Some(DataType::Date),
            FunctionName::ParseTimestamp | FunctionName::AtTimeZone => Some(DataType::Timestamp),

            FunctionName::MakeDate => Some(DataType::Date),
            FunctionName::MakeTimestamp => Some(DataType::Timestamp),
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

            FunctionName::JustifyDays
            | FunctionName::JustifyHours
            | FunctionName::JustifyInterval => Some(DataType::Interval),

            FunctionName::UnixDate
            | FunctionName::UnixSeconds
            | FunctionName::UnixMillis
            | FunctionName::UnixMicros
            | FunctionName::DatetimeDiff
            | FunctionName::TimeDiff => Some(DataType::Int64),

            FunctionName::DateFromUnixDate | FunctionName::LastDay => Some(DataType::Date),

            FunctionName::TimestampSeconds
            | FunctionName::TimestampMillis
            | FunctionName::TimestampMicros
            | FunctionName::TimestampAdd
            | FunctionName::TimestampSub => Some(DataType::Timestamp),

            FunctionName::DatetimeAdd | FunctionName::DatetimeSub | FunctionName::DatetimeTrunc => {
                Some(DataType::DateTime)
            }

            FunctionName::TimeAdd | FunctionName::TimeSub | FunctionName::TimeTrunc => {
                Some(DataType::Time)
            }

            FunctionName::Year
            | FunctionName::Month
            | FunctionName::Day
            | FunctionName::Hour
            | FunctionName::Minute
            | FunctionName::Second
            | FunctionName::Quarter
            | FunctionName::Week
            | FunctionName::Dow
            | FunctionName::Doy
            | FunctionName::Dayofweek
            | FunctionName::Dayofyear => Some(DataType::Int64),

            FunctionName::FormatTimestamp | FunctionName::DateFormat | FunctionName::FormatDate => {
                Some(DataType::String)
            }

            FunctionName::Nextval
            | FunctionName::Currval
            | FunctionName::Setval
            | FunctionName::Lastval => Some(DataType::Int64),

            FunctionName::ArrayLength | FunctionName::Cardinality => Some(DataType::Int64),
            FunctionName::ArrayPosition => Some(DataType::Int64),
            FunctionName::ArrayContains | FunctionName::ArrayContainsAll => Some(DataType::Bool),
            FunctionName::Split
            | FunctionName::StringSplit
            | FunctionName::SplitByChar
            | FunctionName::SplitByString
            | FunctionName::SplitByRegexp
            | FunctionName::SplitByWhitespace
            | FunctionName::SplitByNonAlpha
            | FunctionName::AlphaTokens
            | FunctionName::ExtractAll
            | FunctionName::Ngrams
            | FunctionName::Tokens => Some(DataType::Array(Box::new(DataType::String))),
            FunctionName::SplitPart => Some(DataType::String),
            FunctionName::ArrayStringConcat => Some(DataType::String),
            FunctionName::ExtractAllGroupsHorizontal | FunctionName::ExtractAllGroupsVertical => {
                Some(DataType::Array(Box::new(DataType::Array(Box::new(
                    DataType::String,
                )))))
            }
            FunctionName::StringToArray => Some(DataType::Array(Box::new(DataType::String))),

            FunctionName::Stem
            | FunctionName::Lemmatize
            | FunctionName::DetectLanguage
            | FunctionName::DetectLanguageUnknown
            | FunctionName::DetectCharset
            | FunctionName::DetectTonality
            | FunctionName::DetectProgrammingLanguage
            | FunctionName::NormalizeQuery => Some(DataType::String),
            FunctionName::Synonyms | FunctionName::DetectLanguageMixed => {
                Some(DataType::Array(Box::new(DataType::String)))
            }
            FunctionName::NormalizedQueryHash
            | FunctionName::NgramSimHash
            | FunctionName::WordShingleSimHash => Some(DataType::Int64),
            FunctionName::WordShingleMinHash => Some(DataType::Array(Box::new(DataType::Int64))),

            FunctionName::L1Norm
            | FunctionName::L2Norm
            | FunctionName::LinfNorm
            | FunctionName::LpNorm
            | FunctionName::L1Distance
            | FunctionName::L2Distance
            | FunctionName::LinfDistance
            | FunctionName::LpDistance
            | FunctionName::CosineDistance
            | FunctionName::DotProduct
            | FunctionName::L2SquaredDistance => Some(DataType::Float64),
            FunctionName::L1Normalize
            | FunctionName::L2Normalize
            | FunctionName::LinfNormalize
            | FunctionName::LpNormalize => Some(DataType::Array(Box::new(DataType::Float64))),

            FunctionName::GenerateArray => Some(DataType::Array(Box::new(DataType::Int64))),
            FunctionName::GenerateDateArray => Some(DataType::Array(Box::new(DataType::Date))),
            FunctionName::GenerateTimestampArray => {
                Some(DataType::Array(Box::new(DataType::Timestamp)))
            }
            FunctionName::GenerateUuid
            | FunctionName::Uuid
            | FunctionName::GenRandomUuid
            | FunctionName::Newid
            | FunctionName::UuidGenerateV4
            | FunctionName::UuidGenerateV1
            | FunctionName::Uuidv4
            | FunctionName::Uuidv7 => Some(DataType::String),
            FunctionName::GenerateUuidArray => Some(DataType::Array(Box::new(DataType::String))),

            FunctionName::GenRandomBytes | FunctionName::Digest => Some(DataType::Bytes),
            FunctionName::Encode => Some(DataType::String),
            FunctionName::Decode => Some(DataType::Bytes),

            FunctionName::HstoreExists
            | FunctionName::HstoreExistsAll
            | FunctionName::HstoreExistsAny
            | FunctionName::HstoreContains
            | FunctionName::HstoreContainedBy
            | FunctionName::HstoreDefined
            | FunctionName::Defined
            | FunctionName::Exist => Some(DataType::Bool),
            FunctionName::HstoreConcat
            | FunctionName::HstoreDelete
            | FunctionName::HstoreDeleteKey
            | FunctionName::HstoreDeleteKeys
            | FunctionName::HstoreDeleteHstore
            | FunctionName::HstoreSlice
            | FunctionName::Slice
            | FunctionName::Delete
            | FunctionName::Hstore => Some(DataType::Hstore),
            FunctionName::HstoreAkeys | FunctionName::Akeys => {
                Some(DataType::Array(Box::new(DataType::String)))
            }
            FunctionName::Skeys | FunctionName::Svals => Some(DataType::String),
            FunctionName::HstoreAvals | FunctionName::Avals => {
                Some(DataType::Array(Box::new(DataType::String)))
            }
            FunctionName::HstoreToJson | FunctionName::HstoreToJsonb => Some(DataType::Json),
            FunctionName::HstoreToArray => Some(DataType::Array(Box::new(DataType::String))),
            FunctionName::HstoreToMatrix => Some(DataType::Array(Box::new(DataType::Array(
                Box::new(DataType::String),
            )))),
            FunctionName::HstoreGet => Some(DataType::String),
            FunctionName::HstoreGetValues => Some(DataType::Array(Box::new(DataType::String))),

            FunctionName::ArrayReverse
            | FunctionName::ArraySort
            | FunctionName::ArrayDistinct
            | FunctionName::ArrayReplace => Self::infer_array_type_from_first_arg(args, schema),

            FunctionName::ArrayAppend | FunctionName::ArrayRemove => {
                Self::infer_array_type_with_element_fallback(0, 1, args, schema)
            }

            FunctionName::ArrayPrepend => {
                Self::infer_array_type_with_element_fallback(1, 0, args, schema)
            }

            FunctionName::ArrayConcat | FunctionName::ArrayCat => {
                Self::infer_array_type_from_non_empty_array(args, schema)
            }

            FunctionName::Arraymap
            | FunctionName::Arrayfilter
            | FunctionName::Arraysort
            | FunctionName::Arrayreversesort => {
                if args.len() >= 2 {
                    Self::infer_expr_type_with_schema(&args[1], schema)
                } else {
                    None
                }
            }

            FunctionName::Arrayexists
            | FunctionName::Arrayall
            | FunctionName::Arraycount
            | FunctionName::Arrayfirstindex
            | FunctionName::Arraylastindex => Some(DataType::Int64),

            FunctionName::Arrayfirst | FunctionName::Arraylast => {
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

            FunctionName::Arraysum | FunctionName::Arraymin | FunctionName::Arraymax => {
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

            FunctionName::Arrayavg => Some(DataType::Float64),

            FunctionName::Arrayfold => {
                if args.len() >= 3 {
                    Self::infer_expr_type_with_schema(&args[2], schema)
                } else {
                    None
                }
            }

            FunctionName::Arrayreduce => {
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

            FunctionName::Arrayreduceinranges => Some(DataType::Array(Box::new(DataType::Unknown))),

            FunctionName::Arraycumsum
            | FunctionName::Arraycumsumnonnegative
            | FunctionName::Arraydifference => Some(DataType::Array(Box::new(DataType::Int64))),

            FunctionName::Arraysplit | FunctionName::Arrayreversesplit => {
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

            FunctionName::Arraycompact => {
                if !args.is_empty() {
                    Self::infer_expr_type_with_schema(&args[0], schema)
                } else {
                    None
                }
            }

            FunctionName::Arrayzip => Some(DataType::Array(Box::new(DataType::Struct(vec![])))),

            FunctionName::Arrayauc => Some(DataType::Float64),

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

            FunctionName::Coalesce
            | FunctionName::Ifnull
            | FunctionName::Nvl
            | FunctionName::Isnull => Self::infer_coalesce_type(args, schema),
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

            FunctionName::JsonAgg
            | FunctionName::JsonbAgg
            | FunctionName::JsonObjectAgg
            | FunctionName::JsonbObjectAgg => Some(DataType::Json),

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
            FunctionName::JsonExtractJson => {
                if let Some(first_arg) = args.first() {
                    if let Some(arg_type) = Self::infer_expr_type_with_schema(first_arg, schema) {
                        if matches!(arg_type, DataType::Hstore) {
                            return Some(DataType::String);
                        }
                    }
                }
                Some(DataType::Json)
            }
            FunctionName::JsonArray
            | FunctionName::JsonObject
            | FunctionName::ParseJson
            | FunctionName::ToJson
            | FunctionName::ToJsonb
            | FunctionName::JsonBuildArray
            | FunctionName::JsonbBuildArray
            | FunctionName::JsonBuildObject
            | FunctionName::JsonbBuildObject
            | FunctionName::JsonStripNulls
            | FunctionName::JsonbStripNulls
            | FunctionName::JsonbInsert => Some(DataType::Json),

            FunctionName::ToJsonString | FunctionName::JsonbPretty => Some(DataType::String),
            FunctionName::JsonObjectKeys | FunctionName::JsonbObjectKeys => Some(DataType::String),

            FunctionName::JsonQuery => Some(DataType::Json),
            FunctionName::JsonType | FunctionName::JsonTypeof => Some(DataType::String),
            FunctionName::JsonExists => Some(DataType::Bool),
            FunctionName::JsonbPathExists | FunctionName::JsonbPathMatch => Some(DataType::Bool),
            FunctionName::JsonbPathQueryFirst => Some(DataType::Json),
            FunctionName::JsonbContains => Some(DataType::Bool),
            FunctionName::JsonbConcat
            | FunctionName::JsonbDelete
            | FunctionName::JsonbDeletePath
            | FunctionName::JsonbSet => Some(DataType::Json),
            FunctionName::JsonLength | FunctionName::JsonArrayLength => Some(DataType::Int64),
            FunctionName::JsonKeys => Some(DataType::Json),

            FunctionName::IsJsonValue
            | FunctionName::IsJsonArray
            | FunctionName::IsJsonObject
            | FunctionName::IsJsonScalar => Some(DataType::Bool),
            FunctionName::IsNotJsonValue
            | FunctionName::IsNotJsonArray
            | FunctionName::IsNotJsonObject
            | FunctionName::IsNotJsonScalar => Some(DataType::Bool),

            FunctionName::JsonSet | FunctionName::JsonRemove => Some(DataType::Json),
            FunctionName::JsonExtractArray | FunctionName::JsonQueryArray => {
                Some(DataType::Array(Box::new(DataType::Json)))
            }
            FunctionName::JsonValueArray => Some(DataType::Array(Box::new(DataType::String))),
            FunctionName::LaxBool | FunctionName::JsonBool => Some(DataType::Bool),
            FunctionName::LaxInt64 | FunctionName::JsonInt64 => Some(DataType::Int64),
            FunctionName::LaxFloat64 | FunctionName::JsonFloat64 => Some(DataType::Float64),
            FunctionName::LaxString | FunctionName::JsonStrict => Some(DataType::String),

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
            FunctionName::JsonValueText => Some(DataType::String),
            FunctionName::JsonExtractPathArray => Some(DataType::Json),
            FunctionName::JsonExtractPathArrayText => Some(DataType::String),

            FunctionName::BitAnd | FunctionName::BitOr | FunctionName::BitXor => {
                Some(DataType::Int64)
            }

            FunctionName::BitCount | FunctionName::GetBit => Some(DataType::Int64),

            FunctionName::SetBit => Some(DataType::Bytes),

            FunctionName::PositionCaseInsensitive
            | FunctionName::PositionUtf8
            | FunctionName::PositionCaseInsensitiveUtf8
            | FunctionName::CountSubstrings
            | FunctionName::CountSubstringsCaseInsensitive
            | FunctionName::CountMatches
            | FunctionName::CountMatchesCaseInsensitive
            | FunctionName::MultiSearchFirstIndex
            | FunctionName::MultiSearchFirstPosition
            | FunctionName::MultiMatchAnyIndex => Some(DataType::Int64),

            FunctionName::HasToken
            | FunctionName::HasTokenCaseInsensitive
            | FunctionName::Match
            | FunctionName::MultiSearchAny
            | FunctionName::MultiMatchAny => Some(DataType::Int64),

            FunctionName::MultiSearchAllPositions => Some(DataType::Array(Box::new(
                DataType::Array(Box::new(DataType::Int64)),
            ))),

            FunctionName::MultiMatchAllIndices => Some(DataType::Array(Box::new(DataType::Int64))),

            FunctionName::ExtractGroups => Some(DataType::Array(Box::new(DataType::String))),

            FunctionName::NgramDistance | FunctionName::NgramSearch => Some(DataType::Float64),

            FunctionName::ReplaceOne
            | FunctionName::ReplaceAll
            | FunctionName::TrimBoth
            | FunctionName::RegexpQuoteMeta
            | FunctionName::TranslateUtf8
            | FunctionName::NormalizeUtf8Nfc
            | FunctionName::NormalizeUtf8Nfd
            | FunctionName::NormalizeUtf8Nfkc
            | FunctionName::NormalizeUtf8Nfkd => Some(DataType::String),

            FunctionName::BoolAnd | FunctionName::BoolOr | FunctionName::Every => {
                Some(DataType::Bool)
            }

            FunctionName::ApproxCountDistinct
            | FunctionName::ApproxDistinct
            | FunctionName::Ndv => Some(DataType::Int64),
            FunctionName::ApproxQuantiles => Some(DataType::Array(Box::new(DataType::Float64))),
            FunctionName::ApproxTopCount | FunctionName::ApproxTopSum => {
                Some(DataType::Array(Box::new(DataType::String)))
            }

            FunctionName::IntervalLiteral | FunctionName::IntervalParse => Some(DataType::Interval),

            FunctionName::NetIpFromString
            | FunctionName::NetSafeIpFromString
            | FunctionName::NetIpv4FromInt64
            | FunctionName::NetIpNetMask
            | FunctionName::NetIpTrunc => Some(DataType::Bytes),
            FunctionName::NetIpv4ToInt64 => Some(DataType::Int64),

            FunctionName::ToInt8
            | FunctionName::ToInt16
            | FunctionName::ToInt32
            | FunctionName::ToInt64
            | FunctionName::ToUInt8
            | FunctionName::ToUInt16
            | FunctionName::ToUInt32
            | FunctionName::ToUInt64
            | FunctionName::ToInt64OrNull
            | FunctionName::ToInt64OrZero
            | FunctionName::ReinterpretAsInt64 => Some(DataType::Int64),

            FunctionName::ToFloat32
            | FunctionName::ToFloat64
            | FunctionName::ToFloat64OrNull
            | FunctionName::ToFloat64OrZero => Some(DataType::Float64),

            FunctionName::ChToString
            | FunctionName::ToFixedString
            | FunctionName::ReinterpretAsString
            | FunctionName::ToTypeName => Some(DataType::String),

            FunctionName::ToDateOrNull => Some(DataType::Date),

            FunctionName::ChToDateTime
            | FunctionName::ToDateTime64
            | FunctionName::ToDateTimeOrNull
            | FunctionName::ChParseDateTime
            | FunctionName::ParseDateTimeBestEffort
            | FunctionName::ParseDateTimeBestEffortOrNull => Some(DataType::Timestamp),

            FunctionName::ToDecimal32 | FunctionName::ToDecimal64 | FunctionName::ToDecimal128 => {
                Some(DataType::Numeric(None))
            }

            FunctionName::AccurateCast | FunctionName::AccurateCastOrNull => {
                if args.len() >= 2 {
                    if let crate::optimizer::expr::Expr::Literal(
                        crate::optimizer::expr::LiteralValue::String(s),
                    ) = &args[1]
                    {
                        return match s.to_uppercase().as_str() {
                            "INT8" | "INT16" | "INT32" | "INT64" => Some(DataType::Int64),
                            "UINT8" | "UINT16" | "UINT32" | "UINT64" => Some(DataType::Int64),
                            "FLOAT32" | "FLOAT64" => Some(DataType::Float64),
                            "STRING" => Some(DataType::String),
                            "DATE" => Some(DataType::Date),
                            "DATETIME" | "DATETIME64" => Some(DataType::Timestamp),
                            _ => None,
                        };
                    }
                }
                None
            }

            FunctionName::NetIpToString
            | FunctionName::NetHost
            | FunctionName::NetPublicSuffix
            | FunctionName::NetRegDomain => Some(DataType::String),

            FunctionName::KeysKeysetChain
            | FunctionName::AeadEncrypt
            | FunctionName::DeterministicEncrypt => Some(DataType::Bytes),
            FunctionName::AeadDecryptBytes | FunctionName::DeterministicDecryptBytes => {
                Some(DataType::Bytes)
            }
            FunctionName::AeadDecryptString | FunctionName::DeterministicDecryptString => {
                Some(DataType::String)
            }

            FunctionName::ToTsvector
            | FunctionName::ToTsquery
            | FunctionName::PlaintoTsquery
            | FunctionName::PhrasetoTsquery
            | FunctionName::WebsearchToTsquery
            | FunctionName::TsHeadline
            | FunctionName::Strip
            | FunctionName::Setweight
            | FunctionName::TsvectorConcat
            | FunctionName::TsqueryAnd
            | FunctionName::TsqueryOr
            | FunctionName::TsqueryNot => Some(DataType::String),
            FunctionName::TsRank | FunctionName::TsRankCd => Some(DataType::Float64),
            FunctionName::TsMatch => Some(DataType::Bool),
            FunctionName::TsvectorLength => Some(DataType::Int64),

            FunctionName::Point => Some(DataType::Point),
            FunctionName::Box => Some(DataType::PgBox),
            FunctionName::Circle => Some(DataType::Circle),

            FunctionName::Area
            | FunctionName::Width
            | FunctionName::Height
            | FunctionName::Radius
            | FunctionName::Diameter
            | FunctionName::Distance => Some(DataType::Float64),

            FunctionName::Center => Some(DataType::Point),

            FunctionName::PointX | FunctionName::PointY => Some(DataType::Float64),

            FunctionName::StGeogpoint
            | FunctionName::StGeogfromtext
            | FunctionName::StGeogfromgeojson
            | FunctionName::StMakeline
            | FunctionName::StMakepolygon
            | FunctionName::StCentroid
            | FunctionName::StStartpoint
            | FunctionName::StEndpoint
            | FunctionName::StPointn
            | FunctionName::StBoundary
            | FunctionName::StBuffer
            | FunctionName::StBufferwithtolerance
            | FunctionName::StClosestpoint
            | FunctionName::StConvexhull
            | FunctionName::StDifference
            | FunctionName::StIntersection
            | FunctionName::StSimplify
            | FunctionName::StSnaptogrid
            | FunctionName::StUnion
            | FunctionName::StBoundingbox
            | FunctionName::StGeogpointfromgeohash => Some(DataType::Geography),

            FunctionName::StX
            | FunctionName::StY
            | FunctionName::StDistance
            | FunctionName::StLength
            | FunctionName::StArea
            | FunctionName::StPerimeter
            | FunctionName::StMaxdistance
            | FunctionName::StAzimuth => Some(DataType::Float64),

            FunctionName::StContains
            | FunctionName::StCovers
            | FunctionName::StCoveredby
            | FunctionName::StDisjoint
            | FunctionName::StDwithin
            | FunctionName::StEquals
            | FunctionName::StIntersects
            | FunctionName::StTouches
            | FunctionName::StWithin
            | FunctionName::StIsempty
            | FunctionName::StIsclosed
            | FunctionName::StIscollection => Some(DataType::Bool),

            FunctionName::StNumpoints | FunctionName::StNpoints | FunctionName::StDimension => {
                Some(DataType::Int64)
            }

            FunctionName::StAstext
            | FunctionName::StAsgeojson
            | FunctionName::StGeometrytype
            | FunctionName::StGeohash => Some(DataType::String),

            FunctionName::StAsbinary => Some(DataType::Bytes),

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

            FunctionName::CurrentDatabase
            | FunctionName::CurrentUser
            | FunctionName::Version
            | FunctionName::Timezone
            | FunctionName::ServerTimezone
            | FunctionName::HostName
            | FunctionName::Fqdn
            | FunctionName::DumpColumnStructure
            | FunctionName::QueryId
            | FunctionName::InitialQueryId
            | FunctionName::ServerUuid
            | FunctionName::GetSetting
            | FunctionName::PgTypeof
            | FunctionName::SessionUser
            | FunctionName::CurrentSchema
            | FunctionName::CurrentCatalog
            | FunctionName::CurrentSetting
            | FunctionName::SetConfig
            | FunctionName::PgSizePretty
            | FunctionName::PgCurrentSnapshot
            | FunctionName::PgGetViewdef
            | FunctionName::ObjDescription
            | FunctionName::ColDescription
            | FunctionName::ShobjDescription
            | FunctionName::InetClientAddr
            | FunctionName::InetServerAddr => Some(DataType::String),

            FunctionName::Uptime
            | FunctionName::BlockNumber
            | FunctionName::RowNumberInBlock
            | FunctionName::RowNumberInAllBlocks
            | FunctionName::BlockSize
            | FunctionName::CountDigits
            | FunctionName::PgBackendPid
            | FunctionName::PgColumnSize
            | FunctionName::PgDatabaseSize
            | FunctionName::PgTableSize
            | FunctionName::PgIndexesSize
            | FunctionName::PgTotalRelationSize
            | FunctionName::PgRelationSize
            | FunctionName::PgTablespaceSize
            | FunctionName::InetClientPort
            | FunctionName::InetServerPort
            | FunctionName::TxidCurrent
            | FunctionName::TxidSnapshotXmin
            | FunctionName::TxidSnapshotXmax
            | FunctionName::PgCurrentXactId
            | FunctionName::PgSnapshotXmin
            | FunctionName::PgSnapshotXmax => Some(DataType::Int64),

            FunctionName::IsFinite
            | FunctionName::IsInfinite
            | FunctionName::IsNan
            | FunctionName::IsDecimalOverflow
            | FunctionName::PgIsInRecovery
            | FunctionName::HasTablePrivilege
            | FunctionName::HasSchemaPrivilege
            | FunctionName::HasDatabasePrivilege
            | FunctionName::HasColumnPrivilege
            | FunctionName::TxidVisibleInSnapshot
            | FunctionName::PgVisibleInSnapshot => Some(DataType::Bool),

            FunctionName::TxidCurrentSnapshot
            | FunctionName::TxidStatus
            | FunctionName::PgXactStatus => Some(DataType::String),

            FunctionName::TxidSnapshotXip | FunctionName::PgSnapshotXip => {
                Some(DataType::Array(Box::new(DataType::Int64)))
            }

            FunctionName::TxidCurrentIfAssigned | FunctionName::PgCurrentXactIdIfAssigned => None,

            FunctionName::PgConfLoadTime | FunctionName::PgPostmasterStartTime => {
                Some(DataType::Timestamp)
            }

            FunctionName::DefaultValueOfArgumentType => {
                if !args.is_empty() {
                    Self::infer_expr_type_with_schema(&args[0], schema)
                } else {
                    None
                }
            }

            FunctionName::DefaultValueOfTypeName => None,

            FunctionName::CurrentSchemas => Some(DataType::Array(Box::new(DataType::String))),

            // Tuple functions
            FunctionName::Tuple => {
                let mut struct_fields = Vec::with_capacity(args.len());
                for (i, arg) in args.iter().enumerate() {
                    let field_type = Self::infer_expr_type_with_schema(arg, schema)
                        .unwrap_or(yachtsql_core::types::DataType::Unknown);
                    struct_fields.push(yachtsql_core::types::StructField {
                        name: (i + 1).to_string(),
                        data_type: field_type,
                    });
                }
                Some(DataType::Struct(struct_fields))
            }

            FunctionName::Untuple
            | FunctionName::TuplePlus
            | FunctionName::TupleMinus
            | FunctionName::TupleMultiply
            | FunctionName::TupleDivide
            | FunctionName::TupleNegate
            | FunctionName::TupleMultiplyByNumber
            | FunctionName::TupleDivideByNumber
            | FunctionName::TupleConcat
            | FunctionName::TupleIntDiv
            | FunctionName::TupleIntDivOrZero
            | FunctionName::TupleModulo
            | FunctionName::TupleModuloByNumber => Some(DataType::Struct(vec![])),

            FunctionName::TupleElement => {
                if args.len() >= 2 {
                    let tuple_type = Self::infer_expr_type_with_schema(&args[0], schema);
                    if let Some(DataType::Struct(fields)) = tuple_type {
                        if let yachtsql_optimizer::expr::Expr::Literal(
                            yachtsql_optimizer::expr::LiteralValue::Int64(idx),
                        ) = &args[1]
                        {
                            let idx = *idx as usize;
                            if idx > 0 && idx <= fields.len() {
                                return Some(fields[idx - 1].data_type.clone());
                            }
                        }
                        if !fields.is_empty() {
                            return Some(fields[0].data_type.clone());
                        }
                    }
                }
                None
            }

            FunctionName::TupleHammingDistance => Some(DataType::Int64),

            FunctionName::TupleToNameValuePairs => {
                Some(DataType::Array(Box::new(DataType::Struct(vec![]))))
            }

            FunctionName::TupleNames => Some(DataType::Array(Box::new(DataType::String))),

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
            Expr::Grouping { .. } | Expr::GroupingId { .. } => Some(DataType::Int64),
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
            Expr::Tuple(exprs) => {
                let mut struct_fields = Vec::with_capacity(exprs.len());
                for (i, expr) in exprs.iter().enumerate() {
                    let field_type = Self::infer_expr_type_with_schema(expr, schema)
                        .unwrap_or(DataType::Unknown);
                    struct_fields.push(StructField {
                        name: (i + 1).to_string(),
                        data_type: field_type,
                    });
                }
                Some(DataType::Struct(struct_fields))
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
            Expr::Grouping { .. } | Expr::GroupingId { .. } => Some(DataType::Int64),
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
