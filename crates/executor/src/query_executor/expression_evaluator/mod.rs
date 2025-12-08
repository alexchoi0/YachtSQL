use std::collections::HashMap;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use regex::Regex;
use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, UnaryOperator};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};
use yachtsql_storage::{Row, Schema, TypeRegistry};

const CRC32_TABLE: [u32; 256] = [
    0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA, 0x076DC419, 0x706AF48F, 0xE963A535, 0x9E6495A3,
    0x0EDB8832, 0x79DCB8A4, 0xE0D5E91E, 0x97D2D988, 0x09B64C2B, 0x7EB17CBD, 0xE7B82D07, 0x90BF1D91,
    0x1DB71064, 0x6AB020F2, 0xF3B97148, 0x84BE41DE, 0x1ADAD47D, 0x6DDDE4EB, 0xF4D4B551, 0x83D385C7,
    0x136C9856, 0x646BA8C0, 0xFD62F97A, 0x8A65C9EC, 0x14015C4F, 0x63066CD9, 0xFA0F3D63, 0x8D080DF5,
    0x3B6E20C8, 0x4C69105E, 0xD56041E4, 0xA2677172, 0x3C03E4D1, 0x4B04D447, 0xD20D85FD, 0xA50AB56B,
    0x35B5A8FA, 0x42B2986C, 0xDBBBC9D6, 0xACBCF940, 0x32D86CE3, 0x45DF5C75, 0xDCD60DCF, 0xABD13D59,
    0x26D930AC, 0x51DE003A, 0xC8D75180, 0xBFD06116, 0x21B4F4B5, 0x56B3C423, 0xCFBA9599, 0xB8BDA50F,
    0x2802B89E, 0x5F058808, 0xC60CD9B2, 0xB10BE924, 0x2F6F7C87, 0x58684C11, 0xC1611DAB, 0xB6662D3D,
    0x76DC4190, 0x01DB7106, 0x98D220BC, 0xEFD5102A, 0x71B18589, 0x06B6B51F, 0x9FBFE4A5, 0xE8B8D433,
    0x7807C9A2, 0x0F00F934, 0x9609A88E, 0xE10E9818, 0x7F6A0DBB, 0x086D3D2D, 0x91646C97, 0xE6635C01,
    0x6B6B51F4, 0x1C6C6162, 0x856530D8, 0xF262004E, 0x6C0695ED, 0x1B01A57B, 0x8208F4C1, 0xF50FC457,
    0x65B0D9C6, 0x12B7E950, 0x8BBEB8EA, 0xFCB9887C, 0x62DD1DDF, 0x15DA2D49, 0x8CD37CF3, 0xFBD44C65,
    0x4DB26158, 0x3AB551CE, 0xA3BC0074, 0xD4BB30E2, 0x4ADFA541, 0x3DD895D7, 0xA4D1C46D, 0xD3D6F4FB,
    0x4369E96A, 0x346ED9FC, 0xAD678846, 0xDA60B8D0, 0x44042D73, 0x33031DE5, 0xAA0A4C5F, 0xDD0D7CC9,
    0x5005713C, 0x270241AA, 0xBE0B1010, 0xC90C2086, 0x5768B525, 0x206F85B3, 0xB966D409, 0xCE61E49F,
    0x5EDEF90E, 0x29D9C998, 0xB0D09822, 0xC7D7A8B4, 0x59B33D17, 0x2EB40D81, 0xB7BD5C3B, 0xC0BA6CAD,
    0xEDB88320, 0x9ABFB3B6, 0x03B6E20C, 0x74B1D29A, 0xEAD54739, 0x9DD277AF, 0x04DB2615, 0x73DC1683,
    0xE3630B12, 0x94643B84, 0x0D6D6A3E, 0x7A6A5AA8, 0xE40ECF0B, 0x9309FF9D, 0x0A00AE27, 0x7D079EB1,
    0xF00F9344, 0x8708A3D2, 0x1E01F268, 0x6906C2FE, 0xF762575D, 0x806567CB, 0x196C3671, 0x6E6B06E7,
    0xFED41B76, 0x89D32BE0, 0x10DA7A5A, 0x67DD4ACC, 0xF9B9DF6F, 0x8EBEEFF9, 0x17B7BE43, 0x60B08ED5,
    0xD6D6A3E8, 0xA1D1937E, 0x38D8C2C4, 0x4FDFF252, 0xD1BB67F1, 0xA6BC5767, 0x3FB506DD, 0x48B2364B,
    0xD80D2BDA, 0xAF0A1B4C, 0x36034AF6, 0x41047A60, 0xDF60EFC3, 0xA867DF55, 0x316E8EEF, 0x4669BE79,
    0xCB61B38C, 0xBC66831A, 0x256FD2A0, 0x5268E236, 0xCC0C7795, 0xBB0B4703, 0x220216B9, 0x5505262F,
    0xC5BA3BBE, 0xB2BD0B28, 0x2BB45A92, 0x5CB36A04, 0xC2D7FFA7, 0xB5D0CF31, 0x2CD99E8B, 0x5BDEAE1D,
    0x9B64C2B0, 0xEC63F226, 0x756AA39C, 0x026D930A, 0x9C0906A9, 0xEB0E363F, 0x72076785, 0x05005713,
    0x95BF4A82, 0xE2B87A14, 0x7BB12BAE, 0x0CB61B38, 0x92D28E9B, 0xE5D5BE0D, 0x7CDCEFB7, 0x0BDBDF21,
    0x86D3D2D4, 0xF1D4E242, 0x68DDB3F8, 0x1FDA836E, 0x81BE16CD, 0xF6B9265B, 0x6FB077E1, 0x18B74777,
    0x88085AE6, 0xFF0F6A70, 0x66063BCA, 0x11010B5C, 0x8F659EFF, 0xF862AE69, 0x616BFFD3, 0x166CCF45,
    0xA00AE278, 0xD70DD2EE, 0x4E048354, 0x3903B3C2, 0xA7672661, 0xD06016F7, 0x4969474D, 0x3E6E77DB,
    0xAED16A4A, 0xD9D65ADC, 0x40DF0B66, 0x37D83BF0, 0xA9BCAE53, 0xDEBB9EC5, 0x47B2CF7F, 0x30B5FFE9,
    0xBDBDF21C, 0xCABAC28A, 0x53B39330, 0x24B4A3A6, 0xBAD03605, 0xCDD706B3, 0x54DE5729, 0x23D967BF,
    0xB3667A2E, 0xC4614AB8, 0x5D681B02, 0x2A6F2B94, 0xB40BBE37, 0xC30C8EA1, 0x5A05DF1B, 0x2D02EF8D,
];

const CRC32C_TABLE: [u32; 256] = [
    0x00000000, 0xF26B8303, 0xE13B70F7, 0x1350F3F4, 0xC79A971F, 0x35F1141C, 0x26A1E7E8, 0xD4CA64EB,
    0x8AD958CF, 0x78B2DBCC, 0x6BE22838, 0x9989AB3B, 0x4D43CFD0, 0xBF284CD3, 0xAC78BF27, 0x5E133C24,
    0x105EC76F, 0xE235446C, 0xF165B798, 0x030E349B, 0xD7C45070, 0x25AFD373, 0x36FF2087, 0xC494A384,
    0x9A879FA0, 0x68EC1CA3, 0x7BBCEF57, 0x89D76C54, 0x5D1D08BF, 0xAF768BBC, 0xBC267848, 0x4E4DFB4B,
    0x20BD8EDE, 0xD2D60DDD, 0xC186FE29, 0x33ED7D2A, 0xE72719C1, 0x154C9AC2, 0x061C6936, 0xF477EA35,
    0xAA64D611, 0x580F5512, 0x4B5FA6E6, 0xB93425E5, 0x6DFE410E, 0x9F95C20D, 0x8CC531F9, 0x7EAEB2FA,
    0x30E349B1, 0xC288CAB2, 0xD1D83946, 0x23B3BA45, 0xF779DEAE, 0x05125DAD, 0x1642AE59, 0xE4292D5A,
    0xBA3A117E, 0x4851927D, 0x5B016189, 0xA96AE28A, 0x7DA08661, 0x8FCB0562, 0x9C9BF696, 0x6EF07595,
    0x417B1DBC, 0xB3109EBF, 0xA0406D4B, 0x522BEE48, 0x86E18AA3, 0x748A09A0, 0x67DAFA54, 0x95B17957,
    0xCBA24573, 0x39C9C670, 0x2A993584, 0xD8F2B687, 0x0C38D26C, 0xFE53516F, 0xED03A29B, 0x1F682198,
    0x5125DAD3, 0xA34E59D0, 0xB01EAA24, 0x42752927, 0x96BF4DCC, 0x64D4CECF, 0x77843D3B, 0x85EFBE38,
    0xDBFC821C, 0x2997011F, 0x3AC7F2EB, 0xC8AC71E8, 0x1C661503, 0xEE0D9600, 0xFD5D65F4, 0x0F36E6F7,
    0x61C69362, 0x93AD1061, 0x80FDE395, 0x72966096, 0xA65C047D, 0x5437877E, 0x4767748A, 0xB50CF789,
    0xEB1FCBAD, 0x197448AE, 0x0A24BB5A, 0xF84F3859, 0x2C855CB2, 0xDEEEDFB1, 0xCDBE2C45, 0x3FD5AF46,
    0x7198540D, 0x83F3D70E, 0x90A324FA, 0x62C8A7F9, 0xB602C312, 0x44694011, 0x5739B3E5, 0xA55230E6,
    0xFB410CC2, 0x092A8FC1, 0x1A7A7C35, 0xE811FF36, 0x3CDB9BDD, 0xCEB018DE, 0xDDE0EB2A, 0x2F8B6829,
    0x82F63B78, 0x709DB87B, 0x63CD4B8F, 0x91A6C88C, 0x456CAC67, 0xB7072F64, 0xA457DC90, 0x563C5F93,
    0x082F63B7, 0xFA44E0B4, 0xE9141340, 0x1B7F9043, 0xCFB5F4A8, 0x3DDE77AB, 0x2E8E845F, 0xDCE5075C,
    0x92A8FC17, 0x60C37F14, 0x73938CE0, 0x81F80FE3, 0x55326B08, 0xA759E80B, 0xB4091BFF, 0x466298FC,
    0x1871A4D8, 0xEA1A27DB, 0xF94AD42F, 0x0B21572C, 0xDFEB33C7, 0x2D80B0C4, 0x3ED04330, 0xCCBBC033,
    0xA24BB5A6, 0x502036A5, 0x4370C551, 0xB11B4652, 0x65D122B9, 0x97BAA1BA, 0x84EA524E, 0x7681D14D,
    0x2892ED69, 0xDAF96E6A, 0xC9A99D9E, 0x3BC21E9D, 0xEF087A76, 0x1D63F975, 0x0E330A81, 0xFC588982,
    0xB21572C9, 0x407EF1CA, 0x532E023E, 0xA145813D, 0x758FE5D6, 0x87E466D5, 0x94B49521, 0x66DF1622,
    0x38CC2A06, 0xCAA7A905, 0xD9F75AF1, 0x2B9CD9F2, 0xFF56BD19, 0x0D3D3E1A, 0x1E6DCDEE, 0xEC064EED,
    0xC38D26C4, 0x31E6A5C7, 0x22B65633, 0xD0DDD530, 0x0417B1DB, 0xF67C32D8, 0xE52CC12C, 0x1747422F,
    0x49547E0B, 0xBB3FFD08, 0xA86F0EFC, 0x5A048DFF, 0x8ECEE914, 0x7CA56A17, 0x6FF599E3, 0x9D9E1AE0,
    0xD3D3E1AB, 0x21B862A8, 0x32E8915C, 0xC083125F, 0x144976B4, 0xE622F5B7, 0xF5720643, 0x07198540,
    0x590AB964, 0xAB613A67, 0xB831C993, 0x4A5A4A90, 0x9E902E7B, 0x6CFBAD78, 0x7FAB5E8C, 0x8DC0DD8F,
    0xE330A81A, 0x115B2B19, 0x020BD8ED, 0xF0605BEE, 0x24AA3F05, 0xD6C1BC06, 0xC5914FF2, 0x37FACCF1,
    0x69E9F0D5, 0x9B8273D6, 0x88D28022, 0x7AB90321, 0xAE7367CA, 0x5C18E4C9, 0x4F48173D, 0xBD23943E,
    0xF36E6F75, 0x0105EC76, 0x12551F82, 0xE03E9C81, 0x34F4F86A, 0xC69F7B69, 0xD5CF889D, 0x27A40B9E,
    0x79B737BA, 0x8BDCB4B9, 0x988C474D, 0x6AE7C44E, 0xBE2DA0A5, 0x4C4623A6, 0x5F16D052, 0xAD7D5351,
];

pub struct ExpressionEvaluator<'a> {
    schema: &'a Schema,
    left_schema: Option<&'a Schema>,
    right_schema: Option<&'a Schema>,
    left_col_count: Option<usize>,
    table_map: HashMap<String, TableSide>,
    type_registry: Option<&'a TypeRegistry>,
    owned_type_registry: Option<TypeRegistry>,
    dialect: crate::DialectType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableSide {
    Left,
    Right,
}

impl<'a> ExpressionEvaluator<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            left_schema: None,
            right_schema: None,
            left_col_count: None,
            table_map: HashMap::new(),
            type_registry: None,
            owned_type_registry: None,
            dialect: crate::DialectType::BigQuery,
        }
    }

    pub fn with_dialect(mut self, dialect: crate::DialectType) -> Self {
        self.dialect = dialect;
        self
    }

    pub fn with_type_registry(mut self, registry: &'a TypeRegistry) -> Self {
        self.type_registry = Some(registry);
        self
    }

    pub fn with_owned_type_registry(mut self, registry: TypeRegistry) -> Self {
        self.owned_type_registry = Some(registry);
        self
    }

    pub fn new_for_join(
        combined_schema: &'a Schema,
        left_schema: &'a Schema,
        right_schema: &'a Schema,
        left_table_name: &str,
        right_table_name: &str,
    ) -> Self {
        let left_col_count = left_schema.fields().len();
        let mut table_map = HashMap::new();
        table_map.insert(left_table_name.to_string(), TableSide::Left);
        table_map.insert(right_table_name.to_string(), TableSide::Right);

        Self {
            schema: combined_schema,
            left_schema: Some(left_schema),
            right_schema: Some(right_schema),
            left_col_count: Some(left_col_count),
            table_map,
            type_registry: None,
            owned_type_registry: None,
            dialect: crate::DialectType::BigQuery,
        }
    }

    pub fn new_for_multi_table_delete(
        combined_schema: &'a Schema,
        table_schemas: &[Schema],
        table_names: &[String],
    ) -> Self {
        let mut table_map = HashMap::new();
        for (i, name) in table_names.iter().enumerate() {
            let side = if i == 0 {
                TableSide::Left
            } else {
                TableSide::Right
            };
            table_map.insert(name.clone(), side);
        }

        if table_schemas.len() >= 2 {
            let left_col_count = table_schemas[0].fields().len();
            Self {
                schema: combined_schema,
                left_schema: None,
                right_schema: None,
                left_col_count: Some(left_col_count),
                table_map,
                type_registry: None,
                owned_type_registry: None,
                dialect: crate::DialectType::BigQuery,
            }
        } else {
            Self {
                schema: combined_schema,
                left_schema: None,
                right_schema: None,
                left_col_count: None,
                table_map,
                type_registry: None,
                owned_type_registry: None,
                dialect: crate::DialectType::BigQuery,
            }
        }
    }

    pub fn evaluate_where(&self, expr: &SqlExpr, row: &Row) -> Result<bool> {
        match expr {
            SqlExpr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And | BinaryOperator::Or => {
                    let value = self.evaluate_condition_expr(expr, row)?;
                    self.value_to_bool(&value)
                }

                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo
                | BinaryOperator::StringConcat
                | BinaryOperator::Gt
                | BinaryOperator::Lt
                | BinaryOperator::GtEq
                | BinaryOperator::LtEq
                | BinaryOperator::Spaceship
                | BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Xor
                | BinaryOperator::BitwiseOr
                | BinaryOperator::BitwiseAnd
                | BinaryOperator::BitwiseXor
                | BinaryOperator::DuckIntegerDivide
                | BinaryOperator::MyIntegerDivide
                | BinaryOperator::Match
                | BinaryOperator::Regexp
                | BinaryOperator::Custom(_)
                | BinaryOperator::PGBitwiseXor
                | BinaryOperator::PGBitwiseShiftLeft
                | BinaryOperator::PGBitwiseShiftRight
                | BinaryOperator::PGExp
                | BinaryOperator::PGOverlap
                | BinaryOperator::PGRegexMatch
                | BinaryOperator::PGRegexIMatch
                | BinaryOperator::PGRegexNotMatch
                | BinaryOperator::PGRegexNotIMatch
                | BinaryOperator::PGLikeMatch
                | BinaryOperator::PGILikeMatch
                | BinaryOperator::PGNotLikeMatch
                | BinaryOperator::PGNotILikeMatch
                | BinaryOperator::PGStartsWith
                | BinaryOperator::Arrow
                | BinaryOperator::LongArrow
                | BinaryOperator::HashArrow
                | BinaryOperator::HashLongArrow
                | BinaryOperator::AtAt
                | BinaryOperator::AtArrow
                | BinaryOperator::ArrowAt
                | BinaryOperator::HashMinus
                | BinaryOperator::AtQuestion
                | BinaryOperator::Question
                | BinaryOperator::QuestionAnd
                | BinaryOperator::QuestionPipe
                | BinaryOperator::PGCustomBinaryOperator(_)
                | BinaryOperator::Overlaps
                | BinaryOperator::DoubleHash
                | BinaryOperator::LtDashGt
                | BinaryOperator::AndLt
                | BinaryOperator::AndGt
                | BinaryOperator::LtLtPipe
                | BinaryOperator::PipeGtGt
                | BinaryOperator::AndLtPipe
                | BinaryOperator::PipeAndGt
                | BinaryOperator::LtCaret
                | BinaryOperator::GtCaret
                | BinaryOperator::QuestionHash
                | BinaryOperator::QuestionDash
                | BinaryOperator::QuestionDashPipe
                | BinaryOperator::QuestionDoublePipe
                | BinaryOperator::At
                | BinaryOperator::TildeEq
                | BinaryOperator::Assignment => self.evaluate_binary_op(left, op, right, row),
            },

            SqlExpr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => {
                    let value = self.evaluate_condition_expr(expr, row)?;
                    if value.is_null() {
                        Ok(false)
                    } else if let Some(b) = value.as_bool() {
                        Ok(!b)
                    } else {
                        Err(Error::TypeMismatch {
                            expected: "BOOL".to_string(),
                            actual: format!("{:?}", value.data_type()),
                        })
                    }
                }
                UnaryOperator::Plus
                | UnaryOperator::Minus
                | UnaryOperator::PGBitwiseNot
                | UnaryOperator::PGSquareRoot
                | UnaryOperator::PGCubeRoot
                | UnaryOperator::PGPostfixFactorial
                | UnaryOperator::PGPrefixFactorial
                | UnaryOperator::PGAbs
                | UnaryOperator::BangNot
                | UnaryOperator::AtDashAt
                | UnaryOperator::DoubleAt
                | UnaryOperator::Hash
                | UnaryOperator::QuestionDash
                | UnaryOperator::QuestionPipe => Err(Error::UnsupportedFeature(format!(
                    "Unary operator {:?} not supported in WHERE",
                    op
                ))),
            },

            SqlExpr::IsNull(expr) => {
                let value = self.evaluate_expr(expr, row)?;
                Ok(value.is_null())
            }

            SqlExpr::IsNotNull(expr) => {
                let value = self.evaluate_expr(expr, row)?;
                Ok(!value.is_null())
            }

            SqlExpr::Identifier(_) => {
                let value = self.evaluate_expr(expr, row)?;
                self.value_to_bool(&value)
            }

            SqlExpr::Value(_) => {
                let value = self.evaluate_expr(expr, row)?;
                self.value_to_bool(&value)
            }

            SqlExpr::Nested(inner) => self.evaluate_where(inner, row),

            SqlExpr::Function(_) => {
                let value = self.evaluate_expr(expr, row)?;
                self.value_to_bool(&value)
            }

            SqlExpr::Position { expr, r#in } => {
                let substring = self.evaluate_expr(expr, row)?;
                let string = self.evaluate_expr(r#in, row)?;

                if substring.is_null() || string.is_null() {
                    return Ok(false);
                }

                match (substring.as_str(), string.as_str()) {
                    (Some(needle), Some(haystack)) => Ok(haystack.contains(needle)),
                    _ => Ok(false),
                }
            }

            SqlExpr::Like { .. } => {
                let value = self.evaluate_condition_expr(expr, row)?;
                self.value_to_bool(&value)
            }

            SqlExpr::ILike { .. } => {
                let value = self.evaluate_condition_expr(expr, row)?;
                self.value_to_bool(&value)
            }

            SqlExpr::SimilarTo { .. } => {
                let value = self.evaluate_condition_expr(expr, row)?;
                self.value_to_bool(&value)
            }

            SqlExpr::InList { .. } => {
                let value = self.evaluate_condition_expr(expr, row)?;
                self.value_to_bool(&value)
            }

            SqlExpr::Between { .. } => {
                let value = self.evaluate_condition_expr(expr, row)?;
                self.value_to_bool(&value)
            }

            SqlExpr::AnyOp { .. } | SqlExpr::AllOp { .. } => {
                let value = self.evaluate_condition_expr(expr, row)?;
                self.value_to_bool(&value)
            }

            _ => Err(Error::UnsupportedFeature(format!(
                "Expression {:?} not supported in WHERE clause",
                expr
            ))),
        }
    }

    pub fn evaluate_condition_expr(&self, expr: &SqlExpr, row: &Row) -> Result<Value> {
        use sqlparser::ast::{Expr as SqlExpr, UnaryOperator};

        match expr {
            SqlExpr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => {
                    let left_val = self.evaluate_condition_expr(left, row)?;
                    let right_val = self.evaluate_condition_expr(right, row)?;
                    self.combine_logical_and(left_val, right_val)
                }
                BinaryOperator::Or => {
                    let left_val = self.evaluate_condition_expr(left, row)?;
                    let right_val = self.evaluate_condition_expr(right, row)?;
                    self.combine_logical_or(left_val, right_val)
                }
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq => {
                    let left_val = self.evaluate_expr(left, row)?;
                    let right_val = self.evaluate_expr(right, row)?;
                    if left_val.is_null() || right_val.is_null() {
                        Ok(Value::null())
                    } else {
                        let enum_labels = self
                            .get_enum_labels_for_expr(left)
                            .or_else(|| self.get_enum_labels_for_expr(right));
                        match self.evaluate_comparison_with_enum_labels(
                            op,
                            &left_val,
                            &right_val,
                            enum_labels.as_deref(),
                        ) {
                            Ok(result) => Ok(Value::bool_val(result)),
                            Err(Error::NullComparison) => Ok(Value::null()),
                            Err(e) => Err(e),
                        }
                    }
                }
                _ => self.evaluate_expr(expr, row),
            },
            SqlExpr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => {
                    let value = self.evaluate_condition_expr(expr, row)?;
                    if let Some(b) = value.as_bool() {
                        Ok(Value::bool_val(!b))
                    } else if value.is_null() {
                        Ok(Value::null())
                    } else {
                        Err(Self::invalid_condition_value(&value))
                    }
                }
                _ => self.evaluate_expr(expr, row),
            },
            SqlExpr::IsNull(inner) => {
                let value = self.evaluate_expr(inner, row)?;
                Ok(Value::bool_val(value.is_null()))
            }
            SqlExpr::IsNotNull(inner) => {
                let value = self.evaluate_expr(inner, row)?;
                Ok(Value::bool_val(!value.is_null()))
            }
            SqlExpr::Nested(inner) => self.evaluate_condition_expr(inner, row),
            SqlExpr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let value = self.evaluate_expr(expr, row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let low_val = self.evaluate_expr(low, row)?;
                let high_val = self.evaluate_expr(high, row)?;
                if low_val.is_null() || high_val.is_null() {
                    return Ok(Value::null());
                }

                let enum_labels = self.get_enum_labels_for_expr(expr);
                let ge_low = match self.evaluate_comparison_with_enum_labels(
                    &BinaryOperator::GtEq,
                    &value,
                    &low_val,
                    enum_labels.as_deref(),
                ) {
                    Ok(result) => result,
                    Err(Error::NullComparison) => return Ok(Value::null()),
                    Err(e) => return Err(e),
                };
                let le_high = match self.evaluate_comparison_with_enum_labels(
                    &BinaryOperator::LtEq,
                    &value,
                    &high_val,
                    enum_labels.as_deref(),
                ) {
                    Ok(result) => result,
                    Err(Error::NullComparison) => return Ok(Value::null()),
                    Err(e) => return Err(e),
                };
                let result = ge_low && le_high;
                Ok(Value::bool_val(if *negated { !result } else { result }))
            }
            SqlExpr::InList {
                expr,
                list,
                negated,
                ..
            } => {
                let target = self.evaluate_expr(expr, row)?;
                if target.is_null() {
                    return Ok(Value::null());
                }
                let mut found = false;
                let mut saw_null = false;
                for item in list {
                    let value = self.evaluate_expr(item, row)?;
                    if value.is_null() {
                        saw_null = true;
                        continue;
                    }
                    match self.compare_values(&target, &value) {
                        Ok(std::cmp::Ordering::Equal) => {
                            found = true;
                            break;
                        }
                        Ok(_) => {}
                        Err(Error::NullComparison) => {
                            saw_null = true;
                        }
                        Err(e) => return Err(e),
                    }
                }
                if found {
                    Ok(Value::bool_val(!negated))
                } else if saw_null {
                    Ok(Value::null())
                } else {
                    Ok(Value::bool_val(*negated))
                }
            }
            SqlExpr::Like {
                expr,
                pattern,
                escape_char,
                negated,
                ..
            } => {
                let value = self.evaluate_expr(expr, row)?;
                let pattern_value = self.evaluate_expr(pattern, row)?;
                if value.is_null() || pattern_value.is_null() {
                    return Ok(Value::null());
                }
                let input = if let Some(s) = value.as_str() {
                    s.to_string()
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: value.data_type().to_string(),
                    });
                };
                let pattern_str = if let Some(s) = pattern_value.as_str() {
                    s.to_string()
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: pattern_value.data_type().to_string(),
                    });
                };
                let escape = match escape_char {
                    Some(sqlparser::ast::Value::SingleQuotedString(s))
                    | Some(sqlparser::ast::Value::DoubleQuotedString(s)) => {
                        let mut chars = s.chars();
                        let first = chars.next();
                        if chars.next().is_some() {
                            return Err(Error::invalid_query(
                                "LIKE escape must be a single character".to_string(),
                            ));
                        }
                        first
                    }
                    Some(other) => {
                        return Err(Error::invalid_query(format!(
                            "LIKE escape must be a single character string literal, got {:?}",
                            other
                        )));
                    }
                    None => None,
                };
                let regex = compile_like_pattern(&pattern_str, escape)?;
                let matches = regex.is_match(&input);
                Ok(Value::bool_val(if *negated { !matches } else { matches }))
            }
            SqlExpr::ILike {
                expr,
                pattern,
                escape_char,
                negated,
                ..
            } => {
                let value = self.evaluate_expr(expr, row)?;
                let pattern_value = self.evaluate_expr(pattern, row)?;
                if value.is_null() || pattern_value.is_null() {
                    return Ok(Value::null());
                }
                let input = if let Some(s) = value.as_str() {
                    s.to_string()
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: value.data_type().to_string(),
                    });
                };
                let pattern_str = if let Some(s) = pattern_value.as_str() {
                    s.to_string()
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: pattern_value.data_type().to_string(),
                    });
                };
                let escape = match escape_char {
                    Some(sqlparser::ast::Value::SingleQuotedString(s))
                    | Some(sqlparser::ast::Value::DoubleQuotedString(s)) => {
                        let mut chars = s.chars();
                        let first = chars.next();
                        if chars.next().is_some() {
                            return Err(Error::invalid_query(
                                "ILIKE escape must be a single character".to_string(),
                            ));
                        }
                        first
                    }
                    Some(other) => {
                        return Err(Error::invalid_query(format!(
                            "ILIKE escape must be a single character string literal, got {:?}",
                            other
                        )));
                    }
                    None => None,
                };
                let regex = compile_ilike_pattern(&pattern_str, escape)?;
                let matches = regex.is_match(&input);
                Ok(Value::bool_val(if *negated { !matches } else { matches }))
            }
            SqlExpr::SimilarTo {
                expr,
                pattern,
                escape_char,
                negated,
            } => {
                let value = self.evaluate_expr(expr, row)?;
                let pattern_value = self.evaluate_expr(pattern, row)?;
                if value.is_null() || pattern_value.is_null() {
                    return Ok(Value::null());
                }
                let input = if let Some(s) = value.as_str() {
                    s.to_string()
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: value.data_type().to_string(),
                    });
                };
                let pattern_str = if let Some(s) = pattern_value.as_str() {
                    s.to_string()
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: pattern_value.data_type().to_string(),
                    });
                };
                let escape = match escape_char {
                    Some(sqlparser::ast::Value::SingleQuotedString(s))
                    | Some(sqlparser::ast::Value::DoubleQuotedString(s)) => {
                        let mut chars = s.chars();
                        let first = chars.next();
                        if chars.next().is_some() {
                            return Err(Error::invalid_query(
                                "SIMILAR TO escape must be a single character".to_string(),
                            ));
                        }
                        first
                    }
                    Some(other) => {
                        return Err(Error::invalid_query(format!(
                            "SIMILAR TO escape must be a single character string literal, got {:?}",
                            other
                        )));
                    }
                    None => None,
                };
                let matches = crate::pattern_matching::matches_similar_to_with_escape(
                    &input,
                    &pattern_str,
                    escape,
                )
                .map_err(|e| Error::invalid_query(format!("Invalid SIMILAR TO pattern: {}", e)))?;
                Ok(Value::bool_val(if *negated { !matches } else { matches }))
            }
            SqlExpr::AnyOp {
                left,
                compare_op,
                right,
                ..
            } => {
                let left_value = self.evaluate_expr(left, row)?;
                if left_value.is_null() {
                    return Ok(Value::null());
                }

                let right_value = self.evaluate_expr(right, row)?;
                if right_value.is_null() {
                    return Ok(Value::null());
                }

                let array_values = match right_value.as_array() {
                    Some(arr) => arr,
                    None => {
                        return Err(Error::TypeMismatch {
                            expected: "ARRAY".to_string(),
                            actual: right_value.data_type().to_string(),
                        });
                    }
                };

                let mut saw_null = false;
                for val in array_values {
                    if val.is_null() {
                        saw_null = true;
                        continue;
                    }
                    match self.evaluate_comparison(compare_op, &left_value, val) {
                        Ok(true) => return Ok(Value::bool_val(true)),
                        Ok(false) => {}
                        Err(Error::NullComparison) => {
                            saw_null = true;
                        }
                        Err(e) => return Err(e),
                    }
                }
                if saw_null {
                    Ok(Value::null())
                } else {
                    Ok(Value::bool_val(false))
                }
            }
            SqlExpr::AllOp {
                left,
                compare_op,
                right,
            } => {
                let left_value = self.evaluate_expr(left, row)?;
                if left_value.is_null() {
                    return Ok(Value::null());
                }

                let right_value = self.evaluate_expr(right, row)?;
                if right_value.is_null() {
                    return Ok(Value::null());
                }

                let array_values = match right_value.as_array() {
                    Some(arr) => arr,
                    None => {
                        return Err(Error::TypeMismatch {
                            expected: "ARRAY".to_string(),
                            actual: right_value.data_type().to_string(),
                        });
                    }
                };

                let mut saw_null = false;
                for val in array_values {
                    if val.is_null() {
                        saw_null = true;
                        continue;
                    }
                    match self.evaluate_comparison(compare_op, &left_value, val) {
                        Ok(true) => {}
                        Ok(false) => return Ok(Value::bool_val(false)),
                        Err(Error::NullComparison) => {
                            saw_null = true;
                        }
                        Err(e) => return Err(e),
                    }
                }
                if saw_null {
                    Ok(Value::null())
                } else {
                    Ok(Value::bool_val(true))
                }
            }
            _ => self.evaluate_expr(expr, row),
        }
    }

    fn combine_logical_and(&self, left: Value, right: Value) -> Result<Value> {
        if let Some(true) = left.as_bool() {
            if right.as_bool().is_some() || right.is_null() {
                Ok(right)
            } else {
                Err(Self::invalid_condition_value(&right))
            }
        } else if let Some(false) = left.as_bool() {
            Ok(Value::bool_val(false))
        } else if left.is_null() {
            if let Some(false) = right.as_bool() {
                Ok(Value::bool_val(false))
            } else if right.as_bool() == Some(true) || right.is_null() {
                Ok(Value::null())
            } else {
                Err(Self::invalid_condition_value(&right))
            }
        } else {
            Err(Self::invalid_condition_value(&left))
        }
    }

    fn combine_logical_or(&self, left: Value, right: Value) -> Result<Value> {
        if let Some(true) = left.as_bool() {
            Ok(Value::bool_val(true))
        } else if let Some(false) = left.as_bool() {
            if right.as_bool().is_some() || right.is_null() {
                Ok(right)
            } else {
                Err(Self::invalid_condition_value(&right))
            }
        } else if left.is_null() {
            if let Some(true) = right.as_bool() {
                Ok(Value::bool_val(true))
            } else if right.as_bool() == Some(false) || right.is_null() {
                Ok(Value::null())
            } else {
                Err(Self::invalid_condition_value(&right))
            }
        } else {
            Err(Self::invalid_condition_value(&left))
        }
    }

    fn invalid_condition_value(value: &Value) -> Error {
        Error::TypeMismatch {
            expected: "BOOL".to_string(),
            actual: format!("{:?}", value.data_type()),
        }
    }

    fn evaluate_binary_op(
        &self,
        left: &SqlExpr,
        op: &BinaryOperator,
        right: &SqlExpr,
        row: &Row,
    ) -> Result<bool> {
        match op {
            BinaryOperator::And => {
                let left_result = self.evaluate_where(left, row)?;
                if !left_result {
                    return Ok(false);
                }
                self.evaluate_where(right, row)
            }
            BinaryOperator::Or => {
                let left_result = self.evaluate_where(left, row)?;
                if left_result {
                    return Ok(true);
                }
                self.evaluate_where(right, row)
            }
            _ => {
                let left_val = self.evaluate_expr(left, row)?;
                let right_val = self.evaluate_expr(right, row)?;

                let enum_labels = self
                    .get_enum_labels_for_expr(left)
                    .or_else(|| self.get_enum_labels_for_expr(right));
                match self.evaluate_comparison_with_enum_labels(
                    op,
                    &left_val,
                    &right_val,
                    enum_labels.as_deref(),
                ) {
                    Ok(result) => Ok(result),
                    Err(Error::NullComparison) => Ok(false),
                    Err(e) => Err(e),
                }
            }
        }
    }

    pub fn evaluate_expr(&self, expr: &SqlExpr, row: &Row) -> Result<Value> {
        match expr {
            SqlExpr::Identifier(ident) => self.get_column_value(row, &ident.value),

            SqlExpr::CompoundIdentifier(parts) => {
                if parts.len() == 2 {
                    let first_part = &parts[0].value;
                    let second_part = &parts[1].value;

                    if let Some(field) = self.schema.field(first_part) {
                        match &field.data_type {
                            DataType::Struct(_) | DataType::Custom(_) => {
                                let struct_value = self.get_column_value(row, first_part)?;
                                if struct_value.is_null() {
                                    return Ok(Value::null());
                                }
                                if let Some(map) = struct_value.as_struct() {
                                    let value = if let Some(v) = map.get(second_part) {
                                        v.clone()
                                    } else {
                                        let field_lower = second_part.to_lowercase();
                                        let found = map
                                            .iter()
                                            .find(|(k, _)| k.to_lowercase() == field_lower);
                                        match found {
                                            Some((_, v)) => v.clone(),
                                            None => {
                                                let available_fields: Vec<_> = map.keys().collect();
                                                return Err(Error::InvalidQuery(format!(
                                                    "Field '{}' not found in composite type. Available fields: {:?}",
                                                    second_part, available_fields
                                                )));
                                            }
                                        }
                                    };
                                    return Ok(value);
                                } else {
                                    return Err(Error::TypeMismatch {
                                        expected: "STRUCT".to_string(),
                                        actual: struct_value.data_type().to_string(),
                                    });
                                }
                            }
                            _ => self.get_qualified_column_value(row, first_part, second_part),
                        }
                    } else {
                        self.get_qualified_column_value(row, first_part, second_part)
                    }
                } else if parts.len() == 1 {
                    self.get_column_value(row, &parts[0].value)
                } else if parts.len() > 2 {
                    let first_part = &parts[0].value;
                    let mut current_value = if let Some(field) = self.schema.field(first_part) {
                        match &field.data_type {
                            DataType::Struct(_) | DataType::Custom(_) => {
                                self.get_column_value(row, first_part)?
                            }
                            _ => {
                                let second_part = &parts[1].value;
                                self.get_qualified_column_value(row, first_part, second_part)?
                            }
                        }
                    } else {
                        let second_part = &parts[1].value;
                        self.get_qualified_column_value(row, first_part, second_part)?
                    };

                    let start_idx = if self.schema.field(first_part).is_some() {
                        1
                    } else {
                        2
                    };

                    for part in &parts[start_idx..] {
                        if current_value.is_null() {
                            return Ok(Value::null());
                        }
                        if let Some(map) = current_value.as_struct() {
                            current_value = if let Some(v) = map.get(&part.value) {
                                v.clone()
                            } else {
                                let field_lower = part.value.to_lowercase();
                                let found =
                                    map.iter().find(|(k, _)| k.to_lowercase() == field_lower);
                                match found {
                                    Some((_, v)) => v.clone(),
                                    None => {
                                        let available_fields: Vec<_> = map.keys().collect();
                                        return Err(Error::InvalidQuery(format!(
                                            "Field '{}' not found in composite type. Available fields: {:?}",
                                            part.value, available_fields
                                        )));
                                    }
                                }
                            };
                        } else {
                            return Err(Error::TypeMismatch {
                                expected: "STRUCT".to_string(),
                                actual: current_value.data_type().to_string(),
                            });
                        }
                    }

                    Ok(current_value)
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Invalid column reference: {}",
                        parts
                            .iter()
                            .map(|p| p.value.as_str())
                            .collect::<Vec<_>>()
                            .join(".")
                    )))
                }
            }

            SqlExpr::Value(sql_value) => self.sql_value_to_value(sql_value),
            SqlExpr::TypedString(typed) => {
                use sqlparser::ast::{DataType as SqlDataType, Value as SqlValue};

                let literal = match &typed.value.value {
                    SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => s.clone(),
                    _ => {
                        return Err(Error::InvalidQuery(
                            "Typed string literal must be a string".to_string(),
                        ));
                    }
                };

                match &typed.data_type {
                    SqlDataType::Date => NaiveDate::parse_from_str(&literal, "%Y-%m-%d")
                        .map(Value::date)
                        .map_err(|_| {
                            Error::InvalidQuery(format!("Invalid DATE literal '{}'", literal))
                        }),
                    SqlDataType::Time(_, _) => {
                        let parsed = NaiveTime::parse_from_str(&literal, "%H:%M:%S")
                            .or_else(|_| NaiveTime::parse_from_str(&literal, "%H:%M:%S%.f"))
                            .map_err(|_| {
                                Error::InvalidQuery(format!("Invalid TIME literal '{}'", literal))
                            })?;
                        Ok(Value::time(parsed))
                    }
                    SqlDataType::Timestamp(_, tz_info) => {
                        use sqlparser::ast::TimezoneInfo;
                        use yachtsql_core::types::parse_timestamp_to_utc;

                        let is_with_tz =
                            matches!(tz_info, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz);

                        if is_with_tz {
                            parse_timestamp_to_utc(&literal)
                                .map(Value::timestamp)
                                .ok_or_else(|| {
                                    Error::InvalidQuery(format!(
                                        "Invalid TIMESTAMP WITH TIME ZONE literal '{}'. Expected format: YYYY-MM-DD HH:MM:SS[.ffffff][+/-HH:MM | timezone_name]",
                                        literal
                                    ))
                                })
                        } else {
                            let parsed =
                                NaiveDateTime::parse_from_str(&literal, "%Y-%m-%d %H:%M:%S")
                                    .or_else(|_| {
                                        NaiveDateTime::parse_from_str(
                                            &literal,
                                            "%Y-%m-%d %H:%M:%S%.f",
                                        )
                                    })
                                    .map_err(|_| {
                                        Error::InvalidQuery(format!(
                                            "Invalid TIMESTAMP literal '{}'",
                                            literal
                                        ))
                                    })?;
                            Ok(Value::timestamp(
                                DateTime::<Utc>::from_naive_utc_and_offset(parsed, Utc),
                            ))
                        }
                    }
                    SqlDataType::Uuid => {
                        use yachtsql_core::types::parse_uuid_strict;
                        parse_uuid_strict(&literal).map_err(|e| {
                            Error::InvalidQuery(format!(
                                "Invalid UUID literal '{}': {}",
                                literal, e
                            ))
                        })
                    }
                    SqlDataType::Custom(name, _) => {
                        let type_name = name.to_string().to_uppercase();
                        match type_name.as_str() {
                            "MACADDR" => {
                                use yachtsql_core::types::MacAddress;
                                MacAddress::parse(&literal, false)
                                    .map(Value::macaddr)
                                    .ok_or_else(|| {
                                        Error::InvalidQuery(format!(
                                            "Invalid MACADDR literal '{}'",
                                            literal
                                        ))
                                    })
                            }
                            "MACADDR8" => {
                                use yachtsql_core::types::MacAddress;
                                MacAddress::parse(&literal, true)
                                    .or_else(|| {
                                        MacAddress::parse(&literal, false).map(|mac| mac.to_eui64())
                                    })
                                    .map(Value::macaddr8)
                                    .ok_or_else(|| {
                                        Error::InvalidQuery(format!(
                                            "Invalid MACADDR8 literal '{}'",
                                            literal
                                        ))
                                    })
                            }
                            _ => Err(Error::UnsupportedFeature(format!(
                                "Typed string type {:?} not supported",
                                typed.data_type
                            ))),
                        }
                    }

                    SqlDataType::GeometricType(kind) => {
                        use sqlparser::ast::GeometricTypeKind;
                        use yachtsql_core::types::{PgBox, PgCircle, PgPoint};
                        match kind {
                            GeometricTypeKind::Point => {

                                PgPoint::parse(&literal).map(Value::point).ok_or_else(|| {
                                    Error::InvalidQuery(format!(
                                        "Invalid POINT literal '{}'. Expected format: (x,y)",
                                        literal
                                    ))
                                })
                            }
                            GeometricTypeKind::GeometricBox => {

                                PgBox::parse(&literal)
                                    .map(Value::pgbox)
                                    .ok_or_else(|| {
                                        Error::InvalidQuery(format!(
                                            "Invalid BOX literal '{}'. Expected format: ((x1,y1),(x2,y2))",
                                            literal
                                        ))
                                    })
                            }
                            GeometricTypeKind::Circle => {

                                PgCircle::parse(&literal).map(Value::circle).ok_or_else(|| {
                                    Error::InvalidQuery(format!(
                                        "Invalid CIRCLE literal '{}'. Expected format: <(x,y),r>",
                                        literal
                                    ))
                                })
                            }
                            _ => Err(Error::UnsupportedFeature(format!(
                                "Geometric type {:?} not supported",
                                kind
                            ))),
                        }
                    }
                    _ => Err(Error::UnsupportedFeature(format!(
                        "Typed string type {:?} not supported",
                        typed.data_type
                    ))),
                }
            }

            SqlExpr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => {
                    let left_val = self.evaluate_expr(left, row)?;
                    let right_val = self.evaluate_expr(right, row)?;
                    self.combine_logical_and(left_val, right_val)
                }
                BinaryOperator::Or => {
                    let left_val = self.evaluate_expr(left, row)?;
                    let right_val = self.evaluate_expr(right, row)?;
                    self.combine_logical_or(left_val, right_val)
                }
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq => {
                    let left_val = self.evaluate_expr(left, row)?;
                    let right_val = self.evaluate_expr(right, row)?;
                    if left_val.is_null() || right_val.is_null() {
                        Ok(Value::null())
                    } else {
                        match self.evaluate_comparison(op, &left_val, &right_val) {
                            Ok(result) => Ok(Value::bool_val(result)),
                            Err(Error::NullComparison) => Ok(Value::null()),
                            Err(e) => Err(e),
                        }
                    }
                }
                _ => {
                    let left_val = self.evaluate_expr(left, row)?;
                    let right_val = self.evaluate_expr(right, row)?;
                    self.apply_arithmetic_op(&left_val, op, &right_val)
                }
            },

            SqlExpr::UnaryOp { op, expr } => {
                let value = self.evaluate_expr(expr, row)?;
                match op {
                    UnaryOperator::Plus => Ok(value),
                    UnaryOperator::Minus => self.negate_value(&value),
                    UnaryOperator::Not => self.apply_not(&value),
                    UnaryOperator::PGBitwiseNot
                    | UnaryOperator::PGSquareRoot
                    | UnaryOperator::PGCubeRoot
                    | UnaryOperator::PGPostfixFactorial
                    | UnaryOperator::PGPrefixFactorial
                    | UnaryOperator::PGAbs
                    | UnaryOperator::BangNot
                    | UnaryOperator::AtDashAt
                    | UnaryOperator::DoubleAt
                    | UnaryOperator::Hash
                    | UnaryOperator::QuestionDash
                    | UnaryOperator::QuestionPipe => Err(Error::UnsupportedFeature(format!(
                        "Unary operator {:?} not supported",
                        op
                    ))),
                }
            }

            SqlExpr::Nested(inner) => self.evaluate_expr(inner, row),

            SqlExpr::Function(func) => self.evaluate_function(func, row),

            SqlExpr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let value = self.evaluate_expr(expr, row)?;

                let start = if let Some(from_expr) = substring_from {
                    let from_val = self.evaluate_expr(from_expr, row)?;
                    if let Some(i) = from_val.as_i64() {
                        if i < 1 { 0 } else { (i - 1) as usize }
                    } else {
                        0
                    }
                } else {
                    0
                };

                if let Some(b) = value.as_bytes() {
                    let subbytes = if let Some(for_expr) = substring_for {
                        let for_val = self.evaluate_expr(for_expr, row)?;
                        if let Some(len) = for_val.as_i64() {
                            if len <= 0 {
                                Vec::new()
                            } else {
                                let end = (start + len as usize).min(b.len());
                                if start >= b.len() {
                                    Vec::new()
                                } else {
                                    b[start..end].to_vec()
                                }
                            }
                        } else {
                            if start >= b.len() {
                                Vec::new()
                            } else {
                                b[start..].to_vec()
                            }
                        }
                    } else if start >= b.len() {
                        Vec::new()
                    } else {
                        b[start..].to_vec()
                    };
                    Ok(Value::bytes(subbytes))
                } else {
                    let s = self.value_to_string(&value)?;

                    let result = if let Some(for_expr) = substring_for {
                        let for_val = self.evaluate_expr(for_expr, row)?;
                        if let Some(len) = for_val.as_i64() {
                            let len = len as usize;
                            let end = (start + len).min(s.len());
                            s.chars().skip(start).take(end - start).collect()
                        } else {
                            s.chars().skip(start).collect()
                        }
                    } else {
                        s.chars().skip(start).collect()
                    };

                    Ok(Value::string(result))
                }
            }

            SqlExpr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                let operand_value = if let Some(op_expr) = operand {
                    Some(self.evaluate_expr(op_expr, row)?)
                } else {
                    None
                };

                for case_when in conditions {
                    let matched = if let Some(ref op_val) = operand_value {
                        let cond_val = self.evaluate_expr(&case_when.condition, row)?;
                        op_val == &cond_val
                    } else {
                        self.evaluate_where(&case_when.condition, row)?
                    };

                    if matched {
                        return self.evaluate_expr(&case_when.result, row);
                    }
                }

                if let Some(else_expr) = else_result {
                    self.evaluate_expr(else_expr, row)
                } else {
                    Ok(Value::null())
                }
            }

            SqlExpr::Cast {
                expr, data_type, ..
            } => {
                let value = self.evaluate_expr(expr, row)?;
                self.cast_value(value, data_type)
            }

            SqlExpr::Position { expr, r#in } => {
                let substring = self.evaluate_expr(expr, row)?;
                let string = self.evaluate_expr(r#in, row)?;

                if substring.is_null() || string.is_null() {
                    return Ok(Value::null());
                }

                match (substring.as_str(), string.as_str()) {
                    (Some(needle), Some(haystack)) => {
                        if let Some(pos) = haystack.find(needle) {
                            let char_pos = haystack[..pos].chars().count() + 1;
                            Ok(Value::int64(char_pos as i64))
                        } else {
                            Ok(Value::int64(0))
                        }
                    }
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: format!("{}, {}", substring.data_type(), string.data_type()),
                    }),
                }
            }

            SqlExpr::Trim {
                expr,
                trim_where,
                trim_what,
                ..
            } => {
                let value = self.evaluate_expr(expr, row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }

                if let Some(s) = value.as_str() {
                    use sqlparser::ast::TrimWhereField;
                    let result = match trim_where {
                        Some(TrimWhereField::Leading) => s.trim_start().to_string(),
                        Some(TrimWhereField::Trailing) => s.trim_end().to_string(),
                        Some(TrimWhereField::Both) | None => {
                            if let Some(trim_expr) = trim_what {
                                let trim_chars = self.evaluate_expr(trim_expr, row)?;
                                if let Some(chars) = trim_chars.as_str() {
                                    s.trim_matches(|c| chars.contains(c)).to_string()
                                } else {
                                    s.trim().to_string()
                                }
                            } else {
                                s.trim().to_string()
                            }
                        }
                    };
                    Ok(Value::string(result))
                } else {
                    Err(Error::InvalidQuery(
                        "TRIM requires a string argument".to_string(),
                    ))
                }
            }

            SqlExpr::Array(array) => {
                let mut values = Vec::new();
                for elem_expr in &array.elem {
                    let value = self.evaluate_expr(elem_expr, row)?;
                    values.push(value);
                }

                Self::validate_array_value_homogeneity(&values)?;

                Ok(Value::array(values))
            }

            SqlExpr::Like {
                expr,
                pattern,
                escape_char,
                negated,
                ..
            } => self.evaluate_condition_expr(
                &SqlExpr::Like {
                    expr: expr.clone(),
                    pattern: pattern.clone(),
                    escape_char: escape_char.clone(),
                    negated: *negated,
                    any: false,
                },
                row,
            ),

            SqlExpr::CompoundFieldAccess { root, access_chain } => {
                use sqlparser::ast::{AccessExpr, Subscript};

                let mut value = self.evaluate_expr(root, row)?;

                for operation in access_chain {
                    match operation {
                        AccessExpr::Subscript(subscript) => match subscript {
                            Subscript::Index { index } => {
                                let index_value = self.evaluate_expr(index, row)?;
                                value = yachtsql_functions::array::array_subscript(
                                    &value,
                                    &index_value,
                                )?;
                            }
                            Subscript::Slice {
                                lower_bound,
                                upper_bound,
                                ..
                            } => {
                                let start_val = if let Some(s) = lower_bound {
                                    Some(self.evaluate_expr(s, row)?)
                                } else {
                                    None
                                };
                                let end_val = if let Some(e) = upper_bound {
                                    Some(self.evaluate_expr(e, row)?)
                                } else {
                                    None
                                };
                                value = yachtsql_functions::array::array_slice(
                                    &value,
                                    start_val.as_ref(),
                                    end_val.as_ref(),
                                )?;
                            }
                        },
                        AccessExpr::Dot(field_expr) => {
                            let field_name = match field_expr {
                                SqlExpr::Identifier(ident) => &ident.value,
                                _ => {
                                    return Err(Error::UnsupportedFeature(format!(
                                        "Only identifier-based dot access is supported, got: {}",
                                        field_expr
                                    )));
                                }
                            };

                            if value.is_null() {
                                value = Value::null();
                            } else if let Some(map) = value.as_struct() {
                                value = if let Some(v) = map.get(field_name) {
                                    v.clone()
                                } else {
                                    let field_lower = field_name.to_lowercase();
                                    let found =
                                        map.iter().find(|(k, _)| k.to_lowercase() == field_lower);
                                    match found {
                                        Some((_, v)) => v.clone(),
                                        None => {
                                            let available_fields: Vec<_> = map.keys().collect();
                                            return Err(Error::InvalidQuery(format!(
                                                "Field '{}' not found in composite type. Available fields: {:?}",
                                                field_name, available_fields
                                            )));
                                        }
                                    }
                                };
                            } else {
                                return Err(Error::TypeMismatch {
                                    expected: "STRUCT".to_string(),
                                    actual: value.data_type().to_string(),
                                });
                            }
                        }
                    }
                }

                Ok(value)
            }

            SqlExpr::IsNull(inner) => {
                let value = self.evaluate_expr(inner, row)?;
                Ok(Value::bool_val(value.is_null()))
            }

            SqlExpr::IsNotNull(inner) => {
                let value = self.evaluate_expr(inner, row)?;
                Ok(Value::bool_val(!value.is_null()))
            }

            SqlExpr::Ceil { expr, .. } => {
                let value = self.evaluate_expr(expr, row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(f) = value.as_f64() {
                    Ok(Value::float64(f.ceil()))
                } else if let Some(i) = value.as_i64() {
                    Ok(Value::float64(i as f64))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "NUMERIC".to_string(),
                        actual: format!("{:?}", value.data_type()),
                    })
                }
            }

            SqlExpr::Floor { expr, .. } => {
                let value = self.evaluate_expr(expr, row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(f) = value.as_f64() {
                    Ok(Value::float64(f.floor()))
                } else if let Some(i) = value.as_i64() {
                    Ok(Value::float64(i as f64))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "NUMERIC".to_string(),
                        actual: format!("{:?}", value.data_type()),
                    })
                }
            }

            SqlExpr::SimilarTo {
                expr,
                pattern,
                escape_char,
                negated,
            } => self.evaluate_condition_expr(
                &SqlExpr::SimilarTo {
                    expr: expr.clone(),
                    pattern: pattern.clone(),
                    escape_char: escape_char.clone(),
                    negated: *negated,
                },
                row,
            ),
            SqlExpr::Interval(interval) => self.evaluate_interval(interval),
            SqlExpr::AtTimeZone {
                timestamp,
                time_zone,
            } => {
                let is_plain_timestamp = self.is_plain_timestamp_expr(timestamp);

                let ts_value = self.evaluate_expr(timestamp, row)?;
                if ts_value.is_null() {
                    return Ok(Value::null());
                }

                let utc_dt = ts_value.as_timestamp().ok_or_else(|| Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: format!("{:?}", ts_value.data_type()),
                })?;

                let tz_value = self.evaluate_expr(time_zone, row)?;
                if tz_value.is_null() {
                    return Ok(Value::null());
                }

                let tz_str = tz_value.as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: format!("{:?}", tz_value.data_type()),
                })?;

                if is_plain_timestamp {
                    self.interpret_as_timezone(utc_dt, tz_str)
                } else {
                    self.apply_at_time_zone(utc_dt, tz_str)
                }
            }

            SqlExpr::Extract { field, expr, .. } => {
                use sqlparser::ast::DateTimeField;

                let date_val = self.evaluate_expr(expr, row)?;
                if date_val.is_null() {
                    return Ok(Value::null());
                }

                if let Some(d) = date_val.as_date() {
                    match field {
                        DateTimeField::Year => Ok(Value::int64(d.year() as i64)),
                        DateTimeField::Month => Ok(Value::int64(d.month() as i64)),
                        DateTimeField::Day => Ok(Value::int64(d.day() as i64)),
                        DateTimeField::Week(_) => Ok(Value::int64(d.iso_week().week() as i64)),
                        DateTimeField::Quarter => {
                            let quarter = ((d.month() - 1) / 3) + 1;
                            Ok(Value::int64(quarter as i64))
                        }
                        DateTimeField::Dow | DateTimeField::DayOfWeek => Ok(Value::int64(
                            (d.weekday().num_days_from_sunday() as i64) + 1,
                        )),
                        DateTimeField::Doy | DateTimeField::DayOfYear => {
                            Ok(Value::int64(d.ordinal() as i64))
                        }
                        _ => Err(Error::InvalidQuery(format!(
                            "Cannot extract {:?} from DATE",
                            field
                        ))),
                    }
                } else if let Some(ts) = date_val.as_timestamp() {
                    match field {
                        DateTimeField::Year => Ok(Value::int64(ts.year() as i64)),
                        DateTimeField::Month => Ok(Value::int64(ts.month() as i64)),
                        DateTimeField::Day => Ok(Value::int64(ts.day() as i64)),
                        DateTimeField::Hour => Ok(Value::int64(ts.hour() as i64)),
                        DateTimeField::Minute => Ok(Value::int64(ts.minute() as i64)),
                        DateTimeField::Second => Ok(Value::int64(ts.second() as i64)),
                        DateTimeField::Week(_) => Ok(Value::int64(ts.iso_week().week() as i64)),
                        DateTimeField::Quarter => {
                            let quarter = ((ts.month() - 1) / 3) + 1;
                            Ok(Value::int64(quarter as i64))
                        }
                        DateTimeField::Dow | DateTimeField::DayOfWeek => {
                            Ok(Value::int64(ts.weekday().num_days_from_sunday() as i64))
                        }
                        DateTimeField::Doy | DateTimeField::DayOfYear => {
                            Ok(Value::int64(ts.ordinal() as i64))
                        }
                        DateTimeField::Epoch => Ok(Value::int64(ts.timestamp())),
                        DateTimeField::Millisecond | DateTimeField::Milliseconds => {
                            Ok(Value::int64(ts.timestamp_subsec_millis() as i64))
                        }
                        DateTimeField::Microsecond | DateTimeField::Microseconds => {
                            Ok(Value::int64(ts.timestamp_subsec_micros() as i64))
                        }
                        _ => Err(Error::InvalidQuery(format!(
                            "Cannot extract {:?} from TIMESTAMP",
                            field
                        ))),
                    }
                } else if let Some(t) = date_val.as_time() {
                    match field {
                        DateTimeField::Hour => Ok(Value::int64(t.hour() as i64)),
                        DateTimeField::Minute => Ok(Value::int64(t.minute() as i64)),
                        DateTimeField::Second => Ok(Value::int64(t.second() as i64)),
                        _ => Err(Error::InvalidQuery(format!(
                            "Cannot extract {:?} from TIME",
                            field
                        ))),
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "DATE, TIMESTAMP, or TIME".to_string(),
                        actual: format!("{:?}", date_val.data_type()),
                    })
                }
            }

            SqlExpr::IsDistinctFrom(left, right) => {
                let left_val = self.evaluate_expr(left, row)?;
                let right_val = self.evaluate_expr(right, row)?;

                let is_distinct = match (left_val.is_null(), right_val.is_null()) {
                    (true, true) => false,
                    (true, false) | (false, true) => true,
                    (false, false) => !self.values_are_distinct_from_equal(&left_val, &right_val),
                };
                Ok(Value::bool_val(is_distinct))
            }

            SqlExpr::IsNotDistinctFrom(left, right) => {
                let left_val = self.evaluate_expr(left, row)?;
                let right_val = self.evaluate_expr(right, row)?;

                let is_not_distinct = match (left_val.is_null(), right_val.is_null()) {
                    (true, true) => true,
                    (true, false) | (false, true) => false,
                    (false, false) => self.values_are_distinct_from_equal(&left_val, &right_val),
                };
                Ok(Value::bool_val(is_not_distinct))
            }

            SqlExpr::Tuple(exprs) => {
                let mut result_map = indexmap::IndexMap::new();
                for (i, expr) in exprs.iter().enumerate() {
                    let value = self.evaluate_expr(expr, row)?;

                    let field_name = format!("f{}", i + 1);
                    result_map.insert(field_name, value);
                }
                Ok(Value::struct_val(result_map))
            }

            _ => Err(Error::UnsupportedFeature(format!(
                "Expression {:?} not supported",
                expr
            ))),
        }
    }

    fn get_column_value(&self, row: &Row, col_name: &str) -> Result<Value> {
        let matching_indices: Vec<usize> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| f.name == col_name)
            .map(|(i, _)| i)
            .collect();

        match matching_indices.len() {
            0 => Err(Error::ColumnNotFound(col_name.to_string())),
            1 => Ok(row.values()[matching_indices[0]].clone()),
            _ => {
                let is_join_context = self.left_schema.is_some()
                    || self.right_schema.is_some()
                    || self.has_multiple_source_tables(&matching_indices);

                if is_join_context {
                    Err(Error::InvalidQuery(format!(
                        "Column reference '{}' is ambiguous - it exists in multiple tables. Use table.column syntax to disambiguate.",
                        col_name
                    )))
                } else {
                    Ok(row.values()[matching_indices[0]].clone())
                }
            }
        }
    }

    fn has_multiple_source_tables(&self, indices: &[usize]) -> bool {
        let fields = self.schema.fields();
        let source_tables: std::collections::HashSet<_> = indices
            .iter()
            .filter_map(|&idx| fields[idx].source_table.as_ref())
            .collect();

        source_tables.len() > 1
    }

    fn get_qualified_column_value(
        &self,
        row: &Row,
        table_name: &str,
        col_name: &str,
    ) -> Result<Value> {
        if let Some(idx) = self
            .schema
            .fields()
            .iter()
            .position(|f| f.source_table.as_deref() == Some(table_name) && f.name == col_name)
        {
            return Ok(row.values()[idx].clone());
        }

        if let (Some(left_schema), Some(right_schema), Some(left_col_count)) =
            (self.left_schema, self.right_schema, self.left_col_count)
        {
            let table_side = self.table_map.get(table_name);

            match table_side {
                Some(TableSide::Left) => {
                    if let Some(left_idx) =
                        left_schema.fields().iter().position(|f| f.name == col_name)
                    {
                        return Ok(row.values()[left_idx].clone());
                    }
                    Err(Error::ColumnNotFound(format!(
                        "{}.{}",
                        table_name, col_name
                    )))
                }
                Some(TableSide::Right) => {
                    if let Some(right_idx) = right_schema
                        .fields()
                        .iter()
                        .position(|f| f.name == col_name)
                    {
                        let combined_idx = left_col_count + right_idx;
                        return Ok(row.values()[combined_idx].clone());
                    }
                    Err(Error::ColumnNotFound(format!(
                        "{}.{}",
                        table_name, col_name
                    )))
                }
                None => {
                    if let Some(left_idx) =
                        left_schema.fields().iter().position(|f| f.name == col_name)
                    {
                        return Ok(row.values()[left_idx].clone());
                    }
                    if let Some(right_idx) = right_schema
                        .fields()
                        .iter()
                        .position(|f| f.name == col_name)
                    {
                        let combined_idx = left_col_count + right_idx;
                        return Ok(row.values()[combined_idx].clone());
                    }
                    Err(Error::ColumnNotFound(col_name.to_string()))
                }
            }
        } else if let Some(left_col_count) = self.left_col_count {
            let table_side = self.table_map.get(table_name);

            match table_side {
                Some(TableSide::Left) => {
                    let left_fields = &self.schema.fields()[..left_col_count];
                    if let Some(left_idx) = left_fields.iter().position(|f| f.name == col_name) {
                        return Ok(row.values()[left_idx].clone());
                    }
                    Err(Error::ColumnNotFound(format!(
                        "{}.{}",
                        table_name, col_name
                    )))
                }
                Some(TableSide::Right) => {
                    let right_fields = &self.schema.fields()[left_col_count..];
                    if let Some(right_idx) = right_fields.iter().position(|f| f.name == col_name) {
                        let combined_idx = left_col_count + right_idx;
                        return Ok(row.values()[combined_idx].clone());
                    }
                    Err(Error::ColumnNotFound(format!(
                        "{}.{}",
                        table_name, col_name
                    )))
                }
                None => {
                    let left_fields = &self.schema.fields()[..left_col_count];
                    if let Some(left_idx) = left_fields.iter().position(|f| f.name == col_name) {
                        return Ok(row.values()[left_idx].clone());
                    }
                    let right_fields = &self.schema.fields()[left_col_count..];
                    if let Some(right_idx) = right_fields.iter().position(|f| f.name == col_name) {
                        let combined_idx = left_col_count + right_idx;
                        return Ok(row.values()[combined_idx].clone());
                    }
                    Err(Error::ColumnNotFound(col_name.to_string()))
                }
            }
        } else {
            self.get_column_value(row, col_name)
        }
    }

    fn sql_value_to_value(&self, sql_value: &sqlparser::ast::ValueWithSpan) -> Result<Value> {
        use sqlparser::ast::Value as SqlValue;

        match &sql_value.value {
            SqlValue::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::int64(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Value::float64(f))
                } else {
                    Err(Error::InvalidQuery(format!("Invalid number: {}", n)))
                }
            }
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                if let Some(hex_str) = s.strip_prefix("\\x") {
                    Self::parse_hex_literal(hex_str)
                } else {
                    Ok(Value::string(s.clone()))
                }
            }
            SqlValue::SingleQuotedByteStringLiteral(s)
            | SqlValue::DoubleQuotedByteStringLiteral(s)
            | SqlValue::TripleSingleQuotedByteStringLiteral(s)
            | SqlValue::TripleDoubleQuotedByteStringLiteral(s) => {
                if let Some(hex_str) = s.strip_prefix("\\x") {
                    Self::parse_hex_literal(hex_str)
                } else {
                    Ok(Value::bytes(s.as_bytes().to_vec()))
                }
            }
            SqlValue::HexStringLiteral(s) => Self::parse_hex_literal(s),
            SqlValue::Boolean(b) => Ok(Value::bool_val(*b)),
            SqlValue::Null => Ok(Value::null()),
            other => Err(Error::UnsupportedFeature(format!(
                "SQL value {:?} not supported",
                other
            ))),
        }
    }

    fn parse_hex_literal(hex_str: &str) -> Result<Value> {
        if !hex_str.len().is_multiple_of(2) {
            return Err(Error::InvalidQuery(format!(
                "Hex literal must have even number of characters, got {}",
                hex_str.len()
            )));
        }

        let mut bytes = Vec::with_capacity(hex_str.len() / 2);
        for i in (0..hex_str.len()).step_by(2) {
            let byte_str = &hex_str[i..i + 2];
            match u8::from_str_radix(byte_str, 16) {
                Ok(byte) => bytes.push(byte),
                Err(_) => {
                    return Err(Error::InvalidQuery(format!(
                        "Invalid hex characters in literal: '{}'",
                        byte_str
                    )));
                }
            }
        }
        Ok(Value::bytes(bytes))
    }

    fn evaluate_comparison(
        &self,
        op: &BinaryOperator,
        left: &Value,
        right: &Value,
    ) -> Result<bool> {
        self.evaluate_comparison_with_enum_labels(op, left, right, None)
    }

    fn evaluate_comparison_with_enum_labels(
        &self,
        op: &BinaryOperator,
        left: &Value,
        right: &Value,
        enum_labels: Option<&[String]>,
    ) -> Result<bool> {
        let has_null = left.is_null() || right.is_null();

        match op {
            BinaryOperator::Eq => {
                if has_null {
                    Ok(false)
                } else {
                    Ok(
                        self.compare_values_with_enum_labels(left, right, enum_labels)?
                            == std::cmp::Ordering::Equal,
                    )
                }
            }
            BinaryOperator::NotEq => {
                if has_null {
                    Ok(false)
                } else {
                    Ok(
                        self.compare_values_with_enum_labels(left, right, enum_labels)?
                            != std::cmp::Ordering::Equal,
                    )
                }
            }
            BinaryOperator::Lt => {
                if has_null {
                    Ok(false)
                } else {
                    Ok(
                        self.compare_values_with_enum_labels(left, right, enum_labels)?
                            == std::cmp::Ordering::Less,
                    )
                }
            }
            BinaryOperator::LtEq => {
                if has_null {
                    Ok(false)
                } else {
                    let cmp = self.compare_values_with_enum_labels(left, right, enum_labels)?;
                    Ok(cmp != std::cmp::Ordering::Greater)
                }
            }
            BinaryOperator::Gt => {
                if has_null {
                    Ok(false)
                } else {
                    Ok(
                        self.compare_values_with_enum_labels(left, right, enum_labels)?
                            == std::cmp::Ordering::Greater,
                    )
                }
            }
            BinaryOperator::GtEq => {
                if has_null {
                    Ok(false)
                } else {
                    let cmp = self.compare_values_with_enum_labels(left, right, enum_labels)?;
                    Ok(cmp != std::cmp::Ordering::Less)
                }
            }
            BinaryOperator::AtArrow => {
                if has_null {
                    Ok(false)
                } else {
                    if left.as_hstore().is_some() {
                        let result = yachtsql_functions::hstore::hstore_contains(left, right)?;
                        result.as_bool().ok_or_else(|| {
                            Error::InvalidQuery("Hstore contains should return boolean".to_string())
                        })
                    } else if left.as_pgbox().is_some() || left.as_circle().is_some() {
                        let result = yachtsql_functions::geometric::contains(left, right)?;
                        result.as_bool().ok_or_else(|| {
                            Error::InvalidQuery(
                                "Geometric contains should return boolean".to_string(),
                            )
                        })
                    } else {
                        let result = yachtsql_functions::array::array_contains_array(left, right)?;
                        result.as_bool().ok_or_else(|| {
                            Error::InvalidQuery("Array contains should return boolean".to_string())
                        })
                    }
                }
            }
            BinaryOperator::ArrowAt => {
                if has_null {
                    Ok(false)
                } else {
                    if left.as_hstore().is_some() {
                        let result = yachtsql_functions::hstore::hstore_contained_by(left, right)?;
                        result.as_bool().ok_or_else(|| {
                            Error::InvalidQuery(
                                "Hstore contained_by should return boolean".to_string(),
                            )
                        })
                    } else if left.as_point().is_some() {
                        let result = yachtsql_functions::geometric::contained_by(left, right)?;
                        result.as_bool().ok_or_else(|| {
                            Error::InvalidQuery(
                                "Geometric contained_by should return boolean".to_string(),
                            )
                        })
                    } else {
                        let result = yachtsql_functions::array::array_contained_by(left, right)?;
                        result.as_bool().ok_or_else(|| {
                            Error::InvalidQuery(
                                "Array contained by should return boolean".to_string(),
                            )
                        })
                    }
                }
            }
            BinaryOperator::PGOverlap => {
                if has_null {
                    Ok(false)
                } else {
                    if left.as_pgbox().is_some() || left.as_circle().is_some() {
                        let result = yachtsql_functions::geometric::overlaps(left, right)?;
                        result.as_bool().ok_or_else(|| {
                            Error::InvalidQuery(
                                "Geometric overlaps should return boolean".to_string(),
                            )
                        })
                    } else {
                        let result = yachtsql_functions::array::array_overlap(left, right)?;
                        result.as_bool().ok_or_else(|| {
                            Error::InvalidQuery("Array overlap should return boolean".to_string())
                        })
                    }
                }
            }

            BinaryOperator::LtDashGt => {
                if has_null {
                    Ok(false)
                } else {
                    let result = yachtsql_functions::geometric::distance(left, right)?;
                    if let Some(dist) = result.as_f64() {
                        Ok(dist != 0.0)
                    } else {
                        Err(Error::InvalidQuery(
                            "Distance should return float64".to_string(),
                        ))
                    }
                }
            }

            BinaryOperator::Question => {
                if has_null {
                    Ok(false)
                } else if left.as_hstore().is_some() {
                    let result = yachtsql_functions::hstore::hstore_exists(left, right)?;
                    result.as_bool().ok_or_else(|| {
                        Error::InvalidQuery("Hstore exists should return boolean".to_string())
                    })
                } else {
                    Err(Error::TypeMismatch {
                        expected: "HSTORE".to_string(),
                        actual: format!("{:?}", left.data_type()),
                    })
                }
            }

            BinaryOperator::QuestionAnd => {
                if has_null {
                    Ok(false)
                } else if left.as_hstore().is_some() {
                    let result = yachtsql_functions::hstore::hstore_exists_all(left, right)?;
                    result.as_bool().ok_or_else(|| {
                        Error::InvalidQuery("Hstore exists_all should return boolean".to_string())
                    })
                } else {
                    Err(Error::TypeMismatch {
                        expected: "HSTORE".to_string(),
                        actual: format!("{:?}", left.data_type()),
                    })
                }
            }

            BinaryOperator::QuestionPipe => {
                if has_null {
                    Ok(false)
                } else if left.as_hstore().is_some() {
                    let result = yachtsql_functions::hstore::hstore_exists_any(left, right)?;
                    result.as_bool().ok_or_else(|| {
                        Error::InvalidQuery("Hstore exists_any should return boolean".to_string())
                    })
                } else {
                    Err(Error::TypeMismatch {
                        expected: "HSTORE".to_string(),
                        actual: format!("{:?}", left.data_type()),
                    })
                }
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Comparison operator {:?} not supported",
                op
            ))),
        }
    }

    fn compare_values(&self, left: &Value, right: &Value) -> Result<std::cmp::Ordering> {
        self.compare_values_with_enum_labels(left, right, None)
    }

    fn compare_values_with_enum_labels(
        &self,
        left: &Value,
        right: &Value,
        enum_labels: Option<&[String]>,
    ) -> Result<std::cmp::Ordering> {
        use std::cmp::Ordering;

        if left.is_null() && right.is_null() {
            return Ok(Ordering::Equal);
        }
        if left.is_null() {
            return Ok(Ordering::Less);
        }
        if right.is_null() {
            return Ok(Ordering::Greater);
        }

        if let (Some(a), Some(b)) = (left.as_i64(), right.as_i64()) {
            return Ok(a.cmp(&b));
        }

        if let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) {
            return Ok(yachtsql_core::float_utils::float_cmp(&a, &b));
        }

        if let (Some(a), Some(b)) = (left.as_i64(), right.as_f64()) {
            return Ok(yachtsql_core::float_utils::float_cmp(&(a as f64), &b));
        }

        if let (Some(a), Some(b)) = (left.as_f64(), right.as_i64()) {
            return Ok(yachtsql_core::float_utils::float_cmp(&a, &(b as f64)));
        }

        if let (Some(a), Some(b)) = (left.as_f64(), right.as_numeric()) {
            use rust_decimal::prelude::ToPrimitive;
            let b_f64 = b.to_f64().unwrap_or(0.0);
            return Ok(yachtsql_core::float_utils::float_cmp(&a, &b_f64));
        }

        if let (Some(a), Some(b)) = (left.as_numeric(), right.as_f64()) {
            use rust_decimal::prelude::ToPrimitive;
            let a_f64 = a.to_f64().unwrap_or(0.0);
            return Ok(yachtsql_core::float_utils::float_cmp(&a_f64, &b));
        }

        if let (Some(a), Some(b)) = (left.as_numeric(), right.as_numeric()) {
            return Ok(a.cmp(&b));
        }

        if let (Some(a), Some(b)) = (left.as_str(), right.as_str()) {
            if let Some(labels) = enum_labels {
                let a_pos = labels.iter().position(|l| l == a);
                let b_pos = labels.iter().position(|l| l == b);
                if let (Some(a_ord), Some(b_ord)) = (a_pos, b_pos) {
                    return Ok(a_ord.cmp(&b_ord));
                }
            }

            return Ok(a.to_lowercase().cmp(&b.to_lowercase()));
        }

        if let (Some(a), Some(b)) = (left.as_bool(), right.as_bool()) {
            return Ok(a.cmp(&b));
        }

        if let (Some(a), Some(b)) = (left.as_date(), right.as_date()) {
            return Ok(a.cmp(&b));
        }

        if let (Some(a), Some(b)) = (left.as_timestamp(), right.as_timestamp()) {
            return Ok(a.cmp(&b));
        }

        if let (Some(a), Some(b)) = (left.as_time(), right.as_time()) {
            return Ok(a.cmp(&b));
        }

        if let (Some(a), Some(b)) = (left.as_bytes(), right.as_bytes()) {
            return Ok(a.cmp(b));
        }

        if let (Some(a), Some(b)) = (left.as_interval(), right.as_interval()) {
            return Ok(a.cmp(&b));
        }

        if let (Some(a), Some(b)) = (left.as_macaddr(), right.as_macaddr()) {
            return Ok(a.cmp(&b));
        }

        if let (Some(a), Some(b)) = (left.as_macaddr8(), right.as_macaddr8()) {
            return Ok(a.cmp(&b));
        }

        if let (Some(left_map), Some(right_map)) = (left.as_struct(), right.as_struct()) {
            let left_vals: Vec<&Value> = left_map.values().collect();
            let right_vals: Vec<&Value> = right_map.values().collect();

            if left_vals.len() != right_vals.len() {
                return Err(Error::TypeMismatch {
                    expected: format!("Struct with {} fields", left_vals.len()),
                    actual: format!("Struct with {} fields", right_vals.len()),
                });
            }

            for (l, r) in left_vals.iter().zip(right_vals.iter()) {
                if l.is_null() || r.is_null() {
                    return Err(Error::NullComparison);
                }
            }

            for (l, r) in left_vals.iter().zip(right_vals.iter()) {
                let cmp = self.compare_values(l, r)?;
                if cmp != Ordering::Equal {
                    return Ok(cmp);
                }
            }
            return Ok(Ordering::Equal);
        }

        Err(Error::TypeMismatch {
            expected: format!("{:?}", left.data_type()),
            actual: format!("{:?}", right.data_type()),
        })
    }

    fn get_column_data_type(&self, col_name: &str) -> Option<&DataType> {
        self.schema
            .fields()
            .iter()
            .find(|f| f.name == col_name)
            .map(|f| &f.data_type)
    }

    fn get_enum_labels_for_expr(&self, expr: &SqlExpr) -> Option<Vec<String>> {
        match expr {
            SqlExpr::Identifier(ident) => {
                if let Some(DataType::Enum { labels, .. }) = self.get_column_data_type(&ident.value)
                {
                    Some(labels.clone())
                } else {
                    None
                }
            }
            SqlExpr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let col_name = &parts[1].value;
                if let Some(DataType::Enum { labels, .. }) = self.get_column_data_type(col_name) {
                    Some(labels.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn evaluate_like(&self, text: &Value, pattern: &Value, case_insensitive: bool) -> Result<bool> {
        let text_str = text.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: format!("{:?}", text.data_type()),
        })?;
        let pattern_str = pattern.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: format!("{:?}", pattern.data_type()),
        })?;

        let mut regex_pattern = String::from("^");
        let mut chars = pattern_str.chars().peekable();
        while let Some(ch) = chars.next() {
            match ch {
                '%' => regex_pattern.push_str(".*"),
                '_' => regex_pattern.push('.'),
                '\\' => {
                    if let Some(&next_ch) = chars.peek() {
                        if next_ch == '%' || next_ch == '_' || next_ch == '\\' {
                            regex_pattern.push_str(&regex::escape(&next_ch.to_string()));
                            chars.next();
                        } else {
                            regex_pattern.push_str(r"\\");
                        }
                    } else {
                        regex_pattern.push_str(r"\\");
                    }
                }
                _ => regex_pattern.push_str(&regex::escape(&ch.to_string())),
            }
        }
        regex_pattern.push('$');

        let regex = if case_insensitive {
            regex::RegexBuilder::new(&regex_pattern)
                .case_insensitive(true)
                .build()
                .map_err(|e| Error::InvalidQuery(format!("Invalid LIKE pattern: {}", e)))?
        } else {
            regex::Regex::new(&regex_pattern)
                .map_err(|e| Error::InvalidQuery(format!("Invalid LIKE pattern: {}", e)))?
        };

        Ok(regex.is_match(text_str))
    }

    fn values_equal(&self, left: &Value, right: &Value) -> Result<bool> {
        Ok(self.compare_values(left, right)? == std::cmp::Ordering::Equal)
    }

    fn values_are_distinct_from_equal(&self, left: &Value, right: &Value) -> bool {
        if let (Some(left_map), Some(right_map)) = (left.as_struct(), right.as_struct()) {
            if left_map.len() != right_map.len() {
                return false;
            }

            for ((lk, lv), (rk, rv)) in left_map.iter().zip(right_map.iter()) {
                if lk != rk {
                    return false;
                }

                match (lv.is_null(), rv.is_null()) {
                    (true, true) => continue,
                    (true, false) | (false, true) => return false,
                    (false, false) => {
                        if !self.values_are_distinct_from_equal(lv, rv) {
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        self.compare_values(left, right)
            .map(|ord| ord == std::cmp::Ordering::Equal)
            .unwrap_or(false)
    }

    fn apply_arithmetic_op(
        &self,
        left: &Value,
        op: &BinaryOperator,
        right: &Value,
    ) -> Result<Value> {
        if left.is_null() || right.is_null() {
            return Ok(Value::null());
        }

        match op {
            BinaryOperator::Plus => {
                if let (Some(ts), Some(interval)) = (left.as_timestamp(), right.as_interval()) {
                    return self.add_interval_to_timestamp(ts, interval);
                }

                if let (Some(interval), Some(ts)) = (left.as_interval(), right.as_timestamp()) {
                    return self.add_interval_to_timestamp(ts, interval);
                }

                if let (Some(date), Some(interval)) = (left.as_date(), right.as_interval()) {
                    return self.add_interval_to_date(date, interval);
                }

                if let (Some(interval), Some(date)) = (left.as_interval(), right.as_date()) {
                    return self.add_interval_to_date(date, interval);
                }

                if let (Some(a), Some(b)) = (left.as_i64(), right.as_i64()) {
                    a.checked_add(b)
                        .map(Value::int64)
                        .ok_or_else(|| Error::ArithmeticOverflow {
                            operation: "addition".to_string(),
                            left: a.to_string(),
                            right: b.to_string(),
                        })
                } else if let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) {
                    Ok(Value::float64(a + b))
                } else if let (Some(a), Some(b)) = (left.as_i64(), right.as_f64()) {
                    Ok(Value::float64(a as f64 + b))
                } else if let (Some(a), Some(b)) = (left.as_f64(), right.as_i64()) {
                    Ok(Value::float64(a + b as f64))
                } else if let (Some(ts), Some(interval)) =
                    (left.as_timestamp(), right.as_interval())
                {
                    interval
                        .add_to_timestamp(ts)
                        .map(Value::timestamp)
                        .ok_or_else(|| {
                            Error::invalid_query(
                                "Timestamp overflow in interval addition".to_string(),
                            )
                        })
                } else if let (Some(interval), Some(ts)) =
                    (left.as_interval(), right.as_timestamp())
                {
                    interval
                        .add_to_timestamp(ts)
                        .map(Value::timestamp)
                        .ok_or_else(|| {
                            Error::invalid_query(
                                "Timestamp overflow in interval addition".to_string(),
                            )
                        })
                } else if let (Some(dt), Some(interval)) = (left.as_datetime(), right.as_interval())
                {
                    interval
                        .add_to_timestamp(dt)
                        .map(Value::datetime)
                        .ok_or_else(|| {
                            Error::invalid_query(
                                "DateTime overflow in interval addition".to_string(),
                            )
                        })
                } else if let (Some(interval), Some(dt)) = (left.as_interval(), right.as_datetime())
                {
                    interval
                        .add_to_timestamp(dt)
                        .map(Value::datetime)
                        .ok_or_else(|| {
                            Error::invalid_query(
                                "DateTime overflow in interval addition".to_string(),
                            )
                        })
                } else if let (Some(a), Some(b)) = (left.as_interval(), right.as_interval()) {
                    Ok(Value::interval(a.add(b)))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "numeric types or timestamp/interval".to_string(),
                        actual: format!("{:?} + {:?}", left.data_type(), right.data_type()),
                    })
                }
            }
            BinaryOperator::Minus => {
                if left.as_hstore().is_some() {
                    if right.as_str().is_some() {
                        return yachtsql_functions::hstore::hstore_delete_key(left, right);
                    } else if right.as_array().is_some() {
                        return yachtsql_functions::hstore::hstore_delete_keys(left, right);
                    } else if right.as_hstore().is_some() {
                        return yachtsql_functions::hstore::hstore_delete_hstore(left, right);
                    }
                }

                if let (Some(ts), Some(interval)) = (left.as_timestamp(), right.as_interval()) {
                    return self.subtract_interval_from_timestamp(ts, interval);
                }

                if let (Some(ts1), Some(ts2)) = (left.as_timestamp(), right.as_timestamp()) {
                    return self.subtract_timestamps(ts1, ts2);
                }

                if let (Some(date), Some(interval)) = (left.as_date(), right.as_interval()) {
                    let negated = yachtsql_core::types::Interval {
                        months: -interval.months,
                        days: -interval.days,
                        micros: -interval.micros,
                    };
                    return self.add_interval_to_date(date, &negated);
                }

                if let (Some(a), Some(b)) = (left.as_i64(), right.as_i64()) {
                    a.checked_sub(b)
                        .map(Value::int64)
                        .ok_or_else(|| Error::ArithmeticOverflow {
                            operation: "subtraction".to_string(),
                            left: a.to_string(),
                            right: b.to_string(),
                        })
                } else if let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) {
                    Ok(Value::float64(a - b))
                } else if let (Some(a), Some(b)) = (left.as_i64(), right.as_f64()) {
                    Ok(Value::float64(a as f64 - b))
                } else if let (Some(a), Some(b)) = (left.as_f64(), right.as_i64()) {
                    Ok(Value::float64(a - b as f64))
                } else if let (Some(ts), Some(interval)) =
                    (left.as_timestamp(), right.as_interval())
                {
                    interval
                        .sub_from_timestamp(ts)
                        .map(Value::timestamp)
                        .ok_or_else(|| {
                            Error::invalid_query(
                                "Timestamp underflow in interval subtraction".to_string(),
                            )
                        })
                } else if let (Some(dt), Some(interval)) = (left.as_datetime(), right.as_interval())
                {
                    interval
                        .sub_from_timestamp(dt)
                        .map(Value::datetime)
                        .ok_or_else(|| {
                            Error::invalid_query(
                                "DateTime underflow in interval subtraction".to_string(),
                            )
                        })
                } else if let (Some(a), Some(b)) = (left.as_interval(), right.as_interval()) {
                    Ok(Value::interval(a.sub(b)))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "numeric types or timestamp/interval".to_string(),
                        actual: format!("{:?} - {:?}", left.data_type(), right.data_type()),
                    })
                }
            }
            BinaryOperator::Multiply => {
                if let (Some(a), Some(b)) = (left.as_i64(), right.as_i64()) {
                    a.checked_mul(b)
                        .map(Value::int64)
                        .ok_or_else(|| Error::ArithmeticOverflow {
                            operation: "multiplication".to_string(),
                            left: a.to_string(),
                            right: b.to_string(),
                        })
                } else if let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) {
                    Ok(Value::float64(a * b))
                } else if let (Some(a), Some(b)) = (left.as_i64(), right.as_f64()) {
                    Ok(Value::float64(a as f64 * b))
                } else if let (Some(a), Some(b)) = (left.as_f64(), right.as_i64()) {
                    Ok(Value::float64(a * b as f64))
                } else if let (Some(interval), Some(scalar)) = (left.as_interval(), right.as_i64())
                {
                    Ok(Value::interval(interval.mul(scalar)))
                } else if let (Some(scalar), Some(interval)) = (left.as_i64(), right.as_interval())
                {
                    Ok(Value::interval(interval.mul(scalar)))
                } else if let (Some(interval), Some(scalar)) = (left.as_interval(), right.as_f64())
                {
                    Ok(Value::interval(interval.mul(scalar as i64)))
                } else if let (Some(scalar), Some(interval)) = (left.as_f64(), right.as_interval())
                {
                    Ok(Value::interval(interval.mul(scalar as i64)))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "numeric types or interval * scalar".to_string(),
                        actual: format!("{:?} * {:?}", left.data_type(), right.data_type()),
                    })
                }
            }
            BinaryOperator::Divide => {
                if let (Some(a), Some(b)) = (left.as_i64(), right.as_i64()) {
                    if b == 0 {
                        Err(Error::ExecutionError("Division by zero".to_string()))
                    } else {
                        Ok(Value::int64(a / b))
                    }
                } else if let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) {
                    Ok(Value::float64(a / b))
                } else if let (Some(a), Some(b)) = (left.as_i64(), right.as_f64()) {
                    Ok(Value::float64(a as f64 / b))
                } else if let (Some(a), Some(b)) = (left.as_f64(), right.as_i64()) {
                    if b == 0 {
                        Ok(Value::float64(a / 0.0))
                    } else {
                        Ok(Value::float64(a / b as f64))
                    }
                } else if let (Some(interval), Some(scalar)) = (left.as_interval(), right.as_i64())
                {
                    if scalar == 0 {
                        Err(Error::ExecutionError("Division by zero".to_string()))
                    } else {
                        interval.div(scalar).map(Value::interval).ok_or_else(|| {
                            Error::ExecutionError("Interval division failed".to_string())
                        })
                    }
                } else if let (Some(interval), Some(scalar)) = (left.as_interval(), right.as_f64())
                {
                    if scalar == 0.0 {
                        Err(Error::ExecutionError("Division by zero".to_string()))
                    } else {
                        interval
                            .div(scalar as i64)
                            .map(Value::interval)
                            .ok_or_else(|| {
                                Error::ExecutionError("Interval division failed".to_string())
                            })
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "numeric types or interval / scalar".to_string(),
                        actual: format!("{:?} / {:?}", left.data_type(), right.data_type()),
                    })
                }
            }
            BinaryOperator::Modulo => {
                if let (Some(a), Some(b)) = (left.as_i64(), right.as_i64()) {
                    if b == 0 {
                        Err(Error::ExecutionError("Modulo by zero".to_string()))
                    } else {
                        Ok(Value::int64(a % b))
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "INT64".to_string(),
                        actual: format!("{:?} % {:?}", left.data_type(), right.data_type()),
                    })
                }
            }
            BinaryOperator::StringConcat => {
                if left.as_hstore().is_some() {
                    if right.as_hstore().is_some() {
                        yachtsql_functions::hstore::hstore_concat(left, right)
                    } else if let Some(s) = right.as_str() {
                        let right_hstore = yachtsql_functions::hstore::hstore_from_text(
                            &Value::string(s.to_string()),
                        )?;
                        yachtsql_functions::hstore::hstore_concat(left, &right_hstore)
                    } else {
                        Err(Error::TypeMismatch {
                            expected: "HSTORE or TEXT".to_string(),
                            actual: format!("{:?}", right.data_type()),
                        })
                    }
                } else if let (Some(l), Some(r)) = (left.as_bytes(), right.as_bytes()) {
                    let mut result = l.to_vec();
                    result.extend_from_slice(r);
                    Ok(Value::bytes(result))
                } else if left.as_str().is_some() || right.as_str().is_some() {
                    let left_str = self.value_to_string(left)?;
                    let right_str = self.value_to_string(right)?;
                    Ok(Value::string(format!("{}{}", left_str, right_str)))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING, BYTES, or HSTORE".to_string(),
                        actual: format!("{:?} || {:?}", left.data_type(), right.data_type()),
                    })
                }
            }
            BinaryOperator::AtArrow => {
                if left.as_hstore().is_some() {
                    yachtsql_functions::hstore::hstore_contains(left, right)
                } else if left.as_pgbox().is_some() || left.as_circle().is_some() {
                    yachtsql_functions::geometric::contains(left, right)
                } else {
                    yachtsql_functions::array::array_contains_array(left, right)
                }
            }
            BinaryOperator::ArrowAt => {
                if left.as_hstore().is_some() {
                    yachtsql_functions::hstore::hstore_contained_by(left, right)
                } else if left.as_point().is_some() {
                    yachtsql_functions::geometric::contained_by(left, right)
                } else {
                    yachtsql_functions::array::array_contained_by(left, right)
                }
            }
            BinaryOperator::PGOverlap => {
                if left.as_pgbox().is_some() || left.as_circle().is_some() {
                    yachtsql_functions::geometric::overlaps(left, right)
                } else {
                    yachtsql_functions::array::array_overlap(left, right)
                }
            }
            BinaryOperator::Arrow => {
                if left.as_hstore().is_some() {
                    if right.as_array().is_some() {
                        yachtsql_functions::hstore::hstore_get_values(left, right)
                    } else {
                        yachtsql_functions::hstore::hstore_get(left, right)
                    }
                } else if left.as_json().is_some() {
                    let key = right.as_str().ok_or_else(|| Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: format!("{:?}", right.data_type()),
                    })?;
                    yachtsql_functions::json::json_extract_json(left, key)
                } else {
                    Err(Error::TypeMismatch {
                        expected: "HSTORE or JSON".to_string(),
                        actual: format!("{:?}", left.data_type()),
                    })
                }
            }
            BinaryOperator::LongArrow => {
                if left.as_hstore().is_some() {
                    yachtsql_functions::hstore::hstore_get(left, right)
                } else if left.as_json().is_some() {
                    let key = right.as_str().ok_or_else(|| Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: format!("{:?}", right.data_type()),
                    })?;
                    yachtsql_functions::json::json_value_text(left, key)
                } else {
                    Err(Error::TypeMismatch {
                        expected: "HSTORE or JSON".to_string(),
                        actual: format!("{:?}", left.data_type()),
                    })
                }
            }

            BinaryOperator::LtDashGt => yachtsql_functions::geometric::distance(left, right),

            BinaryOperator::Question => {
                if left.as_hstore().is_some() {
                    yachtsql_functions::hstore::hstore_exists(left, right)
                } else {
                    Err(Error::TypeMismatch {
                        expected: "HSTORE".to_string(),
                        actual: format!("{:?}", left.data_type()),
                    })
                }
            }

            BinaryOperator::QuestionAnd => {
                if left.as_hstore().is_some() {
                    yachtsql_functions::hstore::hstore_exists_all(left, right)
                } else {
                    Err(Error::TypeMismatch {
                        expected: "HSTORE".to_string(),
                        actual: format!("{:?}", left.data_type()),
                    })
                }
            }

            BinaryOperator::QuestionPipe => {
                if left.as_hstore().is_some() {
                    yachtsql_functions::hstore::hstore_exists_any(left, right)
                } else {
                    Err(Error::TypeMismatch {
                        expected: "HSTORE".to_string(),
                        actual: format!("{:?}", left.data_type()),
                    })
                }
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Arithmetic operator {:?} not supported",
                op
            ))),
        }
    }

    fn value_to_bool(&self, value: &Value) -> Result<bool> {
        if let Some(b) = value.as_bool() {
            Ok(b)
        } else if let Some(i) = value.as_i64() {
            Ok(i != 0)
        } else if value.is_null() {
            Ok(false)
        } else {
            Err(Error::TypeMismatch {
                expected: "BOOL".to_string(),
                actual: format!("{:?}", value.data_type()),
            })
        }
    }

    fn negate_value(&self, value: &Value) -> Result<Value> {
        if let Some(i) = value.as_i64() {
            if i == i64::MIN {
                return Err(Error::ArithmeticOverflow {
                    operation: "negation".to_string(),
                    left: i.to_string(),
                    right: "0".to_string(),
                });
            }
            Ok(Value::int64(-i))
        } else if let Some(f) = value.as_f64() {
            if f == 9223372036854775808.0 {
                Ok(Value::int64(i64::MIN))
            } else {
                Ok(Value::float64(-f))
            }
        } else if value.is_null() {
            Ok(Value::null())
        } else {
            Err(Error::TypeMismatch {
                expected: "numeric type".to_string(),
                actual: format!("{:?}", value.data_type()),
            })
        }
    }

    fn apply_not(&self, value: &Value) -> Result<Value> {
        if let Some(b) = value.as_bool() {
            Ok(Value::bool_val(!b))
        } else if value.is_null() {
            Ok(Value::null())
        } else {
            Err(Error::TypeMismatch {
                expected: "BOOL".to_string(),
                actual: format!("{:?}", value.data_type()),
            })
        }
    }

    fn value_to_string(&self, value: &Value) -> Result<String> {
        if let Some(s) = value.as_str() {
            Ok(s.to_string())
        } else if let Some(i) = value.as_i64() {
            Ok(i.to_string())
        } else if let Some(f) = value.as_f64() {
            Ok(f.to_string())
        } else if let Some(n) = value.as_numeric() {
            Ok(n.to_string())
        } else if let Some(b) = value.as_bool() {
            Ok(b.to_string())
        } else if let Some(d) = value.as_date() {
            Ok(d.format("%Y-%m-%d").to_string())
        } else if let Some(ts) = value.as_timestamp() {
            Ok(ts.format("%Y-%m-%d %H:%M:%S").to_string())
        } else if let Some(dt) = value.as_datetime() {
            Ok(dt.format("%Y-%m-%d %H:%M:%S").to_string())
        } else if let Some(t) = value.as_time() {
            Ok(t.format("%H:%M:%S").to_string())
        } else if let Some(p) = value.as_point() {
            let x = if p.x.fract() == 0.0 {
                format!("{}", p.x as i64)
            } else {
                format!("{}", p.x)
            };
            let y = if p.y.fract() == 0.0 {
                format!("{}", p.y as i64)
            } else {
                format!("{}", p.y)
            };
            Ok(format!("({},{})", x, y))
        } else if let Some(b) = value.as_pgbox() {
            let x1 = if b.high.x.fract() == 0.0 {
                format!("{}", b.high.x as i64)
            } else {
                format!("{}", b.high.x)
            };
            let y1 = if b.high.y.fract() == 0.0 {
                format!("{}", b.high.y as i64)
            } else {
                format!("{}", b.high.y)
            };
            let x2 = if b.low.x.fract() == 0.0 {
                format!("{}", b.low.x as i64)
            } else {
                format!("{}", b.low.x)
            };
            let y2 = if b.low.y.fract() == 0.0 {
                format!("{}", b.low.y as i64)
            } else {
                format!("{}", b.low.y)
            };
            Ok(format!("(({},{}),({},{}))", x1, y1, x2, y2))
        } else if let Some(c) = value.as_circle() {
            let x = if c.center.x.fract() == 0.0 {
                format!("{}", c.center.x as i64)
            } else {
                format!("{}", c.center.x)
            };
            let y = if c.center.y.fract() == 0.0 {
                format!("{}", c.center.y as i64)
            } else {
                format!("{}", c.center.y)
            };
            let r = if c.radius.fract() == 0.0 {
                format!("{}", c.radius as i64)
            } else {
                format!("{}", c.radius)
            };
            Ok(format!("<({},{}),{}>", x, y, r))
        } else if value.is_null() {
            Ok(String::new())
        } else if value.as_bytes().is_some() {
            Err(Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: "BYTES".to_string(),
            })
        } else {
            Ok(format!("{:?}", value))
        }
    }

    fn cast_value(&self, value: Value, target_type: &sqlparser::ast::DataType) -> Result<Value> {
        use sqlparser::ast::DataType as SqlDataType;

        match target_type {
            SqlDataType::Int64
            | SqlDataType::BigInt(_)
            | SqlDataType::Int(_)
            | SqlDataType::Integer(_) => {
                if let Some(i) = value.as_i64() {
                    Ok(Value::int64(i))
                } else if let Some(f) = value.as_f64() {
                    if f.is_nan() || f.is_infinite() {
                        Err(Error::TypeCoercionError {
                            from_type: "FLOAT64".to_string(),
                            to_type: "INT64".to_string(),
                            reason: "Cannot CAST NaN or Infinity to INT64".to_string(),
                        })
                    } else if f < i64::MIN as f64 || f > i64::MAX as f64 {
                        Err(Error::TypeCoercionError {
                            from_type: "FLOAT64".to_string(),
                            to_type: "INT64".to_string(),
                            reason: format!("Value {} is out of range for INT64 (overflow)", f),
                        })
                    } else {
                        Ok(Value::int64(f as i64))
                    }
                } else if let Some(s) = value.as_str() {
                    s.parse::<i64>()
                        .map(Value::int64)
                        .map_err(|_| Error::InvalidQuery(format!("Cannot cast '{}' to INT64", s)))
                } else if let Some(b) = value.as_bool() {
                    Ok(Value::int64(if b { 1 } else { 0 }))
                } else if value.is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot cast {:?} to INT64",
                        value
                    )))
                }
            }
            SqlDataType::Float64
            | SqlDataType::Float(_)
            | SqlDataType::Double(_)
            | SqlDataType::DoublePrecision => {
                if let Some(f) = value.as_f64() {
                    Ok(Value::float64(f))
                } else if let Some(i) = value.as_i64() {
                    Ok(Value::float64(i as f64))
                } else if let Some(b) = value.as_bool() {
                    Ok(Value::float64(if b { 1.0 } else { 0.0 }))
                } else if let Some(s) = value.as_str() {
                    s.parse::<f64>()
                        .map(Value::float64)
                        .map_err(|_| Error::InvalidQuery(format!("Cannot cast '{}' to FLOAT64", s)))
                } else if value.is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot cast {:?} to FLOAT64",
                        value
                    )))
                }
            }
            SqlDataType::String(_) | SqlDataType::Varchar(_) | SqlDataType::Text => {
                if value.is_null() {
                    Ok(Value::null())
                } else if let Some(bytes) = value.as_bytes() {
                    String::from_utf8(bytes.to_vec())
                        .map(Value::string)
                        .map_err(|_| {
                            Error::InvalidQuery(
                                "Cannot cast BYTES to STRING: invalid UTF-8 sequence".to_string(),
                            )
                        })
                } else {
                    Ok(Value::string(self.value_to_string(&value)?))
                }
            }
            SqlDataType::Boolean | SqlDataType::Bool => {
                if let Some(b) = value.as_bool() {
                    Ok(Value::bool_val(b))
                } else if let Some(i) = value.as_i64() {
                    Ok(Value::bool_val(i != 0))
                } else if let Some(s) = value.as_str() {
                    let s_lower = s.to_lowercase();
                    if s_lower == "true" || s_lower == "t" || s_lower == "1" {
                        Ok(Value::bool_val(true))
                    } else if s_lower == "false" || s_lower == "f" || s_lower == "0" {
                        Ok(Value::bool_val(false))
                    } else {
                        Err(Error::InvalidQuery(format!(
                            "Cannot cast '{}' to BOOLEAN",
                            s
                        )))
                    }
                } else if value.is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot cast {:?} to BOOLEAN",
                        value
                    )))
                }
            }
            SqlDataType::Decimal(precision_info)
            | SqlDataType::Dec(precision_info)
            | SqlDataType::Numeric(precision_info) => {
                use std::str::FromStr;

                use rust_decimal::Decimal;

                let precision_scale = match precision_info {
                    sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                        Some(((*p) as u8, (*s) as u8))
                    }
                    sqlparser::ast::ExactNumberInfo::Precision(p) => Some(((*p) as u8, 0)),
                    sqlparser::ast::ExactNumberInfo::None => None,
                };

                if value.is_null() {
                    return Ok(Value::null());
                }

                let decimal = if let Some(n) = value.as_numeric() {
                    n
                } else if let Some(i) = value.as_i64() {
                    Decimal::from(i)
                } else if let Some(f) = value.as_f64() {
                    if f.is_nan() || f.is_infinite() {
                        return Err(Error::TypeCoercionError {
                            from_type: "FLOAT64".to_string(),
                            to_type: "NUMERIC".to_string(),
                            reason: format!(
                                "Cannot CAST {} to NUMERIC (NaN or Infinity not supported)",
                                if f.is_nan() { "NaN" } else { "Infinity" }
                            ),
                        });
                    }
                    Decimal::from_f64_retain(f).ok_or_else(|| {
                        Error::InvalidQuery(format!("Cannot convert FLOAT64 {} to NUMERIC", f))
                    })?
                } else if let Some(s) = value.as_str() {
                    Decimal::from_str(s.trim()).map_err(|_| {
                        Error::InvalidQuery(format!(
                            "Invalid numeric value: cannot cast '{}' to NUMERIC",
                            s
                        ))
                    })?
                } else if let Some(b) = value.as_bool() {
                    if b { Decimal::ONE } else { Decimal::ZERO }
                } else {
                    return Err(Error::InvalidQuery(format!(
                        "Cannot cast {:?} to NUMERIC",
                        value
                    )));
                };

                use crate::query_executor::execution::apply_numeric_precision_scale;
                let rounded = apply_numeric_precision_scale(decimal, &precision_scale)?;
                Ok(Value::numeric(rounded))
            }
            SqlDataType::Bytea
            | SqlDataType::Varbinary(_)
            | SqlDataType::Binary(_)
            | SqlDataType::Blob(_) => {
                if let Some(b) = value.as_bytes() {
                    Ok(Value::bytes(b.to_vec()))
                } else if let Some(s) = value.as_str() {
                    Ok(Value::bytes(s.as_bytes().to_vec()))
                } else if let Some(i) = value.as_i64() {
                    Ok(Value::bytes(i.to_be_bytes().to_vec()))
                } else if value.is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot cast {:?} to BYTES",
                        value
                    )))
                }
            }
            SqlDataType::Date => {
                if let Some(d) = value.as_date() {
                    Ok(Value::date(d))
                } else if let Some(s) = value.as_str() {
                    NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
                        .map(Value::date)
                        .map_err(|_| {
                            Error::InvalidOperation(format!("Cannot cast '{}' to DATE", s))
                        })
                } else if let Some(ts) = value.as_timestamp() {
                    Ok(Value::date(ts.date_naive()))
                } else if let Some(dt) = value.as_datetime() {
                    Ok(Value::date(dt.date_naive()))
                } else if value.is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot cast {:?} to DATE",
                        value
                    )))
                }
            }
            SqlDataType::Timestamp(_, _) => {
                if let Some(ts) = value.as_timestamp() {
                    Ok(Value::timestamp(ts))
                } else if let Some(s) = value.as_str() {
                    crate::types::parse_timestamp_to_utc(s.trim())
                        .map(Value::timestamp)
                        .ok_or_else(|| {
                            Error::InvalidOperation(format!("Cannot cast '{}' to TIMESTAMP", s))
                        })
                } else if let Some(d) = value.as_date() {
                    use chrono::NaiveTime;
                    let ndt = d.and_time(
                        NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is always valid"),
                    );
                    Ok(Value::timestamp(
                        DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc),
                    ))
                } else if let Some(dt) = value.as_datetime() {
                    Ok(Value::timestamp(dt))
                } else if value.is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot cast {:?} to TIMESTAMP",
                        value
                    )))
                }
            }
            SqlDataType::Interval { .. } => {
                if value.is_null() {
                    Ok(Value::null())
                } else if let Some(interval) = value.as_interval() {
                    Ok(Value::interval(interval.clone()))
                } else if let Some(s) = value.as_str() {
                    Self::parse_interval_string(s)
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot cast {:?} to INTERVAL",
                        value
                    )))
                }
            }

            SqlDataType::Custom(type_name, _modifiers) => {
                let type_name_str = type_name.to_string().to_uppercase();
                match type_name_str.as_str() {
                    "HSTORE" => {
                        if value.is_null() {
                            Ok(Value::null())
                        } else if let Some(hstore) = value.as_hstore() {
                            Ok(Value::hstore(hstore.clone()))
                        } else if let Some(s) = value.as_str() {
                            Self::parse_hstore_string(s)
                        } else {
                            Err(Error::InvalidQuery(format!(
                                "Cannot cast {:?} to HSTORE",
                                value
                            )))
                        }
                    }
                    "MACADDR" => {
                        if value.is_null() {
                            Ok(Value::null())
                        } else if let Some(mac) = value.as_macaddr() {
                            Ok(Value::macaddr(mac.clone()))
                        } else if let Some(s) = value.as_str() {
                            use yachtsql_core::types::MacAddress;
                            MacAddress::parse(s, false)
                                .map(Value::macaddr)
                                .ok_or_else(|| {
                                    Error::InvalidQuery(format!("Invalid MACADDR: '{}'", s))
                                })
                        } else {
                            Err(Error::InvalidQuery(format!(
                                "Cannot cast {:?} to MACADDR",
                                value
                            )))
                        }
                    }
                    "MACADDR8" => {
                        if value.is_null() {
                            Ok(Value::null())
                        } else if let Some(mac) = value.as_macaddr8() {
                            Ok(Value::macaddr8(mac.clone()))
                        } else if let Some(s) = value.as_str() {
                            use yachtsql_core::types::MacAddress;

                            MacAddress::parse(s, true)
                                .or_else(|| MacAddress::parse(s, false).map(|mac| mac.to_eui64()))
                                .map(Value::macaddr8)
                                .ok_or_else(|| {
                                    Error::InvalidQuery(format!("Invalid MACADDR8: '{}'", s))
                                })
                        } else {
                            Err(Error::InvalidQuery(format!(
                                "Cannot cast {:?} to MACADDR8",
                                value
                            )))
                        }
                    }
                    _ => {
                        let type_name_lower = type_name.to_string().to_lowercase();
                        let type_id = type_name.to_string();

                        if value.is_null() {
                            return Ok(Value::null());
                        }

                        if let Some(str_val) = value.as_str() {
                            let enum_labels = self.schema.fields().iter().find_map(|f| {
                                if let DataType::Enum {
                                    type_name: ft_name,
                                    labels,
                                } = &f.data_type
                                {
                                    if ft_name.to_lowercase() == type_name_lower {
                                        Some(labels.clone())
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            });

                            if let Some(labels) = enum_labels {
                                if labels.contains(&str_val.to_string()) {
                                    return Ok(value);
                                } else {
                                    return Err(Error::InvalidQuery(format!(
                                        "invalid input value for enum {}: \"{}\"",
                                        type_name_lower, str_val
                                    )));
                                }
                            } else {
                                return Ok(value);
                            }
                        }

                        if let Some(source_struct) = value.as_struct() {
                            let user_type = if let Some(registry) = self.type_registry {
                                registry.get_type(&type_id)
                            } else if let Some(registry) = &self.owned_type_registry {
                                registry.get_type(&type_id)
                            } else {
                                return Err(Error::InternalError(
                                    "Type registry not available for composite type cast"
                                        .to_string(),
                                ));
                            }
                            .ok_or_else(|| {
                                Error::InvalidQuery(format!("Type '{}' does not exist", type_id))
                            })?;

                            let yachtsql_storage::TypeDefinition::Composite(fields) =
                                &user_type.definition;

                            if source_struct.len() != fields.len() {
                                return Err(Error::InvalidQuery(format!(
                                    "Cannot cast ROW with {} fields to type '{}' which has {} fields",
                                    source_struct.len(),
                                    type_id,
                                    fields.len()
                                )));
                            }

                            let mut result_map = indexmap::IndexMap::new();

                            for (idx, field_def) in fields.iter().enumerate() {
                                let source_value = if let Some(v) =
                                    source_struct.get(&field_def.name)
                                {
                                    v.clone()
                                } else {
                                    let anon_name = format!("f{}", idx + 1);
                                    source_struct.get(&anon_name).ok_or_else(|| {
                                        Error::InvalidQuery(format!(
                                            "Cannot find field '{}' or '{}' in source struct for cast to type '{}'",
                                            field_def.name, anon_name, type_id
                                        ))
                                    })?.clone()
                                };

                                let coerced_value = yachtsql_core::types::conversion::coerce_value(
                                    source_value,
                                    &field_def.data_type,
                                )?;

                                result_map.insert(field_def.name.clone(), coerced_value);
                            }

                            return Ok(Value::struct_val(result_map));
                        }

                        Err(Error::InvalidQuery(format!(
                            "Cannot cast {:?} to {}",
                            value, type_name_lower
                        )))
                    }
                }
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "CAST to {:?} not yet supported",
                target_type
            ))),
        }
    }

    fn evaluate_function(&self, func: &sqlparser::ast::Function, row: &Row) -> Result<Value> {
        use sqlparser::ast::{FunctionArgumentList, FunctionArguments};

        let func_name = func.name.to_string().to_uppercase();

        let args = match &func.args {
            FunctionArguments::List(FunctionArgumentList { args, .. }) => args,
            _ => {
                return Err(Error::UnsupportedFeature(
                    "Non-list function arguments not supported".to_string(),
                ));
            }
        };

        match func_name.as_str() {
            "ROW" => {
                let mut struct_map = indexmap::IndexMap::new();
                for (idx, arg) in args.iter().enumerate() {
                    let value = self.evaluate_function_arg(arg, row)?;
                    struct_map.insert(format!("f{}", idx + 1), value);
                }
                Ok(Value::struct_val(struct_map))
            }
            "UPPER" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "UPPER() requires exactly 1 argument".to_string(),
                    ));
                }
                let arg = self.evaluate_function_arg(&args[0], row)?;
                let s = self.value_to_string(&arg)?;
                Ok(Value::string(s.to_uppercase()))
            }
            "LOWER" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "LOWER() requires exactly 1 argument".to_string(),
                    ));
                }
                let arg = self.evaluate_function_arg(&args[0], row)?;
                let s = self.value_to_string(&arg)?;
                Ok(Value::string(s.to_lowercase()))
            }

            "CASEFOLD" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "CASEFOLD() requires exactly 1 argument".to_string(),
                    ));
                }
                let arg = self.evaluate_function_arg(&args[0], row)?;
                if arg.is_null() {
                    return Ok(Value::null());
                }
                let s = self.value_to_string(&arg)?;

                let folded: String = s
                    .chars()
                    .flat_map(|c| match c {
                        'ß' => vec!['s', 's'],
                        'ẞ' => vec!['s', 's'],
                        'İ' => vec!['i', '\u{0307}'],
                        _ => c.to_lowercase().collect::<Vec<_>>(),
                    })
                    .collect();
                Ok(Value::string(folded))
            }
            "LEFT" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "LEFT() requires exactly 2 arguments".to_string(),
                    ));
                }
                let string = self.evaluate_function_arg(&args[0], row)?;
                let n = self.evaluate_function_arg(&args[1], row)?;

                if string.is_null() || n.is_null() {
                    return Ok(Value::null());
                }

                let s = self.value_to_string(&string)?;
                if let Some(count) = n.as_i64() {
                    let count = count.max(0) as usize;
                    Ok(Value::string(s.chars().take(count).collect()))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "INT64".to_string(),
                        actual: n.data_type().to_string(),
                    })
                }
            }
            "RIGHT" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "RIGHT() requires exactly 2 arguments".to_string(),
                    ));
                }
                let string = self.evaluate_function_arg(&args[0], row)?;
                let n = self.evaluate_function_arg(&args[1], row)?;

                if string.is_null() || n.is_null() {
                    return Ok(Value::null());
                }

                let s = self.value_to_string(&string)?;
                if let Some(count) = n.as_i64() {
                    let count = count.max(0) as usize;
                    let chars: Vec<char> = s.chars().collect();
                    let start = if count >= chars.len() {
                        0
                    } else {
                        chars.len() - count
                    };
                    Ok(Value::string(chars[start..].iter().collect()))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "INT64".to_string(),
                        actual: n.data_type().to_string(),
                    })
                }
            }
            "CHR" | "CHAR" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(format!(
                        "{}() requires exactly 1 argument",
                        func_name
                    )));
                }
                let arg = self.evaluate_function_arg(&args[0], row)?;

                if arg.is_null() {
                    return Ok(Value::null());
                }

                if let Some(code) = arg.as_i64() {
                    if code < 0 || code > 1114111 {
                        return Err(Error::InvalidQuery(format!(
                            "Character code {} is out of range",
                            code
                        )));
                    }
                    match char::from_u32(code as u32) {
                        Some(c) => Ok(Value::string(c.to_string())),
                        None => Err(Error::InvalidQuery(format!(
                            "Invalid character code: {}",
                            code
                        ))),
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "INT64".to_string(),
                        actual: arg.data_type().to_string(),
                    })
                }
            }
            "INITCAP" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "INITCAP() requires exactly 1 argument".to_string(),
                    ));
                }
                let arg = self.evaluate_function_arg(&args[0], row)?;

                if arg.is_null() {
                    return Ok(Value::null());
                }

                let s = self.value_to_string(&arg)?;
                let mut result = String::new();
                let mut capitalize_next = true;

                for ch in s.chars() {
                    if ch.is_alphabetic() {
                        if capitalize_next {
                            result.push_str(&ch.to_uppercase().to_string());
                            capitalize_next = false;
                        } else {
                            result.push_str(&ch.to_lowercase().to_string());
                        }
                    } else {
                        result.push(ch);
                        capitalize_next = true;
                    }
                }

                Ok(Value::string(result))
            }
            "CONCAT" => {
                let mut result = String::new();
                for arg in args {
                    let value = self.evaluate_function_arg(arg, row)?;
                    result.push_str(&self.value_to_string(&value)?);
                }
                Ok(Value::string(result))
            }
            "LENGTH" | "LEN" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(format!(
                        "{}() requires exactly 1 argument",
                        func_name
                    )));
                }
                let arg = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::scalar::eval_length(&arg)
            }
            "SUBSTRING" | "SUBSTR" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "SUBSTRING() requires 2 or 3 arguments".to_string(),
                    ));
                }
                let string_val = self.evaluate_function_arg(&args[0], row)?;
                let s = self.value_to_string(&string_val)?;

                let start_val = self.evaluate_function_arg(&args[1], row)?;
                let start = if let Some(i) = start_val.as_i64() {
                    if i < 1 { 0 } else { (i - 1) as usize }
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "INT64".to_string(),
                        actual: format!("{:?}", start_val.data_type()),
                    });
                };

                let result = if args.len() == 3 {
                    let len_val = self.evaluate_function_arg(&args[2], row)?;
                    let len = if let Some(i) = len_val.as_i64() {
                        i as usize
                    } else {
                        return Err(Error::TypeMismatch {
                            expected: "INT64".to_string(),
                            actual: format!("{:?}", len_val.data_type()),
                        });
                    };
                    let end = (start + len).min(s.len());
                    s.chars().skip(start).take(end - start).collect()
                } else {
                    s.chars().skip(start).collect()
                };

                Ok(Value::string(result))
            }
            "INSTR" | "STRPOS" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(format!(
                        "{}() requires exactly 2 arguments",
                        func_name
                    )));
                }
                let haystack_val = self.evaluate_function_arg(&args[0], row)?;
                let needle_val = self.evaluate_function_arg(&args[1], row)?;
                let haystack = self.value_to_string(&haystack_val)?;
                let needle = self.value_to_string(&needle_val)?;

                let position = haystack.find(&needle).map(|pos| pos + 1).unwrap_or(0);
                Ok(Value::int64(position as i64))
            }
            "COALESCE" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "COALESCE() requires at least 1 argument".to_string(),
                    ));
                }

                for arg in args {
                    let value = self.evaluate_function_arg(arg, row)?;
                    if !value.is_null() {
                        return Ok(value);
                    }
                }

                Ok(Value::null())
            }
            "NULLIF" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "NULLIF() requires exactly 2 arguments".to_string(),
                    ));
                }
                let value1 = self.evaluate_function_arg(&args[0], row)?;
                let value2 = self.evaluate_function_arg(&args[1], row)?;

                if self.values_equal(&value1, &value2)? {
                    Ok(Value::null())
                } else {
                    Ok(value1)
                }
            }
            "GREATEST" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "GREATEST() requires at least 1 argument".to_string(),
                    ));
                }

                let mut max_value: Option<Value> = None;
                for arg in args {
                    let value = self.evaluate_function_arg(arg, row)?;
                    if value.is_null() {
                        continue;
                    }
                    match &max_value {
                        None => max_value = Some(value),
                        Some(current_max) => {
                            if self.compare_values(&value, current_max)?
                                == std::cmp::Ordering::Greater
                            {
                                max_value = Some(value);
                            }
                        }
                    }
                }

                Ok(max_value.unwrap_or(Value::null()))
            }
            "LEAST" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "LEAST() requires at least 1 argument".to_string(),
                    ));
                }

                let mut min_value: Option<Value> = None;
                for arg in args {
                    let value = self.evaluate_function_arg(arg, row)?;
                    if value.is_null() {
                        continue;
                    }
                    match &min_value {
                        None => min_value = Some(value),
                        Some(current_min) => {
                            if self.compare_values(&value, current_min)? == std::cmp::Ordering::Less
                            {
                                min_value = Some(value);
                            }
                        }
                    }
                }

                Ok(min_value.unwrap_or(Value::null()))
            }
            "IFNULL" | "NVL" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(format!(
                        "{}() requires exactly 2 arguments",
                        func_name
                    )));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    self.evaluate_function_arg(&args[1], row)
                } else {
                    Ok(value)
                }
            }
            "IF" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery(
                        "IF() requires exactly 3 arguments".to_string(),
                    ));
                }
                let condition = self.evaluate_function_arg(&args[0], row)?;

                let is_true = condition.as_bool().unwrap_or(false);
                if is_true {
                    self.evaluate_function_arg(&args[1], row)
                } else {
                    self.evaluate_function_arg(&args[2], row)
                }
            }
            "IIF" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery(
                        "IIF() requires exactly 3 arguments".to_string(),
                    ));
                }
                let condition = self.evaluate_function_arg(&args[0], row)?;
                let is_true = if condition.is_null() {
                    false
                } else if let Some(b) = condition.as_bool() {
                    b
                } else if let Some(i) = condition.as_i64() {
                    i != 0
                } else if let Some(f) = condition.as_f64() {
                    f != 0.0
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "BOOL or numeric".to_string(),
                        actual: condition.data_type().to_string(),
                    });
                };
                if is_true {
                    self.evaluate_function_arg(&args[1], row)
                } else {
                    self.evaluate_function_arg(&args[2], row)
                }
            }
            "DECODE" => {
                if args.len() == 2 {
                    let text_val = self.evaluate_function_arg(&args[0], row)?;
                    let format_val = self.evaluate_function_arg(&args[1], row)?;
                    if text_val.is_null() || format_val.is_null() {
                        return Ok(Value::null());
                    }

                    if let Some(format) = format_val.as_str() {
                        let format_lower = format.to_lowercase();
                        if format_lower == "hex"
                            || format_lower == "base64"
                            || format_lower == "escape"
                        {
                            let text = text_val.as_str().ok_or_else(|| Error::TypeMismatch {
                                expected: "STRING".to_string(),
                                actual: text_val.data_type().to_string(),
                            })?;
                            let decoded = match format_lower.as_str() {
                                "hex" => hex::decode(text).map_err(|e| {
                                    Error::invalid_query(format!(
                                        "DECODE: invalid hex string: {}",
                                        e
                                    ))
                                })?,
                                "base64" => {
                                    use base64::Engine as _;
                                    use base64::engine::general_purpose::STANDARD;
                                    STANDARD.decode(text).map_err(|e| {
                                        Error::invalid_query(format!(
                                            "DECODE: invalid base64 string: {}",
                                            e
                                        ))
                                    })?
                                }
                                "escape" => {
                                    let mut result = Vec::new();
                                    let mut chars = text.chars().peekable();
                                    while let Some(c) = chars.next() {
                                        if c == '\\' {
                                            let mut octal = String::new();
                                            for _ in 0..3 {
                                                if let Some(&next) = chars.peek()
                                                    && next >= '0'
                                                    && next <= '7'
                                                {
                                                    octal.push(chars.next().unwrap());
                                                } else {
                                                    break;
                                                }
                                            }
                                            if !octal.is_empty() {
                                                let byte = u8::from_str_radix(&octal, 8).map_err(
                                                    |_| {
                                                        Error::invalid_query(format!(
                                                            "DECODE: invalid escape sequence \\{}",
                                                            octal
                                                        ))
                                                    },
                                                )?;
                                                result.push(byte);
                                            } else if let Some(&next) = chars.peek() {
                                                if next == '\\' {
                                                    chars.next();
                                                    result.push(b'\\');
                                                } else {
                                                    result.push(b'\\');
                                                }
                                            } else {
                                                result.push(b'\\');
                                            }
                                        } else {
                                            result.push(c as u8);
                                        }
                                    }
                                    result
                                }
                                _ => unreachable!(),
                            };
                            return Ok(Value::bytes(decoded));
                        }
                    }
                }

                if args.len() < 3 {
                    return Err(Error::InvalidQuery(
                        "DECODE() requires at least 3 arguments for conditional decode, or 2 arguments for format decode (text, 'hex'|'base64'|'escape')".to_string(),
                    ));
                }
                let expr = self.evaluate_function_arg(&args[0], row)?;

                let pairs_start = 1;
                let remaining = args.len() - 1;
                let has_default = remaining % 2 == 1;
                let pairs_count = remaining / 2;

                for i in 0..pairs_count {
                    let search_idx = pairs_start + i * 2;
                    let result_idx = pairs_start + i * 2 + 1;
                    let search = self.evaluate_function_arg(&args[search_idx], row)?;

                    let matches = if expr.is_null() && search.is_null() {
                        true
                    } else if expr.is_null() || search.is_null() {
                        false
                    } else {
                        self.values_equal(&expr, &search)?
                    };

                    if matches {
                        return self.evaluate_function_arg(&args[result_idx], row);
                    }
                }

                if has_default {
                    self.evaluate_function_arg(&args[args.len() - 1], row)
                } else {
                    Ok(Value::null())
                }
            }
            "ISNULL" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "ISNULL() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                Ok(Value::bool_val(value.is_null()))
            }
            "ABS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "ABS() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(n) = value.as_i64() {
                    Ok(Value::int64(n.abs()))
                } else if let Some(f) = value.as_f64() {
                    Ok(Value::float64(f.abs()))
                } else {
                    Err(Error::InvalidQuery(
                        "ABS() requires a numeric argument".to_string(),
                    ))
                }
            }
            "SIGN" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "SIGN() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(n) = value.as_i64() {
                    Ok(Value::int64(if n > 0 {
                        1
                    } else if n < 0 {
                        -1
                    } else {
                        0
                    }))
                } else if let Some(f) = value.as_f64() {
                    Ok(Value::int64(if f > 0.0 {
                        1
                    } else if f < 0.0 {
                        -1
                    } else {
                        0
                    }))
                } else {
                    Err(Error::InvalidQuery(
                        "SIGN() requires a numeric argument".to_string(),
                    ))
                }
            }
            "ROUND" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::InvalidQuery(
                        "ROUND() requires 1 or 2 arguments".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                let decimals = if args.len() == 2 {
                    let d_val = self.evaluate_function_arg(&args[1], row)?;
                    if let Some(d) = d_val.as_i64() {
                        d
                    } else {
                        return Err(Error::InvalidQuery(
                            "ROUND() decimals must be an integer".to_string(),
                        ));
                    }
                } else {
                    0
                };

                if value.is_null() {
                    return Ok(Value::null());
                }

                if let Some(f) = value.as_f64() {
                    let multiplier = 10_f64.powi(decimals as i32);
                    Ok(Value::float64((f * multiplier).round() / multiplier))
                } else if let Some(n) = value.as_i64() {
                    if decimals >= 0 {
                        Ok(Value::int64(n))
                    } else {
                        let divisor = 10_i64.pow((-decimals) as u32);
                        Ok(Value::int64(
                            ((n as f64 / divisor as f64).round() as i64) * divisor,
                        ))
                    }
                } else {
                    Err(Error::InvalidQuery(
                        "ROUND() requires a numeric argument".to_string(),
                    ))
                }
            }
            "CEIL" | "CEILING" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(format!(
                        "{}() requires exactly 1 argument",
                        func_name
                    )));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(f) = value.as_f64() {
                    Ok(Value::float64(f.ceil()))
                } else if let Some(n) = value.as_i64() {
                    Ok(Value::int64(n))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "{}() requires a numeric argument",
                        func_name
                    )))
                }
            }
            "FLOOR" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "FLOOR() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(f) = value.as_f64() {
                    Ok(Value::float64(f.floor()))
                } else if let Some(n) = value.as_i64() {
                    Ok(Value::int64(n))
                } else {
                    Err(Error::InvalidQuery(
                        "FLOOR() requires a numeric argument".to_string(),
                    ))
                }
            }
            "TRUNC" | "TRUNCATE" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::InvalidQuery(format!(
                        "{}() requires 1 or 2 arguments",
                        func_name
                    )));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }

                let precision = if args.len() == 2 {
                    let prec_val = self.evaluate_function_arg(&args[1], row)?;
                    if prec_val.is_null() {
                        return Ok(Value::null());
                    }
                    prec_val.as_i64().ok_or_else(|| {
                        Error::InvalidQuery(format!("{}() precision must be an integer", func_name))
                    })?
                } else {
                    0
                };

                if let Some(f) = value.as_f64() {
                    if precision == 0 {
                        Ok(Value::float64(f.trunc()))
                    } else {
                        let multiplier = 10_f64.powi(precision as i32);
                        Ok(Value::float64((f * multiplier).trunc() / multiplier))
                    }
                } else if let Some(n) = value.as_i64() {
                    Ok(Value::int64(n))
                } else if let Some(mac) = value.as_macaddr() {
                    Ok(Value::macaddr(mac.trunc()))
                } else if let Some(mac) = value.as_macaddr8() {
                    Ok(Value::macaddr8(mac.trunc()))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "{}() requires a numeric argument",
                        func_name
                    )))
                }
            }
            "SQRT" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "SQRT() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let num = if let Some(f) = value.as_f64() {
                    f
                } else if let Some(n) = value.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "SQRT() requires a numeric argument".to_string(),
                    ));
                };
                if num < 0.0 {
                    return Err(Error::InvalidQuery(
                        "SQRT input must be non-negative".to_string(),
                    ));
                }
                Ok(Value::float64(num.sqrt()))
            }
            "CBRT" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "CBRT() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let num = if let Some(f) = value.as_f64() {
                    f
                } else if let Some(n) = value.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "CBRT() requires a numeric argument".to_string(),
                    ));
                };
                Ok(Value::float64(num.cbrt()))
            }
            "POWER" | "POW" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(format!(
                        "{}() requires exactly 2 arguments",
                        func_name
                    )));
                }
                let base = self.evaluate_function_arg(&args[0], row)?;
                let exponent = self.evaluate_function_arg(&args[1], row)?;
                if base.is_null() || exponent.is_null() {
                    return Ok(Value::null());
                }
                let base_num = if let Some(f) = base.as_f64() {
                    f
                } else if let Some(n) = base.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(format!(
                        "{}() requires numeric arguments",
                        func_name
                    )));
                };
                let exp_num = if let Some(f) = exponent.as_f64() {
                    f
                } else if let Some(n) = exponent.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(format!(
                        "{}() requires numeric arguments",
                        func_name
                    )));
                };
                if base_num < 0.0 && exp_num.fract() != 0.0 {
                    return Err(Error::InvalidQuery(
                        "Cannot raise negative number to fractional power (would produce complex result)".to_string(),
                    ));
                }
                Ok(Value::float64(base_num.powf(exp_num)))
            }
            "EXP" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "EXP() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let num = if let Some(f) = value.as_f64() {
                    f
                } else if let Some(n) = value.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "EXP() requires a numeric argument".to_string(),
                    ));
                };
                Ok(Value::float64(num.exp()))
            }
            "LN" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "LN() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let num = if let Some(f) = value.as_f64() {
                    f
                } else if let Some(n) = value.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "LN() requires a numeric argument".to_string(),
                    ));
                };
                if num <= 0.0 {
                    return Err(Error::InvalidQuery("LN input must be positive".to_string()));
                }
                Ok(Value::float64(num.ln()))
            }
            "LOG" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::InvalidQuery(
                        "LOG() requires 1 or 2 arguments".to_string(),
                    ));
                }
                if args.len() == 1 {
                    let value = self.evaluate_function_arg(&args[0], row)?;
                    if value.is_null() {
                        return Ok(Value::null());
                    }
                    let num = if let Some(f) = value.as_f64() {
                        f
                    } else if let Some(n) = value.as_i64() {
                        n as f64
                    } else {
                        return Err(Error::InvalidQuery(
                            "LOG() requires a numeric argument".to_string(),
                        ));
                    };
                    if num <= 0.0 {
                        return Err(Error::InvalidQuery(
                            "LOG input must be positive".to_string(),
                        ));
                    }
                    Ok(Value::float64(num.ln()))
                } else {
                    let base = self.evaluate_function_arg(&args[0], row)?;
                    let value = self.evaluate_function_arg(&args[1], row)?;
                    if base.is_null() || value.is_null() {
                        return Ok(Value::null());
                    }
                    let base_num = if let Some(f) = base.as_f64() {
                        f
                    } else if let Some(n) = base.as_i64() {
                        n as f64
                    } else {
                        return Err(Error::InvalidQuery(
                            "LOG() requires numeric arguments".to_string(),
                        ));
                    };
                    let val_num = if let Some(f) = value.as_f64() {
                        f
                    } else if let Some(n) = value.as_i64() {
                        n as f64
                    } else {
                        return Err(Error::InvalidQuery(
                            "LOG() requires numeric arguments".to_string(),
                        ));
                    };
                    if base_num <= 0.0 || base_num == 1.0 {
                        return Err(Error::InvalidQuery(
                            "LOG base must be positive and not equal to 1".to_string(),
                        ));
                    }
                    if val_num <= 0.0 {
                        return Err(Error::InvalidQuery(
                            "LOG input must be positive".to_string(),
                        ));
                    }
                    Ok(Value::float64(val_num.log(base_num)))
                }
            }
            "LOG10" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "LOG10() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let num = if let Some(f) = value.as_f64() {
                    f
                } else if let Some(n) = value.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "LOG10() requires a numeric argument".to_string(),
                    ));
                };
                if num <= 0.0 {
                    return Err(Error::InvalidQuery(
                        "LOG10 input must be positive".to_string(),
                    ));
                }
                Ok(Value::float64(num.log10()))
            }
            "LOG2" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "LOG2() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let num = if let Some(f) = value.as_f64() {
                    f
                } else if let Some(n) = value.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "LOG2() requires a numeric argument".to_string(),
                    ));
                };
                if num <= 0.0 {
                    return Err(Error::InvalidQuery(
                        "LOG2 input must be positive".to_string(),
                    ));
                }
                Ok(Value::float64(num.log2()))
            }
            "MOD" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "MOD() requires exactly 2 arguments".to_string(),
                    ));
                }
                let dividend = self.evaluate_function_arg(&args[0], row)?;
                let divisor = self.evaluate_function_arg(&args[1], row)?;
                if dividend.is_null() || divisor.is_null() {
                    return Ok(Value::null());
                }

                if let (Some(a), Some(b)) = (dividend.as_i64(), divisor.as_i64()) {
                    if b == 0 {
                        return Err(Error::InvalidQuery("Division by zero".to_string()));
                    }
                    Ok(Value::int64(a % b))
                } else {
                    let a = if let Some(f) = dividend.as_f64() {
                        f
                    } else if let Some(n) = dividend.as_i64() {
                        n as f64
                    } else {
                        return Err(Error::InvalidQuery(
                            "MOD() requires numeric arguments".to_string(),
                        ));
                    };
                    let b = if let Some(f) = divisor.as_f64() {
                        f
                    } else if let Some(n) = divisor.as_i64() {
                        n as f64
                    } else {
                        return Err(Error::InvalidQuery(
                            "MOD() requires numeric arguments".to_string(),
                        ));
                    };
                    if b == 0.0 {
                        return Err(Error::InvalidQuery("Division by zero".to_string()));
                    }
                    Ok(Value::float64(a % b))
                }
            }
            "SIN" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "SIN() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let num = if let Some(f) = value.as_f64() {
                    f
                } else if let Some(n) = value.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "SIN() requires a numeric argument".to_string(),
                    ));
                };
                Ok(Value::float64(num.sin()))
            }
            "COS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "COS() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let num = if let Some(f) = value.as_f64() {
                    f
                } else if let Some(n) = value.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "COS() requires a numeric argument".to_string(),
                    ));
                };
                Ok(Value::float64(num.cos()))
            }
            "TAN" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "TAN() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let num = if let Some(f) = value.as_f64() {
                    f
                } else if let Some(n) = value.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "TAN() requires a numeric argument".to_string(),
                    ));
                };
                Ok(Value::float64(num.tan()))
            }
            "ASIN" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "ASIN() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let num = if let Some(f) = value.as_f64() {
                    f
                } else if let Some(n) = value.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "ASIN() requires a numeric argument".to_string(),
                    ));
                };
                if !(-1.0..=1.0).contains(&num) {
                    return Err(Error::InvalidQuery(
                        "ASIN input must be in range [-1, 1]".to_string(),
                    ));
                }
                Ok(Value::float64(num.asin()))
            }
            "ACOS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "ACOS() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let num = if let Some(f) = value.as_f64() {
                    f
                } else if let Some(n) = value.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "ACOS() requires a numeric argument".to_string(),
                    ));
                };
                if !(-1.0..=1.0).contains(&num) {
                    return Err(Error::InvalidQuery(
                        "ACOS input must be in range [-1, 1]".to_string(),
                    ));
                }
                Ok(Value::float64(num.acos()))
            }
            "ATAN" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "ATAN() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let num = if let Some(f) = value.as_f64() {
                    f
                } else if let Some(n) = value.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "ATAN() requires a numeric argument".to_string(),
                    ));
                };
                Ok(Value::float64(num.atan()))
            }
            "ATAN2" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "ATAN2() requires exactly 2 arguments".to_string(),
                    ));
                }
                let y = self.evaluate_function_arg(&args[0], row)?;
                let x = self.evaluate_function_arg(&args[1], row)?;
                if y.is_null() || x.is_null() {
                    return Ok(Value::null());
                }
                let y_num = if let Some(f) = y.as_f64() {
                    f
                } else if let Some(n) = y.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "ATAN2() requires numeric arguments".to_string(),
                    ));
                };
                let x_num = if let Some(f) = x.as_f64() {
                    f
                } else if let Some(n) = x.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "ATAN2() requires numeric arguments".to_string(),
                    ));
                };
                Ok(Value::float64(y_num.atan2(x_num)))
            }
            "SINH" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "SINH() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let num = if let Some(f) = value.as_f64() {
                    f
                } else if let Some(n) = value.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "SINH() requires a numeric argument".to_string(),
                    ));
                };
                Ok(Value::float64(num.sinh()))
            }
            "COSH" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "COSH() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let num = if let Some(f) = value.as_f64() {
                    f
                } else if let Some(n) = value.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "COSH() requires a numeric argument".to_string(),
                    ));
                };
                Ok(Value::float64(num.cosh()))
            }
            "TANH" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "TANH() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let num = if let Some(f) = value.as_f64() {
                    f
                } else if let Some(n) = value.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "TANH() requires a numeric argument".to_string(),
                    ));
                };
                Ok(Value::float64(num.tanh()))
            }
            "RADIANS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "RADIANS() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let num = if let Some(f) = value.as_f64() {
                    f
                } else if let Some(n) = value.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "RADIANS() requires a numeric argument".to_string(),
                    ));
                };
                Ok(Value::float64(num.to_radians()))
            }
            "DEGREES" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "DEGREES() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                let num = if let Some(f) = value.as_f64() {
                    f
                } else if let Some(n) = value.as_i64() {
                    n as f64
                } else {
                    return Err(Error::InvalidQuery(
                        "DEGREES() requires a numeric argument".to_string(),
                    ));
                };
                Ok(Value::float64(num.to_degrees()))
            }
            "PI" => {
                if !args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "PI() requires no arguments".to_string(),
                    ));
                }
                Ok(Value::float64(std::f64::consts::PI))
            }
            "E" => {
                if !args.is_empty() {
                    return Err(Error::InvalidQuery("E() requires no arguments".to_string()));
                }
                Ok(Value::float64(std::f64::consts::E))
            }
            "RANDOM" | "RAND" => {
                if !args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "RANDOM() requires no arguments".to_string(),
                    ));
                }
                use rand::Rng;
                let mut rng = rand::thread_rng();
                Ok(Value::float64(rng.r#gen::<f64>()))
            }

            "GEN_RANDOM_UUID" | "UUID_GENERATE_V4" | "UUIDV4" => {
                if !args.is_empty() {
                    return Err(Error::InvalidQuery(format!(
                        "{}() requires no arguments",
                        func_name
                    )));
                }
                use rand::Rng;
                let mut rng = rand::thread_rng();
                let mut bytes = [0u8; 16];
                rng.fill(&mut bytes);

                bytes[6] = (bytes[6] & 0x0f) | 0x40;

                bytes[8] = (bytes[8] & 0x3f) | 0x80;
                let uuid = format!(
                    "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    bytes[0],
                    bytes[1],
                    bytes[2],
                    bytes[3],
                    bytes[4],
                    bytes[5],
                    bytes[6],
                    bytes[7],
                    bytes[8],
                    bytes[9],
                    bytes[10],
                    bytes[11],
                    bytes[12],
                    bytes[13],
                    bytes[14],
                    bytes[15]
                );
                Ok(Value::string(uuid))
            }

            "UUIDV7" => {
                if !args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "UUIDV7() requires no arguments".to_string(),
                    ));
                }
                use std::time::{SystemTime, UNIX_EPOCH};

                use rand::Rng;

                let mut rng = rand::thread_rng();
                let mut bytes = [0u8; 16];

                let timestamp_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                bytes[0] = ((timestamp_ms >> 40) & 0xFF) as u8;
                bytes[1] = ((timestamp_ms >> 32) & 0xFF) as u8;
                bytes[2] = ((timestamp_ms >> 24) & 0xFF) as u8;
                bytes[3] = ((timestamp_ms >> 16) & 0xFF) as u8;
                bytes[4] = ((timestamp_ms >> 8) & 0xFF) as u8;
                bytes[5] = (timestamp_ms & 0xFF) as u8;

                rng.fill(&mut bytes[6..]);

                bytes[6] = (bytes[6] & 0x0f) | 0x70;

                bytes[8] = (bytes[8] & 0x3f) | 0x80;

                let uuid = format!(
                    "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    bytes[0],
                    bytes[1],
                    bytes[2],
                    bytes[3],
                    bytes[4],
                    bytes[5],
                    bytes[6],
                    bytes[7],
                    bytes[8],
                    bytes[9],
                    bytes[10],
                    bytes[11],
                    bytes[12],
                    bytes[13],
                    bytes[14],
                    bytes[15]
                );
                Ok(Value::string(uuid))
            }
            "UUID_NIL" => {
                if !args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "UUID_NIL() requires no arguments".to_string(),
                    ));
                }
                Ok(Value::string(
                    "00000000-0000-0000-0000-000000000000".to_string(),
                ))
            }

            "GEN_RANDOM_BYTES" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "GEN_RANDOM_BYTES() requires exactly 1 argument".to_string(),
                    ));
                }
                let length_val = self.evaluate_function_arg(&args[0], row)?;
                if length_val.is_null() {
                    return Ok(Value::null());
                }
                let length = length_val.as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: length_val.data_type().to_string(),
                })?;
                if length < 0 || length > 1024 * 1024 {
                    return Err(Error::invalid_query(format!(
                        "GEN_RANDOM_BYTES: length must be between 0 and 1048576, got {}",
                        length
                    )));
                }
                use rand::Rng;
                let mut rng = rand::thread_rng();
                let bytes: Vec<u8> = (0..length).map(|_| rng.r#gen()).collect();
                Ok(Value::bytes(bytes))
            }
            "DIGEST" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "DIGEST() requires exactly 2 arguments: data, algorithm".to_string(),
                    ));
                }
                let data_val = self.evaluate_function_arg(&args[0], row)?;
                let algo_val = self.evaluate_function_arg(&args[1], row)?;
                if data_val.is_null() || algo_val.is_null() {
                    return Ok(Value::null());
                }
                let data = if let Some(s) = data_val.as_str() {
                    s.as_bytes().to_vec()
                } else if let Some(b) = data_val.as_bytes() {
                    b.to_vec()
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "STRING or BYTES".to_string(),
                        actual: data_val.data_type().to_string(),
                    });
                };
                let algorithm = algo_val.as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: algo_val.data_type().to_string(),
                })?;
                use sha2::{Digest, Sha256, Sha512};
                let hash_bytes = match algorithm.to_lowercase().as_str() {
                    "md5" => md5::compute(&data).to_vec(),
                    "sha1" => {
                        use sha1::{Digest as Sha1Digest, Sha1};
                        let mut hasher = Sha1::new();
                        hasher.update(&data);
                        hasher.finalize().to_vec()
                    }
                    "sha256" => {
                        let mut hasher = Sha256::new();
                        hasher.update(&data);
                        hasher.finalize().to_vec()
                    }
                    "sha512" => {
                        let mut hasher = Sha512::new();
                        hasher.update(&data);
                        hasher.finalize().to_vec()
                    }
                    _ => {
                        return Err(Error::invalid_query(format!(
                            "DIGEST: unknown algorithm '{}'. Supported: md5, sha1, sha256, sha512",
                            algorithm
                        )));
                    }
                };
                Ok(Value::bytes(hash_bytes))
            }
            "ENCODE" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "ENCODE() requires exactly 2 arguments: data, format".to_string(),
                    ));
                }
                let data_val = self.evaluate_function_arg(&args[0], row)?;
                let format_val = self.evaluate_function_arg(&args[1], row)?;
                if data_val.is_null() || format_val.is_null() {
                    return Ok(Value::null());
                }
                let data = data_val.as_bytes().ok_or_else(|| Error::TypeMismatch {
                    expected: "BYTES".to_string(),
                    actual: data_val.data_type().to_string(),
                })?;
                let format = format_val.as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: format_val.data_type().to_string(),
                })?;
                let encoded = match format.to_lowercase().as_str() {
                    "hex" => hex::encode(data),
                    "base64" => {
                        use base64::Engine as _;
                        use base64::engine::general_purpose::STANDARD;
                        STANDARD.encode(data)
                    }
                    _ => {
                        return Err(Error::invalid_query(format!(
                            "ENCODE: unknown format '{}'. Supported: hex, base64",
                            format
                        )));
                    }
                };
                Ok(Value::string(encoded))
            }

            "CRC32" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "CRC32() requires exactly 1 argument".to_string(),
                    ));
                }
                let arg = self.evaluate_function_arg(&args[0], row)?;
                if arg.is_null() {
                    return Ok(Value::null());
                }
                let data = if let Some(s) = arg.as_str() {
                    s.as_bytes().to_vec()
                } else if let Some(b) = arg.as_bytes() {
                    b.to_vec()
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "STRING or BYTES".to_string(),
                        actual: arg.data_type().to_string(),
                    });
                };

                let mut crc: u32 = 0xFFFFFFFF;
                for byte in &data {
                    let index = ((crc ^ (*byte as u32)) & 0xFF) as usize;
                    crc = CRC32_TABLE[index] ^ (crc >> 8);
                }
                Ok(Value::int64((!crc) as i64))
            }

            "CRC32C" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "CRC32C() requires exactly 1 argument".to_string(),
                    ));
                }
                let arg = self.evaluate_function_arg(&args[0], row)?;
                if arg.is_null() {
                    return Ok(Value::null());
                }
                let data = if let Some(s) = arg.as_str() {
                    s.as_bytes().to_vec()
                } else if let Some(b) = arg.as_bytes() {
                    b.to_vec()
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "STRING or BYTES".to_string(),
                        actual: arg.data_type().to_string(),
                    });
                };

                let mut crc: u32 = 0xFFFFFFFF;
                for byte in &data {
                    let index = ((crc ^ (*byte as u32)) & 0xFF) as usize;
                    crc = CRC32C_TABLE[index] ^ (crc >> 8);
                }
                Ok(Value::int64((!crc) as i64))
            }

            "GAMMA" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "GAMMA() requires exactly 1 argument".to_string(),
                    ));
                }
                let arg = self.evaluate_function_arg(&args[0], row)?;
                if arg.is_null() {
                    return Ok(Value::null());
                }
                let x = if let Some(f) = arg.as_f64() {
                    f
                } else if let Some(i) = arg.as_i64() {
                    i as f64
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "numeric".to_string(),
                        actual: arg.data_type().to_string(),
                    });
                };

                let result = gamma_function(x);
                Ok(Value::float64(result))
            }

            "LGAMMA" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "LGAMMA() requires exactly 1 argument".to_string(),
                    ));
                }
                let arg = self.evaluate_function_arg(&args[0], row)?;
                if arg.is_null() {
                    return Ok(Value::null());
                }
                let x = if let Some(f) = arg.as_f64() {
                    f
                } else if let Some(i) = arg.as_i64() {
                    i as f64
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "numeric".to_string(),
                        actual: arg.data_type().to_string(),
                    });
                };

                let result = lgamma_function(x);
                Ok(Value::float64(result))
            }
            "TRIM" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "TRIM() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = value.as_str() {
                    Ok(Value::string(s.trim().to_string()))
                } else {
                    Err(Error::InvalidQuery(
                        "TRIM() requires a string argument".to_string(),
                    ))
                }
            }
            "LTRIM" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "LTRIM() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = value.as_str() {
                    Ok(Value::string(s.trim_start().to_string()))
                } else {
                    Err(Error::InvalidQuery(
                        "LTRIM() requires a string argument".to_string(),
                    ))
                }
            }
            "RTRIM" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "RTRIM() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = value.as_str() {
                    Ok(Value::string(s.trim_end().to_string()))
                } else {
                    Err(Error::InvalidQuery(
                        "RTRIM() requires a string argument".to_string(),
                    ))
                }
            }
            "REVERSE" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "REVERSE() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = value.as_str() {
                    Ok(Value::string(s.chars().rev().collect()))
                } else if let Some(b) = value.as_bytes() {
                    let mut reversed = b.to_vec();
                    reversed.reverse();
                    Ok(Value::bytes(reversed))
                } else {
                    Err(Error::InvalidQuery(
                        "REVERSE() requires a string or bytea argument".to_string(),
                    ))
                }
            }

            "TO_NUMBER" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "TO_NUMBER() requires exactly 2 arguments".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                let format = self.evaluate_function_arg(&args[1], row)?;

                if value.is_null() || format.is_null() {
                    return Ok(Value::null());
                }

                let input = value.as_str().ok_or_else(|| {
                    Error::InvalidQuery(
                        "TO_NUMBER() requires a string as first argument".to_string(),
                    )
                })?;
                let fmt = format.as_str().ok_or_else(|| {
                    Error::InvalidQuery(
                        "TO_NUMBER() requires a string as format argument".to_string(),
                    )
                })?;

                if fmt.eq_ignore_ascii_case("RN") {
                    let result = parse_roman_numeral(input)?;
                    Ok(Value::int64(result))
                } else {
                    let num: f64 = input.trim().parse().map_err(|_| {
                        Error::InvalidQuery(format!("Cannot parse '{}' as a number", input))
                    })?;
                    Ok(Value::float64(num))
                }
            }
            "REPEAT" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "REPEAT() requires exactly 2 arguments".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                let count = self.evaluate_function_arg(&args[1], row)?;

                if value.is_null() || count.is_null() {
                    return Ok(Value::null());
                }

                if let (Some(s), Some(n)) = (value.as_str(), count.as_i64()) {
                    if n < 0 {
                        return Err(Error::InvalidQuery(
                            "REPEAT() count must be non-negative".to_string(),
                        ));
                    }
                    Ok(Value::string(s.repeat(n as usize)))
                } else {
                    Err(Error::InvalidQuery(
                        "REPEAT() requires a string and integer argument".to_string(),
                    ))
                }
            }
            "LPAD" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "LPAD() requires 2 or 3 arguments".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                let length = self.evaluate_function_arg(&args[1], row)?;
                let fill = if args.len() == 3 {
                    self.evaluate_function_arg(&args[2], row)?
                } else {
                    Value::string(" ".to_string())
                };

                if value.is_null() || length.is_null() {
                    return Ok(Value::null());
                }

                if let (Some(s), Some(len)) = (value.as_str(), length.as_i64()) {
                    let len = len as usize;
                    let fill_str = fill.as_str().unwrap_or(" ");
                    let char_count = s.chars().count();

                    if len <= char_count {
                        Ok(Value::string(s.chars().take(len).collect::<String>()))
                    } else {
                        let pad_count = len - char_count;
                        let fill_chars: Vec<char> = fill_str.chars().collect();
                        if fill_chars.is_empty() {
                            Ok(Value::string(s.to_string()))
                        } else {
                            let mut result = String::with_capacity(len);
                            for i in 0..pad_count {
                                result.push(fill_chars[i % fill_chars.len()]);
                            }
                            result.push_str(s);
                            Ok(Value::string(result))
                        }
                    }
                } else {
                    Err(Error::InvalidQuery(
                        "LPAD() requires a string and integer argument".to_string(),
                    ))
                }
            }
            "RPAD" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "RPAD() requires 2 or 3 arguments".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                let length = self.evaluate_function_arg(&args[1], row)?;
                let fill = if args.len() == 3 {
                    self.evaluate_function_arg(&args[2], row)?
                } else {
                    Value::string(" ".to_string())
                };

                if value.is_null() || length.is_null() {
                    return Ok(Value::null());
                }

                if let (Some(s), Some(len)) = (value.as_str(), length.as_i64()) {
                    let len = len as usize;
                    let fill_str = fill.as_str().unwrap_or(" ");
                    let char_count = s.chars().count();

                    if len <= char_count {
                        Ok(Value::string(s.chars().take(len).collect::<String>()))
                    } else {
                        let pad_count = len - char_count;
                        let fill_chars: Vec<char> = fill_str.chars().collect();
                        if fill_chars.is_empty() {
                            Ok(Value::string(s.to_string()))
                        } else {
                            let mut result = String::with_capacity(len);
                            result.push_str(s);
                            for i in 0..pad_count {
                                result.push(fill_chars[i % fill_chars.len()]);
                            }
                            Ok(Value::string(result))
                        }
                    }
                } else {
                    Err(Error::InvalidQuery(
                        "RPAD() requires a string and integer argument".to_string(),
                    ))
                }
            }
            "ASCII" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "ASCII() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;

                if value.is_null() {
                    return Ok(Value::null());
                }

                if let Some(s) = value.as_str() {
                    if let Some(c) = s.chars().next() {
                        Ok(Value::int64(c as i64))
                    } else {
                        Ok(Value::int64(0))
                    }
                } else {
                    Err(Error::InvalidQuery(
                        "ASCII() requires a string argument".to_string(),
                    ))
                }
            }
            "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "CHAR_LENGTH() requires exactly 1 argument".to_string(),
                    ));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;

                if value.is_null() {
                    return Ok(Value::null());
                }

                if let Some(s) = value.as_str() {
                    Ok(Value::int64(s.chars().count() as i64))
                } else {
                    Err(Error::InvalidQuery(
                        "CHAR_LENGTH() requires a string argument".to_string(),
                    ))
                }
            }
            "FORMAT" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "FORMAT() requires at least 1 argument".to_string(),
                    ));
                }
                let format_str = self.evaluate_function_arg(&args[0], row)?;

                if format_str.is_null() {
                    return Ok(Value::null());
                }

                let fmt = format_str.as_str().ok_or_else(|| {
                    Error::InvalidQuery("FORMAT() first argument must be a string".to_string())
                })?;

                let mut format_args: Vec<Value> = Vec::new();
                for arg in args.iter().skip(1) {
                    format_args.push(self.evaluate_function_arg(arg, row)?);
                }

                let mut result = String::new();
                let mut chars = fmt.chars().peekable();
                let mut arg_idx = 0;

                while let Some(c) = chars.next() {
                    if c == '%' {
                        if let Some(&next) = chars.peek() {
                            if next == '%' {
                                result.push('%');
                                chars.next();
                            } else {
                                let mut width = String::new();
                                let mut precision = String::new();
                                let mut in_precision = false;

                                while let Some(&ch) = chars.peek() {
                                    if ch.is_ascii_digit() {
                                        if in_precision {
                                            precision.push(ch);
                                        } else {
                                            width.push(ch);
                                        }
                                        chars.next();
                                    } else if ch == '.' && !in_precision {
                                        in_precision = true;
                                        chars.next();
                                    } else {
                                        break;
                                    }
                                }

                                if let Some(spec) = chars.next() {
                                    if arg_idx < format_args.len() {
                                        let arg_val = &format_args[arg_idx];
                                        arg_idx += 1;

                                        let formatted = match spec {
                                            's' => {
                                                if arg_val.is_null() {
                                                    "".to_string()
                                                } else if let Some(s) = arg_val.as_str() {
                                                    s.to_string()
                                                } else {
                                                    arg_val.to_string()
                                                }
                                            }
                                            'd' | 'i' => {
                                                if arg_val.is_null() {
                                                    "".to_string()
                                                } else if let Some(n) = arg_val.as_i64() {
                                                    format!("{}", n)
                                                } else {
                                                    arg_val.to_string()
                                                }
                                            }
                                            'f' => {
                                                if arg_val.is_null() {
                                                    "".to_string()
                                                } else if let Some(n) = arg_val.as_f64() {
                                                    let prec =
                                                        precision.parse::<usize>().unwrap_or(6);
                                                    format!("{:.prec$}", n, prec = prec)
                                                } else if let Some(d) = arg_val.as_numeric() {
                                                    let prec =
                                                        precision.parse::<usize>().unwrap_or(6);

                                                    use rust_decimal::prelude::ToPrimitive;
                                                    let f = d.to_f64().unwrap_or(0.0);
                                                    format!("{:.prec$}", f, prec = prec)
                                                } else {
                                                    arg_val.to_string()
                                                }
                                            }
                                            _ => arg_val.to_string(),
                                        };

                                        if let Ok(w) = width.parse::<usize>() {
                                            if w > formatted.len() {
                                                result.push_str(&" ".repeat(w - formatted.len()));
                                            }
                                        }
                                        result.push_str(&formatted);
                                    }
                                }
                            }
                        } else {
                            result.push('%');
                        }
                    } else {
                        result.push(c);
                    }
                }

                Ok(Value::string(result))
            }
            "MD5" | "SHA1" | "SHA256" | "SHA512" | "TO_HEX" | "FROM_HEX" | "TO_BASE64"
            | "FROM_BASE64" | "FARM_FINGERPRINT" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(format!(
                        "{}() requires exactly 1 argument",
                        func_name
                    )));
                }
                let value = self.evaluate_function_arg(&args[0], row)?;
                let return_hex = matches!(
                    self.dialect,
                    crate::DialectType::PostgreSQL | crate::DialectType::ClickHouse
                );
                match func_name.as_str() {
                    "MD5" => yachtsql_functions::scalar::eval_md5(&value, return_hex),
                    "SHA1" => yachtsql_functions::scalar::eval_sha1(&value, return_hex),
                    "SHA256" => yachtsql_functions::scalar::eval_sha256(&value, return_hex),
                    "SHA512" => yachtsql_functions::scalar::eval_sha512(&value, return_hex),
                    "TO_HEX" => yachtsql_functions::scalar::eval_to_hex(&value),
                    "FROM_HEX" => yachtsql_functions::scalar::eval_from_hex(&value),
                    "TO_BASE64" => yachtsql_functions::scalar::eval_to_base64(&value),
                    "FROM_BASE64" => yachtsql_functions::scalar::eval_from_base64(&value),
                    "FARM_FINGERPRINT" => yachtsql_functions::scalar::eval_farm_fingerprint(&value),
                    _ => unreachable!(),
                }
            }
            "CURRENT_DATE" => {
                if !args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "CURRENT_DATE() takes no arguments".to_string(),
                    ));
                }
                Ok(Value::date(chrono::Utc::now().naive_utc().date()))
            }
            "CURRENT_TIMESTAMP" => {
                if !args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "CURRENT_TIMESTAMP() takes no arguments".to_string(),
                    ));
                }
                Ok(Value::timestamp(chrono::Utc::now()))
            }
            "DATE_ADD" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "DATE_ADD() requires exactly 2 arguments".to_string(),
                    ));
                }
                let date_val = self.evaluate_function_arg(&args[0], row)?;
                let days_val = self.evaluate_function_arg(&args[1], row)?;

                if date_val.is_null() || days_val.is_null() {
                    return Ok(Value::null());
                }

                let date = date_val.as_date().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: format!("{:?}", date_val.data_type()),
                })?;

                let days = days_val.as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: format!("{:?}", days_val.data_type()),
                })?;

                use chrono::Duration;
                date.checked_add_signed(Duration::days(days))
                    .map(Value::date)
                    .ok_or_else(|| Error::invalid_query("Date overflow in DATE_ADD"))
            }
            "DATE_DIFF" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "DATE_DIFF() requires exactly 2 arguments".to_string(),
                    ));
                }
                let date1_val = self.evaluate_function_arg(&args[0], row)?;
                let date2_val = self.evaluate_function_arg(&args[1], row)?;

                if date1_val.is_null() || date2_val.is_null() {
                    return Ok(Value::null());
                }

                let date1 = date1_val.as_date().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: format!("{:?}", date1_val.data_type()),
                })?;

                let date2 = date2_val.as_date().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: format!("{:?}", date2_val.data_type()),
                })?;

                Ok(Value::int64(date1.signed_duration_since(date2).num_days()))
            }
            "MAKE_DATE" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery(
                        "MAKE_DATE() requires exactly 3 arguments (year, month, day)".to_string(),
                    ));
                }
                let year_val = self.evaluate_function_arg(&args[0], row)?;
                let month_val = self.evaluate_function_arg(&args[1], row)?;
                let day_val = self.evaluate_function_arg(&args[2], row)?;

                if year_val.is_null() || month_val.is_null() || day_val.is_null() {
                    return Ok(Value::null());
                }

                let year = year_val.as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: format!("{:?}", year_val.data_type()),
                })?;

                let month = month_val.as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: format!("{:?}", month_val.data_type()),
                })?;

                let day = day_val.as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: format!("{:?}", day_val.data_type()),
                })?;

                match NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32) {
                    Some(date) => Ok(Value::date(date)),
                    None => Err(Error::invalid_query(format!(
                        "invalid date for MAKE_DATE: year={}, month={}, day={}",
                        year, month, day
                    ))),
                }
            }
            "MAKE_TIMESTAMP" => {
                if args.len() != 6 {
                    return Err(Error::InvalidQuery(
                        "MAKE_TIMESTAMP() requires exactly 6 arguments (year, month, day, hour, minute, second)".to_string(),
                    ));
                }
                let year_val = self.evaluate_function_arg(&args[0], row)?;
                let month_val = self.evaluate_function_arg(&args[1], row)?;
                let day_val = self.evaluate_function_arg(&args[2], row)?;
                let hour_val = self.evaluate_function_arg(&args[3], row)?;
                let minute_val = self.evaluate_function_arg(&args[4], row)?;
                let second_val = self.evaluate_function_arg(&args[5], row)?;

                if year_val.is_null()
                    || month_val.is_null()
                    || day_val.is_null()
                    || hour_val.is_null()
                    || minute_val.is_null()
                    || second_val.is_null()
                {
                    return Ok(Value::null());
                }

                let year = year_val.as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: format!("{:?}", year_val.data_type()),
                })?;

                let month = month_val.as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: format!("{:?}", month_val.data_type()),
                })?;

                let day = day_val.as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: format!("{:?}", day_val.data_type()),
                })?;

                let hour = hour_val.as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: format!("{:?}", hour_val.data_type()),
                })?;

                let minute = minute_val.as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: format!("{:?}", minute_val.data_type()),
                })?;

                let second = second_val.as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: format!("{:?}", second_val.data_type()),
                })?;

                match NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32) {
                    Some(date) => {
                        match date.and_hms_opt(hour as u32, minute as u32, second as u32) {
                            Some(naive_datetime) => {
                                let timestamp =
                                    DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc);
                                Ok(Value::timestamp(timestamp))
                            }
                            None => Err(Error::invalid_query(format!(
                                "invalid time for MAKE_TIMESTAMP: {}:{:02}:{:02}",
                                hour, minute, second
                            ))),
                        }
                    }
                    None => Err(Error::invalid_query(format!(
                        "invalid date for MAKE_TIMESTAMP: year={}, month={}, day={}",
                        year, month, day
                    ))),
                }
            }
            "CURRENT_TIME" => {
                if !args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "CURRENT_TIME() takes no arguments".to_string(),
                    ));
                }
                Ok(Value::timestamp(chrono::Utc::now()))
            }
            "AGE" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::InvalidQuery(
                        "AGE() requires 1 or 2 arguments".to_string(),
                    ));
                }

                let (end_val, start_val) = if args.len() == 1 {
                    (
                        Value::timestamp(chrono::Utc::now()),
                        self.evaluate_function_arg(&args[0], row)?,
                    )
                } else {
                    (
                        self.evaluate_function_arg(&args[0], row)?,
                        self.evaluate_function_arg(&args[1], row)?,
                    )
                };

                if end_val.is_null() || start_val.is_null() {
                    return Ok(Value::null());
                }

                let days_diff = if let (Some(end_date), Some(start_date)) =
                    (end_val.as_date(), start_val.as_date())
                {
                    end_date.signed_duration_since(start_date).num_days()
                } else if let (Some(end_ts), Some(start_ts)) =
                    (end_val.as_timestamp(), start_val.as_timestamp())
                {
                    end_ts.signed_duration_since(start_ts).num_days()
                } else if let (Some(end_date), Some(start_ts)) =
                    (end_val.as_date(), start_val.as_timestamp())
                {
                    let end_datetime = NaiveDateTime::new(
                        end_date,
                        NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is always valid"),
                    );
                    let end_ts_utc = DateTime::<Utc>::from_naive_utc_and_offset(end_datetime, Utc);
                    end_ts_utc.signed_duration_since(start_ts).num_days()
                } else if let (Some(end_ts), Some(start_date)) =
                    (end_val.as_timestamp(), start_val.as_date())
                {
                    let start_datetime = NaiveDateTime::new(
                        start_date,
                        NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is always valid"),
                    );
                    let start_ts_utc =
                        DateTime::<Utc>::from_naive_utc_and_offset(start_datetime, Utc);
                    end_ts.signed_duration_since(start_ts_utc).num_days()
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "DATE or TIMESTAMP".to_string(),
                        actual: format!(
                            "{:?} and {:?}",
                            end_val.data_type(),
                            start_val.data_type()
                        ),
                    });
                };

                Ok(Value::int64(days_diff))
            }
            "NOW" => {
                if !args.is_empty() {
                    return Err(Error::InvalidQuery("NOW() takes no arguments".to_string()));
                }
                Ok(Value::timestamp(chrono::Utc::now()))
            }
            "YEAR" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "YEAR() requires exactly 1 argument".to_string(),
                    ));
                }
                let val = self.evaluate_function_arg(&args[0], row)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                if let Some(d) = val.as_date() {
                    return Ok(Value::int64(d.year() as i64));
                }
                if let Some(ts) = val.as_timestamp() {
                    return Ok(Value::int64(ts.year() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "DATE or TIMESTAMP".to_string(),
                    actual: format!("{:?}", val.data_type()),
                })
            }
            "MONTH" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "MONTH() requires exactly 1 argument".to_string(),
                    ));
                }
                let val = self.evaluate_function_arg(&args[0], row)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                if let Some(d) = val.as_date() {
                    return Ok(Value::int64(d.month() as i64));
                }
                if let Some(ts) = val.as_timestamp() {
                    return Ok(Value::int64(ts.month() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "DATE or TIMESTAMP".to_string(),
                    actual: format!("{:?}", val.data_type()),
                })
            }
            "DAY" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "DAY() requires exactly 1 argument".to_string(),
                    ));
                }
                let val = self.evaluate_function_arg(&args[0], row)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                if let Some(d) = val.as_date() {
                    return Ok(Value::int64(d.day() as i64));
                }
                if let Some(ts) = val.as_timestamp() {
                    return Ok(Value::int64(ts.day() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "DATE or TIMESTAMP".to_string(),
                    actual: format!("{:?}", val.data_type()),
                })
            }
            "HOUR" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "HOUR() requires exactly 1 argument".to_string(),
                    ));
                }
                let val = self.evaluate_function_arg(&args[0], row)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = val.as_timestamp() {
                    return Ok(Value::int64(ts.hour() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: format!("{:?}", val.data_type()),
                })
            }
            "MINUTE" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "MINUTE() requires exactly 1 argument".to_string(),
                    ));
                }
                let val = self.evaluate_function_arg(&args[0], row)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = val.as_timestamp() {
                    return Ok(Value::int64(ts.minute() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: format!("{:?}", val.data_type()),
                })
            }
            "SECOND" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "SECOND() requires exactly 1 argument".to_string(),
                    ));
                }
                let val = self.evaluate_function_arg(&args[0], row)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = val.as_timestamp() {
                    return Ok(Value::int64(ts.second() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: format!("{:?}", val.data_type()),
                })
            }
            "QUARTER" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "QUARTER() requires exactly 1 argument".to_string(),
                    ));
                }
                let val = self.evaluate_function_arg(&args[0], row)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                let month = if let Some(d) = val.as_date() {
                    d.month()
                } else if let Some(ts) = val.as_timestamp() {
                    ts.month()
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "DATE or TIMESTAMP".to_string(),
                        actual: format!("{:?}", val.data_type()),
                    });
                };
                let quarter = (month - 1) / 3 + 1;
                Ok(Value::int64(quarter as i64))
            }
            "WEEK" | "ISOWEEK" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(format!(
                        "{}() requires exactly 1 argument",
                        func_name
                    )));
                }
                let val = self.evaluate_function_arg(&args[0], row)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                if let Some(d) = val.as_date() {
                    return Ok(Value::int64(d.iso_week().week() as i64));
                }
                if let Some(ts) = val.as_timestamp() {
                    return Ok(Value::int64(ts.iso_week().week() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "DATE or TIMESTAMP".to_string(),
                    actual: format!("{:?}", val.data_type()),
                })
            }
            "DAYOFWEEK" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "DAYOFWEEK() requires exactly 1 argument".to_string(),
                    ));
                }
                let val = self.evaluate_function_arg(&args[0], row)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                if let Some(d) = val.as_date() {
                    return Ok(Value::int64(d.weekday().num_days_from_sunday() as i64 + 1));
                }
                if let Some(ts) = val.as_timestamp() {
                    return Ok(Value::int64(ts.weekday().num_days_from_sunday() as i64 + 1));
                }
                Err(Error::TypeMismatch {
                    expected: "DATE or TIMESTAMP".to_string(),
                    actual: format!("{:?}", val.data_type()),
                })
            }
            "DAYOFYEAR" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "DAYOFYEAR() requires exactly 1 argument".to_string(),
                    ));
                }
                let val = self.evaluate_function_arg(&args[0], row)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                if let Some(d) = val.as_date() {
                    return Ok(Value::int64(d.ordinal() as i64));
                }
                if let Some(ts) = val.as_timestamp() {
                    return Ok(Value::int64(ts.ordinal() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "DATE or TIMESTAMP".to_string(),
                    actual: format!("{:?}", val.data_type()),
                })
            }
            "DATE_SUB" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "DATE_SUB() requires exactly 2 arguments".to_string(),
                    ));
                }
                let date_val = self.evaluate_function_arg(&args[0], row)?;
                let days_val = self.evaluate_function_arg(&args[1], row)?;

                if date_val.is_null() || days_val.is_null() {
                    return Ok(Value::null());
                }

                let date = date_val.as_date().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: format!("{:?}", date_val.data_type()),
                })?;

                let days = days_val.as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: format!("{:?}", days_val.data_type()),
                })?;

                use chrono::Duration;
                date.checked_sub_signed(Duration::days(days))
                    .map(Value::date)
                    .ok_or_else(|| Error::invalid_query("Date overflow in DATE_SUB"))
            }
            "DATE_TRUNC" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "DATE_TRUNC() requires exactly 2 arguments".to_string(),
                    ));
                }
                let precision_val = self.evaluate_function_arg(&args[0], row)?;
                let date_val = self.evaluate_function_arg(&args[1], row)?;

                if precision_val.is_null() || date_val.is_null() {
                    return Ok(Value::null());
                }

                let precision = precision_val.as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: format!("{:?}", precision_val.data_type()),
                })?;

                let date = date_val.as_date().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: format!("{:?}", date_val.data_type()),
                })?;

                use chrono::Duration;
                let truncated = match precision.to_uppercase().as_str() {
                    "YEAR" => date
                        .with_month(1)
                        .and_then(|d| d.with_day(1))
                        .ok_or_else(|| Error::invalid_query("Failed to truncate to YEAR"))?,
                    "MONTH" => date
                        .with_day(1)
                        .ok_or_else(|| Error::invalid_query("Failed to truncate to MONTH"))?,
                    "DAY" => date,
                    "WEEK" => {
                        let diff = date.weekday().num_days_from_monday();
                        date - Duration::days(diff as i64)
                    }
                    "QUARTER" => {
                        let start_month = ((date.month() - 1) / 3) * 3 + 1;
                        date.with_month(start_month)
                            .and_then(|d| d.with_day(1))
                            .ok_or_else(|| Error::invalid_query("Failed to truncate to QUARTER"))?
                    }
                    _ => {
                        return Err(Error::invalid_query(format!(
                            "Unknown precision for DATE_TRUNC: {}",
                            precision
                        )));
                    }
                };
                Ok(Value::date(truncated))
            }
            "LAST_DAY" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "LAST_DAY() requires exactly 1 argument".to_string(),
                    ));
                }
                let date_val = self.evaluate_function_arg(&args[0], row)?;
                if date_val.is_null() {
                    return Ok(Value::null());
                }

                let date = date_val.as_date().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: format!("{:?}", date_val.data_type()),
                })?;

                let next_month = if date.month() == 12 {
                    NaiveDate::from_ymd_opt(date.year() + 1, 1, 1)
                } else {
                    NaiveDate::from_ymd_opt(date.year(), date.month() + 1, 1)
                };

                next_month
                    .and_then(|nm| nm.pred_opt())
                    .map(Value::date)
                    .ok_or_else(|| Error::invalid_query("Failed to calculate last day of month"))
            }
            "FORMAT_DATE" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "FORMAT_DATE() requires exactly 2 arguments".to_string(),
                    ));
                }
                let format_val = self.evaluate_function_arg(&args[0], row)?;
                let date_val = self.evaluate_function_arg(&args[1], row)?;

                if format_val.is_null() || date_val.is_null() {
                    return Ok(Value::null());
                }

                let format = format_val.as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: format!("{:?}", format_val.data_type()),
                })?;

                let date = date_val.as_date().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: format!("{:?}", date_val.data_type()),
                })?;

                let result = format
                    .replace("%Y", &format!("{:04}", date.year()))
                    .replace("%y", &format!("{:02}", date.year() % 100))
                    .replace("%m", &format!("{:02}", date.month()))
                    .replace("%d", &format!("{:02}", date.day()));
                Ok(Value::string(result))
            }
            "REPLACE" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery(
                        "REPLACE() requires exactly 3 arguments".to_string(),
                    ));
                }
                let string_val = self.evaluate_function_arg(&args[0], row)?;
                let from_val = self.evaluate_function_arg(&args[1], row)?;
                let to_val = self.evaluate_function_arg(&args[2], row)?;

                if string_val.is_null() || from_val.is_null() || to_val.is_null() {
                    return Ok(Value::null());
                }

                if let (Some(s), Some(from), Some(to)) =
                    (string_val.as_str(), from_val.as_str(), to_val.as_str())
                {
                    Ok(Value::string(s.replace(from, to)))
                } else {
                    Err(Error::InvalidQuery(
                        "REPLACE() requires string arguments".to_string(),
                    ))
                }
            }
            "STARTS_WITH" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "STARTS_WITH() requires exactly 2 arguments".to_string(),
                    ));
                }
                let string_val = self.evaluate_function_arg(&args[0], row)?;
                let prefix_val = self.evaluate_function_arg(&args[1], row)?;

                if string_val.is_null() || prefix_val.is_null() {
                    return Ok(Value::null());
                }

                if let (Some(s), Some(prefix)) = (string_val.as_str(), prefix_val.as_str()) {
                    Ok(Value::bool_val(s.starts_with(prefix)))
                } else {
                    Err(Error::InvalidQuery(
                        "STARTS_WITH() requires string arguments".to_string(),
                    ))
                }
            }
            "ENDS_WITH" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "ENDS_WITH() requires exactly 2 arguments".to_string(),
                    ));
                }
                let string_val = self.evaluate_function_arg(&args[0], row)?;
                let suffix_val = self.evaluate_function_arg(&args[1], row)?;

                if string_val.is_null() || suffix_val.is_null() {
                    return Ok(Value::null());
                }

                if let (Some(s), Some(suffix)) = (string_val.as_str(), suffix_val.as_str()) {
                    Ok(Value::bool_val(s.ends_with(suffix)))
                } else {
                    Err(Error::InvalidQuery(
                        "ENDS_WITH() requires string arguments".to_string(),
                    ))
                }
            }
            "REGEXP_CONTAINS" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "REGEXP_CONTAINS() requires exactly 2 arguments".to_string(),
                    ));
                }
                let string_val = self.evaluate_function_arg(&args[0], row)?;
                let pattern_val = self.evaluate_function_arg(&args[1], row)?;

                if string_val.is_null() || pattern_val.is_null() {
                    return Ok(Value::null());
                }

                if let (Some(s), Some(pattern)) = (string_val.as_str(), pattern_val.as_str()) {
                    match regex::Regex::new(pattern) {
                        Ok(re) => Ok(Value::bool_val(re.is_match(s))),
                        Err(e) => Err(Error::InvalidQuery(format!("Invalid regex pattern: {}", e))),
                    }
                } else {
                    Err(Error::InvalidQuery(
                        "REGEXP_CONTAINS() requires string arguments".to_string(),
                    ))
                }
            }
            "REGEXP_EXTRACT" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "REGEXP_EXTRACT() requires exactly 2 arguments".to_string(),
                    ));
                }
                let string_val = self.evaluate_function_arg(&args[0], row)?;
                let pattern_val = self.evaluate_function_arg(&args[1], row)?;

                if string_val.is_null() || pattern_val.is_null() {
                    return Ok(Value::null());
                }

                if let (Some(s), Some(pattern)) = (string_val.as_str(), pattern_val.as_str()) {
                    match regex::Regex::new(pattern) {
                        Ok(re) => {
                            if let Some(captures) = re.captures(s) {
                                let result = if captures.len() > 1 {
                                    captures.get(1).map(|m| m.as_str().to_string())
                                } else {
                                    captures.get(0).map(|m| m.as_str().to_string())
                                };
                                Ok(result.map(Value::string).unwrap_or(Value::null()))
                            } else {
                                Ok(Value::null())
                            }
                        }
                        Err(e) => Err(Error::InvalidQuery(format!("Invalid regex pattern: {}", e))),
                    }
                } else {
                    Err(Error::InvalidQuery(
                        "REGEXP_EXTRACT() requires string arguments".to_string(),
                    ))
                }
            }
            "REGEXP_REPLACE" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery(
                        "REGEXP_REPLACE() requires exactly 3 arguments".to_string(),
                    ));
                }
                let string_val = self.evaluate_function_arg(&args[0], row)?;
                let pattern_val = self.evaluate_function_arg(&args[1], row)?;
                let replacement_val = self.evaluate_function_arg(&args[2], row)?;

                if string_val.is_null() || pattern_val.is_null() || replacement_val.is_null() {
                    return Ok(Value::null());
                }

                if let (Some(s), Some(pattern), Some(replacement)) = (
                    string_val.as_str(),
                    pattern_val.as_str(),
                    replacement_val.as_str(),
                ) {
                    match regex::Regex::new(pattern) {
                        Ok(re) => Ok(Value::string(re.replace_all(s, replacement).to_string())),
                        Err(e) => Err(Error::InvalidQuery(format!("Invalid regex pattern: {}", e))),
                    }
                } else {
                    Err(Error::InvalidQuery(
                        "REGEXP_REPLACE() requires string arguments".to_string(),
                    ))
                }
            }
            "YACHTSQL.IS_FEATURE_ENABLED" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "yachtsql.is_feature_enabled() requires exactly 1 argument".to_string(),
                    ));
                }
                let feature_arg = self.evaluate_function_arg(&args[0], row)?;

                if feature_arg.is_null() {
                    return Ok(Value::null());
                }

                let feature_str = if let Some(s) = feature_arg.as_str() {
                    s.trim().to_string()
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: feature_arg.data_type().to_string(),
                    });
                };

                use crate::query_executor::evaluator::physical_plan::FEATURE_REGISTRY_CONTEXT;
                let registry = FEATURE_REGISTRY_CONTEXT
                    .with(|ctx| ctx.borrow().clone())
                    .ok_or_else(|| {
                        Error::InternalError(
                            "Feature registry context missing for yachtsql.is_feature_enabled"
                                .to_string(),
                        )
                    })?;

                let feature_id = registry
                    .all_features()
                    .find(|feature| {
                        feature
                            .id
                            .as_str()
                            .eq_ignore_ascii_case(feature_str.as_str())
                    })
                    .map(|feature| feature.id)
                    .ok_or_else(|| {
                        Error::unsupported_feature(format!("Unknown feature id '{}'", feature_str))
                    })?;

                let enabled = registry.is_enabled(feature_id);
                Ok(Value::bool_val(enabled))
            }
            "ARRAY_LENGTH" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "ARRAY_LENGTH() requires exactly 1 argument".to_string(),
                    ));
                }
                let arg = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::array::array_length(&arg)
            }
            "ARRAY_REVERSE" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "ARRAY_REVERSE() requires exactly 1 argument".to_string(),
                    ));
                }
                let arg = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::array::array_reverse(arg)
            }
            "ARRAY_CONCAT" | "ARRAY_CAT" => {
                let mut arr_args = Vec::new();
                for arg in args {
                    let value = self.evaluate_function_arg(arg, row)?;
                    arr_args.push(value);
                }
                yachtsql_functions::array::array_concat(&arr_args)
            }
            "ARRAY_APPEND" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "ARRAY_APPEND() requires exactly 2 arguments".to_string(),
                    ));
                }
                let array = self.evaluate_function_arg(&args[0], row)?;
                let element = self.evaluate_function_arg(&args[1], row)?;
                yachtsql_functions::array::array_append(array, element)
            }
            "ARRAY_PREPEND" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "ARRAY_PREPEND() requires exactly 2 arguments".to_string(),
                    ));
                }
                let element = self.evaluate_function_arg(&args[0], row)?;
                let array = self.evaluate_function_arg(&args[1], row)?;
                yachtsql_functions::array::array_prepend(element, array)
            }
            "ARRAY_POSITION" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "ARRAY_POSITION() requires exactly 2 arguments".to_string(),
                    ));
                }
                let array = self.evaluate_function_arg(&args[0], row)?;
                let search = self.evaluate_function_arg(&args[1], row)?;
                yachtsql_functions::array::array_position(&array, &search)
            }
            "ARRAY_CONTAINS" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "ARRAY_CONTAINS() requires exactly 2 arguments".to_string(),
                    ));
                }
                let array = self.evaluate_function_arg(&args[0], row)?;
                let search = self.evaluate_function_arg(&args[1], row)?;
                yachtsql_functions::array::array_contains(&array, &search)
            }
            "ARRAY_REMOVE" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "ARRAY_REMOVE() requires exactly 2 arguments".to_string(),
                    ));
                }
                let array = self.evaluate_function_arg(&args[0], row)?;
                let element = self.evaluate_function_arg(&args[1], row)?;
                yachtsql_functions::array::array_remove(array, &element)
            }
            "ARRAY_REPLACE" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery(
                        "ARRAY_REPLACE() requires exactly 3 arguments".to_string(),
                    ));
                }
                let array = self.evaluate_function_arg(&args[0], row)?;
                let old_value = self.evaluate_function_arg(&args[1], row)?;
                let new_value = self.evaluate_function_arg(&args[2], row)?;
                yachtsql_functions::array::array_replace(array, &old_value, &new_value)
            }
            "ARRAY_SORT" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "ARRAY_SORT() requires exactly 1 argument".to_string(),
                    ));
                }
                let arg = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::array::array_sort(arg)
            }
            "ARRAY_DISTINCT" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "ARRAY_DISTINCT() requires exactly 1 argument".to_string(),
                    ));
                }
                let arg = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::array::array_distinct(arg)
            }
            "GENERATE_ARRAY" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "GENERATE_ARRAY() requires 2 or 3 arguments".to_string(),
                    ));
                }
                let start = self.evaluate_function_arg(&args[0], row)?;
                let end = self.evaluate_function_arg(&args[1], row)?;
                let step = if args.len() == 3 {
                    Some(self.evaluate_function_arg(&args[2], row)?)
                } else {
                    None
                };
                yachtsql_functions::array::generate_array(&start, &end, step.as_ref())
            }
            "TRANSLATE" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery(
                        "TRANSLATE() requires exactly 3 arguments".to_string(),
                    ));
                }
                let string = self.evaluate_function_arg(&args[0], row)?;
                let from = self.evaluate_function_arg(&args[1], row)?;
                let to = self.evaluate_function_arg(&args[2], row)?;

                if string.is_null() || from.is_null() || to.is_null() {
                    return Ok(Value::null());
                }

                match (string.as_str(), from.as_str(), to.as_str()) {
                    (Some(s), Some(from_chars), Some(to_chars)) => {
                        let from_vec: Vec<char> = from_chars.chars().collect();
                        let to_vec: Vec<char> = to_chars.chars().collect();

                        let result: String = s
                            .chars()
                            .filter_map(|ch| {
                                if let Some(pos) = from_vec.iter().position(|&c| c == ch) {
                                    to_vec.get(pos).copied()
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
                            string.data_type(),
                            from.data_type(),
                            to.data_type()
                        ),
                    }),
                }
            }
            "QUOTE_IDENT" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "QUOTE_IDENT() requires exactly 1 argument".to_string(),
                    ));
                }
                let arg = self.evaluate_function_arg(&args[0], row)?;
                if arg.is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = arg.as_str() {
                    let escaped = s.replace('"', "\"\"");
                    Ok(Value::string(format!("\"{}\"", escaped)))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: arg.data_type().to_string(),
                    })
                }
            }
            "QUOTE_LITERAL" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "QUOTE_LITERAL() requires exactly 1 argument".to_string(),
                    ));
                }
                let arg = self.evaluate_function_arg(&args[0], row)?;
                if arg.is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = arg.as_str() {
                    let escaped = s.replace('\'', "''");
                    Ok(Value::string(format!("'{}'", escaped)))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: arg.data_type().to_string(),
                    })
                }
            }
            "QUOTE_NULLABLE" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "QUOTE_NULLABLE() requires exactly 1 argument".to_string(),
                    ));
                }
                let arg = self.evaluate_function_arg(&args[0], row)?;
                if arg.is_null() {
                    return Ok(Value::string("NULL".to_string()));
                }
                if let Some(s) = arg.as_str() {
                    let escaped = s.replace('\'', "''");
                    Ok(Value::string(format!("'{}'", escaped)))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: arg.data_type().to_string(),
                    })
                }
            }
            "SPLIT_PART" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery(
                        "SPLIT_PART() requires exactly 3 arguments".to_string(),
                    ));
                }
                let string = self.evaluate_function_arg(&args[0], row)?;
                let delimiter = self.evaluate_function_arg(&args[1], row)?;
                let field = self.evaluate_function_arg(&args[2], row)?;

                if string.is_null() || delimiter.is_null() || field.is_null() {
                    return Ok(Value::null());
                }

                match (string.as_str(), delimiter.as_str(), field.as_i64()) {
                    (Some(s), Some(delim), Some(field_num)) => {
                        if field_num == 0 {
                            return Err(Error::InvalidQuery(
                                "SPLIT_PART field position must be non-zero".to_string(),
                            ));
                        }

                        let parts: Vec<&str> = if delim.is_empty() {
                            vec![s]
                        } else {
                            s.split(delim).collect()
                        };

                        let idx = if field_num < 0 {
                            let abs_field = (-field_num) as usize;
                            if abs_field > parts.len() {
                                return Ok(Value::string(String::new()));
                            }
                            parts.len() - abs_field
                        } else {
                            let field_idx = (field_num - 1) as usize;
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
                            string.data_type(),
                            delimiter.data_type(),
                            field.data_type()
                        ),
                    }),
                }
            }
            "REGEXP_SUBSTR" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "REGEXP_SUBSTR() requires exactly 2 arguments".to_string(),
                    ));
                }
                let string = self.evaluate_function_arg(&args[0], row)?;
                let pattern = self.evaluate_function_arg(&args[1], row)?;

                if string.is_null() || pattern.is_null() {
                    return Ok(Value::null());
                }

                match (string.as_str(), pattern.as_str()) {
                    (Some(s), Some(p)) => match Regex::new(p) {
                        Ok(re) => {
                            if let Some(mat) = re.find(s) {
                                Ok(Value::string(mat.as_str().to_string()))
                            } else {
                                Ok(Value::null())
                            }
                        }
                        Err(e) => Err(Error::InvalidQuery(format!("Invalid regex pattern: {}", e))),
                    },
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, STRING".to_string(),
                        actual: format!("{}, {}", string.data_type(), pattern.data_type()),
                    }),
                }
            }
            "STRING_TO_ARRAY" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "STRING_TO_ARRAY() requires 2 or 3 arguments".to_string(),
                    ));
                }
                let string = self.evaluate_function_arg(&args[0], row)?;
                let delimiter = self.evaluate_function_arg(&args[1], row)?;

                if string.is_null() || delimiter.is_null() {
                    return Ok(Value::null());
                }

                let null_string: Option<String> = if args.len() == 3 {
                    let null_arg = self.evaluate_function_arg(&args[2], row)?;
                    if !null_arg.is_null() {
                        null_arg.as_str().map(|s| s.to_string())
                    } else {
                        None
                    }
                } else {
                    None
                };

                match (string.as_str(), delimiter.as_str()) {
                    (Some(s), Some(delim)) => {
                        let parts: Vec<Value> = if delim.is_empty() {
                            s.chars()
                                .map(|c| {
                                    let char_str = c.to_string();
                                    if null_string
                                        .as_ref()
                                        .map(|ns| ns == &char_str)
                                        .unwrap_or(false)
                                    {
                                        Value::null()
                                    } else {
                                        Value::string(char_str)
                                    }
                                })
                                .collect()
                        } else {
                            s.split(delim)
                                .map(|part| {
                                    if null_string.as_ref().map(|ns| ns == part).unwrap_or(false) {
                                        Value::null()
                                    } else {
                                        Value::string(part.to_string())
                                    }
                                })
                                .collect()
                        };
                        Ok(Value::array(parts))
                    }
                    _ => Err(Error::TypeMismatch {
                        expected: "STRING, STRING".to_string(),
                        actual: format!("{}, {}", string.data_type(), delimiter.data_type()),
                    }),
                }
            }

            "TO_TSVECTOR" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::InvalidQuery(
                        "TO_TSVECTOR() requires 1 or 2 arguments".to_string(),
                    ));
                }
                let text_arg = if args.len() == 1 { &args[0] } else { &args[1] };
                let text = self.evaluate_function_arg(text_arg, row)?;
                if text.is_null() {
                    return Ok(Value::null());
                }
                let s = self.value_to_string(&text)?;
                let vector = yachtsql_functions::fulltext::to_tsvector(&s);
                Ok(Value::string(
                    yachtsql_functions::fulltext::tsvector_to_string(&vector),
                ))
            }
            "TO_TSQUERY" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::InvalidQuery(
                        "TO_TSQUERY() requires 1 or 2 arguments".to_string(),
                    ));
                }
                let text_arg = if args.len() == 1 { &args[0] } else { &args[1] };
                let text = self.evaluate_function_arg(text_arg, row)?;
                if text.is_null() {
                    return Ok(Value::null());
                }
                let s = self.value_to_string(&text)?;
                let query = yachtsql_functions::fulltext::to_tsquery(&s)?;
                Ok(Value::string(
                    yachtsql_functions::fulltext::tsquery_to_string(&query),
                ))
            }
            "PLAINTO_TSQUERY" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::InvalidQuery(
                        "PLAINTO_TSQUERY() requires 1 or 2 arguments".to_string(),
                    ));
                }
                let text_arg = if args.len() == 1 { &args[0] } else { &args[1] };
                let text = self.evaluate_function_arg(text_arg, row)?;
                if text.is_null() {
                    return Ok(Value::null());
                }
                let s = self.value_to_string(&text)?;
                let query = yachtsql_functions::fulltext::plainto_tsquery(&s);
                Ok(Value::string(
                    yachtsql_functions::fulltext::tsquery_to_string(&query),
                ))
            }
            "PHRASETO_TSQUERY" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::InvalidQuery(
                        "PHRASETO_TSQUERY() requires 1 or 2 arguments".to_string(),
                    ));
                }
                let text_arg = if args.len() == 1 { &args[0] } else { &args[1] };
                let text = self.evaluate_function_arg(text_arg, row)?;
                if text.is_null() {
                    return Ok(Value::null());
                }
                let s = self.value_to_string(&text)?;
                let query = yachtsql_functions::fulltext::phraseto_tsquery(&s);
                Ok(Value::string(
                    yachtsql_functions::fulltext::tsquery_to_string(&query),
                ))
            }
            "WEBSEARCH_TO_TSQUERY" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::InvalidQuery(
                        "WEBSEARCH_TO_TSQUERY() requires 1 or 2 arguments".to_string(),
                    ));
                }
                let text_arg = if args.len() == 1 { &args[0] } else { &args[1] };
                let text = self.evaluate_function_arg(text_arg, row)?;
                if text.is_null() {
                    return Ok(Value::null());
                }
                let s = self.value_to_string(&text)?;
                let query = yachtsql_functions::fulltext::websearch_to_tsquery(&s);
                Ok(Value::string(
                    yachtsql_functions::fulltext::tsquery_to_string(&query),
                ))
            }
            "TS_MATCH" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "TS_MATCH() requires exactly 2 arguments".to_string(),
                    ));
                }
                let vector_val = self.evaluate_function_arg(&args[0], row)?;
                let query_val = self.evaluate_function_arg(&args[1], row)?;
                if vector_val.is_null() || query_val.is_null() {
                    return Ok(Value::null());
                }
                let vector_str = self.value_to_string(&vector_val)?;
                let query_str = self.value_to_string(&query_val)?;
                let vector = yachtsql_functions::fulltext::parse_tsvector(&vector_str)?;
                let query = yachtsql_functions::fulltext::to_tsquery(&query_str)?;
                Ok(Value::bool_val(query.matches(&vector)))
            }
            "TS_RANK" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "TS_RANK() requires exactly 2 arguments".to_string(),
                    ));
                }
                let vector_val = self.evaluate_function_arg(&args[0], row)?;
                let query_val = self.evaluate_function_arg(&args[1], row)?;
                if vector_val.is_null() || query_val.is_null() {
                    return Ok(Value::null());
                }
                let vector_str = self.value_to_string(&vector_val)?;
                let query_str = self.value_to_string(&query_val)?;
                let vector = yachtsql_functions::fulltext::parse_tsvector(&vector_str)?;
                let query = yachtsql_functions::fulltext::to_tsquery(&query_str)?;
                let rank = yachtsql_functions::fulltext::ts_rank(&vector, &query);
                Ok(Value::float64(rank))
            }
            "TS_RANK_CD" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "TS_RANK_CD() requires exactly 2 arguments".to_string(),
                    ));
                }
                let vector_val = self.evaluate_function_arg(&args[0], row)?;
                let query_val = self.evaluate_function_arg(&args[1], row)?;
                if vector_val.is_null() || query_val.is_null() {
                    return Ok(Value::null());
                }
                let vector_str = self.value_to_string(&vector_val)?;
                let query_str = self.value_to_string(&query_val)?;
                let vector = yachtsql_functions::fulltext::parse_tsvector(&vector_str)?;
                let query = yachtsql_functions::fulltext::to_tsquery(&query_str)?;
                let rank = yachtsql_functions::fulltext::ts_rank_cd(&vector, &query);
                Ok(Value::float64(rank))
            }
            "TS_HEADLINE" => {
                if args.len() < 2 || args.len() > 4 {
                    return Err(Error::InvalidQuery(
                        "TS_HEADLINE() requires 2 to 4 arguments".to_string(),
                    ));
                }
                let doc_val = self.evaluate_function_arg(&args[0], row)?;
                let query_val = self.evaluate_function_arg(&args[1], row)?;
                if doc_val.is_null() || query_val.is_null() {
                    return Ok(Value::null());
                }
                let document = self.value_to_string(&doc_val)?;
                let query_str = self.value_to_string(&query_val)?;
                let query = yachtsql_functions::fulltext::to_tsquery(&query_str)?;
                let options = yachtsql_functions::fulltext::HeadlineOptions::default();
                let headline =
                    yachtsql_functions::fulltext::ts_headline(&document, &query, &options);
                Ok(Value::string(headline))
            }
            "TSVECTOR_LENGTH" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "TSVECTOR_LENGTH() requires exactly 1 argument".to_string(),
                    ));
                }
                let vector_val = self.evaluate_function_arg(&args[0], row)?;
                if vector_val.is_null() {
                    return Ok(Value::null());
                }
                let vector_str = self.value_to_string(&vector_val)?;
                let vector = yachtsql_functions::fulltext::parse_tsvector(&vector_str)?;
                Ok(Value::int64(yachtsql_functions::fulltext::tsvector_length(
                    &vector,
                )))
            }
            "STRIP" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "STRIP() requires exactly 1 argument".to_string(),
                    ));
                }
                let vector_val = self.evaluate_function_arg(&args[0], row)?;
                if vector_val.is_null() {
                    return Ok(Value::null());
                }
                let vector_str = self.value_to_string(&vector_val)?;
                let vector = yachtsql_functions::fulltext::parse_tsvector(&vector_str)?;
                let stripped = yachtsql_functions::fulltext::tsvector_strip(&vector);
                Ok(Value::string(
                    yachtsql_functions::fulltext::tsvector_to_string(&stripped),
                ))
            }
            "SETWEIGHT" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "SETWEIGHT() requires exactly 2 arguments".to_string(),
                    ));
                }
                let vector_val = self.evaluate_function_arg(&args[0], row)?;
                let weight_val = self.evaluate_function_arg(&args[1], row)?;
                if vector_val.is_null() || weight_val.is_null() {
                    return Ok(Value::null());
                }
                let vector_str = self.value_to_string(&vector_val)?;
                let weight_str = self.value_to_string(&weight_val)?;
                let weight = weight_str
                    .chars()
                    .next()
                    .and_then(yachtsql_functions::fulltext::Weight::from_char)
                    .ok_or_else(|| {
                        Error::InvalidQuery(format!(
                            "Invalid weight '{}'. Must be A, B, C, or D",
                            weight_str
                        ))
                    })?;
                let vector = yachtsql_functions::fulltext::parse_tsvector(&vector_str)?;
                let weighted = yachtsql_functions::fulltext::tsvector_setweight(&vector, weight);
                Ok(Value::string(
                    yachtsql_functions::fulltext::tsvector_to_string(&weighted),
                ))
            }
            "TSVECTOR_CONCAT" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "TSVECTOR_CONCAT() requires exactly 2 arguments".to_string(),
                    ));
                }
                let a_val = self.evaluate_function_arg(&args[0], row)?;
                let b_val = self.evaluate_function_arg(&args[1], row)?;
                if a_val.is_null() || b_val.is_null() {
                    return Ok(Value::null());
                }
                let a_str = self.value_to_string(&a_val)?;
                let b_str = self.value_to_string(&b_val)?;
                let a = yachtsql_functions::fulltext::parse_tsvector(&a_str)?;
                let b = yachtsql_functions::fulltext::parse_tsvector(&b_str)?;
                let result = yachtsql_functions::fulltext::tsvector_concat(&a, &b);
                Ok(Value::string(
                    yachtsql_functions::fulltext::tsvector_to_string(&result),
                ))
            }
            "TSQUERY_AND" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "TSQUERY_AND() requires exactly 2 arguments".to_string(),
                    ));
                }
                let a_val = self.evaluate_function_arg(&args[0], row)?;
                let b_val = self.evaluate_function_arg(&args[1], row)?;
                if a_val.is_null() || b_val.is_null() {
                    return Ok(Value::null());
                }
                let a_str = self.value_to_string(&a_val)?;
                let b_str = self.value_to_string(&b_val)?;
                let a = yachtsql_functions::fulltext::to_tsquery(&a_str)?;
                let b = yachtsql_functions::fulltext::to_tsquery(&b_str)?;
                let result = a.and(b);
                Ok(Value::string(
                    yachtsql_functions::fulltext::tsquery_to_string(&result),
                ))
            }
            "TSQUERY_OR" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "TSQUERY_OR() requires exactly 2 arguments".to_string(),
                    ));
                }
                let a_val = self.evaluate_function_arg(&args[0], row)?;
                let b_val = self.evaluate_function_arg(&args[1], row)?;
                if a_val.is_null() || b_val.is_null() {
                    return Ok(Value::null());
                }
                let a_str = self.value_to_string(&a_val)?;
                let b_str = self.value_to_string(&b_val)?;
                let a = yachtsql_functions::fulltext::to_tsquery(&a_str)?;
                let b = yachtsql_functions::fulltext::to_tsquery(&b_str)?;
                let result = a.or(b);
                Ok(Value::string(
                    yachtsql_functions::fulltext::tsquery_to_string(&result),
                ))
            }
            "TSQUERY_NOT" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "TSQUERY_NOT() requires exactly 1 argument".to_string(),
                    ));
                }
                let query_val = self.evaluate_function_arg(&args[0], row)?;
                if query_val.is_null() {
                    return Ok(Value::null());
                }
                let query_str = self.value_to_string(&query_val)?;
                let query = yachtsql_functions::fulltext::to_tsquery(&query_str)?;
                let result = query.negate();
                Ok(Value::string(
                    yachtsql_functions::fulltext::tsquery_to_string(&result),
                ))
            }

            "POINT" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "POINT() requires exactly 2 arguments (x, y)".to_string(),
                    ));
                }
                let x = self.evaluate_function_arg(&args[0], row)?;
                let y = self.evaluate_function_arg(&args[1], row)?;
                yachtsql_functions::geometric::point_constructor(&x, &y)
            }
            "BOX" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "BOX() requires exactly 2 arguments (point1, point2)".to_string(),
                    ));
                }
                let p1 = self.evaluate_function_arg(&args[0], row)?;
                let p2 = self.evaluate_function_arg(&args[1], row)?;
                yachtsql_functions::geometric::box_constructor(&p1, &p2)
            }
            "CIRCLE" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "CIRCLE() requires exactly 2 arguments (center_point, radius)".to_string(),
                    ));
                }
                let center = self.evaluate_function_arg(&args[0], row)?;
                let radius = self.evaluate_function_arg(&args[1], row)?;
                yachtsql_functions::geometric::circle_constructor(&center, &radius)
            }
            "AREA" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "AREA() requires exactly 1 argument (box or circle)".to_string(),
                    ));
                }
                let geom = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::geometric::area(&geom)
            }
            "CENTER" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "CENTER() requires exactly 1 argument (box or circle)".to_string(),
                    ));
                }
                let geom = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::geometric::center(&geom)
            }
            "DIAMETER" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "DIAMETER() requires exactly 1 argument (circle)".to_string(),
                    ));
                }
                let circle = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::geometric::diameter(&circle)
            }
            "RADIUS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "RADIUS() requires exactly 1 argument (circle)".to_string(),
                    ));
                }
                let circle = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::geometric::radius(&circle)
            }
            "WIDTH" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "WIDTH() requires exactly 1 argument (box)".to_string(),
                    ));
                }
                let box_val = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::geometric::width(&box_val)
            }
            "HEIGHT" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "HEIGHT() requires exactly 1 argument (box)".to_string(),
                    ));
                }
                let box_val = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::geometric::height(&box_val)
            }

            "HSTORE" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "HSTORE() requires 2 arguments (keys[], values[])".to_string(),
                    ));
                }
                let keys = self.evaluate_function_arg(&args[0], row)?;
                let values = self.evaluate_function_arg(&args[1], row)?;
                yachtsql_functions::hstore::hstore_from_arrays(&keys, &values)
            }
            "AKEYS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "AKEYS() requires exactly 1 argument (hstore)".to_string(),
                    ));
                }
                let hstore = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::hstore::hstore_akeys(&hstore)
            }
            "AVALS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "AVALS() requires exactly 1 argument (hstore)".to_string(),
                    ));
                }
                let hstore = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::hstore::hstore_avals(&hstore)
            }
            "SKEYS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "SKEYS() requires exactly 1 argument (hstore)".to_string(),
                    ));
                }
                let hstore = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::hstore::hstore_akeys(&hstore)
            }
            "SVALS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "SVALS() requires exactly 1 argument (hstore)".to_string(),
                    ));
                }
                let hstore = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::hstore::hstore_avals(&hstore)
            }
            "HSTORE_TO_JSON" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "HSTORE_TO_JSON() requires exactly 1 argument (hstore)".to_string(),
                    ));
                }
                let hstore = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::hstore::hstore_to_json(&hstore)
            }
            "HSTORE_TO_JSONB" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "HSTORE_TO_JSONB() requires exactly 1 argument (hstore)".to_string(),
                    ));
                }
                let hstore = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::hstore::hstore_to_jsonb(&hstore)
            }
            "HSTORE_TO_ARRAY" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "HSTORE_TO_ARRAY() requires exactly 1 argument (hstore)".to_string(),
                    ));
                }
                let hstore = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::hstore::hstore_to_array(&hstore)
            }
            "HSTORE_TO_MATRIX" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "HSTORE_TO_MATRIX() requires exactly 1 argument (hstore)".to_string(),
                    ));
                }
                let hstore = self.evaluate_function_arg(&args[0], row)?;
                yachtsql_functions::hstore::hstore_to_matrix(&hstore)
            }
            "SLICE" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "SLICE() requires 2 arguments (hstore, keys[])".to_string(),
                    ));
                }
                let hstore = self.evaluate_function_arg(&args[0], row)?;
                let keys = self.evaluate_function_arg(&args[1], row)?;
                yachtsql_functions::hstore::hstore_slice(&hstore, &keys)
            }
            "DEFINED" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "DEFINED() requires 2 arguments (hstore, key)".to_string(),
                    ));
                }
                let hstore = self.evaluate_function_arg(&args[0], row)?;
                let key = self.evaluate_function_arg(&args[1], row)?;
                yachtsql_functions::hstore::hstore_defined(&hstore, &key)
            }
            "DELETE" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "DELETE() requires 2 arguments (hstore, key)".to_string(),
                    ));
                }
                let hstore = self.evaluate_function_arg(&args[0], row)?;
                let key = self.evaluate_function_arg(&args[1], row)?;
                yachtsql_functions::hstore::hstore_delete(&hstore, &key)
            }
            "EXIST" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "EXIST() requires 2 arguments (hstore, key)".to_string(),
                    ));
                }
                let hstore = self.evaluate_function_arg(&args[0], row)?;
                let key = self.evaluate_function_arg(&args[1], row)?;
                yachtsql_functions::hstore::hstore_exist(&hstore, &key)
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Function {} not supported in SELECT expressions",
                func_name
            ))),
        }
    }

    fn evaluate_function_arg(&self, arg: &sqlparser::ast::FunctionArg, row: &Row) -> Result<Value> {
        use sqlparser::ast::FunctionArg;
        match arg {
            FunctionArg::Unnamed(expr_arg) => {
                use sqlparser::ast::FunctionArgExpr;
                match expr_arg {
                    FunctionArgExpr::Expr(expr) => self.evaluate_expr(expr, row),
                    FunctionArgExpr::Wildcard => Err(Error::InvalidQuery(
                        "Wildcard not allowed in function arguments".to_string(),
                    )),
                    FunctionArgExpr::QualifiedWildcard(_) => Err(Error::InvalidQuery(
                        "Qualified wildcard not allowed in function arguments".to_string(),
                    )),
                }
            }
            FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                use sqlparser::ast::FunctionArgExpr;
                match arg {
                    FunctionArgExpr::Expr(expr) => self.evaluate_expr(expr, row),
                    FunctionArgExpr::Wildcard => Err(Error::InvalidQuery(
                        "Wildcard not allowed in function arguments".to_string(),
                    )),
                    FunctionArgExpr::QualifiedWildcard(_) => Err(Error::InvalidQuery(
                        "Qualified wildcard not allowed in function arguments".to_string(),
                    )),
                }
            }
        }
    }

    fn evaluate_interval(&self, interval: &sqlparser::ast::Interval) -> Result<Value> {
        use sqlparser::ast::DateTimeField;
        use yachtsql_core::types::Interval;

        let value_str = match interval.value.as_ref() {
            sqlparser::ast::Expr::Value(v) => match &v.value {
                sqlparser::ast::Value::Number(n, _) => n.clone(),
                sqlparser::ast::Value::SingleQuotedString(s)
                | sqlparser::ast::Value::DoubleQuotedString(s) => s.clone(),
                _ => {
                    return Err(Error::invalid_query(
                        "INTERVAL value must be a number or string literal".to_string(),
                    ));
                }
            },
            sqlparser::ast::Expr::UnaryOp {
                op: sqlparser::ast::UnaryOperator::Minus,
                expr,
            } => match expr.as_ref() {
                sqlparser::ast::Expr::Value(v) => match &v.value {
                    sqlparser::ast::Value::Number(n, _) => format!("-{}", n),
                    sqlparser::ast::Value::SingleQuotedString(s)
                    | sqlparser::ast::Value::DoubleQuotedString(s) => format!("-{}", s),
                    _ => {
                        return Err(Error::invalid_query(
                            "INTERVAL value must be a number or string literal".to_string(),
                        ));
                    }
                },
                _ => {
                    return Err(Error::unsupported_feature(
                        "Complex interval value expressions not supported".to_string(),
                    ));
                }
            },
            _ => {
                return Err(Error::invalid_query(
                    "INTERVAL value must be a number or string literal".to_string(),
                ));
            }
        };

        let leading_field = interval.leading_field.as_ref();

        if let Some(field) = leading_field {
            let float_value: f64 = value_str.parse().map_err(|_| {
                Error::invalid_query(format!("Invalid interval value: '{}'", value_str))
            })?;

            let interval_value = match field {
                DateTimeField::Year => Interval {
                    months: (float_value * 12.0) as i32,
                    days: 0,
                    micros: 0,
                },
                DateTimeField::Month => Interval {
                    months: float_value as i32,
                    days: 0,
                    micros: 0,
                },
                DateTimeField::Week(_) => Interval {
                    months: 0,
                    days: (float_value * 7.0) as i32,
                    micros: 0,
                },
                DateTimeField::Day => {
                    let whole_days = float_value.trunc() as i32;
                    let frac_micros = (float_value.fract() * 24.0 * 3600.0 * 1_000_000.0) as i64;
                    Interval {
                        months: 0,
                        days: whole_days,
                        micros: frac_micros,
                    }
                }
                DateTimeField::Hour => Interval {
                    months: 0,
                    days: 0,
                    micros: (float_value * 3600.0 * 1_000_000.0) as i64,
                },
                DateTimeField::Minute => Interval {
                    months: 0,
                    days: 0,
                    micros: (float_value * 60.0 * 1_000_000.0) as i64,
                },
                DateTimeField::Second => Interval {
                    months: 0,
                    days: 0,
                    micros: (float_value * 1_000_000.0) as i64,
                },
                DateTimeField::Millisecond | DateTimeField::Milliseconds => Interval {
                    months: 0,
                    days: 0,
                    micros: (float_value * 1_000.0) as i64,
                },
                DateTimeField::Microsecond | DateTimeField::Microseconds => Interval {
                    months: 0,
                    days: 0,
                    micros: float_value as i64,
                },
                _ => {
                    return Err(Error::unsupported_feature(format!(
                        "Interval field {:?} not supported",
                        field
                    )));
                }
            };

            return Ok(Value::interval(interval_value));
        }

        Self::parse_interval_string(&value_str)
    }

    fn parse_interval_string(s: &str) -> Result<Value> {
        use yachtsql_core::types::Interval;

        let mut months: i32 = 0;
        let mut days: i32 = 0;
        let mut micros: i64 = 0;

        let s_lower = s.to_lowercase();
        let tokens: Vec<&str> = s_lower.split_whitespace().collect();
        let mut i = 0;

        while i < tokens.len() {
            if let Ok(value) = tokens[i].parse::<i64>() {
                if i + 1 < tokens.len() {
                    let unit = tokens[i + 1];
                    match unit {
                        "year" | "years" => {
                            months += (value * 12) as i32;
                            i += 2;
                        }
                        "month" | "months" => {
                            months += value as i32;
                            i += 2;
                        }
                        "week" | "weeks" => {
                            days += (value * 7) as i32;
                            i += 2;
                        }
                        "day" | "days" => {
                            days += value as i32;
                            i += 2;
                        }
                        "hour" | "hours" => {
                            micros += value * 3600 * 1_000_000;
                            i += 2;
                        }
                        "minute" | "minutes" => {
                            micros += value * 60 * 1_000_000;
                            i += 2;
                        }
                        "second" | "seconds" => {
                            micros += value * 1_000_000;
                            i += 2;
                        }
                        "millisecond" | "milliseconds" => {
                            micros += value * 1_000;
                            i += 2;
                        }
                        "microsecond" | "microseconds" => {
                            micros += value;
                            i += 2;
                        }
                        _ => {
                            i += 1;
                        }
                    }
                } else {
                    days += value as i32;
                    i += 1;
                }
            } else if let Ok(value) = tokens[i].parse::<f64>() {
                if i + 1 < tokens.len() {
                    let unit = tokens[i + 1];
                    match unit {
                        "second" | "seconds" => {
                            micros += (value * 1_000_000.0) as i64;
                            i += 2;
                        }
                        "minute" | "minutes" => {
                            micros += (value * 60.0 * 1_000_000.0) as i64;
                            i += 2;
                        }
                        "hour" | "hours" => {
                            micros += (value * 3600.0 * 1_000_000.0) as i64;
                            i += 2;
                        }
                        "day" | "days" => {
                            let whole_days = value.trunc() as i32;
                            let frac_micros = (value.fract() * 24.0 * 3600.0 * 1_000_000.0) as i64;
                            days += whole_days;
                            micros += frac_micros;
                            i += 2;
                        }
                        _ => {
                            i += 1;
                        }
                    }
                } else {
                    i += 1;
                }
            } else {
                i += 1;
            }
        }

        Ok(Value::interval(Interval {
            months,
            days,
            micros,
        }))
    }

    fn parse_hstore_string(s: &str) -> Result<Value> {
        use std::rc::Rc;

        use indexmap::IndexMap;

        let mut hstore: IndexMap<String, Option<String>> = IndexMap::new();
        let s = s.trim();

        if s.is_empty() {
            return Ok(Value::hstore(hstore));
        }

        let mut key = String::new();
        let mut value = String::new();
        let mut in_key = true;
        let mut in_quotes = false;
        let mut escape_next = false;
        let mut chars = s.chars().peekable();

        while let Some(ch) = chars.next() {
            if escape_next {
                if in_key {
                    key.push(ch);
                } else {
                    value.push(ch);
                }
                escape_next = false;
                continue;
            }

            match ch {
                '\\' => {
                    escape_next = true;
                }
                '"' => {
                    in_quotes = !in_quotes;
                }
                '=' if !in_quotes => {
                    if chars.peek() == Some(&'>') {
                        chars.next();
                        in_key = false;

                        while chars.peek() == Some(&' ') {
                            chars.next();
                        }
                    } else if in_key {
                        key.push(ch);
                    } else {
                        value.push(ch);
                    }
                }
                ',' if !in_quotes => {
                    let final_value = if value.trim().eq_ignore_ascii_case("null") {
                        None
                    } else {
                        Some(value.trim().to_string())
                    };
                    hstore.insert(key.trim().to_string(), final_value);
                    key.clear();
                    value.clear();
                    in_key = true;

                    while chars.peek() == Some(&' ') {
                        chars.next();
                    }
                }
                _ => {
                    if in_key {
                        key.push(ch);
                    } else {
                        value.push(ch);
                    }
                }
            }
        }

        if !key.is_empty() || !value.is_empty() {
            let final_key = key.trim().to_string();
            let final_value = if value.trim().eq_ignore_ascii_case("null") {
                None
            } else {
                Some(value.trim().to_string())
            };
            hstore.insert(final_key, final_value);
        }

        Ok(Value::hstore(hstore))
    }

    fn validate_array_value_homogeneity(values: &[Value]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let first_type = values
            .iter()
            .find_map(|val| Self::get_value_type_category(val));

        if let Some(expected_type) = first_type {
            for val in values {
                if let Some(val_type) = Self::get_value_type_category(val) {
                    if val_type != expected_type {
                        return Err(Error::invalid_query(format!(
                            "Array elements must have compatible types, found both {:?} and {:?}",
                            expected_type, val_type
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    fn get_value_type_category(value: &Value) -> Option<&'static str> {
        if value.is_null() {
            return None;
        }

        use yachtsql_core::types::DataType;
        match value.data_type() {
            DataType::Int64 => Some("integer"),
            DataType::Float64 => Some("numeric"),
            DataType::Numeric(_) | DataType::BigNumeric => Some("numeric"),
            DataType::String => Some("string"),
            DataType::Bool => Some("boolean"),
            DataType::Date => Some("date"),
            DataType::DateTime => Some("datetime"),
            DataType::Timestamp | DataType::TimestampTz => Some("timestamp"),
            DataType::Time => Some("time"),
            DataType::Interval => Some("interval"),
            DataType::Array(_) => Some("array"),
            DataType::Bytes => Some("bytes"),
            DataType::Uuid => Some("uuid"),
            DataType::Json => Some("json"),
            DataType::Vector(_) => Some("vector"),
            DataType::Range(_) => Some("range"),
            DataType::Struct(_) => Some("struct"),
            DataType::Geography => Some("geography"),
            DataType::Point | DataType::PgBox | DataType::Circle => Some("geometric"),
            DataType::Enum { .. } => Some("enum"),
            DataType::Serial | DataType::BigSerial => Some("integer"),
            DataType::Inet => Some("inet"),
            DataType::Cidr => Some("cidr"),
            DataType::Hstore => Some("hstore"),
            DataType::Map(_, _) => Some("map"),
            DataType::Custom(_) => Some("composite"),
            DataType::Unknown => None,
            DataType::MacAddr => Some("macaddr"),
            DataType::MacAddr8 => Some("macaddr8"),
        }
    }

    fn add_interval_to_timestamp(
        &self,
        ts: DateTime<Utc>,
        interval: &yachtsql_core::types::Interval,
    ) -> Result<Value> {
        use chrono::{Datelike, Duration, Timelike};

        let naive = ts.naive_utc();

        let mut year = naive.year();
        let mut month = naive.month() as i32 + interval.months;

        while month > 12 {
            month -= 12;
            year += 1;
        }
        while month < 1 {
            month += 12;
            year -= 1;
        }

        let max_day = days_in_month(year, month as u32);
        let day = naive.day().min(max_day);

        let with_months = chrono::NaiveDate::from_ymd_opt(year, month as u32, day)
            .unwrap_or(naive.date())
            .and_hms_micro_opt(
                naive.hour(),
                naive.minute(),
                naive.second(),
                naive.nanosecond() / 1000,
            )
            .unwrap_or(naive);

        let with_days = with_months + Duration::days(interval.days as i64);

        let with_micros = with_days + Duration::microseconds(interval.micros);

        Ok(Value::timestamp(
            DateTime::<Utc>::from_naive_utc_and_offset(with_micros, Utc),
        ))
    }

    fn subtract_interval_from_timestamp(
        &self,
        ts: DateTime<Utc>,
        interval: &yachtsql_core::types::Interval,
    ) -> Result<Value> {
        let negated = yachtsql_core::types::Interval {
            months: -interval.months,
            days: -interval.days,
            micros: -interval.micros,
        };
        self.add_interval_to_timestamp(ts, &negated)
    }

    fn subtract_timestamps(&self, ts1: DateTime<Utc>, ts2: DateTime<Utc>) -> Result<Value> {
        let diff = ts1.signed_duration_since(ts2);
        let total_micros = diff.num_microseconds().unwrap_or(0);

        Ok(Value::interval(yachtsql_core::types::Interval {
            months: 0,
            days: 0,
            micros: total_micros,
        }))
    }

    fn add_interval_to_date(
        &self,
        date: chrono::NaiveDate,
        interval: &yachtsql_core::types::Interval,
    ) -> Result<Value> {
        use chrono::{Datelike, Duration};

        let mut year = date.year();
        let mut month = date.month() as i32 + interval.months;

        while month > 12 {
            month -= 12;
            year += 1;
        }
        while month < 1 {
            month += 12;
            year -= 1;
        }

        let max_day = days_in_month(year, month as u32);
        let day = date.day().min(max_day);

        let with_months = chrono::NaiveDate::from_ymd_opt(year, month as u32, day).unwrap_or(date);

        let extra_days = interval.micros / (24 * 3600 * 1_000_000);
        let with_days = with_months + Duration::days(interval.days as i64 + extra_days);

        Ok(Value::date(with_days))
    }

    fn is_plain_timestamp_expr(&self, expr: &SqlExpr) -> bool {
        use sqlparser::ast::TimezoneInfo;

        match expr {
            SqlExpr::TypedString(typed) => match &typed.data_type {
                sqlparser::ast::DataType::Timestamp(_, tz_info) => {
                    matches!(tz_info, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone)
                }
                _ => false,
            },
            SqlExpr::Nested(inner) => self.is_plain_timestamp_expr(inner),
            _ => false,
        }
    }

    fn interpret_as_timezone(&self, naive_ts: DateTime<Utc>, tz_str: &str) -> Result<Value> {
        use chrono::TimeZone;
        use chrono_tz::Tz;

        let tz: Tz = if let Ok(tz) = tz_str.parse() {
            tz
        } else {
            match tz_str.to_uppercase().as_str() {
                "UTC" | "GMT" => "UTC".parse().unwrap(),
                "EST" | "EDT" => "America/New_York".parse().unwrap(),
                "PST" | "PDT" => "America/Los_Angeles".parse().unwrap(),
                "CST" | "CDT" => "America/Chicago".parse().unwrap(),
                "MST" | "MDT" => "America/Denver".parse().unwrap(),
                "BST" => "Europe/London".parse().unwrap(),
                "CET" | "CEST" => "Europe/Paris".parse().unwrap(),
                "JST" => "Asia/Tokyo".parse().unwrap(),
                "IST" => "Asia/Kolkata".parse().unwrap(),
                "AEST" | "AEDT" => "Australia/Sydney".parse().unwrap(),
                _ => {
                    return Err(Error::InvalidQuery(format!(
                        "Unknown timezone: '{}'. Use IANA timezone names like 'America/New_York' or 'UTC'",
                        tz_str
                    )));
                }
            }
        };

        let naive_local = naive_ts.naive_utc();

        match tz.from_local_datetime(&naive_local).single() {
            Some(local_dt) => Ok(Value::timestamp(local_dt.with_timezone(&Utc))),
            None => {
                if let Some(local_dt) = tz.from_local_datetime(&naive_local).earliest() {
                    Ok(Value::timestamp(local_dt.with_timezone(&Utc)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot convert timestamp to timezone '{}': time does not exist (DST gap)",
                        tz_str
                    )))
                }
            }
        }
    }

    fn apply_at_time_zone(&self, utc_dt: DateTime<Utc>, tz_str: &str) -> Result<Value> {
        use chrono::TimeZone;
        use chrono_tz::Tz;

        let tz: Tz = if let Ok(tz) = tz_str.parse() {
            tz
        } else {
            match tz_str.to_uppercase().as_str() {
                "UTC" | "GMT" => "UTC".parse().unwrap(),
                "EST" | "EDT" => "America/New_York".parse().unwrap(),
                "PST" | "PDT" => "America/Los_Angeles".parse().unwrap(),
                "CST" | "CDT" => "America/Chicago".parse().unwrap(),
                "MST" | "MDT" => "America/Denver".parse().unwrap(),
                "BST" => "Europe/London".parse().unwrap(),
                "CET" | "CEST" => "Europe/Paris".parse().unwrap(),
                "JST" => "Asia/Tokyo".parse().unwrap(),
                "IST" => "Asia/Kolkata".parse().unwrap(),
                "AEST" | "AEDT" => "Australia/Sydney".parse().unwrap(),
                _ => {
                    return Err(Error::InvalidQuery(format!(
                        "Unknown timezone: '{}'. Use IANA timezone names like 'America/New_York' or 'UTC'",
                        tz_str
                    )));
                }
            }
        };

        let local_dt = utc_dt.with_timezone(&tz);

        let naive_local = local_dt.naive_local();
        Ok(Value::timestamp(
            DateTime::<Utc>::from_naive_utc_and_offset(naive_local, Utc),
        ))
    }
}

fn compile_like_pattern(pattern: &str, escape_char: Option<char>) -> Result<Regex> {
    let mut regex = String::from("^");
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        if Some(ch) == escape_char {
            if let Some(next) = chars.next() {
                regex.push_str(&regex::escape(&next.to_string()));
            } else {
                return Err(Error::invalid_query(
                    "LIKE pattern ends with escape character".to_string(),
                ));
            }
            continue;
        }

        match ch {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            _ => regex.push_str(&regex::escape(&ch.to_string())),
        }
    }
    regex.push('$');
    Regex::new(&regex)
        .map_err(|e| Error::invalid_query(format!("Invalid LIKE pattern '{}': {}", pattern, e)))
}

fn compile_ilike_pattern(pattern: &str, escape_char: Option<char>) -> Result<Regex> {
    let mut regex = String::from("^");
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        if Some(ch) == escape_char {
            if let Some(next) = chars.next() {
                regex.push_str(&regex::escape(&next.to_string()));
            } else {
                return Err(Error::invalid_query(
                    "ILIKE pattern ends with escape character".to_string(),
                ));
            }
            continue;
        }

        match ch {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            _ => regex.push_str(&regex::escape(&ch.to_string())),
        }
    }
    regex.push('$');
    regex::RegexBuilder::new(&regex)
        .case_insensitive(true)
        .build()
        .map_err(|e| Error::invalid_query(format!("Invalid ILIKE pattern '{}': {}", pattern, e)))
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

fn parse_roman_numeral(s: &str) -> Result<i64> {
    let s = s.trim().to_uppercase();
    if s.is_empty() {
        return Err(Error::InvalidQuery(
            "Empty Roman numeral string".to_string(),
        ));
    }

    let mut result: i64 = 0;
    let mut prev_value: i64 = 0;

    for c in s.chars().rev() {
        let value = match c {
            'I' => 1,
            'V' => 5,
            'X' => 10,
            'L' => 50,
            'C' => 100,
            'D' => 500,
            'M' => 1000,
            _ => {
                return Err(Error::InvalidQuery(format!(
                    "Invalid Roman numeral character: '{}'",
                    c
                )));
            }
        };

        if value < prev_value {
            result -= value;
        } else {
            result += value;
        }
        prev_value = value;
    }

    Ok(result)
}

#[allow(clippy::excessive_precision)]
const LANCZOS_COEF: [f64; 9] = [
    0.99999999999980993,
    676.5203681218851,
    -1259.1392167224028,
    771.32342877765313,
    -176.61502916214059,
    12.507343278686905,
    -0.13857109526572012,
    9.9843695780195716e-6,
    1.5056327351493116e-7,
];

fn gamma_function(x: f64) -> f64 {
    if x.is_nan() {
        return f64::NAN;
    }
    if x.is_infinite() {
        return if x > 0.0 { f64::INFINITY } else { f64::NAN };
    }

    if x <= 0.0 && x.fract() == 0.0 {
        return f64::NAN;
    }

    if x < 0.5 {
        let pi = std::f64::consts::PI;
        return pi / ((pi * x).sin() * gamma_function(1.0 - x));
    }

    let z = x - 1.0;
    let mut ag = LANCZOS_COEF[0];
    for (i, &coef) in LANCZOS_COEF.iter().enumerate().skip(1) {
        ag += coef / (z + i as f64);
    }
    let t = z + 7.5;
    let sqrt_2pi = (2.0 * std::f64::consts::PI).sqrt();
    sqrt_2pi * t.powf(z + 0.5) * (-t).exp() * ag
}

fn lgamma_function(x: f64) -> f64 {
    if x.is_nan() {
        return f64::NAN;
    }
    if x.is_infinite() {
        return if x > 0.0 { f64::INFINITY } else { f64::NAN };
    }

    if x <= 0.0 && x.fract() == 0.0 {
        return f64::INFINITY;
    }

    if x > 0.0 {
        let z = x - 1.0;
        let mut ag = LANCZOS_COEF[0];
        for (i, &coef) in LANCZOS_COEF.iter().enumerate().skip(1) {
            ag += coef / (z + i as f64);
        }
        let t = z + 7.5;
        let log_sqrt_2pi = 0.5 * (2.0 * std::f64::consts::PI).ln();
        return log_sqrt_2pi + (z + 0.5) * t.ln() - t + ag.ln();
    }

    let pi = std::f64::consts::PI;
    let sin_val = (pi * x).sin().abs();
    pi.ln() - sin_val.ln() - lgamma_function(1.0 - x)
}

#[cfg(test)]
mod tests;
