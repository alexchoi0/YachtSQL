use std::collections::HashMap;
use std::rc::Rc;

use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;
use yachtsql_storage::{Column, Field, Schema};

use super::ExecutionPlan;
use crate::RecordBatch;

#[derive(Debug)]
pub struct AggregateExec {
    input: Rc<dyn ExecutionPlan>,
    schema: Schema,
    group_by: Vec<Expr>,
    aggregates: Vec<(Expr, Option<String>)>,
    having: Option<Expr>,
}

impl AggregateExec {
    pub(crate) fn contains_aggregate(expr: &Expr) -> bool {
        match expr {
            Expr::Aggregate { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::contains_aggregate(left) || Self::contains_aggregate(right)
            }
            Expr::UnaryOp { expr, .. } => Self::contains_aggregate(expr),
            Expr::Function { args, .. } => args.iter().any(|a| Self::contains_aggregate(a)),
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand.as_ref().map_or(false, |o| Self::contains_aggregate(o))
                    || when_then
                        .iter()
                        .any(|(w, t)| Self::contains_aggregate(w) || Self::contains_aggregate(t))
                    || else_expr
                        .as_ref()
                        .map_or(false, |e| Self::contains_aggregate(e))
            }
            Expr::Cast { expr, .. } | Expr::TryCast { expr, .. } => Self::contains_aggregate(expr),
            Expr::InList { expr, list, .. } => {
                Self::contains_aggregate(expr) || list.iter().any(|e| Self::contains_aggregate(e))
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::contains_aggregate(expr)
                    || Self::contains_aggregate(low)
                    || Self::contains_aggregate(high)
            }
            _ => false,
        }
    }

    pub fn new(
        input: Rc<dyn ExecutionPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<(Expr, Option<String>)>,
        having: Option<Expr>,
    ) -> Result<Self> {
        use yachtsql_core::error::Error;

        for (agg_expr, _) in &aggregates {
            if let Expr::Aggregate { filter, .. } = agg_expr {
                if let Some(filter_expr) = filter {
                    if Self::contains_aggregate(filter_expr) {
                        return Err(Error::InvalidQuery(
                            "FILTER clause cannot contain aggregate functions".to_string(),
                        ));
                    }
                }
            }
        }

        let mut fields = Vec::new();

        let input_schema = input.schema();
        for (idx, group_expr) in group_by.iter().enumerate() {
            let field_name = if let Expr::Column { name, .. } = group_expr {
                name.clone()
            } else {
                format!("group_{}", idx)
            };

            let data_type = Self::infer_expr_type(group_expr, input_schema)
                .unwrap_or(yachtsql_core::types::DataType::String);

            fields.push(Field::nullable(field_name, data_type));
        }

        for (idx, (agg_expr, alias)) in aggregates.iter().enumerate() {
            let field_name = alias.clone().unwrap_or_else(|| {
                Self::expr_to_field_name(agg_expr).unwrap_or_else(|| format!("agg_{}", idx))
            });

            let data_type = Self::infer_aggregate_type(agg_expr, input_schema)
                .unwrap_or(yachtsql_core::types::DataType::Float64);
            fields.push(Field::nullable(field_name, data_type));
        }

        let schema = Schema::from_fields(fields);

        Ok(Self {
            input,
            schema,
            group_by,
            aggregates,
            having,
        })
    }

    fn compute_group_key(&self, batch: &RecordBatch, row_idx: usize) -> Result<Vec<Value>> {
        let mut key = Vec::with_capacity(self.group_by.len());
        for expr in &self.group_by {
            let value = self.evaluate_expr(expr, batch, row_idx)?;
            key.push(value);
        }
        Ok(key)
    }

    fn evaluate_expr(&self, expr: &Expr, batch: &RecordBatch, row_idx: usize) -> Result<Value> {
        use super::ProjectionWithExprExec;
        ProjectionWithExprExec::evaluate_expr(expr, batch, row_idx)
    }

    fn evaluate_aggregate_arg(
        &self,
        agg_expr: &Expr,
        batch: &RecordBatch,
        row_idx: usize,
    ) -> Result<Value> {
        match agg_expr {
            Expr::Aggregate { name, args, filter, .. } => {
                if let Some(filter_expr) = filter {
                    let filter_result = self.evaluate_expr(filter_expr, batch, row_idx)?;
                    if filter_result.as_bool() != Some(true) {
                        return Ok(Value::null());
                    }
                }
                if args.is_empty() {
                    Ok(Value::int64(1))
                } else if args.len() >= 2
                    && matches!(
                        name.as_str(),
                        "CORR" | "COVAR_POP" | "COVAR_SAMP" | "REGR_SLOPE" | "REGR_INTERCEPT"
                    )
                {
                    let mut values = Vec::with_capacity(args.len());
                    for arg in args {
                        values.push(self.evaluate_expr(arg, batch, row_idx)?);
                    }
                    Ok(Value::array(values))
                } else {
                    self.evaluate_expr(&args[0], batch, row_idx)
                }
            }

            _ => self.evaluate_expr(agg_expr, batch, row_idx),
        }
    }

    pub(crate) fn infer_expr_type(
        expr: &Expr,
        schema: &Schema,
    ) -> Option<yachtsql_core::types::DataType> {
        match expr {
            Expr::Column { name, .. } => schema
                .fields()
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(name))
                .map(|f| f.data_type.clone()),
            Expr::Literal(lit) => {
                use yachtsql_core::types::DataType;
                use yachtsql_ir::expr::LiteralValue;
                Some(match lit {
                    LiteralValue::Int64(_) => DataType::Int64,
                    LiteralValue::Float64(_) => DataType::Float64,
                    LiteralValue::Numeric(_) => DataType::Numeric(None),
                    LiteralValue::String(_) => DataType::String,
                    LiteralValue::Boolean(_) => DataType::Bool,
                    LiteralValue::Date(_) => DataType::Date,
                    LiteralValue::Timestamp(_) => DataType::Timestamp,
                    LiteralValue::Json(_) => DataType::Json,
                    LiteralValue::Bytes(_) => DataType::Bytes,
                    LiteralValue::Uuid(_) => DataType::Uuid,
                    LiteralValue::Interval(_) => DataType::Interval,
                    LiteralValue::Array(elements) => {
                        let elem_type = elements
                            .first()
                            .and_then(|e| Self::infer_expr_type(e, schema))
                            .unwrap_or(DataType::String);
                        DataType::Array(Box::new(elem_type))
                    }
                    LiteralValue::Vector(v) => DataType::Vector(v.len()),
                    LiteralValue::Null => return None,
                    LiteralValue::Range(_) => {
                        DataType::Range(yachtsql_core::types::RangeType::Int4Range)
                    }
                    LiteralValue::Point(_) => DataType::Point,
                    LiteralValue::PgBox(_) => DataType::PgBox,
                    LiteralValue::Circle(_) => DataType::Circle,
                    LiteralValue::MacAddr(_) => DataType::MacAddr,
                    LiteralValue::MacAddr8(_) => DataType::MacAddr8,
                })
            }
            Expr::Cast { data_type, .. } | Expr::TryCast { data_type, .. } => {
                use yachtsql_core::types::DataType;
                use yachtsql_ir::expr::CastDataType;
                Some(match data_type {
                    CastDataType::Int64 => DataType::Int64,
                    CastDataType::Float64 => DataType::Float64,
                    CastDataType::Numeric(p) => DataType::Numeric(*p),
                    CastDataType::String => DataType::String,
                    CastDataType::Bool => DataType::Bool,
                    CastDataType::Date => DataType::Date,
                    CastDataType::DateTime => DataType::DateTime,
                    CastDataType::Time => DataType::Time,
                    CastDataType::Timestamp => DataType::Timestamp,
                    CastDataType::TimestampTz => DataType::TimestampTz,
                    CastDataType::Bytes => DataType::Bytes,
                    CastDataType::Json => DataType::Json,
                    CastDataType::Uuid => DataType::Uuid,
                    CastDataType::Interval => DataType::Interval,
                    CastDataType::Geography => DataType::Geography,
                    CastDataType::Vector(dims) => DataType::Vector(*dims),
                    CastDataType::Hstore => DataType::Hstore,
                    CastDataType::MacAddr => DataType::MacAddr,
                    CastDataType::MacAddr8 => DataType::MacAddr8,
                    CastDataType::Array(inner) => {
                        let inner_expr = Expr::Cast {
                            expr: Box::new(Expr::Literal(yachtsql_ir::expr::LiteralValue::Null)),
                            data_type: (**inner).clone(),
                        };
                        let inner_type =
                            Self::infer_expr_type(&inner_expr, schema).unwrap_or(DataType::String);
                        DataType::Array(Box::new(inner_type))
                    }
                    CastDataType::Custom(name, _) => DataType::Custom(name.clone()),
                })
            }
            Expr::BinaryOp { left, op, right } => {
                use yachtsql_core::types::DataType;
                use yachtsql_ir::expr::BinaryOp;
                match op {
                    BinaryOp::Equal
                    | BinaryOp::NotEqual
                    | BinaryOp::LessThan
                    | BinaryOp::LessThanOrEqual
                    | BinaryOp::GreaterThan
                    | BinaryOp::GreaterThanOrEqual
                    | BinaryOp::And
                    | BinaryOp::Or => Some(DataType::Bool),
                    BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide => {
                        let left_type = Self::infer_expr_type(left, schema);
                        let right_type = Self::infer_expr_type(right, schema);
                        match (&left_type, &right_type) {
                            (Some(DataType::Float64), _) | (_, Some(DataType::Float64)) => {
                                Some(DataType::Float64)
                            }
                            (Some(DataType::Numeric(_)), _) | (_, Some(DataType::Numeric(_))) => {
                                Some(DataType::Numeric(None))
                            }
                            (Some(DataType::Int64), _) | (_, Some(DataType::Int64)) => {
                                Some(DataType::Int64)
                            }
                            _ => left_type.or(right_type),
                        }
                    }
                    _ => None,
                }
            }
            Expr::Function { name, args } => {
                use yachtsql_core::types::DataType;
                match name.as_str() {
                    "COUNT" | "LENGTH" | "CHAR_LENGTH" => Some(DataType::Int64),
                    "SUM" | "MIN" | "MAX" => {
                        args.first().and_then(|a| Self::infer_expr_type(a, schema))
                    }
                    "AVG" | "STDDEV" | "VARIANCE" => Some(DataType::Float64),
                    "CONCAT" | "UPPER" | "LOWER" | "TRIM" => Some(DataType::String),
                    "COALESCE" => args.iter().find_map(|a| Self::infer_expr_type(a, schema)),
                    _ => None,
                }
            }
            Expr::Case {
                when_then,
                else_expr,
                ..
            } => {
                for (_, then_expr) in when_then {
                    if let Some(t) = Self::infer_expr_type(then_expr, schema) {
                        return Some(t);
                    }
                }
                if let Some(else_e) = else_expr {
                    return Self::infer_expr_type(else_e, schema);
                }
                None
            }
            _ => None,
        }
    }

    pub(crate) fn infer_aggregate_type(
        expr: &Expr,
        schema: &Schema,
    ) -> Option<yachtsql_core::types::DataType> {
        use yachtsql_core::types::DataType;
        use yachtsql_ir::FunctionName;

        match expr {
            Expr::Aggregate { name, args, .. } => match name {
                FunctionName::Count => Some(DataType::Int64),
                FunctionName::Sum | FunctionName::Min | FunctionName::Max => {
                    if let Some(arg) = args.first() {
                        Self::infer_expr_type(arg, schema)
                    } else {
                        Some(DataType::Float64)
                    }
                }
                FunctionName::Avg => {
                    if let Some(arg) = args.first() {
                        let input_type = Self::infer_expr_type(arg, schema);
                        match input_type {
                            Some(DataType::Numeric(precision)) => {
                                Some(DataType::Numeric(precision))
                            }
                            _ => Some(DataType::Float64),
                        }
                    } else {
                        Some(DataType::Float64)
                    }
                }

                FunctionName::Stddev
                | FunctionName::StddevPop
                | FunctionName::StddevSamp
                | FunctionName::Variance
                | FunctionName::VarPop
                | FunctionName::VarSamp => Some(DataType::Float64),

                FunctionName::ApproxQuantiles => Some(DataType::Array(Box::new(DataType::Float64))),

                FunctionName::ArrayAgg => {
                    if let Some(arg) = args.first() {
                        let elem_type =
                            Self::infer_expr_type(arg, schema).unwrap_or(DataType::String);
                        Some(DataType::Array(Box::new(elem_type)))
                    } else {
                        Some(DataType::Array(Box::new(DataType::String)))
                    }
                }

                FunctionName::StringAgg => Some(DataType::String),

                _ => Some(DataType::Float64),
            },
            _ => None,
        }
    }

    fn evaluate_having(&self, _group_values: &[Value], _agg_values: &[Value]) -> Result<bool> {
        match &self.having {
            None => Ok(true),
            Some(_having_expr) => Ok(true),
        }
    }

    pub(crate) fn expr_to_field_name(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Aggregate {
                name,
                args,
                distinct,
                ..
            } => {
                let first_is_wildcard = args.first().is_some_and(|e| matches!(e, Expr::Wildcard));
                let arg_str = if args.is_empty() || first_is_wildcard {
                    "*".to_string()
                } else if let Some(Expr::Column { name: col_name, .. }) = args.first() {
                    col_name.clone()
                } else {
                    "...".to_string()
                };
                if *distinct {
                    Some(format!("{}(DISTINCT {})", name.as_str(), arg_str))
                } else {
                    Some(format!("{}({})", name.as_str(), arg_str))
                }
            }
            Expr::Column { name, .. } => Some(name.clone()),
            Expr::Literal(lit) => Some(format!("{:?}", lit)),
            _ => None,
        }
    }
}

impl ExecutionPlan for AggregateExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let input_batches = self.input.execute()?;

        if input_batches.is_empty() {
            return Ok(vec![RecordBatch::empty(self.schema.clone())]);
        }

        let mut groups: HashMap<Vec<u8>, (Vec<Value>, Vec<Vec<Value>>)> = HashMap::new();

        for input_batch in input_batches {
            let num_rows = input_batch.num_rows();

            for row_idx in 0..num_rows {
                let group_key = self.compute_group_key(&input_batch, row_idx)?;
                let key_bytes = serialize_key(&group_key);

                let mut agg_input_values = Vec::new();
                for (agg_expr, _) in &self.aggregates {
                    let value = self.evaluate_aggregate_arg(agg_expr, &input_batch, row_idx)?;
                    agg_input_values.push(value);
                }

                groups
                    .entry(key_bytes)
                    .or_insert_with(|| (group_key.clone(), Vec::new()))
                    .1
                    .push(agg_input_values);
            }
        }

        if groups.is_empty() && self.group_by.is_empty() {
            let empty_agg_values: Vec<Value> = self
                .aggregates
                .iter()
                .map(|(agg_expr, _)| match agg_expr {
                    Expr::Aggregate { name, .. } => {
                        use yachtsql_ir::FunctionName;
                        match name {
                            FunctionName::Count => Value::int64(0),
                            _ => Value::null(),
                        }
                    }
                    _ => Value::null(),
                })
                .collect();

            let mut columns = Vec::new();

            for (idx, field) in self.schema.fields().iter().enumerate() {
                let mut column = Column::new(&field.data_type, 1);
                column.push(empty_agg_values.get(idx).cloned().unwrap_or(Value::null()))?;
                columns.push(column);
            }

            return Ok(vec![RecordBatch::new(self.schema.clone(), columns)?]);
        }

        let mut result_rows = Vec::new();

        for (group_values, agg_input_rows) in groups.values() {
            let agg_values = self.compute_aggregates(agg_input_rows)?;

            if self.evaluate_having(group_values, &agg_values)? {
                let mut row = group_values.clone();
                row.extend(agg_values);
                result_rows.push(row);
            }
        }

        if result_rows.is_empty() {
            return Ok(vec![RecordBatch::empty(self.schema.clone())]);
        }

        let num_output_rows = result_rows.len();
        let num_cols = self.schema.fields().len();
        let mut columns = Vec::new();

        for col_idx in 0..num_cols {
            let field = &self.schema.fields()[col_idx];
            let mut column = Column::new(&field.data_type, num_output_rows);

            for row in &result_rows {
                column.push(row[col_idx].clone())?;
            }

            columns.push(column);
        }

        Ok(vec![RecordBatch::new(self.schema.clone(), columns)?])
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn describe(&self) -> String {
        format!(
            "Aggregate [group_by: {}, aggregates: {}]",
            self.group_by.len(),
            self.aggregates.len()
        )
    }
}

impl AggregateExec {
    fn try_create_column(values: &[&Value]) -> Option<Column> {
        if values.is_empty() {
            return None;
        }

        let first_type = values.iter().find(|v| !v.is_null())?;

        if first_type.as_i64().is_some() {
            let mut column = Column::new(&yachtsql_core::types::DataType::Int64, values.len());
            for val in values {
                if column.push((*val).clone()).is_err() {
                    return None;
                }
            }
            return Some(column);
        }

        if first_type.as_f64().is_some() {
            let mut column = Column::new(&yachtsql_core::types::DataType::Float64, values.len());
            for val in values {
                if column.push((*val).clone()).is_err() {
                    return None;
                }
            }
            return Some(column);
        }

        None
    }

    fn compute_aggregates(&self, agg_input_rows: &[Vec<Value>]) -> Result<Vec<Value>> {
        let mut result = Vec::with_capacity(self.aggregates.len());

        for agg_idx in 0..self.aggregates.len() {
            let values: Vec<&Value> = agg_input_rows.iter().map(|row| &row[agg_idx]).collect();

            let agg_result = match &self.aggregates[agg_idx].0 {
                Expr::Aggregate {
                    name,
                    args,
                    distinct,
                    ..
                } => match name.as_str() {
                    "COUNT" => {
                        if *distinct {
                            let mut unique_values = std::collections::HashSet::new();
                            for val in &values {
                                if !val.is_null() {
                                    unique_values.insert(format!("{:?}", *val));
                                }
                            }
                            Value::int64(unique_values.len() as i64)
                        } else {
                            let count = values.iter().filter(|v| !v.is_null()).count();
                            Value::int64(count as i64)
                        }
                    }
                    "COUNTIF" => {
                        let count = values.iter().filter(|v| v.as_bool() == Some(true)).count();
                        Value::int64(count as i64)
                    }
                    "SUM" => {
                        let has_numeric = values.iter().any(|v| v.as_numeric().is_some());

                        if has_numeric {
                            let mut sum = rust_decimal::Decimal::ZERO;
                            let mut has_values = false;
                            for val in &values {
                                if let Some(n) = val.as_numeric() {
                                    sum += n;
                                    has_values = true;
                                } else if let Some(i) = val.as_i64() {
                                    sum += rust_decimal::Decimal::from(i);
                                    has_values = true;
                                } else if let Some(f) = val.as_f64() {
                                    if let Some(d) = rust_decimal::Decimal::from_f64_retain(f) {
                                        sum += d;
                                        has_values = true;
                                    }
                                }
                            }
                            if has_values {
                                Value::numeric(sum)
                            } else {
                                Value::null()
                            }
                        } else if let Some(column) = Self::try_create_column(&values) {
                            match column {
                                Column::Int64 { .. } => {
                                    if let Ok(sum) = column.sum_i64() {
                                        Value::int64(sum)
                                    } else {
                                        Value::null()
                                    }
                                }
                                Column::Float64 { .. } => {
                                    if let Ok(sum) = column.sum_f64() {
                                        Value::float64(sum)
                                    } else {
                                        Value::null()
                                    }
                                }
                                _ => {
                                    let mut sum = 0.0;
                                    let mut has_values = false;
                                    for val in values {
                                        if let Some(i) = val.as_i64() {
                                            sum += i as f64;
                                            has_values = true;
                                        } else if let Some(f) = val.as_f64() {
                                            sum += f;
                                            has_values = true;
                                        }
                                    }
                                    if has_values {
                                        Value::float64(sum)
                                    } else {
                                        Value::null()
                                    }
                                }
                            }
                        } else {
                            let mut sum = 0.0;
                            let mut has_values = false;
                            for val in values {
                                if let Some(i) = val.as_i64() {
                                    sum += i as f64;
                                    has_values = true;
                                } else if let Some(f) = val.as_f64() {
                                    sum += f;
                                    has_values = true;
                                }
                            }
                            if has_values {
                                Value::float64(sum)
                            } else {
                                Value::null()
                            }
                        }
                    }
                    "AVG" => {
                        let has_numeric = values.iter().any(|v| v.as_numeric().is_some());

                        if has_numeric {
                            let mut sum = rust_decimal::Decimal::ZERO;
                            let mut count = 0u32;
                            for val in &values {
                                if let Some(n) = val.as_numeric() {
                                    sum += n;
                                    count += 1;
                                } else if let Some(i) = val.as_i64() {
                                    sum += rust_decimal::Decimal::from(i);
                                    count += 1;
                                } else if let Some(f) = val.as_f64() {
                                    if let Some(d) = rust_decimal::Decimal::from_f64_retain(f) {
                                        sum += d;
                                        count += 1;
                                    }
                                }
                            }
                            if count > 0 {
                                let avg = sum / rust_decimal::Decimal::from(count);
                                Value::numeric(avg)
                            } else {
                                Value::null()
                            }
                        } else if let Some(column) = Self::try_create_column(&values) {
                            match column {
                                Column::Int64 { .. } => {
                                    if let Ok(Some(avg)) = column.avg_i64() {
                                        Value::float64(avg)
                                    } else {
                                        Value::null()
                                    }
                                }
                                Column::Float64 { .. } => {
                                    if let Ok(Some(avg)) = column.avg_f64() {
                                        Value::float64(avg)
                                    } else {
                                        Value::null()
                                    }
                                }
                                _ => {
                                    let mut sum = 0.0;
                                    let mut count = 0;
                                    for val in values {
                                        if let Some(i) = val.as_i64() {
                                            sum += i as f64;
                                            count += 1;
                                        } else if let Some(f) = val.as_f64() {
                                            sum += f;
                                            count += 1;
                                        }
                                    }
                                    if count > 0 {
                                        Value::float64(sum / count as f64)
                                    } else {
                                        Value::null()
                                    }
                                }
                            }
                        } else {
                            let mut sum = 0.0;
                            let mut count = 0;
                            for val in values {
                                if let Some(i) = val.as_i64() {
                                    sum += i as f64;
                                    count += 1;
                                } else if let Some(f) = val.as_f64() {
                                    sum += f;
                                    count += 1;
                                }
                            }
                            if count > 0 {
                                Value::float64(sum / count as f64)
                            } else {
                                Value::null()
                            }
                        }
                    }
                    "MIN" => {
                        if let Some(column) = Self::try_create_column(&values) {
                            match column {
                                Column::Int64 { .. } => {
                                    if let Ok(Some(min)) = column.min_i64() {
                                        Value::int64(min)
                                    } else {
                                        Value::null()
                                    }
                                }
                                _ => {
                                    let mut min: Option<Value> = None;
                                    for val in values {
                                        if val.is_null() {
                                            continue;
                                        }
                                        min = Some(match min {
                                            None => (*val).clone(),
                                            Some(ref current_min) => {
                                                if compare_values(val, current_min)?
                                                    == std::cmp::Ordering::Less
                                                {
                                                    (*val).clone()
                                                } else {
                                                    current_min.clone()
                                                }
                                            }
                                        });
                                    }
                                    min.unwrap_or(Value::null())
                                }
                            }
                        } else {
                            let mut min: Option<Value> = None;
                            for val in values {
                                if val.is_null() {
                                    continue;
                                }
                                min = Some(match min {
                                    None => (*val).clone(),
                                    Some(ref current_min) => {
                                        if compare_values(val, current_min)?
                                            == std::cmp::Ordering::Less
                                        {
                                            (*val).clone()
                                        } else {
                                            current_min.clone()
                                        }
                                    }
                                });
                            }
                            min.unwrap_or(Value::null())
                        }
                    }
                    "MAX" => {
                        if let Some(column) = Self::try_create_column(&values) {
                            match column {
                                Column::Int64 { .. } => {
                                    if let Ok(Some(max)) = column.max_i64() {
                                        Value::int64(max)
                                    } else {
                                        Value::null()
                                    }
                                }
                                _ => {
                                    let mut max: Option<Value> = None;
                                    for val in values {
                                        if val.is_null() {
                                            continue;
                                        }
                                        max = Some(match max {
                                            None => (*val).clone(),
                                            Some(ref current_max) => {
                                                if compare_values(val, current_max)?
                                                    == std::cmp::Ordering::Greater
                                                {
                                                    (*val).clone()
                                                } else {
                                                    current_max.clone()
                                                }
                                            }
                                        });
                                    }
                                    max.unwrap_or(Value::null())
                                }
                            }
                        } else {
                            let mut max: Option<Value> = None;
                            for val in values {
                                if val.is_null() {
                                    continue;
                                }
                                max = Some(match max {
                                    None => (*val).clone(),
                                    Some(ref current_max) => {
                                        if compare_values(val, current_max)?
                                            == std::cmp::Ordering::Greater
                                        {
                                            (*val).clone()
                                        } else {
                                            current_max.clone()
                                        }
                                    }
                                });
                            }
                            max.unwrap_or(Value::null())
                        }
                    }
                    "VARIANCE" | "VAR_SAMP" | "STDDEV_SAMP" | "STDDEV" | "VAR_POP"
                    | "STDDEV_POP" => {
                        if let Some(column) = Self::try_create_column(&values) {
                            match column {
                                Column::Float64 { .. } => {
                                    let result = match name.as_str() {
                                        "VAR_POP" => column.variance_pop_f64(),
                                        "VAR_SAMP" | "VARIANCE" => column.variance_samp_f64(),
                                        "STDDEV_POP" => column.stddev_pop_f64(),
                                        "STDDEV_SAMP" | "STDDEV" => column.stddev_samp_f64(),
                                        _ => Ok(None),
                                    };
                                    match result {
                                        Ok(Some(val)) => Value::float64(val),
                                        _ => Value::null(),
                                    }
                                }
                                Column::Int64 { data, nulls } => {
                                    let mut float_data =
                                        aligned_vec::AVec::with_capacity(64, data.len());
                                    for &val in data.as_slice() {
                                        float_data.push(val as f64);
                                    }
                                    let float_col = Column::Float64 {
                                        data: float_data,
                                        nulls: nulls.clone(),
                                    };
                                    let result = match name.as_str() {
                                        "VAR_POP" => float_col.variance_pop_f64(),
                                        "VAR_SAMP" | "VARIANCE" => float_col.variance_samp_f64(),
                                        "STDDEV_POP" => float_col.stddev_pop_f64(),
                                        "STDDEV_SAMP" | "STDDEV" => float_col.stddev_samp_f64(),
                                        _ => Ok(None),
                                    };
                                    match result {
                                        Ok(Some(val)) => Value::float64(val),
                                        _ => Value::null(),
                                    }
                                }
                                _ => Value::null(),
                            }
                        } else {
                            Value::null()
                        }
                    }
                    "APPROX_QUANTILES" => {
                        let num_quantiles = if args.len() >= 2 {
                            match &args[1] {
                                Expr::Literal(yachtsql_ir::expr::LiteralValue::Int64(n)) => {
                                    *n as usize
                                }
                                _ => 4,
                            }
                        } else {
                            4
                        };

                        let mut numeric_values: Vec<f64> = values
                            .iter()
                            .filter_map(|v| {
                                if v.is_null() {
                                    None
                                } else if let Some(i) = v.as_i64() {
                                    Some(i as f64)
                                } else if let Some(f) = v.as_f64() {
                                    Some(f)
                                } else {
                                    None
                                }
                            })
                            .collect();

                        if numeric_values.is_empty() {
                            Value::null()
                        } else {
                            numeric_values.sort_by(|a, b| {
                                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                            });

                            let mut quantile_values = Vec::with_capacity(num_quantiles + 1);
                            let len = numeric_values.len();

                            for i in 0..=num_quantiles {
                                let p = i as f64 / num_quantiles as f64;
                                let idx = (p * (len - 1) as f64).round() as usize;
                                quantile_values.push(Value::float64(numeric_values[idx]));
                            }

                            Value::array(quantile_values)
                        }
                    }
                    "ARRAY_AGG" => {
                        let array_values: Vec<Value> =
                            values.iter().map(|v| (*v).clone()).collect();
                        if *distinct {
                            let mut unique_strs = std::collections::HashSet::new();
                            let mut unique_values = Vec::new();
                            for val in array_values {
                                let str_repr = format!("{:?}", val);
                                if unique_strs.insert(str_repr) {
                                    unique_values.push(val);
                                }
                            }
                            Value::array(unique_values)
                        } else {
                            Value::array(array_values)
                        }
                    }
                    "STRING_AGG" => {
                        let delimiter = if args.len() >= 2 {
                            match &args[1] {
                                Expr::Literal(yachtsql_ir::expr::LiteralValue::String(s)) => {
                                    s.clone()
                                }
                                _ => String::new(),
                            }
                        } else {
                            String::new()
                        };

                        let string_values: Vec<String> = values
                            .iter()
                            .filter_map(|v| {
                                if v.is_null() {
                                    None
                                } else {
                                    v.as_str().map(|s| s.to_string())
                                }
                            })
                            .collect();

                        if string_values.is_empty() {
                            Value::null()
                        } else {
                            Value::string(string_values.join(&delimiter))
                        }
                    }
                    "CORR" => {
                        let mut count = 0usize;
                        let mut mean_x = 0.0f64;
                        let mut mean_y = 0.0f64;
                        let mut m2_x = 0.0f64;
                        let mut m2_y = 0.0f64;
                        let mut coproduct = 0.0f64;

                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    if let (Some(x), Some(y)) = (arr[0].as_f64(), arr[1].as_f64()) {
                                        count += 1;
                                        let n = count as f64;
                                        let delta_x = x - mean_x;
                                        let delta_y = y - mean_y;
                                        mean_x += delta_x / n;
                                        mean_y += delta_y / n;
                                        let delta_x2 = x - mean_x;
                                        let delta_y2 = y - mean_y;
                                        m2_x += delta_x * delta_x2;
                                        m2_y += delta_y * delta_y2;
                                        coproduct += delta_x * delta_y2;
                                    }
                                }
                            }
                        }

                        if count < 2 {
                            Value::null()
                        } else {
                            let var_x = m2_x / count as f64;
                            let var_y = m2_y / count as f64;
                            if var_x == 0.0 || var_y == 0.0 {
                                Value::null()
                            } else {
                                let corr = coproduct / (count as f64 * var_x.sqrt() * var_y.sqrt());
                                Value::float64(corr)
                            }
                        }
                    }
                    "COVAR_POP" => {
                        let mut count = 0usize;
                        let mut mean_x = 0.0f64;
                        let mut mean_y = 0.0f64;
                        let mut coproduct = 0.0f64;

                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    if let (Some(x), Some(y)) = (arr[0].as_f64(), arr[1].as_f64()) {
                                        count += 1;
                                        let n = count as f64;
                                        let delta_x = x - mean_x;
                                        mean_x += delta_x / n;
                                        let delta_y = y - mean_y;
                                        mean_y += delta_y / n;
                                        let delta_y2 = y - mean_y;
                                        coproduct += delta_x * delta_y2;
                                    }
                                }
                            }
                        }

                        if count == 0 {
                            Value::null()
                        } else {
                            Value::float64(coproduct / count as f64)
                        }
                    }
                    "COVAR_SAMP" => {
                        let mut count = 0usize;
                        let mut mean_x = 0.0f64;
                        let mut mean_y = 0.0f64;
                        let mut coproduct = 0.0f64;

                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    if let (Some(x), Some(y)) = (arr[0].as_f64(), arr[1].as_f64()) {
                                        count += 1;
                                        let n = count as f64;
                                        let delta_x = x - mean_x;
                                        mean_x += delta_x / n;
                                        let delta_y = y - mean_y;
                                        mean_y += delta_y / n;
                                        let delta_y2 = y - mean_y;
                                        coproduct += delta_x * delta_y2;
                                    }
                                }
                            }
                        }

                        if count < 2 {
                            Value::null()
                        } else {
                            Value::float64(coproduct / (count - 1) as f64)
                        }
                    }
                    _ => Value::null(),
                },
                _ => Value::null(),
            };

            result.push(agg_result);
        }

        Ok(result)
    }
}

fn serialize_key(key: &[Value]) -> Vec<u8> {
    let serialized = serde_json::to_string(key).unwrap_or_default();
    serialized.into_bytes()
}

#[derive(Debug)]
pub struct SortAggregateExec {
    input: Rc<dyn ExecutionPlan>,
    schema: Schema,
    group_by: Vec<Expr>,
    aggregates: Vec<(Expr, Option<String>)>,
    having: Option<Expr>,
}

impl SortAggregateExec {
    pub fn new(
        input: Rc<dyn ExecutionPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<(Expr, Option<String>)>,
        having: Option<Expr>,
    ) -> Result<Self> {
        use yachtsql_core::error::Error;

        for (agg_expr, _) in &aggregates {
            if let Expr::Aggregate { filter, .. } = agg_expr {
                if let Some(filter_expr) = filter {
                    if AggregateExec::contains_aggregate(filter_expr) {
                        return Err(Error::InvalidQuery(
                            "FILTER clause cannot contain aggregate functions".to_string(),
                        ));
                    }
                }
            }
        }

        let mut fields = Vec::new();

        let input_schema = input.schema();
        for (idx, group_expr) in group_by.iter().enumerate() {
            let field_name = if let Expr::Column { name, .. } = group_expr {
                name.clone()
            } else {
                format!("group_{}", idx)
            };

            let data_type = AggregateExec::infer_expr_type(group_expr, input_schema)
                .unwrap_or(yachtsql_core::types::DataType::String);

            fields.push(Field::nullable(field_name, data_type));
        }

        for (idx, (agg_expr, alias)) in aggregates.iter().enumerate() {
            let field_name = alias.clone().unwrap_or_else(|| {
                AggregateExec::expr_to_field_name(agg_expr)
                    .unwrap_or_else(|| format!("agg_{}", idx))
            });

            let data_type = AggregateExec::infer_aggregate_type(agg_expr, input_schema)
                .unwrap_or(yachtsql_core::types::DataType::Float64);
            fields.push(Field::nullable(field_name, data_type));
        }

        let schema = Schema::from_fields(fields);

        Ok(Self {
            input,
            schema,
            group_by,
            aggregates,
            having,
        })
    }

    fn compute_group_key(&self, batch: &RecordBatch, row_idx: usize) -> Result<Vec<Value>> {
        let mut key = Vec::with_capacity(self.group_by.len());
        for expr in &self.group_by {
            let value = self.evaluate_expr(expr, batch, row_idx)?;
            key.push(value);
        }
        Ok(key)
    }

    fn evaluate_expr(&self, expr: &Expr, batch: &RecordBatch, row_idx: usize) -> Result<Value> {
        use super::ProjectionWithExprExec;
        ProjectionWithExprExec::evaluate_expr(expr, batch, row_idx)
    }

    fn evaluate_aggregate_arg(
        &self,
        agg_expr: &Expr,
        batch: &RecordBatch,
        row_idx: usize,
    ) -> Result<Value> {
        match agg_expr {
            Expr::Aggregate { name, args, filter, .. } => {
                if let Some(filter_expr) = filter {
                    let filter_result = self.evaluate_expr(filter_expr, batch, row_idx)?;
                    if filter_result.as_bool() != Some(true) {
                        return Ok(Value::null());
                    }
                }
                if args.is_empty() {
                    Ok(Value::int64(1))
                } else if args.len() >= 2
                    && matches!(
                        name.as_str(),
                        "CORR" | "COVAR_POP" | "COVAR_SAMP" | "REGR_SLOPE" | "REGR_INTERCEPT"
                    )
                {
                    let mut values = Vec::with_capacity(args.len());
                    for arg in args {
                        values.push(self.evaluate_expr(arg, batch, row_idx)?);
                    }
                    Ok(Value::array(values))
                } else {
                    self.evaluate_expr(&args[0], batch, row_idx)
                }
            }
            _ => self.evaluate_expr(agg_expr, batch, row_idx),
        }
    }

    fn keys_equal(&self, a: &[Value], b: &[Value]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        for (va, vb) in a.iter().zip(b.iter()) {
            if !values_equal(va, vb) {
                return false;
            }
        }
        true
    }

    fn evaluate_having(&self, _group_values: &[Value], _agg_values: &[Value]) -> Result<bool> {
        match &self.having {
            None => Ok(true),
            Some(_having_expr) => Ok(true),
        }
    }

    fn compute_aggregates_streaming(&self, agg_input_rows: &[Vec<Value>]) -> Result<Vec<Value>> {
        let mut result = Vec::with_capacity(self.aggregates.len());

        for agg_idx in 0..self.aggregates.len() {
            let values: Vec<&Value> = agg_input_rows.iter().map(|row| &row[agg_idx]).collect();

            let agg_result = match &self.aggregates[agg_idx].0 {
                Expr::Aggregate {
                    name,
                    args,
                    distinct,
                    ..
                } => match name.as_str() {
                    "COUNT" => {
                        if *distinct {
                            let mut unique_values = std::collections::HashSet::new();
                            for val in &values {
                                if !val.is_null() {
                                    unique_values.insert(format!("{:?}", *val));
                                }
                            }
                            Value::int64(unique_values.len() as i64)
                        } else {
                            let count = values.iter().filter(|v| !v.is_null()).count();
                            Value::int64(count as i64)
                        }
                    }
                    "COUNTIF" => {
                        let count = values.iter().filter(|v| v.as_bool() == Some(true)).count();
                        Value::int64(count as i64)
                    }
                    "SUM" => {
                        let has_numeric = values.iter().any(|v| v.as_numeric().is_some());
                        if has_numeric {
                            let mut sum = rust_decimal::Decimal::ZERO;
                            let mut has_values = false;
                            for val in &values {
                                if let Some(n) = val.as_numeric() {
                                    sum += n;
                                    has_values = true;
                                } else if let Some(i) = val.as_i64() {
                                    sum += rust_decimal::Decimal::from(i);
                                    has_values = true;
                                } else if let Some(f) = val.as_f64() {
                                    if let Some(d) = rust_decimal::Decimal::from_f64_retain(f) {
                                        sum += d;
                                        has_values = true;
                                    }
                                }
                            }
                            if has_values {
                                Value::numeric(sum)
                            } else {
                                Value::null()
                            }
                        } else {
                            let mut sum = 0.0f64;
                            let mut has_values = false;
                            for val in &values {
                                if let Some(i) = val.as_i64() {
                                    sum += i as f64;
                                    has_values = true;
                                } else if let Some(f) = val.as_f64() {
                                    sum += f;
                                    has_values = true;
                                }
                            }
                            if has_values {
                                Value::float64(sum)
                            } else {
                                Value::null()
                            }
                        }
                    }
                    "AVG" => {
                        let has_numeric = values.iter().any(|v| v.as_numeric().is_some());
                        if has_numeric {
                            let mut sum = rust_decimal::Decimal::ZERO;
                            let mut count = 0u32;
                            for val in &values {
                                if let Some(n) = val.as_numeric() {
                                    sum += n;
                                    count += 1;
                                } else if let Some(i) = val.as_i64() {
                                    sum += rust_decimal::Decimal::from(i);
                                    count += 1;
                                } else if let Some(f) = val.as_f64() {
                                    if let Some(d) = rust_decimal::Decimal::from_f64_retain(f) {
                                        sum += d;
                                        count += 1;
                                    }
                                }
                            }
                            if count > 0 {
                                Value::numeric(sum / rust_decimal::Decimal::from(count))
                            } else {
                                Value::null()
                            }
                        } else {
                            let mut sum = 0.0f64;
                            let mut count = 0usize;
                            for val in &values {
                                if let Some(i) = val.as_i64() {
                                    sum += i as f64;
                                    count += 1;
                                } else if let Some(f) = val.as_f64() {
                                    sum += f;
                                    count += 1;
                                }
                            }
                            if count > 0 {
                                Value::float64(sum / count as f64)
                            } else {
                                Value::null()
                            }
                        }
                    }
                    "MIN" => {
                        let mut min: Option<Value> = None;
                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            min = Some(match min {
                                None => (*val).clone(),
                                Some(ref current_min) => {
                                    if compare_values(val, current_min)? == std::cmp::Ordering::Less
                                    {
                                        (*val).clone()
                                    } else {
                                        current_min.clone()
                                    }
                                }
                            });
                        }
                        min.unwrap_or(Value::null())
                    }
                    "MAX" => {
                        let mut max: Option<Value> = None;
                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            max = Some(match max {
                                None => (*val).clone(),
                                Some(ref current_max) => {
                                    if compare_values(val, current_max)?
                                        == std::cmp::Ordering::Greater
                                    {
                                        (*val).clone()
                                    } else {
                                        current_max.clone()
                                    }
                                }
                            });
                        }
                        max.unwrap_or(Value::null())
                    }
                    "ARRAY_AGG" => {
                        let array_values: Vec<Value> =
                            values.iter().map(|v| (*v).clone()).collect();
                        if *distinct {
                            let mut unique_strs = std::collections::HashSet::new();
                            let mut unique_values = Vec::new();
                            for val in array_values {
                                let str_repr = format!("{:?}", val);
                                if unique_strs.insert(str_repr) {
                                    unique_values.push(val);
                                }
                            }
                            Value::array(unique_values)
                        } else {
                            Value::array(array_values)
                        }
                    }
                    "STRING_AGG" => {
                        let delimiter = if args.len() >= 2 {
                            match &args[1] {
                                Expr::Literal(yachtsql_ir::expr::LiteralValue::String(s)) => {
                                    s.clone()
                                }
                                _ => String::new(),
                            }
                        } else {
                            String::new()
                        };
                        let string_values: Vec<String> = values
                            .iter()
                            .filter_map(|v| {
                                if v.is_null() {
                                    None
                                } else {
                                    v.as_str().map(|s| s.to_string())
                                }
                            })
                            .collect();
                        if string_values.is_empty() {
                            Value::null()
                        } else {
                            Value::string(string_values.join(&delimiter))
                        }
                    }
                    "CORR" => {
                        let mut count = 0usize;
                        let mut mean_x = 0.0f64;
                        let mut mean_y = 0.0f64;
                        let mut m2_x = 0.0f64;
                        let mut m2_y = 0.0f64;
                        let mut coproduct = 0.0f64;

                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    if let (Some(x), Some(y)) = (arr[0].as_f64(), arr[1].as_f64()) {
                                        count += 1;
                                        let n = count as f64;
                                        let delta_x = x - mean_x;
                                        let delta_y = y - mean_y;
                                        mean_x += delta_x / n;
                                        mean_y += delta_y / n;
                                        let delta_x2 = x - mean_x;
                                        let delta_y2 = y - mean_y;
                                        m2_x += delta_x * delta_x2;
                                        m2_y += delta_y * delta_y2;
                                        coproduct += delta_x * delta_y2;
                                    }
                                }
                            }
                        }

                        if count < 2 {
                            Value::null()
                        } else {
                            let var_x = m2_x / count as f64;
                            let var_y = m2_y / count as f64;
                            if var_x == 0.0 || var_y == 0.0 {
                                Value::null()
                            } else {
                                let corr = coproduct / (count as f64 * var_x.sqrt() * var_y.sqrt());
                                Value::float64(corr)
                            }
                        }
                    }
                    "COVAR_POP" => {
                        let mut count = 0usize;
                        let mut mean_x = 0.0f64;
                        let mut mean_y = 0.0f64;
                        let mut coproduct = 0.0f64;

                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    if let (Some(x), Some(y)) = (arr[0].as_f64(), arr[1].as_f64()) {
                                        count += 1;
                                        let n = count as f64;
                                        let delta_x = x - mean_x;
                                        mean_x += delta_x / n;
                                        let delta_y = y - mean_y;
                                        mean_y += delta_y / n;
                                        let delta_y2 = y - mean_y;
                                        coproduct += delta_x * delta_y2;
                                    }
                                }
                            }
                        }

                        if count == 0 {
                            Value::null()
                        } else {
                            Value::float64(coproduct / count as f64)
                        }
                    }
                    "COVAR_SAMP" => {
                        let mut count = 0usize;
                        let mut mean_x = 0.0f64;
                        let mut mean_y = 0.0f64;
                        let mut coproduct = 0.0f64;

                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    if let (Some(x), Some(y)) = (arr[0].as_f64(), arr[1].as_f64()) {
                                        count += 1;
                                        let n = count as f64;
                                        let delta_x = x - mean_x;
                                        mean_x += delta_x / n;
                                        let delta_y = y - mean_y;
                                        mean_y += delta_y / n;
                                        let delta_y2 = y - mean_y;
                                        coproduct += delta_x * delta_y2;
                                    }
                                }
                            }
                        }

                        if count < 2 {
                            Value::null()
                        } else {
                            Value::float64(coproduct / (count - 1) as f64)
                        }
                    }
                    _ => Value::null(),
                },
                _ => Value::null(),
            };

            result.push(agg_result);
        }

        Ok(result)
    }
}

impl ExecutionPlan for SortAggregateExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let input_batches = self.input.execute()?;

        if input_batches.is_empty() {
            return Ok(vec![RecordBatch::empty(self.schema.clone())]);
        }

        let mut result_rows: Vec<Vec<Value>> = Vec::new();
        let mut current_group_key: Option<Vec<Value>> = None;
        let mut current_group_agg_inputs: Vec<Vec<Value>> = Vec::new();

        for input_batch in &input_batches {
            let num_rows = input_batch.num_rows();

            for row_idx in 0..num_rows {
                let group_key = self.compute_group_key(input_batch, row_idx)?;

                let same_group = current_group_key
                    .as_ref()
                    .map(|k| self.keys_equal(k, &group_key))
                    .unwrap_or(false);

                if !same_group {
                    if let Some(ref prev_key) = current_group_key {
                        let agg_values =
                            self.compute_aggregates_streaming(&current_group_agg_inputs)?;
                        if self.evaluate_having(prev_key, &agg_values)? {
                            let mut row = prev_key.clone();
                            row.extend(agg_values);
                            result_rows.push(row);
                        }
                    }
                    current_group_key = Some(group_key.clone());
                    current_group_agg_inputs.clear();
                }

                let mut agg_input_values = Vec::new();
                for (agg_expr, _) in &self.aggregates {
                    let value = self.evaluate_aggregate_arg(agg_expr, input_batch, row_idx)?;
                    agg_input_values.push(value);
                }
                current_group_agg_inputs.push(agg_input_values);
            }
        }

        if let Some(ref prev_key) = current_group_key {
            let agg_values = self.compute_aggregates_streaming(&current_group_agg_inputs)?;
            if self.evaluate_having(prev_key, &agg_values)? {
                let mut row = prev_key.clone();
                row.extend(agg_values);
                result_rows.push(row);
            }
        }

        if result_rows.is_empty() && self.group_by.is_empty() {
            let empty_agg_values: Vec<Value> = self
                .aggregates
                .iter()
                .map(|(agg_expr, _)| match agg_expr {
                    Expr::Aggregate { name, .. } => {
                        use yachtsql_ir::FunctionName;
                        match name {
                            FunctionName::Count => Value::int64(0),
                            _ => Value::null(),
                        }
                    }
                    _ => Value::null(),
                })
                .collect();

            let mut columns = Vec::new();
            for (idx, field) in self.schema.fields().iter().enumerate() {
                let mut column = Column::new(&field.data_type, 1);
                column.push(empty_agg_values.get(idx).cloned().unwrap_or(Value::null()))?;
                columns.push(column);
            }

            return Ok(vec![RecordBatch::new(self.schema.clone(), columns)?]);
        }

        if result_rows.is_empty() {
            return Ok(vec![RecordBatch::empty(self.schema.clone())]);
        }

        let num_output_rows = result_rows.len();
        let num_cols = self.schema.fields().len();
        let mut columns = Vec::new();

        for col_idx in 0..num_cols {
            let field = &self.schema.fields()[col_idx];
            let mut column = Column::new(&field.data_type, num_output_rows);

            for row in &result_rows {
                column.push(row[col_idx].clone())?;
            }

            columns.push(column);
        }

        Ok(vec![RecordBatch::new(self.schema.clone(), columns)?])
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn describe(&self) -> String {
        format!(
            "SortAggregate [group_by: {}, aggregates: {}]",
            self.group_by.len(),
            self.aggregates.len()
        )
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    if a.is_null() && b.is_null() {
        return true;
    }
    if a.is_null() || b.is_null() {
        return false;
    }
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return x == y;
    }
    if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
        return (x - y).abs() < f64::EPSILON;
    }
    if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
        return x == y;
    }
    if let (Some(x), Some(y)) = (a.as_bool(), b.as_bool()) {
        return x == y;
    }
    false
}

fn compare_values(a: &Value, b: &Value) -> Result<std::cmp::Ordering> {
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return Ok(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
        return Ok(x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal));
    }
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_f64()) {
        return Ok((x as f64)
            .partial_cmp(&y)
            .unwrap_or(std::cmp::Ordering::Equal));
    }
    if let (Some(x), Some(y)) = (a.as_f64(), b.as_i64()) {
        return Ok(x
            .partial_cmp(&(y as f64))
            .unwrap_or(std::cmp::Ordering::Equal));
    }
    if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
        return Ok(x.cmp(y));
    }
    Ok(std::cmp::Ordering::Equal)
}

#[cfg(test)]
mod tests {
    use yachtsql_ir::FunctionName;

    use super::*;
    use crate::query_executor::evaluator::physical_plan::TableScanExec;

    #[test]
    fn test_serialize_key() {
        let key = vec![Value::int64(1), Value::string("test".to_string())];
        let bytes = serialize_key(&key);
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_serialize_key_consistency() {
        let key1 = vec![Value::int64(42), Value::string("hello".to_string())];
        let key2 = vec![Value::int64(42), Value::string("hello".to_string())];
        assert_eq!(serialize_key(&key1), serialize_key(&key2));
    }

    #[test]
    fn test_aggregate_count() {
        let schema = yachtsql_storage::Schema::from_fields(vec![
            yachtsql_storage::Field::required(
                "id".to_string(),
                yachtsql_core::types::DataType::Int64,
            ),
            yachtsql_storage::Field::required(
                "value".to_string(),
                yachtsql_core::types::DataType::Int64,
            ),
        ]);

        let mut col1 = yachtsql_storage::Column::new(&yachtsql_core::types::DataType::Int64, 3);
        col1.push(Value::int64(1)).unwrap();
        col1.push(Value::int64(1)).unwrap();
        col1.push(Value::int64(2)).unwrap();

        let mut col2 = yachtsql_storage::Column::new(&yachtsql_core::types::DataType::Int64, 3);
        col2.push(Value::int64(10)).unwrap();
        col2.push(Value::int64(20)).unwrap();
        col2.push(Value::int64(30)).unwrap();

        let batch = crate::RecordBatch::new(schema.clone(), vec![col1, col2]).unwrap();
        let input_exec = Rc::new(TableScanExec::new(
            schema.clone(),
            "test".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let group_by = vec![yachtsql_optimizer::expr::Expr::Column {
            name: "id".to_string(),
            table: None,
        }];

        let aggregates = vec![(
            yachtsql_optimizer::expr::Expr::Aggregate {
                name: FunctionName::Count,
                args: vec![yachtsql_optimizer::expr::Expr::Column {
                    name: "value".to_string(),
                    table: None,
                }],
                distinct: false,
                order_by: None,
                filter: None,
            },
            Some("count".to_string()),
        )];

        let agg_exec = AggregateExec::new(input_exec, group_by, aggregates, None);
        assert!(agg_exec.is_ok());
    }

    #[test]
    fn test_compare_values() {
        assert_eq!(
            compare_values(&Value::int64(5), &Value::int64(10)).unwrap(),
            std::cmp::Ordering::Less
        );

        assert_eq!(
            compare_values(
                &Value::string("a".to_string()),
                &Value::string("b".to_string())
            )
            .unwrap(),
            std::cmp::Ordering::Less
        );

        assert_eq!(
            compare_values(&Value::int64(5), &Value::float64(5.0)).unwrap(),
            std::cmp::Ordering::Equal
        );
    }
}
