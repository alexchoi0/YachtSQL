use std::rc::Rc;

use debug_print::debug_eprintln;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};
use yachtsql_ir::expr::LiteralValue;
use yachtsql_optimizer::expr::{Expr, OrderByExpr};
use yachtsql_storage::{Column, Schema};

use super::ExecutionPlan;
use crate::Table;

#[derive(Debug)]
pub struct SortExec {
    input: Rc<dyn ExecutionPlan>,
    schema: Schema,

    sort_exprs: Vec<OrderByExpr>,

    enum_labels: Vec<Option<Vec<String>>>,
}

impl SortExec {
    pub fn new(input: Rc<dyn ExecutionPlan>, sort_exprs: Vec<OrderByExpr>) -> Result<Self> {
        let schema = input.schema().clone();

        let sort_exprs = sort_exprs
            .into_iter()
            .map(|mut expr| {
                expr.expr = Self::resolve_positional_reference(expr.expr, &schema);
                expr
            })
            .collect::<Vec<_>>();

        for sort_expr in &sort_exprs {
            Self::validate_expr_columns(&sort_expr.expr, &schema)?;
        }

        let mut enum_labels = Vec::with_capacity(sort_exprs.len());
        for sort_expr in &sort_exprs {
            let labels = Self::get_enum_labels_for_expr(&sort_expr.expr, &schema);
            debug_eprintln!(
                "[executor::sort] sort_expr={:?} labels={:?}",
                sort_expr.expr,
                labels
            );
            enum_labels.push(labels);
        }

        debug_eprintln!("[executor::sort] schema fields:");
        for field in schema.fields() {
            debug_eprintln!(
                "[executor::sort]   - {} : {:?} (source_table={:?})",
                field.name,
                field.data_type,
                field.source_table
            );
        }

        Ok(Self {
            input,
            schema,
            sort_exprs,
            enum_labels,
        })
    }

    fn resolve_positional_reference(expr: Expr, schema: &Schema) -> Expr {
        match &expr {
            Expr::Literal(LiteralValue::Int64(pos)) => {
                let pos = *pos as usize;
                if pos > 0 && pos <= schema.fields().len() {
                    let field = &schema.fields()[pos - 1];
                    return Expr::Column {
                        name: field.name.clone(),
                        table: None,
                    };
                }
                expr
            }
            _ => expr,
        }
    }

    fn get_enum_labels_for_expr(expr: &Expr, schema: &Schema) -> Option<Vec<String>> {
        match expr {
            Expr::Column { name, .. } => {
                for field in schema.fields() {
                    if field.name == *name {
                        if let DataType::Enum { labels, .. } = &field.data_type {
                            return Some(labels.clone());
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }

    fn validate_expr_columns(expr: &Expr, schema: &Schema) -> Result<()> {
        match expr {
            Expr::Column { name, table } => {
                let found = if let Some(t) = table {
                    schema.field_index_qualified(name, Some(t)).is_some()
                        || schema.field_index(name).is_some()
                        || schema.field_index(&format!("{}.{}", t, name)).is_some()
                } else {
                    schema.field_index(name).is_some()
                };
                if found {
                    Ok(())
                } else {
                    Err(Error::column_not_found(name.clone()))
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::validate_expr_columns(left, schema)?;
                Self::validate_expr_columns(right, schema)
            }
            Expr::UnaryOp { expr, .. } => Self::validate_expr_columns(expr, schema),
            Expr::Function { args, .. } => {
                for arg in args {
                    Self::validate_expr_columns(arg, schema)?;
                }
                Ok(())
            }
            Expr::Aggregate { args, filter, .. } => {
                for arg in args {
                    Self::validate_expr_columns(arg, schema)?;
                }
                if let Some(f) = filter {
                    Self::validate_expr_columns(f, schema)?;
                }
                Ok(())
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(op) = operand {
                    Self::validate_expr_columns(op, schema)?;
                }
                for (when_expr, then_expr) in when_then {
                    Self::validate_expr_columns(when_expr, schema)?;
                    Self::validate_expr_columns(then_expr, schema)?;
                }
                if let Some(el) = else_expr {
                    Self::validate_expr_columns(el, schema)?;
                }
                Ok(())
            }
            Expr::Cast { expr, .. } => Self::validate_expr_columns(expr, schema),
            Expr::TryCast { expr, .. } => Self::validate_expr_columns(expr, schema),
            Expr::InList { expr, list, .. } => {
                Self::validate_expr_columns(expr, schema)?;
                for item in list {
                    Self::validate_expr_columns(item, schema)?;
                }
                Ok(())
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::validate_expr_columns(expr, schema)?;
                Self::validate_expr_columns(low, schema)?;
                Self::validate_expr_columns(high, schema)
            }
            Expr::Subquery { .. } => Ok(()),
            Expr::Literal(_) => Ok(()),
            Expr::Wildcard => Ok(()),
            Expr::QualifiedWildcard { .. } => Ok(()),
            Expr::ExpressionWildcard { expr } => Self::validate_expr_columns(expr, schema),
            Expr::Tuple(exprs) => {
                for e in exprs {
                    Self::validate_expr_columns(e, schema)?;
                }
                Ok(())
            }
            Expr::StructLiteral { fields } => {
                for field in fields {
                    Self::validate_expr_columns(&field.expr, schema)?;
                }
                Ok(())
            }
            Expr::StructFieldAccess { expr, .. } => Self::validate_expr_columns(expr, schema),
            Expr::TupleElementAccess { expr, .. } => Self::validate_expr_columns(expr, schema),
            Expr::WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for arg in args {
                    Self::validate_expr_columns(arg, schema)?;
                }
                for e in partition_by {
                    Self::validate_expr_columns(e, schema)?;
                }
                for ob in order_by {
                    Self::validate_expr_columns(&ob.expr, schema)?;
                }
                Ok(())
            }
            Expr::Exists { .. } => Ok(()),
            Expr::InSubquery { expr, .. } => Self::validate_expr_columns(expr, schema),
            Expr::InTable { expr, .. } => Self::validate_expr_columns(expr, schema),
            Expr::TupleInList { tuple, list, .. } => {
                for e in tuple {
                    Self::validate_expr_columns(e, schema)?;
                }
                for tup in list {
                    for e in tup {
                        Self::validate_expr_columns(e, schema)?;
                    }
                }
                Ok(())
            }
            Expr::TupleInSubquery { tuple, .. } => {
                for e in tuple {
                    Self::validate_expr_columns(e, schema)?;
                }
                Ok(())
            }
            Expr::ArrayIndex { array, index, .. } => {
                Self::validate_expr_columns(array, schema)?;
                Self::validate_expr_columns(index, schema)
            }
            Expr::ArraySlice { array, start, end } => {
                Self::validate_expr_columns(array, schema)?;
                if let Some(s) = start {
                    Self::validate_expr_columns(s, schema)?;
                }
                if let Some(e) = end {
                    Self::validate_expr_columns(e, schema)?;
                }
                Ok(())
            }
            Expr::AnyOp { left, right, .. } => {
                Self::validate_expr_columns(left, schema)?;
                Self::validate_expr_columns(right, schema)
            }
            Expr::AllOp { left, right, .. } => {
                Self::validate_expr_columns(left, schema)?;
                Self::validate_expr_columns(right, schema)
            }
            Expr::ScalarSubquery { .. } => Ok(()),
            Expr::ArraySubquery { .. } => Ok(()),
            Expr::Grouping { .. } => Ok(()),
            Expr::GroupingId { .. } => Ok(()),
            Expr::Excluded { .. } => Ok(()),
            Expr::IsDistinctFrom { left, right, .. } => {
                Self::validate_expr_columns(left, schema)?;
                Self::validate_expr_columns(right, schema)
            }
            Expr::Lambda { body, .. } => Self::validate_expr_columns(body, schema),
        }
    }

    fn evaluate_sort_key(&self, batch: &Table, row_idx: usize) -> Result<Vec<Value>> {
        let mut key = Vec::with_capacity(self.sort_exprs.len());
        for sort_expr in &self.sort_exprs {
            let value = self.evaluate_expr(&sort_expr.expr, batch, row_idx)?;
            key.push(value);
        }
        Ok(key)
    }

    fn evaluate_expr(&self, expr: &Expr, batch: &Table, row_idx: usize) -> Result<Value> {
        use super::ProjectionWithExprExec;
        ProjectionWithExprExec::evaluate_expr(expr, batch, row_idx)
    }

    fn get_fill_column_index(&self, sort_idx: usize) -> Option<usize> {
        let sort_expr = &self.sort_exprs[sort_idx];
        if let Expr::Column { name, .. } = &sort_expr.expr {
            self.schema.field_index(name)
        } else {
            None
        }
    }

    fn evaluate_fill_expr(&self, expr: &Expr) -> Result<Value> {
        match expr {
            Expr::Literal(lit) => Ok(lit.to_value()),
            _ => Ok(Value::null()),
        }
    }

    fn apply_with_fill(
        &self,
        sorted_rows: Vec<(Vec<Value>, Vec<Value>)>,
    ) -> Result<Vec<Vec<Value>>> {
        if sorted_rows.is_empty() {
            return Ok(vec![]);
        }

        let first_fill_idx = self
            .sort_exprs
            .iter()
            .position(|e| e.with_fill.is_some())
            .unwrap_or(0);

        let sort_expr = &self.sort_exprs[first_fill_idx];
        let fill_opts = sort_expr.with_fill.as_ref().unwrap();
        let col_idx = match self.get_fill_column_index(first_fill_idx) {
            Some(idx) => idx,
            None => return Ok(sorted_rows.into_iter().map(|(_, row)| row).collect()),
        };

        let is_asc = sort_expr.asc.unwrap_or(true);
        let step = fill_opts
            .step
            .as_ref()
            .map(|e| self.evaluate_fill_expr(e))
            .transpose()?
            .unwrap_or_else(|| Value::int64(if is_asc { 1 } else { -1 }));

        let from_val = fill_opts
            .from
            .as_ref()
            .map(|e| self.evaluate_fill_expr(e))
            .transpose()?;
        let to_val = fill_opts
            .to
            .as_ref()
            .map(|e| self.evaluate_fill_expr(e))
            .transpose()?;

        let mut result: Vec<Vec<Value>> = Vec::new();
        let num_cols = self.schema.fields().len();

        let first_row = &sorted_rows[0].1;
        if let Some(ref from) = from_val {
            let first_key = &first_row[col_idx];
            let mut current = from.clone();
            while self.should_fill_before(&current, first_key, &step, is_asc)? {
                let mut fill_row = vec![Value::null(); num_cols];
                fill_row[col_idx] = current.clone();
                result.push(fill_row);
                current = self.add_step(&current, &step)?;
            }
        }

        for (i, (_, row)) in sorted_rows.iter().enumerate() {
            result.push(row.clone());

            if i + 1 < sorted_rows.len() {
                let current_key = &row[col_idx];
                let next_key = &sorted_rows[i + 1].1[col_idx];

                if !current_key.is_null() && !next_key.is_null() {
                    let mut fill_val = self.add_step(current_key, &step)?;
                    while self.should_fill_between(&fill_val, next_key, &step, is_asc)? {
                        let mut fill_row = vec![Value::null(); num_cols];
                        fill_row[col_idx] = fill_val.clone();
                        result.push(fill_row);
                        fill_val = self.add_step(&fill_val, &step)?;
                    }
                }
            }
        }

        if let Some(ref to) = to_val {
            let last_row = &sorted_rows.last().unwrap().1;
            let last_key = &last_row[col_idx];
            if !last_key.is_null() {
                let mut current = self.add_step(last_key, &step)?;
                while self.should_fill_to(&current, to, &step, is_asc)? {
                    let mut fill_row = vec![Value::null(); num_cols];
                    fill_row[col_idx] = current.clone();
                    result.push(fill_row);
                    current = self.add_step(&current, &step)?;
                }
            }
        }

        Ok(result)
    }

    fn should_fill_before(
        &self,
        current: &Value,
        target: &Value,
        _step: &Value,
        is_asc: bool,
    ) -> Result<bool> {
        if current.is_null() || target.is_null() {
            return Ok(false);
        }
        let cmp = compare_values(current, target)?;
        Ok(if is_asc {
            cmp == std::cmp::Ordering::Less
        } else {
            cmp == std::cmp::Ordering::Greater
        })
    }

    fn should_fill_between(
        &self,
        current: &Value,
        target: &Value,
        _step: &Value,
        is_asc: bool,
    ) -> Result<bool> {
        if current.is_null() || target.is_null() {
            return Ok(false);
        }
        let cmp = compare_values(current, target)?;
        Ok(if is_asc {
            cmp == std::cmp::Ordering::Less
        } else {
            cmp == std::cmp::Ordering::Greater
        })
    }

    fn should_fill_to(
        &self,
        current: &Value,
        to: &Value,
        _step: &Value,
        is_asc: bool,
    ) -> Result<bool> {
        if current.is_null() || to.is_null() {
            return Ok(false);
        }
        let cmp = compare_values(current, to)?;
        Ok(if is_asc {
            cmp != std::cmp::Ordering::Greater
        } else {
            cmp != std::cmp::Ordering::Less
        })
    }

    fn add_step(&self, value: &Value, step: &Value) -> Result<Value> {
        use chrono::Duration;
        use yachtsql_core::types::Date32Value;

        if value.is_null() {
            return Ok(Value::null());
        }

        if let (Some(v), Some(s)) = (value.as_i64(), step.as_i64()) {
            return Ok(Value::int64(v + s));
        }
        if let (Some(v), Some(s)) = (value.as_f64(), step.as_f64()) {
            return Ok(Value::float64(v + s));
        }
        if let Some(d) = value.as_date() {
            let step_days = step.as_i64().unwrap_or(1);
            return Ok(Value::date(d + Duration::days(step_days)));
        }
        if let Some(ts) = value.as_timestamp() {
            if let Some(interval) = step.as_interval() {
                let days = interval.days as i64;
                let months = interval.months as i64;
                let micros = interval.micros;
                let step_duration = Duration::days(days)
                    + Duration::days(months * 30)
                    + Duration::microseconds(micros);
                return Ok(Value::timestamp(ts + step_duration));
            }
            let step_val = step.as_i64().unwrap_or(1);
            return Ok(Value::timestamp(ts + Duration::seconds(step_val)));
        }
        if let Some(dt) = value.as_datetime() {
            if let Some(interval) = step.as_interval() {
                let days = interval.days as i64;
                let months = interval.months as i64;
                let micros = interval.micros;
                let step_duration = Duration::days(days)
                    + Duration::days(months * 30)
                    + Duration::microseconds(micros);
                return Ok(Value::datetime(dt + step_duration));
            }
            let step_val = step.as_i64().unwrap_or(1);
            return Ok(Value::datetime(dt + Duration::seconds(step_val)));
        }
        if let Some(d32) = value.as_date32() {
            let step_days = step.as_i64().unwrap_or(1) as i32;
            return Ok(Value::date32(Date32Value(d32.0 + step_days)));
        }

        Ok(value.clone())
    }

    fn compare_sort_keys(&self, a: &[Value], b: &[Value]) -> Result<std::cmp::Ordering> {
        for (idx, sort_expr) in self.sort_exprs.iter().enumerate() {
            let val_a = &a[idx];
            let val_b = &b[idx];

            let null_cmp = if val_a.is_null() && val_b.is_null() {
                std::cmp::Ordering::Equal
            } else if val_a.is_null() {
                if sort_expr.nulls_first.unwrap_or(false) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            } else if val_b.is_null() {
                if sort_expr.nulls_first.unwrap_or(false) {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            } else {
                std::cmp::Ordering::Equal
            };

            if null_cmp != std::cmp::Ordering::Equal {
                return Ok(null_cmp);
            }

            let cmp = if let Some(labels) = &self.enum_labels[idx] {
                compare_enum_values(val_a, val_b, labels)?
            } else {
                compare_values(val_a, val_b)?
            };

            if cmp != std::cmp::Ordering::Equal {
                let is_asc = sort_expr.asc.unwrap_or(true);
                return Ok(if is_asc { cmp } else { cmp.reverse() });
            }
        }

        Ok(std::cmp::Ordering::Equal)
    }
}

impl ExecutionPlan for SortExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn statistics(&self) -> super::ExecutionStatistics {
        let sort_columns: Vec<String> = self
            .sort_exprs
            .iter()
            .filter_map(|sort_expr| match &sort_expr.expr {
                Expr::Column { name, .. } => Some(name.clone()),
                _ => None,
            })
            .collect();

        super::ExecutionStatistics {
            num_rows: self.input.statistics().num_rows,
            memory_usage: None,
            is_sorted: true,
            sort_columns: if sort_columns.is_empty() {
                None
            } else {
                Some(sort_columns)
            },
        }
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let input_batches = self.input.execute()?;

        if input_batches.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let mut all_rows: Vec<(Vec<Value>, Vec<Value>)> = Vec::new();

        for input_batch in input_batches {
            let num_rows = input_batch.num_rows();

            debug_eprintln!(
                "[executor::sort] input_batch schema: {:?}",
                input_batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| format!("{} ({:?})", f.name, f.source_table))
                    .collect::<Vec<_>>()
            );

            for row_idx in 0..num_rows {
                let sort_key = self.evaluate_sort_key(&input_batch, row_idx)?;
                debug_eprintln!("[executor::sort] row {} sort_key={:?}", row_idx, sort_key);

                let mut row_data = Vec::new();
                for col in input_batch.expect_columns() {
                    row_data.push(col.get(row_idx)?);
                }

                all_rows.push((sort_key, row_data));
            }
        }

        all_rows.sort_by(|a, b| {
            self.compare_sort_keys(&a.0, &b.0)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        if all_rows.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let has_with_fill = self.sort_exprs.iter().any(|e| e.with_fill.is_some());
        let final_rows = if has_with_fill {
            self.apply_with_fill(all_rows)?
        } else {
            all_rows.into_iter().map(|(_, row)| row).collect()
        };

        let num_output_rows = final_rows.len();
        let num_cols = self.schema.fields().len();
        let mut columns = Vec::new();

        for col_idx in 0..num_cols {
            let field = &self.schema.fields()[col_idx];
            let mut column = Column::new(&field.data_type, num_output_rows);

            for row_data in &final_rows {
                column.push(row_data[col_idx].clone())?;
            }

            columns.push(column);
        }

        Ok(vec![Table::new(self.schema.clone(), columns)?])
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn describe(&self) -> String {
        format!("Sort [columns: {}]", self.sort_exprs.len())
    }
}

fn compare_enum_values(a: &Value, b: &Value, labels: &[String]) -> Result<std::cmp::Ordering> {
    if a.is_null() && b.is_null() {
        return Ok(std::cmp::Ordering::Equal);
    }

    let pos_a = a.as_str().and_then(|s| labels.iter().position(|l| l == s));
    let pos_b = b.as_str().and_then(|s| labels.iter().position(|l| l == s));

    match (pos_a, pos_b) {
        (Some(a), Some(b)) => Ok(a.cmp(&b)),

        (None, Some(_)) => Ok(std::cmp::Ordering::Greater),
        (Some(_), None) => Ok(std::cmp::Ordering::Less),
        (None, None) => Ok(std::cmp::Ordering::Equal),
    }
}

fn compare_values(a: &Value, b: &Value) -> Result<std::cmp::Ordering> {
    if a.is_null() && b.is_null() {
        return Ok(std::cmp::Ordering::Equal);
    }

    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return Ok(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
        return Ok(x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal));
    }

    if let Some(x) = a.as_i64() {
        if let Some(y) = b.as_f64() {
            return Ok((x as f64)
                .partial_cmp(&y)
                .unwrap_or(std::cmp::Ordering::Equal));
        }
    }
    if let Some(x) = a.as_f64() {
        if let Some(y) = b.as_i64() {
            return Ok(x
                .partial_cmp(&(y as f64))
                .unwrap_or(std::cmp::Ordering::Equal));
        }
    }
    if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
        return Ok(x.cmp(y));
    }
    if let (Some(fs_a), Some(fs_b)) = (a.as_fixed_string(), b.as_fixed_string()) {
        return Ok(fs_a.data.cmp(&fs_b.data));
    }
    if let (Some(fs), Some(s)) = (a.as_fixed_string(), b.as_str()) {
        return Ok(fs.to_string_lossy().cmp(&s.to_string()));
    }
    if let (Some(s), Some(fs)) = (a.as_str(), b.as_fixed_string()) {
        return Ok(s.to_string().cmp(&fs.to_string_lossy()));
    }
    if let (Some(x), Some(y)) = (a.as_bool(), b.as_bool()) {
        return Ok(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_date(), b.as_date()) {
        return Ok(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_timestamp(), b.as_timestamp()) {
        return Ok(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_datetime(), b.as_datetime()) {
        return Ok(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_uuid(), b.as_uuid()) {
        return Ok(x.cmp(y));
    }
    if let (Some(x), Some(y)) = (a.as_ipv4(), b.as_ipv4()) {
        return Ok(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_ipv6(), b.as_ipv6()) {
        return Ok(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_date32(), b.as_date32()) {
        return Ok(x.0.cmp(&y.0));
    }
    if let (Some(x), Some(y)) = (a.as_bytes(), b.as_bytes()) {
        return Ok(x.cmp(y));
    }
    if let (Some(x_struct), Some(y_struct)) = (a.as_struct(), b.as_struct()) {
        for (x_val, y_val) in x_struct.values().zip(y_struct.values()) {
            let cmp = compare_values(x_val, y_val)?;
            if cmp != std::cmp::Ordering::Equal {
                return Ok(cmp);
            }
        }
        return Ok(x_struct.len().cmp(&y_struct.len()));
    }
    Ok(std::cmp::Ordering::Equal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_executor::evaluator::physical_plan::TableScanExec;

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
    }

    #[test]
    fn test_compare_values_numeric() {
        assert_eq!(
            compare_values(&Value::int64(42), &Value::int64(42)).unwrap(),
            std::cmp::Ordering::Equal
        );

        assert_eq!(
            compare_values(&Value::float64(3.14), &Value::float64(2.71)).unwrap(),
            std::cmp::Ordering::Greater
        );

        assert_eq!(
            compare_values(&Value::int64(5), &Value::float64(5.0)).unwrap(),
            std::cmp::Ordering::Equal
        );
    }

    #[test]
    fn test_sort_exec_creation() {
        let schema = yachtsql_storage::Schema::from_fields(vec![
            yachtsql_storage::Field::required(
                "id".to_string(),
                yachtsql_core::types::DataType::Int64,
            ),
            yachtsql_storage::Field::required(
                "name".to_string(),
                yachtsql_core::types::DataType::String,
            ),
        ]);

        let input_exec = Rc::new(TableScanExec::new(
            schema.clone(),
            "test".to_string(),
            Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let sort_exprs = vec![OrderByExpr {
            expr: yachtsql_optimizer::expr::Expr::Column {
                name: "id".to_string(),
                table: None,
            },
            asc: Some(true),
            nulls_first: None,
            collation: None,
            with_fill: None,
        }];

        let sort_exec = SortExec::new(input_exec, sort_exprs);
        assert!(sort_exec.is_ok());
    }

    #[test]
    fn test_sort_exec_schema() {
        let schema =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "value".to_string(),
                yachtsql_core::types::DataType::Int64,
            )]);

        let input_exec = Rc::new(TableScanExec::new(
            schema.clone(),
            "test".to_string(),
            Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let sort_exprs = vec![OrderByExpr {
            expr: yachtsql_optimizer::expr::Expr::Column {
                name: "value".to_string(),
                table: None,
            },
            asc: Some(false),
            nulls_first: Some(true),
            collation: None,
            with_fill: None,
        }];

        let sort_exec = SortExec::new(input_exec, sort_exprs).unwrap();
        assert_eq!(sort_exec.schema().fields().len(), 1);
    }
}
