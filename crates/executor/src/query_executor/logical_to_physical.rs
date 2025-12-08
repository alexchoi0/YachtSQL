use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use yachtsql_core::error::{Error, Result};
use yachtsql_ir::plan::{LogicalPlan, PlanNode};

use super::evaluator::physical_plan::{
    AggregateExec, AggregateStrategy, ArrayJoinExec, CteExec, DistinctExec, DistinctOnExec,
    EmptyRelationExec, ExceptExec, ExecutionPlan, FilterExec, HashJoinExec, IndexScanExec,
    IntersectExec, JoinStrategy, LateralJoinExec, LimitExec, MaterializedViewScanExec, MergeExec,
    MergeJoinExec, NestedLoopJoinExec, PhysicalPlan, PivotAggregateFunction, PivotExec,
    ProjectionWithExprExec, SampleSize, SamplingMethod, SortAggregateExec, SortExec,
    SubqueryScanExec, TableSampleExec, TableScanExec, TableValuedFunctionExec, UnionExec,
    UnnestExec, UnpivotExec, ValuesExec, WindowExec, infer_values_schema,
};
use super::returning::{
    ReturningColumn, ReturningColumnOrigin, ReturningExpressionItem, ReturningSpec,
};
use crate::information_schema::{InformationSchemaProvider, InformationSchemaTable};
use crate::system_schema::{SystemSchemaProvider, SystemTable};

#[allow(dead_code)]
pub struct LogicalToPhysicalPlanner {
    storage: Rc<RefCell<yachtsql_storage::Storage>>,
    dialect: crate::DialectType,
    cte_plans: RefCell<HashMap<String, Rc<dyn ExecutionPlan>>>,
}

impl LogicalToPhysicalPlanner {
    fn validate_expr(expr: &yachtsql_ir::expr::Expr) -> Result<()> {
        use yachtsql_ir::expr::Expr;
        use yachtsql_ir::function::FunctionName;

        match expr {
            Expr::Function { name, args } => {
                if matches!(name, FunctionName::If | FunctionName::Iif) && args.len() != 3 {
                    return Err(Error::InvalidOperation(format!(
                        "{:?} function requires exactly 3 arguments (condition, true_value, false_value), got {}",
                        name,
                        args.len()
                    )));
                }
                for arg in args {
                    Self::validate_expr(arg)?;
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::validate_expr(left)?;
                Self::validate_expr(right)?;
            }
            Expr::UnaryOp { expr: inner, .. } => {
                Self::validate_expr(inner)?;
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(op) = operand {
                    Self::validate_expr(op)?;
                }
                for (when_expr, then_expr) in when_then {
                    Self::validate_expr(when_expr)?;
                    Self::validate_expr(then_expr)?;
                }
                if let Some(el) = else_expr {
                    Self::validate_expr(el)?;
                }
            }
            Expr::Cast { expr: inner, .. } | Expr::TryCast { expr: inner, .. } => {
                Self::validate_expr(inner)?;
            }
            Expr::Aggregate { args, filter, .. } => {
                for arg in args {
                    Self::validate_expr(arg)?;
                }
                if let Some(f) = filter {
                    Self::validate_expr(f)?;
                }
            }
            Expr::InList {
                expr: inner, list, ..
            } => {
                Self::validate_expr(inner)?;
                for item in list {
                    Self::validate_expr(item)?;
                }
            }
            Expr::Between {
                expr: inner,
                low,
                high,
                ..
            } => {
                Self::validate_expr(inner)?;
                Self::validate_expr(low)?;
                Self::validate_expr(high)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn infer_projection_schema(
        &self,
        expressions: &[(yachtsql_ir::expr::Expr, Option<String>)],
        input_schema: &yachtsql_storage::Schema,
    ) -> Result<yachtsql_storage::Schema> {
        use yachtsql_storage::{Field, Schema};

        let mut fields = Vec::with_capacity(expressions.len());

        for (idx, (expr, alias)) in expressions.iter().enumerate() {
            Self::validate_expr(expr)?;
            Self::validate_column_references(expr, input_schema)?;

            let data_type = ProjectionWithExprExec::infer_expr_type_with_schema(expr, input_schema)
                .unwrap_or(yachtsql_core::types::DataType::Unknown);

            let field_name = if let Some(alias) = alias {
                alias.clone()
            } else {
                match expr {
                    yachtsql_ir::expr::Expr::Column { name, .. } => name.clone(),
                    _ => format!("expr_{}", idx),
                }
            };

            fields.push(Field::nullable(field_name, data_type));
        }

        Ok(Schema::from_fields(fields))
    }

    fn validate_column_references(
        expr: &yachtsql_ir::expr::Expr,
        schema: &yachtsql_storage::Schema,
    ) -> Result<()> {
        use yachtsql_ir::expr::Expr;

        match expr {
            Expr::Column { name, table } => {
                if table.is_none() {
                    let matching_fields: Vec<_> = schema
                        .fields()
                        .iter()
                        .filter(|f| f.name.eq_ignore_ascii_case(name))
                        .collect();

                    if matching_fields.len() > 1 {
                        let source_tables: std::collections::HashSet<_> = matching_fields
                            .iter()
                            .filter_map(|f| f.source_table.as_ref())
                            .collect();

                        if source_tables.len() > 1 {
                            return Err(Error::InvalidQuery(format!(
                                "Column reference '{}' is ambiguous - it exists in multiple tables. Use table.column syntax to disambiguate.",
                                name
                            )));
                        }
                    }
                }

                if schema.field(name).is_some() {
                    return Ok(());
                }
                if let Some(table_name) = table {
                    if let Some(field) = schema.field(table_name) {
                        if let yachtsql_core::types::DataType::Struct(fields) = &field.data_type {
                            if fields.iter().any(|f| f.name.eq_ignore_ascii_case(name)) {
                                return Ok(());
                            }
                        }
                    }
                }
                Err(Error::ColumnNotFound(name.clone()))
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::validate_column_references(left, schema)?;
                Self::validate_column_references(right, schema)
            }
            Expr::UnaryOp { expr: inner, .. } => Self::validate_column_references(inner, schema),
            Expr::Function { args, .. } => {
                for arg in args {
                    Self::validate_column_references(arg, schema)?;
                }
                Ok(())
            }
            Expr::Aggregate { args, filter, .. } => {
                for arg in args {
                    Self::validate_column_references(arg, schema)?;
                }
                if let Some(f) = filter {
                    Self::validate_column_references(f, schema)?;
                }
                Ok(())
            }
            Expr::Cast { expr: inner, .. } | Expr::TryCast { expr: inner, .. } => {
                Self::validate_column_references(inner, schema)
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(op) = operand {
                    Self::validate_column_references(op, schema)?;
                }
                for (when_expr, then_expr) in when_then {
                    Self::validate_column_references(when_expr, schema)?;
                    Self::validate_column_references(then_expr, schema)?;
                }
                if let Some(el) = else_expr {
                    Self::validate_column_references(el, schema)?;
                }
                Ok(())
            }
            Expr::InList {
                expr: inner, list, ..
            } => {
                Self::validate_column_references(inner, schema)?;
                for item in list {
                    Self::validate_column_references(item, schema)?;
                }
                Ok(())
            }
            Expr::Between {
                expr: inner,
                low,
                high,
                ..
            } => {
                Self::validate_column_references(inner, schema)?;
                Self::validate_column_references(low, schema)?;
                Self::validate_column_references(high, schema)
            }
            Expr::StructFieldAccess { expr: inner, .. } => {
                Self::validate_column_references(inner, schema)
            }
            Expr::ArrayIndex { array, index, .. } => {
                Self::validate_column_references(array, schema)?;
                Self::validate_column_references(index, schema)
            }
            _ => Ok(()),
        }
    }

    fn collect_table_names(&self, node: &PlanNode) -> std::collections::HashSet<String> {
        let mut tables = std::collections::HashSet::new();
        self.collect_table_names_recursive(node, &mut tables);
        tables
    }

    fn collect_table_names_recursive(
        &self,
        node: &PlanNode,
        tables: &mut std::collections::HashSet<String>,
    ) {
        match node {
            PlanNode::Scan {
                table_name, alias, ..
            } => {
                if let Some(alias) = alias {
                    tables.insert(alias.clone());
                }
                tables.insert(table_name.clone());
            }
            _ => {
                for child in node.children() {
                    self.collect_table_names_recursive(child, tables);
                }
            }
        }
    }

    fn get_expr_table(&self, expr: &yachtsql_ir::expr::Expr) -> Option<String> {
        match expr {
            yachtsql_ir::expr::Expr::Column { table, .. } => table.clone(),
            yachtsql_ir::expr::Expr::BinaryOp { left, op: _, right } => {
                let left_table = self.get_expr_table(left);
                let right_table = self.get_expr_table(right);
                match (&left_table, &right_table) {
                    (Some(l), Some(r)) if l != r => panic!(
                        "get_expr_table: BinaryOp references columns from different tables: {} and {}",
                        l, r
                    ),
                    (Some(_), _) => left_table,
                    (_, Some(_)) => right_table,
                    (None, None) => None,
                }
            }
            yachtsql_ir::expr::Expr::Function { args, .. } => {
                let mut found_table: Option<String> = None;
                for arg in args {
                    let arg_table = self.get_expr_table(arg);
                    match (&found_table, &arg_table) {
                        (Some(t1), Some(t2)) if t1 != t2 => panic!(
                            "get_expr_table: Function references columns from different tables: {} and {}",
                            t1, t2
                        ),
                        (None, Some(_)) => found_table = arg_table,
                        _ => {}
                    }
                }
                found_table
            }
            yachtsql_ir::expr::Expr::Literal(_) => None,
            yachtsql_ir::expr::Expr::StructLiteral { .. } => None,
            yachtsql_ir::expr::Expr::Tuple(_) => None,
            yachtsql_ir::expr::Expr::Cast { expr, .. } => self.get_expr_table(expr),
            yachtsql_ir::expr::Expr::TryCast { expr, .. } => self.get_expr_table(expr),
            yachtsql_ir::expr::Expr::UnaryOp { expr, .. } => self.get_expr_table(expr),
            _ => panic!("get_expr_table: unhandled expression type: {:?}", expr),
        }
    }

    fn parse_join_conditions(
        &self,
        expr: &yachtsql_ir::expr::Expr,
        left_tables: &std::collections::HashSet<String>,
    ) -> Vec<(yachtsql_ir::expr::Expr, yachtsql_ir::expr::Expr)> {
        use yachtsql_ir::expr::{BinaryOp, Expr};

        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::Equal,
                right,
            } => {
                let left_table = self.get_expr_table(left);
                let right_table = self.get_expr_table(right);

                let left_is_left_side =
                    left_table.as_ref().is_some_and(|t| left_tables.contains(t));
                let right_is_left_side = right_table
                    .as_ref()
                    .is_some_and(|t| left_tables.contains(t));

                let left_resolved = self.resolve_custom_type_fields(left);
                let right_resolved = self.resolve_custom_type_fields(right);

                if left_is_left_side && !right_is_left_side {
                    vec![(left_resolved, right_resolved)]
                } else if right_is_left_side && !left_is_left_side {
                    vec![(right_resolved, left_resolved)]
                } else {
                    vec![(left_resolved, right_resolved)]
                }
            }

            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                let mut conditions = Vec::new();
                conditions.extend(self.parse_join_conditions(left, left_tables));
                conditions.extend(self.parse_join_conditions(right, left_tables));
                conditions
            }

            _ => Vec::new(),
        }
    }

    fn expand_nested_custom_types(
        &self,
        fields: &[yachtsql_core::types::StructField],
    ) -> Vec<yachtsql_core::types::StructField> {
        use yachtsql_core::types::StructField;

        fields
            .iter()
            .map(|field| {
                let expanded_type = self.expand_custom_data_type(&field.data_type);
                StructField {
                    name: field.name.clone(),
                    data_type: expanded_type,
                }
            })
            .collect()
    }

    fn expand_schema_custom_types(
        &self,
        schema: &yachtsql_storage::Schema,
    ) -> yachtsql_storage::Schema {
        use yachtsql_storage::Schema;

        let expanded_fields = schema
            .fields()
            .iter()
            .map(|field| {
                let mut new_field = field.clone();
                new_field.data_type = self.expand_custom_data_type(&field.data_type);
                new_field
            })
            .collect();

        Schema::from_fields(expanded_fields)
    }

    fn expand_custom_data_type(
        &self,
        data_type: &yachtsql_core::types::DataType,
    ) -> yachtsql_core::types::DataType {
        use yachtsql_core::types::DataType;

        match data_type {
            DataType::Custom(type_name) => {
                let composite_fields_cloned = {
                    let storage = self.storage.borrow();
                    storage
                        .get_dataset("default")
                        .and_then(|ds| ds.types().get_type(type_name))
                        .and_then(|udt| udt.definition.as_composite())
                        .cloned()
                };
                if let Some(composite_fields) = composite_fields_cloned {
                    let expanded_fields = self.expand_nested_custom_types(&composite_fields);
                    DataType::Struct(expanded_fields)
                } else {
                    data_type.clone()
                }
            }
            DataType::Struct(fields) => DataType::Struct(self.expand_nested_custom_types(fields)),
            DataType::Array(inner) => {
                DataType::Array(Box::new(self.expand_custom_data_type(inner)))
            }
            _ => data_type.clone(),
        }
    }

    fn resolve_custom_type_fields(
        &self,
        expr: &yachtsql_ir::expr::Expr,
    ) -> yachtsql_ir::expr::Expr {
        use yachtsql_ir::expr::{CastDataType, Expr};

        match expr {
            Expr::Cast {
                expr: inner,
                data_type,
            } => {
                debug_print::debug_eprintln!(
                    "[logical_to_physical::resolve] Cast with data_type={:?}, inner expr type: {}",
                    data_type,
                    std::any::type_name::<yachtsql_ir::expr::Expr>()
                );
                debug_print::debug_eprintln!(
                    "[logical_to_physical::resolve] Inner expr: {:?}",
                    inner
                );
                let resolved_inner = self.resolve_custom_type_fields(inner);
                let resolved_data_type = match data_type {
                    CastDataType::Custom(name, fields) if fields.is_empty() => {
                        match name.to_uppercase().as_str() {
                            "MACADDR" => CastDataType::MacAddr,
                            "MACADDR8" => CastDataType::MacAddr8,
                            "HSTORE" => CastDataType::Hstore,
                            "INTERVAL" => CastDataType::Interval,
                            "UUID" => CastDataType::Uuid,
                            "INT4RANGE" => CastDataType::Int4Range,
                            "INT8RANGE" => CastDataType::Int8Range,
                            "NUMRANGE" => CastDataType::NumRange,
                            "TSRANGE" => CastDataType::TsRange,
                            "TSTZRANGE" => CastDataType::TsTzRange,
                            "DATERANGE" => CastDataType::DateRange,
                            _ => {
                                let composite_fields_cloned = {
                                    let storage = self.storage.borrow();
                                    storage
                                        .get_dataset("default")
                                        .and_then(|ds| ds.types().get_type(name))
                                        .and_then(|udt| udt.definition.as_composite())
                                        .cloned()
                                };
                                if let Some(composite_fields) = composite_fields_cloned {
                                    let expanded_fields =
                                        self.expand_nested_custom_types(&composite_fields);
                                    debug_print::debug_eprintln!(
                                        "[logical_to_physical::resolve] Resolved type '{}' to fields {:?}",
                                        name,
                                        expanded_fields
                                            .iter()
                                            .map(|f| (&f.name, &f.data_type))
                                            .collect::<Vec<_>>()
                                    );
                                    CastDataType::Custom(name.clone(), expanded_fields)
                                } else {
                                    debug_print::debug_eprintln!(
                                        "[logical_to_physical::resolve] Type '{}' not found in registry",
                                        name
                                    );
                                    data_type.clone()
                                }
                            }
                        }
                    }
                    _ => data_type.clone(),
                };
                Expr::Cast {
                    expr: Box::new(resolved_inner),
                    data_type: resolved_data_type,
                }
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(self.resolve_custom_type_fields(left)),
                op: op.clone(),
                right: Box::new(self.resolve_custom_type_fields(right)),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op: op.clone(),
                expr: Box::new(self.resolve_custom_type_fields(inner)),
            },
            Expr::Function { name, args } => Expr::Function {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| self.resolve_custom_type_fields(a))
                    .collect(),
            },
            Expr::StructFieldAccess { expr: inner, field } => Expr::StructFieldAccess {
                expr: Box::new(self.resolve_custom_type_fields(inner)),
                field: field.clone(),
            },
            Expr::Tuple(exprs) => {
                debug_print::debug_eprintln!(
                    "[logical_to_physical::resolve] Tuple with {} elements",
                    exprs.len()
                );
                Expr::Tuple(
                    exprs
                        .iter()
                        .map(|e| self.resolve_custom_type_fields(e))
                        .collect(),
                )
            }
            Expr::StructLiteral { fields } => {
                use yachtsql_ir::expr::StructLiteralField;
                debug_print::debug_eprintln!(
                    "[logical_to_physical::resolve] StructLiteral with {} fields",
                    fields.len()
                );
                Expr::StructLiteral {
                    fields: fields
                        .iter()
                        .map(|field| StructLiteralField {
                            name: field.name.clone(),
                            expr: self.resolve_custom_type_fields(&field.expr),
                            declared_type: field.declared_type.clone(),
                        })
                        .collect(),
                }
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => Expr::Case {
                operand: operand
                    .as_ref()
                    .map(|o| Box::new(self.resolve_custom_type_fields(o))),
                when_then: when_then
                    .iter()
                    .map(|(w, t)| {
                        (
                            self.resolve_custom_type_fields(w),
                            self.resolve_custom_type_fields(t),
                        )
                    })
                    .collect(),
                else_expr: else_expr
                    .as_ref()
                    .map(|e| Box::new(self.resolve_custom_type_fields(e))),
            },
            Expr::IsDistinctFrom {
                left,
                right,
                negated,
            } => Expr::IsDistinctFrom {
                left: Box::new(self.resolve_custom_type_fields(left)),
                right: Box::new(self.resolve_custom_type_fields(right)),
                negated: *negated,
            },
            _ => expr.clone(),
        }
    }

    fn resolve_custom_type_in_expressions(
        &self,
        expressions: &[(yachtsql_ir::expr::Expr, Option<String>)],
    ) -> Vec<(yachtsql_ir::expr::Expr, Option<String>)> {
        expressions
            .iter()
            .map(|(expr, alias)| (self.resolve_custom_type_fields(expr), alias.clone()))
            .collect()
    }

    fn validate_join_condition_types(
        &self,
        condition: &yachtsql_ir::expr::Expr,
        left_schema: &yachtsql_storage::Schema,
        right_schema: &yachtsql_storage::Schema,
    ) -> Result<()> {
        use yachtsql_ir::expr::{BinaryOp, Expr};

        let combined_schema = {
            let mut fields = left_schema.fields().to_vec();
            fields.extend(right_schema.fields().to_vec());
            yachtsql_storage::Schema::from_fields(fields)
        };

        match condition {
            Expr::BinaryOp { left, op, right } => {
                if matches!(op, BinaryOp::Equal) {
                    let left_type = self.infer_expr_type(left, &combined_schema);
                    let right_type = self.infer_expr_type(right, &combined_schema);

                    if !self.types_are_compatible(&left_type, &right_type) {
                        return Err(Error::InvalidQuery(format!(
                            "Type mismatch in join condition: cannot compare {:?} with {:?}",
                            left_type, right_type
                        )));
                    }
                }

                if matches!(op, BinaryOp::And | BinaryOp::Or) {
                    self.validate_join_condition_types(left, left_schema, right_schema)?;
                    self.validate_join_condition_types(right, left_schema, right_schema)?;
                }

                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn infer_expr_type(
        &self,
        expr: &yachtsql_ir::expr::Expr,
        schema: &yachtsql_storage::Schema,
    ) -> yachtsql_core::types::DataType {
        use yachtsql_core::types::DataType;
        use yachtsql_ir::expr::Expr;

        match expr {
            Expr::Column { name, table } => {
                for field in schema.fields() {
                    if let Some(tbl) = table {
                        if let Some(source) = &field.source_table {
                            if source.eq_ignore_ascii_case(tbl)
                                && field.name.eq_ignore_ascii_case(name)
                            {
                                return field.data_type.clone();
                            }
                        }
                    }
                    if field.name.eq_ignore_ascii_case(name) {
                        return field.data_type.clone();
                    }
                }
                DataType::String
            }
            Expr::Literal(lit) => self.infer_literal_type(&Expr::Literal(lit.clone())),
            Expr::Cast { data_type, .. } | Expr::TryCast { data_type, .. } => {
                self.cast_data_type_to_data_type(data_type)
            }
            Expr::Tuple(exprs) => {
                let fields: Vec<_> = exprs
                    .iter()
                    .enumerate()
                    .map(|(i, e)| yachtsql_core::types::StructField {
                        name: format!("f{}", i + 1),
                        data_type: self.infer_expr_type(e, schema),
                    })
                    .collect();
                DataType::Struct(fields)
            }
            Expr::StructLiteral { fields } => {
                let struct_fields: Vec<_> = fields
                    .iter()
                    .map(|f| yachtsql_core::types::StructField {
                        name: f.name.clone(),
                        data_type: self.infer_expr_type(&f.expr, schema),
                    })
                    .collect();
                DataType::Struct(struct_fields)
            }
            Expr::BinaryOp { left, .. } => self.infer_expr_type(left, schema),
            Expr::Function { name, args } => {
                use yachtsql_ir::function::FunctionName;
                match name {
                    FunctionName::Substring
                    | FunctionName::Upper
                    | FunctionName::Lower
                    | FunctionName::Trim
                    | FunctionName::Ltrim
                    | FunctionName::Rtrim
                    | FunctionName::Concat
                    | FunctionName::Replace
                    | FunctionName::Reverse
                    | FunctionName::Left
                    | FunctionName::Right => DataType::String,
                    FunctionName::Position
                    | FunctionName::Length
                    | FunctionName::CharLength
                    | FunctionName::Abs
                    | FunctionName::Ceil
                    | FunctionName::Floor
                    | FunctionName::Round
                    | FunctionName::Sign => DataType::Int64,
                    FunctionName::Coalesce
                    | FunctionName::Nullif
                    | FunctionName::Greatest
                    | FunctionName::Least => {
                        if let Some(first_arg) = args.first() {
                            self.infer_expr_type(first_arg, schema)
                        } else {
                            DataType::String
                        }
                    }
                    FunctionName::CurrentTime | FunctionName::Localtime => DataType::Time,
                    FunctionName::CurrentDate => DataType::Date,
                    FunctionName::CurrentTimestamp
                    | FunctionName::Localtimestamp
                    | FunctionName::Now => DataType::Timestamp,
                    _ => panic!("infer_expr_type: unhandled function: {:?}", name),
                }
            }
            _ => panic!("infer_expr_type: unhandled expression type: {:?}", expr),
        }
    }

    fn types_are_compatible(
        &self,
        left: &yachtsql_core::types::DataType,
        right: &yachtsql_core::types::DataType,
    ) -> bool {
        use yachtsql_core::types::DataType;

        if left == right {
            return true;
        }

        let is_numeric = |t: &DataType| {
            matches!(
                t,
                DataType::Int64 | DataType::Float64 | DataType::Numeric(_)
            )
        };

        if is_numeric(left) && is_numeric(right) {
            return true;
        }

        let is_string = |t: &DataType| matches!(t, DataType::String);
        let is_date_like =
            |t: &DataType| matches!(t, DataType::Date | DataType::DateTime | DataType::Timestamp);

        if is_string(left) && is_string(right) {
            return true;
        }

        if is_date_like(left) && is_date_like(right) {
            return true;
        }

        if let (DataType::Struct(left_fields), DataType::Struct(right_fields)) = (left, right) {
            if left_fields.is_empty() || right_fields.is_empty() {
                return true;
            }
            if left_fields.len() == right_fields.len() {
                return left_fields
                    .iter()
                    .zip(right_fields.iter())
                    .all(|(l, r)| self.types_are_compatible(&l.data_type, &r.data_type));
            }
        }

        false
    }
}

impl LogicalToPhysicalPlanner {
    pub fn new(storage: Rc<RefCell<yachtsql_storage::Storage>>) -> Self {
        Self {
            storage,
            dialect: crate::DialectType::BigQuery,
            cte_plans: RefCell::new(HashMap::new()),
        }
    }

    pub fn with_dialect(mut self, dialect: crate::DialectType) -> Self {
        self.dialect = dialect;
        self
    }

    pub fn create_physical_plan(&self, logical_plan: &LogicalPlan) -> Result<PhysicalPlan> {
        let root_exec = self.plan_node_to_exec(logical_plan.root())?;
        Ok(PhysicalPlan::new(root_exec))
    }

    fn plan_node_to_exec(&self, node: &PlanNode) -> Result<Rc<dyn ExecutionPlan>> {
        match node {
            PlanNode::Scan {
                table_name,
                alias,
                projection: _,
            } => {
                {
                    let cte_plans = self.cte_plans.borrow();
                    if let Some(cte_plan) = cte_plans.get(table_name) {
                        return Ok(Rc::new(SubqueryScanExec::new(Rc::clone(cte_plan))));
                    }
                }

                let (dataset_name, table_id) = if let Some(dot_pos) = table_name.find('.') {
                    let dataset = &table_name[..dot_pos];
                    let table = &table_name[dot_pos + 1..];
                    (dataset, table)
                } else {
                    ("default", table_name.as_str())
                };

                let view_info = {
                    let storage = self.storage.borrow();
                    if let Some(dataset) = storage.get_dataset(dataset_name) {
                        dataset.views().get_view(table_id).map(|v| {
                            (
                                v.is_materialized(),
                                v.sql.clone(),
                                v.get_materialized_data().map(|d| d.to_vec()),
                                v.get_materialized_schema().cloned(),
                            )
                        })
                    } else {
                        None
                    }
                };

                if let Some((is_materialized, view_sql, mat_data, mat_schema)) = view_info {
                    debug_print::debug_eprintln!(
                        "[executor::logical_to_physical] Expanding view '{}' with SQL: {}",
                        table_id,
                        view_sql
                    );

                    if is_materialized {
                        if let (Some(schema), Some(rows)) = (mat_schema, mat_data) {
                            let source_table = alias.as_ref().unwrap_or(table_name);
                            let schema_with_source = schema.with_source_table(source_table);
                            let batch = crate::Table::from_rows(schema_with_source.clone(), rows)?;
                            return Ok(Rc::new(MaterializedViewScanExec::new(
                                schema_with_source,
                                batch,
                            )));
                        }
                    }

                    let parser = yachtsql_parser::Parser::new();
                    let stmts = parser.parse_sql(&view_sql).map_err(|e| {
                        Error::InvalidQuery(format!("Failed to parse view SQL: {}", e))
                    })?;

                    if stmts.is_empty() {
                        return Err(Error::InvalidQuery(
                            "View SQL produced no statements".to_string(),
                        ));
                    }

                    let sql_stmt = stmts[0].unwrap_standard();
                    let query = match sql_stmt {
                        sqlparser::ast::Statement::Query(q) => q,
                        _ => {
                            return Err(Error::InvalidQuery(
                                "View SQL is not a SELECT statement".to_string(),
                            ));
                        }
                    };

                    let plan_builder = yachtsql_parser::LogicalPlanBuilder::new()
                        .with_storage(Rc::clone(&self.storage));
                    let logical_plan = plan_builder.query_to_plan(query)?;

                    let view_exec = self.plan_node_to_exec(logical_plan.root())?;
                    let source_table = alias.as_ref().unwrap_or(table_name);
                    let view_schema = view_exec.schema().with_source_table(source_table);

                    return Ok(Rc::new(SubqueryScanExec::new_with_schema(
                        view_exec,
                        view_schema,
                    )));
                }

                if dataset_name.eq_ignore_ascii_case("information_schema") {
                    debug_print::debug_eprintln!(
                        "[executor::logical_to_physical] Handling information_schema query for table '{}'",
                        table_id
                    );
                    let info_table = InformationSchemaTable::from_str(table_id)?;
                    let provider = InformationSchemaProvider::new(Rc::clone(&self.storage));
                    let (schema, rows) = provider.query(info_table)?;

                    let source_table = alias.as_ref().unwrap_or(table_name);
                    let schema_with_source = schema.with_source_table(source_table);
                    let batch = crate::Table::from_rows(schema_with_source.clone(), rows)?;
                    return Ok(Rc::new(MaterializedViewScanExec::new(
                        schema_with_source,
                        batch,
                    )));
                }

                if dataset_name.eq_ignore_ascii_case("system") {
                    debug_print::debug_eprintln!(
                        "[executor::logical_to_physical] Handling system query for table '{}'",
                        table_id
                    );
                    let system_table = SystemTable::from_str(table_id)?;
                    let provider = SystemSchemaProvider::new(Rc::clone(&self.storage));
                    let (schema, rows) = provider.query(system_table)?;

                    let source_table = alias.as_ref().unwrap_or(table_name);
                    let schema_with_source = schema.with_source_table(source_table);
                    let batch = crate::Table::from_rows(schema_with_source.clone(), rows)?;
                    return Ok(Rc::new(MaterializedViewScanExec::new(
                        schema_with_source,
                        batch,
                    )));
                }

                let storage = self.storage.borrow_mut();

                let dataset = storage.get_dataset(dataset_name).ok_or_else(|| {
                    Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_name))
                })?;

                let table = dataset.get_table(table_id).ok_or_else(|| {
                    Error::TableNotFound(format!("Table '{}' not found", table_id))
                })?;

                let source_table = alias.as_ref().unwrap_or(table_name);
                let base_schema = table.schema().with_source_table(source_table);
                drop(storage);

                let schema = self.expand_schema_custom_types(&base_schema);

                Ok(Rc::new(TableScanExec::new(
                    schema,
                    table_name.clone(),
                    Rc::clone(&self.storage),
                )))
            }

            PlanNode::IndexScan {
                table_name,
                alias,
                index_name,
                predicate,
                projection: _,
            } => {
                let storage = self.storage.borrow_mut();

                let (dataset_name, table_id) = if let Some(dot_pos) = table_name.find('.') {
                    let dataset = &table_name[..dot_pos];
                    let table = &table_name[dot_pos + 1..];
                    (dataset, table)
                } else {
                    ("default", table_name.as_str())
                };

                let dataset = storage.get_dataset(dataset_name).ok_or_else(|| {
                    Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_name))
                })?;

                let table = dataset.get_table(table_id).ok_or_else(|| {
                    Error::TableNotFound(format!("Table '{}' not found", table_id))
                })?;

                let source_table = alias.as_ref().unwrap_or(table_name);
                let base_schema = table.schema().with_source_table(source_table);
                drop(storage);

                let schema = self.expand_schema_custom_types(&base_schema);

                Ok(Rc::new(IndexScanExec::new(
                    schema,
                    table_name.clone(),
                    index_name.clone(),
                    predicate.clone(),
                    Rc::clone(&self.storage),
                )))
            }

            PlanNode::Filter { input, predicate } => {
                let input_exec = self.plan_node_to_exec(input)?;

                Ok(Rc::new(FilterExec::new(input_exec, predicate.clone())))
            }

            PlanNode::Projection { input, expressions } => {
                let input_exec = self.plan_node_to_exec(input)?;
                let input_schema = input_exec.schema();

                let resolved_expressions = self.resolve_custom_type_in_expressions(expressions);
                let output_schema =
                    self.infer_projection_schema(&resolved_expressions, input_schema)?;

                Ok(Rc::new(
                    ProjectionWithExprExec::new(input_exec, output_schema, resolved_expressions)
                        .with_dialect(self.dialect),
                ))
            }

            PlanNode::Join {
                left,
                right,
                on,
                join_type,
            } => {
                let left_exec = self.plan_node_to_exec(left)?;
                let right_exec = self.plan_node_to_exec(right)?;

                self.validate_join_condition_types(on, left_exec.schema(), right_exec.schema())?;

                let left_tables = self.collect_table_names(left);
                let join_conditions = self.parse_join_conditions(on, &left_tables);
                let is_equi_join = !join_conditions.is_empty();

                let left_stats = left_exec.statistics();
                let right_stats = right_exec.statistics();

                let left_sorted = left_stats.is_sorted;
                let right_sorted = right_stats.is_sorted;
                let left_rows = left_stats.num_rows;

                let strategy =
                    JoinStrategy::select(left_sorted, right_sorted, is_equi_join, None, left_rows);

                match strategy {
                    JoinStrategy::Merge => Ok(Rc::new(MergeJoinExec::new(
                        left_exec,
                        right_exec,
                        join_type.clone(),
                        join_conditions,
                    )?)),
                    JoinStrategy::NestedLoop => Ok(Rc::new(NestedLoopJoinExec::new(
                        left_exec,
                        right_exec,
                        join_type.clone(),
                        Some(on.clone()),
                    )?)),
                    JoinStrategy::Hash | JoinStrategy::IndexNestedLoop { .. } => {
                        Ok(Rc::new(HashJoinExec::new(
                            left_exec,
                            right_exec,
                            join_type.clone(),
                            join_conditions,
                        )?))
                    }
                }
            }

            PlanNode::LateralJoin {
                left,
                right,
                on: _,
                join_type,
            } => {
                let left_exec = self.plan_node_to_exec(left)?;

                Ok(Rc::new(LateralJoinExec::new(
                    left_exec,
                    (**right).clone(),
                    join_type.clone(),
                    Rc::clone(&self.storage),
                )?))
            }

            PlanNode::Aggregate {
                group_by,
                aggregates,
                input,
                grouping_metadata: _,
            } => {
                let input_exec = self.plan_node_to_exec(input)?;

                let agg_with_aliases: Vec<(yachtsql_ir::expr::Expr, Option<String>)> =
                    aggregates.iter().map(|expr| (expr.clone(), None)).collect();

                let input_stats = input_exec.statistics();
                let input_sorted = input_stats.is_sorted;

                let input_sort_columns: Vec<String> =
                    input_stats.sort_columns.clone().unwrap_or_default();

                let group_by_columns: Vec<String> = group_by
                    .iter()
                    .filter_map(|expr| {
                        if let yachtsql_ir::expr::Expr::Column { name, .. } = expr {
                            Some(name.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                let strategy = AggregateStrategy::select_with_columns(
                    input_sorted,
                    &input_sort_columns,
                    &group_by_columns,
                );

                match strategy {
                    AggregateStrategy::Sort => Ok(Rc::new(SortAggregateExec::new(
                        input_exec,
                        group_by.clone(),
                        agg_with_aliases,
                        None,
                    )?)),
                    AggregateStrategy::Hash => Ok(Rc::new(AggregateExec::new(
                        input_exec,
                        group_by.clone(),
                        agg_with_aliases,
                        None,
                    )?)),
                }
            }

            PlanNode::Sort { order_by, input } => {
                let input_exec = self.plan_node_to_exec(input)?;

                Ok(Rc::new(SortExec::new(input_exec, order_by.clone())?))
            }

            PlanNode::Limit {
                limit,
                offset,
                input,
            } => {
                let input_exec = self.plan_node_to_exec(input)?;
                Ok(Rc::new(LimitExec::new(input_exec, *limit, *offset)))
            }

            PlanNode::Distinct { input } => {
                let input_exec = self.plan_node_to_exec(input)?;
                Ok(Rc::new(DistinctExec::new(input_exec)))
            }

            PlanNode::DistinctOn { expressions, input } => {
                let input_exec = self.plan_node_to_exec(input)?;

                Ok(Rc::new(DistinctOnExec::new(
                    input_exec,
                    expressions.clone(),
                )))
            }

            PlanNode::Union { left, right, all } => {
                let left_exec = self.plan_node_to_exec(left)?;
                let right_exec = self.plan_node_to_exec(right)?;
                Ok(Rc::new(UnionExec::new(left_exec, right_exec, *all)?))
            }

            PlanNode::Intersect { left, right, all } => {
                let left_exec = self.plan_node_to_exec(left)?;
                let right_exec = self.plan_node_to_exec(right)?;
                Ok(Rc::new(IntersectExec::new(left_exec, right_exec, *all)?))
            }

            PlanNode::Except { left, right, all } => {
                let left_exec = self.plan_node_to_exec(left)?;
                let right_exec = self.plan_node_to_exec(right)?;
                Ok(Rc::new(ExceptExec::new(left_exec, right_exec, *all)?))
            }

            PlanNode::Merge {
                target_table,
                target_alias,
                source,
                source_alias,
                on_condition,
                when_matched,
                when_not_matched,
                when_not_matched_by_source,
                returning,
            } => {
                let source_exec = self.plan_node_to_exec(source)?;

                let returning_spec = self.convert_returning_clause(returning)?;

                let storage = Rc::clone(&self.storage);

                let fk_enforcer =
                    Rc::new(crate::query_executor::enforcement::ForeignKeyEnforcer::new());

                Ok(Rc::new(MergeExec::new(
                    target_table.clone(),
                    target_alias.clone(),
                    source_exec,
                    source_alias.clone(),
                    on_condition.clone(),
                    when_matched.clone(),
                    when_not_matched.clone(),
                    when_not_matched_by_source.clone(),
                    returning_spec,
                    storage,
                    fk_enforcer,
                )?))
            }

            PlanNode::Unnest {
                array_expr,
                alias,
                column_alias,
                with_offset,
                offset_alias,
            } => {
                use yachtsql_storage::{Field, Schema};

                let element_name = column_alias
                    .clone()
                    .or_else(|| alias.clone())
                    .unwrap_or_else(|| "value".to_string());

                let element_type = self.infer_unnest_element_type(array_expr);

                let mut field = Field::nullable(element_name, element_type);
                if let Some(table_alias) = alias {
                    field = field.with_source_table(table_alias.clone());
                }
                let mut fields = vec![field];

                if *with_offset {
                    let offset_name = offset_alias
                        .clone()
                        .unwrap_or_else(|| "ordinality".to_string());
                    let mut offset_field =
                        Field::nullable(offset_name, yachtsql_core::types::DataType::Int64);
                    if let Some(table_alias) = alias {
                        offset_field = offset_field.with_source_table(table_alias.clone());
                    }
                    fields.push(offset_field);
                }

                let schema = Schema::from_fields(fields);

                let optimizer_expr = self.convert_ir_expr_to_optimizer(array_expr);

                Ok(Rc::new(UnnestExec {
                    schema,
                    array_expr: optimizer_expr,
                    with_offset: *with_offset,
                }))
            }

            PlanNode::Cte {
                name,
                cte_plan,
                input,
                recursive: _,
                use_union_all: _,
                materialization_hint,
            } => {
                let cte_exec = self.plan_node_to_exec(cte_plan)?;

                self.cte_plans
                    .borrow_mut()
                    .insert(name.clone(), Rc::clone(&cte_exec));

                let input_exec = self.plan_node_to_exec(input)?;

                self.cte_plans.borrow_mut().remove(name);

                let materialized = match materialization_hint {
                    Some(sqlparser::ast::CteAsMaterialized::Materialized) => true,
                    Some(sqlparser::ast::CteAsMaterialized::NotMaterialized) => false,
                    None => true,
                };

                Ok(Rc::new(CteExec::new(cte_exec, input_exec, materialized)))
            }

            PlanNode::EmptyRelation => {
                let schema = yachtsql_storage::Schema::from_fields(vec![]);
                Ok(Rc::new(EmptyRelationExec::new(schema)))
            }

            PlanNode::Values { rows } => {
                let schema = infer_values_schema(rows);
                Ok(Rc::new(ValuesExec::new(schema, rows.clone())))
            }

            PlanNode::Window {
                window_exprs,
                input,
            } => {
                let input_exec = self.plan_node_to_exec(input)?;

                Ok(Rc::new(WindowExec::new(input_exec, window_exprs.clone())?))
            }

            PlanNode::SubqueryScan { subquery, alias: _ } => {
                let subquery_exec = self.plan_node_to_exec(subquery)?;

                Ok(Rc::new(SubqueryScanExec::new(subquery_exec)))
            }

            PlanNode::ArrayJoin {
                input,
                arrays,
                is_left,
                is_unaligned,
            } => {
                let input_exec = self.plan_node_to_exec(input)?;
                Ok(Rc::new(ArrayJoinExec::new(
                    input_exec,
                    arrays.clone(),
                    *is_left,
                    *is_unaligned,
                )?))
            }

            PlanNode::TableSample {
                input,
                method,
                size,
                seed,
            } => {
                let input_exec = self.plan_node_to_exec(input)?;

                let physical_method = match method {
                    yachtsql_ir::plan::SamplingMethod::Bernoulli => SamplingMethod::Bernoulli,
                    yachtsql_ir::plan::SamplingMethod::System => SamplingMethod::System,
                };

                let physical_size = match size {
                    yachtsql_ir::plan::SampleSize::Percent(p) => SampleSize::Percent(*p),
                    yachtsql_ir::plan::SampleSize::Rows(r) => SampleSize::Rows(*r),
                };

                Ok(Rc::new(TableSampleExec::new(
                    input_exec,
                    physical_method,
                    physical_size,
                    *seed,
                )?))
            }

            PlanNode::Pivot {
                input,
                aggregate_expr,
                aggregate_function,
                pivot_column,
                pivot_values,
                group_by_columns,
            } => {
                let input_exec = self.plan_node_to_exec(input)?;

                let agg_fn = PivotAggregateFunction::from_name(aggregate_function)?;

                Ok(Rc::new(PivotExec::new(
                    input_exec,
                    aggregate_expr.clone(),
                    agg_fn,
                    pivot_column.clone(),
                    pivot_values.clone(),
                    group_by_columns.clone(),
                )?))
            }

            PlanNode::Unpivot {
                input,
                value_column,
                name_column,
                unpivot_columns,
            } => {
                let input_exec = self.plan_node_to_exec(input)?;

                Ok(Rc::new(UnpivotExec::new(
                    input_exec,
                    value_column.clone(),
                    name_column.clone(),
                    unpivot_columns.clone(),
                )?))
            }

            PlanNode::TableValuedFunction {
                function_name,
                args,
                alias: _,
            } => {
                use yachtsql_storage::{Field, Schema};

                let schema = match function_name.to_uppercase().as_str() {
                    "EACH" => Schema::from_fields(vec![
                        Field::nullable("key", yachtsql_core::types::DataType::String),
                        Field::nullable("value", yachtsql_core::types::DataType::String),
                    ]),
                    "JSON_EACH" | "JSONB_EACH" => Schema::from_fields(vec![
                        Field::nullable("key", yachtsql_core::types::DataType::String),
                        Field::nullable("value", yachtsql_core::types::DataType::Json),
                    ]),
                    "JSON_EACH_TEXT" | "JSONB_EACH_TEXT" => Schema::from_fields(vec![
                        Field::nullable("key", yachtsql_core::types::DataType::String),
                        Field::nullable("value", yachtsql_core::types::DataType::String),
                    ]),
                    "SKEYS" => Schema::from_fields(vec![Field::nullable(
                        "key",
                        yachtsql_core::types::DataType::String,
                    )]),
                    "SVALS" => Schema::from_fields(vec![Field::nullable(
                        "value",
                        yachtsql_core::types::DataType::String,
                    )]),
                    "POPULATE_RECORD" => {
                        if args.is_empty() {
                            return Err(Error::InvalidQuery(
                                "populate_record requires at least 1 argument".to_string(),
                            ));
                        }
                        self.extract_record_type_schema(&args[0])?
                    }
                    _ => {
                        return Err(Error::UnsupportedFeature(format!(
                            "Table-valued function '{}' not yet supported in optimizer path",
                            function_name
                        )));
                    }
                };

                Ok(Rc::new(TableValuedFunctionExec::new(
                    schema,
                    function_name.clone(),
                    args.clone(),
                    Rc::clone(&self.storage),
                )))
            }

            _ => Err(Error::UnsupportedFeature(format!(
                "Physical plan conversion not yet implemented for {:?}",
                node
            ))),
        }
    }

    fn infer_returning_expr_type(
        &self,
        expr: &yachtsql_ir::expr::Expr,
    ) -> Result<yachtsql_core::types::DataType> {
        use yachtsql_ir::expr::Expr;
        match expr {
            Expr::Column { .. } => Ok(yachtsql_core::types::DataType::String),

            Expr::Function { name, args, .. }
                if matches!(name, yachtsql_ir::FunctionName::MergeAction) && args.is_empty() =>
            {
                Ok(yachtsql_core::types::DataType::String)
            }

            _ => panic!(
                "infer_returning_expr_type: unhandled expression type: {:?}",
                expr
            ),
        }
    }

    fn convert_returning_clause(
        &self,
        returning: &Option<Vec<(yachtsql_ir::expr::Expr, Option<String>)>>,
    ) -> Result<ReturningSpec> {
        match returning {
            None => Ok(ReturningSpec::None),
            Some(items) if items.is_empty() => Ok(ReturningSpec::None),
            Some(items)
                if items.len() == 1 && matches!(&items[0].0, yachtsql_ir::expr::Expr::Wildcard) =>
            {
                Ok(ReturningSpec::AllColumns)
            }
            Some(items) => {
                let mut all_columns = true;
                for (expr, _) in items {
                    if !matches!(expr, yachtsql_ir::expr::Expr::Column { .. }) {
                        all_columns = false;
                        break;
                    }
                }

                if all_columns {
                    let columns = items
                        .iter()
                        .map(|(expr, alias)| {
                            if let yachtsql_ir::expr::Expr::Column { name, table, .. } = expr {
                                let origin = match table.as_deref() {
                                    Some("target") | Some("t") => ReturningColumnOrigin::Target,
                                    Some("source") | Some("s") => ReturningColumnOrigin::Source,
                                    Some(table_name) => {
                                        ReturningColumnOrigin::Table(table_name.to_string())
                                    }
                                    None => ReturningColumnOrigin::Target,
                                };
                                Ok(ReturningColumn {
                                    source_name: name.clone(),
                                    output_name: alias.clone().unwrap_or_else(|| name.clone()),
                                    origin,
                                })
                            } else {
                                Err(Error::internal("Expected column reference"))
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(ReturningSpec::Columns(columns))
                } else {
                    let expr_items = items
                        .iter()
                        .map(|(expr, alias)| {
                            let data_type = self.infer_returning_expr_type(expr)?;
                            let origin = ReturningColumnOrigin::Expression;
                            Ok(ReturningExpressionItem {
                                expr: expr.clone(),
                                output_name: alias.clone(),
                                data_type,
                                origin,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(ReturningSpec::Expressions(expr_items))
                }
            }
        }
    }

    fn infer_unnest_element_type(
        &self,
        expr: &yachtsql_ir::expr::Expr,
    ) -> yachtsql_core::types::DataType {
        use yachtsql_core::types::DataType;
        use yachtsql_ir::expr::Expr;

        match expr {
            Expr::Literal(yachtsql_ir::expr::LiteralValue::Array(elements)) => {
                if let Some(first) = elements.first() {
                    self.infer_literal_type(first)
                } else {
                    DataType::String
                }
            }

            Expr::Literal(yachtsql_ir::expr::LiteralValue::Null) => DataType::Unknown,

            Expr::Cast { data_type, .. } => self.cast_data_type_to_data_type(data_type),

            _ => panic!(
                "infer_aggregate_result_type: unhandled expression type: {:?}",
                expr
            ),
        }
    }

    fn cast_data_type_to_data_type(
        &self,
        cast_type: &yachtsql_ir::expr::CastDataType,
    ) -> yachtsql_core::types::DataType {
        use yachtsql_core::types::DataType;
        use yachtsql_ir::expr::CastDataType;

        match cast_type {
            CastDataType::Int64 => DataType::Int64,
            CastDataType::Float64 => DataType::Float64,
            CastDataType::Numeric(prec) => DataType::Numeric(*prec),
            CastDataType::String => DataType::String,
            CastDataType::Bytes => DataType::Bytes,
            CastDataType::Date => DataType::Date,
            CastDataType::DateTime => DataType::DateTime,
            CastDataType::Time => DataType::Time,
            CastDataType::Timestamp => DataType::Timestamp,
            CastDataType::TimestampTz => DataType::Timestamp,
            CastDataType::Bool => DataType::Bool,
            CastDataType::Json => DataType::Json,
            CastDataType::Array(inner) => {
                DataType::Array(Box::new(self.cast_data_type_to_data_type(inner)))
            }
            CastDataType::Geography => DataType::Geography,
            CastDataType::Uuid => DataType::Uuid,
            CastDataType::Interval => DataType::Interval,
            CastDataType::Vector(dim) => DataType::Vector(*dim),
            CastDataType::Hstore => DataType::Hstore,
            CastDataType::MacAddr => DataType::MacAddr,
            CastDataType::MacAddr8 => DataType::MacAddr8,
            CastDataType::Int4Range => DataType::Range(yachtsql_core::types::RangeType::Int4Range),
            CastDataType::Int8Range => DataType::Range(yachtsql_core::types::RangeType::Int8Range),
            CastDataType::NumRange => DataType::Range(yachtsql_core::types::RangeType::NumRange),
            CastDataType::TsRange => DataType::Range(yachtsql_core::types::RangeType::TsRange),
            CastDataType::TsTzRange => DataType::Range(yachtsql_core::types::RangeType::TsTzRange),
            CastDataType::DateRange => DataType::Range(yachtsql_core::types::RangeType::DateRange),
            CastDataType::Custom(_, fields) => DataType::Struct(fields.clone()),
        }
    }

    fn infer_literal_type(&self, expr: &yachtsql_ir::expr::Expr) -> yachtsql_core::types::DataType {
        use yachtsql_core::types::DataType;
        use yachtsql_ir::expr::{Expr, LiteralValue};

        match expr {
            Expr::Literal(lit) => match lit {
                LiteralValue::Null => DataType::Unknown,
                LiteralValue::Boolean(_) => DataType::Bool,
                LiteralValue::Int64(_) => DataType::Int64,
                LiteralValue::Float64(_) => DataType::Float64,
                LiteralValue::Numeric(_) => DataType::Numeric(None),
                LiteralValue::String(_) => DataType::String,
                LiteralValue::Bytes(_) => DataType::Bytes,
                LiteralValue::Date(_) => DataType::Date,
                LiteralValue::Timestamp(_) => DataType::Timestamp,
                LiteralValue::Json(_) => DataType::Json,
                LiteralValue::Uuid(_) => DataType::Uuid,
                LiteralValue::Vector(v) => DataType::Vector(v.len()),
                LiteralValue::Interval(_) => DataType::Interval,
                LiteralValue::Range(_) => {
                    DataType::Range(yachtsql_core::types::RangeType::Int8Range)
                }
                LiteralValue::Point(_) => DataType::Point,
                LiteralValue::PgBox(_) => DataType::PgBox,
                LiteralValue::Circle(_) => DataType::Circle,
                LiteralValue::Array(_) => DataType::String,
                LiteralValue::MacAddr(_) => DataType::MacAddr,
                LiteralValue::MacAddr8(_) => DataType::MacAddr8,
            },
            _ => panic!("infer_literal_type: unhandled expression type: {:?}", expr),
        }
    }

    fn convert_ir_expr_to_optimizer(
        &self,
        expr: &yachtsql_ir::expr::Expr,
    ) -> yachtsql_optimizer::expr::Expr {
        expr.clone()
    }

    fn extract_record_type_schema(
        &self,
        expr: &yachtsql_ir::expr::Expr,
    ) -> Result<yachtsql_storage::Schema> {
        use yachtsql_ir::expr::{CastDataType, Expr};
        use yachtsql_storage::{Field, Schema};

        match expr {
            Expr::Cast { data_type, .. } => match data_type {
                CastDataType::Custom(type_name, fields) => {
                    if !fields.is_empty() {
                        let schema_fields: Vec<Field> = fields
                            .iter()
                            .map(|f| Field::nullable(&f.name, f.data_type.clone()))
                            .collect();
                        Ok(Schema::from_fields(schema_fields))
                    } else {
                        let storage = self.storage.borrow();
                        if let Some(table) = storage.get_table(type_name) {
                            return Ok(table.schema().clone());
                        }
                        Err(Error::InvalidQuery(format!(
                            "Could not find table/type '{}' for populate_record",
                            type_name
                        )))
                    }
                }
                _ => Err(Error::InvalidQuery(
                    "populate_record requires first argument to be cast to a record type"
                        .to_string(),
                )),
            },
            _ => Err(Error::InvalidQuery(
                "populate_record requires first argument to be cast to a record type".to_string(),
            )),
        }
    }
}
