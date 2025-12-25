use rust_decimal::Decimal;
use sqlparser::ast;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::DataType;
use yachtsql_ir::plan::{FunctionArg, FunctionBody};
use yachtsql_ir::{
    AggregateFunction, BinaryOp, DateTimeField, Expr, JsonPathElement, Literal, LogicalPlan,
    PlanSchema, ScalarFunction, SortExpr, TrimWhere, UnaryOp, WeekStartDay, WhenClause,
    WindowFrame, WindowFrameBound, WindowFrameUnit, WindowFunction,
};

use crate::FunctionDefinition;

pub type SubqueryPlannerFn<'a> = &'a dyn Fn(&ast::Query) -> Result<LogicalPlan>;
pub type UdfResolverFn<'a> = &'a dyn Fn(&str) -> Option<FunctionDefinition>;

pub struct ExprPlanner;

impl ExprPlanner {
    pub fn plan_expr(sql_expr: &ast::Expr, schema: &PlanSchema) -> Result<Expr> {
        Self::plan_expr_with_subquery(sql_expr, schema, None)
    }

    pub fn plan_expr_with_subquery(
        sql_expr: &ast::Expr,
        schema: &PlanSchema,
        subquery_planner: Option<SubqueryPlannerFn>,
    ) -> Result<Expr> {
        Self::plan_expr_full(sql_expr, schema, subquery_planner, &[], None)
    }

    pub fn plan_expr_with_named_windows(
        sql_expr: &ast::Expr,
        schema: &PlanSchema,
        subquery_planner: Option<SubqueryPlannerFn>,
        named_windows: &[ast::NamedWindowDefinition],
    ) -> Result<Expr> {
        Self::plan_expr_full(sql_expr, schema, subquery_planner, named_windows, None)
    }

    pub fn plan_expr_with_udf_resolver(
        sql_expr: &ast::Expr,
        schema: &PlanSchema,
        subquery_planner: Option<SubqueryPlannerFn>,
        named_windows: &[ast::NamedWindowDefinition],
        udf_resolver: Option<UdfResolverFn>,
    ) -> Result<Expr> {
        Self::plan_expr_full(
            sql_expr,
            schema,
            subquery_planner,
            named_windows,
            udf_resolver,
        )
    }

    fn plan_expr_full(
        sql_expr: &ast::Expr,
        schema: &PlanSchema,
        subquery_planner: Option<SubqueryPlannerFn>,
        named_windows: &[ast::NamedWindowDefinition],
        udf_resolver: Option<UdfResolverFn>,
    ) -> Result<Expr> {
        match sql_expr {
            ast::Expr::Identifier(ident) => {
                if ident.value.eq_ignore_ascii_case("DEFAULT") {
                    return Ok(Expr::Default);
                }
                if ident.value.starts_with('@') {
                    return Ok(Expr::Variable {
                        name: ident.value.clone(),
                    });
                }
                Self::resolve_column(&ident.value, None, schema)
            }

            ast::Expr::CompoundIdentifier(parts) => {
                Self::resolve_compound_identifier(parts, schema)
            }

            ast::Expr::Value(val) => {
                if let ast::Value::Placeholder(name) = &val.value {
                    return Ok(Expr::Variable { name: name.clone() });
                }
                Ok(Expr::Literal(Self::plan_literal(&val.value)?))
            }

            ast::Expr::BinaryOp { left, op, right } => {
                let left = Self::plan_expr_full(
                    left,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let right = Self::plan_expr_full(
                    right,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let op = Self::plan_binary_op(op)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                })
            }

            ast::Expr::UnaryOp { op, expr } => {
                if matches!(
                    (op, expr.as_ref()),
                    (
                        ast::UnaryOperator::Minus,
                        ast::Expr::Value(ast::ValueWithSpan {
                            value: ast::Value::Number(n, _),
                            ..
                        })
                    ) if n == "9223372036854775808"
                ) {
                    return Ok(Expr::Literal(Literal::Int64(i64::MIN)));
                }
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let op = Self::plan_unary_op(op)?;
                Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(expr),
                })
            }

            ast::Expr::Function(func) => {
                Self::plan_function(func, schema, subquery_planner, named_windows, udf_resolver)
            }

            ast::Expr::IsNull(inner) => {
                let expr = Self::plan_expr_full(
                    inner,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::IsNull {
                    expr: Box::new(expr),
                    negated: false,
                })
            }

            ast::Expr::IsNotNull(inner) => {
                let expr = Self::plan_expr_full(
                    inner,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::IsNull {
                    expr: Box::new(expr),
                    negated: true,
                })
            }

            ast::Expr::Nested(inner) => {
                Self::plan_expr_full(inner, schema, subquery_planner, named_windows, udf_resolver)
            }

            ast::Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => Self::plan_case(
                operand.as_deref(),
                conditions,
                else_result.as_deref(),
                schema,
            ),

            ast::Expr::Cast {
                expr,
                data_type,
                kind,
                ..
            } => {
                use sqlparser::ast::CastKind;
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let data_type = Self::plan_data_type(data_type)?;
                let safe = matches!(kind, CastKind::SafeCast | CastKind::TryCast);
                Ok(Expr::Cast {
                    expr: Box::new(expr),
                    data_type,
                    safe,
                })
            }

            ast::Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let list = list
                    .iter()
                    .map(|e| {
                        Self::plan_expr_full(
                            e,
                            schema,
                            subquery_planner,
                            named_windows,
                            udf_resolver,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::InList {
                    expr: Box::new(expr),
                    list,
                    negated: *negated,
                })
            }

            ast::Expr::InUnnest {
                expr,
                array_expr,
                negated,
            } => {
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let array_expr = Self::plan_expr_full(
                    array_expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::InUnnest {
                    expr: Box::new(expr),
                    array_expr: Box::new(array_expr),
                    negated: *negated,
                })
            }

            ast::Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let low = Self::plan_expr_full(
                    low,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let high = Self::plan_expr_full(
                    high,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::Between {
                    expr: Box::new(expr),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated: *negated,
                })
            }

            ast::Expr::Like {
                expr,
                pattern,
                negated,
                any,
                ..
            } => {
                if *any && let ast::Expr::Tuple(patterns) = pattern.as_ref() {
                    return Self::plan_like_all_any_from_patterns(
                        expr,
                        patterns,
                        *negated,
                        false,
                        false,
                        schema,
                        subquery_planner,
                    );
                }
                if let Some((is_all, patterns)) = Self::extract_all_any_patterns(pattern) {
                    return Self::plan_like_all_any_from_patterns(
                        expr,
                        &patterns,
                        *negated,
                        is_all,
                        false,
                        schema,
                        subquery_planner,
                    );
                }
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let pattern = Self::plan_expr_full(
                    pattern,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::Like {
                    expr: Box::new(expr),
                    pattern: Box::new(pattern),
                    negated: *negated,
                    case_insensitive: false,
                })
            }

            ast::Expr::ILike {
                expr,
                pattern,
                negated,
                any,
                ..
            } => {
                if *any && let ast::Expr::Tuple(patterns) = pattern.as_ref() {
                    return Self::plan_like_all_any_from_patterns(
                        expr,
                        patterns,
                        *negated,
                        false,
                        true,
                        schema,
                        subquery_planner,
                    );
                }
                if let Some((is_all, patterns)) = Self::extract_all_any_patterns(pattern) {
                    return Self::plan_like_all_any_from_patterns(
                        expr,
                        &patterns,
                        *negated,
                        is_all,
                        true,
                        schema,
                        subquery_planner,
                    );
                }
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let pattern = Self::plan_expr_full(
                    pattern,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::Like {
                    expr: Box::new(expr),
                    pattern: Box::new(pattern),
                    negated: *negated,
                    case_insensitive: true,
                })
            }

            ast::Expr::AllOp {
                left,
                compare_op,
                right,
            } => Self::plan_all_any_op(left, compare_op, right, true, schema, subquery_planner),

            ast::Expr::AnyOp {
                left,
                compare_op,
                right,
                ..
            } => Self::plan_all_any_op(left, compare_op, right, false, schema, subquery_planner),

            ast::Expr::IsTrue(inner) => {
                let expr = Self::plan_expr_full(
                    inner,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::BinaryOp {
                    left: Box::new(expr),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::Literal(Literal::Bool(true))),
                })
            }

            ast::Expr::IsFalse(inner) => {
                let expr = Self::plan_expr_full(
                    inner,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::BinaryOp {
                    left: Box::new(expr),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::Literal(Literal::Bool(false))),
                })
            }

            ast::Expr::Array(arr) => {
                let element_type = arr
                    .element_type
                    .as_ref()
                    .map(|dt| Self::plan_data_type(dt))
                    .transpose()?;

                let struct_field_names: Option<Vec<String>> = element_type.as_ref().and_then(|t| {
                    if let DataType::Struct(fields) = t {
                        Some(fields.iter().map(|f| f.name.clone()).collect())
                    } else {
                        None
                    }
                });

                let elements = arr
                    .elem
                    .iter()
                    .map(|e| {
                        let mut expr = Self::plan_expr_full(
                            e,
                            schema,
                            subquery_planner,
                            named_windows,
                            udf_resolver,
                        )?;
                        if let (Some(names), Expr::Struct { fields }) = (&struct_field_names, &expr)
                        {
                            let new_fields: Vec<(Option<String>, Expr)> = fields
                                .iter()
                                .enumerate()
                                .map(|(i, (old_name, field_expr))| {
                                    let name = if old_name.is_some() {
                                        old_name.clone()
                                    } else if i < names.len() {
                                        Some(names[i].clone())
                                    } else {
                                        None
                                    };
                                    (name, field_expr.clone())
                                })
                                .collect();
                            expr = Expr::Struct { fields: new_fields };
                        }
                        Ok(expr)
                    })
                    .collect::<Result<Vec<_>>>()?;

                let all_literals = elements.iter().all(|e| matches!(e, Expr::Literal(_)));
                if all_literals {
                    let literals: Vec<Literal> = elements
                        .into_iter()
                        .filter_map(|e| match e {
                            Expr::Literal(lit) => Some(lit),
                            Expr::Struct { fields } => {
                                let struct_fields: Vec<(String, Literal)> = fields
                                    .into_iter()
                                    .enumerate()
                                    .filter_map(|(i, (name, expr))| {
                                        if let Expr::Literal(lit) = expr {
                                            let field_name =
                                                name.unwrap_or_else(|| format!("_field{}", i));
                                            Some((field_name, lit))
                                        } else {
                                            None
                                        }
                                    })
                                    .collect();
                                Some(Literal::Struct(struct_fields))
                            }
                            _ => None,
                        })
                        .collect();
                    Ok(Expr::Literal(Literal::Array(literals)))
                } else {
                    Ok(Expr::Array {
                        elements,
                        element_type,
                    })
                }
            }

            ast::Expr::Interval(interval) => Self::plan_interval(interval, schema),

            ast::Expr::CompoundFieldAccess { root, access_chain } => {
                let mut result = Self::plan_expr_full(
                    root,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                for accessor in access_chain {
                    match accessor {
                        ast::AccessExpr::Subscript(sub) => match sub {
                            ast::Subscript::Index { index } => {
                                let index_expr = Self::plan_expr_full(
                                    index,
                                    schema,
                                    subquery_planner,
                                    named_windows,
                                    udf_resolver,
                                )?;
                                result = Expr::ArrayAccess {
                                    array: Box::new(result),
                                    index: Box::new(index_expr),
                                };
                            }
                            ast::Subscript::Slice { .. } => {
                                return Err(Error::unsupported("Array slice not yet supported"));
                            }
                        },
                        ast::AccessExpr::Dot(ident) => {
                            let field_name = match ident {
                                ast::Expr::Identifier(id) => id.value.clone(),
                                _ => {
                                    return Err(Error::unsupported(format!(
                                        "Unsupported field accessor: {:?}",
                                        ident
                                    )));
                                }
                            };
                            result = Expr::StructAccess {
                                expr: Box::new(result),
                                field: field_name,
                            };
                        }
                    }
                }
                Ok(result)
            }

            ast::Expr::TypedString(typed_string) => {
                let ir_data_type = Self::plan_data_type(&typed_string.data_type)?;
                let value_str = match &typed_string.value.value {
                    ast::Value::SingleQuotedString(s) => s.clone(),
                    ast::Value::DoubleQuotedString(s) => s.clone(),
                    _ => format!("{}", typed_string.value.value),
                };
                Ok(Expr::TypedString {
                    data_type: ir_data_type,
                    value: value_str,
                })
            }

            ast::Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let start = substring_from
                    .as_ref()
                    .map(|e| {
                        Self::plan_expr_full(
                            e,
                            schema,
                            subquery_planner,
                            named_windows,
                            udf_resolver,
                        )
                    })
                    .transpose()?
                    .map(Box::new);
                let length = substring_for
                    .as_ref()
                    .map(|e| {
                        Self::plan_expr_full(
                            e,
                            schema,
                            subquery_planner,
                            named_windows,
                            udf_resolver,
                        )
                    })
                    .transpose()?
                    .map(Box::new);
                Ok(Expr::Substring {
                    expr: Box::new(expr),
                    start,
                    length,
                })
            }

            ast::Expr::Trim {
                expr,
                trim_where,
                trim_what,
                trim_characters,
            } => {
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let trim_what = if let Some(e) = trim_what {
                    Some(Box::new(Self::plan_expr_full(
                        e,
                        schema,
                        subquery_planner,
                        named_windows,
                        udf_resolver,
                    )?))
                } else if let Some(chars) = trim_characters {
                    if let Some(first_char) = chars.first() {
                        Some(Box::new(Self::plan_expr_full(
                            first_char,
                            schema,
                            subquery_planner,
                            named_windows,
                            udf_resolver,
                        )?))
                    } else {
                        None
                    }
                } else {
                    None
                };
                let trim_where_ir = match trim_where {
                    Some(ast::TrimWhereField::Both) | None => TrimWhere::Both,
                    Some(ast::TrimWhereField::Leading) => TrimWhere::Leading,
                    Some(ast::TrimWhereField::Trailing) => TrimWhere::Trailing,
                };
                Ok(Expr::Trim {
                    expr: Box::new(expr),
                    trim_what,
                    trim_where: trim_where_ir,
                })
            }

            ast::Expr::IsDistinctFrom(left, right) => {
                let left = Self::plan_expr_full(
                    left,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let right = Self::plan_expr_full(
                    right,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::IsDistinctFrom {
                    left: Box::new(left),
                    right: Box::new(right),
                    negated: false,
                })
            }

            ast::Expr::IsNotDistinctFrom(left, right) => {
                let left = Self::plan_expr_full(
                    left,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let right = Self::plan_expr_full(
                    right,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::IsDistinctFrom {
                    left: Box::new(left),
                    right: Box::new(right),
                    negated: true,
                })
            }

            ast::Expr::Floor { expr, .. } => {
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::ScalarFunction {
                    name: ScalarFunction::Floor,
                    args: vec![expr],
                })
            }

            ast::Expr::Ceil { expr, .. } => {
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::ScalarFunction {
                    name: ScalarFunction::Ceil,
                    args: vec![expr],
                })
            }

            ast::Expr::Struct { values, .. } => {
                let mut fields = Vec::new();
                for value in values {
                    match value {
                        ast::Expr::Named { expr, name } => {
                            let ir_expr = Self::plan_expr_full(
                                expr,
                                schema,
                                subquery_planner,
                                named_windows,
                                udf_resolver,
                            )?;
                            fields.push((Some(name.value.clone()), ir_expr));
                        }
                        other => {
                            let ir_expr = Self::plan_expr_full(
                                other,
                                schema,
                                subquery_planner,
                                named_windows,
                                udf_resolver,
                            )?;
                            fields.push((None, ir_expr));
                        }
                    }
                }
                Ok(Expr::Struct { fields })
            }

            ast::Expr::Tuple(exprs) => {
                let fields: Vec<(Option<String>, Expr)> = exprs
                    .iter()
                    .map(|e| {
                        let expr = Self::plan_expr_full(
                            e,
                            schema,
                            subquery_planner,
                            named_windows,
                            udf_resolver,
                        )?;
                        Ok((None, expr))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::Struct { fields })
            }

            ast::Expr::Extract { field, expr, .. } => {
                let ir_field = Self::plan_datetime_field(field)?;
                let ir_expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::Extract {
                    field: ir_field,
                    expr: Box::new(ir_expr),
                })
            }

            ast::Expr::Exists { subquery, negated } => {
                let planner = subquery_planner.ok_or_else(|| {
                    Error::unsupported("EXISTS subquery requires subquery planner context")
                })?;
                let plan = planner(subquery)?;
                Ok(Expr::Exists {
                    subquery: Box::new(plan),
                    negated: *negated,
                })
            }

            ast::Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let planned_expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let planner = subquery_planner.ok_or_else(|| {
                    Error::unsupported("IN subquery requires subquery planner context")
                })?;
                let plan = planner(subquery)?;
                Ok(Expr::InSubquery {
                    expr: Box::new(planned_expr),
                    subquery: Box::new(plan),
                    negated: *negated,
                })
            }

            ast::Expr::Subquery(query) => {
                let planner = subquery_planner.ok_or_else(|| {
                    Error::unsupported("Scalar subquery requires subquery planner context")
                })?;
                let plan = planner(query)?;
                Ok(Expr::Subquery(Box::new(plan)))
            }

            ast::Expr::Lambda(lambda) => {
                let params: Vec<String> = lambda.params.iter().map(|p| p.value.clone()).collect();
                let body = Self::plan_expr_full(
                    &lambda.body,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::Lambda {
                    params,
                    body: Box::new(body),
                })
            }

            _ => Err(Error::unsupported(format!(
                "Unsupported expression: {:?}",
                sql_expr
            ))),
        }
    }

    fn resolve_column(name: &str, table: Option<&str>, schema: &PlanSchema) -> Result<Expr> {
        let index = schema.field_index_qualified(name, table);

        if index.is_none() && table.is_none() && Self::is_date_part_keyword(name) {
            return Ok(Expr::Literal(Literal::String(name.to_uppercase())));
        }

        if index.is_none() && table.is_none() {
            let struct_fields: Vec<_> = schema
                .fields
                .iter()
                .enumerate()
                .filter(|(_, f)| {
                    f.table
                        .as_ref()
                        .is_some_and(|t| t.eq_ignore_ascii_case(name))
                })
                .collect();
            if !struct_fields.is_empty() {
                let field_exprs: Vec<(Option<String>, Expr)> = struct_fields
                    .iter()
                    .map(|(i, f)| {
                        (
                            Some(f.name.clone()),
                            Expr::Column {
                                table: Some(name.to_string()),
                                name: f.name.clone(),
                                index: Some(*i),
                            },
                        )
                    })
                    .collect();
                return Ok(Expr::Struct {
                    fields: field_exprs,
                });
            }
        }

        Ok(Expr::Column {
            table: table.map(String::from),
            name: name.to_string(),
            index,
        })
    }

    fn resolve_compound_identifier(parts: &[ast::Ident], schema: &PlanSchema) -> Result<Expr> {
        if parts.is_empty() {
            return Err(Error::InvalidQuery("Empty compound identifier".into()));
        }

        if parts.len() == 1 {
            let name = &parts[0].value;
            return Self::resolve_column(name, None, schema);
        }

        let first_part = &parts[0].value;
        if let Some((idx, field)) = schema.field_by_name(first_part) {
            match &field.data_type {
                DataType::Struct(_) => {
                    let mut expr = Expr::Column {
                        table: None,
                        name: first_part.clone(),
                        index: Some(idx),
                    };
                    for part in &parts[1..] {
                        expr = Expr::StructAccess {
                            expr: Box::new(expr),
                            field: part.value.clone(),
                        };
                    }
                    return Ok(expr);
                }
                DataType::Json => {
                    let base = Expr::Column {
                        table: None,
                        name: first_part.clone(),
                        index: Some(idx),
                    };
                    let path: Vec<JsonPathElement> = parts[1..]
                        .iter()
                        .map(|p| JsonPathElement::Key(p.value.clone()))
                        .collect();
                    return Ok(Expr::JsonAccess {
                        expr: Box::new(base),
                        path,
                    });
                }
                _ => {}
            }
        }

        let (table, name) = (
            Some(
                parts[..parts.len() - 1]
                    .iter()
                    .map(|p| p.value.clone())
                    .collect::<Vec<_>>()
                    .join("."),
            ),
            parts.last().map(|p| p.value.clone()).unwrap_or_default(),
        );
        Self::resolve_column(&name, table.as_deref(), schema)
    }

    fn plan_all_any_op(
        left: &ast::Expr,
        compare_op: &ast::BinaryOperator,
        right: &ast::Expr,
        is_all: bool,
        schema: &PlanSchema,
        subquery_planner: Option<SubqueryPlannerFn>,
    ) -> Result<Expr> {
        let left_expr = Self::plan_expr_with_subquery(left, schema, subquery_planner)?;

        let patterns = match right {
            ast::Expr::Tuple(exprs) => exprs
                .iter()
                .map(|e| Self::plan_expr_with_subquery(e, schema, subquery_planner))
                .collect::<Result<Vec<_>>>()?,
            ast::Expr::Array(arr) => arr
                .elem
                .iter()
                .map(|e| Self::plan_expr_with_subquery(e, schema, subquery_planner))
                .collect::<Result<Vec<_>>>()?,
            other => vec![Self::plan_expr_with_subquery(
                other,
                schema,
                subquery_planner,
            )?],
        };

        if patterns.is_empty() {
            return Ok(Expr::Literal(Literal::Bool(is_all)));
        }

        let (case_insensitive, is_like) = match compare_op {
            ast::BinaryOperator::PGLikeMatch => (false, true),
            ast::BinaryOperator::PGILikeMatch => (true, true),
            ast::BinaryOperator::PGNotLikeMatch => (false, true),
            ast::BinaryOperator::PGNotILikeMatch => (true, true),
            _ => (false, false),
        };

        let negated = matches!(
            compare_op,
            ast::BinaryOperator::PGNotLikeMatch | ast::BinaryOperator::PGNotILikeMatch
        );

        let comparisons: Vec<Expr> = patterns
            .into_iter()
            .map(|pattern| {
                if is_like {
                    Expr::Like {
                        expr: Box::new(left_expr.clone()),
                        pattern: Box::new(pattern),
                        negated,
                        case_insensitive,
                    }
                } else {
                    let op = Self::plan_binary_op(compare_op).unwrap_or(BinaryOp::Eq);
                    Expr::BinaryOp {
                        left: Box::new(left_expr.clone()),
                        op,
                        right: Box::new(pattern),
                    }
                }
            })
            .collect();

        let combine_op = if is_all { BinaryOp::And } else { BinaryOp::Or };

        let mut result = comparisons[0].clone();
        for comp in comparisons.into_iter().skip(1) {
            result = Expr::BinaryOp {
                left: Box::new(result),
                op: combine_op,
                right: Box::new(comp),
            };
        }

        Ok(result)
    }

    fn extract_all_any_patterns(pattern: &ast::Expr) -> Option<(bool, Vec<ast::Expr>)> {
        match pattern {
            ast::Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                let is_all = match name.as_str() {
                    "ALL" => true,
                    "ANY" | "SOME" => false,
                    _ => return None,
                };

                let patterns = match &func.args {
                    ast::FunctionArguments::List(arg_list) => arg_list
                        .args
                        .iter()
                        .filter_map(|arg| match arg {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                                Some(e.clone())
                            }
                            _ => None,
                        })
                        .collect(),
                    ast::FunctionArguments::None => vec![],
                    ast::FunctionArguments::Subquery(_) => return None,
                };

                Some((is_all, patterns))
            }
            ast::Expr::AllOp { right, .. } => match right.as_ref() {
                ast::Expr::Tuple(exprs) => Some((true, exprs.clone())),
                other => Some((true, vec![other.clone()])),
            },
            ast::Expr::AnyOp { right, .. } => match right.as_ref() {
                ast::Expr::Tuple(exprs) => Some((false, exprs.clone())),
                other => Some((false, vec![other.clone()])),
            },
            _ => None,
        }
    }

    fn plan_like_all_any_from_patterns(
        expr: &ast::Expr,
        pattern_exprs: &[ast::Expr],
        negated: bool,
        is_all: bool,
        case_insensitive: bool,
        schema: &PlanSchema,
        subquery_planner: Option<SubqueryPlannerFn>,
    ) -> Result<Expr> {
        let left_expr = Self::plan_expr_with_subquery(expr, schema, subquery_planner)?;

        if pattern_exprs.is_empty() {
            return Ok(Expr::Literal(Literal::Bool(is_all)));
        }

        let patterns: Vec<Expr> = pattern_exprs
            .iter()
            .map(|e| Self::plan_expr_with_subquery(e, schema, subquery_planner))
            .collect::<Result<Vec<_>>>()?;

        let comparisons: Vec<Expr> = patterns
            .into_iter()
            .map(|pattern| Expr::Like {
                expr: Box::new(left_expr.clone()),
                pattern: Box::new(pattern),
                negated,
                case_insensitive,
            })
            .collect();

        let combine_op = if is_all { BinaryOp::And } else { BinaryOp::Or };

        let mut result = comparisons[0].clone();
        for comp in comparisons.into_iter().skip(1) {
            result = Expr::BinaryOp {
                left: Box::new(result),
                op: combine_op,
                right: Box::new(comp),
            };
        }

        Ok(result)
    }

    fn is_date_part_keyword(name: &str) -> bool {
        matches!(
            name.to_uppercase().as_str(),
            "YEAR"
                | "ISOYEAR"
                | "QUARTER"
                | "MONTH"
                | "WEEK"
                | "ISOWEEK"
                | "DAY"
                | "DAYOFWEEK"
                | "DAYOFYEAR"
                | "HOUR"
                | "MINUTE"
                | "SECOND"
                | "MILLISECOND"
                | "MICROSECOND"
                | "NANOSECOND"
                | "DATE"
                | "TIME"
                | "DATETIME"
        )
    }

    fn plan_datetime_field(field: &ast::DateTimeField) -> Result<DateTimeField> {
        match field {
            ast::DateTimeField::Year => Ok(DateTimeField::Year),
            ast::DateTimeField::Month => Ok(DateTimeField::Month),
            ast::DateTimeField::Week(opt_ident) => {
                let start_day = match opt_ident {
                    Some(ident) => match ident.value.to_uppercase().as_str() {
                        "SUNDAY" => WeekStartDay::Sunday,
                        "MONDAY" => WeekStartDay::Monday,
                        "TUESDAY" => WeekStartDay::Tuesday,
                        "WEDNESDAY" => WeekStartDay::Wednesday,
                        "THURSDAY" => WeekStartDay::Thursday,
                        "FRIDAY" => WeekStartDay::Friday,
                        "SATURDAY" => WeekStartDay::Saturday,
                        _ => {
                            return Err(Error::unsupported(format!(
                                "Unsupported WEEK start day: {}",
                                ident.value
                            )));
                        }
                    },
                    None => WeekStartDay::Sunday,
                };
                Ok(DateTimeField::Week(start_day))
            }
            ast::DateTimeField::Day => Ok(DateTimeField::Day),
            ast::DateTimeField::DayOfWeek => Ok(DateTimeField::DayOfWeek),
            ast::DateTimeField::DayOfYear => Ok(DateTimeField::DayOfYear),
            ast::DateTimeField::Hour => Ok(DateTimeField::Hour),
            ast::DateTimeField::Minute => Ok(DateTimeField::Minute),
            ast::DateTimeField::Second => Ok(DateTimeField::Second),
            ast::DateTimeField::Millisecond | ast::DateTimeField::Milliseconds => {
                Ok(DateTimeField::Millisecond)
            }
            ast::DateTimeField::Microsecond | ast::DateTimeField::Microseconds => {
                Ok(DateTimeField::Microsecond)
            }
            ast::DateTimeField::Nanosecond | ast::DateTimeField::Nanoseconds => {
                Ok(DateTimeField::Nanosecond)
            }
            ast::DateTimeField::Date => Ok(DateTimeField::Date),
            ast::DateTimeField::Time => Ok(DateTimeField::Time),
            ast::DateTimeField::Datetime => Ok(DateTimeField::Datetime),
            ast::DateTimeField::Quarter => Ok(DateTimeField::Quarter),
            ast::DateTimeField::Isoyear => Ok(DateTimeField::IsoYear),
            ast::DateTimeField::IsoWeek => Ok(DateTimeField::IsoWeek),
            ast::DateTimeField::Timezone => Ok(DateTimeField::Timezone),
            ast::DateTimeField::TimezoneHour => Ok(DateTimeField::TimezoneHour),
            ast::DateTimeField::TimezoneMinute => Ok(DateTimeField::TimezoneMinute),
            _ => Err(Error::unsupported(format!(
                "Unsupported datetime field: {:?}",
                field
            ))),
        }
    }

    fn plan_literal(val: &ast::Value) -> Result<Literal> {
        match val {
            ast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Literal::Int64(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Literal::Float64(ordered_float::OrderedFloat(f)))
                } else if let Ok(d) = n.parse::<Decimal>() {
                    Ok(Literal::Numeric(d))
                } else {
                    Err(Error::parse_error(format!("Invalid number: {}", n)))
                }
            }
            ast::Value::SingleQuotedString(s)
            | ast::Value::DoubleQuotedString(s)
            | ast::Value::TripleSingleQuotedString(s)
            | ast::Value::TripleDoubleQuotedString(s) => Ok(Literal::String(unescape_unicode(s))),
            ast::Value::SingleQuotedRawStringLiteral(s)
            | ast::Value::DoubleQuotedRawStringLiteral(s)
            | ast::Value::TripleSingleQuotedRawStringLiteral(s)
            | ast::Value::TripleDoubleQuotedRawStringLiteral(s) => Ok(Literal::String(s.clone())),
            ast::Value::Boolean(b) => Ok(Literal::Bool(*b)),
            ast::Value::Null => Ok(Literal::Null),
            ast::Value::HexStringLiteral(s) => {
                let bytes = hex::decode(s)
                    .map_err(|e| Error::parse_error(format!("Invalid hex string: {}", e)))?;
                Ok(Literal::Bytes(bytes))
            }
            ast::Value::SingleQuotedByteStringLiteral(s)
            | ast::Value::DoubleQuotedByteStringLiteral(s) => {
                Ok(Literal::Bytes(parse_byte_string_escapes(s)))
            }
            _ => Err(Error::unsupported(format!(
                "Unsupported literal: {:?}",
                val
            ))),
        }
    }

    pub fn plan_binary_op(op: &ast::BinaryOperator) -> Result<BinaryOp> {
        match op {
            ast::BinaryOperator::Plus => Ok(BinaryOp::Add),
            ast::BinaryOperator::Minus => Ok(BinaryOp::Sub),
            ast::BinaryOperator::Multiply => Ok(BinaryOp::Mul),
            ast::BinaryOperator::Divide => Ok(BinaryOp::Div),
            ast::BinaryOperator::Modulo => Ok(BinaryOp::Mod),
            ast::BinaryOperator::Eq => Ok(BinaryOp::Eq),
            ast::BinaryOperator::NotEq => Ok(BinaryOp::NotEq),
            ast::BinaryOperator::Lt => Ok(BinaryOp::Lt),
            ast::BinaryOperator::LtEq => Ok(BinaryOp::LtEq),
            ast::BinaryOperator::Gt => Ok(BinaryOp::Gt),
            ast::BinaryOperator::GtEq => Ok(BinaryOp::GtEq),
            ast::BinaryOperator::And => Ok(BinaryOp::And),
            ast::BinaryOperator::Or => Ok(BinaryOp::Or),
            ast::BinaryOperator::StringConcat => Ok(BinaryOp::Concat),
            ast::BinaryOperator::BitwiseAnd => Ok(BinaryOp::BitwiseAnd),
            ast::BinaryOperator::BitwiseOr => Ok(BinaryOp::BitwiseOr),
            ast::BinaryOperator::BitwiseXor => Ok(BinaryOp::BitwiseXor),
            ast::BinaryOperator::PGBitwiseShiftLeft => Ok(BinaryOp::ShiftLeft),
            ast::BinaryOperator::PGBitwiseShiftRight => Ok(BinaryOp::ShiftRight),
            _ => Err(Error::unsupported(format!(
                "Unsupported binary operator: {:?}",
                op
            ))),
        }
    }

    pub fn plan_unary_op(op: &ast::UnaryOperator) -> Result<UnaryOp> {
        match op {
            ast::UnaryOperator::Not => Ok(UnaryOp::Not),
            ast::UnaryOperator::Minus => Ok(UnaryOp::Minus),
            ast::UnaryOperator::Plus => Ok(UnaryOp::Plus),
            ast::UnaryOperator::PGBitwiseNot => Ok(UnaryOp::BitwiseNot),
            _ => Err(Error::unsupported(format!(
                "Unsupported unary operator: {:?}",
                op
            ))),
        }
    }

    fn plan_function(
        func: &ast::Function,
        schema: &PlanSchema,
        subquery_planner: Option<SubqueryPlannerFn>,
        named_windows: &[ast::NamedWindowDefinition],
        udf_resolver: Option<UdfResolverFn>,
    ) -> Result<Expr> {
        let name = func.name.to_string().to_uppercase();

        if name == "ARRAY"
            && let ast::FunctionArguments::Subquery(subquery) = &func.args
        {
            let planner = subquery_planner.ok_or_else(|| {
                Error::unsupported("ARRAY subquery requires subquery planner context")
            })?;
            let plan = planner(subquery)?;
            return Ok(Expr::ArraySubquery(Box::new(plan)));
        }

        if let Some(over) = &func.over {
            if let Some(window_func) = Self::try_window_function(&name) {
                let args = Self::extract_function_args(func, schema)?;
                let (partition_by, order_by, frame) =
                    Self::plan_window_spec(over, schema, named_windows)?;
                return Ok(Expr::Window {
                    func: window_func,
                    args,
                    partition_by,
                    order_by,
                    frame,
                });
            }
            if let Some(agg_func) = Self::try_aggregate_function(&name) {
                let distinct = matches!(
                    &func.args,
                    ast::FunctionArguments::List(list) if list.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct)
                );
                let args = Self::extract_function_args(func, schema)?;
                let (partition_by, order_by, frame) =
                    Self::plan_window_spec(over, schema, named_windows)?;
                return Ok(Expr::AggregateWindow {
                    func: agg_func,
                    args,
                    distinct,
                    partition_by,
                    order_by,
                    frame,
                });
            }
        }

        if let Some(agg_func) = Self::try_aggregate_function(&name) {
            let (distinct, order_by, limit, ignore_nulls) = match &func.args {
                ast::FunctionArguments::List(list) => {
                    let distinct =
                        list.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct);
                    let mut order_by_exprs = Vec::new();
                    let mut limit = None;
                    let mut ignore_nulls = false;
                    for clause in &list.clauses {
                        match clause {
                            ast::FunctionArgumentClause::OrderBy(order_list) => {
                                for ob in order_list {
                                    let expr = Self::plan_expr(&ob.expr, schema)?;
                                    let asc = ob.options.asc.is_none_or(|asc| asc);
                                    order_by_exprs.push(SortExpr {
                                        expr,
                                        asc,
                                        nulls_first: ob.options.nulls_first.unwrap_or(!asc),
                                    });
                                }
                            }
                            ast::FunctionArgumentClause::Limit(ast::Expr::Value(
                                ast::ValueWithSpan {
                                    value: ast::Value::Number(n, _),
                                    ..
                                },
                            )) => {
                                limit = n.parse::<usize>().ok();
                            }
                            ast::FunctionArgumentClause::Limit(_) => {}
                            ast::FunctionArgumentClause::IgnoreOrRespectNulls(treatment) => {
                                ignore_nulls = matches!(treatment, ast::NullTreatment::IgnoreNulls);
                            }
                            _ => {}
                        }
                    }
                    (distinct, order_by_exprs, limit, ignore_nulls)
                }
                _ => (false, Vec::new(), None, false),
            };
            let args = Self::extract_function_args(func, schema)?;
            return Ok(Expr::Aggregate {
                func: agg_func,
                args,
                distinct,
                filter: None,
                order_by,
                limit,
                ignore_nulls,
            });
        }

        if name == "MAKE_INTERVAL" {
            let args = Self::extract_make_interval_args(func, schema)?;
            return Ok(Expr::ScalarFunction {
                name: ScalarFunction::MakeInterval,
                args,
            });
        }

        if matches!(
            name.as_str(),
            "TIMESTAMP_TRUNC" | "DATETIME_TRUNC" | "DATE_TRUNC" | "TIME_TRUNC"
        ) {
            let args = Self::extract_trunc_args(func, schema)?;
            let scalar_func = Self::try_scalar_function(&name)?;
            return Ok(Expr::ScalarFunction {
                name: scalar_func,
                args,
            });
        }

        if name == "NORMALIZE" {
            let args = Self::extract_normalize_args(func, schema)?;
            return Ok(Expr::ScalarFunction {
                name: ScalarFunction::Normalize,
                args,
            });
        }

        if let Some(resolver) = udf_resolver
            && let Some(udf) = resolver(&name)
            && matches!(&udf.body, FunctionBody::Sql(_))
        {
            let call_args = Self::extract_function_args_full(
                func,
                schema,
                subquery_planner,
                named_windows,
                udf_resolver,
            )?;
            if let FunctionBody::Sql(body_expr) = &udf.body {
                let substituted =
                    Self::substitute_parameters(body_expr, &udf.parameters, &call_args);
                let result = Self::apply_struct_field_names(substituted, &udf.return_type);
                return Ok(result);
            }
        }

        let args = Self::extract_function_args(func, schema)?;
        let scalar_func = Self::try_scalar_function(&name)?;
        Ok(Expr::ScalarFunction {
            name: scalar_func,
            args,
        })
    }

    fn extract_trunc_args(func: &ast::Function, schema: &PlanSchema) -> Result<Vec<Expr>> {
        match &func.args {
            ast::FunctionArguments::None => Ok(vec![]),
            ast::FunctionArguments::Subquery(_) => {
                Err(Error::unsupported("Subquery function arguments"))
            }
            ast::FunctionArguments::List(list) => {
                let mut args = Vec::new();
                for (i, arg) in list.args.iter().enumerate() {
                    match arg {
                        ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                            ast::FunctionArgExpr::Expr(e) => {
                                if i == 1 {
                                    let part_str = Self::extract_date_part_string(e)?;
                                    args.push(Expr::Literal(Literal::String(part_str)));
                                } else {
                                    args.push(Self::plan_expr(e, schema)?);
                                }
                            }
                            ast::FunctionArgExpr::Wildcard => {
                                args.push(Expr::Wildcard { table: None });
                            }
                            ast::FunctionArgExpr::QualifiedWildcard(name) => {
                                args.push(Expr::Wildcard {
                                    table: Some(name.to_string()),
                                });
                            }
                            ast::FunctionArgExpr::TableRef(_) => {
                                return Err(Error::unsupported(
                                    "TABLE argument in scalar function",
                                ));
                            }
                        },
                        ast::FunctionArg::Named { arg, .. } => {
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                args.push(Self::plan_expr(e, schema)?);
                            }
                        }
                        ast::FunctionArg::ExprNamed { arg, .. } => {
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                args.push(Self::plan_expr(e, schema)?);
                            }
                        }
                    }
                }
                Ok(args)
            }
        }
    }

    fn extract_normalize_args(func: &ast::Function, schema: &PlanSchema) -> Result<Vec<Expr>> {
        match &func.args {
            ast::FunctionArguments::None => Ok(vec![]),
            ast::FunctionArguments::Subquery(_) => {
                Err(Error::unsupported("Subquery function arguments"))
            }
            ast::FunctionArguments::List(list) => {
                let mut args = Vec::new();
                for (i, arg) in list.args.iter().enumerate() {
                    match arg {
                        ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                            ast::FunctionArgExpr::Expr(e) => {
                                if i == 1 {
                                    let mode = Self::extract_normalize_mode(e)?;
                                    args.push(Expr::Literal(Literal::String(mode)));
                                } else {
                                    args.push(Self::plan_expr(e, schema)?);
                                }
                            }
                            ast::FunctionArgExpr::Wildcard => {
                                args.push(Expr::Wildcard { table: None });
                            }
                            ast::FunctionArgExpr::QualifiedWildcard(name) => {
                                args.push(Expr::Wildcard {
                                    table: Some(name.to_string()),
                                });
                            }
                            ast::FunctionArgExpr::TableRef(_) => {
                                return Err(Error::unsupported(
                                    "TABLE argument in scalar function",
                                ));
                            }
                        },
                        ast::FunctionArg::Named { arg, .. } => {
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                args.push(Self::plan_expr(e, schema)?);
                            }
                        }
                        ast::FunctionArg::ExprNamed { arg, .. } => {
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                args.push(Self::plan_expr(e, schema)?);
                            }
                        }
                    }
                }
                Ok(args)
            }
        }
    }

    fn extract_normalize_mode(e: &ast::Expr) -> Result<String> {
        match e {
            ast::Expr::Identifier(ident) => {
                let mode = ident.value.to_uppercase();
                match mode.as_str() {
                    "NFC" | "NFKC" | "NFD" | "NFKD" => Ok(mode),
                    _ => Err(Error::invalid_query(format!(
                        "Invalid normalization mode: {}. Expected NFC, NFKC, NFD, or NFKD",
                        mode
                    ))),
                }
            }
            _ => Err(Error::invalid_query(format!(
                "Normalization mode must be an identifier (NFC, NFKC, NFD, or NFKD), got: {}",
                e
            ))),
        }
    }

    fn extract_date_part_string(e: &ast::Expr) -> Result<String> {
        match e {
            ast::Expr::Identifier(ident) => Ok(ident.value.to_uppercase()),
            ast::Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                if let ast::FunctionArguments::List(list) = &func.args
                    && let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                        ast::Expr::Identifier(day_ident),
                    ))) = list.args.first()
                {
                    return Ok(format!("{}_{}", name, day_ident.value.to_uppercase()));
                }
                Ok(name)
            }
            _ => Ok(e.to_string().to_uppercase()),
        }
    }

    fn extract_make_interval_args(func: &ast::Function, schema: &PlanSchema) -> Result<Vec<Expr>> {
        let mut years: Option<Expr> = None;
        let mut months: Option<Expr> = None;
        let mut days: Option<Expr> = None;
        let mut hours: Option<Expr> = None;
        let mut minutes: Option<Expr> = None;
        let mut seconds: Option<Expr> = None;

        match &func.args {
            ast::FunctionArguments::List(list) => {
                for (i, arg) in list.args.iter().enumerate() {
                    match arg {
                        ast::FunctionArg::Named { name, arg, .. } => {
                            let param_name = name.value.to_uppercase();
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                let expr = Self::plan_expr(e, schema)?;
                                match param_name.as_str() {
                                    "YEAR" | "YEARS" => years = Some(expr),
                                    "MONTH" | "MONTHS" => months = Some(expr),
                                    "DAY" | "DAYS" => days = Some(expr),
                                    "HOUR" | "HOURS" => hours = Some(expr),
                                    "MINUTE" | "MINUTES" => minutes = Some(expr),
                                    "SECOND" | "SECONDS" => seconds = Some(expr),
                                    _ => {}
                                }
                            }
                        }
                        ast::FunctionArg::ExprNamed { name, arg, .. } => {
                            let param_name = match name {
                                ast::Expr::Identifier(ident) => ident.value.to_uppercase(),
                                _ => continue,
                            };
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                let expr = Self::plan_expr(e, schema)?;
                                match param_name.as_str() {
                                    "YEAR" | "YEARS" => years = Some(expr),
                                    "MONTH" | "MONTHS" => months = Some(expr),
                                    "DAY" | "DAYS" => days = Some(expr),
                                    "HOUR" | "HOURS" => hours = Some(expr),
                                    "MINUTE" | "MINUTES" => minutes = Some(expr),
                                    "SECOND" | "SECONDS" => seconds = Some(expr),
                                    _ => {}
                                }
                            }
                        }
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                            let expr = Self::plan_expr(e, schema)?;
                            match i {
                                0 => years = Some(expr),
                                1 => months = Some(expr),
                                2 => days = Some(expr),
                                3 => hours = Some(expr),
                                4 => minutes = Some(expr),
                                5 => seconds = Some(expr),
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                }
            }
            ast::FunctionArguments::None => {}
            ast::FunctionArguments::Subquery(_) => {
                return Err(Error::unsupported("Subquery function arguments"));
            }
        }

        let zero = Expr::Literal(Literal::Int64(0));
        Ok(vec![
            years.unwrap_or_else(|| zero.clone()),
            months.unwrap_or_else(|| zero.clone()),
            days.unwrap_or_else(|| zero.clone()),
            hours.unwrap_or_else(|| zero.clone()),
            minutes.unwrap_or_else(|| zero.clone()),
            seconds.unwrap_or(zero),
        ])
    }

    fn try_window_function(name: &str) -> Option<WindowFunction> {
        match name {
            "ROW_NUMBER" => Some(WindowFunction::RowNumber),
            "RANK" => Some(WindowFunction::Rank),
            "DENSE_RANK" => Some(WindowFunction::DenseRank),
            "PERCENT_RANK" => Some(WindowFunction::PercentRank),
            "CUME_DIST" => Some(WindowFunction::CumeDist),
            "NTILE" => Some(WindowFunction::Ntile),
            "LEAD" => Some(WindowFunction::Lead),
            "LAG" => Some(WindowFunction::Lag),
            "FIRST_VALUE" => Some(WindowFunction::FirstValue),
            "LAST_VALUE" => Some(WindowFunction::LastValue),
            "NTH_VALUE" => Some(WindowFunction::NthValue),
            _ => None,
        }
    }

    fn plan_window_spec(
        over: &ast::WindowType,
        schema: &PlanSchema,
        named_windows: &[ast::NamedWindowDefinition],
    ) -> Result<(Vec<Expr>, Vec<SortExpr>, Option<WindowFrame>)> {
        match over {
            ast::WindowType::WindowSpec(spec) => Self::plan_window_spec_inner(spec, schema),
            ast::WindowType::NamedWindow(name) => {
                let name_str = name.value.to_uppercase();
                for def in named_windows {
                    if def.0.value.to_uppercase() == name_str {
                        return match &def.1 {
                            ast::NamedWindowExpr::WindowSpec(spec) => {
                                Self::plan_window_spec_inner(spec, schema)
                            }
                            ast::NamedWindowExpr::NamedWindow(ref_name) => Self::plan_window_spec(
                                &ast::WindowType::NamedWindow(ref_name.clone()),
                                schema,
                                named_windows,
                            ),
                        };
                    }
                }
                Err(Error::invalid_query(format!(
                    "Named window '{}' not found",
                    name.value
                )))
            }
        }
    }

    fn plan_window_spec_inner(
        spec: &ast::WindowSpec,
        schema: &PlanSchema,
    ) -> Result<(Vec<Expr>, Vec<SortExpr>, Option<WindowFrame>)> {
        let partition_by = spec
            .partition_by
            .iter()
            .map(|e| Self::plan_expr(e, schema))
            .collect::<Result<Vec<_>>>()?;

        let order_by = spec
            .order_by
            .iter()
            .map(|ob| {
                let expr = Self::plan_expr(&ob.expr, schema)?;
                Ok(SortExpr {
                    expr,
                    asc: ob.options.asc.unwrap_or(true),
                    nulls_first: ob.options.nulls_first.unwrap_or(false),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let frame = spec.window_frame.as_ref().map(|f| WindowFrame {
            unit: match f.units {
                ast::WindowFrameUnits::Rows => WindowFrameUnit::Rows,
                ast::WindowFrameUnits::Range => WindowFrameUnit::Range,
                ast::WindowFrameUnits::Groups => WindowFrameUnit::Groups,
            },
            start: Self::plan_window_bound(&f.start_bound),
            end: f.end_bound.as_ref().map(Self::plan_window_bound),
        });

        Ok((partition_by, order_by, frame))
    }

    fn plan_window_bound(bound: &ast::WindowFrameBound) -> WindowFrameBound {
        match bound {
            ast::WindowFrameBound::CurrentRow => WindowFrameBound::CurrentRow,
            ast::WindowFrameBound::Preceding(None) => WindowFrameBound::Preceding(None),
            ast::WindowFrameBound::Preceding(Some(e)) => {
                if let ast::Expr::Value(v) = e.as_ref()
                    && let ast::Value::Number(n, _) = &v.value
                    && let Ok(num) = n.parse::<u64>()
                {
                    return WindowFrameBound::Preceding(Some(num));
                }
                WindowFrameBound::Preceding(None)
            }
            ast::WindowFrameBound::Following(None) => WindowFrameBound::Following(None),
            ast::WindowFrameBound::Following(Some(e)) => {
                if let ast::Expr::Value(v) = e.as_ref()
                    && let ast::Value::Number(n, _) = &v.value
                    && let Ok(num) = n.parse::<u64>()
                {
                    return WindowFrameBound::Following(Some(num));
                }
                WindowFrameBound::Following(None)
            }
        }
    }

    fn try_aggregate_function(name: &str) -> Option<AggregateFunction> {
        match name {
            "COUNT" => Some(AggregateFunction::Count),
            "SUM" => Some(AggregateFunction::Sum),
            "AVG" => Some(AggregateFunction::Avg),
            "MIN" => Some(AggregateFunction::Min),
            "MAX" => Some(AggregateFunction::Max),
            "ARRAY_AGG" => Some(AggregateFunction::ArrayAgg),
            "STRING_AGG" | "LISTAGG" => Some(AggregateFunction::StringAgg),
            "XMLAGG" => Some(AggregateFunction::XmlAgg),
            "ANY_VALUE" => Some(AggregateFunction::AnyValue),
            "COUNTIF" | "COUNT_IF" => Some(AggregateFunction::CountIf),
            "SUMIF" | "SUM_IF" => Some(AggregateFunction::SumIf),
            "AVGIF" | "AVG_IF" => Some(AggregateFunction::AvgIf),
            "MINIF" | "MIN_IF" => Some(AggregateFunction::MinIf),
            "MAXIF" | "MAX_IF" => Some(AggregateFunction::MaxIf),
            "GROUPING" => Some(AggregateFunction::Grouping),
            "GROUPING_ID" => Some(AggregateFunction::GroupingId),
            "LOGICAL_AND" | "BOOL_AND" => Some(AggregateFunction::LogicalAnd),
            "LOGICAL_OR" | "BOOL_OR" => Some(AggregateFunction::LogicalOr),
            "BIT_AND" => Some(AggregateFunction::BitAnd),
            "BIT_OR" => Some(AggregateFunction::BitOr),
            "BIT_XOR" => Some(AggregateFunction::BitXor),
            "APPROX_COUNT_DISTINCT" => Some(AggregateFunction::ApproxCountDistinct),
            "APPROX_QUANTILES" => Some(AggregateFunction::ApproxQuantiles),
            "APPROX_TOP_COUNT" => Some(AggregateFunction::ApproxTopCount),
            "APPROX_TOP_SUM" => Some(AggregateFunction::ApproxTopSum),
            "CORR" => Some(AggregateFunction::Corr),
            "COVAR_POP" => Some(AggregateFunction::CovarPop),
            "COVAR_SAMP" => Some(AggregateFunction::CovarSamp),
            "STDDEV" => Some(AggregateFunction::Stddev),
            "STDDEV_POP" => Some(AggregateFunction::StddevPop),
            "STDDEV_SAMP" => Some(AggregateFunction::StddevSamp),
            "VARIANCE" | "VAR" => Some(AggregateFunction::Variance),
            "VAR_POP" => Some(AggregateFunction::VarPop),
            "VAR_SAMP" => Some(AggregateFunction::VarSamp),
            _ => None,
        }
    }

    fn try_scalar_function(name: &str) -> Result<ScalarFunction> {
        match name {
            "UPPER" => Ok(ScalarFunction::Upper),
            "LOWER" => Ok(ScalarFunction::Lower),
            "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => Ok(ScalarFunction::Length),
            "TRIM" => Ok(ScalarFunction::Trim),
            "LTRIM" => Ok(ScalarFunction::LTrim),
            "RTRIM" => Ok(ScalarFunction::RTrim),
            "SUBSTR" | "SUBSTRING" => Ok(ScalarFunction::Substr),
            "CONCAT" => Ok(ScalarFunction::Concat),
            "REPLACE" => Ok(ScalarFunction::Replace),
            "REVERSE" => Ok(ScalarFunction::Reverse),
            "LEFT" => Ok(ScalarFunction::Left),
            "RIGHT" => Ok(ScalarFunction::Right),
            "REPEAT" => Ok(ScalarFunction::Repeat),
            "STARTS_WITH" => Ok(ScalarFunction::StartsWith),
            "ENDS_WITH" => Ok(ScalarFunction::EndsWith),
            "CONTAINS" => Ok(ScalarFunction::Contains),
            "STRPOS" => Ok(ScalarFunction::Strpos),
            "INSTR" => Ok(ScalarFunction::Instr),
            "SPLIT" => Ok(ScalarFunction::Split),
            "FORMAT" => Ok(ScalarFunction::Format),
            "BYTE_LENGTH" => Ok(ScalarFunction::ByteLength),
            "INITCAP" => Ok(ScalarFunction::Initcap),
            "LPAD" => Ok(ScalarFunction::Lpad),
            "RPAD" => Ok(ScalarFunction::Rpad),
            "TRANSLATE" => Ok(ScalarFunction::Translate),
            "SOUNDEX" => Ok(ScalarFunction::Soundex),
            "NORMALIZE" => Ok(ScalarFunction::Normalize),
            "ASCII" => Ok(ScalarFunction::Ascii),
            "CHR" => Ok(ScalarFunction::Chr),
            "UNICODE" => Ok(ScalarFunction::Unicode),
            "OCTET_LENGTH" => Ok(ScalarFunction::ByteLength),
            "TO_CODE_POINTS" => Ok(ScalarFunction::ToCodePoints),
            "CODE_POINTS_TO_STRING" => Ok(ScalarFunction::CodePointsToString),
            "CODE_POINTS_TO_BYTES" => Ok(ScalarFunction::CodePointsToBytes),
            "NORMALIZE_AND_CASEFOLD" => Ok(ScalarFunction::NormalizeAndCasefold),
            "EDIT_DISTANCE" => Ok(ScalarFunction::EditDistance),
            "CONTAINS_SUBSTR" => Ok(ScalarFunction::ContainsSubstr),
            "TO_BASE64" => Ok(ScalarFunction::ToBase64),
            "FROM_BASE64" => Ok(ScalarFunction::FromBase64),
            "TO_BASE32" => Ok(ScalarFunction::ToBase32),
            "FROM_BASE32" => Ok(ScalarFunction::FromBase32),
            "TO_HEX" => Ok(ScalarFunction::ToHex),
            "FROM_HEX" => Ok(ScalarFunction::FromHex),
            "REGEXP_CONTAINS" => Ok(ScalarFunction::RegexpContains),
            "REGEXP_EXTRACT" => Ok(ScalarFunction::RegexpExtract),
            "REGEXP_EXTRACT_ALL" => Ok(ScalarFunction::RegexpExtractAll),
            "REGEXP_REPLACE" => Ok(ScalarFunction::RegexpReplace),
            "ABS" => Ok(ScalarFunction::Abs),
            "ROUND" => Ok(ScalarFunction::Round),
            "FLOOR" => Ok(ScalarFunction::Floor),
            "CEIL" | "CEILING" => Ok(ScalarFunction::Ceil),
            "SQRT" => Ok(ScalarFunction::Sqrt),
            "CBRT" => Ok(ScalarFunction::Cbrt),
            "POWER" | "POW" => Ok(ScalarFunction::Power),
            "MOD" => Ok(ScalarFunction::Mod),
            "SIGN" => Ok(ScalarFunction::Sign),
            "EXP" => Ok(ScalarFunction::Exp),
            "LN" => Ok(ScalarFunction::Ln),
            "LOG" => Ok(ScalarFunction::Log),
            "LOG10" => Ok(ScalarFunction::Log10),
            "GREATEST" => Ok(ScalarFunction::Greatest),
            "LEAST" => Ok(ScalarFunction::Least),
            "DIV" => Ok(ScalarFunction::Div),
            "TRUNC" | "TRUNCATE" => Ok(ScalarFunction::Trunc),
            "SIN" => Ok(ScalarFunction::Sin),
            "COS" => Ok(ScalarFunction::Cos),
            "TAN" => Ok(ScalarFunction::Tan),
            "ASIN" => Ok(ScalarFunction::Asin),
            "ACOS" => Ok(ScalarFunction::Acos),
            "ATAN" => Ok(ScalarFunction::Atan),
            "ATAN2" => Ok(ScalarFunction::Atan2),
            "SINH" => Ok(ScalarFunction::Sinh),
            "COSH" => Ok(ScalarFunction::Cosh),
            "TANH" => Ok(ScalarFunction::Tanh),
            "ASINH" => Ok(ScalarFunction::Asinh),
            "ACOSH" => Ok(ScalarFunction::Acosh),
            "ATANH" => Ok(ScalarFunction::Atanh),
            "COT" => Ok(ScalarFunction::Cot),
            "CSC" => Ok(ScalarFunction::Csc),
            "SEC" => Ok(ScalarFunction::Sec),
            "COTH" => Ok(ScalarFunction::Coth),
            "CSCH" => Ok(ScalarFunction::Csch),
            "SECH" => Ok(ScalarFunction::Sech),
            "SAFE_DIVIDE" => Ok(ScalarFunction::SafeDivide),
            "SAFE_MULTIPLY" => Ok(ScalarFunction::SafeMultiply),
            "SAFE_ADD" => Ok(ScalarFunction::SafeAdd),
            "SAFE_SUBTRACT" => Ok(ScalarFunction::SafeSubtract),
            "SAFE_NEGATE" => Ok(ScalarFunction::SafeNegate),
            "IEEE_DIVIDE" => Ok(ScalarFunction::IeeeDivide),
            "IS_NAN" => Ok(ScalarFunction::IsNan),
            "IS_INF" => Ok(ScalarFunction::IsInf),
            "COSINE_DISTANCE" => Ok(ScalarFunction::CosineDistance),
            "EUCLIDEAN_DISTANCE" => Ok(ScalarFunction::EuclideanDistance),
            "RAND" => Ok(ScalarFunction::Rand),
            "PI" => Ok(ScalarFunction::Pi),
            "COALESCE" => Ok(ScalarFunction::Coalesce),
            "IFNULL" => Ok(ScalarFunction::IfNull),
            "NULLIF" => Ok(ScalarFunction::NullIf),
            "IF" => Ok(ScalarFunction::If),
            "ZEROIFNULL" => Ok(ScalarFunction::Zeroifnull),
            "NVL" => Ok(ScalarFunction::Nvl),
            "NVL2" => Ok(ScalarFunction::Nvl2),
            "CURRENT_DATE" => Ok(ScalarFunction::CurrentDate),
            "CURRENT_TIMESTAMP" | "NOW" => Ok(ScalarFunction::CurrentTimestamp),
            "CURRENT_TIME" => Ok(ScalarFunction::CurrentTime),
            "CURRENT_DATETIME" => Ok(ScalarFunction::CurrentDatetime),
            "EXTRACT" => Ok(ScalarFunction::Extract),
            "DATE_ADD" => Ok(ScalarFunction::DateAdd),
            "DATE_SUB" => Ok(ScalarFunction::DateSub),
            "DATE_DIFF" | "DATEDIFF" => Ok(ScalarFunction::DateDiff),
            "DATE_TRUNC" => Ok(ScalarFunction::DateTrunc),
            "DATE_BUCKET" => Ok(ScalarFunction::DateBucket),
            "DATETIME_TRUNC" => Ok(ScalarFunction::DatetimeTrunc),
            "TIMESTAMP_TRUNC" => Ok(ScalarFunction::TimestampTrunc),
            "TIME_TRUNC" => Ok(ScalarFunction::TimeTrunc),
            "FORMAT_DATE" => Ok(ScalarFunction::FormatDate),
            "FORMAT_TIMESTAMP" => Ok(ScalarFunction::FormatTimestamp),
            "FORMAT_DATETIME" => Ok(ScalarFunction::FormatDatetime),
            "FORMAT_TIME" => Ok(ScalarFunction::FormatTime),
            "PARSE_DATE" => Ok(ScalarFunction::ParseDate),
            "PARSE_TIMESTAMP" => Ok(ScalarFunction::ParseTimestamp),
            "PARSE_DATETIME" => Ok(ScalarFunction::ParseDatetime),
            "PARSE_TIME" => Ok(ScalarFunction::ParseTime),
            "DATE" => Ok(ScalarFunction::Date),
            "TIME" => Ok(ScalarFunction::Time),
            "DATETIME" => Ok(ScalarFunction::Datetime),
            "TIMESTAMP" => Ok(ScalarFunction::Timestamp),
            "TIMESTAMP_MICROS" => Ok(ScalarFunction::TimestampMicros),
            "TIMESTAMP_MILLIS" => Ok(ScalarFunction::TimestampMillis),
            "TIMESTAMP_SECONDS" => Ok(ScalarFunction::TimestampSeconds),
            "UNIX_DATE" => Ok(ScalarFunction::UnixDate),
            "UNIX_MICROS" => Ok(ScalarFunction::UnixMicros),
            "UNIX_MILLIS" => Ok(ScalarFunction::UnixMillis),
            "UNIX_SECONDS" => Ok(ScalarFunction::UnixSeconds),
            "DATE_FROM_UNIX_DATE" => Ok(ScalarFunction::DateFromUnixDate),
            "LAST_DAY" => Ok(ScalarFunction::LastDay),
            "DATETIME_BUCKET" => Ok(ScalarFunction::DatetimeBucket),
            "TIMESTAMP_BUCKET" => Ok(ScalarFunction::TimestampBucket),
            "MAKE_INTERVAL" => Ok(ScalarFunction::MakeInterval),
            "JUSTIFY_DAYS" => Ok(ScalarFunction::JustifyDays),
            "JUSTIFY_HOURS" => Ok(ScalarFunction::JustifyHours),
            "JUSTIFY_INTERVAL" => Ok(ScalarFunction::JustifyInterval),
            "GENERATE_DATE_ARRAY" => Ok(ScalarFunction::GenerateDateArray),
            "GENERATE_TIMESTAMP_ARRAY" => Ok(ScalarFunction::GenerateTimestampArray),
            "ARRAY_LENGTH" => Ok(ScalarFunction::ArrayLength),
            "ARRAY_TO_STRING" => Ok(ScalarFunction::ArrayToString),
            "ARRAY_CONCAT" => Ok(ScalarFunction::ArrayConcat),
            "ARRAY_REVERSE" => Ok(ScalarFunction::ArrayReverse),
            "GENERATE_ARRAY" => Ok(ScalarFunction::GenerateArray),
            "TO_JSON" => Ok(ScalarFunction::ToJson),
            "TO_JSON_STRING" => Ok(ScalarFunction::ToJsonString),
            "JSON_EXTRACT" => Ok(ScalarFunction::JsonExtract),
            "JSON_EXTRACT_SCALAR" => Ok(ScalarFunction::JsonExtractScalar),
            "JSON_EXTRACT_ARRAY" => Ok(ScalarFunction::JsonExtractArray),
            "JSON_QUERY" => Ok(ScalarFunction::JsonQuery),
            "JSON_VALUE" => Ok(ScalarFunction::JsonValue),
            "PARSE_JSON" => Ok(ScalarFunction::ParseJson),
            "JSON_TYPE" => Ok(ScalarFunction::JsonType),
            "STRING" => Ok(ScalarFunction::String),
            "SAFE_CAST" => Ok(ScalarFunction::SafeCast),
            "CAST" => Ok(ScalarFunction::Cast),
            "TYPEOF" => Ok(ScalarFunction::TypeOf),
            "MD5" => Ok(ScalarFunction::Md5),
            "SHA1" => Ok(ScalarFunction::Sha1),
            "SHA256" => Ok(ScalarFunction::Sha256),
            "SHA512" => Ok(ScalarFunction::Sha512),
            "FARM_FINGERPRINT" => Ok(ScalarFunction::FarmFingerprint),
            "GENERATE_UUID" => Ok(ScalarFunction::GenerateUuid),
            "ERROR" => Ok(ScalarFunction::Error),
            "RANGE" => Ok(ScalarFunction::Range),
            "RANGE_BUCKET" => Ok(ScalarFunction::RangeBucket),
            "SAFE_CONVERT_BYTES_TO_STRING" => Ok(ScalarFunction::SafeConvertBytesToString),
            "CONVERT_BYTES_TO_STRING" => Ok(ScalarFunction::ConvertBytesToString),
            "BIT_COUNT" => Ok(ScalarFunction::BitCount),
            "INT64" => Ok(ScalarFunction::Int64FromJson),
            "FLOAT64" => Ok(ScalarFunction::Float64FromJson),
            "BOOL" => Ok(ScalarFunction::BoolFromJson),
            "OFFSET" => Ok(ScalarFunction::ArrayOffset),
            "ORDINAL" => Ok(ScalarFunction::ArrayOrdinal),
            "SAFE_OFFSET" => Ok(ScalarFunction::SafeOffset),
            "SAFE_ORDINAL" => Ok(ScalarFunction::SafeOrdinal),
            _ => Ok(ScalarFunction::Custom(name.to_string())),
        }
    }

    fn extract_function_args(func: &ast::Function, schema: &PlanSchema) -> Result<Vec<Expr>> {
        match &func.args {
            ast::FunctionArguments::None => Ok(vec![]),
            ast::FunctionArguments::Subquery(_) => {
                Err(Error::unsupported("Subquery function arguments"))
            }
            ast::FunctionArguments::List(list) => {
                let mut args = Vec::new();
                for arg in &list.args {
                    match arg {
                        ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                            ast::FunctionArgExpr::Expr(e) => {
                                args.push(Self::plan_expr(e, schema)?);
                            }
                            ast::FunctionArgExpr::Wildcard => {
                                args.push(Expr::Wildcard { table: None });
                            }
                            ast::FunctionArgExpr::QualifiedWildcard(name) => {
                                args.push(Expr::Wildcard {
                                    table: Some(name.to_string()),
                                });
                            }
                            ast::FunctionArgExpr::TableRef(_) => {
                                return Err(Error::unsupported(
                                    "TABLE argument in scalar function",
                                ));
                            }
                        },
                        ast::FunctionArg::Named { arg, .. } => {
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                args.push(Self::plan_expr(e, schema)?);
                            }
                        }
                        ast::FunctionArg::ExprNamed { arg, .. } => {
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                args.push(Self::plan_expr(e, schema)?);
                            }
                        }
                    }
                }
                Ok(args)
            }
        }
    }

    fn extract_function_args_full(
        func: &ast::Function,
        schema: &PlanSchema,
        subquery_planner: Option<SubqueryPlannerFn>,
        named_windows: &[ast::NamedWindowDefinition],
        udf_resolver: Option<UdfResolverFn>,
    ) -> Result<Vec<Expr>> {
        match &func.args {
            ast::FunctionArguments::None => Ok(vec![]),
            ast::FunctionArguments::Subquery(_) => {
                Err(Error::unsupported("Subquery function arguments"))
            }
            ast::FunctionArguments::List(list) => {
                let mut args = Vec::new();
                for arg in &list.args {
                    match arg {
                        ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                            ast::FunctionArgExpr::Expr(e) => {
                                args.push(Self::plan_expr_full(
                                    e,
                                    schema,
                                    subquery_planner,
                                    named_windows,
                                    udf_resolver,
                                )?);
                            }
                            ast::FunctionArgExpr::Wildcard => {
                                args.push(Expr::Wildcard { table: None });
                            }
                            ast::FunctionArgExpr::QualifiedWildcard(name) => {
                                args.push(Expr::Wildcard {
                                    table: Some(name.to_string()),
                                });
                            }
                            ast::FunctionArgExpr::TableRef(_) => {
                                return Err(Error::unsupported(
                                    "TABLE argument in scalar function",
                                ));
                            }
                        },
                        ast::FunctionArg::Named { arg, .. } => {
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                args.push(Self::plan_expr_full(
                                    e,
                                    schema,
                                    subquery_planner,
                                    named_windows,
                                    udf_resolver,
                                )?);
                            }
                        }
                        ast::FunctionArg::ExprNamed { arg, .. } => {
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                args.push(Self::plan_expr_full(
                                    e,
                                    schema,
                                    subquery_planner,
                                    named_windows,
                                    udf_resolver,
                                )?);
                            }
                        }
                    }
                }
                Ok(args)
            }
        }
    }

    fn plan_case(
        operand: Option<&ast::Expr>,
        conditions: &[ast::CaseWhen],
        else_result: Option<&ast::Expr>,
        schema: &PlanSchema,
    ) -> Result<Expr> {
        let operand = operand
            .map(|e| Self::plan_expr(e, schema))
            .transpose()?
            .map(Box::new);

        let mut when_clauses = Vec::new();
        for cw in conditions {
            let condition = Self::plan_expr(&cw.condition, schema)?;
            let result = Self::plan_expr(&cw.result, schema)?;
            when_clauses.push(WhenClause { condition, result });
        }

        let else_result = else_result
            .map(|e| Self::plan_expr(e, schema))
            .transpose()?
            .map(Box::new);

        Ok(Expr::Case {
            operand,
            when_clauses,
            else_result,
        })
    }

    fn plan_data_type(sql_type: &ast::DataType) -> Result<yachtsql_common::types::DataType> {
        use yachtsql_common::types::DataType;
        match sql_type {
            ast::DataType::Boolean | ast::DataType::Bool => Ok(DataType::Bool),
            ast::DataType::Int64 | ast::DataType::BigInt(_) => Ok(DataType::Int64),
            ast::DataType::Float64 | ast::DataType::Double(_) => Ok(DataType::Float64),
            ast::DataType::Numeric(info) | ast::DataType::Decimal(info) => {
                let ps = match info {
                    ast::ExactNumberInfo::PrecisionAndScale(p, s) => Some((*p as u8, *s as u8)),
                    ast::ExactNumberInfo::Precision(p) => Some((*p as u8, 0)),
                    ast::ExactNumberInfo::None => None,
                };
                Ok(DataType::Numeric(ps))
            }
            ast::DataType::BigNumeric(_) => Ok(DataType::BigNumeric),
            ast::DataType::String(_) | ast::DataType::Varchar(_) | ast::DataType::Text => {
                Ok(DataType::String)
            }
            ast::DataType::Bytes(_) | ast::DataType::Bytea => Ok(DataType::Bytes),
            ast::DataType::Date => Ok(DataType::Date),
            ast::DataType::Time(..) => Ok(DataType::Time),
            ast::DataType::Datetime(_) => Ok(DataType::DateTime),
            ast::DataType::Timestamp(..) => Ok(DataType::Timestamp),
            ast::DataType::JSON | ast::DataType::JSONB => Ok(DataType::Json),
            ast::DataType::Interval { .. } => Ok(DataType::Interval),
            ast::DataType::Array(elem_def) => {
                let inner = match elem_def {
                    ast::ArrayElemTypeDef::AngleBracket(dt)
                    | ast::ArrayElemTypeDef::SquareBracket(dt, _)
                    | ast::ArrayElemTypeDef::Parenthesis(dt) => Self::plan_data_type(dt)?,
                    ast::ArrayElemTypeDef::None => DataType::Unknown,
                };
                Ok(DataType::Array(Box::new(inner)))
            }
            ast::DataType::Struct(fields, _) => {
                let struct_fields = fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        let name = f
                            .field_name
                            .as_ref()
                            .map(|id| id.value.clone())
                            .unwrap_or_else(|| format!("_field{}", i));
                        let field_type =
                            Self::plan_data_type(&f.field_type).unwrap_or(DataType::Unknown);
                        yachtsql_common::types::StructField {
                            name,
                            data_type: field_type,
                        }
                    })
                    .collect();
                Ok(DataType::Struct(struct_fields))
            }
            ast::DataType::Range(inner) => {
                let inner_type = Self::plan_data_type(inner)?;
                Ok(DataType::Range(Box::new(inner_type)))
            }
            _ => Ok(DataType::Unknown),
        }
    }

    fn plan_interval(interval: &ast::Interval, schema: &PlanSchema) -> Result<Expr> {
        use yachtsql_common::types::IntervalValue;

        let value_expr = Self::plan_expr(&interval.value, schema)?;
        let value = match &value_expr {
            Expr::Literal(Literal::Int64(n)) => *n,
            Expr::UnaryOp {
                op: UnaryOp::Minus,
                expr,
            } => match expr.as_ref() {
                Expr::Literal(Literal::Int64(n)) => -*n,
                _ => {
                    return Err(Error::parse_error(
                        "INTERVAL value must be an integer literal",
                    ));
                }
            },
            _ => {
                return Err(Error::parse_error(
                    "INTERVAL value must be an integer literal",
                ));
            }
        };

        let (months, days, nanos) = match &interval.leading_field {
            Some(ast::DateTimeField::Year) | Some(ast::DateTimeField::Years) => {
                (value as i32 * 12, 0, 0i64)
            }
            Some(ast::DateTimeField::Month) | Some(ast::DateTimeField::Months) => {
                (value as i32, 0, 0i64)
            }
            Some(ast::DateTimeField::Day) | Some(ast::DateTimeField::Days) => {
                (0, value as i32, 0i64)
            }
            Some(ast::DateTimeField::Hour) | Some(ast::DateTimeField::Hours) => (
                0,
                0,
                value * IntervalValue::MICROS_PER_HOUR * IntervalValue::NANOS_PER_MICRO,
            ),
            Some(ast::DateTimeField::Minute) | Some(ast::DateTimeField::Minutes) => (
                0,
                0,
                value * IntervalValue::MICROS_PER_MINUTE * IntervalValue::NANOS_PER_MICRO,
            ),
            Some(ast::DateTimeField::Second) | Some(ast::DateTimeField::Seconds) => (
                0,
                0,
                value * IntervalValue::MICROS_PER_SECOND * IntervalValue::NANOS_PER_MICRO,
            ),
            Some(ast::DateTimeField::Millisecond) | Some(ast::DateTimeField::Milliseconds) => {
                (0, 0, value * 1_000_000)
            }
            Some(ast::DateTimeField::Microsecond) | Some(ast::DateTimeField::Microseconds) => {
                (0, 0, value * 1_000)
            }
            None => (0, value as i32, 0i64),
            _ => {
                return Err(Error::unsupported(format!(
                    "Unsupported interval field: {:?}",
                    interval.leading_field
                )));
            }
        };

        Ok(Expr::Literal(Literal::Interval {
            months,
            days,
            nanos,
        }))
    }

    fn substitute_parameters(expr: &Expr, params: &[FunctionArg], args: &[Expr]) -> Expr {
        let mut param_map: std::collections::HashMap<String, Expr> =
            std::collections::HashMap::new();
        for (i, param) in params.iter().enumerate() {
            let value = if i < args.len() {
                args[i].clone()
            } else if let Some(default) = &param.default {
                default.clone()
            } else {
                continue;
            };
            param_map.insert(param.name.to_uppercase(), value);
        }
        let param_ref_map: std::collections::HashMap<String, &Expr> =
            param_map.iter().map(|(k, v)| (k.clone(), v)).collect();

        Self::substitute_expr(expr, &param_ref_map)
    }

    fn apply_struct_field_names(expr: Expr, return_type: &DataType) -> Expr {
        use yachtsql_common::types::DataType;
        match (&expr, return_type) {
            (Expr::Struct { fields }, DataType::Struct(struct_fields)) => {
                let new_fields: Vec<(Option<String>, Expr)> = fields
                    .iter()
                    .enumerate()
                    .map(|(i, (existing_name, field_expr))| {
                        let field_name = if existing_name.is_some() {
                            existing_name.clone()
                        } else if i < struct_fields.len() {
                            Some(struct_fields[i].name.clone())
                        } else {
                            None
                        };
                        (field_name, field_expr.clone())
                    })
                    .collect();
                Expr::Struct { fields: new_fields }
            }
            _ => expr,
        }
    }

    fn substitute_expr(expr: &Expr, param_map: &std::collections::HashMap<String, &Expr>) -> Expr {
        match expr {
            Expr::Column { name, table, index } => {
                let upper_name = name.to_uppercase();
                if table.is_none()
                    && let Some(replacement) = param_map.get(&upper_name)
                {
                    return (*replacement).clone();
                }
                Expr::Column {
                    name: name.clone(),
                    table: table.clone(),
                    index: *index,
                }
            }
            Expr::Aggregate {
                func,
                args,
                distinct,
                filter,
                order_by,
                limit,
                ignore_nulls,
            } => Expr::Aggregate {
                func: *func,
                args: args
                    .iter()
                    .map(|a| Self::substitute_expr(a, param_map))
                    .collect(),
                distinct: *distinct,
                filter: filter
                    .as_ref()
                    .map(|f| Box::new(Self::substitute_expr(f, param_map))),
                order_by: order_by
                    .iter()
                    .map(|o| SortExpr {
                        expr: Self::substitute_expr(&o.expr, param_map),
                        asc: o.asc,
                        nulls_first: o.nulls_first,
                    })
                    .collect(),
                limit: *limit,
                ignore_nulls: *ignore_nulls,
            },
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::substitute_expr(left, param_map)),
                op: *op,
                right: Box::new(Self::substitute_expr(right, param_map)),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(Self::substitute_expr(inner, param_map)),
            },
            Expr::ScalarFunction { name, args } => {
                if let ScalarFunction::Custom(func_name) = name
                    && let Some(replacement) = param_map.get(&func_name.to_uppercase())
                    && let Expr::Lambda {
                        params: lambda_params,
                        body: lambda_body,
                    } = replacement
                {
                    let substituted_args: Vec<Expr> = args
                        .iter()
                        .map(|a| Self::substitute_expr(a, param_map))
                        .collect();
                    let lambda_param_map: std::collections::HashMap<String, &Expr> = lambda_params
                        .iter()
                        .zip(substituted_args.iter())
                        .map(|(p, a)| (p.to_uppercase(), a))
                        .collect();
                    return Self::substitute_expr(lambda_body, &lambda_param_map);
                }
                Expr::ScalarFunction {
                    name: name.clone(),
                    args: args
                        .iter()
                        .map(|a| Self::substitute_expr(a, param_map))
                        .collect(),
                }
            }
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => Expr::Case {
                operand: operand
                    .as_ref()
                    .map(|o| Box::new(Self::substitute_expr(o, param_map))),
                when_clauses: when_clauses
                    .iter()
                    .map(|wc| WhenClause {
                        condition: Self::substitute_expr(&wc.condition, param_map),
                        result: Self::substitute_expr(&wc.result, param_map),
                    })
                    .collect(),
                else_result: else_result
                    .as_ref()
                    .map(|e| Box::new(Self::substitute_expr(e, param_map))),
            },
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => Expr::Cast {
                expr: Box::new(Self::substitute_expr(inner, param_map)),
                data_type: data_type.clone(),
                safe: *safe,
            },
            Expr::Alias { expr: inner, name } => Expr::Alias {
                expr: Box::new(Self::substitute_expr(inner, param_map)),
                name: name.clone(),
            },
            Expr::IsNull {
                expr: inner,
                negated,
            } => Expr::IsNull {
                expr: Box::new(Self::substitute_expr(inner, param_map)),
                negated: *negated,
            },
            Expr::Between {
                expr: inner,
                low,
                high,
                negated,
            } => Expr::Between {
                expr: Box::new(Self::substitute_expr(inner, param_map)),
                low: Box::new(Self::substitute_expr(low, param_map)),
                high: Box::new(Self::substitute_expr(high, param_map)),
                negated: *negated,
            },
            Expr::InList {
                expr: inner,
                list,
                negated,
            } => Expr::InList {
                expr: Box::new(Self::substitute_expr(inner, param_map)),
                list: list
                    .iter()
                    .map(|e| Self::substitute_expr(e, param_map))
                    .collect(),
                negated: *negated,
            },
            Expr::Struct { fields } => Expr::Struct {
                fields: fields
                    .iter()
                    .map(|(name, e)| (name.clone(), Self::substitute_expr(e, param_map)))
                    .collect(),
            },
            Expr::StructAccess { expr: inner, field } => Expr::StructAccess {
                expr: Box::new(Self::substitute_expr(inner, param_map)),
                field: field.clone(),
            },
            Expr::Array {
                elements,
                element_type,
            } => Expr::Array {
                elements: elements
                    .iter()
                    .map(|e| Self::substitute_expr(e, param_map))
                    .collect(),
                element_type: element_type.clone(),
            },
            Expr::ArrayAccess { array, index } => Expr::ArrayAccess {
                array: Box::new(Self::substitute_expr(array, param_map)),
                index: Box::new(Self::substitute_expr(index, param_map)),
            },
            Expr::Window {
                func,
                args,
                partition_by,
                order_by,
                frame,
            } => Expr::Window {
                func: *func,
                args: args
                    .iter()
                    .map(|a| Self::substitute_expr(a, param_map))
                    .collect(),
                partition_by: partition_by
                    .iter()
                    .map(|e| Self::substitute_expr(e, param_map))
                    .collect(),
                order_by: order_by
                    .iter()
                    .map(|o| SortExpr {
                        expr: Self::substitute_expr(&o.expr, param_map),
                        asc: o.asc,
                        nulls_first: o.nulls_first,
                    })
                    .collect(),
                frame: frame.clone(),
            },
            Expr::AggregateWindow {
                func,
                args,
                distinct,
                partition_by,
                order_by,
                frame,
            } => Expr::AggregateWindow {
                func: *func,
                args: args
                    .iter()
                    .map(|a| Self::substitute_expr(a, param_map))
                    .collect(),
                distinct: *distinct,
                partition_by: partition_by
                    .iter()
                    .map(|e| Self::substitute_expr(e, param_map))
                    .collect(),
                order_by: order_by
                    .iter()
                    .map(|o| SortExpr {
                        expr: Self::substitute_expr(&o.expr, param_map),
                        asc: o.asc,
                        nulls_first: o.nulls_first,
                    })
                    .collect(),
                frame: frame.clone(),
            },
            Expr::Like {
                expr: inner,
                pattern,
                negated,
                case_insensitive,
            } => Expr::Like {
                expr: Box::new(Self::substitute_expr(inner, param_map)),
                pattern: Box::new(Self::substitute_expr(pattern, param_map)),
                negated: *negated,
                case_insensitive: *case_insensitive,
            },
            Expr::Extract { field, expr: inner } => Expr::Extract {
                field: *field,
                expr: Box::new(Self::substitute_expr(inner, param_map)),
            },
            Expr::Substring {
                expr: inner,
                start,
                length,
            } => Expr::Substring {
                expr: Box::new(Self::substitute_expr(inner, param_map)),
                start: start
                    .as_ref()
                    .map(|s| Box::new(Self::substitute_expr(s, param_map))),
                length: length
                    .as_ref()
                    .map(|l| Box::new(Self::substitute_expr(l, param_map))),
            },
            Expr::Trim {
                expr: inner,
                trim_what,
                trim_where,
            } => Expr::Trim {
                expr: Box::new(Self::substitute_expr(inner, param_map)),
                trim_what: trim_what
                    .as_ref()
                    .map(|t| Box::new(Self::substitute_expr(t, param_map))),
                trim_where: *trim_where,
            },
            Expr::Position { substr, string } => Expr::Position {
                substr: Box::new(Self::substitute_expr(substr, param_map)),
                string: Box::new(Self::substitute_expr(string, param_map)),
            },
            Expr::Overlay {
                expr: inner,
                overlay_what,
                overlay_from,
                overlay_for,
            } => Expr::Overlay {
                expr: Box::new(Self::substitute_expr(inner, param_map)),
                overlay_what: Box::new(Self::substitute_expr(overlay_what, param_map)),
                overlay_from: Box::new(Self::substitute_expr(overlay_from, param_map)),
                overlay_for: overlay_for
                    .as_ref()
                    .map(|f| Box::new(Self::substitute_expr(f, param_map))),
            },
            Expr::Interval {
                value,
                leading_field,
            } => Expr::Interval {
                value: Box::new(Self::substitute_expr(value, param_map)),
                leading_field: *leading_field,
            },
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => Expr::AtTimeZone {
                timestamp: Box::new(Self::substitute_expr(timestamp, param_map)),
                time_zone: Box::new(Self::substitute_expr(time_zone, param_map)),
            },
            Expr::JsonAccess { expr: inner, path } => Expr::JsonAccess {
                expr: Box::new(Self::substitute_expr(inner, param_map)),
                path: path.clone(),
            },
            Expr::InUnnest {
                expr: inner,
                array_expr,
                negated,
            } => Expr::InUnnest {
                expr: Box::new(Self::substitute_expr(inner, param_map)),
                array_expr: Box::new(Self::substitute_expr(array_expr, param_map)),
                negated: *negated,
            },
            Expr::UserDefinedAggregate {
                name,
                args,
                distinct,
                filter,
            } => Expr::UserDefinedAggregate {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| Self::substitute_expr(a, param_map))
                    .collect(),
                distinct: *distinct,
                filter: filter
                    .as_ref()
                    .map(|f| Box::new(Self::substitute_expr(f, param_map))),
            },
            Expr::IsDistinctFrom {
                left,
                right,
                negated,
            } => Expr::IsDistinctFrom {
                left: Box::new(Self::substitute_expr(left, param_map)),
                right: Box::new(Self::substitute_expr(right, param_map)),
                negated: *negated,
            },
            Expr::Lambda { params, body } => Expr::Lambda {
                params: params.clone(),
                body: Box::new(Self::substitute_expr(body, param_map)),
            },
            other => other.clone(),
        }
    }
}

fn unescape_unicode(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.peek() {
                Some('u') => {
                    chars.next();
                    let hex: String = chars.by_ref().take(4).collect();
                    if hex.len() == 4
                        && let Ok(code_point) = u32::from_str_radix(&hex, 16)
                        && let Some(unicode_char) = char::from_u32(code_point)
                    {
                        result.push(unicode_char);
                        continue;
                    }
                    result.push_str("\\u");
                    result.push_str(&hex);
                }
                Some('U') => {
                    chars.next();
                    let hex: String = chars.by_ref().take(8).collect();
                    if hex.len() == 8
                        && let Ok(code_point) = u32::from_str_radix(&hex, 16)
                        && let Some(unicode_char) = char::from_u32(code_point)
                    {
                        result.push(unicode_char);
                        continue;
                    }
                    result.push_str("\\U");
                    result.push_str(&hex);
                }
                Some('n') => {
                    chars.next();
                    result.push('\n');
                }
                Some('t') => {
                    chars.next();
                    result.push('\t');
                }
                Some('r') => {
                    chars.next();
                    result.push('\r');
                }
                Some('\\') => {
                    chars.next();
                    result.push('\\');
                }
                Some('\'') => {
                    chars.next();
                    result.push('\'');
                }
                Some('"') => {
                    chars.next();
                    result.push('"');
                }
                _ => {
                    result.push('\\');
                }
            }
        } else {
            result.push(c);
        }
    }
    result
}

fn parse_byte_string_escapes(s: &str) -> Vec<u8> {
    let mut result = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.peek() {
                Some('x') | Some('X') => {
                    chars.next();
                    let mut hex = String::new();
                    for _ in 0..2 {
                        if let Some(&c) = chars.peek()
                            && c.is_ascii_hexdigit()
                        {
                            hex.push(c);
                            chars.next();
                        }
                    }
                    if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                        result.push(byte);
                    }
                }
                Some('n') => {
                    chars.next();
                    result.push(b'\n');
                }
                Some('t') => {
                    chars.next();
                    result.push(b'\t');
                }
                Some('r') => {
                    chars.next();
                    result.push(b'\r');
                }
                Some('\\') => {
                    chars.next();
                    result.push(b'\\');
                }
                Some('\'') => {
                    chars.next();
                    result.push(b'\'');
                }
                Some('"') => {
                    chars.next();
                    result.push(b'"');
                }
                _ => {
                    result.push(b'\\');
                }
            }
        } else {
            let mut buf = [0u8; 4];
            let encoded = c.encode_utf8(&mut buf);
            result.extend_from_slice(encoded.as_bytes());
        }
    }
    result
}
