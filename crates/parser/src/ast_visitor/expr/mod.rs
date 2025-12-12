mod functions;
mod identifiers;
mod literals;
mod operators;
mod predicates;
mod special;
mod subqueries;

use sqlparser::ast;
use yachtsql_core::error::{Error, Result};
use yachtsql_ir::expr::{BinaryOp, CastDataType, Expr, LiteralValue, UnaryOp};

use super::LogicalPlanBuilder;

impl LogicalPlanBuilder {
    pub(super) fn sql_expr_to_expr_for_cast(
        &self,
        expr: &ast::Expr,
        target_type: &CastDataType,
    ) -> Result<Expr> {
        if matches!(target_type, CastDataType::Json)
            && let ast::Expr::Value(value) = expr
            && let ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) =
                &value.value
        {
            return Ok(Expr::Literal(LiteralValue::String(s.clone())));
        }
        self.sql_expr_to_expr(expr)
    }

    pub(super) fn sql_expr_to_expr(&self, expr: &ast::Expr) -> Result<Expr> {
        match expr {
            ast::Expr::Identifier(ident) => {
                if let Some(var) = self.get_session_variable(&ident.value) {
                    match &var.value {
                        Some(v) => Ok(Expr::Literal(self.value_to_literal(v)?)),
                        None => Err(Error::invalid_query(format!(
                            "Variable '{}' has not been assigned a value",
                            ident.value
                        ))),
                    }
                } else {
                    Ok(Expr::column(ident.value.clone()))
                }
            }

            ast::Expr::CompoundIdentifier(idents) => self.convert_compound_identifier_expr(idents),

            ast::Expr::CompoundFieldAccess { root, access_chain } => {
                self.convert_composite_access(root, access_chain)
            }

            ast::Expr::Nested(nested) => self.sql_expr_to_expr(nested),

            ast::Expr::Value(value) => Ok(Expr::Literal(self.sql_value_to_literal(value)?)),

            ast::Expr::TypedString(typed) => {
                let value_owned = typed.value.value.clone().into_string().ok_or_else(|| {
                    Error::invalid_query("Typed string literal must be a string value".to_string())
                })?;
                self.convert_typed_string(&typed.data_type, &value_owned)
            }

            ast::Expr::Collate { expr, collation } => self.convert_collate(expr, collation),

            ast::Expr::BinaryOp { left, op, right } => match op {
                ast::BinaryOperator::Arrow => {
                    if matches!(right.as_ref(), ast::Expr::Array(_)) {
                        let left_expr = self.sql_expr_to_expr(left)?;
                        let right_expr = self.sql_expr_to_expr(right)?;
                        Ok(Expr::Function {
                            name: yachtsql_ir::FunctionName::parse("HSTORE_GET_VALUES"),
                            args: vec![left_expr, right_expr],
                        })
                    } else {
                        self.make_json_arrow_function("JSON_EXTRACT_JSON", left, right)
                    }
                }
                ast::BinaryOperator::LongArrow => {
                    self.make_json_arrow_function("JSON_VALUE_TEXT", left, right)
                }
                ast::BinaryOperator::AtArrow => {
                    let left_expr = self.sql_expr_to_expr(left)?;
                    let right_expr = self.sql_expr_to_expr(right)?;
                    Ok(Expr::binary_op(
                        left_expr,
                        yachtsql_ir::BinaryOp::ArrayContains,
                        right_expr,
                    ))
                }
                ast::BinaryOperator::HashArrow => {
                    self.make_json_path_array_function("JSON_EXTRACT_PATH_ARRAY", left, right)
                }
                ast::BinaryOperator::HashLongArrow => {
                    self.make_json_path_array_function("JSON_EXTRACT_PATH_ARRAY_TEXT", left, right)
                }
                ast::BinaryOperator::Question => {
                    let left_expr = self.sql_expr_to_expr(left)?;
                    let right_expr = self.sql_expr_to_expr(right)?;
                    Ok(Expr::Function {
                        name: yachtsql_ir::FunctionName::parse("HSTORE_EXISTS"),
                        args: vec![left_expr, right_expr],
                    })
                }
                ast::BinaryOperator::QuestionAnd => {
                    let left_expr = self.sql_expr_to_expr(left)?;
                    let right_expr = self.sql_expr_to_expr(right)?;
                    Ok(Expr::Function {
                        name: yachtsql_ir::FunctionName::parse("HSTORE_EXISTS_ALL"),
                        args: vec![left_expr, right_expr],
                    })
                }
                ast::BinaryOperator::QuestionPipe => {
                    let left_expr = self.sql_expr_to_expr(left)?;
                    let right_expr = self.sql_expr_to_expr(right)?;
                    Ok(Expr::Function {
                        name: yachtsql_ir::FunctionName::parse("HSTORE_EXISTS_ANY"),
                        args: vec![left_expr, right_expr],
                    })
                }
                ast::BinaryOperator::Custom(op_str) => match op_str.as_str() {
                    "#>" => {
                        self.make_json_path_array_function("JSON_EXTRACT_PATH_ARRAY", left, right)
                    }
                    "#>>" => self.make_json_path_array_function(
                        "JSON_EXTRACT_PATH_ARRAY_TEXT",
                        left,
                        right,
                    ),
                    _ => {
                        let left_expr = self.sql_expr_to_expr(left)?;
                        let right_expr = self.sql_expr_to_expr(right)?;
                        let binary_op = self.sql_binary_op_to_op(op)?;
                        Ok(Expr::binary_op(left_expr, binary_op, right_expr))
                    }
                },
                _ => {
                    let left_expr = self.sql_expr_to_expr(left)?;
                    let right_expr = self.sql_expr_to_expr(right)?;
                    let binary_op = self.sql_binary_op_to_op(op)?;
                    Ok(Expr::binary_op(left_expr, binary_op, right_expr))
                }
            },

            ast::Expr::UnaryOp { op, expr } => {
                if *op == ast::UnaryOperator::Minus
                    && let ast::Expr::Value(value) = expr.as_ref()
                    && let ast::Value::Number(s, _) = &value.value
                    && !s.contains('.')
                    && !s.to_lowercase().contains('e')
                {
                    let neg_str = format!("-{}", s);
                    if let Ok(i) = neg_str.parse::<i64>() {
                        return Ok(Expr::Literal(LiteralValue::Int64(i)));
                    }
                }
                let inner_expr = self.sql_expr_to_expr(expr)?;
                let unary_op = self.sql_unary_op_to_op(op)?;
                Ok(Expr::unary_op(unary_op, inner_expr))
            }

            ast::Expr::Function(function) => {
                let name_str = function.name.to_string();
                let name = yachtsql_ir::FunctionName::parse(&name_str);
                let (args, has_distinct, order_by_clauses) = match &function.args {
                    ast::FunctionArguments::None => (Vec::new(), false, None),
                    ast::FunctionArguments::Subquery(subquery) => {
                        if name_str.eq_ignore_ascii_case("ARRAY") {
                            let plan = self.query_to_plan(subquery)?;
                            return Ok(Expr::ArraySubquery {
                                subquery: Box::new(plan.root().clone()),
                            });
                        }
                        return Err(Error::unsupported_feature(
                            "Subquery in function not supported".to_string(),
                        ));
                    }
                    ast::FunctionArguments::List(arg_list) => {
                        let distinct = matches!(
                            arg_list.duplicate_treatment,
                            Some(ast::DuplicateTreatment::Distinct)
                        );
                        let args = arg_list
                            .args
                            .iter()
                            .map(|arg| match arg {
                                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                                    self.sql_expr_to_expr(e)
                                }
                                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                                    Ok(Expr::Wildcard)
                                }
                                ast::FunctionArg::Named {
                                    arg: ast::FunctionArgExpr::Expr(e),
                                    ..
                                } => self.sql_expr_to_expr(e),
                                _ => Err(Error::unsupported_feature(
                                    "Function arg type not supported".to_string(),
                                )),
                            })
                            .collect::<Result<Vec<_>>>()?;

                        let order_by = if !arg_list.clauses.is_empty() {
                            let mut order_exprs = Vec::new();
                            for clause in &arg_list.clauses {
                                if let ast::FunctionArgumentClause::OrderBy(order_by_list) = clause
                                {
                                    for order_expr in order_by_list {
                                        let expr = self.sql_expr_to_expr(&order_expr.expr)?;
                                        order_exprs.push(yachtsql_ir::expr::OrderByExpr {
                                            expr,
                                            asc: order_expr.options.asc,
                                            nulls_first: order_expr.options.nulls_first,
                                            collation: None,
                                            with_fill: None,
                                        });
                                    }
                                }
                            }
                            if order_exprs.is_empty() {
                                None
                            } else {
                                Some(order_exprs)
                            }
                        } else {
                            None
                        };

                        (args, distinct, order_by)
                    }
                };

                if let Some(udf_expanded) = self.expand_udf_call(&name_str, &args, function)? {
                    return Ok(udf_expanded);
                }

                if let Some(normalized) = self.normalize_dialect_function(&name_str, &args)? {
                    return Ok(normalized);
                }

                if (name_str.eq_ignore_ascii_case("IFNULL")
                    || name_str.eq_ignore_ascii_case("NULLIF"))
                    && args.len() != 2
                {
                    return Err(Error::invalid_query(format!(
                        "{} requires exactly 2 arguments, got {}",
                        name_str.to_uppercase(),
                        args.len()
                    )));
                }

                let order_by_clauses = if !function.within_group.is_empty() {
                    let mut order_exprs = Vec::new();
                    for order_expr in &function.within_group {
                        let expr = self.sql_expr_to_expr(&order_expr.expr)?;
                        order_exprs.push(yachtsql_ir::expr::OrderByExpr {
                            expr,
                            asc: order_expr.options.asc,
                            nulls_first: order_expr.options.nulls_first,
                            collation: None,
                            with_fill: None,
                        });
                    }
                    Some(order_exprs)
                } else {
                    order_by_clauses
                };

                if let Some(over) = &function.over {
                    let spec = match over {
                        ast::WindowType::WindowSpec(spec) => {
                            if let Some(window_name_ident) = &spec.window_name {
                                let window_name = window_name_ident.value.clone();
                                let base_spec =
                                    self.get_named_window(&window_name).ok_or_else(|| {
                                        Error::invalid_query(format!(
                                            "Undefined window name '{}' in OVER clause",
                                            window_name
                                        ))
                                    })?;

                                self.merge_window_specs(&base_spec, spec)?
                            } else {
                                spec.clone()
                            }
                        }
                        ast::WindowType::NamedWindow(ident) => {
                            let window_name = ident.value.clone();
                            self.get_named_window(&window_name).ok_or_else(|| {
                                Error::invalid_query(format!(
                                    "Undefined window name '{}' in OVER clause",
                                    window_name
                                ))
                            })?
                        }
                    };

                    let partition_by = spec
                        .partition_by
                        .iter()
                        .map(|e| self.sql_expr_to_expr(e))
                        .collect::<Result<Vec<_>>>()?;

                    let order_by = spec
                        .order_by
                        .iter()
                        .map(|order_expr| {
                            let expr = self.sql_expr_to_expr(&order_expr.expr)?;
                            Ok(yachtsql_ir::expr::OrderByExpr {
                                expr,
                                asc: order_expr.options.asc,
                                nulls_first: order_expr.options.nulls_first,
                                collation: None,
                                with_fill: None,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let frame_units =
                        spec.window_frame
                            .as_ref()
                            .map(|window_frame| match window_frame.units {
                                ast::WindowFrameUnits::Rows => {
                                    yachtsql_ir::expr::WindowFrameUnits::Rows
                                }
                                ast::WindowFrameUnits::Range => {
                                    yachtsql_ir::expr::WindowFrameUnits::Range
                                }
                                ast::WindowFrameUnits::Groups => {
                                    yachtsql_ir::expr::WindowFrameUnits::Groups
                                }
                            });

                    let (frame_start_offset, frame_end_offset) =
                        if let Some(window_frame) = &spec.window_frame {
                            let start_offset = Self::parse_frame_bound(&window_frame.start_bound);
                            let end_offset = if let Some(ref end_bound) = window_frame.end_bound {
                                Self::parse_frame_bound(end_bound)
                            } else {
                                Some(0)
                            };
                            (start_offset, end_offset)
                        } else {
                            (None, Some(0))
                        };

                    let exclude = self.parse_exclude_from_current_sql();

                    return Ok(Expr::WindowFunction {
                        name,
                        args,
                        partition_by,
                        order_by,
                        frame_units,
                        frame_start_offset,
                        frame_end_offset,
                        exclude,
                        null_treatment: None,
                    });
                }

                let filter = if let Some(filter_expr) = &function.filter {
                    Some(Box::new(self.sql_expr_to_expr(filter_expr)?))
                } else {
                    None
                };

                let parameters = match &function.parameters {
                    ast::FunctionArguments::None => Vec::new(),
                    ast::FunctionArguments::Subquery(_) => Vec::new(),
                    ast::FunctionArguments::List(arg_list) => arg_list
                        .args
                        .iter()
                        .filter_map(|arg| match arg {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                                self.sql_expr_to_expr(e).ok()
                            }
                            _ => None,
                        })
                        .collect(),
                };

                let args = if !parameters.is_empty() {
                    let mut all_args = parameters;
                    all_args.extend(args);
                    all_args
                } else {
                    args
                };

                if name_str.eq_ignore_ascii_case("GROUPING") {
                    if args.len() != 1 {
                        return Err(Error::invalid_query(format!(
                            "GROUPING() requires exactly 1 argument, got {}",
                            args.len()
                        )));
                    }

                    let column_name = match &args[0] {
                        Expr::Column { name, .. } => name.clone(),
                        _ => {
                            return Err(Error::invalid_query(
                                "GROUPING() argument must be a column reference".to_string(),
                            ));
                        }
                    };
                    return Ok(Expr::Grouping {
                        column: column_name,
                    });
                }

                if name_str.eq_ignore_ascii_case("GROUPING_ID") {
                    if args.is_empty() {
                        return Err(Error::invalid_query(
                            "GROUPING_ID() requires at least 1 argument".to_string(),
                        ));
                    }

                    let mut columns = Vec::with_capacity(args.len());
                    for arg in &args {
                        match arg {
                            Expr::Column { name, .. } => columns.push(name.clone()),
                            _ => {
                                return Err(Error::invalid_query(
                                    "GROUPING_ID() arguments must be column references".to_string(),
                                ));
                            }
                        }
                    }
                    return Ok(Expr::GroupingId { columns });
                }

                if name.is_aggregate() && has_distinct {
                    Ok(Expr::Aggregate {
                        name,
                        args,
                        distinct: true,
                        order_by: order_by_clauses,
                        filter,
                    })
                } else if name.is_aggregate() {
                    Ok(Expr::Aggregate {
                        name,
                        args,
                        distinct: false,
                        order_by: order_by_clauses,
                        filter,
                    })
                } else {
                    Ok(Expr::Function { name, args })
                }
            }

            ast::Expr::Wildcard(_) => Ok(Expr::Wildcard),

            ast::Expr::IsNull(expr) => {
                let inner = self.sql_expr_to_expr(expr)?;
                Ok(Expr::unary_op(UnaryOp::IsNull, inner))
            }

            ast::Expr::IsNotNull(expr) => {
                let inner = self.sql_expr_to_expr(expr)?;
                Ok(Expr::unary_op(UnaryOp::IsNotNull, inner))
            }

            ast::Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => self.convert_case(operand, conditions, else_result),

            ast::Expr::InList {
                expr,
                list,
                negated,
            } => self.convert_in_list(expr, list, *negated),

            ast::Expr::InUnnest {
                expr,
                array_expr,
                negated,
            } => {
                let search_expr = self.sql_expr_to_expr(expr)?;
                let array = self.sql_expr_to_expr(array_expr)?;
                let contains = Expr::Function {
                    name: yachtsql_ir::FunctionName::ArrayContains,
                    args: vec![array, search_expr],
                };
                if *negated {
                    Ok(Expr::unary_op(UnaryOp::Not, contains))
                } else {
                    Ok(contains)
                }
            }

            ast::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => self.convert_between(expr, low, high, *negated),

            ast::Expr::Like {
                negated,
                expr,
                pattern,
                escape_char: _,
                any,
            } => {
                if *any {
                    self.convert_like_any_all_expr(expr, pattern, *negated, BinaryOp::Like, true)
                } else if let ast::Expr::Function(func) = pattern.as_ref() {
                    let is_all = func.name.0.len() == 1
                        && matches!(&func.name.0[0], ast::ObjectNamePart::Identifier(ident) if ident.value.eq_ignore_ascii_case("ALL"));
                    if is_all {
                        let all_args = &func.args;
                        if let ast::FunctionArguments::List(arg_list) = all_args {
                            let patterns: Vec<ast::Expr> = arg_list
                                .args
                                .iter()
                                .filter_map(|arg| {
                                    if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                        e,
                                    )) = arg
                                    {
                                        Some(e.clone())
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            let tuple_expr = ast::Expr::Tuple(patterns);
                            self.convert_like_any_all_expr(
                                expr,
                                &tuple_expr,
                                *negated,
                                BinaryOp::Like,
                                false,
                            )
                        } else {
                            self.convert_like_expr(
                                expr,
                                pattern,
                                *negated,
                                BinaryOp::Like,
                                BinaryOp::NotLike,
                            )
                        }
                    } else {
                        self.convert_like_expr(
                            expr,
                            pattern,
                            *negated,
                            BinaryOp::Like,
                            BinaryOp::NotLike,
                        )
                    }
                } else {
                    self.convert_like_expr(
                        expr,
                        pattern,
                        *negated,
                        BinaryOp::Like,
                        BinaryOp::NotLike,
                    )
                }
            }

            ast::Expr::ILike {
                negated,
                expr,
                pattern,
                escape_char: _,
                any,
            } => {
                if *any {
                    self.convert_like_any_all_expr(expr, pattern, *negated, BinaryOp::ILike, true)
                } else if let ast::Expr::Function(func) = pattern.as_ref() {
                    let is_all = func.name.0.len() == 1
                        && matches!(&func.name.0[0], ast::ObjectNamePart::Identifier(ident) if ident.value.eq_ignore_ascii_case("ALL"));
                    if is_all {
                        let all_args = &func.args;
                        if let ast::FunctionArguments::List(arg_list) = all_args {
                            let patterns: Vec<ast::Expr> = arg_list
                                .args
                                .iter()
                                .filter_map(|arg| {
                                    if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                        e,
                                    )) = arg
                                    {
                                        Some(e.clone())
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            let tuple_expr = ast::Expr::Tuple(patterns);
                            self.convert_like_any_all_expr(
                                expr,
                                &tuple_expr,
                                *negated,
                                BinaryOp::ILike,
                                false,
                            )
                        } else {
                            self.convert_like_expr(
                                expr,
                                pattern,
                                *negated,
                                BinaryOp::ILike,
                                BinaryOp::NotILike,
                            )
                        }
                    } else {
                        self.convert_like_expr(
                            expr,
                            pattern,
                            *negated,
                            BinaryOp::ILike,
                            BinaryOp::NotILike,
                        )
                    }
                } else {
                    self.convert_like_expr(
                        expr,
                        pattern,
                        *negated,
                        BinaryOp::ILike,
                        BinaryOp::NotILike,
                    )
                }
            }

            ast::Expr::SimilarTo {
                negated,
                expr,
                pattern,
                escape_char: _,
            } => self.convert_like_expr(
                expr,
                pattern,
                *negated,
                BinaryOp::SimilarTo,
                BinaryOp::NotSimilarTo,
            ),

            ast::Expr::Cast {
                kind,
                expr,
                data_type,
                ..
            } => self.convert_cast(expr, data_type, kind),

            ast::Expr::Subquery(query) => self.convert_scalar_subquery(query),

            ast::Expr::Exists { subquery, negated } => self.convert_exists(subquery, *negated),

            ast::Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => self.convert_in_subquery(expr, subquery, *negated),

            ast::Expr::Trim {
                expr,
                trim_where,
                trim_what,
                trim_characters,
            } => self.convert_trim(expr, trim_where, trim_what, trim_characters),

            ast::Expr::Position { expr, r#in } => self.convert_position(expr, r#in),

            ast::Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => self.convert_substring(expr, substring_from, substring_for),

            ast::Expr::Ceil { expr, .. } => {
                let arg_expr = self.sql_expr_to_expr(expr)?;
                Ok(Expr::Function {
                    name: yachtsql_ir::FunctionName::Ceil,
                    args: vec![arg_expr],
                })
            }

            ast::Expr::Floor { expr, field } => {
                let arg_expr = self.sql_expr_to_expr(expr)?;
                let args = match field {
                    ast::CeilFloorKind::Scale(ast::Value::Number(n, _)) => {
                        let scale_val = n.parse::<i64>().unwrap_or(0);
                        vec![
                            arg_expr,
                            Expr::Literal(yachtsql_ir::expr::LiteralValue::Int64(scale_val)),
                        ]
                    }
                    _ => vec![arg_expr],
                };
                Ok(Expr::Function {
                    name: yachtsql_ir::FunctionName::Floor,
                    args,
                })
            }

            ast::Expr::AnyOp {
                left,
                compare_op,
                right,
                is_some: _,
            } => self.convert_any_op(left, compare_op, right),

            ast::Expr::AllOp {
                left,
                compare_op,
                right,
            } => self.convert_all_op(left, compare_op, right),

            ast::Expr::Interval(interval) => self.convert_interval(interval),

            ast::Expr::Extract {
                field,
                syntax: _,
                expr: extract_expr,
            } => self.convert_extract(field, extract_expr.as_ref()),

            ast::Expr::Array(ast::Array { elem, .. }) => {
                let elements = elem
                    .iter()
                    .map(|e| self.sql_expr_to_expr(e))
                    .collect::<Result<Vec<_>>>()?;

                self.validate_array_type_homogeneity(&elements)?;

                Ok(Expr::Literal(LiteralValue::Array(elements)))
            }

            ast::Expr::Struct { values, fields } => {
                let struct_fields = self.build_struct_literal_fields(values, fields)?;
                Ok(Expr::StructLiteral {
                    fields: struct_fields,
                })
            }

            ast::Expr::Named { expr, .. } => self.sql_expr_to_expr(expr),

            ast::Expr::Tuple(exprs) => {
                let tuple_exprs = exprs
                    .iter()
                    .map(|e| self.sql_expr_to_expr(e))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::Tuple(tuple_exprs))
            }

            ast::Expr::IsDistinctFrom(left, right) => {
                let left_expr = self.sql_expr_to_expr(left)?;
                let right_expr = self.sql_expr_to_expr(right)?;
                Ok(Expr::IsDistinctFrom {
                    left: Box::new(left_expr),
                    right: Box::new(right_expr),
                    negated: false,
                })
            }

            ast::Expr::IsNotDistinctFrom(left, right) => {
                let left_expr = self.sql_expr_to_expr(left)?;
                let right_expr = self.sql_expr_to_expr(right)?;
                Ok(Expr::IsDistinctFrom {
                    left: Box::new(left_expr),
                    right: Box::new(right_expr),
                    negated: true,
                })
            }

            ast::Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => {
                let ts_expr = self.sql_expr_to_expr(timestamp)?;
                let tz_expr = self.sql_expr_to_expr(time_zone)?;
                Ok(Expr::Function {
                    name: yachtsql_ir::FunctionName::parse("AT_TIME_ZONE"),
                    args: vec![ts_expr, tz_expr],
                })
            }

            ast::Expr::Lambda(lambda) => {
                let params: Vec<String> = match &lambda.params {
                    ast::OneOrManyWithParens::One(ident) => vec![ident.value.clone()],
                    ast::OneOrManyWithParens::Many(idents) => {
                        idents.iter().map(|i| i.value.clone()).collect()
                    }
                };
                let body = self.sql_expr_to_expr(&lambda.body)?;
                Ok(Expr::Lambda {
                    params,
                    body: Box::new(body),
                })
            }

            ast::Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                let string_expr = self.sql_expr_to_expr(expr)?;
                let replacement_expr = self.sql_expr_to_expr(overlay_what)?;
                let start_expr = self.sql_expr_to_expr(overlay_from)?;
                let mut args = vec![string_expr, replacement_expr, start_expr];
                if let Some(length_expr) = overlay_for {
                    args.push(self.sql_expr_to_expr(length_expr)?);
                }
                Ok(Expr::Function {
                    name: yachtsql_ir::FunctionName::parse("OVERLAY"),
                    args,
                })
            }

            _ => Err(Error::unsupported_feature(format!(
                "Expression type not supported: {:?}",
                expr
            ))),
        }
    }

    fn validate_array_type_homogeneity(&self, elements: &[Expr]) -> Result<()> {
        if elements.is_empty() {
            return Ok(());
        }

        let first_type = elements
            .iter()
            .find_map(|elem| Self::get_literal_type_category(elem));

        if let Some(expected_type) = first_type {
            for elem in elements {
                if let Some(elem_type) = Self::get_literal_type_category(elem)
                    && elem_type != expected_type
                {
                    return Err(Error::invalid_query(format!(
                        "Array elements must have compatible types, found both {:?} and {:?}",
                        expected_type, elem_type
                    )));
                }
            }
        }

        Ok(())
    }

    fn get_literal_type_category(expr: &Expr) -> Option<&'static str> {
        match expr {
            Expr::Literal(lit) => match lit {
                LiteralValue::Null => None,
                LiteralValue::Int64(_) => Some("integer"),
                LiteralValue::Float64(_) | LiteralValue::Numeric(_) => Some("numeric"),
                LiteralValue::String(_) => Some("string"),
                LiteralValue::Boolean(_) => Some("boolean"),
                LiteralValue::Date(_) => Some("date"),
                LiteralValue::Time(_) => Some("time"),
                LiteralValue::DateTime(_) => Some("datetime"),
                LiteralValue::Timestamp(_) => Some("timestamp"),
                LiteralValue::Interval(_) => Some("interval"),
                LiteralValue::Array(_) => Some("array"),
                LiteralValue::Bytes(_) => Some("bytes"),
                LiteralValue::Uuid(_) => Some("uuid"),
                LiteralValue::Json(_) => Some("json"),
                LiteralValue::Vector(_) => Some("vector"),
                LiteralValue::Range(_) => Some("range"),
                LiteralValue::Point(_) => Some("point"),
                LiteralValue::PgBox(_) => Some("box"),
                LiteralValue::Circle(_) => Some("circle"),
                LiteralValue::Line(_) => Some("line"),
                LiteralValue::Lseg(_) => Some("lseg"),
                LiteralValue::Path(_) => Some("path"),
                LiteralValue::Polygon(_) => Some("polygon"),
                LiteralValue::MacAddr(_) => Some("macaddr"),
                LiteralValue::MacAddr8(_) => Some("macaddr8"),
            },

            _ => None,
        }
    }

    fn convert_date_part_column_to_string(expr: &Expr) -> Expr {
        if let Expr::Column { name, table: None } = expr {
            let upper = name.to_uppercase();
            let date_parts = [
                "YEAR",
                "MONTH",
                "DAY",
                "HOUR",
                "MINUTE",
                "SECOND",
                "WEEK",
                "QUARTER",
                "DAYOFWEEK",
                "DAYOFYEAR",
                "MILLISECOND",
                "MICROSECOND",
                "NANOSECOND",
                "ISOWEEK",
                "ISOYEAR",
            ];
            if date_parts.contains(&upper.as_str()) {
                return Expr::Literal(LiteralValue::String(upper));
            }
        }
        expr.clone()
    }

    fn convert_normalization_form_column_to_string(expr: &Expr) -> Expr {
        if let Expr::Column { name, table: None } = expr {
            let upper = name.to_uppercase();
            let forms = ["NFC", "NFD", "NFKC", "NFKD"];
            if forms.contains(&upper.as_str()) {
                return Expr::Literal(LiteralValue::String(upper));
            }
        }
        expr.clone()
    }

    fn expand_udf_call(
        &self,
        name_str: &str,
        args: &[Expr],
        _func: &sqlparser::ast::Function,
    ) -> Result<Option<Expr>> {
        let udf = match self.get_udf(name_str) {
            Some(udf) => udf.clone(),
            None => return Ok(None),
        };

        if args.len() != udf.parameters.len() {
            return Err(Error::invalid_query(format!(
                "UDF {} expects {} arguments, got {}",
                name_str,
                udf.parameters.len(),
                args.len()
            )));
        }

        let param_map: std::collections::HashMap<String, Expr> = udf
            .parameters
            .iter()
            .zip(args.iter())
            .map(|(param, arg)| (param.to_uppercase(), arg.clone()))
            .collect();

        let body_expr = self.sql_expr_to_expr(&udf.body)?;

        let expanded = Self::substitute_udf_params(body_expr, &param_map);
        let expanded = Self::apply_struct_return_type_field_names(expanded, &udf.return_type);
        Ok(Some(expanded))
    }

    fn apply_struct_return_type_field_names(
        expr: Expr,
        return_type: &Option<sqlparser::ast::DataType>,
    ) -> Expr {
        use yachtsql_ir::expr::StructLiteralField;

        let struct_fields = match return_type {
            Some(sqlparser::ast::DataType::Struct(fields, _)) => fields,
            _ => return expr,
        };

        match expr {
            Expr::StructLiteral { fields } if fields.len() == struct_fields.len() => {
                let renamed_fields: Vec<StructLiteralField> = fields
                    .into_iter()
                    .zip(struct_fields.iter())
                    .map(|(mut field, type_field)| {
                        if let Some(ref name_ident) = type_field.field_name {
                            field.name = name_ident.value.clone();
                        }
                        field
                    })
                    .collect();
                Expr::StructLiteral {
                    fields: renamed_fields,
                }
            }
            _ => expr,
        }
    }

    fn substitute_udf_params(expr: Expr, params: &std::collections::HashMap<String, Expr>) -> Expr {
        match expr {
            Expr::Column {
                ref name,
                table: None,
            } => {
                let name_upper = name.to_uppercase();
                if let Some(replacement) = params.get(&name_upper) {
                    return replacement.clone();
                }
                expr
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::substitute_udf_params(*left, params)),
                op,
                right: Box::new(Self::substitute_udf_params(*right, params)),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op,
                expr: Box::new(Self::substitute_udf_params(*inner, params)),
            },
            Expr::Function { name, args } => Expr::Function {
                name,
                args: args
                    .into_iter()
                    .map(|a| Self::substitute_udf_params(a, params))
                    .collect(),
            },
            Expr::Cast {
                expr: inner,
                data_type,
            } => Expr::Cast {
                expr: Box::new(Self::substitute_udf_params(*inner, params)),
                data_type,
            },
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => Expr::Case {
                operand: operand.map(|o| Box::new(Self::substitute_udf_params(*o, params))),
                when_then: when_then
                    .into_iter()
                    .map(|(w, t)| {
                        (
                            Self::substitute_udf_params(w, params),
                            Self::substitute_udf_params(t, params),
                        )
                    })
                    .collect(),
                else_expr: else_expr.map(|e| Box::new(Self::substitute_udf_params(*e, params))),
            },
            Expr::StructLiteral { fields } => {
                use yachtsql_ir::expr::StructLiteralField;
                let substituted_fields: Vec<StructLiteralField> = fields
                    .into_iter()
                    .map(|f| StructLiteralField {
                        name: f.name,
                        expr: Self::substitute_udf_params(f.expr, params),
                        declared_type: f.declared_type,
                    })
                    .collect();
                Expr::StructLiteral {
                    fields: substituted_fields,
                }
            }
            _ => expr,
        }
    }

    fn normalize_interval_arg(expr: &Expr) -> Expr {
        expr.clone()
    }

    fn normalize_dialect_function(&self, name_str: &str, args: &[Expr]) -> Result<Option<Expr>> {
        use yachtsql_ir::expr::{BinaryOp, CastDataType, LiteralValue, StructLiteralField};

        use crate::DialectType;

        if name_str.to_uppercase().as_str() == "ROW" {
            let fields: Vec<StructLiteralField> = args
                .iter()
                .enumerate()
                .map(|(idx, expr)| StructLiteralField {
                    name: format!("f{}", idx + 1),
                    expr: expr.clone(),
                    declared_type: None,
                })
                .collect();

            return Ok(Some(Expr::StructLiteral { fields }));
        }

        let normalized = match self.dialect {
            DialectType::BigQuery => match name_str.to_uppercase().as_str() {
                "DATE_TRUNC" => {
                    if args.len() != 2 {
                        return Err(Error::invalid_query(format!(
                            "DATE_TRUNC requires exactly 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let date_expr = args[0].clone();
                    let unit_expr = Self::convert_date_part_column_to_string(&args[1]);
                    Some(Expr::Function {
                        name: yachtsql_ir::FunctionName::DateTrunc,
                        args: vec![unit_expr, date_expr],
                    })
                }
                "TIMESTAMP_TRUNC" => {
                    if args.len() < 2 {
                        return Err(Error::invalid_query(format!(
                            "TIMESTAMP_TRUNC requires at least 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let ts_expr = args[0].clone();
                    let unit_expr = Self::convert_date_part_column_to_string(&args[1]);
                    let mut new_args = vec![ts_expr, unit_expr];
                    if args.len() > 2 {
                        new_args.extend(args[2..].iter().cloned());
                    }
                    Some(Expr::Function {
                        name: yachtsql_ir::FunctionName::TimestampTrunc,
                        args: new_args,
                    })
                }
                "DATE_ADD" | "DATE_SUB" => {
                    if args.len() != 2 {
                        return Err(Error::invalid_query(format!(
                            "{} requires exactly 2 arguments, got {}",
                            name_str.to_uppercase(),
                            args.len()
                        )));
                    }
                    let date_expr = args[0].clone();
                    let interval_expr = Self::normalize_interval_arg(&args[1]);
                    let name = if name_str.to_uppercase() == "DATE_ADD" {
                        yachtsql_ir::FunctionName::DateAdd
                    } else {
                        yachtsql_ir::FunctionName::DateSub
                    };
                    Some(Expr::Function {
                        name,
                        args: vec![date_expr, interval_expr],
                    })
                }
                "DATE_DIFF" => {
                    if args.len() != 3 {
                        return Err(Error::invalid_query(format!(
                            "DATE_DIFF requires exactly 3 arguments, got {}",
                            args.len()
                        )));
                    }
                    let date1 = args[0].clone();
                    let date2 = args[1].clone();
                    let unit_expr = Self::convert_date_part_column_to_string(&args[2]);
                    Some(Expr::Function {
                        name: yachtsql_ir::FunctionName::DateDiff,
                        args: vec![date1, date2, unit_expr],
                    })
                }
                "TIMESTAMP_DIFF" => {
                    if args.len() != 3 {
                        return Err(Error::invalid_query(format!(
                            "TIMESTAMP_DIFF requires exactly 3 arguments, got {}",
                            args.len()
                        )));
                    }
                    let ts1 = args[0].clone();
                    let ts2 = args[1].clone();
                    let unit_expr = Self::convert_date_part_column_to_string(&args[2]);
                    Some(Expr::Function {
                        name: yachtsql_ir::FunctionName::TimestampDiff,
                        args: vec![ts1, ts2, unit_expr],
                    })
                }
                "DATETIME_ADD" => {
                    if args.len() != 2 {
                        return Err(Error::invalid_query(format!(
                            "DATETIME_ADD requires exactly 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let dt_expr = args[0].clone();
                    let interval_expr = Self::normalize_interval_arg(&args[1]);
                    Some(Expr::Function {
                        name: yachtsql_ir::FunctionName::DatetimeAdd,
                        args: vec![dt_expr, interval_expr],
                    })
                }
                "DATETIME_SUB" => {
                    if args.len() != 2 {
                        return Err(Error::invalid_query(format!(
                            "DATETIME_SUB requires exactly 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let dt_expr = args[0].clone();
                    let interval_expr = Self::normalize_interval_arg(&args[1]);
                    Some(Expr::Function {
                        name: yachtsql_ir::FunctionName::DatetimeSub,
                        args: vec![dt_expr, interval_expr],
                    })
                }
                "DATETIME_DIFF" => {
                    if args.len() != 3 {
                        return Err(Error::invalid_query(format!(
                            "DATETIME_DIFF requires exactly 3 arguments, got {}",
                            args.len()
                        )));
                    }
                    let dt1 = args[0].clone();
                    let dt2 = args[1].clone();
                    let unit_expr = Self::convert_date_part_column_to_string(&args[2]);
                    Some(Expr::Function {
                        name: yachtsql_ir::FunctionName::DatetimeDiff,
                        args: vec![dt1, dt2, unit_expr],
                    })
                }
                "DATETIME_TRUNC" => {
                    if args.len() != 2 {
                        return Err(Error::invalid_query(format!(
                            "DATETIME_TRUNC requires exactly 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let dt_expr = args[0].clone();
                    let unit_expr = Self::convert_date_part_column_to_string(&args[1]);
                    Some(Expr::Function {
                        name: yachtsql_ir::FunctionName::DatetimeTrunc,
                        args: vec![dt_expr, unit_expr],
                    })
                }
                "TIME_ADD" => {
                    if args.len() != 2 {
                        return Err(Error::invalid_query(format!(
                            "TIME_ADD requires exactly 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let time_expr = args[0].clone();
                    let interval_expr = Self::normalize_interval_arg(&args[1]);
                    Some(Expr::Function {
                        name: yachtsql_ir::FunctionName::TimeAdd,
                        args: vec![time_expr, interval_expr],
                    })
                }
                "TIME_SUB" => {
                    if args.len() != 2 {
                        return Err(Error::invalid_query(format!(
                            "TIME_SUB requires exactly 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let time_expr = args[0].clone();
                    let interval_expr = Self::normalize_interval_arg(&args[1]);
                    Some(Expr::Function {
                        name: yachtsql_ir::FunctionName::TimeSub,
                        args: vec![time_expr, interval_expr],
                    })
                }
                "TIME_DIFF" => {
                    if args.len() != 3 {
                        return Err(Error::invalid_query(format!(
                            "TIME_DIFF requires exactly 3 arguments, got {}",
                            args.len()
                        )));
                    }
                    let time1 = args[0].clone();
                    let time2 = args[1].clone();
                    let unit_expr = Self::convert_date_part_column_to_string(&args[2]);
                    Some(Expr::Function {
                        name: yachtsql_ir::FunctionName::TimeDiff,
                        args: vec![time1, time2, unit_expr],
                    })
                }
                "TIME_TRUNC" => {
                    if args.len() != 2 {
                        return Err(Error::invalid_query(format!(
                            "TIME_TRUNC requires exactly 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let time_expr = args[0].clone();
                    let unit_expr = Self::convert_date_part_column_to_string(&args[1]);
                    Some(Expr::Function {
                        name: yachtsql_ir::FunctionName::TimeTrunc,
                        args: vec![time_expr, unit_expr],
                    })
                }
                "SAFE_DIVIDE" => {
                    if args.len() != 2 {
                        return Err(Error::invalid_query(format!(
                            "SAFE_DIVIDE requires exactly 2 arguments, got {}",
                            args.len()
                        )));
                    }

                    let numerator = args[0].clone();
                    let denominator = args[1].clone();

                    Some(Expr::Case {
                        operand: None,
                        when_then: vec![(
                            Expr::BinaryOp {
                                left: Box::new(denominator.clone()),
                                op: BinaryOp::Equal,
                                right: Box::new(Expr::Literal(LiteralValue::Int64(0))),
                            },
                            Expr::Literal(LiteralValue::Null),
                        )],
                        else_expr: Some(Box::new(Expr::BinaryOp {
                            left: Box::new(numerator),
                            op: BinaryOp::Divide,
                            right: Box::new(denominator),
                        })),
                    })
                }
                _ => None,
            },
            DialectType::ClickHouse => match name_str.to_uppercase().as_str() {
                "TO_INT64" | "TOINT64" => {
                    if args.len() != 1 {
                        return Err(Error::invalid_query(format!(
                            "TO_INT64 requires exactly 1 argument, got {}",
                            args.len()
                        )));
                    }
                    Some(Expr::Cast {
                        expr: Box::new(args[0].clone()),
                        data_type: CastDataType::Int64,
                    })
                }
                "TO_STRING" | "TOSTRING" => {
                    if args.len() != 1 {
                        return Err(Error::invalid_query(format!(
                            "TO_STRING requires exactly 1 argument, got {}",
                            args.len()
                        )));
                    }
                    Some(Expr::Cast {
                        expr: Box::new(args[0].clone()),
                        data_type: CastDataType::String,
                    })
                }
                "TO_FLOAT64" | "TOFLOAT64" => {
                    if args.len() != 1 {
                        return Err(Error::invalid_query(format!(
                            "TO_FLOAT64 requires exactly 1 argument, got {}",
                            args.len()
                        )));
                    }
                    Some(Expr::Cast {
                        expr: Box::new(args[0].clone()),
                        data_type: CastDataType::Float64,
                    })
                }
                "IF" => {
                    if args.len() != 3 {
                        return Err(Error::invalid_query(format!(
                            "IF requires exactly 3 arguments, got {}",
                            args.len()
                        )));
                    }

                    Some(Expr::Case {
                        operand: None,
                        when_then: vec![(args[0].clone(), args[1].clone())],
                        else_expr: Some(Box::new(args[2].clone())),
                    })
                }
                _ => None,
            },
            DialectType::PostgreSQL => match name_str.to_uppercase().as_str() {
                "NORMALIZE" | "IS_NORMALIZED" => {
                    if args.len() == 2 {
                        let text_arg = args[0].clone();
                        let form_arg = Self::convert_normalization_form_column_to_string(&args[1]);
                        Some(Expr::Function {
                            name: yachtsql_ir::FunctionName::parse(name_str),
                            args: vec![text_arg, form_arg],
                        })
                    } else {
                        None
                    }
                }
                _ => None,
            },
        };

        Ok(normalized)
    }
}
