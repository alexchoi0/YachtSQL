use std::collections::HashMap;

use sqlparser::ast::{self, CteAsMaterialized};
use yachtsql_core::error::{Error, Result};
use yachtsql_ir::expr::{Expr, LiteralValue};
use yachtsql_ir::plan::{JoinType, LogicalPlan, PlanNode};

use super::{AliasScopeGuard, GroupingExtension, LogicalPlanBuilder};

impl LogicalPlanBuilder {
    fn detect_union_all_in_cte(plan: &PlanNode) -> bool {
        match plan {
            PlanNode::Union { all, .. } => *all,
            _ => false,
        }
    }

    pub fn query_to_plan(&self, query: &ast::Query) -> Result<LogicalPlan> {
        let distinct_on_exprs = if let ast::SetExpr::Select(select) = query.body.as_ref() {
            if let Some(ast::Distinct::On(ref exprs)) = select.distinct {
                Some(
                    exprs
                        .iter()
                        .map(|e| self.sql_expr_to_expr(e))
                        .collect::<Result<Vec<_>>>()?,
                )
            } else {
                None
            }
        } else {
            None
        };

        let plan = if let Some(with) = &query.with {
            let mut current_plan = self.set_expr_to_plan(query.body.as_ref())?;

            for cte in with.cte_tables.iter().rev() {
                let cte_name = cte.alias.name.value.clone();

                if with.recursive && cte.materialized == Some(CteAsMaterialized::NotMaterialized) {
                    return Err(Error::recursive_cte_not_materialized(&cte_name));
                }

                let cte_plan = self.query_to_plan(&cte.query)?;

                let use_union_all = if with.recursive {
                    Self::detect_union_all_in_cte(cte_plan.root.as_ref())
                } else {
                    false
                };

                current_plan = LogicalPlan::new(PlanNode::Cte {
                    name: cte_name,
                    cte_plan: cte_plan.root,
                    input: current_plan.root,
                    recursive: with.recursive,
                    use_union_all,
                    materialization_hint: cte.materialized.clone(),
                });
            }

            current_plan
        } else {
            self.set_expr_to_plan(query.body.as_ref())?
        };

        let mut plan = plan;
        let mut order_by_exprs = if let Some(order_by_clause) = &query.order_by {
            Some(self.convert_order_by_exprs(order_by_clause)?)
        } else {
            None
        };

        if let Some(ref sort_exprs) = order_by_exprs {
            if let Some(ref distinct_exprs) = distinct_on_exprs {
                if sort_exprs.is_empty() {
                    return Err(Error::invalid_query(
                        "SELECT DISTINCT ON requires ORDER BY clause".to_string(),
                    ));
                }

                for (i, distinct_expr) in distinct_exprs.iter().enumerate() {
                    if i >= sort_exprs.len() {
                        return Err(Error::invalid_query(
                            "SELECT DISTINCT ON expressions must match leftmost ORDER BY expressions"
                                .to_string(),
                        ));
                    }

                    if distinct_expr != &sort_exprs[i].expr {
                        return Err(Error::invalid_query(format!(
                            "SELECT DISTINCT ON expression does not match ORDER BY expression at position {}",
                            i + 1
                        )));
                    }
                }
            }
        } else if distinct_on_exprs.is_some() {
            return Err(Error::invalid_query(
                "SELECT DISTINCT ON requires ORDER BY clause".to_string(),
            ));
        }

        if let Some(sort_exprs) = order_by_exprs.take() {
            let (modified_plan, original_projection) =
                self.inject_order_by_columns_into_projection(&plan, &sort_exprs);

            plan = LogicalPlan::new(PlanNode::Sort {
                order_by: sort_exprs,
                input: modified_plan.root,
            });

            if let Some(orig_exprs) = original_projection {
                let final_exprs: Vec<(Expr, Option<String>)> = orig_exprs
                    .iter()
                    .enumerate()
                    .map(|(idx, (expr, alias))| {
                        let col_name = if let Some(alias_name) = alias {
                            alias_name.clone()
                        } else {
                            match expr {
                                Expr::Column { name, .. } => name.clone(),
                                _ => format!("expr_{}", idx),
                            }
                        };
                        (
                            Expr::Column {
                                name: col_name,
                                table: None,
                            },
                            alias.clone(),
                        )
                    })
                    .collect();
                plan = LogicalPlan::new(PlanNode::Projection {
                    expressions: final_exprs,
                    input: plan.root,
                });
            }
        }

        if let Some(distinct_exprs) = distinct_on_exprs {
            plan = LogicalPlan::new(PlanNode::DistinctOn {
                expressions: distinct_exprs,
                input: plan.root,
            });
        }

        let (limit_val, offset_val) = self.parse_limit_clause(&query.limit_clause)?;

        if limit_val.is_some() || offset_val > 0 {
            plan = LogicalPlan::new(PlanNode::Limit {
                limit: limit_val.unwrap_or(usize::MAX),
                offset: offset_val,
                input: plan.root,
            });
        }

        Ok(plan)
    }

    fn set_expr_to_plan(&self, set_expr: &ast::SetExpr) -> Result<LogicalPlan> {
        match set_expr {
            ast::SetExpr::Select(select) => self.select_to_plan(select),
            ast::SetExpr::Query(query) => self.query_to_plan(query),
            ast::SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                let left_plan = self.set_expr_to_plan(left)?;
                let right_plan = self.set_expr_to_plan(right)?;

                let all = matches!(set_quantifier, ast::SetQuantifier::All);

                let node = match op {
                    ast::SetOperator::Union => PlanNode::Union {
                        left: left_plan.root,
                        right: right_plan.root,
                        all,
                    },
                    ast::SetOperator::Intersect => PlanNode::Intersect {
                        left: left_plan.root,
                        right: right_plan.root,
                        all,
                    },
                    ast::SetOperator::Except | ast::SetOperator::Minus => PlanNode::Except {
                        left: left_plan.root,
                        right: right_plan.root,
                        all,
                    },
                };

                Ok(LogicalPlan::new(node))
            }
            _ => Err(Error::unsupported_feature(format!(
                "Set expression not supported: {:?}",
                set_expr
            ))),
        }
    }

    fn select_to_plan(&self, select: &ast::Select) -> Result<LogicalPlan> {
        let from_plan = self.tables_to_plan(&select.from)?;

        let mut plan = from_plan;
        let alias_scope = self.collect_table_aliases(&select.from);
        let _alias_guard = AliasScopeGuard::new(self, alias_scope);

        if !select.named_window.is_empty() {
            let mut window_map = HashMap::new();
            for named_window_def in &select.named_window {
                let window_spec = self.resolve_named_window_expr(&named_window_def.1)?;
                let name_str = named_window_def.0.value.clone();

                if window_map.contains_key(&name_str) {
                    return Err(Error::invalid_query(format!(
                        "Duplicate window name '{}' in WINDOW clause",
                        name_str
                    )));
                }

                window_map.insert(name_str, window_spec);
            }
            self.set_named_windows(window_map);
        } else {
            self.clear_named_windows();
        }

        let has_distinct = matches!(select.distinct, Some(ast::Distinct::Distinct));

        if let Some(selection) = &select.selection {
            let predicate = self.sql_expr_to_expr(selection)?;
            plan = LogicalPlan::new(PlanNode::Filter {
                predicate,
                input: plan.root,
            });
        }

        let projection_exprs = select
            .projection
            .iter()
            .map(|item| self.select_item_to_expr(item))
            .collect::<Result<Vec<_>>>()?;

        let has_window_functions = projection_exprs
            .iter()
            .any(|(expr, _)| self.has_window_function(expr));

        let (group_by_exprs, _has_rollup, _has_cube, _has_grouping_sets, _explicit_grouping_sets): (
            Vec<Expr>,
            bool,
            bool,
            bool,
            Vec<Vec<Expr>>,
        ) = if let ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
            let mut all_group_cols = Vec::new();
            let mut is_rollup = false;
            let mut is_cube = false;
            let mut is_grouping_sets = false;
            let mut explicit_grouping_sets: Vec<Vec<Expr>> = Vec::new();

            for ast_expr in exprs {

                if let Some(ext) = self.extract_grouping_extension(ast_expr)? {

                    is_rollup |= ext.is_rollup;
                    is_cube |= ext.is_cube;
                    is_grouping_sets |= ext.is_grouping_sets;
                    for col in ext.columns {
                        all_group_cols.push(self.sql_expr_to_expr(&col)?);
                    }

                    if ext.is_grouping_sets && !ext.explicit_sets.is_empty() {
                        for set in ext.explicit_sets {
                            let converted_set: Result<Vec<Expr>> = set.iter()
                                .map(|e| self.sql_expr_to_expr(e))
                                .collect();
                            explicit_grouping_sets.push(converted_set?);
                        }
                    }
                } else {

                    all_group_cols.push(self.sql_expr_to_expr(ast_expr)?);
                }
            }

            (all_group_cols, is_rollup, is_cube, is_grouping_sets, explicit_grouping_sets)
        } else {
            (vec![], false, false, false, Vec::new())
        };

        if select.having.is_some() && group_by_exprs.is_empty() {
            return Err(Error::invalid_query(
                "HAVING clause requires GROUP BY".to_string(),
            ));
        }

        let has_aggregates = projection_exprs
            .iter()
            .any(|(expr, _)| self.has_aggregate(expr));

        if has_aggregates || !group_by_exprs.is_empty() {
            let mut all_aggregates: Vec<Expr> = Vec::new();
            for (expr, _alias) in &projection_exprs {
                self.collect_aggregates_from_expr(expr, &mut all_aggregates);
            }

            if let Some(having_expr) = &select.having {
                let having_pred = self.sql_expr_to_expr(having_expr)?;
                self.collect_aggregates_from_expr(&having_pred, &mut all_aggregates);
            }

            if _has_rollup || _has_cube || _has_grouping_sets {
                plan = self.build_rollup_cube_plan(
                    plan,
                    &group_by_exprs,
                    all_aggregates.clone(),
                    _has_rollup,
                    _has_cube,
                    _has_grouping_sets,
                    &_explicit_grouping_sets,
                )?;
            } else {
                plan = LogicalPlan::new(PlanNode::Aggregate {
                    group_by: group_by_exprs.clone(),
                    aggregates: all_aggregates,
                    input: plan.root,
                    grouping_metadata: None,
                });
            }

            if let Some(having_expr) = &select.having {
                let having_predicate = self.sql_expr_to_expr(having_expr)?;

                let having_predicate_rewritten =
                    self.rewrite_post_aggregate_expr(&having_predicate, &group_by_exprs);
                plan = LogicalPlan::new(PlanNode::Filter {
                    predicate: having_predicate_rewritten,
                    input: plan.root,
                });
            }

            if has_window_functions {
                let mut all_windows: Vec<(Expr, String)> = Vec::new();
                let mut counter = 0usize;

                for (expr, alias) in &projection_exprs {
                    if self.is_window_function(expr) {
                        let rewritten_window = self.rewrite_window_order_by_aggregates(expr);
                        let col_name = alias.clone().unwrap_or_else(|| match expr {
                            Expr::WindowFunction { name, .. } => format!("{}(...)", name.as_str()),
                            _ => "window_result".to_string(),
                        });
                        all_windows.push((rewritten_window, col_name));
                    } else if self.has_window_function(expr) {
                        self.extract_window_functions_with_aggregate_rewrite(
                            expr,
                            &mut all_windows,
                            &mut counter,
                        );
                    }
                }

                if !all_windows.is_empty() {
                    let window_exprs: Vec<(Expr, Option<String>)> = all_windows
                        .iter()
                        .map(|(expr, col_name)| (expr.clone(), Some(col_name.clone())))
                        .collect();

                    plan = LogicalPlan::new(PlanNode::Window {
                        window_exprs,
                        input: plan.root,
                    });
                }

                let final_projection: Vec<(Expr, Option<String>)> = projection_exprs
                    .iter()
                    .map(|(expr, alias)| {
                        if let Some(group_expr_ref) =
                            self.rewrite_group_by_expr_to_column(expr, &group_by_exprs)
                        {
                            return (group_expr_ref, alias.clone());
                        }

                        if self.is_window_function(expr) {
                            let col_name = alias.clone().unwrap_or_else(|| match expr {
                                Expr::WindowFunction { name, .. } => {
                                    format!("{}(...)", name.as_str())
                                }
                                _ => "window_result".to_string(),
                            });
                            return (
                                Expr::Column {
                                    name: col_name,
                                    table: None,
                                },
                                alias.clone(),
                            );
                        }

                        if self.has_window_function(expr) {
                            let replaced = self.replace_window_functions(expr, &all_windows);
                            return (replaced, alias.clone());
                        }

                        match expr {
                            Expr::Function { name, args } if self.is_aggregate(expr) => {
                                let first_is_wildcard =
                                    args.first().is_some_and(|expr| expr.is_wildcard());
                                let col_name = if args.is_empty() || first_is_wildcard {
                                    format!("{}(*)", name.as_str())
                                } else if let Some(Expr::Column { name: col_name, .. }) =
                                    args.first()
                                {
                                    format!("{}({})", name.as_str(), col_name)
                                } else {
                                    format!("{}(...)", name.as_str())
                                };
                                (
                                    Expr::Column {
                                        name: col_name,
                                        table: None,
                                    },
                                    alias.clone(),
                                )
                            }
                            Expr::Aggregate {
                                name,
                                args,
                                distinct,
                                ..
                            } => {
                                let first_is_wildcard =
                                    args.first().is_some_and(|expr| expr.is_wildcard());
                                let col_name = if args.is_empty() || first_is_wildcard {
                                    if *distinct {
                                        format!("{}(DISTINCT *)", name.as_str())
                                    } else {
                                        format!("{}(*)", name.as_str())
                                    }
                                } else if let Some(Expr::Column { name: col_name, .. }) =
                                    args.first()
                                {
                                    if *distinct {
                                        format!("{}(DISTINCT {})", name.as_str(), col_name)
                                    } else {
                                        format!("{}({})", name.as_str(), col_name)
                                    }
                                } else if *distinct {
                                    format!("{}(DISTINCT ...)", name.as_str())
                                } else {
                                    format!("{}(...)", name.as_str())
                                };
                                (
                                    Expr::Column {
                                        name: col_name,
                                        table: None,
                                    },
                                    alias.clone(),
                                )
                            }

                            _ => (
                                self.rewrite_post_aggregate_expr(expr, &group_by_exprs),
                                alias.clone(),
                            ),
                        }
                    })
                    .collect();

                plan = LogicalPlan::new(PlanNode::Projection {
                    expressions: final_projection,
                    input: plan.root,
                });
            } else {
                let final_projection: Vec<(Expr, Option<String>)> = projection_exprs
                    .iter()
                    .map(|(expr, alias)| {
                        if let Some(group_expr_ref) =
                            self.rewrite_group_by_expr_to_column(expr, &group_by_exprs)
                        {
                            return (group_expr_ref, alias.clone());
                        }

                        match expr {
                            Expr::Function { name, args } if self.is_aggregate(expr) => {
                                let first_is_wildcard =
                                    args.first().is_some_and(|expr| expr.is_wildcard());
                                let col_name = if args.is_empty() || first_is_wildcard {
                                    format!("{}(*)", name.as_str())
                                } else if let Some(Expr::Column { name: col_name, .. }) =
                                    args.first()
                                {
                                    format!("{}({})", name.as_str(), col_name)
                                } else {
                                    format!("{}(...)", name.as_str())
                                };
                                (
                                    Expr::Column {
                                        name: col_name,
                                        table: None,
                                    },
                                    alias.clone(),
                                )
                            }
                            Expr::Aggregate {
                                name,
                                args,
                                distinct,
                                ..
                            } => {
                                let first_is_wildcard =
                                    args.first().is_some_and(|expr| expr.is_wildcard());
                                let col_name = if args.is_empty() || first_is_wildcard {
                                    if *distinct {
                                        format!("{}(DISTINCT *)", name.as_str())
                                    } else {
                                        format!("{}(*)", name.as_str())
                                    }
                                } else if let Some(Expr::Column { name: col_name, .. }) =
                                    args.first()
                                {
                                    if *distinct {
                                        format!("{}(DISTINCT {})", name.as_str(), col_name)
                                    } else {
                                        format!("{}({})", name.as_str(), col_name)
                                    }
                                } else if *distinct {
                                    format!("{}(DISTINCT ...)", name.as_str())
                                } else {
                                    format!("{}(...)", name.as_str())
                                };
                                (
                                    Expr::Column {
                                        name: col_name,
                                        table: None,
                                    },
                                    alias.clone(),
                                )
                            }

                            _ => (
                                self.rewrite_post_aggregate_expr(expr, &group_by_exprs),
                                alias.clone(),
                            ),
                        }
                    })
                    .collect();

                if final_projection.iter().any(|(_, alias)| alias.is_some()) {
                    plan = LogicalPlan::new(PlanNode::Projection {
                        expressions: final_projection,
                        input: plan.root,
                    });
                }
            }
        } else if !projection_exprs.iter().all(|(e, _)| e.is_wildcard()) {
            if has_window_functions {
                let mut all_windows: Vec<(Expr, String)> = Vec::new();
                let mut counter = 0usize;

                for (expr, alias) in &projection_exprs {
                    if self.is_window_function(expr) {
                        let col_name = alias.clone().unwrap_or_else(|| match expr {
                            Expr::WindowFunction { name, .. } => format!("{}(...)", name.as_str()),
                            _ => "window_result".to_string(),
                        });
                        all_windows.push((expr.clone(), col_name));
                    } else if self.has_window_function(expr) {
                        self.extract_window_functions(expr, &mut all_windows, &mut counter);
                    }
                }

                if !all_windows.is_empty() {
                    let window_exprs: Vec<(Expr, Option<String>)> = all_windows
                        .iter()
                        .map(|(expr, col_name)| (expr.clone(), Some(col_name.clone())))
                        .collect();

                    plan = LogicalPlan::new(PlanNode::Window {
                        window_exprs,
                        input: plan.root,
                    });
                }

                let final_projection: Vec<(Expr, Option<String>)> = projection_exprs
                    .iter()
                    .map(|(expr, alias)| {
                        if self.is_window_function(expr) {
                            let col_name = alias.clone().unwrap_or_else(|| match expr {
                                Expr::WindowFunction { name, .. } => {
                                    format!("{}(...)", name.as_str())
                                }
                                _ => "window_result".to_string(),
                            });
                            (
                                Expr::Column {
                                    name: col_name,
                                    table: None,
                                },
                                alias.clone(),
                            )
                        } else if self.has_window_function(expr) {
                            let replaced = self.replace_window_functions(expr, &all_windows);
                            (replaced, alias.clone())
                        } else {
                            (expr.clone(), alias.clone())
                        }
                    })
                    .collect();

                plan = LogicalPlan::new(PlanNode::Projection {
                    expressions: final_projection,
                    input: plan.root,
                });
            } else {
                plan = LogicalPlan::new(PlanNode::Projection {
                    expressions: projection_exprs,
                    input: plan.root,
                });
            }
        }

        if has_distinct {
            plan = LogicalPlan::new(PlanNode::Distinct { input: plan.root });
        }

        Ok(plan)
    }

    fn extract_grouping_extension(
        &self,
        ast_expr: &ast::Expr,
    ) -> Result<Option<GroupingExtension>> {
        match ast_expr {
            ast::Expr::Rollup(sets) => {
                let mut columns = Vec::new();
                for set in sets {
                    for expr in set {
                        columns.push(expr.clone());
                    }
                }
                return Ok(Some(GroupingExtension {
                    columns,
                    is_rollup: true,
                    is_cube: false,
                    is_grouping_sets: false,
                    explicit_sets: Vec::new(),
                }));
            }
            ast::Expr::Cube(sets) => {
                let mut columns = Vec::new();
                for set in sets {
                    for expr in set {
                        columns.push(expr.clone());
                    }
                }
                return Ok(Some(GroupingExtension {
                    columns,
                    is_rollup: false,
                    is_cube: true,
                    is_grouping_sets: false,
                    explicit_sets: Vec::new(),
                }));
            }
            ast::Expr::GroupingSets(sets) => {
                let mut columns = Vec::new();
                for set in sets {
                    for expr in set {
                        if !columns
                            .iter()
                            .any(|e| format!("{:?}", e) == format!("{:?}", expr))
                        {
                            columns.push(expr.clone());
                        }
                    }
                }

                let explicit_sets: Vec<Vec<ast::Expr>> = sets
                    .iter()
                    .map(|set| set.iter().map(|e| (*e).clone()).collect())
                    .collect();

                return Ok(Some(GroupingExtension {
                    columns,
                    is_rollup: false,
                    is_cube: false,
                    is_grouping_sets: true,
                    explicit_sets,
                }));
            }
            _ => {}
        }

        if let ast::Expr::Function(func) = ast_expr {
            let func_name = func.name.to_string().to_uppercase();
            match func_name.as_str() {
                "ROLLUP" | "CUBE" | "GROUPING" => {
                    let args = match &func.args {
                        ast::FunctionArguments::List(list) => &list.args,
                        _ => return Ok(None),
                    };

                    let mut columns = Vec::new();
                    for arg in args {
                        match arg {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                                columns.push(expr.clone());
                            }
                            ast::FunctionArg::Named {
                                arg: ast::FunctionArgExpr::Expr(expr),
                                ..
                            } => {
                                columns.push(expr.clone());
                            }
                            _ => {}
                        }
                    }

                    return Ok(Some(GroupingExtension {
                        columns,
                        is_rollup: func_name == "ROLLUP",
                        is_cube: func_name == "CUBE",
                        is_grouping_sets: func_name == "GROUPING",
                        explicit_sets: Vec::new(),
                    }));
                }
                _ => {}
            }
        }
        Ok(None)
    }

    #[allow(clippy::too_many_arguments)]
    fn build_rollup_cube_plan(
        &self,
        plan: LogicalPlan,
        group_by_exprs: &[Expr],
        aggregates: Vec<Expr>,
        is_rollup: bool,
        is_cube: bool,
        is_grouping_sets: bool,
        explicit_grouping_sets: &[Vec<Expr>],
    ) -> Result<LogicalPlan> {
        let grouping_sets = if is_grouping_sets && !explicit_grouping_sets.is_empty() {
            explicit_grouping_sets.to_vec()
        } else {
            self.generate_grouping_sets(group_by_exprs, is_rollup, is_cube)?
        };

        let aggregate_plans =
            self.build_grouping_set_plans(&plan, group_by_exprs, &aggregates, &grouping_sets);

        self.combine_plans_with_union(aggregate_plans)
    }

    fn build_grouping_set_plans(
        &self,
        plan: &LogicalPlan,
        all_group_cols: &[Expr],
        aggregates: &[Expr],
        grouping_sets: &[Vec<Expr>],
    ) -> Vec<PlanNode> {
        let all_column_names = self.extract_column_names(all_group_cols);

        grouping_sets
            .iter()
            .enumerate()
            .map(|(set_id, grouping_set)| {
                let grouped_column_names = self.extract_column_names(grouping_set);

                let metadata = self.create_grouping_metadata(
                    &all_column_names,
                    &grouped_column_names,
                    set_id,
                    grouping_sets.len(),
                );

                let agg_node = Box::new(PlanNode::Aggregate {
                    group_by: grouping_set.clone(),
                    aggregates: aggregates.to_vec(),
                    input: plan.root.clone(),
                    grouping_metadata: Some(metadata),
                });

                let projection_exprs =
                    self.build_normalized_projection(all_group_cols, grouping_set, aggregates);

                PlanNode::Projection {
                    expressions: projection_exprs,
                    input: agg_node,
                }
            })
            .collect()
    }

    fn build_normalized_projection(
        &self,
        all_group_cols: &[Expr],
        current_grouping_set: &[Expr],
        aggregates: &[Expr],
    ) -> Vec<(Expr, Option<String>)> {
        let mut projection_exprs = Vec::new();

        for orig_col in all_group_cols {
            let expr_with_alias =
                if let Some(idx) = current_grouping_set.iter().position(|e| e == orig_col) {
                    self.build_grouped_column_ref(orig_col, idx)
                } else {
                    self.build_null_column_placeholder(orig_col, projection_exprs.len())
                };
            projection_exprs.push(expr_with_alias);
        }

        for (idx, agg) in aggregates.iter().enumerate() {
            let col_ref = Expr::Column {
                name: format!("agg_{}", idx),
                table: None,
            };

            let alias = Self::format_aggregate_column_name_from_expr(agg);

            projection_exprs.push((col_ref, Some(alias)));
        }

        projection_exprs
    }

    fn format_aggregate_column_name_from_expr(expr: &Expr) -> String {
        match expr {
            Expr::Aggregate {
                name,
                args,
                distinct,
                ..
            } => Self::format_aggregate_column_name(name, args, *distinct),
            Expr::Function { name, args } => Self::format_aggregate_column_name(name, args, false),
            _ => "agg".to_string(),
        }
    }

    fn build_grouped_column_ref(&self, expr: &Expr, group_index: usize) -> (Expr, Option<String>) {
        let original_name = match expr {
            Expr::Column { name, .. } => name.clone(),
            _ => format!("col_{}", group_index),
        };

        (
            Expr::Column {
                name: format!("group_{}", group_index),
                table: None,
            },
            Some(original_name),
        )
    }

    fn build_null_column_placeholder(
        &self,
        expr: &Expr,
        fallback_index: usize,
    ) -> (Expr, Option<String>) {
        let col_name = match expr {
            Expr::Column { name, .. } => name.clone(),
            _ => format!("col_{}", fallback_index),
        };

        (
            Expr::Literal(yachtsql_ir::expr::LiteralValue::Null),
            Some(col_name),
        )
    }

    fn combine_plans_with_union(&self, plans: Vec<PlanNode>) -> Result<LogicalPlan> {
        if plans.is_empty() {
            return Err(Error::InternalError(
                "No grouping sets generated".to_string(),
            ));
        }

        if plans.len() == 1 {
            return Ok(LogicalPlan::new(plans[0].clone()));
        }

        let mut result_plan = Box::new(plans[plans.len() - 1].clone());
        for plan in plans.iter().take(plans.len() - 1).rev() {
            result_plan = Box::new(PlanNode::Union {
                left: Box::new(plan.clone()),
                right: result_plan,
                all: true,
            });
        }

        Ok(LogicalPlan::new((*result_plan).clone()))
    }

    fn generate_grouping_sets(
        &self,
        columns: &[Expr],
        is_rollup: bool,
        is_cube: bool,
    ) -> Result<Vec<Vec<Expr>>> {
        if is_rollup {
            Ok(yachtsql_optimizer::grouping_sets::generate_rollup_sets(
                columns.to_vec(),
            ))
        } else if is_cube {
            yachtsql_optimizer::grouping_sets::generate_cube_sets(columns.to_vec())
        } else {
            Ok(vec![columns.to_vec()])
        }
    }

    fn create_grouping_metadata(
        &self,
        _all_columns: &[String],
        grouped_columns: &[String],
        set_id: usize,
        total_sets: usize,
    ) -> yachtsql_ir::plan::GroupingMetadata {
        yachtsql_ir::plan::GroupingMetadata {
            grouped_columns: grouped_columns.to_vec(),
            grouping_set_id: set_id,
            total_sets,
        }
    }

    fn extract_column_names(&self, exprs: &[Expr]) -> Vec<String> {
        exprs
            .iter()
            .filter_map(|e| match e {
                Expr::Column { name, .. } => Some(name.clone()),
                _ => None,
            })
            .collect()
    }

    fn format_aggregate_column_name(
        func_name: &yachtsql_ir::FunctionName,
        args: &[Expr],
        distinct: bool,
    ) -> String {
        let func_upper = func_name.as_str();

        let first_is_wildcard = args.first().is_some_and(|expr| expr.is_wildcard());
        let arg_str = if args.is_empty() || first_is_wildcard {
            "*".to_string()
        } else if let Some(Expr::Column { name, .. }) = args.first() {
            name.clone()
        } else {
            "...".to_string()
        };

        if distinct {
            format!("{}(DISTINCT {})", func_upper, arg_str)
        } else {
            format!("{}({})", func_upper, arg_str)
        }
    }

    fn is_aggregate(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function { name, .. } => {
                matches!(name.as_str(), crate::aggregate_function_names!())
            }
            Expr::Aggregate { .. } => true,
            _ => false,
        }
    }

    fn is_window_function(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::WindowFunction { .. })
    }

    #[allow(clippy::only_used_in_recursion)]
    fn has_aggregate(&self, expr: &Expr) -> bool {
        if self.is_aggregate(expr) {
            return true;
        }
        match expr {
            Expr::BinaryOp { left, right, .. } => {
                self.has_aggregate(left) || self.has_aggregate(right)
            }
            Expr::UnaryOp { expr, .. } => self.has_aggregate(expr),
            Expr::Function { args, .. } => args.iter().any(|arg| self.has_aggregate(arg)),
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand.as_ref().is_some_and(|o| self.has_aggregate(o))
                    || when_then
                        .iter()
                        .any(|(w, t)| self.has_aggregate(w) || self.has_aggregate(t))
                    || else_expr.as_ref().is_some_and(|e| self.has_aggregate(e))
            }
            Expr::Cast { expr, .. } | Expr::TryCast { expr, .. } => self.has_aggregate(expr),
            Expr::InList { expr, list, .. } => {
                self.has_aggregate(expr) || list.iter().any(|item| self.has_aggregate(item))
            }
            Expr::Between {
                expr, low, high, ..
            } => self.has_aggregate(expr) || self.has_aggregate(low) || self.has_aggregate(high),
            _ => false,
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn has_window_function(&self, expr: &Expr) -> bool {
        match expr {
            Expr::WindowFunction { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                self.has_window_function(left) || self.has_window_function(right)
            }
            Expr::UnaryOp { expr, .. } => self.has_window_function(expr),
            Expr::Function { args, .. } | Expr::Aggregate { args, .. } => {
                args.iter().any(|arg| self.has_window_function(arg))
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|o| self.has_window_function(o))
                    || when_then
                        .iter()
                        .any(|(w, t)| self.has_window_function(w) || self.has_window_function(t))
                    || else_expr
                        .as_ref()
                        .is_some_and(|e| self.has_window_function(e))
            }
            Expr::Cast { expr, .. } | Expr::TryCast { expr, .. } => self.has_window_function(expr),
            _ => false,
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn extract_window_functions(
        &self,
        expr: &Expr,
        windows: &mut Vec<(Expr, String)>,
        counter: &mut usize,
    ) {
        match expr {
            Expr::WindowFunction { name, .. } => {
                let col_name = format!("__window_{}_{}", name.as_str().to_lowercase(), *counter);
                *counter += 1;
                windows.push((expr.clone(), col_name));
            }
            Expr::BinaryOp { left, right, .. } => {
                self.extract_window_functions(left, windows, counter);
                self.extract_window_functions(right, windows, counter);
            }
            Expr::UnaryOp { expr: inner, .. } => {
                self.extract_window_functions(inner, windows, counter);
            }
            Expr::Function { args, .. } | Expr::Aggregate { args, .. } => {
                for arg in args {
                    self.extract_window_functions(arg, windows, counter);
                }
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(o) = operand {
                    self.extract_window_functions(o, windows, counter);
                }
                for (w, t) in when_then {
                    self.extract_window_functions(w, windows, counter);
                    self.extract_window_functions(t, windows, counter);
                }
                if let Some(e) = else_expr {
                    self.extract_window_functions(e, windows, counter);
                }
            }
            Expr::Cast { expr: inner, .. } | Expr::TryCast { expr: inner, .. } => {
                self.extract_window_functions(inner, windows, counter);
            }
            _ => {}
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn replace_window_functions(&self, expr: &Expr, windows: &[(Expr, String)]) -> Expr {
        match expr {
            Expr::WindowFunction { .. } => {
                if let Some((_, col_name)) = windows.iter().find(|(w, _)| w == expr) {
                    Expr::Column {
                        name: col_name.clone(),
                        table: None,
                    }
                } else {
                    expr.clone()
                }
            }
            Expr::BinaryOp { op, left, right } => Expr::BinaryOp {
                op: *op,
                left: Box::new(self.replace_window_functions(left, windows)),
                right: Box::new(self.replace_window_functions(right, windows)),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(self.replace_window_functions(inner, windows)),
            },
            Expr::Function { name, args } => Expr::Function {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| self.replace_window_functions(arg, windows))
                    .collect(),
            },
            Expr::Aggregate {
                name,
                args,
                distinct,
                order_by,
                filter,
            } => Expr::Aggregate {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| self.replace_window_functions(arg, windows))
                    .collect(),
                distinct: *distinct,
                order_by: order_by.clone(),
                filter: filter.clone(),
            },
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => Expr::Case {
                operand: operand
                    .as_ref()
                    .map(|o| Box::new(self.replace_window_functions(o, windows))),
                when_then: when_then
                    .iter()
                    .map(|(w, t)| {
                        (
                            self.replace_window_functions(w, windows),
                            self.replace_window_functions(t, windows),
                        )
                    })
                    .collect(),
                else_expr: else_expr
                    .as_ref()
                    .map(|e| Box::new(self.replace_window_functions(e, windows))),
            },
            Expr::Cast {
                expr: inner,
                data_type,
            } => Expr::Cast {
                expr: Box::new(self.replace_window_functions(inner, windows)),
                data_type: data_type.clone(),
            },
            Expr::TryCast {
                expr: inner,
                data_type,
            } => Expr::TryCast {
                expr: Box::new(self.replace_window_functions(inner, windows)),
                data_type: data_type.clone(),
            },
            _ => expr.clone(),
        }
    }

    fn rewrite_window_order_by_aggregates(&self, expr: &Expr) -> Expr {
        let Expr::WindowFunction {
            name,
            args,
            partition_by,
            order_by,
            frame_units,
            frame_start_offset,
            frame_end_offset,
            exclude,
            null_treatment,
        } = expr
        else {
            return expr.clone();
        };

        let rewritten_order_by: Vec<yachtsql_ir::expr::OrderByExpr> = order_by
            .iter()
            .map(|ob| {
                let rewritten_expr = self.rewrite_aggregate_to_column(&ob.expr);
                yachtsql_ir::expr::OrderByExpr {
                    expr: rewritten_expr,
                    asc: ob.asc,
                    nulls_first: ob.nulls_first,
                    collation: ob.collation.clone(),
                }
            })
            .collect();

        Expr::WindowFunction {
            name: name.clone(),
            args: args.clone(),
            partition_by: partition_by.clone(),
            order_by: rewritten_order_by,
            frame_units: *frame_units,
            frame_start_offset: *frame_start_offset,
            frame_end_offset: *frame_end_offset,
            exclude: *exclude,
            null_treatment: *null_treatment,
        }
    }

    fn rewrite_aggregate_to_column(&self, expr: &Expr) -> Expr {
        match expr {
            Expr::Aggregate {
                name,
                args,
                distinct,
                ..
            } => {
                let first_is_wildcard = args.first().is_some_and(|e| e.is_wildcard());
                let col_name = if args.is_empty() || first_is_wildcard {
                    if *distinct {
                        format!("{}(DISTINCT *)", name.as_str())
                    } else {
                        format!("{}(*)", name.as_str())
                    }
                } else if let Some(Expr::Column { name: col_name, .. }) = args.first() {
                    if *distinct {
                        format!("{}(DISTINCT {})", name.as_str(), col_name)
                    } else {
                        format!("{}({})", name.as_str(), col_name)
                    }
                } else if *distinct {
                    format!("{}(DISTINCT ...)", name.as_str())
                } else {
                    format!("{}(...)", name.as_str())
                };
                Expr::Column {
                    name: col_name,
                    table: None,
                }
            }
            Expr::Function { name, args } if self.is_aggregate(expr) => {
                let first_is_wildcard = args.first().is_some_and(|e| e.is_wildcard());
                let col_name = if args.is_empty() || first_is_wildcard {
                    format!("{}(*)", name.as_str())
                } else if let Some(Expr::Column { name: col_name, .. }) = args.first() {
                    format!("{}({})", name.as_str(), col_name)
                } else {
                    format!("{}(...)", name.as_str())
                };
                Expr::Column {
                    name: col_name,
                    table: None,
                }
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(self.rewrite_aggregate_to_column(left)),
                op: *op,
                right: Box::new(self.rewrite_aggregate_to_column(right)),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(self.rewrite_aggregate_to_column(inner)),
            },
            Expr::Function { name, args } => Expr::Function {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| self.rewrite_aggregate_to_column(arg))
                    .collect(),
            },
            Expr::Cast {
                expr: inner,
                data_type,
            } => Expr::Cast {
                expr: Box::new(self.rewrite_aggregate_to_column(inner)),
                data_type: data_type.clone(),
            },
            Expr::TryCast {
                expr: inner,
                data_type,
            } => Expr::TryCast {
                expr: Box::new(self.rewrite_aggregate_to_column(inner)),
                data_type: data_type.clone(),
            },
            _ => expr.clone(),
        }
    }

    fn extract_window_functions_with_aggregate_rewrite(
        &self,
        expr: &Expr,
        windows: &mut Vec<(Expr, String)>,
        counter: &mut usize,
    ) {
        match expr {
            Expr::WindowFunction { name, .. } => {
                let rewritten_window = self.rewrite_window_order_by_aggregates(expr);
                let col_name = format!("__window_{}_{}", name.as_str().to_lowercase(), *counter);
                *counter += 1;
                windows.push((rewritten_window, col_name));
            }
            Expr::BinaryOp { left, right, .. } => {
                self.extract_window_functions_with_aggregate_rewrite(left, windows, counter);
                self.extract_window_functions_with_aggregate_rewrite(right, windows, counter);
            }
            Expr::UnaryOp { expr: inner, .. } => {
                self.extract_window_functions_with_aggregate_rewrite(inner, windows, counter);
            }
            Expr::Function { args, .. } | Expr::Aggregate { args, .. } => {
                for arg in args {
                    self.extract_window_functions_with_aggregate_rewrite(arg, windows, counter);
                }
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(o) = operand {
                    self.extract_window_functions_with_aggregate_rewrite(o, windows, counter);
                }
                for (w, t) in when_then {
                    self.extract_window_functions_with_aggregate_rewrite(w, windows, counter);
                    self.extract_window_functions_with_aggregate_rewrite(t, windows, counter);
                }
                if let Some(e) = else_expr {
                    self.extract_window_functions_with_aggregate_rewrite(e, windows, counter);
                }
            }
            Expr::Cast { expr: inner, .. } | Expr::TryCast { expr: inner, .. } => {
                self.extract_window_functions_with_aggregate_rewrite(inner, windows, counter);
            }
            _ => {}
        }
    }

    fn group_by_expr_column_name(index: usize) -> String {
        format!("group_{}", index)
    }

    fn rewrite_group_by_expr_to_column(
        &self,
        expr: &Expr,
        group_by_exprs: &[Expr],
    ) -> Option<Expr> {
        group_by_exprs
            .iter()
            .enumerate()
            .find_map(|(idx, group_expr)| {
                if group_expr == expr {
                    if let Expr::Column { .. } = group_expr {
                        Some(group_expr.clone())
                    } else {
                        Some(Expr::Column {
                            name: Self::group_by_expr_column_name(idx),
                            table: None,
                        })
                    }
                } else {
                    None
                }
            })
    }

    fn rewrite_post_aggregate_expr(&self, expr: &Expr, group_by_exprs: &[Expr]) -> Expr {
        if let Some(group_expr_ref) = self.rewrite_group_by_expr_to_column(expr, group_by_exprs) {
            return group_expr_ref;
        }

        match expr {
            Expr::Function { name, args } if self.is_aggregate(expr) => {
                let first_is_wildcard = args.first().is_some_and(|expr| expr.is_wildcard());
                let col_name = if args.is_empty() || first_is_wildcard {
                    format!("{}(*)", name.as_str())
                } else if let Some(Expr::Column { name: col_name, .. }) = args.first() {
                    format!("{}({})", name.as_str(), col_name)
                } else {
                    format!("{}(...)", name.as_str())
                };
                Expr::Column {
                    name: col_name,
                    table: None,
                }
            }
            Expr::Aggregate {
                name,
                args,
                distinct,
                ..
            } => {
                let first_is_wildcard = args.first().is_some_and(|expr| expr.is_wildcard());
                let col_name = if args.is_empty() || first_is_wildcard {
                    if *distinct {
                        format!("{}(DISTINCT *)", name.as_str())
                    } else {
                        format!("{}(*)", name.as_str())
                    }
                } else if let Some(Expr::Column { name: col_name, .. }) = args.first() {
                    if *distinct {
                        format!("{}(DISTINCT {})", name.as_str(), col_name)
                    } else {
                        format!("{}({})", name.as_str(), col_name)
                    }
                } else if *distinct {
                    format!("{}(DISTINCT ...)", name.as_str())
                } else {
                    format!("{}(...)", name.as_str())
                };
                Expr::Column {
                    name: col_name,
                    table: None,
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let left_rewritten = self.rewrite_post_aggregate_expr(left, group_by_exprs);
                let right_rewritten = self.rewrite_post_aggregate_expr(right, group_by_exprs);
                Expr::BinaryOp {
                    left: Box::new(left_rewritten),
                    op: *op,
                    right: Box::new(right_rewritten),
                }
            }
            Expr::UnaryOp { op, expr: inner } => {
                let inner_rewritten = self.rewrite_post_aggregate_expr(inner, group_by_exprs);
                Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(inner_rewritten),
                }
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                let rewritten_operand = operand
                    .as_ref()
                    .map(|op| self.rewrite_post_aggregate_expr(op, group_by_exprs))
                    .map(Box::new);
                let rewritten_when_then: Vec<(Expr, Expr)> = when_then
                    .iter()
                    .map(|(when_expr, then_expr)| {
                        (
                            self.rewrite_post_aggregate_expr(when_expr, group_by_exprs),
                            self.rewrite_post_aggregate_expr(then_expr, group_by_exprs),
                        )
                    })
                    .collect();
                let rewritten_else = else_expr
                    .as_ref()
                    .map(|expr| self.rewrite_post_aggregate_expr(expr, group_by_exprs))
                    .map(Box::new);

                Expr::Case {
                    operand: rewritten_operand,
                    when_then: rewritten_when_then,
                    else_expr: rewritten_else,
                }
            }

            Expr::Function { name, args } => {
                let rewritten_args: Vec<Expr> = args
                    .iter()
                    .map(|arg| self.rewrite_post_aggregate_expr(arg, group_by_exprs))
                    .collect();
                Expr::Function {
                    name: name.clone(),
                    args: rewritten_args,
                }
            }

            _ => expr.clone(),
        }
    }

    fn collect_aggregates_from_expr(&self, expr: &Expr, aggregates: &mut Vec<Expr>) {
        match expr {
            Expr::Function { .. } | Expr::Aggregate { .. } if self.is_aggregate(expr) => {
                self.add_unique_aggregate(expr, aggregates);
            }

            Expr::Function { args, .. } => {
                for arg in args {
                    self.collect_aggregates_from_expr(arg, aggregates);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.collect_aggregates_from_expr(left, aggregates);
                self.collect_aggregates_from_expr(right, aggregates);
            }
            Expr::UnaryOp { expr: inner, .. } => {
                self.collect_aggregates_from_expr(inner, aggregates);
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(op) = operand {
                    self.collect_aggregates_from_expr(op, aggregates);
                }
                for (when, then) in when_then {
                    self.collect_aggregates_from_expr(when, aggregates);
                    self.collect_aggregates_from_expr(then, aggregates);
                }
                if let Some(else_e) = else_expr {
                    self.collect_aggregates_from_expr(else_e, aggregates);
                }
            }
            Expr::InList {
                expr: inner, list, ..
            } => {
                self.collect_aggregates_from_expr(inner, aggregates);
                for item in list {
                    self.collect_aggregates_from_expr(item, aggregates);
                }
            }
            Expr::TupleInList { tuple, list, .. } => {
                for t_expr in tuple {
                    self.collect_aggregates_from_expr(t_expr, aggregates);
                }
                for tuple_list in list {
                    for item in tuple_list {
                        self.collect_aggregates_from_expr(item, aggregates);
                    }
                }
            }
            Expr::TupleInSubquery { tuple, .. } => {
                for t_expr in tuple {
                    self.collect_aggregates_from_expr(t_expr, aggregates);
                }
            }
            Expr::Between {
                expr: inner,
                low,
                high,
                ..
            } => {
                self.collect_aggregates_from_expr(inner, aggregates);
                self.collect_aggregates_from_expr(low, aggregates);
                self.collect_aggregates_from_expr(high, aggregates);
            }
            _ => {}
        }
    }

    fn add_unique_aggregate(&self, expr: &Expr, aggregates: &mut Vec<Expr>) {
        if !aggregates.iter().any(|agg| agg == expr) {
            aggregates.push(expr.clone());
        }
    }

    #[allow(clippy::type_complexity)]
    fn inject_order_by_columns_into_projection(
        &self,
        plan: &LogicalPlan,
        sort_exprs: &[yachtsql_optimizer::expr::OrderByExpr],
    ) -> (LogicalPlan, Option<Vec<(Expr, Option<String>)>>) {
        let PlanNode::Projection { expressions, input } = plan.root.as_ref() else {
            return (plan.clone(), None);
        };

        let mut order_by_columns: Vec<(String, Option<String>)> = Vec::new();
        for sort_expr in sort_exprs {
            Self::collect_column_refs(&sort_expr.expr, &mut order_by_columns);
        }

        let mut projection_output_columns = std::collections::HashSet::new();
        for (expr, alias) in expressions {
            if let Some(alias_name) = alias {
                projection_output_columns.insert((alias_name.clone(), None));
                projection_output_columns.insert((alias_name.clone(), Some(alias_name.clone())));
            }
            if let Expr::Column { name, table } = expr {
                projection_output_columns.insert((name.clone(), table.clone()));
                projection_output_columns.insert((name.clone(), None));
            }
        }

        let missing_columns: Vec<(String, Option<String>)> = order_by_columns
            .into_iter()
            .filter(|col| !projection_output_columns.contains(col))
            .collect();

        if missing_columns.is_empty() {
            return (plan.clone(), None);
        }

        let original_exprs = expressions.clone();

        let mut new_expressions = expressions.clone();
        for (col_name, col_table) in missing_columns {
            new_expressions.push((
                Expr::Column {
                    name: col_name,
                    table: col_table,
                },
                None,
            ));
        }

        let modified_plan = LogicalPlan::new(PlanNode::Projection {
            expressions: new_expressions,
            input: input.clone(),
        });

        (modified_plan, Some(original_exprs))
    }

    fn collect_column_refs(expr: &Expr, columns: &mut Vec<(String, Option<String>)>) {
        match expr {
            Expr::Column { name, table } => {
                let col_ref = (name.clone(), table.clone());
                if !columns.contains(&col_ref) {
                    columns.push(col_ref);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_column_refs(left, columns);
                Self::collect_column_refs(right, columns);
            }
            Expr::UnaryOp { expr: inner, .. } => {
                Self::collect_column_refs(inner, columns);
            }
            Expr::Function { args, .. } | Expr::Aggregate { args, .. } => {
                for arg in args {
                    Self::collect_column_refs(arg, columns);
                }
            }
            Expr::Cast { expr, .. } | Expr::TryCast { expr, .. } => {
                Self::collect_column_refs(expr, columns);
            }
            _ => {}
        }
    }

    fn convert_order_by_exprs(
        &self,
        order_by: &ast::OrderBy,
    ) -> Result<Vec<yachtsql_ir::expr::OrderByExpr>> {
        if order_by.interpolate.is_some() {
            return Err(Error::unsupported_feature(
                "ORDER BY INTERPOLATE clause not supported".to_string(),
            ));
        }

        let order_by_exprs = match &order_by.kind {
            ast::OrderByKind::Expressions(exprs) => exprs,
            ast::OrderByKind::All(_) => {
                return Err(Error::unsupported_feature(
                    "ORDER BY ALL not supported".to_string(),
                ));
            }
        };

        order_by_exprs
            .iter()
            .map(|order_expr| {
                if order_expr.with_fill.is_some() {
                    return Err(Error::unsupported_feature(
                        "ORDER BY WITH FILL not supported".to_string(),
                    ));
                }

                let asc = order_expr.options.asc;
                let nulls_first = order_expr.options.nulls_first;

                let (expr, collation) = if let ast::Expr::Collate {
                    expr: inner_expr,
                    collation: coll_name,
                } = &order_expr.expr
                {
                    let inner = self.sql_expr_to_expr(inner_expr.as_ref())?;
                    let coll_name_str = Self::object_name_to_string(coll_name);

                    let normalized_name = coll_name_str
                        .strip_prefix("yachtsql.")
                        .unwrap_or(&coll_name_str)
                        .to_string();

                    use yachtsql_core::types::collation::CollationRegistry;
                    if CollationRegistry::global().get(&normalized_name).is_none() {
                        return Err(Error::undefined_collation(&normalized_name));
                    }

                    (inner, Some(normalized_name))
                } else {
                    (self.sql_expr_to_expr(&order_expr.expr)?, None)
                };

                Ok(yachtsql_ir::expr::OrderByExpr {
                    expr,
                    asc,
                    nulls_first,
                    collation,
                })
            })
            .collect()
    }

    fn parse_limit_clause(
        &self,
        limit_clause: &Option<ast::LimitClause>,
    ) -> Result<(Option<usize>, usize)> {
        let mut limit_val = None;
        let mut offset_val = 0;

        if let Some(clause) = limit_clause {
            match clause {
                ast::LimitClause::LimitOffset {
                    limit,
                    limit_by,
                    offset,
                } => {
                    if !limit_by.is_empty() {
                        return Err(Error::unsupported_feature(
                            "LIMIT ... BY is not supported yet".to_string(),
                        ));
                    }

                    if let Some(limit_expr) = limit {
                        limit_val = Some(Self::parse_positive_integer(limit_expr, "LIMIT")?);
                    }

                    if let Some(offset_expr) = offset {
                        offset_val = Self::parse_positive_integer(&offset_expr.value, "OFFSET")?;
                    }
                }
                ast::LimitClause::OffsetCommaLimit { offset, limit } => {
                    offset_val = Self::parse_positive_integer(offset, "OFFSET")?;
                    limit_val = Some(Self::parse_positive_integer(limit, "LIMIT")?);
                }
            }
        }

        Ok((limit_val, offset_val))
    }

    fn parse_positive_integer(expr: &ast::Expr, label: &str) -> Result<usize> {
        match expr {
            ast::Expr::Value(ast::ValueWithSpan {
                value: ast::Value::Number(num_str, _),
                ..
            }) => num_str
                .parse::<usize>()
                .map_err(|_| Error::invalid_query(format!("Invalid {label} value"))),
            ast::Expr::UnaryOp {
                op: ast::UnaryOperator::Minus,
                ..
            } => Err(Error::invalid_query(format!(
                "{label} value must be non-negative"
            ))),
            _ => Err(Error::invalid_query(format!("Invalid {label} value"))),
        }
    }

    fn object_names_to_idents(cols: &[ast::ObjectName]) -> Result<Vec<ast::Ident>> {
        cols.iter()
            .map(|object_name| {
                object_name
                    .0
                    .last()
                    .and_then(|part| part.as_ident().cloned())
                    .ok_or_else(|| {
                        Error::invalid_query(
                            "USING clause identifiers must not be empty".to_string(),
                        )
                    })
            })
            .collect()
    }

    fn tables_to_plan(&self, from: &[ast::TableWithJoins]) -> Result<LogicalPlan> {
        if from.is_empty() {
            return Ok(LogicalPlan::new(PlanNode::EmptyRelation));
        }

        let table_with_joins = &from[0];
        let mut plan = self.table_factor_to_plan(&table_with_joins.relation)?;

        for join in &table_with_joins.joins {
            let is_lateral = Self::is_lateral_derived_table(&join.relation);

            let right_plan = self.plan_join_relation(&join.relation, &plan)?;

            let join_type = match join.join_operator {
                ast::JoinOperator::Join(_) => JoinType::Inner,
                ast::JoinOperator::Inner(_) => JoinType::Inner,
                ast::JoinOperator::Left(_) => JoinType::Left,
                ast::JoinOperator::LeftOuter(_) => JoinType::Left,
                ast::JoinOperator::Right(_) => JoinType::Right,
                ast::JoinOperator::RightOuter(_) => JoinType::Right,
                ast::JoinOperator::FullOuter(_) => JoinType::Full,
                ast::JoinOperator::CrossJoin(_) => JoinType::Cross,

                ast::JoinOperator::CrossApply => JoinType::Cross,

                ast::JoinOperator::OuterApply => JoinType::Left,
                _ => {
                    return Err(Error::unsupported_feature(
                        "Join type not supported".to_string(),
                    ));
                }
            };

            let is_apply = matches!(
                join.join_operator,
                ast::JoinOperator::CrossApply | ast::JoinOperator::OuterApply
            );
            let is_lateral = is_lateral || is_apply;

            let on_expr = match &join.join_operator {
                ast::JoinOperator::Join(ast::JoinConstraint::On(expr))
                | ast::JoinOperator::Inner(ast::JoinConstraint::On(expr))
                | ast::JoinOperator::Left(ast::JoinConstraint::On(expr))
                | ast::JoinOperator::LeftOuter(ast::JoinConstraint::On(expr))
                | ast::JoinOperator::Right(ast::JoinConstraint::On(expr))
                | ast::JoinOperator::RightOuter(ast::JoinConstraint::On(expr))
                | ast::JoinOperator::FullOuter(ast::JoinConstraint::On(expr)) => {
                    self.sql_expr_to_expr(expr)?
                }
                ast::JoinOperator::Join(ast::JoinConstraint::Using(cols))
                | ast::JoinOperator::Inner(ast::JoinConstraint::Using(cols))
                | ast::JoinOperator::Left(ast::JoinConstraint::Using(cols))
                | ast::JoinOperator::LeftOuter(ast::JoinConstraint::Using(cols))
                | ast::JoinOperator::Right(ast::JoinConstraint::Using(cols))
                | ast::JoinOperator::RightOuter(ast::JoinConstraint::Using(cols))
                | ast::JoinOperator::FullOuter(ast::JoinConstraint::Using(cols)) => {
                    let ident_cols = Self::object_names_to_idents(cols)?;
                    self.build_using_condition(&ident_cols)?
                }
                ast::JoinOperator::Join(ast::JoinConstraint::Natural)
                | ast::JoinOperator::Inner(ast::JoinConstraint::Natural)
                | ast::JoinOperator::Left(ast::JoinConstraint::Natural)
                | ast::JoinOperator::LeftOuter(ast::JoinConstraint::Natural)
                | ast::JoinOperator::Right(ast::JoinConstraint::Natural)
                | ast::JoinOperator::RightOuter(ast::JoinConstraint::Natural)
                | ast::JoinOperator::FullOuter(ast::JoinConstraint::Natural) => {
                    self.build_natural_join_condition(&plan, &right_plan)?
                }
                ast::JoinOperator::CrossJoin(_) => Expr::Literal(LiteralValue::Boolean(true)),

                ast::JoinOperator::CrossApply | ast::JoinOperator::OuterApply => {
                    Expr::Literal(LiteralValue::Boolean(true))
                }
                _ => {
                    return Err(Error::unsupported_feature(
                        "Join constraint not supported".to_string(),
                    ));
                }
            };

            plan = if is_lateral {
                LogicalPlan::new(PlanNode::LateralJoin {
                    left: plan.root,
                    right: right_plan.root,
                    on: on_expr,
                    join_type,
                })
            } else {
                LogicalPlan::new(PlanNode::Join {
                    left: plan.root,
                    right: right_plan.root,
                    on: on_expr,
                    join_type,
                })
            };
        }

        for table_with_joins in from.iter().skip(1) {
            let is_lateral = Self::is_lateral_derived_table(&table_with_joins.relation);
            let right_plan = self.plan_join_relation(&table_with_joins.relation, &plan)?;
            plan = if is_lateral {
                LogicalPlan::new(PlanNode::LateralJoin {
                    left: plan.root,
                    right: right_plan.root,
                    on: Expr::Literal(LiteralValue::Boolean(true)),
                    join_type: JoinType::Cross,
                })
            } else {
                LogicalPlan::new(PlanNode::Join {
                    left: plan.root,
                    right: right_plan.root,
                    on: Expr::Literal(LiteralValue::Boolean(true)),
                    join_type: JoinType::Cross,
                })
            };

            for join in &table_with_joins.joins {
                let is_lateral = Self::is_lateral_derived_table(&join.relation);

                let right_plan = self.plan_join_relation(&join.relation, &plan)?;

                let join_type = match join.join_operator {
                    ast::JoinOperator::Join(_) => JoinType::Inner,
                    ast::JoinOperator::Inner(_) => JoinType::Inner,
                    ast::JoinOperator::Left(_) => JoinType::Left,
                    ast::JoinOperator::LeftOuter(_) => JoinType::Left,
                    ast::JoinOperator::Right(_) => JoinType::Right,
                    ast::JoinOperator::RightOuter(_) => JoinType::Right,
                    ast::JoinOperator::FullOuter(_) => JoinType::Full,
                    ast::JoinOperator::CrossJoin(_) => JoinType::Cross,
                    ast::JoinOperator::CrossApply => JoinType::Cross,
                    ast::JoinOperator::OuterApply => JoinType::Left,
                    _ => {
                        return Err(Error::unsupported_feature(
                            "Join type not supported".to_string(),
                        ));
                    }
                };

                let is_apply = matches!(
                    join.join_operator,
                    ast::JoinOperator::CrossApply | ast::JoinOperator::OuterApply
                );
                let is_lateral = is_lateral || is_apply;

                let on_expr = match &join.join_operator {
                    ast::JoinOperator::Join(ast::JoinConstraint::On(expr))
                    | ast::JoinOperator::Inner(ast::JoinConstraint::On(expr))
                    | ast::JoinOperator::Left(ast::JoinConstraint::On(expr))
                    | ast::JoinOperator::LeftOuter(ast::JoinConstraint::On(expr))
                    | ast::JoinOperator::Right(ast::JoinConstraint::On(expr))
                    | ast::JoinOperator::RightOuter(ast::JoinConstraint::On(expr))
                    | ast::JoinOperator::FullOuter(ast::JoinConstraint::On(expr)) => {
                        self.sql_expr_to_expr(expr)?
                    }
                    ast::JoinOperator::Join(ast::JoinConstraint::Using(cols))
                    | ast::JoinOperator::Inner(ast::JoinConstraint::Using(cols))
                    | ast::JoinOperator::Left(ast::JoinConstraint::Using(cols))
                    | ast::JoinOperator::LeftOuter(ast::JoinConstraint::Using(cols))
                    | ast::JoinOperator::Right(ast::JoinConstraint::Using(cols))
                    | ast::JoinOperator::RightOuter(ast::JoinConstraint::Using(cols))
                    | ast::JoinOperator::FullOuter(ast::JoinConstraint::Using(cols)) => {
                        let ident_cols = Self::object_names_to_idents(cols)?;
                        self.build_using_condition(&ident_cols)?
                    }
                    ast::JoinOperator::Join(ast::JoinConstraint::Natural)
                    | ast::JoinOperator::Inner(ast::JoinConstraint::Natural)
                    | ast::JoinOperator::Left(ast::JoinConstraint::Natural)
                    | ast::JoinOperator::LeftOuter(ast::JoinConstraint::Natural)
                    | ast::JoinOperator::Right(ast::JoinConstraint::Natural)
                    | ast::JoinOperator::RightOuter(ast::JoinConstraint::Natural)
                    | ast::JoinOperator::FullOuter(ast::JoinConstraint::Natural) => {
                        self.build_natural_join_condition(&plan, &right_plan)?
                    }
                    ast::JoinOperator::CrossJoin(_) => Expr::Literal(LiteralValue::Boolean(true)),
                    ast::JoinOperator::CrossApply | ast::JoinOperator::OuterApply => {
                        Expr::Literal(LiteralValue::Boolean(true))
                    }
                    _ => {
                        return Err(Error::unsupported_feature(
                            "Join constraint not supported".to_string(),
                        ));
                    }
                };

                plan = if is_lateral {
                    LogicalPlan::new(PlanNode::LateralJoin {
                        left: plan.root,
                        right: right_plan.root,
                        on: on_expr,
                        join_type,
                    })
                } else {
                    LogicalPlan::new(PlanNode::Join {
                        left: plan.root,
                        right: right_plan.root,
                        on: on_expr,
                        join_type,
                    })
                };
            }
        }

        Ok(plan)
    }

    pub(super) fn table_factor_to_plan(&self, factor: &ast::TableFactor) -> Result<LogicalPlan> {
        match factor {
            ast::TableFactor::Table {
                name,
                alias,
                args: Some(func_args),
                ..
            } => {
                let function_name = name.to_string();
                let table_alias = alias.as_ref().map(|a| a.name.value.clone());

                let args = func_args
                    .args
                    .iter()
                    .filter_map(|arg| match arg {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                            Some(self.sql_expr_to_expr(expr))
                        }
                        ast::FunctionArg::Named {
                            arg: ast::FunctionArgExpr::Expr(expr),
                            ..
                        } => Some(self.sql_expr_to_expr(expr)),
                        _ => None,
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(LogicalPlan::new(PlanNode::TableValuedFunction {
                    function_name,
                    args,
                    alias: table_alias,
                }))
            }
            ast::TableFactor::Table { name, alias, .. } => {
                let original_name = name.to_string();

                let table_name = self.resolve_table_name(&original_name);
                let table_alias = alias.as_ref().map(|a| a.name.value.clone());
                Ok(LogicalPlan::new(PlanNode::Scan {
                    table_name,
                    alias: table_alias,
                    projection: None,
                }))
            }
            ast::TableFactor::Derived {
                lateral: _lateral,
                subquery,
                alias,
                ..
            } => {
                if alias.is_none() {
                    return Err(Error::invalid_query(
                        "Subquery in FROM clause must have an alias".to_string(),
                    ));
                }

                let subquery_plan = self.query_to_plan(subquery)?;
                let alias_name =
                    alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| {
                            let next_id = self.next_subquery_alias_id();
                            format!("__subquery_{}", next_id)
                        });

                Ok(LogicalPlan::new(PlanNode::SubqueryScan {
                    subquery: Box::new(subquery_plan.root().clone()),
                    alias: alias_name,
                }))
            }
            ast::TableFactor::UNNEST {
                array_exprs,
                alias,
                with_offset,
                with_offset_alias,
                with_ordinality,
                ..
            } => {
                if array_exprs.len() != 1 {
                    return Err(Error::unsupported_feature(
                        "UNNEST with multiple arrays not yet supported".to_string(),
                    ));
                }

                let array_expr = self.sql_expr_to_expr(&array_exprs[0])?;
                let table_alias = alias.as_ref().map(|a| a.name.value.clone());
                let column_alias = alias
                    .as_ref()
                    .and_then(|a| a.columns.first().map(|c| c.name.value.clone()));
                let offset_alias_name = with_offset_alias.as_ref().map(|a| a.value.clone());

                let has_position_column = *with_offset || *with_ordinality;

                Ok(LogicalPlan::new(PlanNode::Unnest {
                    array_expr,
                    alias: table_alias,
                    column_alias,
                    with_offset: has_position_column,
                    offset_alias: offset_alias_name,
                }))
            }
            ast::TableFactor::Function {
                name, args, alias, ..
            } => {
                let function_name = Self::object_name_to_string(name);
                let table_alias = alias.as_ref().map(|a| a.name.value.clone());

                let converted_args = args
                    .iter()
                    .filter_map(|arg| match arg {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                            Some(self.sql_expr_to_expr(expr))
                        }
                        ast::FunctionArg::Named {
                            arg: ast::FunctionArgExpr::Expr(expr),
                            ..
                        } => Some(self.sql_expr_to_expr(expr)),
                        _ => None,
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(LogicalPlan::new(PlanNode::TableValuedFunction {
                    function_name,
                    args: converted_args,
                    alias: table_alias,
                }))
            }
            _ => Err(Error::unsupported_feature(
                "Table factor type not supported".to_string(),
            )),
        }
    }
}
