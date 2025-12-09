use std::cell::RefCell;
use std::rc::Rc;

use yachtsql_core::error::{Error, Result};
use yachtsql_optimizer::LogicalPlan;
use yachtsql_optimizer::plan::PlanNode;
use yachtsql_storage::{Field, Schema};

use super::physical_plan::{
    AggregateExec, ArrayJoinExec, CteExec, DistinctExec, DistinctOnExec, EmptyRelationExec,
    ExceptExec, ExecutionPlan, FilterExec, HashJoinExec, IntersectExec, LimitExec,
    NestedLoopJoinExec, PhysicalPlan, PivotAggregateFunction, PivotExec, SampleSize,
    SamplingMethod, SortExec, SubqueryScanExec, TableSampleExec, TableScanExec, UnionExec,
    UnnestExec, UnpivotExec, WindowExec,
};

pub struct PhysicalPlanner {
    _dialect: yachtsql_parser::DialectType,
    _feature_registry: Rc<yachtsql_capability::FeatureRegistry>,
}

impl PhysicalPlanner {
    pub fn new(
        dialect: yachtsql_parser::DialectType,
        feature_registry: Rc<yachtsql_capability::FeatureRegistry>,
    ) -> Self {
        Self {
            _dialect: dialect,
            _feature_registry: feature_registry,
        }
    }

    pub fn create_physical_plan(&self, logical_plan: LogicalPlan) -> Result<PhysicalPlan> {
        let root_exec = self.create_exec_node(logical_plan.root())?;
        Ok(PhysicalPlan::new(root_exec))
    }

    fn create_exec_node(&self, node: &PlanNode) -> Result<Rc<dyn ExecutionPlan>> {
        match node {
            PlanNode::Scan {
                table_name,
                alias,
                projection,
                ..
            } => self.create_table_scan(table_name, alias.as_deref(), projection.as_deref()),



            PlanNode::IndexScan { .. } => {
                Err(Error::UnsupportedFeature(
                    "IndexScan requires storage access. Use LogicalToPhysicalPlanner instead.".to_string(),
                ))
            }

            PlanNode::Filter { predicate, input } => self.create_filter(predicate, input),

            PlanNode::Projection { expressions, input } => {
                self.create_projection(expressions, input)
            }

            PlanNode::Limit {
                limit,
                offset,
                input,
            } => self.create_limit(*limit, *offset, input),

            PlanNode::Distinct { input } => self.create_distinct(input),

            PlanNode::DistinctOn { expressions, input } => {
                self.create_distinct_on(expressions, input)
            }

            PlanNode::Window {
                window_exprs,
                input,
            } => self.create_window(window_exprs, input),

            PlanNode::Unnest {
                array_expr,
                alias: _,
                column_alias: _,
                with_offset,
                offset_alias: _,
            } => self.create_unnest(array_expr, *with_offset),

            PlanNode::TableValuedFunction { .. } => Err(Error::unsupported_feature(
                "Physical plan generation for TableValuedFunction requires storage access. Use LogicalToPhysicalPlanner instead."
                    .to_string(),
            )),

            PlanNode::ArrayJoin {
                input,
                arrays,
                is_left,
                is_unaligned,
            } => self.create_array_join(input, arrays, *is_left, *is_unaligned),

            PlanNode::Aggregate {
                group_by,
                aggregates,
                input,
                grouping_metadata: _,
            } => self.create_aggregate(group_by, aggregates, None, input),

            PlanNode::Sort {
                order_by,
                input,
            } => self.create_sort(order_by, input),

            PlanNode::Join {
                left,
                right,
                join_type,
                on,
            } => self.create_join(left, right, *join_type, Some(on)),

            PlanNode::LateralJoin {
                left,
                right,
                join_type,
                on: _,
            } => self.create_lateral_join(left, right, *join_type),

            PlanNode::Union { left, right, all } => self.create_union(left, right, *all),

            PlanNode::Intersect { left, right, all } => self.create_intersect(left, right, *all),

            PlanNode::Except { left, right, all } => self.create_except(left, right, *all),

            PlanNode::Cte {
                name: _,
                cte_plan,
                input,
                recursive: _,
                use_union_all: _,
                materialization_hint,
                column_aliases: _,
            } => self.create_cte(cte_plan, input, materialization_hint),

            PlanNode::SubqueryScan { subquery, alias: _ } => self.create_subquery_scan(subquery),

            PlanNode::Update { .. } => Err(Error::unsupported_feature(
                "Physical plan generation for Update not yet implemented (use DML executor)"
                    .to_string(),
            )),

            PlanNode::Delete { .. } => Err(Error::unsupported_feature(
                "Physical plan generation for Delete not yet implemented (use DML executor)"
                    .to_string(),
            )),

            PlanNode::Truncate { .. } => Err(Error::unsupported_feature(
                "Physical plan generation for Truncate not yet implemented (use DML executor)"
                    .to_string(),
            )),

            PlanNode::InsertOnConflict { .. } => Err(Error::unsupported_feature(
                "Physical plan generation for InsertOnConflict not yet implemented (use DML executor)"
                    .to_string(),
            )),

            PlanNode::Insert { .. } => Err(Error::unsupported_feature(
                "Physical plan generation for Insert not yet implemented (use DML executor)"
                    .to_string(),
            )),

            PlanNode::Merge { .. } => Err(Error::unsupported_feature(
                "Physical plan generation for Merge not yet implemented (use DML executor)"
                    .to_string(),
            )),

            PlanNode::AlterTable { .. } => Err(Error::unsupported_feature(
                "Physical plan generation for AlterTable not yet implemented (use DDL executor)"
                    .to_string(),
            )),

            PlanNode::EmptyRelation => {
                let schema = Schema::from_fields(vec![]);
                Ok(Rc::new(EmptyRelationExec::new(schema)))
            }

            PlanNode::Values { rows } => {
                use super::physical_plan::{infer_values_schema, ValuesExec};
                let schema = infer_values_schema(rows);
                Ok(Rc::new(ValuesExec::new(schema, rows.clone())))
            }

            PlanNode::TableSample {
                input,
                method,
                size,
                seed,
            } => self.create_tablesample(input, method, size, *seed),

            PlanNode::Pivot {
                input,
                aggregate_expr,
                aggregate_function,
                pivot_column,
                pivot_values,
                group_by_columns,
            } => self.create_pivot(
                input,
                aggregate_expr,
                aggregate_function,
                pivot_column,
                pivot_values,
                group_by_columns,
            ),

            PlanNode::Unpivot {
                input,
                value_column,
                name_column,
                unpivot_columns,
            } => self.create_unpivot(input, value_column, name_column, unpivot_columns),
        }
    }

    fn create_table_scan(
        &self,
        table_name: &str,
        _alias: Option<&str>,
        _projection: Option<&[String]>,
    ) -> Result<Rc<dyn ExecutionPlan>> {
        let schema = Schema::from_fields(vec![]);
        let storage = Rc::new(RefCell::new(yachtsql_storage::Storage::new()));
        Ok(Rc::new(TableScanExec::new(
            schema,
            table_name.to_string(),
            storage,
        )))
    }

    fn create_filter(
        &self,
        _predicate: &yachtsql_optimizer::expr::Expr,
        input: &PlanNode,
    ) -> Result<Rc<dyn ExecutionPlan>> {
        let input_exec = self.create_exec_node(input)?;
        Ok(Rc::new(FilterExec::new(input_exec, _predicate.clone())))
    }

    #[allow(dead_code)]
    fn create_projection(
        &self,
        _expressions: &[(yachtsql_optimizer::expr::Expr, Option<String>)],
        _input: &PlanNode,
    ) -> Result<Rc<dyn ExecutionPlan>> {
        Err(Error::unsupported_feature(
            "ProjectionWithExprExec not implemented".to_string(),
        ))
    }

    fn create_limit(
        &self,
        limit: usize,
        offset: usize,
        input: &PlanNode,
    ) -> Result<Rc<dyn ExecutionPlan>> {
        let input_exec = self.create_exec_node(input)?;
        Ok(Rc::new(LimitExec::new(input_exec, limit, offset)))
    }

    fn create_distinct(&self, input: &PlanNode) -> Result<Rc<dyn ExecutionPlan>> {
        let input_exec = self.create_exec_node(input)?;
        Ok(Rc::new(DistinctExec::new(input_exec)))
    }

    fn create_distinct_on(
        &self,
        expressions: &[yachtsql_optimizer::expr::Expr],
        input: &PlanNode,
    ) -> Result<Rc<dyn ExecutionPlan>> {
        let input_exec = self.create_exec_node(input)?;
        Ok(Rc::new(DistinctOnExec::new(
            input_exec,
            expressions.to_vec(),
        )))
    }

    fn create_window(
        &self,
        window_exprs: &[(yachtsql_optimizer::expr::Expr, Option<String>)],
        input: &PlanNode,
    ) -> Result<Rc<dyn ExecutionPlan>> {
        let input_exec = self.create_exec_node(input)?;
        Ok(Rc::new(WindowExec::new(input_exec, window_exprs.to_vec())?))
    }

    fn create_unnest(
        &self,
        array_expr: &yachtsql_optimizer::expr::Expr,
        with_offset: bool,
    ) -> Result<Rc<dyn ExecutionPlan>> {
        let element_type = yachtsql_core::types::DataType::String;
        let mut fields = vec![Field::nullable("element".to_string(), element_type)];

        if with_offset {
            fields.push(Field::required(
                "offset".to_string(),
                yachtsql_core::types::DataType::Int64,
            ));
        }

        let schema = Schema::from_fields(fields);

        Ok(Rc::new(UnnestExec {
            schema,
            array_expr: array_expr.clone(),
            with_offset,
        }))
    }

    fn create_array_join(
        &self,
        input: &PlanNode,
        arrays: &[(yachtsql_optimizer::expr::Expr, Option<String>)],
        is_left: bool,
        is_unaligned: bool,
    ) -> Result<Rc<dyn ExecutionPlan>> {
        let input_exec = self.create_exec_node(input)?;
        Ok(Rc::new(ArrayJoinExec::new(
            input_exec,
            arrays.to_vec(),
            is_left,
            is_unaligned,
        )?))
    }

    fn create_aggregate(
        &self,
        group_by: &[yachtsql_optimizer::expr::Expr],
        aggregates: &[yachtsql_optimizer::expr::Expr],
        having: Option<&yachtsql_optimizer::expr::Expr>,
        input: &PlanNode,
    ) -> Result<Rc<dyn ExecutionPlan>> {
        let input_exec = self.create_exec_node(input)?;
        let aggregates_with_alias: Vec<(yachtsql_optimizer::expr::Expr, Option<String>)> =
            aggregates
                .iter()
                .enumerate()
                .map(|(i, expr)| (expr.clone(), Some(format!("agg_{}", i))))
                .collect();
        Ok(Rc::new(AggregateExec::new(
            input_exec,
            group_by.to_vec(),
            aggregates_with_alias,
            having.cloned(),
        )?))
    }

    fn create_sort(
        &self,
        order_by: &[yachtsql_optimizer::expr::OrderByExpr],
        input: &PlanNode,
    ) -> Result<Rc<dyn ExecutionPlan>> {
        let input_exec = self.create_exec_node(input)?;
        Ok(Rc::new(SortExec::new(input_exec, order_by.to_vec())?))
    }

    fn create_join(
        &self,
        left: &PlanNode,
        right: &PlanNode,
        join_type: yachtsql_optimizer::plan::JoinType,
        on_condition: Option<&yachtsql_optimizer::expr::Expr>,
    ) -> Result<Rc<dyn ExecutionPlan>> {
        let left_exec = self.create_exec_node(left)?;
        let right_exec = self.create_exec_node(right)?;

        let left_tables = self.collect_table_names(left);
        let right_tables = self.collect_table_names(right);

        match on_condition {
            Some(condition) => {
                let equi_conditions =
                    self.extract_equi_join_conditions(condition, &left_tables, &right_tables);
                if !equi_conditions.is_empty() {
                    Ok(Rc::new(HashJoinExec::new(
                        left_exec,
                        right_exec,
                        join_type,
                        equi_conditions,
                    )?))
                } else {
                    Ok(Rc::new(NestedLoopJoinExec::new(
                        left_exec,
                        right_exec,
                        join_type,
                        Some(condition.clone()),
                    )?))
                }
            }
            None => Ok(Rc::new(NestedLoopJoinExec::new(
                left_exec, right_exec, join_type, None,
            )?)),
        }
    }

    fn create_lateral_join(
        &self,
        _left: &PlanNode,
        _right: &PlanNode,
        _join_type: yachtsql_optimizer::plan::JoinType,
    ) -> Result<Rc<dyn ExecutionPlan>> {
        Err(Error::unsupported_feature(
            "LATERAL joins must be executed via LogicalToPhysicalPlanner".to_string(),
        ))
    }

    fn collect_table_names(&self, plan: &PlanNode) -> std::collections::HashSet<String> {
        let mut tables = std::collections::HashSet::new();
        self.collect_table_names_recursive(plan, &mut tables);
        tables
    }

    fn collect_table_names_recursive(
        &self,
        plan: &PlanNode,
        tables: &mut std::collections::HashSet<String>,
    ) {
        match plan {
            PlanNode::Scan {
                table_name, alias, ..
            } => {
                if let Some(alias) = alias {
                    tables.insert(alias.clone());
                }
                tables.insert(table_name.clone());
            }
            _ => {
                for child in plan.children() {
                    self.collect_table_names_recursive(child, tables);
                }
            }
        }
    }

    fn get_expr_table(&self, expr: &yachtsql_optimizer::expr::Expr) -> Option<String> {
        match expr {
            yachtsql_optimizer::expr::Expr::Column { table, .. } => table.clone(),
            yachtsql_optimizer::expr::Expr::BinaryOp { left, op: _, right } => {
                let left_table = self.get_expr_table(left);
                let right_table = self.get_expr_table(right);
                match (&left_table, &right_table) {
                    (Some(l), Some(r)) if l != r => panic!(
                        "get_expr_table: BinaryOp references columns from different tables: {} and {}",
                        l, r
                    ),
                    (Some(_), _) => left_table,
                    (_, Some(_)) => right_table,
                    (None, None) => panic!(
                        "get_expr_table: BinaryOp has no column references (both sides are literals)"
                    ),
                }
            }
            _ => panic!("get_expr_table: unhandled expression type: {:?}", expr),
        }
    }

    fn extract_equi_join_conditions(
        &self,
        condition: &yachtsql_optimizer::expr::Expr,
        left_tables: &std::collections::HashSet<String>,
        _right_tables: &std::collections::HashSet<String>,
    ) -> Vec<(
        yachtsql_optimizer::expr::Expr,
        yachtsql_optimizer::expr::Expr,
    )> {
        let mut equi_conditions = Vec::new();

        match condition {
            yachtsql_optimizer::expr::Expr::BinaryOp { left, op, right } => {
                if *op == yachtsql_optimizer::BinaryOp::Equal {
                    let left_table = self.get_expr_table(left);
                    let right_table = self.get_expr_table(right);

                    let left_is_left_side =
                        left_table.as_ref().is_some_and(|t| left_tables.contains(t));
                    let right_is_left_side = right_table
                        .as_ref()
                        .is_some_and(|t| left_tables.contains(t));

                    if left_is_left_side && !right_is_left_side {
                        equi_conditions.push((*left.clone(), *right.clone()));
                    } else if right_is_left_side && !left_is_left_side {
                        equi_conditions.push((*right.clone(), *left.clone()));
                    } else {
                        equi_conditions.push((*left.clone(), *right.clone()));
                    }
                } else if *op == yachtsql_optimizer::BinaryOp::And {
                    equi_conditions.extend(self.extract_equi_join_conditions(
                        left,
                        left_tables,
                        _right_tables,
                    ));
                    equi_conditions.extend(self.extract_equi_join_conditions(
                        right,
                        left_tables,
                        _right_tables,
                    ));
                }
            }
            _ => {}
        }

        equi_conditions
    }

    fn create_union(
        &self,
        left: &PlanNode,
        right: &PlanNode,
        all: bool,
    ) -> Result<Rc<dyn ExecutionPlan>> {
        let left_exec = self.create_exec_node(left)?;
        let right_exec = self.create_exec_node(right)?;
        Ok(Rc::new(UnionExec::new(left_exec, right_exec, all)?))
    }

    fn create_intersect(
        &self,
        left: &PlanNode,
        right: &PlanNode,
        all: bool,
    ) -> Result<Rc<dyn ExecutionPlan>> {
        let left_exec = self.create_exec_node(left)?;
        let right_exec = self.create_exec_node(right)?;
        Ok(Rc::new(IntersectExec::new(left_exec, right_exec, all)?))
    }

    fn create_except(
        &self,
        left: &PlanNode,
        right: &PlanNode,
        all: bool,
    ) -> Result<Rc<dyn ExecutionPlan>> {
        let left_exec = self.create_exec_node(left)?;
        let right_exec = self.create_exec_node(right)?;
        Ok(Rc::new(ExceptExec::new(left_exec, right_exec, all)?))
    }

    fn create_cte(
        &self,
        cte_plan: &PlanNode,
        input: &PlanNode,
        materialization_hint: &Option<sqlparser::ast::CteAsMaterialized>,
    ) -> Result<Rc<dyn ExecutionPlan>> {
        let cte_exec = self.create_exec_node(cte_plan)?;
        let input_exec = self.create_exec_node(input)?;

        let materialized = match materialization_hint {
            Some(sqlparser::ast::CteAsMaterialized::Materialized) => true,
            Some(sqlparser::ast::CteAsMaterialized::NotMaterialized) => false,
            None => true,
        };

        Ok(Rc::new(CteExec::new(cte_exec, input_exec, materialized)))
    }

    fn create_subquery_scan(&self, subquery: &PlanNode) -> Result<Rc<dyn ExecutionPlan>> {
        let subquery_exec = self.create_exec_node(subquery)?;
        Ok(Rc::new(SubqueryScanExec::new(subquery_exec)))
    }

    fn create_tablesample(
        &self,
        input: &PlanNode,
        method: &yachtsql_optimizer::plan::SamplingMethod,
        size: &yachtsql_optimizer::plan::SampleSize,
        seed: Option<u64>,
    ) -> Result<Rc<dyn ExecutionPlan>> {
        let input_exec = self.create_exec_node(input)?;

        let physical_method = match method {
            yachtsql_optimizer::plan::SamplingMethod::Bernoulli => SamplingMethod::Bernoulli,
            yachtsql_optimizer::plan::SamplingMethod::System => SamplingMethod::System,
        };

        let physical_size = match size {
            yachtsql_optimizer::plan::SampleSize::Percent(pct) => SampleSize::Percent(*pct),
            yachtsql_optimizer::plan::SampleSize::Rows(rows) => SampleSize::Rows(*rows),
        };

        Ok(Rc::new(TableSampleExec::new(
            input_exec,
            physical_method,
            physical_size,
            seed,
        )?))
    }

    fn create_pivot(
        &self,
        input: &PlanNode,
        aggregate_expr: &yachtsql_optimizer::expr::Expr,
        aggregate_function: &str,
        pivot_column: &str,
        pivot_values: &[yachtsql_core::types::Value],
        group_by_columns: &[String],
    ) -> Result<Rc<dyn ExecutionPlan>> {
        let input_exec = self.create_exec_node(input)?;

        let agg_fn = match aggregate_function.to_uppercase().as_str() {
            "SUM" => PivotAggregateFunction::Sum,
            "AVG" => PivotAggregateFunction::Avg,
            "COUNT" => PivotAggregateFunction::Count,
            "MIN" => PivotAggregateFunction::Min,
            "MAX" => PivotAggregateFunction::Max,
            "FIRST" => PivotAggregateFunction::First,
            "LAST" => PivotAggregateFunction::Last,
            _ => {
                return Err(Error::unsupported_feature(format!(
                    "Unsupported PIVOT aggregate function: {}",
                    aggregate_function
                )));
            }
        };

        Ok(Rc::new(PivotExec::new(
            input_exec,
            aggregate_expr.clone(),
            agg_fn,
            pivot_column.to_string(),
            pivot_values.to_vec(),
            group_by_columns.to_vec(),
        )?))
    }

    fn create_unpivot(
        &self,
        input: &PlanNode,
        value_column: &str,
        name_column: &str,
        unpivot_columns: &[String],
    ) -> Result<Rc<dyn ExecutionPlan>> {
        let input_exec = self.create_exec_node(input)?;

        Ok(Rc::new(UnpivotExec::new(
            input_exec,
            value_column.to_string(),
            name_column.to_string(),
            unpivot_columns.to_vec(),
        )?))
    }
}

impl Default for PhysicalPlanner {
    fn default() -> Self {
        Self::new(
            yachtsql_parser::DialectType::PostgreSQL,
            Rc::new(yachtsql_capability::FeatureRegistry::new(
                yachtsql_parser::DialectType::PostgreSQL,
            )),
        )
    }
}
