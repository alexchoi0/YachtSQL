use yachtsql_common::error::Result;
use yachtsql_ir::LogicalPlan;

pub trait OptimizationRule: Send + Sync {
    fn name(&self) -> &'static str;
    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>>;
}

pub struct Optimizer {
    rules: Vec<Box<dyn OptimizationRule>>,
    max_iterations: usize,
}

impl Optimizer {
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            max_iterations: 10,
        }
    }

    pub fn with_rule(mut self, rule: Box<dyn OptimizationRule>) -> Self {
        self.rules.push(rule);
        self
    }

    pub fn with_max_iterations(mut self, max: usize) -> Self {
        self.max_iterations = max;
        self
    }

    pub fn optimize(&self, mut plan: LogicalPlan) -> Result<LogicalPlan> {
        for _ in 0..self.max_iterations {
            let mut changed = false;
            for rule in &self.rules {
                if let Some(new_plan) = rule.optimize(&plan)? {
                    plan = new_plan;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        Ok(plan)
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

pub fn default_optimizer() -> Optimizer {
    Optimizer::new()
        .with_rule(Box::new(ConstantFolding))
        .with_rule(Box::new(PredicatePushdown))
}

pub struct ConstantFolding;

impl OptimizationRule for ConstantFolding {
    fn name(&self) -> &'static str {
        "constant_folding"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let folded = self.fold_plan(plan);
        if &folded != plan {
            Ok(Some(folded))
        } else {
            Ok(None)
        }
    }
}

impl ConstantFolding {
    fn fold_plan(&self, plan: &LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                let folded_input = self.fold_plan(input);
                let folded_predicate = self.fold_expr(predicate);

                if let yachtsql_ir::Expr::Literal(yachtsql_ir::Literal::Bool(true)) =
                    &folded_predicate
                {
                    return folded_input;
                }

                LogicalPlan::Filter {
                    input: Box::new(folded_input),
                    predicate: folded_predicate,
                }
            }
            LogicalPlan::Project {
                input,
                expressions,
                schema,
            } => LogicalPlan::Project {
                input: Box::new(self.fold_plan(input)),
                expressions: expressions.iter().map(|e| self.fold_expr(e)).collect(),
                schema: schema.clone(),
            },
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                schema,
            } => LogicalPlan::Aggregate {
                input: Box::new(self.fold_plan(input)),
                group_by: group_by.iter().map(|e| self.fold_expr(e)).collect(),
                aggregates: aggregates.iter().map(|e| self.fold_expr(e)).collect(),
                schema: schema.clone(),
            },
            LogicalPlan::Sort { input, sort_exprs } => LogicalPlan::Sort {
                input: Box::new(self.fold_plan(input)),
                sort_exprs: sort_exprs.clone(),
            },
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.fold_plan(input)),
                limit: *limit,
                offset: *offset,
            },
            LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
                input: Box::new(self.fold_plan(input)),
            },
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                schema,
            } => LogicalPlan::Join {
                left: Box::new(self.fold_plan(left)),
                right: Box::new(self.fold_plan(right)),
                join_type: *join_type,
                condition: condition.as_ref().map(|c| self.fold_expr(c)),
                schema: schema.clone(),
            },
            other => other.clone(),
        }
    }

    fn fold_expr(&self, expr: &yachtsql_ir::Expr) -> yachtsql_ir::Expr {
        use yachtsql_ir::{BinaryOp, Expr, Literal};

        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left = self.fold_expr(left);
                let right = self.fold_expr(right);

                match (&left, op, &right) {
                    (
                        Expr::Literal(Literal::Int64(a)),
                        BinaryOp::Add,
                        Expr::Literal(Literal::Int64(b)),
                    ) => Expr::Literal(Literal::Int64(a + b)),
                    (
                        Expr::Literal(Literal::Int64(a)),
                        BinaryOp::Sub,
                        Expr::Literal(Literal::Int64(b)),
                    ) => Expr::Literal(Literal::Int64(a - b)),
                    (
                        Expr::Literal(Literal::Int64(a)),
                        BinaryOp::Mul,
                        Expr::Literal(Literal::Int64(b)),
                    ) => Expr::Literal(Literal::Int64(a * b)),
                    (
                        Expr::Literal(Literal::Bool(a)),
                        BinaryOp::And,
                        Expr::Literal(Literal::Bool(b)),
                    ) => Expr::Literal(Literal::Bool(*a && *b)),
                    (
                        Expr::Literal(Literal::Bool(a)),
                        BinaryOp::Or,
                        Expr::Literal(Literal::Bool(b)),
                    ) => Expr::Literal(Literal::Bool(*a || *b)),
                    (Expr::Literal(Literal::Bool(true)), BinaryOp::And, other)
                    | (other, BinaryOp::And, Expr::Literal(Literal::Bool(true))) => other.clone(),
                    (Expr::Literal(Literal::Bool(false)), BinaryOp::And, _)
                    | (_, BinaryOp::And, Expr::Literal(Literal::Bool(false))) => {
                        Expr::Literal(Literal::Bool(false))
                    }
                    (Expr::Literal(Literal::Bool(true)), BinaryOp::Or, _)
                    | (_, BinaryOp::Or, Expr::Literal(Literal::Bool(true))) => {
                        Expr::Literal(Literal::Bool(true))
                    }
                    (Expr::Literal(Literal::Bool(false)), BinaryOp::Or, other)
                    | (other, BinaryOp::Or, Expr::Literal(Literal::Bool(false))) => other.clone(),
                    _ => Expr::BinaryOp {
                        left: Box::new(left),
                        op: *op,
                        right: Box::new(right),
                    },
                }
            }
            Expr::UnaryOp { op, expr } => {
                use yachtsql_ir::UnaryOp;
                let folded = self.fold_expr(expr);
                match (&folded, op) {
                    (Expr::Literal(Literal::Bool(b)), UnaryOp::Not) => {
                        Expr::Literal(Literal::Bool(!b))
                    }
                    (Expr::Literal(Literal::Int64(n)), UnaryOp::Minus) => {
                        Expr::Literal(Literal::Int64(-n))
                    }
                    _ => Expr::UnaryOp {
                        op: *op,
                        expr: Box::new(folded),
                    },
                }
            }
            Expr::Alias { expr, name } => Expr::Alias {
                expr: Box::new(self.fold_expr(expr)),
                name: name.clone(),
            },
            other => other.clone(),
        }
    }
}

pub struct PredicatePushdown;

impl OptimizationRule for PredicatePushdown {
    fn name(&self) -> &'static str {
        "predicate_pushdown"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let pushed = self.push_predicates(plan);
        if &pushed != plan {
            Ok(Some(pushed))
        } else {
            Ok(None)
        }
    }
}

impl PredicatePushdown {
    fn push_predicates(&self, plan: &LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                let pushed_input = self.push_predicates(input);
                self.try_push_filter(pushed_input, predicate.clone())
            }
            LogicalPlan::Project {
                input,
                expressions,
                schema,
            } => LogicalPlan::Project {
                input: Box::new(self.push_predicates(input)),
                expressions: expressions.clone(),
                schema: schema.clone(),
            },
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                schema,
            } => LogicalPlan::Join {
                left: Box::new(self.push_predicates(left)),
                right: Box::new(self.push_predicates(right)),
                join_type: *join_type,
                condition: condition.clone(),
                schema: schema.clone(),
            },
            LogicalPlan::Sort { input, sort_exprs } => LogicalPlan::Sort {
                input: Box::new(self.push_predicates(input)),
                sort_exprs: sort_exprs.clone(),
            },
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.push_predicates(input)),
                limit: *limit,
                offset: *offset,
            },
            LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
                input: Box::new(self.push_predicates(input)),
            },
            other => other.clone(),
        }
    }

    fn try_push_filter(&self, plan: LogicalPlan, predicate: yachtsql_ir::Expr) -> LogicalPlan {
        match plan {
            LogicalPlan::Project {
                input,
                expressions,
                schema,
            } => LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Project {
                    input,
                    expressions,
                    schema,
                }),
                predicate,
            },
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                schema,
            } => {
                if let Some((left_pred, right_pred, remaining)) =
                    self.split_predicate_for_join(&predicate, &left, &right)
                {
                    let new_left = if let Some(lp) = left_pred {
                        Box::new(LogicalPlan::Filter {
                            input: left,
                            predicate: lp,
                        })
                    } else {
                        left
                    };

                    let new_right = if let Some(rp) = right_pred {
                        Box::new(LogicalPlan::Filter {
                            input: right,
                            predicate: rp,
                        })
                    } else {
                        right
                    };

                    let join = LogicalPlan::Join {
                        left: new_left,
                        right: new_right,
                        join_type,
                        condition,
                        schema,
                    };

                    if let Some(rem) = remaining {
                        LogicalPlan::Filter {
                            input: Box::new(join),
                            predicate: rem,
                        }
                    } else {
                        join
                    }
                } else {
                    LogicalPlan::Filter {
                        input: Box::new(LogicalPlan::Join {
                            left,
                            right,
                            join_type,
                            condition,
                            schema,
                        }),
                        predicate,
                    }
                }
            }
            other => LogicalPlan::Filter {
                input: Box::new(other),
                predicate,
            },
        }
    }

    fn split_predicate_for_join(
        &self,
        _predicate: &yachtsql_ir::Expr,
        _left: &LogicalPlan,
        _right: &LogicalPlan,
    ) -> Option<(
        Option<yachtsql_ir::Expr>,
        Option<yachtsql_ir::Expr>,
        Option<yachtsql_ir::Expr>,
    )> {
        None
    }
}
