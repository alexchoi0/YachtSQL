use yachtsql_ir::expr::{Expr, OrderByExpr};
use yachtsql_ir::plan::PlanNode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortColumn {
    pub column_name: String,
    pub table: Option<String>,
    pub ascending: bool,
    pub nulls_first: bool,
}

impl SortColumn {
    pub fn new(column_name: String, ascending: bool) -> Self {
        Self {
            column_name,
            table: None,
            ascending,
            nulls_first: !ascending,
        }
    }

    pub fn with_table(mut self, table: Option<String>) -> Self {
        self.table = table;
        self
    }

    pub fn with_nulls_first(mut self, nulls_first: bool) -> Self {
        self.nulls_first = nulls_first;
        self
    }

    pub fn from_expr(expr: &Expr, ascending: bool, nulls_first: bool) -> Option<Self> {
        match expr {
            Expr::Column { name, table } => Some(Self {
                column_name: name.clone(),
                table: table.clone(),
                ascending,
                nulls_first,
            }),
            _ => None,
        }
    }

    pub fn matches_column(&self, other: &SortColumn) -> bool {
        self.column_name.eq_ignore_ascii_case(&other.column_name)
            && self.ascending == other.ascending
    }

    pub fn matches_column_ignore_direction(&self, other: &SortColumn) -> bool {
        self.column_name.eq_ignore_ascii_case(&other.column_name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OrderingProperty {
    pub columns: Vec<SortColumn>,
}

impl OrderingProperty {
    pub fn empty() -> Self {
        Self { columns: vec![] }
    }

    pub fn new(columns: Vec<SortColumn>) -> Self {
        Self { columns }
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn satisfies(&self, required: &OrderingProperty) -> bool {
        if required.is_empty() {
            return true;
        }
        if self.columns.len() < required.columns.len() {
            return false;
        }
        required
            .columns
            .iter()
            .zip(&self.columns)
            .all(|(req, prov)| prov.matches_column(req))
    }

    pub fn satisfies_prefix(&self, required: &OrderingProperty) -> bool {
        if required.is_empty() {
            return true;
        }
        let prefix_len = self.columns.len().min(required.columns.len());
        self.columns
            .iter()
            .take(prefix_len)
            .zip(required.columns.iter().take(prefix_len))
            .all(|(prov, req)| prov.matches_column(req))
    }

    pub fn prefix_match_len(&self, other: &OrderingProperty) -> usize {
        self.columns
            .iter()
            .zip(&other.columns)
            .take_while(|(a, b)| a.matches_column(b))
            .count()
    }

    pub fn from_order_by(order_by: &[OrderByExpr]) -> Self {
        let columns = order_by
            .iter()
            .filter_map(|o| {
                let ascending = o.asc.unwrap_or(true);
                let nulls_first = o.nulls_first.unwrap_or(!ascending);
                SortColumn::from_expr(&o.expr, ascending, nulls_first)
            })
            .collect();
        Self { columns }
    }

    pub fn from_exprs(exprs: &[Expr]) -> Self {
        let columns = exprs
            .iter()
            .filter_map(|e| SortColumn::from_expr(e, true, false))
            .collect();
        Self { columns }
    }

    pub fn from_column_names(names: &[String]) -> Self {
        let columns = names
            .iter()
            .map(|n| SortColumn::new(n.clone(), true))
            .collect();
        Self { columns }
    }

    pub fn from_index_columns(columns: &[(String, bool)]) -> Self {
        let sort_columns = columns
            .iter()
            .map(|(name, asc)| SortColumn::new(name.clone(), *asc))
            .collect();
        Self {
            columns: sort_columns,
        }
    }

    pub fn to_column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.column_name.clone()).collect()
    }

    pub fn with_column(mut self, column: SortColumn) -> Self {
        self.columns.push(column);
        self
    }

    pub fn merge_with(&self, other: &OrderingProperty) -> OrderingProperty {
        if self.is_empty() {
            return other.clone();
        }
        if other.is_empty() {
            return self.clone();
        }

        let prefix_len = self.prefix_match_len(other);
        if prefix_len == self.len() {
            other.clone()
        } else if prefix_len == other.len() {
            self.clone()
        } else {
            OrderingProperty::new(self.columns[..prefix_len].to_vec())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OrderingRequirement {
    pub orderings: Vec<OrderingProperty>,
}

impl OrderingRequirement {
    pub fn empty() -> Self {
        Self { orderings: vec![] }
    }

    pub fn single(ordering: OrderingProperty) -> Self {
        if ordering.is_empty() {
            Self::empty()
        } else {
            Self {
                orderings: vec![ordering],
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.orderings.is_empty() || self.orderings.iter().all(|o| o.is_empty())
    }

    pub fn add(&mut self, ordering: OrderingProperty) {
        if !ordering.is_empty() {
            self.orderings.push(ordering);
        }
    }

    pub fn any_satisfied_by(&self, provided: &OrderingProperty) -> bool {
        if self.is_empty() {
            return true;
        }
        self.orderings.iter().any(|req| provided.satisfies(req))
    }

    pub fn best_match<'a>(&'a self, provided: &OrderingProperty) -> Option<&'a OrderingProperty> {
        self.orderings
            .iter()
            .filter(|req| provided.satisfies(req))
            .max_by_key(|req| req.len())
    }
}

pub fn provided_ordering(node: &PlanNode, child_ordering: &OrderingProperty) -> OrderingProperty {
    match node {
        PlanNode::Sort { order_by, .. } => OrderingProperty::from_order_by(order_by),

        PlanNode::IndexScan { .. } => OrderingProperty::empty(),

        PlanNode::Scan { .. } => OrderingProperty::empty(),

        PlanNode::Filter { .. } => child_ordering.clone(),

        PlanNode::Projection { expressions, .. } => {
            let projected_columns: Vec<String> = expressions
                .iter()
                .filter_map(|(expr, alias)| match expr {
                    Expr::Column { name, .. } => {
                        Some(alias.clone().unwrap_or_else(|| name.clone()))
                    }
                    _ => alias.clone(),
                })
                .collect();

            let preserved: Vec<SortColumn> = child_ordering
                .columns
                .iter()
                .filter(|col| {
                    projected_columns
                        .iter()
                        .any(|p| p.eq_ignore_ascii_case(&col.column_name))
                })
                .cloned()
                .collect();

            OrderingProperty::new(preserved)
        }

        PlanNode::Aggregate { .. } => OrderingProperty::empty(),

        PlanNode::Join { .. } | PlanNode::AsOfJoin { .. } | PlanNode::LateralJoin { .. } => {
            OrderingProperty::empty()
        }

        PlanNode::Limit { .. } => child_ordering.clone(),

        PlanNode::Distinct { .. } | PlanNode::DistinctOn { .. } => OrderingProperty::empty(),

        PlanNode::Union { .. } | PlanNode::Intersect { .. } | PlanNode::Except { .. } => {
            OrderingProperty::empty()
        }

        PlanNode::SubqueryScan { .. } => child_ordering.clone(),

        PlanNode::Cte { .. } => child_ordering.clone(),

        PlanNode::Window { .. } => child_ordering.clone(),

        PlanNode::TableSample { .. } => OrderingProperty::empty(),

        PlanNode::Pivot { .. } | PlanNode::Unpivot { .. } => OrderingProperty::empty(),

        PlanNode::Update { .. }
        | PlanNode::Delete { .. }
        | PlanNode::Insert { .. }
        | PlanNode::Truncate { .. }
        | PlanNode::AlterTable { .. }
        | PlanNode::InsertOnConflict { .. }
        | PlanNode::Merge { .. } => OrderingProperty::empty(),

        PlanNode::Unnest { .. }
        | PlanNode::TableValuedFunction { .. }
        | PlanNode::ArrayJoin { .. }
        | PlanNode::EmptyRelation
        | PlanNode::Values { .. } => OrderingProperty::empty(),
    }
}

pub fn required_ordering(node: &PlanNode) -> OrderingRequirement {
    match node {
        PlanNode::Sort { order_by, .. } => {
            OrderingRequirement::single(OrderingProperty::from_order_by(order_by))
        }

        PlanNode::Aggregate { group_by, .. } if !group_by.is_empty() => {
            OrderingRequirement::single(OrderingProperty::from_exprs(group_by))
        }

        PlanNode::Join { on, .. } | PlanNode::LateralJoin { on, .. } => {
            if let Some(keys) = extract_equi_join_keys(on) {
                OrderingRequirement::single(OrderingProperty::from_column_names(&keys))
            } else {
                OrderingRequirement::empty()
            }
        }

        PlanNode::AsOfJoin {
            equality_condition, ..
        } => {
            if let Some(keys) = extract_equi_join_keys(equality_condition) {
                OrderingRequirement::single(OrderingProperty::from_column_names(&keys))
            } else {
                OrderingRequirement::empty()
            }
        }

        PlanNode::Window { window_exprs, .. } => {
            let mut requirement = OrderingRequirement::empty();
            for (expr, _) in window_exprs {
                if let Expr::WindowFunction {
                    partition_by,
                    order_by,
                    ..
                } = expr
                {
                    let mut cols: Vec<SortColumn> = partition_by
                        .iter()
                        .filter_map(|e| SortColumn::from_expr(e, true, false))
                        .collect();

                    cols.extend(order_by.iter().filter_map(|o| {
                        let asc = o.asc.unwrap_or(true);
                        let nulls_first = o.nulls_first.unwrap_or(!asc);
                        SortColumn::from_expr(&o.expr, asc, nulls_first)
                    }));

                    if !cols.is_empty() {
                        requirement.add(OrderingProperty::new(cols));
                    }
                }
            }
            requirement
        }

        PlanNode::DistinctOn { expressions, .. } => {
            OrderingRequirement::single(OrderingProperty::from_exprs(expressions))
        }

        _ => OrderingRequirement::empty(),
    }
}

fn extract_equi_join_keys(on: &Expr) -> Option<Vec<String>> {
    match on {
        Expr::BinaryOp {
            left,
            op: yachtsql_ir::expr::BinaryOp::Equal,
            right,
        } => {
            if let (
                Expr::Column {
                    name: left_name, ..
                },
                Expr::Column {
                    name: right_name, ..
                },
            ) = (left.as_ref(), right.as_ref())
            {
                Some(vec![left_name.clone(), right_name.clone()])
            } else {
                None
            }
        }
        Expr::BinaryOp {
            left,
            op: yachtsql_ir::expr::BinaryOp::And,
            right,
        } => {
            let left_keys = extract_equi_join_keys(left)?;
            let right_keys = extract_equi_join_keys(right)?;
            let mut keys = left_keys;
            keys.extend(right_keys);
            Some(keys)
        }
        _ => None,
    }
}

pub fn is_equi_join(on: &Expr) -> bool {
    extract_equi_join_keys(on).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_column_creation() {
        let col = SortColumn::new("id".to_string(), true);
        assert_eq!(col.column_name, "id");
        assert!(col.ascending);
        assert!(!col.nulls_first);

        let col_desc = SortColumn::new("name".to_string(), false);
        assert!(!col_desc.ascending);
        assert!(col_desc.nulls_first);
    }

    #[test]
    fn test_sort_column_matches() {
        let col1 = SortColumn::new("id".to_string(), true);
        let col2 = SortColumn::new("ID".to_string(), true);
        let col3 = SortColumn::new("id".to_string(), false);

        assert!(col1.matches_column(&col2));
        assert!(!col1.matches_column(&col3));
        assert!(col1.matches_column_ignore_direction(&col3));
    }

    #[test]
    fn test_ordering_property_empty() {
        let empty = OrderingProperty::empty();
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
    }

    #[test]
    fn test_ordering_property_satisfies_empty_required() {
        let provided = OrderingProperty::new(vec![SortColumn::new("a".to_string(), true)]);
        let required = OrderingProperty::empty();
        assert!(provided.satisfies(&required));
    }

    #[test]
    fn test_ordering_property_satisfies_exact_match() {
        let provided = OrderingProperty::new(vec![SortColumn::new("a".to_string(), true)]);
        let required = OrderingProperty::new(vec![SortColumn::new("a".to_string(), true)]);
        assert!(provided.satisfies(&required));
    }

    #[test]
    fn test_ordering_property_satisfies_prefix() {
        let provided = OrderingProperty::new(vec![
            SortColumn::new("a".to_string(), true),
            SortColumn::new("b".to_string(), true),
        ]);
        let required = OrderingProperty::new(vec![SortColumn::new("a".to_string(), true)]);
        assert!(provided.satisfies(&required));
    }

    #[test]
    fn test_ordering_property_not_satisfies_longer_required() {
        let provided = OrderingProperty::new(vec![SortColumn::new("a".to_string(), true)]);
        let required = OrderingProperty::new(vec![
            SortColumn::new("a".to_string(), true),
            SortColumn::new("b".to_string(), true),
        ]);
        assert!(!provided.satisfies(&required));
    }

    #[test]
    fn test_ordering_property_not_satisfies_different_direction() {
        let provided = OrderingProperty::new(vec![SortColumn::new("a".to_string(), true)]);
        let required = OrderingProperty::new(vec![SortColumn::new("a".to_string(), false)]);
        assert!(!provided.satisfies(&required));
    }

    #[test]
    fn test_ordering_property_from_order_by() {
        let order_by = vec![
            OrderByExpr {
                expr: Expr::Column {
                    name: "category".to_string(),
                    table: None,
                },
                asc: Some(true),
                nulls_first: Some(false),
                collation: None,
                with_fill: None,
            },
            OrderByExpr {
                expr: Expr::Column {
                    name: "amount".to_string(),
                    table: None,
                },
                asc: Some(false),
                nulls_first: Some(true),
                collation: None,
                with_fill: None,
            },
        ];

        let ordering = OrderingProperty::from_order_by(&order_by);
        assert_eq!(ordering.len(), 2);
        assert_eq!(ordering.columns[0].column_name, "category");
        assert!(ordering.columns[0].ascending);
        assert_eq!(ordering.columns[1].column_name, "amount");
        assert!(!ordering.columns[1].ascending);
    }

    #[test]
    fn test_ordering_property_from_exprs() {
        let exprs = vec![
            Expr::Column {
                name: "a".to_string(),
                table: None,
            },
            Expr::Column {
                name: "b".to_string(),
                table: Some("t".to_string()),
            },
        ];

        let ordering = OrderingProperty::from_exprs(&exprs);
        assert_eq!(ordering.len(), 2);
        assert_eq!(ordering.columns[0].column_name, "a");
        assert_eq!(ordering.columns[1].column_name, "b");
    }

    #[test]
    fn test_ordering_property_prefix_match_len() {
        let a = OrderingProperty::new(vec![
            SortColumn::new("x".to_string(), true),
            SortColumn::new("y".to_string(), true),
        ]);
        let b = OrderingProperty::new(vec![
            SortColumn::new("x".to_string(), true),
            SortColumn::new("z".to_string(), true),
        ]);
        assert_eq!(a.prefix_match_len(&b), 1);
    }

    #[test]
    fn test_ordering_requirement_any_satisfied() {
        let mut req = OrderingRequirement::empty();
        req.add(OrderingProperty::new(vec![SortColumn::new(
            "a".to_string(),
            true,
        )]));
        req.add(OrderingProperty::new(vec![SortColumn::new(
            "b".to_string(),
            true,
        )]));

        let provided_a = OrderingProperty::new(vec![SortColumn::new("a".to_string(), true)]);
        let provided_b = OrderingProperty::new(vec![SortColumn::new("b".to_string(), true)]);
        let provided_c = OrderingProperty::new(vec![SortColumn::new("c".to_string(), true)]);

        assert!(req.any_satisfied_by(&provided_a));
        assert!(req.any_satisfied_by(&provided_b));
        assert!(!req.any_satisfied_by(&provided_c));
    }

    #[test]
    fn test_is_equi_join() {
        let equi = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "a".to_string(),
                table: Some("t1".to_string()),
            }),
            op: yachtsql_ir::expr::BinaryOp::Equal,
            right: Box::new(Expr::Column {
                name: "b".to_string(),
                table: Some("t2".to_string()),
            }),
        };
        assert!(is_equi_join(&equi));

        let non_equi = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "a".to_string(),
                table: None,
            }),
            op: yachtsql_ir::expr::BinaryOp::GreaterThan,
            right: Box::new(Expr::Column {
                name: "b".to_string(),
                table: None,
            }),
        };
        assert!(!is_equi_join(&non_equi));
    }

    #[test]
    fn test_merge_orderings() {
        let a = OrderingProperty::new(vec![
            SortColumn::new("x".to_string(), true),
            SortColumn::new("y".to_string(), true),
        ]);
        let b = OrderingProperty::new(vec![
            SortColumn::new("x".to_string(), true),
            SortColumn::new("y".to_string(), true),
            SortColumn::new("z".to_string(), true),
        ]);

        let merged = a.merge_with(&b);
        assert_eq!(merged.len(), 3);
    }
}
