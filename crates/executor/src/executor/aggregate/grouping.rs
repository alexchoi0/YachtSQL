use yachtsql_ir::Expr;

pub(super) fn get_grouping_column_index(agg_expr: &Expr, group_by: &[Expr]) -> Option<usize> {
    let arg = match agg_expr {
        Expr::Aggregate { args, .. } => args.first(),
        Expr::Alias { expr, .. } => {
            return get_grouping_column_index(expr, group_by);
        }
        _ => None,
    }?;

    for (i, group_expr) in group_by.iter().enumerate() {
        if exprs_match(arg, group_expr) {
            return Some(i);
        }
    }
    None
}

pub(super) fn exprs_match(a: &Expr, b: &Expr) -> bool {
    match (a, b) {
        (
            Expr::Column {
                name: n1,
                table: t1,
                ..
            },
            Expr::Column {
                name: n2,
                table: t2,
                ..
            },
        ) => {
            n1.eq_ignore_ascii_case(n2)
                && match (t1, t2) {
                    (Some(t1), Some(t2)) => t1.eq_ignore_ascii_case(t2),
                    (None, None) => true,
                    _ => true,
                }
        }
        (Expr::Alias { expr: e1, .. }, e2) => exprs_match(e1, e2),
        (e1, Expr::Alias { expr: e2, .. }) => exprs_match(e1, e2),
        _ => format!("{:?}", a) == format!("{:?}", b),
    }
}

pub(super) fn compute_grouping_id(agg_expr: &Expr, group_by: &[Expr], active_indices: &[usize]) -> i64 {
    let args = match agg_expr {
        Expr::Aggregate { args, .. } => args,
        Expr::Alias { expr, .. } => {
            return compute_grouping_id(expr, group_by, active_indices);
        }
        _ => return 0,
    };

    let mut result: i64 = 0;
    let n = args.len();
    for (arg_pos, arg) in args.iter().enumerate() {
        let mut is_active = true;
        for (i, group_expr) in group_by.iter().enumerate() {
            if exprs_match(arg, group_expr) {
                is_active = active_indices.contains(&i);
                break;
            }
        }
        if !is_active {
            result |= 1 << (n - 1 - arg_pos);
        }
    }
    result
}
