use yachtsql_common::error::Error;
use yachtsql_ir::expr::Expr;
use yachtsql_ir::plan::GroupingMetadata;

const MAX_CUBE_COLUMNS: usize = 10;

pub fn generate_rollup_sets(columns: Vec<Expr>) -> Vec<Vec<Expr>> {
    let mut sets = Vec::new();

    for i in 0..=columns.len() {
        let set = columns[..columns.len() - i].to_vec();
        sets.push(set);
    }

    sets
}

pub fn generate_cube_sets(columns: Vec<Expr>) -> Result<Vec<Vec<Expr>>, Error> {
    let n = columns.len();

    if n > MAX_CUBE_COLUMNS {
        let num_sets = 1 << n;
        return Err(Error::ExecutionError(format!(
            "CUBE with {} columns would produce {} grouping sets. Maximum allowed is {} columns ({} sets). \
             Consider using GROUPING SETS with explicit combinations.",
            n,
            num_sets,
            MAX_CUBE_COLUMNS,
            1 << MAX_CUBE_COLUMNS
        )));
    }

    let num_sets = 1 << n;
    let mut sets = Vec::with_capacity(num_sets);

    for mask in (0..num_sets).rev() {
        let mut set = Vec::new();
        for (i, col) in columns.iter().enumerate() {
            if mask & (1 << i) != 0 {
                set.push(col.clone());
            }
        }
        sets.push(set);
    }

    Ok(sets)
}

pub fn generate_explicit_sets(sets: Vec<Vec<Expr>>) -> Result<Vec<Vec<Expr>>, Error> {
    if sets.is_empty() {
        return Err(Error::ExecutionError(
            "GROUPING SETS cannot be empty. Use GROUPING SETS(()) for grand total only."
                .to_string(),
        ));
    }

    Ok(sets)
}

pub fn create_grouping_metadata(
    grouping_set: &[Expr],
    grouping_set_id: usize,
    total_sets: usize,
) -> GroupingMetadata {
    let grouped_columns = grouping_set
        .iter()
        .filter_map(|expr| match expr {
            Expr::Column { name, .. } => Some(name.clone()),

            _ => None,
        })
        .collect();

    GroupingMetadata {
        grouped_columns,
        grouping_set_id,
        total_sets,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str) -> Expr {
        Expr::Column {
            name: name.to_string(),
            table: None,
        }
    }

    #[test]
    fn test_rollup_single_column() {
        let sets = generate_rollup_sets(vec![col("a")]);
        assert_eq!(sets.len(), 2);
        assert_eq!(sets[0].len(), 1);
        assert_eq!(sets[1].len(), 0);
    }

    #[test]
    fn test_rollup_two_columns() {
        let sets = generate_rollup_sets(vec![col("a"), col("b")]);
        assert_eq!(sets.len(), 3);
        assert_eq!(sets[0].len(), 2);
        assert_eq!(sets[1].len(), 1);
        assert_eq!(sets[2].len(), 0);
    }

    #[test]
    fn test_rollup_three_columns() {
        let sets = generate_rollup_sets(vec![col("a"), col("b"), col("c")]);
        assert_eq!(sets.len(), 4);
        assert_eq!(sets[0].len(), 3);
        assert_eq!(sets[1].len(), 2);
        assert_eq!(sets[2].len(), 1);
        assert_eq!(sets[3].len(), 0);
    }

    #[test]
    fn test_cube_two_columns() {
        let sets = generate_cube_sets(vec![col("a"), col("b")]).unwrap();
        assert_eq!(sets.len(), 4);
    }

    #[test]
    fn test_cube_three_columns() {
        let sets = generate_cube_sets(vec![col("a"), col("b"), col("c")]).unwrap();
        assert_eq!(sets.len(), 8);
    }

    #[test]
    fn test_cube_max_columns() {
        let cols: Vec<Expr> = (0..MAX_CUBE_COLUMNS)
            .map(|i| col(&format!("col{}", i)))
            .collect();
        let sets = generate_cube_sets(cols).unwrap();
        assert_eq!(sets.len(), 1 << MAX_CUBE_COLUMNS);
    }

    #[test]
    fn test_explicit_sets_custom() {
        let sets = vec![vec![col("a"), col("b")], vec![col("a")], vec![]];
        let result = generate_explicit_sets(sets).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_explicit_sets_empty_error() {
        let result = generate_explicit_sets(vec![]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_explicit_sets_duplicates_allowed() {
        let sets = vec![vec![col("a")], vec![col("a")]];
        let result = generate_explicit_sets(sets).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_grouping_metadata() {
        let set = vec![col("region"), col("product")];
        let metadata = create_grouping_metadata(&set, 0, 3);

        assert_eq!(metadata.grouped_columns, vec!["region", "product"]);
        assert_eq!(metadata.grouping_set_id, 0);
        assert_eq!(metadata.total_sets, 3);
    }
}
