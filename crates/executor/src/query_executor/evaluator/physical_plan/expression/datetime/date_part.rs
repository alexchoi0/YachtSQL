use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_date_part(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::eval_extract(args, batch, row_idx)
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    fn schema_with_string_and_date() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("part", DataType::String),
            Field::nullable("date_col", DataType::Date),
        ])
    }

    #[test]
    fn extracts_year_from_date() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let batch = create_batch(
            schema_with_string_and_date(),
            vec![vec![Value::string("YEAR".into()), Value::date(date)]],
        );
        let args = vec![Expr::column("part"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_date_part(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(2024));
    }

    #[test]
    fn delegates_to_extract() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let batch = create_batch(
            schema_with_string_and_date(),
            vec![vec![Value::string("MONTH".into()), Value::date(date)]],
        );
        let args = vec![Expr::column("part"), Expr::column("date_col")];

        let date_part_result =
            ProjectionWithExprExec::eval_date_part(&args, &batch, 0).expect("success");
        let extract_result =
            ProjectionWithExprExec::eval_extract(&args, &batch, 0).expect("success");

        assert_eq!(date_part_result, extract_result);
        assert_eq!(date_part_result, Value::int64(3));
    }

    #[test]
    fn propagates_null() {
        let batch = create_batch(
            schema_with_string_and_date(),
            vec![vec![Value::string("YEAR".into()), Value::null()]],
        );
        let args = vec![Expr::column("part"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_date_part(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }
}
