mod error;
mod expr_planner;
mod planner;

pub use error::PlannerError;
pub use planner::Planner;
use sqlparser::dialect::BigQueryDialect;
use sqlparser::parser::Parser;
use yachtsql_common::error::Result;
use yachtsql_ir::LogicalPlan;
use yachtsql_storage::Schema;

pub struct ViewDefinition {
    pub query: String,
    pub column_aliases: Vec<String>,
}

pub trait CatalogProvider {
    fn get_table_schema(&self, name: &str) -> Option<Schema>;
    fn get_view(&self, name: &str) -> Option<ViewDefinition>;
}

pub fn parse_sql(sql: &str) -> Result<Vec<sqlparser::ast::Statement>> {
    let dialect = BigQueryDialect {};
    Parser::parse_sql(&dialect, sql)
        .map_err(|e| yachtsql_common::error::Error::parse_error(e.to_string()))
}

pub fn plan_statement<C: CatalogProvider>(
    stmt: &sqlparser::ast::Statement,
    catalog: &C,
) -> Result<LogicalPlan> {
    let planner = Planner::new(catalog);
    planner.plan_statement(stmt)
}

pub fn parse_and_plan<C: CatalogProvider>(sql: &str, catalog: &C) -> Result<LogicalPlan> {
    let statements = parse_sql(sql)?;

    if statements.is_empty() {
        return Err(yachtsql_common::error::Error::parse_error(
            "Empty SQL statement",
        ));
    }

    if statements.len() > 1 {
        return Err(yachtsql_common::error::Error::parse_error(
            "Multiple statements not supported",
        ));
    }

    plan_statement(&statements[0], catalog)
}
