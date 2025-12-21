use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use comfy_table::Table;
use comfy_table::presets::UTF8_FULL;
use yachtsql::YachtSQLEngine;

#[derive(Parser)]
#[command(name = "yachtsql")]
#[command(about = "YachtSQL - Lightweight in-memory SQL database", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a SQL query
    Query {
        /// The SQL query to execute
        sql: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Query { sql } => {
            execute_query(&sql)?;
        }
    }

    Ok(())
}

fn execute_query(sql: &str) -> Result<()> {
    let mut engine = YachtSQLEngine::new();

    let result = engine.query(sql).context("Failed to execute SQL query")?;

    if result.rows.is_empty() {
        println!("(0 rows)");
        return Ok(());
    }

    let mut table = Table::new();
    table.load_preset(UTF8_FULL);

    let headers: Vec<&str> = result.schema.iter().map(|c| c.name.as_str()).collect();
    table.set_header(headers);

    for row in &result.rows {
        let values: Vec<String> = row.values().iter().map(|v| format!("{}", v)).collect();
        table.add_row(values);
    }

    println!("{table}");
    println!("({} rows)", result.rows.len());

    Ok(())
}
