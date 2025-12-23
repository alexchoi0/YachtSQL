use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use comfy_table::Table;
use comfy_table::presets::UTF8_FULL;
use directories::ProjectDirs;
use uuid::Uuid;
use yachtsql::YachtSQLEngine;
use yachtsql_executor::QueryExecutor;

#[derive(Parser)]
#[command(name = "yachtsql")]
#[command(about = "YachtSQL - Lightweight in-memory SQL database", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Query {
        sql: String,
        #[arg(short, long)]
        session_id: Option<String>,
    },
    Session {
        #[command(subcommand)]
        command: SessionCommands,
    },
}

#[derive(Subcommand)]
enum SessionCommands {
    New,
    List,
    Delete { session_id: String },
    Clean,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Query { sql, session_id } => {
            execute_query(&sql, session_id.as_deref())?;
        }
        Commands::Session { command } => match command {
            SessionCommands::New => {
                let session_id = create_session()?;
                println!("{}", session_id);
            }
            SessionCommands::List => {
                list_sessions()?;
            }
            SessionCommands::Delete { session_id } => {
                delete_session(&session_id)?;
                println!("Session {} deleted", session_id);
            }
            SessionCommands::Clean => {
                clean_sessions()?;
                println!("All sessions deleted");
            }
        },
    }

    Ok(())
}

fn get_sessions_dir() -> Result<PathBuf> {
    let proj_dirs = ProjectDirs::from("com.github", "alexchoi0", "yachtsql")
        .context("Failed to determine project directories")?;
    let data_dir = proj_dirs.data_dir().join("sessions");
    fs::create_dir_all(&data_dir)?;
    Ok(data_dir)
}

fn get_session_path(session_id: &str) -> Result<PathBuf> {
    let sessions_dir = get_sessions_dir()?;
    Ok(sessions_dir.join(format!("{}.bin", session_id)))
}

fn create_session() -> Result<String> {
    let session_id = Uuid::new_v4().to_string();
    let executor = QueryExecutor::new();
    save_executor(&session_id, &executor)?;
    Ok(session_id)
}

fn list_sessions() -> Result<()> {
    let sessions_dir = get_sessions_dir()?;
    let mut sessions: Vec<String> = Vec::new();

    for entry in fs::read_dir(&sessions_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "bin")
            && let Some(stem) = path.file_stem()
        {
            sessions.push(stem.to_string_lossy().to_string());
        }
    }

    if sessions.is_empty() {
        println!("No sessions found");
    } else {
        println!("Sessions:");
        for session in sessions {
            println!("  {}", session);
        }
    }

    Ok(())
}

fn delete_session(session_id: &str) -> Result<()> {
    let path = get_session_path(session_id)?;
    if path.exists() {
        fs::remove_file(&path)?;
    } else {
        anyhow::bail!("Session not found: {}", session_id);
    }
    Ok(())
}

fn clean_sessions() -> Result<()> {
    let sessions_dir = get_sessions_dir()?;
    for entry in fs::read_dir(&sessions_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "bin") {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}

fn load_executor(session_id: &str) -> Result<QueryExecutor> {
    let path = get_session_path(session_id)?;
    if !path.exists() {
        anyhow::bail!("Session not found: {}", session_id);
    }
    let data = fs::read(&path)?;
    let executor: QueryExecutor =
        bincode::deserialize(&data).context("Failed to deserialize session")?;
    Ok(executor)
}

fn save_executor(session_id: &str, executor: &QueryExecutor) -> Result<()> {
    let path = get_session_path(session_id)?;
    let data = bincode::serialize(executor).context("Failed to serialize session")?;
    fs::write(&path, data)?;
    Ok(())
}

fn execute_query(sql: &str, session_id: Option<&str>) -> Result<()> {
    match session_id {
        Some(id) => {
            let mut executor = load_executor(id)?;
            let table = executor
                .execute_sql(sql)
                .context("Failed to execute SQL query")?;
            save_executor(id, &executor)?;
            print_table(&table)?;
        }
        None => {
            let engine = YachtSQLEngine::new();
            let mut session = engine.create_session();
            let result = session.query(sql).context("Failed to execute SQL query")?;
            print_query_result(&result)?;
        }
    }
    Ok(())
}

fn print_table(table: &yachtsql_executor::Table) -> Result<()> {
    let row_count = table.row_count();
    if row_count == 0 {
        println!("(0 rows)");
        return Ok(());
    }

    let mut output = Table::new();
    output.load_preset(UTF8_FULL);

    let headers: Vec<&str> = table
        .schema()
        .fields()
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    output.set_header(headers);

    for i in 0..row_count {
        let row = table.get_row(i)?;
        let values: Vec<String> = row.values().iter().map(|v| format!("{}", v)).collect();
        output.add_row(values);
    }

    println!("{output}");
    println!("({} rows)", row_count);

    Ok(())
}

fn print_query_result(result: &yachtsql::QueryResult) -> Result<()> {
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
