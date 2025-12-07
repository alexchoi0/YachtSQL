use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use yachtsql::{DialectType, QueryExecutor, Table};

#[derive(Debug, Clone)]
struct Statement {
    sql: String,
    tag: Option<String>,
    expected_rows_values: Vec<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct TestCase {
    pub name: String,
    statements: Vec<Statement>,
    expected_rows: Option<usize>,
    expected_cols: Option<usize>,
    #[allow(dead_code)]
    expected_description: Option<String>,
    should_error: bool,
}

#[derive(Debug)]
pub struct TestSuite {
    #[allow(dead_code)]
    file_path: PathBuf,
    test_cases: Vec<TestCase>,
}

fn parse_expected(line: &str) -> (Option<usize>, Option<usize>, Option<String>) {
    if let Some(caps) = regex::Regex::new(r"-- Expected:\s+(\d+)\s+rows?,\s+(\d+)\s+columns?")
        .unwrap()
        .captures(line)
    {
        return (
            caps.get(1).and_then(|m| m.as_str().parse().ok()),
            caps.get(2).and_then(|m| m.as_str().parse().ok()),
            None,
        );
    }

    if let Some(caps) = regex::Regex::new(r"-- Expected:\s+(\d+)\s+rows?")
        .unwrap()
        .captures(line)
    {
        return (
            caps.get(1).and_then(|m| m.as_str().parse().ok()),
            None,
            None,
        );
    }

    if let Some(desc) = line.strip_prefix("-- Expected:") {
        return (None, None, Some(desc.trim().to_string()));
    }

    (None, None, None)
}

fn is_sql_statement(line: &str) -> bool {
    let line_upper = line.trim().to_uppercase();
    const KEYWORDS: [&str; 10] = [
        "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "COPY", "WITH", "MERGE",
    ];
    KEYWORDS.iter().any(|kw| line_upper.starts_with(kw))
}

fn parse_expected_json(lines: &[&str], mut idx: usize) -> (Vec<serde_json::Value>, usize) {
    let mut json_lines = Vec::new();

    idx += 1;

    while idx < lines.len() {
        let line = lines[idx];
        if !line.trim_start().starts_with("--") {
            break;
        }

        let stripped = line
            .trim_start_matches("--")
            .trim_start_matches(' ')
            .trim_end();

        if stripped.is_empty() {
            idx += 1;
            continue;
        }

        json_lines.push(stripped);
        idx += 1;
    }

    let joined = json_lines.join("\n");
    if joined.is_empty() {
        return (Vec::new(), idx.saturating_sub(1));
    }

    let parsed: serde_json::Value =
        serde_json::from_str(&joined).expect("Failed to parse JSON expectation");

    match parsed {
        serde_json::Value::Array(arr) => (arr, idx.saturating_sub(1)),
        other @ serde_json::Value::Null
        | other @ serde_json::Value::Bool(_)
        | other @ serde_json::Value::Number(_)
        | other @ serde_json::Value::String(_)
        | other @ serde_json::Value::Object(_) => (vec![other], idx.saturating_sub(1)),
    }
}

pub fn parse_sql_file(file_path: &Path) -> TestSuite {
    let content = fs::read_to_string(file_path).expect("Failed to read SQL file");
    let lines: Vec<&str> = content.lines().collect();

    let mut test_cases = Vec::new();
    let mut current_statements: Vec<Statement> = Vec::new();
    let mut expected_rows = None;
    let mut expected_cols = None;
    let mut expected_desc = None;
    let mut test_name: Option<String> = None;
    let mut test_index = 0;
    let mut should_error = false;
    let mut pending_tag: Option<String> = None;
    let mut pending_expectations: Vec<serde_json::Value> = Vec::new();

    let mut i = 0;
    while i < lines.len() {
        let line = lines[i].trim_end();

        if line.contains("-- Test:") {
            if !current_statements.is_empty() {
                test_cases.push(TestCase {
                    name: test_name
                        .take()
                        .unwrap_or_else(|| format!("test_case_{:03}", test_index)),
                    statements: current_statements.clone(),
                    expected_rows,
                    expected_cols,
                    expected_description: expected_desc.clone(),
                    should_error,
                });
                test_index += 1;
            }

            current_statements.clear();
            test_name = line
                .split("-- Test:")
                .nth(1)
                .map(|s| {
                    s.trim()
                        .replace(|c: char| !c.is_alphanumeric() && c != '_', "_")
                        .to_lowercase()
                })
                .map(|s| {
                    if s.starts_with("test_") {
                        s
                    } else {
                        format!("test_{}", s)
                    }
                });
            expected_rows = None;
            expected_cols = None;
            expected_desc = None;
            should_error = false;
            pending_tag = None;
            pending_expectations.clear();
        } else if line.contains("-- Expected:") {
            let (rows, cols, desc) = parse_expected(line);
            expected_rows = rows;
            expected_cols = cols;
            expected_desc = desc;
        } else if line.contains("-- ExpectJSON:") {
            let (values, new_idx) = parse_expected_json(&lines, i);
            pending_expectations = values;
            i = new_idx;
        } else if let Some(tag) = line.strip_prefix("-- Tag:") {
            pending_tag = Some(tag.trim().to_string());
        } else if line.contains("-- ERROR:") || line.to_lowercase().contains("should fail") {
            should_error = true;
        } else if is_sql_statement(line) {
            let mut statement = line.to_string();

            i += 1;
            while i < lines.len() {
                let next_line = lines[i].trim();
                if next_line.is_empty() || next_line.starts_with("--") {
                    break;
                }
                statement.push(' ');
                statement.push_str(next_line);
                if next_line.ends_with(';') {
                    break;
                }
                i += 1;
            }

            if !statement.ends_with(';') {
                statement.push(';');
            }

            current_statements.push(Statement {
                sql: statement,
                tag: pending_tag.take(),
                expected_rows_values: std::mem::take(&mut pending_expectations),
            });
        }

        i += 1;
    }

    if !current_statements.is_empty() {
        test_cases.push(TestCase {
            name: test_name.unwrap_or_else(|| format!("test_case_{:03}", test_index)),
            statements: current_statements,
            expected_rows,
            expected_cols,
            expected_description: expected_desc,
            should_error,
        });
    }

    TestSuite {
        file_path: file_path.to_path_buf(),
        test_cases,
    }
}

fn cast_value(value: &yachtsql::Value) -> serde_json::Value {
    if value.is_null() {
        return serde_json::Value::Null;
    }
    if let Some(b) = value.as_bool() {
        return serde_json::Value::Bool(b);
    }
    if let Some(i) = value.as_i64() {
        return serde_json::json!(i);
    }
    if let Some(f) = value.as_f64() {
        return serde_json::json!(f);
    }
    if let Some(n) = value.as_numeric() {
        return serde_json::json!(n.to_string());
    }
    if let Some(s) = value.as_str() {
        return serde_json::Value::String(s.to_string());
    }
    if let Some(bytes) = value.as_bytes() {
        return serde_json::Value::Array(bytes.iter().map(|b| serde_json::json!(*b)).collect());
    }
    if let Some(d) = value.as_date() {
        return serde_json::json!(d.to_string());
    }
    if let Some(dt) = value.as_datetime() {
        return serde_json::json!(dt.to_string());
    }
    if let Some(t) = value.as_time() {
        return serde_json::json!(t.to_string());
    }
    if let Some(ts) = value.as_timestamp() {
        return serde_json::json!(ts.to_string());
    }
    if let Some(arr) = value.as_array() {
        return serde_json::Value::Array(arr.iter().map(cast_value).collect());
    }
    if let Some(fields) = value.as_struct() {
        let map = fields
            .iter()
            .map(|(k, v)| (k.clone(), cast_value(v)))
            .collect();
        return serde_json::Value::Object(map);
    }
    if let Some(j) = value.as_json() {
        return j.clone();
    }
    if let Some(u) = value.as_uuid() {
        return serde_json::json!(u.to_string());
    }
    if let Some(g) = value.as_geography() {
        return serde_json::Value::String(g.to_string());
    }

    serde_json::Value::Null
}

fn result_rows_to_json(result: &Table) -> Vec<serde_json::Value> {
    if result.num_columns() == 0 {
        return Vec::new();
    }

    let schema = result.schema();
    let mut rows = Vec::with_capacity(result.num_rows());

    for row_idx in 0..result.num_rows() {
        let mut object = serde_json::Map::with_capacity(result.num_columns());

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let value_json = result
                .column(col_idx)
                .and_then(|column| column.get(row_idx).ok())
                .map(|value| cast_value(&value))
                .unwrap_or(serde_json::Value::Null);
            object.insert(field.name.clone(), value_json);
        }

        rows.push(serde_json::Value::Object(object));
    }

    rows
}

pub fn run_test_case(
    test_case: &TestCase,
    expectations: &HashMap<String, Vec<serde_json::Value>>,
) -> Result<(), String> {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    for (i, statement) in test_case.statements.iter().enumerate() {
        let stmt = &statement.sql;
        let is_last = i == test_case.statements.len() - 1;
        let normalized = stmt.trim().to_uppercase();
        let is_query = normalized.starts_with("SELECT")
            || normalized.starts_with("WITH")
            || normalized.contains("RETURNING");

        if is_query && is_last && !test_case.should_error {
            match executor.execute_sql(stmt) {
                Ok(result) => {
                    if let Some(expected_rows) = test_case.expected_rows
                        && result.num_rows() != expected_rows
                    {
                        return Err(format!(
                            "Row count mismatch: expected {}, got {}",
                            expected_rows,
                            result.num_rows()
                        ));
                    }

                    if let Some(expected_cols) = test_case.expected_cols
                        && result.num_columns() != expected_cols
                    {
                        return Err(format!(
                            "Column count mismatch: expected {}, got {}",
                            expected_cols,
                            result.num_columns()
                        ));
                    }

                    let expected_values = if let Some(tag) = &statement.tag {
                        expectations.get(tag)
                    } else if !statement.expected_rows_values.is_empty() {
                        Some(&statement.expected_rows_values)
                    } else {
                        None
                    };

                    if let Some(expected) = expected_values {
                        let actual = result_rows_to_json(&result);
                        if &actual != expected {
                            return Err(format!(
                                "Row values mismatch.\nExpected: {}\nActual: {}",
                                serde_json::to_string_pretty(expected).unwrap(),
                                serde_json::to_string_pretty(&actual).unwrap(),
                            ));
                        }
                    }
                }
                Err(e) => {
                    if test_case.should_error {
                        return Ok(());
                    } else {
                        return Err(format!("Query failed: {}", e));
                    }
                }
            }
        } else if let Err(e) = executor.execute_sql(stmt) {
            if test_case.should_error && is_last {
                return Ok(());
            } else {
                return Err(format!("Statement failed: {} - Error: {}", stmt, e));
            }
        } else if is_last
            && (!statement.expected_rows_values.is_empty()
                || statement
                    .tag
                    .as_ref()
                    .is_some_and(|tag| expectations.contains_key(tag)))
        {
            return Err("Expected row values for non-query statement".to_string());
        }
    }

    Ok(())
}

#[allow(dead_code)]
pub fn find_sql_test_files() -> Vec<PathBuf> {
    let mut files = Vec::new();
    let test_dir = Path::new("tests/sql-queries");

    if !test_dir.exists() {
        return files;
    }

    fn visit_dir(dir: &Path, files: &mut Vec<PathBuf>) {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    visit_dir(&path, files);
                } else if path.extension().is_some_and(|e| e == "sql")
                    && path
                        .file_name()
                        .is_some_and(|n| n.to_string_lossy().ends_with("_test.sql"))
                {
                    files.push(path);
                }
            }
        }
    }

    visit_dir(test_dir, &mut files);
    files.sort();
    files
}

#[allow(dead_code)]
pub fn run_sql_test_file(path: &str) -> (usize, Vec<String>) {
    run_sql_test_file_with_expectations(path, &HashMap::new())
}

pub fn run_sql_test_file_with_expectations(
    path: &str,
    expectations: &HashMap<String, Vec<serde_json::Value>>,
) -> (usize, Vec<String>) {
    let full_path = Path::new(path);
    let suite = parse_sql_file(full_path);

    let total = suite.test_cases.len();

    let mut failures = Vec::new();
    for test_case in &suite.test_cases {
        if let Err(err) = run_test_case(test_case, expectations) {
            failures.push(format!("{}: {}", test_case.name, err));
        }
    }

    (total, failures)
}
