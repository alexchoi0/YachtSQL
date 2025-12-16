#!/usr/bin/env python3
"""
Generate tests from bigquery-graph.json queries.
Transforms queries to use CTEs with dummy data instead of actual table references.
"""

import json
import re
import hashlib
from pathlib import Path

def sanitize_name(name):
    """Convert table name to a valid Rust test function name"""
    parts = name.split('.')
    table_name = parts[-1] if parts else name
    result = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
    if result and result[0].isdigit():
        result = '_' + result
    return result.lower()

def extract_tables_from_sql(sql):
    """Extract table references from SQL (backtick-quoted identifiers)"""
    pattern = r'`([^`]+)`'
    matches = re.findall(pattern, sql)
    return list(set(matches))

def table_to_cte_name(table_ref):
    """Convert a table reference to a CTE-friendly name"""
    parts = table_ref.replace('-', '_').split('.')
    if len(parts) >= 3:
        return f"{parts[1]}_{parts[2]}"
    elif len(parts) == 2:
        return f"{parts[0]}_{parts[1]}"
    return parts[0] if parts else "table_data"

def transform_sql_to_cte(sql, tables):
    """Transform SQL to use CTEs instead of table references"""
    transformed = sql
    table_cte_map = {}

    for table_ref in tables:
        cte_name = table_to_cte_name(table_ref)
        if cte_name in table_cte_map.values():
            cte_name = cte_name + "_" + hashlib.md5(table_ref.encode()).hexdigest()[:4]
        table_cte_map[table_ref] = cte_name
        transformed = transformed.replace(f'`{table_ref}`', cte_name)

    return transformed, table_cte_map

def generate_cte_dummy_data(cte_name):
    """Generate a simple CTE with dummy data"""
    return f"{cte_name} AS (SELECT 1 AS id, 'dummy' AS name)"

def escape_sql_for_rust(sql):
    """Escape SQL for use in Rust string literal"""
    escaped = sql.replace('\r\n', '\n')
    escaped = escaped.replace('\r', '\n')
    escaped = escaped.replace('\\', '\\\\')
    escaped = escaped.replace('"', '\\"')
    return escaped

def generate_test(name, sql, tables, func_name=None):
    """Generate a Rust test function for a query"""
    if func_name is None:
        func_name = f"test_{sanitize_name(name)}"

    transformed_sql, table_cte_map = transform_sql_to_cte(sql, tables)

    cte_parts = []
    for table_ref, cte_name in table_cte_map.items():
        cte_parts.append(generate_cte_dummy_data(cte_name))

    has_existing_with = transformed_sql.strip().upper().startswith('WITH')

    if has_existing_with:
        cte_prefix = ",\n".join(cte_parts)
        if cte_prefix:
            first_with_match = re.match(r'(?i)(WITH\s+)', transformed_sql.strip())
            if first_with_match:
                rest_of_sql = transformed_sql.strip()[len(first_with_match.group(1)):]
                final_sql = f"WITH\n{cte_prefix},\n{rest_of_sql}"
            else:
                final_sql = f"WITH\n{cte_prefix},\n{transformed_sql}"
        else:
            final_sql = transformed_sql
    else:
        if cte_parts:
            cte_prefix = "WITH\n" + ",\n".join(cte_parts)
            final_sql = f"{cte_prefix}\n{transformed_sql}"
        else:
            final_sql = transformed_sql

    escaped_sql = escape_sql_for_rust(final_sql)

    test_code = f'''#[test]
#[ignore = "Implement me!"]
fn {func_name}() {{
    let mut executor = create_executor();
    let _result = executor.execute_sql(
        "{escaped_sql}"
    );
}}
'''
    return test_code

def main():
    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    json_path = project_root / "bigquery-graph.json"
    output_path = project_root / "tests" / "bigquery" / "queries" / "real_sql_workloads.rs"

    with open(json_path, 'r') as f:
        data = json.load(f)

    non_trivial = []
    for name, info in data.get('tables', {}).items():
        sql = info.get('sql', '')
        sql_stripped = ' '.join(sql.split()).strip()

        if re.match(r'^(--.*\s)*SELECT \* FROM `[^`]+`\s*$', sql_stripped, re.IGNORECASE):
            continue
        if re.match(r'^SELECT \* EXCEPT\([^)]+\) FROM `[^`]+`\s*$', sql_stripped, re.IGNORECASE):
            continue

        sql_upper = sql_stripped.upper()
        is_complex = (
            len(sql_stripped) > 100 or
            'JOIN' in sql_upper or
            'GROUP BY' in sql_upper or
            'WITH' in sql_upper or
            'UNION' in sql_upper or
            'CASE' in sql_upper or
            sql_upper.count('SELECT') > 1
        )

        if is_complex:
            tables = extract_tables_from_sql(sql)
            non_trivial.append({
                'name': name,
                'sql': sql,
                'tables': tables
            })

    seen_names = {}
    tests = []

    for query in non_trivial:
        base_func_name = f"test_{sanitize_name(query['name'])}"
        if base_func_name in seen_names:
            seen_names[base_func_name] += 1
            func_name = f"{base_func_name}_{seen_names[base_func_name]}"
        else:
            seen_names[base_func_name] = 0
            func_name = base_func_name
        query['func_name'] = func_name

        test_code = generate_test(query['name'], query['sql'], query['tables'], func_name)
        tests.append(test_code)

    header = '''use crate::common::create_executor;

'''

    output_content = header + "\n".join(tests)

    with open(output_path, 'w') as f:
        f.write(output_content)

    print(f"Generated {len(tests)} tests to {output_path}")

if __name__ == "__main__":
    main()
