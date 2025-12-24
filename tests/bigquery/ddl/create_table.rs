use crate::assert_table_eq;
use crate::common::create_session;

#[test]
fn test_create_simple_table() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM users").unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_create_table_with_multiple_columns() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE products (id INT64, name STRING, price FLOAT64, in_stock BOOLEAN)",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO products VALUES (1, 'Laptop', 999.99, true)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM products").unwrap();
    assert_table_eq!(result, [[1, "Laptop", 999.99, true]]);
}

#[test]
fn test_create_table_with_primary_key() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE items (id INT64 PRIMARY KEY, name STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO items VALUES (1, 'Widget')")
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM items WHERE id = 1")
        .unwrap();
    assert_table_eq!(result, [[1, "Widget"]]);
}

#[test]
fn test_create_table_with_not_null() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE required (id INT64, name STRING NOT NULL)")
        .unwrap();

    session
        .execute_sql("INSERT INTO required VALUES (1, 'Test')")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM required").unwrap();
    assert_table_eq!(result, [[1, "Test"]]);
}

#[test]
fn test_create_table_with_default() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE defaults (id INT64, status STRING DEFAULT 'pending')")
        .unwrap();

    session
        .execute_sql("INSERT INTO defaults (id) VALUES (1)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM defaults").unwrap();
    assert_table_eq!(result, [[1, "pending"]]);
}

#[test]
fn test_create_table_if_not_exists() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE test_table (id INT64)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE IF NOT EXISTS test_table (id INT64)")
        .unwrap();

    session
        .execute_sql("INSERT INTO test_table VALUES (1)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM test_table").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_with_check_constraint() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE positive (id INT64, value INT64 CHECK (value > 0))")
        .unwrap();

    session
        .execute_sql("INSERT INTO positive VALUES (1, 10)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM positive").unwrap();
    assert_table_eq!(result, [[1, 10]]);
}

#[test]
fn test_create_table_with_composite_primary_key() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE composite (a INT64, b INT64, c STRING, PRIMARY KEY (a, b))")
        .unwrap();

    session
        .execute_sql("INSERT INTO composite VALUES (1, 2, 'test')")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM composite").unwrap();
    assert_table_eq!(result, [[1, 2, "test"]]);
}

#[test]
fn test_create_or_replace_table() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE replaceable (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO replaceable VALUES (1, 'Original')")
        .unwrap();

    session
        .execute_sql("CREATE OR REPLACE TABLE replaceable (id INT64, value INT64)")
        .unwrap();

    session
        .execute_sql("INSERT INTO replaceable VALUES (2, 100)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM replaceable").unwrap();
    assert_table_eq!(result, [[2, 100]]);
}

#[test]
fn test_create_temp_table() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TEMP TABLE temp_data (x INT64, y STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO temp_data VALUES (5, 'foo')")
        .unwrap();

    session
        .execute_sql("INSERT INTO temp_data VALUES (6, 'bar')")
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM temp_data ORDER BY x")
        .unwrap();
    assert_table_eq!(result, [[5, "foo"], [6, "bar"]]);
}

#[test]
fn test_create_temporary_table() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TEMPORARY TABLE session_data (id INT64)")
        .unwrap();

    session
        .execute_sql("INSERT INTO session_data VALUES (1), (2), (3)")
        .unwrap();

    let result = session
        .execute_sql("SELECT SUM(id) FROM session_data")
        .unwrap();
    assert_table_eq!(result, [[6]]);
}

#[test]
fn test_create_table_with_struct_column() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE with_struct (
                id INT64,
                info STRUCT<name STRING, age INT64>
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO with_struct VALUES (1, STRUCT('Alice', 30))")
        .unwrap();

    let result = session
        .execute_sql("SELECT id, info.name FROM with_struct")
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_create_table_with_nested_struct() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE nested_struct (
                id INT64,
                y STRUCT<
                    a ARRAY<STRING>,
                    b BOOL
                >
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO nested_struct VALUES (1, STRUCT(['x', 'y', 'z'], true))")
        .unwrap();

    let result = session
        .execute_sql("SELECT id, y.b FROM nested_struct")
        .unwrap();
    assert_table_eq!(result, [[1, true]]);
}

#[test]
fn test_create_table_with_array_column() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE with_array (id INT64, tags ARRAY<STRING>)")
        .unwrap();

    session
        .execute_sql("INSERT INTO with_array VALUES (1, ['red', 'green', 'blue'])")
        .unwrap();

    let result = session
        .execute_sql("SELECT id, ARRAY_LENGTH(tags) FROM with_array")
        .unwrap();
    assert_table_eq!(result, [[1, 3]]);
}

#[test]
fn test_create_table_as_select() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE source_table (id INT64, name STRING, value INT64)")
        .unwrap();

    session
        .execute_sql("INSERT INTO source_table VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30)")
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE derived_table AS
            SELECT name, SUM(value) as total
            FROM source_table
            GROUP BY name",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM derived_table ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["a", 40], ["b", 20]]);
}

#[test]
fn test_create_table_as_select_with_options() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE base_data (id INT64, category STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO base_data VALUES (1, 'X'), (2, 'Y')")
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE categorized
            OPTIONS(description = 'Derived from base_data')
            AS SELECT * FROM base_data",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM categorized ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "X"], [2, "Y"]]);
}

#[test]
#[ignore]
fn test_create_table_with_required_struct_field() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE required_struct (
                x INT64 NOT NULL,
                y STRUCT<
                    a ARRAY<STRING>,
                    b BOOL NOT NULL,
                    c FLOAT64
                > NOT NULL,
                z STRING
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO required_struct VALUES (1, STRUCT(['a'], true, 1.5), 'test')")
        .unwrap();

    let result = session
        .execute_sql("SELECT x, y.b FROM required_struct")
        .unwrap();
    assert_table_eq!(result, [[1, true]]);
}

#[test]
fn test_create_table_with_parameterized_string() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE param_string (id INT64, code STRING(10))")
        .unwrap();

    session
        .execute_sql("INSERT INTO param_string VALUES (1, 'ABC')")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM param_string").unwrap();
    assert_table_eq!(result, [[1, "ABC"]]);
}

#[test]
fn test_create_table_with_parameterized_numeric() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE param_numeric (
                id INT64,
                price NUMERIC(15, 2)
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO param_numeric VALUES (1, 123.45)")
        .unwrap();

    let result = session.execute_sql("SELECT id FROM param_numeric").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_with_parameterized_bignumeric() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE param_bignumeric (id INT64, big_value BIGNUMERIC(35))")
        .unwrap();

    session
        .execute_sql("INSERT INTO param_bignumeric VALUES (1, 12345678901234567890)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM param_bignumeric")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_with_parameterized_bytes() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE param_bytes (
                id INT64,
                data BYTES(5)
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO param_bytes VALUES (1, b'hello')")
        .unwrap();

    let result = session.execute_sql("SELECT id FROM param_bytes").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_with_rounding_mode() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE with_rounding (
                id INT64,
                value NUMERIC(10, 2) OPTIONS(rounding_mode = 'ROUND_HALF_EVEN')
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO with_rounding VALUES (1, 2.25)")
        .unwrap();

    let result = session.execute_sql("SELECT id FROM with_rounding").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_primary_key_not_enforced() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE pk_not_enforced (
                id INT64,
                name STRING,
                PRIMARY KEY (id) NOT ENFORCED
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO pk_not_enforced VALUES (1, 'Alice')")
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM pk_not_enforced")
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_create_table_with_array_of_struct() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE array_of_struct (
                corpus STRING,
                top_words ARRAY<STRUCT<word STRING, word_count INT64>>
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO array_of_struct VALUES (
                'shakespeare',
                [STRUCT('the', 100), STRUCT('and', 80)]
            )",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT corpus, ARRAY_LENGTH(top_words) FROM array_of_struct")
        .unwrap();
    assert_table_eq!(result, [["shakespeare", 2]]);
}

#[test]
fn test_create_table_with_multiple_constraints() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE multi_constraint (
                id INT64 NOT NULL,
                email STRING NOT NULL,
                age INT64,
                PRIMARY KEY (id) NOT ENFORCED
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO multi_constraint VALUES (1, 'alice@example.com', 30)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id, email FROM multi_constraint")
        .unwrap();
    assert_table_eq!(result, [[1, "alice@example.com"]]);
}

#[test]
fn test_create_table_with_default_expression() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE with_defaults (
                id INT64,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                status STRING DEFAULT 'active',
                priority INT64 DEFAULT 1
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO with_defaults (id) VALUES (1)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id, status, priority FROM with_defaults")
        .unwrap();
    assert_table_eq!(result, [[1, "active", 1]]);
}

#[test]
fn test_create_table_with_complex_nested_types() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE complex_nested (
                x INT64 OPTIONS(description = 'An optional INTEGER field'),
                y STRUCT<
                    a ARRAY<STRING> OPTIONS(description = 'A repeated STRING field'),
                    b BOOL
                >
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO complex_nested VALUES (1, STRUCT(['hello', 'world'], false))")
        .unwrap();

    let result = session
        .execute_sql("SELECT x, y.a[OFFSET(0)] FROM complex_nested")
        .unwrap();
    assert_table_eq!(result, [[1, "hello"]]);
}

#[test]
fn test_create_table_ctas_preserves_types() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE source_types (a INT64, b FLOAT64, c STRING, d BOOL)")
        .unwrap();

    session
        .execute_sql("INSERT INTO source_types VALUES (1, 1.5, 'test', true)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE copy_types AS SELECT * FROM source_types")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM copy_types").unwrap();
    assert_table_eq!(result, [[1, 1.5, "test", true]]);
}

#[test]
fn test_create_table_with_all_data_types() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE all_types (
                col_bool BOOL,
                col_int64 INT64,
                col_float64 FLOAT64,
                col_numeric NUMERIC,
                col_bignumeric BIGNUMERIC,
                col_string STRING,
                col_bytes BYTES,
                col_date DATE,
                col_time TIME,
                col_datetime DATETIME,
                col_timestamp TIMESTAMP
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO all_types VALUES (
                true, 42, 3.14, 123.45, 9999999999999999999,
                'hello', b'world',
                DATE '2024-01-15',
                TIME '10:30:00',
                DATETIME '2024-01-15 10:30:00',
                TIMESTAMP '2024-01-15 10:30:00 UTC'
            )",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT col_bool, col_int64 FROM all_types")
        .unwrap();
    assert_table_eq!(result, [[true, 42]]);
}

#[test]
fn test_create_table_like() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE source_schema (id INT64, name STRING, value FLOAT64)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE copy_schema LIKE source_schema")
        .unwrap();

    session
        .execute_sql("INSERT INTO copy_schema VALUES (1, 'test', 1.5)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM copy_schema").unwrap();
    assert_table_eq!(result, [[1, "test", 1.5]]);
}

#[test]
#[ignore]
fn test_create_table_copy() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE original_data (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO original_data VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    session
        .execute_sql("CREATE TABLE copied_data COPY original_data")
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM copied_data ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"]]);
}

#[test]
fn test_create_table_with_foreign_key() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE departments (dept_id INT64, name STRING, PRIMARY KEY (dept_id) NOT ENFORCED)")
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE employees (
                emp_id INT64,
                name STRING,
                dept_id INT64,
                PRIMARY KEY (emp_id) NOT ENFORCED,
                FOREIGN KEY (dept_id) REFERENCES departments(dept_id) NOT ENFORCED
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO departments VALUES (1, 'Engineering')")
        .unwrap();

    session
        .execute_sql("INSERT INTO employees VALUES (100, 'Alice', 1)")
        .unwrap();

    let result = session
        .execute_sql("SELECT emp_id, name FROM employees")
        .unwrap();
    assert_table_eq!(result, [[100, "Alice"]]);
}

#[test]
fn test_create_table_with_named_constraint() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE parent_table (id INT64, PRIMARY KEY (id) NOT ENFORCED)")
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE child_table (
                id INT64,
                parent_id INT64,
                CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent_table(id) NOT ENFORCED
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO parent_table VALUES (1)")
        .unwrap();

    session
        .execute_sql("INSERT INTO child_table VALUES (10, 1)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM child_table").unwrap();
    assert_table_eq!(result, [[10, 1]]);
}

#[test]
#[ignore]
fn test_create_table_with_default_collate() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE collated_table (
                a STRING,
                b STRING,
                c INT64
            )
            DEFAULT COLLATE 'und:ci'",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO collated_table VALUES ('Hello', 'WORLD', 1)")
        .unwrap();

    let result = session
        .execute_sql("SELECT a FROM collated_table WHERE a = 'hello'")
        .unwrap();
    assert_table_eq!(result, [["Hello"]]);
}

#[test]
#[ignore]
fn test_create_table_with_column_collate() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE column_collate (
                case_sensitive STRING,
                case_insensitive STRING COLLATE 'und:ci'
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO column_collate VALUES ('Test', 'Test')")
        .unwrap();

    let result = session
        .execute_sql("SELECT case_insensitive FROM column_collate WHERE case_insensitive = 'test'")
        .unwrap();
    assert_table_eq!(result, [["Test"]]);
}

#[test]
fn test_create_table_with_multiple_foreign_keys() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE categories (cat_id INT64, PRIMARY KEY (cat_id) NOT ENFORCED)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE suppliers (sup_id INT64, PRIMARY KEY (sup_id) NOT ENFORCED)")
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE products (
                prod_id INT64,
                cat_id INT64,
                sup_id INT64,
                name STRING,
                PRIMARY KEY (prod_id) NOT ENFORCED,
                FOREIGN KEY (cat_id) REFERENCES categories(cat_id) NOT ENFORCED,
                FOREIGN KEY (sup_id) REFERENCES suppliers(sup_id) NOT ENFORCED
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO categories VALUES (1)")
        .unwrap();

    session
        .execute_sql("INSERT INTO suppliers VALUES (10)")
        .unwrap();

    session
        .execute_sql("INSERT INTO products VALUES (100, 1, 10, 'Widget')")
        .unwrap();

    let result = session.execute_sql("SELECT name FROM products").unwrap();
    assert_table_eq!(result, [["Widget"]]);
}

#[test]
fn test_create_table_with_column_reference() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE ref_parent (id INT64 PRIMARY KEY NOT ENFORCED, name STRING)")
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE ref_child (
                id INT64,
                parent_id INT64 REFERENCES ref_parent(id) NOT ENFORCED
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO ref_parent VALUES (1, 'Parent')")
        .unwrap();

    session
        .execute_sql("INSERT INTO ref_child VALUES (10, 1)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM ref_child").unwrap();
    assert_table_eq!(result, [[10, 1]]);
}

#[test]
fn test_create_table_with_friendly_name() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE friendly_table (id INT64)
            OPTIONS (friendly_name = 'My Friendly Table')",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO friendly_table VALUES (1)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM friendly_table")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_with_multiple_options() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE multi_options (id INT64, created_date DATE)
            PARTITION BY created_date
            OPTIONS (
                description = 'A table with multiple options',
                friendly_name = 'Multi-Option Table',
                labels = [('env', 'test'), ('team', 'data')],
                partition_expiration_days = 30,
                require_partition_filter = false
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO multi_options VALUES (1, DATE '2024-01-15')")
        .unwrap();

    let result = session.execute_sql("SELECT id FROM multi_options").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore]
fn test_create_table_dataset_qualified_name() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE mydataset.qualified_table (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO mydataset.qualified_table VALUES (1, 'test')")
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM mydataset.qualified_table")
        .unwrap();
    assert_table_eq!(result, [[1, "test"]]);
}

#[test]
#[ignore]
fn test_create_table_backtick_qualified_name() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE `myproject.mydataset.backtick_table` (id INT64)")
        .unwrap();

    session
        .execute_sql("INSERT INTO `myproject.mydataset.backtick_table` VALUES (1)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM `myproject.mydataset.backtick_table`")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_with_default_rounding_mode() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE default_rounding (
                id INT64,
                amount NUMERIC
            )
            OPTIONS (default_rounding_mode = 'ROUND_HALF_EVEN')",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO default_rounding VALUES (1, 2.25)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM default_rounding")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore]
fn test_create_table_complex_from_docs() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE mydataset.newtable
            (
                x INT64 OPTIONS(description='An optional INTEGER field'),
                y STRUCT <
                    a ARRAY <STRING> OPTIONS(description='A repeated STRING field'),
                    b BOOL
                >
            )
            PARTITION BY _PARTITIONDATE
            OPTIONS(
                expiration_timestamp=TIMESTAMP '2025-01-01 00:00:00 UTC',
                partition_expiration_days=1,
                description='a table that expires in 2025, with each partition living for 24 hours',
                labels=[('org_unit', 'development')]
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO mydataset.newtable VALUES (1, STRUCT(['hello'], true))")
        .unwrap();

    let result = session
        .execute_sql("SELECT x FROM mydataset.newtable")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_clone() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE source_for_clone (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO source_for_clone VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    session
        .execute_sql("CREATE TABLE cloned_table CLONE source_for_clone")
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM cloned_table ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"]]);
}

#[test]
fn test_create_table_clone_if_not_exists() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE clone_source (id INT64)")
        .unwrap();

    session
        .execute_sql("INSERT INTO clone_source VALUES (1), (2)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE clone_dest CLONE clone_source")
        .unwrap();

    session
        .execute_sql("CREATE TABLE IF NOT EXISTS clone_dest CLONE clone_source")
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM clone_dest")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_create_or_replace_table_clone() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE clone_src1 (id INT64, val INT64)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE clone_src2 (id INT64, val INT64)")
        .unwrap();

    session
        .execute_sql("INSERT INTO clone_src1 VALUES (1, 100)")
        .unwrap();

    session
        .execute_sql("INSERT INTO clone_src2 VALUES (2, 200)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE clone_target CLONE clone_src1")
        .unwrap();

    session
        .execute_sql("CREATE OR REPLACE TABLE clone_target CLONE clone_src2")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM clone_target").unwrap();
    assert_table_eq!(result, [[2, 200]]);
}

#[test]
#[ignore]
fn test_create_table_clone_isolation() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE isolation_src (id INT64, value INT64)")
        .unwrap();

    session
        .execute_sql("INSERT INTO isolation_src VALUES (1, 100)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE isolation_clone CLONE isolation_src")
        .unwrap();

    session
        .execute_sql("UPDATE isolation_src SET value = 999 WHERE id = 1")
        .unwrap();

    let clone_result = session
        .execute_sql("SELECT value FROM isolation_clone WHERE id = 1")
        .unwrap();
    assert_table_eq!(clone_result, [[100]]);

    let src_result = session
        .execute_sql("SELECT value FROM isolation_src WHERE id = 1")
        .unwrap();
    assert_table_eq!(src_result, [[999]]);
}

#[test]
#[ignore]
fn test_create_table_clone_for_system_time() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE time_src (id INT64, value INT64)")
        .unwrap();

    session
        .execute_sql("INSERT INTO time_src VALUES (1, 100)")
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE time_clone CLONE time_src
            FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)",
        )
        .unwrap();

    let result = session.execute_sql("SELECT * FROM time_clone").unwrap();
    assert_table_eq!(result, [[1, 100]]);
}

#[test]
#[ignore]
fn test_create_table_clone_with_options() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE opts_src (id INT64)")
        .unwrap();

    session
        .execute_sql("INSERT INTO opts_src VALUES (1)")
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE opts_clone CLONE opts_src
            OPTIONS(description='Cloned table with options')",
        )
        .unwrap();

    let result = session.execute_sql("SELECT id FROM opts_clone").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore]
fn test_create_external_table() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE EXTERNAL TABLE external_data (
                id INT64,
                name STRING,
                value FLOAT64
            )
            OPTIONS (
                format = 'CSV',
                uris = ['gs://bucket/data.csv']
            )",
        )
        .unwrap();

    let result = session.execute_sql("SELECT * FROM external_data");
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_create_external_table_with_schema() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE EXTERNAL TABLE ext_parquet (
                user_id INT64,
                event_type STRING,
                event_time TIMESTAMP
            )
            OPTIONS (
                format = 'PARQUET',
                uris = ['gs://bucket/events/*.parquet']
            )",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_create_external_table_if_not_exists() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE EXTERNAL TABLE IF NOT EXISTS ext_json (
                data STRING
            )
            OPTIONS (
                format = 'JSON',
                uris = ['gs://bucket/data.json']
            )",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_create_or_replace_external_table() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE EXTERNAL TABLE ext_avro (
                id INT64
            )
            OPTIONS (
                format = 'AVRO',
                uris = ['gs://bucket/data.avro']
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "CREATE OR REPLACE EXTERNAL TABLE ext_avro (
                id INT64,
                name STRING
            )
            OPTIONS (
                format = 'AVRO',
                uris = ['gs://bucket/data_v2.avro']
            )",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_create_external_table_with_hive_partitioning() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE EXTERNAL TABLE ext_partitioned (
                id INT64,
                name STRING
            )
            WITH PARTITION COLUMNS (
                dt DATE
            )
            OPTIONS (
                format = 'PARQUET',
                uris = ['gs://bucket/data/*'],
                hive_partition_uri_prefix = 'gs://bucket/data'
            )",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_create_external_table_bigtable() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE EXTERNAL TABLE ext_bigtable
            OPTIONS (
                format = 'CLOUD_BIGTABLE',
                uris = ['https://googleapis.com/bigtable/projects/project/instances/instance/tables/table']
            )",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_create_external_table_with_connection() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE EXTERNAL TABLE ext_with_connection (
                id INT64
            )
            WITH CONNECTION `project.region.connection_id`
            OPTIONS (
                format = 'PARQUET',
                uris = ['gs://bucket/data.parquet']
            )",
        )
        .unwrap();
}

#[test]
fn test_create_snapshot_table() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE snapshot_src (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO snapshot_src VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    session
        .execute_sql(
            "CREATE SNAPSHOT TABLE my_snapshot
            CLONE snapshot_src
            OPTIONS(
                expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR),
                friendly_name='my_table_snapshot'
            )",
        )
        .unwrap();
}

#[test]
fn test_create_snapshot_table_if_not_exists() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE snap_src (id INT64)")
        .unwrap();

    session
        .execute_sql("CREATE SNAPSHOT TABLE snap1 CLONE snap_src")
        .unwrap();

    session
        .execute_sql("CREATE SNAPSHOT TABLE IF NOT EXISTS snap1 CLONE snap_src")
        .unwrap();
}

#[test]
fn test_create_snapshot_table_for_system_time() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE history_src (id INT64, value INT64)")
        .unwrap();

    session
        .execute_sql("INSERT INTO history_src VALUES (1, 100)")
        .unwrap();

    session
        .execute_sql(
            "CREATE SNAPSHOT TABLE historical_snap
            CLONE history_src
            FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)",
        )
        .unwrap();
}

#[test]
fn test_drop_snapshot_table() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE snap_drop_src (id INT64)")
        .unwrap();

    session
        .execute_sql("CREATE SNAPSHOT TABLE snap_to_drop CLONE snap_drop_src")
        .unwrap();

    session
        .execute_sql("DROP SNAPSHOT TABLE snap_to_drop")
        .unwrap();
}

#[test]
fn test_drop_snapshot_table_if_exists() {
    let mut session = create_session();

    let result = session.execute_sql("DROP SNAPSHOT TABLE IF EXISTS nonexistent_snapshot");
    assert!(result.is_ok());
}

#[test]
fn test_create_table_like_with_options() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE like_src (id INT64, name STRING, value FLOAT64)")
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE like_dest
            LIKE like_src
            OPTIONS(description='Copied from like_src')",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO like_dest VALUES (1, 'test', 1.5)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM like_dest").unwrap();
    assert_table_eq!(result, [[1, "test", 1.5]]);
}

#[test]
fn test_create_table_like_with_as_select() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE like_as_src (id INT64, value INT64)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE like_data (id INT64, value INT64)")
        .unwrap();

    session
        .execute_sql("INSERT INTO like_data VALUES (1, 100), (2, 200)")
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE like_as_dest
            LIKE like_as_src
            AS SELECT * FROM like_data",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM like_as_dest ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, 200]]);
}

#[test]
#[ignore]
fn test_create_table_copy_with_options() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE copy_src (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO copy_src VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE copy_dest COPY copy_src
            OPTIONS(description='Copied table with data')",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM copy_dest ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"]]);
}

#[test]
#[ignore]
fn test_create_or_replace_table_copy() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE copy_or_replace_src (id INT64)")
        .unwrap();

    session
        .execute_sql("INSERT INTO copy_or_replace_src VALUES (1)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE copy_or_replace_dest (x INT64)")
        .unwrap();

    session
        .execute_sql("CREATE OR REPLACE TABLE copy_or_replace_dest COPY copy_or_replace_src")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM copy_or_replace_dest")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_with_kms_key() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE kms_table (id INT64)
            OPTIONS (
                kms_key_name = 'projects/my-project/locations/us/keyRings/my-keyring/cryptoKeys/my-key'
            )",
        )
        .unwrap();
}

#[test]
fn test_create_table_with_max_staleness() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE staleness_table (id INT64)
            OPTIONS (
                max_staleness = INTERVAL '4:0:0' HOUR TO SECOND
            )",
        )
        .unwrap();
}

#[test]
fn test_create_table_with_change_history() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE change_history_table (id INT64, value STRING)
            OPTIONS (
                enable_change_history = TRUE
            )",
        )
        .unwrap();
}

#[test]
fn test_create_table_with_composite_foreign_key() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE composite_pk (
                a INT64,
                b INT64,
                name STRING,
                PRIMARY KEY (a, b) NOT ENFORCED
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE composite_fk (
                id INT64,
                ref_a INT64,
                ref_b INT64,
                FOREIGN KEY (ref_a, ref_b) REFERENCES composite_pk(a, b) NOT ENFORCED
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO composite_pk VALUES (1, 2, 'test')")
        .unwrap();

    session
        .execute_sql("INSERT INTO composite_fk VALUES (100, 1, 2)")
        .unwrap();

    let result = session.execute_sql("SELECT id FROM composite_fk").unwrap();
    assert_table_eq!(result, [[100]]);
}

#[test]
fn test_create_table_with_deeply_nested_struct() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE deep_nested (
                id INT64,
                data STRUCT<
                    level1 STRUCT<
                        level2 STRUCT<
                            value STRING
                        >
                    >
                >
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO deep_nested VALUES (1, STRUCT(STRUCT(STRUCT('deep'))))")
        .unwrap();

    let result = session
        .execute_sql("SELECT data.level1.level2.value FROM deep_nested")
        .unwrap();
    assert_table_eq!(result, [["deep"]]);
}

#[test]
fn test_create_table_with_geography_type() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE geo_table (
                id INT64,
                location GEOGRAPHY
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO geo_table VALUES (1, ST_GEOGPOINT(-122.4194, 37.7749))")
        .unwrap();

    let result = session.execute_sql("SELECT id FROM geo_table").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_with_json_type() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE json_table (
                id INT64,
                data JSON
            )",
        )
        .unwrap();

    session
        .execute_sql(r#"INSERT INTO json_table VALUES (1, JSON '{"name": "test", "value": 42}')"#)
        .unwrap();

    let result = session.execute_sql("SELECT id FROM json_table").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_with_interval_type() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE interval_table (
                id INT64,
                duration INTERVAL
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO interval_table VALUES (1, INTERVAL 1 DAY)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM interval_table")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_with_range_type() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE range_table (
                id INT64,
                date_range RANGE<DATE>
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO range_table VALUES (1, RANGE(DATE '2024-01-01', DATE '2024-12-31'))",
        )
        .unwrap();

    let result = session.execute_sql("SELECT id FROM range_table").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_with_generate_uuid_default() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE uuid_table (
                id STRING DEFAULT GENERATE_UUID(),
                name STRING
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO uuid_table (name) VALUES ('test')")
        .unwrap();

    let result = session.execute_sql("SELECT name FROM uuid_table").unwrap();
    assert_table_eq!(result, [["test"]]);
}

#[test]
fn test_create_table_with_current_date_default() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE date_default_table (
                id INT64,
                created_date DATE DEFAULT CURRENT_DATE()
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO date_default_table (id) VALUES (1)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM date_default_table")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_with_session_user_default() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE session_user_table (
                id INT64,
                created_by STRING DEFAULT SESSION_USER()
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO session_user_table (id) VALUES (1)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM session_user_table")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_partition_by_year() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE yearly_partitioned (
                id INT64,
                event_date DATE
            )
            PARTITION BY DATE_TRUNC(event_date, YEAR)",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO yearly_partitioned VALUES (1, DATE '2024-06-15')")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM yearly_partitioned")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_partition_by_hour() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE hourly_partitioned (
                id INT64,
                event_time TIMESTAMP
            )
            PARTITION BY TIMESTAMP_TRUNC(event_time, HOUR)",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO hourly_partitioned VALUES (1, TIMESTAMP '2024-01-15 10:30:00 UTC')",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM hourly_partitioned")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_clustered_no_partition() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE clustered_only (
                customer_id STRING,
                transaction_amount NUMERIC
            )
            CLUSTER BY customer_id",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO clustered_only VALUES ('C001', 100.50)")
        .unwrap();

    let result = session
        .execute_sql("SELECT customer_id FROM clustered_only")
        .unwrap();
    assert_table_eq!(result, [["C001"]]);
}

#[test]
fn test_create_table_partitioned_and_clustered() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE part_cluster_table (
                input_timestamp TIMESTAMP,
                customer_id STRING,
                transaction_amount NUMERIC
            )
            PARTITION BY TIMESTAMP_TRUNC(input_timestamp, HOUR)
            CLUSTER BY customer_id
            OPTIONS (
                partition_expiration_days = 3,
                description = 'a table clustered by customer_id'
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO part_cluster_table VALUES (TIMESTAMP '2024-01-15 10:00:00 UTC', 'C001', 100.00)",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT customer_id FROM part_cluster_table")
        .unwrap();
    assert_table_eq!(result, [["C001"]]);
}

#[test]
fn test_create_table_with_struct_array_complex() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE complex_nested_table (
                order_id INT64,
                items ARRAY<STRUCT<
                    product_id INT64,
                    name STRING,
                    quantity INT64,
                    price NUMERIC,
                    attributes STRUCT<color STRING, size STRING>
                >>
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO complex_nested_table VALUES (
                1,
                [STRUCT(101, 'Widget', 2, 9.99, STRUCT('red', 'medium'))]
            )",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT order_id, ARRAY_LENGTH(items) FROM complex_nested_table")
        .unwrap();
    assert_table_eq!(result, [[1, 1]]);
}

#[test]
fn test_create_table_with_explicit_mode() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE explicit_mode_table (
                required_field INT64 NOT NULL,
                optional_field STRING,
                repeated_field ARRAY<INT64>
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO explicit_mode_table VALUES (1, NULL, [1, 2, 3])")
        .unwrap();

    let result = session
        .execute_sql("SELECT required_field, ARRAY_LENGTH(repeated_field) FROM explicit_mode_table")
        .unwrap();
    assert_table_eq!(result, [[1, 3]]);
}

#[test]
fn test_create_table_column_with_description() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE column_desc_table (
                id INT64 OPTIONS(description = 'Unique identifier'),
                name STRING OPTIONS(description = 'Customer full name'),
                email STRING OPTIONS(description = 'Contact email address')
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO column_desc_table VALUES (1, 'Alice', 'alice@example.com')")
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM column_desc_table")
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}
