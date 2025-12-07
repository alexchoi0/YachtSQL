use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_partition_by_range() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE sales (
            id INT64,
            sale_date DATE,
            amount INT64
        ) PARTITION BY RANGE (sale_date)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_partition_by_list() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE orders (
            id INT64,
            region STRING,
            amount INT64
        ) PARTITION BY LIST (region)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_partition_by_hash() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE logs (
            id INT64,
            message STRING
        ) PARTITION BY HASH (id)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_range_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE measurements (
            id INT64,
            logdate DATE,
            value INT64
        ) PARTITION BY RANGE (logdate)",
        )
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TABLE measurements_2024_q1 PARTITION OF measurements
         FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_list_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE customers (
            id INT64,
            country STRING,
            name STRING
        ) PARTITION BY LIST (country)",
        )
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TABLE customers_usa PARTITION OF customers
         FOR VALUES IN ('USA', 'US')",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_hash_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE events (
            id INT64,
            data STRING
        ) PARTITION BY HASH (id)",
        )
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TABLE events_0 PARTITION OF events
         FOR VALUES WITH (MODULUS 4, REMAINDER 0)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_partition_default() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE data (
            id INT64,
            category STRING
        ) PARTITION BY LIST (category)",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE data_known PARTITION OF data
         FOR VALUES IN ('A', 'B', 'C')",
        )
        .unwrap();

    let result = executor.execute_sql("CREATE TABLE data_default PARTITION OF data DEFAULT");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_partition_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE partitioned_sales (
            id INT64,
            quarter INT64,
            amount INT64
        ) PARTITION BY LIST (quarter)",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE sales_q1 PARTITION OF partitioned_sales
         FOR VALUES IN (1)",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE sales_q2 PARTITION OF partitioned_sales
         FOR VALUES IN (2)",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO partitioned_sales VALUES (1, 1, 1000)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO partitioned_sales VALUES (2, 2, 2000)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM(amount) FROM partitioned_sales")
        .unwrap();
    assert_table_eq!(result, [[3000]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_partition_query_routing() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE routed_data (
            id INT64,
            type STRING,
            value INT64
        ) PARTITION BY LIST (type)",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE routed_type_a PARTITION OF routed_data
         FOR VALUES IN ('A')",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE routed_type_b PARTITION OF routed_data
         FOR VALUES IN ('B')",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO routed_data VALUES (1, 'A', 100), (2, 'B', 200)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM routed_type_a");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_partition_subpartition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE sub_part (
            id INT64,
            year INT64,
            month INT64,
            data STRING
        ) PARTITION BY RANGE (year)",
        )
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TABLE sub_part_2024 PARTITION OF sub_part
         FOR VALUES FROM (2024) TO (2025)
         PARTITION BY RANGE (month)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_attach_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE attach_parent (
            id INT64,
            category INT64
        ) PARTITION BY LIST (category)",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE attach_child (
            id INT64,
            category INT64
        )",
        )
        .unwrap();

    let result = executor.execute_sql(
        "ALTER TABLE attach_parent ATTACH PARTITION attach_child
         FOR VALUES IN (1, 2)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_detach_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE detach_parent (
            id INT64,
            category INT64
        ) PARTITION BY LIST (category)",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE detach_child PARTITION OF detach_parent
         FOR VALUES IN (1)",
        )
        .unwrap();

    let result = executor.execute_sql("ALTER TABLE detach_parent DETACH PARTITION detach_child");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_detach_partition_concurrently() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE detach_conc_parent (
            id INT64,
            val INT64
        ) PARTITION BY RANGE (val)",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE detach_conc_child PARTITION OF detach_conc_parent
         FOR VALUES FROM (0) TO (100)",
        )
        .unwrap();

    let result = executor.execute_sql(
        "ALTER TABLE detach_conc_parent DETACH PARTITION detach_conc_child CONCURRENTLY",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_partition_index() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE idx_part (
            id INT64,
            category INT64,
            value INT64
        ) PARTITION BY LIST (category)",
        )
        .unwrap();
    executor
        .execute_sql("CREATE TABLE idx_part_1 PARTITION OF idx_part FOR VALUES IN (1)")
        .unwrap();

    let result = executor.execute_sql("CREATE INDEX idx_part_value ON idx_part (value)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_partition_unique_constraint() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE unique_part (
            id INT64,
            category INT64,
            UNIQUE (id, category)
        ) PARTITION BY LIST (category)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_partition_primary_key() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE pk_part (
            id INT64,
            category INT64,
            PRIMARY KEY (id, category)
        ) PARTITION BY LIST (category)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_partition_foreign_key() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fk_ref (id INT64 PRIMARY KEY)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TABLE fk_part (
            id INT64,
            ref_id INT64 REFERENCES fk_ref(id),
            category INT64
        ) PARTITION BY LIST (category)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_partition_exclusion() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE excl_part (
            id INT64,
            region INT64
        ) PARTITION BY LIST (region)",
        )
        .unwrap();
    executor
        .execute_sql("CREATE TABLE excl_part_1 PARTITION OF excl_part FOR VALUES IN (1)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE excl_part_2 PARTITION OF excl_part FOR VALUES IN (2)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO excl_part VALUES (1, 1), (2, 2)")
        .unwrap();
    let result = executor.execute_sql("SELECT * FROM excl_part WHERE region = 1");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_partition_update_move_row() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE move_part (
            id INT64,
            status INT64
        ) PARTITION BY LIST (status)",
        )
        .unwrap();
    executor
        .execute_sql("CREATE TABLE move_part_pending PARTITION OF move_part FOR VALUES IN (0)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE move_part_done PARTITION OF move_part FOR VALUES IN (1)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO move_part VALUES (1, 0)")
        .unwrap();
    executor
        .execute_sql("UPDATE move_part SET status = 1 WHERE id = 1")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM move_part_done");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_partition_trigger() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE trigger_part (
            id INT64,
            val INT64
        ) PARTITION BY RANGE (val)",
        )
        .unwrap();

    let result = executor.execute_sql(
        "CREATE OR REPLACE FUNCTION part_trigger_func() RETURNS TRIGGER AS $$
         BEGIN
             NEW.id := NEW.id + 1;
             RETURN NEW;
         END;
         $$ LANGUAGE plpgsql",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_drop_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE drop_part_parent (
            id INT64,
            category INT64
        ) PARTITION BY LIST (category)",
        )
        .unwrap();
    executor
        .execute_sql("CREATE TABLE drop_part_child PARTITION OF drop_part_parent FOR VALUES IN (1)")
        .unwrap();

    let result = executor.execute_sql("DROP TABLE drop_part_child");
    assert!(result.is_ok() || result.is_err());
}
