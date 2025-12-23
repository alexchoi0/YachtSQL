#![cfg(feature = "concurrent")]

use std::time::{Duration, Instant};

use tokio::task::JoinSet;
use yachtsql_executor::AsyncQueryExecutor;

#[tokio::test]
async fn test_concurrent_reads_different_tables() {
    let executor = AsyncQueryExecutor::new();

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .await
        .unwrap();
    executor
        .execute_sql("CREATE TABLE orders (id INT64, user_id INT64, amount FLOAT64)")
        .await
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 1, 100.0), (2, 1, 200.0), (3, 2, 150.0)")
        .await
        .unwrap();

    let start = Instant::now();

    let mut handles = JoinSet::new();

    for i in 0..10 {
        let exec = executor.clone();
        handles.spawn(async move {
            if i % 2 == 0 {
                exec.execute_sql("SELECT * FROM users").await
            } else {
                exec.execute_sql("SELECT * FROM orders").await
            }
        });
    }

    let mut results = Vec::new();
    while let Some(res) = handles.join_next().await {
        results.push(res.unwrap());
    }

    let elapsed = start.elapsed();
    println!("Concurrent reads on different tables: {:?}", elapsed);

    for result in results {
        assert!(result.is_ok());
    }
}

#[tokio::test]
async fn test_concurrent_reads_same_table() {
    let executor = AsyncQueryExecutor::new();

    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING, price FLOAT64)")
        .await
        .unwrap();
    executor.execute_sql("INSERT INTO products VALUES (1, 'Laptop', 999.99), (2, 'Mouse', 29.99), (3, 'Keyboard', 79.99)").await.unwrap();

    let start = Instant::now();

    let mut handles = JoinSet::new();

    for _ in 0..10 {
        let exec = executor.clone();
        handles.spawn(async move {
            exec.execute_sql("SELECT * FROM products WHERE price > 50")
                .await
        });
    }

    let mut results = Vec::new();
    while let Some(res) = handles.join_next().await {
        results.push(res.unwrap());
    }

    let elapsed = start.elapsed();
    println!("Concurrent reads on same table: {:?}", elapsed);

    for result in results {
        let table = result.unwrap();
        assert_eq!(table.row_count(), 2);
    }
}

#[tokio::test]
async fn test_concurrent_read_write_different_tables() {
    let executor = AsyncQueryExecutor::new();

    executor
        .execute_sql("CREATE TABLE table_a (id INT64, value STRING)")
        .await
        .unwrap();
    executor
        .execute_sql("CREATE TABLE table_b (id INT64, value STRING)")
        .await
        .unwrap();

    executor
        .execute_sql("INSERT INTO table_a VALUES (1, 'a1'), (2, 'a2')")
        .await
        .unwrap();
    executor
        .execute_sql("INSERT INTO table_b VALUES (1, 'b1'), (2, 'b2')")
        .await
        .unwrap();

    let start = Instant::now();

    let mut handles = JoinSet::new();

    for i in 0..10 {
        let exec = executor.clone();
        handles.spawn(async move {
            if i % 2 == 0 {
                exec.execute_sql("SELECT * FROM table_a").await
            } else {
                exec.execute_sql("INSERT INTO table_b VALUES (100, 'new')")
                    .await
            }
        });
    }

    while let Some(res) = handles.join_next().await {
        assert!(res.unwrap().is_ok());
    }

    let elapsed = start.elapsed();
    println!("Concurrent read/write on different tables: {:?}", elapsed);

    let result = executor.execute_sql("SELECT * FROM table_b").await.unwrap();
    assert!(result.row_count() >= 2);
}

#[tokio::test]
async fn test_sequential_writes_same_table() {
    let executor = AsyncQueryExecutor::new();

    executor
        .execute_sql("CREATE TABLE counter (id INT64, count INT64)")
        .await
        .unwrap();
    executor
        .execute_sql("INSERT INTO counter VALUES (1, 0)")
        .await
        .unwrap();

    let start = Instant::now();

    let mut handles = JoinSet::new();

    for i in 0..5 {
        let exec = executor.clone();
        handles.spawn(async move {
            exec.execute_sql(&format!("INSERT INTO counter VALUES ({}, {})", i + 10, i))
                .await
        });
    }

    while let Some(res) = handles.join_next().await {
        assert!(res.unwrap().is_ok());
    }

    let elapsed = start.elapsed();
    println!("Sequential writes to same table: {:?}", elapsed);

    let result = executor.execute_sql("SELECT * FROM counter").await.unwrap();
    assert_eq!(result.row_count(), 6);
}

#[tokio::test]
async fn test_batch_execution() {
    let executor = AsyncQueryExecutor::new();

    executor
        .execute_sql("CREATE TABLE batch_test (id INT64, value STRING)")
        .await
        .unwrap();
    executor
        .execute_sql("INSERT INTO batch_test VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        .await
        .unwrap();

    let queries = vec![
        "SELECT * FROM batch_test".to_string(),
        "SELECT * FROM batch_test WHERE id > 1".to_string(),
        "SELECT * FROM batch_test WHERE id = 2".to_string(),
    ];

    let start = Instant::now();
    let results = executor.execute_batch(queries).await;
    let elapsed = start.elapsed();

    println!("Batch execution of 3 queries: {:?}", elapsed);

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].as_ref().unwrap().row_count(), 3);
    assert_eq!(results[1].as_ref().unwrap().row_count(), 2);
    assert_eq!(results[2].as_ref().unwrap().row_count(), 1);
}

#[tokio::test]
async fn test_read_write_isolation() {
    let executor = AsyncQueryExecutor::new();

    executor
        .execute_sql("CREATE TABLE isolation_test (id INT64, status STRING)")
        .await
        .unwrap();
    executor
        .execute_sql("INSERT INTO isolation_test VALUES (1, 'initial')")
        .await
        .unwrap();

    let exec1 = executor.clone();
    let exec2 = executor.clone();

    let read_handle = tokio::spawn(async move {
        let mut results = Vec::new();
        for _ in 0..5 {
            let result = exec1
                .execute_sql("SELECT * FROM isolation_test")
                .await
                .unwrap();
            results.push(result.row_count());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        results
    });

    let write_handle = tokio::spawn(async move {
        for i in 2..=4 {
            exec2
                .execute_sql(&format!(
                    "INSERT INTO isolation_test VALUES ({}, 'added')",
                    i
                ))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    });

    let (read_results, _) = tokio::join!(read_handle, write_handle);
    let counts = read_results.unwrap();

    println!("Row counts during concurrent operations: {:?}", counts);

    assert!(counts.iter().all(|&c| c >= 1));

    let final_result = executor
        .execute_sql("SELECT * FROM isolation_test")
        .await
        .unwrap();
    assert_eq!(final_result.row_count(), 4);
}

#[tokio::test]
async fn test_delete_during_reads() {
    let executor = AsyncQueryExecutor::new();

    executor
        .execute_sql("CREATE TABLE delete_test (id INT64, name STRING)")
        .await
        .unwrap();
    for i in 1..=10 {
        executor
            .execute_sql(&format!(
                "INSERT INTO delete_test VALUES ({}, 'item{}')",
                i, i
            ))
            .await
            .unwrap();
    }

    let exec1 = executor.clone();
    let exec2 = executor.clone();

    let read_handle = tokio::spawn(async move {
        let mut results = Vec::new();
        for _ in 0..5 {
            let result = exec1
                .execute_sql("SELECT * FROM delete_test")
                .await
                .unwrap();
            results.push(result.row_count());
        }
        results
    });

    let delete_handle = tokio::spawn(async move {
        exec2
            .execute_sql("DELETE FROM delete_test WHERE id > 5")
            .await
    });

    let (read_results, delete_result) = tokio::join!(read_handle, delete_handle);

    assert!(delete_result.unwrap().is_ok());
    let counts = read_results.unwrap();
    println!("Row counts during delete: {:?}", counts);

    let final_result = executor
        .execute_sql("SELECT * FROM delete_test")
        .await
        .unwrap();
    assert_eq!(final_result.row_count(), 5);
}

#[tokio::test]
async fn test_update_during_reads() {
    let executor = AsyncQueryExecutor::new();

    executor
        .execute_sql("CREATE TABLE update_test (id INT64, value INT64)")
        .await
        .unwrap();
    executor
        .execute_sql("INSERT INTO update_test VALUES (1, 100), (2, 200), (3, 300)")
        .await
        .unwrap();

    let exec1 = executor.clone();
    let exec2 = executor.clone();

    let read_handle = tokio::spawn(async move {
        let mut results = Vec::new();
        for _ in 0..3 {
            let result = exec1
                .execute_sql("SELECT * FROM update_test WHERE value > 150")
                .await
                .unwrap();
            results.push(result.row_count());
        }
        results
    });

    let update_handle = tokio::spawn(async move {
        exec2
            .execute_sql("UPDATE update_test SET value = 50 WHERE id = 2")
            .await
    });

    let (read_results, update_result) = tokio::join!(read_handle, update_handle);

    assert!(update_result.unwrap().is_ok());
    println!("Row counts during update: {:?}", read_results.unwrap());

    let final_result = executor
        .execute_sql("SELECT * FROM update_test WHERE value > 150")
        .await
        .unwrap();
    assert_eq!(final_result.row_count(), 1);
}

#[tokio::test]
async fn test_high_concurrency_reads() {
    let executor = AsyncQueryExecutor::new();

    executor
        .execute_sql("CREATE TABLE stress_test (id INT64, data STRING)")
        .await
        .unwrap();
    for i in 1..=100 {
        executor
            .execute_sql(&format!(
                "INSERT INTO stress_test VALUES ({}, 'data{}')",
                i, i
            ))
            .await
            .unwrap();
    }

    let start = Instant::now();

    let mut handles = JoinSet::new();
    for _ in 0..50 {
        let exec = executor.clone();
        handles.spawn(async move {
            exec.execute_sql("SELECT * FROM stress_test WHERE id > 50")
                .await
        });
    }

    let mut results = Vec::new();
    while let Some(res) = handles.join_next().await {
        results.push(res.unwrap());
    }

    let elapsed = start.elapsed();
    println!(
        "50 concurrent reads: {:?} ({:.2} queries/sec)",
        elapsed,
        50.0 / elapsed.as_secs_f64()
    );

    for result in results {
        let table = result.unwrap();
        assert_eq!(table.row_count(), 50);
    }
}

#[tokio::test]
async fn test_parallel_queries_on_multiple_tables() {
    let executor = AsyncQueryExecutor::new();

    for i in 1..=5 {
        executor
            .execute_sql(&format!("CREATE TABLE table_{} (id INT64, value INT64)", i))
            .await
            .unwrap();
        for j in 1..=20 {
            executor
                .execute_sql(&format!(
                    "INSERT INTO table_{} VALUES ({}, {})",
                    i,
                    j,
                    j * i
                ))
                .await
                .unwrap();
        }
    }

    let start = Instant::now();

    let mut handles = JoinSet::new();
    for table_num in 1..=5 {
        for _ in 0..10 {
            let exec = executor.clone();
            handles.spawn(async move {
                exec.execute_sql(&format!("SELECT * FROM table_{}", table_num))
                    .await
            });
        }
    }

    let mut success_count = 0;
    while let Some(res) = handles.join_next().await {
        if res.unwrap().is_ok() {
            success_count += 1;
        }
    }

    let elapsed = start.elapsed();
    println!(
        "50 parallel queries on 5 tables: {:?} ({:.2} queries/sec)",
        elapsed,
        50.0 / elapsed.as_secs_f64()
    );

    assert_eq!(success_count, 50);
}
