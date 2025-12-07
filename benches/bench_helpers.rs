use yachtsql::QueryExecutor;

pub const ROW_COUNTS: [usize; 5] = [10, 20, 40, 80, 160];

pub fn setup_users_table(executor: &mut QueryExecutor, rows: usize) {
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, email STRING, age INT64)")
        .expect("Failed to create users table");

    for i in 0..rows {
        let name = format!("User{}", i);
        let email = format!("user{}@example.com", i);
        let age = 20 + (i % 50);
        executor
            .execute_sql(&format!(
                "INSERT INTO users VALUES ({}, '{}', '{}', {})",
                i, name, email, age
            ))
            .expect("Failed to insert user");
    }
}

pub fn setup_orders_table(executor: &mut QueryExecutor, rows: usize) {
    executor
        .execute_sql(
            "CREATE TABLE orders (id INT64, user_id INT64, amount FLOAT64, status STRING, category STRING)",
        )
        .expect("Failed to create orders table");

    let statuses = ["pending", "completed", "cancelled", "shipped"];
    let categories = ["electronics", "clothing", "food", "books", "toys"];

    for i in 0..rows {
        let user_id = i % 20;
        let amount = 10.0 + (i as f64 * 1.5);
        let status = statuses[i % statuses.len()];
        let category = categories[i % categories.len()];
        executor
            .execute_sql(&format!(
                "INSERT INTO orders VALUES ({}, {}, {}, '{}', '{}')",
                i, user_id, amount, status, category
            ))
            .expect("Failed to insert order");
    }
}

pub fn setup_products_table(executor: &mut QueryExecutor, rows: usize) {
    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING, price FLOAT64, stock INT64)")
        .expect("Failed to create products table");

    for i in 0..rows {
        let name = format!("Product{}", i);
        let price = 5.0 + (i as f64 * 2.5);
        let stock = 10 + (i % 100);
        executor
            .execute_sql(&format!(
                "INSERT INTO products VALUES ({}, '{}', {}, {})",
                i, name, price, stock
            ))
            .expect("Failed to insert product");
    }
}

pub fn setup_join_tables(executor: &mut QueryExecutor, rows: usize) {
    setup_users_table(executor, rows);
    setup_orders_table(executor, rows * 3);
}
