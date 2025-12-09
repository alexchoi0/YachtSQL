use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_kafka_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE kafka_test (
                id UInt64,
                message String,
                timestamp DateTime
            ) ENGINE = Kafka
            SETTINGS
                kafka_broker_list = 'localhost:9092',
                kafka_topic_list = 'test_topic',
                kafka_group_name = 'test_group',
                kafka_format = 'JSONEachRow'",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_kafka_engine_multiple_topics() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE kafka_multi (
                data String
            ) ENGINE = Kafka
            SETTINGS
                kafka_broker_list = 'broker1:9092,broker2:9092',
                kafka_topic_list = 'topic1,topic2,topic3',
                kafka_group_name = 'consumer_group',
                kafka_format = 'CSV',
                kafka_num_consumers = 2",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_s3_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE s3_test (
                id UInt64,
                name String,
                value Float64
            ) ENGINE = S3('https://bucket.s3.amazonaws.com/data.csv', 'CSV')",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_s3_engine_with_credentials() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE s3_auth (
                id UInt64,
                data String
            ) ENGINE = S3(
                'https://bucket.s3.amazonaws.com/path/data.parquet',
                'access_key_id',
                'secret_access_key',
                'Parquet'
            )",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_s3_engine_with_globs() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE s3_glob (
                date Date,
                value Int64
            ) ENGINE = S3('https://bucket.s3.amazonaws.com/data/**/file_*.csv', 'CSV')",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_s3queue_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE s3queue_test (
                id UInt64,
                data String
            ) ENGINE = S3Queue(
                'https://bucket.s3.amazonaws.com/queue/',
                'JSONEachRow'
            )",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_hdfs_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE hdfs_test (
                id UInt64,
                name String,
                amount Decimal64(2)
            ) ENGINE = HDFS('hdfs://hdfs1:9000/data/file.csv', 'CSV')",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_hdfs_engine_with_globs() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE hdfs_glob (
                timestamp DateTime,
                metric Float64
            ) ENGINE = HDFS('hdfs://hdfs1:9000/logs/*/data_*.tsv', 'TabSeparated')",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_mysql_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE mysql_test (
                id UInt64,
                name String
            ) ENGINE = MySQL(
                'host:3306',
                'database',
                'table',
                'user',
                'password'
            )",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Fix me!"]
#[test]
fn test_mysql_engine_with_options() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE mysql_opts (
                id Int64,
                value String
            ) ENGINE = MySQL(
                'mysql-server:3306',
                'mydb',
                'mytable',
                'root',
                'pass',
                1
            )",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_postgresql_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE pg_test (
                id UInt64,
                name String
            ) ENGINE = PostgreSQL(
                'host:5432',
                'database',
                'table',
                'user',
                'password'
            )",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_postgresql_engine_with_schema() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE pg_schema (
                id Int64,
                data String
            ) ENGINE = PostgreSQL(
                'pg-server:5432',
                'mydb',
                'mytable',
                'user',
                'pass',
                'myschema'
            )",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_mongodb_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE mongo_test (
                id String,
                name String,
                value Float64
            ) ENGINE = MongoDB(
                'mongodb://localhost:27017',
                'database',
                'collection'
            )",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_jdbc_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE jdbc_test (
                id UInt64,
                name String
            ) ENGINE = JDBC(
                'jdbc:mysql://localhost:3306/db',
                'schema',
                'table'
            )",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_odbc_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE odbc_test (
                id UInt64,
                name String
            ) ENGINE = ODBC(
                'DSN=mydsn',
                'database',
                'table'
            )",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Fix me!"]
#[test]
fn test_redis_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE redis_test (
                key String,
                value String
            ) ENGINE = Redis(
                'localhost:6379',
                0,
                'password',
                10
            )",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_sqlite_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE sqlite_test (
                id Int64,
                name String
            ) ENGINE = SQLite('/path/to/database.db', 'tablename')",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Fix me!"]
#[test]
fn test_rabbitmq_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE rabbitmq_test (
                id UInt64,
                message String
            ) ENGINE = RabbitMQ
            SETTINGS
                rabbitmq_host_port = 'localhost:5672',
                rabbitmq_exchange_name = 'exchange',
                rabbitmq_format = 'JSONEachRow'",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_nats_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE nats_test (
                id UInt64,
                data String
            ) ENGINE = NATS
            SETTINGS
                nats_url = 'localhost:4222',
                nats_subjects = 'subject1,subject2',
                nats_format = 'JSONEachRow'",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_azure_blob_storage_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE azure_test (
                id UInt64,
                name String
            ) ENGINE = AzureBlobStorage(
                'https://account.blob.core.windows.net/container/path',
                'account_name',
                'account_key',
                'CSV'
            )",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_deltalake_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE delta_test (
                id UInt64,
                name String,
                value Float64
            ) ENGINE = DeltaLake('s3://bucket/delta_table/')",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_iceberg_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE iceberg_test (
                id UInt64,
                ts DateTime,
                data String
            ) ENGINE = Iceberg('s3://bucket/iceberg_table/')",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_hudi_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE hudi_test (
                id UInt64,
                name String
            ) ENGINE = Hudi('s3://bucket/hudi_table/')",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_external_distributed() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE ext_dist (
                id UInt64,
                value Int64
            ) ENGINE = ExternalDistributed(
                'PostgreSQL',
                'host1:5432,host2:5432',
                'database',
                'table',
                'user',
                'password'
            )",
        )
        .unwrap();
    assert_table_eq!(result, []);
}
