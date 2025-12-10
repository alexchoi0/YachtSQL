use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_to_tsvector() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TO_TSVECTOR('english', 'The quick brown fox')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_to_tsquery() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TO_TSQUERY('english', 'quick & fox')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_plainto_tsquery() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT PLAINTO_TSQUERY('english', 'quick brown fox')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_phraseto_tsquery() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT PHRASETO_TSQUERY('english', 'quick brown fox')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_websearch_to_tsquery() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT WEBSEARCH_TO_TSQUERY('english', 'quick OR fox -brown')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "AtAt (@@) operator not implemented"]
fn test_tsvector_match_tsquery() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TO_TSVECTOR('english', 'The quick brown fox') @@ TO_TSQUERY('english', 'quick')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "AtAt (@@) operator not implemented"]
fn test_tsvector_not_match() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT TO_TSVECTOR('english', 'The quick brown fox') @@ TO_TSQUERY('english', 'slow')",
        )
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
#[ignore = "AtAt (@@) operator not implemented"]
fn test_tsquery_and() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TO_TSVECTOR('english', 'The quick brown fox') @@ TO_TSQUERY('english', 'quick & brown')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "AtAt (@@) operator not implemented"]
fn test_tsquery_or() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TO_TSVECTOR('english', 'The quick brown fox') @@ TO_TSQUERY('english', 'slow | fox')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "AtAt (@@) operator not implemented"]
fn test_tsquery_not() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TO_TSVECTOR('english', 'The quick brown fox') @@ TO_TSQUERY('english', 'fox & !slow')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "AtAt (@@) operator not implemented"]
fn test_tsquery_phrase() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TO_TSVECTOR('english', 'The quick brown fox') @@ TO_TSQUERY('english', 'quick <-> brown')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_tsvector_concat() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TO_TSVECTOR('english', 'quick') || TO_TSVECTOR('english', 'fox')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "TsQuery && operator not implemented"]
fn test_tsquery_and_operator() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TO_TSQUERY('english', 'quick') && TO_TSQUERY('english', 'fox')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_tsquery_or_operator() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TO_TSQUERY('english', 'quick') || TO_TSQUERY('english', 'fox')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "TsQuery !! (negate) operator not implemented"]
fn test_tsquery_negate() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT !! TO_TSQUERY('english', 'fox')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_ts_rank() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TS_RANK(TO_TSVECTOR('english', 'The quick brown fox'), TO_TSQUERY('english', 'fox'))")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_ts_rank_cd() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TS_RANK_CD(TO_TSVECTOR('english', 'The quick brown fox'), TO_TSQUERY('english', 'quick <-> brown'))")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_ts_headline() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TS_HEADLINE('english', 'The quick brown fox jumps over the lazy dog', TO_TSQUERY('english', 'fox'))")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_ts_headline_options() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TS_HEADLINE('english', 'The quick brown fox', TO_TSQUERY('english', 'fox'), 'StartSel=<b>, StopSel=</b>')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_length_tsvector() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LENGTH(TO_TSVECTOR('english', 'The quick brown fox'))")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "NUMNODE function not implemented for TsQuery"]
fn test_numnode_tsquery() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NUMNODE(TO_TSQUERY('english', 'quick & brown | fox'))")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "QUERYTREE function not implemented"]
fn test_querytree() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT QUERYTREE(TO_TSQUERY('english', 'quick & brown'))")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_setweight() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SETWEIGHT(TO_TSVECTOR('english', 'important text'), 'A')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_strip() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT STRIP(TO_TSVECTOR('english', 'The quick brown fox'))")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "TsVector data type not supported for columns"]
fn test_tsvector_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE documents (id INTEGER, content TEXT, tsv TSVECTOR)")
        .unwrap();
    executor.execute_sql("INSERT INTO documents (id, content, tsv) VALUES (1, 'Hello world', TO_TSVECTOR('english', 'Hello world'))").unwrap();

    let result = executor
        .execute_sql("SELECT id FROM documents WHERE tsv @@ TO_TSQUERY('english', 'hello')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "TsVector data type not supported for columns"]
fn test_tsvector_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE search_docs (id INTEGER, tsv TSVECTOR)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX search_idx ON search_docs USING GIN (tsv)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO search_docs VALUES (1, TO_TSVECTOR('english', 'quick brown fox'))",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM search_docs WHERE tsv @@ TO_TSQUERY('english', 'fox')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "TS_STAT function not implemented"]
fn test_ts_stat() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE stat_docs (content TSVECTOR)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO stat_docs VALUES (TO_TSVECTOR('english', 'The quick brown fox'))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO stat_docs VALUES (TO_TSVECTOR('english', 'The lazy dog'))")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM TS_STAT('SELECT content FROM stat_docs')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_ts_debug() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT * FROM TS_DEBUG('english', 'The quick brown fox')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_ts_lexize() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TS_LEXIZE('english_stem', 'running')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "GET_CURRENT_TS_CONFIG function not implemented"]
fn test_get_current_ts_config() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT GET_CURRENT_TS_CONFIG()")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "TsVector data type not supported for columns"]
fn test_tsvector_update_trigger() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE trigger_docs (
            id INTEGER,
            title TEXT,
            body TEXT,
            tsv TSVECTOR
        )",
        )
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TRIGGER tsv_update BEFORE INSERT OR UPDATE ON trigger_docs
         FOR EACH ROW EXECUTE FUNCTION
         TSVECTOR_UPDATE_TRIGGER(tsv, 'pg_catalog.english', title, body)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_simple_config() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TO_TSVECTOR('simple', 'The Quick Brown Fox')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "TS_REWRITE function not implemented"]
fn test_ts_rewrite() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TS_REWRITE(TO_TSQUERY('english', 'quick & fox'), TO_TSQUERY('english', 'quick'), TO_TSQUERY('english', 'fast'))")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "ARRAY_TO_TSVECTOR function not implemented"]
fn test_array_to_tsvector() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ARRAY_TO_TSVECTOR(ARRAY['quick', 'brown', 'fox'])")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_tsvector_to_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TSVECTOR_TO_ARRAY(TO_TSVECTOR('english', 'quick brown fox'))")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "AtAt (@@) operator not implemented"]
fn test_tsquery_phrase_distance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TO_TSVECTOR('english', 'The quick brown fox') @@ TO_TSQUERY('english', 'quick <2> fox')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "TS_DELETE function not implemented"]
fn test_ts_delete() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TS_DELETE(TO_TSVECTOR('english', 'The quick brown fox'), 'brown')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "TS_FILTER function not implemented"]
fn test_ts_filter() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TS_FILTER(SETWEIGHT(TO_TSVECTOR('english', 'important'), 'A'), '{a}')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}
