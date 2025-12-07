use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_stem() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT stem('en', 'running')")
        .unwrap();
    assert_table_eq!(result, [["run"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_stem_multiple_words() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT stem('en', 'cats'), stem('en', 'running'), stem('en', 'better')")
        .unwrap();
    assert_table_eq!(result, [["cat", "run", "better"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_stem_different_languages() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT stem('en', 'walking'), stem('ru', 'бегущий'), stem('de', 'laufend')")
        .unwrap();
    assert_table_eq!(result, [["walk", "бегущ", "laufend"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_lemmatize() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT lemmatize('en', 'running')")
        .unwrap();
    assert_table_eq!(result, [["run"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_synonyms() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT synonyms('en', 'big')")
        .unwrap();
    assert_table_eq!(result, [[["large", "great", "huge"]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_detect_language() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT detectLanguage('This is an English text')")
        .unwrap();
    assert_table_eq!(result, [["en"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_detect_language_mixed() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT detectLanguageMixed('Ich liebe Paris and London')")
        .unwrap();
    assert_table_eq!(result, [[["de", "en"]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_detect_language_unknown() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT detectLanguageUnknown('12345')")
        .unwrap();
    assert_table_eq!(result, [["unknown"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_detect_charset() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT detectCharset('Hello World')")
        .unwrap();
    assert_table_eq!(result, [["UTF-8"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_detect_tone_sentiment() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT detectTonality('I love this product, it is amazing!')")
        .unwrap();
    assert_table_eq!(result, [["positive"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_detect_programming_language() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT detectProgrammingLanguage('def hello(): print(\"world\")')")
        .unwrap();
    assert_table_eq!(result, [["Python"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_tokenize() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tokens('Hello, World! How are you?')")
        .unwrap();
    assert_table_eq!(result, [[["Hello", "World", "How", "are", "you"]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_ngrams_text() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ngrams('hello', 3)").unwrap();
    assert_table_eq!(result, [[["hel", "ell", "llo"]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_normalize_query() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT normalizeQuery('SELECT * FROM table WHERE id = 123')")
        .unwrap();
    assert_table_eq!(result, [["SELECT * FROM table WHERE id = ?"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_normalized_query_hash() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT normalizedQueryHash('SELECT * FROM table WHERE id = 123')")
        .unwrap();
    assert!(result.num_rows() == 1); // Hash value is implementation-specific
}

#[ignore = "Implement me!"]
#[test]
fn test_nlp_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE texts (id UInt32, content String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO texts VALUES
            (1, 'The quick brown fox jumps'),
            (2, 'Bonjour le monde'),
            (3, 'Hallo Welt')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, content, detectLanguage(content) AS lang
            FROM texts
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "The quick brown fox jumps", "en"],
            [2, "Bonjour le monde", "fr"],
            [3, "Hallo Welt", "de"]
        ]
    );
}

#[ignore = "Implement me!"]
#[test]
fn test_word_shingles() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT wordShingleMinHash('The quick brown fox', 3, 3)")
        .unwrap();
    assert!(result.num_rows() == 1); // Hash values are implementation-specific
}

#[ignore = "Implement me!"]
#[test]
fn test_word_shingle_sim_hash() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT wordShingleSimHash('The quick brown fox', 3)")
        .unwrap();
    assert!(result.num_rows() == 1); // Hash value is implementation-specific
}

#[ignore = "Implement me!"]
#[test]
fn test_sentence_similarity() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT ngramSimHash('The quick brown fox', 3),
                    ngramSimHash('The fast brown fox', 3)",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // Hash values are implementation-specific
}
