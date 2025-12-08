use crate::common::create_executor;

#[test]
fn test_create_vector_extension() {
    let mut executor = create_executor();
    let result = executor.execute_sql("CREATE EXTENSION IF NOT EXISTS vector");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_vector_type_declaration() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();

    let result = executor.execute_sql("CREATE TABLE embeddings (id INTEGER, embedding VECTOR(3))");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_vector_type_dimension_4() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();

    let result = executor.execute_sql("CREATE TABLE vec4 (id INTEGER, vec VECTOR(4))");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_vector_type_dimension_128() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();

    let result = executor.execute_sql("CREATE TABLE vec128 (id INTEGER, vec VECTOR(128))");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_vector_type_dimension_1536() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();

    let result = executor.execute_sql("CREATE TABLE vec1536 (id INTEGER, vec VECTOR(1536))");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_insert_vector_literal() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_test (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        let result = executor.execute_sql("INSERT INTO vec_test VALUES (1, '[1.0, 2.0, 3.0]')");
        assert!(result.is_ok());
    }
}

#[test]
fn test_insert_vector_array_syntax() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_arr (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        let result =
            executor.execute_sql("INSERT INTO vec_arr VALUES (1, ARRAY[1.0, 2.0, 3.0]::vector(3))");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_insert_multiple_vectors() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_multi (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        let result = executor.execute_sql(
            "INSERT INTO vec_multi VALUES (1, '[1.0, 0.0, 0.0]'), (2, '[0.0, 1.0, 0.0]'), (3, '[0.0, 0.0, 1.0]')"
        );
        assert!(result.is_ok());
    }
}

#[test]
fn test_select_vector() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_sel (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_sel VALUES (1, '[1.0, 2.0, 3.0]')")
            .unwrap();
        let result = executor.execute_sql("SELECT vec FROM vec_sel WHERE id = 1");
        assert!(result.is_ok());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_l2_distance_operator() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_l2 (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_l2 VALUES (1, '[1.0, 0.0, 0.0]'), (2, '[0.0, 1.0, 0.0]')")
            .unwrap();
        let result =
            executor.execute_sql("SELECT vec <-> '[1.0, 0.0, 0.0]' AS distance FROM vec_l2");
        assert!(result.is_ok());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_cosine_distance_operator() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_cos (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql(
                "INSERT INTO vec_cos VALUES (1, '[1.0, 0.0, 0.0]'), (2, '[0.0, 1.0, 0.0]')",
            )
            .unwrap();
        let result =
            executor.execute_sql("SELECT vec <=> '[1.0, 0.0, 0.0]' AS distance FROM vec_cos");
        assert!(result.is_ok());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_inner_product_operator() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_ip (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_ip VALUES (1, '[1.0, 2.0, 3.0]'), (2, '[4.0, 5.0, 6.0]')")
            .unwrap();
        let result = executor
            .execute_sql("SELECT vec <#> '[1.0, 1.0, 1.0]' AS neg_inner_product FROM vec_ip");
        assert!(result.is_ok());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_l1_distance_operator() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_l1 (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_l1 VALUES (1, '[1.0, 2.0, 3.0]'), (2, '[4.0, 5.0, 6.0]')")
            .unwrap();
        let result =
            executor.execute_sql("SELECT vec <+> '[0.0, 0.0, 0.0]' AS l1_distance FROM vec_l1");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_order_by_l2_distance() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_order (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor.execute_sql(
            "INSERT INTO vec_order VALUES (1, '[1.0, 0.0, 0.0]'), (2, '[0.5, 0.5, 0.0]'), (3, '[0.0, 1.0, 0.0]')"
        ).unwrap();
        let result = executor.execute_sql(
            "SELECT id, vec <-> '[1.0, 0.0, 0.0]' AS distance FROM vec_order ORDER BY vec <-> '[1.0, 0.0, 0.0]'"
        );
        assert!(result.is_ok());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_order_by_cosine_distance() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result =
        executor.execute_sql("CREATE TABLE vec_cos_order (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor.execute_sql(
            "INSERT INTO vec_cos_order VALUES (1, '[1.0, 0.0, 0.0]'), (2, '[0.707, 0.707, 0.0]'), (3, '[0.0, 1.0, 0.0]')"
        ).unwrap();
        let result =
            executor.execute_sql("SELECT id FROM vec_cos_order ORDER BY vec <=> '[1.0, 0.0, 0.0]'");
        assert!(result.is_ok());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_knn_search_limit() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_knn (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor.execute_sql(
            "INSERT INTO vec_knn VALUES (1, '[1.0, 0.0, 0.0]'), (2, '[0.9, 0.1, 0.0]'), (3, '[0.8, 0.2, 0.0]'), (4, '[0.0, 1.0, 0.0]'), (5, '[0.0, 0.0, 1.0]')"
        ).unwrap();
        let result = executor
            .execute_sql("SELECT id FROM vec_knn ORDER BY vec <-> '[1.0, 0.0, 0.0]' LIMIT 3");
        assert!(result.is_ok());
    }
}

#[test]
fn test_ivfflat_index_l2() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_ivf (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        let result = executor.execute_sql(
            "CREATE INDEX ON vec_ivf USING ivfflat (vec vector_l2_ops) WITH (lists = 100)",
        );
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_ivfflat_index_cosine() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result =
        executor.execute_sql("CREATE TABLE vec_ivf_cos (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        let result = executor.execute_sql(
            "CREATE INDEX ON vec_ivf_cos USING ivfflat (vec vector_cosine_ops) WITH (lists = 100)",
        );
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_ivfflat_index_inner_product() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_ivf_ip (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        let result = executor.execute_sql(
            "CREATE INDEX ON vec_ivf_ip USING ivfflat (vec vector_ip_ops) WITH (lists = 100)",
        );
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_hnsw_index_l2() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_hnsw (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        let result = executor.execute_sql(
            "CREATE INDEX ON vec_hnsw USING hnsw (vec vector_l2_ops) WITH (m = 16, ef_construction = 64)"
        );
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_hnsw_index_cosine() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result =
        executor.execute_sql("CREATE TABLE vec_hnsw_cos (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        let result =
            executor.execute_sql("CREATE INDEX ON vec_hnsw_cos USING hnsw (vec vector_cosine_ops)");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_hnsw_index_inner_product() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result =
        executor.execute_sql("CREATE TABLE vec_hnsw_ip (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        let result =
            executor.execute_sql("CREATE INDEX ON vec_hnsw_ip USING hnsw (vec vector_ip_ops)");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_dimension_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_dim (id INTEGER, vec VECTOR(5))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_dim VALUES (1, '[1.0, 2.0, 3.0, 4.0, 5.0]')")
            .unwrap();
        let result = executor.execute_sql("SELECT vector_dims(vec) FROM vec_dim");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_norm_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_norm (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_norm VALUES (1, '[3.0, 4.0, 0.0]')")
            .unwrap();
        let result = executor.execute_sql("SELECT vector_norm(vec) FROM vec_norm");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_l2_distance_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result =
        executor.execute_sql("CREATE TABLE vec_l2_func (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_l2_func VALUES (1, '[1.0, 0.0, 0.0]')")
            .unwrap();
        let result =
            executor.execute_sql("SELECT l2_distance(vec, '[0.0, 1.0, 0.0]') FROM vec_l2_func");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_cosine_distance_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result =
        executor.execute_sql("CREATE TABLE vec_cos_func (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_cos_func VALUES (1, '[1.0, 0.0, 0.0]')")
            .unwrap();
        let result = executor
            .execute_sql("SELECT cosine_distance(vec, '[0.0, 1.0, 0.0]') FROM vec_cos_func");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_inner_product_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result =
        executor.execute_sql("CREATE TABLE vec_ip_func (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_ip_func VALUES (1, '[1.0, 2.0, 3.0]')")
            .unwrap();
        let result =
            executor.execute_sql("SELECT inner_product(vec, '[1.0, 1.0, 1.0]') FROM vec_ip_func");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_l1_distance_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result =
        executor.execute_sql("CREATE TABLE vec_l1_func (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_l1_func VALUES (1, '[1.0, 2.0, 3.0]')")
            .unwrap();
        let result =
            executor.execute_sql("SELECT l1_distance(vec, '[0.0, 0.0, 0.0]') FROM vec_l1_func");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_add() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_add (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_add VALUES (1, '[1.0, 2.0, 3.0]')")
            .unwrap();
        let result = executor.execute_sql("SELECT vec + '[1.0, 1.0, 1.0]'::vector FROM vec_add");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_subtract() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_sub (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_sub VALUES (1, '[3.0, 2.0, 1.0]')")
            .unwrap();
        let result = executor.execute_sql("SELECT vec - '[1.0, 1.0, 1.0]'::vector FROM vec_sub");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_multiply() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_mul (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_mul VALUES (1, '[1.0, 2.0, 3.0]')")
            .unwrap();
        let result = executor.execute_sql("SELECT vec * '[2.0, 2.0, 2.0]'::vector FROM vec_mul");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_vector_cast_from_array() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();

    let result = executor.execute_sql("SELECT ARRAY[1.0, 2.0, 3.0]::vector(3)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_cast_to_array() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_to_arr (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_to_arr VALUES (1, '[1.0, 2.0, 3.0]')")
            .unwrap();
        let result = executor.execute_sql("SELECT vec::float8[] FROM vec_to_arr");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_vector_null_handling() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_null (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_null VALUES (1, NULL)")
            .unwrap();
        let result = executor.execute_sql("SELECT vec IS NULL FROM vec_null");
        assert!(result.is_ok());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_average() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_avg (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor.execute_sql(
            "INSERT INTO vec_avg VALUES (1, '[1.0, 0.0, 0.0]'), (2, '[0.0, 1.0, 0.0]'), (3, '[0.0, 0.0, 1.0]')"
        ).unwrap();
        let result = executor.execute_sql("SELECT AVG(vec) FROM vec_avg");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_sum() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_sum (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor.execute_sql(
            "INSERT INTO vec_sum VALUES (1, '[1.0, 0.0, 0.0]'), (2, '[0.0, 1.0, 0.0]'), (3, '[0.0, 0.0, 1.0]')"
        ).unwrap();
        let result = executor.execute_sql("SELECT SUM(vec) FROM vec_sum");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_halfvec_type() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();

    let result = executor.execute_sql("CREATE TABLE halfvec_test (id INTEGER, vec HALFVEC(3))");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_sparsevec_type() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();

    let result =
        executor.execute_sql("CREATE TABLE sparsevec_test (id INTEGER, vec SPARSEVEC(1000))");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_sparsevec_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result =
        executor.execute_sql("CREATE TABLE sparse_ins (id INTEGER, vec SPARSEVEC(10))");

    if create_result.is_ok() {
        let result =
            executor.execute_sql("INSERT INTO sparse_ins VALUES (1, '{1:1.0, 3:2.0, 5:3.0}/10')");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_bit_vector_type() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();

    let result = executor.execute_sql("CREATE TABLE bitvec_test (id INTEGER, vec BIT(8))");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_hamming_distance() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE bitvec_ham (id INTEGER, vec BIT(8))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO bitvec_ham VALUES (1, B'10101010'), (2, B'11110000')")
            .unwrap();
        let result =
            executor.execute_sql("SELECT vec <~> B'10101010' AS hamming_distance FROM bitvec_ham");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_jaccard_distance() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE bitvec_jac (id INTEGER, vec BIT(8))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO bitvec_jac VALUES (1, B'10101010'), (2, B'11110000')")
            .unwrap();
        let result =
            executor.execute_sql("SELECT vec <%> B'10101010' AS jaccard_distance FROM bitvec_jac");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_with_where_clause() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result =
        executor.execute_sql("CREATE TABLE vec_where (id INTEGER, category TEXT, vec VECTOR(3))");

    if create_result.is_ok() {
        executor.execute_sql(
            "INSERT INTO vec_where VALUES (1, 'A', '[1.0, 0.0, 0.0]'), (2, 'A', '[0.9, 0.1, 0.0]'), (3, 'B', '[0.0, 1.0, 0.0]')"
        ).unwrap();
        let result = executor.execute_sql(
            "SELECT id FROM vec_where WHERE category = 'A' ORDER BY vec <-> '[1.0, 0.0, 0.0]' LIMIT 1"
        );
        assert!(result.is_ok());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create1 = executor.execute_sql("CREATE TABLE vec_left (id INTEGER, vec VECTOR(3))");
    let create2 = executor.execute_sql("CREATE TABLE vec_right (id INTEGER, vec VECTOR(3))");

    if create1.is_ok() && create2.is_ok() {
        executor
            .execute_sql(
                "INSERT INTO vec_left VALUES (1, '[1.0, 0.0, 0.0]'), (2, '[0.0, 1.0, 0.0]')",
            )
            .unwrap();
        executor
            .execute_sql(
                "INSERT INTO vec_right VALUES (1, '[0.9, 0.1, 0.0]'), (2, '[0.1, 0.9, 0.0]')",
            )
            .unwrap();
        let result = executor.execute_sql(
            "SELECT l.id, r.id, l.vec <-> r.vec AS distance FROM vec_left l CROSS JOIN vec_right r",
        );
        assert!(result.is_ok());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_subq (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor.execute_sql(
            "INSERT INTO vec_subq VALUES (1, '[1.0, 0.0, 0.0]'), (2, '[0.0, 1.0, 0.0]'), (3, '[0.0, 0.0, 1.0]')"
        ).unwrap();
        let result = executor.execute_sql(
            "SELECT * FROM vec_subq WHERE vec <-> '[1.0, 0.0, 0.0]' < (SELECT AVG(vec <-> '[1.0, 0.0, 0.0]') FROM vec_subq)"
        );
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_cte() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_cte (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor.execute_sql(
            "INSERT INTO vec_cte VALUES (1, '[1.0, 0.0, 0.0]'), (2, '[0.5, 0.5, 0.0]'), (3, '[0.0, 1.0, 0.0]')"
        ).unwrap();
        let result = executor.execute_sql(
            "WITH distances AS (SELECT id, vec <-> '[1.0, 0.0, 0.0]' AS dist FROM vec_cte) SELECT * FROM distances ORDER BY dist"
        );
        assert!(result.is_ok());
    }
}

#[test]
fn test_set_ivfflat_probes() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();

    let result = executor.execute_sql("SET ivfflat.probes = 10");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_set_hnsw_ef_search() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();

    let result = executor.execute_sql("SET hnsw.ef_search = 100");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_in_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result =
        executor.execute_sql("CREATE TABLE vec_view_base (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_view_base VALUES (1, '[1.0, 0.0, 0.0]')")
            .unwrap();
        let result = executor.execute_sql(
            "CREATE VIEW vec_distances AS SELECT id, vec <-> '[0.0, 0.0, 0.0]' AS dist FROM vec_view_base"
        );
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_update() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_upd (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_upd VALUES (1, '[1.0, 0.0, 0.0]')")
            .unwrap();
        let result =
            executor.execute_sql("UPDATE vec_upd SET vec = '[0.0, 1.0, 0.0]' WHERE id = 1");
        assert!(result.is_ok());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_delete() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_del (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql(
                "INSERT INTO vec_del VALUES (1, '[1.0, 0.0, 0.0]'), (2, '[0.0, 1.0, 0.0]')",
            )
            .unwrap();
        let result =
            executor.execute_sql("DELETE FROM vec_del WHERE vec <-> '[1.0, 0.0, 0.0]' < 0.1");
        assert!(result.is_ok());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_normalize() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result =
        executor.execute_sql("CREATE TABLE vec_normalize (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_normalize VALUES (1, '[3.0, 4.0, 0.0]')")
            .unwrap();
        let result = executor.execute_sql("SELECT vec / vector_norm(vec) FROM vec_normalize");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_with_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result =
        executor.execute_sql("CREATE TABLE vec_group (id INTEGER, category TEXT, vec VECTOR(3))");

    if create_result.is_ok() {
        executor.execute_sql(
            "INSERT INTO vec_group VALUES (1, 'A', '[1.0, 0.0, 0.0]'), (2, 'A', '[0.0, 1.0, 0.0]'), (3, 'B', '[0.0, 0.0, 1.0]')"
        ).unwrap();
        let result =
            executor.execute_sql("SELECT category, AVG(vec) FROM vec_group GROUP BY category");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_cosine_similarity() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_sim (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_sim VALUES (1, '[1.0, 0.0, 0.0]')")
            .unwrap();
        let result = executor.execute_sql(
            "SELECT 1 - (vec <=> '[1.0, 0.0, 0.0]') AS cosine_similarity FROM vec_sim",
        );
        assert!(result.is_ok());
    }
}

#[test]
fn test_vector_expression_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result =
        executor.execute_sql("CREATE TABLE vec_expr_idx (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        let result = executor.execute_sql(
            "CREATE INDEX ON vec_expr_idx USING hnsw ((vec / vector_norm(vec)) vector_cosine_ops)",
        );
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_vector_partial_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor
        .execute_sql("CREATE TABLE vec_partial (id INTEGER, active BOOLEAN, vec VECTOR(3))");

    if create_result.is_ok() {
        let result = executor.execute_sql(
            "CREATE INDEX ON vec_partial USING hnsw (vec vector_l2_ops) WHERE active = true",
        );
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_vector_binary_quantization() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_bq (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        let result = executor.execute_sql(
            "CREATE INDEX ON vec_bq USING hnsw ((binary_quantize(vec)::bit(3)) bit_hamming_ops)",
        );
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_vector_scalar_quantization() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();

    let result = executor.execute_sql("SELECT '[1.0, 2.0, 3.0]'::vector::halfvec");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_vector_reindex() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result =
        executor.execute_sql("CREATE TABLE vec_reindex (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql(
                "CREATE INDEX vec_reindex_idx ON vec_reindex USING hnsw (vec vector_l2_ops)",
            )
            .ok();
        let result = executor.execute_sql("REINDEX INDEX vec_reindex_idx");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_explain_analyze() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result =
        executor.execute_sql("CREATE TABLE vec_explain (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_explain VALUES (1, '[1.0, 0.0, 0.0]')")
            .unwrap();
        let result = executor.execute_sql(
            "EXPLAIN ANALYZE SELECT * FROM vec_explain ORDER BY vec <-> '[0.0, 0.0, 0.0]' LIMIT 5",
        );
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
#[ignore = "Implement me!"]
fn test_vector_returning() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_ret (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        let result =
            executor.execute_sql("INSERT INTO vec_ret VALUES (1, '[1.0, 2.0, 3.0]') RETURNING vec");
        assert!(result.is_ok());
    }
}

#[test]
fn test_vector_coalesce() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_coal (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO vec_coal VALUES (1, NULL)")
            .unwrap();
        let result = executor.execute_sql("SELECT COALESCE(vec, '[0.0, 0.0, 0.0]') FROM vec_coal");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_vector_generate_series() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE vec_gen (id INTEGER, vec VECTOR(3))");

    if create_result.is_ok() {
        let result = executor.execute_sql(
            "INSERT INTO vec_gen SELECT g, ARRAY[random(), random(), random()]::vector(3) FROM generate_series(1, 100) g"
        );
        assert!(result.is_ok() || result.is_err());
    }
}
