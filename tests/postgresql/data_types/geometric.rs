use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_point_literal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT POINT(1.0, 2.0)").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_point_string_literal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT '(1,2)'::POINT").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_line_literal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LINE '{1,2,3}'").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_lseg_literal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LSEG '[(0,0),(1,1)]'").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_box_literal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT BOX '((0,0),(1,1))'").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_path_open() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT PATH '[(0,0),(1,1),(2,0)]'")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_path_closed() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT PATH '((0,0),(1,1),(2,0))'")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_polygon_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POLYGON '((0,0),(1,0),(1,1),(0,1))'")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_circle_literal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT CIRCLE '<(0,0),5>'").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_point_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE locations (id INTEGER, pos POINT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO locations VALUES (1, POINT(10, 20))")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM locations").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_point_distance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POINT(0, 0) <-> POINT(3, 4)")
        .unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_point_add() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POINT(1, 2) + POINT(3, 4)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_point_subtract() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POINT(3, 4) - POINT(1, 2)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_point_multiply() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POINT(2, 3) * POINT(4, 5)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_point_divide() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POINT(10, 20) / POINT(2, 2)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_box_intersects() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT BOX '((0,0),(2,2))' && BOX '((1,1),(3,3))'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_box_contains_point() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT BOX '((0,0),(2,2))' @> POINT(1, 1)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_point_contained_in_box() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POINT(1, 1) <@ BOX '((0,0),(2,2))'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_lseg_parallel() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LSEG '[(0,0),(1,1)]' ?|| LSEG '[(2,2),(3,3)]'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_lseg_perpendicular() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LSEG '[(0,0),(1,0)]' ?-| LSEG '[(0,0),(0,1)]'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_lseg_intersects() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LSEG '[(0,0),(2,2)]' ?# LSEG '[(0,2),(2,0)]'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_circle_contains_point() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CIRCLE '<(0,0),5>' @> POINT(3, 4)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_polygon_contains_point() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POLYGON '((0,0),(4,0),(4,4),(0,4))' @> POINT(2, 2)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_area_box() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT AREA(BOX '((0,0),(2,3))')")
        .unwrap();
    assert_table_eq!(result, [[6.0]]);
}

#[test]
fn test_area_circle() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT AREA(CIRCLE '<(0,0),1>')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_area_polygon() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT AREA(POLYGON '((0,0),(4,0),(4,3),(0,3))')")
        .unwrap();
    assert_table_eq!(result, [[12.0]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_center_box() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CENTER(BOX '((0,0),(4,4))')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_center_circle() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CENTER(CIRCLE '<(3,4),5>')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_diameter_circle() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DIAMETER(CIRCLE '<(0,0),5>')")
        .unwrap();
    assert_table_eq!(result, [[10.0]]);
}

#[test]
fn test_radius_circle() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT RADIUS(CIRCLE '<(0,0),5>')")
        .unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[test]
fn test_height_box() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT HEIGHT(BOX '((0,0),(3,5))')")
        .unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[test]
fn test_width_box() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT WIDTH(BOX '((0,0),(3,5))')")
        .unwrap();
    assert_table_eq!(result, [[3.0]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_length_lseg() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LENGTH(LSEG '[(0,0),(3,4)]')")
        .unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_length_path() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LENGTH(PATH '[(0,0),(3,4)]')")
        .unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_npoints_path() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NPOINTS(PATH '[(0,0),(1,1),(2,0)]')")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_npoints_polygon() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NPOINTS(POLYGON '((0,0),(1,0),(1,1),(0,1))')")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_isclosed_path() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ISCLOSED(PATH '((0,0),(1,1),(2,0))')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_isopen_path() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ISOPEN(PATH '[(0,0),(1,1),(2,0)]')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_popen() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POPEN(PATH '((0,0),(1,1),(2,0))')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_pclose() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT PCLOSE(PATH '[(0,0),(1,1),(2,0)]')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_box_to_circle() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CIRCLE(BOX '((0,0),(2,2))')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_box_to_polygon() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POLYGON(BOX '((0,0),(2,2))')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_circle_to_box() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT BOX(CIRCLE '<(0,0),1>')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_circle_to_polygon() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POLYGON(12, CIRCLE '<(0,0),1>')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_bound_box() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT BOUND_BOX(BOX '((0,0),(1,1))', BOX '((3,3),(4,4))')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_geometric_gist_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE geo_data (id INTEGER, loc POINT)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX geo_idx ON geo_data USING GIST (loc)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO geo_data VALUES (1, POINT(1, 2)), (2, POINT(3, 4))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM geo_data WHERE loc <@ BOX '((0,0),(5,5))' ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}
