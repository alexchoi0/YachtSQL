use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_point_create() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE point_test (id INT64, location Point)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO point_test VALUES (1, (10.5, 20.3))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, location FROM point_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_ring_create() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ring_test (id INT64, boundary Ring)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO ring_test VALUES (1, [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM ring_test").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_polygon_create() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE polygon_test (id INT64, area Polygon)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO polygon_test VALUES (1, [[(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]]",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM polygon_test").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_multipolygon_create() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE multipolygon_test (id INT64, areas MultiPolygon)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO multipolygon_test VALUES (1, [[[(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]]]")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM multipolygon_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_point_distance() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE point_dist (p1 Point, p2 Point)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO point_dist VALUES ((0, 0), (3, 4))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT L2Distance(p1, p2) FROM point_dist")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_polygon_area() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE polygon_area (p Polygon)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO polygon_area VALUES ([[(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]]",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT polygonAreaCartesian(p) FROM polygon_area")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_point_in_polygon() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pip_test (pt Point, poly Polygon)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO pip_test VALUES ((5, 5), [[(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]]",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT pointInPolygon(pt, poly) FROM pip_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_geo_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE geo_null (id INT64, location Nullable(Point))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO geo_null VALUES (1, (10, 20)), (2, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM geo_null WHERE location IS NULL")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_polygon_perimeter() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE polygon_perim (p Polygon)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO polygon_perim VALUES ([[(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]]",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT polygonPerimeterCartesian(p) FROM polygon_perim")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_polygon_convex_hull() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE polygon_hull (p Polygon)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO polygon_hull VALUES ([[(0, 0), (10, 0), (5, 5), (10, 10), (0, 10), (0, 0)]]")
        .unwrap();

    let result = executor
        .execute_sql("SELECT polygonConvexHullCartesian(p) FROM polygon_hull")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_polygons_intersection() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE poly_intersect (p1 Polygon, p2 Polygon)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO poly_intersect VALUES ([[(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]], [[(5, 5), (15, 5), (15, 15), (5, 15), (5, 5)]]")
        .unwrap();

    let result = executor
        .execute_sql("SELECT polygonsIntersectionCartesian(p1, p2) FROM poly_intersect")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_polygons_union() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE poly_union (p1 Polygon, p2 Polygon)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO poly_union VALUES ([[(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]], [[(5, 5), (15, 5), (15, 15), (5, 15), (5, 5)]]")
        .unwrap();

    let result = executor
        .execute_sql("SELECT polygonsUnionCartesian(p1, p2) FROM poly_union")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
