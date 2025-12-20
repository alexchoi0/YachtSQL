use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_geography_point() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT ST_ASTEXT(ST_GEOGPOINT(-122.4194, 37.7749))")
        .unwrap();
    assert_table_eq!(result, [["POINT(-122.4194 37.7749)"]]);
}

#[test]
fn test_geography_from_text() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT ST_ASTEXT(ST_GEOGFROMTEXT('POINT(-122.4194 37.7749)'))")
        .unwrap();
    assert_table_eq!(result, [["POINT(-122.4194 37.7749)"]]);
}

#[test]
fn test_geography_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE locations (id INT64, name STRING, location GEOGRAPHY)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO locations VALUES (1, 'San Francisco', ST_GEOGPOINT(-122.4194, 37.7749))",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT name FROM locations").unwrap();
    assert_table_eq!(result, [["San Francisco"]]);
}

#[test]
fn test_geography_line() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_ASTEXT(ST_GEOGFROMTEXT('LINESTRING(-122.4194 37.7749, -118.2437 34.0522)'))",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [["LINESTRING(-122.4194 37.7749, -118.2437 34.0522)"]]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_geography_polygon() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_ASTEXT(ST_GEOGFROMTEXT('POLYGON((-122.5 37.5, -122.5 38.0, -122.0 38.0, -122.0 37.5, -122.5 37.5))'))",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [["POLYGON((-122.5 37.5, -122.5 38, -122 38, -122 37.5, -122.5 37.5))"]]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_st_distance() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ROUND(ST_DISTANCE(
                ST_GEOGPOINT(-122.4194, 37.7749),
                ST_GEOGPOINT(-118.2437, 34.0522)
            ), 0)",
        )
        .unwrap();
    assert_table_eq!(result, [[559042.0]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_contains() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_CONTAINS(
                ST_GEOGFROMTEXT('POLYGON((-123 37, -123 38, -121 38, -121 37, -123 37))'),
                ST_GEOGPOINT(-122, 37.5)
            )",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_within() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_WITHIN(
                ST_GEOGPOINT(-122, 37.5),
                ST_GEOGFROMTEXT('POLYGON((-123 37, -123 38, -121 38, -121 37, -123 37))')
            )",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_intersects() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_INTERSECTS(
                ST_GEOGFROMTEXT('LINESTRING(-122 37, -121 38)'),
                ST_GEOGFROMTEXT('LINESTRING(-123 37.5, -120 37.5)')
            )",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_covers() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_COVERS(
                ST_GEOGFROMTEXT('POLYGON((-123 37, -123 38, -121 38, -121 37, -123 37))'),
                ST_GEOGPOINT(-122, 37.5)
            )",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_st_dwithin() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_DWITHIN(
                ST_GEOGPOINT(-122.4194, 37.7749),
                ST_GEOGPOINT(-122.4184, 37.7759),
                1000
            )",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_area() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ROUND(ST_AREA(ST_GEOGFROMTEXT('POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))')), 0)",
        )
        .unwrap();
    assert_table_eq!(result, [[9813924697.0]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_length() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT ROUND(ST_LENGTH(ST_GEOGFROMTEXT('LINESTRING(-122 37, -121 38)')), 0)")
        .unwrap();
    assert_table_eq!(result, [[141903.0]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_perimeter() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ROUND(ST_PERIMETER(ST_GEOGFROMTEXT('POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))')), 0)",
        )
        .unwrap();
    assert_table_eq!(result, [[398817.0]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_centroid() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_ASTEXT(ST_CENTROID(ST_GEOGFROMTEXT('POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))')))",
        )
        .unwrap();
    assert_table_eq!(result, [["POINT(-121.5 37.5)"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_buffer() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_BUFFER(ST_GEOGPOINT(-122.4194, 37.7749), 1000))")
        .unwrap();
    assert_table_eq!(result, [["Polygon"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_union() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_GEOMETRYTYPE(ST_UNION(
                ST_GEOGFROMTEXT('POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))'),
                ST_GEOGFROMTEXT('POLYGON((-121.5 37.5, -121.5 38.5, -120.5 38.5, -120.5 37.5, -121.5 37.5))')
            ))",
        )
        .unwrap();
    assert_table_eq!(result, [["MultiPolygon"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_intersection() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_GEOMETRYTYPE(ST_INTERSECTION(
                ST_GEOGFROMTEXT('POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))'),
                ST_GEOGFROMTEXT('POLYGON((-121.5 37.5, -121.5 38.5, -120.5 38.5, -120.5 37.5, -121.5 37.5))')
            ))",
        )
        .unwrap();
    assert_table_eq!(result, [["MultiPolygon"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_difference() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_GEOMETRYTYPE(ST_DIFFERENCE(
                ST_GEOGFROMTEXT('POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))'),
                ST_GEOGFROMTEXT('POLYGON((-121.5 37.5, -121.5 38.5, -120.5 38.5, -120.5 37.5, -121.5 37.5))')
            ))",
        )
        .unwrap();
    assert_table_eq!(result, [["MultiPolygon"]]);
}

#[test]
fn test_st_x() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT ST_X(ST_GEOGPOINT(-122.4194, 37.7749))")
        .unwrap();
    assert_table_eq!(result, [[-122.4194]]);
}

#[test]
fn test_st_y() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT ST_Y(ST_GEOGPOINT(-122.4194, 37.7749))")
        .unwrap();
    assert_table_eq!(result, [[37.7749]]);
}

#[test]
fn test_st_astext() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT ST_ASTEXT(ST_GEOGPOINT(-122.4194, 37.7749))")
        .unwrap();
    assert_table_eq!(result, [["POINT(-122.4194 37.7749)"]]);
}

#[test]
fn test_st_asgeojson() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT ST_ASGEOJSON(ST_GEOGPOINT(-122.4194, 37.7749))")
        .unwrap();
    assert_table_eq!(
        result,
        [["{\"type\":\"Point\",\"coordinates\":[-122.4194,37.7749]}"]]
    );
}

#[test]
fn test_st_asbinary() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT LENGTH(ST_ASBINARY(ST_GEOGPOINT(-122.4194, 37.7749)))")
        .unwrap();
    assert_table_eq!(result, [[24]]);
}

#[test]
fn test_st_geogfromgeojson() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_ASTEXT(ST_GEOGFROMGEOJSON('{\"type\": \"Point\", \"coordinates\": [-122.4194, 37.7749]}'))",
        )
        .unwrap();
    assert_table_eq!(result, [["POINT(-122.4194 37.7749)"]]);
}

#[test]
fn test_st_makeline() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_ASTEXT(ST_MAKELINE(ST_GEOGPOINT(-122, 37), ST_GEOGPOINT(-121, 38)))",
        )
        .unwrap();
    assert_table_eq!(result, [["LINESTRING(-122 37, -121 38)"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_makepolygon() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_ASTEXT(ST_MAKEPOLYGON(ST_GEOGFROMTEXT('LINESTRING(-122 37, -122 38, -121 38, -121 37, -122 37)')))",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [["POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))"]]
    );
}

#[test]
fn test_st_numpoints() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_NUMPOINTS(ST_GEOGFROMTEXT('LINESTRING(-122 37, -121 38, -120 37)'))",
        )
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_st_dimension() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT ST_DIMENSION(ST_GEOGPOINT(-122, 37))")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_st_dimension_polygon() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_DIMENSION(ST_GEOGFROMTEXT('POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))'))",
        )
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_st_iscollection() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT ST_ISCOLLECTION(ST_GEOGPOINT(-122, 37))")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_st_isempty() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT ST_ISEMPTY(ST_GEOGPOINT(-122, 37))")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_geography_in_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cities (id INT64, name STRING, location GEOGRAPHY)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE regions (id INT64, name STRING, boundary GEOGRAPHY)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO cities VALUES (1, 'San Francisco', ST_GEOGPOINT(-122.4194, 37.7749))",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO regions VALUES (1, 'Bay Area', ST_GEOGFROMTEXT('POLYGON((-123 37, -123 38, -121 38, -121 37, -123 37))'))",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT c.name, r.name AS region
            FROM cities c
            JOIN regions r ON ST_CONTAINS(r.boundary, c.location)",
        )
        .unwrap();
    assert_table_eq!(result, [["San Francisco", "Bay Area"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_snaptogrid() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT ST_ASTEXT(ST_SNAPTOGRID(ST_GEOGPOINT(-122.4194, 37.7749), 0.01))")
        .unwrap();
    assert_table_eq!(result, [["POINT(-122.42 37.77)"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_simplify() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_GEOMETRYTYPE(ST_SIMPLIFY(ST_GEOGFROMTEXT('LINESTRING(-122 37, -122.1 37.1, -122 37.2, -121.9 37.1, -122 37)'), 1000))",
        )
        .unwrap();
    assert_table_eq!(result, [["LineString"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_convexhull() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_GEOMETRYTYPE(ST_CONVEXHULL(ST_GEOGFROMTEXT('MULTIPOINT(-122 37, -121 38, -120 37, -121 36)')))",
        )
        .unwrap();
    assert_table_eq!(result, [["Polygon"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_closestpoint() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_GEOMETRYTYPE(ST_CLOSESTPOINT(
                ST_GEOGFROMTEXT('LINESTRING(-122 37, -121 38)'),
                ST_GEOGPOINT(-121.5, 37.2)
            ))",
        )
        .unwrap();
    assert_table_eq!(result, [["Point"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_st_boundingbox() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT ST_ASTEXT(ST_BOUNDINGBOX(ST_GEOGFROMTEXT('POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))')))",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [["POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))"]]
    );
}
