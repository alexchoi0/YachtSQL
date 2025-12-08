use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_st_geogpoint() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_ASTEXT(ST_GEOGPOINT(-122.4194, 37.7749))")
        .unwrap();
    assert_table_eq!(result, [["POINT(-122.4194 37.7749)"]]);
}

#[test]
fn test_st_makeline() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT ST_ASTEXT(ST_MAKELINE(ST_GEOGPOINT(-122.4194, 37.7749), ST_GEOGPOINT(-73.9352, 40.7128)))",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [["LINESTRING(-122.4194 37.7749, -73.9352 40.7128)"]]
    );
}

#[test]
fn test_st_makepolygon() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_ASTEXT(ST_MAKEPOLYGON(ST_MAKELINE([ST_GEOGPOINT(0, 0), ST_GEOGPOINT(1, 0), ST_GEOGPOINT(1, 1), ST_GEOGPOINT(0, 1), ST_GEOGPOINT(0, 0)])))")
        .unwrap();
    assert_table_eq!(result, [["POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"]]);
}

#[test]
fn test_st_geogfromtext() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_ASTEXT(ST_GEOGFROMTEXT('POINT(-122.4194 37.7749)'))")
        .unwrap();
    assert_table_eq!(result, [["POINT(-122.4194 37.7749)"]]);
}

#[test]
fn test_st_geogfromgeojson() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_ASTEXT(ST_GEOGFROMGEOJSON('{\"type\": \"Point\", \"coordinates\": [-122.4194, 37.7749]}'))")
        .unwrap();
    assert_table_eq!(result, [["POINT(-122.4194 37.7749)"]]);
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
fn test_st_distance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT ROUND(ST_DISTANCE(ST_GEOGPOINT(-122.4194, 37.7749), ST_GEOGPOINT(-73.9352, 40.7128)), 0)",
        )
        .unwrap();
    assert_table_eq!(result, [[4134936.0]]);
}

#[test]
fn test_st_length() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT ROUND(ST_LENGTH(ST_MAKELINE(ST_GEOGPOINT(0, 0), ST_GEOGPOINT(1, 0))), 0)",
        )
        .unwrap();
    assert_table_eq!(result, [[111195.0]]);
}

#[test]
fn test_st_area() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT ROUND(ST_AREA(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')), 0)",
        )
        .unwrap();
    assert_table_eq!(result, [[12363718145.0]]);
}

#[test]
fn test_st_perimeter() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT ROUND(ST_PERIMETER(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')), 0)",
        )
        .unwrap();
    assert_table_eq!(result, [[444763.0]]);
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
fn test_st_centroid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT ST_ASTEXT(ST_CENTROID(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')))",
        )
        .unwrap();
    assert_table_eq!(result, [["POINT(0.4 0.4)"]]);
}

#[test]
fn test_st_contains() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_CONTAINS(ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_GEOGPOINT(1, 1))")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_st_intersects() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_INTERSECTS(ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_GEOGPOINT(1, 1))")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_st_within() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_WITHIN(ST_GEOGPOINT(1, 1), ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_st_dwithin() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_DWITHIN(ST_GEOGPOINT(0, 0), ST_GEOGPOINT(0.001, 0), 1000)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_st_touches() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_TOUCHES(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GEOGFROMTEXT('POLYGON((1 0, 2 0, 2 1, 1 1, 1 0))'))")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_st_boundary() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_BOUNDARY(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')))")
        .unwrap();
    assert_table_eq!(result, [["LineString"]]);
}

#[test]
fn test_st_buffer() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_BUFFER(ST_GEOGPOINT(0, 0), 1000))")
        .unwrap();
    assert_table_eq!(result, [["Polygon"]]);
}

#[test]
fn test_st_bufferwithtolerance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_BUFFERWITHTOLERANCE(ST_GEOGPOINT(0, 0), 1000, 10))")
        .unwrap();
    assert_table_eq!(result, [["Polygon"]]);
}

#[test]
fn test_st_union() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_UNION(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GEOGFROMTEXT('POLYGON((0.5 0, 1.5 0, 1.5 1, 0.5 1, 0.5 0))')))")
        .unwrap();
    assert_table_eq!(result, [["Polygon"]]);
}

#[test]
fn test_st_intersection() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_INTERSECTION(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GEOGFROMTEXT('POLYGON((0.5 0, 1.5 0, 1.5 1, 0.5 1, 0.5 0))')))")
        .unwrap();
    assert_table_eq!(result, [["Polygon"]]);
}

#[test]
fn test_st_difference() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_DIFFERENCE(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GEOGFROMTEXT('POLYGON((0.5 0, 1.5 0, 1.5 1, 0.5 1, 0.5 0))')))")
        .unwrap();
    assert_table_eq!(result, [["Polygon"]]);
}

#[test]
fn test_st_simplify() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT ST_GEOMETRYTYPE(ST_SIMPLIFY(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 0.1, 2 0, 3 0.1, 4 0)'), 100))",
        )
        .unwrap();
    assert_table_eq!(result, [["LineString"]]);
}

#[test]
fn test_st_snaptogrid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT ST_ASTEXT(ST_SNAPTOGRID(ST_GEOGPOINT(-122.4194567, 37.7749123), 0.0001))",
        )
        .unwrap();
    assert_table_eq!(result, [["POINT(-122.4195 37.7749)"]]);
}

#[test]
fn test_st_numpoints() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_NUMPOINTS(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1, 2 0)'))")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_st_pointn() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_ASTEXT(ST_POINTN(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1, 2 0)'), 2))")
        .unwrap();
    assert_table_eq!(result, [["POINT(1 1)"]]);
}

#[test]
fn test_st_startpoint() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT ST_ASTEXT(ST_STARTPOINT(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1, 2 0)')))",
        )
        .unwrap();
    assert_table_eq!(result, [["POINT(0 0)"]]);
}

#[test]
fn test_st_endpoint() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_ASTEXT(ST_ENDPOINT(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1, 2 0)')))")
        .unwrap();
    assert_table_eq!(result, [["POINT(2 0)"]]);
}

#[test]
fn test_st_isclosed() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_ISCLOSED(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1, 0 0)'))")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_st_isempty() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_ISEMPTY(ST_GEOGFROMTEXT('GEOMETRYCOLLECTION EMPTY'))")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_st_dimension() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_DIMENSION(ST_GEOGPOINT(0, 0))")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_st_geometrytype() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_GEOGPOINT(0, 0))")
        .unwrap();
    assert_table_eq!(result, [["Point"]]);
}

#[test]
fn test_st_maxdistance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ROUND(ST_MAXDISTANCE(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GEOGPOINT(2, 2)), 0)")
        .unwrap();
    assert_table_eq!(result, [[314475.0]]);
}

#[test]
fn test_st_closestpoint() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_CLOSESTPOINT(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1, 2 0)'), ST_GEOGPOINT(1, 0)))")
        .unwrap();
    assert_table_eq!(result, [["Point"]]);
}

#[test]
fn test_st_convexhull() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT ST_GEOMETRYTYPE(ST_CONVEXHULL(ST_GEOGFROMTEXT('MULTIPOINT(0 0, 1 0, 0.5 1, 0.5 0.5)')))",
        )
        .unwrap();
    assert_table_eq!(result, [["Polygon"]]);
}

#[test]
fn test_geography_in_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE locations (id INT64, name STRING, location GEOGRAPHY)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO locations VALUES (1, 'San Francisco', ST_GEOGPOINT(-122.4194, 37.7749)), (2, 'New York', ST_GEOGPOINT(-73.9352, 40.7128))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM locations WHERE ST_DWITHIN(location, ST_GEOGPOINT(-122.4, 37.8), 50000) ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["San Francisco"]]);
}

#[test]
fn test_st_geohash() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SUBSTR(ST_GEOHASH(ST_GEOGPOINT(-122.4194, 37.7749)), 1, 6)")
        .unwrap();
    assert_table_eq!(result, [["9q8yyk"]]);
}

#[test]
fn test_st_geogpointfromgeohash() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_GEOGPOINTFROMGEOHASH('9q8yyk'))")
        .unwrap();
    assert_table_eq!(result, [["Point"]]);
}
