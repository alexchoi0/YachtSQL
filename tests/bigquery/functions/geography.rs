use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_st_geogpoint() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_ASTEXT(ST_GEOGPOINT(-122.4194, 37.7749))")
        .await
        .unwrap();
    assert_table_eq!(result, [["POINT(-122.4194 37.7749)"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_makeline() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT ST_ASTEXT(ST_MAKELINE(ST_GEOGPOINT(-122.4194, 37.7749), ST_GEOGPOINT(-73.9352, 40.7128)))",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [["LINESTRING(-122.4194 37.7749, -73.9352 40.7128)"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_makepolygon() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_ASTEXT(ST_MAKEPOLYGON(ST_MAKELINE([ST_GEOGPOINT(0, 0), ST_GEOGPOINT(1, 0), ST_GEOGPOINT(1, 1), ST_GEOGPOINT(0, 1), ST_GEOGPOINT(0, 0)])))").await
        .unwrap();
    assert_table_eq!(result, [["POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_geogfromtext() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_ASTEXT(ST_GEOGFROMTEXT('POINT(-122.4194 37.7749)'))")
        .await
        .unwrap();
    assert_table_eq!(result, [["POINT(-122.4194 37.7749)"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_geogfromgeojson() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_ASTEXT(ST_GEOGFROMGEOJSON('{\"type\": \"Point\", \"coordinates\": [-122.4194, 37.7749]}'))").await
        .unwrap();
    assert_table_eq!(result, [["POINT(-122.4194 37.7749)"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_astext() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_ASTEXT(ST_GEOGPOINT(-122.4194, 37.7749))")
        .await
        .unwrap();
    assert_table_eq!(result, [["POINT(-122.4194 37.7749)"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_asgeojson() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_ASGEOJSON(ST_GEOGPOINT(-122.4194, 37.7749))")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [["{\"type\":\"Point\",\"coordinates\":[-122.4194,37.7749]}"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_distance() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT ROUND(ST_DISTANCE(ST_GEOGPOINT(-122.4194, 37.7749), ST_GEOGPOINT(-73.9352, 40.7128)), 0)",
        ).await
        .unwrap();
    assert_table_eq!(result, [[4145004.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_length() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT ROUND(ST_LENGTH(ST_MAKELINE(ST_GEOGPOINT(0, 0), ST_GEOGPOINT(1, 0))), 0)",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[111319.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_area() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT ROUND(ST_AREA(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')), 0)",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[12308778361.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_perimeter() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT ROUND(ST_PERIMETER(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')), 0)",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[443771.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_x() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_X(ST_GEOGPOINT(-122.4194, 37.7749))")
        .await
        .unwrap();
    assert_table_eq!(result, [[-122.4194]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_y() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_Y(ST_GEOGPOINT(-122.4194, 37.7749))")
        .await
        .unwrap();
    assert_table_eq!(result, [[37.7749]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_centroid() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT ST_ASTEXT(ST_CENTROID(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')))",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["POINT(0.5 0.5)"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_contains() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_CONTAINS(ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_GEOGPOINT(1, 1))").await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_intersects() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_INTERSECTS(ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_GEOGPOINT(1, 1))").await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_within() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_WITHIN(ST_GEOGPOINT(1, 1), ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))").await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_dwithin() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_DWITHIN(ST_GEOGPOINT(0, 0), ST_GEOGPOINT(0.001, 0), 1000)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_touches() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_TOUCHES(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GEOGFROMTEXT('POLYGON((1 0, 2 0, 2 1, 1 1, 1 0))'))").await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_boundary() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_BOUNDARY(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')))").await
        .unwrap();
    assert_table_eq!(result, [["LineString"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_buffer() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_BUFFER(ST_GEOGPOINT(0, 0), 1000))")
        .await
        .unwrap();
    assert_table_eq!(result, [["Polygon"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_bufferwithtolerance() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_BUFFERWITHTOLERANCE(ST_GEOGPOINT(0, 0), 1000, 10))")
        .await
        .unwrap();
    assert_table_eq!(result, [["Polygon"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_union() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_UNION(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GEOGFROMTEXT('POLYGON((0.5 0, 1.5 0, 1.5 1, 0.5 1, 0.5 0))')))").await
        .unwrap();
    assert_table_eq!(result, [["MultiPolygon"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_intersection() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_INTERSECTION(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GEOGFROMTEXT('POLYGON((0.5 0, 1.5 0, 1.5 1, 0.5 1, 0.5 0))')))").await
        .unwrap();
    assert_table_eq!(result, [["MultiPolygon"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_difference() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_DIFFERENCE(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GEOGFROMTEXT('POLYGON((0.5 0, 1.5 0, 1.5 1, 0.5 1, 0.5 0))')))").await
        .unwrap();
    assert_table_eq!(result, [["MultiPolygon"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_simplify() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT ST_GEOMETRYTYPE(ST_SIMPLIFY(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 0.1, 2 0, 3 0.1, 4 0)'), 100))",
        ).await
        .unwrap();
    assert_table_eq!(result, [["LineString"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_snaptogrid() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT ST_ASTEXT(ST_SNAPTOGRID(ST_GEOGPOINT(-122.4194567, 37.7749123), 0.0001))",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["POINT(-122.4195 37.7749)"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_numpoints() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_NUMPOINTS(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1, 2 0)'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_pointn() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_ASTEXT(ST_POINTN(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1, 2 0)'), 2))")
        .await
        .unwrap();
    assert_table_eq!(result, [["POINT(1 1)"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_startpoint() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT ST_ASTEXT(ST_STARTPOINT(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1, 2 0)')))",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["POINT(0 0)"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_endpoint() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_ASTEXT(ST_ENDPOINT(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1, 2 0)')))")
        .await
        .unwrap();
    assert_table_eq!(result, [["POINT(2 0)"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_isclosed() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_ISCLOSED(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1, 0 0)'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_isempty() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_ISEMPTY(ST_GEOGFROMTEXT('GEOMETRYCOLLECTION EMPTY'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_dimension() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_DIMENSION(ST_GEOGPOINT(0, 0))")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_geometrytype() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_GEOGPOINT(0, 0))")
        .await
        .unwrap();
    assert_table_eq!(result, [["Point"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_maxdistance() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(ST_MAXDISTANCE(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GEOGPOINT(2, 2)), 0)").await
        .unwrap();
    assert_table_eq!(result, [[313776.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_closestpoint() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_CLOSESTPOINT(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1, 2 0)'), ST_GEOGPOINT(1, 0)))").await
        .unwrap();
    assert_table_eq!(result, [["Point"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_convexhull() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT ST_GEOMETRYTYPE(ST_CONVEXHULL(ST_GEOGFROMTEXT('MULTIPOINT(0 0, 1 0, 0.5 1, 0.5 0.5)')))",
        ).await
        .unwrap();
    assert_table_eq!(result, [["Polygon"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_geography_in_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE locations (id INT64, name STRING, location GEOGRAPHY)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO locations VALUES (1, 'San Francisco', ST_GEOGPOINT(-122.4194, 37.7749)), (2, 'New York', ST_GEOGPOINT(-73.9352, 40.7128))").await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM locations WHERE ST_DWITHIN(location, ST_GEOGPOINT(-122.4, 37.8), 50000) ORDER BY name").await
        .unwrap();
    assert_table_eq!(result, [["San Francisco"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_geohash() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTR(ST_GEOHASH(ST_GEOGPOINT(-122.4194, 37.7749)), 1, 6)")
        .await
        .unwrap();
    assert_table_eq!(result, [["9q8yyk"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_st_geogpointfromgeohash() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_GEOMETRYTYPE(ST_GEOGPOINTFROMGEOHASH('9q8yyk'))")
        .await
        .unwrap();
    assert_table_eq!(result, [["Point"]]);
}
