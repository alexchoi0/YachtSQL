use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_great_circle_distance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT greatCircleDistance(55.755831, 37.617673, 40.712776, -74.005974)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_geo_distance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT geoDistance(55.755831, 37.617673, 40.712776, -74.005974)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_point_in_ellipses() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT pointInEllipses(0, 0, 0, 0, 10, 10)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_point_in_polygon() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT pointInPolygon((3, 3), [(0, 0), (10, 0), (10, 10), (0, 10)])")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_geohash_encode() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT geohashEncode(55.755831, 37.617673, 12)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_geohash_decode() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT geohashDecode('ucfv0j341n')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_geohashes_in_box() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT geohashesInBox(55.7, 37.5, 55.8, 37.7, 4)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_is_valid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT h3IsValid(617700169983721471)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_get_resolution() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT h3GetResolution(617700169983721471)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_edge_length_m() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT h3EdgeLengthM(1)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_edge_angle() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT h3EdgeAngle(1)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_hex_area_km2() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT h3HexAreaKm2(5)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_to_geo() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT h3ToGeo(617700169983721471)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_to_geo_boundary() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT h3ToGeoBoundary(617700169983721471)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_geo_to_h3() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT geoToH3(55.755831, 37.617673, 15)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_k_ring() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT h3kRing(617700169983721471, 1)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_get_base_cell() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT h3GetBaseCell(617700169983721471)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_is_pentagon() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT h3IsPentagon(617700169983721471)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_is_res_class_iii() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT h3IsResClassIII(617700169983721471)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_get_faces() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT h3GetFaces(617700169983721471)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_cell_area_m2() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT h3CellAreaM2(617700169983721471)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_cell_area_rads2() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT h3CellAreaRads2(617700169983721471)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_to_parent() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT h3ToParent(617700169983721471, 1)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_to_children() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT h3ToChildren(599686042433355775, 6)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_distance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT h3Distance(617700169983721471, 617700169983721472)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_h3_line() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT h3Line(617700169983721471, 617700169983721472)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_s2_cell_id_to_long_lat() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT S2CellIdToLongLat(5765691994461192192)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_long_lat_to_s2_cell_id() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT longLatToS2CellId(55.755831, 37.617673, 15)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_geo_in_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE locations (id INT64, name STRING, lat FLOAT64, lon FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO locations VALUES (1, 'Moscow', 55.755831, 37.617673), (2, 'NYC', 40.712776, -74.005974)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name, geoDistance(lat, lon, 55.755831, 37.617673) AS dist FROM locations ORDER BY dist")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}
