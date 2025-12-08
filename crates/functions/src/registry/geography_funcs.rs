use std::rc::Rc;

use yachtsql_core::error::Error;
use yachtsql_core::types::{DataType, Value};

use super::FunctionRegistry;
use crate::geography;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ST_GeogFromText".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_GeogFromText".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Geography,
            variadic: false,
            evaluator: |args| {
                if let Some(wkt) = args[0].as_str() {
                    let geom = geography::parse_wkt(wkt)?;

                    Ok(Value::geography(geom.to_wkt()))
                } else if args[0].is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ST_GeogPoint".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_GeogPoint".to_string(),
            arg_types: vec![DataType::Float64, DataType::Float64],
            return_type: DataType::Geography,
            variadic: false,
            evaluator: |args| {
                let lon = if let Some(f) = args[0].as_f64() {
                    f
                } else if let Some(i) = args[0].as_i64() {
                    i as f64
                } else if args[0].is_null() {
                    return Ok(Value::null());
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "FLOAT64".to_string(),
                        actual: args[0].data_type().to_string(),
                    });
                };

                let lat = if let Some(f) = args[1].as_f64() {
                    f
                } else if let Some(i) = args[1].as_i64() {
                    i as f64
                } else if args[1].is_null() {
                    return Ok(Value::null());
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "FLOAT64".to_string(),
                        actual: args[1].data_type().to_string(),
                    });
                };

                let geom = geography::Geometry::Point { lon, lat };
                geom.validate()?;

                Ok(Value::geography(geom.to_wkt()))
            },
        }),
    );

    registry.register_scalar(
        "ST_AsText".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_AsText".to_string(),
            arg_types: vec![DataType::Geography],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if let Some(wkt) = args[0].as_geography() {
                    Ok(Value::string(wkt.to_string()))
                } else if args[0].is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ST_X".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_X".to_string(),
            arg_types: vec![DataType::Geography],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if let Some(wkt) = args[0].as_geography() {
                    let geom = geography::parse_wkt(wkt)?;
                    match geom {
                        geography::Geometry::Point { lon, .. } => Ok(Value::float64(lon)),
                        _ => Err(Error::invalid_query(
                            "ST_X requires a POINT geometry".to_string(),
                        )),
                    }
                } else if args[0].is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ST_Y".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_Y".to_string(),
            arg_types: vec![DataType::Geography],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if let Some(wkt) = args[0].as_geography() {
                    let geom = geography::parse_wkt(wkt)?;
                    match geom {
                        geography::Geometry::Point { lat, .. } => Ok(Value::float64(lat)),
                        _ => Err(Error::invalid_query(
                            "ST_Y requires a POINT geometry".to_string(),
                        )),
                    }
                } else if args[0].is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ST_GeometryType".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_GeometryType".to_string(),
            arg_types: vec![DataType::Geography],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if let Some(wkt) = args[0].as_geography() {
                    let geom = geography::parse_wkt(wkt)?;
                    let type_name = match geom {
                        geography::Geometry::Point { .. } => "ST_Point",
                        geography::Geometry::MultiPoint { .. } => "ST_MultiPoint",
                        geography::Geometry::LineString { .. } => "ST_LineString",
                        geography::Geometry::Polygon { .. } => "ST_Polygon",
                        geography::Geometry::Empty => "ST_GeomCollection",
                    };
                    Ok(Value::string(type_name.to_string()))
                } else if args[0].is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ST_IsEmpty".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_IsEmpty".to_string(),
            arg_types: vec![DataType::Geography],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if let Some(wkt) = args[0].as_geography() {
                    let geom = geography::parse_wkt(wkt)?;
                    Ok(Value::bool_val(matches!(geom, geography::Geometry::Empty)))
                } else if args[0].is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ST_GeogFromGeoJSON".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_GeogFromGeoJSON".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Geography,
            variadic: false,
            evaluator: |args| {
                if let Some(geojson) = args[0].as_str() {
                    let geom = geography::parse_geojson(geojson)?;

                    Ok(Value::geography(geom.to_wkt()))
                } else if args[0].is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ST_AsGeoJSON".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_AsGeoJSON".to_string(),
            arg_types: vec![DataType::Geography],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if let Some(wkt) = args[0].as_geography() {
                    let geom = geography::parse_wkt(wkt)?;
                    Ok(Value::string(geom.to_geojson()))
                } else if args[0].is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ST_Distance".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_Distance".to_string(),
            arg_types: vec![DataType::Geography, DataType::Geography],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                if let (Some(wkt1), Some(wkt2)) = (args[0].as_geography(), args[1].as_geography()) {
                    let geom1 = geography::parse_wkt(wkt1)?;
                    let geom2 = geography::parse_wkt(wkt2)?;
                    let distance = geom1.distance(&geom2)?;
                    Ok(Value::float64(distance))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: format!("{:?}, {:?}", args[0].data_type(), args[1].data_type()),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ST_Length".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_Length".to_string(),
            arg_types: vec![DataType::Geography],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if let Some(wkt) = args[0].as_geography() {
                    let geom = geography::parse_wkt(wkt)?;
                    let length = geom.length()?;
                    Ok(Value::float64(length))
                } else if args[0].is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ST_Area".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_Area".to_string(),
            arg_types: vec![DataType::Geography],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if let Some(wkt) = args[0].as_geography() {
                    let geom = geography::parse_wkt(wkt)?;
                    let area = geom.area()?;
                    Ok(Value::float64(area))
                } else if args[0].is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ST_Perimeter".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_Perimeter".to_string(),
            arg_types: vec![DataType::Geography],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if let Some(wkt) = args[0].as_geography() {
                    let geom = geography::parse_wkt(wkt)?;
                    let perimeter = geom.perimeter()?;
                    Ok(Value::float64(perimeter))
                } else if args[0].is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ST_Contains".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_Contains".to_string(),
            arg_types: vec![DataType::Geography, DataType::Geography],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                if let (Some(wkt1), Some(wkt2)) = (args[0].as_geography(), args[1].as_geography()) {
                    let geom1 = geography::parse_wkt(wkt1)?;
                    let geom2 = geography::parse_wkt(wkt2)?;
                    let contains = geom1.contains(&geom2)?;
                    Ok(Value::bool_val(contains))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: format!("{:?}, {:?}", args[0].data_type(), args[1].data_type()),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ST_Intersects".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_Intersects".to_string(),
            arg_types: vec![DataType::Geography, DataType::Geography],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                if let (Some(wkt1), Some(wkt2)) = (args[0].as_geography(), args[1].as_geography()) {
                    let geom1 = geography::parse_wkt(wkt1)?;
                    let geom2 = geography::parse_wkt(wkt2)?;
                    let intersects = geom1.intersects(&geom2)?;
                    Ok(Value::bool_val(intersects))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: format!("{:?}, {:?}", args[0].data_type(), args[1].data_type()),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ST_Disjoint".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_Disjoint".to_string(),
            arg_types: vec![DataType::Geography, DataType::Geography],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                if let (Some(wkt1), Some(wkt2)) = (args[0].as_geography(), args[1].as_geography()) {
                    let geom1 = geography::parse_wkt(wkt1)?;
                    let geom2 = geography::parse_wkt(wkt2)?;
                    let disjoint = geom1.disjoint(&geom2)?;
                    Ok(Value::bool_val(disjoint))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: format!("{:?}, {:?}", args[0].data_type(), args[1].data_type()),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ST_Dimension".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_Dimension".to_string(),
            arg_types: vec![DataType::Geography],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if let Some(wkt) = args[0].as_geography() {
                    let geom = geography::parse_wkt(wkt)?;
                    Ok(Value::int64(geom.dimension() as i64))
                } else if args[0].is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ST_MakeLine".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_MakeLine".to_string(),
            arg_types: vec![DataType::Geography],
            return_type: DataType::Geography,
            variadic: true,
            evaluator: |args| {
                let mut points = Vec::new();
                for arg in args {
                    if let Some(wkt) = arg.as_geography() {
                        let geom = geography::parse_wkt(wkt)?;
                        match geom {
                            geography::Geometry::Point { lon, lat } => {
                                points.push((lon, lat));
                            }
                            _ => {
                                return Err(Error::invalid_query(
                                    "ST_MakeLine requires POINT geometries".to_string(),
                                ));
                            }
                        }
                    } else if arg.is_null() {
                        return Ok(Value::null());
                    } else {
                        return Err(Error::TypeMismatch {
                            expected: "GEOGRAPHY".to_string(),
                            actual: arg.data_type().to_string(),
                        });
                    }
                }
                let line = geography::make_line(points)?;
                Ok(Value::geography(line.to_wkt()))
            },
        }),
    );

    registry.register_scalar(
        "ST_MakePolygon".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ST_MakePolygon".to_string(),
            arg_types: vec![DataType::Geography],
            return_type: DataType::Geography,
            variadic: false,
            evaluator: |args| {
                if let Some(wkt) = args[0].as_geography() {
                    let geom = geography::parse_wkt(wkt)?;
                    let polygon = geography::make_polygon(&geom)?;
                    Ok(Value::geography(polygon.to_wkt()))
                } else if args[0].is_null() {
                    Ok(Value::null())
                } else {
                    Err(Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );
}
