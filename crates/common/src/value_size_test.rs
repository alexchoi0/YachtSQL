#[cfg(test)]
#[allow(clippy::approx_constant)]
mod value_size_tests {
    use std::mem;

    use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
    use indexmap::IndexMap;
    use rust_decimal::Decimal;
    use uuid::Uuid;

    use crate::types::Value;

    #[test]
    fn test_value_size() {
        let value_size = mem::size_of::<Value>();
        println!("\n=== Value Enum Size Analysis ===");
        println!("Total Value size: {} bytes", value_size);
        println!("Target: 16 bytes");
        println!(
            "Current overhead: {} bytes ({:.1}% waste)\n",
            value_size - 16,
            ((value_size - 16) as f64 / value_size as f64) * 100.0
        );

        println!("=== Individual Variant Analysis ===");

        let variants = vec![
            ("Null", Value::null()),
            ("Bool(true)", Value::bool_val(true)),
            ("Int64(42)", Value::int64(42)),
            ("Float64(3.14)", Value::float64(3.14)),
            ("Numeric(Decimal)", Value::numeric(Decimal::new(100, 2))),
            ("String(empty)", Value::string(String::new())),
            ("String(3 chars)", Value::string("abc".to_string())),
            (
                "String(15 chars)",
                Value::string("123456789012345".to_string()),
            ),
            ("String(100 chars)", Value::string("a".repeat(100))),
            ("Bytes(empty)", Value::bytes(vec![])),
            ("Bytes(10)", Value::bytes(vec![0; 10])),
            (
                "Date",
                Value::date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            ),
            (
                "Time",
                Value::time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
            ),
            ("Timestamp", Value::timestamp(Utc::now())),
            ("DateTime", Value::datetime(Utc::now())),
            ("Uuid", Value::uuid(Uuid::new_v4())),
            ("Array(empty)", Value::array(vec![])),
            (
                "Array(3 ints)",
                Value::array(vec![Value::int64(1), Value::int64(2), Value::int64(3)]),
            ),
            ("Json(null)", Value::json(serde_json::Value::Null)),
            ("Default", Value::default_value()),
        ];

        for (name, _value) in variants {
            println!("{:20} {} bytes (same for all variants)", name, value_size);
        }

        println!("\n=== Component Sizes ===");
        println!("String:              {} bytes", mem::size_of::<String>());
        println!("Vec<u8>:             {} bytes", mem::size_of::<Vec<u8>>());
        println!(
            "Vec<Value>:          {} bytes",
            mem::size_of::<Vec<Value>>()
        );
        println!(
            "IndexMap:            {} bytes",
            mem::size_of::<IndexMap<String, Value>>()
        );
        println!("Decimal:             {} bytes", mem::size_of::<Decimal>());
        println!("NaiveDate:           {} bytes", mem::size_of::<NaiveDate>());
        println!("NaiveTime:           {} bytes", mem::size_of::<NaiveTime>());
        println!(
            "DateTime<Utc>:       {} bytes",
            mem::size_of::<DateTime<Utc>>()
        );
        println!("Uuid:                {} bytes", mem::size_of::<Uuid>());
        println!(
            "serde_json::Value:   {} bytes",
            mem::size_of::<serde_json::Value>()
        );
        println!("bool:                {} bytes", mem::size_of::<bool>());
        println!("i64:                 {} bytes", mem::size_of::<i64>());
        println!("f64:                 {} bytes", mem::size_of::<f64>());

        println!("\n=== Optimization Opportunities ===");

        let int64_waste = value_size - mem::size_of::<i64>() - 1;
        let bool_waste = value_size - mem::size_of::<bool>() - 1;
        let null_waste = value_size - 1;

        println!(
            "Null waste:          {} bytes ({:.1}% of Value)",
            null_waste,
            (null_waste as f64 / value_size as f64) * 100.0
        );
        println!(
            "Bool waste:          {} bytes ({:.1}% of Value)",
            bool_waste,
            (bool_waste as f64 / value_size as f64) * 100.0
        );
        println!(
            "Int64 waste:         {} bytes ({:.1}% of Value)",
            int64_waste,
            (int64_waste as f64 / value_size as f64) * 100.0
        );

        println!("\n=== Typical Workload Analysis ===");

        let string_waste = value_size.saturating_sub(24);
        let avg_waste = 0.60 * int64_waste as f64
            + 0.20 * string_waste as f64
            + 0.10 * null_waste as f64
            + 0.10 * 0.0;

        println!("Average waste per value: {:.1} bytes", avg_waste);
        println!(
            "Percentage waste: {:.1}%",
            (avg_waste / value_size as f64) * 100.0
        );

        println!("\n=== 16-Byte Target Analysis ===");
        println!("Current: {} bytes", value_size);
        println!("Target:  16 bytes");
        println!(
            "Reduction needed: {} bytes ({:.1}%)",
            value_size - 16,
            ((value_size - 16) as f64 / value_size as f64) * 100.0
        );
    }

    #[test]
    fn test_proposed_small_value() {
        #[repr(C)]
        struct SmallValue {
            tag: u8,
            data: [u8; 15],
        }

        let small_size = mem::size_of::<SmallValue>();
        println!("\n=== Proposed SmallValue ===");
        println!("SmallValue size: {} bytes", small_size);
        println!("Target: 16 bytes");

        assert_eq!(small_size, 16, "SmallValue should be exactly 16 bytes");
    }

    #[test]
    fn test_box_pointer_size() {
        let box_size = mem::size_of::<Box<String>>();
        println!("\n=== Heap Pointer Sizes ===");
        println!("Box<String>: {} bytes", box_size);
        println!(
            "Box<Vec<Value>>: {} bytes",
            mem::size_of::<Box<Vec<Value>>>()
        );
        println!(
            "Rc<String>: {} bytes",
            mem::size_of::<std::rc::Rc<String>>()
        );

        assert_eq!(box_size, 8, "Box pointer should be 8 bytes on 64-bit");
    }
}
