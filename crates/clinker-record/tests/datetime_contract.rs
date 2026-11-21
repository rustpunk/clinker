//! Frozen date/time payloads from the pre-migration dependency. These fixtures
//! pin the public date range, UTC normalization, and spill bytes independently
//! of a serializer/deserializer round-trip using the same implementation.

use clinker_record::{Value, coerce_to_date, coerce_to_datetime};

fn assert_wire(value: Value, json: &str, bytes: &[u8]) {
    assert_eq!(serde_json::to_string(&value).unwrap(), json);
    assert_eq!(postcard::to_stdvec(&value).unwrap(), bytes);
    assert_eq!(postcard::from_bytes::<Value>(bytes).unwrap(), value);
    assert_eq!(serde_json::from_str::<Value>(json).unwrap(), value);
}

#[test]
fn date_spill_bytes_preserve_the_full_calendar_range() {
    let cases: &[(&str, &str, &[u8])] = &[
        (
            "-262143-01-01",
            r#"{"Date":-95746129}"#,
            &[5, 161, 225, 167, 91],
        ),
        ("0001-01-01", r#"{"Date":1}"#, &[5, 2]),
        ("1970-01-01", r#"{"Date":719163}"#, &[5, 246, 228, 87]),
        (
            "+262142-12-31",
            r#"{"Date":95745399}"#,
            &[5, 238, 213, 167, 91],
        ),
    ];
    for &(input, json, bytes) in cases {
        let value = coerce_to_date(&Value::String(input.into()), &["%Y-%m-%d"]).unwrap();
        assert_wire(value, json, bytes);
    }
}

#[test]
fn datetime_spill_bytes_preserve_offsets_extreme_eras_and_nanoseconds() {
    let cases: &[(&str, &str, &[u8])] = &[
        (
            "1969-12-31T23:59:59.999999999Z",
            r#"{"DateTime":-1}"#,
            &[6, 1],
        ),
        (
            "2026-01-31T08:27:00+05:30",
            r#"{"DateTime":1769828220000000000}"#,
            &[6, 128, 224, 254, 232, 243, 216, 216, 143, 49],
        ),
    ];
    for &(input, json, bytes) in cases {
        let value = coerce_to_datetime(&Value::String(input.into()), &[]).unwrap();
        assert_wire(value, json, bytes);
    }

    let explicit: &[(&str, &str, &[u8])] = &[
        (
            "1600-01-01T00:00:00.123456789",
            r#"{"DateTime":-11676095999876543211}"#,
            &[6, 213, 203, 129, 192, 224, 220, 233, 137, 196, 2],
        ),
        (
            "+200000-07-04T06:30:15.5",
            r#"{"DateTime":6249239188215500000000}"#,
            &[6, 128, 172, 210, 181, 230, 242, 197, 157, 139, 203, 10],
        ),
        (
            "-50000-03-21T09:15:00.25",
            r#"{"DateTime":-1640007873899750000000}"#,
            &[6, 255, 149, 196, 227, 175, 139, 180, 174, 207, 227, 2],
        ),
    ];
    for &(input, json, bytes) in explicit {
        let value =
            coerce_to_datetime(&Value::String(input.into()), &["%Y-%m-%dT%H:%M:%S%.f"]).unwrap();
        assert_wire(value, json, bytes);
    }
}
