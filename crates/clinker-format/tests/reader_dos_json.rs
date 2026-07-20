//! Pathological-JSON safety tests for the streaming JSON reader.
//!
//! These pin the reader's behavior on the malformed/adversarial inputs a
//! DoS-minded producer emits — deeply nested structures, truncated documents —
//! so a dependency-default change (serde_json's parse recursion limit) or a
//! regression in the reader's own flatten cap surfaces as a failing test rather
//! than a stack overflow or silent wrong result. Companion to the XML suite in
//! `reader_dos_xml.rs` and the YAML DoS suite in `clinker-plan`.

use std::io::Cursor;

use clinker_format::FormatReader;
use clinker_format::error::FormatError;
use clinker_format::json::reader::{JsonReader, JsonReaderConfig};
use clinker_record::Value;

/// A single-line JSON object nested `depth` levels deep under the key `k`:
/// `{"k":{"k":…{"k":1}…}}`. Read as one NDJSON line.
fn nested_object_line(depth: usize) -> String {
    let mut s = String::with_capacity(depth * 6 + 2);
    for _ in 0..depth {
        s.push_str("{\"k\":");
    }
    s.push('1');
    for _ in 0..depth {
        s.push('}');
    }
    s
}

fn reader(bytes: Vec<u8>) -> JsonReader {
    JsonReader::from_reader(Cursor::new(bytes), JsonReaderConfig::default())
        .expect("reader construction reads only the first structural byte")
}

/// Drive `schema()` then drain every record, returning the first error the
/// reader surfaces (or `Ok(count)` when the whole document reads cleanly).
/// Returning at all proves the reader did not overflow the stack.
fn drain(r: &mut JsonReader) -> Result<usize, FormatError> {
    r.schema()?;
    let mut n = 0;
    while r.next_record()?.is_some() {
        n += 1;
    }
    Ok(n)
}

#[test]
fn deeply_nested_object_is_rejected_not_stack_overflow() {
    // 3000 levels is well past serde_json's default parse recursion limit
    // (128). The NDJSON path parses each line with `serde_json::from_str`,
    // whose recursion guard returns a clean error rather than recursing into a
    // stack overflow. If a future serde_json disabled that guard by default,
    // this reader would regress to overflowing — so the test both pins the
    // guarantee and proves the process survives (it returns an error value).
    let mut r = reader(nested_object_line(3000).into_bytes());
    match r.schema() {
        Err(FormatError::Json(_)) => {}
        other => panic!("expected a Json parse error for over-deep nesting, got {other:?}"),
    }
}

#[test]
fn deeply_nested_top_level_array_element_is_rejected() {
    // A top-level array routes through the streaming element scanner, which
    // owns its own iterative nesting bound (never the call stack). A single
    // element nested 3000 deep is rejected cleanly instead of overflowing.
    let mut r = reader(format!("[{}]", nested_object_line(3000)).into_bytes());
    match drain(&mut r) {
        Err(FormatError::Json(_)) => {}
        other => panic!("expected a Json error draining an over-deep array element, got {other:?}"),
    }
}

#[test]
fn flatten_sentinel_marks_objects_deeper_than_the_flatten_cap() {
    // 100 levels sits within serde_json's 128 parse limit but past the reader's
    // 64-level flatten cap, so the flattener stops descending and records its
    // "[max depth]" sentinel rather than recursing without bound. Pins the cap
    // as observable behavior at the reader boundary.
    let mut r = reader(nested_object_line(100).into_bytes());
    let rec = r
        .next_record()
        .expect("a 100-deep object parses within serde's limit")
        .expect("one record");
    let has_sentinel = rec
        .iter_all_fields()
        .any(|(_, v)| v == &Value::String("[max depth]".into()));
    assert!(
        has_sentinel,
        "flatten cap must surface a `[max depth]` sentinel value; fields: {:?}",
        rec.iter_all_fields().collect::<Vec<_>>()
    );
    assert!(r.next_record().unwrap().is_none());
}

#[test]
fn ndjson_truncated_object_errors_cleanly() {
    // A line cut off mid-value is a parse error, not a panic or a silent
    // empty record.
    let mut r = reader(br#"{"a":1,"b":"#.to_vec());
    match r.schema() {
        Err(FormatError::Json(_)) => {}
        other => panic!("expected a Json error for a truncated object, got {other:?}"),
    }
}

#[test]
fn unterminated_top_level_array_errors_on_drain() {
    // The first element parses; the missing closing `]` surfaces as an error
    // when the stream reaches for the next element — never a silent truncation
    // that would drop the rest of a real document.
    let mut r = reader(br#"[{"a":1}"#.to_vec());
    let _ = r.schema().expect("first element parses");
    let first = r.next_record().expect("first record").expect("one element");
    assert_eq!(first.get("a"), Some(&Value::Integer(1)));
    match r.next_record() {
        Err(FormatError::Json(_)) => {}
        other => panic!("expected a Json error for an unterminated array, got {other:?}"),
    }
}
