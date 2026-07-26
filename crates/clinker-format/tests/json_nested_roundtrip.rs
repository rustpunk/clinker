//! Nested JSON survives a read followed by a write with no transform between.
//!
//! The reader flattens nested objects into dotted column names and the writer
//! expands them back, so the two are inverses over the whole shape a JSON
//! document can carry: objects to any depth, arrays of objects, arrays of
//! scalars, and nulls.

use clinker_format::json::reader::{JsonReader, JsonReaderConfig};
use clinker_format::json::writer::{JsonOutputMode, JsonWriter, JsonWriterConfig};
use clinker_format::{FormatReader, FormatWriter};

/// Read `input` as a single-record NDJSON document and write it straight back,
/// with no transform between, returning the emitted JSON.
fn read_then_write(input: &str) -> serde_json::Value {
    let mut reader = JsonReader::from_reader(
        std::io::Cursor::new(input.as_bytes().to_vec()),
        JsonReaderConfig::default(),
    )
    .expect("reader opens");
    let schema = reader.schema().expect("schema inferred");

    let mut buf = Vec::new();
    {
        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            // The reader emits an explicit null for a source null, so keeping
            // nulls is what makes the round trip lossless.
            preserve_nulls: true,
            ..Default::default()
        };
        let mut writer = JsonWriter::new(&mut buf, schema, config);
        while let Some(record) = reader.next_record().expect("record reads") {
            writer.write_record(&record).expect("record writes");
        }
        writer.flush().expect("writer flushes");
    }
    let out = String::from_utf8(buf).expect("utf-8 output");
    serde_json::from_str(out.trim_end()).expect("valid JSON output")
}

fn assert_round_trips(input: &str) {
    let expected: serde_json::Value = serde_json::from_str(input).expect("valid JSON input");
    assert_eq!(read_then_write(input), expected, "input: {input}");
}

#[test]
fn nested_objects_round_trip_to_any_depth() {
    assert_round_trips(r#"{"customer":{"name":"Ada","email":"ada@example.com"},"id":7}"#);
    assert_round_trips(r#"{"a":{"b":{"c":{"d":1}}},"top":2}"#);
}

#[test]
fn a_prefix_shared_by_interleaved_keys_regroups() {
    // The reader flattens depth-first, so the writer sees `a.x`, `n`, `a.y`
    // only when the source itself interleaved them — which JSON cannot express
    // for one object. What it can express is two sibling objects, and both come
    // back whole.
    assert_round_trips(r#"{"a":{"x":1},"n":2,"b":{"y":3}}"#);
}

#[test]
fn arrays_survive_whole() {
    // The reader does not flatten arrays: an array stays one column's value and
    // the writer serializes it natively, so its interior shape is untouched.
    assert_round_trips(r#"{"items":[{"sku":"A","qty":1},{"sku":"B","qty":2}]}"#);
    assert_round_trips(r#"{"tags":["x","y","z"],"id":1}"#);
    assert_round_trips(r#"{"order":{"lines":[{"n":1}]},"id":2}"#);
}

#[test]
fn nulls_survive_when_preserved() {
    assert_round_trips(r#"{"a":{"b":null},"c":null}"#);
}

#[test]
fn an_empty_nested_object_contributes_no_column() {
    // `{"a":{}}` flattens to no column at all, so nothing comes back for it —
    // the writer's own rule that an object with no emitted descendants emits no
    // key keeps the two ends agreeing.
    assert_eq!(
        read_then_write(r#"{"a":{},"b":1}"#),
        serde_json::json!({"b":1})
    );
}

#[test]
fn a_literal_dotted_source_key_comes_back_nested() {
    // A source key that literally contains `.` reaches the writer as the column
    // `a.b`, indistinguishable from a nested `{"a":{"b":…}}`, because the
    // reader joins path segments without escaping them. The writer then nests
    // it. Closing this gap is the read side's half of the grammar, tracked by
    // https://github.com/rustpunk/clinker/issues/920 — the reader's join has to
    // escape each key the way `field_path::encode_segment` does. Pinned here so
    // that flip is a deliberate change to this expectation rather than a
    // silent one.
    assert_eq!(
        read_then_write(r#"{"a.b":1}"#),
        serde_json::json!({"a":{"b":1}})
    );
}
