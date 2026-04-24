//! JSON writer supporting array mode, NDJSON mode, pretty-printing,
//! and null omission. Implements `FormatWriter`.

use std::io::Write;
use std::sync::Arc;

use clinker_record::{Record, Schema, Value};

use crate::error::FormatError;
use crate::traits::FormatWriter;

/// JSON output format mode.
#[derive(Debug, Clone, Copy, Default)]
pub enum JsonOutputMode {
    /// `[{...},{...},...]` — valid JSON array.
    #[default]
    Array,
    /// One JSON object per line, no wrapper.
    Ndjson,
}

#[derive(Clone)]
pub struct JsonWriterConfig {
    pub format: JsonOutputMode,
    pub pretty: bool,
    pub preserve_nulls: bool,
}

impl Default for JsonWriterConfig {
    fn default() -> Self {
        Self {
            format: JsonOutputMode::Array,
            pretty: false,
            preserve_nulls: false,
        }
    }
}

pub struct JsonWriter<W: Write> {
    writer: W,
    /// Schema held for the writer's lifetime. Post-rip the writer emits
    /// records via `Record::iter_all_fields` (schema-positional) so the
    /// field is not read per-record, but keeping the `Arc` pins the
    /// schema against unintended drop by factory callers.
    _schema: Arc<Schema>,
    config: JsonWriterConfig,
    records_written: u64,
}

impl<W: Write> JsonWriter<W> {
    pub fn new(writer: W, schema: Arc<Schema>, config: JsonWriterConfig) -> Self {
        Self {
            writer,
            _schema: schema,
            config,
            records_written: 0,
        }
    }

    /// Serialize a record to a JSON object in schema-column order.
    /// `preserve_nulls: false` omits keys with Null values. Metadata is
    /// stripped from the default output; callers that want to emit
    /// `$meta.*` keys into the JSON object must opt in at a higher
    /// layer (the Output node's `include_metadata` flag).
    fn record_to_json(&self, record: &Record) -> serde_json::Value {
        use serde_json::{Map, Value as Jv};

        let mut obj = Map::with_capacity(record.field_count());
        for (col, val) in record.iter_all_fields() {
            if !self.config.preserve_nulls && val.is_null() {
                continue;
            }
            obj.insert(col.to_string(), clinker_to_json(val));
        }
        Jv::Object(obj)
    }
}

impl<W: Write + Send> FormatWriter for JsonWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        let json_val = self.record_to_json(record);

        let serialized = if self.config.pretty {
            serde_json::to_string_pretty(&json_val)
        } else {
            serde_json::to_string(&json_val)
        }
        .map_err(|e| FormatError::Json(e.to_string()))?;

        match self.config.format {
            JsonOutputMode::Array => {
                if self.records_written == 0 {
                    self.writer.write_all(b"[\n").map_err(FormatError::Io)?;
                } else {
                    self.writer.write_all(b",\n").map_err(FormatError::Io)?;
                }
                self.writer
                    .write_all(serialized.as_bytes())
                    .map_err(FormatError::Io)?;
            }
            JsonOutputMode::Ndjson => {
                if self.records_written > 0 {
                    self.writer.write_all(b"\n").map_err(FormatError::Io)?;
                }
                self.writer
                    .write_all(serialized.as_bytes())
                    .map_err(FormatError::Io)?;
            }
        }

        self.records_written += 1;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        if let JsonOutputMode::Array = self.config.format {
            if self.records_written > 0 {
                self.writer.write_all(b"\n]\n").map_err(FormatError::Io)?;
            } else {
                self.writer.write_all(b"[]\n").map_err(FormatError::Io)?;
            }
        }
        self.writer.flush().map_err(FormatError::Io)?;
        Ok(())
    }
}

fn clinker_to_json(val: &Value) -> serde_json::Value {
    use serde_json::Value as Jv;
    match val {
        Value::Null => Jv::Null,
        Value::Bool(b) => Jv::Bool(*b),
        Value::Integer(i) => Jv::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(Jv::Number)
            .unwrap_or(Jv::Null),
        Value::String(s) => Jv::String(s.to_string()),
        Value::Date(d) => Jv::String(d.to_string()),
        Value::DateTime(dt) => Jv::String(dt.to_string()),
        Value::Array(arr) => Jv::Array(arr.iter().map(clinker_to_json).collect()),
        Value::Map(m) => {
            let obj = m
                .iter()
                .map(|(k, v)| (k.to_string(), clinker_to_json(v)))
                .collect();
            Jv::Object(obj)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::reader::{JsonReader, JsonReaderConfig};
    use crate::traits::FormatReader;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            "name".into(),
            "age".into(),
            "active".into(),
        ]))
    }

    fn make_record(schema: &Arc<Schema>, name: &str, age: i64, active: bool) -> Record {
        Record::new(
            Arc::clone(schema),
            vec![
                Value::String(name.into()),
                Value::Integer(age),
                Value::Bool(active),
            ],
        )
    }

    fn write_records(config: JsonWriterConfig, records: &[Record], schema: &Arc<Schema>) -> String {
        let mut buf = Vec::new();
        let mut w = JsonWriter::new(&mut buf, Arc::clone(schema), config);
        for r in records {
            w.write_record(r).unwrap();
        }
        w.flush().unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_json_write_array_mode() {
        let schema = test_schema();
        let records = vec![
            make_record(&schema, "Alice", 30, true),
            make_record(&schema, "Bob", 25, false),
            make_record(&schema, "Carol", 35, true),
        ];
        let output = write_records(JsonWriterConfig::default(), &records, &schema);
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0]["name"], "Alice");
        assert_eq!(arr[1]["age"], 25);
        assert_eq!(arr[2]["active"], true);
    }

    #[test]
    fn test_json_write_ndjson_mode() {
        let schema = test_schema();
        let records = vec![
            make_record(&schema, "Alice", 30, true),
            make_record(&schema, "Bob", 25, false),
            make_record(&schema, "Carol", 35, true),
        ];
        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            ..Default::default()
        };
        let output = write_records(config, &records, &schema);
        let lines: Vec<&str> = output.trim().split('\n').collect();
        assert_eq!(lines.len(), 3);
        for line in &lines {
            let _: serde_json::Value = serde_json::from_str(line).unwrap();
        }
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["name"], "Alice");
    }

    #[test]
    fn test_json_write_pretty() {
        let schema = test_schema();
        let records = vec![make_record(&schema, "Alice", 30, true)];
        let config = JsonWriterConfig {
            pretty: true,
            ..Default::default()
        };
        let output = write_records(config, &records, &schema);
        // Pretty output has indentation within objects
        assert!(
            output.contains("  \"name\""),
            "Pretty output should be indented: {output}"
        );
    }

    #[test]
    fn test_json_write_omit_nulls() {
        let schema = Arc::new(Schema::new(vec!["a".into(), "b".into()]));
        let record = Record::new(
            Arc::clone(&schema),
            vec![Value::String("hello".into()), Value::Null],
        );
        let config = JsonWriterConfig {
            preserve_nulls: false,
            ..Default::default()
        };
        let output = write_records(config, &[record], &schema);
        assert!(
            !output.contains("\"b\""),
            "Null field 'b' should be omitted: {output}"
        );
        assert!(output.contains("\"a\""));
    }

    #[test]
    fn test_json_write_preserve_nulls() {
        let schema = Arc::new(Schema::new(vec!["a".into(), "b".into()]));
        let record = Record::new(
            Arc::clone(&schema),
            vec![Value::String("hello".into()), Value::Null],
        );
        let config = JsonWriterConfig {
            preserve_nulls: true,
            ..Default::default()
        };
        let output = write_records(config, &[record], &schema);
        assert!(
            output.contains("\"b\":null") || output.contains("\"b\": null"),
            "Null field 'b' should be present: {output}"
        );
    }

    #[test]
    fn test_json_writer_emits_schema_fields_only() {
        // Default writer contract: schema columns only; metadata (and
        // the `$meta.*` namespace) does not leak into the JSON object
        // unless a higher layer opts in.
        let schema = Arc::new(Schema::new(vec!["a".into()]));
        let mut record = Record::new(Arc::clone(&schema), vec![Value::Integer(1)]);
        record
            .set_meta("ignored", Value::String("in_meta".into()))
            .unwrap();
        let output = write_records(JsonWriterConfig::default(), &[record], &schema);
        assert!(output.contains("\"a\""));
        assert!(!output.contains("ignored"));
        assert!(!output.contains("in_meta"));
    }

    #[test]
    fn test_json_write_field_ordering() {
        // Schema fields emit in schema order — the widened schema is
        // authoritative; there is no overflow ordering to reason about.
        let schema = Arc::new(Schema::new(vec![
            "z_field".into(),
            "a_field".into(),
            "m_field".into(),
        ]));
        let record = Record::new(
            Arc::clone(&schema),
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        );

        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            ..Default::default()
        };
        let output = write_records(config, &[record], &schema);
        let z_pos = output.find("z_field").unwrap();
        let a_pos = output.find("a_field").unwrap();
        let m_pos = output.find("m_field").unwrap();
        assert!(z_pos < a_pos, "schema field z comes before a");
        assert!(a_pos < m_pos, "schema fields emit in schema order");
    }

    #[test]
    fn test_json_roundtrip_reader_writer() {
        let schema = test_schema();
        let records = vec![
            make_record(&schema, "Alice", 30, true),
            make_record(&schema, "Bob", 25, false),
        ];

        // Write as NDJSON
        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            preserve_nulls: true,
            ..Default::default()
        };
        let written = write_records(config, &records, &schema);

        // Read back
        let mut reader = JsonReader::from_reader(
            std::io::Cursor::new(written.as_bytes().to_vec()),
            JsonReaderConfig::default(),
        )
        .unwrap();
        let _s = reader.schema().unwrap();
        let r1 = reader.next_record().unwrap().unwrap();
        let r2 = reader.next_record().unwrap().unwrap();
        assert!(reader.next_record().unwrap().is_none());

        assert_eq!(r1.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(r1.get("age"), Some(&Value::Integer(30)));
        assert_eq!(r1.get("active"), Some(&Value::Bool(true)));
        assert_eq!(r2.get("name"), Some(&Value::String("Bob".into())));
        assert_eq!(r2.get("age"), Some(&Value::Integer(25)));
    }
}
