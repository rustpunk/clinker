//! JSON writer supporting array mode, NDJSON mode, pretty-printing,
//! and null omission. Implements `FormatWriter`.
//!
//! Under `reconstruct_envelope` the whole-stream array framing is REPLACED by
//! per-document object framing: each enveloped document becomes one JSON
//! object `{ "<header>": {...}, "body": [ ... ], "<footer>": {...} }`, with the
//! header/footer keys named by the source's `$doc` sections and the footer
//! optionally carrying a streaming record count. Multiple documents in one
//! stream are each individually framed — wrapped in an outer array in `array`
//! mode, or one object per line in `ndjson` mode. The body array streams one
//! record at a time, so no document is ever buffered.

use std::io::Write;
use std::sync::Arc;

use clinker_record::{DocumentContext, Record, Schema, Value};

use crate::envelope_writer::{EnvelopeFramer, OutputEnvelopeSpec};
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
    /// Whether engine-stamped schema columns (`$ck.<field>` correlation
    /// snapshots) appear as keys in the emitted JSON object. Defaults
    /// to `false` to keep engine-internal namespaces out of output.
    pub include_engine_stamped: bool,
    /// Per-document envelope reconstruction. `None` (the default) keeps the
    /// whole-stream array / NDJSON framing byte-identical to today. `Some` is
    /// set by the executor only under `reconstruct_envelope: true` and
    /// reframes the output to one object per document.
    pub envelope: Option<OutputEnvelopeSpec>,
}

impl Default for JsonWriterConfig {
    fn default() -> Self {
        Self {
            format: JsonOutputMode::Array,
            pretty: false,
            preserve_nulls: false,
            include_engine_stamped: false,
            envelope: None,
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
    /// Per-document envelope framer + state machine, present only when
    /// `config.envelope` is. Drives the per-document-object reframe.
    envelope: Option<EnvelopeState>,
    /// Reusable serialization buffer. Each record is serialized straight into
    /// this `Vec` (via a borrowing `serde::Serialize` impl, no intermediate
    /// `serde_json::Value` tree) and then written to the sink; the allocation
    /// is retained across records so per-record cost is amortized. Bounded by
    /// the widest single record, never the whole stream.
    scratch: Vec<u8>,
}

/// Per-document-object framing state for the envelope reframe. Tracks the
/// running framer (header/footer sections + the body record count, which also
/// drives the body-array comma), whether ANY document object has been emitted
/// (for the outer-array comma in `array` mode), and whether a document object
/// is currently open. Holds no body record — bounded memory.
struct EnvelopeState {
    framer: EnvelopeFramer,
    /// Whether at least one document object has been emitted this stream —
    /// drives the outer-array `[\n` vs `,\n` separator in `array` mode.
    any_doc_written: bool,
    /// Whether a document object is currently open (between begin/end).
    doc_open: bool,
}

impl<W: Write> JsonWriter<W> {
    pub fn new(writer: W, schema: Arc<Schema>, config: JsonWriterConfig) -> Self {
        let envelope = config
            .envelope
            .clone()
            .and_then(OutputEnvelopeSpec::into_framer)
            .map(|framer| EnvelopeState {
                framer,
                any_doc_written: false,
                doc_open: false,
            });
        Self {
            writer,
            _schema: schema,
            config,
            records_written: 0,
            envelope,
            scratch: Vec::new(),
        }
    }

    /// Serialize a record as a JSON object into the reusable `scratch` buffer
    /// in schema-column order. `preserve_nulls: false` omits keys with Null
    /// values. Engine-stamped columns are stripped from the default output;
    /// callers opt in via `include_engine_stamped`. Keys borrow the schema's
    /// column names and values borrow the record's [`Value`]s, so no
    /// intermediate `serde_json::Value` tree is built.
    ///
    /// The buffer is cleared first, and the sink is written only by the caller
    /// on success, so a mid-record error (non-finite float) leaves no partial
    /// bytes downstream.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Json`] when a field holds a non-finite float
    /// (NaN or an infinity), which JSON cannot represent.
    fn serialize_record(&mut self, record: &Record) -> Result<(), FormatError> {
        use serde::Serialize as _;
        self.scratch.clear();
        let obj = RecordObjectSer {
            record,
            preserve_nulls: self.config.preserve_nulls,
            include_engine_stamped: self.config.include_engine_stamped,
        };
        let result = if self.config.pretty {
            obj.serialize(&mut serde_json::Serializer::pretty(&mut self.scratch))
        } else {
            obj.serialize(&mut serde_json::Serializer::new(&mut self.scratch))
        };
        result.map_err(|e| FormatError::Json(e.to_string()))
    }

    /// Serialize a JSON value with the configured pretty/compact mode.
    fn serialize_value(&self, value: &serde_json::Value) -> Result<String, FormatError> {
        let s = if self.config.pretty {
            serde_json::to_string_pretty(value)
        } else {
            serde_json::to_string(value)
        }
        .map_err(|e| FormatError::Json(e.to_string()))?;
        Ok(s)
    }

    /// Build a JSON object from an envelope section's ordered fields, plus an
    /// optional trailing computed-count entry. `null` is always emitted for a
    /// section (envelope sections are small typed metadata, not body records),
    /// so the round-trip stays faithful regardless of `preserve_nulls`.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Json`] when a section field holds a non-finite
    /// float (NaN or an infinity), which JSON cannot represent.
    fn section_object(
        fields: &indexmap::IndexMap<Box<str>, Value>,
        count: Option<(&str, i64)>,
    ) -> Result<serde_json::Value, FormatError> {
        use serde_json::{Map, Value as Jv};
        let mut obj = Map::new();
        for (name, value) in fields {
            obj.insert(name.to_string(), clinker_to_json(value)?);
        }
        if let Some((field, n)) = count {
            obj.insert(field.to_string(), Jv::Number(n.into()));
        }
        Ok(Jv::Object(obj))
    }

    /// Append one body record to the currently-open document object's `body`
    /// array, comma-separating from prior body records (off the framer's
    /// running record count, so there is no duplicate counter).
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Json`] when NO document object is open — a record
    /// reached an enveloped JSON writer without a `begin_document` (a record
    /// with no originating document, e.g. a `<merged>` fan-in / aggregate row).
    /// The plan-time guard (E347) rejects the pipeline shapes that produce
    /// such records upstream of an enveloped Output, so this is a defense-in-
    /// depth safety net: it raises a clean error rather than writing record
    /// bytes outside any `{...,"body":[` object (which would be malformed JSON).
    fn write_enveloped_record(&mut self, record: &Record) -> Result<(), FormatError> {
        self.serialize_record(record)?;
        // Read the framer state through a shared borrow that ends before the
        // sink writes, so the `envelope`, `writer`, and `scratch` field borrows
        // stay disjoint.
        let env = self
            .envelope
            .as_ref()
            .expect("write_enveloped_record only called with an envelope state");
        if !env.doc_open {
            return Err(FormatError::Json(
                "enveloped JSON output received a record with no open document — a record \
                 with no originating document (a fan-in / aggregate row) cannot be framed"
                    .to_string(),
            ));
        }
        let need_comma = env.framer.record_count() > 0;
        if need_comma {
            self.writer.write_all(b",").map_err(FormatError::Io)?;
        }
        self.writer
            .write_all(&self.scratch)
            .map_err(FormatError::Io)?;
        self.envelope
            .as_mut()
            .expect("envelope state present after doc-open guard")
            .framer
            .count_record();
        Ok(())
    }
}

impl<W: Write + Send> FormatWriter for JsonWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        // Envelope mode: append to the open document object's `body` array.
        // A document is always open here (the dispatch arm fires
        // `begin_document` before the first record), so this never writes a
        // stray top-level record.
        if self.envelope.is_some() {
            return self.write_enveloped_record(record);
        }

        self.serialize_record(record)?;

        match self.config.format {
            JsonOutputMode::Array => {
                if self.records_written == 0 {
                    self.writer.write_all(b"[\n").map_err(FormatError::Io)?;
                } else {
                    self.writer.write_all(b",\n").map_err(FormatError::Io)?;
                }
                self.writer
                    .write_all(&self.scratch)
                    .map_err(FormatError::Io)?;
            }
            JsonOutputMode::Ndjson => {
                if self.records_written > 0 {
                    self.writer.write_all(b"\n").map_err(FormatError::Io)?;
                }
                self.writer
                    .write_all(&self.scratch)
                    .map_err(FormatError::Io)?;
            }
        }

        self.records_written += 1;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        match self.envelope.as_ref() {
            // Envelope mode: in `array` mode the per-document objects are
            // wrapped in an outer array, closed here; `ndjson` mode needs no
            // wrapper. An empty stream emits `[]` (array) / nothing (ndjson),
            // matching the non-enveloped empty shape.
            Some(env) => {
                if let JsonOutputMode::Array = self.config.format {
                    if env.any_doc_written {
                        self.writer.write_all(b"\n]\n").map_err(FormatError::Io)?;
                    } else {
                        self.writer.write_all(b"[]\n").map_err(FormatError::Io)?;
                    }
                }
            }
            None => {
                if let JsonOutputMode::Array = self.config.format {
                    if self.records_written > 0 {
                        self.writer.write_all(b"\n]\n").map_err(FormatError::Io)?;
                    } else {
                        self.writer.write_all(b"[]\n").map_err(FormatError::Io)?;
                    }
                }
            }
        }
        self.writer.flush().map_err(FormatError::Io)?;
        Ok(())
    }

    /// Drain the underlying sink without writing the closing array bytes, so
    /// byte-limit split accounting can observe the size mid-document. The
    /// finalizing `]` (or `[]` for an empty file) is emitted only by
    /// [`Self::flush`] at end of file / rotation.
    fn flush_bytes(&mut self) -> Result<(), FormatError> {
        self.writer.flush().map_err(FormatError::Io)
    }

    fn begin_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        let Some(env) = self.envelope.as_mut() else {
            return Ok(());
        };
        env.framer.begin();
        // Build the optional header object only when the document carries the
        // configured section (a missing section emits no `"header"` key).
        let header = env
            .framer
            .header_fields(doc)
            .map(|fields| Self::section_object(fields, None))
            .transpose()?;
        let any_doc_written = env.any_doc_written;
        // Outer framing between document objects: `[\n` / `,\n` (array) or a
        // newline separator (ndjson).
        match self.config.format {
            JsonOutputMode::Array => {
                if any_doc_written {
                    self.writer.write_all(b",\n").map_err(FormatError::Io)?;
                } else {
                    self.writer.write_all(b"[\n").map_err(FormatError::Io)?;
                }
            }
            JsonOutputMode::Ndjson => {
                if any_doc_written {
                    self.writer.write_all(b"\n").map_err(FormatError::Io)?;
                }
            }
        }
        // Open the document object, emit the optional header, open the body
        // array. The body's records stream in via `write_record`.
        self.writer.write_all(b"{").map_err(FormatError::Io)?;
        if let Some(header) = header {
            let s = self.serialize_value(&header)?;
            self.writer
                .write_all(b"\"header\":")
                .map_err(FormatError::Io)?;
            self.writer
                .write_all(s.as_bytes())
                .map_err(FormatError::Io)?;
            self.writer.write_all(b",").map_err(FormatError::Io)?;
        }
        self.writer
            .write_all(b"\"body\":[")
            .map_err(FormatError::Io)?;
        if let Some(env) = self.envelope.as_mut() {
            env.doc_open = true;
        }
        Ok(())
    }

    fn end_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        if self.envelope.as_ref().is_none_or(|e| !e.doc_open) {
            return Ok(());
        }
        // Close the body array.
        self.writer.write_all(b"]").map_err(FormatError::Io)?;
        // Emit the optional footer only when the document carries the
        // configured footer section (the computed count rides it).
        let footer = {
            let env = self
                .envelope
                .as_ref()
                .expect("doc_open implies an envelope state");
            env.framer
                .footer_fields(doc)
                .map(|fields| Self::section_object(fields, env.framer.footer_count()))
                .transpose()?
        };
        if let Some(footer) = footer {
            let s = self.serialize_value(&footer)?;
            self.writer
                .write_all(b",\"footer\":")
                .map_err(FormatError::Io)?;
            self.writer
                .write_all(s.as_bytes())
                .map_err(FormatError::Io)?;
        }
        // Close the document object.
        self.writer.write_all(b"}").map_err(FormatError::Io)?;
        if let Some(env) = self.envelope.as_mut() {
            env.any_doc_written = true;
            env.doc_open = false;
        }
        Ok(())
    }
}

/// Converts a clinker [`Value`] to a `serde_json::Value`.
///
/// # Errors
///
/// Returns [`FormatError::Json`] for a non-finite float (NaN or an infinity):
/// JSON numbers cannot represent them, and mapping to `null` would be
/// indistinguishable from a source null on read-back.
fn clinker_to_json(val: &Value) -> Result<serde_json::Value, FormatError> {
    use serde_json::Value as Jv;
    Ok(match val {
        Value::Null => Jv::Null,
        Value::Bool(b) => Jv::Bool(*b),
        Value::Integer(i) => Jv::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(Jv::Number)
            .ok_or_else(|| {
                FormatError::Json(format!(
                    "non-finite float {f} has no JSON representation; \
                     filter or replace the value before the JSON output"
                ))
            })?,
        // JSON has no exact-decimal type, and a JSON number would collapse the
        // scale (2.50 -> 2.5) and risk binary-float reinterpretation by
        // consumers. Emit the scale-preserving string form to keep the value
        // exact end-to-end.
        Value::Decimal(d) => Jv::String(d.to_string()),
        Value::String(s) => Jv::String(s.to_string()),
        Value::Date(d) => Jv::String(d.to_string()),
        Value::DateTime(dt) => Jv::String(dt.to_string()),
        Value::Array(arr) => Jv::Array(arr.iter().map(clinker_to_json).collect::<Result<_, _>>()?),
        Value::Map(m) => {
            let obj = m
                .iter()
                .map(|(k, v)| Ok((k.to_string(), clinker_to_json(v)?)))
                .collect::<Result<_, FormatError>>()?;
            Jv::Object(obj)
        }
    })
}

/// Borrows a record and serializes its fields as a JSON object directly to the
/// target serializer, with no intermediate `serde_json::Value` tree. Keys
/// borrow the schema's column names; values borrow the record's [`Value`]s via
/// [`ValueSer`]. Mirrors the field selection of the (removed) `record_to_json`:
/// honors `preserve_nulls` (skips null fields when false) and
/// `include_engine_stamped` (whether `$`-namespaced correlation columns
/// appear).
struct RecordObjectSer<'a> {
    record: &'a Record,
    preserve_nulls: bool,
    include_engine_stamped: bool,
}

impl serde::Serialize for RecordObjectSer<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        // `None` length hint: serde_json ignores it, and the exact post-null-
        // skip entry count would need a second pass over the fields.
        let mut map = serializer.serialize_map(None)?;
        if self.include_engine_stamped {
            for (col, val) in self.record.iter_all_fields() {
                if !self.preserve_nulls && val.is_null() {
                    continue;
                }
                map.serialize_entry(col, &ValueSer(val))?;
            }
        } else {
            for (col, val) in self.record.iter_user_fields() {
                if !self.preserve_nulls && val.is_null() {
                    continue;
                }
                map.serialize_entry(col, &ValueSer(val))?;
            }
        }
        map.end()
    }
}

/// Borrows a clinker [`Value`] and serializes it directly, mirroring
/// [`clinker_to_json`] variant-for-variant without allocating an intermediate
/// `serde_json::Value`. A non-finite float is rejected with the same message
/// `clinker_to_json` raises, surfaced through the serializer's error type so it
/// arrives at the caller as [`FormatError::Json`] with identical text.
struct ValueSer<'a>(&'a Value);

impl serde::Serialize for ValueSer<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{Error as _, SerializeMap, SerializeSeq};
        match self.0 {
            Value::Null => serializer.serialize_unit(),
            Value::Bool(b) => serializer.serialize_bool(*b),
            Value::Integer(i) => serializer.serialize_i64(*i),
            Value::Float(f) => {
                // serde_json's default would silently coerce a non-finite float
                // to `null`, indistinguishable from a source null on read-back;
                // reject with the same text `clinker_to_json` uses.
                if !f.is_finite() {
                    return Err(S::Error::custom(format!(
                        "non-finite float {f} has no JSON representation; \
                         filter or replace the value before the JSON output"
                    )));
                }
                serializer.serialize_f64(*f)
            }
            // JSON has no exact-decimal type; emit the scale-preserving string
            // form (matches `clinker_to_json`).
            Value::Decimal(d) => serializer.serialize_str(&d.to_string()),
            Value::String(s) => serializer.serialize_str(s.as_str()),
            Value::Date(d) => serializer.serialize_str(&d.to_string()),
            Value::DateTime(dt) => serializer.serialize_str(&dt.to_string()),
            Value::Array(arr) => {
                let mut seq = serializer.serialize_seq(Some(arr.len()))?;
                for v in arr {
                    seq.serialize_element(&ValueSer(v))?;
                }
                seq.end()
            }
            Value::Map(m) => {
                let mut map = serializer.serialize_map(Some(m.len()))?;
                for (k, v) in m.iter() {
                    map.serialize_entry(k.as_ref(), &ValueSer(v))?;
                }
                map.end()
            }
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

    #[test]
    fn test_json_write_wide_and_long_value_roundtrip() {
        // Exercises the buffer-reuse serialize path across a reused writer with
        // a wide schema and a long string value, then reads back to confirm the
        // scratch buffer is fully rewritten per record (no stale-byte bleed) and
        // the round-trip is faithful.
        let cols: Vec<Box<str>> = (0..40).map(|i| format!("c{i}").into()).collect();
        let schema = Arc::new(Schema::new(cols));
        let long = "x".repeat(500);
        let mk = |seed: i64, tail: &str| {
            let mut vals: Vec<Value> = (0..40).map(|i| Value::Integer(seed + i as i64)).collect();
            // Overwrite one column with a long string to stress the value path.
            vals[17] = Value::String(format!("{long}-{tail}").into());
            Record::new(Arc::clone(&schema), vals)
        };
        let records = vec![mk(0, "first"), mk(100, "second")];
        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            preserve_nulls: true,
            ..Default::default()
        };
        let written = write_records(config, &records, &schema);

        let mut reader = JsonReader::from_reader(
            std::io::Cursor::new(written.into_bytes()),
            JsonReaderConfig::default(),
        )
        .unwrap();
        let _s = reader.schema().unwrap();
        let r0 = reader.next_record().unwrap().unwrap();
        let r1 = reader.next_record().unwrap().unwrap();
        assert!(reader.next_record().unwrap().is_none());
        assert_eq!(r0.get("c0"), Some(&Value::Integer(0)));
        assert_eq!(r0.get("c39"), Some(&Value::Integer(39)));
        assert_eq!(
            r0.get("c17"),
            Some(&Value::String(format!("{long}-first").into()))
        );
        assert_eq!(r1.get("c0"), Some(&Value::Integer(100)));
        assert_eq!(
            r1.get("c17"),
            Some(&Value::String(format!("{long}-second").into()))
        );
    }

    #[test]
    fn test_json_write_finite_float_roundtrips_exactly() {
        let schema = Arc::new(Schema::new(vec!["reading".into()]));
        let record = Record::new(Arc::clone(&schema), vec![Value::Float(2.5)]);
        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            ..Default::default()
        };
        let output = write_records(config, &[record], &schema);
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        assert_eq!(parsed["reading"], serde_json::json!(2.5));
    }

    #[test]
    fn test_json_write_rejects_non_finite_floats() {
        let schema = Arc::new(Schema::new(vec!["reading".into()]));
        for (val, rendered) in [
            (f64::NAN, "NaN"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
        ] {
            let record = Record::new(Arc::clone(&schema), vec![Value::Float(val)]);
            let mut buf = Vec::new();
            let mut w = JsonWriter::new(&mut buf, Arc::clone(&schema), JsonWriterConfig::default());
            let err = w.write_record(&record).unwrap_err();
            match err {
                FormatError::Json(msg) => assert!(
                    msg.contains(&format!("non-finite float {rendered}")),
                    "expected the non-finite message for {rendered}, got: {msg}"
                ),
                other => panic!("expected FormatError::Json for {rendered}, got {other:?}"),
            }
            drop(w);
            assert!(
                buf.is_empty(),
                "no partial bytes for a rejected record, got: {:?}",
                String::from_utf8_lossy(&buf)
            );
        }
    }

    #[test]
    fn test_json_write_rejects_non_finite_float_nested_in_array() {
        let schema = Arc::new(Schema::new(vec!["readings".into()]));
        let record = Record::new(
            Arc::clone(&schema),
            vec![Value::Array(vec![
                Value::Float(1.0),
                Value::Float(f64::INFINITY),
            ])],
        );
        let mut buf = Vec::new();
        let mut w = JsonWriter::new(&mut buf, Arc::clone(&schema), JsonWriterConfig::default());
        let err = w.write_record(&record).unwrap_err();
        match err {
            FormatError::Json(msg) => assert!(
                msg.contains("non-finite float inf"),
                "expected the non-finite message, got: {msg}"
            ),
            other => panic!("expected FormatError::Json, got {other:?}"),
        }
    }

    use crate::envelope_writer::test_doc_with_sections as doc_with_sections;

    fn amount_record(schema: &Arc<Schema>, n: i64) -> Record {
        Record::new(Arc::clone(schema), vec![Value::Integer(n)])
    }

    #[test]
    fn json_envelope_record_with_no_open_document_errors_cleanly() {
        // Defense-in-depth: the plan-time E347 guard rejects pipelines that
        // route lineage-stripped (`<merged>`) records into an enveloped JSON
        // Output, so `begin_document` always fires first in production. If one
        // ever reached here without an open document, the writer must raise a
        // clean error rather than emit record bytes outside any `body` array
        // (which would be malformed JSON).
        let schema = Arc::new(Schema::new(vec!["amount".into()]));
        let config = JsonWriterConfig {
            format: JsonOutputMode::Array,
            envelope: Some(crate::envelope_writer::OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut buf = Vec::new();
        let mut w = JsonWriter::new(&mut buf, Arc::clone(&schema), config);
        // No begin_document — write straight into the envelope writer.
        let err = w.write_record(&amount_record(&schema, 1)).unwrap_err();
        match err {
            FormatError::Json(msg) => assert!(
                msg.contains("no open document"),
                "expected the no-open-document message, got: {msg}"
            ),
            other => panic!("expected FormatError::Json, got {other:?}"),
        }
    }

    #[test]
    fn json_envelope_array_mode_frames_each_document_as_an_object() {
        let schema = Arc::new(Schema::new(vec!["amount".into()]));
        let config = JsonWriterConfig {
            format: JsonOutputMode::Array,
            envelope: Some(crate::envelope_writer::OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                footer_from_doc: Some("Foot".into()),
                footer_record_count_field: Some("count".into()),
            }),
            ..Default::default()
        };
        let doc_a = doc_with_sections(&[
            ("Head", &[("batch_id", Value::String("A".into()))]),
            ("Foot", &[("checksum", Value::String("SUM-A".into()))]),
        ]);
        let doc_b = doc_with_sections(&[
            ("Head", &[("batch_id", Value::String("B".into()))]),
            ("Foot", &[("checksum", Value::String("SUM-B".into()))]),
        ]);
        let mut buf = Vec::new();
        {
            let mut w = JsonWriter::new(&mut buf, Arc::clone(&schema), config);
            w.begin_document(&doc_a).unwrap();
            w.write_record(&amount_record(&schema, 10)).unwrap();
            w.write_record(&amount_record(&schema, 20)).unwrap();
            w.end_document(&doc_a).unwrap();
            w.begin_document(&doc_b).unwrap();
            w.write_record(&amount_record(&schema, 30)).unwrap();
            w.end_document(&doc_b).unwrap();
            w.flush().unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        // Valid JSON: an outer array of two document objects.
        let parsed: serde_json::Value = serde_json::from_str(&out).expect("valid JSON array");
        let arr = parsed.as_array().expect("outer array");
        assert_eq!(arr.len(), 2, "one object per document: {out}");
        assert_eq!(arr[0]["header"]["batch_id"], "A");
        assert_eq!(
            arr[0]["body"],
            serde_json::json!([{"amount":10},{"amount":20}])
        );
        assert_eq!(arr[0]["footer"]["checksum"], "SUM-A");
        assert_eq!(arr[0]["footer"]["count"], 2);
        assert_eq!(arr[1]["header"]["batch_id"], "B");
        assert_eq!(arr[1]["footer"]["count"], 1, "count resets per document");
    }

    #[test]
    fn json_envelope_ndjson_mode_one_document_object_per_line() {
        let schema = Arc::new(Schema::new(vec!["amount".into()]));
        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            envelope: Some(crate::envelope_writer::OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let doc_a = doc_with_sections(&[("Head", &[("batch_id", Value::String("A".into()))])]);
        let doc_b = doc_with_sections(&[("Head", &[("batch_id", Value::String("B".into()))])]);
        let mut buf = Vec::new();
        {
            let mut w = JsonWriter::new(&mut buf, Arc::clone(&schema), config);
            w.begin_document(&doc_a).unwrap();
            w.write_record(&amount_record(&schema, 10)).unwrap();
            w.end_document(&doc_a).unwrap();
            w.begin_document(&doc_b).unwrap();
            w.write_record(&amount_record(&schema, 20)).unwrap();
            w.end_document(&doc_b).unwrap();
            w.flush().unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2, "one document object per line: {out}");
        let d0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(d0["header"]["batch_id"], "A");
        assert_eq!(d0["body"], serde_json::json!([{"amount":10}]));
    }

    #[test]
    fn json_envelope_rejects_non_finite_float_in_section() {
        let schema = Arc::new(Schema::new(vec!["amount".into()]));
        let config = JsonWriterConfig {
            format: JsonOutputMode::Array,
            envelope: Some(crate::envelope_writer::OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let doc = doc_with_sections(&[("Head", &[("ratio", Value::Float(f64::NAN))])]);
        let mut buf = Vec::new();
        let mut w = JsonWriter::new(&mut buf, Arc::clone(&schema), config);
        let err = w.begin_document(&doc).unwrap_err();
        match err {
            FormatError::Json(msg) => assert!(
                msg.contains("non-finite float NaN"),
                "expected the non-finite message, got: {msg}"
            ),
            other => panic!("expected FormatError::Json, got {other:?}"),
        }
    }

    #[test]
    fn json_envelope_rejects_non_finite_float_in_body_record() {
        let schema = Arc::new(Schema::new(vec!["amount".into()]));
        let config = JsonWriterConfig {
            format: JsonOutputMode::Array,
            envelope: Some(crate::envelope_writer::OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let doc = doc_with_sections(&[("Head", &[("batch_id", Value::String("A".into()))])]);
        let mut buf = Vec::new();
        let mut w = JsonWriter::new(&mut buf, Arc::clone(&schema), config);
        w.begin_document(&doc).unwrap();
        let record = Record::new(Arc::clone(&schema), vec![Value::Float(f64::NEG_INFINITY)]);
        let err = w.write_record(&record).unwrap_err();
        match err {
            FormatError::Json(msg) => assert!(
                msg.contains("non-finite float -inf"),
                "expected the non-finite message, got: {msg}"
            ),
            other => panic!("expected FormatError::Json, got {other:?}"),
        }
    }

    #[test]
    fn json_envelope_off_is_byte_identical() {
        // No envelope spec: array framing is the plain whole-stream array.
        let schema = test_schema();
        let records = vec![make_record(&schema, "Alice", 30, true)];
        let baseline = write_records(JsonWriterConfig::default(), &records, &schema);
        let parsed: serde_json::Value = serde_json::from_str(&baseline).unwrap();
        assert!(
            parsed.as_array().unwrap()[0]["body"].is_null(),
            "no per-doc body key"
        );
        assert_eq!(parsed.as_array().unwrap()[0]["name"], "Alice");
    }
}
