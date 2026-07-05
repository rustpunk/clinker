use std::io::Write;
use std::sync::{Arc, Mutex};

use clinker_record::{DocumentContext, Record, Schema, Value};

use crate::envelope_writer::{EnvelopeFramer, OutputEnvelopeSpec};
use crate::error::FormatError;
use crate::traits::FormatWriter;

/// Configuration for the CSV writer.
#[derive(Clone)]
pub struct CsvWriterConfig {
    pub delimiter: u8,
    pub include_header: bool,
    /// Whether engine-stamped schema columns (today: `$ck.<field>`
    /// correlation snapshots) are emitted into the CSV. Defaults to
    /// `false` — engine-internal namespaces are stripped from the
    /// default output unless the Output node opts in via
    /// `include_correlation_keys: true`.
    pub include_engine_stamped: bool,
    /// Per-document envelope reconstruction. `None` (the default) renders no
    /// framing and keeps the output byte-identical to the boundary-unaware
    /// path. `Some` is set by the executor only when the Output declares
    /// `reconstruct_envelope: true` with a non-empty envelope config.
    pub envelope: Option<OutputEnvelopeSpec>,
}

impl Default for CsvWriterConfig {
    fn default() -> Self {
        Self {
            delimiter: b',',
            include_header: true,
            include_engine_stamped: false,
            envelope: None,
        }
    }
}

/// Streaming CSV writer wrapping `csv::Writer`.
///
/// Emits cells in the writer's pinned schema order — never the
/// record's own schema order — so values stay aligned with the header
/// even when an upstream projection or widening reordered the
/// record's fields. Fields the record lacks emit an empty cell.
/// Header row is written on first `write_record()` call if
/// `include_header` is true.
///
/// Under `reconstruct_envelope`, `begin_document` emits the header section's
/// field values as one CSV row before the document's body, and `end_document`
/// emits the footer section's values (plus the streaming record count) as one
/// CSV row after it — no document is buffered, so framing stays O(1-record).
pub struct CsvWriter<W: Write> {
    inner: csv::Writer<W>,
    schema: Arc<Schema>,
    config: CsvWriterConfig,
    header_written: bool,
    /// Per-document envelope framer, present only when `config.envelope` is.
    framer: Option<EnvelopeFramer>,
}

impl<W: Write> CsvWriter<W> {
    pub fn new(writer: W, schema: Arc<Schema>, config: CsvWriterConfig) -> Self {
        let framer = config
            .envelope
            .clone()
            .and_then(OutputEnvelopeSpec::into_framer);
        // Envelope header/footer rows carry a different field count than the
        // body rows (a section's fields vs the writer schema), so the writer
        // must accept ragged records when framing is active. The non-envelope
        // path keeps the strict equal-length default; body rows match the
        // header length by construction (both derive from the pinned schema),
        // so the guard is a backstop rather than a drift detector.
        let inner = csv::WriterBuilder::new()
            .delimiter(config.delimiter)
            .flexible(framer.is_some())
            .from_writer(writer);
        Self {
            inner,
            schema,
            config,
            header_written: false,
            framer,
        }
    }

    /// Emit one envelope section's field values (in declared order) as a CSV
    /// row, optionally appending a trailing computed-count cell.
    fn write_section_row(
        inner: &mut csv::Writer<W>,
        fields: &indexmap::IndexMap<Box<str>, Value>,
        count: Option<(&str, i64)>,
    ) -> Result<(), FormatError> {
        let mut cells: Vec<String> = Vec::new();
        for (name, value) in fields {
            cells.push(value_to_csv_cell(name, value)?);
        }
        if let Some((_field, n)) = count {
            cells.push(n.to_string());
        }
        inner.write_record(&cells)?;
        Ok(())
    }

    /// Write a pre-captured header row, bypassing first-record discovery.
    ///
    /// Used by `SplittingWriter` on rotation to ensure all split files
    /// have identical headers (captured from the first file).
    pub fn write_preset_header(&mut self, header: &[Box<str>]) -> Result<(), FormatError> {
        let refs: Vec<&str> = header.iter().map(|h| h.as_ref()).collect();
        self.inner.write_record(&refs)?;
        self.header_written = true;
        Ok(())
    }

    /// Borrow the underlying writer (e.g. for byte-count inspection).
    pub fn get_ref(&self) -> &W {
        self.inner.get_ref()
    }
}

impl<W: Write + Send> FormatWriter for CsvWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        // Header is built from the writer's pinned schema. Engine-stamped
        // columns (today: `$ck.<field>`) are stripped unless the Output
        // node opts in.
        //
        // Under envelope framing the schema column-header row is SUPPRESSED:
        // it would otherwise emit once, before the first document's envelope
        // header row, giving documents inconsistent layouts (header row only
        // on the first). The per-document envelope header section replaces it.
        if self.config.include_header && !self.header_written && self.framer.is_none() {
            let header: Vec<&str> =
                filtered_header_columns(&self.schema, self.config.include_engine_stamped);
            self.inner.write_record(&header)?;
            self.header_written = true;
        }

        // Body cells follow the writer's pinned schema — the same
        // column set and order the header row was built from — not
        // the record's own schema, which an upstream projection or
        // widening may order differently for the same fields. Fields
        // the record lacks emit an empty cell (an explicit Null),
        // matching the fixed-width writer's missing-field policy.
        let columns = filtered_header_columns(&self.schema, self.config.include_engine_stamped);
        let mut fields: Vec<String> = Vec::with_capacity(columns.len());
        for col in columns {
            let cell = match record.get(col) {
                Some(v) => value_to_csv_cell(col, v)?,
                None => String::new(),
            };
            fields.push(cell);
        }

        self.inner.write_record(&fields)?;
        if let Some(framer) = self.framer.as_mut() {
            framer.count_record();
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        self.inner.flush()?;
        Ok(())
    }

    fn begin_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        let Some(framer) = self.framer.as_mut() else {
            return Ok(());
        };
        framer.begin();
        // Only emit a header row when the document actually carries the
        // configured section — a missing section writes nothing.
        if let Some(fields) = framer.header_fields(doc) {
            Self::write_section_row(&mut self.inner, fields, None)?;
        }
        Ok(())
    }

    fn end_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        let Some(framer) = self.framer.as_ref() else {
            return Ok(());
        };
        // Likewise the footer: emit only when the configured section is
        // present; the computed count rides the present footer section.
        if let Some(fields) = framer.footer_fields(doc) {
            let count = framer.footer_count();
            Self::write_section_row(&mut self.inner, fields, count)?;
        }
        Ok(())
    }
}

/// CSV writer wrapper that captures the header (schema columns only)
/// from the first record into shared state for replay on split rotation.
///
/// Only used by the CSV writer factory when splitting is enabled.
/// Subsequent split files receive the captured header via `write_preset_header()`.
/// Non-CSV formats do not need this — their factories are stateless.
pub struct HeaderCapturingCsvWriter<W: Write> {
    inner: CsvWriter<W>,
    schema: Arc<Schema>,
    shared_header: Arc<Mutex<Option<Vec<Box<str>>>>>,
    captured: bool,
}

impl<W: Write> HeaderCapturingCsvWriter<W> {
    pub fn new(
        inner: CsvWriter<W>,
        schema: Arc<Schema>,
        shared_header: Arc<Mutex<Option<Vec<Box<str>>>>>,
    ) -> Self {
        Self {
            inner,
            schema,
            shared_header,
            captured: false,
        }
    }
}

impl<W: Write + Send> FormatWriter for HeaderCapturingCsvWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        if !self.captured {
            // Capture header so split rotations replay the same column
            // set; engine-stamped columns are filtered identically to
            // the inner CSV writer.
            let include = self.inner.config.include_engine_stamped;
            let header: Vec<Box<str>> = self
                .schema
                .columns()
                .iter()
                .enumerate()
                .filter(|(i, _)| {
                    include
                        || self
                            .schema
                            .field_metadata(*i)
                            .is_none_or(|m| !m.is_engine_stamped())
                })
                .map(|(_, name)| name.clone())
                .collect();
            *self.shared_header.lock().unwrap() = Some(header);
            self.captured = true;
        }
        self.inner.write_record(record)
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        self.inner.flush()
    }
}

/// Build the CSV header name list from `schema`, optionally including
/// engine-stamped columns.
fn filtered_header_columns(schema: &Arc<Schema>, include_engine_stamped: bool) -> Vec<&str> {
    schema
        .columns()
        .iter()
        .enumerate()
        .filter(|(i, _)| {
            include_engine_stamped
                || schema
                    .field_metadata(*i)
                    .is_none_or(|m| !m.is_engine_stamped())
        })
        .map(|(_, c)| c.as_ref())
        .collect()
}

/// Serialize a Value to a CSV cell string.
///
/// `Value::Map` has no canonical scalar serialization for a CSV cell
/// (silently JSON-encoding hides routing bugs — e.g. a `$widened`
/// sidecar reaching the writer without the projection layer's
/// `include_unmapped: true` expansion), so this returns
/// `FormatError::UnserializableMapValue` carrying the offending
/// column. CSV is the single point of truth for map rejection on
/// this path; there is no upstream pre-walk.
fn value_to_csv_cell(col: &str, value: &Value) -> Result<String, FormatError> {
    Ok(match value {
        Value::Null => String::new(),
        Value::Bool(b) => if *b { "true" } else { "false" }.into(),
        Value::Integer(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        // A decimal carries its own (column) scale; Display preserves it.
        Value::Decimal(d) => d.to_string(),
        Value::String(s) => s.to_string(),
        Value::Date(d) => d.format("%Y-%m-%d").to_string(),
        Value::DateTime(dt) => dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
        Value::Array(arr) => serde_json::to_string(arr).unwrap_or_default(),
        Value::Map(_) => {
            return Err(FormatError::UnserializableMapValue {
                format: "CSV",
                column: col.to_string(),
            });
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv::reader::{CsvReader, CsvReaderConfig};
    use crate::traits::FormatReader;

    fn make_schema(cols: &[&str]) -> Arc<Schema> {
        Arc::new(Schema::new(cols.iter().map(|c| (*c).into()).collect()))
    }

    fn make_record(schema: &Arc<Schema>, values: Vec<Value>) -> Record {
        Record::new(Arc::clone(schema), values)
    }

    fn write_to_string(
        schema: &Arc<Schema>,
        config: CsvWriterConfig,
        records: &[Record],
    ) -> String {
        let mut buf = Vec::new();
        {
            let mut writer = CsvWriter::new(&mut buf, Arc::clone(schema), config);
            for r in records {
                writer.write_record(r).unwrap();
            }
            writer.flush().unwrap();
        }
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_csv_writer_basic_output() {
        let schema = make_schema(&["name", "age"]);
        let records = vec![
            make_record(
                &schema,
                vec![Value::String("Alice".into()), Value::String("30".into())],
            ),
            make_record(
                &schema,
                vec![Value::String("Bob".into()), Value::String("25".into())],
            ),
            make_record(
                &schema,
                vec![Value::String("Charlie".into()), Value::String("35".into())],
            ),
        ];
        let output = write_to_string(&schema, CsvWriterConfig::default(), &records);
        assert_eq!(output, "name,age\nAlice,30\nBob,25\nCharlie,35\n");
    }

    #[test]
    fn test_csv_writer_with_header() {
        let schema = make_schema(&["x", "y"]);
        let records = vec![make_record(
            &schema,
            vec![Value::Integer(1), Value::Integer(2)],
        )];
        let output = write_to_string(
            &schema,
            CsvWriterConfig {
                include_header: true,
                ..Default::default()
            },
            &records,
        );
        assert!(output.starts_with("x,y\n"));
    }

    #[test]
    fn test_csv_writer_no_header() {
        let schema = make_schema(&["x", "y"]);
        let records = vec![make_record(
            &schema,
            vec![Value::Integer(1), Value::Integer(2)],
        )];
        let output = write_to_string(
            &schema,
            CsvWriterConfig {
                include_header: false,
                ..Default::default()
            },
            &records,
        );
        assert_eq!(output, "1,2\n");
    }

    #[test]
    fn test_csv_writer_null_as_empty() {
        let schema = make_schema(&["a", "b", "c"]);
        let records = vec![make_record(
            &schema,
            vec![
                Value::String("x".into()),
                Value::Null,
                Value::String("z".into()),
            ],
        )];
        let output = write_to_string(&schema, CsvWriterConfig::default(), &records);
        // Null becomes empty string between delimiters
        assert_eq!(output, "a,b,c\nx,,z\n");
    }

    fn make_schema_with_engine_stamp(user_col: &str, stamp_col: &str) -> Arc<Schema> {
        use clinker_record::FieldMetadata;
        use clinker_record::SchemaBuilder;
        SchemaBuilder::new()
            .with_field(user_col)
            .with_field_meta(stamp_col, FieldMetadata::source_correlation(user_col))
            .build()
    }

    #[test]
    fn test_csv_writer_strips_engine_stamped_by_default() {
        let schema = make_schema_with_engine_stamp("id", "$ck.id");
        let record = make_record(&schema, vec![Value::Integer(7), Value::Integer(7)]);
        let output = write_to_string(&schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "id\n7\n");
        assert!(!output.contains("$ck.id"));
    }

    #[test]
    fn test_csv_writer_includes_engine_stamped_on_opt_in() {
        let schema = make_schema_with_engine_stamp("id", "$ck.id");
        let record = make_record(&schema, vec![Value::Integer(7), Value::Integer(7)]);
        let config = CsvWriterConfig {
            include_engine_stamped: true,
            ..Default::default()
        };
        let output = write_to_string(&schema, config, &[record]);
        assert_eq!(output, "id,$ck.id\n7,7\n");
    }

    #[test]
    fn test_csv_writer_widened_schema_emit_order() {
        // Widened schema controls output order; the fixture declares
        // every emitted column up front, so record.set always lands at
        // a known slot and the writer walks them in schema order.
        let schema = make_schema(&["id", "zulu", "alpha", "mike"]);
        let record = make_record(
            &schema,
            vec![
                Value::Integer(1),
                Value::String("z".into()),
                Value::String("a".into()),
                Value::String("m".into()),
            ],
        );
        let output = write_to_string(&schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "id,zulu,alpha,mike\n1,z,a,m\n");
    }

    /// Body cells follow the writer's pinned schema order, so a record
    /// whose own schema orders the same fields differently still lands
    /// each value under its header column.
    #[test]
    fn test_csv_writer_record_schema_order_differs_from_writer_schema() {
        let writer_schema = make_schema(&["a", "b"]);
        let record_schema = make_schema(&["b", "a"]);
        let record = make_record(
            &record_schema,
            vec![Value::String("B".into()), Value::String("A".into())],
        );
        let output = write_to_string(&writer_schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "a,b\nA,B\n");
    }

    /// A writer-schema column absent from the record emits an empty
    /// cell, keeping later columns aligned with the header.
    #[test]
    fn test_csv_writer_missing_field_emits_empty_cell() {
        let writer_schema = make_schema(&["a", "b", "c"]);
        let record_schema = make_schema(&["c", "a"]);
        let record = make_record(
            &record_schema,
            vec![Value::String("C".into()), Value::String("A".into())],
        );
        let output = write_to_string(&writer_schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "a,b,c\nA,,C\n");
    }

    /// Engine-stamped stripping is keyed off the writer's pinned
    /// schema — the same schema the header filter consults — so a
    /// record schema lacking the stamp metadata cannot smuggle the
    /// column's value into the body row.
    #[test]
    fn test_csv_writer_engine_stamp_filter_uses_writer_schema() {
        let writer_schema = make_schema_with_engine_stamp("id", "$ck.id");
        // Same columns, but this record's schema carries no metadata.
        let record_schema = make_schema(&["id", "$ck.id"]);
        let record = make_record(&record_schema, vec![Value::Integer(7), Value::Integer(7)]);
        let output = write_to_string(&writer_schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "id\n7\n");
    }

    /// Record fields the writer schema does not declare are not
    /// emitted — the pinned schema is the output contract.
    #[test]
    fn test_csv_writer_ignores_fields_outside_writer_schema() {
        let writer_schema = make_schema(&["a", "b"]);
        let record_schema = make_schema(&["a", "extra", "b"]);
        let record = make_record(
            &record_schema,
            vec![
                Value::String("A".into()),
                Value::String("X".into()),
                Value::String("B".into()),
            ],
        );
        let output = write_to_string(&writer_schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "a,b\nA,B\n");
    }

    #[test]
    fn test_csv_writer_quoting_special_chars() {
        let schema = make_schema(&["name", "bio"]);
        let records = vec![make_record(
            &schema,
            vec![
                Value::String("Alice".into()),
                Value::String("Likes commas, and\nnewlines".into()),
            ],
        )];
        let output = write_to_string(&schema, CsvWriterConfig::default(), &records);
        // csv crate should quote the field containing comma and newline
        assert!(output.contains("\"Likes commas, and\nnewlines\""));
    }

    #[test]
    fn test_csv_roundtrip_lossless() {
        let input = "name,age,active\nAlice,30,true\nBob,25,false\nCharlie,35,true\n";

        // Read
        let mut reader = CsvReader::from_reader(input.as_bytes(), CsvReaderConfig::default());
        let schema = reader.schema().unwrap();
        let mut records = Vec::new();
        while let Some(r) = reader.next_record().unwrap() {
            records.push(r);
        }
        assert_eq!(records.len(), 3);

        // Write
        let output = write_to_string(&schema, CsvWriterConfig::default(), &records);

        // Read again
        let mut reader2 = CsvReader::from_reader(output.as_bytes(), CsvReaderConfig::default());
        let schema2 = reader2.schema().unwrap();
        let mut records2 = Vec::new();
        while let Some(r) = reader2.next_record().unwrap() {
            records2.push(r);
        }

        // Schemas match
        assert_eq!(schema.columns(), schema2.columns());

        // Records match field by field
        assert_eq!(records.len(), records2.len());
        for (r1, r2) in records.iter().zip(records2.iter()) {
            for col in schema.columns() {
                assert_eq!(r1.get(col), r2.get(col), "mismatch on column {col}");
            }
        }
    }

    /// CSV writer rejects `Value::Map` payloads with
    /// `FormatError::UnserializableMapValue`. The pre-walk in
    /// `write_record` catches the misroute (e.g. a `$widened`
    /// sidecar reaching the writer without `include_unmapped: true`
    /// expansion) before the value-to-cell function silently
    /// JSON-encodes the map into a single CSV cell.
    #[test]
    fn test_csv_writer_rejects_map_value() {
        use indexmap::IndexMap;
        let schema = make_schema(&["id", "payload"]);
        let mut sidecar: IndexMap<Box<str>, Value> = IndexMap::new();
        sidecar.insert("a".into(), Value::Integer(1));
        sidecar.insert("b".into(), Value::String("two".into()));
        let record = make_record(
            &schema,
            vec![Value::Integer(7), Value::Map(Box::new(sidecar))],
        );
        let mut buf = Vec::new();
        let mut writer = CsvWriter::new(&mut buf, Arc::clone(&schema), CsvWriterConfig::default());
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::UnserializableMapValue { format, column } => {
                assert_eq!(format, "CSV");
                assert_eq!(column, "payload");
            }
            other => panic!("expected UnserializableMapValue, got {other:?}"),
        }
    }

    use crate::envelope_writer::test_doc_with_sections as doc_with_sections;

    #[test]
    fn csv_envelope_frames_header_body_footer() {
        let schema = make_schema(&["amount"]);
        let config = CsvWriterConfig {
            include_header: false,
            envelope: Some(OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                footer_from_doc: Some("Foot".into()),
                footer_record_count_field: Some("count".into()),
            }),
            ..Default::default()
        };
        let doc = doc_with_sections(&[
            ("Head", &[("batch_id", Value::String("A".into()))]),
            ("Foot", &[("checksum", Value::String("SUM".into()))]),
        ]);
        let mut buf = Vec::new();
        {
            let mut writer = CsvWriter::new(&mut buf, Arc::clone(&schema), config);
            writer.begin_document(&doc).unwrap();
            writer
                .write_record(&make_record(&schema, vec![Value::Integer(10)]))
                .unwrap();
            writer
                .write_record(&make_record(&schema, vec![Value::Integer(20)]))
                .unwrap();
            writer.end_document(&doc).unwrap();
            writer.flush().unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        assert_eq!(out, "A\n10\n20\nSUM,2\n", "got: {out}");
    }

    #[test]
    fn csv_envelope_off_is_byte_identical() {
        // No envelope spec: begin/end_document are no-ops and the body is the
        // plain CSV the boundary-unaware path produces.
        let schema = make_schema(&["amount"]);
        let doc = doc_with_sections(&[("Head", &[("batch_id", Value::String("A".into()))])]);
        let mut buf = Vec::new();
        {
            let mut writer = CsvWriter::new(
                &mut buf,
                Arc::clone(&schema),
                CsvWriterConfig {
                    include_header: false,
                    ..Default::default()
                },
            );
            writer.begin_document(&doc).unwrap();
            writer
                .write_record(&make_record(&schema, vec![Value::Integer(10)]))
                .unwrap();
            writer.end_document(&doc).unwrap();
            writer.flush().unwrap();
        }
        assert_eq!(String::from_utf8(buf).unwrap(), "10\n");
    }
}
