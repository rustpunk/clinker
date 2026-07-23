use std::io::Write;
use std::sync::{Arc, Mutex};

use clinker_record::{DocumentContext, Record, Schema, Value};

use crate::envelope_writer::{EnvelopeFramer, OutputEnvelopeSpec};
use crate::error::FormatError;
use crate::json::writer::clinker_to_json;
use crate::multi_value::{JoinValues, OnConflict};
use crate::schema::DEFAULT_VALUE_DELIMITER;
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
    /// Raise [`FormatError::SchemaDrift`] when a record carries a user column
    /// the pinned schema does not name, instead of silently skipping it.
    ///
    /// Set by the executor to the Output's `include_unmapped`: when the user
    /// asked to carry every column through (`include_unmapped: true`), a
    /// record column with no header slot is a data-loss drift that must fail
    /// loudly. The buffered Output arm pre-widens the header to the batch
    /// union, so this guard is a no-op there; it earns its keep on the
    /// bounded-memory paths that pin the header to the first record (the
    /// streaming fused arm and envelope framing), where a union is impossible.
    ///
    /// Left `false` (the default) the writer keeps the "output schema is the
    /// column contract — extra record fields are not written" behavior, which
    /// `include_unmapped: false` relies on to narrow output deliberately.
    pub error_on_undeclared_columns: bool,
    /// Per-column overrides for how a `multiple:` field is joined into one
    /// delimited cell. A `Value::Array` at any column joins with the default
    /// delimiter `;` and `on_conflict: error` unless an entry here names the
    /// column, mirroring the source-side `split_values`. Empty by default;
    /// populated from the output's `join_values`.
    pub join_values: Vec<JoinValues>,
}

impl Default for CsvWriterConfig {
    fn default() -> Self {
        Self {
            delimiter: b',',
            include_header: true,
            include_engine_stamped: false,
            envelope: None,
            error_on_undeclared_columns: false,
            join_values: Vec::new(),
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
    /// Indices into `schema.columns()` of the columns actually emitted (after
    /// engine-stamped filtering), computed once at construction. Both the
    /// header row and every body row derive their column set from this, so no
    /// per-record filtering allocation is needed.
    column_indices: Vec<usize>,
    /// Reusable per-record cell buffer, one `String` per emitted column. Each
    /// row clears and rewrites these in place, retaining their capacity across
    /// records rather than allocating a fresh row vector every record.
    row_buf: Vec<String>,
    /// Per-emitted-column `join_values` override, index-aligned to
    /// `column_indices`: `Some(entry)` for a column a `join_values` entry names,
    /// `None` otherwise. Built once at construction so a body row's array cell is
    /// a positional lookup rather than a name scan over `config.join_values` per
    /// column per record (mirrors the CSV reader's `split_specs`).
    join_overrides: Vec<Option<JoinValues>>,
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
        let column_indices = filtered_column_indices(&schema, config.include_engine_stamped);
        let row_buf = vec![String::new(); column_indices.len()];
        let join_overrides: Vec<Option<JoinValues>> = column_indices
            .iter()
            .map(|&idx| {
                let name = schema.columns()[idx].as_ref();
                config.join_values.iter().find(|j| j.field == name).cloned()
            })
            .collect();
        Self {
            inner,
            schema,
            config,
            header_written: false,
            column_indices,
            row_buf,
            join_overrides,
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
        // In lossless mode (`include_unmapped: true`), a record column with no
        // header slot is a data-loss drift that must fail loudly rather than
        // be silently written narrower (issue #805). This never trips on the
        // buffered Output arm — its header is the batch union of every
        // record's columns — but it is the loud-drift backstop on the
        // bounded-memory paths that pin the header to the first record (the
        // streaming fused arm and envelope framing), where a union cannot be
        // pre-scanned. `iter_user_fields` excludes engine stamps and the
        // `$widened` sidecar, so only a genuine undeclared column trips it.
        // When the flag is off, the pinned schema stays a deliberate column
        // contract (extra fields are not written) — the `include_unmapped:
        // false` narrowing behavior.
        if self.config.error_on_undeclared_columns {
            for (name, _) in record.iter_user_fields() {
                if !self.schema.contains(name) {
                    return Err(FormatError::SchemaDrift {
                        format: "CSV",
                        column: name.to_string(),
                    });
                }
            }
        }

        // Header is built from the writer's pinned schema. Engine-stamped
        // columns (today: `$ck.<field>`) are stripped unless the Output
        // node opts in.
        //
        // Under envelope framing the schema column-header row is SUPPRESSED:
        // it would otherwise emit once, before the first document's envelope
        // header row, giving documents inconsistent layouts (header row only
        // on the first). The per-document envelope header section replaces it.
        if self.config.include_header && !self.header_written && self.framer.is_none() {
            // One-time header row, derived from the precomputed column indices.
            let header: Vec<&str> = self
                .column_indices
                .iter()
                .map(|&i| self.schema.columns()[i].as_ref())
                .collect();
            self.inner.write_record(&header)?;
            self.header_written = true;
        }

        // Body cells follow the writer's pinned schema — the same column set
        // and order the header row was built from — not the record's own
        // schema, which an upstream projection or widening may order
        // differently for the same fields. Fields the record lacks emit an
        // empty cell (an explicit Null), matching the fixed-width writer's
        // missing-field policy. Each cell is written into the reusable row
        // buffer (cleared first, so a failed map cell never leaves stale bytes).
        let Self {
            inner,
            schema,
            column_indices,
            row_buf,
            join_overrides,
            framer,
            ..
        } = self;
        let columns = schema.columns();
        for ((slot, &idx), join) in row_buf
            .iter_mut()
            .zip(column_indices.iter())
            .zip(join_overrides.iter())
        {
            let col = columns[idx].as_ref();
            slot.clear();
            if let Some(v) = record.get(col) {
                write_csv_cell(slot, col, v, join.as_ref())?;
            }
        }

        inner.write_record(&*row_buf)?;
        if let Some(framer) = framer.as_mut() {
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

    /// Forward the non-finalizing drain to the inner CSV writer so byte-limit
    /// split accounting sees the true on-disk size (see the wrapper-delegation
    /// contract on [`FormatWriter::flush_bytes`]).
    fn flush_bytes(&mut self) -> Result<(), FormatError> {
        self.inner.flush_bytes()
    }

    /// Forward per-document opening framing to the inner CSV writer; without
    /// this the envelope header row would be silently dropped when a split CSV
    /// output reconstructs an envelope.
    fn begin_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        self.inner.begin_document(doc)
    }

    /// Forward per-document closing framing to the inner CSV writer; see
    /// [`Self::begin_document`].
    fn end_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        self.inner.end_document(doc)
    }

    fn bytes_written(&self) -> Option<u64> {
        self.inner.bytes_written()
    }
}

/// The indices into `schema.columns()` of the columns the CSV writer emits,
/// after filtering engine-stamped columns unless opted in. Computed once and
/// reused for both the header row and every body row.
fn filtered_column_indices(schema: &Arc<Schema>, include_engine_stamped: bool) -> Vec<usize> {
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
        .map(|(i, _)| i)
        .collect()
}

/// Serialize a Value into a CSV cell string for an envelope `$doc` section row
/// (not the record hot path). Envelope section values are document metadata, not
/// `multiple:` data columns, so this renders scalars and rejects an `Array`/`Map`
/// loudly via [`write_scalar_cell`] — a section value that is a collection is a
/// misroute, and joining it silently would ship malformed envelope output.
fn value_to_csv_cell(col: &str, value: &Value) -> Result<String, FormatError> {
    let mut buf = String::new();
    write_scalar_cell(&mut buf, col, value)?;
    Ok(buf)
}

/// Append a Value's CSV cell rendering to `buf`. Sharing one renderer keeps the
/// record hot path and the envelope section path byte-identical.
///
/// A `Value::Array` is a `multiple:` field: it is JOINED into one delimited cell
/// per the column's `join_values` policy (`join`, defaulting to `;` /
/// `on_conflict: error` when the column has no entry). See [`write_joined_cell`].
///
/// `Value::Map` still has no canonical scalar serialization for a CSV cell
/// (silently JSON-encoding it hides routing bugs — e.g. a `$widened` sidecar
/// reaching the writer without the projection layer's `include_unmapped: true`
/// expansion), so it returns `FormatError::UnserializableMapValue`. CSV is the
/// single point of truth for map rejection on this path; there is no upstream
/// pre-walk.
fn write_csv_cell(
    buf: &mut String,
    col: &str,
    value: &Value,
    join: Option<&JoinValues>,
) -> Result<(), FormatError> {
    match value {
        Value::Array(items) => write_joined_cell(buf, col, value, items, join),
        Value::Map(_) => Err(FormatError::UnserializableMapValue {
            format: "CSV",
            column: col.to_string(),
        }),
        scalar => write_scalar_cell(buf, col, scalar),
    }
}

/// Render one scalar `Value` into a CSV cell. A `Value::Array`/`Value::Map`
/// reaching here is a nested element inside a `multiple:` field (a flat cell
/// cannot hold nested structure) and is rejected — the same misroute detection
/// the top-level cell renderer applies.
fn write_scalar_cell(buf: &mut String, col: &str, value: &Value) -> Result<(), FormatError> {
    use std::fmt::Write as _;
    match value {
        Value::Null => {}
        Value::Bool(b) => buf.push_str(if *b { "true" } else { "false" }),
        Value::Integer(n) => {
            let _ = write!(buf, "{n}");
        }
        Value::Float(f) => {
            let _ = write!(buf, "{f}");
        }
        // A decimal carries its own (column) scale; Display preserves it.
        Value::Decimal(d) => {
            let _ = write!(buf, "{d}");
        }
        Value::String(s) => buf.push_str(s.as_str()),
        Value::Date(d) => {
            let _ = write!(buf, "{}", d.format("%Y-%m-%d"));
        }
        // `%.f` appends the fractional second only when the sub-second field is
        // non-zero, and trims to 3/6/9 digits — so a whole-second datetime stays
        // byte-identical to the plain `%Y-%m-%dT%H:%M:%S` rendering while
        // millisecond/microsecond/nanosecond precision survives to text (#883).
        Value::DateTime(dt) => {
            let _ = write!(buf, "{}", dt.format("%Y-%m-%dT%H:%M:%S%.f"));
        }
        Value::Array(_) => {
            return Err(FormatError::UnserializableArrayValue {
                format: "CSV",
                column: col.to_string(),
            });
        }
        Value::Map(_) => {
            return Err(FormatError::UnserializableMapValue {
                format: "CSV",
                column: col.to_string(),
            });
        }
    }
    Ok(())
}

/// Join a `multiple:` field's values into one delimited CSV cell.
///
/// `join` supplies the delimiter, collision policy, and escape character; when
/// the column has no `join_values` entry the defaults apply (`;`,
/// `on_conflict: error`, backslash escape). The three policies:
///
/// - `error`: if any rendered value contains the delimiter, refuse the record
///   with [`FormatError::MultiValueDelimiterCollision`] (routed to the DLQ by
///   the sink) rather than emit a cell that would split back wrongly.
/// - `escape`: escape both the delimiter and the escape character inside each
///   value, invertible by a matching `split_values` `escape:`.
/// - `encode_json`: emit the whole field as an embedded JSON array (via the
///   shared [`clinker_to_json`]), recovered by a matching `split_values`
///   `json: true`. Lossless for any value.
///
/// Under `error` and `escape` a nested `Value::Array`/`Value::Map` element is
/// rejected — a flat delimited cell cannot hold nested structure. `encode_json`
/// serializes nested structure as nested JSON, so it accepts it.
fn write_joined_cell(
    buf: &mut String,
    col: &str,
    value: &Value,
    items: &[Value],
    join: Option<&JoinValues>,
) -> Result<(), FormatError> {
    let delimiter = join.map_or(DEFAULT_VALUE_DELIMITER, |j| j.delimiter.as_str());
    let on_conflict = join.map_or(OnConflict::Error, |j| j.on_conflict);
    let escape = join.map_or("\\", |j| j.escape.as_str());

    if on_conflict == OnConflict::EncodeJson {
        // The whole field becomes a JSON array. `clinker_to_json` walks the
        // array (rejecting a non-finite float the same way the JSON writer
        // does); a nested element serializes as nested JSON, which is lossless.
        let json = clinker_to_json(value)?;
        let text = serde_json::to_string(&json).map_err(|e| {
            FormatError::Json(format!("encode_json for column {col:?} failed: {e}"))
        })?;
        buf.push_str(&text);
        return Ok(());
    }

    let mut elem = String::new();
    // `encode_json` returned above, so only `escape` and `error` reach the join
    // loop. `escape` rewrites each value into `escaped`; `error` uses the value
    // verbatim after checking for a delimiter collision.
    let escaping = on_conflict == OnConflict::Escape;
    let mut escaped = String::new();
    for (i, item) in items.iter().enumerate() {
        elem.clear();
        write_scalar_cell(&mut elem, col, item)?;
        let piece: &str = if escaping {
            escaped.clear();
            escape_into(&elem, delimiter, escape, &mut escaped);
            escaped.as_str()
        } else if elem.contains(delimiter) {
            return Err(FormatError::MultiValueDelimiterCollision {
                format: "CSV",
                column: col.to_string(),
                value: elem,
            });
        } else {
            elem.as_str()
        };
        if i > 0 {
            buf.push_str(delimiter);
        }
        buf.push_str(piece);
    }
    Ok(())
}

/// Append `text` to `out`, prefixing each delimiter and each escape character
/// with the escape character. The inverse of the reader's un-escaping split, so
/// a value carrying the delimiter (or the escape char itself) round-trips
/// exactly. Delimiter and escape are treated as single characters (their first
/// `char`); the plan-time gate rejects a multi-character escape.
fn escape_into(text: &str, delimiter: &str, escape: &str, out: &mut String) {
    let (Some(esc), Some(delim)) = (escape.chars().next(), delimiter.chars().next()) else {
        out.push_str(text);
        return;
    };
    for c in text.chars() {
        if c == esc || c == delim {
            out.push(esc);
        }
        out.push(c);
    }
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

    /// Sub-second datetimes must carry their fractional part into CSV text. The
    /// prior whole-second `%Y-%m-%dT%H:%M:%S` rendering silently dropped
    /// millisecond/microsecond/nanosecond precision (#883). `%.f` emits nothing
    /// for a whole-second value (nanos=0 stays byte-identical) and otherwise
    /// trims to a 3/6/9-digit group, so each precision tier renders exactly.
    #[test]
    fn test_csv_writer_datetime_subsecond_preserved() {
        use chrono::NaiveDate;
        let schema = make_schema(&["ts"]);
        let base = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let cases = [
            (0, "2024-01-15T10:30:00"),
            (123_000_000, "2024-01-15T10:30:00.123"),
            (123_456_000, "2024-01-15T10:30:00.123456"),
            (123_456_789, "2024-01-15T10:30:00.123456789"),
            (1, "2024-01-15T10:30:00.000000001"),
        ];
        for (nanos, expected) in cases {
            let dt = base.and_hms_nano_opt(10, 30, 0, nanos).unwrap();
            let record = make_record(&schema, vec![Value::DateTime(dt)]);
            let output = write_to_string(&schema, CsvWriterConfig::default(), &[record]);
            assert_eq!(output, format!("ts\n{expected}\n"), "nanos={nanos}");
        }
    }

    /// The emitted sub-second text is round-trippable: reading the CSV cell back
    /// out and parsing it with a fractional-second-aware pattern reconstructs
    /// the exact `NaiveDateTime`, nanoseconds intact.
    #[test]
    fn test_csv_writer_datetime_subsecond_roundtrips() {
        use chrono::{NaiveDate, NaiveDateTime};
        let schema = make_schema(&["ts"]);
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_nano_opt(10, 30, 0, 123_456_789)
            .unwrap();
        let record = make_record(&schema, vec![Value::DateTime(dt)]);
        let output = write_to_string(&schema, CsvWriterConfig::default(), &[record]);

        // Read the single data cell back out through the CSV reader (which
        // yields the raw text as a string — no typed coercion on this path).
        let mut reader = CsvReader::from_reader(output.as_bytes(), CsvReaderConfig::default());
        let _ = reader.schema().unwrap();
        let row = reader.next_record().unwrap().unwrap();
        let cell = match row.get("ts").unwrap() {
            Value::String(s) => s.as_str().to_owned(),
            other => panic!("expected string cell, got {other:?}"),
        };

        let parsed = NaiveDateTime::parse_from_str(&cell, "%Y-%m-%dT%H:%M:%S%.f").unwrap();
        assert_eq!(parsed, dt);
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

    /// The reusable row buffer must be cleared per cell, so a short value
    /// following a long value in the same column carries no leftover bytes —
    /// the canonical buffer-reuse bug.
    #[test]
    fn test_csv_writer_row_buffer_reuse_no_stale_bytes() {
        let schema = make_schema(&["v", "w"]);
        let records = vec![
            make_record(
                &schema,
                vec![
                    Value::String("a-very-long-value-here".into()),
                    Value::String("xxxxxxxx".into()),
                ],
            ),
            make_record(
                &schema,
                vec![Value::String("hi".into()), Value::String("y".into())],
            ),
        ];
        let output = write_to_string(&schema, CsvWriterConfig::default(), &records);
        assert_eq!(output, "v,w\na-very-long-value-here,xxxxxxxx\nhi,y\n");
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

    use crate::multi_value::{JoinValues, OnConflict};

    fn join_config(entries: Vec<JoinValues>) -> CsvWriterConfig {
        CsvWriterConfig {
            join_values: entries,
            ..Default::default()
        }
    }

    /// With no `join_values` entry, a `Value::Array` of scalars joins into one
    /// cell with the default `;` delimiter — AC#1, the write-side inverse of the
    /// reader's default split, requiring no configuration.
    #[test]
    fn join_values_default_semicolon_no_config() {
        let schema = make_schema(&["id", "tags"]);
        let record = make_record(
            &schema,
            vec![
                Value::Integer(7),
                Value::Array(vec![
                    Value::String("a".into()),
                    Value::String("b".into()),
                    Value::String("c".into()),
                ]),
            ],
        );
        let output = write_to_string(&schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "id,tags\n7,a;b;c\n");
    }

    /// An empty array emits an empty cell; a one-element array emits that value
    /// with no delimiter (AC#5). A second column keeps the empty cell
    /// unambiguous (a lone empty field on a one-column row is CSV-quoted `""`).
    #[test]
    fn join_values_empty_and_single() {
        let schema = make_schema(&["id", "tags"]);
        let empty = make_record(&schema, vec![Value::Integer(1), Value::Array(Vec::new())]);
        let single = make_record(
            &schema,
            vec![
                Value::Integer(2),
                Value::Array(vec![Value::String("solo".into())]),
            ],
        );
        let output = write_to_string(&schema, CsvWriterConfig::default(), &[empty, single]);
        assert_eq!(output, "id,tags\n1,\n2,solo\n");
    }

    /// A custom per-column delimiter is honored.
    #[test]
    fn join_values_custom_delimiter() {
        let schema = make_schema(&["codes"]);
        let record = make_record(
            &schema,
            vec![Value::Array(vec![
                Value::String("x".into()),
                Value::String("y".into()),
            ])],
        );
        let config = join_config(vec![JoinValues {
            field: "codes".into(),
            delimiter: "|".into(),
            on_conflict: OnConflict::Error,
            escape: "\\".into(),
            repeat_as: None,
            wrap_in: None,
        }]);
        let output = write_to_string(&schema, config, &[record]);
        assert_eq!(output, "codes\nx|y\n");
    }

    /// The central AC#2 test at the writer boundary: a value that contains the
    /// delimiter under `on_conflict: error` refuses the record with a
    /// `MultiValueDelimiterCollision` naming the field and the offending value,
    /// and no corrupted body cell is emitted.
    #[test]
    fn join_values_on_conflict_error_names_field_and_value() {
        let schema = make_schema(&["tags"]);
        let record = make_record(
            &schema,
            vec![Value::Array(vec![
                Value::String("a;b".into()),
                Value::String("c".into()),
            ])],
        );
        let mut buf = Vec::new();
        let mut writer = CsvWriter::new(&mut buf, Arc::clone(&schema), CsvWriterConfig::default());
        let err = writer.write_record(&record).unwrap_err();
        assert!(err.is_join_collision());
        match err {
            FormatError::MultiValueDelimiterCollision {
                format,
                column,
                value,
            } => {
                assert_eq!(format, "CSV");
                assert_eq!(column, "tags");
                assert_eq!(value, "a;b");
            }
            other => panic!("expected MultiValueDelimiterCollision, got {other:?}"),
        }
        drop(writer);
        assert!(
            !String::from_utf8_lossy(&buf).contains("a;b"),
            "no corrupted cell must be emitted for the failing record"
        );
    }

    /// `on_conflict: escape` escapes the delimiter (and the escape char itself),
    /// and the CSV reader's matching `split_values` `escape:` recovers the exact
    /// original values — a full round trip (AC#3).
    #[test]
    fn join_values_escape_round_trips() {
        let schema = make_schema(&["tags"]);
        let original = vec![
            Value::String("a;b".into()),
            Value::String(r"c\d".into()),
            Value::String("e".into()),
        ];
        let record = make_record(&schema, vec![Value::Array(original.clone())]);
        let config = join_config(vec![JoinValues {
            field: "tags".into(),
            delimiter: ";".into(),
            on_conflict: OnConflict::Escape,
            escape: "\\".into(),
            repeat_as: None,
            wrap_in: None,
        }]);
        let output = write_to_string(&schema, config, &[record]);

        let read_config = CsvReaderConfig {
            split_values: vec![crate::multi_value::SplitValues {
                field: "tags".into(),
                delimiter: ";".into(),
                escape: "\\".into(),
                json: false,
            }],
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(output.as_bytes(), read_config);
        reader.schema().unwrap();
        let back = reader.next_record().unwrap().unwrap();
        assert_eq!(back.get("tags"), Some(&Value::Array(original)));
    }

    /// `on_conflict: encode_json` round-trips exactly, including values carrying
    /// the delimiter, quotes, and newlines (AC#4).
    #[test]
    fn join_values_encode_json_round_trips() {
        let schema = make_schema(&["payload"]);
        let original = vec![
            Value::String("a;b".into()),
            Value::String("c\"d".into()),
            Value::String("e\nf".into()),
        ];
        let record = make_record(&schema, vec![Value::Array(original.clone())]);
        let config = join_config(vec![JoinValues {
            field: "payload".into(),
            delimiter: ";".into(),
            on_conflict: OnConflict::EncodeJson,
            escape: "\\".into(),
            repeat_as: None,
            wrap_in: None,
        }]);
        let output = write_to_string(&schema, config, &[record]);

        let read_config = CsvReaderConfig {
            split_values: vec![crate::multi_value::SplitValues {
                field: "payload".into(),
                delimiter: ";".into(),
                escape: String::new(),
                json: true,
            }],
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(output.as_bytes(), read_config);
        reader.schema().unwrap();
        let back = reader.next_record().unwrap().unwrap();
        assert_eq!(back.get("payload"), Some(&Value::Array(original)));
    }

    /// AC#6 interaction: a value containing BOTH the intra-cell delimiter and the
    /// CSV field delimiter is handled correctly under each policy. Under
    /// `escape` the cell is CSV-quoted (it holds a comma) yet still recovers.
    #[test]
    fn join_values_interaction_with_csv_field_delimiter() {
        let schema = make_schema(&["tags"]);
        let original = vec![Value::String("a,b;c".into()), Value::String("d".into())];
        let record = make_record(&schema, vec![Value::Array(original.clone())]);

        // error: the ';' inside "a,b;c" collides.
        let mut buf = Vec::new();
        let mut w = CsvWriter::new(&mut buf, Arc::clone(&schema), CsvWriterConfig::default());
        assert!(w.write_record(&record).unwrap_err().is_join_collision());

        // escape: round-trips through the reader despite the CSV comma-quoting.
        let config = join_config(vec![JoinValues {
            field: "tags".into(),
            delimiter: ";".into(),
            on_conflict: OnConflict::Escape,
            escape: "\\".into(),
            repeat_as: None,
            wrap_in: None,
        }]);
        let out = write_to_string(&schema, config, &[record]);
        let read_config = CsvReaderConfig {
            split_values: vec![crate::multi_value::SplitValues {
                field: "tags".into(),
                delimiter: ";".into(),
                escape: "\\".into(),
                json: false,
            }],
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(out.as_bytes(), read_config);
        reader.schema().unwrap();
        let back = reader.next_record().unwrap().unwrap();
        assert_eq!(back.get("tags"), Some(&Value::Array(original)));
    }

    /// A single empty-string value `[""]` is indistinguishable from an empty
    /// field under the delimited policies (both emit an empty cell, which reads
    /// back as zero values); `encode_json` preserves it. This pins the documented
    /// limitation so a future change to the empty-cell contract is deliberate.
    #[test]
    fn join_values_single_empty_string_collapses_under_delimited_but_not_json() {
        let schema = make_schema(&["id", "tags"]);
        let record = make_record(
            &schema,
            vec![
                Value::Integer(1),
                Value::Array(vec![Value::String("".into())]),
            ],
        );
        // Delimited (default error): empty cell, reads back as [] (0 values).
        let delimited = write_to_string(
            &schema,
            CsvWriterConfig::default(),
            std::slice::from_ref(&record),
        );
        assert_eq!(delimited, "id,tags\n1,\n");

        // encode_json: [""], distinguishable from an empty field.
        let json_cfg = join_config(vec![JoinValues {
            field: "tags".into(),
            delimiter: ";".into(),
            on_conflict: OnConflict::EncodeJson,
            escape: "\\".into(),
            repeat_as: None,
            wrap_in: None,
        }]);
        let json = write_to_string(&schema, json_cfg, &[record]);
        assert_eq!(json, "id,tags\n1,\"[\"\"\"\"]\"\n");
    }

    /// An envelope `$doc` section value that is an array is rejected loudly (it
    /// is document metadata, not a `multiple:` data column) — it must not be
    /// silently joined the way a record cell is.
    #[test]
    fn envelope_section_cell_rejects_an_array() {
        let err = value_to_csv_cell("checksum", &Value::Array(vec![Value::String("a".into())]))
            .unwrap_err();
        match err {
            FormatError::UnserializableArrayValue { format, column } => {
                assert_eq!(format, "CSV");
                assert_eq!(column, "checksum");
            }
            other => panic!("expected UnserializableArrayValue, got {other:?}"),
        }
    }

    /// A `Value::Array` carrying a nested `Array`/`Map` element is still
    /// rejected — a flat cell cannot hold nested structure, so the misroute
    /// detection is not lost when scalar arrays start joining.
    #[test]
    fn join_values_rejects_nested_element() {
        let schema = make_schema(&["tags"]);
        let record = make_record(
            &schema,
            vec![Value::Array(vec![
                Value::String("a".into()),
                Value::Array(vec![Value::String("nested".into())]),
            ])],
        );
        let mut buf = Vec::new();
        let mut writer = CsvWriter::new(&mut buf, Arc::clone(&schema), CsvWriterConfig::default());
        match writer.write_record(&record).unwrap_err() {
            FormatError::UnserializableArrayValue { column, .. } => assert_eq!(column, "tags"),
            other => panic!("expected UnserializableArrayValue for nested element, got {other:?}"),
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

    /// `HeaderCapturingCsvWriter` must forward `begin_document`/`end_document`
    /// to its inner CSV writer: with an envelope spec the inner writer emits a
    /// header row on begin and a footer row on end, so the wrapped output must
    /// match the un-wrapped enveloped writer's framing rather than dropping it.
    #[test]
    fn header_capturing_csv_writer_forwards_document_framing() {
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
            let inner = CsvWriter::new(&mut buf, Arc::clone(&schema), config);
            let shared_header = Arc::new(Mutex::new(None));
            let mut writer =
                HeaderCapturingCsvWriter::new(inner, Arc::clone(&schema), shared_header);
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
        assert_eq!(String::from_utf8(buf).unwrap(), "A\n10\n20\nSUM,2\n");
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

    /// In lossless mode (`error_on_undeclared_columns: true`, mirroring
    /// `include_unmapped: true`) a record carrying a user column the pinned
    /// schema lacks raises SchemaDrift rather than silently writing a narrower
    /// row. This is the bounded-memory backstop for paths that pin the header
    /// to the first record and cannot pre-scan a union (issue #805).
    #[test]
    fn csv_undeclared_column_is_schema_drift_in_lossless_mode() {
        let writer_schema = make_schema(&["amount"]);
        let config = CsvWriterConfig {
            error_on_undeclared_columns: true,
            ..Default::default()
        };
        let mut buf = Vec::new();
        let mut writer = CsvWriter::new(&mut buf, Arc::clone(&writer_schema), config);
        // Record carries a `region` column the pinned schema lacks.
        let drift_schema = make_schema(&["amount", "region"]);
        let record = make_record(
            &drift_schema,
            vec![Value::Integer(10), Value::String("US".into())],
        );
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::SchemaDrift { format, column } => {
                assert_eq!(format, "CSV");
                assert_eq!(column, "region");
            }
            other => panic!("expected SchemaDrift, got {other:?}"),
        }
    }

    /// The same guard fires under envelope framing (the header is suppressed
    /// and the body streams headerless, so a union is impossible there too).
    #[test]
    fn csv_envelope_schema_drift_is_loud() {
        let writer_schema = make_schema(&["amount"]);
        let config = CsvWriterConfig {
            include_header: false,
            error_on_undeclared_columns: true,
            envelope: Some(OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                footer_from_doc: None,
                footer_record_count_field: None,
            }),
            ..Default::default()
        };
        let doc = doc_with_sections(&[("Head", &[("batch_id", Value::String("A".into()))])]);
        let mut buf = Vec::new();
        let mut writer = CsvWriter::new(&mut buf, Arc::clone(&writer_schema), config);
        writer.begin_document(&doc).unwrap();
        let drift_schema = make_schema(&["amount", "region"]);
        let record = make_record(
            &drift_schema,
            vec![Value::Integer(10), Value::String("US".into())],
        );
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::SchemaDrift { column, .. } => assert_eq!(column, "region"),
            other => panic!("expected SchemaDrift, got {other:?}"),
        }
    }

    /// A record whose columns are all within the pinned schema writes normally
    /// even in lossless mode — the drift guard is not a false positive on the
    /// no-drift case (including a record missing a declared column).
    #[test]
    fn csv_lossless_mode_no_drift_writes_normally() {
        let schema = make_schema(&["a", "b"]);
        let config = CsvWriterConfig {
            error_on_undeclared_columns: true,
            ..Default::default()
        };
        // Second record is missing `b` — a legitimate absent (empty) cell, not
        // drift.
        let records = vec![
            make_record(
                &schema,
                vec![Value::String("A".into()), Value::String("B".into())],
            ),
            make_record(&make_schema(&["a"]), vec![Value::String("A2".into())]),
        ];
        let output = write_to_string(&schema, config, &records);
        assert_eq!(output, "a,b\nA,B\nA2,\n");
    }

    /// With the guard off (the `include_unmapped: false` narrowing contract),
    /// a record field outside the pinned schema is silently not written — the
    /// output schema stays the deliberate column contract.
    #[test]
    fn csv_undeclared_column_dropped_when_guard_off() {
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
        // Default config: error_on_undeclared_columns is false.
        let output = write_to_string(&writer_schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "a,b\nA,B\n");
    }
}
