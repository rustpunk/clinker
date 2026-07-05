//! HL7 v2 file writer.
//!
//! Reconstructs an HL7 v2 file around emitted body records: an optional
//! `FHS` file header (literal config fields or echoed from a `$doc`
//! section), an optional `BHS` batch header, the `MSH..` messages
//! themselves (re-emitted from the record stream), and — when an envelope
//! was configured — closing `BTS`/`FTS` trailers with recomputed counts.
//! Record columns map positionally: `seg_id` is the segment tag, `f01..`
//! are the data fields. Field data is escaped on output and decoded by the
//! reader, so a reader → writer → reader round-trip is byte-faithful even
//! for values that carry delimiter characters as literal data.
//!
//! HL7 messages have no per-message trailer segment — the next `MSH` (or a
//! batch/file trailer, or end of input) is the message boundary — so the
//! writer simply re-emits each segment in order, tracking message and batch
//! counts for the optional `BTS`/`FTS` trailers. The `MSH` header segment
//! arrives as a body record (the reader emits it), so the writer writes it
//! verbatim rather than synthesizing one; its `MSH-2` encoding-characters
//! field is written without escaping, mirroring the reader's verbatim
//! treatment.
//!
//! Memory model: streaming and O(1) in held state — only the current
//! message and batch counts are retained. `flush` is the single
//! end-of-stream finalizer: it writes the `BTS`/`FTS` trailers (when an
//! envelope was opened) exactly once. A file is one envelope, so byte-limit
//! file splitting (which would flush — and thus finalize — mid-stream) is
//! rejected for HL7 outputs at config-validation time; this writer is
//! therefore only ever flushed at true end of stream.

use std::io::Write;
use std::sync::Arc;

use clinker_record::{Record, Schema, Value};

use crate::error::FormatError;
use crate::hl7::RAW_FIELDS_KEY;
use crate::hl7::field_split::{SplitCoord, parse_split_column, reassemble_field};
use crate::hl7::tokenizer::Delimiters;
use crate::traits::FormatWriter;

/// Configuration for the HL7 writer.
#[derive(Clone, Default)]
pub struct Hl7WriterConfig {
    /// Literal `FHS` data fields written verbatim as a file header. When
    /// set, the writer opens the file with an `FHS` and closes it with an
    /// `FTS`. Takes precedence over `file_header_from_doc`.
    pub file_header: Option<Vec<String>>,
    /// Name of a `$doc` section to echo the `FHS` fields from (the reader's
    /// positional `FHS` envelope section), enabling round-trip file-header
    /// reconstruction. Used only when `file_header` is unset.
    pub file_header_from_doc: Option<String>,
    /// Literal `BHS` data fields written verbatim as a batch header. When
    /// set, the writer wraps the messages in a `BHS..BTS` batch.
    pub batch_header: Option<Vec<String>>,
    /// Write a newline after each segment's carriage-return terminator for
    /// readability. The YAML option default is `true`, applied by the
    /// config builder; the plain `Default` for this struct leaves it
    /// `false`, so a caller constructing the config directly sets it.
    pub segment_newline: bool,
}

/// Streaming HL7 v2 file writer.
pub struct Hl7Writer<W: Write> {
    writer: W,
    config: Hl7WriterConfig,
    delims: Delimiters,
    /// Wire field columns keyed by the 1-based position parsed from the
    /// column name (`f01` → 1), each paired with its schema index. The
    /// numeric suffix — not schema-declaration order — determines the wire
    /// slot, so a gapped (`f01, f03`) or reordered schema maps values to
    /// the positions the names declare.
    field_columns: Vec<FieldColumn>,
    /// Split-leaf columns (`f08_c1`, `f03_r2_c1_s3`, …) resolved to their
    /// wire coordinate. Each contributes one leaf to its field's
    /// re-assembled wire value; the field position is taken from the
    /// coordinate, not declaration order.
    split_columns: Vec<SplitColumnRef>,
    seg_id_idx: Option<usize>,
    header_written: bool,
    finalized: bool,
    /// `true` once an `FHS` file header was emitted, so `flush` writes the
    /// matching `FTS`.
    file_open: bool,
    /// `true` once a `BHS` batch header was emitted, so `flush` writes the
    /// matching `BTS`.
    batch_open: bool,
    /// Count of `MSH` messages written, for the `BTS01` batch message count.
    message_count: u64,
    /// Count of `BHS` batches written, for the `FTS01` file batch count.
    batch_count: u64,
}

/// A schema `fNN` field column resolved to its wire position and the schema
/// index its value is read from.
struct FieldColumn {
    /// 1-based wire field position parsed from the `fNN` suffix.
    position: usize,
    /// The column name (`f01`) — used verbatim in error messages.
    name: String,
    /// Index into the record's value list.
    schema_index: usize,
}

/// A schema split-leaf column (`f08_c1`) resolved to its wire coordinate and
/// the schema index its value is read from.
struct SplitColumnRef {
    /// The `(field, repetition, component, sub-component)` coordinate.
    coord: SplitCoord,
    /// The column name (`f08_c1`) — used verbatim in error messages.
    name: String,
    /// Index into the record's value list.
    schema_index: usize,
}

impl<W: Write> Hl7Writer<W> {
    /// Build a writer over a sink with the given schema and config. The
    /// schema's `seg_id` and `fNN` columns are resolved to positional
    /// indices once. The `set_ref` / `set_type` columns are read from the
    /// `MSH` record itself, so they need no separate index.
    pub fn new(writer: W, schema: Arc<Schema>, config: Hl7WriterConfig) -> Self {
        let mut field_columns = Vec::new();
        let mut split_columns = Vec::new();
        let mut seg_id_idx = None;
        for (i, col) in schema.columns().iter().enumerate() {
            match &**col {
                "seg_id" => seg_id_idx = Some(i),
                // set_ref / set_type are reader-stamped echoes of MSH
                // fields; the MSH record carries the authoritative values,
                // so they are not mapped to wire fields here.
                "set_ref" | "set_type" => {}
                name => {
                    // A structured split-leaf name (`f08_c1`) re-assembles
                    // into its field; a plain `fNN` maps to its wire slot.
                    // The two grammars are disjoint — `field_position`
                    // rejects any name with a non-digit after `f`.
                    if let Some(coord) = parse_split_column(name) {
                        split_columns.push(SplitColumnRef {
                            coord,
                            name: name.to_string(),
                            schema_index: i,
                        });
                    } else if let Some(position) = field_position(name) {
                        field_columns.push(FieldColumn {
                            position,
                            name: name.to_string(),
                            schema_index: i,
                        });
                    }
                }
            }
        }
        field_columns.sort_by_key(|c| c.position);
        split_columns.sort_by_key(|c| c.coord.field_index);
        Self {
            writer,
            config,
            delims: Delimiters::default_set(),
            field_columns,
            split_columns,
            seg_id_idx,
            header_written: false,
            finalized: false,
            file_open: false,
            batch_open: false,
            message_count: 0,
            batch_count: 0,
        }
    }

    /// Emit the optional `FHS` file header and `BHS` batch header on the
    /// first record.
    fn write_header(&mut self, record: &Record) -> Result<(), FormatError> {
        if self.header_written {
            return Ok(());
        }
        self.header_written = true;

        if let Some(fhs) = self.resolve_file_header(record)? {
            self.write_segment("FHS", &fhs, true)?;
            self.file_open = true;
        }
        if let Some(bhs) = self.config.batch_header.clone() {
            self.write_segment("BHS", &bhs, true)?;
            self.batch_open = true;
            self.batch_count += 1;
        }
        Ok(())
    }

    /// Resolve the optional `FHS` fields: the literal config list wins;
    /// otherwise echo from the named `$doc` section; otherwise `None` (a
    /// bare MSH-led file with no file envelope).
    fn resolve_file_header(&self, record: &Record) -> Result<Option<Vec<String>>, FormatError> {
        if let Some(literal) = &self.config.file_header {
            return Ok(Some(literal.clone()));
        }
        if let Some(section) = &self.config.file_header_from_doc {
            let ctx = record.doc_ctx();
            if let Some(Value::Array(raw)) = ctx.get_section_field(section, RAW_FIELDS_KEY) {
                let fields = raw
                    .iter()
                    .enumerate()
                    .map(|(i, v)| value_to_field(v, &format!("{section}.f{:02}", i + 1)))
                    .collect::<Result<Vec<_>, _>>()?;
                return Ok(Some(fields));
            }
            // Fallback: collect the declared positional `f01, f02, …` keys
            // into wire order. A gapped section (e.g. `f01` and `f03`
            // present, `f02` absent) must not truncate at the gap — pad the
            // missing slot with an empty field and keep the later values at
            // the positions their keys declare, mirroring how `record_fields`
            // maps body records. `DocumentContext` exposes only point
            // lookups, so the scan runs to a fixed ceiling that comfortably
            // exceeds any real FHS/BHS header width.
            const HEADER_FIELD_SCAN_LIMIT: usize = 99;
            let mut fields: Vec<String> = Vec::new();
            for i in 0..HEADER_FIELD_SCAN_LIMIT {
                let key = format!("f{:02}", i + 1);
                if let Some(v) = ctx.get_section_field(section, &key) {
                    // Pad any skipped lower positions before placing this one.
                    fields.resize(i, String::new());
                    fields.push(value_to_field(&v, &format!("{section}.{key}"))?);
                }
            }
            if fields.is_empty() {
                return Err(FormatError::Hl7(format!(
                    "file_header_from_doc names section {section:?}, but the record's document \
                     context carries no FHS fields for it. Ensure the source declares a \
                     `segment: \"FHS\"` envelope section of the same name."
                )));
            }
            return Ok(Some(fields));
        }
        Ok(None)
    }

    /// Map a body record to its segment tag and field list, then write it.
    /// An `MSH` segment is counted as a message and its `MSH-2`
    /// encoding-characters field is written verbatim; other segments are
    /// written with their field data escaped.
    fn write_body_record(&mut self, record: &Record) -> Result<(), FormatError> {
        self.reject_unknown_columns(record)?;
        let values = record.values();
        let seg_id = match self.seg_id_idx.and_then(|i| values.get(i)) {
            Some(v) => value_to_field(v, "seg_id")?,
            None => String::new(),
        };
        if seg_id.is_empty() {
            return Err(FormatError::Hl7(
                "record carries no `seg_id`; every HL7 record needs a segment tag to write".into(),
            ));
        }

        // Batch/file headers and trailers are reconstructed by the writer
        // from config, so an envelope segment that rode through the record
        // stream is not re-emitted; an MSH still drives the message count.
        if matches!(seg_id.as_str(), "FHS" | "FTS" | "BHS" | "BTS") {
            return Ok(());
        }

        let is_msh = seg_id == "MSH";
        if is_msh {
            self.message_count += 1;
        }
        let fields = self.record_fields(record)?;
        self.write_segment(&seg_id, &fields, is_msh)?;
        Ok(())
    }

    /// Collect a record's field columns into wire-position order, filling any
    /// gap left by a missing position with an empty field, then trimming
    /// trailing empties so no fabricated delimiters appear.
    ///
    /// A verbatim `fNN` column sets its wire field directly. A split field's
    /// leaf columns (`f08_c1`, …) are re-assembled into the wire field at
    /// their shared position by joining on the component / sub-component /
    /// repetition separators — the exact inverse of the reader's split, so an
    /// HL7→HL7 round-trip is byte-faithful.
    fn record_fields(&self, record: &Record) -> Result<Vec<String>, FormatError> {
        let values = record.values();
        let max_verbatim = self.field_columns.iter().map(|c| c.position).max();
        let max_split = self.split_columns.iter().map(|c| c.coord.field_index).max();
        let max_position = max_verbatim.max(max_split).unwrap_or(0);
        let mut fields: Vec<String> = vec![String::new(); max_position];

        for col in &self.field_columns {
            let text = match values.get(col.schema_index) {
                Some(v) => value_to_field(v, &col.name)?,
                None => String::new(),
            };
            fields[col.position - 1] = text;
        }

        // Re-assemble each split field from its leaf columns. The leaves of
        // one field share a `field_index`; `split_columns` is sorted by it,
        // so contiguous runs belong to the same field.
        let mut start = 0;
        while start < self.split_columns.len() {
            let field_index = self.split_columns[start].coord.field_index;
            let mut end = start;
            let mut leaves: Vec<(SplitCoord, String)> = Vec::new();
            while end < self.split_columns.len()
                && self.split_columns[end].coord.field_index == field_index
            {
                let col = &self.split_columns[end];
                let text = match values.get(col.schema_index) {
                    Some(v) => value_to_field(v, &col.name)?,
                    None => String::new(),
                };
                leaves.push((col.coord, text));
                end += 1;
            }
            fields[field_index - 1] = reassemble_field(&leaves, &self.delims);
            start = end;
        }

        while matches!(fields.last(), Some(s) if s.is_empty()) {
            fields.pop();
        }
        Ok(fields)
    }

    /// Reject a record carrying a user column the writer does not map.
    /// Engine-namespaced columns (`$`-prefixed) and the reader-stamped
    /// `set_ref`/`set_type` echoes are excluded from the check.
    fn reject_unknown_columns(&self, record: &Record) -> Result<(), FormatError> {
        for (name, _) in record.iter_user_fields() {
            if name.starts_with('$') {
                continue;
            }
            let known = matches!(name, "seg_id" | "set_ref" | "set_type") || is_field_column(name);
            if !known {
                return Err(FormatError::Hl7(format!(
                    "record column {name:?} has no HL7 mapping. The writer maps only `seg_id`, \
                     `set_ref`, `set_type`, positional `fNN` field columns, and split-leaf \
                     columns (`f08_c1`, `f03_r2_c1_s3`); project the record to those before output"
                )));
            }
        }
        Ok(())
    }

    /// Write one segment: tag, field separator, escaped fields joined by the
    /// field separator, carriage-return terminator, optional newline.
    ///
    /// A header segment (`is_header`) carries its encoding-characters field
    /// (field index 0) verbatim — escaping it would corrupt the delimiter
    /// declaration. All other field data is escaped so a delimiter byte in
    /// data round-trips as an HL7 escape sequence rather than corrupting the
    /// segment.
    fn write_segment(
        &mut self,
        tag: &str,
        fields: &[String],
        is_header: bool,
    ) -> Result<(), FormatError> {
        self.writer
            .write_all(tag.as_bytes())
            .map_err(FormatError::Io)?;
        for (idx, field) in fields.iter().enumerate() {
            self.writer
                .write_all(&[self.delims.field])
                .map_err(FormatError::Io)?;
            if is_header && idx == 0 {
                self.writer
                    .write_all(field.as_bytes())
                    .map_err(FormatError::Io)?;
            } else {
                self.write_escaped(field)?;
            }
        }
        self.writer.write_all(b"\r").map_err(FormatError::Io)?;
        if self.config.segment_newline {
            self.writer.write_all(b"\n").map_err(FormatError::Io)?;
        }
        Ok(())
    }

    /// Write a field, escaping only the bytes the reader would otherwise
    /// misread as structure: the field separator (`|` → `\F\`), the escape
    /// character itself (`\` → `\E\`), and the segment terminator (carriage
    /// return → `\X0D\`).
    ///
    /// The component (`^`), repetition (`~`), and sub-component (`&`)
    /// separators are written **verbatim**, mirroring the reader, which keeps
    /// them as a field's internal composite/repetition structure rather than
    /// decoding them. Escaping them here would collapse a composite field
    /// like `PATID^^^MRN` into a single component of escaped literals,
    /// destroying the structure; leaving them verbatim makes the
    /// reader → writer → reader round-trip byte-faithful, exactly as the X12
    /// writer leaves its sub-element separator verbatim.
    fn write_escaped(&mut self, field: &str) -> Result<(), FormatError> {
        for &b in field.as_bytes() {
            // The carriage return is the segment terminator; a scalar field
            // value normally cannot contain it, but escape it defensively as
            // the numeric `\X0D\` form so an embedded CR cannot split a
            // segment.
            if b == b'\r' {
                self.writer
                    .write_all(&[self.delims.escape])
                    .map_err(FormatError::Io)?;
                self.writer.write_all(b"X0D").map_err(FormatError::Io)?;
                self.writer
                    .write_all(&[self.delims.escape])
                    .map_err(FormatError::Io)?;
                continue;
            }
            let escape: Option<&[u8]> = if b == self.delims.escape {
                Some(b"E")
            } else if b == self.delims.field {
                Some(b"F")
            } else {
                None
            };
            match escape {
                Some(code) => {
                    self.writer
                        .write_all(&[self.delims.escape])
                        .map_err(FormatError::Io)?;
                    self.writer.write_all(code).map_err(FormatError::Io)?;
                    self.writer
                        .write_all(&[self.delims.escape])
                        .map_err(FormatError::Io)?;
                }
                None => self.writer.write_all(&[b]).map_err(FormatError::Io)?,
            }
        }
        Ok(())
    }
}

impl<W: Write + Send> FormatWriter for Hl7Writer<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        self.write_header(record)?;
        self.write_body_record(record)
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        if self.finalized {
            return Ok(());
        }
        self.finalized = true;
        // Close the batch then the file with recomputed counts, but only if
        // a header was opened — a bare MSH-led output writes no trailers.
        if self.batch_open {
            let count = self.message_count.to_string();
            self.write_segment("BTS", &[count], false)?;
            self.batch_open = false;
        }
        if self.file_open {
            let count = self.batch_count.to_string();
            self.write_segment("FTS", &[count], false)?;
            self.file_open = false;
        }
        self.writer.flush().map_err(FormatError::Io)?;
        Ok(())
    }

    /// Drain the underlying sink without emitting the batch/file trailers, so
    /// byte-limit split accounting stays non-finalizing. Plan validation
    /// rejects byte splitting for this single-envelope format; this keeps the
    /// trait contract honest regardless, with the `BTS`/`FTS` trailers written
    /// only by [`Self::flush`].
    fn flush_bytes(&mut self) -> Result<(), FormatError> {
        self.writer.flush().map_err(FormatError::Io)
    }
}

/// Whether a schema column name is a writable HL7 field column: a verbatim
/// positional column (`f01`, `f02`, …) or a structured split-leaf column
/// (`f08_c1`, `f03_r2_c1_s3`, …). Both map to a wire field.
fn is_field_column(name: &str) -> bool {
    field_position(name).is_some() || parse_split_column(name).is_some()
}

/// Parse the 1-based wire field position from a column name. `f01` → 1,
/// `f12` → 12. Returns `None` when the name is not an `f`-prefixed digit
/// run or names position 0 (`f00`, which has no wire slot).
fn field_position(name: &str) -> Option<usize> {
    let rest = name.strip_prefix('f')?;
    if rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let position: usize = rest.parse().ok()?;
    (position >= 1).then_some(position)
}

/// Render a `Value` to HL7 field text. `Null` renders as the empty string
/// (an absent field); scalars render via their natural string form. A
/// `Value::Map` or `Value::Array` has no scalar HL7 field representation,
/// so it raises rather than silently emitting an empty cell — mirroring the
/// other formats' explicit-error posture for non-scalar payloads.
fn value_to_field(value: &Value, column: &str) -> Result<String, FormatError> {
    let text = match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Decimal(d) => d.to_string(),
        Value::String(s) => s.to_string(),
        Value::Date(d) => d.to_string(),
        Value::DateTime(dt) => dt.to_string(),
        Value::Map(_) | Value::Array(_) => {
            return Err(FormatError::UnserializableMapValue {
                format: "hl7",
                column: column.to_string(),
            });
        }
    };
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A schema with `seg_id`, `set_ref`, `set_type`, and `f01..f10`.
    fn schema() -> Arc<Schema> {
        let mut cols: Vec<Box<str>> = vec!["seg_id".into(), "set_ref".into(), "set_type".into()];
        for i in 1..=10 {
            cols.push(format!("f{i:02}").into_boxed_str());
        }
        Arc::new(Schema::new(cols))
    }

    fn record(schema: &Arc<Schema>, seg: &str, fields: &[&str]) -> Record {
        let mut values = vec![Value::String(seg.into()), Value::Null, Value::Null];
        for i in 0..10 {
            match fields.get(i) {
                Some(f) if !f.is_empty() => values.push(Value::String((*f).into())),
                _ => values.push(Value::Null),
            }
        }
        Record::new(Arc::clone(schema), values)
    }

    fn write_all(config: Hl7WriterConfig, records: &[Record], schema: &Arc<Schema>) -> String {
        let mut buf = Vec::new();
        let mut w = Hl7Writer::new(Cursor::new(&mut buf), Arc::clone(schema), config);
        for r in records {
            w.write_record(r).unwrap();
        }
        w.flush().unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn msh_and_body_segments_written_verbatim() {
        let s = schema();
        let msh = record(
            &s,
            "MSH",
            &["^~\\&", "SENDAPP", "", "", "", "", "", "ADT^A01", "MSG001"],
        );
        let pid = record(&s, "PID", &["1", "", "PATID"]);
        let out = write_all(Hl7WriterConfig::default(), &[msh, pid], &s);
        // The MSH-2 encoding chars are written verbatim (not escaped).
        assert!(out.starts_with("MSH|^~\\&|SENDAPP|"), "{out}");
        // PID writes positionally.
        assert!(out.contains("PID|1||PATID\r"), "{out}");
        // No trailers without an envelope config.
        assert!(!out.contains("FTS"), "{out}");
        assert!(!out.contains("BTS"), "{out}");
    }

    #[test]
    fn only_field_and_escape_chars_are_escaped() {
        let s = schema();
        // An OBX value carries a literal field separator '|', a component
        // separator '^', and a literal escape char '\'. Only the field
        // separator and the escape char are escaped; the component separator
        // is part of the field's composite structure and is left verbatim,
        // mirroring the reader (which keeps it verbatim) and the X12 writer.
        let msh = record(&s, "MSH", &["^~\\&", "S"]);
        let obx = record(&s, "OBX", &["1", "TX", "note", "", "a|b^c\\d"]);
        let out = write_all(Hl7WriterConfig::default(), &[msh, obx], &s);
        assert!(out.contains("a\\F\\b^c\\E\\d"), "{out}");
        // The component separator must not be escaped to \S\.
        assert!(
            !out.contains("\\S\\"),
            "component separator wrongly escaped: {out}"
        );
    }

    #[test]
    fn composite_field_round_trips_verbatim() {
        // A composite CX field (`PATID^^^^MRN`) read verbatim by the reader
        // must re-emit byte-identically — escaping the component separators
        // would collapse it into one component of escaped literals.
        let s = schema();
        let msh = record(&s, "MSH", &["^~\\&", "S"]);
        let pid = record(&s, "PID", &["1", "", "PATID^^^^MRN"]);
        let out = write_all(Hl7WriterConfig::default(), &[msh, pid], &s);
        assert!(out.contains("PID|1||PATID^^^^MRN\r"), "{out}");
    }

    #[test]
    fn embedded_carriage_return_is_hex_escaped() {
        // A scalar value carrying a carriage return cannot be written raw —
        // it would split the segment. It is escaped as the numeric \X0D\
        // form, which the reader decodes back to a literal CR.
        let s = schema();
        let msh = record(&s, "MSH", &["^~\\&", "S"]);
        let nte = record(&s, "NTE", &["1", "", "a\rb"]);
        let out = write_all(Hl7WriterConfig::default(), &[msh, nte], &s);
        assert!(out.contains("a\\X0D\\b"), "{out}");
        // The raw CR must not appear inside the field (only as the segment
        // terminator after the field).
        assert!(!out.contains("a\rb"), "raw CR leaked into field: {out}");
    }

    #[test]
    fn trailing_nulls_not_padded() {
        let s = schema();
        let msh = record(&s, "MSH", &["^~\\&", "S"]);
        let pid = record(&s, "PID", &["1"]);
        let out = write_all(Hl7WriterConfig::default(), &[msh, pid], &s);
        // PID has one field; no trailing '|' delimiters.
        assert!(out.contains("PID|1\r"), "{out}");
        assert!(!out.contains("PID|1|"), "{out}");
    }

    #[test]
    fn file_and_batch_envelope_reconstructed_with_counts() {
        let s = schema();
        let msh1 = record(
            &s,
            "MSH",
            &["^~\\&", "S", "", "", "", "", "", "ADT^A01", "M1"],
        );
        let pid1 = record(&s, "PID", &["1"]);
        let msh2 = record(
            &s,
            "MSH",
            &["^~\\&", "S", "", "", "", "", "", "ADT^A01", "M2"],
        );
        let pid2 = record(&s, "PID", &["2"]);
        let cfg = Hl7WriterConfig {
            file_header: Some(vec!["^~\\&".into(), "SENDAPP".into()]),
            batch_header: Some(vec!["^~\\&".into(), "SENDAPP".into()]),
            segment_newline: false,
            ..Default::default()
        };
        let out = write_all(cfg, &[msh1, pid1, msh2, pid2], &s);
        assert!(out.starts_with("FHS|^~\\&|SENDAPP\r"), "{out}");
        assert!(out.contains("BHS|^~\\&|SENDAPP\r"), "{out}");
        // Two messages -> BTS|2; one batch -> FTS|1.
        assert!(out.contains("BTS|2\r"), "{out}");
        assert!(out.contains("FTS|1\r"), "{out}");
    }

    #[test]
    fn file_header_echoed_from_doc_section() {
        use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord, Value as RecVal};
        use indexmap::IndexMap;

        let s = schema();
        let raw = RecVal::Array(vec![
            RecVal::String("^~\\&".into()),
            RecVal::String("SENDAPP".into()),
            RecVal::String("FILE9".into()),
        ]);
        let mut file_doc: IndexMap<Box<str>, RecVal> = IndexMap::new();
        file_doc.insert(super::RAW_FIELDS_KEY.into(), raw);
        let mut sections: IndexMap<Box<str>, RecVal> = IndexMap::new();
        sections.insert("file".into(), RecVal::Map(Box::new(file_doc)));
        let ctx = Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from("a.hl7"),
            EnvelopeRecord::from_sections(sections),
        ));

        let mut msh = record(
            &s,
            "MSH",
            &["^~\\&", "S", "", "", "", "", "", "ADT^A01", "M1"],
        );
        msh.set_doc_ctx(ctx);

        let cfg = Hl7WriterConfig {
            file_header_from_doc: Some("file".into()),
            segment_newline: false,
            ..Default::default()
        };
        let out = write_all(cfg, &[msh], &s);
        assert!(out.starts_with("FHS|^~\\&|SENDAPP|FILE9\r"), "{out}");
        // One message, zero batches -> FTS|0.
        assert!(out.contains("FTS|0\r"), "{out}");
    }

    #[test]
    fn gapped_doc_header_fallback_pads_missing_positions() {
        // The positional-key fallback (no `$raw` array present) must not stop
        // at the first missing `fNN` key: with `f01` and `f03` declared but
        // `f02` absent, the FHS header must carry an empty field at position 2
        // rather than truncating at the gap.
        use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord, Value as RecVal};
        use indexmap::IndexMap;

        let s = schema();
        let mut file_doc: IndexMap<Box<str>, RecVal> = IndexMap::new();
        file_doc.insert("f01".into(), RecVal::String("^~\\&".into()));
        // f02 deliberately absent — the gap.
        file_doc.insert("f03".into(), RecVal::String("FILE9".into()));
        let mut sections: IndexMap<Box<str>, RecVal> = IndexMap::new();
        sections.insert("file".into(), RecVal::Map(Box::new(file_doc)));
        let ctx = Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from("a.hl7"),
            EnvelopeRecord::from_sections(sections),
        ));

        let mut msh = record(&s, "MSH", &["^~\\&", "S"]);
        msh.set_doc_ctx(ctx);

        let cfg = Hl7WriterConfig {
            file_header_from_doc: Some("file".into()),
            segment_newline: false,
            ..Default::default()
        };
        let out = write_all(cfg, &[msh], &s);
        // FHS-2 encoding chars, then an empty FHS-3, then FILE9 at FHS-4 —
        // the gap is preserved as an empty field, not collapsed.
        assert!(out.starts_with("FHS|^~\\&||FILE9\r"), "{out}");
    }

    #[test]
    fn unrecognized_column_errors_by_name() {
        let cols: Vec<Box<str>> = vec!["seg_id".into(), "amount".into()];
        let schema = Arc::new(Schema::new(cols));
        let record = Record::new(
            Arc::clone(&schema),
            vec![Value::String("PID".into()), Value::String("99".into())],
        );
        let mut buf = Vec::new();
        let mut w = Hl7Writer::new(
            Cursor::new(&mut buf),
            Arc::clone(&schema),
            Hl7WriterConfig::default(),
        );
        let err = w.write_record(&record).unwrap_err();
        assert!(matches!(err, FormatError::Hl7(m) if m.contains("amount")));
    }

    #[test]
    fn engine_stamped_columns_excluded() {
        let cols: Vec<Box<str>> = vec!["seg_id".into(), "f01".into(), "$source.file".into()];
        let schema = Arc::new(Schema::new(cols));
        let values = vec![
            Value::String("PID".into()),
            Value::String("1".into()),
            Value::String("/tmp/a.hl7".into()),
        ];
        let record = Record::new(Arc::clone(&schema), values);
        let out = write_all(Hl7WriterConfig::default(), &[record], &schema);
        assert!(out.contains("PID|1\r"), "{out}");
        assert!(!out.contains("/tmp/a.hl7"), "{out}");
    }

    #[test]
    fn map_value_in_field_column_errors_by_name() {
        use indexmap::IndexMap;
        let cols: Vec<Box<str>> = vec!["seg_id".into(), "f01".into()];
        let schema = Arc::new(Schema::new(cols));
        let mut nested: IndexMap<Box<str>, Value> = IndexMap::new();
        nested.insert("k".into(), Value::String("v".into()));
        let record = Record::new(
            Arc::clone(&schema),
            vec![Value::String("PID".into()), Value::Map(Box::new(nested))],
        );
        let mut buf = Vec::new();
        let mut w = Hl7Writer::new(
            Cursor::new(&mut buf),
            Arc::clone(&schema),
            Hl7WriterConfig::default(),
        );
        let err = w.write_record(&record).unwrap_err();
        assert!(
            matches!(&err, FormatError::UnserializableMapValue { format: "hl7", column } if column == "f01"),
            "expected UnserializableMapValue for f01, got: {err:?}"
        );
    }

    #[test]
    fn flush_finalize_idempotent() {
        let s = schema();
        let msh = record(&s, "MSH", &["^~\\&", "S"]);
        let mut buf = Vec::new();
        let cfg = Hl7WriterConfig {
            file_header: Some(vec!["^~\\&".into(), "S".into()]),
            segment_newline: false,
            ..Default::default()
        };
        let mut w = Hl7Writer::new(Cursor::new(&mut buf), Arc::clone(&s), cfg);
        w.write_record(&msh).unwrap();
        w.flush().unwrap();
        w.flush().unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert_eq!(out.matches("FTS").count(), 1, "{out}");
    }

    #[test]
    fn zero_records_writes_nothing() {
        let s = schema();
        let out = write_all(Hl7WriterConfig::default(), &[], &s);
        assert!(out.is_empty(), "{out}");
    }

    #[test]
    fn gapped_field_columns_map_by_position() {
        let cols: Vec<Box<str>> = vec!["seg_id".into(), "f01".into(), "f03".into()];
        let schema = Arc::new(Schema::new(cols));
        let record = Record::new(
            Arc::clone(&schema),
            vec![
                Value::String("PID".into()),
                Value::String("a".into()),
                Value::String("c".into()),
            ],
        );
        let out = write_all(Hl7WriterConfig::default(), &[record], &schema);
        assert!(out.contains("PID|a||c\r"), "gapped mapping wrong: {out}");
    }

    #[test]
    fn split_columns_reassemble_into_wire_field() {
        // Component leaf columns re-join on the component separator at their
        // shared wire position; the separator is verbatim, not escaped.
        let cols: Vec<Box<str>> = vec![
            "seg_id".into(),
            "f01".into(),
            "f03_c1".into(),
            "f03_c2".into(),
        ];
        let schema = Arc::new(Schema::new(cols));
        let record = Record::new(
            Arc::clone(&schema),
            vec![
                Value::String("PID".into()),
                Value::String("1".into()),
                Value::String("PATID".into()),
                Value::String("MRN".into()),
            ],
        );
        let out = write_all(Hl7WriterConfig::default(), &[record], &schema);
        assert!(
            out.contains("PID|1||PATID^MRN\r"),
            "reassembly wrong: {out}"
        );
        assert!(!out.contains("\\S\\"), "component separator escaped: {out}");
    }

    #[test]
    fn split_columns_trim_trailing_empty_components() {
        // A trailing-empty component must not fabricate a `^` inside the
        // re-assembled field. The split sits at wire field 3, so fields 1
        // and 2 are empty (`PID|||…`), exactly as a verbatim `f03` would be.
        let cols: Vec<Box<str>> = vec!["seg_id".into(), "f03_c1".into(), "f03_c2".into()];
        let schema = Arc::new(Schema::new(cols));
        let record = Record::new(
            Arc::clone(&schema),
            vec![
                Value::String("PID".into()),
                Value::String("PATID".into()),
                Value::Null,
            ],
        );
        let out = write_all(Hl7WriterConfig::default(), &[record], &schema);
        assert!(out.contains("PID|||PATID\r"), "{out}");
        assert!(!out.contains("PATID^"), "fabricated trailing sep: {out}");
    }

    #[test]
    fn split_columns_reassemble_repetitions_and_subcomponents() {
        let cols: Vec<Box<str>> = vec![
            "seg_id".into(),
            "f03_r1_c1_s1".into(),
            "f03_r1_c1_s2".into(),
            "f03_r2_c1_s1".into(),
        ];
        let schema = Arc::new(Schema::new(cols));
        let record = Record::new(
            Arc::clone(&schema),
            vec![
                Value::String("PID".into()),
                Value::String("A".into()),
                Value::String("B".into()),
                Value::String("C".into()),
            ],
        );
        let out = write_all(Hl7WriterConfig::default(), &[record], &schema);
        // Sub-components join on `&`, repetitions on `~`, at wire field 3.
        assert!(out.contains("PID|||A&B~C\r"), "{out}");
    }

    #[test]
    fn split_leaf_value_escapes_embedded_field_separator() {
        // A literal field separator inside a split leaf is still escaped on
        // output; only the structural component separator is verbatim.
        let cols: Vec<Box<str>> = vec!["seg_id".into(), "f03_c1".into(), "f03_c2".into()];
        let schema = Arc::new(Schema::new(cols));
        let record = Record::new(
            Arc::clone(&schema),
            vec![
                Value::String("PID".into()),
                Value::String("a|b".into()),
                Value::String("MRN".into()),
            ],
        );
        let out = write_all(Hl7WriterConfig::default(), &[record], &schema);
        // `a|b` → `a\F\b`, joined to `MRN` with a verbatim `^`, at wire field 3.
        assert!(out.contains("PID|||a\\F\\b^MRN\r"), "{out}");
    }
}
