//! UN/EDIFACT interchange writer.
//!
//! Reconstructs the interchange envelope around emitted body records:
//! an optional `UNA`, a `UNB` header (literal config elements or echoed
//! from a `$doc` section), `UNH..UNT` messages grouped on the `msg_ref`
//! column, and a closing `UNZ`. Record columns map positionally —
//! `seg_id` is the segment tag, `e01..` are the data elements. Element
//! data is release-escaped on output and decoded by the reader, so a
//! reader→writer→reader round-trip is byte-faithful even for values
//! that carry service characters as literal data.
//!
//! Memory model: streaming and O(1) in held state — only the
//! currently-open message's running segment count and the interchange
//! message count are retained. `flush` is the single end-of-stream
//! finalizer: it closes the open message and writes the `UNZ` trailer
//! with recomputed counts, exactly once. An interchange is one envelope,
//! so byte-limit file splitting (which would flush — and thus finalize —
//! mid-stream) is rejected for EDIFACT outputs at config-validation
//! time; this writer is therefore only ever flushed at true end of
//! stream.

use std::io::Write;
use std::sync::Arc;

use clinker_record::{Record, Schema, Value};

use crate::edifact::tokenizer::Delimiters;
use crate::edifact::{RAW_ELEMENTS_KEY, unb_control_reference};
use crate::error::FormatError;
use crate::traits::FormatWriter;

/// Configuration for the EDIFACT writer.
#[derive(Clone, Default)]
pub struct EdifactWriterConfig {
    /// Literal `UNB` data elements. When set, the writer emits exactly
    /// these as the interchange header. Takes precedence over
    /// `interchange_from_doc`.
    pub interchange: Option<Vec<String>>,
    /// Name of a `$doc` section to echo the `UNB` elements from (the
    /// reader's `UNB` positional section), enabling round-trip
    /// reconstruction. Used only when `interchange` is unset.
    pub interchange_from_doc: Option<String>,
    /// Fallback message type when a record carries no `msg_type` column
    /// value (e.g. body records whose UNH was synthesized upstream).
    pub message_type: Option<String>,
    /// Emit a leading `UNA` service-string-advice segment. The YAML
    /// option default is `false` (Level-A delimiters are implicit).
    pub write_una: bool,
    /// Write a newline after each segment terminator for readability.
    /// The YAML option default is `true`, applied by the config builder;
    /// the plain `Default` for this struct leaves it `false`, so a
    /// caller constructing the config directly sets it explicitly.
    pub segment_newline: bool,
}

/// Streaming EDIFACT interchange writer.
pub struct EdifactWriter<W: Write> {
    writer: W,
    config: EdifactWriterConfig,
    delims: Delimiters,
    /// Wire element columns keyed by the 1-based position parsed from the
    /// column name (`e01` → 1), each paired with its schema index. The
    /// numeric suffix — not schema-declaration order — determines the wire
    /// slot, so a gapped (`e01, e03`) or reordered (`e02, e01`) schema maps
    /// values to the positions the names declare, with gaps emitted as
    /// empty elements rather than silently shifting later values left.
    element_columns: Vec<ElementColumn>,
    seg_id_idx: Option<usize>,
    msg_ref_idx: Option<usize>,
    msg_type_idx: Option<usize>,
    header_written: bool,
    finalized: bool,
    unb_control_ref: String,
    message_count: u64,
    open_message: Option<OpenMessage>,
}

/// A schema `eNN` element column resolved to its wire position and the
/// schema index its value is read from.
struct ElementColumn {
    /// 1-based wire element position parsed from the `eNN` suffix.
    position: usize,
    /// The column name (`e01`) — used verbatim in error messages so a
    /// misrouted value names the exact field the user must fix.
    name: String,
    /// Index into the record's value list.
    schema_index: usize,
}

/// State for the message currently being written.
struct OpenMessage {
    reference: String,
    /// Segments written so far including `UNH` (the closing `UNT` adds
    /// one more when the message closes).
    segment_count: u64,
}

impl<W: Write> EdifactWriter<W> {
    /// Build a writer over a sink with the given schema and config. The
    /// schema's `seg_id` / `msg_ref` / `msg_type` / `eNN` columns are
    /// resolved to positional indices once.
    pub fn new(writer: W, schema: Arc<Schema>, config: EdifactWriterConfig) -> Self {
        let mut element_columns = Vec::new();
        let mut seg_id_idx = None;
        let mut msg_ref_idx = None;
        let mut msg_type_idx = None;
        for (i, col) in schema.columns().iter().enumerate() {
            match &**col {
                "seg_id" => seg_id_idx = Some(i),
                "msg_ref" => msg_ref_idx = Some(i),
                "msg_type" => msg_type_idx = Some(i),
                name => {
                    if let Some(position) = element_position(name) {
                        element_columns.push(ElementColumn {
                            position,
                            name: name.to_string(),
                            schema_index: i,
                        });
                    }
                }
            }
        }
        // Order by the declared wire position so a reordered schema
        // (`e02` before `e01`) still emits elements in ascending position.
        element_columns.sort_by_key(|c| c.position);
        Self {
            writer,
            config,
            delims: Delimiters::level_a(),
            element_columns,
            seg_id_idx,
            msg_ref_idx,
            msg_type_idx,
            header_written: false,
            finalized: false,
            unb_control_ref: String::new(),
            message_count: 0,
            open_message: None,
        }
    }

    /// Emit the optional `UNA` and the `UNB` header on the first record.
    fn write_header(&mut self, record: &Record) -> Result<(), FormatError> {
        if self.header_written {
            return Ok(());
        }
        self.header_written = true;

        if self.config.write_una {
            // UNA carries the six service chars in fixed order; the
            // segment is exactly 9 bytes and is not terminator-delimited.
            let una = [
                b'U',
                b'N',
                b'A',
                self.delims.component,
                self.delims.element,
                self.delims.decimal,
                self.delims.release,
                self.delims.repetition,
                self.delims.terminator,
            ];
            self.writer.write_all(&una).map_err(FormatError::Io)?;
            if self.config.segment_newline {
                self.writer.write_all(b"\n").map_err(FormatError::Io)?;
            }
        }

        let unb_elements = self.resolve_unb_elements(record)?;
        // Echo the same control reference into the UNZ trailer that the
        // reader validates against, located structurally (after the
        // mandatory leading composites) so an empty optional element in
        // the header does not push the trailer out of sync with it.
        // Default to empty when the header carries no control reference.
        self.unb_control_ref = unb_control_reference(&unb_elements)
            .unwrap_or_default()
            .to_owned();
        self.write_segment("UNB", &unb_elements)?;
        Ok(())
    }

    /// Resolve the `UNB` data elements: the literal config list wins;
    /// otherwise echo from the named `$doc` section on the record;
    /// otherwise a precise configuration error.
    fn resolve_unb_elements(&self, record: &Record) -> Result<Vec<String>, FormatError> {
        if let Some(literal) = &self.config.interchange {
            return Ok(literal.clone());
        }
        if let Some(section) = &self.config.interchange_from_doc {
            let ctx = record.doc_ctx();
            // Preferred path: the EDIFACT reader stashes the complete,
            // ordered UNB element list (empty middle elements included)
            // under an engine-internal key. Reconstructing from it
            // round-trips a UNB header faithfully without the user having
            // to declare every contiguous `eNN` field — and without
            // truncating at an empty element the way a declared-field
            // walk would.
            if let Some(Value::Array(raw)) = ctx.get_section_field(section, RAW_ELEMENTS_KEY) {
                // Each raw item is a stashed UNB element string; name the
                // positional slot so a non-scalar item names its field.
                return raw
                    .iter()
                    .enumerate()
                    .map(|(i, v)| value_to_element(v, &format!("{section}.e{:02}", i + 1)))
                    .collect();
            }
            // Fallback for a section not produced by the EDIFACT reader
            // (e.g. literal author-supplied `$doc` fields): walk the
            // declared positional `e01, e02, …` keys until one is absent.
            // This cannot represent an empty middle element, so it is the
            // best-effort path, not the round-trip path.
            let mut elements: Vec<String> = Vec::new();
            for i in 0.. {
                let key = format!("e{:02}", i + 1);
                match ctx.get_section_field(section, &key) {
                    Some(v) => elements.push(value_to_element(&v, &format!("{section}.{key}"))?),
                    None => break,
                }
            }
            if elements.is_empty() {
                return Err(FormatError::Edifact(format!(
                    "interchange_from_doc names section {section:?}, but the record's \
                     document context carries no UNB elements for it. Ensure the source \
                     declares a `segment: \"UNB\"` envelope section of the same name."
                )));
            }
            return Ok(elements);
        }
        Err(FormatError::Edifact(
            "no interchange header configured: set the writer's `interchange` literal \
             elements or `interchange_from_doc` section name so a UNB header can be \
             written"
                .into(),
        ))
    }

    /// Map a body record to its segment tag and trimmed element list,
    /// then write it, opening or closing `UNH..UNT` messages on
    /// `msg_ref` transitions.
    fn write_body_record(&mut self, record: &Record) -> Result<(), FormatError> {
        self.reject_unknown_columns(record)?;
        let values = record.values();
        let seg_id = match self.seg_id_idx.and_then(|i| values.get(i)) {
            Some(v) => value_to_element(v, "seg_id")?,
            None => String::new(),
        };

        // Service segments embedded in the record stream are skipped —
        // the writer reconstructs envelopes itself, so a UNH/UNT/UNB/UNZ
        // body record (e.g. from an EDIFACT→EDIFACT pipeline that emits
        // them verbatim) must not be double-written.
        let msg_ref = match self.msg_ref_idx.and_then(|i| values.get(i)) {
            Some(v) => value_to_element(v, "msg_ref")?,
            None => String::new(),
        };
        let msg_type = match self.msg_type_idx.and_then(|i| values.get(i)) {
            Some(v) => value_to_element(v, "msg_type")?,
            None => String::new(),
        };

        if matches!(seg_id.as_str(), "UNB" | "UNZ" | "UNH" | "UNT") {
            // Drive message grouping from the UNH record's identity but
            // never echo the service segment verbatim.
            self.transition_message(&msg_ref, &msg_type)?;
            return Ok(());
        }

        self.transition_message(&msg_ref, &msg_type)?;

        let elements = self.record_elements(record)?;
        self.write_segment(&seg_id, &elements)?;
        if let Some(msg) = self.open_message.as_mut() {
            msg.segment_count += 1;
        }
        Ok(())
    }

    /// Open a new `UNH` message (closing any previous one) when the
    /// `msg_ref` changes, or open the first message. A record with an
    /// empty `msg_ref` joins the current message; if none is open, it
    /// opens an anonymous single message.
    fn transition_message(&mut self, msg_ref: &str, msg_type: &str) -> Result<(), FormatError> {
        let need_new = match &self.open_message {
            None => true,
            Some(open) => !msg_ref.is_empty() && open.reference != *msg_ref,
        };
        if !need_new {
            return Ok(());
        }
        self.close_open_message()?;

        let resolved_type = if !msg_type.is_empty() {
            msg_type.to_string()
        } else if let Some(t) = &self.config.message_type {
            t.clone()
        } else {
            return Err(FormatError::Edifact(
                "cannot open a UNH message: the record carries no `msg_type` and the \
                 writer has no `message_type` fallback configured"
                    .into(),
            ));
        };
        let reference = if msg_ref.is_empty() {
            (self.message_count + 1).to_string()
        } else {
            msg_ref.to_string()
        };
        self.write_segment("UNH", &[reference.clone(), resolved_type])?;
        self.message_count += 1;
        self.open_message = Some(OpenMessage {
            reference,
            segment_count: 1,
        });
        Ok(())
    }

    /// Close the open message with its recomputed `UNT` trailer (segment
    /// count including `UNH`+`UNT`, message reference echo).
    fn close_open_message(&mut self) -> Result<(), FormatError> {
        if let Some(msg) = self.open_message.take() {
            let count = msg.segment_count + 1;
            self.write_segment("UNT", &[count.to_string(), msg.reference])?;
        }
        Ok(())
    }

    /// Collect a record's `eNN` columns into wire-position order, filling
    /// any gap left by a missing position with an empty element, then
    /// trimming trailing empties so no fabricated delimiters appear.
    ///
    /// A column's wire slot is its parsed `eNN` position (1-based), not its
    /// schema-declaration order: a `seg_id, e01, e03` record writes `e03`'s
    /// value as wire element 3 with element 2 empty, never as element 2.
    fn record_elements(&self, record: &Record) -> Result<Vec<String>, FormatError> {
        let values = record.values();
        let max_position = self
            .element_columns
            .iter()
            .map(|c| c.position)
            .max()
            .unwrap_or(0);
        let mut elements: Vec<String> = vec![String::new(); max_position];
        for col in &self.element_columns {
            let text = match values.get(col.schema_index) {
                Some(v) => value_to_element(v, &col.name)?,
                None => String::new(),
            };
            // position is 1-based; slot is position-1.
            elements[col.position - 1] = text;
        }
        while matches!(elements.last(), Some(s) if s.is_empty()) {
            elements.pop();
        }
        Ok(elements)
    }

    /// Reject a record carrying a user column the writer does not map.
    /// Engine-namespaced columns (`$`-prefixed: `$source.*`, `$ck.*`,
    /// `$widened`) are excluded from the check and never appear in
    /// EDIFACT output by design.
    fn reject_unknown_columns(&self, record: &Record) -> Result<(), FormatError> {
        for (name, _) in record.iter_user_fields() {
            if name.starts_with('$') {
                continue;
            }
            let known =
                matches!(name, "seg_id" | "msg_ref" | "msg_type") || is_element_column(name);
            if !known {
                return Err(FormatError::Edifact(format!(
                    "record column {name:?} has no EDIFACT mapping. The writer maps only \
                     `seg_id`, `msg_ref`, `msg_type`, and positional `eNN` element \
                     columns; project the record to those before output"
                )));
            }
        }
        Ok(())
    }

    /// Write one segment: tag, element separator, release-escaped
    /// elements joined by the element separator, terminator, optional
    /// newline.
    ///
    /// Element data is release-escaped so a data byte that collides with
    /// a service character does not corrupt the interchange: an
    /// apostrophe in `O'BRIEN` is emitted as `O?'BRIEN` (not a premature
    /// terminator) and a literal `+` in `A+B` as `A?+B` (not a second
    /// element). The reader decodes these on the next read, so the
    /// round-trip is byte-faithful for any value the writer is handed —
    /// including values computed by a Transform or sourced from CSV/JSON,
    /// which were never EDIFACT-escaped to begin with.
    fn write_segment(&mut self, tag: &str, elements: &[String]) -> Result<(), FormatError> {
        self.writer
            .write_all(tag.as_bytes())
            .map_err(FormatError::Io)?;
        for element in elements {
            self.writer
                .write_all(&[self.delims.element])
                .map_err(FormatError::Io)?;
            self.write_escaped_element(element)?;
        }
        self.writer
            .write_all(&[self.delims.terminator])
            .map_err(FormatError::Io)?;
        if self.config.segment_newline {
            self.writer.write_all(b"\n").map_err(FormatError::Io)?;
        }
        Ok(())
    }

    /// Write one element, prefixing each service character that would
    /// otherwise be read as a delimiter with the release character.
    ///
    /// Escaped: the segment terminator, the element separator, and the
    /// release character itself. The component separator is *not* escaped
    /// — a `:` in the stored value is treated as the element's own
    /// composite structure (mirroring the reader, which keeps unescaped
    /// component separators verbatim), so composite elements like
    /// `UNOA:1` re-emit unchanged.
    fn write_escaped_element(&mut self, element: &str) -> Result<(), FormatError> {
        let release = self.delims.release;
        let terminator = self.delims.terminator;
        let separator = self.delims.element;
        for &b in element.as_bytes() {
            if b == release || b == terminator || b == separator {
                self.writer.write_all(&[release]).map_err(FormatError::Io)?;
            }
            self.writer.write_all(&[b]).map_err(FormatError::Io)?;
        }
        Ok(())
    }
}

impl<W: Write + Send> FormatWriter for EdifactWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        self.write_header(record)?;
        self.write_body_record(record)
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        if self.finalized {
            return Ok(());
        }
        self.finalized = true;
        self.close_open_message()?;
        // Only write UNZ if a header was actually emitted; a writer that
        // saw zero records produces nothing rather than a bare trailer.
        if self.header_written {
            let control_ref = self.unb_control_ref.clone();
            self.write_segment("UNZ", &[self.message_count.to_string(), control_ref])?;
        }
        self.writer.flush().map_err(FormatError::Io)?;
        Ok(())
    }
}

/// Whether a schema column name is a positional EDIFACT element column
/// (`e01`, `e02`, …): an `e` followed by one or more ASCII digits.
fn is_element_column(name: &str) -> bool {
    element_position(name).is_some()
}

/// Parse the 1-based wire element position from a column name. `e01` → 1,
/// `e12` → 12. Returns `None` when the name is not an `e`-prefixed digit
/// run or names position 0 (`e00`, which has no wire slot).
fn element_position(name: &str) -> Option<usize> {
    let rest = name.strip_prefix('e')?;
    if rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let position: usize = rest.parse().ok()?;
    (position >= 1).then_some(position)
}

/// Render a `Value` to EDIFACT element text (pre-escaping). `Null` renders
/// as the empty string (an absent element); scalars render via their
/// natural string form. A `Value::Map` or `Value::Array` has no scalar
/// EDIFACT element representation, so it raises rather than silently
/// emitting an empty cell or a fabricated comma-join — mirroring the
/// CSV/XML/fixed-width writers' explicit-error posture for non-scalar
/// payloads. `column` names the offending field for the error.
///
/// Release-escaping of any service character in the result happens at
/// write time in [`EdifactWriter::write_escaped_element`].
fn value_to_element(value: &Value, column: &str) -> Result<String, FormatError> {
    let text = match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.to_string(),
        Value::Date(d) => d.to_string(),
        Value::DateTime(dt) => dt.to_string(),
        Value::Map(_) | Value::Array(_) => {
            return Err(FormatError::UnserializableMapValue {
                format: "edifact",
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

    fn schema() -> Arc<Schema> {
        let mut cols: Vec<Box<str>> = vec!["seg_id".into(), "msg_ref".into(), "msg_type".into()];
        for i in 1..=4 {
            cols.push(format!("e{i:02}").into_boxed_str());
        }
        Arc::new(Schema::new(cols))
    }

    fn body(
        schema: &Arc<Schema>,
        seg: &str,
        msg_ref: &str,
        msg_type: &str,
        els: &[&str],
    ) -> Record {
        let mut values = vec![
            Value::String(seg.into()),
            if msg_ref.is_empty() {
                Value::Null
            } else {
                Value::String(msg_ref.into())
            },
            if msg_type.is_empty() {
                Value::Null
            } else {
                Value::String(msg_type.into())
            },
        ];
        for i in 0..4 {
            match els.get(i) {
                Some(e) => values.push(Value::String((*e).into())),
                None => values.push(Value::Null),
            }
        }
        Record::new(Arc::clone(schema), values)
    }

    fn write_all(config: EdifactWriterConfig, records: &[Record], schema: &Arc<Schema>) -> String {
        let mut buf = Vec::new();
        let mut w = EdifactWriter::new(Cursor::new(&mut buf), Arc::clone(schema), config);
        for r in records {
            w.write_record(r).unwrap();
        }
        w.flush().unwrap();
        String::from_utf8(buf).unwrap()
    }

    fn literal_config() -> EdifactWriterConfig {
        EdifactWriterConfig {
            interchange: Some(vec![
                "UNOA:1".into(),
                "S".into(),
                "R".into(),
                "240101:1200".into(),
                "REF1".into(),
            ]),
            segment_newline: false,
            ..Default::default()
        }
    }

    #[test]
    fn unb_from_literal_options() {
        let s = schema();
        let out = write_all(
            literal_config(),
            &[body(&s, "BGM", "M1", "ORDERS", &["220"])],
            &s,
        );
        assert!(out.starts_with("UNB+UNOA:1+S+R+240101:1200+REF1'"));
    }

    #[test]
    fn unb_echoed_from_doc_section() {
        use clinker_record::{DocumentContext, DocumentId, Value as RecVal};
        use indexmap::IndexMap;

        // Build a record carrying a `$doc.unb` section with positional
        // e01..e05 UNB elements, as the EDIFACT reader's prepare_document
        // would attach.
        let s = schema();
        let mut unb: IndexMap<Box<str>, RecVal> = IndexMap::new();
        unb.insert("e01".into(), RecVal::String("UNOA:1".into()));
        unb.insert("e02".into(), RecVal::String("S".into()));
        unb.insert("e03".into(), RecVal::String("R".into()));
        unb.insert("e04".into(), RecVal::String("240101:1200".into()));
        unb.insert("e05".into(), RecVal::String("REF1".into()));
        let mut sections: IndexMap<Box<str>, RecVal> = IndexMap::new();
        sections.insert("unb".into(), RecVal::Map(Box::new(unb)));
        let ctx = Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from("orders.edi"),
            sections,
        ));

        let mut record = body(&s, "BGM", "M1", "ORDERS", &["220"]);
        record.set_doc_ctx(ctx);

        let cfg = EdifactWriterConfig {
            interchange_from_doc: Some("unb".into()),
            segment_newline: false,
            ..Default::default()
        };
        let out = write_all(cfg, &[record], &s);
        // The UNB header is reconstructed verbatim from the $doc section,
        // and the UNZ trailer echoes its control reference.
        assert!(out.starts_with("UNB+UNOA:1+S+R+240101:1200+REF1'"), "{out}");
        assert!(out.contains("UNZ+1+REF1'"), "{out}");
    }

    #[test]
    fn unb_from_doc_raw_preserves_empty_middle_element() {
        use clinker_record::{DocumentContext, DocumentId, Value as RecVal};
        use indexmap::IndexMap;

        // A real UNB whose element 4 (index 3) is empty. The reader
        // stashes the complete element list under the engine-internal raw
        // key; the writer must reconstruct it without truncating at the
        // empty element, so the control reference "REF9" (element 6)
        // survives.
        let s = schema();
        let raw = RecVal::Array(vec![
            RecVal::String("UNOA:1".into()),
            RecVal::String("S".into()),
            RecVal::String("R".into()),
            RecVal::String("".into()),
            RecVal::String("240101:1200".into()),
            RecVal::String("REF9".into()),
        ]);
        let mut unb: IndexMap<Box<str>, RecVal> = IndexMap::new();
        unb.insert(super::RAW_ELEMENTS_KEY.into(), raw);
        let mut sections: IndexMap<Box<str>, RecVal> = IndexMap::new();
        sections.insert("unb".into(), RecVal::Map(Box::new(unb)));
        let ctx = Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from("orders.edi"),
            sections,
        ));
        let mut record = body(&s, "BGM", "M1", "ORDERS", &["220"]);
        record.set_doc_ctx(ctx);

        let cfg = EdifactWriterConfig {
            interchange_from_doc: Some("unb".into()),
            segment_newline: false,
            ..Default::default()
        };
        let out = write_all(cfg, &[record], &s);
        // The full UNB is reconstructed — the empty middle element is not
        // dropped, so nothing after it is lost.
        assert!(
            out.starts_with("UNB+UNOA:1+S+R++240101:1200+REF9'"),
            "UNB was truncated at the empty element: {out}"
        );
        // The UNZ control-reference echo is taken from the UNB control
        // reference (the 5th data element, index 4) and is self-consistent
        // with the reconstructed UNB, so a re-read validates.
        assert!(out.contains("UNZ+1+240101:1200'"), "{out}");
    }

    #[test]
    fn unz_echoes_control_ref_shifted_past_empty_padding() {
        use clinker_record::{DocumentContext, DocumentId, Value as RecVal};
        use indexmap::IndexMap;

        // A UNB with all four mandatory leading composites populated plus
        // an empty optional element (index 4) ahead of the control
        // reference, which shifts "REF1" to index 5. A fixed index-4
        // echo would emit the empty padding into UNZ, producing a trailer
        // that contradicts its own header; the structural locator echoes
        // the real reference so the output validates on re-read.
        let s = schema();
        let raw = RecVal::Array(vec![
            RecVal::String("UNOA:1".into()),
            RecVal::String("S".into()),
            RecVal::String("R".into()),
            RecVal::String("240101:1200".into()),
            RecVal::String("".into()),
            RecVal::String("REF1".into()),
        ]);
        let mut unb: IndexMap<Box<str>, RecVal> = IndexMap::new();
        unb.insert(super::RAW_ELEMENTS_KEY.into(), raw);
        let mut sections: IndexMap<Box<str>, RecVal> = IndexMap::new();
        sections.insert("unb".into(), RecVal::Map(Box::new(unb)));
        let ctx = Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from("orders.edi"),
            sections,
        ));
        let mut record = body(&s, "BGM", "M1", "ORDERS", &["220"]);
        record.set_doc_ctx(ctx);

        let cfg = EdifactWriterConfig {
            interchange_from_doc: Some("unb".into()),
            segment_newline: false,
            ..Default::default()
        };
        let out = write_all(cfg, &[record], &s);
        assert!(
            out.starts_with("UNB+UNOA:1+S+R+240101:1200++REF1'"),
            "UNB header not reconstructed verbatim: {out}"
        );
        // UNZ echoes the structurally-located control reference, not the
        // empty padding at index 4, so the trailer matches its header.
        assert!(out.contains("UNZ+1+REF1'"), "{out}");
    }

    #[test]
    fn unz_echoes_control_ref_past_split_date_time() {
        use clinker_record::{DocumentContext, DocumentId, Value as RecVal};
        use indexmap::IndexMap;

        // A UNB whose S004 date/time is transmitted as two separate
        // elements (date "240101" at index 3, time "1200" at index 4)
        // rather than the conformant single "240101:1200" composite. The
        // true control reference "REF1" sits at index 5. Reconstructing the
        // header from the stashed element list must keep the split verbatim,
        // and the UNZ echo must skip the time part to name the real
        // reference so the output validates on re-read.
        let s = schema();
        let raw = RecVal::Array(vec![
            RecVal::String("UNOA:1".into()),
            RecVal::String("SENDER".into()),
            RecVal::String("RECEIVER".into()),
            RecVal::String("240101".into()),
            RecVal::String("1200".into()),
            RecVal::String("REF1".into()),
        ]);
        let mut unb: IndexMap<Box<str>, RecVal> = IndexMap::new();
        unb.insert(super::RAW_ELEMENTS_KEY.into(), raw);
        let mut sections: IndexMap<Box<str>, RecVal> = IndexMap::new();
        sections.insert("unb".into(), RecVal::Map(Box::new(unb)));
        let ctx = Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from("orders.edi"),
            sections,
        ));
        let mut record = body(&s, "BGM", "M1", "ORDERS", &["220"]);
        record.set_doc_ctx(ctx);

        let cfg = EdifactWriterConfig {
            interchange_from_doc: Some("unb".into()),
            segment_newline: false,
            ..Default::default()
        };
        let out = write_all(cfg, &[record], &s);
        assert!(
            out.starts_with("UNB+UNOA:1+SENDER+RECEIVER+240101+1200+REF1'"),
            "split date/time UNB not reconstructed verbatim: {out}"
        );
        // UNZ echoes the real reference, not the time part "1200".
        assert!(out.contains("UNZ+1+REF1'"), "{out}");
    }

    #[test]
    fn una_emitted_when_enabled() {
        let s = schema();
        let mut cfg = literal_config();
        cfg.write_una = true;
        let out = write_all(cfg, &[body(&s, "BGM", "M1", "ORDERS", &["220"])], &s);
        assert!(out.starts_with("UNA:+.? '"));
    }

    #[test]
    fn missing_interchange_config_errors() {
        let s = schema();
        let cfg = EdifactWriterConfig {
            segment_newline: false,
            ..Default::default()
        };
        let mut buf = Vec::new();
        let mut w = EdifactWriter::new(Cursor::new(&mut buf), Arc::clone(&s), cfg);
        let err = w
            .write_record(&body(&s, "BGM", "M1", "ORDERS", &["220"]))
            .unwrap_err();
        assert!(matches!(err, FormatError::Edifact(m) if m.contains("no interchange header")));
    }

    #[test]
    fn unt_counts_include_unh_and_unt_and_echo_msg_ref() {
        let s = schema();
        let out = write_all(
            literal_config(),
            &[
                body(&s, "BGM", "M1", "ORDERS", &["220"]),
                body(&s, "NAD", "M1", "ORDERS", &["BY"]),
            ],
            &s,
        );
        // UNH + BGM + NAD + UNT = 4 segments; UNT echoes M1.
        assert!(out.contains("UNT+4+M1'"), "output was: {out}");
    }

    #[test]
    fn unz_count_and_unb_ref_echo() {
        let s = schema();
        let out = write_all(
            literal_config(),
            &[
                body(&s, "BGM", "M1", "ORDERS", &["220"]),
                body(&s, "BGM", "M2", "ORDERS", &["220"]),
            ],
            &s,
        );
        assert!(out.contains("UNZ+2+REF1'"), "output was: {out}");
    }

    #[test]
    fn message_grouping_on_msg_ref_transition() {
        let s = schema();
        let out = write_all(
            literal_config(),
            &[
                body(&s, "BGM", "M1", "ORDERS", &["a"]),
                body(&s, "BGM", "M2", "ORDERS", &["b"]),
            ],
            &s,
        );
        assert_eq!(out.matches("UNH+").count(), 2);
        assert!(out.contains("UNH+M1+ORDERS'"));
        assert!(out.contains("UNH+M2+ORDERS'"));
    }

    #[test]
    fn trailing_nulls_not_padded() {
        let s = schema();
        let out = write_all(
            literal_config(),
            &[body(&s, "BGM", "M1", "ORDERS", &["220"])],
            &s,
        );
        // BGM has one element; no trailing '+' delimiters for the null
        // e02..e04 columns.
        assert!(out.contains("BGM+220'"), "output was: {out}");
        assert!(!out.contains("BGM+220+'"));
    }

    #[test]
    fn element_separator_in_data_is_release_escaped() {
        // A literal '+' in element data must be emitted as ?+ so the
        // reader does not split it into a second element.
        let s = schema();
        let out = write_all(
            literal_config(),
            &[body(&s, "BGM", "M1", "ORDERS", &["A+B"])],
            &s,
        );
        assert!(out.contains("BGM+A?+B'"), "output was: {out}");
    }

    #[test]
    fn terminator_in_data_is_release_escaped() {
        // A literal apostrophe in element data must be emitted as ?' so
        // it does not terminate the segment early.
        let s = schema();
        let out = write_all(
            literal_config(),
            &[body(&s, "FTX", "M1", "ORDERS", &["O'BRIEN"])],
            &s,
        );
        assert!(out.contains("FTX+O?'BRIEN'"), "output was: {out}");
    }

    #[test]
    fn release_char_in_data_is_doubled() {
        // A literal '?' in element data must be emitted as ?? so the
        // reader decodes it back to a single '?'.
        let s = schema();
        let out = write_all(
            literal_config(),
            &[body(&s, "FTX", "M1", "ORDERS", &["why?"])],
            &s,
        );
        assert!(out.contains("FTX+why??'"), "output was: {out}");
    }

    #[test]
    fn component_separator_in_data_is_not_escaped() {
        // The composite element "UNOA:1" must re-emit unchanged — the ':'
        // is structural, not a data byte to escape.
        let s = schema();
        let out = write_all(
            literal_config(),
            &[body(&s, "UNH", "M1", "ORDERS:D:96A:UN", &["UNOA:1"])],
            &s,
        );
        assert!(out.contains("UNOA:1"), "output was: {out}");
        assert!(
            !out.contains("UNOA?:1"),
            "component sep was wrongly escaped: {out}"
        );
    }

    #[test]
    fn engine_stamped_columns_excluded() {
        // A `$`-prefixed engine column on the schema must not trip the
        // unknown-column rejection, and must not be emitted.
        let cols: Vec<Box<str>> = vec![
            "seg_id".into(),
            "msg_ref".into(),
            "msg_type".into(),
            "e01".into(),
            "$source.file".into(),
        ];
        let schema = Arc::new(Schema::new(cols));
        let values = vec![
            Value::String("BGM".into()),
            Value::String("M1".into()),
            Value::String("ORDERS".into()),
            Value::String("220".into()),
            Value::String("/tmp/x.edi".into()),
        ];
        let record = Record::new(Arc::clone(&schema), values);
        let out = write_all(literal_config(), &[record], &schema);
        assert!(out.contains("BGM+220'"));
        assert!(!out.contains("/tmp/x.edi"));
    }

    #[test]
    fn unrecognized_column_errors_by_name() {
        let cols: Vec<Box<str>> = vec![
            "seg_id".into(),
            "msg_ref".into(),
            "msg_type".into(),
            "amount".into(),
        ];
        let schema = Arc::new(Schema::new(cols));
        let record = Record::new(
            Arc::clone(&schema),
            vec![
                Value::String("BGM".into()),
                Value::String("M1".into()),
                Value::String("ORDERS".into()),
                Value::String("99".into()),
            ],
        );
        let mut buf = Vec::new();
        let mut w =
            EdifactWriter::new(Cursor::new(&mut buf), Arc::clone(&schema), literal_config());
        let err = w.write_record(&record).unwrap_err();
        assert!(matches!(err, FormatError::Edifact(m) if m.contains("amount")));
    }

    #[test]
    fn flush_finalize_idempotent() {
        let s = schema();
        let mut buf = Vec::new();
        let mut w = EdifactWriter::new(Cursor::new(&mut buf), Arc::clone(&s), literal_config());
        w.write_record(&body(&s, "BGM", "M1", "ORDERS", &["220"]))
            .unwrap();
        w.flush().unwrap();
        w.flush().unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert_eq!(out.matches("UNZ+").count(), 1, "output was: {out}");
    }

    #[test]
    fn zero_records_writes_nothing() {
        let s = schema();
        let out = write_all(literal_config(), &[], &s);
        assert!(out.is_empty(), "output was: {out}");
    }

    #[test]
    fn gapped_element_columns_map_by_position_not_schema_order() {
        // A schema declaring only `seg_id, e01, e03` must place e03's value
        // at wire element 3 with element 2 empty — never collapse it left
        // into element 2.
        let cols: Vec<Box<str>> = vec!["seg_id".into(), "e01".into(), "e03".into()];
        let schema = Arc::new(Schema::new(cols));
        let record = Record::new(
            Arc::clone(&schema),
            vec![
                Value::String("BGM".into()),
                Value::String("a".into()),
                Value::String("c".into()),
            ],
        );
        let mut cfg = literal_config();
        cfg.message_type = Some("ORDERS".into());
        let out = write_all(cfg, &[record], &schema);
        assert!(out.contains("BGM+a++c'"), "gapped mapping wrong: {out}");
    }

    #[test]
    fn reordered_element_columns_emit_in_position_order() {
        // A schema declaring `e02` before `e01` must still emit e01 first.
        let cols: Vec<Box<str>> = vec!["seg_id".into(), "e02".into(), "e01".into()];
        let schema = Arc::new(Schema::new(cols));
        let record = Record::new(
            Arc::clone(&schema),
            vec![
                Value::String("BGM".into()),
                Value::String("second".into()),
                Value::String("first".into()),
            ],
        );
        let mut cfg = literal_config();
        cfg.message_type = Some("ORDERS".into());
        let out = write_all(cfg, &[record], &schema);
        assert!(
            out.contains("BGM+first+second'"),
            "reordered mapping wrong: {out}"
        );
    }

    #[test]
    fn map_value_in_element_column_errors_by_name() {
        use indexmap::IndexMap;
        // A nested object in an `eNN` column has no scalar EDIFACT element
        // form; the writer must reject it explicitly rather than emit an
        // empty element.
        let cols: Vec<Box<str>> = vec!["seg_id".into(), "e01".into()];
        let schema = Arc::new(Schema::new(cols));
        let mut nested: IndexMap<Box<str>, Value> = IndexMap::new();
        nested.insert("k".into(), Value::String("v".into()));
        let record = Record::new(
            Arc::clone(&schema),
            vec![Value::String("BGM".into()), Value::Map(Box::new(nested))],
        );
        let mut cfg = literal_config();
        cfg.message_type = Some("ORDERS".into());
        let mut buf = Vec::new();
        let mut w = EdifactWriter::new(Cursor::new(&mut buf), Arc::clone(&schema), cfg);
        let err = w.write_record(&record).unwrap_err();
        assert!(
            matches!(&err, FormatError::UnserializableMapValue { format: "edifact", column } if column == "e01"),
            "expected UnserializableMapValue for e01, got: {err:?}"
        );
    }
}
