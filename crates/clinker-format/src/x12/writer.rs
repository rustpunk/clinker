//! ANSI ASC X12 interchange writer.
//!
//! Reconstructs the three-tier interchange envelope around emitted body
//! records: an `ISA` header (literal config elements or echoed from a
//! `$doc` section), one `GS..GE` functional group, `ST..SE` transaction
//! sets grouped on the `set_ref` column, and a closing `IEA`. Record
//! columns map positionally — `seg_id` is the segment tag, `e01..` are the
//! data elements.
//!
//! X12 has **no** release/escape character: a data value carrying a
//! delimiter byte (the element separator, the sub-element separator, or
//! the segment terminator) is unrepresentable. Rather than emit a
//! structurally corrupt interchange, the writer rejects such a value with
//! a precise error naming the offending column.
//!
//! Memory model: streaming and O(1) in held state — only the currently
//! open transaction set's segment count, the functional group's set count,
//! and the interchange group count are retained. `flush` is the single
//! end-of-stream finalizer: it closes the open set, the group, and writes
//! the `IEA` trailer with recomputed counts, exactly once. An interchange
//! is one envelope, so byte-limit file splitting (which would flush — and
//! thus finalize — mid-stream) is rejected for X12 outputs at
//! config-validation time; this writer is therefore only ever flushed at
//! true end of stream.

use std::io::Write;
use std::sync::Arc;

use clinker_record::{Record, Schema, Value};

use crate::error::FormatError;
use crate::traits::FormatWriter;
use crate::x12::RAW_ELEMENTS_KEY;
use crate::x12::charset::Charset;
use crate::x12::tokenizer::Delimiters;

/// Default X12 service delimiters used when no `ISA` header dictates
/// otherwise: `*` element, `:` sub-element, `~` terminator. When the
/// header is echoed from a `$doc` section, its `ISA16` sub-element byte
/// overrides the default; the literal-`interchange` path keeps these
/// defaults.
fn default_delimiters() -> Delimiters {
    Delimiters {
        element: b'*',
        subelement: b':',
        terminator: b'~',
    }
}

/// Configuration for the X12 writer.
#[derive(Clone, Default)]
pub struct X12WriterConfig {
    /// Literal `ISA` data elements (the 16 fixed-width ISA fields). When
    /// set, the writer emits exactly these as the interchange header.
    /// Takes precedence over `interchange_from_doc`.
    pub interchange: Option<Vec<String>>,
    /// Name of a `$doc` section to echo the `ISA` elements from (the
    /// reader's `ISA` positional section), enabling round-trip
    /// reconstruction. Used only when `interchange` is unset.
    pub interchange_from_doc: Option<String>,
    /// Literal `GS` functional-group header elements (`GS01..GS08`). The
    /// `GS06` group control number is recomputed by the writer, so the
    /// configured value at that position is overwritten. Required to open
    /// a functional group.
    pub group_header: Option<Vec<String>>,
    /// Fallback transaction set type (`ST01`) when a record carries no
    /// `set_type` column value.
    pub set_type: Option<String>,
    /// Write a newline after each segment terminator for readability. The
    /// YAML option default is `true`, applied by the config builder; the
    /// plain `Default` for this struct leaves it `false`, so a caller
    /// constructing the config directly sets it explicitly.
    pub segment_newline: bool,
    /// Character set element text is encoded through. Defaults to UTF-8; set
    /// to match the reader's charset so a non-UTF-8 interchange round-trips
    /// byte-faithfully. A character the charset cannot represent is rejected
    /// rather than emitted truncated.
    pub charset: Charset,
}

/// Streaming X12 interchange writer.
pub struct X12Writer<W: Write> {
    writer: W,
    config: X12WriterConfig,
    delims: Delimiters,
    /// Wire element columns keyed by the 1-based position parsed from the
    /// column name (`e01` → 1), each paired with its schema index. The
    /// numeric suffix — not schema-declaration order — determines the wire
    /// slot, so a gapped (`e01, e03`) or reordered (`e02, e01`) schema maps
    /// values to the positions the names declare, with gaps emitted as
    /// empty elements rather than silently shifting later values left.
    element_columns: Vec<ElementColumn>,
    seg_id_idx: Option<usize>,
    set_ref_idx: Option<usize>,
    set_type_idx: Option<usize>,
    header_written: bool,
    finalized: bool,
    /// `ISA13` interchange control number, echoed in the `IEA02` trailer.
    isa_control_number: String,
    /// `GS06` group control number for the single open functional group.
    group_control_number: String,
    /// Count of `ST` transaction sets written, for the `GE01` count.
    set_count: u64,
    /// Count of `GS` functional groups written, for the `IEA01` count.
    group_count: u64,
    open_set: Option<OpenSet>,
    group_open: bool,
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

/// State for the transaction set currently being written.
struct OpenSet {
    /// `ST02` control number echoed by `SE02`.
    control_number: String,
    /// Segments written so far including `ST` (the closing `SE` adds one
    /// more when the set closes).
    segment_count: u64,
}

impl<W: Write> X12Writer<W> {
    /// Build a writer over a sink with the given schema and config. The
    /// schema's `seg_id` / `set_ref` / `set_type` / `eNN` columns are
    /// resolved to positional indices once.
    pub fn new(writer: W, schema: Arc<Schema>, config: X12WriterConfig) -> Self {
        let mut element_columns = Vec::new();
        let mut seg_id_idx = None;
        let mut set_ref_idx = None;
        let mut set_type_idx = None;
        for (i, col) in schema.columns().iter().enumerate() {
            match &**col {
                "seg_id" => seg_id_idx = Some(i),
                "set_ref" => set_ref_idx = Some(i),
                "set_type" => set_type_idx = Some(i),
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
        element_columns.sort_by_key(|c| c.position);
        Self {
            writer,
            config,
            delims: default_delimiters(),
            element_columns,
            seg_id_idx,
            set_ref_idx,
            set_type_idx,
            header_written: false,
            finalized: false,
            isa_control_number: String::new(),
            group_control_number: String::new(),
            set_count: 0,
            group_count: 0,
            open_set: None,
            group_open: false,
        }
    }

    /// Emit the `ISA` interchange header and the `GS` functional-group
    /// header on the first record.
    fn write_header(&mut self, record: &Record) -> Result<(), FormatError> {
        if self.header_written {
            return Ok(());
        }
        self.header_written = true;

        let isa_elements = self.resolve_isa_elements(record)?;
        // The ISA control number (ISA13, element index 12) is echoed in
        // the IEA trailer; default to empty when the header is short.
        self.isa_control_number = isa_elements.get(12).cloned().unwrap_or_default();
        // The ISA is written verbatim with the writer's element separator
        // and terminator; its fields are fixed-width control values, never
        // trailing-empty. ISA16 declares the sub-element separator, but the
        // writer keeps the element separator and terminator at their
        // defaults so the header is self-consistent on re-read.
        self.write_segment("ISA", &isa_elements)?;
        self.open_functional_group()?;
        Ok(())
    }

    /// Resolve the `ISA` data elements: the literal config list wins;
    /// otherwise echo from the named `$doc` section on the record;
    /// otherwise a precise configuration error.
    fn resolve_isa_elements(&self, record: &Record) -> Result<Vec<String>, FormatError> {
        if let Some(literal) = &self.config.interchange {
            return Ok(literal.clone());
        }
        if let Some(section) = &self.config.interchange_from_doc {
            let ctx = record.doc_ctx();
            // Preferred path: the X12 reader stashes the complete, ordered
            // ISA element list under an engine-internal key. Reconstructing
            // from it round-trips an ISA header faithfully without the user
            // declaring every `eNN` field.
            if let Some(Value::Array(raw)) = ctx.get_section_field(section, RAW_ELEMENTS_KEY) {
                return raw
                    .iter()
                    .enumerate()
                    .map(|(i, v)| value_to_element(v, &format!("{section}.e{:02}", i + 1)))
                    .collect();
            }
            // Fallback for a section not produced by the X12 reader (e.g.
            // literal author-supplied `$doc` fields): walk the declared
            // positional `e01, e02, …` keys until one is absent.
            let mut elements: Vec<String> = Vec::new();
            for i in 0.. {
                let key = format!("e{:02}", i + 1);
                match ctx.get_section_field(section, &key) {
                    Some(v) => elements.push(value_to_element(&v, &format!("{section}.{key}"))?),
                    None => break,
                }
            }
            if elements.is_empty() {
                return Err(FormatError::X12(format!(
                    "interchange_from_doc names section {section:?}, but the record's \
                     document context carries no ISA elements for it. Ensure the source \
                     declares a `segment: \"ISA\"` envelope section of the same name."
                )));
            }
            return Ok(elements);
        }
        Err(FormatError::X12(
            "no interchange header configured: set the writer's `interchange` literal \
             elements or `interchange_from_doc` section name so an ISA header can be written"
                .into(),
        ))
    }

    /// Open the single functional group from the configured `group_header`,
    /// recomputing its `GS06` control number.
    fn open_functional_group(&mut self) -> Result<(), FormatError> {
        let header = self.config.group_header.clone().ok_or_else(|| {
            FormatError::X12(
                "no functional-group header configured: set the writer's `group_header` \
                 (GS01..GS08 elements) so a GS..GE group can wrap the transaction sets"
                    .into(),
            )
        })?;
        self.group_count += 1;
        // GS06 (element index 5) is the group control number; recompute it
        // so it is unique per group and echoed correctly by GE.
        self.group_control_number = self.group_count.to_string();
        let mut gs = header;
        set_or_push(&mut gs, 5, self.group_control_number.clone());
        self.write_segment("GS", &gs)?;
        self.group_open = true;
        Ok(())
    }

    /// Map a body record to its segment tag and element list, then write
    /// it, opening or closing `ST..SE` transaction sets on `set_ref`
    /// transitions.
    fn write_body_record(&mut self, record: &Record) -> Result<(), FormatError> {
        self.reject_unknown_columns(record)?;
        let values = record.values();
        let seg_id = match self.seg_id_idx.and_then(|i| values.get(i)) {
            Some(v) => value_to_element(v, "seg_id")?,
            None => String::new(),
        };
        let set_ref = match self.set_ref_idx.and_then(|i| values.get(i)) {
            Some(v) => value_to_element(v, "set_ref")?,
            None => String::new(),
        };
        let set_type = match self.set_type_idx.and_then(|i| values.get(i)) {
            Some(v) => value_to_element(v, "set_type")?,
            None => String::new(),
        };

        // Service segments embedded in the record stream are skipped — the
        // writer reconstructs envelopes itself, so an ISA/GS/ST/SE/GE/IEA
        // body record (e.g. from an X12→X12 pipeline that emits them
        // verbatim) must not be double-written, but still drives set
        // grouping when it is an ST.
        if matches!(seg_id.as_str(), "ISA" | "IEA" | "GS" | "GE" | "ST" | "SE") {
            self.transition_set(&set_ref, &set_type)?;
            return Ok(());
        }

        self.transition_set(&set_ref, &set_type)?;

        let elements = self.record_elements(record)?;
        self.write_segment(&seg_id, &elements)?;
        if let Some(set) = self.open_set.as_mut() {
            set.segment_count += 1;
        }
        Ok(())
    }

    /// Open a new `ST` transaction set (closing any previous one) when the
    /// `set_ref` changes, or open the first set. A record with an empty
    /// `set_ref` joins the current set; if none is open, it opens an
    /// anonymous single set.
    fn transition_set(&mut self, set_ref: &str, set_type: &str) -> Result<(), FormatError> {
        let need_new = match &self.open_set {
            None => true,
            Some(open) => !set_ref.is_empty() && open.control_number != *set_ref,
        };
        if !need_new {
            return Ok(());
        }
        self.close_open_set()?;

        let resolved_type = if !set_type.is_empty() {
            set_type.to_string()
        } else if let Some(t) = &self.config.set_type {
            t.clone()
        } else {
            return Err(FormatError::X12(
                "cannot open an ST transaction set: the record carries no `set_type` and \
                 the writer has no `set_type` fallback configured"
                    .into(),
            ));
        };
        let control_number = if set_ref.is_empty() {
            format!("{:04}", self.set_count + 1)
        } else {
            set_ref.to_string()
        };
        self.write_segment("ST", &[resolved_type, control_number.clone()])?;
        self.set_count += 1;
        self.open_set = Some(OpenSet {
            control_number,
            segment_count: 1,
        });
        Ok(())
    }

    /// Close the open transaction set with its recomputed `SE` trailer
    /// (segment count including `ST`+`SE`, set control-number echo).
    fn close_open_set(&mut self) -> Result<(), FormatError> {
        if let Some(set) = self.open_set.take() {
            let count = set.segment_count + 1;
            self.write_segment("SE", &[count.to_string(), set.control_number])?;
        }
        Ok(())
    }

    /// Close the open functional group with its recomputed `GE` trailer
    /// (transaction-set count, group control-number echo).
    fn close_functional_group(&mut self) -> Result<(), FormatError> {
        if self.group_open {
            self.group_open = false;
            self.write_segment(
                "GE",
                &[
                    self.set_count.to_string(),
                    self.group_control_number.clone(),
                ],
            )?;
        }
        Ok(())
    }

    /// Collect a record's `eNN` columns into wire-position order, filling
    /// any gap left by a missing position with an empty element, then
    /// trimming trailing empties so no fabricated delimiters appear.
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
        // Trim trailing empties so no fabricated delimiters appear after the
        // last populated element.
        while matches!(elements.last(), Some(s) if s.is_empty()) {
            elements.pop();
        }
        Ok(elements)
    }

    /// Reject a record carrying a user column the writer does not map.
    /// Engine-namespaced columns (`$`-prefixed) are excluded from the
    /// check and never appear in X12 output by design.
    fn reject_unknown_columns(&self, record: &Record) -> Result<(), FormatError> {
        for (name, _) in record.iter_user_fields() {
            if name.starts_with('$') {
                continue;
            }
            let known =
                matches!(name, "seg_id" | "set_ref" | "set_type") || is_element_column(name);
            if !known {
                return Err(FormatError::X12(format!(
                    "record column {name:?} has no X12 mapping. The writer maps only \
                     `seg_id`, `set_ref`, `set_type`, and positional `eNN` element \
                     columns; project the record to those before output"
                )));
            }
        }
        Ok(())
    }

    /// Write one segment verbatim: tag, element separator, elements joined
    /// by the element separator, terminator, optional newline.
    ///
    /// The caller is responsible for the element list's shape — body
    /// records trim their trailing empties in [`Self::record_elements`]
    /// before reaching here, while the fixed-shape `ISA` header and the
    /// count-bearing trailers (`SE`/`GE`/`IEA`) carry no trailing empties to
    /// trim. X12 has no escape mechanism, so an element value carrying a
    /// delimiter byte is rejected rather than corrupting the interchange.
    fn write_segment(&mut self, tag: &str, elements: &[String]) -> Result<(), FormatError> {
        self.writer
            .write_all(tag.as_bytes())
            .map_err(FormatError::Io)?;
        for element in elements {
            self.reject_delimiter_in_data(element, tag)?;
            self.writer
                .write_all(&[self.delims.element])
                .map_err(FormatError::Io)?;
            // Encode element text through the configured charset so a
            // non-UTF-8 interchange round-trips byte-faithfully; a character
            // the charset cannot represent is rejected, not truncated.
            let encoded = self.config.charset.encode(element)?;
            self.writer.write_all(&encoded).map_err(FormatError::Io)?;
        }
        self.writer
            .write_all(&[self.delims.terminator])
            .map_err(FormatError::Io)?;
        if self.config.segment_newline {
            self.writer.write_all(b"\n").map_err(FormatError::Io)?;
        }
        Ok(())
    }

    /// Reject an element value that carries a structural delimiter byte. X12
    /// cannot escape a delimiter inside data, so a value containing the
    /// element separator or the segment terminator would silently corrupt
    /// the interchange. The sub-element (component) separator is deliberately
    /// *not* rejected — a composite element value legitimately carries it as
    /// internal structure, mirroring the reader, which keeps it verbatim. The
    /// error names the offending segment so the user can re-route or
    /// re-encode the value.
    fn reject_delimiter_in_data(&self, element: &str, tag: &str) -> Result<(), FormatError> {
        for &b in element.as_bytes() {
            if b == self.delims.element || b == self.delims.terminator {
                return Err(FormatError::X12(format!(
                    "segment {tag:?} element value {element:?} contains the X12 \
                     {} delimiter byte {:?}. X12 has no escape character, so a delimiter \
                     cannot appear inside data; re-encode the value or choose delimiters \
                     the data does not contain.",
                    if b == self.delims.element {
                        "element"
                    } else {
                        "segment-terminator"
                    },
                    b as char
                )));
            }
        }
        Ok(())
    }
}

impl<W: Write + Send> FormatWriter for X12Writer<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        self.write_header(record)?;
        self.write_body_record(record)
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        if self.finalized {
            return Ok(());
        }
        self.finalized = true;
        self.close_open_set()?;
        self.close_functional_group()?;
        // Only write IEA if a header was actually emitted; a writer that
        // saw zero records produces nothing rather than a bare trailer.
        if self.header_written {
            let control_number = self.isa_control_number.clone();
            self.write_segment("IEA", &[self.group_count.to_string(), control_number])?;
        }
        self.writer.flush().map_err(FormatError::Io)?;
        Ok(())
    }
}

/// Set element at index `i` (0-based) to `value`, growing the vector with
/// empty elements if it is shorter than `i`. Used to overwrite the `GS06`
/// control number at its fixed position regardless of the configured
/// header's length.
fn set_or_push(elements: &mut Vec<String>, i: usize, value: String) {
    while elements.len() <= i {
        elements.push(String::new());
    }
    elements[i] = value;
}

/// Whether a schema column name is a positional X12 element column
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

/// Render a `Value` to X12 element text. `Null` renders as the empty
/// string (an absent element); scalars render via their natural string
/// form. A `Value::Map` or `Value::Array` has no scalar X12 element
/// representation, so it raises rather than silently emitting an empty
/// cell — mirroring the CSV/XML/fixed-width/EDIFACT writers'
/// explicit-error posture for non-scalar payloads. `column` names the
/// offending field for the error.
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
                format: "x12",
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

    const ISA_ELEMENTS: &[&str] = &[
        "00",
        "          ",
        "00",
        "          ",
        "ZZ",
        "SENDER         ",
        "ZZ",
        "RECEIVER       ",
        "240101",
        "1200",
        "U",
        "00401",
        "000000001",
        "0",
        "P",
        ":",
    ];

    const GS_HEADER: &[&str] = &[
        "PO", "SENDER", "RECEIVER", "20240101", "1200", "1", "X", "004010",
    ];

    fn schema() -> Arc<Schema> {
        let mut cols: Vec<Box<str>> = vec!["seg_id".into(), "set_ref".into(), "set_type".into()];
        for i in 1..=4 {
            cols.push(format!("e{i:02}").into_boxed_str());
        }
        Arc::new(Schema::new(cols))
    }

    fn body(
        schema: &Arc<Schema>,
        seg: &str,
        set_ref: &str,
        set_type: &str,
        els: &[&str],
    ) -> Record {
        let mut values = vec![
            Value::String(seg.into()),
            if set_ref.is_empty() {
                Value::Null
            } else {
                Value::String(set_ref.into())
            },
            if set_type.is_empty() {
                Value::Null
            } else {
                Value::String(set_type.into())
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

    fn literal_config() -> X12WriterConfig {
        X12WriterConfig {
            interchange: Some(ISA_ELEMENTS.iter().map(|s| s.to_string()).collect()),
            group_header: Some(GS_HEADER.iter().map(|s| s.to_string()).collect()),
            segment_newline: false,
            ..Default::default()
        }
    }

    fn write_all(config: X12WriterConfig, records: &[Record], schema: &Arc<Schema>) -> String {
        let mut buf = Vec::new();
        let mut w = X12Writer::new(Cursor::new(&mut buf), Arc::clone(schema), config);
        for r in records {
            w.write_record(r).unwrap();
        }
        w.flush().unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn isa_from_literal_options() {
        let s = schema();
        let out = write_all(
            literal_config(),
            &[body(&s, "BEG", "0001", "850", &["00"])],
            &s,
        );
        assert!(
            out.starts_with(
                "ISA*00*          *00*          *ZZ*SENDER         \
                 *ZZ*RECEIVER       *240101*1200*U*00401*000000001*0*P*:~"
            ),
            "{out}"
        );
    }

    #[test]
    fn three_tier_envelope_reconstructed() {
        let s = schema();
        let out = write_all(
            literal_config(),
            &[
                body(&s, "BEG", "0001", "850", &["00"]),
                body(&s, "PO1", "0001", "850", &["1"]),
            ],
            &s,
        );
        // GS opens, ST opens, body, SE closes (ST+BEG+PO1+SE = 4), GE
        // closes (1 set), IEA closes (1 group, echoing ISA13).
        assert!(
            out.contains("GS*PO*SENDER*RECEIVER*20240101*1200*1*X*004010~"),
            "{out}"
        );
        assert!(out.contains("ST*850*0001~"), "{out}");
        assert!(out.contains("SE*4*0001~"), "{out}");
        assert!(out.contains("GE*1*1~"), "{out}");
        assert!(out.contains("IEA*1*000000001~"), "{out}");
    }

    #[test]
    fn isa_echoed_from_doc_section() {
        use clinker_record::{DocumentContext, DocumentId, Value as RecVal};
        use indexmap::IndexMap;

        let s = schema();
        let raw = RecVal::Array(
            ISA_ELEMENTS
                .iter()
                .map(|e| RecVal::String((*e).into()))
                .collect(),
        );
        let mut isa: IndexMap<Box<str>, RecVal> = IndexMap::new();
        isa.insert(super::RAW_ELEMENTS_KEY.into(), raw);
        let mut sections: IndexMap<Box<str>, RecVal> = IndexMap::new();
        sections.insert("interchange".into(), RecVal::Map(Box::new(isa)));
        let ctx = Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from("orders.x12"),
            sections,
        ));

        let mut record = body(&s, "BEG", "0001", "850", &["00"]);
        record.set_doc_ctx(ctx);

        let cfg = X12WriterConfig {
            interchange_from_doc: Some("interchange".into()),
            group_header: Some(GS_HEADER.iter().map(|s| s.to_string()).collect()),
            segment_newline: false,
            ..Default::default()
        };
        let out = write_all(cfg, &[record], &s);
        assert!(out.contains("000000001"), "{out}");
        assert!(out.contains("IEA*1*000000001~"), "{out}");
    }

    #[test]
    fn se_counts_include_st_and_se_and_echo_set_ref() {
        let s = schema();
        let out = write_all(
            literal_config(),
            &[
                body(&s, "BEG", "0001", "850", &["00"]),
                body(&s, "PO1", "0001", "850", &["1"]),
            ],
            &s,
        );
        // ST + BEG + PO1 + SE = 4 segments; SE echoes 0001.
        assert!(out.contains("SE*4*0001~"), "{out}");
    }

    #[test]
    fn set_grouping_on_set_ref_transition() {
        let s = schema();
        let out = write_all(
            literal_config(),
            &[
                body(&s, "BEG", "0001", "850", &["a"]),
                body(&s, "BEG", "0002", "850", &["b"]),
            ],
            &s,
        );
        assert_eq!(out.matches("ST*").count(), 2);
        assert!(out.contains("ST*850*0001~"));
        assert!(out.contains("ST*850*0002~"));
        // Two sets in one group; GE echoes the set count.
        assert!(out.contains("GE*2*1~"), "{out}");
    }

    #[test]
    fn trailing_nulls_not_padded() {
        let s = schema();
        let out = write_all(
            literal_config(),
            &[body(&s, "BEG", "0001", "850", &["00"])],
            &s,
        );
        // BEG has one element; no trailing '*' delimiters for null e02..e04.
        assert!(out.contains("BEG*00~"), "{out}");
        assert!(!out.contains("BEG*00*~"));
    }

    #[test]
    fn gapped_element_columns_map_by_position() {
        let cols: Vec<Box<str>> = vec!["seg_id".into(), "e01".into(), "e03".into()];
        let schema = Arc::new(Schema::new(cols));
        let record = Record::new(
            Arc::clone(&schema),
            vec![
                Value::String("PO1".into()),
                Value::String("a".into()),
                Value::String("c".into()),
            ],
        );
        let mut cfg = literal_config();
        cfg.set_type = Some("850".into());
        let out = write_all(cfg, &[record], &schema);
        assert!(out.contains("PO1*a**c~"), "gapped mapping wrong: {out}");
    }

    #[test]
    fn reordered_element_columns_emit_in_position_order() {
        let cols: Vec<Box<str>> = vec!["seg_id".into(), "e02".into(), "e01".into()];
        let schema = Arc::new(Schema::new(cols));
        let record = Record::new(
            Arc::clone(&schema),
            vec![
                Value::String("PO1".into()),
                Value::String("second".into()),
                Value::String("first".into()),
            ],
        );
        let mut cfg = literal_config();
        cfg.set_type = Some("850".into());
        let out = write_all(cfg, &[record], &schema);
        assert!(
            out.contains("PO1*first*second~"),
            "reordered mapping wrong: {out}"
        );
    }

    #[test]
    fn delimiter_byte_in_data_is_rejected() {
        // X12 has no escape: an element value carrying the element
        // separator must be rejected, not silently corrupt the segment.
        let s = schema();
        let mut buf = Vec::new();
        let mut w = X12Writer::new(Cursor::new(&mut buf), Arc::clone(&s), literal_config());
        let err = w
            .write_record(&body(&s, "BEG", "0001", "850", &["A*B"]))
            .unwrap_err();
        assert!(
            matches!(&err, FormatError::X12(m) if m.contains("element") && m.contains("no escape")),
            "expected delimiter rejection, got {err:?}"
        );
    }

    #[test]
    fn terminator_byte_in_data_is_rejected() {
        let s = schema();
        let mut buf = Vec::new();
        let mut w = X12Writer::new(Cursor::new(&mut buf), Arc::clone(&s), literal_config());
        let err = w
            .write_record(&body(&s, "BEG", "0001", "850", &["A~B"]))
            .unwrap_err();
        assert!(
            matches!(&err, FormatError::X12(m) if m.contains("segment-terminator")),
            "expected terminator rejection, got {err:?}"
        );
    }

    #[test]
    fn missing_interchange_config_errors() {
        let s = schema();
        let cfg = X12WriterConfig {
            group_header: Some(GS_HEADER.iter().map(|s| s.to_string()).collect()),
            segment_newline: false,
            ..Default::default()
        };
        let mut buf = Vec::new();
        let mut w = X12Writer::new(Cursor::new(&mut buf), Arc::clone(&s), cfg);
        let err = w
            .write_record(&body(&s, "BEG", "0001", "850", &["00"]))
            .unwrap_err();
        assert!(matches!(err, FormatError::X12(m) if m.contains("no interchange header")));
    }

    #[test]
    fn missing_group_header_config_errors() {
        let s = schema();
        let cfg = X12WriterConfig {
            interchange: Some(ISA_ELEMENTS.iter().map(|s| s.to_string()).collect()),
            segment_newline: false,
            ..Default::default()
        };
        let mut buf = Vec::new();
        let mut w = X12Writer::new(Cursor::new(&mut buf), Arc::clone(&s), cfg);
        let err = w
            .write_record(&body(&s, "BEG", "0001", "850", &["00"]))
            .unwrap_err();
        assert!(matches!(err, FormatError::X12(m) if m.contains("no functional-group header")));
    }

    #[test]
    fn engine_stamped_columns_excluded() {
        let cols: Vec<Box<str>> = vec![
            "seg_id".into(),
            "set_ref".into(),
            "set_type".into(),
            "e01".into(),
            "$source.file".into(),
        ];
        let schema = Arc::new(Schema::new(cols));
        let values = vec![
            Value::String("BEG".into()),
            Value::String("0001".into()),
            Value::String("850".into()),
            Value::String("00".into()),
            Value::String("/tmp/x.x12".into()),
        ];
        let record = Record::new(Arc::clone(&schema), values);
        let out = write_all(literal_config(), &[record], &schema);
        assert!(out.contains("BEG*00~"));
        assert!(!out.contains("/tmp/x.x12"));
    }

    #[test]
    fn unrecognized_column_errors_by_name() {
        let cols: Vec<Box<str>> = vec![
            "seg_id".into(),
            "set_ref".into(),
            "set_type".into(),
            "amount".into(),
        ];
        let schema = Arc::new(Schema::new(cols));
        let record = Record::new(
            Arc::clone(&schema),
            vec![
                Value::String("BEG".into()),
                Value::String("0001".into()),
                Value::String("850".into()),
                Value::String("99".into()),
            ],
        );
        let mut buf = Vec::new();
        let mut w = X12Writer::new(Cursor::new(&mut buf), Arc::clone(&schema), literal_config());
        let err = w.write_record(&record).unwrap_err();
        assert!(matches!(err, FormatError::X12(m) if m.contains("amount")));
    }

    #[test]
    fn flush_finalize_idempotent() {
        let s = schema();
        let mut buf = Vec::new();
        let mut w = X12Writer::new(Cursor::new(&mut buf), Arc::clone(&s), literal_config());
        w.write_record(&body(&s, "BEG", "0001", "850", &["00"]))
            .unwrap();
        w.flush().unwrap();
        w.flush().unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert_eq!(out.matches("IEA*").count(), 1, "{out}");
    }

    #[test]
    fn zero_records_writes_nothing() {
        let s = schema();
        let out = write_all(literal_config(), &[], &s);
        assert!(out.is_empty(), "{out}");
    }

    #[test]
    fn map_value_in_element_column_errors_by_name() {
        use indexmap::IndexMap;
        let cols: Vec<Box<str>> = vec!["seg_id".into(), "e01".into()];
        let schema = Arc::new(Schema::new(cols));
        let mut nested: IndexMap<Box<str>, Value> = IndexMap::new();
        nested.insert("k".into(), Value::String("v".into()));
        let record = Record::new(
            Arc::clone(&schema),
            vec![Value::String("PO1".into()), Value::Map(Box::new(nested))],
        );
        let mut cfg = literal_config();
        cfg.set_type = Some("850".into());
        let mut buf = Vec::new();
        let mut w = X12Writer::new(Cursor::new(&mut buf), Arc::clone(&schema), cfg);
        let err = w.write_record(&record).unwrap_err();
        assert!(
            matches!(&err, FormatError::UnserializableMapValue { format: "x12", column } if column == "e01"),
            "expected UnserializableMapValue for e01, got: {err:?}"
        );
    }
}
