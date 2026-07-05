//! ANSI ASC X12 interchange writer.
//!
//! Reconstructs the three-tier interchange envelope around emitted body
//! records: an `ISA` header (literal config elements or echoed from a
//! `$doc` section), `GS..GE` functional groups grouped on the optional
//! `group_ref` column, `ST..SE` transaction sets grouped on the `set_ref`
//! column, and a closing `IEA`. Record columns map positionally — `seg_id`
//! is the segment tag, `e01..` are the data elements.
//!
//! A record stream carrying several distinct `group_ref` values writes one
//! `GS..GE` functional group per value inside the single `ISA..IEA`
//! interchange — a `PO` group of purchase orders and an `IN` group of
//! invoices, say, share one envelope. With no `group_ref` column the whole
//! stream collapses to a single functional group, byte-identical to a
//! stream whose `group_ref` never changes; the `group_ref` grouping is the
//! exact analog of how `set_ref` drives `ST..SE` boundaries one tier down.
//!
//! X12 has **no** release/escape character: a data value carrying a
//! delimiter byte (the element separator, the sub-element separator, or
//! the segment terminator) is unrepresentable. Rather than emit a
//! structurally corrupt interchange, the writer rejects such a value with
//! a precise error naming the offending column.
//!
//! Memory model: streaming and O(1) in held state — only the currently
//! open transaction set's segment count, the open functional group's set
//! count and control number, and the interchange group count are retained.
//! `flush` is the single end-of-stream finalizer: it closes the open set,
//! the open group, and writes the `IEA` trailer with recomputed counts,
//! exactly once. An interchange is one envelope, so byte-limit file
//! splitting (which would flush — and thus finalize — mid-stream) is
//! rejected for X12 outputs at config-validation time; this writer is
//! therefore only ever flushed at true end of stream.

use std::io::Write;
use std::sync::Arc;

use clinker_record::{Record, Schema, Value};

use crate::charset::Charset;
use crate::error::FormatError;
use crate::traits::FormatWriter;
use crate::x12::tokenizer::Delimiters;
use crate::x12::{DELIMITERS_KEY, RAW_ELEMENTS_KEY};

/// Default X12 service delimiters: `*` element, `:` sub-element, `~`
/// terminator. A `$doc` section produced by the X12 reader carries the
/// source interchange's discovered delimiter set, which the
/// `interchange_from_doc` path adopts wholesale before the first segment
/// is written; the literal-`interchange` path and a hand-authored section
/// (no delimiter stamp) keep these defaults.
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
    group_ref_idx: Option<usize>,
    header_written: bool,
    finalized: bool,
    /// `ISA13` interchange control number, echoed in the `IEA02` trailer.
    isa_control_number: String,
    /// Count of `GS` functional groups written, for the `IEA01` count.
    group_count: u64,
    open_set: Option<OpenSet>,
    open_group: Option<OpenGroup>,
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

/// State for the functional group currently being written. Carried per
/// group so the closing `GE01` reflects only the sets inside *this* group,
/// not the interchange-wide running total — the multi-group analog of how
/// [`OpenSet`] scopes the `SE01` segment count to a single set.
struct OpenGroup {
    /// The record-level `group_ref` value that opened the group, or empty
    /// when the stream carries no `group_ref` column. A non-empty value
    /// closes the group and opens a new one when it changes.
    group_ref: String,
    /// `GS06` group control number echoed by `GE02`.
    control_number: String,
    /// `ST` transaction sets opened in this group so far, for the `GE01`
    /// count and for deriving anonymous set control numbers within it.
    set_count: u64,
}

impl<W: Write> X12Writer<W> {
    /// Build a writer over a sink with the given schema and config. The
    /// schema's `seg_id` / `set_ref` / `set_type` / `group_ref` / `eNN`
    /// columns are resolved to positional indices once.
    pub fn new(writer: W, schema: Arc<Schema>, config: X12WriterConfig) -> Self {
        let mut element_columns = Vec::new();
        let mut seg_id_idx = None;
        let mut set_ref_idx = None;
        let mut set_type_idx = None;
        let mut group_ref_idx = None;
        for (i, col) in schema.columns().iter().enumerate() {
            match &**col {
                "seg_id" => seg_id_idx = Some(i),
                "set_ref" => set_ref_idx = Some(i),
                "set_type" => set_type_idx = Some(i),
                "group_ref" => group_ref_idx = Some(i),
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
            group_ref_idx,
            header_written: false,
            finalized: false,
            isa_control_number: String::new(),
            group_count: 0,
            open_set: None,
            open_group: None,
        }
    }

    /// Emit the `ISA` interchange header on the first record. The first
    /// `GS` functional group opens lazily on the first body segment, driven
    /// by that record's `group_ref`, exactly as the first `ST` set does — so
    /// the group control number can echo a discriminator the header has not
    /// yet seen.
    fn write_header(&mut self, record: &Record) -> Result<(), FormatError> {
        if self.header_written {
            return Ok(());
        }
        self.header_written = true;

        let isa_elements = self.resolve_isa_elements(record)?;
        // Adopting the source delimiter set must precede the first
        // write_segment so the ISA itself — and every segment after it —
        // emits with the original bytes, keeping the header's declared
        // delimiters self-consistent with the interchange on re-read.
        self.adopt_doc_delimiters(record)?;
        // The ISA control number (ISA13, element index 12) is echoed in
        // the IEA trailer; default to empty when the header is short.
        self.isa_control_number = isa_elements.get(12).cloned().unwrap_or_default();
        // The ISA is written verbatim with the writer's element separator
        // and terminator; its fields are fixed-width control values, never
        // trailing-empty.
        self.write_segment("ISA", &isa_elements)?;
        Ok(())
    }

    /// Adopt the source interchange's delimiter set when echoing the
    /// header from a `$doc` section the X12 reader produced. The reader
    /// stashes the discovered `[element, subelement, terminator]` bytes
    /// beside the raw `ISA` elements; adopting them here means the whole
    /// reconstructed interchange — header, envelopes, and body — emits
    /// with the original bytes, and the delimiter-in-data rejection checks
    /// the bytes actually in effect. The literal-`interchange` path and a
    /// section without the stamp (hand-authored `$doc` fields) keep the
    /// defaults.
    fn adopt_doc_delimiters(&mut self, record: &Record) -> Result<(), FormatError> {
        if self.config.interchange.is_some() {
            return Ok(());
        }
        let Some(section) = &self.config.interchange_from_doc else {
            return Ok(());
        };
        let Some(stamp) = record.doc_ctx().get_section_field(section, DELIMITERS_KEY) else {
            return Ok(());
        };
        self.delims = Delimiters::from_doc_value(&stamp).map_err(|e| {
            FormatError::X12(format!(
                "section {section:?}: malformed delimiter stamp {DELIMITERS_KEY:?}: {e}"
            ))
        })?;
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

    /// Open a functional group from the configured `group_header`,
    /// recomputing its `GS06` control number. A non-empty `group_ref` echoes
    /// as the control number (the discriminator *is* the group identity, the
    /// `GS06`/`GE02` analog of how a non-empty `set_ref` echoes as `ST02`);
    /// an empty `group_ref` falls back to the sequential group ordinal so a
    /// stream without a `group_ref` column keeps the single-group `1, 2, …`
    /// numbering unchanged.
    fn open_functional_group(&mut self, group_ref: &str) -> Result<(), FormatError> {
        let header = self.config.group_header.clone().ok_or_else(|| {
            FormatError::X12(
                "no functional-group header configured: set the writer's `group_header` \
                 (GS01..GS08 elements) so a GS..GE group can wrap the transaction sets"
                    .into(),
            )
        })?;
        self.group_count += 1;
        let control_number = if group_ref.is_empty() {
            self.group_count.to_string()
        } else {
            group_ref.to_string()
        };
        // GS06 (element index 5) is the group control number; recompute it
        // so it is unique per group and echoed correctly by GE.
        let mut gs = header;
        set_or_push(&mut gs, 5, control_number.clone());
        self.write_segment("GS", &gs)?;
        self.open_group = Some(OpenGroup {
            group_ref: group_ref.to_string(),
            control_number,
            set_count: 0,
        });
        Ok(())
    }

    /// Open a new `GS` functional group (closing any previous one) when the
    /// `group_ref` changes, or open the first group. A record with an empty
    /// `group_ref` joins the current group; if none is open, it opens the
    /// sole group — so a stream with no `group_ref` column produces exactly
    /// one `GS..GE`, byte-identical to the single-group output.
    fn transition_group(&mut self, group_ref: &str) -> Result<(), FormatError> {
        let need_new = match &self.open_group {
            None => true,
            Some(open) => !group_ref.is_empty() && open.group_ref != *group_ref,
        };
        if !need_new {
            return Ok(());
        }
        self.close_functional_group()?;
        self.open_functional_group(group_ref)
    }

    /// Map a body record to its segment tag and element list, then write
    /// it, opening or closing `GS..GE` functional groups on `group_ref`
    /// transitions and `ST..SE` transaction sets on `set_ref` transitions.
    fn write_body_record(&mut self, record: &Record) -> Result<(), FormatError> {
        self.reject_unknown_columns(record)?;
        let values = record.values();
        let seg_id = match self.seg_id_idx.and_then(|i| values.get(i)) {
            Some(v) => value_to_element(v, "seg_id")?,
            None => String::new(),
        };
        let group_ref = match self.group_ref_idx.and_then(|i| values.get(i)) {
            Some(v) => value_to_element(v, "group_ref")?,
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

        // The group boundary is the outer tier, so it transitions first: a
        // group change closes the open set then the open group before
        // opening the new group, which the next `transition_set` populates.
        self.transition_group(&group_ref)?;

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
        // Anonymous sets number from the *open group's* set count, so the
        // generated `ST02` control numbers restart per group rather than
        // running interchange-wide. A set is always opened inside a group —
        // `transition_group` has run for this record — so the group is open
        // here; `unwrap_or(0)` keeps the path total in the unreachable case.
        let group_set_count = self.open_group.as_ref().map(|g| g.set_count).unwrap_or(0);
        let control_number = if set_ref.is_empty() {
            format!("{:04}", group_set_count + 1)
        } else {
            set_ref.to_string()
        };
        self.write_segment("ST", &[resolved_type, control_number.clone()])?;
        if let Some(group) = self.open_group.as_mut() {
            group.set_count += 1;
        }
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

    /// Close the open functional group with its recomputed `GE` trailer:
    /// `GE01` is the count of transaction sets in *this* group (not the
    /// interchange-wide total), `GE02` echoes the group control number. Any
    /// transaction set still open is closed first, since `SE` always
    /// precedes the enclosing `GE`.
    fn close_functional_group(&mut self) -> Result<(), FormatError> {
        self.close_open_set()?;
        if let Some(group) = self.open_group.take() {
            self.write_segment("GE", &[group.set_count.to_string(), group.control_number])?;
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
            let known = matches!(name, "seg_id" | "set_ref" | "set_type" | "group_ref")
                || is_element_column(name);
            if !known {
                return Err(FormatError::X12(format!(
                    "record column {name:?} has no X12 mapping. The writer maps only \
                     `seg_id`, `set_ref`, `set_type`, `group_ref`, and positional `eNN` \
                     element columns; project the record to those before output"
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
        // Closing the group cascades to close any open transaction set
        // first, so the SE/GE ordering holds at end of stream too.
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

    /// Drain the underlying sink without emitting the interchange trailer, so
    /// byte-limit split accounting stays non-finalizing. Plan validation
    /// rejects byte splitting for this single-envelope format; this keeps the
    /// trait contract honest regardless, with the `IEA` trailer written only by
    /// [`Self::flush`].
    fn flush_bytes(&mut self) -> Result<(), FormatError> {
        self.writer.flush().map_err(FormatError::Io)
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
        Value::Decimal(d) => d.to_string(),
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

    /// Build a document context carrying an `interchange` section with the
    /// given raw `ISA` element list and, optionally, a delimiter stamp —
    /// the shape the X12 reader's `prepare_document` produces.
    fn doc_ctx_with_isa(
        elements: &[&str],
        stamp: Option<Value>,
    ) -> Arc<clinker_record::DocumentContext> {
        use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord};
        use indexmap::IndexMap;

        let raw = Value::Array(
            elements
                .iter()
                .map(|e| Value::String((*e).into()))
                .collect(),
        );
        let mut isa: IndexMap<Box<str>, Value> = IndexMap::new();
        isa.insert(super::RAW_ELEMENTS_KEY.into(), raw);
        if let Some(v) = stamp {
            isa.insert(super::DELIMITERS_KEY.into(), v);
        }
        let mut sections: IndexMap<Box<str>, Value> = IndexMap::new();
        sections.insert("interchange".into(), Value::Map(Box::new(isa)));
        Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from("orders.x12"),
            EnvelopeRecord::from_sections(sections),
        ))
    }

    fn from_doc_config() -> X12WriterConfig {
        X12WriterConfig {
            interchange_from_doc: Some("interchange".into()),
            group_header: Some(GS_HEADER.iter().map(|s| s.to_string()).collect()),
            segment_newline: false,
            ..Default::default()
        }
    }

    #[test]
    fn isa_echoed_from_doc_section() {
        let s = schema();
        let mut record = body(&s, "BEG", "0001", "850", &["00"]);
        record.set_doc_ctx(doc_ctx_with_isa(ISA_ELEMENTS, None));

        let out = write_all(from_doc_config(), &[record], &s);
        assert!(out.contains("000000001"), "{out}");
        assert!(out.contains("IEA*1*000000001~"), "{out}");
        // A section without a delimiter stamp (hand-authored `$doc` fields)
        // keeps the default `*`/`~` delimiters.
        assert!(out.starts_with("ISA*00*"), "{out}");
    }

    /// The [`ISA_ELEMENTS`] fixture under `|`/`^`/`!` delimiters — `ISA16`
    /// (the declared sub-element separator) is `^` instead of `:`.
    const CUSTOM_DELIM_ISA_ELEMENTS: &[&str] = &[
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
        "^",
    ];

    fn custom_delims() -> Delimiters {
        Delimiters {
            element: b'|',
            subelement: b'^',
            terminator: b'!',
        }
    }

    #[test]
    fn doc_delimiter_stamp_reconstructs_with_source_delimiters() {
        // A section carrying the reader's delimiter stamp emits the whole
        // interchange — ISA, envelopes, and body — with the source's
        // delimiter bytes, not the writer defaults.
        let s = schema();
        let mut record = body(&s, "BEG", "0001", "850", &["00"]);
        record.set_doc_ctx(doc_ctx_with_isa(
            CUSTOM_DELIM_ISA_ELEMENTS,
            Some(custom_delims().to_doc_value()),
        ));

        let out = write_all(from_doc_config(), &[record], &s);
        assert_eq!(
            out,
            "ISA|00|          |00|          |ZZ|SENDER         \
             |ZZ|RECEIVER       |240101|1200|U|00401|000000001|0|P|^!\
             GS|PO|SENDER|RECEIVER|20240101|1200|1|X|004010!\
             ST|850|0001!BEG|00!SE|3|0001!GE|1|1!IEA|1|000000001!"
        );
    }

    #[test]
    fn literal_interchange_ignores_doc_delimiter_stamp() {
        // The literal `interchange` config wins over the doc section
        // entirely, delimiters included: output keeps the defaults even
        // when the record's context carries a custom stamp.
        let s = schema();
        let mut record = body(&s, "BEG", "0001", "850", &["00"]);
        record.set_doc_ctx(doc_ctx_with_isa(
            CUSTOM_DELIM_ISA_ELEMENTS,
            Some(custom_delims().to_doc_value()),
        ));

        let mut cfg = literal_config();
        cfg.interchange_from_doc = Some("interchange".into());
        let out = write_all(cfg, &[record], &s);
        assert!(out.starts_with("ISA*00*"), "{out}");
        assert!(out.contains("IEA*1*000000001~"), "{out}");
        assert!(!out.contains('|'), "custom bytes must not leak: {out}");
    }

    #[test]
    fn adopted_delimiters_drive_delimiter_in_data_rejection() {
        // Under the adopted '|' element separator, a '*' in data is plain
        // text; a '|' is the structural byte that must be rejected.
        let s = schema();
        let stamped_body = |element: &str| {
            let mut record = body(&s, "BEG", "0001", "850", &[element]);
            record.set_doc_ctx(doc_ctx_with_isa(
                CUSTOM_DELIM_ISA_ELEMENTS,
                Some(custom_delims().to_doc_value()),
            ));
            record
        };

        let out = write_all(from_doc_config(), &[stamped_body("A*B")], &s);
        assert!(out.contains("BEG|A*B!"), "{out}");

        let mut buf = Vec::new();
        let mut w = X12Writer::new(Cursor::new(&mut buf), Arc::clone(&s), from_doc_config());
        let err = w.write_record(&stamped_body("A|B")).unwrap_err();
        assert!(
            matches!(&err, FormatError::X12(m) if m.contains("element") && m.contains("no escape")),
            "expected rejection on the adopted element byte, got {err:?}"
        );
    }

    #[test]
    fn malformed_delimiter_stamp_errors() {
        // The stamp key is engine-internal, so a non-conforming value is
        // corruption — rejected with a precise error, never silently
        // defaulted.
        let s = schema();
        let mut record = body(&s, "BEG", "0001", "850", &["00"]);
        record.set_doc_ctx(doc_ctx_with_isa(
            ISA_ELEMENTS,
            Some(Value::String("junk".into())),
        ));

        let mut buf = Vec::new();
        let mut w = X12Writer::new(Cursor::new(&mut buf), Arc::clone(&s), from_doc_config());
        let err = w.write_record(&record).unwrap_err();
        assert!(
            matches!(&err, FormatError::X12(m) if m.contains("malformed delimiter stamp")),
            "expected a stamp-shape error, got {err:?}"
        );
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

    /// Schema with a `group_ref` discriminator column ahead of `set_ref`,
    /// for the multi-functional-group output tests.
    fn grouped_schema() -> Arc<Schema> {
        let mut cols: Vec<Box<str>> = vec![
            "seg_id".into(),
            "group_ref".into(),
            "set_ref".into(),
            "set_type".into(),
        ];
        for i in 1..=4 {
            cols.push(format!("e{i:02}").into_boxed_str());
        }
        Arc::new(Schema::new(cols))
    }

    /// Body record carrying a `group_ref` value alongside `set_ref` /
    /// `set_type`, mirroring [`body`] but with the group discriminator.
    fn grouped_body(
        schema: &Arc<Schema>,
        seg: &str,
        group_ref: &str,
        set_ref: &str,
        set_type: &str,
        els: &[&str],
    ) -> Record {
        let string_or_null = |s: &str| {
            if s.is_empty() {
                Value::Null
            } else {
                Value::String(s.into())
            }
        };
        let mut values = vec![
            Value::String(seg.into()),
            string_or_null(group_ref),
            string_or_null(set_ref),
            string_or_null(set_type),
        ];
        for i in 0..4 {
            match els.get(i) {
                Some(e) => values.push(Value::String((*e).into())),
                None => values.push(Value::Null),
            }
        }
        Record::new(Arc::clone(schema), values)
    }

    #[test]
    fn group_ref_transition_opens_a_second_functional_group() {
        let s = grouped_schema();
        let out = write_all(
            literal_config(),
            &[
                grouped_body(&s, "BEG", "PO", "0001", "850", &["a"]),
                grouped_body(&s, "INV", "IN", "0002", "810", &["b"]),
            ],
            &s,
        );
        // Two distinct group_ref values → two GS..GE groups in one ISA..IEA.
        assert_eq!(out.matches("GS*").count(), 2, "{out}");
        assert_eq!(out.matches("GE*").count(), 2, "{out}");
        // GS06 echoes the discriminator; each GE claims its own single set
        // and echoes the matching control number.
        assert!(
            out.contains("GS*PO*SENDER*RECEIVER*20240101*1200*PO*X*004010~"),
            "{out}"
        );
        assert!(
            out.contains("GS*PO*SENDER*RECEIVER*20240101*1200*IN*X*004010~"),
            "{out}"
        );
        assert!(out.contains("GE*1*PO~"), "first group GE wrong: {out}");
        assert!(out.contains("GE*1*IN~"), "second group GE wrong: {out}");
        // IEA claims both functional groups.
        assert!(out.contains("IEA*2*000000001~"), "{out}");
    }

    #[test]
    fn ge_counts_are_per_group_not_interchange_wide() {
        // First group holds two sets, second holds one. Each GE01 must
        // reflect only its own group's sets, not the running total.
        let s = grouped_schema();
        let out = write_all(
            literal_config(),
            &[
                grouped_body(&s, "BEG", "PO", "0001", "850", &["a"]),
                grouped_body(&s, "BEG", "PO", "0002", "850", &["b"]),
                grouped_body(&s, "INV", "IN", "0003", "810", &["c"]),
            ],
            &s,
        );
        // First group: 2 sets; second group: 1 set.
        assert!(
            out.contains("GE*2*PO~"),
            "first GE should claim 2 sets: {out}"
        );
        assert!(
            out.contains("GE*1*IN~"),
            "second GE should claim 1 set: {out}"
        );
        assert!(out.contains("IEA*2*000000001~"), "{out}");
    }

    #[test]
    fn empty_group_ref_keeps_one_group_byte_identical() {
        // A `group_ref` column whose value never changes (here always empty)
        // must collapse to exactly one GS..GE, identical to a stream with no
        // group_ref column at all.
        let grouped = grouped_schema();
        let with_group = write_all(
            literal_config(),
            &[
                grouped_body(&grouped, "BEG", "", "0001", "850", &["a"]),
                grouped_body(&grouped, "PO1", "", "0001", "850", &["b"]),
            ],
            &grouped,
        );
        let plain = schema();
        let without_group = write_all(
            literal_config(),
            &[
                body(&plain, "BEG", "0001", "850", &["a"]),
                body(&plain, "PO1", "0001", "850", &["b"]),
            ],
            &plain,
        );
        assert_eq!(
            with_group, without_group,
            "empty group_ref must produce byte-identical output to no group_ref column"
        );
        assert_eq!(with_group.matches("GS*").count(), 1, "{with_group}");
        assert!(with_group.contains("GE*1*1~"), "{with_group}");
    }

    #[test]
    fn anonymous_set_numbers_restart_per_group() {
        // With no set_ref but distinct group_ref values, each group's
        // generated ST02 control number restarts at 0001 within its group.
        let cols: Vec<Box<str>> = vec!["seg_id".into(), "group_ref".into()];
        let schema = Arc::new(Schema::new(cols));
        let rec = |seg: &str, group: &str| {
            Record::new(
                Arc::clone(&schema),
                vec![Value::String(seg.into()), Value::String(group.into())],
            )
        };
        let mut cfg = literal_config();
        cfg.set_type = Some("850".into());
        let out = write_all(cfg, &[rec("BEG", "PO"), rec("INV", "IN")], &schema);
        // Each group opens an anonymous set numbered from that group's count.
        assert_eq!(out.matches("ST*850*0001~").count(), 2, "{out}");
        assert!(out.contains("GE*1*PO~"), "{out}");
        assert!(out.contains("GE*1*IN~"), "{out}");
    }

    #[test]
    fn group_ref_column_is_accepted_not_rejected() {
        // The group discriminator is a recognized control column, never an
        // unmapped user field.
        let s = grouped_schema();
        let out = write_all(
            literal_config(),
            &[grouped_body(&s, "BEG", "PO", "0001", "850", &["00"])],
            &s,
        );
        assert!(out.contains("BEG*00~"), "{out}");
        // group_ref drives the group, it is never emitted as a data element.
        assert!(
            !out.contains("*PO*PO*"),
            "group_ref leaked into a segment: {out}"
        );
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
