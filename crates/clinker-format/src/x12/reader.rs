//! ANSI ASC X12 interchange reader.
//!
//! Streaming, row-at-a-time: each non-service segment of an interchange
//! becomes one [`Record`] with a static positional schema
//! `[seg_id, set_ref, set_type, e01..e<max>]`. Service segments
//! (`ISA`/`IEA`/`GS`/`GE`/`SE`) are consumed by the reader to drive the
//! three-tier envelope and inline structural validation; the `ST` segment
//! that opens a transaction set **is** emitted as a body record. Service
//! segments are never emitted.
//!
//! The three envelope tiers map onto nested document-context levels: the
//! `ISA` interchange is the file-level document served by
//! [`FormatReader::prepare_document`], and each `GS` functional group and
//! `ST` transaction set opens a nested level via [`EnvelopeEvent`] the
//! source ingest driver drains after each `next_record`. A `GS` queues an
//! `OpenLevel` carrying the group's `$doc` sections; an `ST` queues a
//! nested `OpenLevel`; an `SE` queues `CloseLevel` (transaction set) and a
//! `GE` queues `CloseLevel` (functional group). The `ISA`-level close is
//! the driver's end-of-input / file-transition sweep.
//!
//! Memory model: only the interchange header (`ISA`) is pre-scanned and
//! retained, so `prepare_document` exposes the `ISA` control fields as a
//! `$doc` section with O(ISA) held memory. The body streams one segment at
//! a time — the whole interchange is never buffered. Trailer control
//! counts (`SE`/`GE`/`IEA`) are validated as they arrive, not surfaced as
//! envelope sections, because they follow the body they close.

use std::io::{BufReader, Read};
use std::sync::Arc;

use clinker_record::{Record, Schema, Value};
use indexmap::IndexMap;

use crate::charset::Charset;
use crate::envelope::{
    EnvelopeConfig, EnvelopeEvent, EnvelopeExtract, FrameRole, NestedEnvelopeSection,
    coerce_section_fields,
};
use crate::error::FormatError;
use crate::traits::FormatReader;
use crate::x12::RAW_ELEMENTS_KEY;
use crate::x12::tokenizer::{ParsedSegment, SegmentTokenizer, split_isa, split_segment};

/// Default ceiling on the number of positional element columns the record
/// schema exposes. A segment carrying more data elements than this errors
/// with guidance rather than silently truncating.
///
/// Public so the planner's `$doc` positional-element validation can bound
/// against the same ceiling the reader enforces, rather than duplicating
/// the literal (which would silently drift if either changed).
pub const DEFAULT_MAX_ELEMENTS: usize = 32;

/// The envelope segment tags `prepare_document` can extract as `$doc`
/// sections. Only the interchange header `ISA` is resolvable from the
/// bounded header pre-scan; `GS` and `ST` headers arrive mid-body and
/// surface as nested document levels instead.
const ISA_TAG: &str = "ISA";

/// Configuration for the X12 reader.
pub struct X12ReaderConfig {
    /// Number of positional `eNN` element columns on the record schema. A
    /// body segment with more data elements than this is rejected.
    pub max_elements: usize,
    /// Character set used to decode element text. X12 carries no in-band
    /// element naming the body repertoire, so it is declared on the source
    /// and defaults to UTF-8; a non-UTF-8 interchange (e.g. Latin-1 high
    /// bytes in free-text fields) declares the matching encoding.
    pub charset: Charset,
    /// Optional user-declared name + typed field schema for the `GS`
    /// functional-group nested level. When set, the reader surfaces the
    /// group under the chosen section name with its declared elements
    /// coerced to their types. When `None`, the group surfaces under the
    /// default `functional_group` section keyed by untyped positional
    /// `eNN` strings.
    pub group_section: Option<NestedEnvelopeSection>,
    /// Optional user-declared name + typed field schema for the `ST`
    /// transaction-set nested level, mirroring `group_section`. When
    /// `None`, the set surfaces under the default `transaction_set`
    /// section keyed by untyped positional `eNN` strings.
    pub set_section: Option<NestedEnvelopeSection>,
}

impl Default for X12ReaderConfig {
    fn default() -> Self {
        Self {
            max_elements: DEFAULT_MAX_ELEMENTS,
            charset: Charset::default(),
            group_section: None,
            set_section: None,
        }
    }
}

/// Streaming X12 interchange reader.
///
/// Holds the tokenizer, the static positional schema, the inline
/// envelope-validation state for all three tiers, and the queue of
/// nested-envelope events the source ingest driver drains after each
/// `next_record`. The `ISA` elements are stashed at initialization so
/// [`FormatReader::prepare_document`] can serve an `ISA` envelope section
/// without re-reading the source.
pub struct X12Reader<R: Read> {
    tokenizer: SegmentTokenizer<BufReader<R>>,
    schema: Arc<Schema>,
    max_elements: usize,
    initialized: bool,
    /// Raw `ISA` data elements, stashed at init for envelope serving.
    isa_elements: Vec<String>,
    /// `ISA13` interchange control number, echoed by `IEA02`.
    isa_control_number: Option<String>,
    /// State of the functional group currently open, if any.
    open_group: Option<OpenGroup>,
    /// State of the transaction set currently streaming, if any.
    open_set: Option<OpenSet>,
    /// Count of `GS` groups seen, checked against `IEA01`.
    group_count: u64,
    /// `true` once `IEA` has been consumed; any further segment is an
    /// after-trailer-content error.
    interchange_closed: bool,
    /// Nested-envelope events queued during the most recent `next_record`,
    /// drained by the driver via [`FormatReader::take_envelope_events`].
    pending_events: Vec<EnvelopeEvent>,
    /// User-declared name + typed schema for the `GS` level, applied when
    /// opening each functional group. `None` keeps the positional default.
    group_section: Option<NestedEnvelopeSection>,
    /// User-declared name + typed schema for the `ST` level, applied when
    /// opening each transaction set. `None` keeps the positional default.
    set_section: Option<NestedEnvelopeSection>,
    done: bool,
}

/// Per-group validation state for the functional group currently open.
struct OpenGroup {
    /// `GS06` group control number, echoed by `GE02`.
    control_number: String,
    /// Count of transaction sets (`ST`) seen in this group, checked
    /// against the `GE01` transaction-set count.
    set_count: u64,
}

/// Per-set validation state for the transaction set currently streaming.
struct OpenSet {
    /// `ST02` transaction set control number, echoed by `SE02`.
    control_number: String,
    /// `ST01` transaction set identifier code (e.g. `850`), stamped on
    /// every record of the set as `set_type`.
    set_type: String,
    /// Count of segments in this set including `ST` (and, at close, `SE`),
    /// checked against the `SE01` segment count.
    segment_count: u64,
}

impl<R: Read> X12Reader<R> {
    /// Build a reader over any `Read` source with the given element-width
    /// configuration. Delimiter discovery and `ISA` consumption are
    /// deferred to the first read.
    pub fn new(reader: R, config: X12ReaderConfig) -> Self {
        let schema = build_schema(config.max_elements);
        Self {
            tokenizer: SegmentTokenizer::new(BufReader::new(reader), config.charset),
            schema,
            max_elements: config.max_elements,
            initialized: false,
            isa_elements: Vec::new(),
            isa_control_number: None,
            open_group: None,
            open_set: None,
            group_count: 0,
            interchange_closed: false,
            pending_events: Vec::new(),
            group_section: config.group_section,
            set_section: config.set_section,
            done: false,
        }
    }

    /// Consume the fixed 106-byte `ISA` header, stashing the interchange
    /// control number (`ISA13`) and the `ISA` data elements for envelope
    /// serving. Idempotent.
    fn ensure_initialized(&mut self) -> Result<(), FormatError> {
        if self.initialized {
            return Ok(());
        }
        self.initialized = true;

        let header = self.tokenizer.read_isa_header()?;
        let delims = self.tokenizer.delimiters();
        let parsed = split_isa(&header, &delims, self.tokenizer.charset())?;
        // ISA13 (the interchange control number) is element index 12. It
        // is located structurally — the 13th element of the split header —
        // not by absolute byte offset, so producer padding quirks do not
        // misalign it.
        self.isa_control_number = parsed.elements.get(12).cloned();
        self.isa_elements = parsed.elements;
        Ok(())
    }

    /// Pull the next service-or-body segment and advance the three-tier
    /// envelope state. Returns the body record to emit, or `None` at clean
    /// interchange end. Service segments are consumed transparently (the
    /// loop continues) so the caller only ever sees body records; envelope
    /// boundaries crossed on the way are queued for the driver.
    fn pull_next(&mut self) -> Result<Option<Record>, FormatError> {
        loop {
            let raw = match self.tokenizer.next_segment()? {
                Some(s) => s,
                None => {
                    if !self.interchange_closed {
                        return Err(FormatError::X12(
                            "interchange truncated: reached end of input with no IEA trailer"
                                .into(),
                        ));
                    }
                    self.done = true;
                    return Ok(None);
                }
            };
            let delims = self.tokenizer.delimiters();
            let segment = split_segment(&raw, &delims);

            if self.interchange_closed {
                return Err(FormatError::X12(format!(
                    "segment {:?} found after the IEA interchange trailer; \
                     content past IEA is not permitted",
                    segment.tag
                )));
            }

            match segment.tag.as_str() {
                "ISA" => {
                    return Err(FormatError::X12(
                        "a second ISA segment appeared; nested or repeated interchanges \
                         in one stream are not supported"
                            .into(),
                    ));
                }
                "GS" => {
                    self.open_functional_group(&segment)?;
                    // GS opens a nested level — queue the OpenLevel and
                    // continue to the first body segment (the ST).
                }
                "GE" => {
                    self.close_functional_group(&segment)?;
                    // GE is a service segment — consume and continue.
                }
                "ST" => {
                    self.open_transaction_set(&segment)?;
                    let record = self.body_record(&segment)?;
                    return Ok(Some(record));
                }
                "SE" => {
                    self.close_transaction_set(&segment)?;
                    // SE is a service segment — consume and continue.
                }
                "IEA" => {
                    self.close_interchange(&segment)?;
                    // IEA is a service segment — consume and continue to
                    // the EOF check on the next loop turn.
                }
                _ => {
                    let set = self.open_set.as_mut().ok_or_else(|| {
                        FormatError::X12(format!(
                            "body segment {:?} appeared outside any ST..SE transaction set",
                            segment.tag
                        ))
                    })?;
                    set.segment_count += 1;
                    let record = self.body_record(&segment)?;
                    return Ok(Some(record));
                }
            }
        }
    }

    /// Open a functional group on a `GS` segment, queuing the group's
    /// `OpenLevel` so the driver opens a nested document context. `GS06`
    /// (element index 5) is the group control number echoed by `GE02`.
    fn open_functional_group(&mut self, gs: &ParsedSegment) -> Result<(), FormatError> {
        if self.open_group.is_some() {
            return Err(FormatError::X12(
                "GS opened a new functional group before the previous group's GE trailer; \
                 groups must be GS..GE balanced"
                    .into(),
            ));
        }
        let control_number = gs.elements.get(5).cloned().unwrap_or_default();
        self.open_group = Some(OpenGroup {
            control_number,
            set_count: 0,
        });
        self.group_count += 1;
        let sections = nested_sections(gs, self.group_section.as_ref(), DEFAULT_GROUP_SECTION)?;
        // A `GS` functional group stays inside the interchange frame — the
        // X12 framing document is the `ISA` interchange, so its nested levels
        // inherit the interchange grain.
        self.pending_events.push(EnvelopeEvent::OpenLevel {
            sections,
            frame: FrameRole::Inherit,
        });
        Ok(())
    }

    /// Validate and close the open functional group against its `GE`
    /// trailer, queuing a `CloseLevel`. `GE01` (element index 0) is the
    /// transaction-set count; `GE02` (element index 1) echoes the `GS06`
    /// group control number.
    fn close_functional_group(&mut self, ge: &ParsedSegment) -> Result<(), FormatError> {
        if self.open_set.is_some() {
            return Err(FormatError::X12(
                "GE functional-group trailer arrived before the last transaction set's SE".into(),
            ));
        }
        let group = self.open_group.take().ok_or_else(|| {
            FormatError::X12("GE trailer with no open GS functional group".into())
        })?;
        let claimed = parse_count(ge.elements.first(), "GE", "transaction-set count")?;
        if claimed != group.set_count {
            return Err(FormatError::x12_structural_count(format!(
                "GE transaction-set count mismatch for group {:?}: trailer claims {claimed}, \
                 group contains {} transaction sets",
                group.control_number, group.set_count
            )));
        }
        let echoed = ge.elements.get(1).map(|s| s.as_str()).unwrap_or("");
        if echoed != group.control_number {
            return Err(FormatError::X12(format!(
                "GE group control number {echoed:?} does not echo the GS06 control number {:?}",
                group.control_number
            )));
        }
        self.pending_events.push(EnvelopeEvent::CloseLevel);
        Ok(())
    }

    /// Open a transaction set on an `ST` segment, queuing a nested
    /// `OpenLevel`. `ST01` (element index 0) is the set identifier code
    /// (`set_type`); `ST02` (element index 1) is the set control number
    /// echoed by `SE02`.
    fn open_transaction_set(&mut self, st: &ParsedSegment) -> Result<(), FormatError> {
        if self.open_group.is_none() {
            return Err(FormatError::X12(
                "ST transaction set appeared outside any GS..GE functional group".into(),
            ));
        }
        if self.open_set.is_some() {
            return Err(FormatError::X12(
                "ST opened a new transaction set before the previous set's SE trailer; \
                 sets must be ST..SE balanced"
                    .into(),
            ));
        }
        let set_type = st.elements.first().cloned().unwrap_or_default();
        let control_number = st.elements.get(1).cloned().unwrap_or_default();
        self.open_set = Some(OpenSet {
            control_number,
            set_type,
            // ST counts as the first segment of the set.
            segment_count: 1,
        });
        if let Some(group) = self.open_group.as_mut() {
            group.set_count += 1;
        }
        let sections = nested_sections(st, self.set_section.as_ref(), DEFAULT_SET_SECTION)?;
        // An `ST` transaction set also stays inside the interchange frame, so
        // a whole `ISA..IEA` interchange reconstructs as one output envelope
        // rather than one per transaction set.
        self.pending_events.push(EnvelopeEvent::OpenLevel {
            sections,
            frame: FrameRole::Inherit,
        });
        Ok(())
    }

    /// Validate and close the open transaction set against its `SE`
    /// trailer, queuing a `CloseLevel`. `SE01` (element index 0) is the
    /// segment count (`ST`..`SE` inclusive); `SE02` (element index 1)
    /// echoes the `ST02` set control number.
    fn close_transaction_set(&mut self, se: &ParsedSegment) -> Result<(), FormatError> {
        let set = self
            .open_set
            .take()
            .ok_or_else(|| FormatError::X12("SE trailer with no open ST transaction set".into()))?;
        let claimed = parse_count(se.elements.first(), "SE", "segment count")?;
        // The running count tallied ST + body segments; the SE segment
        // itself completes the count.
        let actual = set.segment_count + 1;
        if claimed != actual {
            return Err(FormatError::x12_structural_count(format!(
                "SE segment count mismatch for transaction set {:?}: trailer claims {claimed}, \
                 set contains {actual} (ST..SE inclusive)",
                set.control_number
            )));
        }
        let echoed = se.elements.get(1).map(|s| s.as_str()).unwrap_or("");
        if echoed != set.control_number {
            return Err(FormatError::X12(format!(
                "SE set control number {echoed:?} does not echo the ST02 control number {:?}",
                set.control_number
            )));
        }
        self.pending_events.push(EnvelopeEvent::CloseLevel);
        Ok(())
    }

    /// Validate and close the interchange against its `IEA` trailer.
    /// `IEA01` (element index 0) is the functional-group count; `IEA02`
    /// (element index 1) echoes the `ISA13` interchange control number.
    fn close_interchange(&mut self, iea: &ParsedSegment) -> Result<(), FormatError> {
        if self.open_group.is_some() {
            return Err(FormatError::X12(
                "IEA interchange trailer arrived before the last group's GE".into(),
            ));
        }
        let claimed = parse_count(iea.elements.first(), "IEA", "functional-group count")?;
        if claimed != self.group_count {
            return Err(FormatError::x12_structural_count(format!(
                "IEA functional-group count mismatch: trailer claims {claimed}, interchange \
                 contains {} groups",
                self.group_count
            )));
        }
        if let Some(expected) = &self.isa_control_number {
            let echoed = iea.elements.get(1).map(|s| s.as_str()).unwrap_or("");
            if echoed != expected {
                return Err(FormatError::X12(format!(
                    "IEA interchange control number {echoed:?} does not echo the ISA13 \
                     control number {expected:?}"
                )));
            }
        }
        self.interchange_closed = true;
        Ok(())
    }

    /// Map a parsed segment to a positional [`Record`] under the static
    /// schema. `seg_id`, `set_ref`, and `set_type` are stamped from the
    /// open transaction set; the data elements fill `e01..`. Absent
    /// trailing elements stay `Value::Null`; an element count past
    /// `max_elements` errors with guidance.
    fn body_record(&self, segment: &ParsedSegment) -> Result<Record, FormatError> {
        if segment.elements.len() > self.max_elements {
            return Err(FormatError::X12(format!(
                "segment {:?} carries {} data elements, exceeding the configured \
                 max_elements of {}; raise the source's `max_elements` option",
                segment.tag,
                segment.elements.len(),
                self.max_elements
            )));
        }
        let (set_ref, set_type) = match self.open_set.as_ref() {
            Some(set) => (set.control_number.clone(), set.set_type.clone()),
            None => (String::new(), String::new()),
        };

        let mut values: Vec<Value> = Vec::with_capacity(3 + self.max_elements);
        values.push(Value::String(segment.tag.as_str().into()));
        values.push(string_or_null(&set_ref));
        values.push(string_or_null(&set_type));
        for i in 0..self.max_elements {
            match segment.elements.get(i) {
                Some(e) => values.push(string_or_null(e)),
                None => values.push(Value::Null),
            }
        }
        Ok(Record::new(Arc::clone(&self.schema), values))
    }
}

impl<R: Read + Send> FormatReader for X12Reader<R> {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        Ok(Arc::clone(&self.schema))
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        if self.done {
            return Ok(None);
        }
        self.ensure_initialized()?;
        self.pull_next()
    }

    fn take_envelope_events(&mut self) -> Vec<EnvelopeEvent> {
        std::mem::take(&mut self.pending_events)
    }

    fn prepare_document(
        &mut self,
        config: &EnvelopeConfig,
    ) -> Result<IndexMap<Box<str>, Value>, FormatError> {
        if config.is_empty() {
            return Ok(IndexMap::new());
        }
        self.ensure_initialized()?;

        let mut out: IndexMap<Box<str>, Value> = IndexMap::with_capacity(config.sections.len());
        for (name, section) in &config.sections {
            let segment_tag = match &section.extract {
                EnvelopeExtract::Segment(tag) => tag.as_str(),
                EnvelopeExtract::XmlPath(_)
                | EnvelopeExtract::JsonPointer(_)
                | EnvelopeExtract::RecordType(_) => {
                    return Err(FormatError::X12(format!(
                        "envelope section {name:?}: declared a non-`segment` extract \
                         against an X12 source. Use `segment` (e.g. \
                         `extract: {{ segment: \"ISA\" }}`) for X12 envelope sections."
                    )));
                }
            };
            if segment_tag != ISA_TAG {
                return Err(FormatError::X12(format!(
                    "envelope section {name:?}: segment {segment_tag:?} is not extractable \
                     as a file-level $doc section. Only the interchange header `ISA` is \
                     available from the header pre-scan; `GS` and `ST` headers surface as \
                     nested document levels, and trailer segments (`SE`/`GE`/`IEA`) are \
                     validated by the reader, not exposed as $doc fields."
                )));
            }
            let raw = positional_pairs(&self.isa_elements);
            let mut typed =
                coerce_section_fields(raw, &section.fields).map_err(FormatError::X12)?;
            // Stash the complete, ordered ISA element list under an
            // engine-internal key so a writer can reconstruct the header
            // losslessly via `interchange_from_doc`. The declared `fields`
            // drive typed CXL `$doc.<section>.<field>` access and
            // intentionally drop empty/undeclared elements; that filtered
            // view cannot round-trip an ISA whose elements include
            // fixed-width blanks, so the raw list is carried alongside it.
            let raw_elements: Vec<Value> = self
                .isa_elements
                .iter()
                .map(|e| Value::String(e.as_str().into()))
                .collect();
            typed.insert(Box::from(RAW_ELEMENTS_KEY), Value::Array(raw_elements));
            out.insert(Box::from(name.as_str()), Value::Map(Box::new(typed)));
        }
        Ok(out)
    }
}

/// Section name the `GS` functional group surfaces under when the source
/// declares no name of its own.
const DEFAULT_GROUP_SECTION: &str = "functional_group";

/// Section name the `ST` transaction set surfaces under when the source
/// declares no name of its own.
const DEFAULT_SET_SECTION: &str = "transaction_set";

/// Build the `$doc` sections for a nested `GS`/`ST` level.
///
/// A nested level surfaces its header through the document context exactly
/// as the file-level `ISA` does. When the source declares a section name
/// and typed `fields` schema for the level, the segment's positional
/// elements are coerced to the declared types under the chosen name — so
/// `$doc.<chosen>.<field>` resolves to a typed value. With no declaration,
/// the level surfaces under `default_name` keyed by untyped positional
/// `eNN` strings, the same behavior every X12 source had before nested
/// declarations existed.
///
/// # Errors
///
/// Returns a [`FormatError::X12`] if a declared element fails coercion to
/// its declared type (the wire value cannot be parsed as the field's type).
fn nested_sections(
    segment: &ParsedSegment,
    declared: Option<&NestedEnvelopeSection>,
    default_name: &str,
) -> Result<IndexMap<Box<str>, Value>, FormatError> {
    let mut sections: IndexMap<Box<str>, Value> = IndexMap::with_capacity(1);
    match declared {
        Some(section) => {
            // Coerce the level's positional elements through the declared
            // field schema, exactly as the file-level ISA section does, so
            // a declared GS/ST field is typed the same way a declared ISA
            // field is.
            let raw = positional_pairs(&segment.elements);
            let typed = coerce_section_fields(raw, &section.fields).map_err(FormatError::X12)?;
            sections.insert(
                Box::from(section.name.as_str()),
                Value::Map(Box::new(typed)),
            );
        }
        None => {
            let mut fields: IndexMap<Box<str>, Value> =
                IndexMap::with_capacity(segment.elements.len());
            for (i, e) in segment.elements.iter().enumerate() {
                fields.insert(positional_key(i).into_boxed_str(), string_or_null(e));
            }
            sections.insert(Box::from(default_name), Value::Map(Box::new(fields)));
        }
    }
    Ok(sections)
}

/// Parse a trailer control count, naming the segment and field on failure.
fn parse_count(raw: Option<&String>, segment: &str, field: &str) -> Result<u64, FormatError> {
    let text =
        raw.ok_or_else(|| FormatError::X12(format!("{segment} missing its {field} element")))?;
    text.trim()
        .parse()
        .map_err(|_| FormatError::X12(format!("{segment} {field} {text:?} is not a number")))
}

/// Build the static positional schema `[seg_id, set_ref, set_type,
/// e01..e<max>]`. All columns are string-typed; element text is stored
/// verbatim so the round-trip is lossless.
fn build_schema(max_elements: usize) -> Arc<Schema> {
    let mut columns: Vec<Box<str>> = Vec::with_capacity(3 + max_elements);
    columns.push(Box::from("seg_id"));
    columns.push(Box::from("set_ref"));
    columns.push(Box::from("set_type"));
    for i in 0..max_elements {
        columns.push(positional_key(i).into_boxed_str());
    }
    Arc::new(Schema::new(columns))
}

/// Positional element column name for element index `i`: `e01`, `e02`, …
fn positional_key(i: usize) -> String {
    format!("e{:02}", i + 1)
}

/// Pair each element with its positional key (`e01`, `e02`, …) for
/// [`coerce_section_fields`], the raw input shape both the file-level `ISA`
/// section and a declared nested `GS`/`ST` section coerce against.
fn positional_pairs(elements: &[String]) -> Vec<(String, String)> {
    elements
        .iter()
        .enumerate()
        .map(|(i, e)| (positional_key(i), e.clone()))
        .collect()
}

/// Map an element string to a `Value`: empty text becomes `Null` so an
/// absent or blank element is uniformly null at the schema slot.
fn string_or_null(s: &str) -> Value {
    if s.is_empty() {
        Value::Null
    } else {
        Value::String(s.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{EnvelopeFieldType, EnvelopeSection};
    use std::io::Cursor;

    /// The canonical 106-byte ISA header (`*` element, `:` sub-element,
    /// `~` terminator) with interchange control number `000000001`.
    const ISA: &str = "ISA*00*          *00*          *ZZ*SENDER         \
        *ZZ*RECEIVER       *240101*1200*U*00401*000000001*0*P*:~";

    /// A minimal valid single-set 850 interchange: ISA, one GS..GE group
    /// containing one ST..SE set with two body segments (BEG, PO1). SE
    /// claims 4 segments (ST, BEG, PO1, SE); GE claims 1 set echoing
    /// group control `1`; IEA claims 1 group echoing `000000001`.
    fn interchange() -> String {
        format!(
            "{ISA}\
            GS*PO*SENDER*RECEIVER*20240101*1200*1*X*004010~\
            ST*850*0001~\
            BEG*00*NE*PO12345**20240101~\
            PO1*1*10*EA*9.99~\
            SE*4*0001~\
            GE*1*1~\
            IEA*1*000000001~"
        )
    }

    fn reader(data: &str) -> X12Reader<Cursor<Vec<u8>>> {
        X12Reader::new(
            Cursor::new(data.as_bytes().to_vec()),
            X12ReaderConfig::default(),
        )
    }

    fn collect(data: &str) -> Vec<Record> {
        let mut r = reader(data);
        let mut out = Vec::new();
        while let Some(rec) = r.next_record().unwrap() {
            out.push(rec);
        }
        out
    }

    #[test]
    fn one_record_per_body_segment_with_positional_columns() {
        let recs = collect(&interchange());
        // ST, BEG, PO1 are body records; ISA/GS/GE/SE/IEA are not.
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[0].get("seg_id"), Some(&Value::String("ST".into())));
        assert_eq!(recs[1].get("seg_id"), Some(&Value::String("BEG".into())));
        assert_eq!(recs[1].get("e01"), Some(&Value::String("00".into())));
        assert_eq!(recs[1].get("e02"), Some(&Value::String("NE".into())));
        assert_eq!(recs[2].get("seg_id"), Some(&Value::String("PO1".into())));
    }

    #[test]
    fn set_ref_and_type_stamped_on_every_record() {
        let recs = collect(&interchange());
        for rec in &recs {
            assert_eq!(rec.get("set_ref"), Some(&Value::String("0001".into())));
            assert_eq!(rec.get("set_type"), Some(&Value::String("850".into())));
        }
    }

    #[test]
    fn service_segments_never_emitted_as_body() {
        let recs = collect(&interchange());
        for rec in &recs {
            let seg = rec.get("seg_id").unwrap();
            assert!(
                !matches!(seg, Value::String(s) if matches!(&**s, "ISA" | "IEA" | "GS" | "GE" | "SE"))
            );
        }
    }

    #[test]
    fn absent_trailing_elements_are_null() {
        let recs = collect(&interchange());
        // BEG has 5 elements (one empty in the middle); e06.. are null.
        assert_eq!(recs[1].get("e06"), Some(&Value::Null));
        // BEG element 4 is empty on the wire (PO12345**20240101), null.
        assert_eq!(recs[1].get("e04"), Some(&Value::Null));
    }

    #[test]
    fn nested_envelope_events_open_group_then_set() {
        // The driver drains events after each next_record. The first body
        // record (ST) must be preceded by OpenLevel(group), OpenLevel(set).
        let data = interchange();
        let mut r = reader(&data);
        let _first = r.next_record().unwrap().unwrap(); // ST
        let events = r.take_envelope_events();
        assert_eq!(events.len(), 2, "GS and ST each open one level");
        assert!(matches!(events[0], EnvelopeEvent::OpenLevel { .. }));
        assert!(matches!(events[1], EnvelopeEvent::OpenLevel { .. }));
    }

    #[test]
    fn nested_envelope_events_close_set_then_group_at_end() {
        let data = interchange();
        let mut r = reader(&data);
        // Drain all body records, taking events after each.
        while r.next_record().unwrap().is_some() {
            let _ = r.take_envelope_events();
        }
        // The terminal next_record crosses SE, GE, IEA — SE and GE each
        // queue a CloseLevel (IEA's close is the driver's sweep).
        let trailing = r.take_envelope_events();
        assert_eq!(trailing.len(), 2, "SE and GE each close one level");
        assert!(matches!(trailing[0], EnvelopeEvent::CloseLevel));
        assert!(matches!(trailing[1], EnvelopeEvent::CloseLevel));
    }

    #[test]
    fn group_section_exposes_gs_control_number() {
        // Inspect the OpenLevel sections directly.
        let data = interchange();
        let mut r = reader(&data);
        let _ = r.next_record().unwrap().unwrap();
        let events = r.take_envelope_events();
        let EnvelopeEvent::OpenLevel { sections, .. } = &events[0] else {
            panic!("expected group OpenLevel");
        };
        let group = match sections.get("functional_group").unwrap() {
            Value::Map(m) => m,
            other => panic!("expected map, got {other:?}"),
        };
        // GS06 (group control number) is positional e06.
        assert_eq!(group.get("e06"), Some(&Value::String("1".into())));
    }

    #[test]
    fn set_section_exposes_st_fields() {
        let data = interchange();
        let mut r = reader(&data);
        let _ = r.next_record().unwrap().unwrap();
        let events = r.take_envelope_events();
        let EnvelopeEvent::OpenLevel { sections, .. } = &events[1] else {
            panic!("expected set OpenLevel");
        };
        let set = match sections.get("transaction_set").unwrap() {
            Value::Map(m) => m,
            other => panic!("expected map, got {other:?}"),
        };
        assert_eq!(set.get("e01"), Some(&Value::String("850".into())));
        assert_eq!(set.get("e02"), Some(&Value::String("0001".into())));
    }

    /// Build a [`NestedEnvelopeSection`] from a chosen name and a list of
    /// `(positional element, declared type)` field declarations.
    fn nested_section(name: &str, fields: &[(&str, EnvelopeFieldType)]) -> NestedEnvelopeSection {
        let mut field_map = IndexMap::new();
        for (k, ty) in fields {
            field_map.insert((*k).to_string(), *ty);
        }
        NestedEnvelopeSection {
            name: name.to_string(),
            fields: field_map,
        }
    }

    /// The first body record's two `OpenLevel` events carry the GS section
    /// (index 0) and the ST section (index 1).
    fn first_open_levels(mut r: X12Reader<Cursor<Vec<u8>>>) -> Vec<IndexMap<Box<str>, Value>> {
        let _ = r.next_record().unwrap().unwrap();
        r.take_envelope_events()
            .into_iter()
            .filter_map(|e| match e {
                EnvelopeEvent::OpenLevel { sections, .. } => Some(sections),
                EnvelopeEvent::CloseLevel => None,
            })
            .collect()
    }

    #[test]
    fn declared_group_and_set_sections_surface_under_chosen_names_typed() {
        // A source naming the GS level `group` with a numeric control field
        // and the ST level `txn` with a numeric id and string control sees
        // both levels under those names, each element coerced to its
        // declared type rather than a positional `eNN` string.
        let config = X12ReaderConfig {
            group_section: Some(nested_section("group", &[("e06", EnvelopeFieldType::Int)])),
            set_section: Some(nested_section(
                "txn",
                &[
                    ("e01", EnvelopeFieldType::Int),
                    ("e02", EnvelopeFieldType::String),
                ],
            )),
            ..Default::default()
        };
        let r = X12Reader::new(Cursor::new(interchange().into_bytes()), config);
        let levels = first_open_levels(r);
        assert_eq!(levels.len(), 2, "GS and ST each open one level");

        let group = match levels[0].get("group").unwrap() {
            Value::Map(m) => m,
            other => panic!("expected group map, got {other:?}"),
        };
        // GS06 (group control number "1") coerces to an integer.
        assert_eq!(group.get("e06"), Some(&Value::Integer(1)));
        // The default `functional_group` name is NOT used when a name is
        // declared.
        assert!(levels[0].get("functional_group").is_none());

        let txn = match levels[1].get("txn").unwrap() {
            Value::Map(m) => m,
            other => panic!("expected txn map, got {other:?}"),
        };
        // ST01 (set id "850") coerces to an integer; ST02 stays a string.
        assert_eq!(txn.get("e01"), Some(&Value::Integer(850)));
        assert_eq!(txn.get("e02"), Some(&Value::String("0001".into())));
        assert!(levels[1].get("transaction_set").is_none());
    }

    #[test]
    fn undeclared_nested_levels_keep_positional_default_names() {
        // With no declaration the GS/ST levels surface under the default
        // `functional_group` / `transaction_set` names keyed by untyped
        // positional `eNN` strings — the pre-declaration behavior.
        let levels = first_open_levels(reader(&interchange()));
        assert_eq!(levels.len(), 2);
        assert!(levels[0].get("functional_group").is_some());
        assert!(levels[1].get("transaction_set").is_some());
        let group = match levels[0].get("functional_group").unwrap() {
            Value::Map(m) => m,
            other => panic!("expected map, got {other:?}"),
        };
        // Untyped: the group control number stays a string.
        assert_eq!(group.get("e06"), Some(&Value::String("1".into())));
    }

    #[test]
    fn declaring_only_the_group_section_leaves_the_set_positional() {
        // The two levels are declared independently: naming only the GS
        // level types it while the ST level keeps the positional default.
        let config = X12ReaderConfig {
            group_section: Some(nested_section("grp", &[("e01", EnvelopeFieldType::String)])),
            ..Default::default()
        };
        let r = X12Reader::new(Cursor::new(interchange().into_bytes()), config);
        let levels = first_open_levels(r);
        assert!(levels[0].get("grp").is_some());
        assert!(levels[0].get("functional_group").is_none());
        // The undeclared ST level still uses the default name.
        assert!(levels[1].get("transaction_set").is_some());
    }

    #[test]
    fn undeclared_elements_are_dropped_from_a_declared_section() {
        // A declared field schema is the contract: only declared elements
        // surface, so an element the schema omits is absent from the typed
        // view even though it is on the wire.
        let config = X12ReaderConfig {
            set_section: Some(nested_section("txn", &[("e01", EnvelopeFieldType::String)])),
            ..Default::default()
        };
        let r = X12Reader::new(Cursor::new(interchange().into_bytes()), config);
        let levels = first_open_levels(r);
        let txn = match levels[1].get("txn").unwrap() {
            Value::Map(m) => m,
            other => panic!("expected map, got {other:?}"),
        };
        assert_eq!(txn.get("e01"), Some(&Value::String("850".into())));
        // ST02 is on the wire but undeclared, so it is not in the section.
        assert!(txn.get("e02").is_none());
    }

    #[test]
    fn declared_section_coercion_failure_errors() {
        // Declaring a non-numeric element as an integer fails the read with
        // a coercion error, the same way a mistyped ISA field does.
        let config = X12ReaderConfig {
            group_section: Some(nested_section(
                // GS01 is the functional id code "PO" — not an integer.
                "grp",
                &[("e01", EnvelopeFieldType::Int)],
            )),
            ..Default::default()
        };
        let mut r = X12Reader::new(Cursor::new(interchange().into_bytes()), config);
        let err = r.next_record().unwrap_err();
        assert!(
            matches!(&err, FormatError::X12(m) if m.contains("cannot coerce")),
            "expected a coercion error, got {err:?}"
        );
    }

    #[test]
    fn multi_set_multi_group_interchange_streams_all_bodies() {
        let data = format!(
            "{ISA}\
            GS*PO*S*R*20240101*1200*1*X*004010~\
            ST*850*0001~BEG*00*NE*A**20240101~SE*3*0001~\
            ST*850*0002~BEG*00*NE*B**20240101~SE*3*0002~\
            GE*2*1~\
            GS*PO*S*R*20240101*1300*2*X*004010~\
            ST*850*0003~BEG*00*NE*C**20240101~SE*3*0003~\
            GE*1*2~\
            IEA*2*000000001~"
        );
        let recs = collect(&data);
        // 3 sets × (ST + BEG) = 6 body records.
        assert_eq!(recs.len(), 6);
        assert_eq!(recs[0].get("set_ref"), Some(&Value::String("0001".into())));
        assert_eq!(recs[2].get("set_ref"), Some(&Value::String("0002".into())));
        assert_eq!(recs[4].get("set_ref"), Some(&Value::String("0003".into())));
    }

    #[test]
    fn se_segment_count_mismatch_errors() {
        let data = format!(
            "{ISA}GS*PO*S*R*20240101*1200*1*X*004010~\
            ST*850*0001~BEG*00*NE*A**20240101~SE*9*0001~GE*1*1~IEA*1*000000001~"
        );
        let err = error_from(&data);
        assert!(
            matches!(err, FormatError::StructuralCount { format: "X12", ref message } if message.contains("SE segment count mismatch"))
        );
    }

    #[test]
    fn se_ref_st_ref_mismatch_errors() {
        let data = format!(
            "{ISA}GS*PO*S*R*20240101*1200*1*X*004010~\
            ST*850*0001~BEG*00*NE*A**20240101~SE*3*WRONG~GE*1*1~IEA*1*000000001~"
        );
        let err = error_from(&data);
        assert!(matches!(err, FormatError::X12(m) if m.contains("does not echo the ST02")));
    }

    #[test]
    fn ge_set_count_mismatch_errors() {
        let data = format!(
            "{ISA}GS*PO*S*R*20240101*1200*1*X*004010~\
            ST*850*0001~BEG*00*NE*A**20240101~SE*3*0001~GE*9*1~IEA*1*000000001~"
        );
        let err = error_from(&data);
        assert!(
            matches!(err, FormatError::StructuralCount { format: "X12", ref message } if message.contains("GE transaction-set count mismatch"))
        );
    }

    #[test]
    fn ge_ref_gs_ref_mismatch_errors() {
        let data = format!(
            "{ISA}GS*PO*S*R*20240101*1200*1*X*004010~\
            ST*850*0001~BEG*00*NE*A**20240101~SE*3*0001~GE*1*WRONG~IEA*1*000000001~"
        );
        let err = error_from(&data);
        assert!(matches!(err, FormatError::X12(m) if m.contains("does not echo the GS06")));
    }

    #[test]
    fn iea_group_count_mismatch_errors() {
        let data = format!(
            "{ISA}GS*PO*S*R*20240101*1200*1*X*004010~\
            ST*850*0001~BEG*00*NE*A**20240101~SE*3*0001~GE*1*1~IEA*9*000000001~"
        );
        let err = error_from(&data);
        assert!(
            matches!(err, FormatError::StructuralCount { format: "X12", ref message } if message.contains("IEA functional-group count mismatch"))
        );
    }

    #[test]
    fn iea_control_number_mismatch_errors() {
        let data = format!(
            "{ISA}GS*PO*S*R*20240101*1200*1*X*004010~\
            ST*850*0001~BEG*00*NE*A**20240101~SE*3*0001~GE*1*1~IEA*1*999999999~"
        );
        let err = error_from(&data);
        assert!(matches!(err, FormatError::X12(m) if m.contains("does not echo the ISA13")));
    }

    #[test]
    fn missing_iea_at_eof_errors() {
        let data = format!(
            "{ISA}GS*PO*S*R*20240101*1200*1*X*004010~\
            ST*850*0001~BEG*00*NE*A**20240101~SE*3*0001~GE*1*1~"
        );
        let err = error_from(&data);
        assert!(matches!(err, FormatError::X12(m) if m.contains("no IEA")));
    }

    #[test]
    fn content_after_iea_errors() {
        let data = format!(
            "{ISA}GS*PO*S*R*20240101*1200*1*X*004010~\
            ST*850*0001~BEG*00*NE*A**20240101~SE*3*0001~GE*1*1~IEA*1*000000001~BEG*99~"
        );
        let err = error_from(&data);
        assert!(matches!(err, FormatError::X12(m) if m.contains("after the IEA")));
    }

    #[test]
    fn body_segment_outside_set_errors() {
        let data =
            format!("{ISA}GS*PO*S*R*20240101*1200*1*X*004010~BEG*00~GE*0*1~IEA*1*000000001~");
        let err = error_from(&data);
        assert!(matches!(err, FormatError::X12(m) if m.contains("outside any ST..SE")));
    }

    #[test]
    fn st_outside_group_errors() {
        // An ST with no enclosing GS is rejected. (GS is consumed before
        // ST in a valid stream; here we omit it.)
        let data = format!("{ISA}ST*850*0001~SE*2*0001~IEA*0*000000001~");
        let err = error_from(&data);
        assert!(matches!(err, FormatError::X12(m) if m.contains("outside any GS..GE")));
    }

    #[test]
    fn max_elements_overflow_errors_with_guidance() {
        let data = format!(
            "{ISA}GS*PO*S*R*20240101*1200*1*X*004010~\
            ST*850*0001~BEG*a*b*c*d*e~SE*3*0001~GE*1*1~IEA*1*000000001~"
        );
        let mut r = X12Reader::new(
            Cursor::new(data.into_bytes()),
            X12ReaderConfig {
                max_elements: 2,
                ..Default::default()
            },
        );
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected element overflow"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::X12(m) if m.contains("max_elements")));
    }

    fn error_from(data: &str) -> FormatError {
        let mut r = reader(data);
        loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected an error, got clean end"),
                Err(e) => break e,
            }
        }
    }

    fn isa_section(fields: &[(&str, EnvelopeFieldType)]) -> EnvelopeConfig {
        let mut cfg = EnvelopeConfig::default();
        let mut field_map = IndexMap::new();
        for (k, ty) in fields {
            field_map.insert((*k).to_string(), *ty);
        }
        cfg.sections.insert(
            "interchange".to_string(),
            EnvelopeSection {
                extract: EnvelopeExtract::Segment("ISA".to_string()),
                fields: field_map,
            },
        );
        cfg
    }

    #[test]
    fn prepare_document_extracts_isa_positional_fields_typed() {
        let cfg = isa_section(&[("e13", EnvelopeFieldType::String)]);
        let mut r = reader(&interchange());
        let sections = r.prepare_document(&cfg).unwrap();
        let interchange = match sections.get("interchange").unwrap() {
            Value::Map(m) => m,
            other => panic!("expected map, got {other:?}"),
        };
        // ISA13 (element 13) is the interchange control number.
        assert_eq!(
            interchange.get("e13"),
            Some(&Value::String("000000001".into()))
        );
        // Body still streams after the header pre-scan.
        let recs: Vec<_> = std::iter::from_fn(|| r.next_record().unwrap()).collect();
        assert_eq!(recs.len(), 3);
    }

    #[test]
    fn prepare_document_rejects_non_isa_segment() {
        let mut cfg = EnvelopeConfig::default();
        cfg.sections.insert(
            "group".to_string(),
            EnvelopeSection {
                extract: EnvelopeExtract::Segment("GS".to_string()),
                fields: IndexMap::new(),
            },
        );
        let mut r = reader(&interchange());
        let err = r.prepare_document(&cfg).unwrap_err();
        assert!(matches!(err, FormatError::X12(m) if m.contains("not extractable")));
    }

    #[test]
    fn prepare_document_rejects_xml_path_extract() {
        let mut cfg = EnvelopeConfig::default();
        cfg.sections.insert(
            "bad".to_string(),
            EnvelopeSection {
                extract: EnvelopeExtract::XmlPath("/doc".to_string()),
                fields: IndexMap::new(),
            },
        );
        let mut r = reader(&interchange());
        let err = r.prepare_document(&cfg).unwrap_err();
        assert!(matches!(err, FormatError::X12(m) if m.contains("non-`segment`")));
    }

    /// Full reader → `$doc` → writer → reader round-trip exercising the
    /// ISA header reconstruction and the three-tier envelope rebuild.
    #[test]
    fn reader_doc_writer_round_trip_preserves_isa_and_structure() {
        use crate::traits::FormatWriter;
        use crate::x12::writer::{X12Writer, X12WriterConfig};
        use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord};

        let input = interchange();

        // 1. Read the ISA envelope section and body records.
        let cfg = isa_section(&[("e13", EnvelopeFieldType::String)]);
        let mut r = reader(&input);
        let sections = r.prepare_document(&cfg).unwrap();
        let body_recs: Vec<Record> = std::iter::from_fn(|| r.next_record().unwrap()).collect();
        assert_eq!(body_recs.len(), 3); // ST, BEG, PO1

        // 2. Attach the document context and re-emit through the writer.
        let ctx = Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from("orders.x12"),
            EnvelopeRecord::from_sections(sections),
        ));
        let schema = r.schema().unwrap();
        let out = {
            let mut buf = Vec::new();
            let mut w = X12Writer::new(
                std::io::Cursor::new(&mut buf),
                Arc::clone(&schema),
                X12WriterConfig {
                    interchange_from_doc: Some("interchange".into()),
                    group_header: Some(vec![
                        "PO".into(),
                        "SENDER".into(),
                        "RECEIVER".into(),
                        "20240101".into(),
                        "1200".into(),
                        "1".into(),
                        "X".into(),
                        "004010".into(),
                    ]),
                    segment_newline: false,
                    ..Default::default()
                },
            );
            for rec in &body_recs {
                let mut rec = rec.clone();
                rec.set_doc_ctx(Arc::clone(&ctx));
                w.write_record(&rec).unwrap();
            }
            w.flush().unwrap();
            String::from_utf8(buf).unwrap()
        };

        // The reconstructed interchange opens with the echoed ISA and
        // closes with an IEA echoing its control number.
        assert!(out.starts_with(ISA), "{out}");
        assert!(out.contains("IEA*1*000000001~"), "{out}");
        assert!(out.contains("ST*850*0001~"), "{out}");
        assert!(out.contains("SE*4*0001~"), "{out}");
        assert!(out.contains("GE*1*1~"), "{out}");

        // 3. Re-read the emitted interchange: it validates and yields the
        // same body records.
        let recs2 = collect(&out);
        assert_eq!(recs2.len(), 3);
        assert_eq!(recs2[2].get("seg_id"), Some(&Value::String("PO1".into())));
    }

    /// A minimal interchange whose body carries Latin-1 high bytes (`0xE9`
    /// for é, `0xF1` for ñ) in a free-text element. The bytes are not valid
    /// UTF-8, so the default reader rejects them; the Latin-1 reader decodes
    /// them to the matching codepoints.
    fn latin1_interchange() -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(ISA.as_bytes());
        data.extend_from_slice(b"GS*PO*S*R*20240101*1200*1*X*004010~");
        data.extend_from_slice(b"ST*850*0001~");
        // N1*BT*Caf\xE9 A\xF1o~ — a free-text name with two Latin-1 high bytes.
        data.extend_from_slice(b"N1*BT*Caf");
        data.push(0xE9);
        data.extend_from_slice(b" A");
        data.push(0xF1);
        data.extend_from_slice(b"o~");
        data.extend_from_slice(b"SE*3*0001~");
        data.extend_from_slice(b"GE*1*1~");
        data.extend_from_slice(b"IEA*1*000000001~");
        data
    }

    fn reader_bytes(data: Vec<u8>, charset: Charset) -> X12Reader<Cursor<Vec<u8>>> {
        X12Reader::new(
            Cursor::new(data),
            X12ReaderConfig {
                charset,
                ..Default::default()
            },
        )
    }

    #[test]
    fn latin1_body_high_bytes_decode_to_codepoints() {
        let mut r = reader_bytes(latin1_interchange(), Charset::Latin1);
        let recs: Vec<_> = std::iter::from_fn(|| r.next_record().unwrap()).collect();
        // ST and N1 are body records.
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[1].get("seg_id"), Some(&Value::String("N1".into())));
        // The free-text element decodes through Latin-1: 0xE9→é, 0xF1→ñ.
        assert_eq!(recs[1].get("e02"), Some(&Value::String("Café Año".into())));
    }

    #[test]
    fn latin1_body_rejected_under_default_utf8() {
        let mut r = reader_bytes(latin1_interchange(), Charset::Utf8);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected a UTF-8 decode error"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Charset(m) if m.contains("not valid UTF-8")));
    }

    /// The headline acceptance: a Latin-1 interchange read with the matching
    /// charset and re-written with it produces byte-identical output, and
    /// the re-read body matches.
    #[test]
    fn latin1_read_write_read_is_byte_faithful() {
        use crate::traits::{FormatReader, FormatWriter};
        use crate::x12::writer::{X12Writer, X12WriterConfig};

        let input = latin1_interchange();

        // Read with the Latin-1 charset.
        let mut r = reader_bytes(input.clone(), Charset::Latin1);
        let body_recs: Vec<Record> = std::iter::from_fn(|| r.next_record().unwrap()).collect();
        let schema = r.schema().unwrap();

        // Re-emit through a Latin-1 writer with the literal ISA/GS headers,
        // matching the input fixture so the bytes line up exactly.
        let out: Vec<u8> = {
            let mut buf = Vec::new();
            let mut w = X12Writer::new(
                Cursor::new(&mut buf),
                Arc::clone(&schema),
                X12WriterConfig {
                    interchange: Some(
                        split_isa(
                            ISA.as_bytes(),
                            &crate::x12::tokenizer::Delimiters {
                                element: b'*',
                                subelement: b':',
                                terminator: b'~',
                            },
                            Charset::Latin1,
                        )
                        .unwrap()
                        .elements,
                    ),
                    group_header: Some(
                        ["PO", "S", "R", "20240101", "1200", "1", "X", "004010"]
                            .iter()
                            .map(|s| s.to_string())
                            .collect(),
                    ),
                    segment_newline: false,
                    charset: Charset::Latin1,
                    ..Default::default()
                },
            );
            for rec in &body_recs {
                w.write_record(rec).unwrap();
            }
            w.flush().unwrap();
            buf
        };

        // The output bytes equal the input bytes, including the two Latin-1
        // high bytes — the round-trip never passed through UTF-8.
        assert_eq!(out, input, "Latin-1 round-trip must be byte-faithful");

        // Re-read the emitted bytes under Latin-1: the body decodes the same.
        let mut r2 = reader_bytes(out, Charset::Latin1);
        let recs2: Vec<_> = std::iter::from_fn(|| r2.next_record().unwrap()).collect();
        assert_eq!(recs2.len(), 2);
        assert_eq!(recs2[1].get("e02"), Some(&Value::String("Café Año".into())));
    }
}
