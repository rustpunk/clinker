//! HL7 v2 file reader.
//!
//! Streaming, row-at-a-time: each segment of an HL7 v2 file becomes one
//! [`Record`] with a static positional schema
//! `[seg_id, set_ref, set_type, f01..f<max>]`. The `MSH` header that opens
//! a message **is** emitted as a body record (its fields carry the
//! message's metadata, and downstream CXL reads them positionally).
//! Optional batch/file envelope segments (`FHS`/`FTS`, `BHS`/`BTS`) are
//! consumed by the reader to drive nested document levels and validate
//! batch/file counts inline; they are never emitted as body records.
//!
//! The optional envelope tiers map onto nested document-context levels: an
//! `FHS` file header is the file-level document served by
//! [`FormatReader::prepare_document`], and each `BHS` batch and `MSH`
//! message opens a nested level via [`EnvelopeEvent`] the source ingest
//! driver drains after each `next_record`. A `BHS` queues an `OpenLevel`
//! carrying the batch's `$doc` section; an `MSH` queues a nested
//! `OpenLevel` for the message and is then emitted. A message closes (its
//! `CloseLevel` queued) when the next `MSH`, `BTS`, `FTS`, or end-of-input
//! arrives — HL7 messages have no explicit per-message trailer segment, so
//! the next header *is* the boundary. A `BTS` queues `CloseLevel` for the
//! batch and a `FTS` ends the file-level document via the driver's
//! end-of-input / file-transition sweep.
//!
//! All tiers are optional. A bare stream of `MSH` messages with no `FHS`
//! or `BHS` wrapping is valid: each message still opens and closes its own
//! nested level, and there is simply no file-level or batch-level section.
//!
//! Memory model: only the file header (`FHS`) is pre-scanned and retained,
//! so `prepare_document` exposes the `FHS` fields as a `$doc` section with
//! O(FHS) held memory. The body streams one segment at a time — the whole
//! file is never buffered. Trailer counts (`BTS`/`FTS`) are validated as
//! they arrive, not surfaced as envelope sections, because they follow the
//! body they close.

use std::io::{BufReader, Read};
use std::sync::Arc;

use clinker_record::{Record, Schema, Value};
use indexmap::IndexMap;

use crate::envelope::{EnvelopeConfig, EnvelopeEvent, EnvelopeExtract, coerce_section_fields};
use crate::error::FormatError;
use crate::hl7::RAW_FIELDS_KEY;
use crate::hl7::field_split::Hl7FieldSplit;
use crate::hl7::tokenizer::{
    Delimiters, ParsedSegment, SegmentTokenizer, raw_field, split_segment,
};
use crate::traits::FormatReader;

/// Default ceiling on the number of positional field columns the record
/// schema exposes. A segment carrying more data fields than this errors
/// with guidance rather than silently truncating.
const DEFAULT_MAX_FIELDS: usize = 64;

/// The envelope segment tag `prepare_document` can extract as a `$doc`
/// section. Only the file header `FHS` is resolvable from the bounded
/// header pre-scan; `BHS` batches and `MSH` messages arrive mid-body and
/// surface as nested document levels instead.
const FHS_TAG: &str = "FHS";

/// 1-based wire field position of the message type (`MSH-9`). The MSH
/// off-by-one — the `MSH-1` field separator is implicit and `MSH-2`
/// encoding characters is the first split field — puts `MSH-9` at field
/// list index 7.
const MSH_MESSAGE_TYPE_INDEX: usize = 7;

/// 1-based wire field position of the message control id (`MSH-10`), the
/// per-message reference carried on every record of the message. Field
/// list index 8 under the MSH off-by-one.
const MSH_CONTROL_ID_INDEX: usize = 8;

/// Configuration for the HL7 reader.
pub struct Hl7ReaderConfig {
    /// Number of positional `fNN` field columns on the record schema. A
    /// body segment with more data fields than this is rejected.
    pub max_fields: usize,
    /// Opt-in composite-field splits. Each entry explodes one positional
    /// field into per-repetition / per-component / per-sub-component
    /// columns, replacing the verbatim `fNN` column with the structured
    /// columns; the writer re-assembles the wire field from them. Empty by
    /// default, so the positional model is byte-identical to today unless a
    /// split is declared.
    pub split_fields: Vec<Hl7FieldSplit>,
}

impl Default for Hl7ReaderConfig {
    fn default() -> Self {
        Self {
            max_fields: DEFAULT_MAX_FIELDS,
            split_fields: Vec::new(),
        }
    }
}

/// Streaming HL7 v2 file reader.
///
/// Holds the tokenizer, the static positional schema, the inline
/// envelope-validation state for the optional file/batch/message tiers, and
/// the queue of nested-envelope events the source ingest driver drains
/// after each `next_record`. The `FHS` fields are stashed at
/// initialization so [`FormatReader::prepare_document`] can serve an `FHS`
/// envelope section without re-reading the source.
pub struct Hl7Reader<R: Read> {
    tokenizer: SegmentTokenizer<BufReader<R>>,
    schema: Arc<Schema>,
    max_fields: usize,
    /// The ordered field-column layout: each positional field is either kept
    /// verbatim as one `fNN` column or exploded into its split-leaf columns.
    /// Built once from the config so `body_record` maps every segment the
    /// same way.
    field_layout: Vec<FieldGroup>,
    initialized: bool,
    /// The first segment, read during delimiter discovery and held until
    /// the body stream consumes it (an `MSH` is emitted, an `FHS`/`BHS`
    /// drives the envelope).
    pending_segment: Option<String>,
    /// Raw `FHS` data fields, stashed at init when the file opens with a
    /// file header, for envelope serving.
    fhs_fields: Vec<String>,
    /// State of the batch currently open, if any.
    open_batch: Option<OpenBatch>,
    /// State of the message currently streaming, if any.
    open_message: Option<OpenMessage>,
    /// `true` once the body stream has consumed the `FHS` header, so a
    /// second `FHS` is an error. Set independently of `fhs_fields`, which
    /// `prepare_document` may populate from a peek before the body stream
    /// reaches the segment.
    file_opened: bool,
    /// Count of `BHS` batches seen, checked against `FTS01`.
    batch_count: u64,
    /// `true` once `FTS` has been consumed; any further segment is an
    /// after-trailer-content error.
    file_closed: bool,
    /// Nested-envelope events queued during the most recent `next_record`,
    /// drained by the driver via [`FormatReader::take_envelope_events`].
    pending_events: Vec<EnvelopeEvent>,
    done: bool,
}

/// One positional field's place in the record schema: kept verbatim as a
/// single `fNN` column, or exploded into its split-leaf columns.
///
/// The layout is the single source of both the column list (for the schema)
/// and the per-segment value mapping (in `body_record`), so the reader and
/// its schema never disagree on which columns a split field contributes.
enum FieldGroup {
    /// A field kept whole at its 1-based wire position, as `fNN`.
    Verbatim { position: usize },
    /// A field exploded per its split declaration.
    Split(Hl7FieldSplit),
}

impl FieldGroup {
    /// The schema column names this group contributes, in column order.
    fn column_names(&self) -> Vec<String> {
        match self {
            FieldGroup::Verbatim { position } => vec![positional_key(position - 1)],
            FieldGroup::Split(split) => split.column_names(),
        }
    }
}

/// Build the ordered field-column layout from the field width and the split
/// declarations: positions `1..=max_fields`, with each declared split
/// replacing its verbatim `fNN` slot. A split naming a position outside the
/// `max_fields` range is dropped here — the plan-config layer validates the
/// in-range invariant with source spans before the reader is built.
fn build_field_layout(max_fields: usize, splits: &[Hl7FieldSplit]) -> Vec<FieldGroup> {
    let mut layout = Vec::with_capacity(max_fields);
    for position in 1..=max_fields {
        match splits.iter().find(|s| s.field_index == position) {
            Some(split) => layout.push(FieldGroup::Split(*split)),
            None => layout.push(FieldGroup::Verbatim { position }),
        }
    }
    layout
}

/// Per-batch validation state for the batch currently open.
struct OpenBatch {
    /// Count of messages (`MSH`) seen in this batch, checked against the
    /// `BTS01` batch message count.
    message_count: u64,
}

/// Per-message state for the message currently streaming.
struct OpenMessage {
    /// `MSH-10` message control id, stamped on every record of the message
    /// as `set_ref`.
    control_id: String,
    /// `MSH-9` message type, stamped on every record as `set_type`.
    message_type: String,
}

impl<R: Read> Hl7Reader<R> {
    /// Build a reader over any `Read` source with the given field-width
    /// configuration. Delimiter discovery and the first-segment read are
    /// deferred to the first call.
    pub fn new(reader: R, config: Hl7ReaderConfig) -> Self {
        let field_layout = build_field_layout(config.max_fields, &config.split_fields);
        let schema = build_schema(&field_layout);
        Self {
            tokenizer: SegmentTokenizer::new(BufReader::new(reader)),
            schema,
            max_fields: config.max_fields,
            field_layout,
            initialized: false,
            pending_segment: None,
            fhs_fields: Vec::new(),
            open_batch: None,
            open_message: None,
            file_opened: false,
            batch_count: 0,
            file_closed: false,
            pending_events: Vec::new(),
            done: false,
        }
    }

    /// Read the first segment for delimiter discovery, stashing it as the
    /// pending segment so the body stream consumes it. The first segment
    /// resolves the delimiter set; whether it is `MSH`, `FHS`, or `BHS` is
    /// decided when the body stream processes it. Idempotent.
    fn ensure_initialized(&mut self) -> Result<(), FormatError> {
        if self.initialized {
            return Ok(());
        }
        self.initialized = true;
        // The first segment must carry the MSH delimiter declaration. An
        // FHS/BHS-led file still embeds an MSH for each message, but HL7
        // requires the file's delimiters to match the first MSH; producers
        // universally repeat the same encoding chars in FHS/BHS, so the
        // tokenizer reads the field separator + encoding chars from the
        // first segment regardless of its tag.
        let first = self.tokenizer.read_first_segment()?;
        self.pending_segment = Some(first);
        Ok(())
    }

    /// Pull the next segment from the pending slot or the tokenizer.
    fn next_raw(&mut self) -> Result<Option<String>, FormatError> {
        if let Some(seg) = self.pending_segment.take() {
            return Ok(Some(seg));
        }
        self.tokenizer.next_segment()
    }

    /// Pull the next service-or-body segment and advance the envelope
    /// state. Returns the body record to emit, or `None` at clean
    /// end-of-input. Envelope segments are consumed transparently (the loop
    /// continues) so the caller only ever sees body records; envelope
    /// boundaries crossed on the way are queued for the driver.
    fn pull_next(&mut self) -> Result<Option<Record>, FormatError> {
        loop {
            let raw = match self.next_raw()? {
                Some(s) => s,
                None => {
                    // End of input. Close every still-open level so the
                    // queued envelope events balance: first any open message
                    // (which has no trailer — EOF is its natural boundary),
                    // then an open batch whose `BTS` the file omitted. HL7
                    // batch/file trailers are advisory and real files
                    // frequently drop them, so a missing `BTS`/`FTS` at EOF
                    // is a clean close, not a truncation error; the optional
                    // count check only runs when a trailer is actually
                    // present.
                    self.close_message_if_open();
                    self.close_batch_if_open();
                    self.done = true;
                    return Ok(None);
                }
            };
            let delims = self.tokenizer.delimiters();
            let segment = split_segment(&raw, &delims);

            if self.file_closed {
                return Err(FormatError::Hl7(format!(
                    "segment {:?} found after the FTS file trailer; content past FTS is not \
                     permitted",
                    segment.tag
                )));
            }

            match segment.tag.as_str() {
                "FHS" => self.open_file(&segment)?,
                "FTS" => self.close_file(&segment)?,
                "BHS" => self.open_batch(&segment)?,
                "BTS" => self.close_batch(&segment)?,
                "MSH" => {
                    self.open_message(&segment);
                    let record = self.body_record(&raw, &segment)?;
                    return Ok(Some(record));
                }
                _ => {
                    if self.open_message.is_none() {
                        return Err(FormatError::Hl7(format!(
                            "body segment {:?} appeared before any MSH message header",
                            segment.tag
                        )));
                    }
                    let record = self.body_record(&raw, &segment)?;
                    return Ok(Some(record));
                }
            }
        }
    }

    /// Open the file-level document on an `FHS` segment, stashing its fields
    /// for `prepare_document`. The `FHS` is the file-level document served
    /// by the driver's pre-scan, so it queues no nested event. The
    /// `file_opened` flag — not the presence of `fhs_fields`, which a
    /// `prepare_document` peek may have populated first — guards against a
    /// genuine second `FHS`.
    fn open_file(&mut self, fhs: &ParsedSegment) -> Result<(), FormatError> {
        if self.file_opened || self.batch_count > 0 || self.open_message.is_some() {
            return Err(FormatError::Hl7(
                "a second FHS file header appeared, or one followed batch/message content; \
                 a file carries at most one FHS, before any batch or message"
                    .into(),
            ));
        }
        self.fhs_fields = fhs.fields.clone();
        self.file_opened = true;
        Ok(())
    }

    /// Validate and close the file against its `FTS` trailer. `FTS-1`
    /// (field index 0) is the file batch count.
    fn close_file(&mut self, fts: &ParsedSegment) -> Result<(), FormatError> {
        self.close_message_if_open();
        if self.open_batch.is_some() {
            return Err(FormatError::Hl7(
                "FTS file trailer arrived before the last batch's BTS".into(),
            ));
        }
        // FTS-1 is optional in practice; validate it only when present.
        if let Some(claimed) = parse_optional_count(fts.fields.first(), "FTS", "file batch count")?
            && claimed != self.batch_count
        {
            return Err(FormatError::hl7_structural_count(format!(
                "FTS file batch count mismatch: trailer claims {claimed}, file contains \
                 {} batches",
                self.batch_count
            )));
        }
        self.file_closed = true;
        Ok(())
    }

    /// Open a batch on a `BHS` segment, queuing the batch's `OpenLevel` so
    /// the driver opens a nested document context.
    fn open_batch(&mut self, bhs: &ParsedSegment) -> Result<(), FormatError> {
        self.close_message_if_open();
        if self.open_batch.is_some() {
            return Err(FormatError::Hl7(
                "BHS opened a new batch before the previous batch's BTS trailer; batches must \
                 be BHS..BTS balanced"
                    .into(),
            ));
        }
        self.open_batch = Some(OpenBatch { message_count: 0 });
        self.batch_count += 1;
        self.pending_events.push(EnvelopeEvent::OpenLevel {
            sections: positional_section("batch", &bhs.fields),
        });
        Ok(())
    }

    /// Validate and close the open batch against its `BTS` trailer, queuing
    /// a `CloseLevel`. `BTS-1` (field index 0) is the batch message count.
    fn close_batch(&mut self, bts: &ParsedSegment) -> Result<(), FormatError> {
        self.close_message_if_open();
        let batch = self
            .open_batch
            .take()
            .ok_or_else(|| FormatError::Hl7("BTS trailer with no open BHS batch".into()))?;
        if let Some(claimed) =
            parse_optional_count(bts.fields.first(), "BTS", "batch message count")?
            && claimed != batch.message_count
        {
            return Err(FormatError::hl7_structural_count(format!(
                "BTS batch message count mismatch: trailer claims {claimed}, batch contains \
                 {} messages",
                batch.message_count
            )));
        }
        self.pending_events.push(EnvelopeEvent::CloseLevel);
        Ok(())
    }

    /// Open a message on an `MSH` segment, queuing a nested `OpenLevel` and
    /// tallying it against the open batch. The previous message (if any) is
    /// closed first — HL7 has no per-message trailer, so the next `MSH` is
    /// the boundary.
    fn open_message(&mut self, msh: &ParsedSegment) {
        self.close_message_if_open();
        let message_type = msh
            .fields
            .get(MSH_MESSAGE_TYPE_INDEX)
            .cloned()
            .unwrap_or_default();
        let control_id = msh
            .fields
            .get(MSH_CONTROL_ID_INDEX)
            .cloned()
            .unwrap_or_default();
        if let Some(batch) = self.open_batch.as_mut() {
            batch.message_count += 1;
        }
        self.pending_events.push(EnvelopeEvent::OpenLevel {
            sections: message_section(msh),
        });
        self.open_message = Some(OpenMessage {
            control_id,
            message_type,
        });
    }

    /// Close the open message (if any), queuing its `CloseLevel`. A no-op
    /// when no message is open. Called on the next `MSH`/`BHS`/`BTS`/`FTS`
    /// and at end-of-input.
    fn close_message_if_open(&mut self) {
        if self.open_message.take().is_some() {
            self.pending_events.push(EnvelopeEvent::CloseLevel);
        }
    }

    /// Close the open batch (if any) without a `BTS` count check, queuing
    /// its `CloseLevel`. A no-op when no batch is open. Used at end-of-input
    /// for a batch whose `BTS` trailer the file omitted, so the queued
    /// `OpenLevel`/`CloseLevel` events stay balanced. An explicit `BTS`
    /// trailer goes through [`Self::close_batch`] instead, which also
    /// validates the optional message count.
    fn close_batch_if_open(&mut self) {
        if self.open_batch.take().is_some() {
            self.pending_events.push(EnvelopeEvent::CloseLevel);
        }
    }

    /// Map a parsed segment to a positional [`Record`] under the static
    /// schema. `seg_id`, `set_ref`, and `set_type` are stamped from the
    /// open message; the data fields follow the field layout — verbatim
    /// fields fill one `fNN` column each, split fields explode into their
    /// structured columns. Absent fields and leaves stay `Value::Null`; a
    /// field count past `max_fields`, or a split field whose structure
    /// overflows its declared width, errors with guidance.
    fn body_record(&self, raw: &str, segment: &ParsedSegment) -> Result<Record, FormatError> {
        if segment.fields.len() > self.max_fields {
            return Err(FormatError::Hl7(format!(
                "segment {:?} carries {} data fields, exceeding the configured max_fields of {}; \
                 raise the source's `max_fields` option",
                segment.tag,
                segment.fields.len(),
                self.max_fields
            )));
        }
        let (set_ref, set_type) = match self.open_message.as_ref() {
            Some(msg) => (msg.control_id.clone(), msg.message_type.clone()),
            None => (String::new(), String::new()),
        };
        let delims = self.tokenizer.delimiters();

        let mut values: Vec<Value> = Vec::with_capacity(3 + self.schema.columns().len());
        values.push(Value::String(segment.tag.as_str().into()));
        values.push(string_or_null(&set_ref));
        values.push(string_or_null(&set_type));
        for group in &self.field_layout {
            self.push_field_group(raw, segment, group, &delims, &mut values)?;
        }
        Ok(Record::new(Arc::clone(&self.schema), values))
    }

    /// Append a field group's values to a record's value list: one cell for a
    /// verbatim field (the decoded value, or `Null` when absent), or one cell
    /// per leaf for a split field. A split reads the *raw* field text (re-cut
    /// from the original segment string) so an escaped separator stays inside
    /// its leaf rather than being mistaken for a structural boundary.
    fn push_field_group(
        &self,
        raw: &str,
        segment: &ParsedSegment,
        group: &FieldGroup,
        delims: &Delimiters,
        values: &mut Vec<Value>,
    ) -> Result<(), FormatError> {
        match group {
            FieldGroup::Verbatim { position } => match segment.fields.get(position - 1) {
                Some(f) => values.push(string_or_null(f)),
                None => values.push(Value::Null),
            },
            FieldGroup::Split(split) => {
                let leaves = match raw_field(raw, delims, split.field_index) {
                    Some(raw_text) => split.split_value(&raw_text, delims)?,
                    // The split field is absent on this segment (a shorter
                    // segment than the split position): every leaf is null.
                    None => vec![None; split.column_count()],
                };
                for leaf in leaves {
                    values.push(match leaf {
                        Some(text) => string_or_null(&text),
                        None => Value::Null,
                    });
                }
            }
        }
        Ok(())
    }
}

impl<R: Read + Send> FormatReader for Hl7Reader<R> {
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
        // The FHS header (if present) is the only declared-envelope-section
        // source. It is the first segment only when the file opens with a
        // file header; peek the pending segment to populate `fhs_fields`
        // before serving, without consuming a body record.
        if self.fhs_fields.is_empty()
            && let Some(pending) = self.pending_segment.as_ref()
        {
            let delims = self.tokenizer.delimiters();
            let parsed = split_segment(pending, &delims);
            if parsed.tag == "FHS" {
                self.fhs_fields = parsed.fields;
            }
        }

        let mut out: IndexMap<Box<str>, Value> = IndexMap::with_capacity(config.sections.len());
        for (name, section) in &config.sections {
            let segment_tag = match &section.extract {
                EnvelopeExtract::Segment(tag) => tag.as_str(),
                EnvelopeExtract::XmlPath(_) | EnvelopeExtract::JsonPointer(_) => {
                    return Err(FormatError::Hl7(format!(
                        "envelope section {name:?}: declared a non-`segment` extract against an \
                         HL7 source. Use `segment` (e.g. `extract: {{ segment: \"FHS\" }}`) for \
                         HL7 envelope sections."
                    )));
                }
            };
            if segment_tag != FHS_TAG {
                return Err(FormatError::Hl7(format!(
                    "envelope section {name:?}: segment {segment_tag:?} is not extractable as a \
                     file-level $doc section. Only the file header `FHS` is available from the \
                     header pre-scan; `BHS` batches and `MSH` messages surface as nested document \
                     levels, and trailer segments (`BTS`/`FTS`) are validated by the reader, not \
                     exposed as $doc fields."
                )));
            }
            if self.fhs_fields.is_empty() {
                return Err(FormatError::Hl7(format!(
                    "envelope section {name:?}: declared a `segment: \"FHS\"` extract, but the \
                     file has no FHS file header. A bare MSH-led HL7 file has no file-level \
                     envelope; remove the section or wrap the messages in an FHS..FTS file."
                )));
            }
            let raw: Vec<(String, String)> = self
                .fhs_fields
                .iter()
                .enumerate()
                .map(|(i, f)| (positional_key(i), f.clone()))
                .collect();
            let typed = coerce_section_fields(raw, &section.fields).map_err(FormatError::Hl7)?;
            out.insert(Box::from(name.as_str()), Value::Map(Box::new(typed)));
        }
        Ok(out)
    }
}

/// Build the `MSH` message-level `$doc` section under a fixed
/// `transaction_set` name keyed by positional `fNN` fields, plus the raw
/// field list under the engine-internal key for lossless writer
/// reconstruction of an echoed `MSH` header.
fn message_section(msh: &ParsedSegment) -> IndexMap<Box<str>, Value> {
    let mut sections = positional_section("transaction_set", &msh.fields);
    if let Some(Value::Map(fields)) = sections.get_mut("transaction_set") {
        let raw: Vec<Value> = msh
            .fields
            .iter()
            .map(|f| Value::String(f.as_str().into()))
            .collect();
        fields.insert(Box::from(RAW_FIELDS_KEY), Value::Array(raw));
    }
    sections
}

/// Build a single-named `$doc` section whose fields are the segment's
/// positional fields (`f01`, `f02`, …). Used for the nested `BHS`/`MSH`
/// levels, which carry their whole header verbatim rather than a
/// user-declared field schema.
fn positional_section(name: &str, fields: &[String]) -> IndexMap<Box<str>, Value> {
    let mut payload: IndexMap<Box<str>, Value> = IndexMap::with_capacity(fields.len());
    for (i, f) in fields.iter().enumerate() {
        payload.insert(positional_key(i).into_boxed_str(), string_or_null(f));
    }
    let mut sections: IndexMap<Box<str>, Value> = IndexMap::with_capacity(1);
    sections.insert(Box::from(name), Value::Map(Box::new(payload)));
    sections
}

/// Parse an optional trailer count: `None`/empty field disables the check,
/// a non-numeric field is an error naming the segment and count.
fn parse_optional_count(
    raw: Option<&String>,
    segment: &str,
    field: &str,
) -> Result<Option<u64>, FormatError> {
    match raw {
        None => Ok(None),
        Some(text) if text.trim().is_empty() => Ok(None),
        Some(text) => {
            text.trim().parse().map(Some).map_err(|_| {
                FormatError::Hl7(format!("{segment} {field} {text:?} is not a number"))
            })
        }
    }
}

/// Build the static positional schema `[seg_id, set_ref, set_type, <field
/// columns>]` from the field layout. A verbatim field contributes one `fNN`
/// column; a split field contributes its structured leaf columns in place of
/// `fNN`. All columns are string-typed; field text is stored verbatim
/// (escapes decoded) so the round-trip is lossless.
fn build_schema(layout: &[FieldGroup]) -> Arc<Schema> {
    let mut columns: Vec<Box<str>> = Vec::with_capacity(3 + layout.len());
    columns.push(Box::from("seg_id"));
    columns.push(Box::from("set_ref"));
    columns.push(Box::from("set_type"));
    for group in layout {
        for name in group.column_names() {
            columns.push(name.into_boxed_str());
        }
    }
    Arc::new(Schema::new(columns))
}

/// Positional field column name for field index `i`: `f01`, `f02`, …
fn positional_key(i: usize) -> String {
    format!("f{:02}", i + 1)
}

/// Map a field string to a `Value`: empty text becomes `Null` so an absent
/// or blank field is uniformly null at the schema slot.
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

    /// A minimal MSH header with the conventional `|^~\&` delimiters,
    /// message type `ADT^A01`, control id `MSG001`.
    const MSH: &str =
        "MSH|^~\\&|SENDAPP|SENDFAC|RCVAPP|RCVFAC|20240101120000||ADT^A01|MSG001|P|2.5";

    /// A bare single message (no batch/file envelope): MSH, EVN, PID, PV1.
    fn adt_message() -> String {
        format!("{MSH}\rEVN|A01|20240101120000\rPID|1||PATID^^^MRN||DOE^JOHN\rPV1|1|I")
    }

    fn reader(data: &str) -> Hl7Reader<Cursor<Vec<u8>>> {
        Hl7Reader::new(
            Cursor::new(data.as_bytes().to_vec()),
            Hl7ReaderConfig::default(),
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

    fn reader_with_splits(
        data: &str,
        split_fields: Vec<Hl7FieldSplit>,
    ) -> Hl7Reader<Cursor<Vec<u8>>> {
        Hl7Reader::new(
            Cursor::new(data.as_bytes().to_vec()),
            Hl7ReaderConfig {
                max_fields: DEFAULT_MAX_FIELDS,
                split_fields,
            },
        )
    }

    fn collect_with_splits(data: &str, split_fields: Vec<Hl7FieldSplit>) -> Vec<Record> {
        let mut r = reader_with_splits(data, split_fields);
        let mut out = Vec::new();
        while let Some(rec) = r.next_record().unwrap() {
            out.push(rec);
        }
        out
    }

    fn split(field_index: usize, reps: usize, comps: usize, subs: usize) -> Hl7FieldSplit {
        Hl7FieldSplit {
            field_index,
            repetitions: reps,
            components: comps,
            subcomponents: subs,
        }
    }

    #[test]
    fn one_record_per_segment_including_msh() {
        let recs = collect(&adt_message());
        // MSH, EVN, PID, PV1 are all body records.
        assert_eq!(recs.len(), 4);
        assert_eq!(recs[0].get("seg_id"), Some(&Value::String("MSH".into())));
        assert_eq!(recs[1].get("seg_id"), Some(&Value::String("EVN".into())));
        assert_eq!(recs[2].get("seg_id"), Some(&Value::String("PID".into())));
        assert_eq!(recs[3].get("seg_id"), Some(&Value::String("PV1".into())));
    }

    #[test]
    fn msh_off_by_one_field_positions() {
        let recs = collect(&adt_message());
        let msh = &recs[0];
        // MSH-2 (encoding chars) is f01, kept verbatim.
        assert_eq!(msh.get("f01"), Some(&Value::String("^~\\&".into())));
        // MSH-3 (sending app) is f02.
        assert_eq!(msh.get("f02"), Some(&Value::String("SENDAPP".into())));
        // MSH-9 (message type) is f08.
        assert_eq!(msh.get("f08"), Some(&Value::String("ADT^A01".into())));
        // MSH-10 (control id) is f09.
        assert_eq!(msh.get("f09"), Some(&Value::String("MSG001".into())));
    }

    #[test]
    fn set_ref_and_type_stamped_on_every_record() {
        let recs = collect(&adt_message());
        for rec in &recs {
            assert_eq!(rec.get("set_ref"), Some(&Value::String("MSG001".into())));
            assert_eq!(rec.get("set_type"), Some(&Value::String("ADT^A01".into())));
        }
    }

    #[test]
    fn envelope_segments_never_emitted_as_body() {
        let batched = format!("FHS|^~\\&|SENDAPP\rBHS|^~\\&|SENDAPP\r{MSH}\rPID|1\rBTS|1\rFTS|1");
        let recs = collect(&batched);
        for rec in &recs {
            let seg = rec.get("seg_id").unwrap();
            assert!(
                !matches!(seg, Value::String(s) if matches!(&**s, "FHS" | "FTS" | "BHS" | "BTS"))
            );
        }
        // MSH and PID are the only body records.
        assert_eq!(recs.len(), 2);
    }

    #[test]
    fn absent_trailing_fields_are_null() {
        let recs = collect(&adt_message());
        // EVN has 2 fields (A01, timestamp); f03.. are null.
        assert_eq!(recs[1].get("f03"), Some(&Value::Null));
    }

    #[test]
    fn nested_events_open_message_for_bare_msh() {
        let mut r = reader(&adt_message());
        let _first = r.next_record().unwrap().unwrap(); // MSH
        let events = r.take_envelope_events();
        assert_eq!(events.len(), 1, "MSH opens one message level");
        assert!(matches!(events[0], EnvelopeEvent::OpenLevel { .. }));
    }

    #[test]
    fn nested_events_close_message_at_end() {
        let mut r = reader(&adt_message());
        while r.next_record().unwrap().is_some() {
            let _ = r.take_envelope_events();
        }
        // The terminal next_record closes the message level.
        let trailing = r.take_envelope_events();
        assert_eq!(trailing.len(), 1, "the message closes at end-of-input");
        assert!(matches!(trailing[0], EnvelopeEvent::CloseLevel));
    }

    #[test]
    fn message_section_exposes_msh_fields() {
        let mut r = reader(&adt_message());
        let _ = r.next_record().unwrap().unwrap();
        let events = r.take_envelope_events();
        let EnvelopeEvent::OpenLevel { sections } = &events[0] else {
            panic!("expected message OpenLevel");
        };
        let set = match sections.get("transaction_set").unwrap() {
            Value::Map(m) => m,
            other => panic!("expected map, got {other:?}"),
        };
        // MSH-9 (message type) is f08; MSH-10 (control id) is f09.
        assert_eq!(set.get("f08"), Some(&Value::String("ADT^A01".into())));
        assert_eq!(set.get("f09"), Some(&Value::String("MSG001".into())));
    }

    #[test]
    fn multi_message_bare_stream_closes_each_message() {
        let m2 = MSH.replace("MSG001", "MSG002");
        let data = format!("{MSH}\rPID|1\r{m2}\rPID|2");
        let mut r = reader(&data);
        // MSH(1)
        let _ = r.next_record().unwrap().unwrap();
        assert_eq!(r.take_envelope_events().len(), 1); // open msg 1
        // PID|1
        let _ = r.next_record().unwrap().unwrap();
        assert!(r.take_envelope_events().is_empty());
        // MSH(2) closes msg 1 and opens msg 2.
        let _ = r.next_record().unwrap().unwrap();
        let evs = r.take_envelope_events();
        assert_eq!(evs.len(), 2);
        assert!(matches!(evs[0], EnvelopeEvent::CloseLevel));
        assert!(matches!(evs[1], EnvelopeEvent::OpenLevel { .. }));
    }

    #[test]
    fn batch_nesting_opens_batch_then_message() {
        let data = format!("BHS|^~\\&|SENDAPP\r{MSH}\rPID|1\rBTS|1");
        let mut r = reader(&data);
        let _ = r.next_record().unwrap().unwrap(); // MSH
        let events = r.take_envelope_events();
        // BHS opens the batch, MSH opens the message.
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], EnvelopeEvent::OpenLevel { .. }));
        assert!(matches!(events[1], EnvelopeEvent::OpenLevel { .. }));
    }

    /// Drain every envelope event a reader produces across the whole stream,
    /// including the terminal end-of-input `next_record`.
    fn drain_all_events(data: &str) -> Vec<EnvelopeEvent> {
        let mut r = reader(data);
        let mut events = Vec::new();
        loop {
            match r.next_record().unwrap() {
                Some(_) => events.extend(r.take_envelope_events()),
                None => {
                    events.extend(r.take_envelope_events());
                    break;
                }
            }
        }
        events
    }

    fn count_open_close(events: &[EnvelopeEvent]) -> (usize, usize) {
        let opens = events
            .iter()
            .filter(|e| matches!(e, EnvelopeEvent::OpenLevel { .. }))
            .count();
        let closes = events
            .iter()
            .filter(|e| matches!(e, EnvelopeEvent::CloseLevel))
            .count();
        (opens, closes)
    }

    #[test]
    fn unterminated_batch_at_eof_still_balances_events() {
        // A BHS-opened batch (and its MSH message) with no BTS/FTS at EOF —
        // a common shape for HL7 batch files that drop the advisory trailers.
        // The reader must close both the message and the batch at EOF so the
        // OpenLevel/CloseLevel events stay balanced; an unbalanced batch
        // OpenLevel would leave the driver's document stack open forever.
        let data = format!("BHS|^~\\&|S\r{MSH}\rPID|1");
        let events = drain_all_events(&data);
        let (opens, closes) = count_open_close(&events);
        assert_eq!(opens, 2, "BHS + MSH each open one level");
        assert_eq!(
            closes, opens,
            "every opened level must close by EOF; events: {events:?}"
        );
    }

    #[test]
    fn unterminated_batch_with_trailer_still_balances_events() {
        // The same batch with its BTS/FTS present must balance identically.
        let data = format!("FHS|^~\\&|S\rBHS|^~\\&|S\r{MSH}\rPID|1\rBTS|1\rFTS|1");
        let events = drain_all_events(&data);
        let (opens, closes) = count_open_close(&events);
        assert_eq!(
            opens, 2,
            "BHS + MSH each open one level (FHS is file-level)"
        );
        assert_eq!(closes, opens, "events: {events:?}");
    }

    #[test]
    fn bts_message_count_mismatch_errors() {
        let data = format!("BHS|^~\\&|S\r{MSH}\rPID|1\rBTS|9");
        let mut r = reader(&data);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected count mismatch"),
                Err(e) => break e,
            }
        };
        assert!(
            matches!(err, FormatError::StructuralCount { format: "HL7", ref message } if message.contains("BTS batch message count mismatch"))
        );
    }

    #[test]
    fn fts_batch_count_mismatch_errors() {
        let data = format!("FHS|^~\\&|S\rBHS|^~\\&|S\r{MSH}\rPID|1\rBTS|1\rFTS|9");
        let mut r = reader(&data);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected file batch count mismatch"),
                Err(e) => break e,
            }
        };
        assert!(
            matches!(err, FormatError::StructuralCount { format: "HL7", ref message } if message.contains("FTS file batch count mismatch"))
        );
    }

    #[test]
    fn empty_count_disables_the_check() {
        // BTS with no count field validates clean.
        let data = format!("BHS|^~\\&|S\r{MSH}\rPID|1\rBTS");
        let recs = collect(&data);
        assert_eq!(recs.len(), 2);
    }

    #[test]
    fn content_after_fts_errors() {
        let data = format!("FHS|^~\\&|S\r{MSH}\rPID|1\rFTS|0\rPID|99");
        let mut r = reader(&data);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected after-trailer error"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Hl7(m) if m.contains("after the FTS")));
    }

    #[test]
    fn body_before_any_header_errors() {
        // A file whose first segment is an ordinary body segment (no
        // MSH/FHS/BHS header) is malformed; the tokenizer rejects it at
        // delimiter discovery.
        let data = "PID|1||X\rNTE|note";
        let mut r = reader(data);
        let err = r.next_record().unwrap_err();
        assert!(matches!(err, FormatError::Hl7(m) if m.contains("must begin with an MSH")));
    }

    #[test]
    fn max_fields_overflow_errors_with_guidance() {
        let data = format!("{MSH}\rZZZ|a|b|c|d|e");
        let mut r = Hl7Reader::new(
            Cursor::new(data.into_bytes()),
            Hl7ReaderConfig {
                max_fields: 2,
                split_fields: Vec::new(),
            },
        );
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected field overflow"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Hl7(m) if m.contains("max_fields")));
    }

    #[test]
    fn split_replaces_verbatim_column_with_components() {
        // Splitting MSH-9 (f08) into two components exposes the message code
        // and trigger event as separate columns; the verbatim `f08` is gone.
        let recs = collect_with_splits(&adt_message(), vec![split(8, 1, 2, 1)]);
        let msh = &recs[0];
        assert_eq!(msh.get("f08_c1"), Some(&Value::String("ADT".into())));
        assert_eq!(msh.get("f08_c2"), Some(&Value::String("A01".into())));
        // The whole-field column is replaced by the split columns.
        assert_eq!(msh.get("f08"), None);
        // Neighboring fields are untouched and keep their verbatim columns.
        assert_eq!(msh.get("f07"), Some(&Value::Null)); // MSH-8 empty
        assert_eq!(msh.get("f09"), Some(&Value::String("MSG001".into())));
    }

    #[test]
    fn split_applies_to_every_segment_positionally() {
        // A split on f03 explodes field 3 of *every* segment, not just MSH.
        // PID-3 here is `PATID^^^MRN`; EVN-3 is absent.
        let recs = collect_with_splits(&adt_message(), vec![split(3, 1, 4, 1)]);
        let pid = &recs[2];
        assert_eq!(pid.get("f03_c1"), Some(&Value::String("PATID".into())));
        assert_eq!(pid.get("f03_c2"), Some(&Value::Null)); // empty component
        assert_eq!(pid.get("f03_c3"), Some(&Value::Null));
        assert_eq!(pid.get("f03_c4"), Some(&Value::String("MRN".into())));
        // EVN has no field 3 — every leaf is null.
        let evn = &recs[1];
        assert_eq!(evn.get("f03_c1"), Some(&Value::Null));
        assert_eq!(evn.get("f03_c4"), Some(&Value::Null));
    }

    #[test]
    fn split_default_behavior_unchanged_without_declaration() {
        // With no split declared, the field stays verbatim — byte-identical
        // to the positional model.
        let recs = collect(&adt_message());
        assert_eq!(
            recs[2].get("f03"),
            Some(&Value::String("PATID^^^MRN".into()))
        );
    }

    #[test]
    fn split_component_overflow_errors() {
        // f08 carries two components; declaring one is an overflow.
        let mut r = reader_with_splits(&adt_message(), vec![split(8, 1, 1, 1)]);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected component overflow"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Hl7(m) if m.contains("component")));
    }

    #[test]
    fn split_on_escaped_separator_keeps_one_component() {
        // A field carrying an escaped component separator (`\S\`) is one
        // component of literal data, not two — the split must run on the raw
        // bytes before the escape decodes to a `^`.
        let data = format!("{MSH}\rOBX|1|TX|note||CODE\\S\\TEXT");
        let recs = collect_with_splits(&data, vec![split(5, 1, 2, 1)]);
        let obx = &recs[1];
        // The whole escaped value decodes into one component.
        assert_eq!(obx.get("f05_c1"), Some(&Value::String("CODE^TEXT".into())));
        assert_eq!(obx.get("f05_c2"), Some(&Value::Null));
    }

    fn fhs_section(fields: &[(&str, EnvelopeFieldType)]) -> EnvelopeConfig {
        let mut cfg = EnvelopeConfig::default();
        let mut field_map = IndexMap::new();
        for (k, ty) in fields {
            field_map.insert((*k).to_string(), *ty);
        }
        cfg.sections.insert(
            "file".to_string(),
            EnvelopeSection {
                extract: EnvelopeExtract::Segment("FHS".to_string()),
                fields: field_map,
            },
        );
        cfg
    }

    #[test]
    fn prepare_document_extracts_fhs_fields_typed() {
        let file =
            format!("FHS|^~\\&|SENDAPP|SENDFAC|RCVAPP|RCVFAC|20240101|FILE42\r{MSH}\rPID|1\rFTS|0");
        let cfg = fhs_section(&[("f07", EnvelopeFieldType::String)]);
        let mut r = reader(&file);
        let sections = r.prepare_document(&cfg).unwrap();
        let file_doc = match sections.get("file").unwrap() {
            Value::Map(m) => m,
            other => panic!("expected map, got {other:?}"),
        };
        // FHS-8 (file name / id) is f07 under the off-by-one.
        assert_eq!(file_doc.get("f07"), Some(&Value::String("FILE42".into())));
        // Body still streams after the header pre-scan.
        let recs: Vec<_> = std::iter::from_fn(|| r.next_record().unwrap()).collect();
        assert_eq!(recs.len(), 2); // MSH, PID
    }

    #[test]
    fn prepare_document_rejects_non_fhs_segment() {
        let mut cfg = EnvelopeConfig::default();
        cfg.sections.insert(
            "batch".to_string(),
            EnvelopeSection {
                extract: EnvelopeExtract::Segment("BHS".to_string()),
                fields: IndexMap::new(),
            },
        );
        let mut r = reader(&adt_message());
        let err = r.prepare_document(&cfg).unwrap_err();
        assert!(matches!(err, FormatError::Hl7(m) if m.contains("not extractable")));
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
        let mut r = reader(&adt_message());
        let err = r.prepare_document(&cfg).unwrap_err();
        assert!(matches!(err, FormatError::Hl7(m) if m.contains("non-`segment`")));
    }

    #[test]
    fn prepare_document_rejects_fhs_section_when_file_has_no_fhs() {
        // A bare MSH-led file has no FHS; declaring an FHS section is a
        // config-against-data mistake the reader surfaces.
        let cfg = fhs_section(&[("f07", EnvelopeFieldType::String)]);
        let mut r = reader(&adt_message());
        let err = r.prepare_document(&cfg).unwrap_err();
        assert!(matches!(err, FormatError::Hl7(m) if m.contains("no FHS file header")));
    }

    /// Full reader → `$doc` → writer → reader round-trip exercising the MSH
    /// header reconstruction, the message-level envelope, and escape
    /// re-encoding.
    #[test]
    fn reader_doc_writer_round_trip_preserves_msh_and_escapes() {
        use crate::hl7::writer::{Hl7Writer, Hl7WriterConfig};
        use crate::traits::FormatWriter;
        use clinker_record::{DocumentContext, DocumentId};

        // PID-3 is a real composite CX field with four components and a
        // repetition (`~`); the OBX field carries a literal '|' that arrives
        // escaped as \F\. The composite/repetition structure must round-trip
        // byte-identically (kept verbatim), while the escaped '|' decodes and
        // re-escapes.
        let input = format!("{MSH}\rPID|1||PATID^^^MRN~ALT^^^MR\rOBX|1|TX|note||a\\F\\b");

        let mut r = reader(&input);
        let body_recs: Vec<Record> = std::iter::from_fn(|| r.next_record().unwrap()).collect();
        assert_eq!(body_recs.len(), 3); // MSH, PID, OBX
        // PID-3 is kept verbatim (components and repetition intact).
        assert_eq!(
            body_recs[1].get("f03"),
            Some(&Value::String("PATID^^^MRN~ALT^^^MR".into()))
        );
        // The decoded OBX value is clean (no wire escape).
        assert_eq!(body_recs[2].get("f05"), Some(&Value::String("a|b".into())));

        // Attach a document context (no FHS section; the message-level MSH
        // is rebuilt from the record stream) and re-emit through the writer.
        let ctx = Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from("adt.hl7"),
            IndexMap::new(),
        ));
        let schema = r.schema().unwrap();
        let out = {
            let mut buf = Vec::new();
            let mut w = Hl7Writer::new(
                std::io::Cursor::new(&mut buf),
                Arc::clone(&schema),
                Hl7WriterConfig::default(),
            );
            for rec in &body_recs {
                let mut rec = rec.clone();
                rec.set_doc_ctx(Arc::clone(&ctx));
                w.write_record(&rec).unwrap();
            }
            w.flush().unwrap();
            String::from_utf8(buf).unwrap()
        };

        // The MSH header is re-emitted, the composite PID-3 is byte-identical
        // (its component '^' and repetition '~' separators are NOT escaped),
        // and the literal '|' is re-escaped.
        assert!(out.starts_with("MSH|^~\\&|"), "{out}");
        assert!(out.contains("PID|1||PATID^^^MRN~ALT^^^MR\r"), "{out}");
        assert!(out.contains("OBX|1|TX|note||a\\F\\b"), "{out}");

        // Re-read the emitted file: the composite survives and the decoded
        // value matches.
        let recs2 = collect(&out);
        assert_eq!(recs2.len(), 3);
        assert_eq!(
            recs2[1].get("f03"),
            Some(&Value::String("PATID^^^MRN~ALT^^^MR".into()))
        );
        assert_eq!(recs2[2].get("f05"), Some(&Value::String("a|b".into())));
    }

    /// A split composite field must re-assemble byte-identically on write:
    /// reader explodes the field into component columns, the writer rejoins
    /// them on the discovered separator (verbatim, never escaped), and a
    /// re-read reproduces the same components.
    #[test]
    fn split_field_reassembles_byte_identically_on_round_trip() {
        use crate::hl7::writer::{Hl7Writer, Hl7WriterConfig};
        use crate::traits::FormatWriter;

        // Split MSH-9 (f08) into two components and PID-3 (f03) into four.
        let splits = vec![split(8, 1, 2, 1), split(3, 1, 4, 1)];
        let input = format!("{MSH}\rPID|1||PATID^^^MRN");

        let body_recs = collect_with_splits(&input, splits.clone());
        assert_eq!(body_recs.len(), 2); // MSH, PID
        assert_eq!(
            body_recs[0].get("f08_c1"),
            Some(&Value::String("ADT".into()))
        );
        assert_eq!(
            body_recs[0].get("f08_c2"),
            Some(&Value::String("A01".into()))
        );
        assert_eq!(
            body_recs[1].get("f03_c1"),
            Some(&Value::String("PATID".into()))
        );
        assert_eq!(
            body_recs[1].get("f03_c4"),
            Some(&Value::String("MRN".into()))
        );

        // Re-emit: the writer re-assembles the split columns into the exact
        // wire fields, separators verbatim.
        let schema = reader_with_splits(&input, splits.clone()).schema().unwrap();
        let out = {
            let mut buf = Vec::new();
            let mut w = Hl7Writer::new(
                std::io::Cursor::new(&mut buf),
                Arc::clone(&schema),
                Hl7WriterConfig::default(),
            );
            for rec in &body_recs {
                w.write_record(rec).unwrap();
            }
            w.flush().unwrap();
            String::from_utf8(buf).unwrap()
        };
        // MSH-9 and PID-3 are reconstructed byte-identically; no `\S\` escape.
        assert!(out.contains("|ADT^A01|"), "MSH-9 reassembly wrong: {out}");
        assert!(out.contains("PID|1||PATID^^^MRN\r"), "PID-3 wrong: {out}");
        assert!(!out.contains("\\S\\"), "component separator escaped: {out}");

        // Re-read with the same splits: the components come back identical.
        let recs2 = collect_with_splits(&out, splits);
        assert_eq!(recs2[1].get("f03_c4"), Some(&Value::String("MRN".into())));
        assert_eq!(recs2[0].get("f08_c2"), Some(&Value::String("A01".into())));
    }
}
