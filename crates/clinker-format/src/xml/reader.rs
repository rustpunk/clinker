//! Streaming XML reader using quick-xml's pull parser.
//!
//! Navigates to `record_path`, extracts attributes with configurable prefix,
//! flattens nested elements with `.` separator, handles namespaces, and
//! applies the source's multi-value declarations (`split_to_rows` fan-out,
//! `split_values` in-cell parsing, and schema-level `multiple:` collection) to
//! repeated child elements.
//!
//! **O(1 record) memory, no whole-document buffer:** the body walks the
//! document element-at-a-time from a freshly re-opened `BufReader` —
//! quick-xml's `read_event_into` pulls one event at a time, so only a single
//! record's `element_stack` plus the event buffer is live at once, never the
//! whole input. A `split_to_rows` fan-out expands one record element into
//! several records; the expansion queue is bounded by that one element's
//! fan-out, never the whole input.
//!
//! Envelope-aware sources run a streaming pre-scan before any body record
//! emits: it walks the document once over its *own* freshly re-opened reader,
//! flattening ONLY the subtrees the declared `$doc.*` paths name (every other
//! element's body is event-walked and dropped, never allocated) into a
//! path-pruned index capped by `max_index_bytes`, charged incrementally so an
//! oversized declared section aborts mid-parse before its subtree fully
//! materializes. The pre-scan and the body each open their own [`Read`] from
//! the [`ReopenableSource`], so neither consumes the other and no shared
//! whole-file byte buffer is retained for a file-backed input. See
//! [`crate::xml::streaming`] for the event-driven pruned-extraction pass.

use std::io::{BufRead, BufReader, Read};
use std::ops::Range;
use std::sync::Arc;

use indexmap::IndexMap;
use quick_xml::Reader as XmlParser;
use quick_xml::escape;
use quick_xml::events::{BytesRef, Event};

use clinker_record::{Record, Schema, SchemaBuilder, Value};

use cxl::analyzer::doc_paths::DocPath;

use crate::bom::UTF8_BOM;
use crate::doc_index::DocArenaIndex;
use crate::envelope::{EnvelopeConfig, EnvelopeExtract, coerce_section_fields};
use crate::error::{FanOutLimitFailure, FormatError};
use crate::multi_value::{SplitToRows, SplitToRowsMode, SplitValues, split_text_value};
use crate::numeric_observation::{
    NumericObservation, NumericObserver, NumericParserOutcome, observe_xml_scalar,
};
use crate::record_path::{RecordPath, RecordPathSyntax};
use crate::source::{ReopenableSource, SourceIdentity};
use crate::traits::FormatReader;
use crate::xml::streaming::{SectionTarget, extract_sections};

/// XML reader configuration.
pub struct XmlReaderConfig {
    pub record_path: Option<String>,
    pub attribute_prefix: String,
    pub namespace_handling: NamespaceMode,
    /// Fields the source schema declares `multiple: true`, by physical name.
    /// Every occurrence of such a field collects into one `Value::Array` in
    /// document order; a single occurrence yields a one-element array.
    pub multi_value_fields: Vec<String>,
    /// Fan-out declarations, applied in declaration order — so two entries
    /// multiply, exactly as two nested loops would.
    pub split_to_rows: Vec<SplitToRows>,
    /// Maximum rows one input element may emit through `split_to_rows`.
    /// Zero disables the ceiling.
    pub max_output_rows_per_input: u64,
    /// In-cell parse declarations: a field's text is split on its delimiter
    /// into the several values a `multiple: true` column holds.
    pub split_values: Vec<SplitValues>,
    /// `$doc.*` envelope paths a program downstream of this source
    /// references, attributed to this source by the planner. The envelope
    /// pre-scan retains only the sections these paths name; a declared
    /// section no program reads is skipped, never materialized. Empty when
    /// no downstream program reads any `$doc` path.
    pub declared_doc_paths: Vec<DocPath>,
    /// Hard cap on the bytes the envelope pre-scan's path-pruned index may
    /// retain. The cap is charged incrementally as each section's payload is
    /// built and fires mid-parse (before OOM). `None` disables the cap; the
    /// source plumbing supplies a finite default.
    pub max_index_bytes: Option<usize>,
}

impl Default for XmlReaderConfig {
    fn default() -> Self {
        Self {
            record_path: None,
            attribute_prefix: "@".into(),
            namespace_handling: NamespaceMode::Strip,
            multi_value_fields: Vec::new(),
            split_to_rows: Vec::new(),
            max_output_rows_per_input: 0,
            split_values: Vec::new(),
            declared_doc_paths: Vec::new(),
            max_index_bytes: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum NamespaceMode {
    Strip,
    Qualify,
}

/// One record element's raw extraction: flattened `(key, value)` pairs in
/// document order with repeated keys intact, plus the field ranges covered
/// by each declared fan-out field's element occurrences.
#[derive(Clone)]
struct RawRecord {
    fields: Vec<(String, String)>,
    /// Index-aligned with `XmlReaderConfig::split_to_rows`: for each declared
    /// field, the half-open ranges into `fields` spanning each occurrence of
    /// that element, in document order. An occurrence with no extracted
    /// fields (`<Item></Item>`) contributes an empty range, so the fan-out
    /// still emits a record for it.
    split_instances: Vec<Vec<Range<usize>>>,
    /// Dotted names of value-less, non-fan-out self-closing elements (`<x/>`)
    /// that pushed no field of their own. These are recorded only so schema
    /// inference can see the column — record assembly never reads them, which
    /// is what keeps a repeated `<x/><x/>` on a non-`multiple:` column from
    /// tripping the undeclared-repeat guard while a column appearing ONLY as
    /// `<x/>` in the first record still surfaces in the inferred schema.
    presence: Vec<String>,
}

impl RawRecord {
    /// A raw record with no fan-out occurrences — an attributes-only record
    /// element has no child elements for a declared field to match.
    fn without_instances(fields: Vec<(String, String)>, field_count: usize) -> Self {
        RawRecord {
            fields,
            split_instances: vec![Vec::new(); field_count],
            presence: Vec::new(),
        }
    }
}

/// Preserve one raw XML record for a structured source rejection without
/// collapsing repeated field names. Each array item records one flattened
/// field occurrence in document order.
fn raw_xml_record_value(raw: &RawRecord) -> Value {
    Value::Array(
        raw.fields
            .iter()
            .map(|(field, value)| {
                let mut occurrence = IndexMap::with_capacity(2);
                occurrence.insert("field".into(), Value::String(field.clone().into()));
                occurrence.insert("value".into(), Value::String(value.clone().into()));
                Value::Map(Box::new(occurrence))
            })
            .collect(),
    )
}

/// A flattened field tagged with its index in the original extraction, so
/// occurrence ranges (recorded against that original order) stay meaningful
/// across the sequential per-field fan-out.
///
/// [`SYNTHETIC_FIELD_INDEX`] marks a field the fan-out itself produced (a
/// `position_column`), which belongs to no occurrence range.
type IndexedField = (usize, String, String);

/// Original-extraction index for a field the fan-out synthesized rather than
/// read. `usize::MAX` sits past every real range, so a later fan-out entry
/// treats it as a trailing field and carries it through untouched.
const SYNTHETIC_FIELD_INDEX: usize = usize::MAX;

/// One declaration level in a lazy XML fan-out.
///
/// Fields are partitioned once into the record head/tail and occurrence
/// buckets. Selecting another occurrence clones one output row, not the other
/// rows in the cartesian product.
struct XmlExpansionFrame {
    head: Vec<IndexedField>,
    tail: Vec<IndexedField>,
    occurrences: Vec<Vec<IndexedField>>,
    selected: usize,
    entry: SplitToRows,
}

impl XmlExpansionFrame {
    fn new(
        record: Vec<IndexedField>,
        entry: SplitToRows,
        instances: &[Range<usize>],
    ) -> Option<Self> {
        if instances.is_empty() {
            return entry.keep_empty.then_some(Self {
                head: record,
                tail: Vec::new(),
                occurrences: Vec::new(),
                selected: 0,
                entry,
            });
        }

        let anchor = instances[0].start;
        let span = instances.last().map_or(0, |range| range.end);
        let mut owner = vec![None; span];
        for (position, range) in instances.iter().enumerate() {
            for slot in &mut owner[range.clone()] {
                *slot = Some(position);
            }
        }

        let mut occurrences = vec![Vec::new(); instances.len()];
        let mut head = Vec::new();
        let mut tail = Vec::new();
        for field in record {
            match owner.get(field.0).copied().flatten() {
                Some(position) => occurrences[position].push((
                    field.0,
                    projected_key(&field.1, &entry.field, entry.mode),
                    field.2,
                )),
                None if field.0 < anchor => head.push(field),
                None => tail.push(field),
            }
        }
        Some(Self {
            head,
            tail,
            occurrences,
            selected: 0,
            entry,
        })
    }

    fn render(&self) -> Vec<IndexedField> {
        let Some(selected) = self.occurrences.get(self.selected) else {
            let mut record = self.head.clone();
            record.extend(self.tail.iter().cloned());
            return record;
        };
        let mut occurrence = selected.clone();
        if let Some(column) = &self.entry.position_column {
            occurrence.retain(|field| field.1 != *column);
            occurrence.push((
                SYNTHETIC_FIELD_INDEX,
                column.clone(),
                (self.selected + 1).to_string(),
            ));
        }

        let mut shadowed: std::collections::HashSet<String> =
            std::collections::HashSet::with_capacity(occurrence.len());
        if self.entry.mode == SplitToRowsMode::Extract {
            shadowed.extend(occurrence.iter().map(|field| field.1.clone()));
        }
        if let Some(column) = &self.entry.position_column {
            shadowed.insert(column.clone());
        }
        let mut record = self
            .head
            .iter()
            .filter(|field| !shadowed.contains(field.1.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        record.extend(occurrence);
        record.extend(
            self.tail
                .iter()
                .filter(|field| !shadowed.contains(field.1.as_str()))
                .cloned(),
        );
        record
    }

    fn advance(&mut self) -> bool {
        let selections = self.occurrences.len().max(1);
        if self.selected + 1 >= selections {
            return false;
        }
        self.selected += 1;
        true
    }
}

/// Suspended mixed-radix expansion of one XML record element.
struct XmlExpansionCursor {
    entries: Vec<SplitToRows>,
    instances: Vec<Vec<Range<usize>>>,
    frames: Vec<XmlExpansionFrame>,
    current: Option<Vec<IndexedField>>,
    max_output_rows: u64,
    emitted: u64,
    failure: Option<FanOutLimitFailure>,
}

impl XmlExpansionCursor {
    fn new(
        raw: RawRecord,
        entries: &[SplitToRows],
        max_output_rows: u64,
        original_record: Option<Value>,
    ) -> Self {
        let RawRecord {
            fields,
            split_instances,
            ..
        } = raw;
        let record = fields
            .into_iter()
            .enumerate()
            .map(|(index, (key, value))| (index, key, value))
            .collect();
        let mut cursor = Self {
            entries: entries.to_vec(),
            instances: split_instances,
            frames: Vec::with_capacity(entries.len()),
            current: None,
            max_output_rows,
            emitted: 0,
            failure: original_record.map(|original_record| FanOutLimitFailure {
                field: entries
                    .last()
                    .map_or_else(String::new, |entry| entry.field.clone()),
                limit: max_output_rows,
                actual: u128::from(max_output_rows) + 1,
                original_record,
            }),
        };
        cursor.descend(record, 0);
        cursor
    }

    fn descend(&mut self, mut record: Vec<IndexedField>, mut depth: usize) -> bool {
        while depth < self.entries.len() {
            let Some(frame) =
                XmlExpansionFrame::new(record, self.entries[depth].clone(), &self.instances[depth])
            else {
                return false;
            };
            record = frame.render();
            self.frames.push(frame);
            depth += 1;
        }
        self.current = Some(record);
        true
    }

    fn advance(&mut self) {
        let mut depth = self.frames.len();
        while depth > 0 {
            depth -= 1;
            if self.frames[depth].advance() {
                self.frames.truncate(depth + 1);
                let record = self.frames[depth].render();
                if self.descend(record, depth + 1) {
                    return;
                }
                depth = self.frames.len();
            } else {
                self.frames.truncate(depth);
            }
        }
        self.current = None;
    }
}

impl Iterator for XmlExpansionCursor {
    type Item = Result<Vec<(String, String)>, FormatError>;

    fn next(&mut self) -> Option<Self::Item> {
        let output = self.current.take()?;
        if self.max_output_rows != 0 && self.emitted >= self.max_output_rows {
            self.frames.clear();
            return Some(Err(FormatError::FanOutLimit(Box::new(
                self.failure
                    .take()
                    .expect("a finite fan-out limit carries its original record"),
            ))));
        }
        self.emitted += 1;
        self.advance();
        Some(Ok(output
            .into_iter()
            .map(|(_, key, value)| (key, value))
            .collect()))
    }
}

/// A fan-out element currently being extracted. Declared fields never nest
/// (rejected at plan time, E358), so at most one occurrence is open at a time.
struct OpenInstance {
    /// Index into `XmlReaderConfig::split_to_rows`.
    path: usize,
    /// First field index belonging to this occurrence.
    fields_from: usize,
    /// `element_stack` length while this occurrence's element is open; the
    /// occurrence closes when the stack shrinks below it.
    stack_len: usize,
}

/// A quick-xml pull parser over a re-opened, BOM-stripped `BufReader`.
///
/// Both the body parser and the envelope pre-scan parse over this same reader
/// shape — a fresh `Read` from the [`ReopenableSource`], never a whole-document
/// byte buffer.
pub(crate) type BodyParser = XmlParser<BufReader<Box<dyn Read + Send>>>;

/// Streaming XML reader.
///
/// Walks the body element-at-a-time from a freshly re-opened `BufReader`, so
/// only one record's `element_stack` plus the event buffer is live at once —
/// never a whole-document byte buffer. The envelope pre-scan and the body
/// iteration each open their own [`Read`] from `source`, so a post-body
/// section (extracted before the first record emits) is available without
/// retaining the input: a path-backed source is read twice, never buffered.
///
/// The envelope pre-scan retains only the declared sections' subtrees, each
/// bounded by `max_index_bytes` (charged incrementally, aborting mid-parse on
/// an oversized section), so held memory is O(declared sections) plus one
/// live record's array-path expansion while the reader exists.
pub struct XmlReader {
    /// The re-openable byte source. Body iteration and the envelope pre-scan
    /// each open their own fresh [`Read`] from it, so no whole-document buffer
    /// is held for a file-backed (path) source.
    source: ReopenableSource,
    /// Content identity of the bytes the body open read, captured at
    /// construction. The envelope pre-scan re-opens the source and confirms it
    /// sees the same content, so a path-backed input rewritten between the two
    /// passes fails loud instead of splicing a stale envelope onto a new body.
    body_identity: SourceIdentity,
    parser: BodyParser,
    config: XmlReaderConfig,
    schema: Option<Arc<Schema>>,
    buf: Vec<u8>,
    /// Path segments from record_path, e.g., ["Orders", "Order"].
    path_segments: Vec<String>,
    /// How many segments we've matched so far during descent.
    matched_depth: usize,
    /// Current XML depth (incremented on Start, decremented on End).
    xml_depth: usize,
    /// Suspended expansion of the current input element. Schema inference
    /// reads one output eagerly, while later outputs are generated one at a
    /// time so the cartesian product is never retained.
    pending: Option<XmlExpansionCursor>,
    /// First raw output used for schema inference. Assembly remains deferred
    /// to `next_record` so record-local structural errors reach the DLQ path.
    deferred_first: Option<RawRecord>,
    /// Whether we've finished all records.
    done: bool,
    /// Optional authoring-only sink receiving one bounded scalar observation
    /// before XML inference becomes a record [`Value`].
    numeric_observer: Option<NumericObserver>,
}

impl XmlReader {
    /// Build a reader over a re-openable byte source.
    ///
    /// Streaming, O(1 record): the body opens one fresh [`Read`] from `source`
    /// (and the envelope pre-scan opens a second), so a file-backed source is
    /// never buffered whole.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError`] if the source cannot be opened or the leading
    /// BOM probe fails. Construction reads no further: quick-xml pulls events
    /// lazily, so a parse error surfaces later from `next_record`.
    pub fn from_source(
        source: ReopenableSource,
        config: XmlReaderConfig,
    ) -> Result<Self, FormatError> {
        Self::from_source_with_observer(source, config, None)
    }

    /// Build a streaming reader that publishes parser-owned scalar numeric
    /// evidence before record-value conversion.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`from_source`](Self::from_source).
    pub fn from_source_observing(
        source: ReopenableSource,
        config: XmlReaderConfig,
        observer: NumericObserver,
    ) -> Result<Self, FormatError> {
        Self::from_source_with_observer(source, config, Some(observer))
    }

    fn from_source_with_observer(
        source: ReopenableSource,
        config: XmlReaderConfig,
        numeric_observer: Option<NumericObserver>,
    ) -> Result<Self, FormatError> {
        // XML runs two passes (envelope pre-scan + body stream), so the source
        // must be re-openable. A `Path`/`Buffered` source passes through; a
        // pathless `OneShot` is buffered here, on the reader-building thread —
        // bounded because such inputs are small.
        let source = source.into_reopenable().map_err(FormatError::Io)?;
        let (parser, body_identity) = Self::open_body(&source)?;

        let path_segments = match config.record_path.as_deref() {
            Some(raw) => RecordPath::parse(RecordPathSyntax::Xml, raw)
                .map_err(|e| FormatError::Xml(e.to_string()))?
                .into_segments(),
            None => Vec::new(),
        };

        Ok(XmlReader {
            source,
            body_identity,
            parser,
            config,
            schema: None,
            buf: Vec::new(),
            path_segments,
            matched_depth: 0,
            xml_depth: 0,
            pending: None,
            deferred_first: None,
            done: false,
            numeric_observer,
        })
    }

    /// Build a reader by buffering a one-shot `Read` into a re-openable source.
    ///
    /// For pathless inputs (test cursors, the `<inline>`/`<empty>` slots, REST
    /// bodies) that have no on-disk path to re-open: the bytes are captured
    /// once into a small buffered `ReopenableSource`. Bounded because such
    /// inputs are small by construction; file-backed sources use
    /// [`from_source`](Self::from_source) with a path-backed `ReopenableSource` instead
    /// and are never buffered whole.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError`] on a read failure or the same open errors as
    /// [`from_source`](Self::from_source).
    pub fn from_reader<R: Read + Send + 'static>(
        reader: R,
        config: XmlReaderConfig,
    ) -> Result<Self, FormatError> {
        let source = ReopenableSource::buffer(reader).map_err(FormatError::Io)?;
        Self::from_source(source, config)
    }

    /// Buffer a pathless XML source and publish numeric observations while it
    /// streams.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`from_reader`](Self::from_reader).
    pub fn from_reader_observing<R: Read + Send + 'static>(
        reader: R,
        config: XmlReaderConfig,
        observer: NumericObserver,
    ) -> Result<Self, FormatError> {
        let source = ReopenableSource::buffer(reader).map_err(FormatError::Io)?;
        Self::from_source_observing(source, config, observer)
    }

    /// Open a fresh `BufReader` from the source with a leading UTF-8 BOM
    /// stripped, returning the content-identity snapshot of the bytes it reads.
    /// Each pass (body, pre-scan) re-opens, so the strip happens per open
    /// rather than once over a shared buffer; the identity lets a later pass
    /// detect the input changing between passes.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Io`] if the source cannot be opened or the BOM
    /// probe read fails.
    fn open_buf(
        source: &ReopenableSource,
    ) -> Result<(BufReader<Box<dyn Read + Send>>, SourceIdentity), FormatError> {
        let (reader, identity) = source.open_with_identity().map_err(FormatError::Io)?;
        let mut buf = BufReader::new(reader);
        strip_leading_bom(&mut buf)?;
        Ok((buf, identity))
    }

    /// Open the body parser over a fresh `BufReader` and snapshot the identity
    /// of the bytes it read, so the envelope pre-scan can confirm it re-opens
    /// the same content.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Io`] if the source cannot be opened.
    fn open_body(source: &ReopenableSource) -> Result<(BodyParser, SourceIdentity), FormatError> {
        let (buf, identity) = Self::open_buf(source)?;
        let parser = XmlParser::from_reader(buf);
        // Text-node whitespace is trimmed when a run is finalized
        // ([`finalize_text_run`]), not per parser event: quick-xml splits a
        // text node into `Text` + `GeneralRef` fragments, and per-fragment
        // trimming would eat whitespace adjacent to a reference. Trimming the
        // reassembled raw node instead keeps reference-produced whitespace.
        Ok((parser, identity))
    }

    /// Navigate to the record_path and read one complete record element.
    /// Returns None when no more records exist.
    fn read_next_record_raw(&mut self) -> Result<Option<RawRecord>, FormatError> {
        if self.done {
            return Ok(None);
        }

        loop {
            self.buf.clear();
            let event = self
                .parser
                .read_event_into(&mut self.buf)
                .map_err(|e| FormatError::Xml(e.to_string()))?;

            match event {
                Event::Start(ref e) => {
                    let name = elem_name_static(&self.config.namespace_handling, &e.name());
                    self.xml_depth += 1;

                    if self.matched_depth < self.path_segments.len() {
                        if name == self.path_segments[self.matched_depth] {
                            self.matched_depth += 1;
                            if self.matched_depth == self.path_segments.len() {
                                let attrs =
                                    extract_attributes_static(&self.config.attribute_prefix, e)?;
                                let raw = self.extract_record_fields(&name, attrs)?;
                                return Ok(Some(raw));
                            }
                        } else {
                            self.skip_subtree(&name)?;
                        }
                    } else {
                        let attrs = extract_attributes_static(&self.config.attribute_prefix, e)?;
                        let raw = self.extract_record_fields(&name, attrs)?;
                        return Ok(Some(raw));
                    }
                }
                Event::Empty(ref e) => {
                    let name = elem_name_static(&self.config.namespace_handling, &e.name());

                    if self.matched_depth < self.path_segments.len() {
                        if name == self.path_segments[self.matched_depth]
                            && self.matched_depth == self.path_segments.len() - 1
                        {
                            let fields =
                                extract_attributes_static(&self.config.attribute_prefix, e)?;
                            return Ok(Some(RawRecord::without_instances(
                                fields,
                                self.config.split_to_rows.len(),
                            )));
                        }
                    } else {
                        let fields = extract_attributes_static(&self.config.attribute_prefix, e)?;
                        return Ok(Some(RawRecord::without_instances(
                            fields,
                            self.config.split_to_rows.len(),
                        )));
                    }
                }
                Event::End(_) => {
                    self.xml_depth -= 1;
                    if self.matched_depth > 0 && self.xml_depth < self.matched_depth {
                        self.matched_depth -= 1;
                    }
                }
                Event::Eof => {
                    // A clean end leaves every element closed (`xml_depth == 0`).
                    // A non-zero depth means the input was cut off inside the
                    // record container or one of its ancestors — a truncated
                    // document, not an exhausted record set — so fail loud
                    // rather than reporting a silent end-of-records.
                    if self.xml_depth > 0 {
                        return Err(FormatError::Xml(format!(
                            "unexpected end of XML document: {} element(s) were \
                             still open when the input ended (missing closing tag)",
                            self.xml_depth
                        )));
                    }
                    self.done = true;
                    return Ok(None);
                }
                Event::Text(_)
                | Event::GeneralRef(_)
                | Event::CData(_)
                | Event::Comment(_)
                | Event::Decl(_)
                | Event::PI(_)
                | Event::DocType(_) => {
                    // Skip non-element events at navigation level
                }
            }
        }
    }

    /// Extract all fields from a record element (attributes + nested children),
    /// tracking the field ranges each declared fan-out field's element
    /// occurrences cover. Uses a separate buffer to avoid borrow conflicts
    /// with self.parser.
    fn extract_record_fields(
        &mut self,
        record_name: &str,
        start_attrs: Vec<(String, String)>,
    ) -> Result<RawRecord, FormatError> {
        let mut fields = start_attrs;
        let mut split_instances: Vec<Vec<Range<usize>>> =
            vec![Vec::new(); self.config.split_to_rows.len()];
        let mut presence: Vec<String> = Vec::new();
        let mut open_instance: Option<OpenInstance> = None;
        let record_depth = self.xml_depth;
        let mut element_stack: Vec<String> = Vec::new();
        // Parallel to `element_stack`: whether each open element has yet
        // contributed a child element or a non-empty text node. An element that
        // closes with this still `false` is a value-less LEAF (`<x></x>` /
        // `<x/>`, attributes aside), distinct from a branch like `<Tags>` whose
        // only content is child elements.
        let mut element_content: Vec<bool> = Vec::new();
        // Raw source form of the current text node, accumulated across the
        // `Text` + `GeneralRef` fragment run quick-xml splits a text node into.
        // Flushed — trimmed and reference-resolved — at the next structural
        // event, yielding one field per text node exactly as a single `Text`
        // event did before the 0.41 reference split.
        let mut text_run = String::new();
        let mut buf2 = Vec::new();

        loop {
            buf2.clear();
            let event = self
                .parser
                .read_event_into(&mut buf2)
                .map_err(|e| FormatError::Xml(e.to_string()))?;

            // Any event other than a text fragment terminates the current text
            // node; resolve and push it before handling the structural event.
            if !matches!(&event, Event::Text(_) | Event::GeneralRef(_)) {
                flush_text_field(
                    &mut fields,
                    &element_stack,
                    &mut text_run,
                    &mut element_content,
                )?;
            }

            match event {
                Event::Start(ref e) => {
                    self.xml_depth += 1;
                    let name = elem_name_static(&self.config.namespace_handling, &e.name());
                    // This element is a child of whatever is currently open, so
                    // its parent is a branch, not a value-less leaf.
                    if let Some(has_content) = element_content.last_mut() {
                        *has_content = true;
                    }
                    element_stack.push(name);
                    element_content.push(false);
                    let prefix = element_stack.join(".");
                    // Opening an element named by a declared fan-out field
                    // starts a new occurrence; the attributes pushed below are
                    // its first fields. Fields never nest, so one open slot
                    // suffices.
                    if open_instance.is_none()
                        && let Some(pi) = self.split_field_index(&prefix)
                    {
                        open_instance = Some(OpenInstance {
                            path: pi,
                            fields_from: fields.len(),
                            stack_len: element_stack.len(),
                        });
                    }
                    let child_attrs = extract_attributes_static(&self.config.attribute_prefix, e)?;
                    for (key, val) in child_attrs {
                        fields.push((format!("{prefix}.{key}"), val));
                    }
                }
                Event::End(_) => {
                    self.xml_depth -= 1;
                    if self.xml_depth < record_depth {
                        break;
                    }
                    let had_content = element_content.pop().unwrap_or(true);
                    // The element being closed is the innermost open one; capture
                    // its column before popping the stack.
                    let closed_prefix = element_stack.join(".");
                    element_stack.pop();
                    // An empty-body leaf (`<x></x>`, attributes aside) contributed
                    // no child and no text: value-less for its own column, handled
                    // exactly like the self-closing `<x/>` form — collected as a
                    // null occurrence on a `multiple:` column, recorded as schema
                    // presence otherwise. A branch (`<Tags>` with children) has
                    // content, so it is never mistaken for a leaf.
                    if !had_content && !closed_prefix.is_empty() {
                        self.record_value_less_occurrence(
                            closed_prefix,
                            &mut fields,
                            &mut presence,
                        );
                    }
                    if let Some(ref open) = open_instance
                        && element_stack.len() < open.stack_len
                    {
                        split_instances[open.path].push(open.fields_from..fields.len());
                        open_instance = None;
                    }
                }
                Event::Empty(ref e) => {
                    // A self-closing element is a child of whatever is open, so
                    // its parent is a branch, not a value-less leaf.
                    if let Some(has_content) = element_content.last_mut() {
                        *has_content = true;
                    }
                    let name = elem_name_static(&self.config.namespace_handling, &e.name());
                    let prefix = if element_stack.is_empty() {
                        name.clone()
                    } else {
                        format!("{}.{name}", element_stack.join("."))
                    };
                    let instance_from = fields.len();
                    // A self-closing element carries no text for its own column,
                    // so it is value-less for that column whether or not it has
                    // attributes. Its attributes (which key to `prefix.@attr`,
                    // distinct columns) go into `fields`; its own value-less
                    // occurrence is handled by `record_value_less_occurrence`
                    // below, keyed on the element's column — NOT on whether the
                    // element pushed any field at all, so an attribute push does
                    // not mask the empty occurrence.
                    let child_attrs = extract_attributes_static(&self.config.attribute_prefix, e)?;
                    for (key, val) in child_attrs {
                        fields.push((format!("{prefix}.{key}"), val));
                    }
                    // A self-closing element named by a declared fan-out field
                    // is a complete occurrence on its own; its range spans the
                    // attributes pushed above, or is empty when it has none —
                    // the same empty range the empty-body form contributes.
                    if open_instance.is_none()
                        && let Some(pi) = self.split_field_index(&prefix)
                    {
                        split_instances[pi].push(instance_from..fields.len());
                    }
                    // Collect the empty occurrence (`multiple:` column) or record
                    // schema presence (any other non-fan-out column), exactly as
                    // the empty-body `<x></x>` form does at its `End`.
                    self.record_value_less_occurrence(prefix, &mut fields, &mut presence);
                }
                Event::Text(ref t) => {
                    text_run.push_str(&t.decode().map_err(|e| FormatError::Xml(e.to_string()))?);
                }
                Event::GeneralRef(ref r) => {
                    append_general_ref(&mut text_run, r)?;
                }
                Event::CData(ref cd) => {
                    let text = String::from_utf8_lossy(cd.as_ref()).into_owned();
                    if !text.is_empty() {
                        let field_name = element_stack.join(".");
                        if !field_name.is_empty() {
                            fields.push((field_name, text));
                            // CDATA text is content, so its element is not a
                            // value-less leaf.
                            if let Some(has_content) = element_content.last_mut() {
                                *has_content = true;
                            }
                        }
                    }
                }
                Event::Eof => {
                    // The record element's own `End` breaks the loop above; an
                    // EOF here means the input was cut off before that close,
                    // leaving the record (or an open child) truncated. Name the
                    // deepest open element so the failure points at the cut.
                    let open_path = if element_stack.is_empty() {
                        record_name.to_string()
                    } else {
                        format!("{record_name}.{}", element_stack.join("."))
                    };
                    return Err(FormatError::Xml(format!(
                        "unexpected end of XML document inside element {open_path:?}: \
                         the input ended before its closing tag"
                    )));
                }
                _ => {}
            }
        }

        Ok(RawRecord {
            fields,
            split_instances,
            presence,
        })
    }

    /// Index of the declared fan-out field exactly matching this dotted
    /// element path, if any.
    fn split_field_index(&self, dotted: &str) -> Option<usize> {
        self.config
            .split_to_rows
            .iter()
            .position(|e| e.field == dotted)
    }

    /// Start a lazy expansion over the declared `split_to_rows` fields.
    ///
    /// A record with no occurrence of a field — an empty repetition or an
    /// absent element, which XML cannot distinguish — passes through unchanged
    /// under the default `keep_empty: true`, and is dropped when the author
    /// opts out. Memory is bounded by the parent element, declaration-depth
    /// cursor tables, and one output row rather than the occurrence product.
    fn apply_split_to_rows(
        &self,
        raw: RawRecord,
        original_record: Option<Value>,
    ) -> XmlExpansionCursor {
        let limit = original_record
            .as_ref()
            .map_or(0, |_| self.config.max_output_rows_per_input);
        XmlExpansionCursor::new(raw, &self.config.split_to_rows, limit, original_record)
    }

    /// Skip an entire subtree (from current Start to its matching End).
    ///
    /// `element_name` is the subtree's root element, used only to name the
    /// failure. Returns [`FormatError::Xml`] if the input ends before the
    /// subtree closes — a truncated document must fail loud rather than
    /// silently swallow an unfinished, skipped-over element.
    fn skip_subtree(&mut self, element_name: &str) -> Result<(), FormatError> {
        let target_depth = self.xml_depth;
        loop {
            self.buf.clear();
            let event = self
                .parser
                .read_event_into(&mut self.buf)
                .map_err(|e| FormatError::Xml(e.to_string()))?;
            match event {
                Event::Start(_) => self.xml_depth += 1,
                Event::End(_) => {
                    self.xml_depth -= 1;
                    if self.xml_depth < target_depth {
                        return Ok(());
                    }
                }
                Event::Eof => {
                    return Err(FormatError::Xml(format!(
                        "unexpected end of XML document while skipping element \
                         {element_name:?}: the input ended before its closing tag"
                    )));
                }
                _ => {}
            }
        }
    }

    /// Converts raw field pairs to a Record carrying the element's
    /// actual key set (per-record schema). Each emitted record's
    /// `Arc<Schema>` reflects exactly the keys present in that XML
    /// element — the per-Source `OnUnmapped` policy at the dispatch
    /// layer reconciles records against the user-declared schema.
    ///
    /// A child element that repeats is collected into a `Value::Array` only
    /// when its column is declared `multiple: true`; a repeat on any other
    /// column returns [`FormatError::UndeclaredRepeatedField`] rather than
    /// silently keeping the first occurrence and dropping the rest.
    fn fields_to_record(&self, fields: Vec<(String, String)>) -> Result<Record, FormatError> {
        // Keyed by the boxed column name so the slot map costs exactly one
        // clone per distinct field, the same as the `HashSet` first-wins dedup
        // it replaced: this runs once per field per record on every XML
        // pipeline, including the majority that declare no multi-value column.
        let mut slot: std::collections::HashMap<Box<str>, usize> =
            std::collections::HashMap::with_capacity(fields.len());
        let mut columns: Vec<Box<str>> = Vec::with_capacity(fields.len());
        let mut values: Vec<Value> = Vec::with_capacity(fields.len());
        for (key, val) in fields {
            let observation = observe_xml_scalar(&val);
            let value = inferred_xml_value(&val, &observation);
            if let Some(observer) = &self.numeric_observer {
                observer.observe(&key, observation);
            }
            match slot.get(key.as_str()) {
                Some(&i) => match &mut values[i] {
                    // A repeated key on a `multiple:` column accumulates in
                    // document order.
                    Value::Array(items) => items.push(value),
                    // A repeated key on any other column would keep the first
                    // value and silently drop this one. Refuse loudly instead:
                    // an undeclared repeat is a data-loss hazard, not a
                    // first-wins convenience.
                    _ => {
                        return Err(FormatError::UndeclaredRepeatedField {
                            format: "XML",
                            field: key,
                        });
                    }
                },
                None => {
                    let multiple = self.is_multi_value(&key);
                    let name = key.into_boxed_str();
                    slot.insert(name.clone(), columns.len());
                    columns.push(name);
                    values.push(if multiple {
                        Value::Array(vec![value])
                    } else {
                        value
                    });
                }
            }
        }
        for entry in &self.config.split_values {
            if let Some(&i) = slot.get(entry.field.as_str()) {
                values[i] = split_text_value(&values[i], &entry.delimiter);
            }
        }
        let schema = Arc::new(Schema::new(columns));
        Ok(Record::new(schema, values))
    }

    /// Whether the source schema declares this flattened field `multiple: true`.
    fn is_multi_value(&self, key: &str) -> bool {
        self.config.multi_value_fields.iter().any(|f| f == key)
    }

    /// Record a value-less occurrence of the column named `name` — an empty-body
    /// `<x></x>` or a self-closing `<x/>`, whether or not it carries attributes.
    ///
    /// The two empty forms behave identically: on a declared `multiple:` column
    /// the occurrence is a real (null) array element, pushed into `fields` so it
    /// keeps its array position and projects with every other field; on any other
    /// column it pushes NO field — which keeps a repeated `<x/><x/>` (or
    /// `<x></x><x></x>`) on a non-`multiple:` column from tripping the
    /// undeclared-repeat guard — but records the column's projected presence so a
    /// column appearing ONLY as an empty element in the first record still
    /// surfaces in the inferred schema. A declared fan-out field is handled by
    /// its occurrence range, not here.
    ///
    /// The `multiple:` decision is taken on the PROJECTED name, not the raw dotted
    /// `name`. `multi_value_fields` holds the projected physical name (`tag`),
    /// while inside a `split_to_rows` group `name` is still the raw path
    /// (`Item.tag`); classifying on the raw name would miss the declaration and
    /// silently drop the null occurrence, whereas the valued sibling — which runs
    /// through `fields_to_record` AFTER fan-out projection — collects it. Project
    /// first so both paths agree and the empty middle occurrence keeps its array
    /// slot (`<tag>a</tag><tag/><tag>b</tag>` → `[a, null, b]`). The null field is
    /// pushed under its RAW `name` so it fans out and projects exactly like every
    /// valued sibling.
    fn record_value_less_occurrence(
        &self,
        name: String,
        fields: &mut Vec<(String, String)>,
        presence: &mut Vec<String>,
    ) {
        let projected = self.project_presence_name(&name);
        if self.is_multi_value(&projected) {
            fields.push((name, String::new()));
        } else if self.split_field_index(&name).is_none() && !presence.contains(&projected) {
            presence.push(projected);
        }
    }

    /// Project a value-less column's raw dotted name through the same
    /// `split_to_rows` name transformation its valued sibling's field undergoes,
    /// so the empty form contributes the SAME inferred-schema column name a valued
    /// occurrence would (`Item.middle` inside an `Item` extract fan-out → `middle`)
    /// rather than a phantom raw-prefix column. A name outside every fan-out group
    /// is a head/tail field and keeps its raw name.
    fn project_presence_name(&self, name: &str) -> String {
        let mut current = name.to_string();
        for entry in &self.config.split_to_rows {
            // Only a strict descendant of the group is owned-and-renamed by an
            // occurrence; a name merely sharing a prefix segment is not.
            if current
                .strip_prefix(entry.field.as_str())
                .is_some_and(|rest| rest.starts_with('.'))
            {
                current = projected_key(&current, &entry.field, entry.mode);
            }
        }
        current
    }
}

impl FormatReader for XmlReader {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        if let Some(ref s) = self.schema {
            return Ok(Arc::clone(s));
        }

        // Infer the schema from the first expanded record's field NAMES
        // (preserving order). The fan-out applies before inference, so the
        // columns it lifts or synthesizes are what the schema reflects.
        //
        // Name inference is deliberately infallible. The per-record assembly
        // that CAN fail — an undeclared repeated field on a non-`multiple:`
        // column — is deferred to `next_record`, so a first-record repeat
        // surfaces through the executor's record loop (where it can be
        // dead-lettered under `dlq_granularity: document`) rather than through
        // this eager `schema` call, which the ingest setup makes before the
        // loop begins and whose error would abort the whole run.
        //
        // Read forward until a record element actually expands to something: a
        // `keep_empty: false` entry drops an element with no occurrence, and
        // inferring from that dropped expansion would cache a column-less
        // schema for the whole source while records kept flowing.
        let (raw, first, presence) = loop {
            let Some(raw) = self.read_next_record_raw()? else {
                let s = SchemaBuilder::new().build();
                self.schema = Some(Arc::clone(&s));
                self.done = true;
                return Ok(s);
            };
            // Value-less self-closing columns push no field, so they never reach
            // the expansion; carry their names alongside so inference keeps them.
            let presence = raw.presence.clone();
            let mut expanded = self.apply_split_to_rows(raw.clone(), None);
            if let Some(first) = expanded.next().transpose()? {
                break (raw, first, presence);
            }
        };

        let mut seen = std::collections::HashSet::new();
        // Real field names first (in document order), then any value-less
        // self-closing columns that contributed no field — so a column present
        // only as `<x/>` in the first record is not silently absent.
        let schema = first
            .iter()
            .map(|(k, _)| k.clone())
            .chain(presence)
            .filter_map(|k| {
                if seen.insert(k.clone()) {
                    Some(k.into_boxed_str())
                } else {
                    None
                }
            })
            .collect::<SchemaBuilder>()
            .build();

        // Keep the first input raw so fallible assembly and the per-input
        // output ceiling both run from `next_record`, inside the executor's
        // record loop. Schema inference must not consume an uncounted output
        // or retain a cursor with the runtime ceiling disabled.
        self.deferred_first = Some(raw);
        self.schema = Some(Arc::clone(&schema));

        Ok(schema)
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        if self.schema.is_none() {
            self.schema()?;
        }

        loop {
            if let Some(raw) = self.deferred_first.take() {
                let original_record = (self.config.max_output_rows_per_input != 0)
                    .then(|| raw_xml_record_value(&raw));
                let mut expanded = self.apply_split_to_rows(raw, original_record);
                if let Some(fields) = expanded.next().transpose()? {
                    self.pending = Some(expanded);
                    return Ok(Some(self.fields_to_record(fields)?));
                }
            }
            if let Some(mut pending) = self.pending.take()
                && let Some(fields) = pending.next().transpose()?
            {
                self.pending = Some(pending);
                return Ok(Some(self.fields_to_record(fields)?));
            }
            let raw = match self.read_next_record_raw()? {
                Some(r) => r,
                None => return Ok(None),
            };
            let original_record =
                (self.config.max_output_rows_per_input != 0).then(|| raw_xml_record_value(&raw));
            let mut expanded = self.apply_split_to_rows(raw, original_record);
            if let Some(fields) = expanded.next().transpose()? {
                self.pending = Some(expanded);
                return Ok(Some(self.fields_to_record(fields)?));
            }
        }
    }

    fn prepare_document(
        &mut self,
        config: &EnvelopeConfig,
    ) -> Result<IndexMap<Box<str>, Value>, FormatError> {
        if config.is_empty() {
            return Ok(IndexMap::new());
        }

        // The path-pruned index is the retention authority: it knows which
        // sections some downstream program reads. A declared section no
        // program references is not extracted at all — so when no `$doc`
        // path is attributed to this source, the pre-scan skips the whole
        // document.
        let mut index =
            DocArenaIndex::new(&self.config.declared_doc_paths, self.config.max_index_bytes);
        if index.is_empty() {
            return Ok(IndexMap::new());
        }

        // Compile only the wanted sections' XmlPaths into path-segment
        // targets; a JsonPointer/Segment arrival means a config-for-wrong-
        // format mistake and surfaces as a format error. Sections the index
        // does not want are dropped here so the streaming pass never
        // descends into them.
        let mut targets: Vec<SectionTarget> = Vec::new();
        for (name, section) in &config.sections {
            if !index.wants_section(name) {
                continue;
            }
            match &section.extract {
                EnvelopeExtract::XmlPath(p) => {
                    targets.push(SectionTarget::new(Box::from(name.as_str()), p));
                }
                EnvelopeExtract::JsonPointer(_) => {
                    return Err(FormatError::Xml(format!(
                        "envelope section {name:?}: declared `json_pointer` extract \
                         against an XML source. Use `xml_path` for XML envelope sections."
                    )));
                }
                EnvelopeExtract::Segment(_) | EnvelopeExtract::RecordType(_) => {
                    return Err(FormatError::Xml(format!(
                        "envelope section {name:?}: declared a flat-file extract \
                         (`segment` / `record_type`) against an XML source. Those \
                         extracts are for flat-file formats (EDIFACT, multi-record \
                         CSV / fixed-width); use `xml_path` for XML."
                    )));
                }
            }
        }

        // Single streaming pass over a freshly re-opened reader: only the
        // matched subtrees are flattened; every unmatched element body is
        // event-walked and dropped. The cap is charged *as each declared
        // section's payload is built*, so an oversized declared section aborts
        // the parse mid-subtree rather than after the whole subtree
        // materializes. Body iteration opens its own independent reader, so
        // this pass does not consume it and no shared whole-file buffer is
        // held.
        //
        // Confirm the pre-scan re-opens the same content the body opened. A
        // path-backed input replaced or truncated between the two opens (an
        // external producer re-emitting mid-run) would otherwise splice this
        // envelope onto a body parsed from different bytes; the `(len, mtime)`
        // identity check fails loud instead. This is a cheap courtesy guard
        // under the finite-batch input-stability contract, not a fingerprint —
        // see `SourceIdentity`.
        let (prescan, prescan_identity) = Self::open_buf(&self.source)?;
        prescan_identity
            .ensure_matches(&self.body_identity)
            .map_err(FormatError::Io)?;
        let matched = extract_sections(
            prescan,
            &targets,
            &self.config.namespace_handling,
            &self.config.attribute_prefix,
            self.config.max_index_bytes,
        )?;

        // Coerce each matched payload to its declared field schema and retain
        // it in the index, which accounts the coerced (field-filtered)
        // retained bytes against the same cap. The streaming pass already
        // bounded the raw parse; the index accounts what is actually kept.
        for (name, payload) in matched {
            let section = match config.sections.get(&*name) {
                Some(s) => s,
                None => continue,
            };
            let typed =
                coerce_section_fields(payload, &section.fields).map_err(FormatError::Xml)?;
            let path = doc_path_for_section(&name);
            index
                .insert(&path, Value::Map(Box::new(typed)))
                .map_err(FormatError::Xml)?;
        }
        Ok(index.into_sections())
    }
}

/// Build the section-level [`DocPath`] under which a whole matched section
/// payload is retained.
///
/// XML retains an envelope section as one flattened map (one element subtree
/// → one map of `$doc.<section>.<field>` values), so the insert key is the
/// section, not an individual field; [`DocArenaIndex::insert`] groups by
/// `path.section`. The `field`/`indices` axes carry no meaning for a
/// section-granular retention and are left empty.
fn doc_path_for_section(name: &str) -> DocPath {
    DocPath {
        section: name.into(),
        field: Box::from(""),
        indices: Vec::new(),
    }
}

/// Resolve an element's name under the configured namespace policy.
///
/// `Strip` drops the namespace prefix (keeping the local name); `Qualify`
/// keeps the full namespace-qualified name. Shared by body iteration and
/// the envelope streaming pre-scan so both map element names identically.
pub(crate) fn elem_name_static(ns: &NamespaceMode, qname: &quick_xml::name::QName) -> String {
    let local = qname.local_name();
    let bytes = match ns {
        NamespaceMode::Strip => local.as_ref(),
        NamespaceMode::Qualify => qname.as_ref(),
    };
    String::from_utf8_lossy(bytes).into_owned()
}

/// Extract an element's attributes as `(prefixed_key, value)` pairs.
///
/// Each attribute key is prefixed with `prefix` (default `@`) so attributes
/// and child elements never collide in the flattened field set. Shared by
/// body iteration and the envelope streaming pre-scan.
pub(crate) fn extract_attributes_static(
    prefix: &str,
    elem: &quick_xml::events::BytesStart,
) -> Result<Vec<(String, String)>, FormatError> {
    let mut attrs = Vec::new();
    for attr in elem.attributes() {
        let attr = attr.map_err(|e| FormatError::Xml(e.to_string()))?;
        let key = String::from_utf8_lossy(attr.key.as_ref()).into_owned();
        // Resolve entity and character references over the UTF-8-decoded raw
        // value — the exact behavior of the removed `unescape_value()`. The
        // `normalized_value` replacement additionally collapses literal tab / CR
        // / LF to a space (XML attribute-value normalization), which would alter
        // attribute values carrying literal whitespace, so it is not used here.
        let decoded = std::str::from_utf8(attr.value.as_ref())
            .map_err(|e| FormatError::Xml(e.to_string()))?;
        let val = escape::unescape(decoded)
            .map_err(|e| FormatError::Xml(e.to_string()))?
            .into_owned();
        attrs.push((format!("{prefix}{key}"), val));
    }
    Ok(attrs)
}

/// True for the whitespace characters XML (and quick-xml's `trim_text`)
/// trims from a text node: space, tab, carriage return, line feed.
fn is_xml_whitespace(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\r' | '\n')
}

/// Append a general or character reference to a raw text run in its source
/// form (`&name;`), so it resolves alongside the rest of the run in
/// [`finalize_text_run`].
///
/// quick-xml emits each `&name;` in a text node as its own `GeneralRef`
/// event carrying just the inner `name` (`amp`, `#65`, `#x41`, …); rewrapping
/// it lets one [`escape::unescape`] pass decode the whole node.
pub(crate) fn append_general_ref(raw: &mut String, r: &BytesRef) -> Result<(), FormatError> {
    let name = r.decode().map_err(|e| FormatError::Xml(e.to_string()))?;
    raw.push('&');
    raw.push_str(&name);
    raw.push(';');
    Ok(())
}

/// Resolve one accumulated text node's raw source form into its final value.
///
/// Reproduces the pre-0.41 `trim_text(true)` + `BytesText::unescape()`
/// behavior: the raw node — `Text` fragments verbatim, each reference rewrapped
/// as `&name;` by [`append_general_ref`] — is edge-trimmed on the XML
/// whitespace set, then predefined entities and character references are
/// resolved by [`escape::unescape`]. Trimming the reassembled raw node (rather
/// than each fragment) preserves whitespace produced by a reference such as
/// `&#32;`, and an unknown entity still errors, exactly as before.
pub(crate) fn finalize_text_run(raw: &str) -> Result<String, FormatError> {
    let trimmed = raw.trim_matches(is_xml_whitespace);
    let value = escape::unescape(trimmed).map_err(|e| FormatError::Xml(e.to_string()))?;
    Ok(value.into_owned())
}

/// The name an occurrence's field carries on the output record.
///
/// Under [`SplitToRowsMode::Split`] the record shape is preserved, so the key
/// keeps its full dotted path. Under [`SplitToRowsMode::Extract`] the
/// occurrence becomes the record, so the declared field's prefix is lifted off
/// — `Item.name` becomes `name`. A repeated scalar element's own text has no
/// remainder to lift, so it keeps the path's last segment (`Tag`), which is
/// the only name it could sensibly carry.
fn projected_key(key: &str, path: &str, mode: SplitToRowsMode) -> String {
    match mode {
        SplitToRowsMode::Split => key.to_string(),
        SplitToRowsMode::Extract => strip_field_prefix(key, path).to_string(),
    }
}

/// The name `key` carries once `path`'s prefix is lifted off it. Borrows from
/// whichever input survives, so the shadowing check can build a set without
/// allocating a string per occurrence field.
fn strip_field_prefix<'a>(key: &'a str, path: &'a str) -> &'a str {
    match key.strip_prefix(path) {
        Some(rest) if !rest.is_empty() => rest.trim_start_matches('.'),
        _ => path.rsplit('.').next().unwrap_or(path),
    }
}

/// Resolve the current text run and push it as a field keyed by the innermost
/// open element's dotted path, marking that element as having contributed
/// content. Text directly under the record element (empty path) and an empty
/// resolved value push nothing. A value-less element is NOT handled here — it is
/// resolved at its `End` (empty-body `<x></x>`) or in the `Event::Empty` arm
/// (self-closing `<x/>`), where "value-less" means the element contributed no
/// child element and no non-empty text, independent of any attributes it
/// carries. Clears `text_run` for the next node.
fn flush_text_field(
    fields: &mut Vec<(String, String)>,
    element_stack: &[String],
    text_run: &mut String,
    element_content: &mut [bool],
) -> Result<(), FormatError> {
    let value = finalize_text_run(text_run)?;
    text_run.clear();
    let field_name = element_stack.join(".");
    if field_name.is_empty() || value.is_empty() {
        return Ok(());
    }
    fields.push((field_name, value));
    if let Some(has_content) = element_content.last_mut() {
        *has_content = true;
    }
    Ok(())
}

/// Consume a single leading UTF-8 BOM from a freshly opened reader, if present.
///
/// Each pass re-opens its own `Read`, so a Windows-authored file (Excel /
/// PowerShell utf8 export) carries the BOM on every open; stripping it here
/// clears the marker before it precedes the prolog/root element, for both body
/// iteration and the envelope pre-scan. The `BufReader`'s default capacity
/// exceeds the 3-byte BOM, so the marker is always wholly inside the first fill.
///
/// # Errors
///
/// Returns [`FormatError::Io`] if the probe read fails.
fn strip_leading_bom(reader: &mut BufReader<Box<dyn Read + Send>>) -> Result<(), FormatError> {
    let buf = reader.fill_buf().map_err(FormatError::Io)?;
    if buf.starts_with(&UTF8_BOM) {
        reader.consume(UTF8_BOM.len());
    }
    Ok(())
}

fn inferred_xml_value(s: &str, observation: &NumericObservation) -> Value {
    match observation.parser_outcome() {
        NumericParserOutcome::NoValue => return Value::Null,
        NumericParserOutcome::Integer(value) => return Value::Integer(*value),
        NumericParserOutcome::Float(value) => return Value::Float(*value),
        NumericParserOutcome::NonNumeric | NumericParserOutcome::Rejected(_) => {}
    }
    match s {
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        _ => Value::String(s.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::EnvelopeFieldType;
    use std::io::Cursor;

    fn reader_from_str(xml: &str, config: XmlReaderConfig) -> XmlReader {
        XmlReader::from_reader(Cursor::new(xml.as_bytes().to_vec()), config)
            .expect("XML buffer read")
    }

    fn default_config_with_path(path: &str) -> XmlReaderConfig {
        XmlReaderConfig {
            record_path: Some(path.into()),
            ..Default::default()
        }
    }

    /// `(section name, XPath, [(field name, type)])` for one envelope section.
    type SectionSpec<'a> = (&'a str, &'a str, &'a [(&'a str, EnvelopeFieldType)]);

    /// Declared `$doc.*` paths covering every `(section, field)` in `specs`,
    /// so the path-pruned index wants all of them — the runtime stand-in for
    /// the planner's per-source attribution.
    fn declared_paths(specs: &[SectionSpec]) -> Vec<DocPath> {
        let mut out = Vec::new();
        for (section, _xpath, fields) in specs {
            for (field, _ty) in *fields {
                out.push(DocPath {
                    section: (*section).into(),
                    field: (*field).into(),
                    indices: Vec::new(),
                });
            }
        }
        out
    }

    /// A reader config over `record_path` whose declared paths want every
    /// section in `specs`, for an envelope-bearing source.
    fn envelope_reader_config(specs: &[SectionSpec], record_path: &str) -> XmlReaderConfig {
        XmlReaderConfig {
            record_path: Some(record_path.into()),
            declared_doc_paths: declared_paths(specs),
            ..Default::default()
        }
    }

    /// A reader config wanting a single section named `Bad`, so the
    /// wrong-format extract validation fires for that section.
    fn config_wanting_bad_section(record_path: &str) -> XmlReaderConfig {
        XmlReaderConfig {
            record_path: Some(record_path.into()),
            declared_doc_paths: vec![DocPath {
                section: "Bad".into(),
                field: "any".into(),
                indices: Vec::new(),
            }],
            ..Default::default()
        }
    }

    /// A catalogue the rejected paths below all *look* like they would match,
    /// so a test that reads zero records is reading zero from a real document.
    const CATALOG: &str = r#"<catalog>
        <product><product_id>1</product_id><name>A</name></product>
        <product><product_id>2</product_id><name>B</name></product>
    </catalog>"#;

    #[test]
    fn xpath_shaped_record_path_fails_at_construction() {
        // Every form here used to build a segment list containing an empty
        // string, which no element name equals: the reader ran to EOF and the
        // pipeline reported success over zero records.
        for raw in ["//product", "/catalog/product", "catalog//product", ""] {
            let Err(err) = XmlReader::from_reader(
                Cursor::new(CATALOG.as_bytes().to_vec()),
                default_config_with_path(raw),
            ) else {
                panic!("{raw:?}: must be rejected at construction");
            };
            let msg = match err {
                FormatError::Xml(m) => m,
                other => panic!("{raw:?}: expected FormatError::Xml, got {other:?}"),
            };
            assert!(msg.contains("record_path"), "{raw:?}: {msg}");
        }
    }

    #[test]
    fn jsonpath_shaped_record_path_on_an_xml_source_fails_at_construction() {
        let Err(err) = XmlReader::from_reader(
            Cursor::new(CATALOG.as_bytes().to_vec()),
            default_config_with_path("$.product"),
        ) else {
            panic!("must be rejected at construction");
        };
        match err {
            FormatError::Xml(m) => assert!(m.contains("JSONPath"), "{m}"),
            other => panic!("expected FormatError::Xml, got {other:?}"),
        }
    }

    #[test]
    fn the_corrected_record_path_still_streams_every_record() {
        // The positive twin of the rejections above: the XML-name segment rule
        // must not over-reject a path that matches.
        let mut reader = reader_from_str(CATALOG, default_config_with_path("catalog/product"));
        let first = reader.next_record().expect("record").expect("first");
        assert_eq!(first.get("product_id"), Some(&Value::Integer(1)));
        let second = reader.next_record().expect("record").expect("second");
        assert_eq!(second.get("product_id"), Some(&Value::Integer(2)));
        assert!(reader.next_record().expect("eof").is_none());
    }

    fn envelope_config(sections: &[SectionSpec]) -> EnvelopeConfig {
        use crate::envelope::EnvelopeSection;
        let mut cfg = EnvelopeConfig::default();
        for (name, xpath, fields) in sections {
            let mut field_map = IndexMap::new();
            for (fname, ftype) in *fields {
                field_map.insert((*fname).to_string(), *ftype);
            }
            cfg.sections.insert(
                (*name).to_string(),
                EnvelopeSection {
                    extract: EnvelopeExtract::XmlPath((*xpath).to_string()),
                    fields: field_map,
                },
            );
        }
        cfg
    }

    fn unwrap_section_map(value: &Value) -> &IndexMap<Box<str>, Value> {
        match value {
            Value::Map(m) => m,
            other => panic!("expected Value::Map, got {other:?}"),
        }
    }

    #[test]
    fn prepare_document_extracts_head_and_foot_arbitrary_names() {
        // Section names are user-chosen. The engine treats them as
        // opaque identifiers — `BatchInfo` and `Summary` are equally
        // valid as `Head` / `Foot`. The pre-scan must extract both
        // before the first body record streams.
        let xml = r#"<doc>
            <BatchInfo><batch_id>RUN-001</batch_id><count>42</count></BatchInfo>
            <records><record><x>1</x></record><record><x>2</x></record></records>
            <Summary><hash>abc</hash><processed>2</processed></Summary>
        </doc>"#;
        let specs: &[SectionSpec] = &[
            (
                "BatchInfo",
                "/doc/BatchInfo",
                &[
                    ("batch_id", EnvelopeFieldType::String),
                    ("count", EnvelopeFieldType::Int),
                ],
            ),
            (
                "Summary",
                "/doc/Summary",
                &[
                    ("hash", EnvelopeFieldType::String),
                    ("processed", EnvelopeFieldType::Int),
                ],
            ),
        ];
        let cfg = envelope_config(specs);

        let mut reader = reader_from_str(xml, envelope_reader_config(specs, "doc/records/record"));
        let sections = reader.prepare_document(&cfg).expect("envelope pre-scan");

        // Both sections present — the post-body section is available
        // alongside the pre-body section.
        assert_eq!(sections.len(), 2);
        let head = unwrap_section_map(sections.get("BatchInfo").expect("BatchInfo extracted"));
        assert_eq!(head.get("batch_id"), Some(&Value::String("RUN-001".into())));
        assert_eq!(head.get("count"), Some(&Value::Integer(42)));

        let foot = unwrap_section_map(sections.get("Summary").expect("Summary extracted"));
        assert_eq!(foot.get("hash"), Some(&Value::String("abc".into())));
        assert_eq!(foot.get("processed"), Some(&Value::Integer(2)));

        // Body iteration still works from byte 0; envelope pre-scan
        // does not consume the body parser state.
        let r1 = reader.next_record().expect("body record").expect("first");
        assert_eq!(r1.get("x"), Some(&Value::Integer(1)));
        let r2 = reader.next_record().expect("body record").expect("second");
        assert_eq!(r2.get("x"), Some(&Value::Integer(2)));
        assert!(reader.next_record().expect("eof").is_none());
    }

    #[test]
    fn open_buf_strips_the_bom_on_every_open() {
        // The body and the envelope pre-scan each call `open_buf` on their own
        // fresh `Read`, so a Windows-authored file (Excel / PowerShell utf8
        // export) presents the leading BOM to *both* opens. The strip must
        // therefore live in `open_buf` (the shared per-open path), not in one
        // caller. quick-xml 0.37 tolerates a stray prolog BOM, so a strip
        // regression would NOT surface at the record/section level — it would
        // only show as raw BOM bytes leading the parser's input. Assert the
        // contract at that byte level, independent of quick-xml: every
        // `open_buf` hands back a reader whose first bytes are the document,
        // not `\u{feff}`. Two opens prove the strip is per-open, not one-shot.
        let mut bytes = UTF8_BOM.to_vec();
        bytes.extend_from_slice(b"<doc><x>1</x></doc>");
        let source = ReopenableSource::buffer(Cursor::new(bytes)).expect("buffer source");

        for pass in ["body", "pre-scan"] {
            let (mut buf, _identity) = XmlReader::open_buf(&source).expect("open_buf");
            let head = buf.fill_buf().expect("fill");
            assert!(
                head.starts_with(b"<doc>"),
                "{pass} open leaked a BOM: stream starts with {:?}",
                &head[..head.len().min(UTF8_BOM.len() + 2)]
            );
            assert!(
                !head.starts_with(&UTF8_BOM),
                "{pass} open left the BOM in place"
            );
        }
    }

    #[test]
    fn open_buf_passes_through_a_bomless_open_unchanged() {
        // A file with no BOM (the common case) must not lose its first bytes:
        // `strip_leading_bom` consumes only when the marker is present, so the
        // document element survives the probe intact.
        let source =
            ReopenableSource::buffer(Cursor::new(b"<doc><x>1</x></doc>".to_vec())).expect("buffer");
        let (mut buf, _identity) = XmlReader::open_buf(&source).expect("open_buf");
        assert!(buf.fill_buf().expect("fill").starts_with(b"<doc>"));
    }

    #[test]
    fn prepare_document_extracts_sections_from_a_bom_prefixed_source() {
        // End-to-end companion to `open_buf_strips_the_bom_on_every_open`: a
        // BOM-prefixed envelope-bearing document still yields clean section
        // values and clean body records, exercising the pre-scan and body
        // opens through the full `prepare_document` / `next_record` path.
        let xml = r#"<doc>
            <BatchInfo><batch_id>RUN-001</batch_id><count>42</count></BatchInfo>
            <records><record><x>1</x></record><record><x>2</x></record></records>
            <Summary><hash>abc</hash></Summary>
        </doc>"#;
        let mut bytes = UTF8_BOM.to_vec();
        bytes.extend_from_slice(xml.as_bytes());

        let specs: &[SectionSpec] = &[
            (
                "BatchInfo",
                "/doc/BatchInfo",
                &[
                    ("batch_id", EnvelopeFieldType::String),
                    ("count", EnvelopeFieldType::Int),
                ],
            ),
            (
                "Summary",
                "/doc/Summary",
                &[("hash", EnvelopeFieldType::String)],
            ),
        ];
        let cfg = envelope_config(specs);

        let mut reader = XmlReader::from_reader(
            Cursor::new(bytes),
            envelope_reader_config(specs, "doc/records/record"),
        )
        .expect("XML buffer read");
        let sections = reader.prepare_document(&cfg).expect("envelope pre-scan");
        assert_eq!(sections.len(), 2);

        let head = unwrap_section_map(sections.get("BatchInfo").expect("BatchInfo extracted"));
        assert_eq!(head.get("batch_id"), Some(&Value::String("RUN-001".into())));
        assert_eq!(head.get("count"), Some(&Value::Integer(42)));
        let foot = unwrap_section_map(sections.get("Summary").expect("Summary extracted"));
        assert_eq!(foot.get("hash"), Some(&Value::String("abc".into())));

        let r1 = reader.next_record().expect("body record").expect("first");
        assert_eq!(r1.get("x"), Some(&Value::Integer(1)));
        let r2 = reader.next_record().expect("body record").expect("second");
        assert_eq!(r2.get("x"), Some(&Value::Integer(2)));
        assert!(reader.next_record().expect("eof").is_none());
    }

    #[test]
    fn prepare_document_empty_config_returns_empty() {
        let xml = r#"<doc><a><x>1</x></a></doc>"#;
        let mut reader = reader_from_str(xml, default_config_with_path("doc/a"));
        let sections = reader
            .prepare_document(&EnvelopeConfig::default())
            .expect("empty config");
        assert!(sections.is_empty());
    }

    #[test]
    fn prepare_document_rejects_json_pointer_extract() {
        use crate::envelope::EnvelopeSection;
        let xml = r#"<doc><a><x>1</x></a></doc>"#;
        let mut cfg = EnvelopeConfig::default();
        cfg.sections.insert(
            "Bad".into(),
            EnvelopeSection {
                extract: EnvelopeExtract::JsonPointer("/doc/Bad".into()),
                fields: IndexMap::new(),
            },
        );
        let mut reader = reader_from_str(xml, config_wanting_bad_section("doc/a"));
        let err = reader.prepare_document(&cfg).unwrap_err();
        assert!(matches!(err, FormatError::Xml(msg) if msg.contains("json_pointer")));
    }

    #[test]
    fn prepare_document_rejects_segment_extract() {
        use crate::envelope::EnvelopeSection;
        let xml = r#"<doc><a><x>1</x></a></doc>"#;
        let mut cfg = EnvelopeConfig::default();
        cfg.sections.insert(
            "Bad".into(),
            EnvelopeSection {
                extract: EnvelopeExtract::Segment("UNB".into()),
                fields: IndexMap::new(),
            },
        );
        let mut reader = reader_from_str(xml, config_wanting_bad_section("doc/a"));
        let err = reader.prepare_document(&cfg).unwrap_err();
        assert!(matches!(err, FormatError::Xml(msg) if msg.contains("segment")));
    }

    #[test]
    fn prepare_document_missing_section_yields_no_entry() {
        // A section that the config declares but the XML doesn't carry
        // is absent from the returned map; CXL resolves missing
        // sections to `Value::Null`.
        let xml = r#"<doc><records><record><x>1</x></record></records></doc>"#;
        let specs: &[SectionSpec] = &[(
            "Trailer",
            "/doc/Trailer",
            &[("count", EnvelopeFieldType::Int)],
        )];
        let cfg = envelope_config(specs);
        let mut reader = reader_from_str(xml, envelope_reader_config(specs, "doc/records/record"));
        let sections = reader.prepare_document(&cfg).expect("scan ok");
        assert!(sections.is_empty());
    }

    #[test]
    fn prepare_document_coerces_typed_fields() {
        let xml = r#"<doc>
            <Meta>
                <run_date>2026-05-22</run_date>
                <enabled>true</enabled>
                <ratio>0.5</ratio>
            </Meta>
            <records><record><x>1</x></record></records>
        </doc>"#;
        let specs: &[SectionSpec] = &[(
            "Meta",
            "/doc/Meta",
            &[
                ("run_date", EnvelopeFieldType::Date),
                ("enabled", EnvelopeFieldType::Bool),
                ("ratio", EnvelopeFieldType::Float),
            ],
        )];
        let cfg = envelope_config(specs);
        let mut reader = reader_from_str(xml, envelope_reader_config(specs, "doc/records/record"));
        let sections = reader.prepare_document(&cfg).expect("scan ok");
        let meta = unwrap_section_map(sections.get("Meta").unwrap());
        assert!(matches!(meta.get("run_date"), Some(Value::Date(_))));
        assert_eq!(meta.get("enabled"), Some(&Value::Bool(true)));
        assert_eq!(meta.get("ratio"), Some(&Value::Float(0.5)));
    }

    #[test]
    fn body_records_navigate_past_sibling_envelope_sections() {
        // record_path body sits between a head section and a tail
        // section; navigation must skip both siblings and still yield
        // every record.
        let xml = r#"<doc>
  <BatchInfo><batch_id>RUN-001</batch_id></BatchInfo>
  <records>
    <record><amount>10</amount></record>
    <record><amount>20</amount></record>
    <record><amount>30</amount></record>
  </records>
  <Summary><total>3</total></Summary>
</doc>"#;
        let mut r = reader_from_str(xml, default_config_with_path("doc/records/record"));
        let _ = r.schema().unwrap();
        let mut n = 0;
        while let Some(_rec) = r.next_record().unwrap() {
            n += 1;
        }
        assert_eq!(n, 3, "expected 3 body records past sibling sections");
    }

    #[test]
    fn test_xml_record_path_navigation() {
        let xml = r#"<Root><Orders><Order><id>1</id><name>Alice</name></Order><Order><id>2</id><name>Bob</name></Order></Orders></Root>"#;
        let mut r = reader_from_str(xml, default_config_with_path("Root/Orders/Order"));
        let s = r.schema().unwrap();
        assert!(s.columns().iter().any(|c| &**c == "id"));
        assert!(s.columns().iter().any(|c| &**c == "name"));

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        assert_eq!(r1.get("name"), Some(&Value::String("Alice".into())));

        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("id"), Some(&Value::Integer(2)));

        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_xml_strips_leading_bom() {
        // A leading UTF-8 BOM (Windows utf8 export) must be stripped, or it
        // precedes the root element and breaks element-path matching.
        let xml = r#"<Root><Orders><Order><id>1</id><name>Alice</name></Order></Orders></Root>"#;
        let mut bytes = crate::bom::UTF8_BOM.to_vec();
        bytes.extend_from_slice(xml.as_bytes());
        let mut r = XmlReader::from_reader(
            Cursor::new(bytes),
            default_config_with_path("Root/Orders/Order"),
        )
        .expect("XML buffer read");

        let rec = r.next_record().unwrap().unwrap();
        assert_eq!(rec.get("id"), Some(&Value::Integer(1)));
        assert_eq!(rec.get("name"), Some(&Value::String("Alice".into())));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_xml_attribute_extraction_default_prefix() {
        let xml = r#"<Root><Item id="5" status="open"><name>Widget</name></Item></Root>"#;
        let mut r = reader_from_str(xml, default_config_with_path("Root/Item"));
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("@id"), Some(&Value::Integer(5)));
        assert_eq!(r1.get("@status"), Some(&Value::String("open".into())));
        assert_eq!(r1.get("name"), Some(&Value::String("Widget".into())));
    }

    #[test]
    fn test_xml_attribute_custom_prefix() {
        let xml = r#"<Root><Item id="5"/></Root>"#;
        let config = XmlReaderConfig {
            record_path: Some("Root/Item".into()),
            attribute_prefix: "_".into(),
            ..Default::default()
        };
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("_id"), Some(&Value::Integer(5)));
    }

    #[test]
    fn test_xml_namespace_strip() {
        let xml = r#"<ns:Root><ns:Item><ns:name>Alice</ns:name></ns:Item></ns:Root>"#;
        let config = XmlReaderConfig {
            record_path: Some("Root/Item".into()),
            namespace_handling: NamespaceMode::Strip,
            ..Default::default()
        };
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("name"), Some(&Value::String("Alice".into())));
    }

    #[test]
    fn test_xml_namespace_qualify() {
        let xml = r#"<ns:Root><ns:Item><ns:name>Alice</ns:name></ns:Item></ns:Root>"#;
        let config = XmlReaderConfig {
            record_path: Some("ns:Root/ns:Item".into()),
            namespace_handling: NamespaceMode::Qualify,
            ..Default::default()
        };
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("ns:name"), Some(&Value::String("Alice".into())));
    }

    #[test]
    fn test_xml_nested_element_flattening() {
        let xml = r#"<Root><Row><Address><City>NYC</City><State>NY</State></Address></Row></Root>"#;
        let mut r = reader_from_str(xml, default_config_with_path("Root/Row"));
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("Address.City"), Some(&Value::String("NYC".into())));
        assert_eq!(r1.get("Address.State"), Some(&Value::String("NY".into())));
    }

    /// A reader config over `record_path` with the given fan-out entries.
    fn split_config(record_path: &str, entries: Vec<SplitToRows>) -> XmlReaderConfig {
        XmlReaderConfig {
            record_path: Some(record_path.into()),
            split_to_rows: entries,
            ..Default::default()
        }
    }

    /// A reader config over `record_path` whose schema declares the given
    /// fields `multiple: true`.
    fn multi_value_config(record_path: &str, fields: &[&str]) -> XmlReaderConfig {
        XmlReaderConfig {
            record_path: Some(record_path.into()),
            multi_value_fields: fields.iter().map(|f| f.to_string()).collect(),
            ..Default::default()
        }
    }

    /// A fan-out entry preserving the record shape: the occurrence's fields
    /// keep their dotted path.
    fn split(field: &str) -> SplitToRows {
        SplitToRows {
            mode: SplitToRowsMode::Split,
            ..SplitToRows::bare(field)
        }
    }

    /// A fan-out entry lifting the occurrence out from under its field name.
    fn extract(field: &str) -> SplitToRows {
        SplitToRows::bare(field)
    }

    fn column_names(schema: &Schema) -> Vec<&str> {
        schema.columns().iter().map(|c| &**c).collect()
    }

    #[test]
    fn split_to_rows_fans_repeated_children_out() {
        // Repeated <Item> children fan out into one record per occurrence;
        // the parent's fields are duplicated onto each, and under `split` the
        // exploded fields keep their full dotted names.
        let xml = r#"<Root><Order><id>1</id><Item><name>A</name><qty>2</qty></Item><Item><name>B</name><qty>3</qty></Item></Order></Root>"#;
        let config = split_config("Root/Order", vec![split("Item")]);
        let mut r = reader_from_str(xml, config);
        let s = r.schema().unwrap();
        // Schema reflects the first fanned-out record's columns.
        assert_eq!(column_names(&s), ["id", "Item.name", "Item.qty"]);

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        assert_eq!(r1.get("Item.name"), Some(&Value::String("A".into())));
        assert_eq!(r1.get("Item.qty"), Some(&Value::Integer(2)));

        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("id"), Some(&Value::Integer(1)));
        assert_eq!(r2.get("Item.name"), Some(&Value::String("B".into())));
        assert_eq!(r2.get("Item.qty"), Some(&Value::Integer(3)));

        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn split_to_rows_extract_lifts_the_occurrence_out_of_its_field() {
        // The default mode makes the occurrence the record: its fields lose
        // the declared field's prefix, and the parent's fields merge on.
        let xml = r#"<Root><Order><id>1</id><Item><name>A</name></Item><Item><name>B</name></Item></Order></Root>"#;
        let config = split_config("Root/Order", vec![extract("Item")]);
        let mut r = reader_from_str(xml, config);
        let s = r.schema().unwrap();
        assert_eq!(column_names(&s), ["id", "name"]);

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        assert_eq!(r1.get("name"), Some(&Value::String("A".into())));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("name"), Some(&Value::String("B".into())));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn extract_lifted_field_wins_over_a_same_named_parent_field() {
        // Under `extract` the occurrence IS the record, so its own `name` must
        // reach the output — not be shadowed by the parent's `name` that the
        // merge brought along.
        let xml =
            r#"<Root><Order><name>OUTER</name><Item><name>INNER</name></Item></Order></Root>"#;
        let config = split_config("Root/Order", vec![extract("Item")]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("name"), Some(&Value::String("INNER".into())));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn split_mode_keeps_both_sides_of_a_name_clash() {
        // `split` keeps the dotted path, so the two fields never collide and
        // the parent's value survives alongside the occurrence's.
        let xml =
            r#"<Root><Order><name>OUTER</name><Item><name>INNER</name></Item></Order></Root>"#;
        let config = split_config("Root/Order", vec![split("Item")]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("name"), Some(&Value::String("OUTER".into())));
        assert_eq!(r1.get("Item.name"), Some(&Value::String("INNER".into())));
    }

    #[test]
    fn split_to_rows_position_column_numbers_the_occurrences() {
        let xml = r#"<Root><Order><id>1</id><Tag>a</Tag><Tag>b</Tag></Order></Root>"#;
        let entry = SplitToRows {
            position_column: Some("tag_no".into()),
            ..SplitToRows::bare("Tag")
        };
        let config = split_config("Root/Order", vec![entry]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("Tag"), Some(&Value::String("a".into())));
        assert_eq!(r1.get("tag_no"), Some(&Value::Integer(1)));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("Tag"), Some(&Value::String("b".into())));
        assert_eq!(r2.get("tag_no"), Some(&Value::Integer(2)));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn position_column_wins_over_a_same_named_child_of_the_occurrence() {
        // The author named the position column; a child of the occurrence that
        // happens to share the name loses it. Without this the record carried
        // both, and the first-wins duplicate collapse in `fields_to_record`
        // kept the document's value and discarded the position — while the
        // JSON reader kept the position, so one declaration meant two things.
        let xml = r#"<Root><Order><id>1</id><Item><line_no>99</line_no><sku>a</sku></Item>
                     <Item><line_no>98</line_no><sku>b</sku></Item></Order></Root>"#;
        let entry = SplitToRows {
            position_column: Some("line_no".into()),
            ..extract("Item")
        };
        let config = split_config("Root/Order", vec![entry]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("sku"), Some(&Value::String("a".into())));
        assert_eq!(r1.get("line_no"), Some(&Value::Integer(1)));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("sku"), Some(&Value::String("b".into())));
        assert_eq!(r2.get("line_no"), Some(&Value::Integer(2)));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn multiple_collects_repeated_scalar_children_into_one_array() {
        // A `multiple: true` column collects every occurrence in document
        // order, rather than keeping only the first.
        let xml = r#"<Root><Row><id>7</id><Tag>a</Tag><Tag>b</Tag><Tag>c</Tag></Row></Root>"#;
        let config = multi_value_config("Root/Row", &["Tag"]);
        let mut r = reader_from_str(xml, config);
        let s = r.schema().unwrap();
        assert_eq!(column_names(&s), ["id", "Tag"]);

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::Integer(7)));
        assert_eq!(
            r1.get("Tag"),
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::String("b".into()),
                Value::String("c".into()),
            ]))
        );
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn multiple_collects_each_repeated_subfield_independently() {
        // Declaring the container's flattened children multi-value collects
        // each of them independently, in document order.
        let xml = r#"<Root><Order><id>1</id><Item><name>A</name><qty>2</qty></Item><Item><name>B</name><qty>3</qty></Item></Order></Root>"#;
        let config = multi_value_config("Root/Order", &["Item.name", "Item.qty"]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        assert_eq!(
            r1.get("Item.name"),
            Some(&Value::Array(vec![
                Value::String("A".into()),
                Value::String("B".into()),
            ]))
        );
        assert_eq!(
            r1.get("Item.qty"),
            Some(&Value::Array(vec![Value::Integer(2), Value::Integer(3)]))
        );
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn multiple_wraps_a_single_occurrence_in_a_one_element_array() {
        // The declaration, not the document, decides the shape: one
        // occurrence is still an array, so downstream code never has to
        // branch on how many values happened to arrive.
        let xml = r#"<Root><Row><Tag>only</Tag></Row></Root>"#;
        let config = multi_value_config("Root/Row", &["Tag"]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(
            r1.get("Tag"),
            Some(&Value::Array(vec![Value::String("only".into())]))
        );
    }

    #[test]
    fn multiple_absent_field_leaves_the_record_intact() {
        // An absent multi-value element does not suppress the record; the
        // column is simply not present, and the declared-schema reprojection
        // fills it.
        let xml = r#"<Root><Row><id>7</id></Row></Root>"#;
        let config = multi_value_config("Root/Row", &["Tag"]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::Integer(7)));
        assert_eq!(r1.get("Tag"), None);
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn multiple_on_an_empty_element_still_yields_an_array() {
        // `<Tag></Tag>` carries no text, so the collected array holds one
        // empty value rather than the record losing the column. An empty text
        // node resolves to `Null` (the reader's empty-string rule), and a
        // declared `multiple:` column preserves that occurrence instead of
        // dropping it.
        let xml = r#"<Root><Row><id>1</id><Tag></Tag></Row></Root>"#;
        let config = multi_value_config("Root/Row", &["Tag"]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        assert_eq!(r1.get("Tag"), Some(&Value::Array(vec![Value::Null])));
    }

    #[test]
    fn multiple_preserves_an_empty_repeated_element_positionally() {
        // `<Tag>a</Tag><Tag></Tag><Tag>b</Tag>` on a `multiple:` column must
        // collect three occurrences in order — the empty middle element is a
        // real array element, not a gap to be squeezed out. Dropping it would
        // lose position and prevent a faithful round-trip.
        let xml = r#"<Root><Row><Tag>a</Tag><Tag></Tag><Tag>b</Tag></Row></Root>"#;
        let config = multi_value_config("Root/Row", &["Tag"]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(
            r1.get("Tag"),
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::Null,
                Value::String("b".into()),
            ]))
        );
    }

    #[test]
    fn multiple_preserves_an_empty_self_closing_element_positionally() {
        // The self-closing empty form `<Tag/>` must behave exactly like the
        // empty-body `<Tag></Tag>` on a `multiple:` column: a real empty
        // occurrence collected in position, so the two forms stay symmetric and
        // neither squeezes the empty middle out of `[a, "", b]`.
        let xml = r#"<Root><Row><Tag>a</Tag><Tag/><Tag>b</Tag></Row></Root>"#;
        let config = multi_value_config("Root/Row", &["Tag"]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(
            r1.get("Tag"),
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::Null,
                Value::String("b".into()),
            ]))
        );
    }

    #[test]
    fn multiple_preserves_an_empty_element_with_an_attribute_positionally() {
        // A value-less element that carries an ATTRIBUTE is still value-less for
        // its OWN column — the attribute keys to a distinct `Tag.@x` column. Both
        // the self-closing `<Tag x="1"/>` and empty-body `<Tag x="1"></Tag>`
        // forms must collect the empty (null) occurrence in position, so the
        // attribute push does not mask it: `[a, null, b]`, never `[a, b]`.
        for middle in [r#"<Tag x="1"/>"#, r#"<Tag x="1"></Tag>"#] {
            let xml = format!("<Root><Row><Tag>a</Tag>{middle}<Tag>b</Tag></Row></Root>");
            let config = multi_value_config("Root/Row", &["Tag"]);
            let mut r = reader_from_str(&xml, config);
            let _s = r.schema().unwrap();

            let r1 = r.next_record().unwrap().unwrap();
            assert_eq!(
                r1.get("Tag"),
                Some(&Value::Array(vec![
                    Value::String("a".into()),
                    Value::Null,
                    Value::String("b".into()),
                ])),
                "middle form {middle:?}"
            );
            // The attribute still lands on its own (non-`multiple:`) column.
            assert_eq!(
                r1.get("Tag.@x"),
                Some(&Value::Integer(1)),
                "form {middle:?}"
            );
        }
    }

    #[test]
    fn attributed_self_closing_on_a_non_multiple_column_does_not_drop_or_misassemble() {
        // The non-`multiple:` counterpart: a value-less self-closing element with
        // an attribute must not error and must not invent a value for its own
        // column — only the attribute column is populated.
        let xml = r#"<Root><Row><id>1</id><middle x="9"/></Row></Root>"#;
        let config = default_config_with_path("Root/Row");
        let mut r = reader_from_str(xml, config);
        let rec = r
            .next_record()
            .expect("value-less self-closing with an attribute must not error")
            .expect("one record");
        assert_eq!(rec.get("id"), Some(&Value::Integer(1)));
        assert_eq!(rec.get("middle"), None, "own column stays value-less");
        assert_eq!(rec.get("middle.@x"), Some(&Value::Integer(9)));
    }

    #[test]
    fn self_closing_only_inside_extract_fan_out_uses_the_projected_schema_name() {
        // Under `split_to_rows: extract`, a valued `<name>` inside `<Item>`
        // projects to column `name` (the group prefix is lifted off). A
        // self-closing-only `<middle/>` inside `<Item>` must contribute the SAME
        // projected name `middle`, never a phantom raw `Item.middle` that no
        // record carries.
        let xml = r#"<Root><Row><Item><name>x</name><middle/></Item></Row></Root>"#;
        let config = split_config("Root/Row", vec![extract("Item")]);
        let mut r = reader_from_str(xml, config);
        let s = r.schema().unwrap();
        let cols = column_names(&s);
        assert!(cols.contains(&"name"), "valued column projected: {cols:?}");
        assert!(
            cols.contains(&"middle"),
            "self-closing column uses the projected name: {cols:?}"
        );
        assert!(
            !cols.contains(&"Item.middle"),
            "no phantom raw-prefix column: {cols:?}"
        );
    }

    #[test]
    fn empty_body_and_self_closing_only_columns_agree_in_the_inferred_schema() {
        // Schema presence is consistent across the two empty forms: a column that
        // appears only as an empty-body `<a></a>` and one only as a self-closing
        // `<b/>` both surface in the inferred schema.
        let xml = r#"<Root><Row><id>1</id><a></a><b/></Row></Root>"#;
        let config = default_config_with_path("Root/Row");
        let mut r = reader_from_str(xml, config);
        let s = r.schema().unwrap();
        let cols = column_names(&s);
        assert!(
            cols.contains(&"a"),
            "empty-body-only column present: {cols:?}"
        );
        assert!(
            cols.contains(&"b"),
            "self-closing-only column present: {cols:?}"
        );
        assert!(cols.contains(&"id"));
    }

    #[test]
    fn split_values_on_an_absent_field_leaves_the_record_intact() {
        let xml = r#"<Root><Row><id>1</id></Row></Root>"#;
        let config = XmlReaderConfig {
            split_values: vec![SplitValues::bare("Tag")],
            ..multi_value_config("Root/Row", &["Tag"])
        };
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        assert_eq!(r1.get("Tag"), None);
    }

    #[test]
    fn extract_names_a_dotted_scalar_field_by_its_last_segment() {
        // The prefix is lifted off, so a repeated scalar under a dotted path
        // lands under the element's own name — the same column the JSON
        // reader produces for the same declaration.
        let xml = r#"<Root><Row><Tags><Tag>a</Tag><Tag>b</Tag></Tags></Row></Root>"#;
        let config = split_config("Root/Row", vec![extract("Tags.Tag")]);
        let mut r = reader_from_str(xml, config);
        let s = r.schema().unwrap();
        assert_eq!(column_names(&s), ["Tag"]);

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("Tag"), Some(&Value::String("a".into())));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("Tag"), Some(&Value::String("b".into())));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn position_column_wins_over_a_same_named_data_field() {
        // The index was explicitly asked for, so it is not shadowed by a
        // document field that happens to share the name.
        let xml = r#"<Root><Row><line_no>99</line_no><Tag>a</Tag><Tag>b</Tag></Row></Root>"#;
        let entry = SplitToRows {
            position_column: Some("line_no".into()),
            ..SplitToRows::bare("Tag")
        };
        let config = split_config("Root/Row", vec![entry]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("line_no"), Some(&Value::Integer(1)));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("line_no"), Some(&Value::Integer(2)));
    }

    #[test]
    fn keep_empty_false_on_the_first_element_still_infers_a_schema() {
        // The first record element is dropped, so inference must read forward
        // rather than caching a column-less schema for a source that goes on
        // emitting records.
        let xml = r#"<Root>
            <Order><id>1</id></Order>
            <Order><id>2</id><Item><name>A</name></Item></Order>
        </Root>"#;
        let entry = SplitToRows {
            keep_empty: false,
            mode: SplitToRowsMode::Split,
            ..SplitToRows::bare("Item")
        };
        let config = split_config("Root/Order", vec![entry]);
        let mut r = reader_from_str(xml, config);
        let s = r.schema().unwrap();
        assert_eq!(column_names(&s), ["id", "Item.name"]);

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::Integer(2)));
        assert_eq!(r1.get("Item.name"), Some(&Value::String("A".into())));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn split_values_parses_a_delimited_cell_into_several_values() {
        let xml = r#"<Root><Row><id>7</id><Tag>a;b;c</Tag></Row></Root>"#;
        let config = XmlReaderConfig {
            split_values: vec![SplitValues::bare("Tag")],
            ..multi_value_config("Root/Row", &["Tag"])
        };
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(
            r1.get("Tag"),
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::String("b".into()),
                Value::String("c".into()),
            ]))
        );
    }

    #[test]
    fn split_to_rows_fans_a_repeated_scalar_out_by_value() {
        // A repeated scalar element fans out to one record per value, keyed
        // by the element's own path under both modes.
        let xml = r#"<Root><Row><id>7</id><Tag>a</Tag><Tag>b</Tag></Row></Root>"#;
        let config = split_config("Root/Row", vec![extract("Tag")]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::Integer(7)));
        assert_eq!(r1.get("Tag"), Some(&Value::String("a".into())));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("id"), Some(&Value::Integer(7)));
        assert_eq!(r2.get("Tag"), Some(&Value::String("b".into())));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn split_to_rows_carries_instance_attributes() {
        // Attributes on the repeated element belong to their occurrence, not
        // to the shared parent fields.
        let xml = r#"<Root><Order><id>1</id><Item sku="X"><name>A</name></Item><Item sku="Y"><name>B</name></Item></Order></Root>"#;
        let config = split_config("Root/Order", vec![split("Item")]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("Item.@sku"), Some(&Value::String("X".into())));
        assert_eq!(r1.get("Item.name"), Some(&Value::String("A".into())));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("Item.@sku"), Some(&Value::String("Y".into())));
        assert_eq!(r2.get("Item.name"), Some(&Value::String("B".into())));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn split_to_rows_empty_occurrence_yields_parent_only_record() {
        // An occurrence with no content (`<Item></Item>`) still fans out to
        // its own record — one carrying just the parent fields.
        let xml =
            r#"<Root><Order><id>1</id><Item><name>A</name></Item><Item></Item></Order></Root>"#;
        let config = split_config("Root/Order", vec![split("Item")]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        assert_eq!(r1.get("Item.name"), Some(&Value::String("A".into())));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("id"), Some(&Value::Integer(1)));
        assert_eq!(r2.get("Item.name"), None);
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn split_to_rows_absent_element_passes_record_through_by_default() {
        // XML cannot distinguish an empty repetition from an absent element,
        // and `keep_empty` defaults to true, so the record is emitted
        // unchanged rather than vanishing.
        let xml = r#"<Root><Order><id>1</id></Order></Root>"#;
        let config = split_config("Root/Order", vec![split("Item")]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn split_to_rows_keep_empty_false_drops_the_recordless_parent() {
        // Opting out is the only way to lose the record — the inverse of the
        // default, and never the default.
        let xml = r#"<Root><Order><id>1</id></Order></Root>"#;
        let entry = SplitToRows {
            keep_empty: false,
            ..SplitToRows::bare("Item")
        };
        let config = split_config("Root/Order", vec![entry]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn split_to_rows_matches_a_dotted_nested_field() {
        // The declared field is the repeated element's dotted path relative
        // to the record element, matching the flattened field names.
        let xml = r#"<Root><Order><id>1</id><Items><Item><name>A</name></Item><Item><name>B</name></Item></Items></Order></Root>"#;
        let config = split_config("Root/Order", vec![split("Items.Item")]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("Items.Item.name"), Some(&Value::String("A".into())));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("Items.Item.name"), Some(&Value::String("B".into())));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn xml_undeclared_repeated_element_is_a_loud_error() {
        // A child element that repeats while its column is named by neither a
        // fan-out nor a `multiple:` declaration is refused loudly: keeping the
        // first value and dropping the rest is silent data loss. `<Dup>`
        // repeats here; `<Tag>` is fanned out so it never repeats per record.
        let xml = r#"<Root><Row><Dup>x</Dup><Dup>y</Dup><Tag>a</Tag><Tag>b</Tag></Row></Root>"#;
        let config = split_config("Root/Row", vec![split("Tag")]);
        let mut r = reader_from_str(xml, config);

        // Schema inference is infallible (it only reads the column names), so
        // the loud error is deferred to record assembly in `next_record` — this
        // is what lets the executor dead-letter a first-record repeat instead of
        // aborting during its eager pre-loop `schema` call.
        let _s = r
            .schema()
            .expect("schema inference does not assemble records");
        let err = r
            .next_record()
            .expect_err("undeclared repeat must fail loud");
        assert!(
            matches!(err, FormatError::UndeclaredRepeatedField { format: "XML", ref field } if field == "Dup"),
            "names the offending field: {err:?}"
        );
        assert!(
            err.is_document_structural(),
            "dead-letterable, not fatal-only"
        );
    }

    #[test]
    fn xml_repeated_self_closing_empty_element_is_not_an_error() {
        // Repeated value-less self-closing elements must behave exactly like
        // their empty-body twins: they contribute no field, so a non-`multiple:`
        // column that merely appears twice as `<x/>` is NOT a data-loss repeat
        // and must not be rejected. (Regression guard for the self-closing /
        // empty-body asymmetry adjacent to issue #920.)
        let xml = r#"<Root><Row><id>1</id><middle_name/><middle_name/></Row></Root>"#;
        let config = XmlReaderConfig {
            record_path: Some("Root/Row".into()),
            ..Default::default()
        };
        let mut r = reader_from_str(xml, config);
        let rec = r
            .next_record()
            .expect("repeated empty self-closing element must not error")
            .expect("one record");
        assert_eq!(rec.get("id"), Some(&Value::Integer(1)));
        // The value-less element yields no field at all, matching `<x></x>`.
        assert_eq!(rec.get("middle_name"), None);
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn self_closing_only_column_is_present_in_the_inferred_schema() {
        // A column that appears ONLY as a value-less self-closing `<x/>` in the
        // first record pushes no field, but it must not vanish from the inferred
        // schema — otherwise the column is silently absent for the whole source.
        // Its bare presence is recorded for schema inference without a repeated
        // `<x/><x/>` tripping the undeclared-repeat guard.
        let xml = r#"<Root><Row><id>1</id><middle_name/></Row></Root>"#;
        let config = XmlReaderConfig {
            record_path: Some("Root/Row".into()),
            ..Default::default()
        };
        let mut r = reader_from_str(xml, config);
        let s = r.schema().unwrap();
        assert!(
            s.columns().iter().any(|c| &**c == "middle_name"),
            "self-closing-only column must stay in the schema: {:?}",
            s.columns()
        );
        assert!(s.columns().iter().any(|c| &**c == "id"));
    }

    #[test]
    fn repeated_self_closing_only_column_is_present_and_does_not_error() {
        // The presence contribution must not resurrect the undeclared-repeat
        // false positive: a column appearing only as a repeated `<x/><x/>` on a
        // non-`multiple:` schema still surfaces in the inferred schema AND
        // assembles a record without a loud error.
        let xml = r#"<Root><Row><id>1</id><middle_name/><middle_name/></Row></Root>"#;
        let config = XmlReaderConfig {
            record_path: Some("Root/Row".into()),
            ..Default::default()
        };
        let mut r = reader_from_str(xml, config);
        let s = r.schema().unwrap();
        assert!(s.columns().iter().any(|c| &**c == "middle_name"));
        let rec = r
            .next_record()
            .expect("repeated self-closing must not error")
            .expect("one record");
        assert_eq!(rec.get("id"), Some(&Value::Integer(1)));
    }

    #[test]
    fn xml_undeclared_repeat_without_any_fan_out_is_a_loud_error() {
        // A bare reproduction: no fan-out at all, a plainly repeated
        // element on a column that is not `multiple:`. The first
        // `next_record` surfaces the loud error rather than a first-wins record.
        let xml = r#"<Root><Row><Dup>x</Dup><Dup>y</Dup></Row></Root>"#;
        let config = XmlReaderConfig {
            record_path: Some("Root/Row".into()),
            ..Default::default()
        };
        let mut r = reader_from_str(xml, config);
        let err = r
            .next_record()
            .expect_err("undeclared repeat must fail loud");
        assert!(
            matches!(err, FormatError::UndeclaredRepeatedField { format: "XML", ref field } if field == "Dup"),
            "names the offending field: {err:?}"
        );
    }

    #[test]
    fn xml_declared_multiple_repeat_still_collects() {
        // The escape hatch: declaring the column `multiple: true` collects
        // every occurrence into an array, in document order — no error.
        let xml = r#"<Root><Row><Dup>x</Dup><Dup>y</Dup></Row></Root>"#;
        let config = multi_value_config("Root/Row", &["Dup"]);
        let mut r = reader_from_str(xml, config);
        let rec = r.next_record().unwrap().unwrap();
        assert_eq!(
            rec.get("Dup"),
            Some(&Value::Array(vec![
                Value::String("x".into()),
                Value::String("y".into()),
            ]))
        );
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn multiple_column_survives_an_unrelated_fan_out() {
        // The collected array lands on every record the fan-out produced.
        let xml = r#"<Root><Order><id>1</id><Item><name>A</name></Item><Item><name>B</name></Item><Tag>x</Tag><Tag>y</Tag></Order></Root>"#;
        let config = XmlReaderConfig {
            multi_value_fields: vec!["Tag".into()],
            ..split_config("Root/Order", vec![split("Item")])
        };
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let tags = Value::Array(vec![Value::String("x".into()), Value::String("y".into())]);
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("Item.name"), Some(&Value::String("A".into())));
        assert_eq!(r1.get("Tag"), Some(&tags));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("Item.name"), Some(&Value::String("B".into())));
        assert_eq!(r2.get("Tag"), Some(&tags));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn empty_occurrence_inside_extract_fan_out_keeps_its_array_slot() {
        // A `multiple:` column INSIDE a `split_to_rows: extract` group: the raw
        // occurrences are `Item.tag`, projected to `tag` (the group prefix is
        // lifted off). An empty middle `<tag/>` must survive as a positional null
        // in the collected array, exactly as its valued siblings survive — the
        // empty-occurrence path must classify on the projected name (`tag`), not
        // the raw path (`Item.tag`), which `multi_value_fields` never holds.
        let xml = r#"<Root><Order><Item><tag>a</tag><tag/><tag>b</tag></Item></Order></Root>"#;
        let config = XmlReaderConfig {
            multi_value_fields: vec!["tag".into()],
            ..split_config("Root/Order", vec![extract("Item")])
        };
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(
            r1.get("tag"),
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::Null,
                Value::String("b".into()),
            ])),
            "empty middle occurrence keeps its positional null inside the fan-out"
        );
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn empty_occurrence_inside_split_fan_out_keeps_its_array_slot() {
        // Under `split` mode the occurrence keeps its dotted name (`Item.tag`), so
        // that is the projected/physical name the schema declares `multiple:`. The
        // empty middle occurrence must still be a positional null under that name.
        let xml = r#"<Root><Order><Item><tag>a</tag><tag/><tag>b</tag></Item></Order></Root>"#;
        let config = XmlReaderConfig {
            multi_value_fields: vec!["Item.tag".into()],
            ..split_config("Root/Order", vec![split("Item")])
        };
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(
            r1.get("Item.tag"),
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::Null,
                Value::String("b".into()),
            ])),
            "split mode keeps the dotted name and the positional null"
        );
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn empty_occurrence_at_top_level_keeps_its_array_slot() {
        // The top-level case (no fan-out): raw name == projected name, so this
        // path already worked. Pinned so the fan-out projection change cannot
        // regress the simplest case.
        let xml = r#"<Root><Row><tag>a</tag><tag/><tag>b</tag></Row></Root>"#;
        let config = multi_value_config("Root/Row", &["tag"]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(
            r1.get("tag"),
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::Null,
                Value::String("b".into()),
            ]))
        );
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn two_fan_out_fields_multiply() {
        // Two declared fan-out fields multiply, mirroring the JSON reader's
        // sequential fan-out: every A occurrence pairs with every B
        // occurrence.
        let xml = r#"<Root><Order><A>1</A><A>2</A><B>x</B><B>y</B></Order></Root>"#;
        let config = split_config("Root/Order", vec![split("A"), split("B")]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let mut pairs = Vec::new();
        while let Some(rec) = r.next_record().unwrap() {
            pairs.push((rec.get("A").cloned(), rec.get("B").cloned()));
        }
        let expected: Vec<(Option<Value>, Option<Value>)> =
            [(1, "x"), (1, "y"), (2, "x"), (2, "y")]
                .into_iter()
                .map(|(a, b)| (Some(Value::Integer(a)), Some(Value::String(b.into()))))
                .collect();
        assert_eq!(pairs, expected);
    }

    #[test]
    fn cartesian_fan_out_retains_occurrences_not_output_product() {
        let occurrence_count = 64;
        let mut xml = String::from("<Root><Order><id>1</id>");
        for i in 0..occurrence_count {
            xml.push_str(&format!("<A><a>{i}</a></A>"));
        }
        for i in 0..occurrence_count {
            xml.push_str(&format!("<B><b>{i}</b></B>"));
        }
        xml.push_str("</Order></Root>");
        let config = split_config("Root/Order", vec![split("A"), split("B")]);
        let mut reader = reader_from_str(&xml, config);
        reader.schema().unwrap();

        let first = reader.next_record().unwrap().unwrap();
        assert_eq!(first.get("A.a"), Some(&Value::Integer(0)));
        assert_eq!(first.get("B.b"), Some(&Value::Integer(0)));
        let cursor = reader.pending.as_ref().expect("remaining expansion");
        assert_eq!(cursor.frames.len(), 2);
        assert_eq!(
            cursor
                .frames
                .iter()
                .map(|frame| frame.occurrences.len())
                .sum::<usize>(),
            occurrence_count * 2,
            "cursor retains the radix inputs, not their product"
        );

        let mut rows = 1;
        while reader.next_record().unwrap().is_some() {
            rows += 1;
        }
        assert_eq!(rows, occurrence_count * occurrence_count);
    }

    #[test]
    fn fan_out_limit_emits_exact_ceiling_then_rejects_original_input() {
        let xml = concat!(
            "<Root><Order><id>7</id>",
            "<A><a>0</a></A><A><a>1</a></A>",
            "<B><b>0</b></B><B><b>1</b></B><B><b>2</b></B>",
            "</Order></Root>",
        );
        let mut config = split_config("Root/Order", vec![split("A"), split("B")]);
        config.max_output_rows_per_input = 4;
        let mut reader = reader_from_str(xml, config);
        reader
            .schema()
            .expect("schema inference is not a fan-out attempt");

        let mut pairs = Vec::new();
        for _ in 0..4 {
            let record = reader.next_record().unwrap().unwrap();
            pairs.push((record.get("A.a").cloned(), record.get("B.b").cloned()));
        }
        assert_eq!(
            pairs,
            vec![
                (Some(Value::Integer(0)), Some(Value::Integer(0))),
                (Some(Value::Integer(0)), Some(Value::Integer(1))),
                (Some(Value::Integer(0)), Some(Value::Integer(2))),
                (Some(Value::Integer(1)), Some(Value::Integer(0))),
            ]
        );

        let error = reader.next_record().unwrap_err();
        let FormatError::FanOutLimit(failure) = error else {
            panic!("expected structured fan-out limit failure, got {error}");
        };
        assert_eq!(failure.field, "B");
        assert_eq!(failure.limit, 4);
        assert_eq!(failure.actual, 5);
        let Value::Array(original) = failure.original_record else {
            panic!("XML rejection must retain ordered raw field occurrences");
        };
        assert!(original.len() >= 6);
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn zero_fan_out_limit_keeps_the_full_product() {
        let xml = "<Root><Order><A>0</A><A>1</A><B>0</B><B>1</B><B>2</B></Order></Root>";
        let mut config = split_config("Root/Order", vec![split("A"), split("B")]);
        config.max_output_rows_per_input = 0;
        let mut reader = reader_from_str(xml, config);
        reader.schema().unwrap();
        let mut rows = 0;
        while reader.next_record().unwrap().is_some() {
            if let Some(cursor) = &reader.pending {
                assert!(
                    cursor.failure.is_none(),
                    "an unlimited source must not retain a DLQ copy of its input"
                );
            }
            rows += 1;
        }
        assert_eq!(rows, 6);
    }

    #[test]
    fn fan_out_spans_multiple_record_elements() {
        // The expansion queue drains per record element: two Orders with two
        // Items each yield four records, in document order.
        let xml = r#"<Root>
            <Order><id>1</id><Item><name>A</name></Item><Item><name>B</name></Item></Order>
            <Order><id>2</id><Item><name>C</name></Item><Item><name>D</name></Item></Order>
        </Root>"#;
        let config = split_config("Root/Order", vec![split("Item")]);
        let mut r = reader_from_str(xml, config);
        let _s = r.schema().unwrap();

        let mut rows = Vec::new();
        while let Some(rec) = r.next_record().unwrap() {
            rows.push((rec.get("id").cloned(), rec.get("Item.name").cloned()));
        }
        let expected: Vec<(Option<Value>, Option<Value>)> =
            [(1, "A"), (1, "B"), (2, "C"), (2, "D")]
                .into_iter()
                .map(|(id, name)| (Some(Value::Integer(id)), Some(Value::String(name.into()))))
                .collect();
        assert_eq!(rows, expected);
    }

    #[test]
    fn nested_field_paths_are_detected_by_the_shared_predicate() {
        use crate::multi_value::under_field_path;

        // The disjointness rule the plan-time gate (E358) enforces reads the
        // same predicate the reader's occurrence tracking relies on.
        assert!(under_field_path("Item.part", "Item"));
        assert!(under_field_path("Item", "Item"));
        assert!(!under_field_path("Items", "Item"));
        assert!(!under_field_path("Other.Item", "Item"));
    }

    #[test]
    fn test_xml_cdata_handling() {
        let xml = r#"<Root><Row><content><![CDATA[some & content]]></content></Row></Root>"#;
        let mut r = reader_from_str(xml, default_config_with_path("Root/Row"));
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(
            r1.get("content"),
            Some(&Value::String("some & content".into()))
        );
    }

    #[test]
    fn test_xml_empty_no_records() {
        let xml = r#"<Root><Orders></Orders></Root>"#;
        let mut r = reader_from_str(xml, default_config_with_path("Root/Orders/Item"));
        let s = r.schema().unwrap();
        assert_eq!(s.columns().len(), 0);
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_xml_reader_emits_per_record_schema() {
        // Each emitted record carries the actual element/attribute
        // names present on its XML element. The dispatch-layer
        // `CoercingReader` applies the per-Source `OnUnmapped` policy
        // against the user-declared schema.
        let xml = r#"<Root>
            <Items>
                <Item><id>1</id><name>Alice</name></Item>
                <Item><id>2</id><name>Bob</name><bonus>flagged</bonus></Item>
            </Items>
        </Root>"#;
        let mut r = reader_from_str(xml, default_config_with_path("Root/Items/Item"));
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("name"), Some(&Value::String("Bob".into())));
        assert_eq!(r2.get("bonus"), Some(&Value::String("flagged".into())));
    }

    #[test]
    fn test_xml_reads_entities_and_char_refs_in_element_text() {
        // Predefined entities and character references in element text decode
        // to their characters. quick-xml emits each reference as its own event
        // separate from the surrounding text; the reader reassembles the whole
        // text node before decoding, so a value split across references is
        // recovered intact rather than truncated at the first fragment.
        let xml = r#"<Root><Item><amp>a &amp; b</amp><lt>&lt;tag&gt;</lt><num>&#65;&#66;</num></Item></Root>"#;
        let mut r = reader_from_str(xml, default_config_with_path("Root/Item"));
        let _s = r.schema().unwrap();
        let rec = r.next_record().unwrap().unwrap();
        assert_eq!(rec.get("amp"), Some(&Value::String("a & b".into())));
        assert_eq!(rec.get("lt"), Some(&Value::String("<tag>".into())));
        assert_eq!(rec.get("num"), Some(&Value::String("AB".into())));
    }

    #[test]
    fn test_xml_text_trims_source_whitespace_but_preserves_reference_whitespace() {
        // Text-node whitespace trimming applies to the source bytes, not the
        // decoded value: literal leading/trailing whitespace is trimmed, but
        // whitespace that surrounds — or is produced by — a reference is kept.
        let xml = concat!(
            "<Root><Item>",
            "<pad>  hello  </pad>",       // literal edge whitespace trimmed
            "<around>x &amp; y</around>", // spaces around an entity preserved
            "<charws>&#32;hi</charws>",   // leading space from a char ref preserved
            "</Item></Root>",
        );
        let mut r = reader_from_str(xml, default_config_with_path("Root/Item"));
        let _s = r.schema().unwrap();
        let rec = r.next_record().unwrap().unwrap();
        assert_eq!(rec.get("pad"), Some(&Value::String("hello".into())));
        assert_eq!(rec.get("around"), Some(&Value::String("x & y".into())));
        assert_eq!(rec.get("charws"), Some(&Value::String(" hi".into())));
    }

    #[test]
    fn test_xml_attribute_preserves_literal_whitespace() {
        // Attribute decoding resolves references only; it does not apply XML
        // attribute-value whitespace normalization, so a literal newline or tab
        // inside a value is preserved rather than collapsed to a space.
        let xml = "<Root><Item title=\"Hello\nWorld\" tab=\"a\tb\"/></Root>";
        let mut r = reader_from_str(xml, default_config_with_path("Root/Item"));
        let _s = r.schema().unwrap();
        let rec = r.next_record().unwrap().unwrap();
        assert_eq!(
            rec.get("@title"),
            Some(&Value::String("Hello\nWorld".into()))
        );
        assert_eq!(rec.get("@tab"), Some(&Value::String("a\tb".into())));
    }

    #[test]
    fn test_xml_attribute_resolves_entities_and_char_refs() {
        // Predefined entities and character references in an attribute value
        // decode to their characters, matching the prior `unescape_value`.
        let xml = r#"<Root><Item note="a &amp; b &#65;"/></Root>"#;
        let mut r = reader_from_str(xml, default_config_with_path("Root/Item"));
        let _s = r.schema().unwrap();
        let rec = r.next_record().unwrap().unwrap();
        assert_eq!(rec.get("@note"), Some(&Value::String("a & b A".into())));
    }

    #[test]
    fn test_xml_unknown_entity_in_text_errors() {
        // An unrecognized general entity in element text is rejected, matching
        // the strict decoding the reader applied before the parser upgrade.
        // Schema inference reads the first record eagerly, so the error may
        // surface from either `schema` or `next_record`.
        let xml = r#"<Root><Item><v>&nope;</v></Item></Root>"#;
        let mut r = reader_from_str(xml, default_config_with_path("Root/Item"));
        let outcome = match r.schema() {
            Err(e) => Err(e),
            Ok(_) => r.next_record(),
        };
        assert!(matches!(outcome, Err(FormatError::Xml(_))));
    }
}
