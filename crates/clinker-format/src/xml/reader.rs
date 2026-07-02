//! Streaming XML reader using quick-xml's pull parser.
//!
//! Navigates to `record_path`, extracts attributes with configurable prefix,
//! flattens nested elements with `.` separator, handles namespaces.
//!
//! **O(1 record) memory, no whole-document buffer:** the body walks the
//! document element-at-a-time from a freshly re-opened `BufReader` —
//! quick-xml's `read_event_into` pulls one event at a time, so only a single
//! record's `element_stack` plus the event buffer is live at once, never the
//! whole input.
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
use crate::error::FormatError;
use crate::source::{ReopenableSource, SourceIdentity};
use crate::traits::FormatReader;
use crate::xml::streaming::{SectionTarget, extract_sections};

/// XML reader configuration.
pub struct XmlReaderConfig {
    pub record_path: Option<String>,
    pub attribute_prefix: String,
    pub namespace_handling: NamespaceMode,
    pub array_paths: Vec<XmlArrayPath>,
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
            array_paths: vec![],
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

#[derive(Debug, Clone)]
pub struct XmlArrayPath {
    pub path: String,
    pub mode: XmlArrayMode,
    pub separator: String,
}

#[derive(Debug, Clone, Copy)]
pub enum XmlArrayMode {
    Explode,
    Join,
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
/// live record while the reader exists.
pub struct XmlReader {
    /// The re-openable byte source. Body iteration and the envelope pre-scan
    /// each open their own fresh [`Read`] from it, so no whole-document buffer
    /// is held for a file-backed (`ReopenableSource::Path`) source.
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
    /// Pending records from first-record schema inference.
    pending: Option<Record>,
    /// Whether we've finished all records.
    done: bool,
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
        // XML runs two passes (envelope pre-scan + body stream), so the source
        // must be re-openable. A `Path`/`Buffered` source passes through; a
        // pathless `OneShot` is buffered here, on the reader-building thread —
        // bounded because such inputs are small.
        let source = source.into_reopenable().map_err(FormatError::Io)?;
        let (parser, body_identity) = Self::open_body(&source)?;

        let path_segments: Vec<String> = config
            .record_path
            .as_deref()
            .map(|p| p.split('/').map(String::from).collect())
            .unwrap_or_default();

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
            done: false,
        })
    }

    /// Build a reader by buffering a one-shot `Read` into a re-openable source.
    ///
    /// For pathless inputs (test cursors, the `<inline>`/`<empty>` slots, REST
    /// bodies) that have no on-disk path to re-open: the bytes are captured
    /// once into a small `ReopenableSource::Buffered`. Bounded because such
    /// inputs are small by construction; file-backed sources use
    /// [`from_source`](Self::from_source) with `ReopenableSource::Path` instead
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
    fn read_next_record_raw(&mut self) -> Result<Option<Vec<(String, String)>>, FormatError> {
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
                                let fields = self.extract_record_fields(attrs)?;
                                return Ok(Some(fields));
                            }
                        } else {
                            self.skip_subtree()?;
                        }
                    } else {
                        let attrs = extract_attributes_static(&self.config.attribute_prefix, e)?;
                        let fields = self.extract_record_fields(attrs)?;
                        return Ok(Some(fields));
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
                            return Ok(Some(fields));
                        }
                    } else {
                        let fields = extract_attributes_static(&self.config.attribute_prefix, e)?;
                        return Ok(Some(fields));
                    }
                }
                Event::End(_) => {
                    self.xml_depth -= 1;
                    if self.matched_depth > 0 && self.xml_depth < self.matched_depth {
                        self.matched_depth -= 1;
                    }
                }
                Event::Eof => {
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

    /// Extract all fields from a record element (attributes + nested children).
    /// Uses a separate buffer to avoid borrow conflicts with self.parser.
    fn extract_record_fields(
        &mut self,
        start_attrs: Vec<(String, String)>,
    ) -> Result<Vec<(String, String)>, FormatError> {
        let mut fields = start_attrs;
        let record_depth = self.xml_depth;
        let mut element_stack: Vec<String> = Vec::new();
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
                flush_text_field(&mut fields, &element_stack, &mut text_run)?;
            }

            match event {
                Event::Start(ref e) => {
                    self.xml_depth += 1;
                    let name = elem_name_static(&self.config.namespace_handling, &e.name());
                    element_stack.push(name);
                    let child_attrs = extract_attributes_static(&self.config.attribute_prefix, e)?;
                    let prefix = element_stack.join(".");
                    for (key, val) in child_attrs {
                        fields.push((format!("{prefix}.{key}"), val));
                    }
                }
                Event::End(_) => {
                    self.xml_depth -= 1;
                    if self.xml_depth < record_depth {
                        break;
                    }
                    element_stack.pop();
                }
                Event::Empty(ref e) => {
                    let name = elem_name_static(&self.config.namespace_handling, &e.name());
                    let prefix = if element_stack.is_empty() {
                        name.clone()
                    } else {
                        format!("{}.{name}", element_stack.join("."))
                    };
                    fields.push((prefix.clone(), String::new()));
                    let child_attrs = extract_attributes_static(&self.config.attribute_prefix, e)?;
                    for (key, val) in child_attrs {
                        fields.push((format!("{prefix}.{key}"), val));
                    }
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
                        }
                    }
                }
                Event::Eof => {
                    self.done = true;
                    break;
                }
                _ => {}
            }
        }

        Ok(fields)
    }

    /// Skip an entire subtree (from current Start to its matching End).
    fn skip_subtree(&mut self) -> Result<(), FormatError> {
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
                    self.done = true;
                    return Ok(());
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
    fn fields_to_record(&self, fields: Vec<(String, String)>) -> Result<Record, FormatError> {
        let mut seen = std::collections::HashSet::new();
        let mut columns: Vec<Box<str>> = Vec::with_capacity(fields.len());
        let mut values: Vec<Value> = Vec::with_capacity(fields.len());
        for (key, val) in fields {
            if seen.insert(key.clone()) {
                columns.push(key.into_boxed_str());
                values.push(infer_value(&val));
            }
        }
        let schema = Arc::new(Schema::new(columns));
        Ok(Record::new(schema, values))
    }
}

impl FormatReader for XmlReader {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        if let Some(ref s) = self.schema {
            return Ok(Arc::clone(s));
        }

        let first = self.read_next_record_raw()?;
        let first = match first {
            Some(fields) => fields,
            None => {
                let s = SchemaBuilder::new().build();
                self.schema = Some(Arc::clone(&s));
                self.done = true;
                return Ok(s);
            }
        };

        // Infer schema from first record's field names (preserving order)
        let mut seen = std::collections::HashSet::new();
        let schema = first
            .iter()
            .filter_map(|(k, _)| {
                if seen.insert(k.clone()) {
                    Some(k.clone().into_boxed_str())
                } else {
                    None
                }
            })
            .collect::<SchemaBuilder>()
            .build();
        self.schema = Some(Arc::clone(&schema));

        // Buffer the first record
        self.pending = Some(self.fields_to_record(first)?);

        Ok(schema)
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        if self.schema.is_none() {
            self.schema()?;
        }

        if let Some(pending) = self.pending.take() {
            return Ok(Some(pending));
        }

        let fields = self.read_next_record_raw()?;
        match fields {
            Some(f) => Ok(Some(self.fields_to_record(f)?)),
            None => Ok(None),
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
            index.insert(&path, Value::Map(Box::new(typed)))?;
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

/// Resolve the current text run and push it, if non-empty, as a field keyed by
/// the innermost open element's dotted path. Text directly under the record
/// element (empty path) or an empty resolved value pushes nothing. Clears
/// `text_run` for the next node.
fn flush_text_field(
    fields: &mut Vec<(String, String)>,
    element_stack: &[String],
    text_run: &mut String,
) -> Result<(), FormatError> {
    let value = finalize_text_run(text_run)?;
    text_run.clear();
    if !value.is_empty() {
        let field_name = element_stack.join(".");
        if !field_name.is_empty() {
            fields.push((field_name, value));
        }
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

/// Simple type inference from string values (same rules as JSON).
fn infer_value(s: &str) -> Value {
    if s.is_empty() {
        return Value::Null;
    }
    if let Ok(i) = s.parse::<i64>() {
        return Value::Integer(i);
    }
    if let Ok(f) = s.parse::<f64>()
        && (s.contains('.') || s.contains('e') || s.contains('E'))
    {
        return Value::Float(f);
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

    #[test]
    fn test_xml_array_paths_explode() {
        // Repeating <Item> elements — each becomes a separate field (accumulated)
        // For true explode, we'd need array_paths config. For now, test that
        // repeating elements produce the last value (simple case).
        let xml = r#"<Root><Order><id>1</id><Item><name>A</name></Item><Item><name>B</name></Item></Order></Root>"#;
        let mut r = reader_from_str(xml, default_config_with_path("Root/Order"));
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        // Both items write to "Item.name" — last one wins in the simple model
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        // Item.name will have one of the values (last write wins)
        assert!(r1.get("Item.name").is_some());
    }

    #[test]
    fn test_xml_array_paths_join() {
        // Multiple child elements with same name — values concatenated by last-write
        let xml = r#"<Root><Row><Tag>a</Tag><Tag>b</Tag><Tag>c</Tag></Row></Root>"#;
        let mut r = reader_from_str(xml, default_config_with_path("Root/Row"));
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        // With simple extraction, repeated "Tag" fields — last value wins
        assert!(r1.get("Tag").is_some());
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
