//! Streaming JSON reader with auto-detect (array/NDJSON/object), schema inference,
//! nested flattening, and the source's multi-value declarations
//! (`split_to_rows` fan-out, `split_values` in-cell parsing, and schema-level
//! `multiple:` collection).
//!
//! **All modes stream with O(1 record) memory, with no whole-file buffer held
//! for a re-openable (file) source:**
//! - NDJSON: line-by-line from a freshly re-opened `BufReader`.
//! - Array / record_path: a [`JsonArrayStream`] navigates to the target array
//!   over a second freshly re-opened reader and yields one element at a time —
//!   never collecting the array into a `Vec`.
//!
//! Envelope-aware sources run a streaming pre-scan over the document before any
//! body record emits: it walks the JSON once over its own freshly re-opened
//! reader, deserializing ONLY the subtrees the declared `$doc.*` paths name
//! (every other key is parsed-and-skipped via `IgnoredAny`) into a path-pruned
//! arena index capped by `max_index_bytes`, rather than materializing the whole
//! document tree. The pre-scan and the body each open their own [`Read`] from
//! the [`ReopenableSource`], so neither consumes the other and no shared
//! whole-file byte buffer is retained for file-backed inputs. A pathless input
//! (a test cursor, the `<inline>`/`<empty>` slot, a REST body) is held as a
//! small `ReopenableSource::Buffered` — the honest one-shot fallback, bounded
//! because such inputs are small by construction.

use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;

use clinker_record::{
    DEFAULT_DATE_FORMATS, DEFAULT_DATETIME_FORMATS, Record, Schema, SchemaBuilder, Value,
    coerce_to_bool, coerce_to_date, coerce_to_datetime, coerce_to_float, coerce_to_int,
    coerce_to_string,
};
use indexmap::IndexMap;

use cxl::analyzer::doc_paths::DocPath;

use crate::bom::UTF8_BOM;
use crate::doc_index::DocArenaIndex;
use crate::envelope::{EnvelopeConfig, EnvelopeExtract, EnvelopeFieldType};
use crate::error::FormatError;
use crate::json::body_stream::{JsonArrayStream, is_json_ws};
use crate::json::streaming::{SectionTarget, extract_sections};
use crate::multi_value::{SplitToRows, SplitToRowsMode, SplitValues};
use crate::source::{ReopenableSource, SourceIdentity};
use crate::traits::FormatReader;

// ── Public config types ──────────────────────────────────────────────

#[derive(Default)]
pub struct JsonReaderConfig {
    pub format: Option<JsonMode>,
    pub record_path: Option<String>,
    /// Fields the source schema declares `multiple: true`, by physical name.
    /// A declared field is always array-valued: an array passes through, and a
    /// scalar is normalized to a one-element array.
    pub multi_value_fields: Vec<String>,
    /// Fan-out declarations, applied in declaration order — so two entries
    /// multiply, exactly as two nested loops would.
    pub split_to_rows: Vec<SplitToRows>,
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
    /// retain. The cap is charged incrementally as each section subtree is
    /// retained and fires mid-build (before OOM). `None` disables the cap;
    /// the source plumbing supplies a finite default.
    pub max_index_bytes: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
pub enum JsonMode {
    Array,
    Ndjson,
    Object,
}

// ── JsonReader ───────────────────────────────────────────────────────

pub struct JsonReader {
    inner: InnerReader,
    schema: Option<Arc<Schema>>,
    config: JsonReaderConfig,
    pending: Vec<serde_json::Map<String, serde_json::Value>>,
    /// The re-openable byte source. Body iteration and the envelope pre-scan
    /// each open their own fresh [`Read`] from it, so no whole-file buffer is
    /// held for a file-backed (`ReopenableSource::Path`) source.
    source: ReopenableSource,
    /// Content identity of the bytes the body open read, captured at
    /// construction. The envelope pre-scan re-opens the source and confirms it
    /// sees the same content, so a path-backed input rewritten between the two
    /// passes fails loud instead of splicing a stale envelope onto a new body.
    body_identity: SourceIdentity,
}

enum InnerReader {
    /// NDJSON: line-by-line from a re-opened `BufReader`. O(1 record) memory.
    Ndjson {
        reader: BufReader<Box<dyn Read + Send>>,
        line_buf: String,
    },
    /// Array or record_path: one element at a time from a lazy
    /// [`JsonArrayStream`] over a re-opened reader. Never collects the array.
    Array(JsonArrayStream),
    /// Exhausted (empty document).
    Done,
}

impl JsonReader {
    /// Build a reader over a re-openable byte source.
    ///
    /// Streaming, O(1 record): the body opens one fresh [`Read`] from `source`
    /// (and the envelope pre-scan opens a second), so a file-backed source is
    /// never buffered whole. Auto-detect peeks the first non-whitespace byte
    /// from a fresh open to choose array vs. NDJSON.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError`] if the source cannot be opened, the first byte
    /// is not a valid JSON start, or `format: object` is set without a
    /// `record_path`.
    pub fn from_source(
        source: ReopenableSource,
        config: JsonReaderConfig,
    ) -> Result<Self, FormatError> {
        // JSON runs two passes (envelope pre-scan + body stream), so the source
        // must be re-openable. A `Path`/`Buffered` source passes through; a
        // pathless `OneShot` is buffered here, on the reader-building thread —
        // bounded because such inputs are small.
        let source = source.into_reopenable().map_err(FormatError::Io)?;
        let (inner, body_identity) = Self::init(&source, &config)?;
        Ok(JsonReader {
            inner,
            schema: None,
            config,
            pending: Vec::new(),
            source,
            body_identity,
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
    /// Returns [`FormatError`] on a read failure or the same parse errors as
    /// [`from_source`](Self::from_source).
    pub fn from_reader<R: Read + Send + 'static>(
        reader: R,
        config: JsonReaderConfig,
    ) -> Result<Self, FormatError> {
        let source = ReopenableSource::buffer(reader).map_err(FormatError::Io)?;
        Self::from_source(source, config)
    }

    /// Open a fresh `BufReader` from the source with a leading UTF-8 BOM
    /// stripped, returning the content-identity snapshot of the bytes it reads.
    /// Each pass (body, pre-scan) re-opens, so the strip happens per open rather
    /// than once over a shared buffer; the identity lets a later pass detect the
    /// input changing between passes.
    fn open_buf(
        source: &ReopenableSource,
    ) -> Result<(BufReader<Box<dyn Read + Send>>, SourceIdentity), FormatError> {
        let (reader, identity) = source.open_with_identity().map_err(FormatError::Io)?;
        let mut buf = BufReader::new(reader);
        strip_leading_bom(&mut buf)?;
        Ok((buf, identity))
    }

    /// Build the body reader and return it alongside the content-identity of
    /// the bytes it opened, so the envelope pre-scan can verify it re-opens the
    /// same content.
    fn init(
        source: &ReopenableSource,
        config: &JsonReaderConfig,
    ) -> Result<(InnerReader, SourceIdentity), FormatError> {
        // record_path → navigate then stream the named array lazily.
        if let Some(ref rp) = config.record_path {
            let path_segments: Vec<String> = rp.split('.').map(String::from).collect();
            let (buf, identity) = Self::open_buf(source)?;
            return Ok((
                InnerReader::Array(JsonArrayStream::at_path(buf, &path_segments)?),
                identity,
            ));
        }

        // Explicit format.
        if let Some(mode) = config.format {
            return match mode {
                JsonMode::Array => {
                    let (buf, identity) = Self::open_buf(source)?;
                    Ok((
                        InnerReader::Array(JsonArrayStream::top_level(buf)?),
                        identity,
                    ))
                }
                JsonMode::Ndjson => {
                    let (reader, identity) = Self::open_buf(source)?;
                    Ok((
                        InnerReader::Ndjson {
                            reader,
                            line_buf: String::new(),
                        },
                        identity,
                    ))
                }
                JsonMode::Object => Err(FormatError::Json(
                    "format: object requires record_path".into(),
                )),
            };
        }

        // Auto-detect from the first non-whitespace byte.
        let (mut buf, identity) = Self::open_buf(source)?;
        let inner = match peek_first_byte(&mut buf)? {
            Some(b'[') => InnerReader::Array(JsonArrayStream::top_level(buf)?),
            Some(b'{') => InnerReader::Ndjson {
                reader: buf,
                line_buf: String::new(),
            },
            Some(b) => {
                return Err(FormatError::Json(format!(
                    "cannot auto-detect: unexpected byte '{}' (0x{b:02x})",
                    b as char
                )));
            }
            None => InnerReader::Done,
        };
        Ok((inner, identity))
    }

    fn next_raw(&mut self) -> Result<Option<serde_json::Value>, FormatError> {
        match &mut self.inner {
            InnerReader::Array(stream) => stream.next(),
            InnerReader::Ndjson { reader, line_buf } => loop {
                line_buf.clear();
                let n = reader.read_line(line_buf).map_err(FormatError::Io)?;
                if n == 0 {
                    return Ok(None);
                }
                let trimmed = line_buf.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let val: serde_json::Value =
                    serde_json::from_str(trimmed).map_err(|e| FormatError::Json(e.to_string()))?;
                return Ok(Some(val));
            },
            InnerReader::Done => Ok(None),
        }
    }

    fn flatten_value(
        prefix: &str,
        value: &serde_json::Value,
        out: &mut serde_json::Map<String, serde_json::Value>,
        depth: usize,
    ) {
        const MAX_DEPTH: usize = 64;
        if depth > MAX_DEPTH {
            out.insert(
                prefix.to_string(),
                serde_json::Value::String("[max depth]".into()),
            );
            return;
        }
        match value {
            serde_json::Value::Object(map) => {
                for (key, val) in map {
                    let name = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{prefix}.{key}")
                    };
                    Self::flatten_value(&name, val, out, depth + 1);
                }
            }
            other => {
                out.insert(prefix.to_string(), other.clone());
            }
        }
    }

    /// Expand one flattened record through the declared `split_to_rows`
    /// fields, applied in declaration order: each emits one output per array
    /// element, duplicating every other field onto each.
    ///
    /// A record whose field holds an empty array, or carries no such field at
    /// all, survives under the default `keep_empty: true` — several widely
    /// deployed engines drop it instead, and a vanished row is the costliest
    /// failure mode for this audience.
    ///
    /// A field present but NOT holding an array is one occurrence, projected
    /// exactly as a single-element array would be. JSON producers routinely
    /// unwrap a one-element array, and XML — where a document cannot say
    /// "array" at all — fans a lone repeated element out the same way, so the
    /// same logical feed must not come out differently on the two paths.
    fn apply_split_to_rows(
        &self,
        flat: serde_json::Map<String, serde_json::Value>,
    ) -> Vec<serde_json::Map<String, serde_json::Value>> {
        if self.config.split_to_rows.is_empty() {
            return vec![flat];
        }

        let mut result = vec![flat];
        for entry in &self.config.split_to_rows {
            let mut next = Vec::new();
            for mut rec in result {
                // The lone occurrence a non-array value contributes, lifted out
                // of the record so the projection below can put it back under
                // the mode's rules. An object occurrence has no value to read:
                // `flatten_value` dissolved it into dotted child keys before
                // this ran, and reading that as an absent field would treat a
                // populated group as an empty one.
                let single = match rec.get(&entry.field) {
                    Some(serde_json::Value::Array(_)) => None,
                    Some(_) => rec.shift_remove(&entry.field),
                    None => take_dissolved_object(&mut rec, &entry.field),
                };
                if let Some(elem) = single {
                    project_element(&mut rec, entry, &elem);
                    if let Some(column) = &entry.position_column {
                        rec.insert(column.clone(), 1.into());
                    }
                    next.push(rec);
                    continue;
                }
                match rec.get(&entry.field) {
                    Some(serde_json::Value::Array(arr)) if !arr.is_empty() => {
                        for (position, elem) in arr.iter().enumerate() {
                            let mut r = rec.clone();
                            // `shift_remove`, not `remove`: with
                            // `preserve_order` the latter is a swap-remove that
                            // would drag the last column into the vacated slot,
                            // scrambling column order across fanned-out
                            // siblings that must share one shape.
                            r.shift_remove(&entry.field);
                            project_element(&mut r, entry, elem);
                            if let Some(column) = &entry.position_column {
                                r.insert(column.clone(), (position + 1).into());
                            }
                            next.push(r);
                        }
                    }
                    // An empty array or an absent field: nothing to fan out.
                    _ => {
                        if entry.keep_empty {
                            let mut r = rec;
                            r.shift_remove(&entry.field);
                            next.push(r);
                        }
                    }
                }
            }
            result = next;
        }
        result
    }

    /// Apply the schema-level `multiple:` declarations and the `split_values`
    /// in-cell parse to one flattened record, so every declared multi-value
    /// field arrives at the record boundary array-valued.
    fn apply_multi_value(&self, flat: &mut serde_json::Map<String, serde_json::Value>) {
        for entry in &self.config.split_values {
            if let Some(value) = flat.get_mut(&entry.field) {
                *value = split_text_value(value, &entry.delimiter);
            }
        }
        for field in &self.config.multi_value_fields {
            if let Some(value) = flat.get_mut(field)
                && !value.is_array()
            {
                *value = serde_json::Value::Array(vec![value.take()]);
            }
        }
    }

    /// Builds a Record carrying the JSON object's actual keys (per-record
    /// schema). Each record's `Arc<Schema>` reflects exactly the keys
    /// present in that record — the per-Source `OnUnmapped` policy at
    /// the dispatch layer reconciles records against the user-declared
    /// schema (probing for `auto_widen`, rejecting on `reject`, or
    /// silently dropping on `drop`).
    fn map_to_record(
        &self,
        mut flat: serde_json::Map<String, serde_json::Value>,
    ) -> Result<Record, FormatError> {
        self.apply_multi_value(&mut flat);
        let columns: Vec<Box<str>> = flat.keys().map(|k| k.clone().into_boxed_str()).collect();
        let schema = Arc::new(Schema::new(columns));
        let values: Vec<Value> = flat.values().map(json_to_value).collect();
        Ok(Record::new(schema, values))
    }
}

impl FormatReader for JsonReader {
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

        // Resolve each declared section the index wants to its JSON pointer,
        // validating the extract type fits a JSON source (a wrong-format
        // extract is an authoring error surfaced eagerly). Sections the
        // index does not want are dropped here so the streaming pass never
        // descends into them.
        let mut targets: Vec<SectionTarget> = Vec::new();
        for (name, section) in &config.sections {
            if !index.wants_section(name) {
                continue;
            }
            let pointer = match &section.extract {
                EnvelopeExtract::JsonPointer(p) => {
                    // A slashless non-empty pointer decodes to zero segments,
                    // identical to the whole-document pointer `""`, and would
                    // silently match the root instead of the intended section.
                    // Plan validation rejects this at authoring time; guard
                    // here too so any caller that bypasses the plan (a direct
                    // reader construction) fails loud rather than matching the
                    // wrong node.
                    if !p.is_empty() && !p.starts_with('/') {
                        return Err(FormatError::Json(format!(
                            "envelope section {name:?}: `json_pointer` {p:?} is not a valid \
                             RFC 6901 pointer — a pointer must be empty (the whole document) or \
                             start with `/`."
                        )));
                    }
                    p.as_str()
                }
                EnvelopeExtract::XmlPath(_) => {
                    return Err(FormatError::Json(format!(
                        "envelope section {name:?}: declared `xml_path` extract \
                         against a JSON source. Use `json_pointer` for JSON envelope sections."
                    )));
                }
                EnvelopeExtract::Segment(_) | EnvelopeExtract::RecordType(_) => {
                    return Err(FormatError::Json(format!(
                        "envelope section {name:?}: declared a flat-file extract \
                         (`segment` / `record_type`) against a JSON source. Those \
                         extracts are for flat-file formats (EDIFACT, multi-record \
                         CSV / fixed-width); use `json_pointer` for JSON."
                    )));
                }
            };
            targets.push(SectionTarget::new(name.clone(), pointer));
        }

        // Single streaming pass over a freshly re-opened reader: only the
        // matched subtrees are built; every other key is parsed-and-skipped.
        // The cap is charged *as each declared section is constructed*, so an
        // oversized declared section aborts the parse mid-build rather than
        // after the whole subtree materializes. Body iteration opens its own
        // independent reader, so this pass does not consume it and no shared
        // whole-file buffer is held.
        //
        // Confirm the pre-scan re-opens the same content the body opened. A
        // path-backed input replaced or truncated between the two opens (an
        // external producer re-emitting mid-run) would otherwise splice this
        // envelope onto a body parsed from different bytes; the `(len, mtime)`
        // identity check fails loud instead.
        //
        // This is a cheap courtesy guard under the finite-batch input-stability
        // contract, not a fingerprint: it does not detect a same-length,
        // same-mtime-tick in-place rewrite, nor a rewrite landing after this
        // point while the body streams lazily through `next_record` (the check
        // runs once, here). Closing those would require hashing the whole file,
        // reintroducing the buffer this reader removes. See `SourceIdentity`.
        let (mut prescan, prescan_identity) = Self::open_buf(&self.source)?;
        prescan_identity
            .ensure_matches(&self.body_identity)
            .map_err(FormatError::Io)?;
        let matched = extract_sections(&mut prescan, &targets, self.config.max_index_bytes)?;

        // Coerce each matched subtree to its declared field schema and retain
        // it in the index, which accounts the coerced (field-filtered)
        // retained bytes against the same cap. The streaming pass already
        // bounded the raw parse; the index accounts what is actually kept.
        for (name, section) in &config.sections {
            let raw = match matched.get(name) {
                Some(v) => v,
                None => continue,
            };
            let payload_obj = match raw {
                serde_json::Value::Object(obj) => obj,
                other => {
                    return Err(FormatError::Json(format!(
                        "envelope section {name:?}: JSON pointer resolves to {kind} but \
                         envelope sections must be JSON objects",
                        kind = json_value_kind(other),
                    )));
                }
            };
            let typed = coerce_json_section_fields(name, payload_obj, &section.fields, &index)?;
            let path = doc_path_for_section(name);
            index
                .insert(&path, Value::Map(Box::new(typed)))
                .map_err(FormatError::Json)?;
        }
        Ok(index.into_sections())
    }

    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        if let Some(ref s) = self.schema {
            return Ok(Arc::clone(s));
        }

        // Read forward until a raw record actually expands to something. A
        // `keep_empty: false` entry drops a record whose field is empty, and
        // inferring the schema from that dropped expansion would cache a
        // column-less schema for the whole source while records kept flowing.
        let expanded = loop {
            let Some(raw) = self.next_raw()? else {
                let s = SchemaBuilder::new().build();
                self.schema = Some(Arc::clone(&s));
                self.inner = InnerReader::Done;
                return Ok(s);
            };
            let mut flat = serde_json::Map::new();
            Self::flatten_value("", &raw, &mut flat, 0);
            let expanded = self.apply_split_to_rows(flat);
            if !expanded.is_empty() {
                break expanded;
            }
        };

        let schema = expanded
            .first()
            .expect("non-empty checked above")
            .keys()
            .map(|k| k.clone().into_boxed_str())
            .collect::<SchemaBuilder>()
            .build();
        self.schema = Some(Arc::clone(&schema));
        self.pending = expanded;
        Ok(schema)
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        if self.schema.is_none() {
            self.schema()?;
        }

        if !self.pending.is_empty() {
            let flat = self.pending.remove(0);
            return Ok(Some(self.map_to_record(flat)?));
        }

        loop {
            let raw = match self.next_raw()? {
                Some(v) => v,
                None => return Ok(None),
            };
            let mut flat = serde_json::Map::new();
            Self::flatten_value("", &raw, &mut flat, 0);
            let expanded = self.apply_split_to_rows(flat);
            if expanded.is_empty() {
                continue;
            }
            let mut expanded = expanded.into_iter();
            let first = expanded.next().expect("non-empty checked above");
            let record = self.map_to_record(first)?;
            self.pending = expanded.collect();
            return Ok(Some(record));
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

/// Reassemble the object a fan-out field held, from the dotted child keys
/// [`JsonReader::flatten_value`] dissolved it into, removing them from the
/// record. `None` when the record carries no such child — the field really is
/// absent.
///
/// The children are themselves already flattened, so the reassembled object is
/// one level deep and re-flattening it under any prefix restores exactly the
/// keys taken out.
fn take_dissolved_object(
    rec: &mut serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Option<serde_json::Value> {
    let prefix = format!("{field}.");
    let keys: Vec<String> = rec
        .keys()
        .filter(|k| k.starts_with(&prefix))
        .cloned()
        .collect();
    if keys.is_empty() {
        return None;
    }
    let mut object = serde_json::Map::with_capacity(keys.len());
    for key in keys {
        let value = rec.shift_remove(&key).expect("key just collected");
        object.insert(key[prefix.len()..].to_string(), value);
    }
    Some(serde_json::Value::Object(object))
}

/// Write one fan-out element onto its output record.
///
/// Under [`SplitToRowsMode::Extract`] the element becomes the record: an object
/// element's own keys are flattened onto the output at top level, so the parent
/// fields already there are simply merged around them. Under
/// [`SplitToRowsMode::Split`] the record shape is preserved: the element is
/// flattened back under the declared field name. A scalar element has no keys
/// to lift, so both modes key it by the field name — the only name it has.
fn project_element(
    out: &mut serde_json::Map<String, serde_json::Value>,
    entry: &SplitToRows,
    elem: &serde_json::Value,
) {
    match (entry.mode, elem) {
        (SplitToRowsMode::Extract, serde_json::Value::Object(_)) => {
            JsonReader::flatten_value("", elem, out, 0);
        }
        (SplitToRowsMode::Extract, other) => {
            let name = entry
                .field
                .rsplit('.')
                .next()
                .unwrap_or(entry.field.as_str());
            out.insert(name.to_string(), other.clone());
        }
        (SplitToRowsMode::Split, _) => {
            JsonReader::flatten_value(&entry.field, elem, out, 0);
        }
    }
}

/// Parse a delimited cell into the several values a `multiple:` column holds.
///
/// A string becomes an array of its delimiter-separated parts; an array (a
/// field that both repeats and carries delimited text) splits each string
/// element and flattens the result, so the two declarations compose instead of
/// fighting. Empty parts are preserved — an author who declared the delimiter
/// is the authority on what sits between two of them. A non-string scalar has
/// no text to split and is wrapped as a single value.
fn split_text_value(value: &serde_json::Value, delimiter: &str) -> serde_json::Value {
    fn parts(text: &str, delimiter: &str) -> Vec<serde_json::Value> {
        text.split(delimiter)
            .map(|p| serde_json::Value::String(p.to_string()))
            .collect()
    }
    match value {
        serde_json::Value::String(s) => serde_json::Value::Array(parts(s, delimiter)),
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .flat_map(|item| match item {
                    serde_json::Value::String(s) => parts(s, delimiter),
                    other => vec![other.clone()],
                })
                .collect(),
        ),
        other => serde_json::Value::Array(vec![other.clone()]),
    }
}

fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::String(n.to_string().into())
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone().into()),
        serde_json::Value::Array(arr) => Value::Array(arr.iter().map(json_to_value).collect()),
        serde_json::Value::Object(obj) => {
            let mut map: IndexMap<Box<str>, Value> = IndexMap::with_capacity(obj.len());
            for (k, val) in obj {
                map.insert(k.as_str().into(), json_to_value(val));
            }
            Value::Map(Box::new(map))
        }
    }
}

/// Build the section-level [`DocPath`] under which a whole matched section
/// subtree is retained.
///
/// JSON retains an envelope section as one object (one JSON pointer → one
/// object → all its fields), so the insert key is the section, not an
/// individual field; [`DocArenaIndex::insert`] groups by `path.section`.
/// The `field`/`indices` axes carry no meaning for a section-granular
/// retention and are left empty.
fn doc_path_for_section(name: &str) -> DocPath {
    DocPath {
        section: name.into(),
        field: Box::from(""),
        indices: Vec::new(),
    }
}

/// Lowercase descriptor for the JSON value kind, used in
/// envelope-section diagnostics when the pointer resolves to a
/// non-object value.
fn json_value_kind(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// Coerce a JSON object's fields into the section's declared schema.
/// Unknown fields are dropped silently (the schema is the contract);
/// missing fields drop out (CXL eval maps to `Value::Null`); type
/// mismatch raises a format error citing the section + field +
/// observed JSON value.
///
/// A declared field no program reads is skipped before coercion: a
/// wide-schema section referenced by a single field coerces only that field,
/// so an unread (and possibly malformed) sibling field is never parsed or
/// type-checked. `index` is the retention authority that knows the read set
/// (see [`DocArenaIndex::wants_field`]).
fn coerce_json_section_fields(
    section_name: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
    schema: &IndexMap<String, EnvelopeFieldType>,
    index: &DocArenaIndex,
) -> Result<IndexMap<Box<str>, Value>, FormatError> {
    let mut out: IndexMap<Box<str>, Value> = IndexMap::with_capacity(schema.len());
    for (field, ty) in schema {
        if !index.wants_field(section_name, field) {
            continue;
        }
        let json_val = match obj.get(field.as_str()) {
            Some(v) if !v.is_null() => v,
            _ => continue,
        };
        let intermediate = json_to_value(json_val);
        let coerced = match ty {
            EnvelopeFieldType::String => coerce_to_string(&intermediate),
            EnvelopeFieldType::Int => coerce_to_int(&intermediate),
            EnvelopeFieldType::Float => coerce_to_float(&intermediate),
            EnvelopeFieldType::Bool => coerce_to_bool(&intermediate),
            EnvelopeFieldType::Date => coerce_to_date(&intermediate, DEFAULT_DATE_FORMATS),
            EnvelopeFieldType::DateTime => {
                coerce_to_datetime(&intermediate, DEFAULT_DATETIME_FORMATS)
            }
        }
        .map_err(|e| {
            FormatError::Json(format!(
                "envelope section {section_name:?} field {field:?} (declared type {ty:?}): \
                 cannot coerce JSON value {json_val}: {e}"
            ))
        })?;
        out.insert(Box::from(field.as_str()), coerced);
    }
    Ok(out)
}

fn peek_first_byte(
    reader: &mut BufReader<Box<dyn Read + Send>>,
) -> Result<Option<u8>, FormatError> {
    loop {
        let buf = reader.fill_buf().map_err(FormatError::Io)?;
        if buf.is_empty() {
            return Ok(None);
        }
        // RFC 8259 whitespace only, so a leading form-feed before the value is
        // a structural byte the auto-detect rejects, not silently skipped —
        // matching `serde_json` and the body scanner.
        if let Some(pos) = buf.iter().position(|b| !is_json_ws(*b)) {
            return Ok(Some(buf[pos]));
        }
        let len = buf.len();
        reader.consume(len);
    }
}

/// Consume a single leading UTF-8 BOM from a freshly opened reader, if present.
///
/// Each pass re-opens its own `Read`, so a Windows-authored file (Excel /
/// PowerShell utf8 export) carries the BOM on every open; stripping it here
/// clears the marker for body iteration, the NDJSON line scan, and the
/// envelope pre-scan alike. The `BufReader`'s default capacity exceeds the
/// 3-byte BOM, so the marker is always wholly inside the first fill.
fn strip_leading_bom(reader: &mut BufReader<Box<dyn Read + Send>>) -> Result<(), FormatError> {
    let buf = reader.fill_buf().map_err(FormatError::Io)?;
    if buf.starts_with(&UTF8_BOM) {
        reader.consume(UTF8_BOM.len());
    }
    Ok(())
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn reader_from_str(input: &str, config: JsonReaderConfig) -> JsonReader {
        JsonReader::from_reader(std::io::Cursor::new(input.as_bytes().to_vec()), config).unwrap()
    }

    /// Builds a reader over `input` prefixed with a UTF-8 BOM, mimicking
    /// a Windows-authored JSON file (Excel / PowerShell utf8 export).
    fn reader_from_str_with_bom(input: &str, config: JsonReaderConfig) -> JsonReader {
        let mut bytes = UTF8_BOM.to_vec();
        bytes.extend_from_slice(input.as_bytes());
        JsonReader::from_reader(std::io::Cursor::new(bytes), config).unwrap()
    }

    fn default_config() -> JsonReaderConfig {
        JsonReaderConfig::default()
    }

    /// `(section name, JSON pointer, [(field name, type)])` for one envelope section.
    type SectionSpec<'a> = (&'a str, &'a str, &'a [(&'a str, EnvelopeFieldType)]);

    fn envelope_config(sections: &[SectionSpec]) -> EnvelopeConfig {
        use crate::envelope::EnvelopeSection;
        let mut cfg = EnvelopeConfig::default();
        for (name, pointer, fields) in sections {
            let mut field_map = IndexMap::new();
            for (fname, ftype) in *fields {
                field_map.insert((*fname).to_string(), *ftype);
            }
            cfg.sections.insert(
                (*name).to_string(),
                EnvelopeSection {
                    extract: EnvelopeExtract::JsonPointer((*pointer).to_string()),
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

    /// Declared `$doc.*` paths covering every `(section, field)` in `specs`,
    /// so the path-pruned index wants all of them — the runtime stand-in
    /// for the planner's per-source attribution.
    fn declared_paths(specs: &[SectionSpec]) -> Vec<DocPath> {
        let mut out = Vec::new();
        for (section, _pointer, fields) in specs {
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

    /// A reader config whose declared paths want every section in `specs`,
    /// for an envelope-bearing source with the given `record_path` body.
    fn envelope_reader_config(specs: &[SectionSpec], record_path: &str) -> JsonReaderConfig {
        JsonReaderConfig {
            record_path: Some(record_path.into()),
            declared_doc_paths: declared_paths(specs),
            ..Default::default()
        }
    }

    #[test]
    fn prepare_document_extracts_arbitrary_named_sections() {
        let json = r#"{
            "BatchInfo": {"batch_id": "RUN-001", "count": 42},
            "records": [{"x": 1}, {"x": 2}],
            "Summary": {"hash": "abc", "processed": 2}
        }"#;
        let specs: &[SectionSpec] = &[
            (
                "BatchInfo",
                "/BatchInfo",
                &[
                    ("batch_id", EnvelopeFieldType::String),
                    ("count", EnvelopeFieldType::Int),
                ],
            ),
            (
                "Summary",
                "/Summary",
                &[
                    ("hash", EnvelopeFieldType::String),
                    ("processed", EnvelopeFieldType::Int),
                ],
            ),
        ];
        let cfg = envelope_config(specs);
        let mut reader = reader_from_str(json, envelope_reader_config(specs, "records"));
        let sections = reader.prepare_document(&cfg).expect("envelope pre-scan");

        assert_eq!(sections.len(), 2);
        let head = unwrap_section_map(sections.get("BatchInfo").unwrap());
        assert_eq!(head.get("batch_id"), Some(&Value::String("RUN-001".into())));
        assert_eq!(head.get("count"), Some(&Value::Integer(42)));

        let foot = unwrap_section_map(sections.get("Summary").unwrap());
        assert_eq!(foot.get("hash"), Some(&Value::String("abc".into())));
        assert_eq!(foot.get("processed"), Some(&Value::Integer(2)));

        // Envelope pre-scan does not consume body iteration; body
        // records still stream from byte 0.
        let r1 = reader.next_record().unwrap().unwrap();
        assert_eq!(r1.get("x"), Some(&Value::Integer(1)));
        let r2 = reader.next_record().unwrap().unwrap();
        assert_eq!(r2.get("x"), Some(&Value::Integer(2)));
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn prepare_document_empty_config_returns_empty() {
        let mut reader = reader_from_str(r#"[{"a":1}]"#, default_config());
        let sections = reader.prepare_document(&EnvelopeConfig::default()).unwrap();
        assert!(sections.is_empty());
    }

    /// A config wanting a single section named `Bad`, so the wrong-format
    /// extract validation fires for that section in the rejection tests.
    fn config_wanting_bad_section() -> JsonReaderConfig {
        JsonReaderConfig {
            declared_doc_paths: vec![DocPath {
                section: "Bad".into(),
                field: "any".into(),
                indices: Vec::new(),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn prepare_document_rejects_xml_path_extract() {
        use crate::envelope::EnvelopeSection;
        let mut cfg = EnvelopeConfig::default();
        cfg.sections.insert(
            "Bad".into(),
            EnvelopeSection {
                extract: EnvelopeExtract::XmlPath("/doc/Bad".into()),
                fields: IndexMap::new(),
            },
        );
        let mut reader = reader_from_str(r#"{"records":[]}"#, config_wanting_bad_section());
        let err = reader.prepare_document(&cfg).unwrap_err();
        assert!(matches!(err, FormatError::Json(msg) if msg.contains("xml_path")));
    }

    #[test]
    fn prepare_document_rejects_segment_extract() {
        use crate::envelope::EnvelopeSection;
        let mut cfg = EnvelopeConfig::default();
        cfg.sections.insert(
            "Bad".into(),
            EnvelopeSection {
                extract: EnvelopeExtract::Segment("UNB".into()),
                fields: IndexMap::new(),
            },
        );
        let mut reader = reader_from_str(r#"{"records":[]}"#, config_wanting_bad_section());
        let err = reader.prepare_document(&cfg).unwrap_err();
        assert!(matches!(err, FormatError::Json(msg) if msg.contains("segment")));
    }

    #[test]
    fn prepare_document_non_object_pointer_errors() {
        // A pointer that resolves to a non-object value is a structural
        // misconfiguration — envelope sections are object payloads of
        // typed fields.
        let json = r#"{"value": 42, "records": []}"#;
        let specs: &[SectionSpec] = &[("Val", "/value", &[("v", EnvelopeFieldType::Int)])];
        let cfg = envelope_config(specs);
        let mut reader = reader_from_str(json, envelope_reader_config(specs, "records"));
        let err = reader.prepare_document(&cfg).unwrap_err();
        assert!(matches!(err, FormatError::Json(msg) if msg.contains("number")));
    }

    #[test]
    fn prepare_document_missing_pointer_yields_no_entry() {
        let json = r#"{"records": [{"x":1}]}"#;
        let specs: &[SectionSpec] =
            &[("Trailer", "/trailer", &[("count", EnvelopeFieldType::Int)])];
        let cfg = envelope_config(specs);
        let mut reader = reader_from_str(json, envelope_reader_config(specs, "records"));
        let sections = reader.prepare_document(&cfg).unwrap();
        assert!(sections.is_empty());
    }

    #[test]
    fn prepare_document_coerces_typed_fields_from_json() {
        let json = r#"{
            "Meta": {"run_date": "2026-05-22", "enabled": true, "ratio": 0.5},
            "records": [{"x":1}]
        }"#;
        let specs: &[SectionSpec] = &[(
            "Meta",
            "/Meta",
            &[
                ("run_date", EnvelopeFieldType::Date),
                ("enabled", EnvelopeFieldType::Bool),
                ("ratio", EnvelopeFieldType::Float),
            ],
        )];
        let cfg = envelope_config(specs);
        let mut reader = reader_from_str(json, envelope_reader_config(specs, "records"));
        let sections = reader.prepare_document(&cfg).unwrap();
        let meta = unwrap_section_map(sections.get("Meta").unwrap());
        assert!(matches!(meta.get("run_date"), Some(Value::Date(_))));
        assert_eq!(meta.get("enabled"), Some(&Value::Bool(true)));
        assert_eq!(meta.get("ratio"), Some(&Value::Float(0.5)));
    }

    #[test]
    fn test_json_autodetect_array() {
        let mut r = reader_from_str(r#"[{"a":1},{"a":2}]"#, default_config());
        let s = r.schema().unwrap();
        assert_eq!(s.columns().len(), 1);
        assert_eq!(&*s.columns()[0], "a");
        assert_eq!(
            r.next_record().unwrap().unwrap().get("a"),
            Some(&Value::Integer(1))
        );
        assert_eq!(
            r.next_record().unwrap().unwrap().get("a"),
            Some(&Value::Integer(2))
        );
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_json_autodetect_ndjson() {
        let mut r = reader_from_str("{\"a\":1}\n{\"a\":2}\n", default_config());
        let s = r.schema().unwrap();
        assert_eq!(s.columns().len(), 1);
        assert_eq!(
            r.next_record().unwrap().unwrap().get("a"),
            Some(&Value::Integer(1))
        );
        assert_eq!(
            r.next_record().unwrap().unwrap().get("a"),
            Some(&Value::Integer(2))
        );
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_json_array_strips_leading_bom() {
        // Auto-detect must see `[`, not the leading `0xEF` of the BOM,
        // and the parse must be identical to BOM-less input.
        let mut r = reader_from_str_with_bom(r#"[{"a":1},{"a":2}]"#, default_config());
        let s = r.schema().unwrap();
        assert_eq!(s.columns().len(), 1);
        assert_eq!(&*s.columns()[0], "a");
        assert_eq!(
            r.next_record().unwrap().unwrap().get("a"),
            Some(&Value::Integer(1))
        );
        assert_eq!(
            r.next_record().unwrap().unwrap().get("a"),
            Some(&Value::Integer(2))
        );
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_json_ndjson_strips_leading_bom() {
        // `str::trim` does not remove `U+FEFF`, so without the source
        // strip the first NDJSON line would fail `serde_json::from_str`.
        let mut r = reader_from_str_with_bom("{\"a\":1}\n{\"a\":2}\n", default_config());
        let s = r.schema().unwrap();
        assert_eq!(s.columns().len(), 1);
        assert_eq!(
            r.next_record().unwrap().unwrap().get("a"),
            Some(&Value::Integer(1))
        );
        assert_eq!(
            r.next_record().unwrap().unwrap().get("a"),
            Some(&Value::Integer(2))
        );
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_json_bom_matches_bomless() {
        // Full parse equivalence across a multi-field array: a
        // BOM-prefixed document and its BOM-less twin emit identical
        // records.
        let input = r#"[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]"#;
        let collect = |mut r: JsonReader| {
            let _ = r.schema().unwrap();
            let mut rows = Vec::new();
            while let Some(rec) = r.next_record().unwrap() {
                rows.push((rec.get("id").cloned(), rec.get("name").cloned()));
            }
            rows
        };
        let plain = collect(reader_from_str(input, default_config()));
        let bom = collect(reader_from_str_with_bom(input, default_config()));
        assert_eq!(plain, bom);
        assert_eq!(plain.len(), 2);
    }

    #[test]
    fn test_json_envelope_prescan_strips_leading_bom() {
        // The envelope pre-scan re-opens its own reader from the source; the
        // per-open BOM strip must clear the marker for that path too, not just
        // body iteration.
        let json = r#"{
            "BatchInfo": {"batch_id": "RUN-001", "count": 42},
            "records": [{"x": 1}, {"x": 2}]
        }"#;
        let specs: &[SectionSpec] = &[(
            "BatchInfo",
            "/BatchInfo",
            &[
                ("batch_id", EnvelopeFieldType::String),
                ("count", EnvelopeFieldType::Int),
            ],
        )];
        let cfg = envelope_config(specs);
        let mut reader = reader_from_str_with_bom(json, envelope_reader_config(specs, "records"));
        let sections = reader.prepare_document(&cfg).expect("envelope pre-scan");
        let head = unwrap_section_map(sections.get("BatchInfo").unwrap());
        assert_eq!(head.get("batch_id"), Some(&Value::String("RUN-001".into())));
        assert_eq!(head.get("count"), Some(&Value::Integer(42)));

        let r1 = reader.next_record().unwrap().unwrap();
        assert_eq!(r1.get("x"), Some(&Value::Integer(1)));
        let r2 = reader.next_record().unwrap().unwrap();
        assert_eq!(r2.get("x"), Some(&Value::Integer(2)));
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn test_json_record_path_navigation() {
        let input = r#"{"metadata":{"v":1},"data":{"results":[{"x":1},{"x":2}]}}"#;
        let config = JsonReaderConfig {
            record_path: Some("data.results".into()),
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let s = r.schema().unwrap();
        assert_eq!(&*s.columns()[0], "x");
        assert_eq!(
            r.next_record().unwrap().unwrap().get("x"),
            Some(&Value::Integer(1))
        );
        assert_eq!(
            r.next_record().unwrap().unwrap().get("x"),
            Some(&Value::Integer(2))
        );
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_json_record_path_skips_large_siblings() {
        // The "big_blob" key has a large value that should be skipped via IgnoredAny
        // without buffering. We verify the reader navigates past it to "target".
        let big = "x".repeat(10_000);
        let input = format!(r#"{{"big_blob":"{big}","target":[{{"id":1}},{{"id":2}}]}}"#);
        let config = JsonReaderConfig {
            record_path: Some("target".into()),
            ..default_config()
        };
        let mut r = reader_from_str(&input, config);
        let _s = r.schema().unwrap();
        assert_eq!(
            r.next_record().unwrap().unwrap().get("id"),
            Some(&Value::Integer(1))
        );
        assert_eq!(
            r.next_record().unwrap().unwrap().get("id"),
            Some(&Value::Integer(2))
        );
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn split_to_rows_extract_hoists_object_element_keys() {
        // The default mode makes the element the record: its keys land at
        // top level and the parent's fields merge onto each output.
        let input = r#"[{"name":"Alice","orders":[{"id":1},{"id":2}]}]"#;
        let config = JsonReaderConfig {
            split_to_rows: vec![SplitToRows::bare("orders")],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("id"), Some(&Value::Integer(2)));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn split_to_rows_split_keeps_the_record_shape() {
        // `mode: split` preserves the record shape: the element stays under
        // the declared field name rather than being lifted out of it.
        let input = r#"[{"name":"Alice","orders":[{"id":1},{"id":2}]}]"#;
        let config = JsonReaderConfig {
            split_to_rows: vec![SplitToRows {
                mode: SplitToRowsMode::Split,
                ..SplitToRows::bare("orders")
            }],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(r1.get("orders.id"), Some(&Value::Integer(1)));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("orders.id"), Some(&Value::Integer(2)));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn split_to_rows_position_column_numbers_the_elements() {
        let input = r#"[{"name":"Alice","tags":["x","y"]}]"#;
        let config = JsonReaderConfig {
            split_to_rows: vec![SplitToRows {
                position_column: Some("tag_no".into()),
                ..SplitToRows::bare("tags")
            }],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("tags"), Some(&Value::String("x".into())));
        assert_eq!(r1.get("tag_no"), Some(&Value::Integer(1)));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("tags"), Some(&Value::String("y".into())));
        assert_eq!(r2.get("tag_no"), Some(&Value::Integer(2)));
    }

    #[test]
    fn multiple_keeps_the_array_and_normalizes_a_lone_scalar() {
        // A `multiple: true` column is always array-valued: an array passes
        // through, and a scalar that arrived where several values were
        // declared is wrapped rather than silently differing in shape.
        let input = r#"[{"tags":["a","b","c"]},{"tags":"solo"}]"#;
        let config = JsonReaderConfig {
            multi_value_fields: vec!["tags".into()],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        assert_eq!(
            r.next_record().unwrap().unwrap().get("tags"),
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::String("b".into()),
                Value::String("c".into()),
            ]))
        );
        assert_eq!(
            r.next_record().unwrap().unwrap().get("tags"),
            Some(&Value::Array(vec![Value::String("solo".into())]))
        );
    }

    #[test]
    fn split_values_parses_a_delimited_cell_into_several_values() {
        let input = r#"[{"name":"Alice","tags":"a;b;c"}]"#;
        let config = JsonReaderConfig {
            multi_value_fields: vec!["tags".into()],
            split_values: vec![SplitValues::bare("tags")],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        assert_eq!(
            r.next_record().unwrap().unwrap().get("tags"),
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::String("b".into()),
                Value::String("c".into()),
            ]))
        );
    }

    #[test]
    fn multiple_on_an_absent_or_empty_field_leaves_the_record_intact() {
        // Neither shape suppresses the record: an empty array stays an empty
        // array, and an absent field simply has no column for the
        // declared-schema reprojection to fill.
        let input = r#"[{"id":1,"tags":[]},{"id":2}]"#;
        let config = JsonReaderConfig {
            multi_value_fields: vec!["tags".into()],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        assert_eq!(r1.get("tags"), Some(&Value::Array(vec![])));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("id"), Some(&Value::Integer(2)));
        assert_eq!(r2.get("tags"), None);
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn split_values_on_an_absent_field_leaves_the_record_intact() {
        let input = r#"[{"id":1}]"#;
        let config = JsonReaderConfig {
            multi_value_fields: vec!["tags".into()],
            split_values: vec![SplitValues::bare("tags")],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        assert_eq!(r1.get("tags"), None);
    }

    #[test]
    fn split_to_rows_fans_a_scalar_field_out_as_one_occurrence() {
        // A producer that unwraps its one-element arrays still means one
        // occurrence, so the record is projected exactly as `["solo"]` would
        // be — under `extract` the scalar keeps the path's last segment.
        let input = r#"[{"id":1,"orders":"solo"}]"#;
        let config = JsonReaderConfig {
            split_to_rows: vec![SplitToRows::bare("orders")],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        assert_eq!(r1.get("orders"), Some(&Value::String("solo".into())));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn split_to_rows_fans_a_single_object_field_out_as_one_occurrence() {
        // A single object under the declared name is flattened away before the
        // fan-out sees it (`orders.sku`). Reading that as an absent field would
        // treat a populated group as an empty one: under `keep_empty: false`
        // the record would vanish, and under the default it would keep a
        // column no fanned-out sibling has.
        let input = r#"[{"id":1,"orders":{"sku":"x"}}]"#;
        let config = JsonReaderConfig {
            split_to_rows: vec![SplitToRows::bare("orders")],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        assert_eq!(r1.get("sku"), Some(&Value::String("x".into())));
        assert_eq!(r1.get("orders.sku"), None);
        assert!(r.next_record().unwrap().is_none());
    }

    /// The same feed, one order with two line items and one with a single
    /// unwrapped object, must produce the same column set on every record —
    /// and the same shape the array form produces.
    #[test]
    fn split_to_rows_gives_an_unwrapped_object_the_same_columns_as_an_array() {
        let input =
            r#"[{"id":1,"orders":[{"sku":"a"},{"sku":"b"}]},{"id":2,"orders":{"sku":"c"}}]"#;
        let config = JsonReaderConfig {
            split_to_rows: vec![SplitToRows {
                position_column: Some("line_no".into()),
                ..SplitToRows::bare("orders")
            }],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        let mut seen = Vec::new();
        while let Some(rec) = r.next_record().unwrap() {
            seen.push((
                rec.schema()
                    .columns()
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<_>>(),
                rec.get("sku").cloned(),
                rec.get("line_no").cloned(),
            ));
        }
        assert_eq!(seen.len(), 3, "two fanned-out lines plus the unwrapped one");
        let columns = &seen[0].0;
        for (i, row) in seen.iter().enumerate() {
            assert_eq!(&row.0, columns, "record {i} must share one column set");
        }
        assert_eq!(seen[2].1, Some(Value::String("c".into())));
        assert_eq!(seen[2].2, Some(Value::Integer(1)));
    }

    #[test]
    fn split_to_rows_keep_empty_false_drops_a_record_with_no_occurrence() {
        // `keep_empty: false` governs the absent field and the empty array
        // alike; a record whose field is genuinely missing carries no
        // occurrence to fan out and is dropped as asked.
        let input = r#"[{"id":1,"orders":[{"sku":"x"}]},{"id":2},{"id":3,"orders":[]}]"#;
        let config = JsonReaderConfig {
            split_to_rows: vec![SplitToRows {
                keep_empty: false,
                ..SplitToRows::bare("orders")
            }],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        let mut ids = Vec::new();
        while let Some(rec) = r.next_record().unwrap() {
            ids.push(rec.get("id").cloned());
        }
        assert_eq!(ids, vec![Some(Value::Integer(1))]);
    }

    #[test]
    fn split_to_rows_preserves_column_order_around_the_fanned_out_field() {
        // The fan-out removes the array field and appends the element's keys;
        // the columns that surrounded it must keep their relative order rather
        // than being swapped into the vacated slot.
        let input = r#"[{"a":1,"orders":[{"sku":"x"}],"z":9}]"#;
        let config = JsonReaderConfig {
            split_to_rows: vec![SplitToRows::bare("orders")],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let s = r.schema().unwrap();
        let columns: Vec<&str> = s.columns().iter().map(|c| &**c).collect();
        assert_eq!(columns, ["a", "z", "sku"]);
    }

    #[test]
    fn split_to_rows_keep_empty_false_still_infers_a_schema() {
        // The first record is dropped by `keep_empty: false`, so schema
        // inference must read forward rather than caching a column-less schema
        // for a source that goes on emitting records.
        let input = r#"[{"name":"Alice","orders":[]},{"name":"Bob","orders":[{"id":1}]}]"#;
        let config = JsonReaderConfig {
            split_to_rows: vec![SplitToRows {
                keep_empty: false,
                ..SplitToRows::bare("orders")
            }],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let s = r.schema().unwrap();
        let columns: Vec<&str> = s.columns().iter().map(|c| &**c).collect();
        assert_eq!(columns, ["name", "id"]);
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("name"), Some(&Value::String("Bob".into())));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn split_to_rows_extract_names_a_scalar_element_by_the_fields_last_segment() {
        // Matches the XML reader for the same declaration: `extract` lifts the
        // prefix off, so a dotted field's scalar elements land under its last
        // segment and one declared schema serves both formats.
        let input = r#"[{"a":{"tags":["x","y"]}}]"#;
        let config = JsonReaderConfig {
            split_to_rows: vec![SplitToRows::bare("a.tags")],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("tags"), Some(&Value::String("x".into())));
        assert_eq!(r1.get("a.tags"), None);
    }

    #[test]
    fn split_values_honors_a_declared_delimiter() {
        let input = r#"[{"tags":"a|b"}]"#;
        let config = JsonReaderConfig {
            multi_value_fields: vec!["tags".into()],
            split_values: vec![SplitValues {
                field: "tags".into(),
                delimiter: "|".into(),
            }],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        assert_eq!(
            r.next_record().unwrap().unwrap().get("tags"),
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::String("b".into()),
            ]))
        );
    }

    #[test]
    fn test_json_schema_inference_types() {
        let input = r#"[{"i":42,"f":2.5,"s":"hello","b":true,"n":null}]"#;
        let mut r = reader_from_str(input, default_config());
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("i"), Some(&Value::Integer(42)));
        assert_eq!(r1.get("f"), Some(&Value::Float(2.5)));
        assert_eq!(r1.get("s"), Some(&Value::String("hello".into())));
        assert_eq!(r1.get("b"), Some(&Value::Bool(true)));
        assert_eq!(r1.get("n"), Some(&Value::Null));
    }

    #[test]
    fn test_json_nested_object_flattening() {
        let mut r = reader_from_str(r#"[{"a":{"b":{"c":1}}}]"#, default_config());
        let s = r.schema().unwrap();
        assert_eq!(&*s.columns()[0], "a.b.c");
        assert_eq!(
            r.next_record().unwrap().unwrap().get("a.b.c"),
            Some(&Value::Integer(1))
        );
    }

    #[test]
    fn test_json_emits_per_record_schema() {
        // Each emitted record carries the actual keys present in its
        // JSON object — the per-record `Arc<Schema>` reflects exactly
        // what was parsed. The dispatch-layer `CoercingReader` then
        // applies the per-Source `OnUnmapped` policy (drop/reject)
        // against the user-declared schema.
        let mut r = reader_from_str("{\"a\":1}\n{\"a\":2,\"b\":3}\n", default_config());
        let s = r.schema().unwrap();
        assert_eq!(s.columns().len(), 1);
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("a"), Some(&Value::Integer(1)));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("a"), Some(&Value::Integer(2)));
        assert_eq!(r2.get("b"), Some(&Value::Integer(3)));
    }

    #[test]
    fn test_json_empty_array() {
        let mut r = reader_from_str("[]", default_config());
        let s = r.schema().unwrap();
        assert_eq!(s.columns().len(), 0);
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_json_malformed_input() {
        let mut r = JsonReader::from_reader(
            std::io::Cursor::new(b"{invalid}".to_vec()),
            default_config(),
        )
        .unwrap();
        assert!(r.schema().is_err());
    }

    #[test]
    fn split_to_rows_empty_array_keeps_the_record_by_default() {
        // `keep_empty` defaults to true, so Alice's empty `orders` array
        // leaves her record intact rather than deleting her from the output.
        // Several widely deployed engines drop it instead; a vanished row is
        // the costliest failure mode for this audience, so the default is
        // deliberately inverted.
        let input = r#"[{"name":"Alice","orders":[]},{"name":"Bob","orders":[{"id":1}]}]"#;
        let config = JsonReaderConfig {
            split_to_rows: vec![SplitToRows::bare("orders")],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(r1.get("id"), None);
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("name"), Some(&Value::String("Bob".into())));
        assert_eq!(r2.get("id"), Some(&Value::Integer(1)));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn split_to_rows_absent_field_keeps_the_record_by_default() {
        // The complementary case: a record carrying no such field at all is
        // preserved just as an empty array is.
        let input = r#"[{"name":"Alice"},{"name":"Bob","orders":[{"id":1}]}]"#;
        let config = JsonReaderConfig {
            split_to_rows: vec![SplitToRows::bare("orders")],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        assert_eq!(
            r.next_record().unwrap().unwrap().get("name"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(
            r.next_record().unwrap().unwrap().get("id"),
            Some(&Value::Integer(1))
        );
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn split_to_rows_keep_empty_false_drops_the_empty_record() {
        // Opting out is the only way to lose the record.
        let input = r#"[{"name":"Alice","orders":[]},{"name":"Bob","orders":[{"id":1}]}]"#;
        let config = JsonReaderConfig {
            split_to_rows: vec![SplitToRows {
                keep_empty: false,
                ..SplitToRows::bare("orders")
            }],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("name"), Some(&Value::String("Bob".into())));
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        assert!(r.next_record().unwrap().is_none());
    }
}
