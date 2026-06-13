//! Streaming JSON reader with auto-detect (array/NDJSON/object), schema inference,
//! nested flattening, and array_paths explode/join.
//!
//! **All modes stream with O(1 record) memory:**
//! - NDJSON: line-by-line from BufReader.
//! - Array: `visit_seq` via DeserializeSeed — one element at a time.
//! - record_path: `DeserializeSeed` + `IgnoredAny` navigates to the target
//!   path, skipping non-matching keys without allocation, then streams the
//!   array. Total memory: ~10KB buffer + one record.
//!
//! Envelope-aware sources run a streaming pre-scan over the document
//! before any body record emits: it walks the JSON once, deserializing
//! ONLY the subtrees the declared `$doc.*` paths name (every other key is
//! parsed-and-skipped via `IgnoredAny`) into a path-pruned arena index
//! capped by `max_index_bytes`, rather than materializing the whole
//! document tree. The pre-scan reads from a fresh cursor over the shared
//! source buffer, so it does not consume body iteration.
//!
//! The source buffer itself is still a full `read_to_end` at construction
//! — it backs both the body parser's cursor and the pre-scan's cursor, so
//! there is one read from disk regardless of envelope declarations. Only
//! the pre-scan's parsed-tree materialization is removed here; shrinking
//! the raw byte buffer for purely-streamable bodies is a separate
//! follow-up.

use std::io::{BufRead, BufReader, Cursor, Read};
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
use crate::json::streaming::{SectionTarget, extract_sections};
use crate::traits::FormatReader;

// ── Public config types ──────────────────────────────────────────────

#[derive(Default)]
pub struct JsonReaderConfig {
    pub format: Option<JsonMode>,
    pub record_path: Option<String>,
    pub array_paths: Vec<ArrayPathSpec>,
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

#[derive(Debug, Clone)]
pub struct ArrayPathSpec {
    pub path: String,
    pub mode: ArrayPathMode,
    pub separator: String,
}

#[derive(Debug, Clone, Copy)]
pub enum ArrayPathMode {
    Explode,
    Join,
}

// ── JsonReader ───────────────────────────────────────────────────────

pub struct JsonReader {
    inner: InnerReader,
    schema: Option<Arc<Schema>>,
    config: JsonReaderConfig,
    pending: Vec<serde_json::Map<String, serde_json::Value>>,
    /// Buffered source bytes shared with the envelope pre-scan. The body
    /// parser drains a cursor over these bytes; `prepare_document` opens a
    /// fresh cursor over the same bytes and streams a single
    /// `DeserializeSeed` pass that retains only declared `$doc.*` subtrees,
    /// rather than re-parsing the whole document tree.
    raw_bytes: Arc<[u8]>,
}

enum InnerReader {
    /// NDJSON: line-by-line. O(1 record) memory.
    Ndjson {
        reader: BufReader<Box<dyn Read + Send>>,
        line_buf: String,
    },
    /// Array or record_path: records collected via streaming DeserializeSeed.
    /// Elements are deserialized one at a time and pushed to a Vec during the
    /// single-pass `deserialize` call, then yielded lazily via `pos`.
    Collected {
        records: Vec<serde_json::Value>,
        pos: usize,
    },
    /// Exhausted.
    Done,
}

impl JsonReader {
    pub fn from_reader<R: Read + Send + 'static>(
        mut reader: R,
        config: JsonReaderConfig,
    ) -> Result<Self, FormatError> {
        let mut bytes_vec = Vec::new();
        reader.read_to_end(&mut bytes_vec)?;
        // Strip a single leading UTF-8 BOM once at the source. `raw_bytes`
        // feeds every JSON path — array/object auto-detect, the NDJSON
        // line scan, and the envelope pre-scan's `serde_json::from_slice`
        // — so one strip here clears the marker from all of them.
        if bytes_vec.starts_with(&UTF8_BOM) {
            bytes_vec.drain(..UTF8_BOM.len());
        }
        let raw_bytes: Arc<[u8]> = Arc::from(bytes_vec);
        let body_cursor: Box<dyn Read + Send> = Box::new(Cursor::new(Arc::clone(&raw_bytes)));
        let mut buf = BufReader::new(body_cursor);
        let inner = Self::init(&mut buf, &config)?;
        Ok(JsonReader {
            inner,
            schema: None,
            config,
            pending: Vec::new(),
            raw_bytes,
        })
    }

    fn init(
        buf: &mut BufReader<Box<dyn Read + Send>>,
        config: &JsonReaderConfig,
    ) -> Result<InnerReader, FormatError> {
        // record_path → streaming DeserializeSeed navigation
        if let Some(ref rp) = config.record_path {
            let path_segments: Vec<String> = rp.split('.').map(String::from).collect();
            let records = stream_path_array(buf, &path_segments)?;
            return Ok(InnerReader::Collected { records, pos: 0 });
        }

        // Explicit format
        if let Some(mode) = config.format {
            return match mode {
                JsonMode::Array => {
                    let records = stream_top_level_array(buf)?;
                    Ok(InnerReader::Collected { records, pos: 0 })
                }
                JsonMode::Ndjson => {
                    let owned = std::mem::replace(buf, BufReader::new(Box::new(std::io::empty())));
                    Ok(InnerReader::Ndjson {
                        reader: owned,
                        line_buf: String::new(),
                    })
                }
                JsonMode::Object => Err(FormatError::Json(
                    "format: object requires record_path".into(),
                )),
            };
        }

        // Auto-detect
        let first = peek_first_byte(buf)?;
        match first {
            Some(b'[') => {
                let records = stream_top_level_array(buf)?;
                Ok(InnerReader::Collected { records, pos: 0 })
            }
            Some(b'{') => {
                let owned = std::mem::replace(buf, BufReader::new(Box::new(std::io::empty())));
                Ok(InnerReader::Ndjson {
                    reader: owned,
                    line_buf: String::new(),
                })
            }
            Some(b) => Err(FormatError::Json(format!(
                "cannot auto-detect: unexpected byte '{}' (0x{b:02x})",
                b as char
            ))),
            None => Ok(InnerReader::Done),
        }
    }

    fn next_raw(&mut self) -> Result<Option<serde_json::Value>, FormatError> {
        match &mut self.inner {
            InnerReader::Collected { records, pos } => {
                if *pos < records.len() {
                    let v = records[*pos].clone();
                    *pos += 1;
                    Ok(Some(v))
                } else {
                    Ok(None)
                }
            }
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

    fn apply_array_paths(
        &self,
        flat: serde_json::Map<String, serde_json::Value>,
    ) -> Vec<serde_json::Map<String, serde_json::Value>> {
        if self.config.array_paths.is_empty() {
            return vec![flat];
        }

        let mut result = vec![flat];
        for ap in &self.config.array_paths {
            let mut next = Vec::new();
            for rec in result {
                if let Some(serde_json::Value::Array(arr)) = rec.get(&ap.path) {
                    if arr.is_empty() {
                        match ap.mode {
                            ArrayPathMode::Explode => {}
                            ArrayPathMode::Join => {
                                let mut r = rec.clone();
                                r.insert(ap.path.clone(), serde_json::Value::String(String::new()));
                                next.push(r);
                            }
                        }
                        continue;
                    }
                    match ap.mode {
                        ArrayPathMode::Explode => {
                            for elem in arr {
                                let mut r = rec.clone();
                                if let serde_json::Value::Object(obj) = elem {
                                    r.remove(&ap.path);
                                    for (k, v) in obj {
                                        r.insert(k.clone(), v.clone());
                                    }
                                } else {
                                    r.insert(ap.path.clone(), elem.clone());
                                }
                                next.push(r);
                            }
                        }
                        ArrayPathMode::Join => {
                            let joined: String = arr
                                .iter()
                                .map(|v| match v {
                                    serde_json::Value::String(s) => s.clone(),
                                    o => o.to_string(),
                                })
                                .collect::<Vec<_>>()
                                .join(&ap.separator);
                            let mut r = rec.clone();
                            r.insert(ap.path.clone(), serde_json::Value::String(joined));
                            next.push(r);
                        }
                    }
                } else {
                    next.push(rec);
                }
            }
            result = next;
        }
        result
    }

    /// Builds a Record carrying the JSON object's actual keys (per-record
    /// schema). Each record's `Arc<Schema>` reflects exactly the keys
    /// present in that record — the per-Source `OnUnmapped` policy at
    /// the dispatch layer reconciles records against the user-declared
    /// schema (probing for `auto_widen`, rejecting on `reject`, or
    /// silently dropping on `drop`).
    fn map_to_record(
        &self,
        flat: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<Record, FormatError> {
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
                EnvelopeExtract::JsonPointer(p) => p.as_str(),
                EnvelopeExtract::XmlPath(_) => {
                    return Err(FormatError::Json(format!(
                        "envelope section {name:?}: declared `xml_path` extract \
                         against a JSON source. Use `json_pointer` for JSON envelope sections."
                    )));
                }
                EnvelopeExtract::Segment(_) => {
                    return Err(FormatError::Json(format!(
                        "envelope section {name:?}: declared `segment` extract \
                         against a JSON source. The `segment` extract is for \
                         flat-file formats (EDIFACT); use `json_pointer` for JSON."
                    )));
                }
            };
            targets.push(SectionTarget::new(name.clone(), pointer));
        }

        // Single streaming pass over a fresh cursor: only the matched
        // subtrees are built; every other key is parsed-and-skipped. The cap
        // is charged *as each declared section is constructed*, so an
        // oversized declared section aborts the parse mid-build rather than
        // after the whole subtree materializes. Body iteration keeps its own
        // independent cursor over `raw_bytes`.
        let mut prescan = BufReader::new(Cursor::new(Arc::clone(&self.raw_bytes)));
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
            let typed = coerce_json_section_fields(name, payload_obj, &section.fields)?;
            let path = doc_path_for_section(name);
            index.insert(&path, Value::Map(Box::new(typed)))?;
        }
        Ok(index.into_sections())
    }

    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        if let Some(ref s) = self.schema {
            return Ok(Arc::clone(s));
        }

        let first = self.next_raw()?;
        let first = match first {
            Some(v) => v,
            None => {
                let s = SchemaBuilder::new().build();
                self.schema = Some(Arc::clone(&s));
                self.inner = InnerReader::Done;
                return Ok(s);
            }
        };

        let mut flat = serde_json::Map::new();
        Self::flatten_value("", &first, &mut flat, 0);
        let expanded = self.apply_array_paths(flat);

        let empty = serde_json::Map::new();
        let first_flat = expanded.first().unwrap_or(&empty);
        let schema = first_flat
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
            return Ok(Some(self.map_to_record(&flat)?));
        }

        loop {
            let raw = match self.next_raw()? {
                Some(v) => v,
                None => return Ok(None),
            };
            let mut flat = serde_json::Map::new();
            Self::flatten_value("", &raw, &mut flat, 0);
            let expanded = self.apply_array_paths(flat);
            if expanded.is_empty() {
                continue;
            }
            let record = self.map_to_record(&expanded[0])?;
            self.pending = expanded.into_iter().skip(1).collect();
            return Ok(Some(record));
        }
    }
}

// ── Streaming DeserializeSeed for path navigation ────────────────────
//
// These types use serde's Deserializer API to navigate through a JSON
// object tree without buffering. Non-matching keys are skipped via
// `IgnoredAny` which parses but discards values (zero allocation).
// At the target path, array elements are deserialized one at a time
// as `serde_json::Value` and collected into a Vec.

use serde::de::{self, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};

/// Navigate to a nested path, then collect array elements.
/// Memory: O(N elements) for the collected results, but each element is
/// deserialized individually (not buffered as a raw tree of the whole file).
struct PathNavigator<'a> {
    path: &'a [String],
    results: &'a mut Vec<serde_json::Value>,
}

impl<'de, 'a> DeserializeSeed<'de> for PathNavigator<'a> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        if self.path.is_empty() {
            // Arrived at target — stream the array
            deserializer.deserialize_seq(ArrayCollector {
                results: self.results,
            })
        } else {
            // Navigate one level deeper
            deserializer.deserialize_map(ObjectNavigator {
                target_key: &self.path[0],
                remaining: &self.path[1..],
                results: self.results,
            })
        }
    }
}

struct ObjectNavigator<'a> {
    target_key: &'a str,
    remaining: &'a [String],
    results: &'a mut Vec<serde_json::Value>,
}

impl<'de, 'a> Visitor<'de> for ObjectNavigator<'a> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "an object containing key '{}'", self.target_key)
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<(), M::Error> {
        while let Some(key) = map.next_key::<String>()? {
            if key == self.target_key {
                // Found target key — descend into its value
                map.next_value_seed(PathNavigator {
                    path: self.remaining,
                    results: self.results,
                })?;
                // Skip remaining keys in this object
                while (map.next_key::<IgnoredAny>()?).is_some() {
                    map.next_value::<IgnoredAny>()?;
                }
                return Ok(());
            } else {
                // Skip this key's value entirely — zero allocation
                map.next_value::<IgnoredAny>()?;
            }
        }
        Err(de::Error::custom(format!(
            "key '{}' not found",
            self.target_key
        )))
    }
}

struct ArrayCollector<'a> {
    results: &'a mut Vec<serde_json::Value>,
}

impl<'de, 'a> Visitor<'de> for ArrayCollector<'a> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("an array of records")
    }

    fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<(), S::Error> {
        while let Some(element) = seq.next_element::<serde_json::Value>()? {
            self.results.push(element);
        }
        Ok(())
    }
}

/// Stream array elements from a nested path using DeserializeSeed.
/// Navigates to the path via `IgnoredAny` (zero-alloc skip), then
/// deserializes each array element individually.
fn stream_path_array(
    reader: &mut BufReader<Box<dyn Read + Send>>,
    path: &[String],
) -> Result<Vec<serde_json::Value>, FormatError> {
    let mut results = Vec::new();
    let mut de = serde_json::Deserializer::from_reader(reader);
    PathNavigator {
        path,
        results: &mut results,
    }
    .deserialize(&mut de)
    .map_err(|e| FormatError::Json(e.to_string()))?;
    Ok(results)
}

/// Stream elements from a top-level JSON array using DeserializeSeed.
fn stream_top_level_array(
    reader: &mut BufReader<Box<dyn Read + Send>>,
) -> Result<Vec<serde_json::Value>, FormatError> {
    let mut results = Vec::new();
    let mut de = serde_json::Deserializer::from_reader(reader);
    de.deserialize_seq(ArrayCollector {
        results: &mut results,
    })
    .map_err(|e| FormatError::Json(e.to_string()))?;
    Ok(results)
}

// ── Helpers ──────────────────────────────────────────────────────────

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
fn coerce_json_section_fields(
    section_name: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
    schema: &IndexMap<String, EnvelopeFieldType>,
) -> Result<IndexMap<Box<str>, Value>, FormatError> {
    let mut out: IndexMap<Box<str>, Value> = IndexMap::with_capacity(schema.len());
    for (field, ty) in schema {
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
        if let Some(pos) = buf.iter().position(|b| !b.is_ascii_whitespace()) {
            return Ok(Some(buf[pos]));
        }
        let len = buf.len();
        reader.consume(len);
    }
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
        // The envelope pre-scan streams `raw_bytes` from a fresh cursor;
        // the source-level strip must clear the BOM for that path too, not
        // just body iteration.
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
    fn test_json_array_paths_explode() {
        let input = r#"[{"name":"Alice","orders":[{"id":1},{"id":2}]}]"#;
        let config = JsonReaderConfig {
            array_paths: vec![ArrayPathSpec {
                path: "orders".into(),
                mode: ArrayPathMode::Explode,
                separator: ",".into(),
            }],
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
    fn test_json_array_paths_join() {
        let input = r#"[{"name":"Alice","tags":["a","b","c"]}]"#;
        let config = JsonReaderConfig {
            array_paths: vec![ArrayPathSpec {
                path: "tags".into(),
                mode: ArrayPathMode::Join,
                separator: ",".into(),
            }],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        assert_eq!(
            r.next_record().unwrap().unwrap().get("tags"),
            Some(&Value::String("a,b,c".into()))
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
    fn test_json_array_paths_empty_array() {
        // Alice's empty `orders` array suppresses her record entirely
        // under Explode mode; Bob's expanded record is the only output.
        // Keys absent from the inferred schema (which here is just
        // `orders.id`) are silently dropped.
        let input = r#"[{"name":"Alice","orders":[]},{"name":"Bob","orders":[{"id":1}]}]"#;
        let config = JsonReaderConfig {
            array_paths: vec![ArrayPathSpec {
                path: "orders".into(),
                mode: ArrayPathMode::Explode,
                separator: ",".into(),
            }],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let s = r.schema().unwrap();
        // Schema inference walks Alice's record only (the streaming
        // reader can't peek further); her empty `orders` array yields
        // no nested columns, so the inferred schema is empty. Bob's
        // expanded record carries its own per-record schema with
        // `name` and the exploded `orders.id` field — the dispatch
        // layer applies the per-Source `OnUnmapped` policy against
        // the user-declared schema.
        assert_eq!(s.columns().len(), 0);
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.iter_all_fields().count(), 2);
        assert!(r.next_record().unwrap().is_none());
    }
}
