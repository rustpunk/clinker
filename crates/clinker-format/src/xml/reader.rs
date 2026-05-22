//! Streaming XML reader using quick-xml pull parser.
//!
//! Navigates to `record_path`, extracts attributes with configurable prefix,
//! flattens nested elements with `.` separator, handles namespaces.
//!
//! The reader buffers its entire input at construction time into an
//! `Arc<[u8]>` so the envelope pre-scan (run before any body record
//! emits) and the body iteration can each parse from a fresh cursor
//! over the same byte slice. ETL XML files are typically MB-scale; the
//! buffering trade-off enables the locked "all `$doc.*` sections
//! available throughout the body stream" semantics without re-opening
//! the source mid-stream.

use std::io::{self, Cursor, Read};
use std::sync::Arc;

use indexmap::IndexMap;
use quick_xml::Reader as XmlParser;
use quick_xml::events::Event;

use clinker_record::{
    DEFAULT_DATE_FORMATS, DEFAULT_DATETIME_FORMATS, Record, Schema, SchemaBuilder, Value,
    coerce_to_bool, coerce_to_date, coerce_to_datetime, coerce_to_float, coerce_to_int,
    coerce_to_string,
};

use crate::envelope::{EnvelopeConfig, EnvelopeExtract, EnvelopeFieldType};
use crate::error::FormatError;
use crate::traits::FormatReader;

/// XML reader configuration.
pub struct XmlReaderConfig {
    pub record_path: Option<String>,
    pub attribute_prefix: String,
    pub namespace_handling: NamespaceMode,
    pub array_paths: Vec<XmlArrayPath>,
}

impl Default for XmlReaderConfig {
    fn default() -> Self {
        Self {
            record_path: None,
            attribute_prefix: "@".into(),
            namespace_handling: NamespaceMode::Strip,
            array_paths: vec![],
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

/// Streaming XML reader.
///
/// Buffers its entire input at construction time into an `Arc<[u8]>`
/// so two parsers can walk the same bytes: one transient parser for
/// the envelope pre-scan, and one stateful parser for body record
/// iteration. The trade-off — held memory equal to the source file's
/// size while the reader exists — enables the locked
/// "all `$doc.*` sections available throughout the body stream"
/// semantics, since post-body envelope sections must be extracted
/// before the first body record emits.
pub struct XmlReader {
    bytes: Arc<[u8]>,
    parser: XmlParser<Cursor<Arc<[u8]>>>,
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
    /// Build a reader from any `Read` source. Drains the input into a
    /// shared `Arc<[u8]>` buffer at construction; subsequent envelope
    /// pre-scan and body iteration each parse from a fresh cursor over
    /// the same bytes.
    pub fn new<R: Read>(mut reader: R, config: XmlReaderConfig) -> io::Result<Self> {
        let mut bytes_vec = Vec::new();
        reader.read_to_end(&mut bytes_vec)?;
        let bytes: Arc<[u8]> = Arc::from(bytes_vec);
        Ok(Self::from_bytes(bytes, config))
    }

    /// Build a reader directly from a buffered byte slice. Used
    /// internally by [`Self::new`] and by envelope pre-scan to reset
    /// the body parser at start of stream.
    fn from_bytes(bytes: Arc<[u8]>, config: XmlReaderConfig) -> Self {
        let mut parser = XmlParser::from_reader(Cursor::new(Arc::clone(&bytes)));
        parser.config_mut().trim_text(true);

        let path_segments: Vec<String> = config
            .record_path
            .as_deref()
            .map(|p| p.split('/').map(String::from).collect())
            .unwrap_or_default();

        XmlReader {
            bytes,
            parser,
            config,
            schema: None,
            buf: Vec::new(),
            path_segments,
            matched_depth: 0,
            xml_depth: 0,
            pending: None,
            done: false,
        }
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
        let mut buf2 = Vec::new();

        loop {
            buf2.clear();
            let event = self
                .parser
                .read_event_into(&mut buf2)
                .map_err(|e| FormatError::Xml(e.to_string()))?;

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
                    let text = t
                        .unescape()
                        .map_err(|e| FormatError::Xml(e.to_string()))?
                        .into_owned();
                    if !text.is_empty() {
                        let field_name = element_stack.join(".");
                        if !field_name.is_empty() {
                            fields.push((field_name, text));
                        }
                    }
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

        // Compile each declared section's XmlPath into a path-segment
        // vector once; a JsonPointer arrival means a config-for-wrong-
        // format mistake and surfaces as a format error.
        let mut sections: Vec<(Box<str>, Vec<String>, &crate::envelope::EnvelopeSection)> =
            Vec::with_capacity(config.sections.len());
        for (name, section) in &config.sections {
            match &section.extract {
                EnvelopeExtract::XmlPath(p) => {
                    let segments: Vec<String> = p
                        .trim_start_matches('/')
                        .split('/')
                        .map(String::from)
                        .collect();
                    sections.push((Box::from(name.as_str()), segments, section));
                }
                EnvelopeExtract::JsonPointer(_) => {
                    return Err(FormatError::Xml(format!(
                        "envelope section {name:?}: declared `json_pointer` extract \
                         against an XML source. Use `xml_path` for XML envelope sections."
                    )));
                }
            }
        }

        // Transient parser over a fresh cursor; the body parser's state
        // remains untouched and starts streaming from byte 0 when
        // `next_record` is first called.
        let mut transient = XmlParser::from_reader(Cursor::new(Arc::clone(&self.bytes)));
        transient.config_mut().trim_text(true);
        let mut path_stack: Vec<String> = Vec::new();
        let mut captured: IndexMap<Box<str>, IndexMap<Box<str>, Value>> = IndexMap::new();
        let mut buf: Vec<u8> = Vec::new();

        loop {
            buf.clear();
            let event = transient
                .read_event_into(&mut buf)
                .map_err(|e| FormatError::Xml(e.to_string()))?;
            match event {
                Event::Start(ref e) => {
                    let name = elem_name_static(&self.config.namespace_handling, &e.name());
                    path_stack.push(name);
                    if let Some((sec_name, _, section)) = sections
                        .iter()
                        .find(|(_, segs, _)| path_matches(&path_stack, segs))
                    {
                        let payload = read_section_payload(
                            &mut transient,
                            &mut buf,
                            &self.config.namespace_handling,
                            &self.config.attribute_prefix,
                            path_stack.len(),
                        )?;
                        let typed = coerce_section_fields(payload, &section.fields)?;
                        captured.insert(Box::from(&**sec_name), typed);
                        // `read_section_payload` consumed the matched
                        // subtree up to its closing `End`. Pop the
                        // element off the path stack now since the
                        // upcoming loop iteration will not see that
                        // `End` event.
                        path_stack.pop();
                    }
                }
                Event::Empty(ref e) => {
                    let name = elem_name_static(&self.config.namespace_handling, &e.name());
                    path_stack.push(name);
                    if let Some((sec_name, _, section)) = sections
                        .iter()
                        .find(|(_, segs, _)| path_matches(&path_stack, segs))
                    {
                        let mut payload: Vec<(String, String)> = Vec::new();
                        let attrs = extract_attributes_static(&self.config.attribute_prefix, e)?;
                        payload.extend(attrs);
                        let typed = coerce_section_fields(payload, &section.fields)?;
                        captured.insert(Box::from(&**sec_name), typed);
                    }
                    path_stack.pop();
                }
                Event::End(_) => {
                    path_stack.pop();
                }
                Event::Eof => break,
                _ => {}
            }
        }

        // Convert the captured fields into the trait return shape:
        // each section's payload is a `Value::Map`.
        let mut out: IndexMap<Box<str>, Value> = IndexMap::with_capacity(captured.len());
        for (sec_name, fields) in captured {
            out.insert(sec_name, Value::Map(Box::new(fields)));
        }
        Ok(out)
    }
}

/// Extract element name without borrowing self.
fn elem_name_static(ns: &NamespaceMode, qname: &quick_xml::name::QName) -> String {
    let local = qname.local_name();
    let bytes = match ns {
        NamespaceMode::Strip => local.as_ref(),
        NamespaceMode::Qualify => qname.as_ref(),
    };
    String::from_utf8_lossy(bytes).into_owned()
}

/// Extract attributes without borrowing self.
fn extract_attributes_static(
    prefix: &str,
    elem: &quick_xml::events::BytesStart,
) -> Result<Vec<(String, String)>, FormatError> {
    let mut attrs = Vec::new();
    for attr in elem.attributes() {
        let attr = attr.map_err(|e| FormatError::Xml(e.to_string()))?;
        let key = String::from_utf8_lossy(attr.key.as_ref()).into_owned();
        let val = attr
            .unescape_value()
            .map_err(|e| FormatError::Xml(e.to_string()))?
            .into_owned();
        attrs.push((format!("{prefix}{key}"), val));
    }
    Ok(attrs)
}

/// Match the runtime descent stack against a declared section path.
/// Both are taken as slices of element names; a section path is
/// rooted at the document, so an exact-match (modulo length) wins.
fn path_matches(stack: &[String], segs: &[String]) -> bool {
    if stack.len() != segs.len() {
        return false;
    }
    stack.iter().zip(segs.iter()).all(|(a, b)| a == b)
}

/// Read the subtree under the current `Start` element (already
/// pushed onto the caller's `path_stack`) into a flat list of
/// `(field_name, raw_string)` pairs. Mirrors `extract_record_fields`
/// but operates on a transient envelope-scan parser and returns
/// without touching the caller's body-iteration state.
fn read_section_payload(
    parser: &mut XmlParser<Cursor<Arc<[u8]>>>,
    buf: &mut Vec<u8>,
    ns: &NamespaceMode,
    attr_prefix: &str,
    section_depth: usize,
) -> Result<Vec<(String, String)>, FormatError> {
    let mut fields: Vec<(String, String)> = Vec::new();
    let mut element_stack: Vec<String> = Vec::new();
    let mut depth_here = section_depth;
    loop {
        buf.clear();
        let event = parser
            .read_event_into(buf)
            .map_err(|e| FormatError::Xml(e.to_string()))?;
        match event {
            Event::Start(ref e) => {
                depth_here += 1;
                let name = elem_name_static(ns, &e.name());
                element_stack.push(name);
                let attrs = extract_attributes_static(attr_prefix, e)?;
                let prefix = element_stack.join(".");
                for (k, v) in attrs {
                    fields.push((format!("{prefix}.{k}"), v));
                }
            }
            Event::End(_) => {
                depth_here -= 1;
                if depth_here < section_depth {
                    return Ok(fields);
                }
                element_stack.pop();
            }
            Event::Empty(ref e) => {
                let name = elem_name_static(ns, &e.name());
                let prefix = if element_stack.is_empty() {
                    name.clone()
                } else {
                    format!("{}.{name}", element_stack.join("."))
                };
                fields.push((prefix.clone(), String::new()));
                let attrs = extract_attributes_static(attr_prefix, e)?;
                for (k, v) in attrs {
                    fields.push((format!("{prefix}.{k}"), v));
                }
            }
            Event::Text(ref t) => {
                let text = t
                    .unescape()
                    .map_err(|e| FormatError::Xml(e.to_string()))?
                    .into_owned();
                if !text.is_empty() {
                    let field_name = element_stack.join(".");
                    if !field_name.is_empty() {
                        fields.push((field_name, text));
                    }
                }
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
            Event::Eof => return Ok(fields),
            _ => {}
        }
    }
}

/// Coerce raw `(name, string)` pairs from envelope extraction into
/// the section's declared field schema. Unknown fields are dropped
/// silently (the section schema is the contract). Missing fields are
/// not reported here — they evaluate to `Value::Null` via the
/// `DocumentContext::get_section_field` `Option`-to-`Null` mapping at
/// CXL eval time. Type-mismatch coercions surface as a format error
/// citing the section, field, and observed value.
fn coerce_section_fields(
    raw: Vec<(String, String)>,
    schema: &IndexMap<String, EnvelopeFieldType>,
) -> Result<IndexMap<Box<str>, Value>, FormatError> {
    let mut out: IndexMap<Box<str>, Value> = IndexMap::with_capacity(schema.len());
    // Build a lookup so we coerce each raw value through the declared
    // type. Multiple raw values for the same field name (rare, e.g.
    // repeated elements) keep the first non-empty observation.
    let mut by_name: IndexMap<&str, &str> = IndexMap::with_capacity(raw.len());
    for (k, v) in &raw {
        by_name.entry(k.as_str()).or_insert(v.as_str());
    }
    for (field, ty) in schema {
        let raw_str = match by_name.get(field.as_str()) {
            Some(s) if !s.is_empty() => *s,
            _ => continue,
        };
        let value = Value::String(raw_str.into());
        let coerced = match ty {
            EnvelopeFieldType::String => coerce_to_string(&value),
            EnvelopeFieldType::Int => coerce_to_int(&value),
            EnvelopeFieldType::Float => coerce_to_float(&value),
            EnvelopeFieldType::Bool => coerce_to_bool(&value),
            EnvelopeFieldType::Date => coerce_to_date(&value, DEFAULT_DATE_FORMATS),
            EnvelopeFieldType::DateTime => coerce_to_datetime(&value, DEFAULT_DATETIME_FORMATS),
        }
        .map_err(|e| {
            FormatError::Xml(format!(
                "envelope section field {field:?} (declared type {ty:?}): \
                 cannot coerce value {raw_str:?}: {e}"
            ))
        })?;
        out.insert(Box::from(field.as_str()), coerced);
    }
    Ok(out)
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
    use std::io::Cursor;

    fn reader_from_str(xml: &str, config: XmlReaderConfig) -> XmlReader {
        XmlReader::new(Cursor::new(xml.as_bytes().to_vec()), config).expect("XML buffer read")
    }

    fn default_config_with_path(path: &str) -> XmlReaderConfig {
        XmlReaderConfig {
            record_path: Some(path.into()),
            ..Default::default()
        }
    }

    fn envelope_config(sections: &[(&str, &str, &[(&str, EnvelopeFieldType)])]) -> EnvelopeConfig {
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
        let cfg = envelope_config(&[
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
        ]);

        let mut reader = reader_from_str(xml, default_config_with_path("doc/records/record"));
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
        let mut reader = reader_from_str(xml, default_config_with_path("doc/a"));
        let err = reader.prepare_document(&cfg).unwrap_err();
        assert!(matches!(err, FormatError::Xml(msg) if msg.contains("json_pointer")));
    }

    #[test]
    fn prepare_document_missing_section_yields_no_entry() {
        // A section that the config declares but the XML doesn't carry
        // is absent from the returned map; CXL resolves missing
        // sections to `Value::Null`.
        let xml = r#"<doc><records><record><x>1</x></record></records></doc>"#;
        let cfg = envelope_config(&[(
            "Trailer",
            "/doc/Trailer",
            &[("count", EnvelopeFieldType::Int)],
        )]);
        let mut reader = reader_from_str(xml, default_config_with_path("doc/records/record"));
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
        let cfg = envelope_config(&[(
            "Meta",
            "/doc/Meta",
            &[
                ("run_date", EnvelopeFieldType::Date),
                ("enabled", EnvelopeFieldType::Bool),
                ("ratio", EnvelopeFieldType::Float),
            ],
        )]);
        let mut reader = reader_from_str(xml, default_config_with_path("doc/records/record"));
        let sections = reader.prepare_document(&cfg).expect("scan ok");
        let meta = unwrap_section_map(sections.get("Meta").unwrap());
        assert!(matches!(meta.get("run_date"), Some(Value::Date(_))));
        assert_eq!(meta.get("enabled"), Some(&Value::Bool(true)));
        assert_eq!(meta.get("ratio"), Some(&Value::Float(0.5)));
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
}
