//! Streaming envelope pre-scan for XML sources.
//!
//! Walks an XML document exactly once through quick-xml's pull parser,
//! building a flat `(field, raw_string)` payload *only* for the subtrees a
//! source's declared envelope sections point at and dropping every other
//! element's body without allocating it. This replaces a full-tree capture
//! of every declared section — which materialized each matched subtree
//! before any cap could fire — so the pre-scan's retained memory scales
//! with the declared sections rather than the document size.
//!
//! Unmatched elements are counted-and-dropped: quick-xml does not
//! materialize the body of an element the walk never recurses into, so the
//! only allocation per declared section happens inside
//! [`read_section_payload`], which is called solely on a path match.
//!
//! Blocking, bounded: the pre-scan runs before any body record streams and
//! returns the matched sections' flat payloads. A declared section's
//! payload is built through a byte-counting accumulator that charges each
//! retained field name + value against a shared cap and aborts the parse
//! the moment the cumulative retained total crosses `max_index_bytes` — so
//! even a single oversized declared section fails loud at ~the cap, before
//! the whole subtree materializes, not after.

use std::io::{BufReader, Read};

use quick_xml::Reader as XmlParser;
use quick_xml::events::Event;

use crate::error::FormatError;
use crate::xml::reader::{BodyParser, NamespaceMode, elem_name_static, extract_attributes_static};

/// One matched section's name paired with its flat `(field, raw_string)`
/// payload, as produced by [`extract_sections`].
pub(crate) type MatchedSection = (Box<str>, Vec<(String, String)>);

/// One declared section's slash-path, decoded to element-name segments,
/// plus its author-chosen name.
///
/// A leading `/` is trimmed before splitting, so `/doc/Summary` and
/// `doc/Summary` yield the same segments — matching the existing
/// `prepare_document` segment compile.
pub(crate) struct SectionTarget {
    /// Author-chosen section name the matched payload is recorded under.
    pub name: Box<str>,
    /// Element-name path segments, rooted at the document element.
    pub segments: Vec<String>,
}

impl SectionTarget {
    /// Decode a slash-path into element-name segments, trimming a leading
    /// `/` so a rooted path and an unrooted one compile identically.
    pub(crate) fn new(name: Box<str>, path: &str) -> Self {
        let segments = path
            .trim_start_matches('/')
            .split('/')
            .map(String::from)
            .collect();
        SectionTarget { name, segments }
    }
}

/// Walk an XML document once, building a flat payload only for the subtrees
/// named by `targets` and charging their retained bytes against
/// `max_index_bytes`.
///
/// Each matched section's subtree is flattened into `(field, raw_string)`
/// pairs through a byte-counting accumulator; every unmatched element is
/// counted-and-dropped without allocating its body. The cap is charged *as
/// each pair is appended*, so an oversized declared section aborts the
/// parse mid-subtree rather than after the whole subtree materializes. A
/// target whose path never appears in the document yields no payload (a
/// graceful miss), matching the absent-section convention.
///
/// # Errors
///
/// Returns [`FormatError::Xml`] on an XML parse failure, or naming the
/// offending section and the cap when a declared section's retained bytes
/// cross `max_index_bytes` mid-parse.
pub(crate) fn extract_sections(
    reader: BufReader<Box<dyn Read + Send>>,
    targets: &[SectionTarget],
    ns: &NamespaceMode,
    attr_prefix: &str,
    max_index_bytes: Option<usize>,
) -> Result<Vec<MatchedSection>, FormatError> {
    let mut parser = XmlParser::from_reader(reader);
    parser.config_mut().trim_text(true);

    let parse_ctx = SectionParseCtx { ns, attr_prefix };
    let mut path_stack: Vec<String> = Vec::new();
    let mut captured: Vec<MatchedSection> = Vec::new();
    let mut charge = ByteCharge {
        retained_bytes: 0,
        max_index_bytes,
    };
    let mut buf: Vec<u8> = Vec::new();

    loop {
        buf.clear();
        let event = parser
            .read_event_into(&mut buf)
            .map_err(|e| FormatError::Xml(e.to_string()))?;
        match event {
            Event::Start(ref e) => {
                let name = elem_name_static(ns, &e.name());
                path_stack.push(name);
                if let Some(target) = targets
                    .iter()
                    .find(|t| path_matches(&path_stack, &t.segments))
                {
                    // The matched section element's OWN attributes belong to
                    // the section, addressable as bare `@attr` (no element
                    // prefix) — exactly as the body reader seeds a record's
                    // start-tag attributes. Seed them before the children so
                    // `<Summary id="5">` exposes `$doc.Summary.@id`; the
                    // children's nested fields follow.
                    let mut seed: Vec<(String, String)> = Vec::new();
                    for (key, val) in extract_attributes_static(attr_prefix, e)? {
                        charge.charge(key.len() + val.len(), &target.name)?;
                        seed.push((key, val));
                    }
                    let payload = read_section_payload(
                        &mut parser,
                        &mut buf,
                        &parse_ctx,
                        path_stack.len(),
                        &target.name,
                        &mut charge,
                        seed,
                    )?;
                    captured.push((target.name.clone(), payload));
                    // `read_section_payload` consumed the matched subtree up
                    // to its closing `End`; the upcoming loop iteration will
                    // not see that `End`, so pop the element now.
                    path_stack.pop();
                }
            }
            Event::Empty(ref e) => {
                let name = elem_name_static(ns, &e.name());
                path_stack.push(name);
                if let Some(target) = targets
                    .iter()
                    .find(|t| path_matches(&path_stack, &t.segments))
                {
                    let mut payload: Vec<(String, String)> = Vec::new();
                    for (key, val) in extract_attributes_static(attr_prefix, e)? {
                        charge.charge(key.len() + val.len(), &target.name)?;
                        payload.push((key, val));
                    }
                    captured.push((target.name.clone(), payload));
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

    Ok(captured)
}

/// The two static per-parse settings the section walk threads unchanged
/// through every element: namespace handling and the attribute-key prefix.
/// Grouped so the subtree reader carries them as one borrow.
struct SectionParseCtx<'a> {
    ns: &'a NamespaceMode,
    attr_prefix: &'a str,
}

/// Running retained-byte total for the streaming pass, charged against the
/// configured cap as each retained field is appended.
struct ByteCharge {
    retained_bytes: usize,
    max_index_bytes: Option<usize>,
}

impl ByteCharge {
    /// Charge `bytes` against the cap; abort with a [`FormatError::Xml`]
    /// naming `section` and the cap when the running total would cross it.
    fn charge(&mut self, bytes: usize, section: &str) -> Result<(), FormatError> {
        let projected = self.retained_bytes.saturating_add(bytes);
        if let Some(cap) = self.max_index_bytes
            && projected > cap
        {
            return Err(FormatError::Xml(format!(
                "envelope document-index cap exceeded: building section {section:?} \
                 crossed the {cap}-byte `max_index_bytes` cap mid-parse. Raise \
                 `max_index_bytes` on the source, or narrow the `$doc.*` paths the \
                 pipeline reads (declare paths over envelope metadata, not bulk elements)."
            )));
        }
        self.retained_bytes = projected;
        Ok(())
    }
}

/// Match the runtime descent stack against a declared section path. Both
/// are element-name slices; a section path is rooted at the document, so an
/// exact match (modulo length) wins.
fn path_matches(stack: &[String], segs: &[String]) -> bool {
    stack.len() == segs.len() && stack.iter().zip(segs.iter()).all(|(a, b)| a == b)
}

/// Read the subtree under the current `Start` element (already pushed onto
/// the caller's `path_stack`) into a flat list of `(field_name, raw_string)`
/// pairs, charging each retained pair against `charge` and aborting
/// mid-subtree the instant the running total crosses the cap.
///
/// `seed` carries the matched element's own start-tag attributes (already
/// charged by the caller), which lead the payload as bare `@attr` keys before
/// the children's nested fields — matching the body reader's start-attribute
/// seeding.
///
/// Produces the same flat payload the body record extractor does — same
/// `.`-joined nested-element prefixes, attribute prefixing, and CData /
/// text handling — so a declared section's output is byte-identical to the
/// body reader's for the same element. Returns without touching the
/// caller's body-iteration state.
///
/// # Errors
///
/// Returns [`FormatError::Xml`] on a parse failure, or naming `section` and
/// the cap when the retained bytes cross `max_index_bytes` mid-subtree.
fn read_section_payload(
    parser: &mut BodyParser,
    buf: &mut Vec<u8>,
    ctx: &SectionParseCtx,
    section_depth: usize,
    section: &str,
    charge: &mut ByteCharge,
    seed: Vec<(String, String)>,
) -> Result<Vec<(String, String)>, FormatError> {
    let mut fields: Vec<(String, String)> = seed;
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
                let name = elem_name_static(ctx.ns, &e.name());
                element_stack.push(name);
                let attrs = extract_attributes_static(ctx.attr_prefix, e)?;
                let prefix = element_stack.join(".");
                for (k, v) in attrs {
                    let key = format!("{prefix}.{k}");
                    charge.charge(key.len() + v.len(), section)?;
                    fields.push((key, v));
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
                let name = elem_name_static(ctx.ns, &e.name());
                let prefix = if element_stack.is_empty() {
                    name.clone()
                } else {
                    format!("{}.{name}", element_stack.join("."))
                };
                charge.charge(prefix.len(), section)?;
                fields.push((prefix.clone(), String::new()));
                let attrs = extract_attributes_static(ctx.attr_prefix, e)?;
                for (k, v) in attrs {
                    let key = format!("{prefix}.{k}");
                    charge.charge(key.len() + v.len(), section)?;
                    fields.push((key, v));
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
                        charge.charge(field_name.len() + text.len(), section)?;
                        fields.push((field_name, text));
                    }
                }
            }
            Event::CData(ref cd) => {
                let text = String::from_utf8_lossy(cd.as_ref()).into_owned();
                if !text.is_empty() {
                    let field_name = element_stack.join(".");
                    if !field_name.is_empty() {
                        charge.charge(field_name.len() + text.len(), section)?;
                        fields.push((field_name, text));
                    }
                }
            }
            Event::Eof => return Ok(fields),
            _ => {}
        }
    }
}
