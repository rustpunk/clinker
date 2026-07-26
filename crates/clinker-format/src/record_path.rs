//! The `record_path` grammar shared by the XML and JSON readers.
//!
//! `record_path` names the element (XML) or object key (JSON) whose contents
//! become one record each. Both readers used to build their segment list with a
//! bare `split`, which accepted anything: an XPath-shaped `//product` produced a
//! leading empty segment that no element name can equal, so the run completed
//! with zero records and no error. Parsing the string through one grammar makes
//! every unsupported form fail loud at the boundary that first sees it.
//!
//! The two grammars are deliberately separate types of path, not dialects of
//! one: XML segments are element names matched level by level from the document
//! element, JSON segments are object keys descended from the document root.
//!
//! Bounded work: one pass over a config string, allocating only the segment
//! vector the readers already built.

use std::fmt;

use crate::xml::writer::is_valid_xml_name;

/// Which of the two `record_path` grammars a string is written in.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RecordPathSyntax {
    /// Slash-separated XML element names, rooted at the document element.
    Xml,
    /// Dot-separated JSON object keys, descended from the document root.
    Json,
}

impl RecordPathSyntax {
    /// The separator between segments.
    fn separator(self) -> char {
        match self {
            RecordPathSyntax::Xml => '/',
            RecordPathSyntax::Json => '.',
        }
    }

    /// One sentence naming the grammar, used to open every diagnostic.
    fn grammar(self) -> &'static str {
        match self {
            RecordPathSyntax::Xml => {
                "a slash-separated path of XML element names, matched level by level from the \
                 document element (for example `catalog/product`)"
            }
            RecordPathSyntax::Json => {
                "a dot-separated path of object keys, descended from the document root (for \
                 example `data.rows`)"
            }
        }
    }

    /// What omitting the key entirely does, so a diagnostic can distinguish
    /// "omitted" from "present but empty".
    fn omission(self) -> &'static str {
        match self {
            RecordPathSyntax::Xml => "every top-level element becomes one record",
            RecordPathSyntax::Json => "the reader auto-detects the document shape",
        }
    }

    /// The full guidance paragraph a plan-time diagnostic attaches as help.
    pub fn help(self) -> String {
        match self {
            RecordPathSyntax::Xml => "`record_path` on an `xml` source is a slash-separated path \
                                      of XML element names, matched level by level from the \
                                      document element: no leading or doubled `/`, no empty \
                                      segments, and no XPath axes, predicates, or wildcards. \
                                      Every segment must be a legal XML element name; a namespace \
                                      prefix (`ns:Order`) is allowed and matches under \
                                      `namespace_handling: qualify`. Omit `record_path` entirely \
                                      and every top-level element becomes one record. Run \
                                      `clinker explain --code E363` for the full grammar."
                .to_string(),
            RecordPathSyntax::Json => "`record_path` on a `json` source is a dot-separated path \
                                       of object keys descended from the document root: no `$.` \
                                       JSONPath root marker, no leading `/`, and no empty \
                                       segments. It takes precedence over `format:`, so pair it \
                                       with `format: object` or leave `format:` off. Omit \
                                       `record_path` entirely and the reader auto-detects the \
                                       document shape. Run `clinker explain --code E363` for the \
                                       full grammar."
                .to_string(),
        }
    }
}

/// Why a `record_path` string is not a path in its declared grammar.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordPathErrorKind {
    /// The string is present but empty — distinct from omitting the key.
    Empty,
    /// An XPath descendant-or-self step (`//`), which matches at any depth.
    XPathDescendant,
    /// A leading separator, as an absolute XPath or JSON Pointer would carry.
    Rooted,
    /// A leading `$.`, the JSONPath root marker.
    JsonPathRoot,
    /// A separator with nothing between it and the next one, or the end.
    EmptySegment { index: usize },
    /// A segment no XML element can be named, so it can never match.
    NotAnXmlName { index: usize, segment: String },
}

/// A `record_path` string rejected by its grammar, carrying enough context to
/// render a message that names the actual grammar and a corrected path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordPathError {
    pub syntax: RecordPathSyntax,
    pub raw: String,
    pub kind: RecordPathErrorKind,
}

impl RecordPathError {
    /// The guidance paragraph a plan-time diagnostic attaches as help.
    pub fn help(&self) -> String {
        self.syntax.help()
    }

    /// The nearest string in the grammar, when mechanically stripping the
    /// offending syntax yields one. Offered as a corrected example only — none
    /// of these forms is accepted as an alias, because `//product` in real
    /// XPath descends any number of levels and silently accepting it would
    /// promise a match the reader cannot make.
    fn corrected(&self) -> Option<String> {
        let sep = self.syntax.separator();
        let doubled = format!("{sep}{sep}");
        let mut s = match self.syntax {
            RecordPathSyntax::Xml => self.raw.clone(),
            RecordPathSyntax::Json => self
                .raw
                .strip_prefix("$.")
                .unwrap_or(&self.raw)
                .trim_start_matches('/')
                .to_string(),
        };
        // Each pass strictly shortens the string, so this terminates.
        while s.contains(&doubled) {
            s = s.replace(&doubled, &sep.to_string());
        }
        let s = s.trim_matches(sep).to_string();
        RecordPath::parse(self.syntax, &s).ok().map(|_| s)
    }
}

impl fmt::Display for RecordPathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let raw = &self.raw;
        let grammar = self.syntax.grammar();
        match &self.kind {
            RecordPathErrorKind::Empty => {
                write!(
                    f,
                    "`record_path` is empty; it must be {grammar}. Omit the key entirely and {}",
                    self.syntax.omission()
                )?;
            }
            RecordPathErrorKind::XPathDescendant => {
                write!(
                    f,
                    "`record_path` {raw:?} uses the XPath descendant step `//`, which matches at \
                     any depth; `record_path` is {grammar}, so name every enclosing element"
                )?;
            }
            RecordPathErrorKind::Rooted => {
                write!(
                    f,
                    "`record_path` {raw:?} starts with `/`, as an absolute XPath or JSON Pointer \
                     would; `record_path` is {grammar} and is already anchored there"
                )?;
            }
            RecordPathErrorKind::JsonPathRoot => {
                write!(
                    f,
                    "`record_path` {raw:?} starts with the JSONPath root marker `$.`, which is \
                     not part of the grammar; `record_path` is {grammar}"
                )?;
            }
            RecordPathErrorKind::EmptySegment { index } => {
                write!(
                    f,
                    "`record_path` {raw:?} has an empty segment at position {}; it must be \
                     {grammar}",
                    index + 1
                )?;
            }
            RecordPathErrorKind::NotAnXmlName { index, segment } => {
                write!(
                    f,
                    "`record_path` {raw:?} segment {} ({segment:?}) is not an XML element name, \
                     so no element can ever match it; `record_path` is {grammar} — XPath axes, \
                     predicates, and wildcards are not supported",
                    index + 1
                )?;
            }
        }
        if let Some(fixed) = self.corrected() {
            write!(f, ". Write {fixed:?} instead")?;
        }
        Ok(())
    }
}

impl std::error::Error for RecordPathError {}

/// A validated `record_path`: the segments the reader descends, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordPath(Vec<String>);

impl RecordPath {
    /// Parse `raw` in `syntax`.
    ///
    /// # Errors
    ///
    /// Returns [`RecordPathError`] for any string that is not a path in the
    /// declared grammar. Every rejected form either matches nothing (yielding a
    /// silent empty run) or fails obscurely deeper in the reader.
    pub fn parse(syntax: RecordPathSyntax, raw: &str) -> Result<Self, RecordPathError> {
        let err = |kind| RecordPathError {
            syntax,
            raw: raw.to_string(),
            kind,
        };
        if raw.is_empty() {
            return Err(err(RecordPathErrorKind::Empty));
        }
        // Checked ahead of the separator rules for both grammars: `$` cannot
        // start an XML name either, so an XML path written as JSONPath gets the
        // message that names what it actually is.
        if raw.starts_with("$.") {
            return Err(err(RecordPathErrorKind::JsonPathRoot));
        }
        if syntax == RecordPathSyntax::Xml && raw.contains("//") {
            // Ahead of the rooted check so `//product` is reported as the XPath
            // descendant step it is rather than as a stray leading slash.
            return Err(err(RecordPathErrorKind::XPathDescendant));
        }
        // Rejected for JSON too, though `/` is a legal character inside an
        // object key: a leading `/` reads as a JSON Pointer, and a top-level key
        // whose name genuinely starts with `/` is not reachable through
        // `record_path`.
        if raw.starts_with('/') {
            return Err(err(RecordPathErrorKind::Rooted));
        }

        let mut segments = Vec::new();
        for (index, segment) in raw.split(syntax.separator()).enumerate() {
            if segment.is_empty() {
                return Err(err(RecordPathErrorKind::EmptySegment { index }));
            }
            if syntax == RecordPathSyntax::Xml && !is_xml_element_name(segment) {
                return Err(err(RecordPathErrorKind::NotAnXmlName {
                    index,
                    segment: segment.to_string(),
                }));
            }
            segments.push(segment.to_string());
        }
        Ok(RecordPath(segments))
    }

    /// The segments to descend, outermost first.
    pub fn segments(&self) -> &[String] {
        &self.0
    }

    /// Consume the path, yielding the segments a reader keeps for matching.
    pub fn into_segments(self) -> Vec<String> {
        self.0
    }
}

/// True when `segment` can name an XML element the reader will meet.
///
/// Namespaces in XML narrows an element name from the XML 1.0 `Name`
/// production to a `QName`: an optional prefix and a local part, each an
/// `NCName`. Splitting on `:` and checking each part with the writer's `Name`
/// predicate gives exactly that — and rejects an XPath axis step
/// (`child::product`), whose two colons no namespace-well-formed document can
/// carry.
fn is_xml_element_name(segment: &str) -> bool {
    let part_ok = |p: &str| !p.is_empty() && is_valid_xml_name(p);
    let mut parts = segment.split(':');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(local), None, _) => part_ok(local),
        (Some(prefix), Some(local), None) => part_ok(prefix) && part_ok(local),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use RecordPathErrorKind as Kind;
    use RecordPathSyntax::{Json, Xml};

    fn parse_err(syntax: RecordPathSyntax, raw: &str) -> RecordPathError {
        RecordPath::parse(syntax, raw).expect_err("must be rejected")
    }

    fn segments(syntax: RecordPathSyntax, raw: &str) -> Vec<String> {
        RecordPath::parse(syntax, raw)
            .unwrap_or_else(|e| panic!("{raw:?} must parse, got: {e}"))
            .into_segments()
    }

    #[test]
    fn accepted_paths_split_exactly_as_the_readers_used_to() {
        // Round-trip against the ad-hoc `split` both readers ran before the
        // grammar existed: every currently-valid path must keep its segments.
        for raw in [
            "records/record",
            "Orders/Order",
            "doc/records/record",
            "PurchaseOrders/Order",
            "root/data/record",
            "records",
            "ns:Root/ns:Item",
            "Item-2/Order.v1",
        ] {
            let old: Vec<String> = raw.split('/').map(String::from).collect();
            assert_eq!(segments(Xml, raw), old, "xml {raw:?}");
        }
        for raw in [
            "data.rows",
            "data.records",
            "batch_records",
            "records",
            "$schema.rows",
            "a/b.rows",
        ] {
            let old: Vec<String> = raw.split('.').map(String::from).collect();
            assert_eq!(segments(Json, raw), old, "json {raw:?}");
        }
    }

    #[test]
    fn rejected_xml_paths_name_their_violation() {
        let cases: &[(&str, Kind)] = &[
            ("", Kind::Empty),
            ("//product", Kind::XPathDescendant),
            ("Orders//Order", Kind::XPathDescendant),
            ("/Orders/Order", Kind::Rooted),
            ("Orders/", Kind::EmptySegment { index: 1 }),
            (
                "//product[@id]",
                // The descendant step is found before the predicate.
                Kind::XPathDescendant,
            ),
            (
                "product[@id]",
                Kind::NotAnXmlName {
                    index: 0,
                    segment: "product[@id]".into(),
                },
            ),
            (
                "child::product",
                Kind::NotAnXmlName {
                    index: 0,
                    segment: "child::product".into(),
                },
            ),
            (
                "*",
                Kind::NotAnXmlName {
                    index: 0,
                    segment: "*".into(),
                },
            ),
            (
                "catalog/pro duct",
                Kind::NotAnXmlName {
                    index: 1,
                    segment: "pro duct".into(),
                },
            ),
            (
                "$.data",
                // The JSONPath marker is named even on an XML source.
                Kind::JsonPathRoot,
            ),
        ];
        for (raw, kind) in cases {
            assert_eq!(parse_err(Xml, raw).kind, *kind, "xml {raw:?}");
        }
    }

    #[test]
    fn rejected_json_paths_name_their_violation() {
        let cases: &[(&str, Kind)] = &[
            ("", Kind::Empty),
            ("$.data", Kind::JsonPathRoot),
            ("$.a.b", Kind::JsonPathRoot),
            ("/data", Kind::Rooted),
            ("//data", Kind::Rooted),
            (".data", Kind::EmptySegment { index: 0 }),
            ("data..rows", Kind::EmptySegment { index: 1 }),
            ("data.", Kind::EmptySegment { index: 1 }),
        ];
        for (raw, kind) in cases {
            assert_eq!(parse_err(Json, raw).kind, *kind, "json {raw:?}");
        }
    }

    #[test]
    fn only_the_exact_jsonpath_prefix_is_rejected() {
        // Narrowing the `$.` marker costs a top-level key literally named `$`
        // followed by a dot; a `$`-prefixed key stays addressable.
        assert_eq!(segments(Json, "$"), vec!["$".to_string()]);
        assert_eq!(
            segments(Json, "$schema.rows"),
            vec!["$schema".to_string(), "rows".to_string()]
        );
    }

    #[test]
    fn messages_carry_the_corrected_path() {
        let cases = [
            (Xml, "//product", "\"product\""),
            (Xml, "/Orders/Order", "\"Orders/Order\""),
            (Xml, "Orders//Order", "\"Orders/Order\""),
            (Xml, "Orders/", "\"Orders\""),
            (Json, "$.data", "\"data\""),
            (Json, "/data", "\"data\""),
            (Json, "data..rows", "\"data.rows\""),
            (Json, "data.", "\"data\""),
        ];
        for (syntax, raw, fixed) in cases {
            let msg = parse_err(syntax, raw).to_string();
            assert!(
                msg.contains(&format!("Write {fixed} instead")),
                "{raw:?}: {msg}"
            );
        }
    }

    #[test]
    fn messages_omit_a_correction_that_is_still_invalid() {
        for (syntax, raw) in [(Xml, "child::product"), (Xml, "//product[@id]"), (Xml, "")] {
            let msg = parse_err(syntax, raw).to_string();
            assert!(!msg.contains("Write "), "{raw:?}: {msg}");
        }
    }

    #[test]
    fn messages_name_the_grammar_the_author_reached_for() {
        assert!(parse_err(Xml, "//product").to_string().contains("XPath"));
        assert!(parse_err(Json, "$.data").to_string().contains("JSONPath"));
        assert!(
            parse_err(Xml, "")
                .to_string()
                .contains("every top-level element becomes one record")
        );
        assert!(
            parse_err(Json, "")
                .to_string()
                .contains("auto-detects the document shape")
        );
    }
}
