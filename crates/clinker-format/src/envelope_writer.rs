//! Shared per-document output-envelope reconstruction for the generic
//! formats (CSV / JSON / XML / fixed-width).
//!
//! Each generic [`crate::FormatWriter`] holds an optional [`OutputEnvelopeSpec`]
//! and, when `reconstruct_envelope` is active, renders a per-document header on
//! [`crate::FormatWriter::begin_document`] (echoing a named `$doc` section) and
//! a footer on [`crate::FormatWriter::end_document`] (echoing a named section
//! plus an optional streaming-computed record count). The body streams between
//! them one record at a time — no document is ever buffered, so framing is
//! O(1-record) regardless of document size.
//!
//! This module owns only the format-agnostic parts: the spec, the running
//! record counter, and resolving a section's ordered fields off a
//! [`DocumentContext`]. Each writer renders those fields in its own native
//! shape (a CSV row, a JSON object, an XML element, a fixed-width line).

use clinker_record::{DocumentContext, Value};
use indexmap::IndexMap;

/// Format-local mirror of the plan's `OutputEnvelopeConfig`, carried on each
/// generic writer's config. clinker-format does not depend on clinker-plan, so
/// the executor's writer registry maps the plan config onto this struct when
/// it builds a writer.
///
/// All three fields are optional and independent: a header-only envelope sets
/// only `header_from_doc`; a footer that just stamps a count sets
/// `footer_from_doc` + `footer_record_count_field`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutputEnvelopeSpec {
    /// `$doc` section echoed as the per-document header (before the body).
    pub header_from_doc: Option<String>,
    /// `$doc` section echoed as the per-document footer (after the body).
    pub footer_from_doc: Option<String>,
    /// Field name under which the streaming-computed body record count is
    /// injected into the footer. Requires `footer_from_doc`.
    pub footer_record_count_field: Option<String>,
}

impl OutputEnvelopeSpec {
    /// `true` when nothing is declared — the writer renders no framing.
    pub fn is_empty(&self) -> bool {
        self.header_from_doc.is_none()
            && self.footer_from_doc.is_none()
            && self.footer_record_count_field.is_none()
    }
}

/// Per-document envelope state held by a writer across the document's body.
///
/// Holds the active spec and a single running record counter — never any body
/// record — so the writer's footprint stays O(1). `begin` resets the count for
/// the new document; each body record increments it; `end` reads it into the
/// computed footer field.
#[derive(Debug, Default)]
pub struct EnvelopeFramer {
    spec: OutputEnvelopeSpec,
    /// Body records written for the currently-open document. Reset on each
    /// `begin_document`, incremented per record, read by the footer.
    record_count: u64,
}

impl EnvelopeFramer {
    /// Build a framer for `spec`. A writer constructs one only when its config
    /// carries a non-empty spec under an active `reconstruct_envelope`.
    pub fn new(spec: OutputEnvelopeSpec) -> Self {
        Self {
            spec,
            record_count: 0,
        }
    }

    /// Reset the body record counter at the start of a new document. Call from
    /// `begin_document` before rendering the header.
    pub fn begin(&mut self) {
        self.record_count = 0;
    }

    /// Count one body record for the open document. Call from `write_record`.
    pub fn count_record(&mut self) {
        self.record_count = self.record_count.saturating_add(1);
    }

    /// The header section's ordered fields for `doc`, or `None` when no header
    /// is configured or the named section is absent from this document.
    pub fn header_fields<'a>(
        &self,
        doc: &'a DocumentContext,
    ) -> Option<&'a IndexMap<Box<str>, Value>> {
        self.spec
            .header_from_doc
            .as_deref()
            .and_then(|name| doc.section_fields(name))
    }

    /// The footer section's ordered fields for `doc`, or `None` when no footer
    /// is configured or the named section is absent. The streaming-computed
    /// count (if `footer_record_count_field` is set) is appended SEPARATELY by
    /// [`Self::footer_count`] so a writer renders both without cloning the
    /// section map.
    pub fn footer_fields<'a>(
        &self,
        doc: &'a DocumentContext,
    ) -> Option<&'a IndexMap<Box<str>, Value>> {
        self.spec
            .footer_from_doc
            .as_deref()
            .and_then(|name| doc.section_fields(name))
    }

    /// The `(field_name, count_value)` pair to append to the footer, or `None`
    /// when no computed count is configured. The count is the number of body
    /// records written for the document just closed.
    pub fn footer_count(&self) -> Option<(&str, i64)> {
        self.spec
            .footer_record_count_field
            .as_deref()
            .map(|field| (field, self.record_count as i64))
    }

    /// Whether a footer should be emitted at all — either a named section is
    /// configured, or a computed count is (a count with no section still
    /// emits a footer carrying just the count).
    pub fn has_footer(&self) -> bool {
        self.spec.footer_from_doc.is_some() || self.spec.footer_record_count_field.is_some()
    }

    /// Whether a header section is configured.
    pub fn has_header(&self) -> bool {
        self.spec.header_from_doc.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::DocumentId;
    use std::sync::Arc;

    fn section(fields: &[(&str, Value)]) -> Value {
        let mut m: IndexMap<Box<str>, Value> = IndexMap::new();
        for (k, v) in fields {
            m.insert(Box::from(*k), v.clone());
        }
        Value::Map(Box::new(m))
    }

    fn doc_with(section_name: &str, fields: &[(&str, Value)]) -> DocumentContext {
        let mut sections = IndexMap::new();
        sections.insert(Box::from(section_name), section(fields));
        DocumentContext::new(DocumentId::next(), Arc::from("f.csv"), sections)
    }

    #[test]
    fn header_and_footer_fields_resolve_in_declared_order() {
        let doc = doc_with(
            "Head",
            &[
                ("batch_id", Value::String("B1".into())),
                ("run_date", Value::String("2026-06-13".into())),
            ],
        );
        let framer = EnvelopeFramer::new(OutputEnvelopeSpec {
            header_from_doc: Some("Head".into()),
            ..Default::default()
        });
        let fields = framer.header_fields(&doc).expect("header section present");
        let names: Vec<&str> = fields.keys().map(|k| k.as_ref()).collect();
        assert_eq!(names, vec!["batch_id", "run_date"]);
        assert!(framer.has_header());
        assert!(!framer.has_footer());
    }

    #[test]
    fn missing_section_resolves_none() {
        let doc = doc_with("Head", &[("x", Value::Integer(1))]);
        let framer = EnvelopeFramer::new(OutputEnvelopeSpec {
            header_from_doc: Some("Nope".into()),
            ..Default::default()
        });
        assert!(framer.header_fields(&doc).is_none());
    }

    #[test]
    fn footer_count_tracks_body_records_and_resets_per_document() {
        let mut framer = EnvelopeFramer::new(OutputEnvelopeSpec {
            footer_from_doc: Some("Foot".into()),
            footer_record_count_field: Some("count".into()),
            ..Default::default()
        });
        framer.begin();
        framer.count_record();
        framer.count_record();
        framer.count_record();
        assert_eq!(framer.footer_count(), Some(("count", 3)));
        // A new document resets the counter.
        framer.begin();
        framer.count_record();
        assert_eq!(framer.footer_count(), Some(("count", 1)));
        assert!(framer.has_footer());
    }

    #[test]
    fn empty_spec_is_empty() {
        assert!(OutputEnvelopeSpec::default().is_empty());
        assert!(
            !OutputEnvelopeSpec {
                header_from_doc: Some("H".into()),
                ..Default::default()
            }
            .is_empty()
        );
    }
}
