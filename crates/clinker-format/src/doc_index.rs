//! Path-pruned retention of a document's declared `$doc.*` subtrees.
//!
//! A [`DocArenaIndex`] is the format-agnostic accumulator a document
//! reader's envelope pre-scan builds in a single streaming pass. It is
//! seeded from the [`DocPath`] set the planner attributed to one source —
//! the envelope paths some downstream program actually references — and
//! retains *only* the subtrees those paths name. Every section the
//! programs never read is parsed-and-skipped by the reader, never handed
//! to the index, so retained memory scales with the declared paths rather
//! than the document size.
//!
//! The index is parser-agnostic: [`DocArenaIndex::insert`] takes an
//! already-built [`Value`] subtree, so a serde-driven JSON pre-scan and a
//! quick-xml event-driven XML pre-scan feed the same type. Retained bytes
//! are charged incrementally as each subtree is inserted and checked
//! against the configured cap *before* the subtree is stored, so an
//! over-budget document fails loud mid-build rather than after a full
//! materialization — the bounded-memory posture the engine commits to.

use clinker_record::Value;
use cxl::analyzer::doc_paths::DocPath;
use indexmap::IndexMap;

use crate::error::FormatError;

/// Compact, path-pruned retention of a document's declared `$doc.*`
/// subtrees.
///
/// Blocking, bounded: built in one streaming pass by a format reader's
/// envelope pre-scan. Retains ONLY the subtrees named by the declared
/// [`DocPath`] set — every undeclared key/element is parsed-and-skipped by
/// the reader, never stored. Total retained bytes are charged
/// incrementally against `max_index_bytes`; the cap fires mid-build
/// (before OOM), not post-hoc.
pub struct DocArenaIndex {
    /// Field-level retention per section, in first-seen order. Its key set is
    /// the declared section set: a reader's pre-scan consults
    /// [`Self::wants_section`] (an O(1) key lookup here) before descending into
    /// a section, so an undeclared section is skipped without materializing it,
    /// and consults [`Self::wants_field`] so a reader coerces only the fields
    /// some program actually reads. A section maps to `Some(set)` when every
    /// path into it names a concrete field — only those fields are retained. It
    /// maps to `None` (retain all fields) when any path references the whole
    /// section (empty field) or reaches in by index, since an index selects an
    /// element of the already-retained subtree and a whole-section reference
    /// needs every field. Absent from the map ⇒ no path wants the section at
    /// all.
    wanted_fields: IndexMap<Box<str>, Option<Vec<Box<str>>>>,
    /// Retained section subtrees, keyed by section name in insertion
    /// order. One entry per inserted section.
    sections: IndexMap<Box<str>, Value>,
    /// Running sum of the heap-size estimate of every retained subtree.
    retained_bytes: usize,
    /// Hard cap on `retained_bytes`. `None` disables the cap (the reader's
    /// config supplies a finite default in practice).
    max_index_bytes: Option<usize>,
}

impl DocArenaIndex {
    /// Build an index seeded from one source's declared `$doc.*` paths and
    /// an optional retention cap.
    ///
    /// The path set is the planner-attributed set for a single source, so
    /// a multi-source run never tells one source to retain another's
    /// sections. `max_index_bytes` caps total retained bytes; the cap is
    /// checked incrementally in [`Self::insert`].
    pub fn new(declared: &[DocPath], max_index_bytes: Option<usize>) -> Self {
        let mut wanted_fields: IndexMap<Box<str>, Option<Vec<Box<str>>>> = IndexMap::new();
        for path in declared {
            // A whole-section reference (empty field) or any indexed reference
            // promotes the section to retain-all (`None`); a concrete field
            // adds to the section's set unless the section is already
            // retain-all. `IndexMap`'s entry API keeps the first-seen order.
            let entry = wanted_fields
                .entry(path.section.clone())
                .or_insert_with(|| Some(Vec::new()));
            if path.field.is_empty() || !path.indices.is_empty() {
                *entry = None;
            } else if let Some(fields) = entry
                && !fields.iter().any(|f| f.as_ref() == path.field.as_ref())
            {
                fields.push(path.field.clone());
            }
        }
        DocArenaIndex {
            wanted_fields,
            sections: IndexMap::new(),
            retained_bytes: 0,
            max_index_bytes,
        }
    }

    /// `true` when no path was declared — the reader's pre-scan can skip
    /// the document entirely.
    pub fn is_empty(&self) -> bool {
        self.wanted_fields.is_empty()
    }

    /// `true` when some declared path references `section` — the reader
    /// descends into the section; otherwise it skips the section without
    /// materializing its subtree. O(1): the declared section set is exactly
    /// the `wanted_fields` key set.
    pub fn wants_section(&self, section: &str) -> bool {
        self.wanted_fields.contains_key(section)
    }

    /// `true` when some declared path reads `field` of `section` — the reader
    /// coerces and retains the field; otherwise it skips coercing it.
    ///
    /// A section reached only by concrete `(section, field)` paths prunes to
    /// exactly those fields. A section reached by a whole-section reference
    /// (empty field) or any indexed path retains every field (an index selects
    /// an element of the already-retained subtree), so this returns `true` for
    /// any field of such a section. A section no path references at all wants
    /// no fields — but a reader gates on [`Self::wants_section`] first, so this
    /// is reached only for a wanted section.
    pub fn wants_field(&self, section: &str, field: &str) -> bool {
        match self.wanted_fields.get(section) {
            // Retain-all: whole-section or indexed reference.
            Some(None) => true,
            // Concrete field set: retain only the named fields.
            Some(Some(fields)) => fields.iter().any(|f| f.as_ref() == field),
            // Section not referenced by any path.
            None => false,
        }
    }

    /// Charge an already-built `value`'s heap-size estimate against the cap,
    /// then retain it under `path.section`.
    ///
    /// This is the index's authoritative retained-bytes accountant for a
    /// finished subtree, and the parser-agnostic store seam: an XML reader
    /// hands an event-built `Value` here exactly as the JSON reader hands a
    /// coerced one. The byte charge is computed *before* the value is stored
    /// and added to the running total; if the new total would exceed
    /// `max_index_bytes` the value is dropped and an error returns. A reader
    /// that builds a subtree through a streaming parse should *also* bound
    /// the raw parse incrementally (see the JSON reader's byte-counting
    /// pre-scan), so a single oversized declared subtree never fully
    /// materializes before reaching this accountant. A repeated section name
    /// overwrites the prior retention (a reader inserts each section once).
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Json`] naming the offending section and the
    /// cap when retaining `value` would push total retained bytes past
    /// `max_index_bytes`.
    pub fn insert(&mut self, path: &DocPath, value: Value) -> Result<(), FormatError> {
        // `Value::heap_size` is the canonical real-heap-bytes estimate the
        // engine uses for allocation accounting elsewhere; reuse it so the
        // index cap measures the same bytes the RSS budget does. Add the
        // section key's bytes, since the retained map owns that too.
        let charge = path.section.len() + value.heap_size();
        let projected = self.retained_bytes.saturating_add(charge);
        if let Some(cap) = self.max_index_bytes
            && projected > cap
        {
            return Err(FormatError::Json(format!(
                "envelope document-index cap exceeded: retaining section {section:?} \
                 ({charge} bytes) would push the index to {projected} bytes, over the \
                 {cap}-byte `max_index_bytes` cap. Raise `max_index_bytes` on the source, \
                 or narrow the `$doc.*` paths the pipeline reads.",
                section = path.section,
            )));
        }
        self.retained_bytes = projected;
        self.sections.insert(path.section.clone(), value);
        Ok(())
    }

    /// Total heap-size estimate of every retained subtree — a test and
    /// diagnostic hook proving the index retained orders of magnitude less
    /// than the input document.
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Lower the retained subtrees to the envelope-section map a reader's
    /// `prepare_document` returns: section name → its retained [`Value`],
    /// in insertion order.
    pub fn into_sections(self) -> IndexMap<Box<str>, Value> {
        self.sections
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cxl::analyzer::doc_paths::DocIndex;

    fn path(section: &str, field: &str) -> DocPath {
        DocPath {
            section: section.into(),
            field: field.into(),
            indices: Vec::new(),
        }
    }

    fn indexed_path(section: &str, field: &str, indices: Vec<DocIndex>) -> DocPath {
        DocPath {
            section: section.into(),
            field: field.into(),
            indices,
        }
    }

    fn map(pairs: &[(&str, Value)]) -> Value {
        let mut m: IndexMap<Box<str>, Value> = IndexMap::new();
        for (k, v) in pairs {
            m.insert((*k).into(), v.clone());
        }
        Value::Map(Box::new(m))
    }

    #[test]
    fn empty_when_no_paths_declared() {
        let idx = DocArenaIndex::new(&[], Some(64));
        assert!(idx.is_empty());
        assert!(!idx.wants_section("Anything"));
    }

    #[test]
    fn wants_section_matches_declared_section() {
        let idx = DocArenaIndex::new(&[path("Summary", "record_count")], None);
        assert!(!idx.is_empty());
        assert!(idx.wants_section("Summary"));
        assert!(!idx.wants_section("Header"));
    }

    #[test]
    fn distinct_fields_of_one_section_collapse_to_a_single_wanted_section() {
        // Two paths into the same section (different fields) want that one
        // section once — the JSON pre-scan retains the whole section object,
        // so field granularity does not split retention.
        let idx = DocArenaIndex::new(
            &[path("Summary", "record_count"), path("Summary", "checksum")],
            None,
        );
        assert!(idx.wants_section("Summary"));
        assert!(!idx.wants_section("Header"));
    }

    #[test]
    fn indexed_paths_collapse_to_their_section() {
        // `$doc.summary.items[0]` and `$doc.summary.items[1]` both name the
        // `summary` section — section pruning ignores the field and trailing
        // index, which select an element of the already-retained subtree.
        let idx = DocArenaIndex::new(
            &[
                indexed_path("summary", "items", vec![DocIndex::Int(0)]),
                indexed_path("summary", "items", vec![DocIndex::Int(1)]),
            ],
            None,
        );
        assert!(idx.wants_section("summary"));
    }

    #[test]
    fn wants_field_prunes_to_concrete_fields_only() {
        // A section reached by two concrete fields retains exactly those; a
        // sibling field the program never reads is pruned.
        let idx = DocArenaIndex::new(
            &[path("Summary", "record_count"), path("Summary", "checksum")],
            None,
        );
        assert!(idx.wants_field("Summary", "record_count"));
        assert!(idx.wants_field("Summary", "checksum"));
        assert!(!idx.wants_field("Summary", "unread"));
        // A section no path references wants nothing.
        assert!(!idx.wants_field("Header", "anything"));
    }

    #[test]
    fn wants_field_whole_section_reference_retains_all() {
        // An empty-field (whole-section) reference promotes the section to
        // retain-all, so an unnamed field is still wanted.
        let idx = DocArenaIndex::new(&[path("Summary", "")], None);
        assert!(idx.wants_section("Summary"));
        assert!(idx.wants_field("Summary", "anything"));
        assert!(idx.wants_field("Summary", "else_entirely"));
    }

    #[test]
    fn wants_field_indexed_reference_retains_all_of_its_section() {
        // An indexed path selects an element of the already-retained subtree,
        // so the whole section is retained — every field is wanted, even a
        // concrete sibling field declared alongside.
        let idx = DocArenaIndex::new(
            &[
                indexed_path("summary", "items", vec![DocIndex::Int(0)]),
                path("summary", "checksum"),
            ],
            None,
        );
        assert!(idx.wants_field("summary", "items"));
        assert!(idx.wants_field("summary", "checksum"));
        // Retain-all wins even though `checksum` is a concrete field — the
        // indexed reference dominates the section's retention.
        assert!(idx.wants_field("summary", "unread_sibling"));
    }

    #[test]
    fn insert_retains_under_section_name() {
        let mut idx = DocArenaIndex::new(&[path("Summary", "record_count")], None);
        idx.insert(
            &path("Summary", "record_count"),
            map(&[("record_count", Value::Integer(42))]),
        )
        .unwrap();
        let sections = idx.into_sections();
        assert_eq!(sections.len(), 1);
        let summary = match sections.get("Summary").unwrap() {
            Value::Map(m) => m,
            other => panic!("expected map, got {other:?}"),
        };
        assert_eq!(summary.get("record_count"), Some(&Value::Integer(42)));
    }

    #[test]
    fn retained_bytes_grows_monotonically_with_inserts() {
        let mut idx = DocArenaIndex::new(&[path("A", "x"), path("B", "y")], None);
        assert_eq!(idx.retained_bytes(), 0);
        idx.insert(
            &path("A", "x"),
            map(&[("x", Value::String("short".into()))]),
        )
        .unwrap();
        let after_first = idx.retained_bytes();
        assert!(after_first > 0);
        idx.insert(
            &path("B", "y"),
            map(&[("y", Value::String("a".repeat(1000).into()))]),
        )
        .unwrap();
        let after_second = idx.retained_bytes();
        assert!(after_second > after_first);
        // The large string dominates the second charge.
        assert!(after_second - after_first >= 1000);
    }

    #[test]
    fn retained_bytes_charges_heap_backed_string_payload() {
        // A heap-backed (long) string section charges its byte length, so a
        // large declared section is reflected in the running total.
        let mut idx = DocArenaIndex::new(&[path("Big", "blob")], None);
        idx.insert(
            &path("Big", "blob"),
            map(&[("blob", Value::String("x".repeat(5000).into()))]),
        )
        .unwrap();
        assert!(idx.retained_bytes() >= 5000);
    }

    #[test]
    fn insert_errors_when_charge_exceeds_cap() {
        // Cap tiny; a payload above it must be rejected before storage.
        let mut idx = DocArenaIndex::new(&[path("Big", "blob")], Some(64));
        let payload = map(&[("blob", Value::String("z".repeat(10_000).into()))]);
        let err = idx
            .insert(&path("Big", "blob"), payload)
            .expect_err("over-cap insert must error");
        match err {
            FormatError::Json(msg) => {
                assert!(
                    msg.contains("max_index_bytes"),
                    "message names the cap: {msg}"
                );
                assert!(msg.contains("Big"), "message names the section: {msg}");
            }
            other => panic!("expected FormatError::Json, got {other:?}"),
        }
        // The over-cap value was not stored.
        assert!(idx.into_sections().is_empty());
    }

    #[test]
    fn insert_accumulates_until_cap_then_errors() {
        // First insert fits, second tips over the cap — proving the cap is
        // checked against the running total, not per-insert.
        let mut idx = DocArenaIndex::new(&[path("A", "x"), path("B", "y")], Some(300));
        idx.insert(
            &path("A", "x"),
            map(&[("x", Value::String("a".repeat(100).into()))]),
        )
        .expect("first insert fits under the running cap");
        let err = idx
            .insert(
                &path("B", "y"),
                map(&[("y", Value::String("b".repeat(400).into()))]),
            )
            .expect_err("second insert tips the running total over the cap");
        assert!(matches!(err, FormatError::Json(msg) if msg.contains("B")));
        // Only the first section survived.
        let sections = idx.into_sections();
        assert_eq!(sections.len(), 1);
        assert!(sections.contains_key("A"));
    }

    #[test]
    fn no_cap_retains_arbitrarily_large_subtree() {
        let mut idx = DocArenaIndex::new(&[path("Big", "blob")], None);
        idx.insert(
            &path("Big", "blob"),
            map(&[("blob", Value::String("z".repeat(100_000).into()))]),
        )
        .expect("no cap means no rejection");
        assert!(idx.retained_bytes() >= 100_000);
    }

    #[test]
    fn cap_gates_on_a_scalar_map_structural_backing() {
        // A many-field map of small scalars owns almost no key/value heap, so
        // its retention cost is dominated by the IndexMap structural backing.
        // Setting the cap in the gap between the key/value bytes and the full
        // charge pins that the backing term itself decides the cap — a future
        // change to per-entry backing can't move that boundary unnoticed.
        let section = "Summary";
        let build = || {
            map(&[
                ("a", Value::Integer(1)),
                ("b", Value::Integer(2)),
                ("c", Value::Integer(3)),
                ("d", Value::Bool(true)),
                ("e", Value::Integer(5)),
                ("f", Value::Integer(6)),
                ("g", Value::Bool(false)),
                ("h", Value::Integer(8)),
            ])
        };
        // `build()` is deterministic, so every instance has identical capacity
        // and heap_size; derive the caps from one measured instance.
        let payload = build();
        let inner = payload.as_map().unwrap();
        let keys_and_values: usize = inner.iter().map(|(k, v)| k.len() + v.heap_size()).sum();
        let full_value_heap = payload.heap_size();
        assert!(
            full_value_heap > keys_and_values,
            "a scalar map must carry structural backing beyond its key/value bytes"
        );
        let bytes_only_charge = section.len() + keys_and_values;
        let full_charge = section.len() + full_value_heap;

        // A cap above the key/value bytes but below the full charge rejects the
        // insert: the structural backing is what tips it over.
        let mut tight = DocArenaIndex::new(&[path(section, "a")], Some(bytes_only_charge + 1));
        let err = tight
            .insert(&path(section, "a"), build())
            .expect_err("structural backing must push the charge over the cap");
        assert!(matches!(err, FormatError::Json(msg) if msg.contains("max_index_bytes")));
        assert!(tight.into_sections().is_empty());

        // A cap equal to the full charge clears, confirming the rejection above
        // came from the backing term rather than the bytes.
        let mut roomy = DocArenaIndex::new(&[path(section, "a")], Some(full_charge));
        roomy
            .insert(&path(section, "a"), build())
            .expect("full charge fits under a cap equal to it");
        assert_eq!(roomy.retained_bytes(), full_charge);
    }
}
