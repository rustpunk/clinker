//! Shared envelope-folding primitives for the `Envelope` node's strategies.
//!
//! The consolidation strategies (`concat`, and the upcoming choose-one /
//! folding variants) all face the same first step: a materialized, multi-
//! document body has to be reduced to the *set of headers* it carries, one
//! header per output document. This module owns that reduction so every
//! strategy shares one encoding of what "the body's headers" means rather than
//! re-deriving it.

use clinker_record::{DocumentGrain, EnvelopeRecord, Record};

/// The distinct non-empty headers a materialized body carries, in body order.
///
/// One [`DocumentGrain`] is one output document, and the *first record of a
/// grain* is that document's representative — its
/// [`envelope_record`](clinker_record::DocumentContext::envelope_record) is the
/// document's header. (Each record carries the innermost fully-merged envelope,
/// exactly what a reconstruct-envelope writer attaches per output document, so
/// any record of a grain would do; the first is simply the one reached first.)
///
/// The derivation:
///
/// - **Grain-keyed, not punctuation-keyed.** Headers come from the body
///   *records* grouped by grain, never from `DocumentOpen` punctuations. Ingest
///   mints one open per envelope *level*, so a nested format (X12 ISA → GS → ST)
///   opens several cumulative envelopes for one logical document; counting opens
///   would see three headers where there is one. A grain spanning all three
///   levels is one document with one header (the innermost merged envelope).
///
/// - **Empty headers contribute nothing.** A grain whose representative
///   envelope [`is_empty`](EnvelopeRecord::is_empty) (a headerless or synthetic
///   document) adds no header, so a headless document coexists with a single
///   real header without conflict. A grain with no body records never appears in
///   the input slice at all, so an ancestor-frame / empty-body document — which
///   emits no body records — likewise contributes no header.
///
/// - **Deduped by [`EnvelopeRecord::same_header`], not `PartialEq`.** Two
///   documents whose headers agree on every user-visible field fold to one even
///   when they differ in engine-injected `$`-prefixed keys (an X12 `ISA`
///   control number, a timestamp in the preserved `$raw` bytes). The first
///   occurrence's full [`EnvelopeRecord`] is the one retained, so a downstream
///   writer reconstructs from the first document's raw bytes.
///
/// The result preserves first-seen order. Its length is the count a multi-header
/// consolidation guard rejects on (`>= 2` distinct headers cannot share one
/// consolidated document); its first element is the consolidated header when the
/// body carries exactly one.
///
/// Cost: one pass over `records` (an `O(records)` grain-membership probe set)
/// plus an `O(grains²)` `same_header` dedup — both bounded by the document
/// count, not the record count, because the work per record after the first of
/// its grain is a single hashset hit.
pub(crate) fn distinct_body_headers(records: &[(Record, u64)]) -> Vec<EnvelopeRecord> {
    let mut seen_grains: std::collections::HashSet<DocumentGrain> =
        std::collections::HashSet::new();
    let mut headers: Vec<EnvelopeRecord> = Vec::new();
    for (record, _) in records {
        let doc = record.doc_ctx();
        if !seen_grains.insert(doc.grain()) {
            continue;
        }
        let env = doc.envelope_record();
        if !env.is_empty() && !headers.iter().any(|h| h.same_header(env)) {
            headers.push(env.clone());
        }
    }
    headers
}
