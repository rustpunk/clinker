//! `PlanNode::Envelope` dispatch arm.
//!
//! Frames a body stream into per-document documents.
//!
//! With the [`EnvelopeStrategy::Preserve`] strategy this is a transparent
//! framing stage: it drains the body predecessor's full output and re-parks
//! every record into its successors' slots **with the record's document context
//! and grain unchanged**, forwarding the document-boundary punctuations
//! verbatim. A downstream Output therefore frames on the same grains it would
//! have seen without the node — the `preserve` Envelope is byte-identical to
//! today's per-document framing, now declarable as an explicit composable stage.
//!
//! With the [`EnvelopeStrategy::Concat`] strategy the node consolidates: it
//! mints ONE fresh document context for the whole body and re-stamps every
//! drained record onto it, then re-frames the body with a single
//! `DocumentOpen`/`DocumentClose` pair. A multi-document body therefore frames
//! as one document (one open, one close). The consolidated header is the body
//! grains' common envelope — one grain is one output document, so the distinct
//! non-empty headers are folded from the body RECORDS grouped by grain (each
//! record carries the innermost fully-merged envelope, exactly what a
//! reconstruct-envelope writer attaches per output document). An empty set
//! yields a headerless document and a single member yields that common header.
//! Per-level `DocumentOpen` punctuations and ancestor-frame / empty-body
//! documents emit no body records, so they are framing artifacts that do NOT
//! contribute a header — a nested X12 interchange (ISA/GS/ST) whose three levels
//! each open a cumulative envelope is one body grain with one header, not three.
//! Re-stamping touches only the grain (framing) and the ambient `$doc.*` view —
//! each record's `$source.*` is a real tail column stamped at ingest, so it is
//! untouched and concat is lossless on per-record provenance. Two or more
//! distinct non-empty headers across the body grains is rejected
//! ([`PipelineError::EnvelopeMultiHeaderConflict`], E350) rather than silently
//! shadowed.
//!
//! # Inputs
//!
//! The node always wires a `body` input. When a `header:` port is also wired,
//! the node consumes a second predecessor — a 1-row-per-grain header stream —
//! and **replaces** each body grain's ambient envelope with the matching header
//! record (transform-in-place header replacement, attach-by-grain). The plan's
//! `header_upstream` index identifies which predecessor is the header; the body
//! is the other one. A wired `trailer:` port stays rejected at plan validation.
//!
//! # Memory model
//!
//! The node drains the body predecessor's full `NodeBuffer` and materializes
//! it into its own `NodeBuffer` slot — the same re-park model as Cull and
//! Merge. A wired header stream is drained into a per-grain map (one entry per
//! header document, bounded by the document count, not the record count). The
//! slot's arbitrator wrapper provides the memory bound (it spills the slot when
//! the budget trips), so the resident set is the slot's records, not an
//! incrementally-streamed subset. The node registers no spillable stage
//! consumer of its own.

use std::collections::HashMap;
use std::sync::Arc;

use petgraph::Direction;
use petgraph::graph::NodeIndex;

use crate::executor::dispatch::{
    ExecutorContext, admit_node_buffer, drain_node_buffer_slot, node_buffer_spill_allowed,
};
use crate::executor::envelope::distinct_body_headers;
use crate::executor::node_buffer::DrainedEvents;
use crate::executor::stream_event::{Punctuation, PunctuationKind};
use clinker_plan::config::pipeline_node::EnvelopeStrategy;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode, single_predecessor};
use clinker_record::{DocumentContext, DocumentGrain, DocumentId, EnvelopeRecord, Record};

/// Execute the `Envelope` arm for `node_idx`. Drains the body predecessor and
/// re-parks the framed body into this node's own `node_buffers` slot.
///
/// `Preserve` re-parks every record with its document context and grain
/// unchanged, forwarding the body's punctuations. `Concat` re-stamps every
/// record onto one consolidated document context and re-frames with a single
/// open/close pair. When a `header:` port is wired, both strategies first
/// replace each body grain's ambient envelope with the grain-matched wired
/// header record, so `preserve` frames each document with its replacement
/// header and `concat` folds the now-replaced headers.
///
/// # Errors
///
/// Returns [`PipelineError::EnvelopeMultiHeaderConflict`] (E350) under `Concat`
/// when the body grains carry two or more distinct non-empty envelope headers —
/// one consolidated document cannot frame two headers without dropping one.
///
/// Returns [`PipelineError::EnvelopeHeaderGrainUnmatched`] (E351) when a wired
/// header record carries a grain that grounds to no in-flight body grain (or is
/// the synthetic / ungrounded grain), so the node cannot place it by grain.
///
/// Returns [`PipelineError::EnvelopeHeaderMultipleForGrain`] (E352) when the
/// wired header stream carries two or more records for one body grain — exactly
/// one header record per document grain is required.
pub(crate) fn dispatch_envelope(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError> {
    let PlanNode::Envelope {
        ref name,
        strategy,
        ref header_input,
        header_upstream,
        ..
    } = *node
    else {
        unreachable!("dispatch_envelope called with non-Envelope node");
    };

    // Resolve the body predecessor. With no wired header the Envelope has a
    // single incoming neighbor (the body). With a wired header it has two — the
    // body and the header — distinguished by `header_upstream`; the body is the
    // other one. Draining the body slot returns its records and punctuations.
    let header_pred = if header_input.is_empty() {
        None
    } else {
        Some(resolved_header_predecessor(
            name,
            header_input,
            header_upstream,
        )?)
    };
    let body_pred = match header_pred {
        Some(header_pred) => body_predecessor_excluding(current_dag, node_idx, name, header_pred)?,
        None => single_predecessor(current_dag, node_idx, "envelope", name)?,
    };
    let (mut records, puncts) = match drain_node_buffer_slot(ctx, body_pred) {
        Some(nb) => nb.drain_split()?,
        None => (Vec::new(), Vec::new()),
    };

    // Drain the wired header stream and replace each body grain's ambient
    // envelope with its grain-matched header record. This runs BEFORE the
    // per-strategy step, so `preserve` forwards the replaced headers verbatim
    // and `concat` folds them — the replacement is strategy-independent.
    if let Some(header_pred) = header_pred {
        // The header stream's own punctuations are intentionally dropped: the
        // body's punctuations drive output framing, and the header is a
        // 1-row-per-grain metadata stream whose document boundaries are not part
        // of the framed body.
        let (header_records, _header_puncts) = match drain_node_buffer_slot(ctx, header_pred) {
            Some(nb) => nb.drain_split()?,
            None => (Vec::new(), Vec::new()),
        };
        replace_headers_by_grain(name, &mut records, &header_records)?;
    }

    // Exhaustive over `EnvelopeStrategy`. `preserve` forwards the body
    // (with any replaced headers) unchanged; `concat` consolidates the whole
    // body onto one document; the synthesizing strategies are additive variants
    // that will add their own arms. Both arms re-park into THIS node's own slot
    // — every downstream consumer resolves its input from its predecessor's
    // slot, so writing to `node_idx` is exactly where each successor looks (the
    // Transform/Sort linear-producer convention, not the Route/Cull
    // per-successor fork).
    let (records, puncts) = match strategy {
        EnvelopeStrategy::Preserve => {
            // Leave each record's grain untouched (header replacement, when
            // wired, preserved the grain) and forward the body's punctuations
            // verbatim.
            (records, puncts)
        }
        EnvelopeStrategy::Concat => consolidate(name, records, puncts)?,
    };

    let nb = admit_node_buffer(
        ctx,
        name,
        node_idx,
        records,
        puncts,
        node_buffer_spill_allowed(current_dag, node_idx),
    )?;
    ctx.node_buffers.insert(node_idx, nb);
    Ok(())
}

/// Unwrap the resolved header predecessor index for a node whose `header:` port
/// is wired (non-empty `header_input`).
///
/// # Errors
///
/// Returns [`PipelineError::Internal`] when `header_upstream` is `None` despite
/// a wired header — the resolution post-pass guarantees a wired header that
/// passed E004 validation resolves to a predecessor, so a `None` here is an
/// engine bug, not user error.
fn resolved_header_predecessor(
    name: &str,
    header_input: &str,
    header_upstream: Option<NodeIndex>,
) -> Result<NodeIndex, PipelineError> {
    header_upstream.ok_or_else(|| PipelineError::Internal {
        op: "envelope",
        node: name.to_string(),
        detail: format!(
            "wired header input {header_input:?} did not resolve to a predecessor NodeIndex"
        ),
    })
}

/// Resolve the body predecessor as the node's single incoming neighbor that is
/// NOT the header predecessor. The node has two incoming neighbors when a
/// header is wired — the body and the header.
///
/// # Errors
///
/// Returns [`PipelineError::Internal`] when the node does not have exactly two
/// incoming neighbors, or when neither is the header predecessor — both
/// planner-shape invariants the resolution post-pass guarantees.
fn body_predecessor_excluding(
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    name: &str,
    header_pred: NodeIndex,
) -> Result<NodeIndex, PipelineError> {
    let preds: Vec<NodeIndex> = current_dag
        .graph
        .neighbors_directed(node_idx, Direction::Incoming)
        .collect();
    match preds.as_slice() {
        [a, b] if *a == header_pred => Ok(*b),
        [a, b] if *b == header_pred => Ok(*a),
        [_, _] => Err(PipelineError::Internal {
            op: "envelope",
            node: name.to_string(),
            detail: "resolved header predecessor is not an incoming neighbor".to_string(),
        }),
        _ => Err(PipelineError::Internal {
            op: "envelope",
            node: name.to_string(),
            detail: format!(
                "envelope with a wired header expects exactly 2 predecessors, got {}",
                preds.len()
            ),
        }),
    }
}

/// Replace each body record's ambient envelope with the wired header record
/// carried on the SAME document grain — transform-in-place header replacement.
///
/// The grain is the join key: a header record's
/// [`grain`](clinker_record::DocumentContext::grain) names the body document it
/// frames, and its
/// [`envelope_record`](clinker_record::DocumentContext::envelope_record) is the
/// replacement header. Each matched body record's document context is rebuilt
/// via [`with_replaced_envelope`](clinker_record::DocumentContext::with_replaced_envelope),
/// which keeps the grain (and the rest of the framing identity) while swapping
/// the header — so framing boundaries, which key on grain, are unchanged and
/// only the rendered header differs. A body grain with no wired header keeps its
/// ambient envelope.
///
/// Records re-stamped onto the same grain share one `Arc<DocumentContext>`, so
/// the replacement allocates one context per replaced grain, not per record. The
/// header `EnvelopeRecord` is cloned exactly once per grain — when that single
/// shared context is built — never copied into an intermediate map.
///
/// # Errors
///
/// Returns [`PipelineError::EnvelopeHeaderGrainUnmatched`] (E351) when a header
/// record carries the synthetic grain (an ungrounded, in-pipeline-synthesized
/// record) or a grain that matches no in-flight body document — the node cannot
/// place a header that grounds to no body.
///
/// Returns [`PipelineError::EnvelopeHeaderMultipleForGrain`] (E352) when two or
/// more header records carry the SAME body grain — exactly one header record per
/// document grain is required, so a second is silent data loss, not a fold.
fn replace_headers_by_grain(
    name: &str,
    body: &mut [(Record, u64)],
    header_records: &[(Record, u64)],
) -> Result<(), PipelineError> {
    if header_records.is_empty() {
        return Ok(());
    }

    // The set of grains the body actually carries. A header must ground to one
    // of these; the synthetic grain (an ungrounded record) is never among them.
    let body_grains: std::collections::HashSet<DocumentGrain> =
        body.iter().map(|(r, _)| r.doc_ctx().grain()).collect();

    // Index each body grain to its replacement header record (by position in
    // `header_records`, so the envelope is not cloned here — it is cloned once,
    // later, into the single shared context per grain). Reject a header whose
    // grain grounds to no body document (E351); the synthetic grain fails the
    // same membership test, as it names no source document. Reject a SECOND
    // header on a grain already mapped (E352): one header per grain is the
    // node's locked invariant, and last-write-wins would silently drop the
    // first header.
    let mut header_idx_by_grain: HashMap<DocumentGrain, usize> = HashMap::new();
    for (pos, (header, _)) in header_records.iter().enumerate() {
        let grain = header.doc_ctx().grain();
        if !body_grains.contains(&grain) {
            return Err(PipelineError::EnvelopeHeaderGrainUnmatched {
                envelope: name.to_string(),
                grain: format!("{:?}", grain.document_id()),
            });
        }
        if header_idx_by_grain.insert(grain, pos).is_some() {
            return Err(PipelineError::EnvelopeHeaderMultipleForGrain {
                envelope: name.to_string(),
                grain: format!("{:?}", grain.document_id()),
            });
        }
    }

    // Re-stamp each matched body record with a context carrying the replacement
    // header on the same grain. Cache one rebuilt `Arc<DocumentContext>` per
    // grain so records of a grain share it (a refcount bump, not a re-build);
    // the header envelope is cloned exactly once, into that shared context.
    let mut rebuilt: HashMap<DocumentGrain, Arc<DocumentContext>> = HashMap::new();
    for (record, _) in body.iter_mut() {
        let grain = record.doc_ctx().grain();
        let Some(&header_pos) = header_idx_by_grain.get(&grain) else {
            continue;
        };
        let ctx = rebuilt.entry(grain).or_insert_with(|| {
            let replacement = header_records[header_pos].0.doc_ctx().envelope_record();
            Arc::new(record.doc_ctx().with_replaced_envelope(replacement.clone()))
        });
        record.set_doc_ctx(Arc::clone(ctx));
    }
    Ok(())
}

/// Collapse a multi-document body onto ONE consolidated document context.
///
/// Re-stamps every record's document context to the consolidated one (its grain
/// — the framing key — and its ambient `$doc.*` view become the consolidated
/// document's; each record's `$source.*` is a real tail column from ingest and
/// is untouched), and replaces the incoming punctuations with exactly one
/// `DocumentOpen`/`DocumentClose` pair so the body frames as a single document.
///
/// The consolidated header is the body grains' common envelope: the body
/// records grouped by [`DocumentGrain`] are the output documents (one grain is
/// one document), and each record carries the innermost fully-merged envelope a
/// reconstruct-envelope writer would attach. Folding the distinct non-empty
/// envelopes across grains yields zero (a headerless document), one (that common
/// header, with a colliding empty document coexisting), or — rejected — two or
/// more. Headers are derived from records and NOT from the incoming
/// `DocumentOpen` punctuations: ingest mints one open per envelope *level*, so a
/// nested format (X12 ISA/GS/ST) opens several cumulative envelopes for one
/// document, and ancestor-frame / empty-body documents open with no body records
/// at all — neither is an output document, so neither contributes a header.
///
/// An empty body (no records and no punctuations) emits nothing.
///
/// # Errors
///
/// Returns [`PipelineError::EnvelopeMultiHeaderConflict`] (E350) when two or
/// more distinct non-empty headers would land under the one document.
fn consolidate(
    name: &str,
    mut records: Vec<(Record, u64)>,
    puncts: Vec<Punctuation>,
) -> Result<DrainedEvents, PipelineError> {
    // An empty body frames nothing — no document to open or close.
    if records.is_empty() && puncts.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    // Fold the body grains' headers down to the distinct non-empty set. Headers
    // are deduped by `same_header`, so two documents that agree on every
    // user-visible field fold to one even when their engine-preserved `$raw`
    // bytes differ (e.g. an X12 `ISA` control number) — a structural compare
    // would split them and wrongly raise E350.
    let distinct_headers = distinct_body_headers(&records);

    if distinct_headers.len() >= 2 {
        return Err(PipelineError::EnvelopeMultiHeaderConflict {
            envelope: name.to_string(),
            header_count: distinct_headers.len(),
        });
    }

    // The consolidated document keeps the FIRST header's full `EnvelopeRecord`
    // (engine keys included), so a reconstruct-envelope writer rebuilds from the
    // first document's raw bytes.
    let consolidated_header = distinct_headers
        .into_iter()
        .next()
        .unwrap_or_else(EnvelopeRecord::empty);

    // The consolidated document inherits the first incoming document's source
    // file as its representative identity, preferring the first `DocumentOpen`.
    // That path only seeds the document-DLQ representative and is otherwise
    // informational here; per-record `$source.file` is a tail column stamped at
    // ingest and is unaffected by the re-stamp.
    let source_file = puncts
        .iter()
        .find(|p| p.kind() == PunctuationKind::DocumentOpen)
        .map(|p| Arc::clone(p.source_file()))
        .or_else(|| {
            records
                .first()
                .map(|(r, _)| Arc::clone(r.doc_ctx().source_file()))
        })
        .unwrap_or_else(|| Arc::from(""));

    let ctx = Arc::new(DocumentContext::new(
        DocumentId::next(),
        source_file,
        consolidated_header,
    ));

    // Re-stamp every record's document context to the consolidated one in place
    // — only the grain/`$doc.*` view changes; the `$source.*` tail column is
    // untouched, so concat stays lossless on per-record provenance.
    for (record, _) in records.iter_mut() {
        record.set_doc_ctx(Arc::clone(&ctx));
    }

    // Re-frame: discard the incoming per-document boundaries and emit exactly
    // one open and one close on the consolidated context.
    let framing = vec![
        Punctuation::document_open(Arc::clone(&ctx)),
        Punctuation::document_close(ctx),
    ];

    Ok((records, framing))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::{Schema, SchemaBuilder, Value};
    use indexmap::IndexMap;

    fn body_schema() -> Arc<Schema> {
        SchemaBuilder::with_capacity(1).with_field("id").build()
    }

    /// An `EnvelopeRecord` with one `interchange` section carrying `batch_id`.
    fn header_envelope(batch_id: &str) -> EnvelopeRecord {
        let mut field = IndexMap::new();
        field.insert(Box::from("batch_id"), Value::String(batch_id.into()));
        let mut sections = IndexMap::new();
        sections.insert(Box::from("interchange"), Value::Map(Box::new(field)));
        EnvelopeRecord::from_sections(sections)
    }

    /// A document context for one file, carrying `header_batch_id` as its
    /// ambient `interchange.batch_id`. Its grain is minted from a fresh id.
    fn doc_ctx(file: &str, header_batch_id: &str) -> Arc<DocumentContext> {
        Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from(file),
            header_envelope(header_batch_id),
        ))
    }

    fn body_record(ctx: &Arc<DocumentContext>, id: i64) -> Record {
        let mut r = Record::new(body_schema(), vec![Value::Integer(id)]);
        r.set_doc_ctx(Arc::clone(ctx));
        r
    }

    /// A header record carrying `ctx`'s grain but a REPLACEMENT envelope
    /// (`with_replaced_envelope` preserves the grain, swaps the header). This is
    /// the shape a grain-preserving header rewrite produces: same body grain,
    /// different header values.
    fn header_record(ctx: &Arc<DocumentContext>, replacement: EnvelopeRecord) -> Record {
        let mut r = Record::new(body_schema(), vec![Value::Integer(0)]);
        r.set_doc_ctx(Arc::new(ctx.with_replaced_envelope(replacement)));
        r
    }

    fn batch_id_of(record: &Record) -> Option<Value> {
        record
            .doc_ctx()
            .get_section_field("interchange", "batch_id")
    }

    #[test]
    fn replace_attaches_rewritten_header_to_the_matching_grain() {
        // Two body documents (grain A carries two records, grain B one). The
        // wired header stream carries one rewritten header per grain, on the
        // body's grain. After replacement, each body record's ambient header is
        // the REWRITTEN value, not the body's original ambient header — the
        // headline transform-in-place replacement.
        let ctx_a = doc_ctx("a.x12", "ORIG-A");
        let ctx_b = doc_ctx("b.x12", "ORIG-B");
        let mut body = vec![
            (body_record(&ctx_a, 1), 0),
            (body_record(&ctx_a, 2), 1),
            (body_record(&ctx_b, 3), 2),
        ];
        let headers = vec![
            (header_record(&ctx_a, header_envelope("REWRITTEN-A")), 0),
            (header_record(&ctx_b, header_envelope("REWRITTEN-B")), 1),
        ];

        replace_headers_by_grain("framed", &mut body, &headers)
            .expect("matched headers attach without error");

        // The two grain-A records carry REWRITTEN-A; the grain-B record carries
        // REWRITTEN-B. A no-op (failing to replace) would leave ORIG-* here.
        assert_eq!(
            batch_id_of(&body[0].0),
            Some(Value::String("REWRITTEN-A".into())),
            "grain-A record 1 frames with the rewritten header"
        );
        assert_eq!(
            batch_id_of(&body[1].0),
            Some(Value::String("REWRITTEN-A".into())),
            "grain-A record 2 frames with the rewritten header"
        );
        assert_eq!(
            batch_id_of(&body[2].0),
            Some(Value::String("REWRITTEN-B".into())),
            "grain-B record frames with its own rewritten header, not grain-A's"
        );

        // The grain (the framing key) is preserved exactly — replacement swaps
        // only the header, so a downstream Output frames on the same grains.
        assert_eq!(
            body[0].0.doc_ctx().grain(),
            ctx_a.grain(),
            "replacement preserves the body grain (framing key) for grain A"
        );
        assert_eq!(
            body[2].0.doc_ctx().grain(),
            ctx_b.grain(),
            "replacement preserves the body grain (framing key) for grain B"
        );

        // Records of one grain share a single rebuilt context (one allocation
        // per replaced grain, not per record).
        assert!(
            Arc::ptr_eq(body[0].0.doc_ctx(), body[1].0.doc_ctx()),
            "the two grain-A records share one rebuilt document context"
        );
    }

    #[test]
    fn body_grain_without_a_wired_header_keeps_its_ambient_envelope() {
        // Grain A has a wired replacement header; grain B has none. Grain B must
        // keep its ambient header untouched — replacement is per-matched-grain,
        // not all-or-nothing.
        let ctx_a = doc_ctx("a.x12", "ORIG-A");
        let ctx_b = doc_ctx("b.x12", "ORIG-B");
        let mut body = vec![(body_record(&ctx_a, 1), 0), (body_record(&ctx_b, 2), 1)];
        let headers = vec![(header_record(&ctx_a, header_envelope("REWRITTEN-A")), 0)];

        replace_headers_by_grain("framed", &mut body, &headers)
            .expect("a partial header stream is valid");

        assert_eq!(
            batch_id_of(&body[0].0),
            Some(Value::String("REWRITTEN-A".into())),
            "grain A frames with its replacement header"
        );
        assert_eq!(
            batch_id_of(&body[1].0),
            Some(Value::String("ORIG-B".into())),
            "grain B, having no wired header, keeps its ambient header"
        );
        assert!(
            Arc::ptr_eq(body[1].0.doc_ctx(), &ctx_b),
            "an unreplaced grain keeps its original context Arc untouched"
        );
    }

    #[test]
    fn header_grain_matching_no_body_grain_is_e351() {
        // The wired header carries a grain (file c.x12) absent from the body
        // (files a/b). The node cannot place a header that grounds to no body
        // document, so it aborts with E351 — pin the code AND the variant.
        let ctx_a = doc_ctx("a.x12", "ORIG-A");
        let ctx_orphan = doc_ctx("c.x12", "ORPHAN");
        let mut body = vec![(body_record(&ctx_a, 1), 0)];
        let headers = vec![(
            header_record(&ctx_orphan, header_envelope("REWRITTEN-C")),
            0,
        )];

        let err = replace_headers_by_grain("framed", &mut body, &headers)
            .expect_err("an unmatched header grain must abort");
        let msg = err.to_string();
        assert!(
            msg.contains("E351"),
            "the unmatched-grain abort pins diagnostic code E351, got: {msg}"
        );
        match err {
            PipelineError::EnvelopeHeaderGrainUnmatched { envelope, grain } => {
                assert_eq!(envelope, "framed", "the diagnostic names the node");
                let expected = format!("{:?}", ctx_orphan.grain().document_id());
                assert_eq!(
                    grain, expected,
                    "the payload carries the offending header's grain"
                );
            }
            other => panic!("expected EnvelopeHeaderGrainUnmatched, got: {other:?}"),
        }

        // The body is left untouched on the error path (no partial replacement).
        assert_eq!(
            batch_id_of(&body[0].0),
            Some(Value::String("ORIG-A".into())),
            "a rejected header stream leaves the body's ambient headers intact"
        );
    }

    #[test]
    fn header_with_synthetic_grain_is_e351() {
        // A header record carrying the synthetic (ungrounded) grain — the grain
        // a Transform stamps onto an in-pipeline-synthesized record — grounds to
        // no body document and is rejected, even though the body is non-empty.
        let ctx_a = doc_ctx("a.x12", "ORIG-A");
        let mut body = vec![(body_record(&ctx_a, 1), 0)];
        // A record left on the synthetic context (Record::new default).
        let synthetic_header = Record::new(body_schema(), vec![Value::Integer(0)]);
        assert_eq!(
            synthetic_header.doc_ctx().grain(),
            DocumentGrain::SYNTHETIC,
            "the default-constructed record carries the synthetic grain"
        );
        let headers = vec![(synthetic_header, 0)];

        let err = replace_headers_by_grain("framed", &mut body, &headers)
            .expect_err("a synthetic-grain header must abort");
        assert!(
            matches!(err, PipelineError::EnvelopeHeaderGrainUnmatched { .. }),
            "a synthetic grain is rejected as ungrounded: {err}"
        );
    }

    #[test]
    fn two_headers_on_one_grain_is_e352() {
        // The wired header stream carries TWO records on grain A — a duplicate
        // header for one body document. The node attaches exactly one header per
        // grain and has no fold rule for a second, so it aborts with E352 rather
        // than silently last-write-wins (which would drop the first header). Pin
        // the code AND the variant payload.
        let ctx_a = doc_ctx("a.x12", "ORIG-A");
        let mut body = vec![(body_record(&ctx_a, 1), 0)];
        let headers = vec![
            (header_record(&ctx_a, header_envelope("REWRITE-1")), 0),
            (header_record(&ctx_a, header_envelope("REWRITE-2")), 1),
        ];

        let err = replace_headers_by_grain("framed", &mut body, &headers)
            .expect_err("two headers on one grain must abort");
        let msg = err.to_string();
        assert!(
            msg.contains("E352"),
            "the duplicate-header abort pins diagnostic code E352, got: {msg}"
        );
        match err {
            PipelineError::EnvelopeHeaderMultipleForGrain { envelope, grain } => {
                assert_eq!(envelope, "framed", "the diagnostic names the node");
                let expected = format!("{:?}", ctx_a.grain().document_id());
                assert_eq!(grain, expected, "the payload carries the duplicated grain");
            }
            other => panic!("expected EnvelopeHeaderMultipleForGrain, got: {other:?}"),
        }

        // The body is left untouched on the error path (no partial replacement).
        assert_eq!(
            batch_id_of(&body[0].0),
            Some(Value::String("ORIG-A".into())),
            "a rejected header stream leaves the body's ambient header intact"
        );
    }

    #[test]
    fn an_empty_header_stream_is_a_no_op() {
        // No wired header records → the body passes through unchanged (the
        // strategy then frames with each grain's ambient header).
        let ctx_a = doc_ctx("a.x12", "ORIG-A");
        let mut body = vec![(body_record(&ctx_a, 1), 0)];
        replace_headers_by_grain("framed", &mut body, &[]).expect("empty header stream is valid");
        assert_eq!(
            batch_id_of(&body[0].0),
            Some(Value::String("ORIG-A".into())),
            "an empty header stream leaves the ambient header in place"
        );
        assert!(
            Arc::ptr_eq(body[0].0.doc_ctx(), &ctx_a),
            "an empty header stream leaves the context Arc untouched"
        );
    }
}
