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
//! The node has a single wired predecessor this release: the `body` input.
//! The optional `header:` / `trailer:` ports are rejected when wired at plan
//! validation, so [`single_predecessor`] resolves exactly the body stream.
//!
//! # Memory model
//!
//! The node drains the body predecessor's full `NodeBuffer` and materializes
//! it into its own `NodeBuffer` slot — the same re-park model as Cull and
//! Merge. The slot's arbitrator wrapper provides the memory bound (it spills
//! the slot when the budget trips), so the resident set is the slot's records,
//! not an incrementally-streamed subset. The node registers no spillable stage
//! consumer of its own.

use std::sync::Arc;

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
use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord, Record};

/// Execute the `Envelope` arm for `node_idx`. Drains the body predecessor and
/// re-parks the framed body into this node's own `node_buffers` slot.
///
/// `Preserve` re-parks every record with its document context and grain
/// unchanged, forwarding the body's punctuations. `Concat` re-stamps every
/// record onto one consolidated document context and re-frames with a single
/// open/close pair.
///
/// # Errors
///
/// Returns [`PipelineError::EnvelopeMultiHeaderConflict`] (E350) under `Concat`
/// when the body grains carry two or more distinct non-empty envelope headers —
/// one consolidated document cannot frame two headers without dropping one.
pub(crate) fn dispatch_envelope(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError> {
    let PlanNode::Envelope {
        ref name, strategy, ..
    } = *node
    else {
        unreachable!("dispatch_envelope called with non-Envelope node");
    };

    let pred = single_predecessor(current_dag, node_idx, "envelope", name)?;
    let (records, puncts) = match drain_node_buffer_slot(ctx, pred) {
        Some(nb) => nb.drain_split()?,
        None => (Vec::new(), Vec::new()),
    };

    // Exhaustive over `EnvelopeStrategy`. `preserve` forwards the body
    // unchanged; `concat` consolidates the whole body onto one document; the
    // synthesizing strategies are additive variants that will add their own
    // arms. Both arms re-park into THIS node's own slot — every downstream
    // consumer resolves its input from its predecessor's slot, so writing to
    // `node_idx` is exactly where each successor looks (the Transform/Sort
    // linear-producer convention, not the Route/Cull per-successor fork).
    let (records, puncts) = match strategy {
        EnvelopeStrategy::Preserve => {
            // Leave each record's document context and grain untouched and
            // forward the body's punctuations verbatim.
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
