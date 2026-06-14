//! `PlanNode::Envelope` dispatch arm.
//!
//! Frames a body stream into per-document documents. With the
//! [`EnvelopeStrategy::Preserve`] strategy this is a transparent framing
//! stage: it drains the body predecessor's full output and re-parks every
//! record into its successors' slots **with the record's document context and
//! grain unchanged**, forwarding the document-boundary punctuations verbatim.
//! A downstream Output therefore frames on the same grains it would have seen
//! without the node — the `preserve` Envelope is byte-identical to today's
//! per-document framing, now declarable as an explicit composable stage.
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

use petgraph::graph::NodeIndex;

use crate::executor::dispatch::{
    ExecutorContext, admit_node_buffer, drain_node_buffer_slot, node_buffer_spill_allowed,
};
use clinker_plan::config::pipeline_node::EnvelopeStrategy;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode, single_predecessor};

/// Execute the `Envelope` arm for `node_idx`. Drains the body predecessor and,
/// for the `Preserve` strategy, re-parks every record into this node's own
/// `node_buffers` slot with the document context and grain unchanged,
/// forwarding the body's punctuations.
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
    // unchanged; the consolidation and synthesizing strategies are additive
    // variants that will add their own arms.
    match strategy {
        EnvelopeStrategy::Preserve => {
            // Re-park the drained body into THIS node's own slot, leaving each
            // record's document context and grain untouched and forwarding the
            // body's punctuations verbatim. Every downstream consumer resolves
            // its input from its predecessor's slot — a linear consumer
            // (Transform / Aggregate / Output / Sort / Reshape / Cull) drains
            // the predecessor it computes, and a fan-in consumer (Merge /
            // Combine) reads each predecessor slot directly — so writing to
            // `node_idx` is exactly where each successor looks. This mirrors the
            // Transform/Sort linear-producer convention (own-slot write), not
            // the Route/Cull fork convention (per-successor-slot write).
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
    }
}
