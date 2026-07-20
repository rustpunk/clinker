//! `PlanNode::Cull` dispatch arm.
//!
//! Per-correlation-group record removal with a side-output port. Blocking
//! grouping operator: it drains the predecessor's full output, groups
//! records by `partition_by`, evaluates each rule's group-level
//! `drop_group_when` predicate over the whole group, and routes every
//! record of a dropped group to the `removed_to` output port while every
//! record of an untriggered group flows to the main output port. The
//! dispatcher's `Cull` arm is a single delegating call into
//! [`dispatch_cull`].
//!
//! # Two output ports (not the DLQ)
//!
//! `removed_to` is a first-class producer-side output port, mirroring the
//! Route fan-out seam — NOT the dead-letter queue. Removed records are not
//! errors; they are valid rows the operator deliberately partitions onto a
//! second stream (an audit sink, a reprocessing branch). The split is
//! resolved off the live edge graph: each outgoing edge carries a
//! [`producer_port`](clinker_plan::plan::execution::PlanEdge::producer_port)
//! tag, so an edge tagged with the `removed_to` port name receives removed
//! rows and every other edge (the bare main reference, tagged `None`)
//! receives kept rows. Both ports carry the unchanged upstream schema —
//! Cull does not widen. Per-port output volume is counted downstream at the
//! Output nodes each port feeds (the same place Route's branch volumes are
//! counted), not at this arm.
//!
//! # Memory model
//!
//! Blocking and grouped: the whole input materializes into per-group
//! buffers before any output row leaves, because the group-level
//! `drop_group_when` predicate is an aggregate property of the whole group
//! and cannot be folded into a per-record keep/remove decision. The buffer
//! is governed by the central
//! [`MemoryArbitrator`](crate::pipeline::memory::MemoryArbitrator): a
//! registered [`CullConsumer`] reports the live group-buffer bytes, and when
//! the soft threshold trips the arm spills whole input records through a
//! [`SpillWriter`], then re-splits the reloaded group at finalize. A single
//! oversized group is spilled incrementally (sliced by the upper bits of
//! each record's admission sequence) so the ingest-time resident peak stays
//! bounded under skew. The consumer registers at priority `15` (between
//! grace-hash and external sort) and cannot back-pressure: there is no
//! upstream channel to gate once the predecessor has drained.
//!
//! The drop decision per group is computed by folding the same records
//! through an in-memory aggregate over the predicate (group-by =
//! `partition_by`); that aggregate state is O(groups) and is not spilled —
//! only the raw buffered records (which dominate the footprint) spill. The
//! decision state cannot spill either (Cull has no upstream channel to gate,
//! so it can neither back-pressure nor evict this state), so its live
//! footprint is instead read against the hard budget on every admission and
//! fails loud with [`PipelineError::MemoryBudgetExceeded`] once the group
//! cardinality would carry it past the ceiling — the many-distinct-groups
//! analogue of the single-giant-group finalize failure below.
//!
//! A single correlation group whose reloaded footprint exceeds the finalize
//! budget fails loud with a [`PipelineError`] rather than risking an
//! out-of-memory crash on reload.

use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::{GroupByKey, Record, Schema, SchemaBuilder, Value};
use cxl::eval::ProgramEvaluator;
use petgraph::Direction;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;

use crate::executor::dispatch::{
    ExecutorContext, NodeBufferKey, admit_node_buffer, drain_node_buffer_slot,
    node_buffer_spill_allowed, source_file_arc_of, source_name_arc_of,
};
use crate::pipeline::memory::{
    ConsumerHandle, ConsumerSpillError, MemoryArbitrator, MemoryConsumer,
};
use crate::pipeline::spill::{SpillFile, SpillWriter};
use clinker_plan::BudgetCategory;
use clinker_plan::config::pipeline_node::CullBody;
use clinker_plan::config::{SortField, SortOrder};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode, single_predecessor};

/// Spill priority for the Cull group buffer: between grace-hash (`10`) and
/// external sort (`20`), matching Reshape. A grouped record buffer is
/// costlier to evict than grace partitions (reload re-splits the group) but
/// cheaper than an external-sort merge. The plan-time mirror in
/// `arbitration_class` returns the same constant.
const CULL_SPILL_PRIORITY: i32 = 15;

/// Maximum partition fan-out used to slice one oversized group across
/// successive spill waves. 12 bits (4096 partitions) matches the grace-hash
/// cap: beyond it, per-file overhead outweighs further skew reduction.
const MAX_SKEW_PARTITION_BITS: u32 = 12;

// Synthetic output column the decision aggregate emits the per-group removal
// decision into. Shared with the compile-time binder, which builds the
// `emit <col> = (rule_1) or …` decision program with this target — both sides
// reference the one constant in clinker-plan so they cannot drift.
use clinker_plan::config::pipeline_node::CULL_DROP_DECISION_COLUMN as DROP_DECISION_COLUMN;

/// Arbitrator-facing wrapper over the Cull group buffer.
///
/// Blocking operator: reports the live group-buffer byte footprint through a
/// shared [`ConsumerHandle`] and cannot back-pressure (no upstream channel
/// remains once the predecessor has drained). On `try_spill` it flips the
/// handle's spill-request flag; the dispatch loop reads it at the next
/// grouping boundary and evicts resident groups to disk in-thread.
struct CullConsumer {
    handle: Arc<ConsumerHandle>,
}

impl CullConsumer {
    fn new(handle: Arc<ConsumerHandle>) -> Self {
        Self { handle }
    }
}

impl MemoryConsumer for CullConsumer {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }

    fn spill_priority(&self) -> i32 {
        CULL_SPILL_PRIORITY
    }

    fn try_spill(&self, target_bytes: u64) -> Result<u64, ConsumerSpillError> {
        self.handle.request_spill();
        let bytes = self.handle.bytes();
        if bytes >= target_bytes {
            Ok(bytes)
        } else {
            Err(ConsumerSpillError::BelowTarget {
                target: target_bytes,
                freed: bytes,
            })
        }
    }

    fn can_back_pressure(&self) -> bool {
        false
    }
}

/// Execute the `Cull` arm for `node_idx`. Drains the predecessor, groups by
/// `partition_by`, evaluates each group's `drop_group_when` predicate over
/// the whole group, and splits kept vs. removed records onto the main and
/// `removed_to` output ports. Blocking: the full input materializes before
/// the first output row leaves.
pub(crate) fn dispatch_cull(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError> {
    let PlanNode::Cull {
        ref name,
        ref config,
        ref output_schema,
        ref compiled,
        ref typed,
        ..
    } = *node
    else {
        unreachable!("dispatch_cull called with non-Cull node");
    };

    let pred = single_predecessor(current_dag, node_idx, "cull", name)?;
    let (input, input_puncts): (
        Vec<(Record, u64)>,
        Vec<crate::executor::stream_event::Punctuation>,
    ) = match drain_node_buffer_slot(ctx, pred) {
        Some(nb) => nb.drain_split()?,
        None => (Vec::new(), Vec::new()),
    };

    if input.is_empty() {
        // Empty-input fast path returns BEFORE any consumer registers, so
        // there is nothing to deregister: a `ConsumerId` left registered on
        // an early return inflates the run's peak for every downstream stage.
        // Both ports still receive the punctuation broadcast so each
        // downstream subgraph sees the document boundaries.
        emit_ports(
            ctx,
            current_dag,
            node_idx,
            name,
            config,
            Vec::new(),
            Vec::new(),
            input_puncts,
        )?;
        return Ok(());
    }

    // Register the group buffer with the arbitrator only after the
    // empty-input guard, so the no-work path never leaks a consumer.
    let handle = ConsumerHandle::new();
    let consumer_id = ctx
        .memory_budget
        .register_consumer(Arc::new(CullConsumer::new(handle.clone())));

    // Every exit path past this point must deregister `consumer_id`, so the
    // grouping/finalize work runs inside a helper whose result is matched
    // below — success and hard error both funnel through the single
    // `unregister_consumer` call.
    let result = run_cull_grouped(
        ctx,
        current_dag,
        node_idx,
        name,
        config,
        output_schema,
        compiled,
        typed,
        input,
        input_puncts,
        &handle,
    );
    ctx.memory_budget.unregister_consumer(consumer_id);
    result
}

/// Group the drained input, spilling resident groups to disk under memory
/// pressure, compute the per-group `drop_group_when` decision, then split
/// each group's records (reloading spilled groups) onto the main and
/// `removed_to` output ports.
///
/// Split out of [`dispatch_cull`] so the consumer deregistration there
/// covers every exit — success and hard error both return through this
/// function's `Result`.
#[allow(clippy::too_many_arguments)]
fn run_cull_grouped(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    name: &str,
    config: &CullBody,
    output_schema: &Arc<Schema>,
    compiled: &Arc<cxl::plan::CompiledAggregate>,
    typed: &Arc<cxl::typecheck::TypedProgram>,
    input: Vec<(Record, u64)>,
    input_puncts: Vec<crate::executor::stream_event::Punctuation>,
    handle: &Arc<ConsumerHandle>,
) -> Result<(), PipelineError> {
    // The schema is uniform across a node_buffer slot, so the first record's
    // schema drives the spill schema for the raw-record group buffer.
    let input_schema = input[0].0.schema().clone();

    // Compute the per-group removal decision by folding every record through
    // an in-memory aggregate over the OR of all rules' `drop_group_when`
    // predicates (group-by = `partition_by`), built once at lowering and
    // carried on the node. The aggregate's per-group state is O(groups) and
    // is not spilled — only the raw buffered records (which dominate the
    // footprint) spill, below. That unspilled O(groups) state is instead
    // bounded by a hard-limit gate inside `compute_drop_decisions`: an
    // unbounded group cardinality fails loud rather than growing it uncounted.
    let drop_decisions = compute_drop_decisions(ctx, name, config, compiled, typed, &input)?;

    let spill_compress = ctx
        .spill_compress
        .resolve_for_schema(output_schema.column_count(), ctx.batch_size as u64);

    let budget = Arc::clone(&ctx.memory_budget);
    let spill_root = Arc::clone(&ctx.spill_root_path);

    let mut buffer = CullGroupBuffer::new(Arc::clone(&input_schema), spill_compress);
    for (record, row_num) in input {
        let key = partition_key(name, &record, &config.partition_by)?;
        buffer.push(key, record, row_num);
        handle.set_bytes(buffer.resident_bytes() as u64);
        // Poll for self-spill pressure at every admission. `should_spill_self`
        // updates the peak and reports the soft-threshold crossing WITHOUT
        // running the pausing arbitration round: Cull relieves pressure by
        // spilling its OWN buffer in-thread, and that round can elect and
        // pause a back-pressureable Source in a concurrent DAG branch that
        // Cull never resumes — the deadlock the hash aggregator avoids the
        // same way. `take_spill_request` still honors a spill nudge another
        // stage's arbitration round set on this consumer.
        if budget.should_spill_self() || handle.take_spill_request() {
            buffer.spill_until_under_budget(name, &budget, &spill_root, handle)?;
            handle.set_bytes(buffer.resident_bytes() as u64);
        }
    }

    // Finalize: stream each group back in first-seen order (from memory when
    // it never spilled, else reloaded from its spill files and restored to
    // arrival order), then route every record of the group to the kept or
    // removed bucket per the precomputed decision. The hard limit gates a
    // single correlation group too large to observe whole.
    let hard_limit = budget.hard_limit();
    let mut kept: Vec<(Record, u64)> = Vec::new();
    let mut removed: Vec<(Record, u64)> = Vec::new();
    let group_order = buffer.take_group_order();
    for key in group_order {
        let mut group = buffer.take_group(name, &key, hard_limit)?;
        handle.set_bytes(buffer.resident_bytes() as u64);
        if !config.order_by.is_empty() {
            sort_group(&mut group, &config.order_by);
        }
        // Every buffered group must have a computed decision: the decision
        // aggregate is keyed by the same `partition_key`, over the same
        // records, so a missing key is an internal invariant violation — fail
        // loud rather than silently default a should-be-removed group to the
        // main port.
        let Some(&drop) = drop_decisions.get(&key) else {
            return Err(PipelineError::Internal {
                op: "cull",
                node: name.to_string(),
                detail: "a buffered correlation group has no computed removal decision — the \
                         dispatch buffer and the decision aggregate diverged on the partition key"
                    .to_string(),
            });
        };
        if drop {
            removed.append(&mut group);
        } else {
            kept.append(&mut group);
        }
    }

    emit_ports(
        ctx,
        current_dag,
        node_idx,
        name,
        config,
        kept,
        removed,
        input_puncts,
    )
}

/// Fold every input record through an in-memory aggregate over the OR of all
/// rules' `drop_group_when` predicates and return a per-group removal
/// decision keyed identically to the dispatch buffer.
///
/// The predicate program is `emit <decision> = (rule_1) or (rule_2) or …`,
/// typechecked and run in aggregate context (group-by = `partition_by`), so
/// an aggregate predicate such as `count(*) > 100` is well-formed. The
/// finalized per-group record carries the group-by columns and the boolean
/// decision; the decision map is keyed by `partition_key` over that record so
/// it matches the dispatch buffer's group keys exactly.
///
/// # Memory
///
/// The aggregate and the decision map it feeds are both O(distinct groups) and
/// run fully in memory (`budget: 0`, `spill_dir: None`) — the raw-record
/// dispatch buffer carries the spill burden, this state does not. Because Cull
/// cannot back-pressure or spill this state, the aggregate's live footprint is
/// read against the run's hard limit on every admission, and the decision
/// map's estimated footprint is checked before it is built; either overflowing
/// the budget returns [`PipelineError::MemoryBudgetExceeded`] rather than
/// growing the state uncounted toward an out-of-memory crash.
fn compute_drop_decisions(
    ctx: &mut ExecutorContext<'_>,
    name: &str,
    config: &CullBody,
    compiled: &Arc<cxl::plan::CompiledAggregate>,
    typed: &Arc<cxl::typecheck::TypedProgram>,
    input: &[(Record, u64)],
) -> Result<HashMap<Vec<GroupByKey>, bool>, PipelineError> {
    use crate::aggregation::{AggregateStream, AggregatorConfig, SortRow};

    let evaluator = ProgramEvaluator::new(Arc::clone(typed), false);

    // Output schema = partition-by columns ++ the boolean decision column.
    // Spill schema is unused (the predicate aggregate runs in-memory: budget
    // 0 disables the group-count cap and `spill_dir: None` keeps every group
    // resident), but `AggregatorConfig` still requires a value.
    let drop_output_schema: Arc<Schema> = config
        .partition_by
        .iter()
        .map(|s| Box::<str>::from(s.as_str()))
        .chain([Box::<str>::from(DROP_DECISION_COLUMN)])
        .collect::<SchemaBuilder>()
        .build();
    let spill_schema: Arc<Schema> = config
        .partition_by
        .iter()
        .map(|s| Box::<str>::from(s.as_str()))
        .chain([Box::<str>::from("__acc_state")])
        .collect::<SchemaBuilder>()
        .build();

    let handle = ConsumerHandle::new();
    let mut stream = AggregateStream::for_node(
        clinker_plan::plan::types::AggregateStrategy::Hash,
        AggregatorConfig {
            compiled: Arc::clone(compiled),
            evaluator,
            output_schema: Arc::clone(&drop_output_schema),
            spill_schema,
            // In-memory only: the raw-record buffer carries the spill burden,
            // and the aggregate state is O(groups) — never spilled. The handle
            // mirrors that live O(groups) footprint so the ingest loop below
            // can gate it against the hard limit; the aggregate can neither
            // spill (`spill_dir: None`) nor back-pressure, so an unbounded group
            // cardinality must fail loud rather than grow the state uncounted.
            memory_budget: 0,
            spill_dir: None,
            spill_compress: false,
            transform_name: format!("cull:{name}"),
            consumer_handle: Arc::clone(&handle),
            // The predicate aggregate runs in-memory (`budget: 0`,
            // `spill_dir: None`) so it never spills, but `AggregatorConfig`
            // requires an arbitrator; the run's shared one is the natural fit.
            arbitrator: Arc::clone(&ctx.memory_budget),
        },
    )?;

    // The decision aggregate cannot spill (`budget: 0`, `spill_dir: None`) or
    // back-pressure, so its O(groups) accumulator state is arbitrator-invisible
    // and grows unchecked with the group cardinality. Gate the live footprint
    // the aggregate mirrors into `handle` against the run's hard limit on every
    // admission and fail loud once it would carry the state past the ceiling. A
    // `hard_limit` of 0 means "no limit" (matching the finalize giant-group
    // gate in `take_group`), so the read is skipped.
    let hard_limit = ctx.memory_budget.hard_limit();
    let mut emitted: Vec<SortRow> = Vec::new();
    for (record, row_num) in input {
        let source_file = source_file_arc_of(record);
        let source_name = source_name_arc_of(record);
        let eval_ctx =
            ctx.eval_ctx_for_record(&source_file, &source_name, *row_num, record.doc_ctx());
        stream
            .add_record(record, *row_num, &eval_ctx, &mut emitted)
            .map_err(|e| cull_predicate_error(name, e))?;
        let live = handle.bytes();
        if hard_limit > 0 && live > hard_limit {
            return Err(cull_decision_budget_error(name, live, hard_limit));
        }
    }
    let finalize_ctx = ctx.merged_eval_ctx();
    stream
        .finalize(&finalize_ctx, &mut emitted)
        .map_err(|e| cull_predicate_error(name, e))?;

    // Each finalized row carries the group-by columns and the decision.
    // Re-derive the group key from those columns with the SAME `partition_key`
    // the dispatch buffer uses, so a buffered group and its decision can never
    // key differently. The finalized record's group-by columns hold the
    // group's key values verbatim (the aggregate stamps each group-by column
    // from its key tuple), so this reproduces the buffer key exactly.
    //
    // The decisions map is a second O(groups) structure — one entry per
    // emitted group, each a `Vec<GroupByKey>` key plus a bool in a hash slot.
    // The ingest gate above bounds the aggregate's own state; estimate this
    // derived map's resident footprint and fail loud before allocating it so a
    // group cardinality that would overflow the budget aborts cleanly here too.
    let per_entry = std::mem::size_of::<(Vec<GroupByKey>, bool)>()
        + config.partition_by.len() * std::mem::size_of::<GroupByKey>();
    let decisions_bytes = (emitted.len() as u64).saturating_mul(per_entry as u64);
    if hard_limit > 0 && decisions_bytes > hard_limit {
        return Err(cull_decision_budget_error(
            name,
            decisions_bytes,
            hard_limit,
        ));
    }
    let mut decisions: HashMap<Vec<GroupByKey>, bool> = HashMap::with_capacity(emitted.len());
    for (record, _) in emitted {
        let key = partition_key(name, &record, &config.partition_by)?;
        let drop = matches!(record.get(DROP_DECISION_COLUMN), Some(Value::Bool(true)));
        decisions.insert(key, drop);
    }
    Ok(decisions)
}

/// Deliver the kept and removed record sets to their producer-side output
/// ports. A successor whose incoming edge is tagged with the `removed_to` port
/// draws removed rows; every other successor (the bare main reference, tagged
/// `None`) draws kept rows.
///
/// Where the records land depends on how the successor reads its input. A
/// single-output consumer (Transform / Output / Reshape / Cull) checks its own
/// `node_buffers` slot first, so its port-selected records go into the
/// successor's own slot (`(succ, None)`). A multi-input consumer (Merge /
/// Combine) reads its *predecessor's* slot directly, keyed by the incoming
/// edge's producer port, so each distinct producer port lands in this Cull's
/// own `(node_idx, Some(port))` slot — the `main` and `removed_to` ports stay
/// separate, which a Combine build side depends on. Both ports broadcast
/// `input_puncts` so each downstream subgraph drives its own per-document
/// accumulators.
#[allow(clippy::too_many_arguments)]
fn emit_ports(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    name: &str,
    config: &CullBody,
    kept: Vec<(Record, u64)>,
    removed: Vec<(Record, u64)>,
    input_puncts: Vec<crate::executor::stream_event::Punctuation>,
) -> Result<(), PipelineError> {
    // The edge's `producer_port` tag selects the record set: `Some(removed_to)`
    // → removed rows, anything else (the untagged main reference, or an
    // explicit `main`) → kept rows.
    let removed_port = config.removed_to.as_str();
    let is_removed_port = |port: Option<&str>| matches!(port, Some(p) if p == removed_port);

    let cull_region_producer = current_dag.deferred_region_at(node_idx).map(|r| r.producer);
    let active_body = ctx.window_runtime.active_stack.last().copied();

    // Predecessor-slot readers (Merge / Combine) drain by incoming edge, so one
    // slot per distinct producer output port lands in this Cull's own slot
    // keyed `(node_idx, Some(port))`. Materialize each port's set once (multiple
    // consumers of the same port share the slot, as they did pre-port). Own-slot
    // readers admit inline into their own slot.
    let mut pred_ports: Vec<Option<Box<str>>> = Vec::new();
    // Collect outgoing edges first so the mutable `ctx` admissions below do not
    // overlap the immutable graph borrow.
    let outgoing: Vec<(NodeIndex, Option<Box<str>>, petgraph::graph::EdgeIndex)> = current_dag
        .graph
        .edges_directed(node_idx, Direction::Outgoing)
        .map(|edge| {
            (
                edge.target(),
                edge.weight().producer_port.as_deref().map(Box::from),
                edge.id(),
            )
        })
        .collect();

    for (succ, port, edge_id) in outgoing {
        let records: &Vec<(Record, u64)> = if is_removed_port(port.as_deref()) {
            &removed
        } else {
            &kept
        };
        // Cross-region tee: a successor inside a deferred region this Cull is
        // not in needs this port's records parked on the matching outgoing edge
        // so the commit-time deferred dispatcher receives exactly them.
        // In-region and non-deferred edges skip the tee; their `node_buffers`
        // slot already covers them.
        let succ_region_producer = current_dag.deferred_region_at(succ).map(|r| r.producer);
        let crosses = match (cull_region_producer, succ_region_producer) {
            (None, Some(_)) => true,
            (Some(p), Some(t)) if p != t => true,
            _ => false,
        };
        if crosses {
            let row_bytes_each: u64 = records
                .first()
                .map(|(rec, _)| {
                    (std::mem::size_of::<Value>() * rec.schema().column_count()
                        + std::mem::size_of::<(Record, u64)>()) as u64
                })
                .unwrap_or(0);
            for (record, rn) in records {
                // Fail loud if an oversized cross-region tee would blow the
                // budget, mirroring the Route tee — parking unbounded records
                // into `region_input_buffers` must not silently overshoot.
                if row_bytes_each > 0 && ctx.memory_budget.should_abort() {
                    return Err(PipelineError::MemoryBudgetExceeded {
                        node: name.to_string(),
                        used: ctx.memory_budget.peak_rss().unwrap_or(0),
                        limit: ctx.memory_budget.hard_limit(),
                        source: BudgetCategory::Arena,
                        detail: Some("Cull cross-region tee admission".to_string()),
                    });
                }
                ctx.region_input_buffers
                    .entry((active_body, edge_id))
                    .or_default()
                    .push((record.clone(), *rn));
            }
        }
        if reads_predecessor_slot(&current_dag.graph[succ]) {
            if !pred_ports.contains(&port) {
                pred_ports.push(port);
            }
        } else {
            // Own-slot reader: its port-selected records go into its own slot.
            let records = records.clone();
            let nb = admit_node_buffer(
                ctx,
                name,
                succ,
                records,
                input_puncts.clone(),
                node_buffer_spill_allowed(current_dag, succ),
            )?;
            ctx.node_buffers.insert(succ.into(), nb);
        }
    }
    let spill_allowed = node_buffer_spill_allowed(current_dag, node_idx);
    for port in pred_ports {
        let records = if is_removed_port(port.as_deref()) {
            removed.clone()
        } else {
            kept.clone()
        };
        let key = NodeBufferKey::with_port(node_idx, port.as_deref());
        let nb = admit_node_buffer(
            ctx,
            name,
            key.clone(),
            records,
            input_puncts.clone(),
            spill_allowed,
        )?;
        ctx.node_buffers.insert(key, nb);
    }
    Ok(())
}

/// Whether `node` drains its *predecessor's* `node_buffers` slot rather than
/// checking its own slot first. Merge and Combine read each predecessor slot
/// directly; every other consumer (Transform / Output / Reshape / Cull / Sort
/// / Aggregation) checks its own slot first, so a producer writes into the
/// consumer's slot for those.
pub(crate) fn reads_predecessor_slot(node: &PlanNode) -> bool {
    matches!(node, PlanNode::Merge { .. } | PlanNode::Combine { .. })
}

// ---------------------------------------------------------------------
// Per-group input buffer with arbitrator-driven spill.
//
// Mirrors the Reshape group buffer: holds every drained input record
// grouped by `partition_by` in first-seen order, spilling whole resident
// groups (or slices of one oversized group) to disk under memory pressure,
// and reloading each group in arrival order at finalize.
// ---------------------------------------------------------------------

/// One buffered input record plus the two `u64`s the spill round-trip must
/// preserve: a Cull-local admission sequence that orders the record within
/// its group across a multi-source merge, and the upstream source row number
/// carried through for output.
struct BufferedRecord {
    record: Record,
    /// Cull-local monotonic admission sequence; the within-group sort key.
    seq: u64,
    /// Upstream source row number, carried through to the output ports.
    row_num: u64,
}

/// Spill payload for a buffered Cull input record: the admission sequence and
/// the source row number, both reconstructed on reload.
type CullSpillPayload = (u64, u64);

/// One group's buffered input records: a resident tail plus any slices
/// already evicted to disk.
struct CullGroupState {
    resident: Vec<BufferedRecord>,
    resident_bytes: usize,
    spilled_bytes: usize,
    spilled: Vec<SpillFile<CullSpillPayload>>,
}

impl CullGroupState {
    fn new() -> Self {
        Self {
            resident: Vec::new(),
            resident_bytes: 0,
            spilled_bytes: 0,
            spilled: Vec::new(),
        }
    }
}

/// Per-group input buffer with arbitrator-driven spill.
///
/// Holds every drained input record grouped by `partition_by` in first-seen
/// order. Under memory pressure it evicts whole resident groups to disk
/// largest-first; a single group too large to fit on its own is sliced by
/// the upper bits of each record's admission sequence so successive waves
/// spill fractions of the one group. At finalize each group is streamed back
/// — resident records plus reloaded spill slices in arrival order — and split
/// onto the output ports.
struct CullGroupBuffer {
    input_schema: Arc<Schema>,
    compress: bool,
    group_order: Vec<Vec<GroupByKey>>,
    groups: HashMap<Vec<GroupByKey>, CullGroupState>,
    resident_bytes: usize,
    next_seq: u64,
}

impl CullGroupBuffer {
    fn new(input_schema: Arc<Schema>, compress: bool) -> Self {
        Self {
            input_schema,
            compress,
            group_order: Vec::new(),
            groups: HashMap::new(),
            resident_bytes: 0,
            next_seq: 0,
        }
    }

    fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    /// Admit one record into its group, stamping a Cull-local admission
    /// sequence and recording first-seen group order.
    fn push(&mut self, key: Vec<GroupByKey>, record: Record, row_num: u64) {
        let bytes = estimated_input_bytes(&record);
        let seq = self.next_seq;
        self.next_seq += 1;
        let order = &mut self.group_order;
        let state = self.groups.entry(key.clone()).or_insert_with(|| {
            order.push(key);
            CullGroupState::new()
        });
        state.resident.push(BufferedRecord {
            record,
            seq,
            row_num,
        });
        state.resident_bytes += bytes;
        self.resident_bytes += bytes;
    }

    /// Take the first-seen group order, consuming it for the finalize drain.
    fn take_group_order(&mut self) -> Vec<Vec<GroupByKey>> {
        std::mem::take(&mut self.group_order)
    }

    /// Evict resident groups to disk, largest-first, until the resident
    /// footprint drops back under the soft threshold. The stop condition keys
    /// on the live, decreasing `resident_bytes` rather than the arbitrator's
    /// monotonic `peak_rss` high-water, so under a budget below baseline RSS
    /// it stops at the threshold instead of draining every group.
    fn spill_until_under_budget(
        &mut self,
        node_name: &str,
        budget: &MemoryArbitrator,
        spill_root: &std::path::Path,
        handle: &Arc<ConsumerHandle>,
    ) -> Result<(), PipelineError> {
        let soft = budget.spill_threshold_bytes() as usize;
        while self.resident_bytes > soft {
            let Some(key) = self.largest_resident_group() else {
                break;
            };
            let resident_bytes = self.groups[&key].resident_bytes;
            if resident_bytes > soft && soft > 0 {
                self.spill_group_partitioned(node_name, budget, spill_root, &key, soft)?;
            } else {
                self.spill_group_whole(node_name, budget, spill_root, &key)?;
            }
            handle.set_bytes(self.resident_bytes as u64);
        }
        Ok(())
    }

    /// Key of the resident group holding the most in-memory bytes, or `None`
    /// when every group is fully spilled.
    fn largest_resident_group(&self) -> Option<Vec<GroupByKey>> {
        self.groups
            .iter()
            .filter(|(_, s)| !s.resident.is_empty())
            .max_by_key(|(_, s)| s.resident_bytes)
            .map(|(k, _)| k.clone())
    }

    /// Evict one group's entire resident tail to a fresh spill file.
    fn spill_group_whole(
        &mut self,
        node_name: &str,
        budget: &MemoryArbitrator,
        spill_root: &std::path::Path,
        key: &[GroupByKey],
    ) -> Result<(), PipelineError> {
        let state = self
            .groups
            .get_mut(key)
            .expect("spill target group present");
        let records = std::mem::take(&mut state.resident);
        let freed = std::mem::take(&mut state.resident_bytes);
        self.resident_bytes -= freed;
        let file = write_spill_slice(
            node_name,
            budget,
            spill_root,
            &self.input_schema,
            self.compress,
            records.iter(),
        )?;
        let state = self
            .groups
            .get_mut(key)
            .expect("spill target group present");
        state.spilled.push(file);
        state.spilled_bytes += freed;
        Ok(())
    }

    /// Slice one oversized group's resident tail across hash partitions,
    /// spilling enough partitions to bring the group's resident footprint
    /// under `soft`. The partition assignment uses the upper bits of each
    /// record's admission sequence, so a single giant group is evicted in
    /// fractions across successive waves and a duplicate-content group still
    /// distributes evenly.
    fn spill_group_partitioned(
        &mut self,
        node_name: &str,
        budget: &MemoryArbitrator,
        spill_root: &std::path::Path,
        key: &[GroupByKey],
        soft: usize,
    ) -> Result<(), PipelineError> {
        let state = self
            .groups
            .get_mut(key)
            .expect("partition-spill target group present");
        let resident = std::mem::take(&mut state.resident);
        let total_bytes = std::mem::take(&mut state.resident_bytes);
        self.resident_bytes -= total_bytes;

        let parts_wanted = total_bytes.div_ceil(soft.max(1)).max(2);
        let bits =
            (usize::BITS - (parts_wanted - 1).leading_zeros()).clamp(1, MAX_SKEW_PARTITION_BITS);
        let shift = 64 - bits;

        let n = 1usize << bits;
        let mut buckets: Vec<Vec<BufferedRecord>> = (0..n).map(|_| Vec::new()).collect();
        let mut bucket_bytes: Vec<usize> = vec![0; n];
        for buffered in resident {
            // Partition on the admission sequence via the SplitMix64 finalizer:
            // the sequence is dense, so it varies only in its low bits, but the
            // slicer reads the upper bits — the finalizer spreads that low-bit
            // variation across the whole word so buckets fill evenly.
            let p = ((crate::sketch::splitmix64(buffered.seq) >> shift) as usize) & (n - 1);
            bucket_bytes[p] += estimated_input_bytes(&buffered.record);
            buckets[p].push(buffered);
        }

        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by_key(|&p| std::cmp::Reverse(bucket_bytes[p]));
        let mut remaining: usize = bucket_bytes.iter().sum();
        let mut spilled_files: Vec<SpillFile<CullSpillPayload>> = Vec::new();
        for &p in &order {
            if remaining <= soft || buckets[p].is_empty() {
                continue;
            }
            let slice = std::mem::take(&mut buckets[p]);
            remaining -= bucket_bytes[p];
            let file = write_spill_slice(
                node_name,
                budget,
                spill_root,
                &self.input_schema,
                self.compress,
                slice.iter(),
            )?;
            spilled_files.push(file);
        }

        let tail: Vec<BufferedRecord> = buckets.into_iter().flatten().collect();
        let spilled_now = total_bytes - remaining;
        let state = self
            .groups
            .get_mut(key)
            .expect("partition-spill target group present");
        state.resident = tail;
        state.resident_bytes = remaining;
        state.spilled_bytes += spilled_now;
        state.spilled.extend(spilled_files);
        self.resident_bytes += remaining;
        Ok(())
    }

    /// Reload one group's full record set in original arrival order.
    ///
    /// A group whose reloaded footprint (`resident_bytes + spilled_bytes`)
    /// exceeds `hard_limit` cannot be observed within budget — the
    /// `drop_group_when` decision needs the whole group — so finalize fails
    /// loud rather than OOM on reload. A spilled group's records arrive in
    /// spill-file then bucket order, so the merged group is re-sorted by the
    /// Cull-local admission sequence (globally unique and monotonic in
    /// arrival order, surviving a multi-source merge where source row numbers
    /// collide) so it emits identically to a resident group.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::Internal`] when a single correlation group's
    /// reloaded footprint exceeds `hard_limit`, or when a spill file cannot be
    /// read back.
    fn take_group(
        &mut self,
        node_name: &str,
        key: &[GroupByKey],
        hard_limit: u64,
    ) -> Result<Vec<(Record, u64)>, PipelineError> {
        let state = self.groups.remove(key).expect("group key present in order");
        self.resident_bytes -= state.resident_bytes;

        let group_bytes = (state.resident_bytes + state.spilled_bytes) as u64;
        if hard_limit > 0 && group_bytes > hard_limit {
            return Err(cull_giant_group_error(node_name, group_bytes, hard_limit));
        }

        if state.spilled.is_empty() {
            return Ok(state
                .resident
                .into_iter()
                .map(|b| (b.record, b.row_num))
                .collect());
        }

        let mut group: Vec<BufferedRecord> = Vec::new();
        for file in &state.spilled {
            let reader = file.reader().map_err(|e| cull_spill_error(node_name, e))?;
            for pair in reader {
                let (record, (seq, row_num)) = pair.map_err(|e| cull_spill_error(node_name, e))?;
                group.push(BufferedRecord {
                    record,
                    seq,
                    row_num,
                });
            }
        }
        group.extend(state.resident);
        group.sort_by_key(|b| b.seq);
        Ok(group.into_iter().map(|b| (b.record, b.row_num)).collect())
    }
}

/// Write one input-record slice to a fresh spill file, charging its on-disk
/// byte size against the arbitrator's per-stage and pipeline-wide spill
/// totals so `--explain` calibration and the disk-spill cap both see it.
fn write_spill_slice<'a>(
    node_name: &str,
    budget: &MemoryArbitrator,
    spill_root: &std::path::Path,
    input_schema: &Arc<Schema>,
    compress: bool,
    records: impl Iterator<Item = &'a BufferedRecord>,
) -> Result<SpillFile<CullSpillPayload>, PipelineError> {
    let mut writer: SpillWriter<CullSpillPayload> =
        SpillWriter::new(Arc::clone(input_schema), Some(spill_root), compress)
            .map_err(|e| cull_spill_error(node_name, e))?;
    for buffered in records {
        writer
            .write_pair(&buffered.record, &(buffered.seq, buffered.row_num))
            .map_err(|e| cull_spill_error(node_name, e))?;
    }
    let file = writer
        .finish()
        .map_err(|e| cull_spill_error(node_name, e))?;
    let bytes = std::fs::metadata(file.path()).map(|m| m.len()).unwrap_or(0);
    if budget.record_spill_bytes(node_name, bytes) {
        return Err(PipelineError::spill_cap_exceeded(
            node_name,
            budget.max_spill_bytes(),
            bytes,
            budget.cumulative_spill_bytes(),
        ));
    }
    Ok(file)
}

/// One input record's estimated buffered footprint: heap bytes plus the
/// owning [`BufferedRecord`] header overhead.
fn estimated_input_bytes(record: &Record) -> usize {
    record.estimated_heap_size() + std::mem::size_of::<BufferedRecord>()
}

/// Extract the `partition_by` key tuple from a record, using the **exact**
/// construction the decision aggregate's internal grouping uses
/// ([`value_to_group_key`] with `Ok(None) → GroupByKey::Null`).
///
/// Keying the dispatch buffer and the decision aggregate by the same function
/// is load-bearing: any divergence (for example collapsing an empty string to
/// `Null` here while the aggregate keeps it as a distinct `Str("")` group)
/// would merge two groups in the buffer that the aggregate split — or vice
/// versa — and the merged group would then be routed by whichever decision the
/// `HashMap` happened to insert last. So this does not special-case empty
/// strings: `account=""` and `account=null` are distinct groups in both the
/// buffer and the aggregate, exactly as every other aggregate / correlation
/// grouping in the engine treats them.
///
/// # Errors
///
/// Returns [`PipelineError::Internal`] when a `partition_by` value cannot form
/// a group key (a NaN, array, or map cell) — the same hard failure the
/// aggregate raises, so the two keyings stay in lockstep rather than the buffer
/// silently degrading a cell the aggregate rejects.
fn partition_key(
    node_name: &str,
    record: &Record,
    partition_by: &[String],
) -> Result<Vec<GroupByKey>, PipelineError> {
    partition_by
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            let v = record.get(f).unwrap_or(&Value::Null);
            match clinker_record::value_to_group_key(v, f, idx as u64) {
                Ok(Some(gk)) => Ok(gk),
                Ok(None) => Ok(GroupByKey::Null),
                Err(e) => Err(PipelineError::Internal {
                    op: "cull",
                    node: node_name.to_string(),
                    detail: format!("partition_by field {f:?} cannot form a group key: {e}"),
                }),
            }
        })
        .collect()
}

/// Sort a group in place by `order_by`, stable across equal keys (so arrival
/// order breaks ties deterministically). Nulls sort last regardless of
/// direction (SQL convention).
fn sort_group(group: &mut [(Record, u64)], order_by: &[SortField]) {
    group.sort_by(|(a, _), (b, _)| {
        for sf in order_by {
            let av = a.get(&sf.field).unwrap_or(&Value::Null);
            let bv = b.get(&sf.field).unwrap_or(&Value::Null);
            let ord = match (av, bv) {
                (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
                (Value::Null, _) => std::cmp::Ordering::Greater,
                (_, Value::Null) => std::cmp::Ordering::Less,
                _ => av.partial_cmp(bv).unwrap_or(std::cmp::Ordering::Equal),
            };
            let ord = match sf.order {
                SortOrder::Asc => ord,
                SortOrder::Desc => ord.reverse(),
            };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// Wrap a spill read/write fault from the Cull group buffer as a hard
/// pipeline error. Spill faults are I/O failures, not data errors, so they
/// abort the run rather than route anywhere.
fn cull_spill_error(node_name: &str, e: clinker_plan::SpillError) -> PipelineError {
    PipelineError::Internal {
        op: "cull",
        node: node_name.to_string(),
        detail: format!("group-buffer spill failed: {e}"),
    }
}

/// Wrap a predicate-aggregate compile or eval fault as a hard pipeline error.
/// `bind_cull` already typechecked each rule fragment, so a failure here is a
/// setup-invariant violation that aborts the run.
fn cull_predicate_error(node_name: &str, e: crate::aggregation::HashAggError) -> PipelineError {
    PipelineError::Internal {
        op: "cull",
        node: node_name.to_string(),
        detail: format!("drop_group_when predicate evaluation failed: {e}"),
    }
}

/// Hard error for a single correlation group that exceeds the finalize memory
/// budget. The `drop_group_when` decision needs the whole group observable at
/// once, so a group bigger than the budget has no in-budget representation —
/// fail loud rather than OOM on reload.
fn cull_giant_group_error(node_name: &str, group_bytes: u64, hard_limit: u64) -> PipelineError {
    PipelineError::Internal {
        op: "cull",
        node: node_name.to_string(),
        detail: format!(
            "a single Cull correlation group is ~{group_bytes} bytes, which exceeds the memory \
             budget of {hard_limit} bytes. Cull evaluates its group-level removal predicate \
             against the whole group at once, so a single group must fit the budget even though \
             cross-group and ingest-time peaks spill to disk. Raise `memory.limit`, or partition \
             the input so no one correlation group is this large."
        ),
    }
}

/// Hard error (E310) for the Cull drop-decision aggregate outgrowing the
/// memory budget. The decision aggregate and the per-group decision map it
/// feeds are both O(distinct groups) and run in-memory: the raw-record buffer
/// carries the spill burden, this state does not, and Cull cannot
/// back-pressure. So a group cardinality whose decision state alone exceeds the
/// budget has no in-budget representation — fail loud rather than grow it
/// uncounted toward an out-of-memory crash. `used` is the offending live or
/// estimated footprint; `hard_limit` is the configured ceiling.
fn cull_decision_budget_error(node_name: &str, used: u64, hard_limit: u64) -> PipelineError {
    PipelineError::MemoryBudgetExceeded {
        node: node_name.to_string(),
        used,
        limit: hard_limit,
        source: BudgetCategory::Arena,
        detail: Some("Cull drop-decision aggregate state".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::memory::NoOpPolicy;
    use clinker_record::Schema;

    fn record(schema: &Arc<Schema>, account: Value) -> Record {
        Record::new(Arc::clone(schema), vec![account])
    }

    /// An arbitrator whose soft limit is `soft_bytes` and whose seeded peak
    /// RSS is well below it, so spilling is driven purely by the buffer's own
    /// resident-byte total rather than the live process RSS.
    fn arbitrator(soft_bytes: u64) -> MemoryArbitrator {
        // soft = limit * 0.80, so limit = soft / 0.80.
        let limit = (soft_bytes as f64 / 0.80) as u64;
        let arb = MemoryArbitrator::with_policy(limit, 0.80, 0.70, Box::new(NoOpPolicy));
        arb.set_peak_rss_for_test(1);
        arb
    }

    // The decision aggregate groups by `value_to_group_key`, which keeps an
    // empty string as a distinct `Str("")` key and maps only a true null to
    // `Null`. The dispatch buffer MUST key identically, or an empty-string row
    // and a null row would merge in one but split in the other, and the merged
    // group would be routed by whichever decision the HashMap inserted last.
    #[test]
    fn partition_key_keeps_empty_string_distinct_from_null() {
        let schema: Arc<Schema> = Arc::new(Schema::new(vec!["account".into()]));
        let part = vec!["account".to_string()];

        let empty = partition_key("c", &record(&schema, Value::String("".into())), &part).unwrap();
        let null = partition_key("c", &record(&schema, Value::Null), &part).unwrap();
        let nonempty =
            partition_key("c", &record(&schema, Value::String("X".into())), &part).unwrap();

        // Empty-string and null are DISTINCT keys (the divergence bug collapsed
        // both to Null), and neither equals a non-empty string key.
        assert_eq!(empty, vec![GroupByKey::Str("".into())]);
        assert_eq!(null, vec![GroupByKey::Null]);
        assert_ne!(empty, null);
        assert_ne!(empty, nonempty);

        // And the keys match the canonical aggregate construction exactly, so
        // the buffer and the decision aggregate can never diverge.
        let canon = |v: &Value| {
            clinker_record::value_to_group_key(v, "account", 0)
                .unwrap()
                .map(|gk| vec![gk])
                .unwrap_or_else(|| vec![GroupByKey::Null])
        };
        assert_eq!(empty, canon(&Value::String("".into())));
        assert_eq!(null, canon(&Value::Null));
    }

    // The disk-spill cap must abort the spill instead of writing past
    // `storage.spill.disk_cap_bytes`. The bug discarded the overflow signal
    // `record_spill_bytes` returns, so eviction kept writing unbounded.
    #[test]
    fn spill_past_disk_cap_fails_with_spill_cap_exceeded() {
        let schema: Arc<Schema> = Arc::new(Schema::new(vec!["account".into()]));
        let spill_root = tempfile::tempdir().unwrap();
        let arb = arbitrator(512);
        arb.set_max_spill_bytes(1);
        let handle = ConsumerHandle::new();
        let mut buffer = CullGroupBuffer::new(Arc::clone(&schema), true);
        // One group, ~5 KiB resident against a 512 B soft limit, so the
        // spill loop must evict — and the first flush already exceeds the
        // one-byte disk cap.
        for row_num in 0..64u64 {
            let payload = format!("{row_num:063}");
            buffer.push(
                vec![GroupByKey::Str("g".into())],
                record(&schema, Value::String(payload.into())),
                row_num,
            );
        }
        handle.set_bytes(buffer.resident_bytes() as u64);
        let err = buffer
            .spill_until_under_budget("cl", &arb, spill_root.path(), &handle)
            .expect_err("a one-byte disk cap must abort the first spill write");
        match &err {
            PipelineError::SpillCapExceeded {
                node,
                cap,
                attempted,
                current,
            } => {
                assert_eq!(node, "cl");
                assert_eq!(*cap, 1, "reported cap must equal the configured quota");
                assert!(*attempted > 0, "the overflowing flush must report its size");
                assert!(
                    *current > *cap,
                    "cumulative spilled ({current}) must exceed the cap ({cap})"
                );
            }
            other => panic!("disk-cap overflow must surface SpillCapExceeded; got {other:?}"),
        }
        assert!(
            arb.cumulative_spill_bytes() > 1,
            "cumulative_spill_bytes must reflect the overflowing write"
        );
    }
}
