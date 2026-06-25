//! `PlanNode::Reshape` dispatch arm.
//!
//! Per-correlation-group synthesis and trigger-row mutation. Blocking
//! grouping operator: it drains the predecessor's full output, groups
//! records by `partition_by`, optionally orders each group by `order_by`,
//! and per rule mutates the rows whose `when` predicate fires while
//! synthesizing new rows derived from those trigger rows. The dispatcher's
//! `Reshape` arm is a single delegating call into [`dispatch_reshape`].
//!
//! # Memory model
//!
//! Blocking and grouped: the whole input materializes into per-group
//! buffers before any output row leaves, because group observation must
//! see the complete group (the no-cascade contract forbids incremental
//! folding). The buffer is governed by the central
//! [`MemoryArbitrator`](crate::pipeline::memory::MemoryArbitrator): a
//! registered [`ReshapeConsumer`] reports the live group-buffer bytes, and
//! when the soft threshold trips the arm spills whole *input* records — not
//! post-processed output rows — through a [`SpillWriter`], then re-runs
//! mutation and synthesis on the reloaded group at finalize.
//!
//! Spilling input records (rather than output rows) is load-bearing for two
//! reasons. First, it preserves the no-cascade snapshot: synthesis re-runs
//! against the reloaded group exactly as it would in memory. Second, it
//! sidesteps a schema-width hazard — a `copy_from: none` synthesized row is
//! built against the *wider* output schema, while input records carry the
//! narrower input schema; a single spill file reconstructs every row against
//! one stored schema, so round-tripping a synthesized row would corrupt it.
//! Input records share one uniform schema per buffer slot, so the round-trip
//! is exact.
//!
//! A single oversized group is spilled incrementally — partitioned by the
//! upper bits of each record's admission sequence so successive spill waves
//! evict slices of the one group rather than buffering it whole — while
//! smaller groups stay resident under the byte budget. The consumer registers
//! at priority `15` (between grace-hash and external sort) and cannot
//! back-pressure: there is no upstream channel to gate once the predecessor
//! has drained.
//!
//! # No-cascade contract
//!
//! Every rule observes the **original** group snapshot. A row mutated by
//! rule A is not re-observed by rule B; conflicting writes are detected and
//! the whole group is rolled back rather than silently order-dependent.

use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::{GroupByKey, Record, Schema, Value};
use cxl::eval::{EvalContext, EvalResult, ProgramEvaluator};
use petgraph::graph::NodeIndex;

use crate::executor::DlqEntry;
use crate::executor::dispatch::{
    ExecutorContext, admit_node_buffer, drain_node_buffer_slot, node_buffer_spill_allowed,
    push_dlq, source_file_arc_of, source_name_arc_of, tee_emit_to_region_input_buffers,
};
use crate::pipeline::memory::{
    ConsumerHandle, ConsumerSpillError, MemoryArbitrator, MemoryConsumer,
};
use crate::pipeline::spill::{SpillFile, SpillWriter};
use clinker_core_types::dlq::{DlqErrorCategory, stage_reshape_mutation_conflict};
use clinker_plan::config::pipeline_node::{
    CopyFrom, RESHAPE_MUTATED_BY_COLUMN, RESHAPE_SYNTHESIZED_BY_COLUMN, RESHAPE_SYNTHETIC_COLUMN,
    ReshapeBody,
};
use clinker_plan::config::{SortField, SortOrder};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{
    CompiledReshapeRule, ExecutionPlanDag, PlanNode, single_predecessor,
};

use crate::executor::NullStorage;

/// A compiled rule: the trigger predicate plus the per-field mutation and
/// synthesis assignment evaluators, all built against the live input
/// schema at dispatch time (the same condition-compile seam Route uses).
struct CompiledRule {
    name: String,
    /// `filter <when>` — `Emit` iff the row is a trigger row.
    when: ProgramEvaluator,
    /// `mutate.set` field → `emit <field> = <expr>` evaluator.
    set: Vec<(String, ProgramEvaluator)>,
    /// Present iff the rule synthesizes rows.
    synth: Option<CompiledSynth>,
}

/// Compiled synthesis action.
struct CompiledSynth {
    copy_from: CopyFrom,
    /// `overrides` field → `emit <field> = <expr>` evaluator.
    overrides: Vec<(String, ProgramEvaluator)>,
}

/// Spill priority for the Reshape group buffer: between grace-hash (`10`)
/// and external sort (`20`). A grouped record buffer is costlier to evict
/// than grace partitions (reload re-runs synthesis CPU) but cheaper than an
/// external-sort merge. The plan-time mirror in `arbitration_class` returns
/// the same constant.
const RESHAPE_SPILL_PRIORITY: i32 = 15;

/// Maximum partition fan-out used to slice one oversized group across
/// successive spill waves. 12 bits (4096 partitions) matches the grace-hash
/// cap: beyond it, per-file overhead outweighs further skew reduction.
const MAX_SKEW_PARTITION_BITS: u32 = 12;

/// Arbitrator-facing wrapper over the Reshape group buffer.
///
/// Blocking operator: reports the live group-buffer byte footprint through a
/// shared [`ConsumerHandle`] and cannot back-pressure (no upstream channel
/// remains once the predecessor has drained). On `try_spill` it flips the
/// handle's spill-request flag; the dispatch loop reads it at the next
/// grouping boundary and evicts resident groups to disk in-thread, mirroring
/// the aggregate spill-request handshake.
struct ReshapeConsumer {
    handle: Arc<ConsumerHandle>,
}

impl ReshapeConsumer {
    fn new(handle: Arc<ConsumerHandle>) -> Self {
        Self { handle }
    }
}

impl MemoryConsumer for ReshapeConsumer {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }

    fn spill_priority(&self) -> i32 {
        RESHAPE_SPILL_PRIORITY
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

/// A detected runtime mutation conflict: two rules wrote the same field on
/// the same group row. Captured as owned data so the DLQ push (which needs
/// `&mut ExecutorContext`) runs after the eval context's immutable borrow
/// is released.
struct MutationConflict {
    rule_a: String,
    rule_b: String,
    field: String,
    record: Record,
    row_num: u64,
}

/// Execute the `Reshape` arm for `node_idx`. Drains the predecessor,
/// groups by `partition_by`, applies each rule's mutation and synthesis
/// per group, routes mutation conflicts to the DLQ (rolling back the whole
/// group), and emits originals (mutated, audit-stamped) followed by
/// synthesized rows. Blocking: the full input materializes before the
/// first output row leaves.
pub(crate) fn dispatch_reshape(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError> {
    let PlanNode::Reshape {
        ref name,
        ref config,
        ref output_schema,
        ref compiled_rules,
        ..
    } = *node
    else {
        unreachable!("dispatch_reshape called with non-Reshape node");
    };

    let pred = single_predecessor(current_dag, node_idx, "reshape", name)?;
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
        tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &[])?;
        let nb = admit_node_buffer(
            ctx,
            name,
            node_idx,
            Vec::new(),
            input_puncts,
            node_buffer_spill_allowed(current_dag, node_idx),
        )?;
        ctx.node_buffers.insert(node_idx, nb);
        return Ok(());
    }

    // Register the group buffer with the arbitrator only after the
    // empty-input guard, so the no-work path never leaks a consumer. The
    // handle's byte counter tracks live resident-group bytes; the wrapper
    // reads it on every arbitration round.
    let handle = ConsumerHandle::new();
    let consumer_id = ctx
        .memory_budget
        .register_consumer(Arc::new(ReshapeConsumer::new(handle.clone())));

    // Every exit path past this point must deregister `consumer_id`, so the
    // grouping/finalize work runs inside a closure whose result is matched
    // below — success, conflict-DLQ, and hard error all funnel through the
    // single `unregister_consumer` call.
    let result = run_reshape_grouped(
        ctx,
        current_dag,
        node_idx,
        name,
        config,
        output_schema,
        compiled_rules,
        input,
        input_puncts,
        &handle,
    );
    ctx.memory_budget.unregister_consumer(consumer_id);
    result
}

/// Group the drained input, spilling resident groups to disk under memory
/// pressure, then finalize each group (reloading spilled groups) through
/// [`process_group`] and admit the output buffer.
///
/// Split out of [`dispatch_reshape`] so the consumer deregistration there
/// covers every exit — success, mutation-conflict DLQ, and hard error all
/// return through this function's `Result`.
#[allow(clippy::too_many_arguments)]
fn run_reshape_grouped(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    name: &str,
    config: &ReshapeBody,
    output_schema: &Arc<Schema>,
    compiled_rules: &[CompiledReshapeRule],
    input: Vec<(Record, u64)>,
    input_puncts: Vec<crate::executor::stream_event::Punctuation>,
    handle: &Arc<ConsumerHandle>,
) -> Result<(), PipelineError> {
    // Rebuild the per-dispatch evaluators from the node's compiled rule
    // programs (typechecked once at lowering). The `ProgramEvaluator` is
    // non-`Clone` and holds per-thread state, so it is reconstructed here
    // rather than carried on the node — the same seam Route/Aggregation use.
    let mut rules = build_rules(compiled_rules);
    // The schema is uniform across a node_buffer slot, so the first record's
    // schema drives the spill schema for the raw-record group buffer.
    let input_schema = input[0].0.schema().clone();

    // The spill file stores INPUT records, so the spill schema is the input
    // schema. The compression FLAG, however, is resolved against the
    // output-schema width so the on-disk format matches what `--explain`
    // projects for this stage — `spill_decision_column_count` reads the
    // stored (widened) output schema, and the two must agree. The flag is
    // independent of the schema header, so an output-width-resolved flag over
    // an input-schema file is consistent.
    let spill_compress = ctx
        .spill_compress
        .resolve_for_schema(output_schema.column_count(), ctx.batch_size as u64);

    // Accumulate records into per-group buffers, spilling resident groups (or
    // slices of one oversized group) to disk whenever the arbitrator reports
    // the soft threshold is crossed. `handle` mirrors the live resident-byte
    // footprint for the arbitration round.
    // The spill path needs only the arbitrator and the spill root, not the
    // whole executor context — clone the cheap handles up front so the per-
    // record loop borrows neither `ctx` mutably nor immutably across the call.
    let budget = Arc::clone(&ctx.memory_budget);
    let spill_root = Arc::clone(&ctx.spill_root_path);

    let mut buffer = ReshapeGroupBuffer::new(Arc::clone(&input_schema), spill_compress);
    for (record, row_num) in input {
        let key = partition_key(&record, &config.partition_by);
        buffer.push(key, record, row_num);
        handle.set_bytes(buffer.resident_bytes() as u64);
        // Poll for self-spill pressure at every admission. `should_spill_self`
        // updates the peak and reports the soft-threshold crossing WITHOUT
        // running the pausing arbitration round: Reshape relieves pressure by
        // spilling its OWN buffer in-thread, and that round can elect and
        // `pause()` a back-pressureable Source in a concurrent DAG branch that
        // Reshape never resumes — the deadlock the hash aggregator avoids the
        // same way. `take_spill_request` still honors a spill nudge another
        // stage's arbitration round set on this consumer.
        if budget.should_spill_self() || handle.take_spill_request() {
            buffer.spill_until_under_budget(name, &budget, &spill_root, handle)?;
            handle.set_bytes(buffer.resident_bytes() as u64);
        }
    }

    // Finalize: stream each group back in first-seen order (from memory when
    // it never spilled, else reloaded from its spill files and restored to
    // arrival order), re-sort by `order_by`, and run the rule kernel
    // unchanged. Releasing each group's resident bytes as it finalizes keeps
    // the buffer footprint shrinking through the drain. The hard limit gates
    // a single correlation group that is too large to observe whole.
    let hard_limit = budget.hard_limit();
    let mut out: Vec<(Record, u64)> = Vec::new();
    let group_order = buffer.take_group_order();
    for key in group_order {
        let mut group = buffer.take_group(name, &key, hard_limit)?;
        handle.set_bytes(buffer.resident_bytes() as u64);
        if !config.order_by.is_empty() {
            sort_group(&mut group, &config.order_by);
        }
        process_group(ctx, name, &mut rules, output_schema, group, &mut out)?;
    }

    tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &out)?;
    let nb = admit_node_buffer(
        ctx,
        name,
        node_idx,
        out,
        input_puncts,
        node_buffer_spill_allowed(current_dag, node_idx),
    )?;
    ctx.node_buffers.insert(node_idx, nb);
    Ok(())
}

/// One buffered input record plus the two `u64`s the spill round-trip must
/// preserve: a Reshape-local admission sequence that orders the record within
/// its group, and the upstream source row number the rule kernel needs for
/// source attribution and DLQ row identity.
///
/// `seq` — not the source row number — is the ordering key. The source row
/// number is unique only per source: a `Merge` of two sources forwards each
/// source's row number without renumbering, so two records from different
/// sources can share a row number and land in the same group. Sorting a
/// reloaded group by the source row number would then reorder those records
/// relative to a resident group. The Reshape-local sequence is assigned once
/// per admitted record from a single monotonic counter, so it is globally
/// unique across all sources and captures the true merged arrival order.
struct BufferedRecord {
    record: Record,
    /// Reshape-local monotonic admission sequence; the within-group sort key.
    seq: u64,
    /// Upstream source row number, carried through for the rule kernel.
    row_num: u64,
}

/// Spill payload for a buffered Reshape input record: the admission sequence
/// and the source row number, both reconstructed on reload. `SpillWriter` is
/// generic over the payload type, so this changes only what Reshape stores
/// per record — not the shared spill envelope or `RecordPayload`.
type ReshapeSpillPayload = (u64, u64);

/// One group's buffered input records: a resident tail plus any slices
/// already evicted to disk.
///
/// Spill writes the raw input records (one uniform input schema), so reload
/// reconstructs them exactly and synthesis re-runs in memory at finalize.
/// A group may hold both spilled slices and a resident tail at once — under
/// skew, one oversized group is sliced across successive spill waves while
/// its newest arrivals stay resident until the next trip.
struct ReshapeGroupState {
    /// Records still in memory for this group, in arrival order.
    resident: Vec<BufferedRecord>,
    /// Estimated heap bytes of `resident`, maintained incrementally so the
    /// buffer's running total never rescans the group.
    resident_bytes: usize,
    /// Estimated in-memory heap bytes of the records already spilled to disk,
    /// summed at spill time. Drives the finalize-budget guard: a group whose
    /// reloaded footprint (`resident_bytes + spilled_bytes`) exceeds the hard
    /// limit cannot be observed whole, so finalize fails loud rather than
    /// OOMing on reload.
    spilled_bytes: usize,
    /// Input-record slices already written to disk, oldest first. Reloaded at
    /// finalize and merged with `resident` back into arrival order (by the
    /// admission sequence) so a spilled group emits identically to a resident
    /// one.
    spilled: Vec<SpillFile<ReshapeSpillPayload>>,
}

impl ReshapeGroupState {
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
/// spill slices of the one group rather than buffering it whole. At finalize
/// each group is streamed back — resident records plus reloaded spill slices in
/// arrival order — and the kernel runs unchanged.
struct ReshapeGroupBuffer {
    /// Input schema every spill file stores and reloads against.
    input_schema: Arc<Schema>,
    /// Whether spill files are LZ4-framed. Resolved once against the
    /// output-schema width so the on-disk format matches `--explain`.
    compress: bool,
    /// First-seen group order, the emission order at finalize.
    group_order: Vec<Vec<GroupByKey>>,
    /// Per-group buffered state.
    groups: HashMap<Vec<GroupByKey>, ReshapeGroupState>,
    /// Sum of every group's `resident_bytes`. Mirrored into the consumer
    /// handle so the arbitrator sees the live resident footprint.
    resident_bytes: usize,
    /// Next Reshape-local admission sequence. Incremented once per admitted
    /// record so the value stamped on each record is globally unique and
    /// monotonic in true (merged) arrival order across every source.
    next_seq: u64,
}

impl ReshapeGroupBuffer {
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

    /// Total resident (in-memory) input-record bytes across all groups.
    fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    /// Admit one record into its group, stamping a Reshape-local admission
    /// sequence and recording first-seen group order.
    fn push(&mut self, key: Vec<GroupByKey>, record: Record, row_num: u64) {
        let bytes = estimated_input_bytes(&record);
        let seq = self.next_seq;
        self.next_seq += 1;
        let order = &mut self.group_order;
        let state = self.groups.entry(key.clone()).or_insert_with(|| {
            order.push(key);
            ReshapeGroupState::new()
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
    /// footprint drops back under the soft threshold (or nothing resident
    /// remains to evict). Refreshes `handle` to the live resident total after
    /// each eviction so the loop stops as soon as the buffer is back under
    /// budget rather than draining every group.
    ///
    /// Largest-resident-group-first matches the grace-hash Largest-Size
    /// victim policy: freeing the biggest holder reclaims the most headroom
    /// per spill. A single group whose resident tail alone still exceeds the
    /// budget is sliced incrementally by admission sequence rather than
    /// buffered whole — the cross-group resident peak stays bounded even under
    /// one giant correlation group.
    ///
    /// The stop condition keys on `self.resident_bytes` rather than the
    /// arbitrator's `should_spill_self`: that poll reads the monotonic
    /// `peak_rss` high-water, which never falls once crossed, so under a
    /// budget below baseline RSS it would report pressure forever and evict
    /// every group. The resident-byte total is the live, decreasing quantity
    /// that actually reflects what spilling reclaims.
    fn spill_until_under_budget(
        &mut self,
        node_name: &str,
        budget: &MemoryArbitrator,
        spill_root: &std::path::Path,
        handle: &Arc<ConsumerHandle>,
    ) -> Result<(), PipelineError> {
        let soft = budget.spill_threshold_bytes() as usize;
        // A zero/absent soft limit means "spill everything on any trip" — the
        // caller only enters this loop when the arbitrator already reported
        // pressure, so fall back to draining all resident groups.
        while self.resident_bytes > soft {
            let Some(key) = self.largest_resident_group() else {
                break;
            };
            let resident_bytes = self.groups[&key].resident_bytes;
            if resident_bytes > soft && soft > 0 {
                // One group dwarfs the budget on its own: slice it across
                // partitions so each wave evicts a fraction, never the whole
                // group at once.
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
        // Carry the evicted records' in-memory footprint into the group's
        // on-disk total so the finalize-budget guard can size the reloaded
        // group without re-reading the file.
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

        // Fan-out sized so each partition is roughly one `soft`-sized slice:
        // ceil(total / soft) rounded up to a power of two, capped at 4096.
        let parts_wanted = total_bytes.div_ceil(soft.max(1)).max(2);
        let bits =
            (usize::BITS - (parts_wanted - 1).leading_zeros()).clamp(1, MAX_SKEW_PARTITION_BITS);
        let shift = 64 - bits;

        let n = 1usize << bits;
        let mut buckets: Vec<Vec<BufferedRecord>> = (0..n).map(|_| Vec::new()).collect();
        let mut bucket_bytes: Vec<usize> = vec![0; n];
        for buffered in resident {
            // Partition on the admission sequence via the SplitMix64 finalizer:
            // the sequence is dense (`0, 1, 2, …`) so it varies only in its low
            // bits, but the slicer reads the *upper* bits — the finalizer
            // spreads that low-bit variation across the whole word so buckets
            // fill evenly. The sequence is globally unique, so a group of
            // duplicate-content records still distributes instead of collapsing
            // into one bucket. Deterministic, so a record lands in the same
            // bucket on every wave.
            let p = ((crate::sketch::splitmix64(buffered.seq) >> shift) as usize) & (n - 1);
            bucket_bytes[p] += estimated_input_bytes(&buffered.record);
            buckets[p].push(buffered);
        }

        // Spill whole buckets, largest first, until the group's residual
        // resident footprint drops under `soft`. `remaining` tracks the
        // un-spilled byte total straight off `bucket_bytes`, so the tail's
        // footprint is known without re-walking its records below.
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by_key(|&p| std::cmp::Reverse(bucket_bytes[p]));
        let mut remaining: usize = bucket_bytes.iter().sum();
        let mut spilled_files: Vec<SpillFile<ReshapeSpillPayload>> = Vec::new();
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

        // Re-seat the un-spilled buckets as the group's resident tail (its
        // byte total is `remaining`) and record the freshly written slices.
        // The spilled fraction is `total_bytes - remaining`, accumulated into
        // the group's on-disk total for the finalize-budget guard.
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

    /// Reload one group's full record set in original arrival order, ready
    /// for the no-cascade rule kernel.
    ///
    /// No-cascade requires the WHOLE group resident at finalize (every rule
    /// observes the complete group), so a group whose reloaded footprint
    /// (`resident_bytes + spilled_bytes`) exceeds `hard_limit` cannot be
    /// observed within budget. The skew slicing bounds the *ingest* peak, but
    /// one correlation group bigger than the finalize budget has no in-budget
    /// representation — fail loud here rather than OOM on reload. A future
    /// two-pass finalize could lift this.
    ///
    /// A spilled group's records arrive in spill-file then bucket order, not
    /// arrival order, so the merged group is re-sorted by the Reshape-local
    /// admission sequence — a globally-unique, monotonic-in-arrival-order key
    /// that survives a multi-source `Merge` where source row numbers collide.
    /// A spilled group then emits identically to a resident one, which is
    /// essential because output must not depend on the memory budget. The
    /// never-spilled fast path keeps its already-ordered resident Vec
    /// untouched. The returned tuples carry the source row number the rule
    /// kernel needs; the admission sequence has done its job by reload.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::Internal`] when a single correlation group's
    /// reloaded footprint exceeds `hard_limit`, or when a spill file cannot
    /// be read back.
    fn take_group(
        &mut self,
        node_name: &str,
        key: &[GroupByKey],
        hard_limit: u64,
    ) -> Result<Vec<(Record, u64)>, PipelineError> {
        let state = self.groups.remove(key).expect("group key present in order");
        self.resident_bytes -= state.resident_bytes;

        // Bounded-memory pillar: a group larger than the finalize budget
        // cannot be observed whole, so refuse it rather than OOM on reload.
        let group_bytes = (state.resident_bytes + state.spilled_bytes) as u64;
        if hard_limit > 0 && group_bytes > hard_limit {
            return Err(reshape_giant_group_error(
                node_name,
                group_bytes,
                hard_limit,
            ));
        }

        if state.spilled.is_empty() {
            // Fast path: never spilled, resident Vec already in arrival order.
            return Ok(state
                .resident
                .into_iter()
                .map(|b| (b.record, b.row_num))
                .collect());
        }

        let mut group: Vec<BufferedRecord> = Vec::new();
        for file in &state.spilled {
            let reader = file
                .reader()
                .map_err(|e| reshape_spill_error(node_name, e))?;
            for pair in reader {
                let (record, (seq, row_num)) =
                    pair.map_err(|e| reshape_spill_error(node_name, e))?;
                group.push(BufferedRecord {
                    record,
                    seq,
                    row_num,
                });
            }
        }
        group.extend(state.resident);
        // Restore arrival order: spill round-trips reorder records by file and
        // bucket, so a spilled group must be re-sorted by its admission
        // sequence to emit identically to a resident group.
        group.sort_by_key(|b| b.seq);
        Ok(group.into_iter().map(|b| (b.record, b.row_num)).collect())
    }
}

/// Write one input-record slice to a fresh spill file, charging its on-disk
/// byte size against the arbitrator's per-stage and pipeline-wide spill
/// totals so `--explain` calibration and the disk-spill cap both see it.
///
/// Each record's payload carries its admission sequence (the reload sort key)
/// and its source row number (for the rule kernel) so both survive the
/// round-trip.
fn write_spill_slice<'a>(
    node_name: &str,
    budget: &MemoryArbitrator,
    spill_root: &std::path::Path,
    input_schema: &Arc<Schema>,
    compress: bool,
    records: impl Iterator<Item = &'a BufferedRecord>,
) -> Result<SpillFile<ReshapeSpillPayload>, PipelineError> {
    let mut writer: SpillWriter<ReshapeSpillPayload> =
        SpillWriter::new(Arc::clone(input_schema), Some(spill_root), compress)
            .map_err(|e| reshape_spill_error(node_name, e))?;
    for buffered in records {
        writer
            .write_pair(&buffered.record, &(buffered.seq, buffered.row_num))
            .map_err(|e| reshape_spill_error(node_name, e))?;
    }
    let file = writer
        .finish()
        .map_err(|e| reshape_spill_error(node_name, e))?;
    let bytes = std::fs::metadata(file.path()).map(|m| m.len()).unwrap_or(0);
    budget.record_spill_bytes(node_name, bytes);
    Ok(file)
}

/// One input record's estimated buffered footprint: heap bytes plus the
/// owning [`BufferedRecord`] header overhead.
fn estimated_input_bytes(record: &Record) -> usize {
    record.estimated_heap_size() + std::mem::size_of::<BufferedRecord>()
}

/// Wrap a spill read/write fault from the Reshape group buffer as a hard
/// pipeline error. Spill faults are I/O failures, not data errors, so they
/// abort the run rather than route to the DLQ.
fn reshape_spill_error(node_name: &str, e: clinker_plan::SpillError) -> PipelineError {
    PipelineError::Internal {
        op: "reshape",
        node: node_name.to_string(),
        detail: format!("group-buffer spill failed: {e}"),
    }
}

/// Hard error for a single correlation group that exceeds the finalize
/// memory budget. The no-cascade contract requires the whole group resident
/// to observe rules, so a group bigger than the budget has no in-budget
/// representation — fail loud rather than OOM on reload.
fn reshape_giant_group_error(node_name: &str, group_bytes: u64, hard_limit: u64) -> PipelineError {
    PipelineError::Internal {
        op: "reshape",
        node: node_name.to_string(),
        detail: format!(
            "a single Reshape correlation group is ~{group_bytes} bytes, which exceeds the \
             memory budget of {hard_limit} bytes. Reshape applies its rules against the whole \
             group at once (the no-cascade contract), so a single group must fit the budget even \
             though cross-group and ingest-time peaks spill to disk. Raise `memory.limit`, or \
             partition the input so no one correlation group is this large."
        ),
    }
}

/// Apply every rule to one group against its original snapshot, detect
/// cross-rule mutation conflicts, and append the group's output rows.
///
/// On a mutation conflict the whole group rolls back: a `MutationConflict`
/// DLQ entry is pushed for the colliding row and no output row from this
/// group reaches the buffer (matching the correlation-group atomicity
/// contract).
fn process_group(
    ctx: &mut ExecutorContext<'_>,
    node_name: &str,
    rules: &mut [CompiledRule],
    output_schema: &Arc<clinker_record::Schema>,
    group: Vec<(Record, u64)>,
    out: &mut Vec<(Record, u64)>,
) -> Result<(), PipelineError> {
    // Per original row: accumulated field writes (field → (value, rule
    // name)). A second write to an already-written field by a different
    // rule is a conflict; the stored rule name names the prior writer.
    let mut mutations: Vec<HashMap<String, (Value, String)>> =
        (0..group.len()).map(|_| HashMap::new()).collect();
    // Per original row: the rule labels that mutated it, for the
    // `$meta.mutated_by` audit stamp.
    let mut mutated_by: Vec<Vec<String>> = (0..group.len()).map(|_| Vec::new()).collect();
    // Synthesized rows accumulated across rules: `(record, source_row_num)`.
    let mut synthesized: Vec<(Record, u64)> = Vec::new();
    // First detected cross-rule mutation conflict, captured as owned data
    // so the whole group can roll back to the DLQ once the immutable
    // `ctx` borrow held by the eval context is released.
    let mut conflict: Option<MutationConflict> = None;

    'rules: for rule in rules.iter_mut() {
        for (row_idx, (record, row_num)) in group.iter().enumerate() {
            // Per-record evaluation context carries the record's own
            // source attribution and envelope. The Arcs live for the
            // record's loop iteration so the borrowing `EvalContext` is
            // valid across every rule-program eval below.
            let source_file = source_file_arc_of(record);
            let source_name = source_name_arc_of(record);
            let eval_ctx =
                ctx.eval_ctx_for_record(&source_file, &source_name, *row_num, record.doc_ctx());

            // `when` predicate: a trigger row emits.
            let triggered = matches!(
                rule.when
                    .eval_record::<NullStorage>(&eval_ctx, record, None)
                    .map_err(|e| reshape_eval_error(node_name, &rule.name, "when", e))?,
                EvalResult::Emit { .. } | EvalResult::EmitMany { .. }
            );
            if !triggered {
                continue;
            }

            // Mutation: evaluate each `set` expression and stage the write.
            let mut rule_mutated_this_row = false;
            for (field, evaluator) in rule.set.iter_mut() {
                let value = eval_scalar(evaluator, &eval_ctx, record, field)
                    .map_err(|e| reshape_eval_error(node_name, &rule.name, field, e))?;
                if let Some((_, prior_rule)) = mutations[row_idx].get(field) {
                    // Content-dependent collision two rules could not be
                    // proven disjoint at compile time. Capture the conflict
                    // and stop — the whole group rolls back to the DLQ.
                    conflict = Some(MutationConflict {
                        rule_a: prior_rule.clone(),
                        rule_b: rule.name.clone(),
                        field: field.clone(),
                        record: record.clone(),
                        row_num: *row_num,
                    });
                    break 'rules;
                }
                mutations[row_idx].insert(field.clone(), (value, rule.name.clone()));
                rule_mutated_this_row = true;
            }
            if rule_mutated_this_row {
                mutated_by[row_idx].push(rule_label(node_name, &rule.name));
            }

            // Synthesis: derive a new row from this trigger row.
            if let Some(synth) = rule.synth.as_mut() {
                let mut new_row = match synth.copy_from {
                    CopyFrom::Trigger => clone_into_schema(record, output_schema),
                    // Born sized to the full output schema (upstream cols +
                    // the three `$meta.*` audit cols) so each override lands
                    // in its schema-indexed slot. Binding guarantees a
                    // `copy_from: none` rule overrides every user column, so
                    // no `Null` survives below.
                    CopyFrom::None => Record::new(
                        Arc::clone(output_schema),
                        vec![Value::Null; output_schema.column_count()],
                    ),
                };
                for (field, evaluator) in synth.overrides.iter_mut() {
                    let value = eval_scalar(evaluator, &eval_ctx, record, field)
                        .map_err(|e| reshape_eval_error(node_name, &rule.name, field, e))?;
                    new_row.set(field, value);
                }
                stamp_audit(&mut new_row, true, &rule_label(node_name, &rule.name), &[]);
                synthesized.push((new_row, *row_num));
            }
        }
    }

    // Conflict: the whole correlation group rolls back. No output row from
    // this group reaches the buffer.
    if let Some(c) = conflict {
        return dlq_group_conflict(ctx, node_name, &c);
    }

    // No conflict: emit originals (with staged mutations + audit stamps)
    // followed by synthesized rows, preserving within-group order.
    for (row_idx, (record, row_num)) in group.into_iter().enumerate() {
        let mut row = clone_into_schema(&record, output_schema);
        for (field, (value, _)) in mutations[row_idx].drain() {
            row.set(&field, value);
        }
        stamp_audit(&mut row, false, "", &mutated_by[row_idx]);
        out.push((row, row_num));
    }
    out.append(&mut synthesized);
    Ok(())
}

/// Push a `MutationConflict` DLQ entry for the colliding row and roll the
/// whole group back. Returns `Ok(())` so the caller skips emitting this
/// group's rows.
fn dlq_group_conflict(
    ctx: &mut ExecutorContext<'_>,
    node_name: &str,
    conflict: &MutationConflict,
) -> Result<(), PipelineError> {
    let MutationConflict {
        rule_a,
        rule_b,
        field,
        record,
        row_num,
    } = conflict;
    let stage = stage_reshape_mutation_conflict(node_name, rule_a, rule_b);
    let source_name = source_name_arc_of(record);
    push_dlq(
        ctx,
        DlqEntry {
            source_row: *row_num,
            category: DlqErrorCategory::MutationConflict,
            error_message: format!(
                "reshape {node_name:?}: rules {rule_a:?} and {rule_b:?} both write field \
                 {field:?} on the same row — the correlation group is rolled back"
            ),
            original_record: record.clone(),
            stage: Some(stage),
            route: None,
            trigger: true,
            source_name,
            triggering_field: Some(Arc::from(field.as_str())),
            triggering_value: None,
        },
    )
}

/// Evaluate a single-`emit` scalar program and extract the assigned value.
fn eval_scalar(
    evaluator: &mut ProgramEvaluator,
    eval_ctx: &EvalContext<'_>,
    record: &Record,
    field: &str,
) -> Result<Value, cxl::eval::EvalError> {
    match evaluator.eval_record::<NullStorage>(eval_ctx, record, None)? {
        EvalResult::Emit { mut fields, .. } => {
            Ok(fields.shift_remove(field).unwrap_or(Value::Null))
        }
        EvalResult::EmitMany { .. } | EvalResult::Skip(_) => Ok(Value::Null),
    }
}

/// Rebuild the per-dispatch executor rules from the node's compiled rule
/// programs. Each [`CompiledReshapeRule`] carries the typechecked
/// `Arc<TypedProgram>`s (built once at lowering); the `ProgramEvaluator`
/// (non-`Clone`, holds per-thread state) is reconstructed here per dispatch,
/// mirroring how Route and Aggregation rebuild their evaluators off-node.
fn build_rules(compiled_rules: &[CompiledReshapeRule]) -> Vec<CompiledRule> {
    compiled_rules
        .iter()
        .map(|r| CompiledRule {
            name: r.name.clone(),
            when: ProgramEvaluator::new(Arc::clone(&r.when), false),
            set: r
                .set
                .iter()
                .map(|(field, prog)| {
                    (
                        field.clone(),
                        ProgramEvaluator::new(Arc::clone(prog), false),
                    )
                })
                .collect(),
            synth: r.synth.as_ref().map(|s| CompiledSynth {
                copy_from: s.copy_from,
                overrides: s
                    .overrides
                    .iter()
                    .map(|(field, prog)| {
                        (
                            field.clone(),
                            ProgramEvaluator::new(Arc::clone(prog), false),
                        )
                    })
                    .collect(),
            }),
        })
        .collect()
}

/// Extract the `partition_by` key tuple from a record. Mirrors the
/// correlation-buffer keying: an empty string and a null both map to
/// `GroupByKey::Null` so a missing partition value groups consistently.
fn partition_key(record: &Record, partition_by: &[String]) -> Vec<GroupByKey> {
    partition_by
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            let v = record.get(f).unwrap_or(&Value::Null);
            if v.is_null() || matches!(v, Value::String(s) if s.is_empty()) {
                return GroupByKey::Null;
            }
            clinker_record::value_to_group_key(v, f, None, idx as u64)
                .ok()
                .flatten()
                .unwrap_or(GroupByKey::Null)
        })
        .collect()
}

/// Sort a group in place by `order_by`, stable across equal keys (so
/// arrival order breaks ties deterministically).
fn sort_group(group: &mut [(Record, u64)], order_by: &[SortField]) {
    group.sort_by(|(a, _), (b, _)| {
        for sf in order_by {
            let av = a.get(&sf.field).unwrap_or(&Value::Null);
            let bv = b.get(&sf.field).unwrap_or(&Value::Null);
            let ord = match (av, bv) {
                (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
                // Nulls sort last regardless of direction (SQL convention).
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

/// Re-key a record onto the (audit-widened) output schema, carrying every
/// matching column value through and defaulting new columns to null.
fn clone_into_schema(record: &Record, schema: &Arc<clinker_record::Schema>) -> Record {
    let mut values = Vec::with_capacity(schema.column_count());
    for col in schema.columns() {
        values.push(record.get(col.as_ref()).cloned().unwrap_or(Value::Null));
    }
    Record::new(Arc::clone(schema), values)
}

/// Write the three `$meta.*` audit columns onto a row.
fn stamp_audit(row: &mut Record, synthetic: bool, synthesized_by: &str, mutated_by: &[String]) {
    row.set(RESHAPE_SYNTHETIC_COLUMN, Value::Bool(synthetic));
    row.set(
        RESHAPE_SYNTHESIZED_BY_COLUMN,
        Value::String(synthesized_by.into()),
    );
    row.set(
        RESHAPE_MUTATED_BY_COLUMN,
        Value::String(mutated_by.join(",").into()),
    );
}

/// `<node>:<rule>` audit label.
fn rule_label(node_name: &str, rule_name: &str) -> String {
    format!("{node_name}:{rule_name}")
}

/// Wrap a CXL eval error from a rule fragment as a hard pipeline error.
/// Rule-fragment eval errors are setup-invariant violations (the fragment
/// typechecked at bind time), so they abort rather than route to the DLQ.
fn reshape_eval_error(
    node_name: &str,
    rule_name: &str,
    field: &str,
    e: cxl::eval::EvalError,
) -> PipelineError {
    PipelineError::Internal {
        op: "reshape",
        node: node_name.to_string(),
        detail: format!("rule {rule_name:?} field {field:?} eval failed: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::memory::NoOpPolicy;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec!["gid".into(), "payload".into()]))
    }

    /// One record carrying a `gid` group key and a fixed-width `payload`
    /// string, so each record has a predictable, non-trivial heap footprint.
    fn rec(schema: &Arc<Schema>, gid: &str, payload: &str) -> Record {
        Record::new(
            Arc::clone(schema),
            vec![Value::String(gid.into()), Value::String(payload.into())],
        )
    }

    /// An arbitrator whose soft limit is `soft_bytes` and whose seeded peak
    /// RSS is well below it, so `should_spill_self` is driven purely by the
    /// caller's explicit spill calls rather than the live process RSS.
    fn arbitrator(soft_bytes: u64) -> MemoryArbitrator {
        // soft = limit * 0.80, so limit = soft / 0.80.
        let limit = (soft_bytes as f64 / 0.80) as u64;
        let arb = MemoryArbitrator::with_policy(limit, 0.80, Box::new(NoOpPolicy));
        // Keep the RSS arm quiet: a tiny seeded peak never trips the soft
        // limit, so the buffer's own resident-byte total governs spilling.
        arb.set_peak_rss_for_test(1);
        arb
    }

    /// The single group key these tests partition on.
    fn single_group_key() -> Vec<GroupByKey> {
        vec![GroupByKey::Str("g".into())]
    }

    /// Accumulate `n` records into one group, spilling whenever the resident
    /// footprint exceeds the arbitrator's soft limit — the same admit-then-
    /// spill cadence the dispatch loop runs. Returns the populated buffer.
    fn fill_single_group(
        schema: &Arc<Schema>,
        arb: &MemoryArbitrator,
        spill_root: &std::path::Path,
        n: u64,
    ) -> ReshapeGroupBuffer {
        let handle = ConsumerHandle::new();
        let mut buffer = ReshapeGroupBuffer::new(Arc::clone(schema), true);
        for row_num in 0..n {
            let payload = format!("{row_num:063}");
            buffer.push(single_group_key(), rec(schema, "g", &payload), row_num);
            handle.set_bytes(buffer.resident_bytes() as u64);
            if arb.spill_threshold_bytes() < buffer.resident_bytes() as u64 {
                buffer
                    .spill_until_under_budget("rs", arb, spill_root, &handle)
                    .unwrap();
            }
        }
        buffer
    }

    // F2: a group with no `order_by` whose records partition-spill across
    // buckets must reload in arrival (row-number) order, identical to the
    // never-spilled path. The bug reassembled by bucket/spill order, making
    // emit order depend on the memory budget.
    #[test]
    fn partition_spill_preserves_arrival_order() {
        let schema = schema();
        let spill_root = tempfile::tempdir().unwrap();
        // One group, 64 records, each ~64-byte payload. Total ~5 KiB; a 512 B
        // soft limit forces the single group to partition-spill.
        let arb = arbitrator(512);
        let key = single_group_key();
        let mut buffer = fill_single_group(&schema, &arb, spill_root.path(), 64);
        // The group must have spilled (otherwise this asserts nothing).
        assert!(
            !buffer.groups[&key].spilled.is_empty(),
            "the single oversized group must have partition-spilled"
        );
        let group = buffer.take_group("rs", &key, 0).unwrap();
        let row_nums: Vec<u64> = group.iter().map(|(_, rn)| *rn).collect();
        let expected: Vec<u64> = (0..64).collect();
        assert_eq!(
            row_nums, expected,
            "a partition-spilled group must reload in arrival (row-number) order"
        );
    }

    // F3: the spill loop must stop as soon as the resident footprint is back
    // under the soft threshold — it must NOT evict every resident group. The
    // bug read the monotonic RSS high-water and drained everything on the
    // first trip.
    #[test]
    fn spill_stops_at_soft_threshold_without_draining_all_groups() {
        let schema = schema();
        let spill_root = tempfile::tempdir().unwrap();
        // 20 small groups, each one ~80-byte record. Soft limit sized to hold
        // roughly half of them resident.
        let mut buffer = ReshapeGroupBuffer::new(Arc::clone(&schema), true);
        for g in 0..20u64 {
            let payload = format!("{g:063}");
            buffer.push(
                vec![GroupByKey::Int(g as i64)],
                rec(&schema, &g.to_string(), &payload),
                g,
            );
        }
        let total = buffer.resident_bytes();
        // Soft = ~40% of the total footprint, so spilling must stop with a
        // meaningful resident remainder rather than draining to zero.
        let soft = (total / 2) as u64;
        let arb = arbitrator(soft.max(1));
        let handle = ConsumerHandle::new();
        handle.set_bytes(total as u64);

        buffer
            .spill_until_under_budget("rs", &arb, spill_root.path(), &handle)
            .unwrap();

        assert!(
            buffer.resident_bytes() <= arb.spill_threshold_bytes() as usize,
            "spill loop must bring the resident footprint under the soft limit: \
             resident={} soft={}",
            buffer.resident_bytes(),
            arb.spill_threshold_bytes()
        );
        assert!(
            buffer.resident_bytes() > 0,
            "spill loop must NOT drain every group — some must remain resident \
             under the soft limit (resident={})",
            buffer.resident_bytes()
        );
        // The handle the arbitrator reads must reflect the reduced footprint,
        // not the stale pre-spill total.
        assert_eq!(
            handle.bytes() as usize,
            buffer.resident_bytes(),
            "the consumer handle must track the live resident footprint after spilling"
        );
    }

    // F4 prerequisite: `take_group` refuses a single correlation group whose
    // reloaded footprint exceeds the hard limit rather than reloading it and
    // risking an OOM. The skew slicing bounds ingest peak, not the finalize
    // reload of one giant group.
    #[test]
    fn take_group_rejects_a_group_larger_than_the_hard_limit() {
        let schema = schema();
        let spill_root = tempfile::tempdir().unwrap();
        let arb = arbitrator(512);
        let key = single_group_key();
        let mut buffer = fill_single_group(&schema, &arb, spill_root.path(), 64);
        // A hard limit far below the group's reloaded footprint must fail loud.
        let err = buffer
            .take_group("rs", &key, 256)
            .expect_err("a group exceeding the hard limit must be rejected at finalize");
        let detail = format!("{err:?}");
        assert!(
            detail.contains("single Reshape correlation group"),
            "the diagnostic must name the giant-group limitation: {detail}"
        );
    }
}
