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
//! post-processed output rows — through a [`SpillWriter<u64>`], then re-runs
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
//! upper bits of its arrival hash so successive spill waves evict slices of
//! the one group rather than buffering it whole — while smaller groups stay
//! resident under the byte budget. The consumer registers at priority `15`
//! (between grace-hash and external sort) and cannot back-pressure: there is
//! no upstream channel to gate once the predecessor has drained.
//!
//! # No-cascade contract
//!
//! Every rule observes the **original** group snapshot. A row mutated by
//! rule A is not re-observed by rule B; conflicting writes are detected and
//! the whole group is rolled back rather than silently order-dependent.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use clinker_record::{GroupByKey, Record, Schema, Value};
use cxl::eval::{EvalContext, EvalResult, ProgramEvaluator};
use cxl::typecheck::{QualifiedField, Row, Type};
use petgraph::graph::NodeIndex;

use crate::executor::DlqEntry;
use crate::executor::dispatch::{
    ExecutorContext, admit_node_buffer, drain_node_buffer_slot, node_buffer_spill_allowed,
    push_dlq, source_file_arc_of, source_name_arc_of, tee_emit_to_region_input_buffers,
};
use crate::pipeline::memory::{ConsumerHandle, ConsumerSpillError, MemoryConsumer};
use crate::pipeline::spill::{SpillFile, SpillWriter};
use clinker_core_types::dlq::{DlqErrorCategory, stage_reshape_mutation_conflict};
use clinker_plan::config::pipeline_node::{
    CopyFrom, RESHAPE_MUTATED_BY_COLUMN, RESHAPE_SYNTHESIZED_BY_COLUMN, RESHAPE_SYNTHETIC_COLUMN,
    ReshapeBody,
};
use clinker_plan::config::{SortField, SortOrder};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode, single_predecessor};

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
    input: Vec<(Record, u64)>,
    input_puncts: Vec<crate::executor::stream_event::Punctuation>,
    handle: &Arc<ConsumerHandle>,
) -> Result<(), PipelineError> {
    // Compile every rule's predicate / assignment program against the live
    // input schema. The schema is uniform across a node_buffer slot, so the
    // first record's schema drives the typecheck row.
    let input_schema = input[0].0.schema().clone();
    let mut rules = compile_rules(name, config, &input_schema)?;

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
    let mut buffer = ReshapeGroupBuffer::new(Arc::clone(&input_schema), spill_compress);
    for (record, row_num) in input {
        let key = partition_key(&record, &config.partition_by);
        buffer.push(key, record, row_num);
        handle.set_bytes(buffer.resident_bytes() as u64);
        // Poll the arbitrator at every admission. `should_spill` trips on RSS
        // or the summed consumer bytes crossing the soft limit and flips this
        // consumer's spill-request flag when the policy elects it; either trip
        // evicts groups until the resident footprint drops back under budget.
        if ctx.memory_budget.should_spill() || handle.take_spill_request() {
            buffer.spill_until_under_budget(name, ctx)?;
            handle.set_bytes(buffer.resident_bytes() as u64);
        }
    }

    // Finalize: stream each group back in first-seen order (from memory when
    // it never spilled, else reloaded from its spill files), re-sort, and run
    // the rule kernel unchanged. Releasing each group's resident bytes as it
    // finalizes keeps the buffer footprint shrinking through the drain.
    let mut out: Vec<(Record, u64)> = Vec::new();
    let group_order = buffer.take_group_order();
    for key in group_order {
        let mut group = buffer.take_group(name, &key)?;
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
    resident: Vec<(Record, u64)>,
    /// Estimated heap bytes of `resident`, maintained incrementally so the
    /// buffer's running total never rescans the group.
    resident_bytes: usize,
    /// Input-record slices already written to disk, oldest first. Reloaded in
    /// order at finalize and concatenated ahead of `resident` so arrival
    /// order survives the round-trip.
    spilled: Vec<SpillFile<u64>>,
}

impl ReshapeGroupState {
    fn new() -> Self {
        Self {
            resident: Vec::new(),
            resident_bytes: 0,
            spilled: Vec::new(),
        }
    }
}

/// Per-group input buffer with arbitrator-driven spill.
///
/// Holds every drained input record grouped by `partition_by` in first-seen
/// order. Under memory pressure it evicts whole resident groups to disk
/// largest-first; a single group too large to fit on its own is sliced by
/// the upper bits of each record's arrival hash so successive waves spill
/// slices of the one group rather than buffering it whole. At finalize each
/// group is streamed back — resident records plus reloaded spill slices in
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
}

impl ReshapeGroupBuffer {
    fn new(input_schema: Arc<Schema>, compress: bool) -> Self {
        Self {
            input_schema,
            compress,
            group_order: Vec::new(),
            groups: HashMap::new(),
            resident_bytes: 0,
        }
    }

    /// Total resident (in-memory) input-record bytes across all groups.
    fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    /// Admit one record into its group, recording first-seen order.
    fn push(&mut self, key: Vec<GroupByKey>, record: Record, row_num: u64) {
        let bytes = estimated_input_bytes(&record);
        let order = &mut self.group_order;
        let state = self.groups.entry(key.clone()).or_insert_with(|| {
            order.push(key);
            ReshapeGroupState::new()
        });
        state.resident.push((record, row_num));
        state.resident_bytes += bytes;
        self.resident_bytes += bytes;
    }

    /// Take the first-seen group order, consuming it for the finalize drain.
    fn take_group_order(&mut self) -> Vec<Vec<GroupByKey>> {
        std::mem::take(&mut self.group_order)
    }

    /// Evict resident groups to disk until the arbitrator no longer reports
    /// pressure (or nothing resident remains to evict).
    ///
    /// Largest-resident-group-first, matching the grace-hash Largest-Size
    /// victim policy: freeing the biggest holder reclaims the most headroom
    /// per spill. A single group whose resident tail alone still trips the
    /// budget is sliced incrementally by arrival hash rather than buffered
    /// whole — bounded-memory must hold even for one giant correlation group.
    fn spill_until_under_budget(
        &mut self,
        node_name: &str,
        ctx: &mut ExecutorContext<'_>,
    ) -> Result<(), PipelineError> {
        let soft = ctx.memory_budget.spill_threshold_bytes() as usize;
        // Spill while pressure persists. Re-poll RSS via `should_spill_self`
        // (no arbitration round — that could pause the drained-out Source) so
        // a transient RSS spike that the OS reclaims lets accumulation resume
        // in memory rather than thrashing one spill per record.
        while self.resident_bytes > 0 && ctx.memory_budget.should_spill_self() {
            let Some(key) = self.largest_resident_group() else {
                break;
            };
            let resident_bytes = self.groups[&key].resident_bytes;
            if resident_bytes > soft && soft > 0 {
                // One group dwarfs the budget on its own: slice it across
                // partitions so each wave evicts a fraction, never the whole
                // group at once.
                self.spill_group_partitioned(node_name, ctx, &key, soft)?;
            } else {
                self.spill_group_whole(node_name, ctx, &key)?;
            }
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
        ctx: &mut ExecutorContext<'_>,
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
            ctx,
            &self.input_schema,
            self.compress,
            records.iter(),
        )?;
        self.groups
            .get_mut(key)
            .expect("spill target group present")
            .spilled
            .push(file);
        Ok(())
    }

    /// Slice one oversized group's resident tail across hash partitions,
    /// spilling enough partitions to bring the group's resident footprint
    /// under `soft`. The partition assignment uses the upper bits of each
    /// record's arrival hash (independent of insertion order), so a single
    /// giant group is evicted in fractions across successive waves.
    fn spill_group_partitioned(
        &mut self,
        node_name: &str,
        ctx: &mut ExecutorContext<'_>,
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
        let mut buckets: Vec<Vec<(Record, u64)>> = (0..n).map(|_| Vec::new()).collect();
        let mut bucket_bytes: Vec<usize> = vec![0; n];
        for (record, row_num) in resident {
            let p = ((hash_partition_seed(&record, row_num) >> shift) as usize) & (n - 1);
            bucket_bytes[p] += estimated_input_bytes(&record);
            buckets[p].push((record, row_num));
        }

        // Spill whole buckets, largest first, until the group's residual
        // resident footprint drops under `soft`. `remaining` tracks the
        // un-spilled byte total straight off `bucket_bytes`, so the tail's
        // footprint is known without re-walking its records below.
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by_key(|&p| std::cmp::Reverse(bucket_bytes[p]));
        let mut remaining: usize = bucket_bytes.iter().sum();
        let mut spilled_files: Vec<SpillFile<u64>> = Vec::new();
        for &p in &order {
            if remaining <= soft || buckets[p].is_empty() {
                continue;
            }
            let slice = std::mem::take(&mut buckets[p]);
            remaining -= bucket_bytes[p];
            let file = write_spill_slice(
                node_name,
                ctx,
                &self.input_schema,
                self.compress,
                slice.iter(),
            )?;
            spilled_files.push(file);
        }

        // Re-seat the un-spilled buckets as the group's resident tail (its
        // byte total is `remaining`) and record the freshly written slices.
        let tail: Vec<(Record, u64)> = buckets.into_iter().flatten().collect();
        let state = self
            .groups
            .get_mut(key)
            .expect("partition-spill target group present");
        state.resident = tail;
        state.resident_bytes = remaining;
        state.spilled.extend(spilled_files);
        self.resident_bytes += remaining;
        Ok(())
    }

    /// Reload one group's full record set in arrival order: every spilled
    /// slice (oldest first) followed by the resident tail. Releases the
    /// group's resident bytes from the running total as it drains.
    fn take_group(
        &mut self,
        node_name: &str,
        key: &[GroupByKey],
    ) -> Result<Vec<(Record, u64)>, PipelineError> {
        let state = self.groups.remove(key).expect("group key present in order");
        self.resident_bytes -= state.resident_bytes;
        let mut group: Vec<(Record, u64)> = Vec::new();
        for file in &state.spilled {
            let reader = file
                .reader()
                .map_err(|e| reshape_spill_error(node_name, e))?;
            for pair in reader {
                group.push(pair.map_err(|e| reshape_spill_error(node_name, e))?);
            }
        }
        group.extend(state.resident);
        Ok(group)
    }
}

/// Write one input-record slice to a fresh spill file, charging its on-disk
/// byte size against the arbitrator's per-stage and pipeline-wide spill
/// totals so `--explain` calibration and the disk-spill cap both see it.
fn write_spill_slice<'a>(
    node_name: &str,
    ctx: &mut ExecutorContext<'_>,
    input_schema: &Arc<Schema>,
    compress: bool,
    records: impl Iterator<Item = &'a (Record, u64)>,
) -> Result<SpillFile<u64>, PipelineError> {
    let mut writer: SpillWriter<u64> = SpillWriter::new(
        Arc::clone(input_schema),
        Some(&ctx.spill_root_path),
        compress,
    )
    .map_err(|e| reshape_spill_error(node_name, e))?;
    for (record, row_num) in records {
        writer
            .write_pair(record, row_num)
            .map_err(|e| reshape_spill_error(node_name, e))?;
    }
    let file = writer
        .finish()
        .map_err(|e| reshape_spill_error(node_name, e))?;
    let bytes = std::fs::metadata(file.path()).map(|m| m.len()).unwrap_or(0);
    ctx.memory_budget.record_spill_bytes(node_name, bytes);
    Ok(file)
}

/// One input record's estimated buffered footprint: heap bytes plus the
/// owning `Record` header and the `(Record, u64)` tuple overhead.
fn estimated_input_bytes(record: &Record) -> usize {
    record.estimated_heap_size() + std::mem::size_of::<(Record, u64)>()
}

/// Deterministic per-record hash used to slice one oversized group across
/// partitions. Combines the record's values with its row number so two
/// records with identical content still distribute across buckets, keeping a
/// degenerate all-duplicate group from collapsing into a single partition.
fn hash_partition_seed(record: &Record, row_num: u64) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for value in record.values() {
        hash_value(value, &mut hasher);
    }
    row_num.hash(&mut hasher);
    hasher.finish()
}

/// Fold one `Value` into `hasher`. `Float` hashes via its bit pattern (NaN
/// included) so the function is total over every `Value` variant.
fn hash_value(value: &Value, hasher: &mut impl Hasher) {
    match value {
        Value::Null => 0u8.hash(hasher),
        Value::Bool(b) => b.hash(hasher),
        Value::Integer(i) => i.hash(hasher),
        Value::Float(f) => f.to_bits().hash(hasher),
        Value::String(s) => s.as_str().hash(hasher),
        Value::Date(d) => d.hash(hasher),
        Value::DateTime(dt) => dt.hash(hasher),
        Value::Array(arr) => {
            for v in arr {
                hash_value(v, hasher);
            }
        }
        Value::Map(m) => {
            for (k, v) in m.iter() {
                k.hash(hasher);
                hash_value(v, hasher);
            }
        }
    }
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

/// Compile each rule's `when` / `set` / `overrides` CXL against `schema`.
fn compile_rules(
    node_name: &str,
    config: &ReshapeBody,
    schema: &Arc<clinker_record::Schema>,
) -> Result<Vec<CompiledRule>, PipelineError> {
    let type_cols: indexmap::IndexMap<QualifiedField, Type> = schema
        .columns()
        .iter()
        .map(|c| (QualifiedField::bare(c.as_ref()), Type::Any))
        .collect();
    let row = Row::closed(type_cols, cxl::lexer::Span::new(0, 0));
    let field_refs: Vec<&str> = schema.columns().iter().map(|c| c.as_ref()).collect();

    let mut compiled = Vec::with_capacity(config.rules.len());
    for rule in &config.rules {
        let when = compile_program(
            node_name,
            &rule.name,
            "when",
            &format!("filter {}", rule.when.source),
            &row,
            &field_refs,
        )?;
        let mut set = Vec::new();
        if let Some(mutate) = &rule.mutate {
            for (field, value) in &mutate.set {
                let prog = compile_program(
                    node_name,
                    &rule.name,
                    field,
                    &format!("emit {field} = {}", value.source),
                    &row,
                    &field_refs,
                )?;
                set.push((field.clone(), prog));
            }
        }
        let synth = match &rule.synthesize {
            None => None,
            Some(s) => {
                let mut overrides = Vec::new();
                for (field, value) in &s.overrides {
                    let prog = compile_program(
                        node_name,
                        &rule.name,
                        field,
                        &format!("emit {field} = {}", value.source),
                        &row,
                        &field_refs,
                    )?;
                    overrides.push((field.clone(), prog));
                }
                Some(CompiledSynth {
                    copy_from: s.copy_from,
                    overrides,
                })
            }
        };
        compiled.push(CompiledRule {
            name: rule.name.clone(),
            when,
            set,
            synth,
        });
    }
    Ok(compiled)
}

/// Parse → resolve → typecheck a single CXL fragment into a
/// [`ProgramEvaluator`]. Type errors here are a planner invariant violation
/// (bind_schema already typechecked the same fragment), so they surface as
/// `PipelineError::Compilation`.
fn compile_program(
    node_name: &str,
    rule_name: &str,
    field: &str,
    source: &str,
    row: &Row,
    field_refs: &[&str],
) -> Result<ProgramEvaluator, PipelineError> {
    let scoped_vars = cxl::resolve::ScopedVarsRegistry::default();
    let make_err = |messages: Vec<String>| PipelineError::Compilation {
        transform_name: format!("reshape:{node_name}:{rule_name}:{field}"),
        messages,
    };

    let parse_result = cxl::parser::Parser::parse(source);
    if !parse_result.errors.is_empty() {
        return Err(make_err(
            parse_result
                .errors
                .iter()
                .map(|e| e.message.clone())
                .collect(),
        ));
    }
    let resolved = cxl::resolve::resolve_program_with_modules_and_vars(
        parse_result.ast,
        field_refs,
        parse_result.node_count,
        &std::collections::HashMap::new(),
        &scoped_vars,
    )
    .map_err(|diags| make_err(diags.into_iter().map(|d| d.message).collect()))?;
    let typed = cxl::typecheck::pass::type_check_with_mode_and_vars(
        resolved,
        row,
        cxl::typecheck::pass::AggregateMode::Row,
        &scoped_vars,
    )
    .map_err(|diags| {
        make_err(
            diags
                .into_iter()
                .filter(|d| !d.is_warning)
                .map(|d| d.message)
                .collect(),
        )
    })?;
    Ok(ProgramEvaluator::new(Arc::new(typed), false))
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
