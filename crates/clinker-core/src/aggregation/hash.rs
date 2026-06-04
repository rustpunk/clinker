//! Hash aggregation engine — the default `AggregateStrategy::Hash` path.
//!
//! Holds the per-group hash table ([`HashAggregator`]), the prototype
//! clone factory ([`AccumulatorFactory`]), the per-group memory
//! estimator, the arbitrator-facing [`AggregateConsumer`], and the
//! shared group-finalize helpers ([`finalize_group_inner`],
//! [`empty_global_fold_row`], [`group_by_sort_fields`]) that the
//! streaming path reuses to guarantee byte-identical output.

use std::path::PathBuf;
use std::sync::Arc;

use clinker_record::accumulator::{AccumulatorEnum, AccumulatorRow};
use clinker_record::group_key::value_to_group_key;
use clinker_record::schema::Schema;
use clinker_record::{GroupByKey, Record, RecordStorage, Value};
use cxl::ast::Expr;
use cxl::eval::{EvalContext, ProgramEvaluator, eval_expr};
use cxl::plan::{AggregateBinding, BindingArg, CompiledAggregate};

use crate::config::{NullOrder, SortField, SortOrder};
use crate::pipeline::loser_tree::LoserTree;

use super::error::HashAggError;
use super::spill::{
    AggMergeEntry, AggSpillEntry, AggSpillFile, AggSpillReader, AggSpillWriter, SpillState,
};
use super::{
    AggregateEvalScope, AggregatorGroupState, BufferedGroupState, SortRow, dispatch_binding,
    eval_binding_arg_value, eval_expr_in_agg_scope, fold_buffered_state, merge_buffered_sidecars,
    merge_group_sidecars,
};

pub(super) struct NullStorage;
impl RecordStorage for NullStorage {
    fn resolve_field(&self, _: u64, _: &str) -> Option<&Value> {
        None
    }
    fn resolve_qualified(&self, _: u64, _: &str, _: &str) -> Option<&Value> {
        None
    }
    fn available_fields(&self, _: u64) -> Vec<&str> {
        Vec::new()
    }
    fn record_count(&self) -> u64 {
        0
    }
}

/// Per-group prototype clone factory.
///
/// Holds an `Arc<CompiledAggregate>` for the bindings vector and a
/// pre-built prototype `AccumulatorRow` produced once at construction.
/// Each new group clones the prototype rather than re-running the
/// `AccumulatorEnum::for_type` factory.
pub struct AccumulatorFactory {
    compiled: Arc<CompiledAggregate>,
    prototype: AccumulatorRow,
}

impl AccumulatorFactory {
    pub fn new(compiled: Arc<CompiledAggregate>) -> Self {
        let prototype: AccumulatorRow = compiled
            .bindings
            .iter()
            .map(|b| AccumulatorEnum::for_type(&b.acc_type))
            .collect();
        Self {
            compiled,
            prototype,
        }
    }

    /// Build a fresh accumulator row for a newly seen group key.
    pub fn create_accumulators(&self) -> AccumulatorRow {
        self.prototype.clone()
    }

    /// Borrow the binding list authored by the extractor.
    pub fn bindings(&self) -> &[AggregateBinding] {
        &self.compiled.bindings
    }

    /// Borrow the underlying `CompiledAggregate` for paths that need
    /// the group-by metadata or pre-aggregation filter.
    pub fn compiled(&self) -> &Arc<CompiledAggregate> {
        &self.compiled
    }
}

// ---------------------------------------------------------------------------
// Per-group memory estimation (PostgreSQL hash_agg_entry_size pattern)
// ---------------------------------------------------------------------------

/// Conservative estimate of total memory consumed per distinct group in the
/// hash table. Accounts for the hashbrown bucket entry, key heap, accumulator
/// row heap, and IndexMap sidecar overhead.
///
/// Used to pre-compute `max_groups = (budget * 0.6) / estimated_bytes` so the
/// spill trigger can fire on a simple group-count comparison (O(1)) instead of
/// the broken `allocation_size() * 3 > headroom` heuristic that only saw the
/// bucket array.
fn estimated_bytes_per_group(factory: &AccumulatorFactory, gb_count: usize) -> usize {
    // hashbrown stores (Key, Value) inline in the bucket array + 1 control byte.
    let bucket_entry = std::mem::size_of::<(Vec<GroupByKey>, AggregatorGroupState)>() + 1;

    // Vec<GroupByKey> heap: one GroupByKey enum per group-by column, plus an
    // average 32 bytes of string heap per field (typical 10-50 byte keys).
    let key_heap = gb_count * (std::mem::size_of::<GroupByKey>() + 32);

    // AccumulatorRow heap: Vec header is inline in AggregatorGroupState,
    // elements are on the heap.
    let acc_heap = factory.compiled().bindings.len() * std::mem::size_of::<AccumulatorEnum>();

    // hashbrown targets ~87.5% load factor → ~1.15x bucket overallocation.
    let raw = bucket_entry + key_heap + acc_heap;
    (raw as f64 * 1.15) as usize
}

/// Hash aggregation engine — D1 default strategy.
///
/// Holds the per-group hash table, the prototype clone factory from
/// 16.3.7, the pre-aggregation row filter (D9), the residual evaluator
/// (16.3.12), and the bookkeeping needed for the resize-aware spill
/// trigger (D1) and the metadata common-only propagation (D11 revised).
///
/// The 16.3.8 commit lands `new` + `add_record` + memory accounting and
/// the spill-trigger check. `spill`, `finalize`, and `merge_spilled`
/// arrive in 16.3.10–16.3.12 as part of the same atomic 16.3 commit
/// (interim sub-tasks compile because the executor dispatch arm in
/// `PlanNode::Aggregation` is still an `unreachable!` stub until 16.3.13).
pub struct HashAggregator {
    groups: hashbrown::HashMap<Vec<GroupByKey>, AggregatorGroupState>,
    factory: AccumulatorFactory,
    group_by_indices: Vec<u32>,
    group_by_fields: Vec<String>,
    pre_agg_filter: Option<Expr>,
    value_heap_bytes: usize,
    memory_budget: usize,
    spill_files: Vec<AggSpillFile>,
    spill_schema: Arc<Schema>,
    spill_dir: Option<PathBuf>,
    output_schema: Arc<Schema>,
    transform_name: String,
    evaluator: ProgramEvaluator,
    /// Source-row counter — incremented after the pre-agg filter accepts
    /// a record. Used by the global-fold empty-input special case in
    /// `finalize` (D12, D44).
    rows_seen: u64,
    /// Pre-computed maximum group count before spill fires. Derived from
    /// `(memory_budget * 60%) / estimated_bytes_per_group`. The 60% share
    /// leaves 40% for `value_heap_bytes` growth (Collect accumulators,
    /// meta tracker observations). `usize::MAX` when budget is 0.
    max_groups: usize,
    /// Counter for periodic RSS backstop polling. Checked every 4096
    /// records to limit /proc/self/statm read overhead.
    records_since_rss_check: u32,
    /// Flat `(input_row_id, group_index)` lineage map, allocated only
    /// when `compiled.requires_lineage` is true. Append-only on the hot
    /// path; the per-entry footprint (8 bytes) is charged into
    /// `value_heap_bytes` so the existing spill-trigger thresholds gate
    /// it without a separate budget knob. Drained on every `spill()`
    /// call: post-spill the per-group `AggregatorGroupState.input_rows`
    /// holds the canonical lineage shape, and the flat vec is rebuilt
    /// from there during finalize when callers need the dense form.
    lineage: Option<Vec<(u64, u32)>>,
    /// Buffer-mode per-group state, populated only when
    /// `compiled.requires_buffer_mode` is true. Mutually exclusive with
    /// `groups` at the dispatch level — `add_record` consults
    /// `buffer_mode` once and routes to exactly one of the two maps.
    /// Both maps land empty by default; `hashbrown::HashMap::new()`
    /// does not allocate, so the strict path observes zero overhead.
    buffered_groups: hashbrown::HashMap<Vec<GroupByKey>, BufferedGroupState>,
    /// Mirror of `compiled.requires_buffer_mode`. Hoisted to a per-
    /// aggregator field so the hot loop dispatches without an
    /// `Arc<CompiledAggregate>` deref on every record.
    buffer_mode: bool,
    /// Shared `ConsumerHandle` for the pipeline-scoped arbitrator's
    /// `AggregateConsumer` wrapper. Every `value_heap_bytes` mutation
    /// goes through `add_value_heap_bytes` / `sub_value_heap_bytes`
    /// / `reset_value_heap_bytes`, which mirror the running total into
    /// `handle.bytes`. The arbitrator's pull-mode `current_usage`
    /// reads this counter and ranks the aggregator in policy
    /// decisions; the consumer wrapper's `try_spill` flips
    /// `handle.spill_requested`, which the hot loop is expected to
    /// read at batch boundaries once that integration lands.
    consumer_handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
}

impl HashAggregator {
    /// Construct a new aggregator from a compiled aggregate plan.
    ///
    /// `spill_schema` and `spill_dir` are wired for the spill path that
    /// lands in 16.3.10. The 16.3.8 hot loop only touches them through
    /// the `value_heap_bytes` budget check.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        compiled: Arc<CompiledAggregate>,
        evaluator: ProgramEvaluator,
        output_schema: Arc<Schema>,
        spill_schema: Arc<Schema>,
        memory_budget: usize,
        spill_dir: Option<PathBuf>,
        transform_name: impl Into<String>,
        consumer_handle: Arc<crate::pipeline::memory::ConsumerHandle>,
    ) -> Self {
        let group_by_indices = compiled.group_by_indices.clone();
        let group_by_fields = compiled.group_by_fields.clone();
        let pre_agg_filter = compiled.pre_agg_filter.clone();
        let requires_lineage = compiled.requires_lineage;
        let buffer_mode = compiled.requires_buffer_mode;
        debug_assert!(
            !(requires_lineage && buffer_mode),
            "lineage and buffer-mode are mutually exclusive retraction strategies"
        );
        let factory = AccumulatorFactory::new(compiled);
        let estimated = estimated_bytes_per_group(&factory, group_by_indices.len());
        // `.max(1)` clamp: integer division yields 0 when a single group's
        // estimated footprint exceeds the 60% share of the budget. Without
        // the clamp the downstream check `self.groups.len() >= self.max_groups`
        // is `usize >= 0` — always true — and spill fires on every record
        // insertion. Clamping to 1 lets at least one group land before
        // spilling, the minimum sensible behavior at pathologically small
        // budgets.
        let max_groups = if memory_budget > 0 && estimated > 0 {
            ((memory_budget * 60 / 100) / estimated).max(1)
        } else {
            usize::MAX
        };
        // The empty-vec form `Some(Vec::new())` does not allocate until
        // the first push, so the lineage-disabled path observes
        // `lineage = None` and pays exactly zero per-record overhead;
        // the lineage-enabled path eats one branch and an inline
        // `Some` discriminator on every `add_record`.
        let lineage = if requires_lineage {
            Some(Vec::new())
        } else {
            None
        };
        Self {
            groups: hashbrown::HashMap::new(),
            factory,
            group_by_indices,
            group_by_fields,
            pre_agg_filter,
            value_heap_bytes: 0,
            memory_budget,
            spill_files: Vec::new(),
            spill_schema,
            spill_dir,
            output_schema,
            transform_name: transform_name.into(),
            evaluator,
            rows_seen: 0,
            max_groups,
            records_since_rss_check: 0,
            lineage,
            buffered_groups: hashbrown::HashMap::new(),
            buffer_mode,
            consumer_handle,
        }
    }

    /// Add `n` to `value_heap_bytes` with saturating arithmetic and
    /// mirror the new total into the pipeline-scoped arbitrator's
    /// `ConsumerHandle`. Every site that previously inlined
    /// `self.value_heap_bytes = self.value_heap_bytes.saturating_add(n)`
    /// routes through this method so the arbitrator's pull-mode
    /// `current_usage` stays aligned with the operator's internal
    /// counter at every batch boundary.
    fn add_value_heap_bytes(&mut self, n: usize) {
        self.value_heap_bytes = self.value_heap_bytes.saturating_add(n);
        self.consumer_handle.set_bytes(self.value_heap_bytes as u64);
    }

    /// Subtract `n` from `value_heap_bytes` with saturating arithmetic
    /// and mirror the new total into the consumer handle. Pairs with
    /// `add_value_heap_bytes`; saturating-sub prevents the counter
    /// from wrapping below zero when retraction or buffer-mode
    /// discharge over-counts.
    fn sub_value_heap_bytes(&mut self, n: usize) {
        self.value_heap_bytes = self.value_heap_bytes.saturating_sub(n);
        self.consumer_handle.set_bytes(self.value_heap_bytes as u64);
    }

    /// Reset `value_heap_bytes` to zero on spill drain and mirror to
    /// the consumer handle. The arbitrator's `current_usage` drops to
    /// zero in lockstep with the in-memory drain.
    fn reset_value_heap_bytes(&mut self) {
        self.value_heap_bytes = 0;
        self.consumer_handle.set_bytes(0);
    }

    /// Borrow the in-memory group table. Public for finalize and tests.
    pub fn groups(&self) -> &hashbrown::HashMap<Vec<GroupByKey>, AggregatorGroupState> {
        &self.groups
    }

    /// Resolve a group's `input_rows` slice from its stable in-memory
    /// `group_index`, walking whichever map (`groups` for the lineage
    /// path, `buffered_groups` for the buffer path) is populated for
    /// this aggregator. The two maps are mutually exclusive at runtime
    /// because `buffer_mode` is fixed by `compiled.requires_buffer_mode`
    /// at construction.
    ///
    /// Returns `None` when no group with `group_index` exists in the
    /// in-memory state — typically because the aggregate has spilled
    /// (post-merge index space is reassigned by the recovery loop) or
    /// because the index pre-dates a merge step that compacted the
    /// table. The retraction orchestrator's detect phase treats `None`
    /// as a degrade-fallback signal: the wide-net union over each
    /// trigger cell's `error_rows` continues to cover the affected
    /// aggregate group through the strict-collateral path.
    ///
    /// O(groups) — typical aggregate workloads carry hundreds of
    /// groups in memory at most. A cached `HashMap<u32, &Vec<u32>>`
    /// is an obvious future optimization but not warranted at current
    /// call frequencies (one walk per detect-phase synthetic-CK
    /// trigger column).
    pub(crate) fn input_rows_by_group_index(&self, group_index: u32) -> Option<&[(u64, Arc<str>)]> {
        if self.buffer_mode {
            self.buffered_groups
                .values()
                .find(|s| s.group_index == group_index)
                .map(|s| s.input_rows.as_slice())
        } else {
            self.groups
                .values()
                .find(|s| s.group_index == group_index)
                .map(|s| s.input_rows.as_slice())
        }
    }

    /// Group-by column indices in the input record schema. Mirrors the
    /// `CompiledAggregate.group_by_indices` projection the orchestrator's
    /// recompute phase needs to align pre-/post-retract output rows by
    /// group key.
    pub fn group_by_indices(&self) -> &[u32] {
        &self.group_by_indices
    }

    /// Total bytes currently charged into the per-group value heap. Used
    /// by the spill trigger and by tests verifying memory accounting.
    pub fn value_heap_bytes(&self) -> usize {
        self.value_heap_bytes
    }

    /// Pre-computed maximum group count before spill fires.
    pub fn max_groups(&self) -> usize {
        self.max_groups
    }

    /// Number of input records that have passed the pre-aggregation
    /// filter (D44). Drives the global-fold empty-input special case.
    pub fn rows_seen(&self) -> u64 {
        self.rows_seen
    }

    /// Borrow the spill file vector. Empty until 16.3.10 wires the
    /// spill writer.
    pub fn spill_files(&self) -> &[AggSpillFile] {
        &self.spill_files
    }

    /// True when the aggregator's output schema carries a synthetic
    /// `$ck.aggregate.<transform_name>` column — the relaxed-aggregate
    /// shadow lineage finalize stamps with the per-group index. Strict
    /// aggregates have no such column and the predicate is constant
    /// `false` for them, so the runtime counter increment short-circuits
    /// at the call site without paying the per-record schema lookup.
    pub(crate) fn emits_synthetic_ck(&self) -> bool {
        self.output_schema
            .index(&format!("$ck.aggregate.{}", self.transform_name))
            .is_some()
    }

    /// Drive one input record through the aggregator (D1, D9, D10, D11
    /// revised, D44). Order:
    ///
    /// 1. Pre-aggregation filter — skip on `false`.
    /// 2. `rows_seen += 1` (post-filter, per D44).
    /// 3. Extract the group-key tuple.
    /// 4. Look up / insert per-group state via the prototype factory.
    /// 5. Dispatch each `BindingArg` to its accumulator slot.
    /// 6. Resize-aware spill trigger.
    pub fn add_record(
        &mut self,
        record: &Record,
        row_num: u64,
        ctx: &EvalContext,
    ) -> Result<(), HashAggError> {
        if self.buffer_mode {
            return self.add_record_buffered(record, row_num, ctx);
        }
        // 1. Pre-aggregation filter (D9).
        if let Some(filter) = &self.pre_agg_filter {
            let mut env: std::collections::HashMap<String, Value> =
                std::collections::HashMap::new();
            let v = eval_expr::<NullStorage>(
                filter,
                self.evaluator.typed(),
                ctx,
                record,
                None,
                &mut env,
            )?;
            if v != Value::Bool(true) {
                return Ok(());
            }
        }

        // 2. Post-filter row counter (D44).
        self.rows_seen = self.rows_seen.saturating_add(1);

        // 3. Group key extraction (D10 — no schema pin).
        let mut key: Vec<GroupByKey> = Vec::with_capacity(self.group_by_indices.len());
        for (i, idx) in self.group_by_indices.iter().enumerate() {
            let field_name = self
                .group_by_fields
                .get(i)
                .map(String::as_str)
                .unwrap_or("");
            let val = record
                .values()
                .get(*idx as usize)
                .cloned()
                .unwrap_or(Value::Null);
            match value_to_group_key(&val, field_name, None, ctx.source_row) {
                Ok(Some(gk)) => key.push(gk),
                Ok(None) => key.push(GroupByKey::Null),
                Err(e) => {
                    return Err(HashAggError::GroupKey {
                        field: field_name.to_string(),
                        row: ctx.source_row,
                        message: e.to_string(),
                    });
                }
            }
        }

        // 4. Look up / insert per-group state. On the lineage path the
        //    insertion-order index is stamped on each new group so
        //    `add_record` can append `(row_num, group_index)` to the
        //    flat lineage vec without a parallel HashMap; the closure
        //    only fires for new groups, so existing keys keep their
        //    original index.
        let next_group_idx = self.groups.len() as u32;
        let group_state = self.groups.entry(key).or_insert_with(|| {
            let mut state = AggregatorGroupState::new(self.factory.create_accumulators());
            state.group_index = next_group_idx;
            state
        });

        // Track the minimum row_num across every input record folded
        // into this group — emitted as the finalized SortRow's row
        // number so downstream sort-stable operators preserve the
        // earliest input row's position.
        if row_num < group_state.min_row_num {
            group_state.min_row_num = row_num;
        }

        // Lineage recording. Cost: one branch + two appends per record.
        // The 8-byte flat-vec entry plus the 4-byte per-group input_rows
        // entry are charged into `value_heap_bytes` so the existing 40%
        // value-heap spill threshold gates lineage memory without a
        // separate budget knob.
        let lineage_active = self.lineage.is_some();
        if let Some(lin) = self.lineage.as_mut() {
            // `row_num` is u64 in the API; lineage stores u32 because
            // every existing aggregation pipeline is bounded by the
            // 4 GiB hash-aggregation budget. A row number exceeding
            // u32::MAX requires no special handling here — the truncated
            // lineage entry will sit alongside the full-precision
            // `min_row_num`, which is what downstream sort-stable
            // ordering uses.
            let row_u64 = row_num;
            let group_idx = group_state.group_index;
            lin.push((row_u64, group_idx));
            // Source identity is extracted from the record's
            // engine-stamped `$source.name` column so retract scopes can
            // narrow rewinds to the failing source's contributions only.
            let source_name = crate::executor::dispatch::source_name_arc_of(record);
            group_state.input_rows.push((row_u64, source_name));
            // Lineage memory: flat `(row_u64, group_idx)` plus the
            // per-group `(u64, Arc<str>)` entry. The Arc fat pointer is
            // 16 bytes and shares the underlying source-name allocation
            // across every row's entry, so per-row charge is dominated
            // by the inline tuple bytes; the source-name allocation
            // itself is charged once per distinct source (small,
            // bounded) and folded into the flat-vec accounting below.
            // Inlined add: a live `group_state` borrow into `self.groups`
            // prevents going through `add_value_heap_bytes(&mut self)`.
            // Field-level borrows of `value_heap_bytes` and the
            // `&self`-method `consumer_handle.set_bytes` are non-
            // overlapping borrows the borrow checker can split.
            self.value_heap_bytes = self.value_heap_bytes.saturating_add(
                std::mem::size_of::<(u64, u32)>() + std::mem::size_of::<(u64, Arc<str>)>(),
            );
            self.consumer_handle.set_bytes(self.value_heap_bytes as u64);
        }

        // 5. BindingArg dispatch hot loop (D1).
        //
        // Lineage path evaluates each binding's argument to a Value
        // first, caches it on the per-group `retract_values`, and then
        // folds it through the accumulator. Caching the Value lets the
        // commit-step `retract_row` walk back exactly the same
        // contribution; the alternative — re-evaluating the binding
        // expression against the original record at retract time —
        // would either need the original record retained (memory cost
        // outpaces this Vec) or be impossible for bindings that close
        // over now-expired Transform locals. The non-lineage strict
        // path keeps the original tight per-binding dispatch.
        let bindings = self.factory.compiled().bindings.clone();
        let mut delta: usize = 0;
        if lineage_active {
            let mut row_values: Vec<Value> = Vec::with_capacity(bindings.len());
            let mut row_heap_bytes: usize = 0;
            for (binding, acc) in bindings.iter().zip(group_state.row.iter_mut()) {
                match &binding.arg {
                    BindingArg::Pair(a, b) => {
                        let va = eval_binding_arg_value(a, record, ctx, &self.evaluator)?;
                        let vb = eval_binding_arg_value(b, record, ctx, &self.evaluator)?;
                        // Lineage + Pair only co-occur on a relaxed-CK
                        // aggregate whose only Pair binding is
                        // WeightedAvg, which is BufferRequired and would
                        // route through buffer-mode instead. The branch
                        // is kept defensively so a future Pair binding
                        // that enters the Reversible set does not break
                        // here.
                        row_heap_bytes = row_heap_bytes
                            .saturating_add(va.heap_size())
                            .saturating_add(vb.heap_size());
                        row_values.push(va.clone());
                        row_values.push(vb.clone());
                        acc.add_weighted(&va, &vb);
                    }
                    BindingArg::Field(idx) => {
                        let v = record
                            .values()
                            .get(*idx as usize)
                            .cloned()
                            .unwrap_or(Value::Null);
                        row_heap_bytes = row_heap_bytes.saturating_add(v.heap_size());
                        delta += acc.add(&v);
                        row_values.push(v);
                    }
                    BindingArg::Wildcard => {
                        delta += acc.add(&Value::Null);
                        row_values.push(Value::Null);
                    }
                    BindingArg::Expr(e) => {
                        let mut env: std::collections::HashMap<String, Value> =
                            std::collections::HashMap::new();
                        let v = eval_expr::<NullStorage>(
                            e,
                            self.evaluator.typed(),
                            ctx,
                            record,
                            None,
                            &mut env,
                        )?;
                        row_heap_bytes = row_heap_bytes.saturating_add(v.heap_size());
                        delta += acc.add(&v);
                        row_values.push(v);
                    }
                }
            }
            // Charge the per-row retract-values cache: enum-tag bytes
            // for each Value slot plus recursive heap plus the inner
            // Vec's header.
            let row_charge = std::mem::size_of::<Value>().saturating_mul(row_values.len())
                + row_heap_bytes
                + std::mem::size_of::<Vec<Value>>();
            // Inlined add — same `group_state` split-borrow as the
            // lineage path above.
            self.value_heap_bytes = self.value_heap_bytes.saturating_add(row_charge);
            self.consumer_handle.set_bytes(self.value_heap_bytes as u64);
            group_state.retract_values.push(row_values);
        } else {
            for (binding, acc) in bindings.iter().zip(group_state.row.iter_mut()) {
                delta += dispatch_binding(&binding.arg, acc, record, ctx, &self.evaluator)?;
            }
        }
        self.add_value_heap_bytes(delta);

        // 7. Dual-threshold spill trigger (PostgreSQL hash_agg_check_limits
        // pattern). Primary: group count exceeds pre-computed max_groups
        // (60% of budget / estimated_bytes_per_group). Secondary:
        // value_heap_bytes exceeds 40% of budget (catches Collect
        // accumulators that grow unbounded per group).
        if (self.memory_budget > 0
            && (self.groups.len() >= self.max_groups
                || self.value_heap_bytes > self.memory_budget * 40 / 100))
            || self.consumer_handle.take_spill_request()
        {
            self.spill()?;
        }

        // 8. RSS backstop — periodic process-wide memory check. Catches
        //    cases where the per-group estimate underestimates or Collect
        //    accumulators grow beyond what value_heap_bytes tracks.
        self.records_since_rss_check += 1;
        if self.records_since_rss_check >= 4096 {
            self.records_since_rss_check = 0;
            if self.memory_budget > 0
                && let Some(rss) = crate::pipeline::memory::rss_bytes()
            {
                let backstop = (self.memory_budget as u64) * 85 / 100;
                if rss > backstop {
                    tracing::warn!(
                        transform = %self.transform_name,
                        rss_mb = rss / (1024 * 1024),
                        budget_mb = self.memory_budget / (1024 * 1024),
                        groups = self.groups.len(),
                        "RSS backstop triggered — force-spilling"
                    );
                    self.spill()?;
                }
            }
        }
        Ok(())
    }

    /// Buffer-mode ingest path. Steps 1–4 mirror `add_record` (filter,
    /// row counter, group key, per-group state lookup); steps 5–8
    /// diverge:
    ///
    /// 5. Evaluate every binding's argument into a `Vec<Value>` and
    ///    apply the hard-limit guard before mutating per-group state:
    ///    if a single record's contribution exceeds the entire memory
    ///    budget, spilling cannot rescue us and the panic fires.
    /// 6. Push the per-row values onto `BufferedGroupState.contributions`
    ///    and charge `O(n_bindings × value_size)` into `value_heap_bytes`.
    /// 7. Same dual-threshold spill trigger as fold-mode.
    /// 8. RSS backstop, same shape as fold-mode.
    fn add_record_buffered(
        &mut self,
        record: &Record,
        row_num: u64,
        ctx: &EvalContext,
    ) -> Result<(), HashAggError> {
        // 1. Pre-aggregation filter (same as fold-mode).
        if let Some(filter) = &self.pre_agg_filter {
            let mut env: std::collections::HashMap<String, Value> =
                std::collections::HashMap::new();
            let v = eval_expr::<NullStorage>(
                filter,
                self.evaluator.typed(),
                ctx,
                record,
                None,
                &mut env,
            )?;
            if v != Value::Bool(true) {
                return Ok(());
            }
        }

        // 2. Post-filter row counter.
        self.rows_seen = self.rows_seen.saturating_add(1);

        // 3. Group key extraction (same as fold-mode).
        let mut key: Vec<GroupByKey> = Vec::with_capacity(self.group_by_indices.len());
        for (i, idx) in self.group_by_indices.iter().enumerate() {
            let field_name = self
                .group_by_fields
                .get(i)
                .map(String::as_str)
                .unwrap_or("");
            let val = record
                .values()
                .get(*idx as usize)
                .cloned()
                .unwrap_or(Value::Null);
            match value_to_group_key(&val, field_name, None, ctx.source_row) {
                Ok(Some(gk)) => key.push(gk),
                Ok(None) => key.push(GroupByKey::Null),
                Err(e) => {
                    return Err(HashAggError::GroupKey {
                        field: field_name.to_string(),
                        row: ctx.source_row,
                        message: e.to_string(),
                    });
                }
            }
        }

        // 4. Look up / insert per-group buffered state. Insertion-order
        //    index stamping mirrors the fold-mode lineage path so the
        //    rollback step can address groups by index without a parallel
        //    HashMap lookup.
        let next_group_idx = self.buffered_groups.len() as u32;
        let group_state = self.buffered_groups.entry(key).or_insert_with(|| {
            let mut state = BufferedGroupState::new();
            state.group_index = next_group_idx;
            state
        });

        if row_num < group_state.min_row_num {
            group_state.min_row_num = row_num;
        }

        // 5. Evaluate each binding's argument and push the row's
        //    per-binding `Vec<Value>` onto `contributions`. `Pair`
        //    bindings (`weighted_avg(value, weight)`) collapse into a
        //    two-Value inner vec so the rollback step can replay the
        //    exact same `(value, weight)` pair through `add_weighted`.
        let bindings = self.factory.compiled().bindings.clone();
        let row_u64 = row_num;
        let mut row_values: Vec<Value> = Vec::with_capacity(bindings.len());
        let mut row_heap_bytes: usize = 0;
        for binding in bindings.iter() {
            match &binding.arg {
                BindingArg::Pair(a, b) => {
                    let va = eval_binding_arg_value(a, record, ctx, &self.evaluator)?;
                    let vb = eval_binding_arg_value(b, record, ctx, &self.evaluator)?;
                    row_heap_bytes = row_heap_bytes
                        .saturating_add(va.heap_size())
                        .saturating_add(vb.heap_size());
                    // `Pair` bindings widen the row to two slots: the
                    // rollback path replays them through `add_weighted`.
                    row_values.push(va);
                    row_values.push(vb);
                }
                _ => {
                    let v = eval_binding_arg_value(&binding.arg, record, ctx, &self.evaluator)?;
                    row_heap_bytes = row_heap_bytes.saturating_add(v.heap_size());
                    row_values.push(v);
                }
            }
        }
        // Memory cost for this row: enum-tag bytes for every Value slot
        // plus the recursive heap of each Value plus the outer `Vec`'s
        // capacity (one inner Vec per row) plus the per-row
        // `(u64, Arc<str>)` lineage tuple.
        let row_value_count = row_values.len();
        let row_charge = std::mem::size_of::<Value>().saturating_mul(row_value_count)
            + row_heap_bytes
            + std::mem::size_of::<Vec<Value>>()
            + std::mem::size_of::<(u64, Arc<str>)>();
        // Hard-limit guard. A single record whose contributions alone
        // exceed the entire memory budget cannot be held in memory and
        // cannot be salvaged by spilling — the very next add of the same
        // shape will repeat the overflow. Panic loudly. The relaxed-
        // correlation-key commit step's degrade-fallback surface will
        // replace this guard with a strict-collateral DLQ path for the
        // affected groups when it lands.
        if self.memory_budget > 0 && row_charge > self.memory_budget {
            panic!(
                "buffered contributions exceeded hard limit (row_charge={}, budget={}); \
                 relaxed-correlation-key group cannot be recomputed; this is a runtime \
                 guard until the relaxed-correlation-key commit step's degrade-fallback \
                 lands",
                row_charge, self.memory_budget
            );
        }
        // Source identity is extracted at ingest so the buffer path's
        // retract walk can scope rewinds per source, matching the
        // lineage path's per-source narrowing semantics.
        let source_name = crate::executor::dispatch::source_name_arc_of(record);
        group_state.contributions.push(row_values);
        group_state.input_rows.push((row_u64, source_name));
        self.add_value_heap_bytes(row_charge);

        // 7. Spill trigger — same dual-threshold check as fold-mode.
        //    `groups.len()` is always 0 in buffer-mode; the buffered
        //    map's group count is the relevant signal.
        if (self.memory_budget > 0
            && (self.buffered_groups.len() >= self.max_groups
                || self.value_heap_bytes > self.memory_budget * 40 / 100))
            || self.consumer_handle.take_spill_request()
        {
            self.spill()?;
        }

        // 8. RSS backstop — same shape as fold-mode.
        self.records_since_rss_check += 1;
        if self.records_since_rss_check >= 4096 {
            self.records_since_rss_check = 0;
            if self.memory_budget > 0
                && let Some(rss) = crate::pipeline::memory::rss_bytes()
            {
                let backstop = (self.memory_budget as u64) * 85 / 100;
                if rss > backstop {
                    tracing::warn!(
                        transform = %self.transform_name,
                        rss_mb = rss / (1024 * 1024),
                        budget_mb = self.memory_budget / (1024 * 1024),
                        groups = self.buffered_groups.len(),
                        "RSS backstop triggered — force-spilling"
                    );
                    self.spill()?;
                }
            }
        }
        Ok(())
    }

    /// Retract one previously-ingested row's contribution to its group.
    ///
    /// Looks up the group containing `input_row_id` via `self.lineage`
    /// (fold-mode) or by linear scan over `BufferedGroupState.input_rows`
    /// (buffer-mode), then walks back the row's contribution:
    ///
    /// * **Lineage / fold path** — for each binding's accumulator in the
    ///   group, calls `acc.sub(&stored_value)` against the corresponding
    ///   slot in `AggregatorGroupState.retract_values`. Reversible-only
    ///   bindings are guaranteed by `set_retraction_flags`.
    /// * **Buffer path** — removes the matching entry from
    ///   `BufferedGroupState.contributions`/`input_rows`. The next
    ///   `finalize` call re-folds the surviving contributions through a
    ///   fresh accumulator row, so `BufferRequired` bindings (`Min`,
    ///   `Max`, `Avg`, `WeightedAvg`) recompute byte-identically to a
    ///   feed-from-scratch over the surviving rows.
    ///
    /// Both paths return [`HashAggError::Spill`] when the row id is not
    /// found in any in-memory group; spilled groups cannot be retracted
    /// in-place because the on-disk encoding is finalized accumulator
    /// state. The orchestrator's degrade-fallback surface catches that
    /// case and routes the affected groups through strict-collateral DLQ
    /// per the relaxed-CK fallback contract.
    pub(crate) fn retract_row(
        &mut self,
        input_row_id: u64,
        source: &Arc<str>,
    ) -> Result<(), HashAggError> {
        if !self.spill_files.is_empty() {
            return Err(HashAggError::Spill(format!(
                "retract_row {input_row_id} called on aggregator with spilled groups; \
                 in-place retraction is only available against in-memory state"
            )));
        }

        if self.buffer_mode {
            for state in self.buffered_groups.values_mut() {
                if let Some(idx) = state
                    .input_rows
                    .iter()
                    .position(|(r, sn)| *r == input_row_id && sn.as_ref() == source.as_ref())
                {
                    let removed = state.contributions.remove(idx);
                    state.input_rows.remove(idx);
                    let row_charge = std::mem::size_of::<Value>().saturating_mul(removed.len())
                        + removed.iter().map(Value::heap_size).sum::<usize>()
                        + std::mem::size_of::<Vec<Value>>()
                        + std::mem::size_of::<(u64, Arc<str>)>();
                    self.sub_value_heap_bytes(row_charge);
                    return Ok(());
                }
            }
            return Err(HashAggError::Spill(format!(
                "retract_row {input_row_id} not found in any buffered group"
            )));
        }

        // Lineage path: walk every group, find the row in `input_rows`,
        // sub each binding's accumulator with the cached retract Value.
        let bindings = self.factory.compiled().bindings.clone();
        for state in self.groups.values_mut() {
            let Some(idx) = state
                .input_rows
                .iter()
                .position(|(r, sn)| *r == input_row_id && sn.as_ref() == source.as_ref())
            else {
                continue;
            };
            let values = state.retract_values.remove(idx);
            state.input_rows.remove(idx);
            let mut value_cursor = 0usize;
            let mut total_delta: isize = 0;
            for (binding, acc) in bindings.iter().zip(state.row.iter_mut()) {
                match &binding.arg {
                    BindingArg::Pair(_, _) => {
                        // Lineage + Pair only co-occurs through the
                        // defensive branch in `add_record`; today every
                        // Pair-shaped binding (`WeightedAvg`) is
                        // BufferRequired and runs through the buffer
                        // arm above.
                        debug_assert!(false, "Pair binding under lineage path is unreachable");
                        value_cursor += 2;
                    }
                    _ => {
                        let v = values.get(value_cursor).cloned().unwrap_or(Value::Null);
                        total_delta += acc.sub(&v);
                        value_cursor += 1;
                    }
                }
            }
            // Apply the heap delta. The retract_values entry has already
            // been removed; charge that off too.
            let removed_row_charge = std::mem::size_of::<Value>().saturating_mul(values.len())
                + values.iter().map(Value::heap_size).sum::<usize>()
                + std::mem::size_of::<Vec<Value>>()
                + std::mem::size_of::<(u64, Arc<str>)>();
            self.sub_value_heap_bytes(removed_row_charge);
            // Negative `total_delta` is a shrink; saturating-sub clamps
            // at zero against the running total.
            if total_delta < 0 {
                self.sub_value_heap_bytes((-total_delta) as usize);
            } else {
                self.add_value_heap_bytes(total_delta as usize);
            }
            return Ok(());
        }
        Err(HashAggError::Spill(format!(
            "retract_row {input_row_id} not found in any lineage group"
        )))
    }

    /// Drive the aggregator to completion without consuming it. Used by
    /// the relaxed-CK commit path so the orchestrator can call `retract_row`
    /// against the same instance that produced the original output, then
    /// re-finalize and produce updated rows.
    ///
    /// Cannot fold the spill-recovery path through this entry because
    /// `finalize_with_spill` consumes spill files; the relaxed-CK path
    /// rejects retract on a spilled aggregator (see [`Self::retract_row`])
    /// so this routine only runs on in-memory state.
    pub(crate) fn finalize_in_place(
        &mut self,
        ctx: &EvalContext,
        out: &mut Vec<SortRow>,
    ) -> Result<(), HashAggError> {
        if !self.spill_files.is_empty() {
            return Err(HashAggError::Spill(
                "finalize_in_place called on aggregator with spilled groups".to_string(),
            ));
        }
        if self.buffer_mode {
            for (key, buffered) in self.buffered_groups.iter() {
                // Skip fully-retracted groups for the same reason the
                // fold-mode arm below does: the relaxed-CK retract path
                // can drain a group's contributions to empty, and
                // emitting the identity-folded record would leak a
                // phantom group the user never produced.
                if buffered.input_rows.is_empty() {
                    continue;
                }
                let folded = fold_buffered_state(&self.factory, buffered.clone())?;
                let record = finalize_group_inner(
                    &self.factory,
                    &self.output_schema,
                    &self.transform_name,
                    key,
                    &folded,
                )?;
                let row_num = if folded.min_row_num == u64::MAX {
                    0
                } else {
                    folded.min_row_num
                };
                out.push((record, row_num));
            }
        } else {
            for (key, state) in self.groups.iter() {
                // Skip groups whose lineage was fully retracted: under
                // the relaxed-CK protocol every contributing source row
                // can be removed via `retract_row`, leaving the group's
                // accumulator at its identity element. Emitting that
                // identity row leaks a "phantom" group into the output
                // (e.g. `total=0, n=0`) that the user never produced;
                // skipping aligns with a strict-path baseline that
                // never saw the failing source rows.
                if state.input_rows.is_empty() {
                    continue;
                }
                let record = self.finalize_group(key, state, ctx)?;
                let row_num = if state.min_row_num == u64::MAX {
                    0
                } else {
                    state.min_row_num
                };
                out.push((record, row_num));
            }
        }
        Ok(())
    }

    /// Spill the in-memory groups to disk as postcard + LZ4.
    ///
    /// 1. Drain `self.groups` (fold-mode) or `self.buffered_groups`
    ///    (buffer-mode) into a Vec, wrapping each state in `SpillState`.
    /// 2. Encode sort keys and sort by memcmp bytes so the k-way merge
    ///    can walk entries in group-key order.
    /// 3. Write each `AggSpillEntry` (sort_key + group_key + state) as a
    ///    length-prefixed postcard record into an LZ4 frame-compressed
    ///    temp file. Postcard is a compact binary format with no
    ///    self-describing framing of its own; the explicit length prefix
    ///    delimits records inside the LZ4 frame.
    /// 4. Push the resulting `AggSpillFile` onto `self.spill_files`.
    /// 5. Reset `value_heap_bytes = 0`.
    fn spill(&mut self) -> Result<(), HashAggError> {
        // 1. Drain. Buffer-mode aggregators write `SpillState::Buffered`
        //    entries, fold-mode write `Folded`. Both modes flow through
        //    one writer pipeline so the postcard+LZ4 framing and sort-key
        //    encoding stay shared.
        let drained: Vec<(Vec<GroupByKey>, SpillState)> = if self.buffer_mode {
            self.buffered_groups
                .drain()
                .map(|(k, s)| (k, SpillState::Buffered(s)))
                .collect()
        } else {
            self.groups
                .drain()
                .map(|(k, s)| (k, SpillState::Folded(s)))
                .collect()
        };

        // 2. Encode sort keys via SortKeyEncoder (same encoding the
        //    streaming merge path uses).
        let sort_fields = group_by_sort_fields(&self.group_by_fields, &self.spill_schema);
        let encoder = crate::pipeline::sort_key::SortKeyEncoder::new(sort_fields);

        let gb_count = self.group_by_indices.len();
        let schema_cols = self.spill_schema.column_count();
        let mut prepared: Vec<(Vec<u8>, usize)> = Vec::with_capacity(drained.len());
        for (idx, (key, _state)) in drained.iter().enumerate() {
            let mut values: Vec<Value> = Vec::with_capacity(schema_cols);
            for gk in key {
                values.push(gk.to_value());
            }
            // Pad the non-group-by tail (`__acc_state`, `__meta_tracker`) with
            // Null so the synth record matches the spill schema's column count.
            // `SortKeyEncoder` only reads the group-by prefix; everything past
            // it is encoder-irrelevant but must be present to satisfy
            // `Record::new`'s schema/values length invariant.
            for _ in gb_count..schema_cols {
                values.push(Value::Null);
            }
            let synth = Record::new(Arc::clone(&self.spill_schema), values);
            let mut buf = Vec::new();
            encoder.encode_into(&synth, &mut buf);
            prepared.push((buf, idx));
        }
        prepared.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        // 3. Write sorted entries as postcard + LZ4.
        let mut writer = AggSpillWriter::new(self.spill_dir.as_deref())?;
        for (sort_key, idx) in prepared {
            let (key, state) = &drained[idx];
            let entry = AggSpillEntry {
                sort_key,
                group_key: key.clone(),
                state: state.clone(),
            };
            writer.write_entry(&entry)?;
        }

        let spill_file = writer.finish()?;
        self.spill_files.push(spill_file);

        // 5. All per-group value heap bytes are now off-process.
        self.reset_value_heap_bytes();
        // The flat lineage vec mirrored the in-memory groups; once the
        // groups have been drained, the per-group `input_rows` vectors
        // (already serialized inside each `AggSpillEntry.state`) are the
        // canonical lineage shape. The flat representation is rebuilt at
        // finalize time when a downstream caller asks for it.
        if let Some(lin) = self.lineage.as_mut() {
            lin.clear();
        }
        Ok(())
    }

    /// Drive the aggregator to completion.
    ///
    /// Dispatches across three paths (D2, D11 revised, D12, D13):
    ///
    /// 1. **Global-fold empty-input special case** (D12): no rows
    ///    passed the pre-agg filter, no group-by columns, no spill
    ///    files — emit a single defaulted row via the factory
    ///    prototype so SQL-standard global aggregates over empty
    ///    inputs still produce a result.
    /// 2. **In-memory fast path**: no spill files — drain `self.groups`
    ///    and finalize each group through `finalize_group`.
    /// 3. **Spill recovery path**: flush remaining in-memory state as
    ///    one final spill file, then k-way merge every spill file
    ///    through a `LoserTree`, merging `AccumulatorRow` partials at
    ///    each key boundary, and route each finalized group through the
    ///    same `finalize_group` helper so both paths agree byte-for-byte.
    pub fn finalize(
        mut self,
        ctx: &EvalContext,
        out: &mut Vec<SortRow>,
    ) -> Result<(), HashAggError> {
        // Global-fold empty-input special case. Delegated to the shared
        // `empty_global_fold_row` helper so the streaming path produces
        // a byte-identical record.
        if self.rows_seen == 0 && self.group_by_indices.is_empty() && self.spill_files.is_empty() {
            let record =
                empty_global_fold_row(&self.factory, &self.output_schema, &self.transform_name)?;
            out.push((record, 0));
            return Ok(());
        }

        if self.spill_files.is_empty() {
            if self.buffer_mode {
                // Buffer-mode in-memory fast path: fold each per-group
                // contribution buffer into a fresh accumulator row,
                // then emit through the shared `finalize_group_inner`
                // so the byte-identical guarantee with the fold path
                // holds for the no-retraction baseline.
                let entries: Vec<(Vec<GroupByKey>, BufferedGroupState)> =
                    self.buffered_groups.drain().collect();
                out.reserve(entries.len());
                for (key, buffered) in entries {
                    let state = fold_buffered_state(&self.factory, buffered)?;
                    let record = self.finalize_group(&key, &state, ctx)?;
                    let row_num = if state.min_row_num == u64::MAX {
                        0
                    } else {
                        state.min_row_num
                    };
                    out.push((record, row_num));
                }
            } else {
                let entries: Vec<(Vec<GroupByKey>, AggregatorGroupState)> =
                    self.groups.drain().collect();
                out.reserve(entries.len());
                for (key, state) in entries {
                    let record = self.finalize_group(&key, &state, ctx)?;
                    let row_num = if state.min_row_num == u64::MAX {
                        0
                    } else {
                        state.min_row_num
                    };
                    out.push((record, row_num));
                }
            }
            Ok(())
        } else {
            self.finalize_with_spill(ctx, out)
        }
    }

    /// Finalize one group into an output [`Record`]. Thin wrapper that
    /// delegates to the free [`finalize_group_inner`] so the streaming
    /// aggregator can share byte-identical emission logic.
    fn finalize_group(
        &self,
        key: &[GroupByKey],
        state: &AggregatorGroupState,
        _ctx: &EvalContext,
    ) -> Result<Record, HashAggError> {
        finalize_group_inner(
            &self.factory,
            &self.output_schema,
            &self.transform_name,
            key,
            state,
        )
    }

    /// Spill-recovery finalize path. Flushes remaining in-memory state
    /// as a final spill file, then k-way merges every spill file via a
    /// `LoserTree` keyed on the group-by columns.
    ///
    /// Fold-mode: at every group-key boundary, merges `AccumulatorRow`
    /// partials associatively (commutative and associative across spill
    /// files).
    ///
    /// Buffer-mode: at every group-key boundary, concatenates raw
    /// per-row contributions and sidecars; at boundary close, folds the
    /// accumulated contributions into a fresh accumulator row through
    /// `fold_buffered_state` so emission goes through the same
    /// `finalize_group_inner` as fold-mode (byte-identical output for
    /// the no-retraction baseline).
    fn finalize_with_spill(
        mut self,
        ctx: &EvalContext,
        out: &mut Vec<SortRow>,
    ) -> Result<(), HashAggError> {
        if !self.groups.is_empty() || !self.buffered_groups.is_empty() {
            self.spill()?;
        }

        // Open binary readers over every spill file.
        let mut readers: Vec<AggSpillReader> = self
            .spill_files
            .iter()
            .map(|f| f.reader())
            .collect::<Result<Vec<_>, _>>()?;

        // Prime the loser tree with one entry per reader.
        let mut initial: Vec<Option<AggMergeEntry>> = Vec::with_capacity(readers.len());
        for reader in &mut readers {
            initial.push(reader.next_entry()?);
        }
        let mut tree = LoserTree::new(initial);

        // Direct merge loop: detect group boundaries via sort key bytes,
        // merge accumulator rows + sidecars within each boundary, and
        // finalize when the key changes.
        let factory = &self.factory;
        let output_schema = &self.output_schema;
        let transform_name = &self.transform_name;

        let mut current_key: Option<Vec<u8>> = None;
        let mut current_group_key: Option<Vec<GroupByKey>> = None;
        let mut current_state: Option<SpillState> = None;

        while tree.winner().is_some() {
            let stream_idx = tree.winner_index();
            let winner = tree.winner().unwrap();
            let same_group = current_key.as_ref().is_some_and(|k| k == &winner.sort_key);

            if same_group {
                // Merge into current group, branching on the variant.
                // Cross-variant merges are a planner bug — every spill
                // file from a single aggregator carries one variant
                // exclusively because `buffer_mode` is fixed at
                // construction time.
                match (current_state.as_mut().unwrap(), winner.state.clone()) {
                    (SpillState::Folded(cur), SpillState::Folded(src)) => {
                        for (a, b) in cur.row.iter_mut().zip(src.row.iter()) {
                            AccumulatorEnum::merge(a, b);
                        }
                        let mut src_no_row = src;
                        src_no_row.row.clear();
                        merge_group_sidecars(cur, src_no_row);
                    }
                    (SpillState::Buffered(cur), SpillState::Buffered(src)) => {
                        merge_buffered_sidecars(cur, src);
                    }
                    _ => {
                        return Err(HashAggError::Spill(
                            "spill-merge encountered mixed Folded/Buffered partials \
                             for a single group key — every aggregator emits one \
                             variant exclusively"
                                .to_string(),
                        ));
                    }
                }
            } else {
                // Finalize previous group (if any).
                if let (Some(gk), Some(state)) = (current_group_key.take(), current_state.take()) {
                    let folded = match state {
                        SpillState::Folded(s) => s,
                        SpillState::Buffered(b) => fold_buffered_state(factory, b)?,
                    };
                    let record =
                        finalize_group_inner(factory, output_schema, transform_name, &gk, &folded)?;
                    let row_num = if folded.min_row_num == u64::MAX {
                        0
                    } else {
                        folded.min_row_num
                    };
                    out.push((record, row_num));
                }
                // Start new group from the winner.
                current_key = Some(winner.sort_key.clone());
                current_group_key = Some(winner.group_key.clone());
                current_state = Some(winner.state.clone());
            }

            // Advance the winning stream.
            let next = readers[stream_idx].next_entry()?;
            tree.replace_winner(next);
        }

        // Finalize the last group.
        if let (Some(gk), Some(state)) = (current_group_key.take(), current_state.take()) {
            let folded = match state {
                SpillState::Folded(s) => s,
                SpillState::Buffered(b) => fold_buffered_state(factory, b)?,
            };
            let record =
                finalize_group_inner(factory, output_schema, transform_name, &gk, &folded)?;
            let row_num = if folded.min_row_num == u64::MAX {
                0
            } else {
                folded.min_row_num
            };
            out.push((record, row_num));
        }

        let _ = ctx; // reserved for future per-group eval scope
        Ok(())
    }
}

/// `MemoryConsumer` wrapper for a `HashAggregator`. Holds an
/// `Arc<ConsumerHandle>` shared with the aggregator: the aggregator
/// updates `handle.bytes` as it admits records and drains on spill;
/// the consumer reads the value when the arbitrator polls. `try_spill`
/// flips the handle's spill-request flag, which the aggregator's hot
/// loop reads at the next batch boundary and reacts to by invoking
/// its existing in-thread spill path.
///
/// `spill_priority = 30`: hash-aggregation spill is the most
/// expensive to drive — sort-by-encoded-key, write every group,
/// rebuild via `LoserTree` k-way merge on finalize. Last-resort
/// victim. `can_back_pressure = false`: an in-flight aggregate has
/// no upstream channel to gate; pausing mid-aggregation would either
/// lose accumulation or require additional buffering with no payoff.
pub struct AggregateConsumer {
    handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
}

impl AggregateConsumer {
    pub fn new(handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>) -> Self {
        Self { handle }
    }
}

impl crate::pipeline::memory::MemoryConsumer for AggregateConsumer {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }

    fn spill_priority(&self) -> i32 {
        30
    }

    fn try_spill(
        &self,
        target_bytes: u64,
    ) -> Result<u64, crate::pipeline::memory::ConsumerSpillError> {
        self.handle.request_spill();
        let bytes = self.handle.bytes();
        if bytes >= target_bytes {
            Ok(bytes)
        } else {
            Err(crate::pipeline::memory::ConsumerSpillError::BelowTarget {
                target: target_bytes,
                freed: bytes,
            })
        }
    }

    fn can_back_pressure(&self) -> bool {
        false
    }
}

/// Finalize one group into an output [`Record`]. Shared by the in-memory
/// fast path, the spill-recovery path, and `StreamingAggregator<AddRaw>`
/// so all three produce byte-identical results.
///
/// 1. Finalize each accumulator in `state.row` into a slot vector.
/// 2. Evaluate every compiled emit residual in an [`AggregateEvalScope`]
///    and populate `values` by output-schema column name.
/// 3. Build the output `Record`.
pub(crate) fn finalize_group_inner(
    factory: &AccumulatorFactory,
    output_schema: &Arc<Schema>,
    transform_name: &str,
    key: &[GroupByKey],
    state: &AggregatorGroupState,
) -> Result<Record, HashAggError> {
    let bindings = factory.bindings();
    let mut slots: Vec<Value> = Vec::with_capacity(state.row.len());
    for (i, acc) in state.row.iter().enumerate() {
        let v = acc.finalize().map_err(|e| HashAggError::Accumulator {
            transform: transform_name.to_string(),
            binding: bindings[i].output_name.to_string(),
            source: e,
        })?;
        slots.push(v);
    }

    let scope = AggregateEvalScope { key, slots: &slots };
    let compiled = factory.compiled();
    let mut values: Vec<Value> = vec![Value::Null; output_schema.column_count()];
    // Stamp every group-by column from the group key tuple. The CXL
    // parser blocks `emit $ck.* = ...` so engine-stamped group-by
    // columns (`$ck.<field>` shadow columns) have no covering emit
    // and would otherwise leave the slot at `Value::Null`. User-emit
    // statements still run after this loop and override the slot for
    // any group-by column they cover, so explicit `emit X = X` writes
    // win on collision.
    for (key_idx, gb_name) in compiled.group_by_fields.iter().enumerate() {
        if let Some(out_idx) = output_schema.index(gb_name)
            && let Some(gk) = key.get(key_idx)
        {
            values[out_idx] = gk.to_value();
        }
    }
    for emit in &compiled.emits {
        let v = eval_expr_in_agg_scope(&emit.residual, &scope).map_err(HashAggError::Residual)?;
        if let Some(idx) = output_schema.index(&emit.output_name) {
            values[idx] = v;
        }
    }
    // Stamp the synthetic aggregate-group-index column for relaxed
    // aggregates. Strict aggregates have no such column on their
    // `output_schema` (the schema-widening pass only appends one for
    // relaxed aggregates), so the lookup short-circuits and the strict
    // path pays zero overhead. The CXL parser blocks `emit $ck.* = ...`
    // so no user-emit collides for the slot; the write order is still
    // chosen so any future engine emit cannot overwrite the synthetic
    // value silently.
    let synthetic_ck = format!("$ck.aggregate.{transform_name}");
    if let Some(idx) = output_schema.index(&synthetic_ck) {
        values[idx] = Value::Integer(state.group_index as i64);
    }

    let record = Record::new(Arc::clone(output_schema), values);
    Ok(record)
}

/// Single source of truth for the `Vec<SortField>` configuration used
/// to encode group-by columns for the streaming aggregator and the
/// spill write/read paths.
///
/// Returns one ASC nulls-first `SortField` per group-by column. The
/// schema parameter is currently unused but is plumbed so a future
/// type-aware encoding (e.g. dict/binary) can read column types
/// without changing the call sites.
pub(crate) fn group_by_sort_fields(group_by_fields: &[String], _schema: &Schema) -> Vec<SortField> {
    group_by_fields
        .iter()
        .map(|name| SortField {
            field: name.clone(),
            order: SortOrder::Asc,
            null_order: Some(NullOrder::First),
        })
        .collect()
}

/// Single source of truth for the empty-input global-fold record.
///
/// Both the hash path (`HashAggregator::finalize`) and the streaming
/// path (`StreamingAggregator::flush`) call this to produce one
/// defaulted output record when an empty stream and an empty group-by
/// would otherwise yield zero rows. Mirrors DataFusion's
/// `AggregateStream` empty-input branch.
pub(crate) fn empty_global_fold_row(
    factory: &AccumulatorFactory,
    output_schema: &Arc<Schema>,
    transform_name: &str,
) -> Result<Record, HashAggError> {
    let empty_key: Vec<GroupByKey> = Vec::new();
    let state = AggregatorGroupState::new(factory.create_accumulators());
    finalize_group_inner(factory, output_schema, transform_name, &empty_key, &state)
}

#[cfg(test)]
mod spill_trigger_tests {
    use super::*;
    use cxl::eval::{EvalContext, ProgramEvaluator, StableEvalContext};
    use cxl::parser::Parser;
    use cxl::plan::extract_aggregates;
    use cxl::resolve::pass::resolve_program;
    use cxl::typecheck::Row;
    use cxl::typecheck::pass::{AggregateMode, type_check_with_mode};
    use cxl::typecheck::types::Type;
    use indexmap::IndexMap;

    fn make_schema(cols: &[&str]) -> Arc<Schema> {
        Arc::new(Schema::new(cols.iter().map(|c| (*c).into()).collect()))
    }

    fn make_record(schema: &Arc<Schema>, vals: Vec<Value>) -> Record {
        Record::new(Arc::clone(schema), vals)
    }

    /// Compile a CXL aggregate snippet against input_fields, returning a
    /// fully initialized HashAggregator ready for `add_record` calls.
    fn build_test_aggregator(
        input_fields: &[(&str, Type)],
        group_by: &[&str],
        cxl_src: &str,
        memory_budget: usize,
        spill_dir: Option<std::path::PathBuf>,
    ) -> HashAggregator {
        build_test_aggregator_relaxed(
            input_fields,
            group_by,
            cxl_src,
            memory_budget,
            spill_dir,
            false,
        )
    }

    /// Like `build_test_aggregator` but configures retraction-strategy
    /// flags on the extracted `CompiledAggregate` as if the planner had
    /// classified this aggregate as relaxed (lineage for all-Reversible
    /// bindings, buffer-mode for any BufferRequired binding). Tests
    /// pass `is_relaxed = true` to exercise the retraction paths
    /// directly; the runtime planner makes the same call once it
    /// determines an aggregate's `group_by` omits a correlation-key
    /// field.
    fn build_test_aggregator_relaxed(
        input_fields: &[(&str, Type)],
        group_by: &[&str],
        cxl_src: &str,
        memory_budget: usize,
        spill_dir: Option<std::path::PathBuf>,
        is_relaxed: bool,
    ) -> HashAggregator {
        let parsed = Parser::parse(cxl_src);
        assert!(
            parsed.errors.is_empty(),
            "parse errors: {:?}",
            parsed.errors
        );
        let field_names: Vec<&str> = input_fields.iter().map(|(n, _)| *n).collect();
        let resolved =
            resolve_program(parsed.ast, &field_names, parsed.node_count).expect("resolve");
        let schema_map: IndexMap<cxl::typecheck::QualifiedField, Type> = input_fields
            .iter()
            .map(|(n, t)| (cxl::typecheck::QualifiedField::bare(*n), t.clone()))
            .collect();
        let row = Row::closed(schema_map, cxl::lexer::Span::new(0, 0));
        let mode = AggregateMode::GroupBy {
            group_by_fields: group_by.iter().map(|s| (*s).to_string()).collect(),
        };
        let typed = type_check_with_mode(resolved, &row, mode).expect("typecheck");
        let schema_names: Vec<String> =
            input_fields.iter().map(|(n, _)| (*n).to_string()).collect();
        let group_by_owned: Vec<String> = group_by.iter().map(|s| (*s).to_string()).collect();
        let mut compiled =
            extract_aggregates(&typed, &group_by_owned, &schema_names).expect("extract_aggregates");
        // Mirror the planner: relaxed aggregates flip exactly one of
        // the two retraction-strategy flags. Tests that want the strict
        // (zero-overhead) path leave `is_relaxed = false` and the
        // constructor sees both flags as `false`.
        compiled.set_retraction_flags(is_relaxed);

        let output_columns: Vec<Box<str>> = compiled
            .emits
            .iter()
            .map(|e| e.output_name.clone())
            .collect();
        let output_schema = Arc::new(Schema::new(output_columns));

        let mut spill_cols: Vec<Box<str>> = group_by_owned
            .iter()
            .map(|s| Box::<str>::from(s.as_str()))
            .collect();
        spill_cols.push("__acc_state".into());
        let spill_schema = Arc::new(Schema::new(spill_cols));

        let evaluator = ProgramEvaluator::new(Arc::new(typed), false);

        HashAggregator::new(
            Arc::new(compiled),
            evaluator,
            output_schema,
            spill_schema,
            memory_budget,
            spill_dir,
            "test_agg",
            crate::pipeline::memory::ConsumerHandle::new(),
        )
    }

    fn ctx_for<'a>(stable: &'a StableEvalContext, file: &'a Arc<str>, row: u64) -> EvalContext<'a> {
        EvalContext::test_with_file(stable, file, row)
    }

    // ------------------------------------------------------------------
    // estimated_bytes_per_group sanity
    // ------------------------------------------------------------------

    #[test]
    fn test_estimated_bytes_positive() {
        let agg = build_test_aggregator(
            &[("k", Type::String)],
            &["k"],
            "emit k = k\nemit n = count(*)",
            512 * 1024 * 1024,
            None,
        );
        assert!(
            agg.max_groups() > 0 && agg.max_groups() < usize::MAX,
            "max_groups should be a finite positive value, got {}",
            agg.max_groups(),
        );
    }

    // ------------------------------------------------------------------
    // Spill trigger fires on group count
    // ------------------------------------------------------------------

    #[test]
    fn test_spill_fires_at_max_groups() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let input = make_schema(&["k"]);
        let mut agg = build_test_aggregator(
            &[("k", Type::String)],
            &["k"],
            "emit k = k\nemit n = count(*)",
            10_000, // 10KB budget — tiny
            Some(tmp.path().to_path_buf()),
        );
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        for i in 0..500u64 {
            let r = make_record(&input, vec![Value::String(format!("key_{i}").into())]);
            agg.add_record(&r, i, &ctx_for(&stable, &file, i))
                .expect("add_record");
        }
        assert!(
            !agg.spill_files().is_empty(),
            "tiny budget + many unique keys must trigger at least one spill"
        );
    }

    // ------------------------------------------------------------------
    // No spill within generous budget
    // ------------------------------------------------------------------

    #[test]
    fn test_no_spill_within_budget() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let input = make_schema(&["k"]);
        let mut agg = build_test_aggregator(
            &[("k", Type::String)],
            &["k"],
            "emit k = k\nemit n = count(*)",
            10_000_000, // 10MB — plenty for 10 groups
            Some(tmp.path().to_path_buf()),
        );
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        for i in 0..10u64 {
            let r = make_record(&input, vec![Value::String(format!("key_{i}").into())]);
            agg.add_record(&r, i, &ctx_for(&stable, &file, i)).unwrap();
        }
        assert!(
            agg.spill_files().is_empty(),
            "generous budget + few keys should not trigger spill"
        );
    }

    // ------------------------------------------------------------------
    // Spill-merge correctness: 4 keys × 16 records
    // ------------------------------------------------------------------

    #[test]
    fn test_spill_merge_correctness() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let input = make_schema(&["k"]);
        let mut agg = build_test_aggregator(
            &[("k", Type::String)],
            &["k"],
            "emit k = k\nemit n = count(*)",
            1024,
            Some(tmp.path().to_path_buf()),
        );
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        let keys = ["a", "b", "c", "d"];
        for i in 0..64u64 {
            let k = keys[(i as usize) % keys.len()];
            let r = make_record(&input, vec![Value::String(k.into())]);
            agg.add_record(&r, i, &ctx_for(&stable, &file, i)).unwrap();
        }
        assert!(
            !agg.spill_files().is_empty(),
            "expected at least one spill under tiny budget"
        );

        let ctx = ctx_for(&stable, &file, 0);
        let mut out: Vec<SortRow> = Vec::new();
        agg.finalize(&ctx, &mut out).expect("finalize after spill");
        assert_eq!(out.len(), 4, "four distinct groups after spill-merge");
        for (rec, _row_num) in &out {
            assert_eq!(
                rec.values()[1],
                Value::Integer(16),
                "per-group count(*) preserved through spill merge"
            );
        }
    }

    // ------------------------------------------------------------------
    // Multi-spill merge: 8 keys × 32 records, >= 2 spill files
    // ------------------------------------------------------------------

    #[test]
    fn test_multi_spill_merge() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let input = make_schema(&["k"]);
        let mut agg = build_test_aggregator(
            &[("k", Type::String)],
            &["k"],
            "emit k = k\nemit n = count(*)",
            512,
            Some(tmp.path().to_path_buf()),
        );
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        let keys = ["a", "b", "c", "d", "e", "f", "g", "h"];
        for i in 0..256u64 {
            let k = keys[(i as usize) % keys.len()];
            let r = make_record(&input, vec![Value::String(k.into())]);
            agg.add_record(&r, i, &ctx_for(&stable, &file, i)).unwrap();
        }
        assert!(
            agg.spill_files().len() >= 2,
            "expected multiple spill files, got {}",
            agg.spill_files().len()
        );

        let ctx = ctx_for(&stable, &file, 0);
        let mut out: Vec<SortRow> = Vec::new();
        agg.finalize(&ctx, &mut out)
            .expect("multi-spill finalize merges correctly");
        assert_eq!(out.len(), 8, "eight distinct groups after k-way merge");
        for (rec, _) in &out {
            assert_eq!(
                rec.values()[1],
                Value::Integer(32),
                "32 records per group survive multi-round merge"
            );
        }
    }

    // ------------------------------------------------------------------
    // RSS backstop fires exclusively — neither the group-count nor
    // value-heap thresholds are reachable under the configured budget
    // and workload, so the only path that can spill is the RSS check.
    // ------------------------------------------------------------------

    #[test]
    fn test_rss_backstop_fires_exclusively() {
        if crate::pipeline::memory::rss_bytes().is_none() {
            return; // Skip on unsupported platforms
        }
        let tmp = tempfile::tempdir().expect("tempdir");
        let input = make_schema(&["k"]);
        // 1 MB budget:
        //   - max_groups resolves to hundreds (>> the 10 unique keys below),
        //     so the primary group-count threshold cannot fire.
        //   - value_heap_bytes stays ~0 (count(*) is i64-per-group),
        //     so the 40% value-heap threshold cannot fire.
        //   - RSS of any real test process is tens of MB >> 85% of 1 MB,
        //     so the RSS backstop is the only path that can spill.
        const BUDGET: usize = 1024 * 1024;
        let mut agg = build_test_aggregator(
            &[("k", Type::String)],
            &["k"],
            "emit k = k\nemit n = count(*)",
            BUDGET,
            Some(tmp.path().to_path_buf()),
        );
        // Precondition assertions make the test self-documenting about
        // which branch it claims to exercise.
        assert!(
            agg.max_groups() >= 100,
            "precondition: max_groups={} must dwarf the 10-key workload \
             (otherwise the primary group-count threshold fires and this \
             test is not exercising the RSS backstop)",
            agg.max_groups()
        );

        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        // Feed > 4096 records across only 10 unique keys so the group-count
        // primary threshold stays inert (groups.len() plateaus at 10).
        for i in 0..5_000u64 {
            let key = format!("k{}", i % 10);
            let r = make_record(&input, vec![Value::String(key.into())]);
            agg.add_record(&r, i, &ctx_for(&stable, &file, i))
                .expect("add_record");
        }
        assert!(
            agg.value_heap_bytes() < BUDGET * 40 / 100,
            "precondition: value_heap_bytes={} must stay under 40% of budget \
             (otherwise the secondary value-heap threshold fires and this \
             test is not exercising the RSS backstop)",
            agg.value_heap_bytes()
        );
        assert!(
            !agg.spill_files().is_empty(),
            "RSS backstop should spill when process RSS exceeds 85% of a 1 MB budget"
        );
    }

    // ------------------------------------------------------------------
    // Lineage path: allocation gate, recording correctness, spill round
    // trip. The runtime retract path that consumes lineage is not yet
    // wired; these tests pin the observable shape that retract will
    // depend on (which rows mapped to which groups, byte-exact across
    // a spill round trip).
    // ------------------------------------------------------------------

    #[test]
    fn test_lineage_disabled_is_none() {
        // Strict mode (the default): `requires_lineage` stays `false`,
        // the `lineage` field is `None`, and a few records leave the
        // accumulator with the same memory shape it had before this
        // commit landed.
        let input = make_schema(&["k"]);
        let mut agg = build_test_aggregator(
            &[("k", Type::String)],
            &["k"],
            "emit k = k\nemit n = count(*)",
            10_000_000,
            None,
        );
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        for i in 0..5u64 {
            let r = make_record(&input, vec![Value::String(format!("k{}", i % 2).into())]);
            agg.add_record(&r, i, &ctx_for(&stable, &file, i)).unwrap();
        }
        assert!(
            agg.lineage.is_none(),
            "strict aggregates must not allocate the lineage map"
        );
        for state in agg.groups().values() {
            assert!(
                state.input_rows.is_empty(),
                "strict aggregates must not populate per-group input_rows"
            );
        }
    }

    #[test]
    fn test_lineage_recording_pairs_rows_with_groups() {
        // 5 records across 2 groups (alternating); flat lineage must
        // record every row, and the two groups' indices must be
        // distinct. The actual u32 values for `group_index` are an
        // implementation detail (insertion order); the test asserts
        // structural invariants that the retract path will rely on.
        let input = make_schema(&["k"]);
        let mut agg = build_test_aggregator_relaxed(
            &[("k", Type::String)],
            &["k"],
            "emit k = k\nemit n = count(*)",
            10_000_000,
            None,
            true,
        );
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        let keys = ["a", "b", "a", "b", "a"];
        for (i, k) in keys.iter().enumerate() {
            let r = make_record(&input, vec![Value::String((*k).into())]);
            agg.add_record(&r, i as u64, &ctx_for(&stable, &file, i as u64))
                .unwrap();
        }
        let lineage = agg.lineage.as_ref().expect("lineage allocated");
        assert_eq!(lineage.len(), 5, "every record must produce one entry");
        // Row numbers must be the input order 0..5.
        let rows: Vec<u64> = lineage.iter().map(|(r, _)| *r).collect();
        assert_eq!(rows, vec![0, 1, 2, 3, 4]);
        // Two distinct group indices, alternating with the input keys.
        let g0 = lineage[0].1;
        let g1 = lineage[1].1;
        assert_ne!(
            g0, g1,
            "two distinct keys must yield two distinct group indices"
        );
        assert_eq!(lineage[2].1, g0, "key 'a' must route to its first group");
        assert_eq!(lineage[3].1, g1, "key 'b' must route to its first group");
        assert_eq!(lineage[4].1, g0, "key 'a' must route to its first group");

        // Per-group input_rows mirror the flat lineage partition.
        let groups = agg.groups();
        assert_eq!(groups.len(), 2);
        let mut input_row_lists: Vec<Vec<u64>> = groups
            .values()
            .map(|s| s.input_rows.iter().map(|(r, _)| *r).collect())
            .collect();
        input_row_lists.sort();
        assert_eq!(input_row_lists, vec![vec![0, 2, 4], vec![1, 3]]);
    }

    #[test]
    fn test_lineage_round_trips_through_spill() {
        // Force at least one spill, then finalize. The spill clears the
        // flat lineage vec but the per-group `input_rows` ride along
        // with each `AggSpillEntry` and survive the merge — both as
        // `merge_group_sidecars` concatenations across spill files and
        // as the original lists for groups that never had to merge.
        let tmp = tempfile::tempdir().expect("tempdir");
        let input = make_schema(&["k"]);
        let mut agg = build_test_aggregator_relaxed(
            &[("k", Type::String)],
            &["k"],
            "emit k = k\nemit n = count(*)",
            1024,
            Some(tmp.path().to_path_buf()),
            true,
        );
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        let keys = ["a", "b", "c", "d"];
        for i in 0..32u64 {
            let k = keys[(i as usize) % keys.len()];
            let r = make_record(&input, vec![Value::String(k.into())]);
            agg.add_record(&r, i, &ctx_for(&stable, &file, i)).unwrap();
        }
        assert!(
            !agg.spill_files().is_empty(),
            "tiny budget must trigger at least one spill"
        );

        // Walk every spill file's entries directly, verifying that
        // `input_rows` round-trips byte-for-byte through postcard+LZ4.
        let mut by_key: std::collections::HashMap<String, Vec<u64>> =
            std::collections::HashMap::new();
        for f in agg.spill_files() {
            let mut reader = f.reader().expect("open spill reader");
            while let Some(entry) = reader.next_entry().expect("read entry") {
                let key_str = match &entry.group_key[0] {
                    GroupByKey::Str(s) => s.to_string(),
                    other => panic!("unexpected group key shape: {other:?}"),
                };
                let folded = match &entry.state {
                    SpillState::Folded(s) => s,
                    SpillState::Buffered(_) => panic!(
                        "lineage path must produce SpillState::Folded entries; \
                         buffer-mode is the complementary retraction strategy"
                    ),
                };
                by_key
                    .entry(key_str)
                    .or_default()
                    .extend(folded.input_rows.iter().map(|(r, _)| *r));
            }
        }
        // Drain in-memory groups too — anything spill missed.
        for (k, state) in agg.groups() {
            let key_str = match &k[0] {
                GroupByKey::Str(s) => s.to_string(),
                other => panic!("unexpected group key shape: {other:?}"),
            };
            by_key
                .entry(key_str)
                .or_default()
                .extend(state.input_rows.iter().map(|(r, _)| *r));
        }

        for (k, mut rows) in by_key.into_iter() {
            rows.sort();
            // Recover the expected row indices for this key from the
            // input pattern: positions where `keys[i % 4] == k`.
            let key_idx = keys.iter().position(|kk| *kk == k).expect("known key");
            let expected: Vec<u64> = (0..32u64)
                .filter(|i| (*i as usize) % 4 == key_idx)
                .collect();
            assert_eq!(
                rows, expected,
                "lineage for key {k:?} must survive spill round-trip exactly"
            );
        }
    }

    // ------------------------------------------------------------------
    // Buffer-mode aggregator path
    //
    // The buffer-mode path is the complement of the lineage path under
    // relaxed-correlation-key semantics: when at least one binding is
    // BufferRequired (Min/Max/Avg/WeightedAvg), the aggregator holds
    // raw per-row contributions instead of folded accumulator state so
    // the rollback step can recompute affected groups from
    // `contributions − retracted_rows`. The fold-mode path stays the
    // default for strict aggregates and pays zero overhead.
    // ------------------------------------------------------------------

    #[test]
    fn test_mode_selection_relaxed_min_only_selects_buffer_mode() {
        // A single BufferRequired binding (`min`) under relaxed-CK opt-in
        // must route the whole aggregate to buffer-mode.
        let agg = build_test_aggregator_relaxed(
            &[("k", Type::String), ("v", Type::Int)],
            &["k"],
            "emit k = k\nemit lo = min(v)",
            10_000_000,
            None,
            true,
        );
        assert!(agg.buffer_mode, "relaxed min(v) must select buffer-mode");
        assert!(
            agg.lineage.is_none(),
            "buffer-mode must not allocate the lineage map"
        );
    }

    #[test]
    fn test_mode_selection_relaxed_sum_only_selects_lineage_mode() {
        // Pure-Reversible bindings under relaxed-CK opt-in stay on the
        // lineage (fold) path.
        let agg = build_test_aggregator_relaxed(
            &[("k", Type::String), ("v", Type::Int)],
            &["k"],
            "emit k = k\nemit total = sum(v)",
            10_000_000,
            None,
            true,
        );
        assert!(
            !agg.buffer_mode,
            "all-Reversible must not select buffer-mode"
        );
        assert!(
            agg.lineage.is_some(),
            "all-Reversible relaxed must allocate lineage"
        );
    }

    #[test]
    fn test_mode_selection_relaxed_mixed_sum_and_min_selects_buffer_mode() {
        // Mixed bindings (Sum + Min): the BufferRequired binding alone
        // is enough to flip the aggregate to buffer-mode.
        let agg = build_test_aggregator_relaxed(
            &[("k", Type::String), ("v", Type::Int)],
            &["k"],
            "emit k = k\nemit total = sum(v)\nemit lo = min(v)",
            10_000_000,
            None,
            true,
        );
        assert!(agg.buffer_mode);
        assert!(agg.lineage.is_none());
    }

    #[test]
    fn test_mode_selection_strict_min_stays_on_fold_path() {
        // Strict (non-relaxed) aggregates pay zero retraction overhead
        // regardless of binding shape.
        let agg = build_test_aggregator_relaxed(
            &[("k", Type::String), ("v", Type::Int)],
            &["k"],
            "emit k = k\nemit lo = min(v)",
            10_000_000,
            None,
            false,
        );
        assert!(!agg.buffer_mode);
        assert!(agg.lineage.is_none());
    }

    #[test]
    fn test_buffer_mode_records_per_row_contributions() {
        // Two BufferRequired bindings (`min(v)` and `max(v)`) — every
        // ingested row appends one entry whose two-Value inner vec
        // matches the binding-slot order.
        let input = make_schema(&["k", "v"]);
        let mut agg = build_test_aggregator_relaxed(
            &[("k", Type::String), ("v", Type::Int)],
            &["k"],
            "emit k = k\nemit lo = min(v)\nemit hi = max(v)",
            10_000_000,
            None,
            true,
        );
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        // Two groups, three rows each — interleaved so per-group
        // contributions land in input-row order.
        let inputs: &[(&str, i64)] = &[
            ("a", 10),
            ("b", 100),
            ("a", 20),
            ("b", 200),
            ("a", 30),
            ("b", 300),
        ];
        for (i, (k, v)) in inputs.iter().enumerate() {
            let r = make_record(&input, vec![Value::String((*k).into()), Value::Integer(*v)]);
            agg.add_record(&r, i as u64, &ctx_for(&stable, &file, i as u64))
                .unwrap();
        }
        assert!(
            agg.groups.is_empty(),
            "buffer-mode must not populate the fold-path map"
        );
        assert_eq!(agg.buffered_groups.len(), 2, "two distinct keys");
        // Each per-group state must carry exactly three rows of the
        // input pattern, with the inner Vec<Value> matching the
        // binding evaluation order (min(v) at slot 0, max(v) at slot 1
        // — both reading the same column, so each row's inner vec is
        // `[v, v]`).
        let mut by_key: std::collections::HashMap<String, Vec<Vec<Value>>> =
            std::collections::HashMap::new();
        for (k, state) in &agg.buffered_groups {
            let key_str = match &k[0] {
                GroupByKey::Str(s) => s.to_string(),
                other => panic!("unexpected group key shape: {other:?}"),
            };
            by_key.insert(key_str, state.contributions.clone());
        }
        let a = by_key.remove("a").expect("group a");
        let b = by_key.remove("b").expect("group b");
        assert_eq!(
            a,
            vec![
                vec![Value::Integer(10), Value::Integer(10)],
                vec![Value::Integer(20), Value::Integer(20)],
                vec![Value::Integer(30), Value::Integer(30)],
            ]
        );
        assert_eq!(
            b,
            vec![
                vec![Value::Integer(100), Value::Integer(100)],
                vec![Value::Integer(200), Value::Integer(200)],
                vec![Value::Integer(300), Value::Integer(300)],
            ]
        );
        // Per-group input_rows mirror the lineage path's shape so
        // the rollback step indexes into `contributions` by row.
        let mut a_rows: Vec<u64> = agg
            .buffered_groups
            .iter()
            .find(|(k, _)| matches!(&k[0], GroupByKey::Str(s) if s.as_ref() == "a"))
            .map(|(_, s)| s.input_rows.iter().map(|(r, _)| *r).collect())
            .unwrap();
        a_rows.sort();
        assert_eq!(a_rows, vec![0, 2, 4]);
    }

    #[test]
    fn test_buffer_mode_value_heap_charges_string_binding() {
        // String-typed binding: every per-row charge folds the String's
        // length into `value_heap_bytes` so the soft-spill threshold
        // sees buffer-mode growth without a separate budget knob.
        let input = make_schema(&["k", "s"]);
        let mut agg = build_test_aggregator_relaxed(
            &[("k", Type::String), ("s", Type::String)],
            &["k"],
            "emit k = k\nemit lo = min(s)",
            10_000_000,
            None,
            true,
        );
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        let baseline = agg.value_heap_bytes();

        let payload = "abcdefghij"; // 10 bytes of String heap
        let r = make_record(
            &input,
            vec![Value::String("k1".into()), Value::String(payload.into())],
        );
        agg.add_record(&r, 0, &ctx_for(&stable, &file, 0)).unwrap();

        let after = agg.value_heap_bytes();
        let delta = after - baseline;
        // Every charge must include the String's heap bytes; tighter
        // structural sizes (Vec<Value>, enum-tag) ride on top.
        assert!(
            delta >= payload.len(),
            "value_heap_bytes delta {delta} must include the {} bytes of String heap",
            payload.len()
        );
        assert!(
            delta >= payload.len() + std::mem::size_of::<Value>(),
            "value_heap_bytes delta {delta} must include the Value enum tag for the slot"
        );
    }

    #[test]
    fn test_buffer_mode_spill_round_trip_preserves_contributions() {
        // Force at least one spill in buffer mode, then walk every
        // spill file's entries directly — `BufferedGroupState` must
        // round-trip byte-identical through postcard+LZ4.
        let tmp = tempfile::tempdir().expect("tempdir");
        let input = make_schema(&["k", "v"]);
        let mut agg = build_test_aggregator_relaxed(
            &[("k", Type::String), ("v", Type::Int)],
            &["k"],
            "emit k = k\nemit lo = min(v)",
            1024,
            Some(tmp.path().to_path_buf()),
            true,
        );
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        let keys = ["a", "b", "c", "d"];
        for i in 0..32u64 {
            let k = keys[(i as usize) % keys.len()];
            let v = i as i64;
            let r = make_record(&input, vec![Value::String(k.into()), Value::Integer(v)]);
            agg.add_record(&r, i, &ctx_for(&stable, &file, i)).unwrap();
        }
        assert!(
            !agg.spill_files().is_empty(),
            "tiny budget must trigger at least one spill"
        );

        // Reassemble per-group contributions from every spill file plus
        // any in-memory remainder, then compare to the deterministic
        // input pattern.
        let mut by_key: std::collections::HashMap<String, Vec<(u64, i64)>> =
            std::collections::HashMap::new();
        for f in agg.spill_files() {
            let mut reader = f.reader().expect("open spill reader");
            while let Some(entry) = reader.next_entry().expect("read entry") {
                let key_str = match &entry.group_key[0] {
                    GroupByKey::Str(s) => s.to_string(),
                    other => panic!("unexpected group key shape: {other:?}"),
                };
                let buffered = match entry.state {
                    SpillState::Buffered(b) => b,
                    SpillState::Folded(_) => {
                        panic!("buffer-mode spill must produce SpillState::Buffered entries")
                    }
                };
                let bucket = by_key.entry(key_str).or_default();
                for ((row, _src), contrib) in buffered
                    .input_rows
                    .iter()
                    .zip(buffered.contributions.iter())
                {
                    let v = match contrib.first().expect("one slot for min(v)") {
                        Value::Integer(n) => *n,
                        other => panic!("expected Integer, got {other:?}"),
                    };
                    bucket.push((*row, v));
                }
            }
        }
        for (k, state) in &agg.buffered_groups {
            let key_str = match &k[0] {
                GroupByKey::Str(s) => s.to_string(),
                other => panic!("unexpected group key shape: {other:?}"),
            };
            let bucket = by_key.entry(key_str).or_default();
            for ((row, _src), contrib) in state.input_rows.iter().zip(state.contributions.iter()) {
                let v = match contrib.first().expect("one slot for min(v)") {
                    Value::Integer(n) => *n,
                    other => panic!("expected Integer, got {other:?}"),
                };
                bucket.push((*row, v));
            }
        }

        for (k, mut rows) in by_key.into_iter() {
            rows.sort_by_key(|(r, _)| *r);
            let key_idx = keys.iter().position(|kk| *kk == k).expect("known key");
            let expected: Vec<(u64, i64)> = (0..32u64)
                .filter(|i| (*i as usize) % 4 == key_idx)
                .map(|i| (i, i as i64))
                .collect();
            assert_eq!(
                rows, expected,
                "buffer-mode contributions for key {k:?} must survive spill round-trip exactly"
            );
        }
    }

    #[test]
    fn test_buffer_mode_finalize_after_spill_matches_fold_baseline() {
        // Finalize after spill in buffer-mode must produce the same
        // per-group aggregates the fold path produces over identical
        // input — the no-retraction baseline is byte-equivalent.
        let tmp = tempfile::tempdir().expect("tempdir");
        let input = make_schema(&["k", "v"]);
        let mut buf_agg = build_test_aggregator_relaxed(
            &[("k", Type::String), ("v", Type::Int)],
            &["k"],
            "emit k = k\nemit lo = min(v)",
            1024,
            Some(tmp.path().to_path_buf()),
            true,
        );
        let mut fold_agg = build_test_aggregator(
            &[("k", Type::String), ("v", Type::Int)],
            &["k"],
            "emit k = k\nemit lo = min(v)",
            10_000_000,
            None,
        );
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        let keys = ["a", "b", "c", "d"];
        for i in 0..32u64 {
            let k = keys[(i as usize) % keys.len()];
            let v = i as i64;
            let r = make_record(&input, vec![Value::String(k.into()), Value::Integer(v)]);
            buf_agg
                .add_record(&r, i, &ctx_for(&stable, &file, i))
                .unwrap();
            fold_agg
                .add_record(&r, i, &ctx_for(&stable, &file, i))
                .unwrap();
        }
        assert!(
            !buf_agg.spill_files().is_empty(),
            "tiny budget must trigger at least one spill"
        );
        let ctx = ctx_for(&stable, &file, 0);
        let mut buf_out: Vec<SortRow> = Vec::new();
        buf_agg.finalize(&ctx, &mut buf_out).expect("buf finalize");
        let mut fold_out: Vec<SortRow> = Vec::new();
        fold_agg
            .finalize(&ctx, &mut fold_out)
            .expect("fold finalize");
        // Compare key->min(v) maps; row_num ordering through spill is
        // sort-stable but row_num may differ because the fold path
        // never spilled. Aggregate values are what the gate enforces.
        let project = |rows: Vec<SortRow>| -> std::collections::BTreeMap<String, i64> {
            rows.into_iter()
                .map(|(rec, _)| {
                    let vals = rec.values();
                    let k = match &vals[0] {
                        Value::String(s) => s.to_string(),
                        other => panic!("expected key string, got {other:?}"),
                    };
                    let v = match &vals[1] {
                        Value::Integer(n) => *n,
                        other => panic!("expected min Integer, got {other:?}"),
                    };
                    (k, v)
                })
                .collect()
        };
        assert_eq!(project(buf_out), project(fold_out));
    }

    #[test]
    #[should_panic(expected = "buffered contributions exceeded hard limit")]
    fn test_buffer_mode_panics_when_single_row_exceeds_budget() {
        // Hard-limit guard. A single record's contributions exceeding
        // the entire memory budget is the unaffordable case — spilling
        // cannot help because the just-charged row is already over the
        // hard limit and the next add of the same shape repeats the
        // overflow. This test exercises the dedicated panic path; the
        // `#[should_panic]` is the consumer of the runtime guard, not a
        // demotion of a previously asserting test.
        let input = make_schema(&["k", "s"]);
        // Budget intentionally tiny; single record String alone exceeds it.
        let mut agg = build_test_aggregator_relaxed(
            &[("k", Type::String), ("s", Type::String)],
            &["k"],
            "emit k = k\nemit lo = min(s)",
            16,
            None,
            true,
        );
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        // A 2 KiB string blows past the 16-byte budget.
        let big = "x".repeat(2048);
        let r = make_record(
            &input,
            vec![Value::String("k1".into()), Value::String(big.into())],
        );
        // The first add_record triggers a soft spill (budget > 0 and
        // value_heap_bytes far exceeds 40% of 16) which drains. Spill
        // does not actually rescue because the just-charged row's
        // contribution already exceeded the entire budget; the
        // post-spill hard-limit guard fires the panic.
        let _ = agg.add_record(&r, 0, &ctx_for(&stable, &file, 0));
    }

    // ----------------------------------------------------------------
    // Buffer-mode retract round-trip — feed N, retract M, finalize_in_place
    // equals feed-(N-M)-from-scratch byte-identically. One test per
    // BufferRequired variant: Min, Max, Avg, WeightedAvg.
    // ----------------------------------------------------------------

    fn run_with_retract<F>(
        cxl: &str,
        input_fields: &[(&str, Type)],
        group_by: &[&str],
        rows: &[(Vec<Value>, u64)],
        retract: &[u64],
        feed_subset: F,
    ) -> (Vec<Record>, Vec<Record>)
    where
        F: Fn(&[(Vec<Value>, u64)]) -> Vec<(Vec<Value>, u64)>,
    {
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        let input = make_schema(&input_fields.iter().map(|(n, _)| *n).collect::<Vec<_>>());

        let mut full_agg = build_test_aggregator_relaxed(
            input_fields,
            group_by,
            cxl,
            10 * 1024 * 1024,
            None,
            true,
        );
        for (vals, rn) in rows {
            full_agg
                .add_record(
                    &make_record(&input, vals.clone()),
                    *rn,
                    &ctx_for(&stable, &file, *rn),
                )
                .expect("add_record");
        }
        // Test records carry no `$source.name` stamp, so every ingest
        // landed under `MERGED_SOURCE_NAME` — pass the same Arc here so
        // the source-equality match resolves.
        let merged_name = crate::executor::dispatch::source_name_arc_of(&make_record(
            &input,
            rows.first().expect("at least one input row").0.clone(),
        ));
        for &row_id in retract {
            full_agg
                .retract_row(row_id, &merged_name)
                .expect("retract_row");
        }
        let mut out_after_retract = Vec::new();
        full_agg
            .finalize_in_place(&ctx_for(&stable, &file, 0), &mut out_after_retract)
            .expect("finalize_in_place");

        // Baseline: feed only the surviving subset from scratch.
        let surviving = feed_subset(rows);
        let mut baseline_agg = build_test_aggregator_relaxed(
            input_fields,
            group_by,
            cxl,
            10 * 1024 * 1024,
            None,
            true,
        );
        for (vals, rn) in &surviving {
            baseline_agg
                .add_record(
                    &make_record(&input, vals.clone()),
                    *rn,
                    &ctx_for(&stable, &file, *rn),
                )
                .expect("add_record");
        }
        let mut out_baseline = Vec::new();
        baseline_agg
            .finalize_in_place(&ctx_for(&stable, &file, 0), &mut out_baseline)
            .expect("finalize_in_place");

        let after: Vec<Record> = out_after_retract.into_iter().map(|(r, _)| r).collect();
        let baseline: Vec<Record> = out_baseline.into_iter().map(|(r, _)| r).collect();
        (after, baseline)
    }

    fn record_set_eq(a: &[Record], b: &[Record]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        for ra in a {
            let mut matched = false;
            for rb in b {
                if ra.values().len() != rb.values().len() {
                    continue;
                }
                if ra
                    .values()
                    .iter()
                    .zip(rb.values().iter())
                    .all(|(x, y)| match (x, y) {
                        (Value::Float(fx), Value::Float(fy)) => {
                            (fx - fy).abs() < 1e-9 || fx.to_bits() == fy.to_bits()
                        }
                        (Value::Integer(ix), Value::Integer(iy)) => ix == iy,
                        (Value::String(sx), Value::String(sy)) => sx == sy,
                        (Value::Null, Value::Null) => true,
                        _ => false,
                    })
                {
                    matched = true;
                    break;
                }
            }
            if !matched {
                return false;
            }
        }
        true
    }

    #[test]
    fn test_buffer_mode_retract_min_single_group_matches_baseline() {
        let rows = vec![
            (vec![Value::String("g".into()), Value::Integer(10)], 0),
            (vec![Value::String("g".into()), Value::Integer(5)], 1),
            (vec![Value::String("g".into()), Value::Integer(20)], 2),
        ];
        let (after, baseline) = run_with_retract(
            "emit k = k\nemit lo = min(v)",
            &[("k", Type::String), ("v", Type::Int)],
            &["k"],
            &rows,
            &[1],
            |all| all.iter().filter(|(_, rn)| *rn != 1).cloned().collect(),
        );
        assert!(
            record_set_eq(&after, &baseline),
            "Min retract: {after:?} != {baseline:?}"
        );
    }

    #[test]
    fn test_buffer_mode_retract_max_many_groups_matches_baseline() {
        let rows = vec![
            (vec![Value::String("g1".into()), Value::Integer(10)], 0),
            (vec![Value::String("g1".into()), Value::Integer(5)], 1),
            (vec![Value::String("g2".into()), Value::Integer(20)], 2),
            (vec![Value::String("g2".into()), Value::Integer(30)], 3),
        ];
        let (after, baseline) = run_with_retract(
            "emit k = k\nemit hi = max(v)",
            &[("k", Type::String), ("v", Type::Int)],
            &["k"],
            &rows,
            &[3],
            |all| all.iter().filter(|(_, rn)| *rn != 3).cloned().collect(),
        );
        assert!(
            record_set_eq(&after, &baseline),
            "Max retract many-groups: {after:?} != {baseline:?}"
        );
    }

    #[test]
    fn test_buffer_mode_retract_avg_recomputes_from_survivors() {
        let rows = vec![
            (vec![Value::String("g".into()), Value::Float(1.0)], 0),
            (vec![Value::String("g".into()), Value::Float(2.0)], 1),
            (vec![Value::String("g".into()), Value::Float(3.0)], 2),
        ];
        let (after, baseline) = run_with_retract(
            "emit k = k\nemit a = avg(v)",
            &[("k", Type::String), ("v", Type::Float)],
            &["k"],
            &rows,
            &[2],
            |all| all.iter().filter(|(_, rn)| *rn != 2).cloned().collect(),
        );
        assert!(
            record_set_eq(&after, &baseline),
            "Avg retract: {after:?} != {baseline:?}"
        );
    }

    #[test]
    fn test_buffer_mode_retract_weighted_avg_recomputes_from_survivors() {
        let rows = vec![
            (
                vec![
                    Value::String("g".into()),
                    Value::Float(1.0),
                    Value::Float(2.0),
                ],
                0,
            ),
            (
                vec![
                    Value::String("g".into()),
                    Value::Float(2.0),
                    Value::Float(1.0),
                ],
                1,
            ),
            (
                vec![
                    Value::String("g".into()),
                    Value::Float(4.0),
                    Value::Float(3.0),
                ],
                2,
            ),
        ];
        let (after, baseline) = run_with_retract(
            "emit k = k\nemit wa = weighted_avg(v, w)",
            &[("k", Type::String), ("v", Type::Float), ("w", Type::Float)],
            &["k"],
            &rows,
            &[1],
            |all| all.iter().filter(|(_, rn)| *rn != 1).cloned().collect(),
        );
        assert!(
            record_set_eq(&after, &baseline),
            "WeightedAvg retract: {after:?} != {baseline:?}"
        );
    }

    // ----------------------------------------------------------------
    // Per-source lineage narrowing — `retract_row` matches both row
    // and source. Two sources independently number rows starting at 0,
    // so the same `(row_id, group)` can land twice in one group from
    // different upstreams; the `Vec<(u64, Arc<str>)>` lineage tuple
    // must scope each retract to a single source's contribution.
    // ----------------------------------------------------------------

    /// Build a record carrying an engine-stamped `$source.name` column.
    /// Mirrors what the dispatch layer hands the aggregator after
    /// reading from a Source node — `source_name_arc_of` reads from
    /// the `FieldMetadata::SourceName` slot, not from a separate
    /// argument, so add_record must see the stamp on the record itself.
    fn make_record_with_source(
        user_schema_cols: &[&str],
        values: Vec<Value>,
        source_name: &str,
    ) -> Record {
        let mut builder = clinker_record::SchemaBuilder::new();
        for c in user_schema_cols {
            builder = builder.with_field(*c);
        }
        builder =
            builder.with_field_meta("$source.name", clinker_record::FieldMetadata::source_name());
        let schema = builder.build();
        let mut full_values = values;
        full_values.push(Value::String(source_name.into()));
        Record::new(schema, full_values)
    }

    #[test]
    fn test_retract_row_narrows_by_source_when_two_sources_share_a_row_id() {
        // Lineage path: SUM is Reversible-only, so a relaxed aggregator
        // builds in-memory groups under the lineage strategy and stores
        // each contribution under a `(row_id, source_name)` tuple. Per-
        // source rewinds rely on that tuple matching on both fields —
        // a regression to bare `u64` would silently retract whichever
        // source happened to hit the row id first.
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        let mut agg = build_test_aggregator_relaxed(
            &[("k", Type::String), ("v", Type::Int)],
            &["k"],
            "emit k = k\nemit total = sum(v)",
            10 * 1024 * 1024,
            None,
            true,
        );

        // Two sources independently emit row_num=5 into the same group
        // "g". Without per-source narrowing the second add would
        // collide with the first in the lineage vec.
        let r_a = make_record_with_source(
            &["k", "v"],
            vec![Value::String("g".into()), Value::Integer(10)],
            "src_a",
        );
        let r_b = make_record_with_source(
            &["k", "v"],
            vec![Value::String("g".into()), Value::Integer(100)],
            "src_b",
        );
        agg.add_record(&r_a, 5, &ctx_for(&stable, &file, 5))
            .expect("add src_a");
        agg.add_record(&r_b, 5, &ctx_for(&stable, &file, 5))
            .expect("add src_b");

        // Sanity: both contributions present in the same group.
        let mut out = Vec::new();
        agg.finalize_in_place(&ctx_for(&stable, &file, 0), &mut out)
            .expect("finalize_in_place initial");
        assert_eq!(out.len(), 1, "single group expected");
        let total_idx = out[0]
            .0
            .schema()
            .index("total")
            .expect("total column on output");
        match out[0].0.values().get(total_idx).expect("total slot") {
            Value::Integer(n) => assert_eq!(*n, 110, "sum of both sources"),
            other => panic!("expected Integer total, got {other:?}"),
        }

        // Retract row 5 scoped to src_a: src_b's contribution at the
        // identical row id must survive.
        let src_a: Arc<str> = Arc::from("src_a");
        agg.retract_row(5, &src_a).expect("retract src_a");
        let mut out = Vec::new();
        agg.finalize_in_place(&ctx_for(&stable, &file, 0), &mut out)
            .expect("finalize_in_place after src_a retract");
        assert_eq!(out.len(), 1, "group still present");
        match out[0].0.values().get(total_idx).expect("total slot") {
            Value::Integer(n) => assert_eq!(
                *n, 100,
                "src_b's 100 must remain after src_a retract at the same row id"
            ),
            other => panic!("expected Integer total, got {other:?}"),
        }

        // Retract the remaining src_b contribution at row 5 — the
        // lineage tuple still matches because src_b's `(5, "src_b")`
        // entry survived the previous narrowed retract. A successful
        // Ok return here is the load-bearing assertion: a bare-u64
        // lineage would have removed src_b's entry on the first
        // retract above and this call would fail with `not found in
        // any lineage group`.
        let src_b: Arc<str> = Arc::from("src_b");
        agg.retract_row(5, &src_b)
            .expect("retract src_b at the same row id must succeed");
    }

    #[test]
    fn test_retract_row_narrows_by_source_in_buffer_mode() {
        // Buffer-mode path: MIN is BufferRequired, so the aggregator
        // routes through `add_record_buffered` / `buffered_groups` and
        // stores `(row_id, source_name)` tuples on `input_rows`. The
        // narrowing invariant has to hold for both retract strategies,
        // so this companion test pins the buffer path too.
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        let mut agg = build_test_aggregator_relaxed(
            &[("k", Type::String), ("v", Type::Int)],
            &["k"],
            "emit k = k\nemit lo = min(v)",
            10 * 1024 * 1024,
            None,
            true,
        );
        assert!(
            agg.buffer_mode,
            "min(v) under relaxed must select buffer-mode"
        );

        // src_a contributes a smaller value at row 5; src_b a larger
        // one at the same row id. Retract src_a — the survivor min
        // is src_b's 100, not src_a's 10.
        let r_a = make_record_with_source(
            &["k", "v"],
            vec![Value::String("g".into()), Value::Integer(10)],
            "src_a",
        );
        let r_b = make_record_with_source(
            &["k", "v"],
            vec![Value::String("g".into()), Value::Integer(100)],
            "src_b",
        );
        agg.add_record(&r_a, 5, &ctx_for(&stable, &file, 5))
            .expect("add src_a");
        agg.add_record(&r_b, 5, &ctx_for(&stable, &file, 5))
            .expect("add src_b");

        let src_a: Arc<str> = Arc::from("src_a");
        agg.retract_row(5, &src_a).expect("retract src_a");
        let mut out = Vec::new();
        agg.finalize_in_place(&ctx_for(&stable, &file, 0), &mut out)
            .expect("finalize_in_place");
        assert_eq!(out.len(), 1, "single group expected");
        let lo_idx = out[0].0.schema().index("lo").expect("lo column on output");
        match out[0].0.values().get(lo_idx).expect("lo slot") {
            Value::Integer(n) => assert_eq!(
                *n, 100,
                "src_b's value must survive a same-row-id retract scoped to src_a"
            ),
            other => panic!("expected Integer lo, got {other:?}"),
        }
    }

    // ----------------------------------------------------------------
    // Synthetic aggregate-CK column emission
    //
    // Mirrors the planner's schema-widening pass: a relaxed aggregate's
    // output schema gains one trailing `$ck.aggregate.<name>` column
    // tagged `FieldMetadata::AggregateGroupIndex`. The runtime stamps
    // each finalized record's slot with the in-memory group index so
    // downstream detect-phase fan-out can resolve a buffer-cell key
    // back to the contributing source rows via `input_rows`.
    // ----------------------------------------------------------------

    /// Like `build_test_aggregator_relaxed` but appends an explicit
    /// `$ck.aggregate.<transform_name>` column to the output schema,
    /// mimicking what the schema-widening pass does at compile time
    /// for relaxed aggregates.
    fn build_test_aggregator_with_synthetic_ck(
        input_fields: &[(&str, Type)],
        group_by: &[&str],
        cxl_src: &str,
        memory_budget: usize,
        spill_dir: Option<std::path::PathBuf>,
    ) -> HashAggregator {
        let parsed = Parser::parse(cxl_src);
        assert!(
            parsed.errors.is_empty(),
            "parse errors: {:?}",
            parsed.errors
        );
        let field_names: Vec<&str> = input_fields.iter().map(|(n, _)| *n).collect();
        let resolved =
            resolve_program(parsed.ast, &field_names, parsed.node_count).expect("resolve");
        let schema_map: IndexMap<cxl::typecheck::QualifiedField, Type> = input_fields
            .iter()
            .map(|(n, t)| (cxl::typecheck::QualifiedField::bare(*n), t.clone()))
            .collect();
        let row = Row::closed(schema_map, cxl::lexer::Span::new(0, 0));
        let mode = AggregateMode::GroupBy {
            group_by_fields: group_by.iter().map(|s| (*s).to_string()).collect(),
        };
        let typed = type_check_with_mode(resolved, &row, mode).expect("typecheck");
        let schema_names: Vec<String> =
            input_fields.iter().map(|(n, _)| (*n).to_string()).collect();
        let group_by_owned: Vec<String> = group_by.iter().map(|s| (*s).to_string()).collect();
        let mut compiled =
            extract_aggregates(&typed, &group_by_owned, &schema_names).expect("extract_aggregates");
        compiled.set_retraction_flags(true);

        let transform_name = "test_agg";
        // Build the output schema mirroring the widening pass: user
        // emits first, then the synthetic CK column at the tail.
        let mut builder = clinker_record::SchemaBuilder::new();
        for emit in compiled.emits.iter() {
            builder = builder.with_field(emit.output_name.clone());
        }
        let synthetic_name = format!("$ck.aggregate.{transform_name}");
        builder = builder.with_field_meta(
            synthetic_name,
            clinker_record::FieldMetadata::aggregate_group_index(transform_name),
        );
        let output_schema = builder.build();

        let mut spill_cols: Vec<Box<str>> = group_by_owned
            .iter()
            .map(|s| Box::<str>::from(s.as_str()))
            .collect();
        spill_cols.push("__acc_state".into());
        let spill_schema = Arc::new(Schema::new(spill_cols));

        let evaluator = ProgramEvaluator::new(Arc::new(typed), false);

        HashAggregator::new(
            Arc::new(compiled),
            evaluator,
            output_schema,
            spill_schema,
            memory_budget,
            spill_dir,
            transform_name,
            crate::pipeline::memory::ConsumerHandle::new(),
        )
    }

    #[test]
    fn test_synthetic_ck_lineage_mode_finalize_stamps_group_index() {
        // Lineage path (Reversible-only bindings): finalize must stamp
        // the synthetic column from each group's in-memory group_index.
        let input = make_schema(&["k", "v"]);
        let mut agg = build_test_aggregator_with_synthetic_ck(
            &[("k", Type::String), ("v", Type::Int)],
            &["k"],
            "emit k = k\nemit total = sum(v)",
            10_000_000,
            None,
        );
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        // Two groups; the first inserted gets group_index 0, the
        // second gets 1.
        for (i, (k, v)) in [("a", 1i64), ("b", 10), ("a", 2), ("b", 20)]
            .iter()
            .enumerate()
        {
            let r = make_record(&input, vec![Value::String((*k).into()), Value::Integer(*v)]);
            agg.add_record(&r, i as u64, &ctx_for(&stable, &file, i as u64))
                .unwrap();
        }
        let mut out = Vec::new();
        agg.finalize_in_place(&ctx_for(&stable, &file, 0), &mut out)
            .expect("finalize_in_place");
        let synthetic_idx = out[0]
            .0
            .schema()
            .index("$ck.aggregate.test_agg")
            .expect("synthetic column on output schema");
        // Each emitted record's synthetic slot must carry an Integer
        // value matching its in-memory group index. With two groups
        // the set of indices is exactly {0, 1}.
        let mut indices: Vec<i64> = Vec::new();
        for (record, _) in &out {
            match record.values().get(synthetic_idx).expect("synthetic slot") {
                Value::Integer(n) => indices.push(*n),
                other => panic!("synthetic CK must be Integer, got {other:?}"),
            }
        }
        indices.sort();
        assert_eq!(indices, vec![0, 1]);
    }

    #[test]
    fn test_synthetic_ck_buffer_mode_finalize_stamps_group_index() {
        // Buffer-mode path (any BufferRequired binding): the same
        // stamp must apply after `fold_buffered_state` reconstructs
        // the AggregatorGroupState. Confirms `fold_buffered_state`
        // preserves `group_index` and the runtime stamp fires for
        // both retraction strategies.
        let input = make_schema(&["k", "v"]);
        let mut agg = build_test_aggregator_with_synthetic_ck(
            &[("k", Type::String), ("v", Type::Int)],
            &["k"],
            "emit k = k\nemit lo = min(v)",
            10_000_000,
            None,
        );
        assert!(
            agg.buffer_mode,
            "min(v) under relaxed must select buffer-mode"
        );
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        for (i, (k, v)) in [("a", 5i64), ("b", 50), ("a", 1), ("b", 10)]
            .iter()
            .enumerate()
        {
            let r = make_record(&input, vec![Value::String((*k).into()), Value::Integer(*v)]);
            agg.add_record(&r, i as u64, &ctx_for(&stable, &file, i as u64))
                .unwrap();
        }
        let mut out = Vec::new();
        agg.finalize_in_place(&ctx_for(&stable, &file, 0), &mut out)
            .expect("finalize_in_place");
        let synthetic_idx = out[0]
            .0
            .schema()
            .index("$ck.aggregate.test_agg")
            .expect("synthetic column on output schema");
        let mut indices: Vec<i64> = Vec::new();
        for (record, _) in &out {
            match record.values().get(synthetic_idx).expect("synthetic slot") {
                Value::Integer(n) => indices.push(*n),
                other => panic!("synthetic CK must be Integer, got {other:?}"),
            }
        }
        indices.sort();
        assert_eq!(indices, vec![0, 1]);
    }

    #[test]
    fn test_synthetic_ck_strict_aggregator_pays_zero_overhead() {
        // Strict (non-relaxed) aggregator's output schema does not
        // carry the synthetic column; the finalize lookup short-circuits
        // and the emitted record has no synthetic slot.
        let input = make_schema(&["k"]);
        let mut agg = build_test_aggregator(
            &[("k", Type::String)],
            &["k"],
            "emit k = k\nemit n = count(*)",
            10_000_000,
            None,
        );
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        for i in 0..4u64 {
            let r = make_record(&input, vec![Value::String("a".into())]);
            agg.add_record(&r, i, &ctx_for(&stable, &file, i)).unwrap();
        }
        let mut out = Vec::new();
        agg.finalize(&ctx_for(&stable, &file, 0), &mut out)
            .expect("finalize");
        for (record, _) in &out {
            assert!(
                record.schema().index("$ck.aggregate.test_agg").is_none(),
                "strict aggregator's output schema must not carry the synthetic column"
            );
        }
    }

    #[test]
    fn test_synthetic_ck_survives_spill_round_trip_via_finalize() {
        // Force at least one spill, then finalize through the
        // spill-recovery path. The synthetic column must survive: each
        // emitted record's synthetic slot is set from the merged
        // `AggregatorGroupState.group_index` reconstructed by the
        // spill-merge loop. `group_index` itself is derive-Serialize on
        // both `AggregatorGroupState` and `BufferedGroupState`, so the
        // postcard round trip preserves the value end-to-end.
        let tmp = tempfile::tempdir().expect("tempdir");
        let input = make_schema(&["k"]);
        let mut agg = build_test_aggregator_with_synthetic_ck(
            &[("k", Type::String)],
            &["k"],
            "emit k = k\nemit n = count(*)",
            1024,
            Some(tmp.path().to_path_buf()),
        );
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("t.csv");
        let keys = ["a", "b", "c", "d"];
        for i in 0..32u64 {
            let k = keys[(i as usize) % keys.len()];
            let r = make_record(&input, vec![Value::String(k.into())]);
            agg.add_record(&r, i, &ctx_for(&stable, &file, i)).unwrap();
        }
        assert!(
            !agg.spill_files().is_empty(),
            "tiny budget must trigger at least one spill"
        );

        let mut out = Vec::new();
        agg.finalize(&ctx_for(&stable, &file, 0), &mut out)
            .expect("finalize after spill");
        let synthetic_idx = out[0]
            .0
            .schema()
            .index("$ck.aggregate.test_agg")
            .expect("synthetic column on output schema");
        // Four distinct keys → four distinct group indices reassigned
        // by the spill-recovery loop. Exact ordering is not contracted,
        // but each row must carry a non-negative Integer.
        let mut indices: Vec<i64> = Vec::new();
        for (record, _) in &out {
            match record.values().get(synthetic_idx).expect("synthetic slot") {
                Value::Integer(n) => {
                    assert!(*n >= 0, "synthetic CK must be a non-negative group index");
                    indices.push(*n);
                }
                other => panic!("synthetic CK must be Integer, got {other:?}"),
            }
        }
        assert_eq!(indices.len(), 4, "one row per distinct key");
    }
}
