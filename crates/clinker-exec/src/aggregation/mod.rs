//! Hash + streaming aggregation engine.
//!
//! This module is split into focused submodules:
//!
//! * [`error`] — the finalize-time [`AggregateEvalError`] /
//!   [`HashAggError`] taxonomy plus its `PipelineError` mapping.
//! * [`hash`] — the [`HashAggregator`] per-group hash table, the
//!   [`AccumulatorFactory`] prototype clone factory, the
//!   [`AggregateConsumer`] arbitrator hook, and the shared group
//!   finalize helpers.
//! * [`spill`] — the binary spill writer/reader/merge machinery
//!   ([`SpillState`], [`AggSpillFile`]).
//!
//! The streaming aggregator, the residual evaluator, the
//! `AccumulatorOp` ingestion modes, the sidecar-merge helpers, and the
//! plan-time streaming-eligibility qualifier live here in the hub
//! alongside the re-exports that preserve the
//! `clinker_exec::aggregation::*` public surface.
//!
//! Runtime types referenced by `PlanNode::Aggregation`'s executor dispatch:
//!
//! * [`clinker_plan::plan::types::AggregateStrategy`] — plan-time enum (also
//!   surfaced on the plan node) selecting the per-group hash table or
//!   the streaming single-group fold.
//! * [`AggregateInput`] — dual input mode: a freshly read input record
//!   or a previously spilled per-group state being recovered.
//! * [`AccumulatorFactory`] — clones a per-group prototype
//!   `AccumulatorRow` from the plan-time `CompiledAggregate`'s
//!   bindings, avoiding repeat factory dispatch on every new key.
//! * [`AggregateEvalScope`] / [`eval_expr_in_agg_scope`] — finalize-time
//!   evaluator that resolves `Expr::AggSlot` to a finalized
//!   accumulator slot value and `Expr::GroupKey` to a group-key column
//!   value.

mod error;
mod hash;
mod spill;

pub use error::{AggregateEvalError, HashAggError};
pub use hash::{AccumulatorFactory, AggregateConsumer, AggregatorConfig, HashAggregator};
pub use spill::{AggSpillFile, SpillState};

use hash::NullStorage;
pub(crate) use hash::{empty_global_fold_row, finalize_group_inner, group_by_sort_fields};

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use clinker_record::accumulator::{AccumulatorEnum, AccumulatorRow};
use clinker_record::group_key::value_to_group_key;
use clinker_record::schema::Schema;
use clinker_record::{GroupByKey, Record, Value};
use cxl::ast::{BinOp, Expr, LiteralValue, UnaryOp};
use cxl::eval::{EvalContext, EvalError, ProgramEvaluator, eval_expr};
use cxl::plan::{AggregateBinding, BindingArg, CompiledAggregate};

use clinker_plan::error::PipelineError;
use clinker_plan::plan::types::AggregateStrategy;

/// Input to an aggregator's `add` path. Live input records and
/// recovered spilled state both flow through the same dispatch so the
/// merge phase can reuse the hash table without a separate code path.
pub enum AggregateInput {
    /// A freshly produced input record from the upstream stage.
    RawRecord(Record),
    /// A previously spilled per-group state, being merged back during
    /// the spill recovery phase. The key is the full group-by tuple in
    /// declaration order.
    SpilledState {
        key: Vec<GroupByKey>,
        row: AccumulatorRow,
    },
}

// ---------------------------------------------------------------------------
// Finalize-time residual evaluator
// ---------------------------------------------------------------------------

/// Evaluation scope for an aggregate emit residual.
///
/// `slots[i]` is the finalized value of `bindings[i]` for the current
/// group. `key[i]` is the i'th group-by column for the current group.
/// `Expr::AggSlot { slot }` resolves to `slots[slot]`. `Expr::GroupKey
/// { slot }` resolves to `key[slot].to_value()`.
pub struct AggregateEvalScope<'a> {
    pub key: &'a [GroupByKey],
    pub slots: &'a [Value],
}

/// Evaluate a post-extraction residual expression in aggregate scope.
///
/// Resolves [`Expr::AggSlot`] / [`Expr::GroupKey`] leaves directly
/// against `scope` and walks arithmetic / comparison / logical /
/// coalesce / `if` / unary nodes recursively. Constructs that need a
/// row context (`FieldRef`, `MethodCall`, `WindowCall`, `Match`,
/// metadata access) are surfaced as
/// [`AggregateEvalError::UnsupportedResidual`].
pub fn eval_expr_in_agg_scope(
    expr: &Expr,
    scope: &AggregateEvalScope<'_>,
) -> Result<Value, AggregateEvalError> {
    match expr {
        Expr::AggSlot { slot, .. } => {
            scope
                .slots
                .get(*slot as usize)
                .cloned()
                .ok_or(AggregateEvalError::SlotOutOfRange {
                    slot: *slot,
                    slot_count: scope.slots.len(),
                })
        }
        Expr::GroupKey { slot, .. } => scope
            .key
            .get(*slot as usize)
            .map(GroupByKey::to_value)
            .ok_or(AggregateEvalError::GroupKeyOutOfRange {
                slot: *slot,
                key_count: scope.key.len(),
            }),
        Expr::Literal { value, .. } => Ok(literal_to_value(value)),
        Expr::Binary { op, lhs, rhs, .. } => {
            let l = eval_expr_in_agg_scope(lhs, scope)?;
            let r = eval_expr_in_agg_scope(rhs, scope)?;
            eval_binary(*op, l, r)
        }
        Expr::Unary { op, operand, .. } => {
            let v = eval_expr_in_agg_scope(operand, scope)?;
            eval_unary(*op, v)
        }
        Expr::Coalesce { lhs, rhs, .. } => {
            let l = eval_expr_in_agg_scope(lhs, scope)?;
            if matches!(l, Value::Null) {
                eval_expr_in_agg_scope(rhs, scope)
            } else {
                Ok(l)
            }
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            let c = eval_expr_in_agg_scope(condition, scope)?;
            if matches!(c, Value::Bool(true)) {
                eval_expr_in_agg_scope(then_branch, scope)
            } else if let Some(e) = else_branch {
                eval_expr_in_agg_scope(e, scope)
            } else {
                Ok(Value::Null)
            }
        }
        Expr::Now { .. } => Err(AggregateEvalError::UnsupportedResidual { what: "now" }),
        Expr::FieldRef { .. } | Expr::QualifiedFieldRef { .. } => {
            Err(AggregateEvalError::UnsupportedResidual { what: "field-ref" })
        }
        Expr::PipelineAccess { .. }
        | Expr::VarsAccess { .. }
        | Expr::SourceAccess { .. }
        | Expr::QualifiedSourceAccess { .. }
        | Expr::RecordAccess { .. }
        | Expr::DocAccess { .. } => Err(AggregateEvalError::UnsupportedResidual {
            what: "$pipeline/$source/$record/$doc access",
        }),
        Expr::MethodCall { .. } => Err(AggregateEvalError::UnsupportedResidual {
            what: "method call",
        }),
        Expr::Match { .. } => Err(AggregateEvalError::UnsupportedResidual { what: "match" }),
        Expr::WindowCall { .. } => Err(AggregateEvalError::UnsupportedResidual {
            what: "window call",
        }),
        Expr::AggCall { .. } => Err(AggregateEvalError::UnsupportedResidual {
            what: "raw AggCall — should have been extracted",
        }),
        Expr::Wildcard { .. } => Err(AggregateEvalError::UnsupportedResidual {
            what: "wildcard outside count(*)",
        }),
        Expr::IndexAccess { .. } => Err(AggregateEvalError::UnsupportedResidual {
            what: "bracket-index access",
        }),
        Expr::Closure { .. } => Err(AggregateEvalError::UnsupportedResidual { what: "closure" }),
    }
}

fn literal_to_value(lit: &LiteralValue) -> Value {
    match lit {
        LiteralValue::Int(n) => Value::Integer(*n),
        LiteralValue::Float(f) => Value::Float(*f),
        LiteralValue::String(s) => Value::String(s.clone()),
        LiteralValue::Bool(b) => Value::Bool(*b),
        LiteralValue::Date(d) => Value::Date(*d),
        LiteralValue::Null => Value::Null,
    }
}

fn eval_binary(op: BinOp, l: Value, r: Value) -> Result<Value, AggregateEvalError> {
    use Value::{Bool, Float, Integer, Null};
    let v = match (op, l, r) {
        (BinOp::Add, Integer(a), Integer(b)) => Integer(a.saturating_add(b)),
        (BinOp::Sub, Integer(a), Integer(b)) => Integer(a.saturating_sub(b)),
        (BinOp::Mul, Integer(a), Integer(b)) => Integer(a.saturating_mul(b)),
        (BinOp::Div, Integer(_), Integer(0)) => Null,
        (BinOp::Div, Integer(a), Integer(b)) => Integer(a / b),
        (BinOp::Mod, Integer(_), Integer(0)) => Null,
        (BinOp::Mod, Integer(a), Integer(b)) => Integer(a % b),
        (BinOp::Add, Float(a), Float(b)) => Float(a + b),
        (BinOp::Sub, Float(a), Float(b)) => Float(a - b),
        (BinOp::Mul, Float(a), Float(b)) => Float(a * b),
        (BinOp::Div, Float(a), Float(b)) => Float(a / b),
        (BinOp::Add, Integer(a), Float(b)) | (BinOp::Add, Float(b), Integer(a)) => {
            Float(a as f64 + b)
        }
        (BinOp::Sub, Integer(a), Float(b)) => Float(a as f64 - b),
        (BinOp::Sub, Float(a), Integer(b)) => Float(a - b as f64),
        (BinOp::Mul, Integer(a), Float(b)) | (BinOp::Mul, Float(b), Integer(a)) => {
            Float(a as f64 * b)
        }
        (BinOp::Div, Integer(a), Float(b)) => Float(a as f64 / b),
        (BinOp::Div, Float(a), Integer(b)) => Float(a / b as f64),
        (BinOp::Eq, a, b) => Bool(a == b),
        (BinOp::Neq, a, b) => Bool(a != b),
        (BinOp::And, Bool(a), Bool(b)) => Bool(a && b),
        (BinOp::Or, Bool(a), Bool(b)) => Bool(a || b),
        (_, Null, _) | (_, _, Null) => Null,
        _ => {
            return Err(AggregateEvalError::UnsupportedResidual {
                what: "binary op type combination",
            });
        }
    };
    Ok(v)
}

fn eval_unary(op: UnaryOp, v: Value) -> Result<Value, AggregateEvalError> {
    Ok(match (op, v) {
        (UnaryOp::Neg, Value::Integer(n)) => Value::Integer(n.saturating_neg()),
        (UnaryOp::Neg, Value::Float(f)) => Value::Float(-f),
        (UnaryOp::Not, Value::Bool(b)) => Value::Bool(!b),
        (_, Value::Null) => Value::Null,
        _ => {
            return Err(AggregateEvalError::UnsupportedResidual {
                what: "unary op type combination",
            });
        }
    })
}

// ---------------------------------------------------------------------------
// HashAggregator
// ---------------------------------------------------------------------------

/// Per-group state held inside the hash table.
///
/// `row` is the row of accumulators (one slot per `AggregateBinding`)
/// being driven by the hot loop.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatorGroupState {
    pub row: AccumulatorRow,
    /// Minimum `row_num` across every input record folded into this
    /// group. Initialized to `u64::MAX`; finalize emits this as the
    /// `SortRow` row-number so downstream sort-stable operators preserve
    /// the earliest input row's position. The global-fold empty-input
    /// case emits `0` because no record ever updates this field.
    pub min_row_num: u64,
    /// Stable in-memory index of this group within the owning
    /// `HashAggregator.groups` map at insertion time. Populated only on
    /// the lineage path (when `compiled.requires_lineage` is true) so a
    /// flat `(input_row, group_index)` map can be reconstructed without
    /// a parallel HashMap. Meaningless after spill+merge — index space
    /// is reassigned by the recovery loop.
    pub group_index: u32,
    /// Per-group `(input_row_number, source_name)` list. Populated only
    /// on the lineage path; empty otherwise (an empty `Vec` does not
    /// allocate). The source name pairs each entry with its originating
    /// Source so a relaxed-CK retract can scope rewinds per source
    /// without colliding across the per-source `row_num` namespaces.
    /// Spilled alongside accumulator state so the post-merge
    /// `AggregatorGroupState` preserves which input rows folded into
    /// each group across the round trip; serialization uses serde's
    /// `rc` feature to deserialize `Arc<str>` from owned strings on
    /// recovery.
    pub input_rows: Vec<(u64, Arc<str>)>,
    /// Per-row evaluated binding values, parallel to `input_rows`.
    /// `retract_values[i]` is the per-binding `Vec<Value>` produced by
    /// the i-th ingested row (in `CompiledAggregate.bindings[]` order),
    /// stored only on the lineage path when retraction is enabled. Empty
    /// otherwise (no allocation cost on the strict path).
    ///
    /// Necessary because `AccumulatorEnum::sub` needs the original
    /// observed value to walk back its contribution; the folded
    /// accumulator state alone has thrown that information away. Storage
    /// shape mirrors `BufferedGroupState.contributions` so a future
    /// convergence of the two retraction strategies can share one
    /// per-row-Vec layout.
    pub retract_values: Vec<Vec<Value>>,
}

impl AggregatorGroupState {
    pub(crate) fn new(row: AccumulatorRow) -> Self {
        Self {
            row,
            min_row_num: u64::MAX,
            group_index: 0,
            input_rows: Vec::new(),
            retract_values: Vec::new(),
        }
    }
}

/// Per-group state for buffer-mode aggregation.
///
/// Parallel to `AggregatorGroupState` but carries raw per-row contributions
/// instead of folded accumulator state. Used when a relaxed-correlation-key
/// aggregate has at least one `BufferRequired` accumulator binding (`Min`,
/// `Max`, `Avg`, `WeightedAvg`) — the rollback step needs to recompute
/// affected groups from `contributions − retracted_rows` because those
/// accumulators do not admit an O(1) inverse op without precision loss
/// (`Avg`, `WeightedAvg`) or without the surviving multiset (`Min`, `Max`).
///
/// Storage shape: `contributions[i]` is the per-binding evaluated value
/// vector for the i-th input row folded into this group, stored in
/// `bindings[]` order. Row-major layout (one `Vec<Value>` per row, each
/// of length `bindings.len()`) chosen over columnar (`Vec<Vec<Value>>`
/// indexed by binding) because retraction operates per-row: removing a
/// retracted row by index is O(n_bindings) on the row-major layout
/// versus O(rows_per_binding × n_bindings) on the columnar one, and the
/// hot path (append on each ingested record) is one push of one inner
/// `Vec` either way.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferedGroupState {
    /// Minimum `row_num` across every input record folded into this
    /// group. Same semantics as `AggregatorGroupState.min_row_num`:
    /// downstream sort-stable operators key off the earliest input
    /// row's position.
    pub min_row_num: u64,
    /// Stable in-memory index of this group within
    /// `HashAggregator.buffered_groups` at insertion time, mirroring
    /// the lineage path's group index. Meaningless after spill+merge.
    pub group_index: u32,
    /// Per-group `(input_row_number, source_name)` list. Mirrors
    /// `AggregatorGroupState.input_rows` so the spill-merge concat
    /// round-trips through the same shape on both retraction strategies.
    /// `Arc<str>` deserialization uses serde's `rc` feature (owned
    /// copy on recovery).
    pub input_rows: Vec<(u64, Arc<str>)>,
    /// Per-row evaluated binding values. `contributions[i][slot]`
    /// holds the value the binding at slot `slot` produced for the
    /// i-th ingested row, in `CompiledAggregate.bindings[]` order.
    pub contributions: Vec<Vec<Value>>,
}

impl BufferedGroupState {
    pub(crate) fn new() -> Self {
        Self {
            min_row_num: u64::MAX,
            group_index: 0,
            input_rows: Vec::new(),
            contributions: Vec::new(),
        }
    }
}

/// Per-node-buffer row produced by every executor node that emits into
/// `node_buffers`. The Record is authoritative: all emitted fields have
/// been applied via `Record::set` onto the widened schema. Downstream
/// nodes resolve field references against the Record directly (no
/// parallel bookkeeping threading).
pub type SortRow = (Record, u64);

// ---------------------------------------------------------------------------
// AggregateStream wrapper enum
// ---------------------------------------------------------------------------

/// Executor-facing aggregation dispatch wrapper.
///
/// The `#[non_exhaustive]` attribute reserves the option of adding new
/// variants non-breakingly — avoiding the DataFusion #12086 failure
/// mode of placeholder generic parameters in dispatch enums.
#[non_exhaustive]
pub enum AggregateStream {
    Hash(Box<HashAggregator>),
    /// Streaming aggregation over a pre-sorted upstream.
    Streaming(Box<StreamingAggregator<AddRaw>>),
}

impl AggregateStream {
    /// Construct the stream for a single `PlanNode::Aggregation` node.
    ///
    /// Streaming strategy is rejected with a fallible
    /// `PipelineError::Internal` (NOT `todo!()` / `unreachable!()`) per
    /// the DataFusion #12086 lesson on unreachable arms in long-lived
    /// executor dispatch tables.
    pub fn for_node(
        strategy: AggregateStrategy,
        config: AggregatorConfig,
    ) -> Result<Self, PipelineError> {
        match strategy {
            AggregateStrategy::Hash => Ok(Self::Hash(Box::new(HashAggregator::new(config)))),
            AggregateStrategy::Streaming => {
                // The streaming path ignores the spill schema, memory
                // budget, spill directory, and consumer handle: it folds
                // a pre-sorted upstream key-by-key without accumulating an
                // in-memory hash table, so there is nothing to spill or
                // charge into the arbitrator.
                let AggregatorConfig {
                    compiled,
                    evaluator,
                    output_schema,
                    transform_name,
                    ..
                } = config;
                Ok(Self::Streaming(Box::new(StreamingAggregator::new_for_raw(
                    compiled,
                    evaluator,
                    output_schema,
                    transform_name,
                ))))
            }
        }
    }

    /// Drive one input record through the wrapper. The Hash arm ignores
    /// `out` (it defers all emission to `finalize`). The Streaming arm
    /// pushes one finalized `SortRow` into `out` for every key boundary
    /// crossed by this record.
    pub fn add_record(
        &mut self,
        record: &Record,
        row_num: u64,
        ctx: &EvalContext,
        out: &mut Vec<SortRow>,
    ) -> Result<(), HashAggError> {
        match self {
            Self::Hash(h) => {
                let _ = &out; // Hash defers emission to finalize.
                h.add_record(record, row_num, ctx)
            }
            Self::Streaming(s) => s.add_record(record, row_num, ctx, out),
        }
    }

    /// Drain the wrapper into `out`. Both arms append into the existing
    /// vector — the executor's drain loop is shape-uniform across arms.
    pub fn finalize(self, ctx: &EvalContext, out: &mut Vec<SortRow>) -> Result<(), HashAggError> {
        match self {
            Self::Hash(h) => h.finalize(ctx, out),
            Self::Streaming(s) => s.flush(ctx, out),
        }
    }

    /// Convert the stream into a boxed [`HashAggregator`] owning the
    /// in-memory state, plus a snapshot of the Hash-arm group-by indices.
    /// Returns `None` for the `Streaming` arm — that path is rejected
    /// for retraction-mode aggregates at compile time (E15Y), so the
    /// commit orchestrator only ever calls this on a Hash-arm stream.
    pub fn into_retained_hash(self) -> Option<Box<HashAggregator>> {
        match self {
            Self::Hash(h) => Some(h),
            Self::Streaming(_) => None,
        }
    }
}

/// Dispatch one `BindingArg` to its accumulator. Returns the heap-byte
/// delta produced by the accumulator's `add` call so the hash aggregator
/// can fold it into `value_heap_bytes`.
pub(super) fn dispatch_binding(
    arg: &BindingArg,
    acc: &mut AccumulatorEnum,
    record: &Record,
    ctx: &EvalContext,
    evaluator: &ProgramEvaluator,
) -> Result<usize, HashAggError> {
    match arg {
        BindingArg::Field(idx) => {
            let v = record
                .values()
                .get(*idx as usize)
                .cloned()
                .unwrap_or(Value::Null);
            Ok(acc.add(&v))
        }
        BindingArg::Wildcard => Ok(acc.add(&Value::Null)),
        BindingArg::Expr(e) => {
            let mut env: std::collections::HashMap<String, Value> =
                std::collections::HashMap::new();
            let v = eval_expr::<NullStorage>(e, evaluator.typed(), ctx, record, None, &mut env)?;
            Ok(acc.add(&v))
        }
        BindingArg::Pair(a, b) => {
            let va = eval_binding_arg_value(a, record, ctx, evaluator)?;
            let vb = eval_binding_arg_value(b, record, ctx, evaluator)?;
            acc.add_weighted(&va, &vb);
            Ok(0)
        }
    }
}

pub(super) fn eval_binding_arg_value(
    arg: &BindingArg,
    record: &Record,
    ctx: &EvalContext,
    evaluator: &ProgramEvaluator,
) -> Result<Value, HashAggError> {
    match arg {
        BindingArg::Field(idx) => Ok(record
            .values()
            .get(*idx as usize)
            .cloned()
            .unwrap_or(Value::Null)),
        BindingArg::Wildcard => Ok(Value::Null),
        BindingArg::Expr(e) => {
            let mut env: std::collections::HashMap<String, Value> =
                std::collections::HashMap::new();
            Ok(eval_expr::<NullStorage>(
                e,
                evaluator.typed(),
                ctx,
                record,
                None,
                &mut env,
            )?)
        }
        BindingArg::Pair(_, _) => Err(HashAggError::EvalFailed(EvalError::new(
            cxl::eval::EvalErrorKind::TypeMismatch {
                expected: "scalar binding arg",
                got: "nested Pair",
            },
            cxl::lexer::Span::new(0, 0),
        ))),
    }
}

// ---------------------------------------------------------------------------
// StreamingAggregator<Op: AccumulatorOp> — raw + merge modes
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// AccumulatorOp trait + AddRaw / MergeState impls
// ---------------------------------------------------------------------------
//
// The trait lives here in `clinker-exec`'s aggregation module (NOT
// `clinker-record`) because `MergeState::Input` references
// `AggregatorGroupState`, defined alongside the hash aggregator.
// `clinker-record` has no dependency on `cxl`, so the `apply_row`
// signature — which takes `&[AggregateBinding]` — would also be
// unexpressible there.

/// Monomorphization marker trait for the streaming aggregator's
/// ingestion hot loop.
///
/// Each implementation pins an `Input` type and an `apply_row` fast
/// path. The streaming aggregator is generic over `Op: AccumulatorOp`
/// so rustc compiles one specialized loop per mode with no runtime
/// dispatch cost.
///
/// * [`AddRaw`] — `Input = Record`. One incoming raw record per call;
///   `apply_row` indexes `record.values[binding.input_field_index]`
///   for the fast `BindingArg::Field` case. The `BindingArg::Expr` /
///   `BindingArg::Pair` general paths cannot be expressed through this
///   narrow signature (no `EvalContext` / `ProgramEvaluator`); the
///   streaming aggregator's full `add_record` continues to fall back
///   to the general `dispatch_binding` path for those, and `apply_row`
///   debug-asserts in that case to surface any future caller that
///   wired the fast path to a non-trivial binding arg.
/// * [`MergeState`] — `Input = (Vec<u8>, AggregatorGroupState)`. Each
///   call carries an encoded sort key (read by the LoserTree
///   comparator) plus the full per-group state: the `AccumulatorRow`
///   and `min_row_num`. `apply_row` folds the incoming state into the
///   currently-open per-group `row` via `AccumulatorEnum::merge`. The
///   `min_row_num` merge lives on `GroupBoundary::push` because it
///   operates on the open group's full `AggregatorGroupState`, not on
///   the `row` alone. The full-state shape (rather than just
///   `AccumulatorRow`) is required because dropping `min_row_num` on
///   the spill-recovery path would yield a different value than the
///   in-memory path, breaking byte-identical finalization.
pub trait AccumulatorOp: Default + 'static {
    /// Per-call input payload. Monomorphized per implementation.
    type Input;

    /// Fold one input into a per-group `AccumulatorRow`.
    ///
    /// `bindings` is the compiled binding list owned by the
    /// `AccumulatorFactory`; `row` is the currently-open group's
    /// accumulator row; `input` is the op-specific payload.
    fn apply_row(row: &mut AccumulatorRow, bindings: &[AggregateBinding], input: &Self::Input);
}

/// Ingestion mode: raw upstream `Record` values arriving in verified
/// pre-sorted order on the group-by prefix.
#[derive(Debug, Default)]
pub struct AddRaw;

impl AccumulatorOp for AddRaw {
    type Input = Record;

    #[inline(always)]
    fn apply_row(row: &mut AccumulatorRow, bindings: &[AggregateBinding], input: &Self::Input) {
        // Fast path: index the record's value column per binding and
        // dispatch the accumulator's `add` directly. The
        // `BindingArg::Expr` / `BindingArg::Pair` general paths need
        // an `EvalContext` + `ProgramEvaluator`, which this narrow
        // trait signature intentionally does not plumb — callers that
        // hit those variants drop to the general `dispatch_binding`
        // path. This fast-path specialization is reserved for future
        // hot-loop optimization (DataFusion blog 2023-08-05 per-record
        // allocation pattern).
        for (binding, acc) in bindings.iter().zip(row.iter_mut()) {
            match &binding.arg {
                BindingArg::Field(idx) => {
                    let v = input
                        .values()
                        .get(*idx as usize)
                        .cloned()
                        .unwrap_or(Value::Null);
                    acc.add(&v);
                }
                BindingArg::Wildcard => {
                    acc.add(&Value::Null);
                }
                BindingArg::Expr(_) | BindingArg::Pair(_, _) => {
                    debug_assert!(
                        false,
                        "AddRaw::apply_row reached an Expr/Pair binding; \
                         streaming aggregator must dispatch these through \
                         the general `dispatch_binding` path"
                    );
                }
            }
        }
    }
}

/// Ingestion mode: merge pre-aggregated per-group state coming from a
/// spill file (or any upstream that already folded rows into an
/// `AggregatorGroupState`).
///
/// `Input` carries the **full** state so sidecar reductions (D57) and
/// the metadata common tracker (D11 revised) survive spill recovery
/// with byte-identical semantics to the in-memory path.
#[derive(Debug, Default)]
pub struct MergeState;

impl AccumulatorOp for MergeState {
    /// Encoded sort key (read by the LoserTree comparator) + the full
    /// per-group state. The sort key is produced by `SortKeyEncoder`
    /// and matches the declared sort-order direction.
    type Input = (Vec<u8>, AggregatorGroupState);

    #[inline(always)]
    fn apply_row(row: &mut AccumulatorRow, _bindings: &[AggregateBinding], input: &Self::Input) {
        // Merge the incoming `AccumulatorRow` into the currently-open
        // group's row slot-by-slot via `AccumulatorEnum::merge`. The
        // `min_row_num` lives on `AggregatorGroupState` and is merged
        // by [`crate::pipeline::streaming_merge::GroupBoundary::push`]
        // when it installs / folds the open group — `apply_row` stays
        // focused on accumulator-row reduction so the two merge
        // surfaces stay decoupled.
        let (_encoded_key, other_state) = input;
        for (a, b) in row.iter_mut().zip(other_state.row.iter()) {
            AccumulatorEnum::merge(a, b);
        }
    }
}

/// Associative merge of per-group `min_row_num` and per-row sidecars.
/// Operates on a currently-open group's `AggregatorGroupState` and
/// folds another state's state in.
///
/// Separated out so both the `GroupBoundary` state machine (for the
/// raw-streaming path) and the spill-recovery path can call the
/// identical reduction logic. Exposed at crate scope for tests.
pub(crate) fn merge_group_sidecars(dst: &mut AggregatorGroupState, src: AggregatorGroupState) {
    // `min_row_num`: monotonic min. `u64::MAX` is the identity.
    if src.min_row_num < dst.min_row_num {
        dst.min_row_num = src.min_row_num;
    }
    // `input_rows`: concatenate. The lineage path is the only producer
    // of non-empty `input_rows`; the strict path leaves both sides
    // empty and the extension is a no-op. `group_index` is left at
    // `dst`'s value — its post-merge identity is reassigned by the
    // recovery loop, so neither side's index is canonical here.
    dst.input_rows.extend(src.input_rows);
    dst.retract_values.extend(src.retract_values);
}

/// Concatenate two `BufferedGroupState` partials produced by separate
/// spill files for the same group key. Mirrors `merge_group_sidecars`
/// for the buffer-mode path.
pub(crate) fn merge_buffered_sidecars(dst: &mut BufferedGroupState, src: BufferedGroupState) {
    if src.min_row_num < dst.min_row_num {
        dst.min_row_num = src.min_row_num;
    }
    dst.input_rows.extend(src.input_rows);
    dst.contributions.extend(src.contributions);
}

/// Fold a `BufferedGroupState`'s contributions into a fresh
/// `AggregatorGroupState`, ready for `finalize_group_inner`. Each row
/// in `contributions` is shaped by the bindings: scalar arg variants
/// (`Field`, `Wildcard`, `Expr`) consume one slot; `Pair` consumes two
/// (value + weight) — matched against the binding shape so
/// `add_weighted` sees the original `(value, weight)` pair.
pub(super) fn fold_buffered_state(
    factory: &AccumulatorFactory,
    buffered: BufferedGroupState,
) -> Result<AggregatorGroupState, HashAggError> {
    let bindings = factory.compiled().bindings.clone();
    let mut row = factory.create_accumulators();
    for contrib in &buffered.contributions {
        let mut cursor = 0usize;
        for (binding, acc) in bindings.iter().zip(row.iter_mut()) {
            match &binding.arg {
                BindingArg::Pair(_, _) => {
                    let va = contrib.get(cursor).cloned().unwrap_or(Value::Null);
                    let vb = contrib.get(cursor + 1).cloned().unwrap_or(Value::Null);
                    cursor += 2;
                    acc.add_weighted(&va, &vb);
                }
                _ => {
                    let v = contrib.get(cursor).cloned().unwrap_or(Value::Null);
                    cursor += 1;
                    acc.add(&v);
                }
            }
        }
    }
    Ok(AggregatorGroupState {
        row,
        min_row_num: buffered.min_row_num,
        group_index: buffered.group_index,
        input_rows: buffered.input_rows,
        // `fold_buffered_state` is the buffer→folded conversion driver
        // that runs at finalize. Buffer-mode aggregators populate
        // `BufferedGroupState.contributions` directly; the lineage-only
        // `retract_values` cache is empty here because the resulting
        // folded state is consumed by `finalize_group_inner` rather
        // than re-handed to a retract pass.
        retract_values: Vec::new(),
    })
}

/// Boundary-merging aggregator monomorphized over an `AccumulatorOp`.
///
/// * `StreamingAggregator<AddRaw>` consumes raw upstream records that
///   arrive in verified sorted order on the group-by prefix, folds
///   each record into a per-group `AccumulatorEnum` row via the same
///   `dispatch_binding` hot loop used by `HashAggregator`, and emits a
///   finalized `SortRow` every time the group-key boundary advances.
/// * `StreamingAggregator<MergeState>` is retained as a forward-looking
///   stub for the out-of-core spill-recovery path that may consume
///   pre-aggregated partials. The spill-recovery path currently lives
///   in `HashAggregator::finalize_with_spill`, which is what the
///   executor actually uses; the `MergeState` inherent impl exists so
///   the type surface survives for future out-of-core work.
///
/// Both variants share the [`GroupBoundary`](crate::pipeline::streaming_merge::GroupBoundary)
/// state machine (DataFusion PR #4301 pattern), guaranteeing that all
/// three finalize paths — in-memory, spill-merge, and raw-streaming —
/// emit byte-identical `SortRow` values on identical inputs.
pub struct StreamingAggregator<Op: AccumulatorOp> {
    factory: AccumulatorFactory,
    output_schema: Arc<Schema>,
    group_by_indices: Vec<u32>,
    group_by_fields: Vec<String>,
    pre_agg_filter: Option<Expr>,
    evaluator: ProgramEvaluator,
    transform_name: String,
    rows_seen: u64,
    boundary: crate::pipeline::streaming_merge::GroupBoundary,
    /// Output buffer — drained on every `add_record` call into the
    /// caller-owned `out` vec. Retained as a field for the legacy
    /// `pending` fast-path code path.
    pending: Vec<SortRow>,
    _mode: std::marker::PhantomData<Op>,
}

impl StreamingAggregator<AddRaw> {
    /// Construct a raw-mode streaming aggregator.
    ///
    /// Accepts the same knobs as `HashAggregator::new` minus the spill
    /// plumbing (`StreamingAggregator<AddRaw>` never materializes a
    /// hash table, so no budget / spill-dir / spill schema is needed).
    pub fn new_for_raw(
        compiled: Arc<CompiledAggregate>,
        evaluator: ProgramEvaluator,
        output_schema: Arc<Schema>,
        transform_name: impl Into<String>,
    ) -> Self {
        let group_by_indices = compiled.group_by_indices.clone();
        let group_by_fields = compiled.group_by_fields.clone();
        let pre_agg_filter = compiled.pre_agg_filter.clone();
        let factory = AccumulatorFactory::new(compiled);
        let sort_fields = group_by_sort_fields(&group_by_fields, &output_schema);
        let encoder = crate::pipeline::sort_key::SortKeyEncoder::new(sort_fields);
        let boundary = crate::pipeline::streaming_merge::GroupBoundary::new(
            encoder,
            crate::pipeline::streaming_merge::StreamingErrorMode::UserInput,
        );
        Self {
            factory,
            output_schema,
            group_by_indices,
            group_by_fields,
            pre_agg_filter,
            evaluator,
            transform_name: transform_name.into(),
            rows_seen: 0,
            boundary,
            pending: Vec::new(),
            _mode: std::marker::PhantomData,
        }
    }

    /// Drive one input record through the streaming aggregator.
    ///
    /// 1. Apply the pre-aggregation filter (D9).
    /// 2. Extract the group-key tuple (D10).
    /// 3. Build a fresh per-group state seeded with a clone of the
    ///    factory prototype.
    /// 4. Fold each binding via `dispatch_binding` into that state.
    /// 5. Record the minimum `row_num` for the group so finalize
    ///    preserves the earliest input row's position.
    /// 6. Push into the shared `GroupBoundary` detector; on a key
    ///    boundary the previous group's finalized `SortRow` lands in
    ///    `self.pending`. A strictly lesser key is routed through
    ///    `HashAggError::SortOrderViolation` → hard abort.
    pub fn add_record(
        &mut self,
        record: &Record,
        row_num: u64,
        ctx: &EvalContext,
        out: &mut Vec<SortRow>,
    ) -> Result<(), HashAggError> {
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
        self.rows_seen = self.rows_seen.saturating_add(1);

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

        // Seed a fresh per-group state for this record. `GroupBoundary`
        // merges peer states if the key matches the currently-open
        // group, so we always construct a singleton and let the state
        // machine collapse it.
        let mut state = AggregatorGroupState::new(self.factory.create_accumulators());
        let bindings = self.factory.compiled().bindings.clone();
        for (binding, acc) in bindings.iter().zip(state.row.iter_mut()) {
            dispatch_binding(&binding.arg, acc, record, ctx, &self.evaluator)?;
        }

        // Seed `min_row_num` on the fresh per-group state; the boundary
        // detector's merge-peer path keeps the minimum across input
        // records folded into the same open group.
        state.min_row_num = row_num;

        // Encode the group-by columns into boundary.current via the
        // owned encoder. We feed it the input record directly.
        let mut scratch = std::mem::take(&mut self.boundary.current);
        self.boundary.encoder().encode_into(record, &mut scratch);
        self.boundary.current = scratch;

        // The finalize closure receives the previously-open input record
        // (held inside `GroupBoundary::open_record`) and re-extracts the
        // semantic group key from it via the same group-by indices used
        // above. No external shadow of the key is needed.
        let factory = &self.factory;
        let output_schema = &self.output_schema;
        let transform_name = self.transform_name.clone();
        let group_by_indices = self.group_by_indices.clone();
        let group_by_fields = self.group_by_fields.clone();
        let source_row = ctx.source_row;
        let finalize_closure =
            |rec: &Record, s: &AggregatorGroupState| -> Result<Record, HashAggError> {
                let mut k: Vec<GroupByKey> = Vec::with_capacity(group_by_indices.len());
                for (i, idx) in group_by_indices.iter().enumerate() {
                    let field_name = group_by_fields.get(i).map(String::as_str).unwrap_or("");
                    let val = rec
                        .values()
                        .get(*idx as usize)
                        .cloned()
                        .unwrap_or(Value::Null);
                    match value_to_group_key(&val, field_name, None, source_row) {
                        Ok(Some(gk)) => k.push(gk),
                        Ok(None) => k.push(GroupByKey::Null),
                        Err(e) => {
                            return Err(HashAggError::GroupKey {
                                field: field_name.to_string(),
                                row: source_row,
                                message: e.to_string(),
                            });
                        }
                    }
                }
                finalize_group_inner(factory, output_schema, &transform_name, &k, s)
            };

        // Push pending rows from the boundary directly into the
        // caller-owned `out` vec, then drain any pre-existing pending
        // rows from `self.pending` into `out` as well to preserve order.
        if !self.pending.is_empty() {
            out.append(&mut self.pending);
        }
        let _ = key;
        self.boundary
            .push(state, record.clone(), row_num, &finalize_closure, out)?;

        let _ = ctx; // ctx already used above
        Ok(())
    }

    /// Drain all emitted boundary rows plus the final open group.
    pub fn flush(mut self, _ctx: &EvalContext, out: &mut Vec<SortRow>) -> Result<(), HashAggError> {
        // Empty input + empty group_by → emit one defaulted global-fold
        // row.
        if self.rows_seen == 0 && self.group_by_indices.is_empty() {
            let record =
                empty_global_fold_row(&self.factory, &self.output_schema, &self.transform_name)?;
            out.push((record, 0));
            return Ok(());
        }
        if !self.pending.is_empty() {
            out.append(&mut self.pending);
        }
        let factory = self.factory;
        let output_schema = self.output_schema;
        let transform_name = self.transform_name;
        let group_by_indices = self.group_by_indices.clone();
        let group_by_fields = self.group_by_fields.clone();
        let finalize_closure =
            move |rec: &Record, s: &AggregatorGroupState| -> Result<Record, HashAggError> {
                let mut k: Vec<GroupByKey> = Vec::with_capacity(group_by_indices.len());
                for (i, idx) in group_by_indices.iter().enumerate() {
                    let field_name = group_by_fields.get(i).map(String::as_str).unwrap_or("");
                    let val = rec
                        .values()
                        .get(*idx as usize)
                        .cloned()
                        .unwrap_or(Value::Null);
                    match value_to_group_key(&val, field_name, None, 0) {
                        Ok(Some(gk)) => k.push(gk),
                        Ok(None) => k.push(GroupByKey::Null),
                        Err(e) => {
                            return Err(HashAggError::GroupKey {
                                field: field_name.to_string(),
                                row: 0,
                                message: e.to_string(),
                            });
                        }
                    }
                }
                finalize_group_inner(&factory, &output_schema, &transform_name, &k, s)
            };
        self.boundary.flush(&finalize_closure, out)?;
        Ok(())
    }
}

impl StreamingAggregator<MergeState> {
    /// Construct a merge-mode streaming aggregator. Retained as a
    /// forward-compat stub for future out-of-core merge recovery; the
    /// executor's spill-recovery path currently lives inside
    /// `HashAggregator::finalize_with_spill`.
    pub fn new_for_merge(
        compiled: Arc<CompiledAggregate>,
        evaluator: ProgramEvaluator,
        output_schema: Arc<Schema>,
        transform_name: impl Into<String>,
    ) -> Self {
        let group_by_indices = compiled.group_by_indices.clone();
        let group_by_fields = compiled.group_by_fields.clone();
        let pre_agg_filter = compiled.pre_agg_filter.clone();
        let factory = AccumulatorFactory::new(compiled);
        let sort_fields = group_by_sort_fields(&group_by_fields, &output_schema);
        let encoder = crate::pipeline::sort_key::SortKeyEncoder::new(sort_fields);
        let boundary = crate::pipeline::streaming_merge::GroupBoundary::new(
            encoder,
            crate::pipeline::streaming_merge::StreamingErrorMode::SpillMerge,
        );
        Self {
            factory,
            output_schema,
            group_by_indices,
            group_by_fields,
            pre_agg_filter,
            evaluator,
            transform_name: transform_name.into(),
            rows_seen: 0,
            boundary,
            pending: Vec::new(),
            _mode: std::marker::PhantomData,
        }
    }
}

impl<Op: AccumulatorOp> StreamingAggregator<Op> {
    /// Debug-inspect accessor used by the structural O(1) memory test
    /// and by the Kiln debugger's streaming-agg state overlay. Returns
    /// 1 when a per-group state is currently open
    /// (between key boundaries), 0 when no group is open (before the
    /// first record or immediately after a flush).
    ///
    /// The "row count" framing matches the spec; in practice the
    /// streaming aggregator never holds more than a single open per-group
    /// state regardless of input size, so the value is structurally
    /// bounded to {0, 1} — that bound IS the O(1) memory invariant.
    pub fn current_row_count(&self) -> usize {
        if self.boundary.is_group_open() { 1 } else { 0 }
    }
}

#[cfg(test)]
mod accumulator_op_tests {
    use super::*;
    use clinker_record::accumulator::{AccumulatorEnum, AggregateType};

    fn state_with_row(row: AccumulatorRow) -> AggregatorGroupState {
        AggregatorGroupState::new(row)
    }

    // ----- merge_group_sidecars -----

    #[test]
    fn test_merge_sidecars_min_row_num_is_min() {
        let mut dst = state_with_row(Vec::new());
        dst.min_row_num = 10;
        let mut src = state_with_row(Vec::new());
        src.min_row_num = 3;
        merge_group_sidecars(&mut dst, src);
        assert_eq!(dst.min_row_num, 3);
    }

    #[test]
    fn test_merge_sidecars_min_row_num_identity_is_u64_max() {
        let mut dst = state_with_row(Vec::new());
        dst.min_row_num = 7;
        let src = state_with_row(Vec::new()); // min_row_num = u64::MAX
        merge_group_sidecars(&mut dst, src);
        assert_eq!(dst.min_row_num, 7, "u64::MAX must act as identity");
    }

    // ----- AccumulatorOp::apply_row: AddRaw -----

    #[test]
    fn test_addraw_apply_row_field_binding_sums_values() {
        // Build a single-binding row: sum(values[0]).
        let bindings = vec![AggregateBinding {
            output_name: "sum_x".into(),
            arg: BindingArg::Field(0),
            acc_type: AggregateType::Sum,
        }];
        let mut row: AccumulatorRow = vec![AccumulatorEnum::for_type(&AggregateType::Sum)];

        let schema = Arc::new(clinker_record::Schema::new(vec!["x".into()]));
        let r1 = Record::new(Arc::clone(&schema), vec![Value::Integer(10)]);
        let r2 = Record::new(Arc::clone(&schema), vec![Value::Integer(32)]);

        <AddRaw as AccumulatorOp>::apply_row(&mut row, &bindings, &r1);
        <AddRaw as AccumulatorOp>::apply_row(&mut row, &bindings, &r2);

        let v = row[0].finalize().expect("finalize");
        assert_eq!(v, Value::Integer(42));
    }

    // ----- AccumulatorOp::apply_row: MergeState -----

    #[test]
    fn test_mergestate_apply_row_merges_accumulator_rows() {
        // Two Sum accumulators, one pre-loaded with 10 and the other
        // with 32. MergeState::apply_row must merge slot-by-slot so
        // the destination holds 42. The D57 sidecars travel on the
        // `AggregatorGroupState` and are exercised separately via
        // `merge_group_sidecars`.
        let mut dst_acc = AccumulatorEnum::for_type(&AggregateType::Sum);
        dst_acc.add(&Value::Integer(10));
        let mut row_dst: AccumulatorRow = vec![dst_acc];

        let mut src_acc = AccumulatorEnum::for_type(&AggregateType::Sum);
        src_acc.add(&Value::Integer(32));
        let src_row: AccumulatorRow = vec![src_acc];
        let src_state = state_with_row(src_row);

        let input: (Vec<u8>, AggregatorGroupState) = (vec![0xAA, 0xBB], src_state);
        <MergeState as AccumulatorOp>::apply_row(&mut row_dst, &[], &input);

        let v = row_dst[0].finalize().expect("finalize");
        assert_eq!(v, Value::Integer(42));
    }

    #[test]
    fn test_mergestate_input_type_carries_full_group_state() {
        // Compile-time proof that the `Input` associated type is the
        // full `(Vec<u8>, AggregatorGroupState)`. If `MergeState::Input`
        // ever regresses to `(Vec<u8>, AccumulatorRow)` this test stops
        // compiling.
        fn assert_input_is_full_state(_: &<MergeState as AccumulatorOp>::Input) {}
        let st = state_with_row(Vec::new());
        let input: (Vec<u8>, AggregatorGroupState) = (Vec::new(), st);
        assert_input_is_full_state(&input);
        // Confirm `min_row_num` is addressable on the `Input` type —
        // if it is missing this line fails to compile.
        let _ = input.1.min_row_num;
    }
}
