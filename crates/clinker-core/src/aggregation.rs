//! Hash + streaming aggregation engine for Phase 16.
//!
//! Task 16.3.7 introduces the runtime types referenced by
//! `PlanNode::Aggregation`'s executor dispatch (16.3.13):
//!
//! * [`AggregateStrategy`] — plan-time enum (also surfaced on the plan
//!   node) selecting the per-group hash table or the streaming
//!   single-group fold.
//! * [`AggregateInput`] — dual input mode: a freshly read input record
//!   or a previously spilled per-group state being recovered.
//! * [`AccumulatorFactory`] — clones a per-group prototype
//!   `AccumulatorRow` from the plan-time `CompiledAggregate`'s
//!   bindings, avoiding repeat factory dispatch on every new key.
//! * [`AggregateEvalScope`] / [`eval_expr_in_agg_scope`] — finalize-time
//!   evaluator that resolves `Expr::AggSlot` to a finalized
//!   accumulator slot value and `Expr::GroupKey` to a group-key column
//!   value.
//!
//! The full `HashAggregator`, `MetadataCommonTracker`, spill paths,
//! and executor dispatch arm land in Tasks 16.3.8–16.3.13 as part of
//! the same atomic 16.3 commit.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use std::collections::HashSet;
use std::path::PathBuf;

use clinker_record::accumulator::{AccumulatorEnum, AccumulatorError, AccumulatorRow};
use clinker_record::group_key::value_to_group_key;
use clinker_record::schema::Schema;
use clinker_record::{GroupByKey, Record, RecordStorage, Value};
use cxl::ast::{BinOp, Expr, LiteralValue, UnaryOp};
use cxl::eval::{EvalContext, EvalError, ProgramEvaluator, eval_expr};
use cxl::plan::{AggregateBinding, BindingArg, CompiledAggregate};
use indexmap::IndexMap;

use crate::config::{NullOrder, SortField, SortOrder};
use crate::error::PipelineError;
use crate::pipeline::loser_tree::{LoserTree, MergeEntry};
use crate::pipeline::sort_key::encode_sort_key;
use crate::pipeline::spill::{SpillFile, SpillWriter};

/// Local stand-in to satisfy `eval_expr`'s `S: RecordStorage` type
/// parameter when no window context is in play. The aggregator hot loop
/// resolves field references through the input `Record` (which
/// implements `FieldResolver`); the storage parameter is unused.
struct NullStorage;
impl RecordStorage for NullStorage {
    fn resolve_field(&self, _: u32, _: &str) -> Option<Value> {
        None
    }
    fn resolve_qualified(&self, _: u32, _: &str, _: &str) -> Option<Value> {
        None
    }
    fn available_fields(&self, _: u32) -> Vec<&str> {
        Vec::new()
    }
    fn record_count(&self) -> u32 {
        0
    }
}

/// Aggregation strategy selected at plan-compile time.
///
/// `Hash` is the universal default. `Streaming` is wired by Task 16.4
/// when the input is provably sorted on the full group-by prefix —
/// allowing a one-group-at-a-time fold that never materializes a hash
/// table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateStrategy {
    Hash,
    Streaming,
}

/// Input to an aggregator's `add` path. Live input records and
/// recovered spilled state both flow through the same dispatch so the
/// merge phase (Task 16.3.11) can reuse the hash table without a
/// separate code path.
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

/// Errors that can arise while evaluating a residual in aggregate
/// scope. The full taxonomy lands with Task 16.3.13 (`PipelineError`
/// integration); for the 16.3.7 surface area we need just enough to
/// compile.
#[derive(Debug, Clone)]
pub enum AggregateEvalError {
    /// `Expr::AggSlot { slot }` referenced a slot index outside the
    /// finalized slot vector. Indicates an extractor / executor
    /// mismatch — should be unreachable in a correctly compiled plan.
    SlotOutOfRange { slot: u32, slot_count: usize },
    /// `Expr::GroupKey { slot }` referenced a group-key column outside
    /// the key tuple.
    GroupKeyOutOfRange { slot: u32, key_count: usize },
    /// A residual contained a construct that the finalize-time
    /// evaluator does not yet support. Task 16.3.12 expands the
    /// supported surface area; until then any unsupported expression
    /// is reported via this variant rather than panicking.
    UnsupportedResidual { what: &'static str },
}

impl std::fmt::Display for AggregateEvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateEvalError::SlotOutOfRange { slot, slot_count } => write!(
                f,
                "AggSlot {{ slot: {slot} }} out of range (slot_count: {slot_count})"
            ),
            AggregateEvalError::GroupKeyOutOfRange { slot, key_count } => write!(
                f,
                "GroupKey {{ slot: {slot} }} out of range (key_count: {key_count})"
            ),
            AggregateEvalError::UnsupportedResidual { what } => {
                write!(f, "unsupported aggregate residual construct: {what}")
            }
        }
    }
}

impl std::error::Error for AggregateEvalError {}

/// Evaluate a post-extraction residual expression in aggregate scope.
///
/// Resolves [`Expr::AggSlot`] / [`Expr::GroupKey`] leaves directly
/// against `scope` and walks arithmetic / comparison / logical /
/// coalesce / `if` / unary nodes recursively. Constructs that need a
/// row context (`FieldRef`, `MethodCall`, `WindowCall`, `Match`,
/// metadata access) are surfaced as
/// [`AggregateEvalError::UnsupportedResidual`] until Task 16.3.12
/// extends the surface area in lockstep with the executor dispatch.
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
        Expr::MetaAccess { .. } | Expr::PipelineAccess { .. } => {
            Err(AggregateEvalError::UnsupportedResidual {
                what: "$meta/$pipeline access",
            })
        }
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
// MetadataCommonTracker (Task 16.3.8a, Decision D11 revised)
// ---------------------------------------------------------------------------

/// Per-key state inside a [`MetadataCommonTracker`].
///
/// `value` holds the common value seen for this key as long as every
/// observation has agreed; once a conflicting observation arrives the
/// slot transitions to `conflicting = true` and `value` is cleared. The
/// slot itself is retained so the merge path remains associative across
/// spill recovery — clearing the slot would otherwise let a
/// later-merged partial revive a value that an earlier observation had
/// already invalidated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommonState {
    pub value: Option<Value>,
    pub conflicting: bool,
}

/// Per-group "Keep Only Common Attributes" metadata tracker (NiFi
/// MergeContent default semantics).
///
/// The hash aggregator carries one of these per group-key. On every
/// input record it walks the record's metadata map and calls
/// [`MetadataCommonTracker::observe`] for every key that the user has
/// not explicitly emitted via `emit $meta.X = ...`. At finalize time,
/// keys whose `CommonState` is `!conflicting && value.is_some()` are
/// emitted; everything else is dropped (visible by absence).
///
/// The tracker is serde-derived because it travels with the per-group
/// `AccumulatorRow` through the spill path — see Task 16.3.10.
/// Recovery merges partial trackers via [`MetadataCommonTracker::merge`]
/// which is associative: any disagreement on either side, or any
/// already-`conflicting` slot, transitions the merged slot to
/// conflicting.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetadataCommonTracker {
    pub keys: HashMap<Box<str>, CommonState>,
}

/// Outcome of a single [`MetadataCommonTracker::observe`] call.
///
/// Both pieces of information are needed by the hash aggregator hot
/// loop: `heap_delta` is folded into `value_heap_bytes` for memory
/// accounting, while `became_conflict` triggers a one-shot structured
/// WARN log via `meta_conflict_logged`. The phase-16 spec quotes both
/// behaviors but is internally inconsistent on a single return type;
/// the struct surfaces them together to satisfy both call sites without
/// a hidden second lookup.
#[derive(Debug, Clone, Copy, Default)]
pub struct ObserveResult {
    /// Bytes added to `value_heap_bytes` by this observation. Always
    /// non-negative — when an equal-value observation transitions a key
    /// to conflicting, the slot is retained (so its `CommonState`
    /// footprint stays charged) and the cleared `Value`'s heap bytes
    /// are reported as `0` rather than as a negative delta. The hash
    /// aggregator's spill trigger watches the totals through a separate
    /// resize-aware check; precise reclamation is unnecessary here.
    pub heap_delta: usize,
    /// `true` iff this observation flipped the key's `CommonState` from
    /// non-conflicting to conflicting. The aggregator emits one
    /// structured WARN per (transform, key) on the first such flip.
    pub became_conflict: bool,
}

impl MetadataCommonTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Observe one `(key, value)` pair from an input record's metadata.
    pub fn observe(&mut self, key: &str, value: &Value) -> ObserveResult {
        match self.keys.get_mut(key) {
            None => {
                let delta = key.len()
                    + std::mem::size_of::<CommonState>()
                    + std::mem::size_of::<Value>()
                    + value.heap_size();
                self.keys.insert(
                    key.into(),
                    CommonState {
                        value: Some(value.clone()),
                        conflicting: false,
                    },
                );
                ObserveResult {
                    heap_delta: delta,
                    became_conflict: false,
                }
            }
            Some(state) if state.conflicting => ObserveResult::default(),
            Some(state) => {
                // Non-conflicting slot. Compare against the stored value.
                let agrees = matches!(state.value.as_ref(), Some(v) if v == value);
                if agrees {
                    ObserveResult::default()
                } else {
                    state.value = None;
                    state.conflicting = true;
                    ObserveResult {
                        heap_delta: 0,
                        became_conflict: true,
                    }
                }
            }
        }
    }

    /// Associative merge for spill recovery.
    pub fn merge(&mut self, other: MetadataCommonTracker) {
        for (k, other_state) in other.keys {
            match self.keys.get_mut(k.as_ref()) {
                None => {
                    self.keys.insert(k, other_state);
                }
                Some(self_state) => {
                    if self_state.conflicting || other_state.conflicting {
                        self_state.value = None;
                        self_state.conflicting = true;
                    } else {
                        let agrees = matches!(
                            (self_state.value.as_ref(), other_state.value.as_ref()),
                            (Some(a), Some(b)) if a == b
                        );
                        if !agrees {
                            self_state.value = None;
                            self_state.conflicting = true;
                        }
                    }
                }
            }
        }
    }

    /// Heap footprint of every retained key + every retained `Value`.
    /// Charged into `HashAggregator::value_heap_bytes` for spill-trigger
    /// accounting (parity with `CollectState::heap_size`).
    pub fn heap_size(&self) -> usize {
        self.keys
            .iter()
            .map(|(k, s)| {
                k.len()
                    + std::mem::size_of::<CommonState>()
                    + std::mem::size_of::<Value>()
                    + s.value.as_ref().map(Value::heap_size).unwrap_or(0)
            })
            .sum()
    }

    /// Drain non-conflicting key/value pairs at finalize time. Conflicting
    /// or empty slots are filtered out (visible-by-absence semantics).
    pub fn finalize(self) -> Vec<(Box<str>, Value)> {
        self.keys
            .into_iter()
            .filter_map(|(k, s)| match (s.conflicting, s.value) {
                (false, Some(v)) => Some((k, v)),
                _ => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tracker_tests {
    use super::*;

    fn s(v: &str) -> Value {
        Value::String(v.into())
    }

    #[test]
    fn first_observation_inserts_with_positive_delta() {
        let mut t = MetadataCommonTracker::new();
        let r = t.observe("source_file", &s("a.csv"));
        assert!(r.heap_delta > 0);
        assert!(!r.became_conflict);
        assert_eq!(t.keys.len(), 1);
        assert_eq!(t.keys["source_file"].value.as_ref(), Some(&s("a.csv")));
        assert!(!t.keys["source_file"].conflicting);
    }

    #[test]
    fn equal_observation_is_noop() {
        let mut t = MetadataCommonTracker::new();
        t.observe("k", &s("v"));
        let r = t.observe("k", &s("v"));
        assert_eq!(r.heap_delta, 0);
        assert!(!r.became_conflict);
        assert!(!t.keys["k"].conflicting);
    }

    #[test]
    fn conflict_transitions_clear_value_and_flag() {
        let mut t = MetadataCommonTracker::new();
        t.observe("k", &s("a"));
        let r = t.observe("k", &s("b"));
        assert_eq!(r.heap_delta, 0);
        assert!(r.became_conflict);
        assert!(t.keys["k"].conflicting);
        assert!(t.keys["k"].value.is_none());
        // A subsequent observation on a conflicting slot is a no-op and
        // does NOT re-flag.
        let r2 = t.observe("k", &s("c"));
        assert_eq!(r2.heap_delta, 0);
        assert!(!r2.became_conflict);
    }

    #[test]
    fn finalize_drops_conflicting_keys() {
        let mut t = MetadataCommonTracker::new();
        t.observe("keep", &s("v"));
        t.observe("keep", &s("v"));
        t.observe("drop", &s("a"));
        t.observe("drop", &s("b"));
        let mut out = t.finalize();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(out, vec![("keep".into(), s("v"))]);
    }

    #[test]
    fn test_metadata_common_tracker_serde_roundtrip() {
        let mut t = MetadataCommonTracker::new();
        t.observe("source", &s("a.csv"));
        t.observe("clash", &s("x"));
        t.observe("clash", &s("y"));
        let json = serde_json::to_string(&t).unwrap();
        let back: MetadataCommonTracker = serde_json::from_str(&json).unwrap();
        assert_eq!(back.keys["source"].value.as_ref(), Some(&s("a.csv")));
        assert!(!back.keys["source"].conflicting);
        assert!(back.keys["clash"].conflicting);
        assert!(back.keys["clash"].value.is_none());
    }

    #[test]
    fn test_metadata_common_tracker_merge_associative() {
        // Build three partials A, B, C and verify (A∪B)∪C == A∪(B∪C)
        // both structurally and that any conflict in any operand
        // propagates to the result.
        let build = |pairs: &[(&str, &str)]| -> MetadataCommonTracker {
            let mut t = MetadataCommonTracker::new();
            for (k, v) in pairs {
                t.observe(k, &s(v));
            }
            t
        };

        // A: {src=a, region=us}
        // B: {src=a, env=prod}        — agrees with A on src
        // C: {src=b, region=us}       — disagrees with A on src
        let a = build(&[("src", "a"), ("region", "us")]);
        let b = build(&[("src", "a"), ("env", "prod")]);
        let c = build(&[("src", "b"), ("region", "us")]);

        let mut left = a.clone();
        left.merge(b.clone());
        left.merge(c.clone());

        let mut right_inner = b;
        right_inner.merge(c);
        let mut right = a;
        right.merge(right_inner);

        // src must be conflicting on both sides; region kept; env kept.
        for t in [&left, &right] {
            assert!(t.keys["src"].conflicting, "src should be conflicting");
            assert!(t.keys["src"].value.is_none());
            assert_eq!(t.keys["region"].value.as_ref(), Some(&s("us")));
            assert!(!t.keys["region"].conflicting);
            assert_eq!(t.keys["env"].value.as_ref(), Some(&s("prod")));
            assert!(!t.keys["env"].conflicting);
        }
    }

    #[test]
    fn heap_size_grows_with_inserted_keys() {
        let mut t = MetadataCommonTracker::new();
        let h0 = t.heap_size();
        t.observe("k1", &s("hello"));
        let h1 = t.heap_size();
        assert!(h1 > h0);
        // Conflict transition retains the slot but clears the value:
        // size should not exceed the post-insertion size.
        t.observe("k1", &s("world"));
        let h2 = t.heap_size();
        assert!(h2 <= h1);
    }
}

// ---------------------------------------------------------------------------
// HashAggregator (Task 16.3.8)
// ---------------------------------------------------------------------------

/// Per-group state held inside the hash table.
///
/// `row` is the row of accumulators (one slot per `AggregateBinding`)
/// being driven by the hot loop. `meta_tracker` is the per-group
/// "Keep Only Common Attributes" propagator (D11 revised) that
/// observes every input record's metadata so finalize can emit
/// non-conflicting keys without per-key user wiring.
#[derive(Debug, Clone)]
pub struct AggregatorGroupState {
    pub row: AccumulatorRow,
    pub meta_tracker: MetadataCommonTracker,
    /// D57 sidecar — minimum `row_num` across every input record folded
    /// into this group. Initialized to `u64::MAX`; the executor stores the
    /// reduced value into the SortRow tuple at finalize time. The
    /// global-fold empty-input case (D12) emits `0` because no record
    /// ever updates this field.
    pub min_row_num: u64,
    /// D57 sidecar — intersection of every record's `emitted` IndexMap
    /// (the executor's per-record "accumulated emitted" sidecar). `None`
    /// before the first record; `Some(...)` after, narrowing on each
    /// add. Empty IndexMap means "no common keys".
    pub common_emitted: Option<IndexMap<String, Value>>,
    /// D57 sidecar — union of every record's `accumulated` IndexMap
    /// (the executor's per-record "accumulated metadata" sidecar). On
    /// key conflicts the **first** value wins (insertion order); this
    /// matches the existing `MetadataCommonTracker` first-seen semantics.
    pub union_accumulated: IndexMap<String, Value>,
}

impl AggregatorGroupState {
    pub(crate) fn new(row: AccumulatorRow) -> Self {
        Self {
            row,
            meta_tracker: MetadataCommonTracker::new(),
            min_row_num: u64::MAX,
            common_emitted: None,
            union_accumulated: IndexMap::new(),
        }
    }
}

/// D57 helper — narrow `prev` to keys present in `incoming` whose values
/// match. Used to fold the per-record `emitted` sidecar into a per-group
/// intersection.
fn intersect_emitted(
    prev: IndexMap<String, Value>,
    incoming: &IndexMap<String, Value>,
) -> IndexMap<String, Value> {
    let mut out = IndexMap::with_capacity(prev.len().min(incoming.len()));
    for (k, v) in prev {
        if let Some(other) = incoming.get(&k)
            && other == &v
        {
            out.insert(k, v);
        }
    }
    out
}

/// D57 helper — first-seen union: keep `dst` values where they exist,
/// insert from `src` only when the key is missing. Mirrors the
/// `MetadataCommonTracker` first-seen-wins precedent.
fn union_accumulated_into(dst: &mut IndexMap<String, Value>, src: &IndexMap<String, Value>) {
    for (k, v) in src {
        if !dst.contains_key(k) {
            dst.insert(k.clone(), v.clone());
        }
    }
}

/// Per-node-buffer row produced by every executor node that emits into
/// `node_buffers`. Mirrors the local 4-tuple type alias used by
/// `PlanNode::Sort` and `execute_dag_branching`.
pub type SortRow = (
    Record,
    u64,
    IndexMap<String, Value>,
    IndexMap<String, Value>,
);

/// Errors raised by the hash aggregator hot loop. The 16.3.13 dispatch
/// arm wraps these into `PipelineError` for DLQ routing; until then the
/// engine surfaces them via this dedicated enum so the runtime stays
/// decoupled from the executor's error taxonomy.
#[derive(Debug)]
pub enum HashAggError {
    /// A `BindingArg::Expr` failed to evaluate against the input record.
    EvalFailed(EvalError),
    /// `value_to_group_key` rejected an input value (NaN, unsupported
    /// type, etc.). Carries the field name and row number for routing
    /// in the executor dispatch path.
    GroupKey {
        field: String,
        row: u32,
        message: String,
    },
    /// Spill path raised an I/O error. Stub until 16.3.10 lands the
    /// real spill writer integration.
    Spill(String),
    /// An accumulator's `finalize()` failed (e.g. `SumOverflow`). Routed
    /// to `PipelineError::Accumulator` by the 16.3.13 dispatch arm.
    Accumulator {
        transform: String,
        binding: String,
        source: AccumulatorError,
    },
    /// A post-extraction emit residual failed to evaluate in
    /// `AggregateEvalScope`. Indicates an extractor/executor mismatch or
    /// an unsupported residual construct.
    Residual(AggregateEvalError),
    /// Streaming aggregation observed an input record whose group-by key
    /// sorts strictly before the currently-open group, violating the
    /// pre-sorted-input invariant of `StreamingAggregator<AddRaw>`. This
    /// is always a hard abort regardless of error strategy — it means
    /// either the upstream ordering contract was wrong or the plan
    /// property pass qualified streaming aggregation on a node whose
    /// output was not actually sorted (DataFusion #12086-class bug).
    /// Phase 16 Task 16.4.3 — widened from `{ message: String }` so the
    /// `GroupBoundary` can hand the executor both pre/next encoded keys
    /// for diagnostics. The user-input vs spill-merge distinction is
    /// captured in two separate variants because the two cases route
    /// differently through `From<HashAggError> for PipelineError`.
    SortOrderViolation {
        prev_key_debug: String,
        next_key_debug: String,
    },
    /// Spill-merge produced an out-of-order key — Clinker bug, not a
    /// user data error. Always hard-aborts. Phase 16 Task 16.4.3.
    MergeSortOrderViolation {
        prev_key_debug: String,
        next_key_debug: String,
    },
}

impl std::fmt::Display for HashAggError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HashAggError::EvalFailed(e) => write!(f, "binding expression eval failed: {e:?}"),
            HashAggError::GroupKey {
                field,
                row,
                message,
            } => write!(
                f,
                "group-key extraction failed for `{field}` at row {row}: {message}"
            ),
            HashAggError::Spill(msg) => write!(f, "spill failed: {msg}"),
            HashAggError::Accumulator {
                transform,
                binding,
                source,
            } => write!(
                f,
                "aggregate {transform}.{binding}: accumulator finalize failed: {source:?}"
            ),
            HashAggError::Residual(e) => write!(f, "aggregate residual eval failed: {e}"),
            HashAggError::SortOrderViolation {
                prev_key_debug,
                next_key_debug,
            } => write!(
                f,
                "streaming aggregate sort-order violation: prev={prev_key_debug} next={next_key_debug}"
            ),
            HashAggError::MergeSortOrderViolation {
                prev_key_debug,
                next_key_debug,
            } => write!(
                f,
                "spill-merge sort-order violation (Clinker bug): prev={prev_key_debug} next={next_key_debug}"
            ),
        }
    }
}

impl std::error::Error for HashAggError {}

impl From<EvalError> for HashAggError {
    fn from(e: EvalError) -> Self {
        HashAggError::EvalFailed(e)
    }
}

/// Map a `HashAggError` to a `PipelineError` for the executor dispatch
/// arm. Accumulator-finalize errors get a dedicated typed variant; the
/// remaining cases are wrapped in `PipelineError::Eval` (data errors) or
/// `PipelineError::Internal` (engine bugs / unsupported residuals).
impl From<HashAggError> for PipelineError {
    fn from(e: HashAggError) -> Self {
        match e {
            HashAggError::EvalFailed(eval) => PipelineError::Eval(eval),
            HashAggError::GroupKey {
                field,
                row,
                message,
            } => PipelineError::Internal {
                op: "aggregation",
                node: String::new(),
                detail: format!("group-key field `{field}` at row {row}: {message}"),
            },
            HashAggError::Spill(msg) => PipelineError::Internal {
                op: "aggregation",
                node: String::new(),
                detail: format!("spill failed: {msg}"),
            },
            HashAggError::Accumulator {
                transform,
                binding,
                source,
            } => PipelineError::Accumulator {
                transform,
                binding,
                source,
            },
            HashAggError::Residual(r) => PipelineError::Internal {
                op: "aggregation",
                node: String::new(),
                detail: format!("aggregate residual eval failed: {r}"),
            },
            HashAggError::SortOrderViolation {
                prev_key_debug,
                next_key_debug,
            } => PipelineError::SortOrderViolation {
                message: format!(
                    "streaming aggregation requires sorted input; prev={prev_key_debug} next={next_key_debug}"
                ),
            },
            HashAggError::MergeSortOrderViolation {
                prev_key_debug,
                next_key_debug,
            } => PipelineError::MergeSortOrderViolation {
                message: format!(
                    "internal Clinker bug — LoserTree produced out-of-order keys: prev={prev_key_debug} next={next_key_debug}"
                ),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// AggregateStream wrapper enum (Task 16.3.13)
// ---------------------------------------------------------------------------

/// Executor-facing aggregation dispatch wrapper.
///
/// Single variant in 16.3.13. The `#[non_exhaustive]` attribute lets
/// Task 16.4 add a `Streaming(StreamingAggregator<AddRaw>)` variant
/// non-breakingly, in the same commit that introduces the real
/// `AccumulatorOp` / `AddRaw` types — avoiding the DataFusion #12086
/// failure mode of placeholder generic parameters in dispatch enums.
#[non_exhaustive]
pub enum AggregateStream {
    Hash(Box<HashAggregator>),
    /// Streaming aggregation over a pre-sorted upstream. Landed in
    /// Phase 16 Task 16.4.0 alongside the real `AccumulatorOp` /
    /// `AddRaw` types and the shared streaming-merge module.
    Streaming(Box<StreamingAggregator<AddRaw>>),
}

impl AggregateStream {
    /// Construct the stream for a single `PlanNode::Aggregation` node.
    ///
    /// Streaming strategy is rejected with a fallible
    /// `PipelineError::Internal` (NOT `todo!()` / `unreachable!()`) per
    /// the DataFusion #12086 lesson on unreachable arms in long-lived
    /// executor dispatch tables. The real `Streaming` arm lands in Task
    /// 16.4 alongside its inner `StreamingAggregator<AddRaw>`.
    #[allow(clippy::too_many_arguments)]
    pub fn for_node(
        compiled: Arc<CompiledAggregate>,
        evaluator: ProgramEvaluator,
        strategy: AggregateStrategy,
        output_schema: Arc<Schema>,
        spill_schema: Arc<Schema>,
        spill_sort_fields: Vec<SortField>,
        memory_budget: usize,
        spill_dir: Option<PathBuf>,
        transform_name: String,
    ) -> Result<Self, PipelineError> {
        let _ = (&spill_schema, &spill_sort_fields, memory_budget, &spill_dir);
        match strategy {
            AggregateStrategy::Hash => Ok(Self::Hash(Box::new(HashAggregator::new(
                compiled,
                evaluator,
                output_schema,
                spill_schema,
                spill_sort_fields,
                memory_budget,
                spill_dir,
                transform_name,
            )))),
            AggregateStrategy::Streaming => {
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
    /// crossed by this record. Phase 16 Task 16.4.3 (D-α/D-γ debt).
    #[allow(clippy::too_many_arguments)]
    pub fn add_record(
        &mut self,
        record: &Record,
        row_num: u64,
        emitted: &IndexMap<String, Value>,
        accumulated: &IndexMap<String, Value>,
        ctx: &EvalContext,
        out: &mut Vec<SortRow>,
    ) -> Result<(), HashAggError> {
        match self {
            Self::Hash(h) => {
                let _ = &out; // Hash defers emission to finalize.
                h.add_record(record, row_num, emitted, accumulated, ctx)
            }
            Self::Streaming(s) => s.add_record(record, row_num, emitted, accumulated, ctx, out),
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
    spill_files: Vec<SpillFile<()>>,
    spill_schema: Arc<Schema>,
    #[allow(dead_code)]
    spill_sort_fields: Vec<SortField>,
    spill_dir: Option<PathBuf>,
    #[allow(dead_code)]
    output_schema: Arc<Schema>,
    transform_name: String,
    evaluator: ProgramEvaluator,
    /// User-emitted `$meta.X` keys — these are skipped by the auto
    /// common-only tracker because the user has taken explicit control
    /// (D11 revised). Built once at construction from `compiled.emits`.
    explicit_meta_keys: HashSet<Box<str>>,
    /// One-shot WARN dedup for metadata conflict logging. Per-pipeline-run
    /// scope (D11 revised).
    meta_conflict_logged: HashSet<Box<str>>,
    /// Source-row counter — incremented after the pre-agg filter accepts
    /// a record. Used by the global-fold empty-input special case in
    /// `finalize` (D12, D44).
    rows_seen: u64,
}

impl HashAggregator {
    /// Construct a new aggregator from a compiled aggregate plan.
    ///
    /// `spill_schema`, `spill_sort_fields`, and `spill_dir` are wired
    /// for the spill path that lands in 16.3.10. The 16.3.8 hot loop
    /// only touches them through the `value_heap_bytes` budget check.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        compiled: Arc<CompiledAggregate>,
        evaluator: ProgramEvaluator,
        output_schema: Arc<Schema>,
        spill_schema: Arc<Schema>,
        spill_sort_fields: Vec<SortField>,
        memory_budget: usize,
        spill_dir: Option<PathBuf>,
        transform_name: impl Into<String>,
    ) -> Self {
        let group_by_indices = compiled.group_by_indices.clone();
        let group_by_fields = compiled.group_by_fields.clone();
        let pre_agg_filter = compiled.pre_agg_filter.clone();
        let explicit_meta_keys: HashSet<Box<str>> = compiled
            .emits
            .iter()
            .filter(|e| e.is_meta)
            .map(|e| e.output_name.clone())
            .collect();
        let factory = AccumulatorFactory::new(compiled);
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
            spill_sort_fields,
            spill_dir,
            output_schema,
            transform_name: transform_name.into(),
            evaluator,
            explicit_meta_keys,
            meta_conflict_logged: HashSet::new(),
            rows_seen: 0,
        }
    }

    /// Borrow the in-memory group table. Public for finalize and tests.
    pub fn groups(&self) -> &hashbrown::HashMap<Vec<GroupByKey>, AggregatorGroupState> {
        &self.groups
    }

    /// Total bytes currently charged into the per-group value heap. Used
    /// by the spill trigger and by tests verifying memory accounting.
    pub fn value_heap_bytes(&self) -> usize {
        self.value_heap_bytes
    }

    /// Number of input records that have passed the pre-aggregation
    /// filter (D44). Drives the global-fold empty-input special case.
    pub fn rows_seen(&self) -> u64 {
        self.rows_seen
    }

    /// Borrow the spill file vector. Empty until 16.3.10 wires the
    /// spill writer.
    pub fn spill_files(&self) -> &[SpillFile<()>] {
        &self.spill_files
    }

    /// Drive one input record through the aggregator (D1, D9, D10, D11
    /// revised, D44). Order:
    ///
    /// 1. Pre-aggregation filter — skip on `false`.
    /// 2. `rows_seen += 1` (post-filter, per D44).
    /// 3. Extract the group-key tuple.
    /// 4. Look up / insert per-group state via the prototype factory.
    /// 5. Dispatch each `BindingArg` to its accumulator slot.
    /// 6. Walk record metadata into the per-group `MetadataCommonTracker`,
    ///    skipping keys the user has explicitly emitted.
    /// 7. Resize-aware spill trigger.
    pub fn add_record(
        &mut self,
        record: &Record,
        row_num: u64,
        emitted: &IndexMap<String, Value>,
        accumulated: &IndexMap<String, Value>,
        ctx: &EvalContext,
    ) -> Result<(), HashAggError> {
        // 1. Pre-aggregation filter (D9).
        if let Some(filter) = &self.pre_agg_filter {
            let env: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
            let meta: indexmap::IndexMap<String, Value> = indexmap::IndexMap::new();
            let v = eval_expr::<NullStorage>(
                filter,
                self.evaluator.typed(),
                ctx,
                record,
                None,
                &env,
                &meta,
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
            match value_to_group_key(&val, field_name, None, ctx.source_row as u32) {
                Ok(Some(gk)) => key.push(gk),
                Ok(None) => key.push(GroupByKey::Null),
                Err(e) => {
                    return Err(HashAggError::GroupKey {
                        field: field_name.to_string(),
                        row: ctx.source_row as u32,
                        message: e.to_string(),
                    });
                }
            }
        }

        // 4. Look up / insert per-group state.
        let group_state = self
            .groups
            .entry(key)
            .or_insert_with(|| AggregatorGroupState::new(self.factory.create_accumulators()));

        // D57 sidecar reductions — fold the per-record (row_num, emitted,
        // accumulated) triple into the per-group state. These three values
        // become the SortRow tuple's last three slots at finalize time.
        if row_num < group_state.min_row_num {
            group_state.min_row_num = row_num;
        }
        group_state.common_emitted = Some(match group_state.common_emitted.take() {
            None => emitted.clone(),
            Some(prev) => intersect_emitted(prev, emitted),
        });
        union_accumulated_into(&mut group_state.union_accumulated, accumulated);

        // 5. BindingArg dispatch hot loop (D1).
        let bindings = self.factory.compiled().bindings.clone();
        let mut delta: usize = 0;
        for (binding, acc) in bindings.iter().zip(group_state.row.iter_mut()) {
            delta += dispatch_binding(&binding.arg, acc, record, ctx, &self.evaluator)?;
        }
        self.value_heap_bytes = self.value_heap_bytes.saturating_add(delta);

        // 6. Metadata common-only propagation (D11 revised).
        if record.has_meta() {
            // Snapshot to a small Vec so we can mutate trackers without
            // borrowing record metadata across the observe call.
            let observations: Vec<(Box<str>, Value)> = record
                .iter_meta()
                .filter(|(k, _)| !self.explicit_meta_keys.contains(*k))
                .map(|(k, v)| (Box::<str>::from(k), v.clone()))
                .collect();
            for (k, v) in observations {
                let r = group_state.meta_tracker.observe(&k, &v);
                self.value_heap_bytes = self.value_heap_bytes.saturating_add(r.heap_delta);
                if r.became_conflict && self.meta_conflict_logged.insert(k.clone()) {
                    tracing::warn!(
                        transform = %self.transform_name,
                        meta_key = %k,
                        "aggregate dropped $meta.{} for at least one group: \
                         conflicting values across input records (use `emit $meta.{} = any($meta.{})` \
                         to keep the first-seen value)",
                        k,
                        k,
                        k
                    );
                }
            }
        }

        // 7. Resize-aware spill trigger (D1). Uses hashbrown's
        // `allocation_size` so the trigger fires before the next resize
        // doubles the table footprint.
        let table_alloc = self.groups.allocation_size();
        let headroom = self.memory_budget.saturating_sub(self.value_heap_bytes);
        if table_alloc.saturating_mul(3) > headroom && self.memory_budget > 0 {
            self.spill()?;
        }
        Ok(())
    }

    /// Spill the in-memory groups to disk (Task 16.3.10).
    ///
    /// 1. Drain `self.groups` into a Vec.
    /// 2. Sort by group key via [`compare_group_keys`] so the merge
    ///    phase (16.3.12) can do a k-way LoserTree walk.
    /// 3. Build one [`Record`] per group matching `spill_schema`
    ///    (group-by columns ++ `__acc_state` ++ `__meta_tracker`). The
    ///    accumulator row and the metadata common-tracker are serialized
    ///    as JSON strings in their respective columns.
    /// 4. Stream through [`SpillWriter`], finish the writer, and push
    ///    the resulting [`SpillFile`] onto `self.spill_files`.
    /// 5. Reset `value_heap_bytes = 0` because every per-group value
    ///    heap is now off-process.
    ///
    /// Per D11 revised: `MetadataCommonTracker` is serialized alongside
    /// the accumulator row so the spill-merge path can associatively
    /// merge partial trackers at key boundaries during recovery.
    fn spill(&mut self) -> Result<(), HashAggError> {
        let spill_err = |e: crate::pipeline::spill::SpillError| HashAggError::Spill(e.to_string());
        let json_err = |e: serde_json::Error| HashAggError::Spill(e.to_string());

        // 1. Drain.
        let drained: Vec<(Vec<GroupByKey>, AggregatorGroupState)> = self.groups.drain().collect();

        // 2. Encode each group's key into bytes via the same
        //    `SortKeyEncoder` configuration the read side will use, then
        //    sort by raw `Vec<u8>` (memcmp). The encoded buffers are
        //    transient — they drop at end of scope. Phase 16 Task 16.4.3.
        let sort_fields = group_by_sort_fields(&self.group_by_fields, &self.spill_schema);
        let encoder = crate::pipeline::sort_key::SortKeyEncoder::new(sort_fields);

        // Pre-build synthetic records once so we can encode them via
        // the same `Record`-based encoder both write and read sides use.
        let gb_count = self.group_by_indices.len();
        let mut prepared: Vec<(Vec<u8>, usize)> = Vec::with_capacity(drained.len());
        for (idx, (key, _state)) in drained.iter().enumerate() {
            let mut values: Vec<Value> = Vec::with_capacity(gb_count + 2);
            for gk in key {
                values.push(gk.to_value());
            }
            values.push(Value::Null);
            values.push(Value::Null);
            let synth = Record::new(Arc::clone(&self.spill_schema), values);
            let mut buf = Vec::new();
            encoder.encode_into(&synth, &mut buf);
            prepared.push((buf, idx));
        }
        prepared.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let mut writer: SpillWriter<()> =
            SpillWriter::new(Arc::clone(&self.spill_schema), self.spill_dir.as_deref())
                .map_err(spill_err)?;

        for (_encoded, idx) in &prepared {
            let (key, state) = &drained[*idx];
            let acc_json = serde_json::to_string(&state.row).map_err(json_err)?;
            let meta_json = serde_json::to_string(&state.meta_tracker).map_err(json_err)?;

            let mut values: Vec<Value> = Vec::with_capacity(gb_count + 2);
            for gk in key {
                values.push(gk.to_value());
            }
            values.push(Value::String(acc_json.into()));
            values.push(Value::String(meta_json.into()));

            let record = Record::new(Arc::clone(&self.spill_schema), values);
            writer.write_record(&record).map_err(spill_err)?;
        }

        let spill_file = writer.finish().map_err(spill_err)?;
        self.spill_files.push(spill_file);

        // 5. All per-group value heap bytes are now off-process.
        self.value_heap_bytes = 0;
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
    ///    through a `LoserTree`, merging `AccumulatorRow` +
    ///    `MetadataCommonTracker` partials at each key boundary, and
    ///    route each finalized group through the same
    ///    `finalize_group` helper so both paths agree byte-for-byte.
    pub fn finalize(
        mut self,
        ctx: &EvalContext,
        out: &mut Vec<SortRow>,
    ) -> Result<(), HashAggError> {
        // D12: global-fold empty-input special case. Delegated to the
        // shared `empty_global_fold_row` helper so the streaming path
        // produces a byte-identical record (Phase 16 Task 16.4.3).
        if self.rows_seen == 0 && self.group_by_indices.is_empty() && self.spill_files.is_empty() {
            let record =
                empty_global_fold_row(&self.factory, &self.output_schema, &self.transform_name)?;
            out.push((record, 0, IndexMap::new(), IndexMap::new()));
            return Ok(());
        }

        if self.spill_files.is_empty() {
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
                let emitted = state.common_emitted.unwrap_or_default();
                out.push((record, row_num, emitted, state.union_accumulated));
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
    /// `LoserTree` keyed on the group-by columns. At every group-key
    /// boundary in the merge stream, `AccumulatorEnum::merge` and
    /// `MetadataCommonTracker::merge` combine partial states
    /// associatively — conflicts propagate correctly because both
    /// merges are associative per D11 revised.
    fn finalize_with_spill(
        mut self,
        ctx: &EvalContext,
        out: &mut Vec<SortRow>,
    ) -> Result<(), HashAggError> {
        if !self.groups.is_empty() {
            self.spill()?;
        }

        // Open readers over every spill file.
        let spill_err = |e: crate::pipeline::spill::SpillError| HashAggError::Spill(e.to_string());
        let mut readers: Vec<crate::pipeline::spill::SpillReader<()>> = self
            .spill_files
            .iter()
            .map(|f| f.reader().map_err(spill_err))
            .collect::<Result<Vec<_>, _>>()?;

        // Sort fields for both the LoserTree's own ordering key and the
        // GroupBoundary's encoder. Single source of truth.
        let sort_fields = group_by_sort_fields(&self.group_by_fields, &self.spill_schema);

        // Prime the loser tree with one entry per reader.
        let mut initial: Vec<Option<MergeEntry>> = Vec::with_capacity(readers.len());
        for reader in &mut readers {
            match reader.next() {
                Some(Ok((record, _))) => {
                    let key = encode_sort_key(&record, &sort_fields);
                    initial.push(Some(MergeEntry { key, record }));
                }
                Some(Err(e)) => return Err(HashAggError::Spill(e.to_string())),
                None => initial.push(None),
            }
        }
        let mut tree = LoserTree::new(initial);

        let gb_count = self.group_by_indices.len();
        // FALLBACK per drill-pass-7 Option (b): the spill envelope does
        // NOT carry the D57 sidecars. For spill-recovered groups we push
        // identity sidecars into the shared `GroupBoundary` detector,
        // now byte-keyed (Phase 16 Task 16.4.3 — "Single-Encoder
        // Two-Phase Bytes").
        let factory = &self.factory;
        let output_schema = &self.output_schema;
        let transform_name = self.transform_name.clone();

        let encoder = crate::pipeline::sort_key::SortKeyEncoder::new(sort_fields.clone());
        let mut boundary = crate::pipeline::streaming_merge::GroupBoundary::new(
            encoder,
            crate::pipeline::streaming_merge::StreamingErrorMode::SpillMerge,
        );

        // The finalize closure receives the previously-open record (held
        // by `GroupBoundary::open_record`) and re-derives the semantic
        // group key from it via `decode_spill_record`. No `RefCell`
        // shadow of the key needed.
        let gb_fields = self.group_by_fields.clone();
        let finalize_closure =
            |rec: &Record, s: &AggregatorGroupState| -> Result<Record, HashAggError> {
                let (key, _state) = decode_spill_record(rec, &gb_fields, gb_count)?;
                finalize_group_inner(factory, output_schema, &transform_name, &key, s)
            };

        while tree.winner().is_some() {
            let stream_idx = tree.winner_index();
            let record = tree.winner().unwrap().record.clone();

            let (_key, state) = decode_spill_record(&record, &self.group_by_fields, gb_count)?;

            // Encode the spilled record's group-by columns into
            // boundary.current via the owned encoder.
            let mut scratch = std::mem::take(&mut boundary.current);
            boundary.encoder().encode_into(&record, &mut scratch);
            boundary.current = scratch;

            // The boundary stores `record` as the new open_record so the
            // finalize closure receives it on the *next* boundary.
            boundary.push(
                state,
                record,
                (0, IndexMap::new(), IndexMap::new()),
                &finalize_closure,
                out,
            )?;

            // Advance the winning stream.
            let next = match readers[stream_idx].next() {
                Some(Ok((record, _))) => {
                    let key = encode_sort_key(&record, &sort_fields);
                    Some(MergeEntry { key, record })
                }
                Some(Err(e)) => return Err(HashAggError::Spill(e.to_string())),
                None => None,
            };
            tree.replace_winner(next);
        }

        boundary.flush(&finalize_closure, out)?;
        let _ = ctx; // reserved for future per-group eval scope
        Ok(())
    }
}

/// Finalize one group into an output [`Record`]. Shared by the in-memory
/// fast path, the spill-recovery path, and `StreamingAggregator<AddRaw>`
/// so all three produce byte-identical results.
///
/// 1. Finalize each accumulator in `state.row` into a slot vector.
/// 2. Evaluate every compiled emit residual in an [`AggregateEvalScope`];
///    `is_meta` emits collect into `user_meta`, data emits populate
///    `values` by output-schema column name.
/// 3. Merge the auto-tracked common metadata (D11 revised) — user
///    explicit emits win on conflict, the tracker fills only keys the
///    user did not supply.
/// 4. Build the output `Record` and install metadata.
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
    let mut user_meta: IndexMap<Box<str>, Value> = IndexMap::new();
    for emit in &compiled.emits {
        let v = eval_expr_in_agg_scope(&emit.residual, &scope).map_err(HashAggError::Residual)?;
        if emit.is_meta {
            user_meta.insert(emit.output_name.clone(), v);
        } else if let Some(idx) = output_schema.index(&emit.output_name) {
            values[idx] = v;
        }
    }

    let auto_pairs = state.meta_tracker.clone().finalize();
    for (k, v) in auto_pairs {
        if !user_meta.contains_key(&k) {
            user_meta.insert(k, v);
        }
    }

    let mut record = Record::new(Arc::clone(output_schema), values);
    for (k, v) in user_meta {
        let _ = record.set_meta(&k, v);
    }
    Ok(record)
}

/// Decode one spill record produced by [`HashAggregator::spill`] back
/// into its `(group_key, AggregatorGroupState)` representation.
///
/// The spill schema is: `[group-by columns..] ++ __acc_state ++
/// __meta_tracker`. Both trailer columns hold JSON-serialized
/// `AccumulatorRow` / `MetadataCommonTracker` strings.
fn decode_spill_record(
    record: &Record,
    gb_fields: &[String],
    gb_count: usize,
) -> Result<(Vec<GroupByKey>, AggregatorGroupState), HashAggError> {
    let values = record.values();
    if values.len() < gb_count + 2 {
        return Err(HashAggError::Spill(format!(
            "spill record has {} columns, expected at least {}",
            values.len(),
            gb_count + 2
        )));
    }

    let mut key: Vec<GroupByKey> = Vec::with_capacity(gb_count);
    for (i, name) in gb_fields.iter().enumerate() {
        let v = &values[i];
        match value_to_group_key(v, name, None, 0)
            .map_err(|e| HashAggError::Spill(format!("spill group-key decode: {e:?}")))?
        {
            Some(k) => key.push(k),
            None => key.push(GroupByKey::Null),
        }
    }

    let acc_json = match &values[gb_count] {
        Value::String(s) => s.as_ref(),
        other => {
            return Err(HashAggError::Spill(format!(
                "spill __acc_state column is not a string: {other:?}"
            )));
        }
    };
    let meta_json = match &values[gb_count + 1] {
        Value::String(s) => s.as_ref(),
        other => {
            return Err(HashAggError::Spill(format!(
                "spill __meta_tracker column is not a string: {other:?}"
            )));
        }
    };

    let row: AccumulatorRow = serde_json::from_str(acc_json)
        .map_err(|e| HashAggError::Spill(format!("__acc_state decode: {e}")))?;
    let meta_tracker: MetadataCommonTracker = serde_json::from_str(meta_json)
        .map_err(|e| HashAggError::Spill(format!("__meta_tracker decode: {e}")))?;

    // Spill-recovered state has no D57 sidecars (see Option (b) fallback
    // comment in `finalize_with_spill`). Sidecars stay at identity values.
    let mut state = AggregatorGroupState::new(row);
    state.meta_tracker = meta_tracker;
    Ok((key, state))
}

/// Single source of truth for the `Vec<SortField>` configuration used
/// to encode group-by columns for the streaming aggregator and the
/// spill write/read paths. Phase 16 Task 16.4.3 (D74).
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

/// Single source of truth for the empty-input global-fold record (D12).
///
/// Both the hash path (`HashAggregator::finalize`) and the streaming
/// path (`StreamingAggregator::flush`) call this to produce one
/// defaulted output record when an empty stream and an empty group-by
/// would otherwise yield zero rows. Mirrors DataFusion's
/// `AggregateStream` empty-input branch. Phase 16 Task 16.4.3 (D73).
pub(crate) fn empty_global_fold_row(
    factory: &AccumulatorFactory,
    output_schema: &Arc<Schema>,
    transform_name: &str,
) -> Result<Record, HashAggError> {
    let empty_key: Vec<GroupByKey> = Vec::new();
    let state = AggregatorGroupState::new(factory.create_accumulators());
    finalize_group_inner(factory, output_schema, transform_name, &empty_key, &state)
}

/// Dispatch one `BindingArg` to its accumulator. Returns the heap-byte
/// delta produced by the accumulator's `add` call so the hash aggregator
/// can fold it into `value_heap_bytes`.
fn dispatch_binding(
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
            let env: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
            let meta: indexmap::IndexMap<String, Value> = indexmap::IndexMap::new();
            let v = eval_expr::<NullStorage>(e, evaluator.typed(), ctx, record, None, &env, &meta)?;
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

fn eval_binding_arg_value(
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
            let env: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
            let meta: indexmap::IndexMap<String, Value> = indexmap::IndexMap::new();
            Ok(eval_expr::<NullStorage>(
                e,
                evaluator.typed(),
                ctx,
                record,
                None,
                &env,
                &meta,
            )?)
        }
        BindingArg::Pair(_, _) => Err(HashAggError::EvalFailed(EvalError {
            kind: cxl::eval::EvalErrorKind::TypeMismatch {
                expected: "scalar binding arg",
                got: "nested Pair",
            },
            span: cxl::lexer::Span::new(0, 0),
        })),
    }
}

// ---------------------------------------------------------------------------
// StreamingAggregator<Op: AccumulatorOp> — raw + merge modes (Task 16.4.0)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// AccumulatorOp trait + AddRaw / MergeState impls (Task 16.4.2)
// ---------------------------------------------------------------------------
//
// The trait lives here in `clinker-core/src/aggregation.rs` (NOT
// `clinker-record`) because `MergeState::Input` references
// `AggregatorGroupState`, which in turn holds a `MetadataCommonTracker`
// defined alongside the hash aggregator. `clinker-record` has no
// dependency on `cxl`, so the `apply_row` signature — which takes
// `&[AggregateBinding]` — would also be unexpressible there. Phase 16
// Task 16.4.2 explicitly specifies `aggregation.rs` as the location.

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
///   comparator) plus the FULL per-group state: the `AccumulatorRow`,
///   the three D57 sidecar fields (`min_row_num`, `common_emitted`,
///   `union_accumulated`), and the `MetadataCommonTracker`. `apply_row`
///   folds the incoming state into the currently-open per-group
///   `row` via `AccumulatorEnum::merge`. The sidecar / tracker merges
///   live on `GroupBoundary::push` because they operate on the open
///   group's full `AggregatorGroupState`, not on the `row` alone.
///
/// **Audit fix Gap B:** pre-drill-pass-6 wording had
/// `MergeState::Input = (Vec<u8>, AccumulatorRow)`, which silently
/// dropped the D57 sidecar fields on the spill-recovery path. That
/// would have produced a different `min_row_num` than the in-memory
/// path and violated the very invariant D68 was created to enforce.
/// Widening to the full `AggregatorGroupState` is the minimum-surface
/// fix.
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
/// pre-sorted order on the group-by prefix. Phase 16 Task 16.4.2.
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
        // path. This fast-path specialization is what Task 16.4.2
        // reserves for future hot-loop optimization (DataFusion
        // blog 2023-08-05 per-record allocation pattern).
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
/// `AggregatorGroupState`). Phase 16 Task 16.4.2.
///
/// `Input` carries the **full** state so sidecar reductions (D57) and
/// the metadata common tracker (D11 revised) survive spill recovery
/// with byte-identical semantics to the in-memory path.
#[derive(Debug, Default)]
pub struct MergeState;

impl AccumulatorOp for MergeState {
    /// Encoded sort key (read by the LoserTree comparator) + the full
    /// per-group state. The sort key is produced by `SortKeyEncoder`
    /// (Task 16.4.1) and matches the declared sort-order direction.
    type Input = (Vec<u8>, AggregatorGroupState);

    #[inline(always)]
    fn apply_row(row: &mut AccumulatorRow, _bindings: &[AggregateBinding], input: &Self::Input) {
        // Merge the incoming `AccumulatorRow` into the currently-open
        // group's row slot-by-slot via `AccumulatorEnum::merge`. The
        // sidecar fields (`min_row_num`, `common_emitted`,
        // `union_accumulated`) and the `MetadataCommonTracker` live on
        // `AggregatorGroupState` and are merged by
        // [`crate::pipeline::streaming_merge::GroupBoundary::push`]
        // when it installs / folds the open group — `apply_row` stays
        // focused on accumulator-row reduction so the two merge
        // surfaces stay decoupled.
        let (_encoded_key, other_state) = input;
        for (a, b) in row.iter_mut().zip(other_state.row.iter()) {
            AccumulatorEnum::merge(a, b);
        }
    }
}

/// Associative merge of the three D57 sidecar fields plus the
/// metadata common tracker. Operates on a currently-open group's
/// `AggregatorGroupState` and folds another state's sidecars in.
///
/// Separated out so both the `GroupBoundary` state machine (for the
/// raw-streaming path) and the spill-recovery path can call the
/// identical reduction logic. Exposed at crate scope for tests.
pub(crate) fn merge_group_sidecars(dst: &mut AggregatorGroupState, src: AggregatorGroupState) {
    // `min_row_num`: monotonic min. `u64::MAX` is the identity.
    if src.min_row_num < dst.min_row_num {
        dst.min_row_num = src.min_row_num;
    }
    // `common_emitted`: set-intersection of keys where values agree.
    dst.common_emitted = Some(match (dst.common_emitted.take(), src.common_emitted) {
        (None, None) => IndexMap::new(),
        (Some(prev), None) => prev,
        (None, Some(s)) => s,
        (Some(prev), Some(s)) => intersect_emitted(prev, &s),
    });
    // `union_accumulated`: first-seen-wins union.
    union_accumulated_into(&mut dst.union_accumulated, &src.union_accumulated);
    // `MetadataCommonTracker`: associative merge per D11 revised.
    dst.meta_tracker.merge(src.meta_tracker);
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
///   the decisions-log type surface survives for Phase 17+ work.
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
    explicit_meta_keys: HashSet<Box<str>>,
    meta_conflict_logged: HashSet<Box<str>>,
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
        let explicit_meta_keys: HashSet<Box<str>> = compiled
            .emits
            .iter()
            .filter(|e| e.is_meta)
            .map(|e| e.output_name.clone())
            .collect();
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
            explicit_meta_keys,
            meta_conflict_logged: HashSet::new(),
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
    /// 5. Populate the D57 sidecar fields from the per-record
    ///    `row_num` / `emitted` / `accumulated` inputs.
    /// 6. Push into the shared `GroupBoundary` detector; on a key
    ///    boundary the previous group's finalized `SortRow` lands in
    ///    `self.pending`. A strictly lesser key is routed through
    ///    `HashAggError::SortOrderViolation` → hard abort.
    #[allow(clippy::too_many_arguments)]
    pub fn add_record(
        &mut self,
        record: &Record,
        row_num: u64,
        emitted: &IndexMap<String, Value>,
        accumulated: &IndexMap<String, Value>,
        ctx: &EvalContext,
        out: &mut Vec<SortRow>,
    ) -> Result<(), HashAggError> {
        if let Some(filter) = &self.pre_agg_filter {
            let env: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
            let meta: indexmap::IndexMap<String, Value> = indexmap::IndexMap::new();
            let v = eval_expr::<NullStorage>(
                filter,
                self.evaluator.typed(),
                ctx,
                record,
                None,
                &env,
                &meta,
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
            match value_to_group_key(&val, field_name, None, ctx.source_row as u32) {
                Ok(Some(gk)) => key.push(gk),
                Ok(None) => key.push(GroupByKey::Null),
                Err(e) => {
                    return Err(HashAggError::GroupKey {
                        field: field_name.to_string(),
                        row: ctx.source_row as u32,
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

        // D11 revised: walk record metadata into the fresh tracker.
        if record.has_meta() {
            let observations: Vec<(Box<str>, Value)> = record
                .iter_meta()
                .filter(|(k, _)| !self.explicit_meta_keys.contains(*k))
                .map(|(k, v)| (Box::<str>::from(k), v.clone()))
                .collect();
            for (k, v) in observations {
                let r = state.meta_tracker.observe(&k, &v);
                if r.became_conflict && self.meta_conflict_logged.insert(k.clone()) {
                    tracing::warn!(
                        transform = %self.transform_name,
                        meta_key = %k,
                        "streaming aggregate dropped $meta.{} for at least one group: \
                         conflicting values across input records",
                        k
                    );
                }
            }
        }

        // D57 sidecar seed — single-record values feed directly into
        // the freshly-constructed per-group state. `GroupBoundary`'s
        // `push` merges these in per-group once the key is installed.
        state.min_row_num = row_num;
        if !emitted.is_empty() {
            state.common_emitted = Some(emitted.clone());
        } else {
            state.common_emitted = Some(IndexMap::new());
        }
        state.union_accumulated = accumulated.clone();

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
        let source_row = ctx.source_row as u32;
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
        self.boundary.push(
            state,
            record.clone(),
            (row_num, emitted.clone(), accumulated.clone()),
            &finalize_closure,
            out,
        )?;

        let _ = ctx; // ctx already used above
        Ok(())
    }

    /// Drain all emitted boundary rows plus the final open group.
    /// Phase 16 Task 16.4.3 — D12 special case.
    pub fn flush(mut self, _ctx: &EvalContext, out: &mut Vec<SortRow>) -> Result<(), HashAggError> {
        // D12 special case: empty input + empty group_by → emit one
        // defaulted global-fold row.
        if self.rows_seen == 0 && self.group_by_indices.is_empty() {
            let record =
                empty_global_fold_row(&self.factory, &self.output_schema, &self.transform_name)?;
            out.push((record, 0, IndexMap::new(), IndexMap::new()));
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
    /// forward-compat stub for Phase 17+ out-of-core merge recovery;
    /// the executor's spill-recovery path currently lives inside
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
        let explicit_meta_keys: HashSet<Box<str>> = compiled
            .emits
            .iter()
            .filter(|e| e.is_meta)
            .map(|e| e.output_name.clone())
            .collect();
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
            explicit_meta_keys,
            meta_conflict_logged: HashSet::new(),
            rows_seen: 0,
            boundary,
            pending: Vec::new(),
            _mode: std::marker::PhantomData,
        }
    }
}

impl<Op: AccumulatorOp> StreamingAggregator<Op> {
    /// Task 16.4.10 — debug-inspect accessor used by the structural O(1)
    /// memory test (G7) and by the Kiln debugger's streaming-agg state
    /// overlay. Returns 1 when a per-group state is currently open
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

// ---------------------------------------------------------------------------
// Task 16.4.6 — plan-time streaming eligibility
// ---------------------------------------------------------------------------

/// Outcome of evaluating whether an aggregation can run in streaming mode
/// given its parent node's physical properties. Returned by
/// [`qualifies_for_streaming`] and consumed by `compile_transforms()` in
/// Task 16.4.9a to pick between `PlanNode::StreamingAggregation` and
/// `PlanNode::HashAggregation`.
#[derive(Debug, Clone)]
pub enum StreamingEligibility {
    /// Parent stream qualifies. `effective_group_by` is the group-by list
    /// possibly reordered to match the sort-prefix (PostgreSQL PG 17
    /// `get_useful_group_keys_orderings()` pattern). `qualified_sort_order`
    /// is the subset of the parent sort order that covers the group-by.
    Streaming {
        effective_group_by: Vec<String>,
        qualified_sort_order: Vec<SortField>,
    },
    /// Parent stream does not qualify. `reason` is a human-readable string
    /// surfaced in `--explain` output and Kiln canvas tooltips.
    HashFallback { reason: String },
}

/// Plan-time qualifier for streaming aggregation. Reads the parent node's
/// physical properties and the aggregate's `group_by` list, returns a
/// [`StreamingEligibility`] describing whether streaming is allowed and, if
/// so, the effective group-by order and qualified sort prefix.
///
/// Rules (see phase-16-aggregation.md Task 16.4.6 for provenance):
/// - Global fold (`group_by` empty) always streams — a single output row
///   with no sort requirement.
/// - `Single` partitioning passes; `HashPartitioned` passes iff its keys
///   cover the group-by; `RoundRobin` always falls back.
/// - A declared sort order is required. The first `group_by.len()` fields
///   of the sort order (the sort prefix) must cover the group-by as a set.
///   Partial or disjoint prefixes fall back to hash.
/// - When the sort prefix covers the group-by in a different order, the
///   group-by is reordered to match the sort prefix (PG 17 pattern).
pub fn qualifies_for_streaming(
    parent_props: &crate::plan::properties::NodeProperties,
    group_by: &[String],
) -> StreamingEligibility {
    use crate::plan::properties::PartitioningKind;

    // Global fold — always streams, no sort needed.
    if group_by.is_empty() {
        return StreamingEligibility::Streaming {
            effective_group_by: Vec::new(),
            qualified_sort_order: Vec::new(),
        };
    }

    // (a) Partitioning check.
    match &parent_props.partitioning.kind {
        PartitioningKind::Single => {}
        PartitioningKind::HashPartitioned { keys, .. }
            if group_by.iter().all(|g| keys.iter().any(|k| k == g)) => {}
        PartitioningKind::HashPartitioned { keys, .. } => {
            return StreamingEligibility::HashFallback {
                reason: format!(
                    "input is hash-partitioned on {:?}, which does not cover group-by {:?}",
                    keys, group_by
                ),
            };
        }
        PartitioningKind::RoundRobin { num_partitions } => {
            return StreamingEligibility::HashFallback {
                reason: format!(
                    "input is round-robin partitioned across {} partitions; \
                     streaming aggregation requires a single stream or hash-partitioned on group keys",
                    num_partitions
                ),
            };
        }
    }

    // (b) Ordering check.
    let sort_order = match &parent_props.ordering.sort_order {
        Some(so) => so,
        None => {
            return StreamingEligibility::HashFallback {
                reason: format!(
                    "input has no declared sort order ({})",
                    ordering_provenance_summary(&parent_props.ordering.provenance)
                ),
            };
        }
    };

    if sort_order.len() < group_by.len() {
        return StreamingEligibility::HashFallback {
            reason: format!(
                "sort order {:?} is shorter than group-by {:?}; streaming requires full prefix coverage",
                sort_order.iter().map(|s| &s.field).collect::<Vec<_>>(),
                group_by
            ),
        };
    }

    let prefix: HashSet<&str> = sort_order[..group_by.len()]
        .iter()
        .map(|s| s.field.as_str())
        .collect();
    let gb_set: HashSet<&str> = group_by.iter().map(|s| s.as_str()).collect();

    if prefix != gb_set {
        return StreamingEligibility::HashFallback {
            reason: format!(
                "sort order prefix {:?} does not cover group-by {:?}",
                sort_order[..group_by.len()]
                    .iter()
                    .map(|s| &s.field)
                    .collect::<Vec<_>>(),
                group_by
            ),
        };
    }

    // (c) Group-by reordered to match sort prefix (PG 17 pattern).
    let effective_group_by: Vec<String> = sort_order[..group_by.len()]
        .iter()
        .map(|s| s.field.clone())
        .collect();
    let qualified_sort_order: Vec<SortField> = sort_order[..group_by.len()].to_vec();

    StreamingEligibility::Streaming {
        effective_group_by,
        qualified_sort_order,
    }
}

fn ordering_provenance_summary(p: &crate::plan::properties::OrderingProvenance) -> String {
    use crate::plan::properties::OrderingProvenance as OP;
    match p {
        OP::NoOrdering => "no sort_order declared on input".into(),
        OP::DeclaredOnInput { input_name } => {
            format!("declared on input `{}`", input_name)
        }
        OP::Preserved { from_node } => format!("preserved from `{}`", from_node),
        OP::DestroyedByTransformWriteSet {
            at_node,
            sort_fields_lost,
            ..
        } => format!(
            "destroyed by transform `{}` writing {:?}",
            at_node, sort_fields_lost
        ),
        OP::DestroyedByDistinct { at_node, .. } => {
            format!("destroyed by `distinct` in transform `{}`", at_node)
        }
        OP::DestroyedByHashAggregate { at_node, .. } => {
            format!("destroyed by hash aggregate `{}`", at_node)
        }
        OP::DestroyedByMergeMismatch { at_node, .. } => {
            format!("destroyed by merge mismatch at `{}`", at_node)
        }
        OP::IntroducedByStreamingAggregate { at_node, .. } => {
            format!("introduced by streaming aggregate `{}`", at_node)
        }
    }
}

#[cfg(test)]
mod accumulator_op_tests {
    use super::*;
    use clinker_record::accumulator::{AccumulatorEnum, AggregateType};

    fn sv(s: &str) -> Value {
        Value::String(s.into())
    }

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

    #[test]
    fn test_merge_sidecars_emitted_intersect_keeps_agreeing_keys() {
        let mut dst = state_with_row(Vec::new());
        let mut a = IndexMap::new();
        a.insert("k1".to_string(), sv("v1"));
        a.insert("k2".to_string(), sv("v2"));
        dst.common_emitted = Some(a);

        let mut src = state_with_row(Vec::new());
        let mut b = IndexMap::new();
        b.insert("k1".to_string(), sv("v1")); // agrees
        b.insert("k2".to_string(), sv("DIFFERENT")); // conflicts → dropped
        b.insert("k3".to_string(), sv("v3")); // not in dst → dropped
        src.common_emitted = Some(b);

        merge_group_sidecars(&mut dst, src);
        let out = dst.common_emitted.expect("present");
        assert_eq!(out.len(), 1);
        assert_eq!(out.get("k1"), Some(&sv("v1")));
    }

    #[test]
    fn test_merge_sidecars_accumulated_union_first_seen_wins() {
        let mut dst = state_with_row(Vec::new());
        dst.union_accumulated.insert("k1".to_string(), sv("first"));

        let mut src = state_with_row(Vec::new());
        src.union_accumulated
            .insert("k1".to_string(), sv("SHOULD_NOT_WIN"));
        src.union_accumulated.insert("k2".to_string(), sv("new"));

        merge_group_sidecars(&mut dst, src);
        assert_eq!(dst.union_accumulated.get("k1"), Some(&sv("first")));
        assert_eq!(dst.union_accumulated.get("k2"), Some(&sv("new")));
    }

    #[test]
    fn test_merge_sidecars_metadata_tracker_associative() {
        // dst sees {source_file: a.csv}; src sees {source_file: a.csv,
        // other: x}. After merge dst must retain source_file and pick
        // up `other` as common-only (both trackers saw it consistently).
        let mut dst = state_with_row(Vec::new());
        let _ = dst.meta_tracker.observe("source_file", &sv("a.csv"));

        let mut src = state_with_row(Vec::new());
        let _ = src.meta_tracker.observe("source_file", &sv("a.csv"));
        let _ = src.meta_tracker.observe("other", &sv("x"));

        merge_group_sidecars(&mut dst, src);
        let finalized = dst.meta_tracker.finalize();
        let keys: Vec<&str> = finalized.iter().map(|(k, _)| k.as_ref()).collect();
        assert!(
            keys.contains(&"source_file"),
            "source_file missing: {keys:?}"
        );
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
        // full `(Vec<u8>, AggregatorGroupState)` (audit fix Gap B).
        // If Task 16.4.2 ever regresses to `(Vec<u8>, AccumulatorRow)`
        // this test stops compiling.
        fn assert_input_is_full_state(_: &<MergeState as AccumulatorOp>::Input) {}
        let st = state_with_row(Vec::new());
        let input: (Vec<u8>, AggregatorGroupState) = (Vec::new(), st);
        assert_input_is_full_state(&input);
        // And confirm sidecar + tracker fields are addressable on the
        // `Input` type — if they are missing this line fails to
        // compile.
        let _ = input.1.min_row_num;
        let _ = &input.1.common_emitted;
        let _ = &input.1.union_accumulated;
        let _ = &input.1.meta_tracker;
    }

    // ----- qualifies_for_streaming (Task 16.4.6) -----

    use crate::config::SortOrder as SO;
    use crate::plan::properties::{
        NodeProperties, Ordering, OrderingProvenance, Partitioning, PartitioningKind,
        PartitioningProvenance,
    };

    fn sf(field: &str) -> SortField {
        SortField {
            field: field.into(),
            order: SO::Asc,
            null_order: None,
        }
    }

    fn props_single_with_sort(sort: Option<Vec<SortField>>) -> NodeProperties {
        NodeProperties {
            ordering: Ordering {
                sort_order: sort,
                provenance: OrderingProvenance::DeclaredOnInput {
                    input_name: "src".into(),
                },
            },
            partitioning: Partitioning {
                kind: PartitioningKind::Single,
                provenance: PartitioningProvenance::SingleStream,
            },
        }
    }

    fn props_with_partitioning(kind: PartitioningKind) -> NodeProperties {
        NodeProperties {
            ordering: Ordering {
                sort_order: None,
                provenance: OrderingProvenance::NoOrdering,
            },
            partitioning: Partitioning {
                kind,
                provenance: PartitioningProvenance::SingleStream,
            },
        }
    }

    #[test]
    fn test_qualifies_single_partition_matching_sort() {
        let props = props_single_with_sort(Some(vec![sf("a"), sf("b")]));
        let gb = vec!["a".to_string(), "b".to_string()];
        match qualifies_for_streaming(&props, &gb) {
            StreamingEligibility::Streaming {
                effective_group_by,
                qualified_sort_order,
            } => {
                assert_eq!(effective_group_by, vec!["a", "b"]);
                assert_eq!(qualified_sort_order.len(), 2);
            }
            other => panic!("expected Streaming, got {:?}", other),
        }
    }

    #[test]
    fn test_qualifies_partial_sort_prefix_falls_back() {
        let props = props_single_with_sort(Some(vec![sf("a")]));
        let gb = vec!["a".to_string(), "b".to_string()];
        match qualifies_for_streaming(&props, &gb) {
            StreamingEligibility::HashFallback { reason } => {
                assert!(reason.contains("shorter than"), "reason: {}", reason);
            }
            other => panic!("expected HashFallback, got {:?}", other),
        }
    }

    #[test]
    fn test_qualifies_disjoint_sort_fields_falls_back() {
        let props = props_single_with_sort(Some(vec![sf("x"), sf("y")]));
        let gb = vec!["a".to_string(), "b".to_string()];
        match qualifies_for_streaming(&props, &gb) {
            StreamingEligibility::HashFallback { reason } => {
                assert!(
                    reason.contains("does not cover group-by"),
                    "reason: {}",
                    reason
                );
            }
            other => panic!("expected HashFallback, got {:?}", other),
        }
    }

    #[test]
    fn test_qualifies_reorders_group_by_to_match_sort() {
        let props = props_single_with_sort(Some(vec![sf("region"), sf("dept")]));
        let gb = vec!["dept".to_string(), "region".to_string()];
        match qualifies_for_streaming(&props, &gb) {
            StreamingEligibility::Streaming {
                effective_group_by, ..
            } => {
                assert_eq!(effective_group_by, vec!["region", "dept"]);
            }
            other => panic!("expected Streaming, got {:?}", other),
        }
    }

    #[test]
    fn test_qualifies_round_robin_partitioning_falls_back() {
        let props = props_with_partitioning(PartitioningKind::RoundRobin { num_partitions: 4 });
        let gb = vec!["a".to_string()];
        match qualifies_for_streaming(&props, &gb) {
            StreamingEligibility::HashFallback { reason } => {
                assert!(reason.contains("round-robin"), "reason: {}", reason);
            }
            other => panic!("expected HashFallback, got {:?}", other),
        }
    }

    #[test]
    fn test_qualifies_hash_partitioned_covering_keys_ok() {
        let props = NodeProperties {
            ordering: Ordering {
                sort_order: Some(vec![sf("a"), sf("b")]),
                provenance: OrderingProvenance::DeclaredOnInput {
                    input_name: "src".into(),
                },
            },
            partitioning: Partitioning {
                kind: PartitioningKind::HashPartitioned {
                    keys: vec!["a".into(), "b".into(), "c".into()],
                    num_partitions: 4,
                },
                provenance: PartitioningProvenance::SingleStream,
            },
        };
        let gb = vec!["a".to_string(), "b".to_string()];
        assert!(matches!(
            qualifies_for_streaming(&props, &gb),
            StreamingEligibility::Streaming { .. }
        ));
    }

    #[test]
    fn test_qualifies_global_fold_always_streaming() {
        // No sort, RoundRobin partitioning — still streams because group_by is empty.
        let props = props_with_partitioning(PartitioningKind::RoundRobin { num_partitions: 8 });
        match qualifies_for_streaming(&props, &[]) {
            StreamingEligibility::Streaming {
                effective_group_by,
                qualified_sort_order,
            } => {
                assert!(effective_group_by.is_empty());
                assert!(qualified_sort_order.is_empty());
            }
            other => panic!("expected Streaming, got {:?}", other),
        }
    }
}
