//! Hash + streaming aggregation engine.
//!
//! Runtime types referenced by `PlanNode::Aggregation`'s executor dispatch:
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
use crate::pipeline::loser_tree::LoserTree;

/// Local stand-in to satisfy `eval_expr`'s `S: RecordStorage` type
/// parameter when no window context is in play. The aggregator hot loop
/// resolves field references through the input `Record` (which
/// implements `FieldResolver`); the storage parameter is unused.
struct NullStorage;
impl RecordStorage for NullStorage {
    fn resolve_field(&self, _: u32, _: &str) -> Option<&Value> {
        None
    }
    fn resolve_qualified(&self, _: u32, _: &str, _: &str) -> Option<&Value> {
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
/// `Hash` is the universal default. `Streaming` is used when the input
/// is provably sorted on the full group-by prefix — allowing a
/// one-group-at-a-time fold that never materializes a hash table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateStrategy {
    Hash,
    Streaming,
}

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
/// row heap, IndexMap sidecar overhead, and MetadataCommonTracker base.
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

    // MetadataCommonTracker: empty HashMap base allocation + headroom for
    // a few observed keys.
    let meta_base = std::mem::size_of::<MetadataCommonTracker>() + 128;

    // hashbrown targets ~87.5% load factor → ~1.15x bucket overallocation.
    let raw = bucket_entry + key_heap + acc_heap + meta_base;
    (raw as f64 * 1.15) as usize
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
/// scope.
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
    /// evaluator does not yet support. Unsupported expressions are
    /// reported via this variant rather than panicking.
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
// MetadataCommonTracker
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
/// `AccumulatorRow` through the spill path.
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
/// WARN log via `meta_conflict_logged`. The struct surfaces both
/// together so the caller never does a hidden second lookup.
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
// HashAggregator
// ---------------------------------------------------------------------------

/// Per-group state held inside the hash table.
///
/// `row` is the row of accumulators (one slot per `AggregateBinding`)
/// being driven by the hot loop. `meta_tracker` is the per-group
/// "Keep Only Common Attributes" propagator (D11 revised) that
/// observes every input record's metadata so finalize can emit
/// non-conflicting keys without per-key user wiring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatorGroupState {
    pub row: AccumulatorRow,
    pub meta_tracker: MetadataCommonTracker,
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
    /// Per-group input-row-number list. Populated only on the lineage
    /// path; empty otherwise (an empty `Vec` does not allocate). Spilled
    /// alongside accumulator state so the post-merge `AggregatorGroupState`
    /// preserves which input rows folded into each group across the
    /// round trip.
    pub input_rows: Vec<u32>,
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
            meta_tracker: MetadataCommonTracker::new(),
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
    pub meta_tracker: MetadataCommonTracker,
    /// Minimum `row_num` across every input record folded into this
    /// group. Same semantics as `AggregatorGroupState.min_row_num`:
    /// downstream sort-stable operators key off the earliest input
    /// row's position.
    pub min_row_num: u64,
    /// Stable in-memory index of this group within
    /// `HashAggregator.buffered_groups` at insertion time, mirroring
    /// the lineage path's group index. Meaningless after spill+merge.
    pub group_index: u32,
    /// Per-group input-row-number list. Mirrors
    /// `AggregatorGroupState.input_rows` so the spill-merge concat
    /// round-trips through the same shape on both retraction strategies.
    pub input_rows: Vec<u32>,
    /// Per-row evaluated binding values. `contributions[i][slot]`
    /// holds the value the binding at slot `slot` produced for the
    /// i-th ingested row, in `CompiledAggregate.bindings[]` order.
    pub contributions: Vec<Vec<Value>>,
}

impl BufferedGroupState {
    pub(crate) fn new() -> Self {
        Self {
            meta_tracker: MetadataCommonTracker::new(),
            min_row_num: u64::MAX,
            group_index: 0,
            input_rows: Vec::new(),
            contributions: Vec::new(),
        }
    }
}

/// Per-node-buffer row produced by every executor node that emits into
/// `node_buffers`. The Record is authoritative: all emitted fields have
/// been applied via `Record::set` onto the widened schema, and all
/// `$meta.*` writes via `Record::set_meta`. Downstream nodes resolve
/// field references against the Record directly (no parallel
/// bookkeeping threading).
pub type SortRow = (Record, u64);

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
    /// `GroupBoundary` hands the executor both pre/next encoded keys for
    /// diagnostics. The user-input vs spill-merge distinction is
    /// captured in two separate variants because the two cases route
    /// differently through `From<HashAggError> for PipelineError`.
    SortOrderViolation {
        prev_key_debug: String,
        next_key_debug: String,
    },
    /// Spill-merge produced an out-of-order key — Clinker bug, not a
    /// user data error. Always hard-aborts.
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
    #[allow(clippy::too_many_arguments)]
    pub fn for_node(
        compiled: Arc<CompiledAggregate>,
        evaluator: ProgramEvaluator,
        strategy: AggregateStrategy,
        output_schema: Arc<Schema>,
        spill_schema: Arc<Schema>,
        memory_budget: usize,
        spill_dir: Option<PathBuf>,
        transform_name: String,
    ) -> Result<Self, PipelineError> {
        let _ = (&spill_schema, memory_budget, &spill_dir);
        match strategy {
            AggregateStrategy::Hash => Ok(Self::Hash(Box::new(HashAggregator::new(
                compiled,
                evaluator,
                output_schema,
                spill_schema,
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
    lineage: Option<Vec<(u32, u32)>>,
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
            explicit_meta_keys,
            meta_conflict_logged: HashSet::new(),
            rows_seen: 0,
            max_groups,
            records_since_rss_check: 0,
            lineage,
            buffered_groups: hashbrown::HashMap::new(),
            buffer_mode,
        }
    }

    /// Borrow the in-memory group table. Public for finalize and tests.
    pub fn groups(&self) -> &hashbrown::HashMap<Vec<GroupByKey>, AggregatorGroupState> {
        &self.groups
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
        ctx: &EvalContext,
    ) -> Result<(), HashAggError> {
        if self.buffer_mode {
            return self.add_record_buffered(record, row_num, ctx);
        }
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
            let row_u32 = row_num as u32;
            let group_idx = group_state.group_index;
            lin.push((row_u32, group_idx));
            group_state.input_rows.push(row_u32);
            self.value_heap_bytes = self
                .value_heap_bytes
                .saturating_add(std::mem::size_of::<(u32, u32)>() + std::mem::size_of::<u32>());
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
                        let env: std::collections::HashMap<String, Value> =
                            std::collections::HashMap::new();
                        let meta: indexmap::IndexMap<String, Value> = indexmap::IndexMap::new();
                        let v = eval_expr::<NullStorage>(
                            e,
                            self.evaluator.typed(),
                            ctx,
                            record,
                            None,
                            &env,
                            &meta,
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
            self.value_heap_bytes = self.value_heap_bytes.saturating_add(row_charge);
            group_state.retract_values.push(row_values);
        } else {
            for (binding, acc) in bindings.iter().zip(group_state.row.iter_mut()) {
                delta += dispatch_binding(&binding.arg, acc, record, ctx, &self.evaluator)?;
            }
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

        // 7. Dual-threshold spill trigger (PostgreSQL hash_agg_check_limits
        // pattern). Primary: group count exceeds pre-computed max_groups
        // (60% of budget / estimated_bytes_per_group). Secondary:
        // value_heap_bytes exceeds 40% of budget (catches Collect
        // accumulators that grow unbounded per group).
        if self.memory_budget > 0
            && (self.groups.len() >= self.max_groups
                || self.value_heap_bytes > self.memory_budget * 40 / 100)
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
        let row_u32 = row_num as u32;
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
        // capacity (one inner Vec per row).
        let row_value_count = row_values.len();
        let row_charge = std::mem::size_of::<Value>().saturating_mul(row_value_count)
            + row_heap_bytes
            + std::mem::size_of::<Vec<Value>>()
            + std::mem::size_of::<u32>();
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
        group_state.contributions.push(row_values);
        group_state.input_rows.push(row_u32);
        self.value_heap_bytes = self.value_heap_bytes.saturating_add(row_charge);

        // 6. Metadata common-only propagation, identical shape to
        //    fold-mode. Buffer-mode does not defer metadata observation
        //    to the rollback step because conflict tracking is
        //    associative — rebuilding it from scratch would mean
        //    re-walking every contribution's metadata at finalize.
        if record.has_meta() {
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

        // 7. Spill trigger — same dual-threshold check as fold-mode.
        //    `groups.len()` is always 0 in buffer-mode; the buffered
        //    map's group count is the relevant signal.
        if self.memory_budget > 0
            && (self.buffered_groups.len() >= self.max_groups
                || self.value_heap_bytes > self.memory_budget * 40 / 100)
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
    pub(crate) fn retract_row(&mut self, input_row_id: u32) -> Result<(), HashAggError> {
        if !self.spill_files.is_empty() {
            return Err(HashAggError::Spill(format!(
                "retract_row {input_row_id} called on aggregator with spilled groups; \
                 in-place retraction is only available against in-memory state"
            )));
        }

        if self.buffer_mode {
            for state in self.buffered_groups.values_mut() {
                if let Some(idx) = state.input_rows.iter().position(|r| *r == input_row_id) {
                    let removed = state.contributions.remove(idx);
                    state.input_rows.remove(idx);
                    let row_charge = std::mem::size_of::<Value>().saturating_mul(removed.len())
                        + removed.iter().map(Value::heap_size).sum::<usize>()
                        + std::mem::size_of::<Vec<Value>>()
                        + std::mem::size_of::<u32>();
                    self.value_heap_bytes = self.value_heap_bytes.saturating_sub(row_charge);
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
            let Some(idx) = state.input_rows.iter().position(|r| *r == input_row_id) else {
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
                + std::mem::size_of::<u32>();
            self.value_heap_bytes = self.value_heap_bytes.saturating_sub(removed_row_charge);
            // Negative `total_delta` is a shrink; saturating-sub clamps
            // at zero against the running total.
            if total_delta < 0 {
                self.value_heap_bytes = self
                    .value_heap_bytes
                    .saturating_sub((-total_delta) as usize);
            } else {
                self.value_heap_bytes = self.value_heap_bytes.saturating_add(total_delta as usize);
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
        let mut prepared: Vec<(Vec<u8>, usize)> = Vec::with_capacity(drained.len());
        for (idx, (key, _state)) in drained.iter().enumerate() {
            let mut values: Vec<Value> = Vec::with_capacity(gb_count + 2);
            for gk in key {
                values.push(gk.to_value());
            }
            // Pad to match spill_schema column count for SortKeyEncoder.
            values.push(Value::Null);
            values.push(Value::Null);
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
        self.value_heap_bytes = 0;
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
    ///    through a `LoserTree`, merging `AccumulatorRow` +
    ///    `MetadataCommonTracker` partials at each key boundary, and
    ///    route each finalized group through the same
    ///    `finalize_group` helper so both paths agree byte-for-byte.
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
    /// and `MetadataCommonTracker` partials associatively (both
    /// operations are commutative and associative across spill files).
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

// ---------------------------------------------------------------------------
// Binary aggregation spill infrastructure (postcard + LZ4)
// ---------------------------------------------------------------------------

/// Merge entry for the binary aggregation spill path. Ordered by
/// pre-encoded sort key bytes for the `LoserTree` k-way merge.
struct AggMergeEntry {
    sort_key: Vec<u8>,
    group_key: Vec<GroupByKey>,
    state: SpillState,
}

impl PartialEq for AggMergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.sort_key == other.sort_key
    }
}
impl Eq for AggMergeEntry {}
impl PartialOrd for AggMergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for AggMergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sort_key.cmp(&other.sort_key)
    }
}

/// Per-group payload an `AggSpillEntry` carries. The spill format
/// stays a single entry shape — fold-mode aggregators write only
/// `Folded`, buffer-mode aggregators write only `Buffered`, the merge
/// path branches on the variant. One on-disk format covers both
/// retraction-strategy paths so the spill writer/reader/merger pair is
/// not duplicated per mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpillState {
    /// Fold-mode payload: per-group accumulator row + sidecars.
    Folded(AggregatorGroupState),
    /// Buffer-mode payload: per-row raw contributions + sidecars.
    Buffered(BufferedGroupState),
}

/// Binary spill entry serialized to disk via postcard + LZ4.
#[derive(Serialize, Deserialize)]
struct AggSpillEntry {
    sort_key: Vec<u8>,
    group_key: Vec<GroupByKey>,
    state: SpillState,
}

/// Binary aggregation spill writer. Writes `AggSpillEntry` records as
/// length-prefixed postcard payloads into an LZ4 frame-compressed temp
/// file. Postcard has no record-level framing of its own, so each entry
/// is preceded by a 4-byte little-endian u32 holding the encoded
/// payload length. No schema header — the caller knows the group-by
/// layout at construction time.
struct AggSpillWriter {
    encoder: lz4_flex::frame::FrameEncoder<std::io::BufWriter<tempfile::NamedTempFile>>,
}

impl AggSpillWriter {
    fn new(spill_dir: Option<&std::path::Path>) -> Result<Self, HashAggError> {
        let temp_file = if let Some(dir) = spill_dir {
            tempfile::NamedTempFile::new_in(dir)
        } else {
            tempfile::NamedTempFile::new()
        }
        .map_err(|e| HashAggError::Spill(e.to_string()))?;
        let buf_writer = std::io::BufWriter::new(temp_file);
        let encoder = lz4_flex::frame::FrameEncoder::new(buf_writer);
        Ok(Self { encoder })
    }

    fn write_entry(&mut self, entry: &AggSpillEntry) -> Result<(), HashAggError> {
        use std::io::Write;
        let bytes = postcard::to_stdvec(entry).map_err(|e| HashAggError::Spill(e.to_string()))?;
        let len = u32::try_from(bytes.len())
            .map_err(|_| HashAggError::Spill("spill entry exceeds 4 GiB".to_string()))?;
        self.encoder
            .write_all(&len.to_le_bytes())
            .map_err(|e| HashAggError::Spill(e.to_string()))?;
        self.encoder
            .write_all(&bytes)
            .map_err(|e| HashAggError::Spill(e.to_string()))?;
        Ok(())
    }

    fn finish(self) -> Result<AggSpillFile, HashAggError> {
        let buf_writer = self
            .encoder
            .finish()
            .map_err(|e| HashAggError::Spill(e.to_string()))?;
        let temp_file = buf_writer
            .into_inner()
            .map_err(|e| HashAggError::Spill(e.into_error().to_string()))?;
        let path = temp_file.into_temp_path();
        Ok(AggSpillFile { path })
    }
}

/// Completed binary spill file handle. Auto-deletes on drop via `TempPath`.
pub struct AggSpillFile {
    path: tempfile::TempPath,
}

impl AggSpillFile {
    fn reader(&self) -> Result<AggSpillReader, HashAggError> {
        let file =
            std::fs::File::open(&self.path).map_err(|e| HashAggError::Spill(e.to_string()))?;
        let decoder = lz4_flex::frame::FrameDecoder::new(file);
        let buf_reader = std::io::BufReader::new(decoder);
        Ok(AggSpillReader {
            reader: buf_reader,
            len_buf: [0u8; 4],
        })
    }
}

/// Binary aggregation spill reader. Yields `AggMergeEntry` by reading a
/// 4-byte little-endian length prefix followed by a postcard-encoded
/// payload from an LZ4 frame-compressed file. A clean-boundary EOF on
/// the length prefix terminates the stream cleanly; any other read or
/// decode failure surfaces as `HashAggError::Spill`.
struct AggSpillReader {
    reader: std::io::BufReader<lz4_flex::frame::FrameDecoder<std::fs::File>>,
    len_buf: [u8; 4],
}

impl AggSpillReader {
    fn next_entry(&mut self) -> Result<Option<AggMergeEntry>, HashAggError> {
        use std::io::Read;
        match self.reader.read_exact(&mut self.len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(HashAggError::Spill(e.to_string())),
        }
        let len = u32::from_le_bytes(self.len_buf) as usize;
        let mut buf = vec![0u8; len];
        self.reader
            .read_exact(&mut buf)
            .map_err(|e| HashAggError::Spill(e.to_string()))?;
        let entry: AggSpillEntry =
            postcard::from_bytes(&buf).map_err(|e| HashAggError::Spill(e.to_string()))?;
        Ok(Some(AggMergeEntry {
            sort_key: entry.sort_key,
            group_key: entry.group_key,
            state: entry.state,
        }))
    }
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
// StreamingAggregator<Op: AccumulatorOp> — raw + merge modes
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// AccumulatorOp trait + AddRaw / MergeState impls
// ---------------------------------------------------------------------------
//
// The trait lives here in `clinker-core/src/aggregation.rs` (NOT
// `clinker-record`) because `MergeState::Input` references
// `AggregatorGroupState`, which in turn holds a `MetadataCommonTracker`
// defined alongside the hash aggregator. `clinker-record` has no
// dependency on `cxl`, so the `apply_row` signature — which takes
// `&[AggregateBinding]` — would also be unexpressible there.

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
///   comparator) plus the full per-group state: the `AccumulatorRow`,
///   `min_row_num`, and the `MetadataCommonTracker`. `apply_row` folds
///   the incoming state into the currently-open per-group `row` via
///   `AccumulatorEnum::merge`. The `min_row_num` + tracker merges live
///   on `GroupBoundary::push` because they operate on the open group's
///   full `AggregatorGroupState`, not on the `row` alone.
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
        // `min_row_num` and `MetadataCommonTracker` live on
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

/// Associative merge of per-group `min_row_num` plus the metadata
/// common tracker. Operates on a currently-open group's
/// `AggregatorGroupState` and folds another state's state in.
///
/// Separated out so both the `GroupBoundary` state machine (for the
/// raw-streaming path) and the spill-recovery path can call the
/// identical reduction logic. Exposed at crate scope for tests.
pub(crate) fn merge_group_sidecars(dst: &mut AggregatorGroupState, src: AggregatorGroupState) {
    // `min_row_num`: monotonic min. `u64::MAX` is the identity.
    if src.min_row_num < dst.min_row_num {
        dst.min_row_num = src.min_row_num;
    }
    // `MetadataCommonTracker`: associative merge per D11 revised.
    dst.meta_tracker.merge(src.meta_tracker);
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
    dst.meta_tracker.merge(src.meta_tracker);
    dst.input_rows.extend(src.input_rows);
    dst.contributions.extend(src.contributions);
}

/// Fold a `BufferedGroupState`'s contributions into a fresh
/// `AggregatorGroupState`, ready for `finalize_group_inner`. Each row
/// in `contributions` is shaped by the bindings: scalar arg variants
/// (`Field`, `Wildcard`, `Expr`) consume one slot; `Pair` consumes two
/// (value + weight) — matched against the binding shape so
/// `add_weighted` sees the original `(value, weight)` pair.
fn fold_buffered_state(
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
        meta_tracker: buffered.meta_tracker,
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

// ---------------------------------------------------------------------------
// Plan-time streaming eligibility
// ---------------------------------------------------------------------------

/// Outcome of evaluating whether an aggregation can run in streaming mode
/// given its parent node's physical properties. Returned by
/// [`qualifies_for_streaming`] and consumed by the planner lowering to
/// pick between `PlanNode::StreamingAggregation` and
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
/// Rules:
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
        OP::DestroyedByCombine { at_node, .. } => {
            format!("destroyed by combine `{}`", at_node)
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
        // If `MergeState::Input` ever regresses to
        // `(Vec<u8>, AccumulatorRow)` this test stops compiling.
        fn assert_input_is_full_state(_: &<MergeState as AccumulatorOp>::Input) {}
        let st = state_with_row(Vec::new());
        let input: (Vec<u8>, AggregatorGroupState) = (Vec::new(), st);
        assert_input_is_full_state(&input);
        // Confirm `min_row_num` + tracker are addressable on the
        // `Input` type — if they are missing this line fails to
        // compile.
        let _ = input.1.min_row_num;
        let _ = &input.1.meta_tracker;
    }

    // ----- qualifies_for_streaming -----

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
            ck_set: std::collections::BTreeSet::new(),
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
            ck_set: std::collections::BTreeSet::new(),
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
            ck_set: std::collections::BTreeSet::new(),
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
            .filter(|e| !e.is_meta)
            .map(|e| e.output_name.clone())
            .collect();
        let output_schema = Arc::new(Schema::new(output_columns));

        let mut spill_cols: Vec<Box<str>> = group_by_owned
            .iter()
            .map(|s| Box::<str>::from(s.as_str()))
            .collect();
        spill_cols.push("__acc_state".into());
        spill_cols.push("__meta_tracker".into());
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
        )
    }

    fn ctx_for<'a>(stable: &'a StableEvalContext, file: &'a Arc<str>, row: u64) -> EvalContext<'a> {
        EvalContext {
            stable,
            source_file: file,
            source_row: row,
        }
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
        let rows: Vec<u32> = lineage.iter().map(|(r, _)| *r).collect();
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
        let mut input_row_lists: Vec<Vec<u32>> =
            groups.values().map(|s| s.input_rows.clone()).collect();
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
        let mut by_key: std::collections::HashMap<String, Vec<u32>> =
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
                    .extend(folded.input_rows.iter().copied());
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
                .extend(state.input_rows.iter().copied());
        }

        for (k, mut rows) in by_key.into_iter() {
            rows.sort();
            // Recover the expected row indices for this key from the
            // input pattern: positions where `keys[i % 4] == k`.
            let key_idx = keys.iter().position(|kk| *kk == k).expect("known key");
            let expected: Vec<u32> = (0..32u32)
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
        let mut a_rows: Vec<u32> = agg
            .buffered_groups
            .iter()
            .find(|(k, _)| matches!(&k[0], GroupByKey::Str(s) if s.as_ref() == "a"))
            .map(|(_, s)| s.input_rows.clone())
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
        let mut by_key: std::collections::HashMap<String, Vec<(u32, i64)>> =
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
                for (row, contrib) in buffered
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
            for (row, contrib) in state.input_rows.iter().zip(state.contributions.iter()) {
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
            let expected: Vec<(u32, i64)> = (0..32u32)
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
        retract: &[u32],
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
        for &row_id in retract {
            full_agg.retract_row(row_id).expect("retract_row");
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
}
