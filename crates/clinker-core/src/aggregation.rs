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

use clinker_record::accumulator::{AccumulatorEnum, AccumulatorRow};
use clinker_record::group_key::value_to_group_key;
use clinker_record::schema::Schema;
use clinker_record::{GroupByKey, Record, RecordStorage, Value};
use cxl::ast::{BinOp, Expr, LiteralValue, UnaryOp};
use cxl::eval::{EvalContext, EvalError, ProgramEvaluator, eval_expr};
use cxl::plan::{AggregateBinding, BindingArg, CompiledAggregate};

use crate::config::SortField;
use crate::pipeline::spill::SpillFile;

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
}

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
        }
    }
}

impl std::error::Error for HashAggError {}

impl From<EvalError> for HashAggError {
    fn from(e: EvalError) -> Self {
        HashAggError::EvalFailed(e)
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
    #[allow(dead_code)]
    spill_schema: Arc<Schema>,
    #[allow(dead_code)]
    spill_sort_fields: Vec<SortField>,
    #[allow(dead_code)]
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
    pub fn add_record(&mut self, record: &Record, ctx: &EvalContext) -> Result<(), HashAggError> {
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
            .or_insert_with(|| AggregatorGroupState {
                row: self.factory.create_accumulators(),
                meta_tracker: MetadataCommonTracker::new(),
            });

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

    /// Spill the in-memory groups to disk. Stub until 16.3.10 lands the
    /// real writer; the surrounding 16.3 sub-tasks call `spill()` from
    /// the resize-aware trigger so the symbol must exist for the crate
    /// to compile in the interim. Tests in 16.3.8 use a memory budget
    /// of 0 (disabled) or a budget large enough to keep the trigger
    /// from firing.
    fn spill(&mut self) -> Result<(), HashAggError> {
        Err(HashAggError::Spill(
            "spill path not yet implemented (Task 16.3.10)".into(),
        ))
    }
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
