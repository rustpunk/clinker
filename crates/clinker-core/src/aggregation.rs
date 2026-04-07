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

use clinker_record::accumulator::{AccumulatorEnum, AccumulatorRow};
use clinker_record::{GroupByKey, Record, Value};
use cxl::ast::{BinOp, Expr, LiteralValue, UnaryOp};
use cxl::plan::{AggregateBinding, CompiledAggregate};

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
