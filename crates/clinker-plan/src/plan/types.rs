//! Plan-layer enums shared across the planner, executor, and
//! aggregation engine.
//!
//! These are compile-time selections baked into the execution plan: the
//! side a combine match came from ([`JoinSide`]) and the aggregation
//! algorithm chosen for a grouped reduction ([`AggregateStrategy`]).
//! They live in the plan layer because the planner produces them and the
//! lower runtime layers merely consume them — keeping the definitions
//! here lets `plan/` stay free of any upward import from `executor/` or
//! `aggregation/`.

use serde::{Deserialize, Serialize};

/// Which side of a combine a matched record came from.
///
/// `Probe` is the driver (streaming) side; `Build` is the materialized
/// hash-table side. A combine has exactly one probe qualifier and one
/// build qualifier — N-ary user-authored combines are rewritten by the
/// plan-time decomposition pass into a chain of binary combines, so by
/// the time this enum is consulted every combine in the DAG is binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinSide {
    Build,
    Probe,
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
