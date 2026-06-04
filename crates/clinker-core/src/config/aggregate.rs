//! Aggregate and Combine node configuration, including correlation keys.

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

/// User-supplied hint for aggregation execution strategy.
///
/// `Auto` (default) lets the optimizer pick Hash vs Streaming based on
/// upstream `OrderingProvenance` and the `qualifies_for_streaming` rules.
/// `Hash` and `Streaming` are user overrides modeled on Informatica's
/// `sorted_input` flag — `Streaming` is a declared performance contract:
/// if the input is not provably sorted for the group-by keys, the
/// planner hard-errors at compile time rather than silently inserting
/// a sort.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateStrategyHint {
    /// Optimizer chooses based on upstream ordering (default).
    #[default]
    Auto,
    /// Force hash aggregation regardless of input ordering.
    Hash,
    /// Require streaming aggregation; compile-time error if input is
    /// not provably sorted for the group-by keys.
    Streaming,
}

/// Configuration for GROUP BY aggregation on a transform.
///
/// Nested `aggregate:` block follows the universal ETL pattern (Beam YAML
/// Combine, SOPE `group_by`, Informatica Aggregator).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateConfig {
    /// Fields to group by. Empty = global fold (one output row).
    #[serde(default)]
    pub group_by: Vec<String>,
    /// CXL source with aggregate function calls.
    pub cxl: String,
    /// User-supplied execution strategy hint. Defaults to `Auto`.
    /// Resolved to a concrete `AggregateStrategy` by the
    /// `select_aggregation_strategies` post-pass in 16.4.9.
    #[serde(default)]
    pub strategy: AggregateStrategyHint,
    /// Event-time window declaration. When `Some`, the aggregate
    /// dispatch arm routes through the time-windowed path: each record
    /// is assigned to its window(s) by `$source.event_time`, per-(key,
    /// window) state accumulates independently, and emission gates on
    /// `min_across_sources >= window_end + allowed_lateness`. Mirrors
    /// `AggregateBody.time_window`. `None` (default) keeps today's
    /// positional aggregate semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_window: Option<crate::config::pipeline_node::TimeWindowSpec>,
    /// Operator-side late-record tolerance for the time-windowed path.
    /// Records arriving at a window after
    /// `min_across_sources >= window_end + allowed_lateness` route to
    /// the DLQ as `LateRecord`. Ignored when `time_window` is `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_lateness: Option<std::time::Duration>,
}

/// User-supplied hint for combine execution strategy.
///
/// `Auto` (default) lets [`crate::plan::combine::select_combine_strategies`]
/// pick from predicate shape and cardinality estimates. `GraceHash` is a
/// user override that forces the disk-spilling partitioned hash join even
/// when cardinality estimates are absent or below the soft-limit threshold
/// — useful for benchmarks and for production pipelines where the user
/// knows the build side does not fit in memory.
///
/// Mirrors [`AggregateStrategyHint`] in shape. `GraceHash` only applies to
/// pure-equi predicates; the planner ignores the hint on mixed equi+range
/// or pure-range nodes, where partition-IEJoin / IEJoin / SortMerge remain
/// the correct strategies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CombineStrategyHint {
    /// Optimizer picks the strategy (default).
    #[default]
    Auto,
    /// Force grace hash join — disk-spilling partitioned hash. Applies
    /// only to pure-equi predicates; ignored otherwise.
    GraceHash,
}

/// Correlation key for grouped DLQ rejection.
///
/// When set on [`ErrorHandlingConfig`], every record in a group sharing a
/// correlation-key value is DLQ'd atomically if any single record in that
/// group fails Transform / Route eval / Output write. Group identity is
/// fixed at ingest: a Transform that rewrites the key field does not
/// change a row's group. Custom [`Deserialize`] accepts a YAML scalar
/// (`correlation_key: foo`) for a single-field key or a sequence
/// (`correlation_key: [a, b]`) for a compound key.
#[derive(Debug, Clone, Serialize)]
pub enum CorrelationKey {
    Single(String),
    Compound(Vec<String>),
}

impl CorrelationKey {
    /// Field names that compose this correlation key, in declaration order.
    pub fn fields(&self) -> Vec<&str> {
        match self {
            Self::Single(f) => vec![f.as_str()],
            Self::Compound(fs) => fs.iter().map(|f| f.as_str()).collect(),
        }
    }
}

impl<'de> Deserialize<'de> for CorrelationKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CorrelationKeyVisitor;

        impl<'de> Visitor<'de> for CorrelationKeyVisitor {
            type Value = CorrelationKey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string (single key) or array of strings (compound key)")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(CorrelationKey::Single(v.to_string()))
            }

            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut fields = Vec::new();
                while let Some(field) = seq.next_element::<String>()? {
                    fields.push(field);
                }
                if fields.is_empty() {
                    return Err(de::Error::custom("correlation_key array must not be empty"));
                }
                if fields.len() == 1 {
                    return Ok(CorrelationKey::Single(fields.remove(0)));
                }
                Ok(CorrelationKey::Compound(fields))
            }
        }

        deserializer.deserialize_any(CorrelationKeyVisitor)
    }
}

/// Selects how a triggered correlation group's collateral records are
/// disposed at commit time.
///
/// Resolution precedence (latter wins): per-pipeline default →
/// per-Combine override (per-input-set fan-out shape) → per-Output override
/// (per-sink fan-out shape). The override surface lets audit-style sinks
/// keep failing-group records that an integrity-style sink would discard.
///
/// * `Any` — every record sharing any correlation-key field with a
///   triggering record is collateral-DLQ'd. The default; matches "if any
///   contributing source had bad data, the joined output is suspect."
/// * `All` — only records sharing the FULL correlation-key tuple with a
///   trigger are collateral-DLQ'd. Records that derived only some CK
///   columns from a failing source pass through to the writer.
/// * `Primary` — only records on the primary correlation-key field
///   (first-listed in the source's `correlation_key`) face collateral
///   rollback. Audit-dump opt-out for sinks that retain enough provenance
///   to accept partial-rollback semantics.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CorrelationFanoutPolicy {
    #[default]
    Any,
    All,
    Primary,
}
