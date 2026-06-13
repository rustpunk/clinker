//! Dead-letter-queue vocabulary: the error-category enum and the
//! stage-label helpers shared between the executor (which constructs DLQ
//! entries) and the CLI sink (which writes them out).

/// DLQ error categories. Passed from the error site — no string
/// matching. Each error path constructs the correct variant at the
/// point of failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlqErrorCategory {
    MissingRequiredField,
    TypeCoercionFailure,
    RequiredFieldConversionFailure,
    NanInOutputField,
    AggregateTypeError,
    ValidationFailure,
    /// Aggregate finalize-time failure (e.g. SumOverflow during finalize()).
    /// Distinct from `AggregateTypeError`, which fires during the per-record
    /// add path. Routed by the executor's aggregation dispatch arm.
    AggregateFinalize,
    /// Collateral entry emitted for non-failing records in a correlation
    /// group whose group was DLQ'd because some other record failed. The
    /// sibling root-cause entry carries `trigger: true` and the original
    /// failure category; collaterals carry `trigger: false` and this
    /// category. Emitted only by the `CorrelationCommit` arm.
    Correlated,
    /// A correlation-key group exceeded `error_handling.max_group_buffer`.
    /// One entry per group with `trigger: true` plus collaterals for the
    /// other buffered records of the same group.
    GroupSizeExceeded,
    /// Record arrived at a time-windowed aggregate after the window
    /// covering its event-time had already closed
    /// (`window_end + allowed_lateness < min_across_sources`). Routed
    /// by the executor's aggregation dispatch arm when `time_window`
    /// is set. Mirrors Flink sideOutputLateData / Beam late-drop /
    /// Spark window late-drop.
    LateRecord,
    /// Per-record `emit each` fan-out produced more output records
    /// than the transform's `max_expansion` ceiling allows. The
    /// originating record is routed to DLQ before the fan-out can
    /// emit any of its truncated body records.
    ExpansionLimitExceeded,
    /// A Combine output-stage CXL evaluation or coercion failed for one
    /// driver row — probe-key extraction, residual-filter eval, or the
    /// matched / `on_miss: null_fields` body eval. Distinct from the
    /// upstream-Transform `TypeCoercionFailure` because the failing row
    /// carries contributing-build lineage: the entry is attributed to
    /// the contributing input source(s) rather than the synthetic merged
    /// source, and the failure rewinds each contributing source's
    /// rollback cursor to the captured pre-fold floor so a downstream
    /// resume reprocesses both the driver and the matched build row.
    /// Routed only under `Continue` / `BestEffort`; `FailFast` propagates
    /// the eval error unchanged. Recovery is uniform across every Combine
    /// join mode — the inline hash build-probe arm and the IEJoin,
    /// grace-hash, and sort-merge kernels all route an output-eval failure
    /// here under `Continue` / `BestEffort`.
    CombineOutputRow,
    /// Two Reshape rules wrote the same field on the same correlation-
    /// group row at runtime (a content-dependent collision the
    /// compile-time static-overlap check could not prove). The whole
    /// correlation group rolls back per `CorrelationCommit` semantics —
    /// no synthesized or mutated rows from that group reach output.
    /// Stage label `reshape:<node>:<rule_a>+<rule_b>` names the colliding
    /// rule pair.
    MutationConflict,
}

impl DlqErrorCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MissingRequiredField => "missing_required_field",
            Self::TypeCoercionFailure => "type_coercion_failure",
            Self::RequiredFieldConversionFailure => "required_field_conversion_failure",
            Self::NanInOutputField => "nan_in_output_field",
            Self::AggregateTypeError => "aggregate_type_error",
            Self::ValidationFailure => "validation_failure",
            Self::AggregateFinalize => "aggregate_finalize",
            Self::Correlated => "correlated",
            Self::GroupSizeExceeded => "group_size_exceeded",
            Self::LateRecord => "late_record",
            Self::ExpansionLimitExceeded => "expansion_limit_exceeded",
            Self::CombineOutputRow => "combine_output_row",
            Self::MutationConflict => "mutation_conflict",
        }
    }
}

/// Stage label helper for Reshape mutation-conflict DLQ entries. Names
/// the colliding rule pair so a reader scanning the DLQ can identify
/// which two rules wrote the same field on the same row.
pub fn stage_reshape_mutation_conflict(node: &str, rule_a: &str, rule_b: &str) -> String {
    format!("reshape:{node}:{rule_a}+{rule_b}")
}

/// Stage label helper for aggregate-transform DLQ entries.
pub fn stage_aggregate(transform: &str) -> String {
    format!("aggregate:{transform}")
}

/// Stage label helper for time-windowed aggregate DLQ entries
/// emitted on late-record drop. Distinct from `stage_aggregate` so a
/// reader scanning the DLQ can tell a late-arrival drop apart from a
/// finalize-time accumulator failure on the same node.
pub fn stage_time_window(transform: &str) -> String {
    format!("time_window:{transform}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dlq_category_as_str_all_variants() {
        assert_eq!(
            DlqErrorCategory::MissingRequiredField.as_str(),
            "missing_required_field"
        );
        assert_eq!(
            DlqErrorCategory::TypeCoercionFailure.as_str(),
            "type_coercion_failure"
        );
        assert_eq!(
            DlqErrorCategory::RequiredFieldConversionFailure.as_str(),
            "required_field_conversion_failure"
        );
        assert_eq!(
            DlqErrorCategory::NanInOutputField.as_str(),
            "nan_in_output_field"
        );
        assert_eq!(
            DlqErrorCategory::AggregateTypeError.as_str(),
            "aggregate_type_error"
        );
        assert_eq!(
            DlqErrorCategory::ValidationFailure.as_str(),
            "validation_failure"
        );
        assert_eq!(
            DlqErrorCategory::AggregateFinalize.as_str(),
            "aggregate_finalize"
        );
        assert_eq!(DlqErrorCategory::Correlated.as_str(), "correlated");
        assert_eq!(
            DlqErrorCategory::GroupSizeExceeded.as_str(),
            "group_size_exceeded"
        );
        assert_eq!(DlqErrorCategory::LateRecord.as_str(), "late_record");
        assert_eq!(
            DlqErrorCategory::ExpansionLimitExceeded.as_str(),
            "expansion_limit_exceeded"
        );
        assert_eq!(
            DlqErrorCategory::CombineOutputRow.as_str(),
            "combine_output_row"
        );
        assert_eq!(
            DlqErrorCategory::MutationConflict.as_str(),
            "mutation_conflict"
        );
    }

    #[test]
    fn test_stage_reshape_mutation_conflict_helper() {
        assert_eq!(
            stage_reshape_mutation_conflict("backfill", "fix_a", "fix_b"),
            "reshape:backfill:fix_a+fix_b"
        );
    }

    #[test]
    fn test_stage_aggregate_helper() {
        assert_eq!(stage_aggregate("daily_totals"), "aggregate:daily_totals");
    }

    #[test]
    fn test_stage_time_window_helper() {
        assert_eq!(
            stage_time_window("hourly_clicks"),
            "time_window:hourly_clicks"
        );
    }
}
