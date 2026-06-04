//! Dead-letter queue entry produced when a record fails evaluation.

use std::sync::Arc;

use clinker_record::Record;

/// Record that failed evaluation, queued for DLQ output.
#[derive(Debug, Clone)]
pub struct DlqEntry {
    pub source_row: u64,
    pub category: crate::dlq::DlqErrorCategory,
    pub error_message: String,
    pub original_record: Record,
    /// Pipeline stage where error occurred.
    /// Convention: "source", "transform:{name}", "route_eval", "output:{name}"
    pub stage: Option<String>,
    /// Route branch name if error occurred during or after routing.
    /// None for pre-routing errors.
    pub route: Option<String>,
    /// `true` if this record's own evaluation caused the DLQ entry.
    /// Serialized as `_cxl_dlq_trigger` column in DLQ CSV.
    pub trigger: bool,
    /// Originating Source-node name. Read from the failing record's
    /// `FieldMetadata::SourceName` engine-stamp at the push site so a
    /// post-Merge / post-Combine DLQ entry still identifies which
    /// upstream Source produced the record. Serialized as
    /// `_cxl_dlq_source_name` in DLQ CSV.
    pub source_name: Arc<str>,
    /// Output field the evaluator was computing when the error fired,
    /// captured at the emit-statement boundary. `None` for collateral
    /// entries that were not directly eval-triggered (correlation
    /// fan-out, group-size overflow, etc.). Serialized as
    /// `_cxl_dlq_triggering_field`.
    pub triggering_field: Option<Arc<str>>,
    /// Value carried by the failing `EvalErrorKind` payload, when the
    /// variant exposes one (conversion source string, out-of-bounds
    /// index, mismatched arity). `None` otherwise. Serialized as
    /// `_cxl_dlq_triggering_value`.
    pub triggering_value: Option<clinker_record::Value>,
}

impl DlqEntry {
    /// Stage: source read error.
    pub fn stage_source() -> String {
        "source".into()
    }

    /// Stage: transform evaluation error.
    pub fn stage_transform(name: &str) -> String {
        format!("transform:{name}")
    }

    /// Stage: route condition evaluation error.
    pub fn stage_route_eval() -> String {
        "route_eval".into()
    }

    /// Stage: output write error.
    pub fn stage_output(name: &str) -> String {
        format!("output:{name}")
    }

    /// Stage: Combine output-stage evaluation error (probe-key, residual,
    /// or body eval for one driver row).
    pub fn stage_combine(name: &str) -> String {
        format!("combine:{name}")
    }
}
