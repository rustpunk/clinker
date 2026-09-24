//! Dead-letter queue entry produced when a record fails evaluation.

use std::sync::Arc;

use clinker_record::Record;
use serde::{Deserialize, Serialize};

use crate::executor::diagnostic_preview::build_diagnostic_preview;

/// Runtime disposition class for one rejected source attempt. Kept beside the
/// source event rather than inferred from diagnostic text, and serialized by
/// the source-order spill path when ordered input is staged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum SourceRejectionKind {
    DeclaredType,
    UnknownRecordType,
    FanOutLimit,
}

impl SourceRejectionKind {
    pub(crate) const fn category(self) -> clinker_core_types::dlq::DlqErrorCategory {
        match self {
            Self::DeclaredType => clinker_core_types::dlq::DlqErrorCategory::TypeCoercionFailure,
            Self::UnknownRecordType => {
                clinker_core_types::dlq::DlqErrorCategory::StructuralValidation
            }
            Self::FanOutLimit => clinker_core_types::dlq::DlqErrorCategory::ExpansionLimitExceeded,
        }
    }

    pub(crate) const fn counts_as_type_error(self) -> bool {
        matches!(self, Self::DeclaredType)
    }
}

/// One decoded source attempt rejected before it could enter the DAG. The
/// complete original representation travels through the same bounded source
/// channel and spillable ordering barrier as successful records.
#[derive(Debug, Clone)]
pub(crate) struct SourceRejectionEvent {
    pub(crate) source_row: crate::executor::stream_event::SourceRowId,
    pub(crate) source_name: Arc<str>,
    pub(crate) source_file: Arc<str>,
    pub(crate) row: u64,
    pub(crate) kind: SourceRejectionKind,
    pub(crate) message: String,
    pub(crate) original_record: Record,
    pub(crate) triggering_field: Box<str>,
    pub(crate) triggering_value: clinker_record::Value,
    /// Taken by the reader when it rejected the attempt, so a rejection the
    /// ordering barrier held or spilled keeps the moment it was observed.
    pub(crate) failed_at: DlqFailureStamp,
}

impl SourceRejectionEvent {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn declared_type(
        source_row: crate::executor::stream_event::SourceRowId,
        source_name: Arc<str>,
        source_file: Arc<str>,
        column: usize,
        field: String,
        declared_type: String,
        original_record: Record,
        original_value: clinker_record::Value,
        message: String,
    ) -> Self {
        let raw = match &original_value {
            clinker_record::Value::String(value) => value.as_str().as_bytes().to_vec(),
            value => value.to_string().into_bytes(),
        };
        let preview = build_diagnostic_preview(&raw, false);
        // Keep the engine-authored reason on the same single-line token path
        // as the value preview. Current coercion reasons exclude input data;
        // this is defense-in-depth against a future error source echoing it.
        let message = build_diagnostic_preview(message.as_bytes(), false).rendered;
        let diagnostic = format!(
            "[E126] source={source_name:?} file={source_file:?} row={} column={column} \
             field={field:?} declared_type={declared_type} preview=\"{}\" \
             original_bytes={}: {message}",
            source_row.ordinal(),
            preview.rendered,
            preview.original_byte_length,
        );
        Self {
            source_row,
            source_name,
            source_file,
            row: source_row.ordinal(),
            kind: SourceRejectionKind::DeclaredType,
            message: diagnostic,
            original_record,
            triggering_field: field.into_boxed_str(),
            triggering_value: original_value,
            failed_at: DlqFailureStamp::now(),
        }
    }

    pub(crate) fn unknown_record_type(
        source_row: crate::executor::stream_event::SourceRowId,
        source_name: Arc<str>,
        source_file: Arc<str>,
        row: u64,
        original_record: Record,
        discriminator: String,
        message: String,
    ) -> Self {
        Self {
            source_row,
            source_name,
            source_file,
            row,
            kind: SourceRejectionKind::UnknownRecordType,
            message,
            original_record,
            triggering_field: "record_type".into(),
            triggering_value: clinker_record::Value::String(discriminator.into()),
            failed_at: DlqFailureStamp::now(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn fan_out_limit(
        source_row: crate::executor::stream_event::SourceRowId,
        source_name: Arc<str>,
        source_file: Arc<str>,
        field: String,
        limit: u64,
        actual: u128,
        original_record: Record,
    ) -> Self {
        Self {
            source_row,
            source_name,
            source_file,
            row: source_row.ordinal(),
            kind: SourceRejectionKind::FanOutLimit,
            message: format!(
                "source fan-out field {field:?} attempted row {actual}, exceeding \
                 `max_output_rows_per_input: {limit}`; the original input was rejected and no \
                 further fan-out rows were emitted"
            ),
            original_record,
            triggering_field: field.into_boxed_str(),
            triggering_value: clinker_record::Value::String(actual.to_string().into()),
            failed_at: DlqFailureStamp::now(),
        }
    }

    pub(crate) const fn category(&self) -> clinker_core_types::dlq::DlqErrorCategory {
        self.kind.category()
    }

    pub(crate) const fn counts_as_type_error(&self) -> bool {
        self.kind.counts_as_type_error()
    }

    pub(crate) fn estimated_heap_size(&self) -> usize {
        self.source_name
            .len()
            .saturating_add(self.source_file.len())
            .saturating_add(self.message.len())
            .saturating_add(self.triggering_field.len())
            .saturating_add(self.triggering_value.heap_size())
            .saturating_add(self.original_record.estimated_heap_size())
    }

    /// Retained rejection bytes not already charged by the executing ledger.
    /// Diagnostic and identity text retain their existing physical estimates.
    pub(crate) fn unaccounted_heap_size(
        &self,
        resources: &clinker_record::owned_storage::AllocationResources,
    ) -> usize {
        self.source_name
            .len()
            .saturating_add(self.source_file.len())
            .saturating_add(self.message.len())
            .saturating_add(self.triggering_field.len())
            .saturating_add(self.triggering_value.unaccounted_heap_size(resources))
            .saturating_add(self.original_record.unaccounted_heap_size(resources))
    }

    /// Single-line bounded diagnostic suitable for stderr and DLQ reason text.
    pub(crate) fn diagnostic_message(&self) -> &str {
        &self.message
    }
}

/// The identity and time of one dead-letter row, and the failure it belongs
/// to: the `_cxl_dlq_id`, `_cxl_dlq_trigger_id` and `_cxl_dlq_timestamp` of
/// the row it becomes.
///
/// Taken with [`Self::now`] where the engine observes the failure, and
/// carried unchanged through every place that holds the failure before its
/// row is written: the correlation and document buffers, side-thread
/// replays, combine kernels and the source ordering barrier. The row encoder
/// only renders it. The id is a UUIDv7 from the process-wide generator, so
/// ids are unique and increase in the order stamps are taken, on any thread.
///
/// The trigger id is the `_cxl_dlq_id` of the trigger row of the failure
/// that produced this row, so every row one failure produced shares it. A
/// trigger's trigger id is its own id ([`Self::now`]). A second row of the
/// same observed failure, such as a Combine build row, keeps it
/// ([`Self::sibling`]). A row the engine condemns because of another row's
/// failure, a correlation collateral or a rejected document's other record,
/// takes its cause's ([`Self::condemned_by`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DlqFailureStamp {
    id: uuid::Uuid,
    trigger_id: uuid::Uuid,
    at: chrono::DateTime<chrono::Utc>,
}

impl DlqFailureStamp {
    /// A fresh id and the current UTC time, for a trigger: the row is its
    /// own failure, so its trigger id is its id.
    pub fn now() -> Self {
        let id = uuid::Uuid::now_v7();
        Self {
            id,
            trigger_id: id,
            at: chrono::Utc::now(),
        }
    }

    /// The dead-letter row's `_cxl_dlq_id`.
    pub fn id(&self) -> uuid::Uuid {
        self.id
    }

    /// The `_cxl_dlq_id` of the trigger row of the failure this row belongs
    /// to: the row's `_cxl_dlq_trigger_id`.
    pub fn trigger_id(&self) -> uuid::Uuid {
        self.trigger_id
    }

    /// When the failure was observed: the row's `_cxl_dlq_timestamp`.
    pub fn at(&self) -> chrono::DateTime<chrono::Utc> {
        self.at
    }

    /// A stamp for a second dead letter produced by the same observed
    /// failure: the same time and trigger id under a fresh id, because every
    /// row's id is unique.
    pub(crate) fn sibling(&self) -> Self {
        Self {
            id: uuid::Uuid::now_v7(),
            trigger_id: self.trigger_id,
            at: self.at,
        }
    }

    /// A stamp for a row condemned now by `cause`'s failure: a fresh id and
    /// the current time, because the engine condemns the row here, and
    /// `cause`'s trigger id, because that failure took the row with it.
    pub(crate) fn condemned_by(cause: &DlqFailureStamp) -> Self {
        Self {
            id: uuid::Uuid::now_v7(),
            trigger_id: cause.trigger_id,
            at: chrono::Utc::now(),
        }
    }

    /// Rebuild a trigger stamp from the parts a spill payload kept: its
    /// trigger id is `id`. Its only caller, the source ordering barrier,
    /// spills source rejections, and every source rejection is its own
    /// trigger, stamped by [`Self::now`].
    pub(crate) fn from_parts(id: uuid::Uuid, at: chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            id,
            trigger_id: id,
            at,
        }
    }
}

/// Record that failed evaluation, queued for DLQ output.
#[derive(Debug, Clone)]
pub struct DlqEntry {
    pub source_row: crate::executor::stream_event::SourceRowId,
    pub category: clinker_core_types::dlq::DlqErrorCategory,
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
    /// When the failure behind this entry was observed, the row's id, and
    /// the id of the failure it belongs to. A trigger carries the stamp
    /// taken at its failure; a collateral entry carries one taken when its
    /// correlation group or document was condemned, naming the failure that
    /// condemned it. Serialized as `_cxl_dlq_id`, `_cxl_dlq_trigger_id` and
    /// `_cxl_dlq_timestamp`.
    pub failed_at: DlqFailureStamp,
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

#[cfg(test)]
mod tests {
    use super::DlqFailureStamp;

    #[test]
    fn stamp_now_is_its_own_failure() {
        let stamp = DlqFailureStamp::now();
        assert_eq!(stamp.trigger_id(), stamp.id());
        assert_eq!(stamp.id().get_version_num(), 7);
    }

    #[test]
    fn sibling_keeps_the_failure_under_a_new_id() {
        let trigger = DlqFailureStamp::now();
        let sibling = trigger.sibling();
        assert_ne!(sibling.id(), trigger.id(), "every row has its own id");
        assert_eq!(sibling.trigger_id(), trigger.id());
        assert_eq!(sibling.at(), trigger.at(), "the same observed failure");
        assert_eq!(
            sibling.sibling().trigger_id(),
            trigger.id(),
            "a sibling of a sibling still names the trigger"
        );
    }

    #[test]
    fn condemned_by_takes_the_cause_failure_with_its_own_id_and_time() {
        let trigger = DlqFailureStamp::now();
        let collateral = DlqFailureStamp::condemned_by(&trigger);
        assert_ne!(collateral.id(), trigger.id(), "every row has its own id");
        assert!(
            collateral.id() > trigger.id(),
            "the collateral is stamped after its cause"
        );
        assert!(
            collateral.at() >= trigger.at(),
            "condemned now, not at the cause's time"
        );
        assert_eq!(collateral.trigger_id(), trigger.id());

        let build = trigger.sibling();
        assert_eq!(
            DlqFailureStamp::condemned_by(&build).trigger_id(),
            trigger.id(),
            "a row condemned by a sibling names the original trigger"
        );
    }

    #[test]
    fn from_parts_rebuilds_a_trigger_stamp() {
        let trigger = DlqFailureStamp::now();
        let rebuilt = DlqFailureStamp::from_parts(trigger.id(), trigger.at());
        assert_eq!(rebuilt, trigger);
        assert_eq!(rebuilt.trigger_id(), rebuilt.id());
    }
}
