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

    /// Single-line bounded diagnostic suitable for stderr and DLQ reason text.
    pub(crate) fn diagnostic_message(&self) -> &str {
        &self.message
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
