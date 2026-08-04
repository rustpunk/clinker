//! Dead-letter queue entry produced when a record fails evaluation.

use std::sync::Arc;

use clinker_record::Record;

use crate::executor::diagnostic_preview::{DiagnosticPreview, build_diagnostic_preview};

/// One decoded source row rejected by its authored type declaration.
#[derive(Debug, Clone)]
pub(crate) struct TypeErrorEvent {
    pub(crate) source_row: crate::executor::stream_event::SourceRowId,
    pub(crate) source_name: Arc<str>,
    pub(crate) source_file: Arc<str>,
    pub(crate) row: u64,
    pub(crate) column: usize,
    pub(crate) field: Box<str>,
    pub(crate) declared_type: Box<str>,
    pub(crate) original_byte_length: usize,
    pub(crate) preview: DiagnosticPreview,
    pub(crate) diagnostic_code: &'static str,
    pub(crate) message: String,
    pub(crate) original_record: Record,
    pub(crate) original_value: clinker_record::Value,
}

impl TypeErrorEvent {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
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
        Self {
            source_row,
            source_name,
            source_file,
            row: source_row.ordinal(),
            column,
            field: field.into_boxed_str(),
            declared_type: declared_type.into_boxed_str(),
            original_byte_length: preview.original_byte_length,
            preview,
            diagnostic_code: "E126",
            message,
            original_record,
            original_value,
        }
    }

    /// Single-line bounded diagnostic suitable for stderr and DLQ reason text.
    pub(crate) fn diagnostic_message(&self) -> String {
        format!(
            "[{}] source={:?} file={:?} row={} column={} field={:?} declared_type={} \
             preview=\"{}\" original_bytes={}: {}",
            self.diagnostic_code,
            self.source_name,
            self.source_file,
            self.row,
            self.column,
            self.field,
            self.declared_type,
            self.preview.rendered,
            self.original_byte_length,
            self.message,
        )
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
