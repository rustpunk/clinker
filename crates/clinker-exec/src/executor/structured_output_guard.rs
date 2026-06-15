//! Runtime guard for structured writers that can frame only one envelope.
//!
//! EDIFACT, X12, HL7, and SWIFT writers currently build one structured
//! envelope for the record stream they receive. Until per-document structured
//! framing lands, a single writer must not receive records from more than one
//! concrete document grain: that would silently merge multiple documents into
//! one interchange/message envelope.

use std::sync::Arc;

use clinker_format::error::FormatError;
use clinker_plan::config::OutputFormat;
use clinker_plan::error::PipelineError;
use clinker_record::{DocumentContext, DocumentGrain};

#[derive(Debug, Clone, Copy)]
pub(crate) enum StructuredOutputFormat {
    Edifact,
    X12,
    Hl7,
    Swift,
}

impl StructuredOutputFormat {
    fn name(self) -> &'static str {
        match self {
            Self::Edifact => "edifact",
            Self::X12 => "x12",
            Self::Hl7 => "hl7",
            Self::Swift => "swift",
        }
    }

    fn error(self, message: String) -> PipelineError {
        let err = match self {
            Self::Edifact => FormatError::Edifact(message),
            Self::X12 => FormatError::X12(message),
            Self::Hl7 => FormatError::Hl7(message),
            Self::Swift => FormatError::Swift(message),
        };
        PipelineError::Format(err)
    }
}

pub(crate) fn structured_output_format(format: &OutputFormat) -> Option<StructuredOutputFormat> {
    match format {
        OutputFormat::Edifact(_) => Some(StructuredOutputFormat::Edifact),
        OutputFormat::X12(_) => Some(StructuredOutputFormat::X12),
        OutputFormat::Hl7(_) => Some(StructuredOutputFormat::Hl7),
        OutputFormat::Swift(_) => Some(StructuredOutputFormat::Swift),
        OutputFormat::Csv(_)
        | OutputFormat::Json(_)
        | OutputFormat::Xml(_)
        | OutputFormat::FixedWidth(_) => None,
    }
}

#[derive(Debug)]
pub(crate) struct StructuredOutputDocumentGuard {
    format: Option<StructuredOutputFormat>,
    first: Option<(DocumentGrain, Arc<str>)>,
}

impl StructuredOutputDocumentGuard {
    pub(crate) fn new(format: &OutputFormat) -> Self {
        Self {
            format: structured_output_format(format),
            first: None,
        }
    }

    pub(crate) fn observe(
        &mut self,
        output_name: &str,
        doc_ctx: &Arc<DocumentContext>,
    ) -> Result<(), PipelineError> {
        let Some(format) = self.format else {
            return Ok(());
        };
        if !crate::executor::document_dlq::is_concrete_file(doc_ctx.source_file()) {
            return Ok(());
        }

        let grain = doc_ctx.grain();
        match &self.first {
            Some((first_grain, _)) if *first_grain == grain => Ok(()),
            Some((first_grain, first_file)) => Err(format.error(format!(
                "output '{output_name}': {} structured output cannot write a multi-document body into one envelope; first document grain {:?} from {:?}, then grain {:?} from {:?}. consolidate intentionally with an envelope node or route each document to a separate output path",
                format.name(),
                first_grain,
                first_file,
                grain,
                doc_ctx.source_file(),
            ))),
            None => {
                self.first = Some((grain, Arc::clone(doc_ctx.source_file())));
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord};

    fn doc(file: &str) -> Arc<DocumentContext> {
        Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from(file),
            EnvelopeRecord::empty(),
        ))
    }

    #[test]
    fn structured_formats_are_guarded() {
        assert!(structured_output_format(&OutputFormat::Edifact(None)).is_some());
        assert!(structured_output_format(&OutputFormat::X12(None)).is_some());
        assert!(structured_output_format(&OutputFormat::Hl7(None)).is_some());
        assert!(structured_output_format(&OutputFormat::Swift(None)).is_some());
    }

    #[test]
    fn rejects_second_concrete_document_grain() {
        let mut guard = StructuredOutputDocumentGuard::new(&OutputFormat::Swift(None));
        let first = doc("a.swift");
        let second = doc("b.swift");

        guard
            .observe("out", &first)
            .expect("first document allowed");
        let err = guard
            .observe("out", &second)
            .expect_err("second concrete document is rejected");
        let message = format!("{err}");
        assert!(message.contains("SWIFT error"), "{message}");
        assert!(message.contains("multi-document body"), "{message}");
        assert!(message.contains("out"), "{message}");
    }

    #[test]
    fn non_structured_outputs_do_not_guard_document_grains() {
        let mut guard = StructuredOutputDocumentGuard::new(&OutputFormat::Csv(None));
        guard.observe("out", &doc("a.csv")).expect("first document");
        guard
            .observe("out", &doc("b.csv"))
            .expect("csv can write record sequences from multiple files");
    }
}
