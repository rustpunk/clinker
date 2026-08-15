//! Bounded advisory coverage reports.
//!
//! These types describe how far `clinker-schema` inspected an authoring
//! artifact. They never represent execution admission; `clinker-plan` remains
//! the sole authority that can produce an executable plan.

use std::path::PathBuf;

/// Maximum field descriptors retained or traversed by one advisory report.
pub const MAX_ADVISORY_FIELDS: usize = 10_000;
/// Maximum nested field depth traversed by advisory analysis.
pub const MAX_ADVISORY_FIELD_DEPTH: usize = 64;
/// Maximum schema references retained by one advisory report.
pub const MAX_ADVISORY_REFERENCES: usize = 4_096;
/// Maximum reasons retained by one advisory report.
pub const MAX_ADVISORY_REASONS: usize = 1_024;

/// What kind of artifact an advisory report describes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReportSubject {
    /// A `.schema.yaml` document.
    Schema,
    /// A pipeline document that may reference schemas.
    Pipeline,
}

/// Explicit reach of an advisory analysis.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoverageStatus {
    /// Every facet represented by the advisory model was inspected.
    Analyzed,
    /// Some facets were inspected and the unsupported remainder is listed.
    Partial,
    /// The artifact had no applicable advisory content.
    Skipped,
    /// The artifact could not be inspected safely or structurally.
    Failed,
}

impl CoverageStatus {
    /// Return the stable report spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Analyzed => "analyzed",
            Self::Partial => "partial",
            Self::Skipped => "skipped",
            Self::Failed => "failed",
        }
    }
}

/// One facet the advisory model can describe or explicitly decline.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CoverageFacet {
    Metadata,
    FieldNames,
    FieldTypes,
    Nullability,
    EnumConstraints,
    NestedFields,
    ArrayElementTypes,
    FormatCompatibility,
    PipelineReferences,
    TransformFieldScan,
    /// Planner-owned external schema syntax not represented by the advisory
    /// metadata document model.
    PlannerSchemaShape,
}

impl CoverageFacet {
    /// Return the stable report spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::FieldNames => "field-names",
            Self::FieldTypes => "field-types",
            Self::Nullability => "nullability",
            Self::EnumConstraints => "enum-constraints",
            Self::NestedFields => "nested-fields",
            Self::ArrayElementTypes => "array-element-types",
            Self::FormatCompatibility => "format-compatibility",
            Self::PipelineReferences => "pipeline-references",
            Self::TransformFieldScan => "transform-field-scan",
            Self::PlannerSchemaShape => "planner-schema-shape",
        }
    }
}

/// File location attached to advisory evidence.
///
/// Line and column remain absent when the underlying typed deserializer does
/// not expose a reliable leaf span. The path is always retained.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReportLocation {
    pub path: PathBuf,
    pub line: Option<usize>,
    pub column: Option<usize>,
}

impl ReportLocation {
    pub fn file(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            line: None,
            column: None,
        }
    }
}

/// One schema reference found through structured pipeline parsing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaReference {
    pub schema_path: PathBuf,
    pub location: ReportLocation,
}

/// One bounded reason for a non-analyzed result or unsupported facet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoverageReason {
    pub code: &'static str,
    pub facet: Option<CoverageFacet>,
    pub location: ReportLocation,
    pub message: String,
}

/// Deterministic advisory report for one schema or pipeline artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaCoverageReport {
    pub subject: ReportSubject,
    pub location: ReportLocation,
    pub status: CoverageStatus,
    pub supported_facets: Vec<CoverageFacet>,
    pub unsupported_facets: Vec<CoverageFacet>,
    pub references: Vec<SchemaReference>,
    pub reasons: Vec<CoverageReason>,
}

impl SchemaCoverageReport {
    pub fn new(subject: ReportSubject, path: impl Into<PathBuf>) -> Self {
        Self {
            subject,
            location: ReportLocation::file(path),
            status: CoverageStatus::Analyzed,
            supported_facets: Vec::new(),
            unsupported_facets: Vec::new(),
            references: Vec::new(),
            reasons: Vec::new(),
        }
    }

    pub(crate) fn support(&mut self, facet: CoverageFacet) {
        if !self.supported_facets.contains(&facet) {
            self.supported_facets.push(facet);
        }
    }

    pub(crate) fn decline(
        &mut self,
        facet: CoverageFacet,
        code: &'static str,
        message: impl Into<String>,
    ) {
        if !self.unsupported_facets.contains(&facet) {
            self.unsupported_facets.push(facet);
        }
        if self.status == CoverageStatus::Analyzed {
            self.status = CoverageStatus::Partial;
        }
        self.reason(code, Some(facet), message);
    }

    pub(crate) fn reason(
        &mut self,
        code: &'static str,
        facet: Option<CoverageFacet>,
        message: impl Into<String>,
    ) {
        if self.reasons.len() < MAX_ADVISORY_REASONS {
            self.reasons.push(CoverageReason {
                code,
                facet,
                location: self.location.clone(),
                message: message.into(),
            });
        }
    }

    pub(crate) fn reference(&mut self, reference: SchemaReference) {
        if self.references.len() < MAX_ADVISORY_REFERENCES {
            self.references.push(reference);
        } else {
            self.status = CoverageStatus::Partial;
            self.reason(
                "reference-limit",
                Some(CoverageFacet::PipelineReferences),
                format!(
                    "schema reference count exceeds the advisory limit of {MAX_ADVISORY_REFERENCES}"
                ),
            );
        }
    }

    pub(crate) fn sort_stably(&mut self) {
        self.supported_facets.sort_by_key(|facet| facet.as_str());
        self.supported_facets.dedup();
        self.unsupported_facets.sort_by_key(|facet| facet.as_str());
        self.unsupported_facets.dedup();
        self.references.sort_by(|left, right| {
            left.schema_path
                .cmp(&right.schema_path)
                .then_with(|| left.location.path.cmp(&right.location.path))
        });
        self.references.dedup();
        self.reasons.sort_by(|left, right| {
            left.code
                .cmp(right.code)
                .then_with(|| left.message.cmp(&right.message))
        });
        self.reasons.dedup();
    }
}
