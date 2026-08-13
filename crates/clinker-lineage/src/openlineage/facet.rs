//! OpenLineage `ColumnLineageDatasetFacet`: per-column field lineage.

use std::collections::BTreeMap;

use clinker_core_types::FailureClassification;
use serde::{Deserialize, Serialize};

use crate::logical_identity::{DatasetSubset, DatasetSubsetDirection, SymlinkIdentifier};

/// Standard OpenLineage location condition used by the subset facet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocationSubsetCondition {
    #[serde(rename = "type")]
    condition_type: String,
    pub locations: Vec<String>,
}

impl LocationSubsetCondition {
    fn new(locations: Vec<String>) -> Self {
        Self {
            condition_type: "location".to_string(),
            locations,
        }
    }
}

/// Standard OpenLineage input/output facet naming one concrete logical member
/// of a stable collection dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetSubsetFacet {
    #[serde(rename = "_producer")]
    pub producer: String,
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    #[serde(rename = "inputCondition", skip_serializing_if = "Option::is_none")]
    pub input_condition: Option<LocationSubsetCondition>,
    #[serde(rename = "outputCondition", skip_serializing_if = "Option::is_none")]
    pub output_condition: Option<LocationSubsetCondition>,
}

impl DatasetSubsetFacet {
    /// Build one role-specific standard subset facet from authorized logical
    /// locations. Subsets for the other dataset role are not serialized.
    pub fn new(subsets: &[DatasetSubset], direction: DatasetSubsetDirection) -> Option<Self> {
        let locations: Vec<String> = subsets
            .iter()
            .filter(|subset| subset.direction() == direction)
            .map(|subset| subset.identifier().to_string())
            .collect();
        if locations.is_empty() {
            return None;
        }
        let schema_url = match direction {
            DatasetSubsetDirection::Input => super::INPUT_DATASET_SUBSET_FACET_SCHEMA_URL,
            DatasetSubsetDirection::Output => super::OUTPUT_DATASET_SUBSET_FACET_SCHEMA_URL,
        };
        let condition = LocationSubsetCondition::new(locations);
        Some(Self {
            producer: super::PRODUCER.to_string(),
            schema_url: schema_url.to_string(),
            input_condition: (direction == DatasetSubsetDirection::Input)
                .then_some(condition.clone()),
            output_condition: (direction == DatasetSubsetDirection::Output).then_some(condition),
        })
    }
}

/// Standard OpenLineage dataset facet listing explicitly authorized alternate
/// table or location identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SymlinksDatasetFacet {
    #[serde(rename = "_producer")]
    pub producer: String,
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    pub identifiers: Vec<SymlinkIdentifier>,
}

impl SymlinksDatasetFacet {
    pub fn new(identifiers: Vec<SymlinkIdentifier>) -> Self {
        Self {
            producer: super::PRODUCER.to_string(),
            schema_url: super::SYMLINKS_DATASET_FACET_SCHEMA_URL.to_string(),
            identifiers,
        }
    }
}

/// Column-level lineage for one dataset.
///
/// DIRECT (value-derivation) lineage is keyed per output column in
/// [`fields`](ColumnLineageDatasetFacet::fields); whole-dataset INDIRECT
/// (influence via filter / join / group / sort) lineage is collected once in
/// [`dataset`](ColumnLineageDatasetFacet::dataset) rather than duplicated across
/// every column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnLineageDatasetFacet {
    /// URI of the software that produced this facet.
    #[serde(rename = "_producer")]
    pub producer: String,
    /// Schema URL of the facet spec this conforms to.
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    /// Output column name -> the input fields its value derives from. A
    /// `BTreeMap` keeps the serialized key order deterministic.
    pub fields: BTreeMap<String, FieldLineage>,
    /// Input fields that influence the dataset as a whole (INDIRECT lineage).
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub dataset: Vec<InputField>,
}

/// The input fields one output column derives from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldLineage {
    #[serde(rename = "inputFields")]
    pub input_fields: Vec<InputField>,
}

/// A reference to one input dataset column plus how it reaches the output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputField {
    pub namespace: String,
    pub name: String,
    pub field: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub transformations: Vec<Transformation>,
}

/// How an input field reaches an output: its derivation/influence class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transformation {
    #[serde(rename = "type")]
    pub transformation_type: TransformationType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtype: Option<TransformationSubtype>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Set when the transformation obfuscates the value (e.g. a hash).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub masking: Option<bool>,
}

/// Whether an output value is derived from the input (`DIRECT`) or merely
/// influenced by it (`INDIRECT`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TransformationType {
    Direct,
    Indirect,
}

/// The specific derivation/influence kind under a [`TransformationType`].
///
/// The OpenLineage spec leaves `subtype` free-form; this crate is a producer and
/// emits only the documented vocabulary below. Parsing foreign lineage carrying
/// an out-of-set subtype is intentionally not supported here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TransformationSubtype {
    Identity,
    Transformation,
    Aggregation,
    Join,
    GroupBy,
    Filter,
    Sort,
    Window,
    Conditional,
}

/// A clinker-specific job facet carrying the pipeline's content fingerprint.
///
/// OpenLineage defines no standard facet for a pipeline-source hash, so this is a
/// producer-defined facet: its `_schemaURL` points at the clinker producer rather
/// than an `openlineage.io` schema. Carrying the hash here — instead of encoding
/// it in the job name — lets consumers correlate runs of the exact same pipeline
/// definition while keeping the job name stable across edits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipelineJobFacet {
    /// URI of the software that produced this facet.
    #[serde(rename = "_producer")]
    pub producer: String,
    /// Schema URL of the facet spec this conforms to (clinker-owned).
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    /// Lowercase hex of the pipeline config's content hash.
    #[serde(rename = "sourceHash")]
    pub source_hash: String,
}

impl PipelineJobFacet {
    /// A pipeline-hash facet stamped with the clinker [`PRODUCER`] and
    /// [`CLINKER_PIPELINE_FACET_SCHEMA_URL`], so the producer/schema-URL
    /// convention lives in this crate rather than at each call site.
    ///
    /// [`PRODUCER`]: super::PRODUCER
    /// [`CLINKER_PIPELINE_FACET_SCHEMA_URL`]: super::CLINKER_PIPELINE_FACET_SCHEMA_URL
    pub fn new(source_hash: String) -> Self {
        Self {
            producer: super::PRODUCER.to_string(),
            schema_url: super::CLINKER_PIPELINE_FACET_SCHEMA_URL.to_string(),
            source_hash,
        }
    }
}

/// Versioned effective semantic fingerprint carried by the lineage job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticPlanJobFacet {
    #[serde(rename = "_producer")]
    pub producer: String,
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    pub algorithm: String,
    #[serde(rename = "semanticSchemaVersion")]
    pub semantic_schema_version: u32,
    pub digest: String,
}

impl SemanticPlanJobFacet {
    pub fn new(algorithm: impl Into<String>, version: u32, digest: impl Into<String>) -> Self {
        Self {
            producer: super::PRODUCER.to_string(),
            schema_url: super::CLINKER_SEMANTIC_PLAN_FACET_SCHEMA_URL.to_string(),
            algorithm: algorithm.into(),
            semantic_schema_version: version,
            digest: digest.into(),
        }
    }
}

/// Caller-owned batch correlation carried unchanged on every run event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchRunFacet {
    #[serde(rename = "_producer")]
    pub producer: String,
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    #[serde(rename = "batchId")]
    pub batch_id: String,
}

impl BatchRunFacet {
    pub fn new(batch_id: impl Into<String>) -> Self {
        Self {
            producer: super::PRODUCER.to_string(),
            schema_url: super::CLINKER_BATCH_FACET_SCHEMA_URL.to_string(),
            batch_id: batch_id.into(),
        }
    }
}

/// The standard OpenLineage error-message run facet.
///
/// Attached to a `FAIL` [`RunEvent`](super::event::RunEvent)'s
/// [`Run`](super::event::Run) to carry the failure's message. Unlike the
/// clinker-defined facets, this is an OpenLineage standard facet; its
/// `_schemaURL` points at `openlineage.io`, pinned to `ErrorMessageRunFacet`
/// `1-0-0`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorMessageRunFacet {
    /// URI of the software that produced this facet.
    #[serde(rename = "_producer")]
    pub producer: String,
    /// Schema URL of the facet spec this conforms to.
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    /// Human-readable failure message.
    pub message: String,
    /// The producer's implementation language. The spec keeps this free-form;
    /// clinker stamps `rust`.
    #[serde(rename = "programmingLanguage")]
    pub programming_language: String,
    /// Optional stack trace or extended diagnostic detail. Clinker's runtime
    /// errors carry no captured backtrace, so this is normally omitted.
    #[serde(rename = "stackTrace", skip_serializing_if = "Option::is_none")]
    pub stack_trace: Option<String>,
}

impl ErrorMessageRunFacet {
    /// An error facet stamped with the clinker [`PRODUCER`], the standard
    /// [`ERROR_MESSAGE_FACET_SCHEMA_URL`], and `programmingLanguage = "rust"`.
    ///
    /// [`PRODUCER`]: super::PRODUCER
    /// [`ERROR_MESSAGE_FACET_SCHEMA_URL`]: super::ERROR_MESSAGE_FACET_SCHEMA_URL
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            producer: super::PRODUCER.to_string(),
            schema_url: super::ERROR_MESSAGE_FACET_SCHEMA_URL.to_string(),
            message: message.into(),
            programming_language: "rust".to_string(),
            stack_trace: None,
        }
    }
}

/// Clinker-owned sanitized failure classification carried by a failed run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClinkerFailureRunFacet {
    /// URI of the software that produced this facet.
    #[serde(rename = "_producer")]
    pub producer: String,
    /// Immutable schema URL for the v1 facet contract.
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    /// Stable namespaced failure code.
    pub code: String,
    /// Stable broad failure category.
    pub category: String,
    /// Stable retry advice.
    #[serde(rename = "retryAdvice")]
    pub retry_advice: String,
    /// Bounded, sanitized human-readable message.
    pub message: String,
}

impl ClinkerFailureRunFacet {
    /// API classification: supported integration API.
    ///
    /// Convert an approved shared failure classification into the lineage wire
    /// contract. Only stable accessors cross the boundary; the core type is not
    /// serialized or re-exported.
    pub fn from_classification(classification: &FailureClassification) -> Self {
        Self {
            producer: super::PRODUCER.to_string(),
            schema_url: super::CLINKER_FAILURE_FACET_SCHEMA_URL.to_string(),
            code: classification.code().to_string(),
            category: classification.category().as_str().to_string(),
            retry_advice: classification.retry_advice().as_str().to_string(),
            message: classification.message().to_string(),
        }
    }
}

/// A clinker-specific run facet carrying whole-run record counts and timing.
///
/// OpenLineage defines no standard run-level record-count facet, so — like
/// [`PipelineJobFacet`] — this is a producer-defined facet whose `_schemaURL`
/// points at the clinker producer. The counts are pipeline-wide run totals, not
/// per-output: the executor does not surface per-output record attribution, so a
/// single run-level count is the honest granularity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunStatsFacet {
    /// URI of the software that produced this facet.
    #[serde(rename = "_producer")]
    pub producer: String,
    /// Schema URL of the facet spec this conforms to (clinker-owned).
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    /// Total records read across all sources.
    #[serde(rename = "recordsRead")]
    pub records_read: u64,
    /// Total records written across all sinks. Exceeds `recordsRead` under
    /// inclusive route fan-out or multi-output sinks.
    #[serde(rename = "recordsWritten")]
    pub records_written: u64,
    /// Records routed to the dead-letter queue.
    #[serde(rename = "recordsDlq")]
    pub records_dlq: u64,
    /// Wall-clock run duration in milliseconds (`finished_at - started_at`).
    #[serde(rename = "durationMs")]
    pub duration_ms: i64,
}

impl RunStatsFacet {
    /// A run-stats facet stamped with the clinker [`PRODUCER`] and
    /// [`CLINKER_RUN_STATS_FACET_SCHEMA_URL`].
    ///
    /// [`PRODUCER`]: super::PRODUCER
    /// [`CLINKER_RUN_STATS_FACET_SCHEMA_URL`]: super::CLINKER_RUN_STATS_FACET_SCHEMA_URL
    pub fn new(
        records_read: u64,
        records_written: u64,
        records_dlq: u64,
        duration_ms: i64,
    ) -> Self {
        Self {
            producer: super::PRODUCER.to_string(),
            schema_url: super::CLINKER_RUN_STATS_FACET_SCHEMA_URL.to_string(),
            records_read,
            records_written,
            records_dlq,
            duration_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subtype_casing_matches_spec() {
        let cases = [
            (TransformationSubtype::Identity, "IDENTITY"),
            (TransformationSubtype::Transformation, "TRANSFORMATION"),
            (TransformationSubtype::Aggregation, "AGGREGATION"),
            (TransformationSubtype::Join, "JOIN"),
            (TransformationSubtype::GroupBy, "GROUP_BY"),
            (TransformationSubtype::Filter, "FILTER"),
            (TransformationSubtype::Sort, "SORT"),
            (TransformationSubtype::Window, "WINDOW"),
            (TransformationSubtype::Conditional, "CONDITIONAL"),
        ];
        for (variant, wire) in cases {
            assert_eq!(serde_json::to_value(variant).unwrap(), wire);
        }
        assert_eq!(
            serde_json::to_value(TransformationType::Direct).unwrap(),
            "DIRECT"
        );
        assert_eq!(
            serde_json::to_value(TransformationType::Indirect).unwrap(),
            "INDIRECT"
        );
    }

    #[test]
    fn shared_failure_adapts_only_stable_sanitized_accessors() {
        let classification = FailureClassification::new(
            "observability.delivery.failed",
            "temporary collector failure",
        )
        .expect("registered failure code");

        let facet = ClinkerFailureRunFacet::from_classification(&classification);
        assert_eq!(facet.code, "observability.delivery.failed");
        assert_eq!(facet.category, "observability");
        assert_eq!(facet.retry_advice, "retry_with_backoff");
        assert_eq!(facet.message, "temporary collector failure");
        assert_eq!(
            facet.schema_url,
            super::super::CLINKER_FAILURE_FACET_SCHEMA_URL
        );
    }
}
