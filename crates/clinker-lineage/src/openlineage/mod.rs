//! The OpenLineage wire data model and NDJSON serializer.
//!
//! Pinned to OpenLineage core spec `2-0-2` and `ColumnLineageDatasetFacet`
//! `1-2-0`; the pinned versions are carried in the schema-URL constants below.

mod event;
mod facet;
mod ndjson;

pub use event::{Dataset, DatasetFacets, EventType, Job, JobFacets, Run, RunEvent, RunFacets};
pub use facet::{
    ColumnLineageDatasetFacet, ErrorMessageRunFacet, FieldLineage, InputField, PipelineJobFacet,
    RunStatsFacet, Transformation, TransformationSubtype, TransformationType,
};
pub use ndjson::write_ndjson;

/// Schema URL for an OpenLineage core run event (the event-level `schemaURL`).
pub const OPENLINEAGE_SCHEMA_URL: &str = "https://openlineage.io/spec/2-0-2/OpenLineage.json";

/// Schema URL for the column-lineage dataset facet (the facet-level `_schemaURL`).
pub const COLUMN_LINEAGE_FACET_SCHEMA_URL: &str =
    "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json";

/// Producer URI stamped on emitted events and facets, identifying this emitter.
pub const PRODUCER: &str = "https://github.com/rustpunk/clinker";

/// OpenLineage namespace for clinker pipeline jobs (the `job.namespace`).
pub const JOB_NAMESPACE: &str = "clinker";

/// Schema URL for the clinker-specific pipeline job facet (the facet-level
/// `_schemaURL`). OpenLineage has no standard pipeline-hash facet, so this points
/// at the clinker producer rather than `openlineage.io`.
pub const CLINKER_PIPELINE_FACET_SCHEMA_URL: &str =
    "https://github.com/rustpunk/clinker/spec/facets/PipelineJobFacet.json";

/// Schema URL for the standard OpenLineage error-message run facet (the
/// facet-level `_schemaURL`). Pinned to `ErrorMessageRunFacet` `1-0-0`; carried on
/// a `FAIL` run event to convey the failure message.
pub const ERROR_MESSAGE_FACET_SCHEMA_URL: &str =
    "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json";

/// Schema URL for the clinker-specific run-statistics run facet (the facet-level
/// `_schemaURL`). OpenLineage has no standard run-level record-count facet, so —
/// like [`CLINKER_PIPELINE_FACET_SCHEMA_URL`] — this points at the clinker
/// producer rather than `openlineage.io`.
pub const CLINKER_RUN_STATS_FACET_SCHEMA_URL: &str =
    "https://github.com/rustpunk/clinker/spec/facets/RunStatsFacet.json";
