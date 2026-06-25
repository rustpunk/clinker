//! The OpenLineage wire data model and NDJSON serializer.
//!
//! Pinned to OpenLineage core spec `2-0-2` and `ColumnLineageDatasetFacet`
//! `1-2-0`; the pinned versions are carried in the schema-URL constants below.

mod event;
mod facet;
mod ndjson;

pub use event::{Dataset, DatasetFacets, EventType, Job, Run, RunEvent};
pub use facet::{
    ColumnLineageDatasetFacet, FieldLineage, InputField, Transformation, TransformationSubtype,
    TransformationType,
};
pub use ndjson::write_ndjson;

/// Schema URL for an OpenLineage core run event (the event-level `schemaURL`).
pub const OPENLINEAGE_SCHEMA_URL: &str = "https://openlineage.io/spec/2-0-2/OpenLineage.json";

/// Schema URL for the column-lineage dataset facet (the facet-level `_schemaURL`).
pub const COLUMN_LINEAGE_FACET_SCHEMA_URL: &str =
    "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json";

/// Producer URI stamped on emitted events and facets, identifying this emitter.
pub const PRODUCER: &str = "https://github.com/rustpunk/clinker";
