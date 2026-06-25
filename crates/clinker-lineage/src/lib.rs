//! OpenLineage lineage emission for Clinker.
//!
//! This crate serializes Clinker pipeline lineage as [OpenLineage] events — the
//! vendor-neutral open standard for dataset and column-level lineage. This tier
//! owns only the wire data model (the [`openlineage`] module) and an NDJSON
//! writer; the builder that walks a compiled plan to populate these structures is
//! layered on in sibling modules.
//!
//! The model is pinned to OpenLineage core spec `2-0-2` and the
//! `ColumnLineageDatasetFacet` `1-2-0`. No general-purpose Rust OpenLineage client
//! exists, so the structs are hand-rolled against the published JSON Schema.
//!
//! [OpenLineage]: https://openlineage.io

pub mod openlineage;

pub use openlineage::{
    COLUMN_LINEAGE_FACET_SCHEMA_URL, ColumnLineageDatasetFacet, Dataset, DatasetFacets, EventType,
    FieldLineage, InputField, Job, OPENLINEAGE_SCHEMA_URL, PRODUCER, Run, RunEvent, Transformation,
    TransformationSubtype, TransformationType, write_ndjson,
};
