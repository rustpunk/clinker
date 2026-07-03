//! OpenLineage lineage emission for Clinker.
//!
//! This crate serializes Clinker pipeline lineage as [OpenLineage] events — the
//! vendor-neutral open standard for dataset and column-level lineage. The
//! [`openlineage`] module owns the wire data model and an NDJSON writer; the
//! [`dataset`] module maps each Source/Output node to its OpenLineage dataset
//! identity; and the [`builder`] module walks a compiled plan to compute
//! column-level lineage from those identities.
//!
//! [`builder::column_lineage`] populates both DIRECT (value-derivation) per-column
//! lineage and whole-dataset INDIRECT influence (filter / join / group-by / sort /
//! conditional), tracing through composition bodies to true source columns.
//! Envelope (`$doc`) reads are traced too — as DIRECT lineage on the originating
//! source for value-carrying reads, and as INDIRECT influence for reads in a
//! route / cull / combine predicate; see that module's documented limitations for
//! the cases still out of scope. The [`emit`] module assembles a built lineage into
//! run events ready for [`openlineage::write_ndjson`]: a static `START`/`COMPLETE`
//! pair for the plan-derived `--lineage` export ([`emit::run_events`]), or live
//! run-lifecycle events tied to an actual execution — a `START` at run begin and a
//! terminal `COMPLETE` / `FAIL` / `ABORT` carrying real run stats, driven by
//! [`emit::LiveRunEmitter`].
//!
//! The model is pinned to OpenLineage core spec `2-0-2` and the
//! `ColumnLineageDatasetFacet` `1-2-0`. No general-purpose Rust OpenLineage client
//! exists, so the structs are hand-rolled against the published JSON Schema.
//!
//! [OpenLineage]: https://openlineage.io

pub mod builder;
pub mod dataset;
pub mod emit;
pub mod openlineage;

pub use builder::{OutputColumnLineage, PlanColumnLineage, column_lineage};
pub use dataset::{DatasetId, FALLBACK_NAMESPACE, FILE_NAMESPACE, dataset_identity};
pub use emit::{LiveRunEmitter, RunStats, Terminal, run_events, start_event, terminal_event};
pub use openlineage::{
    CLINKER_PIPELINE_FACET_SCHEMA_URL, CLINKER_RUN_STATS_FACET_SCHEMA_URL,
    COLUMN_LINEAGE_FACET_SCHEMA_URL, ColumnLineageDatasetFacet, Dataset, DatasetFacets,
    ERROR_MESSAGE_FACET_SCHEMA_URL, ErrorMessageRunFacet, EventType, FieldLineage, InputField,
    JOB_NAMESPACE, Job, JobFacets, OPENLINEAGE_SCHEMA_URL, PRODUCER, PipelineJobFacet, Run,
    RunEvent, RunFacets, RunStatsFacet, Transformation, TransformationSubtype, TransformationType,
    write_ndjson,
};
