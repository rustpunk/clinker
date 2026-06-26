//! OpenLineage run-event model: the envelope that carries dataset lineage.

use serde::{Deserialize, Serialize};

use super::facet::{ColumnLineageDatasetFacet, PipelineJobFacet};

/// A single OpenLineage run-state event.
///
/// A finite batch run is conventionally described by a `START` then a `COMPLETE`
/// event sharing one [`Run::run_id`]; dataset lineage (including the column-lineage
/// facet) is attached to the [`outputs`](RunEvent::outputs) of the `COMPLETE` event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEvent {
    /// Event production time, RFC-3339 in UTC (e.g. `2020-02-22T22:42:42Z`).
    #[serde(rename = "eventTime")]
    pub event_time: String,
    /// URI identifying the software that produced this event.
    pub producer: String,
    /// Schema URL of the core event spec this event conforms to.
    #[serde(rename = "schemaURL")]
    pub schema_url: String,
    #[serde(rename = "eventType")]
    pub event_type: EventType,
    pub run: Run,
    pub job: Job,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub inputs: Vec<Dataset>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub outputs: Vec<Dataset>,
}

/// Run lifecycle state. A batch run issues `START` then one terminal state
/// (`COMPLETE`, `ABORT`, or `FAIL`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum EventType {
    Start,
    Running,
    Complete,
    Abort,
    Fail,
    Other,
}

/// Identity of one run of a [`Job`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Run {
    /// Run identity as a UUID string (the spec constrains this to `format: uuid`).
    #[serde(rename = "runId")]
    pub run_id: String,
}

/// The job (pipeline) a [`Run`] is an execution of.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<JobFacets>,
}

impl Job {
    /// A clinker pipeline job: the [`JOB_NAMESPACE`] namespace, the pipeline
    /// `name`, and a [`PipelineJobFacet`] carrying the pipeline's content hash.
    /// The hash rides in the facet, not the name, so the job name stays stable
    /// across edits while runs of the same definition remain correlatable.
    ///
    /// [`JOB_NAMESPACE`]: super::JOB_NAMESPACE
    pub fn for_pipeline(name: impl Into<String>, source_hash: String) -> Self {
        Job {
            namespace: super::JOB_NAMESPACE.to_string(),
            name: name.into(),
            facets: Some(JobFacets {
                clinker_pipeline: Some(PipelineJobFacet::new(source_hash)),
            }),
        }
    }
}

/// The facet bundle attached to a [`Job`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobFacets {
    /// Clinker pipeline-source fingerprint facet (producer-defined).
    #[serde(rename = "clinker_pipeline", skip_serializing_if = "Option::is_none")]
    pub clinker_pipeline: Option<PipelineJobFacet>,
}

/// An input or output dataset referenced by a run event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dataset {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<DatasetFacets>,
}

/// The facet bundle attached to a [`Dataset`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetFacets {
    #[serde(rename = "columnLineage", skip_serializing_if = "Option::is_none")]
    pub column_lineage: Option<ColumnLineageDatasetFacet>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openlineage::{
        COLUMN_LINEAGE_FACET_SCHEMA_URL, FieldLineage, InputField, OPENLINEAGE_SCHEMA_URL,
        PRODUCER, Transformation, TransformationSubtype, TransformationType,
    };
    use std::collections::BTreeMap;

    fn sample_event() -> RunEvent {
        let mut fields = BTreeMap::new();
        fields.insert(
            "total".to_string(),
            FieldLineage {
                input_fields: vec![InputField {
                    namespace: "file".to_string(),
                    name: "/data/orders.csv".to_string(),
                    field: "amount".to_string(),
                    transformations: vec![Transformation {
                        transformation_type: TransformationType::Direct,
                        subtype: Some(TransformationSubtype::Aggregation),
                        description: None,
                        masking: None,
                    }],
                }],
            },
        );
        let facet = ColumnLineageDatasetFacet {
            producer: PRODUCER.to_string(),
            schema_url: COLUMN_LINEAGE_FACET_SCHEMA_URL.to_string(),
            fields,
            dataset: vec![InputField {
                namespace: "file".to_string(),
                name: "/data/orders.csv".to_string(),
                field: "region".to_string(),
                transformations: vec![Transformation {
                    transformation_type: TransformationType::Indirect,
                    subtype: Some(TransformationSubtype::GroupBy),
                    description: None,
                    masking: None,
                }],
            }],
        };
        RunEvent {
            event_time: "2020-02-22T22:42:42Z".to_string(),
            producer: PRODUCER.to_string(),
            schema_url: OPENLINEAGE_SCHEMA_URL.to_string(),
            event_type: EventType::Complete,
            run: Run {
                run_id: "0190b7e0-0000-7000-8000-000000000000".to_string(),
            },
            job: Job {
                namespace: "clinker".to_string(),
                name: "orders".to_string(),
                facets: None,
            },
            inputs: vec![Dataset {
                namespace: "file".to_string(),
                name: "/data/orders.csv".to_string(),
                facets: None,
            }],
            outputs: vec![Dataset {
                namespace: "file".to_string(),
                name: "/out/summary.csv".to_string(),
                facets: Some(DatasetFacets {
                    column_lineage: Some(facet),
                }),
            }],
        }
    }

    #[test]
    fn serializes_to_openlineage_shape() {
        let v = serde_json::to_value(sample_event()).unwrap();
        // Required event keys.
        assert!(v.get("eventTime").is_some());
        assert!(v.get("producer").is_some());
        assert!(v.get("schemaURL").is_some());
        assert_eq!(v["eventType"], "COMPLETE");
        assert!(v["run"].get("runId").is_some());
        assert!(v["job"].get("namespace").is_some());
        assert!(v["job"].get("name").is_some());
        // The column-lineage facet carries the underscored base-facet fields.
        let facet = &v["outputs"][0]["facets"]["columnLineage"];
        assert!(facet.get("_producer").is_some());
        assert!(facet.get("_schemaURL").is_some());
        // DIRECT lineage keyed per output column.
        let direct = &facet["fields"]["total"]["inputFields"][0]["transformations"][0];
        assert_eq!(direct["type"], "DIRECT");
        assert_eq!(direct["subtype"], "AGGREGATION");
        // INDIRECT lineage in the whole-dataset array, not duplicated per column.
        let indirect = &facet["dataset"][0]["transformations"][0];
        assert_eq!(indirect["type"], "INDIRECT");
        assert_eq!(indirect["subtype"], "GROUP_BY");
    }

    #[test]
    fn round_trips() {
        let event = sample_event();
        let json = serde_json::to_string(&event).unwrap();
        let back: RunEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, back);
    }

    #[test]
    fn round_trips_with_empty_input_output_lists() {
        // Empty inputs/outputs are omitted on the wire (skip_serializing_if) and must
        // deserialize back to empty via `default`, not fail on a missing key.
        let mut event = sample_event();
        event.inputs = Vec::new();
        event.outputs = Vec::new();
        let json = serde_json::to_string(&event).unwrap();
        assert!(!json.contains("\"inputs\""));
        assert!(!json.contains("\"outputs\""));
        let back: RunEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, back);
    }

    #[test]
    fn omits_empty_optionals() {
        // A dataset with no facets omits the key rather than emitting null.
        let v = serde_json::to_value(sample_event()).unwrap();
        assert!(v["inputs"][0].get("facets").is_none());
        // A job with no facets omits the key too.
        assert!(v["job"].get("facets").is_none());
    }

    #[test]
    fn for_pipeline_stamps_namespace_and_hash_facet() {
        use crate::openlineage::{CLINKER_PIPELINE_FACET_SCHEMA_URL, JOB_NAMESPACE, PRODUCER};
        let job = Job::for_pipeline("orders", "deadbeef".to_string());
        assert_eq!(job.namespace, JOB_NAMESPACE);
        assert_eq!(job.name, "orders");
        let facet = job
            .facets
            .as_ref()
            .and_then(|f| f.clinker_pipeline.as_ref())
            .expect("clinker pipeline facet");
        assert_eq!(facet.source_hash, "deadbeef");
        assert_eq!(facet.producer, PRODUCER);
        assert_eq!(facet.schema_url, CLINKER_PIPELINE_FACET_SCHEMA_URL);
    }

    #[test]
    fn job_facet_serializes_under_clinker_pipeline_key() {
        use crate::openlineage::{CLINKER_PIPELINE_FACET_SCHEMA_URL, PipelineJobFacet};
        let mut event = sample_event();
        event.job.facets = Some(JobFacets {
            clinker_pipeline: Some(PipelineJobFacet {
                producer: PRODUCER.to_string(),
                schema_url: CLINKER_PIPELINE_FACET_SCHEMA_URL.to_string(),
                source_hash: "deadbeef".to_string(),
            }),
        });
        let v = serde_json::to_value(&event).unwrap();
        let facet = &v["job"]["facets"]["clinker_pipeline"];
        assert_eq!(facet["sourceHash"], "deadbeef");
        assert!(facet.get("_producer").is_some());
        assert!(facet.get("_schemaURL").is_some());
        // Round-trips through the wire form unchanged.
        let json = serde_json::to_string(&event).unwrap();
        let back: RunEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, back);
    }
}
