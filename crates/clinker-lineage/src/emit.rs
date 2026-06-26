//! Assemble a built column lineage into an OpenLineage run-event pair.
//!
//! [`column_lineage`](crate::column_lineage) computes dataset identities and the
//! per-output column-lineage facet but stays free of run-lifecycle concerns — it
//! has no run id and no clock. This module is the bridge to the wire model: it
//! pairs a `START` and a `COMPLETE` [`RunEvent`] under one `run_id`, attaching the
//! column-lineage facet to the `COMPLETE` event's output datasets, per the
//! finite-batch convention documented on [`RunEvent`].
//!
//! `run_id` and `event_time` are caller-supplied; this crate has no UUID or clock
//! dependency, so the CLI passes the run's `execution_id` and a UTC timestamp.

use crate::builder::PlanColumnLineage;
use crate::openlineage::{
    Dataset, DatasetFacets, EventType, Job, OPENLINEAGE_SCHEMA_URL, PRODUCER, Run, RunEvent,
};

/// Build the `[START, COMPLETE]` OpenLineage event pair describing one run's
/// column lineage.
///
/// Both events share `run_id`, `job`, `event_time`, and the [`PRODUCER`] /
/// [`OPENLINEAGE_SCHEMA_URL`] stamps. The `START` event announces the run with no
/// datasets; the `COMPLETE` event carries the input datasets (facet-less) and the
/// output datasets, each bearing its `columnLineage` facet. This is a static,
/// plan-derived export, so a single `event_time` is used for both events.
pub fn run_events(
    lineage: &PlanColumnLineage,
    run_id: &str,
    job: Job,
    event_time: &str,
) -> Vec<RunEvent> {
    let run = Run {
        run_id: run_id.to_string(),
    };
    let start = RunEvent {
        event_time: event_time.to_string(),
        producer: PRODUCER.to_string(),
        schema_url: OPENLINEAGE_SCHEMA_URL.to_string(),
        event_type: EventType::Start,
        run: run.clone(),
        job: job.clone(),
        inputs: Vec::new(),
        outputs: Vec::new(),
    };

    let inputs: Vec<Dataset> = lineage.inputs.iter().cloned().map(Dataset::from).collect();
    let outputs: Vec<Dataset> = lineage
        .outputs
        .iter()
        .map(|out| {
            let mut dataset = Dataset::from(out.dataset.clone());
            dataset.facets = Some(DatasetFacets {
                column_lineage: Some(out.facet.clone()),
            });
            dataset
        })
        .collect();

    let complete = RunEvent {
        event_time: event_time.to_string(),
        producer: PRODUCER.to_string(),
        schema_url: OPENLINEAGE_SCHEMA_URL.to_string(),
        event_type: EventType::Complete,
        run,
        job,
        inputs,
        outputs,
    };

    vec![start, complete]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::OutputColumnLineage;
    use crate::dataset::DatasetId;
    use crate::openlineage::{
        CLINKER_PIPELINE_FACET_SCHEMA_URL, COLUMN_LINEAGE_FACET_SCHEMA_URL,
        ColumnLineageDatasetFacet, JobFacets, PipelineJobFacet,
    };
    use std::collections::BTreeMap;

    fn sample_lineage() -> PlanColumnLineage {
        PlanColumnLineage {
            inputs: vec![DatasetId {
                namespace: "file".to_string(),
                name: "/w/data/in.csv".to_string(),
            }],
            outputs: vec![OutputColumnLineage {
                dataset: DatasetId {
                    namespace: "file".to_string(),
                    name: "/w/out/out.csv".to_string(),
                },
                facet: ColumnLineageDatasetFacet {
                    producer: PRODUCER.to_string(),
                    schema_url: COLUMN_LINEAGE_FACET_SCHEMA_URL.to_string(),
                    fields: BTreeMap::new(),
                    dataset: Vec::new(),
                },
            }],
        }
    }

    fn sample_job() -> Job {
        Job {
            namespace: "clinker".to_string(),
            name: "demo".to_string(),
            facets: Some(JobFacets {
                clinker_pipeline: Some(PipelineJobFacet {
                    producer: PRODUCER.to_string(),
                    schema_url: CLINKER_PIPELINE_FACET_SCHEMA_URL.to_string(),
                    source_hash: "abc123".to_string(),
                }),
            }),
        }
    }

    #[test]
    fn pairs_start_then_complete_sharing_run_id() {
        let events = run_events(
            &sample_lineage(),
            "run-1",
            sample_job(),
            "2020-02-22T22:42:42Z",
        );
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, EventType::Start);
        assert_eq!(events[1].event_type, EventType::Complete);
        assert_eq!(events[0].run.run_id, "run-1");
        assert_eq!(events[1].run.run_id, "run-1");
    }

    #[test]
    fn facet_attaches_to_complete_outputs_only() {
        let events = run_events(
            &sample_lineage(),
            "run-1",
            sample_job(),
            "2020-02-22T22:42:42Z",
        );
        let (start, complete) = (&events[0], &events[1]);
        // START announces the run with no datasets.
        assert!(start.inputs.is_empty());
        assert!(start.outputs.is_empty());
        // COMPLETE carries inputs (facet-less) and facet-bearing outputs.
        assert_eq!(complete.inputs.len(), 1);
        assert!(complete.inputs[0].facets.is_none());
        assert_eq!(complete.outputs.len(), 1);
        let facets = complete.outputs[0].facets.as_ref().expect("output facets");
        assert!(facets.column_lineage.is_some());
    }

    #[test]
    fn job_facet_carried_on_both_events() {
        let events = run_events(
            &sample_lineage(),
            "run-1",
            sample_job(),
            "2020-02-22T22:42:42Z",
        );
        for event in &events {
            let facet = event
                .job
                .facets
                .as_ref()
                .and_then(|f| f.clinker_pipeline.as_ref())
                .expect("clinker pipeline job facet");
            assert_eq!(facet.source_hash, "abc123");
        }
    }
}
