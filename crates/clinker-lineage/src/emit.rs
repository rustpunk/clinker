//! Assemble a built column lineage into OpenLineage run events.
//!
//! [`column_lineage`](crate::column_lineage) computes dataset identities and the
//! per-output column-lineage facet but stays free of run-lifecycle concerns — it
//! has no run id and no clock. This module is the bridge to the wire model, in two
//! shapes:
//!
//! - [`run_events`] pairs a `START` and terminal [`RunEvent`] for the static,
//!   plan-derived export.
//! - [`start_event`] / [`terminal_event`] describe an actual execution from one
//!   caller-owned immutable [`RunLifecycleFacts`] snapshot.
//!
//! Every identity, timestamp, terminal outcome, and count is caller-supplied;
//! this crate has no UUID, clock, or lifecycle-state dependency.

use clinker_core_types::FailureClassification;

use crate::builder::PlanColumnLineage;
use crate::logical_identity::{DatasetIdentityFacets, DatasetSubsetDirection};
use crate::openlineage::{
    BatchRunFacet, ClinkerFailureRunFacet, Dataset, DatasetFacets, DatasetSubsetFacet,
    ErrorMessageRunFacet, EventType, InputDatasetFacets, Job, OPENLINEAGE_SCHEMA_URL,
    OutputDatasetFacets, PRODUCER, Run, RunEvent, RunFacets, RunStatsFacet, SemanticPlanJobFacet,
    SymlinksDatasetFacet,
};

/// The input datasets of a run carrying only what is true regardless of how
/// the run ended — the shape used on a `START` and on a `FAIL` or `ABORT`
/// terminal.
///
/// The symmetric half of [`output_identities`], and the two state one rule: a
/// symlink names the dataset and rides on every event, while the subset facet
/// says which members this run touched and appears only where it finished
/// touching them.
fn input_identities(lineage: &PlanColumnLineage) -> Vec<Dataset> {
    lineage
        .inputs
        .iter()
        .cloned()
        .map(|identity| {
            let facts = lineage.input_identity_facets.get(&identity);
            let mut dataset = Dataset::from(identity);
            dataset.facets = facts.and_then(symlink_facets);
            dataset
        })
        .collect()
}

/// The alternate identities authorized for a dataset, as the dataset-level
/// facet bundle.
///
/// A symlink is true of the dataset wherever it appears, so it belongs to the
/// dataset itself; the subset facet is a claim about this run's read or write
/// and rides in the position bucket instead — see [`subset_facet`].
fn symlink_facets(identity: &DatasetIdentityFacets) -> Option<DatasetFacets> {
    if identity.symlinks().is_empty() {
        return None;
    }
    Some(DatasetFacets {
        symlinks: Some(SymlinksDatasetFacet::new(identity.symlinks().to_vec())),
        column_lineage: None,
    })
}

/// The concrete members this run consumed or produced, as a role-specific
/// subset facet.
///
/// Its schema type is `InputSubsetInputDatasetFacet` or
/// `OutputSubsetOutputDatasetFacet` — an `InputDatasetFacet` / `OutputDatasetFacet`
/// respectively — so a conformant consumer looks for it under `inputFacets` or
/// `outputFacets`, never under the dataset's own `facets`.
fn subset_facet(
    identity: &DatasetIdentityFacets,
    direction: DatasetSubsetDirection,
) -> Option<DatasetSubsetFacet> {
    DatasetSubsetFacet::new(identity.subsets(), direction)
}

/// The input datasets of a run that finished reading them, with the subset
/// facet naming the members it actually consumed.
///
/// Only a `COMPLETE` may say that. See [`input_identities`] for the shape the
/// other events use.
fn inputs_with_identity_facets(lineage: &PlanColumnLineage) -> Vec<Dataset> {
    lineage
        .inputs
        .iter()
        .cloned()
        .map(|identity| {
            let facts = lineage.input_identity_facets.get(&identity);
            let mut dataset = Dataset::from(identity);
            dataset.facets = facts.and_then(symlink_facets);
            dataset.input_facets = facts
                .and_then(|facts| subset_facet(facts, DatasetSubsetDirection::Input))
                .map(|subset| InputDatasetFacets {
                    subset: Some(subset),
                });
            dataset
        })
        .collect()
}

/// The output datasets of a run carrying only what is true regardless of how
/// the run ended — the shape used on a `START` and on a `FAIL` or `ABORT`
/// terminal, neither of which has a completed claim to make.
///
/// One rule, and it applies to inputs the same way (see [`input_identities`]):
/// a symlink is an alternate name for the dataset wherever it appears, so it
/// belongs on every event; the subset facet says which members *this run*
/// touched, so it belongs only where the run finished touching them. Attaching
/// it to a failed terminal recorded consumption of members a run that died
/// early never read; omitting the symlink left that failure unattributed to
/// the dataset an operator would go looking at.
fn output_identities(lineage: &PlanColumnLineage) -> Vec<Dataset> {
    lineage
        .outputs
        .iter()
        .map(|out| {
            let mut dataset = Dataset::from(out.dataset.clone());
            dataset.facets = symlink_facets(&out.identity_facets);
            dataset
        })
        .collect()
}

/// The output datasets of a run, each bearing its `columnLineage` facet — the
/// shape used on a `COMPLETE` event.
fn outputs_with_lineage(lineage: &PlanColumnLineage) -> Vec<Dataset> {
    lineage
        .outputs
        .iter()
        .map(|out| {
            let mut dataset = Dataset::from(out.dataset.clone());
            let mut facets = symlink_facets(&out.identity_facets).unwrap_or_default();
            facets.column_lineage = Some(out.facet.clone());
            dataset.facets = Some(facets);
            dataset.output_facets =
                subset_facet(&out.identity_facets, DatasetSubsetDirection::Output).map(|subset| {
                    OutputDatasetFacets {
                        subset: Some(subset),
                    }
                });
            dataset
        })
        .collect()
}

/// Build the `[START, COMPLETE]` OpenLineage event pair describing one run's
/// column lineage.
///
/// Both events share `run_id`, `job`, `event_time`, and the [`PRODUCER`] /
/// [`OPENLINEAGE_SCHEMA_URL`] stamps. The `START` event announces the run with no
/// datasets; the `COMPLETE` event carries the input datasets with their
/// identity facets and the output datasets, each bearing its `columnLineage`
/// facet. This is a static, plan-derived export, so a single `event_time` is
/// used for both events.
pub fn run_events(
    lineage: &PlanColumnLineage,
    job: Job,
    lifecycle: &RunLifecycleFacts,
) -> Vec<RunEvent> {
    let mut start = start_event(lineage, job.clone(), &lifecycle.start);
    start.inputs.clear();
    start.outputs.clear();
    vec![start, terminal_event(lineage, job, lifecycle)]
}

/// Whole-run statistics captured at the one authoritative terminal boundary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunStats {
    /// Total records read across all sources.
    pub records_read: u64,
    /// Total records written across all sinks.
    pub records_written: u64,
    /// Records routed to the dead-letter queue.
    pub records_dlq: u64,
    /// Wall-clock run duration in milliseconds.
    pub duration_ms: i64,
}

/// The terminal state of a live run: how the `START` is closed out.
/// API classification: supported integration API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Terminal {
    /// The run finished cleanly — the terminal event carries the column-lineage
    /// facets on its outputs.
    Complete,
    /// The run was cancelled (e.g. a shutdown signal) after a partial drain.
    Abort,
    /// The run errored with one bounded, sanitized shared classification.
    Fail { failure: FailureClassification },
}

/// Immutable correlation and logical-start facts supplied by the CLI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLifecycleStartFacts {
    pub batch_id: String,
    pub execution_id: String,
    pub plan_fingerprint_algorithm: String,
    pub plan_fingerprint_version: u32,
    pub plan_fingerprint_digest: String,
    pub event_time: String,
}

/// The single terminal fact snapshot supplied by the CLI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLifecycleTerminalFacts {
    pub event_time: String,
    pub outcome: Terminal,
    /// Counts an execution observed, or `None` when nothing executed.
    ///
    /// Zeros are not a stand-in for "did not run": to a catalogue they assert
    /// that this pipeline executed and processed nothing, which is
    /// indistinguishable from a real empty run and is exactly what freshness
    /// and volume alerts key on.
    pub stats: Option<RunStats>,
}

/// Read-only owned lifecycle input for lineage event assembly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLifecycleFacts {
    pub start: RunLifecycleStartFacts,
    pub terminal: RunLifecycleTerminalFacts,
}

fn correlated_job(mut job: Job, start: &RunLifecycleStartFacts) -> Job {
    job.facets.get_or_insert_default().clinker_semantic_plan = Some(SemanticPlanJobFacet::new(
        start.plan_fingerprint_algorithm.clone(),
        start.plan_fingerprint_version,
        start.plan_fingerprint_digest.clone(),
    ));
    job
}

/// Build the live `START` event for an actual run.
///
/// Unlike the static [`run_events`] `START` (which is dataset-free), a live
/// `START` announces the run's input and output datasets by identity. Its only
/// run facet is immutable lifecycle correlation; completed dataset facets and
/// run stats arrive on the [`terminal_event`].
pub fn start_event(
    lineage: &PlanColumnLineage,
    job: Job,
    lifecycle: &RunLifecycleStartFacts,
) -> RunEvent {
    RunEvent {
        event_time: lifecycle.event_time.clone(),
        producer: PRODUCER.to_string(),
        schema_url: OPENLINEAGE_SCHEMA_URL.to_string(),
        event_type: EventType::Start,
        run: Run {
            run_id: lifecycle.execution_id.clone(),
            facets: Some(RunFacets {
                clinker_batch: Some(BatchRunFacet::new(lifecycle.batch_id.clone())),
                ..RunFacets::default()
            }),
        },
        job: correlated_job(job, lifecycle),
        inputs: input_identities(lineage),
        outputs: output_identities(lineage),
    }
}

/// Build the live terminal event (`COMPLETE` / `FAIL` / `ABORT`) closing out a
/// run started by [`start_event`].
///
/// The `run` carries a `clinker_runStats` facet built from `stats`, plus — on a
/// [`Terminal::Fail`] — the standard error-message facet. A `COMPLETE` event's
/// outputs bear their `columnLineage` facets; a `FAIL` or `ABORT` carries the
/// output datasets by identity only, because the run did not fully produce them.
pub fn terminal_event(
    lineage: &PlanColumnLineage,
    job: Job,
    lifecycle: &RunLifecycleFacts,
) -> RunEvent {
    let (event_type, failure) = match &lifecycle.terminal.outcome {
        Terminal::Complete => (EventType::Complete, None),
        Terminal::Abort => (EventType::Abort, None),
        Terminal::Fail { failure } => (EventType::Fail, Some(failure)),
    };
    let stats = lifecycle.terminal.stats;
    let run = Run {
        run_id: lifecycle.start.execution_id.clone(),
        facets: Some(RunFacets {
            clinker_batch: Some(BatchRunFacet::new(lifecycle.start.batch_id.clone())),
            run_stats: stats.map(|stats| {
                RunStatsFacet::new(
                    stats.records_read,
                    stats.records_written,
                    stats.records_dlq,
                    stats.duration_ms,
                )
            }),
            error_message: failure.map(|failure| ErrorMessageRunFacet::new(failure.message())),
            clinker_failure: failure.map(ClinkerFailureRunFacet::from_classification),
        }),
    };
    let outputs = if matches!(event_type, EventType::Complete) {
        outputs_with_lineage(lineage)
    } else {
        output_identities(lineage)
    };
    RunEvent {
        event_time: lifecycle.terminal.event_time.clone(),
        producer: PRODUCER.to_string(),
        schema_url: OPENLINEAGE_SCHEMA_URL.to_string(),
        event_type,
        run,
        job: correlated_job(job, &lifecycle.start),
        inputs: if matches!(event_type, EventType::Complete) {
            inputs_with_identity_facets(lineage)
        } else {
            input_identities(lineage)
        },
        outputs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::OutputColumnLineage;
    use crate::dataset::DatasetId;
    use crate::logical_identity::LineageIdentityContext;
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
            input_identity_facets: BTreeMap::new(),
            outputs: vec![OutputColumnLineage {
                dataset: DatasetId {
                    namespace: "file".to_string(),
                    name: "/w/out/out.csv".to_string(),
                },
                identity_facets: Default::default(),
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
                ..JobFacets::default()
            }),
        }
    }

    fn stats() -> RunStats {
        RunStats {
            records_read: 100,
            records_written: 97,
            records_dlq: 3,
            duration_ms: 1234,
        }
    }

    fn start_facts() -> RunLifecycleStartFacts {
        RunLifecycleStartFacts {
            batch_id: "batch-1".to_string(),
            execution_id: "0190b7e0-0000-7000-8000-000000000000".to_string(),
            plan_fingerprint_algorithm: "blake3".to_string(),
            plan_fingerprint_version: 1,
            plan_fingerprint_digest: "ab".repeat(32),
            event_time: "2020-02-22T22:42:42Z".to_string(),
        }
    }

    fn lifecycle(outcome: Terminal) -> RunLifecycleFacts {
        RunLifecycleFacts {
            start: start_facts(),
            terminal: RunLifecycleTerminalFacts {
                event_time: "2020-02-22T22:43:00Z".to_string(),
                outcome,
                stats: Some(stats()),
            },
        }
    }

    /// A run that executed nothing has no counts to report, and zeros are not
    /// a stand-in for that: a catalogue cannot tell `recordsRead = 0` from a
    /// real run that read nothing, and freshness and volume alerts key on
    /// exactly that number.
    #[test]
    fn a_terminal_without_observed_counts_carries_no_run_stats_facet() {
        let mut facts = lifecycle(Terminal::Complete);
        facts.terminal.stats = None;
        let events = run_events(&sample_lineage(), sample_job(), &facts);
        let terminal = events.last().expect("terminal event");
        assert_eq!(terminal.event_type, EventType::Complete);
        assert!(
            terminal
                .run
                .facets
                .as_ref()
                .expect("run facets")
                .run_stats
                .is_none(),
            "a run that did not execute must not report record counts"
        );

        let executed = run_events(
            &sample_lineage(),
            sample_job(),
            &lifecycle(Terminal::Complete),
        );
        assert!(
            executed
                .last()
                .expect("terminal event")
                .run
                .facets
                .as_ref()
                .expect("run facets")
                .run_stats
                .is_some(),
            "an executed run must still report its counts"
        );
    }

    /// The subset facet is a claim about this run's read or write, so its schema
    /// type is `InputSubsetInputDatasetFacet` / `OutputSubsetOutputDatasetFacet`
    /// — an `InputDatasetFacet` / `OutputDatasetFacet`. A consumer following
    /// those schemas looks under `inputFacets` / `outputFacets` and would find
    /// nothing if the facet rode in the dataset's own `facets`, so the emitted
    /// position and the pinned schema URL have to agree. Symlinks and column
    /// lineage are plain dataset facets and stay in `facets`.
    #[test]
    fn subset_rides_in_the_position_bucket_its_schema_names() {
        use crate::logical_identity::{
            DatasetIdentifierType, DatasetSubset, ExternalDatasetIdentity, LineageNodeBinding,
            SymlinkIdentifier,
        };
        use crate::openlineage::{
            INPUT_DATASET_SUBSET_FACET_SCHEMA_URL, OUTPUT_DATASET_SUBSET_FACET_SCHEMA_URL,
        };

        let read = LineageNodeBinding::new(
            "in",
            ExternalDatasetIdentity::catalog("analytics", "orders").unwrap(),
        )
        .with_subset(DatasetSubset::input("dt=2026-08-07").unwrap())
        .with_symlink(
            SymlinkIdentifier::new("hive://cluster", "db.orders", DatasetIdentifierType::Table)
                .unwrap(),
        );
        let written = LineageNodeBinding::new(
            "out",
            ExternalDatasetIdentity::catalog("analytics", "summary").unwrap(),
        )
        .with_subset(DatasetSubset::output("dt=2026-08-07").unwrap());

        let mut lineage = sample_lineage();
        lineage.input_identity_facets = BTreeMap::from([(
            lineage.inputs[0].clone(),
            LineageIdentityContext::external([read])
                .unwrap()
                .require("in")
                .unwrap()
                .facets(),
        )]);
        lineage.outputs[0].identity_facets = LineageIdentityContext::external([written])
            .unwrap()
            .require("out")
            .unwrap()
            .facets();

        let complete = terminal_event(&lineage, sample_job(), &lifecycle(Terminal::Complete));
        let v = serde_json::to_value(&complete).unwrap();

        let input = &v["inputs"][0];
        assert_eq!(
            input["inputFacets"]["subset"]["inputCondition"]["locations"][0],
            "dt=2026-08-07"
        );
        assert_eq!(
            input["inputFacets"]["subset"]["_schemaURL"],
            INPUT_DATASET_SUBSET_FACET_SCHEMA_URL
        );
        assert!(
            input["facets"].get("subset").is_none(),
            "an input-position facet must not ride in the dataset's own facets"
        );
        // A symlink is true of the dataset in any position, so it stays put.
        assert_eq!(
            input["facets"]["symlinks"]["identifiers"][0]["name"],
            "db.orders"
        );

        let output = &v["outputs"][0];
        assert_eq!(
            output["outputFacets"]["subset"]["outputCondition"]["locations"][0],
            "dt=2026-08-07"
        );
        assert_eq!(
            output["outputFacets"]["subset"]["_schemaURL"],
            OUTPUT_DATASET_SUBSET_FACET_SCHEMA_URL
        );
        assert!(output["facets"].get("subset").is_none());
        assert!(
            output["facets"]["columnLineage"].is_object(),
            "column lineage is a plain dataset facet and stays in `facets`"
        );
        assert!(
            output.get("inputFacets").is_none(),
            "a dataset occupies one position, so only that position's bucket is set"
        );

        let back: RunEvent =
            serde_json::from_str(&serde_json::to_string(&complete).unwrap()).expect("round trip");
        assert_eq!(complete, back);
    }

    /// One rule, both roles, every event. A symlink names the dataset and is
    /// always true of it; the subset names what this run touched and is only
    /// true where the run finished touching it. Applying that to outputs and
    /// not inputs left a failed run claiming it had read members it never
    /// reached, and left a START unable to resolve an alternate identity that
    /// a COMPLETE could.
    #[test]
    fn only_a_completed_run_claims_which_members_it_touched() {
        use crate::logical_identity::{
            DatasetIdentifierType, DatasetSubset, ExternalDatasetIdentity, LineageIdentityContext,
            LineageNodeBinding, SymlinkIdentifier,
        };

        let read = LineageNodeBinding::new(
            "in",
            ExternalDatasetIdentity::catalog("analytics", "orders").unwrap(),
        )
        .with_subset(DatasetSubset::input("dt=2026-08-07").unwrap())
        .with_symlink(
            SymlinkIdentifier::new("hive://cluster", "db.orders", DatasetIdentifierType::Table)
                .unwrap(),
        );
        let written = LineageNodeBinding::new(
            "out",
            ExternalDatasetIdentity::catalog("analytics", "summary").unwrap(),
        )
        .with_subset(DatasetSubset::output("dt=2026-08-07").unwrap())
        .with_symlink(
            SymlinkIdentifier::new("hive://cluster", "db.summary", DatasetIdentifierType::Table)
                .unwrap(),
        );

        let mut lineage = sample_lineage();
        lineage.input_identity_facets = BTreeMap::from([(
            lineage.inputs[0].clone(),
            LineageIdentityContext::external([read])
                .unwrap()
                .require("in")
                .unwrap()
                .facets(),
        )]);
        lineage.outputs[0].identity_facets = LineageIdentityContext::external([written])
            .unwrap()
            .require("out")
            .unwrap()
            .facets();

        let start = serde_json::to_value(start_event(&lineage, sample_job(), &start_facts()))
            .expect("the start event serializes");
        for terminal in [
            Terminal::Abort,
            Terminal::Fail {
                failure: FailureClassification::for_code("source.data.invalid")
                    .expect("registered failure"),
            },
        ] {
            let event =
                serde_json::to_value(terminal_event(&lineage, sample_job(), &lifecycle(terminal)))
                    .expect("the terminal event serializes");
            for (role, bucket) in [("inputs", "inputFacets"), ("outputs", "outputFacets")] {
                let dataset = &event[role][0];
                assert!(
                    dataset.get(bucket).is_none() || dataset[bucket].get("subset").is_none(),
                    "a run that did not finish makes no claim about the {role} it touched"
                );
                assert!(
                    dataset["facets"]["symlinks"]["identifiers"][0]["name"].is_string(),
                    "but the {role} dataset's other name is true either way"
                );
            }
        }
        for role in ["inputs", "outputs"] {
            assert!(
                start[role][0]["facets"]["symlinks"]["identifiers"][0]["name"].is_string(),
                "and it is resolvable from the {role} of a START too"
            );
            assert!(
                start[role][0].get("inputFacets").is_none()
                    && start[role][0].get("outputFacets").is_none(),
                "a START has touched nothing yet"
            );
        }
    }

    /// A dataset with no authorized subset carries neither position bucket, so
    /// the keys are absent rather than serialized as empty objects.
    #[test]
    fn position_buckets_are_omitted_without_a_subset() {
        let complete = terminal_event(
            &sample_lineage(),
            sample_job(),
            &lifecycle(Terminal::Complete),
        );
        let v = serde_json::to_value(&complete).unwrap();
        assert!(v["inputs"][0].get("inputFacets").is_none());
        assert!(v["outputs"][0].get("outputFacets").is_none());
    }

    #[test]
    fn pairs_start_then_complete_sharing_run_id() {
        let events = run_events(
            &sample_lineage(),
            sample_job(),
            &lifecycle(Terminal::Complete),
        );
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, EventType::Start);
        assert_eq!(events[1].event_type, EventType::Complete);
        assert_eq!(events[0].run.run_id, start_facts().execution_id);
        assert_eq!(events[1].run.run_id, start_facts().execution_id);
    }

    #[test]
    fn facet_attaches_to_complete_outputs_only() {
        let events = run_events(
            &sample_lineage(),
            sample_job(),
            &lifecycle(Terminal::Complete),
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
            sample_job(),
            &lifecycle(Terminal::Complete),
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

    #[test]
    fn live_start_announces_datasets_without_facets() {
        let start = start_event(&sample_lineage(), sample_job(), &start_facts());
        assert_eq!(start.event_type, EventType::Start);
        assert_eq!(start.run.run_id, start_facts().execution_id);
        // A live START names the datasets by identity, but carries no facets: no
        // column lineage (nothing produced yet); only shared correlation is set.
        assert_eq!(start.inputs.len(), 1);
        assert!(start.inputs[0].facets.is_none());
        assert_eq!(start.outputs.len(), 1);
        assert!(start.outputs[0].facets.is_none());
        assert!(
            start
                .run
                .facets
                .as_ref()
                .and_then(|facets| facets.clinker_batch.as_ref())
                .is_some()
        );
    }

    #[test]
    fn live_complete_carries_lineage_and_run_stats() {
        let complete = terminal_event(
            &sample_lineage(),
            sample_job(),
            &lifecycle(Terminal::Complete),
        );
        assert_eq!(complete.event_type, EventType::Complete);
        // Column lineage rides on the COMPLETE outputs.
        let facets = complete.outputs[0].facets.as_ref().expect("output facets");
        assert!(facets.column_lineage.is_some());
        // Run stats ride on the run facet; no error facet on a clean complete.
        let run_facets = complete.run.facets.as_ref().expect("run facets");
        let rs = run_facets.run_stats.as_ref().expect("run stats facet");
        assert_eq!(rs.records_read, 100);
        assert_eq!(rs.records_written, 97);
        assert_eq!(rs.records_dlq, 3);
        assert_eq!(rs.duration_ms, 1234);
        assert!(run_facets.error_message.is_none());
    }

    #[test]
    fn live_fail_carries_error_and_omits_column_lineage() {
        let fail = terminal_event(
            &sample_lineage(),
            sample_job(),
            &lifecycle(Terminal::Fail {
                failure: FailureClassification::for_code("source.data.invalid")
                    .expect("registered failure"),
            }),
        );
        assert_eq!(fail.event_type, EventType::Fail);
        // A failed run did not fully produce its outputs: identities only, no
        // column-lineage facet.
        assert_eq!(fail.outputs.len(), 1);
        assert!(fail.outputs[0].facets.is_none());
        let run_facets = fail.run.facets.as_ref().expect("run facets");
        assert!(run_facets.run_stats.is_some());
        let err = run_facets.error_message.as_ref().expect("error facet");
        assert_eq!(
            err.message,
            "source data does not satisfy the admitted plan"
        );
        assert_eq!(err.programming_language, "rust");
        assert_eq!(
            run_facets
                .clinker_failure
                .as_ref()
                .expect("failure classification")
                .code,
            "source.data.invalid"
        );
    }

    #[test]
    fn live_abort_has_no_error_and_no_column_lineage() {
        let abort = terminal_event(&sample_lineage(), sample_job(), &lifecycle(Terminal::Abort));
        assert_eq!(abort.event_type, EventType::Abort);
        assert!(abort.outputs[0].facets.is_none());
        let run_facets = abort.run.facets.as_ref().expect("run facets");
        assert!(run_facets.run_stats.is_some());
        assert!(run_facets.error_message.is_none());
    }
}
