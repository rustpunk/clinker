//! Assemble a built column lineage into OpenLineage run events.
//!
//! [`column_lineage`](crate::column_lineage) computes dataset identities and the
//! per-output column-lineage facet but stays free of run-lifecycle concerns — it
//! has no run id and no clock. This module is the bridge to the wire model, in two
//! shapes:
//!
//! - [`run_events`] pairs a `START` and a `COMPLETE` [`RunEvent`] under one
//!   `run_id` for the **static, plan-derived** export (`--lineage`): both events
//!   share one `event_time`, no rows are processed, and the column-lineage facet
//!   rides on the `COMPLETE` outputs.
//! - [`start_event`] / [`terminal_event`], driven by [`LiveRunEmitter`], describe
//!   an **actual execution**: a `START` at run begin and a terminal `COMPLETE` /
//!   `FAIL` / `ABORT` at run end, each with its own `event_time` and the real run
//!   stats ([`RunStats`]) captured at that boundary.
//!
//! `run_id` and every `event_time` are caller-supplied; this crate has no UUID or
//! clock dependency, so the CLI passes the run's `execution_id` and UTC
//! timestamps.

use std::io::{self, Write};

use crate::builder::PlanColumnLineage;
use crate::openlineage::{
    Dataset, DatasetFacets, ErrorMessageRunFacet, EventType, Job, OPENLINEAGE_SCHEMA_URL, PRODUCER,
    Run, RunEvent, RunFacets, RunStatsFacet, write_ndjson,
};

/// The input datasets of a run, as bare identities (no facets).
fn input_datasets(lineage: &PlanColumnLineage) -> Vec<Dataset> {
    lineage.inputs.iter().cloned().map(Dataset::from).collect()
}

/// The output datasets of a run as bare identities (no facets) — the shape used
/// on a `START` and on a non-`COMPLETE` terminal event, which have no completed
/// column lineage to attach.
fn output_identities(lineage: &PlanColumnLineage) -> Vec<Dataset> {
    lineage
        .outputs
        .iter()
        .map(|out| Dataset::from(out.dataset.clone()))
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
            dataset.facets = Some(DatasetFacets {
                column_lineage: Some(out.facet.clone()),
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
/// datasets; the `COMPLETE` event carries the input datasets (facet-less) and the
/// output datasets, each bearing its `columnLineage` facet. This is a static,
/// plan-derived export, so a single `event_time` is used for both events.
pub fn run_events(
    lineage: &PlanColumnLineage,
    run_id: &str,
    job: Job,
    event_time: &str,
) -> Vec<RunEvent> {
    let start = RunEvent {
        event_time: event_time.to_string(),
        producer: PRODUCER.to_string(),
        schema_url: OPENLINEAGE_SCHEMA_URL.to_string(),
        event_type: EventType::Start,
        run: Run::new(run_id),
        job: job.clone(),
        inputs: Vec::new(),
        outputs: Vec::new(),
    };

    let complete = RunEvent {
        event_time: event_time.to_string(),
        producer: PRODUCER.to_string(),
        schema_url: OPENLINEAGE_SCHEMA_URL.to_string(),
        event_type: EventType::Complete,
        run: Run::new(run_id),
        job,
        inputs: input_datasets(lineage),
        outputs: outputs_with_lineage(lineage),
    };

    vec![start, complete]
}

/// Whole-run statistics captured at a live run's terminal boundary and surfaced
/// as the `clinker_runStats` run facet on the terminal event. `Default` (all
/// zero) is the honest shape when no run completed — the [`LiveRunEmitter`] Drop
/// safety net uses it.
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Terminal {
    /// The run finished cleanly — the terminal event carries the column-lineage
    /// facets on its outputs.
    Complete,
    /// The run was cancelled (e.g. a shutdown signal) after a partial drain.
    Abort,
    /// The run errored; `error` is surfaced via the standard error-message facet.
    Fail { error: String },
}

/// Build the live `START` event for an actual run.
///
/// Unlike the static [`run_events`] `START` (which is dataset-free), a live
/// `START` announces the run's input and output datasets by identity — no facets,
/// since nothing has been produced yet. The column-lineage facets and run stats
/// arrive on the [`terminal_event`].
pub fn start_event(
    lineage: &PlanColumnLineage,
    run_id: &str,
    job: Job,
    event_time: &str,
) -> RunEvent {
    RunEvent {
        event_time: event_time.to_string(),
        producer: PRODUCER.to_string(),
        schema_url: OPENLINEAGE_SCHEMA_URL.to_string(),
        event_type: EventType::Start,
        run: Run::new(run_id),
        job,
        inputs: input_datasets(lineage),
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
    run_id: &str,
    job: Job,
    event_time: &str,
    outcome: Terminal,
    stats: RunStats,
) -> RunEvent {
    let (event_type, error) = match outcome {
        Terminal::Complete => (EventType::Complete, None),
        Terminal::Abort => (EventType::Abort, None),
        Terminal::Fail { error } => (EventType::Fail, Some(error)),
    };
    let run = Run {
        run_id: run_id.to_string(),
        facets: Some(RunFacets {
            run_stats: Some(RunStatsFacet::new(
                stats.records_read,
                stats.records_written,
                stats.records_dlq,
                stats.duration_ms,
            )),
            error_message: error.map(ErrorMessageRunFacet::new),
        }),
    };
    let outputs = if matches!(event_type, EventType::Complete) {
        outputs_with_lineage(lineage)
    } else {
        output_identities(lineage)
    };
    RunEvent {
        event_time: event_time.to_string(),
        producer: PRODUCER.to_string(),
        schema_url: OPENLINEAGE_SCHEMA_URL.to_string(),
        event_type,
        run,
        job,
        inputs: input_datasets(lineage),
        outputs,
    }
}

/// Emits live OpenLineage run events to a writer across an actual run's
/// lifecycle, guaranteeing that a `START` is always closed by a terminal event.
///
/// The caller drives it explicitly — [`emit_start`](Self::emit_start) before the
/// run body, then [`emit_terminal`](Self::emit_terminal) once the outcome is
/// known. Each event is written and flushed immediately, so the `START` reaches
/// the sink before the run body executes and a mid-run crash still leaves an
/// observable open run. If the emitter is dropped after a `START` with no explicit
/// terminal (an early return via `?`, or a panic), its `Drop` writes a best-effort
/// `FAIL` so no started run is left dangling.
///
/// The type is generic over the sink `W`, so the CLI wires a file or stdout and
/// tests wire an in-memory buffer. The crate stays clock-free: the caller supplies
/// every timestamp, and the Drop safety net reuses the construction-time
/// timestamp for its emergency `FAIL`.
pub struct LiveRunEmitter<W: Write> {
    writer: W,
    lineage: PlanColumnLineage,
    job: Job,
    run_id: String,
    /// Timestamp captured at construction (the run's begin time). Used for the
    /// `START` and reused by the Drop safety net, keeping this crate clock-free.
    start_time: String,
    started: bool,
    terminal_emitted: bool,
}

impl<W: Write> LiveRunEmitter<W> {
    /// Create an emitter over `writer`. `start_time` is the run's begin timestamp
    /// (RFC-3339 UTC); it stamps the `START` event and the Drop-fallback `FAIL`.
    pub fn new(
        writer: W,
        lineage: PlanColumnLineage,
        job: Job,
        run_id: impl Into<String>,
        start_time: impl Into<String>,
    ) -> Self {
        LiveRunEmitter {
            writer,
            lineage,
            job,
            run_id: run_id.into(),
            start_time: start_time.into(),
            started: false,
            terminal_emitted: false,
        }
    }

    /// Emit and flush the `START` event. Call once, before the run body.
    pub fn emit_start(&mut self) -> io::Result<()> {
        let event = start_event(
            &self.lineage,
            &self.run_id,
            self.job.clone(),
            &self.start_time,
        );
        write_ndjson(std::slice::from_ref(&event), &mut self.writer)?;
        self.started = true;
        Ok(())
    }

    /// Emit and flush the terminal event closing out the run. Call once, after the
    /// outcome is known. `event_time` is the run's end timestamp.
    pub fn emit_terminal(
        &mut self,
        event_time: &str,
        outcome: Terminal,
        stats: RunStats,
    ) -> io::Result<()> {
        let event = terminal_event(
            &self.lineage,
            &self.run_id,
            self.job.clone(),
            event_time,
            outcome,
            stats,
        );
        write_ndjson(std::slice::from_ref(&event), &mut self.writer)?;
        self.terminal_emitted = true;
        Ok(())
    }
}

impl<W: Write> Drop for LiveRunEmitter<W> {
    fn drop(&mut self) {
        // A `START` with no terminal (early `?` return, panic, or a caller that
        // forgot to close out) would leave a dangling open run in the sink. Emit a
        // best-effort `FAIL` so every started run is closed. Errors are
        // unrecoverable in Drop and deliberately ignored; the timestamp falls back
        // to the construction time because this crate holds no clock.
        if self.started && !self.terminal_emitted {
            let event = terminal_event(
                &self.lineage,
                &self.run_id,
                self.job.clone(),
                &self.start_time,
                Terminal::Fail {
                    error: "run ended without reaching a terminal lineage event".to_string(),
                },
                RunStats::default(),
            );
            let _ = write_ndjson(std::slice::from_ref(&event), &mut self.writer);
        }
    }
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

    fn stats() -> RunStats {
        RunStats {
            records_read: 100,
            records_written: 97,
            records_dlq: 3,
            duration_ms: 1234,
        }
    }

    #[test]
    fn live_start_announces_datasets_without_facets() {
        let start = start_event(
            &sample_lineage(),
            "run-1",
            sample_job(),
            "2020-02-22T22:42:42Z",
        );
        assert_eq!(start.event_type, EventType::Start);
        assert_eq!(start.run.run_id, "run-1");
        // A live START names the datasets by identity, but carries no facets: no
        // column lineage (nothing produced yet) and no run facets.
        assert_eq!(start.inputs.len(), 1);
        assert!(start.inputs[0].facets.is_none());
        assert_eq!(start.outputs.len(), 1);
        assert!(start.outputs[0].facets.is_none());
        assert!(start.run.facets.is_none());
    }

    #[test]
    fn live_complete_carries_lineage_and_run_stats() {
        let complete = terminal_event(
            &sample_lineage(),
            "run-1",
            sample_job(),
            "2020-02-22T22:43:00Z",
            Terminal::Complete,
            stats(),
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
            "run-1",
            sample_job(),
            "2020-02-22T22:43:00Z",
            Terminal::Fail {
                error: "source read failed".to_string(),
            },
            stats(),
        );
        assert_eq!(fail.event_type, EventType::Fail);
        // A failed run did not fully produce its outputs: identities only, no
        // column-lineage facet.
        assert_eq!(fail.outputs.len(), 1);
        assert!(fail.outputs[0].facets.is_none());
        let run_facets = fail.run.facets.as_ref().expect("run facets");
        assert!(run_facets.run_stats.is_some());
        let err = run_facets.error_message.as_ref().expect("error facet");
        assert_eq!(err.message, "source read failed");
        assert_eq!(err.programming_language, "rust");
    }

    #[test]
    fn live_abort_has_no_error_and_no_column_lineage() {
        let abort = terminal_event(
            &sample_lineage(),
            "run-1",
            sample_job(),
            "2020-02-22T22:43:00Z",
            Terminal::Abort,
            stats(),
        );
        assert_eq!(abort.event_type, EventType::Abort);
        assert!(abort.outputs[0].facets.is_none());
        let run_facets = abort.run.facets.as_ref().expect("run facets");
        assert!(run_facets.run_stats.is_some());
        assert!(run_facets.error_message.is_none());
    }

    /// Parse the NDJSON `buf` into one JSON value per line.
    fn lines(buf: &[u8]) -> Vec<serde_json::Value> {
        String::from_utf8(buf.to_vec())
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    #[test]
    fn emitter_writes_start_then_complete_sharing_run_id() {
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut emitter = LiveRunEmitter::new(
                &mut buf,
                sample_lineage(),
                sample_job(),
                "run-1",
                "2020-02-22T22:42:42Z",
            );
            emitter.emit_start().unwrap();
            emitter
                .emit_terminal("2020-02-22T22:43:00Z", Terminal::Complete, stats())
                .unwrap();
        }
        let events = lines(&buf);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["eventType"], "START");
        assert_eq!(events[1]["eventType"], "COMPLETE");
        assert_eq!(events[0]["run"]["runId"], "run-1");
        assert_eq!(events[1]["run"]["runId"], "run-1");
        // Distinct event times: START at begin, COMPLETE at end.
        assert_eq!(events[0]["eventTime"], "2020-02-22T22:42:42Z");
        assert_eq!(events[1]["eventTime"], "2020-02-22T22:43:00Z");
        // COMPLETE carries the column-lineage facet and non-zero run stats.
        assert!(events[1]["outputs"][0]["facets"]["columnLineage"].is_object());
        let rs = &events[1]["run"]["facets"]["clinker_runStats"];
        assert_eq!(rs["recordsRead"], 100);
        assert_eq!(rs["recordsWritten"], 97);
    }

    #[test]
    fn emitter_drop_without_terminal_emits_fail() {
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut emitter = LiveRunEmitter::new(
                &mut buf,
                sample_lineage(),
                sample_job(),
                "run-1",
                "2020-02-22T22:42:42Z",
            );
            emitter.emit_start().unwrap();
            // No explicit terminal — dropping here must close the run out as FAIL.
        }
        let events = lines(&buf);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["eventType"], "START");
        assert_eq!(events[1]["eventType"], "FAIL");
        assert!(
            events[1]["run"]["facets"]["errorMessage"]["message"]
                .as_str()
                .unwrap()
                .contains("without reaching a terminal")
        );
    }

    #[test]
    fn emitter_drop_after_terminal_is_silent() {
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut emitter = LiveRunEmitter::new(
                &mut buf,
                sample_lineage(),
                sample_job(),
                "run-1",
                "2020-02-22T22:42:42Z",
            );
            emitter.emit_start().unwrap();
            emitter
                .emit_terminal(
                    "2020-02-22T22:43:00Z",
                    Terminal::Fail {
                        error: "boom".to_string(),
                    },
                    stats(),
                )
                .unwrap();
        }
        // Exactly START + FAIL — Drop must not append a second terminal.
        let events = lines(&buf);
        assert_eq!(events.len(), 2);
        assert_eq!(events[1]["eventType"], "FAIL");
        assert_eq!(
            events[1]["run"]["facets"]["errorMessage"]["message"],
            "boom"
        );
    }
}
