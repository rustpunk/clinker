//! Source ingest-channel consumer lifecycle across every drain arm.
//!
//! Each declared Source registers a `SourceConsumer` with the
//! pipeline-scoped arbitrator whose shared handle mirrors the ingest
//! channel's `queue depth × per-record bytes` on every producer push.
//! The producer never zeroes the estimate, so the arm that drains the
//! receiver must release the registration at disconnect — otherwise the
//! last push's charge stays summed into `sum_consumer_usage()` for the
//! rest of the run and downstream stages spill / abort against memory
//! that already moved on.
//!
//! Three arms can own a source's receiver: the plain Source arm, the
//! fused `Merge.interleave`-of-Sources arm, and the fused
//! Source→Transform arm. A fourth path — a walk that never drains the
//! receiver at all (interrupt before the source's dispatch turn) — is
//! covered by the top-scope teardown sweep. Each test runs one path
//! end-to-end with an injected arbitrator and asserts the registry is
//! empty afterward; a leaked source registration keeps
//! `consumer_count()` at the declared-source count.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;
use std::sync::Arc;

/// Generous hard limit so neither the RSS arm nor the charged-bytes arm
/// of the spill / abort gates trips during these runs — the tests
/// observe registry hygiene, not pressure behavior.
const HARD_LIMIT: u64 = 100 * 1024 * 1024 * 1024;
const SPILL_FRAC: f64 = 0.80;

fn quiet_arbitrator() -> Arc<crate::pipeline::memory::MemoryArbitrator> {
    Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
        HARD_LIMIT,
        SPILL_FRAC,
        0.70,
        crate::pipeline::memory::MemoryArbitrator::default_policy(),
    ))
}

fn csv_reader(name: &str, body: &str) -> (String, crate::source::SourceInput) {
    (
        name.to_string(),
        crate::executor::single_file_reader(
            format!("{name}.csv"),
            Box::new(std::io::Cursor::new(body.as_bytes().to_vec())),
        ),
    )
}

fn sink_writers(names: &[&str]) -> HashMap<String, Box<dyn std::io::Write + Send>> {
    names
        .iter()
        .map(|n| {
            (
                n.to_string(),
                Box::new(SharedBuffer::new()) as Box<dyn std::io::Write + Send>,
            )
        })
        .collect()
}

fn run(
    yaml: &str,
    readers: crate::executor::SourceReaders,
    writers: HashMap<String, Box<dyn std::io::Write + Send>>,
    params: &PipelineRunParams,
    arbitrator: Arc<crate::pipeline::memory::MemoryArbitrator>,
) -> Result<ExecutionReport, PipelineError> {
    let config = clinker_plan::config::parse_config(yaml).expect("parse pipeline YAML");
    PipelineExecutor::run_with_readers_writers_with_arbitrator(
        &config,
        readers,
        writers.into(),
        params,
        clinker_plan::config::CompileContext::default(),
        arbitrator,
    )
}

const EVENTS_CSV: &str = "\
id,region
e1,north
e2,south
e3,north
";

/// Source → Output: the plain Source arm consumes the live receiver and
/// must release the registration at disconnect.
#[test]
fn plain_source_arm_releases_source_consumer_on_drain() {
    let yaml = r#"
pipeline:
  name: source_consumer_plain
nodes:
- type: source
  name: events
  config:
    name: events
    type: csv
    path: events.csv
    schema:
      - { name: id, type: string }
      - { name: region, type: string }
- type: output
  name: out
  input: events
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let arb = quiet_arbitrator();
    let params = PipelineRunParams {
        execution_id: "source-consumer-plain".to_string(),
        batch_id: "batch-0".to_string(),
        ..Default::default()
    };
    let report = run(
        yaml,
        HashMap::from([csv_reader("events", EVENTS_CSV)]),
        sink_writers(&["out"]),
        &params,
        Arc::clone(&arb),
    )
    .expect("pipeline must run");

    assert_eq!(
        report.counters.total_count, 3,
        "records must actually flow so the release is proven non-vacuous"
    );
    assert_eq!(
        arb.consumer_count(),
        0,
        "the drained source's registration must leave the registry"
    );
    assert_eq!(
        arb.sum_consumer_usage(),
        0,
        "no charge may survive the drained source"
    );
}

/// Two Sources → Merge.interleave → Output: the fused Merge arm takes
/// ownership of both receivers and must release each source's
/// registration as its channel closes.
#[test]
fn fused_merge_interleave_releases_source_consumers_on_drain() {
    let yaml = r#"
pipeline:
  name: source_consumer_fused_merge
nodes:
- type: source
  name: src_a
  config:
    name: src_a
    type: csv
    path: a.csv
    schema:
      - { name: id, type: string }
      - { name: region, type: string }
- type: source
  name: src_b
  config:
    name: src_b
    type: csv
    path: b.csv
    schema:
      - { name: id, type: string }
      - { name: region, type: string }
- type: merge
  name: merged
  inputs: [src_a, src_b]
  config:
    mode: interleave
- type: output
  name: out
  input: merged
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let arb = quiet_arbitrator();
    let params = PipelineRunParams {
        execution_id: "source-consumer-fused-merge".to_string(),
        batch_id: "batch-0".to_string(),
        ..Default::default()
    };
    let report = run(
        yaml,
        HashMap::from([
            csv_reader("src_a", EVENTS_CSV),
            csv_reader("src_b", EVENTS_CSV),
        ]),
        sink_writers(&["out"]),
        &params,
        Arc::clone(&arb),
    )
    .expect("pipeline must run");

    assert_eq!(report.counters.total_count, 6);
    assert_eq!(
        arb.consumer_count(),
        0,
        "both fused sources' registrations must leave the registry"
    );
    assert_eq!(arb.sum_consumer_usage(), 0);
}

/// Source → Transform → Output: the fused Transform arm consumes the
/// receiver inline and must release the registration at disconnect.
#[test]
fn fused_transform_releases_source_consumer_on_drain() {
    let yaml = r#"
pipeline:
  name: source_consumer_fused_transform
nodes:
- type: source
  name: events
  config:
    name: events
    type: csv
    path: events.csv
    schema:
      - { name: id, type: string }
      - { name: region, type: string }
- type: transform
  name: shape
  input: events
  config:
    cxl: |
      emit id = id
      emit region = region
- type: output
  name: out
  input: shape
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let arb = quiet_arbitrator();
    let params = PipelineRunParams {
        execution_id: "source-consumer-fused-transform".to_string(),
        batch_id: "batch-0".to_string(),
        ..Default::default()
    };
    let report = run(
        yaml,
        HashMap::from([csv_reader("events", EVENTS_CSV)]),
        sink_writers(&["out"]),
        &params,
        Arc::clone(&arb),
    )
    .expect("pipeline must run");

    assert_eq!(report.counters.total_count, 3);
    assert_eq!(
        arb.consumer_count(),
        0,
        "the fused-transform-drained source's registration must leave the registry"
    );
    assert_eq!(arb.sum_consumer_usage(), 0);
}

/// A writer that signals when the pipeline's first byte arrives, then
/// parks until the test releases it. Freezes the run mid-Output-turn so
/// the test can observe the arbitrator registry between the Source
/// arm's drain and the run's teardown sweeps.
struct GateWriter {
    reached: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    release: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    tripped: bool,
}

impl std::io::Write for GateWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if !self.tripped {
            self.tripped = true;
            {
                let (flag, cv) = &*self.reached;
                *flag.lock().unwrap() = true;
                cv.notify_all();
            }
            let (flag, cv) = &*self.release;
            let mut released = flag.lock().unwrap();
            while !*released {
                released = cv.wait(released).unwrap();
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The release must happen AT the Source arm's receiver disconnect, not
/// merely by end of run: a downstream stage dispatched after the drain
/// makes its spill / abort decisions against `sum_consumer_usage`, so a
/// charge that lingers until the teardown sweep would still distort
/// them. The gated writer freezes the run inside the Output turn —
/// strictly after the Source turn completed — and the registry must
/// already be empty at that point (the Output arm unregistered its
/// input slot when it drained it).
#[test]
fn source_charge_is_released_before_the_downstream_output_writes() {
    let yaml = r#"
pipeline:
  name: source_consumer_midrun
nodes:
- type: source
  name: events
  config:
    name: events
    type: csv
    path: events.csv
    schema:
      - { name: id, type: string }
      - { name: region, type: string }
- type: output
  name: out
  input: events
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let reached = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let release = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let writer = GateWriter {
        reached: Arc::clone(&reached),
        release: Arc::clone(&release),
        tripped: false,
    };
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), Box::new(writer) as _)]);

    let arb = quiet_arbitrator();
    let run_arb = Arc::clone(&arb);
    let yaml_owned = yaml.to_string();
    let run_thread = std::thread::spawn(move || {
        let params = PipelineRunParams {
            execution_id: "source-consumer-midrun".to_string(),
            batch_id: "batch-0".to_string(),
            ..Default::default()
        };
        run(
            &yaml_owned,
            HashMap::from([csv_reader("events", EVENTS_CSV)]),
            writers,
            &params,
            run_arb,
        )
    });

    {
        let (flag, cv) = &*reached;
        let guard = flag.lock().unwrap();
        let (guard, timeout) = cv
            .wait_timeout_while(guard, std::time::Duration::from_secs(60), |r| !*r)
            .unwrap();
        assert!(
            !timeout.timed_out(),
            "the Output turn never reached the writer"
        );
        drop(guard);
    }

    // Frozen inside the Output turn: the Source arm has drained its
    // receiver to disconnect and the Output arm has unregistered its
    // input slot, so the only registration that could remain is a
    // leaked source ingest consumer.
    assert_eq!(
        arb.consumer_count(),
        0,
        "the source's registration must be gone the moment its receiver drains, \
         not at end-of-run teardown"
    );
    assert_eq!(arb.sum_consumer_usage(), 0);

    {
        let (flag, cv) = &*release;
        *flag.lock().unwrap() = true;
        cv.notify_all();
    }
    run_thread
        .join()
        .expect("run thread must not panic")
        .expect("pipeline must run");
    assert_eq!(arb.consumer_count(), 0);
}

/// A shutdown token tripped before the walk starts means no dispatch arm
/// ever drains the receiver — the interrupted run must still leave the
/// registry empty via the top-scope teardown sweep.
#[test]
fn undrained_source_consumers_swept_on_interrupted_run() {
    let yaml = r#"
pipeline:
  name: source_consumer_interrupted
nodes:
- type: source
  name: events
  config:
    name: events
    type: csv
    path: events.csv
    schema:
      - { name: id, type: string }
      - { name: region, type: string }
- type: output
  name: out
  input: events
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let arb = quiet_arbitrator();
    let token = crate::pipeline::shutdown::ShutdownToken::detached();
    token.request();
    let params = PipelineRunParams {
        execution_id: "source-consumer-interrupted".to_string(),
        batch_id: "batch-0".to_string(),
        shutdown_token: Some(token),
        ..Default::default()
    };
    let report = run(
        yaml,
        HashMap::from([csv_reader("events", EVENTS_CSV)]),
        sink_writers(&["out"]),
        &params,
        Arc::clone(&arb),
    )
    .expect("an interrupted run is a graceful stop, not a failure");

    assert!(report.interrupted, "the pre-tripped token must be observed");
    assert_eq!(
        arb.consumer_count(),
        0,
        "an undrained source's registration must be swept at teardown"
    );
    assert_eq!(arb.sum_consumer_usage(), 0);
}
