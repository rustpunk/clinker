//! Smoke test for the runnable end-to-end retraction example pipeline at
//! `examples/pipelines/retract-demo/`.
//!
//! Three load-bearing assertions stay green as the codebase evolves:
//!
//! 1. Bit-for-bit equivalence between the retract-corrected writer
//!    payload and a baseline rerun of the same pipeline with the
//!    sentinel row removed from the input. Anything that breaks the
//!    five-phase correlation-commit chain (lineage drop, retract
//!    mis-attribution, replay overshoot, window buffer-recompute leak)
//!    surfaces here as a value mismatch.
//!
//! 2. The DLQ contains exactly the trigger row, with its `trigger`
//!    flag set. Single-source pipelines under the `Any` fan-out
//!    policy with one bad CK group emit one DLQ entry; collateral
//!    fan-out is not exercised by this fixture (one row per CK
//!    group, so a CK group's only contributor is its own trigger).
//!
//! 3. The retraction counters surfaced by the orchestrator
//!    (`PipelineCounters.retraction.*`) increment in the expected
//!    direction. The demo's geometry (relaxed-CK aggregate plus a
//!    buffer-mode window upstream) exercises three counter paths:
//!    `groups_recomputed` for the aggregator's recompute step,
//!    `partitions_recomputed` for the window's wholesale-recompute
//!    step, and `subdag_replay_rows` for the deltas pushed downstream.
//!    `degrade_fallback_count` stays at zero because the demo's
//!    per-group memory budget never breaches the orchestrator's
//!    degrade threshold.

use std::collections::HashMap;
use std::fs;
use std::io::{self, Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use clinker_core::config::{CompileContext, PipelineConfig, parse_config};
use clinker_core::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams};

#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    fn new() -> Self {
        Self::default()
    }

    fn as_string(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}

/// Path to the workspace root, derived from the crate manifest's
/// `..` walk. Used to locate the `examples/pipelines/retract-demo/`
/// fixture independently of the cwd the test harness runs under.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

fn demo_dir() -> PathBuf {
    workspace_root()
        .join("examples")
        .join("pipelines")
        .join("retract-demo")
}

fn read_demo_yaml() -> String {
    fs::read_to_string(demo_dir().join("pipeline.yaml")).expect("read demo pipeline.yaml")
}

fn read_demo_csv(name: &str) -> String {
    fs::read_to_string(demo_dir().join("data").join(name)).expect("read demo csv")
}

/// Run the demo pipeline against the supplied orders/audit_events CSV
/// payloads. Audit events stream is held constant; orders is the dial
/// the test turns to flip between the full input and the baseline.
fn run_demo(orders_csv: &str, audit_events_csv: &str) -> (ExecutionReport, String) {
    let yaml = read_demo_yaml();
    let config: PipelineConfig = parse_config(&yaml).expect("parse demo pipeline");

    // The pipeline.yaml's `path:` fields point at relative
    // `./output/...` files. Resolve them against `demo_dir()` so the
    // compile-time path validation accepts them; the actual writers
    // are in-memory `SharedBuffer`s, so no file is opened.
    let ctx = CompileContext::new(demo_dir());
    let plan = config.compile(&ctx).expect("compile demo pipeline");

    let primary = config
        .source_configs()
        .next()
        .expect("demo declares orders source")
        .name
        .clone();

    let mut readers: HashMap<String, Box<dyn Read + Send>> = HashMap::new();
    readers.insert(
        "orders".to_string(),
        Box::new(Cursor::new(orders_csv.as_bytes().to_vec())) as Box<dyn Read + Send>,
    );
    readers.insert(
        "audit_events".to_string(),
        Box::new(Cursor::new(audit_events_csv.as_bytes().to_vec())) as Box<dyn Read + Send>,
    );
    assert_eq!(primary, "orders", "demo's primary driving source is orders");

    let report_buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "report".to_string(),
        Box::new(report_buf.clone()) as Box<dyn Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: "retract-demo-smoke".to_string(),
        batch_id: "smoke-001".to_string(),
        pipeline_vars: config
            .pipeline
            .vars
            .as_ref()
            .map(clinker_core::config::convert_pipeline_vars)
            .unwrap_or_default(),
        shutdown_token: None,
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("demo pipeline run");
    (report, report_buf.as_string())
}

/// Return `csv` with every row whose first column matches `excluded`
/// dropped. Header is preserved.
fn drop_orders(csv: &str, excluded: &[&str]) -> String {
    let mut out = String::new();
    for (i, line) in csv.lines().enumerate() {
        if i == 0 {
            out.push_str(line);
            out.push('\n');
            continue;
        }
        let id = line.split(',').next().unwrap_or("");
        if !excluded.contains(&id) {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

/// Stable sort of CSV body lines for diffing against a baseline. The
/// header is preserved; data lines below it sort lexicographically.
fn sort_body(s: &str) -> Vec<String> {
    let mut lines: Vec<String> = s.lines().map(|l| l.to_string()).collect();
    if !lines.is_empty() {
        let header = lines.remove(0);
        lines.sort();
        lines.insert(0, header);
    }
    lines
}

/// Sanity check that the demo fixture still exists. A reorganization
/// of `examples/` that broke the layout would surface here long
/// before the integration assertions.
#[test]
fn demo_files_present() {
    let dir = demo_dir();
    for relative in [
        Path::new("pipeline.yaml"),
        Path::new("orders.schema.yaml"),
        Path::new("audit_events.schema.yaml"),
        Path::new("README.md"),
        Path::new("data/orders.csv"),
        Path::new("data/audit_events.csv"),
    ] {
        let path = dir.join(relative);
        assert!(path.exists(), "demo fixture missing: {}", path.display());
    }
}

/// Bit-for-bit equivalence: retract-corrected output equals baseline
/// rerun output (header + sorted body). The plan-level comparison
/// proves the orchestrator's recompute, replay, and flush phases
/// produce the same CSV the user would have gotten if they had
/// pre-filtered the bad row out of the input.
#[test]
fn demo_retract_output_matches_baseline_rerun() {
    let orders = read_demo_csv("orders.csv");
    let audit_events = read_demo_csv("audit_events.csv");

    let baseline_orders = drop_orders(&orders, &["O08"]);

    let (_retract_report, retract_output) = run_demo(&orders, &audit_events);
    let (baseline_report, baseline_output) = run_demo(&baseline_orders, &audit_events);

    assert_eq!(
        baseline_report.dlq_entries.len(),
        0,
        "baseline rerun has no failing rows; got DLQ entries: {:?}",
        baseline_report.dlq_entries
    );
    assert_eq!(baseline_report.counters.dlq_count, 0);

    assert_eq!(
        sort_body(&retract_output),
        sort_body(&baseline_output),
        "retract-corrected output must equal baseline rerun output:\n\
         retract:\n{retract_output}\nbaseline:\n{baseline_output}"
    );
}

/// DLQ shape: exactly one entry, the trigger, whose source row carries
/// the `BAD` quality_score. The single-source-per-CK-group geometry
/// of the demo's input makes collateral fan-out a no-op (each CK
/// group has only one contributor — itself).
#[test]
fn demo_dlq_contains_only_the_sentinel_trigger() {
    let orders = read_demo_csv("orders.csv");
    let audit_events = read_demo_csv("audit_events.csv");

    let (report, _output) = run_demo(&orders, &audit_events);

    assert_eq!(
        report.dlq_entries.len(),
        1,
        "expected exactly one DLQ entry — the BAD-quality sentinel row, got: {:?}",
        report.dlq_entries
    );
    assert_eq!(report.counters.dlq_count, 1);
    let entry = &report.dlq_entries[0];
    assert!(entry.trigger, "the only DLQ entry is the trigger");
    let bad_carries_o08 = entry
        .original_record
        .values()
        .iter()
        .any(|v| matches!(v, clinker_record::Value::String(s) if s.as_ref() == "O08"));
    assert!(
        bad_carries_o08,
        "trigger row must carry the sentinel order_id O08; got: {entry:?}"
    );
}

/// Retraction metrics counters fire under the demo workload. The
/// demo's geometry (buffer-mode aggregator + buffer-recompute
/// window) exercises every retraction pathway in a single run.
/// `degrade_fallback_count` stays at zero — the per-group memory
/// pressure is well below the orchestrator's degrade threshold.
#[test]
fn demo_retraction_counters_fire_in_the_expected_direction() {
    let orders = read_demo_csv("orders.csv");
    let audit_events = read_demo_csv("audit_events.csv");

    let (report, _output) = run_demo(&orders, &audit_events);
    let r = &report.counters.retraction;

    // Aggregate retract path: relaxed-CK aggregator's recompute phase
    // emits at least one delta when a triggered group's contributors
    // change, so `groups_recomputed` increments per surviving group
    // even when the retracted source row is the only contributor.
    assert!(
        r.groups_recomputed > 0,
        "groups_recomputed must increment on relaxed retract, got {}",
        r.groups_recomputed
    );

    // Window retract path: the partition containing the retracted row
    // wholesale-recomputes its buffered output, so
    // `partitions_recomputed` increments by exactly one (the HR
    // partition the trigger row contributed to).
    assert!(
        r.partitions_recomputed > 0,
        "partitions_recomputed must increment on relaxed retract under a buffer-mode window, \
         got {}",
        r.partitions_recomputed
    );

    // Replay phase: per-iteration counters are summed across the
    // post-recompute deltas pushed downstream. The demo produces 18
    // replay rows under a stable codebase; the loose `> 0` assertion
    // is sufficient for the smoke-test invariant.
    assert!(
        r.subdag_replay_rows > 0,
        "subdag_replay_rows must increment on relaxed retract, got {}",
        r.subdag_replay_rows
    );

    // The demo's per-group memory budget is well below the
    // orchestrator's degrade threshold, so the fallback path never
    // fires. A non-zero observation here would mean the runtime had
    // to drop a group out of the retract-affordable lattice and DLQ
    // it under strict-collateral semantics — a regression worth
    // surfacing in the smoke test.
    assert_eq!(
        r.degrade_fallback_count, 0,
        "demo workload must not trip degrade fallback, got {}",
        r.degrade_fallback_count
    );
}
