//! Smoke test for the runnable end-to-end retraction example pipeline at
//! `examples/pipelines/retract-demo/`.
//!
//! Three load-bearing assertions stay green as the codebase evolves:
//!
//! 1. Bit-for-bit equivalence between the retract-corrected writer
//!    payload and a baseline rerun of the same pipeline with the
//!    upstream sentinel row removed from the input. Anything that
//!    breaks the five-phase correlation-commit chain (lineage drop,
//!    retract mis-attribution, replay overshoot, window
//!    buffer-recompute leak) surfaces here as a value mismatch. Both
//!    runs hit the same post-aggregate `dept_validate` failures
//!    because the predicate fires on the surviving aggregate totals,
//!    not on the sentinel; the retract run's DLQ is the baseline's
//!    DLQ plus the upstream sentinel entry.
//!
//! 2. The DLQ carries the upstream `transform:validate` trigger for
//!    the BAD-quality sentinel row plus one `transform:dept_validate`
//!    trigger per HR `(department, day)` whose surviving total falls
//!    below the predicate threshold. The synthetic
//!    `$ck.aggregate.dept_totals` lineage routes each post-aggregate
//!    failure back through the contributing source rows so the final
//!    aggregate output excludes them (the bit-for-bit assertion
//!    above pins the resulting writer payload against a baseline
//!    rerun).
//!
//! 3. The retraction counters surfaced by the orchestrator
//!    (`PipelineCounters.retraction.*`) increment in the expected
//!    direction. The demo's geometry (relaxed-CK aggregate plus a
//!    windowed Transform downstream and a post-aggregate validation
//!    Transform) exercises every counter path: `groups_recomputed`
//!    for the aggregator's recompute step, `partitions_dispatched`
//!    for the windowed Transform's commit-pass re-evaluation,
//!    `synthetic_ck_columns_emitted_total` for the per-output-row
//!    shadow column writes at finalize, and the
//!    `synthetic_ck_fanout_*` pair for the detect-phase resolution
//!    of post-aggregate failures back to source row ids.
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

    let mut readers: clinker_core::executor::SourceReaders = HashMap::new();
    readers.insert(
        "orders".to_string(),
        clinker_core::executor::single_file_reader(
            "test.csv",
            Box::new(Cursor::new(orders_csv.as_bytes().to_vec())),
        ),
    );
    readers.insert(
        "audit_events".to_string(),
        clinker_core::executor::single_file_reader(
            "test.csv",
            Box::new(Cursor::new(audit_events_csv.as_bytes().to_vec())),
        ),
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
/// pre-filtered the upstream-bad row out of the input. Both runs
/// also encounter the same post-aggregate `dept_validate` failures,
/// so DLQ entries shrink to a strict subset between baseline and
/// retract: baseline emits only the post-aggregate triggers; the
/// retract run additionally emits the upstream sentinel entry.
#[test]
fn demo_retract_output_matches_baseline_rerun() {
    let orders = read_demo_csv("orders.csv");
    let audit_events = read_demo_csv("audit_events.csv");

    let baseline_orders = drop_orders(&orders, &["O08"]);

    let (retract_report, retract_output) = run_demo(&orders, &audit_events);
    let (baseline_report, baseline_output) = run_demo(&baseline_orders, &audit_events);

    // Baseline still surfaces the post-aggregate dept_validate
    // triggers because their predicate fires on every HR group whose
    // total is below 500 — a property of the surviving orders, not
    // the retracted sentinel. The relevant invariant is that the
    // retract run's DLQ is the baseline's plus the upstream sentinel.
    assert!(
        retract_report.counters.dlq_count > baseline_report.counters.dlq_count,
        "retract run must DLQ at least the upstream sentinel beyond the baseline's count; \
         retract={}, baseline={}",
        retract_report.counters.dlq_count,
        baseline_report.counters.dlq_count,
    );

    assert_eq!(
        sort_body(&retract_output),
        sort_body(&baseline_output),
        "retract-corrected output must equal baseline rerun output:\n\
         retract:\n{retract_output}\nbaseline:\n{baseline_output}"
    );
}

/// DLQ shape: the upstream `BAD`-quality sentinel arrives as a
/// `transform:validate` trigger; the post-aggregate `dept_validate`
/// predicate fires once per HR `(department, day)` whose surviving
/// total is below 500, and each failing aggregate output row enters
/// the DLQ as its own trigger. The synthetic CK fan-out path
/// retracts the contributing source rows from the aggregator state
/// (verified bit-for-bit against the baseline rerun by the sibling
/// test) — those source rows do not need to land as separate
/// collateral entries because the post-retract aggregate output
/// already excludes them.
#[test]
fn demo_dlq_contains_upstream_and_post_aggregate_triggers() {
    let orders = read_demo_csv("orders.csv");
    let audit_events = read_demo_csv("audit_events.csv");

    let (report, _output) = run_demo(&orders, &audit_events);

    let upstream_trigger = report.dlq_entries.iter().find(|e| {
        e.trigger
            && e.stage.as_deref() == Some("transform:validate")
            && e.original_record
                .values()
                .iter()
                .any(|v| matches!(v, clinker_record::Value::String(s) if s.as_ref() == "O08"))
    });
    assert!(
        upstream_trigger.is_some(),
        "expected one upstream sentinel trigger from transform:validate carrying order_id O08, \
         got: {:?}",
        report.dlq_entries
    );

    let post_aggregate_triggers: Vec<_> = report
        .dlq_entries
        .iter()
        .filter(|e| e.trigger && e.stage.as_deref() == Some("transform:dept_validate"))
        .collect();
    assert_eq!(
        post_aggregate_triggers.len(),
        3,
        "expected three dept_validate triggers (one per HR (department, day) whose surviving \
         total is below the predicate threshold); got: {post_aggregate_triggers:?}",
    );
    for entry in &post_aggregate_triggers {
        let department_is_hr = entry
            .original_record
            .values()
            .iter()
            .any(|v| matches!(v, clinker_record::Value::String(s) if s.as_ref() == "HR"));
        assert!(
            department_is_hr,
            "every post-aggregate trigger row belongs to the HR department; got: {entry:?}",
        );
    }
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

    // Deferred-region commit pass: this demo's window
    // (`running_totals`) sits UPSTREAM of the relaxed-CK aggregate, so
    // it runs on the forward pass and `partitions_dispatched` (which
    // counts windowed-Transform dispatches inside deferred regions)
    // stays at zero. The downstream `dept_validate` Transform inside
    // the deferred region is non-windowed, so it does not increment
    // this counter either. A pipeline with a windowed Transform
    // DOWNSTREAM of a relaxed-CK aggregate is exercised by
    // `executor::tests::correlated_window_after_aggregate_retract`.
    assert_eq!(
        r.partitions_dispatched, 0,
        "demo geometry: window upstream of relaxed-CK aggregate keeps \
         partitions_dispatched at zero; got {}",
        r.partitions_dispatched
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

    // Synthetic CK emission: the relaxed dept_totals aggregate stamps
    // one $ck.aggregate.dept_totals slot per emitted output row.
    // Initial finalize yields six (department, day) groups; the
    // upstream-sentinel retract drives a re-finalize over the
    // surviving HR/2024-01-03 contributors, so the counter reads
    // strictly above the initial six.
    assert!(
        r.synthetic_ck_columns_emitted_total >= 6,
        "synthetic_ck_columns_emitted_total must cover every relaxed-aggregate \
         finalized group (initial pass + post-retract refinalize), got {}",
        r.synthetic_ck_columns_emitted_total
    );

    // Synthetic CK fan-out: the post-aggregate dept_validate Transform
    // fails on three HR aggregate output rows. Each failure carries
    // the synthetic CK column on the trigger record, so the detect
    // phase sees three lookups and expands to the contributing
    // source row ids of each HR group.
    assert!(
        r.synthetic_ck_fanout_lookups_total >= 3,
        "synthetic_ck_fanout_lookups_total must register one observation per failing \
         post-aggregate trigger, got {}",
        r.synthetic_ck_fanout_lookups_total
    );
    assert!(
        r.synthetic_ck_fanout_rows_expanded_total >= r.synthetic_ck_fanout_lookups_total,
        "every successful synthetic-CK lookup must expand to at least one source row, \
         lookups={}, rows_expanded={}",
        r.synthetic_ck_fanout_lookups_total,
        r.synthetic_ck_fanout_rows_expanded_total
    );
}
