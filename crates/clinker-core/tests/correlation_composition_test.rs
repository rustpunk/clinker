//! Composition-body correlation tests.
//!
//! Locks two properties of the deferred-commit Output buffer across
//! the composition boundary:
//!
//! * A Transform error inside a composition body funnels through the
//!   same correlation-buffer trigger path used by top-level Transform
//!   errors. The whole correlation group DLQs uniformly; clean groups
//!   pass through.
//!
//! * Group identity is captured at Source ingest as
//!   `__cxl_correlation_key` meta and survives a rewrite of the
//!   underlying field — even when the rewrite happens inside a
//!   composition body.
//!
//! These tests are the body-scope siblings of
//! `executor::tests::correlated_dlq::one_fail_dlqs_whole_group` and
//! `executor::tests::correlated_dlq::group_identity_fixed_at_ingest_when_transform_rewrites_key`.

use std::collections::HashMap;
use std::io::{self, Cursor, Read, Write};
use std::path::PathBuf;
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

fn fixture_workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn test_params(config: &PipelineConfig) -> PipelineRunParams {
    let pipeline_vars = config
        .pipeline
        .vars
        .as_ref()
        .map(clinker_core::config::convert_pipeline_vars)
        .unwrap_or_default();
    PipelineRunParams {
        execution_id: "correlation-composition-test".to_string(),
        batch_id: "batch-001".to_string(),
        pipeline_vars,
        shutdown_token: None,
    }
}

fn run_with_composition(yaml: &str, csv_input: &str) -> (ExecutionReport, String) {
    let config = parse_config(yaml).expect("parse pipeline yaml");
    let root = fixture_workspace_root();
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));
    let plan = config.compile(&ctx).expect("compile pipeline");

    let primary = config.source_configs().next().unwrap().name.clone();
    let readers: HashMap<String, Box<dyn Read + Send>> = HashMap::from([(
        primary,
        Box::new(Cursor::new(csv_input.as_bytes().to_vec())) as Box<dyn Read + Send>,
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &test_params(&config),
    )
    .expect("pipeline run");
    (report, buf.as_string())
}

#[test]
fn composition_body_propagates_correlation_dlq() {
    // Source → composition(correlated_validate) → output. The body's
    // single Transform runs `value.to_int()`; the bad row in group A
    // surfaces a transform error inside the body. The dispatcher
    // re-enters via `execute_composition_body`, which calls
    // `dispatch_plan_node` for each body node — the same arm that
    // routes Transform errors through `record_error_to_buffer_if_grouped`
    // when `error_handling.correlation_key` is set. Group A's whole
    // 3-record stream therefore DLQs (1 trigger + 2 collaterals);
    // group B's clean record reaches the writer through the body and
    // out the parent Output sink.
    //
    // This pipeline also exercises `inject_correlation_sort`'s
    // splice between Source and Composition: the synthetic Sort
    // sits one hop upstream of the composition node. Edge-walking
    // composition port resolution (`PlanEdge.port` tag) keeps
    // dispatch coherent across the splice — without that, the
    // composition would read an emptied Source buffer and every
    // record would silently vanish before reaching the Output's
    // deferred-commit buffer.
    let yaml = r#"
pipeline:
  name: correlation_composition_validate
error_handling:
  strategy: continue
  correlation_key: employee_id
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: employee_id, type: string }
        - { name: value, type: string }
  - type: composition
    name: validate_call
    input: src
    use: ../compositions/correlated_validate.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: validate_call
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let csv = "employee_id,value\nA,100\nA,bad\nA,300\nB,400\n";
    let (report, output) = run_with_composition(yaml, csv);

    assert_eq!(
        report.counters.dlq_count, 3,
        "group A: 1 trigger (bad row) + 2 collaterals (surviving body outputs)"
    );
    assert_eq!(
        report.counters.ok_count, 1,
        "only group B's body output reaches the writer"
    );

    let triggers = report.dlq_entries.iter().filter(|e| e.trigger).count();
    let collaterals = report.dlq_entries.iter().filter(|e| !e.trigger).count();
    assert_eq!(triggers, 1, "one trigger for the bad source row");
    assert_eq!(
        collaterals, 2,
        "two collaterals for the surviving group-A body outputs"
    );

    for c in report.dlq_entries.iter().filter(|e| !e.trigger) {
        assert_eq!(
            c.category,
            clinker_core::dlq::DlqErrorCategory::Correlated,
            "body-output collateral carries the Correlated category"
        );
    }

    assert!(
        output.contains("B"),
        "output should contain group B's body output: {output}"
    );
    assert!(
        !output.contains(",bad"),
        "output must not contain the bad source row: {output}"
    );
    assert!(
        !output.contains("A,100") && !output.contains("A,300"),
        "output must NOT contain any group A body outputs (group A failed): {output}"
    );
}

#[test]
fn composition_body_key_rewrite_does_not_change_group_identity() {
    // Source → composition(correlated_rewrite) → output. The body's
    // Transform overwrites `employee_id` to a fixed string before the
    // `value.to_int()` failure fires. Group identity is captured at
    // Source ingest as `__cxl_correlation_key` meta (independent of
    // emitted fields), so the body's rewrite cannot move records
    // between groups.
    //
    // If grouping wrongly used the rewritten field value, every input
    // row would coalesce into one giant `REWRITTEN` group and the
    // single bad row would DLQ all 4 records. The ingest-anchored
    // identity contract says only the original-A group's 3 records
    // DLQ; the original-B group's 1 record cleanly emits.
    let yaml = r#"
pipeline:
  name: correlation_composition_rewrite
error_handling:
  strategy: continue
  correlation_key: employee_id
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: employee_id, type: string }
        - { name: value, type: string }
  - type: composition
    name: rewrite_call
    input: src
    use: ../compositions/correlated_rewrite.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: rewrite_call
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let csv = "employee_id,value\nA,1\nA,bad\nA,3\nB,4\n";
    let (report, _output) = run_with_composition(yaml, csv);

    assert_eq!(
        report.counters.dlq_count, 3,
        "only the original-A group (3 records) DLQs despite the body's rewrite"
    );
    assert_eq!(
        report.counters.ok_count, 1,
        "the original-B group's body output emits cleanly"
    );

    let triggers = report.dlq_entries.iter().filter(|e| e.trigger).count();
    assert_eq!(triggers, 1, "one trigger for the bad source row");
}
