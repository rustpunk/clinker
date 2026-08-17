//! End-to-end effects of source-scoped row identity on terminal accounting.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{
    ExecutionReport, PipelineExecutor, PipelineRunParams, SourceInput, SourceReaders,
};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::plan::CompiledPlan;

fn slot(path: &str, body: &str) -> FileSlot {
    FileSlot::new(
        PathBuf::from(path),
        Box::new(Cursor::new(body.as_bytes().to_vec())),
    )
}

fn params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "source-row-identity-effects".to_string(),
        batch_id: "batch".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

fn compile(yaml: &str) -> CompiledPlan {
    parse_config(yaml)
        .expect("identity-effects fixture parses")
        .compile(&CompileContext::default())
        .expect("identity-effects fixture compiles")
}

fn run(
    plan: &CompiledPlan,
    readers: SourceReaders,
    output_names: &[&str],
) -> (ExecutionReport, HashMap<String, String>) {
    let buffers: HashMap<String, SharedBuffer> = output_names
        .iter()
        .map(|name| ((*name).to_string(), SharedBuffer::new()))
        .collect();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = buffers
        .iter()
        .map(|(name, buffer)| {
            (
                name.clone(),
                Box::new(buffer.clone()) as Box<dyn std::io::Write + Send>,
            )
        })
        .collect();
    let report = PipelineExecutor::run_plan_with_readers_writers(plan, readers, writers, &params())
        .expect("identity-effects fixture executes");
    let outputs = buffers
        .into_iter()
        .map(|(name, buffer)| (name, buffer.as_string()))
        .collect();
    (report, outputs)
}

fn two_csv_readers() -> SourceReaders {
    HashMap::from([
        (
            "src_a".to_string(),
            SourceInput::Files(vec![slot("a.csv", "id,value\na1,10\n")]),
        ),
        (
            "src_b".to_string(),
            SourceInput::Files(vec![slot("b.csv", "id,value\nb1,20\n")]),
        ),
    ])
}

#[test]
fn success_state_counts_same_ordinal_sources_and_deduplicates_route_fanout() {
    let plan = compile(
        r#"
pipeline:
  name: success_identity_fanout
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: string }
        - { name: value, type: int }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: string }
        - { name: value, type: int }
  - type: merge
    name: merged
    inputs: [src_a, src_b]
  - type: route
    name: fanout
    input: merged
    config:
      mode: inclusive
      conditions:
        audit: value > 0
        report: value > 0
      default: audit
  - type: sink
    name: audit
    input: fanout
    config:
      name: audit
      type: csv
      path: audit.csv
      include_unmapped: true
  - type: sink
    name: report
    input: fanout
    config:
      name: report
      type: csv
      path: report.csv
      include_unmapped: true
"#,
    );

    let (report, outputs) = run(&plan, two_csv_readers(), &["audit", "report"]);

    assert_eq!(
        report.counters.ok_count, 2,
        "same ordinal from two Sources counts twice, while two deliveries per row count once"
    );
    assert_eq!(report.counters.records_written, 4);
    for output in outputs.values() {
        assert!(output.contains("a1,10"));
        assert!(output.contains("b1,20"));
    }
}

#[test]
fn success_state_keeps_null_correlation_fallback_rows_source_scoped() {
    let plan = compile(
        r#"
pipeline:
  name: null_correlation_identity
error_handling:
  strategy: continue
  max_group_buffer: 1
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: json
      path: a.json
      options: { format: ndjson }
      correlation_key: id
      schema:
        - { name: id, type: { nullable: string } }
        - { name: value, type: int }
  - type: source
    name: src_b
    config:
      name: src_b
      type: json
      path: b.json
      options: { format: ndjson }
      correlation_key: id
      schema:
        - { name: id, type: { nullable: string } }
        - { name: value, type: int }
  - type: merge
    name: merged
    inputs: [src_a, src_b]
  - type: sink
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#,
    );
    let readers = HashMap::from([
        (
            "src_a".to_string(),
            SourceInput::Files(vec![slot("a.json", "{\"id\":null,\"value\":10}\n")]),
        ),
        (
            "src_b".to_string(),
            SourceInput::Files(vec![slot("b.json", "{\"id\":null,\"value\":20}\n")]),
        ),
    ]);

    let (report, outputs) = run(&plan, readers, &["out"]);

    assert_eq!(report.counters.ok_count, 2);
    assert_eq!(report.counters.dlq_count, 0);
    assert_eq!(report.counters.records_written, 2);
    assert_eq!(outputs["out"].lines().skip(1).count(), 2);
}

#[test]
fn attempt_reset_reuses_compiled_plan_with_fresh_success_membership() {
    let plan = compile(
        r#"
pipeline:
  name: success_identity_attempt_reset
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: string }
        - { name: value, type: int }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: string }
        - { name: value, type: int }
  - type: merge
    name: merged
    inputs: [src_a, src_b]
  - type: sink
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#,
    );

    for attempt in 1..=2 {
        let (report, outputs) = run(&plan, two_csv_readers(), &["out"]);
        assert_eq!(report.counters.ok_count, 2, "attempt {attempt}");
        assert_eq!(report.counters.records_written, 2, "attempt {attempt}");
        assert_eq!(
            outputs["out"].lines().skip(1).count(),
            2,
            "attempt {attempt}"
        );
    }
}

fn fanout_plan(memory_limit: &str) -> CompiledPlan {
    compile(&format!(
        r#"
pipeline:
  name: delivery_identity_spill
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - {{ name: id, type: string }}
        - {{ name: payload, type: string }}
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - {{ name: id, type: string }}
        - {{ name: payload, type: string }}
  - type: merge
    name: merged
    inputs: [src_a, src_b]
  - type: route
    name: fanout
    input: merged
    config:
      mode: inclusive
      conditions:
        audit: id != ""
        report: id != ""
      default: audit
  - type: sink
    name: audit
    input: fanout
    config:
      name: audit
      type: csv
      path: audit.csv
      include_unmapped: true
  - type: sink
    name: report
    input: fanout
    config:
      name: report
      type: csv
      path: report.csv
      include_unmapped: true
"#,
    ))
}

fn large_csv(prefix: &str) -> String {
    let payload = "x".repeat(4 * 1024);
    let mut csv = String::from("id,payload\n");
    for ordinal in 1..=48 {
        csv.push_str(&format!("{prefix}{ordinal},{payload}\n"));
    }
    csv
}

fn large_readers() -> SourceReaders {
    HashMap::from([
        (
            "src_a".to_string(),
            SourceInput::Files(vec![slot("a.csv", &large_csv("a"))]),
        ),
        (
            "src_b".to_string(),
            SourceInput::Files(vec![slot("b.csv", &large_csv("b"))]),
        ),
    ])
}

#[test]
fn deliveries_match_across_resident_and_forced_spill_fanout() {
    let resident = run(&fanout_plan("1G"), large_readers(), &["audit", "report"]);
    let spilled = run(&fanout_plan("64K"), large_readers(), &["audit", "report"]);

    for (label, (report, outputs)) in [("resident", resident), ("spilled", spilled)] {
        assert_eq!(report.counters.ok_count, 96, "{label}");
        assert_eq!(report.counters.records_written, 192, "{label}");
        assert_eq!(report.counters.dlq_count, 0, "{label}");
        assert_eq!(outputs["audit"].lines().skip(1).count(), 96, "{label}");
        assert_eq!(outputs["report"].lines().skip(1).count(), 96, "{label}");
    }
}

#[test]
fn deliveries_state_retains_row_and_terminal_consumer_identity() {
    let compact = |source: &str| source.split_whitespace().collect::<String>();
    let dispatch = compact(include_str!("../src/executor/dispatch.rs"));
    let streaming = compact(include_str!("../src/executor/streaming.rs"));
    let output = compact(include_str!("../src/executor/sink_dispatch.rs"));
    let correlation = compact(include_str!("../src/executor/correlation_dispatch.rs"));

    assert!(
        dispatch.contains("ok_deliveries:HashSet<crate::executor::stream_event::OutputDeliveryId>"),
        "ExecutorContext must retain typed per-consumer delivery evidence"
    );
    for (name, source) in [
        ("streaming", &streaming),
        ("buffered", &output),
        ("correlation", &correlation),
    ] {
        assert!(
            source.contains("OutputDeliveryId::new("),
            "{name} terminal success must record row plus consumer identity"
        );
    }
    assert!(
        correlation.contains("ifctx.ok_source_rows.insert(slot.row_num)"),
        "correlation success remains row-deduplicated after delivery evidence is recorded"
    );
}
