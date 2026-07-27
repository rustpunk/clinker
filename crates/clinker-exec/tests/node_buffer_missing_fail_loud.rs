//! End-to-end regressions for required materialized node-buffer inputs (#1029).
//!
//! The DAGs below cover the addressing/reader-count gaps tracked by #908,
//! #996, and #1013. Route/Cull successor-local slots now execute through the
//! uniform own-slot-first lookup. The remaining #908 and #996 shapes stay here
//! to guarantee that a missing planned slot aborts instead of becoming a
//! successful empty stream.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams};
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::error::PipelineError;

const CSV: &str = "id,dept,amount,status,label\n\
                   1,eng,100,keep,one\n\
                   2,eng,200,bad,two\n\
                   3,sales,50,keep,three\n";

fn pipeline(name: &str, body: &str) -> String {
    format!(
        r#"pipeline:
  name: {name}
error_handling:
  strategy: continue
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: input.csv
      schema:
        - {{ name: id, type: string }}
        - {{ name: dept, type: string }}
        - {{ name: amount, type: int }}
        - {{ name: status, type: string }}
        - {{ name: label, type: string }}
{body}
"#,
    )
}

fn run_pipeline(
    yaml: &str,
    csv: &str,
) -> (
    Result<ExecutionReport, PipelineError>,
    HashMap<String, SharedBuffer>,
) {
    run_pipeline_with_context(yaml, csv, &CompileContext::default())
}

fn run_pipeline_with_context(
    yaml: &str,
    csv: &str,
    compile_context: &CompileContext,
) -> (
    Result<ExecutionReport, PipelineError>,
    HashMap<String, SharedBuffer>,
) {
    let config = parse_config(yaml).expect("fixture pipeline must parse");
    let plan = config
        .compile(compile_context)
        .expect("fixture pipeline must compile");
    let source_name = config
        .source_configs()
        .next()
        .expect("fixture pipeline has one source")
        .name
        .clone();
    let readers = HashMap::from([(
        source_name,
        clinker_exec::executor::single_file_reader(
            "input.csv",
            Box::new(Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]);
    let buffers: HashMap<String, SharedBuffer> = config
        .output_configs()
        .map(|output| (output.name.clone(), SharedBuffer::new()))
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
    let params = PipelineRunParams {
        execution_id: "node-buffer-fail-loud".to_string(),
        batch_id: "node-buffer-fail-loud".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    (
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params),
        buffers,
    )
}

fn assert_missing_input(
    result: Result<ExecutionReport, PipelineError>,
    consumer: &str,
    producer: &str,
    port: Option<&str>,
) {
    let error = result.expect_err("a missing planned input must stop the run");
    let PipelineError::Internal { node, detail, .. } = error else {
        panic!("missing planned input must be PipelineError::Internal, got {error:?}");
    };

    assert_eq!(node, consumer, "the error must name the consuming node");
    assert!(
        detail.contains(producer),
        "the error must name producer {producer:?}: {detail}"
    );
    if let Some(port) = port {
        assert!(
            detail.contains(port),
            "the error must name producer port {port:?}: {detail}"
        );
    }
    assert!(
        detail.contains("run stopped instead of treating it as empty"),
        "the error must explain the fail-closed disposition: {detail}"
    );
}

fn assert_csv_rows(buffer: &SharedBuffer, expected_header: &str, expected_rows: &[&str]) {
    let output = buffer.as_string();
    let mut lines = output.lines();
    assert_eq!(lines.next(), Some(expected_header));
    let mut actual: Vec<&str> = lines.collect();
    let mut expected = expected_rows.to_vec();
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

fn route_pipeline(name: &str, selected_consumer: &str) -> String {
    pipeline(
        name,
        &format!(
            r#"  - type: route
    name: split
    input: rows
    config:
      mode: exclusive
      conditions:
        keep: "status == 'keep'"
      default: drop
{selected_consumer}
  - type: output
    name: dropped
    input: split.drop
    config:
      name: dropped
      type: csv
      path: dropped.csv
"#,
        ),
    )
}

fn cull_pipeline(name: &str, main_consumer: &str) -> String {
    pipeline(
        name,
        &format!(
            r#"  - type: cull
    name: gate
    input: rows
    config:
      partition_by: [id]
      removed_to: removed
      rules:
        - name: remove_bad
          drop_group_when: "sum(if status == 'bad' then 1 else 0) > 0"
{main_consumer}
  - type: output
    name: removed
    input: gate.removed
    config:
      name: removed
      type: csv
      path: removed.csv
"#,
        ),
    )
}

#[test]
fn transform_diamond_stops_when_combine_direct_source_input_was_consumed() {
    let yaml = pipeline(
        "transform_diamond_missing_input",
        r#"  - type: transform
    name: flagged
    input: rows
    config:
      cxl: |
        emit flag = "seen"
  - type: combine
    name: joined
    input:
      detail: rows
      extra: flagged
    config:
      drive: detail
      where: "detail.id == extra.id"
      match: first
      on_miss: null_fields
      cxl: |
        emit id = detail.id
        emit dept = detail.dept
        emit flag = extra.flag
      propagate_ck: driver
  - type: output
    name: out
    input: joined
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );

    assert_missing_input(run_pipeline(&yaml, CSV).0, "joined", "rows", None);
}

#[test]
fn aggregate_diamond_stops_when_combine_direct_source_input_was_consumed() {
    let yaml = pipeline(
        "aggregate_diamond_missing_input",
        r#"  - type: aggregate
    name: summary
    input: rows
    config:
      group_by: [dept]
      cxl: |
        emit dept = dept
        emit n = count(*)
  - type: combine
    name: joined
    input:
      detail: rows
      summary: summary
    config:
      drive: detail
      where: "detail.dept == summary.dept"
      match: first
      on_miss: null_fields
      cxl: |
        emit id = detail.id
        emit dept = detail.dept
        emit n = summary.n
      propagate_ck: driver
  - type: output
    name: out
    input: joined
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );

    assert_missing_input(run_pipeline(&yaml, CSV).0, "joined", "rows", None);
}

#[test]
fn composition_input_stops_when_its_parent_slot_was_consumed() {
    let yaml = r#"pipeline:
  name: composition_missing_parent_input
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: input.csv
      schema:
        - { name: a, type: int }
  - type: composition
    name: composed
    input: src
    use: ../compositions/exec_transform_check.comp.yaml
    inputs:
      inp: src
  - type: transform
    name: sibling
    input: src
    config:
      cxl: |
        emit branch = "sibling"
  - type: output
    name: composed_out
    input: composed
    config:
      name: composed_out
      type: csv
      path: composed.csv
  - type: output
    name: sibling_out
    input: sibling
    config:
      name: sibling_out
      type: csv
      path: sibling.csv
"#;
    let fixture_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures");
    let compile_context =
        CompileContext::with_pipeline_dir(&fixture_root, PathBuf::from("pipelines"));

    assert_missing_input(
        run_pipeline_with_context(yaml, "a\n5\n7\n", &compile_context).0,
        "composed",
        "src",
        None,
    );
}

fn two_direct_outputs(first: &str, second: &str) -> String {
    pipeline(
        "two_direct_outputs_missing_input",
        &format!(
            r#"  - type: transform
    name: prepared
    input: rows
    config:
      cxl: |
        emit marker = "ready"
  - type: output
    name: {first}
    input: prepared
    config:
      name: {first}
      type: csv
      path: {first}.csv
  - type: output
    name: {second}
    input: prepared
    config:
      name: {second}
      type: csv
      path: {second}.csv
"#,
        ),
    )
}

#[test]
fn first_of_two_direct_outputs_fails_loudly_in_alpha_beta_order() {
    let yaml = two_direct_outputs("alpha", "beta");

    assert_missing_input(run_pipeline(&yaml, CSV).0, "alpha", "prepared", None);
}

#[test]
fn first_of_two_direct_outputs_fails_loudly_in_beta_alpha_order() {
    let yaml = two_direct_outputs("beta", "alpha");

    assert_missing_input(run_pipeline(&yaml, CSV).0, "beta", "prepared", None);
}

#[test]
fn route_to_aggregate_delivers_the_selected_branch() {
    let yaml = route_pipeline(
        "route_to_aggregate_missing_input",
        r#"  - type: aggregate
    name: summarize
    input: split.keep
    config:
      group_by: [dept]
      cxl: |
        emit dept = dept
        emit n = count(*)
  - type: output
    name: out
    input: summarize
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );

    let (result, buffers) = run_pipeline(&yaml, CSV);
    let report = result.expect("Route successor slot must feed Aggregate");
    assert_eq!(report.counters.records_written, 3);
    assert_csv_rows(&buffers["out"], "dept,n", &["eng,1", "sales,1"]);
    assert_csv_rows(
        &buffers["dropped"],
        "id,dept,amount,status,label",
        &["2,eng,200,bad,two"],
    );
}

#[test]
fn route_to_reshape_delivers_the_selected_branch() {
    let yaml = route_pipeline(
        "route_to_reshape_missing_input",
        r#"  - type: reshape
    name: relabel
    input: split.keep
    config:
      partition_by: [id]
      rules:
        - name: mark
          when: "true"
          mutate:
            set:
              label: "'reshaped'"
  - type: output
    name: out
    input: relabel
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );

    let (result, buffers) = run_pipeline(&yaml, CSV);
    let report = result.expect("Route successor slot must feed Reshape");
    assert_eq!(report.counters.records_written, 3);
    assert_csv_rows(
        &buffers["out"],
        "id,dept,amount,status,label",
        &["1,eng,100,keep,reshaped", "3,sales,50,keep,reshaped"],
    );
    assert_csv_rows(
        &buffers["dropped"],
        "id,dept,amount,status,label",
        &["2,eng,200,bad,two"],
    );
}

#[test]
fn route_to_cull_delivers_the_selected_branch() {
    let yaml = route_pipeline(
        "route_to_cull_missing_input",
        r#"  - type: cull
    name: filter_kept
    input: split.keep
    config:
      partition_by: [id]
      removed_to: removed
      rules:
        - name: remove_bad
          drop_group_when: "sum(if status == 'bad' then 1 else 0) > 0"
  - type: output
    name: out
    input: filter_kept
    config:
      name: out
      type: csv
      path: out.csv
  - type: output
    name: filtered
    input: filter_kept.removed
    config:
      name: filtered
      type: csv
      path: filtered.csv
"#,
    );

    let (result, buffers) = run_pipeline(&yaml, CSV);
    let report = result.expect("Route successor slot must feed Cull");
    assert_eq!(report.counters.records_written, 3);
    assert_csv_rows(
        &buffers["out"],
        "id,dept,amount,status,label",
        &["1,eng,100,keep,one", "3,sales,50,keep,three"],
    );
    assert!(buffers["filtered"].as_string().is_empty());
    assert_csv_rows(
        &buffers["dropped"],
        "id,dept,amount,status,label",
        &["2,eng,200,bad,two"],
    );
}

#[test]
fn cull_to_aggregate_delivers_main_and_removed_ports() {
    let yaml = cull_pipeline(
        "cull_to_aggregate_missing_input",
        r#"  - type: aggregate
    name: summarize
    input: gate
    config:
      group_by: [dept]
      cxl: |
        emit dept = dept
        emit n = count(*)
  - type: output
    name: out
    input: summarize
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );

    let (result, buffers) = run_pipeline(&yaml, CSV);
    let report = result.expect("Cull successor slot must feed Aggregate");
    assert_eq!(report.counters.records_written, 3);
    assert_csv_rows(&buffers["out"], "dept,n", &["eng,1", "sales,1"]);
    assert_csv_rows(
        &buffers["removed"],
        "id,dept,amount,status,label",
        &["2,eng,200,bad,two"],
    );
}

#[test]
fn cull_to_reshape_delivers_main_and_removed_ports() {
    let yaml = cull_pipeline(
        "cull_to_reshape_missing_input",
        r#"  - type: reshape
    name: relabel
    input: gate
    config:
      partition_by: [id]
      rules:
        - name: mark
          when: "true"
          mutate:
            set:
              label: "'reshaped'"
  - type: output
    name: out
    input: relabel
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );

    let (result, buffers) = run_pipeline(&yaml, CSV);
    let report = result.expect("Cull successor slot must feed Reshape");
    assert_eq!(report.counters.records_written, 3);
    assert_csv_rows(
        &buffers["out"],
        "id,dept,amount,status,label",
        &["1,eng,100,keep,reshaped", "3,sales,50,keep,reshaped"],
    );
    assert_csv_rows(
        &buffers["removed"],
        "id,dept,amount,status,label",
        &["2,eng,200,bad,two"],
    );
}

#[test]
fn a_present_empty_materialized_buffer_remains_a_valid_input() {
    let yaml = pipeline(
        "present_empty_buffer",
        r#"  - type: cull
    name: filter_rows
    input: rows
    config:
      partition_by: [id]
      removed_to: removed
      rules:
        - name: remove_bad
          drop_group_when: "sum(if status == 'bad' then 1 else 0) > 0"
  - type: output
    name: out
    input: filter_rows
    config:
      name: out
      type: csv
      path: out.csv
  - type: output
    name: removed
    input: filter_rows.removed
    config:
      name: removed
      type: csv
      path: removed.csv
"#,
    );

    let (result, _) = run_pipeline(&yaml, "id,dept,amount,status,label\n");
    let report = result.expect("an occupied zero-row buffer is not missing");
    assert_eq!(report.counters.total_count, 0);
    assert_eq!(report.counters.records_written, 0);
}

#[test]
fn fused_transform_and_streaming_output_control_still_succeeds() {
    let yaml = pipeline(
        "fused_transform_streaming_output_control",
        r#"  - type: transform
    name: prepared
    input: rows
    config:
      cxl: |
        emit marker = "ready"
  - type: output
    name: out
    input: prepared
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );

    let (result, buffers) = run_pipeline(&yaml, CSV);
    let report = result.expect("certified fused/streaming paths bypass node buffers");
    assert_eq!(report.counters.records_written, 3);
    assert!(buffers["out"].as_string().contains("ready"));
}
