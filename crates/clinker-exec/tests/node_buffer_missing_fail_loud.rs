//! End-to-end regressions for materialized node-buffer reader accounting.
//!
//! The DAGs below cover producer fan-out across mixed consumers and preserve
//! Route/Cull successor-local precedence.

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
        .sink_configs()
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

fn examples_pipeline_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("examples")
        .join("pipelines")
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
  - type: sink
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
  - type: sink
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
fn transform_diamond_delivers_source_to_branch_and_combine() {
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
  - type: sink
    name: out
    input: joined
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );

    let (result, buffers) = run_pipeline(&yaml, CSV);
    let report = result.expect("Transform diamond must execute");
    assert_eq!(report.counters.records_written, 3);
    assert_csv_rows(
        &buffers["out"],
        "id,dept,flag",
        &["1,eng,seen", "2,eng,seen", "3,sales,seen"],
    );
}

#[test]
fn aggregate_diamond_delivers_source_to_branch_and_combine() {
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
  - type: sink
    name: out
    input: joined
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );

    let (result, buffers) = run_pipeline(&yaml, CSV);
    let report = result.expect("Aggregate diamond must execute");
    assert_eq!(report.counters.records_written, 3);
    assert_csv_rows(
        &buffers["out"],
        "id,dept,n",
        &["1,eng,2", "2,eng,2", "3,sales,1"],
    );
}

#[test]
fn composition_and_transform_siblings_share_the_parent_slot() {
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
  - type: sink
    name: composed_out
    input: composed
    config:
      name: composed_out
      type: csv
      path: composed.csv
  - type: sink
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

    let compiled = parse_config(yaml)
        .expect("parse composition sibling pipeline")
        .compile(&compile_context)
        .expect("compile composition sibling pipeline");
    let parent_source_idx = compiled
        .dag()
        .graph
        .node_indices()
        .find(|idx| compiled.dag().graph[*idx].name() == "src")
        .expect("parent Source exists");
    let body_id = compiled
        .dag()
        .graph
        .node_indices()
        .find_map(|idx| match &compiled.dag().graph[idx] {
            clinker_plan::plan::execution::PlanNode::Composition { name, body, .. }
                if name == "composed" =>
            {
                Some(*body)
            }
            _ => None,
        })
        .expect("composition body exists");
    let body = compiled
        .composition_bodies()
        .get(&body_id)
        .expect("bound composition body exists");
    let body_source_idx = *body
        .port_name_to_node_idx
        .get("inp")
        .expect("body input Source exists");
    assert_eq!(
        parent_source_idx, body_source_idx,
        "the regression must collide parent/body NodeIndex namespaces"
    );
    assert_eq!(
        compiled
            .dag()
            .graph
            .edges_directed(parent_source_idx, petgraph::Direction::Outgoing)
            .count(),
        2,
        "the parent source must publish for two readers"
    );
    assert_eq!(
        body.graph
            .edges_directed(body_source_idx, petgraph::Direction::Outgoing)
            .count(),
        1,
        "the colliding body source must publish for one reader"
    );

    let (result, buffers) = run_pipeline_with_context(yaml, "a\n5\n7\n", &compile_context);
    let report = result.expect("Composition and Transform siblings must both execute");
    assert_eq!(report.counters.records_written, 4);
    assert_csv_rows(&buffers["composed_out"], "a,computed", &["5,10", "7,14"]);
    assert_csv_rows(
        &buffers["sibling_out"],
        "a,branch",
        &["5,sibling", "7,sibling"],
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
  - type: sink
    name: {first}
    input: prepared
    config:
      name: {first}
      type: csv
      path: {first}.csv
  - type: sink
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

fn direct_outputs(outputs: &[(&str, bool)]) -> String {
    let output_nodes = outputs
        .iter()
        .map(|(name, reconstruct_envelope)| {
            let reconstruct = if *reconstruct_envelope {
                "\n      reconstruct_envelope: true"
            } else {
                ""
            };
            format!(
                r#"  - type: sink
    name: {name}
    input: prepared
    config:
      name: {name}
      type: csv
      path: {name}.csv{reconstruct}
"#,
            )
        })
        .collect::<String>();
    pipeline(
        "direct_output_reader_ledger",
        &format!(
            r#"  - type: transform
    name: prepared
    input: rows
    config:
      cxl: |
        emit marker = "ready"
{output_nodes}"#,
        ),
    )
}

fn assert_prepared_rows(buffers: &HashMap<String, SharedBuffer>, outputs: &[&str]) {
    for output in outputs {
        assert_csv_rows(
            &buffers[*output],
            "id,dept,amount,status,label,marker",
            &[
                "1,eng,100,keep,one,ready",
                "2,eng,200,bad,two,ready",
                "3,sales,50,keep,three,ready",
            ],
        );
    }
}

#[test]
fn two_direct_outputs_receive_full_input_in_alpha_beta_order() {
    let yaml = two_direct_outputs("alpha", "beta");
    let (result, buffers) = run_pipeline(&yaml, CSV);
    let report = result.expect("both direct Outputs must execute");
    assert_eq!(report.counters.records_written, 6);
    for output in ["alpha", "beta"] {
        assert_csv_rows(
            &buffers[output],
            "id,dept,amount,status,label,marker",
            &[
                "1,eng,100,keep,one,ready",
                "2,eng,200,bad,two,ready",
                "3,sales,50,keep,three,ready",
            ],
        );
    }
}

#[test]
fn two_direct_outputs_receive_full_input_in_beta_alpha_order() {
    let yaml = two_direct_outputs("beta", "alpha");
    let (result, buffers) = run_pipeline(&yaml, CSV);
    let report = result.expect("both direct Outputs must execute");
    assert_eq!(report.counters.records_written, 6);
    for output in ["alpha", "beta"] {
        assert_csv_rows(
            &buffers[output],
            "id,dept,amount,status,label,marker",
            &[
                "1,eng,100,keep,one,ready",
                "2,eng,200,bad,two,ready",
                "3,sales,50,keep,three,ready",
            ],
        );
    }
}

#[test]
fn three_direct_outputs_each_receive_the_full_predecessor() {
    let yaml = direct_outputs(&[("alpha", false), ("beta", false), ("gamma", false)]);
    let (result, buffers) = run_pipeline(&yaml, CSV);
    let report = result.expect("all three direct Outputs must execute");
    assert_eq!(report.counters.records_written, 9);
    assert_prepared_rows(&buffers, &["alpha", "beta", "gamma"]);
}

#[test]
fn three_direct_outputs_receive_full_input_in_reverse_order() {
    let yaml = direct_outputs(&[("gamma", false), ("beta", false), ("alpha", false)]);
    let (result, buffers) = run_pipeline(&yaml, CSV);
    let report = result.expect("all three reverse-declared Outputs must execute");
    assert_eq!(report.counters.records_written, 9);
    assert_prepared_rows(&buffers, &["alpha", "beta", "gamma"]);
}

#[test]
fn ordinary_then_reconstructed_output_each_receive_the_full_predecessor() {
    let yaml = direct_outputs(&[("ordinary", false), ("reconstructed", true)]);
    let (result, buffers) = run_pipeline(&yaml, CSV);
    let report = result.expect("ordinary and reconstructed Outputs must execute");
    assert_eq!(report.counters.records_written, 6);
    assert_prepared_rows(&buffers, &["ordinary", "reconstructed"]);
}

#[test]
fn reconstructed_then_ordinary_output_each_receive_the_full_predecessor() {
    let yaml = direct_outputs(&[("reconstructed", true), ("ordinary", false)]);
    let (result, buffers) = run_pipeline(&yaml, CSV);
    let report = result.expect("reconstructed and ordinary Outputs must execute");
    assert_eq!(report.counters.records_written, 6);
    assert_prepared_rows(&buffers, &["ordinary", "reconstructed"]);
}

#[test]
fn output_sort_and_aggregate_share_one_materialized_predecessor() {
    let yaml = pipeline(
        "mixed_reader_ledger",
        r#"  - type: transform
    name: prepared
    input: rows
    config:
      cxl: |
        emit marker = "ready"
  - type: sink
    name: direct
    input: prepared
    config:
      name: direct
      type: csv
      path: direct.csv
  - type: reshape
    name: ordered
    input: prepared
    config:
      partition_by: []
      order_by:
        - { field: amount, order: desc }
      rules:
        - name: keep
          when: "true"
  - type: sink
    name: sorted
    input: ordered
    config:
      name: sorted
      type: csv
      path: sorted.csv
  - type: aggregate
    name: summary
    input: prepared
    config:
      group_by: [dept]
      cxl: |
        emit dept = dept
        emit n = count(*)
  - type: sink
    name: summarized
    input: summary
    config:
      name: summarized
      type: csv
      path: summarized.csv
"#,
    );

    let (result, buffers) = run_pipeline(&yaml, CSV);
    let report = result.expect("mixed consumers must all execute");
    assert_eq!(report.counters.records_written, 8);
    assert_prepared_rows(&buffers, &["direct", "sorted"]);
    assert_csv_rows(&buffers["summarized"], "dept,n", &["eng,2", "sales,1"]);
}

#[test]
fn aggregate_sort_and_output_share_one_predecessor_in_reverse_order() {
    let yaml = pipeline(
        "mixed_reader_ledger_reverse",
        r#"  - type: transform
    name: prepared
    input: rows
    config:
      cxl: |
        emit marker = "ready"
  - type: aggregate
    name: summary
    input: prepared
    config:
      group_by: [dept]
      cxl: |
        emit dept = dept
        emit n = count(*)
  - type: sink
    name: summarized
    input: summary
    config:
      name: summarized
      type: csv
      path: summarized.csv
  - type: reshape
    name: ordered
    input: prepared
    config:
      partition_by: []
      order_by:
        - { field: amount, order: desc }
      rules:
        - name: keep
          when: "true"
  - type: sink
    name: sorted
    input: ordered
    config:
      name: sorted
      type: csv
      path: sorted.csv
  - type: sink
    name: direct
    input: prepared
    config:
      name: direct
      type: csv
      path: direct.csv
"#,
    );

    let (result, buffers) = run_pipeline(&yaml, CSV);
    let report = result.expect("reverse-declared mixed consumers must all execute");
    assert_eq!(report.counters.records_written, 8);
    assert_prepared_rows(&buffers, &["direct", "sorted"]);
    assert_csv_rows(&buffers["summarized"], "dept,n", &["eng,2", "sales,1"]);
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
  - type: sink
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
  - type: sink
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
  - type: sink
    name: out
    input: filter_kept
    config:
      name: out
      type: csv
      path: out.csv
  - type: sink
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
  - type: sink
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
  - type: sink
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
  - type: sink
    name: out
    input: filter_rows
    config:
      name: out
      type: csv
      path: out.csv
  - type: sink
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
  - type: sink
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

#[test]
fn shipped_order_fulfillment_delivers_all_rows_to_both_outputs() {
    let examples = examples_pipeline_dir();
    let yaml = std::fs::read_to_string(examples.join("order_fulfillment.yaml"))
        .expect("read order_fulfillment example");
    let orders = std::fs::read(examples.join("data").join("orders.csv"))
        .expect("read order_fulfillment orders");
    let products = std::fs::read(examples.join("data").join("products.csv"))
        .expect("read order_fulfillment products");
    let config = parse_config(&yaml).expect("order_fulfillment example must parse");
    let plan = config
        .compile(&CompileContext::new(&examples))
        .expect("order_fulfillment example must compile");
    let readers = HashMap::from([
        (
            "orders".to_string(),
            clinker_exec::executor::single_file_reader("orders.csv", Box::new(Cursor::new(orders))),
        ),
        (
            "products".to_string(),
            clinker_exec::executor::single_file_reader(
                "products.csv",
                Box::new(Cursor::new(products)),
            ),
        ),
    ]);
    let buffers = HashMap::from([
        ("fulfilled_orders".to_string(), SharedBuffer::new()),
        ("priority_report".to_string(), SharedBuffer::new()),
    ]);
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
        execution_id: "order-fulfillment-shared-output".to_string(),
        batch_id: "order-fulfillment-shared-output".to_string(),
        ..Default::default()
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("shipped order_fulfillment example must execute");

    assert_eq!(report.counters.records_written, 10);
    assert_eq!(report.counters.dlq_count, 0);
    assert_eq!(
        buffers["fulfilled_orders"].as_string(),
        "_route,handling_fee,region\n\
         priority_report,5.99,central\n\
         fulfilled_orders,0,central\n\
         priority_report,0,central\n\
         fulfilled_orders,0,central\n\
         fulfilled_orders,0,central\n"
    );
    let priority_report: serde_json::Value =
        serde_json::from_str(&buffers["priority_report"].as_string())
            .expect("priority_report must be valid JSON");
    assert_eq!(
        priority_report,
        serde_json::json!([
            {
                "order_id": "1",
                "order_date": "2024-01-15",
                "quantity": 5,
                "unit_price": 29.99,
                "product_code": "PROD-A",
                "priority_level": "urgent",
                "order_ref": "ORD-00000001",
                "order_date_parsed": "2024-01-15",
                "line_total": 149.95,
                "product_name": "Widget",
                "category": "tools",
                "weight_kg": "1.5",
                "_route": "priority_report",
                "handling_fee": 5.99,
                "region": "central"
            },
            {
                "order_id": "2",
                "order_date": "2024-01-16",
                "quantity": 10,
                "unit_price": 15.5,
                "product_code": "PROD-B",
                "priority_level": "normal",
                "order_ref": "ORD-00000002",
                "order_date_parsed": "2024-01-16",
                "line_total": 155.0,
                "product_name": "Gadget",
                "category": "electronics",
                "weight_kg": "0.3",
                "_route": "fulfilled_orders",
                "handling_fee": 0.0,
                "region": "central"
            },
            {
                "order_id": "3",
                "order_date": "2024-01-17",
                "quantity": 2,
                "unit_price": 99.99,
                "product_code": "PROD-X",
                "priority_level": "high",
                "order_ref": "ORD-00000003",
                "order_date_parsed": "2024-01-17",
                "line_total": 199.98,
                "_route": "priority_report",
                "handling_fee": 0.0,
                "region": "central"
            },
            {
                "order_id": "4",
                "order_date": "2024-01-18",
                "quantity": 1,
                "unit_price": 49.99,
                "product_code": "PROD-A",
                "priority_level": "low",
                "order_ref": "ORD-00000004",
                "order_date_parsed": "2024-01-18",
                "line_total": 49.99,
                "product_name": "Widget",
                "category": "tools",
                "weight_kg": "1.5",
                "_route": "fulfilled_orders",
                "handling_fee": 0.0,
                "region": "central"
            },
            {
                "order_id": "5",
                "order_date": "2024-01-19",
                "quantity": 8,
                "unit_price": 12.0,
                "product_code": "PROD-B",
                "priority_level": "normal",
                "order_ref": "ORD-00000005",
                "order_date_parsed": "2024-01-19",
                "line_total": 96.0,
                "product_name": "Gadget",
                "category": "electronics",
                "weight_kg": "0.3",
                "_route": "fulfilled_orders",
                "handling_fee": 0.0,
                "region": "central"
            }
        ])
    );
}
