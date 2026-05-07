//! End-to-end test for runtime fan-out writer routing.
//!
//! Constructs a `WriterRegistry` with `fan_out` populated by hand so the
//! dispatch path can be exercised independently of the CLI's
//! plan-driven writer setup. Records flow through a multi-file source
//! and are routed to the matching per-file writer based on the per-row
//! `$source.file` Arc.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use clinker_bench_support::io::SharedBuffer;
use clinker_core::config::{CompileContext, parse_config};
use clinker_core::executor::{PipelineExecutor, PipelineRunParams, WriterRegistry};
use clinker_core::source::multi_file::FileSlot;

const YAML: &str = r#"
pipeline:
  name: fanout_demo
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      glob: ./*.csv
      files:
        on_no_match: skip
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: float }
  - type: output
    name: out
    input: orders
    config:
      name: out
      type: csv
      path: out_{source_file}.csv
      include_unmapped: true
"#;

#[test]
fn dispatch_routes_records_to_per_file_writers() {
    let config = parse_config(YAML).unwrap();
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile fan-out pipeline");

    // Two in-memory source files. The Arcs we hand to FileSlot are the
    // exact ones the executor uses to stamp `$source.file` on each
    // record, so we can pre-build per-file writers keyed by them.
    let arc_a: Arc<str> = Arc::from("orders_a.csv");
    let arc_b: Arc<str> = Arc::from("orders_b.csv");
    let file_a = FileSlot::new(
        PathBuf::from(arc_a.as_ref()),
        Box::new(Cursor::new(
            "order_id,amount\nORD-1,100.0\nORD-2,200.0\n"
                .as_bytes()
                .to_vec(),
        )),
    );
    let file_b = FileSlot::new(
        PathBuf::from(arc_b.as_ref()),
        Box::new(Cursor::new(
            "order_id,amount\nORD-3,300.0\n".as_bytes().to_vec(),
        )),
    );
    let readers: clinker_core::executor::SourceReaders =
        HashMap::from([("orders".to_string(), vec![file_a, file_b])]);

    // Pre-build one in-memory buffer per source file. The dispatcher
    // routes records by exactly these `Arc<str>` keys (Arc-equality
    // is by value, not pointer, since the executor builds its own
    // Arcs from the FileSlot path; the keys here match by `to_string`).
    let buf_a = SharedBuffer::new();
    let buf_b = SharedBuffer::new();

    let mut fan_out: HashMap<String, HashMap<Arc<str>, Box<dyn std::io::Write + Send>>> =
        HashMap::new();
    let mut per_file: HashMap<Arc<str>, Box<dyn std::io::Write + Send>> = HashMap::new();
    per_file.insert(
        Arc::clone(&arc_a),
        Box::new(buf_a.clone()) as Box<dyn std::io::Write + Send>,
    );
    per_file.insert(
        Arc::clone(&arc_b),
        Box::new(buf_b.clone()) as Box<dyn std::io::Write + Send>,
    );
    fan_out.insert("out".to_string(), per_file);

    let writers = WriterRegistry {
        single: HashMap::new(),
        fan_out,
    };

    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("run pipeline");
    assert_eq!(report.counters.total_count, 3);

    // Per-file writer A got rows from orders_a (2 records).
    let out_a = buf_a.as_string();
    assert!(out_a.contains("ORD-1"), "buf_a missing ORD-1: {out_a}");
    assert!(out_a.contains("ORD-2"), "buf_a missing ORD-2: {out_a}");
    assert!(!out_a.contains("ORD-3"), "buf_a leaked ORD-3: {out_a}");

    // Per-file writer B got rows from orders_b (1 record).
    let out_b = buf_b.as_string();
    assert!(out_b.contains("ORD-3"), "buf_b missing ORD-3: {out_b}");
    assert!(!out_b.contains("ORD-1"), "buf_b leaked ORD-1: {out_b}");
}

#[test]
fn fan_out_flag_set_when_template_uses_source_file_token() {
    let config = parse_config(YAML).unwrap();
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile fan-out pipeline");
    let dag = plan.dag();
    let out_idx = dag
        .graph
        .node_indices()
        .find(|i| dag.graph[*i].name() == "out")
        .expect("output node 'out' exists");
    use clinker_core::plan::execution::PlanNode;
    let PlanNode::Output { resolved, .. } = &dag.graph[out_idx] else {
        panic!("expected Output variant");
    };
    let payload = resolved.as_ref().expect("Output payload populated");
    assert!(
        payload.fan_out_per_source_file,
        "Output 'out' uses {{source_file}} token + has FilePartitioned input \
         → fan_out_per_source_file must be true"
    );
}
