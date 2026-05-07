//! End-to-end integration tests for scoped variables and the
//! `state` node. Each test drives a complete pipeline (parse → compile
//! → execute) and asserts on the emitted CSV. Side effects of the
//! state node are observable only through downstream readers, so each
//! fixture pairs a state-node writer with a reader transform that
//! emits the variable value as a column.

use std::collections::HashMap;
use std::io::{self, Cursor, Write};
use std::sync::{Arc, Mutex};

use clinker_core::config::{CompileContext, parse_config};
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};

#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
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

fn test_params(config: &clinker_core::config::PipelineConfig) -> PipelineRunParams {
    let pipeline_vars = config
        .pipeline
        .vars
        .as_ref()
        .map(clinker_core::config::convert_pipeline_vars)
        .unwrap_or_default();
    PipelineRunParams {
        execution_id: "phase-h-test".to_string(),
        batch_id: "batch-h-001".to_string(),
        pipeline_vars,
        shutdown_token: None,
    }
}

fn run_single(yaml: &str, csv_input: &str) -> String {
    let config = parse_config(yaml).expect("parse_config");
    let plan = clinker_core::config::PipelineConfig::compile(&config, &CompileContext::default())
        .expect("compile");
    let readers = HashMap::from([(
        config.source_configs().next().unwrap().name.clone(),
        clinker_core::executor::single_file_reader(
            "test.csv",
            Box::new(Cursor::new(csv_input.as_bytes().to_vec())),
        ),
    )]);
    let buf = SharedBuffer::default();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);
    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &test_params(&config))
        .expect("pipeline run");
    buf.as_string()
}

fn run_multi(yaml: &str, primary: &str, inputs: &[(&str, &str)]) -> String {
    let config = parse_config(yaml).expect("parse_config");
    let plan = clinker_core::config::PipelineConfig::compile(&config, &CompileContext::default())
        .expect("compile");
    let mut readers: clinker_core::executor::SourceReaders = HashMap::new();
    for (name, data) in inputs {
        readers.insert(
            (*name).to_string(),
            clinker_core::executor::single_file_reader(
                "test.csv",
                Box::new(Cursor::new(data.as_bytes().to_vec())),
            ),
        );
    }
    let buf = SharedBuffer::default();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);
    PipelineExecutor::run_plan_with_readers_writers_with_primary(
        &plan,
        primary,
        readers,
        writers,
        &test_params(&config),
    )
    .expect("pipeline run");
    buf.as_string()
}

fn body_lines_sorted(out: &str) -> Vec<String> {
    let mut v: Vec<String> = out.lines().skip(1).map(|s| s.to_string()).collect();
    v.sort();
    v
}

// ─────────────────────────────────────────────────────────────────
// Fixture 1 — pipeline-scope runtime state, reader downstream
// ─────────────────────────────────────────────────────────────────

#[test]
fn state_pipeline_runtime_visible_downstream() {
    let yaml = r#"
pipeline:
  name: state_pipeline_runtime
  vars:
    pipeline:
      last_amount:
        type: int
        default: 0
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: orders.csv
      schema:
        - { name: id, type: int }
        - { name: amount, type: int }
  - type: state
    name: capture
    input: src
    config:
      scope: pipeline
      set:
        - var: last_amount
          cxl: "amount"
  - type: transform
    name: read_back
    input: capture
    config:
      cxl: |
        emit id = id
        emit pipeline_last = $pipeline.last_amount
  - type: output
    name: out
    input: read_back
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let csv = "id,amount\n1,100\n2,200\n3,300\n";
    let out = run_single(yaml, csv);
    let lines = body_lines_sorted(&out);
    assert_eq!(
        lines.len(),
        3,
        "expected 3 output rows, got {lines:?}: {out}"
    );
    for line in &lines {
        let last = line.split(',').next_back().unwrap();
        assert!(
            ["100", "200", "300"].contains(&last),
            "unexpected pipeline_last value in {line:?}"
        );
    }
}

// ─────────────────────────────────────────────────────────────────
// Fixture 2 — source-scope state, reader downstream
// ─────────────────────────────────────────────────────────────────

#[test]
fn state_source_scope_visible_downstream() {
    let yaml = r#"
pipeline:
  name: state_source_scope
  vars:
    source:
      batch_label:
        type: string
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: orders.csv
      schema:
        - { name: id, type: int }
        - { name: label, type: string }
  - type: state
    name: capture
    input: src
    config:
      scope: source
      set:
        - var: batch_label
          cxl: "label"
  - type: transform
    name: read_back
    input: capture
    config:
      cxl: |
        emit id = id
        emit src_label = $source.batch_label
  - type: output
    name: out
    input: read_back
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let csv = "id,label\n1,alpha\n2,beta\n3,gamma\n";
    let out = run_single(yaml, csv);
    let lines = body_lines_sorted(&out);
    assert_eq!(lines.len(), 3, "expected 3 output rows: {out}");
    for line in &lines {
        let last = line.split(',').next_back().unwrap();
        assert!(
            ["alpha", "beta", "gamma"].contains(&last),
            "unexpected src_label in {line:?}"
        );
    }
}

// ─────────────────────────────────────────────────────────────────
// Fixture 3 — record-scope state, reader downstream, never sinks
// ─────────────────────────────────────────────────────────────────

#[test]
fn state_record_scope_visible_downstream_does_not_serialize() {
    let yaml = r#"
pipeline:
  name: state_record_scope
  vars:
    record:
      doubled:
        type: int
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: int }
        - { name: amount, type: int }
  - type: state
    name: capture
    input: src
    config:
      scope: record
      set:
        - var: doubled
          cxl: "amount * 2"
  - type: transform
    name: read_back
    input: capture
    config:
      cxl: |
        emit id = id
        emit derived = $record.doubled
  - type: output
    name: out
    input: read_back
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let csv = "id,amount\n1,5\n2,10\n3,15\n";
    let out = run_single(yaml, csv);
    let lines = body_lines_sorted(&out);
    assert_eq!(lines, vec!["1,10", "2,20", "3,30"]);
    let header = out.lines().next().unwrap();
    assert_eq!(header, "id,derived");
    assert!(
        !out.contains("doubled"),
        "$record.doubled must not leak as a column name in {out:?}"
    );
}

// ─────────────────────────────────────────────────────────────────
// Fixture 4 — composition opt-in via _compose.scoped_vars
// ─────────────────────────────────────────────────────────────────

#[test]
fn state_composition_opt_in_pipeline_var_visible_in_body() {
    // Uses the existing fixture-resident composition that opts in
    // to `$pipeline.cutoff` via `_compose.scoped_vars.pipeline`. The
    // parent declares `cutoff: { type: int, default: 99 }`; the body
    // reads the parent's value and emits it as `cutoff_seen`.
    use clinker_core::config::CompileContext;
    use std::path::PathBuf;

    let yaml = r#"
pipeline:
  name: state_composition_opt_in
  vars:
    pipeline:
      cutoff:
        type: int
        default: 99
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: int }
  - type: composition
    name: body
    input: src
    use: ../compositions/state_uses_declared.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: body
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let config = parse_config(yaml).expect("parse_config");
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures");
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));
    let plan = config.compile(&ctx).expect("compile pipeline");
    let csv = "id\n1\n2\n";
    let readers = HashMap::from([(
        config.source_configs().next().unwrap().name.clone(),
        clinker_core::executor::single_file_reader(
            "test.csv",
            Box::new(Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]);
    let buf = SharedBuffer::default();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);
    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &test_params(&config))
        .expect("pipeline run");
    let out = buf.as_string();
    let lines = body_lines_sorted(&out);
    assert_eq!(lines, vec!["1,99", "2,99"]);
}

// ─────────────────────────────────────────────────────────────────
// Fixture 5 — qualified post-merge $source.<input>.<key> read
// ─────────────────────────────────────────────────────────────────

#[test]
fn state_qualified_post_merge_readable() {
    // Each input has its own writer + var (E170 single-writer rule
    // applies per-(scope, var)). Post-merge reads the value via the
    // qualified form `$source.<input_name>.<field>` — the unqualified
    // form would be rejected by E172 here.
    let yaml = r#"
pipeline:
  name: state_qualified_post_merge
  vars:
    source:
      left_label:
        type: string
      right_label:
        type: string
nodes:
  - type: source
    name: left_src
    config:
      name: left_src
      type: csv
      path: left.csv
      schema:
        - { name: id, type: int }
        - { name: tag_val, type: string }
  - type: source
    name: right_src
    config:
      name: right_src
      type: csv
      path: right.csv
      schema:
        - { name: id, type: int }
        - { name: tag_val, type: string }
  - type: state
    name: tag_left
    input: left_src
    config:
      scope: source
      set:
        - var: left_label
          cxl: "tag_val"
  - type: state
    name: tag_right
    input: right_src
    config:
      scope: source
      set:
        - var: right_label
          cxl: "tag_val"
  - type: merge
    name: merged
    inputs: [tag_left, tag_right]
  - type: transform
    name: read_back
    input: merged
    config:
      cxl: |
        emit id = id
        emit lt = $source.tag_left.left_label
        emit rt = $source.tag_right.right_label
  - type: output
    name: out
    input: read_back
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let left = "id,tag_val\n1,L1\n2,L2\n";
    let right = "id,tag_val\n10,R1\n20,R2\n";
    let out = run_multi(
        yaml,
        "left_src",
        &[("left_src", left), ("right_src", right)],
    );
    let header = out.lines().next().unwrap();
    assert_eq!(header, "id,lt,rt");
    let lines = body_lines_sorted(&out);
    assert_eq!(
        lines.len(),
        4,
        "expected 4 merged rows, got {lines:?}: {out}"
    );
    for line in &lines {
        let cols: Vec<&str> = line.split(',').collect();
        assert!(
            cols[1].starts_with('L') || cols[1].is_empty(),
            "lt should be L* or empty: {line}"
        );
        assert!(
            cols[2].starts_with('R') || cols[2].is_empty(),
            "rt should be R* or empty: {line}"
        );
    }
}

// ─────────────────────────────────────────────────────────────────
// Fixture 6 — pipeline-scope init, reader observes init-time write
// ─────────────────────────────────────────────────────────────────

#[test]
fn state_pipeline_init_visible_in_runtime() {
    // Disjoint init / runtime Sources: the init-phase aggregate
    // computes `max(cutoff)` from `config_src` and writes
    // `$pipeline.max_amount`; the runtime walk reads from
    // `orders_src`. partitions the topo order into init
    // and runtime sub-DAGs, so a Source shared between phases would
    // run only in pass 1; disjoint sources avoid that.
    let yaml = r#"
pipeline:
  name: state_pipeline_init
  vars:
    pipeline:
      max_amount:
        type: int
        default: 0
nodes:
  - type: source
    name: config_src
    config:
      name: config_src
      type: csv
      path: config.csv
      schema:
        - { name: cutoff, type: int }
  - type: aggregate
    name: max_agg
    input: config_src
    config:
      group_by: []
      cxl: |
        emit cap = max(cutoff)
  - type: state
    name: capture_init
    input: max_agg
    config:
      scope: pipeline
      phase: init
      set:
        - var: max_amount
          cxl: "cap"
  - type: source
    name: orders_src
    config:
      name: orders_src
      type: csv
      path: orders.csv
      schema:
        - { name: id, type: int }
        - { name: amount, type: int }
  - type: transform
    name: read_back
    input: orders_src
    config:
      cxl: |
        emit id = id
        emit cap = $pipeline.max_amount
  - type: output
    name: out
    input: read_back
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config_csv = "cutoff\n5\n9\n3\n";
    let orders_csv = "id,amount\n1,100\n2,200\n3,300\n";
    let out = run_multi(
        yaml,
        "orders_src",
        &[("orders_src", orders_csv), ("config_src", config_csv)],
    );
    let lines = body_lines_sorted(&out);
    assert_eq!(lines, vec!["1,9", "2,9", "3,9"]);
}
