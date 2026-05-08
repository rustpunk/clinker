//! End-to-end integration tests for scoped variables. Each test
//! drives a complete pipeline (parse → compile → execute) and
//! asserts on the emitted CSV. Producer Transforms write scoped
//! variables via `declares:`; their effects are observable only
//! through downstream readers, so each fixture pairs a producer
//! Transform with a reader Transform that emits the variable value
//! as a column.

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

fn test_params() -> PipelineRunParams {
    let pipeline_vars = indexmap::IndexMap::new();
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
    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &test_params())
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
        &test_params(),
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
  - type: transform
    name: capture
    input: src
    config:
      declares:
        - { name: last_amount, scope: pipeline, type: int }
      cxl: |
        emit id = id
        emit amount = amount
        emit $pipeline.last_amount = amount
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
  - type: transform
    name: capture
    input: src
    config:
      declares:
        - { name: batch_label, scope: source, type: string }
      cxl: |
        emit id = id
        emit label = label
        emit $source.batch_label = label
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
  - type: transform
    name: capture
    input: src
    config:
      declares:
        - { name: doubled, scope: record, type: int }
      cxl: |
        emit id = id
        emit amount = amount
        emit $record.doubled = amount * 2
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
    // Uses the existing fixture-resident composition that opts in to
    // `$pipeline.cutoff` via `_compose.scoped_vars.pipeline`. A parent
    // Transform declares `cutoff: { type: int, default: 99 }`; the
    // body reads the parent's value and emits it as `cutoff_seen`.
    use clinker_core::config::CompileContext;
    use std::path::PathBuf;

    let yaml = r#"
pipeline:
  name: state_composition_opt_in
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: int }
  - type: transform
    name: parent_writer
    input: src
    config:
      declares:
        - { name: cutoff, scope: pipeline, type: int, default: 99 }
      cxl: |
        emit id = id
  - type: composition
    name: body
    input: parent_writer
    use: ../compositions/declares_used.comp.yaml
    inputs:
      inp: parent_writer
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
    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &test_params())
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
  - type: transform
    name: tag_left
    input: left_src
    config:
      declares:
        - { name: left_label, scope: source, type: string }
      cxl: |
        emit id = id
        emit tag_val = tag_val
        emit $source.left_label = tag_val
  - type: transform
    name: tag_right
    input: right_src
    config:
      declares:
        - { name: right_label, scope: source, type: string }
      cxl: |
        emit id = id
        emit tag_val = tag_val
        emit $source.right_label = tag_val
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
  - type: transform
    name: capture_init
    input: max_agg
    config:
      declares:
        - { name: max_amount, scope: pipeline, type: int }
      phase: init
      cxl: |
        emit cap = cap
        emit $pipeline.max_amount = cap
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

// ─────────────────────────────────────────────────────────────────
// Fixture 7 — multi-file Source, source-scope state writes per-file
// ─────────────────────────────────────────────────────────────────

#[test]
fn state_source_scope_per_file_isolation() {
    // Source-scope vars are keyed by the per-record `source_file`
    // `Arc<str>`. A glob-fed Source produces records whose Arcs swap
    // at each file boundary; the producer Transform writes the var
    // into the per-file slot, and downstream reads see the per-file
    // value.
    use clinker_core::source::multi_file::FileSlot;
    use std::path::PathBuf;

    let yaml = r#"
pipeline:
  name: state_source_multi_file
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
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: transform
    name: capture
    input: orders
    config:
      declares:
        - { name: file_label, scope: source, type: string }
      cxl: |
        emit id = id
        emit tag = tag
        emit $source.file_label = tag
  - type: transform
    name: read_back
    input: capture
    config:
      cxl: |
        emit id = id
        emit label = $source.file_label
  - type: output
    name: out
    input: read_back
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile multi-file pipeline");
    let file_a = FileSlot::new(
        PathBuf::from("orders_a.csv"),
        Box::new(Cursor::new(
            "id,tag\n1,alpha\n2,alpha\n".as_bytes().to_vec(),
        )),
    );
    let file_b = FileSlot::new(
        PathBuf::from("orders_b.csv"),
        Box::new(Cursor::new("id,tag\n3,beta\n".as_bytes().to_vec())),
    );
    let readers: clinker_core::executor::SourceReaders =
        HashMap::from([("orders".to_string(), vec![file_a, file_b])]);
    let buf = SharedBuffer::default();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);
    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &test_params())
        .expect("pipeline run");
    let out = buf.as_string();
    let lines = body_lines_sorted(&out);
    // Per-file slot: rows 1 and 2 (from orders_a.csv) carry "alpha";
    // row 3 (from orders_b.csv) carries "beta". Each Arc keys its own
    // entry in `source_vars`.
    assert_eq!(lines, vec!["1,alpha", "2,alpha", "3,beta"]);
}

// ─────────────────────────────────────────────────────────────────
// Fixture 8 — $vars.<key> static config, frozen at pipeline start
// ─────────────────────────────────────────────────────────────────

#[test]
fn static_vars_visible_in_transform_cxl() {
    // The flat top-level `vars:` block declares static-config knobs
    // with defaults. The transform's CXL filters records against
    // `$vars.cutoff` and emits the value as a column. No producer
    // writes the var; it's frozen at pipeline start (channel overrides
    // would apply once at startup but this fixture has no channel).
    let yaml = r#"
pipeline:
  name: static_vars_smoke
  vars:
    cutoff: { type: int, default: 100 }
    label: { type: string, default: "tier-A" }
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
  - type: transform
    name: filter_and_tag
    input: src
    config:
      cxl: |
        filter amount > $vars.cutoff
        emit id = id
        emit tier = $vars.label
        emit threshold = $vars.cutoff
  - type: output
    name: out
    input: filter_and_tag
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let csv = "id,amount\n1,50\n2,150\n3,200\n";
    let out = run_single(yaml, csv);
    let lines = body_lines_sorted(&out);
    // Records with amount > 100 (cutoff) survive: ids 2 and 3.
    // Each emits the static label and cutoff.
    assert_eq!(lines, vec!["2,tier-A,100", "3,tier-A,100"]);
}
