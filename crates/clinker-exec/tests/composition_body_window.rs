//! Composition body analytic-window integration suite.
//!
//! Body lowering happens before the parent DAG is built; the body-window
//! resolution pass runs post-Stage-5 and binds each body window to a
//! body-local `PlanIndexRoot::Node` plus an exact scope/window/root key.

use std::collections::HashMap;
use std::io::{self, Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use clinker_core_types::diagnostic::is_registered;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::plan::index::PlanIndexRoot;

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

fn test_params() -> PipelineRunParams {
    let pipeline_vars = indexmap::IndexMap::new();
    PipelineRunParams {
        execution_id: "body-window-test".to_string(),
        batch_id: "batch".to_string(),
        pipeline_vars,
        shutdown_token: None,
        ..Default::default()
    }
}

fn write_fixture(path: &Path, contents: &str) {
    std::fs::create_dir_all(path.parent().expect("fixture parent")).expect("create fixture dir");
    std::fs::write(path, contents).expect("write fixture");
}

const INNER_WINDOW_COMPOSITION: &str = r#"
_compose:
  name: inner
  inputs:
    inp:
      schema:
        - { name: department, type: string }
        - { name: amount, type: int }
  outputs:
    out: running
  config_schema: {}
nodes:
  - type: transform
    name: running
    input: inp
    config:
      analytic_window:
        group_by: [department]
      cxl: |
        emit department = department
        emit amount = amount
        emit window_total = $window.sum(amount)
"#;

fn run_csv_pipeline(root: &Path, yaml: &str, csv: &str) -> String {
    let config = parse_config(yaml).expect("parse");
    let ctx = CompileContext::with_pipeline_dir(root, PathBuf::from("pipelines"));
    let plan = config.compile(&ctx).expect("compile");
    let readers = HashMap::from([(
        config.source_configs().next().unwrap().name.clone(),
        clinker_exec::executor::single_file_reader(
            "test.csv",
            Box::new(Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);
    PipelineExecutor::run_plan_with_readers_writers_in_context(
        &plan,
        readers,
        writers,
        &test_params(),
        CompileContext::with_pipeline_dir(root, PathBuf::from("pipelines")),
    )
    .expect("pipeline must run");
    buf.as_string()
}

fn compile_error_codes(root: &Path, yaml: &str) -> Vec<String> {
    let config = parse_config(yaml).expect("parse");
    let ctx = CompileContext::with_pipeline_dir(root, PathBuf::from("pipelines"));
    config
        .compile(&ctx)
        .expect_err("fixture must be rejected during planning")
        .into_iter()
        .map(|diagnostic| diagnostic.code)
        .collect()
}

/// Compile a pipeline whose composition body contains
/// `Aggregate → Transform(window)`. The body's IndexSpec must be
/// rooted at the body's Aggregate (`PlanIndexRoot::Node`) and live
/// in the body's `body_indices_to_build`, NOT in the parent DAG's
/// top-level indices list.
#[test]
fn compiled_body_post_aggregate_window_roots_at_body_node() {
    let yaml = r#"
pipeline:
  name: body_post_aggregate_window
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: department, type: string }
      - { name: amount, type: int }
- type: composition
  name: body
  input: src
  use: ../compositions/body_post_aggregate_window.comp.yaml
  inputs:
    inp: src
- type: sink
  name: out
  input: body
  config:
    name: out
    type: csv
    path: out.csv
    include_unmapped: true
"#;
    let config = parse_config(yaml).expect("parse");
    let root = fixture_workspace_root();
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));
    let plan = config.compile(&ctx).expect("compile must succeed");

    // The top-level DAG must NOT have any indices_to_build entry for
    // a body-internal window.
    let dag = plan.dag();
    assert!(
        dag.indices_to_build.is_empty(),
        "body window's IndexSpec must NOT leak to the top-level DAG; \
         got {:?}",
        dag.indices_to_build
    );

    // The body's BoundBody must carry a single IndexSpec rooted at
    // the body's Aggregate node.
    let bodies: Vec<_> = plan.composition_bodies().values().collect();
    assert_eq!(bodies.len(), 1, "exactly one composition body");
    let body = bodies[0];
    assert_eq!(
        body.body_indices_to_build.len(),
        1,
        "body must carry exactly one IndexSpec for its windowed Transform"
    );
    let spec = &body.body_indices_to_build[0];
    let body_agg_idx = body
        .name_to_idx
        .iter()
        .find(|(name, _)| name.as_str() == "dept_totals")
        .map(|(_, idx)| *idx)
        .expect("body must have dept_totals node");
    let PlanIndexRoot::Node { upstream, .. } = &spec.root;
    assert_eq!(
        *upstream, body_agg_idx,
        "body IndexSpec must root at the body's Aggregate"
    );
}

/// A body window whose first non-pass-through ancestor is the body's
/// synthetic `inp:` Source. The IndexSpec remains local to that body's
/// node-id space; runtime lookup must not escape to a parent slot.
#[test]
fn compiled_body_window_through_input_port_roots_at_local_node() {
    let yaml = r#"
pipeline:
  name: body_parent_node
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: department, type: string }
      - { name: amount, type: int }
- type: composition
  name: body
  input: src
  use: ../compositions/body_parent_node_window.comp.yaml
  inputs:
    inp: src
- type: sink
  name: out
  input: body
  config:
    name: out
    type: csv
    path: out.csv
    include_unmapped: true
"#;
    let config = parse_config(yaml).expect("parse");
    let root = fixture_workspace_root();
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));
    let plan = config.compile(&ctx).expect("compile must succeed");

    let bodies: Vec<_> = plan.composition_bodies().values().collect();
    assert_eq!(bodies.len(), 1, "one body");
    let body = bodies[0];
    assert_eq!(body.body_indices_to_build.len(), 1, "one body IndexSpec");
    let spec = &body.body_indices_to_build[0];
    let local_input_idx = body
        .name_to_idx
        .get("inp")
        .copied()
        .expect("body carries a synthetic input Source");
    let PlanIndexRoot::Node { upstream, .. } = &spec.root;
    assert_eq!(
        *upstream, local_input_idx,
        "body input windows must root at the body's synthetic input Source"
    );
}

#[test]
fn compiled_nested_body_window_is_bound_in_its_own_scope() {
    let temp = tempfile::tempdir().expect("temp workspace");
    let root = temp.path();
    write_fixture(
        &root.join("compositions/inner.comp.yaml"),
        r#"
_compose:
  name: inner
  inputs:
    inp:
      schema:
        - { name: department, type: string }
        - { name: amount, type: int }
  outputs:
    out: running
  config_schema: {}
nodes:
  - type: transform
    name: running
    input: inp
    config:
      analytic_window:
        group_by: [department]
      cxl: |
        emit department = department
        emit amount = amount
        emit running_total = $window.sum(amount)
"#,
    );
    write_fixture(
        &root.join("compositions/outer.comp.yaml"),
        r#"
_compose:
  name: outer
  inputs:
    inp:
      schema:
        - { name: department, type: string }
        - { name: amount, type: int }
  outputs:
    out: inner.out
  config_schema: {}
nodes:
  - type: composition
    name: inner
    input: inp
    use: ./inner.comp.yaml
    inputs:
      inp: inp
"#,
    );

    let yaml = r#"
pipeline:
  name: nested_body_window
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: department, type: string }
      - { name: amount, type: int }
- type: composition
  name: outer
  input: src
  use: ../compositions/outer.comp.yaml
  inputs:
    inp: src
- type: sink
  name: out
  input: outer
  config:
    name: out
    type: csv
    path: out.csv
    include_unmapped: true
"#;
    let config = parse_config(yaml).expect("parse");
    let ctx = CompileContext::with_pipeline_dir(root, PathBuf::from("pipelines"));
    let plan = config.compile(&ctx).expect("nested body must compile");
    let inner = plan
        .composition_bodies()
        .values()
        .find(|body| body.signature_path.ends_with("inner.comp.yaml"))
        .expect("inner body");
    assert_eq!(
        inner.body_indices_to_build.len(),
        1,
        "the recursive binder must visit the nested body"
    );
    let local_input_idx = inner
        .name_to_idx
        .get("inp")
        .copied()
        .expect("inner synthetic input Source");
    assert!(matches!(
        inner.body_indices_to_build[0].root,
        PlanIndexRoot::Node { upstream, .. } if upstream == local_input_idx
    ));
    let binding = inner
        .window_bindings
        .values()
        .next()
        .copied()
        .expect("inner window binding");
    assert_eq!(binding.key.body_scope, inner.body_scope);
    assert_eq!(binding.key.input_root, inner.graph[local_input_idx].id());
    assert_eq!(binding.index, 0);
    let encoded = serde_json::to_string(&binding.key).expect("serialize runtime key");
    let decoded = serde_json::from_str(&encoded).expect("deserialize runtime key");
    assert_eq!(
        binding.key, decoded,
        "runtime key must survive serde roundtrip"
    );
}

#[test]
fn compiled_sibling_body_instances_have_distinct_runtime_scopes() {
    let temp = tempfile::tempdir().expect("temp workspace");
    let root = temp.path();
    write_fixture(
        &root.join("compositions/inner.comp.yaml"),
        r#"
_compose:
  name: inner
  inputs:
    inp:
      schema:
        - { name: department, type: string }
        - { name: amount, type: int }
  outputs:
    out: running
  config_schema: {}
nodes:
  - type: transform
    name: running
    input: inp
    config:
      analytic_window:
        group_by: [department]
      cxl: |
        emit department = department
        emit amount = amount
        emit running_total = $window.sum(amount)
"#,
    );
    write_fixture(
        &root.join("compositions/outer.comp.yaml"),
        r#"
_compose:
  name: outer
  inputs:
    inp:
      schema:
        - { name: department, type: string }
        - { name: amount, type: int }
  outputs:
    out: left.out
  config_schema: {}
nodes:
  - type: composition
    name: left
    input: inp
    use: ./inner.comp.yaml
    inputs: { inp: inp }
  - type: composition
    name: right
    input: inp
    use: ./inner.comp.yaml
    inputs: { inp: inp }
"#,
    );
    let yaml = r#"
pipeline:
  name: sibling_body_windows
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: department, type: string }
      - { name: amount, type: int }
- type: composition
  name: outer
  input: src
  use: ../compositions/outer.comp.yaml
  inputs: { inp: src }
- type: sink
  name: out
  input: outer
  config: { name: out, type: csv, path: out.csv, include_unmapped: true }
"#;
    let config = parse_config(yaml).expect("parse");
    let ctx = CompileContext::with_pipeline_dir(root, PathBuf::from("pipelines"));
    let plan = config.compile(&ctx).expect("sibling bodies compile");
    let keys: Vec<_> = plan
        .composition_bodies()
        .values()
        .filter(|body| body.signature_path.ends_with("inner.comp.yaml"))
        .flat_map(|body| body.window_bindings.values().map(|binding| binding.key))
        .collect();
    assert_eq!(keys.len(), 2, "each sibling call gets a bound window");
    assert_ne!(keys[0].body_scope, keys[1].body_scope);
    assert_ne!(keys[0], keys[1], "sibling runtime keys must not alias");
}

#[test]
fn compiled_body_window_diagnostic_codes_are_registered() {
    for code in [
        "E160", "E161", "E162", "E163", "E165", "E166", "E167", "E168",
    ] {
        assert!(
            is_registered(code),
            "{code} must be in the diagnostic registry"
        );
    }
}

#[test]
fn compiled_body_window_cross_source_lookup_raises_e165() {
    let temp = tempfile::tempdir().expect("temp workspace");
    let root = temp.path();
    write_fixture(
        &root.join("compositions/cross_source.comp.yaml"),
        r#"
_compose:
  name: cross_source
  inputs:
    inp:
      schema:
        - { name: department, type: string }
        - { name: amount, type: int }
  outputs:
    out: running
  config_schema: {}
nodes:
  - type: transform
    name: running
    input: inp
    config:
      analytic_window:
        source: another_input
        group_by: [department]
      cxl: |
        emit department = department
        emit amount = amount
        emit running_total = $window.sum(amount)
"#,
    );
    let yaml = r#"
pipeline: { name: body_cross_source_rejected }
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: department, type: string }
      - { name: amount, type: int }
- type: composition
  name: body
  input: src
  use: ../compositions/cross_source.comp.yaml
  inputs: { inp: src }
- type: sink
  name: out
  input: body
  config: { name: out, type: csv, path: out.csv, include_unmapped: true }
"#;

    let codes = compile_error_codes(root, yaml);
    assert!(
        codes.iter().any(|code| code == "E165"),
        "cross-source body lookup must raise E165, got {codes:?}"
    );
}

#[test]
fn compiled_body_window_merge_root_raises_e166() {
    let temp = tempfile::tempdir().expect("temp workspace");
    let root = temp.path();
    write_fixture(
        &root.join("compositions/merge_root.comp.yaml"),
        r#"
_compose:
  name: merge_root
  inputs:
    left:
      schema:
        - { name: department, type: string }
        - { name: amount, type: int }
    right:
      schema:
        - { name: department, type: string }
        - { name: amount, type: int }
  outputs:
    out: running
  config_schema: {}
nodes:
  - type: merge
    name: joined
    inputs: [left, right]
    config: { mode: interleave }
  - type: transform
    name: running
    input: joined
    config:
      analytic_window:
        group_by: [department]
      cxl: |
        emit department = department
        emit amount = amount
        emit running_total = $window.sum(amount)
"#,
    );
    let yaml = r#"
pipeline: { name: body_merge_root_rejected }
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: department, type: string }
      - { name: amount, type: int }
- type: composition
  name: body
  input: src
  use: ../compositions/merge_root.comp.yaml
  inputs: { left: src, right: src }
- type: sink
  name: out
  input: body
  config: { name: out, type: csv, path: out.csv, include_unmapped: true }
"#;

    let codes = compile_error_codes(root, yaml);
    assert!(
        codes.iter().any(|code| code == "E166"),
        "Merge-rooted body window must raise E166, got {codes:?}"
    );
}

/// End-to-end body-window value correctness. The body's
/// `Aggregate → Transform(window)` must produce correct
/// `running_total` values reaching the writer.
#[test]
fn body_post_aggregate_window_runtime_values_match_aggregate_total() {
    let yaml = r#"
pipeline:
  name: body_post_aggregate_window_runtime
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: department, type: string }
      - { name: amount, type: int }
- type: composition
  name: body
  input: src
  use: ../compositions/body_post_aggregate_window.comp.yaml
  inputs:
    inp: src
- type: sink
  name: out
  input: body
  config:
    name: out
    type: csv
    path: out.csv
    include_unmapped: true
"#;
    let csv = "\
department,amount
HR,10
HR,20
HR,30
ENG,100
ENG,200
ENG,300
";
    let config = parse_config(yaml).expect("parse");
    let root = fixture_workspace_root();
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));
    let plan = config.compile(&ctx).expect("compile");

    let readers = HashMap::from([(
        config.source_configs().next().unwrap().name.clone(),
        clinker_exec::executor::single_file_reader(
            "test.csv",
            Box::new(Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);
    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &test_params())
        .expect("pipeline must run");
    let output = buf.as_string();

    // Body emits one row per dept; running_total over a 1-row partition
    // equals total.
    let mut by_dept: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut lines = output.lines();
    let header_line = lines.next().expect("header");
    let headers: Vec<&str> = header_line.split(',').collect();
    let dept_idx = headers.iter().position(|h| *h == "department").unwrap();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let v: Vec<&str> = line.split(',').collect();
        let row: HashMap<String, String> = headers
            .iter()
            .zip(v.iter())
            .map(|(h, val)| ((*h).to_string(), (*val).to_string()))
            .collect();
        by_dept.insert(v[dept_idx].to_string(), row);
    }
    let hr = by_dept.get("HR").expect("HR row");
    assert_eq!(hr.get("total").unwrap(), "60");
    assert_eq!(
        hr.get("running_total").unwrap(),
        "60",
        "body $window.sum(total) over post-aggregate row must equal total"
    );
    let eng = by_dept.get("ENG").expect("ENG row");
    assert_eq!(eng.get("total").unwrap(), "600");
    assert_eq!(eng.get("running_total").unwrap(), "600");
}

#[test]
fn runtime_nested_body_window_uses_its_own_records() {
    let temp = tempfile::tempdir().expect("temp workspace");
    let root = temp.path();
    write_fixture(
        &root.join("compositions/inner.comp.yaml"),
        INNER_WINDOW_COMPOSITION,
    );
    write_fixture(
        &root.join("compositions/outer.comp.yaml"),
        r#"
_compose:
  name: outer
  inputs:
    inp:
      schema:
        - { name: department, type: string }
        - { name: amount, type: int }
  outputs:
    out: inner.out
  config_schema: {}
nodes:
  - type: composition
    name: inner
    input: inp
    use: ./inner.comp.yaml
    inputs: { inp: inp }
"#,
    );
    let yaml = r#"
pipeline:
  name: nested_body_window_runtime
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: department, type: string }
      - { name: amount, type: int }
- type: composition
  name: outer
  input: src
  use: ../compositions/outer.comp.yaml
  inputs: { inp: src }
- type: sink
  name: out
  input: outer
  config: { name: out, type: csv, path: out.csv, include_unmapped: true }
"#;
    let output = run_csv_pipeline(
        root,
        yaml,
        "department,amount\nHR,10\nHR,20\nENG,100\nENG,200\n",
    );
    let mut totals: Vec<_> = output
        .lines()
        .skip(1)
        .filter_map(|line| line.rsplit(',').next())
        .collect();
    totals.sort_unstable();
    assert_eq!(totals, ["30", "30", "300", "300"]);
}

#[test]
fn runtime_sibling_body_windows_do_not_share_state() {
    let temp = tempfile::tempdir().expect("temp workspace");
    let root = temp.path();
    write_fixture(
        &root.join("compositions/inner.comp.yaml"),
        INNER_WINDOW_COMPOSITION,
    );
    write_fixture(
        &root.join("compositions/outer.comp.yaml"),
        r#"
_compose:
  name: outer
  inputs:
    inp:
      schema:
        - { name: department, type: string }
        - { name: amount, type: int }
  outputs:
    out: merged
  config_schema: {}
nodes:
  - type: composition
    name: left
    input: inp
    use: ./inner.comp.yaml
    inputs: { inp: inp }
  - type: composition
    name: right
    input: inp
    use: ./inner.comp.yaml
    inputs: { inp: inp }
  - type: merge
    name: merged
    inputs: [left, right]
    config: { mode: interleave }
"#,
    );
    let yaml = r#"
pipeline:
  name: sibling_body_window_runtime
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: department, type: string }
      - { name: amount, type: int }
- type: composition
  name: outer
  input: src
  use: ../compositions/outer.comp.yaml
  inputs: { inp: src }
- type: sink
  name: out
  input: outer
  config: { name: out, type: csv, path: out.csv, include_unmapped: true }
"#;
    let output = run_csv_pipeline(root, yaml, "department,amount\nHR,10\nHR,20\n");
    let totals: Vec<_> = output
        .lines()
        .skip(1)
        .filter_map(|line| line.rsplit(',').next())
        .collect();
    assert_eq!(totals.len(), 4, "both sibling calls must emit two rows");
    assert!(
        totals.iter().all(|total| *total == "30"),
        "each sibling must evaluate against only its own body records: {output}"
    );
}

#[test]
fn runtime_reusing_compiled_plan_resets_body_window_state() {
    let temp = tempfile::tempdir().expect("temp workspace");
    let root = temp.path();
    write_fixture(
        &root.join("compositions/inner.comp.yaml"),
        INNER_WINDOW_COMPOSITION,
    );
    let yaml = r#"
pipeline: { name: reused_body_window_runtime }
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: department, type: string }
      - { name: amount, type: int }
- type: composition
  name: body
  input: src
  use: ../compositions/inner.comp.yaml
  inputs: { inp: src }
- type: sink
  name: out
  input: body
  config: { name: out, type: csv, path: out.csv, include_unmapped: true }
"#;
    let config = parse_config(yaml).expect("parse");
    let ctx = CompileContext::with_pipeline_dir(root, PathBuf::from("pipelines"));
    let plan = config.compile(&ctx).expect("compile");
    let csv = "department,amount\nHR,10\nHR,20\n";

    let run = || {
        let readers = HashMap::from([(
            "src".to_string(),
            clinker_exec::executor::single_file_reader(
                "test.csv",
                Box::new(Cursor::new(csv.as_bytes().to_vec())),
            ),
        )]);
        let buffer = SharedBuffer::new();
        let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
            "out".to_string(),
            Box::new(buffer.clone()) as Box<dyn Write + Send>,
        )]);
        PipelineExecutor::run_plan_with_readers_writers_in_context(
            &plan,
            readers,
            writers,
            &test_params(),
            CompileContext::with_pipeline_dir(root, PathBuf::from("pipelines")),
        )
        .expect("reused plan must run");
        buffer.as_string()
    };

    let first = run();
    let second = run();
    assert_eq!(first, second, "body-window state must reset between runs");
    assert!(
        first.lines().skip(1).all(|line| line.ends_with(",30")),
        "each run must evaluate only its own records: {first}"
    );
}

// ── Body E150b — analyzer-driven arena_fields covers payload columns

/// A body Transform whose `analytic_window` references a payload column
/// the upstream body operator did not emit. The body lowering pass's
/// E150b validation must reject at compile time. (The body-side E150b
/// validator was wired earlier in this sprint; this is the test it earns.)
#[test]
fn compiled_body_window_referencing_field_outside_anchor_schema_raises_e168() {
    let yaml = r#"
pipeline:
  name: body_e150b
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: department, type: string }
      - { name: amount, type: int }
- type: composition
  name: body
  input: src
  use: ../compositions/body_e150b.comp.yaml
  inputs:
    inp: src
- type: sink
  name: out
  input: body
  config:
    name: out
    type: csv
    path: out.csv
    include_unmapped: true
"#;
    let config = parse_config(yaml).expect("parse");
    let root = fixture_workspace_root();
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));
    let result = config.compile(&ctx);
    let diags = result.expect_err("body window referencing missing column → E168");
    let codes: Vec<&str> = diags.iter().map(|d| d.code.as_str()).collect();
    assert!(
        codes.contains(&"E168"),
        "expected E168 in body diagnostics, got: {codes:?}"
    );
}
