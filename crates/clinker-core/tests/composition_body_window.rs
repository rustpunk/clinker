//! Composition body analytic-window integration suite.
//!
//! Body lowering happens before the parent DAG is built; the body-window
//! resolution pass runs post-Stage-5 and classifies each body window's
//! IndexSpec into one of the three `PlanIndexRoot` variants:
//!
//!   - `Source(name)` — body declares its own source.
//!   - `Node { upstream, .. }` — first non-pass-through ancestor is a
//!     body operator (Aggregate / Combine / Transform / etc.).
//!   - `ParentNode { upstream, .. }` — first non-pass-through ancestor
//!     is the body's `inp:` port, which resolves through to a parent-DAG
//!     operator at body-executor entry.
//!
//! Each test below pins a different rooting variant.

use std::collections::HashMap;
use std::io::{self, Cursor, Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use clinker_core::config::{CompileContext, PipelineConfig, parse_config};
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};
use clinker_core::plan::index::PlanIndexRoot;

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
        execution_id: "body-window-test".to_string(),
        batch_id: "batch".to_string(),
        pipeline_vars,
        shutdown_token: None,
    }
}

/// Compile a pipeline whose composition body contains
/// `Aggregate → Transform(window)`. The body's IndexSpec must be
/// rooted at the body's Aggregate (`PlanIndexRoot::Node`) and live
/// in the body's `body_indices_to_build`, NOT in the parent DAG's
/// top-level indices list.
#[test]
fn body_post_aggregate_window_roots_at_body_node() {
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
- type: output
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
    let artifacts = plan.artifacts();
    let bodies: Vec<_> = artifacts.composition_bodies.values().collect();
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
    match &spec.root {
        PlanIndexRoot::Node { upstream, .. } => assert_eq!(
            *upstream, body_agg_idx,
            "body IndexSpec must root at the body's Aggregate, not the parent's"
        ),
        other => panic!("expected PlanIndexRoot::Node, got {other:?}"),
    }
}

/// A body window whose first non-pass-through ancestor IS the body's
/// `inp:` port. The port resolves to the parent-DAG Source, so the
/// IndexSpec uses `PlanIndexRoot::ParentNode`. Body executor inherits
/// this slot via `Arc::clone` from the parent's runtime at body entry.
#[test]
fn body_window_through_input_port_roots_at_parent_node() {
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
- type: output
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

    let dag = plan.dag();
    let parent_src_idx = dag
        .graph
        .node_indices()
        .find(|i| dag.graph[*i].name() == "src")
        .expect("parent DAG carries src");

    let artifacts = plan.artifacts();
    let bodies: Vec<_> = artifacts.composition_bodies.values().collect();
    assert_eq!(bodies.len(), 1, "one body");
    let body = bodies[0];
    assert_eq!(body.body_indices_to_build.len(), 1, "one body IndexSpec");
    let spec = &body.body_indices_to_build[0];
    match &spec.root {
        PlanIndexRoot::ParentNode { upstream, .. } => assert_eq!(
            *upstream, parent_src_idx,
            "ParentNode-rooted spec must point at the parent-DAG Source \
             that the body's `inp:` port resolves to"
        ),
        other => {
            panic!("expected PlanIndexRoot::ParentNode resolving to parent src, got {other:?}")
        }
    }
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
- type: output
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

    let primary = "src".to_string();
    let readers = HashMap::from([(
        primary.clone(),
        clinker_core::executor::single_file_reader(
            "test.csv",
            Box::new(Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);
    PipelineExecutor::run_plan_with_readers_writers_with_primary(
        &plan,
        &primary,
        readers,
        writers,
        &test_params(&config),
    )
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

// ── Body E150b — analyzer-driven arena_fields covers payload columns

/// A body Transform whose `analytic_window` references a payload column
/// the upstream body operator did not emit. The body lowering pass's
/// E150b validation must reject at compile time. (The body-side E150b
/// validator was wired earlier in this sprint; this is the test it earns.)
#[test]
fn body_window_referencing_field_outside_anchor_schema_raises_e150b() {
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
- type: output
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
    let diags = result.expect_err("body window referencing missing column → E150b");
    let codes: Vec<&str> = diags.iter().map(|d| d.code.as_str()).collect();
    assert!(
        codes.contains(&"E150b"),
        "expected E150b in body diagnostics, got: {codes:?}"
    );
}
