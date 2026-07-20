//! End-to-end integration tests for the composition body executor.
//!
//! Each test builds a full pipeline that funnels CSV records through
//! a composition node and inspects the output CSV. The tests are the
//! regression gate against the prior pass-through Composition arm —
//! a stub would not produce body-emitted columns, would not recurse
//! into nested compositions, would not branch through route+merge
//! bodies, and would not wrap inner errors with composition-name
//! context.

use std::collections::HashMap;
use std::io::{self, Cursor, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_plan::config::{CompileContext, parse_config};

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
        execution_id: "composition-executor-test".to_string(),
        batch_id: "batch-001".to_string(),
        pipeline_vars,
        shutdown_token: None,
        ..Default::default()
    }
}

fn run_with_composition(
    yaml: &str,
    csv_input: &str,
) -> (clinker_exec::executor::ExecutionReport, String) {
    let config = parse_config(yaml).expect("parse pipeline yaml");
    let root = fixture_workspace_root();
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));
    let plan = config.compile(&ctx).expect("compile pipeline");

    let primary = config.source_configs().next().unwrap().name.clone();
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        primary,
        clinker_exec::executor::single_file_reader(
            "test.csv",
            Box::new(Cursor::new(csv_input.as_bytes().to_vec())),
        ),
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &test_params())
            .expect("pipeline run");
    (report, buf.as_string())
}

#[test]
fn test_composition_body_executes_transform() {
    // A composition whose body is a single transform doubling
    // `a` into `computed`. The body executor must run the
    // transform — a pass-through stub would not produce a
    // `computed` column.
    let yaml = r#"
pipeline:
  name: composition_executor_transform
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: a, type: int }
  - type: composition
    name: doubler_call
    input: src
    use: ../compositions/exec_transform_check.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: doubler_call
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let csv_input = "a\n5\n7\n";
    let (_report, output) = run_with_composition(yaml, csv_input);

    assert!(
        output.contains("computed"),
        "output header must include `computed` produced by body transform; got:\n{output}"
    );
    // The body emitted `computed = a * 2` for each input row.
    assert!(
        output.contains("10"),
        "expected `computed = 10` for input a=5; got:\n{output}"
    );
    assert!(
        output.contains("14"),
        "expected `computed = 14` for input a=7; got:\n{output}"
    );
}

#[test]
fn test_unresolvable_composition_use_fails_run_instead_of_empty_output() {
    // A composition whose `use:` names a `.comp.yaml` that does not exist
    // in the workspace. Compile must fail with the binding diagnostic so
    // the run aborts loudly — the earlier behavior omitted the composition
    // node and let the run "succeed" writing zero records to `out`, a
    // silent empty output indistinguishable from an empty input.
    let yaml = r#"
pipeline:
  name: composition_executor_unresolvable
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: a, type: int }
  - type: composition
    name: missing_call
    input: src
    use: ../compositions/does_not_exist.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: missing_call
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).expect("parse pipeline yaml");
    let root = fixture_workspace_root();
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));

    let diags = config
        .compile(&ctx)
        .expect_err("an unresolvable composition `use:` must fail compile, not run empty");
    assert!(
        diags.iter().any(|d| d.code == "E103"),
        "compile must surface E103 for the unresolvable `use:` path; got: {diags:?}"
    );
}

#[test]
fn test_nested_composition_body_executes() {
    // A composition body whose first node is itself a composition.
    // The body executor must recurse into the inner body so the
    // inner-emitted `computed` AND the outer-emitted `marker`
    // columns both reach the top-level output.
    let yaml = r#"
pipeline:
  name: composition_executor_nested
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: a, type: int }
  - type: composition
    name: nested_call
    input: src
    use: ../compositions/exec_nested_check.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: nested_call
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let csv_input = "a\n3\n";
    let (_report, output) = run_with_composition(yaml, csv_input);

    assert!(
        output.contains("computed"),
        "output must carry inner-body `computed`; got:\n{output}"
    );
    assert!(
        output.contains("marker"),
        "output must carry outer-body `marker`; got:\n{output}"
    );
    assert!(
        output.contains("nested"),
        "output must contain marker value 'nested'; got:\n{output}"
    );
    // Inner doubles a=3 to computed=6.
    assert!(
        output.contains("6"),
        "expected inner-doubled value 6; got:\n{output}"
    );
}

#[test]
fn test_composition_body_with_route_merge() {
    // The body splits inp.a into lo/hi branches via route, tags
    // each branch with a `bucket` column, then merges. Every input
    // row must reach the output exactly once with the correct
    // bucket tag — the prior linear-chain stub could not represent
    // this DAG and would silently drop branches.
    let yaml = r#"
pipeline:
  name: composition_executor_route_merge
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: a, type: int }
  - type: composition
    name: split_call
    input: src
    use: ../compositions/exec_route_merge_check.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: split_call
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let csv_input = "a\n3\n5\n12\n42\n";
    let (_report, output) = run_with_composition(yaml, csv_input);

    let lo_count = output.matches(",lo").count();
    let hi_count = output.matches(",hi").count();
    assert_eq!(
        lo_count, 2,
        "expected 2 lo-bucket rows for a=3 and a=5; got:\n{output}"
    );
    assert_eq!(
        hi_count, 2,
        "expected 2 hi-bucket rows for a=12 and a=42; got:\n{output}"
    );
}

#[test]
fn test_same_named_route_top_level_and_body_resolve_independently() {
    // A top-level route `gate` (threshold tier > 100) and a route ALSO named
    // `gate` inside a composition body (threshold tier > 10) are distinct node
    // instances in distinct graphs and must each evaluate their own conditions.
    // Each Route node carries its own typed branch programs, so a shared node
    // name across scopes cannot cross-wire conditions: tier=50 is `cold` for the
    // top-level route (> 100 is false) but `hot` for the body route (> 10 is
    // true). Cross-wiring would route tier=50 to the top-level `hot` branch,
    // emptying `top_cold`.
    let yaml = r#"
pipeline:
  name: route_scope_collision
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: string }
        - { name: tier, type: int }
  - type: route
    name: gate
    input: src
    config:
      conditions:
        hot: "tier > 100"
      default: cold
  - type: transform
    name: top_hot
    input: gate.hot
    config:
      cxl: |
        emit id = id
        emit tier = tier
        emit via = "top_hot"
  - type: transform
    name: top_cold
    input: gate.cold
    config:
      cxl: |
        emit id = id
        emit tier = tier
        emit via = "top_cold"
  - type: merge
    name: top_merged
    inputs: [top_hot, top_cold]
  - type: output
    name: top_out
    input: top_merged
    config:
      name: top_out
      type: csv
      path: top.csv
      include_unmapped: true
  - type: composition
    name: comp
    input: src
    use: ../compositions/route_scope_body.comp.yaml
    inputs:
      inp: src
  - type: output
    name: comp_out
    input: comp
    config:
      name: comp_out
      type: csv
      path: comp.csv
      include_unmapped: true
"#;
    let csv_input = "id,tier\nr1,50\n";

    let config = parse_config(yaml).expect("parse pipeline yaml");
    let root = fixture_workspace_root();
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));
    let plan = config.compile(&ctx).expect("compile pipeline");

    let primary = config.source_configs().next().unwrap().name.clone();
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        primary,
        clinker_exec::executor::single_file_reader(
            "test.csv",
            Box::new(Cursor::new(csv_input.as_bytes().to_vec())),
        ),
    )]);

    let bufs: HashMap<String, SharedBuffer> = config
        .output_configs()
        .map(|o| (o.name.clone(), SharedBuffer::new()))
        .collect();
    let writers: HashMap<String, Box<dyn Write + Send>> = bufs
        .iter()
        .map(|(name, buf)| (name.clone(), Box::new(buf.clone()) as Box<dyn Write + Send>))
        .collect();

    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &test_params())
        .expect("pipeline run");

    let top_out = bufs.get("top_out").expect("top_out present").as_string();
    let comp_out = bufs.get("comp_out").expect("comp_out present").as_string();

    assert!(
        top_out.contains("top_cold") && !top_out.contains("top_hot"),
        "top-level route (threshold 100) must route tier=50 to its own `cold` \
         branch; got top_out:\n{top_out}\ncomp_out:\n{comp_out}"
    );
    assert!(
        comp_out.contains("body_hot") && !comp_out.contains("body_cold"),
        "body route (threshold 10) must route tier=50 to its own `hot` branch; \
         got comp_out:\n{comp_out}\ntop_out:\n{top_out}"
    );
}

#[test]
fn test_composition_body_error_context() {
    // A body transform that fires a runtime CXL error must surface
    // wrapped with the composition's name so users can locate the
    // failure inside the composition, not just the inner-only
    // message. The body uses `to_int()` on a non-numeric column to
    // trigger a coercion failure under FailFast.
    let yaml = r#"
pipeline:
  name: composition_executor_error
error_handling:
  strategy: fail_fast
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: a, type: string }
  - type: composition
    name: doubler_call
    input: src
    use: ../compositions/exec_runtime_error.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: doubler_call
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let csv_input = "a\nnot-an-integer\n";
    let config = parse_config(yaml).expect("parse pipeline yaml");
    let root = fixture_workspace_root();
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));
    let plan = config.compile(&ctx).expect("compile pipeline");

    let primary = config.source_configs().next().unwrap().name.clone();
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        primary,
        clinker_exec::executor::single_file_reader(
            "test.csv",
            Box::new(Cursor::new(csv_input.as_bytes().to_vec())),
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let result =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &test_params());
    let err = result.expect_err("expected pipeline run to fail");
    let msg = err.to_string();
    assert!(
        msg.contains("in composition 'doubler_call'"),
        "error must wrap inner failure with composition name; got: {msg}"
    );
}

#[test]
fn composition_body_merge_uses_declared_interleave_mode() {
    // The body's Merge declares `mode: interleave`. Because merge mode is now
    // carried on the plan node (not looked up by name in the top-level config,
    // which does not contain body nodes), the body Merge round-robins its two
    // branches. A regression to the name lookup would miss the body node,
    // default to Concat, and emit lo,lo,hi,hi instead of lo,hi,lo,hi.
    let yaml = r#"
pipeline:
  name: composition_body_merge_interleave
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: a, type: int }
  - type: composition
    name: split_merge
    input: src
    use: ../compositions/exec_merge_interleave_check.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: split_merge
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    // Route sends a<10 to `lo`, a>=10 to `hi`: lo_branch=[5,6], hi_branch=[15,16].
    let csv_input = "a\n5\n15\n6\n16\n";
    let (_report, output) = run_with_composition(yaml, csv_input);

    let buckets: Vec<&str> = output
        .lines()
        .filter_map(|l| {
            if l.contains("lo") {
                Some("lo")
            } else if l.contains("hi") {
                Some("hi")
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        buckets,
        vec!["lo", "hi", "lo", "hi"],
        "body Merge must interleave (round-robin) per its declared mode, not \
         concatenate:\n{output}"
    );
}
