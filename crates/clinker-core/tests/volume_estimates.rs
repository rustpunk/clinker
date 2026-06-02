//! Gate tests for per-node input-volume byte estimates.
//!
//! `derive_volume_estimates` seeds `predicted_peak_bytes` at file-backed
//! Sources from the on-disk `path:` file size (resolved against the pipeline
//! file's directory, never the process CWD) and propagates it forward in
//! topological order. A blocking node (hash Aggregate) carries a non-zero
//! `predicted_freed_bytes_on_complete` equal to the volume it accumulates;
//! streaming/fused nodes free nothing.
//!
//! These tests pin the seed, the unknown-fallback, and the propagation +
//! freed-on-complete model. The estimates are computed but intentionally not
//! surfaced in `--explain` output yet, so none of the explain/data goldens
//! may move.

use clinker_core::config::{CompileContext, parse_config};
use clinker_core::plan::compiled::CompiledPlan;
use clinker_core::plan::execution::ExecutionPlanDag;
use clinker_core::plan::properties::NodeProperties;
use std::io::Write;
use std::path::Path;

/// Compile `yaml` with the pipeline-file directory anchored at `anchor` so
/// relative source `path:` strings resolve against the same stable root the
/// runtime discovery layer uses. `pipeline_dir` is empty, so the resolution
/// anchor (`workspace_root.join(pipeline_dir)`) equals `anchor`.
fn compile_anchored(yaml: &str, anchor: &Path) -> CompiledPlan {
    let config = parse_config(yaml).expect("parse");
    let ctx = CompileContext::with_pipeline_dir(anchor, "");
    config.compile(&ctx).expect("compile")
}

fn props_for_node<'a>(plan: &'a ExecutionPlanDag, name: &str) -> &'a NodeProperties {
    let idx = plan
        .graph
        .node_indices()
        .find(|i| plan.graph[*i].name() == name)
        .unwrap_or_else(|| panic!("no node named {name:?}"));
    plan.node_properties
        .get(&idx)
        .unwrap_or_else(|| panic!("no props for node {name:?}"))
}

/// Write `contents` to `<dir>/<name>` and return its byte length.
fn write_file(dir: &Path, name: &str, contents: &str) -> u64 {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).expect("create data file");
    f.write_all(contents.as_bytes()).expect("write data file");
    f.flush().expect("flush data file");
    contents.len() as u64
}

fn single_source_pipeline(path: &str) -> String {
    format!(
        r#"
pipeline:
  name: vol
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: {path}
      schema:
        - {{ name: department, type: string }}
        - {{ name: amount, type: int }}
  - type: output
    name: out
    input: orders
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

#[test]
fn sized_file_source_seeds_nonzero_peak() {
    let tmp = tempfile::tempdir().unwrap();
    let len = write_file(
        tmp.path(),
        "orders.csv",
        "department,amount\nsales,100\nops,250\nsales,75\n",
    );
    assert!(len > 0, "fixture must be non-empty");

    let plan = compile_anchored(&single_source_pipeline("orders.csv"), tmp.path());
    let dag = plan.dag();

    let src = props_for_node(dag, "orders");
    assert_eq!(
        src.predicted_peak_bytes, len,
        "a sized file Source must seed predicted_peak_bytes from the file length"
    );
    // A Source streams records out; it holds nothing it can free on drain.
    assert_eq!(src.predicted_freed_bytes_on_complete, 0);
}

#[test]
fn seed_keyed_by_node_not_config_name() {
    // The node-level `name:` (header) and the nested `config.name:` are
    // independent identifiers the compiler accepts as different. The seed
    // is read off the resolved `SourceConfig` the plan node already carries,
    // so it must land regardless of which identifier is used for lookup.
    // A name map keyed by `config.name` would miss the node here and seed 0.
    let tmp = tempfile::tempdir().unwrap();
    let len = write_file(
        tmp.path(),
        "orders.csv",
        "department,amount\nsales,100\nops,250\n",
    );
    assert!(len > 0);

    let yaml = r#"
pipeline:
  name: vol
nodes:
  - type: source
    name: header_name
    config:
      name: config_name
      type: csv
      path: orders.csv
      schema:
        - { name: department, type: string }
        - { name: amount, type: int }
  - type: output
    name: out
    input: header_name
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let plan = compile_anchored(yaml, tmp.path());
    let dag = plan.dag();

    let src = props_for_node(dag, "header_name");
    assert_eq!(
        src.predicted_peak_bytes, len,
        "the seed must be read off the node's resolved config, not a name map; \
         a node whose header name differs from its config name must still seed its file size"
    );
}

#[test]
fn missing_file_source_seeds_zero() {
    // No file is written; the literal `path:` points at a nonexistent file,
    // so std::fs::metadata fails and the seed is the `0` unknown sentinel.
    let tmp = tempfile::tempdir().unwrap();
    let plan = compile_anchored(&single_source_pipeline("absent.csv"), tmp.path());
    let dag = plan.dag();

    let src = props_for_node(dag, "orders");
    assert_eq!(
        src.predicted_peak_bytes, 0,
        "a missing-file Source must seed 0 (unknown)"
    );
    assert_eq!(src.predicted_freed_bytes_on_complete, 0);
}

#[test]
fn glob_multi_file_source_seeds_zero() {
    // A glob matcher fans out to multiple files at discovery time and has no
    // single literal size to seed, so it stays at the `0` unknown sentinel
    // even when matching files exist on disk.
    let tmp = tempfile::tempdir().unwrap();
    write_file(tmp.path(), "a.csv", "department,amount\nsales,100\n");
    write_file(tmp.path(), "b.csv", "department,amount\nops,250\n");

    let yaml = r#"
pipeline:
  name: vol
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      glob: "*.csv"
      schema:
        - { name: department, type: string }
        - { name: amount, type: int }
  - type: output
    name: out
    input: orders
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let plan = compile_anchored(yaml, tmp.path());
    let dag = plan.dag();

    let src = props_for_node(dag, "orders");
    assert_eq!(
        src.predicted_peak_bytes, 0,
        "a multi-file (glob) Source must seed 0 (unknown)"
    );
}

#[test]
fn propagation_source_transform_aggregate() {
    // Source -> Transform -> Aggregate: the Transform inherits the Source's
    // seeded volume (pass-through), the hash Aggregate's peak equals the
    // volume it must accumulate, and — being blocking — it frees that state
    // on drain. The downstream Output inherits the Aggregate's peak.
    let tmp = tempfile::tempdir().unwrap();
    let len = write_file(
        tmp.path(),
        "orders.csv",
        "department,amount\nsales,100\nops,250\nsales,75\nops,40\n",
    );
    assert!(len > 0);

    let yaml = r#"
pipeline:
  name: vol
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: department, type: string }
        - { name: amount, type: int }
  - type: transform
    name: tagged
    input: orders
    config:
      cxl: |
        emit department = department
        emit amount = amount
  - type: aggregate
    name: dept_totals
    input: tagged
    config:
      group_by: [department]
      cxl: |
        emit department = department
        emit total = sum(amount)
  - type: output
    name: out
    input: dept_totals
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let plan = compile_anchored(yaml, tmp.path());
    let dag = plan.dag();

    let src = props_for_node(dag, "orders");
    let transform = props_for_node(dag, "tagged");
    let agg = props_for_node(dag, "dept_totals");
    let out = props_for_node(dag, "out");

    assert_eq!(src.predicted_peak_bytes, len, "Source seeds the file size");

    // Stateless Transform passes the live volume through unchanged and frees
    // nothing.
    assert_eq!(
        transform.predicted_peak_bytes, len,
        "Transform inherits a non-zero derived peak from its parent"
    );
    assert_eq!(transform.predicted_freed_bytes_on_complete, 0);

    // The hash Aggregate is blocking: its peak is the accumulated input and
    // it frees that state once it has emitted.
    assert_eq!(
        agg.predicted_peak_bytes, len,
        "blocking Aggregate's peak equals its accumulated input volume"
    );
    assert_eq!(
        agg.predicted_freed_bytes_on_complete, len,
        "blocking Aggregate frees its accumulated state on complete"
    );

    // Downstream of the Aggregate the (coarse) volume keeps propagating; the
    // streaming Output frees nothing.
    assert_eq!(out.predicted_peak_bytes, len);
    assert_eq!(out.predicted_freed_bytes_on_complete, 0);
}

#[test]
fn determinism_identical_plan_identical_sizes() {
    // Identical plan + identical on-disk size => identical estimates, twice.
    let mk = || {
        let tmp = tempfile::tempdir().unwrap();
        write_file(tmp.path(), "orders.csv", "department,amount\nsales,100\n");
        let plan = compile_anchored(&single_source_pipeline("orders.csv"), tmp.path());
        let dag = plan.dag();
        props_for_node(dag, "orders").predicted_peak_bytes
    };
    assert_eq!(mk(), mk());
}
