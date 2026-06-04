//! End-to-end determinism of memory-aware dispatch scheduling.
//!
//! The executor resolves dispatch order through the memory arbitrator's
//! `next_runnable` (headroom fit, then largest immediate-freed-on-complete,
//! then largest downstream subtree reclaim, then stable topo index). Whether
//! the scheduler reorders or not, record output and final counters MUST be
//! byte-identical — scheduling steers only which runnable node the engine
//! dispatches next, never what each node emits.
//!
//! Both runs below drive identical in-memory CSV readers keyed by source
//! name, so the only variable is whether per-node volume estimates exist:
//!
//! - With the compile anchor pointed at a directory holding sized `path:`
//!   files, every Source seeds a non-zero `predicted_peak_bytes`, so the
//!   freed-bytes, subtree-reclaim, and headroom-fit tiers are live and the
//!   scheduler is free to reorder the runnable frontier.
//! - With the anchor pointed at an empty directory the `path:` files do not
//!   exist, every estimate is `0`, and the scheduler collapses to the plain
//!   front-to-back topological walk.
//!
//! Equal output across the two proves estimate-driven reordering changes
//! nothing observable in the data — the load-bearing invariant the ops docs
//! state.
//!
//! Output rows are compared as a sorted multiset, not line-for-line: a hash
//! Aggregate emits its groups in `HashMap` iteration order, which is seeded
//! per process by `RandomState` and so varies run-to-run regardless of
//! dispatch order. That orthogonal nondeterminism is not what this test
//! pins; sorting isolates the scheduling variable (the set of emitted
//! group rows and their values) from the aggregate's emission order.

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{
    PipelineExecutor, PipelineRunParams, SourceReaders, single_file_reader,
};
use clinker_plan::config::{CompileContext, parse_config};
use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::path::Path;

/// Two independent `Source -> Aggregate -> Output` chains. The `path:`
/// strings resolve against the compile anchor; the actual bytes always come
/// from the cursor readers, so output is anchor-independent by construction.
const TWO_CHAIN_YAML: &str = r#"
pipeline:
  name: two_chain
nodes:
  - type: source
    name: src_small
    config:
      name: src_small
      type: csv
      path: small.csv
      schema:
        - { name: k, type: string }
        - { name: v, type: int }
  - type: source
    name: src_large
    config:
      name: src_large
      type: csv
      path: large.csv
      schema:
        - { name: k, type: string }
        - { name: v, type: int }
  - type: aggregate
    name: agg_small
    input: src_small
    config:
      group_by: [k]
      cxl: |
        emit k = k
        emit total = sum(v)
  - type: aggregate
    name: agg_large
    input: src_large
    config:
      group_by: [k]
      cxl: |
        emit k = k
        emit total = sum(v)
  - type: output
    name: out_small
    input: agg_small
    config:
      name: out_small
      type: csv
      path: out_small.csv
      include_unmapped: true
  - type: output
    name: out_large
    input: agg_large
    config:
      name: out_large
      type: csv
      path: out_large.csv
      include_unmapped: true
"#;

fn small_csv() -> String {
    let mut s = String::from("k,v\n");
    for i in 0..40 {
        s.push_str(&format!("g{},{}\n", i % 4, i));
    }
    s
}

fn large_csv() -> String {
    let mut s = String::from("k,v\n");
    for i in 0..400 {
        s.push_str(&format!("g{},{}\n", i % 4, i));
    }
    s
}

fn run_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "sched-determinism".to_string(),
        batch_id: "batch-0".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

fn readers() -> SourceReaders {
    let mut r: SourceReaders = HashMap::new();
    r.insert(
        "src_small".to_string(),
        single_file_reader("small.csv", Box::new(Cursor::new(small_csv().into_bytes()))),
    );
    r.insert(
        "src_large".to_string(),
        single_file_reader("large.csv", Box::new(Cursor::new(large_csv().into_bytes()))),
    );
    r
}

/// Run the pipeline once with the compile anchor at `anchor`, returning
/// `(out_small, out_large, total_count)`. The cursor readers supply the data
/// regardless of `anchor`, so the only thing `anchor` controls is whether the
/// `path:` files exist on disk and therefore whether the scheduler sees
/// non-zero volume estimates.
fn run_at(anchor: &Path) -> (String, String, u64) {
    let config = parse_config(TWO_CHAIN_YAML).expect("parse");
    let plan = config
        .compile(&CompileContext::with_pipeline_dir(anchor, ""))
        .expect("compile");

    let out_small = SharedBuffer::new();
    let out_large = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([
        (
            "out_small".to_string(),
            Box::new(out_small.clone()) as Box<dyn Write + Send>,
        ),
        (
            "out_large".to_string(),
            Box::new(out_large.clone()) as Box<dyn Write + Send>,
        ),
    ]);

    let report = PipelineExecutor::run_plan_with_readers_writers_in_context(
        &plan,
        readers(),
        writers,
        &run_params(),
        CompileContext::with_pipeline_dir(anchor, ""),
    )
    .expect("run");

    (
        out_small.as_string(),
        out_large.as_string(),
        report.counters.total_count,
    )
}

/// Output and counters are byte-identical whether the scheduler has volume
/// estimates to reorder by or not. Proves estimate-driven dispatch reordering
/// is invisible in the data.
#[test]
fn scheduling_reorder_does_not_change_output() {
    // Anchor with sized files: non-zero estimates -> scheduler may reorder.
    let sized = tempfile::tempdir().unwrap();
    {
        let mut f = std::fs::File::create(sized.path().join("small.csv")).unwrap();
        f.write_all(small_csv().as_bytes()).unwrap();
        let mut f = std::fs::File::create(sized.path().join("large.csv")).unwrap();
        f.write_all(large_csv().as_bytes()).unwrap();
    }
    // Anchor with no files: every estimate is 0 -> plain topological walk.
    let empty = tempfile::tempdir().unwrap();

    // Premise: the sized anchor really does seed non-zero estimates and the
    // empty one really does seed zero, so the two runs exercise different
    // scheduler tiers.
    let sized_plan = parse_config(TWO_CHAIN_YAML)
        .unwrap()
        .compile(&CompileContext::with_pipeline_dir(sized.path(), ""))
        .unwrap();
    let empty_plan = parse_config(TWO_CHAIN_YAML)
        .unwrap()
        .compile(&CompileContext::with_pipeline_dir(empty.path(), ""))
        .unwrap();
    let peak = |plan: &clinker_plan::plan::compiled::CompiledPlan, node: &str| {
        let dag = plan.dag();
        let idx = dag
            .graph
            .node_indices()
            .find(|i| dag.graph[*i].name() == node)
            .unwrap();
        dag.node_properties[&idx].predicted_peak_bytes
    };
    assert!(
        peak(&sized_plan, "src_large") > 0,
        "sized anchor must seed a non-zero estimate (scheduler tiers live)"
    );
    assert_eq!(
        peak(&empty_plan, "src_large"),
        0,
        "empty anchor must seed zero (scheduler falls back to topo order)"
    );

    let (small_a, large_a, total_a) = run_at(sized.path());
    let (small_b, large_b, total_b) = run_at(empty.path());

    // Sorted multiset of rows: insensitive to the hash Aggregate's
    // per-process group emission order, sensitive to any difference in the
    // groups themselves or their aggregated values.
    let rows = |csv: &str| {
        let mut v: Vec<String> = csv.lines().map(str::to_owned).collect();
        v.sort();
        v
    };

    assert_eq!(
        rows(&small_a),
        rows(&small_b),
        "out_small groups+values must not depend on dispatch order"
    );
    assert_eq!(
        rows(&large_a),
        rows(&large_b),
        "out_large groups+values must not depend on dispatch order"
    );
    assert_eq!(
        total_a, total_b,
        "ingested record count must not depend on dispatch order"
    );
    assert_eq!(total_a, 40 + 400, "all rows from both chains ingested");
}
