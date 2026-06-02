//! Memory-aware dispatch-order selection.
//!
//! `scheduled_pass_order` is the production scheduler the executor's
//! dispatch loop calls: it resolves the order nodes are dispatched in by
//! asking [`MemoryArbitrator::next_runnable`] which runnable node to take
//! next, instead of walking `topo_order` blindly. These tests pin its two
//! load-bearing behaviors against a real compiled [`ExecutionPlanDag`] and
//! a real arbitrator (no mock hint):
//!
//! - With no volume estimates every node predicts `0` bytes, so selection
//!   collapses to lowest topo index and the resolved order is byte-for-byte
//!   `topo_order` — the "no-estimates == today's order" determinism floor.
//! - With real file-size estimates the freed-on-complete tier takes a ready
//!   blocking Aggregate over a lower-indexed fresh Source (reclaiming its
//!   accumulated state before the next buffer charges), and the headroom-fit
//!   tier takes a fitting node over a lower-indexed one that would overflow
//!   the budget. Both are genuine reorders away from stable topo index.

use super::*;
use crate::config::{CompileContext, parse_config};
use crate::pipeline::memory::MemoryArbitrator;
use petgraph::graph::NodeIndex;
use std::collections::HashSet;
use std::path::Path;

/// Two independent `Source -> Aggregate -> Output` chains, the smaller
/// chain declared first so its nodes sort earliest in `topo_order`. The
/// `{small,large}.csv` `path:` strings resolve against the compile anchor.
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

fn write_csv(dir: &Path, name: &str, rows: usize) -> u64 {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).expect("create csv");
    f.write_all(b"k,v\n").unwrap();
    let mut len = 4u64;
    for i in 0..rows {
        let line = format!("group_{},{}\n", i % 8, i);
        f.write_all(line.as_bytes()).unwrap();
        len += line.len() as u64;
    }
    f.flush().unwrap();
    len
}

fn compile_dag(yaml: &str, anchor: &Path) -> crate::plan::compiled::CompiledPlan {
    let config = parse_config(yaml).expect("parse");
    let ctx = CompileContext::with_pipeline_dir(anchor, "");
    config.compile(&ctx).expect("compile")
}

fn node_idx(dag: &crate::plan::execution::ExecutionPlanDag, name: &str) -> NodeIndex {
    dag.graph
        .node_indices()
        .find(|i| dag.graph[*i].name() == name)
        .unwrap_or_else(|| panic!("no node named {name:?}"))
}

/// With no on-disk files the volume seed is `0` everywhere, so the
/// scheduler has no estimate to act on and must reproduce the plain
/// front-to-back `topo_order` walk byte-for-byte. This is the load-bearing
/// determinism guarantee: absent statistics, scheduling order equals the
/// order the executor used before any scheduler existed.
#[test]
fn no_estimates_reproduces_topo_order_exactly() {
    // `anchor` is an empty temp dir, so `small.csv` / `large.csv` do not
    // exist and every Source seeds `predicted_peak_bytes == 0`.
    let tmp = tempfile::tempdir().unwrap();
    let plan = compile_dag(TWO_CHAIN_YAML, tmp.path());
    let dag = plan.dag();

    // Confirm the premise: nothing seeded a non-zero estimate.
    assert!(
        dag.node_properties
            .values()
            .all(|p| p.predicted_peak_bytes == 0),
        "premise broken: a non-existent path seeded a non-zero peak"
    );

    let arbitrator =
        MemoryArbitrator::with_policy(512 * 1024 * 1024, 0.80, MemoryArbitrator::default_policy());
    let all: HashSet<NodeIndex> = dag.topo_order.iter().copied().collect();

    let order = scheduled_pass_order(dag, &arbitrator, &all);
    assert_eq!(
        order, dag.topo_order,
        "with no estimates the scheduler must reproduce topo order exactly"
    );
}

/// The freed-on-complete tiebreak reorders away from stable topo index:
/// when the runnable frontier offers BOTH a fresh Source (frees nothing on
/// drain) and a blocking Aggregate ready to drain (frees its accumulated
/// state), `next_runnable` takes the Aggregate even though the Source has a
/// lower topo index. Draining the Aggregate first reclaims headroom for the
/// budget before the next Source charges its buffer.
///
/// This drives `next_runnable` directly with a frontier the executor's
/// dispatch loop produces mid-walk (the same call `scheduled_pass_order`
/// makes each step), pinning the selection rule on the real plan's
/// predictions rather than a mock hint.
#[test]
fn freed_tiebreak_prefers_draining_blocking_aggregate_over_fresh_source() {
    let tmp = tempfile::tempdir().unwrap();
    let small_len = write_csv(tmp.path(), "small.csv", 64);
    let large_len = write_csv(tmp.path(), "large.csv", 512);
    assert!(small_len > 0 && large_len > small_len);

    let plan = compile_dag(TWO_CHAIN_YAML, tmp.path());
    let dag = plan.dag();

    let agg_small = node_idx(dag, "agg_small");
    let src_large = node_idx(dag, "src_large");

    // Premise: the blocking Aggregate predicts freeing its accumulated input,
    // the fresh Source frees nothing, yet the Source sorts earlier in topo.
    assert!(
        dag.node_properties[&agg_small].predicted_freed_bytes_on_complete > 0,
        "agg_small must predict freeing its accumulated input"
    );
    assert_eq!(
        dag.node_properties[&src_large].predicted_freed_bytes_on_complete, 0,
        "a Source frees nothing on drain"
    );
    let topo_pos = |idx: NodeIndex| dag.topo_order.iter().position(|&n| n == idx).unwrap();

    let arbitrator =
        MemoryArbitrator::with_policy(512 * 1024 * 1024, 0.80, MemoryArbitrator::default_policy());

    // Frontier as the dispatch loop sees it once src_small is done and
    // agg_small has become runnable while src_large is still waiting.
    let frontier = [src_large, agg_small];
    let chosen = arbitrator.next_runnable(
        &frontier,
        dag as &dyn crate::pipeline::memory::SchedulingHint,
    );

    assert_eq!(
        chosen, agg_small,
        "freed-bytes tiebreak must drain the blocking Aggregate before charging the next Source, \
         even though the Source has the lower topo index"
    );
    // Confirm this is a genuine reorder: lowest-stable-index alone would have
    // taken the Source.
    assert!(
        topo_pos(src_large) < topo_pos(agg_small),
        "premise: src_large has the lower topo index, so picking agg_small is a freed-bytes reorder"
    );
}

/// Headroom fit overrides topo index: when remaining headroom can hold a
/// later-indexed Source but not an earlier-indexed one, `next_runnable`
/// takes the fitting node. Choosing the node that fits avoids pushing the
/// budget into spill territory this step — a memory-aware choice a blind
/// topo walk (which would take the lower-index, non-fitting Source) cannot
/// make.
#[test]
fn headroom_fit_prefers_fitting_node_over_lower_index() {
    let tmp = tempfile::tempdir().unwrap();
    write_csv(tmp.path(), "small.csv", 16);
    write_csv(tmp.path(), "large.csv", 4096);
    let plan = compile_dag(TWO_CHAIN_YAML, tmp.path());
    let dag = plan.dag();

    let src_small = node_idx(dag, "src_small");
    let src_large = node_idx(dag, "src_large");
    let small_peak = dag.node_properties[&src_small].predicted_peak_bytes;
    let large_peak = dag.node_properties[&src_large].predicted_peak_bytes;
    assert!(small_peak > 0 && large_peak > small_peak);

    // Soft limit between the two Source peaks: the small Source fits the
    // empty-budget headroom, the large one does not.
    let soft = (small_peak + large_peak) / 2;
    // `with_policy` takes the hard limit and a soft fraction; pick a hard
    // limit whose soft floor equals `soft`.
    let arbitrator =
        MemoryArbitrator::with_policy(soft * 2, 0.50, MemoryArbitrator::default_policy());
    assert_eq!(arbitrator.soft_limit(), soft);
    assert!(small_peak <= soft && large_peak > soft);

    // src_large has the lower topo index but overflows headroom; the
    // scheduler must still prefer the fitting src_small.
    let topo_pos = |idx: NodeIndex| dag.topo_order.iter().position(|&n| n == idx).unwrap();
    assert!(topo_pos(src_large) < topo_pos(src_small));

    let frontier = [src_large, src_small];
    let chosen = arbitrator.next_runnable(
        &frontier,
        dag as &dyn crate::pipeline::memory::SchedulingHint,
    );
    assert_eq!(
        chosen, src_small,
        "headroom fit must prefer the Source that fits remaining headroom over a lower-index \
         Source that would overflow it"
    );
}
