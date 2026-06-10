//! Memory-aware dispatch-order selection.
//!
//! `scheduled_pass_order` is the production scheduler the executor's
//! dispatch loop calls: it resolves the order nodes are dispatched in by
//! asking [`MemoryArbitrator::next_runnable`] which runnable node to take
//! next, instead of walking `topo_order` blindly. These tests pin its
//! load-bearing behaviors against a real compiled [`ExecutionPlanDag`] and
//! a real arbitrator (no mock hint):
//!
//! - With no volume estimates every node predicts `0` bytes, so selection
//!   collapses to lowest topo index and the resolved order is byte-for-byte
//!   `topo_order` — the "no-estimates == today's order" determinism floor.
//! - With real file-size estimates the immediate freed-on-complete tier takes
//!   a ready blocking Aggregate over a lower-indexed fresh Source (reclaiming
//!   its accumulated state before the next buffer charges) — outranking even
//!   the Source's larger downstream subtree reclaim; the subtree-reclaim tier
//!   elects the heavy chain's Source ahead of a lower-indexed light Source
//!   when neither frees anything immediately; and the headroom-fit tier takes
//!   a fitting node over a lower-indexed one that would overflow the budget.
//!   Each is a genuine reorder away from stable topo index.

use super::*;
use crate::pipeline::memory::MemoryArbitrator;
use clinker_plan::config::{CompileContext, parse_config};
use petgraph::graph::NodeIndex;
use std::collections::HashSet;
use std::io::{Cursor, Write};
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

fn compile_dag(yaml: &str, anchor: &Path) -> clinker_plan::plan::compiled::CompiledPlan {
    let config = parse_config(yaml).expect("parse");
    let ctx = CompileContext::with_pipeline_dir(anchor, "");
    config.compile(&ctx).expect("compile")
}

fn node_idx(dag: &clinker_plan::plan::execution::ExecutionPlanDag, name: &str) -> NodeIndex {
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

    let order = scheduled_pass_order(dag, &arbitrator, &all, &std::collections::HashMap::new());
    assert_eq!(
        order, dag.topo_order,
        "with no estimates the scheduler must reproduce topo order exactly"
    );
}

/// The immediate freed-on-complete tier reorders away from stable topo
/// index AND outranks a larger downstream subtree reclaim: when the runnable
/// frontier offers BOTH a fresh Source (frees nothing the instant it drains,
/// but whose downstream Aggregate will eventually reclaim a large state) and
/// a blocking Aggregate ready to drain (frees its accumulated state NOW),
/// `next_runnable` takes the ready Aggregate even though the Source has the
/// lower topo index AND the larger subtree reclaim. Draining the ready
/// Aggregate reclaims headroom this step, which is the peak-minimizing
/// choice; chasing the Source's larger-but-eventual reclaim would instead
/// charge its buffer while the ready Aggregate's state still sits.
///
/// The heavy Source's chain (`large.csv`, 512 rows) accumulates strictly
/// more than the light chain's Aggregate (`small.csv`, 64 rows), so the
/// up-propagated subtree reclaim on `src_large` exceeds `agg_small`'s
/// immediate freed — making this the exact case the immediate-before-subtree
/// tier ordering exists to resolve.
///
/// This drives `next_runnable` directly with a frontier the executor's
/// dispatch loop produces mid-walk (the same call `scheduled_pass_order`
/// makes each step), pinning the selection rule on the real plan's
/// predictions rather than a mock hint.
#[test]
fn immediate_freed_outranks_larger_subtree_reclaim_of_a_fresh_source() {
    let tmp = tempfile::tempdir().unwrap();
    let small_len = write_csv(tmp.path(), "small.csv", 64);
    let large_len = write_csv(tmp.path(), "large.csv", 512);
    assert!(small_len > 0 && large_len > small_len);

    let plan = compile_dag(TWO_CHAIN_YAML, tmp.path());
    let dag = plan.dag();

    let agg_small = node_idx(dag, "agg_small");
    let src_large = node_idx(dag, "src_large");

    // Premise: the ready blocking Aggregate frees its accumulated input the
    // instant it drains; the fresh Source frees NOTHING immediately but its
    // chain unlocks a strictly LARGER eventual reclaim (the heavy chain's
    // Aggregate state propagated up). The Source also sorts earlier in topo.
    let agg_small_immediate = dag.node_properties[&agg_small].predicted_freed_bytes_on_complete;
    let src_large_immediate = dag.node_properties[&src_large].predicted_freed_bytes_on_complete;
    let src_large_subtree = dag.node_properties[&src_large].predicted_subtree_reclaim_bytes;
    assert!(
        agg_small_immediate > 0,
        "agg_small must predict freeing its accumulated input the instant it drains"
    );
    assert_eq!(
        src_large_immediate, 0,
        "a Source frees nothing the instant it drains"
    );
    assert!(
        src_large_subtree > agg_small_immediate,
        "the heavy chain's Source must carry a larger up-propagated subtree reclaim \
         ({src_large_subtree}) than the light Aggregate's immediate freed \
         ({agg_small_immediate}); otherwise this scenario does not exercise the \
         immediate-before-subtree tier ordering"
    );
    let topo_pos = |idx: NodeIndex| dag.topo_order.iter().position(|&n| n == idx).unwrap();

    let arbitrator =
        MemoryArbitrator::with_policy(512 * 1024 * 1024, 0.80, MemoryArbitrator::default_policy());

    // Frontier as the dispatch loop sees it once src_small is done and
    // agg_small has become runnable while src_large is still waiting.
    let frontier = [src_large, agg_small];
    let chosen = arbitrator.next_runnable(
        &frontier,
        dag as &dyn clinker_plan::plan::scheduling_hint::SchedulingHint,
    );

    assert_eq!(
        chosen, agg_small,
        "immediate freed (drain-now reclaim) must drain the ready Aggregate before charging the \
         fresh Source, even though the Source has the lower topo index and the larger eventual \
         subtree reclaim"
    );
    // Confirm this is a genuine reorder: lowest-stable-index alone would have
    // taken the Source.
    assert!(
        topo_pos(src_large) < topo_pos(agg_small),
        "premise: src_large has the lower topo index, so picking agg_small is a real reorder"
    );
}

/// Two independent `Source -> Aggregate -> Output` chains where the LIGHT
/// chain sorts first in `topo_order`, so a blind topo walk (and the
/// no-estimates determinism floor) would dispatch the light Source first.
/// Heavy-first dispatch therefore requires the scheduler to reorder against
/// stable topo index. The light chain is declared LAST because the
/// topological sort emits the most-recently-declared source first, so
/// declaring `src_light` last places it at topo index 0.
const HEAVY_FIRST_YAML: &str = r#"
pipeline:
  name: heavy_first
nodes:
  - type: source
    name: src_heavy
    config:
      name: src_heavy
      type: csv
      path: heavy.csv
      schema:
        - { name: k, type: string }
        - { name: v, type: int }
  - type: source
    name: src_light
    config:
      name: src_light
      type: csv
      path: light.csv
      schema:
        - { name: k, type: string }
        - { name: v, type: int }
  - type: aggregate
    name: agg_heavy
    input: src_heavy
    config:
      group_by: [k]
      cxl: |
        emit k = k
        emit total = sum(v)
  - type: aggregate
    name: agg_light
    input: src_light
    config:
      group_by: [k]
      cxl: |
        emit k = k
        emit total = sum(v)
  - type: output
    name: out_heavy
    input: agg_heavy
    config:
      name: out_heavy
      type: csv
      path: out_heavy.csv
      include_unmapped: true
  - type: output
    name: out_light
    input: agg_light
    config:
      name: out_light
      type: csv
      path: out_light.csv
      include_unmapped: true
"#;

/// The subtree-reclaim tier elects the heavy chain's Source first even
/// though the light chain's Source sorts earlier in topo order: at the start
/// of the walk both Sources are runnable and free nothing the instant they
/// drain (immediate freed `0`), and a no-estimates walk would take the
/// lower-topo-index light Source. The up-propagated subtree reclaim breaks
/// that tie toward the heavy Source — its downstream Aggregate accumulates
/// strictly more, so finishing its chain frees the most capacity.
/// Front-loading the heavy chain is the whole point: it drains and releases
/// its large state before the light chain's output has to coexist with it.
///
/// This exercises the full-DAG `scheduled_pass_order` walk (not a single
/// `next_runnable` frontier), so it pins the production dispatch order the
/// executor resolves.
#[test]
fn scheduler_elects_heavy_chain_source_before_lower_index_light_source() {
    let tmp = tempfile::tempdir().unwrap();
    let light_len = write_csv(tmp.path(), "light.csv", 64);
    let heavy_len = write_csv(tmp.path(), "heavy.csv", 512);
    assert!(light_len > 0 && heavy_len > light_len);

    let plan = compile_dag(HEAVY_FIRST_YAML, tmp.path());
    let dag = plan.dag();

    let src_light = node_idx(dag, "src_light");
    let src_heavy = node_idx(dag, "src_heavy");

    // Premise: the light Source sorts earlier (lower topo index), both
    // Sources free nothing the instant they drain, yet the heavy chain's
    // Source carries the larger up-propagated subtree reclaim — so electing
    // it first is a genuine reorder away from the determinism floor.
    let topo_pos = |idx: NodeIndex| dag.topo_order.iter().position(|&n| n == idx).unwrap();
    assert!(
        topo_pos(src_light) < topo_pos(src_heavy),
        "the light Source must sort earlier in topo order so heavy-first is a real reorder"
    );
    assert_eq!(
        dag.node_properties[&src_light].predicted_freed_bytes_on_complete, 0,
        "a Source frees nothing the instant it drains"
    );
    assert_eq!(
        dag.node_properties[&src_heavy].predicted_freed_bytes_on_complete, 0,
        "a Source frees nothing the instant it drains"
    );
    assert!(
        dag.node_properties[&src_heavy].predicted_subtree_reclaim_bytes
            > dag.node_properties[&src_light].predicted_subtree_reclaim_bytes,
        "the heavy chain's Source must carry the larger up-propagated subtree reclaim"
    );

    // A huge budget so the headroom-fit tier never fires: both Sources fit,
    // so selection turns purely on the freed tiers — exactly the regime the
    // task specifies (no spill, no back-pressure, fully decoupled from RSS).
    let arbitrator = huge_budget_arbitrator();
    let all: HashSet<NodeIndex> = dag.topo_order.iter().copied().collect();
    let order = scheduled_pass_order(dag, &arbitrator, &all, &std::collections::HashMap::new());
    let pos = |idx: NodeIndex| order.iter().position(|&n| n == idx).unwrap();

    assert!(
        pos(src_heavy) < pos(src_light),
        "the scheduler must elect the heavy chain's Source before the lower-topo-index light \
         Source (dispatch positions: heavy={}, light={}), front-loading the chain whose \
         completion frees the most capacity",
        pos(src_heavy),
        pos(src_light),
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
        dag as &dyn clinker_plan::plan::scheduling_hint::SchedulingHint,
    );
    assert_eq!(
        chosen, src_small,
        "headroom fit must prefer the Source that fits remaining headroom over a lower-index \
         Source that would overflow it"
    );
}

/// Hard limit large enough that real process RSS can never push the
/// arbitrator's `peak_rss` above the soft limit, so `should_spill()` never
/// fires for any of these runs. With spill and back-pressure structurally
/// out of reach, the consumer accounting these tests inspect is fully
/// decoupled from process-global RSS: the registry's post-run state is the
/// same under a quiet machine and under a saturated parallel `cargo test`,
/// making the assertions below deterministic. 100 GiB mirrors the
/// huge-budget convention the largest-first arbitration tests use for the
/// same reason.
const HUGE_HARD_LIMIT_BYTES: u64 = 100 * 1024 * 1024 * 1024;

fn huge_budget_arbitrator() -> std::sync::Arc<MemoryArbitrator> {
    std::sync::Arc::new(MemoryArbitrator::with_policy(
        HUGE_HARD_LIMIT_BYTES,
        0.80,
        MemoryArbitrator::default_policy(),
    ))
}

/// Run a single-source, single-output pipeline against a caller-owned
/// arbitrator so the test can read the consumer registry after the run
/// returns. The shared `run_test` helper hides its arbitrator behind the
/// public entry point; this threads an `Arc<MemoryArbitrator>` the caller
/// keeps a clone of, which is what makes the post-run registry observable.
fn run_with_arbitrator(
    yaml: &str,
    csv_input: &str,
    arbitrator: std::sync::Arc<MemoryArbitrator>,
) -> crate::executor::ExecutionReport {
    let config = parse_config(yaml).expect("parse");
    let primary = config.source_configs().next().unwrap().name.clone();
    let readers: crate::executor::SourceReaders = HashMap::from([(
        primary,
        crate::executor::single_file_reader(
            "test.csv",
            Box::new(Cursor::new(csv_input.as_bytes().to_vec())),
        ),
    )]);
    let output_buf = clinker_bench_support::io::SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(output_buf) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "test-exec-id".to_string(),
        batch_id: "test-batch-id".to_string(),
        pipeline_vars: IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    PipelineExecutor::run_with_readers_writers_with_arbitrator(
        &config,
        readers,
        writers.into(),
        &params,
        CompileContext::default(),
        arbitrator,
    )
    .expect("pipeline executes")
}

/// The Source ingest consumer is the only consumer a single-source run
/// intentionally leaves registered for the arbitrator's lifetime (its
/// registration id is discarded at the Source dispatch arm, never
/// unregistered). Every other consumer — Aggregate wrappers included — must
/// be gone once the run drains.
const SURVIVING_SOURCE_CONSUMERS: usize = 1;

/// A single `Source -> Aggregate -> Output` chain whose `collect`
/// accumulator folds each group's values into an `Array`, charging a
/// non-zero `value_heap_bytes` into the Aggregate's `ConsumerHandle` while
/// the strict aggregate runs — concrete state the registry must stop
/// reporting the moment the aggregate finalizes.
const STRICT_AGG_YAML: &str = r#"
pipeline:
  name: strict_agg_consumer_release
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: k, type: string }
        - { name: v, type: int }
  - type: aggregate
    name: agg
    input: src
    config:
      group_by: [k]
      cxl: |
        emit k = k
        emit items = collect(v)
  - type: output
    name: out
    input: agg
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

/// A finalized strict Aggregate must unregister its memory consumer, so its
/// accumulated group state stops contributing to the arbitrator's reported
/// usage for the rest of the run.
///
/// The strict dispatch arm registers an `AggregateConsumer` while the
/// aggregate folds, then `finalize` consumes the stream and emits every
/// group — at which point the working set is gone. Without the
/// arm-exit unregister, the wrapper lingers in the registry and keeps
/// charging the finalized `collect` arrays into `sum_consumer_usage` until
/// the arbitrator itself drops at run teardown, so the reported peak
/// becomes retain-forever rather than reflecting only concurrently-live
/// state. The huge budget keeps `should_spill()` from ever firing, so this
/// release is the only thing the registry state depends on — no RSS
/// coupling, deterministic under parallel `cargo test`.
#[test]
fn finalized_strict_aggregate_releases_its_consumer() {
    let arbitrator = huge_budget_arbitrator();

    let csv = "k,v\n\
        alpha,1\n\
        beta,2\n\
        alpha,3\n\
        gamma,4\n\
        beta,5\n";
    let report = run_with_arbitrator(STRICT_AGG_YAML, csv, std::sync::Arc::clone(&arbitrator));
    assert_eq!(report.counters.dlq_count, 0, "no records should DLQ");

    // The strict aggregate charged a non-zero footprint at its peak (the
    // `collect` arrays) — the premise that makes "release" observable.
    assert!(
        report.peak_consumer_usage_bytes > 0,
        "the strict aggregate must have charged a non-zero footprint at peak"
    );

    // Only the Source ingest consumer may outlive the run. A regression
    // that drops the arm-exit unregister leaves two consumers, with the
    // finalized aggregate still charging its `collect` arrays.
    assert_eq!(
        arbitrator.consumer_count(),
        SURVIVING_SOURCE_CONSUMERS,
        "after the Aggregate finalized, only the Source consumer should remain registered — the \
         finalized AggregateConsumer must have been unregistered (a regression leaves two)"
    );
    // The accounting check: post-run registered usage is strictly below
    // the in-flight peak, because that peak summed the Source ingest
    // consumer AND the live aggregate together while the aggregate folded,
    // whereas only the Source consumer survives the drain. A regression
    // that retained the finalized aggregate would keep its bytes summed in,
    // so post-run usage would instead match the peak rather than fall below
    // it.
    let post_run_usage = arbitrator.sum_consumer_usage();
    assert!(
        post_run_usage < report.peak_consumer_usage_bytes,
        "post-run sum_consumer_usage ({post_run_usage}) is not below the in-flight peak ({}) — the \
         finalized aggregate's bytes appear retained in the registry rather than released",
        report.peak_consumer_usage_bytes,
    );
}

/// A tumbling time-windowed Aggregate fans out into one registered consumer
/// per live window; each must unregister as its window finalizes, so no
/// per-window group state survives the run in the registry.
const WINDOWED_AGG_YAML: &str = r#"
pipeline:
  name: windowed_agg_consumer_release
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      watermark:
        column: event_ts
      schema:
        - { name: k, type: string }
        - { name: v, type: int }
        - { name: event_ts, type: date_time }
  - type: aggregate
    name: agg
    input: src
    config:
      group_by: [k]
      time_window:
        tumbling: { size: 1h }
      cxl: |
        emit k = k
        emit items = collect(v)
  - type: output
    name: out
    input: agg
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

/// Every per-window stream of a time-windowed Aggregate registers its own
/// consumer, and each must be unregistered as its window finalizes. The
/// fixture spreads records across three distinct tumbling-hour buckets, so
/// a regression that unregistered only the last window — or none — would
/// leave extra consumers registered after the run. The huge budget keeps
/// the per-window registration path off the spill/back-pressure path, so
/// the surviving-consumer count is RSS-independent and deterministic.
#[test]
fn finalized_time_windowed_aggregate_releases_every_window_consumer() {
    let arbitrator = huge_budget_arbitrator();

    let csv = "k,v,event_ts\n\
        alpha,1,2024-01-01T00:10:00\n\
        alpha,2,2024-01-01T00:40:00\n\
        beta,3,2024-01-01T01:15:00\n\
        alpha,4,2024-01-01T02:05:00\n\
        beta,5,2024-01-01T02:50:00\n";
    let report = run_with_arbitrator(WINDOWED_AGG_YAML, csv, std::sync::Arc::clone(&arbitrator));
    assert_eq!(report.counters.dlq_count, 0, "no records should DLQ");

    assert_eq!(
        arbitrator.consumer_count(),
        SURVIVING_SOURCE_CONSUMERS,
        "every per-window AggregateConsumer must be unregistered as its window finalizes — only \
         the Source consumer should outlive the run; a regression that released only the last \
         window (or none) leaves extra consumers registered"
    );
}

/// A relaxed-CK pipeline: the `correlation_key` Source feeds an Aggregate
/// whose totals an analytic-window Transform divides by `(total - 30)`. The
/// HR group totals 30, so its ratio divides by zero, routes to the DLQ, and
/// drives the retraction + recompute path. That path keeps the Aggregate's
/// `HashAggregator` parked on `relaxed_aggregator_states` so the commit
/// phase can retract and re-finalize against it, unlike a strict aggregate
/// that finalizes and discards in one step.
const RELAXED_CK_AGG_YAML: &str = r#"
pipeline:
  name: relaxed_ck_agg_consumer_release
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: department, type: string }
        - { name: amount, type: int }
  - type: aggregate
    name: dept_totals
    input: src
    config:
      group_by: [department]
      cxl: |
        emit department = department
        emit total = sum(amount)
  - type: transform
    name: running
    input: dept_totals
    config:
      cxl: |
        emit department = department
        emit total = total
        emit ratio = 1 / (total - 30)
      analytic_window:
        group_by: [department]
  - type: output
    name: out
    input: running
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

/// A relaxed-CK Aggregate parks its `HashAggregator` for the commit phase's
/// retract + re-finalize iterations, so its memory consumer must stay
/// registered while that state is live and be unregistered the moment it is
/// dropped — not retained for the rest of the run.
///
/// The consumer's lifetime is tied to `relaxed_aggregator_states`: a
/// successfully-finalized relaxed aggregate releases its consumer at the
/// top-scope teardown that drains that map after the walk; a degraded one
/// releases it at the `remove` that discards the aggregator mid-run. Either
/// way no `AggregateConsumer` survives the run, so only the Source consumer
/// remains. A regression that drops the parked aggregator without
/// unregistering its consumer would leave the relaxed aggregate's
/// `value_heap_bytes` summed into `sum_consumer_usage` for the rest of the
/// run, with an extra consumer in the registry. The huge budget keeps the
/// run off the spill/back-pressure path entirely, so the result is
/// deterministic regardless of real RSS under parallel `cargo test`.
#[test]
fn finalized_relaxed_ck_aggregate_releases_its_consumer() {
    let arbitrator = huge_budget_arbitrator();

    // HR totals 30 -> `1 / (30 - 30)` divides by zero -> DLQ -> the
    // retraction/recompute path exercises the parked relaxed aggregator.
    let csv = "order_id,department,amount\n\
        O1,HR,10\n\
        O2,HR,20\n\
        O3,ENG,100\n\
        O4,ENG,200\n";
    let report = run_with_arbitrator(RELAXED_CK_AGG_YAML, csv, std::sync::Arc::clone(&arbitrator));
    assert!(
        report.counters.dlq_count > 0,
        "the divide-by-zero on the HR group must route to the DLQ, driving the relaxed retraction \
         path that parks the aggregator on relaxed_aggregator_states"
    );

    assert_eq!(
        arbitrator.consumer_count(),
        SURVIVING_SOURCE_CONSUMERS,
        "after the run drains, the relaxed Aggregate's consumer must be unregistered — only the \
         Source consumer should outlive the run; a regression that drops the parked aggregator \
         without unregistering its consumer leaves its accumulated bytes summed into \
         sum_consumer_usage for the rest of the run"
    );
}
