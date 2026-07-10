//! Pipeline-level coverage for the pure-range IEJoin block-band path.
//!
//! `CombineStrategy::IEJoin` (dispatched as `IEJoin(None)`) runs the bounded
//! block-band path: external-sort each side on the primary inequality key,
//! slice the merged stream into min/max-tagged blocks, prune non-overlapping
//! block-pairs, and run the kernel per surviving pair. These tests drive that
//! path end-to-end through the public executor entry point.
//!
//! Three guarantees are pinned:
//!
//!   1. A pure-range join over a budget that fits the block-pair working set
//!      COMPLETES with correct results (it used to abort with no spill path),
//!      and its consumer is unregistered on the clean exit.
//!   2. The same shape over a fully-pruned (disjoint-range) dataset COMPLETES
//!      under a tight budget while spilling — `per_stage_spill_bytes` for the
//!      node is non-zero — because every block-pair is pruned before the
//!      pre-output charge, so the RSS-sensitive abort is never reached.
//!   3. A non-empty join whose single block-pair plus kernel aux cannot fit
//!      the budget aborts with a typed `MemoryBudgetExceeded { source: Arena,
//!      detail: "iejoin pre-output ..." }` — the genuine last resort — and
//!      still unregisters its consumer on the error path.
//!
//! A tight budget over a non-empty pure-range join necessarily trips the
//! pre-output gate on an RSS-present host (the budget small enough to force
//! spill sits below process RSS), so the "completes while spilling" guarantee
//! is pinned with the fully-pruned dataset in case 2; per-block spill during a
//! completing NON-empty join is exercised directly by the block-module tests,
//! which force it with a test-only block-target override and a budget above
//! RSS.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use clinker_plan::plan::combine::CombineStrategy;
use clinker_plan::plan::execution::PlanNode;
use std::collections::HashMap;
use std::sync::Arc;

/// A tight hard limit far below process RSS. Over a non-empty join it trips the
/// pre-output gate; over a fully-pruned dataset the gate is never reached, so
/// the run completes while the drain and block slicing spill to disk.
const TIGHT_LIMIT: u64 = 32 * 1024;
/// A hard limit comfortably above process RSS, so a small non-empty join
/// completes without tripping the RSS-sensitive abort.
const ROOMY_LIMIT: u64 = 512 * 1024 * 1024;
const SPILL_FRAC: f64 = 0.80;

/// Arbitrator with the given hard limit and `NoOpPolicy` so no victim is ever
/// paused or asked to spill — the block-band path spills on its own byte
/// threshold. `peak_rss` is left unseeded; the real reading governs the
/// pre-output gate.
fn no_op_arbitrator(limit: u64) -> Arc<crate::pipeline::memory::MemoryArbitrator> {
    Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
        limit,
        SPILL_FRAC,
        0.70,
        Box::new(crate::pipeline::memory::NoOpPolicy),
    ))
}

/// Pure-range predicate (two range conjuncts, no equality) so the planner
/// selects `CombineStrategy::IEJoin` and the runtime runs the block-band path.
const PIPELINE_YAML: &str = r#"
pipeline:
  name: iejoin_block_band
nodes:
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: orders.csv
    schema:
      - { name: order_id, type: string }
      - { name: amount, type: int }
- type: source
  name: bands
  config:
    name: bands
    type: csv
    path: bands.csv
    schema:
      - { name: band_id, type: string }
      - { name: lo, type: int }
      - { name: hi, type: int }
- type: combine
  name: banded
  input:
    orders: orders
    bands: bands
  config:
    where: "orders.amount >= bands.lo and orders.amount < bands.hi"
    match: first
    on_miss: skip
    cxl: |
      emit order_id = orders.order_id
      emit amount = orders.amount
      emit band_id = bands.band_id
    propagate_ck: driver
- type: output
  name: out
  input: banded
  config:
    name: out
    type: csv
    path: out.csv
"#;

/// Compile `PIPELINE_YAML` and return the strategy stamped on the `banded`
/// combine node — the guard that keeps the test honest about exercising the
/// pure-range block-band branch.
fn banded_strategy() -> CombineStrategy {
    let config = clinker_plan::config::parse_config(PIPELINE_YAML).expect("parse pipeline YAML");
    let validated = config
        .compile(&clinker_plan::config::CompileContext::default())
        .expect("compile pipeline");
    let dag = validated.dag();
    for idx in dag.graph.node_indices() {
        if let PlanNode::Combine { name, strategy, .. } = &dag.graph[idx]
            && name == "banded"
        {
            return strategy.clone();
        }
    }
    panic!("combine node \"banded\" not present in compiled plan");
}

/// CSV for `n` orders whose amounts are `2*i`.
fn orders_csv(n: usize) -> String {
    let mut s = String::from("order_id,amount\n");
    for i in 0..n {
        s.push_str(&format!("o{i},{}\n", i * 2));
    }
    s
}

/// CSV for `n` bands. `offset` shifts the band ranges: `0` makes band `i` cover
/// `[2*i, 2*i+2)` so order `i` falls in it; a large offset makes every band
/// disjoint from every order amount so the whole join prunes away.
fn bands_csv(n: usize, offset: i64) -> String {
    let mut s = String::from("band_id,lo,hi\n");
    for i in 0..n {
        let lo = offset + (i as i64) * 2;
        s.push_str(&format!("b{i},{},{}\n", lo, lo + 2));
    }
    s
}

/// Run `PIPELINE_YAML` over the given CSV inputs against `arb`, returning the
/// run result and the captured output CSV.
fn run_pipeline(
    orders: String,
    bands: String,
    arb: &Arc<crate::pipeline::memory::MemoryArbitrator>,
) -> (Result<(), PipelineError>, String) {
    let readers: crate::executor::SourceReaders = HashMap::from([
        (
            "orders".to_string(),
            crate::executor::single_file_reader(
                "orders.csv",
                Box::new(std::io::Cursor::new(orders.into_bytes())),
            ),
        ),
        (
            "bands".to_string(),
            crate::executor::single_file_reader(
                "bands.csv",
                Box::new(std::io::Cursor::new(bands.into_bytes())),
            ),
        ),
    ]);

    let out = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let config = clinker_plan::config::parse_config(PIPELINE_YAML).expect("parse pipeline YAML");
    let params = PipelineRunParams {
        execution_id: "iejoin-block-band".to_string(),
        batch_id: "batch-0".to_string(),
        ..Default::default()
    };

    let result = PipelineExecutor::run_with_readers_writers_with_arbitrator(
        &config,
        readers,
        writers.into(),
        &params,
        clinker_plan::config::CompileContext::default(),
        Arc::clone(arb),
    );
    (result.map(|_report| ()), out.as_string())
}

#[test]
fn pure_range_selects_block_band_strategy() {
    assert!(
        matches!(banded_strategy(), CombineStrategy::IEJoin),
        "the pure-range predicate must select the block-band IEJoin strategy"
    );
}

#[test]
fn block_band_completes_non_empty_join_and_releases_consumer() {
    // 500 orders × 500 overlapping bands: order i (amount 2*i) falls exactly in
    // band i. A budget above process RSS fits the block-pair working set, so
    // the join that used to have no spill path now completes.
    let arb = no_op_arbitrator(ROOMY_LIMIT);
    let (result, output) = run_pipeline(orders_csv(500), bands_csv(500, 0), &arb);
    result.expect("the pure-range block-band join must complete under a roomy budget");

    let data_lines: Vec<&str> = output.lines().filter(|l| !l.is_empty()).skip(1).collect();
    assert_eq!(
        data_lines.len(),
        500,
        "every order falls in exactly one band, so first-match emits one row each"
    );
    // Spot-check the banding: order o0 → band b0, order o499 → band b499.
    assert!(
        data_lines.iter().any(|l| l.starts_with("o0,0,b0")),
        "order o0 (amount 0) must band to b0; got rows like {:?}",
        &data_lines[..data_lines.len().min(3)]
    );
    assert!(
        data_lines.iter().any(|l| l.starts_with("o499,998,b499")),
        "order o499 (amount 998) must band to b499"
    );

    assert_eq!(
        arb.consumer_count(),
        0,
        "the block-band consumer must be unregistered on the clean exit"
    );
}

#[test]
fn block_band_completes_while_spilling_when_fully_pruned() {
    // 500 orders × 500 bands, but every band is shifted far above every order
    // amount, so `possible(Gt/Ge, ...)` prunes every block-pair before the
    // pre-output charge. Under a tight budget the drain and block slicing spill
    // to disk, yet the run completes (empty result) because the RSS-sensitive
    // gate is never reached.
    let arb = no_op_arbitrator(TIGHT_LIMIT);
    let (result, output) = run_pipeline(orders_csv(500), bands_csv(500, 1_000_000), &arb);
    result.expect("a fully-pruned pure-range join must complete even under a tight budget");

    let data_lines = output.lines().filter(|l| !l.is_empty()).skip(1).count();
    assert_eq!(
        data_lines, 0,
        "disjoint bands match nothing, so first-match + on_miss:skip emit no rows"
    );

    let spilled = arb
        .per_stage_spill_bytes()
        .get("banded")
        .copied()
        .unwrap_or(0);
    assert!(
        spilled > 0,
        "the 500-row sides must spill their sort runs and blocks under the tight budget; \
         per_stage_spill_bytes[banded] was {spilled}"
    );
    assert_eq!(
        arb.consumer_count(),
        0,
        "the block-band consumer must be unregistered after the completing run"
    );
}

#[test]
fn block_band_pre_output_state_aborts_under_tight_budget() {
    // 500 orders × 500 overlapping bands under the tight budget: every order
    // matches its band, so a block-pair survives the prune and reaches the
    // pre-output charge. That charge (two resident blocks plus the kernel's
    // sort arrays) cannot fit the budget, so the run aborts with the typed
    // pre-output error rather than blowing the ceiling.
    let arb = no_op_arbitrator(TIGHT_LIMIT);
    let (result, output) = run_pipeline(orders_csv(500), bands_csv(500, 0), &arb);
    let err = result.expect_err("the block-pair working set over the tight budget must abort");

    match err {
        PipelineError::MemoryBudgetExceeded {
            node,
            used,
            limit,
            source,
            detail,
        } => {
            assert_eq!(node, "banded", "the abort must name the combine node");
            assert_eq!(
                source,
                clinker_plan::BudgetCategory::Arena,
                "block-band pre-output state is arena-class memory"
            );
            assert_eq!(
                limit, TIGHT_LIMIT,
                "the reported limit must be the hard budget"
            );
            assert!(
                used > TIGHT_LIMIT,
                "the reported estimate ({used}) must exceed the budget ({TIGHT_LIMIT})"
            );
            let detail = detail.expect("the pre-output abort must carry a detail string");
            assert!(
                detail.contains("iejoin pre-output"),
                "the abort must come from the block-band pre-output gate; got: {detail:?}"
            );
        }
        other => panic!("expected MemoryBudgetExceeded from the pre-output gate; got: {other:?}"),
    }

    assert!(
        output.lines().filter(|l| !l.is_empty()).count() <= 1,
        "the run aborted before emitting any matched row, so the sink holds at most a header; \
         got: {output:?}"
    );

    assert_eq!(
        arb.consumer_count(),
        0,
        "the block-band consumer must be unregistered even when the branch aborts"
    );
}
