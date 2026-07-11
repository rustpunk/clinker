//! Pipeline-level coverage for the pure-range IEJoin block-band path.
//!
//! `CombineStrategy::IEJoin` (dispatched as `IEJoin(None)`) runs the bounded
//! block-band path: external-sort each side on the primary inequality key,
//! slice the merged stream into min/max-tagged blocks, prune non-overlapping
//! block-pairs, and run the kernel per surviving pair. These tests drive that
//! path end-to-end through the public executor entry point.
//!
//! The load-bearing guarantee: an input that USED to abort (no spill path) now
//! COMPLETES with correct NON-EMPTY results while spilling, under a budget far
//! below process RSS. That is only possible because the per-pair pre-output
//! abort is a strictly LOCAL check — the one block-pair's resident bytes plus
//! kernel aux against the hard limit — and never consults global process
//! pressure (RSS or the consumer-usage sum). The scan/sort/slice phases answer
//! pressure by spilling. The un-spillable output axis keeps the every-10K
//! global-aware poll, so these fixtures hold their match sets under 10K.
//!
//! Cases:
//!   1. Tight budget, non-empty, wide input forcing multi-run sort spill and
//!      multiple blocks per side → COMPLETES with correct rows, non-zero
//!      per-stage spill, consumer released. (Host-independent: passes on an
//!      RSS-present host precisely because the gate is local.)
//!   2. Roomy budget, non-empty → completes fully resident with zero spill.
//!   3. Tight budget, fully pruned (disjoint ranges) → completes empty while
//!      spilling — the pipeline-level prune observation.
//!   4. Budget below a single block-pair's local footprint → aborts with the
//!      typed pre-output error via the LOCAL arm, consumer released.
//!   5. The equi+range (HashPartitionIEJoin) sibling path, whose pre-output gate
//!      is instead the RSS-independent global-aware check, aborts under a tight
//!      budget with the consumer released — the coverage the block-band rewrite
//!      would otherwise have left untested.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use clinker_plan::plan::combine::CombineStrategy;
use clinker_plan::plan::execution::PlanNode;
use std::collections::HashMap;
use std::sync::Arc;

/// A tight hard limit far below process RSS, but above the local footprint of a
/// single block-pair (two 16 KiB-floored blocks plus kernel aux). A non-empty
/// join completes under it because the pre-output gate is strictly local.
const TIGHT_LIMIT: u64 = 128 * 1024;
/// Below a single block-pair's local footprint (two 16 KiB block floors plus
/// aux), so the first surviving pair trips the local pre-output abort.
const ABORT_LIMIT: u64 = 8 * 1024;
/// Comfortably above process RSS, for the resident/degenerate completion path.
const ROOMY_LIMIT: u64 = 512 * 1024 * 1024;
const SPILL_FRAC: f64 = 0.80;

/// Arbitrator with the given hard limit and `NoOpPolicy` so no victim is ever
/// paused or asked to spill — the block-band path spills on its own byte
/// threshold. `peak_rss` is left unseeded; the real reading would only matter
/// on the output poll, which these fixtures keep under its 10K cadence.
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
/// The `pad` column widens each input record without appearing in the output,
/// so a side can exceed the sort-spill threshold while the emitted rows stay
/// small (keeping the match set under the 10K output poll).
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
      - { name: pad, type: string }
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
      - { name: pad, type: string }
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

/// CSV for `n` orders whose amounts are `2*i`, each padded with `pad_len` bytes.
fn orders_csv(n: usize, pad_len: usize) -> String {
    let pad = "x".repeat(pad_len);
    let mut s = String::from("order_id,amount,pad\n");
    for i in 0..n {
        s.push_str(&format!("o{i},{},{pad}\n", i * 2));
    }
    s
}

/// CSV for `n` bands, each padded with `pad_len` bytes. `offset` shifts the band
/// ranges: `0` makes band `i` cover `[2*i, 2*i+2)` so order `i` falls in it; a
/// large offset makes every band disjoint from every order so the join prunes
/// away entirely.
fn bands_csv(n: usize, offset: i64, pad_len: usize) -> String {
    let pad = "x".repeat(pad_len);
    let mut s = String::from("band_id,lo,hi,pad\n");
    for i in 0..n {
        let lo = offset + (i as i64) * 2;
        s.push_str(&format!("b{i},{},{},{pad}\n", lo, lo + 2));
    }
    s
}

/// Run `PIPELINE_YAML` (the pure-range block-band pipeline) over the given CSV
/// inputs against `arb`, returning the run result and the captured output CSV.
fn run_pipeline(
    orders: String,
    bands: String,
    arb: &Arc<crate::pipeline::memory::MemoryArbitrator>,
) -> (Result<(), PipelineError>, String) {
    run_pipeline_yaml(PIPELINE_YAML, orders, bands, arb)
}

/// Run an arbitrary two-source (`orders` / `bands`) pipeline `yaml` over the
/// given CSV inputs against `arb`, returning the run result and captured output.
fn run_pipeline_yaml(
    yaml: &str,
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

    let config = clinker_plan::config::parse_config(yaml).expect("parse pipeline YAML");
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

fn spilled_bytes(arb: &Arc<crate::pipeline::memory::MemoryArbitrator>) -> u64 {
    arb.per_stage_spill_bytes()
        .get("banded")
        .copied()
        .unwrap_or(0)
}

#[test]
fn pure_range_selects_block_band_strategy() {
    assert!(
        matches!(banded_strategy(), CombineStrategy::IEJoin),
        "the pure-range predicate must select the block-band IEJoin strategy"
    );
}

#[test]
fn block_band_output_is_identical_across_memory_limits() {
    // The determinism invariant, end-to-end: the same data and pipeline run at a
    // tight budget (multi-run sort spill, many blocks per side) and a roomy
    // budget (fully resident) must produce byte-identical output. The tight run
    // spills and re-slices; the roomy run holds everything in RAM; the final
    // deterministic sort makes the emitted CSV the same regardless.
    let (tight_result, tight_out) = run_pipeline(
        orders_csv(400, 200),
        bands_csv(400, 0, 200),
        &no_op_arbitrator(TIGHT_LIMIT),
    );
    tight_result.expect("tight-budget run must complete");
    let (roomy_result, roomy_out) = run_pipeline(
        orders_csv(400, 200),
        bands_csv(400, 0, 200),
        &no_op_arbitrator(ROOMY_LIMIT),
    );
    roomy_result.expect("roomy-budget run must complete");

    assert_eq!(
        tight_out, roomy_out,
        "block-band output must be a pure function of the data, not of pipeline.memory.limit"
    );
    assert!(
        tight_out.lines().filter(|l| !l.is_empty()).count() > 1,
        "the fixture must emit rows for the comparison to be meaningful"
    );
}

#[test]
fn block_band_completes_non_empty_with_spill_under_tight_budget() {
    // 400 orders × 400 overlapping bands: order i (amount 2*i) falls exactly in
    // band i, so there are 400 matches — well under the 10K output poll. Each
    // input record carries a 200-byte pad column (not emitted), so each side is
    // ~140 KB: it exceeds the ~51 KB sort-spill threshold (multi-run sort spill)
    // and slices into many 16 KB blocks. Under the 128 KB budget a single
    // block-pair's local footprint (~two 16 KB blocks + kernel aux) fits, so the
    // run COMPLETES — the input that previously had no spill path now finishes,
    // and does so on this RSS-present host precisely because the per-pair gate
    // is local, not global.
    let arb = no_op_arbitrator(TIGHT_LIMIT);
    let (result, output) = run_pipeline(orders_csv(400, 200), bands_csv(400, 0, 200), &arb);
    result.expect("the wide-input pure-range join must complete under the tight local budget");

    let data_lines: Vec<&str> = output.lines().filter(|l| !l.is_empty()).skip(1).collect();
    assert_eq!(
        data_lines.len(),
        400,
        "every order falls in exactly one band, so first-match emits one row each"
    );
    assert!(
        data_lines.iter().any(|l| l.starts_with("o0,0,b0")),
        "order o0 (amount 0) must band to b0"
    );
    assert!(
        data_lines.iter().any(|l| l.starts_with("o399,798,b399")),
        "order o399 (amount 798) must band to b399"
    );
    assert!(
        spilled_bytes(&arb) > 0,
        "the ~140 KB sides must spill sort runs and blocks under the 128 KB budget; \
         per_stage_spill_bytes[banded] was {}",
        spilled_bytes(&arb)
    );
    assert_eq!(
        arb.consumer_count(),
        0,
        "the block-band consumer must be unregistered on the clean exit"
    );
}

#[test]
fn block_band_completes_non_empty_under_roomy_budget() {
    // The resident path: a roomy budget keeps each small side entirely in RAM
    // (no spill), and the non-empty join completes with correct results.
    let arb = no_op_arbitrator(ROOMY_LIMIT);
    let (result, output) = run_pipeline(orders_csv(500, 0), bands_csv(500, 0, 0), &arb);
    result.expect("the pure-range block-band join must complete under a roomy budget");

    let data_lines: Vec<&str> = output.lines().filter(|l| !l.is_empty()).skip(1).collect();
    assert_eq!(data_lines.len(), 500, "one first-match row per order");
    assert!(
        data_lines.iter().any(|l| l.starts_with("o499,998,b499")),
        "order o499 (amount 998) must band to b499"
    );
    // The no-spill property this test exists to cover, pinned: small sides fit
    // the resident budget, so nothing reaches disk.
    assert_eq!(
        spilled_bytes(&arb),
        0,
        "small sides must stay resident under a roomy budget; per_stage_spill_bytes[banded] was {}",
        spilled_bytes(&arb)
    );
    assert_eq!(
        arb.consumer_count(),
        0,
        "the block-band consumer must be unregistered on the clean exit"
    );
}

#[test]
fn block_band_completes_while_fully_pruned_and_spilling() {
    // 500 orders × 500 bands shifted far above every order amount, so every
    // block-pair is pruned before the pre-output charge. Under a tight budget
    // the drain and block slicing spill to disk, yet the run completes (empty
    // result) — the pipeline-level observation that pruning happens before any
    // per-pair work.
    let arb = no_op_arbitrator(TIGHT_LIMIT);
    let (result, output) = run_pipeline(orders_csv(500, 0), bands_csv(500, 1_000_000, 200), &arb);
    result.expect("a fully-pruned pure-range join must complete even under a tight budget");

    let data_lines = output.lines().filter(|l| !l.is_empty()).skip(1).count();
    assert_eq!(
        data_lines, 0,
        "disjoint bands match nothing, so first-match + on_miss:skip emit no rows"
    );
    assert!(
        spilled_bytes(&arb) > 0,
        "the wide build side must spill under the tight budget; got {}",
        spilled_bytes(&arb)
    );
    assert_eq!(
        arb.consumer_count(),
        0,
        "the block-band consumer must be unregistered after the completing run"
    );
}

#[test]
fn block_band_pre_output_local_abort_under_undersized_budget() {
    // 500 orders × 500 overlapping bands under a budget below a single
    // block-pair's local footprint: every order matches its band, so a pair
    // survives the prune and reaches the pre-output charge. Two 16 KB-floored
    // blocks plus the kernel's sort arrays exceed the 8 KB budget, so the LOCAL
    // gate trips — proving the demoted abort still fires when a lone block-pair
    // genuinely cannot fit, independent of process RSS.
    let arb = no_op_arbitrator(ABORT_LIMIT);
    let (result, output) = run_pipeline(orders_csv(500, 0), bands_csv(500, 0, 0), &arb);
    let err = result.expect_err("a block-pair over the undersized budget must abort");

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
                limit, ABORT_LIMIT,
                "the reported limit must be the hard budget"
            );
            assert!(
                used > ABORT_LIMIT,
                "the reported footprint ({used}) must exceed the budget ({ABORT_LIMIT})"
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

/// An equi+range predicate (one equality conjunct plus one range conjunct) so
/// the planner selects `CombineStrategy::HashPartitionIEJoin`, which holds its
/// hash partitions and per-group sort arrays resident with no spill path. Its
/// pre-output gate is the RSS-independent `should_abort_local` check on the
/// partition / group state — the coverage the deleted test guarded and that no
/// block-band test can exercise (the block-band path spills instead of
/// aborting under input pressure).
const EQUI_RANGE_YAML: &str = r#"
pipeline:
  name: iejoin_equi_range
nodes:
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: orders.csv
    schema:
      - { name: region, type: string }
      - { name: amount, type: int }
- type: source
  name: bands
  config:
    name: bands
    type: csv
    path: bands.csv
    schema:
      - { name: region, type: string }
      - { name: lo, type: int }
- type: combine
  name: banded
  input:
    orders: orders
    bands: bands
  config:
    where: "orders.region == bands.region and orders.amount >= bands.lo"
    match: all
    on_miss: skip
    cxl: |
      emit region = orders.region
      emit amount = orders.amount
      emit lo = bands.lo
    propagate_ck: driver
- type: output
  name: out
  input: banded
  config:
    name: out
    type: csv
    path: out.csv
"#;

/// The strategy stamped on the `banded` node of `EQUI_RANGE_YAML`.
fn equi_range_strategy() -> CombineStrategy {
    let config =
        clinker_plan::config::parse_config(EQUI_RANGE_YAML).expect("parse equi+range YAML");
    let validated = config
        .compile(&clinker_plan::config::CompileContext::default())
        .expect("compile equi+range pipeline");
    let dag = validated.dag();
    for idx in dag.graph.node_indices() {
        if let PlanNode::Combine { name, strategy, .. } = &dag.graph[idx]
            && name == "banded"
        {
            return strategy.clone();
        }
    }
    panic!("combine node \"banded\" not present in compiled equi+range plan");
}

/// CSV for `n` orders across `regions` regions, each amount `2*i`.
fn equi_orders_csv(n: usize, regions: usize) -> String {
    let mut s = String::from("region,amount\n");
    for i in 0..n {
        s.push_str(&format!("r{},{}\n", i % regions, i * 2));
    }
    s
}

/// CSV for `n` bands across `regions` regions, each lo `2*i` so every order
/// matches at least the bands in its region with a lower amount.
fn equi_bands_csv(n: usize, regions: usize) -> String {
    let mut s = String::from("region,lo\n");
    for i in 0..n {
        s.push_str(&format!("r{},{}\n", i % regions, i * 2));
    }
    s
}

#[test]
fn equi_range_selects_hash_partition_strategy() {
    assert!(
        matches!(
            equi_range_strategy(),
            CombineStrategy::HashPartitionIEJoin { .. }
        ),
        "an equi+range predicate must select the hash-partitioned IEJoin strategy, got {:?}",
        equi_range_strategy()
    );
}

#[test]
fn equi_range_pre_output_state_aborts_under_tight_budget_without_rss() {
    // The equi+range (HashPartitionIEJoin) path holds its routed partitions and
    // per-group sort arrays resident — it has no spill path, so an over-budget
    // pre-scan state can only be resolved by a clean abort. Under a tight budget
    // far below the partition/group footprint of 300×300 records, the
    // RSS-independent `should_abort_local` gate must trip with the typed
    // MemoryBudgetExceeded, and the IEJoin consumer must still be unregistered.
    // This is the coverage the deleted block-band rewrite removed; no block-band
    // test drives it, since that path spills instead of aborting.
    let arb = no_op_arbitrator(ABORT_LIMIT);
    let (result, _output) = run_pipeline_yaml(
        EQUI_RANGE_YAML,
        equi_orders_csv(300, 4),
        equi_bands_csv(300, 4),
        &arb,
    );
    let err = result.expect_err("the equi+range pre-scan state must abort under the tight budget");

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
                "equi+range pre-output state is arena-class memory"
            );
            assert_eq!(
                limit, ABORT_LIMIT,
                "the reported limit must be the hard budget"
            );
            assert!(
                used > ABORT_LIMIT,
                "the reported footprint ({used}) must exceed the budget ({ABORT_LIMIT})"
            );
            let detail = detail.expect("the pre-output abort must carry a detail string");
            assert!(
                detail.contains("iejoin pre-output"),
                "the abort must come from the equi+range pre-output gate; got: {detail:?}"
            );
        }
        other => panic!("expected MemoryBudgetExceeded from the pre-output gate; got: {other:?}"),
    }

    assert_eq!(
        arb.consumer_count(),
        0,
        "the equi+range IEJoin consumer must be unregistered even when the branch aborts"
    );
}
