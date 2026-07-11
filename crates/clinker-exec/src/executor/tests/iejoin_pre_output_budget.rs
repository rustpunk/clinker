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

/// Assert `err` is the typed pre-output budget abort: the `banded` combine node,
/// arena-class memory, the reported limit equal to `expected_limit`, a footprint
/// above it, and a pre-output detail string. Shared by the block-band and
/// equi+range abort tests, whose pre-output gates surface the same shape.
fn assert_pre_output_abort(err: PipelineError, expected_limit: u64) {
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
                "pre-output state is arena-class memory"
            );
            assert_eq!(
                limit, expected_limit,
                "the reported limit must be the hard budget"
            );
            assert!(
                used > expected_limit,
                "the reported footprint ({used}) must exceed the budget ({expected_limit})"
            );
            let detail = detail.expect("the pre-output abort must carry a detail string");
            assert!(
                detail.contains("iejoin pre-output"),
                "the abort must come from the pre-output gate; got: {detail:?}"
            );
        }
        other => panic!("expected MemoryBudgetExceeded from the pre-output gate; got: {other:?}"),
    }
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
    let (result, output) = run_pipeline_capture(yaml, orders, bands, "iejoin-block-band", arb);
    (result.map(|_report| ()), output.as_string())
}

/// Shared execution harness for the two-source (`orders` / `bands`) pipelines:
/// wire the CSV readers and the captured output writer, run under `arb` with the
/// given `execution_id`, and return the full execution result alongside the
/// captured output CSV. Both the `(Result<()>, String)` entry point above and
/// the report-returning [`run_pipeline_report`] project from this one body, so
/// their reader / writer / params setup never drifts apart.
fn run_pipeline_capture(
    yaml: &str,
    orders: String,
    bands: String,
    execution_id: &str,
    arb: &Arc<crate::pipeline::memory::MemoryArbitrator>,
) -> (
    Result<crate::executor::ExecutionReport, PipelineError>,
    SharedBuffer,
) {
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
        execution_id: execution_id.to_string(),
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
    (result, out)
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
    assert_pre_output_abort(err, ABORT_LIMIT);

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

/// A pure-range pipeline whose `match: all` result is far larger than its inputs:
/// every order matches every band (each band spans the whole amount range), so a
/// small `n x m` input fans into `n * m` output rows. The block-band path
/// accumulates those in a payload-ordered sort buffer that spills on its own
/// threshold, so the output axis stays bounded even though the result dwarfs the
/// inputs. The match set is kept under the 10K output poll so completion is the
/// output buffer's spill doing the bounding, not the poll aborting.
const EXPLODE_YAML: &str = r#"
pipeline:
  name: iejoin_block_band_explode
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
    match: all
    on_miss: skip
    cxl: |
      emit order_id = orders.order_id
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

/// `n` orders with tiny amounts `i` (no pad, so the order side stays resident).
fn explode_orders_csv(n: usize) -> String {
    let mut s = String::from("order_id,amount\n");
    for i in 0..n {
        s.push_str(&format!("o{i},{i}\n"));
    }
    s
}

/// `m` all-covering bands `[-1, 1_000_000)`, so every order falls in every band
/// and `match: all` emits the full `n x m` cross product.
fn explode_bands_csv(m: usize) -> String {
    let mut s = String::from("band_id,lo,hi\n");
    for j in 0..m {
        s.push_str(&format!("b{j},-1,1000000\n"));
    }
    s
}

#[test]
fn block_band_output_explosion_spills_and_completes_under_tight_budget() {
    // The output-axis bound, end-to-end: a 60 x 60 input (120 tiny records that
    // stay resident) fans into 3600 output rows under `match: all` — a result far
    // larger than the inputs. Under a tight budget the emitted rows overflow the
    // output sort buffer's byte threshold and spill to disk; the dispatcher then
    // streams the sorted runs into a spillable node-buffer that a blocking CSV
    // Output drains. So the run COMPLETES (bounded peak — spills rather than
    // aborts), and its result equals the nested-loop oracle (every order-band
    // pair, since every band covers every amount).
    const N: usize = 60;
    const M: usize = 60;
    let arb = no_op_arbitrator(TIGHT_LIMIT);
    let (result, output) = run_pipeline_yaml(
        EXPLODE_YAML,
        explode_orders_csv(N),
        explode_bands_csv(M),
        &arb,
    );
    result.expect("the output-explosion join must complete under the tight budget by spilling");

    let data_lines: Vec<&str> = output.lines().filter(|l| !l.is_empty()).skip(1).collect();
    assert_eq!(
        data_lines.len(),
        N * M,
        "match: all over all-covering bands emits the full {N}x{M} cross product"
    );

    // The result equals the nested-loop oracle: every (order, band) pair appears
    // exactly once. Header is `order_id,band_id` (the two emitted columns).
    use std::collections::HashSet;
    let emitted: HashSet<(String, String)> = data_lines
        .iter()
        .map(|l| {
            let mut cols = l.split(',');
            let order = cols.next().expect("order_id column").to_string();
            let band = cols.next().expect("band_id column").to_string();
            (order, band)
        })
        .collect();
    assert_eq!(
        emitted.len(),
        N * M,
        "every emitted (order, band) pair must be distinct — no duplicates or drops"
    );
    for i in 0..N {
        for j in 0..M {
            assert!(
                emitted.contains(&(format!("o{i}"), format!("b{j}"))),
                "the nested-loop oracle expects order o{i} banded to b{j}"
            );
        }
    }

    // The inputs are tiny and stay resident, so any spilled bytes come from the
    // output axis: the buffer overflowed its threshold and spilled rather than
    // holding all 3600 rows in RAM.
    assert!(
        spilled_bytes(&arb) > 0,
        "the {}-row output must overflow the output sort buffer and spill; \
         per_stage_spill_bytes[banded] was {}",
        N * M,
        spilled_bytes(&arb)
    );
    assert_eq!(
        arb.consumer_count(),
        0,
        "the block-band consumer must be unregistered on the clean exit"
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
    // Only the equi+range path drives this gate: the pure-range block-band
    // path answers pressure by spilling instead of aborting.
    let arb = no_op_arbitrator(ABORT_LIMIT);
    let (result, _output) = run_pipeline_yaml(
        EQUI_RANGE_YAML,
        equi_orders_csv(300, 4),
        equi_bands_csv(300, 4),
        &arb,
    );
    let err = result.expect_err("the equi+range pre-scan state must abort under the tight budget");
    assert_pre_output_abort(err, ABORT_LIMIT);

    assert_eq!(
        arb.consumer_count(),
        0,
        "the equi+range IEJoin consumer must be unregistered even when the branch aborts"
    );
}

/// Pure-range pipeline whose matched body divides by a band column that is zero
/// for every band, so every matched (order, band) pair defers a recoverable
/// output-eval failure under `strategy: continue`. `match: all` fans each order
/// across every overlapping band, and the padded orders and bands slice into
/// several blocks under a tight budget — so the failures accrue in a
/// block-layout-dependent order that the final dead-letter sort must normalize.
const DLQ_YAML: &str = r#"
pipeline:
  name: iejoin_block_band_dlq
error_handling:
  strategy: continue
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
      - { name: divisor, type: int }
      - { name: pad, type: string }
- type: combine
  name: banded
  input:
    orders: orders
    bands: bands
  config:
    where: "orders.amount >= bands.lo and orders.amount < bands.hi"
    match: all
    on_miss: skip
    cxl: |
      emit order_id = orders.order_id
      emit q = orders.amount / bands.divisor
    propagate_ck: driver
- type: output
  name: out
  input: banded
  config:
    name: out
    type: csv
    path: out.csv
"#;

/// `n` orders with `amount == i`, each padded so the order side slices into
/// several driver blocks under a tight budget.
fn dlq_orders_csv(n: usize, pad_len: usize) -> String {
    let pad = "x".repeat(pad_len);
    let mut s = String::from("order_id,amount,pad\n");
    for i in 0..n {
        s.push_str(&format!("o{i},{i},{pad}\n"));
    }
    s
}

/// `b` bands, each covering `[lo_i, 10000)` with a non-positive `lo_i` and a
/// high `hi` so every order still matches all of them, each with divisor 0
/// (every matched body divides by zero) and a wide pad so the band side slices
/// into several build blocks under a tight budget.
///
/// `lo_i = -((i * 13) % b)` makes the primary range key a permutation of the
/// band input order (coprime stride, so distinct and non-monotonic). The kernel
/// emits a driver's matched builds in `lo`-key order, which the block slicing
/// preserves across layouts — so the emitted build sequence is layout-invariant
/// but is NOT the band input order. Only the `(order, driver_idx, build_idx)`
/// output sort restores input order; a regressed build tag would leave the
/// dead-letter builds in this `lo`-key permutation instead, which the ordering
/// assertion in the DLQ determinism test detects.
fn dlq_bands_csv(b: usize, pad_len: usize) -> String {
    let pad = "x".repeat(pad_len);
    let mut s = String::from("band_id,lo,hi,divisor,pad\n");
    for i in 0..b {
        let lo = -(((i * 13) % b) as i64);
        s.push_str(&format!("b{i},{lo},10000,0,{pad}\n"));
    }
    s
}

/// Run `yaml` over the given CSV inputs against `arb`, returning the full
/// execution report (so a test can inspect its dead-letter entries). Shares the
/// reader / writer / params setup with [`run_pipeline_yaml`] through
/// [`run_pipeline_capture`]; the captured output CSV is produced there too, but
/// this entry point's callers only need the report.
fn run_pipeline_report(
    yaml: &str,
    orders: String,
    bands: String,
    arb: &Arc<crate::pipeline::memory::MemoryArbitrator>,
) -> Result<crate::executor::ExecutionReport, PipelineError> {
    run_pipeline_capture(yaml, orders, bands, "iejoin-block-band-dlq", arb).0
}

#[test]
fn block_band_dlq_order_is_identical_across_memory_limits() {
    // The determinism invariant for the dead-letter output: a pure-range combine
    // whose body eval fails on every matched pair (divide by zero) under
    // `strategy: continue`. At a tight budget both sides slice into several
    // blocks, so the failures accrue in a block-interleaved order; at a roomy
    // budget each side is one block. The final failure sort — keyed on
    // (driver order, driver_idx, BUILD input index) — must make the dead-letter
    // row order a pure function of the data, identical at both limits.
    //
    // A driver's failures all share its source_row, so a source_row-only
    // projection is blind to the third (build input index) sort component.
    // Capture each build-side entry's band INPUT INDEX instead — the band side's
    // `lo` key is a non-monotonic permutation of that index (see
    // `dlq_bands_csv`), so the kernel emits a driver's builds in `lo`-key order,
    // which the block slicing keeps layout-invariant but which is NOT the input
    // order. Only the `(order, driver_idx, build_idx)` sort restores input order,
    // so asserting each driver's builds ascend by input index pins that third
    // component: a regressed build tag (e.g. u64::MAX) would leave them in the
    // `lo`-key permutation and fail the assertion.
    const N_BANDS: usize = 40;
    // One run yields two projections: the FULL CombineOutputRow sequence
    // (trigger and collateral entries alike, as (source_row, trigger)) so a
    // layout-dependent difference in trigger routing cannot hide behind a
    // collateral-only view, and the collateral (build-side) entries' band
    // input indices, which pin the third sort component.
    // (source_row, trigger) for every CombineOutputRow entry; (source_row,
    // band input index) for the collateral subset.
    type DlqSequences = (Vec<(u64, bool)>, Vec<(u64, u64)>);
    let dlq_sequences = |limit: u64| -> DlqSequences {
        let arb = no_op_arbitrator(limit);
        let report = run_pipeline_report(
            DLQ_YAML,
            dlq_orders_csv(60, 1024),
            dlq_bands_csv(N_BANDS, 1024),
            &arb,
        )
        .expect("the continue-strategy run completes, routing failures to the DLQ");
        let rows: Vec<_> = report
            .dlq_entries
            .iter()
            .filter(|e| e.category == clinker_core_types::dlq::DlqErrorCategory::CombineOutputRow)
            .collect();
        let full: Vec<(u64, bool)> = rows.iter().map(|e| (e.source_row, e.trigger)).collect();
        let builds: Vec<(u64, u64)> = rows
            .iter()
            .filter(|e| !e.trigger)
            .map(|e| {
                let band = match e.original_record.get("band_id") {
                    Some(clinker_record::Value::String(s)) => s.to_string(),
                    other => panic!("a build-side DLQ entry must carry band_id; got {other:?}"),
                };
                let idx = band
                    .strip_prefix('b')
                    .and_then(|d| d.parse::<u64>().ok())
                    .unwrap_or_else(|| panic!("band_id must be b<input-index>; got {band:?}"));
                (e.source_row, idx)
            })
            .collect();
        (full, builds)
    };

    let (tight_full, tight) = dlq_sequences(TIGHT_LIMIT);
    let (roomy_full, roomy) = dlq_sequences(ROOMY_LIMIT);
    assert!(
        !tight.is_empty(),
        "every matched pair divides by zero and attributes its build, so the build-side \
         dead-letter output must be non-empty"
    );
    assert_eq!(
        tight_full, roomy_full,
        "the complete dead-letter sequence — trigger entries included — must be a pure \
         function of the data, not of pipeline.memory.limit"
    );
    assert_eq!(
        tight, roomy,
        "block-band dead-letter order (including the build identity the third sort key fixes) \
         must be a pure function of the data, not of pipeline.memory.limit"
    );
    // Each driver's builds must land in ascending INPUT index at both limits —
    // the order the `build_idx` sort component fixes, distinct from the kernel's
    // `lo`-key emission order this fixture forces them out in.
    use std::collections::BTreeMap;
    let mut by_driver: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for (source_row, build_idx) in &tight {
        by_driver.entry(*source_row).or_default().push(*build_idx);
    }
    for (source_row, builds) in &by_driver {
        let expected: Vec<u64> = (0..builds.len() as u64).collect();
        assert_eq!(
            *builds, expected,
            "driver source_row {source_row} must dead-letter its builds in ascending input \
             index (all {N_BANDS} bands match), not the kernel's lo-key emission order"
        );
    }
    // The outer sort's source-row grouping holds too: ascending driver source row.
    let by_source: Vec<u64> = tight.iter().map(|(rn, _)| *rn).collect();
    let mut expected_sources = by_source.clone();
    expected_sources.sort_unstable();
    assert_eq!(
        by_source, expected_sources,
        "the dead-letter rows must land in ascending source-row order after the sort"
    );
}
