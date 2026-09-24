//! Pipeline-level coverage for the IEJoin block-band path.
//!
//! Both `CombineStrategy::IEJoin` (pure-range) and
//! `CombineStrategy::HashPartitionIEJoin` (equi+range) run the one bounded
//! block-band path: external-sort each side on `(equality-hash, primary-range-
//! key, …)`, slice the merged stream into min/max-tagged single-hash blocks,
//! prune non-overlapping same-hash block-pairs, re-verify canonical equality per
//! candidate pair, and run the kernel per surviving pair. These tests drive that
//! path end-to-end through the public executor entry point.
//!
//! Input sort/slice pressure is spillable and the block-pair pre-output gate
//! remains local. Output merge readers have a separate retained footprint:
//! compressed frontiers may exceed a budget that fits a block pair. Those runs
//! must refuse the final range merge before publishing bytes, then release all
//! owners. Feasible executions retain the exact output and spill oracles.
//! The unchanged 8 KiB cases separately prove the local pre-output abort.
//!
//! Local sorter/block-pair fixtures are eagerly decoded external records. The
//! executor currently materializes source/combine inputs before the local
//! spillable sorter; these oracles do not prove whole-input CSV residency fits
//! the tight limit. A separate real file-CSV test below checks finite admission
//! refusal and cleanup, plus exact successful output at the roomy limit.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use clinker_plan::plan::combine::CombineStrategy;
use clinker_plan::plan::execution::PlanNode;
use std::collections::HashMap;
use std::sync::Arc;

/// A tight hard limit far below process RSS, but above the local footprint of a
/// single block-pair (two 16 KiB-floored blocks plus kernel aux). This does not
/// imply that every compressed output frontier and writer can coexist under it.
const TIGHT_LIMIT: u64 = 320 * 1024;
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
- type: sink
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
    let (result, output, _) = run_pipeline_capture(yaml, orders, bands, "iejoin-block-band", arb);
    (result.map(|_report| ()), output.as_string())
}

/// Shared execution harness for the two-source (`orders` / `bands`) pipelines:
/// eagerly decode the CSV fixtures, wire the captured output writer and a
/// dead-letter capture sink, run under `arb` with the given `execution_id`, and
/// return the full execution result alongside the captured output CSV and the
/// dead-letter rows the run wrote. Both the `(Result<()>, String)` entry point above and
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
    Vec<crate::test_support::CapturedDlqRow>,
) {
    let config = clinker_plan::config::parse_config(yaml).expect("parse pipeline YAML");
    let readers = crate::test_support::predecoded_csv_readers(
        &config,
        &clinker_plan::config::CompileContext::default(),
        &[("orders", &orders), ("bands", &bands)],
    );
    let out = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: execution_id.to_string(),
        batch_id: "batch-0".to_string(),
        ..Default::default()
    };

    let sink = crate::test_support::CaptureDlqSink::new();
    let result = PipelineExecutor::run_with_readers_writers_with_arbitrator(
        &config,
        readers,
        sink.registry(writers),
        &params,
        clinker_plan::config::CompileContext::default(),
        Arc::clone(arb),
    );
    (result, out, sink.rows())
}

#[test]
fn file_csv_retention_refuses_tight_budget_and_completes_with_roomy_budget() {
    // Either side alone retains more admitted pad bytes than TIGHT_LIMIT.
    // No producer scheduling or particular failed allocation size is assumed.
    const ROWS: usize = 400;
    let orders = orders_csv(ROWS, 2048);
    let bands = bands_csv(ROWS, 0, 2048);
    let config = clinker_plan::config::parse_config(PIPELINE_YAML).expect("parse pipeline YAML");
    let root = tempfile::tempdir().expect("output staging directory");
    let run_files = |arb: &Arc<crate::pipeline::memory::MemoryArbitrator>,
                     destination: &std::path::Path| {
        let readers = HashMap::from([
            (
                "orders".to_string(),
                crate::executor::single_file_reader(
                    "orders.csv",
                    Box::new(std::io::Cursor::new(orders.as_bytes().to_vec())),
                ),
            ),
            (
                "bands".to_string(),
                crate::executor::single_file_reader(
                    "bands.csv",
                    Box::new(std::io::Cursor::new(bands.as_bytes().to_vec())),
                ),
            ),
        ]);
        // A caller-supplied Write can receive a prefix before a later Source
        // error. The destination-local staging registry is the publication
        // boundary: only a successful run may commit that prefix as output.
        let staging = crate::output::staging::OutputStagingRegistry::default();
        let (_, file) = staging
            .stage_output(
                "out",
                clinker_plan::config::IfExistsPolicy::Error,
                false,
                |_| Ok(destination.to_path_buf()),
            )
            .expect("stage output beside its destination");
        let writers = crate::executor::WriterRegistry {
            single: HashMap::from([(
                "out".to_string(),
                Box::new(file) as Box<dyn std::io::Write + Send>,
            )]),
            output_staging: staging.clone(),
            ..Default::default()
        };
        let params = PipelineRunParams {
            execution_id: "iejoin-file-csv".to_string(),
            batch_id: "batch-0".to_string(),
            ..Default::default()
        };
        let result = PipelineExecutor::run_with_readers_writers_with_arbitrator(
            &config,
            readers,
            writers,
            &params,
            clinker_plan::config::CompileContext::default(),
            Arc::clone(arb),
        );
        (result, staging)
    };
    let tight = no_op_arbitrator(TIGHT_LIMIT);
    let tight_destination = root.path().join("refused.csv");
    let (result, tight_staging) = run_files(&tight, &tight_destination);
    match result.expect_err("retained decoded fields exceed finite source headroom") {
        PipelineError::Format(clinker_format::FormatError::Resource(resource)) => {
            assert_eq!(
                resource.kind,
                clinker_record::owned_storage::ResourceErrorKind::Budget
            );
            assert!(resource.requested > resource.available);
        }
        other => panic!("expected typed CSV resource budget refusal; got {other:?}"),
    }
    assert!(
        !tight_destination.exists(),
        "source refusal must publish no destination"
    );
    assert!(tight_staging.committed_paths("out").is_empty());
    assert_released_output_owners(&tight);

    let roomy = no_op_arbitrator(ROOMY_LIMIT);
    let roomy_destination = root.path().join("completed.csv");
    let (result, roomy_staging) = run_files(&roomy, &roomy_destination);
    result.expect("the identical file CSV inputs fit the roomy budget");
    let mut expected = String::from("order_id,amount,band_id\n");
    for i in 0..ROWS {
        expected.push_str(&format!("o{i},{},b{i}\n", 2 * i));
    }
    assert_eq!(
        std::fs::read(&roomy_destination).expect("published CSV"),
        expected.as_bytes()
    );
    assert_eq!(
        roomy_staging.committed_paths("out"),
        vec![roomy_destination]
    );
    assert_released_output_owners(&roomy);
}

fn spilled_bytes(arb: &Arc<crate::pipeline::memory::MemoryArbitrator>) -> u64 {
    arb.per_stage_spill_bytes()
        .get("banded")
        .copied()
        .unwrap_or(0)
}

/// Preserve the original low-budget pressure scenario and compare two feasible
/// executions without changing input shape, batch size, or compression policy.
fn refused_frontier_then_feasible_output(
    yaml: &str,
    orders: String,
    bands: String,
) -> (String, Arc<crate::pipeline::memory::MemoryArbitrator>) {
    let low = no_op_arbitrator(TIGHT_LIMIT);
    let (result, output) = run_pipeline_yaml(yaml, orders.clone(), bands.clone(), &low);
    let frontier = assert_range_output_frontier_abort(
        result.expect_err("the compressed frontier cannot fit the original budget"),
        TIGHT_LIMIT,
    );
    assert!(
        output.is_empty(),
        "refused range output must publish no header or body"
    );
    assert!(
        low.per_stage_spill_bytes().contains_key("banded"),
        "original operator pressure must still record real spills"
    );
    assert_released_output_owners(&low);

    let startup = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let observed = startup.clone();
    let observer = RunAllocationObserverGuard::install(Box::new(move |memory, _| {
        observed.store(
            memory.writer_resource_usage().memory,
            std::sync::atomic::Ordering::Relaxed,
        );
    }));
    let roomy = no_op_arbitrator(ROOMY_LIMIT);
    let (result, expected) = run_pipeline_yaml(yaml, orders.clone(), bands.clone(), &roomy);
    drop(observer);
    result.expect("roomy reference execution");
    assert_released_output_owners(&roomy);
    let columns = expected
        .lines()
        .next()
        .expect("nonempty output header")
        .split(',')
        .count();
    let batch = crate::executor::batch_handoff::DEFAULT_BATCH_SIZE as u64;
    // The unchanged sender has 256 event slots (executor setup), plus one
    // record held by its receiver. The producer owns one batch backing; its
    // replacement is allocated only after routing consumes that backing.
    // Add this overlap to the rejected physical frontier and the observed
    // generic run-startup allocation, before any writer is prepared. This
    // is a sufficient counterpart, not a claim that the low run can progress.
    // The legacy stream estimate still excludes variable Value heap.
    let overlap = (batch + 256 + 1) * crate::executor::node_buffer::record_byte_cost(columns)
        + batch * std::mem::size_of::<crate::executor::stream_event::StreamEvent>() as u64;
    let startup = startup.load(std::sync::atomic::Ordering::Relaxed);
    let limit = frontier + overlap + startup;
    let feasible = no_op_arbitrator(limit);
    let (result, output) = run_pipeline_yaml(yaml, orders, bands, &feasible);
    result.expect("physical frontier plus handoff and generic startup overlap must fit");
    assert_eq!(
        output, expected,
        "feasible budgets must emit identical bytes"
    );
    assert_released_output_owners(&feasible);
    eprintln!(
        "output ownership: refused frontier={frontier}, handoff allowance={overlap}, startup={startup}, feasible limit={limit}, feasible spill={}",
        spilled_bytes(&feasible)
    );
    (output, feasible)
}

fn assert_range_output_frontier_abort(error: PipelineError, expected_limit: u64) -> u64 {
    let PipelineError::MemoryBudgetExceeded {
        node,
        used,
        limit,
        source,
        detail,
    } = error
    else {
        panic!("expected typed range-output frontier refusal, got {error:?}");
    };
    assert_eq!(node, "banded");
    assert_eq!(source, clinker_plan::BudgetCategory::Arena);
    assert_eq!(limit, expected_limit);
    assert!(used > limit);
    let detail = detail.expect("range refusal explains retained readers and spill bytes");
    let retained = detail
        .strip_prefix("range output merge frontier (")
        .expect("range frontier diagnostic");
    let (readers, retained) = retained.split_once(" readers, ").unwrap();
    let (spill_bytes, _) = retained.split_once(" spill bytes)").unwrap();
    assert!(readers.parse::<u64>().unwrap() > 0);
    assert!(
        spill_bytes.parse::<u64>().unwrap() > 0,
        "refusal must retain real, nonempty spill files"
    );
    used
}

#[test]
fn range_output_frontier_refusal_precedes_json_lines_writer_publication() {
    let yaml = EXPLODE_YAML.replace(
        "type: csv\n    path: out.csv",
        "type: json\n    path: out.json\n    options:\n      format: ndjson",
    );
    assert_ne!(yaml, EXPLODE_YAML);
    let arb = no_op_arbitrator(TIGHT_LIMIT);
    let (result, output, _) = run_pipeline_capture(
        &yaml,
        explode_orders_csv(60),
        explode_bands_csv(60),
        "json-frontier",
        &arb,
    );
    assert_range_output_frontier_abort(result.unwrap_err(), TIGHT_LIMIT);
    assert!(
        output.contents().is_empty(),
        "JSON output must publish no bytes before frontier refusal"
    );
    assert!(arb.per_stage_spill_bytes().contains_key("banded"));
    assert_released_output_owners(&arb);
}

fn assert_released_output_owners(arb: &Arc<crate::pipeline::memory::MemoryArbitrator>) {
    assert_eq!(arb.consumer_count(), 0);
    assert_eq!(arb.sum_consumer_usage(), 0);
    let usage = arb.writer_resource_usage();
    assert_eq!((usage.memory, usage.disk, usage.descriptors), (0, 0, 0));
    assert_eq!(arb.retry_writer_cleanup(), 0);
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
    // and slices into many 16 KB blocks. Under the 320 KiB budget a single
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
        "the ~140 KB sides must spill sort runs and blocks under the 320 KiB budget; \
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
    // One matching order/band pair keeps the spill-backed input scans below the
    // 8 KB budget, then reaches the pre-output charge. The two 16 KB-floored
    // blocks plus the kernel's sort arrays exceed that budget, so the LOCAL
    // gate trips — proving the demoted abort still fires when a lone block-pair
    // genuinely cannot fit, independent of process RSS or input materialization.
    let arb = no_op_arbitrator(ABORT_LIMIT);
    let (result, output) = run_pipeline(orders_csv(1, 0), bands_csv(1, 0, 0), &arb);
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
- type: sink
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

/// The output-explosion pipeline with a BLOCKING downstream: a passthrough
/// `Transform` sits between the combine and the Output, so the combine's sole
/// consumer is not a streaming consumer kind. The combine adopts its spilled
/// output runs whole into a merge-on-drain node-buffer
/// (`adopt_spilled_runs_into_node_buffer`) that the materialized chain drains —
/// the drain target the streaming graft does NOT take.
const EXPLODE_BLOCKING_YAML: &str = r#"
pipeline:
  name: iejoin_block_band_explode_blocking
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
- type: transform
  name: passthrough
  input: banded
  config:
    cxl: |
      emit order_id = order_id
      emit band_id = band_id
- type: sink
  name: out
  input: passthrough
  config:
    name: out
    type: csv
    path: out.csv
"#;

/// Compile `yaml` and return the [`StreamClass`] the streaming-fusion analysis
/// assigns the `banded` combine node — `Streaming` when the dispatcher will
/// graft the block-band output drain onto the sink, `Materialized` when it
/// admits a node-buffer. This is the exact plan-derived predicate the runtime
/// sender-install consults, so it proves which drain branch the run takes.
fn banded_stream_class(yaml: &str) -> clinker_plan::plan::execution::StreamClass {
    let config = clinker_plan::config::parse_config(yaml).expect("parse pipeline YAML");
    let validated = config
        .compile(&clinker_plan::config::CompileContext::default())
        .expect("compile pipeline");
    let dag = validated.dag();
    let classes = clinker_plan::plan::execution::classify_stream_nodes(dag, &config);
    let banded = dag
        .graph
        .node_indices()
        .find(|idx| matches!(&dag.graph[*idx], PlanNode::Combine { name, .. } if name == "banded"))
        .expect("banded combine present in compiled plan");
    classes[&banded]
}

#[test]
fn block_band_output_buffers_under_blocking_downstream() {
    // The block-band output NODE-BUFFER path (the drain target the streaming
    // graft does NOT take): a passthrough Transform sits between the combine and
    // the Output, so the combine's sole consumer is not a streaming consumer
    // kind. The combine admits a node-buffer through the buffered branch of
    // `drain_block_band_output` rather than streaming, and the materialized
    // chain carries the rows to the sink. A 30 x 30 result that stays resident
    // under a roomy budget exercises the buffered `admit_node_buffer` branch
    // (the streamed alternative is pinned by the sibling streaming test); the
    // run COMPLETES and equals the nested-loop oracle.

    // Proof the node-buffer (not the streaming) path is exercised: the combine
    // classifies `Materialized`, so no streaming sender is installed for it and
    // `drain_block_band_output` falls through to the buffered branch.
    assert_eq!(
        banded_stream_class(EXPLODE_BLOCKING_YAML),
        clinker_plan::plan::execution::StreamClass::Materialized,
        "a combine whose consumer is a non-streaming Transform must materialize its output"
    );

    const N: usize = 30;
    const M: usize = 30;
    let arb = no_op_arbitrator(ROOMY_LIMIT);
    let (result, output) = run_pipeline_yaml(
        EXPLODE_BLOCKING_YAML,
        explode_orders_csv(N),
        explode_bands_csv(M),
        &arb,
    );
    result.expect("the buffered blocking-downstream join must complete under the roomy budget");

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

    // A roomy budget keeps the small result resident, so the buffered branch
    // admits an in-memory node-buffer with nothing reaching disk.
    assert_eq!(
        spilled_bytes(&arb),
        0,
        "the {N}x{M} result must stay resident under the roomy budget; \
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
fn block_band_output_explosion_refuses_insufficient_frontier_budget() {
    // The same 60 x 60 explosion spills under the original 320 KiB budget,
    // but its ten compressed readers exceed the range-output frontier limit.
    // Keep the refusal and verify the full oracle under a feasible budget.

    // Proof the streaming path (not the node-buffer path) is exercised: the same
    // plan-derived predicate the runtime sender-install consults classifies the
    // `banded` combine as `Streaming`, so a sender IS installed for it and
    // `drain_block_band_output` takes the streaming branch.
    assert_eq!(
        banded_stream_class(EXPLODE_YAML),
        clinker_plan::plan::execution::StreamClass::Streaming,
        "combine(IEJoin) -> single Output must classify Streaming so the drain streams"
    );

    const N: usize = 60;
    const M: usize = 60;
    let (output, arb) = refused_frontier_then_feasible_output(
        EXPLODE_YAML,
        explode_orders_csv(N),
        explode_bands_csv(M),
    );

    let data_lines: Vec<&str> = output.lines().filter(|l| !l.is_empty()).skip(1).collect();
    assert_eq!(
        data_lines.len(),
        N * M,
        "match: all over all-covering bands emits the full {N}x{M} cross product"
    );

    // The result equals the nested-loop oracle: every (order, band) pair appears
    // exactly once, streamed in the deterministic sort order.
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

    // Preserve real output spill in the feasible execution as well.
    assert!(
        spilled_bytes(&arb) > 0,
        "the {}-row streamed output must overflow and spill under the feasible budget; \
         per_stage_spill_bytes[banded] was {}",
        N * M,
        spilled_bytes(&arb)
    );
    // Both the block-band kernel consumer and the streaming charge consumer must
    // be unregistered by the time the run returns.
    assert_eq!(
        arb.consumer_count(),
        0,
        "every consumer must be unregistered on the clean exit"
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
- type: sink
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

/// Parse a two-column `region,int` CSV (orders' `amount` or bands' `lo`) into
/// `(region, int)` rows, skipping the header. Independent of the executor, so
/// the oracle below is a pure function of the fixture data.
fn parse_region_int_csv(csv: &str) -> Vec<(String, i64)> {
    csv.lines()
        .skip(1)
        .filter(|l| !l.is_empty())
        .map(|l| {
            let mut c = l.split(',');
            let region = c.next().expect("region column").to_string();
            let value = c.next().expect("int column").parse().expect("int value");
            (region, value)
        })
        .collect()
}

/// Nested-loop oracle over the equi+range fixture: the `(region, amount, lo)`
/// triple for every `(order, band)` with matching region AND `amount >= lo`.
/// Each surviving pair yields a distinct triple (an order's amount is unique in
/// its region), so the set doubles as the exact emitted-row set.
fn equi_range_oracle(orders: &str, bands: &str) -> std::collections::HashSet<(String, i64, i64)> {
    let ords = parse_region_int_csv(orders);
    let bnds = parse_region_int_csv(bands);
    let mut out = std::collections::HashSet::new();
    for (oregion, amount) in &ords {
        for (bregion, lo) in &bnds {
            if oregion == bregion && amount >= lo {
                out.insert((oregion.clone(), *amount, *lo));
            }
        }
    }
    out
}

/// Parse the `banded` output CSV (`region,amount,lo`) into the same triple set,
/// keyed by header name so a column-order change surfaces as a parse error
/// rather than silent misalignment.
fn equi_range_output_triples(output: &str) -> std::collections::HashSet<(String, i64, i64)> {
    let mut lines = output.lines().filter(|l| !l.is_empty());
    let header: Vec<&str> = lines.next().expect("output header").split(',').collect();
    let col = |name: &str| {
        header
            .iter()
            .position(|h| *h == name)
            .unwrap_or_else(|| panic!("output missing {name} column; header was {header:?}"))
    };
    let (ri, ai, li) = (col("region"), col("amount"), col("lo"));
    lines
        .map(|l| {
            let cells: Vec<&str> = l.split(',').collect();
            (
                cells[ri].to_string(),
                cells[ai].parse().expect("amount int"),
                cells[li].parse().expect("lo int"),
            )
        })
        .collect()
}

#[test]
fn equi_range_refuses_insufficient_frontier_budget_and_preserves_spill_oracle() {
    // The original 37-reader compressed frontier cannot fit 320 KiB. Keep
    // that refusal, then prove nonempty exact output with feasible headroom.
    let orders = equi_orders_csv(300, 4);
    let bands = equi_bands_csv(300, 4);
    let (output, arb) =
        refused_frontier_then_feasible_output(EQUI_RANGE_YAML, orders.clone(), bands.clone());

    let oracle = equi_range_oracle(&orders, &bands);
    let emitted = equi_range_output_triples(&output);
    assert!(
        !oracle.is_empty(),
        "the fixture must produce matches for the comparison to be meaningful"
    );
    let emitted_rows = output.lines().filter(|l| !l.is_empty()).count() - 1;
    assert_eq!(
        emitted_rows,
        oracle.len(),
        "every (order, band) match must emit exactly one row — no duplicates or drops"
    );
    assert_eq!(
        emitted, oracle,
        "the equi+range result must equal the nested-loop oracle (region-eq AND amount >= lo)"
    );

    // The input and/or output axes spilled under the tight budget: the bound
    // holding rather than the run holding everything resident.
    assert!(
        spilled_bytes(&arb) > 0,
        "the equi+range join must spill under the feasible budget; \
         per_stage_spill_bytes[banded] was {}",
        spilled_bytes(&arb)
    );
    assert_eq!(
        arb.consumer_count(),
        0,
        "the equi+range IEJoin consumer must be unregistered on the clean exit"
    );
}

#[test]
fn equi_range_output_is_identical_across_memory_limits() {
    let (output, _) = refused_frontier_then_feasible_output(
        EQUI_RANGE_YAML,
        equi_orders_csv(300, 4),
        equi_bands_csv(300, 4),
    );
    assert!(output.lines().filter(|line| !line.is_empty()).count() > 1);
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
  dlq: { path: dlq.csv }
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
- type: sink
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
/// execution report and the dead-letter rows the run wrote. Shares the
/// reader / writer / params setup with [`run_pipeline_yaml`] through
/// [`run_pipeline_capture`]; the captured output CSV is produced there too, but
/// this entry point's callers only need the report and the rows.
fn run_pipeline_report(
    yaml: &str,
    orders: String,
    bands: String,
    arb: &Arc<crate::pipeline::memory::MemoryArbitrator>,
) -> Result<
    (
        crate::executor::ExecutionReport,
        Vec<crate::test_support::CapturedDlqRow>,
    ),
    PipelineError,
> {
    let (result, _, rows) = run_pipeline_capture(yaml, orders, bands, "iejoin-block-band-dlq", arb);
    result.map(|report| (report, rows))
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
    // A build-side entry reports its own band's source_row, not its driver's,
    // so the build projection groups each build-side entry under the
    // source_row of the trigger entry immediately before it — the driver whose
    // failure wrote it. Within that group, capture each build-side entry's band
    // INPUT INDEX (and assert its source_row is that index plus one) — the band side's
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
    // (source_row, trigger) for every CombineOutputRow entry; (preceding
    // trigger's source_row, band input index) for the collateral subset.
    type DlqSequences = (Vec<(u64, bool)>, Vec<(u64, u64)>);
    let dlq_sequences = |limit: u64| -> DlqSequences {
        let arb = no_op_arbitrator(limit);
        let (_report, dlq_rows) = run_pipeline_report(
            DLQ_YAML,
            dlq_orders_csv(60, 1024),
            dlq_bands_csv(N_BANDS, 1024),
            &arb,
        )
        .expect("the continue-strategy run completes, routing failures to the DLQ");
        let rows: Vec<_> = dlq_rows
            .iter()
            .filter(|e| {
                e.category()
                    == Some(clinker_core_types::dlq::DlqErrorCategory::CombineOutputRow.as_str())
            })
            .collect();
        let full: Vec<(u64, bool)> = rows.iter().map(|e| (e.source_row(), e.trigger())).collect();
        let mut builds: Vec<(u64, u64)> = Vec::new();
        let mut driver_row: Option<u64> = None;
        for e in &rows {
            if e.trigger() {
                driver_row = Some(e.source_row());
                continue;
            }
            let band = match e.field("band_id") {
                Some(band) if !band.is_empty() => band.to_string(),
                other => panic!("a build-side DLQ entry must carry band_id; got {other:?}"),
            };
            let idx = band
                .strip_prefix('b')
                .and_then(|d| d.parse::<u64>().ok())
                .unwrap_or_else(|| panic!("band_id must be b<input-index>; got {band:?}"));
            assert_eq!(
                e.source_row(),
                idx + 1,
                "build-side entry {band} must report its own band's source_row, not its driver's"
            );
            let driver_row =
                driver_row.expect("a build-side DLQ entry follows the trigger entry it pairs with");
            builds.push((driver_row, idx));
        }
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
