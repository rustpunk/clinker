//! Dead letters of a Combine output-row failure on the join kernels.
//!
//! The IEJoin block-band, sort-merge and grace-hash kernels defer each
//! recoverable output-row failure under `strategy: continue` and route it
//! through the `combine_output_row` path after the kernel returns. Each
//! failure writes the driver row as its trigger and, right after it, the
//! matched build row as a collateral. These tests pin what those rows say
//! under a spilling and a resident arbitrator:
//!
//! - a build-side row names the build record's own Source and its own row,
//!   whichever spill path the build record took;
//! - spilling changes no dead-letter bytes once the generated columns (the
//!   row id, the pairing column and the timestamp) are masked.
//!
//! The harness, the two arbitrators and the masking helper are kernel-neutral
//! so later dead-letter tests over the same kernels reuse them.

use super::combine_consumer_lifecycle::compiled_combine_strategy;
use super::*;
use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
use crate::test_support::CapturedDlqRow;
use clinker_bench_support::io::SharedBuffer;
use clinker_core_types::dlq::DlqErrorCategory;
use clinker_plan::plan::combine::CombineStrategy;
use std::collections::HashMap;
use std::sync::Arc;

/// The Combine every fixture here names.
const COMBINE: &str = "joined";

/// The column that names, on a collateral row, the `_cxl_dlq_id` of the
/// trigger row its failure wrote.
const PAIRING_COLUMN: &str = "_cxl_dlq_trigger_id";

/// Columns whose values are generated per run rather than derived from the
/// data, masked before two runs' dead letters are compared.
const GENERATED_COLUMNS: [&str; 3] = ["_cxl_dlq_id", PAIRING_COLUMN, "_cxl_dlq_timestamp"];

/// A 10 GiB hard limit with a soft limit near 10 KiB and `NoOpPolicy`:
/// `should_spill()` holds for the whole run, so every kernel that consults it
/// spills its build side, while `should_abort()` never fires.
fn spilling_arbitrator() -> Arc<MemoryArbitrator> {
    Arc::new(MemoryArbitrator::with_policy(
        10 * 1024 * 1024 * 1024,
        0.000_001,
        0.000_000_5,
        Box::new(NoOpPolicy),
    ))
}

/// A 10 GiB hard limit with the usual 80 % / 70 % thresholds and
/// `NoOpPolicy`: the fixtures here stay resident.
fn resident_arbitrator() -> Arc<MemoryArbitrator> {
    Arc::new(MemoryArbitrator::with_policy(
        10 * 1024 * 1024 * 1024,
        0.80,
        0.70,
        Box::new(NoOpPolicy),
    ))
}

/// The two arbitrators every kernel shape runs under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Budget {
    Spilling,
    Resident,
}

impl Budget {
    const BOTH: [Budget; 2] = [Budget::Spilling, Budget::Resident];

    fn arbitrator(self) -> Arc<MemoryArbitrator> {
        match self {
            Budget::Spilling => spilling_arbitrator(),
            Budget::Resident => resident_arbitrator(),
        }
    }
}

/// Run `yaml` with the named CSV `inputs` (Source name, CSV text) under
/// `arb`, returning the run result, the text the sink named `out` received,
/// and every dead-letter row the run wrote, in the order it wrote them.
///
/// Inputs are eagerly decoded through `predecoded_csv_readers`; dead letters
/// go to a capture sink, so a test reads the cells the dead-letter file would
/// hold.
fn run_capture(
    yaml: &str,
    inputs: &[(&str, &str)],
    arb: &Arc<MemoryArbitrator>,
) -> (
    Result<crate::executor::ExecutionReport, PipelineError>,
    String,
    Vec<CapturedDlqRow>,
) {
    let config = clinker_plan::config::parse_config(yaml).expect("parse pipeline YAML");
    let readers = crate::test_support::predecoded_csv_readers(
        &config,
        &clinker_plan::config::CompileContext::default(),
        inputs,
    );
    let out = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "combine-kernel-dead-letters".to_string(),
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
    (result, out.as_string(), sink.rows())
}

/// Run a two-source fixture under `budget` and assert its spill premise:
/// the Combine spilled under the spilling arbitrator and did not under the
/// resident one. Returns the dead-letter rows.
fn run_fixture(yaml: &str, drivers: &str, builds: &str, budget: Budget) -> Vec<CapturedDlqRow> {
    let arb = budget.arbitrator();
    let (result, _output, rows) =
        run_capture(yaml, &[("drivers", drivers), ("builds", builds)], &arb);
    let report = result.unwrap_or_else(|err| {
        panic!("the continue-strategy run under the {budget:?} arbitrator must complete: {err}")
    });
    let spilled = report
        .per_stage_spill_bytes
        .get(COMBINE)
        .copied()
        .unwrap_or(0);
    match budget {
        Budget::Spilling => assert!(
            spilled > 0,
            "spill premise: `{COMBINE}` must spill under the spilling arbitrator; \
             per_stage_spill_bytes = {:?}",
            report.per_stage_spill_bytes
        ),
        Budget::Resident => assert_eq!(
            spilled, 0,
            "spill premise: `{COMBINE}` must stay resident under the resident arbitrator; \
             per_stage_spill_bytes = {:?}",
            report.per_stage_spill_bytes
        ),
    }
    rows
}

/// `n` driver rows: `driver_id = d<i>`, `k = key(i)`, `v = i`, and a pad of
/// `pad_len` bytes.
fn drivers_csv(n: usize, pad_len: usize, key: impl Fn(usize) -> i64) -> String {
    let pad = "x".repeat(pad_len);
    let mut s = String::from("driver_id,k,v,pad\n");
    for i in 0..n {
        s.push_str(&format!("d{i},{},{i},{pad}\n", key(i)));
    }
    s
}

/// `n` build rows: `build_id = b<j>`, `k = key(j)`, `lo = -((j * 13) % n)`,
/// `hi = 10000 + j`, `divisor = 0` (every matched body divides by zero), and
/// a pad of `pad_len` bytes.
///
/// `lo` is a non-monotonic permutation of the input order whenever `n` is
/// coprime with 13, and `hi` ascends with it, so every driver `v` in
/// `0..10000` lies in every build's `[lo, hi)`.
fn builds_csv(n: usize, pad_len: usize, key: impl Fn(usize) -> i64) -> String {
    let pad = "x".repeat(pad_len);
    let mut s = String::from("build_id,k,lo,hi,divisor,pad\n");
    for j in 0..n {
        let lo = -(((j * 13) % n) as i64);
        s.push_str(&format!("b{j},{},{lo},{},0,{pad}\n", key(j), 10_000 + j));
    }
    s
}

/// A `sort_order` block on one field, indented for a Source's `config`.
fn sort_order(field: &str) -> String {
    format!("    sort_order:\n      - field: {field}\n")
}

/// The shared fixture: Sources `drivers` and `builds`, the Combine `joined`
/// whose body divides by `builds.divisor`, and the sink `out`, under
/// `strategy: continue` with one dead-letter file. `combine_config` holds the
/// Combine's `where`, `match` and any strategy hint.
fn fixture_yaml(
    drivers_sort: Option<&str>,
    builds_sort: Option<&str>,
    combine_config: &str,
) -> String {
    let drivers_sort = drivers_sort.map(sort_order).unwrap_or_default();
    let builds_sort = builds_sort.map(sort_order).unwrap_or_default();
    format!(
        r#"
pipeline:
  name: combine_kernel_dead_letters
error_handling:
  strategy: continue
  dlq: {{ path: dlq.csv }}
nodes:
- type: source
  name: drivers
  config:
    name: drivers
    type: csv
    path: drivers.csv
{drivers_sort}    schema:
      - {{ name: driver_id, type: string }}
      - {{ name: k, type: int }}
      - {{ name: v, type: int }}
      - {{ name: pad, type: string }}
- type: source
  name: builds
  config:
    name: builds
    type: csv
    path: builds.csv
{builds_sort}    schema:
      - {{ name: build_id, type: string }}
      - {{ name: k, type: int }}
      - {{ name: lo, type: int }}
      - {{ name: hi, type: int }}
      - {{ name: divisor, type: int }}
      - {{ name: pad, type: string }}
- type: combine
  name: {COMBINE}
  input:
    drivers: drivers
    builds: builds
  config:
{combine_config}    on_miss: skip
    cxl: |
      emit driver_id = drivers.driver_id
      emit q = drivers.v / builds.divisor
    propagate_ck: driver
- type: sink
  name: out
  input: {COMBINE}
  config:
    name: out
    type: csv
    path: out.csv
"#
    )
}

/// Pure-range, two conjuncts: the IEJoin block-band kernel.
fn iejoin_yaml(match_mode: &str) -> String {
    fixture_yaml(
        None,
        None,
        &format!(
            "    where: \"drivers.v >= builds.lo and drivers.v < builds.hi\"\n    \
             match: {match_mode}\n"
        ),
    )
}

/// Pure-range, one conjunct, both sides presorted on their range key: the
/// sort-merge kernel.
fn sort_merge_yaml() -> String {
    fixture_yaml(
        Some("v"),
        Some("hi"),
        "    where: \"drivers.v <= builds.hi\"\n    match: all\n",
    )
}

/// Pure-equi with the `grace_hash` hint: the grace-hash kernel.
fn grace_hash_yaml() -> String {
    fixture_yaml(
        None,
        None,
        "    where: \"drivers.k == builds.k\"\n    match: all\n    strategy: grace_hash\n",
    )
}

/// IEJoin and sort-merge sizes: 30 drivers by 40 builds with 1 KiB pads, so
/// the 40 KiB build side exceeds the 16 KiB sort, block and window floors,
/// and every driver matches every build.
const RANGE_DRIVERS: usize = 30;
const RANGE_BUILDS: usize = 40;
const RANGE_PAD: usize = 1024;

fn range_drivers() -> String {
    drivers_csv(RANGE_DRIVERS, RANGE_PAD, |_| 0)
}

fn range_builds() -> String {
    builds_csv(RANGE_BUILDS, RANGE_PAD, |_| 0)
}

/// Grace-hash sizes: 64 builds with `k = j % 32` and 128 drivers with
/// `k = (i + 17) % 32`, 256-byte pads. Each driver matches two builds, and no
/// matched pair shares an ordinal.
const GRACE_DRIVERS: usize = 128;
const GRACE_BUILDS: usize = 64;
const GRACE_KEYS: usize = 32;
const GRACE_PAD: usize = 256;

fn grace_drivers() -> String {
    drivers_csv(GRACE_DRIVERS, GRACE_PAD, |i| ((i + 17) % GRACE_KEYS) as i64)
}

fn grace_builds() -> String {
    builds_csv(GRACE_BUILDS, GRACE_PAD, |j| (j % GRACE_KEYS) as i64)
}

/// The input index a `<prefix><index>` id cell encodes.
fn id_index(row: &CapturedDlqRow, column: &str, prefix: char) -> u64 {
    let cell = row
        .field(column)
        .unwrap_or_else(|| panic!("a dead-letter row must carry {column}"));
    cell.strip_prefix(prefix)
        .and_then(|digits| digits.parse().ok())
        .unwrap_or_else(|| panic!("{column} must be {prefix}<input index>; got {cell:?}"))
}

/// Assert `rows` are `expected_failures` failures, each written as a driver
/// trigger row followed by its build-side row, and that each row names its
/// own Source and its own row, with the build row paired to its trigger.
fn assert_build_rows_attributed(rows: &[CapturedDlqRow], expected_failures: usize, label: &str) {
    let category = DlqErrorCategory::CombineOutputRow.as_str();
    for row in rows {
        assert_eq!(
            row.category(),
            Some(category),
            "{label}: every dead-letter row is a combine output-row failure"
        );
    }
    assert_eq!(
        rows.len(),
        2 * expected_failures,
        "{label}: each of the {expected_failures} failures writes a trigger row and a build row"
    );
    let mut distinct_pairs = 0usize;
    for (n, pair) in rows.chunks(2).enumerate() {
        let (trigger, build) = (&pair[0], &pair[1]);
        assert!(
            trigger.trigger(),
            "{label}: row {} must be a trigger row",
            2 * n
        );
        assert!(
            !build.trigger(),
            "{label}: row {} must be the build-side row after its trigger",
            2 * n + 1
        );
        assert_eq!(
            trigger.field("_cxl_dlq_source_name"),
            Some("drivers"),
            "{label}: failure {n}: the trigger row names the driver Source"
        );
        assert_eq!(
            trigger.source_row(),
            id_index(trigger, "driver_id", 'd') + 1,
            "{label}: failure {n}: the trigger row reports its own driver row"
        );
        assert_eq!(
            build.field("_cxl_dlq_source_name"),
            Some("builds"),
            "{label}: failure {n}: the build-side row names the build Source"
        );
        assert_eq!(
            build.source_row(),
            id_index(build, "build_id", 'b') + 1,
            "{label}: failure {n}: the build-side row reports its own build row, not the \
             driver's ({})",
            trigger.source_row()
        );
        assert_eq!(
            build.field(PAIRING_COLUMN),
            trigger.field("_cxl_dlq_id"),
            "{label}: failure {n}: the build-side row names its trigger in {PAIRING_COLUMN}"
        );
        if trigger.source_row() != build.source_row() {
            distinct_pairs += 1;
        }
    }
    assert!(
        distinct_pairs > 0,
        "{label}: at least one failure's driver and build rows have different row numbers, \
         so the fixture can tell the two identities apart"
    );
}

/// Every row's header and cells with the generated columns masked.
fn masked(rows: &[CapturedDlqRow]) -> Vec<(Vec<String>, Vec<String>)> {
    rows.iter()
        .map(|row| row.masked(&GENERATED_COLUMNS))
        .collect()
}

#[test]
fn block_band_build_rows_carry_their_own_source_row() {
    for (match_mode, failures) in [
        ("all", RANGE_DRIVERS * RANGE_BUILDS),
        ("first", RANGE_DRIVERS),
    ] {
        let yaml = iejoin_yaml(match_mode);
        assert!(
            matches!(
                compiled_combine_strategy(&yaml, COMBINE),
                CombineStrategy::IEJoin
            ),
            "the two-conjunct pure-range fixture must plan the IEJoin kernel"
        );
        for budget in Budget::BOTH {
            let rows = run_fixture(&yaml, &range_drivers(), &range_builds(), budget);
            let label = format!("IEJoin match: {match_mode}, {budget:?}");
            assert_build_rows_attributed(&rows, failures, &label);
            if match_mode == "first" {
                for build in rows.iter().filter(|row| !row.trigger()) {
                    assert_eq!(
                        build.field("build_id"),
                        Some("b0"),
                        "{label}: every driver's first match is the first build"
                    );
                }
            }
        }
    }
}

#[test]
fn sort_merge_build_rows_carry_their_own_source_row() {
    let yaml = sort_merge_yaml();
    assert!(
        matches!(
            compiled_combine_strategy(&yaml, COMBINE),
            CombineStrategy::SortMerge
        ),
        "the presorted single-range fixture must plan the sort-merge kernel"
    );
    for budget in Budget::BOTH {
        let rows = run_fixture(&yaml, &range_drivers(), &range_builds(), budget);
        assert_build_rows_attributed(
            &rows,
            RANGE_DRIVERS * RANGE_BUILDS,
            &format!("sort-merge, {budget:?}"),
        );
    }
}

#[test]
fn grace_hash_build_rows_carry_their_own_source_row() {
    let yaml = grace_hash_yaml();
    assert!(
        matches!(
            compiled_combine_strategy(&yaml, COMBINE),
            CombineStrategy::GraceHash { .. }
        ),
        "the grace_hash hint must plan the grace-hash kernel"
    );
    for budget in Budget::BOTH {
        let rows = run_fixture(&yaml, &grace_drivers(), &grace_builds(), budget);
        assert_build_rows_attributed(
            &rows,
            GRACE_DRIVERS * GRACE_BUILDS / GRACE_KEYS,
            &format!("grace-hash, {budget:?}"),
        );
    }
}

#[test]
fn block_band_dead_letters_identical_across_memory_limits() {
    let yaml = iejoin_yaml("all");
    assert!(matches!(
        compiled_combine_strategy(&yaml, COMBINE),
        CombineStrategy::IEJoin
    ));
    let spilled = run_fixture(&yaml, &range_drivers(), &range_builds(), Budget::Spilling);
    let resident = run_fixture(&yaml, &range_drivers(), &range_builds(), Budget::Resident);
    assert_eq!(spilled.len(), 2 * RANGE_DRIVERS * RANGE_BUILDS);
    assert!(
        masked(&spilled) == masked(&resident),
        "IEJoin: a spilled run must write the same dead-letter rows, in the same order, as a \
         resident run once the generated columns are masked"
    );
}

#[test]
fn sort_merge_dead_letters_identical_across_memory_limits() {
    let yaml = sort_merge_yaml();
    assert!(matches!(
        compiled_combine_strategy(&yaml, COMBINE),
        CombineStrategy::SortMerge
    ));
    let spilled = run_fixture(&yaml, &range_drivers(), &range_builds(), Budget::Spilling);
    let resident = run_fixture(&yaml, &range_drivers(), &range_builds(), Budget::Resident);
    assert_eq!(spilled.len(), 2 * RANGE_DRIVERS * RANGE_BUILDS);
    assert!(
        masked(&spilled) == masked(&resident),
        "sort-merge: a spilled run must write the same dead-letter rows, in the same order, as \
         a resident run once the generated columns are masked"
    );
}

#[test]
fn grace_hash_spilled_dead_letters_match_resident_rows() {
    // Grace visitation order depends on which partitions spilled, so the two
    // runs are compared as sorted (trigger, build) pairs rather than in order.
    let yaml = grace_hash_yaml();
    assert!(matches!(
        compiled_combine_strategy(&yaml, COMBINE),
        CombineStrategy::GraceHash { .. }
    ));
    let spilled = run_fixture(&yaml, &grace_drivers(), &grace_builds(), Budget::Spilling);
    let resident = run_fixture(&yaml, &grace_drivers(), &grace_builds(), Budget::Resident);
    assert_eq!(
        spilled.len(),
        resident.len(),
        "grace-hash: a spilled run writes as many dead-letter rows as a resident run"
    );
    let sorted_pairs = |rows: &[CapturedDlqRow]| {
        let masked = masked(rows);
        let mut pairs: Vec<_> = masked
            .chunks(2)
            .map(|pair| (pair[0].clone(), pair.get(1).cloned()))
            .collect();
        pairs.sort();
        pairs
    };
    assert!(
        sorted_pairs(&spilled) == sorted_pairs(&resident),
        "grace-hash: a spilled run must write the same (trigger, build) dead-letter pairs as a \
         resident run once the generated columns are masked"
    );
}
