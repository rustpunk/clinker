//! End-to-end coverage for the relaxed-CK retract orchestrator under
//! MULTI-SOURCE ingest, where two Sources fan into one relaxed
//! Aggregate and their per-source `row_num` namespaces collide.
//!
//! Each Source carries its own monotonic `row_num` counter (reset to
//! zero per source in `ingest_source`), so `src_a.row_1` and
//! `src_b.row_1` are distinct lineage entries that happen to share an
//! integer. The aggregator stores each contribution as a
//! `(row_num, source_name)` tuple on `input_rows`, and the orchestrator
//! threads that source tag through detect (`affected_row_pairs`),
//! the cumulative `seen_source_rows` dedup set, and the recompute
//! phase's `retract_row(row_id, source)` call. Drop the source half of
//! the tuple anywhere along that chain and the two sources' colliding
//! row ids dedup against each other: the second source's contribution
//! never retracts and its stale rows leak into the writer output.
//!
//! The single-source suites (`correlated_post_aggregate_retract.rs`,
//! `correlated_dlq_retract.rs`) cannot surface this — with one Source
//! every `row_num` is already unique, so a bare-`u64` lineage passes
//! them. The bit-for-bit baseline equivalence below is the load-bearing
//! claim: a regression to source-blind lineage breaks it because one
//! source's HR contribution survives the retract.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_core::config::{CompileContext, parse_config};
use clinker_core::executor::{PipelineExecutor, PipelineRunParams, SourceReaders};
use clinker_core::source::multi_file::FileSlot;

fn slot(name: &str, csv: &str) -> FileSlot {
    FileSlot::new(
        PathBuf::from(format!("{name}.csv")),
        Box::new(Cursor::new(csv.as_bytes().to_vec())),
    )
}

fn writer(buf: &SharedBuffer) -> Box<dyn std::io::Write + Send> {
    Box::new(buf.clone())
}

fn run_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "two-source-retract".to_string(),
        batch_id: "batch".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

/// `[src_a, src_b] → merge → relaxed Aggregate(group_by department) →
/// Transform(1/(total-60)) → Output`. Both sources declare the same
/// `correlation_key`; the Aggregate groups by `department`, which omits
/// the CK, so the planner marks it relaxed and the orchestrator's
/// five-phase retract path runs. The post-aggregate `1/(total-60)`
/// divides by zero exactly when a group totals 60, retracting that
/// group's contributors via the synthetic-CK fan-out over `input_rows`.
const PIPELINE_YAML: &str = r#"
pipeline:
  name: two_source_relaxed_retract
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: department, type: string }
        - { name: amount, type: int }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: department, type: string }
        - { name: amount, type: int }
  - type: merge
    name: merged
    inputs: [src_a, src_b]
  - type: aggregate
    name: dept_totals
    input: merged
    config:
      group_by: [department]
      cxl: |
        emit department = department
        emit total = sum(amount)
        emit n = count(*)
  - type: transform
    name: post_check
    input: dept_totals
    config:
      cxl: |
        emit department = department
        emit total = total
        emit n = n
        emit ratio = 1 / (total - 60)
  - type: output
    name: out
    input: post_check
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

/// Successful run: counters, DLQ entries, rendered writer output, and
/// the per-source DLQ attribution map.
struct RunOutput {
    counters: clinker_record::counters::PipelineCounters,
    dlq: Vec<clinker_core::executor::DlqEntry>,
    output: String,
    per_source_dlq: std::collections::BTreeMap<String, u64>,
}

fn run_two_source(src_a_csv: &str, src_b_csv: &str) -> RunOutput {
    let config = parse_config(PIPELINE_YAML).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let readers: SourceReaders = HashMap::from([
        ("src_a".to_string(), vec![slot("a", src_a_csv)]),
        ("src_b".to_string(), vec![slot("b", src_b_csv)]),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("two-source relaxed-retract pipeline must execute");
    RunOutput {
        counters: report.counters,
        dlq: report.dlq_entries,
        output: buf.as_string(),
        per_source_dlq: report.per_source_dlq_counts,
    }
}

/// Sort the body rows (preserving the header) so output comparison is
/// insensitive to the writer's cross-source emission order.
fn sort_body(s: &str) -> Vec<String> {
    let mut lines: Vec<String> = s.lines().map(str::to_string).collect();
    if !lines.is_empty() {
        let header = lines.remove(0);
        lines.sort();
        lines.insert(0, header);
    }
    lines
}

/// Both sources put their HR rows FIRST (read order = `row_num` order),
/// so HR lands at `src_a.row_{1,2,3}` AND `src_b.row_{1,2,3}` — the
/// colliding-`row_num` condition. HR totals 60 across both sources
/// (six 10s), tripping the post-aggregate `1/(total-60)`. ENG never
/// totals 60, so it survives untouched.
const SRC_A: &str = "\
order_id,department,amount
A1,HR,10
A2,HR,10
A3,HR,10
A4,ENG,100
A5,ENG,200
";
const SRC_B: &str = "\
order_id,department,amount
B1,HR,10
B2,HR,10
B3,HR,10
B4,ENG,300
";

/// The retract must remove HR contributors from BOTH sources. The
/// orchestrator hands `retract_row` the pairs `(1,src_a) … (3,src_a)`
/// and `(1,src_b) … (3,src_b)`; only source-tagged lineage keeps the
/// colliding `row_num`s distinct so all six retract. The baseline
/// reruns the identical pipeline with every HR row pre-excluded from
/// both source files, so the HR group never finalizes. Bit-for-bit
/// equivalence between the retract-corrected output and the baseline
/// proves the source-scoped fan-out reached every contributing row.
#[test]
fn two_source_post_aggregate_retract_narrows_by_source() {
    let baseline_a = "\
order_id,department,amount
A4,ENG,100
A5,ENG,200
";
    let baseline_b = "\
order_id,department,amount
B4,ENG,300
";

    let run = run_two_source(SRC_A, SRC_B);
    let baseline = run_two_source(baseline_a, baseline_b);

    assert_eq!(
        baseline.counters.dlq_count, 0,
        "the HR-free baseline must not trip the divide-by-zero"
    );
    assert!(
        !run.output.contains("HR"),
        "the HR group totals 60 across both sources and must be fully \
         retracted; a source-blind lineage would leave one source's HR \
         contribution stranded:\ngot:\n{}",
        run.output
    );
    assert_eq!(
        sort_body(&run.output),
        sort_body(&baseline.output),
        "two-source per-source retract must produce baseline-equivalent \
         output:\ngot:\n{}\nbaseline:\n{}",
        run.output,
        baseline.output
    );
    assert!(
        run.counters.dlq_count >= 1,
        "the post-aggregate divide-by-zero must land at least one DLQ entry"
    );
    assert!(
        run.dlq.iter().any(|d| d.trigger),
        "the divide-by-zero on the HR aggregate row is the trigger entry"
    );
    // ENG survives intact: its total never equals 60, so the post-check
    // ratio is finite and the group reaches the writer unretracted.
    assert!(
        run.output.contains("ENG"),
        "ENG never totals 60 and must survive the retract:\ngot:\n{}",
        run.output
    );
}

/// Pins the lineage shape the per-source retract depends on: the
/// triggered HR group's `input_rows` must retain all six cross-source
/// contributions even though both sources number their HR rows 1..=3.
/// The detect-phase synthetic-CK fan-out resolves that group's
/// `input_rows` and counts every contribution in
/// `synthetic_ck_fanout_rows_expanded_total`; a reading of 6 confirms
/// both sources' HR rows co-reside in one group's lineage at colliding
/// `row_num`s. The narrowing that keeps those six entries distinct
/// through retract is what
/// `two_source_post_aggregate_retract_narrows_by_source` proves via
/// bit-for-bit baseline equivalence — this test fixes the precondition
/// that test relies on (six contributions, not three) so a regression
/// that dropped one source's rows from the group lineage surfaces here
/// as a count of 3 rather than only as an output-value mismatch there.
#[test]
fn two_source_retract_expands_colliding_row_ids_per_source() {
    let run = run_two_source(SRC_A, SRC_B);

    assert_eq!(
        run.counters
            .retraction
            .synthetic_ck_fanout_rows_expanded_total,
        6,
        "the synthetic-CK fan-out must expand all six HR contributors \
         (3 from src_a + 3 from src_b) even though both sources number \
         their HR rows 1..=3; if either source's contributions were lost \
         to a colliding row_num the count would be 3"
    );

    // The post-aggregate divide-by-zero produces exactly one trigger
    // DLQ entry against the synthetic merged-source name (the failing
    // aggregate-output row carries no upstream Source stamp); the
    // contributing source rows are retracted in place rather than
    // re-emitted as per-row DLQ entries, so the per-source DLQ map
    // stays empty here.
    assert_eq!(
        run.dlq.len(),
        1,
        "one trigger entry for the HR aggregate-row divide-by-zero; \
         got: {:?}",
        run.dlq
    );
    assert!(
        run.dlq[0].trigger,
        "the sole DLQ entry is the divide-by-zero trigger"
    );
    assert!(
        run.per_source_dlq.is_empty(),
        "the retract removes contributors in place; the only DLQ entry \
         is the merged-source trigger, which is filtered from the \
         per-source map; got: {:?}",
        run.per_source_dlq
    );

    // The orchestrator entered the relaxed retract loop and recomputed
    // the HR group.
    assert!(
        run.counters.retraction.iterations >= 1,
        "a relaxed-CK retract must record at least one loop iteration; got {}",
        run.counters.retraction.iterations
    );
    assert_eq!(
        run.counters.retraction.groups_recomputed, 1,
        "exactly the HR group is recomputed after the retract; got {}",
        run.counters.retraction.groups_recomputed
    );
}
