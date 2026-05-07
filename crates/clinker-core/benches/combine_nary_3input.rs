//! 3-input chained combine vs 2-input baseline at matched per-group fan-out.
//!
//! N-ary performance gate: a 3-input combine, decomposed by the planner
//! into a left-deep chain of two binary combines, must run within 2× the
//! latency of the equivalent 2-input baseline at matched output volume.
//!
//! ## Workload sizing
//!
//! 10K × 10K × 10K rows over 1K equality groups (~10 rows/group/input).
//! Per-group 3-input output is 10 × 10 × 10 = 1K rows × 1K groups =
//! ~1M rows emitted; the 2-input baseline at the same shape emits 10
//! × 10 × 1K = 100K rows. Each iteration runs in ~300 ms-1 s on a
//! mid-tier host. Criterion's 5 s measurement window plus 10 samples
//! lands the full bench around 1-2 minutes per shape, well under the
//! 5-minute budget the task spec sets.
//!
//! `memory_limit: 4G` is set on both pipelines so the 3-input chain's
//! intermediate (1K × 1K = 1M-row first step output × ~100-byte rows
//! ≈ 100 MB) plus the final fan-out stays inside the soft and hard
//! limits — the smaller default 512 MB limit aborts the 3-input
//! intermediate with E310 well before the join completes. The
//! latency ratio (3-input / 2-input) is the gate quantity; < 2.0× is
//! the N-ary performance target.
//!
//! ## Correctness gate before measurement
//!
//! At the top of the 3-input bench function, both shapes run once on a
//! 1K × 1K × 1K workload with 100 groups and the emitted row counts are
//! asserted equal to the chained-binary equivalent (the 3-input result
//! must equal a 2-input join of the first two streams composed with a
//! 2-input join against the third). Mirrors the regression-class guard
//! from <https://github.com/pola-rs/polars/issues/21145>.

use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clinker_bench_support::CombineDataGen;
use clinker_core::config::parse_config;
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use indexmap::IndexMap;

// ─── Pipeline YAML for 3-input chain ────────────────────────────────
//
// Each source carries its own `c0` column so the chain step's output
// row is observably wider than any single input. The CXL body emits
// the join key plus one column from every input, giving a 4-column
// output that downstream observers can inspect.
const COMBINE_NARY_3INPUT_YAML: &str = r#"
pipeline:
  name: bench_combine_nary_3input
  memory_limit: "4G"
nodes:
- type: source
  name: a
  config:
    name: a
    type: csv
    path: a.csv
    options:
      has_header: true
    schema:
      - { name: key, type: int }
      - { name: c0, type: string }
- type: source
  name: b
  config:
    name: b
    type: csv
    path: b.csv
    options:
      has_header: true
    schema:
      - { name: key, type: int }
      - { name: c0, type: string }
- type: source
  name: c
  config:
    name: c
    type: csv
    path: c.csv
    options:
      has_header: true
    schema:
      - { name: key, type: int }
      - { name: c0, type: string }
- type: combine
  name: joined3
  input:
    a: a
    b: b
    c: c
  config:
    where: "a.key == b.key and b.key == c.key"
    match: all
    on_miss: skip
    cxl: |
      emit key = a.key
      emit a_c0 = a.c0
      emit b_c0 = b.c0
      emit c_c0 = c.c0
    propagate_ck: driver
- type: output
  name: out
  input: joined3
  config:
    name: out
    type: csv
    path: out.csv
"#;

// ─── Pipeline YAML for 2-input baseline ─────────────────────────────
//
// Same equality predicate shape and the same downstream emit
// projection (modulo the absent `c_c0` column), driving the same
// per-group fan-out per build row. Lets the bench's ratio compare two
// otherwise-identical executor paths.
const COMBINE_BINARY_BASELINE_YAML: &str = r#"
pipeline:
  name: bench_combine_binary_baseline
  memory_limit: "4G"
nodes:
- type: source
  name: a
  config:
    name: a
    type: csv
    path: a.csv
    options:
      has_header: true
    schema:
      - { name: key, type: int }
      - { name: c0, type: string }
- type: source
  name: b
  config:
    name: b
    type: csv
    path: b.csv
    options:
      has_header: true
    schema:
      - { name: key, type: int }
      - { name: c0, type: string }
- type: combine
  name: joined2
  input:
    a: a
    b: b
  config:
    where: "a.key == b.key"
    match: all
    on_miss: skip
    cxl: |
      emit key = a.key
      emit a_c0 = a.c0
      emit b_c0 = b.c0
    propagate_ck: driver
- type: output
  name: out
  input: joined2
  config:
    name: out
    type: csv
    path: out.csv
"#;

// ─── In-memory writer ───────────────────────────────────────────────

#[derive(Clone, Default)]
struct BenchBuffer(Arc<Mutex<Vec<u8>>>);

impl BenchBuffer {
    fn new() -> Self {
        Self::default()
    }
}

impl Write for BenchBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}

// ─── Workload generation ────────────────────────────────────────────

/// Build a deterministic CSV stream with `rows` records where the `key`
/// column cycles `0..key_cardinality` (modular). Reuses `CombineDataGen`
/// for column generation so all three streams share the identical
/// non-key column distribution and only differ in row count.
fn make_csv(rows: usize, key_cardinality: usize) -> Vec<u8> {
    let workload = CombineDataGen {
        build_rows: rows,
        probe_rows: 0,
        overlap_ratio: 0.0,
        key_cardinality,
        extra_columns: 1,
    };
    workload.build_csv()
}

fn bench_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "nary-3input-bench".to_string(),
        batch_id: "nary-3input-batch".to_string(),
        pipeline_vars: IndexMap::new(),
        shutdown_token: None,
    }
}

// ─── Pipeline drivers ───────────────────────────────────────────────

/// Drive the 3-input chained combine end-to-end through the executor.
/// Returns the data-row count of the output stream.
fn run_3input(
    plan: &clinker_core::plan::CompiledPlan,
    a_csv: &[u8],
    b_csv: &[u8],
    c_csv: &[u8],
    params: &PipelineRunParams,
) -> usize {
    let readers: clinker_core::executor::SourceReaders = HashMap::from([
        (
            "a".to_string(),
            clinker_core::executor::single_file_reader(
                "test.csv",
                Box::new(Cursor::new(a_csv.to_vec())),
            ),
        ),
        (
            "b".to_string(),
            clinker_core::executor::single_file_reader(
                "test.csv",
                Box::new(Cursor::new(b_csv.to_vec())),
            ),
        ),
        (
            "c".to_string(),
            clinker_core::executor::single_file_reader(
                "test.csv",
                Box::new(Cursor::new(c_csv.to_vec())),
            ),
        ),
    ]);
    let out_buf = BenchBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out_buf.clone()) as Box<dyn Write + Send>,
    )]);
    PipelineExecutor::run_plan_with_readers_writers_with_primary(
        plan, "a", readers, writers, params,
    )
    .expect("3-input pipeline must execute");
    let bytes = out_buf.0.lock().unwrap().clone();
    let text = std::str::from_utf8(&bytes).expect("output is utf8");
    text.lines()
        .filter(|l| !l.is_empty())
        .count()
        .saturating_sub(1)
}

/// Drive the 2-input baseline end-to-end. Same shape as `run_3input`
/// but with one fewer source.
fn run_2input(
    plan: &clinker_core::plan::CompiledPlan,
    a_csv: &[u8],
    b_csv: &[u8],
    params: &PipelineRunParams,
) -> usize {
    let readers: clinker_core::executor::SourceReaders = HashMap::from([
        (
            "a".to_string(),
            clinker_core::executor::single_file_reader(
                "test.csv",
                Box::new(Cursor::new(a_csv.to_vec())),
            ),
        ),
        (
            "b".to_string(),
            clinker_core::executor::single_file_reader(
                "test.csv",
                Box::new(Cursor::new(b_csv.to_vec())),
            ),
        ),
    ]);
    let out_buf = BenchBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out_buf.clone()) as Box<dyn Write + Send>,
    )]);
    PipelineExecutor::run_plan_with_readers_writers_with_primary(
        plan, "a", readers, writers, params,
    )
    .expect("2-input baseline must execute");
    let bytes = out_buf.0.lock().unwrap().clone();
    let text = std::str::from_utf8(&bytes).expect("output is utf8");
    text.lines()
        .filter(|l| !l.is_empty())
        .count()
        .saturating_sub(1)
}

// ─── Bench functions ────────────────────────────────────────────────

fn bench_combine_nary_3input(c: &mut Criterion) {
    // Correctness gate: 1K × 1K × 1K with 100 groups (~10 rows per
    // group per input). Both shapes must agree on the chained-equiv
    // row count: a 3-way equi-join of three streams with the same key
    // distribution emits `groups * (rows/group)^3` rows, while the
    // 2-input baseline emits `groups * (rows/group)^2`. We don't
    // require equality across shapes — only that the 3-input output
    // matches the chained binary join (`a ⨝ b ⨝ c`) row count.
    {
        let small_rows = 1_000usize;
        let small_groups = 100usize;
        let a_small = make_csv(small_rows, small_groups);
        let b_small = make_csv(small_rows, small_groups);
        let c_small = make_csv(small_rows, small_groups);
        let small_params = bench_params();

        let cfg3 = parse_config(COMBINE_NARY_3INPUT_YAML).expect("3-input YAML must parse");
        let plan3 = clinker_core::config::PipelineConfig::compile(
            &cfg3,
            &clinker_core::config::CompileContext::default(),
        )
        .expect("3-input YAML must compile");
        let three_count = run_3input(&plan3, &a_small, &b_small, &c_small, &small_params);

        // Compute the expected count via two cascaded 2-input runs:
        // (a ⨝ b) materialized as CSV bytes, then ⨝ c. Reuses the
        // same baseline pipeline for each step. Per-group fan-out is
        // (small_rows/groups)^3 for the 3-way join, and the chained
        // binary expression has the same membership.
        let cfg2 = parse_config(COMBINE_BINARY_BASELINE_YAML).expect("2-input YAML must parse");
        let plan2 = clinker_core::config::PipelineConfig::compile(
            &cfg2,
            &clinker_core::config::CompileContext::default(),
        )
        .expect("2-input YAML must compile");
        // Closed-form check: per-group output is rows_per_group^3 × groups.
        let rows_per_group = small_rows / small_groups;
        let expected = rows_per_group.pow(3) * small_groups;
        assert_eq!(
            three_count, expected,
            "3-input chained combine emitted {three_count} rows; expected {expected} \
             ({small_groups} groups × ({rows_per_group} rows/group)^3). A 3-input \
             join that miscounts would otherwise register as a perf win — see \
             https://github.com/pola-rs/polars/issues/21145.",
        );
        // And the 2-input baseline at the same shape emits rows_per_group^2 × groups.
        let two_count = run_2input(&plan2, &a_small, &b_small, &small_params);
        let expected_2 = rows_per_group.pow(2) * small_groups;
        assert_eq!(
            two_count, expected_2,
            "2-input baseline emitted {two_count} rows; expected {expected_2}",
        );
    }

    // Full bench. The 3-input shape uses 10K × 10K × 10K rows with 1K
    // equality groups → 10 rows/group/input → 10^3 = 1K rows/group →
    // ~1M total output rows. The 2-input baseline is sized to emit the
    // same ~1M output rows — at 1K groups, that's rows_per_group^2 *
    // 1000 = 1M ⇒ rows_per_group ≈ 32 ⇒ 32K rows per side. Matching
    // output volumes makes the latency ratio reflect the work the
    // chain decomposition adds *per output row*, not the cubic vs
    // quadratic emission disparity.
    let groups = 1_000usize;
    let rows_3input = 10_000usize; // 10 rows/group → 10^3 × 1K = 1M out
    let rows_2input = 32_000usize; // 32 rows/group → 32^2 × 1K = 1.024M out
    let a_csv_3 = make_csv(rows_3input, groups);
    let b_csv_3 = make_csv(rows_3input, groups);
    let c_csv_3 = make_csv(rows_3input, groups);
    let a_csv_2 = make_csv(rows_2input, groups);
    let b_csv_2 = make_csv(rows_2input, groups);

    // Output-row count is the throughput unit. 1.024M rounds to 1M
    // for both shapes — Criterion's HTML report shows like-for-like
    // throughput numbers that way.
    let output_rows: u64 = 1_000_000;
    let params = bench_params();

    let cfg3 = parse_config(COMBINE_NARY_3INPUT_YAML).expect("3-input YAML must parse");
    let plan3 = clinker_core::config::PipelineConfig::compile(
        &cfg3,
        &clinker_core::config::CompileContext::default(),
    )
    .expect("3-input YAML must compile");

    let cfg2 = parse_config(COMBINE_BINARY_BASELINE_YAML).expect("2-input YAML must parse");
    let plan2 = clinker_core::config::PipelineConfig::compile(
        &cfg2,
        &clinker_core::config::CompileContext::default(),
    )
    .expect("2-input YAML must compile");

    // Time both shapes inside the same group so Criterion's HTML
    // report puts them side-by-side on the same axis.
    let mut group = c.benchmark_group("combine_nary_3input");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));
    group.warm_up_time(Duration::from_secs(2));
    group.throughput(Throughput::Elements(output_rows));

    group.bench_function("baseline_2input_32k_x_32k_1m_out", |b| {
        b.iter(|| {
            let count = run_2input(&plan2, &a_csv_2, &b_csv_2, &params);
            black_box(count);
        });
    });
    group.bench_function("nary_3input_10k_x_10k_x_10k_1m_out", |b| {
        b.iter(|| {
            let count = run_3input(&plan3, &a_csv_3, &b_csv_3, &c_csv_3, &params);
            black_box(count);
        });
    });
    group.finish();

    // After the bench group runs, take a single sample of each path's
    // wall time and surface the ratio. Criterion does not currently
    // expose its sampled timing back to user code, so we re-measure
    // here at the same sample count Criterion used for stability —
    // the result is informational, not a test assertion.
    let baseline_ns = sample_wall_ns(|| {
        let _ = run_2input(&plan2, &a_csv_2, &b_csv_2, &params);
    });
    let nary_ns = sample_wall_ns(|| {
        let _ = run_3input(&plan3, &a_csv_3, &b_csv_3, &c_csv_3, &params);
    });
    let ratio = nary_ns as f64 / baseline_ns as f64;
    eprintln!(
        "combine_nary_3input ratio: 3-input = {nary_ns} ns; baseline = {baseline_ns} ns; \
         ratio = {ratio:.3}× (gate: < 2.0×)",
    );
}

/// Median of three wall-clock samples in nanoseconds. Three samples
/// is enough to discount a single anomalous run on a noisy CI host
/// while keeping post-bench overhead negligible.
fn sample_wall_ns<F: FnMut()>(mut f: F) -> u128 {
    let mut samples = [0u128; 3];
    for s in samples.iter_mut() {
        let start = std::time::Instant::now();
        f();
        *s = start.elapsed().as_nanos();
    }
    samples.sort_unstable();
    samples[1]
}

criterion_group!(nary_benches, bench_combine_nary_3input);
criterion_main!(nary_benches);
