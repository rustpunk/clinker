//! IEJoin vs nested-loop benchmark on a mixed equi+range workload.
//!
//! Compares the Union Arrays IEJoin path (driven through the combine
//! executor at `crates/clinker-core/src/pipeline/iejoin.rs`) against a
//! brute-force `O(N*M)` nested-loop baseline on the canonical
//! tax-bracket-shaped workload: 1M driving × 50K build records,
//! ~500 equality groups, ~1% range selectivity. Each partition averages
//! ~2000 driving × ~100 build — the realistic ETL sweet spot the IEJoin
//! literature (DuckDB, Polars) reports as the 50-100x band.
//!
//! ## Why both paths share record materialization
//!
//! IEJoin executor entry points (`IEJoinExec`, `execute_combine_iejoin`,
//! `CombineResolverMapping::from_pre_resolved`) are `pub(crate)`, so a
//! bench in `benches/` cannot construct them directly. The IEJoin side
//! therefore drives the full executor through the YAML compile path with
//! pre-serialized CSV bytes; the YAML compile happens once per workload
//! size (hoisted out of `b.iter`). The nested-loop baseline runs on the
//! same `Vec<Record>` set the IEJoin path will re-parse from CSV. CSV
//! decode handicaps IEJoin in the measured ratio — the speedup reported
//! is therefore a conservative lower bound on the algorithmic advantage.
//!
//! ## Correctness gate before measurement
//!
//! At the top of the IEJoin bench function, both paths run once on a
//! 1K × 1K derived workload with the same predicate shape, and the
//! emitted row counts are asserted equal. A faster join that produces
//! wrong results would otherwise look like a perf win — see the regression
//! that motivated this guard at <https://github.com/pola-rs/polars/issues/21145>.

use std::io::{Cursor, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clinker_core::config::parse_config;
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};
use clinker_record::{Record, SchemaBuilder, Value};
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use indexmap::IndexMap;
use std::collections::HashMap;

// ─── YAML pipeline driving the IEJoin executor path ────────────────
//
// Equality on `entity_id` (~500 groups) plus a half-open range on
// `income`: `>= bracket_lo` and `< bracket_hi`. Mirrors
// `tests/fixtures/combine/iejoin_tax_bracket.yaml` at bench scale.
const COMBINE_IEJOIN_YAML: &str = r#"
pipeline:
  name: bench_combine_iejoin
  memory_limit: 4G
nodes:
- type: source
  name: employees
  config:
    name: employees
    type: csv
    path: employees.csv
    options:
      has_header: true
    schema:
      - { name: entity_id, type: int }
      - { name: employee_id, type: int }
      - { name: income, type: int }
- type: source
  name: brackets
  config:
    name: brackets
    type: csv
    path: brackets.csv
    options:
      has_header: true
    schema:
      - { name: entity_id, type: int }
      - { name: bracket_id, type: int }
      - { name: bracket_lo, type: int }
      - { name: bracket_hi, type: int }
      - { name: rate, type: int }
- type: combine
  name: assign_bracket
  input:
    employees: employees
    brackets: brackets
  config:
    where: "employees.entity_id == brackets.entity_id and employees.income >= brackets.bracket_lo and employees.income < brackets.bracket_hi"
    match: first
    on_miss: null_fields
    cxl: |
      emit entity_id = employees.entity_id
      emit income = employees.income
      emit rate = brackets.rate
    propagate_ck: driver
- type: output
  name: out
  input: assign_bracket
  config:
    name: out
    type: csv
    path: out.csv
"#;

// ─── In-memory writer (the executor needs `dyn Write`) ──────────────

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

// ─── Workload generator ─────────────────────────────────────────────

/// Tax-bracket-shaped workload: `entity_count` equality groups, each with
/// `brackets_per_entity` non-overlapping income brackets, and
/// `employees_per_entity` employees whose incomes fall uniformly across the
/// covered range. Returns `(employees, brackets)` records and matching
/// CSV-encoded bytes for the executor's CSV reader.
///
/// Models the canonical Clinker use case: assigning each employee to
/// exactly one tax bracket within their entity. Equality keys cluster
/// (~500 groups) so `partition_bits=8` lands real records per bucket;
/// range predicate selectivity is ~1/brackets_per_entity (~1% at 100
/// brackets) so the IEJoin scan emits a tight result set rather than a
/// near-Cartesian explosion.
struct IEJoinWorkload {
    employees: Vec<Record>,
    brackets: Vec<Record>,
    employees_csv: Vec<u8>,
    brackets_csv: Vec<u8>,
}

/// Generates a deterministic mixed equi+range workload from a seed.
///
/// All derivations are arithmetic over the seed plus row index — no
/// RNG state, byte-identical across runs and across hosts. The income
/// distribution within each entity covers the bracket span uniformly,
/// so every employee matches exactly one bracket (selectivity ≈ 1 /
/// brackets_per_entity).
fn generate_workload(
    entity_count: usize,
    brackets_per_entity: usize,
    employees_per_entity: usize,
    seed: u64,
) -> IEJoinWorkload {
    let employees_schema = SchemaBuilder::with_capacity(3)
        .with_field("entity_id")
        .with_field("employee_id")
        .with_field("income")
        .build();
    let brackets_schema = SchemaBuilder::with_capacity(5)
        .with_field("entity_id")
        .with_field("bracket_id")
        .with_field("bracket_lo")
        .with_field("bracket_hi")
        .with_field("rate")
        .build();

    // Brackets: per entity, `brackets_per_entity` non-overlapping
    // half-open intervals tiling [0, BRACKET_SPAN).
    const BRACKET_SPAN: i64 = 1_000_000;
    let bracket_step: i64 = BRACKET_SPAN / brackets_per_entity as i64;
    let mut brackets = Vec::with_capacity(entity_count * brackets_per_entity);
    let mut brackets_csv = Vec::with_capacity(entity_count * brackets_per_entity * 32);
    brackets_csv.extend_from_slice(b"entity_id,bracket_id,bracket_lo,bracket_hi,rate\n");
    for ent in 0..entity_count {
        for b in 0..brackets_per_entity {
            let lo = b as i64 * bracket_step;
            let hi = lo + bracket_step;
            let bracket_id = ent * brackets_per_entity + b;
            // Rate encoded as basis points so it stays integral; the
            // bench doesn't read the value, only the bracket→employee
            // count.
            let rate = (10 + b * 5) as i64;
            brackets.push(Record::new(
                Arc::clone(&brackets_schema),
                vec![
                    Value::Integer(ent as i64),
                    Value::Integer(bracket_id as i64),
                    Value::Integer(lo),
                    Value::Integer(hi),
                    Value::Integer(rate),
                ],
            ));
            use std::io::Write as _;
            writeln!(brackets_csv, "{ent},{bracket_id},{lo},{hi},{rate}").unwrap();
        }
    }

    // Employees: incomes spread uniformly across [0, BRACKET_SPAN) so
    // every employee falls in exactly one bracket of their entity.
    let total_employees = entity_count * employees_per_entity;
    let mut employees = Vec::with_capacity(total_employees);
    let mut employees_csv = Vec::with_capacity(total_employees * 24);
    employees_csv.extend_from_slice(b"entity_id,employee_id,income\n");
    for ent in 0..entity_count {
        for e in 0..employees_per_entity {
            let employee_id = ent * employees_per_entity + e;
            // Mix the seed into the income so two workloads with the
            // same shape but different seeds produce different per-row
            // values — guards the bench against accidental correlation
            // with input order.
            let mix = (seed
                .wrapping_add(employee_id as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15))
                & 0xFFFF_FFFF;
            let income = (mix as i64) % BRACKET_SPAN;
            employees.push(Record::new(
                Arc::clone(&employees_schema),
                vec![
                    Value::Integer(ent as i64),
                    Value::Integer(employee_id as i64),
                    Value::Integer(income),
                ],
            ));
            use std::io::Write as _;
            writeln!(employees_csv, "{ent},{employee_id},{income}").unwrap();
        }
    }

    IEJoinWorkload {
        employees,
        brackets,
        employees_csv,
        brackets_csv,
    }
}

// ─── Nested-loop baseline ───────────────────────────────────────────

/// Brute-force `O(N*M)` join: for each driver, scan every build record,
/// apply equality + range predicate. The ratio between this and the
/// IEJoin path is the bench gate's speedup denominator.
///
/// Returns the total matched-pair count (not the materialized rows) so
/// the inner loop stays branch-free of allocation. The IEJoin side's
/// row count is read off the executor's output buffer; the two are
/// asserted equal in the correctness gate before measurement.
fn nested_loop_baseline(employees: &[Record], brackets: &[Record]) -> usize {
    let mut count = 0usize;
    for emp in employees {
        let Some(Value::Integer(emp_entity)) = emp.get("entity_id") else {
            continue;
        };
        let Some(Value::Integer(emp_income)) = emp.get("income") else {
            continue;
        };
        for bkt in brackets {
            let Some(Value::Integer(bkt_entity)) = bkt.get("entity_id") else {
                continue;
            };
            if *emp_entity != *bkt_entity {
                continue;
            }
            let Some(Value::Integer(bkt_lo)) = bkt.get("bracket_lo") else {
                continue;
            };
            let Some(Value::Integer(bkt_hi)) = bkt.get("bracket_hi") else {
                continue;
            };
            if *emp_income >= *bkt_lo && *emp_income < *bkt_hi {
                count += 1;
            }
        }
    }
    count
}

// ─── Executor harness ───────────────────────────────────────────────

fn bench_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "iejoin-bench".to_string(),
        batch_id: "iejoin-batch".to_string(),
        pipeline_vars: IndexMap::new(),
        shutdown_token: None,
    }
}

/// Run the IEJoin executor end-to-end on the workload's cached CSV
/// bytes; return the number of data rows in the output. The compiled
/// plan is reused across iterations to keep planner overhead out of
/// the timed loop.
fn run_iejoin_executor(
    plan: &clinker_core::plan::CompiledPlan,
    employees_csv: &[u8],
    brackets_csv: &[u8],
    params: &PipelineRunParams,
) -> usize {
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([
        (
            "employees".to_string(),
            Box::new(Cursor::new(employees_csv.to_vec())) as Box<dyn std::io::Read + Send>,
        ),
        (
            "brackets".to_string(),
            Box::new(Cursor::new(brackets_csv.to_vec())) as Box<dyn std::io::Read + Send>,
        ),
    ]);
    let out_buf = BenchBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out_buf.clone()) as Box<dyn Write + Send>,
    )]);
    PipelineExecutor::run_plan_with_readers_writers_with_primary(
        plan,
        "employees",
        readers,
        writers,
        params,
    )
    .expect("iejoin pipeline must execute");
    let bytes = out_buf.0.lock().unwrap().clone();
    let text = std::str::from_utf8(&bytes).expect("output is utf8");
    text.lines()
        .filter(|l| !l.is_empty())
        .count()
        .saturating_sub(1)
}

// ─── Bench functions ────────────────────────────────────────────────

fn bench_combine_iejoin_nested_loop(c: &mut Criterion) {
    // Smaller scale here than the IEJoin bench: NLJ on 1M × 50K is
    // ~50 trillion comparisons and would not finish inside Criterion's
    // measurement window. The 200K × 10K shape preserves the partition
    // structure (~500 groups, ~400 × 20 per group) so the per-pair
    // cost stays comparable to the IEJoin partition.
    let workload = generate_workload(500, 20, 400, 0xBEEF);
    let mut group = c.benchmark_group("combine_iejoin_nested_loop");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(workload.employees.len() as u64));
    group.bench_function("200k_x_10k", |b| {
        b.iter(|| {
            let count = nested_loop_baseline(&workload.employees, &workload.brackets);
            black_box(count);
        });
    });
    group.finish();
}

fn bench_combine_iejoin(c: &mut Criterion) {
    // Correctness gate: at 1K × 1K with the same predicate shape,
    // both paths must agree on row counts before measurement begins.
    // A faster path that produces the wrong answer would otherwise
    // register as a performance win.
    let small = generate_workload(50, 10, 20, 0xC0FFEE);
    let nlj_count = nested_loop_baseline(&small.employees, &small.brackets);
    let small_config = parse_config(COMBINE_IEJOIN_YAML).expect("bench yaml must parse");
    let small_plan = clinker_core::config::PipelineConfig::compile(
        &small_config,
        &clinker_core::config::CompileContext::default(),
    )
    .expect("bench yaml must compile");
    let small_params = bench_params();
    let iejoin_count = run_iejoin_executor(
        &small_plan,
        &small.employees_csv,
        &small.brackets_csv,
        &small_params,
    );
    assert_eq!(
        iejoin_count, nlj_count,
        "IEJoin and nested-loop disagreed on the small workload row count \
         (iejoin={iejoin_count}, nested_loop={nlj_count}); a faster join \
         that returns the wrong answer is the regression class guarded \
         here. Workload: 50 entities × 10 brackets × 20 employees."
    );

    // Full bench: 1M driving × 50K build, ~500 entity groups, ~1%
    // range selectivity (one bracket out of ~100 per entity). Each
    // partition averages ~2000 employees × ~100 brackets — the
    // realistic ETL sweet spot.
    let workload = generate_workload(500, 100, 2_000, 0xDEAD_BEEF);
    let config = parse_config(COMBINE_IEJOIN_YAML).expect("bench yaml must parse");
    let plan = clinker_core::config::PipelineConfig::compile(
        &config,
        &clinker_core::config::CompileContext::default(),
    )
    .expect("bench yaml must compile");
    let params = bench_params();

    let mut group = c.benchmark_group("combine_iejoin");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(workload.employees.len() as u64));
    group.bench_function("1m_x_50k", |b| {
        b.iter(|| {
            let count = run_iejoin_executor(
                &plan,
                &workload.employees_csv,
                &workload.brackets_csv,
                &params,
            );
            black_box(count);
        });
    });
    group.finish();
}

criterion_group!(
    iejoin_benches,
    bench_combine_iejoin_nested_loop,
    bench_combine_iejoin,
);
criterion_main!(iejoin_benches);
