//! Benchmarks for the Combine node (Phase C.0 – C.4).
//!
//! Six criterion functions cover the performance targets from
//! `phase-combine-node/spec.md §Performance Targets`:
//!
//! | Bench                          | Phase filling the body |
//! |--------------------------------|------------------------|
//! | `bench_combine_equi_2input`    | C.2 (equi-join executor) |
//! | `bench_combine_iejoin`         | C.3 (range/theta)        |
//! | `bench_combine_nary_3input`    | C.4 (N-ary cascade)      |
//! | `bench_predicate_decomposition`| C.1 (predicate decomp.)  |
//! | `bench_combine_grace_hash`     | C.4 (Grace hash)         |
//! | `bench_lookup_baseline`        | C.0 (real pipeline; **baseline**) |
//!
//! `bench_lookup_baseline` runs a full end-to-end lookup pipeline at
//! 10K × 100K against the current `lookup` path.
//! Run it manually with `--save-baseline lookup-v1` before C.2 execution
//! so it becomes the regression gate when combine replaces lookup:
//!
//! ```sh
//! cargo bench --bench combine -- bench_lookup_baseline --save-baseline lookup-v1
//! ```
//!
//! The other five functions are scaffolds at C.0 — each spins up a
//! `CombineDataGen` (exercising the real data-generation path) and leaves
//! an empty `b.iter()` body that later phases will fill in when the
//! corresponding executor strategy lands.

use clinker_bench_support::CombineDataGen;
use clinker_core::config::parse_config;
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use indexmap::IndexMap;
use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::sync::{Arc, Mutex};

// ── Shared helpers ────────────────────────────────────────────────

/// Thread-safe in-memory writer (mirrors the helper in `pipeline.rs`).
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

fn bench_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "combine-bench".to_string(),
        batch_id: "combine-batch".to_string(),
        pipeline_vars: IndexMap::new(),
        shutdown_token: None,
    }
}

// ── Scaffold placeholders (bodies fill in later phases) ───────────

fn bench_combine_equi_2input(c: &mut Criterion) {
    let mut group = c.benchmark_group("combine_equi_2input");
    for (label, build, probe) in [
        ("small", 1_000usize, 10_000usize),
        ("medium", 10_000, 100_000),
        ("large", 10_000, 1_000_000), // spec gate: 1M driving × 10K build
    ] {
        // Realistic setup: exercise `CombineDataGen` so the bench touches
        // real data-generation paths even though the inner loop body is
        // C.2 territory. Without this, the scaffold would not catch
        // regressions in the generator.
        let data_gen = CombineDataGen {
            build_rows: build,
            probe_rows: probe,
            overlap_ratio: 0.9,
            key_cardinality: build,
            extra_columns: 3,
        };
        let build_rows = data_gen.build_records();
        let probe_rows = data_gen.probe_records();
        group.throughput(Throughput::Elements(probe as u64));
        group.bench_with_input(BenchmarkId::new("rows", label), &(build, probe), |b, _| {
            b.iter(|| {
                // C.2 fills in the hash-build / probe executor call.
                black_box(&build_rows);
                black_box(&probe_rows);
            });
        });
    }
    group.finish();
}

fn bench_combine_iejoin(c: &mut Criterion) {
    // C.3 fills this in with the IE-join (range + theta) strategy.
    let mut group = c.benchmark_group("combine_iejoin");
    let data_gen = CombineDataGen {
        build_rows: 10_000,
        probe_rows: 100_000,
        overlap_ratio: 0.5,
        key_cardinality: 1_000,
        extra_columns: 2,
    };
    let build_rows = data_gen.build_records();
    let probe_rows = data_gen.probe_records();
    group.throughput(Throughput::Elements(100_000));
    group.bench_function("medium", |b| {
        b.iter(|| {
            // C.3 fills in.
            black_box(&build_rows);
            black_box(&probe_rows);
        });
    });
    group.finish();
}

fn bench_combine_nary_3input(c: &mut Criterion) {
    // C.4 fills this in with the N-ary cascade executor.
    let mut group = c.benchmark_group("combine_nary_3input");
    // Three correlated sets: build × mid × probe.
    let build = CombineDataGen {
        build_rows: 1_000,
        probe_rows: 0,
        overlap_ratio: 0.0,
        key_cardinality: 1_000,
        extra_columns: 1,
    }
    .build_records();
    let mid = CombineDataGen {
        build_rows: 10_000,
        probe_rows: 0,
        overlap_ratio: 0.0,
        key_cardinality: 1_000,
        extra_columns: 1,
    }
    .build_records();
    let probe = CombineDataGen {
        build_rows: 100_000,
        probe_rows: 0,
        overlap_ratio: 0.0,
        key_cardinality: 1_000,
        extra_columns: 1,
    }
    .build_records();
    group.throughput(Throughput::Elements(100_000));
    group.bench_function("1k_10k_100k", |b| {
        b.iter(|| {
            // C.4 fills in.
            black_box(&build);
            black_box(&mid);
            black_box(&probe);
        });
    });
    group.finish();
}

fn bench_predicate_decomposition(c: &mut Criterion) {
    // C.1 fills this in — decomposition of the where-predicate into
    // equality / range / residual conjuncts.
    let mut group = c.benchmark_group("predicate_decomposition");
    group.bench_function("scaffold", |b| {
        b.iter(|| {
            // C.1 fills in: parse a CXL predicate, call decompose(), assert
            // the number of equality/range/residual conjuncts.
            black_box(());
        });
    });
    group.finish();
}

fn bench_combine_grace_hash(c: &mut Criterion) {
    // C.4 fills this in — Grace hash partitioning with disk spill.
    let mut group = c.benchmark_group("combine_grace_hash");
    let data_gen = CombineDataGen {
        build_rows: 1_000_000,
        probe_rows: 0,
        overlap_ratio: 0.0,
        key_cardinality: 1_000_000,
        extra_columns: 4,
    };
    let build_rows = data_gen.build_records();
    group.throughput(Throughput::Elements(1_000_000));
    group.bench_function("1m_build", |b| {
        b.iter(|| {
            // C.4 fills in: force spill threshold low, run partitioned build,
            // probe with 100K, measure end-to-end time.
            black_box(&build_rows);
        });
    });
    group.finish();
}

// ── Real baseline: current lookup path at 10K × 100K ──────────────

/// Real end-to-end pipeline bench that captures the current lookup
/// throughput. This is the **regression gate** for Phase C.2: when
/// combine replaces lookup, its throughput must match or exceed this
/// baseline.
///
/// Workload: 10K build-side products × 100K driving orders, 90%
/// overlap (drill D10). Measures wall-clock time through the full
/// compile + execute path.
fn bench_lookup_baseline(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup_baseline");
    // Generate correlated CSV once per bench invocation — avoid paying the
    // data-gen cost in every iteration.
    let data_gen = CombineDataGen {
        build_rows: 10_000,
        probe_rows: 100_000,
        overlap_ratio: 0.9,
        key_cardinality: 10_000,
        extra_columns: 3,
    };
    let build_csv = data_gen.build_csv();
    let probe_csv = data_gen.probe_csv();

    // Lookup pipeline: products (10K reference table) × orders (100K
    // driving source) joined on `key`. `on_miss: null_fields` preserves
    // all probe rows (miss rate is ~10%).
    //
    // `products` is declared first (reference tables read more
    // naturally before the probe source) and `orders` is passed as
    // the explicit `primary` driving source at call time.
    // Declaration order is irrelevant to the executor — the primary
    // driving input is chosen by name, not position.
    let yaml = r#"
pipeline:
  name: bench_lookup_baseline
error_handling:
  strategy: continue
nodes:
- type: source
  name: products
  config:
    name: products
    path: products.csv
    type: csv
    options:
      has_header: true
    schema:
      - { name: key, type: int }
      - { name: c0, type: string }
      - { name: c1, type: string }
      - { name: c2, type: string }

- type: source
  name: orders
  config:
    name: orders
    path: orders.csv
    type: csv
    options:
      has_header: true
    schema:
      - { name: key, type: int }
      - { name: c0, type: string }
      - { name: c1, type: string }
      - { name: c2, type: string }

- type: transform
  name: enriched
  input: orders
  config:
    lookup:
      source: products
      where: "key == products.key"
      on_miss: null_fields
    cxl: |
      emit order_key = key
      emit order_c0 = c0
      emit product_c1 = products.c1 ?? "UNKNOWN"
      emit product_c2 = products.c2 ?? "UNKNOWN"

- type: output
  name: out
  input: enriched
  config:
    name: out
    path: out.csv
    type: csv
"#;

    let config = parse_config(yaml).expect("lookup baseline YAML must parse");
    let params = bench_params();

    group.throughput(Throughput::Elements(100_000));
    group.sample_size(10); // 10K × 100K is slow; cap samples to keep runtime reasonable.
    group.bench_with_input(BenchmarkId::from_parameter("10k_x_100k"), &(), |b, _| {
        b.iter(|| {
            let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([
                (
                    "products".to_string(),
                    Box::new(Cursor::new(build_csv.clone())) as Box<dyn std::io::Read + Send>,
                ),
                (
                    "orders".to_string(),
                    Box::new(Cursor::new(probe_csv.clone())) as Box<dyn std::io::Read + Send>,
                ),
            ]);
            let out_buf = BenchBuffer::new();
            let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
                "out".to_string(),
                Box::new(out_buf.clone()) as Box<dyn Write + Send>,
            )]);
            let plan = clinker_core::config::PipelineConfig::compile(
                &config,
                &clinker_core::config::CompileContext::default(),
            )
            .expect("lookup baseline must compile");
            let report = PipelineExecutor::run_plan_with_readers_writers_with_primary(
                &plan, "orders", readers, writers, &params,
            )
            .expect("lookup baseline must execute");
            black_box(report);
        });
    });
    group.finish();
}

criterion_group!(
    combine_benches,
    bench_combine_equi_2input,
    bench_combine_iejoin,
    bench_combine_nary_3input,
    bench_predicate_decomposition,
    bench_combine_grace_hash,
    bench_lookup_baseline,
);
criterion_main!(combine_benches);
