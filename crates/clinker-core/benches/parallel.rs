use clinker_bench_support::{CsvPayload, MEDIUM};
use clinker_core::config::parse_config;
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use indexmap::IndexMap;
use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::sync::{Arc, Mutex};

/// Thread-safe in-memory buffer for parallel benchmarks.
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

fn test_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "bench-parallel".to_string(),
        batch_id: "bench-batch".to_string(),
        pipeline_vars: IndexMap::new(),
        shutdown_token: None,
    }
}

// ── Parallel scaling: streaming pipeline ───────────────────────────

fn bench_scaling_streaming(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling_streaming");
    group.sample_size(20); // fewer samples for expensive pipeline runs

    let record_count = MEDIUM;
    let csv_bytes = CsvPayload::generate(
        record_count,
        &clinker_bench_support::FieldKind::default_layout(10),
        16,
        42,
    );

    for threads in [1, 2, 4, 8] {
        let yaml = format!(
            r#"
pipeline:
  name: bench_scaling
  concurrency:
    threads: {threads}
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - {{ name: id, type: string }}

- type: transform
  name: heavy_transform
  input: src
  config:
    cxl: 'let base = f0.to_int() + f2.to_int()

      emit out0 = base * 2

      emit out1 = f1.upper()

      emit out2 = f3.trim()

      emit out3 = if f0.to_int() > 500000 then "high" else "low"

      emit out4 = f5 ?? f3 ?? "default"

      emit out5 = f0.to_float().round(2)

      emit out6 = f7.lower()

      emit out7 = base + f4.to_int()

      emit out8 = f0.to_string()

      '
- type: output
  name: out
  input: heavy_transform
  config:
    name: out
    path: output.csv
    type: csv
"#
        );
        let config = parse_config(&yaml).unwrap();
        let params = test_params();

        group.throughput(Throughput::Elements(record_count as u64));
        group.bench_with_input(BenchmarkId::new("threads", threads), &threads, |b, _| {
            b.iter(|| {
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .unwrap();
                pool.install(|| {
                    let readers: HashMap<String, Box<dyn std::io::Read + Send>> =
                        HashMap::from([(
                            "src".to_string(),
                            Box::new(Cursor::new(csv_bytes.clone()))
                                as Box<dyn std::io::Read + Send>,
                        )]);
                    let buf = BenchBuffer::new();
                    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
                        "out".to_string(),
                        Box::new(buf.clone()) as Box<dyn Write + Send>,
                    )]);
                    let plan = clinker_core::config::PipelineConfig::compile(
                        &config,
                        &clinker_core::config::CompileContext::default(),
                    )
                    .expect("compile");
                    let report = PipelineExecutor::run_plan_with_readers_writers(
                        &plan, readers, writers, &params,
                    )
                    .unwrap();
                    black_box(report);
                });
            });
        });
    }
    group.finish();
}

// ── Parallel scaling: two-pass with windows ────────────────────────

fn bench_scaling_two_pass(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling_two_pass");
    group.sample_size(20);

    let record_count = MEDIUM;
    let csv_bytes = CsvPayload::generate(
        record_count,
        &clinker_bench_support::FieldKind::default_layout(10),
        16,
        42,
    );

    for threads in [1, 2, 4, 8] {
        let yaml = format!(
            r#"
pipeline:
  name: bench_scaling_2pass
  concurrency:
    threads: {threads}
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - {{ name: id, type: string }}

- type: transform
  name: windowed
  input: src
  config:
    cxl: 'emit amount = f0.to_int()

      emit group = f1

      emit total = $window.count()

      emit first_val = $window.first().f0

      '
    analytic_window:
      group_by:
      - f1
      sort_by:
      - field: f0
- type: output
  name: out
  input: windowed
  config:
    name: out
    path: output.csv
    type: csv
"#
        );
        let config = parse_config(&yaml).unwrap();
        let params = test_params();

        group.throughput(Throughput::Elements(record_count as u64));
        group.bench_with_input(BenchmarkId::new("threads", threads), &threads, |b, _| {
            b.iter(|| {
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .unwrap();
                pool.install(|| {
                    let readers: HashMap<String, Box<dyn std::io::Read + Send>> =
                        HashMap::from([(
                            "src".to_string(),
                            Box::new(Cursor::new(csv_bytes.clone()))
                                as Box<dyn std::io::Read + Send>,
                        )]);
                    let buf = BenchBuffer::new();
                    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
                        "out".to_string(),
                        Box::new(buf.clone()) as Box<dyn Write + Send>,
                    )]);
                    let plan = clinker_core::config::PipelineConfig::compile(
                        &config,
                        &clinker_core::config::CompileContext::default(),
                    )
                    .expect("compile");
                    let report = PipelineExecutor::run_plan_with_readers_writers(
                        &plan, readers, writers, &params,
                    )
                    .unwrap();
                    black_box(report);
                });
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_scaling_streaming, bench_scaling_two_pass);
criterion_main!(benches);
