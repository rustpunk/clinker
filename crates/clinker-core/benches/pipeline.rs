use clinker_bench_support::{CsvPayload, MEDIUM, SMALL};
use clinker_core::config::parse_config;
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use indexmap::IndexMap;
use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::sync::{Arc, Mutex};

/// Thread-safe in-memory buffer (duplicates test_helpers::SharedBuffer for bench use).
#[derive(Clone, Default)]
struct BenchBuffer(Arc<Mutex<Vec<u8>>>);

impl BenchBuffer {
    fn new() -> Self {
        Self::default()
    }
    fn len(&self) -> usize {
        self.0.lock().unwrap().len()
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
        execution_id: "bench-exec".to_string(),
        batch_id: "bench-batch".to_string(),
        pipeline_vars: IndexMap::new(),
    }
}

// ── Streaming pipeline (no windows) ────────────────────────────────

fn bench_e2e_streaming(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_streaming");
    let yaml = r#"
pipeline:
  name: bench_streaming
inputs:
  - name: src
    path: input.csv
    type: csv
transformations:
  - name: transform
    cxl: |
      let base = f0.to_int() + f2.to_int()
      emit out0 = base * 2
      emit out1 = f1.upper()
      emit out2 = if f0.to_int() > 500000 then "high" else "low"
outputs:
  - name: out
    path: output.csv
    type: csv
error_handling:
  strategy: continue
"#;
    let config = parse_config(yaml).unwrap();
    let params = test_params();

    for count in [SMALL, MEDIUM] {
        let csv_bytes = CsvPayload::generate(count, 5, 16);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
                    "src".to_string(),
                    Box::new(Cursor::new(csv_bytes.clone())) as Box<dyn std::io::Read + Send>,
                )]);
                let buf = BenchBuffer::new();
                let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
                    "out".to_string(),
                    Box::new(buf.clone()) as Box<dyn Write + Send>,
                )]);
                let report =
                    PipelineExecutor::run_with_readers_writers(&config, readers, writers, &params)
                        .unwrap();
                black_box(report);
            });
        });
    }
    group.finish();
}

// ── Two-pass pipeline (with window functions) ──────────────────────

fn bench_e2e_two_pass(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_two_pass");
    let yaml = r#"
pipeline:
  name: bench_two_pass
inputs:
  - name: src
    path: input.csv
    type: csv
transformations:
  - name: windowed
    cxl: |
      emit amount = f0.to_int()
      emit group = f1
      emit total = $window.count()
      emit first_val = $window.first().f0
    local_window:
      group_by: [f1]
      sort_by:
        - field: f0
outputs:
  - name: out
    path: output.csv
    type: csv
error_handling:
  strategy: continue
"#;
    let config = parse_config(yaml).unwrap();
    let params = test_params();

    for count in [SMALL, MEDIUM] {
        let csv_bytes = CsvPayload::generate(count, 5, 16);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
                    "src".to_string(),
                    Box::new(Cursor::new(csv_bytes.clone())) as Box<dyn std::io::Read + Send>,
                )]);
                let buf = BenchBuffer::new();
                let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
                    "out".to_string(),
                    Box::new(buf.clone()) as Box<dyn Write + Send>,
                )]);
                let report =
                    PipelineExecutor::run_with_readers_writers(&config, readers, writers, &params)
                        .unwrap();
                black_box(report);
            });
        });
    }
    group.finish();
}

// ── Multi-output routing ───────────────────────────────────────────

fn bench_e2e_multi_output(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_multi_output");
    let yaml = r#"
pipeline:
  name: bench_multi_output
inputs:
  - name: src
    path: input.csv
    type: csv
transformations:
  - name: route_transform
    cxl: |
      emit amount = f0.to_int()
      emit label = f1
    route:
      mode: exclusive
      branches:
        - name: high
          condition: "f0.to_int() > 666666"
        - name: medium
          condition: "f0.to_int() > 333333"
      default: low
outputs:
  - name: high
    path: high.csv
    type: csv
  - name: medium
    path: medium.csv
    type: csv
  - name: low
    path: low.csv
    type: csv
error_handling:
  strategy: continue
"#;
    let config = parse_config(yaml).unwrap();
    let params = test_params();

    for count in [SMALL, MEDIUM] {
        let csv_bytes = CsvPayload::generate(count, 5, 16);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
                    "src".to_string(),
                    Box::new(Cursor::new(csv_bytes.clone())) as Box<dyn std::io::Read + Send>,
                )]);
                let high_buf = BenchBuffer::new();
                let medium_buf = BenchBuffer::new();
                let low_buf = BenchBuffer::new();
                let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([
                    (
                        "high".to_string(),
                        Box::new(high_buf) as Box<dyn Write + Send>,
                    ),
                    (
                        "medium".to_string(),
                        Box::new(medium_buf) as Box<dyn Write + Send>,
                    ),
                    (
                        "low".to_string(),
                        Box::new(low_buf) as Box<dyn Write + Send>,
                    ),
                ]);
                let report =
                    PipelineExecutor::run_with_readers_writers(&config, readers, writers, &params)
                        .unwrap();
                black_box(report);
            });
        });
    }
    group.finish();
}

// ── Pipeline with sort ─────────────────────────────────────────────

fn bench_e2e_with_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_with_sort");
    let yaml = r#"
pipeline:
  name: bench_sorted
inputs:
  - name: src
    path: input.csv
    type: csv
transformations:
  - name: sorted_transform
    cxl: |
      emit amount = f0.to_int()
      emit group = f1
      emit running = $window.count()
    local_window:
      group_by: [f1]
      sort_by:
        - field: f0
          order: asc
outputs:
  - name: out
    path: output.csv
    type: csv
error_handling:
  strategy: continue
"#;
    let config = parse_config(yaml).unwrap();
    let params = test_params();

    for count in [SMALL, MEDIUM] {
        let csv_bytes = CsvPayload::generate(count, 5, 16);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
                    "src".to_string(),
                    Box::new(Cursor::new(csv_bytes.clone())) as Box<dyn std::io::Read + Send>,
                )]);
                let buf = BenchBuffer::new();
                let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
                    "out".to_string(),
                    Box::new(buf.clone()) as Box<dyn Write + Send>,
                )]);
                let report =
                    PipelineExecutor::run_with_readers_writers(&config, readers, writers, &params)
                        .unwrap();
                black_box(report);
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_e2e_streaming,
    bench_e2e_two_pass,
    bench_e2e_multi_output,
    bench_e2e_with_sort,
);
criterion_main!(benches);
