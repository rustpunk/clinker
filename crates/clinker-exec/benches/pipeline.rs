use clinker_bench_support::{CsvPayload, MEDIUM, SMALL};
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_plan::config::parse_config;
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
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
        shutdown_token: None,
        ..Default::default()
    }
}

// ── Streaming pipeline (no windows) ────────────────────────────────

fn bench_e2e_streaming(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_streaming");
    let yaml = r#"
pipeline:
  name: bench_streaming
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
      - { name: id, type: string }

- type: transform
  name: transform
  input: src
  config:
    cxl: 'let base = f0.to_int() + f2.to_int()

      emit out0 = base * 2

      emit out1 = f1.upper()

      emit out2 = if f0.to_int() > 500000 then "high" else "low"

      '
- type: output
  name: out
  input: transform
  config:
    name: out
    path: output.csv
    type: csv
"#;
    let config = parse_config(yaml).unwrap();
    let params = test_params();

    for count in [SMALL, MEDIUM] {
        let csv_bytes = CsvPayload::generate(
            count,
            &clinker_bench_support::FieldKind::default_layout(5),
            16,
            42,
        );

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
                    "src".to_string(),
                    clinker_exec::executor::single_file_reader(
                        "test.csv",
                        Box::new(Cursor::new(csv_bytes.clone())),
                    ),
                )]);
                let buf = BenchBuffer::new();
                let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
                    "out".to_string(),
                    Box::new(buf.clone()) as Box<dyn Write + Send>,
                )]);
                let report = PipelineExecutor::run_plan_with_readers_writers(
                    &clinker_plan::config::PipelineConfig::compile(
                        &config,
                        &clinker_plan::config::CompileContext::default(),
                    )
                    .expect("compile"),
                    readers,
                    writers,
                    &params,
                )
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
      - { name: id, type: string }

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
"#;
    let config = parse_config(yaml).unwrap();
    let params = test_params();

    for count in [SMALL, MEDIUM] {
        let csv_bytes = CsvPayload::generate(
            count,
            &clinker_bench_support::FieldKind::default_layout(5),
            16,
            42,
        );

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
                    "src".to_string(),
                    clinker_exec::executor::single_file_reader(
                        "test.csv",
                        Box::new(Cursor::new(csv_bytes.clone())),
                    ),
                )]);
                let buf = BenchBuffer::new();
                let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
                    "out".to_string(),
                    Box::new(buf.clone()) as Box<dyn Write + Send>,
                )]);
                let report = PipelineExecutor::run_plan_with_readers_writers(
                    &clinker_plan::config::PipelineConfig::compile(
                        &config,
                        &clinker_plan::config::CompileContext::default(),
                    )
                    .expect("compile"),
                    readers,
                    writers,
                    &params,
                )
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
      - { name: id, type: string }

- type: transform
  name: route_transform_emit
  input: src
  config:
    cxl: 'emit amount = f0.to_int()

      emit label = f1

      '
- type: route
  name: route_transform
  input: route_transform_emit
  config:
    conditions:
      high: f0.to_int() > 666666
      medium: f0.to_int() > 333333
    default: low
    mode: exclusive
- type: output
  name: high
  input: route_transform
  config:
    name: high
    path: high.csv
    type: csv
- type: output
  name: medium
  input: route_transform
  config:
    name: medium
    path: medium.csv
    type: csv
- type: output
  name: low
  input: route_transform
  config:
    name: low
    path: low.csv
    type: csv
"#;
    let config = parse_config(yaml).unwrap();
    let params = test_params();

    for count in [SMALL, MEDIUM] {
        let csv_bytes = CsvPayload::generate(
            count,
            &clinker_bench_support::FieldKind::default_layout(5),
            16,
            42,
        );

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
                    "src".to_string(),
                    clinker_exec::executor::single_file_reader(
                        "test.csv",
                        Box::new(Cursor::new(csv_bytes.clone())),
                    ),
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
                let report = PipelineExecutor::run_plan_with_readers_writers(
                    &clinker_plan::config::PipelineConfig::compile(
                        &config,
                        &clinker_plan::config::CompileContext::default(),
                    )
                    .expect("compile"),
                    readers,
                    writers,
                    &params,
                )
                .unwrap();
                black_box(report);
            });
        });
    }
    group.finish();
}

// ── Direct materialized fan-out ───────────────────────────────────

fn direct_fan_out_yaml(reader_count: usize, forced_spill_limit: Option<u64>) -> String {
    let mut yaml = String::from(
        r#"
pipeline:
  name: bench_direct_fan_out
"#,
    );
    if let Some(limit) = forced_spill_limit {
        yaml.push_str(&format!(
            "  memory: {{ limit: \"{limit}\", backpressure: spill }}\n"
        ));
    }
    yaml.push_str(
        r#"error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }
"#,
    );
    for reader in 1..=reader_count {
        yaml.push_str(&format!(
            r#"
- type: output
  name: out_{reader}
  input: src
  config:
    name: out_{reader}
    path: out_{reader}.csv
    type: csv
"#,
        ));
    }
    yaml
}

fn run_direct_fan_out_plan(
    plan: &clinker_plan::plan::CompiledPlan,
    reader_count: usize,
    csv_bytes: &[u8],
    params: &PipelineRunParams,
) -> (clinker_exec::executor::ExecutionReport, Vec<BenchBuffer>) {
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        clinker_exec::executor::single_file_reader(
            "test.csv",
            Box::new(Cursor::new(csv_bytes.to_vec())),
        ),
    )]);
    let mut output_buffers = Vec::with_capacity(reader_count);
    let mut writers: HashMap<String, Box<dyn Write + Send>> = HashMap::with_capacity(reader_count);
    for reader in 1..=reader_count {
        let buffer = BenchBuffer::new();
        writers.insert(
            format!("out_{reader}"),
            Box::new(buffer.clone()) as Box<dyn Write + Send>,
        );
        output_buffers.push(buffer);
    }
    let report = PipelineExecutor::run_plan_with_readers_writers(plan, readers, writers, params)
        .expect("direct fan-out benchmark");
    (report, output_buffers)
}

fn calibrated_forced_spill_plan(
    baseline_plan: &clinker_plan::plan::CompiledPlan,
    reader_count: usize,
    csv_bytes: &[u8],
    params: &PipelineRunParams,
) -> (clinker_plan::plan::CompiledPlan, u64) {
    let (baseline, _) = run_direct_fan_out_plan(baseline_plan, reader_count, csv_bytes, params);
    let baseline_rss = baseline
        .peak_rss_bytes
        .expect("forced-spill benchmark requires RSS observation");
    let scan_materialization = baseline.peak_consumer_usage_bytes / 2;
    let hard_limit = baseline_rss.saturating_add(scan_materialization.saturating_mul(2));
    assert!(
        hard_limit.saturating_sub(baseline_rss) > scan_materialization,
        "observed RSS leaves insufficient forced-spill materialization headroom"
    );
    assert!(
        hard_limit.saturating_mul(4) / 5 < baseline_rss,
        "forced-spill soft threshold must stay below the observed RSS"
    );
    let config = parse_config(&direct_fan_out_yaml(reader_count, Some(hard_limit))).unwrap();
    let plan = clinker_plan::config::PipelineConfig::compile(
        &config,
        &clinker_plan::config::CompileContext::default(),
    )
    .expect("compile forced-spill fan-out benchmark");
    (plan, hard_limit)
}

fn bench_e2e_direct_fan_out(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_direct_fan_out");
    let params = test_params();

    for reader_count in 1..=3 {
        let config = parse_config(&direct_fan_out_yaml(reader_count, None)).unwrap();
        let plan = clinker_plan::config::PipelineConfig::compile(
            &config,
            &clinker_plan::config::CompileContext::default(),
        )
        .expect("compile direct fan-out benchmark");

        for count in [SMALL, MEDIUM] {
            let csv_bytes = CsvPayload::generate(
                count,
                &clinker_bench_support::FieldKind::default_layout(5),
                16,
                42,
            );

            group.throughput(Throughput::Elements(count as u64));
            let peak_consumer_usage_bytes = {
                let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
                    "src".to_string(),
                    clinker_exec::executor::single_file_reader(
                        "test.csv",
                        Box::new(Cursor::new(csv_bytes.clone())),
                    ),
                )]);
                let mut writers: HashMap<String, Box<dyn Write + Send>> =
                    HashMap::with_capacity(reader_count);
                for reader in 1..=reader_count {
                    writers.insert(
                        format!("out_{reader}"),
                        Box::new(BenchBuffer::new()) as Box<dyn Write + Send>,
                    );
                }
                PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
                    .expect("direct fan-out benchmark preflight")
                    .peak_consumer_usage_bytes
            };
            eprintln!(
                "e2e_direct_fan_out/{reader_count}_readers/{count}: \
                 peak_consumer_usage_bytes={peak_consumer_usage_bytes}"
            );
            group.bench_with_input(
                BenchmarkId::new(format!("{reader_count}_readers"), count),
                &count,
                |b, _| {
                    b.iter(|| {
                        let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
                            "src".to_string(),
                            clinker_exec::executor::single_file_reader(
                                "test.csv",
                                Box::new(Cursor::new(csv_bytes.clone())),
                            ),
                        )]);
                        let mut output_buffers = Vec::with_capacity(reader_count);
                        let mut writers: HashMap<String, Box<dyn Write + Send>> =
                            HashMap::with_capacity(reader_count);
                        for reader in 1..=reader_count {
                            let buffer = BenchBuffer::new();
                            writers.insert(
                                format!("out_{reader}"),
                                Box::new(buffer.clone()) as Box<dyn Write + Send>,
                            );
                            output_buffers.push(buffer);
                        }
                        let report = PipelineExecutor::run_plan_with_readers_writers(
                            &plan, readers, writers, &params,
                        )
                        .unwrap();
                        black_box(report.peak_consumer_usage_bytes);
                        black_box(&output_buffers);
                        black_box(report);
                    });
                },
            );
        }
    }
    group.finish();
}

/// Shared-slot sequential re-scan under controlled spill pressure. A baseline
/// preflight measures this process with the same inputs and outputs, then sets
/// the hard limit to the observed RSS plus twice one sequential scan. That
/// leaves explicit scan headroom while keeping the 0.8 soft threshold below
/// the observed RSS even when allocator pages from an earlier case are later
/// released. Calibration runs before every measured iteration so allocator
/// growth cannot make the limit stale. The forced preflight rejects either an
/// accidental in-memory run or insufficient materialization headroom.
fn bench_e2e_direct_fan_out_forced_spill(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_direct_fan_out_forced_spill");
    let params = test_params();

    for reader_count in 2..=3 {
        let baseline_config = parse_config(&direct_fan_out_yaml(reader_count, None)).unwrap();
        let baseline_plan = clinker_plan::config::PipelineConfig::compile(
            &baseline_config,
            &clinker_plan::config::CompileContext::default(),
        )
        .expect("compile fan-out benchmark baseline");

        for count in [SMALL, MEDIUM] {
            let csv_bytes = CsvPayload::generate(
                count,
                &clinker_bench_support::FieldKind::default_layout(5),
                16,
                42,
            );
            let (plan, hard_limit) =
                calibrated_forced_spill_plan(&baseline_plan, reader_count, &csv_bytes, &params);
            let (preflight, _) = run_direct_fan_out_plan(&plan, reader_count, &csv_bytes, &params);
            assert!(
                preflight.cumulative_spill_bytes > 0,
                "forced-spill benchmark must exercise the shared spill path"
            );
            eprintln!(
                "e2e_direct_fan_out_forced_spill/{reader_count}_readers/{count}: \
                 hard_limit={hard_limit}, peak_consumer_usage_bytes={}, cumulative_spill_bytes={}",
                preflight.peak_consumer_usage_bytes, preflight.cumulative_spill_bytes
            );

            group.throughput(Throughput::Elements(count as u64));
            group.bench_with_input(
                BenchmarkId::new(format!("{reader_count}_readers"), count),
                &count,
                |b, _| {
                    b.iter_batched(
                        || {
                            calibrated_forced_spill_plan(
                                &baseline_plan,
                                reader_count,
                                &csv_bytes,
                                &params,
                            )
                            .0
                        },
                        |plan| {
                            let (report, output_buffers) =
                                run_direct_fan_out_plan(&plan, reader_count, &csv_bytes, &params);
                            black_box(report.peak_consumer_usage_bytes);
                            black_box(report.cumulative_spill_bytes);
                            black_box(output_buffers);
                            black_box(report);
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    group.finish();
}

// ── Pipeline with sort ─────────────────────────────────────────────

fn bench_e2e_with_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_with_sort");
    let yaml = r#"
pipeline:
  name: bench_sorted
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
      - { name: id, type: string }

- type: transform
  name: sorted_transform
  input: src
  config:
    cxl: 'emit amount = f0.to_int()

      emit group = f1

      emit running = $window.count()

      '
    analytic_window:
      group_by:
      - f1
      sort_by:
      - field: f0
        order: asc
- type: output
  name: out
  input: sorted_transform
  config:
    name: out
    path: output.csv
    type: csv
"#;
    let config = parse_config(yaml).unwrap();
    let params = test_params();

    for count in [SMALL, MEDIUM] {
        let csv_bytes = CsvPayload::generate(
            count,
            &clinker_bench_support::FieldKind::default_layout(5),
            16,
            42,
        );

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
                    "src".to_string(),
                    clinker_exec::executor::single_file_reader(
                        "test.csv",
                        Box::new(Cursor::new(csv_bytes.clone())),
                    ),
                )]);
                let buf = BenchBuffer::new();
                let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
                    "out".to_string(),
                    Box::new(buf.clone()) as Box<dyn Write + Send>,
                )]);
                let report = PipelineExecutor::run_plan_with_readers_writers(
                    &clinker_plan::config::PipelineConfig::compile(
                        &config,
                        &clinker_plan::config::CompileContext::default(),
                    )
                    .expect("compile"),
                    readers,
                    writers,
                    &params,
                )
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
    bench_e2e_direct_fan_out,
    bench_e2e_direct_fan_out_forced_spill,
    bench_e2e_with_sort,
);
criterion_main!(benches);
