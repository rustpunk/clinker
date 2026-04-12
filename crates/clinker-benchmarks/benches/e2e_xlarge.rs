//! End-to-end pipeline benchmarks at XLARGE (1M record) scale.
//!
//! Requires `--features bench-xlarge`. Separate binary to avoid XLARGE setup
//! overhead when running S/M/L benchmarks (Criterion Issue #763: setup runs
//! regardless of regex filter).

use clinker_bench_support::{Scale, cache::BenchDataCache, discover_pipeline_configs};
use clinker_benchmarks::runner::BenchPipelineRunner;
use criterion::{BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group};
use std::time::Duration;

fn pipelines_base() -> std::path::PathBuf {
    clinker_bench_support::workspace_root().join("benches/pipelines")
}

fn bench_xlarge(c: &mut Criterion) {
    let cache = BenchDataCache::default_location();
    let runner = BenchPipelineRunner::new(cache);
    let configs = discover_pipeline_configs(&pipelines_base());

    for entry in &configs {
        let mut group = c.benchmark_group(format!("e2e/{}/{}", entry.category, entry.name));
        group.sample_size(10);
        group.sampling_mode(SamplingMode::Flat);
        group.warm_up_time(Duration::from_secs(1));
        group.throughput(Throughput::Elements(Scale::XLarge.record_count() as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(Scale::XLarge.label()),
            &Scale::XLarge,
            |b, &scale| {
                b.iter(|| runner.run(&entry.path, scale, 20));
            },
        );
        group.finish();
    }
}

criterion_group!(benches, bench_xlarge);

fn main() {
    benches();
    Criterion::default().configure_from_args().final_summary();
}
