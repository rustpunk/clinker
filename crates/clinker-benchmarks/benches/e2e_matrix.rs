//! End-to-end pipeline benchmark matrix: S/M/L scales across all discovered YAML configs.
//!
//! Custom `main()` replaces `criterion_main!` to support an optional summary pass
//! gated on `CLINKER_BENCH_SUMMARY=1`.

use clinker_bench_support::{Scale, cache::BenchDataCache, discover_pipeline_configs};
use clinker_benchmarks::runner::BenchPipelineRunner;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};

fn pipelines_base() -> std::path::PathBuf {
    clinker_bench_support::workspace_root().join("benches/pipelines")
}

fn bench_e2e(c: &mut Criterion) {
    let cache = BenchDataCache::default_location();
    let runner = BenchPipelineRunner::new(cache);
    let configs = discover_pipeline_configs(&pipelines_base());

    for entry in &configs {
        // Pre-flight: verify pipeline compiles and runs correctly at Small scale
        // before entering the timed loop. Surfaces runtime errors (bad CXL, schema
        // mismatches) as panics so `cargo test --benches` catches them.
        runner
            .run(&entry.path, Scale::Small)
            .unwrap_or_else(|e| panic!("pre-flight failed for {}/{}: {e}", entry.category, entry.name));

        let mut group = c.benchmark_group(format!("e2e/{}/{}", entry.category, entry.name));
        for &scale in &[Scale::Small, Scale::Medium, Scale::Large] {
            group.throughput(Throughput::Elements(scale.record_count() as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(scale.label()),
                &scale,
                |b, &scale| {
                    b.iter(|| runner.run(&entry.path, scale));
                },
            );
        }
        group.finish();
    }
}

criterion_group!(benches, bench_e2e);

fn main() {
    benches();

    if std::env::var("CLINKER_BENCH_SUMMARY").is_ok() {
        let cache = BenchDataCache::default_location();
        let runner = BenchPipelineRunner::new(cache);
        let configs = discover_pipeline_configs(&pipelines_base());
        let mut results = Vec::new();
        for entry in &configs {
            if let Ok(report) = runner.run(&entry.path, Scale::Medium) {
                clinker_benchmarks::report::print_summary_table(
                    &format!("{}/{}", entry.category, entry.name),
                    "medium",
                    &report,
                );
                results.push(clinker_benchmarks::report::bench_result_from(
                    entry, "medium", &report,
                ));
            }
        }
        let output_path =
            clinker_bench_support::workspace_root().join("target/bench-results/summary.json");
        clinker_benchmarks::report::write_ci_json(&results, &output_path);
    }

    Criterion::default().configure_from_args().final_summary();
}
