//! End-to-end pipeline benchmark matrix across all discovered YAML configs.
//!
//! `cargo test --benches` executes one Small-scale correctness preflight per
//! config. Real benchmark invocations retain the Small, Medium, and Large
//! Criterion matrix. Custom `main()` also supports an optional summary pass
//! gated on `CLINKER_BENCH_SUMMARY=1`.

use std::ffi::OsStr;

use clinker_bench_support::{ConfigEntry, Scale, cache::BenchDataCache, discover_pipeline_configs};
use clinker_benchmarks::runner::BenchPipelineRunner;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};

fn pipelines_base() -> std::path::PathBuf {
    clinker_bench_support::workspace_root().join("benches/pipelines")
}

fn verify_pipeline(runner: &BenchPipelineRunner, entry: &ConfigEntry) {
    runner.run(&entry.path, Scale::Small).unwrap_or_else(|e| {
        panic!(
            "preflight failed for {}/{}: {e}",
            entry.category, entry.name
        )
    });
}

fn run_preflight_matrix() {
    let cache = BenchDataCache::default_location();
    let runner = BenchPipelineRunner::new(cache);
    let configs = discover_pipeline_configs(&pipelines_base());

    for entry in &configs {
        println!(
            "Testing preflight {}/{} at small scale",
            entry.category, entry.name
        );
        verify_pipeline(&runner, entry);
    }

    println!(
        "Benchmark preflight passed for {} pipeline configs",
        configs.len()
    );
}

/// Whether Criterion will use test mode without any filtering or reporting
/// options that it must parse itself.
fn is_plain_test_mode(args: impl IntoIterator<Item = impl AsRef<OsStr>>) -> bool {
    let mut bench = false;
    let mut test = false;

    for arg in args {
        match arg.as_ref().to_str() {
            Some("--bench") => bench = true,
            Some("--test") => test = true,
            _ => return false,
        }
    }

    !bench || test
}

fn bench_e2e(c: &mut Criterion) {
    let cache = BenchDataCache::default_location();
    let runner = BenchPipelineRunner::new(cache);
    let configs = discover_pipeline_configs(&pipelines_base());

    for entry in &configs {
        // Pre-flight: verify pipeline compiles and runs correctly at Small scale
        // before entering the timed loop. The test-mode entry point below calls
        // the same helper without also traversing the timed size matrix.
        verify_pipeline(&runner, entry);

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
    let args = std::env::args_os().skip(1);
    if std::env::var_os("CLINKER_BENCH_SUMMARY").is_none() && is_plain_test_mode(args) {
        run_preflight_matrix();
        return;
    }

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
