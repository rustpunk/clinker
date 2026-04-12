//! Benchmark: composition expansion (compile) latency.
//! Target: < 10ms for a pipeline with 20 composition call sites.

use std::path::PathBuf;

use criterion::{Criterion, criterion_group, criterion_main};

use clinker_core::config::{CompileContext, PipelineConfig};
use clinker_core::yaml;

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn bench_composition_expansion(c: &mut Criterion) {
    let root = fixture_root();
    let yaml_path = root.join("pipelines/nested_composition_pipeline.yaml");
    let yaml_str = std::fs::read_to_string(&yaml_path).expect("read fixture");
    let config: PipelineConfig = yaml::from_str(&yaml_str).expect("parse fixture");
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));

    c.bench_function("composition_expansion_nested", |b| {
        b.iter(|| {
            let plan = PipelineConfig::compile(&config, &ctx).expect("compile");
            criterion::black_box(plan);
        });
    });
}

criterion_group!(benches, bench_composition_expansion);
criterion_main!(benches);
