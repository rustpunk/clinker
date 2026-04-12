//! Benchmark: channel overlay merge with 100 bound parameters.
//! Target: < 2ms per apply_channel_overlay call.

use std::path::PathBuf;

use criterion::{Criterion, criterion_group, criterion_main};

use clinker_channel::binding::ChannelBinding;
use clinker_channel::overlay::apply_channel_overlay;
use clinker_core::config::composition::{LayerKind, ResolvedValue};
use clinker_core::span::Span;

/// Build a synthetic channel binding with `n` config entries.
fn synthetic_binding(n: usize) -> ChannelBinding {
    let mut yaml = String::from(
        "channel:\n  name: bench_channel\n  target: ./main.yaml\nconfig:\n  default:\n",
    );
    for i in 0..n {
        yaml.push_str(&format!("    node{i}.param{i}: {i}\n"));
    }
    ChannelBinding::from_yaml_bytes(yaml.as_bytes(), PathBuf::from("bench.channel.yaml"))
        .expect("parse synthetic channel")
}

fn bench_channel_merge(c: &mut Criterion) {
    let binding = synthetic_binding(100);

    c.bench_function("channel_merge_100_params", |b| {
        b.iter_batched(
            || {
                // Setup: compile a real fixture plan and populate provenance
                // with 100 synthetic entries matching the binding's keys.
                let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .unwrap()
                    .join("clinker-core/tests/fixtures");
                let yaml_path = root.join("pipelines/nested_composition_pipeline.yaml");
                let yaml = std::fs::read_to_string(&yaml_path).unwrap();
                let config: clinker_core::config::PipelineConfig =
                    clinker_core::yaml::from_str(&yaml).unwrap();
                let ctx = clinker_core::config::CompileContext::with_pipeline_dir(
                    &root,
                    PathBuf::from("pipelines"),
                );
                let mut plan =
                    clinker_core::config::PipelineConfig::compile(&config, &ctx).unwrap();

                // Seed 100 provenance entries matching the binding's keys.
                let prov = plan.provenance_mut();
                for i in 0..100 {
                    prov.insert(
                        format!("node{i}"),
                        format!("param{i}"),
                        ResolvedValue::new(
                            serde_json::json!(0),
                            LayerKind::CompositionDefault,
                            Span::SYNTHETIC,
                        ),
                    );
                }
                plan
            },
            |mut plan| {
                apply_channel_overlay(&mut plan, &binding);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_channel_merge);
criterion_main!(benches);
