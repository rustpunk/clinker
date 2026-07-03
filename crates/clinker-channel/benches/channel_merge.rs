//! Benchmark: channel/group overlay config clobber with 100 bound parameters.
//! Target: < 2ms per `apply_config_and_vars` call.

use std::path::PathBuf;

use criterion::{Criterion, criterion_group, criterion_main};

use clinker_channel::{OverlayResolution, resolve};
use clinker_core_types::Span;
use clinker_plan::config::composition::{LayerKind, ResolvedValue};
use clinker_plan::config::{ChannelLayout, GroupLayout, ShardScheme};

/// Build a tempdir workspace with a `bench` channel whose per-target overlay
/// carries `n` `config:` clobber entries, and resolve it once. The resolution
/// (discovery + parse) is off the hot path; the benched call is the clobber.
fn resolved_overlay(dir: &std::path::Path, n: usize) -> OverlayResolution {
    let overlay_dir = dir.join("channel").join("bench");
    std::fs::create_dir_all(&overlay_dir).expect("create channel dir");
    let mut yaml = String::from("channel:\n  target: ../../main.yaml\nconfig:\n");
    for i in 0..n {
        yaml.push_str(&format!("  node{i}.param{i}: {i}\n"));
    }
    std::fs::write(overlay_dir.join("main.channel.yaml"), yaml).expect("write overlay");

    let channel_layout = ChannelLayout {
        root: PathBuf::from("channel"),
        shard: ShardScheme::None,
    };
    let group_layout = GroupLayout {
        root: PathBuf::from("group"),
    };
    resolve(
        dir,
        &channel_layout,
        &group_layout,
        "main",
        Some("bench"),
        &[],
        true,
    )
    .expect("resolve overlay")
}

fn bench_channel_merge(c: &mut Criterion) {
    let ws = tempfile::tempdir().expect("tempdir");
    let res = resolved_overlay(ws.path(), 100);

    // A compiled fixture plan the clobber writes onto. The 100 synthetic
    // provenance entries match the overlay keys so every clobber lands.
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("clinker-exec/tests/fixtures");

    c.bench_function("channel_merge_100_params", |b| {
        b.iter_batched(
            || {
                let yaml_path = root.join("pipelines/nested_composition_pipeline.yaml");
                let yaml = std::fs::read_to_string(&yaml_path).unwrap();
                let config: clinker_plan::config::PipelineConfig =
                    clinker_plan::yaml::from_str(&yaml).unwrap();
                let ctx = clinker_plan::config::CompileContext::with_pipeline_dir(
                    &root,
                    PathBuf::from("pipelines"),
                );
                let mut plan =
                    clinker_plan::config::PipelineConfig::compile(&config, &ctx).unwrap();

                let prov = plan.provenance_mut();
                for i in 0..100 {
                    prov.insert(
                        format!("node{i}"),
                        format!("param{i}"),
                        ResolvedValue::new(
                            serde_json::json!(0),
                            LayerKind::PipelineDefault,
                            Span::SYNTHETIC,
                        ),
                    );
                }
                (config, plan)
            },
            |(config, mut plan)| {
                res.apply_config_and_vars(&mut plan, &config);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_channel_merge);
criterion_main!(benches);
