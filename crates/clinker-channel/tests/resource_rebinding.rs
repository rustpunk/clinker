use std::path::{Path, PathBuf};

use clinker_channel::{ChannelManifest, Group, OverlayFile, resolve};
use clinker_plan::config::composition::LayerKind;
use clinker_plan::config::{
    ChannelLayout, CompileContext, GroupLayout, PipelineConfig, PipelineNode, ShardScheme,
};

const PIPELINE: &str = r#"pipeline: { name: base }
nodes:
  - type: source
    name: source
    config:
      name: source
      type: csv
      path: input.csv
      schema: [{ name: id, type: string }]
  - type: composition
    name: reader
    input: source
    use: ../composition/reader.comp.yaml
    inputs: { input: source }
    resources: { orders: base_orders }
  - type: output
    name: output
    input: reader.out
    config: { name: output, type: csv, path: output.csv }
"#;

const COMPOSITION: &str = r#"_compose:
  name: reader
  inputs:
    input:
      schema: [{ name: id, type: string }]
  outputs: { out: shape }
  config_schema: {}
  resources_schema:
    orders: { kind: file, required: true }
nodes:
  - type: transform
    name: shape
    input: input
    config:
      cxl: |
        emit id = id
"#;

fn write(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("parent directory");
    }
    std::fs::write(path, contents).expect("fixture");
}

fn workspace() -> tempfile::TempDir {
    let workspace = tempfile::tempdir().expect("workspace");
    write(&workspace.path().join("pipeline/base.yaml"), PIPELINE);
    write(
        &workspace.path().join("composition/reader.comp.yaml"),
        COMPOSITION,
    );
    for (name, value) in [
        ("base.csv", "base"),
        ("group.csv", "group"),
        ("channel.csv", "channel"),
        ("target.csv", "target"),
    ] {
        write(&workspace.path().join("data").join(name), value);
    }
    write(
        &workspace.path().join("clinker.toml"),
        r#"[catalog.resources.base_orders]
kind = "file"
path = "data/base.csv"
[catalog.resources.group_orders]
kind = "file"
path = "data/group.csv"
[catalog.resources.channel_orders]
kind = "file"
path = "data/channel.csv"
[catalog.resources.target_orders]
kind = "file"
path = "data/target.csv"
"#,
    );
    workspace
}

fn channel_layout() -> ChannelLayout {
    ChannelLayout {
        root: PathBuf::from("channel"),
        shard: ShardScheme::None,
    }
}

fn group_layout() -> GroupLayout {
    GroupLayout {
        root: PathBuf::from("group"),
    }
}

fn parse_pipeline(root: &Path) -> PipelineConfig {
    let yaml = std::fs::read_to_string(root.join("pipeline/base.yaml")).expect("pipeline");
    clinker_plan::yaml::from_str(&yaml).expect("pipeline parses")
}

fn effective_plan(
    root: &Path,
    channel: Option<&str>,
    groups: &[String],
) -> clinker_plan::plan::CompiledPlan {
    let resolution = resolve(
        root,
        &channel_layout(),
        &group_layout(),
        "base",
        channel,
        groups,
        true,
    )
    .expect("overlay resolves");
    let config = parse_pipeline(root);
    let mut context = CompileContext::with_pipeline_dir(root, PathBuf::from("pipeline"));
    context.overlay_ops = resolution.op_stream().to_vec();
    config.compile(&context).expect("effective plan compiles")
}

fn binding(plan: &clinker_plan::plan::CompiledPlan) -> &clinker_plan::ResourceBinding {
    let PipelineNode::Composition { resources, .. } = &plan.config().nodes[1].value else {
        panic!("expected composition");
    };
    &resources["orders"]
}

#[test]
fn channel_resource_rebinds_declared_slot_with_provenance() {
    let workspace = workspace();
    write(
        &workspace.path().join("channel/acme/channel.cfg.yaml"),
        "channel: { name: acme, targets: [base] }\nresources: { reader.orders: { value: channel_orders } }\n",
    );
    write(
        &workspace.path().join("channel/acme/base.channel.yaml"),
        "channel: { target: ../../pipeline/base.yaml }\n",
    );
    let plan = effective_plan(workspace.path(), Some("acme"), &[]);
    let binding = binding(&plan);
    assert_eq!(binding.logical_id().as_str(), "channel_orders");
    assert_eq!(
        binding.provenance().winning_layer().expect("winner").kind,
        LayerKind::ChannelWide
    );
    assert_eq!(binding.provenance().provenance.len(), 2);
}

#[test]
fn channel_group_and_target_resource_precedence_keeps_every_attempt() {
    let workspace = workspace();
    write(
        &workspace.path().join("group/enterprise.group.yaml"),
        "group:\n  name: enterprise\n  targets: { pipelines: [base] }\n  priority: 10\nresources: { reader.orders: { value: group_orders } }\n",
    );
    write(
        &workspace.path().join("channel/acme/channel.cfg.yaml"),
        "channel: { name: acme, targets: [base] }\nresources: { reader.orders: { value: channel_orders } }\n",
    );
    write(
        &workspace.path().join("channel/acme/base.channel.yaml"),
        "channel: { target: ../../pipeline/base.yaml }\nresources: { reader.orders: { value: target_orders } }\n",
    );
    let plan = effective_plan(workspace.path(), Some("acme"), &["enterprise".to_string()]);
    let binding = binding(&plan);
    assert_eq!(binding.logical_id().as_str(), "target_orders");
    let provenance = &binding.provenance().provenance;
    assert_eq!(provenance.len(), 4);
    assert!(
        provenance
            .iter()
            .any(|layer| matches!(layer.kind, LayerKind::Group { .. }))
    );
    assert!(
        provenance
            .iter()
            .any(|layer| layer.kind == LayerKind::ChannelWide)
    );
    assert!(
        provenance
            .iter()
            .any(|layer| layer.kind == LayerKind::ChannelPerTarget)
    );
}

#[test]
fn channel_fixed_resource_binding_blocks_higher_layer() {
    let workspace = workspace();
    write(
        &workspace.path().join("channel/acme/channel.cfg.yaml"),
        "channel: { name: acme, targets: [base] }\nresources: { reader.orders: { value: channel_orders, fixed: true } }\n",
    );
    write(
        &workspace.path().join("channel/acme/base.channel.yaml"),
        "channel: { target: ../../pipeline/base.yaml }\nresources: { reader.orders: { value: target_orders } }\n",
    );
    let plan = effective_plan(workspace.path(), Some("acme"), &[]);
    let binding = binding(&plan);
    assert_eq!(binding.logical_id().as_str(), "channel_orders");
    assert_eq!(
        binding.provenance().winning_layer().expect("winner").kind,
        LayerKind::ChannelWide
    );
    assert_eq!(binding.provenance().provenance.len(), 3);
}

#[test]
fn channel_unknown_slot_fails_at_typed_composition_boundary() {
    let workspace = workspace();
    write(
        &workspace.path().join("channel/acme/channel.cfg.yaml"),
        "channel: { name: acme, targets: [base] }\nresources: { reader.internal: { value: channel_orders } }\n",
    );
    write(
        &workspace.path().join("channel/acme/base.channel.yaml"),
        "channel: { target: ../../pipeline/base.yaml }\n",
    );
    let resolution = resolve(
        workspace.path(),
        &channel_layout(),
        &group_layout(),
        "base",
        Some("acme"),
        &[],
        true,
    )
    .expect("overlay resolves");
    let config = parse_pipeline(workspace.path());
    let mut context =
        CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipeline"));
    context.overlay_ops = resolution.op_stream().to_vec();
    let diagnostics = config.compile(&context).expect_err("unknown slot fails");
    assert!(diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "E103"
            && diagnostic.message.contains("internal")
            && diagnostic.message.contains("not declared")
    }));
}

#[test]
fn channel_credential_selectors_and_literal_payloads_are_rejected() {
    for yaml in [
        "channel: { name: acme, targets: [base] }\nresources: { reader.credential_profile: { value: prod } }\n",
        "channel: { name: acme, targets: [base] }\nresources: { reader.orders: { value: { token: literal-secret } } }\n",
    ] {
        let error =
            ChannelManifest::from_yaml_bytes(yaml.as_bytes(), PathBuf::from("channel.cfg.yaml"))
                .expect_err("credentials are not an overlay resource surface")
                .to_string();
        assert!(
            error.contains("credential")
                || error.contains("secret-free logical catalog identities")
                || error.contains("logical resource identity")
                || error.contains("invalid type")
                || error.contains("expected string scalar"),
            "{error}"
        );
        assert!(!error.contains("literal-secret"), "{error}");
    }

    let group_error = Group::from_yaml_bytes(
        b"group:\n  name: enterprise\n  targets: { pipelines: [base] }\nresources: { reader.orders: { value: { token: literal-secret } } }\n",
        PathBuf::from("enterprise.group.yaml"),
    )
    .expect_err("group literal payload is rejected")
    .to_string();
    assert!(group_error.contains("secret-free"), "{group_error}");
    assert!(!group_error.contains("literal-secret"), "{group_error}");

    let target_error = OverlayFile::from_yaml_bytes(
        b"channel: { target: base }\nresources: { reader.orders: { value: { token: literal-secret } } }\n",
        PathBuf::from("base.channel.yaml"),
    )
    .expect_err("target literal payload is rejected")
    .to_string();
    assert!(target_error.contains("secret-free"), "{target_error}");
    assert!(!target_error.contains("literal-secret"), "{target_error}");
}

#[test]
fn channel_group_and_target_parsers_share_the_strict_resource_leaf() {
    let group = Group::from_yaml_bytes(
        b"group:\n  name: enterprise\n  targets: { pipelines: [base] }\nresources: { reader.orders: { value: group_orders, fixed: true } }\n",
        PathBuf::from("enterprise.group.yaml"),
    )
    .expect("group resource parses");
    assert_eq!(
        group.resources["reader.orders"].value.as_str(),
        "group_orders"
    );
    assert!(group.resources["reader.orders"].fixed);

    let target = OverlayFile::from_yaml_bytes(
        b"channel: { target: base }\nresources: { reader.orders: { value: target_orders } }\n",
        PathBuf::from("base.channel.yaml"),
    )
    .expect("target resource parses");
    assert_eq!(
        target.resources["reader.orders"].value.as_str(),
        "target_orders"
    );
}

#[test]
fn channel_resource_rebinding_changes_semantic_fingerprint() {
    let workspace = workspace();
    write(
        &workspace.path().join("channel/acme/channel.cfg.yaml"),
        "channel: { name: acme, targets: [base] }\nresources: { reader.orders: { value: channel_orders } }\n",
    );
    write(
        &workspace.path().join("channel/acme/base.channel.yaml"),
        "channel: { target: ../../pipeline/base.yaml }\n",
    );
    let base = effective_plan(workspace.path(), None, &[])
        .semantic_fingerprint()
        .expect("base fingerprint");
    let rebound = effective_plan(workspace.path(), Some("acme"), &[])
        .semantic_fingerprint()
        .expect("rebound fingerprint");
    assert_ne!(base, rebound);
}

#[test]
fn channel_resource_noop_rebinding_preserves_semantic_fingerprint() {
    let workspace = workspace();
    let base = effective_plan(workspace.path(), None, &[])
        .semantic_fingerprint()
        .expect("base fingerprint");
    write(
        &workspace.path().join("channel/acme/channel.cfg.yaml"),
        "channel: { name: acme, targets: [base] }\nresources: { reader.orders: { value: base_orders } }\n",
    );
    write(
        &workspace.path().join("channel/acme/base.channel.yaml"),
        "channel: { target: ../../pipeline/base.yaml }\n",
    );
    let rebound = effective_plan(workspace.path(), Some("acme"), &[])
        .semantic_fingerprint()
        .expect("rebound fingerprint");
    assert_eq!(base, rebound, "the winning logical identity is unchanged");
}

#[test]
fn channel_nested_resource_address_cannot_penetrate_sealed_body() {
    let yaml = "channel: { name: acme, targets: [base] }\nresources: { reader.nested.orders: { value: target_orders } }\n";
    let error =
        ChannelManifest::from_yaml_bytes(yaml.as_bytes(), PathBuf::from("channel.cfg.yaml"))
            .expect_err("three-part nested resource address is rejected")
            .to_string();
    assert!(error.contains("reader.nested.orders"), "{error}");
    assert!(error.contains("malformed"), "{error}");
    assert!(!error.contains("target.csv"), "{error}");
}
