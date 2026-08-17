//! Catalog identity and target-scope validation for channels and groups.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clinker_channel::{ChannelTarget, Group, discover_channel_resource, validate_group_targets};
use clinker_core_types::{Diagnostic, Severity};
use clinker_plan::config::{CompileContext, PipelineConfig};
use clinker_plan::plan::CompiledPlan;
use clinker_plan::plan::compiled::{ChannelGroupSource, ChannelLayerKind};
use clinker_plan::resources::{CatalogConfig, WorkspaceCatalog};

struct Workspace {
    root: tempfile::TempDir,
    catalog: WorkspaceCatalog,
}

fn write(root: &Path, path: &str, body: &str) {
    let path = root.join(path);
    fs::create_dir_all(path.parent().expect("parent")).expect("create parent");
    fs::write(path, body).expect("write fixture");
}

fn workspace(manifest: &str, target_files: &[(&str, &str)]) -> Workspace {
    let root = tempfile::tempdir().expect("temp workspace");
    write(
        root.path(),
        "pipelines/orders.yaml",
        "pipeline: { name: orders }\nnodes: []\n",
    );
    write(
        root.path(),
        "pipelines/refunds.yaml",
        "pipeline: { name: refunds }\nnodes: []\n",
    );
    write(
        root.path(),
        "compositions/tax.comp.yaml",
        "composition: { name: tax }\ninputs: []\noutputs: []\nnodes: []\n",
    );
    write(root.path(), "channel/acme/channel.cfg.yaml", manifest);
    for (name, body) in target_files {
        write(root.path(), &format!("channel/acme/{name}"), body);
    }

    let config = CatalogConfig {
        pipelines: BTreeMap::from([
            (
                "sales.orders".into(),
                PathBuf::from("pipelines/orders.yaml"),
            ),
            (
                "sales.refunds".into(),
                PathBuf::from("pipelines/refunds.yaml"),
            ),
        ]),
        compositions: BTreeMap::from([(
            "shared.tax".into(),
            PathBuf::from("compositions/tax.comp.yaml"),
        )]),
        channels: BTreeMap::from([("tenant.acme".into(), PathBuf::from("channel/acme"))]),
        ..CatalogConfig::default()
    };
    let catalog = WorkspaceCatalog::load(root.path(), &config).expect("catalog loads");
    Workspace { root, catalog }
}

#[test]
fn targets_channel_selects_only_the_explicit_pipeline_file() {
    let workspace = workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders, sales.refunds]\n",
        &[
            ("first.yaml", "channel: { target: sales.orders }\n"),
            ("second.yaml", "channel: { target: sales.refunds }\n"),
        ],
    );

    let resource = discover_channel_resource(&workspace.catalog, "tenant.acme", "sales.orders")
        .expect("catalog-scoped channel resolves");
    assert_eq!(resource.target.pipeline.as_str(), "sales.orders");
    assert_eq!(resource.overlay.channel.target, "sales.orders");
    assert_eq!(resource.channel_id.as_str(), "tenant.acme");
}

#[test]
fn targets_group_requires_explicit_scope_and_selector_only_narrows_it() {
    let workspace = workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders]\n",
        &[("orders.yaml", "channel: { target: sales.orders }\n")],
    );
    let group = Group::from_yaml_bytes(
        br#"
group:
  name: regulated
  targets:
    pipelines: [sales.orders, sales.refunds]
    compositions: [shared.tax]
  match: labels.region == "west"
"#,
        PathBuf::from("regulated.group.yaml"),
    )
    .expect("group parses");
    let validated = validate_group_targets(&workspace.catalog, &group).expect("targets resolve");
    let selected = ChannelTarget::pipeline("sales.orders").expect("logical id");
    let undeclared = ChannelTarget::pipeline("other.orders").expect("logical id");
    assert!(validated.admits(&selected));
    assert!(
        !validated.admits(&undeclared),
        "selector matches may never widen target scope"
    );
}

#[test]
fn targets_group_rejects_empty_or_omitted_scope() {
    for yaml in [
        br#"group: { name: invalid, targets: { pipelines: [], compositions: [] } }"#.as_slice(),
        br#"group: { name: invalid }"#.as_slice(),
    ] {
        let error = Group::from_yaml_bytes(yaml, PathBuf::from("invalid.group.yaml"))
            .expect_err("groups require a non-empty target set")
            .to_string();
        assert!(error.contains("targets"), "{error}");
        assert!(
            error.contains("pipeline") || error.contains("composition"),
            "{error}"
        );
    }
}

#[test]
fn targets_duplicate_and_undeclared_channel_files_fail_closed() {
    let duplicate = workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders]\n",
        &[
            ("one.yaml", "channel: { target: sales.orders }\n"),
            ("two.yaml", "channel: { target: sales.orders }\n"),
        ],
    );
    let error = discover_channel_resource(&duplicate.catalog, "tenant.acme", "sales.orders")
        .expect_err("duplicate logical target files are ambiguous")
        .to_string();
    assert!(error.contains("sales.orders"), "{error}");
    assert!(error.contains("duplicate"), "{error}");

    let undeclared = workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders]\n",
        &[("refunds.yaml", "channel: { target: sales.refunds }\n")],
    );
    let error = discover_channel_resource(&undeclared.catalog, "tenant.acme", "sales.orders")
        .expect_err("every file must be declared by the manifest")
        .to_string();
    assert!(error.contains("sales.refunds"), "{error}");
    assert!(error.contains("targets"), "{error}");
}

#[test]
fn targets_concurrent_resolution_has_no_cross_target_state() {
    let workspace = workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders, sales.refunds]\n",
        &[
            (
                "a.yaml",
                "channel: { target: sales.orders }\nconfig: { p.limit: { value: 1 } }\n",
            ),
            (
                "b.yaml",
                "channel: { target: sales.refunds }\nconfig: { p.limit: { value: 2 } }\n",
            ),
        ],
    );
    let catalog = Arc::new(workspace.catalog);
    let left = {
        let catalog = Arc::clone(&catalog);
        std::thread::spawn(move || {
            discover_channel_resource(&catalog, "tenant.acme", "sales.orders")
        })
    };
    let right = {
        let catalog = Arc::clone(&catalog);
        std::thread::spawn(move || {
            discover_channel_resource(&catalog, "tenant.acme", "sales.refunds")
        })
    };
    let left = left.join().expect("orders thread").expect("orders target");
    let right = right
        .join()
        .expect("refunds thread")
        .expect("refunds target");
    assert_eq!(left.overlay.config["p.limit"].value, serde_json::json!(1));
    assert_eq!(right.overlay.config["p.limit"].value, serde_json::json!(2));
}

const EXECUTION_PIPELINE: &str = r#"
pipeline:
  name: orders
  vars:
    currency: { type: string, default: base }
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema: [{ name: order_id, type: string }]
  - type: composition
    name: risk
    input: orders
    use: ../compositions/tax.comp.yaml
    inputs: { inp: orders }
    config: { threshold: 0.5 }
  - type: sink
    name: out
    input: risk
    config: { name: out, type: csv, path: out.csv }
"#;

const EXECUTION_COMPOSITION: &str = r#"
_compose:
  name: tax
  inputs:
    inp:
      schema: [{ name: order_id, type: string }]
  outputs: { out: scored }
  config_schema:
    threshold: { type: float, default: 0.5, range: [0.0, 1.0] }
nodes:
  - type: transform
    name: scored
    input: inp
    config: { cxl: "emit order_id = order_id" }
"#;

fn execution_workspace(manifest: &str, overlay: &str, groups: &[(&str, &str)]) -> Workspace {
    let workspace = workspace(manifest, &[("orders.yaml", overlay)]);
    write(
        workspace.root.path(),
        "pipelines/orders.yaml",
        EXECUTION_PIPELINE,
    );
    write(
        workspace.root.path(),
        "compositions/tax.comp.yaml",
        EXECUTION_COMPOSITION,
    );
    for (name, body) in groups {
        write(
            workspace.root.path(),
            &format!("group/{name}.group.yaml"),
            body,
        );
    }
    workspace
}

fn compile_and_apply(
    workspace: &Workspace,
) -> (
    clinker_channel::OverlayResolution,
    CompiledPlan,
    clinker_channel::ChannelOverlayResult,
) {
    let yaml = fs::read_to_string(workspace.root.path().join("pipelines/orders.yaml"))
        .expect("read pipeline");
    let config: PipelineConfig = clinker_plan::yaml::from_str(&yaml).expect("parse pipeline");
    let context =
        CompileContext::with_pipeline_dir(workspace.root.path(), PathBuf::from("pipelines"));
    let mut plan = PipelineConfig::compile(&config, &context).expect("compile pipeline");
    let resolution = clinker_channel::resolve_target_channel(
        workspace.root.path(),
        &workspace.catalog,
        &clinker_plan::config::GroupLayout {
            root: PathBuf::from("group"),
        },
        "sales.orders",
        Some("tenant.acme"),
        &[],
        true,
    )
    .expect("resolve overlays");
    let result = resolution.apply_config_and_vars(&mut plan, &config);
    (resolution, plan, result)
}

fn compile_and_apply_selection(
    workspace: &Workspace,
    channel: Option<&str>,
    groups: &[String],
    auto_groups: bool,
) -> CompiledPlan {
    let yaml = fs::read_to_string(workspace.root.path().join("pipelines/orders.yaml"))
        .expect("read pipeline");
    let config: PipelineConfig = clinker_plan::yaml::from_str(&yaml).expect("parse pipeline");
    let context =
        CompileContext::with_pipeline_dir(workspace.root.path(), PathBuf::from("pipelines"));
    let mut plan = PipelineConfig::compile(&config, &context).expect("compile pipeline");
    let resolution = clinker_channel::resolve_target_channel(
        workspace.root.path(),
        &workspace.catalog,
        &clinker_plan::config::GroupLayout {
            root: PathBuf::from("group"),
        },
        "sales.orders",
        channel,
        groups,
        auto_groups,
    )
    .expect("resolve overlays");
    let result = resolution.apply_config_and_vars(&mut plan, &config);
    assert!(result.diagnostics.is_empty(), "{:?}", result.diagnostics);
    plan
}

fn resolve_config_errors(workspace: &Workspace) -> Vec<Diagnostic> {
    let yaml = fs::read_to_string(workspace.root.path().join("pipelines/orders.yaml"))
        .expect("read pipeline");
    let config: PipelineConfig = clinker_plan::yaml::from_str(&yaml).expect("parse pipeline");
    let context =
        CompileContext::with_pipeline_dir(workspace.root.path(), PathBuf::from("pipelines"));
    let validation_plan = PipelineConfig::compile(&config, &context).expect("compile pipeline");
    let resolution = clinker_channel::resolve_target_channel(
        workspace.root.path(),
        &workspace.catalog,
        &clinker_plan::config::GroupLayout {
            root: PathBuf::from("group"),
        },
        "sales.orders",
        Some("tenant.acme"),
        &[],
        true,
    )
    .expect("resolve overlays");
    resolution
        .resolve_config(&validation_plan)
        .expect_err("invalid candidates must block the executable config fold")
}

fn errors<'a>(diagnostics: &'a [Diagnostic], code: &str) -> Vec<&'a Diagnostic> {
    diagnostics
        .iter()
        .filter(|diagnostic| {
            diagnostic.code == code && matches!(diagnostic.severity, Severity::Error)
        })
        .collect()
}

#[test]
fn target_closure_preserves_catalog_composition_identity() {
    let workspace = execution_workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders]\n",
        "channel: { target: sales.orders }\n",
        &[],
    );
    let resource = discover_channel_resource(&workspace.catalog, "tenant.acme", "sales.orders")
        .expect("target closure resolves");
    assert!(
        resource
            .target
            .compositions
            .iter()
            .any(|identity| identity.as_str() == "shared.tax")
    );
}

#[test]
fn explicit_composition_group_applies_without_a_channel() {
    let workspace = execution_workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders]\n",
        "channel: { target: sales.orders }\n",
        &[(
            "shared",
            "group:\n  name: shared\n  targets: { compositions: [shared.tax] }\nconfig:\n  risk.threshold: { value: 0.7 }\n",
        )],
    );
    let resolution = clinker_channel::resolve_target_channel(
        workspace.root.path(),
        &workspace.catalog,
        &clinker_plan::config::GroupLayout {
            root: PathBuf::from("group"),
        },
        "sales.orders",
        None,
        &["shared".to_string()],
        false,
    )
    .expect("explicit composition group applies from the target closure");
    assert_eq!(resolution.applied_groups().len(), 1);
    assert_eq!(resolution.applied_groups()[0].name, "shared");
}

#[test]
fn precedence_is_pipeline_then_priority_ordered_groups_then_wide_then_target() {
    let workspace = execution_workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders]\nconfig:\n  risk.threshold: { value: 0.9 }\n",
        "channel: { target: sales.orders }\nconfig:\n  risk.threshold: { value: 0.95 }\n",
        &[
            (
                "a-first",
                "group:\n  name: first\n  targets: { pipelines: [sales.orders] }\n  match: 'true'\n  priority: 10\nconfig:\n  risk.threshold: { value: 0.7 }\n",
            ),
            (
                "b-second",
                "group:\n  name: second\n  targets: { compositions: [shared.tax] }\n  match: 'true'\n  priority: 10\nconfig:\n  risk.threshold: { value: 0.8 }\n",
            ),
        ],
    );
    let (resolution, plan, result) = compile_and_apply(&workspace);
    assert!(result.diagnostics.is_empty(), "{:?}", result.diagnostics);
    let names: Vec<&str> = resolution
        .applied_groups()
        .iter()
        .map(|group| group.name.as_str())
        .collect();
    assert_eq!(names, vec!["first", "second"]);
    let resolved = plan
        .provenance()
        .get("risk", "threshold")
        .expect("tracked config");
    assert_eq!(resolved.value, serde_json::json!(0.95));
    assert_eq!(
        resolved.winning_layer().expect("winner").kind,
        clinker_plan::config::composition::LayerKind::ChannelPerTarget
    );
    assert_eq!(
        resolved.provenance.len(),
        5,
        "base plus four overlay layers"
    );
}

#[test]
fn candidates_validate_winning_and_losing_values_at_authored_lines() {
    let workspace = execution_workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders]\nconfig:\n  risk.threshold: { value: wrong-type }\n",
        "channel: { target: sales.orders }\nconfig:\n  risk.threshold: { value: 0.95 }\n",
        &[(
            "losing",
            "group:\n  name: losing\n  targets: { pipelines: [sales.orders] }\n  match: 'true'\nconfig:\n  missing.value: { value: 1 }\n",
        )],
    );
    let (_resolution, _plan, result) = compile_and_apply(&workspace);
    let unknown = errors(&result.diagnostics, "E113");
    let type_errors = errors(&result.diagnostics, "E103");
    assert_eq!(unknown.len(), 1, "losing unknown candidate must still fail");
    assert_eq!(
        type_errors.len(),
        1,
        "losing ill-typed candidate must still fail"
    );
    assert_eq!(unknown[0].primary.span.synthetic_line_number(), Some(6));
    assert_eq!(type_errors[0].primary.span.synthetic_line_number(), Some(5));

    let precompile = resolve_config_errors(&workspace);
    assert_eq!(errors(&precompile, "E113").len(), 1);
    assert_eq!(errors(&precompile, "E103").len(), 1);
}

#[test]
fn fixed_channel_wide_value_rejects_target_override_at_target_span() {
    let workspace = execution_workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders]\nconfig:\n  risk.threshold: { value: 0.9, fixed: true }\n",
        "channel: { target: sales.orders }\nconfig:\n  risk.threshold: { value: 0.95 }\n",
        &[],
    );
    let (_resolution, plan, result) = compile_and_apply(&workspace);
    let fixed = errors(&result.diagnostics, "E103");
    assert_eq!(fixed.len(), 1, "fixed-forbidden override is invalid input");
    assert!(fixed[0].message.contains("fixed"), "{}", fixed[0].message);
    assert_eq!(fixed[0].primary.span.synthetic_line_number(), Some(3));
    assert_eq!(
        plan.provenance()
            .get("risk", "threshold")
            .expect("tracked config")
            .value,
        serde_json::json!(0.9)
    );
}

#[test]
fn variable_candidates_validate_before_fold_and_obey_fixed() {
    let workspace = execution_workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders]\nvars:\n  static:\n    currency: { type: string, default: USD, fixed: true }\n",
        "channel: { target: sales.orders }\nvars:\n  static:\n    currency: { type: string, default: EUR }\n",
        &[(
            "wrong-type",
            "group:\n  name: wrong-type\n  targets: { pipelines: [sales.orders] }\n  match: 'true'\nvars:\n  static:\n    currency: { type: int, default: 7 }\n",
        )],
    );
    let (_resolution, _plan, result) = compile_and_apply(&workspace);
    let type_errors = errors(&result.diagnostics, "E116");
    assert_eq!(type_errors.len(), 1);
    assert_eq!(type_errors[0].primary.span.synthetic_line_number(), Some(7));
    let fixed = errors(&result.diagnostics, "E103");
    assert_eq!(fixed.len(), 1);
    assert!(fixed[0].message.contains("fixed"));
    assert_eq!(fixed[0].primary.span.synthetic_line_number(), Some(4));
    assert_eq!(
        result.static_vars["currency"],
        clinker_record::Value::String("USD".into())
    );
}

#[test]
fn closure_accepts_target_operations_but_rejects_outside_resources() {
    let admitted = execution_workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders]\n",
        "channel: { target: sales.orders }\nsources:\n  orders:\n    options: { has_header: false }\noverrides:\n  - { op: bypass, target: scored }\n",
        &[],
    );
    discover_channel_resource(&admitted.catalog, "tenant.acme", "sales.orders")
        .expect("source and structural targets inside the pipeline closure are admitted");

    write(
        admitted.root.path(),
        "compositions/other.comp.yaml",
        EXECUTION_COMPOSITION,
    );
    write(
        admitted.root.path(),
        "channel/acme/orders.yaml",
        "channel: { target: sales.orders }\noverrides:\n  - { op: add, composition: ../compositions/other.comp.yaml, alias: other, after: orders }\n",
    );
    let error = discover_channel_resource(&admitted.catalog, "tenant.acme", "sales.orders")
        .expect_err("a shared composition outside this pipeline closure must fail")
        .to_string();
    assert!(error.contains("outside"), "{error}");
    assert!(error.contains("target file"), "{error}");
}

#[cfg(unix)]
#[test]
fn channel_containment_rejects_symlinked_target_file() {
    use std::os::unix::fs::symlink;

    let workspace = workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders]\n",
        &[("orders.yaml", "channel: { target: sales.orders }\n")],
    );
    let outside = tempfile::tempdir().expect("outside directory");
    let escaped = outside.path().join("orders.yaml");
    fs::write(&escaped, "channel: { target: sales.orders }\n").expect("outside overlay");
    let target = workspace.root.path().join("channel/acme/orders.yaml");
    fs::remove_file(&target).expect("remove original target");
    symlink(&escaped, &target).expect("symlink target file");

    let error = discover_channel_resource(&workspace.catalog, "tenant.acme", "sales.orders")
        .expect_err("channel target symlinks must fail before parsing")
        .to_string();
    assert!(error.contains("target"), "{error}");
    assert!(error.contains("symlink"), "{error}");
}

#[test]
fn channel_validates_non_selected_target_closure() {
    let workspace = workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders, sales.refunds]\n",
        &[
            ("orders.yaml", "channel: { target: sales.orders }\n"),
            ("refunds.yaml", "channel: { target: sales.refunds }\n"),
        ],
    );
    let outside = tempfile::tempdir().expect("outside directory");
    let escaped = outside.path().join("escaped.comp.yaml");
    fs::write(
        &escaped,
        "composition: { name: escaped }\ninputs: []\noutputs: []\nnodes: []\n",
    )
    .expect("outside composition");
    write(
        workspace.root.path(),
        "pipelines/refunds.yaml",
        &format!(
            "pipeline: {{ name: refunds }}\nnodes:\n  - type: composition\n    name: escaped\n    input: seed\n    use: {}\n    inputs: {{}}\n",
            escaped.display()
        ),
    );

    let error = discover_channel_resource(&workspace.catalog, "tenant.acme", "sales.orders")
        .expect_err("a non-selected target closure must be admitted before selection")
        .to_string();
    assert!(error.contains("sales.refunds"), "{error}");
    assert!(
        error.contains("outside") || error.contains("catalog"),
        "{error}"
    );
}

#[test]
fn channel_identity_includes_groups_in_precedence_order() {
    let workspace = execution_workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders]\n",
        "channel: { target: sales.orders }\n",
        &[
            (
                "a-first",
                "group:\n  name: first\n  targets: { pipelines: [sales.orders] }\n  match: 'true'\n  priority: 10\n",
            ),
            (
                "b-second",
                "group:\n  name: second\n  targets: { pipelines: [sales.orders] }\n  match: 'true'\n  priority: 20\n",
            ),
        ],
    );
    let plan = compile_and_apply_selection(&workspace, Some("tenant.acme"), &[], true);
    let identity = plan.channel_identity().expect("identity stamped");
    assert_eq!(identity.channel.as_deref(), Some("tenant.acme"));
    assert_eq!(
        identity
            .layers
            .iter()
            .map(|layer| layer.kind)
            .collect::<Vec<_>>(),
        vec![
            ChannelLayerKind::PipelineDefault,
            ChannelLayerKind::Group,
            ChannelLayerKind::Group,
            ChannelLayerKind::ChannelWide,
            ChannelLayerKind::ChannelPerTarget,
        ]
    );
    assert_eq!(identity.layers[1].name, "first");
    assert_eq!(identity.layers[1].group_priority, Some(10));
    assert_eq!(identity.layers[1].group_sequence, Some(0));
    assert_eq!(
        identity.layers[1].group_source,
        Some(ChannelGroupSource::Derived)
    );
    assert_eq!(identity.layers[2].name, "second");
    assert_eq!(identity.layers[2].group_sequence, Some(1));
}

#[test]
fn channel_identity_changes_only_for_applied_layers() {
    let workspace = execution_workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders]\n",
        "channel: { target: sales.orders }\n",
        &[
            (
                "active",
                "group:\n  name: active\n  targets: { pipelines: [sales.orders] }\n  match: 'true'\n",
            ),
            (
                "unused",
                "group:\n  name: unused\n  targets: { pipelines: [sales.orders] }\n",
            ),
        ],
    );
    let baseline = compile_and_apply_selection(&workspace, Some("tenant.acme"), &[], true)
        .channel_identity()
        .expect("baseline identity")
        .content_hash;

    write(
        workspace.root.path(),
        "group/unused.group.yaml",
        "group:\n  name: unused\n  targets: { pipelines: [sales.orders] }\n# changed but unapplied\n",
    );
    let unapplied = compile_and_apply_selection(&workspace, Some("tenant.acme"), &[], true)
        .channel_identity()
        .expect("unapplied identity")
        .content_hash;
    assert_eq!(baseline, unapplied);

    write(
        workspace.root.path(),
        "group/active.group.yaml",
        "group:\n  name: active\n  targets: { pipelines: [sales.orders] }\n  match: 'true'\n# changed exact bytes\n",
    );
    let applied = compile_and_apply_selection(&workspace, Some("tenant.acme"), &[], true)
        .channel_identity()
        .expect("applied identity")
        .content_hash;
    assert_ne!(baseline, applied);

    write(
        workspace.root.path(),
        "channel/acme/orders.yaml",
        "channel: { target: sales.orders }\n# changed target bytes\n",
    );
    let target = compile_and_apply_selection(&workspace, Some("tenant.acme"), &[], true)
        .channel_identity()
        .expect("target identity")
        .content_hash;
    assert_ne!(applied, target);
}

#[test]
fn channel_resolution_manual_matrix() {
    let workspace = execution_workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders]\n",
        "channel: { target: sales.orders }\n",
        &[(
            "manual",
            "group:\n  name: manual\n  targets: { pipelines: [sales.orders] }\n",
        )],
    );
    let explicit = vec!["manual".to_string()];

    let group_only = compile_and_apply_selection(&workspace, None, &explicit, false);
    let group_identity = group_only.channel_identity().expect("group-only identity");
    assert_eq!(group_identity.channel, None);
    assert_eq!(
        group_identity
            .layers
            .iter()
            .map(|layer| layer.kind)
            .collect::<Vec<_>>(),
        vec![ChannelLayerKind::PipelineDefault, ChannelLayerKind::Group]
    );
    assert_eq!(
        group_identity.layers[1].group_source,
        Some(ChannelGroupSource::Explicit)
    );

    let channel_only = compile_and_apply_selection(&workspace, Some("tenant.acme"), &[], false);
    assert_eq!(
        channel_only
            .channel_identity()
            .expect("channel-only identity")
            .layers
            .iter()
            .map(|layer| layer.kind)
            .collect::<Vec<_>>(),
        vec![
            ChannelLayerKind::PipelineDefault,
            ChannelLayerKind::ChannelWide,
            ChannelLayerKind::ChannelPerTarget,
        ]
    );

    let combined = compile_and_apply_selection(&workspace, Some("tenant.acme"), &explicit, false);
    assert_eq!(
        combined
            .channel_identity()
            .expect("combined identity")
            .layers
            .len(),
        4
    );
}

#[cfg(unix)]
#[test]
fn channel_layer_io_errors_fail_closed() {
    use std::os::unix::fs::symlink;

    let workspace = execution_workspace(
        "channel:\n  name: tenant.acme\n  targets: [sales.orders]\n",
        "channel: { target: sales.orders }\n",
        &[],
    );
    let outside = tempfile::tempdir().expect("outside directory");
    let source = outside.path().join("escaped.group.yaml");
    fs::write(
        &source,
        "group:\n  name: escaped\n  targets: { pipelines: [sales.orders] }\n",
    )
    .expect("outside group");
    fs::create_dir_all(workspace.root.path().join("group")).expect("group directory");
    symlink(
        &source,
        workspace.root.path().join("group/escaped.group.yaml"),
    )
    .expect("symlink group");

    let error = clinker_channel::resolve_target_channel(
        workspace.root.path(),
        &workspace.catalog,
        &clinker_plan::config::GroupLayout {
            root: PathBuf::from("group"),
        },
        "sales.orders",
        Some("tenant.acme"),
        &[],
        true,
    )
    .expect_err("group discovery errors must not omit a layer")
    .to_string();
    assert!(error.contains("symlink"), "{error}");
    assert!(error.contains("group"), "{error}");
}
