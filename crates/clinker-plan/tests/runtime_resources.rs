use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use clinker_core_types::span::FileId;
use clinker_plan::config::composition::CompositionFile;
use clinker_plan::config::{ClinkerToml, CompileContext, PipelineNode, parse_config};
use clinker_plan::plan::execution::PlanNode;
use clinker_plan::plan::explain_provenance::explain_resource_binding;
use clinker_plan::resources::{
    CatalogConfig, CatalogLimits, CatalogResourceConfig, FileResourceAccess, LogicalResourceId,
    WorkspaceCatalog,
};

const BODY: &str = r#"_compose:
  name: typed_reader
  inputs:
    input:
      schema: [{ name: id, type: string }]
  outputs:
    out: shape
  config_schema: {}
  resources_schema:
    orders:
      kind: file
      required: true

nodes:
  - type: transform
    name: shape
    input: input
    config:
      cxl: |
        emit id = id
"#;

fn write_workspace() -> tempfile::TempDir {
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(workspace.path().join("compositions")).expect("composition dir");
    std::fs::create_dir_all(workspace.path().join("pipelines")).expect("pipeline dir");
    std::fs::create_dir_all(workspace.path().join("data")).expect("data dir");
    std::fs::write(workspace.path().join("data/orders.csv"), "id\n1\n").expect("orders data");
    std::fs::write(workspace.path().join("data/other.csv"), "id\n2\n").expect("other data");
    std::fs::write(workspace.path().join("data/write.csv"), "id\n3\n").expect("write data");
    std::fs::write(workspace.path().join("compositions/typed.comp.yaml"), BODY)
        .expect("composition body");
    std::fs::write(
        workspace.path().join("clinker.toml"),
        r#"[catalog.resources.shared_orders]
kind = "file"
path = "data/orders.csv"
access = "read"

[catalog.resources.other_orders]
kind = "file"
path = "data/other.csv"
access = "read"

[catalog.resources.write_only]
kind = "file"
path = "data/write.csv"
access = "write"
"#,
    )
    .expect("workspace catalog");
    workspace
}

fn pipeline(binding: &str) -> String {
    format!(
        r#"pipeline: {{ name: typed_resource }}
nodes:
  - type: source
    name: source
    config:
      name: source
      type: csv
      path: input.csv
      schema: [{{ name: id, type: string }}]
  - type: composition
    name: typed
    input: source
    use: ../compositions/typed.comp.yaml
    inputs: {{ input: source }}
    resources: {{ orders: {binding} }}
  - type: output
    name: output
    input: typed.out
    config: {{ name: output, type: csv, path: output.csv }}
"#
    )
}

fn compile(workspace: &Path, binding: &str) -> clinker_plan::plan::CompiledPlan {
    parse_config(&pipeline(binding))
        .expect("pipeline parses")
        .compile(&CompileContext::with_pipeline_dir(
            workspace,
            PathBuf::from("pipelines"),
        ))
        .expect("typed resource compiles")
}

#[test]
fn tracer_catalog_resource_binds_one_declared_slot() {
    let workspace = write_workspace();
    let plan = compile(workspace.path(), "shared_orders");
    let PipelineNode::Composition { resources, .. } = &plan.config().nodes[1].value else {
        panic!("expected composition call");
    };
    let binding = &resources["orders"];
    assert_eq!(binding.logical_id().as_str(), "shared_orders");
    assert_eq!(binding.provenance().provenance.len(), 1);
    assert!(binding.provenance().winning_layer().is_some());
    let rendered = format!("{binding:?}");
    assert!(!rendered.contains("orders.csv"));
    assert!(!rendered.contains("token"));
}

#[test]
fn tracer_catalog_descriptor_is_strict_and_secret_free() {
    for invalid in [
        r#"[catalog.resources.orders]
kind = "file"
path = "data/orders.csv"
credential_profile = "prod"
"#,
        r#"[catalog.resources.orders]
kind = "file"
token = "literal-secret"
"#,
        r#"[catalog.resources.orders]
kind = "socket"
path = "data/orders.csv"
"#,
    ] {
        let error = ClinkerToml::parse(invalid)
            .expect_err("unknown, incomplete, and credential-bearing descriptors fail")
            .to_string();
        assert!(
            error.contains("unknown field")
                || error.contains("missing field")
                || error.contains("unknown variant"),
            "{error}"
        );
    }
}

#[test]
fn recursive_missing_extra_and_capability_mismatches_fail_before_execution() {
    let workspace = write_workspace();
    for (binding, expected) in [
        ("missing", "unknown runtime resource"),
        ("write_only", "requires capability Read"),
    ] {
        let diagnostics = parse_config(&pipeline(binding))
            .expect("pipeline parses")
            .compile(&CompileContext::with_pipeline_dir(
                workspace.path(),
                PathBuf::from("pipelines"),
            ))
            .expect_err("invalid binding must fail planning");
        assert!(
            diagnostics.iter().any(|diagnostic| {
                diagnostic.code == "E103" && diagnostic.message.contains(expected)
            }),
            "{diagnostics:?}"
        );
    }

    let extra = pipeline("shared_orders").replace(
        "resources: { orders: shared_orders }",
        "resources: { orders: shared_orders, internal: other_orders }",
    );
    let diagnostics = parse_config(&extra)
        .expect("pipeline parses")
        .compile(&CompileContext::with_pipeline_dir(
            workspace.path(),
            PathBuf::from("pipelines"),
        ))
        .expect_err("sealed internal slot must fail");
    assert!(diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "E103"
            && diagnostic.message.contains("internal")
            && diagnostic.message.contains("not declared")
    }));
}

#[test]
fn recursive_binding_identity_changes_semantic_fingerprint() {
    let workspace = write_workspace();
    let first = compile(workspace.path(), "shared_orders")
        .semantic_fingerprint()
        .expect("first fingerprint");
    let second = compile(workspace.path(), "other_orders")
        .semantic_fingerprint()
        .expect("second fingerprint");
    assert_ne!(first, second);
}

#[test]
fn recursive_bound_body_retains_secret_free_resource_provenance_for_explain() {
    let workspace = write_workspace();
    let plan = compile(workspace.path(), "shared_orders");
    let body_id = plan
        .dag()
        .graph
        .node_weights()
        .find_map(|node| match node {
            PlanNode::Composition { body, .. } => Some(*body),
            _ => None,
        })
        .expect("composition body id");
    let body = plan.body_of(body_id).expect("bound composition body");
    let binding = &body.resource_bindings["orders"];
    let explained = explain_resource_binding("typed", "orders", binding);
    assert!(explained.contains("Resource: typed.orders"));
    assert!(explained.contains("[WON]"));
    assert!(explained.contains("PipelineDefault"));
    assert!(explained.contains("shared_orders"));
    assert!(!explained.contains("orders.csv"));
    assert!(!explained.contains("credential"));
}

#[test]
fn call_surface_alias_is_rejected_with_e377_and_authored_line() {
    let yaml = pipeline("shared_orders").replace(
        "    inputs: { input: source }",
        "    alias: retired_namespace\n    inputs: { input: source }",
    );
    let error = parse_config(&yaml)
        .expect_err("ordinary alias is rejected")
        .to_string();
    assert!(error.contains("E377"), "{error}");
    assert!(error.contains("alias"), "{error}");
    assert!(error.contains("line 14"), "{error}");
    assert!(error.contains("column"), "{error}");
    assert!(error.contains("name: <namespace>"), "{error}");
}

#[test]
fn call_surface_nested_alias_is_rejected_with_e377_at_authored_location() {
    let yaml = r#"_compose:
  name: outer
  inputs:
    input: { schema: [{ name: id, type: string }] }
  outputs: { out: nested.out }
  config_schema: {}
  resources_schema: {}
nodes:
  - type: composition
    name: nested
    input: input
    use: typed.comp.yaml
    alias: ignored
    inputs: { input: input }
"#;
    let error = CompositionFile::parse(
        yaml,
        FileId::new(NonZeroU32::new(1).expect("non-zero file id")),
        PathBuf::from("outer.comp.yaml"),
    )
    .expect_err("nested ordinary alias is rejected")
    .to_string();
    assert!(error.contains("E377"), "{error}");
    assert!(error.contains("alias"), "{error}");
    assert!(error.contains("line 13"), "{error}");
    assert!(error.contains("column"), "{error}");
}

#[test]
fn call_surface_nested_outputs_is_rejected_with_e377_at_authored_location() {
    let yaml = r#"_compose:
  name: outer
  inputs:
    input: { schema: [{ name: id, type: string }] }
  outputs: { out: nested.out }
  config_schema: {}
  resources_schema: {}
nodes:
  - type: composition
    name: nested
    input: input
    use: typed.comp.yaml
    inputs: { input: input }
    outputs: { out: ignored }
"#;
    let error = CompositionFile::parse(
        yaml,
        FileId::new(NonZeroU32::new(1).expect("non-zero file id")),
        PathBuf::from("outer.comp.yaml"),
    )
    .expect_err("nested ordinary outputs are rejected")
    .to_string();
    assert!(error.contains("E377"), "{error}");
    assert!(error.contains("outputs"), "{error}");
    assert!(error.contains("line 14"), "{error}");
    assert!(error.contains("column"), "{error}");
    assert!(error.contains("_compose.outputs"), "{error}");
}

#[test]
fn call_surface_outputs_is_rejected_with_e377_and_paste_ready_correction() {
    let yaml = pipeline("shared_orders").replace(
        "    inputs: { input: source }",
        "    inputs: { input: source }\n    outputs: { out: ignored }",
    );
    let error = parse_config(&yaml)
        .expect_err("ordinary outputs is rejected")
        .to_string();
    assert!(error.contains("E377"), "{error}");
    assert!(error.contains("outputs"), "{error}");
    assert!(error.contains("_compose.outputs"), "{error}");
    assert!(error.contains("<composition-node-name>.<port>"), "{error}");
}

#[test]
fn call_surface_declared_outputs_remain_accepted() {
    let workspace = write_workspace();
    compile(workspace.path(), "shared_orders");
}

#[test]
fn identity_catalog_entry_cap_fails_before_partial_insertion() {
    let workspace = write_workspace();
    let config = CatalogConfig {
        resources: BTreeMap::from([
            (
                "one".to_string(),
                CatalogResourceConfig::File {
                    path: PathBuf::from("data/orders.csv"),
                    access: FileResourceAccess::Read,
                },
            ),
            (
                "two".to_string(),
                CatalogResourceConfig::File {
                    path: PathBuf::from("data/other.csv"),
                    access: FileResourceAccess::Read,
                },
            ),
        ]),
        ..CatalogConfig::default()
    };
    let error = WorkspaceCatalog::load_with_limits(
        workspace.path(),
        &config,
        CatalogLimits {
            max_entries: 1,
            max_descriptor_bytes: usize::MAX,
        },
    )
    .expect_err("cap plus one fails atomically")
    .to_string();
    assert!(error.contains("2 entries"), "{error}");
    assert!(error.contains("fixed limit of 1"), "{error}");
}

#[test]
fn identity_catalog_descriptor_byte_cap_is_checked() {
    let workspace = write_workspace();
    let config = CatalogConfig {
        resources: BTreeMap::from([(
            "one".to_string(),
            CatalogResourceConfig::File {
                path: PathBuf::from("data/orders.csv"),
                access: FileResourceAccess::Read,
            },
        )]),
        ..CatalogConfig::default()
    };
    let error = WorkspaceCatalog::load_with_limits(
        workspace.path(),
        &config,
        CatalogLimits {
            max_entries: usize::MAX,
            max_descriptor_bytes: 1,
        },
    )
    .expect_err("descriptor byte cap fails")
    .to_string();
    assert!(error.contains("descriptor"), "{error}");
    assert!(error.contains("fixed limit of 1"), "{error}");
}

#[test]
fn identity_catalog_resource_path_collision_is_rejected() {
    let workspace = write_workspace();
    let config = CatalogConfig {
        resources: BTreeMap::from([
            (
                "first".to_string(),
                CatalogResourceConfig::File {
                    path: PathBuf::from("data/orders.csv"),
                    access: FileResourceAccess::Read,
                },
            ),
            (
                "second".to_string(),
                CatalogResourceConfig::File {
                    path: PathBuf::from("data/orders.csv"),
                    access: FileResourceAccess::Read,
                },
            ),
        ]),
        ..CatalogConfig::default()
    };
    let error = WorkspaceCatalog::load(workspace.path(), &config)
        .expect_err("one physical resource cannot acquire two identities")
        .to_string();
    assert!(error.contains("first"), "{error}");
    assert!(error.contains("second"), "{error}");
    assert!(error.contains("same canonical target"), "{error}");
}

#[test]
fn identity_resource_dataset_inputs_are_stable_and_secret_free() {
    let workspace = write_workspace();
    let toml = ClinkerToml::load_from_workspace(workspace.path()).expect("workspace config");
    let catalog = WorkspaceCatalog::load(workspace.path(), &toml.catalog).expect("catalog");
    let id = LogicalResourceId::parse("shared_orders").expect("logical id");
    let identity = catalog
        .resolve_resource(&id)
        .expect("resource")
        .dataset_identity();
    assert_eq!(identity.namespace, "clinker-resource:file");
    assert_eq!(identity.name, "shared_orders");
    let rendered = format!("{identity:?}");
    assert!(!rendered.contains("orders.csv"));
    assert!(!rendered.contains("credential"));
}

#[cfg(unix)]
#[test]
fn identity_catalog_retains_the_validated_target_after_symlink_retarget() {
    use std::os::unix::fs::symlink;

    let workspace = write_workspace();
    let outside = tempfile::tempdir().expect("outside directory");
    let alias = workspace.path().join("data/current.csv");
    symlink(workspace.path().join("data/orders.csv"), &alias).expect("inside alias");
    let config = CatalogConfig {
        resources: BTreeMap::from([(
            "current".to_string(),
            CatalogResourceConfig::File {
                path: PathBuf::from("data/current.csv"),
                access: FileResourceAccess::Read,
            },
        )]),
        ..CatalogConfig::default()
    };
    let catalog = WorkspaceCatalog::load(workspace.path(), &config).expect("catalog admits alias");
    let id = LogicalResourceId::parse("current").expect("logical id");
    let admitted = catalog
        .resolve_resource(&id)
        .expect("resource")
        .canonical_target()
        .to_path_buf();

    std::fs::remove_file(&alias).expect("remove admitted alias");
    symlink(outside.path().join("outside.csv"), &alias).expect("retarget alias");

    assert_eq!(
        admitted,
        workspace
            .path()
            .join("data/orders.csv")
            .canonicalize()
            .expect("canonical admitted target")
    );
    assert!(!admitted.starts_with(outside.path()));
}
