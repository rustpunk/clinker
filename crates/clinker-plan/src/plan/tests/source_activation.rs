use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use clinker_core_types::span::FileId;

use crate::config::composition::CompositionFile;
use crate::config::{CompileContext, PipelineConfig, PipelineNode, parse_config};
use crate::plan::CompiledPlan;
use crate::plan::execution::PlanNode;

const BODY: &str = r#"_compose:
  name: catalog_reader
  inputs: {}
  outputs: { out: read }
  config_schema: {}
  resources_schema:
    orders:
      kind: file
      required: true

nodes:
  - type: source
    name: read
    config:
      name: read
      type: csv
      resource: orders
      schema: [{ name: id, type: string }]
"#;

fn write_workspace(body: &str) -> tempfile::TempDir {
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(workspace.path().join("compositions")).expect("composition dir");
    std::fs::create_dir_all(workspace.path().join("pipelines")).expect("pipeline dir");
    std::fs::create_dir_all(workspace.path().join("data")).expect("data dir");
    std::fs::write(workspace.path().join("data/orders.csv"), "id\n1\n").expect("orders data");
    std::fs::write(
        workspace
            .path()
            .join("compositions/catalog_reader.comp.yaml"),
        body,
    )
    .expect("composition body");
    std::fs::write(
        workspace.path().join("clinker.toml"),
        r#"[catalog.resources.shared_orders]
kind = "file"
path = "data/orders.csv"
access = "read"
"#,
    )
    .expect("workspace catalog");
    workspace
}

fn pipeline() -> &'static str {
    r#"pipeline: { name: source_activation }
nodes:
  - type: source
    name: driver
    config:
      name: driver
      type: csv
      path: driver.csv
      schema: [{ name: id, type: string }]
  - type: composition
    name: first
    input: driver
    use: ../compositions/catalog_reader.comp.yaml
    inputs: {}
    resources: { orders: shared_orders }
  - type: composition
    name: second
    input: driver
    use: ../compositions/catalog_reader.comp.yaml
    inputs: {}
    resources: { orders: shared_orders }
"#
}

fn compile(workspace: &Path) -> CompiledPlan {
    parse_config(pipeline())
        .expect("pipeline parses")
        .compile(&CompileContext::with_pipeline_dir(
            workspace,
            PathBuf::from("pipelines"),
        ))
        .unwrap_or_else(|diagnostics| panic!("pipeline compiles: {diagnostics:?}"))
}

fn body_source_instances(
    plan: &CompiledPlan,
) -> Vec<&crate::plan::execution::CompiledSourceInstance> {
    plan.dag()
        .graph
        .node_weights()
        .filter_map(|node| match node {
            PlanNode::Composition { body, .. } => plan.body_of(*body),
            _ => None,
        })
        .flat_map(|body| body.source_instances.iter())
        .collect()
}

#[test]
fn tracer() {
    let workspace = write_workspace(BODY);
    let first_plan = compile(workspace.path());
    let first_instances = body_source_instances(&first_plan);
    assert_eq!(first_instances.len(), 2);
    assert_ne!(first_instances[0].id, first_instances[1].id);
    assert_ne!(
        first_instances[0].id.body_scope,
        first_instances[1].id.body_scope
    );
    assert_eq!(first_instances[0].source_name, "read");

    let requirement = &first_instances[0].resource;
    assert_eq!(requirement.slot, "orders");
    assert_eq!(requirement.binding.logical_id().as_str(), "shared_orders");
    assert_eq!(requirement.kind.label(), "file");
    assert_eq!(
        requirement.required_capabilities.as_ref(),
        requirement.kind.required_capabilities()
    );
    assert_eq!(requirement.opener, requirement.kind.opener_kind());
    assert_eq!(requirement.lifetime, requirement.kind.lifetime());
    assert_eq!(
        requirement.dataset_identity.namespace,
        "clinker-resource:file"
    );
    assert_eq!(requirement.dataset_identity.name, "shared_orders");
    assert!(requirement.binding.provenance().winning_layer().is_some());
    let rendered = format!("{first_instances:?}");
    assert!(!rendered.contains("orders.csv"));
    assert!(!rendered.contains("credential"));
    assert!(!rendered.contains("token"));

    let second_plan = compile(workspace.path());
    let second_ids: Vec<_> = body_source_instances(&second_plan)
        .iter()
        .map(|instance| instance.id)
        .collect();
    let first_ids: Vec<_> = first_instances.iter().map(|instance| instance.id).collect();
    assert_eq!(first_ids, second_ids);
}

#[test]
fn resource_slot_parses_on_a_body_source() {
    let body = CompositionFile::parse(
        BODY,
        FileId::new(NonZeroU32::new(1).expect("non-zero file id")),
        PathBuf::from("compositions/catalog_reader.comp.yaml"),
    )
    .expect("composition parses");
    let PipelineNode::Source { config, .. } = &body.nodes[0].value else {
        panic!("expected body Source");
    };
    assert_eq!(
        config.resource.as_ref().expect("resource slot").value,
        "orders"
    );
}

#[test]
fn resource_backed_body_source_rejects_a_direct_matcher() {
    let body = BODY.replace(
        "      resource: orders",
        "      resource: orders\n      path: private.csv",
    );
    let workspace = write_workspace(&body);
    let diagnostics = parse_config(pipeline())
        .expect("pipeline parses")
        .compile(&CompileContext::with_pipeline_dir(
            workspace.path(),
            PathBuf::from("pipelines"),
        ))
        .expect_err("resource plus path must fail");
    let diagnostic = diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "E103")
        .unwrap_or_else(|| panic!("expected E103: {diagnostics:?}"));
    assert!(diagnostic.message.contains("resource"));
    assert!(diagnostic.message.contains("path"));
    assert!(diagnostic.primary.span.synthetic_line_number().is_some());
    assert!(
        diagnostic
            .help
            .as_deref()
            .is_some_and(|help| help.contains("remove `path`"))
    );
}

#[test]
fn body_source_resource_must_name_a_declared_slot() {
    let body = BODY.replace("resource: orders", "resource: missing");
    let workspace = write_workspace(&body);
    let diagnostics = parse_config(pipeline())
        .expect("pipeline parses")
        .compile(&CompileContext::with_pipeline_dir(
            workspace.path(),
            PathBuf::from("pipelines"),
        ))
        .expect_err("undeclared source slot must fail");
    assert!(diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "E103"
            && diagnostic.message.contains("missing")
            && diagnostic.message.contains("resources_schema")
    }));
}

#[test]
fn body_source_resource_must_be_bound_at_the_call_site() {
    let body = BODY.replace("required: true", "required: false");
    let workspace = write_workspace(&body);
    let yaml = pipeline().replace("    resources: { orders: shared_orders }\n", "");
    let diagnostics = parse_config(&yaml)
        .expect("pipeline parses")
        .compile(&CompileContext::with_pipeline_dir(
            workspace.path(),
            PathBuf::from("pipelines"),
        ))
        .expect_err("a Source cannot use an unbound optional slot");
    assert!(
        diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "E103"
                && diagnostic.message.contains("body source \"read\"")
                && diagnostic.message.contains("orders")
                && diagnostic.message.contains("does not bind")
        }),
        "{diagnostics:?}"
    );
}

#[test]
fn authored_body_source_requires_an_explicit_resource() {
    let body = BODY.replace("      resource: orders", "      path: private.csv");
    let workspace = write_workspace(&body);
    let diagnostics = parse_config(pipeline())
        .expect("pipeline parses")
        .compile(&CompileContext::with_pipeline_dir(
            workspace.path(),
            PathBuf::from("pipelines"),
        ))
        .expect_err("inert direct-path body source must fail");
    assert!(
        diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "E103"
                && diagnostic.message.contains("read")
                && diagnostic.message.contains("resource slot")
                && diagnostic
                    .help
                    .as_deref()
                    .is_some_and(|help| help.contains("resource: <slot>"))
                && diagnostic.primary.span.synthetic_line_number().is_some()
        }),
        "{diagnostics:?}"
    );
}

#[test]
fn top_level_resource_is_rejected_until_it_has_a_binding_surface() {
    let workspace = write_workspace(BODY);
    let yaml = r#"pipeline: { name: top_level_resource }
nodes:
  - type: source
    name: source
    config:
      name: source
      type: csv
      resource: orders
      schema: [{ name: id, type: string }]
"#;
    let config: PipelineConfig = parse_config(yaml).expect("resource syntax parses");
    let diagnostics = config
        .compile(&CompileContext::with_pipeline_dir(
            workspace.path(),
            PathBuf::from("pipelines"),
        ))
        .expect_err("top-level resource must fail closed");
    assert!(diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "E103"
            && diagnostic.message.contains("top-level")
            && diagnostic.message.contains("resource")
            && diagnostic.primary.span.synthetic_line_number().is_some()
    }));
}
