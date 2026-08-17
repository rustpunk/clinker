use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use clinker_core_types::span::FileId;

use crate::config::composition::CompositionFile;
use crate::config::{CompileContext, PipelineConfig, PipelineNode, parse_config};
use crate::plan::CompiledPlan;
use crate::plan::execution::{
    CompiledSourceInstance, CompiledSourceRoot, PlanNode, SourceActivationCapacity,
    SourceActivationGroupKind,
};

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

fn body_source_instances(plan: &CompiledPlan) -> Vec<&CompiledSourceInstance> {
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
    assert_ne!(first_instances[0].id(), first_instances[1].id());
    assert_ne!(first_instances[0].id().scope, first_instances[1].id().scope);
    assert_eq!(first_instances[0].source_name(), "read");

    let requirement = first_instances[0]
        .resource()
        .expect("body Source has a catalog requirement");
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
        .map(|instance| instance.id())
        .collect();
    let first_ids: Vec<_> = first_instances
        .iter()
        .map(|instance| instance.id())
        .collect();
    assert_eq!(first_ids, second_ids);

    let activation = first_plan.dag().source_activation();
    assert!(activation.is_sealed());
    assert_eq!(activation.instances().len(), 3);
    assert_eq!(activation.groups().len(), 3);
    assert_eq!(activation.credential_requirement_ids().len(), 0);
    let second_activation = second_plan.dag().source_activation();
    assert_eq!(activation, second_activation);
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

fn write_composition(workspace: &Path, name: &str, body: &str) {
    std::fs::write(
        workspace
            .join("compositions")
            .join(format!("{name}.comp.yaml")),
        body,
    )
    .expect("composition body");
}

fn compile_pipeline(workspace: &Path, yaml: &str) -> CompiledPlan {
    parse_config(yaml)
        .expect("pipeline parses")
        .compile(&CompileContext::with_pipeline_dir(
            workspace,
            PathBuf::from("pipelines"),
        ))
        .unwrap_or_else(|diagnostics| panic!("pipeline compiles: {diagnostics:?}"))
}

fn one_call_pipeline(body: &str, call_fields: &str) -> String {
    format!(
        r#"pipeline: {{ name: activation_groups }}
nodes:
  - type: source
    name: driver
    config:
      name: driver
      type: csv
      path: driver.csv
      schema: [{{ name: id, type: string }}]
  - type: composition
    name: call
    input: driver
    use: ../compositions/{body}.comp.yaml
{call_fields}"#
    )
}

fn instance_named<'a>(plan: &'a CompiledPlan, name: &str) -> &'a CompiledSourceInstance {
    plan.dag()
        .source_activation()
        .instances()
        .iter()
        .find(|instance| instance.source_name() == name)
        .unwrap_or_else(|| panic!("missing activation instance {name:?}"))
}

fn group_for_instance<'a>(
    plan: &'a CompiledPlan,
    instance: &CompiledSourceInstance,
) -> &'a crate::plan::execution::SourceActivationGroup {
    plan.dag()
        .source_activation()
        .groups()
        .iter()
        .find(|group| group.members().contains(&instance.id()))
        .unwrap_or_else(|| panic!("missing group for {:?}", instance.id()))
}

#[test]
fn input_ports_are_explicit_roots_but_not_activation_instances() {
    let workspace = write_workspace(BODY);
    write_composition(
        workspace.path(),
        "input_only",
        r#"_compose:
  name: input_only
  inputs:
    incoming: { schema: [{ name: id, type: string }] }
  outputs: { out: shape }
  config_schema: {}
  resources_schema: {}
nodes:
  - type: transform
    name: shape
    input: incoming
    config:
      cxl: |
        emit id = id
"#,
    );
    let yaml = one_call_pipeline("input_only", "    inputs: { incoming: driver }\n");
    let plan = compile_pipeline(workspace.path(), &yaml);
    let activation = plan.dag().source_activation();

    assert!(activation.is_sealed());
    assert_eq!(activation.instances().len(), 1);
    assert_eq!(activation.groups().len(), 1);
    assert!(activation.roots().iter().any(|root| {
        matches!(root, CompiledSourceRoot::InputPort { port_name, .. } if port_name.as_ref() == "incoming")
    }));
    assert_eq!(instance_named(&plan, "driver").resource(), None);
}

#[test]
fn nested_body_sources_form_dependency_ordered_singleton_groups() {
    let workspace = write_workspace(BODY);
    write_composition(
        workspace.path(),
        "inner",
        r#"_compose:
  name: inner
  inputs:
    incoming: { schema: [{ name: id, type: string }] }
  outputs: { out: inner_ref }
  config_schema: {}
  resources_schema:
    inner_data: { kind: file, required: true }
nodes:
  - type: source
    name: inner_ref
    config:
      name: inner_ref
      type: csv
      resource: inner_data
      schema: [{ name: id, type: string }]
"#,
    );
    write_composition(
        workspace.path(),
        "outer",
        r#"_compose:
  name: outer
  inputs: {}
  outputs: { out: inner_call }
  config_schema: {}
  resources_schema:
    outer_data: { kind: file, required: true }
    inner_data: { kind: file, required: true }
nodes:
  - type: source
    name: outer_ref
    config:
      name: outer_ref
      type: csv
      resource: outer_data
      schema: [{ name: id, type: string }]
  - type: composition
    name: inner_call
    input: outer_ref
    use: inner.comp.yaml
    inputs: { incoming: outer_ref }
    resources: { inner_data: shared_orders }
"#,
    );
    let yaml = one_call_pipeline(
        "outer",
        "    inputs: {}\n    resources: { outer_data: shared_orders, inner_data: shared_orders }\n",
    );
    let first = compile_pipeline(workspace.path(), &yaml);
    let outer = instance_named(&first, "outer_ref");
    let inner = instance_named(&first, "inner_ref");
    let outer_group = group_for_instance(&first, outer);
    let inner_group = group_for_instance(&first, inner);

    assert_eq!(outer_group.kind(), &SourceActivationGroupKind::Ordinary);
    assert_eq!(inner_group.kind(), &SourceActivationGroupKind::Ordinary);
    assert!(inner_group.dependencies().contains(&outer_group.id()));
    assert!(outer_group.id() < inner_group.id());

    let second = compile_pipeline(workspace.path(), &yaml);
    assert_eq!(
        first.dag().source_activation(),
        second.dag().source_activation()
    );
}

#[test]
fn exclusive_live_interleave_sources_share_one_atomic_group() {
    let workspace = write_workspace(BODY);
    write_composition(
        workspace.path(),
        "interleave",
        r#"_compose:
  name: interleave
  inputs: {}
  outputs: { out: mixed }
  config_schema: {}
  resources_schema:
    left_data: { kind: file, required: true }
    right_data: { kind: file, required: true }
nodes:
  - type: source
    name: left
    config:
      name: left
      type: csv
      resource: left_data
      schema: [{ name: id, type: string }]
  - type: source
    name: right
    config:
      name: right
      type: csv
      resource: right_data
      schema: [{ name: id, type: string }]
  - type: merge
    name: mixed
    inputs: [left, right]
    config:
      mode: interleave
"#,
    );
    let yaml = one_call_pipeline(
        "interleave",
        "    inputs: {}\n    resources: { left_data: shared_orders, right_data: shared_orders }\n",
    );
    let plan = compile_pipeline(workspace.path(), &yaml);
    let left = instance_named(&plan, "left");
    let right = instance_named(&plan, "right");
    let group = group_for_instance(&plan, left);

    assert!(group.members().contains(&right.id()));
    assert!(matches!(
        group.kind(),
        SourceActivationGroupKind::LiveInterleave { consumer_path }
            if consumer_path.len() == 1
    ));
    assert_eq!(group.capacity().resource_units(), 2);
    assert_eq!(group.capacity().opener_units(), 2);
    assert_eq!(group.capacity().credential_handle_units(), 0);
    assert!(group.credential_requirement_ids().is_empty());
    assert_eq!(
        left.resource().unwrap().binding.logical_id(),
        right.resource().unwrap().binding.logical_id()
    );
}

#[test]
fn exclusive_source_transform_path_is_compiled_as_fused() {
    let workspace = write_workspace(BODY);
    write_composition(
        workspace.path(),
        "fused",
        r#"_compose:
  name: fused
  inputs: {}
  outputs: { out: shaped }
  config_schema: {}
  resources_schema:
    data: { kind: file, required: true }
nodes:
  - type: source
    name: raw
    config:
      name: raw
      type: csv
      resource: data
      schema: [{ name: id, type: string }]
  - type: transform
    name: shaped
    input: raw
    config:
      cxl: |
        emit id = id
"#,
    );
    let yaml = one_call_pipeline(
        "fused",
        "    inputs: {}\n    resources: { data: shared_orders }\n",
    );
    let plan = compile_pipeline(workspace.path(), &yaml);
    let raw = instance_named(&plan, "raw");
    let group = group_for_instance(&plan, raw);

    assert!(matches!(
        group.kind(),
        SourceActivationGroupKind::FusedStreaming { consumer_path }
            if !consumer_path.is_empty()
    ));
}

#[test]
fn activation_capacity_overflow_is_rejected() {
    let maximum = SourceActivationCapacity::new(u32::MAX, u32::MAX, u32::MAX);
    let one = SourceActivationCapacity::new(1, 1, 1);
    assert_eq!(maximum.checked_add(one), None);
}

#[test]
fn topology_cycles_and_undeclared_dependencies_keep_authored_spans() {
    let workspace = write_workspace(BODY);
    let cycle = r#"pipeline: { name: cycle }
nodes:
  - type: source
    name: source
    config:
      name: source
      type: csv
      path: source.csv
      schema: [{ name: id, type: string }]
  - type: transform
    name: first
    input: second
    config: { cxl: "emit id = id" }
  - type: transform
    name: second
    input: first
    config: { cxl: "emit id = id" }
"#;
    let cycle_diags = parse_config(cycle)
        .expect("cycle parses")
        .compile(&CompileContext::with_pipeline_dir(
            workspace.path(),
            PathBuf::from("pipelines"),
        ))
        .expect_err("cycle fails");
    let cycle_diag = cycle_diags
        .iter()
        .find(|diagnostic| diagnostic.code == "E003")
        .expect("cycle diagnostic");
    assert!(cycle_diag.primary.span.synthetic_line_number().is_some());

    let undeclared = cycle.replace("input: second", "input: missing");
    let undeclared_diags = parse_config(&undeclared)
        .expect("undeclared input parses")
        .compile(&CompileContext::with_pipeline_dir(
            workspace.path(),
            PathBuf::from("pipelines"),
        ))
        .expect_err("undeclared dependency fails");
    let undeclared_diag = undeclared_diags
        .iter()
        .find(|diagnostic| diagnostic.code == "E004")
        .expect("undeclared-input diagnostic");
    assert!(
        undeclared_diag
            .primary
            .span
            .synthetic_line_number()
            .is_some()
    );
}
