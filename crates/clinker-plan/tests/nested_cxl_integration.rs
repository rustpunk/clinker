//! Downstream planner coverage for nested CXL constructors and comprehensions.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use clinker_core_types::Diagnostic;
use clinker_plan::config::{CompileContext, load_config, parse_config};
use clinker_plan::plan::CompiledPlan;
use clinker_plan::plan::execution::PlanNode;
use clinker_plan::resources::{
    CatalogConfig, CompiledModuleRegistry, LogicalResourceId, ModuleLimits, WorkspaceCatalog,
    collect_cxl_fields_with_composition_identities, collect_direct_imports, compile_module_closure,
};
use cxl::ast::{Expr, MapKey};
use cxl::lexer::Span;
use cxl::module_eval::{ResolvedDeclarationId, ResolvedDeclarationKind};

const PIPELINE: &str = r#"
pipeline: { name: nested_cxl }
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: rows.csv
      schema:
        - { name: value, type: string }
  - type: transform
    name: construct
    input: rows
    config:
      cxl: |
        use root
        emit payload = root.payload(value)
  - type: output
    name: out
    input: construct
    config: { name: out, type: json, path: out.json, include_unmapped: false }
"#;

const NESTED_MODULE: &str = r#"
fn key(x) = x
fn source(x) = [x]
fn item(x) = x
fn keep(x) = x == x
fn payload(x) = {
  static: item.call(x),
  [key.call(x)]: [item.call(entry) for entry in source.call(x) if keep.call(entry)],
}
"#;

fn write_workspace(module: &str) -> (tempfile::TempDir, PathBuf) {
    let workspace = tempfile::tempdir().expect("create temporary workspace");
    let rules = workspace.path().join("rules");
    std::fs::create_dir_all(&rules).expect("create rules directory");
    std::fs::write(rules.join("root.cxl"), module).expect("write root CXL module");
    let pipeline = workspace.path().join("pipeline.yaml");
    std::fs::write(&pipeline, PIPELINE).expect("write pipeline");
    (workspace, pipeline)
}

fn module_registry(workspace: &Path) -> Result<CompiledModuleRegistry, String> {
    let catalog = WorkspaceCatalog::load(workspace, &CatalogConfig::default())
        .map_err(|error| error.to_string())?;
    let rules_root = catalog
        .select_rules_root(None, None)
        .map_err(|error| error.to_string())?;
    compile_module_closure(
        &catalog,
        &rules_root,
        &[LogicalResourceId::parse("root").expect("valid logical module id")],
        ModuleLimits::default(),
    )
    .map_err(|error| error.to_string())
}

fn compile_workspace(workspace: &Path, pipeline: &Path) -> Result<CompiledPlan, Vec<Diagnostic>> {
    let config = load_config(pipeline).expect("load pipeline");
    let pipeline_dir = pipeline
        .parent()
        .expect("pipeline parent")
        .strip_prefix(workspace)
        .expect("pipeline is inside workspace");
    let mut context = CompileContext::with_pipeline_dir(workspace, pipeline_dir);
    let discovery = collect_cxl_fields_with_composition_identities(
        &config.nodes,
        context.workspace_root(),
        &context.pipeline_dir,
    )
    .expect("collect CXL-bearing fields");
    context.composition_body_identities = discovery.identities;
    let roots = collect_direct_imports(&discovery.fields).expect("collect module roots");
    let catalog = WorkspaceCatalog::load(workspace, &CatalogConfig::default())
        .expect("load workspace catalog");
    let rules_root = catalog
        .select_rules_root(None, config.pipeline.rules_path.as_deref().map(Path::new))
        .expect("select rules root");
    context.cxl_modules =
        compile_module_closure(&catalog, &rules_root, &roots, ModuleLimits::default())
            .expect("compile module closure");
    config.compile(&context)
}

fn fingerprint(module: &str) -> [u8; 32] {
    let (workspace, pipeline) = write_workspace(module);
    compile_workspace(workspace.path(), &pipeline)
        .expect("nested module pipeline must compile")
        .semantic_fingerprint()
        .expect("nested module plan must fingerprint")
        .digest()
}

fn declaration(name: &str) -> ResolvedDeclarationId {
    ResolvedDeclarationId {
        module: "root".to_owned(),
        name: name.to_owned(),
        kind: ResolvedDeclarationKind::Function,
    }
}

#[test]
fn nested_module_expansion_and_dependency_collection_keep_every_child() {
    let (workspace, _) = write_workspace(NESTED_MODULE);
    let registry = module_registry(workspace.path()).expect("compile nested module");

    let dependencies = registry
        .declaration_graph()
        .dependencies(&declaration("payload"))
        .expect("payload declaration is retained")
        .iter()
        .map(ResolvedDeclarationId::label)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        dependencies,
        ["root.item", "root.keep", "root.key", "root.source"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        "function references in the static value, computed key, comprehension source, item, and predicate must all survive collection",
    );

    let call_span = Span::new(401, 429);
    let (_, expanded) = registry
        .runtime_modules()
        .expand_function("root", "payload", call_span)
        .expect("expand retained module function");
    let Expr::MapLiteral { entries, span, .. } = expanded else {
        panic!("payload must expand to a map literal");
    };
    assert_eq!(span, call_span, "expansion must retain the caller span");
    assert!(matches!(entries[0].key, MapKey::Static(ref key) if key.as_ref() == "static"));
    assert!(matches!(entries[1].key, MapKey::Computed(_)));
    let Expr::ArrayComprehension {
        binding,
        source,
        item,
        predicate: Some(predicate),
        span,
        ..
    } = &entries[1].value
    else {
        panic!("computed entry must retain its complete array comprehension");
    };
    assert_eq!(binding.as_ref(), "entry");
    for child in [source.as_ref(), item.as_ref(), predicate.as_ref()] {
        assert_eq!(
            child.span(),
            call_span,
            "every expanded child keeps the call span"
        );
    }
    assert_eq!(*span, call_span);
}

#[test]
fn nested_module_semantic_fingerprint_is_layout_stable_and_child_sensitive() {
    let baseline = fingerprint(NESTED_MODULE);
    let reformatted = fingerprint(
        "fn key ( x ) = x\n\nfn source(x)=[x]\nfn item(x)=x\nfn keep(x)=x==x\nfn payload(x)={ static:item.call(x), [key.call(x)]:[ item.call(entry) for entry in source.call(x) if keep.call(entry) ], }\n",
    );
    assert_eq!(baseline, reformatted, "layout and spans are not semantic");

    for (role, changed) in [
        ("static key", NESTED_MODULE.replace("static:", "renamed:")),
        (
            "computed key",
            NESTED_MODULE.replace("[key.call(x)]", "[\"changed\"]"),
        ),
        (
            "comprehension source",
            NESTED_MODULE.replace("source.call(x)", "[x, x]"),
        ),
        (
            "comprehension item",
            NESTED_MODULE.replace("item.call(entry)", "\"changed\""),
        ),
        (
            "comprehension predicate",
            NESTED_MODULE.replace("keep.call(entry)", "false"),
        ),
        (
            "map value",
            NESTED_MODULE.replace("static: item.call(x)", "static: \"changed\""),
        ),
    ] {
        assert_ne!(
            baseline,
            fingerprint(&changed),
            "changing the {role} must change semantic identity",
        );
    }
}

#[test]
fn nested_module_unresolved_references_report_the_original_child_span() {
    for expression in [
        "{[missing_key]: value}",
        "{static: missing_value}",
        "[entry for entry in missing_source]",
        "[missing_item for entry in [value]]",
        "[entry for entry in [value] if missing_predicate]",
    ] {
        let module = format!("fn payload(value) = {expression}\n");
        let expected_start = module
            .find("missing_")
            .expect("fixture contains unresolved child");
        let (workspace, _) = write_workspace(&module);
        let error = module_registry(workspace.path()).expect_err("unresolved child must fail");
        assert!(
            error.contains(&format!("authored span {expected_start}..")),
            "failure must name the original child span for {expression:?}: {error}",
        );
    }
}

fn compile_inline(yaml: &str) -> Result<CompiledPlan, Vec<Diagnostic>> {
    parse_config(yaml)
        .expect("fixture must pass YAML admission")
        .compile(&CompileContext::default())
}

#[test]
fn nested_scope_reads_reach_post_merge_validation() {
    let yaml = r#"
pipeline: { name: nested_scope_read }
nodes:
  - type: source
    name: left
    config:
      name: left
      type: csv
      path: left.csv
      schema: [{ name: value, type: string }]
  - type: source
    name: right
    config:
      name: right
      type: csv
      path: right.csv
      schema: [{ name: value, type: string }]
  - type: transform
    name: declare_batch
    input: left
    config:
      cxl: emit $source.batch_tag = value
      declares: [{ name: batch_tag, scope: source, type: string, default: "" }]
  - type: merge
    name: merged
    inputs: [declare_batch, right]
  - type: transform
    name: read_batch
    input: merged
    config:
      cxl: 'emit payload = {static: [entry for entry in [$source.batch_tag] if true]}'
  - type: output
    name: out
    input: read_batch
    config: { name: out, type: json, path: out.json }
"#;
    let diagnostics = compile_inline(yaml).expect_err("post-merge source read must fail");
    let diagnostic = diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "E172")
        .unwrap_or_else(|| panic!("expected E172 from nested scope read: {diagnostics:?}"));
    assert!(diagnostic.message.contains("$source.batch_tag"));
    assert_ne!(diagnostic.primary.span, clinker_core_types::Span::SYNTHETIC);
}

#[test]
fn nested_body_fields_reach_envelope_header_validation_but_static_keys_do_not() {
    let pipeline = |header: &str| {
        format!(
            r#"
pipeline:
  name: nested_header
  vars:
    label: {{ type: string, default: "ok" }}
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: rows.csv
      schema:
        - {{ name: amount, type: int }}
  - type: envelope
    name: framed
    body: rows
    config:
      strategy: concat
      header:
        metadata:
          payload: '{header}'
  - type: output
    name: out
    input: framed
    config: {{ name: out, type: json, path: out.json }}
"#,
        )
    };

    compile_inline(&pipeline("{static: [$vars.label]}"))
        .expect("a static key and document-open value must compile");
    let diagnostics = compile_inline(&pipeline(
        "{static: [entry for entry in [amount] if entry > 0]}",
    ))
    .expect_err("nested body field must fail header validation");
    let diagnostic = diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "E353")
        .unwrap_or_else(|| panic!("expected E353 from nested body read: {diagnostics:?}"));
    assert!(diagnostic.message.contains("amount"));
    assert_ne!(diagnostic.primary.span, clinker_core_types::Span::SYNTHETIC);
}

fn combine_pipeline(where_expr: &str, body_expr: &str) -> String {
    format!(
        r#"
pipeline: {{ name: nested_combine }}
nodes:
  - type: source
    name: left_src
    config:
      name: left_src
      type: csv
      path: left.csv
      schema: [{{ name: id, type: string }}]
  - type: source
    name: right_src
    config:
      name: right_src
      type: csv
      path: right.csv
      schema: [{{ name: id, type: string }}]
  - type: combine
    name: joined
    input: {{ left: left_src, right: right_src }}
    config:
      where: '{where_expr}'
      cxl: 'emit payload = {body_expr}'
      propagate_ck: driver
  - type: output
    name: out
    input: joined
    config: {{ name: out, type: json, path: out.json }}
"#,
    )
}

#[test]
fn nested_qualifiers_reach_combine_classification_and_column_resolution() {
    let plan = compile_inline(&combine_pipeline(
        "[left.id] == [right.id]",
        "{[left.id]: [right.id]}",
    ))
    .expect("nested qualified references must compile");
    let combine = plan
        .dag()
        .graph
        .node_weights()
        .find(|node| node.name() == "joined")
        .expect("combine node");
    let PlanNode::Combine {
        decomposed_predicate: Some(predicate),
        resolved_column_map,
        ..
    } = combine
    else {
        panic!("joined node must retain its decomposed predicate");
    };
    assert_eq!(predicate.equalities.len(), 1);
    let resolved = resolved_column_map
        .keys()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        resolved,
        ["left.id", "right.id"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        "nested body and predicate qualifiers must reach the retained column map",
    );
}

#[test]
fn nested_unknown_qualified_reference_reports_the_combine_source_span() {
    let diagnostics = compile_inline(&combine_pipeline(
        "left.id == right.id",
        "{static: [right.missing]}",
    ))
    .expect_err("unknown nested qualified field must fail");
    let diagnostic = diagnostics
        .iter()
        .find(|diagnostic| diagnostic.message.contains("right.missing"))
        .unwrap_or_else(|| panic!("expected nested unknown-field diagnostic: {diagnostics:?}"));
    assert_ne!(diagnostic.primary.span, clinker_core_types::Span::SYNTHETIC);
}
