use std::collections::HashMap;
use std::fs;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use clinker_exec::executor::{
    PipelineExecutor, PipelineRunParams, SourceReaders, single_file_reader,
};
use clinker_plan::config::ClinkerToml;
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::plan::CompiledPlan;
use clinker_plan::resources::{
    CatalogResourceKind, LogicalResourceId, ModuleLimits, RulesRootOrigin, WorkspaceCatalog,
    collect_cxl_fields_with_compositions, collect_direct_imports, compile_module_closure,
};

fn write(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, contents).unwrap();
}

#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    fn contents(&self) -> Vec<u8> {
        self.0.lock().unwrap().clone()
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn module_pipeline(rules_path: Option<&str>) -> clinker_plan::config::PipelineConfig {
    module_pipeline_with_cxl(
        rules_path,
        "use app.main as admitted\nemit value = admitted.answer(admitted.ANSWER)",
    )
}

fn module_pipeline_with_cxl(
    rules_path: Option<&str>,
    cxl: &str,
) -> clinker_plan::config::PipelineConfig {
    let rules_path = rules_path
        .map(|path| format!("  rules_path: {path}\n"))
        .unwrap_or_default();
    parse_config(&format!(
        r#"
pipeline:
  name: retained_modules
{rules_path}nodes:
  - type: source
    name: input
    config:
      name: input
      type: csv
      path: input.csv
      schema:
        - {{ name: value, type: int }}
  - type: transform
    name: transform
    input: input
    config:
      cxl: |
{}
  - type: output
    name: output
    input: transform
    config:
      name: output
      type: csv
      path: output.csv
"#,
        cxl.lines()
            .map(|line| format!("        {line}\n"))
            .collect::<String>()
    ))
    .unwrap()
}

fn compile_module_program(
    workspace: &Path,
    modules: &[(&str, &str)],
    cxl: &str,
) -> Result<CompiledPlan, Vec<clinker_core_types::Diagnostic>> {
    for (module_id, source) in modules {
        let module_path = format!("rules/{}.cxl", module_id.replace('.', "/"));
        write(&workspace.join(module_path), source);
    }
    let catalog_config = ClinkerToml::parse("[catalog]\nrules_root = \"rules\"\n").unwrap();
    let catalog = WorkspaceCatalog::load(workspace, &catalog_config.catalog).unwrap();
    let rules_root = catalog.select_rules_root(None, None).unwrap();
    let registry = compile_module_closure(
        &catalog,
        &rules_root,
        &[LogicalResourceId::parse("app.main").unwrap()],
        ModuleLimits::default(),
    )
    .unwrap();
    let mut context = CompileContext::new(workspace);
    context.cxl_modules = registry;
    module_pipeline_with_cxl(None, cxl).compile(&context)
}

fn compile_retained_plan(
    workspace: &Path,
    catalog_rules_root: &str,
    pipeline_rules_root: Option<&str>,
    cli_rules_root: Option<&str>,
) -> CompiledPlan {
    let catalog_config = ClinkerToml::parse(&format!(
        "[catalog]\nrules_root = \"{catalog_rules_root}\"\n"
    ))
    .unwrap();
    let catalog = WorkspaceCatalog::load(workspace, &catalog_config.catalog).unwrap();
    let rules_root = catalog
        .select_rules_root(
            cli_rules_root.map(Path::new),
            pipeline_rules_root.map(Path::new),
        )
        .unwrap();
    let registry = compile_module_closure(
        &catalog,
        &rules_root,
        &[LogicalResourceId::parse("app.main").unwrap()],
        ModuleLimits::default(),
    )
    .unwrap();
    assert_eq!(registry.len(), 2, "direct and transitive modules admitted");

    let config = module_pipeline(pipeline_rules_root);
    let mut context = CompileContext::new(workspace);
    context.cxl_modules = registry;
    config.compile(&context).unwrap()
}

fn run_retained_plan(plan: &CompiledPlan) -> Vec<u8> {
    let readers: SourceReaders = HashMap::from([(
        "input".to_string(),
        single_file_reader(
            PathBuf::from("input.csv"),
            Box::new(Cursor::new(b"value\n42\n".to_vec())),
        ),
    )]);
    let output = SharedBuffer::default();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "output".to_string(),
        Box::new(output.clone()) as Box<dyn Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "retained-modules".to_string(),
        batch_id: "test".to_string(),
        ..Default::default()
    };

    PipelineExecutor::run_plan_with_readers_writers(plan, readers, writers, &params).unwrap();
    output.contents()
}

#[test]
fn imported_function_wrong_arity_fails_compile() {
    let workspace = tempfile::tempdir().unwrap();
    for (arguments, actual) in [("value", 1), ("value, value, value", 3)] {
        let diagnostics = compile_module_program(
            workspace.path(),
            &[("app.main", "fn combine(left, right) = left + right\n")],
            &format!("use app.main as admitted\nemit value = admitted.combine({arguments})"),
        )
        .expect_err("wrong imported-function arity must fail compilation");
        let diagnostic = diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == "E200")
            .expect("wrong arity produces an E200 diagnostic");
        assert!(diagnostic.message.contains("app.main.combine"));
        assert!(diagnostic.message.contains("expects 2 arguments"));
        assert!(diagnostic.message.contains(&format!("got {actual}")));
        assert!(diagnostic.message.contains("admitted.combine"));
    }
}

#[test]
fn imported_function_body_type_mismatch_fails_compile() {
    let workspace = tempfile::tempdir().unwrap();
    let diagnostics = compile_module_program(
        workspace.path(),
        &[("app.main", "fn add_one(value) = value + 1\n")],
        "use app.main as admitted\nemit value = admitted.add_one(\"not a number\")",
    )
    .expect_err("an ill-typed imported body must fail compilation");
    let diagnostic = diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "E200")
        .expect("body mismatch produces an E200 diagnostic");
    assert!(diagnostic.message.contains("app.main.add_one"));
    assert!(diagnostic.message.contains("String"));
    assert!(diagnostic.message.contains("Int"));
}

#[test]
fn nested_imported_function_typechecks() {
    let workspace = tempfile::tempdir().unwrap();
    let plan = compile_module_program(
        workspace.path(),
        &[
            (
                "app.main",
                "use shared.numbers as numbers\nfn normalize(value) = numbers.add_one(value)\n",
            ),
            ("shared.numbers", "fn add_one(value) = value + 1\n"),
        ],
        "use app.main as admitted\nemit value = admitted.normalize(value)",
    )
    .expect("nested imported calls typecheck transitively");

    assert_eq!(run_retained_plan(&plan), b"value\n43\n");
}

#[test]
fn module_function_shadows_builtin_by_resolved_binding() {
    let workspace = tempfile::tempdir().unwrap();
    let plan = compile_module_program(
        workspace.path(),
        &[("app.main", "fn length(value) = value + 1\n")],
        "use app.main as admitted\nemit value = admitted.length(value)",
    )
    .expect("resolved module function takes precedence over the length builtin");

    assert_eq!(run_retained_plan(&plan), b"value\n43\n");
}

#[test]
fn direct_use_catalog_keeps_resource_kinds_distinct() {
    let workspace = tempfile::tempdir().unwrap();
    write(
        &workspace.path().join("rules/shared/dates.cxl"),
        "let EPOCH = 1970\n",
    );
    write(
        &workspace.path().join("schemas/shared/dates.yaml"),
        "columns: []\n",
    );

    let config = ClinkerToml::parse(
        r#"
[catalog]
rules_root = "rules"

[catalog.rules]
"shared.dates" = "rules/shared/dates.cxl"

[catalog.schemas]
"shared.dates" = "schemas/shared/dates.yaml"
"#,
    )
    .unwrap();
    let catalog = WorkspaceCatalog::load(workspace.path(), &config.catalog).unwrap();
    let id = LogicalResourceId::parse("shared.dates").unwrap();

    assert_ne!(
        catalog.resolve(CatalogResourceKind::Rule, &id).unwrap(),
        catalog.resolve(CatalogResourceKind::Schema, &id).unwrap()
    );
}

#[test]
fn direct_use_rules_root_precedence_is_explicit() {
    let workspace = tempfile::tempdir().unwrap();
    for name in ["catalog-rules", "pipeline-rules", "cli-rules"] {
        fs::create_dir(workspace.path().join(name)).unwrap();
    }
    let config = ClinkerToml::parse("[catalog]\nrules_root = \"catalog-rules\"\n").unwrap();
    let catalog = WorkspaceCatalog::load(workspace.path(), &config.catalog).unwrap();

    let selected = catalog
        .select_rules_root(
            Some(Path::new("cli-rules")),
            Some(Path::new("pipeline-rules")),
        )
        .unwrap();
    assert_eq!(selected.origin(), RulesRootOrigin::Cli);
    assert!(selected.path().ends_with("cli-rules"));

    let selected = catalog
        .select_rules_root(None, Some(Path::new("pipeline-rules")))
        .unwrap();
    assert_eq!(selected.origin(), RulesRootOrigin::Pipeline);

    let selected = catalog.select_rules_root(None, None).unwrap();
    assert_eq!(selected.origin(), RulesRootOrigin::Catalog);
}

#[test]
fn direct_use_catalog_rejects_aliases_and_workspace_escape() {
    let workspace = tempfile::tempdir().unwrap();
    write(
        &workspace.path().join("rules/shared.cxl"),
        "let VALUE = 1\n",
    );
    let alias_config = ClinkerToml::parse(
        r#"
[catalog.rules]
first = "rules/shared.cxl"
second = "rules/shared.cxl"
"#,
    )
    .unwrap();
    let error = WorkspaceCatalog::load(workspace.path(), &alias_config.catalog).unwrap_err();
    assert!(error.to_string().contains("first"));
    assert!(error.to_string().contains("second"));

    let escape_config = ClinkerToml::parse(
        r#"
[catalog.rules]
outside = "../outside.cxl"
"#,
    )
    .unwrap();
    let error = WorkspaceCatalog::load(workspace.path(), &escape_config.catalog).unwrap_err();
    assert!(error.to_string().contains("outside"));
    assert!(
        !error
            .to_string()
            .contains(workspace.path().to_string_lossy().as_ref())
    );
}

#[test]
fn direct_use_compiles_only_requested_module() {
    let workspace = tempfile::tempdir().unwrap();
    write(
        &workspace.path().join("rules/shared/dates.cxl"),
        "let EPOCH = 1970\n",
    );
    write(
        &workspace.path().join("rules/private/unused.cxl"),
        "let SECRET = 1\n",
    );
    let config = ClinkerToml::parse(
        r#"
[catalog]
rules_root = "rules"

[catalog.rules]
"shared.dates" = "rules/shared/dates.cxl"
"private.unused" = "rules/private/unused.cxl"
"#,
    )
    .unwrap();
    let catalog = WorkspaceCatalog::load(workspace.path(), &config.catalog).unwrap();
    let root = catalog.select_rules_root(None, None).unwrap();
    let registry = compile_module_closure(
        &catalog,
        &root,
        &[LogicalResourceId::parse("shared.dates").unwrap()],
        ModuleLimits::default(),
    )
    .unwrap();

    assert!(registry.get("shared.dates").is_some());
    assert!(registry.get("private.unused").is_none());

    let parsed =
        cxl::parser::Parser::parse("use shared.dates as dates\nemit epoch = dates.EPOCH\n");
    assert!(parsed.errors.is_empty());
    cxl::resolve::resolve_program_with_modules(
        parsed.ast,
        &[],
        parsed.node_count,
        &registry.module_exports(),
    )
    .unwrap();
}

#[test]
fn imports_follow_transitive_closure_without_reexporting_dependencies() {
    let workspace = tempfile::tempdir().unwrap();
    write(
        &workspace.path().join("rules/app/main.cxl"),
        "use shared.dates as dates\nfn year(value) = value.year()\n",
    );
    write(
        &workspace.path().join("rules/shared/dates.cxl"),
        "use shared.numbers\nlet EPOCH = 1970\n",
    );
    write(
        &workspace.path().join("rules/shared/numbers.cxl"),
        "let ONE = 1\n",
    );
    let config = ClinkerToml::parse("[catalog]\nrules_root = \"rules\"\n").unwrap();
    let catalog = WorkspaceCatalog::load(workspace.path(), &config.catalog).unwrap();
    let root = catalog.select_rules_root(None, None).unwrap();
    let registry = compile_module_closure(
        &catalog,
        &root,
        &[LogicalResourceId::parse("app.main").unwrap()],
        ModuleLimits::default(),
    )
    .unwrap();

    assert_eq!(registry.len(), 3);
    assert!(registry.get("shared.dates").is_some());
    assert!(registry.is_program_visible("app.main"));
    assert!(!registry.is_program_visible("shared.dates"));
    assert_eq!(
        registry.get("app.main").unwrap().imports["dates"].as_str(),
        "shared.dates"
    );
}

#[test]
fn imports_enforce_unique_module_and_byte_limits() {
    let workspace = tempfile::tempdir().unwrap();
    write(
        &workspace.path().join("rules/root.cxl"),
        "use first\nuse second\nlet ROOT = 1\n",
    );
    write(&workspace.path().join("rules/first.cxl"), "let FIRST = 1\n");
    write(
        &workspace.path().join("rules/second.cxl"),
        "let SECOND = 2\n",
    );
    let config = ClinkerToml::parse("[catalog]\nrules_root = \"rules\"\n").unwrap();
    let catalog = WorkspaceCatalog::load(workspace.path(), &config.catalog).unwrap();
    let root = catalog.select_rules_root(None, None).unwrap();
    let limits = ModuleLimits {
        max_modules: 2,
        ..ModuleLimits::default()
    };
    let error = compile_module_closure(
        &catalog,
        &root,
        &[LogicalResourceId::parse("root").unwrap()],
        limits,
    )
    .unwrap_err();
    assert!(error.to_string().contains("2 unique modules"));
}

#[test]
fn dependency_graphs_report_complete_import_cycle() {
    let workspace = tempfile::tempdir().unwrap();
    write(&workspace.path().join("rules/a.cxl"), "use b\nlet A = 1\n");
    write(&workspace.path().join("rules/b.cxl"), "use c\nlet B = 2\n");
    write(&workspace.path().join("rules/c.cxl"), "use a\nlet C = 3\n");
    let config = ClinkerToml::parse("[catalog]\nrules_root = \"rules\"\n").unwrap();
    let catalog = WorkspaceCatalog::load(workspace.path(), &config.catalog).unwrap();
    let root = catalog.select_rules_root(None, None).unwrap();
    let error = compile_module_closure(
        &catalog,
        &root,
        &[LogicalResourceId::parse("a").unwrap()],
        ModuleLimits::default(),
    )
    .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("a -> b -> c -> a"), "{message}");
}

#[test]
fn dependency_graphs_reject_duplicate_import_aliases() {
    let workspace = tempfile::tempdir().unwrap();
    write(
        &workspace.path().join("rules/root.cxl"),
        "use first as common\nuse second as common\nlet ROOT = 1\n",
    );
    write(&workspace.path().join("rules/first.cxl"), "let FIRST = 1\n");
    write(
        &workspace.path().join("rules/second.cxl"),
        "let SECOND = 2\n",
    );
    let config = ClinkerToml::parse("[catalog]\nrules_root = \"rules\"\n").unwrap();
    let catalog = WorkspaceCatalog::load(workspace.path(), &config.catalog).unwrap();
    let root = catalog.select_rules_root(None, None).unwrap();
    let error = compile_module_closure(
        &catalog,
        &root,
        &[LogicalResourceId::parse("root").unwrap()],
        ModuleLimits::default(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("duplicate import alias `common`")
    );
}

#[test]
fn declaration_graph_rejects_private_transitive_alias_access() {
    let workspace = tempfile::tempdir().unwrap();
    write(
        &workspace.path().join("rules/app/main.cxl"),
        "use shared.middle as middle\nlet BAD = hidden.SECRET\n",
    );
    write(
        &workspace.path().join("rules/shared/middle.cxl"),
        "use private.hidden as hidden\nlet PUBLIC = hidden.SECRET\n",
    );
    write(
        &workspace.path().join("rules/private/hidden.cxl"),
        "let SECRET = 7\n",
    );
    let config = ClinkerToml::parse("[catalog]\nrules_root = \"rules\"\n").unwrap();
    let catalog = WorkspaceCatalog::load(workspace.path(), &config.catalog).unwrap();
    let root = catalog.select_rules_root(None, None).unwrap();
    let error = compile_module_closure(
        &catalog,
        &root,
        &[LogicalResourceId::parse("app.main").unwrap()],
        ModuleLimits::default(),
    )
    .unwrap_err();

    let message = error.to_string();
    assert!(message.contains("app.main"), "{message}");
    assert!(message.contains("hidden"), "{message}");
    assert!(message.contains("direct import"), "{message}");
}

#[test]
fn module_execution_uses_retained_registry_after_source_removal() {
    let workspace = tempfile::tempdir().unwrap();
    let origins = [
        ("catalog-rules", None, None),
        ("catalog-rules", Some("pipeline-rules"), None),
        ("catalog-rules", Some("pipeline-rules"), Some("cli-rules")),
    ];

    for (catalog_root, pipeline_root, cli_root) in origins {
        let selected_root = cli_root.or(pipeline_root).unwrap_or(catalog_root);
        write(
            &workspace.path().join(selected_root).join("app/main.cxl"),
            "use shared.base as base\nlet ANSWER = base.BASE\nfn add_one(value) = value + 1\nfn answer(value) = add_one.call(value) + 1\n",
        );
        write(
            &workspace.path().join(selected_root).join("shared/base.cxl"),
            "let BASE = 40\nfn add_two(value) = value + 2\n",
        );

        let plan = compile_retained_plan(workspace.path(), catalog_root, pipeline_root, cli_root);
        fs::remove_dir_all(workspace.path().join(selected_root)).unwrap();

        assert_eq!(run_retained_plan(&plan), b"value\n42\n");
        assert_eq!(run_retained_plan(&plan), b"value\n42\n");
    }
}

#[test]
fn source_files_removed_before_planning_fail_admission() {
    let workspace = tempfile::tempdir().unwrap();
    fs::create_dir(workspace.path().join("rules")).unwrap();
    let config = ClinkerToml::parse("[catalog]\nrules_root = \"rules\"\n").unwrap();
    let catalog = WorkspaceCatalog::load(workspace.path(), &config.catalog).unwrap();
    let rules_root = catalog.select_rules_root(None, None).unwrap();

    let error = compile_module_closure(
        &catalog,
        &rules_root,
        &[LogicalResourceId::parse("app.main").unwrap()],
        ModuleLimits::default(),
    )
    .unwrap_err();

    let message = error.to_string();
    assert!(message.contains("app.main"), "{message}");
    assert!(
        message.contains("cannot be read")
            || message.contains("cannot be opened")
            || message.contains("not found")
            || message.contains("no rule"),
        "{message}"
    );
}

#[test]
fn module_roots_cover_all_typed_cxl_fields_in_authored_order() {
    let config = parse_config(
        r#"
pipeline:
  name: typed_roots
nodes:
  - type: source
    name: input
    config:
      name: input
      type: csv
      path: input.csv
      schema:
        - { name: id, type: int }
  - type: transform
    name: transform
    input: input
    config:
      cxl: "use roots.transform"
      validations:
        - { check: "use roots.validation", message: "ordinary message" }
      log:
        - name: transform.record_seen
          level: info
          when: per_record
          every: 1
          message: "ordinary template"
  - type: aggregate
    name: aggregate
    input: transform
    config:
      group_by: [id]
      cxl: "use roots.aggregate"
  - type: route
    name: route
    input: transform
    config:
      conditions:
        first: "use roots.route_first"
        second: "use roots.route_second"
      default: other
  - type: combine
    name: combine
    input: { left: transform, right: transform }
    config:
      where: "use roots.combine_predicate"
      cxl: "use roots.combine_body"
      propagate_ck: driver
  - type: envelope
    name: envelope
    body: transform
    config:
      header:
        interchange:
          sender: "use roots.envelope_header"
      footer:
        interchange:
          count: "use roots.envelope_footer"
  - type: reshape
    name: reshape
    input: transform
    config:
      partition_by: [id]
      rules:
        - name: fill
          when: "use roots.reshape_when"
          mutate:
            set:
              id: "use roots.reshape_set"
          synthesize:
            copy_from: trigger
            overrides:
              id: "use roots.reshape_override"
  - type: cull
    name: cull
    input: reshape
    config:
      partition_by: [id]
      removed_to: removed
      rules:
        - name: discard
          drop_group_when: "use roots.cull"
"#,
    )
    .expect("typed-root fixture parses");

    let fields =
        collect_cxl_fields_with_compositions(&config.nodes, Path::new("."), Path::new("."))
            .expect("typed roots collect");
    let imports = collect_direct_imports(&fields).expect("typed imports parse");
    let actual = imports
        .iter()
        .map(LogicalResourceId::as_str)
        .collect::<Vec<_>>();

    assert_eq!(
        actual,
        vec![
            "roots.transform",
            "roots.validation",
            "roots.aggregate",
            "roots.route_first",
            "roots.route_second",
            "roots.combine_predicate",
            "roots.combine_body",
            "roots.envelope_header",
            "roots.envelope_footer",
            "roots.reshape_when",
            "roots.reshape_set",
            "roots.reshape_override",
            "roots.cull",
        ]
    );
}

#[test]
fn module_roots_ignore_non_cxl_strings() {
    let config = parse_config(
        r#"
pipeline:
  name: ignored_strings
nodes:
  - type: source
    name: input
    description: "use ignored.description"
    _notes: "use ignored.note"
    config:
      name: input
      type: csv
      path: "use ignored.path"
      options:
        delimiter: "u"
      schema:
        - { name: id, type: int }
  - type: transform
    name: transform
    description: "use ignored.transform_description"
    input: input
    config:
      cxl: "emit id = id"
      validations:
        - check: "id > 0"
          message: "use ignored.message"
      log:
        - name: transform.ignored_template
          level: info
          when: per_record
          every: 1
          message: "use ignored.template"
  - type: composition
    name: nested
    input: transform
    use: missing.comp.yaml
    config:
      option: "use ignored.option"
    resources:
      target: "use ignored.target"
"#,
    )
    .expect("non-CXL fixture parses");

    let mut fields = Vec::new();
    for node in &config.nodes {
        node.value.visit_cxl_fields(
            clinker_plan::config::CxlFieldScope::TopLevel,
            node.referenced.span(),
            &mut |field| fields.push(field),
        );
    }
    let imports = collect_direct_imports(&fields).expect("typed imports parse");

    assert!(imports.is_empty(), "ordinary strings cannot seed modules");
}

#[test]
fn module_roots_cover_nested_composition_closure_once() {
    let workspace = tempfile::tempdir().unwrap();
    write(
        &workspace.path().join("outer.comp.yaml"),
        r#"
_compose:
  name: outer
nodes:
  - type: transform
    name: outer_transform
    input: input
    config:
      cxl: "use roots.outer"
  - type: composition
    name: nested
    input: outer_transform
    use: nested.comp.yaml
"#,
    );
    write(
        &workspace.path().join("nested.comp.yaml"),
        r#"
_compose:
  name: nested
nodes:
  - type: transform
    name: nested_transform
    input: input
    config:
      cxl: "use roots.nested"
"#,
    );
    let config = parse_config(
        r#"
pipeline:
  name: nested_roots
nodes:
  - type: source
    name: input
    config:
      name: input
      type: csv
      path: input.csv
      schema:
        - { name: id, type: int }
  - type: transform
    name: top
    input: input
    config:
      cxl: "use roots.top"
  - type: composition
    name: first
    input: top
    use: outer.comp.yaml
  - type: composition
    name: second
    input: top
    use: outer.comp.yaml
"#,
    )
    .expect("nested fixture parses");

    let fields =
        collect_cxl_fields_with_compositions(&config.nodes, workspace.path(), Path::new("."))
            .expect("reachable composition closure loads");
    let imports = collect_direct_imports(&fields).expect("typed imports parse");
    let actual = imports
        .iter()
        .map(LogicalResourceId::as_str)
        .collect::<Vec<_>>();

    assert_eq!(actual, vec!["roots.top", "roots.outer", "roots.nested"]);
}

#[test]
fn module_roots_from_compositions_obey_the_selected_rules_root() {
    let workspace = tempfile::tempdir().unwrap();
    write(
        &workspace.path().join("body.comp.yaml"),
        r#"
_compose:
  name: body
nodes:
  - type: transform
    name: body_transform
    input: input
    config:
      cxl: "use selected.only"
"#,
    );
    fs::create_dir(workspace.path().join("catalog-rules")).unwrap();
    write(
        &workspace.path().join("cli-rules/selected/only.cxl"),
        "let ANSWER = 42\n",
    );
    let config = parse_config(
        r#"
pipeline:
  name: selected_root
nodes:
  - type: source
    name: input
    config:
      name: input
      type: csv
      path: input.csv
      schema:
        - { name: id, type: int }
  - type: composition
    name: body
    input: input
    use: body.comp.yaml
"#,
    )
    .expect("rules-root fixture parses");
    let fields =
        collect_cxl_fields_with_compositions(&config.nodes, workspace.path(), Path::new("."))
            .expect("composition roots collect");
    let imports = collect_direct_imports(&fields).expect("typed imports parse");
    let catalog_config = ClinkerToml::parse("[catalog]\nrules_root = \"catalog-rules\"\n").unwrap();
    let catalog = WorkspaceCatalog::load(workspace.path(), &catalog_config.catalog).unwrap();
    let rules_root = catalog
        .select_rules_root(Some(Path::new("cli-rules")), None)
        .unwrap();

    let registry = compile_module_closure(&catalog, &rules_root, &imports, ModuleLimits::default())
        .expect("body import resolves under selected root");

    assert!(registry.get("selected.only").is_some());
}

/// A log directive's `condition` can call a module the transform imports.
///
/// The gate is compiled as its own program, so an alias the transform declared
/// is not in scope for it unless the declarations are carried across. Module
/// discovery already walks conditions for exactly this reason; without the
/// declarations the alias resolved nowhere and the pipeline was rejected for a
/// module it had already compiled.
#[test]
fn a_log_condition_resolves_a_module_the_transform_imports() {
    let workspace = tempfile::tempdir().expect("workspace");
    write(
        &workspace.path().join("rules/app/main.cxl"),
        "fn is_large(value) = value > 100\n",
    );
    let catalog_config = ClinkerToml::parse("[catalog]\nrules_root = \"rules\"\n").unwrap();
    let catalog = WorkspaceCatalog::load(workspace.path(), &catalog_config.catalog).unwrap();
    let rules_root = catalog.select_rules_root(None, None).unwrap();
    let registry = compile_module_closure(
        &catalog,
        &rules_root,
        &[LogicalResourceId::parse("app.main").unwrap()],
        ModuleLimits::default(),
    )
    .unwrap();

    let config = parse_config(
        r#"
pipeline:
  name: gate_uses_module
nodes:
  - type: source
    name: input
    config:
      name: input
      type: csv
      path: input.csv
      schema:
        - { name: value, type: int }
  - type: transform
    name: transform
    input: input
    config:
      cxl: |
        use app.main as admitted
        emit value = value
      log:
        - name: transform.large_value
          level: info
          when: per_record
          every: 1
          condition: "admitted.is_large(value)"
          message: "large value"
          fields: [value]
  - type: output
    name: output
    input: transform
    config:
      name: output
      type: csv
      path: output.csv
"#,
    )
    .unwrap();
    let mut context = CompileContext::new(workspace.path());
    context.cxl_modules = registry;
    let compiled = config.compile(&context);
    assert!(
        compiled.is_ok(),
        "a gate must reach a module the transform imports: {:?}",
        compiled.err().map(|diagnostics| diagnostics
            .into_iter()
            .map(|diagnostic| format!("{}: {}", diagnostic.code, diagnostic.message))
            .collect::<Vec<_>>())
    );
}
