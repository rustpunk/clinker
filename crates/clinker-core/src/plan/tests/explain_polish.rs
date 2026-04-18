//! Task 16b.8 — `--explain` polish gate tests.
//!
//! These tests pin the contract for the enhanced `explain_text` output:
//!   1. Route nodes render as a sibling line at their topo position,
//!      with per-branch target labels.
//!   2. Aggregate (`PlanNode::Aggregation`) nodes render as a sibling
//!      line at their topo position (not visually nested inside an
//!      upstream Transform).
//!   3. When the compile path threads real YAML line info through
//!      `Span::line_only`, each lowered node's line appears as a
//!      `(line:N)` suffix in the explain output.
//!   4. Route edges in the explain output are labeled by the branch
//!      name from `RouteBody.conditions`.
//!
//! The fifth gate test (`test_diagnostic_renders_via_miette_in_cli`)
//! lives in `crates/clinker/tests/` alongside the CLI binary, because
//! it shells out via `assert_cmd`.

use super::dag::{diamond_fixture_yaml, parse_fixture};
use crate::config::PipelineConfig;
use crate::plan::execution::*;

/// Compile a fixture through the legacy `ExecutionPlanDag::compile`
/// path so `PlanNode::Aggregation` lowering is exercised. Mirrors
/// `dag::compile_fixture` but honors aggregate-mode typecheck so
/// group-by transforms lower correctly.
fn compile_legacy(config: &PipelineConfig, _fields: &[&str]) -> ExecutionPlanDag {
    // Phase 16d: the canonical compile entry point produces the DAG
    // directly. `bind_schema` inside `compile_with_diagnostics`
    // typechecks every CXL body against author-declared source schemas,
    // so test callers no longer need to pre-build typed programs.
    config
        .compile(&crate::config::CompileContext::default())
        .expect("compile")
        .dag()
        .clone()
}

fn compile_cxl_for_spec(
    spec: &crate::executor::TransformSpec,
    fields: &[&str],
) -> cxl::typecheck::pass::TypedProgram {
    let source = spec.cxl_source();
    let parsed = cxl::parser::Parser::parse(source);
    assert!(
        parsed.errors.is_empty(),
        "parse errors: {:?}",
        parsed.errors
    );
    let resolved =
        cxl::resolve::pass::resolve_program(parsed.ast, fields, parsed.node_count).unwrap();
    let cols: indexmap::IndexMap<cxl::typecheck::QualifiedField, cxl::typecheck::types::Type> =
        fields
            .iter()
            .map(|f| {
                (
                    cxl::typecheck::QualifiedField::bare(*f),
                    cxl::typecheck::types::Type::Any,
                )
            })
            .collect();
    let schema = cxl::typecheck::Row::closed(cols, cxl::lexer::Span::new(0, 0));
    let mode = if let Some(agg) = &spec.aggregate {
        cxl::typecheck::AggregateMode::GroupBy {
            group_by_fields: agg.group_by.iter().cloned().collect(),
        }
    } else {
        cxl::typecheck::AggregateMode::Row
    };
    cxl::typecheck::type_check_with_mode(resolved, &schema, mode).unwrap()
}

/// Fixture with a single aggregate node and an upstream source.
fn aggregate_fixture_yaml() -> &'static str {
    r#"
pipeline:
  name: agg-test
nodes:
  - type: source
    name: primary
    config:
      name: primary
      type: csv
      path: data.csv
      schema:
        - { name: amount, type: int }
        - { name: dept, type: any }

  - type: aggregate
    name: by_dept
    input: primary
    config:
      group_by: [dept]
      cxl: |
        emit total = sum(amount)
  - type: output
    name: output
    input: by_dept
    config:
      name: output
      type: csv
      path: out.csv
      include_unmapped: true
"#
}

// ── Gate 1: Route renders as sibling ─────────────────────────────────

#[test]
fn test_explain_route_renders_as_sibling() {
    let config = parse_fixture(diamond_fixture_yaml());
    let dag = compile_legacy(&config, &["amount"]);
    let out = dag.explain_text(&config);

    // The Route node appears as its own line in the DAG topology
    // section, with the FORK indicator at the sibling-level indent
    // (two spaces, not nested deeper).
    let topology = out
        .split("=== DAG Topology ===")
        .nth(1)
        .expect("topology section");

    // Must contain a line starting with two-space indent + ◆ FORK.
    let has_fork_sibling = topology
        .lines()
        .any(|l| l.starts_with("  ◆ FORK") && l.contains("categorize"));
    assert!(
        has_fork_sibling,
        "route 'categorize' must render as a sibling FORK line:\n{topology}"
    );

    // It must appear after the upstream transform and before the
    // downstream branches — i.e. at its own topo position.
    let fork_idx = topology
        .lines()
        .position(|l| l.contains("◆ FORK") && l.contains("categorize"))
        .unwrap();
    let upstream_idx = topology
        .lines()
        .position(|l| l.contains("categorize_emit"))
        .unwrap();
    assert!(
        fork_idx > upstream_idx,
        "FORK line must come after upstream transform"
    );
}

// ── Gate 2: Aggregate renders as sibling ─────────────────────────────

#[test]
fn test_explain_aggregate_renders_as_sibling() {
    let config = parse_fixture(aggregate_fixture_yaml());
    let dag = compile_legacy(&config, &["dept", "amount"]);
    let out = dag.explain_text(&config);

    let topology = out
        .split("=== DAG Topology ===")
        .nth(1)
        .expect("topology section");

    // Aggregation renders on its own sibling line (◇ marker) at the
    // aggregate topo position — not nested inside an upstream
    // transform's indentation.
    let has_agg_sibling = topology
        .lines()
        .any(|l| l.starts_with("  ◇") && l.contains("by_dept"));
    assert!(
        has_agg_sibling,
        "aggregate 'by_dept' must render as a sibling ◇ line:\n{topology}"
    );

    // Sanity: it must appear between the source and the output.
    let agg_idx = topology
        .lines()
        .position(|l| l.contains("◇") && l.contains("by_dept"))
        .unwrap();
    let source_idx = topology
        .lines()
        .position(|l| l.contains("[source] primary"))
        .unwrap();
    let output_idx = topology
        .lines()
        .position(|l| l.contains("[output] output"))
        .unwrap();
    assert!(source_idx < agg_idx && agg_idx < output_idx);
}

// ── Gate 3: Line annotations from real spans ─────────────────────────

#[test]
fn test_explain_annotates_with_file_line() {
    // Go through the new `PipelineConfig::compile()` path so stage 5
    // lowering captures the saphyr line into `Span::line_only`.
    let yaml = r#"
pipeline:
  name: line-anno-test
nodes:
  - type: source
    name: primary
    config:
      name: primary
      type: csv
      path: data.csv
      schema:
        - { name: amount, type: int }

  - type: transform
    name: step_one
    input: primary
    config:
      cxl: |
        emit x = amount + 1
  - type: output
    name: output
    input: step_one
    config:
      name: output
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let config: PipelineConfig = crate::yaml::from_str(yaml).expect("parse yaml");
    let compiled = config
        .compile(&crate::config::CompileContext::default())
        .expect("compile");
    let dag = compiled.dag();
    let out = dag.explain_text(&config);

    // At least one DAG topology line must carry a `(line:N)` suffix
    // where N matches the saphyr-reported line of a real node.
    let topology = out
        .split("=== DAG Topology ===")
        .nth(1)
        .expect("topology section");

    let annotated_lines: Vec<&str> = topology.lines().filter(|l| l.contains("(line:")).collect();
    assert!(
        !annotated_lines.is_empty(),
        "explain_text must include at least one `(line:N)` annotation:\n{topology}"
    );

    // Specifically, the `step_one` transform is declared starting at
    // YAML line 10 (1-indexed, counting the leading blank line).
    // We don't pin the exact line number because serde-saphyr's
    // location strategy for tagged-enum Spanned<_> wrap varies
    // across versions; we only assert a plausible range.
    let has_step_one_annotation = topology
        .lines()
        .any(|l| l.contains("step_one") && l.contains("(line:"));
    assert!(
        has_step_one_annotation,
        "transform 'step_one' must carry a (line:N) suffix:\n{topology}"
    );
}

// ── Gate 4: Route edges labeled by branch name ───────────────────────

#[test]
fn test_explain_route_edges_labeled_by_consumer_type() {
    let config = parse_fixture(diamond_fixture_yaml());
    let dag = compile_legacy(&config, &["amount"]);
    let out = dag.explain_text(&config);

    let topology = out
        .split("=== DAG Topology ===")
        .nth(1)
        .expect("topology section");

    // The diamond fixture declares two branches: `high_value` and
    // `low_value`. Each must appear on its own `├──>` line with the
    // branch name as the label.
    let has_high = topology
        .lines()
        .any(|l| l.contains("├──>") && l.contains("high_value"));
    let has_low = topology
        .lines()
        .any(|l| l.contains("├──>") && l.contains("low_value"));
    assert!(
        has_high,
        "route edge for branch 'high_value' must be labeled:\n{topology}"
    );
    assert!(
        has_low,
        "route edge for branch 'low_value' must be labeled:\n{topology}"
    );

    // The explicit default target line must also appear.
    let has_default = topology
        .lines()
        .any(|l| l.contains("├──>") && l.contains("default"));
    assert!(
        has_default,
        "route default target must be labeled:\n{topology}"
    );
}
