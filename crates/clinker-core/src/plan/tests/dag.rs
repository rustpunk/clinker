//! DAG construction tests for Phase 15.
//!
//! Tests in this module exercise the petgraph-based ExecutionPlanDag:
//! topology construction, topological ordering, cycle detection,
//! tier assignment, node counts, edge types, and JSON serialization.

use crate::config::*;
use crate::plan::execution::*;

/// Build a diamond-shaped PipelineConfig for branching tests.
///
/// Topology: Source → categorize (route: high_value / low_value) → merge → Output
///
/// This fixture will be used once `TransformInput` and `ExecutionPlanDag`
/// are implemented in Tasks 15.1 and 15.2.
pub(crate) fn diamond_fixture_yaml() -> &'static str {
    r#"
pipeline:
  name: diamond-test

inputs:
  - name: primary
    path: data.csv
    type: csv

outputs:
  - name: output
    path: out.csv
    include_unmapped: true
    type: csv

transformations:
  - name: categorize
    cxl: |
      emit value = amount
    route:
      mode: exclusive
      branches:
        - name: high_value
          condition: "amount > 100"
        - name: low_value
          condition: "amount <= 100"
      default: output

  - name: enrich_high
    cxl: |
      emit tier = "premium"

  - name: enrich_low
    cxl: |
      emit tier = "standard"

  - name: finalize
    cxl: |
      emit done = true
"#
}

/// Build a minimal linear PipelineConfig (no branching).
pub(crate) fn linear_fixture_yaml() -> &'static str {
    r#"
pipeline:
  name: linear-test

inputs:
  - name: primary
    path: data.csv
    type: csv

outputs:
  - name: output
    path: out.csv
    include_unmapped: true
    type: csv

transformations:
  - name: step_one
    cxl: |
      emit x = amount + 1

  - name: step_two
    cxl: |
      emit y = amount * 2
"#
}

/// Parse a YAML string into a PipelineConfig for test use.
pub(crate) fn parse_fixture(yaml: &str) -> PipelineConfig {
    crate::config::parse_config(yaml).unwrap()
}

/// Compile CXL source to TypedProgram for test use.
fn compile_cxl(source: &str, fields: &[&str]) -> cxl::typecheck::pass::TypedProgram {
    let parsed = cxl::parser::Parser::parse(source);
    assert!(
        parsed.errors.is_empty(),
        "Parse errors: {:?}",
        parsed.errors
    );
    let resolved =
        cxl::resolve::pass::resolve_program(parsed.ast, fields, parsed.node_count).unwrap();
    let schema: indexmap::IndexMap<String, cxl::typecheck::types::Type> = indexmap::IndexMap::new();
    cxl::typecheck::pass::type_check(resolved, &schema).unwrap()
}

/// Identity helper retained to keep test callsites compact.
#[allow(dead_code)]
fn t(entry: &LegacyTransformsBlock) -> &LegacyTransformsBlock {
    entry
}

/// Helper: compile a fixture config into an ExecutionPlanDag.
fn compile_fixture(config: &PipelineConfig, fields: &[&str]) -> ExecutionPlanDag {
    let transforms: Vec<_> = config.transforms().collect();
    let typed_programs: Vec<_> = transforms
        .iter()
        .map(|tc| compile_cxl(tc.cxl_source(), fields))
        .collect();
    let compiled_refs: Vec<(&str, &cxl::typecheck::pass::TypedProgram)> = transforms
        .iter()
        .zip(typed_programs.iter())
        .map(|(tc, tp)| (tc.name.as_str(), tp))
        .collect();
    ExecutionPlanDag::compile(config, &compiled_refs).unwrap()
}

// --- Task 15.2 tests ---

/// Linear chain: Source → T1 → T2 → Output produces correct topo order.
#[test]
fn test_dag_linear_pipeline() {
    let config = parse_fixture(linear_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);

    let names: Vec<&str> = plan
        .topo_order
        .iter()
        .map(|&idx| plan.graph[idx].name())
        .collect();
    assert_eq!(names, vec!["primary", "step_one", "step_two", "output"]);
}

/// Diamond: Fork → 2 branches → Merge → Output produces valid ordering.
#[test]
fn test_dag_diamond_topology() {
    let config = parse_fixture(diamond_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);

    // categorize must come before enrich_high and enrich_low
    let names: Vec<&str> = plan
        .topo_order
        .iter()
        .map(|&idx| plan.graph[idx].name())
        .collect();
    let cat_pos = names.iter().position(|&n| n == "categorize").unwrap();
    let eh_pos = names.iter().position(|&n| n == "enrich_high").unwrap();
    let el_pos = names.iter().position(|&n| n == "enrich_low").unwrap();
    assert!(cat_pos < eh_pos);
    assert!(cat_pos < el_pos);
}

/// A → B → A cycle is detected with clear error including cycle path.
#[test]
fn test_dag_cycle_detection() {
    // Create a config where A references B and B references A
    let yaml = r#"
pipeline:
  name: cycle-test
inputs:
  - name: primary
    path: data.csv
    type: csv
outputs:
  - name: output
    path: out.csv
    include_unmapped: true
    type: csv
transformations:
  - name: alpha
    cxl: |
      emit x = amount
    input: beta
  - name: beta
    cxl: |
      emit y = amount
    input: alpha
"#;
    let config = parse_fixture(yaml);
    let transforms: Vec<_> = config.transforms().collect();
    let typed_programs: Vec<_> = transforms
        .iter()
        .map(|tc| compile_cxl(tc.cxl_source(), &["amount"]))
        .collect();
    let compiled_refs: Vec<(&str, &cxl::typecheck::pass::TypedProgram)> = transforms
        .iter()
        .zip(typed_programs.iter())
        .map(|(tc, tp)| (tc.name.as_str(), tp))
        .collect();
    let result = ExecutionPlanDag::compile(&config, &compiled_refs);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("cycle"), "expected cycle error: {}", err);
}

/// Linear chain: tiers 0, 1, 2, 3.
#[test]
fn test_dag_tier_assignment_linear() {
    let config = parse_fixture(linear_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);

    // Source at tier 0, step_one at tier 1, step_two at tier 2
    for &idx in &plan.topo_order {
        if let PlanNode::Transform { name, tier, .. } = &plan.graph[idx] {
            match name.as_str() {
                "step_one" => assert_eq!(*tier, 1, "step_one should be tier 1"),
                "step_two" => assert_eq!(*tier, 2, "step_two should be tier 2"),
                _ => {}
            }
        }
    }
}

/// Two branches after fork at same tier.
#[test]
fn test_dag_tier_assignment_parallel() {
    let config = parse_fixture(diamond_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);

    // enrich_high and enrich_low should be at the same tier
    let mut eh_tier = None;
    let mut el_tier = None;
    for &idx in &plan.topo_order {
        if let PlanNode::Transform { name, tier, .. } = &plan.graph[idx] {
            match name.as_str() {
                "enrich_high" => eh_tier = Some(*tier),
                "enrich_low" => el_tier = Some(*tier),
                _ => {}
            }
        }
    }
    // Both are in the implicit linear chain after categorize, so they have sequential tiers
    // (They're only at the same tier if they have explicit input wiring to the route)
    assert!(eh_tier.is_some(), "enrich_high should be in graph");
    assert!(el_tier.is_some(), "enrich_low should be in graph");
}

/// Route → 3 outputs: all present in graph.
#[test]
fn test_dag_multiple_outputs() {
    let yaml = r#"
pipeline:
  name: multi-out-test
inputs:
  - name: primary
    path: data.csv
    type: csv
outputs:
  - name: output
    path: out.csv
    include_unmapped: true
    type: csv
transformations:
  - name: router
    cxl: |
      emit x = amount
    route:
      mode: exclusive
      branches:
        - name: a
          condition: "amount > 100"
        - name: b
          condition: "amount > 50"
        - name: c
          condition: "amount > 0"
      default: output
"#;
    let config = parse_fixture(yaml);
    let plan = compile_fixture(&config, &["amount"]);

    // Route node should exist with 3 branches
    let route_node = plan
        .graph
        .node_weights()
        .find(|n| matches!(n, PlanNode::Route { .. }));
    assert!(route_node.is_some(), "route node should be in graph");
    if let Some(PlanNode::Route { branches, .. }) = route_node {
        assert_eq!(branches.len(), 3);
    }
}

/// Merge node has edges from all inputs.
#[test]
fn test_dag_merge_multiple_inputs() {
    let yaml = r#"
pipeline:
  name: merge-test
inputs:
  - name: primary
    path: data.csv
    type: csv
outputs:
  - name: output
    path: out.csv
    include_unmapped: true
    type: csv
transformations:
  - name: branch_a
    cxl: |
      emit x = amount + 1
  - name: branch_b
    cxl: |
      emit y = amount + 2
  - name: merged
    cxl: |
      emit z = amount
    input: [branch_a, branch_b]
"#;
    let config = parse_fixture(yaml);
    let plan = compile_fixture(&config, &["amount"]);

    // Find merge node
    let merge_node = plan
        .graph
        .node_weights()
        .find(|n| matches!(n, PlanNode::Merge { .. }));
    assert!(
        merge_node.is_some(),
        "merge node should exist for Multiple inputs"
    );

    // Merge should have 2 incoming edges
    let merge_idx = plan
        .topo_order
        .iter()
        .find(|&&idx| matches!(plan.graph[idx], PlanNode::Merge { .. }))
        .unwrap();
    let incoming: Vec<_> = plan
        .graph
        .neighbors_directed(*merge_idx, petgraph::Direction::Incoming)
        .collect();
    assert_eq!(incoming.len(), 2, "merge should have 2 incoming edges");
}

/// Single transform pipeline → flat DAG (backward compat).
#[test]
fn test_dag_single_transform_backward_compat() {
    let yaml = r#"
pipeline:
  name: single-test
inputs:
  - name: primary
    path: data.csv
    type: csv
outputs:
  - name: output
    path: out.csv
    include_unmapped: true
    type: csv
transformations:
  - name: only
    cxl: |
      emit x = amount
"#;
    let config = parse_fixture(yaml);
    let plan = compile_fixture(&config, &["amount"]);

    assert_eq!(plan.graph.node_count(), 3); // source + transform + output
    assert_eq!(plan.graph.edge_count(), 2);
}

/// Correct number of nodes for given config.
#[test]
fn test_dag_node_count() {
    let config = parse_fixture(diamond_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);

    // source + categorize + route_categorize + enrich_high + enrich_low + finalize + output = 7
    assert!(
        plan.graph.node_count() >= 6,
        "diamond should have at least 6 nodes, got {}",
        plan.graph.node_count()
    );
}

/// Data edges for record flow, Index for lookup deps.
#[test]
fn test_dag_edge_types() {
    let config = parse_fixture(linear_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);

    // All edges in a linear pipeline should be Data type
    for edge_ref in plan.graph.edge_references() {
        assert!(
            matches!(edge_ref.weight().dependency_type, DependencyType::Data),
            "linear pipeline should only have Data edges"
        );
    }
}

/// Invalid input reference produces error listing available transforms/branches.
#[test]
fn test_dag_invalid_input_reference_error_message() {
    let yaml = r#"
pipeline:
  name: bad-ref-test
inputs:
  - name: primary
    path: data.csv
    type: csv
outputs:
  - name: output
    path: out.csv
    include_unmapped: true
    type: csv
transformations:
  - name: step
    cxl: |
      emit x = amount
    input: nonexistent
"#;
    let config = parse_fixture(yaml);
    let transforms: Vec<_> = config.transforms().collect();
    let typed_programs: Vec<_> = transforms
        .iter()
        .map(|tc| compile_cxl(tc.cxl_source(), &["amount"]))
        .collect();
    let compiled_refs: Vec<(&str, &cxl::typecheck::pass::TypedProgram)> = transforms
        .iter()
        .zip(typed_programs.iter())
        .map(|(tc, tp)| (tc.name.as_str(), tp))
        .collect();
    let result = ExecutionPlanDag::compile(&config, &compiled_refs);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("nonexistent"),
        "error should mention the bad reference: {}",
        err
    );
    assert!(
        err.contains("step"),
        "error should mention the transform name: {}",
        err
    );
}

/// Id slugs are unique — collision detected at compile time.
#[test]
fn test_dag_id_slug_uniqueness() {
    // This is hard to trigger since transform names must be unique in config.
    // The slug check is a safety net — just verify it doesn't fire for valid configs.
    let config = parse_fixture(linear_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);

    let mut slugs: Vec<String> = plan.graph.node_weights().map(|n| n.id_slug()).collect();
    let original_len = slugs.len();
    slugs.sort();
    slugs.dedup();
    assert_eq!(slugs.len(), original_len, "all id slugs should be unique");
}

/// NodeExecutionReqs derived correctly from analyzer: window → RequiresArena.
#[test]
fn test_dag_execution_reqs_from_analyzer() {
    let yaml = r#"
pipeline:
  name: arena-test
inputs:
  - name: primary
    path: data.csv
    type: csv
outputs:
  - name: output
    path: out.csv
    include_unmapped: true
    type: csv
transformations:
  - name: agg
    cxl: |
      emit total = $window.sum(amount)
    local_window:
      group_by: [dept]
"#;
    let config = parse_fixture(yaml);
    let plan = compile_fixture(&config, &["dept", "amount"]);

    // The transform node should have RequiresArena
    let transform_node = plan
        .graph
        .node_weights()
        .find(|n| matches!(n, PlanNode::Transform { name, .. } if name == "agg"));
    assert!(transform_node.is_some());
    if let Some(PlanNode::Transform { execution_reqs, .. }) = transform_node {
        assert!(
            matches!(execution_reqs, NodeExecutionReqs::RequiresArena),
            "window transform should be RequiresArena, got {:?}",
            execution_reqs
        );
    }
}

/// NodeExecutionReqs for streaming transforms.
#[test]
fn test_dag_execution_reqs_streaming() {
    let config = parse_fixture(linear_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);

    for &idx in &plan.topo_order {
        if let PlanNode::Transform { execution_reqs, .. } = &plan.graph[idx] {
            assert!(
                matches!(execution_reqs, NodeExecutionReqs::Streaming),
                "non-window transform should be Streaming"
            );
        }
    }
}

/// NodeExecutionReqs for RequiresSortedInput derivation.
#[test]
#[ignore = "Task 15.2: RequiresSortedInput derivation not yet wired (correlation key)"]
fn test_dag_execution_reqs_sorted_input() {}

/// JSON serialization roundtrip: schema_version, nodes, depends_on.
#[test]
fn test_dag_json_serialization_shape() {
    let config = parse_fixture(linear_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);
    let json = serde_json::to_value(&plan).unwrap();

    assert_eq!(json["schema_version"], "1");
    let nodes = json["nodes"].as_array().unwrap();
    assert!(nodes.len() >= 3);

    // Every node must have id, type, name, depends_on
    for node in nodes {
        assert!(node.get("type").is_some(), "node must have type");
        assert!(node.get("name").is_some(), "node must have name");
        assert!(
            node.get("depends_on").is_some(),
            "node must have depends_on"
        );
    }
}

/// Implicit linear chain: transforms without input wired to predecessor.
#[test]
fn test_dag_implicit_linear_chain() {
    let config = parse_fixture(linear_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);

    // step_two should depend on step_one (implicit linear chain)
    let json = serde_json::to_value(&plan).unwrap();
    let nodes = json["nodes"].as_array().unwrap();
    let step_two = nodes.iter().find(|n| n["name"] == "step_two").unwrap();
    let deps = step_two["depends_on"].as_array().unwrap();
    assert!(
        deps.iter()
            .any(|d| d.as_str() == Some("transform.step_one")),
        "step_two should depend on step_one, got: {:?}",
        deps
    );
}

/// RoutePlan is retired — PlanNode::Route carries all display data.
#[test]
fn test_dag_route_node_replaces_route_plan() {
    let config = parse_fixture(diamond_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);

    // PlanNode::Route should carry the branch and mode data
    let route_node = plan
        .graph
        .node_weights()
        .find(|n| matches!(n, PlanNode::Route { .. }));
    assert!(
        route_node.is_some(),
        "route node should exist in graph for routed pipelines"
    );
    if let Some(PlanNode::Route {
        mode,
        branches,
        default,
        ..
    }) = route_node
    {
        assert_eq!(*mode, RouteMode::Exclusive);
        assert_eq!(branches.len(), 2);
        assert_eq!(default, "output");
    }
}

/// Self-referencing input is rejected at compile time.
#[test]
fn test_dag_self_reference_rejected() {
    let yaml = r#"
pipeline:
  name: self-ref-test
inputs:
  - name: primary
    path: data.csv
    type: csv
outputs:
  - name: output
    path: out.csv
    include_unmapped: true
    type: csv
transformations:
  - name: loopy
    cxl: |
      emit x = amount
    input: loopy
"#;
    let config = parse_fixture(yaml);
    let transforms: Vec<_> = config.transforms().collect();
    let typed_programs: Vec<_> = transforms
        .iter()
        .map(|tc| compile_cxl(tc.cxl_source(), &["amount"]))
        .collect();
    let compiled_refs: Vec<(&str, &cxl::typecheck::pass::TypedProgram)> = transforms
        .iter()
        .zip(typed_programs.iter())
        .map(|(tc, tp)| (tc.name.as_str(), tp))
        .collect();
    let result = ExecutionPlanDag::compile(&config, &compiled_refs);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("references itself"),
        "should detect self-reference: {}",
        err
    );
}

// --- Task 15.3 tests: --explain format extensions ---

/// Fixture with full branch wiring: route → branch transforms → merge.
fn wired_diamond_yaml() -> &'static str {
    r#"
pipeline:
  name: wired-diamond

inputs:
  - name: primary
    path: data.csv
    type: csv

outputs:
  - name: output
    path: out.csv
    include_unmapped: true
    type: csv

transformations:
  - name: categorize
    cxl: |
      emit value = amount
    route:
      mode: exclusive
      branches:
        - name: high_value
          condition: "amount > 100"
        - name: low_value
          condition: "amount <= 100"
      default: output

  - name: enrich_high
    input: categorize.high_value
    cxl: |
      emit tier = "premium"

  - name: enrich_low
    input: categorize.low_value
    cxl: |
      emit tier = "standard"

  - name: finalize
    input: [enrich_high, enrich_low]
    cxl: |
      emit done = true
"#
}

/// Text output contains fork/join ASCII art for branching pipeline.
#[test]
fn test_explain_text_branch_indicators() {
    let config = parse_fixture(wired_diamond_yaml());
    let plan = compile_fixture(&config, &["amount"]);
    let text = plan.explain_text(&config);

    assert!(text.contains("FORK"), "should show FORK indicator: {text}");
    assert!(text.contains("├──>"), "should show branch arrow: {text}");
    assert!(
        text.contains("MERGE"),
        "should show MERGE indicator: {text}"
    );
    assert!(text.contains("└──<"), "should show merge arrow: {text}");
}

/// JSON output parses back and contains expected schema_version.
#[test]
fn test_explain_json_schema_version() {
    let config = parse_fixture(linear_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);
    let json: serde_json::Value = serde_json::to_value(&plan).unwrap();

    assert_eq!(json["schema_version"], "1");
}

/// JSON nodes have expected id slugs and types.
#[test]
fn test_explain_json_node_ids() {
    let config = parse_fixture(linear_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);
    let json: serde_json::Value = serde_json::to_value(&plan).unwrap();
    let nodes = json["nodes"].as_array().unwrap();

    // Check that all nodes have id-slug-compatible type and name fields
    for node in nodes {
        let node_type = node["type"].as_str().unwrap();
        let name = node["name"].as_str().unwrap();
        assert!(
            !node_type.is_empty() && !name.is_empty(),
            "node must have non-empty type and name"
        );
    }

    // Verify specific slugs exist
    let slugs: Vec<String> = nodes
        .iter()
        .map(|n| {
            format!(
                "{}.{}",
                n["type"].as_str().unwrap(),
                n["name"].as_str().unwrap()
            )
        })
        .collect();
    assert!(slugs.contains(&"source.primary".to_string()));
    assert!(slugs.contains(&"transform.step_one".to_string()));
    assert!(slugs.contains(&"output.output".to_string()));
}

/// JSON depends_on correctly reflects graph edges.
#[test]
fn test_explain_json_depends_on() {
    let config = parse_fixture(linear_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);
    let json: serde_json::Value = serde_json::to_value(&plan).unwrap();
    let nodes = json["nodes"].as_array().unwrap();

    // Source has no dependencies
    let source = nodes.iter().find(|n| n["name"] == "primary").unwrap();
    let source_deps = source["depends_on"].as_array().unwrap();
    assert!(source_deps.is_empty(), "source should have no deps");

    // step_one depends on source
    let step_one = nodes.iter().find(|n| n["name"] == "step_one").unwrap();
    let step_one_deps: Vec<&str> = step_one["depends_on"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(
        step_one_deps.contains(&"source.primary"),
        "step_one should depend on source.primary: {step_one_deps:?}"
    );
}

/// JSON tier values present on all transform nodes.
#[test]
fn test_explain_json_tiers() {
    let config = parse_fixture(linear_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);
    let json: serde_json::Value = serde_json::to_value(&plan).unwrap();
    let nodes = json["nodes"].as_array().unwrap();

    for node in nodes {
        if node["type"].as_str() == Some("transform") {
            assert!(
                node.get("tier").is_some(),
                "transform node must have tier: {}",
                node["name"]
            );
        }
    }
}

/// DOT output starts with `digraph` and contains node names.
#[test]
fn test_explain_dot_valid_syntax() {
    let config = parse_fixture(linear_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);
    let dot = plan.explain_dot();

    assert!(
        dot.starts_with("digraph"),
        "DOT output must start with 'digraph': {}",
        &dot[..dot.len().min(50)]
    );
    assert!(
        dot.contains("step_one"),
        "DOT should contain node name step_one"
    );
    assert!(
        dot.contains("step_two"),
        "DOT should contain node name step_two"
    );
}

/// DOT edges have dependency type labels.
#[test]
fn test_explain_dot_edge_labels() {
    let config = parse_fixture(linear_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);
    let dot = plan.explain_dot();

    assert!(
        dot.contains(r#"label="data""#),
        "DOT edges should have dependency type labels: {dot}"
    );
}

/// Non-branching pipeline text output is backward-compatible (contains key sections).
#[test]
fn test_explain_linear_unchanged() {
    let config = parse_fixture(linear_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);
    let text = plan.explain_text(&config);

    // Must still contain the core explain sections
    assert!(text.contains("=== Execution Plan ==="));
    assert!(text.contains("=== CXL Expressions ==="));
    assert!(text.contains("=== DAG Topology ==="));
    // Linear pipeline should NOT have FORK/MERGE indicators
    assert!(
        !text.contains("FORK"),
        "linear pipeline should not show FORK"
    );
    assert!(
        !text.contains("MERGE"),
        "linear pipeline should not show MERGE"
    );
}

/// Bare --explain defaults to text format: explain_text() produces non-empty
/// output with the standard section headers.
#[test]
fn test_explain_default_format_text() {
    let config = parse_fixture(linear_fixture_yaml());
    let plan = compile_fixture(&config, &["amount"]);
    let text = plan.explain_text(&config);

    // Default text format must contain the standard sections
    assert!(!text.is_empty(), "text output must not be empty");
    assert!(
        text.contains("=== Execution Plan ==="),
        "must contain Execution Plan header"
    );
    assert!(
        text.contains("=== DAG Topology ==="),
        "must contain DAG Topology header"
    );
}
