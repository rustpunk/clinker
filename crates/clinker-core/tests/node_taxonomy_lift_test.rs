//! Phase 16b Wave 1 gate tests for the node taxonomy lift.
//!
//! Covers parse-time correctness for the unified `nodes:` enum
//! (`PipelineNode`) and the `compile_validate()` topology stages 1–4
//! (duplicates, self-loops, cycles, path validation). Stage 5
//! (per-variant lowering) is Wave 2 — tests that depend on it are
//! marked `#[ignore] // TODO(16b Wave 2)` below.

use clinker_core::config::{PipelineConfig, PipelineNode};
use clinker_core::yaml::{Spanned, from_str};

#[test]
fn test_node_taxonomy_smoke() {
    // Harness compiles and runs.
}

// ---------------------------------------------------------------------
// PipelineNode variant parsing (Wave 1, no lowering required)
// ---------------------------------------------------------------------

fn parse_node(yaml: &str) -> Spanned<PipelineNode> {
    from_str::<Spanned<PipelineNode>>(yaml).expect("parse PipelineNode")
}

#[test]
fn test_pipeline_node_parses_source() {
    let yaml = r#"
type: source
name: input_csv
description: "Customer extract"
config:
  name: input_csv
  type: csv
  path: in.csv
"#;
    let node = parse_node(yaml);
    assert!(matches!(node.value, PipelineNode::Source { .. }));
    assert_eq!(node.value.type_tag(), "source");
    assert_eq!(node.value.name(), "input_csv");
}

#[test]
fn test_pipeline_node_parses_transform() {
    let yaml = r#"
type: transform
name: clean
input: input_csv
config:
  cxl: |
    emit foo = "bar"
"#;
    let node = parse_node(yaml);
    assert!(matches!(node.value, PipelineNode::Transform { .. }));
    assert_eq!(node.value.name(), "clean");
}

#[test]
fn test_pipeline_node_parses_aggregate() {
    let yaml = r#"
type: aggregate
name: rollup
input: clean
config:
  group_by: [region]
  cxl: |
    emit total = sum($record.amount)
"#;
    let node = parse_node(yaml);
    assert!(matches!(node.value, PipelineNode::Aggregate { .. }));
}

#[test]
fn test_pipeline_node_parses_route() {
    let yaml = r#"
type: route
name: split
input: clean
config:
  mode: exclusive
  default: leftover
  conditions:
    domestic: "$record.country == 'US'"
    intl: "$record.country != 'US'"
"#;
    let node = parse_node(yaml);
    assert!(matches!(node.value, PipelineNode::Route { .. }));
}

#[test]
fn test_pipeline_node_parses_merge() {
    let yaml = r#"
type: merge
name: rejoin
inputs:
  - rollup
  - split.intl
"#;
    let node = parse_node(yaml);
    assert!(matches!(node.value, PipelineNode::Merge { .. }));
}

#[test]
fn test_pipeline_node_parses_output() {
    let yaml = r#"
type: output
name: out_us
input: rejoin
config:
  name: out_us
  type: csv
  path: out_us.csv
"#;
    let node = parse_node(yaml);
    assert!(matches!(node.value, PipelineNode::Output { .. }));
}

#[test]
fn test_pipeline_node_rejects_unknown_type() {
    let yaml = r#"
type: bogus
name: x
"#;
    let result = from_str::<Spanned<PipelineNode>>(yaml);
    assert!(result.is_err());
}

#[test]
fn test_transform_missing_cxl_is_parse_error() {
    let yaml = r#"
type: transform
name: clean
input: src
config: {}
"#;
    let result = from_str::<Spanned<PipelineNode>>(yaml);
    assert!(result.is_err(), "missing cxl should be a parse error");
}

#[test]
fn test_transform_no_parallelism_field() {
    // `parallelism:` was deleted in Phase 16b. With deny_unknown_fields
    // on TransformBody it must be rejected.
    let yaml = r#"
type: transform
name: clean
input: src
config:
  cxl: "emit foo = 1"
  parallelism: sequential
"#;
    let result = from_str::<Spanned<PipelineNode>>(yaml);
    assert!(
        result.is_err(),
        "parallelism field on transform must be rejected"
    );
}

#[test]
fn test_pipeline_config_rejects_transform_without_input() {
    let yaml = r#"
pipeline:
  name: t
nodes:
  - type: transform
    name: clean
    config:
      cxl: "emit a = 1"
"#;
    let result = clinker_core::yaml::from_str::<PipelineConfig>(yaml);
    assert!(
        result.is_err(),
        "transform missing `input:` must be a parse error"
    );
}

// ---------------------------------------------------------------------
// compile_validate() — stages 1–4
// ---------------------------------------------------------------------

fn parse_pipeline(yaml: &str) -> PipelineConfig {
    clinker_core::yaml::from_str::<PipelineConfig>(yaml).expect("parse PipelineConfig")
}

#[test]
fn test_pipeline_config_parses_unified_nodes() {
    let yaml = r#"
pipeline:
  name: minimal
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: /tmp/in.csv
  - type: transform
    name: clean
    input: src
    config:
      cxl: "emit foo = 1"
  - type: output
    name: out
    input: clean
    config:
      name: out
      type: csv
      path: /tmp/out.csv
"#;
    let cfg = parse_pipeline(yaml);
    assert_eq!(cfg.nodes.len(), 3);
}

#[test]
fn test_pipeline_config_rejects_duplicate_names_e001() {
    let yaml = r#"
pipeline:
  name: dup
nodes:
  - type: source
    name: dup
    config:
      name: dup
      type: csv
      path: /tmp/a.csv
  - type: transform
    name: dup
    input: dup
    config:
      cxl: "emit a = 1"
"#;
    let cfg = parse_pipeline(yaml);
    let diags = cfg.compile_validate();
    assert!(
        diags.iter().any(|d| d.code == "E001"),
        "expected E001, got: {:?}",
        diags.iter().map(|d| &d.code).collect::<Vec<_>>()
    );
}

#[test]
fn test_pipeline_config_warns_case_only_duplicate_w002() {
    let yaml = r#"
pipeline:
  name: w2
nodes:
  - type: source
    name: clean
    config:
      name: clean
      type: csv
      path: /tmp/a.csv
  - type: transform
    name: Clean
    input: clean
    config:
      cxl: "emit a = 1"
"#;
    let cfg = parse_pipeline(yaml);
    let diags = cfg.compile_validate();
    assert!(
        diags.iter().any(|d| d.code == "W002"),
        "expected W002, got: {:?}",
        diags.iter().map(|d| &d.code).collect::<Vec<_>>()
    );
    // Both nodes still distinct (no E001).
    assert!(!diags.iter().any(|d| d.code == "E001"));
}

#[test]
fn test_self_loop_e002_emitted_before_cycle_check() {
    let yaml = r#"
pipeline:
  name: sl
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: /tmp/a.csv
  - type: transform
    name: loop
    input: loop
    config:
      cxl: "emit a = 1"
"#;
    let cfg = parse_pipeline(yaml);
    let diags = cfg.compile_validate();
    assert!(diags.iter().any(|d| d.code == "E002"));
    // Self-loop should NOT also be reported as E003 (the dedicated
    // E002 pass strips it before tarjan_scc looks at it).
    assert!(!diags.iter().any(|d| d.code == "E003"));
}

#[test]
fn test_self_loop_via_merge_inputs() {
    let yaml = r#"
pipeline:
  name: sl
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: /tmp/a.csv
  - type: merge
    name: combo
    inputs:
      - src
      - combo
"#;
    let cfg = parse_pipeline(yaml);
    let diags = cfg.compile_validate();
    assert!(
        diags.iter().any(|d| d.code == "E002"),
        "expected E002 for self-merge, got {:?}",
        diags.iter().map(|d| &d.code).collect::<Vec<_>>()
    );
}

#[test]
fn test_compile_stages_run_in_fixed_order_topology_only() {
    // Pipeline with one duplicate AND one self-loop must report BOTH —
    // no short-circuit between stages 1 and 2.
    let yaml = r#"
pipeline:
  name: multi
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: /tmp/a.csv
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: /tmp/b.csv
  - type: transform
    name: loop
    input: loop
    config:
      cxl: "emit a = 1"
"#;
    let cfg = parse_pipeline(yaml);
    let diags = cfg.compile_validate();
    assert!(diags.iter().any(|d| d.code == "E001"), "missing E001");
    assert!(diags.iter().any(|d| d.code == "E002"), "missing E002");
}

// ---------------------------------------------------------------------
// Wave 2 — deferred tests (require lowering / Serialize / strict shape)
// ---------------------------------------------------------------------

#[test]
#[ignore = "TODO(16b Wave 2): requires Span plumbing inside tagged variants (serde-saphyr limit)"]
fn test_cxl_source_in_variant_has_span() {}

#[test]
#[ignore = "TODO(16b Wave 2): PipelineNode does not yet derive Serialize"]
fn test_pipeline_node_serialize_then_parse_round_trip() {}

#[test]
#[ignore = "TODO(16b Wave 2): legacy TransformConfig stays as deprecated shim until Wave 2"]
fn test_no_transform_config_anywhere() {}

#[test]
#[ignore = "TODO(16b Wave 2): legacy `transformations:` shape still accepted by Wave 1 shim"]
fn test_pipeline_config_old_shape_fails() {}

#[test]
#[ignore = "TODO(16b Wave 2): requires per-variant lowering (compile() stage 5)"]
fn test_compile_composition_stub_one_diagnostic_per_instance() {}

#[test]
#[ignore = "TODO(16b Wave 2): requires per-variant lowering (compile() stage 5)"]
fn test_compile_composition_does_not_abort() {}
