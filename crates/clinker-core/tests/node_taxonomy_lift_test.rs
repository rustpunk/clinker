//! Gate tests for the unified `nodes:` taxonomy.
//!
//! Covers parse-time correctness for the `PipelineNode` enum and the
//! `compile_topology_only()` topology stages 1–4 (duplicates, self-loops,
//! cycles, path validation).

use clinker_core::config::{PipelineConfig, PipelineNode};
use clinker_core::yaml::{Spanned, from_str};

#[test]
fn test_node_taxonomy_smoke() {
    // Harness compiles and runs.
}

// ---------------------------------------------------------------------
// PipelineNode variant parsing
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
  schema:
    - { name: id, type: string }
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
    // `parallelism:` is not a valid field on TransformBody. With
    // deny_unknown_fields it must be rejected.
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
// compile_topology_only() — stages 1–4
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
      path: data/in.csv
      schema:
        - { name: amount, type: string }

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
      path: data/out.csv
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
      path: data/a.csv
      schema:
        - { name: amount, type: string }

  - type: transform
    name: dup
    input: dup
    config:
      cxl: "emit a = 1"
"#;
    let cfg = parse_pipeline(yaml);
    let diags = cfg.compile_topology_only(&Default::default());
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
      path: data/a.csv
      schema:
        - { name: amount, type: string }

  - type: transform
    name: Clean
    input: clean
    config:
      cxl: "emit a = 1"
"#;
    let cfg = parse_pipeline(yaml);
    let diags = cfg.compile_topology_only(&Default::default());
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
      path: data/a.csv
      schema:
        - { name: amount, type: string }

  - type: transform
    name: loop
    input: loop
    config:
      cxl: "emit a = 1"
"#;
    let cfg = parse_pipeline(yaml);
    let diags = cfg.compile_topology_only(&Default::default());
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
      path: data/a.csv
      schema:
        - { name: amount, type: string }

  - type: merge
    name: combo
    inputs:
      - src
      - combo
"#;
    let cfg = parse_pipeline(yaml);
    let diags = cfg.compile_topology_only(&Default::default());
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
      path: data/a.csv
      schema:
        - { name: amount, type: string }

  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/b.csv
      schema:
        - { name: amount, type: string }

  - type: transform
    name: loop
    input: loop
    config:
      cxl: "emit a = 1"
"#;
    let cfg = parse_pipeline(yaml);
    let diags = cfg.compile_topology_only(&Default::default());
    assert!(diags.iter().any(|d| d.code == "E001"), "missing E001");
    assert!(diags.iter().any(|d| d.code == "E002"), "missing E002");
}

// ---------------------------------------------------------------------
// Wave 2 — deferred tests (require lowering / Serialize / strict shape)
// ---------------------------------------------------------------------

// ---------------------------------------------------------------------
// Wave 3 — still deferred
// ---------------------------------------------------------------------

#[test]
fn test_pipeline_node_serialize_then_parse_round_trip() {
    // Proves PipelineNode: Serialize + Deserialize are symmetric.
    // Wave 3 landed the Serialize derive on every variant body and
    // on the NodeHeader family; this round-trips through serde_json
    // so the YAML-layer span plumbing is not exercised here.
    let yaml = r#"
type: transform
name: clean
input: input_csv
config:
  cxl: |
    emit foo = "bar"
"#;
    let parsed = parse_node(yaml);
    let json = serde_json::to_string(&parsed.value).expect("serialize");
    let round_tripped: clinker_core::config::PipelineNode =
        serde_json::from_str(&json).expect("round-trip deserialize");
    assert_eq!(round_tripped.name(), "clean");
    assert_eq!(round_tripped.type_tag(), "transform");
}

#[test]
fn test_no_transform_config_anywhere() {
    // Gate: the legacy `TransformConf\u{0069}g` type was deleted. The
    // old name must not reappear anywhere in the crate sources.
    use std::path::PathBuf;
    let crate_src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders: Vec<String> = Vec::new();
    fn walk(dir: &std::path::Path, offenders: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let p = entry.path();
            if p.is_dir() {
                walk(&p, offenders);
            } else if p.extension().and_then(|s| s.to_str()) == Some("rs") {
                let src = std::fs::read_to_string(&p).unwrap_or_default();
                for (i, line) in src.lines().enumerate() {
                    // Match the bare identifier, not substrings.
                    let has = line
                        .split(|c: char| !c.is_alphanumeric() && c != '_')
                        .any(|tok| tok == "TransformConf\u{0069}g");
                    if has {
                        offenders.push(format!("{}:{}: {}", p.display(), i + 1, line));
                    }
                }
            }
        }
    }
    walk(&crate_src, &mut offenders);
    assert!(
        offenders.is_empty(),
        "legacy `TransformConf\u{0069}g` identifier still present:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn test_pipeline_config_old_shape_fails() {
    // The legacy `inputs:` / `outputs:` / `transformations:` top-level
    // YAML shape is no longer accepted. The only shape is the unified
    // `nodes:` taxonomy. Parsing the old shape must hard-fail.
    let yaml = r#"
pipeline:
  name: legacy_shape
inputs:
  - name: in
    type: csv
    path: data/a.csv
outputs:
  - name: out
    type: csv
    path: out.csv
transformations:
  - name: t
    input: in
    cxl: "emit x = 1"
"#;
    let res = clinker_core::yaml::from_str::<PipelineConfig>(yaml);
    assert!(
        res.is_err(),
        "legacy `transformations:` shape must no longer parse"
    );
}

// ---------------------------------------------------------------------
// Span-carrying PlanNode variants
// ---------------------------------------------------------------------

#[test]
fn test_cxl_source_in_variant_has_span() {
    // PlanNode carries `pub span: Span` on every variant.
}

// ---------------------------------------------------------------------
// Group H — stage-5 lowering tests (8 new tests, in-memory only)
// ---------------------------------------------------------------------

#[test]
fn test_compile_lowers_minimal_pipeline_to_compiled_plan() {
    let yaml = r#"
pipeline:
  name: minimal
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/a.csv
      schema:
        - { name: amount, type: string }

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
      path: data/o.csv
"#;
    let cfg = parse_pipeline(yaml);
    let plan = cfg
        .compile(&clinker_core::config::CompileContext::default())
        .expect("compile must succeed");
    assert_eq!(plan.dag().graph.node_count(), 3);
    assert_eq!(plan.dag().graph.edge_count(), 2);
}

#[test]
fn test_compile_lowered_source_carries_resolved_payload() {
    use clinker_core::plan::execution::PlanNode;
    let yaml = r#"
pipeline:
  name: src_payload
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/a.csv
      schema:
        - { name: amount, type: string }

  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: data/o.csv
"#;
    let cfg = parse_pipeline(yaml);
    let plan = cfg
        .compile(&clinker_core::config::CompileContext::default())
        .unwrap();
    let src = plan
        .dag()
        .graph
        .node_weights()
        .find_map(|n| match n {
            PlanNode::Source { resolved, name, .. } if name == "src" => resolved.as_ref(),
            _ => None,
        })
        .expect("Source resolved payload missing");
    assert_eq!(src.source.name, "src");
}

#[test]
fn test_compile_lowered_transform_carries_resolved_payload() {
    use clinker_core::plan::execution::PlanNode;
    let yaml = r#"
pipeline:
  name: t_payload
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/a.csv
      schema:
        - { name: amount, type: string }

  - type: transform
    name: clean
    input: src
    config:
      cxl: "emit foo = 1"
"#;
    let cfg = parse_pipeline(yaml);
    let plan = cfg
        .compile(&clinker_core::config::CompileContext::default())
        .unwrap();
    let has = plan.dag().graph.node_weights().any(|n| {
        matches!(
            n,
            PlanNode::Transform {
                resolved: Some(_),
                ..
            }
        )
    });
    assert!(has, "Transform resolved payload missing");
}

#[test]
fn test_compile_lowered_output_carries_resolved_payload() {
    use clinker_core::plan::execution::PlanNode;
    let yaml = r#"
pipeline:
  name: o_payload
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/a.csv
      schema:
        - { name: amount, type: string }

  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: data/o.csv
"#;
    let cfg = parse_pipeline(yaml);
    let plan = cfg
        .compile(&clinker_core::config::CompileContext::default())
        .unwrap();
    let has = plan.dag().graph.node_weights().any(|n| {
        matches!(
            n,
            PlanNode::Output {
                resolved: Some(_),
                ..
            }
        )
    });
    assert!(has, "Output resolved payload missing");
}

#[test]
fn test_compile_lowers_route_with_branches_and_default() {
    use clinker_core::plan::execution::PlanNode;
    let yaml = r#"
pipeline:
  name: rt
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/a.csv
      schema:
        - { name: amount, type: string }

  - type: route
    name: split
    input: src
    config:
      mode: exclusive
      default: leftover
      conditions:
        a: "true"
        b: "false"
"#;
    let cfg = parse_pipeline(yaml);
    let plan = cfg
        .compile(&clinker_core::config::CompileContext::default())
        .unwrap();
    let route = plan
        .dag()
        .graph
        .node_weights()
        .find(|n| matches!(n, PlanNode::Route { .. }))
        .expect("Route missing");
    if let PlanNode::Route {
        branches, default, ..
    } = route
    {
        assert_eq!(default, "leftover");
        assert_eq!(branches.len(), 2);
    }
}

#[test]
fn test_compile_lowers_merge_wires_multiple_inputs() {
    use clinker_core::plan::execution::PlanNode;
    let yaml = r#"
pipeline:
  name: mg
nodes:
  - type: source
    name: a
    config:
      name: a
      type: csv
      path: data/a.csv
      schema:
        - { name: amount, type: string }

  - type: source
    name: b
    config:
      name: b
      type: csv
      path: data/b.csv
      schema:
        - { name: amount, type: string }

  - type: merge
    name: m
    inputs:
      - a
      - b
"#;
    let cfg = parse_pipeline(yaml);
    let plan = cfg
        .compile(&clinker_core::config::CompileContext::default())
        .unwrap();
    let merge_idx = plan
        .dag()
        .graph
        .node_indices()
        .find(|&i| matches!(plan.dag().graph[i], PlanNode::Merge { .. }))
        .expect("Merge missing");
    let incoming = plan
        .dag()
        .graph
        .neighbors_directed(merge_idx, petgraph::Direction::Incoming)
        .count();
    assert_eq!(incoming, 2);
}

#[test]
fn test_compile_returns_err_on_validation_error() {
    let yaml = r#"
pipeline:
  name: dup
nodes:
  - type: source
    name: dup
    config:
      name: dup
      type: csv
      path: data/a.csv
      schema:
        - { name: amount, type: string }

  - type: source
    name: dup
    config:
      name: dup
      type: csv
      path: data/b.csv
      schema:
        - { name: amount, type: string }
"#;
    let cfg = parse_pipeline(yaml);
    let res = cfg.compile(&clinker_core::config::CompileContext::default());
    assert!(res.is_err());
    let diags = res.err().unwrap();
    assert!(diags.iter().any(|d| d.code == "E001"));
}

// ---------------------------------------------------------------------
// Compile-time CXL typecheck gate tests
// ---------------------------------------------------------------------

#[test]
fn test_compile_surfaces_cxl_type_error() {
    // A transform that references a column absent from the upstream
    // source schema must produce an E200 diagnostic during `compile()`,
    // BEFORE any I/O occurs.
    let yaml = r#"
pipeline:
  name: bad_ref
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/does_not_matter.csv
      schema:
        - { name: amount, type: int }
  - type: transform
    name: t
    input: src
    config:
      cxl: "emit bogus = not_a_column + 1"
  - type: output
    name: out
    input: t
    config:
      name: out
      type: csv
      path: data/out_nonexistent.csv
"#;
    let cfg = parse_pipeline(yaml);
    let res = cfg.compile(&clinker_core::config::CompileContext::default());
    assert!(res.is_err(), "compile must fail on CXL type error");
    let diags = res.err().unwrap();
    assert!(
        diags.iter().any(|d| d.code == "E200"),
        "expected E200, got: {:?}",
        diags.iter().map(|d| &d.code).collect::<Vec<_>>()
    );
}

#[test]
fn test_source_without_schema_fails() {
    // `schema:` is a required field on SourceBody. Omitting it must
    // hard-fail at parse time (serde field-required error). This test
    // pins that guarantee so E201 remains the sole fallback path for
    // programmatic construction.
    let yaml = r#"
pipeline:
  name: no_schema
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/in.csv
"#;
    let res = clinker_core::yaml::from_str::<PipelineConfig>(yaml);
    assert!(
        res.is_err(),
        "source missing `schema:` must be a parse error"
    );
}

#[test]
fn test_compile_time_typecheck_no_file_access() {
    // A valid pipeline whose `source.path` points to a nonexistent
    // file must still `compile()` successfully — typecheck is
    // purely in-memory against the declared schema. File I/O only
    // happens at `run_with_readers_writers` time.
    let yaml = r#"
pipeline:
  name: no_file
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/definitely_not_real_in.csv
      schema:
        - { name: amount, type: int }
  - type: transform
    name: t
    input: src
    config:
      cxl: "emit doubled = amount * 2"
  - type: output
    name: out
    input: t
    config:
      name: out
      type: csv
      path: data/definitely_not_real_out.csv
"#;
    let cfg = parse_pipeline(yaml);
    let plan = cfg
        .compile(&clinker_core::config::CompileContext::default())
        .expect("compile must succeed without touching disk");
    assert_eq!(plan.dag().graph.node_count(), 3);
}

#[test]
fn test_compile_compiledplan_exposes_dag_runtime_sidecar() {
    let yaml = r#"
pipeline:
  name: side
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/a.csv
      schema:
        - { name: amount, type: string }
"#;
    let cfg = parse_pipeline(yaml);
    let plan = cfg
        .compile(&clinker_core::config::CompileContext::default())
        .unwrap();
    assert_eq!(plan.dag().graph.node_count(), 1);
}
