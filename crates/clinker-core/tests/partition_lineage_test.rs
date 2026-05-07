//! Verifies that multi-file source matchers propagate `FilePartitioned`
//! lineage through the DAG and that `Merge`/`Combine` clear it with the
//! correct provenance variants. The runtime per-record `$source.file` Arc
//! tagging that consumes this lineage lands in §6; this test pins the
//! plan-time data structure so §6 can build on it without re-deriving.

use clinker_core::config::{CompileContext, parse_config};
use clinker_core::plan::compiled::CompiledPlan;
use clinker_core::plan::execution::ExecutionPlanDag;
use clinker_core::plan::properties::{NodeProperties, PartitioningKind, PartitioningProvenance};

fn compile(yaml: &str) -> CompiledPlan {
    let config = parse_config(yaml).expect("parse");
    config.compile(&CompileContext::default()).expect("compile")
}

fn props_for_node<'a>(plan: &'a ExecutionPlanDag, name: &str) -> &'a NodeProperties {
    let idx = plan
        .graph
        .node_indices()
        .find(|i| plan.graph[*i].name() == name)
        .unwrap_or_else(|| panic!("no node named {name:?}"));
    plan.node_properties
        .get(&idx)
        .unwrap_or_else(|| panic!("no props for node {name:?}"))
}

#[test]
fn literal_path_keeps_single_partitioning() {
    let yaml = r#"
pipeline:
  name: t
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/orders.csv
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let cfg = compile(yaml);
    let plan = cfg.dag();
    let src_props = props_for_node(plan, "src");
    assert!(
        matches!(src_props.partitioning.kind, PartitioningKind::Single),
        "literal path should yield Single partitioning, got {:?}",
        src_props.partitioning.kind
    );
}

#[test]
fn glob_matcher_introduces_file_partitioned() {
    let yaml = r#"
pipeline:
  name: t
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      glob: data/orders_*.csv
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let cfg = compile(yaml);
    let plan = cfg.dag();
    let src_props = props_for_node(plan, "src");
    match &src_props.partitioning.kind {
        PartitioningKind::FilePartitioned { keys } => {
            assert_eq!(keys, &vec!["$source.file".to_string()]);
        }
        other => panic!("expected FilePartitioned, got {other:?}"),
    }
    match &src_props.partitioning.provenance {
        PartitioningProvenance::IntroducedByMultiFileSource { source_name } => {
            assert_eq!(source_name, "src");
        }
        other => panic!("expected IntroducedByMultiFileSource, got {other:?}"),
    }
}

#[test]
fn regex_matcher_introduces_file_partitioned() {
    let yaml = r#"
pipeline:
  name: t
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      regex: '^.*\.csv$'
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let cfg = compile(yaml);
    let plan = cfg.dag();
    assert!(matches!(
        props_for_node(plan, "src").partitioning.kind,
        PartitioningKind::FilePartitioned { .. }
    ));
}

#[test]
fn paths_list_introduces_file_partitioned() {
    let yaml = r#"
pipeline:
  name: t
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      paths:
        - data/jan.csv
        - data/feb.csv
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let cfg = compile(yaml);
    let plan = cfg.dag();
    assert!(matches!(
        props_for_node(plan, "src").partitioning.kind,
        PartitioningKind::FilePartitioned { .. }
    ));
}

#[test]
fn merge_destroys_file_partitioning() {
    let yaml = r#"
pipeline:
  name: t
nodes:
  - type: source
    name: a
    config:
      name: a
      type: csv
      glob: data/a_*.csv
      schema:
        - { name: id, type: int }
  - type: source
    name: b
    config:
      name: b
      type: csv
      path: data/b.csv
      schema:
        - { name: id, type: int }
  - type: merge
    name: m
    inputs: [a, b]
  - type: output
    name: out
    input: m
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let cfg = compile(yaml);
    let plan = cfg.dag();
    let m = props_for_node(plan, "m");
    assert!(
        matches!(m.partitioning.kind, PartitioningKind::Single),
        "merge should drop FilePartitioned to Single, got {:?}",
        m.partitioning.kind
    );
    match &m.partitioning.provenance {
        PartitioningProvenance::DestroyedByMerge { at_node } => {
            assert_eq!(at_node, "m");
        }
        other => panic!("expected DestroyedByMerge, got {other:?}"),
    }
}

#[test]
fn output_inherits_file_partitioning() {
    let yaml = r#"
pipeline:
  name: t
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      glob: data/*.csv
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let cfg = compile(yaml);
    let plan = cfg.dag();
    let out_props = props_for_node(plan, "out");
    assert!(
        matches!(
            out_props.partitioning.kind,
            PartitioningKind::FilePartitioned { .. }
        ),
        "output should inherit FilePartitioned through preserve, got {:?}",
        out_props.partitioning.kind
    );
}
