//! End-to-end surfacing of the compile-time `$doc` path set.
//!
//! `cxl::analyzer::doc_paths` collects every `$doc.<section>.<field>` the
//! pipeline's programs reference; `PipelineConfig::compile` stamps that
//! set onto each lowered `Source` node's `SourceConfig`. These tests pin
//! the plan-build wiring: the path set reaches the Source, and a `$doc`
//! access indexed by a non-literal aborts the compile with E340.

use cxl::analyzer::doc_paths::{DocIndex, DocPath};

use crate::config::{CompileContext, parse_config};
use crate::plan::execution::PlanNode;

/// Compile a document-aware pipeline whose two transforms read distinct
/// `$doc` paths, and return the path set stamped on the single Source.
fn compile_and_read_source_paths(transforms_cxl: &[&str]) -> Vec<DocPath> {
    let mut yaml = String::from(
        r#"
pipeline:
  name: doc_paths_demo
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: xml
      glob: ./*.xml
      options:
        record_path: doc/records/record
      envelope:
        sections:
          BatchInfo:
            extract: { xml_path: "/doc/BatchInfo" }
            fields:
              batch_id: string
          Summary:
            extract: { xml_path: "/doc/Summary" }
            fields:
              total: int
      schema:
        - { name: amount, type: int }
"#,
    );
    let mut prev = "payments".to_string();
    for (i, cxl) in transforms_cxl.iter().enumerate() {
        let name = format!("t{i}");
        yaml.push_str(&format!(
            "  - type: transform\n    name: {name}\n    input: {prev}\n    config:\n      cxl: |\n",
        ));
        for line in cxl.lines() {
            yaml.push_str(&format!("        {line}\n"));
        }
        prev = name;
    }
    yaml.push_str(&format!(
        "  - type: output\n    name: out\n    input: {prev}\n    config:\n      name: out\n      type: csv\n      path: out.csv\n",
    ));

    let config = parse_config(&yaml).expect("parse doc-paths pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile doc-paths pipeline");

    let dag = plan.dag();
    let source = dag
        .graph
        .node_indices()
        .find_map(|idx| match &dag.graph[idx] {
            PlanNode::Source { resolved, .. } => resolved.as_ref(),
            _ => None,
        })
        .expect("source node present with resolved payload");
    source.source.declared_doc_paths.clone()
}

fn path(section: &str, field: &str, indices: Vec<DocIndex>) -> DocPath {
    DocPath {
        section: section.into(),
        field: field.into(),
        indices,
    }
}

#[test]
fn test_doc_paths_surface_onto_source() {
    let paths = compile_and_read_source_paths(&[
        "emit amount = amount\nemit batch = $doc.BatchInfo.batch_id",
        "emit amount = amount\nemit total = $doc.Summary.total",
    ]);
    // Sorted by (section, field): BatchInfo precedes Summary.
    assert_eq!(
        paths,
        vec![
            path("BatchInfo", "batch_id", vec![]),
            path("Summary", "total", vec![]),
        ]
    );
}

#[test]
fn test_doc_path_used_only_in_conditional_still_surfaces() {
    // The `$doc` access lives only inside an `if` branch — the analyzer
    // must still collect it, so the Source must still carry it.
    let paths = compile_and_read_source_paths(&[
        "emit amount = amount\nemit batch = if amount > 0 then $doc.BatchInfo.batch_id else \"none\"",
    ]);
    assert_eq!(paths, vec![path("BatchInfo", "batch_id", vec![])]);
}

#[test]
fn test_pipeline_with_no_doc_access_has_empty_path_set() {
    let paths = compile_and_read_source_paths(&["emit amount = amount + 1"]);
    assert!(paths.is_empty());
}

#[test]
fn test_dynamic_doc_index_aborts_compile_with_e340_at_node_span() {
    // `$doc.Summary.total[amount]` indexes a `$doc` access by a record
    // field, so the declared path cannot be resolved at compile time.
    // The compile must abort with E340, and the diagnostic must anchor
    // at the offending node's source (a non-synthetic span) and carry
    // the help text — not a bare synthetic span with the message alone.
    let yaml = r#"
pipeline:
  name: dynamic_doc_index
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: xml
      glob: ./*.xml
      options:
        record_path: doc/records/record
      envelope:
        sections:
          Summary:
            extract: { xml_path: "/doc/Summary" }
            fields:
              total: int
      schema:
        - { name: amount, type: int }
  - type: transform
    name: t0
    input: payments
    config:
      cxl: |
        emit amount = amount
        emit picked = $doc.Summary.total[amount]
  - type: output
    name: out
    input: t0
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).expect("parse dynamic-index pipeline");
    let result = config.compile(&CompileContext::default());
    let err = result.expect_err("dynamic `$doc` index must fail to compile");
    let diag = err
        .iter()
        .find(|d| d.code == "E340")
        .unwrap_or_else(|| panic!("expected an E340 diagnostic, got: {err:?}"));
    // The primary span points into the source (a real line), not the
    // pre-interning synthetic sentinel.
    assert_ne!(
        diag.primary.span,
        clinker_core_types::span::Span::SYNTHETIC,
        "E340 primary span must not be the synthetic sentinel"
    );
    assert!(
        diag.primary.span.synthetic_line_number().is_some(),
        "E340 primary span must carry the offending node's source line"
    );
    assert!(diag.help.is_some(), "E340 must carry help text");
}

#[test]
fn test_negative_literal_doc_index_compiles() {
    // A negated integer literal is a static from-end index, so the path
    // is resolvable and the compile succeeds.
    let paths = compile_and_read_source_paths(&["emit amount = amount
emit last = $doc.Summary.total[-1]"]);
    assert_eq!(
        paths,
        vec![path("Summary", "total", vec![DocIndex::Int(-1)])]
    );
}

#[test]
fn test_doc_path_in_combine_predicate_is_collected() {
    // A `$doc` access inside a Combine `where:` predicate must reach the
    // declared path set — Combine programs are not in the analyzer's
    // `entries` subset, so this exercises the all-programs walk.
    let yaml = r#"
pipeline:
  name: combine_doc
nodes:
  - type: source
    name: left
    config:
      name: left
      type: xml
      glob: ./l/*.xml
      options:
        record_path: doc/records/record
      envelope:
        sections:
          Head:
            extract: { xml_path: "/doc/Head" }
            fields:
              cutoff: int
      schema:
        - { name: id, type: int }
        - { name: amount, type: int }
  - type: source
    name: right
    config:
      name: right
      type: csv
      path: right.csv
      schema:
        - { name: id, type: int }
        - { name: label, type: string }
  - type: combine
    name: joined
    input:
      l: left
      r: right
    config:
      where: "l.id == r.id and l.amount > $doc.Head.cutoff"
      match: first
      on_miss: skip
      cxl: |
        emit id = l.id
        emit label = r.label
      propagate_ck: driver
  - type: output
    name: out
    input: joined
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).expect("parse combine-doc pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile combine-doc pipeline");
    let dag = plan.dag();
    // Both sources carry the pipeline-wide set (the known
    // over-attachment); assert the Combine-predicate path reached it.
    let any_source_has_path = dag.graph.node_indices().any(|idx| {
        matches!(&dag.graph[idx], PlanNode::Source { resolved: Some(r), .. }
            if r.source.declared_doc_paths.contains(&path("Head", "cutoff", vec![])))
    });
    assert!(
        any_source_has_path,
        "`$doc.Head.cutoff` used in the Combine predicate must reach the declared set"
    );
}

#[test]
fn test_doc_path_in_composition_body_is_collected() {
    // A `$doc` access inside a `.comp.yaml` body transform must reach the
    // declared path set. Body programs live in `artifacts.composition_*`
    // / the shared typed map, separate from the top-level `entries`
    // subset, so this exercises the all-programs walk across the
    // composition boundary. The envelope is declared on the PARENT source
    // (the document context flows into the body via the input port).
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("doc_body.comp.yaml"),
        r#"_compose:
  name: doc_body
  inputs:
    inp:
      schema:
        - { name: amount, type: int }
  outputs:
    out: tagged
  config_schema: {}

nodes:
  - type: transform
    name: tagged
    input: inp
    config:
      cxl: |
        emit amount = amount
        emit cutoff = $doc.Head.cutoff
"#,
    )
    .expect("write comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: composition_doc
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: xml
      glob: ./*.xml
      options:
        record_path: doc/records/record
      envelope:
        sections:
          Head:
            extract: { xml_path: "/doc/Head" }
            fields:
              cutoff: int
      schema:
        - { name: amount, type: int }
  - type: composition
    name: body
    input: payments
    use: ../compositions/doc_body.comp.yaml
    inputs:
      inp: payments
  - type: output
    name: out
    input: body
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let config = parse_config(yaml).expect("parse composition-doc pipeline");
    let ctx = crate::config::CompileContext::with_pipeline_dir(
        workspace.path(),
        std::path::PathBuf::from("pipelines"),
    );
    let compiled = config
        .compile(&ctx)
        .expect("compile composition-doc pipeline");
    let dag = compiled.dag();
    let source = dag
        .graph
        .node_indices()
        .find_map(|idx| match &dag.graph[idx] {
            PlanNode::Source { resolved, .. } => resolved.as_ref(),
            _ => None,
        })
        .expect("parent source present");
    assert!(
        source
            .source
            .declared_doc_paths
            .contains(&path("Head", "cutoff", vec![])),
        "`$doc.Head.cutoff` used in the composition body must reach the declared set, got {:?}",
        source.source.declared_doc_paths
    );
}
