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
fn test_dynamic_doc_index_aborts_compile_with_e339() {
    // `$doc.Summary.total[amount]` indexes a `$doc` access by a record
    // field, so the declared path cannot be resolved at compile time.
    // The compile must abort with E340.
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
    assert!(
        err.iter().any(|d| d.code == "E340"),
        "expected an E340 diagnostic, got: {err:?}"
    );
}
