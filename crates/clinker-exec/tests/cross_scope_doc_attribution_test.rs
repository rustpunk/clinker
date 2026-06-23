//! `$doc` path attribution is scope-correct across composition boundaries.
//!
//! A `$doc` access carries no source qualifier, so the planner attributes
//! each referenced envelope path back to the source(s) feeding the
//! referencing node, then validates it against that source's declared
//! envelope schema. Before the attribution was scope-keyed, a body node
//! sharing a name with a top-level node collided in the bare-name
//! attribution tables, so the body's `$doc` paths were validated against the
//! top-level node's source — a source that need not declare the body's
//! section — spuriously failing compile with E341.
//!
//! This pipeline has a top-level transform `shape` reading `$doc.Head.xval`
//! (its source declares only `Head`) and a composition-body transform ALSO
//! named `shape` reading `$doc.Foot.yval` (its call-site source declares
//! only `Foot`). With scope-correct attribution the pipeline compiles and each
//! source carries exactly its own scope's path; with the old bare-name
//! attribution the body's `Foot` access leaked onto the top-level source and
//! tripped E341.

use std::path::PathBuf;

use clinker_plan::config::pipeline_node::PipelineNode;
use clinker_plan::config::{CompileContext, parse_config};

/// Section names of the `$doc` paths the runtime source body named
/// `source_name` carries after compile (by node identity / `header.name`).
fn source_doc_sections(plan: &clinker_plan::plan::CompiledPlan, source_name: &str) -> Vec<String> {
    plan.config()
        .nodes
        .iter()
        .find_map(|spanned| match &spanned.value {
            PipelineNode::Source { header, config } if header.name == source_name => Some(
                config
                    .source
                    .declared_doc_paths
                    .iter()
                    .map(|p| p.section.to_string())
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .unwrap_or_default()
}

#[test]
fn body_doc_paths_attribute_to_their_own_scope_source() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    // Body transform `shape` reads `$doc.Foot.yval`; its call-site source
    // (`s_body`) declares only the `Foot` section.
    std::fs::write(
        comp_dir.join("scoped_doc.comp.yaml"),
        r#"_compose:
  name: scoped_doc
  inputs:
    inp:
      schema:
        - { name: amount, type: int }
  outputs:
    out: shape.out
  config_schema: {}

nodes:
  - type: transform
    name: shape
    input: inp
    config:
      cxl: |
        emit fy = $doc.Foot.yval
"#,
    )
    .expect("write comp");

    std::fs::create_dir_all(workspace.path().join("pipelines")).expect("mkdir pipelines");

    // Top-level `shape` reads `$doc.Head.x`; its source (`s_top`) declares
    // only the `Head` section. Both sources have a Closed envelope schema, so
    // a path validated against the wrong source's schema fires E341.
    let yaml = r#"
pipeline:
  name: cross_scope_doc
nodes:
  - type: source
    name: s_top
    config:
      name: s_top
      type: xml
      glob: ./top/*.xml
      options:
        record_path: doc/records/record
      envelope:
        sections:
          Head:
            extract: { xml_path: "/doc/Head" }
            fields:
              xval: string
      schema:
        - { name: amount, type: int }
  - type: source
    name: s_body
    config:
      name: s_body
      type: xml
      glob: ./body/*.xml
      options:
        record_path: doc/records/record
      envelope:
        sections:
          Foot:
            extract: { xml_path: "/doc/Foot" }
            fields:
              yval: string
      schema:
        - { name: amount, type: int }
  - type: transform
    name: shape
    input: s_top
    config:
      cxl: |
        emit hx = $doc.Head.xval
  - type: composition
    name: body
    input: s_body
    use: ../compositions/scoped_doc.comp.yaml
    inputs:
      inp: s_body
  - type: output
    name: out_top
    input: shape
    config:
      name: out_top
      type: csv
      path: out_top.csv
      include_unmapped: true
  - type: output
    name: out_body
    input: body
    config:
      name: out_body
      type: csv
      path: out_body.csv
      include_unmapped: true
"#;

    let config = parse_config(yaml).expect("parse pipeline yaml");
    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    // Compiling at all is the headline assertion: with bare-name attribution
    // the body `shape`'s `$doc.Foot.yval` was validated against `s_top` (Closed,
    // no `Foot`) and the compile failed with E341.
    let plan = config
        .compile(&ctx)
        .expect("pipeline must compile: body $doc paths must not leak onto the top-level source");

    let top_sections = source_doc_sections(&plan, "s_top");
    let body_sections = source_doc_sections(&plan, "s_body");

    assert!(
        top_sections == vec!["Head".to_string()],
        "s_top must carry ONLY its own `Head` path, not the body's `Foot`; got {top_sections:?}"
    );
    assert!(
        body_sections == vec!["Foot".to_string()],
        "s_body must carry ONLY the body `shape`'s `Foot` path, not the top-level `Head`; got {body_sections:?}"
    );
}
