//! End-to-end surfacing of the compile-time `$doc` path set.
//!
//! `cxl::analyzer::doc_paths` collects every `$doc.<section>.<field>` the
//! pipeline's programs reference, attributed to the referencing node;
//! `PipelineConfig::compile` traces each path back through the DAG to its
//! source(s) and stamps only those sources' `SourceConfig`. These tests
//! pin the plan-build wiring: each path reaches exactly the source(s)
//! feeding it (never the pipeline-wide union), and a `$doc` access
//! indexed by a non-literal aborts the compile with E340.

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

/// Compile a single-transform XML pipeline whose source declares a
/// `Summary { total: int }` envelope section, with the transform body
/// supplied by the caller. Returns the compile result so a test can assert
/// success or inspect the diagnostics.
fn compile_with_summary_envelope(
    transform_cxl: &str,
) -> Result<crate::plan::CompiledPlan, Vec<clinker_core_types::Diagnostic>> {
    let mut yaml = String::from(
        r#"
pipeline:
  name: doc_validate_demo
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
"#,
    );
    for line in transform_cxl.lines() {
        yaml.push_str(&format!("        {line}\n"));
    }
    yaml.push_str(
        "  - type: output\n    name: out\n    input: t0\n    config:\n      name: out\n      type: csv\n      path: out.csv\n",
    );
    let config = parse_config(&yaml).expect("parse doc-validate pipeline");
    config.compile(&CompileContext::default())
}

#[test]
fn test_undeclared_doc_section_aborts_compile_with_e341() {
    // `$doc.Summry.total` misspells the declared `Summary` section, so the
    // path names a section the XML source does not declare. The compile
    // must abort with E341 anchored at the referencing transform.
    let err = compile_with_summary_envelope("emit amount = amount\nemit t = $doc.Summry.total")
        .expect_err("an undeclared `$doc` section must fail to compile");
    let diag = err
        .iter()
        .find(|d| d.code == "E341")
        .unwrap_or_else(|| panic!("expected an E341 diagnostic, got: {err:?}"));
    assert!(
        diag.message.contains("Summry"),
        "E341 message must name the undeclared section: {}",
        diag.message
    );
    assert!(
        diag.primary.span.synthetic_line_number().is_some(),
        "E341 primary span must carry the referencing node's source line"
    );
    assert!(diag.help.is_some(), "E341 must carry help text");
}

#[test]
fn test_undeclared_doc_field_aborts_compile_with_e341() {
    // `$doc.Summary.totl` names the declared `Summary` section but a field
    // the section does not declare — a distinct typo from a bad section.
    let err = compile_with_summary_envelope("emit amount = amount\nemit t = $doc.Summary.totl")
        .expect_err("an undeclared `$doc` field must fail to compile");
    let diag = err
        .iter()
        .find(|d| d.code == "E341")
        .unwrap_or_else(|| panic!("expected an E341 diagnostic, got: {err:?}"));
    assert!(
        diag.message.contains("totl") && diag.message.contains("Summary"),
        "E341 message must name the undeclared field and its section: {}",
        diag.message
    );
}

#[test]
fn test_declared_doc_path_does_not_trip_e341() {
    // The fully-declared `$doc.Summary.total` must compile without E341.
    let plan = compile_with_summary_envelope("emit amount = amount\nemit t = $doc.Summary.total")
        .expect("a declared `$doc` path must compile");
    // Sanity: the path still surfaces onto the source's declared set.
    let dag = plan.dag();
    let source = dag
        .graph
        .node_indices()
        .find_map(|idx| match &dag.graph[idx] {
            PlanNode::Source { resolved, .. } => resolved.as_ref(),
            _ => None,
        })
        .expect("source node present");
    assert!(
        source
            .source
            .declared_doc_paths
            .contains(&path("Summary", "total", vec![])),
    );
}

#[test]
fn test_undeclared_doc_path_against_segment_format_is_not_validated() {
    // X12 (and the other segment/positional formats) synthesize envelope
    // levels beyond the declared config — `$doc.functional_group.*` and
    // `$doc.transaction_set.*` are reader-derived, not config-declared. The
    // closed-schema E341 check must NOT fire for these, or every multi-level
    // EDI pipeline would be rejected.
    let yaml = r#"
pipeline:
  name: x12_doc_open
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: x12
      path: po.x12
      envelope:
        sections:
          interchange:
            extract: { segment: "ISA" }
            fields:
              e13: string
      schema:
        - { name: seg_id, type: string }
  - type: transform
    name: t0
    input: interchange
    config:
      cxl: |
        emit seg = seg_id
        emit gs06 = $doc.functional_group.e06
        emit st02 = $doc.transaction_set.e02
  - type: output
    name: out
    input: t0
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).expect("parse x12 pipeline");
    let result = config.compile(&CompileContext::default());
    if let Err(err) = &result {
        assert!(
            err.iter().all(|d| d.code != "E341"),
            "segment-format `$doc` paths must not trip E341, got: {err:?}"
        );
    }
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
    let head_cutoff = path("Head", "cutoff", vec![]);
    // A joined record carries the DRIVING input's document context, so a
    // `$doc` access in the Combine predicate is attributed to the driver
    // source only. `left` is declared first (the planner's declaration-
    // order driver default) and is the source whose envelope declares
    // `Head`. The probe-side `right` source — which has no envelope —
    // must NOT be told to extract it.
    let paths_for = |source_name: &str| -> Vec<DocPath> {
        dag.graph
            .node_indices()
            .find_map(|idx| match &dag.graph[idx] {
                PlanNode::Source {
                    name,
                    resolved: Some(r),
                    ..
                } if name == source_name => Some(r.source.declared_doc_paths.clone()),
                _ => None,
            })
            .unwrap_or_default()
    };
    assert!(
        paths_for("left").contains(&head_cutoff),
        "the driving source `left` must carry the Combine-predicate `$doc` path"
    );
    assert!(
        !paths_for("right").contains(&head_cutoff),
        "the probe source `right` must NOT carry the driver's `$doc` path"
    );
}

#[test]
fn test_doc_path_in_route_branch_condition_is_collected() {
    // A `$doc` access referenced ONLY in a Route branch condition must
    // reach the declared path set. A Route's node-keyed typed program is
    // an empty body and its branch conditions compile straight to runtime
    // evaluators, so this exercises the route-branch side-table that feeds
    // the all-programs walk. The path attributes to the Route's upstream
    // source (the Route inherits its input's source set).
    let yaml = r#"
pipeline:
  name: route_doc
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
  - type: route
    name: split
    input: payments
    config:
      mode: exclusive
      conditions:
        big: "amount > $doc.Head.cutoff"
      default: small
  - type: output
    name: big_out
    input: split.big
    config:
      name: big_out
      type: csv
      path: big.csv
  - type: output
    name: small_out
    input: split.small
    config:
      name: small_out
      type: csv
      path: small.csv
"#;
    let config = parse_config(yaml).expect("parse route-doc pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile route-doc pipeline");
    let dag = plan.dag();
    let source = dag
        .graph
        .node_indices()
        .find_map(|idx| match &dag.graph[idx] {
            PlanNode::Source { resolved, .. } => resolved.as_ref(),
            _ => None,
        })
        .expect("source node present with resolved payload");
    assert!(
        source
            .source
            .declared_doc_paths
            .contains(&path("Head", "cutoff", vec![])),
        "`$doc.Head.cutoff` used only in a route branch condition must reach the \
         declared set, got {:?}",
        source.source.declared_doc_paths
    );
}

#[test]
fn test_multi_source_paths_are_attributed_per_source() {
    // Two independent single-source chains read disjoint `$doc` paths.
    // Each source must carry ONLY the path its own downstream program
    // reads — never the pipeline-wide union — so a multi-source run does
    // not tell a source to extract another source's envelope section.
    let yaml = r#"
pipeline:
  name: multi_source_doc
nodes:
  - type: source
    name: alpha
    config:
      name: alpha
      type: xml
      glob: ./a/*.xml
      options:
        record_path: doc/records/record
      envelope:
        sections:
          AHead:
            extract: { xml_path: "/doc/AHead" }
            fields:
              a_batch: string
      schema:
        - { name: amount, type: int }
  - type: source
    name: beta
    config:
      name: beta
      type: xml
      glob: ./b/*.xml
      options:
        record_path: doc/records/record
      envelope:
        sections:
          BHead:
            extract: { xml_path: "/doc/BHead" }
            fields:
              b_batch: string
      schema:
        - { name: amount, type: int }
  - type: transform
    name: tag_alpha
    input: alpha
    config:
      cxl: |
        emit amount = amount
        emit a = $doc.AHead.a_batch
  - type: transform
    name: tag_beta
    input: beta
    config:
      cxl: |
        emit amount = amount
        emit b = $doc.BHead.b_batch
  - type: merge
    name: merged
    inputs: [tag_alpha, tag_beta]
  - type: output
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).expect("parse multi-source pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile multi-source pipeline");
    let dag = plan.dag();
    let paths_for = |source_name: &str| -> Vec<DocPath> {
        dag.graph
            .node_indices()
            .find_map(|idx| match &dag.graph[idx] {
                PlanNode::Source {
                    name,
                    resolved: Some(r),
                    ..
                } if name == source_name => Some(r.source.declared_doc_paths.clone()),
                _ => None,
            })
            .unwrap_or_default()
    };
    // `alpha` carries only its own section; `beta` only its own.
    assert_eq!(paths_for("alpha"), vec![path("AHead", "a_batch", vec![])]);
    assert_eq!(paths_for("beta"), vec![path("BHead", "b_batch", vec![])]);
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

#[test]
fn test_multi_port_composition_forwards_all_port_sources() {
    // A composition with TWO bound input ports merges them and surfaces one
    // output. A downstream node reading `$doc` off the composition output
    // could see a record from EITHER port's source, so the path must be
    // attributed to BOTH port sources — not just the primary `input:` port.
    // The composition node forwards only `header.input` (the primary port)
    // through the generic DAG-edge view, so this pins that the source
    // attribution unions every `inputs:` port instead.
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("two_port.comp.yaml"),
        r#"_compose:
  name: two_port
  inputs:
    primary:
      schema:
        - { name: amount, type: int }
    reference:
      schema:
        - { name: amount, type: int }
  outputs:
    out: merged
  config_schema: {}

nodes:
  - type: merge
    name: merged
    inputs: [primary, reference]
"#,
    )
    .expect("write comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    // Both sources declare the SAME envelope section so a `$doc.Shared.tag`
    // read downstream of the merge typechecks regardless of which port's
    // record is in hand.
    let yaml = r#"
pipeline:
  name: multi_port_doc
nodes:
  - type: source
    name: customers
    config:
      name: customers
      type: xml
      glob: ./c/*.xml
      options:
        record_path: doc/records/record
      envelope:
        sections:
          Shared:
            extract: { xml_path: "/doc/Shared" }
            fields:
              tag: string
      schema:
        - { name: amount, type: int }
  - type: source
    name: zip_lookup
    config:
      name: zip_lookup
      type: xml
      glob: ./z/*.xml
      options:
        record_path: doc/records/record
      envelope:
        sections:
          Shared:
            extract: { xml_path: "/doc/Shared" }
            fields:
              tag: string
      schema:
        - { name: amount, type: int }
  - type: composition
    name: combined
    input: customers
    use: ../compositions/two_port.comp.yaml
    inputs:
      primary: customers
      reference: zip_lookup
  - type: transform
    name: tag
    input: combined
    config:
      cxl: |
        emit amount = amount
        emit tag = $doc.Shared.tag
  - type: output
    name: out
    input: tag
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).expect("parse multi-port pipeline");
    let ctx = crate::config::CompileContext::with_pipeline_dir(
        workspace.path(),
        std::path::PathBuf::from("pipelines"),
    );
    let compiled = config.compile(&ctx).expect("compile multi-port pipeline");
    let dag = compiled.dag();
    let paths_for = |source_name: &str| -> Vec<DocPath> {
        dag.graph
            .node_indices()
            .find_map(|idx| match &dag.graph[idx] {
                PlanNode::Source {
                    name,
                    resolved: Some(r),
                    ..
                } if name == source_name => Some(r.source.declared_doc_paths.clone()),
                _ => None,
            })
            .unwrap_or_default()
    };
    let shared_tag = path("Shared", "tag", vec![]);
    // The downstream `$doc` read must reach BOTH the primary (`customers`)
    // and the non-primary (`zip_lookup`) port sources.
    assert!(
        paths_for("customers").contains(&shared_tag),
        "primary port source `customers` must carry the downstream `$doc` path"
    );
    assert!(
        paths_for("zip_lookup").contains(&shared_tag),
        "non-primary port source `zip_lookup` must carry the downstream `$doc` path, got {:?}",
        paths_for("zip_lookup")
    );
}
