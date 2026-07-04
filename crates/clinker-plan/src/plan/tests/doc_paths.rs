//! End-to-end surfacing of the compile-time `$doc` path set.
//!
//! `cxl::analyzer::doc_paths` collects every `$doc.<section>.<field>` the
//! pipeline's programs reference, attributed to the referencing node;
//! `PipelineConfig::compile` traces each path back through the DAG to its
//! source(s) and stamps only those sources' `SourceConfig`. These tests
//! pin the plan-build wiring: each path reaches exactly the source(s)
//! feeding it (never the pipeline-wide union), and a `$doc` access
//! indexed by a non-literal aborts the compile with E340.

use clinker_core_types::Diagnostic;
use cxl::analyzer::doc_paths::{DocIndex, DocPath};

use crate::config::{CompileContext, parse_config};

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

    // Read the live stamp off the runtime `PipelineConfig` source body — the
    // set the executor's ingest path actually reads (the DAG payload does not
    // carry it).
    source_doc_paths(&plan, "payments")
}

/// The `$doc` path set stamped on the runtime source body named `source_name`
/// (by node identity / `header.name`), as the executor's ingest path reads
/// it. Returns an empty vec when no such source carries paths.
fn source_doc_paths(plan: &crate::plan::CompiledPlan, source_name: &str) -> Vec<DocPath> {
    use crate::config::pipeline_node::PipelineNode;
    plan.config()
        .nodes
        .iter()
        .find_map(|spanned| match &spanned.value {
            PipelineNode::Source { header, config } if header.name == source_name => {
                Some(config.source.declared_doc_paths.clone())
            }
            _ => None,
        })
        .unwrap_or_default()
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
fn test_doc_paths_surface_when_header_name_differs_from_config_name() {
    // A Source whose node identity (`header.name`) differs from its nested
    // `config.name:` is a shape the compiler accepts. The `$doc` attribution
    // map is keyed by node identity, so the runtime stamp must look up by
    // `header.name` and still land the path set on that node's source body —
    // otherwise the pre-scan prunes every section and `$doc.*` resolves
    // empty at run time.
    let yaml = r#"
pipeline:
  name: divergent_name_doc
nodes:
  - type: source
    name: node_identity
    config:
      name: config_name
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
    input: node_identity
    config:
      cxl: |
        emit amount = amount
        emit total = $doc.Summary.total
  - type: output
    name: out
    input: t0
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).expect("parse divergent-name pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile divergent-name pipeline");
    // The stamp keys on node identity (`node_identity`), not the inner
    // `config.name:` (`config_name`).
    let paths = source_doc_paths(&plan, "node_identity");
    assert_eq!(
        paths,
        vec![path("Summary", "total", vec![])],
        "the `$doc` path must reach the source body keyed by node identity"
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
    assert!(source_doc_paths(&plan, "payments").contains(&path("Summary", "total", vec![])));
}

/// Compile an X12 pipeline whose source declares an `ISA` interchange
/// envelope section (`e13: string`) and a transform body supplied by the
/// caller. Returns the compile result so a test can assert success or
/// inspect diagnostics. The X12 reader synthesizes `functional_group` and
/// `transaction_set` levels keyed positionally beyond the declared header.
fn compile_x12(transform_cxl: &str) -> Result<crate::plan::CompiledPlan, Vec<Diagnostic>> {
    let mut yaml = String::from(
        r#"
pipeline:
  name: x12_doc_validate
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
"#,
    );
    for line in transform_cxl.lines() {
        yaml.push_str(&format!("        {line}\n"));
    }
    yaml.push_str(
        "  - type: output\n    name: out\n    input: t0\n    config:\n      name: out\n      type: csv\n      path: out.csv\n",
    );
    let config = parse_config(&yaml).expect("parse x12 pipeline");
    config.compile(&CompileContext::default())
}

#[test]
fn test_segment_format_legitimate_wire_paths_compile() {
    // X12 synthesizes `functional_group` / `transaction_set` levels keyed
    // by positional `eNN` elements beyond the declared `ISA` header. A
    // reference to a synthesized section with an in-range positional
    // element is a legitimate wire path and must NOT trip any `$doc`
    // diagnostic — a false positive here would reject every multi-level
    // EDI pipeline.
    let plan = compile_x12(
        "emit seg = seg_id\n\
         emit isa13 = $doc.interchange.e13\n\
         emit gs06 = $doc.functional_group.e06\n\
         emit st02 = $doc.transaction_set.e02",
    )
    .expect("legitimate X12 wire paths must compile");
    // Sanity: the declared-header path still surfaces onto the source set.
    assert!(source_doc_paths(&plan, "interchange").contains(&path("interchange", "e13", vec![])));
}

#[test]
fn test_segment_format_misspelled_section_aborts_with_e348() {
    // `functonal_group` misspells the synthesized `functional_group`
    // section. A segment-format `$doc` typo must now be caught with E348
    // (the contract changed from "not validated" — this is #481).
    let err = compile_x12("emit seg = seg_id\nemit gs06 = $doc.functonal_group.e06")
        .expect_err("a misspelled synthesized section must fail to compile");
    let diag = err
        .iter()
        .find(|d| d.code == "E348")
        .unwrap_or_else(|| panic!("expected an E348 diagnostic, got: {err:?}"));
    assert!(
        diag.message.contains("functonal_group"),
        "E348 message must name the undeclared section: {}",
        diag.message
    );
    assert!(
        diag.primary.span.synthetic_line_number().is_some(),
        "E348 primary span must carry the referencing node's source line"
    );
    assert!(diag.help.is_some(), "E348 must carry help text");
}

#[test]
fn test_segment_format_out_of_range_positional_element_aborts_with_e348() {
    // `e99` is past the X12 default `max_elements` of 32, so the reader can
    // never serve it — a typo that resolves null at run time. E348 catches
    // the positional-bound case.
    let err = compile_x12("emit seg = seg_id\nemit st = $doc.transaction_set.e99")
        .expect_err("an out-of-range positional element must fail to compile");
    let diag = err
        .iter()
        .find(|d| d.code == "E348")
        .unwrap_or_else(|| panic!("expected an E348 diagnostic, got: {err:?}"));
    assert!(
        diag.message.contains("e99") && diag.message.contains("32"),
        "E348 message must name the offending element and the bound: {}",
        diag.message
    );
}

#[test]
fn test_segment_format_in_range_positional_element_compiles() {
    // `e32` is exactly at the X12 default `max_elements` of 32 — the body
    // segment's element ceiling, so it must compile (boundary check).
    //
    // NOTE: this exercises the LOOSE body-segment bound, not a tight
    // per-segment guarantee. A real `ST` transaction-set header carries
    // only ~3 elements, so `$doc.transaction_set.e32` would resolve null at
    // run time — yet it compiles, because the precise per-segment header
    // arity is not known at plan time (and the reader itself caps only the
    // body element count). Tightening to per-segment arity is tracked at
    // https://github.com/rustpunk/clinker/issues/558.
    compile_x12("emit seg = seg_id\nemit st = $doc.transaction_set.e32")
        .expect("the boundary positional element must compile");
}

#[test]
fn test_segment_format_unpadded_positional_element_aborts_with_e348() {
    // The reader keys columns zero-padded (`e07`), and a `$doc` access
    // resolves by the literal identifier. `$doc.transaction_set.e7` would
    // look up a non-existent `e7` column and resolve null — a near-miss
    // that must be caught, not accepted as in-range. The canonical `e07`
    // compiles; the unpadded `e7` trips E348.
    compile_x12("emit seg = seg_id\nemit st = $doc.transaction_set.e07")
        .expect("the canonical zero-padded element must compile");
    let err = compile_x12("emit seg = seg_id\nemit st = $doc.transaction_set.e7")
        .expect_err("an unpadded positional element must fail to compile");
    assert!(
        err.iter().any(|d| d.code == "E348"),
        "expected an E348 diagnostic for the unpadded element, got: {err:?}"
    );
}

#[test]
fn test_segment_format_non_positional_field_aborts_with_e348() {
    // A non-`eNN` field name on a synthesized positional section names no
    // declared field and no positional element — a typo. E348 catches it.
    let err = compile_x12("emit seg = seg_id\nemit st = $doc.transaction_set.amount")
        .expect_err("a non-positional field on a positional section must fail");
    assert!(
        err.iter().any(|d| d.code == "E348"),
        "expected an E348 diagnostic, got: {err:?}"
    );
}

#[test]
fn test_segment_format_declared_header_section_is_closed() {
    // The declared file-level `ISA` interchange section is CLOSED — the
    // reader serves only its declared fields (`e13: string`), coercing
    // nothing else. So an undeclared field is a typo even when it LOOKS
    // like an in-range positional element (`e14`): the reader never serves
    // it, and it would resolve null. Both `e14` (in-range positional but
    // undeclared) and `zzz` (neither) must trip E348 — a false negative
    // here would let a null-resolving path compile.
    for field in ["e14", "zzz"] {
        let cxl = format!("emit seg = seg_id\nemit isa = $doc.interchange.{field}");
        let err = compile_x12(&cxl)
            .err()
            .unwrap_or_else(|| panic!("`$doc.interchange.{field}` should fail to compile"));
        assert!(
            err.iter().any(|d| d.code == "E348"),
            "an undeclared field on the closed ISA section must trip E348 ({field}): {err:?}"
        );
    }
}

#[test]
fn test_hl7_synthesized_sections_and_positional_bound() {
    // HL7 synthesizes `batch` / `transaction_set` keyed by `fNN` fields
    // bounded by `max_fields` (default 64). A legitimate `fNN` compiles; a
    // misspelled section trips E348.
    fn compile_hl7(transform_cxl: &str) -> Result<crate::plan::CompiledPlan, Vec<Diagnostic>> {
        let mut yaml = String::from(
            r#"
pipeline:
  name: hl7_doc_validate
nodes:
  - type: source
    name: msgs
    config:
      name: msgs
      type: hl7
      path: msgs.hl7
      schema:
        - { name: seg_id, type: string }
  - type: transform
    name: t0
    input: msgs
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
        let config = parse_config(&yaml).expect("parse hl7 pipeline");
        config.compile(&CompileContext::default())
    }

    compile_hl7("emit seg = seg_id\nemit msh = $doc.transaction_set.f09")
        .expect("a legitimate HL7 fNN path must compile");

    let err = compile_hl7("emit seg = seg_id\nemit b = $doc.batchh.f01")
        .expect_err("a misspelled HL7 section must fail");
    assert!(
        err.iter().any(|d| d.code == "E348"),
        "expected an E348 diagnostic, got: {err:?}"
    );
}

#[test]
fn test_x12_typed_nested_group_section_is_closed_to_declared_fields() {
    // When the source gives the `GS` level a typed nested schema
    // (`group_section`), the reader renames the level and serves ONLY its
    // declared fields — the default positional `eNN` keying no longer
    // applies. So the level is closed: a declared field compiles, a field
    // it does not declare (even an in-range positional one) trips E348, and
    // the OLD default name `functional_group` no longer exists.
    fn compile(transform_cxl: &str) -> Result<crate::plan::CompiledPlan, Vec<Diagnostic>> {
        let mut yaml = String::from(
            r#"
pipeline:
  name: x12_group_section
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: x12
      path: po.x12
      options:
        group_section:
          name: grp
          fields:
            e06: string
      schema:
        - { name: seg_id, type: string }
  - type: transform
    name: t0
    input: interchange
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
        let config = parse_config(&yaml).expect("parse x12 group_section pipeline");
        config.compile(&CompileContext::default())
    }

    // The declared nested field resolves under the renamed section.
    compile("emit seg = seg_id\nemit g = $doc.grp.e06")
        .expect("a declared nested field on the renamed group section must compile");

    // An undeclared in-range positional element on the now-closed level is
    // a typo — the reader serves only `e06`.
    let err = compile("emit seg = seg_id\nemit g = $doc.grp.e07")
        .expect_err("an undeclared field on the typed nested section must fail");
    assert!(
        err.iter().any(|d| d.code == "E348"),
        "expected an E348 diagnostic for the undeclared nested field, got: {err:?}"
    );

    // The default name no longer exists once the level is renamed.
    let err = compile("emit seg = seg_id\nemit g = $doc.functional_group.e06")
        .expect_err("the default section name must not exist after a rename");
    assert!(
        err.iter().any(|d| d.code == "E348"),
        "expected an E348 diagnostic for the stale default name, got: {err:?}"
    );
}

/// Compile a JSON pipeline whose source uses the `rest` transport, with an
/// optional `envelope:` block and a transform body supplied by the caller.
fn compile_rest(
    with_envelope: bool,
    transform_cxl: &str,
) -> Result<crate::plan::CompiledPlan, Vec<Diagnostic>> {
    let envelope_block = if with_envelope {
        "      envelope:\n        sections:\n          Head:\n            extract: { json_pointer: \"/Head\" }\n            fields:\n              batch_id: string\n"
    } else {
        ""
    };
    let mut yaml = format!(
        r#"
pipeline:
  name: rest_doc_validate
nodes:
  - type: source
    name: api
    config:
      name: api
      type: json
      transport:
        kind: rest
        url: https://api.example.com/v1/records
        max_pages: 25
      options:
        record_path: records
{envelope_block}      schema:
        - {{ name: amount, type: int }}
  - type: transform
    name: t0
    input: api
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
    let config = parse_config(&yaml).expect("parse rest pipeline");
    config.compile(&CompileContext::default())
}

#[test]
fn test_rest_doc_access_aborts_with_e349() {
    // A `$doc` read against a REST source (no envelope declared) can never
    // resolve — a REST pull buffers no document. E349 rejects it (#507).
    let err = compile_rest(false, "emit amount = amount\nemit b = $doc.Head.batch_id")
        .expect_err("a `$doc` read against a rest source must fail to compile");
    let diag = err
        .iter()
        .find(|d| d.code == "E349")
        .unwrap_or_else(|| panic!("expected an E349 diagnostic, got: {err:?}"));
    assert!(
        diag.message.contains("rest source") && diag.message.contains("batch_id"),
        "E349 message must name the rest source and the access: {}",
        diag.message
    );
    assert!(diag.help.is_some(), "E349 must carry help text");
}

#[test]
fn test_rest_envelope_declaration_aborts_with_e349() {
    // Declaring an `envelope:` block on a REST source is inert and rejected
    // even when no downstream node reads `$doc` from it (#507).
    let err = compile_rest(true, "emit amount = amount")
        .expect_err("an `envelope:` block on a rest source must fail to compile");
    let diag = err
        .iter()
        .find(|d| d.code == "E349")
        .unwrap_or_else(|| panic!("expected an E349 diagnostic, got: {err:?}"));
    assert!(
        diag.message.contains("envelope") && diag.message.contains("inert"),
        "E349 message must explain the inert envelope: {}",
        diag.message
    );
}

#[test]
fn test_rest_without_doc_or_envelope_compiles() {
    // A REST source that neither declares an envelope nor is read via `$doc`
    // is unaffected — the guard fires only on an actual `$doc`/envelope use.
    compile_rest(false, "emit amount = amount + 1")
        .expect("a plain rest source with no `$doc` use must compile");
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
    let head_cutoff = path("Head", "cutoff", vec![]);
    // A joined record carries the DRIVING input's document context, so a
    // `$doc` access in the Combine predicate is attributed to the driver
    // source only. `left` is declared first (the planner's declaration-
    // order driver default) and is the source whose envelope declares
    // `Head`. The probe-side `right` source — which has no envelope —
    // must NOT be told to extract it.
    assert!(
        source_doc_paths(&plan, "left").contains(&head_cutoff),
        "the driving source `left` must carry the Combine-predicate `$doc` path"
    );
    assert!(
        !source_doc_paths(&plan, "right").contains(&head_cutoff),
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
    let paths = source_doc_paths(&plan, "payments");
    assert!(
        paths.contains(&path("Head", "cutoff", vec![])),
        "`$doc.Head.cutoff` used only in a route branch condition must reach the \
         declared set, got {paths:?}"
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
    // `alpha` carries only its own section; `beta` only its own.
    assert_eq!(
        source_doc_paths(&plan, "alpha"),
        vec![path("AHead", "a_batch", vec![])]
    );
    assert_eq!(
        source_doc_paths(&plan, "beta"),
        vec![path("BHead", "b_batch", vec![])]
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
    let paths = source_doc_paths(&compiled, "payments");
    assert!(
        paths.contains(&path("Head", "cutoff", vec![])),
        "`$doc.Head.cutoff` used in the composition body must reach the declared set, got {paths:?}"
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
    let shared_tag = path("Shared", "tag", vec![]);
    // The downstream `$doc` read must reach BOTH the primary (`customers`)
    // and the non-primary (`zip_lookup`) port sources.
    assert!(
        source_doc_paths(&compiled, "customers").contains(&shared_tag),
        "primary port source `customers` must carry the downstream `$doc` path"
    );
    let zip_paths = source_doc_paths(&compiled, "zip_lookup");
    assert!(
        zip_paths.contains(&shared_tag),
        "non-primary port source `zip_lookup` must carry the downstream `$doc` path, got {zip_paths:?}"
    );
}

/// A pipeline whose single source declares an `envelope:` block containing
/// `body` in place of the envelope's `sections:` / a section's `extract:` /
/// `fields:` keys — supplied by the caller so each test targets one nesting
/// level.
fn envelope_typo_yaml(body: &str) -> String {
    format!(
        r#"
pipeline:
  name: envelope_typo
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
{body}
      schema:
        - {{ name: amount, type: int }}
  - type: output
    name: out
    input: payments
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

#[test]
fn test_unknown_envelope_top_level_key_fails_at_parse_time() {
    // A misspelled `sections:` must fail at plan parse time — a silently
    // ignored envelope block would drop every declared section and change
    // document-context behavior without any diagnostic.
    let yaml = envelope_typo_yaml(
        r#"        sectionz:
          Head:
            extract: { xml_path: "/doc/Head" }
            fields:
              batch_id: string"#,
    );
    let err = parse_config(&yaml).expect_err("misspelled `sections:` must fail to parse");
    assert!(
        err.to_string().contains("sectionz"),
        "error must name the unknown envelope key, got: {err}"
    );
}

#[test]
fn test_unknown_envelope_section_key_fails_at_parse_time() {
    // A typo inside a declared section (here `extractt:`) must fail at plan
    // parse time instead of leaving the section without its extraction rule.
    let yaml = envelope_typo_yaml(
        r#"        sections:
          Head:
            extract: { xml_path: "/doc/Head" }
            extractt: { xml_path: "/doc/Head" }
            fields:
              batch_id: string"#,
    );
    let err = parse_config(&yaml).expect_err("unknown key inside a section must fail to parse");
    assert!(
        err.to_string().contains("extractt"),
        "error must name the unknown section key, got: {err}"
    );
}
