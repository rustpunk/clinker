//! `.` in a node name is refused for every node kind (E010).
//!
//! The character addresses a route branch (`split.high`) and a node inside
//! a composition call site (`enrich.ref`). A node whose own name carries
//! one renders identically to one of those paths, so the derived key names
//! two different things. The check once ran only for Transform, Aggregate
//! and Route, which left the two kinds that become lineage dataset nodes —
//! Source and Output — and the call site itself able to declare a name the
//! rest of the engine cannot address unambiguously.

use std::path::PathBuf;

use clinker_plan::config::{CompileContext, parse_config};

/// Compile at the public boundary and return the E010 diagnostics.
fn e010_messages(yaml: &str, ctx: &CompileContext) -> Vec<String> {
    let config = parse_config(yaml).expect("fixture must parse as YAML");
    let diags = config
        .compile(ctx)
        .expect_err("a dotted node name must fail compilation");
    diags
        .iter()
        .filter(|d| d.code == "E010")
        .map(|d| d.message.clone())
        .collect()
}

/// Every E010 message names the offending node, states the rule, and
/// carries a corrected name the author can paste.
fn assert_names_rule_and_correction(messages: &[String], offending: &str, corrected: &str) {
    let message = messages
        .first()
        .unwrap_or_else(|| panic!("expected an E010 diagnostic for node {offending:?}; got none"));
    assert!(
        message.contains(offending),
        "the diagnostic must name the offending node {offending:?}: {message:?}"
    );
    assert!(
        message.contains("reserved"),
        "the diagnostic must state the rule it broke: {message:?}"
    );
    assert!(
        message.contains(corrected),
        "the diagnostic must offer {corrected:?} as a pasteable correction: {message:?}"
    );
}

#[test]
fn a_dotted_source_name_is_refused() {
    let yaml = r#"
pipeline:
  name: dotted_source
nodes:
  - type: source
    name: raw.orders
    config:
      name: raw.orders
      type: csv
      path: in.csv
      schema:
        - { name: amount, type: int }
  - type: sink
    name: out
    input: raw.orders
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let messages = e010_messages(yaml, &CompileContext::default());
    assert_names_rule_and_correction(&messages, "raw.orders", "raw_orders");
}

#[test]
fn a_dotted_output_name_is_refused() {
    let yaml = r#"
pipeline:
  name: dotted_output
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: amount, type: int }
  - type: sink
    name: enrich.ref
    input: src
    config:
      name: enrich.ref
      type: csv
      path: out.csv
"#;
    let messages = e010_messages(yaml, &CompileContext::default());
    assert_names_rule_and_correction(&messages, "enrich.ref", "enrich_ref");
}

/// A composition call site's own name is the prefix of every derived key
/// inside its body, so it is the one name that must not carry a `.`.
#[test]
fn a_dotted_composition_call_site_name_is_refused() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("passthrough.comp.yaml"),
        r#"_compose:
  name: passthrough
  inputs:
    inp:
      schema:
        - { name: amount, type: int }
  outputs:
    out: pass
  config_schema: {}

nodes:
  - type: transform
    name: pass
    input: inp
    config:
      cxl: |
        emit amount = amount
"#,
    )
    .expect("write comp");
    std::fs::create_dir_all(workspace.path().join("pipelines")).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: dotted_call_site
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: amount, type: int }
  - type: composition
    name: enrich.step
    input: src
    use: ../compositions/passthrough.comp.yaml
    inputs:
      inp: src
  - type: sink
    name: out
    input: enrich.step
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    let messages = e010_messages(yaml, &ctx);
    assert_names_rule_and_correction(&messages, "enrich.step", "enrich_step");
}

/// The body of a composition is never walked by the top-level pass, and a
/// dotted body node name is the same ambiguity one level down: its key is
/// the call-site path joined to its name.
#[test]
fn a_dotted_composition_body_node_name_is_refused() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("dotted_body.comp.yaml"),
        r#"_compose:
  name: dotted_body
  inputs:
    inp:
      schema:
        - { name: amount, type: int }
  outputs:
    out: pass.through
  config_schema: {}

nodes:
  - type: transform
    name: pass.through
    input: inp
    config:
      cxl: |
        emit amount = amount
"#,
    )
    .expect("write comp");
    std::fs::create_dir_all(workspace.path().join("pipelines")).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: dotted_body_node
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: amount, type: int }
  - type: composition
    name: enrich
    input: src
    use: ../compositions/dotted_body.comp.yaml
    inputs:
      inp: src
  - type: sink
    name: out
    input: enrich
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    let messages = e010_messages(yaml, &ctx);
    assert_names_rule_and_correction(&messages, "pass.through", "pass_through");
}

/// The rule is about the name a node declares, not about references: a
/// route branch consumed as `split.high` is exactly what the `.` is
/// reserved for and must still compile.
#[test]
fn a_branch_reference_is_still_accepted() {
    let yaml = r#"
pipeline:
  name: branch_reference
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: amount, type: int }
  - type: route
    name: split
    input: src
    config:
      mode: exclusive
      conditions:
        high: "amount > 100"
      default: low
  - type: sink
    name: high_out
    input: split.high
    config:
      name: high_out
      type: csv
      path: high.csv
  - type: sink
    name: low_out
    input: split.low
    config:
      name: low_out
      type: csv
      path: low.csv
"#;
    let config = parse_config(yaml).expect("fixture must parse as YAML");
    let diags = config.compile_topology_only(&CompileContext::default());
    assert!(
        !diags.iter().any(|d| d.code == "E010"),
        "consuming a route branch is the reserved use of `.`, not a violation: {diags:?}"
    );
}
