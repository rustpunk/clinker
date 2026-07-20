//! Plan-time output-envelope section-name validation (`E346`) across
//! composition input ports.
//!
//! `E346` rejects a `reconstruct_envelope` Output whose `header_from_doc` /
//! `footer_from_doc` names a section that no FEEDING source declares. The
//! feeding-source set is traced upstream through the node graph; a
//! `Composition` must be resolved through EVERY bound `inputs:` port, not just
//! its primary port, so a section declared on a source bound to a non-primary
//! port stays reachable and is not falsely rejected.

use clinker_plan::config::parse_config;

/// Two sources feed a composition on distinct input ports (`primary`,
/// `reference`); a `concat` `envelope` node consolidates the composition output
/// so the `reconstruct_envelope` Output reaches the E346 section-name check
/// rather than the E347 lineage-stripper guard. `header_from_doc` names the
/// caller-supplied section. The composition body file is never resolved —
/// `parse_config` validates the top-level graph, where the port bindings live.
fn multi_port_pipeline(header_section: &str) -> String {
    format!(
        r#"
pipeline:
  name: comp_port_envelope
nodes:
  - type: source
    name: src_primary
    config:
      name: src_primary
      type: json
      glob: ./p/*.json
      options:
        record_path: records
      envelope:
        sections:
          HeadPrimary:
            extract: {{ json_pointer: "/HeadPrimary" }}
            fields:
              id: string
      schema:
        - {{ name: amount, type: int }}
  - type: source
    name: src_reference
    config:
      name: src_reference
      type: json
      glob: ./r/*.json
      options:
        record_path: records
      envelope:
        sections:
          HeadReference:
            extract: {{ json_pointer: "/HeadReference" }}
            fields:
              id: string
      schema:
        - {{ name: amount, type: int }}
  - type: composition
    name: enrich
    input: src_primary
    use: ./enrich.comp.yaml
    inputs:
      primary: src_primary
      reference: src_reference
  - type: envelope
    name: framed
    body: enrich
    config: {{ strategy: concat }}
  - type: output
    name: out
    input: framed
    config:
      name: out
      type: csv
      path: out.csv
      reconstruct_envelope: true
      options:
        envelope:
          header_from_doc: {header_section}
"#,
    )
}

#[test]
fn envelope_section_on_non_primary_composition_port_is_accepted() {
    // `HeadReference` is declared on `src_reference`, bound to the composition's
    // NON-primary `reference` port. Scoping E346 to the composition's primary
    // port alone (`header.input` = `src_primary`) would falsely reject it;
    // unioning every bound port keeps it reachable.
    parse_config(&multi_port_pipeline("HeadReference"))
        .expect("a section on a non-primary composition port must not trip E346");
}

#[test]
fn envelope_section_on_primary_composition_port_is_accepted() {
    // The primary-port source's section stays reachable too — the fix widens
    // the feeding-source set, it does not shift it off the primary port.
    parse_config(&multi_port_pipeline("HeadPrimary"))
        .expect("a section on the primary composition port must not trip E346");
}

#[test]
fn envelope_section_declared_on_no_feeding_port_is_rejected_e346() {
    // A genuinely-undeclared section (declared by no source on any bound port)
    // is still rejected — widening the feeding-source scope does not disable the
    // check.
    let err = parse_config(&multi_port_pipeline("HeadMissing"))
        .expect_err("an undeclared section must still trip E346");
    let msg = err.to_string();
    assert!(msg.contains("E346"), "expected E346, got: {msg}");
    assert!(
        msg.contains("HeadMissing"),
        "message should name the undeclared section: {msg}"
    );
}
