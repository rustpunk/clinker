//! Snapshot + targeted gates for the `--explain` arbitration annotation.
//!
//! Each non-fused node carries an `arbitration: spill_priority=.., can_back_pressure=..`
//! line in the **Physical Properties** stanza, and every `node_buffers`
//! slot between non-fused stages gets a `=== Buffer Edges ===` pseudo-node.
//! The numbers are a plan-time mirror of the runtime `MemoryConsumer`
//! impls, so these gates pin the mirror against silent drift:
//!
//! - a hash Aggregate renders `spill_priority=30, can_back_pressure=false`,
//! - a Source renders `spill_priority=N/A, can_back_pressure=true`
//!   (it frees zero on spill; only its pause is a real lever),
//! - the Source→Aggregate edge renders a priority-0, non-back-pressureable
//!   `node_buffer` slot.

use clinker_core::config::{CompileContext, PipelineConfig, parse_config};

fn render_explain(yaml: &str) -> String {
    let config: PipelineConfig = parse_config(yaml).expect("parse_config");
    let plan = config.compile(&CompileContext::default()).expect("compile");
    plan.dag().explain_text(&config)
}

/// Source → Aggregate → Output: the canonical bounded-memory shape. The
/// Aggregate has no upstream sort, so the optimizer keeps it on the hash
/// strategy — the spillable case the arbitrator ranks at priority 30.
fn source_aggregate_output_yaml() -> &'static str {
    r#"
pipeline:
  name: arbitration_explain_demo
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: department, type: string }
        - { name: amount, type: int }

  - type: aggregate
    name: dept_totals
    input: orders
    config:
      group_by: [department]
      cxl: |
        emit department = department
        emit total = sum(amount)

  - type: output
    name: out
    input: dept_totals
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#
}

/// Pin the full explain text so any change to the arbitration surface is
/// reviewed line-by-line in the `cargo insta review` diff before landing.
#[test]
fn explain_renders_arbitration_annotation() {
    let text = render_explain(source_aggregate_output_yaml());
    insta::assert_snapshot!("explain_source_aggregate_output", text);
}

/// Slice the `arbitration:` value out of the Physical Properties stanza
/// for one node slug. Stops at the first non-indented line so a missing
/// arbitration line panics inside the right stanza rather than walking
/// into the next node.
fn arbitration_line_for(text: &str, id_slug: &str) -> String {
    let header = "=== Physical Properties ===\n";
    let start = text.find(header).expect("Physical Properties section");
    let block = &text[start + header.len()..];
    let needle = format!("{id_slug}:\n");
    let stanza_start = block
        .find(&needle)
        .unwrap_or_else(|| panic!("missing Physical Properties stanza for `{id_slug}`:\n{block}"));
    let after_header = &block[stanza_start + needle.len()..];
    for line in after_header.lines() {
        if !line.starts_with("  ") {
            break;
        }
        if let Some(rest) = line.strip_prefix("  arbitration: ") {
            return rest.to_string();
        }
    }
    panic!("`arbitration:` line missing inside `{id_slug}` stanza:\n{text}")
}

/// Guard the load-bearing semantics directly so a snapshot auto-accept
/// cannot silently invert the back-pressure flags or the spill priority:
/// the Source pauses (and never spills), the hash Aggregate spills at
/// priority 30 (and never pauses).
#[test]
fn explain_arbitration_semantics_are_not_inverted() {
    let text = render_explain(source_aggregate_output_yaml());

    assert_eq!(
        arbitration_line_for(&text, "source.orders"),
        "spill_priority=N/A, can_back_pressure=true",
        "Source frees zero on spill; only its pause is a real lever"
    );
    assert_eq!(
        arbitration_line_for(&text, "aggregation.dept_totals"),
        "spill_priority=30, can_back_pressure=false",
        "hash Aggregate accumulates the full group table and spills it; it never pauses"
    );

    // The materialized Source→Aggregate boundary surfaces as a priority-0
    // node_buffer edge — the cheapest spill victim class.
    assert!(
        text.contains("=== Buffer Edges ===")
            && text.contains("edge source.orders -> aggregation.dept_totals:")
            && text.contains("arbitration: spill_priority=0, can_back_pressure=false"),
        "expected a priority-0 node_buffer edge for the materialized Source→Aggregate boundary, got:\n{text}"
    );
}
