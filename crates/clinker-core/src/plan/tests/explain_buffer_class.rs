//! `--explain` buffer-class annotation gates.
//!
//! Each test compiles a fixture pipeline, renders the explain text, then
//! reads the `buffer:` line out of the **Physical Properties** stanza for
//! the named nodes. The runtime correspondence the label encodes is:
//!
//! - `streaming`  — the node's output does not pass through a
//!   `ctx.node_buffers` slot at runtime (fused Source/Transform chain
//!   or a sink Output).
//! - `materialized` — the node's output is admitted into `node_buffers`
//!   between dispatch arms and charges `MemoryBudget`.
//!
//! These gates pin the annotation contract so it stays stable across
//! future executor / planner refactors.
//!
//! Acceptance for issue #124.

use super::dag::parse_fixture;
use crate::config::{CompileContext, PipelineConfig};
use crate::plan::execution::ExecutionPlanDag;

fn compile(config: &PipelineConfig) -> ExecutionPlanDag {
    config
        .compile(&CompileContext::default())
        .expect("compile")
        .dag()
        .clone()
}

/// Slice the Physical Properties block out of an explain text dump.
///
/// Returns the substring between the section header and the next blank
/// `===` divider so per-node grep doesn't accidentally walk into the
/// Route / Combine / DAG-Topology sections below.
fn physical_properties_block(text: &str) -> &str {
    let header = "=== Physical Properties ===\n";
    let start = text.find(header).expect("Physical Properties section");
    let after_header = &text[start + header.len()..];
    let end = after_header.find("\n===").unwrap_or(after_header.len());
    &after_header[..end]
}

/// Find the `buffer:` value for the node whose id slug header opens its
/// Physical Properties stanza. Panics if the slug isn't present so the
/// failure message points at the missing node rather than a silent
/// `None`.
fn buffer_class_for(block: &str, id_slug: &str) -> String {
    let needle = format!("{id_slug}:\n");
    let stanza_start = block
        .find(&needle)
        .unwrap_or_else(|| panic!("missing Physical Properties stanza for `{id_slug}`:\n{block}"));
    let after_header = &block[stanza_start + needle.len()..];
    for line in after_header.lines() {
        if !line.starts_with("  ") {
            break;
        }
        if let Some(rest) = line.strip_prefix("  buffer: ") {
            return rest.to_string();
        }
    }
    panic!("`buffer:` line missing inside `{id_slug}` stanza:\n{block}")
}

// ── Gate 1: fully fused streaming chain ──────────────────────────────

fn linear_streaming_yaml() -> &'static str {
    r#"
pipeline:
  name: streaming-chain
nodes:
  - type: source
    name: primary
    config:
      name: primary
      type: csv
      path: data.csv
      schema:
        - { name: amount, type: float }

  - type: transform
    name: scale
    input: primary
    config:
      cxl: |
        emit doubled = amount * 2

  - type: output
    name: out
    input: scale
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#
}

/// Source → Transform → Output: the executor's fused-transform pass
/// claims the Source's receiver for the Transform's per-record path, so
/// none of the three stages allocate a `node_buffers` slot. Every
/// `buffer:` line must read `streaming`.
#[test]
fn buffer_class_marks_every_stage_streaming_in_fused_chain() {
    let config = parse_fixture(linear_streaming_yaml());
    let dag = compile(&config);
    let text = dag.explain_text(&config);
    let block = physical_properties_block(&text);

    assert_eq!(buffer_class_for(block, "source.primary"), "streaming");
    assert_eq!(buffer_class_for(block, "transform.scale"), "streaming");
    assert_eq!(buffer_class_for(block, "output.out"), "streaming");
}

// ── Gate 2: route fan-out forces materialization ─────────────────────

fn route_fanout_yaml() -> &'static str {
    r#"
pipeline:
  name: route-fanout
nodes:
  - type: source
    name: primary
    config:
      name: primary
      type: csv
      path: data.csv
      schema:
        - { name: amount, type: float }

  - type: route
    name: categorize
    input: primary
    config:
      mode: exclusive
      conditions:
        out_high: "amount > 100"
        out_low: "amount <= 100"
      default: out_low

  - type: output
    name: out_high
    input: categorize.out_high
    config:
      name: out_high
      type: csv
      path: out_high.csv
      include_unmapped: true

  - type: output
    name: out_low
    input: categorize.out_low
    config:
      name: out_low
      type: csv
      path: out_low.csv
      include_unmapped: true
"#
}

/// Source → Route → {OutputA, OutputB}: the Route node admits records
/// into each successor's `node_buffers` slot, and the upstream Source
/// — no Transform follow-up to fuse into — also drains into
/// `node_buffers[source]`. Only the sink Outputs stay streaming.
#[test]
fn buffer_class_marks_route_materialized_and_sinks_streaming() {
    let config = parse_fixture(route_fanout_yaml());
    let dag = compile(&config);
    let text = dag.explain_text(&config);
    let block = physical_properties_block(&text);

    assert_eq!(buffer_class_for(block, "route.categorize"), "materialized");
    assert_eq!(buffer_class_for(block, "source.primary"), "materialized");
    assert_eq!(buffer_class_for(block, "output.out_high"), "streaming");
    assert_eq!(buffer_class_for(block, "output.out_low"), "streaming");
}
