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
use std::io::Write;
use std::path::Path;

fn render_explain(yaml: &str) -> String {
    let config: PipelineConfig = parse_config(yaml).expect("parse_config");
    let plan = config.compile(&CompileContext::default()).expect("compile");
    plan.dag().explain_text(&config)
}

/// Render explain text with source `path:` strings resolved against
/// `anchor` (the pipeline file's directory), so a committed/fixed-size
/// input file produces a deterministic, environment-independent volume
/// prediction in the rendered surface.
fn render_explain_anchored(yaml: &str, anchor: &Path) -> String {
    let config: PipelineConfig = parse_config(yaml).expect("parse_config");
    let ctx = CompileContext::with_pipeline_dir(anchor, "");
    let plan = config.compile(&ctx).expect("compile");
    plan.dag().explain_text(&config)
}

/// Write `contents` to `<dir>/<name>` and assert its on-disk length so a
/// snapshot's predicted-byte values are pinned to a known, fixed size.
fn write_sized(dir: &Path, name: &str, contents: &str, expected_len: u64) {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).expect("create data file");
    f.write_all(contents.as_bytes()).expect("write data file");
    f.flush().expect("flush data file");
    assert_eq!(
        contents.len() as u64,
        expected_len,
        "fixture `{name}` must be exactly {expected_len} bytes for a stable snapshot"
    );
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

/// A 1024-byte CSV body for `orders.csv`: an 18-byte header plus rows,
/// the final row padded with a digit run so the on-disk length is exactly
/// 1 KiB. `format_bytes(1024)` renders this as `1K`, giving the snapshot a
/// clean, fixed predicted-volume value.
fn orders_csv_1kib() -> String {
    let header = "department,amount\n"; // 18 bytes
    let mut body = String::from(header);
    body.push_str("sales,100\n"); // 10 bytes
    body.push_str("ops,250\n"); // 8 bytes
    // 18 + 10 + 8 = 36 bytes so far; pad to 1024 with one final row whose
    // amount field is a digit run. `"sales,"` (6) + `"\n"` (1) = 7 bytes of
    // framing, so the digit run is 1024 - 36 - 7 = 981 digits.
    body.push_str("sales,");
    body.push_str(&"9".repeat(981));
    body.push('\n');
    body
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

    // The fixture's `orders.csv` does not exist relative to the default
    // compile anchor, so every node's volume seed is the `0B` unknown
    // sentinel — a deterministic, environment-independent value.
    assert_eq!(
        arbitration_line_for(&text, "source.orders"),
        "spill_priority=N/A, can_back_pressure=true, predicted_peak=0B, predicted_freed=0B",
        "Source frees zero on spill; only its pause is a real lever"
    );
    assert_eq!(
        arbitration_line_for(&text, "aggregation.dept_totals"),
        "spill_priority=30, can_back_pressure=false, predicted_peak=0B, predicted_freed=0B",
        "hash Aggregate accumulates the full group table and spills it; it never pauses"
    );

    // The materialized Source→Aggregate boundary surfaces as a priority-0
    // node_buffer edge — the cheapest spill victim class.
    assert!(
        text.contains("=== Buffer Edges ===")
            && text.contains("edge source.orders -> aggregation.dept_totals:")
            && text.contains(
                "arbitration: spill_priority=0, can_back_pressure=false, \
                 predicted_peak=0B, predicted_freed=0B"
            ),
        "expected a priority-0 node_buffer edge for the materialized Source→Aggregate boundary, got:\n{text}"
    );
}

/// Pin the full explain text for a pipeline whose Source reads a sized
/// (1 KiB) input file, so the `predicted_peak` / `predicted_freed`
/// annotations render NON-ZERO values rather than only the `0B` unknown
/// case. The blocking hash Aggregate accumulates the whole 1 KiB and frees
/// it on drain, so it renders `predicted_peak=1K, predicted_freed=1K`; the
/// streaming Source and Output carry the volume through but free nothing.
/// Anchored at a tempdir so the size is fixed and CWD-independent.
#[test]
fn explain_renders_nonzero_volume_predictions() {
    let tmp = tempfile::tempdir().unwrap();
    write_sized(tmp.path(), "orders.csv", &orders_csv_1kib(), 1024);

    let text = render_explain_anchored(source_aggregate_output_yaml(), tmp.path());
    insta::assert_snapshot!("explain_source_aggregate_output_sized", text);
}

/// Guard the non-zero volume semantics directly so a snapshot auto-accept
/// cannot silently zero the predictions or swap peak/freed: the Source's
/// peak equals the file size and it frees nothing; the blocking Aggregate's
/// peak and freed both equal the accumulated volume.
#[test]
fn explain_nonzero_predictions_are_surfaced() {
    let tmp = tempfile::tempdir().unwrap();
    write_sized(tmp.path(), "orders.csv", &orders_csv_1kib(), 1024);

    let text = render_explain_anchored(source_aggregate_output_yaml(), tmp.path());

    assert_eq!(
        arbitration_line_for(&text, "source.orders"),
        "spill_priority=N/A, can_back_pressure=true, predicted_peak=1K, predicted_freed=0B",
        "a sized Source seeds its file size as peak and frees nothing live"
    );
    assert_eq!(
        arbitration_line_for(&text, "aggregation.dept_totals"),
        "spill_priority=30, can_back_pressure=false, predicted_peak=1K, predicted_freed=1K",
        "a blocking hash Aggregate's peak is its accumulated input and it frees that on drain"
    );

    // The node_buffer slot between the sized Source and the Aggregate holds
    // the producer's materialized output, so it carries the producer's
    // non-zero predicted volume.
    assert!(
        text.contains(
            "arbitration: spill_priority=0, can_back_pressure=false, \
             predicted_peak=1K, predicted_freed=1K (producer: source)"
        ),
        "the Source→Aggregate node_buffer edge must carry the producer's non-zero volume, got:\n{text}"
    );
}
