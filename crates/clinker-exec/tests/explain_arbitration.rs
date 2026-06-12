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

use clinker_plan::config::{CompileContext, PipelineConfig, parse_config};
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

/// Render the artifacts-aware `--explain` text (the surface the CLI emits):
/// the combine block plus the `=== Statistics ===` section. Anchored so
/// the statistics catalog's file-metadata row counts are deterministic.
fn render_explain_with_artifacts_anchored(yaml: &str, anchor: &Path) -> String {
    let config: PipelineConfig = parse_config(yaml).expect("parse_config");
    let ctx = CompileContext::with_pipeline_dir(anchor, "");
    let plan = config.compile(&ctx).expect("compile");
    plan.dag()
        .explain_text_with_artifacts(&config, plan.artifacts())
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
        "spill_priority=N/A, can_back_pressure=true, predicted_peak=0B, predicted_freed=0B, \
         predicted_subtree_reclaim=0B",
        "Source frees zero on spill; only its pause is a real lever"
    );
    assert_eq!(
        arbitration_line_for(&text, "aggregation.dept_totals"),
        "spill_priority=30, can_back_pressure=false, predicted_peak=0B, predicted_freed=0B, \
         predicted_subtree_reclaim=0B",
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
        "spill_priority=N/A, can_back_pressure=true, predicted_peak=1K, predicted_freed=0B, \
         predicted_subtree_reclaim=1K",
        "a sized Source seeds its file size as peak, frees nothing live the instant it drains, \
         but its downstream Aggregate's accumulated state propagates up as its subtree reclaim"
    );
    assert_eq!(
        arbitration_line_for(&text, "aggregation.dept_totals"),
        "spill_priority=30, can_back_pressure=false, predicted_peak=1K, predicted_freed=1K, \
         predicted_subtree_reclaim=1K",
        "a blocking hash Aggregate's peak is its accumulated input and it frees that on drain; \
         its subtree reclaim equals its own freed (nothing downstream accumulates more)"
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

/// The `=== Estimated Spill Volume ===` section lists one estimate per
/// blocking stage and a total. With a sized 1 KiB input feeding a hash
/// Aggregate, the section names the Aggregate at its `1K` predicted peak and
/// reports a `1K` total — the per-stage estimate AC#1 surfaces.
#[test]
fn explain_renders_per_stage_spill_estimate() {
    let tmp = tempfile::tempdir().unwrap();
    write_sized(tmp.path(), "orders.csv", &orders_csv_1kib(), 1024);

    let text = render_explain_anchored(source_aggregate_output_yaml(), tmp.path());

    assert!(
        text.contains("=== Estimated Spill Volume ==="),
        "explain text must carry the per-stage spill-estimate section, got:\n{text}"
    );
    let section_start = text
        .find("=== Estimated Spill Volume ===")
        .expect("estimate section present");
    let section = &text[section_start..];
    assert!(
        section.contains("dept_totals") && section.contains("→ 1K"),
        "the hash Aggregate stage must show its 1K estimate, got:\n{section}"
    );
    assert!(
        section.contains("Total: 1K"),
        "the section must report a 1K total for the single spilling stage, got:\n{section}"
    );
}

/// A streaming-only pipeline (Source → Transform → Output, no blocking
/// operator) has nothing that spills, so the estimate section is omitted
/// entirely rather than rendering an empty or zero block.
#[test]
fn explain_omits_spill_estimate_for_streaming_only() {
    let yaml = r#"
pipeline:
  name: streaming_only
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
  - type: transform
    name: tag
    input: orders
    config:
      cxl: |
        emit department = department
        emit amount = amount
  - type: output
    name: out
    input: tag
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let text = render_explain(yaml);
    assert!(
        !text.contains("=== Estimated Spill Volume ==="),
        "a streaming-only pipeline has no spilling stage, so the estimate section must be omitted, \
         got:\n{text}"
    );
}

/// A pure-equi combine over two CSV sources whose on-disk sizes seed
/// metadata-derived row counts. With a tight `memory.limit`, the smaller
/// (build) side's row count × the per-record byte estimate breaches the
/// soft limit, so the planner picks `GraceHash` over the default in-memory
/// hash — a plan-time decision driven entirely by file metadata, before
/// any record is read. The two-source CSV bodies are padded to exact byte
/// lengths so the seeded row counts (bytes ÷ 1024) are deterministic.
fn grace_from_metadata_yaml() -> &'static str {
    r#"
pipeline:
  name: grace_from_metadata
  memory:
    limit: 64K
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: product_id, type: string }
        - { name: amount, type: string }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
  - type: combine
    name: enriched
    input:
      orders: orders
      products: products
    config:
      where: "orders.product_id == products.product_id"
      match: first
      on_miss: null_fields
      cxl: |
        emit product_id = orders.product_id
        emit amount = orders.amount
        emit name = products.name
      propagate_ck: driver
  - type: output
    name: out
    input: enriched
    config:
      name: out
      type: csv
      path: out.csv
"#
}

/// Write a CSV with `header` and rows padded so the file is exactly
/// `target_len` bytes — the seed `--explain` divides by 1024 to land a
/// deterministic row count.
fn write_padded_csv(dir: &Path, name: &str, header: &str, target_len: u64) {
    let mut body = String::new();
    body.push_str(header);
    body.push('\n');
    // Each data row is `p<n>,<pad>` — grow until the file reaches the
    // target length, padding the final row so the byte count is exact.
    let mut row = 0u64;
    while (body.len() as u64) < target_len {
        let remaining = target_len - body.len() as u64;
        let line = format!("p{row},{}", "x".repeat(8));
        if (line.len() as u64 + 1) <= remaining {
            body.push_str(&line);
            body.push('\n');
        } else {
            // Final padding to hit the exact length with a comment-free tail.
            body.push_str(&"y".repeat(remaining as usize));
        }
        row += 1;
    }
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).expect("create csv");
    f.write_all(body.as_bytes()).expect("write csv");
    f.flush().expect("flush csv");
    assert_eq!(
        body.len() as u64,
        target_len,
        "fixture `{name}` must be exactly {target_len} bytes for a stable row-count seed"
    );
}

/// Pin the full artifacts-aware explain text for a plan whose GraceHash
/// choice is driven by file-metadata row counts, so the `Statistics`
/// section and the strategy citation are reviewed line-by-line on change.
#[test]
fn explain_cites_metadata_driven_grace_hash() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // products = 60 KiB → ≈58 rows (build side); orders = 90 KiB → ≈87
    // rows. min(58, 87) = 58; 58 * 1024 ≈ 59 KB ≥ 80% of the 64 KiB limit,
    // so GraceHash fires.
    write_padded_csv(tmp.path(), "products.csv", "product_id,name", 60 * 1024);
    write_padded_csv(tmp.path(), "orders.csv", "product_id,amount", 90 * 1024);

    let text = render_explain_with_artifacts_anchored(grace_from_metadata_yaml(), tmp.path());

    assert!(
        text.contains("Strategy: GraceHash"),
        "metadata-derived build row count must drive GraceHash; got:\n{text}"
    );
    assert!(
        text.contains("=== Statistics ==="),
        "explain must carry the Statistics section when sources are sized; got:\n{text}"
    );
    assert!(
        text.contains("[file metadata] (informs combine build/probe + partition bits)"),
        "row counts must cite their file-metadata provenance and the decision they inform; \
         got:\n{text}"
    );
    insta::assert_snapshot!("explain_metadata_driven_grace_hash", text);
}
