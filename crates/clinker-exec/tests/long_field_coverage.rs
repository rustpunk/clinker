//! End-to-end coverage that long (>23-byte) string fields flow correctly
//! through every operator.
//!
//! Short field values (<=23 bytes — the dominant ETL shape) live inline in
//! the `SmolStr` backing `Value::String`; values past that boundary take the
//! Arc-backed heap representation. The existing fixtures are short-field
//! dominated, so these tests pin the long-field arm specifically: 36-char
//! UUIDs, postal addresses, and free-text comments exercised through
//! Transform string methods, Aggregate group-by/distinct keying, a Combine
//! join keyed on a long UUID, the spill/reload serde path, and CSV/JSON
//! output — each with exact expected output, not merely a non-crash check.
//!
//! Every test asserts the participating field values are genuinely >23 bytes
//! so a future schema change can't silently downgrade the coverage to the
//! inline arm.

use std::collections::HashMap;
use std::io::{Cursor, Write};

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams, SourceReaders};
use clinker_plan::config::{CompileContext, parse_config};

/// A 36-char UUID — past the 23-byte inline boundary, so it is Arc-backed.
const TICKET_UUID_1: &str = "3f29c4b1-8a6e-4d2f-9c17-0b5e8a1d6e42";
const TICKET_UUID_2: &str = "a7e3f10c-5b29-4e6d-8f41-2c9a0d7b3e15";
const TICKET_UUID_3: &str = "c2b8d6a4-1f37-4a90-b5e2-8d0c4f6a9b71";
const AGENT_UUID_EAST: &str = "9b1d7c34-2e5a-4f80-a6c9-1d3b7e2f5a88";
const AGENT_UUID_WEST: &str = "7f2c5a18-9d3e-4061-b8a4-2c6f9e1d7a05";

/// Asserts that every value the test relies on is genuinely heap-backed,
/// so the coverage exercises the Arc path rather than inline storage.
fn assert_all_heap_backed(values: &[&str]) {
    for v in values {
        assert!(
            v.len() > 23,
            "fixture value {v:?} is {} bytes — must exceed the 23-byte inline \
             boundary to exercise the Arc-backed long-field path",
            v.len()
        );
    }
}

fn run_params(id: &str) -> PipelineRunParams {
    PipelineRunParams {
        execution_id: id.to_string(),
        batch_id: format!("{id}-batch"),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

/// Compile `yaml`, feed `(source_name, csv)` readers, capture the single
/// output named `output_name`, and return `(report, output_string)`.
fn run_pipeline(
    yaml: &str,
    inputs: &[(&str, &str)],
    output_name: &str,
    id: &str,
) -> (ExecutionReport, String) {
    let config = parse_config(yaml).expect("fixture pipeline must parse");
    let plan = config
        .compile(&CompileContext::default())
        .expect("fixture pipeline must compile");

    let readers: SourceReaders = inputs
        .iter()
        .map(|(name, csv)| {
            (
                (*name).to_string(),
                clinker_exec::executor::single_file_reader(
                    format!("{name}.csv"),
                    Box::new(Cursor::new(csv.as_bytes().to_vec())),
                ),
            )
        })
        .collect();

    let sink = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        output_name.to_string(),
        Box::new(sink.clone()) as Box<dyn Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params(id))
            .expect("pipeline must run to completion");
    (report, sink.as_string())
}

/// Sort the CSV body (everything after the header) so set-equality holds
/// regardless of hash-table emit order.
fn sorted_body(output: &str) -> Vec<String> {
    let mut lines: Vec<String> = output.lines().skip(1).map(str::to_string).collect();
    lines.sort();
    lines
}

// ── Transform: string methods over long fields ─────────────────────────────

/// `substring`, `upper`, `concat`, and `length` applied to 36-char UUIDs and
/// free-text comments produce exactly the expected values, and a filter on a
/// long-field-derived column keeps the right rows.
#[test]
fn transform_string_methods_on_long_fields_exact_output() {
    assert_all_heap_backed(&[TICKET_UUID_1, TICKET_UUID_2]);

    let yaml = r#"
pipeline:
  name: long_field_transform
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: ticket_uuid, type: string }
        - { name: comment, type: string }
  - type: transform
    name: derive
    input: src
    config:
      cxl: |
        emit uuid_prefix = ticket_uuid.substring(0, 8)
        emit uuid_len = ticket_uuid.length()
        emit comment_upper = comment.upper()
        emit tagged = ticket_uuid.substring(0, 8).concat("|", comment)
        filter comment.length() > 4
  - type: output
    name: out
    input: derive
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: false
"#;

    // Row 2's comment "skip" is 4 chars, so `length() > 4` drops it; rows 1
    // and 3 survive. This proves the filter sees the full-width long value.
    let csv = format!(
        "ticket_uuid,comment\n\
         {TICKET_UUID_1},hello world\n\
         {TICKET_UUID_2},skip\n\
         {TICKET_UUID_3},final note\n"
    );
    let (report, output) = run_pipeline(yaml, &[("src", &csv)], "out", "transform-long");
    assert_eq!(report.counters.dlq_count, 0);
    assert_eq!(
        report.counters.ok_count, 2,
        "the 4-char comment row is filtered out"
    );

    assert_eq!(
        sorted_body(&output),
        vec![
            // uuid_prefix, uuid_len, comment_upper, tagged
            "3f29c4b1,36,HELLO WORLD,3f29c4b1|hello world".to_string(),
            "c2b8d6a4,36,FINAL NOTE,c2b8d6a4|final note".to_string(),
        ],
        "string methods over the 36-char UUID and comment must produce exact values"
    );
}

// ── Aggregate: group-by on a long string key ───────────────────────────────

/// Grouping by a 36-char agent UUID produces one output row per distinct
/// UUID with correct counts — proving the `Value::String` → `GroupByKey::Str`
/// round-trip preserves equality for heap-backed strings.
#[test]
fn aggregate_group_by_long_uuid_key_exact_counts() {
    assert_all_heap_backed(&[AGENT_UUID_EAST, AGENT_UUID_WEST]);

    let yaml = r#"
pipeline:
  name: long_field_group_by
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: agent_uuid, type: string }
        - { name: ticket_id, type: string }
  - type: aggregate
    name: by_agent
    input: src
    config:
      group_by: [agent_uuid]
      cxl: |
        emit agent_uuid = agent_uuid
        emit ticket_count = count(*)
  - type: output
    name: out
    input: by_agent
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

    // Three rows for EAST, one for WEST: keying must collapse the three
    // identical 36-char UUIDs into a single group of count 3.
    let csv = format!(
        "agent_uuid,ticket_id\n\
         {AGENT_UUID_EAST},t1\n\
         {AGENT_UUID_EAST},t2\n\
         {AGENT_UUID_WEST},t3\n\
         {AGENT_UUID_EAST},t4\n"
    );
    let (report, output) = run_pipeline(yaml, &[("src", &csv)], "out", "agg-long");
    assert_eq!(report.counters.dlq_count, 0);
    assert_eq!(report.counters.ok_count, 2, "two distinct agent UUIDs");

    assert_eq!(
        sorted_body(&output),
        vec![
            format!("{AGENT_UUID_WEST},1"),
            format!("{AGENT_UUID_EAST},3"),
        ],
        "the three identical 36-char UUIDs must collapse into one group of 3"
    );
}

// ── Distinct: dedup keyed on a long string value ───────────────────────────

/// `distinct by` on a 36-char UUID drops exact-duplicate keys and keeps
/// every distinct one — the same heap-backed-string equality path as
/// group-by, exercised through the `distinct` statement.
#[test]
fn distinct_by_long_uuid_drops_duplicates_exact_output() {
    assert_all_heap_backed(&[TICKET_UUID_1, TICKET_UUID_2, TICKET_UUID_3]);

    let yaml = r#"
pipeline:
  name: long_field_distinct
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: ticket_uuid, type: string }
  - type: transform
    name: dedup
    input: src
    config:
      cxl: |
        emit ticket_uuid = ticket_uuid
        distinct by ticket_uuid
  - type: output
    name: out
    input: dedup
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

    // UUID_1 appears three times, UUID_2 twice, UUID_3 once: distinct must
    // keep exactly one of each.
    let csv = format!(
        "ticket_uuid\n\
         {TICKET_UUID_1}\n\
         {TICKET_UUID_2}\n\
         {TICKET_UUID_1}\n\
         {TICKET_UUID_3}\n\
         {TICKET_UUID_2}\n\
         {TICKET_UUID_1}\n"
    );
    let (report, output) = run_pipeline(yaml, &[("src", &csv)], "out", "distinct-long");
    assert_eq!(report.counters.dlq_count, 0);

    assert_eq!(
        sorted_body(&output),
        vec![
            TICKET_UUID_1.to_string(),
            TICKET_UUID_2.to_string(),
            TICKET_UUID_3.to_string(),
        ],
        "distinct by a 36-char UUID must keep exactly one row per distinct value"
    );
}

// ── Combine: join keyed on / carrying long fields ──────────────────────────

/// An equi-join on the 36-char agent UUID matches the right rows and carries
/// long fields (UUID, address) onto the joined output — proving long-string
/// equality keying through the Combine path.
#[test]
fn combine_join_on_long_uuid_key_carries_long_fields() {
    let address = "1600 Pennsylvania Avenue NW, Washington, DC 20500";
    assert_all_heap_backed(&[
        TICKET_UUID_1,
        TICKET_UUID_2,
        AGENT_UUID_EAST,
        AGENT_UUID_WEST,
        address,
    ]);

    let yaml = r#"
pipeline:
  name: long_field_combine
error_handling:
  strategy: continue
nodes:
  - type: source
    name: tickets
    config:
      name: tickets
      type: csv
      path: tickets.csv
      schema:
        - { name: ticket_uuid, type: string }
        - { name: agent_uuid, type: string }
        - { name: shipping_address, type: string }
  - type: source
    name: agents
    config:
      name: agents
      type: csv
      path: agents.csv
      schema:
        - { name: agent_uuid, type: string }
        - { name: agent_name, type: string }
  - type: combine
    name: enriched
    input:
      t: tickets
      a: agents
    config:
      where: "t.agent_uuid == a.agent_uuid"
      match: first
      on_miss: skip
      cxl: |
        emit ticket_uuid = t.ticket_uuid
        emit agent_name = a.agent_name
        emit shipping_address = t.shipping_address
      propagate_ck: driver
  - type: output
    name: out
    input: enriched
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

    // ticket 1 → EAST agent, ticket 2 → WEST agent. Both join keys are
    // 36-char UUIDs, so a correct match requires heap-backed-string equality.
    let tickets = format!(
        "ticket_uuid,agent_uuid,shipping_address\n\
         {TICKET_UUID_1},{AGENT_UUID_EAST},\"{address}\"\n\
         {TICKET_UUID_2},{AGENT_UUID_WEST},\"350 Fifth Avenue, New York, NY 10118\"\n"
    );
    let agents = format!(
        "agent_uuid,agent_name\n\
         {AGENT_UUID_EAST},Priya Raghunathan\n\
         {AGENT_UUID_WEST},Yuki Watanabe\n"
    );
    let (report, output) = run_pipeline(
        yaml,
        &[("tickets", &tickets), ("agents", &agents)],
        "out",
        "combine-long",
    );
    assert_eq!(report.counters.dlq_count, 0);
    assert_eq!(report.counters.ok_count, 2, "both tickets join to an agent");

    assert_eq!(
        sorted_body(&output),
        vec![
            format!("{TICKET_UUID_1},Priya Raghunathan,\"{address}\""),
            format!("{TICKET_UUID_2},Yuki Watanabe,\"350 Fifth Avenue, New York, NY 10118\""),
        ],
        "the long-UUID join must pair each ticket with the agent whose 36-char UUID matches"
    );
}

// ── Spill round-trip: long-field-heavy data forced over the budget ─────────

/// Under a 1 MiB spill budget, a long-field-heavy buffer is forced to disk
/// and reloaded; the run still emits every row with its long values intact.
/// This exercises the Arc-backed `SmolStr` postcard serde path through a real
/// spill commit + reload, not just the in-memory clone path.
#[test]
fn spill_roundtrip_preserves_long_fields() {
    // RSS-gated spill predicate: without an RSS reading the buffer stays in
    // memory and `cumulative_spill_bytes` never moves, so skip rather than
    // assert a false negative — matching the existing soft-spill coverage.
    if clinker_exec::pipeline::memory::rss_bytes().is_none() {
        return;
    }

    let yaml = r#"
pipeline:
  name: long_field_spill
  memory: { limit: "1M", backpressure: spill }
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: row_uuid, type: string }
        - { name: note, type: string }
  - type: route
    name: fan
    input: src
    config:
      mode: exclusive
      conditions:
        keep: "true"
      default: drop
  - type: output
    name: out
    input: fan.keep
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

    // Each row carries a 36-char UUID plus a long free-text note (>23B), so
    // the Source's node_buffer is dominated by Arc-backed strings. 4 000 rows
    // charge well past the 819 KiB soft floor but stay under the 1 MiB hard
    // limit, so the producer-side buffer spills and reloads.
    const ROWS: usize = 4_000;
    let note = "free-text customer note that comfortably exceeds the inline boundary";
    assert!(note.len() > 23);
    let mut csv = String::from("row_uuid,note\n");
    for i in 0..ROWS {
        // A per-row unique 36-char UUID keeps every value on the heap.
        csv.push_str(&format!(
            "{:08x}-8a6e-4d2f-9c17-0b5e8a1d6e42,{note} #{i}\n",
            i as u32
        ));
    }

    let (report, output) = run_pipeline(yaml, &[("src", &csv)], "out", "spill-long");
    assert_eq!(report.counters.dlq_count, 0);
    assert_eq!(report.counters.total_count as usize, ROWS);
    assert!(
        report.cumulative_spill_bytes > 0,
        "the long-field-heavy buffer must spill at least once under a 1 MiB budget; \
         cumulative_spill_bytes = {}",
        report.cumulative_spill_bytes
    );

    // Correctness after spill+reload: every row survived with its long values
    // byte-intact. Spot-check the first and last UUID/note pair, and confirm
    // the full row count round-trips.
    let body: Vec<&str> = output.lines().skip(1).filter(|l| !l.is_empty()).collect();
    assert_eq!(body.len(), ROWS, "every spilled row must reload");
    assert!(
        body.iter()
            .any(|l| l.contains("00000000-8a6e-4d2f-9c17-0b5e8a1d6e42")
                && l.contains(&format!("{note} #0"))),
        "the first row's long UUID and note must survive the spill round-trip"
    );
    assert!(
        body.iter().any(|l| l.contains(&format!(
            "{:08x}-8a6e-4d2f-9c17-0b5e8a1d6e42",
            (ROWS - 1) as u32
        )) && l.contains(&format!("{note} #{}", ROWS - 1))),
        "the last row's long UUID and note must survive the spill round-trip"
    );
}

// ── Output: long fields written without truncation (CSV + JSON) ────────────

/// A 36-char UUID, a long address, and a long free-text comment are written
/// verbatim to CSV — no truncation, exact-byte fidelity.
#[test]
fn output_csv_writes_long_fields_without_truncation() {
    let address = "233 South Wacker Drive, Chicago, IL 60606";
    // A comma in the comment forces CSV quoting, so the assertion also covers
    // the writer's quote path at full long-field width.
    let comment =
        "The mobile app crashes on launch after the latest update, reproduced on two devices.";
    assert_all_heap_backed(&[TICKET_UUID_1, address, comment]);

    let yaml = r#"
pipeline:
  name: long_field_csv_out
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: ticket_uuid, type: string }
        - { name: shipping_address, type: string }
        - { name: comment, type: string }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

    let csv = format!(
        "ticket_uuid,shipping_address,comment\n{TICKET_UUID_1},\"{address}\",\"{comment}\"\n"
    );
    let (report, output) = run_pipeline(yaml, &[("src", &csv)], "out", "csv-out-long");
    assert_eq!(report.counters.dlq_count, 0);
    assert_eq!(report.counters.ok_count, 1);

    let body: Vec<&str> = output.lines().skip(1).filter(|l| !l.is_empty()).collect();
    assert_eq!(body.len(), 1);
    assert_eq!(
        body[0],
        format!("{TICKET_UUID_1},\"{address}\",\"{comment}\""),
        "CSV output must reproduce the long fields verbatim, no truncation"
    );
}

/// The same long fields round-trip through the JSON array writer with exact
/// values — parsed structurally so the assertion is robust to key ordering.
#[test]
fn output_json_writes_long_fields_without_truncation() {
    let address = "233 South Wacker Drive, Chicago, IL 60606";
    let comment =
        "The mobile app crashes on launch after the latest update, reproduced on two devices.";
    assert_all_heap_backed(&[TICKET_UUID_1, address, comment]);

    let yaml = r#"
pipeline:
  name: long_field_json_out
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: ticket_uuid, type: string }
        - { name: shipping_address, type: string }
        - { name: comment, type: string }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: json
      path: out.json
      include_unmapped: true
"#;

    let csv = format!(
        "ticket_uuid,shipping_address,comment\n{TICKET_UUID_1},\"{address}\",\"{comment}\"\n"
    );
    let (report, output) = run_pipeline(yaml, &[("src", &csv)], "out", "json-out-long");
    assert_eq!(report.counters.dlq_count, 0);
    assert_eq!(report.counters.ok_count, 1);

    let parsed: serde_json::Value =
        serde_json::from_str(&output).expect("JSON output must parse as a value");
    let rows = parsed.as_array().expect("default JSON output is an array");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(
        row["ticket_uuid"],
        serde_json::json!(TICKET_UUID_1),
        "the 36-char UUID must round-trip through JSON verbatim"
    );
    assert_eq!(
        row["shipping_address"],
        serde_json::json!(address),
        "the long address must round-trip through JSON verbatim"
    );
    assert_eq!(
        row["comment"],
        serde_json::json!(comment),
        "the long comment must round-trip through JSON verbatim"
    );
}
