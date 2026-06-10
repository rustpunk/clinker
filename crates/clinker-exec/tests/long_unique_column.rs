//! End-to-end coverage for the `long_unique` per-column storage hint.
//!
//! A column flagged `long_unique` in the source `schema:` block stores its
//! string values in the header-free `Box`-backed `FieldStr` arm rather than
//! the default inline-or-`Arc`-shared arm. The flag is a footprint
//! optimization only: a value in the unique arm must compare, group, join, and
//! sort identically to an equal-content value in any other arm. These tests
//! drive that equivalence through real operators — group-by, distinct, and a
//! cross-source join where one side's key is unique-arm and the other side's
//! is default-arm — and confirm the on-disk output encoding is unaffected.

use std::collections::HashMap;
use std::io::{Cursor, Write};

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams, SourceReaders};
use clinker_plan::config::{CompileContext, parse_config};

/// 36-char UUIDs — past the 23-byte inline boundary, so the default arm is
/// `Arc`-backed and the flagged arm is `Box`-backed (both heap, no inline).
const UUID_A: &str = "3f29c4b1-8a6e-4d2f-9c17-0b5e8a1d6e42";
const UUID_B: &str = "a7e3f10c-5b29-4e6d-8f41-2c9a0d7b3e15";
const UUID_C: &str = "c2b8d6a4-1f37-4a90-b5e2-8d0c4f6a9b71";

fn assert_long(values: &[&str]) {
    for v in values {
        assert!(
            v.len() > 23,
            "fixture value {v:?} is {} bytes — must exceed the 23-byte inline \
             boundary so the unique vs shared arm distinction is exercised",
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

fn sorted_body(output: &str) -> Vec<String> {
    let mut lines: Vec<String> = output.lines().skip(1).map(str::to_string).collect();
    lines.sort();
    lines
}

/// Grouping by a `long_unique`-flagged UUID column produces one row per
/// distinct UUID with correct counts. Proves the unique-arm value round-trips
/// through `GroupByKey::Str` with content equality intact — the storage arm is
/// invisible to grouping.
#[test]
fn group_by_long_unique_key_exact_counts() {
    assert_long(&[UUID_A, UUID_B]);

    let yaml = r#"
pipeline:
  name: long_unique_group_by
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: agent_uuid, type: string, long_unique: true }
        - { name: region, type: string }
  - type: aggregate
    name: agg
    input: src
    config:
      group_by: [agent_uuid]
      cxl: |
        emit agent_uuid = agent_uuid
        emit ticket_count = count(*)
  - type: output
    name: out
    input: agg
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

    // UUID_A appears 3x, UUID_B 2x. Grouping on the unique-arm key must
    // collapse equal UUIDs together exactly as the default arm would.
    let csv = format!(
        "agent_uuid,region\n\
         {UUID_A},east\n\
         {UUID_B},west\n\
         {UUID_A},east\n\
         {UUID_A},east\n\
         {UUID_B},west\n"
    );
    let (report, output) = run_pipeline(yaml, &[("src", &csv)], "out", "lu-group");
    assert_eq!(report.counters.dlq_count, 0);
    assert_eq!(report.counters.ok_count, 2, "two distinct agent UUIDs");

    assert_eq!(
        sorted_body(&output),
        vec![format!("{UUID_A},3"), format!("{UUID_B},2")],
        "grouping on a long_unique key must produce the same counts as any arm"
    );
}

/// A cross-source join where the build side's key column is flagged
/// `long_unique` and the probe side's identical key column is the default arm.
/// Equal UUID content from different arms must match — the load-bearing safety
/// guarantee that the arm is a footprint hint, not a value domain. Run through
/// an actual Combine equi-join.
#[test]
fn join_unique_arm_build_against_default_arm_probe() {
    assert_long(&[UUID_A, UUID_B, UUID_C]);

    let yaml = r#"
pipeline:
  name: long_unique_join
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
        - { name: ticket_id, type: string }
        - { name: agent_uuid, type: string }
  - type: source
    name: agents
    config:
      name: agents
      type: csv
      path: agents.csv
      schema:
        - { name: agent_uuid, type: string, long_unique: true }
        - { name: agent_name, type: string }
  - type: combine
    name: joined
    input:
      t: tickets
      a: agents
    config:
      where: "t.agent_uuid == a.agent_uuid"
      match: first
      on_miss: skip
      cxl: |
        emit ticket_id = t.ticket_id
        emit agent_uuid = t.agent_uuid
        emit agent_name = a.agent_name
      propagate_ck: driver
  - type: output
    name: out
    input: joined
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

    // agents.agent_uuid is unique-arm; tickets.agent_uuid is default-arm.
    // The join keys must match across arms for equal UUID content.
    let agents = format!(
        "agent_uuid,agent_name\n\
         {UUID_A},Ada\n\
         {UUID_B},Boris\n"
    );
    let tickets = format!(
        "ticket_id,agent_uuid\n\
         T1,{UUID_A}\n\
         T2,{UUID_B}\n\
         T3,{UUID_A}\n\
         T4,{UUID_C}\n"
    );
    let (report, output) = run_pipeline(
        yaml,
        &[("tickets", &tickets), ("agents", &agents)],
        "out",
        "lu-join",
    );
    assert_eq!(report.counters.dlq_count, 0);

    // T4 (UUID_C) has no matching agent and is skipped (`on_miss: skip`). The
    // three matching tickets pair with their agent across the arm boundary.
    assert_eq!(
        sorted_body(&output),
        vec![
            format!("T1,{UUID_A},Ada"),
            format!("T2,{UUID_B},Boris"),
            format!("T3,{UUID_A},Ada"),
        ],
        "a default-arm probe key must match a unique-arm build key on equal content"
    );
}

/// Distinct over a `long_unique` column deduplicates by content, not arm:
/// repeated UUIDs collapse to one row each. Proves distinct keying is
/// arm-agnostic end to end.
#[test]
fn distinct_long_unique_dedupes_by_content() {
    assert_long(&[UUID_A, UUID_B]);

    let yaml = r#"
pipeline:
  name: long_unique_distinct
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: agent_uuid, type: string, long_unique: true }
  - type: transform
    name: dedup
    input: src
    config:
      cxl: |
        emit agent_uuid = agent_uuid
        distinct by agent_uuid
  - type: output
    name: out
    input: dedup
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

    let csv = format!(
        "agent_uuid\n\
         {UUID_A}\n\
         {UUID_B}\n\
         {UUID_A}\n\
         {UUID_A}\n\
         {UUID_B}\n"
    );
    let (report, output) = run_pipeline(yaml, &[("src", &csv)], "out", "lu-distinct");
    assert_eq!(report.counters.dlq_count, 0);

    assert_eq!(
        sorted_body(&output),
        vec![UUID_A.to_string(), UUID_B.to_string()],
        "distinct over a long_unique column must dedupe by content"
    );
}

/// The unique-arm value survives the CSV output path byte-exact — the arm is
/// invisible to the writer, which serializes the underlying `str`.
#[test]
fn long_unique_value_round_trips_to_output_byte_exact() {
    let note = "a free-text comment that is comfortably past the inline boundary";
    assert!(note.len() > 23);

    let yaml = r#"
pipeline:
  name: long_unique_passthrough
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: note, type: string, long_unique: true }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

    let csv = format!("note\n{note}\n");
    let (report, output) = run_pipeline(yaml, &[("src", &csv)], "out", "lu-passthrough");
    assert_eq!(report.counters.dlq_count, 0);
    assert_eq!(sorted_body(&output), vec![note.to_string()]);
}
