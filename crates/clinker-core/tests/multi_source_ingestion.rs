//! End-to-end coverage for the silent-corruption topologies fixed by
//! the multi-source `SourceStream` rewiring in sprint 1 of umbrella #50
//! (closes #47). Each topology constructs two CSV sources with
//! non-overlapping `id` ranges so that any leak of one source's records
//! into the other's chain is immediately visible in the output.
//!
//! Pre-fix surface (the bug, schema-match across sources):
//!
//! | Topology | Symptom |
//! |---|---|
//! | `[src_a, src_b] → merge → out` | `src_b` emits `src_a`'s records (#47) |
//! | `src_a → tfm → out_a`, `src_b → tfm → out_b` | `src_b`'s chain reads `src_a`'s records |
//! | `src_a → tfm → merge`, `src_b → tfm → merge` | same fallthrough at the Source dispatcher |
//! | `src_a → out_a`, `src_b → tfm → out_b` | non-primary single-output chain reads primary |
//!
//! The fix wires every non-primary Source through `SourceStream` in the
//! executor's preload pass, removing the silent fallthrough to
//! `ctx.all_records`. The Source dispatcher's third arm is now a
//! defense-in-depth `PipelineError::Internal` (loud, not silent) when
//! a non-primary Source somehow lacks preloaded records.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_core::config::{CompileContext, parse_config};
use clinker_core::executor::{PipelineExecutor, PipelineRunParams, SourceReaders};
use clinker_core::source::multi_file::FileSlot;

fn slot(name: &str, csv: &str) -> FileSlot {
    FileSlot::new(
        PathBuf::from(format!("{name}.csv")),
        Box::new(Cursor::new(csv.as_bytes().to_vec())),
    )
}

fn writer(buf: &SharedBuffer) -> Box<dyn std::io::Write + Send> {
    Box::new(buf.clone())
}

fn run_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

/// Topology 1 — `[src_a, src_b] → merge → out`. This is the canonical
/// shape from #47. Pre-fix, the output silently contained `src_a`'s
/// records twice; post-fix it carries the union of both sources.
#[test]
fn merge_direct_two_sources_yields_union() {
    let yaml = r#"
pipeline:
  name: merge_direct_two_sources
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: merge
    name: merged
    inputs: [src_a, src_b]
  - type: output
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            vec![slot("a", "id,tag\n1,a-one\n2,a-two\n3,a-three\n")],
        ),
        (
            "src_b".to_string(),
            vec![slot("b", "id,tag\n10,b-ten\n11,b-eleven\n12,b-twelve\n")],
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("multi-source merge must execute");
    // `report.counters.total_count` only tracks the primary stream
    // (a sub-#51 accounting gap that closes when the primary moves
    // onto `SourceStream` too); the output buffer is the
    // authoritative check.
    assert_eq!(report.counters.dlq_count, 0);

    let output = buf.as_string();
    let body: Vec<&str> = output.lines().skip(1).collect();
    assert_eq!(body.len(), 6, "expected 6 records, got:\n{output}");
    // src_a's ids 1,2,3 each appear exactly once with their original tags.
    for (id, tag) in [(1, "a-one"), (2, "a-two"), (3, "a-three")] {
        let needle = format!("{id},{tag}");
        let hits = body.iter().filter(|row| row.contains(&needle)).count();
        assert_eq!(
            hits, 1,
            "row `{needle}` should appear exactly once; got: {output}"
        );
    }
    // src_b's ids 10,11,12 each appear exactly once with their original tags.
    for (id, tag) in [(10, "b-ten"), (11, "b-eleven"), (12, "b-twelve")] {
        let needle = format!("{id},{tag}");
        let hits = body.iter().filter(|row| row.contains(&needle)).count();
        assert_eq!(
            hits, 1,
            "row `{needle}` should appear exactly once; got: {output}"
        );
    }
}

/// Topology 2 — `src_a → tfm_a → out_a`, `src_b → tfm_b → out_b`. The
/// two chains are wholly disjoint at the DAG level; the bug pre-fix
/// was that `src_b`'s dispatch silently emitted `src_a`'s records,
/// so `out_b` carried `src_a`'s data instead of `src_b`'s.
#[test]
fn disjoint_parallel_chains_dont_cross_streams() {
    let yaml = r#"
pipeline:
  name: disjoint_parallel
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: transform
    name: tfm_a
    input: src_a
    config:
      cxl: |
        emit id = id
        emit tag = tag
  - type: transform
    name: tfm_b
    input: src_b
    config:
      cxl: |
        emit id = id
        emit tag = tag
  - type: output
    name: out_a
    input: tfm_a
    config:
      name: out_a
      type: csv
      path: out_a.csv
  - type: output
    name: out_b
    input: tfm_b
    config:
      name: out_b
      type: csv
      path: out_b.csv
"#;
    let config = parse_config(yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            vec![slot("a", "id,tag\n1,a-one\n2,a-two\n3,a-three\n")],
        ),
        (
            "src_b".to_string(),
            vec![slot("b", "id,tag\n10,b-ten\n11,b-eleven\n12,b-twelve\n")],
        ),
    ]);
    let buf_a = SharedBuffer::new();
    let buf_b = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([
        ("out_a".to_string(), writer(&buf_a)),
        ("out_b".to_string(), writer(&buf_b)),
    ]);

    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
        .expect("disjoint parallel chains must execute");

    let out_a = buf_a.as_string();
    let out_b = buf_b.as_string();
    let body_a: Vec<&str> = out_a.lines().skip(1).collect();
    let body_b: Vec<&str> = out_b.lines().skip(1).collect();
    assert_eq!(body_a.len(), 3, "out_a expected 3 records, got:\n{out_a}");
    assert_eq!(body_b.len(), 3, "out_b expected 3 records, got:\n{out_b}");
    for needle in ["a-one", "a-two", "a-three"] {
        assert!(out_a.contains(needle), "out_a missing `{needle}`: {out_a}");
        assert!(
            !out_b.contains(needle),
            "out_b leaked src_a tag `{needle}` — silent corruption!\n{out_b}"
        );
    }
    for needle in ["b-ten", "b-eleven", "b-twelve"] {
        assert!(out_b.contains(needle), "out_b missing `{needle}`: {out_b}");
        assert!(
            !out_a.contains(needle),
            "out_a leaked src_b tag `{needle}` — silent corruption!\n{out_a}"
        );
    }
}

/// Topology 3 — `src_a → tfm_a → merge`, `src_b → tfm_b → merge`.
/// Each source flows through its own transform before fan-in. The
/// bug surface pre-fix was identical to topology 1: the second
/// Source's dispatch silently emitted the primary's records, so the
/// merge collapsed to two copies of `src_a`-derived rows. The
/// transforms also stamp an `origin` field so a regression would be
/// visible even if the ids overlap.
#[test]
fn chained_into_merge_preserves_per_source_data() {
    let yaml = r#"
pipeline:
  name: chained_into_merge
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: transform
    name: tfm_a
    input: src_a
    config:
      cxl: |
        emit id = id
        emit tag = tag
        emit origin = "A"
  - type: transform
    name: tfm_b
    input: src_b
    config:
      cxl: |
        emit id = id
        emit tag = tag
        emit origin = "B"
  - type: merge
    name: merged
    inputs: [tfm_a, tfm_b]
  - type: output
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            vec![slot("a", "id,tag\n1,a-one\n2,a-two\n3,a-three\n")],
        ),
        (
            "src_b".to_string(),
            vec![slot("b", "id,tag\n10,b-ten\n11,b-eleven\n12,b-twelve\n")],
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("chained-into-merge must execute");
    assert_eq!(report.counters.dlq_count, 0);

    let output = buf.as_string();
    // Output buffer is authoritative; `total_count` is primary-only
    // (sub-#51 accounting gap).
    assert_eq!(
        output.lines().skip(1).count(),
        6,
        "expected 6 merged records, got:\n{output}"
    );
    // Every row stamped origin=A should carry an `a-*` tag; every B
    // row should carry `b-*`. A silent leak would have a B-stamped
    // row paired with an `a-*` tag (or vice versa).
    for line in output.lines().skip(1) {
        if line.contains(",A,") || line.ends_with(",A") {
            assert!(
                line.contains("a-"),
                "origin=A row carries non-A tag — silent corruption: `{line}`\n{output}"
            );
        }
        if line.contains(",B,") || line.ends_with(",B") {
            assert!(
                line.contains("b-"),
                "origin=B row carries non-B tag — silent corruption: `{line}`\n{output}"
            );
        }
    }
    assert_eq!(
        output.matches("a-").count(),
        3,
        "expected 3 src_a-derived rows: {output}"
    );
    assert_eq!(
        output.matches("b-").count(),
        3,
        "expected 3 src_b-derived rows: {output}"
    );
}

/// Topology 4 — `src_a → out_a` (primary, identity passthrough),
/// `src_b → tfm → out_b` (non-primary single-output chain). Pre-fix,
/// `src_b`'s chain silently read the primary's records; post-fix the
/// preload's third pass ingests src_b and the dispatcher reads from
/// its own stream.
#[test]
fn non_primary_single_output_reads_own_records() {
    let yaml = r#"
pipeline:
  name: non_primary_single_output
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: transform
    name: stamp_b
    input: src_b
    config:
      cxl: |
        emit id = id
        emit tag = tag
  - type: output
    name: out_a
    input: src_a
    config:
      name: out_a
      type: csv
      path: out_a.csv
  - type: output
    name: out_b
    input: stamp_b
    config:
      name: out_b
      type: csv
      path: out_b.csv
"#;
    let config = parse_config(yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            vec![slot("a", "id,tag\n1,a-one\n2,a-two\n3,a-three\n")],
        ),
        (
            "src_b".to_string(),
            vec![slot("b", "id,tag\n10,b-ten\n11,b-eleven\n12,b-twelve\n")],
        ),
    ]);
    let buf_a = SharedBuffer::new();
    let buf_b = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([
        ("out_a".to_string(), writer(&buf_a)),
        ("out_b".to_string(), writer(&buf_b)),
    ]);

    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
        .expect("non-primary chain must execute");

    let out_a = buf_a.as_string();
    let out_b = buf_b.as_string();
    let body_b: Vec<&str> = out_b.lines().skip(1).collect();
    assert_eq!(body_b.len(), 3, "out_b expected 3 records, got:\n{out_b}");
    for needle in ["b-ten", "b-eleven", "b-twelve"] {
        assert!(out_b.contains(needle), "out_b missing `{needle}`: {out_b}");
    }
    for needle in ["a-one", "a-two", "a-three"] {
        assert!(
            !out_b.contains(needle),
            "out_b leaked src_a tag `{needle}` — silent corruption!\n{out_b}"
        );
        assert!(out_a.contains(needle), "out_a missing `{needle}`: {out_a}");
    }
}
