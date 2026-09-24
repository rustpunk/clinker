//! Whole-document dead-lettering across more than one Sink.
//!
//! Under `dlq_granularity: document` a document marked failed stays failed
//! for the whole run: no Sink writes any of its records, however many Sinks
//! read them, and no Sink runs before an operator that could still condemn
//! a document it holds. These tests pin that guarantee on the two shapes
//! that broke it: two Sinks reading one Transform, and a Sink whose sibling
//! branch condemns the document.
//!
//! Every pipeline reads one CSV Source `events` over two in-memory files.
//! `a.csv` holds `a1,1`, `a2,bad`, `a3,3`, so an integer coercion fails on
//! `a2` and condemns document `a.csv`; `b.csv` is clean.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_core_types::dlq::DlqErrorCategory;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};
use clinker_record::PipelineCounters;

#[path = "common/dlq_sink.rs"]
mod dlq_sink;

use dlq_sink::{CollectingDlqSink, DlqRow};

const A_CSV: &str = "id,value\na1,1\na2,bad\na3,3\n";
const B_CSV: &str = "id,value\nb1,1\nb2,2\n";

/// The pipeline preamble every test shares: continue-on-error with a
/// dead-letter file, and one document-granularity CSV Source `events`.
/// `dlq` is the body of the `dlq:` mapping.
fn preamble(dlq: &str) -> String {
    format!(
        r#"
pipeline: {{ name: doc_dlq_fanout }}
error_handling: {{ strategy: continue, dlq: {{ {dlq} }} }}
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      glob: ./*.csv
      dlq_granularity: document
      files: {{ on_no_match: skip }}
      schema:
        - {{ name: id, type: string }}
        - {{ name: value, type: string }}
"#
    )
}

fn sink(name: &str, input: &str) -> String {
    format!(
        r#"  - type: sink
    name: {name}
    input: {input}
    config: {{ name: {name}, type: csv, path: {name}.csv, include_unmapped: true }}
"#
    )
}

fn transform(name: &str, input: &str, cxl: &str) -> String {
    format!(
        r#"  - type: transform
    name: {name}
    input: {input}
    config:
      cxl: |
{cxl}
"#
    )
}

const VALIDATE_CXL: &str = "        emit id = id\n        emit val = value.to_int()";
const PASS_CXL: &str = "        emit id = id\n        emit val = value";

/// Shape (a): one Transform `validate` feeding two Sinks `out1` and `out2`.
fn two_sinks_one_transform(dlq: &str) -> String {
    let mut yaml = preamble(dlq);
    yaml.push_str(&transform("validate", "events", VALIDATE_CXL));
    yaml.push_str(&sink("out1", "validate"));
    yaml.push_str(&sink("out2", "validate"));
    yaml
}

/// Shape (b): two sibling branches off `events`. The condemning branch
/// (`t2 -> out2`) is declared before the clean one (`t1 -> out1`); in this
/// declaration order the topological sort visits `t1` and `out1` first, so
/// without the Sink barrier `out1` publishes document `a.csv` before `t2`
/// condemns it.
fn sibling_branches() -> String {
    let mut yaml = preamble("path: rejected.csv");
    yaml.push_str(&transform("t2", "events", VALIDATE_CXL));
    yaml.push_str(&sink("out2", "t2"));
    yaml.push_str(&transform("t1", "events", PASS_CXL));
    yaml.push_str(&sink("out1", "t1"));
    yaml
}

/// Compile and run `yaml` over `files`, each an in-memory document, with
/// one in-memory writer per Sink name in `sinks`. Returns the run counters,
/// the dead-letter rows, and each Sink's body lines (header skipped,
/// sorted).
fn run_fanout(
    yaml: &str,
    files: &[(&str, &str)],
    sinks: &[&str],
) -> (PipelineCounters, Vec<DlqRow>, BTreeMap<String, Vec<String>>) {
    let config = parse_config(yaml).expect("parse fan-out pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile fan-out pipeline");

    let slots: Vec<FileSlot> = files
        .iter()
        .map(|(name, body)| {
            FileSlot::new(
                PathBuf::from(*name),
                Box::new(Cursor::new(body.as_bytes().to_vec())),
            )
        })
        .collect();
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "events".to_string(),
        clinker_exec::executor::SourceInput::Files(slots),
    )]);

    let buffers: BTreeMap<String, SharedBuffer> = sinks
        .iter()
        .map(|name| (name.to_string(), SharedBuffer::new()))
        .collect();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = buffers
        .iter()
        .map(|(name, buf)| {
            (
                name.clone(),
                Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
            )
        })
        .collect();

    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    let dlq = CollectingDlqSink::new();
    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        dlq_sink::registry(writers, &dlq),
        &params,
    )
    .expect("run fan-out pipeline");

    let bodies = buffers
        .into_iter()
        .map(|(name, buf)| {
            let mut body: Vec<String> =
                buf.as_string().lines().skip(1).map(str::to_owned).collect();
            body.sort();
            (name, body)
        })
        .collect();
    (report.counters, dlq.rows(), bodies)
}

/// The first cell of every body line: the record's `id`.
fn ids(body: &[String]) -> Vec<&str> {
    body.iter()
        .map(|line| line.split(',').next().unwrap_or_default())
        .collect()
}

/// The dead-letter rows hold exactly the ids `expected`, each once; exactly
/// one of them, `a2`, is the trigger and keeps its own category; every
/// other row is a `document_rejected` collateral paired with the trigger
/// through `_cxl_dlq_trigger_id`.
fn assert_rejected_once(rows: &[DlqRow], expected: &[&str]) {
    let mut seen: Vec<&str> = rows
        .iter()
        .map(|row| row.field("id").expect("every dead-letter row carries id"))
        .collect();
    seen.sort_unstable();
    let mut want = expected.to_vec();
    want.sort_unstable();
    assert_eq!(
        seen, want,
        "the dead-letter output holds each record of the rejected document exactly once"
    );

    let triggers: Vec<&DlqRow> = rows.iter().filter(|row| row.trigger()).collect();
    assert_eq!(triggers.len(), 1, "exactly one trigger row");
    let trigger = triggers[0];
    assert_eq!(trigger.field("id"), Some("a2"), "a2 is the trigger");
    assert_ne!(
        trigger.category(),
        Some(DlqErrorCategory::DocumentRejected.as_str()),
        "the trigger keeps its own failure category"
    );
    let trigger_id = trigger
        .field("_cxl_dlq_id")
        .expect("every dead-letter row carries _cxl_dlq_id");

    for row in rows.iter().filter(|row| !row.trigger()) {
        assert_eq!(
            row.category(),
            Some(DlqErrorCategory::DocumentRejected.as_str()),
            "every collateral carries the document_rejected category"
        );
        assert_eq!(
            row.field("_cxl_dlq_trigger_id"),
            Some(trigger_id),
            "every collateral names the trigger's _cxl_dlq_id"
        );
    }
}

#[test]
fn rejected_document_publishes_nothing_at_any_sink() {
    let yaml = two_sinks_one_transform("path: rejected.csv");
    let (counters, rows, bodies) = run_fanout(
        &yaml,
        &[("a.csv", A_CSV), ("b.csv", B_CSV)],
        &["out1", "out2"],
    );

    for name in ["out1", "out2"] {
        assert_eq!(
            ids(&bodies[name]),
            ["b1", "b2"],
            "Sink {name} writes only the clean document"
        );
    }
    assert_rejected_once(&rows, &["a1", "a2", "a3"]);
    assert_eq!(counters.ok_count, 2, "ok_count counts no rejected row");
    assert_eq!(counters.dlq_count, 3, "each rejected row is counted once");
}

#[test]
fn sibling_branch_failure_condemns_before_any_sink_writes() {
    let (counters, rows, bodies) = run_fanout(
        &sibling_branches(),
        &[("a.csv", A_CSV), ("b.csv", B_CSV)],
        &["out1", "out2"],
    );

    for name in ["out1", "out2"] {
        assert_eq!(
            ids(&bodies[name]),
            ["b1", "b2"],
            "Sink {name} writes only the clean document"
        );
    }
    assert_rejected_once(&rows, &["a1", "a2", "a3"]);
    assert_eq!(counters.ok_count, 2, "ok_count counts no rejected row");
    assert_eq!(counters.dlq_count, 3, "each rejected row is counted once");
}

#[test]
fn dlq_count_counts_each_row_once_across_sinks() {
    // 3 of 6 rows are dead-lettered, a rate of 0.5 under the 0.6 ceiling. A
    // dead-letter entry per Sink copy would reach 6 of 6 and trip the rate.
    let yaml = two_sinks_one_transform("path: rejected.csv, min_records: 1, max_rate: 0.6");
    let (counters, rows, _) = run_fanout(
        &yaml,
        &[("a.csv", A_CSV), ("b.csv", "id,value\nb1,1\nb2,2\nb3,3\n")],
        &["out1", "out2"],
    );

    assert_eq!(counters.dlq_count, 3, "each rejected row is counted once");
    assert_eq!(counters.ok_count, 3, "ok_count counts no rejected row");
    let distinct: BTreeSet<(String, u64)> = rows
        .iter()
        .map(|row| (row.source_name().to_owned(), row.source_row()))
        .collect();
    assert_eq!(
        distinct.len(),
        3,
        "the dead-letter rows name three distinct source rows"
    );
}

/// `--explain` lists the nodes in the order the run dispatches them, so
/// under document granularity it lists every Sink after every operator.
#[test]
fn explain_lists_every_sink_after_every_operator() {
    let yaml = sibling_branches();
    let config = parse_config(&yaml).expect("parse fan-out pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile fan-out pipeline");
    let text = plan.dag().explain_text(&config);

    let line_of = |needle: &str| {
        text.lines()
            .position(|line| line.contains(needle))
            .unwrap_or_else(|| panic!("explain lists {needle:?}:\n{text}"))
    };
    let t2 = line_of("transform.t2:");
    assert!(line_of("sink.out1:") > t2, "sink.out1 follows transform.t2");
    assert!(line_of("sink.out2:") > t2, "sink.out2 follows transform.t2");
    insta::assert_snapshot!("explain_sinks_after_operators", text);
}
