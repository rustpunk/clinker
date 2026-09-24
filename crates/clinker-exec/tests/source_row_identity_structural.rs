//! End-to-end identity coverage for structural document and reshape carriers.
//!
//! These fixtures terminate otherwise-successful paths in observable
//! dead-letter rows so assertions can compare the exact source identity (source
//! name and row ordinal) without adding a test-only executor hook.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{
    ExecutionReport, PipelineExecutor, PipelineRunParams, SourceReaders, SourceRowId,
};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::plan::CompiledPlan;
use clinker_plan::plan::execution::PlanNode;

#[path = "common/dlq_sink.rs"]
mod dlq_sink;

use dlq_sink::{CollectingDlqSink, DlqRow};

const ISA: &str = "ISA*00*          *00*          *ZZ*SENDER         \
    *ZZ*RECEIVER       *240101*1200*U*00401*000000001*0*P*:~";

fn compile(yaml: &str) -> CompiledPlan {
    parse_config(yaml)
        .expect("structural identity fixture must parse")
        .compile(&CompileContext::default())
        .expect("structural identity fixture must compile")
}

fn source_identity(plan: &CompiledPlan, name: &str, ordinal: u64) -> SourceRowId {
    let source = plan
        .dag()
        .graph
        .node_weights()
        .find(|node| matches!(node, PlanNode::Source { name: node_name, .. } if node_name == name))
        .unwrap_or_else(|| panic!("missing Source {name:?}"));
    SourceRowId::new(source.id(), ordinal)
}

/// Run `plan` to completion, returning its report and the dead-letter rows
/// the executor wrote.
fn run(
    plan: &CompiledPlan,
    readers: SourceReaders,
    outputs: &[&str],
) -> (ExecutionReport, Vec<DlqRow>) {
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = outputs
        .iter()
        .map(|name| {
            (
                (*name).to_string(),
                Box::new(SharedBuffer::new()) as Box<dyn std::io::Write + Send>,
            )
        })
        .collect();
    let params = PipelineRunParams {
        execution_id: "source-row-structural".to_string(),
        batch_id: "source-row-structural".to_string(),
        ..Default::default()
    };
    let sink = CollectingDlqSink::new();
    let report = PipelineExecutor::run_plan_with_readers_writers(
        plan,
        readers,
        dlq_sink::registry(writers, &sink),
        &params,
    )
    .expect("structural identity fixture must complete under continue strategy");
    (report, sink.rows())
}

/// A dead-letter row's source identity: its source name and row ordinal.
fn row_identity(row: &DlqRow) -> (String, u64) {
    (row.source_name().to_string(), row.source_row())
}

#[test]
fn envelope_concat_keeps_same_ordinal_sources_distinct() {
    let plan = compile(
        r#"
pipeline:
  name: envelope_identity
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
  - type: merge
    name: merged
    inputs: [src_a, src_b]
    config: { mode: concat }
  - type: envelope
    name: one_document
    body: merged
    config: { strategy: concat }
  - type: transform
    name: observe_identity
    input: one_document
    config:
      cxl: "emit failure = 1 / 0"
  - type: sink
    name: out
    input: observe_identity
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );
    let expected = [
        source_identity(&plan, "src_a", 1),
        source_identity(&plan, "src_b", 1),
    ];
    let readers = HashMap::from([
        (
            "src_a".to_string(),
            clinker_exec::executor::single_file_reader(
                "a.csv",
                Box::new(Cursor::new(b"id\n10\n".to_vec())),
            ),
        ),
        (
            "src_b".to_string(),
            clinker_exec::executor::single_file_reader(
                "b.csv",
                Box::new(Cursor::new(b"id\n20\n".to_vec())),
            ),
        ),
    ]);

    let (_, rows) = run(&plan, readers, &["out"]);
    let observed: Vec<_> = rows.iter().map(row_identity).collect();

    assert_eq!(
        observed,
        vec![
            ("src_a".to_string(), expected[0].ordinal()),
            ("src_b".to_string(), expected[1].ordinal()),
        ]
    );
    assert_eq!(expected[0].ordinal(), expected[1].ordinal());
    assert_ne!(expected[0].source(), expected[1].source());
}

#[test]
fn envelope_structural_reject_uses_first_body_representative_identity() {
    let plan = compile(
        r#"
pipeline:
  name: envelope_structural_identity
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: x12
      glob: ./*.x12
      dlq_granularity: document
      schema:
        - { name: seg_id, type: string }
        - { name: set_ref, type: string }
        - { name: e01, type: string }
  - type: envelope
    name: one_document
    body: interchange
    config: { strategy: concat }
  - type: sink
    name: out
    input: one_document
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );
    let fixture = format!(
        "{ISA}{}",
        "GS*PO*SENDER*RECEIVER*20240101*1200*1*X*004010~\
         ST*850*0001~\
         BEG*00*NE*PO12345**20240101~\
         PO1*1*10*EA*9.99~\
         SE*99*0001~\
         GE*1*1~\
         IEA*1*000000001~"
    );
    let readers = HashMap::from([(
        "interchange".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![FileSlot::new(
            PathBuf::from("po.x12"),
            Box::new(Cursor::new(fixture.into_bytes())),
        )]),
    )]);

    let (_, rows) = run(&plan, readers, &["out"]);
    let trigger = rows
        .iter()
        .find(|row| row.trigger())
        .expect("malformed document has one structural trigger");

    assert_eq!(
        row_identity(trigger),
        (
            "interchange".to_string(),
            source_identity(&plan, "interchange", 1).ordinal()
        )
    );
    assert_eq!(trigger.field("seg_id"), Some("ST"));
    assert_eq!(trigger.source_file(), "po.x12");
}

fn run_reshape_identity(
    memory_limit: &str,
) -> (ExecutionReport, Vec<DlqRow>, SourceRowId, SourceRowId) {
    let plan = compile(&format!(
        r#"
pipeline:
  name: reshape_identity
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - {{ name: account, type: string }}
        - {{ name: rank, type: int }}
        - {{ name: tag, type: string }}
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - {{ name: account, type: string }}
        - {{ name: rank, type: int }}
        - {{ name: tag, type: string }}
  - type: merge
    name: merged
    inputs: [src_b, src_a]
    config: {{ mode: concat }}
  - type: reshape
    name: backfill
    input: merged
    config:
      partition_by: [account]
      order_by:
        - {{ field: rank, order: asc }}
      rules:
        - name: synthesize_first
          when: "tag == 'b-000'"
          mutate:
            set:
              tag: "tag"
          synthesize:
            copy_from: trigger
            overrides:
              tag: "'synthetic'"
  - type: transform
    name: observe_identity
    input: backfill
    config:
      cxl: "emit failure = 1 / 0"
  - type: sink
    name: out
    input: observe_identity
    config:
      name: out
      type: csv
      path: out.csv
"#,
    ));
    let expected_a = source_identity(&plan, "src_a", 1);
    let expected_b = source_identity(&plan, "src_b", 1);
    let mut csv_a = String::from("account,rank,tag\n");
    let mut csv_b = String::from("account,rank,tag\n");
    for ordinal in 0..50 {
        csv_a.push_str(&format!("X,0,a-{ordinal:03}\n"));
        csv_b.push_str(&format!("X,0,b-{ordinal:03}\n"));
    }
    let readers = HashMap::from([
        (
            "src_a".to_string(),
            clinker_exec::executor::single_file_reader(
                "a.csv",
                Box::new(Cursor::new(csv_a.into_bytes())),
            ),
        ),
        (
            "src_b".to_string(),
            clinker_exec::executor::single_file_reader(
                "b.csv",
                Box::new(Cursor::new(csv_b.into_bytes())),
            ),
        ),
    ]);
    let (report, rows) = run(&plan, readers, &["out"]);
    (report, rows, expected_a, expected_b)
}

fn reshape_observed_identity(rows: &[DlqRow]) -> Vec<(String, (String, u64))> {
    rows.iter()
        .map(|row| {
            let tag = match row.field("tag") {
                Some(tag) if !tag.is_empty() => tag.to_string(),
                other => panic!("reshape output row must carry a string tag, got {other:?}"),
            };
            (tag, row_identity(row))
        })
        .collect()
}

/// A typed source identity in the form a dead-letter row carries it.
fn identity_cell(name: &str, id: SourceRowId) -> (String, u64) {
    (name.to_string(), id.ordinal())
}

#[test]
fn reshape_resident_and_spilled_paths_preserve_pairing_and_authored_order() {
    let (spilled, spilled_dlq, expected_a, expected_b) = run_reshape_identity("48K");
    let (resident, resident_dlq, resident_a, resident_b) = run_reshape_identity("512M");

    assert!(
        spilled.cumulative_spill_bytes > 0,
        "the constrained run must exercise reshape spill"
    );
    assert_eq!(resident.cumulative_spill_bytes, 0);
    assert_eq!((expected_a, expected_b), (resident_a, resident_b));

    let spilled_rows = reshape_observed_identity(&spilled_dlq);
    let resident_rows = reshape_observed_identity(&resident_dlq);
    assert_eq!(
        spilled_rows, resident_rows,
        "spill must be identity-transparent"
    );
    assert_eq!(spilled_rows.len(), 101, "100 originals plus one synthesis");

    assert_eq!(
        spilled_rows[0],
        ("b-000".to_string(), identity_cell("src_b", expected_b))
    );
    assert_eq!(
        spilled_rows[50],
        ("a-000".to_string(), identity_cell("src_a", expected_a))
    );
    assert_eq!(
        spilled_rows.last(),
        Some(&("synthetic".to_string(), identity_cell("src_b", expected_b))),
        "the synthesized row keeps its trigger's exact source identity"
    );
    assert_eq!(expected_a.ordinal(), expected_b.ordinal());
    assert_ne!(expected_a.source(), expected_b.source());
}

#[test]
fn reshape_carrier_has_no_scalar_identity_adapter() {
    let source = include_str!("../src/executor/reshape_dispatch.rs");

    assert!(
        !source.contains("fn push<R>"),
        "reshape admission must require SourceRowId directly"
    );
    assert!(
        !source.contains("row_num.into()"),
        "reshape must not reconstruct source identity from a scalar"
    );
}
