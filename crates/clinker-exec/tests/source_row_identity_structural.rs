//! End-to-end identity coverage for structural document and reshape carriers.
//!
//! These fixtures terminate otherwise-successful paths in observable DLQ
//! entries so assertions can compare the exact typed source identity without
//! adding a test-only executor hook.

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
use clinker_record::Value;

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

fn run(plan: &CompiledPlan, readers: SourceReaders, outputs: &[&str]) -> ExecutionReport {
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
    PipelineExecutor::run_plan_with_readers_writers(plan, readers, writers, &params)
        .expect("structural identity fixture must complete under continue strategy")
}

#[test]
fn envelope_concat_keeps_same_ordinal_sources_distinct() {
    let plan = compile(
        r#"
pipeline:
  name: envelope_identity
error_handling:
  strategy: continue
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
    let expected = vec![
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

    let report = run(&plan, readers, &["out"]);
    let observed: Vec<_> = report
        .dlq_entries
        .iter()
        .map(|entry| entry.source_row)
        .collect();

    assert_eq!(observed, expected);
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

    let report = run(&plan, readers, &["out"]);
    let trigger = report
        .dlq_entries
        .iter()
        .find(|entry| entry.trigger)
        .expect("malformed document has one structural trigger");

    assert_eq!(trigger.source_row, source_identity(&plan, "interchange", 1));
    assert_eq!(
        trigger.original_record.get("seg_id"),
        Some(&Value::from("ST"))
    );
    assert_eq!(
        trigger.original_record.doc_ctx().source_file().as_ref(),
        "po.x12"
    );
}

fn run_reshape_identity(memory_limit: &str) -> (ExecutionReport, SourceRowId, SourceRowId) {
    let plan = compile(&format!(
        r#"
pipeline:
  name: reshape_identity
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
error_handling:
  strategy: continue
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
    (run(&plan, readers, &["out"]), expected_a, expected_b)
}

fn reshape_observed_identity(report: &ExecutionReport) -> Vec<(String, SourceRowId)> {
    report
        .dlq_entries
        .iter()
        .map(|entry| {
            let tag = match entry.original_record.get("tag") {
                Some(Value::String(tag)) => tag.to_string(),
                other => panic!("reshape output row must carry a string tag, got {other:?}"),
            };
            (tag, entry.source_row)
        })
        .collect()
}

#[test]
fn reshape_resident_and_spilled_paths_preserve_pairing_and_authored_order() {
    let (spilled, expected_a, expected_b) = run_reshape_identity("48K");
    let (resident, resident_a, resident_b) = run_reshape_identity("512M");

    assert!(
        spilled.cumulative_spill_bytes > 0,
        "the constrained run must exercise reshape spill"
    );
    assert_eq!(resident.cumulative_spill_bytes, 0);
    assert_eq!((expected_a, expected_b), (resident_a, resident_b));

    let spilled_rows = reshape_observed_identity(&spilled);
    let resident_rows = reshape_observed_identity(&resident);
    assert_eq!(
        spilled_rows, resident_rows,
        "spill must be identity-transparent"
    );
    assert_eq!(spilled_rows.len(), 101, "100 originals plus one synthesis");

    assert_eq!(spilled_rows[0], ("b-000".to_string(), expected_b));
    assert_eq!(spilled_rows[50], ("a-000".to_string(), expected_a));
    assert_eq!(
        spilled_rows.last(),
        Some(&("synthetic".to_string(), expected_b)),
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
