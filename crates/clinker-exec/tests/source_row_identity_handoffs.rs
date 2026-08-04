//! End-to-end identity coverage for ordinary record-preserving, fan-out, and
//! ordering operators.
//!
//! The executor deliberately exposes source identity on DLQ entries. These
//! fixtures terminate otherwise-successful operator paths in a failing
//! Transform so the assertions can observe the exact [`SourceRowId`] that
//! arrived at the end of each path without adding a test-only runtime hook.

use std::collections::HashMap;
use std::io::Cursor;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{
    ExecutionReport, PipelineExecutor, PipelineRunParams, SourceReaders, SourceRowId,
};
use clinker_exec::pipeline::sort_buffer::{SortBuffer, SortedOutput};
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::config::{SortField, SortOrder};
use clinker_plan::plan::CompiledPlan;
use clinker_plan::plan::execution::PlanNode;
use clinker_plan::plan::{EntityRef, PlanNodeId};
use clinker_record::{Record, Schema, Value};

fn compile(yaml: &str) -> CompiledPlan {
    parse_config(yaml)
        .expect("identity handoff fixture must parse")
        .compile(&CompileContext::default())
        .expect("identity handoff fixture must compile")
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

fn readers(inputs: &[(&str, &str)]) -> SourceReaders {
    inputs
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
        .collect()
}

fn run(plan: &CompiledPlan, inputs: &[(&str, &str)], outputs: &[&str]) -> ExecutionReport {
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
        execution_id: "source-row-handoffs".to_string(),
        batch_id: "source-row-handoffs".to_string(),
        ..Default::default()
    };
    PipelineExecutor::run_plan_with_readers_writers(plan, readers(inputs), writers, &params)
        .expect("identity handoff fixture must complete under continue strategy")
}

#[test]
fn fanout_source_and_transform_keep_same_ordinal_sources_distinct() {
    let plan = compile(
        r#"
pipeline:
  name: source_transform_identity
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
  - type: transform
    name: transform_a
    input: src_a
    config:
      cxl: "emit id = id"
  - type: transform
    name: transform_b
    input: src_b
    config:
      cxl: "emit id = id"
  - type: merge
    name: merged
    inputs: [transform_a, transform_b]
  - type: transform
    name: observe_identity
    input: merged
    config:
      cxl: "emit failure = 1 / 0"
  - type: output
    name: out
    input: observe_identity
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );
    let expected_a = source_identity(&plan, "src_a", 1);
    let expected_b = source_identity(&plan, "src_b", 1);

    let report = run(
        &plan,
        &[("src_a", "id\n10\n"), ("src_b", "id\n20\n")],
        &["out"],
    );
    let observed: Vec<SourceRowId> = report
        .dlq_entries
        .iter()
        .map(|entry| entry.source_row)
        .collect();

    assert_eq!(observed, vec![expected_a, expected_b]);
    assert_ne!(expected_a, expected_b);
    assert_eq!(expected_a.ordinal(), expected_b.ordinal());
}

#[test]
fn fanout_route_copies_one_typed_identity_to_every_branch() {
    let plan = compile(
        r#"
pipeline:
  name: route_identity
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: id, type: int }
  - type: route
    name: duplicate
    input: src
    config:
      mode: inclusive
      conditions:
        left: "id > 0"
        right: "id > 0"
      default: left
  - type: transform
    name: fail_left
    input: duplicate.left
    config:
      cxl: "emit failure = 1 / 0"
  - type: transform
    name: fail_right
    input: duplicate.right
    config:
      cxl: "emit failure = 1 / 0"
  - type: output
    name: left_out
    input: fail_left
    config:
      name: left_out
      type: csv
      path: left.csv
  - type: output
    name: right_out
    input: fail_right
    config:
      name: right_out
      type: csv
      path: right.csv
"#,
    );
    let expected = source_identity(&plan, "src", 1);

    let report = run(&plan, &[("src", "id\n7\n")], &["left_out", "right_out"]);
    let observed: Vec<SourceRowId> = report
        .dlq_entries
        .iter()
        .map(|entry| entry.source_row)
        .collect();

    assert_eq!(observed, vec![expected, expected]);
}

#[test]
fn fanout_cull_kept_and_removed_ports_retain_exact_identity() {
    let plan = compile(
        r#"
pipeline:
  name: cull_identity
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: account, type: string }
        - { name: status, type: string }
  - type: cull
    name: cull
    input: src
    config:
      partition_by: [account]
      removed_to: removed
      rules:
        - name: error_group
          drop_group_when: "sum(if status == 'error' then 1 else 0) > 0"
  - type: transform
    name: fail_kept
    input: cull
    config:
      cxl: "emit failure = 1 / 0"
  - type: transform
    name: fail_removed
    input: cull.removed
    config:
      cxl: "emit failure = 1 / 0"
  - type: output
    name: kept_out
    input: fail_kept
    config:
      name: kept_out
      type: csv
      path: kept.csv
  - type: output
    name: removed_out
    input: fail_removed
    config:
      name: removed_out
      type: csv
      path: removed.csv
"#,
    );
    let removed = source_identity(&plan, "src", 1);
    let kept = source_identity(&plan, "src", 2);

    let report = run(
        &plan,
        &[("src", "account,status\nA,error\nB,ok\n")],
        &["kept_out", "removed_out"],
    );
    let mut observed: Vec<(String, SourceRowId)> = report
        .dlq_entries
        .iter()
        .map(|entry| {
            (
                entry
                    .original_record
                    .get("account")
                    .expect("account field")
                    .to_string(),
                entry.source_row,
            )
        })
        .collect();
    observed.sort_by(|left, right| left.0.cmp(&right.0));

    assert_eq!(
        observed,
        vec![("A".to_string(), removed), ("B".to_string(), kept)]
    );
}

#[test]
fn fanout_dispatch_has_no_scalar_cull_reconstruction_and_charges_typed_carriers() {
    let compact = |source: &str| source.split_whitespace().collect::<String>();
    let cull = compact(include_str!("../src/executor/cull_dispatch.rs"));
    let route = compact(include_str!("../src/executor/route_dispatch.rs"));
    let dispatch = compact(include_str!("../src/executor/dispatch.rs"));
    let node_buffer = compact(include_str!("../src/executor/node_buffer.rs"));

    assert!(
        !cull.contains("fnpush<R>") && !cull.contains("row_num.into()"),
        "Cull production admission must require SourceRowId directly"
    );
    assert!(route.contains("size_of::<(Record,crate::executor::stream_event::SourceRowId)>"));
    assert!(dispatch.contains("record_byte_cost(first.schema().column_count())"));
    assert!(node_buffer.contains("size_of::<(Record,SourceRowId)>()"));
    assert!(
        std::mem::size_of::<SourceRowId>() > std::mem::size_of::<u64>(),
        "the accounting regression matters only while the typed carrier is wider"
    );
}

fn fanout_spill_pipeline(memory_limit: &str) -> CompiledPlan {
    compile(&format!(
        r#"
pipeline:
  name: ordinary_handoff_spill
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - {{ name: id, type: int }}
        - {{ name: region, type: string }}
        - {{ name: payload, type: string }}
        - {{ name: value, type: int }}
        - {{ name: ts, type: int }}
  - type: transform
    name: passthrough
    input: src
    config:
      cxl: |
        emit id = id
        emit region = region
        emit payload = payload
        emit value = value
        emit ts = ts
  - type: route
    name: selected
    input: passthrough
    config:
      mode: exclusive
      conditions:
        yes: "id > 0"
      default: no
  - type: transform
    name: observe_identity
    input: selected.yes
    config:
      cxl: "emit failure = 1 / 0"
  - type: output
    name: out
    input: observe_identity
    config:
      name: out
      type: csv
      path: out.csv
"#
    ))
}

fn fanout_spill_csv(rows: u64) -> String {
    let mut csv = String::from("id,region,payload,value,ts\n");
    for ordinal in 1..=rows {
        csv.push_str(&format!(
            "{ordinal},a,{:0128},{ordinal},{ordinal}\n",
            ordinal
        ));
    }
    csv
}

#[test]
fn fanout_resident_and_node_buffer_spill_keep_identical_typed_membership() {
    if clinker_exec::pipeline::memory::rss_bytes().is_none() {
        return;
    }

    const ROWS: u64 = 2_000;
    let csv = fanout_spill_csv(ROWS);
    let resident_plan = fanout_spill_pipeline("1G");
    let spilled_plan = fanout_spill_pipeline("1M");
    let resident = run(&resident_plan, &[("src", &csv)], &["out"]);
    let spilled = run(&spilled_plan, &[("src", &csv)], &["out"]);

    let resident_membership: Vec<SourceRowId> = resident
        .dlq_entries
        .iter()
        .map(|entry| entry.source_row)
        .collect();
    let spilled_membership: Vec<SourceRowId> = spilled
        .dlq_entries
        .iter()
        .map(|entry| entry.source_row)
        .collect();

    assert_eq!(resident_membership, spilled_membership);
    assert_eq!(resident_membership.len(), ROWS as usize);
    assert_eq!(resident_membership[0].ordinal(), 1);
    assert_eq!(resident_membership.last().unwrap().ordinal(), ROWS);
    assert_eq!(resident.cumulative_spill_bytes, 0);
    assert!(
        spilled.cumulative_spill_bytes > 0,
        "the low-memory run must exercise a node-buffer spill"
    );
}

fn ordering_record(schema: &std::sync::Arc<Schema>, key: i64, label: &str) -> Record {
    Record::new(
        std::sync::Arc::clone(schema),
        vec![Value::Integer(key), Value::from(label)],
    )
}

fn ordering_sort_rows(force_spill: bool) -> Vec<(Record, SourceRowId)> {
    let schema = std::sync::Arc::new(Schema::new(vec!["key".into(), "label".into()]));
    let source_a = PlanNodeId::new(40);
    let source_b = PlanNodeId::new(41);
    let input = [
        (
            ordering_record(&schema, 2, "a"),
            SourceRowId::new(source_a, 1),
        ),
        (
            ordering_record(&schema, 1, "b"),
            SourceRowId::new(source_b, 1),
        ),
        (
            ordering_record(&schema, 1, "c"),
            SourceRowId::new(source_a, 2),
        ),
        (
            ordering_record(&schema, 3, "d"),
            SourceRowId::new(source_b, 2),
        ),
    ];
    let spill_dir = tempfile::tempdir().expect("sort spill directory");
    let mut buffer: SortBuffer<SourceRowId> = SortBuffer::new(
        vec![SortField {
            field: "key".to_string(),
            order: SortOrder::Asc,
            null_order: None,
        }],
        usize::MAX,
        Some(spill_dir.path().to_path_buf()),
        true,
        std::sync::Arc::clone(&schema),
    );
    for (record, identity) in input {
        buffer.push(record, identity);
    }

    if force_spill {
        buffer.sort_and_spill().expect("sort run spills");
    }
    let (sorted, _) = buffer.finish().expect("sort finishes");
    match sorted {
        SortedOutput::InMemory(rows) => rows,
        SortedOutput::Spilled(files) => files
            .into_iter()
            .flat_map(|file| {
                file.reader()
                    .expect("open sort spill")
                    .map(|pair| pair.expect("read sort spill pair"))
            })
            .collect(),
    }
}

#[test]
fn ordering_sort_keeps_identity_paired_in_resident_and_spill_paths() {
    let resident = ordering_sort_rows(false);
    let spilled = ordering_sort_rows(true);
    let pairing = |rows: &[(Record, SourceRowId)]| {
        rows.iter()
            .map(|(record, identity)| (record.get("label").expect("label").to_string(), *identity))
            .collect::<Vec<_>>()
    };
    let expected = vec![
        ("b".to_string(), SourceRowId::new(PlanNodeId::new(41), 1)),
        ("c".to_string(), SourceRowId::new(PlanNodeId::new(40), 2)),
        ("a".to_string(), SourceRowId::new(PlanNodeId::new(40), 1)),
        ("d".to_string(), SourceRowId::new(PlanNodeId::new(41), 2)),
    ];

    assert_eq!(pairing(&resident), expected);
    assert_eq!(pairing(&spilled), expected);
}

fn ordering_merge_plan(mode_config: &str) -> CompiledPlan {
    let config = if mode_config.is_empty() {
        String::new()
    } else {
        format!("    config:\n      {mode_config}\n")
    };
    compile(&format!(
        r#"
pipeline:
  name: merge_identity
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
        - {{ name: id, type: int }}
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - {{ name: id, type: int }}
  - type: merge
    name: merged
    inputs: [src_a, src_b]
{config}  - type: transform
    name: observe_identity
    input: merged
    config:
      cxl: "emit failure = 1 / 0"
  - type: output
    name: out
    input: observe_identity
    config:
      name: out
      type: csv
      path: out.csv
"#
    ))
}

fn ordering_merge_identities(mode_config: &str) -> (Vec<SourceRowId>, Vec<SourceRowId>) {
    let plan = ordering_merge_plan(mode_config);
    let expected = vec![
        source_identity(&plan, "src_a", 1),
        source_identity(&plan, "src_a", 2),
        source_identity(&plan, "src_b", 1),
        source_identity(&plan, "src_b", 2),
    ];
    let report = run(
        &plan,
        &[("src_a", "id\n1\n2\n"), ("src_b", "id\n10\n20\n")],
        &["out"],
    );
    let observed = report
        .dlq_entries
        .iter()
        .map(|entry| entry.source_row)
        .collect();
    (observed, expected)
}

#[test]
fn ordering_merge_concat_seeded_and_unseeded_interleave_preserve_membership() {
    let (concat, expected) = ordering_merge_identities("");
    assert_eq!(concat, expected, "concat retains declaration order");

    for mode in [
        "mode: interleave",
        "mode: interleave\n      interleave_seed: 42",
    ] {
        let (mut observed, mut expected) = ordering_merge_identities(mode);
        observed.sort_unstable();
        expected.sort_unstable();
        assert_eq!(observed, expected, "{mode} must preserve typed membership");
    }
}

#[test]
fn ordering_dispatch_has_no_scalar_sort_reconstruction() {
    let compact = |source: &str| source.split_whitespace().collect::<String>();
    let sort = compact(include_str!("../src/executor/sort_dispatch.rs"));
    let merge = compact(include_str!("../src/executor/merge_dispatch.rs"));

    assert!(!sort.contains("SortBuffer<u64>"));
    assert!(!sort.contains("(iasu64).into()"));
    assert!(sort.contains("SortBuffer<crate::executor::stream_event::SourceRowId>"));
    assert!(merge.contains("VecDeque<(Record,crate::executor::stream_event::SourceRowId)>"));
}
