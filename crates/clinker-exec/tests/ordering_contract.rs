//! Differential contract tests for authored ordering promises.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Write};
use std::path::PathBuf;
use std::sync::Arc;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{
    PipelineExecutor, PipelineRunParams, SourceInput, SourceReaders, WriterRegistry,
    single_file_reader,
};
use clinker_exec::pipeline::sort_key::{compare_authored_keys, stable_sort_key_for_record};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, PipelineConfig};
use clinker_plan::config::{NullOrder, SortField, SortOrder};
use clinker_plan::plan::combine::CombineStrategy;
use clinker_plan::plan::execution::{
    CORRELATION_SORT_PREFIX, OrderGuarantee, OrderScope, OutputOrderPromise, PlanNode,
    WriterBoundaryMode, WriterOrderDisposition, WriterPartitionKey, assert_order_contract,
    certify_streaming_edge, select_order_compatible_strategy,
};
use clinker_record::{Record, Schema, Value};

fn compile(yaml: &str) -> clinker_plan::plan::compiled::CompiledPlan {
    let config: PipelineConfig =
        clinker_plan::yaml::from_str(yaml).expect("ordering fixture must parse");
    PipelineConfig::compile(&config, &CompileContext::default()).expect("fixture must compile")
}

fn ordering_record(values: Vec<Value>) -> Record {
    Record::new(
        Arc::new(Schema::new(vec![
            "primary".into(),
            "secondary".into(),
            "identity".into(),
        ])),
        values,
    )
}

fn sort_field(field: &str, order: SortOrder, null_order: NullOrder) -> SortField {
    SortField {
        field: field.to_string(),
        order,
        null_order: Some(null_order),
    }
}

fn compare_ordered_or_multiset(promise: &OutputOrderPromise, left: &[String], right: &[String]) {
    match promise {
        OutputOrderPromise::Exact(_) => assert_eq!(left, right, "promised sequence changed"),
        OutputOrderPromise::Unordered => {
            let mut left = left.to_vec();
            let mut right = right.to_vec();
            left.sort();
            right.sort();
            assert_eq!(left, right, "unordered strategy changed record membership");
        }
    }
}

fn order_contract(
    plan: &clinker_plan::plan::compiled::CompiledPlan,
) -> clinker_plan::plan::execution::ExecutionOrderContract {
    let inputs: HashMap<_, _> = plan
        .config()
        .source_bodies()
        .map(|body| (body.source.name.clone(), body))
        .collect();
    plan.dag()
        .derive_order_contracts(&inputs)
        .expect("derive contracts")
}

fn writer_boundary<'a>(
    plan: &'a clinker_plan::plan::compiled::CompiledPlan,
    output: &str,
) -> &'a clinker_plan::plan::execution::PhysicalWriterBoundary {
    plan.dag()
        .order_contract()
        .writer_boundaries
        .iter()
        .find(|boundary| boundary.output_name == output)
        .unwrap_or_else(|| panic!("missing physical writer boundary for Output '{output}'"))
}

fn writer_boundary_base(source_extra: &str, output_extra: &str) -> String {
    format!(
        r#"
pipeline:
  name: writer_boundary
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: rows.csv
{source_extra}      schema:
        - {{ name: key, type: int }}
        - {{ name: group, type: string }}
        - {{ name: payload, type: string }}
  - type: output
    name: out
    input: rows
    config:
      name: out
      type: csv
      path: out.csv
{output_extra}"#
    )
}

#[test]
fn writer_boundary_planning_mode_matrix() {
    let records = compile(&writer_boundary_base("", ""));
    let records_boundary = writer_boundary(&records, "out");
    assert_eq!(records_boundary.mode, WriterBoundaryMode::RecordsOnly);
    assert_eq!(records_boundary.partition.key, WriterPartitionKey::Single);
    assert_eq!(records_boundary.guarantee, OrderGuarantee::StableArrival);
    assert_eq!(
        records_boundary.disposition,
        WriterOrderDisposition::Preserve
    );

    let fanout = compile(
        r#"
pipeline:
  name: writer_boundary_fanout
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      glob: ./*.csv
      schema: [{ name: key, type: int }]
  - type: output
    name: out
    input: rows
    config:
      name: out
      type: csv
      path: out_{source_file}.csv
"#,
    );
    let fanout_boundary = writer_boundary(&fanout, "out");
    assert_eq!(fanout_boundary.mode, WriterBoundaryMode::PerSourceFile);
    assert_eq!(
        fanout_boundary.partition.key,
        WriterPartitionKey::SourceFile
    );
    assert_eq!(
        fanout_boundary.partition.path_template,
        "out_{source_file}.csv"
    );

    let envelope = compile(&writer_boundary_base(
        "",
        "      reconstruct_envelope: true\n",
    ));
    let envelope_boundary = writer_boundary(&envelope, "out");
    assert_eq!(envelope_boundary.mode, WriterBoundaryMode::Envelope);
    assert_eq!(
        envelope_boundary.partition.key,
        WriterPartitionKey::Document
    );

    let document_dlq = compile(
        r#"
pipeline:
  name: writer_boundary_document_dlq
error_handling:
  strategy: continue
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      glob: ./*.csv
      dlq_granularity: document
      schema: [{ name: key, type: int }]
  - type: output
    name: out
    input: rows
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );
    let document_boundary = writer_boundary(&document_dlq, "out");
    assert_eq!(document_boundary.mode, WriterBoundaryMode::DocumentDlq);
    assert_eq!(
        document_boundary.partition.key,
        WriterPartitionKey::Document
    );

    let correlation = compile(&writer_boundary_base("      correlation_key: key\n", ""));
    let correlation_boundary = writer_boundary(&correlation, "out");
    assert_eq!(
        correlation_boundary.mode,
        WriterBoundaryMode::CorrelationDeferred
    );
    assert_eq!(
        correlation_boundary.partition.key,
        WriterPartitionKey::CorrelationGroup
    );

    let streaming = compile(
        r#"
pipeline:
  name: writer_boundary_streaming
nodes:
  - type: source
    name: left
    config:
      name: left
      type: csv
      path: left.csv
      schema: [{ name: key, type: int }]
  - type: source
    name: right
    config:
      name: right
      type: csv
      path: right.csv
      schema: [{ name: key, type: int }]
  - type: merge
    name: merged
    inputs: [left, right]
    config: { mode: concat }
  - type: output
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );
    let streaming_boundary = writer_boundary(&streaming, "out");
    assert_eq!(streaming_boundary.mode, WriterBoundaryMode::Streaming);
    assert_eq!(streaming_boundary.guarantee, OrderGuarantee::StableArrival);

    for plan in [
        &records,
        &fanout,
        &envelope,
        &document_dlq,
        &correlation,
        &streaming,
    ] {
        assert_eq!(
            plan.dag().order_contract().writer_boundaries.len(),
            1,
            "one topology-derived Output consumer must yield one physical boundary template"
        );
    }
}

#[test]
fn writer_boundary_null_drop_planning_matrix() {
    let plan = compile(&writer_boundary_base(
        "",
        r#"      sort_order:
        - { field: key, order: asc, null_order: drop }
        - { field: group, order: desc, null_order: drop }
        - { field: payload, order: asc, null_order: last }
"#,
    ));
    let boundary = writer_boundary(&plan, "out");
    let OrderGuarantee::Sorted(fields) = &boundary.guarantee else {
        panic!("authored terminal order must compile to a sorted boundary guarantee");
    };
    assert_eq!(
        fields
            .iter()
            .map(|field| field.field.as_str())
            .collect::<Vec<_>>(),
        vec!["key", "group", "payload"]
    );
    assert_eq!(
        boundary
            .pre_sort_drop_fields
            .iter()
            .map(|field| field.field.as_str())
            .collect::<Vec<_>>(),
        vec!["key", "group"]
    );
    assert_eq!(
        boundary.disposition,
        WriterOrderDisposition::DeferredSort {
            fields: fields.clone(),
        }
    );
}

#[test]
fn writer_boundary_post_rewrite_proof() {
    let plan = compile(&merge_output_pipeline("64M", true));
    let boundary = writer_boundary(&plan, "out");
    let last_reorder = boundary
        .last_reorder_capable
        .as_ref()
        .expect("post-rewrite enforcement must locate the terminal merge");
    assert_eq!(last_reorder.node_name, "merged");
    let OrderGuarantee::Sorted(authored_fields) = &boundary.guarantee else {
        panic!("authored Output ordering must remain explicit after graph rewrites");
    };
    assert_eq!(
        boundary.disposition,
        WriterOrderDisposition::DeferredSort {
            fields: authored_fields.clone(),
        },
        "a boundary whose complete population reaches the writer must carry the exact comparator"
    );

    let preserving = compile(&writer_boundary_base(
        "      correlation_key: key\n",
        "      sort_order: [key]\n",
    ));
    let mut contract = order_contract(&preserving);
    let preserving_boundary = contract
        .writer_boundaries
        .first_mut()
        .expect("correlation fixture must compile a writer boundary");
    preserving_boundary.mode = WriterBoundaryMode::RecordsOnly;
    preserving
        .dag()
        .enforce_terminal_writer_order(&mut contract)
        .expect("a terminal planner Sort must prove the authored boundary order");
    let preserving_boundary = contract
        .writer_boundaries
        .first()
        .expect("enforced writer boundary");
    let WriterOrderDisposition::ProvenTerminalSort { sort, fields } =
        &preserving_boundary.disposition
    else {
        panic!("preserving boundary must retain a terminal-sort proof");
    };
    assert!(sort.node_name.starts_with(CORRELATION_SORT_PREFIX));
    assert_eq!(fields, &authored_fields[..1]);
}

#[test]
fn writer_boundary_incompatible_mode_diagnostic() {
    let plan = compile(&merge_output_pipeline("64M", true));
    let mut contract = order_contract(&plan);
    contract
        .writer_boundaries
        .first_mut()
        .expect("merge fixture must compile a writer boundary")
        .mode = WriterBoundaryMode::Streaming;

    let error = plan
        .dag()
        .enforce_terminal_writer_order(&mut contract)
        .expect_err("streaming framing cannot promise a complete-population terminal sort");
    let message = error.to_string();
    for expected in [
        "out",
        "streaming",
        "key asc nulls last",
        "merged",
        "sort_order:",
    ] {
        assert!(
            message.contains(expected),
            "diagnostic must contain '{expected}': {message}"
        );
    }
}

fn correlation_writer_pipeline(memory_limit: &str) -> String {
    format!(
        r#"
pipeline:
  name: correlation_writer_boundary
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: rows.csv
      correlation_key: group
      on_unmapped:
        mode: reject
      schema:
        - {{ name: key, type: int }}
        - {{ name: group, type: string }}
        - {{ name: payload, type: string }}
  - type: output
    name: out
    input: rows
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
      sort_order: [key]
"#
    )
}

fn run_correlation_writer(memory_limit: &str) -> String {
    let plan = compile(&correlation_writer_pipeline(memory_limit));
    let readers: SourceReaders = HashMap::from([(
        "rows".to_string(),
        single_file_reader(
            "rows.csv",
            Box::new(Cursor::new(
                b"key,group,payload\n3,a,a3\n1,b,b1\n2,a,a2\n0,b,b0\n".to_vec(),
            )),
        ),
    )]);
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(output.clone()) as Box<dyn Write + Send>,
    )]);

    PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &PipelineRunParams::default(),
    )
    .expect("correlation writer-boundary fixture must run");
    output.as_string()
}

fn run_per_source_file_writer(
    memory_limit: &str,
    worker_threads: usize,
) -> HashMap<String, String> {
    let plan = compile(&format!(
        r#"
pipeline:
  name: per_source_file_writer_boundary
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
  concurrency: {{ threads: {worker_threads} }}
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      glob: ./*.csv
      files: {{ on_no_match: skip }}
      on_unmapped:
        mode: reject
      schema:
        - {{ name: key, type: int }}
        - {{ name: payload, type: string }}
  - type: output
    name: out
    input: rows
    config:
      name: out
      type: csv
      path: out_{{source_file}}.csv
      include_unmapped: true
      sort_order: [key]
"#
    ));
    let file_a: Arc<str> = Arc::from("a.csv");
    let file_b: Arc<str> = Arc::from("b.csv");
    let readers: SourceReaders = HashMap::from([(
        "rows".to_string(),
        SourceInput::Files(vec![
            FileSlot::new(
                PathBuf::from(file_a.as_ref()),
                Box::new(Cursor::new(
                    b"key,payload\n2,a2\n1,a1-first\n1,a1-second\n".to_vec(),
                )),
            ),
            FileSlot::new(
                PathBuf::from(file_b.as_ref()),
                Box::new(Cursor::new(b"key,payload\n3,b3\n0,b0\n".to_vec())),
            ),
        ]),
    )]);
    let out_a = SharedBuffer::new();
    let out_b = SharedBuffer::new();
    let per_file: HashMap<Arc<str>, Box<dyn Write + Send>> = HashMap::from([
        (
            Arc::clone(&file_a),
            Box::new(out_a.clone()) as Box<dyn Write + Send>,
        ),
        (
            Arc::clone(&file_b),
            Box::new(out_b.clone()) as Box<dyn Write + Send>,
        ),
    ]);
    let writers = WriterRegistry {
        single: HashMap::new(),
        fan_out: HashMap::from([("out".to_string(), per_file)]),
        ..WriterRegistry::default()
    };

    PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &PipelineRunParams::default(),
    )
    .expect("per-source-file writer-boundary fixture must run");
    HashMap::from([
        ("a".to_string(), out_a.as_string()),
        ("b".to_string(), out_b.as_string()),
    ])
}

#[test]
fn writer_boundary_mode_matrix() {
    let records = run_output_fixture("64M", 1, false, "records.csv");
    assert!(
        records["out_a"].starts_with("key,payload\n0,"),
        "records-only writer must consume its compiled boundary"
    );

    let fan_out = run_per_source_file_writer("64M", 1);
    assert_eq!(fan_out["a"], "key,payload\n1,a1-first\n1,a1-second\n2,a2\n");
    assert_eq!(fan_out["b"], "key,payload\n0,b0\n3,b3\n");

    let output = run_correlation_writer("64M");
    assert_eq!(
        output, "key,group,payload\n0,b,b0\n1,b,b1\n2,a,a2\n3,a,a3\n",
        "correlation-deferred commit must enforce the complete physical writer boundary"
    );
}

fn output_pipeline(memory_limit: &str, worker_threads: usize, fanout: bool) -> String {
    let second_output = if fanout {
        r#"
  - type: output
    name: out_b
    input: rows
    config:
      name: out_b
      type: csv
      path: out-b.csv
      include_unmapped: true
      sort_order: [key]
"#
    } else {
        ""
    };
    format!(
        r#"
pipeline:
  name: terminal_order_contract
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
  concurrency: {{ threads: {worker_threads} }}
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: rows.csv
      on_unmapped:
        mode: reject
      schema:
        - {{ name: key, type: int }}
        - {{ name: payload, type: string }}
  - type: output
    name: out_a
    input: rows
    config:
      name: out_a
      type: csv
      path: out-a.csv
      include_unmapped: true
      sort_order: [key]
{second_output}"#
    )
}

fn equal_key_csv() -> String {
    let mut csv = String::from("key,payload\n");
    for ordinal in 0..600 {
        csv.push_str(&format!(
            "{},row-{ordinal:04}-{}\n",
            ordinal % 5,
            "x".repeat(48)
        ));
    }
    csv
}

fn run_output_fixture(
    memory_limit: &str,
    worker_threads: usize,
    fanout: bool,
    source_file: &str,
) -> HashMap<String, String> {
    let plan = compile(&output_pipeline(memory_limit, worker_threads, fanout));
    let mut readers: SourceReaders = HashMap::new();
    readers.insert(
        "rows".to_string(),
        single_file_reader(
            source_file,
            Box::new(Cursor::new(equal_key_csv().into_bytes())),
        ),
    );

    let out_a = SharedBuffer::new();
    let out_b = SharedBuffer::new();
    let mut writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out_a".to_string(),
        Box::new(out_a.clone()) as Box<dyn Write + Send>,
    )]);
    if fanout {
        writers.insert("out_b".to_string(), Box::new(out_b.clone()));
    }

    PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &PipelineRunParams::default(),
    )
    .expect("output ordering fixture must run");

    HashMap::from([
        ("out_a".to_string(), out_a.as_string()),
        ("out_b".to_string(), out_b.as_string()),
    ])
}

#[test]
fn writer_boundary_equal_key_stability() {
    let resident = run_output_fixture("64M", 1, false, "resident-a.csv");
    let spilled = run_output_fixture("160K", 4, false, "spilled-z.csv");
    assert_eq!(resident["out_a"], spilled["out_a"]);

    let rows: Vec<_> = resident["out_a"].lines().skip(1).collect();
    let keys: Vec<i64> = rows
        .iter()
        .map(|row| {
            row.split(',')
                .next()
                .expect("key column")
                .parse()
                .expect("integer key")
        })
        .collect();
    assert!(
        keys.windows(2).all(|pair| pair[0] <= pair[1]),
        "terminal output must follow the authored key order"
    );
    for key in 0..5 {
        let observed: Vec<_> = rows
            .iter()
            .filter(|row| row.starts_with(&format!("{key},")))
            .map(|row| row.split(',').nth(1).expect("payload column"))
            .collect();
        let mut expected = observed.clone();
        expected.sort();
        assert_eq!(
            observed, expected,
            "equal-key rows must retain arrival order without a hidden source identity key"
        );
    }
}

#[test]
fn writer_boundary_resident_spill_parity() {
    let resident_records = run_output_fixture("64M", 1, false, "resident.csv");
    let spilled_records = run_output_fixture("160K", 4, false, "spilled.csv");
    assert_eq!(resident_records["out_a"], spilled_records["out_a"]);

    let resident_fan_out = run_per_source_file_writer("64M", 1);
    let spilled_fan_out = run_per_source_file_writer("1200", 4);
    assert_eq!(resident_fan_out, spilled_fan_out);

    assert_eq!(
        run_correlation_writer("64M"),
        run_correlation_writer("1200"),
        "correlation-deferred bytes must not depend on resident versus spill sorting"
    );
}

fn compound_boundary_csv() -> &'static str {
    "key,group,payload\n2,1,last\n1,2,first-b\n,2,drop-null-key\n1,1,first-a\n1,2,second-b\n0,,drop-null-group\n"
}

fn compound_boundary_second_csv() -> &'static str {
    "key,group,payload\n3,1,doc-two-last\n1,2,doc-two-first\n"
}

fn compound_output_order() -> &'static str {
    "key,group,payload\n1,2,first-b\n1,2,second-b\n1,1,first-a\n2,1,last\n1,2,doc-two-first\n3,1,doc-two-last\n"
}

fn compound_sort_yaml(memory_limit: &str, mode: &str) -> String {
    let source_policy = if mode == "document" {
        "      dlq_granularity: document\n"
    } else {
        ""
    };
    let envelope = if mode == "envelope" {
        "      reconstruct_envelope: true\n"
    } else {
        ""
    };
    let error_handling = if mode == "document" {
        "error_handling:\n  strategy: continue\n"
    } else {
        ""
    };
    format!(
        r#"
pipeline:
  name: writer_boundary_{mode}
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
{error_handling}nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      glob: ./*.csv
      files: {{ on_no_match: skip }}
      on_unmapped:
        mode: reject
{source_policy}      schema:
        - {{ name: key, type: {{ nullable: int }} }}
        - {{ name: group, type: {{ nullable: int }} }}
        - {{ name: payload, type: string }}
  - type: output
    name: out
    input: rows
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
{envelope}      sort_order:
        - {{ field: key, order: asc, null_order: drop }}
        - {{ field: group, order: desc, null_order: drop }}
        - {{ field: payload, order: asc, null_order: last }}
"#
    )
}

fn run_compound_boundary(memory_limit: &str, mode: &str) -> String {
    let plan = compile(&compound_sort_yaml(memory_limit, mode));
    let readers: SourceReaders = HashMap::from([(
        "rows".to_string(),
        SourceInput::Files(vec![
            FileSlot::new(
                PathBuf::from(format!("{mode}-one.csv")),
                Box::new(Cursor::new(compound_boundary_csv().as_bytes().to_vec())),
            ),
            FileSlot::new(
                PathBuf::from(format!("{mode}-two.csv")),
                Box::new(Cursor::new(
                    compound_boundary_second_csv().as_bytes().to_vec(),
                )),
            ),
        ]),
    )]);
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(output.clone()) as Box<dyn Write + Send>,
    )]);

    PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &PipelineRunParams::default(),
    )
    .unwrap_or_else(|error| panic!("{mode} writer-boundary fixture failed: {error}"));
    output.as_string()
}

#[test]
fn writer_boundary_document_streaming_complete_populations() {
    assert_eq!(
        run_compound_boundary("64M", "document"),
        compound_output_order(),
        "document-DLQ must order the accepted document at its commit boundary"
    );
    assert_eq!(
        run_compound_boundary("64M", "envelope"),
        compound_output_order(),
        "envelope framing must order the complete document before writing its body"
    );

    let streamed = compile(&merge_output_pipeline("64M", false));
    let boundary = writer_boundary(&streamed, "out");
    assert_eq!(boundary.mode, WriterBoundaryMode::Streaming);
    assert_eq!(boundary.disposition, WriterOrderDisposition::Preserve);
}

#[test]
fn writer_boundary_null_drop_exact_order_resident_spill() {
    for mode in ["document", "envelope"] {
        let resident = run_compound_boundary("64M", mode);
        let spilled = run_compound_boundary("2400", mode);
        assert_eq!(resident, compound_output_order(), "{mode} survivor order");
        assert_eq!(spilled, resident, "{mode} resident/spill parity");
        assert!(
            !resident.contains("drop-null"),
            "{mode} retained a null-drop row"
        );
    }
}

struct BoundaryFailingWriter;

impl Write for BoundaryFailingWriter {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::other("writer boundary primary failure"))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn writer_boundary_failure_cleanup() {
    let plan = compile(&output_pipeline("160K", 4, true));
    let readers: SourceReaders = HashMap::from([(
        "rows".to_string(),
        single_file_reader(
            "rows.csv",
            Box::new(Cursor::new(equal_key_csv().into_bytes())),
        ),
    )]);
    let sibling = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([
        (
            "out_a".to_string(),
            Box::new(BoundaryFailingWriter) as Box<dyn Write + Send>,
        ),
        (
            "out_b".to_string(),
            Box::new(sibling.clone()) as Box<dyn Write + Send>,
        ),
    ]);
    let spill_root = tempfile::tempdir().expect("spill root");
    let params = PipelineRunParams {
        spill_root_dir: Some(spill_root.path().to_path_buf()),
        ..PipelineRunParams::default()
    };

    let error = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect_err("the failing physical writer must fail the run");
    assert!(
        error
            .to_string()
            .contains("writer boundary primary failure"),
        "the first causal writer diagnostic must survive cleanup: {error}"
    );
    assert_eq!(
        sibling.as_string(),
        run_output_fixture("160K", 4, false, "rows.csv")["out_a"],
        "a failed writer must not starve an independent sibling attempt"
    );
    assert_eq!(
        std::fs::read_dir(spill_root.path())
            .expect("spill root remains readable")
            .count(),
        0,
        "writer failure must unlink every run-scoped spill resource"
    );
}

#[test]
fn output_fanout_preserves_each_consumer_sequence_across_workers() {
    let one_worker = run_output_fixture("64M", 1, true, "rows.csv");
    let four_workers = run_output_fixture("64M", 4, true, "rows.csv");
    assert_eq!(one_worker["out_a"], one_worker["out_b"]);
    assert_eq!(one_worker["out_a"], four_workers["out_a"]);
    assert_eq!(one_worker["out_b"], four_workers["out_b"]);
}

fn merge_output_pipeline(memory_limit: &str, sort_output: bool) -> String {
    let sort_order = if sort_output {
        "      sort_order: [key]\n"
    } else {
        ""
    };
    format!(
        r#"
pipeline:
  name: streaming_terminal_order
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
nodes:
  - type: source
    name: left
    config:
      name: left
      type: csv
      path: left.csv
      on_unmapped:
        mode: reject
      schema:
        - {{ name: key, type: int }}
        - {{ name: payload, type: string }}
  - type: source
    name: right
    config:
      name: right
      type: csv
      path: right.csv
      on_unmapped:
        mode: reject
      schema:
        - {{ name: key, type: int }}
        - {{ name: payload, type: string }}
  - type: merge
    name: merged
    inputs: [left, right]
    config:
      mode: concat
  - type: output
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: out.csv
{sort_order}"#
    )
}

fn run_merge_output(memory_limit: &str) -> String {
    let plan = compile(&merge_output_pipeline(memory_limit, true));
    let readers: SourceReaders = HashMap::from([
        (
            "left".to_string(),
            single_file_reader(
                "left.csv",
                Box::new(Cursor::new(
                    b"key,payload\n2,left-late\n1,left-first\n1,left-second\n".to_vec(),
                )),
            ),
        ),
        (
            "right".to_string(),
            single_file_reader(
                "right.csv",
                Box::new(Cursor::new(
                    b"key,payload\n1,right-first\n2,right-late\n".to_vec(),
                )),
            ),
        ),
    ]);
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(output.clone()) as Box<dyn Write + Send>,
    )]);
    PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &PipelineRunParams::default(),
    )
    .expect("stream-eligible terminal sort fixture must run");
    output.as_string()
}

#[test]
fn output_sort_disables_direct_streaming_and_remains_exact_under_spill() {
    let sorted = compile(&merge_output_pipeline("64M", true));
    let sorted_output = sorted
        .dag()
        .graph
        .node_indices()
        .find(|&idx| sorted.dag().graph[idx].name() == "out")
        .expect("sorted Output");
    assert_eq!(
        certify_streaming_edge(
            sorted.dag(),
            sorted_output,
            &HashSet::new(),
            &HashSet::new(),
        ),
        None,
        "authored terminal sort must materialize before writing"
    );

    let unsorted = compile(&merge_output_pipeline("64M", false));
    let unsorted_output = unsorted
        .dag()
        .graph
        .node_indices()
        .find(|&idx| unsorted.dag().graph[idx].name() == "out")
        .expect("unsorted Output");
    assert!(
        certify_streaming_edge(
            unsorted.dag(),
            unsorted_output,
            &HashSet::new(),
            &HashSet::new(),
        )
        .is_some(),
        "an otherwise eligible unsorted Output should keep streaming"
    );

    let expected =
        "key,payload\n1,left-first\n1,left-second\n1,right-first\n2,left-late\n2,right-late\n";
    assert_eq!(run_merge_output("64M"), expected);
    assert_eq!(run_merge_output("1200"), expected);
}

#[test]
fn strategies_advertise_exact_concat_and_unordered_interleave() {
    let pipeline = |mode: &str| {
        compile(&format!(
            r#"
pipeline:
  name: merge_contract
nodes:
  - type: source
    name: left
    config:
      name: left
      type: csv
      path: left.csv
      schema: [{{ name: key, type: int }}]
  - type: source
    name: right
    config:
      name: right
      type: csv
      path: right.csv
      schema: [{{ name: key, type: int }}]
  - type: merge
    name: merged
    inputs: [left, right]
    config:
      mode: {mode}
  - type: output
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: out.csv
"#
        ))
    };

    let concat = pipeline("concat");
    let concat_node = concat
        .dag()
        .graph
        .node_weights()
        .find(|node| node.name() == "merged")
        .expect("merge node");
    assert!(matches!(
        select_order_compatible_strategy(&order_contract(&concat), concat_node.id()),
        OutputOrderPromise::Exact(OrderGuarantee::StableArrival)
    ));

    let interleave = pipeline("interleave");
    let interleave_node = interleave
        .dag()
        .graph
        .node_weights()
        .find(|node| node.name() == "merged")
        .expect("merge node");
    let promise =
        select_order_compatible_strategy(&order_contract(&interleave), interleave_node.id());
    assert_eq!(promise, OutputOrderPromise::Unordered);
    compare_ordered_or_multiset(
        &promise,
        &["left-1".into(), "right-1".into()],
        &["right-1".into(), "left-1".into()],
    );

    let seeded = pipeline("interleave\n      interleave_seed: 7");
    let seeded_node = seeded
        .dag()
        .graph
        .node_weights()
        .find(|node| node.name() == "merged")
        .expect("merge node");
    assert!(matches!(
        select_order_compatible_strategy(&order_contract(&seeded), seeded_node.id()),
        OutputOrderPromise::Exact(OrderGuarantee::StableArrival)
    ));
}

#[test]
fn conservative_unordered_promise_accepts_stronger_runtime_strategy() {
    let plan = compile(
        r#"
pipeline:
  name: conservative_merge_contract
nodes:
  - type: source
    name: left
    config:
      name: left
      type: csv
      path: left.csv
      schema: [{ name: key, type: int }]
  - type: source
    name: right
    config:
      name: right
      type: csv
      path: right.csv
      schema: [{ name: key, type: int }]
  - type: merge
    name: merged
    inputs: [left, right]
    config:
      mode: interleave
  - type: output
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );
    let merge = plan
        .dag()
        .graph
        .node_weights()
        .find(|node| node.name() == "merged")
        .expect("merge node");

    assert_order_contract(
        &order_contract(&plan),
        merge,
        OutputOrderPromise::Exact(OrderGuarantee::StableArrival),
    )
    .expect("a stronger runtime promise must satisfy a conservative unordered contract");
}

#[test]
fn strategies_do_not_treat_matching_parent_sorts_through_merge_as_global() {
    let plan = compile(
        r#"
pipeline:
  name: merge_then_range_combine
nodes:
  - type: source
    name: products_a
    config:
      name: products_a
      type: csv
      path: products-a.csv
      sort_order: [price]
      schema:
        - { name: sku, type: string }
        - { name: price, type: int }
  - type: source
    name: products_b
    config:
      name: products_b
      type: csv
      path: products-b.csv
      sort_order: [price]
      schema:
        - { name: sku, type: string }
        - { name: price, type: int }
  - type: merge
    name: products
    inputs: [products_a, products_b]
    config:
      mode: concat
  - type: source
    name: brackets
    config:
      name: brackets
      type: csv
      path: brackets.csv
      sort_order: [max]
      schema:
        - { name: bracket_id, type: string }
        - { name: max, type: int }
  - type: combine
    name: assign_bracket
    input:
      products: products
      brackets: brackets
    config:
      where: "products.price < brackets.max"
      match: first
      on_miss: null_fields
      propagate_ck: driver
      cxl: |
        emit sku = products.sku
        emit bracket_id = brackets.bracket_id
  - type: output
    name: out
    input: assign_bracket
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );

    let combine = plan
        .dag()
        .graph
        .node_weights()
        .find(|node| node.name() == "assign_bracket")
        .expect("combine node");
    let PlanNode::Combine { strategy, .. } = combine else {
        panic!("assign_bracket must compile as Combine");
    };
    assert!(
        !matches!(strategy, CombineStrategy::SortMerge),
        "concatenating independently sorted inputs is not one global sort"
    );
    assert_eq!(
        select_order_compatible_strategy(&order_contract(&plan), combine.id()),
        OutputOrderPromise::Unordered,
        "Combine output sequence is incidental for every current strategy"
    );
}

#[test]
fn planning_attaches_per_file_verification_to_order_dependent_consumers() {
    let plan = compile(
        r#"
pipeline:
  name: per_file_order
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      paths: [part-a.csv, part-b.csv]
      schema:
        - { name: key, type: int }
        - { name: amount, type: int }
      sort_order:
        - { field: key, order: desc, null_order: first }
  - type: aggregate
    name: totals
    input: rows
    config:
      strategy: streaming
      group_by: [key]
      cxl: |
        emit key = key
        emit total = sum(amount)
  - type: output
    name: out
    input: totals
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );

    let inputs: HashMap<_, _> = plan
        .config()
        .source_bodies()
        .map(|body| (body.source.name.clone(), body))
        .collect();
    let contract = plan
        .dag()
        .derive_order_contracts(&inputs)
        .expect("derive contracts");

    let source_order = contract
        .source_orders
        .iter()
        .find(|order| order.source_name == "rows")
        .expect("compiled source order");
    assert_eq!(source_order.scope, OrderScope::PerPhysicalFile);

    let source_edge = contract
        .edges
        .iter()
        .find(|edge| edge.producer_name == "rows")
        .expect("source edge");
    assert_eq!(source_edge.guarantee, OrderGuarantee::StableArrival);

    let requirement = contract
        .requirements
        .iter()
        .find(|requirement| requirement.consumer_name == "totals")
        .expect("streaming aggregate requirement");
    assert_eq!(requirement.scope, OrderScope::PerPhysicalFile);
    assert_eq!(requirement.fields, source_order.fields);
    assert_eq!(requirement.verified_sources, vec![source_order.source_id]);
}

#[test]
fn planning_never_promotes_two_sorted_files_to_one_global_sort() {
    let plan = compile(
        r#"
pipeline:
  name: two_sorted_files
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      paths: [part-a.csv, part-b.csv]
      schema:
        - { name: key, type: int }
      sort_order: [key]
  - type: output
    name: out
    input: rows
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );

    let inputs: HashMap<_, _> = plan
        .config()
        .source_bodies()
        .map(|body| (body.source.name.clone(), body))
        .collect();
    let contract = plan
        .dag()
        .derive_order_contracts(&inputs)
        .expect("derive contracts");
    let source_edge = contract
        .edges
        .iter()
        .find(|edge| edge.producer_name == "rows")
        .expect("source edge");

    assert_eq!(source_edge.guarantee, OrderGuarantee::StableArrival);
    assert!(
        contract
            .source_orders
            .iter()
            .all(|source| source.scope == OrderScope::PerPhysicalFile),
        "source declarations describe records inside each physical file"
    );
}

#[test]
fn planning_marks_unseeded_interleave_as_order_destroying() {
    let plan = compile(
        r#"
pipeline:
  name: interleave_order
nodes:
  - type: source
    name: left
    config:
      name: left
      type: csv
      path: left.csv
      schema: [{ name: key, type: int }]
      sort_order: [key]
  - type: source
    name: right
    config:
      name: right
      type: csv
      path: right.csv
      schema: [{ name: key, type: int }]
      sort_order: [key]
  - type: merge
    name: merged
    inputs: [left, right]
    config:
      mode: interleave
  - type: output
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );

    let inputs: HashMap<_, _> = plan
        .config()
        .source_bodies()
        .map(|body| (body.source.name.clone(), body))
        .collect();
    let contract = plan
        .dag()
        .derive_order_contracts(&inputs)
        .expect("derive contracts");
    let merge_edge = contract
        .edges
        .iter()
        .find(|edge| edge.producer_name == "merged")
        .expect("merge edge");

    assert_eq!(merge_edge.guarantee, OrderGuarantee::Unordered);
}

#[test]
fn planning_terminal_sort_promises_exactly_the_authored_fields() {
    let plan = compile(
        r#"
pipeline:
  name: terminal_sort
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: rows.csv
      schema:
        - { name: key, type: int }
        - { name: payload, type: string }
  - type: output
    name: out
    input: rows
    config:
      name: out
      type: csv
      path: out.csv
      sort_order:
        - { field: key, order: desc, null_order: first }
"#,
    );

    let inputs: HashMap<_, _> = plan
        .config()
        .source_bodies()
        .map(|body| (body.source.name.clone(), body))
        .collect();
    let contract = plan
        .dag()
        .derive_order_contracts(&inputs)
        .expect("derive contracts");
    let terminal = contract
        .terminals
        .iter()
        .find(|terminal| terminal.node_name == "out")
        .expect("terminal order promise");
    let OrderGuarantee::Sorted(fields) = &terminal.guarantee else {
        panic!("terminal sort must compile to an exact sorted guarantee");
    };

    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].field, "key");
    assert_eq!(fields[0].order, SortOrder::Desc);
    assert_eq!(fields[0].null_order, NullOrder::First);
}

#[test]
fn comparator_and_stable_key_agree_on_null_direction_and_numeric_boundaries() {
    let comparisons = [
        (
            Value::Null,
            Value::Integer(i64::MIN),
            SortOrder::Desc,
            NullOrder::First,
        ),
        (
            Value::Null,
            Value::Integer(i64::MAX),
            SortOrder::Desc,
            NullOrder::Last,
        ),
        (
            Value::Integer(i64::MIN),
            Value::Integer(i64::MAX),
            SortOrder::Asc,
            NullOrder::Last,
        ),
        (
            Value::Float(-f64::MAX),
            Value::Float(f64::MAX),
            SortOrder::Desc,
            NullOrder::Last,
        ),
    ];

    for (left, right, order, null_order) in comparisons {
        let fields = [sort_field("primary", order, null_order)];
        let left = ordering_record(vec![left, Value::Null, Value::Integer(99)]);
        let right = ordering_record(vec![right, Value::Null, Value::Integer(1)]);
        let direct = compare_authored_keys(&left, &right, &fields);
        let encoded = stable_sort_key_for_record(&left, &fields)
            .cmp(&stable_sort_key_for_record(&right, &fields));

        assert_eq!(
            encoded, direct,
            "encoded and direct authored-key order drifted"
        );
    }
}

#[test]
fn comparator_compound_unicode_keys_are_framed_without_hidden_identity_ties() {
    let fields = [
        sort_field("primary", SortOrder::Asc, NullOrder::Last),
        sort_field("secondary", SortOrder::Desc, NullOrder::Last),
    ];
    let rows = [
        ordering_record(vec![
            Value::String("a\0".into()),
            Value::String("éclair".into()),
            Value::Integer(1),
        ]),
        ordering_record(vec![
            Value::String("a".into()),
            Value::String("漢字".into()),
            Value::Integer(999),
        ]),
        ordering_record(vec![
            Value::String("a".into()),
            Value::String("漢字".into()),
            Value::Integer(-999),
        ]),
    ];

    assert_eq!(
        compare_authored_keys(&rows[1], &rows[2], &fields),
        Ordering::Equal
    );
    assert_eq!(
        stable_sort_key_for_record(&rows[1], &fields),
        stable_sort_key_for_record(&rows[2], &fields),
        "an unmentioned source identity must not become an ordering tie-breaker"
    );

    for left in &rows {
        for right in &rows {
            assert_eq!(
                stable_sort_key_for_record(left, &fields)
                    .cmp(&stable_sort_key_for_record(right, &fields)),
                compare_authored_keys(left, right, &fields),
                "compound UTF-8 key framing drifted from the authored comparator"
            );
        }
    }
}
