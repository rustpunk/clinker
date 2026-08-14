//! Differential contract tests for authored ordering promises.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Read, Write};
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

/// Run the compound boundary fixture and keep the report, not just the bytes.
fn run_compound_boundary_reported(
    memory_limit: &str,
    mode: &str,
) -> clinker_exec::executor::ExecutionReport {
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
    .unwrap_or_else(|error| panic!("{mode} writer-boundary fixture failed: {error}"))
}

/// A record excluded by `null_order: drop` is counted, and the count is the
/// same whether the sort stayed resident or spilled.
///
/// The fixture drops exactly two rows — one with an empty `key`, one with an
/// empty `group` — so a count that tracked dropping *fields* rather than
/// dropped *records* would still read two here; the compound fixture below
/// separates those.
#[test]
fn null_order_drop_is_counted_and_survives_spilling() {
    for mode in ["document", "envelope"] {
        for limit in ["64M", "2400"] {
            let report = run_compound_boundary_reported(limit, mode);
            assert_eq!(
                report.counters.null_dropped_count, 2,
                "{mode} at limit {limit} must count both null-keyed rows"
            );
            assert_eq!(
                report.counters.filtered_count, 0,
                "{mode} at limit {limit} must not report the drop as a filter"
            );
            assert_eq!(
                report.counters.dlq_count, 0,
                "{mode} at limit {limit} must not report the drop as a DLQ entry"
            );
        }
    }
}

/// The Sort stage reports the population it was handed, not the survivors.
///
/// Before this held, `records_in == records_out` on a stage that had already
/// discarded rows — the signature of a lossless stage, printed by one that
/// was not.
///
/// This asserts only that the stage stopped under-reporting its input. It
/// deliberately does *not* assert `records_in - records_out` equals the drop
/// count: `StageName::Sort` is also the name the aggregate stage times under,
/// where the same difference is a group reduction. On a fixture with no
/// aggregate the two happen to coincide, and an assertion that reads as a
/// contract while holding only as a fixture property is how the rule this
/// change fixes got written in the first place.
#[test]
fn sort_stage_reports_the_population_it_received() {
    for mode in ["document", "envelope"] {
        let report = run_compound_boundary_reported("64M", mode);
        let sorts: Vec<_> = report
            .stages
            .iter()
            .filter(|stage| stage.name == clinker_exec::executor::stage_metrics::StageName::Sort)
            .collect();
        assert!(!sorts.is_empty(), "{mode} must record a Sort stage");
        assert!(
            sorts
                .iter()
                .any(|stage| stage.records_in > stage.records_out),
            "{mode}: a sort that excluded records must report receiving more \
             than it emitted, got {:?}",
            sorts
                .iter()
                .map(|stage| (stage.records_in, stage.records_out))
                .collect::<Vec<_>>()
        );
    }
}

/// A pipeline with no dropping field reports zero, so a non-zero count is
/// always attributable to an authored `null_order: drop`.
#[test]
fn a_sort_without_a_dropping_field_reports_no_drops() {
    let plan = compile(&output_pipeline("64M", 4, true));
    let readers: SourceReaders = HashMap::from([(
        "rows".to_string(),
        single_file_reader(
            "rows.csv",
            Box::new(Cursor::new(equal_key_csv().into_bytes())),
        ),
    )]);
    let out_a = SharedBuffer::new();
    let out_b = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([
        (
            "out_a".to_string(),
            Box::new(out_a.clone()) as Box<dyn Write + Send>,
        ),
        (
            "out_b".to_string(),
            Box::new(out_b.clone()) as Box<dyn Write + Send>,
        ),
    ]);

    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &PipelineRunParams::default(),
    )
    .expect("undropped fixture must run");

    assert_eq!(report.counters.null_dropped_count, 0);
    // Safe to read every `StageName::Sort` entry as an authored sort here only
    // because this fixture has no aggregate node, which times under the same
    // name and legitimately emits fewer rows than it takes.
    for stage in report
        .stages
        .iter()
        .filter(|stage| stage.name == clinker_exec::executor::stage_metrics::StageName::Sort)
    {
        assert_eq!(
            stage.records_in, stage.records_out,
            "a sort that drops nothing must report equal in and out"
        );
    }
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

/// A single-inequality range join whose two sides each declare a sort on
/// the range axis, with each side's file matcher chosen by the caller.
///
/// `path:` resolves to one file and carries a global order; `paths:`
/// resolves to several and orders each of them independently.
fn range_join_over(driver_matcher: &str, build_matcher: &str) -> String {
    format!(
        r#"
pipeline:
  name: range_scope
nodes:
  - type: source
    name: drivers
    config:
      name: drivers
      type: csv
      {driver_matcher}
      sort_order:
        - field: val
      schema:
        - {{ name: did, type: int }}
        - {{ name: val, type: int }}
  - type: source
    name: builds
    config:
      name: builds
      type: csv
      {build_matcher}
      sort_order:
        - field: threshold
      schema:
        - {{ name: tid, type: int }}
        - {{ name: threshold, type: int }}
  - type: combine
    name: banded
    input:
      drivers: drivers
      builds: builds
    config:
      where: "drivers.val < builds.threshold"
      match: first
      on_miss: skip
      cxl: |
        emit did = drivers.did
        emit tid = builds.tid
      propagate_ck: driver
  - type: output
    name: out
    input: banded
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

/// The strategy the planner picked for the single combine in `yaml`,
/// as its `Debug` discriminant — `CombineStrategy` carries no `PartialEq`,
/// so the sibling routing tests compare it this way too.
fn combine_strategy_of(yaml: &str) -> String {
    compile(yaml)
        .dag()
        .graph
        .node_weights()
        .find_map(|node| match node {
            PlanNode::Combine { strategy, .. } => Some(format!("{strategy:?}")),
            _ => None,
        })
        .expect("fixture contains one combine")
}

/// A sort-merge scan walks each input as one sequence, so a sort proven
/// only inside each file cannot license it. Two files that each ascend
/// may still descend across the boundary between them — `100,101,102`
/// followed by `1,2,3` satisfies every per-file check and is not one
/// ordered run. Reading the declaration without its scope took the
/// in-place fast path over exactly that input.
#[test]
fn a_per_file_order_does_not_license_the_sort_merge_scan() {
    for (case, driver, build) in [
        (
            "driver side matches several files",
            "paths: [drivers-a.csv, drivers-b.csv]",
            "path: builds.csv",
        ),
        (
            "build side matches several files",
            "path: drivers.csv",
            "paths: [builds-a.csv, builds-b.csv]",
        ),
        (
            "both sides match several files",
            "paths: [drivers-a.csv, drivers-b.csv]",
            "paths: [builds-a.csv, builds-b.csv]",
        ),
        (
            "a glob resolves at runtime and is per-file for the same reason",
            "glob: ./drivers-*.csv",
            "path: builds.csv",
        ),
    ] {
        assert_eq!(
            combine_strategy_of(&range_join_over(driver, build)),
            "IEJoin",
            "{case}: a per-file order must not satisfy a scan that reads one sequence"
        );
    }
}

/// The scan is handed `presorted: true` and walks both cursors forward,
/// so what licenses it is an ascending order — not merely a sorted one.
/// A descending declaration on the range axis is the same broken promise
/// as an unsorted input: the kernel checks the certification it was given
/// and ends the run naming the planner. Such a join is still perfectly
/// valid, so it belongs to the strategy that sorts for itself.
#[test]
fn a_descending_order_does_not_license_the_sort_merge_scan() {
    let descending = runnable_range_join(
        "path: drivers.csv",
        "{ field: val, order: desc, null_order: last }",
        "path: builds.csv",
        "{ field: threshold, order: asc, null_order: last }",
    );
    assert_eq!(
        combine_strategy_of(&descending),
        "IEJoin",
        "a descending range axis cannot license a forward two-cursor walk"
    );

    // The input is one ordered sequence, so its scope reads global — and it
    // still did not get the scan. Without the direction on the page, that
    // pairing is unreadable: an author sees a globally ordered input next to
    // the strategy for inputs that are not.
    let explained = explain_of(&descending);
    let drivers = explain_block(&explained, "source.drivers:");
    assert!(
        drivers.contains("ordering: val desc")
            && drivers.contains("ordering_scope: global (one ordered sequence across the input)"),
        "a descending single-file source is globally ordered AND descending; \
         explain must say both:\n{drivers}"
    );
    let combine = explain_block(&explained, "Combine 'banded':");
    assert!(
        combine.contains("order val desc — global (one ordered sequence across the input))"),
        "the Combine input line must show the direction that disqualified it:\n{combine}"
    );
}

/// The other half of the same rule: an input that really is one ordered
/// sequence still takes the fast path. A fix that refused every declared
/// order would pass the test above and cost every correct pipeline its
/// strategy.
#[test]
fn a_global_order_still_licenses_the_sort_merge_scan() {
    assert_eq!(
        combine_strategy_of(&range_join_over("path: drivers.csv", "path: builds.csv")),
        "SortMerge",
        "one file per side is one ordered sequence per side"
    );
}

/// One record of a range-join fixture: its id and its range-axis key.
/// `None` writes an empty cell, which reads back as NULL and — under CXL
/// ternary comparison — matches nothing.
type RangeRow = (i64, Option<i64>);

fn range_csv(key_column: &str, rows: &[RangeRow]) -> Vec<u8> {
    let mut out = format!("id,{key_column}\n");
    for (id, key) in rows {
        match key {
            Some(key) => out.push_str(&format!("{id},{key}\n")),
            None => out.push_str(&format!("{id},\n")),
        }
    }
    out.into_bytes()
}

/// A runnable `drivers.val < builds.threshold` join, with each side's file
/// matcher and declared sort supplied by the caller.
///
/// `match: all` so every satisfying pair reaches the output: the failure
/// this fixture exists to catch is a scan that stops finding matches, and
/// `first` would hide a lost pair behind a surviving one.
///
/// Both range keys are declared `{ nullable: int }` so a fixture may place a
/// NULL on the axis; that makes the comparison itself nullable, which is why
/// the `where:` carries the `?? false` an author has to write.
fn runnable_range_join(
    driver_matcher: &str,
    driver_sort: &str,
    build_matcher: &str,
    build_sort: &str,
) -> String {
    format!(
        r#"
pipeline:
  name: range_scope_runtime
nodes:
  - type: source
    name: drivers
    config:
      name: drivers
      type: csv
      {driver_matcher}
      sort_order:
        - {driver_sort}
      schema:
        - {{ name: id, type: int }}
        - {{ name: val, type: {{ nullable: int }} }}
  - type: source
    name: builds
    config:
      name: builds
      type: csv
      {build_matcher}
      sort_order:
        - {build_sort}
      schema:
        - {{ name: id, type: int }}
        - {{ name: threshold, type: {{ nullable: int }} }}
  - type: combine
    name: banded
    input:
      drivers: drivers
      builds: builds
    config:
      where: "(drivers.val < builds.threshold) ?? false"
      match: all
      on_miss: skip
      cxl: |
        emit did = drivers.id
        emit tid = builds.id
      propagate_ck: driver
  - type: output
    name: out
    input: banded
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

/// Run the fixture and return its emitted `did,tid` pairs, sorted.
///
/// Sorted because the assertion is about which pairs the join found, not
/// the sequence it emitted them in: the emit order is a function of the
/// arrival order, which differs between the strategies under comparison.
fn run_range_join(
    yaml: &str,
    driver_files: &[(&str, &[RangeRow])],
    build_files: &[(&str, &[RangeRow])],
) -> Vec<String> {
    let plan = compile(yaml);
    let slots = |files: &[(&str, &[RangeRow])], key_column: &str| {
        SourceInput::Files(
            files
                .iter()
                .map(|(path, rows)| {
                    FileSlot::new(
                        PathBuf::from(path),
                        Box::new(Cursor::new(range_csv(key_column, rows))) as Box<dyn Read + Send>,
                    )
                })
                .collect(),
        )
    };
    let readers: SourceReaders = HashMap::from([
        ("drivers".to_string(), slots(driver_files, "val")),
        ("builds".to_string(), slots(build_files, "threshold")),
    ]);
    let out = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out.clone()) as Box<dyn Write + Send>,
    )]);

    PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &PipelineRunParams::default(),
    )
    .expect("range-join fixture must run");

    let text = out.as_string();
    let mut rows: Vec<String> = text
        .lines()
        .skip(1)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect();
    rows.sort();
    rows
}

/// Every `did,tid` pair satisfying `val < threshold`, by nested loop over
/// the union of each side's files — an independent answer that owes
/// nothing to the join strategy, the declared sorts, or the file split.
fn nested_loop_oracle(
    driver_files: &[(&str, &[RangeRow])],
    build_files: &[(&str, &[RangeRow])],
) -> Vec<String> {
    let flatten = |files: &[(&str, &[RangeRow])]| -> Vec<RangeRow> {
        files
            .iter()
            .flat_map(|(_, rows)| rows.iter().copied())
            .collect()
    };
    let mut pairs = Vec::new();
    for (did, val) in flatten(driver_files) {
        for (tid, threshold) in flatten(build_files) {
            // A NULL on either side of the comparison is neither true nor
            // false, and `on_miss: skip` drops the driver that produced it.
            let (Some(val), Some(threshold)) = (val, threshold) else {
                continue;
            };
            if val < threshold {
                pairs.push(format!("{did},{tid}"));
            }
        }
    }
    pairs.sort();
    pairs
}

/// Two files whose key ranges descend across the match: the first holds
/// the high range, the second the low one.
///
/// Each file ascends internally, so per-file verification passes on both;
/// the sequence a scan reads runs `100,101,102` then `1,2,3` and is not
/// ordered. This is the input the planner rule exists to keep away from a
/// two-cursor scan. Running it, rather than only inspecting the plan,
/// is what proves the rule matters to a result: the scan's kernel
/// re-checks the ascending certification it was handed, so routing this
/// input to it ends the run instead of finishing it.
const REVERSED_RANGE_HIGH_FILE: &[RangeRow] = &[(1, Some(100)), (2, Some(101)), (3, Some(102))];
const REVERSED_RANGE_LOW_FILE: &[RangeRow] = &[(4, Some(1)), (5, Some(2)), (6, Some(3))];

#[test]
fn a_range_join_over_two_files_finds_every_pair_a_nested_loop_finds() {
    let drivers: &[(&str, &[RangeRow])] = &[
        ("drivers-a.csv", REVERSED_RANGE_HIGH_FILE),
        ("drivers-b.csv", REVERSED_RANGE_LOW_FILE),
    ];
    let builds: &[(&str, &[RangeRow])] = &[(
        "builds.csv",
        &[
            (10, Some(2)),
            (11, Some(3)),
            (12, Some(4)),
            (13, Some(101)),
            (14, Some(102)),
            (15, Some(103)),
        ],
    )];

    // Hand-counted: every driver matches every build above it, which is
    // 3+2+1 for the high-range file and 6+5+4 for the low-range one. An
    // oracle that silently went empty would agree with any broken run.
    let oracle = nested_loop_oracle(drivers, builds);
    assert_eq!(oracle.len(), 21);

    let yaml = runnable_range_join(
        "paths: [drivers-a.csv, drivers-b.csv]",
        "{ field: val }",
        "path: builds.csv",
        "{ field: threshold }",
    );
    assert_eq!(
        run_range_join(&yaml, drivers, builds),
        oracle,
        "a join over files that are individually sorted must still find every pair"
    );
}

#[test]
fn a_range_join_finds_every_pair_across_sort_directions_and_null_placement() {
    // Descending files reverse which one has to come first for the
    // concatenation to break: the low range descends before the high one
    // does. Nulls sit at the end the declaration puts them at, so each
    // file satisfies its own declaration.
    let desc_high: &[RangeRow] = &[(1, Some(102)), (2, Some(101)), (3, Some(100))];
    let desc_low: &[RangeRow] = &[(4, Some(3)), (5, Some(2)), (6, Some(1)), (7, None)];
    let asc_nulls_first: &[RangeRow] = &[(1, None), (2, Some(100)), (3, Some(101))];
    let asc_low: &[RangeRow] = &[(4, Some(1)), (5, Some(2))];
    // Duplicate boundary keys on both sides: several drivers sit exactly
    // at a threshold, where a strict `<` must exclude the equal build and
    // admit the next one, once per duplicate.
    let dup_builds: &[RangeRow] = &[
        (10, Some(2)),
        (11, Some(2)),
        (12, Some(3)),
        (13, Some(100)),
        (14, Some(100)),
        (15, Some(101)),
    ];

    for (case, driver_files, driver_sort, build_files, build_sort) in [
        (
            "descending files, nulls last, duplicate boundary keys",
            vec![("drivers-a.csv", desc_low), ("drivers-b.csv", desc_high)],
            "{ field: val, order: desc, null_order: last }",
            vec![("builds.csv", dup_builds)],
            "{ field: threshold }",
        ),
        (
            "ascending files, nulls first, duplicate boundary keys",
            vec![
                ("drivers-a.csv", asc_nulls_first),
                ("drivers-b.csv", asc_low),
            ],
            "{ field: val, order: asc, null_order: first }",
            vec![("builds.csv", dup_builds)],
            "{ field: threshold }",
        ),
        (
            "the multi-file side is the build side",
            vec![("drivers.csv", asc_low)],
            "{ field: val }",
            vec![
                ("builds-a.csv", REVERSED_RANGE_HIGH_FILE),
                ("builds-b.csv", REVERSED_RANGE_LOW_FILE),
            ],
            "{ field: threshold }",
        ),
    ] {
        let driver_matcher = if driver_files.len() == 1 {
            format!("path: {}", driver_files[0].0)
        } else {
            format!(
                "paths: [{}]",
                driver_files
                    .iter()
                    .map(|(path, _)| *path)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let build_matcher = if build_files.len() == 1 {
            format!("path: {}", build_files[0].0)
        } else {
            format!(
                "paths: [{}]",
                build_files
                    .iter()
                    .map(|(path, _)| *path)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let yaml = runnable_range_join(&driver_matcher, driver_sort, &build_matcher, build_sort);
        assert_eq!(
            run_range_join(&yaml, &driver_files, &build_files),
            nested_loop_oracle(&driver_files, &build_files),
            "{case}: every satisfying pair must reach the output"
        );
    }
}

/// The `--explain` block for one node, from its header line to the blank
/// line that closes it.
fn explain_block<'a>(text: &'a str, header: &str) -> &'a str {
    let start = text
        .find(&format!("\n{header}\n"))
        .unwrap_or_else(|| panic!("explain output has no '{header}' block:\n{text}"))
        + 1;
    let rest = &text[start..];
    let end = rest.find("\n\n").map(|at| start + at).unwrap_or(text.len());
    &text[start..end]
}

/// Rendered with the statistics catalog, because the Combine detail block
/// — where the per-input order scope appears — renders only when the
/// catalog is threaded through.
fn explain_of(yaml: &str) -> String {
    let plan = compile(yaml);
    plan.dag()
        .explain_text_with_statistics(plan.config(), plan.statistics())
}

/// An author reading `--explain` has to be able to tell the two apart:
/// "sorted" that holds across the whole input and "sorted" that holds
/// only inside each file read the same in a pipeline's YAML, and only the
/// first licenses a consumer that walks one sequence. So the scope is
/// named where the order is named — on the node's properties, and again
/// on each Combine input, where it is the reason a strategy was or was
/// not available.
#[test]
fn explain_names_the_scope_an_order_was_proven_over() {
    let per_file = explain_of(&runnable_range_join(
        "paths: [drivers-a.csv, drivers-b.csv]",
        "{ field: val }",
        "path: builds.csv",
        "{ field: threshold }",
    ));
    let drivers = explain_block(&per_file, "source.drivers:");
    assert!(
        drivers.contains("ordering: val")
            && drivers.contains(
                "ordering_scope: per partition on $source.file (not one ordered sequence)"
            ),
        "a multi-file source must name the key its order is proven inside:\n{drivers}"
    );
    let builds = explain_block(&per_file, "source.builds:");
    assert!(
        builds.contains("ordering_scope: global (one ordered sequence across the input)"),
        "the single-file side of the same plan must still read as global:\n{builds}"
    );

    let combine = explain_block(&per_file, "Combine 'banded':");
    assert!(
        combine.contains(
            ", order val asc — per partition on $source.file (not one ordered sequence))"
        ),
        "the Combine input line must carry both facts eligibility reads — what the \
         order is and how wide it was proven:\n{combine}"
    );
    assert!(
        combine.contains(", order threshold asc — global (one ordered sequence across the input))"),
        "and must distinguish the input whose order is global:\n{combine}"
    );

    // The same pipeline with one file per side: the scope changes, and it
    // is the only thing about the declared order that changed.
    let global = explain_of(&runnable_range_join(
        "path: drivers.csv",
        "{ field: val }",
        "path: builds.csv",
        "{ field: threshold }",
    ));
    let drivers = explain_block(&global, "source.drivers:");
    assert!(
        drivers.contains("ordering: val")
            && drivers.contains("ordering_scope: global (one ordered sequence across the input)"),
        "a single-file source declares an order over the whole input:\n{drivers}"
    );
    assert!(
        !global.contains("per partition"),
        "no input in this plan is partitioned:\n{global}"
    );
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
