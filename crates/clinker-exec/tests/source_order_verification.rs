//! Source-order policy admission and compiled per-file scope contracts.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, SourceInput, SourceReaders};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{
    CompileContext, NullOrder, OnUnsorted, PipelineConfig, SortOrder, SortableEventShape,
    validate_source_sort_policy,
};
use clinker_plan::plan::execution::{OrderGuarantee, OrderScope};

fn parse(yaml: &str) -> PipelineConfig {
    clinker_plan::yaml::from_str(yaml).expect("source-order fixture must parse")
}

fn source_yaml(policy: &str, sort_order: &str) -> String {
    format!(
        r#"
pipeline:
  name: source_order_policy
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: rows.csv
      schema:
        - {{ name: key, type: int }}
        - {{ name: payload, type: string }}
{sort_order}{policy}  - type: output
    name: out
    input: rows
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

fn run_csv(
    files: &[(&str, &str)],
    on_unsorted: &str,
) -> (
    Result<clinker_exec::executor::ExecutionReport, clinker_plan::error::PipelineError>,
    String,
) {
    run_csv_with_settings(files, on_unsorted, "64M", 2)
}

fn run_csv_with_settings(
    files: &[(&str, &str)],
    on_unsorted: &str,
    memory_limit: &str,
    worker_threads: usize,
) -> (
    Result<clinker_exec::executor::ExecutionReport, clinker_plan::error::PipelineError>,
    String,
) {
    let yaml = format!(
        r#"
pipeline:
  name: source_order_barrier
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
  concurrency: {{ threads: {worker_threads} }}
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: rows.csv
      schema:
        - {{ name: key, type: int }}
        - {{ name: payload, type: string }}
      sort_order: [key]
      on_unsorted: {on_unsorted}
  - type: output
    name: out
    input: rows
    config:
      name: out
      type: csv
      path: out.csv
"#
    );
    let config = parse(&yaml);
    let plan = PipelineConfig::compile(&config, &CompileContext::default()).expect("compile");
    let slots = files
        .iter()
        .map(|(name, csv)| {
            FileSlot::new(
                PathBuf::from(name),
                Box::new(Cursor::new(csv.as_bytes().to_vec())),
            )
        })
        .collect();
    let readers: SourceReaders = HashMap::from([("rows".to_string(), SourceInput::Files(slots))]);
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), Box::new(output.clone()) as _)]);
    let params = PipelineRunParams {
        execution_id: "source-order-test".into(),
        batch_id: "batch".into(),
        ..Default::default()
    };
    let result = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params);
    (result, output.as_string())
}

#[test]
fn compiled_contract_retains_complete_source_order() {
    let config = parse(&source_yaml(
        "      on_unsorted: error\n",
        concat!(
            "      sort_order:\n",
            "        - { field: key, order: desc, null_order: first }\n",
            "        - { field: payload, order: asc, null_order: last }\n",
        ),
    ));
    let plan = PipelineConfig::compile(&config, &CompileContext::default()).expect("compile");
    let contract = plan.dag().order_contract();
    let source = contract
        .source_order_by_id(contract.source_orders[0].source_id)
        .expect("stable source-id lookup");

    assert_eq!(source.source_name, "rows");
    assert_eq!(source.scope, OrderScope::PerPhysicalFile);
    assert_eq!(source.shape, SortableEventShape::Flat);
    assert_eq!(source.on_unsorted, OnUnsorted::Error);
    assert_eq!(source.fields.len(), 2);
    assert_eq!(source.fields[0].field, "key");
    assert_eq!(source.fields[0].field_index, 0);
    assert_eq!(source.fields[0].value_type, cxl::typecheck::Type::Int);
    assert_eq!(source.fields[0].order, SortOrder::Desc);
    assert_eq!(source.fields[0].null_order, NullOrder::First);
    assert_eq!(source.fields[1].field, "payload");
    assert_eq!(source.fields[1].field_index, 1);
    assert_eq!(source.fields[1].value_type, cxl::typecheck::Type::String);
}

#[test]
fn compiled_contract_survives_raw_config_mutation() {
    let mut config = parse(&source_yaml("", "      sort_order: [key]\n"));
    let plan = PipelineConfig::compile(&config, &CompileContext::default()).expect("compile");
    let frozen = plan.dag().order_contract().source_orders[0].clone();

    let source = config
        .nodes
        .iter_mut()
        .find_map(|node| match &mut node.value {
            clinker_plan::config::PipelineNode::Source { config, .. } => Some(config),
            _ => None,
        })
        .expect("source body");
    source.source.sort_order = None;
    source.source.on_unsorted = Some(OnUnsorted::Error);

    assert_eq!(
        plan.dag()
            .order_contract()
            .source_order_by_id(frozen.source_id),
        Some(&frozen),
        "raw config changes must not rewrite compiled order evidence",
    );
}

#[test]
fn compiled_contract_reflects_finalized_graph_and_stable_ids() {
    let config = parse(
        r#"
pipeline:
  name: finalized_order_contract
error_handling:
  strategy: continue
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: rows.csv
      correlation_key: key
      schema:
        - { name: key, type: int }
        - { name: amount, type: int }
      sort_order: [key]
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
      sort_order: [key]
"#,
    );
    let plan = PipelineConfig::compile(&config, &CompileContext::default()).expect("compile");
    let dag = plan.dag();
    let contract = dag.order_contract();
    let source = &contract.source_orders[0];

    assert_eq!(contract.edges.len(), dag.graph.edge_count());
    assert!(contract.edges.iter().all(|edge| {
        dag.index_of(edge.producer_id).is_some() && dag.index_of(edge.consumer_id).is_some()
    }));
    assert!(
        contract
            .edges
            .iter()
            .any(|edge| edge.consumer_name.starts_with("__correlation_commit_")),
        "the frozen contract must include the final structural rewrite",
    );
    let requirement = contract
        .requirements
        .iter()
        .find(|requirement| requirement.consumer_name == "totals")
        .expect("streaming aggregate order requirement");
    assert_eq!(requirement.verified_sources, vec![source.source_id]);
    let terminal = contract
        .terminals
        .iter()
        .find(|terminal| terminal.node_name == "out")
        .expect("terminal order promise");
    assert!(matches!(terminal.guarantee, OrderGuarantee::Sorted(_)));
    assert_eq!(
        dag.order_contract()
            .source_order_by_id(source.source_id)
            .expect("lookup after graph rewrites"),
        source,
    );
}

#[test]
fn runtime_uses_compiled_source_order() {
    let mut raw_config = parse(&source_yaml("", "      sort_order: [key]\n"));
    let plan = PipelineConfig::compile(&raw_config, &CompileContext::default()).expect("compile");
    let frozen = plan.dag().order_contract().source_orders[0].clone();
    let source = raw_config
        .nodes
        .iter_mut()
        .find_map(|node| match &mut node.value {
            clinker_plan::config::PipelineNode::Source { config, .. } => Some(config),
            _ => None,
        })
        .expect("source body");
    source.source.sort_order = None;
    source.source.on_unsorted = None;

    let readers: SourceReaders = HashMap::from([(
        "rows".to_string(),
        SourceInput::Files(vec![FileSlot::new(
            "rows.csv",
            Box::new(Cursor::new(b"key,payload\n2,second\n1,first\n".to_vec())),
        )]),
    )]);
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), Box::new(output.clone()) as _)]);
    PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &PipelineRunParams::default(),
    )
    .expect("compiled warn policy must repair the inverted source");

    assert_eq!(
        plan.dag()
            .order_contract()
            .source_order_by_id(frozen.source_id),
        Some(&frozen),
    );
    assert_eq!(
        output.as_string().lines().skip(1).collect::<Vec<_>>(),
        ["1,first", "2,second"],
    );
}

#[test]
fn compiled_source_order_worker_spill_parity() {
    let mut unsorted = String::from("key,payload\n");
    for key in (0..128).rev() {
        unsorted.push_str(&format!("{key},row-{key:03}-{}\n", "x".repeat(96)));
    }
    let resident_warn = run_csv_with_settings(&[("rows.csv", &unsorted)], "warn", "64M", 1);
    let spilled_warn = run_csv_with_settings(&[("rows.csv", &unsorted)], "warn", "40K", 4);

    resident_warn.0.expect("resident repair");
    spilled_warn.0.expect("spilled repair");
    assert_eq!(resident_warn.1, spilled_warn.1);

    let mut sorted = String::from("key,payload\n");
    for key in 0..128 {
        sorted.push_str(&format!("{key},row-{key:03}-{}\n", "x".repeat(96)));
    }
    let resident_sorted = run_csv_with_settings(&[("rows.csv", &sorted)], "error", "64M", 1);
    let spilled_sorted = run_csv_with_settings(&[("rows.csv", &sorted)], "error", "40K", 4);
    resident_sorted.0.expect("resident sorted input");
    spilled_sorted.0.expect("spilled sorted input");
    assert_eq!(resident_sorted.1, spilled_sorted.1);

    let resident_error = run_csv_with_settings(&[("rows.csv", &unsorted)], "error", "64M", 1);
    let spilled_error = run_csv_with_settings(&[("rows.csv", &unsorted)], "error", "40K", 4);
    assert_eq!(
        resident_error
            .0
            .expect_err("resident inversion")
            .to_string(),
        spilled_error.0.expect_err("spilled inversion").to_string(),
    );
    assert_eq!(resident_error.1, spilled_error.1);
}

#[test]
fn config_on_unsorted_defaults_to_warn_and_accepts_explicit_policies() {
    let config = parse(&source_yaml("", "      sort_order: [key]\n"));
    let source = config.source_bodies().next().expect("source");
    assert_eq!(source.source.on_unsorted, None);
    validate_source_sort_policy(&source.source, &source.schema).expect("valid source policy");

    let plan = PipelineConfig::compile(&config, &CompileContext::default()).expect("compile");
    let inputs: HashMap<_, _> = plan
        .config()
        .source_bodies()
        .map(|body| (body.source.name.clone(), body))
        .collect();
    let contract = plan
        .dag()
        .derive_order_contracts(&inputs)
        .expect("derive contracts");
    assert_eq!(contract.source_orders.len(), 1);
    assert_eq!(contract.source_orders[0].on_unsorted, OnUnsorted::Warn);
    assert_eq!(contract.source_orders[0].fields[0].field, "key");
    assert_eq!(contract.source_orders[0].fields[0].field_index, 0);
    assert_eq!(contract.source_orders[0].fields[0].order, SortOrder::Asc);
    assert_eq!(
        contract.source_orders[0].fields[0].null_order,
        NullOrder::Last
    );

    for (yaml, expected) in [
        (
            source_yaml("      on_unsorted: warn\n", "      sort_order: [key]\n"),
            OnUnsorted::Warn,
        ),
        (
            source_yaml("      on_unsorted: error\n", "      sort_order: [key]\n"),
            OnUnsorted::Error,
        ),
    ] {
        let config = parse(&yaml);
        let source = config.source_bodies().next().expect("source");
        assert_eq!(source.source.on_unsorted, Some(expected));
        validate_source_sort_policy(&source.source, &source.schema)
            .expect("explicit policy must validate");
    }
}

#[test]
fn config_rejects_policy_without_sort_order_with_a_paste_ready_fix() {
    let config = parse(&source_yaml("      on_unsorted: error\n", ""));
    let source = config.source_bodies().next().expect("source");
    let err = validate_source_sort_policy(&source.source, &source.schema)
        .expect_err("policy without sort_order must fail");
    let rendered = err.to_string();
    assert!(rendered.contains("source 'rows'"), "{rendered}");
    assert!(rendered.contains("on_unsorted"), "{rendered}");
    assert!(rendered.contains("sort_order: [key]"), "{rendered}");
    assert!(rendered.contains("remove `on_unsorted`"), "{rendered}");
}

#[test]
fn config_rejects_source_null_drop_with_a_paste_ready_fix() {
    let config = parse(&source_yaml(
        "",
        "      sort_order:\n        - { field: key, order: desc, null_order: drop }\n",
    ));
    let source = config.source_bodies().next().expect("source");
    let err = validate_source_sort_policy(&source.source, &source.schema)
        .expect_err("source null drop must fail");
    let rendered = err.to_string();
    assert!(rendered.contains("source 'rows'"), "{rendered}");
    assert!(rendered.contains("null_order: drop"), "{rendered}");
    assert!(rendered.contains("null_order: first"), "{rendered}");
    assert!(rendered.contains("null_order: last"), "{rendered}");
}

#[test]
fn barrier_sorted_file_releases_once_without_reordering_duplicates() {
    let (report, output) = run_csv(
        &[("rows.csv", "key,payload\n1,first\n1,second\n2,last\n")],
        "error",
    );
    let report = report.expect("sorted input must pass the barrier");
    assert_eq!(report.counters.total_count, 3);
    let body: Vec<_> = output.lines().skip(1).collect();
    assert_eq!(body, ["1,first", "1,second", "2,last"]);
}

#[test]
fn barrier_preserves_empty_flat_and_single_frame_files() {
    let (flat_report, flat_output) = run_csv(&[("empty.csv", "key,payload\n")], "error");
    let flat_report = flat_report.expect("empty flat file must pass the barrier");
    assert_eq!(flat_report.counters.total_count, 0);
    assert!(flat_output.is_empty());

    let yaml = r#"
pipeline:
  name: source_order_empty_swift
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: swift
      path: empty.swift
      schema:
        - { name: block, type: string }
        - { name: tag, type: string }
        - { name: value, type: string }
      sort_order: [tag]
      on_unsorted: error
  - type: output
    name: out
    input: rows
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse(yaml);
    let plan = PipelineConfig::compile(&config, &CompileContext::default()).expect("compile");
    let input = "{1:F01BANKBEBBAXXX0000000000}{2:I103BANKDEFFXXXXN}{5:{CHK:ABC}}";
    let readers: SourceReaders = HashMap::from([(
        "rows".to_string(),
        SourceInput::Files(vec![FileSlot::new(
            "empty.swift",
            Box::new(Cursor::new(input.as_bytes().to_vec())),
        )]),
    )]);
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), Box::new(output.clone()) as _)]);
    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &PipelineRunParams {
            execution_id: "source-order-empty-swift".into(),
            batch_id: "batch".into(),
            ..Default::default()
        },
    )
    .expect("empty single frame must pass the barrier");
    assert_eq!(report.counters.total_count, 0);
    assert!(output.as_string().is_empty());
}

#[test]
fn barrier_error_releases_no_prefix_from_an_inverted_file() {
    for csv in [
        "key,payload\n2,first\n1,second\n3,last\n",
        "key,payload\n1,first\n3,second\n2,third\n4,last\n",
        "key,payload\n1,first\n2,second\n4,third\n3,last\n",
    ] {
        let (result, output) = run_csv(&[("bad.csv", csv)], "error");
        let rendered = result
            .expect_err("an inversion must reject the file")
            .to_string();
        assert!(rendered.contains("source 'rows'"), "{rendered}");
        assert!(rendered.contains("bad.csv"), "{rendered}");
        assert!(
            rendered.contains("rows 1 and 2")
                || rendered.contains("rows 2 and 3")
                || rendered.contains("rows 3 and 4"),
            "{rendered}"
        );
        assert!(
            !output.contains("first") && !output.contains("second") && !output.contains("last"),
            "an unverified prefix leaked: {output}"
        );
    }
}

#[test]
fn barrier_verifies_each_physical_file_independently() {
    let (result, output) = run_csv(
        &[
            ("good.csv", "key,payload\n1,good-one\n2,good-two\n"),
            ("bad.csv", "key,payload\n2,bad-two\n1,bad-one\n"),
        ],
        "error",
    );
    let rendered = result.expect_err("the second file is inverted").to_string();
    assert!(rendered.contains("bad.csv"), "{rendered}");
    assert!(
        output.contains("good-one"),
        "the verified first file was not released: {output}"
    );
    assert!(
        !output.contains("bad-one"),
        "the inverted second file leaked: {output}"
    );
}

#[test]
fn barrier_does_not_compare_across_physical_file_boundaries() {
    let (result, output) = run_csv(
        &[
            ("high.csv", "key,payload\n5,high-five\n6,high-six\n"),
            ("low.csv", "key,payload\n1,low-one\n2,low-two\n"),
        ],
        "error",
    );
    result.expect("each file is sorted even though their concatenation is not");
    let body: Vec<_> = output.lines().skip(1).collect();
    assert_eq!(
        body,
        ["5,high-five", "6,high-six", "1,low-one", "2,low-two"]
    );
}

#[test]
fn barrier_binding_admits_flat_and_one_matching_frame() {
    for format in ["csv", "swift"] {
        let yaml = source_yaml("", "      sort_order: [key]\n")
            .replace("type: csv", &format!("type: {format}"));
        let config = parse(&yaml);
        PipelineConfig::compile(&config, &CompileContext::default())
            .unwrap_or_else(|error| panic!("{format} has a sortable event shape: {error:?}"));
    }
}

#[test]
fn barrier_binding_rejects_readers_with_nested_or_repeated_frames() {
    for format in ["x12", "hl7"] {
        let yaml = source_yaml("", "      sort_order: [key]\n")
            .replace("type: csv", &format!("type: {format}"));
        let config = parse(&yaml);
        let err = PipelineConfig::compile(&config, &CompileContext::default())
            .expect_err("framed reader must be rejected before execution");
        let rendered = err
            .iter()
            .map(|diagnostic| {
                format!(
                    "{} {} {}",
                    diagnostic.code,
                    diagnostic.message,
                    diagnostic.help.as_deref().unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(rendered.contains("source 'rows'"), "{rendered}");
        assert!(rendered.contains("sort_order"), "{rendered}");
        assert!(rendered.contains("remove `sort_order`"), "{rendered}");
        assert!(rendered.contains("single"), "{rendered}");
    }
}
