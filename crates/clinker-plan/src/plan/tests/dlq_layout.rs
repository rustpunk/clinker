//! Compile-time dead-letter layout.
//!
//! Each test compiles an inline-YAML pipeline and asserts the buckets, the
//! routing rule and the headers `CompiledPlan::dlq_layout` reports. The
//! layout is derived from the plan alone, so every assertion here is about
//! what the compiler fixes before any record is read.

use std::collections::BTreeMap;

use crate::config::{
    CompileContext, DlqConfig, DlqPerSourceConfig, ErrorStrategy, PipelineConfig,
    case_sensitive_dir, parse_config,
};
use crate::plan::CompiledPlan;
use crate::plan::dlq_layout::{DlqBucket, DlqLayout};
use crate::plan::execution::PlanNode;

fn compile(yaml: &str) -> CompiledPlan {
    let config: PipelineConfig = parse_config(yaml).expect("parse");
    config.compile(&CompileContext::default()).expect("compile")
}

fn layout(plan: &CompiledPlan) -> &DlqLayout {
    plan.dlq_layout().expect("a DLQ block yields a layout")
}

/// The user columns of the bucket `source` routes to.
fn user_columns<'a>(layout: &'a DlqLayout, source: &str) -> &'a [String] {
    let id = layout
        .bucket_for_source(source)
        .unwrap_or_else(|| panic!("{source} routes to a bucket"));
    layout.bucket(id).user_columns()
}

fn bucket_path(bucket: &DlqBucket) -> String {
    bucket.path().to_string_lossy().into_owned()
}

/// `[src_a, src_b] -> merge -> tfm -> out`, `tfm` failing under `continue`,
/// with the given `error_handling.dlq` body.
fn two_source_merge(dlq: &str) -> String {
    format!(
        r#"
pipeline:
  name: dlq_two_source_merge
error_handling:
  strategy: continue
  dlq:
{dlq}
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - {{ name: id, type: int }}
        - {{ name: amt, type: int }}
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - {{ name: id, type: int }}
        - {{ name: amt, type: int }}
  - type: merge
    name: m
    inputs: [src_a, src_b]
  - type: transform
    name: tfm
    input: m
    config:
      cxl: |
        emit id = id
        emit ratio = amt / id
  - type: sink
    name: out
    input: tfm
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

/// A `per_source` path gets its own bucket; a Source without one, and a row
/// attributed to no Source, fall through to the pipeline-wide file.
#[test]
fn bucket_rule_splits_by_source_path() {
    let plan = compile(&two_source_merge(
        "    path: dlq.csv\n    per_source:\n      src_b:\n        path: dlq_b.csv",
    ));
    let layout = layout(&plan);
    assert_eq!(layout.buckets().len(), 2);

    let wide = layout
        .bucket_for_source("src_a")
        .expect("src_a has a bucket");
    assert!(layout.is_fallback(wide));
    assert_eq!(bucket_path(layout.bucket(wide)), "dlq.csv");
    assert_eq!(layout.sources_for(wide).count(), 0);

    let own = layout
        .bucket_for_source("src_b")
        .expect("src_b has a bucket");
    assert_ne!(own, wide);
    assert!(!layout.is_fallback(own));
    assert_eq!(bucket_path(layout.bucket(own)), "dlq_b.csv");
    assert_eq!(layout.sources_for(own).collect::<Vec<_>>(), ["src_b"]);

    assert_eq!(
        layout.bucket_for_source("<merged>"),
        Some(wide),
        "a row attributed to no Source lands in the pipeline-wide file"
    );
    assert_eq!(layout.buckets()[0].path(), layout.bucket(wide).path());
}

/// A per-source path that differs from the pipeline-wide path only in case
/// names one file on a case-insensitive filesystem, so it shares the
/// pipeline-wide bucket there, and stays a bucket of its own on a
/// case-sensitive one. The expectation follows an owned temp directory's
/// actual case sensitivity, probed the way the bucket rule probes it.
///
/// The layout is derived straight from a `DlqConfig`: compiling the same
/// block through YAML on a case-insensitive filesystem stops at E318, so the
/// rule's collapse is only reachable here.
#[test]
fn bucket_rule_collapses_case_variant_paths_only_when_filesystem_folds() {
    let plan = compile(&two_source_merge("    path: dlq.csv"));
    let dir = tempfile::tempdir().expect("tempdir");
    let lower = dir.path().join("errors.csv");
    let upper = dir.path().join("Errors.csv");
    let dlq = DlqConfig {
        path: Some(lower.to_string_lossy().into_owned()),
        include_reason: None,
        include_source_row: None,
        max_rate: None,
        min_records: None,
        per_source: BTreeMap::from([(
            "src_b".to_owned(),
            DlqPerSourceConfig {
                path: Some(upper.to_string_lossy().into_owned()),
                max_rate: None,
                min_records: None,
            },
        )]),
    };
    let layout = DlqLayout::derive(
        plan.dag(),
        plan.composition_bodies(),
        Some(&dlq),
        ErrorStrategy::Continue,
    )
    .expect("the plan's deferred regions are consistent")
    .expect("a DLQ block yields a layout");

    let a = layout.bucket_for_source("src_a").expect("src_a routes");
    let b = layout.bucket_for_source("src_b").expect("src_b routes");
    assert_eq!(layout.bucket(a).path(), lower);
    if case_sensitive_dir(&lower).unwrap_or(true) {
        assert_eq!(layout.buckets().len(), 2);
        assert_ne!(a, b);
        assert_eq!(layout.bucket(b).path(), upper);
    } else {
        assert_eq!(layout.buckets().len(), 1);
        assert_eq!(
            a, b,
            "a case-variant per-source path reuses the pipeline-wide bucket"
        );
        assert_eq!(
            layout.bucket(b).path(),
            lower,
            "the first claimant names the bucket"
        );
    }
    assert_eq!(
        layout.bucket(a).header(),
        layout.bucket(b).header(),
        "both Sources' failures carry one schema, so both buckets agree"
    );
}

/// A Transform's failing row carries the Transform's input; its columns join
/// the header in that schema's own order, after the Source rejection shape.
#[test]
fn transform_site_header_keeps_natural_order() {
    let plan = compile(
        r#"
pipeline:
  name: dlq_transform_order
error_handling:
  strategy: continue
  dlq:
    path: dlq.csv
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: zulu, type: int }
        - { name: alpha, type: int }
  - type: transform
    name: first
    input: src
    config:
      cxl: |
        emit yankee = zulu + alpha
        emit bravo = zulu - alpha
  - type: transform
    name: second
    input: first
    config:
      cxl: |
        emit ratio = yankee / bravo
  - type: sink
    name: out
    input: second
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );
    let layout = layout(&plan);
    assert_eq!(
        user_columns(layout, "src"),
        ["zulu", "alpha", "_cxl_dlq_source_record", "yankee", "bravo"]
    );
    let header = layout
        .bucket(layout.bucket_for_source("src").unwrap())
        .header();
    assert_eq!(header[0], "_cxl_dlq_id");
    assert_eq!(
        header[1], "_cxl_dlq_failure_id",
        "the failure id sits directly after the id it refers to"
    );
    assert_eq!(header[2], "_cxl_dlq_timestamp");
    assert_eq!(&header[header.len() - 5..], user_columns(layout, "src"));
}

/// Without an `error_handling.dlq` block there is no dead-letter file, so
/// the plan carries no layout, whatever the strategy.
#[test]
fn layout_absent_without_dlq_block() {
    let plan = compile(
        r#"
pipeline:
  name: dlq_absent
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: int }
  - type: sink
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );
    assert!(plan.dlq_layout().is_none());
}

// ---------------------------------------------------------------------------
// Site rules. Each test below pins one row of the dead-letter site table: the
// condition under which a node's runtime arm dead-letters a record, the schema
// that record carries, and the buckets the record's Source attribution can
// reach.
// ---------------------------------------------------------------------------

/// The user columns of the bucket whose path is `path`.
fn columns_at<'a>(layout: &'a DlqLayout, path: &str) -> &'a [String] {
    layout
        .buckets()
        .iter()
        .find(|bucket| bucket_path(bucket) == path)
        .unwrap_or_else(|| panic!("no bucket at {path}"))
        .user_columns()
}

fn has(columns: &[String], name: &str) -> bool {
    columns.iter().any(|c| c == name)
}

/// `src -> pre -> {site}`: `pre` emits a column named `probe` that no node
/// before the site carries, so `probe` reaches the header exactly when the
/// site dead-letters its input. `site` is the YAML of the node(s) after
/// `pre`, reading `pre`; `source_extra` is appended to `src`'s config.
fn probe_chain(strategy: &str, source_extra: &str, site: &str) -> String {
    format!(
        r#"
pipeline:
  name: dlq_probe_chain
error_handling:
  strategy: {strategy}
  dlq:
    path: dlq.csv
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
{source_extra}
      schema:
        - {{ name: id, type: string }}
        - {{ name: amount, type: int }}
        - {{ name: event_ts, type: date_time }}
  - type: transform
    name: pre
    input: src
    config:
      cxl: |
        emit probe = amount + 1
{site}
"#
    )
}

fn probe_reaches_header(yaml: &str) -> bool {
    let plan = compile(yaml);
    has(columns_at(layout(&plan), "dlq.csv"), "probe")
}

const PLAIN_SINK: &str = r#"  - type: sink
    name: out
    input: pre
    config:
      name: out
      type: csv
      path: out.csv"#;

/// A Source rejection carries the reader's declared shape plus the raw
/// physical-row column, and it dead-letters only under `continue`.
#[test]
fn source_rejection_adds_raw_record_column_under_continue() {
    let plan = compile(
        r#"
pipeline:
  name: dlq_source_rejection
error_handling:
  strategy: continue
  dlq:
    path: dlq.csv
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: int }
        - { name: note, type: string }
  - type: sink
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );
    let layout = layout(&plan);
    assert_eq!(
        user_columns(layout, "src"),
        ["id", "note", "_cxl_dlq_source_record"]
    );
}

#[test]
fn source_rejection_absent_under_fail_fast() {
    let plan = compile(
        r#"
pipeline:
  name: dlq_source_rejection_fail_fast
error_handling:
  strategy: fail_fast
  dlq:
    path: dlq.csv
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: int }
        - { name: note, type: string }
  - type: sink
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );
    let layout = layout(&plan);
    assert!(user_columns(layout, "src").is_empty());
    let header = layout
        .bucket(layout.bucket_for_source("src").unwrap())
        .header();
    assert!(header.iter().all(|c| c.starts_with("_cxl_dlq_")));
    assert!(!header.iter().any(|c| c == "_cxl_dlq_source_record"));
}

/// Route, Reshape, windowed and grouped Aggregate, and a correlation-buffered
/// Sink each dead-letter the record they received, so each adds its input.
/// Reshape conflicts and late window records dead-letter under `fail_fast`
/// too; a plain Sink never does.
#[test]
fn route_reshape_window_aggregate_sink_sites_contribute_inputs() {
    let route = r#"  - type: route
    name: split
    input: pre
    config:
      conditions:
        high: probe > 100
      default: low
  - type: sink
    name: high
    input: split
    config:
      name: high
      type: csv
      path: high.csv
  - type: sink
    name: low
    input: split
    config:
      name: low
      type: csv
      path: low.csv"#;
    assert!(probe_reaches_header(&probe_chain("continue", "", route)));
    assert!(!probe_reaches_header(&probe_chain("fail_fast", "", route)));

    let reshape = r#"  - type: reshape
    name: rs
    input: pre
    config:
      partition_by: [id]
      rules:
        - name: r
          when: "probe > 0"
          mutate:
            set:
              amount: "probe"
  - type: sink
    name: out
    input: rs
    config:
      name: out
      type: csv
      path: out.csv"#;
    assert!(probe_reaches_header(&probe_chain("fail_fast", "", reshape)));

    let window = r#"  - type: aggregate
    name: hourly
    input: pre
    config:
      group_by: [id]
      time_window:
        tumbling: { size: 1h }
      cxl: |
        emit id = id
        emit n = count(*)
  - type: sink
    name: out
    input: hourly
    config:
      name: out
      type: csv
      path: out.csv"#;
    let watermark = "      watermark:\n        column: event_ts";
    assert!(probe_reaches_header(&probe_chain(
        "fail_fast",
        watermark,
        window
    )));

    let grouped = r#"  - type: aggregate
    name: totals
    input: pre
    config:
      group_by: [id]
      cxl: |
        emit id = id
        emit total = sum(probe)
  - type: sink
    name: out
    input: totals
    config:
      name: out
      type: csv
      path: out.csv"#;
    assert!(probe_reaches_header(&probe_chain("continue", "", grouped)));
    assert!(!probe_reaches_header(&probe_chain(
        "fail_fast",
        "",
        grouped
    )));

    let correlated = "      correlation_key: id";
    assert!(probe_reaches_header(&probe_chain(
        "continue", correlated, PLAIN_SINK
    )));
    assert!(!probe_reaches_header(&probe_chain(
        "continue", "", PLAIN_SINK
    )));
}

/// `[orders, rates] -> combine -> after -> out`, per-source files for both
/// inputs and a pipeline-wide file, with the combine's `match` mode and body.
fn combine_pipeline(match_and_body: &str) -> String {
    format!(
        r#"
pipeline:
  name: dlq_combine
error_handling:
  strategy: continue
  dlq:
    path: dlq.csv
    per_source:
      orders:
        path: orders_dlq.csv
      rates:
        path: rates_dlq.csv
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - {{ name: order_id, type: int }}
        - {{ name: rate_key, type: string }}
  - type: source
    name: rates
    config:
      name: rates
      type: csv
      path: rates.csv
      schema:
        - {{ name: rkey, type: string }}
        - {{ name: rate, type: int }}
  - type: combine
    name: joined
    input:
      orders: orders
      rates: rates
    config:
      where: "orders.rate_key == rates.rkey"
{match_and_body}
  - type: transform
    name: after
    input: joined
    config:
      cxl: |
        emit checked = order_id + 1
  - type: sink
    name: out
    input: after
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

/// The Source feeding the Combine's driving (probe) side.
fn combine_driver(plan: &CompiledPlan) -> String {
    let dag = plan.dag();
    dag.graph
        .node_weights()
        .find_map(|node| match node {
            PlanNode::Combine {
                driving_upstream: Some(up),
                ..
            } => Some(dag.graph[*up].name().to_owned()),
            _ => None,
        })
        .expect("the combine has a resolved driver")
}

/// A Combine failure dead-letters the probe row and the matched build row as
/// two entries, each attributed to its own Source, so each input's schema
/// reaches only its own Sources' buckets. Downstream of the Combine, a
/// record's Source stamp can only be the driver's (`match: collect` keeps the
/// driver's row shape), so downstream sites attribute to the driver's
/// ancestors alone.
#[test]
fn combine_probe_and_build_attribute_separately() {
    let plan = compile(&combine_pipeline(
        "      match: first\n      on_miss: skip\n      propagate_ck: driver\n      cxl: |\n        emit order_id = orders.order_id\n        emit scaled = orders.order_id * rates.rate",
    ));
    let layout = layout(&plan);
    assert_eq!(
        columns_at(layout, "orders_dlq.csv"),
        ["order_id", "rate_key", "_cxl_dlq_source_record"],
        "the rates schema never reaches the orders file"
    );
    assert_eq!(
        columns_at(layout, "rates_dlq.csv"),
        ["rkey", "rate", "_cxl_dlq_source_record"],
        "the orders schema never reaches the rates file"
    );

    let plan = compile(&combine_pipeline(
        "      match: collect\n      on_miss: null_fields\n      cxl: \"\"\n      propagate_ck: driver",
    ));
    let layout = plan.dlq_layout().expect("a DLQ block yields a layout");
    let (driver_file, build_file, build_qualifier) = match combine_driver(&plan).as_str() {
        "orders" => ("orders_dlq.csv", "rates_dlq.csv", "rates"),
        "rates" => ("rates_dlq.csv", "orders_dlq.csv", "orders"),
        other => panic!("unexpected driver {other}"),
    };
    assert!(
        has(columns_at(layout, driver_file), build_qualifier),
        "the collected column rides the driver's stamp into the driver's file: {:?}",
        columns_at(layout, driver_file)
    );
    assert!(
        !has(columns_at(layout, build_file), build_qualifier),
        "a post-combine row never carries the build side's stamp: {:?}",
        columns_at(layout, build_file)
    );
    assert!(!has(columns_at(layout, "dlq.csv"), build_qualifier));
}

/// A global fold whose failing window buffered no record dead-letters an
/// empty row of the fold's own output schema, attributed to the node rather
/// than a Source. That row reaches the pipeline-wide file alone, and only
/// under `continue`.
#[test]
fn global_fold_aggregate_output_schema_reaches_pipeline_bucket() {
    let pipeline = |strategy: &str| {
        format!(
            r#"
pipeline:
  name: dlq_global_fold
error_handling:
  strategy: {strategy}
  dlq:
    path: dlq.csv
    per_source:
      src:
        path: src_dlq.csv
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - {{ name: id, type: int }}
        - {{ name: amount, type: int }}
  - type: aggregate
    name: fold
    input: src
    config:
      group_by: []
      cxl: |
        emit grand_total = sum(amount)
        emit row_count = count(*)
  - type: sink
    name: out
    input: fold
    config:
      name: out
      type: csv
      path: out.csv
"#
        )
    };
    let plan = compile(&pipeline("continue"));
    let layout = layout(&plan);
    let wide = columns_at(layout, "dlq.csv");
    assert!(
        has(wide, "grand_total") && has(wide, "row_count"),
        "{wide:?}"
    );
    let own = columns_at(layout, "src_dlq.csv");
    assert!(
        !has(own, "grand_total") && !has(own, "row_count"),
        "{own:?}"
    );

    let plan = compile(&pipeline("fail_fast"));
    let wide = columns_at(
        plan.dlq_layout().expect("a DLQ block yields a layout"),
        "dlq.csv",
    );
    assert!(!has(wide, "grand_total"), "{wide:?}");
}

/// A composition body's sites are walked where the call site sits in the
/// parent's plan order: the body Aggregate's input columns land after the
/// parent sites before the call and before the parent sites after it.
#[test]
fn composition_body_site_contributes_at_call_site_position() {
    const FOLD_COMP: &str = r#"_compose:
  name: fold
  inputs:
    inp:
      schema:
        - { name: id, type: int }
        - { name: val, type: int }
  outputs:
    out: body_totals
nodes:
  - type: transform
    name: widen
    input: inp
    config:
      cxl: |
        emit inner = val * 2
  - type: aggregate
    name: body_totals
    input: widen
    config:
      group_by: [id]
      cxl: |
        emit id = id
        emit total = sum(inner)
"#;
    const PIPELINE: &str = r#"
pipeline:
  name: dlq_composition
error_handling:
  strategy: continue
  dlq:
    path: dlq.csv
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: int }
        - { name: val, type: int }
  - type: composition
    name: folded
    input: src
    use: ../compositions/fold.comp.yaml
    inputs:
      inp: src
  - type: transform
    name: post
    input: folded
    config:
      cxl: |
        emit ratio = total / id
  - type: sink
    name: out
    input: post
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(comp_dir.join("fold.comp.yaml"), FOLD_COMP).expect("write comp");
    let ctx =
        CompileContext::with_pipeline_dir(workspace.path(), std::path::PathBuf::from("pipelines"));
    let plan = parse_config(PIPELINE)
        .expect("parse")
        .compile(&ctx)
        .expect("compile");
    assert_eq!(
        user_columns(layout(&plan), "src"),
        ["id", "val", "_cxl_dlq_source_record", "inner", "total"],
        "`inner` exists only inside the body, and it precedes `post`'s input"
    );
}

/// A deferred-region member re-runs in the commit pass on rows pruned to the
/// region's buffer columns. That narrow schema is a projection of the wide
/// one, so it adds no column of its own; a buffer column the wide schema does
/// not carry is a planner defect, reported rather than written into a header.
#[test]
fn deferred_region_narrow_schema_is_subset_of_wide() {
    let plan = compile(
        r#"
pipeline:
  name: dlq_deferred_region
error_handling:
  strategy: continue
  dlq:
    path: dlq.csv
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: dept, type: string }
        - { name: amount, type: int }
  - type: aggregate
    name: dept_totals
    input: orders
    config:
      group_by: [dept]
      cxl: |
        emit total = sum(amount)
  - type: transform
    name: rename
    input: dept_totals
    config:
      cxl: |
        emit dept_name = dept
        emit grand_total = total
  - type: sink
    name: out
    input: rename
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#,
    );
    let dag = plan.dag();
    assert!(
        !dag.deferred_regions.is_empty(),
        "the fixture has a deferred region"
    );
    // Every buffer column a DLQ row can show is already in the header from
    // the wide schema; the engine-stamped ones (`$widened`, `$source.*`)
    // never reach a header at all.
    let header = columns_at(layout(&plan), "dlq.csv").to_vec();
    for region in dag.deferred_regions.values() {
        for column in &region.buffer_schema {
            let engine_stamped = column == "$widened" || column.starts_with("$source.");
            assert!(
                has(&header, column) != engine_stamped,
                "{column}: in {header:?}"
            );
        }
    }

    // A region whose buffer names a column no member's input carries.
    let mut tampered = dag.clone();
    for region in tampered.deferred_regions.values_mut() {
        region.buffer_schema.push("phantom".to_owned());
    }
    let error = DlqLayout::derive(
        &tampered,
        plan.composition_bodies(),
        plan.config().error_handling.dlq.as_ref(),
        ErrorStrategy::Continue,
    )
    .expect_err("a narrow column outside the wide schema is refused");
    assert!(error.contains("phantom"), "{error}");
}

/// A row with no Source stamp -- downstream of a `match: first` Combine or
/// of a grouped Aggregate -- is attributed to no Source, so its columns reach
/// the pipeline-wide file and never a per-source one.
#[test]
fn merged_rows_route_to_pipeline_bucket_only() {
    let plan = compile(&combine_pipeline(
        "      match: first\n      on_miss: skip\n      propagate_ck: driver\n      cxl: |\n        emit order_id = orders.order_id\n        emit scaled = orders.order_id * rates.rate",
    ));
    let layout = layout(&plan);
    assert!(has(columns_at(layout, "dlq.csv"), "scaled"));
    assert!(!has(columns_at(layout, "orders_dlq.csv"), "scaled"));
    assert!(!has(columns_at(layout, "rates_dlq.csv"), "scaled"));
    assert!(
        !has(columns_at(layout, "dlq.csv"), "rkey"),
        "a stamped input row goes to its own Source's file, not the pipeline-wide one"
    );
}

/// Two independent chains with their own per-source files: neither file
/// carries the other chain's columns.
#[test]
fn per_source_buckets_keep_disjoint_headers() {
    let plan = compile(
        r#"
pipeline:
  name: dlq_disjoint
error_handling:
  strategy: continue
  dlq:
    path: dlq.csv
    per_source:
      left:
        path: left_dlq.csv
      right:
        path: right_dlq.csv
nodes:
  - type: source
    name: left
    config:
      name: left
      type: csv
      path: left.csv
      schema:
        - { name: l_id, type: int }
  - type: source
    name: right
    config:
      name: right
      type: csv
      path: right.csv
      schema:
        - { name: r_id, type: int }
  - type: transform
    name: left_calc
    input: left
    config:
      cxl: |
        emit l_ratio = 10 / l_id
  - type: transform
    name: right_calc
    input: right
    config:
      cxl: |
        emit r_ratio = 10 / r_id
  - type: sink
    name: left_out
    input: left_calc
    config:
      name: left_out
      type: csv
      path: left_out.csv
  - type: sink
    name: right_out
    input: right_calc
    config:
      name: right_out
      type: csv
      path: right_out.csv
"#,
    );
    let layout = layout(&plan);
    assert_eq!(
        columns_at(layout, "left_dlq.csv"),
        ["l_id", "_cxl_dlq_source_record"]
    );
    assert_eq!(
        columns_at(layout, "right_dlq.csv"),
        ["r_id", "_cxl_dlq_source_record"]
    );
    assert!(columns_at(layout, "dlq.csv").is_empty());
}

/// Two Sources sharing a column merge into one Transform: the shared column
/// appears once in the pipeline-wide header, beside each Source's own.
#[test]
fn shared_column_appears_once_in_union() {
    let plan = compile(
        r#"
pipeline:
  name: dlq_shared_column
error_handling:
  strategy: continue
  dlq:
    path: dlq.csv
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
        - { name: a_only, type: string }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
        - { name: b_only, type: string }
  - type: merge
    name: m
    inputs: [src_a, src_b]
  - type: transform
    name: tfm
    input: m
    config:
      cxl: |
        emit ratio = 10 / id
  - type: sink
    name: out
    input: tfm
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );
    let columns = columns_at(layout(&plan), "dlq.csv");
    for name in ["id", "a_only", "b_only", "_cxl_dlq_source_record"] {
        assert_eq!(
            columns.iter().filter(|c| *c == name).count(),
            1,
            "{name} appears once in {columns:?}"
        );
    }
    assert_eq!(columns[0], "id", "the first site's schema leads the union");
}

/// `src(json) -> pre -> out`, where `out` is a CSV Sink with the given extra
/// config, and `src` declares `tags` multi-valued when `multiple` is set.
fn sink_chain(strategy: &str, source_extra: &str, multiple: bool, sink_extra: &str) -> String {
    let tags = if multiple {
        "        - { name: tags, type: string, multiple: true }"
    } else {
        "        - { name: tags, type: string }"
    };
    format!(
        r#"
pipeline:
  name: dlq_sink_site
error_handling:
  strategy: {strategy}
  dlq:
    path: dlq.csv
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      glob: ./*.json
      files:
        on_no_match: skip
{source_extra}
      schema:
        - {{ name: id, type: string }}
        - {{ name: amount, type: int }}
{tags}
  - type: transform
    name: pre
    input: src
    config:
      cxl: |
        emit probe = amount + 1
  - type: sink
    name: out
    input: pre
    config:
      name: out
      type: csv
      path: out.csv
{sink_extra}
"#
    )
}

/// A Sink dead-letters only a `join_values` collision (CSV, a multi-valued
/// field whose policy is `error`, the default), a correlation-buffered group,
/// or a `dlq_granularity: document` document; a plain CSV Sink adds nothing.
#[test]
fn sink_contributes_only_when_it_can_dead_letter() {
    let reaches = |yaml: String| probe_reaches_header(&yaml);
    assert!(
        !reaches(sink_chain("continue", "", false, "")),
        "a plain CSV Sink cannot dead-letter"
    );
    assert!(
        reaches(sink_chain(
            "continue",
            "",
            true,
            "      join_values:\n        - { field: tags, delimiter: \";\", on_conflict: error }"
        )),
        "an `on_conflict: error` field can collide"
    );
    assert!(
        reaches(sink_chain("continue", "", true, "")),
        "a multi-valued field without an entry takes the CSV default, `error`"
    );
    assert!(
        !reaches(sink_chain(
            "continue",
            "",
            true,
            "      join_values:\n        - { field: tags, delimiter: \"|\", on_conflict: escape, escape: \"\\\\\" }"
        )),
        "an escaping policy never collides"
    );
    assert!(
        !reaches(sink_chain(
            "fail_fast",
            "",
            true,
            "      join_values:\n        - { field: tags, delimiter: \";\", on_conflict: error }"
        )),
        "a collision aborts under `fail_fast`"
    );
    assert!(
        reaches(sink_chain(
            "continue",
            "      correlation_key: id",
            false,
            ""
        )),
        "a correlation-buffered Sink dead-letters failed groups"
    );
    assert!(
        reaches(sink_chain(
            "continue",
            "      dlq_granularity: document",
            false,
            ""
        )),
        "a document-grained Source's Sink dead-letters whole documents"
    );
}

/// Under `fail_fast` every strategy-guarded arm returns the error instead of
/// dead-lettering, so only the unguarded sites -- here a time window's late
/// records -- contribute: no Source rejection column, no Transform, Route or
/// grouped Aggregate input.
#[test]
fn fail_fast_admits_only_ungated_sites() {
    let plan = compile(
        r#"
pipeline:
  name: dlq_fail_fast
error_handling:
  strategy: fail_fast
  dlq:
    path: dlq.csv
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      watermark:
        column: event_ts
      schema:
        - { name: user_id, type: string }
        - { name: event_ts, type: date_time }
        - { name: amount, type: int }
  - type: transform
    name: pre
    input: src
    config:
      cxl: |
        emit window_probe = amount * 2
  - type: aggregate
    name: hourly
    input: pre
    config:
      group_by: [user_id]
      time_window:
        tumbling: { size: 1h }
      cxl: |
        emit user_id = user_id
        emit n = count(*)
  - type: transform
    name: post
    input: hourly
    config:
      cxl: |
        emit route_probe = n * 2
  - type: route
    name: split
    input: post
    config:
      conditions:
        big: route_probe > 10
      default: small
  - type: aggregate
    name: regroup
    input: split
    config:
      group_by: [user_id]
      cxl: |
        emit user_id = user_id
        emit total = sum(route_probe)
  - type: sink
    name: out
    input: regroup
    config:
      name: out
      type: csv
      path: out.csv
"#,
    );
    assert_eq!(
        columns_at(layout(&plan), "dlq.csv"),
        ["user_id", "event_ts", "amount", "window_probe"],
        "only the windowed Aggregate's input: late records are its sole site"
    );
}
