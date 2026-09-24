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
