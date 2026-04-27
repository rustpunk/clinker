//! Integration test for the relaxed correlation-key retraction
//! counters surfaced via `PipelineCounters.retraction`.
//!
//! Mirrors the γ-shaped fixture from
//! `crates/clinker-core/src/executor/tests/correlated_dlq_retract.rs`:
//! a Source → Transform → relaxed-CK Aggregate → Output chain with one
//! upstream-failing row that drives the orchestrator's recompute and
//! replay phases. The strict-pipeline twin asserts every retraction
//! counter stays at zero so non-opted workloads pay no observability
//! cost beyond the existing strict path.

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_core::config::{CompileContext, parse_config};
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};
use clinker_core::metrics::RetractionMetrics;
use clinker_record::PipelineCounters;

fn run_pipeline(yaml: &str, csv_input: &str) -> PipelineCounters {
    let config = parse_config(yaml).expect("parse_config");
    let plan = config.compile(&CompileContext::default()).expect("compile");
    let params = PipelineRunParams {
        execution_id: "test-exec".to_string(),
        batch_id: "test-batch".to_string(),
        pipeline_vars: config
            .pipeline
            .vars
            .as_ref()
            .map(clinker_core::config::convert_pipeline_vars)
            .unwrap_or_default(),
        shutdown_token: None,
    };
    let primary = config.source_configs().next().unwrap().name.clone();
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
        primary,
        Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec()))
            as Box<dyn std::io::Read + Send>,
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf) as Box<dyn std::io::Write + Send>,
    )]);
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("run");
    report.counters
}

const RELAXED_YAML: &str = r#"
pipeline:
  name: retract_metrics_test
error_handling:
  strategy: continue
  correlation_key: order_id
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: department, type: string }
      - { name: amount, type: string }
- type: transform
  name: validate
  input: src
  config:
    cxl: 'emit order_id = order_id

      emit department = department

      emit amount_int = amount.to_int()

      '
- type: aggregate
  name: dept_totals
  input: validate
  config:
    group_by: [department]
    relaxed_correlation_key: true
    cxl: 'emit department = department

      emit total = sum(amount_int)

      emit n = count(*)

      '
- type: output
  name: out
  input: dept_totals
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;

const STRICT_YAML: &str = r#"
pipeline:
  name: strict_metrics_test
error_handling:
  strategy: continue
  correlation_key: order_id
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: amount, type: string }
- type: transform
  name: validate
  input: src
  config:
    cxl: 'emit order_id = order_id

      emit amount_int = amount.to_int()

      '
- type: output
  name: out
  input: validate
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;

#[test]
fn retraction_counters_fire_under_relaxed_dlq_trigger() {
    // O3 in HR fails to_int and triggers the DLQ path. The relaxed
    // aggregator's recompute pass retracts the bad row id, emits a
    // delta for HR's aggregate output, and the replay phase
    // substitutes the corrected row in the buffered output cell.
    let csv = "\
order_id,department,amount
O1,HR,10
O2,HR,20
O3,HR,BAD
O4,HR,30
O5,ENG,100
O6,ENG,200
";
    let counters = run_pipeline(RELAXED_YAML, csv);

    // Sanity: the bad row triggered exactly one DLQ entry.
    assert_eq!(counters.dlq_count, 1, "one DLQ trigger expected");

    // groups_recomputed counts retract-old / add-new pairs the
    // recompute phase emits. Both HR and ENG aggregates hold lineage
    // and run the recompute (the retract id is a no-op against ENG;
    // the HR group emits a real delta), so the counter increments at
    // least once.
    assert!(
        counters.retraction.groups_recomputed >= 1,
        "groups_recomputed must increment on relaxed retract, got {}",
        counters.retraction.groups_recomputed
    );

    // subdag_replay_rows is iteratively summed across replay layers.
    // The post-aggregate sub-DAG here is Aggregate → Output, one hop,
    // so the counter equals the number of deltas times the number of
    // replay iterations the loop visits.
    assert!(
        counters.retraction.subdag_replay_rows >= 1,
        "subdag_replay_rows must increment on relaxed retract, got {}",
        counters.retraction.subdag_replay_rows
    );

    // output_rows_retracted_total counts buffered output cells the
    // flush phase substituted. The HR aggregate output reaches the
    // buffer and gets retracted in place.
    assert!(
        counters.retraction.output_rows_retracted_total >= 1,
        "output_rows_retracted_total must increment on relaxed retract, got {}",
        counters.retraction.output_rows_retracted_total
    );

    // No window in this fixture, no degrade fallback expected.
    assert_eq!(counters.retraction.partitions_recomputed, 0);
    assert_eq!(counters.retraction.degrade_fallback_count, 0);
}

#[test]
fn retraction_counters_stay_zero_on_strict_pipeline_with_dlq() {
    let csv = "\
order_id,amount
O1,10
O2,bad
O3,20
";
    let counters = run_pipeline(STRICT_YAML, csv);
    assert!(
        counters.dlq_count >= 1,
        "strict pipeline must DLQ the bad row"
    );

    let r = &counters.retraction;
    assert_eq!(r.groups_recomputed, 0);
    assert_eq!(r.partitions_recomputed, 0);
    assert_eq!(r.subdag_replay_rows, 0);
    assert_eq!(r.output_rows_retracted_total, 0);
    assert_eq!(r.degrade_fallback_count, 0);
}

#[test]
fn retraction_metrics_serializes_via_spool_payload() {
    // Verify the runtime → spool conversion preserves every field.
    let runtime = clinker_record::RetractionCounters {
        groups_recomputed: 3,
        partitions_recomputed: 1,
        subdag_replay_rows: 17,
        output_rows_retracted_total: 4,
        degrade_fallback_count: 2,
    };

    let payload = RetractionMetrics::from(&runtime);
    assert_eq!(payload.groups_recomputed, 3);
    assert_eq!(payload.partitions_recomputed, 1);
    assert_eq!(payload.subdag_replay_rows, 17);
    assert_eq!(payload.output_rows_retracted_total, 4);
    assert_eq!(payload.degrade_fallback_count, 2);

    // JSON round-trip — the spool format keeps these as bare u64
    // fields under a `retraction` object.
    let json = serde_json::to_string(&payload).expect("ser");
    assert!(json.contains("\"groups_recomputed\":3"));
    assert!(json.contains("\"partitions_recomputed\":1"));
    assert!(json.contains("\"subdag_replay_rows\":17"));
    assert!(json.contains("\"output_rows_retracted_total\":4"));
    assert!(json.contains("\"degrade_fallback_count\":2"));

    // Old-spool forward-compat: parsing JSON without the retraction
    // object yields a default (all zero) struct rather than a parse
    // error.
    let no_retract_payload: RetractionMetrics =
        serde_json::from_str("{}").expect("default-able from {}");
    assert_eq!(no_retract_payload, RetractionMetrics::default());
}
