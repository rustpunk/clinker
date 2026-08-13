//! One cascading-retract converge is one execution of the transform it passes
//! over, and the exported signals have to say so.
//!
//! The commit pass re-dispatches every deferred-region member once per
//! iteration and keeps only the converged result — the loop restores the
//! forward-pass baseline before it goes round again. Reported per pass, a
//! transform carrying a `log:` directive emitted a span, a `started`, and a
//! `completed` per iteration and counted every pass's rows, so an operator
//! summing `clinker.transform.records` over a two-iteration converge read
//! roughly twice the rows the run actually carried. The counters are exported
//! as monotonic sums, so nothing downstream could have told the difference.

mod common;

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::PipelineRunParams;
use clinker_exec::telemetry::{MetricKey, TelemetryArena, TelemetryReceiver};
use clinker_plan::config::ClinkerToml;

/// A relaxed-CK aggregate feeding a Transform whose `1 / (total - 60)` divides
/// by zero on exactly one department. The failure is discovered at commit, the
/// HR contributors are retracted, and the loop runs the Transform a second
/// time — the same shape the cascading-retraction convergence test uses.
const CONVERGING_PIPELINE: &str = r#"
pipeline:
  name: converge_signals
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    correlation_key: order_id
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: department, type: string }
      - { name: amount, type: int }
- type: aggregate
  name: dept_totals
  input: src
  config:
    group_by: [department]
    cxl: |
      emit department = department
      emit total = sum(amount)
- type: transform
  name: ratio
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit total = total
      emit ratio = 1 / (total - 60)
    log:
      - name: transform.seen
        level: info
        when: per_record
        every: 1
        message: seen
        fields: [total]
- type: output
  name: out
  input: ratio
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;

const INPUT: &str = "\
order_id,department,amount
o1,HR,10
o2,HR,10
o3,HR,10
o4,HR,10
o5,HR,10
o6,HR,10
o7,ENG,100
o8,ENG,200
o9,ENG,300
";

fn observability_policy() -> clinker_plan::config::ResolvedObservabilityPolicy {
    ClinkerToml::parse(
        r#"
[observability]
arena_bytes = "768KB"
ordinary_lane_bytes = "512KB"
high_severity_lane_bytes = "256KB"
max_batch_bytes = "8KB"
max_attributes_per_event = 8
max_attribute_bytes = "256B"
drop_policy = "drop_newest"
sample_every = 1
rate_limit_per_second = 100000
rate_limit_burst = 100000
flush_timeout_ms = 1000

[observability.otlp]
endpoint = "https://collector.invalid"
connect_timeout_ms = 100
request_timeout_ms = 200
retry_max_attempts = 1
retry_total_timeout_ms = 500
max_response_bytes = "1KB"

[observability.otlp.auth]
mode = "none"

[observability.lineage]
queue_bytes = "1KB"
max_event_bytes = "512B"
drop_policy = "drop_newest"
flush_timeout_ms = 500
identity_mode = "local_diagnostic_paths"

[[observability.field_policy]]
event = "transform.seen"
field = "total"
action = "allow"
"#,
    )
    .expect("the telemetry policy parses")
    .resolve_observability(None)
    .expect("the telemetry policy resolves")
}

/// Everything the run put in the arena, by signal.
#[derive(Default)]
struct Exported {
    started: u64,
    completed: u64,
    records: u64,
    spans: usize,
}

fn drain(receiver: &TelemetryReceiver) -> Exported {
    let mut exported = Exported::default();
    while let Some(batch) = receiver.try_recv_batch() {
        exported.started += batch.metric(MetricKey::TransformStarted);
        exported.completed += batch.metric(MetricKey::TransformCompleted);
        exported.records += batch.metric(MetricKey::TransformRecords);
        exported.spans += batch
            .traces()
            .iter()
            .filter(|span| span.logical_node == "ratio")
            .count();
    }
    exported
}

#[test]
fn a_converged_transform_reports_one_execution_not_one_per_iteration() {
    let config = clinker_plan::config::parse_config(CONVERGING_PIPELINE).expect("fixture parses");
    let (producer, receiver) =
        TelemetryArena::reserve(&observability_policy()).expect("arena reserves");
    let params = PipelineRunParams {
        execution_id: "converge-signals".to_string(),
        batch_id: "batch-0".to_string(),
        telemetry_producer: Some(producer),
        ..Default::default()
    };

    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        clinker_exec::executor::single_file_reader(
            "input.csv",
            Box::new(std::io::Cursor::new(INPUT.as_bytes().to_vec())),
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let report = common::run_config(&config, readers, writers, &params)
        .expect("the cascading-retract pipeline converges");
    assert_eq!(
        report.counters.retraction.iterations, 2,
        "the fixture has to actually make the loop go round, or this proves nothing"
    );

    let exported = drain(&receiver);
    assert_eq!(
        exported.spans, 1,
        "one Transform is one span, whatever the loop had to do to converge"
    );
    assert_eq!(exported.started, 1, "a converged transform begins once");
    assert_eq!(exported.completed, 1, "and finishes once");
    assert_eq!(
        exported.records, 1,
        "the converged pass carries the surviving ENG group's single row; \
         counting both passes would have reported the retracted HR row too"
    );
}
