use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, SourceInput, SourceReaders};
use clinker_exec::source::multi_file::FileSlot;
use clinker_exec::telemetry::{
    ArenaSnapshot, LogEvent, LogRecord, MetricKey, RunCorrelation, Severity, SignalField, SpanFact,
    SpanName, SpanStatus, TelemetryArena, TelemetryBatch, TraceSpan, unix_nanos_now,
};
use clinker_plan::config::{ClinkerToml, CompileContext, parse_config};
use clinker_record::Value;

fn policy_with_lanes(
    ordinary: &str,
    high: &str,
    arena: &str,
) -> clinker_plan::config::ResolvedObservabilityPolicy {
    let text = format!(
        r#"
[observability]
arena_bytes = "{arena}"
ordinary_lane_bytes = "{ordinary}"
high_severity_lane_bytes = "{high}"
max_batch_bytes = "512B"
max_attributes_per_event = 4
max_attribute_bytes = "64B"
drop_policy = "drop_newest"
sample_every = 1
rate_limit_per_second = 1000
rate_limit_burst = 1000
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
event = "transform.customer_seen"
field = "customer_id"
action = "allow"

[[observability.field_policy]]
event = "transform.customer_seen"
field = "email"
action = "hash"

[[observability.field_policy]]
event = "transform.customer_seen"
field = "region"
action = "replace"
replacement = "[region]"
"#
    );
    ClinkerToml::parse(&text)
        .expect("telemetry policy parses")
        .resolve_observability(None)
        .expect("telemetry policy resolves")
}

fn transform_policy(
    ordinary: &str,
    high: &str,
    arena: &str,
    max_attribute: &str,
    sample_every: u32,
    rate_limit_per_second: u32,
    rate_limit_burst: u32,
) -> clinker_plan::config::ResolvedObservabilityPolicy {
    // Deliberately declares no rule for execution_id, batch_id, or
    // pipeline_name. Those are engine-supplied run correlation rather than
    // record data, so a deployment must not have to name them here to get
    // telemetry it can join to the run.
    let mut field_policy = String::new();
    field_policy.push_str(
        r#"
[[observability.field_policy]]
event = "transform.customer_seen"
field = "customer_id"
action = "allow"

[[observability.field_policy]]
event = "transform.customer_seen"
field = "email"
action = "hash"

[[observability.field_policy]]
event = "transform.customer_seen"
field = "region"
action = "replace"
replacement = "[region]"

[[observability.field_policy]]
event = "transform.customer_failed"
field = "customer_id"
action = "allow"
"#,
    );

    let text = format!(
        r#"
[observability]
arena_bytes = "{arena}"
ordinary_lane_bytes = "{ordinary}"
high_severity_lane_bytes = "{high}"
max_batch_bytes = "512B"
max_attributes_per_event = 8
max_attribute_bytes = "{max_attribute}"
drop_policy = "drop_newest"
sample_every = {sample_every}
rate_limit_per_second = {rate_limit_per_second}
rate_limit_burst = {rate_limit_burst}
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
{field_policy}
"#,
    );
    ClinkerToml::parse(&text)
        .expect("transform telemetry policy parses")
        .resolve_observability(None)
        .expect("transform telemetry policy resolves")
}

const TRANSFORM_PIPELINE: &str = r#"
pipeline:
  name: transform_observability_runtime
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
  - type: source
    name: customers
    config:
      name: customers
      type: csv
      path: customers.csv
      schema:
        - { name: customer_id, type: string }
        - { name: amount, type: string }
        - { name: email, type: string }
        - { name: region, type: string }
        - { name: secret, type: string }
  - type: transform
    name: observe_customers
    input: customers
    config:
      cxl: |
        emit customer_id = customer_id
        emit amount = amount.to_int()
        emit email = email
        emit region = region
      log:
        - name: transform.starting
          level: debug
          when: before_transform
          message: "Starting customer transform"
        - name: transform.completed
          level: info
          when: after_transform
          message: "Customer transform completed"
        - name: transform.customer_seen
          level: info
          when: per_record
          every: 1
          message: "Customer observed"
          fields: [customer_id, email, region, secret]
        - name: transform.customer_failed
          level: error
          when: on_error
          message: "Customer transform failed"
          fields: [customer_id, secret]
  - type: sink
    name: output
    input: observe_customers
    config:
      name: output
      type: csv
      path: output.csv
      include_unmapped: false
"#;

#[derive(Debug, Eq, PartialEq)]
struct EtlSignature {
    output: Vec<u8>,
    counters: (u64, u64, u64, u64, u64, u64, String),
    dlq: Vec<(u64, String, Option<String>, String)>,
}

struct TransformRun {
    etl: EtlSignature,
    batch: Option<TelemetryBatch>,
    snapshot: Option<ArenaSnapshot>,
}

fn transform_csv(rows: usize) -> String {
    let mut csv = String::from("customer_id,amount,email,region,secret\n");
    for row in 1..=rows {
        let amount = if row == 2 {
            "not-an-integer".to_string()
        } else {
            row.to_string()
        };
        csv.push_str(&format!(
            "customer-{row},{amount},customer-{row}@example.invalid,very-long-region-{row},secret-{row}\n"
        ));
    }
    csv
}

fn run_transform_dispatch(
    rows: usize,
    policy: Option<&clinker_plan::config::ResolvedObservabilityPolicy>,
) -> TransformRun {
    let config = parse_config(TRANSFORM_PIPELINE).expect("transform telemetry fixture parses");
    let plan = config
        .compile(&CompileContext::default())
        .expect("transform telemetry fixture compiles");
    let telemetry = policy.map(|policy| {
        TelemetryArena::reserve(policy).expect("enabled transform telemetry policy creates arena")
    });
    let producer = telemetry.as_ref().map(|(producer, _)| producer.clone());

    let readers: SourceReaders = HashMap::from([(
        "customers".to_string(),
        SourceInput::Files(vec![FileSlot::new(
            PathBuf::from("customers.csv"),
            Box::new(Cursor::new(transform_csv(rows))),
        )]),
    )]);
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "output".to_string(),
        Box::new(output.clone()) as Box<dyn Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "execution-123456789".to_string(),
        batch_id: "batch-987654321".to_string(),
        telemetry_producer: producer.clone(),
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("recoverable transform error completes the run");

    let etl = EtlSignature {
        output: output.contents(),
        counters: (
            report.counters.total_count,
            report.counters.ok_count,
            report.counters.records_written,
            report.counters.dlq_count,
            report.counters.filtered_count,
            report.counters.distinct_count,
            format!("{:?}", report.counters.retraction),
        ),
        dlq: report
            .dlq_entries
            .iter()
            .map(|entry| {
                (
                    entry.source_row.ordinal(),
                    format!("{:?}", entry.category),
                    entry.stage.clone(),
                    entry.source_name.to_string(),
                )
            })
            .collect(),
    };
    let batch = telemetry
        .as_ref()
        .and_then(|(_, receiver)| receiver.try_recv_batch());
    let snapshot = producer.as_ref().map(|producer| producer.snapshot());
    TransformRun {
        etl,
        batch,
        snapshot,
    }
}

fn log<'a>(logs: &'a [LogRecord], event: &str) -> &'a LogRecord {
    logs.iter()
        .find(|record| record.event == event)
        .unwrap_or_else(|| panic!("missing structured event {event}: {logs:?}"))
}

/// One transform is one complete span. Admitting a start fact and an end fact
/// independently let sampling, lane routing, or a full arena deliver a half
/// span, which a collector cannot use.
fn assert_transform_span_is_complete(traces: &[TraceSpan]) {
    let matching = traces
        .iter()
        .filter(|span| span.name == SpanName::Transform && span.logical_node == "observe_customers")
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 1, "one closed span: {matching:?}");
    assert_eq!(
        matching[0].status,
        SpanStatus::Error,
        "the recoverable record error is reflected without leaking its text"
    );
    assert!(
        matching[0].started_at_unix_nanos > 0,
        "a span carries a real start time: {matching:?}"
    );
    assert!(
        matching[0].ended_at_unix_nanos >= matching[0].started_at_unix_nanos,
        "a span never ends before it starts: {matching:?}"
    );
}

fn correlation<'a>() -> RunCorrelation<&'a str> {
    RunCorrelation {
        execution_id: "execution-123456789",
        batch_id: "batch-987654321",
        pipeline_name: "transform_observability_runtime",
    }
}

#[test]
fn telemetry_arena_admits_private_three_signal_batch() {
    let policy = policy_with_lanes("3KB", "1KB", "4KB");
    let (producer, receiver) =
        TelemetryArena::reserve(&policy).expect("enabled policy creates arena");

    let log = LogEvent {
        event: "transform.customer_seen",
        severity: Severity::Info,
        message: "customer observed",
        correlation: correlation(),
        fields: &[
            SignalField::new("customer_id", "42"),
            SignalField::new("email", "customer@example.invalid"),
            SignalField::new("region", "north"),
            SignalField::new("secret", "never-crosses"),
        ],
    };
    assert!(producer.emit_log(log).is_accepted());
    producer.record_metric(MetricKey::TransformRecords, 3);
    assert!(
        producer
            .emit_span(SpanFact {
                name: SpanName::Transform,
                status: SpanStatus::Ok,
                logical_node: "customer_seen",
                started_at_unix_nanos: unix_nanos_now(),
                ended_at_unix_nanos: unix_nanos_now(),
            })
            .is_accepted()
    );

    let batch = receiver
        .try_recv_batch()
        .expect("one bounded batch is ready");
    assert!(batch.presence().logs);
    assert!(batch.presence().metrics);
    assert!(batch.presence().traces);
    assert_eq!(batch.logs().len(), 1);
    assert_eq!(batch.traces().len(), 1);
    assert_eq!(batch.metric(MetricKey::TransformRecords), 3);
    assert!(batch.serialized_bytes() > 0);

    let json = serde_json::to_string(&batch).expect("typed batch serializes");
    assert!(json.contains("customer_id"));
    assert!(json.contains("42"));
    assert!(json.contains("blake3:"));
    assert!(json.contains("[region]"));
    assert!(!json.contains("customer@example.invalid"));
    assert!(!json.contains("never-crosses"));
    assert!(!json.contains("secret"));

    let stats = producer.snapshot();
    assert_eq!(stats.owned_bytes, 4_000);
    assert_eq!(stats.ordinary_capacity_bytes, 3_000);
    assert_eq!(stats.high_capacity_bytes, 1_000);
    assert_eq!(stats.retained_bytes, 0);
    assert_eq!(stats.denied_fields, 1);
    assert!(stats.peak_retained_bytes <= stats.owned_bytes);
}

#[test]
fn telemetry_arena_drop_newest_preserves_high_lane_and_exact_accounting() {
    let policy = policy_with_lanes("1KB", "1KB", "2KB");
    let (producer, receiver) =
        TelemetryArena::reserve(&policy).expect("enabled policy creates arena");

    let ordinary = LogEvent {
        event: "transform.customer_seen",
        severity: Severity::Info,
        message: "ordinary",
        correlation: correlation(),
        fields: &[SignalField::new("customer_id", "1")],
    };
    let mut ordinary_full = false;
    for _ in 0..16 {
        if producer.emit_log(ordinary).is_full() {
            ordinary_full = true;
            break;
        }
    }
    assert!(ordinary_full, "ordinary lane eventually drops newest");

    let high = LogEvent {
        event: "transform.customer_seen",
        severity: Severity::Error,
        message: "static failure",
        correlation: correlation(),
        fields: &[],
    };
    assert!(
        producer.emit_log(high).is_accepted(),
        "ordinary exhaustion cannot consume the reserved high lane"
    );

    let stats = producer.snapshot();
    assert_eq!(stats.owned_bytes, 2_000);
    assert!(stats.retained_bytes <= stats.owned_bytes);
    assert!(stats.ordinary_retained_bytes <= stats.ordinary_capacity_bytes);
    assert!(stats.high_retained_bytes <= stats.high_capacity_bytes);
    assert!(stats.full_drops > 0);

    let batch = receiver
        .try_recv_batch()
        .expect("accepted facts are drainable");
    assert!(
        batch
            .logs()
            .iter()
            .any(|record| record.severity == Severity::Error),
        "the high-severity fact crosses its reserved lane"
    );
    assert!(producer.snapshot().retained_bytes <= producer.snapshot().owned_bytes);
}

/// A full lane is attributed to the lane that filled.
///
/// The lanes hold separate byte reservations, so "the ordinary lane is full"
/// and "the high-severity lane is full" are different conditions with
/// different corrections. A single total reports both as the same number and
/// leaves an operator unable to tell whether any `error` was lost at all.
#[test]
fn a_full_lane_is_attributed_to_the_lane_that_filled() {
    let policy = policy_with_lanes("1KB", "1KB", "2KB");
    let (producer, _receiver) =
        TelemetryArena::reserve(&policy).expect("enabled policy creates arena");
    let ordinary = LogEvent {
        event: "transform.customer_seen",
        severity: Severity::Info,
        message: "ordinary",
        correlation: correlation(),
        fields: &[SignalField::new("customer_id", "1")],
    };
    let high = LogEvent {
        event: "transform.customer_failed",
        severity: Severity::Error,
        message: "static failure",
        correlation: correlation(),
        fields: &[SignalField::new("customer_id", "1")],
    };

    for _ in 0..64 {
        let _ = producer.emit_log(ordinary);
    }
    let ordinary_only = producer.snapshot();
    assert!(
        ordinary_only.ordinary_full_drops > 0,
        "the ordinary lane was filled: {ordinary_only:?}"
    );
    assert_eq!(
        ordinary_only.high_full_drops, 0,
        "ordinary volume must not be reported as high-severity loss"
    );

    for _ in 0..64 {
        let _ = producer.emit_log(high);
    }
    let both = producer.snapshot();
    assert!(
        both.high_full_drops > 0,
        "the high-severity lane was filled too: {both:?}"
    );
    assert_eq!(
        both.ordinary_full_drops, ordinary_only.ordinary_full_drops,
        "filling the high lane cannot change the ordinary lane's accounting"
    );
    assert_eq!(
        both.full_drops,
        both.ordinary_full_drops + both.high_full_drops,
        "the total is exactly the two lanes"
    );
}

#[test]
fn telemetry_arena_bounds_typed_record_values_before_serialization() {
    let policy = policy_with_lanes("3KB", "1KB", "4KB");
    let (producer, receiver) =
        TelemetryArena::reserve(&policy).expect("enabled policy creates arena");
    let value = Value::Array((0..1_000).map(Value::Integer).collect());

    assert!(
        producer
            .emit_log(LogEvent {
                event: "transform.customer_seen",
                severity: Severity::Info,
                message: "bounded typed value",
                correlation: correlation(),
                fields: &[SignalField::from_record("customer_id", &value)],
            })
            .is_accepted()
    );

    let batch = receiver
        .try_recv_batch()
        .expect("bounded typed record value is drainable");
    let rendered = batch.logs()[0]
        .fields
        .get("customer_id")
        .expect("allowed typed field is retained");
    assert!(rendered.len() <= 64);
    assert_eq!(producer.snapshot().truncated_fields, 1);
}

#[test]
fn transform_dispatch_emits_typed_lifecycle_record_error_metrics_and_spans() {
    let disabled = run_transform_dispatch(3, None);
    let policy = transform_policy("4KB", "2KB", "6KB", "64B", 1, 10_000, 10_000);
    let enabled = run_transform_dispatch(3, Some(&policy));

    assert_eq!(
        enabled.etl, disabled.etl,
        "telemetry acceptance cannot alter output, counters, or DLQ decisions"
    );
    assert_eq!(enabled.etl.counters.0, 3);
    assert_eq!(enabled.etl.counters.1, 2);
    assert_eq!(enabled.etl.counters.2, 2);
    assert_eq!(enabled.etl.counters.3, 1);
    assert_eq!(enabled.etl.dlq.len(), 1);

    let batch = enabled
        .batch
        .expect("typed transform telemetry is drainable");
    let starting = log(batch.logs(), "transform.starting");
    assert_eq!(starting.severity, Severity::Debug);
    assert_eq!(starting.message, "Starting customer transform");
    // No field_policy rule names these, and none is required: run correlation
    // is engine-supplied identity, not record data under privacy policy.
    assert_eq!(starting.correlation.execution_id, "execution-123456789");
    assert_eq!(starting.correlation.batch_id, "batch-987654321");
    assert_eq!(
        starting.correlation.pipeline_name,
        "transform_observability_runtime"
    );
    assert!(
        !starting.fields.contains_key("execution_id"),
        "correlation is not a policy-gated record field"
    );

    let completed = log(batch.logs(), "transform.completed");
    assert_eq!(completed.severity, Severity::Info);
    assert_eq!(completed.message, "Customer transform completed");

    let seen = batch
        .logs()
        .iter()
        .filter(|record| record.event == "transform.customer_seen")
        .collect::<Vec<_>>();
    assert_eq!(
        seen.len(),
        3,
        "per-record cadence every=1 fires once per row"
    );
    assert_eq!(
        seen[0].fields.get("customer_id").map(String::as_str),
        Some("customer-1")
    );
    assert!(
        seen[0]
            .fields
            .get("email")
            .is_some_and(|value| value.starts_with("blake3:"))
    );
    assert_eq!(
        seen[0].fields.get("region").map(String::as_str),
        Some("[region]")
    );
    assert!(!seen[0].fields.contains_key("secret"));

    let failed = log(batch.logs(), "transform.customer_failed");
    assert_eq!(failed.severity, Severity::Error);
    assert_eq!(failed.message, "Customer transform failed");
    assert_eq!(
        failed.fields.get("customer_id").map(String::as_str),
        Some("customer-2")
    );
    assert!(!failed.fields.contains_key("secret"));

    let json = serde_json::to_string(&batch).expect("typed batch serializes");
    assert!(
        !json.contains("secret-"),
        "denied record fields never cross the arena"
    );
    assert!(
        !json.contains("not-an-integer"),
        "raw evaluator errors never cross the arena"
    );
    assert!(
        !json.contains("customers.csv"),
        "source paths are not implicit attributes"
    );

    assert_eq!(batch.metric(MetricKey::TransformStarted), 1);
    assert_eq!(batch.metric(MetricKey::TransformCompleted), 1);
    assert_eq!(batch.metric(MetricKey::TransformRecords), 3);
    assert_eq!(batch.metric(MetricKey::TransformErrors), 1);
    assert_transform_span_is_complete(batch.traces());

    // Every emitted event lands with three correlation values and none of them
    // is counted as a privacy denial. `secret` is the only denied field, on the
    // two events that request it.
    let snapshot = enabled
        .snapshot
        .expect("enabled run has exact producer accounting");
    assert_eq!(
        snapshot.denied_fields, 4,
        "only requested record fields with no rule are denials"
    );
    for record in batch.logs() {
        assert_eq!(record.correlation.execution_id, "execution-123456789");
        assert_eq!(record.correlation.batch_id, "batch-987654321");
    }
}

#[test]
fn transform_dispatch_loss_and_truncation_paths_preserve_etl_outcomes() {
    let disabled = run_transform_dispatch(48, None);
    let cases = [
        (
            "truncated",
            transform_policy("8KB", "4KB", "12KB", "8B", 1, 100_000, 100_000),
        ),
        (
            "sampled",
            transform_policy("8KB", "4KB", "12KB", "64B", 7, 100_000, 100_000),
        ),
        (
            "rate_limited",
            transform_policy("8KB", "4KB", "12KB", "64B", 1, 1, 1),
        ),
        (
            "full",
            transform_policy("1KB", "1KB", "2KB", "64B", 1, 100_000, 100_000),
        ),
    ];

    for (name, policy) in cases {
        let enabled = run_transform_dispatch(48, Some(&policy));
        assert_eq!(
            enabled.etl, disabled.etl,
            "{name} telemetry outcome cannot alter output, counters, or DLQ decisions"
        );
        let snapshot = enabled
            .snapshot
            .expect("enabled run has exact producer accounting");
        assert!(snapshot.retained_bytes <= snapshot.owned_bytes, "{name}");
        assert!(
            snapshot.peak_retained_bytes <= snapshot.owned_bytes,
            "{name}"
        );
        match name {
            "truncated" => assert!(snapshot.truncated_fields > 0),
            "sampled" => assert!(snapshot.sampled_drops > 0),
            "rate_limited" => assert!(snapshot.rate_limited_drops > 0),
            "full" => assert!(snapshot.full_drops > 0),
            _ => unreachable!(),
        }
    }
}

/// Admit twenty Error events under `sample_every = 10`, interleaving
/// `info_per_error` Info events before each one, and report how many Errors
/// survived sampling. Both lanes are drained every round so lane capacity
/// never stands in for the sampling decision.
fn admitted_high_severity(info_per_error: usize) -> usize {
    let policy = transform_policy("8KB", "4KB", "12KB", "64B", 10, 100_000, 100_000);
    let (producer, receiver) =
        TelemetryArena::reserve(&policy).expect("enabled policy creates arena");
    let info = LogEvent {
        event: "transform.customer_seen",
        severity: Severity::Info,
        message: "ordinary volume",
        correlation: correlation(),
        fields: &[],
    };
    let error = LogEvent {
        event: "transform.customer_failed",
        severity: Severity::Error,
        message: "static failure",
        correlation: correlation(),
        fields: &[],
    };

    let mut admitted = 0;
    for _ in 0..20 {
        for _ in 0..info_per_error {
            let _ = producer.emit_log(info);
        }
        if producer.emit_log(error).is_accepted() {
            admitted += 1;
        }
        let _ = receiver.try_recv_batch();
    }
    admitted
}

#[test]
fn sampling_counts_within_each_lane_so_ordinary_volume_cannot_discard_high_severity() {
    let quiet = admitted_high_severity(0);
    let flooded = admitted_high_severity(9);

    assert_eq!(
        quiet, 2,
        "sample_every = 10 admits one in ten high-severity events"
    );
    assert_eq!(
        flooded, quiet,
        "the two lanes are disjoint so a flood of Info cannot change which \
         Warn/Error events survive sampling"
    );
}

/// The documented sampling guarantee is checkable from a run's own counters.
///
/// `sampling_counts_within_each_lane_…` proves the behaviour by counting
/// admissions from outside. An author cannot do that on their own pipeline —
/// all they have is the run's reported accounting. Splitting `sampled_drops`
/// per lane is what turns "a Transform's errors keep their one-in-ten share
/// regardless of `info` volume" from a promise in the documentation into
/// something the run itself demonstrates.
#[test]
fn per_lane_sampling_counters_show_the_error_share_holding_under_ordinary_volume() {
    let policy = transform_policy("8KB", "4KB", "12KB", "64B", 10, 100_000, 100_000);
    let (producer, receiver) =
        TelemetryArena::reserve(&policy).expect("enabled policy creates arena");
    let info = LogEvent {
        event: "transform.customer_seen",
        severity: Severity::Info,
        message: "ordinary volume",
        correlation: correlation(),
        fields: &[],
    };
    let error = LogEvent {
        event: "transform.customer_failed",
        severity: Severity::Error,
        message: "static failure",
        correlation: correlation(),
        fields: &[],
    };

    for _ in 0..20 {
        for _ in 0..9 {
            let _ = producer.emit_log(info);
        }
        let _ = producer.emit_log(error);
        let _ = receiver.try_recv_batch();
    }

    let snapshot = producer.snapshot();
    // Twenty errors at one in ten: two admitted, eighteen sampled away. The
    // number does not move with the 180 Info events beside it.
    assert_eq!(
        snapshot.high_sampled_drops, 18,
        "high-severity sampling is unaffected by ordinary volume: {snapshot:?}"
    );
    assert_eq!(
        snapshot.ordinary_sampled_drops, 162,
        "180 Info events at one in ten: {snapshot:?}"
    );
    assert_eq!(
        snapshot.sampled_drops,
        snapshot.ordinary_sampled_drops + snapshot.high_sampled_drops,
        "the total is exactly the two lanes"
    );
}

#[test]
fn run_correlation_crosses_the_arena_without_a_field_policy_rule() {
    // policy_with_lanes declares rules for customer_id, email, and region only.
    let policy = policy_with_lanes("3KB", "1KB", "4KB");
    let (producer, receiver) =
        TelemetryArena::reserve(&policy).expect("enabled policy creates arena");

    assert!(
        producer
            .emit_log(LogEvent {
                event: "transform.customer_seen",
                severity: Severity::Info,
                message: "customer observed",
                correlation: correlation(),
                fields: &[],
            })
            .is_accepted()
    );

    let batch = receiver
        .try_recv_batch()
        .expect("one bounded batch is ready");
    let record = &batch.logs()[0];
    assert_eq!(record.correlation.execution_id, "execution-123456789");
    assert_eq!(record.correlation.batch_id, "batch-987654321");
    assert_eq!(
        record.correlation.pipeline_name,
        "transform_observability_runtime"
    );
    assert_eq!(
        producer.snapshot().denied_fields,
        0,
        "engine-supplied run correlation is never a privacy denial"
    );
}
