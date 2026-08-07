use clinker_exec::telemetry::{
    LogEvent, MetricKey, Severity, SignalField, SpanFact, SpanName, SpanPhase, SpanStatus,
    TelemetryArena,
};
use clinker_plan::config::ClinkerToml;

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
drop_policy = "drop-newest"
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
drop_policy = "drop-newest"
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

#[test]
fn telemetry_arena_admits_private_three_signal_batch() {
    let policy = policy_with_lanes("3KB", "1KB", "4KB");
    let (producer, receiver) = TelemetryArena::new(&policy).expect("enabled policy creates arena");

    let log = LogEvent {
        event: "transform.customer_seen",
        severity: Severity::Info,
        message: "customer observed",
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
                phase: SpanPhase::End,
                status: SpanStatus::Ok,
                logical_node: "customer_seen",
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
    let (producer, receiver) = TelemetryArena::new(&policy).expect("enabled policy creates arena");

    let ordinary = LogEvent {
        event: "transform.customer_seen",
        severity: Severity::Info,
        message: "ordinary",
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
