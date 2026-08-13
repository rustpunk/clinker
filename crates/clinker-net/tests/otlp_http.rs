//! OTLP/HTTP transport contract tests.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use clinker_core_types::{FailureCategory, RetryAdvice};
use ureq::http::HeaderMap;
use ureq::http::header::{AUTHORIZATION, HeaderName, HeaderValue};

#[path = "../src/otlp.rs"]
#[allow(dead_code)]
mod otlp_under_test;

use otlp_under_test::{
    OtlpAuthentication, OtlpCredentialApplicationError, OtlpCredentialApplicator,
    OtlpCredentialRequest, OtlpDeliveryBounds, OtlpDeliveryBudget, OtlpDeliveryFailureKind,
    OtlpRetryCause, OtlpSignal, admit_otlp_endpoint, admitted_loopback_endpoint, send_otlp_json,
};

#[derive(Debug)]
struct CapturedRequest {
    method: String,
    target: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

fn read_request(stream: &mut TcpStream) -> CapturedRequest {
    let mut reader = BufReader::new(stream.try_clone().expect("clone fixture stream"));
    let mut request_line = String::new();
    reader
        .read_line(&mut request_line)
        .expect("read request line");
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().expect("request method").to_owned();
    let target = request_parts.next().expect("request target").to_owned();

    let mut headers = BTreeMap::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).expect("read request header");
        if line == "\r\n" || line == "\n" {
            break;
        }
        let (name, value) = line.split_once(':').expect("header separator");
        headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
    }

    let content_length = headers
        .get("content-length")
        .expect("content-length")
        .parse::<usize>()
        .expect("numeric content-length");
    let mut body = vec![0_u8; content_length];
    reader.read_exact(&mut body).expect("read request body");
    CapturedRequest {
        method,
        target,
        headers,
        body,
    }
}

fn spawn_server(
    response_status: u16,
    response_body: Vec<u8>,
) -> (std::net::SocketAddr, thread::JoinHandle<CapturedRequest>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind OTLP fixture");
    let address = listener.local_addr().expect("fixture address");
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept OTLP request");
        let captured = read_request(&mut stream);
        let reason = if response_status == 200 {
            "OK"
        } else {
            "Fixture"
        };
        let response = format!(
            "HTTP/1.1 {response_status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            response_body.len()
        );
        stream
            .write_all(response.as_bytes())
            .expect("write response headers");
        stream
            .write_all(&response_body)
            .expect("write response body");
        stream.flush().expect("flush response");
        captured
    });
    (address, handle)
}

struct FixtureResponse {
    status: u16,
    body: Vec<u8>,
    header_delay: Duration,
    body_delay: Duration,
}

impl FixtureResponse {
    fn immediate(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            body,
            header_delay: Duration::ZERO,
            body_delay: Duration::ZERO,
        }
    }

    fn delayed_headers(delay: Duration) -> Self {
        Self {
            status: 200,
            body: br#"{}"#.to_vec(),
            header_delay: delay,
            body_delay: Duration::ZERO,
        }
    }

    fn delayed_body(delay: Duration) -> Self {
        Self {
            status: 200,
            body: br#"{}"#.to_vec(),
            header_delay: Duration::ZERO,
            body_delay: delay,
        }
    }
}

fn spawn_sequence_server(
    responses: Vec<FixtureResponse>,
) -> (
    std::net::SocketAddr,
    thread::JoinHandle<Vec<CapturedRequest>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind OTLP sequence fixture");
    let address = listener.local_addr().expect("sequence fixture address");
    let handle = thread::spawn(move || {
        let mut workers = Vec::with_capacity(responses.len());
        for response in responses {
            let (mut stream, _) = listener.accept().expect("accept OTLP sequence request");
            workers.push(thread::spawn(move || {
                let captured = read_request(&mut stream);
                thread::sleep(response.header_delay);
                let reason = if response.status == 200 {
                    "OK"
                } else {
                    "Fixture"
                };
                let headers = format!(
                    "HTTP/1.1 {} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    response.status,
                    response.body.len()
                );
                let _ = stream.write_all(headers.as_bytes());
                let _ = stream.flush();
                thread::sleep(response.body_delay);
                let _ = stream.write_all(&response.body);
                let _ = stream.flush();
                captured
            }));
        }
        workers
            .into_iter()
            .map(|worker| worker.join().expect("join OTLP response worker"))
            .collect()
    });
    (address, handle)
}

fn budget(max_attempts: u32) -> OtlpDeliveryBudget {
    OtlpDeliveryBudget::new(OtlpDeliveryBounds {
        max_request_bytes: 64 * 1024,
        max_response_bytes: 4 * 1024,
        max_attempts,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(2),
        retry_backoff: Duration::from_millis(1),
        total_timeout: Duration::from_secs(5),
    })
    .expect("valid fixture budget")
}

fn response_budget(max_attempts: u32, max_response_bytes: u64) -> OtlpDeliveryBudget {
    OtlpDeliveryBudget::new(OtlpDeliveryBounds {
        max_request_bytes: 64 * 1024,
        max_response_bytes,
        max_attempts,
        connect_timeout: Duration::from_secs(1),
        request_timeout: Duration::from_millis(40),
        retry_backoff: Duration::from_millis(1),
        total_timeout: Duration::from_secs(2),
    })
    .expect("valid response fixture budget")
}

fn total_timeout_budget() -> OtlpDeliveryBudget {
    OtlpDeliveryBudget::new(OtlpDeliveryBounds {
        max_request_bytes: 64 * 1024,
        max_response_bytes: 4 * 1024,
        max_attempts: 2,
        connect_timeout: Duration::from_secs(1),
        request_timeout: Duration::from_secs(1),
        retry_backoff: Duration::from_millis(40),
        total_timeout: Duration::from_millis(40),
    })
    .expect("valid total-timeout fixture budget")
}

fn logs_payload() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "resourceLogs": [{
            "scopeLogs": [{
                "logRecords": [
                    {"timeUnixNano": "1", "body": {"stringValue": "first"}},
                    {"timeUnixNano": "2", "body": {"stringValue": "second"}}
                ]
            }]
        }]
    }))
    .expect("serialize fixture logs")
}

/// Shaped like what the exporter actually sends: the executor's counters are
/// drained as per-flush deltas, so they travel as monotonic sums with explicit
/// delta temporality and both required timestamps, never as gauges.
fn metrics_payload() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "resourceMetrics": [{
            "scopeMetrics": [{
                "metrics": [{
                    "name": "items.processed",
                    "unit": "1",
                    "sum": {
                        "aggregationTemporality": 1,
                        "isMonotonic": true,
                        "dataPoints": [
                            {"startTimeUnixNano": "1", "timeUnixNano": "2", "asInt": "1"},
                            {"startTimeUnixNano": "2", "timeUnixNano": "3", "asInt": "2"}
                        ]
                    }
                }]
            }]
        }]
    }))
    .expect("serialize fixture metrics")
}

fn traces_payload() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "resourceSpans": [{
            "scopeSpans": [{
                "spans": [
                    {"traceId": "00112233445566778899aabbccddeeff", "spanId": "0011223344556677"},
                    {"traceId": "ffeeddccbbaa99887766554433221100", "spanId": "7766554433221100"}
                ]
            }]
        }]
    }))
    .expect("serialize fixture traces")
}

struct FixtureCredential {
    applied_after_admission: Arc<AtomicBool>,
}

struct RefusingCredential;

impl OtlpCredentialApplicator for RefusingCredential {
    fn apply(
        &self,
        _request: &mut OtlpCredentialRequest<'_>,
    ) -> Result<(), OtlpCredentialApplicationError> {
        Err(OtlpCredentialApplicationError::Unavailable)
    }
}

impl OtlpCredentialApplicator for FixtureCredential {
    fn apply(
        &self,
        request: &mut OtlpCredentialRequest<'_>,
    ) -> Result<(), OtlpCredentialApplicationError> {
        assert_eq!(request.admitted_origin().scheme_str(), Some("http"));
        self.applied_after_admission.store(true, Ordering::SeqCst);
        request.insert_header(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer fixture-value"),
        )
    }
}

#[test]
fn endpoint_admission_and_successful_post() {
    for accepted in [
        "https://collector.example.com",
        "https://collector.example.com/",
        "https://COLLECTOR.EXAMPLE.COM:443",
        "https://collector.example.com:4318/",
    ] {
        clinker_net::admit_otlp_endpoint(accepted)
            .unwrap_or_else(|error| panic!("expected admitted endpoint for {accepted:?}: {error}"));
    }

    for rejected in [
        "not a uri",
        "/relative",
        "http://collector.example.com",
        "https://user:pass@collector.example.com",
        "https://collector.example.com/base",
        "https://collector.example.com?tenant=secret",
        "https://collector.example.com#fragment",
        "https://collector.example.com/v1/logs",
    ] {
        let error = match clinker_net::admit_otlp_endpoint(rejected) {
            Ok(_) => panic!("expected endpoint rejection for {rejected:?}"),
            Err(error) => error,
        };
        let rendered = error.to_string();
        assert!(rendered.contains("observability.otlp.endpoint"));
        assert!(rendered.contains("https://collector.example.com"));
        assert!(!rendered.contains(rejected));
        assert!(!rendered.contains("user:pass"));
        assert!(!rendered.contains("tenant=secret"));
    }

    let payload = logs_payload();
    let (address, handle) = spawn_server(200, br#"{}"#.to_vec());
    let endpoint = admitted_loopback_endpoint(address);
    let outcome = send_otlp_json(
        &endpoint,
        OtlpSignal::Logs,
        &payload,
        &budget(1),
        &|| false,
        OtlpAuthentication::None,
    )
    .expect("credential-free OTLP delivery");
    let captured = handle.join().expect("join credential-free fixture");

    assert_eq!(outcome.signal(), OtlpSignal::Logs);
    assert_eq!(outcome.accepted(), 2);
    assert_eq!(outcome.rejected(), 0);
    assert_eq!(outcome.attempts(), 1);
    assert_eq!(captured.method, "POST");
    assert_eq!(captured.target, "/v1/logs");
    assert_eq!(
        captured.headers.get("content-type").map(String::as_str),
        Some("application/json")
    );
    assert_eq!(
        captured
            .headers
            .get("content-length")
            .and_then(|value| value.parse::<usize>().ok()),
        Some(payload.len())
    );
    assert_eq!(captured.body, payload);
    assert!(!captured.headers.contains_key("authorization"));
    assert!(!captured.headers.contains_key("x-api-key"));

    let applied_after_admission = Arc::new(AtomicBool::new(false));
    let credential = FixtureCredential {
        applied_after_admission: Arc::clone(&applied_after_admission),
    };
    let (address, handle) = spawn_server(200, br#"{}"#.to_vec());
    let endpoint = admitted_loopback_endpoint(address);
    let outcome = send_otlp_json(
        &endpoint,
        OtlpSignal::Logs,
        &payload,
        &budget(1),
        &|| false,
        OtlpAuthentication::Referenced(&credential),
    )
    .expect("referenced OTLP delivery");
    let captured = handle.join().expect("join referenced fixture");

    assert_eq!(outcome.accepted(), 2);
    assert!(applied_after_admission.load(Ordering::SeqCst));
    assert_eq!(
        captured.headers.get("authorization").map(String::as_str),
        Some("Bearer fixture-value")
    );

    let mut request_headers = HeaderMap::new();
    for (name, value) in captured.headers {
        request_headers.insert(
            HeaderName::from_bytes(name.as_bytes()).expect("typed captured header"),
            HeaderValue::from_str(&value).expect("typed captured value"),
        );
    }
    assert_eq!(
        request_headers.get(AUTHORIZATION).unwrap(),
        "Bearer fixture-value"
    );
}

#[test]
fn logs_metrics_traces_and_fault_matrix() {
    let unreachable_endpoint = admitted_loopback_endpoint("127.0.0.1:1".parse().unwrap());
    let failure = send_otlp_json(
        &unreachable_endpoint,
        OtlpSignal::Logs,
        br#"{"notResourceLogs":[]}"#,
        &budget(1),
        &|| false,
        OtlpAuthentication::None,
    )
    .expect_err("wrong-signal payload must fail before request construction");
    assert_eq!(failure.kind(), OtlpDeliveryFailureKind::InvalidPayload);
    assert_eq!(failure.attempts(), 0);

    let request_cap = OtlpDeliveryBudget::new(OtlpDeliveryBounds {
        max_request_bytes: 1,
        max_response_bytes: 4 * 1024,
        max_attempts: 1,
        connect_timeout: Duration::from_secs(1),
        request_timeout: Duration::from_secs(1),
        retry_backoff: Duration::ZERO,
        total_timeout: Duration::from_secs(2),
    })
    .expect("valid request-cap fixture budget");
    let failure = send_otlp_json(
        &unreachable_endpoint,
        OtlpSignal::Logs,
        &logs_payload(),
        &request_cap,
        &|| false,
        OtlpAuthentication::None,
    )
    .expect_err("request cap must fail before request construction");
    assert_eq!(failure.kind(), OtlpDeliveryFailureKind::RequestTooLarge);
    assert_eq!(failure.attempts(), 0);

    let failure = send_otlp_json(
        &unreachable_endpoint,
        OtlpSignal::Logs,
        &logs_payload(),
        &budget(1),
        &|| false,
        OtlpAuthentication::Referenced(&RefusingCredential),
    )
    .expect_err("credential refusal must remain typed and sanitized");
    assert_eq!(
        failure.kind(),
        OtlpDeliveryFailureKind::CredentialApplication
    );
    assert_eq!(failure.attempts(), 1);

    for (signal, payload, route) in [
        (OtlpSignal::Logs, logs_payload(), "/v1/logs"),
        (OtlpSignal::Metrics, metrics_payload(), "/v1/metrics"),
        (OtlpSignal::Traces, traces_payload(), "/v1/traces"),
    ] {
        let (address, handle) = spawn_server(200, br#"{}"#.to_vec());
        let endpoint = admitted_loopback_endpoint(address);
        let outcome = send_otlp_json(
            &endpoint,
            signal,
            &payload,
            &budget(1),
            &|| false,
            OtlpAuthentication::None,
        )
        .expect("fixed-route OTLP delivery");
        let captured = handle.join().expect("join fixed-route fixture");
        assert_eq!(captured.target, route);
        assert_eq!(captured.body, payload);
        assert_eq!(outcome.accepted(), 2);
        assert_eq!(outcome.rejected(), 0);
        assert!(!captured.headers.contains_key("authorization"));
    }

    for (signal, payload, rejected_key) in [
        (OtlpSignal::Logs, logs_payload(), "rejectedLogRecords"),
        (OtlpSignal::Metrics, metrics_payload(), "rejectedDataPoints"),
        (OtlpSignal::Traces, traces_payload(), "rejectedSpans"),
    ] {
        let response = serde_json::to_vec(&serde_json::json!({
            "partialSuccess": {rejected_key: "1", "errorMessage": "fixture rejection"}
        }))
        .expect("serialize partial-success fixture");
        let (address, handle) = spawn_server(200, response);
        let endpoint = admitted_loopback_endpoint(address);
        let outcome = send_otlp_json(
            &endpoint,
            signal,
            &payload,
            &budget(1),
            &|| false,
            OtlpAuthentication::None,
        )
        .expect("signal-specific partial success");
        handle.join().expect("join partial-success fixture");
        assert_eq!(outcome.accepted(), 1);
        assert_eq!(outcome.rejected(), 1);
    }

    let (address, handle) = spawn_sequence_server(vec![
        FixtureResponse::immediate(503, br#"{"collector":"unavailable"}"#.to_vec()),
        FixtureResponse::immediate(200, br#"{}"#.to_vec()),
    ]);
    let endpoint = admitted_loopback_endpoint(address);
    let outcome = send_otlp_json(
        &endpoint,
        OtlpSignal::Logs,
        &logs_payload(),
        &budget(2),
        &|| false,
        OtlpAuthentication::None,
    )
    .expect("bounded transient-status retry");
    assert_eq!(outcome.attempts(), 2);
    assert_eq!(handle.join().expect("join retry fixture").len(), 2);

    for (status, cause) in [
        (429, OtlpRetryCause::CollectorThrottled),
        (502, OtlpRetryCause::CollectorUnavailable),
        (503, OtlpRetryCause::CollectorUnavailable),
        (504, OtlpRetryCause::CollectorUnavailable),
    ] {
        let secret_body = br#"{"authorization":"Bearer must-not-escape"}"#.to_vec();
        let (address, handle) = spawn_server(status, secret_body);
        let endpoint = admitted_loopback_endpoint(address);
        let failure = send_otlp_json(
            &endpoint,
            OtlpSignal::Logs,
            &logs_payload(),
            &budget(1),
            &|| false,
            OtlpAuthentication::None,
        )
        .expect_err("retryable status must exhaust the finite one-attempt budget");
        handle.join().expect("join exhausted-status fixture");
        assert_eq!(
            failure.kind(),
            OtlpDeliveryFailureKind::RetryExhausted(cause)
        );
        let classification = failure.classification().expect("classified retry advice");
        assert_eq!(classification.category(), FailureCategory::Observability);
        assert_eq!(classification.retry_advice(), RetryAdvice::RetryWithBackoff);
        assert!(!failure.to_string().contains("must-not-escape"));
        assert!(!format!("{failure:?}").contains("Bearer must-not-escape"));
    }

    let (address, handle) = spawn_server(
        400,
        br#"{"error":"tenant and credential details must-not-escape"}"#.to_vec(),
    );
    let endpoint = admitted_loopback_endpoint(address);
    let failure = send_otlp_json(
        &endpoint,
        OtlpSignal::Logs,
        &logs_payload(),
        &budget(3),
        &|| false,
        OtlpAuthentication::None,
    )
    .expect_err("permanent 4xx rejection");
    handle.join().expect("join permanent-rejection fixture");
    assert_eq!(failure.kind(), OtlpDeliveryFailureKind::CollectorRejected);
    assert_eq!(failure.attempts(), 1);
    assert_eq!(
        failure
            .classification()
            .expect("classified permanent rejection")
            .retry_advice(),
        RetryAdvice::PolicyRequired
    );
    assert!(!failure.to_string().contains("must-not-escape"));

    let (address, handle) =
        spawn_server(200, br#"{"partialSuccess":{"rejectedSpans":"1"}}"#.to_vec());
    let endpoint = admitted_loopback_endpoint(address);
    let failure = send_otlp_json(
        &endpoint,
        OtlpSignal::Logs,
        &logs_payload(),
        &budget(1),
        &|| false,
        OtlpAuthentication::None,
    )
    .expect_err("wrong-signal partial success must fail closed");
    handle.join().expect("join malformed-response fixture");
    assert_eq!(failure.kind(), OtlpDeliveryFailureKind::MalformedResponse);

    let (address, handle) = spawn_server(200, br#"{}padding"#.to_vec());
    let endpoint = admitted_loopback_endpoint(address);
    let failure = send_otlp_json(
        &endpoint,
        OtlpSignal::Logs,
        &logs_payload(),
        &response_budget(1, 2),
        &|| false,
        OtlpAuthentication::None,
    )
    .expect_err("response cap must fail before parsing");
    handle.join().expect("join oversized-response fixture");
    assert_eq!(failure.kind(), OtlpDeliveryFailureKind::ResponseTooLarge);

    let (address, handle) = spawn_server(503, br#"{}"#.to_vec());
    let endpoint = admitted_loopback_endpoint(address);
    let failure = send_otlp_json(
        &endpoint,
        OtlpSignal::Logs,
        &logs_payload(),
        &total_timeout_budget(),
        &|| false,
        OtlpAuthentication::None,
    )
    .expect_err("total worker deadline must bound retry backoff");
    handle.join().expect("join total-timeout fixture");
    assert_eq!(failure.kind(), OtlpDeliveryFailureKind::Timeout);
    assert_eq!(failure.attempts(), 1);

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind connect-failure fixture");
    let unavailable_address = listener.local_addr().expect("connect-failure address");
    drop(listener);
    let endpoint = admitted_loopback_endpoint(unavailable_address);
    let failure = send_otlp_json(
        &endpoint,
        OtlpSignal::Logs,
        &logs_payload(),
        &budget(1),
        &|| false,
        OtlpAuthentication::None,
    )
    .expect_err("connect failure must be typed");
    assert_eq!(
        failure.kind(),
        OtlpDeliveryFailureKind::RetryExhausted(OtlpRetryCause::Connect)
    );

    let failure = send_otlp_json(
        &unreachable_endpoint,
        OtlpSignal::Metrics,
        &metrics_payload(),
        &budget(2),
        &|| true,
        OtlpAuthentication::None,
    )
    .expect_err("shutdown must stop before request construction");
    assert_eq!(failure.kind(), OtlpDeliveryFailureKind::Shutdown);
    assert_eq!(failure.attempts(), 0);
    assert!(failure.classification().is_none());

    // A reply that never starts arriving comes after the request was fully
    // written, so the collector may already hold this batch and be slow to say
    // so. That is the same situation as the delayed body below with less
    // information, and re-sending against a collector whose replies are slower
    // than `request_timeout` would export every batch once per attempt.
    let (address, handle) = spawn_sequence_server(vec![FixtureResponse::delayed_headers(
        Duration::from_millis(120),
    )]);
    let endpoint = admitted_loopback_endpoint(address);
    let failure = send_otlp_json(
        &endpoint,
        OtlpSignal::Traces,
        &traces_payload(),
        &response_budget(2, 4 * 1024),
        &|| false,
        OtlpAuthentication::None,
    )
    .expect_err("a batch the collector may already hold is not sent a second time");
    assert_eq!(failure.kind(), OtlpDeliveryFailureKind::Timeout);
    assert_eq!(
        failure.attempts(),
        1,
        "a request that was fully written must not be written again"
    );
    assert_eq!(handle.join().expect("join header-timeout fixture").len(), 1);

    // A body that never arrives comes after a 200, and a 200 means the
    // collector already has the batch. Re-sending it would ingest the same
    // records twice and count the same monotonic sums twice, so what is lost
    // here is the confirmation, not the delivery, and the attempt ends.
    let (address, handle) = spawn_sequence_server(vec![FixtureResponse::delayed_body(
        Duration::from_millis(120),
    )]);
    let endpoint = admitted_loopback_endpoint(address);
    let failure = send_otlp_json(
        &endpoint,
        OtlpSignal::Metrics,
        &metrics_payload(),
        &response_budget(2, 4 * 1024),
        &|| false,
        OtlpAuthentication::None,
    )
    .expect_err("an unreadable reply to an accepted batch is not a delivery to repeat");
    assert_eq!(
        failure.attempts(),
        1,
        "an accepted batch must never be sent a second time"
    );
    assert_eq!(failure.kind(), OtlpDeliveryFailureKind::Timeout);
    assert_eq!(handle.join().expect("join body-timeout fixture").len(), 1);

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind TLS fixture");
    let address = listener.local_addr().expect("TLS fixture address");
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept TLS fixture");
        let _ = stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}");
        let _ = stream.flush();
    });
    let endpoint = admit_otlp_endpoint(&format!("https://{address}"))
        .expect("admit loopback HTTPS origin for TLS failure");
    let failure = send_otlp_json(
        &endpoint,
        OtlpSignal::Logs,
        &logs_payload(),
        &budget(1),
        &|| false,
        OtlpAuthentication::None,
    )
    .expect_err("plaintext collector on HTTPS origin must be a TLS failure");
    handle.join().expect("join TLS fixture");
    assert_eq!(failure.kind(), OtlpDeliveryFailureKind::Tls);
    assert_eq!(failure.attempts(), 1);
}

/// The specification names one success status, and a rejection is something a
/// 4xx or a 5xx says. An ingest gateway in front of a collector answers
/// `202 Accepted` with the batch taken, and reading that as a permanent refusal
/// reported a delivered export as one no retry could ever fix.
#[test]
fn a_two_hundred_class_answer_is_a_delivery_rather_than_a_refusal() {
    for (status, body) in [
        (202_u16, br#"{}"#.to_vec()),
        (202, Vec::new()),
        // Nothing defines an export-service response for these statuses, so a
        // body that is not one must not be held against the delivery.
        (202, b"queued for ingestion".to_vec()),
        (204, Vec::new()),
    ] {
        let payload = logs_payload();
        let (address, handle) = spawn_server(status, body);
        let endpoint = admitted_loopback_endpoint(address);
        let outcome = send_otlp_json(
            &endpoint,
            OtlpSignal::Logs,
            &payload,
            &budget(1),
            &|| false,
            OtlpAuthentication::None,
        )
        .unwrap_or_else(|error| {
            panic!("{status} accepted the batch, but delivery failed: {error}")
        });
        let captured = handle.join().expect("join accepted-status fixture");
        assert_eq!(captured.body, payload);
        assert_eq!(outcome.accepted(), 2, "{status} took the whole chunk");
        assert_eq!(outcome.rejected(), 0, "{status} reported no rejection");
        assert_eq!(outcome.attempts(), 1);
    }
}

/// A cap an author writes is the largest reply they expect a collector to
/// send, so a reply of exactly that size has to be readable. The request side
/// already admits a payload sitting exactly at its cap.
#[test]
fn a_reply_of_exactly_the_response_cap_is_read() {
    let (address, handle) = spawn_server(200, br#"{}"#.to_vec());
    let endpoint = admitted_loopback_endpoint(address);
    let outcome = send_otlp_json(
        &endpoint,
        OtlpSignal::Logs,
        &logs_payload(),
        &response_budget(1, 2),
        &|| false,
        OtlpAuthentication::None,
    )
    .expect("a reply of exactly the cap is within the cap");
    handle.join().expect("join at-cap response fixture");
    assert_eq!(outcome.accepted(), 2);

    let (address, handle) = spawn_server(200, br#"{} "#.to_vec());
    let endpoint = admitted_loopback_endpoint(address);
    let failure = send_otlp_json(
        &endpoint,
        OtlpSignal::Logs,
        &logs_payload(),
        &response_budget(1, 2),
        &|| false,
        OtlpAuthentication::None,
    )
    .expect_err("one byte past the cap is over it");
    handle.join().expect("join over-cap response fixture");
    assert_eq!(failure.kind(), OtlpDeliveryFailureKind::ResponseTooLarge);
    assert!(
        failure.reached_collector(),
        "the collector answered 200 before its reply proved unreadable"
    );
}
