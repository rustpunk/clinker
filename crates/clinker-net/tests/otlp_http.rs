//! OTLP/HTTP transport contract tests.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use ureq::http::HeaderMap;
use ureq::http::header::{AUTHORIZATION, HeaderName, HeaderValue};

#[path = "../src/otlp.rs"]
#[allow(dead_code)]
mod otlp_under_test;

use otlp_under_test::{
    OtlpAuthentication, OtlpCredentialApplicationError, OtlpCredentialApplicator,
    OtlpCredentialRequest, OtlpDeliveryBudget, OtlpSignal, admitted_loopback_endpoint,
    send_otlp_json,
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
    response_body: &'static [u8],
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
            .write_all(response_body)
            .expect("write response body");
        stream.flush().expect("flush response");
        captured
    });
    (address, handle)
}

fn budget(max_attempts: u32) -> OtlpDeliveryBudget {
    OtlpDeliveryBudget::new(
        64 * 1024,
        4 * 1024,
        max_attempts,
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_millis(1),
        Duration::from_secs(5),
    )
    .expect("valid fixture budget")
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

struct FixtureCredential {
    applied_after_admission: Arc<AtomicBool>,
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
    let (address, handle) = spawn_server(200, br#"{}"#);
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
    let (address, handle) = spawn_server(200, br#"{}"#);
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
