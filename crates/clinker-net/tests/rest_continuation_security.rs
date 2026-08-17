//! Fail-closed continuation and redirect security contracts for REST sources.
#![cfg(feature = "transport")]

use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clinker_exec::source::RecordSource;
use clinker_net::build_rest_source;
use clinker_plan::config::{SourceTransport, parse_config};

// Each case opens real loopback sockets. Keep the cases sequential so the
// transport assertions measure product behavior rather than host socket
// teardown scheduling.
static NETWORK_TEST_LOCK: Mutex<()> = Mutex::new(());

fn network_test_guard() -> std::sync::MutexGuard<'static, ()> {
    NETWORK_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct TestServer {
    url: String,
    requests: Arc<Mutex<Vec<String>>>,
    errors: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl TestServer {
    fn spawn(responses: Vec<String>) -> Self {
        Self::spawn_with(|_| responses)
    }

    fn spawn_with(make_responses: impl FnOnce(&str) -> Vec<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        listener
            .set_nonblocking(true)
            .expect("set test server nonblocking");
        let addr = listener.local_addr().expect("test server address");
        let url = format!("http://{addr}");
        let responses = make_responses(&url);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let thread_requests = Arc::clone(&requests);
        let errors = Arc::new(Mutex::new(Vec::new()));
        let thread_errors = Arc::clone(&errors);
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let mut responses = responses.into_iter();
            let mut completed_connections = Vec::new();
            while !thread_stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        // Accepted sockets inherit nonblocking mode on BSD-family
                        // systems but not on Linux. Normalize the stream before
                        // applying the request read timeout below.
                        if let Err(error) = stream.set_nonblocking(false) {
                            let message =
                                format!("test server could not restore blocking mode: {error}");
                            eprintln!("{message}");
                            thread_errors
                                .lock()
                                .expect("server error lock")
                                .push(message);
                            continue;
                        }
                        let path = match read_request(&mut stream) {
                            Ok(Some(path)) => path,
                            Ok(None) => {
                                let message = "test server connection closed before a request line"
                                    .to_owned();
                                eprintln!("{message}");
                                thread_errors
                                    .lock()
                                    .expect("server error lock")
                                    .push(message);
                                continue;
                            }
                            Err(error) => {
                                let message = format!("test server request read failed: {error}");
                                eprintln!("{message}");
                                thread_errors
                                    .lock()
                                    .expect("server error lock")
                                    .push(message);
                                continue;
                            }
                        };
                        thread_requests.lock().expect("request lock").push(path);
                        let response = responses.next().unwrap_or_else(|| {
                            response(500, "Internal Server Error", &[], r#"{"unexpected":true}"#)
                        });
                        if let Err(error) = stream
                            .write_all(response.as_bytes())
                            .and_then(|()| stream.flush())
                        {
                            let message = format!("test server response write failed: {error}");
                            eprintln!("{message}");
                            thread_errors
                                .lock()
                                .expect("server error lock")
                                .push(message);
                            continue;
                        }
                        // Content-Length frames the response. Keep the socket
                        // alive until fixture teardown so a server-initiated
                        // close cannot race delivery of the final bytes.
                        completed_connections.push(stream);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("accept test request: {error}"),
                }
            }
        });
        Self {
            url,
            requests,
            errors,
            stop,
            handle: Some(handle),
        }
    }

    fn paths(&self) -> Vec<String> {
        self.requests.lock().expect("request lock").clone()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            handle.join().expect("join test server");
        }
        if !thread::panicking() {
            let errors = self.errors.lock().expect("server error lock");
            assert!(
                errors.is_empty(),
                "test server transport failures: {}",
                errors.join("; ")
            );
        }
    }
}

fn read_request(stream: &mut TcpStream) -> io::Result<Option<String>> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut reader = BufReader::new(&mut *stream);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(None);
    }
    loop {
        let mut line = String::new();
        let read = reader.read_line(&mut line)?;
        if read == 0 || line == "\r\n" || line == "\n" {
            break;
        }
    }
    Ok(request_line.split_whitespace().nth(1).map(str::to_owned))
}

fn response(status: u16, reason: &str, headers: &[(&str, &str)], body: &str) -> String {
    let headers = headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect::<String>();
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n{body}",
        body.len()
    )
}

fn ok(headers: &[(&str, &str)], id: i64) -> String {
    response(200, "OK", headers, &format!(r#"[{{"id":{id}}}]"#))
}

fn reader(url: &str, max_pages: u32) -> Box<dyn RecordSource> {
    reader_with_retries(url, max_pages, 0)
}

fn reader_with_retries(url: &str, max_pages: u32, retries: u32) -> Box<dyn RecordSource> {
    reader_with_pagination(
        url,
        max_pages,
        retries,
        "        pagination:\n          strategy: link_header",
    )
}

fn reader_with_pagination(
    url: &str,
    max_pages: u32,
    retries: u32,
    pagination: &str,
) -> Box<dyn RecordSource> {
    let url = serde_json::to_string(url).expect("quote URL");
    let yaml = format!(
        r#"
pipeline:
  name: rest_continuation_security
nodes:
  - type: source
    name: api
    config:
      name: api
      type: json
      options:
        format: array
      transport:
        kind: rest
        url: {url}
        max_pages: {max_pages}
        retries: {retries}
        timeout_secs: 2
{pagination}
      schema:
        - {{ name: id, type: int }}
  - type: sink
    name: out
    input: api
    config:
      name: out
      type: csv
      path: out.csv
"#
    );
    let config = parse_config(&yaml).expect("parse REST security pipeline");
    let body = config.source_bodies().next().expect("REST source body");
    let SourceTransport::Rest(cfg) = body.source.transport.clone() else {
        panic!("expected REST transport")
    };
    build_rest_source(
        cfg,
        &body.source,
        body.schema.as_columns().expect("single-record schema"),
        body.on_unmapped.clone(),
    )
    .expect("build REST source")
}

fn drain_ids(reader: &mut dyn RecordSource) -> Result<Vec<i64>, String> {
    let mut ids = Vec::new();
    loop {
        match reader.next_record() {
            Ok(Some(record)) => {
                let Some(clinker_record::Value::Integer(id)) = record.get("id") else {
                    panic!("record id must be an integer")
                };
                ids.push(*id);
            }
            Ok(None) => return Ok(ids),
            Err(error) => return Err(error.to_string()),
        }
    }
}

fn assert_classification(error: &str, code: &str, category: &str, retry: &str) {
    assert!(error.contains(code), "missing failure code {code}: {error}");
    assert!(
        error.contains(category),
        "missing failure category {category}: {error}"
    );
    assert!(
        error.contains(retry),
        "missing retry advice {retry}: {error}"
    );
}

#[test]
fn request_failures_keep_safe_status_and_target_context() {
    let _guard = network_test_guard();
    let server = TestServer::spawn(vec![response(
        401,
        "Unauthorized",
        &[],
        r#"{"error":"vendor detail must not be reflected"}"#,
    )]);
    let configured = format!("{}/items?api_key=do-not-render", server.url);
    let mut source = reader_with_retries(&configured, 1, 3);

    let error = source
        .next_record()
        .expect_err("HTTP rejection must fail the source");
    assert_eq!(
        error.classification_code(),
        Some("rest.http.client_error"),
        "fatal client errors must never be advertised as retryable infrastructure"
    );
    let error = error.to_string();

    assert!(error.contains("class=http_status_401"), "{error}");
    assert!(error.contains("attempt=1"), "{error}");
    assert!(error.contains("page=1"), "{error}");
    assert!(error.contains("/items"), "{error}");
    assert!(
        !error.contains("api_key"),
        "query names must be redacted: {error}"
    );
    assert!(
        !error.contains("do-not-render"),
        "query values must be redacted: {error}"
    );
    assert!(
        !error.contains("vendor detail"),
        "response bodies must not enter diagnostics: {error}"
    );
    assert_eq!(server.paths(), ["/items?api_key=do-not-render"]);
}

#[test]
fn absolute_relative_and_query_only_links_resolve_against_effective_url() {
    let _guard = network_test_guard();
    let server = TestServer::spawn_with(|url| {
        let absolute = format!("{url}/items?page=2");
        vec![
            ok(
                &[
                    ("Link", &format!("<{absolute}>; rel=\"alternate next\"")),
                    ("Link", "</unrelated>; rel=prev"),
                ],
                1,
            ),
            ok(&[("Link", "<next/page>; rel=\"next alternate\"")], 2),
            ok(&[("Link", "<?page=4>; rel=next")], 3),
            ok(&[], 4),
        ]
    });
    let mut reader = reader(&format!("{}/items", server.url), 10);
    let ids = drain_ids(reader.as_mut()).expect("valid continuation chain");
    assert_eq!(ids, vec![1, 2, 3, 4]);
    assert_eq!(
        server.paths(),
        ["/items", "/items?page=2", "/next/page", "/next/page?page=4"]
    );
}

#[test]
fn next_relation_tokens_are_case_insensitive_across_complete_pulls() {
    let _guard = network_test_guard();
    let server = TestServer::spawn(vec![
        ok(&[("Link", "</two>; rel=\"Next\"")], 1),
        ok(&[("Link", "</three>; rel=\"NEXT\"")], 2),
        ok(&[("Link", "</four>; rel=\"alternate nExT\"")], 3),
        ok(&[], 4),
    ]);
    let mut reader = reader(&format!("{}/one", server.url), 10);

    assert_eq!(
        drain_ids(reader.as_mut()).expect("case-insensitive continuation chain"),
        vec![1, 2, 3, 4]
    );
    assert_eq!(server.paths(), ["/one", "/two", "/three", "/four"]);
}

#[test]
fn unrelated_pagination_strategies_ignore_link_metadata() {
    let _guard = network_test_guard();
    let cases = [
        ("", "none"),
        (
            "        pagination:\n          strategy: offset\n          limit: 2",
            "offset",
        ),
        (
            "        pagination:\n          strategy: cursor_token\n          cursor_param: cursor\n          next_token_pointer: /next",
            "cursor_token",
        ),
    ];
    let link_headers = [
        vec![("Link", "</next; rel=next")],
        vec![
            ("Link", "</next-a>; rel=next"),
            ("Link", "</next-b>; rel=next"),
        ],
        vec![("Link", "<http://example.invalid/next>; rel=next")],
    ];

    for (pagination, strategy) in cases {
        for headers in &link_headers {
            let server = TestServer::spawn(vec![ok(headers, 1)]);
            let mut reader = reader_with_pagination(&server.url, 1, 0, pagination);
            assert_eq!(
                drain_ids(reader.as_mut()).unwrap_or_else(|error| panic!(
                    "{strategy} rejected an unrelated Link: {error}"
                )),
                vec![1],
                "strategy={strategy} headers={headers:?}"
            );
            let expected_path = if strategy == "offset" {
                "/?offset=0&limit=2"
            } else {
                "/"
            };
            assert_eq!(server.paths(), [expected_path]);
        }
    }
}

#[test]
fn transient_body_timeout_retries_the_whole_page() {
    let _guard = network_test_guard();
    let truncated = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 64\r\nConnection: keep-alive\r\n\r\n[".to_owned();
    let server = TestServer::spawn(vec![truncated, ok(&[], 9)]);
    let mut reader = reader_with_retries(&format!("{}/start", server.url), 1, 1);

    assert_eq!(
        drain_ids(reader.as_mut()).expect("transient body timeout should retry"),
        vec![9]
    );
    assert_eq!(server.paths(), ["/start", "/start"]);
}

#[test]
fn exhausted_transient_request_keeps_retryable_classification() {
    let _guard = network_test_guard();
    let server = TestServer::spawn(vec![response(
        503,
        "Service Unavailable",
        &[],
        r#"{"error":"temporary"}"#,
    )]);
    let mut source = reader_with_retries(&format!("{}/start", server.url), 1, 0);

    let error = source
        .next_record()
        .expect_err("exhausted transient response must fail the source");
    assert_eq!(
        error.classification_code(),
        Some("infrastructure.runtime.source_unavailable")
    );
}

#[test]
fn malformed_link_metadata_fails_closed() {
    let _guard = network_test_guard();
    let server = TestServer::spawn(vec![ok(&[("Link", "</next; rel=next")], 1)]);
    let mut reader = reader(&format!("{}/start", server.url), 10);
    let error = drain_ids(reader.as_mut()).expect_err("malformed Link must fail");
    assert_classification(
        &error,
        "rest.protocol.malformed_continuation",
        "source_protocol",
        "policy_required",
    );
}

#[test]
fn multiple_next_targets_fail_as_conflicting() {
    let _guard = network_test_guard();
    let server = TestServer::spawn(vec![ok(
        &[
            ("Link", "</next-a>; rel=next"),
            ("Link", "</next-b>; rel=\"next alternate\""),
        ],
        1,
    )]);
    let mut reader = reader(&format!("{}/start", server.url), 10);
    let error = drain_ids(reader.as_mut()).expect_err("ambiguous next links must fail");
    assert_classification(
        &error,
        "rest.protocol.conflicting_continuation",
        "source_protocol",
        "policy_required",
    );
}

#[test]
fn cross_origin_link_is_rejected_before_foreign_connect() {
    let _guard = network_test_guard();
    let foreign = TcpListener::bind("127.0.0.1:0").expect("bind foreign listener");
    foreign.set_nonblocking(true).expect("foreign nonblocking");
    let foreign_url = format!("http://{}", foreign.local_addr().expect("foreign address"));
    let server = TestServer::spawn(vec![ok(
        &[("Link", &format!("<{foreign_url}/next>; rel=next"))],
        1,
    )]);
    let mut reader = reader(&format!("{}/start", server.url), 10);
    let error = drain_ids(reader.as_mut()).expect_err("foreign link must fail");
    assert_classification(
        &error,
        "rest.security.cross_origin",
        "security_policy",
        "do_not_retry",
    );
    assert!(
        matches!(foreign.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
        "foreign origin must not receive a connection"
    );
}

#[test]
fn same_origin_get_redirect_is_followed_manually() {
    let _guard = network_test_guard();
    let server = TestServer::spawn(vec![
        response(302, "Found", &[("Location", "/final")], ""),
        ok(&[], 7),
    ]);
    let mut reader = reader(&format!("{}/start", server.url), 10);
    assert_eq!(
        drain_ids(reader.as_mut()).expect("same-origin redirect"),
        vec![7]
    );
    assert_eq!(server.paths(), ["/start", "/final"]);
}

#[test]
fn cross_origin_redirect_is_rejected_before_foreign_connect() {
    let _guard = network_test_guard();
    let foreign = TcpListener::bind("127.0.0.1:0").expect("bind foreign listener");
    foreign.set_nonblocking(true).expect("foreign nonblocking");
    let foreign_url = format!("http://{}", foreign.local_addr().expect("foreign address"));
    let server = TestServer::spawn(vec![response(
        302,
        "Found",
        &[("Location", &format!("{foreign_url}/final"))],
        "",
    )]);
    let mut reader = reader(&format!("{}/start", server.url), 10);
    let error = drain_ids(reader.as_mut()).expect_err("foreign redirect must fail");
    assert_classification(
        &error,
        "rest.security.cross_origin",
        "security_policy",
        "do_not_retry",
    );
    assert!(
        matches!(foreign.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
        "foreign origin must not receive a connection"
    );
}

#[test]
fn continuation_cycle_fails_before_repeating_request() {
    let _guard = network_test_guard();
    let server = TestServer::spawn(vec![ok(&[("Link", "</start>; rel=next")], 1)]);
    let mut reader = reader(&format!("{}/start", server.url), 10);
    let error = drain_ids(reader.as_mut()).expect_err("cycle must fail");
    assert_classification(
        &error,
        "rest.protocol.unsupported_continuation",
        "source_protocol",
        "policy_required",
    );
    assert_eq!(server.paths(), ["/start"]);
}

#[test]
fn offered_continuation_beyond_page_bound_fails_instead_of_truncating() {
    let _guard = network_test_guard();
    let server = TestServer::spawn(vec![ok(&[("Link", "</next>; rel=next")], 1)]);
    let mut reader = reader(&format!("{}/start", server.url), 1);
    assert!(
        reader.next_record().expect("first page").is_some(),
        "first admitted page must produce its record"
    );
    let error = reader
        .next_record()
        .expect_err("page-bound exhaustion must fail");
    assert_eq!(
        error.classification_code(),
        Some("rest.protocol.page_limit_reached")
    );
    assert_classification(
        &error.to_string(),
        "rest.protocol.page_limit_reached",
        "source_protocol",
        "policy_required",
    );
    assert_eq!(server.paths(), ["/start"]);
}

#[test]
fn redirect_cycle_fails_with_bounded_requests() {
    let _guard = network_test_guard();
    let server = TestServer::spawn(vec![response(302, "Found", &[("Location", "/start")], "")]);
    let mut reader = reader(&format!("{}/start", server.url), 10);
    let started = Instant::now();
    let error = drain_ids(reader.as_mut()).expect_err("redirect cycle must fail");
    assert_classification(
        &error,
        "rest.protocol.unsupported_continuation",
        "source_protocol",
        "policy_required",
    );
    assert_eq!(server.paths(), ["/start"]);
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn redirect_limit_fails_before_an_unbounded_request_chain() {
    let _guard = network_test_guard();
    let responses = (0..11)
        .map(|index| {
            response(
                302,
                "Found",
                &[("Location", &format!("/hop{}", index + 1))],
                "",
            )
        })
        .collect();
    let server = TestServer::spawn(responses);
    let mut reader = reader(&format!("{}/hop0", server.url), 10);
    let error = drain_ids(reader.as_mut()).expect_err("redirect limit must fail");
    assert_classification(
        &error,
        "rest.protocol.unsupported_continuation",
        "source_protocol",
        "policy_required",
    );
    assert_eq!(
        server.paths().len(),
        11,
        "one initial request plus ten admitted redirects is the hard request bound"
    );
}
