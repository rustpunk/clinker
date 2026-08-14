//! What the REST source re-requests, and what it declines to.
//!
//! The retry budget is a ceiling on retries worth making. A failure that will
//! arrive identically on every attempt is not one of them, and spending the
//! budget on it costs the per-request deadline once per attempt before naming
//! a condition that was already settled at the first.

#![cfg(feature = "transport")]

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use clinker_exec::source::RecordSource;
use clinker_net::build_rest_source;
use clinker_plan::config::{RestSourceConfig, SourceTransport, parse_config};

/// An address nothing is listening on. Bound to learn a free port, then
/// released, so every attempt against it is refused at connect without a
/// fixture having to stay alive to refuse them.
fn closed_address() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().expect("addr");
    drop(listener);
    address
}

/// A server that answers every request with the same reply and counts how many
/// it was asked for. It accepts until the test releases it rather than for a
/// fixed number of requests, so a test can assert that a second request never
/// arrived without the fixture deciding that in advance.
struct CountingServer {
    address: SocketAddr,
    requests: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl CountingServer {
    /// `reply` is written verbatim after each request's headers are read.
    fn spawn(reply: &'static [u8]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind counting fixture");
        let address = listener.local_addr().expect("counting fixture address");
        listener
            .set_nonblocking(true)
            .expect("counting fixture is polled, not blocked on");
        let requests = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let worker = thread::spawn({
            let requests = Arc::clone(&requests);
            let stop = Arc::clone(&stop);
            move || {
                while !stop.load(Ordering::SeqCst) {
                    let Ok((stream, _)) = listener.accept() else {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    };
                    stream
                        .set_nonblocking(false)
                        .expect("serve one request at a time");
                    requests.fetch_add(1, Ordering::SeqCst);
                    let mut reader = BufReader::new(&stream);
                    loop {
                        let mut line = String::new();
                        match reader.read_line(&mut line) {
                            Ok(0) => break,
                            Ok(_) if line == "\r\n" || line == "\n" => break,
                            Ok(_) => {}
                            Err(_) => break,
                        }
                    }
                    let mut stream = &stream;
                    let _ = stream.write_all(reply);
                    let _ = stream.flush();
                }
            }
        });
        Self {
            address,
            requests,
            stop,
            worker: Some(worker),
        }
    }

    fn url(&self) -> String {
        format!("http://{}/rows", self.address)
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

impl Drop for CountingServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn build_reader(url: &str, retries: u32, auth: &str) -> Box<dyn RecordSource> {
    let yaml = format!(
        r#"
pipeline:
  name: rest_retry_test
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
        max_pages: 1
        retries: {retries}
        timeout_secs: 10
{auth}
      schema:
        - {{ name: id, type: int }}
  - type: output
    name: out
    input: api
    config:
      name: out
      type: csv
      path: out.csv
"#
    );
    let config = parse_config(&yaml).expect("parse rest pipeline");
    let body = config.source_bodies().next().expect("source body");
    let SourceTransport::Rest(cfg) = body.source.transport.clone() else {
        panic!("expected rest transport")
    };
    let cfg = RestSourceConfig {
        url: url.to_string(),
        ..cfg
    };
    build_rest_source(
        cfg,
        &body.source,
        body.schema.as_columns().expect("single-record schema"),
        body.on_unmapped.clone(),
    )
    .expect("build rest reader")
}

/// A request this client cannot build is settled at the first attempt.
///
/// The page loop used to gate a retry on the attempt count alone, consulting
/// the crate's transience rule only once a body read had failed. Everything
/// the rule calls permanent was therefore re-attempted `retries` times over —
/// a name that does not resolve, a certificate this client will not trust, a
/// URL it cannot route — so a typo in a source's host cost a source configured
/// `retries: 5, timeout_secs: 30` minutes of DNS and connect cycles to report
/// a condition already known at the first, and named the last attempt as
/// though something had varied between them.
///
/// The unroutable request here is an auth header whose name is not a header
/// name. It reaches the same rule as the misrouted URL and the untrusted
/// certificate, and unlike either it needs no network to be reproducible.
#[test]
fn a_request_that_cannot_be_built_is_attempted_once() {
    let auth = "        auth:\n          scheme: header\n          name: \"Bad Name\"\n          value: \"k\"";
    let mut reader = build_reader(&format!("http://{}/rows", closed_address()), 4, auth);

    let error = reader
        .next_record()
        .expect_err("a header name that is not one cannot be sent");
    let rendered = error.to_string();

    assert!(
        rendered.contains("class=protocol"),
        "the failure is the request this client could not route: {rendered}"
    );
    assert!(
        rendered.contains("attempt=1"),
        "and it is reported against the one attempt that reached it: {rendered}"
    );
}

/// The gate reads the failure's own verdict rather than a shorter budget, so a
/// transient failure still gets every attempt it was configured for. Without
/// this, narrowing the budget would pass the test above just as well.
#[test]
fn a_transient_failure_still_gets_the_whole_budget() {
    let mut reader = build_reader(&format!("http://{}/rows", closed_address()), 2, "");

    let error = reader
        .next_record()
        .expect_err("a refused connection has no page to yield");
    let rendered = error.to_string();

    assert!(
        rendered.contains("attempt=3"),
        "a connect failure spends the configured retries: {rendered}"
    );
    assert!(
        rendered.contains("infrastructure.runtime.source_unavailable"),
        "and is still reported as a source that may come back: {rendered}"
    );
}

/// A reply that arrived and would not be read is settled at the first attempt.
///
/// Body-read failures were answered by the request-phase rule, which admits
/// every failure it cannot name — a framing that does not hold, a stream that
/// will not decode. Those are facts about the bytes that arrived, so the same
/// page was fetched `retries` times over, each paying the full per-request
/// deadline to report the same thing at a higher attempt number.
#[test]
fn a_body_that_will_not_decode_is_read_once() {
    let server = CountingServer::spawn(
        b"HTTP/1.1 200 OK\r\n\
          Content-Type: application/json\r\n\
          Transfer-Encoding: chunked\r\n\
          \r\n\
          not-a-chunk-size\r\n",
    );
    let mut reader = build_reader(&server.url(), 3, "");

    let error = reader
        .next_record()
        .expect_err("a body this client cannot frame yields no page");
    let rendered = error.to_string();

    assert_eq!(
        server.requests(),
        1,
        "the page is not fetched again to be misread again: {rendered}"
    );
    assert!(
        rendered.contains("class=protocol"),
        "the reply began arriving and then would not frame: {rendered}"
    );
    assert!(
        rendered.contains("attempt=1"),
        "and the one attempt that reached it is the one reported: {rendered}"
    );
}

/// The narrowing is about what the reply established, not about the phase
/// having a shorter list. A connection that came apart under the reader is
/// still the ordinary transient the retry budget exists for.
#[test]
fn a_body_cut_off_mid_read_still_gets_the_whole_budget() {
    let server = CountingServer::spawn(
        b"HTTP/1.1 200 OK\r\n\
          Content-Type: application/json\r\n\
          Content-Length: 64\r\n\
          \r\n\
          [{\"id\": 1}",
    );
    let mut reader = build_reader(&server.url(), 2, "");

    let error = reader
        .next_record()
        .expect_err("a truncated body has no complete page to yield");
    let rendered = error.to_string();

    assert_eq!(
        server.requests(),
        3,
        "a dropped stream spends the configured retries: {rendered}"
    );
    assert!(
        rendered.contains("class=response_io_unexpectedeof"),
        "the stream ended under the reader: {rendered}"
    );
    assert!(
        rendered.contains("attempt=3"),
        "and is reported against the last of them: {rendered}"
    );
}
