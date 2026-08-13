//! What the REST source re-requests, and what it declines to.
//!
//! The retry budget is a ceiling on retries worth making. A failure that will
//! arrive identically on every attempt is not one of them, and spending the
//! budget on it costs the per-request deadline once per attempt before naming
//! a condition that was already settled at the first.

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
