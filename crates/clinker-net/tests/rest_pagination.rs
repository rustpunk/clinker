//! REST source integration test against a synthetic paginated HTTP
//! server.
//!
//! A `std::net::TcpListener` on a background `std::thread` serves a small
//! fixed dataset under two pagination strategies (offset/limit and RFC
//! 5988 Link header) and confirms the reader pages to last-page EOF,
//! yielding exactly the dataset with no duplication or truncation.

#![cfg(feature = "transport")]

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use clinker_exec::pipeline::shutdown::ShutdownToken;
use clinker_exec::source::RecordSource;
use clinker_net::build_rest_source;
use clinker_plan::config::{RestSourceConfig, SourceTransport, parse_config};

/// Total rows the fixture serves. Paged so the reader must follow ≥2
/// pages and detect the last one.
const TOTAL_ROWS: usize = 7;
const PAGE_SIZE: usize = 3;

/// A parsed request line: method + path (with query string).
struct Req {
    path: String,
}

fn read_request(stream: &mut TcpStream) -> Option<Req> {
    let mut reader = BufReader::new(stream.try_clone().ok()?);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).ok()? == 0 {
        return None;
    }
    let path = request_line.split_whitespace().nth(1)?.to_string();
    // Drain headers up to the blank line so the socket is at the body.
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line).ok()?;
        if n == 0 || line == "\r\n" || line == "\n" {
            break;
        }
    }
    Some(Req { path })
}

fn query_param(path: &str, key: &str) -> Option<usize> {
    let q = path.split_once('?')?.1;
    q.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        if k == key { v.parse().ok() } else { None }
    })
}

/// Build the JSON body for a page starting at `offset` of `PAGE_SIZE`
/// rows, as a top-level array `[{"id":N,"amount":M}, ...]`.
fn page_body(offset: usize) -> String {
    let end = (offset + PAGE_SIZE).min(TOTAL_ROWS);
    let rows: Vec<String> = (offset..end)
        .map(|i| format!(r#"{{"id":{},"amount":{}}}"#, i, i * 10))
        .collect();
    format!("[{}]", rows.join(","))
}

fn write_response(stream: &mut TcpStream, extra_headers: &str, body: &str) {
    // `Connection: close` forces ureq to open a fresh TCP connection per
    // page, so the single-request-per-accept server loop below serves
    // every page correctly without keep-alive multiplexing.
    let resp = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n{}",
        body.len(),
        extra_headers,
        body
    );
    let _ = stream.write_all(resp.as_bytes());
    let _ = stream.flush();
}

/// Spawn an offset/limit paginated server. Returns the bound base URL and
/// a stop flag (set true after the test to release the accept loop).
fn spawn_offset_server() -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let stop = Arc::new(AtomicBool::new(false));
    let stop_thread = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        for incoming in listener.incoming() {
            if stop_thread.load(Ordering::SeqCst) {
                break;
            }
            let Ok(mut stream) = incoming else { continue };
            let Some(req) = read_request(&mut stream) else {
                continue;
            };
            let offset = query_param(&req.path, "offset").unwrap_or(0);
            write_response(&mut stream, "", &page_body(offset));
            if stop_thread.load(Ordering::SeqCst) {
                break;
            }
        }
    });
    (format!("http://{addr}/rows"), stop, handle)
}

/// Spawn a Link-header paginated server. Each page emits a `Link:
/// <next>; rel="next"` header until the last page, which omits it.
fn spawn_link_server() -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let stop = Arc::new(AtomicBool::new(false));
    let stop_thread = Arc::clone(&stop);
    let base = format!("http://{addr}/rows");
    let base_thread = base.clone();
    let handle = thread::spawn(move || {
        for incoming in listener.incoming() {
            if stop_thread.load(Ordering::SeqCst) {
                break;
            }
            let Ok(mut stream) = incoming else { continue };
            let Some(req) = read_request(&mut stream) else {
                continue;
            };
            let offset = query_param(&req.path, "offset").unwrap_or(0);
            let next_offset = offset + PAGE_SIZE;
            let extra = if next_offset < TOTAL_ROWS {
                format!("Link: <{base_thread}?offset={next_offset}>; rel=\"next\"\r\n")
            } else {
                String::new()
            };
            write_response(&mut stream, &extra, &page_body(offset));
            if stop_thread.load(Ordering::SeqCst) {
                break;
            }
        }
    });
    (base, stop, handle)
}

/// Build a REST reader from a parsed `rest` pipeline through the same
/// public entry point the CLI uses, so the test exercises the real YAML
/// deserialization of the transport surface end to end.
fn build_reader(pagination_yaml: &str, max_pages: u32, url: &str) -> Box<dyn RecordSource> {
    let yaml = format!(
        r#"
pipeline:
  name: rest_test
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
        retries: 2
        timeout_secs: 10
{pagination_yaml}
      schema:
        - {{ name: id, type: int }}
        - {{ name: amount, type: int }}
  - type: sink
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
    // Override the URL with the live server's bound address (the YAML
    // carries a placeholder the bound port replaces).
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

fn drain(reader: &mut dyn RecordSource) -> Vec<i64> {
    let mut ids = Vec::new();
    while let Some(rec) = reader.next_record().expect("next_record") {
        if let Some(clinker_record::Value::Integer(n)) = rec.get("id") {
            ids.push(*n);
        }
    }
    ids
}

/// Wake the accept loop so the server thread observes its `stop` flag and
/// exits, then join it.
fn shutdown_server(url: &str, stop: &Arc<AtomicBool>, handle: thread::JoinHandle<()>) {
    stop.store(true, Ordering::SeqCst);
    let host = url
        .split_once("/rows")
        .unwrap()
        .0
        .trim_start_matches("http://");
    let _ = std::net::TcpStream::connect(host);
    let _ = handle.join();
}

#[test]
fn offset_pagination_drains_to_last_page_eof() {
    let (url, stop, handle) = spawn_offset_server();
    let pagination = "        pagination:\n          strategy: offset\n          limit: 3";
    let mut reader = build_reader(pagination, 100, &url);

    let ids = drain(reader.as_mut());
    shutdown_server(&url, &stop, handle);

    assert_eq!(
        ids,
        (0..TOTAL_ROWS as i64).collect::<Vec<_>>(),
        "offset pagination must yield every row exactly once, in order"
    );
}

#[test]
fn link_header_pagination_drains_to_last_page_eof() {
    let (url, stop, handle) = spawn_link_server();
    let pagination = "        pagination:\n          strategy: link_header";
    let mut reader = build_reader(pagination, 100, &url);

    let ids = drain(reader.as_mut());
    shutdown_server(&url, &stop, handle);

    assert_eq!(
        ids,
        (0..TOTAL_ROWS as i64).collect::<Vec<_>>(),
        "Link-header pagination must yield every row exactly once, in order"
    );
}

#[test]
fn max_pages_cap_fails_if_server_offers_more() {
    // A configured page cap is a traversal bound, not permission to publish a
    // truncated prefix as a completed pull. If the first page still advertises
    // continuation, the reader yields that admitted page and then fails closed.
    let (url, stop, handle) = spawn_link_server();
    let pagination = "        pagination:\n          strategy: link_header";
    let mut reader = build_reader(pagination, 1, &url);

    let mut ids = Vec::new();
    for _ in 0..PAGE_SIZE {
        let record = reader
            .next_record()
            .expect("first admitted page")
            .expect("record from first page");
        let Some(clinker_record::Value::Integer(id)) = record.get("id") else {
            panic!("record id must be an integer")
        };
        ids.push(*id);
    }
    let error = reader
        .next_record()
        .expect_err("offered continuation beyond max_pages must fail");
    shutdown_server(&url, &stop, handle);

    assert_eq!(ids, vec![0, 1, 2]);
    assert!(
        error
            .to_string()
            .contains("rest.protocol.page_limit_reached"),
        "page-bound failure must carry the stable classification: {error}"
    );
}

/// Build the JSON body for a page as a *wrapped object*
/// `{"data":[{…}],"total":N}` on one line — the common paginated API
/// shape. The records live under the `data` key, so the reader must
/// honor `record_path: data` to find them.
fn wrapped_page_body(offset: usize) -> String {
    let end = (offset + PAGE_SIZE).min(TOTAL_ROWS);
    let rows: Vec<String> = (offset..end)
        .map(|i| format!(r#"{{"id":{},"amount":{}}}"#, i, i * 10))
        .collect();
    format!(r#"{{"data":[{}],"total":{}}}"#, rows.join(","), TOTAL_ROWS)
}

/// Spawn an offset server whose bodies are wrapped objects rather than
/// top-level arrays.
fn spawn_wrapped_offset_server() -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let stop = Arc::new(AtomicBool::new(false));
    let stop_thread = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        for incoming in listener.incoming() {
            if stop_thread.load(Ordering::SeqCst) {
                break;
            }
            let Ok(mut stream) = incoming else { continue };
            let Some(req) = read_request(&mut stream) else {
                continue;
            };
            let offset = query_param(&req.path, "offset").unwrap_or(0);
            write_response(&mut stream, "", &wrapped_page_body(offset));
            if stop_thread.load(Ordering::SeqCst) {
                break;
            }
        }
    });
    (format!("http://{addr}/rows"), stop, handle)
}

/// Build a REST reader whose source decodes a `{"data":[…]}` wrapped body
/// (`record_path: data`) under the offset strategy.
fn build_wrapped_offset_reader(url: &str) -> Box<dyn RecordSource> {
    let yaml = format!(
        r#"
pipeline:
  name: rest_wrapped
nodes:
  - type: source
    name: api
    config:
      name: api
      type: json
      options:
        record_path: data
      transport:
        kind: rest
        url: {url}
        max_pages: 100
        pagination:
          strategy: offset
          limit: 3
      schema:
        - {{ name: id, type: int }}
        - {{ name: amount, type: int }}
  - type: sink
    name: out
    input: api
    config:
      name: out
      type: csv
      path: out.csv
"#
    );
    let config = parse_config(&yaml).expect("parse wrapped rest pipeline");
    let body = config.source_bodies().next().expect("source body");
    let SourceTransport::Rest(cfg) = body.source.transport.clone() else {
        panic!("expected rest transport")
    };
    build_rest_source(
        cfg,
        &body.source,
        body.schema.as_columns().expect("single-record schema"),
        body.on_unmapped.clone(),
    )
    .expect("build wrapped rest reader")
}

#[test]
fn offset_pagination_over_wrapped_object_body_drains_all_pages() {
    // Regression: the short-page detector must count the rows the emit
    // path actually yields (honoring `record_path: data`), not re-decode
    // the body with a default config that would see the single wrapper
    // object as one record and stop after page 1 — silently truncating
    // every later page.
    let (url, stop, handle) = spawn_wrapped_offset_server();
    let mut reader = build_wrapped_offset_reader(&url);

    let ids = drain(reader.as_mut());
    shutdown_server(&url, &stop, handle);

    assert_eq!(
        ids,
        (0..TOTAL_ROWS as i64).collect::<Vec<_>>(),
        "offset pagination over a wrapped-object body must yield every row, not truncate to page 1"
    );
}

/// Spawn a cursor-token server: each page carries the records plus a
/// `{"next": "<token>"}` meta field naming the next page, omitted on the
/// last page. The token round-trips back as the `cursor` query param.
fn spawn_cursor_token_server() -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let stop = Arc::new(AtomicBool::new(false));
    let stop_thread = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        for incoming in listener.incoming() {
            if stop_thread.load(Ordering::SeqCst) {
                break;
            }
            let Ok(mut stream) = incoming else { continue };
            let Some(req) = read_request(&mut stream) else {
                continue;
            };
            // The continuation token IS the next offset, encoded as a
            // string. Absent on the first request.
            let offset = token_param(&req.path, "cursor").unwrap_or(0);
            let end = (offset + PAGE_SIZE).min(TOTAL_ROWS);
            let rows: Vec<String> = (offset..end)
                .map(|i| format!(r#"{{"id":{},"amount":{}}}"#, i, i * 10))
                .collect();
            let next_offset = offset + PAGE_SIZE;
            let meta = if next_offset < TOTAL_ROWS {
                format!(r#""next":"{next_offset}""#)
            } else {
                r#""next":null"#.to_string()
            };
            let body = format!(r#"{{"meta":{{{}}},"rows":[{}]}}"#, meta, rows.join(","));
            write_response(&mut stream, "", &body);
            if stop_thread.load(Ordering::SeqCst) {
                break;
            }
        }
    });
    (format!("http://{addr}/rows"), stop, handle)
}

/// Read a query parameter as a string and parse it as a `usize` (the
/// cursor-token server encodes the next offset as a string token).
fn token_param(path: &str, key: &str) -> Option<usize> {
    let q = path.split_once('?')?.1;
    q.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        if k == key { v.parse().ok() } else { None }
    })
}

#[test]
fn cursor_token_pagination_round_trips_to_last_page_eof() {
    // Exercises the full cursor_token advance loop: read the next token
    // from a JSON pointer in each body, send it back as a query param,
    // stop when the token is null.
    let (url, stop, handle) = spawn_cursor_token_server();
    let yaml = format!(
        r#"
pipeline:
  name: rest_cursor
nodes:
  - type: source
    name: api
    config:
      name: api
      type: json
      options:
        record_path: rows
      transport:
        kind: rest
        url: {url}
        max_pages: 100
        pagination:
          strategy: cursor_token
          cursor_param: cursor
          next_token_pointer: /meta/next
      schema:
        - {{ name: id, type: int }}
        - {{ name: amount, type: int }}
  - type: sink
    name: out
    input: api
    config:
      name: out
      type: csv
      path: out.csv
"#
    );
    let config = parse_config(&yaml).expect("parse cursor_token pipeline");
    let body = config.source_bodies().next().expect("source body");
    let SourceTransport::Rest(cfg) = body.source.transport.clone() else {
        panic!("expected rest transport")
    };
    let mut reader = build_rest_source(
        cfg,
        &body.source,
        body.schema.as_columns().expect("single-record schema"),
        body.on_unmapped.clone(),
    )
    .expect("build cursor_token reader");

    let ids = drain(reader.as_mut());
    shutdown_server(&url, &stop, handle);

    assert_eq!(
        ids,
        (0..TOTAL_ROWS as i64).collect::<Vec<_>>(),
        "cursor_token pagination must follow the token chain to the last page"
    );
}

#[test]
fn shutdown_request_stops_the_reader_at_the_next_page_boundary() {
    // The reader polls its cancellation handle at each page boundary. With
    // the token tripped after the first page drains, it must stop with a
    // clean end-of-input (`Ok(None)`) instead of fetching further pages —
    // the same graceful drain a file source performs on SIGINT.
    let (url, stop, handle) = spawn_offset_server();
    let pagination = "        pagination:\n          strategy: offset\n          limit: 3";
    let mut reader = build_reader(pagination, 100, &url);

    let token = ShutdownToken::detached();
    reader.set_shutdown_token(token.clone());

    // Drain the whole first page (PAGE_SIZE rows), then request shutdown.
    let mut ids = Vec::new();
    for _ in 0..PAGE_SIZE {
        match reader.next_record().expect("next_record") {
            Some(rec) => {
                if let Some(clinker_record::Value::Integer(n)) = rec.get("id") {
                    ids.push(*n);
                }
            }
            None => break,
        }
    }
    token.request();

    // The next pull crosses a page boundary, observes the request, and
    // stops cleanly without fetching another page.
    assert!(
        reader
            .next_record()
            .expect("clean stop after shutdown")
            .is_none(),
        "reader must stop at the page boundary after shutdown is requested"
    );

    shutdown_server(&url, &stop, handle);

    assert_eq!(
        ids,
        vec![0, 1, 2],
        "only the first page's rows are emitted before the interrupt"
    );
}

/// A cancelled run whose in-flight page dies mid-body is a cancellation, not
/// an outage. The peer answered, so the "did the peer answer?" rule does not
/// apply; what applies is that the loop was going to retry the body read, and
/// the signal took that retry away. Reporting it as a temporarily unavailable
/// source told a supervisor to back off and try again, and it re-queued a
/// batch an operator had just stopped.
#[test]
fn shutdown_during_a_body_read_surfaces_as_typed_interruption() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind truncating server");
    let address = listener.local_addr().expect("truncating server address");
    let token = ShutdownToken::detached();
    let server_token = token.clone();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept first request");
        read_request(&mut stream).expect("read first request");
        // A complete response head promising more body than will arrive, so
        // the peer has answered and the body read is what fails. The signal is
        // requested before the socket closes, exactly as a cancellation
        // arriving while a page is in flight.
        let head = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 512\r\nConnection: close\r\n\r\n";
        stream.write_all(head).expect("write response head");
        stream
            .write_all(b"[{\"id\":0,")
            .expect("write partial body");
        stream.flush().expect("flush partial body");
        server_token.request();
        drop(stream);
    });
    let url = format!("http://{address}/rows");
    let mut reader = build_reader("", 1, &url);
    reader.set_shutdown_token(token);

    let error = reader
        .next_record()
        .expect_err("a truncated body under a pending shutdown must interrupt");
    handle.join().expect("join truncating server");

    assert!(
        matches!(error, clinker_format::FormatError::Interrupted),
        "a cancelled run must not report its own cancellation as a retryable \
         source failure: got {error:?}"
    );
}

#[test]
fn shutdown_between_retries_surfaces_as_typed_interruption() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind retry server");
    let address = listener.local_addr().expect("retry server address");
    let token = ShutdownToken::detached();
    let server_token = token.clone();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept first request");
        read_request(&mut stream).expect("read first request");
        server_token.request();
        let response =
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        stream.write_all(response).expect("write retry response");
        stream.flush().expect("flush retry response");
    });
    let url = format!("http://{address}/rows");
    let mut reader = build_reader("", 1, &url);
    reader.set_shutdown_token(token);

    let error = reader
        .next_record()
        .expect_err("shutdown between retries must interrupt");
    handle.join().expect("join retry server");

    assert!(matches!(error, clinker_format::FormatError::Interrupted));
}

/// Serve three pages whose `Link` targets carry dot segments, one of them past
/// an empty segment. `/rows` names `/rows//items/../p`, which resolves to
/// `/rows//p`; that page names `/rows/items/../p`, which resolves to `/rows/p`.
///
/// Resolving those by discarding empty segments sent both requests to
/// `/rows/p` — a resource the first of them does not name — and gave the two
/// pages one identity, so the pull ended as a continuation cycle after the
/// second.
fn spawn_dot_segment_link_server() -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let stop = Arc::new(AtomicBool::new(false));
    let stop_thread = Arc::clone(&stop);
    let base = format!("http://{addr}/rows");
    let base_thread = base.clone();
    let handle = thread::spawn(move || {
        for incoming in listener.incoming() {
            if stop_thread.load(Ordering::SeqCst) {
                break;
            }
            let Ok(mut stream) = incoming else { continue };
            let Some(req) = read_request(&mut stream) else {
                continue;
            };
            let (offset, next) = match req.path.as_str() {
                "/rows" => (0, format!("{base_thread}//items/../p")),
                "/rows//p" => (PAGE_SIZE, format!("{base_thread}/items/../p")),
                "/rows/p" => (PAGE_SIZE * 2, String::new()),
                // Any other path is a request this reader should never have
                // built. Answering it with no rows and no continuation makes
                // that visible as missing records rather than as a hang.
                _ => (TOTAL_ROWS, String::new()),
            };
            let extra = if next.is_empty() {
                String::new()
            } else {
                format!("Link: <{next}>; rel=\"next\"\r\n")
            };
            write_response(&mut stream, &extra, &page_body(offset));
            if stop_thread.load(Ordering::SeqCst) {
                break;
            }
        }
    });
    (base, stop, handle)
}

#[test]
fn dot_segments_in_a_link_target_resolve_to_the_page_the_reply_named() {
    let (url, stop, handle) = spawn_dot_segment_link_server();
    let pagination = "        pagination:\n          strategy: link_header";
    let mut reader = build_reader(pagination, 100, &url);

    let drained = {
        let mut ids = Vec::new();
        loop {
            match reader.next_record() {
                Ok(Some(record)) => {
                    if let Some(clinker_record::Value::Integer(id)) = record.get("id") {
                        ids.push(*id);
                    }
                }
                Ok(None) => break Ok(ids),
                Err(error) => break Err(error),
            }
        }
    };
    shutdown_server(&url, &stop, handle);

    let ids = drained.expect("two distinct pages are not a continuation cycle");
    assert_eq!(
        ids,
        (0..TOTAL_ROWS as i64).collect::<Vec<_>>(),
        "each Link target must be fetched as the resource it names, exactly once"
    );
}
