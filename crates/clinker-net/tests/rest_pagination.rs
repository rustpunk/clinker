//! REST source integration test against a synthetic paginated HTTP
//! server.
//!
//! A `std::net::TcpListener` on a background `std::thread` serves a small
//! fixed dataset under two pagination strategies (offset/limit and RFC
//! 5988 Link header) and confirms the reader pages to last-page EOF,
//! yielding exactly the dataset with no duplication or truncation.

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
    // Override the URL with the live server's bound address (the YAML
    // carries a placeholder the bound port replaces).
    let cfg = RestSourceConfig {
        url: url.to_string(),
        ..cfg
    };
    build_rest_source(
        cfg,
        &body.source,
        &body.schema.columns,
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
fn max_pages_cap_halts_even_if_server_offers_more() {
    // The Link server keeps offering a next link until the dataset ends,
    // but a max_pages=1 cap must stop after the first page regardless.
    let (url, stop, handle) = spawn_link_server();
    let pagination = "        pagination:\n          strategy: link_header";
    let mut reader = build_reader(pagination, 1, &url);

    let ids = drain(reader.as_mut());
    shutdown_server(&url, &stop, handle);

    assert_eq!(
        ids,
        vec![0, 1, 2],
        "max_pages cap must stop the reader at the first page"
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
  - type: output
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
        &body.schema.columns,
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
  - type: output
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
        &body.schema.columns,
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
