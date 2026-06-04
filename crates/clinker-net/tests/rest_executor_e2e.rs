//! End-to-end: a `rest` source registered as `SourceInput::Records`
//! drives the full executor on its own ingest thread and emits into the
//! identical crossbeam channel as a file source — no special-case
//! dispatch path. Confirms record counts and `$source.*` provenance
//! resolving to the stable `<source:NAME>` synthetic id.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use clinker_bench_support::io::SharedBuffer;
use clinker_core::executor::{PipelineExecutor, PipelineRunParams, SourceInput, SourceReaders};
use clinker_net::build_rest_source;
use clinker_plan::config::{CompileContext, SourceTransport, parse_config};

const TOTAL_ROWS: usize = 5;
const PAGE_SIZE: usize = 2;

fn read_path(stream: &mut TcpStream) -> Option<String> {
    let mut reader = BufReader::new(stream.try_clone().ok()?);
    let mut line = String::new();
    if reader.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let path = line.split_whitespace().nth(1)?.to_string();
    loop {
        let mut h = String::new();
        let n = reader.read_line(&mut h).ok()?;
        if n == 0 || h == "\r\n" || h == "\n" {
            break;
        }
    }
    Some(path)
}

fn offset_of(path: &str) -> usize {
    path.split_once('?')
        .and_then(|(_, q)| {
            q.split('&').find_map(|kv| {
                let (k, v) = kv.split_once('=')?;
                (k == "offset").then(|| v.parse().ok()).flatten()
            })
        })
        .unwrap_or(0)
}

fn body(offset: usize) -> String {
    let end = (offset + PAGE_SIZE).min(TOTAL_ROWS);
    let rows: Vec<String> = (offset..end)
        .map(|i| format!(r#"{{"id":{i},"label":"row-{i}"}}"#))
        .collect();
    format!("[{}]", rows.join(","))
}

fn spawn_server() -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_t = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        for incoming in listener.incoming() {
            if stop_t.load(Ordering::SeqCst) {
                break;
            }
            let Ok(mut s) = incoming else { continue };
            let Some(path) = read_path(&mut s) else {
                continue;
            };
            let b = body(offset_of(&path));
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                b.len(),
                b
            );
            let _ = s.write_all(resp.as_bytes());
        }
    });
    (format!("http://{addr}/rows"), stop, handle)
}

#[test]
fn rest_source_drives_full_executor_into_shared_channel() {
    let (url, stop, handle) = spawn_server();

    let yaml = format!(
        r#"
pipeline:
  name: rest_e2e
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
        max_pages: 100
        pagination:
          strategy: offset
          limit: 2
      schema:
        - {{ name: id, type: int }}
        - {{ name: label, type: string }}
  - type: transform
    name: stamp
    input: api
    config:
      cxl: |
        emit id = id
        emit label = label
        emit src_file = $source.file
        emit src_name = $source.name
  - type: output
    name: out
    input: stamp
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#
    );

    let config = parse_config(&yaml).expect("parse rest e2e pipeline");
    let body = config.source_bodies().next().unwrap();
    let SourceTransport::Rest(cfg) = body.source.transport.clone() else {
        panic!("expected rest transport")
    };
    let reader = build_rest_source(
        cfg,
        &body.source,
        &body.schema.columns,
        body.on_unmapped.clone(),
    )
    .expect("build rest reader");

    let plan = config.compile(&CompileContext::default()).expect("compile");

    let readers: SourceReaders = HashMap::from([("api".to_string(), SourceInput::Records(reader))]);
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(output.clone()) as Box<dyn Write + Send>,
    )]);

    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &PipelineRunParams {
            execution_id: "rest-e2e".to_string(),
            batch_id: "rest-e2e".to_string(),
            ..Default::default()
        },
    )
    .expect("run rest e2e");

    stop.store(true, Ordering::SeqCst);
    let host = url
        .split_once("/rows")
        .unwrap()
        .0
        .trim_start_matches("http://");
    let _ = TcpStream::connect(host);
    let _ = handle.join();

    assert_eq!(report.counters.total_count, TOTAL_ROWS as u64);
    assert_eq!(report.counters.ok_count, TOTAL_ROWS as u64);

    let out = output.as_string();
    let data: Vec<&str> = out.lines().filter(|l| !l.is_empty()).collect();
    // Header + TOTAL_ROWS rows.
    assert_eq!(data.len(), TOTAL_ROWS + 1, "output:\n{out}");
    // Every body row stamps the stable synthetic id for a pathless source.
    for line in &data[1..] {
        assert!(
            line.contains("<source:api>"),
            "expected synthetic source id in {line:?}; full:\n{out}"
        );
    }
}

/// Spawn a single-page server whose body mixes well-typed rows with one
/// row whose `amount` is a non-numeric string. The string survives the
/// lenient reader coercion unchanged and only fails when a downstream
/// Transform does arithmetic on it — routing that one row to the DLQ.
fn spawn_dirty_server() -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_t = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        for incoming in listener.incoming() {
            if stop_t.load(Ordering::SeqCst) {
                break;
            }
            let Ok(mut s) = incoming else { continue };
            let Some(_path) = read_path(&mut s) else {
                continue;
            };
            let b = r#"[{"id":1,"amount":"10"},{"id":2,"amount":"bad"},{"id":3,"amount":"30"}]"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                b.len(),
                b
            );
            let _ = s.write_all(resp.as_bytes());
        }
    });
    (format!("http://{addr}/rows"), stop, handle)
}

#[test]
fn rest_per_row_decode_failure_routes_to_dlq() {
    // A REST page carrying one un-coercible value routes exactly that row
    // to the DLQ and emits the rest, matching file-source per-row
    // semantics — the pull is not aborted by a single bad row.
    let (url, stop, handle) = spawn_dirty_server();

    let yaml = format!(
        r#"
pipeline:
  name: rest_dlq
error_handling:
  strategy: continue
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
        max_pages: 10
        pagination:
          strategy: none
      schema:
        - {{ name: id, type: int }}
        - {{ name: amount, type: string }}
  - type: transform
    name: doubler
    input: api
    config:
      cxl: |
        emit id = id
        emit doubled = amount.to_int() * 2
  - type: output
    name: out
    input: doubler
    config:
      name: out
      type: csv
      path: out.csv
"#
    );

    let config = parse_config(&yaml).expect("parse rest dlq pipeline");
    let body = config.source_bodies().next().unwrap();
    let SourceTransport::Rest(cfg) = body.source.transport.clone() else {
        panic!("expected rest transport")
    };
    let reader = build_rest_source(
        cfg,
        &body.source,
        &body.schema.columns,
        body.on_unmapped.clone(),
    )
    .expect("build rest reader");

    let plan = config.compile(&CompileContext::default()).expect("compile");
    let readers: SourceReaders = HashMap::from([("api".to_string(), SourceInput::Records(reader))]);
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(output.clone()) as Box<dyn Write + Send>,
    )]);

    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &PipelineRunParams {
            execution_id: "rest-dlq".to_string(),
            batch_id: "rest-dlq".to_string(),
            ..Default::default()
        },
    )
    .expect("run rest dlq pipeline");

    stop.store(true, Ordering::SeqCst);
    let host = url
        .split_once("/rows")
        .unwrap()
        .0
        .trim_start_matches("http://");
    let _ = TcpStream::connect(host);
    let _ = handle.join();

    assert_eq!(report.counters.total_count, 3, "all three rows ingested");
    assert_eq!(report.counters.ok_count, 2, "two well-typed rows emitted");
    assert_eq!(
        report.counters.dlq_count, 1,
        "the one un-coercible row routes to the DLQ, per-row, without aborting the pull"
    );
}
