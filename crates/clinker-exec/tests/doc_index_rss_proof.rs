//! Bounded-memory end-to-end proof: a `Source -> Transform -> Output`
//! pipeline over a nested-envelope document with a multi-hundred-MB (up to
//! 1 GiB) undeclared body must finish with a peak resident-memory delta in
//! the low tens of MiB — never proportional to the body's byte size.
//!
//! Why this proves the pillar. The transform reads only a declared `$doc`
//! head path (`$doc.Head.batch_id`) and emits a single scalar. With no
//! Aggregate / sort / Combine / window, the only thing that could spike RSS
//! proportionally to the body is a reader that buffers the body to reach the
//! trailing `Summary` section that sits *after* it. Both the JSON (#511) and
//! XML (#512) readers stream their bodies, so the pre-scan parses-and-skips
//! the body to reach the trailer and the body never lands in memory. The
//! document is fed through the de-buffered file-slot path
//! (`FileSlot::from_path` over a real temp file), NOT an in-memory `Cursor`,
//! so a whole-file buffer is never created anywhere in the harness — a
//! `Cursor` would defeat the proof by buffering the payload before the
//! reader ever runs.
//!
//! Isolating body bytes from record count. The body grows by per-record
//! *blob* size, not record count: a modest number of records each carry a
//! large undeclared blob. A body-buffer regression scales with the body's
//! BYTES (the blob), so the blob is the variable under test; the record
//! count stays low so the executor's per-output-row `ok_count` accounting
//! (one `u64` per row, held for the run) contributes a negligible, body-byte-
//! independent term. Output is discarded through [`FirstLineSink`] so the
//! harness never accumulates the emitted NDJSON either.
//!
//! The peak is measured with the monotonic high-water-mark
//! [`peak_rss_bytes`] sampled after fixture generation (which streams to
//! disk and adds nothing lasting) and again after the run; the delta is
//! asserted below a hard threshold. The assertion fails LOUDLY on a
//! body-buffer regression — there is no soft `eprintln`-and-continue.
//!
//! Gating. One shared body, sized by parameter. The default CI variants run
//! a ~150 MiB body (well over the 64 MB `max_index_bytes` default and
//! dwarfing the retained index, so bounded-vs-buffered is a real
//! distinction) and finish in seconds. A second `#[ignore]`'d pair runs the
//! full 1 GiB body — a documented heavy-data gate, run with
//! `cargo test -p clinker-exec --test doc_index_rss_proof -- --ignored`.

use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::sync::{Arc, Mutex};

use clinker_bench_support::io::SharedBuffer;
use clinker_bench_support::{FieldKind, write_nested_envelope_json, write_nested_envelope_xml};
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, SourceInput, SourceReaders};
use clinker_exec::pipeline::memory::peak_rss_bytes;
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};
use tempfile::NamedTempFile;

/// Output sink that retains only the first emitted line and discards every
/// byte after it.
///
/// The proof asserts the declared `$doc.Head` section resolves on body
/// records (needs the first output line) but must NOT itself accumulate the
/// whole output stream: at 1 GiB of body that is ~130 MiB of NDJSON, which
/// would dominate the RSS delta and mask what the *engine* retains. A real
/// `clinker run` streams output to a file descriptor and holds nothing, so
/// discarding here measures the engine's true footprint, not the harness's.
#[derive(Clone, Default)]
struct FirstLineSink(Arc<Mutex<FirstLineState>>);

#[derive(Default)]
struct FirstLineState {
    first_line: Vec<u8>,
    done: bool,
}

impl FirstLineSink {
    fn first_line(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap().first_line).into_owned()
    }
}

impl Write for FirstLineSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut st = self.0.lock().unwrap();
        if !st.done {
            if let Some(nl) = buf.iter().position(|&b| b == b'\n') {
                st.first_line.extend_from_slice(&buf[..nl]);
                st.done = true;
            } else {
                st.first_line.extend_from_slice(buf);
            }
        }
        // Report the whole slice consumed — the bytes after the first line
        // are intentionally dropped on the floor.
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Peak-RSS-delta ceiling for the bounded path, in bytes.
///
/// The fused `source -> output` path holds at most a few in-flight records
/// (the bounded ingest channel) plus the tiny retained `$doc` index, so its
/// footprint is in the low tens of MiB regardless of body byte-size. A
/// regression that buffered the body would scale with the body's bytes:
/// ~150 MiB (CI variant) or ~1 GiB (heavy variant), overshooting this
/// ceiling many times over. Started at 100 MiB. If CI noise proves flaky,
/// loosen toward 150 MiB — but the bounded path sits far below either, so a
/// body-buffer regression is caught anywhere in that band.
const RSS_DELTA_THRESHOLD: u64 = 100 * 1024 * 1024;

/// Body shape: a record *count* paired with a per-record *blob* size.
///
/// Body bytes = `records * blob_len` (plus framing). Two competing bounds
/// shape the choice:
///
/// - **In-flight channel.** The bounded ingest channel holds up to its
///   capacity (1024) records, so peak in-flight memory is roughly
///   `min(records, 1024) * blob_len`. `blob_len` must stay small enough that
///   this product sits well under [`RSS_DELTA_THRESHOLD`] even though the
///   *total* body is GB-scale — the bound is O(capacity), not O(body).
/// - **`ok_count` accounting.** The executor holds one `u64` per output row
///   for the whole run, an O(records) term. At the record counts below this
///   is a few MiB at most — negligible and, crucially, independent of the
///   body's *byte* size, the variable a body-buffer regression scales with.
///
/// A 16 KB blob keeps the channel floor near 16 MiB while letting a large
/// `records` count push the on-disk body to GB scale.
#[derive(Clone, Copy)]
struct BodySize {
    records: usize,
    blob_len: usize,
}

/// Per-record blob width, shared by both variants. 16 KB × the 1024-deep
/// channel is a ~16 MiB in-flight floor — comfortably under the threshold —
/// while a large record count scales the body to whatever size is wanted.
const BLOB_LEN: usize = 16 * 1024;

/// CI variant: ~128 MiB of body from 8,000 records × 16 KB blobs. Well over
/// the 64 MB `max_index_bytes` default and orders of magnitude larger than
/// the retained head/trailer index.
const CI_BODY: BodySize = BodySize {
    records: 8_000,
    blob_len: BLOB_LEN,
};

/// Heavy `#[ignore]`'d variant: ~1 GiB of body from 65,536 records × 16 KB
/// blobs. The 65,536-row `ok_count` term is under 2 MiB — negligible against
/// the GB-scale body a buffer regression would resident.
const XLARGE_BODY: BodySize = BodySize {
    records: 65_536,
    blob_len: BLOB_LEN,
};

/// Per-body-record typed-field string width — small; the body's bulk is the
/// `blob`, not these typed fields.
const STRING_LEN: usize = 24;

/// JSON envelope pipeline: a declared `Head` section read by the transform,
/// an undeclared `records` body, a `Summary` trailer that forces the
/// pre-scan to stream past the body. The transform reads only the head path
/// and emits one scalar — no body field is touched.
const JSON_YAML: &str = r#"
pipeline:
  name: doc_rss_proof_json
nodes:
  - type: source
    name: docs
    config:
      name: docs
      type: json
      glob: ./*.json
      options:
        record_path: records
      envelope:
        sections:
          Head:
            extract: { json_pointer: "/Head" }
            fields:
              batch_id: string
      schema:
        - { name: id, type: int }
  - type: transform
    name: tag
    input: docs
    config:
      cxl: |
        emit x = $doc.Head.batch_id
  - type: output
    name: out
    input: tag
    config:
      name: out
      type: json
      options:
        format: ndjson
      path: out.json
      include_unmapped: false
      exclude: [id]
"#;

/// XML twin of [`JSON_YAML`]. Same shape: declared `Head`, undeclared
/// `records` body, `Summary` trailer after the body.
const XML_YAML: &str = r#"
pipeline:
  name: doc_rss_proof_xml
nodes:
  - type: source
    name: docs
    config:
      name: docs
      type: xml
      glob: ./*.xml
      options:
        record_path: doc/records/record
      envelope:
        sections:
          Head:
            extract: { xml_path: "/doc/Head" }
            fields:
              batch_id: string
      schema:
        - { name: id, type: int }
  - type: transform
    name: tag
    input: docs
    config:
      cxl: |
        emit x = $doc.Head.batch_id
  - type: output
    name: out
    input: tag
    config:
      name: out
      type: json
      options:
        format: ndjson
      path: out.json
      include_unmapped: false
      exclude: [id]
"#;

/// Which document format the proof body targets.
#[derive(Clone, Copy)]
enum DocFormat {
    Json,
    Xml,
}

/// Stream a nested-envelope fixture of `size.records` rows (each carrying a
/// `size.blob_len`-byte undeclared blob) to a fresh temp file — extension
/// matching the format so the glob/source type line up — and return the
/// handle. The fixture is written through a `BufWriter` straight to disk: at
/// no point is the multi-hundred-MB body held in memory by the generator.
fn write_fixture(format: DocFormat, size: BodySize) -> NamedTempFile {
    let suffix = match format {
        DocFormat::Json => ".json",
        DocFormat::Xml => ".xml",
    };
    let tmp = tempfile::Builder::new()
        .suffix(suffix)
        .tempfile()
        .expect("create temp fixture file");
    let layout = FieldKind::default_layout(3);
    {
        let mut w = BufWriter::new(tmp.as_file());
        match format {
            DocFormat::Json => {
                write_nested_envelope_json(
                    &mut w,
                    size.records,
                    &layout,
                    STRING_LEN,
                    size.blob_len,
                    42,
                )
                .expect("stream JSON fixture to disk");
            }
            DocFormat::Xml => {
                write_nested_envelope_xml(
                    &mut w,
                    size.records,
                    &layout,
                    STRING_LEN,
                    size.blob_len,
                    42,
                )
                .expect("stream XML fixture to disk");
            }
        }
        w.flush().expect("flush fixture writer");
    }
    tmp
}

/// Run the bounded-memory proof for one format and body size: generate the
/// on-disk fixture, sample the peak-RSS watermark, run the fused pipeline
/// off the de-buffered file slot, sample the watermark again, and assert the
/// delta stays under [`RSS_DELTA_THRESHOLD`].
fn run_proof(format: DocFormat, size: BodySize) {
    let yaml = match format {
        DocFormat::Json => JSON_YAML,
        DocFormat::Xml => XML_YAML,
    };
    let config = parse_config(yaml).expect("parse proof pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile proof pipeline");

    let fixture = write_fixture(format, size);
    let read_path = fixture.path().to_path_buf();

    // De-buffered feed: the reader re-opens the file by path per pass and
    // streams off disk. A `Cursor`/`single_file_reader` would buffer the
    // whole payload and defeat the proof.
    let file = FileSlot::from_path(read_path.clone(), read_path);
    let readers: SourceReaders =
        HashMap::from([("docs".to_string(), SourceInput::Files(vec![file]))]);

    let sink = FirstLineSink::default();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(sink.clone()) as Box<dyn Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: "doc-rss-proof".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    // Sample the high-water mark AFTER fixture generation (which streamed to
    // disk and left nothing resident) and before the run.
    let peak_before = peak_rss_bytes().expect("peak_rss_bytes() on a first-class target");

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("run proof pipeline");

    let peak_after = peak_rss_bytes().expect("peak_rss_bytes() on a first-class target");

    // Every body record streamed end-to-end (proves the body was processed,
    // not silently dropped) and the declared head section resolved on each.
    assert_eq!(
        report.counters.total_count as usize, size.records,
        "every body record must stream through the fused pipeline"
    );
    assert_eq!(report.counters.dlq_count, 0, "no record dead-lettered");
    let first_line = sink.first_line();
    assert!(
        first_line.contains("RUN-00000042"),
        "the declared $doc.Head.batch_id must resolve on body records: {first_line}"
    );

    let delta = peak_after.saturating_sub(peak_before);
    if std::env::var_os("CLINKER_RSS_PROOF_REPORT").is_some() {
        eprintln!(
            "RSS-PROOF-DELTA fmt={} records={} blob_len={} before={peak_before} after={peak_after} delta={delta}",
            match format {
                DocFormat::Json => "json",
                DocFormat::Xml => "xml",
            },
            size.records,
            size.blob_len,
        );
    }
    assert!(
        delta < RSS_DELTA_THRESHOLD,
        "bounded-memory proof FAILED ({}): peak RSS rose by {delta} bytes \
         (before={peak_before}, after={peak_after}) over a body of {} records x {} blob bytes, \
         exceeding the {RSS_DELTA_THRESHOLD}-byte ceiling. A delta that scales with the body's \
         byte size means the reader buffered the undeclared body instead of streaming past it.",
        match format {
            DocFormat::Json => "JSON",
            DocFormat::Xml => "XML",
        },
        size.records,
        size.blob_len,
    );
}

#[test]
fn json_doc_body_is_streamed_not_buffered_ci() {
    run_proof(DocFormat::Json, CI_BODY);
}

#[test]
fn xml_doc_body_is_streamed_not_buffered_ci() {
    run_proof(DocFormat::Xml, CI_BODY);
}

#[test]
#[ignore = "heavy — generates ~1 GiB at test time; run with cargo test -- --ignored"]
fn json_doc_body_is_streamed_not_buffered_1gib() {
    run_proof(DocFormat::Json, XLARGE_BODY);
}

#[test]
#[ignore = "heavy — generates ~1 GiB at test time; run with cargo test -- --ignored"]
fn xml_doc_body_is_streamed_not_buffered_1gib() {
    run_proof(DocFormat::Xml, XLARGE_BODY);
}

// ── max_index_bytes cap propagation (YAML → reader, end-to-end) ──────────

/// A JSON pipeline whose declared `Head.blob` section is intentionally large
/// (~80 KB), parameterized on the `options.max_index_bytes` cap. Proves the
/// YAML cap reaches the reader's streaming pre-scan: a tiny cap aborts
/// mid-parse with the section-named cap error; a generous cap streams clean.
fn cap_pipeline_yaml(max_index_bytes: usize) -> String {
    format!(
        r#"
pipeline:
  name: doc_cap_propagation
nodes:
  - type: source
    name: docs
    config:
      name: docs
      type: json
      glob: ./*.json
      options:
        record_path: records
        max_index_bytes: {max_index_bytes}
      envelope:
        sections:
          Head:
            extract: {{ json_pointer: "/Head" }}
            fields:
              blob: string
      schema:
        - {{ name: id, type: int }}
  - type: transform
    name: tag
    input: docs
    config:
      cxl: |
        emit x = $doc.Head.blob
  - type: output
    name: out
    input: docs
    config:
      name: out
      type: json
      options:
        format: ndjson
      path: out.json
      include_unmapped: false
      exclude: [id]
"#
    )
}

/// Write a JSON document with a large declared `Head.blob` (so the section's
/// own retained bytes can exceed a tiny cap) plus a tiny two-record body, to
/// a temp file. Returns the handle.
fn write_large_head_fixture() -> NamedTempFile {
    let tmp = tempfile::Builder::new()
        .suffix(".json")
        .tempfile()
        .expect("create temp fixture file");
    {
        let mut w = BufWriter::new(tmp.as_file());
        // ~80 KB declared blob — far above the 4 KB cap below, comfortably
        // under any generous cap.
        let blob = "z".repeat(80_000);
        write!(w, "{{\"Head\":{{\"blob\":\"{blob}\"}},\"records\":[").unwrap();
        write!(w, "{{\"id\":1}},{{\"id\":2}}").unwrap();
        write!(w, "]}}").unwrap();
        w.flush().expect("flush fixture");
    }
    tmp
}

/// Build the readers/writers and run the cap pipeline over the large-head
/// fixture, returning the run `Result`.
fn run_cap_pipeline(
    max_index_bytes: usize,
) -> Result<clinker_exec::executor::ExecutionReport, clinker_plan::error::PipelineError> {
    let yaml = cap_pipeline_yaml(max_index_bytes);
    let config = parse_config(&yaml).expect("parse cap pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile cap pipeline");

    let fixture = write_large_head_fixture();
    let read_path = fixture.path().to_path_buf();
    let file = FileSlot::from_path(read_path.clone(), read_path);
    let readers: SourceReaders =
        HashMap::from([("docs".to_string(), SourceInput::Files(vec![file]))]);

    let sink = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> =
        HashMap::from([("out".to_string(), Box::new(sink) as Box<dyn Write + Send>)]);

    let params = PipelineRunParams {
        execution_id: "doc-cap".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    // Keep `fixture` alive until the run finishes (the reader re-opens it by
    // path); `run_plan_with_readers_writers` drains synchronously, so the
    // temp file outlives every read here.
    let result = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params);
    drop(fixture);
    result
}

/// A tiny `max_index_bytes` declared in YAML reaches the reader and aborts
/// the run mid-parse, naming the over-cap section and the cap value.
#[test]
fn tiny_max_index_bytes_cap_aborts_mid_parse() {
    let err = run_cap_pipeline(4096).expect_err("a 4 KB cap must reject the ~80 KB Head section");
    let msg = err.to_string();
    assert!(
        msg.contains("max_index_bytes"),
        "cap error must name the knob: {msg}"
    );
    assert!(
        msg.contains("Head"),
        "cap error must name the section: {msg}"
    );
    assert!(
        msg.contains("mid-parse"),
        "cap error must state the abort is mid-parse: {msg}"
    );
}

/// The same pipeline with a generous `max_index_bytes` streams to
/// completion — proving the tiny-cap failure was the cap firing, not a
/// structural fault in the fixture.
#[test]
fn generous_max_index_bytes_cap_streams_clean() {
    let report = run_cap_pipeline(1_000_000).expect("a 1 MB cap easily fits the ~80 KB Head");
    assert_eq!(report.counters.total_count, 2, "both body records stream");
    assert_eq!(report.counters.dlq_count, 0);
}
