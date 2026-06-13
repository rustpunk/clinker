//! End-to-end document-envelope ingestion: an XML source declares
//! envelope sections at the file head and tail, the reader's pre-scan
//! extracts both before body streaming, and a downstream transform
//! reads `$doc.<section>.<field>` on every body record — including the
//! first row, proving the tail section was available up front.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};

const YAML: &str = r#"
pipeline:
  name: envelope_demo
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: xml
      glob: ./*.xml
      options:
        record_path: doc/records/record
      envelope:
        sections:
          BatchInfo:
            extract: { xml_path: "/doc/BatchInfo" }
            fields:
              batch_id: string
          Summary:
            extract: { xml_path: "/doc/Summary" }
            fields:
              total: int
      schema:
        - { name: amount, type: int }
  - type: transform
    name: tag
    input: payments
    config:
      cxl: |
        emit amount = amount
        emit batch = $doc.BatchInfo.batch_id
  - type: transform
    name: tag2
    input: tag
    config:
      cxl: |
        emit amount = amount
        emit batch = batch
        emit declared_total = $doc.Summary.total
  - type: output
    name: out
    input: tag2
    config:
      name: out
      type: csv
      path: out.csv
"#;

const DOC_XML: &str = r#"<doc>
  <BatchInfo><batch_id>RUN-001</batch_id></BatchInfo>
  <records>
    <record><amount>10</amount></record>
    <record><amount>20</amount></record>
    <record><amount>30</amount></record>
  </records>
  <Summary><total>3</total></Summary>
</doc>"#;

#[test]
fn envelope_sections_available_on_every_body_record() {
    let config = parse_config(YAML).expect("parse envelope pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile envelope pipeline");

    let file = FileSlot::new(
        PathBuf::from("payments.xml"),
        Box::new(Cursor::new(DOC_XML.as_bytes().to_vec())),
    );
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "payments".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![file]),
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("run envelope pipeline");
    assert_eq!(report.counters.total_count, 3, "three body records");
    assert_eq!(report.counters.dlq_count, 0);

    let output = buf.as_string();
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 4, "header + 3 data rows; got: {output}");

    // Head section (BatchInfo) and tail section (Summary) both resolve
    // on EVERY body record — including the first, which proves the
    // tail section was pre-scanned before body streaming began.
    for (i, row) in lines[1..].iter().enumerate() {
        assert!(
            row.contains("RUN-001"),
            "row {i} missing $doc.BatchInfo.batch_id: {row}"
        );
        // `declared_total` is the trailer's record count, read on each
        // body row — available mid-stream because the pre-scan pulled
        // the tail section up front.
        assert!(
            row.contains(",3") || row.ends_with("3"),
            "row {i} missing $doc.Summary.total: {row}"
        );
    }
}

/// `$doc.<section>.<field>` must resolve correctly even when the body record
/// carrying that context has round-tripped through an upstream record spill.
/// A Route fans the envelope source's body out through a materialized branch
/// whose `node_buffers` spill records to disk under a 1 MiB budget (the
/// RSS-soft predicate from `route_fanout_soft_spill.rs`); the downstream
/// Transform then reads `$doc.BatchInfo.batch_id` off the re-hydrated records.
/// If the envelope context were dropped on the spill round-trip, the spilled
/// records would re-hydrate to the empty synthetic context and `$doc.*` would
/// resolve to null — so a non-null, correct value on every spilled body row
/// is the end-to-end proof the interned context survived.
const SPILL_ENVELOPE_YAML: &str = r#"
pipeline:
  name: envelope_spill
  memory: { limit: "1M", backpressure: spill }
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: xml
      glob: ./*.xml
      options:
        record_path: doc/records/record
      envelope:
        sections:
          BatchInfo:
            extract: { xml_path: "/doc/BatchInfo" }
            fields:
              batch_id: string
          Summary:
            extract: { xml_path: "/doc/Summary" }
            fields:
              total: int
      schema:
        - { name: amount, type: int }
        - { name: pad, type: string }
  - type: route
    name: split
    input: payments
    config:
      mode: exclusive
      conditions:
        keep: "true"
      default: drop
  - type: transform
    name: tag
    input: split.keep
    config:
      cxl: |
        emit amount = amount
        emit batch = $doc.BatchInfo.batch_id
        emit declared_total = $doc.Summary.total
  - type: output
    name: out
    input: tag
    config:
      name: out
      type: csv
      path: out.csv
"#;

#[test]
fn doc_context_resolves_after_upstream_record_spill() {
    // RSS-based spill predicate: without RSS reading the upstream
    // node_buffer never spills, so the round-trip under test never fires.
    if clinker_exec::pipeline::memory::rss_bytes().is_none() {
        return;
    }

    // One document, many body rows with a wide `pad` column so the Route
    // branch slot crosses the RSS soft floor and spills its records through
    // the inter-stage SpillWriter. The envelope sections live once per
    // document, interned a single time on spill regardless of row count.
    const ROWS: usize = 1_500;
    let pad = "z".repeat(64);
    let mut xml =
        String::from("<doc>\n  <BatchInfo><batch_id>RUN-777</batch_id></BatchInfo>\n  <records>\n");
    for i in 0..ROWS {
        xml.push_str(&format!(
            "    <record><amount>{i}</amount><pad>{pad}</pad></record>\n"
        ));
    }
    xml.push_str(&format!(
        "  </records>\n  <Summary><total>{ROWS}</total></Summary>\n</doc>"
    ));

    let config = parse_config(SPILL_ENVELOPE_YAML).expect("parse spill-envelope pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile spill-envelope pipeline");

    let file = FileSlot::new(
        PathBuf::from("payments.xml"),
        Box::new(Cursor::new(xml.into_bytes())),
    );
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "payments".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![file]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("run spill-envelope pipeline");
    assert_eq!(report.counters.total_count as usize, ROWS, "all body rows");
    assert_eq!(report.counters.dlq_count, 0);

    // Fail loudly if the run stopped spilling — otherwise the $doc assertion
    // below would pass without ever exercising the spill round-trip.
    assert!(
        report.cumulative_spill_bytes > 0,
        "upstream node_buffer must have spilled records under the 1 MiB budget; \
         cumulative_spill_bytes = {}",
        report.cumulative_spill_bytes,
    );

    // Every output row resolves the head section (batch_id) and tail section
    // (total) off its re-hydrated record — the in-memory baseline value,
    // never the synthetic-context null a dropped context would yield.
    let output = buf.as_string();
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), ROWS + 1, "header + {ROWS} data rows");
    for (i, row) in lines[1..].iter().enumerate() {
        assert!(
            row.contains("RUN-777"),
            "row {i} lost $doc.BatchInfo.batch_id across the spill round-trip: {row}"
        );
        assert!(
            row.contains(&format!(",{ROWS}")),
            "row {i} lost $doc.Summary.total across the spill round-trip: {row}"
        );
    }
}
