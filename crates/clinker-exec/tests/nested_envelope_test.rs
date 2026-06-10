//! Multi-level (nested) envelope ingestion driven through the full
//! source-ingest driver stack.
//!
//! A reader signals nested envelope boundaries via
//! [`clinker_format::EnvelopeEvent`] as it crosses them; the ingest
//! driver maps each `OpenLevel` to a child `DocumentContext` (its sections
//! layered as siblings over the enclosing levels) and a `DocumentOpen`
//! punctuation, and each `CloseLevel` to the matching `DocumentClose`. A
//! record streamed inside the innermost level resolves every enclosing
//! level's `$doc.<section>.<field>` through the unchanged two-level CXL
//! lookup — distinct per-level section names keep the levels apart.
//!
//! These tests drive the real `drive_record_source` loop (not an isolated
//! half) through `PipelineExecutor::run_plan_with_readers_writers`, and
//! cover the cases that previously desynced the level stack:
//!
//! - a trailing envelope level that opens AND closes after the last body
//!   record (header-only inner envelope in trailing position);
//! - a header-only interchange — envelope structure with zero body records
//!   — which must still open a file-level document and emit boundaries
//!   (issue #395);
//! - multiple files, each carrying its own nested envelopes, so a file
//!   transition closes the prior file's whole level stack before the next
//!   file opens.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use clinker_format::{EnvelopeEvent, FormatError};
use clinker_record::{Record, Schema, SchemaBuilder, Value};
use indexmap::IndexMap;

use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, SourceInput};
use clinker_exec::source::RecordSource;

/// One scripted step a [`ScriptedReader`] performs on a `next_record`
/// call: emit a body record, or queue an envelope boundary the driver
/// drains around that call. A step also pins the file the reader is
/// "currently" inside, so the multi-file driver path (file-transition
/// stack close) is exercised.
enum Step {
    /// Queue an `OpenLevel` carrying one section named `name` with a
    /// single string field `tag` = `tag_val`. The driver opens a child
    /// document context layering this section over the enclosing levels.
    Open {
        file: &'static str,
        name: &'static str,
        tag_val: &'static str,
    },
    /// Queue a `CloseLevel`. The driver pops the innermost nested level.
    Close { file: &'static str },
    /// Emit a body record with `id` = the given integer. The record
    /// carries the innermost open level's flattened `$doc.*` sections.
    Record { file: &'static str, id: i64 },
}

/// A `RecordSource` that replays a fixed [`Step`] script, modeling a
/// multi-level envelope reader. Envelope events queued by the steps
/// processed during one `next_record` call are drained by the driver
/// after that call (and at end-of-input) — exactly the contract a real
/// X12 reader meets.
struct ScriptedReader {
    schema: Arc<Schema>,
    steps: std::collections::VecDeque<Step>,
    pending_events: Vec<EnvelopeEvent>,
    current_file: Option<Arc<str>>,
    file_arcs: HashMap<&'static str, Arc<str>>,
}

impl ScriptedReader {
    fn new(steps: Vec<Step>) -> Self {
        let schema = SchemaBuilder::with_capacity(1).with_field("id").build();
        Self {
            schema,
            steps: steps.into_iter().collect(),
            pending_events: Vec::new(),
            current_file: None,
            file_arcs: HashMap::new(),
        }
    }

    /// Resolve (and cache) the stable `Arc<str>` identity for a file name
    /// so the driver's `Arc::ptr_eq` file-transition check sees one
    /// pointer per file, the same way a real multi-file reader does.
    fn file_arc(&mut self, file: &'static str) -> Arc<str> {
        Arc::clone(
            self.file_arcs
                .entry(file)
                .or_insert_with(|| Arc::from(file)),
        )
    }

    fn section(name: &str, tag_val: &str) -> IndexMap<Box<str>, Value> {
        let mut field = IndexMap::new();
        field.insert(Box::from("tag"), Value::String(tag_val.into()));
        let mut sections = IndexMap::new();
        sections.insert(Box::from(name), Value::Map(Box::new(field)));
        sections
    }
}

impl RecordSource for ScriptedReader {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        Ok(Arc::clone(&self.schema))
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        // Process steps until one yields a record (or the script drains).
        // Envelope-event steps queue into `pending_events`, which the
        // driver pulls via `take_envelope_events` after this call returns.
        while let Some(step) = self.steps.pop_front() {
            match step {
                Step::Open {
                    file,
                    name,
                    tag_val,
                } => {
                    self.current_file = Some(self.file_arc(file));
                    self.pending_events.push(EnvelopeEvent::OpenLevel {
                        sections: Self::section(name, tag_val),
                    });
                }
                Step::Close { file } => {
                    self.current_file = Some(self.file_arc(file));
                    self.pending_events.push(EnvelopeEvent::CloseLevel);
                }
                Step::Record { file, id } => {
                    self.current_file = Some(self.file_arc(file));
                    return Ok(Some(Record::new(
                        Arc::clone(&self.schema),
                        vec![Value::Integer(id)],
                    )));
                }
            }
        }
        Ok(None)
    }

    fn current_source_file(&self) -> Option<&Arc<str>> {
        self.current_file.as_ref()
    }

    fn take_envelope_events(&mut self) -> Vec<EnvelopeEvent> {
        std::mem::take(&mut self.pending_events)
    }
}

/// A pipeline whose transform projects the three nested levels' tags onto
/// every body record. `$doc.interchange.tag`, `$doc.group.tag`, and
/// `$doc.transaction.tag` are read through the ordinary two-level `$doc`
/// surface — proving the flattened-sibling representation exposes all
/// three nesting levels without any CXL syntax change.
const PIPELINE: &str = r#"
pipeline:
  name: nested_envelope
nodes:
  - type: source
    name: edi
    config:
      name: edi
      type: csv
      path: placeholder.csv
      schema:
        - { name: id, type: int }
  - type: transform
    name: tag
    input: edi
    config:
      cxl: |
        emit id = id
        emit isa = $doc.interchange.tag
        emit gs = $doc.group.tag
        emit st = $doc.transaction.tag
  - type: output
    name: out
    input: tag
    config:
      name: out
      type: csv
      path: out.csv
"#;

fn run(steps: Vec<Step>) -> (clinker_exec::executor::ExecutionReport, String) {
    let mut config = clinker_plan::config::parse_config(PIPELINE).unwrap();
    // Pathless source: the executor drives the registered `Records`
    // transport, never fs discovery. The reader supplies its own per-file
    // identity through `current_source_file`.
    for spanned in &mut config.nodes {
        if let clinker_plan::config::PipelineNode::Source { config: body, .. } = &mut spanned.value
        {
            body.source.path = None;
        }
    }
    let plan = config
        .compile(&clinker_plan::config::CompileContext::default())
        .expect("compile nested-envelope pipeline");

    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "edi".to_string(),
        SourceInput::Records(Box::new(ScriptedReader::new(steps))),
    )]);

    let output_buf = clinker_bench_support::io::SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(output_buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &PipelineRunParams {
            execution_id: "nested-exec".to_string(),
            batch_id: "nested-batch".to_string(),
            ..Default::default()
        },
    )
    .expect("drive nested-envelope source to completion");

    let output = output_buf.as_string();
    (report, output)
}

#[test]
fn nested_levels_resolve_all_ancestor_sections_on_body_records() {
    // ISA → GS → ST → two records → close ST → close GS → close ISA.
    let steps = vec![
        Step::Open {
            file: "claim.x12",
            name: "interchange",
            tag_val: "ISA-1",
        },
        Step::Open {
            file: "claim.x12",
            name: "group",
            tag_val: "GS-HC",
        },
        Step::Open {
            file: "claim.x12",
            name: "transaction",
            tag_val: "ST-837",
        },
        Step::Record {
            file: "claim.x12",
            id: 1,
        },
        Step::Record {
            file: "claim.x12",
            id: 2,
        },
        Step::Close { file: "claim.x12" }, // ST
        Step::Close { file: "claim.x12" }, // GS
        Step::Close { file: "claim.x12" }, // ISA
    ];

    let (report, output) = run(steps);
    assert_eq!(report.counters.total_count, 2, "two body records");
    assert_eq!(report.counters.dlq_count, 0);

    let lines: Vec<&str> = output.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 3, "header + 2 rows; got:\n{output}");
    // Every body record resolves all THREE nesting levels through the
    // two-level $doc surface — the flattened-sibling representation.
    for row in &lines[1..] {
        assert!(
            row.contains("ISA-1"),
            "row missing $doc.interchange.tag: {row}"
        );
        assert!(row.contains("GS-HC"), "row missing $doc.group.tag: {row}");
        assert!(
            row.contains("ST-837"),
            "row missing $doc.transaction.tag: {row}"
        );
    }
}

#[test]
fn trailing_inner_envelope_after_last_record_does_not_desync() {
    // The desync regression: an inner envelope that OPENS and closes after
    // the last body record. A driver that skips trailing `OpenLevel`
    // events at end-of-input would leave the level stack unbalanced and
    // abort; the run must instead complete cleanly with the records it
    // produced.
    let steps = vec![
        Step::Open {
            file: "trail.x12",
            name: "interchange",
            tag_val: "ISA-9",
        },
        Step::Open {
            file: "trail.x12",
            name: "transaction",
            tag_val: "ST-A",
        },
        Step::Record {
            file: "trail.x12",
            id: 10,
        },
        Step::Close { file: "trail.x12" }, // ST closes after its record
        // A trailing inner envelope: opens AND closes with no body record
        // between, after the final record of the file. This is the
        // sequence that previously desynced the stack at EOF.
        Step::Open {
            file: "trail.x12",
            name: "transaction",
            tag_val: "ST-EMPTY",
        },
        Step::Close { file: "trail.x12" },
        Step::Close { file: "trail.x12" }, // ISA
    ];

    let (report, output) = run(steps);
    // The run completed (no abort) and emitted exactly the one real body
    // record; the trailing empty envelope contributed none.
    assert_eq!(report.counters.total_count, 1);
    assert_eq!(report.counters.dlq_count, 0);
    let lines: Vec<&str> = output.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 2, "header + 1 row; got:\n{output}");
    assert!(lines[1].contains("ISA-9"));
    assert!(lines[1].contains("ST-A"));
}

#[test]
fn header_only_interchange_opens_a_document_with_no_body_records() {
    // Issue #395: a file carrying envelope structure but zero body records
    // must still open a file-level document and emit open/close
    // boundaries. Here the entire script is an interchange that opens and
    // closes with nothing inside.
    let steps = vec![
        Step::Open {
            file: "empty.x12",
            name: "interchange",
            tag_val: "ISA-EMPTY",
        },
        Step::Close { file: "empty.x12" },
    ];

    let (report, _output) = run(steps);
    // Zero body records, and the run completed without aborting — the
    // header-only interchange opened and closed its document cleanly
    // instead of leaving the stack unbalanced.
    assert_eq!(report.counters.total_count, 0);
    assert_eq!(report.counters.dlq_count, 0);
    assert_eq!(report.counters.ok_count, 0);
}

#[test]
fn multi_file_each_with_nested_envelopes_and_a_header_only_file() {
    // Multi-file × nested-envelope through the full driver stack: the
    // first file carries a normal nested interchange with body records,
    // the second is a header-only interchange (zero body). The file
    // transition must close the first file's whole level stack before the
    // second file opens, and the header-only second file must still frame
    // a document.
    let steps = vec![
        // File a: ISA → GS → ST → 1 record → close all.
        Step::Open {
            file: "a.x12",
            name: "interchange",
            tag_val: "A-ISA",
        },
        Step::Open {
            file: "a.x12",
            name: "group",
            tag_val: "A-GS",
        },
        Step::Open {
            file: "a.x12",
            name: "transaction",
            tag_val: "A-ST",
        },
        Step::Record {
            file: "a.x12",
            id: 100,
        },
        Step::Close { file: "a.x12" }, // ST
        Step::Close { file: "a.x12" }, // GS
        Step::Close { file: "a.x12" }, // ISA
        // File b: header-only interchange, no body records.
        Step::Open {
            file: "b.x12",
            name: "interchange",
            tag_val: "B-ISA",
        },
        Step::Close { file: "b.x12" },
    ];

    let (report, output) = run(steps);
    assert_eq!(
        report.counters.total_count, 1,
        "one body record, from file a"
    );
    assert_eq!(report.counters.dlq_count, 0);

    let lines: Vec<&str> = output.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 2, "header + 1 row; got:\n{output}");
    // The single body record sees file a's three nesting levels and never
    // file b's interchange — cross-file context never leaks.
    assert!(lines[1].contains("A-ISA"));
    assert!(lines[1].contains("A-GS"));
    assert!(lines[1].contains("A-ST"));
    assert!(
        !lines[1].contains("B-ISA"),
        "file b context leaked: {}",
        lines[1]
    );
}

#[test]
fn second_file_record_carries_its_own_context_not_the_first_files() {
    // Two files, each with body records inside their own interchange. The
    // file transition must rebind every per-file document context: the
    // second file's record must resolve its OWN interchange tag, never the
    // first file's. This is the observable counterpart to the header-only
    // multi-file case — a record makes the cross-file attribution visible.
    let steps = vec![
        Step::Open {
            file: "a.x12",
            name: "interchange",
            tag_val: "A-ISA",
        },
        Step::Record {
            file: "a.x12",
            id: 1,
        },
        Step::Close { file: "a.x12" },
        Step::Open {
            file: "b.x12",
            name: "interchange",
            tag_val: "B-ISA",
        },
        Step::Record {
            file: "b.x12",
            id: 2,
        },
        Step::Close { file: "b.x12" },
    ];

    let (report, output) = run(steps);
    assert_eq!(report.counters.total_count, 2);
    assert_eq!(report.counters.dlq_count, 0);

    let lines: Vec<&str> = output.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 3, "header + 2 rows; got:\n{output}");
    // CSV order follows input order: row for id=1 (file a), then id=2
    // (file b). Each carries only its own file's interchange tag.
    let row_a = lines[1..]
        .iter()
        .find(|l| l.starts_with("1,"))
        .expect("file a row");
    let row_b = lines[1..]
        .iter()
        .find(|l| l.starts_with("2,"))
        .expect("file b row");
    assert!(
        row_a.contains("A-ISA") && !row_a.contains("B-ISA"),
        "{row_a}"
    );
    assert!(
        row_b.contains("B-ISA") && !row_b.contains("A-ISA"),
        "{row_b}"
    );
}
