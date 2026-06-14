//! End-to-end behavior of the standalone `Envelope` node with the `preserve`
//! framing strategy.
//!
//! `preserve` is a transparent framing stage: it passes body records through
//! with their document context and grain unchanged and forwards the
//! document-boundary punctuations, so a downstream Output frames per grain
//! exactly as it would without the node. These tests prove three claims:
//!
//! - **Differential oracle (the headline):** the SAME pipeline with a
//!   `preserve` Envelope node between the body and a `reconstruct_envelope`
//!   Output produces byte-identical output AND the same per-document framing
//!   count as the pipeline without it. A `preserve` that altered grains, order,
//!   or punctuations would diverge.
//! - **Grain-keyed framing, not file-keyed:** a nested X12-style interchange
//!   (whose `GS`/`ST` levels inherit the interchange grain) frames once per
//!   interchange; an HL7-style multi-message file (each message a fresh grain)
//!   frames once per message.
//! - **Config parse / serde** for the node and its single-variant strategy.
//!
//! Framing is observed through the CSV writer's per-document envelope
//! rendering: with `reconstruct_envelope: true` and an `envelope:` spec naming
//! a `$doc` section, the writer emits one footer row per framed document
//! carrying the streaming-computed body-record count. Counting footer rows
//! yields the document count; the per-footer count validates the body→grain
//! partitioning.

use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::path::PathBuf;
use std::sync::Arc;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, SourceInput};
use clinker_exec::source::RecordSource;
use clinker_exec::source::multi_file::FileSlot;
use clinker_format::{EnvelopeEvent, FormatError, FrameRole};
use clinker_record::{Record, Schema, SchemaBuilder, Value};
use indexmap::IndexMap;

// ─── Scripted multi-level envelope reader ───────────────────────────────
//
// Replays a fixed step script modeling a multi-level envelope reader (cf.
// the nested-envelope ingest tests). Extended here with a per-`Open` frame
// role so a script can model both an X12 interchange (`Inherit` — nested
// levels share the file grain) and an HL7 multi-message file (`NewFrame` —
// each message mints its own grain).

/// One scripted step. `Open` carries one section (`name` → `tag`) and a frame
/// role; `Record` emits a body row; `Close` pops the innermost level.
enum Step {
    Open {
        file: &'static str,
        name: &'static str,
        tag_val: &'static str,
        frame: FrameRole,
    },
    Close {
        file: &'static str,
    },
    Record {
        file: &'static str,
        id: i64,
    },
}

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
        while let Some(step) = self.steps.pop_front() {
            match step {
                Step::Open {
                    file,
                    name,
                    tag_val,
                    frame,
                } => {
                    self.current_file = Some(self.file_arc(file));
                    self.pending_events.push(EnvelopeEvent::OpenLevel {
                        sections: Self::section(name, tag_val),
                        frame,
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

// ─── Pipeline templates ─────────────────────────────────────────────────

/// A pipeline that frames each document into a CSV with a per-document footer
/// carrying the streaming body-record count. When `with_envelope` is set, a
/// `preserve` Envelope node sits between the body Transform and the Output;
/// otherwise the Transform feeds the Output directly. Both feed the SAME
/// `reconstruct_envelope` Output, so the only difference is the presence of
/// the framing stage — the differential-oracle control.
fn scripted_pipeline(with_envelope: bool) -> String {
    // When the Envelope node is present the Output reads from `framed` (the
    // Envelope node); when absent the Output reads directly from the body
    // Transform `tag`. The Transform keeps the same name in both arms, so the
    // only structural difference is the inserted framing stage — the control
    // for the differential oracle.
    let (envelope_node, output_input) = if with_envelope {
        (
            "  - type: envelope\n    name: framed\n    body: tag\n    \
             config: { strategy: preserve }\n",
            "framed",
        )
    } else {
        ("", "tag")
    };
    let transform_name = "tag";
    format!(
        r#"
pipeline:
  name: envelope_scripted
nodes:
  - type: source
    name: edi
    config:
      name: edi
      type: csv
      path: placeholder.csv
      # The `interchange` section is declared so the Output's
      # `footer_from_doc: interchange` passes the feeding-source section-name
      # check; the scripted reader supplies its runtime value per document.
      envelope:
        sections:
          interchange:
            extract: {{ record_type: H }}
            fields:
              tag: string
      schema:
        - {{ name: id, type: int }}
  - type: transform
    name: {transform_name}
    input: edi
    config:
      cxl: |
        emit id = id
        emit tag = $doc.interchange.tag
{envelope_node}  - type: output
    name: out
    input: {output_input}
    config:
      name: out
      type: csv
      path: out.csv
      reconstruct_envelope: true
      options:
        envelope:
          footer_from_doc: interchange
"#
    )
}

/// Compile and run a scripted-reader pipeline, returning the written CSV bytes
/// plus the run counters.
fn run_scripted(
    with_envelope: bool,
    steps: Vec<Step>,
) -> (String, clinker_record::PipelineCounters) {
    let mut config = clinker_plan::config::parse_config(&scripted_pipeline(with_envelope))
        .expect("parse scripted envelope pipeline");
    // Pathless source: drive the registered `Records` transport, not fs.
    for spanned in &mut config.nodes {
        if let clinker_plan::config::PipelineNode::Source { config: body, .. } = &mut spanned.value
        {
            body.source.path = None;
        }
    }
    let plan = config
        .compile(&clinker_plan::config::CompileContext::default())
        .expect("compile scripted envelope pipeline");

    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "edi".to_string(),
        SourceInput::Records(Box::new(ScriptedReader::new(steps))),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &PipelineRunParams {
            execution_id: "env-exec".to_string(),
            batch_id: "env-batch".to_string(),
            ..Default::default()
        },
    )
    .expect("drive scripted envelope pipeline");
    (buf.as_string(), report.counters)
}

/// Count the per-document footer rows the CSV envelope writer rendered. Each
/// Parse the framed CSV into the per-document framing it reveals.
///
/// The CSV writer emits, per framed document, one footer **section row** at the
/// document's close carrying the `interchange.tag` value as its single cell.
/// Body rows are `id,tag` (two cells) and the column header is `id,tag`. So a
/// document boundary is exactly a single-cell row; the body rows between two
/// boundaries belong to the document that boundary closes.
///
/// Returns `(framed_document_count, per_document_body_counts)`. Asserting on
/// both is what makes a grain merge/split visible: a `preserve` that collapsed
/// two grains into one would drop a footer and merge two body counts.
fn footer_stats(csv: &str) -> (usize, Vec<i64>) {
    let mut footers: Vec<i64> = Vec::new();
    let mut pending_body: i64 = 0;
    for line in csv.lines() {
        let cells: Vec<&str> = line.split(',').collect();
        match cells.as_slice() {
            // Column header `id,tag`: a body row's first cell is numeric, the
            // header's is the literal column name `id`. Both are two-cell;
            // the header is skipped by recognizing the literal column names.
            [a, b] if *a == "id" && *b == "tag" => {}
            // Body row `id,tag`: first cell is the numeric id.
            [a, _] if a.parse::<i64>().is_ok() => pending_body += 1,
            // Footer section row: a single cell carrying the document's tag.
            [_single] => {
                footers.push(pending_body);
                pending_body = 0;
            }
            _ => {}
        }
    }
    (footers.len(), footers)
}

// ─── Differential oracle (the headline acceptance) ──────────────────────

#[test]
fn preserve_envelope_is_byte_identical_to_no_envelope() {
    // Two files, each a self-contained interchange (Inherit levels share the
    // file grain), so the run frames exactly twice.
    let steps = || {
        vec![
            Step::Open {
                file: "a.x12",
                name: "interchange",
                tag_val: "ISA-A",
                frame: FrameRole::Inherit,
            },
            Step::Record {
                file: "a.x12",
                id: 1,
            },
            Step::Record {
                file: "a.x12",
                id: 2,
            },
            Step::Close { file: "a.x12" },
            Step::Open {
                file: "b.x12",
                name: "interchange",
                tag_val: "ISA-B",
                frame: FrameRole::Inherit,
            },
            Step::Record {
                file: "b.x12",
                id: 3,
            },
            Step::Close { file: "b.x12" },
        ]
    };

    let (without, without_counters) = run_scripted(false, steps());
    let (with, with_counters) = run_scripted(true, steps());

    // Sanity: the body rows are present and tagged from their own interchange.
    assert!(without.contains("ISA-A"), "missing doc A rows: {without}");
    assert!(without.contains("ISA-B"), "missing doc B rows: {without}");

    // Headline: inserting a `preserve` Envelope node changes nothing — same
    // bytes, same record count. A grain-altering or reordering preserve fails.
    assert_eq!(
        with, without,
        "preserve Envelope must be byte-identical to the no-envelope path"
    );
    assert_eq!(
        with_counters.records_written, without_counters.records_written,
        "records_written invariant under the preserve Envelope node",
    );

    // And the framing count is equal: exactly two framed documents either way,
    // with the body counts (2 then 1) keyed on grain.
    let (without_frames, without_counts) = footer_stats(&without);
    let (with_frames, with_counts) = footer_stats(&with);
    assert_eq!(
        without_frames, 2,
        "two interchanges → two framed documents (control): {without}"
    );
    assert_eq!(
        with_frames, without_frames,
        "preserve Envelope preserves the framing count"
    );
    assert_eq!(
        without_counts,
        vec![2, 1],
        "per-document body counts keyed on grain (control): {without}"
    );
    assert_eq!(
        with_counts, without_counts,
        "preserve Envelope preserves per-document body counts"
    );
}

// ─── Grain-keyed framing: nested X12 interchange ────────────────────────

#[test]
fn nested_x12_interchange_frames_once_per_interchange() {
    // One interchange, three nested levels: ISA → GS → ST → records. Every
    // nested level `Inherit`s the interchange grain, so the whole `ISA..IEA`
    // is ONE framed document regardless of the GS/ST nesting — framing is
    // keyed on grain, not on the (single) source file or the nested levels.
    let steps = vec![
        Step::Open {
            file: "claim.x12",
            name: "interchange",
            tag_val: "ISA-1",
            frame: FrameRole::Inherit,
        },
        Step::Open {
            file: "claim.x12",
            name: "group",
            tag_val: "GS-1",
            frame: FrameRole::Inherit,
        },
        Step::Open {
            file: "claim.x12",
            name: "transaction",
            tag_val: "ST-1",
            frame: FrameRole::Inherit,
        },
        Step::Record {
            file: "claim.x12",
            id: 10,
        },
        Step::Record {
            file: "claim.x12",
            id: 11,
        },
        Step::Close { file: "claim.x12" }, // ST
        Step::Close { file: "claim.x12" }, // GS
        Step::Close { file: "claim.x12" }, // ISA
    ];

    let (csv, _counters) = run_scripted(true, steps);
    let (frames, counts) = footer_stats(&csv);
    assert_eq!(
        frames, 1,
        "nested ISA/GS/ST is ONE framed document (grain-keyed): {csv}"
    );
    assert_eq!(
        counts,
        vec![2],
        "the one interchange frames both body records: {csv}"
    );
}

#[test]
fn two_x12_interchanges_frame_twice() {
    // Two interchanges in two files → two grains → two framed documents. Guards
    // against a framing keyed on something coarser than the interchange grain.
    let steps = vec![
        Step::Open {
            file: "a.x12",
            name: "interchange",
            tag_val: "ISA-A",
            frame: FrameRole::Inherit,
        },
        Step::Record {
            file: "a.x12",
            id: 1,
        },
        Step::Close { file: "a.x12" },
        Step::Open {
            file: "b.x12",
            name: "interchange",
            tag_val: "ISA-B",
            frame: FrameRole::Inherit,
        },
        Step::Record {
            file: "b.x12",
            id: 2,
        },
        Step::Record {
            file: "b.x12",
            id: 3,
        },
        Step::Close { file: "b.x12" },
    ];

    let (csv, _counters) = run_scripted(true, steps);
    let (frames, counts) = footer_stats(&csv);
    assert_eq!(frames, 2, "two interchanges → two framed documents: {csv}");
    assert_eq!(counts, vec![1, 2], "per-interchange body counts: {csv}");
}

// ─── Grain-keyed framing: HL7 multi-message file ────────────────────────

#[test]
fn hl7_multi_message_file_frames_once_per_message() {
    // One file, two HL7-style messages. Each message opens a `NewFrame` level
    // (mints a fresh grain, the way each `MSH` does), so the single file frames
    // TWICE — keyed on the per-message grain, NOT on the source file. A
    // file-keyed framing would collapse this to one document and fail.
    let steps = vec![
        Step::Open {
            file: "batch.hl7",
            name: "interchange",
            tag_val: "MSH-1",
            frame: FrameRole::NewFrame,
        },
        Step::Record {
            file: "batch.hl7",
            id: 1,
        },
        Step::Record {
            file: "batch.hl7",
            id: 2,
        },
        Step::Close { file: "batch.hl7" }, // MSH-1
        Step::Open {
            file: "batch.hl7",
            name: "interchange",
            tag_val: "MSH-2",
            frame: FrameRole::NewFrame,
        },
        Step::Record {
            file: "batch.hl7",
            id: 3,
        },
        Step::Close { file: "batch.hl7" }, // MSH-2
    ];

    let (csv, _counters) = run_scripted(true, steps);
    let (frames, counts) = footer_stats(&csv);
    assert_eq!(
        frames, 2,
        "two HL7 messages in one file → two framed documents (grain-keyed, not file-keyed): {csv}"
    );
    assert_eq!(
        counts,
        vec![2, 1],
        "per-message body counts keyed on the MSH grain: {csv}"
    );
}

#[test]
fn hl7_framing_matches_no_envelope_control() {
    // The same HL7 multi-message file with and without the preserve Envelope
    // node must produce identical framing — proving the node neither merges nor
    // splits the per-message grains.
    let steps = || {
        vec![
            Step::Open {
                file: "batch.hl7",
                name: "interchange",
                tag_val: "MSH-1",
                frame: FrameRole::NewFrame,
            },
            Step::Record {
                file: "batch.hl7",
                id: 1,
            },
            Step::Close { file: "batch.hl7" },
            Step::Open {
                file: "batch.hl7",
                name: "interchange",
                tag_val: "MSH-2",
                frame: FrameRole::NewFrame,
            },
            Step::Record {
                file: "batch.hl7",
                id: 2,
            },
            Step::Close { file: "batch.hl7" },
        ]
    };
    let (without, _) = run_scripted(false, steps());
    let (with, _) = run_scripted(true, steps());
    assert_eq!(
        with, without,
        "preserve Envelope preserves HL7 per-message framing byte-for-byte"
    );
    assert_eq!(footer_stats(&with).0, 2, "two MSH frames survive: {with}");
}

// ─── Punctuation forwarding: per-document Aggregate downstream ───────────

/// A pipeline `source → preserve Envelope → per-document count Aggregate →
/// output` over a multi-file CSV glob. The Aggregate flushes its groups on
/// each document-close punctuation, so it produces one group set per document
/// ONLY if the Envelope forwards those boundary punctuations. A `preserve`
/// that swallowed them would collapse every file into one cross-document
/// aggregate — the regression this test exists to catch. When
/// `with_envelope` is false the Envelope node is omitted (the control).
fn aggregate_pipeline(with_envelope: bool) -> String {
    let (envelope_node, agg_input) = if with_envelope {
        (
            "  - type: envelope\n    name: framed\n    body: events\n    \
             config: { strategy: preserve }\n",
            "framed",
        )
    } else {
        ("", "events")
    };
    format!(
        r#"
pipeline:
  name: envelope_agg
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      glob: ./*.csv
      files:
        on_no_match: skip
      schema:
        - {{ name: category, type: string }}
{envelope_node}  - type: aggregate
    name: by_category
    input: {agg_input}
    config:
      group_by:
        - category
      cxl: |
        emit category = category
        emit n = count(*)
  - type: output
    name: out
    input: by_category
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#
    )
}

/// Run the aggregate pipeline over in-memory CSV files (each file a document),
/// returning the sorted `category,n` body lines.
fn run_aggregate(with_envelope: bool, files: &[(&str, &str)]) -> Vec<String> {
    let config =
        clinker_plan::config::parse_config(&aggregate_pipeline(with_envelope)).expect("parse");
    let plan = config
        .compile(&clinker_plan::config::CompileContext::default())
        .expect("compile");

    let slots: Vec<FileSlot> = files
        .iter()
        .map(|(name, body)| {
            FileSlot::new(
                PathBuf::from(*name),
                Box::new(Cursor::new(body.as_bytes().to_vec())),
            )
        })
        .collect();
    let readers: clinker_exec::executor::SourceReaders =
        HashMap::from([("events".to_string(), SourceInput::Files(slots))]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &PipelineRunParams {
            execution_id: "agg".to_string(),
            batch_id: "agg".to_string(),
            ..Default::default()
        },
    )
    .expect("run aggregate pipeline");

    let mut body: Vec<String> = buf
        .as_string()
        .lines()
        .skip(1)
        .map(str::to_string)
        .collect();
    body.sort();
    body
}

#[test]
fn preserve_envelope_forwards_document_boundaries_to_downstream_aggregate() {
    // Two files = two documents. Category `x` appears twice in A and three
    // times in B. A per-document flush yields x=2 (doc A) and x=3 (doc B) plus
    // y=1 (doc A) — three rows. If the Envelope swallowed the document-close
    // punctuations the Aggregate would collapse to a single cross-document
    // x=5, so this is the punctuation-forwarding regression guard.
    let files = [
        ("a.csv", "category\nx\nx\ny\n"),
        ("b.csv", "category\nx\nx\nx\n"),
    ];

    let with = run_aggregate(true, &files);
    assert_eq!(
        with,
        vec!["x,2".to_string(), "x,3".to_string(), "y,1".to_string()],
        "preserve Envelope must forward per-document closes so the downstream \
         Aggregate flushes per document (x=2 doc A, x=3 doc B), not a merged x=5",
    );

    // And it matches the no-Envelope control exactly — the node changes nothing
    // about the document boundaries the Aggregate observes.
    let without = run_aggregate(false, &files);
    assert_eq!(
        with, without,
        "per-document aggregation is identical with and without the preserve Envelope",
    );
}
