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
/// role; `OpenHeaderless` opens a frame level carrying NO sections (a genuinely
/// headerless document, e.g. an HL7 message whose header extracts no fields);
/// `Record` emits a body row; `Close` pops the innermost level.
enum Step {
    Open {
        file: &'static str,
        name: &'static str,
        tag_val: &'static str,
        frame: FrameRole,
    },
    /// Open a frame whose section carries the same user `tag` field PLUS an
    /// engine-injected `$raw` key (an X12-style raw element shadow). The `raw`
    /// value distinguishes two otherwise-identical headers, modeling the
    /// interchange-control-number drift `same_header` must ignore.
    OpenWithRaw {
        file: &'static str,
        name: &'static str,
        tag_val: &'static str,
        raw: &'static str,
        frame: FrameRole,
    },
    OpenHeaderless {
        file: &'static str,
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

    /// Like [`Self::section`], but the section payload also carries an engine-
    /// injected `$raw` key (the `$`-sigil convention the X12/EDIFACT/HL7 readers
    /// use to stash lossless raw bytes alongside the user fields).
    fn section_with_raw(name: &str, tag_val: &str, raw: &str) -> IndexMap<Box<str>, Value> {
        let mut field = IndexMap::new();
        field.insert(Box::from("tag"), Value::String(tag_val.into()));
        field.insert(Box::from("$raw"), Value::String(raw.into()));
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
                Step::OpenWithRaw {
                    file,
                    name,
                    tag_val,
                    raw,
                    frame,
                } => {
                    self.current_file = Some(self.file_arc(file));
                    self.pending_events.push(EnvelopeEvent::OpenLevel {
                        sections: Self::section_with_raw(name, tag_val, raw),
                        frame,
                    });
                }
                Step::OpenHeaderless { file, frame } => {
                    self.current_file = Some(self.file_arc(file));
                    self.pending_events.push(EnvelopeEvent::OpenLevel {
                        sections: IndexMap::new(),
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

/// A pipeline `source → Envelope → per-document count Aggregate → output` over
/// a multi-file CSV glob. The Aggregate flushes its groups on each
/// document-close punctuation. `strategy` selects the Envelope's framing — or,
/// when `None`, omits the Envelope entirely (the control):
///
/// - `Some("preserve")` forwards each file's boundaries, so the Aggregate
///   produces one group set per file. A `preserve` that swallowed the boundaries
///   would collapse every file into one cross-document aggregate.
/// - `Some("concat")` consolidates all files onto one document, so the Aggregate
///   flushes ONCE — every file's groups merge into a single cross-file set.
fn aggregate_pipeline(strategy: Option<&str>) -> String {
    let (envelope_node, agg_input) = match strategy {
        Some(s) => (
            format!(
                "  - type: envelope\n    name: framed\n    body: events\n    \
                 config: {{ strategy: {s} }}\n"
            ),
            "framed",
        ),
        None => (String::new(), "events"),
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
fn run_aggregate(strategy: Option<&str>, files: &[(&str, &str)]) -> Vec<String> {
    let config = clinker_plan::config::parse_config(&aggregate_pipeline(strategy)).expect("parse");
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

// ─── Concat strategy: multi-document body → one framed document ──────────
//
// `concat` collapses every body grain onto ONE consolidated document context,
// so a multi-document body frames as a single document (one open, one close).
// These fixtures drive the SAME single-source `ScriptedReader`, using two
// `NewFrame` opens to model two distinct documents feeding the Envelope's one
// input port — grain-equivalent to a 2-input "Merge → Envelope" (a Merge would
// hand the Envelope two grains exactly as two `NewFrame` levels do), without the
// extra source-wiring the single-port harness would otherwise need.

/// A concat pipeline: source → body Transform → `concat` Envelope → a SECOND
/// Transform reading `$doc.interchange.tag` (the consolidated header, post-
/// concat) → Output. The post-envelope Transform is what makes the consolidated
/// header observable on a body record: it resolves `$doc` against the document
/// context concat re-stamped, not the source's per-document context.
fn concat_pipeline() -> String {
    r#"
pipeline:
  name: envelope_concat
nodes:
  - type: source
    name: edi
    config:
      name: edi
      type: csv
      path: placeholder.csv
      envelope:
        sections:
          interchange:
            extract: { record_type: H }
            fields:
              tag: string
      schema:
        - { name: id, type: int }
  - type: transform
    name: tag
    input: edi
    config:
      cxl: |
        emit id = id
  - type: envelope
    name: framed
    body: tag
    config: { strategy: concat }
  - type: transform
    name: stamp
    input: framed
    config:
      cxl: |
        emit id = id
        emit doc_tag = $doc.interchange.tag
  - type: output
    name: out
    input: stamp
    config:
      name: out
      type: csv
      path: out.csv
      reconstruct_envelope: true
      options:
        envelope:
          footer_from_doc: interchange
"#
    .to_string()
}

/// Run a concat pipeline over a scripted step list, returning the run result so
/// the caller can assert either the written CSV or the error (the conflict
/// test). The `Ok` payload is the written CSV bytes.
fn run_concat(steps: Vec<Step>) -> Result<String, clinker_plan::error::PipelineError> {
    let mut config =
        clinker_plan::config::parse_config(&concat_pipeline()).expect("parse concat pipeline");
    for spanned in &mut config.nodes {
        if let clinker_plan::config::PipelineNode::Source { config: body, .. } = &mut spanned.value
        {
            body.source.path = None;
        }
    }
    let plan = config
        .compile(&clinker_plan::config::CompileContext::default())
        .expect("compile concat pipeline");

    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "edi".to_string(),
        SourceInput::Records(Box::new(ScriptedReader::new(steps))),
    )]);
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
            execution_id: "concat-exec".to_string(),
            batch_id: "concat-batch".to_string(),
            ..Default::default()
        },
    )?;
    Ok(buf.as_string())
}

/// One `NewFrame` document carrying `id` rows tagged with `tag_val`.
fn concat_doc(file: &'static str, tag_val: &'static str, ids: &[i64]) -> Vec<Step> {
    let mut steps = vec![Step::Open {
        file,
        name: "interchange",
        tag_val,
        frame: FrameRole::NewFrame,
    }];
    for &id in ids {
        steps.push(Step::Record { file, id });
    }
    steps.push(Step::Close { file });
    steps
}

/// One `NewFrame` document whose header carries `tag_val` AND an engine-injected
/// `$raw` value, plus `id` body rows. Two such documents with the same `tag_val`
/// but different `raw` model two files from one trading partner that differ only
/// in an interchange control number stashed in the preserved raw bytes.
fn concat_doc_with_raw(
    file: &'static str,
    tag_val: &'static str,
    raw: &'static str,
    ids: &[i64],
) -> Vec<Step> {
    let mut steps = vec![Step::OpenWithRaw {
        file,
        name: "interchange",
        tag_val,
        raw,
        frame: FrameRole::NewFrame,
    }];
    for &id in ids {
        steps.push(Step::Record { file, id });
    }
    steps.push(Step::Close { file });
    steps
}

/// One genuinely headerless `NewFrame` document carrying `id` rows.
fn concat_headerless_doc(file: &'static str, ids: &[i64]) -> Vec<Step> {
    let mut steps = vec![Step::OpenHeaderless {
        file,
        frame: FrameRole::NewFrame,
    }];
    for &id in ids {
        steps.push(Step::Record { file, id });
    }
    steps.push(Step::Close { file });
    steps
}

/// One nested X12-style interchange: ISA → GS → ST, three `Inherit` levels each
/// carrying a DISTINCT `interchange` section value, then `id` body rows, then
/// three closes. Every level inherits the interchange grain, so the whole
/// `ISA..IEA` is ONE body grain — one output document — whose body records carry
/// the innermost fully-merged envelope (`tag` = the ST value, innermost wins).
///
/// Ingest mints one `DocumentOpen` per level, so this single interchange emits
/// three cumulative non-empty open-envelopes — the shape that wrongly tripped
/// the multi-header guard when it counted per-level opens instead of body grains.
fn nested_x12_interchange_doc(file: &'static str, ids: &[i64]) -> Vec<Step> {
    let mut steps = vec![
        Step::Open {
            file,
            name: "interchange",
            tag_val: "ISA-LEVEL",
            frame: FrameRole::Inherit,
        },
        Step::Open {
            file,
            name: "interchange",
            tag_val: "GS-LEVEL",
            frame: FrameRole::Inherit,
        },
        Step::Open {
            file,
            name: "interchange",
            tag_val: "ST-LEVEL",
            frame: FrameRole::Inherit,
        },
    ];
    for &id in ids {
        steps.push(Step::Record { file, id });
    }
    steps.push(Step::Close { file }); // ST
    steps.push(Step::Close { file }); // GS
    steps.push(Step::Close { file }); // ISA
    steps
}

#[test]
fn concat_carries_a_common_non_empty_header() {
    // Two documents with IDENTICAL non-empty headers (`tag = COMMON`). Concat
    // must NOT flag a conflict — two equal headers fold to one common header —
    // and the consolidated document must carry it: the post-envelope Transform
    // resolves `$doc.interchange.tag` to `COMMON` on every re-stamped body row.
    let mut steps = concat_doc("a.x12", "COMMON", &[1, 2]);
    steps.extend(concat_doc("b.x12", "COMMON", &[3]));

    let csv = run_concat(steps).expect("identical headers fold without conflict");
    let (frames, counts) = footer_stats(&csv);
    assert_eq!(
        frames, 1,
        "identical headers → one consolidated document: {csv}"
    );
    assert_eq!(
        counts,
        vec![3],
        "all three body rows under the one document: {csv}"
    );

    // The common header is readable post-concat: every body row's `doc_tag`
    // column (emitted from `$doc.interchange.tag` AFTER the Envelope) is the
    // common value. A re-stamp that lost the header would yield empty cells.
    let body_rows: Vec<&str> = csv
        .lines()
        .filter(|l| {
            let c: Vec<&str> = l.split(',').collect();
            c.len() == 2 && c[0].parse::<i64>().is_ok()
        })
        .collect();
    assert_eq!(body_rows.len(), 3, "three body rows present: {csv}");
    for row in &body_rows {
        let cells: Vec<&str> = row.split(',').collect();
        assert_eq!(
            cells[1], "COMMON",
            "$doc.interchange.tag resolves to the consolidated common header: {row}"
        );
    }
}

#[test]
fn concat_headed_plus_headerless_carries_the_single_header() {
    // One headed document (`tag = ONLY`) and one genuinely headerless document
    // (a `NewFrame` carrying no sections). The single real header wins; the
    // headerless document coexists without conflict, and the consolidated
    // document carries `ONLY` — readable as `$doc.interchange.tag` on a body row.
    let mut steps = concat_doc("a.x12", "ONLY", &[1]);
    steps.extend(concat_headerless_doc("b.x12", &[2]));

    let csv = run_concat(steps).expect("headed + headerless folds without conflict");
    let (frames, counts) = footer_stats(&csv);
    assert_eq!(
        frames, 1,
        "headed + headerless → one consolidated document: {csv}"
    );
    assert_eq!(
        counts,
        vec![2],
        "both body rows under the one consolidated document: {csv}"
    );
    // The headed document's value is the consolidated header, carried onto every
    // re-stamped body row by the post-envelope `$doc.interchange.tag` read.
    let doc_tags: Vec<&str> = csv
        .lines()
        .filter_map(|l| {
            let c: Vec<&str> = l.split(',').collect();
            (c.len() == 2 && c[0].parse::<i64>().is_ok()).then(|| c[1])
        })
        .collect();
    assert_eq!(
        doc_tags,
        vec!["ONLY", "ONLY"],
        "the single real header is carried onto the consolidated document: {csv}"
    );
}

#[test]
fn concat_two_distinct_headers_is_e350_conflict() {
    // Two documents with DISTINCT non-empty headers (`ISA-A` vs `ISA-B`). One
    // consolidated document cannot frame both, so concat aborts with E350 rather
    // than silently dropping a header. Pin the code — accepting any error would
    // mask a regression that fired the wrong diagnostic.
    let mut steps = concat_doc("a.x12", "ISA-A", &[1]);
    steps.extend(concat_doc("b.x12", "ISA-B", &[2]));

    let err = run_concat(steps).expect_err("distinct headers must abort with E350");
    let msg = err.to_string();
    assert!(
        msg.contains("E350"),
        "the multi-header conflict pins diagnostic code E350, got: {msg}"
    );
    // Destructure rather than `matches!`: the reported `header_count` is the
    // load-bearing payload (it names how many distinct headers clashed), so a
    // bug that fired the variant with the wrong count must fail here.
    match err {
        clinker_plan::error::PipelineError::EnvelopeMultiHeaderConflict {
            header_count, ..
        } => {
            assert_eq!(
                header_count, 2,
                "two distinct headers (ISA-A, ISA-B) → header_count == 2"
            );
        }
        other => panic!("expected the multi-header conflict variant, got: {other:?}"),
    }
}

#[test]
fn concat_headers_differing_only_in_engine_raw_fold_without_conflict() {
    // Two documents whose `interchange` headers agree on every user field
    // (`tag = PARTNER`) but differ in the engine-preserved `$raw` bytes — the
    // X12 interchange-control-number footgun. A structural compare would split
    // them and raise E350; folding by header identity (ignoring the `$`-prefixed
    // engine key) consolidates them into ONE document with no conflict. This
    // would have tripped E350 before headers were compared by identity.
    let mut steps = concat_doc_with_raw("a.x12", "PARTNER", "ISA*...*000000001", &[1, 2]);
    steps.extend(concat_doc_with_raw(
        "b.x12",
        "PARTNER",
        "ISA*...*000000002",
        &[3],
    ));

    let csv =
        run_concat(steps).expect("headers differing only in engine $raw must fold, not conflict");

    // The footer-from-doc renders BOTH the user `tag` field and the engine
    // `$raw` field, so this section's footer is a two-cell row (`tag,$raw`) —
    // unlike the single-field sections `footer_stats` is tuned for. Count the
    // footer rows directly: the body rows are `id,PARTNER` (numeric first cell),
    // the lone footer is `PARTNER,<raw>` (non-numeric first cell).
    let body_rows: Vec<&str> = csv
        .lines()
        .filter(|l| {
            let c: Vec<&str> = l.split(',').collect();
            c.first().is_some_and(|first| first.parse::<i64>().is_ok())
        })
        .collect();
    let footer_rows: Vec<&str> = csv
        .lines()
        .filter(|l| {
            let c: Vec<&str> = l.split(',').collect();
            c.len() == 2 && c[0] == "PARTNER"
        })
        .collect();
    assert_eq!(
        footer_rows.len(),
        1,
        "headers differing only in $raw fold to ONE consolidated document (one footer): {csv}"
    );
    assert_eq!(
        body_rows.len(),
        3,
        "all three body rows land under the one consolidated document: {csv}"
    );
    // The consolidated document keeps the FIRST document's full header, so its
    // footer carries the first document's `$raw` (000000001), never the second's.
    assert_eq!(
        footer_rows[0], "PARTNER,ISA*...*000000001",
        "the consolidated header is the FIRST document's full EnvelopeRecord, $raw included: {csv}"
    );
}

#[test]
fn concat_nested_x12_interchange_is_one_header_not_per_level_conflict() {
    // A single nested X12-style interchange: ISA → GS → ST, three `Inherit`
    // levels. Ingest mints one `DocumentOpen` per level, so this ONE interchange
    // emits three distinct cumulative open-envelopes — but it is one body grain,
    // one output document, with one header (the innermost, fully-merged
    // envelope). Deriving the header set from per-level opens would count three
    // and fire E350 on a perfectly valid single interchange; deriving it from
    // the body grains counts one and succeeds. This is the regression guard.
    let steps = nested_x12_interchange_doc("claim.x12", &[10, 11]);

    let csv =
        run_concat(steps).expect("a single nested interchange must not trip the header guard");
    let (frames, counts) = footer_stats(&csv);
    assert_eq!(
        frames, 1,
        "nested ISA/GS/ST is ONE consolidated document, not a multi-header conflict: {csv}"
    );
    assert_eq!(
        counts,
        vec![2],
        "both body rows under the one consolidated interchange: {csv}"
    );

    // The carried header resolves on every body row: `$doc.interchange.tag`
    // (emitted AFTER the Envelope) is the innermost level's value — innermost
    // shadows ancestor on the merged envelope.
    let doc_tags: Vec<&str> = csv
        .lines()
        .filter_map(|l| {
            let c: Vec<&str> = l.split(',').collect();
            (c.len() == 2 && c[0].parse::<i64>().is_ok()).then(|| c[1])
        })
        .collect();
    assert_eq!(
        doc_tags,
        vec!["ST-LEVEL", "ST-LEVEL"],
        "the consolidated header is the innermost fully-merged envelope: {csv}"
    );
}

#[test]
fn concat_three_documents_same_header_is_one_document_no_conflict() {
    // Three documents, all with the SAME non-empty header (`tag = SHARED`).
    // Three equal headers fold to one common header — concat must NOT flag a
    // conflict and must frame exactly one consolidated document. Guards the
    // dedup against a false-positive (e.g. counting documents instead of
    // distinct headers).
    let mut steps = concat_doc("a.x12", "SHARED", &[1]);
    steps.extend(concat_doc("b.x12", "SHARED", &[2, 3]));
    steps.extend(concat_doc("c.x12", "SHARED", &[4]));

    let csv = run_concat(steps).expect("three identical headers fold without conflict");
    let (frames, counts) = footer_stats(&csv);
    assert_eq!(
        frames, 1,
        "three identical headers → one consolidated document: {csv}"
    );
    assert_eq!(
        counts,
        vec![4],
        "all four body rows under the one document: {csv}"
    );
    let doc_tags: Vec<&str> = csv
        .lines()
        .filter_map(|l| {
            let c: Vec<&str> = l.split(',').collect();
            (c.len() == 2 && c[0].parse::<i64>().is_ok()).then(|| c[1])
        })
        .collect();
    assert_eq!(
        doc_tags,
        vec!["SHARED", "SHARED", "SHARED", "SHARED"],
        "the single common header is carried onto every re-stamped body row: {csv}"
    );
}

#[test]
fn concat_single_document_carries_its_header() {
    // One document in → one consolidated document out, header carried. The
    // degenerate case: concat over a single document is a no-op on framing
    // (still one document) and must not lose the header.
    let steps = concat_doc("solo.x12", "SOLO", &[7, 8, 9]);

    let csv = run_concat(steps).expect("single-document concat succeeds");
    let (frames, counts) = footer_stats(&csv);
    assert_eq!(frames, 1, "one document in → one document out: {csv}");
    assert_eq!(counts, vec![3], "all three body rows framed once: {csv}");
    let doc_tags: Vec<&str> = csv
        .lines()
        .filter_map(|l| {
            let c: Vec<&str> = l.split(',').collect();
            (c.len() == 2 && c[0].parse::<i64>().is_ok()).then(|| c[1])
        })
        .collect();
    assert_eq!(
        doc_tags,
        vec!["SOLO", "SOLO", "SOLO"],
        "the lone document's header is carried through concat: {csv}"
    );
}

#[test]
fn concat_empty_body_emits_no_document() {
    // An empty body (no records, no envelope events) frames nothing — concat
    // opens no document, so the writer renders only its column header and no
    // body or footer rows.
    let csv = run_concat(Vec::new()).expect("empty body succeeds");
    let (frames, counts) = footer_stats(&csv);
    assert_eq!(frames, 0, "an empty body emits no framed document: {csv:?}");
    assert!(
        counts.is_empty(),
        "no per-document body counts for an empty body: {csv:?}"
    );
}

// ─── Concat provenance: per-record `$source.file` survives consolidation ─────

/// A concat pipeline that emits `$source.file` as an output column, to prove the
/// re-stamp does not clobber it. The body Transform carries `$source.file`
/// through; the Envelope consolidates; the post-envelope Output writes it. Unlike
/// `concat_pipeline`, this does NOT reconstruct an envelope footer — the
/// assertion is purely on the per-record tail column.
fn concat_provenance_pipeline() -> String {
    r#"
pipeline:
  name: envelope_concat_provenance
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
        emit src_file = $source.file
  - type: envelope
    name: framed
    body: tag
    config: { strategy: concat }
  - type: output
    name: out
    input: framed
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#
    .to_string()
}

/// Run the provenance pipeline over a scripted step list, returning the written
/// CSV bytes.
fn run_concat_provenance(steps: Vec<Step>) -> String {
    let mut config = clinker_plan::config::parse_config(&concat_provenance_pipeline())
        .expect("parse concat provenance pipeline");
    for spanned in &mut config.nodes {
        if let clinker_plan::config::PipelineNode::Source { config: body, .. } = &mut spanned.value
        {
            body.source.path = None;
        }
    }
    let plan = config
        .compile(&clinker_plan::config::CompileContext::default())
        .expect("compile concat provenance pipeline");

    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "edi".to_string(),
        SourceInput::Records(Box::new(ScriptedReader::new(steps))),
    )]);
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
            execution_id: "concat-prov-exec".to_string(),
            batch_id: "concat-prov-batch".to_string(),
            ..Default::default()
        },
    )
    .expect("drive concat provenance pipeline");
    buf.as_string()
}

#[test]
fn concat_preserves_per_record_source_file() {
    // Two documents from DIFFERENT source files but the SAME header (so no
    // header conflict — the focus is provenance, not the guard). Concat re-stamps
    // the document context onto one consolidated grain, but `$source.file` is a
    // real tail column stamped at ingest — the re-stamp must not touch it. Each
    // record must still carry its ORIGINAL source file after consolidation.
    let mut steps = concat_doc("alpha.x12", "SHARED", &[1, 2]);
    steps.extend(concat_doc("beta.x12", "SHARED", &[3]));

    let csv = run_concat_provenance(steps);
    let lines: Vec<&str> = csv.lines().collect();
    // Header `id,src_file` then three body rows in ingest order.
    let header = lines.first().expect("output has a header line");
    assert!(
        header.starts_with("id,src_file"),
        "expected an id,src_file header, got: {csv}"
    );
    let body: Vec<(&str, &str)> = lines[1..]
        .iter()
        .filter_map(|l| {
            let c: Vec<&str> = l.split(',').collect();
            (c.len() == 2 && c[0].parse::<i64>().is_ok()).then(|| (c[0], c[1]))
        })
        .collect();
    assert_eq!(
        body,
        vec![("1", "alpha.x12"), ("2", "alpha.x12"), ("3", "beta.x12")],
        "each record keeps its original `$source.file` across consolidation: {csv}"
    );
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

    let with = run_aggregate(Some("preserve"), &files);
    assert_eq!(
        with,
        vec!["x,2".to_string(), "x,3".to_string(), "y,1".to_string()],
        "preserve Envelope must forward per-document closes so the downstream \
         Aggregate flushes per document (x=2 doc A, x=3 doc B), not a merged x=5",
    );

    // And it matches the no-Envelope control exactly — the node changes nothing
    // about the document boundaries the Aggregate observes.
    let without = run_aggregate(None, &files);
    assert_eq!(
        with, without,
        "per-document aggregation is identical with and without the preserve Envelope",
    );
}

#[test]
fn concat_envelope_merges_all_files_into_one_aggregate_flush() {
    // The headerless-document framing probe, observed through a downstream
    // Aggregate. Each CSV file is a headerless document (the source declares no
    // `envelope:`). `concat` collapses both files onto ONE consolidated
    // document, so the Aggregate flushes ONCE and category `x` merges across
    // files into a single x=5 (2 from A + 3 from B) — versus the per-file x=2 /
    // x=3 the `preserve` control produces. This is the framing-collapse
    // acceptance: two documents in, one framed document out.
    let files = [
        ("a.csv", "category\nx\nx\ny\n"),
        ("b.csv", "category\nx\nx\nx\n"),
    ];

    let concat = run_aggregate(Some("concat"), &files);
    assert_eq!(
        concat,
        vec!["x,5".to_string(), "y,1".to_string()],
        "concat collapses both files into one document, so the Aggregate flushes \
         once and category x merges cross-file to 5 (not per-file 2 and 3)",
    );

    // The preserve control keeps the files as separate documents — the
    // differential that proves concat actually consolidated the framing.
    let preserve = run_aggregate(Some("preserve"), &files);
    assert_eq!(
        preserve,
        vec!["x,2".to_string(), "x,3".to_string(), "y,1".to_string()],
        "preserve keeps per-file documents, so x stays split 2 / 3",
    );
    assert_ne!(
        concat, preserve,
        "concat and preserve must differ — concat merged the document framing",
    );
}

// ─── Wired `header:` port (transform-in-place header replacement) ────────
//
// A wired `header:` port replaces each body grain's ambient envelope with the
// matching header record, attached by grain. These tests cover the plan-level
// wiring (the header input resolves to its predecessor), the end-to-end error
// path (a header grounded to no body grain aborts the whole run with E351), and
// the still-rejected `trailer:` port. The grain-matched replacement itself is
// proved by the `replace_headers_by_grain` unit tests in `envelope_dispatch`,
// which control grains directly — two independent sources cannot share a grain.

/// A two-source pipeline: a body source and a separate header source, each
/// transformed and wired into an Envelope node as `body:` / `header:`.
fn header_port_pipeline() -> String {
    r#"
pipeline:
  name: envelope_header_port
nodes:
  - type: source
    name: body_src
    config:
      name: body_src
      type: csv
      path: body.csv
      schema:
        - { name: id, type: int }
  - type: source
    name: hdr_src
    config:
      name: hdr_src
      type: csv
      path: hdr.csv
      schema:
        - { name: id, type: int }
  - type: transform
    name: body
    input: body_src
    config:
      cxl: "emit id = id"
  - type: transform
    name: hdr
    input: hdr_src
    config:
      cxl: "emit id = id"
  - type: envelope
    name: framed
    body: body
    header: hdr
    config: { strategy: preserve }
  - type: output
    name: out
    input: framed
    config:
      name: out
      type: csv
      path: out.csv
"#
    .to_string()
}

#[test]
fn wired_header_resolves_to_its_predecessor() {
    // The Envelope's `header_input` lowers to the producer name `hdr`, and the
    // resolution post-pass stamps `header_upstream` with that producer's
    // NodeIndex — distinct from the body predecessor. Without resolution the
    // executor cannot tell the two predecessors apart.
    use clinker_plan::plan::execution::PlanNode;

    let config = clinker_plan::config::parse_config(&header_port_pipeline())
        .expect("a wired header port parses and validates");
    let compiled = config
        .compile(&clinker_plan::config::CompileContext::default())
        .expect("a wired header port compiles");
    let graph = &compiled.dag().graph;

    let envelope_idx = graph
        .node_indices()
        .find(|&i| matches!(&graph[i], PlanNode::Envelope { .. }))
        .expect("the plan has an Envelope node");

    let PlanNode::Envelope {
        header_input,
        header_upstream,
        ..
    } = &graph[envelope_idx]
    else {
        unreachable!("located by the Envelope match above");
    };
    assert_eq!(
        header_input, "hdr",
        "the wired header producer name lowers onto the plan node"
    );
    let resolved = header_upstream.expect("the wired header resolves to a predecessor NodeIndex");
    assert_eq!(
        graph[resolved].name(),
        "hdr",
        "header_upstream points at the header producer, not the body"
    );

    // The header predecessor is genuinely one of the two incoming neighbors,
    // and the OTHER one is the body — the discriminator the executor relies on.
    let preds: Vec<_> = graph
        .neighbors_directed(envelope_idx, petgraph::Direction::Incoming)
        .collect();
    assert_eq!(preds.len(), 2, "a wired header gives the Envelope 2 inputs");
    assert!(
        preds.contains(&resolved),
        "the resolved header predecessor is an incoming neighbor"
    );
    let body_pred = preds
        .iter()
        .find(|&&p| p != resolved)
        .expect("a distinct body predecessor remains");
    assert_eq!(
        graph[*body_pred].name(),
        "body",
        "the non-header predecessor is the body producer"
    );
}

#[test]
fn wired_header_grain_unmatched_to_body_is_e351_end_to_end() {
    // Two independent sources mint independent document grains, so the header
    // records ground to grains the body never carries. Driven through the full
    // executor, the run aborts with E351 — proving the wired header is drained
    // and attach-by-grain runs end to end, not just in isolation.
    let mut config =
        clinker_plan::config::parse_config(&header_port_pipeline()).expect("parse header pipeline");
    for spanned in &mut config.nodes {
        if let clinker_plan::config::PipelineNode::Source { config: body, .. } = &mut spanned.value
        {
            body.source.path = None;
        }
    }
    let plan = config
        .compile(&clinker_plan::config::CompileContext::default())
        .expect("compile header pipeline");

    // One body document (file b.csv) and one header document (file h.csv).
    let body_steps = vec![
        Step::Open {
            file: "b.csv",
            name: "interchange",
            tag_val: "BODY",
            frame: FrameRole::NewFrame,
        },
        Step::Record {
            file: "b.csv",
            id: 1,
        },
        Step::Close { file: "b.csv" },
    ];
    let hdr_steps = vec![
        Step::Open {
            file: "h.csv",
            name: "interchange",
            tag_val: "HEADER",
            frame: FrameRole::NewFrame,
        },
        Step::Record {
            file: "h.csv",
            id: 9,
        },
        Step::Close { file: "h.csv" },
    ];

    let readers: clinker_exec::executor::SourceReaders = HashMap::from([
        (
            "body_src".to_string(),
            SourceInput::Records(Box::new(ScriptedReader::new(body_steps))),
        ),
        (
            "hdr_src".to_string(),
            SourceInput::Records(Box::new(ScriptedReader::new(hdr_steps))),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let err = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &PipelineRunParams {
            execution_id: "hdr-exec".to_string(),
            batch_id: "hdr-batch".to_string(),
            ..Default::default()
        },
    )
    .expect_err("a header grounded to no body grain must abort the run");

    let msg = err.to_string();
    assert!(
        msg.contains("E351"),
        "the unmatched-grain abort pins E351 end to end, got: {msg}"
    );
    match err {
        clinker_plan::error::PipelineError::EnvelopeHeaderGrainUnmatched { envelope, .. } => {
            assert_eq!(envelope, "framed", "the diagnostic names the Envelope node");
        }
        other => panic!("expected EnvelopeHeaderGrainUnmatched, got: {other:?}"),
    }
}

#[test]
fn wired_trailer_is_rejected_at_validation() {
    // `trailer:` has no draining path yet, so a wired value is still a config
    // error — narrowed from the prior header+trailer rejection to trailer only.
    let yaml = r#"
pipeline:
  name: envelope_trailer
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: int }
  - type: transform
    name: body
    input: src
    config:
      cxl: "emit id = id"
  - type: transform
    name: tail
    input: src
    config:
      cxl: "emit id = id"
  - type: envelope
    name: framed
    body: body
    trailer: tail
    config: { strategy: preserve }
  - type: output
    name: out
    input: framed
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let err = clinker_plan::config::parse_config(yaml)
        .expect_err("a wired trailer port is still rejected at validation");
    let msg = err.to_string();
    assert!(
        msg.contains("envelope node 'framed'")
            && msg.contains("`trailer`")
            && msg.contains("not yet supported"),
        "the rejection still names the trailer port specifically, got: {msg}"
    );
}
