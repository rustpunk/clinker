//! Source ingest: format-reader construction and the per-source driver
//! thread body that widens, stamps, and pushes records into the dispatch
//! channel.

use std::io::Read;
use std::sync::Arc;

use clinker_format::ReopenableSource;
use clinker_format::csv::reader::{CsvReader, CsvReaderConfig};
use clinker_format::edifact::reader::{EdifactReader, EdifactReaderConfig};
use clinker_format::fixed_width::reader::{FixedWidthReader, FixedWidthReaderConfig};
use clinker_format::hl7::Hl7FieldSplit;
use clinker_format::hl7::reader::{Hl7Reader, Hl7ReaderConfig};
use clinker_format::json::reader::{
    ArrayPathMode, ArrayPathSpec, JsonMode, JsonReader, JsonReaderConfig,
};
use clinker_format::swift::reader::{SwiftReader, SwiftReaderConfig};
use clinker_format::traits::FormatReader;
use clinker_format::x12::Charset;
use clinker_format::x12::reader::{X12Reader, X12ReaderConfig};
use clinker_format::xml::reader::{
    NamespaceMode, XmlArrayMode, XmlArrayPath, XmlReader, XmlReaderConfig,
};
use clinker_plan::config::PipelineConfig;
use clinker_plan::error::PipelineError;

/// Build a format-specific reader from input config and a re-openable source.
///
/// Dispatches on `InputFormat` to construct the correct reader type. The JSON
/// and XML arms thread the [`ReopenableSource`] straight through so their
/// envelope pre-scan and body stream each open a fresh `Read` without buffering
/// the whole file; every other (one-pass) format opens the source once and
/// streams that single `Read`. Returns `Box<dyn FormatReader>` — all downstream
/// code uses trait methods (`schema()`, `next_record()`).
fn build_format_reader(
    input: &clinker_plan::config::SourceConfig,
    source: ReopenableSource,
) -> Result<Box<dyn FormatReader>, PipelineError> {
    match &input.format {
        clinker_plan::config::InputFormat::Csv(opts) => {
            let config = build_csv_reader_config(opts.as_ref());
            Ok(Box::new(CsvReader::from_reader(
                open_one_shot(&source)?,
                config,
            )))
        }
        clinker_plan::config::InputFormat::Json(opts) => {
            let config = build_json_reader_config(
                opts.as_ref(),
                input.array_paths.as_deref(),
                &input.declared_doc_paths,
            );
            Ok(Box::new(JsonReader::from_source(source, config)?))
        }
        clinker_plan::config::InputFormat::Xml(opts) => {
            let config = build_xml_reader_config(
                opts.as_ref(),
                input.array_paths.as_deref(),
                &input.declared_doc_paths,
            );
            Ok(Box::new(XmlReader::from_source(source, config)?))
        }
        clinker_plan::config::InputFormat::FixedWidth(opts) => {
            let fields = extract_field_defs(input)?;
            let config = build_fw_reader_config(opts.as_ref());
            Ok(Box::new(FixedWidthReader::new(
                open_one_shot(&source)?,
                fields,
                config,
            )?))
        }
        clinker_plan::config::InputFormat::Edifact(opts) => {
            let config = build_edifact_reader_config(opts.as_ref());
            Ok(Box::new(EdifactReader::new(
                open_one_shot(&source)?,
                config,
            )))
        }
        clinker_plan::config::InputFormat::X12(opts) => {
            let config = build_x12_reader_config(opts.as_ref())?;
            Ok(Box::new(X12Reader::new(open_one_shot(&source)?, config)))
        }
        clinker_plan::config::InputFormat::Hl7(opts) => {
            let config = build_hl7_reader_config(opts.as_ref());
            Ok(Box::new(Hl7Reader::new(open_one_shot(&source)?, config)))
        }
        clinker_plan::config::InputFormat::Swift(opts) => {
            let config = build_swift_reader_config(opts.as_ref());
            Ok(Box::new(SwiftReader::new(open_one_shot(&source)?, config)))
        }
    }
}

/// Open a single `Read` from a re-openable source for a one-pass format reader,
/// mapping an open failure to a pipeline config-validation error.
fn open_one_shot(source: &ReopenableSource) -> Result<Box<dyn Read + Send>, PipelineError> {
    source.open().map_err(|e| {
        PipelineError::Config(clinker_plan::config::ConfigError::Validation(format!(
            "failed to open source bytes: {e}"
        )))
    })
}

/// Build a [`MultiFileFormatReader`] wrapping every file the discovery
/// layer returned for this source. Single-file sources go through the
/// same path with a one-element slot list — the wrapper short-circuits
/// transparently.
fn build_multi_file_reader(
    input: &clinker_plan::config::SourceConfig,
    files: Vec<crate::source::multi_file::FileSlot>,
) -> Result<Box<dyn FormatReader>, PipelineError> {
    use crate::source::multi_file::{FactoryFn, MultiFileFormatReader};

    // The factory closure captures the source config by clone so each
    // file gets a fresh format reader configured identically. Format
    // construction errors map to the wrapper's `Schema` variant via
    // `clinker_format::FormatError` so they bubble through the trait
    // boundary intact.
    let owned_config = input.clone();
    let factory: Box<FactoryFn> = Box::new(
        move |source: ReopenableSource,
              _idx: usize|
              -> Result<Box<dyn FormatReader>, clinker_format::FormatError> {
            build_format_reader(&owned_config, source).map_err(|e| {
                clinker_format::FormatError::SchemaInference(format!(
                    "format reader construction failed: {e}"
                ))
            })
        },
    );
    Ok(Box::new(MultiFileFormatReader::new(files, factory)))
}

/// Wrap a format reader with schema-based type coercion if the source
/// declares typed columns in its `schema:` block.
fn wrap_with_schema_coercion(
    reader: Box<dyn FormatReader>,
    config: &PipelineConfig,
    source_name: &str,
) -> Result<Box<dyn FormatReader>, PipelineError> {
    use clinker_plan::config::PipelineNode;
    use clinker_plan::config::pipeline_node::OnUnmapped;

    // Find the source node's schema declaration + on_unmapped policy
    // + format. Format is needed for the auto_widen-on-fixed-width
    // structural-inertness diagnostic below.
    let body_data = config.nodes.iter().find_map(|s| {
        if let PipelineNode::Source {
            header,
            config: body,
        } = &s.value
            && header.name == source_name
        {
            return Some((
                &body.schema.columns,
                body.on_unmapped.clone(),
                body.source.format.clone(),
            ));
        }
        None
    });

    match body_data {
        Some((columns, policy, format)) if !columns.is_empty() => {
            // Fixed-width format: the reader's schema is constructed
            // positionally from the user-declared `FieldDef` list,
            // so the reader cannot ever produce an "undeclared
            // field" — every byte that isn't covered by a FieldDef
            // is structurally invisible. `auto_widen`'s sidecar
            // therefore stays Null forever for fixed-width sources.
            // Surface the inertness once per source at compile time
            // so a user who set `auto_widen` (the engine-wide
            // default) on a fixed-width source sees the no-op
            // explicitly and can either pick `drop`/`reject` (the
            // honest scalar policies for fixed-width) or accept the
            // empty sidecar.
            if matches!(policy, OnUnmapped::AutoWiden)
                && matches!(format, clinker_plan::config::InputFormat::FixedWidth(_))
            {
                tracing::info!(
                    source = source_name,
                    "on_unmapped: auto_widen is structurally inert for fixed_width sources — \
                     the schema is positional, so the reader cannot detect undeclared \
                     fields. The `$widened` sidecar slot will always carry Value::Null. \
                     Set `on_unmapped: drop` or `reject` to make the policy explicit."
                );
            }
            let coercing = crate::pipeline::schema_coerce::CoercingReader::new(
                reader,
                columns,
                policy,
                source_name,
            )
            .map_err(|e| PipelineError::Compilation {
                transform_name: source_name.to_string(),
                messages: vec![format!("schema coercion init error: {e}")],
            })?;
            Ok(Box::new(coercing))
        }
        _ => Ok(reader),
    }
}

/// Convert a record value at a declared watermark column into the
/// canonical i64-nanoseconds event-time stamp folded into
/// [`crate::executor::watermark::PerSourceWatermarks`].
///
/// Accepts [`clinker_record::Value::DateTime`] (preserved at nano
/// resolution via `and_utc().timestamp_nanos_opt()`) and
/// [`clinker_record::Value::Date`] (anchored at 00:00:00 UTC). Null,
/// non-temporal, or out-of-`i64`-nanos-range values return `None`
/// and are silently skipped — late-record dropping is the time-window
/// operator's job.
fn value_to_event_time_nanos(value: &clinker_record::Value) -> Option<i64> {
    use clinker_record::Value;
    match value {
        Value::DateTime(nd) => nd.and_utc().timestamp_nanos_opt(),
        Value::Date(d) => d
            .and_hms_opt(0, 0, 0)
            .and_then(|nd| nd.and_utc().timestamp_nanos_opt()),
        _ => None,
    }
}

/// One ingest thread's outcome, collected by the executor entry once
/// dispatch has drained the paired receiver. The watermark
/// observations are folded back into the executor's owned
/// `PerSourceWatermarks` (per-(source, file) max-event-time map);
/// `total_count` increments `counters.total_count` and seeds the
/// `$source.count` pipeline-wide total.
pub(super) struct IngestTaskOutcome {
    pub(super) source_name: String,
    pub(super) total_count: u64,
    pub(super) watermark_observations: Vec<(Arc<str>, i64)>,
}

/// Ingest a Source's records into a bounded
/// [`SourceIngestChannel`](crate::executor::source_stream::SourceIngestChannel)
/// so the dispatch loop drains the paired crossbeam `Receiver` via
/// `recv`. Used uniformly by every declared Source — no primary
/// asymmetry.
///
/// Each ingested record is widened with two tail engine-stamped
/// columns:
///
/// - `$source.file` (`FieldMetadata::SourceFile`) — per-record
///   originating file `Arc<str>` from
///   `MultiFileFormatReader::current_source_file()`.
/// - `$source.name` (`FieldMetadata::SourceName`) — per-record
///   originating Source-node name as a shared `Arc<str>`. One Arc per
///   Source, cloned by every record from that Source — the runtime
///   cost is one Arc bump per ingested row.
///
/// Stamping at ingest lets `$source.file` / `$source.path` /
/// `$source.name` resolution survive Merge / Combine downstream
/// without external row-keyed arrays. `counters.total_count`
/// increments per record so every source contributes to the
/// pipeline-wide total.
pub(super) fn ingest_source(
    src_cfg: clinker_plan::config::SourceConfig,
    input: crate::source::SourceInput,
    config: PipelineConfig,
    stream: crate::executor::source_stream::SourceIngestChannel,
    shutdown_token: Option<crate::pipeline::shutdown::ShutdownToken>,
) -> Result<IngestTaskOutcome, PipelineError> {
    // Branch once on transport. The file arm builds the
    // MultiFileFormatReader + schema-coercion stack and hands the
    // resulting `Box<dyn FormatReader>` to the shared driver as a
    // `RecordSource`; the records arm hands its already-built row
    // yielder straight through. Everything downstream of the source
    // reader — schema widening, document boundaries, watermark
    // observation, channel push — is identical, so it lives in
    // `drive_record_source`.
    match input {
        crate::source::SourceInput::Files(files) => {
            if files.is_empty() {
                return Err(PipelineError::Config(
                    clinker_plan::config::ConfigError::Validation(format!(
                        "source '{}' has empty file list",
                        src_cfg.name
                    )),
                ));
            }
            let raw_reader = build_multi_file_reader(&src_cfg, files)?;
            let src_reader = wrap_with_schema_coercion(raw_reader, &config, &src_cfg.name)?;
            // The file arm reaches the shared driver through the blanket
            // `RecordSource for Box<dyn FormatReader>` impl.
            drive_record_source(src_cfg, Box::new(src_reader), stream, shutdown_token)
        }
        crate::source::SourceInput::Records(src_reader) => {
            drive_record_source(src_cfg, src_reader, stream, shutdown_token)
        }
    }
}

/// Drive one Source's [`RecordSource`](crate::source::RecordSource) to
/// completion, widening every record with the `$source.*` engine-stamped
/// tail columns, emitting document-boundary punctuation, observing
/// event-time watermarks, and pushing into `stream`. Shared by both the
/// file and non-file ingest arms — see [`ingest_source`].
fn drive_record_source(
    src_cfg: clinker_plan::config::SourceConfig,
    src_reader: Box<dyn crate::source::RecordSource>,
    stream: crate::executor::source_stream::SourceIngestChannel,
    shutdown_token: Option<crate::pipeline::shutdown::ShutdownToken>,
) -> Result<IngestTaskOutcome, PipelineError> {
    {
        let mut src_reader = src_reader;
        // Hand the reader its cancellation handle before any read so a
        // network reader can poll `is_requested()` at page/batch
        // boundaries and stop within the shutdown bound. The file arm's
        // default impl ignores it (the dropped-receiver stop suffices).
        if let Some(token) = shutdown_token {
            src_reader.set_shutdown_token(token);
        }
        let reader_schema = src_reader.schema()?;

        // Widen the reader schema with `$source.file`, `$source.name`,
        // and `$source.event_time` engine-stamped tail columns. The
        // stamps travel with every record so downstream operators
        // resolve them per-record via column reads (see
        // `dispatch::source_file_arc_of`, `dispatch::source_name_arc_of`,
        // and the time-windowed aggregate arm) instead of indexing
        // external row-keyed Vecs. Tail order is load-bearing — see
        // `bind_schema::columns_from_decl`: `$ck.<field>` shadows first,
        // then `$widened`, then `$source.file`, then `$source.name`,
        // then `$source.event_time` last.
        let mut widened_builder =
            clinker_record::SchemaBuilder::with_capacity(reader_schema.column_count() + 3);
        for (idx, col) in reader_schema.columns().iter().enumerate() {
            widened_builder = match reader_schema.field_metadata(idx) {
                Some(meta) => widened_builder.with_field_meta(col.as_ref(), meta.clone()),
                None => widened_builder.with_field(col.as_ref()),
            };
        }
        let widened_schema = widened_builder
            .with_field_meta(
                clinker_plan::config::pipeline_node::SOURCE_FILE_COLUMN,
                clinker_record::FieldMetadata::source_file(),
            )
            .with_field_meta(
                clinker_plan::config::pipeline_node::SOURCE_NAME_COLUMN,
                clinker_record::FieldMetadata::source_name(),
            )
            .with_field_meta(
                clinker_plan::config::pipeline_node::SOURCE_EVENT_TIME_COLUMN,
                clinker_record::FieldMetadata::source_event_time(),
            )
            .build();

        // Fallback `$source.file` for records whose reader exposes no
        // per-record file identity (`current_source_file() == None`):
        // single-file readers and every non-file transport. Derived from
        // the Source node name so a pathless source has one stable
        // synthetic identifier. The document-boundary `need_new_doc`
        // logic keys on this Arc by pointer equality, so a single stable
        // Arc yields exactly one document per pathless source rather than
        // a fresh document per record.
        let static_source_file: Arc<str> = if src_cfg.path_str().is_empty() {
            Arc::from(format!("<source:{}>", src_cfg.name))
        } else {
            Arc::from(src_cfg.path_str())
        };
        let source_name_arc: Arc<str> = Arc::from(src_cfg.name.as_str());

        let (watermark_column_idx, delay_nanos): (Option<usize>, i64) = match src_cfg
            .watermark
            .as_ref()
        {
            None => (None, 0),
            Some(wm) => {
                let idx = widened_schema.index(wm.column.as_str()).ok_or_else(|| {
                    PipelineError::Config(clinker_plan::config::ConfigError::Validation(format!(
                        "source '{}' declares watermark.column = '{}' but no such column \
                         exists on the runtime schema (declared columns: {:?})",
                        src_cfg.name,
                        wm.column,
                        widened_schema.columns(),
                    )))
                })?;
                // Saturating i64 nanos — a 292-year `Duration` is the
                // theoretical ceiling. Anything beyond saturates to
                // `i64::MAX` and stays a useful (if extreme) shift.
                let delay_nanos = wm
                    .delay
                    .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
                    .unwrap_or(0);
                (Some(idx), delay_nanos)
            }
        };

        let mut stream = stream;
        let mut rn: u64 = 0;
        let mut total_count: u64 = 0;
        let mut watermark_observations: Vec<(Arc<str>, i64)> = Vec::new();
        // Per-file envelope-level stack, outermost (file-level) first.
        //
        // A single-level source (XML, JSON, EDIFACT, plain CSV) holds at
        // most one entry: the file-level document opened from the reader's
        // pre-scan. A multi-level source (EDI X12 ISA → GS → ST) pushes a
        // child context per nested `OpenLevel` event and pops it on the
        // matching `CloseLevel`, so the stack depth tracks the live
        // envelope nesting. The INNERMOST entry (`last()`) is the context
        // every body record carries — its flattened sections expose every
        // enclosing level's `$doc.<section>.<field>` through the unchanged
        // two-level lookup. Envelope sections are empty until reader-side
        // pre-scan lands; absent sections resolve to `Value::Null` per the
        // streaming-resolver convention.
        // The file currently being streamed is the `source_file` of the
        // file-level document at the base of the stack (`doc_stack[0]`), so
        // no separate file-tracking variable is needed — the stack is the
        // single source of truth for both the live nesting and the open
        // file's identity.
        let mut doc_stack: Vec<Arc<clinker_record::DocumentContext>> = Vec::new();
        loop {
            match src_reader.next_record() {
                Ok(Some(record)) => {
                    rn += 1;
                    total_count += 1;
                    let file_arc = src_reader
                        .current_source_file()
                        .cloned()
                        .unwrap_or_else(|| Arc::clone(&static_source_file));
                    // Document boundary detection: on the first record, or
                    // when the reader transitions to a new file, close the
                    // prior file's whole level stack and open a fresh
                    // file-level document whose Arc is shared across all
                    // records of this file.
                    if stack_belongs_to_other_file(&doc_stack, &file_arc) {
                        open_file_level_doc(
                            &src_cfg,
                            &mut stream,
                            &mut doc_stack,
                            &mut src_reader,
                            &file_arc,
                        )?;
                    }
                    // Apply the envelope boundaries the reader crossed to
                    // reach this record BEFORE stamping it, so the record
                    // carries the innermost level it actually sits inside.
                    // A reader queues `OpenLevel`/`CloseLevel` as it crosses
                    // each envelope segment during `next_record`; draining
                    // here puts every level opened ahead of this record on
                    // the stack first, and pops every level that closed
                    // before it.
                    apply_envelope_events(
                        &src_cfg,
                        &mut stream,
                        &mut doc_stack,
                        &mut src_reader,
                        &file_arc,
                    )?;
                    let mut values: Vec<clinker_record::Value> = record.values().to_vec();
                    let event_time_value: clinker_record::Value = if let Some(idx) =
                        watermark_column_idx
                        && let Some(value) = values.get(idx)
                        && let Some(raw_nanos) = value_to_event_time_nanos(value)
                    {
                        let effective = raw_nanos.saturating_sub(delay_nanos);
                        watermark_observations.push((Arc::clone(&file_arc), effective));
                        clinker_record::Value::Integer(effective)
                    } else {
                        clinker_record::Value::Null
                    };
                    values.push(clinker_record::Value::from(file_arc.as_ref()));
                    values.push(clinker_record::Value::from(source_name_arc.as_ref()));
                    values.push(event_time_value);
                    let mut widened_record =
                        clinker_record::Record::new(Arc::clone(&widened_schema), values);
                    // The record carries the INNERMOST open level, so its
                    // `$doc.*` sees every enclosing envelope's sections.
                    if let Some(ctx) = doc_stack.last() {
                        widened_record.set_doc_ctx(Arc::clone(ctx));
                    }
                    // A closed channel means the consumer stopped pulling —
                    // a shutdown-token unwind dropped the receiver, or the
                    // downstream finished early. That is a graceful stop, not
                    // a failure: stop producing and return the records pushed
                    // so far. The dispatch side surfaces the interruption (if
                    // any) through `report.interrupted`.
                    if stream.push(widened_record, rn).is_err() {
                        break;
                    }
                }
                Ok(None) => {
                    // End of input. The reader may still hold trailing
                    // envelope events — a header-only interchange (envelope
                    // structure, zero body records), or an inner envelope
                    // that closed, or even one that opened, after the last
                    // body record. Applying them here, NOT skipping them, is
                    // load-bearing: a trailing `OpenLevel` left unapplied
                    // would leave the level stack unbalanced and every
                    // downstream `DocumentClose` count would be off,
                    // aborting the run. This is the symmetric end of the
                    // before-record drain above — every event a reader
                    // queues is applied, whether a record follows it or not.
                    //
                    // The reader's own `current_source_file()` names the
                    // file these trailing events belong to: a header-only
                    // interchange for a NEW file (one whose body produced no
                    // record) points here at that new file, and
                    // `apply_envelope_events` then transitions to it,
                    // closing the prior file's stack first. The static
                    // fallback covers a single-file reader with no
                    // per-record file identity.
                    let file_arc = src_reader
                        .current_source_file()
                        .cloned()
                        .unwrap_or_else(|| Arc::clone(&static_source_file));
                    apply_envelope_events(
                        &src_cfg,
                        &mut stream,
                        &mut doc_stack,
                        &mut src_reader,
                        &file_arc,
                    )?;
                    break;
                }
                Err(other) => {
                    // An envelope trailer's declared count did not match the
                    // body the reader streamed (X12 SE/GE/IEA, EDIFACT
                    // UNT/UNZ, HL7 BTS/FTS). Under `dlq_granularity:
                    // document` this condemns the WHOLE source file rather
                    // than aborting the run: the count is only known here, at
                    // the trailer, AFTER every body record it counts has
                    // already streamed, so the document cannot be rejected
                    // before its first record. Instead, tag the file's
                    // close with a structural-reject payload carrying a
                    // representative document record; the consumer-side
                    // Source arm marks the file failed through the #97
                    // reject seam, and #97's per-file Output buffer rejects
                    // every already-streamed record of the file at its
                    // close. Genuine corruption (truncation, bad delimiters,
                    // control-number echo mismatch) is NOT a `StructuralCount`
                    // and keeps aborting even under the opt-in.
                    if other.is_structural_count()
                        && src_cfg.dlq_granularity == clinker_plan::config::DlqGranularity::Document
                    {
                        let file_arc = src_reader
                            .current_source_file()
                            .cloned()
                            .unwrap_or_else(|| Arc::clone(&static_source_file));
                        // The document context the reject keys on. A malformed
                        // file that streamed at least one body record has its
                        // level stack open, so use the innermost level. A
                        // ZERO-body malformed envelope (a trailer claiming a
                        // count with no body segment — X12 `ISA … IEA*5~`,
                        // EDIFACT `UNB … UNZ` with no `UNH`, HL7 `BHS`/`BTS`
                        // with no `MSH`) never drained its queued `OpenLevel`
                        // into the stack, so synthesize a file-level context
                        // from the file grain instead. Either way the
                        // representative record carries a real (non-synthetic)
                        // document id and the file's `$source.file` stamp, so
                        // the document-DLQ reject keys at the file grain.
                        let doc_ctx = doc_stack.last().cloned().unwrap_or_else(|| {
                            Arc::new(clinker_record::DocumentContext::new(
                                clinker_record::DocumentId::next(),
                                Arc::clone(&file_arc),
                                indexmap::IndexMap::new(),
                            ))
                        });
                        let rep_record = build_representative_record(
                            &widened_schema,
                            &doc_ctx,
                            &file_arc,
                            &source_name_arc,
                        );
                        let reject = crate::executor::stream_event::StructuralReject {
                            record: rep_record,
                            row_num: rn.saturating_add(1),
                            message: other.to_string(),
                        };
                        emit_structural_reject_close(
                            &mut stream,
                            &mut doc_stack,
                            &doc_ctx,
                            reject,
                        )?;
                        // The trailer-count error fires at the file's closing
                        // segment, so this file is fully consumed. Advance to
                        // the next file of a multi-file source and keep
                        // streaming — dead-lettering one malformed file must
                        // not silently drop the clean files after it (that
                        // would turn the prior loud run-abort into a silent
                        // partial success). With the failed file's level stack
                        // already drained above, the next record reopens a
                        // fresh file-level document. A single-file source has
                        // no next file, so this breaks at end-of-input.
                        if src_reader.advance_to_next_file()? {
                            continue;
                        }
                        break;
                    }
                    return Err(other.into());
                }
            }
        }
        // Close every level still open at end-of-input, innermost first.
        // This balances both the file-level document and any nested level
        // a reader left open (a truncated `--dry-run -n` read, or a reader
        // that opens a level it never explicitly closes).
        close_open_levels(&mut stream, &mut doc_stack)?;
        // Drop the sender so the dispatch-side `recv` returns `Err`
        // (channel disconnected) once the channel drains.
        drop(stream);
        Ok(IngestTaskOutcome {
            source_name: src_cfg.name.clone(),
            total_count,
            watermark_observations,
        })
    }
}

/// `true` when no file-level document is open yet, or the open stack
/// belongs to a different file than `file_arc` — i.e. the driver must open
/// a fresh file-level document (closing the prior file's stack first)
/// before streaming `file_arc`'s records or applying its envelope events.
///
/// The file-level document sits at the base of the stack (`doc_stack[0]`)
/// and carries the file's `source_file` identity, so this is a pointer
/// comparison against that base — the same `Arc::ptr_eq` discriminator the
/// reader uses to mark file transitions.
fn stack_belongs_to_other_file(
    doc_stack: &[Arc<clinker_record::DocumentContext>],
    file_arc: &Arc<str>,
) -> bool {
    match doc_stack.first() {
        None => true,
        Some(file_doc) => !Arc::ptr_eq(file_doc.source_file(), file_arc),
    }
}

/// Push a document-boundary punctuation, treating a closed channel as a
/// benign stop.
///
/// A closed channel during punctuation delivery carries the same meaning
/// as during record delivery — the consumer dropped the receiver (a
/// shutdown unwind or an early-completing downstream). No further
/// punctuation matters once the receiver is gone, so the close is a no-op;
/// the driver's record push breaks on the same signal.
fn push_doc_punctuation(
    stream: &mut crate::executor::source_stream::SourceIngestChannel,
    punct: crate::executor::stream_event::Punctuation,
) -> Result<(), PipelineError> {
    match stream.push_punctuation(punct) {
        Ok(()) | Err(crate::executor::source_stream::SourceStreamError::Closed) => Ok(()),
    }
}

/// Close every open level on `doc_stack`, innermost first, draining it to
/// empty.
///
/// Each `DocumentOpen` is balanced by exactly one `DocumentClose` in
/// strict LIFO order. Used at a file transition (the prior file's stack
/// closes before the next file opens) and at end-of-input.
fn close_open_levels(
    stream: &mut crate::executor::source_stream::SourceIngestChannel,
    doc_stack: &mut Vec<Arc<clinker_record::DocumentContext>>,
) -> Result<(), PipelineError> {
    while let Some(level) = doc_stack.pop() {
        push_doc_punctuation(
            stream,
            crate::executor::stream_event::Punctuation::document_close(level),
        )?;
    }
    Ok(())
}

/// Build a representative record for a document the ingest thread condemned
/// for an envelope structural-count failure.
///
/// No single body record is at fault (the whole document is structurally
/// invalid), so this synthesizes one onto the engine-widened schema: the
/// body columns are `Null` and the three engine-stamped tail columns carry
/// the file's `$source.file`, the source's `$source.name`, and a `Null`
/// `$source.event_time`, mirroring the per-record widening in
/// [`drive_record_source`]. The record carries the document's innermost
/// context so its `source_file` stamp keys the document-DLQ reject at the
/// correct file grain. It becomes the `trigger: true` root cause for the
/// file's reject.
fn build_representative_record(
    widened_schema: &Arc<clinker_record::Schema>,
    doc_ctx: &Arc<clinker_record::DocumentContext>,
    file_arc: &Arc<str>,
    source_name_arc: &Arc<str>,
) -> clinker_record::Record {
    let column_count = widened_schema.column_count();
    // The three engine-stamped tail columns are the last three; every
    // body column ahead of them is Null on this synthetic trigger row.
    let mut values: Vec<clinker_record::Value> =
        vec![clinker_record::Value::Null; column_count.saturating_sub(3)];
    values.push(clinker_record::Value::from(file_arc.as_ref()));
    values.push(clinker_record::Value::from(source_name_arc.as_ref()));
    values.push(clinker_record::Value::Null);
    let mut record = clinker_record::Record::new(Arc::clone(widened_schema), values);
    record.set_doc_ctx(Arc::clone(doc_ctx));
    record
}

/// Condemn the open document for an envelope structural-count failure: emit
/// the innermost open level's `DocumentClose` carrying the structural-reject
/// payload, then close every remaining open level (the file-level document
/// and any enclosing nested levels) so the boundary stack stays balanced.
///
/// The reject payload rides the INNERMOST close because that is the level
/// the trailer just failed; the consumer-side reject seam keys on the
/// representative record's `$source.file` stamp, so the file grain is
/// correct regardless of which level carries the payload. Draining the rest
/// of the stack here keeps every `DocumentOpen` balanced by exactly one
/// `DocumentClose`, the invariant downstream per-document consumers rely on.
///
/// When the stack is EMPTY — a zero-body malformed envelope whose queued
/// `OpenLevel` never drained into the stack — the payload rides a standalone
/// `DocumentClose` on the synthesized `synthesized_ctx` so the consumer-side
/// Source arm still marks the file failed and emits its trigger (with no
/// collaterals, since no record of the file streamed). The end-of-DAG sweep
/// `reject_unclosed_failed_documents` then emits the trigger for that
/// recordless document, so a zero-body malformed file still dead-letters.
fn emit_structural_reject_close(
    stream: &mut crate::executor::source_stream::SourceIngestChannel,
    doc_stack: &mut Vec<Arc<clinker_record::DocumentContext>>,
    synthesized_ctx: &Arc<clinker_record::DocumentContext>,
    reject: crate::executor::stream_event::StructuralReject,
) -> Result<(), PipelineError> {
    match doc_stack.pop() {
        Some(innermost) => {
            push_doc_punctuation(
                stream,
                crate::executor::stream_event::Punctuation::structural_reject_close(
                    innermost, reject,
                ),
            )?;
        }
        None => {
            push_doc_punctuation(
                stream,
                crate::executor::stream_event::Punctuation::structural_reject_close(
                    Arc::clone(synthesized_ctx),
                    reject,
                ),
            )?;
        }
    }
    close_open_levels(stream, doc_stack)
}

/// Open the file-level document for `file_arc`, closing the prior file's
/// entire level stack first.
///
/// Runs the envelope pre-scan that populates the file-level sections (the
/// head/tail metadata a single-level reader extracts), emits the
/// file-level `DocumentOpen`, and seeds `doc_stack` with the new
/// file-level context. The file-level document opens on whichever comes
/// first for a file — its first body record or its first envelope event;
/// a header-only interchange with no body still surfaces its envelope and
/// boundaries (issue #395).
///
/// # Errors
///
/// Returns [`PipelineError::Internal`] if the reader's envelope pre-scan
/// fails for a source that declares an `envelope:` config.
fn open_file_level_doc(
    src_cfg: &clinker_plan::config::SourceConfig,
    stream: &mut crate::executor::source_stream::SourceIngestChannel,
    doc_stack: &mut Vec<Arc<clinker_record::DocumentContext>>,
    src_reader: &mut Box<dyn crate::source::RecordSource>,
    file_arc: &Arc<str>,
) -> Result<(), PipelineError> {
    close_open_levels(stream, doc_stack)?;
    // Pre-scan declared envelope sections for the new file. Sources
    // without `envelope:` config — and the default trait impl — return an
    // empty section map. Sections populate before the `DocumentOpen`
    // emits, so every record of this file carries the same fully-populated
    // context.
    let envelope_sections = match src_cfg.envelope.as_ref() {
        Some(cfg) => src_reader
            .prepare_document(cfg)
            .map_err(|e| PipelineError::Internal {
                op: "envelope-pre-scan",
                node: src_cfg.name.clone(),
                detail: e.to_string(),
            })?,
        None => indexmap::IndexMap::new(),
    };
    let new_ctx = Arc::new(clinker_record::DocumentContext::new(
        clinker_record::DocumentId::next(),
        Arc::clone(file_arc),
        envelope_sections,
    ));
    push_doc_punctuation(
        stream,
        crate::executor::stream_event::Punctuation::document_open(Arc::clone(&new_ctx)),
    )?;
    doc_stack.push(new_ctx);
    Ok(())
}

/// Apply the reader's pending nested-envelope events to the level stack.
///
/// `OpenLevel` mints a child of the current innermost level (flattening
/// the enclosing sections in) and emits its `DocumentOpen`; `CloseLevel`
/// pops the innermost nested level and emits its `DocumentClose`. A file
/// that has not yet opened its file-level document opens it here before
/// applying any nested event, so an envelope boundary with no preceding
/// body record still frames the document (issue #395).
///
/// A stray `CloseLevel` that would pop the file-level document or
/// underflow an empty stack is ignored: the file-transition / end-of-input
/// sweep owns the file-level close, and a balanced reader never emits one.
///
/// # Errors
///
/// Returns [`PipelineError::Internal`] if opening the file-level document
/// (when a nested event arrives before any record) triggers an envelope
/// pre-scan failure.
fn apply_envelope_events(
    src_cfg: &clinker_plan::config::SourceConfig,
    stream: &mut crate::executor::source_stream::SourceIngestChannel,
    doc_stack: &mut Vec<Arc<clinker_record::DocumentContext>>,
    src_reader: &mut Box<dyn crate::source::RecordSource>,
    file_arc: &Arc<str>,
) -> Result<(), PipelineError> {
    for event in src_reader.take_envelope_events() {
        match event {
            clinker_format::EnvelopeEvent::OpenLevel { sections } => {
                // Open a fresh file-level document when none is open yet,
                // or when the open stack belongs to a different file than
                // this event — a trailing header-only interchange for a new
                // file (one whose body produced no record) reaches the
                // driver only through this path, so the file transition
                // (closing the prior file's stack) must happen here too,
                // not just on a record boundary. `open_file_level_doc`
                // closes the stale stack before opening the new one.
                if stack_belongs_to_other_file(doc_stack, file_arc) {
                    open_file_level_doc(src_cfg, stream, doc_stack, src_reader, file_arc)?;
                }
                let parent = doc_stack.last().expect("file-level document opened above");
                let child = Arc::new(parent.child(clinker_record::DocumentId::next(), sections));
                push_doc_punctuation(
                    stream,
                    crate::executor::stream_event::Punctuation::document_open(Arc::clone(&child)),
                )?;
                doc_stack.push(child);
            }
            clinker_format::EnvelopeEvent::CloseLevel => {
                // Keep the file-level document (index 0) — its close is the
                // file-transition / EOF sweep's job — so only pop a
                // genuinely nested level.
                if doc_stack.len() > 1
                    && let Some(level) = doc_stack.pop()
                {
                    push_doc_punctuation(
                        stream,
                        crate::executor::stream_event::Punctuation::document_close(level),
                    )?;
                }
            }
        }
    }
    Ok(())
}

/// Extract `Vec<FieldDef>` from `SourceConfig.schema` for fixed-width format.
///
/// Resolves `SchemaSource::Inline` or `SchemaSource::FilePath` to `Vec<FieldDef>`.
/// Returns a config validation error if schema is `None` (fixed-width requires
/// explicit schema with field definitions).
fn extract_field_defs(
    input: &clinker_plan::config::SourceConfig,
) -> Result<Vec<clinker_record::schema_def::FieldDef>, PipelineError> {
    let schema_source = input.schema.as_ref().ok_or_else(|| {
        PipelineError::Config(clinker_plan::config::ConfigError::Validation(
            "fixed-width format requires explicit schema with field definitions".into(),
        ))
    })?;
    let def = match schema_source {
        clinker_plan::config::SchemaSource::Inline(def) => def.clone(),
        clinker_plan::config::SchemaSource::FilePath(path) => {
            clinker_plan::schema::load_schema(std::path::Path::new(path)).map_err(|e| {
                PipelineError::Config(clinker_plan::config::ConfigError::Validation(format!(
                    "failed to load schema from '{path}': {e}",
                )))
            })?
        }
    };
    def.fields.ok_or_else(|| {
        PipelineError::Config(clinker_plan::config::ConfigError::Validation(
            "fixed-width schema must have 'fields' defined".into(),
        ))
    })
}

/// Build CSV reader config from optional CSV input options.
fn build_csv_reader_config(
    opts: Option<&clinker_plan::config::CsvInputOptions>,
) -> CsvReaderConfig {
    let mut config = CsvReaderConfig::default();
    if let Some(opts) = opts {
        if let Some(ref d) = opts.delimiter
            && let Some(b) = d.as_bytes().first()
        {
            config.delimiter = *b;
        }
        if let Some(ref q) = opts.quote_char
            && let Some(b) = q.as_bytes().first()
        {
            config.quote_char = *b;
        }
        if let Some(h) = opts.has_header {
            config.has_header = h;
        }
    }
    config
}

/// Default cap on the JSON envelope pre-scan's path-pruned document index
/// when a source declares no explicit `max_index_bytes`. Finite by design:
/// the index retains only declared `$doc.*` subtrees, so 64 MB is generous
/// headroom for envelope metadata while still failing loud before an
/// unbounded-retention OOM.
const DEFAULT_JSON_MAX_INDEX_BYTES: usize = 64 * 1_000_000;

/// Build JSON reader config from optional JSON input options, array paths,
/// and the source's planner-attributed `$doc.*` path set.
///
/// `declared_doc_paths` is the set of envelope paths some downstream
/// program references through this source; the reader's pre-scan retains
/// only the sections these paths name. When the source declares no
/// `max_index_bytes`, the pre-scan index is capped at
/// [`DEFAULT_JSON_MAX_INDEX_BYTES`].
fn build_json_reader_config(
    opts: Option<&clinker_plan::config::JsonInputOptions>,
    array_paths: Option<&[clinker_plan::config::ArrayPathConfig]>,
    declared_doc_paths: &[cxl::analyzer::doc_paths::DocPath],
) -> JsonReaderConfig {
    let mut config = JsonReaderConfig {
        declared_doc_paths: declared_doc_paths.to_vec(),
        max_index_bytes: Some(DEFAULT_JSON_MAX_INDEX_BYTES),
        ..Default::default()
    };
    if let Some(opts) = opts {
        config.format = opts.format.as_ref().map(|f| match f {
            clinker_plan::config::JsonFormat::Array => JsonMode::Array,
            clinker_plan::config::JsonFormat::Ndjson => JsonMode::Ndjson,
            clinker_plan::config::JsonFormat::Object => JsonMode::Object,
        });
        config.record_path = opts.record_path.clone();
        if let Some(cap) = opts.max_index_bytes {
            config.max_index_bytes = Some(cap.0 as usize);
        }
    }
    if let Some(paths) = array_paths {
        config.array_paths = paths
            .iter()
            .map(|p| ArrayPathSpec {
                path: p.path.clone(),
                mode: match p.mode {
                    clinker_plan::config::ArrayMode::Explode => ArrayPathMode::Explode,
                    clinker_plan::config::ArrayMode::Join => ArrayPathMode::Join,
                },
                separator: p.separator.clone().unwrap_or_else(|| ",".to_string()),
            })
            .collect();
    }
    config
}

/// Default cap on the XML envelope pre-scan's path-pruned document index
/// when a source declares no explicit `max_index_bytes`. Finite by design:
/// the index retains only declared `$doc.*` subtrees, so 64 MB is generous
/// headroom for envelope metadata while still failing loud before an
/// unbounded-retention OOM. Mirrors [`DEFAULT_JSON_MAX_INDEX_BYTES`].
const DEFAULT_XML_MAX_INDEX_BYTES: usize = 64 * 1_000_000;

/// Build XML reader config from optional XML input options, array paths, and
/// the source's planner-attributed `$doc.*` path set.
///
/// `declared_doc_paths` is the set of envelope paths some downstream program
/// references through this source; the reader's pre-scan retains only the
/// sections these paths name. When the source declares no `max_index_bytes`,
/// the pre-scan index is capped at [`DEFAULT_XML_MAX_INDEX_BYTES`].
fn build_xml_reader_config(
    opts: Option<&clinker_plan::config::XmlInputOptions>,
    array_paths: Option<&[clinker_plan::config::ArrayPathConfig]>,
    declared_doc_paths: &[cxl::analyzer::doc_paths::DocPath],
) -> XmlReaderConfig {
    let mut config = XmlReaderConfig {
        declared_doc_paths: declared_doc_paths.to_vec(),
        max_index_bytes: Some(DEFAULT_XML_MAX_INDEX_BYTES),
        ..Default::default()
    };
    if let Some(opts) = opts {
        config.record_path = opts.record_path.clone();
        if let Some(ref prefix) = opts.attribute_prefix {
            config.attribute_prefix = prefix.clone();
        }
        config.namespace_handling = match opts.namespace_handling {
            Some(clinker_plan::config::NamespaceHandling::Qualify) => NamespaceMode::Qualify,
            _ => NamespaceMode::Strip,
        };
        if let Some(cap) = opts.max_index_bytes {
            config.max_index_bytes = Some(cap.0 as usize);
        }
    }
    if let Some(paths) = array_paths {
        config.array_paths = paths
            .iter()
            .map(|p| XmlArrayPath {
                path: p.path.clone(),
                mode: match p.mode {
                    clinker_plan::config::ArrayMode::Explode => XmlArrayMode::Explode,
                    clinker_plan::config::ArrayMode::Join => XmlArrayMode::Join,
                },
                separator: p.separator.clone().unwrap_or_else(|| ",".to_string()),
            })
            .collect();
    }
    config
}

/// Build fixed-width reader config from optional fixed-width input options.
fn build_fw_reader_config(
    opts: Option<&clinker_plan::config::FixedWidthInputOptions>,
) -> FixedWidthReaderConfig {
    let mut config = FixedWidthReaderConfig::default();
    if let Some(opts) = opts
        && let Some(ref sep) = opts.line_separator
    {
        config.line_separator = sep.clone();
    }
    config
}

fn build_edifact_reader_config(
    opts: Option<&clinker_plan::config::EdifactInputOptions>,
) -> EdifactReaderConfig {
    let mut config = EdifactReaderConfig::default();
    if let Some(opts) = opts
        && let Some(max) = opts.max_elements
    {
        config.max_elements = max;
    }
    config
}

fn build_x12_reader_config(
    opts: Option<&clinker_plan::config::X12InputOptions>,
) -> Result<X12ReaderConfig, PipelineError> {
    let mut config = X12ReaderConfig::default();
    if let Some(opts) = opts {
        if let Some(max) = opts.max_elements {
            config.max_elements = max;
        }
        if let Some(encoding) = opts.encoding.as_deref() {
            config.charset = parse_x12_charset(encoding)?;
        }
        config.group_section = opts.group_section.clone();
        config.set_section = opts.set_section.clone();
    }
    Ok(config)
}

/// Resolve a source-declared X12 `encoding` name to a [`Charset`], mapping
/// an unsupported value to a config-validation error so the run fails at
/// startup with the name the user must fix.
fn parse_x12_charset(encoding: &str) -> Result<Charset, PipelineError> {
    Charset::from_name(encoding).map_err(|e| {
        PipelineError::Config(clinker_plan::config::ConfigError::Validation(format!(
            "X12 source: {e}"
        )))
    })
}

fn build_hl7_reader_config(
    opts: Option<&clinker_plan::config::Hl7InputOptions>,
) -> Hl7ReaderConfig {
    let mut config = Hl7ReaderConfig::default();
    if let Some(opts) = opts {
        if let Some(max) = opts.max_fields {
            config.max_fields = max;
        }
        if let Some(splits) = &opts.split_fields {
            // Config validation already rejected an unparseable `field` name,
            // a position past `max_fields`, and a zero axis width, so a field
            // that fails to parse here is dropped defensively rather than
            // surfaced — the split simply contributes no columns.
            config.split_fields = splits
                .iter()
                .filter_map(|s| {
                    s.field_position().map(|field_index| Hl7FieldSplit {
                        field_index,
                        repetitions: s.repetitions,
                        components: s.components,
                        subcomponents: s.subcomponents,
                    })
                })
                .collect();
        }
    }
    config
}

fn build_swift_reader_config(
    opts: Option<&clinker_plan::config::SwiftInputOptions>,
) -> SwiftReaderConfig {
    let mut config = SwiftReaderConfig::default();
    if let Some(opts) = opts
        && let Some(max) = opts.max_fields
    {
        config.max_fields = max;
    }
    config
}

#[cfg(test)]
mod tests {
    //! These tests observe the exact `StreamEvent` punctuation stream the
    //! driver emits — the only place the document open/close boundaries
    //! are visible before downstream operators consume them. End-to-end
    //! pipeline tests assert on record counts and `$doc` resolution; they
    //! cannot see whether a *record-less* boundary (a header-only
    //! interchange, or a trailing empty inner envelope) was emitted at
    //! all, because such a boundary contributes no record and no `$doc`
    //! read. Draining the source channel here pins exactly that: that the
    //! end-of-input drain emits the trailing/header-only document
    //! boundaries instead of dropping them.

    use super::*;
    use crate::executor::stream_event::{PunctuationKind, StreamEvent};
    use clinker_format::EnvelopeEvent;
    use clinker_record::{Schema, SchemaBuilder, Value};
    use indexmap::IndexMap;

    /// A scripted step: emit a record, or queue an envelope boundary the
    /// driver drains around the surrounding `next_record` call.
    enum Step {
        Open(&'static str),
        Close,
        Record(i64),
    }

    /// Replays a fixed [`Step`] script as a `RecordSource`, modeling a
    /// multi-level envelope reader. Single pathless file (no per-record
    /// file identity), so the driver uses one stable synthetic id.
    struct ScriptedReader {
        schema: Arc<Schema>,
        steps: std::collections::VecDeque<Step>,
        pending: Vec<EnvelopeEvent>,
    }

    impl ScriptedReader {
        fn new(steps: Vec<Step>) -> Self {
            Self {
                schema: SchemaBuilder::with_capacity(1).with_field("id").build(),
                steps: steps.into_iter().collect(),
                pending: Vec::new(),
            }
        }
    }

    impl crate::source::RecordSource for ScriptedReader {
        fn schema(&mut self) -> Result<Arc<Schema>, clinker_format::FormatError> {
            Ok(Arc::clone(&self.schema))
        }

        fn next_record(
            &mut self,
        ) -> Result<Option<clinker_record::Record>, clinker_format::FormatError> {
            while let Some(step) = self.steps.pop_front() {
                match step {
                    Step::Open(name) => {
                        let mut field = IndexMap::new();
                        field.insert(Box::from("tag"), Value::String(name.into()));
                        let mut sections = IndexMap::new();
                        sections.insert(Box::from(name), Value::Map(Box::new(field)));
                        self.pending.push(EnvelopeEvent::OpenLevel { sections });
                    }
                    Step::Close => self.pending.push(EnvelopeEvent::CloseLevel),
                    Step::Record(id) => {
                        return Ok(Some(clinker_record::Record::new(
                            Arc::clone(&self.schema),
                            vec![Value::Integer(id)],
                        )));
                    }
                }
            }
            Ok(None)
        }

        fn take_envelope_events(&mut self) -> Vec<EnvelopeEvent> {
            std::mem::take(&mut self.pending)
        }
    }

    /// One observed event from the driver's output channel, projected to
    /// just what the boundary assertions need.
    #[derive(Debug, PartialEq, Eq)]
    enum Observed {
        Open,
        Close,
        Record(i64),
    }

    /// Minimal pathless CSV `SourceConfig` — the transport the driver
    /// drives is supplied directly, so only the name/format matter.
    fn pathless_source_config() -> clinker_plan::config::SourceConfig {
        let yaml = r#"
pipeline:
  name: drive_test
nodes:
  - type: source
    name: edi
    config:
      name: edi
      type: csv
      path: placeholder.csv
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: edi
    config:
      name: out
      type: csv
      path: out.csv
"#;
        let mut config = clinker_plan::config::parse_config(yaml).expect("parse");
        for spanned in &mut config.nodes {
            if let clinker_plan::config::PipelineNode::Source { config: body, .. } =
                &mut spanned.value
            {
                body.source.path = None;
                return body.source.clone();
            }
        }
        unreachable!("source node present")
    }

    /// Drive `drive_record_source` over a pathless source and collect the
    /// ordered raw stream the driver emitted. The reader (scripted stand-in
    /// or a real format reader) is small relative to the channel capacity, so
    /// the driver runs to completion on this thread and drops the sender,
    /// after which the receiver drains cleanly. Each caller projects the
    /// returned `StreamEvent`s into the boundary/record shape it asserts on.
    fn drive_to_stream_events(reader: Box<dyn crate::source::RecordSource>) -> Vec<StreamEvent> {
        let src_cfg = pathless_source_config();
        let handle = crate::pipeline::memory::ConsumerHandle::new();
        let (stream, rx) = crate::executor::source_stream::SourceIngestChannel::new(
            crate::executor::source_stream::SourceIngestChannel::DEFAULT_CAPACITY,
            handle,
        );
        drive_record_source(src_cfg, reader, stream, None).expect("drive");
        rx.iter().collect()
    }

    /// Drive the script and return the ordered stream of records and document
    /// boundaries the driver emitted.
    fn drive(steps: Vec<Step>) -> Vec<Observed> {
        let reader: Box<dyn crate::source::RecordSource> = Box::new(ScriptedReader::new(steps));
        drive_to_stream_events(reader)
            .into_iter()
            .map(|ev| match ev {
                StreamEvent::Record(rec, _) => match rec.values()[0] {
                    Value::Integer(id) => Observed::Record(id),
                    ref other => panic!("unexpected record value {other:?}"),
                },
                StreamEvent::Punctuation(p) => match p.kind() {
                    PunctuationKind::DocumentOpen => Observed::Open,
                    PunctuationKind::DocumentClose => Observed::Close,
                },
            })
            .collect()
    }

    #[test]
    fn nested_levels_emit_balanced_open_close_around_records() {
        // ISA → GS → ST → record → close ST → close GS → close ISA.
        let observed = drive(vec![
            Step::Open("interchange"),
            Step::Open("group"),
            Step::Open("transaction"),
            Step::Record(1),
            Step::Close, // ST
            Step::Close, // GS
            Step::Close, // ISA
        ]);
        // File-level Open, then the three nested Opens, the record, the
        // three nested Closes, and the file-level Close — every Open
        // balanced by a Close, innermost first.
        assert_eq!(
            observed,
            vec![
                Observed::Open, // file-level document
                Observed::Open, // interchange
                Observed::Open, // group
                Observed::Open, // transaction
                Observed::Record(1),
                Observed::Close, // transaction
                Observed::Close, // group
                Observed::Close, // interchange
                Observed::Close, // file-level document
            ]
        );
    }

    #[test]
    fn trailing_inner_envelope_after_last_record_emits_its_boundaries() {
        // The desync regression, observed at the boundary stream: an inner
        // envelope that OPENS and closes after the last body record. The
        // end-of-input drain must emit that trailing open AND its close —
        // reverting the drain to a bare `break` drops both, which this
        // assertion catches.
        let observed = drive(vec![
            Step::Open("interchange"),
            Step::Open("transaction"),
            Step::Record(10),
            Step::Close, // ST closes after its record
            // Trailing inner envelope: opens and closes with no record
            // between, after the file's final record.
            Step::Open("transaction"),
            Step::Close,
            Step::Close, // ISA
        ]);
        assert_eq!(
            observed,
            vec![
                Observed::Open, // file-level document
                Observed::Open, // interchange
                Observed::Open, // transaction
                Observed::Record(10),
                Observed::Close, // transaction
                Observed::Open,  // trailing transaction — emitted by the EOF drain
                Observed::Close, // trailing transaction close
                Observed::Close, // interchange
                Observed::Close, // file-level document
            ]
        );
        // Three of the boundaries (the trailing inner open+close and the
        // interchange close) are produced entirely by the end-of-input
        // drain — there is no body record after the final close to carry
        // them. A bare `break` at end-of-input would emit only the
        // file-level close (the sweep), losing the trailing pair.
        let opens = observed.iter().filter(|o| **o == Observed::Open).count();
        let closes = observed.iter().filter(|o| **o == Observed::Close).count();
        assert_eq!(opens, 4, "file + interchange + 2 transaction opens");
        assert_eq!(closes, 4, "every open balanced by a close");
    }

    #[test]
    fn header_only_interchange_emits_open_and_close_with_no_records() {
        // Issue #395: a file carrying envelope structure but zero body
        // records still opens a document and emits its open/close
        // boundaries. The entire script is an interchange that opens and
        // closes with nothing inside — every boundary here is produced by
        // the end-of-input drain, since no record ever triggers the
        // record-arm open.
        let observed = drive(vec![Step::Open("interchange"), Step::Close]);
        assert_eq!(
            observed,
            vec![
                Observed::Open,  // file-level document, opened by the first envelope event
                Observed::Open,  // interchange
                Observed::Close, // interchange
                Observed::Close, // file-level document
            ],
            "header-only interchange must surface its document boundaries"
        );
        // No records, but four boundary punctuations — the #395 fix. With
        // the end-of-input drain reverted, zero punctuations are emitted
        // and this vector would be empty.
        assert!(
            observed.iter().all(|o| *o != Observed::Record(0)),
            "header-only interchange has no body records"
        );
        assert_eq!(observed.len(), 4);
    }

    /// One observed event from driving a real [`FormatReader`], with records
    /// projected to a chosen string column so a per-record assertion can name
    /// which record landed where in the boundary stream.
    #[derive(Debug, PartialEq, Eq)]
    enum ObservedTagged {
        Open,
        Close,
        Record(String),
    }

    /// Drive a real [`FormatReader`] (not the scripted stand-in) through the
    /// shared ingest loop and return the ordered stream of document
    /// boundaries and records, each record projected to its named string
    /// column. Pins the exact interleaving of `DocumentOpen`/`DocumentClose`
    /// punctuations with body records that the driver emits.
    fn drive_format_reader(
        reader: Box<dyn FormatReader>,
        record_column: &str,
    ) -> Vec<ObservedTagged> {
        let src_reader: Box<dyn crate::source::RecordSource> = Box::new(reader);
        drive_to_stream_events(src_reader)
            .into_iter()
            .map(|ev| match ev {
                StreamEvent::Record(rec, _) => {
                    let tag = match rec.get(record_column) {
                        Some(Value::String(s)) => s.to_string(),
                        other => panic!("record column {record_column:?} not a string: {other:?}"),
                    };
                    ObservedTagged::Record(tag)
                }
                StreamEvent::Punctuation(p) => match p.kind() {
                    PunctuationKind::DocumentOpen => ObservedTagged::Open,
                    PunctuationKind::DocumentClose => ObservedTagged::Close,
                },
            })
            .collect()
    }

    #[test]
    fn swift_message_close_follows_its_last_record() {
        // A SWIFT message's CloseLevel must be applied AFTER the message's
        // last body record, not before it: the driver drains
        // `take_envelope_events` and applies each boundary before pushing the
        // record that pull returned, so a close queued alongside the final
        // record-bearing pull would strand that record outside its own
        // document level. This locks the close onto the terminal `Ok(None)`
        // pull so every body record stays inside the message document.
        let mt103 = "{1:F01BANKBEBBAXXX0000000000}{2:I103BANKDEFFXXXXN}\
            {4:\r\n:20:REF1\r\n:23B:CRED\r\n:32A:240101USD100,00\r\n-}";
        let reader: Box<dyn FormatReader> = Box::new(SwiftReader::new(
            std::io::Cursor::new(mt103.as_bytes().to_vec()),
            SwiftReaderConfig::default(),
        ));
        let observed = drive_format_reader(reader, "tag");
        // File document opens, the message level opens, all three block-4
        // records stream, THEN the message and file levels close. The third
        // record (`32A`) must precede the message `Close`.
        assert_eq!(
            observed,
            vec![
                ObservedTagged::Open, // file-level document
                ObservedTagged::Open, // message level
                ObservedTagged::Record("20".to_string()),
                ObservedTagged::Record("23B".to_string()),
                ObservedTagged::Record("32A".to_string()),
                ObservedTagged::Close, // message level — after the last record
                ObservedTagged::Close, // file-level document
            ],
            "the message Close must follow its last body record"
        );
        // Pin the invariant directly: the last record precedes the first
        // Close. A regression that queues the close on the final
        // record-bearing pull would place a Close before the `32A` record.
        let last_record = observed
            .iter()
            .rposition(|o| matches!(o, ObservedTagged::Record(_)))
            .expect("at least one record");
        let first_close = observed
            .iter()
            .position(|o| *o == ObservedTagged::Close)
            .expect("at least one close");
        assert!(
            last_record < first_close,
            "every body record must precede the message close; got {observed:?}"
        );
    }
}
