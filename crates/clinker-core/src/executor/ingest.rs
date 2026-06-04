//! Source ingest: format-reader construction and the per-source driver
//! thread body that widens, stamps, and pushes records into the dispatch
//! channel.

use std::io::Read;
use std::sync::Arc;

use crate::config::PipelineConfig;
use crate::error::PipelineError;
use clinker_format::csv::reader::{CsvReader, CsvReaderConfig};
use clinker_format::fixed_width::reader::{FixedWidthReader, FixedWidthReaderConfig};
use clinker_format::json::reader::{
    ArrayPathMode, ArrayPathSpec, JsonMode, JsonReader, JsonReaderConfig,
};
use clinker_format::traits::FormatReader;
use clinker_format::xml::reader::{
    NamespaceMode, XmlArrayMode, XmlArrayPath, XmlReader, XmlReaderConfig,
};

/// Build a format-specific reader from input config and raw reader.
///
/// Dispatches on `InputFormat` to construct the correct reader type.
/// Returns `Box<dyn FormatReader>` — all downstream code uses trait methods
/// (`schema()`, `next_record()`).
fn build_format_reader(
    input: &crate::config::SourceConfig,
    reader: Box<dyn Read + Send>,
) -> Result<Box<dyn FormatReader>, PipelineError> {
    match &input.format {
        crate::config::InputFormat::Csv(opts) => {
            let config = build_csv_reader_config(opts.as_ref());
            Ok(Box::new(CsvReader::from_reader(reader, config)))
        }
        crate::config::InputFormat::Json(opts) => {
            let config = build_json_reader_config(opts.as_ref(), input.array_paths.as_deref());
            Ok(Box::new(JsonReader::from_reader(reader, config)?))
        }
        crate::config::InputFormat::Xml(opts) => {
            let config = build_xml_reader_config(opts.as_ref(), input.array_paths.as_deref());
            Ok(Box::new(XmlReader::new(reader, config)?))
        }
        crate::config::InputFormat::FixedWidth(opts) => {
            let fields = extract_field_defs(input)?;
            let config = build_fw_reader_config(opts.as_ref());
            Ok(Box::new(FixedWidthReader::new(reader, fields, config)?))
        }
    }
}

/// Build a [`MultiFileFormatReader`] wrapping every file the discovery
/// layer returned for this source. Single-file sources go through the
/// same path with a one-element slot list — the wrapper short-circuits
/// transparently.
fn build_multi_file_reader(
    input: &crate::config::SourceConfig,
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
        move |reader: Box<dyn Read + Send>,
              _idx: usize|
              -> Result<Box<dyn FormatReader>, clinker_format::FormatError> {
            build_format_reader(&owned_config, reader).map_err(|e| {
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
    use crate::config::PipelineNode;
    use crate::config::pipeline_node::OnUnmapped;

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
                && matches!(format, crate::config::InputFormat::FixedWidth(_))
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
    src_cfg: crate::config::SourceConfig,
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
                    crate::config::ConfigError::Validation(format!(
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
    src_cfg: crate::config::SourceConfig,
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
                crate::config::pipeline_node::SOURCE_FILE_COLUMN,
                clinker_record::FieldMetadata::source_file(),
            )
            .with_field_meta(
                crate::config::pipeline_node::SOURCE_NAME_COLUMN,
                clinker_record::FieldMetadata::source_name(),
            )
            .with_field_meta(
                crate::config::pipeline_node::SOURCE_EVENT_TIME_COLUMN,
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

        let (watermark_column_idx, delay_nanos): (Option<usize>, i64) =
            match src_cfg.watermark.as_ref() {
                None => (None, 0),
                Some(wm) => {
                    let idx = widened_schema.index(wm.column.as_str()).ok_or_else(|| {
                        PipelineError::Config(crate::config::ConfigError::Validation(format!(
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
        // Per-file envelope context. Tracks the document currently
        // being streamed; transitions to a new file emit `DocumentClose`
        // for the previous context and `DocumentOpen` for the new one.
        // Envelope sections are empty until reader-side pre-scan lands
        // — CXL `$doc.<section>.<field>` against this context resolves
        // to `Value::Null`, which is the streaming-resolver convention
        // for an absent section.
        let mut current_doc: Option<Arc<clinker_record::DocumentContext>> = None;
        // A closed channel during punctuation delivery carries the same
        // meaning as during record delivery — the consumer stopped pulling
        // (shutdown unwind or early downstream completion). No further
        // punctuation matters once the receiver is gone, so treat it as a
        // benign no-op; the loop's record push breaks on the same signal.
        let push_punct = |stream: &mut crate::executor::source_stream::SourceIngestChannel,
                          punct: crate::executor::stream_event::Punctuation|
         -> Result<(), PipelineError> {
            match stream.push_punctuation(punct) {
                Ok(()) | Err(crate::executor::source_stream::SourceStreamError::Closed) => Ok(()),
            }
        };
        loop {
            match src_reader.next_record() {
                Ok(Some(record)) => {
                    rn += 1;
                    total_count += 1;
                    let file_arc = src_reader
                        .current_source_file()
                        .cloned()
                        .unwrap_or_else(|| Arc::clone(&static_source_file));
                    // Document boundary detection: on the first record,
                    // or when the reader transitions to a new file, close
                    // the previous document (if any) and open a fresh one
                    // whose Arc is shared across all records of this file.
                    let need_new_doc = match current_doc.as_ref() {
                        None => true,
                        Some(ctx) => !Arc::ptr_eq(ctx.source_file(), &file_arc),
                    };
                    if need_new_doc {
                        if let Some(prev) = current_doc.take() {
                            push_punct(
                                &mut stream,
                                crate::executor::stream_event::Punctuation::document_close(prev),
                            )?;
                        }
                        // Pre-scan declared envelope sections for the new
                        // file. Sources without `envelope:` config pass
                        // an empty section map; the default trait impl
                        // also returns an empty section map. Sections
                        // populate before the first body record's
                        // punctuation emits so every record carries the
                        // same fully-populated context.
                        let envelope_sections = match src_cfg.envelope.as_ref() {
                            Some(cfg) => src_reader.prepare_document(cfg).map_err(|e| {
                                PipelineError::Internal {
                                    op: "envelope-pre-scan",
                                    node: src_cfg.name.clone(),
                                    detail: e.to_string(),
                                }
                            })?,
                            None => indexmap::IndexMap::new(),
                        };
                        let new_ctx = Arc::new(clinker_record::DocumentContext::new(
                            clinker_record::DocumentId::next(),
                            Arc::clone(&file_arc),
                            envelope_sections,
                        ));
                        push_punct(
                            &mut stream,
                            crate::executor::stream_event::Punctuation::document_open(Arc::clone(
                                &new_ctx,
                            )),
                        )?;
                        current_doc = Some(new_ctx);
                    }
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
                    values.push(clinker_record::Value::String(
                        file_arc.as_ref().to_string().into_boxed_str(),
                    ));
                    values.push(clinker_record::Value::String(
                        source_name_arc.as_ref().to_string().into_boxed_str(),
                    ));
                    values.push(event_time_value);
                    let mut widened_record =
                        clinker_record::Record::new(Arc::clone(&widened_schema), values);
                    if let Some(ctx) = current_doc.as_ref() {
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
                Ok(None) => break,
                Err(other) => return Err(other.into()),
            }
        }
        // Close the last document (if any records were emitted).
        if let Some(last) = current_doc.take() {
            push_punct(
                &mut stream,
                crate::executor::stream_event::Punctuation::document_close(last),
            )?;
        }
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

/// Extract `Vec<FieldDef>` from `SourceConfig.schema` for fixed-width format.
///
/// Resolves `SchemaSource::Inline` or `SchemaSource::FilePath` to `Vec<FieldDef>`.
/// Returns a config validation error if schema is `None` (fixed-width requires
/// explicit schema with field definitions).
fn extract_field_defs(
    input: &crate::config::SourceConfig,
) -> Result<Vec<clinker_record::schema_def::FieldDef>, PipelineError> {
    let schema_source = input.schema.as_ref().ok_or_else(|| {
        PipelineError::Config(crate::config::ConfigError::Validation(
            "fixed-width format requires explicit schema with field definitions".into(),
        ))
    })?;
    let def = match schema_source {
        crate::config::SchemaSource::Inline(def) => def.clone(),
        crate::config::SchemaSource::FilePath(path) => {
            crate::schema::load_schema(std::path::Path::new(path)).map_err(|e| {
                PipelineError::Config(crate::config::ConfigError::Validation(format!(
                    "failed to load schema from '{path}': {e}",
                )))
            })?
        }
    };
    def.fields.ok_or_else(|| {
        PipelineError::Config(crate::config::ConfigError::Validation(
            "fixed-width schema must have 'fields' defined".into(),
        ))
    })
}

/// Build CSV reader config from optional CSV input options.
fn build_csv_reader_config(opts: Option<&crate::config::CsvInputOptions>) -> CsvReaderConfig {
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

/// Build JSON reader config from optional JSON input options and array paths.
fn build_json_reader_config(
    opts: Option<&crate::config::JsonInputOptions>,
    array_paths: Option<&[crate::config::ArrayPathConfig]>,
) -> JsonReaderConfig {
    let mut config = JsonReaderConfig::default();
    if let Some(opts) = opts {
        config.format = opts.format.as_ref().map(|f| match f {
            crate::config::JsonFormat::Array => JsonMode::Array,
            crate::config::JsonFormat::Ndjson => JsonMode::Ndjson,
            crate::config::JsonFormat::Object => JsonMode::Object,
        });
        config.record_path = opts.record_path.clone();
    }
    if let Some(paths) = array_paths {
        config.array_paths = paths
            .iter()
            .map(|p| ArrayPathSpec {
                path: p.path.clone(),
                mode: match p.mode {
                    crate::config::ArrayMode::Explode => ArrayPathMode::Explode,
                    crate::config::ArrayMode::Join => ArrayPathMode::Join,
                },
                separator: p.separator.clone().unwrap_or_else(|| ",".to_string()),
            })
            .collect();
    }
    config
}

/// Build XML reader config from optional XML input options and array paths.
fn build_xml_reader_config(
    opts: Option<&crate::config::XmlInputOptions>,
    array_paths: Option<&[crate::config::ArrayPathConfig]>,
) -> XmlReaderConfig {
    let mut config = XmlReaderConfig::default();
    if let Some(opts) = opts {
        config.record_path = opts.record_path.clone();
        if let Some(ref prefix) = opts.attribute_prefix {
            config.attribute_prefix = prefix.clone();
        }
        config.namespace_handling = match opts.namespace_handling {
            Some(crate::config::NamespaceHandling::Qualify) => NamespaceMode::Qualify,
            _ => NamespaceMode::Strip,
        };
    }
    if let Some(paths) = array_paths {
        config.array_paths = paths
            .iter()
            .map(|p| XmlArrayPath {
                path: p.path.clone(),
                mode: match p.mode {
                    crate::config::ArrayMode::Explode => XmlArrayMode::Explode,
                    crate::config::ArrayMode::Join => XmlArrayMode::Join,
                },
                separator: p.separator.clone().unwrap_or_else(|| ",".to_string()),
            })
            .collect();
    }
    config
}

/// Build fixed-width reader config from optional fixed-width input options.
fn build_fw_reader_config(
    opts: Option<&crate::config::FixedWidthInputOptions>,
) -> FixedWidthReaderConfig {
    let mut config = FixedWidthReaderConfig::default();
    if let Some(opts) = opts
        && let Some(ref sep) = opts.line_separator
    {
        config.line_separator = sep.clone();
    }
    config
}
