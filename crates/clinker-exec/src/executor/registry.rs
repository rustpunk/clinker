//! Output writer registry and the format-writer builders that back it.

use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};

use clinker_record::Schema;

use clinker_format::counting::{CountedFormatWriter, CountingWriter, SharedByteCounter};
use clinker_format::csv::writer::{CsvWriter, CsvWriterConfig, HeaderCapturingCsvWriter};
use clinker_format::edifact::writer::{EdifactWriter, EdifactWriterConfig};
use clinker_format::fixed_width::writer::{FixedWidthWriter, FixedWidthWriterConfig};
use clinker_format::hl7::writer::{Hl7Writer, Hl7WriterConfig};
use clinker_format::json::writer::{JsonOutputMode, JsonWriter, JsonWriterConfig};
use clinker_format::splitting::{OversizeGroupPolicy, SplitPolicy, SplittingWriter, WriterFactory};
use clinker_format::swift::writer::{SwiftWriter, SwiftWriterConfig};
use clinker_format::traits::FormatWriter;
use clinker_format::x12::Charset;
use clinker_format::x12::writer::{X12Writer, X12WriterConfig};
use clinker_format::xml::writer::{XmlWriter, XmlWriterConfig};
use clinker_plan::config::{OutputConfig, OutputFormat};
use clinker_plan::error::PipelineError;

/// Output writer registry. Holds two parallel maps:
///
/// - `single`: one writer per output name (the legacy shape; matches
///   one-Output-to-one-file pipelines).
/// - `fan_out`: per-source-file writers for outputs flagged
///   `fan_out_per_source_file` in the plan. Outer key is the output
///   name; inner key is the source-file `Arc<str>` (matching the
///   per-record path read from each record's `$source.file` engine-
///   stamped column).
///
/// Auto-converts from `HashMap<String, Box<dyn Write + Send>>` so
/// existing callers that don't need fan-out keep the simpler shape.
#[derive(Default)]
pub struct WriterRegistry {
    pub single: HashMap<String, Box<dyn Write + Send>>,
    pub fan_out: HashMap<String, HashMap<std::sync::Arc<str>, Box<dyn Write + Send>>>,
}

impl From<HashMap<String, Box<dyn Write + Send>>> for WriterRegistry {
    fn from(single: HashMap<String, Box<dyn Write + Send>>) -> Self {
        Self {
            single,
            fan_out: HashMap::new(),
        }
    }
}

/// Build a CsvWriterConfig from CSV output options and the top-level include_header flag.
fn build_csv_writer_config(
    opts: Option<&clinker_plan::config::CsvOutputOptions>,
    include_header: Option<bool>,
) -> CsvWriterConfig {
    let mut config = CsvWriterConfig::default();
    if let Some(h) = include_header {
        config.include_header = h;
    }
    if let Some(opts) = opts
        && let Some(ref d) = opts.delimiter
        && let Some(b) = d.as_bytes().first()
    {
        config.delimiter = *b;
    }
    config
}

/// Build a JsonWriterConfig from JSON output options.
fn build_json_writer_config(
    opts: Option<&clinker_plan::config::JsonOutputOptions>,
) -> JsonWriterConfig {
    let mut config = JsonWriterConfig::default();
    if let Some(opts) = opts {
        if let Some(ref fmt) = opts.format {
            config.format = match fmt {
                clinker_plan::config::JsonOutputFormat::Array => JsonOutputMode::Array,
                clinker_plan::config::JsonOutputFormat::Ndjson => JsonOutputMode::Ndjson,
            };
        }
        if let Some(pretty) = opts.pretty {
            config.pretty = pretty;
        }
    }
    config
}

/// Build an XmlWriterConfig from XML output options.
fn build_xml_writer_config(
    opts: Option<&clinker_plan::config::XmlOutputOptions>,
) -> XmlWriterConfig {
    let mut config = XmlWriterConfig::default();
    if let Some(opts) = opts {
        if let Some(ref root) = opts.root_element {
            config.root_element = root.clone();
        }
        if let Some(ref rec) = opts.record_element {
            config.record_element = rec.clone();
        }
    }
    config
}

fn build_fw_writer_config(
    opts: Option<&clinker_plan::config::FixedWidthOutputOptions>,
) -> FixedWidthWriterConfig {
    let mut config = FixedWidthWriterConfig::default();
    if let Some(opts) = opts
        && let Some(ref sep) = opts.line_separator
    {
        config.line_separator = sep.clone();
    }
    config
}

fn build_edifact_writer_config(
    opts: Option<&clinker_plan::config::EdifactOutputOptions>,
) -> EdifactWriterConfig {
    // `segment_newline` defaults to `true` (readable per-segment lines);
    // `write_una` to `false`. The struct literal expresses both up front
    // so an unset option falls through to the documented default.
    EdifactWriterConfig {
        interchange: opts.and_then(|o| o.interchange.clone()),
        interchange_from_doc: opts.and_then(|o| o.interchange_from_doc.clone()),
        message_type: opts.and_then(|o| o.message_type.clone()),
        write_una: opts.and_then(|o| o.write_una).unwrap_or(false),
        segment_newline: opts.and_then(|o| o.segment_newline).unwrap_or(true),
    }
}

fn build_x12_writer_config(
    opts: Option<&clinker_plan::config::X12OutputOptions>,
) -> Result<X12WriterConfig, clinker_format::FormatError> {
    // `segment_newline` defaults to `true` (readable per-segment lines).
    // The struct literal expresses it up front so an unset option falls
    // through to the documented default. An unset `encoding` defaults to
    // UTF-8; a declared one is resolved here so a bad name fails at writer
    // construction rather than mid-stream.
    let charset = match opts.and_then(|o| o.encoding.as_deref()) {
        Some(name) => Charset::from_name(name)?,
        None => Charset::default(),
    };
    Ok(X12WriterConfig {
        interchange: opts.and_then(|o| o.interchange.clone()),
        interchange_from_doc: opts.and_then(|o| o.interchange_from_doc.clone()),
        group_header: opts.and_then(|o| o.group_header.clone()),
        set_type: opts.and_then(|o| o.set_type.clone()),
        segment_newline: opts.and_then(|o| o.segment_newline).unwrap_or(true),
        charset,
    })
}

fn build_hl7_writer_config(
    opts: Option<&clinker_plan::config::Hl7OutputOptions>,
) -> Hl7WriterConfig {
    // `segment_newline` defaults to `true` (readable per-segment lines).
    Hl7WriterConfig {
        file_header: opts.and_then(|o| o.file_header.clone()),
        file_header_from_doc: opts.and_then(|o| o.file_header_from_doc.clone()),
        batch_header: opts.and_then(|o| o.batch_header.clone()),
        segment_newline: opts.and_then(|o| o.segment_newline).unwrap_or(true),
    }
}

fn build_swift_writer_config(
    opts: Option<&clinker_plan::config::SwiftOutputOptions>,
) -> SwiftWriterConfig {
    SwiftWriterConfig {
        basic_header: opts.and_then(|o| o.basic_header.clone()),
        basic_header_from_doc: opts.and_then(|o| o.basic_header_from_doc.clone()),
        app_header: opts.and_then(|o| o.app_header.clone()),
        app_header_from_doc: opts.and_then(|o| o.app_header_from_doc.clone()),
        user_header: opts.and_then(|o| o.user_header.clone()),
        user_header_from_doc: opts.and_then(|o| o.user_header_from_doc.clone()),
        trailer: opts.and_then(|o| o.trailer.clone()),
        trailer_from_doc: opts.and_then(|o| o.trailer_from_doc.clone()),
    }
}

/// Extract `Vec<FieldDef>` from an output config's schema for fixed-width format.
///
/// Fixed-width output requires explicit schema with field definitions specifying
/// field names, widths, and optionally start positions, justification, and padding.
fn extract_output_field_defs(
    output: &OutputConfig,
) -> Result<Vec<clinker_record::schema_def::FieldDef>, PipelineError> {
    let schema_source = output.schema.as_ref().ok_or_else(|| {
        PipelineError::Config(clinker_plan::config::ConfigError::Validation(
            "fixed-width output format requires explicit schema with field definitions".into(),
        ))
    })?;
    let def = match schema_source {
        clinker_plan::config::SchemaSource::Inline(def) => def.clone(),
        clinker_plan::config::SchemaSource::FilePath(path) => {
            clinker_plan::schema::load_schema(std::path::Path::new(path)).map_err(|e| {
                PipelineError::Config(clinker_plan::config::ConfigError::Validation(format!(
                    "failed to load output schema from '{path}': {e}",
                )))
            })?
        }
    };
    def.fields.ok_or_else(|| {
        PipelineError::Config(clinker_plan::config::ConfigError::Validation(
            "fixed-width output schema must have 'fields' defined".into(),
        ))
    })
}

/// Build a writer factory closure for the given output format.
///
/// The returned `WriterFactory` creates format writers wrapping a `CountingWriter`.
/// For CSV with `repeat_header`, the first call creates a `HeaderCapturingCsvWriter`
/// and subsequent calls replay the captured header via `write_preset_header`.
/// For fixed-width, the factory captures pre-resolved `FieldDef`s from the output schema.
/// Map the plan's `OutputEnvelopeConfig` onto the format-local
/// `OutputEnvelopeSpec` the writers consume, but only when the Output declares
/// `reconstruct_envelope: true`. Returns `None` (no framing) when the flag is
/// off, no envelope config is declared, or the declared config is empty — so
/// the flag-off path stays byte-identical.
fn resolve_envelope_spec(
    reconstruct_envelope: bool,
    cfg: Option<&clinker_plan::config::OutputEnvelopeConfig>,
) -> Option<clinker_format::OutputEnvelopeSpec> {
    if !reconstruct_envelope {
        return None;
    }
    let cfg = cfg?;
    if cfg.is_empty() {
        return None;
    }
    Some(clinker_format::OutputEnvelopeSpec {
        header_from_doc: cfg.header_from_doc.clone(),
        footer_from_doc: cfg.footer_from_doc.clone(),
        footer_record_count_field: cfg.footer_record_count_field.clone(),
    })
}

fn build_writer_factory(
    format: &OutputFormat,
    include_header: Option<bool>,
    repeat_header: bool,
    field_defs: Option<Vec<clinker_record::schema_def::FieldDef>>,
    include_engine_stamped: bool,
    reconstruct_envelope: bool,
) -> WriterFactory {
    match format {
        OutputFormat::Csv(opts) => {
            let mut csv_config = build_csv_writer_config(opts.as_ref(), include_header);
            csv_config.include_engine_stamped = include_engine_stamped;
            csv_config.envelope = resolve_envelope_spec(
                reconstruct_envelope,
                opts.as_ref().and_then(|o| o.envelope.as_ref()),
            );
            if repeat_header {
                let shared_header: Arc<Mutex<Option<Vec<Box<str>>>>> = Arc::new(Mutex::new(None));
                let call_count = Arc::new(AtomicU32::new(0));
                Box::new(move |counting_writer, schema| {
                    let seq = call_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let inner_csv =
                        CsvWriter::new(counting_writer, schema.clone(), csv_config.clone());
                    if seq == 0 {
                        // First file: capture header from first record
                        Ok(Box::new(HeaderCapturingCsvWriter::new(
                            inner_csv,
                            schema,
                            Arc::clone(&shared_header),
                        )))
                    } else {
                        // Subsequent files: replay captured header
                        let mut csv = inner_csv;
                        if let Some(header) = shared_header.lock().unwrap().as_ref() {
                            csv.write_preset_header(header)?;
                        }
                        Ok(Box::new(csv))
                    }
                })
            } else {
                Box::new(move |counting_writer, schema| {
                    Ok(Box::new(CsvWriter::new(
                        counting_writer,
                        schema,
                        csv_config.clone(),
                    )))
                })
            }
        }
        OutputFormat::Json(opts) => {
            let mut json_config = build_json_writer_config(opts.as_ref());
            json_config.include_engine_stamped = include_engine_stamped;
            json_config.envelope = resolve_envelope_spec(
                reconstruct_envelope,
                opts.as_ref().and_then(|o| o.envelope.as_ref()),
            );
            Box::new(move |counting_writer, schema| {
                Ok(Box::new(JsonWriter::new(
                    counting_writer,
                    schema,
                    json_config.clone(),
                )))
            })
        }
        OutputFormat::Xml(opts) => {
            let mut xml_config = build_xml_writer_config(opts.as_ref());
            xml_config.include_engine_stamped = include_engine_stamped;
            xml_config.envelope = resolve_envelope_spec(
                reconstruct_envelope,
                opts.as_ref().and_then(|o| o.envelope.as_ref()),
            );
            Box::new(move |counting_writer, schema| {
                Ok(Box::new(XmlWriter::new(
                    counting_writer,
                    schema,
                    xml_config.clone(),
                )))
            })
        }
        OutputFormat::FixedWidth(opts) => {
            let mut fw_config = build_fw_writer_config(opts.as_ref());
            fw_config.envelope = resolve_envelope_spec(
                reconstruct_envelope,
                opts.as_ref().and_then(|o| o.envelope.as_ref()),
            );
            let fields = field_defs.expect(
                "fixed-width writer factory requires field_defs — \
                 build_format_writer must validate schema before calling",
            );
            Box::new(move |counting_writer, _schema| {
                Ok(Box::new(FixedWidthWriter::new(
                    counting_writer,
                    fields.clone(),
                    fw_config.clone(),
                )?))
            })
        }
        OutputFormat::Edifact(opts) => {
            let edi_config = build_edifact_writer_config(opts.as_ref());
            Box::new(move |counting_writer, schema| {
                Ok(Box::new(EdifactWriter::new(
                    counting_writer,
                    schema,
                    edi_config.clone(),
                )))
            })
        }
        OutputFormat::X12(opts) => {
            // Resolve the writer config (including charset) once here so the
            // factory only clones an already-validated config and a bad
            // `encoding` name surfaces deterministically, before any output
            // bytes. For a non-split sink the factory runs at executor setup,
            // so the error is a startup failure; for a split sink the
            // SplittingWriter lazy-opens the factory, so it surfaces when the
            // first per-split writer is built (at the first record) — still
            // pre-output and never a corrupt interchange, just not at setup.
            let x12_config = build_x12_writer_config(opts.as_ref());
            Box::new(move |counting_writer, schema| {
                let config = x12_config
                    .as_ref()
                    .map_err(|e| clinker_format::FormatError::X12(e.to_string()))?;
                Ok(Box::new(X12Writer::new(
                    counting_writer,
                    schema,
                    config.clone(),
                )))
            })
        }
        OutputFormat::Hl7(opts) => {
            let hl7_config = build_hl7_writer_config(opts.as_ref());
            Box::new(move |counting_writer, schema| {
                Ok(Box::new(Hl7Writer::new(
                    counting_writer,
                    schema,
                    hl7_config.clone(),
                )))
            })
        }
        OutputFormat::Swift(opts) => {
            let swift_config = build_swift_writer_config(opts.as_ref());
            Box::new(move |counting_writer, schema| {
                Ok(Box::new(SwiftWriter::new(
                    counting_writer,
                    schema,
                    swift_config.clone(),
                )))
            })
        }
    }
}

/// Build a format writer for an output config, handling both split and non-split paths.
///
/// For split outputs: creates a `SplittingWriter` with a file factory and writer factory.
/// For non-split outputs: creates a single writer wrapped in `CountedFormatWriter`.
pub(crate) fn build_format_writer(
    output: &OutputConfig,
    raw_writer: Box<dyn Write + Send>,
    schema: Arc<Schema>,
) -> Result<Box<dyn FormatWriter>, PipelineError> {
    // Extract field definitions for fixed-width output (requires explicit schema).
    let field_defs = if matches!(output.format, OutputFormat::FixedWidth(_)) {
        Some(extract_output_field_defs(output)?)
    } else {
        None
    };

    let repeat_header = output.split.as_ref().is_some_and(|s| s.repeat_header);
    let writer_factory = build_writer_factory(
        &output.format,
        output.include_header,
        repeat_header,
        field_defs,
        output.include_correlation_keys,
        output.reconstruct_envelope,
    );

    if let Some(ref split) = output.split {
        let policy = build_split_policy(split);
        let output_path = output.path.clone();
        let naming = split.naming.clone();
        let if_exists = output.if_exists;
        let unique_suffix_width = output.unique_suffix_width;

        let file_factory: clinker_format::splitting::FileFactory =
            Box::new(move |seq: u32| -> std::io::Result<Box<dyn Write + Send>> {
                let bare = std::path::PathBuf::from(apply_split_naming(&output_path, &naming, seq));
                let path_for_n = |n: Option<u64>| -> Result<
                    std::path::PathBuf,
                    clinker_plan::config::ConfigError,
                > {
                    Ok(match n {
                        None => bare.clone(),
                        Some(k) => {
                            let suffix = if unique_suffix_width == 0 {
                                format!("-{k}")
                            } else {
                                format!("-{:0>width$}", k, width = unique_suffix_width as usize)
                            };
                            crate::output::open::append_suffix_before_ext(&bare, &suffix)
                        }
                    })
                };
                let (_path, file) = crate::output::open::open_output(if_exists, false, path_for_n)
                    .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
                Ok(Box::new(BufWriter::with_capacity(65536, file)))
            });

        // SplittingWriter creates its own files; don't use raw_writer.
        drop(raw_writer);

        Ok(Box::new(SplittingWriter::new(
            file_factory,
            writer_factory,
            schema,
            policy,
        )))
    } else {
        let buf_writer = BufWriter::with_capacity(65536, raw_writer);
        let counter = SharedByteCounter::new();
        let counting_writer = CountingWriter::new(
            Box::new(buf_writer) as Box<dyn Write + Send>,
            counter.clone(),
        );
        let inner = writer_factory(counting_writer, schema).map_err(PipelineError::Format)?;
        Ok(Box::new(CountedFormatWriter::new(inner, counter)))
    }
}

/// Convert serde `SplitConfig` to runtime `SplitPolicy`.
fn build_split_policy(split: &clinker_plan::config::SplitConfig) -> SplitPolicy {
    SplitPolicy {
        max_records: split.max_records,
        max_bytes: split.max_bytes,
        group_key: split.group_key.clone(),
        repeat_header: split.repeat_header,
        oversize_group: match split.oversize_group {
            clinker_plan::config::SplitOversizeGroupPolicy::Warn => OversizeGroupPolicy::Warn,
            clinker_plan::config::SplitOversizeGroupPolicy::Error => OversizeGroupPolicy::Error,
            clinker_plan::config::SplitOversizeGroupPolicy::Allow => OversizeGroupPolicy::Allow,
        },
    }
}

/// Apply `{stem}_{seq:04}.{ext}` naming pattern to an output path.
fn apply_split_naming(base_path: &str, naming: &str, seq: u32) -> String {
    let path = std::path::Path::new(base_path);
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("output");
    let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("dat");
    let parent = path.parent().unwrap_or(std::path::Path::new(""));

    let filename = naming
        .replace("{stem}", stem)
        .replace("{ext}", ext)
        .replace("{seq:04}", &format!("{seq:04}"));

    // Join with a forward slash, not the OS separator: split output paths
    // must be byte-identical across platforms — Windows `Path::join` would
    // emit `\`, diverging from the forward-slash paths authored in config.
    match parent.to_str() {
        Some(p) if !p.is_empty() => format!("{p}/{filename}"),
        _ => filename,
    }
}
