//! Output writer registry and the format-writer builders that back it.

use clinker_record::owned_storage::SharedStorage;
use std::collections::HashMap;
use std::io::{BufWriter, Write};
#[cfg(test)]
use std::sync::Arc;

use clinker_record::Schema;

use clinker_format::counting::{CountedFormatWriter, CountingWriter, SharedByteCounter};
use clinker_format::csv::writer::{
    CsvEncoder, CsvEncoderConfig, CsvEncoderOptions, CsvHeaderCapture, CsvWriterConfig,
};
use clinker_format::edifact::writer::{EdifactWriter, EdifactWriterConfig};
use clinker_format::fixed_width::writer::{FixedWidthWriter, FixedWidthWriterConfig};
use clinker_format::hl7::writer::{Hl7Writer, Hl7WriterConfig};
use clinker_format::json::writer::{
    JsonEncoder, JsonEncoderConfig, JsonOutputMode, JsonWriterConfig,
};
use clinker_format::preparation::WriterResources;
use clinker_format::splitting::{OversizeGroupPolicy, SplitPolicy, SplittingWriter, WriterFactory};
use clinker_format::swift::writer::{SwiftWriter, SwiftWriterConfig};
#[cfg(test)]
use clinker_format::traits::FormatWriter;
use clinker_format::traits::FormatWriterHandle;
use clinker_format::x12::Charset;
use clinker_format::x12::writer::{X12Writer, X12WriterConfig};
use clinker_format::xml::writer::{XmlEncoder, XmlEncoderConfig, XmlEncoderOptions};
use clinker_plan::config::{OutputFormat, SinkConfig};
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
pub struct WriterRegistry {
    pub single: HashMap<String, Box<dyn Write + Send>>,
    pub fan_out: HashMap<String, HashMap<std::sync::Arc<str>, Box<dyn Write + Send>>>,
    /// Per-source resolved base path paired with [`Self::fan_out`]. Split
    /// writers use it when lazily naming each source's segment sequence.
    pub fan_out_paths: HashMap<String, HashMap<std::sync::Arc<str>, String>>,
    /// Shared ledger used when split writers lazily open destination-local
    /// hidden files. The CLI retains a clone and publishes after the run.
    pub output_staging: crate::output::staging::OutputStagingRegistry,
    /// Standalone executor callers have no outer publication owner, so their
    /// registry commits staged split files after every writer has closed.
    pub auto_commit_staged: bool,
}

impl Default for WriterRegistry {
    fn default() -> Self {
        Self {
            single: HashMap::new(),
            fan_out: HashMap::new(),
            fan_out_paths: HashMap::new(),
            output_staging: crate::output::staging::OutputStagingRegistry::default(),
            auto_commit_staged: true,
        }
    }
}

impl From<HashMap<String, Box<dyn Write + Send>>> for WriterRegistry {
    fn from(single: HashMap<String, Box<dyn Write + Send>>) -> Self {
        Self {
            single,
            fan_out: HashMap::new(),
            ..Self::default()
        }
    }
}

/// Build a `CsvWriterConfig` from CSV output options and the top-level
/// `include_header` flag.
///
/// Errors when a configured `delimiter` is not exactly one ASCII byte,
/// mirroring the reader-side guard so an empty, multi-character, or non-ASCII
/// value fails before any output bytes are written rather than being
/// truncated to its first byte.
fn build_csv_writer_config(
    opts: Option<&clinker_plan::config::CsvOutputOptions>,
    include_header: Option<bool>,
) -> Result<CsvWriterConfig, PipelineError> {
    let mut config = CsvWriterConfig::default();
    if let Some(h) = include_header {
        config.include_header = h;
    }
    if let Some(opts) = opts
        && let Some(ref d) = opts.delimiter
    {
        config.delimiter = crate::executor::util::csv_single_byte("delimiter", d)?;
    }
    Ok(config)
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

/// Extract the fixed-width output column list from an output config's `schema:`.
///
/// Fixed-width output requires an explicit single-record column list specifying
/// names, widths, and optionally start positions, justification, and padding.
fn extract_output_field_defs(
    output: &SinkConfig,
) -> Result<Vec<clinker_format::Column>, PipelineError> {
    let schema = output.schema.as_ref().ok_or_else(|| {
        PipelineError::Config(clinker_plan::config::ConfigError::Validation(
            "fixed-width output format requires an explicit `schema:` column list".into(),
        ))
    })?;
    // Resolve an external `.schema.yaml` (File) to its inline form.
    let resolved_file;
    let schema = match schema {
        clinker_format::SourceSchema::File(path) => {
            resolved_file = clinker_plan::schema::load_source_schema(std::path::Path::new(path))
                .map_err(|e| {
                    PipelineError::Config(clinker_plan::config::ConfigError::Validation(format!(
                        "failed to load output schema from '{path}': {e}",
                    )))
                })?;
            &resolved_file
        }
        other => other,
    };
    schema.as_columns().map(<[_]>::to_vec).ok_or_else(|| {
        PipelineError::Config(clinker_plan::config::ConfigError::Validation(
            "fixed-width output schema must be a single-record column list (not a multi-record \
             or generated schema)"
                .into(),
        ))
    })
}

/// Build a writer factory closure for the given output format.
///
/// The returned `WriterFactory` creates format writers wrapping a `CountingWriter`.
/// CSV factories share admitted configuration and header capture. Only successful
/// first-body delivery publishes a header; replay joins each later first body
/// in one prepared operation, with no eager factory output.
/// For fixed-width, the factory captures pre-resolved `Column`s from the output schema.
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
    let spec = clinker_format::OutputEnvelopeSpec::from(cfg?);
    (!spec.is_empty()).then_some(spec)
}

struct CsvFactoryState {
    config: CsvEncoderConfig,
    // Only non-repeating split output needs a second admitted policy. The
    // first file still owns preparation/retry of its automatic header.
    subsequent_config: Option<CsvEncoderConfig>,
    opened: std::cell::Cell<bool>,
    capture: Option<CsvHeaderCapture>,
    resources: WriterResources,
}
impl CsvFactoryState {
    fn build(
        &self,
        destination: CountingWriter<Box<dyn Write + Send>>,
        schema: SharedStorage<Schema>,
    ) -> Result<FormatWriterHandle, clinker_format::FormatError> {
        let config = if self.opened.get() {
            self.subsequent_config.as_ref().unwrap_or(&self.config)
        } else {
            &self.config
        };
        let encoder = CsvEncoder::from_config(schema, config.clone(), self.resources.clone())?;
        let encoder = match &self.capture {
            Some(capture) => encoder.with_header_capture(capture.clone()),
            None => encoder,
        };
        let writer = encoder.into_boxed_writer(destination, self.resources.clone())?;
        self.opened.set(true);
        Ok(writer)
    }
}

fn build_writer_factory(
    output: &SinkConfig,
    repeat_header: bool,
    field_defs: Option<Vec<clinker_format::Column>>,
    resources: WriterResources,
) -> Result<WriterFactory, PipelineError> {
    // Every per-format config field derives from the Sink config; bind them
    // once so the format arms below read them by their original names.
    let include_header = output.include_header;
    let include_engine_stamped = output.include_correlation_keys;
    let preserve_nulls = output.preserve_nulls.unwrap_or(false);
    let reconstruct_envelope = output.reconstruct_envelope;
    let include_unmapped = output.include_unmapped;
    match &output.format {
        OutputFormat::Csv(opts) => {
            let mut csv_config = build_csv_writer_config(opts.as_ref(), include_header)?;
            csv_config.include_engine_stamped = include_engine_stamped;
            // Lossless mode: when the Output carries every column through
            // (`include_unmapped: true`), a record column the pinned header
            // lacks must fail loudly rather than be silently dropped. The
            // buffered Output arm pre-widens the header to the batch union so
            // this never trips there; it is the drift backstop on the
            // bounded-memory paths (streaming fusion, envelope framing) that
            // pin the header to the first record (issue #805).
            csv_config.error_on_undeclared_columns = include_unmapped;
            let envelope = reconstruct_envelope
                .then(|| opts.as_ref().and_then(|o| o.envelope.as_ref()))
                .flatten();
            let mut options = CsvEncoderOptions::from(&csv_config);
            options.charset = opts
                .as_ref()
                .and_then(|o| o.encoding.as_deref())
                .map(|name| Charset::from_output_name(name, "CSV"))
                .transpose()
                .map_err(PipelineError::Format)?
                .unwrap_or_default();
            options.join_values = output.join_values.as_deref().unwrap_or(&[]);
            options.declared_multiple = &output.declared_multiple;
            options.envelope_header = envelope.and_then(|e| e.header_from_doc.as_deref());
            options.envelope_footer = envelope.and_then(|e| e.footer_from_doc.as_deref());
            options.envelope_count = envelope.and_then(|e| e.footer_record_count_field.as_deref());
            let subsequent_config = (output.split.is_some()
                && !repeat_header
                && options.include_header
                && options.envelope_header.is_none()
                && options.envelope_footer.is_none()
                && options.envelope_count.is_none())
            .then(|| {
                CsvEncoderConfig::new(
                    CsvEncoderOptions {
                        include_header: false,
                        ..options
                    },
                    &resources,
                )
            })
            .transpose()
            .map_err(PipelineError::Format)?;
            let csv_config =
                CsvEncoderConfig::new(options, &resources).map_err(PipelineError::Format)?;
            let capture = repeat_header
                .then(|| CsvHeaderCapture::new(&resources))
                .transpose()
                .map_err(PipelineError::Format)?;
            let scope = resources
                .scope()
                .map_err(|error| PipelineError::Format(error.into()))?;
            let state = CsvFactoryState {
                config: csv_config,
                subsequent_config,
                opened: std::cell::Cell::new(false),
                capture,
                resources,
            };
            let factory = move |counting_writer, schema| state.build(counting_writer, schema);
            WriterFactory::try_new(factory, scope.allocation())
                .map_err(|error| PipelineError::Format(error.into()))
        }
        OutputFormat::Json(opts) => {
            let mut json_config = build_json_writer_config(opts.as_ref());
            json_config.include_engine_stamped = include_engine_stamped;
            json_config.preserve_nulls = preserve_nulls;
            let envelope = reconstruct_envelope
                .then(|| opts.as_ref().and_then(|o| o.envelope.as_ref()))
                .flatten();
            let config = JsonEncoderConfig::from_names(
                &json_config,
                envelope.and_then(|e| e.header_from_doc.as_deref()),
                envelope.and_then(|e| e.footer_from_doc.as_deref()),
                envelope.and_then(|e| e.footer_record_count_field.as_deref()),
                &resources,
            )
            .map_err(PipelineError::Format)?;
            let scope = resources
                .scope()
                .map_err(|error| PipelineError::Format(error.into()))?;
            let factory = move |counting_writer, schema| {
                JsonEncoder::from_config(schema, config.clone())?
                    .into_boxed_writer(counting_writer, resources.clone())
            };
            WriterFactory::try_new(factory, scope.allocation())
                .map_err(|error| PipelineError::Format(error.into()))
        }
        OutputFormat::Xml(opts) => {
            let options = opts.as_ref();
            let envelope = reconstruct_envelope
                .then(|| options.and_then(|o| o.envelope.as_ref()))
                .flatten();
            let config = XmlEncoderConfig::new(
                XmlEncoderOptions {
                    root_element: options
                        .and_then(|o| o.root_element.as_deref())
                        .unwrap_or("Root"),
                    record_element: options
                        .and_then(|o| o.record_element.as_deref())
                        .unwrap_or("Record"),
                    attribute_prefix: options
                        .and_then(|o| o.attribute_prefix.as_deref())
                        .unwrap_or("@"),
                    preserve_nulls,
                    include_engine_stamped,
                    join_values: output.join_values.as_deref().unwrap_or_default(),
                    declared_multiple: &output.declared_multiple,
                    envelope_header: envelope.and_then(|e| e.header_from_doc.as_deref()),
                    envelope_footer: envelope.and_then(|e| e.footer_from_doc.as_deref()),
                    envelope_count: envelope.and_then(|e| e.footer_record_count_field.as_deref()),
                },
                &resources,
            )
            .map_err(PipelineError::Format)?;
            let scope = resources
                .scope()
                .map_err(|error| PipelineError::Format(error.into()))?;
            let factory = move |counting_writer, schema| {
                XmlEncoder::from_config(schema, config.clone())?
                    .into_boxed_writer(counting_writer, resources.clone())
            };
            WriterFactory::try_new(factory, scope.allocation())
                .map_err(|error| PipelineError::Format(error.into()))
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
            Ok(WriterFactory::from_legacy(
                move |counting_writer, _schema| {
                    Ok(FormatWriterHandle::from_legacy(Box::new(
                        FixedWidthWriter::new(counting_writer, fields.clone(), fw_config.clone())?,
                    )))
                },
            ))
        }
        OutputFormat::Edifact(opts) => {
            let edi_config = build_edifact_writer_config(opts.as_ref());
            Ok(WriterFactory::from_legacy(
                move |counting_writer, schema| {
                    Ok(FormatWriterHandle::from_legacy(Box::new(
                        EdifactWriter::new(counting_writer, schema, edi_config.clone()),
                    )))
                },
            ))
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
            Ok(WriterFactory::from_legacy(
                move |counting_writer, schema| {
                    let config = x12_config
                        .as_ref()
                        .map_err(|e| clinker_format::FormatError::X12(e.to_string()))?;
                    Ok(FormatWriterHandle::from_legacy(Box::new(X12Writer::new(
                        counting_writer,
                        schema,
                        config.clone(),
                    ))))
                },
            ))
        }
        OutputFormat::Hl7(opts) => {
            let hl7_config = build_hl7_writer_config(opts.as_ref());
            Ok(WriterFactory::from_legacy(
                move |counting_writer, schema| {
                    Ok(FormatWriterHandle::from_legacy(Box::new(Hl7Writer::new(
                        counting_writer,
                        schema,
                        hl7_config.clone(),
                    ))))
                },
            ))
        }
        OutputFormat::Swift(opts) => {
            let swift_config = build_swift_writer_config(opts.as_ref());
            Ok(WriterFactory::from_legacy(
                move |counting_writer, schema| {
                    Ok(FormatWriterHandle::from_legacy(Box::new(SwiftWriter::new(
                        counting_writer,
                        schema,
                        swift_config.clone(),
                    ))))
                },
            ))
        }
    }
}

/// Build a format writer for an output config, handling both split and non-split paths.
///
/// For split outputs: creates a `SplittingWriter` with a file factory and writer factory.
/// For non-split outputs: creates a single writer wrapped in `CountedFormatWriter`.
pub(crate) fn build_format_writer(
    output: &SinkConfig,
    raw_writer: Box<dyn Write + Send>,
    schema: SharedStorage<Schema>,
    output_staging: crate::output::staging::OutputStagingRegistry,
    sink_byte_counter: Option<SharedByteCounter>,
    resources: WriterResources,
) -> Result<FormatWriterHandle, PipelineError> {
    // Extract field definitions for fixed-width output (requires explicit schema).
    let field_defs = if matches!(output.format, OutputFormat::FixedWidth(_)) {
        Some(extract_output_field_defs(output)?)
    } else {
        None
    };

    let repeat_header = output.split.as_ref().is_some_and(|s| s.repeat_header);
    let scope = resources
        .scope()
        .map_err(|error| PipelineError::Format(error.into()))?;
    let writer_factory = build_writer_factory(output, repeat_header, field_defs, resources)?;
    // Prepared codecs already batch complete operations. An additional
    // BufWriter would count bytes before delivery and retry poison on drop.
    let prepared_output = matches!(
        output.format,
        OutputFormat::Csv(_) | OutputFormat::Json(_) | OutputFormat::Xml(_)
    );

    if let Some(ref split) = output.split {
        let policy = build_split_policy(split);
        let output_path = output.path.clone();
        let naming = clinker_plan::config::SplitNaming::parse(&split.naming)
            .map_err(PipelineError::Config)?;
        let if_exists = output.if_exists;
        let unique_suffix_width = output.unique_suffix_width;
        let output_name = output.name.clone();
        let sink_byte_counter_for_files = sink_byte_counter.clone();

        let file_factory: clinker_format::splitting::FileFactory =
            Box::new(move |seq: u32| -> std::io::Result<Box<dyn Write + Send>> {
                let bare = std::path::PathBuf::from(naming.render(&output_path, seq));
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
                let staged = if output_staging.has_run_attempt() {
                    output_staging.stage_attempt_output(
                        crate::output::attempt::ArtifactKind::Split,
                        output_name.clone(),
                        if_exists,
                        false,
                        path_for_n,
                    )
                } else {
                    output_staging.stage_output(output_name.clone(), if_exists, false, path_for_n)
                };
                let (_path, file) = staged.map_err(|e| std::io::Error::other(format!("{e:?}")))?;
                let buffered: Box<dyn Write + Send> = if prepared_output {
                    Box::new(file)
                } else {
                    Box::new(BufWriter::with_capacity(65536, file))
                };
                Ok(match &sink_byte_counter_for_files {
                    Some(counter) => Box::new(CountingWriter::new(buffered, counter.clone())),
                    None => buffered,
                })
            });

        // SplittingWriter creates its own files; don't use raw_writer.
        drop(raw_writer);

        FormatWriterHandle::try_new(
            SplittingWriter::new(file_factory, writer_factory, schema, policy),
            scope.allocation(),
        )
        .map_err(|error| PipelineError::Format(error.into()))
    } else {
        let buf_writer: Box<dyn Write + Send> = if prepared_output {
            raw_writer
        } else {
            Box::new(BufWriter::with_capacity(65536, raw_writer))
        };
        let counter = sink_byte_counter.unwrap_or_default();
        let counting_writer = CountingWriter::new(buf_writer, counter.clone());
        let inner = writer_factory
            .create(counting_writer, schema)
            .map_err(PipelineError::Format)?;
        FormatWriterHandle::try_new(CountedFormatWriter::new(inner, counter), scope.allocation())
            .map_err(|error| PipelineError::Format(error.into()))
    }
}

/// Convert serde `SplitConfig` to runtime `SplitPolicy`.
fn build_split_policy(split: &clinker_plan::config::SplitConfig) -> SplitPolicy {
    SplitPolicy {
        max_records: split.max_records,
        max_bytes: split.max_bytes,
        group_key: split.group_key.clone(),
        oversize_group: match split.oversize_group {
            clinker_plan::config::SplitOversizeGroupPolicy::Warn => OversizeGroupPolicy::Warn,
            clinker_plan::config::SplitOversizeGroupPolicy::Error => OversizeGroupPolicy::Error,
            clinker_plan::config::SplitOversizeGroupPolicy::Allow => OversizeGroupPolicy::Allow,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_plan::config::{CompileContext, parse_config};
    use clinker_record::{Record, Value};

    fn compiled_split_sink(format: &str, declared_multiple: bool) -> SinkConfig {
        let multiple = if declared_multiple {
            ", multiple: true"
        } else {
            ""
        };
        let yaml = format!(
            r#"
pipeline:
  name: split_multi_value_contract
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: in.json
      schema:
        - {{ name: tags, type: any{multiple} }}
  - type: sink
    name: out
    input: src
    config:
      name: out
      type: {format}
      path: out.{format}
      split:
        max_records: 1
"#
        );
        let config = parse_config(&yaml).expect("split pipeline parses");
        let plan = config
            .compile(&CompileContext::default())
            .expect("split pipeline compiles");
        plan.config()
            .sink_configs()
            .next()
            .expect("compiled plan has a Sink")
            .clone()
    }

    #[test]
    fn csv_factory_repeat_header_controls_actual_split_destinations() {
        for include_header in [true, false] {
            for repeat_header in [false, true] {
                let mut sink = compiled_split_sink("csv", false);
                sink.include_header = Some(include_header);
                let provider = clinker_format::preparation::MemoryOnlyResources::new(
                    std::num::NonZeroUsize::new(256 * 1024).unwrap(),
                );
                let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["tags".into()])));
                let outputs = [
                    clinker_bench_support::io::SharedBuffer::new(),
                    clinker_bench_support::io::SharedBuffer::new(),
                ];
                let files = outputs.clone();
                let factory =
                    build_writer_factory(&sink, repeat_header, None, provider.resources()).unwrap();
                let retained = provider.used();
                let scope = provider.resources().scope().unwrap();
                let pressure = scope
                    .reserve(std::alloc::Layout::array::<u8>(256 * 1024 - retained).unwrap())
                    .unwrap();
                let refused = factory.create(
                    CountingWriter::new(Box::new(outputs[0].clone()), SharedByteCounter::new()),
                    schema.clone(),
                );
                assert!(
                    refused.is_err(),
                    "failed construction must not consume the first-file policy"
                );
                drop(pressure);
                assert_eq!(provider.used(), retained);
                assert!(outputs[0].contents().is_empty());
                let mut writer = SplittingWriter::new(
                    Box::new(move |sequence| Ok(Box::new(files[sequence as usize - 1].clone()))),
                    factory,
                    schema.clone(),
                    SplitPolicy {
                        max_records: Some(1),
                        max_bytes: None,
                        group_key: None,
                        oversize_group: OversizeGroupPolicy::Error,
                    },
                );
                for value in [1, 2] {
                    writer
                        .write_record(&Record::new(schema.clone(), vec![Value::Integer(value)]))
                        .unwrap();
                }
                writer.flush().unwrap();
                assert_eq!(
                    outputs[0].contents(),
                    if include_header {
                        b"tags\n1\n".as_slice()
                    } else {
                        b"1\n".as_slice()
                    }
                );
                assert_eq!(
                    outputs[1].contents(),
                    if include_header && repeat_header {
                        b"tags\n2\n".as_slice()
                    } else {
                        b"2\n".as_slice()
                    }
                );
                drop(writer);
                assert_eq!(provider.used(), 0);
            }
        }
    }

    #[test]
    fn split_csv_and_xml_writers_enforce_compiled_multiple_columns() {
        for format in ["csv", "xml"] {
            for declared in [true, false] {
                let temp = tempfile::tempdir().expect("temp output directory");
                let mut sink = compiled_split_sink(format, declared);
                sink.path = temp
                    .path()
                    .join(format!("out.{format}"))
                    .display()
                    .to_string();
                let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["tags".into()])));
                let record = Record::new(
                    schema.clone(),
                    vec![Value::Array(
                        clinker_record::owned_storage::OwnedValues::from_vec(vec![
                            Value::String("a".into()),
                            Value::String("b".into()),
                        ]),
                    )],
                );
                let raw = Box::new(std::io::Cursor::new(Vec::<u8>::new())) as Box<dyn Write + Send>;
                let mut writer = build_format_writer(
                    &sink,
                    raw,
                    schema,
                    crate::output::staging::OutputStagingRegistry::default(),
                    None,
                    clinker_format::preparation::MemoryOnlyResources::new(
                        std::num::NonZeroUsize::new(1024 * 1024).unwrap(),
                    )
                    .resources(),
                )
                .expect("split writer builds");
                let result = writer.write_record(&record);
                if declared {
                    result.unwrap_or_else(|error| {
                        panic!("declared {format} array must write: {error}")
                    });
                    writer
                        .flush()
                        .unwrap_or_else(|error| panic!("{format} writer flushes: {error}"));
                } else {
                    let error = result.expect_err("undeclared split array must fail");
                    let message = error.to_string();
                    assert!(
                        message.contains(format.to_ascii_uppercase().as_str())
                            && message.contains(if format == "csv" { "field 1" } else { "tags" }),
                        "{message}"
                    );
                }
            }
        }
    }

    #[test]
    fn xml_factory_preserves_configured_and_default_names() {
        for custom in [false, true] {
            let mut sink = compiled_split_sink("xml", false);
            sink.format =
                OutputFormat::Xml(custom.then(|| clinker_plan::config::XmlOutputOptions {
                    root_element: Some("Batch".into()),
                    record_element: Some("Row".into()),
                    attribute_prefix: Some("_".into()),
                    ..Default::default()
                }));
            let provider = clinker_format::preparation::MemoryOnlyResources::new(
                std::num::NonZeroUsize::new(128 * 1024).unwrap(),
            );
            let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![if custom {
                "_id".into()
            } else {
                "@id".into()
            }])));
            let output = clinker_bench_support::io::SharedBuffer::new();
            let factory = build_writer_factory(&sink, true, None, provider.resources()).unwrap();
            let mut writer = factory
                .create(
                    CountingWriter::new(Box::new(output.clone()), SharedByteCounter::new()),
                    schema.clone(),
                )
                .unwrap();
            writer
                .write_record(&Record::new(schema, vec![Value::Integer(7)]))
                .unwrap();
            writer.flush().unwrap();
            assert_eq!(
                output.contents(),
                if custom {
                    b"<Batch><Row id=\"7\"></Row></Batch>".as_slice()
                } else {
                    b"<Root><Record id=\"7\"></Record></Root>".as_slice()
                }
            );
            drop(writer);
            drop(factory);
            assert_eq!(provider.used(), 0);
        }
    }

    #[test]
    fn csv_writer_config_defaults_delimiter_to_comma() {
        // With no options the writer keeps the RFC 4180 comma so an existing
        // pipeline's output is byte-identical.
        let config = build_csv_writer_config(None, None).expect("no options resolves");
        assert_eq!(config.delimiter, b',');
    }

    #[test]
    fn csv_factory_encoding_preserves_committed_bytes_after_rejection() {
        let mut sink = compiled_split_sink("csv", false);
        sink.split = None;
        sink.format = OutputFormat::Csv(Some(clinker_plan::config::CsvOutputOptions {
            encoding: Some("latin1".into()),
            ..Default::default()
        }));
        let output = clinker_bench_support::io::SharedBuffer::new();
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["caf\u{e9}".into()])));
        let resources = clinker_format::preparation::MemoryOnlyResources::new(
            std::num::NonZeroUsize::new(256 * 1024).unwrap(),
        );
        let mut writer = build_format_writer(
            &sink,
            Box::new(output.clone()),
            schema.clone(),
            crate::output::staging::OutputStagingRegistry::default(),
            None,
            resources.resources(),
        )
        .unwrap();
        let row = |text: &str| Record::new(schema.clone(), vec![Value::String(text.into())]);
        writer.write_record(&row("caf\u{e9}")).unwrap();
        let before = output.contents();
        assert!(writer.write_record(&row("late\u{20ac}")).is_err());
        assert_eq!(output.contents(), before);
        writer.write_record(&row("next")).unwrap();
        writer.flush().unwrap();
        drop(writer);
        assert_eq!(output.contents(), b"caf\xe9\ncaf\xe9\nnext\n");
        assert_eq!(resources.used(), 0);
    }

    #[test]
    fn csv_writer_config_resolves_single_byte_delimiter() {
        let opts = clinker_plan::config::CsvOutputOptions {
            delimiter: Some("|".into()),
            ..Default::default()
        };
        let config =
            build_csv_writer_config(Some(&opts), None).expect("single-byte delimiter resolves");
        assert_eq!(config.delimiter, b'|');
    }

    #[test]
    fn csv_writer_config_rejects_multichar_delimiter() {
        let opts = clinker_plan::config::CsvOutputOptions {
            delimiter: Some("||".into()),
            ..Default::default()
        };
        let Err(err) = build_csv_writer_config(Some(&opts), None) else {
            panic!("a multi-character output delimiter must fail config build");
        };
        let msg = err.to_string();
        assert!(
            msg.contains("delimiter") && msg.contains("one ASCII byte"),
            "error must name the option and the requirement: {msg}"
        );
    }
}
