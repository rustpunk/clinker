//! Explicit writer headroom for operator-pressure pipeline fixtures.

#[path = "resource_fixtures.rs"]
#[allow(dead_code)] // Some targets only use the predecoded source fixtures.
mod writer_workspace;

pub use writer_workspace::csv_workspace_headroom;

/// Add separately measured CSV workspace to an operator-pressure fixture.
/// Call only for success/spill tests, never to relax an expected resource error.
#[allow(dead_code)] // Some targets only use the predecoded source fixtures.
pub fn add_csv_workspace(
    config: &mut clinker_plan::config::PipelineConfig,
    context: &clinker_plan::config::CompileContext,
) {
    let plan = config.compile(context).expect("compile workspace fixture");
    let schema = plan
        .dag()
        .graph
        .node_weights()
        .filter_map(|node| node.stored_output_schema())
        .max_by_key(|schema| schema.column_count())
        .expect("fixture has a stored schema")
        .clone();
    let sample = clinker_record::Record::new(
        schema.clone(),
        vec![clinker_record::Value::Null; schema.column_count()],
    );
    let writers = config
        .sink_configs()
        .filter(|sink| matches!(sink.format, clinker_plan::config::OutputFormat::Csv(_)))
        .count() as u64;
    let operator_budget = clinker_plan::config::utils::parse_memory_limit_bytes(
        config.pipeline.memory.limit.as_deref(),
    )
    .expect("fixture operator budget");
    let workspace = csv_workspace_headroom(&sample) * writers;
    config.pipeline.memory.limit = Some((operator_budget + workspace).to_string());
}

/// Decode valid CSV fixtures before the run so a downstream pressure test owns
/// its entire runtime budget. This does not exercise whole-input CSV admission.
/// Records retain the compiled source's types/projection and the supplied physical
/// file identities, including open/close events for empty files. Envelope and
/// malformed-input tests must keep the byte-stream source instead.
#[allow(dead_code)] // Each integration target uses only its required fixture helpers.
pub fn predecoded_csv_source(
    config: &clinker_plan::config::PipelineConfig,
    context: &clinker_plan::config::CompileContext,
    source_name: &str,
    files: &[(&str, &str)],
) -> clinker_exec::source::SourceInput {
    use clinker_exec::source::RecordSource;
    use clinker_format::{FormatError, SourceLifecycleEvent};
    use clinker_record::{Record, Schema, owned_storage::SharedStorage};
    use std::sync::Arc;

    struct DecodedFile {
        path: Arc<str>,
        rows: std::vec::IntoIter<Record>,
    }

    struct DecodedCsv {
        schema: SharedStorage<Schema>,
        files: std::vec::IntoIter<DecodedFile>,
        active: Option<DecodedFile>,
        current_file: Option<Arc<str>>,
        events: Vec<SourceLifecycleEvent>,
    }

    impl DecodedCsv {
        fn advance(&mut self) -> bool {
            if let Some(file) = self.active.take() {
                self.events
                    .push(SourceLifecycleEvent::PhysicalFileClose(file.path));
            }
            self.active = self.files.next();
            if let Some(file) = &self.active {
                self.current_file = Some(file.path.clone());
                self.events
                    .push(SourceLifecycleEvent::PhysicalFileOpen(file.path.clone()));
            }
            self.active.is_some()
        }
    }

    impl RecordSource for DecodedCsv {
        fn schema(&mut self) -> Result<SharedStorage<Schema>, FormatError> {
            Ok(self.schema.clone())
        }

        fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
            while let Some(file) = &mut self.active {
                if let Some(row) = file.rows.next() {
                    return Ok(Some(row));
                }
                self.advance();
            }
            Ok(None)
        }

        fn current_source_file(&self) -> Option<&Arc<str>> {
            self.current_file.as_ref()
        }

        fn take_source_lifecycle_events(&mut self) -> Vec<SourceLifecycleEvent> {
            std::mem::take(&mut self.events)
        }

        fn advance_to_next_file(&mut self) -> Result<bool, FormatError> {
            Ok(self.advance())
        }
    }

    let compiled = config.compile(context).expect("compile CSV fixture source");
    let body = compiled
        .config()
        .nodes
        .iter()
        .find_map(|node| match &node.value {
            clinker_plan::config::PipelineNode::Source { header, config }
                if header.name == source_name =>
            {
                Some(config)
            }
            _ => None,
        })
        .expect("fixture source exists in bound config");
    assert!(matches!(
        body.source.format,
        clinker_plan::config::InputFormat::Csv(_)
    ));
    assert!(
        body.source.envelope.is_none(),
        "pressure fixtures have no envelope"
    );
    let mut schema: Option<SharedStorage<Schema>> = None;
    let files = files
        .iter()
        .map(|(path, csv)| {
            let mut reader = clinker_exec::executor::build_source_format_reader(
                &body.source,
                &body.schema,
                body.on_unmapped.clone(),
                clinker_format::ReopenableSource::one_shot(Box::new(std::io::Cursor::new(
                    csv.as_bytes().to_vec(),
                ))),
                None,
            )
            .expect("construct CSV fixture decoder");
            let file_schema = reader.schema().expect("decode CSV fixture schema");
            if let Some(schema) = &schema {
                assert_eq!(
                    schema.columns(),
                    file_schema.columns(),
                    "fixture files must share columns"
                );
            } else {
                schema = Some(file_schema);
            }
            let mut rows = Vec::new();
            while let Some(row) = reader.next_record().expect("decode valid CSV fixture row") {
                rows.push(row);
            }
            DecodedFile {
                path: Arc::from(*path),
                rows: rows.into_iter(),
            }
        })
        .collect::<Vec<_>>()
        .into_iter();
    let mut source = DecodedCsv {
        schema: schema.expect("CSV fixture must supply at least one file"),
        files,
        active: None,
        current_file: None,
        events: Vec::new(),
    };
    source.advance();
    clinker_exec::source::SourceInput::Records(Box::new(source))
}

/// Single-file convenience retaining each source's configured physical path.
#[allow(dead_code)] // Each integration target uses only its required fixture helpers.
pub fn predecoded_csv_readers(
    config: &clinker_plan::config::PipelineConfig,
    context: &clinker_plan::config::CompileContext,
    entries: &[(&str, &str)],
) -> clinker_exec::executor::SourceReaders {
    entries
        .iter()
        .map(|(name, csv)| {
            let path = config
                .source_configs()
                .find(|source| source.name == *name)
                .and_then(|source| source.path.as_deref())
                .expect("single-file fixture source has a configured path");
            (
                (*name).to_string(),
                predecoded_csv_source(config, context, name, &[(path, csv)]),
            )
        })
        .collect()
}
