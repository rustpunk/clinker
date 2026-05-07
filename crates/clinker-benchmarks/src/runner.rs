//! Benchmark pipeline runner.
//!
//! Executes benchmark pipeline configs against cached generated data.
//! Streaming: reads from cached file, writes to in-memory buffer.
//! Supports multi-source pipelines (combine, merge) and nested formats.

use std::collections::HashMap;
use std::io::{BufReader, Cursor};
use std::path::Path;

use clinker_bench_support::cache::{BenchDataCache, DataSpec, NestedWrapper};
use clinker_bench_support::{FieldKind, Scale};
use clinker_core::config::pipeline_node::{PipelineNode, SourceBody};
use clinker_core::config::{CompileContext, InputFormat, load_config};
use clinker_core::error::PipelineError;
use clinker_core::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams};
use indexmap::IndexMap;
use tempfile::TempDir;

use crate::format_mapping::input_format_to_data_format;

/// Executes benchmark pipeline configs against cached generated data.
/// Streaming: reads from cached file, writes to in-memory buffer.
pub struct BenchPipelineRunner {
    cache: BenchDataCache,
}

impl BenchPipelineRunner {
    pub fn new(cache: BenchDataCache) -> Self {
        Self { cache }
    }

    /// Run a pipeline config at the given scale. Returns `ExecutionReport`
    /// with per-stage `StageMetrics` populated.
    ///
    /// Supports multi-source pipelines: generates and caches data for each
    /// source node independently. Field count and types are derived from
    /// each source node's schema declaration.
    pub fn run(&self, config_path: &Path, scale: Scale) -> Result<ExecutionReport, PipelineError> {
        let mut config = load_config(config_path).expect("bench config must parse");

        // Split outputs create files on disk via a file factory, so we need a
        // real temp directory for them instead of the placeholder `bench://` path.
        let _split_dir = rewrite_split_output_paths(&mut config);

        // Count sources to decide whether to tag cache files for disambiguation.
        let source_count = config
            .nodes
            .iter()
            .filter(|n| matches!(&n.value, PipelineNode::Source { .. }))
            .count();

        // Build a reader for each source node.
        let mut readers: clinker_core::executor::SourceReaders = HashMap::new();

        for spanned in &config.nodes {
            if let PipelineNode::Source {
                header,
                config: body,
            } = &spanned.value
            {
                let data_format = input_format_to_data_format(&body.source.format)
                    .expect("bench source format must map");
                let field_types = field_types_for_source(body);
                let field_count = field_types.len();
                let wrapper = nested_wrapper_for_source(body);

                let tag = if source_count > 1 {
                    header.name.clone()
                } else {
                    String::new()
                };

                let data_path = self.cache.get_or_generate(&DataSpec {
                    format: data_format,
                    scale,
                    field_count,
                    string_len: 10,
                    seed: 42,
                    field_types,
                    tag,
                    wrapper,
                });

                let file = std::fs::File::open(&data_path).expect("cached data file must exist");
                readers.insert(
                    body.source.name.clone(),
                    clinker_core::executor::single_file_reader(
                        data_path.clone(),
                        Box::new(BufReader::new(file)),
                    ),
                );
            }
        }

        let mut writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::new();
        for output in config.output_configs() {
            writers.insert(output.name.clone(), Box::new(Cursor::new(Vec::new())) as _);
        }

        let plan = config
            .compile(&CompileContext::new(clinker_bench_support::workspace_root()))
            .expect("bench config must compile");

        let params = PipelineRunParams {
            execution_id: "bench-run".to_string(),
            batch_id: "bench-batch".to_string(),
            pipeline_vars: IndexMap::new(),
            shutdown_token: None,
        };

        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
    }
}

/// Rewrite output paths for split outputs to use a real temp directory.
///
/// Split outputs bypass the provided in-memory writer and create files on disk
/// via a file factory. The placeholder `bench://` paths don't exist on the
/// filesystem, so we rewrite them to a temp directory under `target/`.
/// The returned `TempDir` must be kept alive for the duration of the pipeline run.
fn rewrite_split_output_paths(
    config: &mut clinker_core::config::PipelineConfig,
) -> Option<TempDir> {
    let has_split = config.nodes.iter().any(|n| {
        matches!(&n.value, PipelineNode::Output { config: body, .. } if body.output.split.is_some())
    });
    if !has_split {
        return None;
    }

    // Create a temp dir under the workspace `target/` directory. Use an
    // absolute path because `cargo test` CWD is the crate directory, not
    // the workspace root, so relative paths would resolve incorrectly when
    // the executor's file factory calls `File::create`.
    let base = clinker_bench_support::workspace_root().join("target/bench-split-tmp");
    std::fs::create_dir_all(&base).expect("failed to create bench split tmp dir");
    let dir = TempDir::new_in(&base).expect("failed to create temp dir for split output");
    let dir_path = dir.path().to_string_lossy().into_owned();

    // The security path validator rejects absolute paths by default.
    // Set the env var so both compile() calls (runner + executor internal)
    // allow the absolute temp path. Benchmarks are single-threaded at
    // setup time so this is safe.
    // SAFETY: no other thread reads this env var during config setup.
    unsafe { std::env::set_var("CLINKER_ALLOW_ABSOLUTE_PATHS", "1") };

    for node in &mut config.nodes {
        if let PipelineNode::Output { config: body, .. } = &mut node.value
            && body.output.split.is_some()
        {
            let filename = std::path::Path::new(&body.output.path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("output.csv");
            body.output.path = format!("{dir_path}/{filename}");
        }
    }

    Some(dir)
}

/// Derive per-field type layout from a source node's schema declaration.
///
/// CSV delivers all values as strings, so the default int/string alternation
/// (even=int, odd=string) produces universally parseable data. Only override
/// for types that need a specific string format (Date, DateTime) — a random
/// string like "misljiqatu" can't be parsed as a date.
fn field_types_for_source(body: &SourceBody) -> Vec<FieldKind> {
    use cxl::typecheck::Type;

    body.schema
        .columns
        .iter()
        .enumerate()
        .map(|(i, col)| match &col.ty {
            Type::Date | Type::DateTime => FieldKind::Date,
            _ => {
                if i % 2 == 0 {
                    FieldKind::Int
                } else {
                    FieldKind::String
                }
            }
        })
        .collect()
}

/// Detect nested source configs and return the appropriate `NestedWrapper`.
///
/// When a source has `record_path` set (JSON or XML), the generated data needs
/// to be wrapped in a parent structure so the format reader can navigate to it.
fn nested_wrapper_for_source(body: &SourceBody) -> Option<NestedWrapper> {
    match &body.source.format {
        InputFormat::Json(Some(opts)) => {
            let record_path = opts.record_path.as_deref()?;
            let segments: Vec<String> = record_path.split('.').map(String::from).collect();
            if segments.is_empty() {
                return None;
            }
            Some(NestedWrapper::Json {
                path_segments: segments,
            })
        }
        InputFormat::Xml(Some(opts)) => {
            let record_path = opts.record_path.as_deref()?;
            // XML record_path uses `/` separator and includes the record element.
            // E.g., "root/data/record" → parent_elements = ["root", "data"],
            // and records are wrapped in <root><data>...<record>...</record>...</data></root>
            let segments: Vec<String> = record_path.split('/').map(String::from).collect();
            if segments.len() < 2 {
                return None;
            }
            // All segments except the last are parent elements.
            // The last segment is the record element name (already "record" in our generators).
            Some(NestedWrapper::Xml {
                parent_elements: segments[..segments.len() - 1].to_vec(),
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_runner() -> BenchPipelineRunner {
        BenchPipelineRunner::new(BenchDataCache::default_location())
    }

    fn pipelines_dir() -> std::path::PathBuf {
        clinker_bench_support::workspace_root().join("benches/pipelines")
    }

    /// Runner executes a CSV passthrough config and returns a report.
    #[test]
    fn test_runner_csv_passthrough_returns_report() {
        let runner = test_runner();
        let path = pipelines_dir().join("format/csv_passthrough.yaml");
        let report = runner.run(&path, Scale::Small).unwrap();
        assert!(!report.stages.is_empty());
    }

    /// Runner executes a JSON NDJSON config.
    #[test]
    fn test_runner_json_passthrough_returns_report() {
        let runner = test_runner();
        let path = pipelines_dir().join("format/json_ndjson_passthrough.yaml");
        let report = runner.run(&path, Scale::Small).unwrap();
        assert!(!report.stages.is_empty());
    }

    /// Returned report has at least 3 stage metrics (Compile + SchemaScan + more).
    #[test]
    fn test_runner_report_has_stage_metrics() {
        let runner = test_runner();
        let path = pipelines_dir().join("format/csv_passthrough.yaml");
        let report = runner.run(&path, Scale::Small).unwrap();
        assert!(
            report.stages.len() >= 3,
            "expected >= 3 stages, got {}",
            report.stages.len()
        );
    }

    /// Runner executes an XML passthrough config.
    #[test]
    fn test_runner_xml_passthrough_returns_report() {
        let runner = test_runner();
        let path = pipelines_dir().join("format/xml_passthrough.yaml");
        let report = runner.run(&path, Scale::Small).unwrap();
        assert!(!report.stages.is_empty());
    }

    /// Runner executes a fixed-width passthrough config.
    #[test]
    fn test_runner_fixed_width_passthrough_returns_report() {
        let runner = test_runner();
        let path = pipelines_dir().join("format/fixed_width_passthrough.yaml");
        let report = runner.run(&path, Scale::Small).unwrap();
        assert!(!report.stages.is_empty());
    }

    /// Runner executes a split-by-bytes config.
    #[test]
    fn test_runner_splitting_by_bytes() {
        let runner = test_runner();
        let path = pipelines_dir().join("features/splitting_by_bytes.yaml");
        let report = runner.run(&path, Scale::Small).unwrap();
        assert!(!report.stages.is_empty());
    }

    /// Runner executes a transform→aggregate pipeline (etl classic).
    #[test]
    fn test_runner_etl_classic() {
        let runner = test_runner();
        let path = pipelines_dir().join("realistic/etl_classic.yaml");
        let report = runner.run(&path, Scale::Small).unwrap();
        assert!(!report.stages.is_empty());
    }

    /// Runner executes a nested JSON pipeline.
    #[test]
    fn test_runner_nested_json() {
        let runner = test_runner();
        let path = pipelines_dir().join("realistic/nested_json_etl.yaml");
        let report = runner.run(&path, Scale::Small).unwrap();
        assert!(!report.stages.is_empty());
    }

    /// Runner executes a nested XML pipeline.
    #[test]
    fn test_runner_nested_xml() {
        let runner = test_runner();
        let path = pipelines_dir().join("realistic/nested_xml_etl.yaml");
        let report = runner.run(&path, Scale::Small).unwrap();
        assert!(!report.stages.is_empty());
    }
}
