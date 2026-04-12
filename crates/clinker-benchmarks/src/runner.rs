//! Benchmark pipeline runner.
//!
//! Executes benchmark pipeline configs against cached generated data.
//! Streaming: reads from cached file, writes to in-memory buffer.

use std::collections::HashMap;
use std::io::{BufReader, Cursor};
use std::path::Path;

use clinker_bench_support::Scale;
use clinker_bench_support::cache::{BenchDataCache, DataSpec};
use clinker_core::config::pipeline_node::PipelineNode;
use clinker_core::config::{CompileContext, load_config};
use clinker_core::error::PipelineError;
use clinker_core::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams};
use indexmap::IndexMap;

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
    /// Field count is derived from the source node's schema declaration
    /// so generated data matches the pipeline's expected column count.
    pub fn run(&self, config_path: &Path, scale: Scale) -> Result<ExecutionReport, PipelineError> {
        let config = load_config(config_path).expect("bench config must parse");

        let source = config
            .source_configs()
            .next()
            .expect("bench config must have a source");
        let data_format =
            input_format_to_data_format(&source.format).expect("bench source format must map");

        let field_count = source_schema_field_count(&config);

        let data_path = self.cache.get_or_generate(&DataSpec {
            format: data_format,
            scale,
            field_count,
            string_len: 10,
            seed: 42,
        });

        let file = std::fs::File::open(&data_path).expect("cached data file must exist");
        let readers: HashMap<String, Box<dyn std::io::Read + Send>> =
            HashMap::from([(source.name.clone(), Box::new(BufReader::new(file)) as _)]);

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

/// Derive field count from the first source node's schema declaration.
/// Falls back to 20 if no schema is found (shouldn't happen for bench configs).
fn source_schema_field_count(config: &clinker_core::config::PipelineConfig) -> usize {
    for node in config.nodes.iter() {
        if let PipelineNode::Source { config: body, .. } = &node.value {
            return body.schema.columns.len();
        }
    }
    20
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
}
