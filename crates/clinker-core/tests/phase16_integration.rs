//! Phase 16 end-to-end integration tests for GROUP BY aggregation.
//!
//! Tests full pipeline execution with aggregate transforms: YAML config parsing,
//! CXL aggregate function evaluation, hash/streaming strategy selection, output
//! schema derivation, and multi-output aggregation.

use std::collections::HashMap;
use std::io::{self, Cursor, Read, Write};
use std::sync::{Arc, Mutex};

use clinker_core::config::parse_config;
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};

/// Thread-safe in-memory buffer for capturing output in integration tests.
#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    fn new() -> Self {
        Self::default()
    }

    fn as_string(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}

/// Build PipelineRunParams from a parsed config.
fn test_params(config: &clinker_core::config::PipelineConfig) -> PipelineRunParams {
    let pipeline_vars = config
        .pipeline
        .vars
        .as_ref()
        .map(|v| clinker_core::config::convert_pipeline_vars(v))
        .unwrap_or_default();
    PipelineRunParams {
        execution_id: "integration-test".to_string(),
        batch_id: "batch-001".to_string(),
        pipeline_vars,
    }
}

/// Run a single-input, single-output pipeline. Returns (report, output_csv).
fn run_single(yaml: &str, csv_input: &str) -> (clinker_core::executor::ExecutionReport, String) {
    let config = parse_config(yaml).unwrap();
    let params = test_params(&config);

    let readers: HashMap<String, Box<dyn Read + Send>> = HashMap::from([(
        config.inputs[0].name.clone(),
        Box::new(Cursor::new(csv_input.as_bytes().to_vec())) as Box<dyn Read + Send>,
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        config.outputs[0].name.clone(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_with_readers_writers(&config, readers, writers, &params).unwrap();
    (report, buf.as_string())
}

// ---------- End-to-end aggregation ----------

#[test]
fn test_e2e_group_by_sum_count() {
    // Full pipeline: CSV → aggregate(group_by: [dept], cxl: sum + count) → CSV output
    todo!("Task 16.3")
}

#[test]
fn test_e2e_aggregate_avg_min_max() {
    // Full pipeline with avg, min, max aggregates
    todo!("Task 16.3")
}

#[test]
fn test_e2e_aggregate_collect() {
    // Collect aggregate produces Array in output
    todo!("Task 16.3")
}

#[test]
fn test_e2e_aggregate_weighted_avg() {
    // weighted_avg(salary, hours) end-to-end
    todo!("Task 16.3")
}

#[test]
fn test_e2e_global_fold_no_group_by() {
    // group_by: [] → single output row with global aggregates
    todo!("Task 16.3")
}

#[test]
fn test_e2e_aggregate_null_handling() {
    // NULLs in input: skipped by sum/count(field), included by collect
    todo!("Task 16.3")
}

#[test]
fn test_e2e_aggregate_chained_with_transform() {
    // Transform → Aggregate → Transform: schema flows correctly
    todo!("Task 16.3")
}

#[test]
fn test_e2e_streaming_vs_hash_identical() {
    // Same pipeline with sort_order → streaming produces same results as hash
    todo!("Task 16.4")
}
