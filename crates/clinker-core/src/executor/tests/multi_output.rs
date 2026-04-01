//! Multi-output routing tests for Phase 13.
//!
//! Tests in this module exercise the multi-writer registry, route condition
//! evaluation, per-output channels, and DLQ stage/route extensions.

use super::*;
use crate::test_helpers::SharedBuffer;
use std::collections::HashMap;

/// Build a multi-output test fixture with the given YAML config.
///
/// Returns the parsed `PipelineConfig` and a `HashMap<String, SharedBuffer>`
/// with one entry per output defined in the config. The buffer names match
/// the output `name` fields in the YAML.
fn multi_output_fixture(
    yaml: &str,
) -> (crate::config::PipelineConfig, HashMap<String, SharedBuffer>) {
    let config = crate::config::parse_config(yaml).unwrap();
    let buffers: HashMap<String, SharedBuffer> = config
        .outputs
        .iter()
        .map(|o| (o.name.clone(), SharedBuffer::new()))
        .collect();
    (config, buffers)
}

/// Build default `PipelineRunParams` for tests.
fn test_params(config: &crate::config::PipelineConfig) -> PipelineRunParams {
    let pipeline_vars = config
        .pipeline
        .vars
        .as_ref()
        .map(|v| crate::config::convert_pipeline_vars(v))
        .unwrap_or_default();
    PipelineRunParams {
        execution_id: "test-exec-id".to_string(),
        batch_id: "test-batch-id".to_string(),
        pipeline_vars,
    }
}
