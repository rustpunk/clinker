/// YAML text ↔ PipelineConfig sync.
///
/// Phase 16b: composition / channel features removed. Single-model sync only.
use std::collections::HashMap;
use std::path::Path;

use clinker_core::config::{PipelineConfig, parse_config};

/// Tracks which view most recently edited the pipeline model.
#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub enum EditSource {
    Yaml,
    Inspector,
    #[allow(dead_code)]
    Canvas,
    #[default]
    None,
}

/// Parse a YAML string into a `PipelineConfig`.
#[allow(dead_code)]
pub fn parse_yaml(yaml: &str) -> Result<PipelineConfig, Vec<String>> {
    parse_config(yaml).map_err(|e| vec![e.to_string()])
}

/// Compatibility shim — same as `parse_yaml`.
pub fn parse_yaml_raw_path(yaml: &str) -> Result<PipelineConfig, Vec<String>> {
    parse_yaml(yaml)
}

/// Result of parsing a pipeline YAML.
pub struct ResolvedPipeline {
    pub resolved: PipelineConfig,
}

/// Parse YAML to a ResolvedPipeline. `_workspace_root` is accepted for API
/// compatibility but unused.
pub fn parse_and_resolve_yaml(
    yaml: &str,
    _workspace_root: Option<&Path>,
) -> Result<ResolvedPipeline, Vec<String>> {
    let resolved = parse_yaml(yaml)?;
    Ok(ResolvedPipeline { resolved })
}

#[allow(clippy::large_enum_variant)]
pub enum ParseResult {
    Complete(ResolvedPipeline),
    Partial(clinker_core::partial::PartialPipelineConfig),
    Failed(Vec<String>),
}

pub fn try_parse_yaml(yaml: &str, workspace_root: Option<&Path>) -> ParseResult {
    if let Ok(resolved) = parse_and_resolve_yaml(yaml, workspace_root) {
        return ParseResult::Complete(resolved);
    }
    match clinker_core::partial::parse_partial_config(yaml) {
        Ok(partial) => ParseResult::Partial(partial),
        Err(e) => ParseResult::Failed(vec![e]),
    }
}

/// Serialize a `PipelineConfig` back to YAML text.
pub fn serialize_yaml(config: &PipelineConfig) -> String {
    clinker_core::yaml::to_string(config)
        .unwrap_or_else(|e| format!("# Serialization error: {e}\n"))
}

/// Compute YAML line ranges for each named stage (best-effort).
pub fn compute_yaml_ranges(yaml: &str, config: &PipelineConfig) -> HashMap<String, (usize, usize)> {
    let mut ranges = HashMap::new();
    let lines: Vec<&str> = yaml.lines().collect();

    let mut stage_names: Vec<String> = Vec::new();
    for input in &config.inputs {
        stage_names.push(input.name.clone());
    }
    for transform in config.transforms() {
        stage_names.push(transform.name.clone());
    }
    for output in &config.outputs {
        stage_names.push(output.name.clone());
    }

    for name in &stage_names {
        let name_pattern = format!("name: {name}");
        if let Some(start_idx) = lines.iter().position(|line| line.contains(&name_pattern)) {
            let mut block_start = start_idx;
            if start_idx > 0 {
                let line = lines[start_idx].trim_start();
                if !(line.starts_with("- name:") || line.starts_with("name:")) {
                    for i in (0..start_idx).rev() {
                        let prev = lines[i].trim_start();
                        if prev.starts_with("- ") {
                            block_start = i;
                            break;
                        }
                        if !prev.is_empty() {
                            break;
                        }
                    }
                }
            }

            let base_indent = lines[block_start].len() - lines[block_start].trim_start().len();
            let mut end_idx = start_idx;
            #[allow(clippy::needless_range_loop)]
            for i in (start_idx + 1)..lines.len() {
                let line = lines[i];
                if line.trim().is_empty() {
                    continue;
                }
                let indent = line.len() - line.trim_start().len();
                if indent <= base_indent {
                    break;
                }
                end_idx = i;
            }

            ranges.insert(name.clone(), (block_start + 1, end_idx + 1));
        }
    }

    ranges
}
