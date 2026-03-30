/// Bidirectional sync engine: YAML text ↔ PipelineConfig model.
///
/// The `PipelineConfig` struct is the canonical source of truth (spec §2.3).
/// Two operations keep the YAML text and model in sync:
///
/// - `parse_yaml()`: YAML text → PipelineConfig (on YAML editor keystroke)
/// - `serialize_yaml()`: PipelineConfig → YAML text (on inspector form edit)
///
/// An `EditSource` enum prevents infinite sync loops: when the YAML editor
/// triggers a parse, we don't re-serialize; when the inspector triggers a
/// serialize, we don't re-parse.

use clinker_core::config::{parse_config, PipelineConfig};
use std::collections::HashMap;

/// Tracks which view most recently edited the pipeline model.
/// Used to break the YAML ↔ model sync loop.
#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub enum EditSource {
    /// YAML text was edited directly — parse, don't re-serialize.
    Yaml,
    /// Inspector form field was edited — serialize, don't re-parse.
    Inspector,
    /// Canvas action (add/remove/reorder) — serialize, don't re-parse.
    #[allow(dead_code)]
    Canvas,
    /// No recent edit — initial state.
    #[default]
    None,
}

/// Parse a YAML string into a `PipelineConfig`.
///
/// Returns `Ok(config)` on success, or `Err(messages)` with human-readable
/// error descriptions on failure. Uses `clinker_core::config::parse_config()`
/// which includes environment variable interpolation.
pub fn parse_yaml(yaml: &str) -> Result<PipelineConfig, Vec<String>> {
    match parse_config(yaml) {
        Ok(config) => Ok(config),
        Err(e) => Err(vec![e.to_string()]),
    }
}

/// Serialize a `PipelineConfig` back to YAML text.
///
/// Uses `serde_saphyr::to_string()` for round-trip serialization.
/// Note: comments and formatting from the original YAML are lost.
pub fn serialize_yaml(config: &PipelineConfig) -> String {
    serde_saphyr::to_string(config).unwrap_or_else(|e| format!("# Serialization error: {e}\n"))
}

/// Compute YAML line ranges for each named stage.
///
/// Scans the YAML text for lines containing stage names (inputs, transforms,
/// outputs) and returns a map of name → (start_line, end_line) where lines
/// are 1-indexed and inclusive.
///
/// This is a best-effort heuristic: it looks for `- name: <stage_name>` or
/// `name: <stage_name>` patterns and groups subsequent indented lines.
pub fn compute_yaml_ranges(yaml: &str, config: &PipelineConfig) -> HashMap<String, (usize, usize)> {
    let mut ranges = HashMap::new();
    let lines: Vec<&str> = yaml.lines().collect();

    // Collect all stage names
    let mut stage_names: Vec<String> = Vec::new();
    for input in &config.inputs {
        stage_names.push(input.name.clone());
    }
    for transform in &config.transformations {
        stage_names.push(transform.name.clone());
    }
    for output in &config.outputs {
        stage_names.push(output.name.clone());
    }

    for name in &stage_names {
        // Find the line containing `name: <stage_name>` or `- name: <stage_name>`
        let name_pattern = format!("name: {name}");
        if let Some(start_idx) = lines.iter().position(|line| line.contains(&name_pattern)) {
            let _start_line = start_idx + 1; // 1-indexed (used by callers)

            // Walk backwards to find the list item start (`- name:` or just before)
            let mut block_start = start_idx;
            if start_idx > 0 {
                let line = lines[start_idx].trim_start();
                if line.starts_with("- name:") || line.starts_with("name:") {
                    // Already at the block start
                } else {
                    // Check if previous line has `- ` prefix at same or less indent
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

            // Walk forward to find the end of this block (next item at same indent level, or section end)
            let base_indent = lines[block_start].len() - lines[block_start].trim_start().len();
            let mut end_idx = start_idx;
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

            ranges.insert(name.clone(), (block_start + 1, end_idx + 1)); // 1-indexed
        }
    }

    ranges
}
