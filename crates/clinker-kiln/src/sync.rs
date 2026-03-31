/// Bidirectional sync engine: YAML text ↔ dual pipeline model.
///
/// The sync engine maintains two models:
///
/// - `RawPipelineConfig`: preserves `_import` directives — the authoritative
///   model for YAML serialization. Round-trips faithfully.
/// - `PipelineConfig`: fully resolved (imports expanded) — consumed by canvas,
///   inspector, and engine.
///
/// Data flow:
/// ```text
/// YAML text → parse → RawPipelineConfig → resolve → PipelineConfig
///                           ↑                              ↓
///                      serialize ←── mutate ←── inspector edit
/// ```
///
/// An `EditSource` enum prevents infinite sync loops: when the YAML editor
/// triggers a parse, we don't re-serialize; when the inspector triggers a
/// serialize, we don't re-parse.

use std::collections::HashMap;
use std::path::Path;

use clinker_core::composition::{
    parse_raw_config, resolve_imports, CompositionError, CompositionOrigin, RawPipelineConfig,
    RawTransformEntry, ResolvedComposition,
};
use clinker_core::config::{parse_config, PipelineConfig, TransformEntry};

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

// ── Parsing ─────────────────────────────────────────────────────────────

/// Parse a YAML string into a `PipelineConfig` (legacy path, no composition support).
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

/// Parse a YAML string into a `RawPipelineConfig`, preserving `_import` directives.
///
/// This is the primary parse path for Kiln. The raw config can be serialized
/// back to YAML without losing `_import` structure.
pub fn parse_raw_yaml(yaml: &str) -> Result<RawPipelineConfig, Vec<String>> {
    match parse_raw_config(yaml) {
        Ok(config) => Ok(config),
        Err(e) => Err(vec![e.to_string()]),
    }
}

/// Parse + resolve: YAML → RawPipelineConfig → resolved PipelineConfig.
///
/// Returns the resolved config, composition metadata, and any resolution errors.
/// If the YAML has no `_import` directives, compositions will be empty.
pub fn parse_and_resolve_yaml(
    yaml: &str,
    workspace_root: Option<&Path>,
) -> Result<ResolvedPipeline, Vec<String>> {
    let raw = parse_raw_yaml(yaml)?;

    // If workspace root is available, resolve imports
    let (resolved, compositions) = if let Some(root) = workspace_root {
        match resolve_imports(&raw, root) {
            Ok((config, comps)) => (config, comps),
            Err(errors) => {
                return Err(errors.iter().map(|e| e.to_string()).collect());
            }
        }
    } else {
        // No workspace — inline-only transforms, skip import resolution
        let config = raw_to_resolved_no_imports(&raw)?;
        (config, Vec::new())
    };

    Ok(ResolvedPipeline {
        raw,
        resolved,
        compositions,
    })
}

/// Result of parsing and resolving a pipeline YAML.
pub struct ResolvedPipeline {
    /// Raw config preserving `_import` directives (for serialization).
    pub raw: RawPipelineConfig,
    /// Fully resolved config (for canvas/inspector/engine).
    pub resolved: PipelineConfig,
    /// Metadata about resolved compositions (for canvas rendering).
    pub compositions: Vec<ResolvedComposition>,
}

/// Convert a RawPipelineConfig to PipelineConfig without resolving imports.
///
/// Used when no workspace root is available. Import directives are skipped.
fn raw_to_resolved_no_imports(raw: &RawPipelineConfig) -> Result<PipelineConfig, Vec<String>> {
    let transforms: Vec<TransformEntry> = raw
        .transformations
        .iter()
        .filter_map(|entry| match entry {
            RawTransformEntry::Inline(t) => Some(TransformEntry::Transform(t.clone())),
            RawTransformEntry::Import(_) => None,
        })
        .collect();

    Ok(PipelineConfig {
        pipeline: raw.pipeline.clone(),
        inputs: raw.inputs.clone(),
        outputs: raw.outputs.clone(),
        transformations: transforms,
        error_handling: raw.error_handling.clone(),
        notes: raw.notes.clone(),
    })
}

/// Parse YAML into a PipelineConfig using the raw (composition-aware) path.
///
/// Skips import resolution (no workspace root). Used by tab constructors
/// where workspace context isn't available yet — imports resolve on the
/// first sync effect run after tab creation.
pub fn parse_yaml_raw_path(yaml: &str) -> Result<PipelineConfig, Vec<String>> {
    let raw = parse_raw_yaml(yaml)?;
    raw_to_resolved_no_imports(&raw)
}

// ── Partial parsing (graceful degradation) ──────────────────────────────

/// Result of attempting to parse a pipeline YAML with fallback.
pub enum ParseResult {
    /// Fully successful parse — all items valid, imports resolved.
    Complete(ResolvedPipeline),
    /// YAML is syntactically valid but some items failed to deserialize.
    Partial(clinker_core::partial::PartialPipelineConfig),
    /// Total failure — YAML syntax is broken, nothing recoverable.
    Failed(Vec<String>),
}

/// Try to parse pipeline YAML with graceful fallback.
///
/// 1. Fast path: `parse_and_resolve_yaml` (all-or-nothing).
/// 2. Fallback: `parse_partial_config` (per-item).
/// 3. If both fail: return `Failed`.
pub fn try_parse_yaml(yaml: &str, workspace_root: Option<&Path>) -> ParseResult {
    // Fast path: try full parse
    match parse_and_resolve_yaml(yaml, workspace_root) {
        Ok(resolved) => return ParseResult::Complete(resolved),
        Err(_) => {}
    }

    // Fallback: per-item partial parse
    match clinker_core::partial::parse_partial_config(yaml) {
        Ok(partial) => ParseResult::Partial(partial),
        Err(e) => ParseResult::Failed(vec![e]),
    }
}

// ── Serialization ───────────────────────────────────────────────────────

/// Serialize a `PipelineConfig` back to YAML text (legacy path, no composition support).
///
/// Uses `serde_saphyr::to_string()` for round-trip serialization.
/// Note: comments and formatting from the original YAML are lost.
pub fn serialize_yaml(config: &PipelineConfig) -> String {
    serde_saphyr::to_string(config).unwrap_or_else(|e| format!("# Serialization error: {e}\n"))
}

/// Serialize a `RawPipelineConfig` back to YAML text, preserving `_import` directives.
///
/// This is the primary serialize path for Kiln.
pub fn serialize_raw_yaml(config: &RawPipelineConfig) -> String {
    serde_saphyr::to_string(config).unwrap_or_else(|e| format!("# Serialization error: {e}\n"))
}

// ── Inspector override editing ──────────────────────────────────────────

/// Apply a CXL override from the inspector to the raw pipeline config.
///
/// When the user edits the CXL of a composition-sourced transform in the inspector,
/// this function creates/updates/removes the override entry in the `_import` directive.
///
/// Returns `true` if the raw config was modified.
pub fn apply_composition_override(
    raw: &mut RawPipelineConfig,
    origin: &CompositionOrigin,
    new_cxl: &str,
) -> bool {
    // Find the matching _import directive
    for entry in &mut raw.transformations {
        if let RawTransformEntry::Import(directive) = entry {
            if directive.path == origin.composition_path {
                // Compare with original (whitespace-normalized)
                if new_cxl.trim() == origin.original_cxl.trim() {
                    // Reverted to base — remove override
                    if directive.overrides.contains_key(&origin.transform_name) {
                        directive.overrides.swap_remove(&origin.transform_name);
                        return true;
                    }
                    return false;
                } else {
                    // Different from base — upsert override
                    let ovr = directive
                        .overrides
                        .entry(origin.transform_name.clone())
                        .or_insert_with(|| clinker_core::composition::TransformOverride {
                            cxl: None,
                            description: None,
                            local_window: None,
                        });
                    ovr.cxl = Some(new_cxl.to_string());
                    return true;
                }
            }
        }
    }
    false
}

// ── Composition cross-validation ────────────────────────────────────────

/// Validate composition contracts against the schema-derived field set.
///
/// For each resolved composition, checks that `requires` fields are available
/// from upstream schemas and transforms, and that `produces` fields are emitted.
///
/// This bridges the gap between `clinker-schema` (knows field names) and
/// `clinker-core::composition` (knows contracts). Called after both schema
/// validation and import resolution complete.
pub fn validate_composition_contracts(
    resolved: &ResolvedPipeline,
    schema_fields: &[String],
) -> Vec<clinker_core::composition::ContractWarning> {
    let mut warnings = Vec::new();

    // Build progressive field set: schema fields + emitted fields from earlier transforms
    let mut available: Vec<String> = schema_fields.to_vec();

    // Walk resolved transforms in order, validating contracts at composition boundaries
    let mut comp_idx = 0;
    for transform in resolved.resolved.transforms() {
        // Check if this transform is the first in a composition
        if comp_idx < resolved.compositions.len() {
            let comp = &resolved.compositions[comp_idx];
            if comp
                .transform_names
                .first()
                .is_some_and(|first| first == &transform.name)
            {
                // This is the entry point of a composition — validate its contract
                if let Some(ref contract) = comp.meta.contract {
                    for req in &contract.requires {
                        if !available.iter().any(|f| f == &req.name) {
                            warnings.push(clinker_core::composition::ContractWarning {
                                composition_path: comp.path.clone(),
                                message: format!(
                                    "Required field '{}' (type: {}) not available at import point",
                                    req.name, req.r#type
                                ),
                                kind: clinker_core::composition::ContractWarningKind::MissingRequiredField,
                            });
                        }
                    }
                }
                comp_idx += 1;
            }
        }

        // Add emitted fields from this transform to the available set
        for line in transform.cxl.lines() {
            let trimmed = line.trim();
            if let Some(rest) = trimmed.strip_prefix("emit ") {
                if let Some(field_name) = rest.split('=').next() {
                    let field = field_name.trim();
                    if !field.is_empty() {
                        available.push(field.to_string());
                    }
                }
            }
        }
    }

    warnings
}

// ── YAML line ranges ────────────────────────────────────────────────────

/// Compute YAML line ranges for each named stage.
///
/// Scans the YAML text for lines containing stage names (inputs, transforms,
/// outputs) and returns a map of name → (start_line, end_line) where lines
/// are 1-indexed and inclusive.
///
/// This is a best-effort heuristic: it looks for `- name: <stage_name>` or
/// `name: <stage_name>` patterns and groups subsequent indented lines.
pub fn compute_yaml_ranges(
    yaml: &str,
    config: &PipelineConfig,
) -> HashMap<String, (usize, usize)> {
    let mut ranges = HashMap::new();
    let lines: Vec<&str> = yaml.lines().collect();

    // Collect all stage names
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
