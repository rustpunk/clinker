/// Pipeline composition system: reusable transformation groups.
///
/// Compositions are `.comp.yaml` files containing a `_compose` metadata block
/// and a `transformations` array. Pipelines import them via `_import` directives
/// in their own `transformations` array.
///
/// The composition system uses a dual-model architecture:
/// - `RawPipelineConfig` preserves `_import` directives for round-trip YAML serialization
/// - `PipelineConfig` (from `config.rs`) is the resolved (expanded) form consumed by
///   the canvas, inspector, and engine
///
/// Resolution happens at parse time: `_import` directives are expanded inline,
/// overrides merged by transformation name, and circular imports rejected.
use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::config::{
    ConfigError, ErrorHandlingConfig, InputConfig, OutputConfig, PipelineConfig, PipelineMeta,
    TransformConfig, TransformEntry,
};

// ── Composition file types (.comp.yaml) ─────────────────────────────────

/// A parsed `.comp.yaml` file — a reusable group of transformations.
///
/// Uses `Vec<RawTransformEntry>` (not `Vec<TransformConfig>`) because compositions
/// can themselves import other compositions via `_import` directives.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositionConfig {
    /// Composition metadata block.
    #[serde(rename = "_compose")]
    pub compose: CompositionMeta,
    /// The transformations this composition provides (may include nested `_import` directives).
    pub transformations: Vec<RawTransformEntry>,
}

/// Metadata block from the `_compose` key in a `.comp.yaml` file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompositionMeta {
    pub name: String,
    pub description: Option<String>,
    pub version: Option<String>,
    pub contract: Option<Contract>,
}

/// Field contract: declares what fields a composition requires and produces.
///
/// Contract validation produces warnings only, never errors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Contract {
    #[serde(default)]
    pub requires: Vec<ContractField>,
    #[serde(default)]
    pub produces: Vec<ContractField>,
}

/// A single field in a contract's `requires` or `produces` list.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContractField {
    pub name: String,
    /// Field type hint: `string`, `int`, `float`, `bool`, `date`.
    pub r#type: String,
}

// ── Raw pipeline config (preserves _import directives) ──────────────────

/// Pipeline config that preserves `_import` directives for round-trip YAML serialization.
///
/// This is the authoritative model for serialization. `PipelineConfig` (from `config.rs`)
/// is derived from this via `resolve_imports()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawPipelineConfig {
    pub pipeline: PipelineMeta,
    pub inputs: Vec<InputConfig>,
    pub outputs: Vec<OutputConfig>,
    pub transformations: Vec<RawTransformEntry>,
    #[serde(default)]
    pub error_handling: ErrorHandlingConfig,
    /// Kiln IDE metadata: pipeline-level notes. Ignored by the engine.
    #[serde(default, rename = "_notes")]
    pub notes: Option<serde_json::Value>,
}

/// A single entry in the `transformations` array — either an inline transform or an import.
///
/// Uses `#[serde(untagged)]` so the YAML array can freely mix both kinds.
/// Serde tries `Import` first (it requires the `_import` field), falling back to `Inline`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RawTransformEntry {
    /// An `_import` directive referencing a `.comp.yaml` file.
    Import(ImportDirective),
    /// A regular inline transformation.
    Inline(Box<TransformConfig>),
}

/// An `_import` directive in a pipeline's transformations list.
///
/// References a `.comp.yaml` file and optionally overrides specific transformations
/// within it by name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportDirective {
    /// Path to the `.comp.yaml` file, relative to workspace root.
    #[serde(rename = "_import")]
    pub path: String,
    /// Per-transformation overrides keyed by transformation name.
    /// Only specified fields are replaced (partial merge).
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub overrides: IndexMap<String, TransformOverride>,
}

/// Partial override for a single transformation inside an imported composition.
///
/// All fields are optional — only specified fields replace the base composition's values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformOverride {
    pub cxl: Option<String>,
    pub description: Option<String>,
    pub local_window: Option<serde_json::Value>,
}

// ── Resolution types ────────────────────────────────────────────────────

/// Metadata about a resolved composition, used by the UI for rendering.
#[derive(Debug, Clone)]
pub struct ResolvedComposition {
    /// Path to the `.comp.yaml` file (relative to workspace root).
    pub path: String,
    /// Composition metadata.
    pub meta: CompositionMeta,
    /// Names of the transformations from this composition (in order).
    pub transform_names: Vec<String>,
    /// Number of overrides applied from the `_import` directive.
    pub override_count: usize,
    /// Per-transform origin data: (transform_name, original_cxl_before_override).
    pub origins: Vec<CompositionOrigin>,
}

/// Tracks a resolved transform's origin for reverse-mapping inspector edits to overrides.
#[derive(Debug, Clone, PartialEq)]
pub struct CompositionOrigin {
    /// Path to the `.comp.yaml` file (relative to workspace root).
    pub composition_path: String,
    /// The transform's name within the composition.
    pub transform_name: String,
    /// The original CXL from the `.comp.yaml` file, before any override was applied.
    pub original_cxl: String,
}

/// Warning from contract validation. Compositions only produce warnings, never errors.
#[derive(Debug, Clone)]
pub struct ContractWarning {
    /// Which composition produced this warning.
    pub composition_path: String,
    pub message: String,
    pub kind: ContractWarningKind,
}

/// Kinds of contract validation warnings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractWarningKind {
    /// A required field is not present in the upstream field set.
    MissingRequiredField,
    /// A field declared in `produces` is not emitted by the composition's CXL.
    UnproducedField,
}

// ── Composition errors ──────────────────────────────────────────────────

/// Errors during composition resolution.
#[derive(Debug, Clone)]
pub enum CompositionError {
    /// The `.comp.yaml` file could not be read.
    IoError { path: String, message: String },
    /// The `.comp.yaml` file could not be parsed.
    ParseError { path: String, message: String },
    /// Circular import detected.
    CircularImport { path: String, chain: Vec<String> },
    /// An override targets a transformation name that doesn't exist.
    OverrideTargetNotFound {
        import_path: String,
        transform_name: String,
        available: Vec<String>,
    },
}

impl fmt::Display for CompositionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IoError { path, message } => {
                write!(f, "composition I/O error for '{path}': {message}")
            }
            Self::ParseError { path, message } => {
                write!(f, "composition parse error in '{path}': {message}")
            }
            Self::CircularImport { path, chain } => {
                write!(
                    f,
                    "circular composition import detected: {} → {path}",
                    chain.join(" → ")
                )
            }
            Self::OverrideTargetNotFound {
                import_path,
                transform_name,
                available,
            } => {
                write!(
                    f,
                    "override target '{transform_name}' not found in '{import_path}'. Available: [{}]",
                    available.join(", ")
                )
            }
        }
    }
}

// ── Parsing ─────────────────────────────────────────────────────────────

/// Parse a raw pipeline config from YAML, preserving `_import` directives.
///
/// Environment variable interpolation is applied before deserialization.
pub fn parse_raw_config(yaml: &str) -> Result<RawPipelineConfig, ConfigError> {
    parse_raw_config_with_vars(yaml, &[])
}

/// Parse a raw pipeline config with extra variables for interpolation.
///
/// `extra_vars` are checked before system env vars during `${VAR}` expansion.
pub fn parse_raw_config_with_vars(
    yaml: &str,
    extra_vars: &[(&str, &str)],
) -> Result<RawPipelineConfig, ConfigError> {
    let interpolated = crate::config::interpolate_env_vars(yaml, extra_vars)?;
    let config: RawPipelineConfig = serde_saphyr::from_str(&interpolated)?;
    Ok(config)
}

/// Load and parse a raw pipeline config from a YAML file with extra variables.
pub fn load_raw_config_with_vars(
    path: &Path,
    extra_vars: &[(&str, &str)],
) -> Result<RawPipelineConfig, ConfigError> {
    let yaml = std::fs::read_to_string(path)?;
    parse_raw_config_with_vars(&yaml, extra_vars)
}

/// Convert a `RawPipelineConfig` to a resolved `PipelineConfig` without
/// resolving any imports (no workspace available).
///
/// Returns an error if any `_import` directives exist, since they cannot
/// be resolved without a workspace root.
pub fn raw_to_config_no_imports(
    raw: &RawPipelineConfig,
) -> Result<(PipelineConfig, Vec<ResolvedComposition>), Vec<CompositionError>> {
    let mut resolved_transforms: Vec<TransformConfig> = Vec::new();
    let mut errors: Vec<CompositionError> = Vec::new();

    for entry in &raw.transformations {
        match entry {
            RawTransformEntry::Inline(transform) => {
                resolved_transforms.push((**transform).clone());
            }
            RawTransformEntry::Import(directive) => {
                errors.push(CompositionError::IoError {
                    path: directive.path.clone(),
                    message: "_import requires a workspace (clinker.toml) for resolution".into(),
                });
            }
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    let config = PipelineConfig {
        pipeline: raw.pipeline.clone(),
        inputs: raw.inputs.clone(),
        outputs: raw.outputs.clone(),
        transformations: resolved_transforms
            .into_iter()
            .map(TransformEntry::Transform)
            .collect(),
        error_handling: raw.error_handling.clone(),
        notes: raw.notes.clone(),
    };

    Ok((config, Vec::new()))
}

/// Parse a `.comp.yaml` file from a YAML string.
pub fn parse_composition(yaml: &str) -> Result<CompositionConfig, ConfigError> {
    let interpolated = crate::config::interpolate_env_vars(yaml, &[])?;
    let config: CompositionConfig = serde_saphyr::from_str(&interpolated)?;
    Ok(config)
}

/// Load and parse a `.comp.yaml` file from disk.
pub fn load_composition(path: &Path) -> Result<CompositionConfig, CompositionError> {
    let yaml = std::fs::read_to_string(path).map_err(|e| CompositionError::IoError {
        path: path.display().to_string(),
        message: e.to_string(),
    })?;
    parse_composition(&yaml).map_err(|e| CompositionError::ParseError {
        path: path.display().to_string(),
        message: e.to_string(),
    })
}

// ── Resolution ──────────────────────────────────────────────────────────

/// Resolve all `_import` directives in a raw pipeline config, expanding them inline.
///
/// Returns the resolved `PipelineConfig` (with all imports expanded) and metadata
/// about each resolved composition (for canvas rendering and inspector override editing).
///
/// # Errors
///
/// Returns errors for I/O failures, parse failures, circular imports, and
/// override targets that don't exist.
pub fn resolve_imports(
    raw: &RawPipelineConfig,
    workspace_root: &Path,
) -> Result<(PipelineConfig, Vec<ResolvedComposition>), Vec<CompositionError>> {
    let mut visited = HashSet::new();
    resolve_imports_inner(raw, workspace_root, &mut visited)
}

fn resolve_imports_inner(
    raw: &RawPipelineConfig,
    workspace_root: &Path,
    visited: &mut HashSet<PathBuf>,
) -> Result<(PipelineConfig, Vec<ResolvedComposition>), Vec<CompositionError>> {
    let mut resolved_transforms: Vec<TransformConfig> = Vec::new();
    let mut compositions: Vec<ResolvedComposition> = Vec::new();
    let mut errors: Vec<CompositionError> = Vec::new();

    resolve_entries(
        &raw.transformations,
        workspace_root,
        visited,
        &mut resolved_transforms,
        &mut compositions,
        &mut errors,
    );

    if !errors.is_empty() {
        return Err(errors);
    }

    let resolved = PipelineConfig {
        pipeline: raw.pipeline.clone(),
        inputs: raw.inputs.clone(),
        outputs: raw.outputs.clone(),
        transformations: resolved_transforms
            .into_iter()
            .map(TransformEntry::Transform)
            .collect(),
        error_handling: raw.error_handling.clone(),
        notes: raw.notes.clone(),
    };

    Ok((resolved, compositions))
}

/// Recursively resolve a list of `RawTransformEntry` items, expanding imports inline.
///
/// Used by both pipeline resolution and nested composition resolution.
fn resolve_entries(
    entries: &[RawTransformEntry],
    workspace_root: &Path,
    visited: &mut HashSet<PathBuf>,
    resolved_transforms: &mut Vec<TransformConfig>,
    compositions: &mut Vec<ResolvedComposition>,
    errors: &mut Vec<CompositionError>,
) {
    for entry in entries {
        match entry {
            RawTransformEntry::Inline(transform) => {
                resolved_transforms.push((**transform).clone());
            }
            RawTransformEntry::Import(directive) => {
                resolve_import(
                    directive,
                    workspace_root,
                    visited,
                    resolved_transforms,
                    compositions,
                    errors,
                );
            }
        }
    }
}

/// Resolve a single `_import` directive: load the composition, apply overrides,
/// and recursively resolve any nested imports within the composition.
fn resolve_import(
    directive: &ImportDirective,
    workspace_root: &Path,
    visited: &mut HashSet<PathBuf>,
    resolved_transforms: &mut Vec<TransformConfig>,
    compositions: &mut Vec<ResolvedComposition>,
    errors: &mut Vec<CompositionError>,
) {
    let comp_path = workspace_root.join(&directive.path);
    let canonical = comp_path
        .canonicalize()
        .unwrap_or_else(|_| comp_path.clone());

    // Circular import detection
    if visited.contains(&canonical) {
        let chain: Vec<String> = visited.iter().map(|p| p.display().to_string()).collect();
        errors.push(CompositionError::CircularImport {
            path: directive.path.clone(),
            chain,
        });
        return;
    }

    visited.insert(canonical.clone());

    // Load and parse the composition
    let comp = match load_composition(&comp_path) {
        Ok(c) => c,
        Err(e) => {
            errors.push(e);
            visited.remove(&canonical);
            return;
        }
    };

    // Collect inline transform names for override validation
    let inline_names: Vec<String> = comp
        .transformations
        .iter()
        .filter_map(|entry| match entry {
            RawTransformEntry::Inline(t) => Some(t.name.clone()),
            RawTransformEntry::Import(_) => None,
        })
        .collect();

    // Validate override targets exist (overrides only apply to inline transforms)
    for override_name in directive.overrides.keys() {
        if !inline_names.contains(override_name) {
            errors.push(CompositionError::OverrideTargetNotFound {
                import_path: directive.path.clone(),
                transform_name: override_name.clone(),
                available: inline_names.clone(),
            });
        }
    }

    // Process each entry in the composition, applying overrides to inline transforms
    // and recursively resolving nested imports
    let mut origins = Vec::new();
    let mut override_count = 0;
    let mut transform_names = Vec::new();

    for entry in &comp.transformations {
        match entry {
            RawTransformEntry::Inline(transform) => {
                let mut transform = (**transform).clone();
                let original_cxl = transform.cxl.clone();
                transform_names.push(transform.name.clone());

                if let Some(ovr) = directive.overrides.get(&transform.name) {
                    if let Some(ref cxl) = ovr.cxl {
                        transform.cxl = cxl.clone();
                        override_count += 1;
                    }
                    if let Some(ref desc) = ovr.description {
                        transform.description = Some(desc.clone());
                    }
                    if let Some(ref lw) = ovr.local_window {
                        transform.local_window = Some(lw.clone());
                    }
                }

                origins.push(CompositionOrigin {
                    composition_path: directive.path.clone(),
                    transform_name: transform.name.clone(),
                    original_cxl,
                });

                resolved_transforms.push(transform);
            }
            RawTransformEntry::Import(nested_directive) => {
                // Recursively resolve nested imports
                resolve_import(
                    nested_directive,
                    workspace_root,
                    visited,
                    resolved_transforms,
                    compositions,
                    errors,
                );
            }
        }
    }

    compositions.push(ResolvedComposition {
        path: directive.path.clone(),
        meta: comp.compose.clone(),
        transform_names,
        override_count,
        origins,
    });

    visited.remove(&canonical);
}

// ── Contract validation ─────────────────────────────────────────────────

/// Validate a composition's contract against the upstream field set.
///
/// Checks that `requires` fields exist upstream and that `produces` fields
/// are actually emitted by the composition's CXL. Returns warnings only.
pub fn validate_contract(
    composition: &CompositionConfig,
    composition_path: &str,
    upstream_fields: &[String],
) -> Vec<ContractWarning> {
    let mut warnings = Vec::new();

    if let Some(ref contract) = composition.compose.contract {
        // Check requires: all required fields must be in upstream set
        for field in &contract.requires {
            if !upstream_fields.contains(&field.name) {
                warnings.push(ContractWarning {
                    composition_path: composition_path.to_string(),
                    message: format!(
                        "Required field '{}' (type: {}) not found upstream",
                        field.name, field.r#type
                    ),
                    kind: ContractWarningKind::MissingRequiredField,
                });
            }
        }

        // Check produces: all produced fields should appear in emit statements
        // This is a heuristic — we look for `emit <field_name>` in the CXL text
        let all_cxl: String = composition
            .transformations
            .iter()
            .filter_map(|entry| match entry {
                RawTransformEntry::Inline(t) => Some(t.cxl.as_str()),
                RawTransformEntry::Import(_) => None,
            })
            .collect::<Vec<_>>()
            .join("\n");

        for field in &contract.produces {
            let emit_pattern = format!("emit {}", field.name);
            if !all_cxl.contains(&emit_pattern) {
                warnings.push(ContractWarning {
                    composition_path: composition_path.to_string(),
                    message: format!(
                        "Declared output field '{}' not found in any emit statement",
                        field.name
                    ),
                    kind: ContractWarningKind::UnproducedField,
                });
            }
        }
    }

    warnings
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn write_file(dir: &Path, name: &str, content: &str) {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
    }

    fn as_transform(entry: &TransformEntry) -> &TransformConfig {
        match entry {
            TransformEntry::Transform(t) => t,
            _ => panic!("expected Transform variant"),
        }
    }

    const COMP_YAML: &str = r#"
_compose:
  name: "Clean Customer Data"
  description: "Normalize names and validate emails"
  version: "1.2"
  contract:
    requires:
      - name: first_name
        type: string
      - name: email
        type: string
    produces:
      - name: full_name
        type: string
      - name: email_lower
        type: string

transformations:
  - name: normalize_names
    cxl: |
      emit full_name = first_name + " " + last_name
      emit email_lower = email
  - name: validate_emails
    cxl: |
      emit email_valid = email_lower
"#;

    /// Helper: extract inline transform name from a RawTransformEntry.
    fn inline_name(entry: &RawTransformEntry) -> &str {
        match entry {
            RawTransformEntry::Inline(t) => &t.name,
            RawTransformEntry::Import(d) => panic!("Expected Inline, got Import({})", d.path),
        }
    }

    #[test]
    fn test_parse_composition() {
        let comp = parse_composition(COMP_YAML).unwrap();
        assert_eq!(comp.compose.name, "Clean Customer Data");
        assert_eq!(comp.compose.version.as_deref(), Some("1.2"));
        assert_eq!(comp.transformations.len(), 2);
        assert_eq!(inline_name(&comp.transformations[0]), "normalize_names");
        assert_eq!(inline_name(&comp.transformations[1]), "validate_emails");

        let contract = comp.compose.contract.as_ref().unwrap();
        assert_eq!(contract.requires.len(), 2);
        assert_eq!(contract.produces.len(), 2);
        assert_eq!(contract.requires[0].name, "first_name");
        assert_eq!(contract.produces[0].name, "full_name");
    }

    #[test]
    fn test_parse_raw_pipeline_with_import() {
        let yaml = r#"
pipeline:
  name: test-pipeline

inputs:
  - name: source
    type: csv
    path: /tmp/input.csv

outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv

transformations:
  - name: pre_clean
    cxl: "emit x = a"
  - _import: compositions/clean_customer.comp.yaml
    overrides:
      validate_emails:
        cxl: |
          emit email_valid = email_lower.contains("@")
  - name: post_process
    cxl: "emit y = b"
"#;
        let raw = parse_raw_config(yaml).unwrap();
        assert_eq!(raw.transformations.len(), 3);

        // First is inline
        assert!(
            matches!(&raw.transformations[0], RawTransformEntry::Inline(t) if t.name == "pre_clean")
        );

        // Second is import
        match &raw.transformations[1] {
            RawTransformEntry::Import(directive) => {
                assert_eq!(directive.path, "compositions/clean_customer.comp.yaml");
                assert_eq!(directive.overrides.len(), 1);
                assert!(directive.overrides.contains_key("validate_emails"));
                let ovr = &directive.overrides["validate_emails"];
                assert!(ovr.cxl.is_some());
            }
            _ => panic!("Expected Import variant"),
        }

        // Third is inline
        assert!(
            matches!(&raw.transformations[2], RawTransformEntry::Inline(t) if t.name == "post_process")
        );
    }

    #[test]
    fn test_resolve_imports_expands_transforms() {
        let dir = TempDir::new().unwrap();
        write_file(dir.path(), "compositions/clean.comp.yaml", COMP_YAML);

        let yaml = r#"
pipeline:
  name: test-pipeline

inputs:
  - name: source
    type: csv
    path: /tmp/input.csv

outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv

transformations:
  - name: pre_clean
    cxl: "emit x = a"
  - _import: compositions/clean.comp.yaml
  - name: post_process
    cxl: "emit y = b"
"#;
        let raw = parse_raw_config(yaml).unwrap();
        let (resolved, compositions) = resolve_imports(&raw, dir.path()).unwrap();

        // 1 inline + 2 from composition + 1 inline = 4
        assert_eq!(resolved.transformations.len(), 4);
        assert_eq!(as_transform(&resolved.transformations[0]).name, "pre_clean");
        assert_eq!(
            as_transform(&resolved.transformations[1]).name,
            "normalize_names"
        );
        assert_eq!(
            as_transform(&resolved.transformations[2]).name,
            "validate_emails"
        );
        assert_eq!(
            as_transform(&resolved.transformations[3]).name,
            "post_process"
        );

        // Composition metadata
        assert_eq!(compositions.len(), 1);
        assert_eq!(compositions[0].meta.name, "Clean Customer Data");
        assert_eq!(compositions[0].transform_names.len(), 2);
        assert_eq!(compositions[0].override_count, 0);
    }

    #[test]
    fn test_resolve_imports_applies_overrides() {
        let dir = TempDir::new().unwrap();
        write_file(dir.path(), "compositions/clean.comp.yaml", COMP_YAML);

        let yaml = r#"
pipeline:
  name: test-pipeline

inputs:
  - name: source
    type: csv
    path: /tmp/input.csv

outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv

transformations:
  - _import: compositions/clean.comp.yaml
    overrides:
      validate_emails:
        cxl: "emit email_valid = email_lower.contains(\"@\")"
"#;
        let raw = parse_raw_config(yaml).unwrap();
        let (resolved, compositions) = resolve_imports(&raw, dir.path()).unwrap();

        assert_eq!(resolved.transformations.len(), 2);
        // First transform unchanged
        assert!(
            as_transform(&resolved.transformations[0])
                .cxl
                .contains("emit full_name")
        );
        // Second transform overridden
        assert!(
            as_transform(&resolved.transformations[1])
                .cxl
                .contains("contains")
        );

        // Override count tracked
        assert_eq!(compositions[0].override_count, 1);

        // Original CXL preserved in origins
        let origin = &compositions[0].origins[1];
        assert_eq!(origin.transform_name, "validate_emails");
        assert!(
            origin
                .original_cxl
                .contains("emit email_valid = email_lower")
        );
    }

    #[test]
    fn test_resolve_imports_override_target_not_found() {
        let dir = TempDir::new().unwrap();
        write_file(dir.path(), "compositions/clean.comp.yaml", COMP_YAML);

        let yaml = r#"
pipeline:
  name: test-pipeline

inputs:
  - name: source
    type: csv
    path: /tmp/input.csv

outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv

transformations:
  - _import: compositions/clean.comp.yaml
    overrides:
      nonexistent_transform:
        cxl: "emit x = 1"
"#;
        let raw = parse_raw_config(yaml).unwrap();
        let result = resolve_imports(&raw, dir.path());
        assert!(result.is_err());

        let errors = result.unwrap_err();
        assert_eq!(errors.len(), 1);
        match &errors[0] {
            CompositionError::OverrideTargetNotFound {
                transform_name,
                available,
                ..
            } => {
                assert_eq!(transform_name, "nonexistent_transform");
                assert!(available.contains(&"normalize_names".to_string()));
                assert!(available.contains(&"validate_emails".to_string()));
            }
            other => panic!("Expected OverrideTargetNotFound, got: {other}"),
        }
    }

    #[test]
    fn test_circular_import_detected() {
        let dir = TempDir::new().unwrap();

        // comp_a imports comp_b, comp_b imports comp_a
        let comp_a = r#"
_compose:
  name: "Comp A"

transformations:
  - _import: compositions/comp_b.comp.yaml
"#;
        let comp_b = r#"
_compose:
  name: "Comp B"

transformations:
  - _import: compositions/comp_a.comp.yaml
"#;
        write_file(dir.path(), "compositions/comp_a.comp.yaml", comp_a);
        write_file(dir.path(), "compositions/comp_b.comp.yaml", comp_b);

        let yaml = r#"
pipeline:
  name: test-pipeline

inputs:
  - name: source
    type: csv
    path: /tmp/input.csv

outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv

transformations:
  - _import: compositions/comp_a.comp.yaml
"#;
        let raw = parse_raw_config(yaml).unwrap();
        let result = resolve_imports(&raw, dir.path());
        assert!(result.is_err());

        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, CompositionError::CircularImport { .. }))
        );
    }

    #[test]
    fn test_nested_imports() {
        let dir = TempDir::new().unwrap();

        let inner_comp = r#"
_compose:
  name: "Inner"
  version: "1.0"

transformations:
  - name: inner_step
    cxl: "emit inner_out = inner_in"
"#;
        let outer_comp = r#"
_compose:
  name: "Outer"
  version: "2.0"

transformations:
  - name: outer_pre
    cxl: "emit outer_mid = outer_in"
  - _import: compositions/inner.comp.yaml
  - name: outer_post
    cxl: "emit outer_out = inner_out"
"#;
        write_file(dir.path(), "compositions/inner.comp.yaml", inner_comp);
        write_file(dir.path(), "compositions/outer.comp.yaml", outer_comp);

        let yaml = r#"
pipeline:
  name: test-pipeline

inputs:
  - name: source
    type: csv
    path: /tmp/input.csv

outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv

transformations:
  - _import: compositions/outer.comp.yaml
"#;
        let raw = parse_raw_config(yaml).unwrap();
        let (resolved, compositions) = resolve_imports(&raw, dir.path()).unwrap();

        // outer_pre + inner_step + outer_post = 3
        assert_eq!(resolved.transformations.len(), 3);
        assert_eq!(as_transform(&resolved.transformations[0]).name, "outer_pre");
        assert_eq!(
            as_transform(&resolved.transformations[1]).name,
            "inner_step"
        );
        assert_eq!(
            as_transform(&resolved.transformations[2]).name,
            "outer_post"
        );

        // Two compositions resolved (outer and inner)
        assert_eq!(compositions.len(), 2);
    }

    #[test]
    fn test_contract_validation_missing_required_field() {
        let comp = parse_composition(COMP_YAML).unwrap();
        let upstream = vec!["email".to_string()]; // missing first_name

        let warnings = validate_contract(&comp, "clean.comp.yaml", &upstream);
        assert!(
            warnings
                .iter()
                .any(|w| w.kind == ContractWarningKind::MissingRequiredField
                    && w.message.contains("first_name"))
        );
    }

    #[test]
    fn test_contract_validation_all_satisfied() {
        let comp = parse_composition(COMP_YAML).unwrap();
        let upstream = vec!["first_name".to_string(), "email".to_string()];

        let warnings = validate_contract(&comp, "clean.comp.yaml", &upstream);
        // No missing required fields (produces warnings may still fire for heuristic check)
        assert!(
            !warnings
                .iter()
                .any(|w| w.kind == ContractWarningKind::MissingRequiredField)
        );
    }

    #[test]
    fn test_raw_pipeline_roundtrip_serialization() {
        let yaml = r#"
pipeline:
  name: test-pipeline

inputs:
  - name: source
    type: csv
    path: /tmp/input.csv

outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv

transformations:
  - name: pre_clean
    cxl: "emit x = a"
  - _import: compositions/clean.comp.yaml
    overrides:
      validate_emails:
        cxl: "emit email_valid = true"
  - name: post_process
    cxl: "emit y = b"
"#;
        let raw = parse_raw_config(yaml).unwrap();
        let serialized = serde_saphyr::to_string(&raw).unwrap();

        // Re-parse the serialized YAML
        let reparsed = parse_raw_config(&serialized).unwrap();
        assert_eq!(reparsed.transformations.len(), 3);

        // Import directive preserved
        match &reparsed.transformations[1] {
            RawTransformEntry::Import(directive) => {
                assert_eq!(directive.path, "compositions/clean.comp.yaml");
                assert_eq!(directive.overrides.len(), 1);
            }
            _ => panic!("Expected Import variant after round-trip"),
        }
    }

    #[test]
    fn test_composition_io_error() {
        let dir = TempDir::new().unwrap();
        // No composition file on disk

        let yaml = r#"
pipeline:
  name: test-pipeline

inputs:
  - name: source
    type: csv
    path: /tmp/input.csv

outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv

transformations:
  - _import: compositions/missing.comp.yaml
"#;
        let raw = parse_raw_config(yaml).unwrap();
        let result = resolve_imports(&raw, dir.path());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .iter()
                .any(|e| matches!(e, CompositionError::IoError { .. }))
        );
    }
}
