use indexmap::IndexMap;
use serde::Deserialize;
use std::path::{Path, PathBuf};

use clinker_core::config::{
    ErrorHandlingConfig, InputConfig, OutputConfig, PipelineConfig, SchemaSource, SortFieldSpec,
    TransformConfig,
};
use clinker_record::schema_def::FieldDef;

use crate::composition::ProvenanceMap;
use crate::error::ChannelError;
use crate::workspace::WorkspaceRoot;

// ---------------------------------------------------------------------------
// when: condition evaluator
// ---------------------------------------------------------------------------

/// Evaluate a `when:` condition string (post-interpolation).
///
/// Split-based parser checking operators in order:
/// `" not in "` → `" in "` → `" != "` → `" == "` → error.
///
/// List values must not contain commas (documented limitation).
pub fn eval_when(condition: &str) -> Result<bool, ChannelError> {
    if let Some((lhs, rhs)) = condition.split_once(" not in ") {
        let list = parse_bracket_list(rhs.trim())?;
        Ok(!list.contains(&lhs.trim().to_string()))
    } else if let Some((lhs, rhs)) = condition.split_once(" in ") {
        let list = parse_bracket_list(rhs.trim())?;
        Ok(list.contains(&lhs.trim().to_string()))
    } else if let Some((lhs, rhs)) = condition.split_once(" != ") {
        Ok(lhs.trim() != rhs.trim())
    } else if let Some((lhs, rhs)) = condition.split_once(" == ") {
        Ok(lhs.trim() == rhs.trim())
    } else {
        Err(ChannelError::WhenParseError {
            expr: condition.to_string(),
        })
    }
}

fn parse_bracket_list(s: &str) -> Result<Vec<String>, ChannelError> {
    let s = s.trim();
    let inner = s
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .ok_or_else(|| ChannelError::WhenParseError {
            expr: s.to_string(),
        })?;
    Ok(inner.split(',').map(|v| v.trim().to_string()).collect())
}

// ---------------------------------------------------------------------------
// Override structs
// ---------------------------------------------------------------------------

/// A parsed `.channel.yaml` override file.
#[derive(Debug, Deserialize)]
pub struct ChannelOverride {
    #[serde(rename = "_channel")]
    pub header: ChannelOverrideHeader,

    #[serde(default)]
    pub inputs: IndexMap<String, InputOverrideDelta>,

    #[serde(default)]
    pub transformations: IndexMap<String, TransformOverrideDelta>,

    /// Add individual inline transforms at a named anchor.
    #[serde(default)]
    pub add_transformations: Vec<AddTransformation>,

    /// Add entire composition blocks (first-class override operation).
    #[serde(default)]
    pub add_compositions: Vec<AddComposition>,

    /// Remove individual transforms by name.
    #[serde(default)]
    pub remove_transformations: Vec<String>,

    /// Remove ALL transforms from a composition by its path.
    #[serde(default)]
    pub remove_compositions: Vec<RemoveComposition>,

    #[serde(default)]
    pub outputs: IndexMap<String, OutputOverrideDelta>,

    #[serde(default)]
    pub error_handling: Option<ErrorHandlingConfig>,
}

/// Header section of a `.channel.yaml` override file.
#[derive(Debug, Deserialize)]
pub struct ChannelOverrideHeader {
    pub pipeline: String,
    #[serde(default)]
    pub description: Option<String>,
    /// Raw condition string; evaluated after interpolation.
    #[serde(default)]
    pub when: Option<String>,
}

/// An inline transform to add at a named anchor position.
#[derive(Debug, Deserialize)]
pub struct AddTransformation {
    pub name: String,
    pub after: String,
    pub cxl: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub local_window: Option<serde_json::Value>,
}

/// Add a composition block at a named anchor position.
#[derive(Debug, Deserialize)]
pub struct AddComposition {
    pub after: String,
    /// Workspace-root-relative path to `.comp.yaml`.
    pub r#ref: String,
}

/// Remove ALL transforms originating from a composition by its path.
#[derive(Debug, Deserialize)]
pub struct RemoveComposition {
    /// Workspace-root-relative path to `.comp.yaml`.
    pub r#ref: String,
}

// ---------------------------------------------------------------------------
// Delta structs — all fields Option<> for partial merge
// ---------------------------------------------------------------------------

/// Partial override for an input. Only specified fields are replaced.
// SYNC: InputConfig — update when InputConfig gains new fields
#[derive(Debug, Deserialize)]
pub struct InputOverrideDelta {
    pub path: Option<String>,
    pub schema: Option<SchemaSource>,
    pub schema_overrides: Option<Vec<FieldDef>>,
    pub sort_order: Option<Vec<SortFieldSpec>>,
}

/// Partial override for a transformation. Only specified fields are replaced.
// SYNC: TransformConfig — update when TransformConfig gains new fields
#[derive(Debug, Deserialize)]
pub struct TransformOverrideDelta {
    pub cxl: Option<String>,
    pub description: Option<String>,
    pub local_window: Option<serde_json::Value>,
}

/// Partial override for an output. Only specified fields are replaced.
// SYNC: OutputConfig — update when OutputConfig gains new fields
#[derive(Debug, Deserialize)]
pub struct OutputOverrideDelta {
    pub path: Option<String>,
    pub mapping: Option<IndexMap<String, String>>,
    pub exclude: Option<Vec<String>>,
    pub sort_order: Option<Vec<SortFieldSpec>>,
    pub include_header: Option<bool>,
    pub preserve_nulls: Option<bool>,
}

// ---------------------------------------------------------------------------
// ChannelOverride impl
// ---------------------------------------------------------------------------

impl ChannelOverride {
    /// Derive the expected `.channel.yaml` path from pipeline file stem + channel dir.
    ///
    /// E.g. `pipelines/customer_etl.yaml` + `channels/acme-corp/`
    /// → `channels/acme-corp/customer_etl.channel.yaml`
    pub fn path_for(pipeline: &Path, channel_dir: &Path) -> PathBuf {
        let stem = pipeline.file_stem().unwrap_or_default().to_string_lossy();
        channel_dir.join(format!("{}.channel.yaml", stem))
    }

    /// Load an override file. Returns `None` if the file does not exist
    /// (channel has no override for this pipeline). Errors on parse failure.
    pub fn load(
        path: &Path,
        channel_vars: &[(&str, &str)],
    ) -> Result<Option<Self>, ChannelError> {
        let raw = match std::fs::read_to_string(path) {
            Ok(content) => content,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(ChannelError::Io(e)),
        };

        let interpolated = clinker_core::config::interpolate_env_vars(&raw, channel_vars)
            .map_err(|e| {
                ChannelError::Validation(format!(
                    "failed to interpolate variables in {}: {}",
                    path.display(),
                    e
                ))
            })?;

        let parsed: ChannelOverride = serde_saphyr::from_str(&interpolated)?;
        Ok(Some(parsed))
    }

    /// Returns `true` if the `when:` condition is absent or evaluates to true.
    pub fn when_passes(&self) -> bool {
        match &self.header.when {
            None => true,
            Some(cond) => eval_when(cond).unwrap_or(false),
        }
    }
}

// ---------------------------------------------------------------------------
// 9-operation merge algorithm
// ---------------------------------------------------------------------------

/// Apply a channel override to a pipeline config.
///
/// Operations (in order):
/// 1. Evaluate `when:` — if false, return config unchanged.
/// 2. Override inputs — partial merge by name.
/// 3. Override transformations — partial merge by name.
/// 4. Add transformations — insert after named anchor.
/// 5. Add compositions — load `.comp.yaml`, inline transforms at anchor.
/// 6. Remove transformations — by name.
/// 7. Remove compositions — remove all transforms from a composition by provenance.
/// 8. Override outputs — partial merge by name.
/// 9. Override error_handling — full replacement if present.
pub fn resolve_channel(
    mut config: PipelineConfig,
    override_file: &ChannelOverride,
    workspace: &WorkspaceRoot,
    channel_vars: &[(&str, &str)],
    provenance: &mut ProvenanceMap,
) -> Result<PipelineConfig, ChannelError> {
    // 1. Evaluate when: condition
    if !override_file.when_passes() {
        return Ok(config);
    }

    let override_path = PathBuf::from("<override>");

    // 2. Override inputs — partial merge by name
    for (name, delta) in &override_file.inputs {
        let input = config
            .inputs
            .iter_mut()
            .find(|i| i.name == *name)
            .ok_or_else(|| ChannelError::OverrideTargetNotFound {
                name: name.clone(),
                file: override_path.clone(),
            })?;
        apply_input_delta(input, delta);
    }

    // 3. Override transformations — partial merge by name
    for (name, delta) in &override_file.transformations {
        let transform = config
            .transformations
            .iter_mut()
            .find(|t| t.name == *name)
            .ok_or_else(|| ChannelError::OverrideTargetNotFound {
                name: name.clone(),
                file: override_path.clone(),
            })?;
        apply_transform_delta(transform, delta);
    }

    // 4. Add transformations — insert after named anchor
    for add in &override_file.add_transformations {
        let anchor_pos = config
            .transformations
            .iter()
            .position(|t| t.name == add.after)
            .ok_or_else(|| ChannelError::AddAfterNotFound {
                name: add.name.clone(),
                after: add.after.clone(),
                file: override_path.clone(),
            })?;

        let new_transform = TransformConfig {
            name: add.name.clone(),
            description: add.description.clone(),
            cxl: add.cxl.clone(),
            local_window: add.local_window.clone(),
            log: None,
            validations: None,
        };
        config.transformations.insert(anchor_pos + 1, new_transform);
    }

    // 5. Add compositions — load .comp.yaml, inline transforms at anchor
    for add_comp in &override_file.add_compositions {
        let comp_path = workspace.resolve_comp_path(&add_comp.r#ref);
        if !comp_path.exists() {
            return Err(ChannelError::CompositionNotFound {
                path: comp_path,
            });
        }

        let anchor_pos = config
            .transformations
            .iter()
            .position(|t| t.name == add_comp.after)
            .ok_or_else(|| ChannelError::AddAfterNotFound {
                name: format!("composition:{}", add_comp.r#ref),
                after: add_comp.after.clone(),
                file: override_path.clone(),
            })?;

        // Load composition and inline its transforms
        let comp = crate::composition::CompositionFile::load(&comp_path)?;
        let canonical = std::fs::canonicalize(&comp_path)
            .map_err(|_| ChannelError::CompositionNotFound {
                path: comp_path.clone(),
            })?;

        // Resolve any nested _import in the composition
        let resolved_transforms = crate::composition::resolve_composition_transforms(
            comp.transformations,
            workspace,
            channel_vars,
            provenance,
        )?;

        // Record provenance for all added transforms
        for t in &resolved_transforms {
            provenance.insert(t.name.clone(), canonical.clone());
        }

        // Insert after anchor
        let insert_at = anchor_pos + 1;
        for (i, t) in resolved_transforms.into_iter().enumerate() {
            config.transformations.insert(insert_at + i, t);
        }
    }

    // 6. Remove transformations — by name (warning if not found)
    for name in &override_file.remove_transformations {
        let before_len = config.transformations.len();
        config.transformations.retain(|t| t.name != *name);
        if config.transformations.len() == before_len {
            tracing::warn!("remove_transformations: '{}' not found — skipping", name);
        }
        // Also clean up provenance
        provenance.remove(name);
    }

    // 7. Remove compositions — remove all transforms with matching provenance
    for remove_comp in &override_file.remove_compositions {
        let comp_path = workspace.resolve_comp_path(&remove_comp.r#ref);
        let canonical = match std::fs::canonicalize(&comp_path) {
            Ok(p) => p,
            Err(_) => {
                tracing::warn!(
                    "remove_compositions: cannot canonicalize '{}' — skipping",
                    remove_comp.r#ref
                );
                continue;
            }
        };

        // Find all transform names from this composition
        let names_to_remove: Vec<String> = provenance
            .iter()
            .filter(|(_, path)| **path == canonical)
            .map(|(name, _)| name.clone())
            .collect();

        if names_to_remove.is_empty() {
            tracing::warn!(
                "remove_compositions: no transforms from '{}' found — skipping",
                remove_comp.r#ref
            );
        }

        for name in &names_to_remove {
            config.transformations.retain(|t| t.name != *name);
            provenance.remove(name);
        }
    }

    // 8. Override outputs — partial merge by name
    for (name, delta) in &override_file.outputs {
        let output = config
            .outputs
            .iter_mut()
            .find(|o| o.name == *name)
            .ok_or_else(|| ChannelError::OverrideTargetNotFound {
                name: name.clone(),
                file: override_path.clone(),
            })?;
        apply_output_delta(output, delta);
    }

    // 9. Override error_handling — full replacement if present
    if let Some(ref eh) = override_file.error_handling {
        config.error_handling = eh.clone();
    }

    Ok(config)
}

// ---------------------------------------------------------------------------
// Delta application helpers
// ---------------------------------------------------------------------------

/// Primary public entrypoint. Loads group overrides (in inherits order),
/// then channel-specific override. Each step calls `resolve_channel()`.
///
/// `pipeline_path` is needed to derive `.channel.yaml` filenames via `path_for()`.
pub fn resolve_channel_with_inheritance(
    mut config: PipelineConfig,
    channel_id: &str,
    pipeline_path: &Path,
    workspace: &WorkspaceRoot,
    channel_vars: &[(&str, &str)],
    provenance: &mut ProvenanceMap,
) -> Result<PipelineConfig, ChannelError> {
    let manifest =
        crate::manifest::ChannelManifest::load(&workspace.channel_dir(channel_id))?;

    // Apply group overrides in order
    let group_ids = match &manifest.metadata.inherits {
        crate::manifest::ChannelInherits::None => vec![],
        crate::manifest::ChannelInherits::Single(id) => vec![id.as_str()],
        crate::manifest::ChannelInherits::Multiple(ids) => {
            ids.iter().map(|s| s.as_str()).collect()
        }
    };

    for group_id in &group_ids {
        let group_dir = workspace.group_dir(group_id);
        if !group_dir.exists() {
            return Err(ChannelError::GroupNotFound {
                id: group_id.to_string(),
            });
        }

        // Load group's .channel.yaml for this pipeline (may not exist → skip)
        let override_path = ChannelOverride::path_for(pipeline_path, &group_dir);
        if let Some(co) = ChannelOverride::load(&override_path, channel_vars)? {
            if co.when_passes() {
                config = resolve_channel(config, &co, workspace, channel_vars, provenance)?;
            }
        }
    }

    // Apply channel-specific override (always wins over groups)
    let channel_dir = workspace.channel_dir(channel_id);
    let override_path = ChannelOverride::path_for(pipeline_path, &channel_dir);
    if let Some(co) = ChannelOverride::load(&override_path, channel_vars)? {
        if co.when_passes() {
            config = resolve_channel(config, &co, workspace, channel_vars, provenance)?;
        }
    }

    Ok(config)
}

// ---------------------------------------------------------------------------
// Delta application helpers
// ---------------------------------------------------------------------------

fn apply_input_delta(input: &mut InputConfig, delta: &InputOverrideDelta) {
    if let Some(ref path) = delta.path {
        input.path = path.clone();
    }
    if let Some(ref schema) = delta.schema {
        input.schema = Some(schema.clone());
    }
    if let Some(ref overrides) = delta.schema_overrides {
        input.schema_overrides = Some(overrides.clone());
    }
    if let Some(ref sort) = delta.sort_order {
        input.sort_order = Some(sort.clone());
    }
}

fn apply_transform_delta(transform: &mut TransformConfig, delta: &TransformOverrideDelta) {
    if let Some(ref cxl) = delta.cxl {
        transform.cxl = cxl.clone();
    }
    if let Some(ref desc) = delta.description {
        transform.description = Some(desc.clone());
    }
    if let Some(ref lw) = delta.local_window {
        transform.local_window = Some(lw.clone());
    }
}

fn apply_output_delta(output: &mut OutputConfig, delta: &OutputOverrideDelta) {
    if let Some(ref path) = delta.path {
        output.path = path.clone();
    }
    if let Some(ref mapping) = delta.mapping {
        output.mapping = Some(mapping.clone());
    }
    if let Some(ref exclude) = delta.exclude {
        output.exclude = Some(exclude.clone());
    }
    if let Some(ref sort) = delta.sort_order {
        output.sort_order = Some(sort.clone());
    }
    if let Some(include_header) = delta.include_header {
        output.include_header = Some(include_header);
    }
    if let Some(preserve_nulls) = delta.preserve_nulls {
        output.preserve_nulls = Some(preserve_nulls);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_core::config::{
        ErrorStrategy, InputFormat, OutputFormat, PipelineMeta,
    };

    /// Helper: minimal pipeline config for testing.
    fn base_config() -> PipelineConfig {
        PipelineConfig {
            pipeline: PipelineMeta {
                name: "test".into(),
                memory_limit: None,
                vars: None,
                date_formats: None,
                rules_path: None,
                concurrency: None,
                date_locale: None,
                log_rules: None,
                include_provenance: None,
                metrics: None,
            },
            inputs: vec![InputConfig {
                name: "customers".into(),
                path: "/data/input.csv".into(),
                schema: None,
                schema_overrides: None,
                array_paths: None,
                sort_order: None,
                format: InputFormat::Csv(None),
            }],
            outputs: vec![OutputConfig {
                name: "results".into(),
                path: "/data/output.csv".into(),
                include_unmapped: false,
                include_header: None,
                mapping: Some(IndexMap::from([
                    ("name".into(), "name".into()),
                ])),
                exclude: None,
                sort_order: None,
                preserve_nulls: None,
                format: OutputFormat::Csv(None),
            }],
            transformations: vec![
                TransformConfig {
                    name: "filter_active".into(),
                    description: Some("Filter active records".into()),
                    cxl: "if status != \"active\"\n  drop\nend".into(),
                    local_window: None,
                    log: None,
                    validations: None,
                },
                TransformConfig {
                    name: "normalize_names".into(),
                    description: None,
                    cxl: "emit name = name.trim()".into(),
                    local_window: None,
                    log: None,
                    validations: None,
                },
            ],
            error_handling: ErrorHandlingConfig::default(),
        }
    }

    // ---- when: condition tests ----

    #[test]
    fn test_when_eq_true() {
        assert!(eval_when("prod == prod").unwrap());
    }

    #[test]
    fn test_when_eq_false() {
        assert!(!eval_when("dev == prod").unwrap());
    }

    #[test]
    fn test_when_neq_true() {
        assert!(eval_when("prod != dev").unwrap());
    }

    #[test]
    fn test_when_neq_false() {
        assert!(!eval_when("dev != dev").unwrap());
    }

    #[test]
    fn test_when_in_true() {
        assert!(eval_when("staging in [prod, staging]").unwrap());
    }

    #[test]
    fn test_when_in_false() {
        assert!(!eval_when("dev in [prod, staging]").unwrap());
    }

    #[test]
    fn test_when_not_in_true() {
        assert!(eval_when("prod not in [dev, test]").unwrap());
    }

    #[test]
    fn test_when_not_in_false() {
        assert!(!eval_when("dev not in [dev, test]").unwrap());
    }

    #[test]
    fn test_when_absent_always_applies() {
        let co = ChannelOverride {
            header: ChannelOverrideHeader {
                pipeline: "test.yaml".into(),
                description: None,
                when: None,
            },
            inputs: IndexMap::new(),
            transformations: IndexMap::new(),
            add_transformations: vec![],
            add_compositions: vec![],
            remove_transformations: vec![],
            remove_compositions: vec![],
            outputs: IndexMap::new(),
            error_handling: None,
        };
        assert!(co.when_passes());
    }

    #[test]
    fn test_when_parse_error() {
        let result = eval_when("this is garbage");
        assert!(matches!(result, Err(ChannelError::WhenParseError { .. })));
    }

    // ---- merge tests ----

    /// Helper: create a workspace for tests that need one.
    fn test_workspace() -> (tempfile::TempDir, WorkspaceRoot) {
        let tmp = tempfile::TempDir::new().unwrap();
        let ws = tmp.path();
        std::fs::write(ws.join("clinker.toml"), "[workspace]\n").unwrap();
        let workspace = WorkspaceRoot::at(ws).unwrap();
        (tmp, workspace)
    }

    #[test]
    fn test_override_input_path() {
        let (_tmp, workspace) = test_workspace();
        let mut provenance = ProvenanceMap::new();

        let co = ChannelOverride {
            header: ChannelOverrideHeader {
                pipeline: "test.yaml".into(),
                description: None,
                when: None,
            },
            inputs: IndexMap::from([(
                "customers".into(),
                InputOverrideDelta {
                    path: Some("/data/acme/input.csv".into()),
                    schema: None,
                    schema_overrides: None,
                    sort_order: None,
                },
            )]),
            transformations: IndexMap::new(),
            add_transformations: vec![],
            add_compositions: vec![],
            remove_transformations: vec![],
            remove_compositions: vec![],
            outputs: IndexMap::new(),
            error_handling: None,
        };

        let config = base_config();
        let result = resolve_channel(config, &co, &workspace, &[], &mut provenance).unwrap();
        assert_eq!(result.inputs[0].path, "/data/acme/input.csv");
        // Other fields preserved
        assert_eq!(result.inputs[0].name, "customers");
    }

    #[test]
    fn test_override_transform_cxl() {
        let (_tmp, workspace) = test_workspace();
        let mut provenance = ProvenanceMap::new();

        let co = ChannelOverride {
            header: ChannelOverrideHeader {
                pipeline: "test.yaml".into(),
                description: None,
                when: None,
            },
            inputs: IndexMap::new(),
            transformations: IndexMap::from([(
                "filter_active".into(),
                TransformOverrideDelta {
                    cxl: Some("if status.lower() != \"active\"\n  drop\nend".into()),
                    description: None,
                    local_window: None,
                },
            )]),
            add_transformations: vec![],
            add_compositions: vec![],
            remove_transformations: vec![],
            remove_compositions: vec![],
            outputs: IndexMap::new(),
            error_handling: None,
        };

        let config = base_config();
        let result = resolve_channel(config, &co, &workspace, &[], &mut provenance).unwrap();
        assert!(result.transformations[0].cxl.contains("status.lower()"));
        // Description preserved (not overridden)
        assert_eq!(
            result.transformations[0].description.as_deref(),
            Some("Filter active records")
        );
    }

    #[test]
    fn test_add_transformation_inline() {
        let (_tmp, workspace) = test_workspace();
        let mut provenance = ProvenanceMap::new();

        let co = ChannelOverride {
            header: ChannelOverrideHeader {
                pipeline: "test.yaml".into(),
                description: None,
                when: None,
            },
            inputs: IndexMap::new(),
            transformations: IndexMap::new(),
            add_transformations: vec![AddTransformation {
                name: "acme_validation".into(),
                after: "filter_active".into(),
                cxl: "emit flag = true".into(),
                description: None,
                local_window: None,
            }],
            add_compositions: vec![],
            remove_transformations: vec![],
            remove_compositions: vec![],
            outputs: IndexMap::new(),
            error_handling: None,
        };

        let config = base_config();
        let result = resolve_channel(config, &co, &workspace, &[], &mut provenance).unwrap();
        assert_eq!(result.transformations.len(), 3);
        assert_eq!(result.transformations[0].name, "filter_active");
        assert_eq!(result.transformations[1].name, "acme_validation");
        assert_eq!(result.transformations[2].name, "normalize_names");
    }

    #[test]
    fn test_add_transformation_order() {
        let (_tmp, workspace) = test_workspace();
        let mut provenance = ProvenanceMap::new();

        let co = ChannelOverride {
            header: ChannelOverrideHeader {
                pipeline: "test.yaml".into(),
                description: None,
                when: None,
            },
            inputs: IndexMap::new(),
            transformations: IndexMap::new(),
            add_transformations: vec![
                AddTransformation {
                    name: "first_add".into(),
                    after: "filter_active".into(),
                    cxl: "emit a = 1".into(),
                    description: None,
                    local_window: None,
                },
                AddTransformation {
                    name: "second_add".into(),
                    after: "first_add".into(),
                    cxl: "emit b = 2".into(),
                    description: None,
                    local_window: None,
                },
            ],
            add_compositions: vec![],
            remove_transformations: vec![],
            remove_compositions: vec![],
            outputs: IndexMap::new(),
            error_handling: None,
        };

        let config = base_config();
        let result = resolve_channel(config, &co, &workspace, &[], &mut provenance).unwrap();
        assert_eq!(result.transformations.len(), 4);
        assert_eq!(result.transformations[0].name, "filter_active");
        assert_eq!(result.transformations[1].name, "first_add");
        assert_eq!(result.transformations[2].name, "second_add");
        assert_eq!(result.transformations[3].name, "normalize_names");
    }

    #[test]
    fn test_add_composition_inlines_block() {
        let (tmp, workspace) = test_workspace();
        let mut provenance = ProvenanceMap::new();

        // Create a composition file
        let comp_dir = tmp.path().join("compositions");
        std::fs::create_dir_all(&comp_dir).unwrap();
        std::fs::write(
            comp_dir.join("audit.comp.yaml"),
            r#"
_composition:
  name: audit_block

transformations:
  - name: audit_stamp
    cxl: "emit audit_ts = now()"
  - name: audit_user
    cxl: "emit audit_user = \"system\""
"#,
        )
        .unwrap();

        let co = ChannelOverride {
            header: ChannelOverrideHeader {
                pipeline: "test.yaml".into(),
                description: None,
                when: None,
            },
            inputs: IndexMap::new(),
            transformations: IndexMap::new(),
            add_transformations: vec![],
            add_compositions: vec![AddComposition {
                after: "filter_active".into(),
                r#ref: "compositions/audit.comp.yaml".into(),
            }],
            remove_transformations: vec![],
            remove_compositions: vec![],
            outputs: IndexMap::new(),
            error_handling: None,
        };

        let config = base_config();
        let result = resolve_channel(config, &co, &workspace, &[], &mut provenance).unwrap();
        assert_eq!(result.transformations.len(), 4);
        assert_eq!(result.transformations[1].name, "audit_stamp");
        assert_eq!(result.transformations[2].name, "audit_user");
        // Provenance should track both
        assert!(provenance.contains_key("audit_stamp"));
        assert!(provenance.contains_key("audit_user"));
    }

    #[test]
    fn test_add_composition_anchor_not_found() {
        let (tmp, workspace) = test_workspace();
        let mut provenance = ProvenanceMap::new();

        let comp_dir = tmp.path().join("compositions");
        std::fs::create_dir_all(&comp_dir).unwrap();
        std::fs::write(
            comp_dir.join("audit.comp.yaml"),
            "_composition:\n  name: audit\ntransformations:\n  - name: x\n    cxl: \"emit x = 1\"\n",
        )
        .unwrap();

        let co = ChannelOverride {
            header: ChannelOverrideHeader {
                pipeline: "test.yaml".into(),
                description: None,
                when: None,
            },
            inputs: IndexMap::new(),
            transformations: IndexMap::new(),
            add_transformations: vec![],
            add_compositions: vec![AddComposition {
                after: "nonexistent_anchor".into(),
                r#ref: "compositions/audit.comp.yaml".into(),
            }],
            remove_transformations: vec![],
            remove_compositions: vec![],
            outputs: IndexMap::new(),
            error_handling: None,
        };

        let config = base_config();
        let result = resolve_channel(config, &co, &workspace, &[], &mut provenance);
        assert!(matches!(result, Err(ChannelError::AddAfterNotFound { .. })));
    }

    #[test]
    fn test_add_composition_ref_not_found() {
        let (_tmp, workspace) = test_workspace();
        let mut provenance = ProvenanceMap::new();

        let co = ChannelOverride {
            header: ChannelOverrideHeader {
                pipeline: "test.yaml".into(),
                description: None,
                when: None,
            },
            inputs: IndexMap::new(),
            transformations: IndexMap::new(),
            add_transformations: vec![],
            add_compositions: vec![AddComposition {
                after: "filter_active".into(),
                r#ref: "compositions/nonexistent.comp.yaml".into(),
            }],
            remove_transformations: vec![],
            remove_compositions: vec![],
            outputs: IndexMap::new(),
            error_handling: None,
        };

        let config = base_config();
        let result = resolve_channel(config, &co, &workspace, &[], &mut provenance);
        assert!(matches!(
            result,
            Err(ChannelError::CompositionNotFound { .. })
        ));
    }

    #[test]
    fn test_remove_transformation_by_name() {
        let (_tmp, workspace) = test_workspace();
        let mut provenance = ProvenanceMap::new();

        let co = ChannelOverride {
            header: ChannelOverrideHeader {
                pipeline: "test.yaml".into(),
                description: None,
                when: None,
            },
            inputs: IndexMap::new(),
            transformations: IndexMap::new(),
            add_transformations: vec![],
            add_compositions: vec![],
            remove_transformations: vec!["normalize_names".into()],
            remove_compositions: vec![],
            outputs: IndexMap::new(),
            error_handling: None,
        };

        let config = base_config();
        let result = resolve_channel(config, &co, &workspace, &[], &mut provenance).unwrap();
        assert_eq!(result.transformations.len(), 1);
        assert_eq!(result.transformations[0].name, "filter_active");
    }

    #[test]
    fn test_remove_transformation_not_found_is_warning() {
        let (_tmp, workspace) = test_workspace();
        let mut provenance = ProvenanceMap::new();

        let co = ChannelOverride {
            header: ChannelOverrideHeader {
                pipeline: "test.yaml".into(),
                description: None,
                when: None,
            },
            inputs: IndexMap::new(),
            transformations: IndexMap::new(),
            add_transformations: vec![],
            add_compositions: vec![],
            remove_transformations: vec!["nonexistent_transform".into()],
            remove_compositions: vec![],
            outputs: IndexMap::new(),
            error_handling: None,
        };

        let config = base_config();
        // Should not error, just warn
        let result = resolve_channel(config, &co, &workspace, &[], &mut provenance).unwrap();
        assert_eq!(result.transformations.len(), 2); // unchanged
    }

    #[test]
    fn test_remove_composition_removes_all_from_source() {
        let (tmp, workspace) = test_workspace();
        let mut provenance = ProvenanceMap::new();

        // Create a composition file to establish canonical path
        let comp_dir = tmp.path().join("compositions");
        std::fs::create_dir_all(&comp_dir).unwrap();
        let comp_path = comp_dir.join("legacy.comp.yaml");
        std::fs::write(
            &comp_path,
            "_composition:\n  name: legacy\ntransformations:\n  - name: legacy_a\n    cxl: \"emit a = 1\"\n",
        )
        .unwrap();

        let canonical = std::fs::canonicalize(&comp_path).unwrap();

        // Pre-populate: base config with transforms that came from this composition
        let mut config = base_config();
        config.transformations.push(TransformConfig {
            name: "legacy_a".into(),
            description: None,
            cxl: "emit a = 1".into(),
            local_window: None,
            log: None,
            validations: None,
        });
        config.transformations.push(TransformConfig {
            name: "legacy_b".into(),
            description: None,
            cxl: "emit b = 2".into(),
            local_window: None,
            log: None,
            validations: None,
        });
        provenance.insert("legacy_a".into(), canonical.clone());
        provenance.insert("legacy_b".into(), canonical.clone());

        let co = ChannelOverride {
            header: ChannelOverrideHeader {
                pipeline: "test.yaml".into(),
                description: None,
                when: None,
            },
            inputs: IndexMap::new(),
            transformations: IndexMap::new(),
            add_transformations: vec![],
            add_compositions: vec![],
            remove_transformations: vec![],
            remove_compositions: vec![RemoveComposition {
                r#ref: "compositions/legacy.comp.yaml".into(),
            }],
            outputs: IndexMap::new(),
            error_handling: None,
        };

        let result = resolve_channel(config, &co, &workspace, &[], &mut provenance).unwrap();
        // Both legacy transforms removed
        assert_eq!(result.transformations.len(), 2); // only original filter_active + normalize_names
        assert!(!result.transformations.iter().any(|t| t.name.starts_with("legacy")));
        assert!(!provenance.contains_key("legacy_a"));
        assert!(!provenance.contains_key("legacy_b"));
    }

    #[test]
    fn test_remove_composition_not_found_is_warning() {
        let (tmp, workspace) = test_workspace();
        let mut provenance = ProvenanceMap::new();

        // Create the file so canonicalize works, but don't put it in provenance
        let comp_dir = tmp.path().join("compositions");
        std::fs::create_dir_all(&comp_dir).unwrap();
        std::fs::write(
            comp_dir.join("unknown.comp.yaml"),
            "_composition:\n  name: unknown\ntransformations: []\n",
        )
        .unwrap();

        let co = ChannelOverride {
            header: ChannelOverrideHeader {
                pipeline: "test.yaml".into(),
                description: None,
                when: None,
            },
            inputs: IndexMap::new(),
            transformations: IndexMap::new(),
            add_transformations: vec![],
            add_compositions: vec![],
            remove_transformations: vec![],
            remove_compositions: vec![RemoveComposition {
                r#ref: "compositions/unknown.comp.yaml".into(),
            }],
            outputs: IndexMap::new(),
            error_handling: None,
        };

        let config = base_config();
        // Should not error, just warn
        let result = resolve_channel(config, &co, &workspace, &[], &mut provenance).unwrap();
        assert_eq!(result.transformations.len(), 2); // unchanged
    }

    #[test]
    fn test_override_output_path() {
        let (_tmp, workspace) = test_workspace();
        let mut provenance = ProvenanceMap::new();

        let co = ChannelOverride {
            header: ChannelOverrideHeader {
                pipeline: "test.yaml".into(),
                description: None,
                when: None,
            },
            inputs: IndexMap::new(),
            transformations: IndexMap::new(),
            add_transformations: vec![],
            add_compositions: vec![],
            remove_transformations: vec![],
            remove_compositions: vec![],
            outputs: IndexMap::from([(
                "results".into(),
                OutputOverrideDelta {
                    path: Some("/data/acme/output.csv".into()),
                    mapping: None,
                    exclude: None,
                    sort_order: None,
                    include_header: None,
                    preserve_nulls: None,
                },
            )]),
            error_handling: None,
        };

        let config = base_config();
        let result = resolve_channel(config, &co, &workspace, &[], &mut provenance).unwrap();
        assert_eq!(result.outputs[0].path, "/data/acme/output.csv");
        // Mapping preserved
        assert!(result.outputs[0].mapping.is_some());
    }

    #[test]
    fn test_override_error_handling_replacement() {
        let (_tmp, workspace) = test_workspace();
        let mut provenance = ProvenanceMap::new();

        let co = ChannelOverride {
            header: ChannelOverrideHeader {
                pipeline: "test.yaml".into(),
                description: None,
                when: None,
            },
            inputs: IndexMap::new(),
            transformations: IndexMap::new(),
            add_transformations: vec![],
            add_compositions: vec![],
            remove_transformations: vec![],
            remove_compositions: vec![],
            outputs: IndexMap::new(),
            error_handling: Some(ErrorHandlingConfig {
                strategy: ErrorStrategy::Continue,
                dlq: None,
                type_error_threshold: None,
            }),
        };

        let config = base_config();
        let result = resolve_channel(config, &co, &workspace, &[], &mut provenance).unwrap();
        assert_eq!(result.error_handling.strategy, ErrorStrategy::Continue);
    }

    #[test]
    fn test_override_target_not_found_errors() {
        let (_tmp, workspace) = test_workspace();
        let mut provenance = ProvenanceMap::new();

        let co = ChannelOverride {
            header: ChannelOverrideHeader {
                pipeline: "test.yaml".into(),
                description: None,
                when: None,
            },
            inputs: IndexMap::from([(
                "nonexistent_input".into(),
                InputOverrideDelta {
                    path: Some("/data/new.csv".into()),
                    schema: None,
                    schema_overrides: None,
                    sort_order: None,
                },
            )]),
            transformations: IndexMap::new(),
            add_transformations: vec![],
            add_compositions: vec![],
            remove_transformations: vec![],
            remove_compositions: vec![],
            outputs: IndexMap::new(),
            error_handling: None,
        };

        let config = base_config();
        let result = resolve_channel(config, &co, &workspace, &[], &mut provenance);
        assert!(matches!(
            result,
            Err(ChannelError::OverrideTargetNotFound { .. })
        ));
    }

    #[test]
    fn test_channel_vars_in_paths() {
        let (_tmp, workspace) = test_workspace();
        let mut provenance = ProvenanceMap::new();

        // Load from YAML with ${CHANNEL_DATA_DIR} already substituted
        // (interpolation happens before parsing — we test the end state)
        let co = ChannelOverride {
            header: ChannelOverrideHeader {
                pipeline: "test.yaml".into(),
                description: None,
                when: None,
            },
            inputs: IndexMap::from([(
                "customers".into(),
                InputOverrideDelta {
                    path: Some("/data/acme/customers.csv".into()),
                    schema: None,
                    schema_overrides: None,
                    sort_order: None,
                },
            )]),
            transformations: IndexMap::new(),
            add_transformations: vec![],
            add_compositions: vec![],
            remove_transformations: vec![],
            remove_compositions: vec![],
            outputs: IndexMap::new(),
            error_handling: None,
        };

        let config = base_config();
        let result = resolve_channel(config, &co, &workspace, &[], &mut provenance).unwrap();
        assert_eq!(result.inputs[0].path, "/data/acme/customers.csv");
    }

    // ---- Group inheritance tests ----

    /// Helper: set up a workspace with channel + group directories and a base pipeline.
    fn setup_inheritance_workspace() -> (tempfile::TempDir, WorkspaceRoot, PathBuf) {
        let tmp = tempfile::TempDir::new().unwrap();
        let ws = tmp.path();
        std::fs::write(ws.join("clinker.toml"), "[workspace]\n").unwrap();

        // Create channel dir with channel.yaml
        let channel_dir = ws.join("channels/acme");
        std::fs::create_dir_all(&channel_dir).unwrap();

        // Create groups dir
        std::fs::create_dir_all(ws.join("_groups")).unwrap();

        // Create a pipeline file
        let pipeline = ws.join("pipelines/etl.yaml");
        std::fs::create_dir_all(ws.join("pipelines")).unwrap();
        std::fs::write(&pipeline, "").unwrap();

        let workspace = WorkspaceRoot::at(ws).unwrap();
        (tmp, workspace, pipeline)
    }

    #[test]
    fn test_group_single_applies_before_channel() {
        let (tmp, workspace, pipeline) = setup_inheritance_workspace();
        let ws = tmp.path();

        // Channel inherits from enterprise-base
        std::fs::write(
            ws.join("channels/acme/channel.yaml"),
            "_channel:\n  id: acme\n  name: Acme\n  inherits: enterprise-base\n",
        )
        .unwrap();

        // Group override: changes filter_active CXL
        let group_dir = ws.join("_groups/enterprise-base");
        std::fs::create_dir_all(&group_dir).unwrap();
        std::fs::write(
            group_dir.join("etl.channel.yaml"),
            r#"
_channel:
  pipeline: etl.yaml
transformations:
  filter_active:
    cxl: "emit group_applied = true"
"#,
        )
        .unwrap();

        // Channel override: changes normalize_names CXL
        std::fs::write(
            ws.join("channels/acme/etl.channel.yaml"),
            r#"
_channel:
  pipeline: etl.yaml
transformations:
  normalize_names:
    cxl: "emit channel_applied = true"
"#,
        )
        .unwrap();

        let mut provenance = ProvenanceMap::new();
        let config = base_config();
        let result = resolve_channel_with_inheritance(
            config, "acme", &pipeline, &workspace, &[], &mut provenance,
        )
        .unwrap();

        // Group changed filter_active
        assert!(result.transformations[0].cxl.contains("group_applied"));
        // Channel changed normalize_names
        assert!(result.transformations[1].cxl.contains("channel_applied"));
    }

    #[test]
    fn test_group_multiple_order() {
        let (tmp, workspace, pipeline) = setup_inheritance_workspace();
        let ws = tmp.path();

        // Channel inherits from [group-a, group-b]
        std::fs::write(
            ws.join("channels/acme/channel.yaml"),
            "_channel:\n  id: acme\n  name: Acme\n  inherits: [group-a, group-b]\n",
        )
        .unwrap();

        // Group A: changes filter_active
        let group_a = ws.join("_groups/group-a");
        std::fs::create_dir_all(&group_a).unwrap();
        std::fs::write(
            group_a.join("etl.channel.yaml"),
            "_channel:\n  pipeline: etl.yaml\ntransformations:\n  filter_active:\n    cxl: \"emit from_group_a = true\"\n",
        )
        .unwrap();

        // Group B: also changes filter_active (should win over A)
        let group_b = ws.join("_groups/group-b");
        std::fs::create_dir_all(&group_b).unwrap();
        std::fs::write(
            group_b.join("etl.channel.yaml"),
            "_channel:\n  pipeline: etl.yaml\ntransformations:\n  filter_active:\n    cxl: \"emit from_group_b = true\"\n",
        )
        .unwrap();

        // No channel-specific override
        let mut provenance = ProvenanceMap::new();
        let config = base_config();
        let result = resolve_channel_with_inheritance(
            config, "acme", &pipeline, &workspace, &[], &mut provenance,
        )
        .unwrap();

        // Group B (later) wins over Group A on same transform
        assert!(result.transformations[0].cxl.contains("from_group_b"));
    }

    #[test]
    fn test_group_channel_wins() {
        let (tmp, workspace, pipeline) = setup_inheritance_workspace();
        let ws = tmp.path();

        std::fs::write(
            ws.join("channels/acme/channel.yaml"),
            "_channel:\n  id: acme\n  name: Acme\n  inherits: enterprise-base\n",
        )
        .unwrap();

        // Group override: changes filter_active
        let group_dir = ws.join("_groups/enterprise-base");
        std::fs::create_dir_all(&group_dir).unwrap();
        std::fs::write(
            group_dir.join("etl.channel.yaml"),
            "_channel:\n  pipeline: etl.yaml\ntransformations:\n  filter_active:\n    cxl: \"emit from_group = true\"\n",
        )
        .unwrap();

        // Channel override: also changes filter_active (should win)
        std::fs::write(
            ws.join("channels/acme/etl.channel.yaml"),
            "_channel:\n  pipeline: etl.yaml\ntransformations:\n  filter_active:\n    cxl: \"emit from_channel = true\"\n",
        )
        .unwrap();

        let mut provenance = ProvenanceMap::new();
        let config = base_config();
        let result = resolve_channel_with_inheritance(
            config, "acme", &pipeline, &workspace, &[], &mut provenance,
        )
        .unwrap();

        // Channel wins over group
        assert!(result.transformations[0].cxl.contains("from_channel"));
    }

    #[test]
    fn test_group_no_override_for_pipeline() {
        let (tmp, workspace, pipeline) = setup_inheritance_workspace();
        let ws = tmp.path();

        std::fs::write(
            ws.join("channels/acme/channel.yaml"),
            "_channel:\n  id: acme\n  name: Acme\n  inherits: enterprise-base\n",
        )
        .unwrap();

        // Group dir exists but has NO etl.channel.yaml → skip
        let group_dir = ws.join("_groups/enterprise-base");
        std::fs::create_dir_all(&group_dir).unwrap();

        let mut provenance = ProvenanceMap::new();
        let config = base_config();
        let result = resolve_channel_with_inheritance(
            config, "acme", &pipeline, &workspace, &[], &mut provenance,
        )
        .unwrap();

        // Base config unchanged
        assert!(result.transformations[0].cxl.contains("status != \"active\""));
    }

    #[test]
    fn test_group_not_found() {
        let (tmp, workspace, pipeline) = setup_inheritance_workspace();
        let ws = tmp.path();

        std::fs::write(
            ws.join("channels/acme/channel.yaml"),
            "_channel:\n  id: acme\n  name: Acme\n  inherits: nonexistent\n",
        )
        .unwrap();

        let mut provenance = ProvenanceMap::new();
        let config = base_config();
        let result = resolve_channel_with_inheritance(
            config, "acme", &pipeline, &workspace, &[], &mut provenance,
        );

        assert!(matches!(result, Err(ChannelError::GroupNotFound { .. })));
    }

    #[test]
    fn test_group_when_condition_skips() {
        let (tmp, workspace, pipeline) = setup_inheritance_workspace();
        let ws = tmp.path();

        std::fs::write(
            ws.join("channels/acme/channel.yaml"),
            "_channel:\n  id: acme\n  name: Acme\n  inherits: enterprise-base\n",
        )
        .unwrap();

        // Group override with when: condition that is false
        let group_dir = ws.join("_groups/enterprise-base");
        std::fs::create_dir_all(&group_dir).unwrap();
        std::fs::write(
            group_dir.join("etl.channel.yaml"),
            "_channel:\n  pipeline: etl.yaml\n  when: \"dev == prod\"\ntransformations:\n  filter_active:\n    cxl: \"emit should_not_apply = true\"\n",
        )
        .unwrap();

        let mut provenance = ProvenanceMap::new();
        let config = base_config();
        let result = resolve_channel_with_inheritance(
            config, "acme", &pipeline, &workspace, &[], &mut provenance,
        )
        .unwrap();

        // Group override skipped — base unchanged
        assert!(!result.transformations[0].cxl.contains("should_not_apply"));
    }

    #[test]
    fn test_group_remove_composition_propagates() {
        let (tmp, workspace, pipeline) = setup_inheritance_workspace();
        let ws = tmp.path();

        // Create composition file
        let comp_dir = tmp.path().join("compositions");
        std::fs::create_dir_all(&comp_dir).unwrap();
        let comp_path = comp_dir.join("legacy.comp.yaml");
        std::fs::write(
            &comp_path,
            "_composition:\n  name: legacy\ntransformations:\n  - name: legacy_step\n    cxl: \"emit x = 1\"\n",
        )
        .unwrap();
        let canonical = std::fs::canonicalize(&comp_path).unwrap();

        std::fs::write(
            ws.join("channels/acme/channel.yaml"),
            "_channel:\n  id: acme\n  name: Acme\n  inherits: enterprise-base\n",
        )
        .unwrap();

        // Group removes the composition
        let group_dir = ws.join("_groups/enterprise-base");
        std::fs::create_dir_all(&group_dir).unwrap();
        std::fs::write(
            group_dir.join("etl.channel.yaml"),
            "_channel:\n  pipeline: etl.yaml\nremove_compositions:\n  - ref: compositions/legacy.comp.yaml\n",
        )
        .unwrap();

        // Pre-populate config and provenance with composition transforms
        let mut config = base_config();
        config.transformations.push(TransformConfig {
            name: "legacy_step".into(),
            description: None,
            cxl: "emit x = 1".into(),
            local_window: None,
            log: None,
            validations: None,
        });
        let mut provenance = ProvenanceMap::new();
        provenance.insert("legacy_step".into(), canonical);

        let result = resolve_channel_with_inheritance(
            config, "acme", &pipeline, &workspace, &[], &mut provenance,
        )
        .unwrap();

        // Group removed the composition's transforms — channel sees them gone
        assert!(!result.transformations.iter().any(|t| t.name == "legacy_step"));
        assert!(!provenance.contains_key("legacy_step"));
    }

    #[test]
    fn test_no_inheritance() {
        let (tmp, workspace, pipeline) = setup_inheritance_workspace();
        let ws = tmp.path();

        // Channel with no inherits
        std::fs::write(
            ws.join("channels/acme/channel.yaml"),
            "_channel:\n  id: acme\n  name: Acme\n",
        )
        .unwrap();

        // Channel-specific override
        std::fs::write(
            ws.join("channels/acme/etl.channel.yaml"),
            "_channel:\n  pipeline: etl.yaml\ntransformations:\n  filter_active:\n    cxl: \"emit channel_only = true\"\n",
        )
        .unwrap();

        let mut provenance = ProvenanceMap::new();
        let config = base_config();
        let result = resolve_channel_with_inheritance(
            config, "acme", &pipeline, &workspace, &[], &mut provenance,
        )
        .unwrap();

        assert!(result.transformations[0].cxl.contains("channel_only"));
    }
}
