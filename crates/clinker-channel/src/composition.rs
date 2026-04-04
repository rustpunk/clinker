use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use clinker_core::config::{PipelineConfig, TransformConfig, TransformEntry};
use serde::Deserialize;

use crate::error::ChannelError;
use crate::workspace::WorkspaceRoot;

/// Maps transform name → source `.comp.yaml` path (workspace-root-relative, canonical).
/// Transforms not from a composition are absent from this map.
pub type ProvenanceMap = HashMap<String, PathBuf>;

const MAX_IMPORT_DEPTH: usize = 10;

/// A parsed `.comp.yaml` composition file.
#[derive(Debug, Deserialize)]
pub struct CompositionFile {
    #[serde(rename = "_compose")]
    pub header: CompositionHeader,
    #[serde(default)]
    pub transformations: Vec<TransformEntry>,
}

/// Header section of a `.comp.yaml` file.
#[derive(Debug, Deserialize)]
pub struct CompositionHeader {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
}

impl CompositionFile {
    /// Load and parse a `.comp.yaml` file.
    pub fn load(path: &Path) -> Result<Self, ChannelError> {
        let raw = std::fs::read_to_string(path).map_err(|_| ChannelError::CompositionNotFound {
            path: path.to_path_buf(),
        })?;
        let parsed: CompositionFile = serde_saphyr::from_str(&raw)?;
        Ok(parsed)
    }
}

/// Resolve all `_import` directives in a pipeline's transformations list.
///
/// Mutates `config.transformations` in place — all `_import` entries are replaced
/// by the transforms from the referenced `.comp.yaml` files (recursively).
///
/// Returns a `ProvenanceMap` mapping transform names to their source `.comp.yaml`
/// canonical path. Transforms native to the pipeline are absent from the map.
pub fn resolve_compositions(
    config: &mut PipelineConfig,
    workspace: &WorkspaceRoot,
    channel_vars: &[(&str, &str)],
) -> Result<ProvenanceMap, ChannelError> {
    let mut provenance = ProvenanceMap::new();
    let mut seen = HashSet::new();
    config.transformations = resolve_entries(
        std::mem::take(&mut config.transformations),
        workspace,
        channel_vars,
        &mut provenance,
        &mut seen,
        0,
    )?;
    Ok(provenance)
}

fn resolve_entries(
    entries: Vec<TransformEntry>,
    workspace: &WorkspaceRoot,
    channel_vars: &[(&str, &str)],
    provenance: &mut ProvenanceMap,
    seen: &mut HashSet<PathBuf>,
    depth: usize,
) -> Result<Vec<TransformEntry>, ChannelError> {
    if depth > MAX_IMPORT_DEPTH {
        return Err(ChannelError::ImportDepthExceeded {
            path: PathBuf::new(),
            depth,
        });
    }
    let mut result = Vec::new();
    for entry in entries {
        match entry {
            TransformEntry::Import { _import } => {
                // Interpolate channel vars in the import path
                let interpolated_path =
                    clinker_core::config::interpolate_env_vars(&_import, channel_vars).map_err(
                        |e| {
                            ChannelError::Validation(format!(
                                "failed to interpolate _import path '{}': {}",
                                _import, e
                            ))
                        },
                    )?;

                let path = workspace.resolve_comp_path(&interpolated_path);
                let canonical = std::fs::canonicalize(&path)
                    .map_err(|_| ChannelError::CompositionNotFound { path: path.clone() })?;

                if !seen.insert(canonical.clone()) {
                    return Err(ChannelError::CircularImport { path: canonical });
                }

                let comp = CompositionFile::load(&path)?;
                let resolved = resolve_entries(
                    comp.transformations,
                    workspace,
                    channel_vars,
                    provenance,
                    seen,
                    depth + 1,
                )?;

                for resolved_entry in &resolved {
                    if let TransformEntry::Transform(t) = resolved_entry {
                        provenance.insert(t.name.clone(), canonical.clone());
                    }
                }

                result.extend(resolved);
                seen.remove(&canonical); // allow same comp in different branches (diamond imports)
            }
            t @ TransformEntry::Transform(_) => result.push(t),
        }
    }
    Ok(result)
}

/// Resolve a list of transforms from a composition file, handling any nested
/// imports. Returns flat `Vec<TransformConfig>` and populates provenance.
///
/// Used by `add_compositions` in overrides (Task 10.3).
pub fn resolve_composition_transforms(
    entries: Vec<TransformEntry>,
    workspace: &WorkspaceRoot,
    channel_vars: &[(&str, &str)],
    provenance: &mut ProvenanceMap,
) -> Result<Vec<TransformConfig>, ChannelError> {
    let mut seen = HashSet::new();
    let resolved = resolve_entries(entries, workspace, channel_vars, provenance, &mut seen, 0)?;
    Ok(resolved
        .into_iter()
        .filter_map(|entry| match entry {
            TransformEntry::Transform(t) => Some(t),
            TransformEntry::Import { .. } => None, // should not happen after resolution
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::WorkspaceRoot;
    use clinker_core::config::*;

    /// Helper: create workspace with clinker.toml and return (TempDir, WorkspaceRoot).
    fn test_workspace() -> (tempfile::TempDir, WorkspaceRoot) {
        let tmp = tempfile::TempDir::new().unwrap();
        let ws = tmp.path();
        std::fs::write(ws.join("clinker.toml"), "[workspace]\n").unwrap();
        let workspace = WorkspaceRoot::at(ws).unwrap();
        (tmp, workspace)
    }

    /// Helper: write a composition file to the workspace.
    fn write_comp(ws_root: &Path, rel_path: &str, content: &str) {
        let full_path = ws_root.join(rel_path);
        std::fs::create_dir_all(full_path.parent().unwrap()).unwrap();
        std::fs::write(full_path, content).unwrap();
    }

    /// Helper: minimal pipeline config for testing.
    fn base_config_with_transforms(transforms: Vec<TransformEntry>) -> PipelineConfig {
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
                name: "primary".into(),
                path: "data.csv".into(),
                schema: None,
                schema_overrides: None,
                array_paths: None,
                sort_order: None,
                format: InputFormat::Csv(None),
                notes: None,
            }],
            outputs: vec![OutputConfig {
                name: "results".into(),
                path: "output.csv".into(),
                include_unmapped: false,
                include_header: None,
                mapping: None,
                exclude: None,
                sort_order: None,
                preserve_nulls: None,
                include_metadata: Default::default(),
                split: None,
                format: OutputFormat::Csv(None),
                notes: None,
            }],
            transformations: transforms,
            error_handling: ErrorHandlingConfig::default(),
            notes: None,
        }
    }

    fn make_transform(name: &str, cxl: &str) -> TransformEntry {
        TransformEntry::Transform(TransformConfig {
            name: name.into(),
            description: None,
            cxl: cxl.into(),
            local_window: None,
            log: None,
            validations: None,
            route: None,
            notes: None,
        })
    }

    fn make_import(path: &str) -> TransformEntry {
        TransformEntry::Import {
            _import: path.into(),
        }
    }

    #[test]
    fn test_composition_load_basic() {
        let (tmp, _ws) = test_workspace();
        write_comp(
            tmp.path(),
            "compositions/clean.comp.yaml",
            r#"
_compose:
  name: clean_customer
  description: "Standard normalization"

transformations:
  - name: trim_names
    cxl: "emit first_name = first_name.trim()"
  - name: lower_email
    cxl: "emit email = email.lower()"
"#,
        );

        let comp = CompositionFile::load(&tmp.path().join("compositions/clean.comp.yaml")).unwrap();
        assert_eq!(comp.header.name, "clean_customer");
        assert_eq!(
            comp.header.description.as_deref(),
            Some("Standard normalization")
        );
        assert_eq!(comp.transformations.len(), 2);
    }

    #[test]
    fn test_resolve_inlines_at_position() {
        let (tmp, ws) = test_workspace();
        write_comp(
            tmp.path(),
            "compositions/audit.comp.yaml",
            r#"
_compose:
  name: audit

transformations:
  - name: audit_stamp
    cxl: "emit ts = now()"
"#,
        );

        let mut config = base_config_with_transforms(vec![
            make_transform("before", "emit a = 1"),
            make_import("compositions/audit.comp.yaml"),
            make_transform("after", "emit b = 2"),
        ]);

        let provenance = resolve_compositions(&mut config, &ws, &[]).unwrap();
        assert_eq!(config.transformations.len(), 3);
        let names: Vec<_> = config.transforms().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["before", "audit_stamp", "after"]);
        assert!(provenance.contains_key("audit_stamp"));
    }

    #[test]
    fn test_resolve_multiple_imports() {
        let (tmp, ws) = test_workspace();
        write_comp(
            tmp.path(),
            "compositions/a.comp.yaml",
            "_compose:\n  name: a\ntransformations:\n  - name: from_a\n    cxl: \"emit a = 1\"\n",
        );
        write_comp(
            tmp.path(),
            "compositions/b.comp.yaml",
            "_compose:\n  name: b\ntransformations:\n  - name: from_b\n    cxl: \"emit b = 2\"\n",
        );

        let mut config = base_config_with_transforms(vec![
            make_import("compositions/a.comp.yaml"),
            make_import("compositions/b.comp.yaml"),
        ]);

        resolve_compositions(&mut config, &ws, &[]).unwrap();
        let names: Vec<_> = config.transforms().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["from_a", "from_b"]);
    }

    #[test]
    fn test_resolve_preserves_surrounding() {
        let (tmp, ws) = test_workspace();
        write_comp(
            tmp.path(),
            "compositions/mid.comp.yaml",
            "_compose:\n  name: mid\ntransformations:\n  - name: imported\n    cxl: \"emit x = 1\"\n",
        );

        let mut config = base_config_with_transforms(vec![
            make_transform("first", "emit a = 1"),
            make_import("compositions/mid.comp.yaml"),
            make_transform("last", "emit z = 26"),
        ]);

        resolve_compositions(&mut config, &ws, &[]).unwrap();
        let names: Vec<_> = config.transforms().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["first", "imported", "last"]);
    }

    #[test]
    fn test_resolve_nested_import() {
        let (tmp, ws) = test_workspace();
        write_comp(
            tmp.path(),
            "compositions/inner.comp.yaml",
            "_compose:\n  name: inner\ntransformations:\n  - name: deep\n    cxl: \"emit deep = true\"\n",
        );
        write_comp(
            tmp.path(),
            "compositions/outer.comp.yaml",
            "_compose:\n  name: outer\ntransformations:\n  - name: shallow\n    cxl: \"emit shallow = true\"\n  - _import: compositions/inner.comp.yaml\n",
        );

        let mut config =
            base_config_with_transforms(vec![make_import("compositions/outer.comp.yaml")]);

        let provenance = resolve_compositions(&mut config, &ws, &[]).unwrap();
        let names: Vec<_> = config.transforms().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["shallow", "deep"]);
        assert!(provenance.contains_key("shallow"));
        assert!(provenance.contains_key("deep"));
    }

    #[test]
    fn test_resolve_circular_detected() {
        let (tmp, ws) = test_workspace();
        write_comp(
            tmp.path(),
            "compositions/a.comp.yaml",
            "_compose:\n  name: a\ntransformations:\n  - _import: compositions/b.comp.yaml\n",
        );
        write_comp(
            tmp.path(),
            "compositions/b.comp.yaml",
            "_compose:\n  name: b\ntransformations:\n  - _import: compositions/a.comp.yaml\n",
        );

        let mut config = base_config_with_transforms(vec![make_import("compositions/a.comp.yaml")]);

        let result = resolve_compositions(&mut config, &ws, &[]);
        assert!(matches!(result, Err(ChannelError::CircularImport { .. })));
    }

    #[test]
    fn test_resolve_depth_limit() {
        let (tmp, ws) = test_workspace();
        // Create a chain of 12 compositions (exceeds MAX_IMPORT_DEPTH=10)
        for i in 0..12 {
            let next = if i < 11 {
                format!("  - _import: compositions/chain_{}.comp.yaml\n", i + 1)
            } else {
                "  - name: end\n    cxl: \"emit x = 1\"\n".to_string()
            };
            write_comp(
                tmp.path(),
                &format!("compositions/chain_{}.comp.yaml", i),
                &format!("_compose:\n  name: chain_{}\ntransformations:\n{}", i, next),
            );
        }

        let mut config =
            base_config_with_transforms(vec![make_import("compositions/chain_0.comp.yaml")]);

        let result = resolve_compositions(&mut config, &ws, &[]);
        assert!(matches!(
            result,
            Err(ChannelError::ImportDepthExceeded { .. })
        ));
    }

    #[test]
    fn test_resolve_provenance_map_populated() {
        let (tmp, ws) = test_workspace();
        write_comp(
            tmp.path(),
            "compositions/audit.comp.yaml",
            "_compose:\n  name: audit\ntransformations:\n  - name: audit_ts\n    cxl: \"emit ts = now()\"\n  - name: audit_user\n    cxl: \"emit user = system\"\n",
        );

        let mut config = base_config_with_transforms(vec![
            make_transform("native", "emit x = 1"),
            make_import("compositions/audit.comp.yaml"),
        ]);

        let provenance = resolve_compositions(&mut config, &ws, &[]).unwrap();
        assert!(provenance.contains_key("audit_ts"));
        assert!(provenance.contains_key("audit_user"));
        // Both point to the same canonical path
        assert_eq!(provenance["audit_ts"], provenance["audit_user"]);
    }

    #[test]
    fn test_resolve_native_transforms_absent_from_provenance() {
        let (tmp, ws) = test_workspace();
        write_comp(
            tmp.path(),
            "compositions/audit.comp.yaml",
            "_compose:\n  name: audit\ntransformations:\n  - name: imported\n    cxl: \"emit x = 1\"\n",
        );

        let mut config = base_config_with_transforms(vec![
            make_transform("native", "emit a = 1"),
            make_import("compositions/audit.comp.yaml"),
        ]);

        let provenance = resolve_compositions(&mut config, &ws, &[]).unwrap();
        assert!(!provenance.contains_key("native"));
        assert!(provenance.contains_key("imported"));
    }

    #[test]
    fn test_resolve_channel_var_in_path() {
        let (tmp, ws) = test_workspace();
        write_comp(
            tmp.path(),
            "libs/audit.comp.yaml",
            "_compose:\n  name: audit\ntransformations:\n  - name: from_var_path\n    cxl: \"emit x = 1\"\n",
        );

        let mut config =
            base_config_with_transforms(vec![make_import("${COMP_DIR}/audit.comp.yaml")]);

        let vars = [("COMP_DIR", "libs")];
        let provenance = resolve_compositions(&mut config, &ws, &vars).unwrap();
        let names: Vec<_> = config.transforms().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["from_var_path"]);
        assert!(provenance.contains_key("from_var_path"));
    }

    #[test]
    fn test_resolve_workspace_relative_path() {
        let (tmp, ws) = test_workspace();
        // Composition at workspace_root/shared/comps/audit.comp.yaml
        write_comp(
            tmp.path(),
            "shared/comps/audit.comp.yaml",
            "_compose:\n  name: audit\ntransformations:\n  - name: ws_relative\n    cxl: \"emit x = 1\"\n",
        );

        // Import path is workspace-root-relative, NOT relative to the importing file
        let mut config =
            base_config_with_transforms(vec![make_import("shared/comps/audit.comp.yaml")]);

        resolve_compositions(&mut config, &ws, &[]).unwrap();
        let names: Vec<_> = config.transforms().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["ws_relative"]);
    }

    #[test]
    fn test_resolve_not_found() {
        let (_tmp, ws) = test_workspace();
        let mut config =
            base_config_with_transforms(vec![make_import("compositions/nonexistent.comp.yaml")]);

        let result = resolve_compositions(&mut config, &ws, &[]);
        assert!(matches!(
            result,
            Err(ChannelError::CompositionNotFound { .. })
        ));
    }

    #[test]
    fn test_resolve_no_imports_noop() {
        let (_tmp, ws) = test_workspace();
        let mut config = base_config_with_transforms(vec![
            make_transform("a", "emit a = 1"),
            make_transform("b", "emit b = 2"),
        ]);

        let provenance = resolve_compositions(&mut config, &ws, &[]).unwrap();
        assert!(provenance.is_empty());
        assert_eq!(config.transformations.len(), 2);
        let names: Vec<_> = config.transforms().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b"]);
    }
}
