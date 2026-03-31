//! Schema and pipeline discovery from workspace directories.
//!
//! Scans the workspace for `.schema.yaml` files and pipeline YAML files,
//! parses schemas, resolves `schema:` references in pipelines to populate
//! `referencing_pipelines`, and builds a `SchemaIndex`.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::model::{SchemaIndex, SourceSchema};
use crate::parse::{SchemaParseError, parse_schema};

/// Default schema directory name relative to workspace root.
pub const DEFAULT_SCHEMA_DIR: &str = "schemas";

/// Discover all `.schema.yaml` files in the given directory (non-recursive).
///
/// Returns parsed schemas with their `path` fields set. The caller is
/// responsible for populating `referencing_pipelines` via
/// [`resolve_schema_references`].
pub fn discover_schemas(
    schema_dir: &Path,
) -> Vec<Result<SourceSchema, (PathBuf, SchemaParseError)>> {
    let Ok(entries) = fs::read_dir(schema_dir) else {
        return Vec::new();
    };

    entries
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|ext| ext == "yaml" || ext == "yml")
                && entry
                    .path()
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .is_some_and(|name| name.ends_with(".schema"))
        })
        .map(|entry| {
            let path = entry.path();
            let content = fs::read_to_string(&path)
                .map_err(|e| (path.clone(), SchemaParseError::Io(e.to_string())))?;
            parse_schema(&content, &path).map_err(|e| (path, e))
        })
        .collect()
}

/// Discover pipeline YAML files in the workspace.
///
/// Scans the workspace root for `.yaml`/`.yml` files (non-recursive by default),
/// and optionally uses include/exclude glob patterns from the manifest.
/// Excludes the schema directory and common non-pipeline directories.
pub fn discover_pipelines(
    workspace_root: &Path,
    schema_dir: &str,
    include_globs: &[String],
    exclude_globs: &[String],
) -> Vec<PathBuf> {
    // If include patterns are specified, use them; otherwise scan root
    if !include_globs.is_empty() {
        return discover_pipelines_with_globs(workspace_root, include_globs, exclude_globs);
    }

    // Default: scan workspace root (non-recursive) for YAML files
    let Ok(entries) = fs::read_dir(workspace_root) else {
        return Vec::new();
    };

    let schema_path = workspace_root.join(schema_dir);
    let templates_path = workspace_root.join("templates");

    entries
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            let path = entry.path();
            // Must be a file
            if !path.is_file() {
                return false;
            }
            // Must be YAML
            let is_yaml = path
                .extension()
                .is_some_and(|ext| ext == "yaml" || ext == "yml");
            if !is_yaml {
                return false;
            }
            // Exclude schema files
            let is_schema = path
                .file_stem()
                .and_then(|s| s.to_str())
                .is_some_and(|name| name.ends_with(".schema"));
            if is_schema {
                return false;
            }
            // Exclude kiln.toml companion files
            let is_kiln_meta = path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|name| name.starts_with(".kiln"));
            if is_kiln_meta {
                return false;
            }
            // Exclude if inside schema or template dirs
            if path.starts_with(&schema_path) || path.starts_with(&templates_path) {
                return false;
            }
            true
        })
        .map(|entry| entry.path())
        .collect()
}

/// Discover pipelines using include/exclude glob patterns.
fn discover_pipelines_with_globs(
    workspace_root: &Path,
    include_globs: &[String],
    _exclude_globs: &[String],
) -> Vec<PathBuf> {
    // Simple glob expansion — match files in workspace
    let mut results = Vec::new();
    for pattern in include_globs {
        let full_pattern = workspace_root.join(pattern).display().to_string();
        if let Ok(paths) = glob_paths(&full_pattern) {
            results.extend(paths);
        }
    }
    results.sort();
    results.dedup();
    results
}

/// Simple glob expansion using std::fs.
fn glob_paths(pattern: &str) -> Result<Vec<PathBuf>, ()> {
    // For now, just check if the pattern is a simple directory/*.yaml
    // Full glob support can be added with the `glob` crate later.
    let path = Path::new(pattern);
    if let Some(parent) = path.parent()
        && parent.is_dir()
    {
        let ext_match = path.extension().and_then(|e| e.to_str()).unwrap_or("yaml");
        let Ok(entries) = fs::read_dir(parent) else {
            return Ok(Vec::new());
        };
        return Ok(entries
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .is_some_and(|ext| ext == ext_match)
            })
            .map(|e| e.path())
            .collect());
    }
    Ok(Vec::new())
}

/// Extract `schema:` references from a pipeline YAML file.
///
/// Parses the YAML as raw text (fast regex scan) rather than full deserialization,
/// because we only need the schema paths — not the full pipeline model.
/// Returns schema paths as they appear in the YAML (may be relative).
pub fn extract_schema_refs(pipeline_path: &Path) -> Vec<String> {
    let Ok(content) = fs::read_to_string(pipeline_path) else {
        return Vec::new();
    };
    extract_schema_refs_from_str(&content)
}

/// Extract `schema:` references from YAML text.
fn extract_schema_refs_from_str(yaml: &str) -> Vec<String> {
    let mut refs = Vec::new();
    for line in yaml.lines() {
        let trimmed = line.trim();
        // Match "schema: path/to/file.schema.yaml" (not schema_overrides, schema_inline)
        if let Some(value) = trimmed.strip_prefix("schema:") {
            let value = value.trim().trim_matches('"').trim_matches('\'');
            if !value.is_empty() && !value.starts_with('{') && !value.starts_with('[') {
                refs.push(value.to_string());
            }
        }
    }
    refs
}

/// Resolve `schema:` references from pipelines to populate `referencing_pipelines`
/// on each schema.
///
/// `schema_refs` maps pipeline path → list of schema paths referenced in that pipeline.
/// Schema paths in pipelines are resolved relative to the workspace root.
pub fn resolve_schema_references(
    schemas: &mut [SourceSchema],
    workspace_root: &Path,
    pipeline_refs: &HashMap<PathBuf, Vec<String>>,
) {
    for (pipeline_path, refs) in pipeline_refs {
        for schema_ref in refs {
            // Resolve the schema path relative to workspace root
            let resolved = workspace_root.join(schema_ref);
            let canonical = resolved.canonicalize().unwrap_or(resolved);

            for schema in schemas.iter_mut() {
                let schema_canonical = schema
                    .path
                    .canonicalize()
                    .unwrap_or_else(|_| schema.path.clone());
                if schema_canonical == canonical
                    && !schema.referencing_pipelines.contains(pipeline_path)
                {
                    schema.referencing_pipelines.push(pipeline_path.clone());
                }
            }
        }
    }
}

/// Full workspace discovery: find schemas, find pipelines, resolve references,
/// build index.
///
/// This is the main entry point for schema discovery. Call on workspace load
/// and on file change events.
pub fn build_workspace_schema_index(
    workspace_root: &Path,
    schema_dir: &str,
    include_globs: &[String],
    exclude_globs: &[String],
) -> (SchemaIndex, Vec<(PathBuf, SchemaParseError)>) {
    let schema_path = workspace_root.join(schema_dir);

    // 1. Discover and parse schemas
    let results = discover_schemas(&schema_path);
    let mut schemas = Vec::new();
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok(schema) => schemas.push(schema),
            Err(e) => errors.push(e),
        }
    }

    // 2. Discover pipeline files
    let pipelines = discover_pipelines(workspace_root, schema_dir, include_globs, exclude_globs);

    // 3. Extract schema references from pipelines
    let mut pipeline_refs: HashMap<PathBuf, Vec<String>> = HashMap::new();
    for pipeline_path in &pipelines {
        let refs = extract_schema_refs(pipeline_path);
        if !refs.is_empty() {
            pipeline_refs.insert(pipeline_path.clone(), refs);
        }
    }

    // 4. Resolve references
    resolve_schema_references(&mut schemas, workspace_root, &pipeline_refs);

    // 5. Build index
    let index = SchemaIndex::build(schemas);

    (index, errors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn write_file(dir: &Path, name: &str, content: &str) {
        fs::write(dir.join(name), content).unwrap();
    }

    #[test]
    fn test_discover_schemas_in_directory() {
        let tmp = TempDir::new().unwrap();
        let schemas_dir = tmp.path().join("schemas");
        fs::create_dir(&schemas_dir).unwrap();

        write_file(
            &schemas_dir,
            "customers.schema.yaml",
            r#"
_schema:
  name: customers
  format: csv
fields:
  - name: id
    type: int
    nullable: false
  - name: email
    type: string
"#,
        );

        write_file(
            &schemas_dir,
            "events.schema.yaml",
            r#"
_schema:
  name: events
  format: jsonl
fields:
  - name: event_id
    type: string
    nullable: false
"#,
        );

        // Non-schema YAML should be ignored
        write_file(&schemas_dir, "not-a-schema.yaml", "key: value");

        let results = discover_schemas(&schemas_dir);
        let schemas: Vec<_> = results.into_iter().filter_map(|r| r.ok()).collect();

        assert_eq!(schemas.len(), 2);
        let names: Vec<_> = schemas.iter().map(|s| s.metadata.name.as_str()).collect();
        assert!(names.contains(&"customers"));
        assert!(names.contains(&"events"));
    }

    #[test]
    fn test_extract_schema_refs_from_yaml() {
        let yaml = r#"
pipeline:
  name: test

inputs:
  - name: source
    type: csv
    path: ./data/customers.csv
    schema: schemas/customers.schema.yaml

transformations:
  - name: enrich
    cxl: "emit x = a"

outputs:
  - name: dest
    type: csv
    path: ./output.csv
"#;
        let refs = extract_schema_refs_from_str(yaml);
        assert_eq!(refs, vec!["schemas/customers.schema.yaml"]);
    }

    #[test]
    fn test_extract_schema_refs_ignores_inline_and_overrides() {
        let yaml = r#"
inputs:
  - name: source
    type: csv
    path: ./data/input.csv
    schema: schemas/main.schema.yaml
    schema_overrides:
      - name: id
        type: int
"#;
        let refs = extract_schema_refs_from_str(yaml);
        // Should only get the schema: reference, not schema_overrides
        assert_eq!(refs, vec!["schemas/main.schema.yaml"]);
    }

    #[test]
    fn test_full_workspace_discovery() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();

        // Create schema dir
        let schemas_dir = root.join("schemas");
        fs::create_dir(&schemas_dir).unwrap();

        write_file(
            &schemas_dir,
            "customers.schema.yaml",
            r#"
_schema:
  name: customers
  format: csv
fields:
  - name: id
    type: int
    nullable: false
  - name: email
    type: string
"#,
        );

        // Create pipeline file referencing the schema
        write_file(
            root,
            "pipeline.yaml",
            r#"
pipeline:
  name: test

inputs:
  - name: source
    type: csv
    path: ./data/customers.csv
    schema: schemas/customers.schema.yaml

transformations:
  - name: t1
    cxl: "emit x = id"

outputs:
  - name: dest
    type: csv
    path: ./output.csv
"#,
        );

        let (index, errors) = build_workspace_schema_index(root, "schemas", &[], &[]);

        assert!(errors.is_empty());
        assert_eq!(index.len(), 1);

        // Schema should know about the referencing pipeline
        let schema_path = schemas_dir.join("customers.schema.yaml");
        let schema = index.get(&schema_path).unwrap();
        assert_eq!(schema.metadata.name, "customers");
        assert_eq!(schema.referencing_pipelines.len(), 1);

        // Field index should work
        assert!(!index.schemas_with_field("id").is_empty());
        assert!(!index.schemas_with_field("email").is_empty());
    }

    #[test]
    fn test_discover_pipelines_excludes_schemas_and_templates() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();

        // Create various directories and files
        fs::create_dir(root.join("schemas")).unwrap();
        fs::create_dir(root.join("templates")).unwrap();

        write_file(root, "pipeline.yaml", "pipeline: {name: test}");
        write_file(root, "another.yml", "pipeline: {name: test2}");
        write_file(
            &root.join("schemas"),
            "test.schema.yaml",
            "_schema: {name: x, format: csv}",
        );
        write_file(root, "readme.txt", "not yaml");

        let pipelines = discover_pipelines(root, "schemas", &[], &[]);

        let names: Vec<_> = pipelines
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();
        assert!(names.contains(&"pipeline.yaml"));
        assert!(names.contains(&"another.yml"));
        assert!(!names.iter().any(|n| n.contains("schema")));
    }
}
