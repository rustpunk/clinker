//! Schema and pipeline discovery from workspace directories.
//!
//! Scans the workspace for `.schema.yaml` files and pipeline YAML files,
//! parses schemas, resolves `schema:` references in pipelines to populate
//! `referencing_pipelines`, and builds a `SchemaIndex`.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::model::{SchemaIndex, SourceSchema};
use crate::parse::{SchemaParseError, analyze_schema_file, parse_schema};
use crate::report::{
    CoverageFacet, CoverageStatus, ReportLocation, ReportSubject, SchemaCoverageReport,
    SchemaReference,
};

/// Default schema directory name relative to workspace root.
pub const DEFAULT_SCHEMA_DIR: &str = "schemas";

/// Complete bounded advisory result for one workspace scan.
#[derive(Debug)]
pub struct WorkspaceSchemaAnalysis {
    pub index: SchemaIndex,
    pub reports: Vec<SchemaCoverageReport>,
    pub parse_errors: Vec<(PathBuf, SchemaParseError)>,
}

/// Returns whether the path's extension is a YAML extension, ignoring case.
///
/// Case-folds the extension so that case-preserving filesystems (macOS APFS,
/// Windows NTFS) surface files stored as `*.YAML`/`*.YML` identically to the
/// lowercase forms a case-sensitive Linux filesystem would carry.
fn has_yaml_extension(path: &Path) -> bool {
    path.extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("yaml") || ext.eq_ignore_ascii_case("yml"))
}

/// Returns whether the path's file stem ends in the `.schema` marker, ignoring case.
///
/// The stem of `customers.schema.YAML` is `customers.schema`; case-folding the
/// `.schema` suffix lets a schema authored as `customers.SCHEMA.yaml` on a
/// case-preserving filesystem still be recognized as a schema file.
fn has_schema_stem(path: &Path) -> bool {
    path.file_stem()
        .and_then(|s| s.to_str())
        .is_some_and(|stem| {
            stem.len() >= ".schema".len()
                && stem[stem.len() - ".schema".len()..].eq_ignore_ascii_case(".schema")
        })
}

/// Discover all `.schema.yaml` files in the given directory (non-recursive).
///
/// Extension and `.schema` stem matching is case-insensitive, so a schema
/// stored as `customers.schema.YAML` on a case-preserving filesystem is found
/// alongside the lowercase form. Returns parsed schemas with their `path`
/// fields set. The caller is responsible for populating
/// `referencing_pipelines` via [`resolve_schema_references`].
pub fn discover_schemas(
    schema_dir: &Path,
) -> Vec<Result<SourceSchema, (PathBuf, SchemaParseError)>> {
    let Ok(entries) = fs::read_dir(schema_dir) else {
        return Vec::new();
    };

    entries
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            let path = entry.path();
            has_yaml_extension(&path) && has_schema_stem(&path)
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
/// Extension matching and the `.schema` exclusion are case-insensitive, so a
/// pipeline saved as `flow.YAML` is discovered and a `*.schema.YAML` file is
/// still excluded on case-preserving filesystems. Excludes the schema directory
/// and common non-pipeline directories.
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

    let mut paths: Vec<_> = entries
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            let path = entry.path();
            // Must be a file
            if !path.is_file() {
                return false;
            }
            // Must be YAML
            if !has_yaml_extension(&path) {
                return false;
            }
            // Exclude schema files
            if has_schema_stem(&path) {
                return false;
            }
            // Exclude if inside schema or template dirs
            if path.starts_with(&schema_path) || path.starts_with(&templates_path) {
                return false;
            }
            true
        })
        .map(|entry| entry.path())
        .collect();
    paths.sort();
    paths
}

/// Discover pipelines using include/exclude glob patterns.
fn discover_pipelines_with_globs(
    workspace_root: &Path,
    include_globs: &[String],
    exclude_globs: &[String],
) -> Vec<PathBuf> {
    // Simple glob expansion — match files in workspace.
    let excluded: HashSet<PathBuf> = exclude_globs
        .iter()
        .flat_map(|pattern| {
            let full_pattern = workspace_root.join(pattern).display().to_string();
            glob_paths(&full_pattern).unwrap_or_default()
        })
        .collect();

    let mut results = Vec::new();
    for pattern in include_globs {
        let full_pattern = workspace_root.join(pattern).display().to_string();
        if let Ok(paths) = glob_paths(&full_pattern) {
            results.extend(paths.into_iter().filter(|p| !excluded.contains(p)));
        }
    }
    results.sort();
    results.dedup();
    results
}

/// Simple glob expansion using std::fs.
fn glob_paths(pattern: &str) -> Result<Vec<PathBuf>, ()> {
    // For now, support simple directory/name patterns with `*` and `?` in the
    // filename component. Full recursive glob support can be added with the
    // `glob` crate later if this crate takes that dependency deliberately.
    let path = Path::new(pattern);
    if let Some(parent) = path.parent()
        && parent.is_dir()
    {
        let Some(file_pattern) = path.file_name().and_then(|n| n.to_str()) else {
            return Ok(Vec::new());
        };
        let Ok(entries) = fs::read_dir(parent) else {
            return Ok(Vec::new());
        };
        return Ok(entries
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| wildcard_match_ascii_case_insensitive(file_pattern, name))
            })
            .map(|e| e.path())
            .collect());
    }
    Ok(Vec::new())
}

fn wildcard_match_ascii_case_insensitive(pattern: &str, candidate: &str) -> bool {
    let pattern = pattern.as_bytes();
    let candidate = candidate.as_bytes();
    let mut p = 0;
    let mut c = 0;
    let mut star = None;
    let mut match_after_star = 0;

    while c < candidate.len() {
        if p < pattern.len()
            && (pattern[p] == b'?' || pattern[p].eq_ignore_ascii_case(&candidate[c]))
        {
            p += 1;
            c += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star = Some(p);
            p += 1;
            match_after_star = c;
        } else if let Some(star_idx) = star {
            p = star_idx + 1;
            match_after_star += 1;
            c = match_after_star;
        } else {
            return false;
        }
    }

    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }

    p == pattern.len()
}

/// Extract external schema references through the canonical typed pipeline parser.
///
/// Malformed or retired pipeline syntax produces no references here; callers
/// that need the reason use [`analyze_pipeline_file`].
pub fn extract_schema_refs(pipeline_path: &Path) -> Vec<String> {
    let workspace_root = pipeline_path.parent().unwrap_or_else(|| Path::new("."));
    analyze_pipeline_file(pipeline_path, workspace_root)
        .references
        .into_iter()
        .filter_map(|reference| reference.schema_path.into_os_string().into_string().ok())
        .collect()
}

/// Structured in-memory reference extraction used by focused parser tests.
#[cfg(test)]
fn extract_schema_refs_from_str(yaml: &str) -> Vec<String> {
    let Ok(config) = clinker_plan::config::parse_config(yaml) else {
        return Vec::new();
    };
    let mut references: Vec<String> = config
        .source_bodies()
        .filter_map(|body| match &body.schema {
            clinker_plan::config::SourceSchema::File(path) => Some(path.clone()),
            _ => None,
        })
        .collect();
    references.sort();
    references.dedup();
    references
}

/// Analyze a pipeline's external schema references without compiling it.
pub fn analyze_pipeline_file(pipeline_path: &Path, workspace_root: &Path) -> SchemaCoverageReport {
    let mut report = SchemaCoverageReport::new(ReportSubject::Pipeline, pipeline_path);
    let yaml = match fs::read_to_string(pipeline_path) {
        Ok(yaml) => yaml,
        Err(error) => {
            report.status = CoverageStatus::Failed;
            report.reason(
                "read-failed",
                None,
                format!("pipeline read failed: {error}"),
            );
            report.sort_stably();
            return report;
        }
    };

    if yaml.trim().is_empty() {
        report.status = CoverageStatus::Skipped;
        report.reason("empty-document", None, "pipeline document is empty");
        report.sort_stably();
        return report;
    }

    let config = match clinker_plan::config::parse_config(&yaml) {
        Ok(config) => config,
        Err(error) => {
            report.status = CoverageStatus::Failed;
            report.reason(
                "pipeline-parse-failed",
                None,
                format!("pipeline structure could not be inspected: {error}"),
            );
            report.sort_stably();
            return report;
        }
    };

    for body in config.source_bodies() {
        if let clinker_plan::config::SourceSchema::File(path) = &body.schema {
            let schema_path = PathBuf::from(path);
            report.reference(SchemaReference {
                schema_path: schema_path.clone(),
                location: ReportLocation::file(pipeline_path),
            });
            if !workspace_root.join(&schema_path).is_file() {
                report.status = CoverageStatus::Failed;
                report.reason(
                    "unresolved-reference",
                    Some(CoverageFacet::PipelineReferences),
                    format!("schema reference '{}' does not resolve to a file", path),
                );
            }
        }
    }

    if report.references.is_empty() {
        report.status = CoverageStatus::Skipped;
        report.reason(
            "no-external-schema",
            Some(CoverageFacet::PipelineReferences),
            "pipeline declares no external schema references",
        );
    } else if report.status == CoverageStatus::Analyzed {
        report.support(CoverageFacet::PipelineReferences);
    }
    report.sort_stably();
    report
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
    let analysis =
        analyze_workspace_schemas(workspace_root, schema_dir, include_globs, exclude_globs);
    (analysis.index, analysis.parse_errors)
}

/// Discover schemas and pipelines through typed parsing and retain explicit
/// advisory coverage for every inspected artifact.
pub fn analyze_workspace_schemas(
    workspace_root: &Path,
    schema_dir: &str,
    include_globs: &[String],
    exclude_globs: &[String],
) -> WorkspaceSchemaAnalysis {
    let schema_path = workspace_root.join(schema_dir);

    // 1. Discover and analyze schemas in deterministic path order.
    let mut schema_files = Vec::new();
    if let Ok(entries) = fs::read_dir(&schema_path) {
        schema_files.extend(entries.filter_map(|entry| entry.ok()).filter_map(|entry| {
            let path = entry.path();
            (has_yaml_extension(&path) && has_schema_stem(&path)).then_some(path)
        }));
    }
    schema_files.sort();

    let mut schemas = Vec::new();
    let mut errors = Vec::new();
    let mut reports = Vec::new();
    for path in schema_files {
        let analysis = analyze_schema_file(&path);
        if let Some(schema) = analysis.schema {
            schemas.push(schema);
        }
        if let Some(error) = analysis.parse_error {
            errors.push((path, error));
        }
        reports.push(analysis.report);
    }

    // 2. Discover pipeline files
    let pipelines = discover_pipelines(workspace_root, schema_dir, include_globs, exclude_globs);

    // 3. Extract schema references from pipelines through the typed parser.
    let mut pipeline_refs: HashMap<PathBuf, Vec<String>> = HashMap::new();
    for pipeline_path in &pipelines {
        let report = analyze_pipeline_file(pipeline_path, workspace_root);
        let refs: Vec<String> = report
            .references
            .iter()
            .filter_map(|reference| reference.schema_path.to_str().map(str::to_owned))
            .collect();
        if !refs.is_empty() {
            pipeline_refs.insert(pipeline_path.clone(), refs);
        }
        reports.push(report);
    }

    // 4. Resolve references
    resolve_schema_references(&mut schemas, workspace_root, &pipeline_refs);

    // 5. Build index
    reports.sort_by(|left, right| left.location.path.cmp(&right.location.path));

    WorkspaceSchemaAnalysis {
        index: SchemaIndex::build(schemas),
        reports,
        parse_errors: errors,
    }
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

nodes:
  - type: source
    name: source
    config:
      name: source
      type: csv
      path: ./data/customers.csv
      schema: schemas/customers.schema.yaml
"#;
        let refs = extract_schema_refs_from_str(yaml);
        assert_eq!(refs, vec!["schemas/customers.schema.yaml"]);
    }

    #[test]
    fn test_extract_schema_refs_ignores_inline_schemas() {
        let yaml = r#"
pipeline:
  name: test
nodes:
  - type: source
    name: external
    config:
      name: external
      type: csv
      path: ./data/input.csv
      schema: schemas/main.schema.yaml
  - type: source
    name: inline
    config:
      name: inline
      type: csv
      path: ./data/inline.csv
      schema:
        - { name: id, type: int }
"#;
        let refs = extract_schema_refs_from_str(yaml);
        // Only the external file form is a discovery reference.
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

nodes:
  - type: source
    name: source
    config:
      name: source
      type: csv
      path: ./data/customers.csv
      schema: schemas/customers.schema.yaml
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

    #[test]
    fn test_discover_schemas_mixed_case_extension_and_stem() {
        let tmp = TempDir::new().unwrap();
        let schemas_dir = tmp.path().join("schemas");
        fs::create_dir(&schemas_dir).unwrap();

        // Uppercase extension — the case a macOS APFS / Windows NTFS file
        // preserves verbatim and a case-sensitive Linux scan would miss.
        write_file(
            &schemas_dir,
            "customers.schema.YAML",
            r#"
_schema:
  name: customers
  format: csv
fields:
  - name: id
    type: int
    nullable: false
"#,
        );

        // Uppercase short extension.
        write_file(
            &schemas_dir,
            "orders.schema.YML",
            r#"
_schema:
  name: orders
  format: csv
fields:
  - name: order_id
    type: string
    nullable: false
"#,
        );

        // Uppercase `.SCHEMA` stem marker.
        write_file(
            &schemas_dir,
            "events.SCHEMA.yaml",
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

        let results = discover_schemas(&schemas_dir);
        let schemas: Vec<_> = results.into_iter().filter_map(|r| r.ok()).collect();

        assert_eq!(schemas.len(), 3);
        let names: Vec<_> = schemas.iter().map(|s| s.metadata.name.as_str()).collect();
        assert!(names.contains(&"customers"));
        assert!(names.contains(&"orders"));
        assert!(names.contains(&"events"));
    }

    #[test]
    fn test_discover_pipelines_mixed_case_extension() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();

        fs::create_dir(root.join("schemas")).unwrap();

        // Mixed-case pipeline extensions must be discovered.
        write_file(root, "flow.YAML", "pipeline: {name: test}");
        write_file(root, "other.YML", "pipeline: {name: test2}");
        // A mixed-case schema file at the root must still be excluded.
        write_file(
            root,
            "inline.schema.YAML",
            "_schema: {name: x, format: csv}",
        );

        let pipelines = discover_pipelines(root, "schemas", &[], &[]);

        let names: Vec<_> = pipelines
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();
        assert!(names.contains(&"flow.YAML"));
        assert!(names.contains(&"other.YML"));
        assert!(
            !names
                .iter()
                .any(|n| n.to_ascii_lowercase().contains(".schema."))
        );
    }

    #[test]
    fn test_glob_discovery_mixed_case_extension() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        fs::create_dir(root.join("schemas")).unwrap();

        // The glob-driven path (manifest `include` globs) must match a
        // case-preserving filesystem's on-disk casing: `*.yaml` finds
        // `flow.YAML`.
        write_file(root, "flow.YAML", "pipeline: {name: test}");

        let pipelines = discover_pipelines(root, "schemas", &["*.yaml".to_string()], &[]);
        let names: Vec<_> = pipelines
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();
        assert!(
            names.contains(&"flow.YAML"),
            "glob discovery missed mixed-case extension: {names:?}"
        );
    }

    #[test]
    fn test_glob_discovery_applies_exclude_globs() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        fs::create_dir(root.join("schemas")).unwrap();

        write_file(root, "keep.yaml", "pipeline: {name: keep}");
        write_file(root, "skip.yaml", "pipeline: {name: skip}");
        write_file(root, "skip-extra.yaml", "pipeline: {name: skip_extra}");

        let pipelines = discover_pipelines(
            root,
            "schemas",
            &["*.yaml".to_string()],
            &["skip*.yaml".to_string()],
        );
        let names: Vec<_> = pipelines
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();

        assert_eq!(names, vec!["keep.yaml"]);
    }

    #[test]
    fn test_glob_discovery_exact_pattern_does_not_overmatch_by_extension() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        fs::create_dir(root.join("schemas")).unwrap();

        write_file(root, "pipeline.yaml", "pipeline: {name: keep}");
        write_file(root, "other.yaml", "pipeline: {name: other}");

        let pipelines = discover_pipelines(root, "schemas", &["pipeline.yaml".to_string()], &[]);
        let names: Vec<_> = pipelines
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();

        assert_eq!(names, vec!["pipeline.yaml"]);
    }

    #[test]
    fn test_full_workspace_discovery_mixed_case_extension() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();

        let schemas_dir = root.join("schemas");
        fs::create_dir(&schemas_dir).unwrap();

        // Schema stored with an uppercase extension, as a case-preserving
        // filesystem would surface it.
        write_file(
            &schemas_dir,
            "customers.schema.YAML",
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
            root,
            "pipeline.yaml",
            r#"
pipeline:
  name: test

nodes:
  - type: source
    name: source
    config:
      name: source
      type: csv
      path: ./data/customers.csv
      schema: schemas/customers.schema.YAML
"#,
        );

        let (index, errors) = build_workspace_schema_index(root, "schemas", &[], &[]);

        assert!(errors.is_empty());
        assert_eq!(index.len(), 1);

        // The mixed-case schema file is discovered, parsed, and bound to its
        // referencing pipeline.
        let schema_path = schemas_dir.join("customers.schema.YAML");
        let schema = index.get(&schema_path).unwrap();
        assert_eq!(schema.metadata.name, "customers");
        assert_eq!(schema.referencing_pipelines.len(), 1);
    }
}
