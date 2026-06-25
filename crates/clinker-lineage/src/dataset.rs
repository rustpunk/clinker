//! Plan-time mapping from Clinker plan nodes to OpenLineage dataset identities.
//!
//! The first piece of the builder that walks a compiled plan: a pure,
//! deterministic function from each [`PlanNode::Source`] / [`PlanNode::Output`]
//! to the `{namespace, name}` pair that names one logical dataset. Like
//! `--explain`, it reads no data and touches no filesystem — paths are resolved
//! *lexically* against the workspace root, never via [`std::fs::canonicalize`],
//! so the result is stable across machines and works for not-yet-created output
//! paths. Column-lineage facets are layered on later; this module only
//! establishes dataset identity.
//!
//! ## Canonicalization rule
//!
//! A file dataset name has forward-slash separators and no trailing slash, and
//! is absolute whenever `base_dir` is absolute (the CLI guarantees that by
//! canonicalizing the workspace root). A relative matcher is resolved against
//! `base_dir` (the directory containing the pipeline YAML); an already-absolute
//! matcher is used as-is. Resolution is lexical — `.` and `..` segments are
//! preserved verbatim, never collapsed against the filesystem — and the on-disk
//! format is **not** encoded in the name, per the OpenLineage naming convention.

use std::path::Path;

use clinker_plan::config::{OutputConfig, SourceConfig};
use clinker_plan::plan::execution::PlanNode;

use crate::openlineage::Dataset;

/// OpenLineage namespace for datasets backed by the local filesystem.
///
/// Per the OpenLineage naming convention a local file dataset is namespace
/// `file` with the resolved path as its name (absolute when `base_dir` is).
pub const FILE_NAMESPACE: &str = "file";

/// Fallback namespace for datasets with no filesystem identity — network
/// sources (`transport: rest`), or a node whose resolved config is unavailable.
/// The dataset name is then the plan node's own name; richer network-dataset
/// naming is intentionally outside this module's scope.
pub const FALLBACK_NAMESPACE: &str = "clinker";

/// A plan-derived OpenLineage dataset identity: the `{namespace, name}` pair
/// naming one logical input or output dataset.
///
/// Identity only — deliberately distinct from the wire [`Dataset`], which also
/// carries facets. Deriving `Hash`/`Eq` lets a later builder group a run's
/// inputs and outputs by dataset before attaching column-lineage facets.
/// Convert to a facet-less wire [`Dataset`] via `Dataset::from`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatasetId {
    pub namespace: String,
    pub name: String,
}

impl DatasetId {
    /// A filesystem dataset: the [`FILE_NAMESPACE`] plus a resolved path name
    /// (absolute when `base_dir` is absolute).
    fn file(name: String) -> Self {
        Self {
            namespace: FILE_NAMESPACE.to_string(),
            name,
        }
    }

    /// A name-only dataset: the [`FALLBACK_NAMESPACE`] plus the node/source name,
    /// for sources with no filesystem identity.
    fn fallback(name: impl Into<String>) -> Self {
        Self {
            namespace: FALLBACK_NAMESPACE.to_string(),
            name: name.into(),
        }
    }
}

impl From<DatasetId> for Dataset {
    fn from(id: DatasetId) -> Self {
        Dataset {
            namespace: id.namespace,
            name: id.name,
            facets: None,
        }
    }
}

/// The OpenLineage dataset identity of a plan node, or `None` for nodes that are
/// not datasets (every variant but [`Source`](PlanNode::Source) and
/// [`Output`](PlanNode::Output)).
///
/// `base_dir` is the workspace root — the directory containing the pipeline
/// YAML, the same `base_dir` `clinker_plan`'s discovery layer takes. A
/// Source/Output whose resolved payload is unavailable (e.g. after a serde
/// round-trip, since the payload is `#[serde(skip)]`) falls back to the
/// [`FALLBACK_NAMESPACE`] plus the node name rather than inventing a path.
pub fn dataset_identity(node: &PlanNode, base_dir: &Path) -> Option<DatasetId> {
    match node {
        PlanNode::Source { name, resolved, .. } => Some(match resolved {
            Some(payload) => source_dataset_identity(&payload.source, base_dir),
            None => DatasetId::fallback(name.as_str()),
        }),
        PlanNode::Output { name, resolved, .. } => Some(match resolved {
            Some(payload) => output_dataset_identity(&payload.output, base_dir),
            None => DatasetId::fallback(name.as_str()),
        }),
        _ => None,
    }
}

/// Dataset identity of a source node from its parsed [`SourceConfig`].
///
/// - non-file transport (`transport: rest`) → [`FALLBACK_NAMESPACE`] + source name.
/// - literal `path:` → [`FILE_NAMESPACE`] + the absolutized path.
/// - `glob:` / `regex:` / `paths:` → [`FILE_NAMESPACE`] + a single directory
///   dataset (the matcher's base directory). The concrete matched files are
///   unknown at plan time, so a multi-file matcher maps to one logical
///   directory dataset rather than per-file datasets.
pub(crate) fn source_dataset_identity(source: &SourceConfig, base_dir: &Path) -> DatasetId {
    if !source.transport.is_file() {
        return DatasetId::fallback(source.name.as_str());
    }
    let name = if let Some(path) = source.path.as_deref() {
        absolutize(base_dir, path)
    } else if let Some(glob) = source.glob.as_deref() {
        absolutize(base_dir, glob_base(glob))
    } else if let Some(regex) = source.regex.as_deref() {
        absolutize(base_dir, regex_base(regex))
    } else if let Some(paths) = source.paths.as_deref() {
        common_parent(paths, base_dir)
    } else {
        // A file source with no matcher set is rejected at config validation;
        // fall back to the source name rather than inventing a path.
        return DatasetId::fallback(source.name.as_str());
    };
    DatasetId::file(name)
}

/// Dataset identity of an output node from its parsed [`OutputConfig`].
///
/// Always [`FILE_NAMESPACE`] + the absolutized output path. The path may be a
/// template carrying tokens (e.g. `{source_file}`) or driving per-file `split:`
/// fan-out; tokens are **not** resolved at plan time — the literal template
/// form is kept verbatim as the dataset name.
pub(crate) fn output_dataset_identity(output: &OutputConfig, base_dir: &Path) -> DatasetId {
    DatasetId::file(absolutize(base_dir, &output.path))
}

/// Resolve `raw` against `base_dir` (unless already absolute) and normalize to
/// the documented canonical form: forward-slash separators, no trailing slash.
/// Lexical only — it never consults the filesystem, so it is deterministic and
/// valid for paths that do not yet exist.
fn absolutize(base_dir: &Path, raw: &str) -> String {
    let raw_path = Path::new(raw);
    let joined = if raw_path.is_absolute() {
        raw_path.to_path_buf()
    } else {
        base_dir.join(raw_path)
    };
    let normalized = joined.to_string_lossy().replace('\\', "/");
    let trimmed = normalized.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Glob metacharacters that end the literal directory prefix of a `glob:` matcher.
const GLOB_META: &[char] = &['*', '?', '[', ']', '{', '}'];

/// Regex metacharacters that end the literal directory prefix of a `regex:` matcher.
const REGEX_META: &[char] = &[
    '.', '^', '$', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '\\',
];

/// The directory a `glob:` matcher is rooted at: the longest leading run of
/// `/`-separated components containing no glob metacharacter. Returns `""` when
/// the first component is already a wildcard (e.g. `**/*.csv`), denoting
/// `base_dir` itself.
fn glob_base(pattern: &str) -> &str {
    leading_literal_dir(pattern, GLOB_META)
}

/// The directory a `regex:` matcher is rooted at. A regex is matched against
/// full paths under the workspace root, so the base is the literal prefix
/// before the first regex-special component; an anchored or wildcard-leading
/// regex yields `""` (i.e. `base_dir`).
fn regex_base(pattern: &str) -> &str {
    // A regex is matched unanchored against full paths *under* the workspace
    // root, so a leading `/` is a literal separator, not a filesystem anchor.
    // Strip it so the literal prefix resolves under `base_dir` rather than
    // escaping to the filesystem root (unlike a glob, which discovery honors
    // as absolute).
    let pattern = pattern.strip_prefix('/').unwrap_or(pattern);
    leading_literal_dir(pattern, REGEX_META)
}

/// The longest leading run of `/`-separated components of `pattern` none of
/// which contains a character in `meta`.
fn leading_literal_dir<'a>(pattern: &'a str, meta: &[char]) -> &'a str {
    let mut end = 0;
    let mut cursor = 0;
    for component in pattern.split('/') {
        if component.chars().any(|c| meta.contains(&c)) {
            break;
        }
        end = cursor + component.len();
        cursor = end + 1; // step past the '/' separator
    }
    &pattern[..end]
}

/// The common parent directory of an explicit `paths:` list, as one directory
/// dataset: each path is absolutized, then the longest shared leading run of
/// components across their parent directories is taken.
fn common_parent(paths: &[String], base_dir: &Path) -> String {
    if paths.is_empty() {
        return absolutize(base_dir, "");
    }
    let parents: Vec<String> = paths
        .iter()
        .map(|p| parent_dir(&absolutize(base_dir, p)))
        .collect();
    longest_common_dir(&parents)
}

/// The directory containing `path` — everything up to its last `/`. A root-level
/// path (`/file`) yields `/`; a path with no `/` yields `""`.
fn parent_dir(path: &str) -> String {
    match path.rfind('/') {
        Some(0) => "/".to_string(),
        Some(idx) => path[..idx].to_string(),
        None => String::new(),
    }
}

/// The longest common leading run of `/`-separated components shared by every
/// entry in `dirs`. An empty shared prefix collapses to the filesystem root `/`.
fn longest_common_dir(dirs: &[String]) -> String {
    let Some(first) = dirs.first() else {
        return String::new();
    };
    let first: Vec<&str> = first.split('/').collect();
    let mut shared = first.len();
    for dir in &dirs[1..] {
        let components: Vec<&str> = dir.split('/').collect();
        let mut i = 0;
        while i < shared && i < components.len() && components[i] == first[i] {
            i += 1;
        }
        shared = i;
    }
    let joined = first[..shared].join("/");
    if joined.is_empty() {
        "/".to_string()
    } else {
        joined
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use clinker_core_types::span::Span;
    use clinker_plan::plan::execution::{PlanOutputPayload, PlanSourcePayload};
    use clinker_plan::plan::{EntityRef, PlanNodeId};
    use clinker_record::Schema;
    use serde_json::json;

    fn base() -> &'static Path {
        Path::new("/work")
    }

    fn source_config(value: serde_json::Value) -> SourceConfig {
        serde_json::from_value(value).expect("valid source config")
    }

    fn output_config(value: serde_json::Value) -> OutputConfig {
        serde_json::from_value(value).expect("valid output config")
    }

    fn source_node(name: &str, resolved: Option<PlanSourcePayload>) -> PlanNode {
        PlanNode::Source {
            name: name.to_string(),
            id: PlanNodeId::new(0),
            span: Span::SYNTHETIC,
            resolved: resolved.map(Box::new),
            output_schema: Arc::new(Schema::new(vec![])),
        }
    }

    fn output_node(name: &str, resolved: Option<PlanOutputPayload>) -> PlanNode {
        PlanNode::Output {
            name: name.to_string(),
            id: PlanNodeId::new(0),
            span: Span::SYNTHETIC,
            resolved: resolved.map(Box::new),
        }
    }

    fn merge_node(name: &str) -> PlanNode {
        PlanNode::Merge {
            name: name.to_string(),
            id: PlanNodeId::new(0),
            span: Span::SYNTHETIC,
            output_schema: Arc::new(Schema::new(vec![])),
        }
    }

    // --- source config → identity ---

    #[test]
    fn literal_path_is_absolutized_file_dataset() {
        let cfg = source_config(json!({"name": "in", "path": "data/in.csv", "type": "csv"}));
        assert_eq!(
            source_dataset_identity(&cfg, base()),
            DatasetId::file("/work/data/in.csv".to_string())
        );
    }

    #[test]
    fn glob_maps_to_leading_literal_directory() {
        let cfg = source_config(json!({"name": "in", "glob": "data/2024/*.csv", "type": "csv"}));
        assert_eq!(
            source_dataset_identity(&cfg, base()),
            DatasetId::file("/work/data/2024".to_string())
        );
    }

    #[test]
    fn wildcard_leading_glob_maps_to_base_dir() {
        let cfg = source_config(json!({"name": "in", "glob": "**/*.csv", "type": "csv"}));
        assert_eq!(
            source_dataset_identity(&cfg, base()),
            DatasetId::file("/work".to_string())
        );
    }

    #[test]
    fn metachar_free_glob_names_the_literal_path() {
        // `.` is not a glob metacharacter, so a wildcard-free glob carries its
        // whole pattern through as the (file) path name.
        let cfg = source_config(json!({"name": "in", "glob": "data/in.csv", "type": "csv"}));
        assert_eq!(
            source_dataset_identity(&cfg, base()),
            DatasetId::file("/work/data/in.csv".to_string())
        );
    }

    #[test]
    fn regex_maps_to_literal_prefix_directory() {
        let cfg =
            source_config(json!({"name": "in", "regex": "logs/app-\\d+\\.log", "type": "csv"}));
        assert_eq!(
            source_dataset_identity(&cfg, base()),
            DatasetId::file("/work/logs".to_string())
        );
    }

    #[test]
    fn leading_slash_regex_stays_under_base_dir() {
        // A regex is matched unanchored under base_dir, so a leading `/` is a
        // literal separator — the dataset must not escape to the filesystem root.
        let cfg =
            source_config(json!({"name": "in", "regex": "/abs/logs/.*\\.log", "type": "csv"}));
        assert_eq!(
            source_dataset_identity(&cfg, base()),
            DatasetId::file("/work/abs/logs".to_string())
        );
    }

    #[test]
    fn paths_map_to_common_parent_directory() {
        let cfg = source_config(json!({
            "name": "in",
            "paths": ["data/a.csv", "data/b.csv"],
            "type": "csv",
        }));
        assert_eq!(
            source_dataset_identity(&cfg, base()),
            DatasetId::file("/work/data".to_string())
        );
    }

    #[test]
    fn divergent_paths_climb_to_shared_parent() {
        let cfg = source_config(json!({
            "name": "in",
            "paths": ["east/a.csv", "west/b.csv"],
            "type": "csv",
        }));
        assert_eq!(
            source_dataset_identity(&cfg, base()),
            DatasetId::file("/work".to_string())
        );
    }

    #[test]
    fn single_element_paths_yields_its_parent_directory() {
        // A one-element `paths:` list is still a directory dataset (the file's
        // parent), distinct from the same path as a literal `path:` (the file).
        let cfg = source_config(json!({
            "name": "in",
            "paths": ["data/sub/only.csv"],
            "type": "csv",
        }));
        assert_eq!(
            source_dataset_identity(&cfg, base()),
            DatasetId::file("/work/data/sub".to_string())
        );
    }

    #[test]
    fn rest_source_falls_back_to_name() {
        let cfg = source_config(json!({
            "name": "orders_api",
            "type": "json",
            "transport": {"kind": "rest", "url": "https://api.example.test/v1/orders", "max_pages": 1},
        }));
        assert_eq!(
            source_dataset_identity(&cfg, base()),
            DatasetId::fallback("orders_api")
        );
    }

    // --- output config → identity ---

    #[test]
    fn output_path_is_absolutized_file_dataset() {
        let cfg = output_config(json!({"name": "out", "path": "out/summary.csv", "type": "csv"}));
        assert_eq!(
            output_dataset_identity(&cfg, base()),
            DatasetId::file("/work/out/summary.csv".to_string())
        );
    }

    #[test]
    fn output_template_tokens_are_kept_literal() {
        let cfg =
            output_config(json!({"name": "out", "path": "out/{source_file}.csv", "type": "csv"}));
        assert_eq!(
            output_dataset_identity(&cfg, base()),
            DatasetId::file("/work/out/{source_file}.csv".to_string())
        );
    }

    // --- node-level dispatch ---

    #[test]
    fn source_node_delegates_to_source_identity() {
        let payload = PlanSourcePayload {
            source: source_config(json!({"name": "in", "path": "data/in.csv", "type": "csv"})),
            validated_path: None,
        };
        let node = source_node("in", Some(payload));
        assert_eq!(
            dataset_identity(&node, base()),
            Some(DatasetId::file("/work/data/in.csv".to_string()))
        );
    }

    #[test]
    fn output_node_delegates_to_output_identity() {
        let payload = PlanOutputPayload {
            output: output_config(json!({"name": "out", "path": "out.csv", "type": "csv"})),
            validated_path: None,
            fan_out_per_source_file: false,
        };
        let node = output_node("out", Some(payload));
        assert_eq!(
            dataset_identity(&node, base()),
            Some(DatasetId::file("/work/out.csv".to_string()))
        );
    }

    #[test]
    fn unresolved_source_node_falls_back_to_node_name() {
        let node = source_node("orphan", None);
        assert_eq!(
            dataset_identity(&node, base()),
            Some(DatasetId::fallback("orphan"))
        );
    }

    #[test]
    fn unresolved_output_node_falls_back_to_node_name() {
        let node = output_node("sink", None);
        assert_eq!(
            dataset_identity(&node, base()),
            Some(DatasetId::fallback("sink"))
        );
    }

    #[test]
    fn non_dataset_node_has_no_identity() {
        assert_eq!(dataset_identity(&merge_node("merge"), base()), None);
    }

    // --- path helpers ---

    #[test]
    fn absolutize_normalizes_separators_and_trailing_slash() {
        assert_eq!(absolutize(base(), "a\\b\\c.csv"), "/work/a/b/c.csv");
        assert_eq!(absolutize(base(), "a/b/"), "/work/a/b");
        assert_eq!(absolutize(base(), ""), "/work");
        // Lexical resolution preserves `..` verbatim (never collapsed).
        assert_eq!(
            absolutize(base(), "../shared/a.csv"),
            "/work/../shared/a.csv"
        );
    }

    #[test]
    fn glob_base_takes_leading_literal_components() {
        assert_eq!(glob_base("data/2024/*.csv"), "data/2024");
        assert_eq!(glob_base("**/*.csv"), "");
        assert_eq!(glob_base("logs/*"), "logs");
        // `.` is not a glob metacharacter — a wildcard-free glob is all literal.
        assert_eq!(glob_base("data/in.csv"), "data/in.csv");
    }

    #[test]
    fn regex_base_stops_at_first_regex_metachar() {
        assert_eq!(regex_base("logs/app-\\d+\\.log"), "logs");
        assert_eq!(regex_base("^anchored/.*"), "");
        assert_eq!(regex_base("a/b/c"), "a/b/c");
        // A leading `/` is a literal separator under base_dir, not an anchor.
        assert_eq!(regex_base("/abs/logs/.*"), "abs/logs");
    }

    // --- wire conversion ---

    #[test]
    fn dataset_id_converts_to_facet_less_wire_dataset() {
        let id = DatasetId::file("/work/in.csv".to_string());
        let dataset: Dataset = id.into();
        assert_eq!(dataset.namespace, "file");
        assert_eq!(dataset.name, "/work/in.csv");
        assert!(dataset.facets.is_none());
    }
}
