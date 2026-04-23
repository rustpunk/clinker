//! Workspace-wide search engine.
//!
//! Two modes: text search (substring/regex across YAML files) and structural
//! search (query pipeline topology using a DSL). Results link directly to
//! the relevant stage in the relevant pipeline.
//!
//! Spec: clinker-kiln-search-schemas-templates-addendum.md §S2.

use std::fs;
use std::path::{Path, PathBuf};

use regex::Regex;
use serde::{Deserialize, Serialize};

/// Which search mode is active.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SearchMode {
    #[default]
    Text,
    Structural,
}

/// Text search options.
#[derive(Clone, Debug, Default)]
pub struct TextSearchOptions {
    /// Use regex instead of substring match.
    pub regex: bool,
    /// Case-sensitive matching.
    pub case_sensitive: bool,
    /// Match whole words only.
    pub whole_word: bool,
}

/// A single text search match within a file.
#[derive(Clone, Debug, PartialEq)]
pub struct TextSearchMatch {
    /// 1-based line number.
    pub line: usize,
    /// The full line content.
    pub content: String,
    /// Byte offset of match start within the line.
    pub match_start: usize,
    /// Byte offset of match end within the line.
    pub match_end: usize,
}

/// Text search results grouped by file.
#[derive(Clone, Debug, PartialEq)]
pub struct TextSearchFileResult {
    /// Path to the matched file (relative to workspace root).
    pub path: String,
    /// Absolute path for file operations.
    pub abs_path: PathBuf,
    /// Individual line matches within this file.
    pub matches: Vec<TextSearchMatch>,
}

/// A structural search query tag (e.g., `input:employees` or `field:email`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StructuralTag {
    pub key: String,
    pub value: String,
}

/// A structural search match — a stage in a pipeline.
#[derive(Clone, Debug, PartialEq)]
pub struct StructuralSearchMatch {
    /// Pipeline file path (relative to workspace).
    pub pipeline_path: String,
    /// Stage name that matched.
    pub stage_name: String,
    /// Stage type (input, transformation, output).
    pub stage_type: String,
    /// Which part of the config matched (for display).
    pub matched_detail: String,
}

/// A saved or recent search query.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchHistoryEntry {
    /// The query string or serialized tags.
    pub query: String,
    /// Which mode was used.
    pub mode: String,
    /// ISO timestamp of when the search was performed.
    pub timestamp: String,
    /// Optional user-assigned label (for saved queries).
    #[serde(default)]
    pub label: Option<String>,
}

// ── Text search engine ──────────────────────────────────────────────────

/// Perform a text search across all YAML files in the workspace.
///
/// Returns results grouped by file, each with line-level matches.
/// Empty query returns no results.
pub fn text_search(
    workspace_root: &Path,
    query: &str,
    options: &TextSearchOptions,
) -> Vec<TextSearchFileResult> {
    if query.is_empty() {
        return Vec::new();
    }

    let yaml_files = discover_yaml_files(workspace_root);
    let matcher = build_matcher(query, options);

    let Some(matcher) = matcher else {
        return Vec::new(); // Invalid regex
    };

    let mut results = Vec::new();

    for file_path in yaml_files {
        let Ok(content) = fs::read_to_string(&file_path) else {
            continue;
        };

        let relative = file_path
            .strip_prefix(workspace_root)
            .unwrap_or(&file_path)
            .display()
            .to_string();

        let matches = search_content(&content, &matcher);

        if !matches.is_empty() {
            results.push(TextSearchFileResult {
                path: relative,
                abs_path: file_path,
                matches,
            });
        }
    }

    results
}

// ── Internal helpers ────────────────────────────────────────────────────

/// Compiled matcher — either regex or literal string.
enum Matcher {
    Regex(Regex),
    Literal {
        pattern: String,
        case_sensitive: bool,
    },
}

/// Build a matcher from query + options.
fn build_matcher(query: &str, options: &TextSearchOptions) -> Option<Matcher> {
    if options.regex {
        let pattern = if options.whole_word {
            format!(r"\b{query}\b")
        } else {
            query.to_string()
        };

        let re = if options.case_sensitive {
            Regex::new(&pattern).ok()?
        } else {
            Regex::new(&format!("(?i){pattern}")).ok()?
        };

        Some(Matcher::Regex(re))
    } else if options.whole_word {
        // Use regex for whole-word matching even in literal mode
        let escaped = regex::escape(query);
        let pattern = format!(r"\b{escaped}\b");
        let re = if options.case_sensitive {
            Regex::new(&pattern).ok()?
        } else {
            Regex::new(&format!("(?i){pattern}")).ok()?
        };
        Some(Matcher::Regex(re))
    } else {
        Some(Matcher::Literal {
            pattern: query.to_string(),
            case_sensitive: options.case_sensitive,
        })
    }
}

/// Search file content with a compiled matcher, returning line-level matches.
fn search_content(content: &str, matcher: &Matcher) -> Vec<TextSearchMatch> {
    let mut matches = Vec::new();

    for (line_idx, line) in content.lines().enumerate() {
        let line_matches: Vec<(usize, usize)> = match matcher {
            Matcher::Regex(re) => re.find_iter(line).map(|m| (m.start(), m.end())).collect(),
            Matcher::Literal {
                pattern,
                case_sensitive,
            } => {
                if *case_sensitive {
                    find_all_literal(line, pattern)
                } else {
                    find_all_literal_ci(line, pattern)
                }
            }
        };

        for (start, end) in line_matches {
            matches.push(TextSearchMatch {
                line: line_idx + 1,
                content: line.to_string(),
                match_start: start,
                match_end: end,
            });
        }
    }

    matches
}

/// Find all occurrences of a literal pattern (case-sensitive).
fn find_all_literal(haystack: &str, needle: &str) -> Vec<(usize, usize)> {
    let mut results = Vec::new();
    let mut start = 0;
    while let Some(pos) = haystack[start..].find(needle) {
        let abs_pos = start + pos;
        results.push((abs_pos, abs_pos + needle.len()));
        start = abs_pos + needle.len();
    }
    results
}

/// Find all occurrences of a literal pattern (case-insensitive).
fn find_all_literal_ci(haystack: &str, needle: &str) -> Vec<(usize, usize)> {
    let lower_haystack = haystack.to_lowercase();
    let lower_needle = needle.to_lowercase();
    let mut results = Vec::new();
    let mut start = 0;
    while let Some(pos) = lower_haystack[start..].find(&lower_needle) {
        let abs_pos = start + pos;
        results.push((abs_pos, abs_pos + needle.len()));
        start = abs_pos + needle.len();
    }
    results
}

/// Discover all YAML files in the workspace root (non-recursive for now).
fn discover_yaml_files(root: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(root) else {
        return Vec::new();
    };

    let mut files: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            let path = e.path();
            path.is_file()
                && path
                    .extension()
                    .is_some_and(|ext| ext == "yaml" || ext == "yml")
        })
        .map(|e| e.path())
        .collect();

    // Also scan common subdirectories
    for subdir in &["pipelines", "schemas", "templates", "compositions"] {
        let sub = root.join(subdir);
        if sub.is_dir()
            && let Ok(entries) = fs::read_dir(&sub)
        {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path();
                if path.is_file()
                    && path
                        .extension()
                        .is_some_and(|ext| ext == "yaml" || ext == "yml")
                {
                    files.push(path);
                }
            }
        }
    }

    files.sort();
    files
}

// ── Structural search engine ────────────────────────────────────────────

/// Valid structural search keys.
pub const STRUCTURAL_KEYS: &[&str] = &[
    "input",
    "transform",
    "output",
    "field",
    "schema",
    "expr",
    "has",
    "pipeline",
    "import",
    "composition",
    "override",
];

/// Parse a raw DSL string into structural tags.
///
/// Input: "input:employees field:email" → [("input","employees"), ("field","email")]
/// Handles both space-separated and typed pill format.
pub fn parse_structural_query(query: &str) -> Vec<StructuralTag> {
    let mut tags = Vec::new();

    for token in query.split_whitespace() {
        if let Some((key, value)) = token.split_once(':')
            && !key.is_empty()
            && !value.is_empty()
        {
            tags.push(StructuralTag {
                key: key.to_string(),
                value: value.to_string(),
            });
        }
    }

    tags
}

/// Execute a structural search across workspace pipeline files.
///
/// All tags combine with AND — a stage must match every tag to appear in results.
pub fn structural_search(
    workspace_root: &Path,
    tags: &[StructuralTag],
) -> Vec<StructuralSearchMatch> {
    if tags.is_empty() {
        return Vec::new();
    }

    let yaml_files = discover_yaml_files(workspace_root);
    let mut results = Vec::new();

    for file_path in yaml_files {
        let Ok(content) = fs::read_to_string(&file_path) else {
            continue;
        };

        let relative = file_path
            .strip_prefix(workspace_root)
            .unwrap_or(&file_path)
            .display()
            .to_string();

        // Parse as pipeline config for structural matching
        let Ok(config) = clinker_core::config::parse_config(&content) else {
            continue;
        };

        // Check pipeline-level tags
        for tag in tags {
            if tag.key == "pipeline" {
                let name_lower = config.pipeline.name.to_lowercase();
                if name_lower.contains(&tag.value.to_lowercase()) {
                    results.push(StructuralSearchMatch {
                        pipeline_path: relative.clone(),
                        stage_name: config.pipeline.name.clone(),
                        stage_type: "pipeline".to_string(),
                        matched_detail: format!("name: {}", config.pipeline.name),
                    });
                }
            }
        }

        // Check inputs
        for input in config.source_configs() {
            if stage_matches_tags(tags, "input", &input.name, &content_for_input(input)) {
                let detail = format!("type: {}, path: {}", input.format.format_name(), input.path);
                results.push(StructuralSearchMatch {
                    pipeline_path: relative.clone(),
                    stage_name: input.name.clone(),
                    stage_type: "input".to_string(),
                    matched_detail: detail,
                });
            }
        }

        // Check transformations
        for transform in config.transform_views() {
            if stage_matches_tags(tags, "transform", transform.name, transform.cxl_source()) {
                let detail = transform
                    .description
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| {
                        transform
                            .cxl_source()
                            .lines()
                            .next()
                            .unwrap_or("")
                            .to_string()
                    });
                results.push(StructuralSearchMatch {
                    pipeline_path: relative.clone(),
                    stage_name: transform.name.to_string(),
                    stage_type: "transform".to_string(),
                    matched_detail: detail,
                });
            }
        }

        // Check outputs
        for output in config.output_configs() {
            if stage_matches_tags(
                tags,
                "output",
                &output.name,
                &format!("{} {}", output.format.format_name(), output.path),
            ) {
                let detail = format!(
                    "type: {}, path: {}",
                    output.format.format_name(),
                    output.path
                );
                results.push(StructuralSearchMatch {
                    pipeline_path: relative.clone(),
                    stage_name: output.name.clone(),
                    stage_type: "output".to_string(),
                    matched_detail: detail,
                });
            }
        }
    }

    results
}

/// Check if a stage matches all structural tags (AND logic).
fn stage_matches_tags(tags: &[StructuralTag], stage_type: &str, name: &str, content: &str) -> bool {
    let content_lower = content.to_lowercase();
    let name_lower = name.to_lowercase();

    tags.iter().all(|tag| {
        let val_lower = tag.value.to_lowercase();
        match tag.key.as_str() {
            "input" => stage_type == "input" && name_lower.contains(&val_lower),
            "transform" => stage_type == "transform" && name_lower.contains(&val_lower),
            "output" => stage_type == "output" && name_lower.contains(&val_lower),
            "field" | "column" => content_lower.contains(&val_lower),
            "expr" => content_lower.contains(&val_lower),
            "schema" => content_lower.contains(&val_lower),
            "has" => match val_lower.as_str() {
                "_notes" => content_lower.contains("_notes"),
                "description" => content_lower.contains("description"),
                _ => false,
            },
            "pipeline" => false, // Handled separately
            _ => false,
        }
    })
}

/// Build searchable content string for an input stage.
fn content_for_input(input: &clinker_core::config::SourceConfig) -> String {
    let mut content = format!(
        "{} {} {}",
        input.name,
        input.format.format_name(),
        input.path
    );
    if let Some(ref schema) = input.schema {
        content.push_str(&format!(" schema:{schema:?}"));
    }
    content
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_case_insensitive() {
        let content = "Hello World\nhello again\nHELLO ALL";
        let opts = TextSearchOptions {
            regex: false,
            case_sensitive: false,
            whole_word: false,
        };
        let matcher = build_matcher("hello", &opts).unwrap();
        let matches = search_content(content, &matcher);
        assert_eq!(matches.len(), 3);
        assert_eq!(matches[0].line, 1);
        assert_eq!(matches[1].line, 2);
        assert_eq!(matches[2].line, 3);
    }

    #[test]
    fn test_literal_case_sensitive() {
        let content = "Hello World\nhello again\nHELLO ALL";
        let opts = TextSearchOptions {
            regex: false,
            case_sensitive: true,
            whole_word: false,
        };
        let matcher = build_matcher("hello", &opts).unwrap();
        let matches = search_content(content, &matcher);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].line, 2);
    }

    #[test]
    fn test_regex_search() {
        let content = "email: test@example.com\nname: John\nemail: other@test.org";
        let opts = TextSearchOptions {
            regex: true,
            case_sensitive: false,
            whole_word: false,
        };
        let matcher = build_matcher(r"email:\s+\S+@\S+", &opts).unwrap();
        let matches = search_content(content, &matcher);
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn test_whole_word() {
        let content = "customer_id: 123\ncustomer: John\nid: 456";
        let opts = TextSearchOptions {
            regex: false,
            case_sensitive: false,
            whole_word: true,
        };
        let matcher = build_matcher("id", &opts).unwrap();
        let matches = search_content(content, &matcher);
        // "id" as whole word only matches line 3 (not "customer_id")
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].line, 3);
    }

    #[test]
    fn test_multiple_matches_per_line() {
        let content = "a b a b a";
        let opts = TextSearchOptions::default();
        let matcher = build_matcher("a", &opts).unwrap();
        let matches = search_content(content, &matcher);
        assert_eq!(matches.len(), 3);
        assert_eq!(matches[0].match_start, 0);
        assert_eq!(matches[1].match_start, 4);
        assert_eq!(matches[2].match_start, 8);
    }

    #[test]
    fn test_parse_structural_query() {
        let tags = parse_structural_query("input:employees field:email");
        assert_eq!(tags.len(), 2);
        assert_eq!(
            tags[0],
            StructuralTag {
                key: "input".to_string(),
                value: "employees".to_string()
            }
        );
        assert_eq!(
            tags[1],
            StructuralTag {
                key: "field".to_string(),
                value: "email".to_string()
            }
        );
    }

    #[test]
    fn test_parse_structural_query_empty() {
        assert!(parse_structural_query("").is_empty());
        assert!(parse_structural_query("no-colon").is_empty());
    }

    #[test]
    fn test_stage_matches_tags_and_logic() {
        let tags = vec![
            StructuralTag {
                key: "transform".to_string(),
                value: "compute".to_string(),
            },
            StructuralTag {
                key: "expr".to_string(),
                value: "email".to_string(),
            },
        ];

        // Both match
        assert!(stage_matches_tags(
            &tags,
            "transform",
            "compute_fields",
            "emit domain = email.split"
        ));

        // Name doesn't match
        assert!(!stage_matches_tags(
            &tags,
            "transform",
            "filter_step",
            "emit domain = email.split"
        ));

        // Content doesn't match
        assert!(!stage_matches_tags(
            &tags,
            "transform",
            "compute_fields",
            "emit x = y + z"
        ));
    }
}
