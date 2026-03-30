//! Phase E: Index planning.
//!
//! Extracts `local_window` configs from `TransformConfig`, combines with
//! `AnalysisReport` field sets, deduplicates into `Vec<IndexSpec>`.

use std::collections::HashSet;

use serde::Deserialize;

use crate::config::{SortField, TransformConfig};

/// Typed representation of the `local_window` YAML block on a transform.
/// Deserialized from `TransformConfig.local_window: Option<serde_json::Value>`.
#[derive(Debug, Clone, Deserialize)]
pub struct LocalWindowConfig {
    /// Reference source name. If None or same as primary input, it's a same-source window.
    pub source: Option<String>,
    /// Fields to group the partition by.
    pub group_by: Vec<String>,
    /// Fields to sort the partition by (within each group).
    #[serde(default)]
    pub sort_by: Vec<SortField>,
    /// Expression to evaluate against the primary record for cross-source lookup.
    pub on: Option<String>,
}

/// Parse `local_window` from the raw JSON value on a TransformConfig.
pub fn parse_local_window(
    transform: &TransformConfig,
) -> Result<Option<LocalWindowConfig>, PlanIndexError> {
    match &transform.local_window {
        None => Ok(None),
        Some(value) => {
            let config: LocalWindowConfig =
                serde_json::from_value(value.clone()).map_err(|e| PlanIndexError::InvalidLocalWindow {
                    transform: transform.name.clone(),
                    message: e.to_string(),
                })?;
            Ok(Some(config))
        }
    }
}

/// Specification for one secondary index to build during Phase 1.
#[derive(Debug, Clone)]
pub struct IndexSpec {
    /// Source input name this index is built from.
    pub source: String,
    /// Fields to group partition keys by.
    pub group_by: Vec<String>,
    /// Fields to sort within each partition.
    pub sort_by: Vec<SortField>,
    /// All fields the Arena must store for this index: union of group_by + sort_by + window-referenced.
    pub arena_fields: Vec<String>,
    /// True if the source config declares a sort_order matching this index's sort_by.
    pub already_sorted: bool,
}

/// Deduplicate index specs: two transforms with the same (source, group_by, sort_by)
/// share one IndexSpec. Arena fields are unioned.
pub fn deduplicate_indices(raw_specs: Vec<RawIndexRequest>) -> Vec<IndexSpec> {
    let mut deduped: Vec<IndexSpec> = Vec::new();

    for req in raw_specs {
        let existing = deduped.iter_mut().find(|spec| {
            spec.source == req.source
                && spec.group_by == req.group_by
                && sort_fields_equal(&spec.sort_by, &req.sort_by)
        });

        match existing {
            Some(spec) => {
                // Merge arena_fields
                for field in &req.arena_fields {
                    if !spec.arena_fields.contains(field) {
                        spec.arena_fields.push(field.clone());
                    }
                }
            }
            None => {
                deduped.push(IndexSpec {
                    source: req.source,
                    group_by: req.group_by,
                    sort_by: req.sort_by,
                    arena_fields: req.arena_fields,
                    already_sorted: req.already_sorted,
                });
            }
        }
    }

    deduped
}

/// Raw index request before deduplication. One per (transform, local_window) pair.
#[derive(Debug, Clone)]
pub struct RawIndexRequest {
    pub source: String,
    pub group_by: Vec<String>,
    pub sort_by: Vec<SortField>,
    pub arena_fields: Vec<String>,
    pub already_sorted: bool,
    /// Index of the transform that requested this index.
    pub transform_index: usize,
}

/// Collect the union of arena_fields across all IndexSpecs for a given source.
pub fn collect_arena_fields(indices: &[IndexSpec], source: &str) -> Vec<String> {
    let mut fields = HashSet::new();
    for spec in indices {
        if spec.source == source {
            for f in &spec.arena_fields {
                fields.insert(f.clone());
            }
        }
    }
    let mut result: Vec<String> = fields.into_iter().collect();
    result.sort(); // deterministic order
    result
}

/// Find the index in `indices` that matches the given (source, group_by, sort_by).
pub fn find_index_for(
    indices: &[IndexSpec],
    source: &str,
    group_by: &[String],
    sort_by: &[SortField],
) -> Option<usize> {
    indices.iter().position(|spec| {
        spec.source == source
            && spec.group_by == *group_by
            && sort_fields_equal(&spec.sort_by, sort_by)
    })
}

fn sort_fields_equal(a: &[SortField], b: &[SortField]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| x.field == y.field && x.order == y.order)
}

/// Errors from index planning.
#[derive(Debug)]
pub enum PlanIndexError {
    InvalidLocalWindow {
        transform: String,
        message: String,
    },
}

impl std::fmt::Display for PlanIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanIndexError::InvalidLocalWindow { transform, message } => {
                write!(
                    f,
                    "invalid local_window on transform '{}': {}",
                    transform, message
                )
            }
        }
    }
}

impl std::error::Error for PlanIndexError {}
