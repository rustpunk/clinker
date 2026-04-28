//! Phase E: Index planning.
//!
//! Extracts `analytic_window` configs from pipeline nodes, combines with
//! `AnalysisReport` field sets, deduplicates into `Vec<IndexSpec>`.

use std::collections::HashSet;

use serde::Deserialize;

use crate::config::SortField;

/// Typed representation of the `analytic_window` YAML block on a transform.
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
    /// Window sits downstream of a relaxed-CK aggregate whose dropped CK
    /// fields overlap this window's `partition_by`. Set to `true` by the
    /// plan-time derivation walk in
    /// `derive_window_buffer_recompute_flags`; the executor's window arm
    /// switches from streaming-emit to per-partition buffered output so
    /// the orchestrator's commit phase can recompute affected partitions
    /// over `partition − retracted_rows`. `#[serde(default)]` is required
    /// because YAML never sets this — it is a derived plan-time property.
    #[serde(default)]
    pub requires_buffer_recompute: bool,
}

/// Parse a transform's `analytic_window` JSON value into a typed
/// [`LocalWindowConfig`]. `None` raw value yields `Ok(None)`. Used by
/// `PipelineConfig::compile_with_diagnostics` while building per-node
/// window configs in declaration order.
pub(crate) fn parse_analytic_window_value(
    raw: &Option<serde_json::Value>,
    transform_name: &str,
) -> Result<Option<LocalWindowConfig>, PlanIndexError> {
    match raw {
        None => Ok(None),
        Some(value) => {
            let config: LocalWindowConfig = serde_json::from_value(value.clone()).map_err(|e| {
                PlanIndexError::InvalidLocalWindow {
                    transform: transform_name.to_string(),
                    message: e.to_string(),
                }
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
    /// Mirror of `LocalWindowConfig.requires_buffer_recompute`. Set during
    /// `deduplicate_indices` from the input `RawIndexRequest`s; the
    /// executor's window arm consults this to decide between streaming
    /// emit and buffered emit. When two transforms share a deduplicated
    /// IndexSpec and any one of them needs buffer-recompute, the merged
    /// spec inherits the flag.
    pub requires_buffer_recompute: bool,
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
                // Lift the buffer-recompute flag if any contributing
                // transform requires it: a shared index that one
                // transform reads in buffered mode and another reads in
                // streaming mode collapses to buffered mode (the buffered
                // path's per-record output is a superset of the streaming
                // path's).
                spec.requires_buffer_recompute |= req.requires_buffer_recompute;
            }
            None => {
                deduped.push(IndexSpec {
                    source: req.source,
                    group_by: req.group_by,
                    sort_by: req.sort_by,
                    arena_fields: req.arena_fields,
                    already_sorted: req.already_sorted,
                    requires_buffer_recompute: req.requires_buffer_recompute,
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
    /// Mirror of `LocalWindowConfig.requires_buffer_recompute` for this
    /// transform. Carried through dedup so the merged `IndexSpec` lifts
    /// the flag when any contributing transform needs buffer recompute.
    pub requires_buffer_recompute: bool,
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
    InvalidLocalWindow { transform: String, message: String },
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
