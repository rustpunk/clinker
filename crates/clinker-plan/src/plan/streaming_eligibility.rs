//! Plan-time qualifier for streaming aggregation.
//!
//! Reads an aggregate's parent physical properties and decides whether the
//! aggregate may run in streaming mode (one group emitted per sort-key
//! boundary) or must fall back to blocking hash aggregation.

use std::collections::HashSet;

use crate::config::SortField;
use crate::plan::properties::{NodeProperties, OrderingProvenance, PartitioningKind};

/// Outcome of evaluating whether an aggregation can run in streaming mode
/// given its parent node's physical properties. Returned by
/// [`qualifies_for_streaming`] and consumed by the planner lowering to
/// pick between streaming and hash aggregation.
#[derive(Debug, Clone)]
pub enum StreamingEligibility {
    /// Parent stream qualifies. `effective_group_by` is the group-by list
    /// possibly reordered to match the sort-prefix (PostgreSQL PG 17
    /// `get_useful_group_keys_orderings()` pattern). `qualified_sort_order`
    /// is the subset of the parent sort order that covers the group-by.
    Streaming {
        effective_group_by: Vec<String>,
        qualified_sort_order: Vec<SortField>,
    },
    /// Parent stream does not qualify. `reason` is a human-readable string
    /// surfaced in `--explain` output.
    HashFallback { reason: String },
}

/// Plan-time qualifier for streaming aggregation. Reads the parent node's
/// physical properties and the aggregate's `group_by` list, returns a
/// [`StreamingEligibility`] describing whether streaming is allowed and, if
/// so, the effective group-by order and qualified sort prefix.
///
/// Rules:
/// - Global fold (`group_by` empty) always streams — a single output row
///   with no sort requirement.
/// - `Single` partitioning passes; `HashPartitioned` passes iff its keys
///   cover the group-by; `RoundRobin` always falls back.
/// - A declared sort order is required. The first `group_by.len()` fields
///   of the sort order (the sort prefix) must cover the group-by as a set.
///   Partial or disjoint prefixes fall back to hash.
/// - When the sort prefix covers the group-by in a different order, the
///   group-by is reordered to match the sort prefix (PG 17 pattern).
pub fn qualifies_for_streaming(
    parent_props: &NodeProperties,
    group_by: &[String],
) -> StreamingEligibility {
    // Global fold — always streams, no sort needed.
    if group_by.is_empty() {
        return StreamingEligibility::Streaming {
            effective_group_by: Vec::new(),
            qualified_sort_order: Vec::new(),
        };
    }

    // (a) Partitioning check.
    match &parent_props.partitioning.kind {
        PartitioningKind::Single => {}
        PartitioningKind::HashPartitioned { keys, .. }
            if group_by.iter().all(|g| keys.iter().any(|k| k == g)) => {}
        PartitioningKind::HashPartitioned { keys, .. } => {
            return StreamingEligibility::HashFallback {
                reason: format!(
                    "input is hash-partitioned on {:?}, which does not cover group-by {:?}",
                    keys, group_by
                ),
            };
        }
        PartitioningKind::RoundRobin { num_partitions } => {
            return StreamingEligibility::HashFallback {
                reason: format!(
                    "input is round-robin partitioned across {} partitions; \
                     streaming aggregation requires a single stream or hash-partitioned on group keys",
                    num_partitions
                ),
            };
        }
        PartitioningKind::FilePartitioned { .. } => {
            // FilePartitioned: records are file-sequential at ingest, so
            // each file's records arrive contiguously. Streaming
            // aggregation is eligible *if* the user's group_by is also
            // covered by the parent's `sort_order` — the per-file
            // contiguity gives an implicit `$source.file` prefix that
            // never disagrees, and the user-typed sort_order takes care
            // of the rest. The runtime per-row `$source.file` Arc
            // becomes part of the aggregate group key without the
            // user having to type it.
        }
    }

    // (b) Ordering check.
    let sort_order = match &parent_props.ordering.sort_order {
        Some(so) => so,
        None => {
            return StreamingEligibility::HashFallback {
                reason: format!(
                    "input has no declared sort order ({})",
                    ordering_provenance_summary(&parent_props.ordering.provenance)
                ),
            };
        }
    };

    if sort_order.len() < group_by.len() {
        return StreamingEligibility::HashFallback {
            reason: format!(
                "sort order {:?} is shorter than group-by {:?}; streaming requires full prefix coverage",
                sort_order.iter().map(|s| &s.field).collect::<Vec<_>>(),
                group_by
            ),
        };
    }

    let prefix: HashSet<&str> = sort_order[..group_by.len()]
        .iter()
        .map(|s| s.field.as_str())
        .collect();
    let gb_set: HashSet<&str> = group_by.iter().map(|s| s.as_str()).collect();

    if prefix != gb_set {
        return StreamingEligibility::HashFallback {
            reason: format!(
                "sort order prefix {:?} does not cover group-by {:?}",
                sort_order[..group_by.len()]
                    .iter()
                    .map(|s| &s.field)
                    .collect::<Vec<_>>(),
                group_by
            ),
        };
    }

    // (c) Group-by reordered to match sort prefix (PG 17 pattern).
    let effective_group_by: Vec<String> = sort_order[..group_by.len()]
        .iter()
        .map(|s| s.field.clone())
        .collect();
    let qualified_sort_order: Vec<SortField> = sort_order[..group_by.len()].to_vec();

    StreamingEligibility::Streaming {
        effective_group_by,
        qualified_sort_order,
    }
}

fn ordering_provenance_summary(p: &OrderingProvenance) -> String {
    use OrderingProvenance as OP;
    match p {
        OP::NoOrdering => "no sort_order declared on input".into(),
        OP::DeclaredOnInput { input_name } => {
            format!("declared on input `{}`", input_name)
        }
        OP::Preserved { from_node } => format!("preserved from `{}`", from_node),
        OP::DestroyedByTransformWriteSet {
            at_node,
            sort_fields_lost,
            ..
        } => format!(
            "destroyed by transform `{}` writing {:?}",
            at_node, sort_fields_lost
        ),
        OP::DestroyedByDistinct { at_node, .. } => {
            format!("destroyed by `distinct` in transform `{}`", at_node)
        }
        OP::DestroyedByHashAggregate { at_node, .. } => {
            format!("destroyed by hash aggregate `{}`", at_node)
        }
        OP::DestroyedByMergeMismatch { at_node, .. } => {
            format!("destroyed by merge mismatch at `{}`", at_node)
        }
        OP::DestroyedByCombine { at_node, .. } => {
            format!("destroyed by combine `{}`", at_node)
        }
        OP::IntroducedByStreamingAggregate { at_node, .. } => {
            format!("introduced by streaming aggregate `{}`", at_node)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SortOrder as SO;
    use crate::plan::properties::{
        NodeProperties, Ordering, OrderingProvenance, Partitioning, PartitioningKind,
        PartitioningProvenance,
    };

    fn sf(field: &str) -> SortField {
        SortField {
            field: field.into(),
            order: SO::Asc,
            null_order: None,
        }
    }

    fn props_single_with_sort(sort: Option<Vec<SortField>>) -> NodeProperties {
        NodeProperties {
            ordering: Ordering {
                sort_order: sort,
                provenance: OrderingProvenance::DeclaredOnInput {
                    input_name: "src".into(),
                },
            },
            partitioning: Partitioning {
                kind: PartitioningKind::Single,
                provenance: PartitioningProvenance::SingleStream,
            },
            ck_set: std::collections::BTreeSet::new(),
            ..NodeProperties::unordered_single()
        }
    }

    fn props_with_partitioning(kind: PartitioningKind) -> NodeProperties {
        NodeProperties {
            ordering: Ordering {
                sort_order: None,
                provenance: OrderingProvenance::NoOrdering,
            },
            partitioning: Partitioning {
                kind,
                provenance: PartitioningProvenance::SingleStream,
            },
            ck_set: std::collections::BTreeSet::new(),
            ..NodeProperties::unordered_single()
        }
    }

    #[test]
    fn test_qualifies_single_partition_matching_sort() {
        let props = props_single_with_sort(Some(vec![sf("a"), sf("b")]));
        let gb = vec!["a".to_string(), "b".to_string()];
        match qualifies_for_streaming(&props, &gb) {
            StreamingEligibility::Streaming {
                effective_group_by,
                qualified_sort_order,
            } => {
                assert_eq!(effective_group_by, vec!["a", "b"]);
                assert_eq!(qualified_sort_order.len(), 2);
            }
            other => panic!("expected Streaming, got {:?}", other),
        }
    }

    #[test]
    fn test_qualifies_partial_sort_prefix_falls_back() {
        let props = props_single_with_sort(Some(vec![sf("a")]));
        let gb = vec!["a".to_string(), "b".to_string()];
        match qualifies_for_streaming(&props, &gb) {
            StreamingEligibility::HashFallback { reason } => {
                assert!(reason.contains("shorter than"), "reason: {}", reason);
            }
            other => panic!("expected HashFallback, got {:?}", other),
        }
    }

    #[test]
    fn test_qualifies_disjoint_sort_fields_falls_back() {
        let props = props_single_with_sort(Some(vec![sf("x"), sf("y")]));
        let gb = vec!["a".to_string(), "b".to_string()];
        match qualifies_for_streaming(&props, &gb) {
            StreamingEligibility::HashFallback { reason } => {
                assert!(
                    reason.contains("does not cover group-by"),
                    "reason: {}",
                    reason
                );
            }
            other => panic!("expected HashFallback, got {:?}", other),
        }
    }

    #[test]
    fn test_qualifies_reorders_group_by_to_match_sort() {
        let props = props_single_with_sort(Some(vec![sf("region"), sf("dept")]));
        let gb = vec!["dept".to_string(), "region".to_string()];
        match qualifies_for_streaming(&props, &gb) {
            StreamingEligibility::Streaming {
                effective_group_by, ..
            } => {
                assert_eq!(effective_group_by, vec!["region", "dept"]);
            }
            other => panic!("expected Streaming, got {:?}", other),
        }
    }

    #[test]
    fn test_qualifies_round_robin_partitioning_falls_back() {
        let props = props_with_partitioning(PartitioningKind::RoundRobin { num_partitions: 4 });
        let gb = vec!["a".to_string()];
        match qualifies_for_streaming(&props, &gb) {
            StreamingEligibility::HashFallback { reason } => {
                assert!(reason.contains("round-robin"), "reason: {}", reason);
            }
            other => panic!("expected HashFallback, got {:?}", other),
        }
    }

    #[test]
    fn test_qualifies_hash_partitioned_covering_keys_ok() {
        let props = NodeProperties {
            ordering: Ordering {
                sort_order: Some(vec![sf("a"), sf("b")]),
                provenance: OrderingProvenance::DeclaredOnInput {
                    input_name: "src".into(),
                },
            },
            partitioning: Partitioning {
                kind: PartitioningKind::HashPartitioned {
                    keys: vec!["a".into(), "b".into(), "c".into()],
                    num_partitions: 4,
                },
                provenance: PartitioningProvenance::SingleStream,
            },
            ck_set: std::collections::BTreeSet::new(),
            ..NodeProperties::unordered_single()
        };
        let gb = vec!["a".to_string(), "b".to_string()];
        assert!(matches!(
            qualifies_for_streaming(&props, &gb),
            StreamingEligibility::Streaming { .. }
        ));
    }

    #[test]
    fn test_qualifies_global_fold_always_streaming() {
        // No sort, RoundRobin partitioning — still streams because group_by is empty.
        let props = props_with_partitioning(PartitioningKind::RoundRobin { num_partitions: 8 });
        match qualifies_for_streaming(&props, &[]) {
            StreamingEligibility::Streaming {
                effective_group_by,
                qualified_sort_order,
            } => {
                assert!(effective_group_by.is_empty());
                assert!(qualified_sort_order.is_empty());
            }
            other => panic!("expected Streaming, got {:?}", other),
        }
    }
}
