//! Enforcer-sort and correlation-key node insertion passes.

use super::*;

use std::collections::{HashMap, HashSet};

use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;

use crate::config::{NullOrder, SortField, SortOrder, SourceConfig};
use crate::error::PipelineError;
use clinker_core_types::span::Span;

/// Reserved name prefix for planner-synthesized [`PlanNode::Sort`] nodes
/// inserted by [`ExecutionPlanDag::insert_enforcer_sorts`] to satisfy a
/// transform's per-operator [`NodeExecutionReqs::RequiresSortedInput`].
pub const ENFORCER_SORT_PREFIX: &str = "__sort_for_";

/// Reserved name prefix for planner-synthesized [`PlanNode::Sort`] nodes
/// inserted by [`ExecutionPlanDag::inject_correlation_sort`] to materialize
/// each source's `correlation_key:` failure-domain grouping. Distinct
/// from [`ENFORCER_SORT_PREFIX`] because the two passes encode different
/// concerns: per-operator algorithm need vs. per-source identity.
pub const CORRELATION_SORT_PREFIX: &str = "__correlation_sort_";

/// Reserved name prefix for planner-synthesized [`PlanNode::CorrelationCommit`]
/// terminal nodes inserted by [`ExecutionPlanDag::inject_correlation_commit`].
/// User node names matching this prefix are rejected at compile time.
pub const CORRELATION_COMMIT_PREFIX: &str = "__correlation_commit_";

/// Match a predecessor node's name against a producer `target` name, treating a
/// [`CORRELATION_SORT_PREFIX`]-prefixed Sort as an alias for the source it was
/// spliced behind.
///
/// `inject_correlation_sort` splices a synthetic `__correlation_sort_<source>`
/// Sort between a source and its consumers; the suffix is the original source
/// name. An input named for that source therefore matches either the source
/// directly or the splice alias. Shared by Combine driver resolution and Envelope
/// header-port resolution, which both find a wired input among incoming neighbors
/// by name.
pub fn matches_upstream_name(node_name: &str, target: &str) -> bool {
    node_name == target
        || node_name
            .strip_prefix(CORRELATION_SORT_PREFIX)
            .is_some_and(|stripped| stripped == target)
}

fn is_reorder_capable_stage(node: &PlanNode) -> bool {
    matches!(
        node,
        PlanNode::Merge { .. }
            | PlanNode::Combine { .. }
            | PlanNode::Aggregation { .. }
            | PlanNode::Reshape { .. }
            | PlanNode::Cull { .. }
            | PlanNode::Envelope { .. }
            | PlanNode::CorrelationCommit { .. }
    ) || matches!(
        node,
        PlanNode::Transform {
            has_distinct: true,
            ..
        }
    )
}

fn writer_sort_fields_match(planned: &[SortField], required: &[ResolvedSortField]) -> bool {
    planned.len() == required.len()
        && planned.iter().zip(required).all(|(planned, required)| {
            planned.field == required.field
                && planned.order == required.order
                && planned.null_order.unwrap_or(NullOrder::Last) == required.null_order
        })
}

fn render_writer_sort_fields(fields: &[ResolvedSortField]) -> String {
    fields
        .iter()
        .map(|field| {
            let order = match field.order {
                SortOrder::Asc => "asc",
                SortOrder::Desc => "desc",
            };
            let null_order = match field.null_order {
                NullOrder::First => "first",
                NullOrder::Last => "last",
                NullOrder::Drop => "drop",
            };
            format!("{} {order} nulls {null_order}", field.field)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_writer_sort_yaml(fields: &[ResolvedSortField]) -> String {
    fields
        .iter()
        .map(|field| {
            let order = match field.order {
                SortOrder::Asc => "asc",
                SortOrder::Desc => "desc",
            };
            let null_order = match field.null_order {
                NullOrder::First => "first",
                NullOrder::Last => "last",
                NullOrder::Drop => "drop",
            };
            format!(
                "  - {{ field: {}, order: {order}, null_order: {null_order} }}",
                field.field
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

impl ExecutionPlanDag {
    /// Prove or defer every physical writer's terminal ordering contract.
    ///
    /// This pass runs only after structural graph rewrites and therefore
    /// records stable node identities for the actual final producer path.
    /// Sorted streaming output is rejected because it exposes records before
    /// the complete boundary population is available for a stable sort.
    pub fn enforce_terminal_writer_order(
        &self,
        contract: &mut ExecutionOrderContract,
    ) -> Result<(), PipelineError> {
        for boundary in &mut contract.writer_boundaries {
            let producer_idx = self
                .index_of_or_scan(boundary.producer.producer)
                .ok_or_else(|| PipelineError::Internal {
                    op: "writer_boundary_order",
                    node: boundary.output_name.clone(),
                    detail: "compiled writer producer has no live post-rewrite DAG node"
                        .to_string(),
                })?;
            let last_reorder = self.last_reorder_capable_stage(producer_idx);
            boundary.last_reorder_capable = last_reorder.clone();

            let OrderGuarantee::Sorted(fields) = &boundary.guarantee else {
                continue;
            };

            if boundary.mode == WriterBoundaryMode::Streaming {
                let authored_keys = render_writer_sort_fields(fields);
                let authored_yaml = render_writer_sort_yaml(fields);
                let broken_stage = last_reorder
                    .as_ref()
                    .map(|stage| stage.node_name.as_str())
                    .unwrap_or_else(|| self.graph[producer_idx].name());
                return Err(PipelineError::Compilation {
                    transform_name: boundary.output_name.clone(),
                    messages: vec![format!(
                        "Sink '{}' uses {} writer mode after stage '{}' but authored keys [{}] require a complete-population stable sort; materialize the Sink after '{}' with:\nsort_order:\n{}\nor remove the Sink `sort_order` to keep streaming",
                        boundary.output_name,
                        boundary.mode.as_str(),
                        broken_stage,
                        authored_keys,
                        broken_stage,
                        authored_yaml,
                    )],
                });
            }

            let preserving_mode = matches!(
                boundary.mode,
                WriterBoundaryMode::RecordsOnly | WriterBoundaryMode::PerSourceFile
            );
            if preserving_mode
                && let PlanNode::Sort {
                    id,
                    name,
                    sort_fields,
                    ..
                } = &self.graph[producer_idx]
                && writer_sort_fields_match(sort_fields, fields)
            {
                boundary.disposition = WriterOrderDisposition::ProvenTerminalSort {
                    sort: WriterOrderStage {
                        node_id: *id,
                        node_name: name.clone(),
                    },
                    fields: fields.clone(),
                };
                continue;
            }

            boundary.disposition = WriterOrderDisposition::DeferredSort {
                fields: fields.clone(),
            };
        }

        Ok(())
    }

    fn last_reorder_capable_stage(&self, producer_idx: NodeIndex) -> Option<WriterOrderStage> {
        let rank_by_index: HashMap<_, _> = self
            .topo_order
            .iter()
            .enumerate()
            .map(|(rank, idx)| (*idx, rank))
            .collect();
        let mut pending = vec![producer_idx];
        let mut visited = HashSet::new();
        let mut last: Option<(usize, WriterOrderStage)> = None;

        while let Some(idx) = pending.pop() {
            if !visited.insert(idx) {
                continue;
            }
            let node = &self.graph[idx];
            if is_reorder_capable_stage(node) {
                let rank = rank_by_index.get(&idx).copied().unwrap_or(idx.index());
                let replace = last.as_ref().is_none_or(|(last_rank, _)| rank > *last_rank);
                if replace {
                    last = Some((
                        rank,
                        WriterOrderStage {
                            node_id: node.id(),
                            node_name: node.name().to_string(),
                        },
                    ));
                }
            }
            pending.extend(
                self.graph
                    .neighbors_directed(idx, petgraph::Direction::Incoming),
            );
        }

        last.map(|(_, stage)| stage)
    }

    /// Enforcer-sort insertion.
    ///
    /// Walks every [`PlanNode::Transform`] whose
    /// [`NodeExecutionReqs::RequiresSortedInput`] is unsatisfied by its
    /// upstream [`PlanNode::Source`]'s declared `sort_order`, and inserts a
    /// new [`PlanNode::Sort`] enforcer node on the connecting edge.
    ///
    /// # Errors
    /// Returns [`PipelineError::Compilation`] if any user-declared node name
    /// starts with the reserved [`ENFORCER_SORT_PREFIX`].
    ///
    /// # Idempotency
    /// A second call adds zero new nodes: enforcers inserted on the first
    /// pass are themselves [`PlanNode::Sort`], and the walk only operates on
    /// [`PlanNode::Source`] direct parents.
    pub fn insert_enforcer_sorts(
        &mut self,
        inputs: &HashMap<String, SourceConfig>,
        artifacts: &mut crate::plan::bind_schema::CompileArtifacts,
    ) -> Result<(), PipelineError> {
        // Reserved-prefix guard: validate up-front so insertions cannot
        // collide with a user-declared name. Covers every reserved
        // planner prefix at once so a misnamed user node is rejected
        // even before the first synthesis pass runs.
        for idx in self.graph.node_indices() {
            let node = &self.graph[idx];
            if matches!(
                node,
                PlanNode::Sort { .. } | PlanNode::CorrelationCommit { .. }
            ) {
                continue;
            }
            let name = node.name();
            for prefix in [
                ENFORCER_SORT_PREFIX,
                CORRELATION_SORT_PREFIX,
                CORRELATION_COMMIT_PREFIX,
            ] {
                if name.starts_with(prefix) {
                    return Err(PipelineError::Compilation {
                        transform_name: name.to_string(),
                        messages: vec![format!(
                            "node name '{}' uses reserved prefix '{}' (planner-synthesized only)",
                            name, prefix
                        )],
                    });
                }
            }
        }

        // Snapshot consumer indices + their required ordering before mutating.
        let consumers: Vec<(NodeIndex, Vec<SortField>)> = self
            .graph
            .node_indices()
            .filter_map(|idx| match &self.graph[idx] {
                PlanNode::Transform {
                    execution_reqs: NodeExecutionReqs::RequiresSortedInput { sort_fields },
                    ..
                } => Some((idx, sort_fields.clone())),
                _ => None,
            })
            .collect();

        for (consumer_idx, required) in consumers {
            // The only requirement class today (correlated DLQ) consumes
            // a Source directly. Find the unique direct Source parent;
            // skip otherwise (future extensions will widen the walk).
            let source_idx = self
                .graph
                .neighbors_directed(consumer_idx, petgraph::Direction::Incoming)
                .find(|&p| matches!(self.graph[p], PlanNode::Source { .. }));
            let Some(source_idx) = source_idx else {
                continue;
            };

            let source_name = self.graph[source_idx].name().to_string();
            let declared: Vec<SortField> = inputs
                .get(&source_name)
                .and_then(|ic| ic.sort_order.as_ref())
                .map(|specs| specs.iter().cloned().map(|s| s.into_sort_field()).collect())
                .unwrap_or_default();

            if source_ordering_satisfies(&declared, &required) {
                continue;
            }

            // Locate and remove the direct Source→consumer edge, capturing
            // its dependency type and both port tags for re-use on the
            // rewritten edges. Both tags belong to the sort→consumer hop:
            // the consumer is unchanged, and the original producer is
            // preserved upstream of the spliced Sort. The source→sort hop
            // carries no port (Sort takes a single unnamed input and the
            // Source emits a single unnamed output).
            let edge_id = self
                .graph
                .find_edge(source_idx, consumer_idx)
                .expect("source parent must have outgoing edge to consumer");
            let dep_type = self.graph[edge_id].dependency_type;
            let consumer_port = self.graph[edge_id].port.clone();
            let producer_port = self.graph[edge_id].producer_port.clone();
            self.graph.remove_edge(edge_id);

            // Planner-synthesized sort enforcer. No YAML node exists
            // for it; it is derived from the consumer's
            // `RequiresSortedInput` requirement and inserted on the
            // edge from the upstream source. `Span::SYNTHETIC` is
            // correct because the node has no author-written origin.
            let consumer_name = self.graph[consumer_idx].name().to_string();
            let sort_node = PlanNode::Sort {
                name: format!("{ENFORCER_SORT_PREFIX}{consumer_name}"),
                id: artifacts.fresh_node_id(),
                span: Span::SYNTHETIC,
                sort_fields: required,
            };
            let sort_idx = self.graph.add_node(sort_node);
            self.graph.add_edge(
                source_idx,
                sort_idx,
                PlanEdge {
                    dependency_type: dep_type,
                    port: None,
                    producer_port: None,
                },
            );
            self.graph.add_edge(
                sort_idx,
                consumer_idx,
                PlanEdge {
                    dependency_type: dep_type,
                    port: consumer_port,
                    producer_port,
                },
            );
        }

        Ok(())
    }

    /// Inject one [`PlanNode::Sort`] downstream of every source whose
    /// `correlation_key:` is declared, so the per-source failure-domain
    /// grouping has the deterministic ordering the commit phase needs.
    ///
    /// No-op for sources without a declared `correlation_key:`, and
    /// idempotent for sources whose declared `sort_order` already starts
    /// with the source's correlation-key fields. Each sort node carries
    /// the source's CK fields (ascending) as a prefix of any declared
    /// sort, mirroring the per-source identity downstream operators
    /// commit against.
    pub fn inject_correlation_sort(
        &mut self,
        sources: &[&crate::config::pipeline_node::SourceBody],
        artifacts: &mut crate::plan::bind_schema::CompileArtifacts,
    ) -> Result<(), PipelineError> {
        for body in sources {
            let Some(correlation_key) = body.correlation_key.as_ref() else {
                continue;
            };
            let source_cfg = &body.source;

            let key_field_names: Vec<String> = correlation_key
                .fields()
                .into_iter()
                .map(String::from)
                .collect();
            let declared: Vec<SortField> = source_cfg
                .sort_order
                .as_ref()
                .map(|specs| specs.iter().cloned().map(|s| s.into_sort_field()).collect())
                .unwrap_or_default();

            // Idempotent satisfaction: declared sort already starts with
            // the correlation key fields. Direction-agnostic.
            let already_satisfied = key_field_names.len() <= declared.len()
                && key_field_names
                    .iter()
                    .zip(declared.iter())
                    .all(|(k, sf)| k == &sf.field);
            if already_satisfied {
                continue;
            }

            let source_name = &source_cfg.name;
            let source_idx = self.graph.node_indices().find(
                |&idx| matches!(&self.graph[idx], PlanNode::Source { name, .. } if name == source_name),
            );
            let Some(source_idx) = source_idx else {
                return Err(PipelineError::Compilation {
                    transform_name: source_name.clone(),
                    messages: vec![format!(
                        "inject_correlation_sort: source '{source_name}' not found in DAG"
                    )],
                });
            };

            let mut sort_fields: Vec<SortField> = key_field_names
                .iter()
                .map(|f| SortField {
                    field: f.clone(),
                    order: crate::config::SortOrder::Asc,
                    null_order: None,
                })
                .collect();
            for sf in declared.into_iter() {
                if !key_field_names.contains(&sf.field) {
                    sort_fields.push(sf);
                }
            }

            // Capture (target, dep_type, consumer_port, producer_port) for
            // every outgoing edge — both port tags must survive the splice
            // so a downstream composition's named-input edge keeps its tag
            // and any producer output port stays on the new sort→target hop.
            let outgoing: Vec<(NodeIndex, DependencyType, Option<String>, Option<String>)> = self
                .graph
                .edges_directed(source_idx, petgraph::Direction::Outgoing)
                .map(|e| {
                    (
                        e.target(),
                        e.weight().dependency_type,
                        e.weight().port.clone(),
                        e.weight().producer_port.clone(),
                    )
                })
                .collect();
            if outgoing.is_empty() {
                continue;
            }

            // Idempotent insertion guard: every outgoing edge already
            // lands on a CORRELATION_SORT_PREFIX Sort node — nothing to do.
            if outgoing.iter().all(|(t, _, _, _)| {
                matches!(&self.graph[*t], PlanNode::Sort { name, .. } if name.starts_with(CORRELATION_SORT_PREFIX))
            }) {
                continue;
            }

            let sort_node = PlanNode::Sort {
                name: format!("{CORRELATION_SORT_PREFIX}{source_name}"),
                id: artifacts.fresh_node_id(),
                span: Span::SYNTHETIC,
                sort_fields,
            };
            let sort_idx = self.graph.add_node(sort_node);
            for (target, dep_type, port, producer_port) in outgoing {
                if target == sort_idx {
                    continue;
                }
                if let Some(edge_id) = self.graph.find_edge(source_idx, target) {
                    self.graph.remove_edge(edge_id);
                }
                self.graph.add_edge(
                    sort_idx,
                    target,
                    PlanEdge {
                        dependency_type: dep_type,
                        port,
                        producer_port,
                    },
                );
            }
            self.graph.add_edge(
                source_idx,
                sort_idx,
                PlanEdge {
                    dependency_type: DependencyType::Data,
                    port: None,
                    producer_port: None,
                },
            );
        }

        Ok(())
    }

    /// Inject the terminal [`PlanNode::CorrelationCommit`] when any
    /// source declares a `correlation_key:`.
    ///
    /// One commit node is created and every existing [`PlanNode::Sink`]
    /// gains an outgoing edge to it. Output writes from the dispatcher
    /// arm route into per-group correlation buffers keyed by group; the
    /// commit arm walks those buffers at end-of-DAG.
    /// Idempotent — calling twice with a commit already present is a
    /// no-op.
    ///
    /// `commit_group_by` carries the union of every source's CK field
    /// names. The runtime's `buffer_key_for_record` keys each record by
    /// every engine-stamped column it carries, so the commit-side
    /// `commit_group_by` is informational; the union is the most useful
    /// shape for `--explain` rendering.
    pub fn inject_correlation_commit(
        &mut self,
        sources: &[&crate::config::pipeline_node::SourceBody],
        max_group_buffer: u64,
        artifacts: &mut crate::plan::bind_schema::CompileArtifacts,
    ) -> Result<(), PipelineError> {
        if sources.iter().all(|b| b.correlation_key.is_none()) {
            return Ok(());
        }

        // Idempotent: bail if a commit node already exists.
        let already = self
            .graph
            .node_weights()
            .any(|n| matches!(n, PlanNode::CorrelationCommit { .. }));
        if already {
            return Ok(());
        }

        let mut commit_group_by: Vec<String> = Vec::new();
        for body in sources {
            if let Some(ck) = body.correlation_key.as_ref() {
                for field in ck.fields() {
                    let owned = field.to_string();
                    if !commit_group_by.contains(&owned) {
                        commit_group_by.push(owned);
                    }
                }
            }
        }

        let output_indices: Vec<NodeIndex> = self
            .graph
            .node_indices()
            .filter(|&idx| matches!(self.graph[idx], PlanNode::Sink { .. }))
            .collect();
        if output_indices.is_empty() {
            return Ok(());
        }

        let commit_node = PlanNode::CorrelationCommit {
            name: format!("{CORRELATION_COMMIT_PREFIX}terminal"),
            id: artifacts.fresh_node_id(),
            span: Span::SYNTHETIC,
            commit_group_by,
            max_group_buffer,
        };
        let commit_idx = self.graph.add_node(commit_node);

        for output_idx in output_indices {
            self.graph.add_edge(
                output_idx,
                commit_idx,
                PlanEdge {
                    dependency_type: DependencyType::Data,
                    port: None,
                    producer_port: None,
                },
            );
        }

        Ok(())
    }

    /// Whether this DAG carries the correlation-key terminal commit
    /// node. Returns `true` iff [`Self::inject_correlation_commit`]
    /// has run on a pipeline with at least one source declaring a
    /// `correlation_key:` AND at least one [`PlanNode::Sink`] was
    /// present.
    pub fn required_sorted_input(&self) -> bool {
        self.graph
            .node_weights()
            .any(|n| matches!(n, PlanNode::CorrelationCommit { .. }))
    }
}
