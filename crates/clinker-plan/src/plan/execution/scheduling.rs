//! Physical-property derivation, volume estimation, and aggregation-strategy
//! selection, plus the `SchedulingHint` projection consumed by the executor.

use super::*;

use std::collections::{BTreeSet, HashMap};

use petgraph::graph::NodeIndex;
use petgraph::visit::Topo;

use crate::config::SortField;
use crate::error::PipelineError;
use crate::plan::properties::{
    NodeProperties, Ordering, OrderingProvenance, Partitioning, PartitioningKind,
    PartitioningProvenance,
};

impl ExecutionPlanDag {
    /// Populate `node_properties` via topological walk.
    ///
    /// Computes [`NodeProperties`] for every node in the DAG, derived from
    /// already-populated parent properties plus declared per-node data
    /// (`SourceConfig.sort_order` for sources, `sort_fields` on
    /// [`PlanNode::Sort`], `write_set` and `has_distinct` on
    /// [`PlanNode::Transform`]). The pass never inspects operator
    /// implementations — it reads only what the plan node already declares,
    /// mirroring Trino's `PropertyDerivations.Visitor` and Spark's
    /// `AliasAwareOutputExpression`.
    ///
    /// # Panics / errors
    ///
    /// Asserts `self.node_properties.is_empty()` on entry as a double-call
    /// guard. Returns [`PipelineError::Compilation`] only if a future variant
    /// rule fails (currently infallible).
    pub fn compute_node_properties(
        &mut self,
        inputs: &HashMap<String, &crate::config::pipeline_node::SourceBody>,
    ) -> Result<(), PipelineError> {
        assert!(
            self.node_properties.is_empty(),
            "compute_node_properties called twice on the same ExecutionPlanDag"
        );

        let mut topo = Topo::new(&self.graph);
        while let Some(idx) = topo.next(&self.graph) {
            let props = {
                let parents: Vec<&NodeProperties> = self
                    .graph
                    .neighbors_directed(idx, petgraph::Direction::Incoming)
                    .filter_map(|p| self.node_properties.get(&p))
                    .collect();
                compute_one(&self.graph[idx], &parents, inputs)
            };
            self.node_properties.insert(idx, props);
        }

        self.populate_fan_out_flags();
        Ok(())
    }

    /// Seed the statistics catalog's Plane A row counts from source file
    /// metadata.
    ///
    /// Reads each Source node's on-disk byte length through the same
    /// [`source_seed_bytes`] metadata read that drives the byte-volume
    /// estimates — never a second data-reading pass — and divides it by the
    /// shared average-record-bytes divisor to land a row-count estimate
    /// keyed by the Source node's name. A source whose size cannot be read
    /// (multi-file `glob`/`regex`/`paths`, a network source, an unreadable
    /// file) records nothing, so the catalog keeps an honest absence rather
    /// than a fabricated zero.
    ///
    /// Runs before combine-strategy selection so the strategy heuristics can
    /// read a build side's row count from the catalog. The byte seed and the
    /// catalog row count are deliberately separate surfaces keyed the same
    /// way: bytes drive the memory arbitrator's peak-volume model, rows
    /// drive the join planner's build-side sizing.
    pub fn seed_statistics_row_counts(
        &self,
        ctx: &crate::config::CompileContext,
        catalog: &mut crate::plan::statistics::StatisticsCatalog,
    ) {
        let anchor = ctx.workspace_root.join(&ctx.pipeline_dir);
        for idx in self.graph.node_indices() {
            if let PlanNode::Source { resolved, .. } = &self.graph[idx] {
                let seed = resolved
                    .as_deref()
                    .and_then(|payload| source_seed_bytes(&payload.source, &anchor));
                catalog.seed_row_count_from_bytes(self.graph[idx].name(), seed);
            }
        }
    }

    /// Attach a coarse per-node input-volume byte prediction to every node
    /// in `node_properties`, derived from the on-disk size of file-backed
    /// Sources and propagated forward in topological order.
    ///
    /// Two predictions land on each node's [`NodeProperties`]:
    ///
    /// - `predicted_peak_bytes` — the live volume the node holds at its
    ///   peak. A file-backed Source seeds it from its single `path:` file's
    ///   `std::fs::metadata` length; a streaming/stateless operator carries
    ///   the same volume it receives from its parents (pass-through); a
    ///   blocking operator (hash Aggregate, sort, grace-hash / sort-merge /
    ///   IEJoin / hash Combine) must accumulate its whole input before it
    ///   can emit, so its peak is the sum of its parents' volume.
    /// - `predicted_freed_bytes_on_complete` — the volume the node releases
    ///   when it finishes draining. A blocking operator frees the state it
    ///   accumulated (its peak); a streaming/fused operator and a Source
    ///   free nothing live, so they report `0`.
    /// - `predicted_subtree_reclaim_bytes` — the largest reclaimable footprint
    ///   anywhere in the node's downstream subtree, propagated up in a second
    ///   reverse-topological pass so a Source's value reflects the eventual
    ///   reclaim its blocking descendants unlock. It is the scheduler's
    ///   lowest-priority tiebreak, used to front-load the independent chain
    ///   whose completion frees the most.
    ///
    /// Blocking-ness is read from [`arbitration_class`] — the same plan-time
    /// mirror of the runtime `MemoryConsumer` impls that `--explain` uses, so
    /// a node counts as blocking exactly when it registers a spillable
    /// consumer at runtime (`spill_priority.is_some()`). Reusing that
    /// classifier means the volume model stays in lock-step with the
    /// arbitrator's notion of which stages hold state, with no second
    /// blocking/streaming taxonomy to keep aligned.
    ///
    /// The pass is deliberately coarse: with no per-node output-cardinality
    /// estimate available, a blocking reducer (e.g. an Aggregate) propagates
    /// its accumulated input volume downstream rather than a smaller emitted
    /// volume. This over-estimates downstream peaks but never under-estimates
    /// them, and — crucially — is a pure function of the plan shape plus the
    /// input files' on-disk sizes. `0` is the universal "unknown" value:
    /// a Source whose size cannot be read seeds `0`, and `0` propagates
    /// inertly through every arm.
    ///
    /// # Determinism
    ///
    /// The estimate depends only on the plan topology and the on-disk file
    /// sizes resolved against a stable anchor — the pipeline file's directory
    /// (`ctx.workspace_root.join(ctx.pipeline_dir)`), never the process CWD —
    /// so an identical plan over identically-sized inputs always yields
    /// identical predictions. Multi-file matchers (`glob`/`regex`/`paths`)
    /// and absent/unreadable files seed `0`, keeping the estimate environment-
    /// independent. The topological walk guarantees every parent's volume is
    /// already set before a child reads it; nodes added by later passes
    /// (e.g. synthetic combine-chain steps) that never received a
    /// `node_properties` row contribute `0`, matching how `--explain`
    /// already skips them.
    ///
    /// Runs after aggregation- and combine-strategy selection so the resolved
    /// strategy is visible to [`arbitration_class`] and no later property
    /// overwrite clobbers the volume fields.
    pub fn derive_volume_estimates(&mut self, ctx: &crate::config::CompileContext) {
        // Stable anchor for resolving relative source `path:` strings — the
        // pipeline file's directory, reconstructed from the compile context
        // as `workspace_root.join(pipeline_dir)`. The CLI builds the context
        // so this equals the runtime discovery anchor (`config.parent()`), so
        // a `--explain` surfacing of these sizes names the same bytes the
        // actual run would read and never depends on the process CWD.
        let anchor = ctx.workspace_root.join(&ctx.pipeline_dir);

        let order = self.topo_order.clone();
        for idx in order {
            // Skip nodes that never received a base `node_properties` row
            // (synthetic combine-chain steps inserted post-derivation). Their
            // downstream consumers treat the missing volume as `0`.
            if !self.node_properties.contains_key(&idx) {
                continue;
            }

            let (peak, freed) = match &self.graph[idx] {
                PlanNode::Source { resolved, .. } => {
                    // Read the seed off the resolved `SourceConfig` the node
                    // already carries — keyed by node identity, not by a
                    // separate name map. This stays correct when a node's
                    // header `name:` differs from its nested `config.name:`,
                    // a shape the compiler accepts.
                    let seed = resolved
                        .as_deref()
                        .and_then(|payload| source_seed_bytes(&payload.source, &anchor))
                        .unwrap_or(0);
                    // A Source streams records out as it reads; it never holds
                    // an accumulated copy it can free on drain.
                    (seed, 0)
                }
                node => {
                    // Sum parents' already-computed peak volume. A missing
                    // parent row (synthetic node) contributes 0.
                    let incoming: u64 = self
                        .graph
                        .neighbors_directed(idx, petgraph::Direction::Incoming)
                        .filter_map(|p| self.node_properties.get(&p))
                        .map(|p| p.predicted_peak_bytes)
                        .fold(0u64, |acc, v| acc.saturating_add(v));
                    // Blocking iff the node registers a spillable consumer at
                    // runtime. Such a node accumulates its whole input before
                    // emitting and frees that state on drain; a streaming /
                    // stateless / sink node holds nothing live to free.
                    let blocking = arbitration_class(node).spill_priority.is_some();
                    let freed = if blocking { incoming } else { 0 };
                    (incoming, freed)
                }
            };

            // The presence check above guarantees this entry exists.
            if let Some(props) = self.node_properties.get_mut(&idx) {
                props.predicted_peak_bytes = peak;
                props.predicted_freed_bytes_on_complete = freed;
            }
        }

        // Second pass, reverse topological order: propagate the largest
        // reclaimable footprint of each node's downstream chain UP to the
        // node itself. A node's `predicted_subtree_reclaim_bytes` is the max
        // of its own `predicted_freed_bytes_on_complete` and the subtree
        // reclaim of every immediate successor THIS node solely feeds — a
        // successor with exactly one incoming edge. Walking children before
        // parents (reverse of the forward topo order) guarantees each
        // successor's value is final before its predecessor reads it.
        //
        // This is what gives a Source feeding a blocking chain a non-zero
        // reclaim value: the Source frees nothing itself, but launching it is
        // the only path to the point where its downstream Aggregate can drain,
        // so the Source inherits that Aggregate's reclaimable state. The
        // scheduler uses it to front-load the chain whose completion frees the
        // most, while keeping immediate (drain-now) reclaim a strictly higher
        // priority.
        //
        // Propagation stops at a convergence node — a successor with more than
        // one incoming edge, e.g. the `Combine` two independent chains feed.
        // Reaching that node requires EVERY parent chain to finish, so no
        // single upstream chain alone "unlocks" its reclaim; charging the
        // whole join to each feeding Source would make every Source's value
        // identical and erase the per-chain distinction the scheduler needs.
        // The join keeps its own reclaim (it elects itself once all inputs are
        // ready and it can drain), and each feeding chain is ranked by the
        // reclaim it owns up to that join. `0` propagates inertly, so the
        // determinism floor — all-zero estimates reproduce topo order — is
        // untouched.
        for &idx in self.topo_order.iter().rev() {
            if !self.node_properties.contains_key(&idx) {
                continue;
            }
            let own_freed = self.node_properties[&idx].predicted_freed_bytes_on_complete;
            let children_reclaim = self
                .graph
                .neighbors_directed(idx, petgraph::Direction::Outgoing)
                .filter(|&c| {
                    // Only a child this node solely feeds carries its reclaim
                    // up; a convergence node (in-degree > 1) does not, so its
                    // post-join reclaim is not double-counted onto every
                    // feeding chain.
                    self.graph
                        .neighbors_directed(c, petgraph::Direction::Incoming)
                        .count()
                        == 1
                })
                .filter_map(|c| self.node_properties.get(&c))
                .map(|p| p.predicted_subtree_reclaim_bytes)
                .max()
                .unwrap_or(0);
            if let Some(props) = self.node_properties.get_mut(&idx) {
                props.predicted_subtree_reclaim_bytes = own_freed.max(children_reclaim);
            }
        }
    }

    /// Mark every Output whose path template references a per-record
    /// token (`{source_file}` / `{source_path}`) AND whose parent
    /// partitioning is `FilePartitioned` for runtime fan-out routing.
    /// The CLI consults this flag to pre-open one writer per source file
    /// rather than one global writer; the dispatcher routes each record
    /// to the right writer via `$source.file`.
    ///
    /// Single-file sources keep `fan_out_per_source_file: false` and
    /// behave as before. Outputs whose template lacks per-record tokens
    /// also stay single, regardless of upstream partitioning — the
    /// emitted single file just concatenates records from every source
    /// file into one writer in arrival order.
    fn populate_fan_out_flags(&mut self) {
        use crate::config::path_template::PathTemplate;

        // Snapshot indices and partitioning so we can mutate plan
        // payloads without holding the immutable borrow on
        // `node_properties`.
        let output_indices: Vec<NodeIndex> = self
            .graph
            .node_indices()
            .filter(|i| matches!(self.graph[*i], PlanNode::Output { .. }))
            .collect();
        let parent_partitioning: HashMap<NodeIndex, PartitioningKind> = output_indices
            .iter()
            .filter_map(|&i| {
                let parent_idx = self
                    .graph
                    .neighbors_directed(i, petgraph::Direction::Incoming)
                    .next()?;
                self.node_properties
                    .get(&parent_idx)
                    .map(|p| (i, p.partitioning.kind.clone()))
            })
            .collect();

        for idx in output_indices {
            let Some(parent_part) = parent_partitioning.get(&idx) else {
                continue;
            };
            let is_partitioned = matches!(parent_part, PartitioningKind::FilePartitioned { .. });
            if !is_partitioned {
                continue;
            }
            let PlanNode::Output {
                resolved: Some(payload),
                ..
            } = &mut self.graph[idx]
            else {
                continue;
            };
            // Parse the template to detect per-record tokens. Failed
            // parses leave the flag at `false`; the existing path-
            // validation pass surfaces the parse error separately.
            let Ok(template) = PathTemplate::parse(&payload.output.path) else {
                continue;
            };
            if template.has_per_record_tokens() {
                payload.fan_out_per_source_file = true;
            }
        }
    }

    /// Mark every [`IndexSpec`] whose owning window-bearing Transform
    /// participates in a relaxed-CK retraction pipeline AND whose
    /// `partition_by` does not cover the source-side correlation-key
    /// set. The flag flips the executor's window arm into buffered emit
    /// mode so the orchestrator's commit-time recompute can rerun the
    /// window over `partition − retracted_rows` and emit per-output
    /// Deltas.
    ///
    /// The trigger captures both directions of the geometry:
    ///
    /// * Window upstream of a relaxed-CK aggregate. The window operates
    ///   on source-side arena positions; a CK group can span multiple
    ///   partitions when `partition_by` is not a CK superset; the
    ///   relaxed aggregate downstream provides the retraction protocol
    ///   that needs the per-partition rollback.
    ///
    /// * Window downstream of a relaxed-CK aggregate. The aggregate's
    ///   dropped CK fields no longer appear in the partition's
    ///   downstream `ck_set`; if the window's `partition_by` references
    ///   one of those fields, partitions can span what would have been
    ///   strict CK boundaries.
    ///
    /// The unified rule: at the window's node, the visible `ck_set` has
    /// at least one field that is not part of the window's
    /// `partition_by` slice. Reads `node_properties.ck_set`, so callers
    /// must invoke `compute_node_properties` first. Idempotent.
    pub(crate) fn derive_window_buffer_recompute_flags(&mut self) {
        let ck_at = |idx: NodeIndex| -> BTreeSet<String> {
            self.node_properties
                .get(&idx)
                .map(|p| p.ck_set.clone())
                .unwrap_or_default()
        };
        derive_window_buffer_recompute_flags_for_graph(
            &self.graph,
            &mut self.indices_to_build,
            ck_at,
        );
    }

    /// Resolve `AggregateStrategyHint` on every `PlanNode::Aggregation`
    /// against upstream `OrderingProvenance`, rewrite the node's
    /// `strategy` field, populate the auxiliary `fallback_reason` /
    /// `skipped_streaming_available` / `qualified_sort_order` fields,
    /// and overwrite the side-table `node_properties[idx].ordering` to
    /// reflect the resolved strategy.
    ///
    /// Runs as a separate post-pass inside `compile()` immediately after
    /// `compute_node_properties()`. Hard-errors at compile time when a
    /// user explicitly requests `strategy: streaming` on an ineligible
    /// input, via the rustc-shaped walker
    /// `render_unordered_streaming_error`.
    pub(crate) fn select_aggregation_strategies(&mut self) -> Result<(), PipelineError> {
        use crate::config::AggregateStrategyHint;
        use crate::plan::properties::{Confidence, render_unordered_streaming_error};
        use crate::plan::streaming_eligibility::{StreamingEligibility, qualifies_for_streaming};
        use crate::plan::types::AggregateStrategy;

        // Collect target indices first to avoid holding a borrow on `graph`
        // while mutating `node_properties`.
        let agg_indices: Vec<NodeIndex> = self
            .topo_order
            .iter()
            .copied()
            .filter(|idx| matches!(self.graph[*idx], PlanNode::Aggregation { .. }))
            .collect();

        for idx in agg_indices {
            let (name, hint, group_by) = match &self.graph[idx] {
                PlanNode::Aggregation { name, config, .. } => {
                    (name.clone(), config.strategy, config.group_by.clone())
                }
                _ => unreachable!(),
            };

            let parent_idx =
                crate::plan::execution::single_predecessor(self, idx, "aggregation", &name)?;
            let parent_props = self
                .node_properties
                .get(&parent_idx)
                .cloned()
                .ok_or_else(|| PipelineError::Internal {
                    op: "aggregation",
                    node: name.clone(),
                    detail: "parent node has no computed NodeProperties".to_string(),
                })?;

            let eligibility = qualifies_for_streaming(&parent_props, &group_by);

            // Resolve hint → (strategy, fallback_reason, skipped_streaming_available,
            // qualified_sort_order). On explicit Streaming + ineligible, hard-error.
            let resolved: ResolvedStrategy = match hint {
                AggregateStrategyHint::Auto => match &eligibility {
                    StreamingEligibility::Streaming {
                        qualified_sort_order,
                        ..
                    } => ResolvedStrategy {
                        strategy: AggregateStrategy::Streaming,
                        fallback_reason: None,
                        skipped_streaming_available: false,
                        qualified_sort_order: Some(qualified_sort_order.clone()),
                    },
                    StreamingEligibility::HashFallback { reason } => ResolvedStrategy {
                        strategy: AggregateStrategy::Hash,
                        fallback_reason: Some(reason.clone()),
                        skipped_streaming_available: false,
                        qualified_sort_order: None,
                    },
                },
                AggregateStrategyHint::Hash => ResolvedStrategy {
                    strategy: AggregateStrategy::Hash,
                    fallback_reason: None,
                    skipped_streaming_available: matches!(
                        eligibility,
                        StreamingEligibility::Streaming { .. }
                    ),
                    qualified_sort_order: None,
                },
                AggregateStrategyHint::Streaming => match &eligibility {
                    StreamingEligibility::Streaming {
                        qualified_sort_order,
                        ..
                    } => ResolvedStrategy {
                        strategy: AggregateStrategy::Streaming,
                        fallback_reason: None,
                        skipped_streaming_available: false,
                        qualified_sort_order: Some(qualified_sort_order.clone()),
                    },
                    StreamingEligibility::HashFallback { .. } => {
                        let msg = render_unordered_streaming_error(&parent_props, &group_by, &name);
                        return Err(PipelineError::Compilation {
                            transform_name: name.clone(),
                            messages: vec![msg],
                        });
                    }
                },
            };

            // Capture parent provenance before mutating self.graph for the
            // streaming-output ordering chain.
            let parent_provenance = parent_props.ordering.provenance.clone();

            // Mutate the PlanNode in place.
            if let PlanNode::Aggregation {
                strategy,
                fallback_reason,
                skipped_streaming_available,
                qualified_sort_order,
                ..
            } = &mut self.graph[idx]
            {
                *strategy = resolved.strategy;
                *fallback_reason = resolved.fallback_reason.clone();
                *skipped_streaming_available = resolved.skipped_streaming_available;
                *qualified_sort_order = resolved.qualified_sort_order.clone();
            }

            // Preserve the CK set computed by `compute_one`. The post-pass
            // overwrites ordering/partitioning to reflect the resolved
            // strategy, but the CK lattice has already been computed and
            // must survive the rewrite — the resolved strategy does not
            // change which `$ck.<field>` columns this aggregate's output
            // carries.
            let preserved_ck_set = self
                .node_properties
                .get(&idx)
                .map(|p| p.ck_set.clone())
                .unwrap_or_default();

            // Overwrite the side-table ordering for this aggregation node
            // (D77 — single source of truth for aggregation ordering).
            let new_props = match resolved.strategy {
                AggregateStrategy::Streaming => NodeProperties {
                    ordering: Ordering {
                        sort_order: resolved.qualified_sort_order.clone(),
                        provenance: OrderingProvenance::IntroducedByStreamingAggregate {
                            at_node: name.clone(),
                            enabled_by: Box::new(parent_provenance),
                        },
                    },
                    partitioning: Partitioning {
                        kind: PartitioningKind::Single,
                        provenance: PartitioningProvenance::SingleStream,
                    },
                    ck_set: preserved_ck_set.clone(),
                    // Volume is re-derived by `derive_volume_estimates`,
                    // which runs after this strategy pass; zero here.
                    ..NodeProperties::unordered_single()
                },
                AggregateStrategy::Hash => NodeProperties {
                    ordering: Ordering {
                        sort_order: None,
                        provenance: OrderingProvenance::DestroyedByHashAggregate {
                            at_node: name.clone(),
                            confidence: Confidence::Proven,
                        },
                    },
                    partitioning: Partitioning {
                        kind: PartitioningKind::Single,
                        provenance: PartitioningProvenance::SingleStream,
                    },
                    ck_set: preserved_ck_set,
                    ..NodeProperties::unordered_single()
                },
            };
            self.node_properties.insert(idx, new_props);
        }

        Ok(())
    }
}

/// Exposes the plan's per-node volume predictions and topological order to
/// the memory arbitrator's runnable-node selection.
///
/// The byte predictions — peak, immediate freed-on-complete, and downstream
/// subtree reclaim — are the plan-time estimates `derive_volume_estimates`
/// stamps onto each node's [`NodeProperties`]; the stable index is the node's
/// position in [`topo_order`](ExecutionPlanDag::topo_order), which is the same
/// order the executor walks the DAG. A node absent from `node_properties`
/// (e.g. a synthetic combine-chain step that never received a base row)
/// predicts `0` bytes, matching how `derive_volume_estimates` treats it.
impl crate::plan::scheduling_hint::SchedulingHint for ExecutionPlanDag {
    fn predicted_peak_bytes(&self, id: NodeIndex) -> u64 {
        self.node_properties
            .get(&id)
            .map(|p| p.predicted_peak_bytes)
            .unwrap_or(0)
    }

    fn predicted_freed_bytes_on_complete(&self, id: NodeIndex) -> u64 {
        self.node_properties
            .get(&id)
            .map(|p| p.predicted_freed_bytes_on_complete)
            .unwrap_or(0)
    }

    fn predicted_subtree_reclaim_bytes(&self, id: NodeIndex) -> u64 {
        self.node_properties
            .get(&id)
            .map(|p| p.predicted_subtree_reclaim_bytes)
            .unwrap_or(0)
    }

    fn stable_index(&self, id: NodeIndex) -> usize {
        // Position in `topo_order` is the plan's canonical node ordering and
        // the exact sequence the executor dispatches in, so it is the stable
        // tiebreak that makes "no-estimates == today's order" hold. A node
        // outside `topo_order` sorts last, deterministically, rather than
        // colliding with the first real node at index 0.
        self.topo_order
            .iter()
            .position(|&n| n == id)
            .unwrap_or(usize::MAX)
    }
}

/// Internal carrier for `select_aggregation_strategies` resolution result.
struct ResolvedStrategy {
    strategy: crate::plan::types::AggregateStrategy,
    fallback_reason: Option<String>,
    skipped_streaming_available: bool,
    qualified_sort_order: Option<Vec<SortField>>,
}

/// Per-node property derivation rule. Pure function over the node and its
/// (already-computed) parent properties — never sees operator implementations.
///
/// Per Decision D46, the `Transform` arm MUST NOT advertise any ordering
/// produced by Phase 6 arena `sort_partition` — that sort is intra-partition
/// inside a `RequiresArena` materialization layer and is invisible at the
/// stream level. The rule below reads only `write_set` / `has_distinct` and
/// the parent's already-computed ordering, so arena-internal sorts cannot
/// leak in.
fn compute_one(
    node: &PlanNode,
    parents: &[&NodeProperties],
    inputs: &HashMap<String, &crate::config::pipeline_node::SourceBody>,
) -> NodeProperties {
    let single_stream_partitioning = || Partitioning {
        kind: PartitioningKind::Single,
        provenance: PartitioningProvenance::SingleStream,
    };
    let parent_partitioning = || {
        parents
            .first()
            .map(|p| p.partitioning.clone())
            .unwrap_or_else(single_stream_partitioning)
    };
    // Default lattice rule for nodes that do not transform CK visibility
    // (Transform, Sort, Route, Output, Composition): preserve the first
    // parent's CK set, or empty when there is no parent.
    let preserve_parent_ck_set = || {
        parents
            .first()
            .map(|p| p.ck_set.clone())
            .unwrap_or_default()
    };

    match node {
        PlanNode::Source { name, .. } => {
            let body = inputs.get(name).copied();
            let sort_order: Option<Vec<SortField>> = body
                .and_then(|b| b.source.sort_order.as_ref())
                .map(|specs| specs.iter().cloned().map(|s| s.into_sort_field()).collect());
            let provenance = if sort_order.is_some() {
                OrderingProvenance::DeclaredOnInput {
                    input_name: name.clone(),
                }
            } else {
                OrderingProvenance::NoOrdering
            };
            // Source observes every CK field declared on its own
            // `correlation_key:` — those columns are shadow-stamped at
            // ingest. Sources without a declared CK contribute an
            // empty set; multi-source pipelines rely on each source's
            // local declaration.
            let ck_set: BTreeSet<String> = body
                .and_then(|b| b.correlation_key.as_ref())
                .map(|ck| {
                    ck.fields()
                        .into_iter()
                        .map(String::from)
                        .collect::<BTreeSet<String>>()
                })
                .unwrap_or_default();
            // Multi-file matchers (`glob` / `regex` / `paths`) produce
            // a `FilePartitioned` lineage carrying `$source.file` as the
            // implicit partition key. Literal `path:` is single-file
            // and stays `Single`. The runtime per-row source-file Arc
            // tagging set up by the executor's source-ingestion phase
            // (§6) makes the partition key visible to downstream
            // stateful nodes.
            let is_multi_file = body
                .map(|b| {
                    b.source.glob.is_some() || b.source.regex.is_some() || b.source.paths.is_some()
                })
                .unwrap_or(false);
            let partitioning = if is_multi_file {
                Partitioning {
                    kind: PartitioningKind::FilePartitioned {
                        keys: vec!["$source.file".to_string()],
                    },
                    provenance: PartitioningProvenance::IntroducedByMultiFileSource {
                        source_name: name.clone(),
                    },
                }
            } else {
                single_stream_partitioning()
            };
            NodeProperties {
                ordering: Ordering {
                    sort_order,
                    provenance,
                },
                partitioning,
                ck_set,
                ..NodeProperties::unordered_single()
            }
        }

        PlanNode::Sort {
            name, sort_fields, ..
        } => NodeProperties {
            ordering: Ordering {
                sort_order: Some(sort_fields.clone()),
                provenance: OrderingProvenance::Preserved {
                    from_node: name.clone(),
                },
            },
            partitioning: parent_partitioning(),
            ck_set: preserve_parent_ck_set(),
            ..NodeProperties::unordered_single()
        },

        PlanNode::Transform {
            name,
            write_set,
            has_distinct,
            ..
        } => {
            let partitioning = parent_partitioning();
            let ck_set = preserve_parent_ck_set();

            // Distinct destroys ordering unconditionally.
            if *has_distinct {
                return NodeProperties {
                    ordering: Ordering {
                        sort_order: None,
                        provenance: OrderingProvenance::DestroyedByDistinct {
                            at_node: name.clone(),
                            confidence: crate::plan::properties::Confidence::Proven,
                        },
                    },
                    partitioning,
                    ck_set,
                    ..NodeProperties::unordered_single()
                };
            }

            // No parent or parent had no ordering — nothing to preserve.
            let parent = parents.first();
            let parent_sort = parent.and_then(|p| p.ordering.sort_order.as_ref());
            let Some(parent_sort) = parent_sort else {
                return NodeProperties {
                    ordering: Ordering {
                        sort_order: None,
                        provenance: parent
                            .map(|p| p.ordering.provenance.clone())
                            .unwrap_or(OrderingProvenance::NoOrdering),
                    },
                    partitioning,
                    ck_set,
                    ..NodeProperties::unordered_single()
                };
            };

            // Intersect write_set with parent's sort-key field names. Note:
            // arena `sort_partition` does not appear here — only
            // record fields written via `emit name = ...` enter `write_set`.
            let lost: Vec<String> = parent_sort
                .iter()
                .filter(|sf| write_set.contains(&sf.field))
                .map(|sf| sf.field.clone())
                .collect();
            if lost.is_empty() {
                NodeProperties {
                    ordering: Ordering {
                        sort_order: Some(parent_sort.clone()),
                        provenance: OrderingProvenance::Preserved {
                            from_node: name.clone(),
                        },
                    },
                    partitioning,
                    ck_set,
                    ..NodeProperties::unordered_single()
                }
            } else {
                NodeProperties {
                    ordering: Ordering {
                        sort_order: None,
                        provenance: OrderingProvenance::DestroyedByTransformWriteSet {
                            at_node: name.clone(),
                            fields_written: write_set.iter().cloned().collect(),
                            sort_fields_lost: lost,
                            confidence: crate::plan::properties::Confidence::Proven,
                        },
                    },
                    partitioning,
                    ck_set,
                    ..NodeProperties::unordered_single()
                }
            }
        }

        PlanNode::Route { name, .. } => {
            // Row-selection only — every branch inherits parent ordering and
            // partitioning unchanged. Provenance is rewritten to point at this
            // node so explain can chain through.
            let parent = match parents.first() {
                Some(p) => p,
                None => return NodeProperties::unordered_single(),
            };
            let provenance = if parent.ordering.sort_order.is_some() {
                OrderingProvenance::Preserved {
                    from_node: name.clone(),
                }
            } else {
                parent.ordering.provenance.clone()
            };
            NodeProperties {
                ordering: Ordering {
                    sort_order: parent.ordering.sort_order.clone(),
                    provenance,
                },
                partitioning: parent.partitioning.clone(),
                ck_set: parent.ck_set.clone(),
                ..NodeProperties::unordered_single()
            }
        }

        PlanNode::Merge { name, .. } => {
            if parents.is_empty() {
                return NodeProperties::unordered_single();
            }
            let first_so = parents[0].ordering.sort_order.clone();
            let all_match = parents
                .iter()
                .all(|p| sort_orders_equal(&p.ordering.sort_order, &first_so));
            // Merge is the explicit opt-out from per-file partitioning:
            // when any parent carries `FilePartitioned`, the merged
            // stream is unpartitioned (the user asked to cross file
            // boundaries). Provenance carries the merge node's name so
            // `--explain` can chain from a downstream global aggregate
            // back to where partitioning was dropped.
            let any_file_partitioned = parents.iter().any(|p| {
                matches!(
                    p.partitioning.kind,
                    PartitioningKind::FilePartitioned { .. }
                )
            });
            let partitioning = if any_file_partitioned {
                Partitioning {
                    kind: PartitioningKind::Single,
                    provenance: PartitioningProvenance::DestroyedByMerge {
                        at_node: name.clone(),
                    },
                }
            } else if parents
                .iter()
                .all(|p| matches!(p.partitioning.kind, PartitioningKind::Single))
            {
                single_stream_partitioning()
            } else {
                parents[0].partitioning.clone()
            };
            // Intersect CK sets across all parents: a CK column is visible
            // post-merge only when every parent stream still carries it.
            // If a sibling branch has already passed through a relaxed
            // Aggregate that dropped a CK field, the smaller visible set
            // wins downstream.
            let ck_set: BTreeSet<String> = {
                let mut iter = parents.iter().map(|p| p.ck_set.clone());
                match iter.next() {
                    Some(first) => iter.fold(first, |acc, next| {
                        acc.intersection(&next).cloned().collect()
                    }),
                    None => BTreeSet::new(),
                }
            };
            if all_match {
                let provenance = if first_so.is_some() {
                    OrderingProvenance::Preserved {
                        from_node: name.clone(),
                    }
                } else {
                    OrderingProvenance::NoOrdering
                };
                NodeProperties {
                    ordering: Ordering {
                        sort_order: first_so,
                        provenance,
                    },
                    partitioning,
                    ck_set,
                    ..NodeProperties::unordered_single()
                }
            } else {
                NodeProperties {
                    ordering: Ordering {
                        sort_order: None,
                        provenance: OrderingProvenance::DestroyedByMergeMismatch {
                            at_node: name.clone(),
                            parent_orderings: parents
                                .iter()
                                .map(|p| p.ordering.sort_order.clone())
                                .collect(),
                            confidence: crate::plan::properties::Confidence::Proven,
                        },
                    },
                    partitioning,
                    ck_set,
                    ..NodeProperties::unordered_single()
                }
            }
        }

        PlanNode::Aggregation { name, config, .. } => {
            // Aggregation node ordering is the sole responsibility of
            // the `select_aggregation_strategies` post-pass, which runs
            // immediately after `compute_node_properties` and overwrites
            // this entry based on the resolved strategy. The defensive
            // default is "no ordering, single stream" so any bug that
            // bypasses the post-pass produces conservative
            // (correct-but-suboptimal) downstream eligibility decisions
            // rather than silently asserting a false ordering.
            //
            // CK lattice rule:
            //   - strict (`group_by ⊇ parent.ck_set`): preserves parent
            //     CK set unchanged.
            //   - relaxed (`group_by` omits any CK field visible in
            //     the parent lattice): intersects parent CK set with
            //     `group_by`, then injects a synthetic
            //     `$ck.aggregate.<name>` entry that the schema-widening
            //     pass already appended to the aggregate's
            //     `output_schema`. The synthetic entry keeps a relaxed
            //     aggregate's downstream consumers correlation-aware
            //     even when the original source CK field has dropped
            //     out of the lattice — detect-phase fan-out resolves
            //     it back to source rows via the aggregator's
            //     `input_rows` lineage.
            let parent_ck = preserve_parent_ck_set();
            let omits_ck = group_by_omits_any_ck_field(&config.group_by, &parent_ck);
            let mut ck_set: BTreeSet<String> = if omits_ck {
                let group_by_set: BTreeSet<&str> =
                    config.group_by.iter().map(String::as_str).collect();
                parent_ck
                    .into_iter()
                    .filter(|f| group_by_set.contains(f.as_str()))
                    .collect()
            } else {
                parent_ck
            };
            if omits_ck {
                ck_set.insert(format!("$ck.aggregate.{name}"));
            }
            // Preserve parent FilePartitioned through aggregation —
            // the aggregate produces one row per (partition, group)
            // tuple, and downstream stages can still route per-file.
            // Single-stream parents stay Single (today's behavior).
            let agg_partitioning = match parents.first() {
                Some(p)
                    if matches!(
                        p.partitioning.kind,
                        PartitioningKind::FilePartitioned { .. }
                    ) =>
                {
                    Partitioning {
                        kind: p.partitioning.kind.clone(),
                        provenance: PartitioningProvenance::Preserved {
                            from_node: name.clone(),
                        },
                    }
                }
                _ => single_stream_partitioning(),
            };
            NodeProperties {
                ordering: Ordering {
                    sort_order: None,
                    provenance: OrderingProvenance::NoOrdering,
                },
                partitioning: agg_partitioning,
                ck_set,
                ..NodeProperties::unordered_single()
            }
        }

        PlanNode::Output { name, .. } => {
            // Terminal — properties still computed for debugging. Inherit
            // parent, rewrite provenance to point at this node when ordering
            // is non-None so explain chains through. CK set is preserved at
            // Output so the inclusion-flag interaction (writer-default
            // strip vs `include_correlation_keys: true`) can consult the
            // surviving CK columns.
            let parent = match parents.first() {
                Some(p) => p,
                None => return NodeProperties::unordered_single(),
            };
            let provenance = if parent.ordering.sort_order.is_some() {
                OrderingProvenance::Preserved {
                    from_node: name.clone(),
                }
            } else {
                parent.ordering.provenance.clone()
            };
            NodeProperties {
                ordering: Ordering {
                    sort_order: parent.ordering.sort_order.clone(),
                    provenance,
                },
                partitioning: parent.partitioning.clone(),
                ck_set: parent.ck_set.clone(),
                ..NodeProperties::unordered_single()
            }
        }

        PlanNode::Composition { name, .. } => {
            // Composition is opaque at the top-level property pass.
            // Inherit parent ordering/partitioning — body-internal
            // properties live on the bound body's per-node rows, not
            // on this node.
            let parent = match parents.first() {
                Some(p) => p,
                None => return NodeProperties::unordered_single(),
            };
            let provenance = if parent.ordering.sort_order.is_some() {
                OrderingProvenance::Preserved {
                    from_node: name.clone(),
                }
            } else {
                parent.ordering.provenance.clone()
            };
            NodeProperties {
                ordering: Ordering {
                    sort_order: parent.ordering.sort_order.clone(),
                    provenance,
                },
                partitioning: parent.partitioning.clone(),
                ck_set: parent.ck_set.clone(),
                ..NodeProperties::unordered_single()
            }
        }

        PlanNode::Reshape { .. } => {
            // Reshape re-orders rows within each group (`order_by`) and
            // injects synthesized rows, so no parent ordering survives;
            // downstream sees an unordered stream. Group identity is
            // preserved — `partition_by` must cover every visible CK
            // field — so the parent CK set flows through unchanged.
            NodeProperties {
                ordering: Ordering {
                    sort_order: None,
                    provenance: OrderingProvenance::NoOrdering,
                },
                partitioning: parent_partitioning(),
                ck_set: preserve_parent_ck_set(),
                ..NodeProperties::unordered_single()
            }
        }

        PlanNode::Cull { .. } => {
            // Cull removes whole correlation groups but neither reorders nor
            // synthesizes rows; the records that survive keep their relative
            // order. The buffer drains group-by-group, though, so the
            // cross-group emission order is not the input order — emit
            // `NoOrdering` rather than claiming preservation. Group identity
            // is preserved (`partition_by` covers every visible CK field), so
            // the parent CK set flows through unchanged on both output ports.
            NodeProperties {
                ordering: Ordering {
                    sort_order: None,
                    provenance: OrderingProvenance::NoOrdering,
                },
                partitioning: parent_partitioning(),
                ck_set: preserve_parent_ck_set(),
                ..NodeProperties::unordered_single()
            }
        }

        PlanNode::Envelope { name, .. } => {
            // Envelope `preserve` re-parks body records in drained order with
            // their grains unchanged — it neither reorders, repartitions, nor
            // transforms CK visibility — so the parent's ordering, partitioning,
            // and CK set all flow through. Provenance is rewritten to point at
            // this node so explain can chain through, matching Route / Output.
            let parent = match parents.first() {
                Some(p) => p,
                None => return NodeProperties::unordered_single(),
            };
            let provenance = if parent.ordering.sort_order.is_some() {
                OrderingProvenance::Preserved {
                    from_node: name.clone(),
                }
            } else {
                parent.ordering.provenance.clone()
            };
            NodeProperties {
                ordering: Ordering {
                    sort_order: parent.ordering.sort_order.clone(),
                    provenance,
                },
                partitioning: parent.partitioning.clone(),
                ck_set: parent.ck_set.clone(),
                ..NodeProperties::unordered_single()
            }
        }

        PlanNode::Combine {
            name, propagate_ck, ..
        } => {
            // Combine always destroys parent ordering: hash-build/probe
            // (and IEJoin, grace hash) do not preserve driving-input
            // order. Emit `DestroyedByCombine { Proven }` so downstream
            // streaming-agg eligibility / `--explain` / Kiln overlays
            // can chain through and suggest "add a sort step between
            // `{combine}` and `{consumer}`". Resolves Phase Combine
            // §OQ-6 and drill D12.
            //
            // CK lattice rule reads `propagate_ck`. The Combine
            // post-pass `select_combine_strategies` runs AFTER
            // `compute_node_properties`, so `driving_input` is empty
            // here — declaration order (first parent = driver) is the
            // fallback, which matches today's runtime driver-resolution
            // behavior at dispatch.
            use crate::config::pipeline_node::PropagateCkSpec;
            let mut ck_set: BTreeSet<String> = match propagate_ck {
                PropagateCkSpec::Driver => parents
                    .first()
                    .map(|p| p.ck_set.clone())
                    .unwrap_or_default(),
                PropagateCkSpec::All => parents
                    .iter()
                    .flat_map(|p| p.ck_set.iter().cloned())
                    .collect(),
                PropagateCkSpec::Named(names) => {
                    let upstream_union: BTreeSet<String> = parents
                        .iter()
                        .flat_map(|p| p.ck_set.iter().cloned())
                        .collect();
                    names.intersection(&upstream_union).cloned().collect()
                }
            };
            // Synthetic CK (`$ck.aggregate.<name>`) is engine-managed
            // lineage from a relaxed aggregate to its source rows; the
            // user did not declare it, so `propagate_ck` has no
            // semantic meaning over it. Union it from every parent so
            // detect-phase fan-out keeps its bridge across the combine
            // boundary regardless of how user-declared source CK is
            // routed.
            let synthetic: BTreeSet<String> = parents
                .iter()
                .flat_map(|p| {
                    p.ck_set
                        .iter()
                        .filter(|f| f.starts_with("$ck.aggregate."))
                        .cloned()
                })
                .collect();
            ck_set.extend(synthetic);
            // Combine destroys parent ORDERING (hash-build/probe /
            // IEJoin / grace hash do not preserve driver-input order),
            // but PRESERVES the driver's per-file PARTITIONING. Output
            // records derive from driver records — each emitted row
            // carries the driver's `$source.file` lineage, so a
            // downstream Output template with `{source_file}` still
            // fans out per driver-source file.
            //
            // We pick the first `FilePartitioned` parent rather than
            // strictly `parents[0]` because the YAML `input:` map
            // declaration order doesn't reliably make it through to
            // the topo-walk parent ordering. Build inputs that happen
            // to be FilePartitioned (atypical) inherit through here
            // too — that's still correct: the output records carry
            // SOMEONE's `$source.file`, and a downstream fan-out
            // routes accordingly. The combine post-pass
            // `select_combine_strategies` lands after this pass and
            // can refine in future iterations.
            let combine_partitioning = parents
                .iter()
                .find_map(|p| match &p.partitioning.kind {
                    PartitioningKind::FilePartitioned { .. } => Some(Partitioning {
                        kind: p.partitioning.kind.clone(),
                        provenance: PartitioningProvenance::Preserved {
                            from_node: name.clone(),
                        },
                    }),
                    _ => None,
                })
                .unwrap_or_else(single_stream_partitioning);
            NodeProperties {
                ordering: Ordering {
                    sort_order: None,
                    provenance: OrderingProvenance::DestroyedByCombine {
                        at_node: name.clone(),
                        confidence: crate::plan::properties::Confidence::Proven,
                    },
                },
                partitioning: combine_partitioning,
                ck_set,
                ..NodeProperties::unordered_single()
            }
        }

        PlanNode::CorrelationCommit { .. } => {
            // Terminal node — no downstream ever consults its ordering.
            // Inherit upstream parent_partitioning() defensively in case
            // a future planner pass walks this slot.
            NodeProperties {
                ordering: parents
                    .first()
                    .map(|p| p.ordering.clone())
                    .unwrap_or(Ordering {
                        sort_order: None,
                        provenance: OrderingProvenance::NoOrdering,
                    }),
                partitioning: parent_partitioning(),
                ck_set: preserve_parent_ck_set(),
                ..NodeProperties::unordered_single()
            }
        }
    }
}

/// Field-wise equality for `Option<Vec<SortField>>` — `SortField` itself does
/// not derive `PartialEq` and we deliberately avoid adding the derive in this
/// task. Used by the `Merge` arm of `compute_one` for parent-ordering
/// reconciliation.
fn sort_orders_equal(a: &Option<Vec<SortField>>, b: &Option<Vec<SortField>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => {
            x.len() == y.len()
                && x.iter().zip(y.iter()).all(|(p, q)| {
                    p.field == q.field && p.order == q.order && p.null_order == q.null_order
                })
        }
        _ => false,
    }
}
