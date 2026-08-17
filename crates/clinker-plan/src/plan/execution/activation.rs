//! Plan-time Source activation inventory and dependency groups.
//!
//! These values describe which typed, logical resources a compiled plan will
//! require once runtime activation is implemented. They deliberately contain
//! no physical path, credential selection, secret, opened handle, I/O state,
//! or thread state. The inventory is sealed only after every top-level and
//! recursively bound body graph has reached its final topology.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;

use clinker_core_types::span::Span;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;

use super::{
    ExecutionPlanDag, PlanEdge, PlanNode, PlanNodeId, SourceActivationFusionKind,
    derive_source_activation_fusions, stable_topological_order,
};
use crate::config::{
    ResourceBinding, ResourceCapability, ResourceKind, ResourceLifetime, ResourceOpenerKind,
};
use crate::credentials::CredentialRequirementName;
use crate::plan::composition_body::{BodyScopeId, BoundBody, CompositionBodies, CompositionBodyId};
use crate::resources::ResourceDatasetIdentity;

/// Scope that owns one compiled external Source.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CompiledSourceScope {
    /// A Source authored directly in the pipeline.
    TopLevel,
    /// A Source authored in one call-site-scoped composition body.
    CompositionBody(BodyScopeId),
}

/// Stable identity of one external Source in one compiled scope.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct CompiledSourceInstanceId {
    /// Top-level or call-site-local body scope.
    pub scope: CompiledSourceScope,
    /// Stable node identity of the authored Source.
    pub source_node: PlanNodeId,
}

/// Secret-free typed resource requirement for one body Source.
///
/// The binding retains the logical catalog identity and complete overlay
/// provenance. The catalog's physical descriptor remains outside
/// [`crate::plan::CompiledPlan`]; only its kind, capabilities, opener family,
/// lifetime, and stable logical dataset identity cross this boundary.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledResourceRequirement {
    /// Authored slot in the enclosing `_compose.resources_schema`.
    pub slot: String,
    /// Winning logical identity plus attempted/winning overlay provenance.
    pub binding: ResourceBinding,
    /// Required descriptor kind.
    pub kind: ResourceKind,
    /// Complete finite capability request for this kind.
    pub required_capabilities: Box<[ResourceCapability]>,
    /// Provider-neutral opener family required later at activation.
    pub opener: ResourceOpenerKind,
    /// Maximum lifetime of the future opened resource.
    pub lifetime: ResourceLifetime,
    /// Stable logical dataset identity, independent of physical location.
    pub dataset_identity: ResourceDatasetIdentity,
}

/// Origin-specific contract for one external Source.
#[derive(Debug, Clone, PartialEq)]
enum CompiledSourceOrigin {
    TopLevel,
    CompositionBody(CompiledResourceRequirement),
}

/// One plan-time external Source instance and its activation requirement.
///
/// The value is fixed, configuration-derived state. It performs no execution
/// work and retains a constant amount of data per authored Source.
#[derive(Clone, PartialEq)]
pub struct CompiledSourceInstance {
    id: CompiledSourceInstanceId,
    source_name: String,
    origin: CompiledSourceOrigin,
    credential_requirement_ids: Box<[CredentialRequirementName]>,
    credential_handle_units: u32,
}

impl fmt::Debug for CompiledSourceInstance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompiledSourceInstance")
            .field("id", &self.id)
            .field("source_name", &self.source_name)
            .field("resource", &self.resource())
            .finish()
    }
}

impl CompiledSourceInstance {
    pub(crate) fn top_level(source_node: PlanNodeId, source_name: String) -> Self {
        Self {
            id: CompiledSourceInstanceId {
                scope: CompiledSourceScope::TopLevel,
                source_node,
            },
            source_name,
            origin: CompiledSourceOrigin::TopLevel,
            credential_requirement_ids: Box::new([]),
            credential_handle_units: 0,
        }
    }

    pub(crate) fn composition_body(
        body_scope: BodyScopeId,
        source_node: PlanNodeId,
        source_name: String,
        resource: CompiledResourceRequirement,
    ) -> Self {
        Self {
            id: CompiledSourceInstanceId {
                scope: CompiledSourceScope::CompositionBody(body_scope),
                source_node,
            },
            source_name,
            origin: CompiledSourceOrigin::CompositionBody(resource),
            credential_requirement_ids: Box::new([]),
            credential_handle_units: 0,
        }
    }

    /// Return the stable scope-qualified Source identity.
    pub fn id(&self) -> CompiledSourceInstanceId {
        self.id
    }

    /// Return the author-facing Source name retained for diagnostics.
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    /// Return the typed logical resource requirement for a body Source.
    ///
    /// Top-level Sources return `None` because the top-level catalog binding
    /// surface has not been designed; they retain their existing direct-file
    /// source contract.
    pub fn resource(&self) -> Option<&CompiledResourceRequirement> {
        match &self.origin {
            CompiledSourceOrigin::TopLevel => None,
            CompiledSourceOrigin::CompositionBody(resource) => Some(resource),
        }
    }

    fn capacity(&self) -> SourceActivationCapacity {
        SourceActivationCapacity::new(1, 1, self.credential_handle_units)
    }
}

/// One root of a compiled scope, with synthetic input ports distinguished from
/// external Sources that require activation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompiledSourceRoot {
    /// External Source represented by exactly one activation group member.
    External {
        /// Stable external Source identity.
        instance: CompiledSourceInstanceId,
    },
    /// Synthetic body input fed by its parent scope without opening a resource.
    InputPort {
        /// Body scope that owns the synthetic root.
        body_scope: BodyScopeId,
        /// Stable identity of the synthetic Source node.
        source_node: PlanNodeId,
        /// Authored composition input-port name.
        port_name: Box<str>,
        /// Parent activation groups that can feed this port.
        dependency_groups: Box<[SourceActivationGroupId]>,
    },
}

/// Dense stable identity of one activation group.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct SourceActivationGroupId(u32);

impl SourceActivationGroupId {
    /// Return the dense zero-based group index.
    pub fn index(self) -> u32 {
        self.0
    }
}

/// Topology proof that determines which Source instances activate together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceActivationGroupKind {
    /// One ordinary Source with no proven shared live-consumer boundary.
    Ordinary,
    /// Multiple exclusive Sources selected live by one unseeded interleave.
    LiveInterleave {
        /// Stable compiled consumer path that proves the grouping.
        consumer_path: Box<[PlanNodeId]>,
    },
    /// One exclusive Source fused into its immediate streaming Transform.
    FusedStreaming {
        /// Stable compiled consumer path that proves the grouping.
        consumer_path: Box<[PlanNodeId]>,
    },
}

/// Checked resource capacity required to activate one group atomically.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct SourceActivationCapacity {
    resource_units: u32,
    opener_units: u32,
    credential_handle_units: u32,
}

impl SourceActivationCapacity {
    /// Construct an exact three-axis capacity requirement.
    pub const fn new(resource_units: u32, opener_units: u32, credential_handle_units: u32) -> Self {
        Self {
            resource_units,
            opener_units,
            credential_handle_units,
        }
    }

    /// Add two requirements, returning `None` rather than wrapping any axis.
    pub fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            resource_units: self.resource_units.checked_add(other.resource_units)?,
            opener_units: self.opener_units.checked_add(other.opener_units)?,
            credential_handle_units: self
                .credential_handle_units
                .checked_add(other.credential_handle_units)?,
        })
    }

    /// Number of independently opened resource sessions.
    pub fn resource_units(self) -> u32 {
        self.resource_units
    }

    /// Number of provider-neutral openers held concurrently.
    pub fn opener_units(self) -> u32 {
        self.opener_units
    }

    /// Number of credential handles held concurrently.
    pub fn credential_handle_units(self) -> u32 {
        self.credential_handle_units
    }
}

/// One deterministic, dependency-aware Source activation group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceActivationGroup {
    id: SourceActivationGroupId,
    kind: SourceActivationGroupKind,
    members: Box<[CompiledSourceInstanceId]>,
    dependencies: Box<[SourceActivationGroupId]>,
    credential_requirement_ids: Box<[CredentialRequirementName]>,
    capacity: SourceActivationCapacity,
}

impl SourceActivationGroup {
    /// Return this group's dense stable identity.
    pub fn id(&self) -> SourceActivationGroupId {
        self.id
    }

    /// Return the topology proof for this group.
    pub fn kind(&self) -> &SourceActivationGroupKind {
        &self.kind
    }

    /// Return external Sources that must activate atomically.
    pub fn members(&self) -> &[CompiledSourceInstanceId] {
        &self.members
    }

    /// Return groups that must activate before this one.
    pub fn dependencies(&self) -> &[SourceActivationGroupId] {
        &self.dependencies
    }

    /// Return the complete deduplicated logical credential requirements.
    pub fn credential_requirement_ids(&self) -> &[CredentialRequirementName] {
        &self.credential_requirement_ids
    }

    /// Return checked aggregate activation capacity.
    pub fn capacity(&self) -> SourceActivationCapacity {
        self.capacity
    }
}

/// Sealed recursive Source activation inventory retained by `CompiledPlan`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SourceActivationPlan {
    sealed: bool,
    instances: Box<[CompiledSourceInstance]>,
    roots: Box<[CompiledSourceRoot]>,
    groups: Box<[SourceActivationGroup]>,
    credential_requirement_ids: Box<[CredentialRequirementName]>,
}

impl SourceActivationPlan {
    /// Whether the inventory covers the finalized top-level and body DAGs.
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// Return every external Source in deterministic recursive order.
    pub fn instances(&self) -> &[CompiledSourceInstance] {
        &self.instances
    }

    /// Return external and synthetic input-port roots.
    pub fn roots(&self) -> &[CompiledSourceRoot] {
        &self.roots
    }

    /// Return activation groups in stable dependency order.
    pub fn groups(&self) -> &[SourceActivationGroup] {
        &self.groups
    }

    /// Return the plan-wide complete logical credential requirement set.
    pub fn credential_requirement_ids(&self) -> &[CredentialRequirementName] {
        &self.credential_requirement_ids
    }

    /// Produce a fixed-cardinality, data-free explain projection.
    pub fn summary(&self) -> SourceActivationSummary {
        SourceActivationSummary {
            sealed: self.sealed,
            instance_count: self.instances.len(),
            input_port_root_count: self
                .roots
                .iter()
                .filter(|root| matches!(root, CompiledSourceRoot::InputPort { .. }))
                .count(),
            group_count: self.groups.len(),
            max_simultaneous_width: self
                .groups
                .iter()
                .map(|group| group.members.len())
                .max()
                .unwrap_or(0),
            credential_requirement_count: self.credential_requirement_ids.len(),
            max_group_resource_units: self
                .groups
                .iter()
                .map(|group| group.capacity.resource_units)
                .max()
                .unwrap_or(0),
            max_group_opener_units: self
                .groups
                .iter()
                .map(|group| group.capacity.opener_units)
                .max()
                .unwrap_or(0),
            max_group_credential_handle_units: self
                .groups
                .iter()
                .map(|group| group.capacity.credential_handle_units)
                .max()
                .unwrap_or(0),
        }
    }
}

/// Fixed-cardinality explain projection for Source activation planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct SourceActivationSummary {
    /// Whether recursive inventory sealing completed.
    pub sealed: bool,
    /// External Source count across every scope.
    pub instance_count: usize,
    /// Synthetic body input-port root count.
    pub input_port_root_count: usize,
    /// Activation group count.
    pub group_count: usize,
    /// Largest atomic Source group.
    pub max_simultaneous_width: usize,
    /// Distinct logical credential requirement count.
    pub credential_requirement_count: usize,
    /// Largest per-group resource-session requirement.
    pub max_group_resource_units: u32,
    /// Largest per-group opener requirement.
    pub max_group_opener_units: u32,
    /// Largest per-group credential-handle requirement.
    pub max_group_credential_handle_units: u32,
}

/// Failure to seal the finalized recursive activation inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceActivationPlanError {
    pub span: Span,
    pub message: String,
}

#[derive(Debug)]
struct DraftGroup {
    kind: SourceActivationGroupKind,
    members: Vec<CompiledSourceInstanceId>,
    dependencies: BTreeSet<usize>,
    credential_requirement_ids: BTreeSet<CredentialRequirementName>,
    capacity: SourceActivationCapacity,
    rank: usize,
    span: Span,
}

#[derive(Debug)]
enum DraftRoot {
    External(CompiledSourceInstanceId),
    InputPort {
        body_scope: BodyScopeId,
        source_node: PlanNodeId,
        port_name: Box<str>,
        dependency_groups: BTreeSet<usize>,
    },
}

#[derive(Debug, Default)]
struct ActivationBuilder {
    instances: Vec<CompiledSourceInstance>,
    roots: Vec<DraftRoot>,
    groups: Vec<DraftGroup>,
    visited_bodies: BTreeSet<CompositionBodyId>,
}

impl ActivationBuilder {
    fn compile_scope(
        &mut self,
        plan: &ExecutionPlanDag,
        body: Option<(CompositionBodyId, &BoundBody)>,
        bodies: &CompositionBodies,
        input_dependencies: &BTreeMap<String, BTreeSet<usize>>,
    ) -> Result<Vec<usize>, SourceActivationPlanError> {
        let supplied_instances = match body {
            Some((body_id, bound)) => {
                if !self.visited_bodies.insert(body_id) {
                    return Err(SourceActivationPlanError {
                        span: Span::SYNTHETIC,
                        message: format!(
                            "composition body {} is reachable from more than one compiled call site",
                            body_id.0
                        ),
                    });
                }
                bound.source_instances.clone()
            }
            None => stable_topological_order(&plan.graph)
                .map_err(|idx| self.topology_error(plan, idx))?
                .into_iter()
                .filter_map(|idx| match &plan.graph[idx] {
                    PlanNode::Source { id, name, .. } => {
                        Some(CompiledSourceInstance::top_level(*id, name.clone()))
                    }
                    _ => None,
                })
                .collect(),
        };

        let base_dependencies: BTreeSet<_> = input_dependencies
            .values()
            .flat_map(|dependencies| dependencies.iter().copied())
            .collect();
        let (source_groups, scope_groups) =
            self.add_scope_groups(plan, supplied_instances, &base_dependencies)?;

        if body.is_none() {
            self.apply_top_level_source_tiers(plan, &source_groups)?;
        }

        let mut port_groups_by_node = HashMap::new();
        if let Some((_, bound)) = body {
            for (port_name, node_idx) in &bound.port_name_to_node_idx {
                let node = &bound.graph[*node_idx];
                let dependencies = input_dependencies
                    .get(port_name)
                    .cloned()
                    .unwrap_or_default();
                port_groups_by_node.insert(node.id(), dependencies.clone());
                self.roots.push(DraftRoot::InputPort {
                    body_scope: bound.body_scope,
                    source_node: node.id(),
                    port_name: port_name.clone().into_boxed_str(),
                    dependency_groups: dependencies,
                });
            }
            self.reject_unclassified_body_sources(bound, &source_groups, &port_groups_by_node)?;
        }

        let stable_topo =
            stable_topological_order(&plan.graph).map_err(|idx| self.topology_error(plan, idx))?;
        let mut composition_groups: HashMap<PlanNodeId, Vec<usize>> = HashMap::new();
        let mut recursive_groups = scope_groups.clone();
        for idx in stable_topo {
            let PlanNode::Composition {
                id,
                body: child_body_id,
                ..
            } = plan.graph[idx]
            else {
                continue;
            };
            let child_body =
                bodies
                    .get(&child_body_id)
                    .ok_or_else(|| SourceActivationPlanError {
                        span: plan.graph[idx].span(),
                        message: format!(
                            "composition {:?} references undeclared bound body {}",
                            plan.graph[idx].name(),
                            child_body_id.0
                        ),
                    })?;
            let mut child_inputs: BTreeMap<String, BTreeSet<usize>> = BTreeMap::new();
            for edge in plan
                .graph
                .edges_directed(idx, petgraph::Direction::Incoming)
            {
                let Some(port) = edge.weight().port.as_ref() else {
                    return Err(SourceActivationPlanError {
                        span: plan.graph[idx].span(),
                        message: format!(
                            "composition {:?} has an undeclared untagged input dependency",
                            plan.graph[idx].name()
                        ),
                    });
                };
                let dependencies = collect_upstream_groups(
                    &plan.graph,
                    edge.source(),
                    &source_groups,
                    &port_groups_by_node,
                    &composition_groups,
                );
                child_inputs
                    .entry(port.clone())
                    .or_default()
                    .extend(dependencies);
            }
            for port_name in child_inputs.keys() {
                if !child_body.input_port_rows.contains_key(port_name) {
                    return Err(SourceActivationPlanError {
                        span: plan.graph[idx].span(),
                        message: format!(
                            "composition {:?} depends on undeclared input port {port_name:?}",
                            plan.graph[idx].name()
                        ),
                    });
                }
            }

            let child_plan = ExecutionPlanDag::from_body(child_body);
            let child_groups = self.compile_scope(
                &child_plan,
                Some((child_body_id, child_body)),
                bodies,
                &child_inputs,
            )?;
            composition_groups.insert(id, child_groups.clone());
            recursive_groups.extend(child_groups);
        }

        Ok(recursive_groups)
    }

    fn add_scope_groups(
        &mut self,
        plan: &ExecutionPlanDag,
        supplied_instances: Vec<CompiledSourceInstance>,
        base_dependencies: &BTreeSet<usize>,
    ) -> Result<(HashMap<PlanNodeId, usize>, Vec<usize>), SourceActivationPlanError> {
        let supplied_by_node: HashMap<_, _> = supplied_instances
            .into_iter()
            .map(|instance| (instance.id.source_node, instance))
            .collect();
        let external_source_ids: HashSet<_> = supplied_by_node.keys().copied().collect();
        let fusions = derive_source_activation_fusions(plan, &external_source_ids)
            .map_err(|idx| self.topology_error(plan, idx))?;
        let fusion_by_source: HashMap<_, _> = fusions
            .iter()
            .enumerate()
            .flat_map(|(fusion_idx, fusion)| {
                fusion
                    .sources
                    .iter()
                    .copied()
                    .map(move |source| (source, fusion_idx))
            })
            .collect();
        let stable_topo =
            stable_topological_order(&plan.graph).map_err(|idx| self.topology_error(plan, idx))?;
        let mut source_groups = HashMap::new();
        let mut scope_groups = Vec::new();
        let mut emitted_fusions = HashSet::new();

        for idx in stable_topo {
            let node_id = plan.graph[idx].id();
            if !external_source_ids.contains(&node_id) {
                continue;
            }
            let (kind, source_ids) = if let Some(&fusion_idx) = fusion_by_source.get(&node_id) {
                if !emitted_fusions.insert(fusion_idx) {
                    continue;
                }
                let fusion = &fusions[fusion_idx];
                let kind = match fusion.kind {
                    SourceActivationFusionKind::LiveInterleave => {
                        SourceActivationGroupKind::LiveInterleave {
                            consumer_path: fusion.consumer_path.clone(),
                        }
                    }
                    SourceActivationFusionKind::FusedStreaming => {
                        SourceActivationGroupKind::FusedStreaming {
                            consumer_path: fusion.consumer_path.clone(),
                        }
                    }
                };
                (kind, fusion.sources.to_vec())
            } else {
                (SourceActivationGroupKind::Ordinary, vec![node_id])
            };

            let mut members = Vec::with_capacity(source_ids.len());
            let mut credentials = BTreeSet::new();
            let mut capacity = SourceActivationCapacity::default();
            let rank = self.instances.len();
            for source_id in &source_ids {
                let instance =
                    supplied_by_node
                        .get(source_id)
                        .ok_or_else(|| SourceActivationPlanError {
                            span: plan.graph[idx].span(),
                            message: format!(
                                "activation group references undeclared Source identity {:?}",
                                source_id
                            ),
                        })?;
                capacity = capacity.checked_add(instance.capacity()).ok_or_else(|| {
                    SourceActivationPlanError {
                        span: plan.graph[idx].span(),
                        message: "Source activation capacity exceeds the supported u32 bound"
                            .to_string(),
                    }
                })?;
                credentials.extend(instance.credential_requirement_ids.iter().cloned());
                members.push(instance.id());
                self.instances.push(instance.clone());
                self.roots.push(DraftRoot::External(instance.id()));
            }
            let group_idx = self.groups.len();
            self.groups.push(DraftGroup {
                kind,
                members,
                dependencies: base_dependencies.clone(),
                credential_requirement_ids: credentials,
                capacity,
                rank,
                span: plan.graph[idx].span(),
            });
            for source_id in source_ids {
                source_groups.insert(source_id, group_idx);
            }
            scope_groups.push(group_idx);
        }

        if source_groups.len() != supplied_by_node.len() {
            let missing = supplied_by_node
                .keys()
                .find(|source_id| !source_groups.contains_key(source_id))
                .copied();
            return Err(SourceActivationPlanError {
                span: Span::SYNTHETIC,
                message: format!(
                    "compiled Source activation inventory omitted Source identity {missing:?}"
                ),
            });
        }
        Ok((source_groups, scope_groups))
    }

    fn reject_unclassified_body_sources(
        &self,
        body: &BoundBody,
        external_groups: &HashMap<PlanNodeId, usize>,
        port_groups: &HashMap<PlanNodeId, BTreeSet<usize>>,
    ) -> Result<(), SourceActivationPlanError> {
        for node in body.graph.node_weights() {
            if matches!(node, PlanNode::Source { .. })
                && !external_groups.contains_key(&node.id())
                && !port_groups.contains_key(&node.id())
            {
                return Err(SourceActivationPlanError {
                    span: node.span(),
                    message: format!(
                        "body Source {:?} is neither an external resource nor an input-port root",
                        node.name()
                    ),
                });
            }
        }
        Ok(())
    }

    fn apply_top_level_source_tiers(
        &mut self,
        plan: &ExecutionPlanDag,
        source_groups: &HashMap<PlanNodeId, usize>,
    ) -> Result<(), SourceActivationPlanError> {
        let source_id_by_name: HashMap<_, _> = plan
            .graph
            .node_weights()
            .filter_map(|node| match node {
                PlanNode::Source { name, id, .. } => Some((name.as_str(), *id)),
                _ => None,
            })
            .collect();
        let mut previous_tier = BTreeSet::new();
        for tier in &plan.source_dag {
            let current: BTreeSet<_> = tier
                .sources
                .iter()
                .filter_map(|name| source_id_by_name.get(name.as_str()))
                .filter_map(|source_id| source_groups.get(source_id))
                .copied()
                .collect();
            for &group in &current {
                if previous_tier.contains(&group) {
                    return Err(SourceActivationPlanError {
                        span: self.groups[group].span,
                        message: "one simultaneous Source group spans dependent source tiers"
                            .to_string(),
                    });
                }
                self.groups[group]
                    .dependencies
                    .extend(previous_tier.iter().copied());
            }
            previous_tier = current;
        }
        Ok(())
    }

    fn topology_error(&self, plan: &ExecutionPlanDag, idx: NodeIndex) -> SourceActivationPlanError {
        SourceActivationPlanError {
            span: plan
                .graph
                .node_weight(idx)
                .map(PlanNode::span)
                .unwrap_or(Span::SYNTHETIC),
            message: "cycle detected while sealing Source activation topology".to_string(),
        }
    }

    fn finish(self) -> Result<SourceActivationPlan, SourceActivationPlanError> {
        let mut indegrees: Vec<_> = self
            .groups
            .iter()
            .map(|group| group.dependencies.len())
            .collect();
        let mut dependents = vec![Vec::new(); self.groups.len()];
        for (group_idx, group) in self.groups.iter().enumerate() {
            for &dependency in &group.dependencies {
                if dependency == group_idx {
                    return Err(SourceActivationPlanError {
                        span: group.span,
                        message: "Source activation group depends on itself".to_string(),
                    });
                }
                dependents[dependency].push(group_idx);
            }
        }
        let mut ready: BTreeSet<_> = self
            .groups
            .iter()
            .enumerate()
            .filter_map(|(idx, group)| (indegrees[idx] == 0).then_some((group.rank, idx)))
            .collect();
        let mut order = Vec::with_capacity(self.groups.len());
        while let Some(&(rank, idx)) = ready.first() {
            ready.remove(&(rank, idx));
            order.push(idx);
            for &dependent in &dependents[idx] {
                indegrees[dependent] -= 1;
                if indegrees[dependent] == 0 {
                    ready.insert((self.groups[dependent].rank, dependent));
                }
            }
        }
        if order.len() != self.groups.len() {
            let idx = indegrees
                .iter()
                .position(|&indegree| indegree > 0)
                .unwrap_or(0);
            return Err(SourceActivationPlanError {
                span: self.groups[idx].span,
                message: "cycle detected between Source activation groups".to_string(),
            });
        }

        let mut remap = vec![SourceActivationGroupId(0); self.groups.len()];
        for (new_idx, &old_idx) in order.iter().enumerate() {
            let dense = u32::try_from(new_idx).map_err(|_| SourceActivationPlanError {
                span: self.groups[old_idx].span,
                message: "Source activation group count exceeds the supported u32 bound"
                    .to_string(),
            })?;
            remap[old_idx] = SourceActivationGroupId(dense);
        }
        let groups: Vec<_> = order
            .into_iter()
            .map(|old_idx| {
                let group = &self.groups[old_idx];
                let mut dependencies: Vec<_> = group
                    .dependencies
                    .iter()
                    .map(|&dependency| remap[dependency])
                    .collect();
                dependencies.sort_unstable();
                SourceActivationGroup {
                    id: remap[old_idx],
                    kind: group.kind.clone(),
                    members: group.members.clone().into_boxed_slice(),
                    dependencies: dependencies.into_boxed_slice(),
                    credential_requirement_ids: group
                        .credential_requirement_ids
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                    capacity: group.capacity,
                }
            })
            .collect();
        let roots = self
            .roots
            .into_iter()
            .map(|root| match root {
                DraftRoot::External(instance) => CompiledSourceRoot::External { instance },
                DraftRoot::InputPort {
                    body_scope,
                    source_node,
                    port_name,
                    dependency_groups,
                } => CompiledSourceRoot::InputPort {
                    body_scope,
                    source_node,
                    port_name,
                    dependency_groups: dependency_groups
                        .into_iter()
                        .map(|dependency| remap[dependency])
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                },
            })
            .collect::<Vec<_>>();
        let credential_requirement_ids: BTreeSet<_> = groups
            .iter()
            .flat_map(|group| group.credential_requirement_ids.iter().cloned())
            .collect();
        Ok(SourceActivationPlan {
            sealed: true,
            instances: self.instances.into_boxed_slice(),
            roots: roots.into_boxed_slice(),
            groups: groups.into_boxed_slice(),
            credential_requirement_ids: credential_requirement_ids
                .into_iter()
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }
}

fn collect_upstream_groups(
    graph: &petgraph::graph::DiGraph<PlanNode, PlanEdge>,
    start: NodeIndex,
    source_groups: &HashMap<PlanNodeId, usize>,
    port_groups: &HashMap<PlanNodeId, BTreeSet<usize>>,
    composition_groups: &HashMap<PlanNodeId, Vec<usize>>,
) -> BTreeSet<usize> {
    let mut pending = vec![start];
    let mut visited = HashSet::new();
    let mut groups = BTreeSet::new();
    while let Some(idx) = pending.pop() {
        if !visited.insert(idx) {
            continue;
        }
        let node_id = graph[idx].id();
        if let Some(&group) = source_groups.get(&node_id) {
            groups.insert(group);
            continue;
        }
        if let Some(dependencies) = port_groups.get(&node_id) {
            groups.extend(dependencies.iter().copied());
            continue;
        }
        if let Some(dependencies) = composition_groups.get(&node_id) {
            groups.extend(dependencies.iter().copied());
            continue;
        }
        pending.extend(graph.neighbors_directed(idx, petgraph::Direction::Incoming));
    }
    groups
}

pub(crate) fn compile_source_activation_plan(
    plan: &ExecutionPlanDag,
    bodies: &CompositionBodies,
) -> Result<SourceActivationPlan, SourceActivationPlanError> {
    let mut builder = ActivationBuilder::default();
    builder.compile_scope(plan, None, bodies, &BTreeMap::new())?;
    if builder.visited_bodies.len() != bodies.len() {
        let missing = bodies
            .keys()
            .find(|body_id| !builder.visited_bodies.contains(body_id))
            .copied()
            .unwrap_or(CompositionBodyId::SENTINEL);
        return Err(SourceActivationPlanError {
            span: Span::SYNTHETIC,
            message: format!(
                "bound composition body {} is absent from the recursive activation inventory",
                missing.0
            ),
        });
    }
    builder.finish()
}
