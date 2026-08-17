//! Plan-time composition Source activation contracts.
//!
//! These values describe which typed, logical resource a body Source will
//! require once runtime activation is implemented. They deliberately contain
//! no physical path, credential selection, secret, opened handle, I/O state,
//! or thread state. One instance is scoped to one bound body call, so two
//! calls to the same composition never share mutable source identity.

use crate::config::{
    ResourceBinding, ResourceCapability, ResourceKind, ResourceLifetime, ResourceOpenerKind,
};
use crate::plan::PlanNodeId;
use crate::plan::composition_body::BodyScopeId;
use crate::resources::ResourceDatasetIdentity;

/// Stable identity of one Source authored in one bound composition body.
///
/// `body_scope` distinguishes separate call sites, while `source_node`
/// distinguishes Sources within that body. Both values are minted in stable
/// declaration order, so compiling identical input twice yields identical
/// identities without relying on an author-chosen name.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct CompiledSourceInstanceId {
    /// Call-site-local body scope.
    pub body_scope: BodyScopeId,
    /// Stable node identity of the authored body Source.
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

/// One plan-time body Source instance and its exact resource dependency.
///
/// The value is fixed, configuration-derived state. It performs no execution
/// work and retains a constant amount of data per authored Source.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledSourceInstance {
    /// Scope-qualified source identity.
    pub id: CompiledSourceInstanceId,
    /// Author-facing source name, retained for diagnostics only.
    pub source_name: String,
    /// Typed, secret-free resource requirement.
    pub resource: CompiledResourceRequirement,
}
