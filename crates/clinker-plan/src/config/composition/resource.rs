//! Secret-free composition resource contracts.
//!
//! A composition call binds a declared slot to one logical workspace-catalog
//! identity. The binding retains the complete overlay provenance chain, but it
//! cannot represent credentials, opened handles, or inline descriptor values.

use clinker_core_types::span::Span;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{LayerKind, ResolvedValue, ResourceKind};
use crate::resources::{LogicalResourceId, ResourceError};
use crate::yaml::Spanned;

/// A typed, secret-free resource descriptor payload.
///
/// Descriptors are admitted by the workspace catalog at plan time. They carry
/// no credential fields and do not open their target.
#[derive(Debug, Clone)]
pub enum Resource {
    /// A file resource whose path is validated against the workspace root.
    File {
        /// Authored workspace-relative path.
        path: std::path::PathBuf,
        /// Source span of the `path:` value.
        span: Span,
    },
}

impl Resource {
    /// Descriptor kind.
    pub fn kind(&self) -> ResourceKind {
        match self {
            Self::File { .. } => ResourceKind::File,
        }
    }
}

/// A closed capability required or provided by a resource kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResourceCapability {
    /// The resource can be read as an input dataset.
    Read,
    /// The resource can be written as an output dataset.
    Write,
}

/// How long an opener produced for a resource may remain live.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceLifetime {
    /// The opener is scoped to one run and must not enter a cached plan.
    Run,
}

/// Runtime opener family required by a typed descriptor.
///
/// This is planning vocabulary only. It does not open the resource and carries
/// no live handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceOpenerKind {
    /// A bounded file opener supplied later by runtime activation.
    File,
}

impl ResourceKind {
    /// Capabilities every binding for this slot kind must provide.
    pub fn required_capabilities(self) -> &'static [ResourceCapability] {
        match self {
            Self::File => &[ResourceCapability::Read],
        }
    }

    /// Required opener family for this slot kind.
    pub fn opener_kind(self) -> ResourceOpenerKind {
        match self {
            Self::File => ResourceOpenerKind::File,
        }
    }

    /// Required opener lifetime for this slot kind.
    pub fn lifetime(self) -> ResourceLifetime {
        match self {
            Self::File => ResourceLifetime::Run,
        }
    }

    /// Stable author-facing kind label.
    pub fn label(self) -> &'static str {
        match self {
            Self::File => "file",
        }
    }
}

/// One secret-free call-site binding and its complete layer provenance.
///
/// The YAML form is deliberately only a scalar logical catalog identity:
/// `resources: { orders: shared.orders }`. Object payloads are rejected, so a
/// call site cannot smuggle a path, credential profile, secret, or live-handle
/// selector into a compiled plan.
#[derive(Clone, PartialEq)]
pub struct ResourceBinding {
    resolved: ResolvedValue<LogicalResourceId>,
}

impl ResourceBinding {
    /// Create a binding contributed by one semantic layer.
    pub fn from_layer(
        logical_id: LogicalResourceId,
        layer: LayerKind,
        span: Span,
        fixed: bool,
    ) -> Self {
        let resolved = if fixed {
            ResolvedValue::new_fixed(logical_id, layer, span)
        } else {
            ResolvedValue::new(logical_id, layer, span)
        };
        Self { resolved }
    }

    /// Winning logical catalog identity.
    pub fn logical_id(&self) -> &LogicalResourceId {
        &self.resolved.value
    }

    /// Complete attempted/winner provenance for explain tooling.
    pub fn provenance(&self) -> &ResolvedValue<LogicalResourceId> {
        &self.resolved
    }

    /// Apply one overlay candidate using the shared fixed-layer semantics.
    pub fn apply_layer(
        &mut self,
        logical_id: LogicalResourceId,
        layer: LayerKind,
        span: Span,
        fixed: bool,
    ) {
        if fixed {
            self.resolved.apply_layer_fixed(logical_id, layer, span);
        } else {
            self.resolved.apply_layer(logical_id, layer, span);
        }
    }
}

impl std::fmt::Debug for ResourceBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResourceBinding")
            .field("logical_id", self.logical_id())
            .field("provenance", &self.resolved.provenance)
            .finish()
    }
}

impl<'de> Deserialize<'de> for ResourceBinding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let authored = Spanned::<String>::deserialize(deserializer)?;
        let logical_id = LogicalResourceId::parse(&authored.value)
            .map_err(|error: ResourceError| serde::de::Error::custom(error.to_string()))?;
        let line = authored.referenced.line() as u32;
        let span = if line == 0 {
            Span::SYNTHETIC
        } else {
            Span::line_only(line)
        };
        Ok(Self::from_layer(
            logical_id,
            LayerKind::PipelineDefault,
            span,
            false,
        ))
    }
}

impl Serialize for ResourceBinding {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.logical_id().as_str().serialize(serializer)
    }
}

/// Whether an authored resource-overlay slot is an attempted credential
/// selector rather than a declared resource slot.
///
/// Credential profiles are a separate explicit preflight choice. Keeping the
/// reserved spellings here gives every overlay layer the same fail-closed
/// check without accepting a second credential-selection surface.
pub fn is_reserved_credential_selector(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().replace('-', "_").as_str(),
        "credential" | "credentials" | "credential_profile" | "profile" | "secret" | "token"
    )
}
