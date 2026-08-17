//! Secret-free credential requirements retained by planning.
//!
//! This module describes what a runtime consumer needs without representing a
//! credential profile, secret value, or opened handle. Provider selection and
//! lease acquisition belong to the application edge.

use std::fmt;
use std::num::NonZeroU32;

use serde::{Deserialize, Deserializer, Serialize};

/// A stable logical name for one credential requirement.
///
/// Names are metadata, never secret values. They use the same conservative
/// dot-separated identifier shape as workspace resources so they remain safe
/// in plans, fingerprints, diagnostics, and lineage metadata.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct CredentialRequirementName(Box<str>);

impl CredentialRequirementName {
    /// Parse a logical requirement name without retaining rejected input.
    pub fn parse(value: &str) -> Result<Self, CredentialRequirementError> {
        if !is_logical_name(value) {
            return Err(CredentialRequirementError::InvalidRequirementName);
        }
        Ok(Self(value.into()))
    }

    /// Return the validated logical name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CredentialRequirementName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CredentialRequirementName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Box::<str>::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

/// A stable logical identifier for a credential-provider implementation kind.
///
/// This is a provider kind such as a request signer, not a deployment profile,
/// endpoint, account, or secret-store location.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct CredentialProviderKind(Box<str>);

impl CredentialProviderKind {
    /// Parse a provider kind without retaining rejected input.
    pub fn parse(value: &str) -> Result<Self, CredentialRequirementError> {
        if !is_logical_name(value) {
            return Err(CredentialRequirementError::InvalidProviderKind);
        }
        Ok(Self(value.into()))
    }

    /// Return the validated provider kind.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CredentialProviderKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CredentialProviderKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Box::<str>::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

/// Provider-neutral operations a resolved credential must support.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CredentialCapability {
    /// Authenticate one already-admitted outbound request.
    AuthenticateRequest,
    /// Establish an authenticated session owned by a runtime consumer.
    OpenSession,
}

/// Maximum scope for which one resolved credential lease may stay live.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CredentialLifetime {
    /// The lease is scoped to a single request or equivalent operation.
    Request,
    /// The lease may stay live for one finite run.
    Run,
}

/// Whether a consumer requires renewal support from its provider.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CredentialRenewal {
    /// The consumer does not require renewal.
    NotRequired,
    /// The provider must support renewal for the admitted lease.
    Required,
}

/// Whether a consumer requires revocation support from its provider.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CredentialRevocation {
    /// The consumer does not require revocation.
    NotRequired,
    /// The provider must support revocation for the admitted lease.
    Required,
}

/// Fixed capacity charged for one resolved credential handle.
///
/// Units are provider-neutral admission units, not bytes and not a count
/// derived from a secret. Runtime registries use them to bound live handles.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct CredentialHandleUnits(NonZeroU32);

impl CredentialHandleUnits {
    /// Construct a non-zero capacity charge.
    pub const fn new(units: NonZeroU32) -> Self {
        Self(units)
    }

    /// Return the non-zero unit count.
    pub const fn get(self) -> NonZeroU32 {
        self.0
    }
}

/// Complete secret-free requirements for one logical credential.
///
/// Capabilities are sorted and deduplicated at construction, making the value
/// deterministic wherever it participates in serialized plan identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct CredentialRequirement {
    name: CredentialRequirementName,
    provider_kind: CredentialProviderKind,
    capabilities: Vec<CredentialCapability>,
    lifetime: CredentialLifetime,
    renewal: CredentialRenewal,
    revocation: CredentialRevocation,
    handle_units: CredentialHandleUnits,
}

impl CredentialRequirement {
    /// Construct a complete requirement after validating its capability set.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: CredentialRequirementName,
        provider_kind: CredentialProviderKind,
        mut capabilities: Vec<CredentialCapability>,
        lifetime: CredentialLifetime,
        renewal: CredentialRenewal,
        revocation: CredentialRevocation,
        handle_units: CredentialHandleUnits,
    ) -> Result<Self, CredentialRequirementError> {
        capabilities.sort_unstable();
        capabilities.dedup();
        if capabilities.is_empty() {
            return Err(CredentialRequirementError::EmptyCapabilities);
        }
        Ok(Self {
            name,
            provider_kind,
            capabilities,
            lifetime,
            renewal,
            revocation,
            handle_units,
        })
    }

    /// Logical requirement name retained in the plan.
    pub fn name(&self) -> &CredentialRequirementName {
        &self.name
    }

    /// Required provider implementation kind.
    pub fn provider_kind(&self) -> &CredentialProviderKind {
        &self.provider_kind
    }

    /// Canonical non-empty capability set.
    pub fn capabilities(&self) -> &[CredentialCapability] {
        &self.capabilities
    }

    /// Maximum admitted lease lifetime.
    pub const fn lifetime(&self) -> CredentialLifetime {
        self.lifetime
    }

    /// Renewal support requirement.
    pub const fn renewal(&self) -> CredentialRenewal {
        self.renewal
    }

    /// Revocation support requirement.
    pub const fn revocation(&self) -> CredentialRevocation {
        self.revocation
    }

    /// Capacity units charged by a live handle.
    pub const fn handle_units(&self) -> CredentialHandleUnits {
        self.handle_units
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialRequirementWire {
    name: CredentialRequirementName,
    provider_kind: CredentialProviderKind,
    capabilities: Vec<CredentialCapability>,
    lifetime: CredentialLifetime,
    renewal: CredentialRenewal,
    revocation: CredentialRevocation,
    handle_units: CredentialHandleUnits,
}

impl<'de> Deserialize<'de> for CredentialRequirement {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = CredentialRequirementWire::deserialize(deserializer)?;
        Self::new(
            wire.name,
            wire.provider_kind,
            wire.capabilities,
            wire.lifetime,
            wire.renewal,
            wire.revocation,
            wire.handle_units,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Validation failures for secret-free credential requirement metadata.
///
/// Variants deliberately retain no rejected string or arbitrary provider
/// payload, so both `Display` and `Debug` are safe at diagnostic boundaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialRequirementError {
    /// The logical requirement name is empty or malformed.
    InvalidRequirementName,
    /// The provider kind is empty or malformed.
    InvalidProviderKind,
    /// A requirement named no usable consumer operation.
    EmptyCapabilities,
}

impl fmt::Display for CredentialRequirementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequirementName => formatter.write_str(
                "credential requirement name is invalid; use a dot-separated logical name such as `service.api`",
            ),
            Self::InvalidProviderKind => formatter.write_str(
                "credential provider kind is invalid; use a dot-separated logical name such as `request-signer`",
            ),
            Self::EmptyCapabilities => formatter.write_str(
                "credential requirement must name at least one provider-neutral capability",
            ),
        }
    }
}

impl std::error::Error for CredentialRequirementError {}

fn is_logical_name(value: &str) -> bool {
    !value.is_empty()
        && value.split('.').all(|part| {
            !part.is_empty()
                && part.chars().all(|character| {
                    character.is_ascii_alphanumeric() || matches!(character, '_' | '-')
                })
        })
}
