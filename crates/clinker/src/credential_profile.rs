//! Explicit run-local credential profile resolution.
//!
//! Profiles and providers are supplied by the application edge. Resolution
//! accepts one already-validated, secret-free requirement and returns one
//! opaque lease; it never searches a plan, environment, channel, or group.

use std::fmt;
use std::marker::PhantomData;

use clinker_plan::credentials::{
    CredentialCapability, CredentialHandleUnits, CredentialLifetime, CredentialProviderKind,
    CredentialRenewal, CredentialRequirement, CredentialRequirementName, CredentialRevocation,
};

/// An explicitly selected deployment credential profile name.
///
/// This type has no default. A credential-bearing run must provide one value
/// independently of its channel, group, or environment selection.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CredentialProfileName(Box<str>);

impl CredentialProfileName {
    /// Parse a profile name without retaining rejected input.
    pub fn parse(value: &str) -> Result<Self, CredentialProfileNameError> {
        if !is_profile_name(value) {
            return Err(CredentialProfileNameError);
        }
        Ok(Self(value.into()))
    }

    /// Return the validated explicit profile name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CredentialProfileName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A malformed explicit profile name.
///
/// Rejected text is not retained, keeping both error representations safe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CredentialProfileNameError;

impl fmt::Display for CredentialProfileNameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "credential profile name is invalid; choose an explicitly configured logical name",
        )
    }
}

impl std::error::Error for CredentialProfileNameError {}

/// Opaque provider-owned state that remains live for a resolved credential.
///
/// The marker deliberately exposes no secret accessor. Dropping the enclosing
/// [`LeasedCredentialHandle`] closes the lease on every ordinary exit path.
pub trait CredentialLease: Send + Sync {}

/// A closed, sanitized failure returned by a credential provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialProviderFailure {
    /// The configured provider could not acquire a lease at this time.
    Unavailable,
    /// The provider refused the logical requirement.
    Refused,
}

impl fmt::Display for CredentialProviderFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("credential provider could not resolve the requirement")
    }
}

impl std::error::Error for CredentialProviderFailure {}

/// Provider-neutral interface used by explicit profile resolution.
///
/// Compatibility metadata is inspected before `resolve` is called. Provider
/// implementations return only an opaque owned lease and a closed failure;
/// arbitrary secret-store errors cannot cross this boundary.
pub trait CredentialProvider {
    /// Stable implementation kind supplied by this provider.
    fn kind(&self) -> &CredentialProviderKind;

    /// Provider-neutral operations supplied by this provider.
    fn capabilities(&self) -> &[CredentialCapability];

    /// Lease lifetimes supplied by this provider.
    fn lifetimes(&self) -> &[CredentialLifetime];

    /// Whether acquired leases can be renewed when required.
    fn supports_renewal(&self) -> bool;

    /// Whether acquired leases can be revoked when required.
    fn supports_revocation(&self) -> bool;

    /// Maximum handle-capacity units admitted for one lease.
    fn handle_capacity(&self) -> CredentialHandleUnits;

    /// Resolve one validated logical requirement to an opaque owned lease.
    fn resolve(
        &self,
        requirement: &CredentialRequirement,
    ) -> Result<Box<dyn CredentialLease>, CredentialProviderFailure>;
}

/// One explicitly named profile backed by borrowed provider registrations.
///
/// The borrowed slice is supplied by the run boundary; this low-level value
/// does not allocate or retain an input-sized registry.
pub struct CredentialProfile<'run> {
    name: CredentialProfileName,
    providers: &'run [&'run dyn CredentialProvider],
}

impl<'run> CredentialProfile<'run> {
    /// Construct a named profile from a borrowed provider slice.
    pub const fn new(
        name: CredentialProfileName,
        providers: &'run [&'run dyn CredentialProvider],
    ) -> Self {
        Self { name, providers }
    }

    /// Explicit profile name.
    pub fn name(&self) -> &CredentialProfileName {
        &self.name
    }

    fn matching_provider(
        &self,
        kind: &CredentialProviderKind,
    ) -> Result<&'run dyn CredentialProvider, CredentialResolutionError> {
        let mut matches = self
            .providers
            .iter()
            .copied()
            .filter(|provider| provider.kind() == kind);
        let provider = matches.next().ok_or(CredentialResolutionError::new(
            CredentialResolutionErrorKind::UnknownProvider,
        ))?;
        if matches.next().is_some() {
            return Err(CredentialResolutionError::new(
                CredentialResolutionErrorKind::AmbiguousProvider,
            ));
        }
        Ok(provider)
    }
}

/// One run-local credential lease with fully redacted diagnostics.
///
/// This type intentionally implements neither `Serialize` nor `Clone`. The
/// private provider-owned guard is released when the handle is dropped, while
/// the public metadata remains limited to secret-free plan identifiers.
pub struct LeasedCredentialHandle<'run> {
    requirement_name: CredentialRequirementName,
    provider_kind: CredentialProviderKind,
    handle_units: CredentialHandleUnits,
    _lease: Box<dyn CredentialLease>,
    _run: PhantomData<&'run CredentialProfile<'run>>,
}

impl LeasedCredentialHandle<'_> {
    /// Secret-free logical requirement satisfied by this handle.
    pub fn requirement_name(&self) -> &CredentialRequirementName {
        &self.requirement_name
    }

    /// Provider kind that owns this handle.
    pub fn provider_kind(&self) -> &CredentialProviderKind {
        &self.provider_kind
    }

    /// Admission units charged while this handle stays live.
    pub const fn handle_units(&self) -> CredentialHandleUnits {
        self.handle_units
    }
}

impl fmt::Debug for LeasedCredentialHandle<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeasedCredentialHandle { credential: <redacted> }")
    }
}

/// Stable category for an explicit profile-resolution failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialResolutionErrorKind {
    /// No supplied profile had the selected name.
    UnknownProfile,
    /// More than one supplied profile had the selected name.
    AmbiguousProfile,
    /// The selected profile had no provider of the required kind.
    UnknownProvider,
    /// The selected profile had duplicate providers of the required kind.
    AmbiguousProvider,
    /// The provider omitted at least one required operation.
    UnsupportedCapability,
    /// The provider cannot supply the required lease lifetime.
    UnsupportedLifetime,
    /// The provider cannot renew a lease that requires renewal.
    UnsupportedRenewal,
    /// The provider cannot revoke a lease that requires revocation.
    UnsupportedRevocation,
    /// The provider cannot admit the required handle-capacity units.
    InsufficientHandleCapacity,
    /// The provider was unavailable while acquiring the lease.
    ProviderUnavailable,
    /// The provider refused the logical requirement.
    ProviderRefused,
}

/// A sanitized explicit profile-resolution failure.
///
/// The error stores only a closed category. It cannot echo a profile name,
/// logical reference, provider payload, or secret through `Display` or `Debug`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CredentialResolutionError {
    kind: CredentialResolutionErrorKind,
}

impl CredentialResolutionError {
    const fn new(kind: CredentialResolutionErrorKind) -> Self {
        Self { kind }
    }

    /// Stable failure category for tests and structured diagnostics.
    pub const fn kind(self) -> CredentialResolutionErrorKind {
        self.kind
    }
}

impl fmt::Display for CredentialResolutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self.kind {
            CredentialResolutionErrorKind::UnknownProfile => {
                "selected credential profile is not configured; choose one explicitly configured profile"
            }
            CredentialResolutionErrorKind::AmbiguousProfile => {
                "selected credential profile is configured more than once; keep exactly one profile with that name"
            }
            CredentialResolutionErrorKind::UnknownProvider => {
                "selected credential profile does not contain the required provider kind"
            }
            CredentialResolutionErrorKind::AmbiguousProvider => {
                "selected credential profile contains the required provider kind more than once"
            }
            CredentialResolutionErrorKind::UnsupportedCapability => {
                "credential provider does not supply every required capability"
            }
            CredentialResolutionErrorKind::UnsupportedLifetime => {
                "credential provider does not supply the required lease lifetime"
            }
            CredentialResolutionErrorKind::UnsupportedRenewal => {
                "credential provider does not support required lease renewal"
            }
            CredentialResolutionErrorKind::UnsupportedRevocation => {
                "credential provider does not support required lease revocation"
            }
            CredentialResolutionErrorKind::InsufficientHandleCapacity => {
                "credential provider cannot admit the required handle capacity"
            }
            CredentialResolutionErrorKind::ProviderUnavailable => {
                "credential provider was unavailable before the run began"
            }
            CredentialResolutionErrorKind::ProviderRefused => {
                "credential provider refused the logical requirement before the run began"
            }
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for CredentialResolutionError {}

/// Resolve one supplied requirement through one explicitly selected profile.
///
/// The function performs exact profile/provider selection and all compatibility
/// checks before the provider can acquire secret-bearing state. It does not
/// inspect a plan or infer a profile from any other run option.
pub fn resolve_explicit_profile<'run>(
    selected: &CredentialProfileName,
    profiles: &'run [CredentialProfile<'run>],
    requirement: &CredentialRequirement,
) -> Result<LeasedCredentialHandle<'run>, CredentialResolutionError> {
    let mut matches = profiles.iter().filter(|profile| profile.name() == selected);
    let profile = matches.next().ok_or(CredentialResolutionError::new(
        CredentialResolutionErrorKind::UnknownProfile,
    ))?;
    if matches.next().is_some() {
        return Err(CredentialResolutionError::new(
            CredentialResolutionErrorKind::AmbiguousProfile,
        ));
    }

    let provider = profile.matching_provider(requirement.provider_kind())?;
    if requirement
        .capabilities()
        .iter()
        .any(|required| !provider.capabilities().contains(required))
    {
        return Err(CredentialResolutionError::new(
            CredentialResolutionErrorKind::UnsupportedCapability,
        ));
    }
    if !provider.lifetimes().contains(&requirement.lifetime()) {
        return Err(CredentialResolutionError::new(
            CredentialResolutionErrorKind::UnsupportedLifetime,
        ));
    }
    if requirement.renewal() == CredentialRenewal::Required && !provider.supports_renewal() {
        return Err(CredentialResolutionError::new(
            CredentialResolutionErrorKind::UnsupportedRenewal,
        ));
    }
    if requirement.revocation() == CredentialRevocation::Required && !provider.supports_revocation()
    {
        return Err(CredentialResolutionError::new(
            CredentialResolutionErrorKind::UnsupportedRevocation,
        ));
    }
    if provider.handle_capacity().get() < requirement.handle_units().get() {
        return Err(CredentialResolutionError::new(
            CredentialResolutionErrorKind::InsufficientHandleCapacity,
        ));
    }

    let lease = provider.resolve(requirement).map_err(|failure| {
        CredentialResolutionError::new(match failure {
            CredentialProviderFailure::Unavailable => {
                CredentialResolutionErrorKind::ProviderUnavailable
            }
            CredentialProviderFailure::Refused => CredentialResolutionErrorKind::ProviderRefused,
        })
    })?;
    Ok(LeasedCredentialHandle {
        requirement_name: requirement.name().clone(),
        provider_kind: requirement.provider_kind().clone(),
        handle_units: requirement.handle_units(),
        _lease: lease,
        _run: PhantomData,
    })
}

fn is_profile_name(value: &str) -> bool {
    !value.is_empty()
        && value.split('.').all(|part| {
            !part.is_empty()
                && part.chars().all(|character| {
                    character.is_ascii_alphanumeric() || matches!(character, '_' | '-')
                })
        })
}
