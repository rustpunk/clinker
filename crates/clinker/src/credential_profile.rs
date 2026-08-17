//! Explicit run-local credential profile resolution.
//!
//! Profiles and providers are supplied by the application edge. Resolution
//! accepts one already-validated, secret-free requirement and returns one
//! opaque lease; it never searches a plan, environment, channel, or group.

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use clinker_exec::pipeline::memory::{
    ConsumerHandle, ConsumerId, ConsumerSpillError, MemoryArbitrator, MemoryConsumer,
};
use clinker_exec::telemetry::{
    MetricKey, SpanFact, SpanName, SpanStatus, TelemetryProducer, unix_nanos_now,
};
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

/// Default maximum number of named profiles admitted for one run.
pub const DEFAULT_MAX_CREDENTIAL_PROFILES: usize = 64;
/// Default maximum provider registrations across every admitted profile.
pub const DEFAULT_MAX_CREDENTIAL_PROVIDERS: usize = 256;
/// Default maximum decoded bytes across profile and provider definitions.
pub const DEFAULT_MAX_CREDENTIAL_DEFINITION_BYTES: usize = 1_048_576;
/// Default maximum number of simultaneously live credential handles.
pub const DEFAULT_MAX_LIVE_CREDENTIAL_HANDLES: usize = 256;

const CREDENTIAL_LIFECYCLE_SCOPE: &str = "credential-lifecycle";

#[derive(Clone, Copy)]
struct OperationSignals {
    started: MetricKey,
    completed: MetricKey,
    failed: MetricKey,
    span: SpanName,
}

const RESOLVE_SIGNALS: OperationSignals = OperationSignals {
    started: MetricKey::CredentialResolveStarted,
    completed: MetricKey::CredentialResolveCompleted,
    failed: MetricKey::CredentialResolveFailed,
    span: SpanName::CredentialResolve,
};

const REVOKE_SIGNALS: OperationSignals = OperationSignals {
    started: MetricKey::CredentialRevokeStarted,
    completed: MetricKey::CredentialRevokeCompleted,
    failed: MetricKey::CredentialRevokeFailed,
    span: SpanName::CredentialRevoke,
};

fn observe_operation<T, E>(
    producer: Option<&TelemetryProducer>,
    signals: OperationSignals,
    operation: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let Some(producer) = producer else {
        return operation();
    };

    let started_at_unix_nanos = unix_nanos_now();
    producer.record_metric(signals.started, 1);
    let result = operation();
    let status = if result.is_ok() {
        producer.record_metric(signals.completed, 1);
        SpanStatus::Ok
    } else {
        producer.record_metric(signals.failed, 1);
        SpanStatus::Error
    };
    let ended_at_unix_nanos = unix_nanos_now().max(started_at_unix_nanos);
    let _ = producer.emit_span(SpanFact {
        name: signals.span,
        status,
        logical_node: CREDENTIAL_LIFECYCLE_SCOPE,
        started_at_unix_nanos,
        ended_at_unix_nanos,
    });
    result
}

/// Fixed admission limits for profile definitions and live handles.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CredentialProfileLimits {
    max_profiles: usize,
    max_providers: usize,
    max_decoded_bytes: usize,
    max_live_handles: usize,
}

impl CredentialProfileLimits {
    /// Construct explicit fixed limits for one run.
    pub const fn new(
        max_profiles: usize,
        max_providers: usize,
        max_decoded_bytes: usize,
        max_live_handles: usize,
    ) -> Self {
        Self {
            max_profiles,
            max_providers,
            max_decoded_bytes,
            max_live_handles,
        }
    }

    /// Maximum admitted profile definitions.
    pub const fn max_profiles(self) -> usize {
        self.max_profiles
    }

    /// Maximum provider registrations across all profiles.
    pub const fn max_providers(self) -> usize {
        self.max_providers
    }

    /// Maximum decoded definition bytes.
    pub const fn max_decoded_bytes(self) -> usize {
        self.max_decoded_bytes
    }

    /// Maximum simultaneously live handles retained by the registry.
    pub const fn max_live_handles(self) -> usize {
        self.max_live_handles
    }
}

impl Default for CredentialProfileLimits {
    fn default() -> Self {
        Self::new(
            DEFAULT_MAX_CREDENTIAL_PROFILES,
            DEFAULT_MAX_CREDENTIAL_PROVIDERS,
            DEFAULT_MAX_CREDENTIAL_DEFINITION_BYTES,
            DEFAULT_MAX_LIVE_CREDENTIAL_HANDLES,
        )
    }
}

/// Opaque provider-owned state that remains live for a resolved credential.
///
/// The marker deliberately exposes no secret accessor. Dropping the enclosing
/// [`LeasedCredentialHandle`] closes the lease on every ordinary exit path.
pub trait CredentialLease: Send + Sync {
    /// True retained bytes owned by this lease, including its boxed payload.
    fn retained_bytes(&self) -> u64;

    /// Revoke provider-side authority before releasing the local lease.
    fn revoke(&mut self) -> Result<(), CredentialLeaseFailure>;
}

/// A closed, sanitized lease-revocation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialLeaseFailure {
    /// The provider could not confirm revocation at cleanup time.
    Unavailable,
}

impl fmt::Display for CredentialLeaseFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("credential lease revocation could not be confirmed")
    }
}

impl std::error::Error for CredentialLeaseFailure {}

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
pub trait CredentialProvider: Send + Sync {
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

    /// Decoded bytes retained by this provider's profile definition.
    ///
    /// The count includes secret-bearing decoded configuration without
    /// exposing it. [`CredentialProfileCatalog::admit`] checks the aggregate
    /// before any definition becomes eligible for resolution.
    fn decoded_definition_bytes(&self) -> usize;

    /// Exact bytes the next resolved lease will retain.
    ///
    /// The registry charges this value before `resolve` may allocate the
    /// lease, then verifies it against [`CredentialLease::retained_bytes`].
    fn lease_retained_bytes(
        &self,
        requirement: &CredentialRequirement,
    ) -> Result<u64, CredentialProviderFailure>;

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

/// A bounded, validated view over supplied credential profile definitions.
///
/// Admission borrows the definitions rather than copying secret-bearing
/// provider state. Counts, decoded bytes, and duplicate names are checked in a
/// complete pass before the catalog can be used for resolution.
pub struct CredentialProfileCatalog<'run> {
    profiles: &'run [CredentialProfile<'run>],
    limits: CredentialProfileLimits,
    provider_count: usize,
    decoded_bytes: usize,
}

impl<'run> CredentialProfileCatalog<'run> {
    /// Admit supplied definitions under fixed count and decoded-byte limits.
    ///
    /// # Errors
    ///
    /// Returns a sanitized error before producing a catalog when any count or
    /// byte ceiling is exceeded, arithmetic overflows, or a profile/provider
    /// kind is duplicated.
    pub fn admit(
        profiles: &'run [CredentialProfile<'run>],
        limits: CredentialProfileLimits,
    ) -> Result<Self, CredentialProfileAdmissionError> {
        if profiles.len() > limits.max_profiles {
            return Err(CredentialProfileAdmissionError::new(
                CredentialProfileAdmissionErrorKind::TooManyProfiles,
            ));
        }

        let mut provider_count = 0usize;
        let mut decoded_bytes = 0usize;
        for profile in profiles {
            provider_count = provider_count
                .checked_add(profile.providers.len())
                .ok_or_else(|| {
                    CredentialProfileAdmissionError::new(
                        CredentialProfileAdmissionErrorKind::DefinitionSizeOverflow,
                    )
                })?;
            if provider_count > limits.max_providers {
                return Err(CredentialProfileAdmissionError::new(
                    CredentialProfileAdmissionErrorKind::TooManyProviders,
                ));
            }

            decoded_bytes = decoded_bytes
                .checked_add(profile.name.as_str().len())
                .ok_or_else(|| {
                    CredentialProfileAdmissionError::new(
                        CredentialProfileAdmissionErrorKind::DefinitionSizeOverflow,
                    )
                })?;
            for &provider in profile.providers {
                decoded_bytes = decoded_bytes
                    .checked_add(provider.kind().as_str().len())
                    .and_then(|bytes| bytes.checked_add(provider.decoded_definition_bytes()))
                    .ok_or_else(|| {
                        CredentialProfileAdmissionError::new(
                            CredentialProfileAdmissionErrorKind::DefinitionSizeOverflow,
                        )
                    })?;
                if decoded_bytes > limits.max_decoded_bytes {
                    return Err(CredentialProfileAdmissionError::new(
                        CredentialProfileAdmissionErrorKind::DecodedBytesExceeded,
                    ));
                }
            }
        }

        for (index, profile) in profiles.iter().enumerate() {
            if profiles[..index]
                .iter()
                .any(|earlier| earlier.name == profile.name)
            {
                return Err(CredentialProfileAdmissionError::new(
                    CredentialProfileAdmissionErrorKind::DuplicateProfile,
                ));
            }
            for (provider_index, provider) in profile.providers.iter().copied().enumerate() {
                if profile.providers[..provider_index]
                    .iter()
                    .copied()
                    .any(|earlier| earlier.kind() == provider.kind())
                {
                    return Err(CredentialProfileAdmissionError::new(
                        CredentialProfileAdmissionErrorKind::DuplicateProvider,
                    ));
                }
            }
        }

        Ok(Self {
            profiles,
            limits,
            provider_count,
            decoded_bytes,
        })
    }

    /// Fixed limits used to admit this catalog.
    pub const fn limits(&self) -> CredentialProfileLimits {
        self.limits
    }

    /// Number of admitted profiles.
    pub fn profile_count(&self) -> usize {
        self.profiles.len()
    }

    /// Number of admitted provider registrations.
    pub const fn provider_count(&self) -> usize {
        self.provider_count
    }

    /// Total decoded definition bytes admitted.
    pub const fn decoded_bytes(&self) -> usize {
        self.decoded_bytes
    }
}

/// Stable category for a profile-catalog admission failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialProfileAdmissionErrorKind {
    /// The profile count exceeded its fixed ceiling.
    TooManyProfiles,
    /// The provider-registration count exceeded its fixed ceiling.
    TooManyProviders,
    /// Decoded definition bytes exceeded their fixed ceiling.
    DecodedBytesExceeded,
    /// Definition byte or count arithmetic overflowed.
    DefinitionSizeOverflow,
    /// A profile name was supplied more than once.
    DuplicateProfile,
    /// One profile supplied the same provider kind more than once.
    DuplicateProvider,
}

/// A sanitized profile-catalog admission failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CredentialProfileAdmissionError {
    kind: CredentialProfileAdmissionErrorKind,
}

impl CredentialProfileAdmissionError {
    const fn new(kind: CredentialProfileAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Stable failure category.
    pub const fn kind(self) -> CredentialProfileAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for CredentialProfileAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self.kind {
            CredentialProfileAdmissionErrorKind::TooManyProfiles => {
                "credential profile count exceeds the fixed admission limit"
            }
            CredentialProfileAdmissionErrorKind::TooManyProviders => {
                "credential provider count exceeds the fixed admission limit"
            }
            CredentialProfileAdmissionErrorKind::DecodedBytesExceeded => {
                "credential profile decoded bytes exceed the fixed admission limit"
            }
            CredentialProfileAdmissionErrorKind::DefinitionSizeOverflow => {
                "credential profile definition size overflowed"
            }
            CredentialProfileAdmissionErrorKind::DuplicateProfile => {
                "credential profile name is configured more than once"
            }
            CredentialProfileAdmissionErrorKind::DuplicateProvider => {
                "credential provider kind is configured more than once in one profile"
            }
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for CredentialProfileAdmissionError {}

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
    retained_bytes: u64,
    lease: Box<dyn CredentialLease>,
    revoked: bool,
    telemetry: Option<TelemetryProducer>,
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

    fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }

    fn revoke(&mut self) -> Result<(), CredentialLeaseFailure> {
        if self.revoked {
            return Ok(());
        }
        let result = observe_operation(self.telemetry.as_ref(), REVOKE_SIGNALS, || {
            self.lease.revoke()
        });
        self.revoked = true;
        result
    }
}

impl fmt::Debug for LeasedCredentialHandle<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeasedCredentialHandle { credential: <redacted> }")
    }
}

impl Drop for LeasedCredentialHandle<'_> {
    fn drop(&mut self) {
        let _ = self.revoke();
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
    /// The resolved lease did not match its pre-allocation byte declaration.
    RetainedBytesMismatch,
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
            CredentialResolutionErrorKind::RetainedBytesMismatch => {
                "credential provider returned a lease with inconsistent retained-byte accounting"
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
    catalog: &CredentialProfileCatalog<'run>,
    requirement: &CredentialRequirement,
) -> Result<LeasedCredentialHandle<'run>, CredentialResolutionError> {
    resolve_explicit_profile_inner(selected, catalog, requirement, None)
}

/// Resolve one supplied requirement and emit lifecycle signals when configured.
///
/// The emitted vocabulary is closed and carries no profile name, provider
/// kind, logical requirement, lease payload, or record value. Signal admission
/// is optional: a dropped completed span cannot change the returned handle or
/// error, and revocation remains owned by the handle.
pub fn resolve_explicit_profile_with_telemetry<'run>(
    selected: &CredentialProfileName,
    catalog: &CredentialProfileCatalog<'run>,
    requirement: &CredentialRequirement,
    producer: &TelemetryProducer,
) -> Result<LeasedCredentialHandle<'run>, CredentialResolutionError> {
    resolve_explicit_profile_inner(selected, catalog, requirement, Some(producer))
}

fn resolve_explicit_profile_inner<'run>(
    selected: &CredentialProfileName,
    catalog: &CredentialProfileCatalog<'run>,
    requirement: &CredentialRequirement,
    producer: Option<&TelemetryProducer>,
) -> Result<LeasedCredentialHandle<'run>, CredentialResolutionError> {
    let provider = compatible_provider(selected, catalog, requirement)?;
    let declared_lease_bytes = provider
        .lease_retained_bytes(requirement)
        .map_err(resolution_provider_error)?;
    let mut lease = observe_operation(producer, RESOLVE_SIGNALS, || provider.resolve(requirement))
        .map_err(resolution_provider_error)?;
    if lease.retained_bytes() != declared_lease_bytes {
        let _ = observe_operation(producer, REVOKE_SIGNALS, || lease.revoke());
        return Err(CredentialResolutionError::new(
            CredentialResolutionErrorKind::RetainedBytesMismatch,
        ));
    }
    let retained_bytes = handle_dynamic_bytes(requirement, declared_lease_bytes).ok_or(
        CredentialResolutionError::new(CredentialResolutionErrorKind::RetainedBytesMismatch),
    )?;
    Ok(LeasedCredentialHandle {
        requirement_name: requirement.name().clone(),
        provider_kind: requirement.provider_kind().clone(),
        handle_units: requirement.handle_units(),
        retained_bytes,
        lease,
        revoked: false,
        telemetry: producer.cloned(),
        _run: PhantomData,
    })
}

fn compatible_provider<'run>(
    selected: &CredentialProfileName,
    catalog: &CredentialProfileCatalog<'run>,
    requirement: &CredentialRequirement,
) -> Result<&'run dyn CredentialProvider, CredentialResolutionError> {
    let mut matches = catalog
        .profiles
        .iter()
        .filter(|profile| profile.name() == selected);
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

    Ok(provider)
}

fn resolution_provider_error(failure: CredentialProviderFailure) -> CredentialResolutionError {
    CredentialResolutionError::new(match failure {
        CredentialProviderFailure::Unavailable => {
            CredentialResolutionErrorKind::ProviderUnavailable
        }
        CredentialProviderFailure::Refused => CredentialResolutionErrorKind::ProviderRefused,
    })
}

fn handle_dynamic_bytes(requirement: &CredentialRequirement, lease_bytes: u64) -> Option<u64> {
    let name_bytes = u64::try_from(requirement.name().as_str().len()).ok()?;
    let kind_bytes = u64::try_from(requirement.provider_kind().as_str().len()).ok()?;
    lease_bytes.checked_add(name_bytes)?.checked_add(kind_bytes)
}

struct CredentialRegistryConsumer {
    handle: Arc<ConsumerHandle>,
}

impl CredentialRegistryConsumer {
    fn new(handle: Arc<ConsumerHandle>) -> Self {
        Self { handle }
    }
}

impl MemoryConsumer for CredentialRegistryConsumer {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }

    fn spill_priority(&self) -> i32 {
        i32::MAX - 1
    }

    fn try_spill(&self, target_bytes: u64) -> Result<u64, ConsumerSpillError> {
        // The arbitrator holds only shared access, while revocation and
        // ordered teardown belong to the registry's run thread. Deliver the
        // request without claiming that this callback released live state.
        self.handle.request_spill();
        Err(ConsumerSpillError::BelowTarget {
            target: target_bytes,
            freed: 0,
        })
    }

    fn can_back_pressure(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod memory_consumer_contract_tests {
    use super::*;

    #[test]
    fn registered_consumer_reports_only_bytes_freed_synchronously() {
        let handle = ConsumerHandle::new();
        handle.set_bytes(4_096);
        let consumer = Arc::new(CredentialRegistryConsumer::new(Arc::clone(&handle)));
        let arbitrator =
            MemoryArbitrator::with_policy(u64::MAX, 0.80, 0.70, MemoryArbitrator::default_policy());
        let consumer_id = arbitrator.register_consumer(consumer.clone());

        let result = consumer.try_spill(2_048);

        assert!(matches!(
            result,
            Err(ConsumerSpillError::BelowTarget {
                target: 2_048,
                freed: 0,
            })
        ));
        assert!(!consumer.can_back_pressure());
        assert_eq!(consumer.current_usage(), 4_096);
        assert!(handle.take_spill_request());
        assert!(arbitrator.unregister_consumer(consumer_id).is_some());
    }
}

/// Run-local owner for every credential lease acquired during preflight.
///
/// The registry preallocates a fixed handle table, registers exactly one
/// [`MemoryConsumer`], and charges each provider-declared lease plus its
/// secret-free handle metadata before the provider may allocate it. Credential
/// state is never written to spill. The consumer reports zero bytes freed when
/// the arbitrator requests a spill, then the registry revokes and releases the
/// complete preflight set at its next owned memory-signal checkpoint.
pub struct CredentialHandleRegistry<'catalog, 'run>
where
    'run: 'catalog,
{
    arbitrator: &'catalog MemoryArbitrator,
    catalog: &'catalog CredentialProfileCatalog<'run>,
    memory_handle: Arc<ConsumerHandle>,
    consumer: Arc<CredentialRegistryConsumer>,
    consumer_id: Option<ConsumerId>,
    handles: Vec<LeasedCredentialHandle<'run>>,
    retained_bytes: u64,
    telemetry: Option<TelemetryProducer>,
}

impl<'catalog, 'run> CredentialHandleRegistry<'catalog, 'run>
where
    'run: 'catalog,
{
    /// Create an empty registry and register its single memory consumer.
    ///
    /// The fixed handle-slot table is allocated before registration and holds
    /// no credential state. Its actual capacity bytes are charged immediately;
    /// every subsequent lease allocation is precharged before provider work.
    ///
    /// # Errors
    ///
    /// Returns [`CredentialRegistryErrorKind::AllocationFailed`] when the
    /// fixed handle table cannot be reserved or its byte size cannot be
    /// represented by the memory consumer.
    pub fn new(
        arbitrator: &'catalog MemoryArbitrator,
        catalog: &'catalog CredentialProfileCatalog<'run>,
    ) -> Result<Self, CredentialRegistryError> {
        Self::new_inner(arbitrator, catalog, None)
    }

    /// Create a registry whose actual resolve and revoke calls emit lifecycle
    /// signals through the supplied fixed arena.
    ///
    /// Signal admission is behavior-neutral. The producer is a fixed set of
    /// shared handles, and each preallocated credential slot already includes
    /// the size of its optional producer clone in registered table bytes.
    ///
    /// # Errors
    ///
    /// Returns [`CredentialRegistryErrorKind::AllocationFailed`] under the
    /// same fixed-table conditions as [`Self::new`].
    pub fn new_with_telemetry(
        arbitrator: &'catalog MemoryArbitrator,
        catalog: &'catalog CredentialProfileCatalog<'run>,
        producer: TelemetryProducer,
    ) -> Result<Self, CredentialRegistryError> {
        Self::new_inner(arbitrator, catalog, Some(producer))
    }

    fn new_inner(
        arbitrator: &'catalog MemoryArbitrator,
        catalog: &'catalog CredentialProfileCatalog<'run>,
        telemetry: Option<TelemetryProducer>,
    ) -> Result<Self, CredentialRegistryError> {
        let mut handles = Vec::new();
        handles
            .try_reserve_exact(catalog.limits.max_live_handles)
            .map_err(|_| {
                CredentialRegistryError::new(CredentialRegistryErrorKind::AllocationFailed)
            })?;
        let table_bytes = handles
            .capacity()
            .checked_mul(std::mem::size_of::<LeasedCredentialHandle<'run>>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| {
                CredentialRegistryError::new(CredentialRegistryErrorKind::AllocationFailed)
            })?;
        let memory_handle = ConsumerHandle::new();
        memory_handle.set_bytes(table_bytes);
        let consumer = Arc::new(CredentialRegistryConsumer::new(Arc::clone(&memory_handle)));
        let consumer_id = arbitrator.register_consumer(consumer.clone());
        Ok(Self {
            arbitrator,
            catalog,
            memory_handle,
            consumer,
            consumer_id: Some(consumer_id),
            handles,
            retained_bytes: table_bytes,
            telemetry,
        })
    }

    /// Acquire and retain one requirement through the explicit profile.
    ///
    /// Pause and spill requests are honored before every growth boundary. A
    /// failed acquisition revokes all earlier handles and unregisters the
    /// registry, so callers cannot accidentally continue from partial
    /// preflight state.
    ///
    /// # Errors
    ///
    /// Returns a sanitized resolution, capacity, memory, accounting, or
    /// provider error. Every error leaves zero live handles and no registered
    /// consumer.
    pub fn acquire(
        &mut self,
        selected: &CredentialProfileName,
        requirement: &CredentialRequirement,
    ) -> Result<&LeasedCredentialHandle<'run>, CredentialRegistryError> {
        self.honor_memory_signals()?;
        if self.handles.len() >= self.catalog.limits.max_live_handles {
            return Err(self.fail_and_close(CredentialRegistryErrorKind::HandleLimitExceeded));
        }

        let provider = match compatible_provider(selected, self.catalog, requirement) {
            Ok(provider) => provider,
            Err(error) => {
                return Err(
                    self.fail_and_close(CredentialRegistryErrorKind::Resolution(error.kind()))
                );
            }
        };
        let lease_bytes = match provider.lease_retained_bytes(requirement) {
            Ok(bytes) => bytes,
            Err(failure) => {
                return Err(self.fail_and_close(CredentialRegistryErrorKind::Resolution(
                    resolution_provider_error(failure).kind(),
                )));
            }
        };
        let Some(dynamic_bytes) = handle_dynamic_bytes(requirement, lease_bytes) else {
            return Err(self.fail_and_close(CredentialRegistryErrorKind::RetainedBytesOverflow));
        };
        let Some(prospective_bytes) = self.retained_bytes.checked_add(dynamic_bytes) else {
            return Err(self.fail_and_close(CredentialRegistryErrorKind::RetainedBytesOverflow));
        };

        self.memory_handle.set_bytes(prospective_bytes);
        if self.arbitrator.should_abort_local(prospective_bytes) {
            return Err(self.fail_and_close(CredentialRegistryErrorKind::MemoryLimitExceeded));
        }

        let mut lease = match observe_operation(self.telemetry.as_ref(), RESOLVE_SIGNALS, || {
            provider.resolve(requirement)
        }) {
            Ok(lease) => lease,
            Err(failure) => {
                self.memory_handle.set_bytes(self.retained_bytes);
                return Err(self.fail_and_close(CredentialRegistryErrorKind::Resolution(
                    resolution_provider_error(failure).kind(),
                )));
            }
        };
        if lease.retained_bytes() != lease_bytes {
            let _ = observe_operation(self.telemetry.as_ref(), REVOKE_SIGNALS, || lease.revoke());
            self.memory_handle.set_bytes(self.retained_bytes);
            return Err(self.fail_and_close(CredentialRegistryErrorKind::RetainedBytesMismatch));
        }

        self.handles.push(LeasedCredentialHandle {
            requirement_name: requirement.name().clone(),
            provider_kind: requirement.provider_kind().clone(),
            handle_units: requirement.handle_units(),
            retained_bytes: dynamic_bytes,
            lease,
            revoked: false,
            telemetry: self.telemetry.clone(),
            _run: PhantomData,
        });
        self.retained_bytes = prospective_bytes;
        Ok(self
            .handles
            .last()
            .expect("a just-pushed credential handle must exist"))
    }

    /// Number of live leases currently owned by the registry.
    pub fn live_handle_count(&self) -> usize {
        self.handles.len()
    }

    /// Bytes currently reported through the registry's consumer handle.
    pub fn retained_bytes(&self) -> u64 {
        self.consumer.current_usage()
    }

    /// Honor delivered pause or spill signals at a registry-owned boundary.
    ///
    /// The registered consumer has no inbound producer and therefore does not
    /// advertise arbitrator backpressure. An explicit run coordinator may
    /// still pause the shared handle before entering this checkpoint. A spill
    /// request always fails closed: every live lease is revoked in reverse
    /// acquisition order and the consumer is unregistered before returning.
    ///
    /// # Errors
    ///
    /// Returns [`CredentialRegistryErrorKind::SpillRequested`] after cleanup
    /// when the arbitrator delivered a spill request, or
    /// [`CredentialRegistryErrorKind::Closed`] after prior teardown.
    pub fn honor_memory_signals(&mut self) -> Result<(), CredentialRegistryError> {
        if self.consumer_id.is_none() {
            return Err(CredentialRegistryError::new(
                CredentialRegistryErrorKind::Closed,
            ));
        }

        if self.memory_handle.take_spill_request() {
            return Err(self.fail_and_close(CredentialRegistryErrorKind::SpillRequested));
        }
        self.memory_handle.wait_while_paused();
        if self.memory_handle.take_spill_request() {
            return Err(self.fail_and_close(CredentialRegistryErrorKind::SpillRequested));
        }
        Ok(())
    }

    /// Revoke and release every handle, then unregister the memory consumer.
    ///
    /// Cleanup continues after a revocation failure so no later lease or
    /// registration survives the error.
    ///
    /// # Errors
    ///
    /// Returns [`CredentialRegistryErrorKind::CleanupFailed`] when at least
    /// one provider could not confirm revocation. All local handles are still
    /// released and the consumer is still unregistered.
    pub fn close(mut self) -> Result<(), CredentialRegistryError> {
        let cleanup_failed = self.release_all();
        self.unregister();
        if cleanup_failed {
            Err(CredentialRegistryError::new(
                CredentialRegistryErrorKind::CleanupFailed,
            ))
        } else {
            Ok(())
        }
    }

    /// Borrow the non-secret memory coordination handle for run control.
    ///
    /// Callers may use this only to observe accounting or deliver explicit
    /// run-coordination pause/spill signals; it exposes no lease. The
    /// registered arbitrator consumer does not advertise backpressure because
    /// the registry has no inbound producer to pause.
    pub fn memory_handle(&self) -> &Arc<ConsumerHandle> {
        &self.memory_handle
    }

    fn fail_and_close(&mut self, kind: CredentialRegistryErrorKind) -> CredentialRegistryError {
        let _ = self.release_all();
        self.unregister();
        CredentialRegistryError::new(kind)
    }

    fn release_all(&mut self) -> bool {
        let mut cleanup_failed = false;
        while let Some(mut handle) = self.handles.pop() {
            let bytes = handle.retained_bytes();
            if handle.revoke().is_err() {
                cleanup_failed = true;
            }
            drop(handle);
            self.retained_bytes = self.retained_bytes.saturating_sub(bytes);
            self.memory_handle.set_bytes(self.retained_bytes);
        }
        cleanup_failed
    }

    fn unregister(&mut self) {
        self.memory_handle.set_bytes(0);
        self.retained_bytes = 0;
        if let Some(consumer_id) = self.consumer_id.take() {
            let _ = self.arbitrator.unregister_consumer(consumer_id);
        }
    }
}

impl Drop for CredentialHandleRegistry<'_, '_> {
    fn drop(&mut self) {
        let _ = self.release_all();
        self.unregister();
    }
}

/// Stable category for registry construction, acquisition, or cleanup failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialRegistryErrorKind {
    /// Profile/provider compatibility or provider acquisition failed.
    Resolution(CredentialResolutionErrorKind),
    /// The fixed handle-entry ceiling was reached.
    HandleLimitExceeded,
    /// Prospective retained bytes exceeded the run memory budget.
    MemoryLimitExceeded,
    /// The arbitrator requested release before another acquisition.
    SpillRequested,
    /// Retained-byte arithmetic overflowed.
    RetainedBytesOverflow,
    /// A provider's declared and actual retained bytes differed.
    RetainedBytesMismatch,
    /// At least one lease could not confirm revocation.
    CleanupFailed,
    /// The fixed handle table could not be allocated or represented.
    AllocationFailed,
    /// An acquisition was attempted after the registry closed.
    Closed,
}

/// A sanitized credential-registry failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CredentialRegistryError {
    kind: CredentialRegistryErrorKind,
}

impl CredentialRegistryError {
    const fn new(kind: CredentialRegistryErrorKind) -> Self {
        Self { kind }
    }

    /// Stable failure category.
    pub const fn kind(self) -> CredentialRegistryErrorKind {
        self.kind
    }
}

impl fmt::Display for CredentialRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self.kind {
            CredentialRegistryErrorKind::Resolution(_) => {
                "credential requirement could not be resolved through the selected profile"
            }
            CredentialRegistryErrorKind::HandleLimitExceeded => {
                "credential live-handle count exceeds the fixed admission limit"
            }
            CredentialRegistryErrorKind::MemoryLimitExceeded => {
                "credential handle bytes exceed the run memory budget"
            }
            CredentialRegistryErrorKind::SpillRequested => {
                "credential acquisition stopped under memory pressure"
            }
            CredentialRegistryErrorKind::RetainedBytesOverflow => {
                "credential retained-byte accounting overflowed"
            }
            CredentialRegistryErrorKind::RetainedBytesMismatch => {
                "credential provider returned inconsistent retained-byte accounting"
            }
            CredentialRegistryErrorKind::CleanupFailed => {
                "credential cleanup released all local handles but revocation was not confirmed"
            }
            CredentialRegistryErrorKind::AllocationFailed => {
                "credential handle registry could not allocate its fixed table"
            }
            CredentialRegistryErrorKind::Closed => "credential handle registry is already closed",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for CredentialRegistryError {}

fn is_profile_name(value: &str) -> bool {
    !value.is_empty()
        && value.split('.').all(|part| {
            !part.is_empty()
                && part.chars().all(|character| {
                    character.is_ascii_alphanumeric() || matches!(character, '_' | '-')
                })
        })
}
