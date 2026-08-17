use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use clinker_exec::pipeline::memory::MemoryArbitrator;
use clinker_plan::credentials::{
    CredentialCapability, CredentialHandleUnits, CredentialLifetime, CredentialProviderKind,
    CredentialRenewal, CredentialRequirement, CredentialRequirementName, CredentialRevocation,
};

#[path = "../src/credential_profile.rs"]
mod credential_profile;

use credential_profile::{
    CredentialHandleRegistry, CredentialLease, CredentialLeaseFailure, CredentialProfile,
    CredentialProfileAdmissionErrorKind, CredentialProfileCatalog, CredentialProfileLimits,
    CredentialProfileName, CredentialProvider, CredentialProviderFailure,
    CredentialRegistryErrorKind, CredentialResolutionErrorKind, LeasedCredentialHandle,
    resolve_explicit_profile,
};

fn requirement(capabilities: Vec<CredentialCapability>) -> CredentialRequirement {
    CredentialRequirement::new(
        CredentialRequirementName::parse("orders.api").expect("valid logical requirement name"),
        CredentialProviderKind::parse("request-signer").expect("valid provider kind"),
        capabilities,
        CredentialLifetime::Run,
        CredentialRenewal::Required,
        CredentialRevocation::Required,
        CredentialHandleUnits::new(NonZeroU32::new(2).expect("non-zero units")),
    )
    .expect("valid requirement")
}

struct FixtureLease {
    _secret: Box<str>,
    id: usize,
    retained_bytes: u64,
    revoke_fails: bool,
    drops: Arc<AtomicUsize>,
    events: Arc<Mutex<Vec<String>>>,
}

impl Drop for FixtureLease {
    fn drop(&mut self) {
        self.events
            .lock()
            .expect("fixture event mutex")
            .push(format!("drop-{}", self.id));
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl CredentialLease for FixtureLease {
    fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }

    fn revoke(&mut self) -> Result<(), CredentialLeaseFailure> {
        self.events
            .lock()
            .expect("fixture event mutex")
            .push(format!("revoke-{}", self.id));
        if self.revoke_fails {
            Err(CredentialLeaseFailure::Unavailable)
        } else {
            Ok(())
        }
    }
}

struct FixtureProvider {
    kind: CredentialProviderKind,
    capabilities: Vec<CredentialCapability>,
    lifetimes: Vec<CredentialLifetime>,
    renewal: bool,
    revocation: bool,
    capacity: CredentialHandleUnits,
    secret: String,
    definition_bytes: usize,
    declared_lease_bytes: AtomicU64,
    failure: Option<CredentialProviderFailure>,
    fail_after: Option<usize>,
    revoke_failure_at: Option<usize>,
    resolve_calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    events: Arc<Mutex<Vec<String>>>,
}

impl CredentialProvider for FixtureProvider {
    fn kind(&self) -> &CredentialProviderKind {
        &self.kind
    }

    fn capabilities(&self) -> &[CredentialCapability] {
        &self.capabilities
    }

    fn lifetimes(&self) -> &[CredentialLifetime] {
        &self.lifetimes
    }

    fn supports_renewal(&self) -> bool {
        self.renewal
    }

    fn supports_revocation(&self) -> bool {
        self.revocation
    }

    fn handle_capacity(&self) -> CredentialHandleUnits {
        self.capacity
    }

    fn decoded_definition_bytes(&self) -> usize {
        self.definition_bytes
    }

    fn lease_retained_bytes(
        &self,
        _requirement: &CredentialRequirement,
    ) -> Result<u64, CredentialProviderFailure> {
        Ok(self.declared_lease_bytes.load(Ordering::SeqCst))
    }

    fn resolve(
        &self,
        _requirement: &CredentialRequirement,
    ) -> Result<Box<dyn CredentialLease>, CredentialProviderFailure> {
        let id = self.resolve_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(failure) = self.failure {
            return Err(failure);
        }
        if self.fail_after.is_some_and(|successful| id > successful) {
            return Err(CredentialProviderFailure::Unavailable);
        }
        let secret: Box<str> = self.secret.clone().into_boxed_str();
        let retained_bytes = u64::try_from(std::mem::size_of::<FixtureLease>() + secret.len())
            .expect("fixture retained bytes fit u64");
        Ok(Box::new(FixtureLease {
            _secret: secret,
            id,
            retained_bytes,
            revoke_fails: self.revoke_failure_at == Some(id),
            drops: Arc::clone(&self.drops),
            events: Arc::clone(&self.events),
        }))
    }
}

fn provider(
    capabilities: Vec<CredentialCapability>,
    lifetimes: Vec<CredentialLifetime>,
    renewal: bool,
    revocation: bool,
    capacity: u32,
) -> FixtureProvider {
    let secret = "lease-secret-must-not-escape".to_owned();
    let lease_bytes = u64::try_from(std::mem::size_of::<FixtureLease>() + secret.len())
        .expect("fixture retained bytes fit u64");
    FixtureProvider {
        kind: CredentialProviderKind::parse("request-signer").expect("valid provider kind"),
        capabilities,
        lifetimes,
        renewal,
        revocation,
        capacity: CredentialHandleUnits::new(
            NonZeroU32::new(capacity).expect("fixture capacity must be non-zero"),
        ),
        secret,
        definition_bytes: 64,
        declared_lease_bytes: AtomicU64::new(lease_bytes),
        failure: None,
        fail_after: None,
        revoke_failure_at: None,
        resolve_calls: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
        events: Arc::new(Mutex::new(Vec::new())),
    }
}

fn admitted_profiles<'run>(
    profiles: &'run [CredentialProfile<'run>],
) -> CredentialProfileCatalog<'run> {
    CredentialProfileCatalog::admit(profiles, CredentialProfileLimits::default())
        .expect("fixture profiles are within default bounds")
}

fn profile_limits(
    max_profiles: usize,
    max_providers: usize,
    max_decoded_bytes: usize,
    max_live_handles: usize,
) -> CredentialProfileLimits {
    CredentialProfileLimits::new(
        max_profiles,
        max_providers,
        max_decoded_bytes,
        max_live_handles,
    )
}

fn memory_arbitrator(limit: u64) -> MemoryArbitrator {
    MemoryArbitrator::with_policy(limit, 0.80, 0.70, MemoryArbitrator::default_policy())
}

fn provider_failure_kind(failure: CredentialProviderFailure) -> CredentialResolutionErrorKind {
    let mut provider = provider(
        vec![CredentialCapability::AuthenticateRequest],
        vec![CredentialLifetime::Run],
        true,
        true,
        2,
    );
    provider.failure = Some(failure);
    let providers: [&dyn CredentialProvider; 1] = [&provider];
    let profiles = [CredentialProfile::new(
        CredentialProfileName::parse("release").expect("valid explicit profile"),
        &providers,
    )];
    let catalog = admitted_profiles(&profiles);
    resolve_explicit_profile(
        &CredentialProfileName::parse("release").expect("valid explicit profile"),
        &catalog,
        &requirement(vec![CredentialCapability::AuthenticateRequest]),
    )
    .expect_err("provider failure must remain typed")
    .kind()
}

#[test]
fn low_level_provider_failures_are_closed_and_sanitized() {
    assert_eq!(
        provider_failure_kind(CredentialProviderFailure::Unavailable),
        CredentialResolutionErrorKind::ProviderUnavailable
    );
    assert_eq!(
        provider_failure_kind(CredentialProviderFailure::Refused),
        CredentialResolutionErrorKind::ProviderRefused
    );
}

#[test]
fn low_level_requirement_is_deterministic_strict_and_secret_free() {
    let requirement = requirement(vec![
        CredentialCapability::OpenSession,
        CredentialCapability::AuthenticateRequest,
        CredentialCapability::OpenSession,
    ]);

    assert_eq!(
        requirement.capabilities(),
        &[
            CredentialCapability::AuthenticateRequest,
            CredentialCapability::OpenSession,
        ]
    );
    let encoded = serde_json::to_value(&requirement).expect("serialize secret-free requirement");
    assert_eq!(
        encoded,
        serde_json::json!({
            "name": "orders.api",
            "provider_kind": "request-signer",
            "capabilities": ["authenticate-request", "open-session"],
            "lifetime": "run",
            "renewal": "required",
            "revocation": "required",
            "handle_units": 2
        })
    );
    let rendered = serde_json::to_string(&encoded).expect("render requirement JSON");
    assert!(!rendered.contains("secret"));
    assert!(!rendered.contains("profile"));

    let decoded: CredentialRequirement =
        serde_json::from_value(encoded).expect("deserialize valid requirement");
    assert_eq!(decoded, requirement);
    assert!(
        serde_json::from_value::<CredentialRequirement>(serde_json::json!({
            "name": "orders.api",
            "provider_kind": "request-signer",
            "capabilities": ["authenticate-request"],
            "lifetime": "run",
            "renewal": "required",
            "revocation": "required",
            "handle_units": 1,
            "token": "must-not-be-accepted"
        }))
        .is_err(),
        "unknown credential-shaped fields must fail closed"
    );

    let rejected = CredentialRequirementName::parse("must-not-print=token")
        .expect_err("invalid name must fail");
    for text in [rejected.to_string(), format!("{rejected:?}")] {
        assert!(!text.contains("must-not-print=token"));
    }
}

#[test]
fn low_level_explicit_profile_resolves_one_redacted_leased_handle() {
    let provider = provider(
        vec![
            CredentialCapability::AuthenticateRequest,
            CredentialCapability::OpenSession,
        ],
        vec![CredentialLifetime::Run],
        true,
        true,
        2,
    );
    let resolve_calls = Arc::clone(&provider.resolve_calls);
    let drops = Arc::clone(&provider.drops);
    let providers: [&dyn CredentialProvider; 1] = [&provider];
    let profiles = [CredentialProfile::new(
        CredentialProfileName::parse("release").expect("valid explicit profile"),
        &providers,
    )];
    let catalog = admitted_profiles(&profiles);
    let selected = CredentialProfileName::parse("release").expect("valid explicit profile");
    let requirement = requirement(vec![
        CredentialCapability::AuthenticateRequest,
        CredentialCapability::OpenSession,
    ]);

    {
        let handle: LeasedCredentialHandle<'_> =
            resolve_explicit_profile(&selected, &catalog, &requirement)
                .expect("compatible explicit profile resolves");
        assert_eq!(handle.requirement_name(), requirement.name());
        assert_eq!(handle.provider_kind(), requirement.provider_kind());
        assert_eq!(handle.handle_units(), requirement.handle_units());
        assert_eq!(
            format!("{handle:?}"),
            "LeasedCredentialHandle { credential: <redacted> }"
        );
        assert!(!format!("{handle:?}").contains("release"));
        assert!(!format!("{handle:?}").contains("orders.api"));
        assert!(!format!("{handle:?}").contains("lease-secret-must-not-escape"));
        assert_eq!(drops.load(Ordering::SeqCst), 0);
    }

    assert_eq!(resolve_calls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn low_level_unknown_or_incompatible_selection_fails_before_provider_resolution() {
    let provider = provider(
        vec![CredentialCapability::AuthenticateRequest],
        vec![CredentialLifetime::Request],
        false,
        false,
        1,
    );
    let resolve_calls = Arc::clone(&provider.resolve_calls);
    let providers: [&dyn CredentialProvider; 1] = [&provider];
    let profiles = [CredentialProfile::new(
        CredentialProfileName::parse("release").expect("valid explicit profile"),
        &providers,
    )];
    let catalog = admitted_profiles(&profiles);
    let empty_profiles = [CredentialProfile::new(
        CredentialProfileName::parse("empty").expect("valid explicit profile"),
        &[],
    )];
    let empty_catalog = admitted_profiles(&empty_profiles);
    let requirement = requirement(vec![CredentialCapability::OpenSession]);

    let cases = [
        (
            resolve_explicit_profile(
                &CredentialProfileName::parse("missing").expect("valid explicit profile"),
                &catalog,
                &requirement,
            )
            .expect_err("unknown profile must fail")
            .kind(),
            CredentialResolutionErrorKind::UnknownProfile,
        ),
        (
            resolve_explicit_profile(
                &CredentialProfileName::parse("empty").expect("valid explicit profile"),
                &empty_catalog,
                &requirement,
            )
            .expect_err("unknown provider must fail")
            .kind(),
            CredentialResolutionErrorKind::UnknownProvider,
        ),
        (
            resolve_explicit_profile(
                &CredentialProfileName::parse("release").expect("valid explicit profile"),
                &catalog,
                &requirement,
            )
            .expect_err("unsupported capability must fail")
            .kind(),
            CredentialResolutionErrorKind::UnsupportedCapability,
        ),
    ];
    for (actual, expected) in cases {
        assert_eq!(actual, expected);
    }
    assert_eq!(resolve_calls.load(Ordering::SeqCst), 0);

    let error = resolve_explicit_profile(
        &CredentialProfileName::parse("release").expect("valid explicit profile"),
        &catalog,
        &requirement,
    )
    .expect_err("incompatible selection must fail");
    for text in [error.to_string(), format!("{error:?}")] {
        assert!(!text.contains("release"));
        assert!(!text.contains("orders.api"));
        assert!(!text.contains("lease-secret-must-not-escape"));
    }
}

#[test]
fn low_level_lifecycle_and_capacity_requirements_fail_closed() {
    let provider = provider(
        vec![
            CredentialCapability::AuthenticateRequest,
            CredentialCapability::OpenSession,
        ],
        vec![CredentialLifetime::Request],
        false,
        false,
        1,
    );
    let resolve_calls = Arc::clone(&provider.resolve_calls);
    let providers: [&dyn CredentialProvider; 1] = [&provider];
    let profiles = [CredentialProfile::new(
        CredentialProfileName::parse("release").expect("valid explicit profile"),
        &providers,
    )];
    let catalog = admitted_profiles(&profiles);
    let selected = CredentialProfileName::parse("release").expect("valid explicit profile");

    let mut requirement = requirement(vec![CredentialCapability::AuthenticateRequest]);
    assert_eq!(
        resolve_explicit_profile(&selected, &catalog, &requirement)
            .expect_err("unsupported lifetime must fail")
            .kind(),
        CredentialResolutionErrorKind::UnsupportedLifetime
    );

    requirement = CredentialRequirement::new(
        requirement.name().clone(),
        requirement.provider_kind().clone(),
        requirement.capabilities().to_vec(),
        CredentialLifetime::Request,
        CredentialRenewal::Required,
        CredentialRevocation::NotRequired,
        CredentialHandleUnits::new(NonZeroU32::new(1).expect("non-zero units")),
    )
    .expect("valid renewal requirement");
    assert_eq!(
        resolve_explicit_profile(&selected, &catalog, &requirement)
            .expect_err("unsupported renewal must fail")
            .kind(),
        CredentialResolutionErrorKind::UnsupportedRenewal
    );

    requirement = CredentialRequirement::new(
        requirement.name().clone(),
        requirement.provider_kind().clone(),
        requirement.capabilities().to_vec(),
        CredentialLifetime::Request,
        CredentialRenewal::NotRequired,
        CredentialRevocation::Required,
        CredentialHandleUnits::new(NonZeroU32::new(1).expect("non-zero units")),
    )
    .expect("valid revocation requirement");
    assert_eq!(
        resolve_explicit_profile(&selected, &catalog, &requirement)
            .expect_err("unsupported revocation must fail")
            .kind(),
        CredentialResolutionErrorKind::UnsupportedRevocation
    );

    requirement = CredentialRequirement::new(
        requirement.name().clone(),
        requirement.provider_kind().clone(),
        requirement.capabilities().to_vec(),
        CredentialLifetime::Request,
        CredentialRenewal::NotRequired,
        CredentialRevocation::NotRequired,
        CredentialHandleUnits::new(NonZeroU32::new(2).expect("non-zero units")),
    )
    .expect("valid capacity requirement");
    assert_eq!(
        resolve_explicit_profile(&selected, &catalog, &requirement)
            .expect_err("insufficient capacity must fail")
            .kind(),
        CredentialResolutionErrorKind::InsufficientHandleCapacity
    );
    assert_eq!(resolve_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn bounds_profile_and_provider_counts_fail_before_catalog_admission() {
    let provider = provider(
        vec![CredentialCapability::AuthenticateRequest],
        vec![CredentialLifetime::Run],
        true,
        true,
        2,
    );
    let providers: [&dyn CredentialProvider; 1] = [&provider];
    let profiles = [
        CredentialProfile::new(
            CredentialProfileName::parse("first").expect("valid explicit profile"),
            &providers,
        ),
        CredentialProfile::new(
            CredentialProfileName::parse("second").expect("valid explicit profile"),
            &providers,
        ),
    ];
    let profile_error =
        match CredentialProfileCatalog::admit(&profiles, profile_limits(1, 2, usize::MAX, 2)) {
            Ok(_) => panic!("profile cap + 1 must fail"),
            Err(error) => error,
        };
    assert_eq!(
        profile_error.kind(),
        CredentialProfileAdmissionErrorKind::TooManyProfiles
    );

    let duplicate_providers: [&dyn CredentialProvider; 2] = [&provider, &provider];
    let one_profile = [CredentialProfile::new(
        CredentialProfileName::parse("release").expect("valid explicit profile"),
        &duplicate_providers,
    )];
    let provider_error =
        match CredentialProfileCatalog::admit(&one_profile, profile_limits(1, 1, usize::MAX, 2)) {
            Ok(_) => panic!("provider cap + 1 must fail"),
            Err(error) => error,
        };
    assert_eq!(
        provider_error.kind(),
        CredentialProfileAdmissionErrorKind::TooManyProviders
    );
    assert_eq!(provider.resolve_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn bounds_decoded_definition_bytes_fail_before_catalog_admission() {
    let mut provider = provider(
        vec![CredentialCapability::AuthenticateRequest],
        vec![CredentialLifetime::Run],
        true,
        true,
        2,
    );
    provider.definition_bytes = 1_024;
    let providers: [&dyn CredentialProvider; 1] = [&provider];
    let profiles = [CredentialProfile::new(
        CredentialProfileName::parse("release").expect("valid explicit profile"),
        &providers,
    )];
    let error = match CredentialProfileCatalog::admit(&profiles, profile_limits(1, 1, 1_023, 2)) {
        Ok(_) => panic!("decoded definition bytes over the cap must fail"),
        Err(error) => error,
    };
    assert_eq!(
        error.kind(),
        CredentialProfileAdmissionErrorKind::DecodedBytesExceeded
    );
    assert_eq!(provider.resolve_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn bounds_handle_cap_plus_one_releases_all_and_unregisters() {
    let provider = provider(
        vec![CredentialCapability::AuthenticateRequest],
        vec![CredentialLifetime::Run],
        true,
        true,
        2,
    );
    let events = Arc::clone(&provider.events);
    let providers: [&dyn CredentialProvider; 1] = [&provider];
    let profiles = [CredentialProfile::new(
        CredentialProfileName::parse("release").expect("valid explicit profile"),
        &providers,
    )];
    let catalog = CredentialProfileCatalog::admit(&profiles, profile_limits(1, 1, usize::MAX, 2))
        .expect("bounded profile catalog");
    let arbitrator = memory_arbitrator(u64::MAX);
    let mut registry =
        CredentialHandleRegistry::new(&arbitrator, &catalog).expect("register handle owner");
    let selected = CredentialProfileName::parse("release").expect("valid explicit profile");
    let requirement = requirement(vec![CredentialCapability::AuthenticateRequest]);

    registry
        .acquire(&selected, &requirement)
        .expect("first handle");
    registry
        .acquire(&selected, &requirement)
        .expect("second handle");
    let error = registry
        .acquire(&selected, &requirement)
        .expect_err("cap + 1 must fail closed");

    assert_eq!(
        error.kind(),
        CredentialRegistryErrorKind::HandleLimitExceeded
    );
    assert_eq!(registry.live_handle_count(), 0);
    assert_eq!(registry.retained_bytes(), 0);
    assert_eq!(arbitrator.consumer_count(), 0);
    assert_eq!(provider.resolve_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        *events.lock().expect("fixture event mutex"),
        ["revoke-2", "drop-2", "revoke-1", "drop-1"]
    );
}

#[test]
fn bounds_memory_overshoot_is_rejected_before_provider_allocation() {
    let provider = provider(
        vec![CredentialCapability::AuthenticateRequest],
        vec![CredentialLifetime::Run],
        true,
        true,
        2,
    );
    provider
        .declared_lease_bytes
        .store(128 * 1024 * 1024, Ordering::SeqCst);
    let providers: [&dyn CredentialProvider; 1] = [&provider];
    let profiles = [CredentialProfile::new(
        CredentialProfileName::parse("release").expect("valid explicit profile"),
        &providers,
    )];
    let catalog = CredentialProfileCatalog::admit(&profiles, profile_limits(1, 1, usize::MAX, 2))
        .expect("bounded profile catalog");
    let arbitrator = memory_arbitrator(64 * 1024 * 1024);
    let mut registry =
        CredentialHandleRegistry::new(&arbitrator, &catalog).expect("register handle owner");

    let error = registry
        .acquire(
            &CredentialProfileName::parse("release").expect("valid explicit profile"),
            &requirement(vec![CredentialCapability::AuthenticateRequest]),
        )
        .expect_err("prospective retained bytes exceed the run budget");

    assert_eq!(
        error.kind(),
        CredentialRegistryErrorKind::MemoryLimitExceeded
    );
    assert_eq!(provider.resolve_calls.load(Ordering::SeqCst), 0);
    assert_eq!(registry.live_handle_count(), 0);
    assert_eq!(registry.retained_bytes(), 0);
    assert_eq!(arbitrator.consumer_count(), 0);
}

#[test]
fn cleanup_drop_revokes_and_releases_in_reverse_acquisition_order() {
    let provider = provider(
        vec![CredentialCapability::AuthenticateRequest],
        vec![CredentialLifetime::Run],
        true,
        true,
        2,
    );
    let events = Arc::clone(&provider.events);
    let providers: [&dyn CredentialProvider; 1] = [&provider];
    let profiles = [CredentialProfile::new(
        CredentialProfileName::parse("release").expect("valid explicit profile"),
        &providers,
    )];
    let catalog = CredentialProfileCatalog::admit(&profiles, profile_limits(1, 1, usize::MAX, 3))
        .expect("bounded profile catalog");
    let arbitrator = memory_arbitrator(u64::MAX);
    let selected = CredentialProfileName::parse("release").expect("valid explicit profile");
    let requirement = requirement(vec![CredentialCapability::AuthenticateRequest]);

    {
        let mut registry =
            CredentialHandleRegistry::new(&arbitrator, &catalog).expect("register handle owner");
        for _ in 0..3 {
            registry
                .acquire(&selected, &requirement)
                .expect("bounded handle acquisition");
        }
        assert_eq!(registry.live_handle_count(), 3);
        assert_eq!(arbitrator.consumer_count(), 1);
    }

    assert_eq!(arbitrator.consumer_count(), 0);
    assert_eq!(
        *events.lock().expect("fixture event mutex"),
        [
            "revoke-3", "drop-3", "revoke-2", "drop-2", "revoke-1", "drop-1",
        ]
    );
}

#[test]
fn cleanup_provider_failure_releases_prior_handles_and_unregisters() {
    let mut provider = provider(
        vec![CredentialCapability::AuthenticateRequest],
        vec![CredentialLifetime::Run],
        true,
        true,
        2,
    );
    provider.fail_after = Some(2);
    let events = Arc::clone(&provider.events);
    let providers: [&dyn CredentialProvider; 1] = [&provider];
    let profiles = [CredentialProfile::new(
        CredentialProfileName::parse("release").expect("valid explicit profile"),
        &providers,
    )];
    let catalog = CredentialProfileCatalog::admit(&profiles, profile_limits(1, 1, usize::MAX, 3))
        .expect("bounded profile catalog");
    let arbitrator = memory_arbitrator(u64::MAX);
    let mut registry =
        CredentialHandleRegistry::new(&arbitrator, &catalog).expect("register handle owner");
    let selected = CredentialProfileName::parse("release").expect("valid explicit profile");
    let requirement = requirement(vec![CredentialCapability::AuthenticateRequest]);
    registry
        .acquire(&selected, &requirement)
        .expect("first handle");
    registry
        .acquire(&selected, &requirement)
        .expect("second handle");

    let error = registry
        .acquire(&selected, &requirement)
        .expect_err("provider failure must close the registry");

    assert_eq!(
        error.kind(),
        CredentialRegistryErrorKind::Resolution(CredentialResolutionErrorKind::ProviderUnavailable)
    );
    assert_eq!(registry.live_handle_count(), 0);
    assert_eq!(arbitrator.consumer_count(), 0);
    assert_eq!(
        *events.lock().expect("fixture event mutex"),
        ["revoke-2", "drop-2", "revoke-1", "drop-1"]
    );
}

#[test]
fn cleanup_spill_request_releases_before_another_acquisition() {
    let provider = provider(
        vec![CredentialCapability::AuthenticateRequest],
        vec![CredentialLifetime::Run],
        true,
        true,
        2,
    );
    let events = Arc::clone(&provider.events);
    let providers: [&dyn CredentialProvider; 1] = [&provider];
    let profiles = [CredentialProfile::new(
        CredentialProfileName::parse("release").expect("valid explicit profile"),
        &providers,
    )];
    let catalog = CredentialProfileCatalog::admit(&profiles, profile_limits(1, 1, usize::MAX, 3))
        .expect("bounded profile catalog");
    let arbitrator = memory_arbitrator(u64::MAX);
    let mut registry =
        CredentialHandleRegistry::new(&arbitrator, &catalog).expect("register handle owner");
    let selected = CredentialProfileName::parse("release").expect("valid explicit profile");
    let requirement = requirement(vec![CredentialCapability::AuthenticateRequest]);
    registry
        .acquire(&selected, &requirement)
        .expect("first handle");
    registry
        .acquire(&selected, &requirement)
        .expect("second handle");
    registry.memory_handle_for_test().request_spill();

    let error = registry
        .acquire(&selected, &requirement)
        .expect_err("spill request must stop further acquisition");

    assert_eq!(error.kind(), CredentialRegistryErrorKind::SpillRequested);
    assert_eq!(provider.resolve_calls.load(Ordering::SeqCst), 2);
    assert_eq!(registry.live_handle_count(), 0);
    assert_eq!(arbitrator.consumer_count(), 0);
    assert_eq!(
        *events.lock().expect("fixture event mutex"),
        ["revoke-2", "drop-2", "revoke-1", "drop-1"]
    );
}

#[test]
fn cleanup_pause_blocks_acquisition_until_resume() {
    let provider = provider(
        vec![CredentialCapability::AuthenticateRequest],
        vec![CredentialLifetime::Run],
        true,
        true,
        2,
    );
    let resolve_calls = Arc::clone(&provider.resolve_calls);
    let providers: [&dyn CredentialProvider; 1] = [&provider];
    let profiles = [CredentialProfile::new(
        CredentialProfileName::parse("release").expect("valid explicit profile"),
        &providers,
    )];
    let catalog = CredentialProfileCatalog::admit(&profiles, profile_limits(1, 1, usize::MAX, 1))
        .expect("bounded profile catalog");
    let arbitrator = memory_arbitrator(u64::MAX);
    let mut registry =
        CredentialHandleRegistry::new(&arbitrator, &catalog).expect("register handle owner");
    let memory_handle = Arc::clone(registry.memory_handle_for_test());
    memory_handle.pause();
    let selected = CredentialProfileName::parse("release").expect("valid explicit profile");
    let requirement = requirement(vec![CredentialCapability::AuthenticateRequest]);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();

    std::thread::scope(|scope| {
        scope.spawn(move || {
            started_tx.send(()).expect("signal acquisition start");
            let acquired = registry.acquire(&selected, &requirement).is_ok();
            done_tx.send(acquired).expect("signal acquisition result");
        });
        started_rx.recv().expect("acquisition thread started");
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_millis(50)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        assert_eq!(resolve_calls.load(Ordering::SeqCst), 0);
        memory_handle.resume();
        assert!(
            done_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("acquisition resumes")
        );
    });

    assert_eq!(resolve_calls.load(Ordering::SeqCst), 1);
    assert_eq!(arbitrator.consumer_count(), 0);
}

#[test]
fn cleanup_revoke_failure_still_releases_every_handle_and_unregisters() {
    let mut provider = provider(
        vec![CredentialCapability::AuthenticateRequest],
        vec![CredentialLifetime::Run],
        true,
        true,
        2,
    );
    provider.revoke_failure_at = Some(2);
    let events = Arc::clone(&provider.events);
    let providers: [&dyn CredentialProvider; 1] = [&provider];
    let profiles = [CredentialProfile::new(
        CredentialProfileName::parse("release").expect("valid explicit profile"),
        &providers,
    )];
    let catalog = CredentialProfileCatalog::admit(&profiles, profile_limits(1, 1, usize::MAX, 3))
        .expect("bounded profile catalog");
    let arbitrator = memory_arbitrator(u64::MAX);
    let mut registry =
        CredentialHandleRegistry::new(&arbitrator, &catalog).expect("register handle owner");
    let selected = CredentialProfileName::parse("release").expect("valid explicit profile");
    let requirement = requirement(vec![CredentialCapability::AuthenticateRequest]);
    for _ in 0..3 {
        registry
            .acquire(&selected, &requirement)
            .expect("bounded handle acquisition");
    }

    let error = registry.close().expect_err("revoke failure remains typed");

    assert_eq!(error.kind(), CredentialRegistryErrorKind::CleanupFailed);
    assert_eq!(arbitrator.consumer_count(), 0);
    assert_eq!(provider.drops.load(Ordering::SeqCst), 3);
    assert_eq!(
        *events.lock().expect("fixture event mutex"),
        [
            "revoke-3", "drop-3", "revoke-2", "drop-2", "revoke-1", "drop-1",
        ]
    );
}
