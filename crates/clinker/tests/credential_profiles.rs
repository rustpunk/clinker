use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use clinker_exec::pipeline::memory::MemoryArbitrator;
use clinker_exec::telemetry::{
    AdmissionOutcome, MetricKey, SpanFact, SpanName, SpanStatus, TelemetryArena, TelemetryProducer,
    TelemetryReceiver,
};
use clinker_plan::config::ClinkerToml;
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
    resolve_explicit_profile, resolve_explicit_profile_with_telemetry,
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
    memory_probe: Option<Arc<MemoryArbitrator>>,
    observed_bytes_at_resolve: Arc<AtomicU64>,
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
        if let Some(arbitrator) = &self.memory_probe {
            self.observed_bytes_at_resolve
                .store(arbitrator.sum_consumer_usage(), Ordering::SeqCst);
        }
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
        memory_probe: None,
        observed_bytes_at_resolve: Arc::new(AtomicU64::new(0)),
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

fn telemetry_arena() -> (TelemetryProducer, TelemetryReceiver) {
    let config = r#"
[observability]
arena_bytes = "768KB"
ordinary_lane_bytes = "512KB"
high_severity_lane_bytes = "256KB"
max_batch_bytes = "8KB"
max_attributes_per_event = 4
max_attribute_bytes = "64B"
drop_policy = "drop_newest"
sample_every = 1
rate_limit_per_second = 100000
rate_limit_burst = 100000
flush_timeout_ms = 1000

[observability.otlp]
endpoint = "https://collector.invalid"
connect_timeout_ms = 100
request_timeout_ms = 200
retry_max_attempts = 1
retry_total_timeout_ms = 500
max_response_bytes = "1KB"

[observability.otlp.auth]
mode = "none"
"#;
    let policy = ClinkerToml::parse(config)
        .expect("telemetry policy parses")
        .resolve_observability(None)
        .expect("telemetry policy resolves");
    TelemetryArena::reserve(&policy).expect("telemetry arena reserves")
}

fn saturate_ordinary_lane(producer: &TelemetryProducer) {
    for sequence in 0..20_000_u64 {
        let outcome = producer.emit_span(SpanFact {
            name: SpanName::Transform,
            status: SpanStatus::Ok,
            logical_node: "fixed-saturation-probe",
            started_at_unix_nanos: sequence,
            ended_at_unix_nanos: sequence,
        });
        if outcome.is_full() {
            return;
        }
        assert!(
            matches!(outcome, AdmissionOutcome::Accepted { .. }),
            "saturation probe was dropped for an unexpected reason: {outcome:?}"
        );
    }
    panic!("ordinary telemetry lane did not reach its fixed capacity");
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
fn bounds_catalog_reports_the_exact_admitted_definition_totals() {
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
    let limits = profile_limits(2, 2, 1_024, 3);
    let catalog = CredentialProfileCatalog::admit(&profiles, limits).expect("bounded catalog");

    assert_eq!(catalog.profile_count(), 2);
    assert_eq!(catalog.provider_count(), 2);
    assert_eq!(catalog.decoded_bytes(), 167);
    assert_eq!(catalog.limits(), limits);
    assert_eq!(limits.max_profiles(), 2);
    assert_eq!(limits.max_providers(), 2);
    assert_eq!(limits.max_decoded_bytes(), 1_024);
    assert_eq!(limits.max_live_handles(), 3);
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
        .expect("first handle fits the run budget");
    provider
        .declared_lease_bytes
        .store(128 * 1024 * 1024, Ordering::SeqCst);
    arbitrator.set_limit(64 * 1024 * 1024);

    let error = registry
        .acquire(&selected, &requirement)
        .expect_err("prospective retained bytes exceed the run budget");

    assert_eq!(
        error.kind(),
        CredentialRegistryErrorKind::MemoryLimitExceeded
    );
    assert_eq!(provider.resolve_calls.load(Ordering::SeqCst), 1);
    assert_eq!(registry.live_handle_count(), 0);
    assert_eq!(registry.retained_bytes(), 0);
    assert_eq!(arbitrator.consumer_count(), 0);
    assert_eq!(
        *events.lock().expect("fixture event mutex"),
        ["revoke-1", "drop-1"]
    );
}

#[test]
fn bounds_successful_lease_bytes_are_reported_before_provider_allocation() {
    let arbitrator = Arc::new(memory_arbitrator(u64::MAX));
    let mut provider = provider(
        vec![CredentialCapability::AuthenticateRequest],
        vec![CredentialLifetime::Run],
        true,
        true,
        2,
    );
    provider.memory_probe = Some(Arc::clone(&arbitrator));
    let observed_bytes = Arc::clone(&provider.observed_bytes_at_resolve);
    let declared_lease_bytes = provider.declared_lease_bytes.load(Ordering::SeqCst);
    let providers: [&dyn CredentialProvider; 1] = [&provider];
    let profiles = [CredentialProfile::new(
        CredentialProfileName::parse("release").expect("valid explicit profile"),
        &providers,
    )];
    let catalog = CredentialProfileCatalog::admit(&profiles, profile_limits(1, 1, usize::MAX, 1))
        .expect("bounded profile catalog");
    let mut registry = CredentialHandleRegistry::new(arbitrator.as_ref(), &catalog)
        .expect("register handle owner");
    let table_bytes = registry.retained_bytes();
    let requirement = requirement(vec![CredentialCapability::AuthenticateRequest]);
    let expected = table_bytes
        + declared_lease_bytes
        + u64::try_from(requirement.name().as_str().len()).expect("name length fits")
        + u64::try_from(requirement.provider_kind().as_str().len()).expect("kind length fits");

    registry
        .acquire(
            &CredentialProfileName::parse("release").expect("valid explicit profile"),
            &requirement,
        )
        .expect("bounded handle acquisition");

    assert_eq!(observed_bytes.load(Ordering::SeqCst), expected);
    assert_eq!(registry.retained_bytes(), expected);
    registry.close().expect("clean registry close");
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
fn cleanup_registered_spill_request_releases_at_next_checkpoint() {
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
    let retained_before = registry.retained_bytes();
    arbitrator.spill_reclaimable(retained_before);
    assert_eq!(registry.retained_bytes(), retained_before);
    assert_eq!(registry.live_handle_count(), 2);
    assert_eq!(arbitrator.consumer_count(), 1);

    let error = registry
        .honor_memory_signals()
        .expect_err("spill checkpoint must close the registry");

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
    let memory_handle = Arc::clone(registry.memory_handle());
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

#[test]
fn lifecycle_resolve_and_revoke_emit_complete_redacted_signals() {
    let provider = provider(
        vec![CredentialCapability::AuthenticateRequest],
        vec![CredentialLifetime::Run],
        true,
        true,
        2,
    );
    let providers: [&dyn CredentialProvider; 1] = [&provider];
    let profiles = [CredentialProfile::new(
        CredentialProfileName::parse("secret-profile-name").expect("valid explicit profile"),
        &providers,
    )];
    let catalog = admitted_profiles(&profiles);
    let selected =
        CredentialProfileName::parse("secret-profile-name").expect("valid explicit profile");
    let requirement = requirement(vec![CredentialCapability::AuthenticateRequest]);
    let (producer, receiver) = telemetry_arena();

    let handle =
        resolve_explicit_profile_with_telemetry(&selected, &catalog, &requirement, &producer)
            .expect("observed resolution succeeds");
    drop(handle);

    let batch = receiver
        .try_recv_batch()
        .expect("credential lifecycle signals are drainable");
    for key in [
        MetricKey::CredentialResolveStarted,
        MetricKey::CredentialResolveCompleted,
        MetricKey::CredentialRevokeStarted,
        MetricKey::CredentialRevokeCompleted,
    ] {
        assert_eq!(batch.metric(key), 1, "one {key:?}");
    }
    for key in [
        MetricKey::CredentialResolveFailed,
        MetricKey::CredentialResolveInterrupted,
        MetricKey::ResourceOpenStarted,
        MetricKey::ResourceOpenCompleted,
        MetricKey::ResourceOpenFailed,
        MetricKey::ResourceOpenInterrupted,
        MetricKey::CredentialRenewStarted,
        MetricKey::CredentialRenewCompleted,
        MetricKey::CredentialRenewFailed,
        MetricKey::CredentialRenewInterrupted,
        MetricKey::CredentialRevokeFailed,
        MetricKey::CredentialRevokeInterrupted,
    ] {
        assert_eq!(
            batch.metric(key),
            0,
            "an operation seam that did not run must not emit {key:?}"
        );
    }
    assert_eq!(batch.traces().len(), 2);
    assert_eq!(batch.traces()[0].name, SpanName::CredentialResolve);
    assert_eq!(batch.traces()[0].status, SpanStatus::Ok);
    assert_eq!(batch.traces()[1].name, SpanName::CredentialRevoke);
    assert_eq!(batch.traces()[1].status, SpanStatus::Ok);
    assert!(batch.traces().iter().all(|span| {
        span.logical_node == "credential-lifecycle"
            && span.started_at_unix_nanos <= span.ended_at_unix_nanos
    }));

    let representations = [
        serde_json::to_string(&batch).expect("telemetry batch serializes"),
        format!("{batch:?}"),
    ];
    for representation in representations {
        for forbidden in [
            "secret-profile-name",
            "orders.api",
            "request-signer",
            "lease-secret-must-not-escape",
        ] {
            assert!(
                !representation.contains(forbidden),
                "credential context escaped into telemetry: {representation}"
            );
        }
    }
}

#[test]
fn lifecycle_failed_resolve_and_revoke_report_closed_failures() {
    let mut resolve_provider = provider(
        vec![CredentialCapability::AuthenticateRequest],
        vec![CredentialLifetime::Run],
        true,
        true,
        2,
    );
    resolve_provider.failure = Some(CredentialProviderFailure::Refused);
    let resolve_providers: [&dyn CredentialProvider; 1] = [&resolve_provider];
    let resolve_profiles = [CredentialProfile::new(
        CredentialProfileName::parse("failure-profile").expect("valid explicit profile"),
        &resolve_providers,
    )];
    let resolve_catalog = admitted_profiles(&resolve_profiles);
    let selected = CredentialProfileName::parse("failure-profile").expect("valid explicit profile");
    let requirement = requirement(vec![CredentialCapability::AuthenticateRequest]);
    let (resolve_producer, resolve_receiver) = telemetry_arena();

    let error = resolve_explicit_profile_with_telemetry(
        &selected,
        &resolve_catalog,
        &requirement,
        &resolve_producer,
    )
    .expect_err("provider refusal remains the operation result");
    assert_eq!(error.kind(), CredentialResolutionErrorKind::ProviderRefused);
    let resolve_batch = resolve_receiver
        .try_recv_batch()
        .expect("failed resolve signals are drainable");
    assert_eq!(resolve_batch.metric(MetricKey::CredentialResolveStarted), 1);
    assert_eq!(resolve_batch.metric(MetricKey::CredentialResolveFailed), 1);
    assert_eq!(
        resolve_batch.metric(MetricKey::CredentialResolveCompleted),
        0
    );
    assert_eq!(resolve_batch.traces().len(), 1);
    assert_eq!(resolve_batch.traces()[0].name, SpanName::CredentialResolve);
    assert_eq!(resolve_batch.traces()[0].status, SpanStatus::Error);

    let mut revoke_provider = provider(
        vec![CredentialCapability::AuthenticateRequest],
        vec![CredentialLifetime::Run],
        true,
        true,
        2,
    );
    revoke_provider.revoke_failure_at = Some(1);
    let revoke_providers: [&dyn CredentialProvider; 1] = [&revoke_provider];
    let revoke_profiles = [CredentialProfile::new(
        CredentialProfileName::parse("failure-profile").expect("valid explicit profile"),
        &revoke_providers,
    )];
    let revoke_catalog = admitted_profiles(&revoke_profiles);
    let arbitrator = memory_arbitrator(u64::MAX);
    let (revoke_producer, revoke_receiver) = telemetry_arena();
    let mut registry =
        CredentialHandleRegistry::new_with_telemetry(&arbitrator, &revoke_catalog, revoke_producer)
            .expect("observed registry starts");
    registry
        .acquire(&selected, &requirement)
        .expect("credential resolves before failed cleanup");
    let error = registry.close().expect_err("revoke failure remains typed");
    assert_eq!(error.kind(), CredentialRegistryErrorKind::CleanupFailed);

    let revoke_batch = revoke_receiver
        .try_recv_batch()
        .expect("failed revoke signals are drainable");
    assert_eq!(revoke_batch.metric(MetricKey::CredentialRevokeStarted), 1);
    assert_eq!(revoke_batch.metric(MetricKey::CredentialRevokeFailed), 1);
    assert_eq!(revoke_batch.metric(MetricKey::CredentialRevokeCompleted), 0);
    assert!(revoke_batch.traces().iter().any(|span| {
        span.name == SpanName::CredentialRevoke && span.status == SpanStatus::Error
    }));
}

#[test]
fn admission_loss_preserves_resolution_and_reverse_cleanup() {
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
        CredentialProfileName::parse("saturated-profile").expect("valid explicit profile"),
        &providers,
    )];
    let catalog = CredentialProfileCatalog::admit(&profiles, profile_limits(1, 1, usize::MAX, 2))
        .expect("bounded profile catalog");
    let selected =
        CredentialProfileName::parse("saturated-profile").expect("valid explicit profile");
    let requirement = requirement(vec![CredentialCapability::AuthenticateRequest]);
    let arbitrator = memory_arbitrator(u64::MAX);
    let (producer, receiver) = telemetry_arena();
    saturate_ordinary_lane(&producer);
    let drops_before = producer.snapshot().full_drops;
    let mut registry =
        CredentialHandleRegistry::new_with_telemetry(&arbitrator, &catalog, producer.clone())
            .expect("observed registry starts under saturated telemetry");

    registry
        .acquire(&selected, &requirement)
        .expect("first result ignores telemetry admission");
    registry
        .acquire(&selected, &requirement)
        .expect("second result ignores telemetry admission");
    registry
        .close()
        .expect("cleanup ignores telemetry admission");

    assert_eq!(provider.resolve_calls.load(Ordering::SeqCst), 2);
    assert_eq!(provider.drops.load(Ordering::SeqCst), 2);
    assert_eq!(arbitrator.consumer_count(), 0);
    assert_eq!(
        *events.lock().expect("fixture event mutex"),
        ["revoke-2", "drop-2", "revoke-1", "drop-1"]
    );
    assert!(
        producer.snapshot().full_drops >= drops_before.saturating_add(4),
        "each completed resolve/revoke span may be dropped without changing behavior"
    );
    let batch = receiver
        .try_recv_batch()
        .expect("fixed counters remain drainable from a full arena");
    assert_eq!(batch.metric(MetricKey::CredentialResolveStarted), 2);
    assert_eq!(batch.metric(MetricKey::CredentialResolveCompleted), 2);
    assert_eq!(batch.metric(MetricKey::CredentialRevokeStarted), 2);
    assert_eq!(batch.metric(MetricKey::CredentialRevokeCompleted), 2);
    assert!(batch.traces().iter().all(|span| {
        !matches!(
            span.name,
            SpanName::CredentialResolve | SpanName::CredentialRevoke
        )
    }));
}
