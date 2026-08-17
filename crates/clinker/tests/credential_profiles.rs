use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use clinker_plan::credentials::{
    CredentialCapability, CredentialHandleUnits, CredentialLifetime, CredentialProviderKind,
    CredentialRenewal, CredentialRequirement, CredentialRequirementName, CredentialRevocation,
};

#[path = "../src/credential_profile.rs"]
mod credential_profile;

use credential_profile::{
    CredentialLease, CredentialProfile, CredentialProfileName, CredentialProvider,
    CredentialProviderFailure, CredentialResolutionErrorKind, LeasedCredentialHandle,
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
    _secret: String,
    drops: Arc<AtomicUsize>,
}

impl Drop for FixtureLease {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl CredentialLease for FixtureLease {}

struct FixtureProvider {
    kind: CredentialProviderKind,
    capabilities: Vec<CredentialCapability>,
    lifetimes: Vec<CredentialLifetime>,
    renewal: bool,
    revocation: bool,
    capacity: CredentialHandleUnits,
    secret: String,
    resolve_calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
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

    fn resolve(
        &self,
        _requirement: &CredentialRequirement,
    ) -> Result<Box<dyn CredentialLease>, CredentialProviderFailure> {
        self.resolve_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(FixtureLease {
            _secret: self.secret.clone(),
            drops: Arc::clone(&self.drops),
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
    FixtureProvider {
        kind: CredentialProviderKind::parse("request-signer").expect("valid provider kind"),
        capabilities,
        lifetimes,
        renewal,
        revocation,
        capacity: CredentialHandleUnits::new(
            NonZeroU32::new(capacity).expect("fixture capacity must be non-zero"),
        ),
        secret: "lease-secret-must-not-escape".to_owned(),
        resolve_calls: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
    }
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
    let selected = CredentialProfileName::parse("release").expect("valid explicit profile");
    let requirement = requirement(vec![
        CredentialCapability::AuthenticateRequest,
        CredentialCapability::OpenSession,
    ]);

    {
        let handle: LeasedCredentialHandle<'_> =
            resolve_explicit_profile(&selected, &profiles, &requirement)
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
    let requirement = requirement(vec![CredentialCapability::OpenSession]);

    let cases = [
        (
            resolve_explicit_profile(
                &CredentialProfileName::parse("missing").expect("valid explicit profile"),
                &profiles,
                &requirement,
            )
            .expect_err("unknown profile must fail")
            .kind(),
            CredentialResolutionErrorKind::UnknownProfile,
        ),
        (
            resolve_explicit_profile(
                &CredentialProfileName::parse("empty").expect("valid explicit profile"),
                &[CredentialProfile::new(
                    CredentialProfileName::parse("empty").expect("valid explicit profile"),
                    &[],
                )],
                &requirement,
            )
            .expect_err("unknown provider must fail")
            .kind(),
            CredentialResolutionErrorKind::UnknownProvider,
        ),
        (
            resolve_explicit_profile(
                &CredentialProfileName::parse("release").expect("valid explicit profile"),
                &profiles,
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
        &profiles,
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
    let selected = CredentialProfileName::parse("release").expect("valid explicit profile");

    let mut requirement = requirement(vec![CredentialCapability::AuthenticateRequest]);
    assert_eq!(
        resolve_explicit_profile(&selected, &profiles, &requirement)
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
        resolve_explicit_profile(&selected, &profiles, &requirement)
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
        resolve_explicit_profile(&selected, &profiles, &requirement)
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
        resolve_explicit_profile(&selected, &profiles, &requirement)
            .expect_err("insufficient capacity must fail")
            .kind(),
        CredentialResolutionErrorKind::InsufficientHandleCapacity
    );
    assert_eq!(resolve_calls.load(Ordering::SeqCst), 0);
}
