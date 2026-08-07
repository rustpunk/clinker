//! Publication failures must report what a supervisor can act on: which
//! artifacts are durable, which are merely visible, and how much staged
//! debt is still owed reclamation.

use std::io::Write;
use std::path::Path;

use clinker_exec::output::attempt::{
    ArtifactKind, ArtifactRegistration, ArtifactState, AttemptFault, AttemptPublication,
    AttemptPublicationOutcome,
};
use clinker_exec::output::containment::PromotionDisposition;
use clinker_exec::output::staging::OutputStagingRegistry;
use clinker_exec::pipeline::shutdown::ShutdownToken;
use clinker_plan::config::{ClinkerToml, ResolvedPublicationPolicy};
use clinker_plan::security::{ValidatedPath, validate_path};

const EXECUTION_ID: &str = "018f47a2-9a41-7a27-b4d6-4f7137e3c159";

fn validated(root: &Path, relative: &str) -> ValidatedPath {
    validate_path(Path::new(relative), root, false).expect("fixture path should validate")
}

fn direct_policy(destination_root: &Path) -> ResolvedPublicationPolicy {
    ClinkerToml::parse("")
        .expect("parse publication fixture")
        .storage
        .publication
        .resolve(destination_root, 1_024, 8_000_000_000)
        .expect("resolve publication fixture")
}

fn registration(
    kind: ArtifactKind,
    root: &Path,
    leaf: &str,
    producer: &str,
) -> ArtifactRegistration {
    ArtifactRegistration::new(
        kind,
        producer,
        leaf,
        validated(root, leaf),
        PromotionDisposition::Replace,
    )
    .expect("valid artifact registration")
}

/// Build a two-artifact run attempt whose staged bytes are ready to publish.
fn ready_run(root: &Path, registry: &OutputStagingRegistry) -> AttemptPublication {
    let registrations = vec![
        registration(ArtifactKind::Primary, root, "first.bin", "first"),
        registration(ArtifactKind::Sidecar, root, "second.bin", "second"),
    ];
    let (mut attempt, mut writers) = AttemptPublication::create_run(
        direct_policy(root),
        registry,
        EXECUTION_ID,
        1_000,
        301_000,
        registrations,
    )
    .expect("create run attempt");
    for writer in &mut writers {
        writer.file_mut().write_all(b"artifact").expect("write");
        attempt
            .mark_ready(writer.artifact_id())
            .expect("mark artifact ready");
    }
    drop(writers);
    attempt
}

#[test]
fn publication_failure_separates_visible_unsynchronized_from_published() {
    let root = tempfile::tempdir().expect("destination");
    let registry = OutputStagingRegistry::default();
    let mut attempt = ready_run(root.path(), &registry);
    attempt.set_fault_for_testing(AttemptFault::DirectorySyncThenManifestReplace);

    let failure = attempt
        .publish_run(&registry, &ShutdownToken::detached())
        .expect_err("terminal manifest write must fail");

    // The first rename landed but its parent directory was never synchronized,
    // so calling it published would tell a supervisor the bytes are durable.
    assert_eq!(
        failure
            .outcome()
            .artifacts()
            .iter()
            .map(|artifact| artifact.state())
            .collect::<Vec<_>>(),
        [
            ArtifactState::VisibleUnsynchronized,
            ArtifactState::Unpublished
        ]
    );
    assert!(root.path().join("first.bin").is_file());
    assert!(!root.path().join("second.bin").exists());
}

#[test]
fn publication_failure_reports_cleanup_debt_owed_by_earlier_promotions() {
    let root = tempfile::tempdir().expect("destination");
    let registry = OutputStagingRegistry::default();
    let mut attempt = ready_run(root.path(), &registry);
    attempt.set_fault_for_testing(AttemptFault::CleanupThenPromotionInterrupted);

    let failure = attempt
        .publish_run(&registry, &ShutdownToken::detached())
        .expect_err("second promotion must fail");

    let AttemptPublicationOutcome::Incomplete {
        cleanup_debt_count, ..
    } = failure.outcome()
    else {
        panic!("interrupted publication must report an incomplete outcome");
    };
    // The first artifact published and leaked its staged file; reporting zero
    // debt tells a supervisor there is nothing left to reclaim.
    assert_eq!(*cleanup_debt_count, 1);
    assert_eq!(
        failure
            .outcome()
            .artifacts()
            .iter()
            .map(|artifact| artifact.state())
            .collect::<Vec<_>>(),
        [ArtifactState::Published, ArtifactState::Unpublished]
    );
    assert!(root.path().join("first.bin").is_file());
    assert!(!root.path().join("second.bin").exists());
}
