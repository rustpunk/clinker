use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

use clinker_exec::output::attempt::{
    ARTIFACT_MAX_ENCODED_BYTES, ArtifactKind, ArtifactManifest, ArtifactRegistration,
    ArtifactState, AttemptFault, AttemptManifest, AttemptPublication, AttemptState,
    AttemptTestStage, CleanupDisposition, MANIFEST_MAX_ARTIFACTS, MANIFEST_MAX_BYTES,
    PUBLICATION_COPY_BUFFER_BYTES, SanitizedPathOptIn,
};
use clinker_exec::output::containment::PromotionDisposition;
use clinker_exec::output::staging::{OutputStagingRegistry, PublicationOutcome};
use clinker_exec::pipeline::shutdown::ShutdownToken;
use clinker_plan::config::{ClinkerToml, PublicationMode, ResolvedPublicationPolicy};
use clinker_plan::security::{ValidatedPath, validate_path};

const EXECUTION_ID: &str = "018f47a2-9a41-7a27-b4d6-4f7137e3c159";

fn validated(root: &Path, relative: &str) -> ValidatedPath {
    validate_path(Path::new(relative), root, false).expect("fixture path should validate")
}

fn begin(root: &Path) -> AttemptPublication {
    AttemptPublication::create(validated(root, "."), EXECUTION_ID, 1_000, 301_000)
        .expect("attempt should be created")
}

fn resolved_policy(
    destination_root: &Path,
    mode: PublicationMode,
    spool: Option<&Path>,
    estimated_attempt_bytes: u64,
) -> ResolvedPublicationPolicy {
    let config = match (mode, spool) {
        (PublicationMode::Direct, _) => String::new(),
        (PublicationMode::LocalThenPublish, Some(spool)) => format!(
            "[storage.publication]\nmode = \"local_then_publish\"\nlocal_spool_dir = \"{}\"\n",
            spool.display().to_string().replace('\\', "\\\\")
        ),
        (PublicationMode::LocalThenPublish, None) => {
            panic!("local_then_publish fixture requires a spool")
        }
    };
    ClinkerToml::parse(&config)
        .expect("parse publication fixture")
        .storage
        .publication
        .resolve(destination_root, estimated_attempt_bytes, 8_000_000_000)
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

#[test]
fn run_attempt_owns_every_artifact_kind_across_bounded_destination_roots() {
    let first = tempfile::tempdir().expect("first destination");
    let second = tempfile::tempdir().expect("second destination");
    let registry = OutputStagingRegistry::default();
    let policy = resolved_policy(first.path(), PublicationMode::Direct, None, 1_024);
    let registrations = vec![
        registration(
            ArtifactKind::Primary,
            first.path(),
            "primary.bin",
            "primary",
        ),
        registration(ArtifactKind::FanOut, first.path(), "fan.bin", "fan-out"),
        registration(ArtifactKind::Split, second.path(), "split.bin", "split"),
        registration(
            ArtifactKind::Dlq,
            second.path(),
            "errors.bin",
            "dead-letter",
        ),
        registration(
            ArtifactKind::Sidecar,
            second.path(),
            "result.meta.json",
            "sidecar",
        ),
    ];

    let (mut attempt, mut writers) = AttemptPublication::create_run(
        policy,
        &registry,
        EXECUTION_ID,
        1_000,
        301_000,
        registrations,
    )
    .expect("create run attempt");

    assert_eq!(attempt.execution_id(), EXECUTION_ID);
    assert_eq!(attempt.destination_root_count(), 2);
    assert_eq!(
        attempt.registered_kinds(),
        vec![
            ArtifactKind::Primary,
            ArtifactKind::FanOut,
            ArtifactKind::Split,
            ArtifactKind::Dlq,
            ArtifactKind::Sidecar,
        ]
    );
    for (index, writer) in writers.iter_mut().enumerate() {
        write!(writer.file_mut(), "artifact {index}").expect("write artifact");
        attempt
            .mark_ready(writer.artifact_id())
            .expect("mark artifact ready");
    }
    assert!(
        writers
            .iter()
            .all(|writer| writer.execution_id() == EXECUTION_ID)
    );
    drop(writers);

    let outcome = attempt
        .publish_run(&registry, &ShutdownToken::detached())
        .expect("publish run attempt")
        .expect("publication gate won");
    assert!(outcome.is_complete());
    assert_eq!(outcome.artifacts().len(), 5);
    assert!(
        outcome
            .artifacts()
            .iter()
            .all(|artifact| artifact.state() == ArtifactState::Published)
    );
    let rendered = format!("{outcome:?}");
    assert!(!rendered.contains(&first.path().display().to_string()));
    assert!(!rendered.contains(&second.path().display().to_string()));
    assert_eq!(
        attempt
            .physical_paths_for_sanitized_output(SanitizedPathOptIn)
            .len(),
        5
    );
    for (root, leaf) in [
        (first.path(), "primary.bin"),
        (first.path(), "fan.bin"),
        (second.path(), "split.bin"),
        (second.path(), "errors.bin"),
        (second.path(), "result.meta.json"),
    ] {
        assert!(root.join(leaf).is_file(), "missing {leaf}");
    }
}

#[test]
fn duplicate_run_registration_refuses_before_attempt_creation() {
    let root = tempfile::tempdir().expect("destination");
    let registry = OutputStagingRegistry::default();
    let policy = resolved_policy(root.path(), PublicationMode::Direct, None, 1_024);
    let registrations = vec![
        registration(ArtifactKind::Primary, root.path(), "same.bin", "primary"),
        registration(ArtifactKind::Sidecar, root.path(), "same.bin", "sidecar"),
    ];

    let error = AttemptPublication::create_run(
        policy,
        &registry,
        EXECUTION_ID,
        1_000,
        301_000,
        registrations,
    )
    .expect_err("duplicate final must fail");

    assert!(error.to_string().contains("collision"), "{error}");
    assert!(!root.path().join(".clinker-attempts").exists());
}

#[test]
fn local_then_publish_copies_in_bounded_chunks_and_verifies_destination() {
    let destination = tempfile::tempdir().expect("destination");
    let spool = tempfile::tempdir().expect("local spool");
    let registry = OutputStagingRegistry::default();
    let body = vec![0x5a; PUBLICATION_COPY_BUFFER_BYTES * 2 + 17];
    let policy = resolved_policy(
        destination.path(),
        PublicationMode::LocalThenPublish,
        Some(spool.path()),
        body.len() as u64,
    );
    let registrations = vec![registration(
        ArtifactKind::Primary,
        destination.path(),
        "result.bin",
        "primary",
    )];
    let (mut attempt, mut writers) = AttemptPublication::create_run(
        policy,
        &registry,
        EXECUTION_ID,
        1_000,
        301_000,
        registrations,
    )
    .expect("create local-then-publish attempt");
    let writer = writers.first_mut().expect("writer");
    writer
        .file_mut()
        .write_all(&body)
        .expect("write local bytes");
    let artifact_id = writer.artifact_id().to_owned();
    drop(writers);
    let local_artifact = spool
        .path()
        .join(".clinker-attempts")
        .join(EXECUTION_ID)
        .join(&artifact_id);
    let destination_artifact = destination
        .path()
        .join(".clinker-attempts")
        .join(EXECUTION_ID)
        .join(&artifact_id);
    assert!(local_artifact.is_file());
    assert!(!destination_artifact.exists());

    attempt.mark_ready(&artifact_id).expect("copy and verify");
    assert!(
        !local_artifact.exists(),
        "spool copy is released only after destination ownership"
    );
    assert_eq!(std::fs::read(&destination_artifact).unwrap(), body);

    let outcome = attempt
        .publish_run(&registry, &ShutdownToken::detached())
        .expect("publish")
        .expect("publication gate won");
    assert!(outcome.is_complete());
    assert_eq!(
        std::fs::read(destination.path().join("result.bin")).unwrap(),
        body
    );
}

#[test]
fn local_then_publish_copy_and_digest_failures_never_fallback_to_direct() {
    for fault in [
        AttemptFault::Copy,
        AttemptFault::DestinationFileSync,
        AttemptFault::Digest,
    ] {
        let destination = tempfile::tempdir().expect("destination");
        let spool = tempfile::tempdir().expect("local spool");
        let registry = OutputStagingRegistry::default();
        let policy = resolved_policy(
            destination.path(),
            PublicationMode::LocalThenPublish,
            Some(spool.path()),
            64,
        );
        let registrations = vec![registration(
            ArtifactKind::Primary,
            destination.path(),
            "result.bin",
            "primary",
        )];
        let (mut attempt, mut writers) = AttemptPublication::create_run(
            policy,
            &registry,
            EXECUTION_ID,
            1_000,
            301_000,
            registrations,
        )
        .expect("create attempt");
        let artifact_id = writers[0].artifact_id().to_owned();
        writers[0]
            .file_mut()
            .write_all(b"failure boundary")
            .expect("write spool");
        drop(writers);
        attempt.set_fault_for_testing(fault);

        assert!(attempt.mark_ready(&artifact_id).is_err(), "fault {fault:?}");
        assert!(!destination.path().join("result.bin").exists());
        assert!(
            spool
                .path()
                .join(".clinker-attempts")
                .join(EXECUTION_ID)
                .join(&artifact_id)
                .exists(),
            "failed {fault:?} must retain the local source"
        );
    }
}

fn stage_ready(
    attempt: &mut AttemptPublication,
    registry: &OutputStagingRegistry,
    root: &Path,
    body: &[u8],
) -> String {
    let (artifact_id, mut file) = attempt
        .stage_direct(
            registry,
            validated(root, "result.bin"),
            "primary-output",
            "result.bin",
            PromotionDisposition::Replace,
        )
        .expect("direct artifact should stage");
    file.write_all(body).expect("artifact write");
    drop(file);
    attempt
        .mark_ready(&artifact_id)
        .expect("artifact should become ready");
    artifact_id
}

#[test]
fn direct_artifact_persists_complete_before_immediate_owned_cleanup() {
    let root = tempfile::tempdir().expect("temporary destination");
    let registry = OutputStagingRegistry::default();
    let mut attempt = begin(root.path());
    let manifest_path = attempt.manifest_path().to_path_buf();
    let attempt_root = attempt.attempt_root().to_path_buf();
    let artifact_id = stage_ready(&mut attempt, &registry, root.path(), b"complete artifact");

    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    attempt.install_test_hook(move |stage| {
        assert_eq!(stage.execution_id, EXECUTION_ID);
        assert_eq!(stage.stage, AttemptTestStage::CompleteBeforeCleanup);
        ready_tx.send(()).expect("signal durable complete");
        release_rx.recv().expect("release cleanup");
    });

    let handle = std::thread::spawn(move || attempt.publish(&registry, &ShutdownToken::detached()));
    ready_rx.recv().expect("complete barrier should be reached");

    let durable = AttemptManifest::read(&manifest_path, 1_000)
        .expect("durable complete manifest should be readable at the barrier");
    assert_eq!(durable.state(), AttemptState::Complete);
    assert_eq!(durable.execution_id(), EXECUTION_ID);
    assert_eq!(durable.artifact_count(), 1);
    assert_eq!(durable.total_bytes(), 17);
    assert_eq!(durable.artifacts()[0].artifact_id(), artifact_id);
    assert_eq!(durable.artifacts()[0].state(), ArtifactState::Published);
    assert_eq!(
        std::fs::read(root.path().join("result.bin")).unwrap(),
        b"complete artifact"
    );

    release_tx.send(()).expect("release cleanup");
    let outcome = handle
        .join()
        .expect("publication thread")
        .expect("publication")
        .expect("publication gate won");
    assert!(matches!(outcome, PublicationOutcome::Complete { .. }));
    assert!(root.path().join("result.bin").is_file());
    assert!(!manifest_path.exists());
    assert!(!attempt_root.exists());
}

#[test]
fn cancellation_before_gate_records_abandoned_without_exposing_a_final() {
    let root = tempfile::tempdir().expect("temporary destination");
    let registry = OutputStagingRegistry::default();
    let mut attempt = begin(root.path());
    stage_ready(&mut attempt, &registry, root.path(), b"cancelled bytes");
    let token = ShutdownToken::detached();
    token.request();

    assert!(
        attempt
            .publish(&registry, &token)
            .expect("cancellation state")
            .is_none()
    );
    assert!(!root.path().join("result.bin").exists());
    let retained = AttemptManifest::read(attempt.manifest_path(), 1_000)
        .expect("abandoned manifest should remain inspectable");
    assert_eq!(retained.state(), AttemptState::Abandoned);
    assert_eq!(retained.artifacts()[0].state(), ArtifactState::Ready);
}

#[test]
fn a_late_cancellation_cannot_rewrite_published_truth() {
    let root = tempfile::tempdir().expect("temporary destination");
    let registry = OutputStagingRegistry::default();
    let mut attempt = begin(root.path());
    stage_ready(&mut attempt, &registry, root.path(), b"published bytes");
    let token = ShutdownToken::detached();
    assert!(token.try_begin_publication());
    token.request();

    let outcome = attempt
        .publish(&registry, &token)
        .expect("publication")
        .expect("publication already owns the gate");
    assert!(outcome.is_complete());
    assert_eq!(
        std::fs::read(root.path().join("result.bin")).unwrap(),
        b"published bytes"
    );
}

#[test]
fn injected_boundaries_leave_exact_non_success_state() {
    for (fault, expected_state, final_visible) in [
        (AttemptFault::Write, ArtifactState::Staging, false),
        (AttemptFault::FileSync, ArtifactState::Staging, false),
        (AttemptFault::ManifestReplace, ArtifactState::Staging, false),
        (
            AttemptFault::BeforeRename,
            ArtifactState::Unpublished,
            false,
        ),
        (
            AttemptFault::DirectorySync,
            ArtifactState::VisibleUnsynchronized,
            true,
        ),
    ] {
        let root = tempfile::tempdir().expect("temporary destination");
        let registry = OutputStagingRegistry::default();
        let mut attempt = begin(root.path());
        let (artifact_id, mut file) = attempt
            .stage_direct(
                &registry,
                validated(root.path(), "result.bin"),
                "primary-output",
                "result.bin",
                PromotionDisposition::Replace,
            )
            .expect("stage artifact");
        file.write_all(b"fault boundary").expect("write fixture");
        drop(file);
        attempt.set_fault_for_testing(fault);

        if matches!(
            fault,
            AttemptFault::Write | AttemptFault::FileSync | AttemptFault::ManifestReplace
        ) {
            assert!(attempt.mark_ready(&artifact_id).is_err());
        } else {
            attempt
                .mark_ready(&artifact_id)
                .expect("ready before publication fault");
            let outcome = attempt
                .publish(&registry, &ShutdownToken::detached())
                .expect("typed incomplete outcome")
                .expect("publication gate won");
            assert!(matches!(outcome, PublicationOutcome::Incomplete { .. }));
        }

        let retained = AttemptManifest::read(attempt.manifest_path(), 1_000)
            .expect("retained manifest should parse");
        assert_ne!(retained.state(), AttemptState::Complete);
        assert_eq!(
            retained.artifacts()[0].state(),
            expected_state,
            "fault {fault:?}"
        );
        assert_eq!(root.path().join("result.bin").exists(), final_visible);
    }
}

fn artifact(
    artifact_id: &str,
    producer_label: &str,
    logical_leaf: &str,
    size_bytes: u64,
) -> ArtifactManifest {
    ArtifactManifest::new(
        artifact_id,
        producer_label,
        logical_leaf,
        size_bytes,
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        ArtifactState::Ready,
    )
    .expect("valid artifact")
}

#[test]
fn manifest_is_compact_canonical_bounded_and_strict() {
    let manifest = AttemptManifest::new(
        EXECUTION_ID,
        1_000,
        301_000,
        AttemptState::Ready,
        vec![artifact("artifact-00000001", "out", "result.bin", 7)],
    )
    .expect("valid manifest");
    let bytes = manifest.to_bytes().expect("canonical bytes");
    assert!(!bytes.ends_with(b"\n"));
    assert_eq!(
        AttemptManifest::from_bytes(&bytes, 1_000).unwrap(),
        manifest
    );

    let canonical = String::from_utf8(bytes).expect("UTF-8 manifest");
    assert!(canonical.starts_with(
        r#"{"schema":"clinker.attempt-manifest/v1","execution_id":"018f47a2-9a41-7a27-b4d6-4f7137e3c159","created_unix_ms":1000,"eligible_after_unix_ms":301000,"state":"ready","artifact_count":1,"total_bytes":7,"artifacts":[{"artifact_id":"artifact-00000001","producer_label":"out","logical_leaf":"result.bin","size_bytes":7,"blake3_hex":"#
    ));

    let invalid_documents = [
        canonical.replacen(
            r#""state":"ready""#,
            r#""state":"ready","state":"ready""#,
            1,
        ),
        canonical.replacen(
            r#""schema":"clinker.attempt-manifest/v1""#,
            r#""schema":"unsupported""#,
            1,
        ),
        canonical.replacen(r#""artifact_count":1"#, r#""artifact_count":2"#, 1),
        canonical.replacen(r#""total_bytes":7"#, r#""total_bytes":8"#, 1),
        canonical.replacen(r#""artifact-00000001""#, r#""artifact-0000000A""#, 1),
        format!(" {canonical}"),
        canonical.replacen(r#""execution_id""#, r#""unknown":0,"execution_id""#, 1),
    ];
    for invalid in invalid_documents {
        assert!(
            AttemptManifest::from_bytes(invalid.as_bytes(), 1_000).is_err(),
            "{invalid}"
        );
    }
    assert!(AttemptManifest::from_bytes(&[0xff, 0xfe], 1_000).is_err());
    assert!(AttemptManifest::from_bytes(b"{", 1_000).is_err());
}

#[test]
fn manifest_rejects_noncanonical_order_overflow_and_ambiguous_clocks() {
    let reversed = AttemptManifest::new(
        EXECUTION_ID,
        1_000,
        301_000,
        AttemptState::Ready,
        vec![
            artifact("artifact-00000002", "second", "second.bin", 2),
            artifact("artifact-00000001", "first", "first.bin", 1),
        ],
    );
    assert!(reversed.is_err());

    assert!(
        AttemptManifest::new(EXECUTION_ID, 2_000, 1_000, AttemptState::Ready, Vec::new(),).is_err()
    );
    let future_created =
        AttemptManifest::new(EXECUTION_ID, 2_000, 3_000, AttemptState::Ready, Vec::new())
            .unwrap()
            .to_bytes()
            .unwrap();
    assert!(AttemptManifest::from_bytes(&future_created, 1_000).is_err());

    let overflow = AttemptManifest::new(
        EXECUTION_ID,
        1_000,
        2_000,
        AttemptState::Ready,
        vec![
            artifact("artifact-00000001", "first", "first.bin", u64::MAX),
            artifact("artifact-00000002", "second", "second.bin", 1),
        ],
    );
    assert!(overflow.is_err());
}

#[test]
fn maximum_manifest_cardinality_stays_below_the_read_limit() {
    let producer = "\"".repeat(95);
    let logical_leaf = "\"".repeat(255);
    let artifacts = (0..MANIFEST_MAX_ARTIFACTS)
        .map(|index| {
            artifact(
                &format!("artifact-{index:08x}"),
                &producer,
                &logical_leaf,
                u64::MAX / MANIFEST_MAX_ARTIFACTS as u64,
            )
        })
        .collect::<Vec<_>>();
    for entry in &artifacts {
        assert!(entry.encoded_len().unwrap() <= ARTIFACT_MAX_ENCODED_BYTES);
    }
    let manifest =
        AttemptManifest::new(EXECUTION_ID, 1_000, 301_000, AttemptState::Ready, artifacts)
            .expect("maximum-cardinality manifest");
    let encoded = manifest.to_bytes().expect("bounded manifest");
    assert!(encoded.len() < MANIFEST_MAX_BYTES);

    let oversized = vec![b' '; MANIFEST_MAX_BYTES + 1];
    assert!(AttemptManifest::from_bytes(&oversized, 1_000).is_err());
}

#[test]
fn retries_are_fresh_and_incomplete_attempts_never_republish() {
    let root = tempfile::tempdir().expect("temporary destination");
    let registry = OutputStagingRegistry::default();
    let mut first = begin(root.path());
    stage_ready(&mut first, &registry, root.path(), b"first");
    first.set_fault_for_testing(AttemptFault::BeforeRename);
    let first_outcome = first
        .publish(&registry, &ShutdownToken::detached())
        .unwrap()
        .unwrap();
    assert!(matches!(
        first_outcome,
        PublicationOutcome::Incomplete { .. }
    ));
    assert!(
        first
            .publish(&registry, &ShutdownToken::detached())
            .is_err()
    );

    let second_id = "018f47a2-9a41-7a27-b4d6-4f7137e3c160";
    let second = AttemptPublication::create(validated(root.path(), "."), second_id, 2_000, 302_000)
        .expect("retry uses a fresh execution identity");
    assert_ne!(first.attempt_root(), second.attempt_root());
}

#[test]
fn cross_filesystem_promotion_has_no_visible_copy_fallback() {
    #[cfg(target_os = "linux")]
    {
        let destination_root = tempfile::tempdir().expect("destination root");
        let source_root = tempfile::tempdir_in("/dev/shm").expect("tmpfs source root");
        let source = source_root.path().join("artifact.bin");
        std::fs::write(&source, b"complete").expect("source bytes");
        let destination = validated(destination_root.path(), "result.bin");
        let source = validate_path(&source, Path::new("/"), true).expect("absolute fixture");
        let boundary = clinker_exec::output::containment::OutputContainment::for_profile(
            destination,
            "local-filesystem",
        )
        .expect("destination containment");
        assert!(
            boundary
                .promote_from(source, PromotionDisposition::Replace)
                .is_err()
        );
        assert!(!destination_root.path().join("result.bin").exists());
    }
}

#[test]
fn cleanup_keeps_live_unowned_malformed_and_unexpected_attempts() {
    let root = tempfile::tempdir().expect("temporary destination");
    let root_path = validated(root.path(), ".");

    let live = begin(root.path());
    assert_eq!(
        AttemptPublication::cleanup(root_path.clone(), EXECUTION_ID, 400_000)
            .expect("live cleanup inspection")
            .disposition(),
        CleanupDisposition::Kept
    );
    assert!(live.attempt_root().exists());
    drop(live);

    let malformed_id = "018f47a2-9a41-7a27-b4d6-4f7137e3c161";
    let malformed_root = root.path().join(".clinker-attempts").join(malformed_id);
    std::fs::create_dir(&malformed_root).expect("malformed attempt root");
    std::fs::write(malformed_root.join("live.lock"), b"").expect("orphan lock");
    std::fs::write(malformed_root.join("manifest.json"), b"{").expect("malformed manifest");
    assert_eq!(
        AttemptPublication::cleanup(root_path.clone(), malformed_id, 400_000)
            .expect("malformed cleanup inspection")
            .disposition(),
        CleanupDisposition::Kept
    );
    assert!(malformed_root.exists());

    let unowned_id = "018f47a2-9a41-7a27-b4d6-4f7137e3c162";
    let unowned_root = root.path().join(".clinker-attempts").join(unowned_id);
    std::fs::create_dir(&unowned_root).expect("unowned matching directory");
    std::fs::write(unowned_root.join("outside-canary"), b"unrelated").expect("unrelated data");
    assert_eq!(
        AttemptPublication::cleanup(root_path, unowned_id, 400_000)
            .expect("unowned cleanup inspection")
            .disposition(),
        CleanupDisposition::Kept
    );
    assert_eq!(
        std::fs::read(unowned_root.join("outside-canary")).unwrap(),
        b"unrelated"
    );
}

#[test]
fn anchored_cleanup_rejects_linked_artifacts_without_mutating_outside_data() {
    let root = tempfile::tempdir().expect("temporary destination");
    let outside = tempfile::tempdir().expect("outside directory");
    let outside_canary = outside.path().join("canary.bin");
    std::fs::write(&outside_canary, b"outside").expect("outside canary");
    let registry = OutputStagingRegistry::default();
    let mut attempt = begin(root.path());
    let artifact_id = stage_ready(&mut attempt, &registry, root.path(), b"owned");
    let attempt_root = attempt.attempt_root().to_path_buf();
    drop(attempt);
    std::fs::remove_file(attempt_root.join(&artifact_id)).expect("remove owned artifact fixture");

    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside_canary, attempt_root.join(&artifact_id))
        .expect("artifact symlink fixture");
    #[cfg(windows)]
    std::os::windows::fs::symlink_file(&outside_canary, attempt_root.join(&artifact_id))
        .expect("artifact symlink fixture");

    assert_eq!(
        AttemptPublication::cleanup(validated(root.path(), "."), EXECUTION_ID, 400_000)
            .expect("linked cleanup inspection")
            .disposition(),
        CleanupDisposition::Kept
    );
    assert_eq!(std::fs::read(outside_canary).unwrap(), b"outside");
    assert!(attempt_root.exists());
}

#[test]
fn attempt_creation_and_cleanup_reject_replaced_directory_components() {
    let root = tempfile::tempdir().expect("temporary destination");
    let outside = tempfile::tempdir().expect("outside directory");
    let outside_canary = outside.path().join("canary.bin");
    std::fs::write(&outside_canary, b"outside").expect("outside canary");
    let namespace = root.path().join(".clinker-attempts");

    #[cfg(unix)]
    std::os::unix::fs::symlink(outside.path(), &namespace).expect("namespace symlink fixture");
    #[cfg(windows)]
    {
        let status = std::process::Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(&namespace)
            .arg(outside.path())
            .status()
            .expect("namespace junction command");
        assert!(status.success(), "namespace junction fixture");
    }

    assert!(
        AttemptPublication::create(validated(root.path(), "."), EXECUTION_ID, 1_000, 301_000)
            .is_err()
    );
    assert_eq!(std::fs::read(&outside_canary).unwrap(), b"outside");
    assert!(!outside.path().join(EXECUTION_ID).exists());

    let inspection =
        AttemptPublication::cleanup(validated(root.path(), "."), EXECUTION_ID, 400_000)
            .expect("replaced namespace is conservatively inspected");
    assert_eq!(inspection.disposition(), CleanupDisposition::Kept);
    assert_eq!(std::fs::read(outside_canary).unwrap(), b"outside");
}

#[test]
fn orphan_cleanup_is_metadata_last_and_idempotent() {
    let root = tempfile::tempdir().expect("temporary destination");
    let registry = OutputStagingRegistry::default();
    let mut attempt = begin(root.path());
    stage_ready(&mut attempt, &registry, root.path(), b"orphaned");
    let attempt_root = attempt.attempt_root().to_path_buf();
    drop(attempt);

    let first = AttemptPublication::cleanup(validated(root.path(), "."), EXECUTION_ID, 400_000)
        .expect("orphan cleanup");
    assert_eq!(first.disposition(), CleanupDisposition::Removed);
    assert_eq!(first.execution_id(), EXECUTION_ID);
    assert!(!attempt_root.exists());

    let repeated = AttemptPublication::cleanup(validated(root.path(), "."), EXECUTION_ID, 400_000)
        .expect("idempotent cleanup");
    assert_eq!(repeated.disposition(), CleanupDisposition::AlreadyAbsent);
}

#[allow(dead_code)]
fn _assert_send_paths(_: PathBuf) {}
