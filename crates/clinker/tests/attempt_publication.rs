use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

use clinker_exec::output::attempt::{
    ARTIFACT_MAX_ENCODED_BYTES, ATTEMPT_EDGE_OUTCOME_TAXONOMY, ATTEMPT_PUBLICATION_PROHIBITIONS,
    ArtifactKind, ArtifactManifest, ArtifactRegistration, ArtifactState, AttemptContinuation,
    AttemptFault, AttemptManifest, AttemptPublication, AttemptQuery, AttemptState,
    AttemptTestStage, CleanupDebtKind, CleanupDisposition, MANIFEST_MAX_ARTIFACTS,
    MANIFEST_MAX_BYTES, PUBLICATION_COPY_BUFFER_BYTES, PurgeDisposition, SanitizedPathOptIn,
};
use clinker_exec::output::containment::PromotionDisposition;
use clinker_exec::output::staging::{OutputStagingRegistry, PublicationOutcome};
use clinker_exec::pipeline::shutdown::ShutdownToken;
use clinker_plan::config::{
    ClinkerToml, CompileContext, PublicationMode, ResolvedPublicationPolicy, load_config_from_str,
};
use clinker_plan::plan::CompiledPlan;
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

fn bounded_policy(
    destination_root: &Path,
    failed_retention_seconds: u64,
    entry_limit: u64,
    byte_limit: u64,
    time_limit_ms: u64,
) -> ResolvedPublicationPolicy {
    let config = format!(
        "[storage.publication]\nfailed_retention_seconds = {failed_retention_seconds}\nsweep_entry_limit = {entry_limit}\nsweep_byte_limit = \"{byte_limit}B\"\nsweep_time_limit_ms = {time_limit_ms}\n"
    );
    ClinkerToml::parse(&config)
        .expect("parse bounded publication fixture")
        .storage
        .publication
        .resolve(destination_root, 1, 8_000_000_000)
        .expect("resolve bounded publication fixture")
}

fn compiled_plan(name: &str) -> CompiledPlan {
    let yaml = format!(
        r#"pipeline:
  name: {name}
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: input.csv
      schema: [{{ name: id, type: int }}]
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: output.csv
"#
    );
    load_config_from_str(&yaml)
        .expect("load query plan")
        .compile(&CompileContext::default())
        .expect("compile query plan")
}

fn query(root: &Path, policy: &ResolvedPublicationPolicy, name: &str) -> AttemptQuery {
    AttemptQuery::new(&compiled_plan(name), policy, vec![validated(root, ".")])
        .expect("construct attempt query")
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
    assert_eq!(std::fs::read(&outside_canary).unwrap(), b"outside");
    assert!(attempt_root.exists());

    let policy = bounded_policy(root.path(), 0, 1_000, 8_000_000_000, 2_000);
    let query = query(root.path(), &policy, "linked_purge_refusal");
    let root_id = query.owned_root_ids()[0].to_owned();
    let request = query
        .purge_execution(&root_id, EXECUTION_ID)
        .expect("typed linked-attempt selector");
    let report = query
        .execute(&request, 400_000, None, &ShutdownToken::detached())
        .expect("linked artifact remains a keep outcome");
    assert_eq!(report.disposition(), PurgeDisposition::Kept);
    assert_eq!(std::fs::read(outside_canary).unwrap(), b"outside");
    assert!(attempt_root.join("manifest.json").exists());
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

#[test]
fn bounded_attempt_listing_stops_at_exact_entry_byte_and_monotonic_time_limits() {
    let root = tempfile::tempdir().expect("temporary destination");
    let attempt = begin(root.path());
    drop(attempt);

    let entry_policy = bounded_policy(root.path(), 86_400, 1, 8_000_000_000, 2_000);
    let entry_query = query(root.path(), &entry_policy, "entry_budget");
    let root_id = entry_query.owned_root_ids()[0].to_owned();
    let entry_page = entry_query
        .list(&root_id, 400_000, None)
        .expect("bounded entry listing");
    assert_eq!(entry_page.considered_entries(), 1);
    assert!(entry_page.considered_bytes() <= entry_policy.sweep_byte_limit());
    assert!(entry_page.continuation().is_some());
    assert!(
        entry_page
            .cleanup_debt()
            .iter()
            .any(|debt| debt.kind() == CleanupDebtKind::EntryBudget)
    );

    let byte_policy = bounded_policy(root.path(), 86_400, 1_000, 1, 2_000);
    let byte_query = query(root.path(), &byte_policy, "byte_budget");
    let root_id = byte_query.owned_root_ids()[0].to_owned();
    let byte_page = byte_query
        .list(&root_id, 400_000, None)
        .expect("bounded byte listing");
    assert!(byte_page.considered_bytes() <= 1);
    assert!(
        byte_page
            .cleanup_debt()
            .iter()
            .any(|debt| debt.kind() == CleanupDebtKind::ByteBudget)
    );

    let time_policy = bounded_policy(root.path(), 86_400, 1_000, 8_000_000_000, 2);
    let time_query = query(root.path(), &time_policy, "time_budget");
    let root_id = time_query.owned_root_ids()[0].to_owned();
    let mut ticks = [0, 0, 2, 2].into_iter();
    let time_page = time_query
        .list_with_elapsed(&root_id, 400_000, None, || ticks.next().unwrap_or(2))
        .expect("deterministic monotonic time listing");
    assert_eq!(time_page.elapsed_ms(), 2);
    assert!(
        time_page
            .cleanup_debt()
            .iter()
            .any(|debt| debt.kind() == CleanupDebtKind::TimeBudget)
    );
}

#[test]
fn continuation_is_versioned_plan_root_selector_bound_and_single_use() {
    let first = tempfile::tempdir().expect("first destination");
    let second = tempfile::tempdir().expect("second destination");
    let first_attempt = begin(first.path());
    drop(first_attempt);
    let policy = bounded_policy(first.path(), 86_400, 1, 8_000_000_000, 2_000);
    let query = AttemptQuery::new(
        &compiled_plan("continuation_binding"),
        &policy,
        vec![validated(first.path(), "."), validated(second.path(), ".")],
    )
    .expect("construct multi-root query");
    let roots = query
        .owned_root_ids()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let (first_root, page) = roots
        .iter()
        .find_map(|root_id| {
            let page = query.list(root_id, 400_000, None).ok()?;
            if page.continuation().is_some() {
                Some((root_id.to_owned(), page))
            } else {
                None
            }
        })
        .expect("one owned root contains the bounded attempt");
    let second_root = roots
        .into_iter()
        .find(|root_id| root_id != &first_root)
        .expect("second owned root");
    let continuation = page.continuation().expect("entry budget continuation");
    let bytes = continuation.to_bytes().expect("canonical continuation");
    let decoded = AttemptContinuation::from_bytes(&bytes).expect("versioned continuation");

    assert!(query.list(&second_root, 400_000, Some(&decoded)).is_err());

    let altered_plan = AttemptQuery::new(
        &compiled_plan("altered_continuation_binding"),
        &policy,
        vec![validated(first.path(), ".")],
    )
    .expect("construct altered-plan query");
    let altered_root = altered_plan.owned_root_ids()[0].to_owned();
    assert!(
        altered_plan
            .list(&altered_root, 400_000, Some(&decoded))
            .is_err()
    );

    let altered_cursor = String::from_utf8(bytes.clone())
        .expect("continuation UTF-8")
        .replacen(EXECUTION_ID, "018f47a2-9a41-7a27-b4d6-4f7137e3c158", 1);
    assert!(AttemptContinuation::from_bytes(altered_cursor.as_bytes()).is_err());

    let stale = String::from_utf8(bytes)
        .expect("continuation UTF-8")
        .replacen("clinker.attempt-continuation/v1", "unsupported", 1);
    assert!(AttemptContinuation::from_bytes(stale.as_bytes()).is_err());

    query
        .list(&first_root, 400_000, Some(&decoded))
        .expect("first continuation use");
    assert!(query.list(&first_root, 400_000, Some(&decoded)).is_err());
}

#[test]
fn inspection_is_path_free_and_reports_owned_manifest_truth_and_ambiguity() {
    let root = tempfile::tempdir().expect("temporary destination");
    let registry = OutputStagingRegistry::default();
    let mut attempt = begin(root.path());
    let artifact_id = stage_ready(&mut attempt, &registry, root.path(), b"retained bytes");
    let attempt_path = attempt.attempt_root().to_path_buf();
    drop(attempt);
    let policy = bounded_policy(root.path(), 86_400, 1_000, 8_000_000_000, 2_000);
    let query = query(root.path(), &policy, "inspect_truth");
    let root_id = query.owned_root_ids()[0].to_owned();

    let inspection = query
        .inspect(&root_id, EXECUTION_ID, 400_000)
        .expect("inspect retained attempt");
    assert_eq!(inspection.execution_id(), EXECUTION_ID);
    assert_eq!(inspection.state(), Some(AttemptState::Ready));
    assert_eq!(inspection.created_unix_ms(), Some(1_000));
    assert_eq!(inspection.eligible_after_unix_ms(), Some(301_000));
    assert_eq!(inspection.artifact_ids(), &[artifact_id]);
    assert!(inspection.cleanup_debt().is_empty());
    assert!(!format!("{inspection:?}").contains(&attempt_path.display().to_string()));
    assert_eq!(
        inspection.physical_path_for_sanitized_output(SanitizedPathOptIn),
        Some(attempt_path.as_path())
    );

    std::fs::write(attempt_path.join("unknown-child"), b"unrelated")
        .expect("unknown child fixture");
    let ambiguous = query
        .inspect(&root_id, EXECUTION_ID, 400_000)
        .expect("ambiguous inspection remains reportable");
    assert_eq!(ambiguous.disposition(), CleanupDisposition::Kept);
    assert!(
        ambiguous
            .cleanup_debt()
            .iter()
            .any(|debt| debt.kind() == CleanupDebtKind::UnknownChild)
    );
    assert!(attempt_path.join("unknown-child").exists());
}

#[test]
fn malformed_linked_and_rollback_clock_attempts_are_kept_and_reported() {
    let root = tempfile::tempdir().expect("temporary destination");
    let malformed_id = "018f47a2-9a41-7a27-b4d6-4f7137e3c161";
    let malformed_root = root.path().join(".clinker-attempts").join(malformed_id);
    std::fs::create_dir_all(&malformed_root).expect("malformed attempt root");
    std::fs::write(malformed_root.join("live.lock"), b"").expect("lock fixture");
    std::fs::write(malformed_root.join("manifest.json"), b"{").expect("malformed fixture");
    let policy = bounded_policy(root.path(), 86_400, 1_000, 8_000_000_000, 2_000);
    let query = query(root.path(), &policy, "ambiguous_truth");
    let root_id = query.owned_root_ids()[0].to_owned();

    let malformed = query
        .inspect(&root_id, malformed_id, 400_000)
        .expect("malformed attempt is a keep outcome");
    assert!(
        malformed
            .cleanup_debt()
            .iter()
            .any(|debt| debt.kind() == CleanupDebtKind::InvalidManifest)
    );
    let list = query
        .list(&root_id, 400_000, None)
        .expect("malformed ownership remains path-free list debt");
    assert!(list.entries().is_empty());
    assert!(
        list.cleanup_debt()
            .iter()
            .any(|debt| debt.kind() == CleanupDebtKind::InvalidManifest)
    );

    let future_id = "018f47a2-9a41-7a27-b4d6-4f7137e3c162";
    let future = AttemptPublication::create(validated(root.path(), "."), future_id, 1_000, 301_000)
        .expect("future-clock fixture");
    drop(future);
    let rollback = query
        .inspect(&root_id, future_id, 500)
        .expect("rollback clock is a keep outcome");
    assert!(
        rollback
            .cleanup_debt()
            .iter()
            .any(|debt| debt.kind() == CleanupDebtKind::ClockAmbiguous)
    );

    let absent = query
        .inspect(&root_id, EXECUTION_ID, 500)
        .expect("missing attempt is still path-confined");
    assert_eq!(absent.disposition(), CleanupDisposition::AlreadyAbsent);
    assert!(malformed_root.exists());
}

#[test]
fn failed_retention_zero_and_default_are_exact_without_weakening_clock_checks() {
    let root = tempfile::tempdir().expect("temporary destination");
    let zero = bounded_policy(root.path(), 0, 1_000, 8_000_000_000, 2_000);
    let default = resolved_policy(root.path(), PublicationMode::Direct, None, 1);
    assert_eq!(zero.failed_retention_seconds(), 0);
    assert_eq!(default.failed_retention_seconds(), 86_400);
    let over_cap = ClinkerToml::parse("[storage.publication]\nfailed_retention_seconds = 604801\n")
        .expect("parse over-cap fixture")
        .storage
        .publication
        .resolve(root.path(), 1, 8_000_000_000);
    assert!(over_cap.is_err(), "the seven-day retention cap is exact");

    let manifest = AttemptManifest::new(
        EXECUTION_ID,
        1_000,
        301_000,
        AttemptState::Abandoned,
        Vec::new(),
    )
    .expect("abandoned manifest");
    assert!(manifest.to_bytes().is_ok());
    assert!(AttemptManifest::from_bytes(&manifest.to_bytes().unwrap(), 999).is_err());

    let attempt = begin(root.path());
    let manifest_path = attempt.manifest_path().to_path_buf();
    drop(attempt);
    std::fs::write(&manifest_path, manifest.to_bytes().unwrap()).expect("abandoned fixture");
    let zero_query = query(root.path(), &zero, "zero_retention");
    let root_id = zero_query.owned_root_ids()[0].to_owned();
    let inspection = zero_query
        .inspect(&root_id, EXECUTION_ID, 1_000)
        .expect("zero-retention inspection");
    assert!(inspection.is_eligible());
    let default_query = query(root.path(), &default, "default_retention");
    let default_root = default_query.owned_root_ids()[0].to_owned();
    assert!(
        !default_query
            .inspect(&default_root, EXECUTION_ID, 400_000)
            .expect("default-retention inspection")
            .is_eligible()
    );
    assert!(
        !default_query
            .inspect(&default_root, EXECUTION_ID, 86_400_999)
            .expect("one millisecond before default retention")
            .is_eligible()
    );
    assert!(
        default_query
            .inspect(&default_root, EXECUTION_ID, 86_401_000)
            .expect("exact default retention boundary")
            .is_eligible()
    );
}

#[test]
fn purge_preview_by_execution_or_expiry_is_bounded_and_non_mutating() {
    let root = tempfile::tempdir().expect("temporary destination");
    let registry = OutputStagingRegistry::default();
    let mut attempt = begin(root.path());
    let artifact_id = stage_ready(&mut attempt, &registry, root.path(), b"preview only");
    let attempt_root = attempt.attempt_root().to_path_buf();
    drop(attempt);
    let policy = bounded_policy(root.path(), 86_400, 1_000, 8_000_000_000, 2_000);
    let query = query(root.path(), &policy, "purge_preview");
    let root_id = query.owned_root_ids()[0].to_owned();

    let exact = query
        .purge_execution(&root_id, EXECUTION_ID)
        .expect("typed execution purge selector");
    let exact_preview = query
        .preview(&exact, 400_000, None)
        .expect("preview exact execution");
    assert_eq!(exact_preview.selected_execution_ids(), &[EXECUTION_ID]);
    assert!(exact_preview.cleanup_debt().is_empty());

    let expired = query
        .purge_expired(&root_id)
        .expect("typed expired selector");
    let expired_preview = query
        .preview(&expired, 400_000, None)
        .expect("preview expired attempts");
    assert_eq!(expired_preview.selected_execution_ids(), &[EXECUTION_ID]);
    assert!(attempt_root.join("manifest.json").is_file());
    assert!(attempt_root.join(artifact_id).is_file());

    let expired_report = query
        .execute(&expired, 400_000, None, &ShutdownToken::detached())
        .expect("expired execution revalidates and purges within one aggregate budget");
    assert_eq!(
        expired_report.disposition(),
        PurgeDisposition::Removed,
        "{expired_report:?}"
    );
    assert_eq!(expired_report.removed_execution_ids(), &[EXECUTION_ID]);
    assert!(expired_report.bounds().considered_entries() <= policy.sweep_entry_limit());
    assert!(expired_report.bounds().considered_bytes() <= policy.sweep_byte_limit());
    assert!(!attempt_root.exists());
}

#[test]
fn purge_is_metadata_last_resumable_and_idempotent_after_partial_progress() {
    let root = tempfile::tempdir().expect("temporary destination");
    let registry = OutputStagingRegistry::default();
    let mut attempt = begin(root.path());
    let (first, mut first_file) = attempt
        .stage_direct(
            &registry,
            validated(root.path(), "result.bin"),
            "primary-output",
            "result.bin",
            PromotionDisposition::Replace,
        )
        .expect("first direct artifact");
    let (second, mut second_file) = attempt
        .stage_direct(
            &registry,
            validated(root.path(), "second.bin"),
            "secondary-output",
            "second.bin",
            PromotionDisposition::Replace,
        )
        .expect("second direct artifact");
    first_file.write_all(b"first").expect("first bytes");
    second_file.write_all(b"second").expect("second bytes");
    drop(first_file);
    drop(second_file);
    attempt.mark_ready(&first).expect("first ready");
    attempt.mark_ready(&second).expect("second ready");
    let attempt_root = attempt.attempt_root().to_path_buf();
    drop(attempt);

    let policy = bounded_policy(root.path(), 86_400, 1_000, 8_000_000_000, 2);
    let query = query(root.path(), &policy, "partial_purge");
    let root_id = query.owned_root_ids()[0].to_owned();
    let request = query
        .purge_execution(&root_id, EXECUTION_ID)
        .expect("typed purge request");
    let shutdown = ShutdownToken::detached();
    let mut calls = 0_u64;
    let partial = query
        .execute_with_elapsed(&request, 400_000, None, &shutdown, || {
            calls += 1;
            if calls <= 7 { 0 } else { 2 }
        })
        .expect("bounded partial purge");
    assert_eq!(partial.disposition(), PurgeDisposition::Partial);
    assert_eq!(partial.removed_artifact_count(), 1);
    assert_eq!(partial.kept_execution_ids(), &[EXECUTION_ID]);
    assert!(partial.continuation().is_some());
    assert!(attempt_root.join("manifest.json").is_file());
    assert!(attempt_root.join("live.lock").is_file());
    assert_ne!(
        attempt_root.join(&first).exists(),
        attempt_root.join(&second).exists(),
        "exactly one owned artifact should be removed before the time stop"
    );

    let continuation = partial.continuation().expect("partial continuation");
    let completed = query
        .execute(
            &request,
            400_000,
            Some(continuation),
            &ShutdownToken::detached(),
        )
        .expect("retry finishes from durable ownership metadata");
    assert_eq!(
        completed.disposition(),
        PurgeDisposition::Removed,
        "{completed:?}"
    );
    assert!(completed.kept_execution_ids().is_empty());
    assert!(!attempt_root.exists());

    let repeated = query
        .execute(&request, 400_000, None, &ShutdownToken::detached())
        .expect("repeated purge is idempotent");
    assert_eq!(repeated.disposition(), PurgeDisposition::AlreadyAbsent);
}

#[test]
fn purge_never_overrides_live_invalid_linked_or_unknown_ownership() {
    let root = tempfile::tempdir().expect("temporary destination");
    let policy = bounded_policy(root.path(), 0, 1_000, 8_000_000_000, 2_000);
    let query = query(root.path(), &policy, "purge_refusals");
    let root_id = query.owned_root_ids()[0].to_owned();
    let live = begin(root.path());
    let request = query
        .purge_execution(&root_id, EXECUTION_ID)
        .expect("typed purge request");
    let live_report = query
        .execute(&request, 400_000, None, &ShutdownToken::detached())
        .expect("live attempt is a keep outcome");
    assert_eq!(live_report.disposition(), PurgeDisposition::Kept);
    assert_eq!(live_report.kept_execution_ids(), &[EXECUTION_ID]);
    assert!(
        live_report
            .cleanup_debt()
            .iter()
            .any(|debt| debt.kind() == CleanupDebtKind::LiveAttempt)
    );
    assert!(live.attempt_root().exists());
    let attempt_root = live.attempt_root().to_path_buf();
    drop(live);

    std::fs::write(attempt_root.join("unrelated"), b"canary").expect("unknown child");
    let unknown = query
        .execute(&request, 400_000, None, &ShutdownToken::detached())
        .expect("unknown child is a keep outcome");
    assert_eq!(unknown.disposition(), PurgeDisposition::Kept);
    assert_eq!(
        std::fs::read(attempt_root.join("unrelated")).unwrap(),
        b"canary"
    );
    assert!(attempt_root.join("manifest.json").exists());
}

#[test]
fn interrupted_purge_keeps_owner_metadata_and_exact_taxonomy_and_prohibitions() {
    let root = tempfile::tempdir().expect("temporary destination");
    let attempt = begin(root.path());
    let attempt_root = attempt.attempt_root().to_path_buf();
    drop(attempt);
    let policy = bounded_policy(root.path(), 0, 1_000, 8_000_000_000, 2_000);
    let query = query(root.path(), &policy, "interrupted_purge");
    let root_id = query.owned_root_ids()[0].to_owned();
    let request = query
        .purge_execution(&root_id, EXECUTION_ID)
        .expect("typed purge request");
    let shutdown = ShutdownToken::detached();
    shutdown.request();
    let report = query
        .execute(&request, 400_000, None, &shutdown)
        .expect("interruption is reported, not inferred as success");
    assert_eq!(report.disposition(), PurgeDisposition::Partial);
    assert!(attempt_root.join("manifest.json").is_file());
    assert!(attempt_root.join("live.lock").is_file());

    assert_eq!(
        ATTEMPT_EDGE_OUTCOME_TAXONOMY,
        [
            "cancellation_no_final",
            "cleanup_liveness",
            "confinement",
            "cross_filesystem_no_copy",
            "rename_visibility",
            "sync_durability",
        ]
    );
    assert_eq!(ATTEMPT_PUBLICATION_PROHIBITIONS.len(), 5);
}

#[allow(dead_code)]
fn _assert_send_paths(_: PathBuf) {}
