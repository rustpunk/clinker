use std::io::Write;
#[cfg(target_os = "linux")]
use std::io::{BufRead, BufReader};
#[cfg(target_os = "linux")]
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
#[cfg(target_os = "linux")]
use std::time::Duration;

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

fn retained_policy(
    destination_root: &Path,
    retained_attempt_limit: u64,
    retained_byte_limit: u64,
    estimated_attempt_bytes: u64,
) -> ResolvedPublicationPolicy {
    let config = format!(
        "[storage.publication]\nfailed_retention_seconds = 0\nmax_attempt_bytes = \"{estimated_attempt_bytes}B\"\nretained_byte_limit = \"{retained_byte_limit}B\"\nretained_attempt_limit = {retained_attempt_limit}\nmin_free_bytes = \"1B\"\n"
    );
    ClinkerToml::parse(&config)
        .expect("parse retained publication fixture")
        .storage
        .publication
        .resolve(destination_root, estimated_attempt_bytes, 8_000_000_000)
        .expect("resolve retained publication fixture")
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

fn assert_root_local_recovery(
    policy: &ResolvedPublicationPolicy,
    roots: &[&Path],
    expected_artifact_counts: &[usize],
    plan_name: &str,
) {
    let query = AttemptQuery::new(
        &compiled_plan(plan_name),
        policy,
        roots.iter().map(|root| validated(root, ".")).collect(),
    )
    .expect("construct multi-root recovery query");
    let root_ids = query
        .owned_root_ids()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let mut observed_counts = Vec::new();
    let mut observed_artifact_ids = Vec::new();
    for root_id in &root_ids {
        let list = query
            .list(root_id, 100_000_000, None)
            .expect("list root-local attempt");
        assert_eq!(list.entries().len(), 1);
        assert!(
            list.cleanup_debt()
                .iter()
                .all(|debt| debt.kind() != CleanupDebtKind::InvalidManifest)
        );
        let inspection = query
            .inspect(root_id, EXECUTION_ID, 100_000_000)
            .expect("inspect root-local attempt");
        assert_eq!(inspection.state(), Some(AttemptState::Abandoned));
        assert!(inspection.is_eligible());
        assert!(
            inspection
                .cleanup_debt()
                .iter()
                .all(|debt| debt.kind() != CleanupDebtKind::Operational)
        );
        observed_counts.push(inspection.artifact_ids().len());
        observed_artifact_ids.extend(inspection.artifact_ids().iter().cloned());

        let request = query
            .purge_execution(root_id, EXECUTION_ID)
            .expect("select root-local attempt");
        let report = query
            .execute(&request, 100_000_000, None, &ShutdownToken::detached())
            .expect("purge root-local attempt");
        assert_eq!(report.disposition(), PurgeDisposition::Removed);
        assert_eq!(
            report.removed_artifact_count(),
            inspection.artifact_ids().len()
        );
    }
    observed_counts.sort_unstable();
    let mut expected = expected_artifact_counts.to_vec();
    expected.sort_unstable();
    assert_eq!(observed_counts, expected);
    observed_artifact_ids.sort();
    assert_eq!(
        observed_artifact_ids,
        vec![
            "artifact-00000001".to_owned(),
            "artifact-00000002".to_owned()
        ]
    );
    for root in roots {
        assert!(!root.join(".clinker-attempts").exists());
    }
}

#[test]
fn failed_direct_multi_root_attempt_is_inspectable_and_purgeable_per_root() {
    let first = tempfile::tempdir().expect("first destination");
    let second = tempfile::tempdir().expect("second destination");
    let registry = OutputStagingRegistry::default();
    let policy = resolved_policy(first.path(), PublicationMode::Direct, None, 1_024);
    let registrations = vec![
        registration(ArtifactKind::Primary, first.path(), "first.bin", "first"),
        registration(ArtifactKind::Sidecar, second.path(), "second.bin", "second"),
    ];
    let (mut attempt, mut writers) = AttemptPublication::create_run(
        policy.clone(),
        &registry,
        EXECUTION_ID,
        1_000,
        301_000,
        registrations,
    )
    .expect("create direct multi-root attempt");
    for writer in &mut writers {
        writer.file_mut().write_all(b"retained").unwrap();
        attempt.mark_ready(writer.artifact_id()).unwrap();
    }
    drop(writers);
    let shutdown = ShutdownToken::detached();
    shutdown.request();
    assert!(attempt.publish_run(&registry, &shutdown).unwrap().is_none());
    drop(attempt);

    let manifests = [first.path(), second.path()].map(|root| {
        AttemptManifest::read(
            &root
                .join(".clinker-attempts")
                .join(EXECUTION_ID)
                .join("manifest.json"),
            100_000_000,
        )
        .expect("each direct root has a durable manifest")
    });
    for manifest in &manifests {
        assert_eq!(manifest.state(), AttemptState::Abandoned);
        assert_eq!(manifest.artifact_count(), 2);
    }
    assert_eq!(manifests[0], manifests[1]);
    assert_root_local_recovery(
        &policy,
        &[first.path(), second.path()],
        &[1, 1],
        "direct_multi_root_recovery",
    );
}

#[test]
fn failed_local_then_publish_multi_root_attempt_is_purgeable_per_root() {
    let first = tempfile::tempdir().expect("first destination");
    let second = tempfile::tempdir().expect("second destination");
    let spool = tempfile::tempdir().expect("local spool");
    let registry = OutputStagingRegistry::default();
    let policy = resolved_policy(
        first.path(),
        PublicationMode::LocalThenPublish,
        Some(spool.path()),
        1_024,
    );
    let registrations = vec![
        registration(ArtifactKind::Primary, first.path(), "first.bin", "first"),
        registration(ArtifactKind::Dlq, second.path(), "errors.bin", "errors"),
    ];
    let (mut attempt, mut writers) = AttemptPublication::create_run(
        policy.clone(),
        &registry,
        EXECUTION_ID,
        1_000,
        301_000,
        registrations,
    )
    .expect("create local-then-publish multi-root attempt");
    for writer in &mut writers {
        writer.file_mut().write_all(b"retained").unwrap();
        attempt.mark_ready(writer.artifact_id()).unwrap();
    }
    drop(writers);
    let shutdown = ShutdownToken::detached();
    shutdown.request();
    assert!(attempt.publish_run(&registry, &shutdown).unwrap().is_none());
    drop(attempt);

    let manifests = [first.path(), second.path(), spool.path()].map(|root| {
        AttemptManifest::read(
            &root
                .join(".clinker-attempts")
                .join(EXECUTION_ID)
                .join("manifest.json"),
            100_000_000,
        )
        .expect("each local-then-publish root has a durable manifest")
    });
    for manifest in &manifests {
        assert_eq!(manifest.state(), AttemptState::Abandoned);
        assert_eq!(manifest.artifact_count(), 2);
    }
    assert!(manifests.windows(2).all(|pair| pair[0] == pair[1]));
    assert_root_local_recovery(
        &policy,
        &[first.path(), second.path(), spool.path()],
        &[0, 1, 1],
        "local_then_publish_multi_root_recovery",
    );
}

#[test]
fn local_then_publish_refuses_same_spool_and_destination_before_attempt_creation() {
    let root = tempfile::tempdir().expect("destination and spool");
    let registry = OutputStagingRegistry::default();
    let policy = resolved_policy(
        root.path(),
        PublicationMode::LocalThenPublish,
        Some(root.path()),
        1_024,
    );
    let error = AttemptPublication::create_run(
        policy,
        &registry,
        EXECUTION_ID,
        1_000,
        301_000,
        vec![registration(
            ArtifactKind::Primary,
            root.path(),
            "result.bin",
            "primary",
        )],
    )
    .expect_err("same spool and destination root must fail preflight");
    assert!(error.to_string().contains("local spool"), "{error}");
    assert!(!root.path().join(".clinker-attempts").exists());
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
fn aggregate_retained_limits_fail_before_attempt_creation() {
    let new_execution_id = "018f47a2-9a41-7a27-b4d6-4f7137e3c260";
    for (case, expected) in [(0, "retained attempt count"), (1, "retained attempt bytes")] {
        let root = tempfile::tempdir().expect("temporary destination");
        let registry = OutputStagingRegistry::default();
        let mut retained = begin(root.path());
        if case == 1 {
            stage_ready(&mut retained, &registry, root.path(), &[b'x'; 500]);
        }
        let retained_root = retained.attempt_root().to_path_buf();
        drop(retained);
        let resolved = if case == 0 {
            retained_policy(root.path(), 1, 8_000_000_000, 1)
        } else {
            retained_policy(root.path(), 8, 1_024, 600)
        };
        let error = AttemptPublication::create_run(
            resolved,
            &registry,
            new_execution_id,
            1_000,
            301_000,
            vec![registration(
                ArtifactKind::Primary,
                root.path(),
                "new.bin",
                "new-output",
            )],
        )
        .expect_err("aggregate admission must fail closed");

        assert!(error.to_string().contains(expected), "{error}");
        assert!(retained_root.exists());
        assert!(
            !root
                .path()
                .join(".clinker-attempts")
                .join(new_execution_id)
                .exists(),
            "rejected admission must not create its attempt root"
        );
    }
}

#[test]
fn aggregate_admission_purges_eligible_attempts_before_counting() {
    let root = tempfile::tempdir().expect("temporary destination");
    let registry = OutputStagingRegistry::default();
    let retained = AttemptPublication::create(validated(root.path(), "."), EXECUTION_ID, 1, 1)
        .expect("eligible retained attempt");
    let retained_root = retained.attempt_root().to_path_buf();
    drop(retained);
    let new_execution_id = "018f47a2-9a41-7a27-b4d6-4f7137e3c261";
    let (attempt, writers) = AttemptPublication::create_run(
        retained_policy(root.path(), 1, 8_000_000_000, 1),
        &registry,
        new_execution_id,
        1_000,
        301_000,
        vec![registration(
            ArtifactKind::Primary,
            root.path(),
            "new.bin",
            "new-output",
        )],
    )
    .expect("eligible debt is removed before aggregate admission");

    assert!(!retained_root.exists());
    assert_eq!(attempt.execution_id(), new_execution_id);
    drop(writers);
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

#[cfg(target_os = "linux")]
fn read_control_message(reader: &mut BufReader<UnixStream>) -> serde_json::Value {
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .expect("read bounded publication control message");
    assert!(
        !line.is_empty(),
        "publication control endpoint closed early"
    );
    serde_json::from_str(&line).expect("publication control message must be JSON")
}

#[cfg(target_os = "linux")]
fn release_control_message(stream: &mut UnixStream, mut message: serde_json::Value) {
    message["action"] = serde_json::Value::String("release".to_owned());
    serde_json::to_writer(&mut *stream, &message).expect("write publication release");
    stream
        .write_all(b"\n")
        .expect("terminate publication release");
    stream.flush().expect("flush publication release");
}

#[cfg(target_os = "linux")]
#[test]
fn qualification_control_binds_every_real_stage_to_attempt_artifact_and_mode() {
    for (mode, expected_stages) in [
        (
            PublicationMode::Direct,
            vec![
                "file_synchronization",
                "rename",
                "parent_directory_synchronization",
                "complete_before_cleanup",
            ],
        ),
        (
            PublicationMode::LocalThenPublish,
            vec![
                "copy",
                "file_synchronization",
                "rename",
                "parent_directory_synchronization",
                "complete_before_cleanup",
            ],
        ),
    ] {
        let destination = tempfile::tempdir().expect("destination");
        let spool = tempfile::tempdir().expect("local spool");
        let registry = OutputStagingRegistry::default();
        let policy = resolved_policy(
            destination.path(),
            mode,
            (mode == PublicationMode::LocalThenPublish).then_some(spool.path()),
            64,
        );
        let (mut attempt, mut writers) = AttemptPublication::create_run(
            policy,
            &registry,
            EXECUTION_ID,
            1_000,
            301_000,
            vec![registration(
                ArtifactKind::Primary,
                destination.path(),
                "result.bin",
                "primary",
            )],
        )
        .expect("create controlled attempt");
        writers[0]
            .file_mut()
            .write_all(b"controlled publication")
            .expect("write controlled artifact");
        let artifact_id = writers[0].artifact_id().to_owned();
        drop(writers);
        let manifest_path = attempt.manifest_path().to_path_buf();
        let attempt_root = attempt.attempt_root().to_path_buf();
        let (attempt_stream, mut harness_stream) = UnixStream::pair().expect("local endpoint");
        attempt
            .install_qualification_stage_control(attempt_stream, Duration::from_secs(2))
            .expect("install qualification control");

        let artifact_for_child = artifact_id.clone();
        let handle = std::thread::spawn(move || {
            attempt.mark_ready(&artifact_for_child)?;
            attempt.publish(&registry, &ShutdownToken::detached())
        });
        let mut reader = BufReader::new(harness_stream.try_clone().expect("clone endpoint"));
        for expected_stage in expected_stages {
            let message = read_control_message(&mut reader);
            assert_eq!(message["schema"], "clinker.attempt-stage-control/v1");
            assert_eq!(message["action"], "stage_ready");
            assert_eq!(message["execution_id"], EXECUTION_ID);
            assert_eq!(message["artifact_id"], artifact_id);
            assert_eq!(
                message["publication_mode"],
                match mode {
                    PublicationMode::Direct => "direct",
                    PublicationMode::LocalThenPublish => "local_then_publish",
                }
            );
            assert_eq!(message["stage"], expected_stage);
            if expected_stage == "complete_before_cleanup" {
                let complete = AttemptManifest::read(&manifest_path, 1_000)
                    .expect("pre-cleanup Complete manifest must be durable");
                assert_eq!(complete.execution_id(), EXECUTION_ID);
                assert_eq!(complete.state(), AttemptState::Complete);
                assert_eq!(complete.artifacts()[0].artifact_id(), artifact_id);
                assert_eq!(complete.artifacts()[0].state(), ArtifactState::Published);
                assert!(destination.path().join("result.bin").is_file());
            }
            release_control_message(&mut harness_stream, message);
        }
        let outcome = handle
            .join()
            .expect("controlled attempt thread")
            .expect("controlled publication")
            .expect("publication gate won");
        assert!(outcome.is_complete());
        assert_eq!(
            std::fs::read(destination.path().join("result.bin")).unwrap(),
            b"controlled publication"
        );
        assert!(!manifest_path.exists());
        assert!(!attempt_root.exists());
        assert!(!destination.path().join(".clinker-attempts").exists());
        assert!(!spool.path().join(".clinker-attempts").exists());
    }
}

#[cfg(target_os = "linux")]
#[test]
fn qualification_control_fails_closed_on_malformed_duplicate_missing_or_cross_attempt_release() {
    for failure in ["malformed", "duplicate", "missing", "cross-attempt"] {
        let destination = tempfile::tempdir().expect("destination");
        let registry = OutputStagingRegistry::default();
        let policy = resolved_policy(destination.path(), PublicationMode::Direct, None, 64);
        let (mut attempt, mut writers) = AttemptPublication::create_run(
            policy,
            &registry,
            EXECUTION_ID,
            1_000,
            301_000,
            vec![registration(
                ArtifactKind::Primary,
                destination.path(),
                "result.bin",
                "primary",
            )],
        )
        .expect("create controlled attempt");
        writers[0].file_mut().write_all(b"retained").unwrap();
        let artifact_id = writers[0].artifact_id().to_owned();
        drop(writers);
        let manifest_path = attempt.manifest_path().to_path_buf();
        let (attempt_stream, mut harness_stream) = UnixStream::pair().expect("local endpoint");
        attempt
            .install_qualification_stage_control(attempt_stream, Duration::from_millis(100))
            .expect("install qualification control");
        let artifact_for_child = artifact_id.clone();
        let handle = std::thread::spawn(move || {
            attempt.mark_ready(&artifact_for_child)?;
            attempt.publish(&registry, &ShutdownToken::detached())
        });
        let mut reader = BufReader::new(harness_stream.try_clone().expect("clone endpoint"));
        let message = read_control_message(&mut reader);
        match failure {
            "malformed" => harness_stream.write_all(b"{}\n").unwrap(),
            "duplicate" => {
                release_control_message(&mut harness_stream, message.clone());
                release_control_message(&mut harness_stream, message);
            }
            "missing" => drop(harness_stream),
            "cross-attempt" => {
                let mut changed = message;
                changed["execution_id"] =
                    serde_json::Value::String("018f47a2-9a41-7a27-b4d6-4f7137e3c160".to_owned());
                release_control_message(&mut harness_stream, changed);
            }
            _ => unreachable!(),
        }
        assert!(
            handle.join().expect("controlled attempt thread").is_err(),
            "{failure} release must fail closed"
        );
        let retained = AttemptManifest::read(&manifest_path, 1_000)
            .expect("failed control retains owner metadata");
        assert_ne!(retained.state(), AttemptState::Complete);
        assert!(!destination.path().join("result.bin").exists());
    }
}

#[cfg(target_os = "linux")]
fn matrix_send(stream: &mut UnixStream, value: &serde_json::Value) {
    serde_json::to_writer(&mut *stream, value).expect("encode matrix control message");
    stream.write_all(b"\n").expect("terminate matrix message");
    stream.flush().expect("flush matrix message");
}

#[cfg(target_os = "linux")]
fn matrix_wait(reader: &mut BufReader<UnixStream>, action: &str, scenario: &str) {
    let message = read_control_message(reader);
    assert_eq!(
        message["schema"],
        "clinker.filesystem-publication-control/1"
    );
    assert_eq!(message["action"], action);
    assert_eq!(message["scenario"], scenario);
    assert_eq!(message.as_object().expect("control object").len(), 3);
}

#[cfg(target_os = "linux")]
fn matrix_connection(mount_root: &Path, scenario: &str, mode: PublicationMode) -> UnixStream {
    let endpoint = mount_root
        .parent()
        .expect("matrix mount has a disposable parent")
        .join("publication-control.sock");
    let mut stream = UnixStream::connect(endpoint).expect("connect publication matrix control");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("bound matrix control reads");
    stream
        .set_write_timeout(Some(Duration::from_secs(30)))
        .expect("bound matrix control writes");
    matrix_send(
        &mut stream,
        &serde_json::json!({
            "action": "scenario_begin",
            "publication_mode": match mode {
                PublicationMode::Direct => "direct",
                PublicationMode::LocalThenPublish => "local_then_publish",
            },
            "scenario": scenario,
            "schema": "clinker.filesystem-publication-control/1",
        }),
    );
    stream
}

#[cfg(target_os = "linux")]
fn matrix_execution_id(index: u64) -> String {
    format!("018f47a2-9a41-7a27-b4d6-{index:012x}")
}

#[cfg(target_os = "linux")]
fn matrix_operator_proof(
    destination: &Path,
    mode: PublicationMode,
    spool: Option<&Path>,
    execution_id: &str,
    plan_name: &str,
) -> AttemptState {
    let policy = resolved_policy(destination, mode, spool, 256 * 1024 * 1024);
    let mut roots = vec![validated(destination, ".")];
    if let Some(spool) = spool {
        roots.push(validated(spool, "."));
    }
    let query = AttemptQuery::new(&compiled_plan(plan_name), &policy, roots)
        .expect("construct mounted operator query");
    let mut observed_state = None;
    for root_id in query
        .owned_root_ids()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>()
    {
        let listed = query
            .list(&root_id, 100_000_000, None)
            .expect("list mounted retained attempt");
        assert_eq!(listed.entries().len(), 1);
        assert!(listed.cleanup_debt().is_empty());
        let inspected = query
            .inspect(&root_id, execution_id, 100_000_000)
            .expect("inspect mounted retained attempt");
        let state = inspected.state().expect("retained state");
        assert_ne!(state, AttemptState::Complete);
        assert_eq!(observed_state.get_or_insert(state), &state);
        assert!(inspected.cleanup_debt().is_empty());
        let request = query
            .purge_execution(&root_id, execution_id)
            .expect("select exact mounted attempt");
        let preview = query
            .preview(&request, 100_000_000, None)
            .expect("preview exact mounted purge");
        assert_eq!(preview.selected_execution_ids(), &[execution_id]);
        assert!(preview.cleanup_debt().is_empty());
    }
    observed_state.expect("at least one mounted operator root")
}

#[cfg(target_os = "linux")]
fn matrix_execute_purge(
    destination: &Path,
    mode: PublicationMode,
    spool: Option<&Path>,
    execution_id: &str,
    plan_name: &str,
) {
    let policy = resolved_policy(destination, mode, spool, 256 * 1024 * 1024);
    let mut roots = vec![validated(destination, ".")];
    if let Some(spool) = spool {
        roots.push(validated(spool, "."));
    }
    let query = AttemptQuery::new(&compiled_plan(plan_name), &policy, roots)
        .expect("construct mounted operator query");
    for root_id in query
        .owned_root_ids()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>()
    {
        let request = query
            .purge_execution(&root_id, execution_id)
            .expect("select exact mounted attempt");
        let report = query
            .execute(&request, 100_000_000, None, &ShutdownToken::detached())
            .expect("execute exact mounted purge");
        assert_eq!(report.disposition(), PurgeDisposition::Removed);
        assert!(report.cleanup_debt().is_empty());
    }
    assert!(
        !destination
            .join(".clinker-attempts")
            .join(execution_id)
            .exists()
    );
    if let Some(spool) = spool {
        assert!(!spool.join(".clinker-attempts").join(execution_id).exists());
    }
}

#[cfg(target_os = "linux")]
fn matrix_attempt(
    destination: &Path,
    mode: PublicationMode,
    spool: Option<&Path>,
    execution_id: &str,
    leaf: &str,
    disposition: PromotionDisposition,
) -> (
    AttemptPublication,
    Vec<clinker_exec::output::attempt::AttemptArtifactWriter>,
    OutputStagingRegistry,
) {
    let registry = OutputStagingRegistry::default();
    let policy = resolved_policy(destination, mode, spool, 256 * 1024 * 1024);
    let (attempt, writers) = AttemptPublication::create_run(
        policy,
        &registry,
        execution_id,
        1_000,
        2_000,
        vec![
            ArtifactRegistration::new(
                ArtifactKind::Primary,
                "matrix-output",
                leaf,
                validated(destination, leaf),
                disposition,
            )
            .expect("matrix registration"),
        ],
    )
    .expect("create mounted matrix attempt");
    (attempt, writers, registry)
}

#[cfg(target_os = "linux")]
fn matrix_success(destination: &Path, mode: PublicationMode, spool: Option<&Path>, index: u64) {
    let scenario = format!(
        "success-{}",
        match mode {
            PublicationMode::Direct => "direct",
            PublicationMode::LocalThenPublish => "local_then_publish",
        }
    );
    let execution_id = matrix_execution_id(index);
    let leaf = format!("{scenario}.bin");
    let mut stream = matrix_connection(destination, &scenario, mode);
    let mut reader = BufReader::new(stream.try_clone().expect("clone matrix endpoint"));
    let (mut attempt, mut writers, registry) = matrix_attempt(
        destination,
        mode,
        spool,
        &execution_id,
        &leaf,
        PromotionDisposition::Replace,
    );
    writers[0]
        .file_mut()
        .write_all(b"mounted success")
        .expect("write mounted success");
    let artifact_id = writers[0].artifact_id().to_owned();
    drop(writers);
    attempt
        .install_qualification_stage_control(
            stream.try_clone().expect("clone qualification endpoint"),
            Duration::from_secs(30),
        )
        .expect("install mounted qualification control");
    attempt
        .mark_ready(&artifact_id)
        .expect("mounted success becomes ready");
    let outcome = attempt
        .publish(&registry, &ShutdownToken::detached())
        .expect("mounted success publication")
        .expect("mounted success owns publication gate");
    assert!(outcome.is_complete());
    assert_eq!(
        std::fs::read(destination.join(&leaf)).unwrap(),
        b"mounted success"
    );
    assert!(
        !destination
            .join(".clinker-attempts")
            .join(&execution_id)
            .exists()
    );
    matrix_send(
        &mut stream,
        &serde_json::json!({
            "action": "success_complete",
            "scenario": scenario,
            "schema": "clinker.filesystem-publication-control/1",
        }),
    );
    matrix_wait(&mut reader, "finish", &scenario);
    std::fs::remove_file(destination.join(leaf)).expect("remove successful matrix final");
}

#[cfg(target_os = "linux")]
fn matrix_ordinary_failure(
    destination: &Path,
    mode: PublicationMode,
    spool: Option<&Path>,
    index: u64,
) {
    let scenario = format!(
        "ordinary-failure-{}",
        match mode {
            PublicationMode::Direct => "direct",
            PublicationMode::LocalThenPublish => "local_then_publish",
        }
    );
    let execution_id = matrix_execution_id(index);
    let leaf = format!("{scenario}.bin");
    std::fs::write(destination.join(&leaf), b"existing final").expect("preexisting final");
    let mut stream = matrix_connection(destination, &scenario, mode);
    let mut reader = BufReader::new(stream.try_clone().expect("clone matrix endpoint"));
    let (mut attempt, mut writers, registry) = matrix_attempt(
        destination,
        mode,
        spool,
        &execution_id,
        &leaf,
        PromotionDisposition::NoReplace,
    );
    writers[0]
        .file_mut()
        .write_all(b"replacement")
        .expect("write colliding artifact");
    let artifact_id = writers[0].artifact_id().to_owned();
    drop(writers);
    attempt
        .mark_ready(&artifact_id)
        .expect("failure becomes ready");
    let non_success = match attempt.publish(&registry, &ShutdownToken::detached()) {
        Err(_) | Ok(None) => true,
        Ok(Some(outcome)) => !outcome.is_complete(),
    };
    assert!(
        non_success,
        "ordinary collision must retain non-success truth"
    );
    drop(attempt);
    let state = matrix_operator_proof(destination, mode, spool, &execution_id, &scenario);
    matrix_send(
        &mut stream,
        &serde_json::json!({
            "action": "recovery_ready",
            "artifact_id": artifact_id,
            "execution_id": execution_id,
            "manifest_state": format!("{state:?}").to_ascii_lowercase(),
            "scenario": scenario,
            "schema": "clinker.filesystem-publication-control/1",
        }),
    );
    matrix_wait(&mut reader, "purge", &scenario);
    matrix_execute_purge(destination, mode, spool, &execution_id, &scenario);
    matrix_send(
        &mut stream,
        &serde_json::json!({
            "action": "purge_complete",
            "scenario": scenario,
            "schema": "clinker.filesystem-publication-control/1",
        }),
    );
    matrix_wait(&mut reader, "finish", &scenario);
    std::fs::remove_file(destination.join(leaf)).expect("remove preexisting final");
}

#[cfg(target_os = "linux")]
fn matrix_interruption(
    destination: &Path,
    mode: PublicationMode,
    spool: Option<&Path>,
    stage: &str,
    index: u64,
) {
    let mode_name = match mode {
        PublicationMode::Direct => "direct",
        PublicationMode::LocalThenPublish => "local_then_publish",
    };
    let scenario = format!("interruption-{mode_name}-{stage}");
    let execution_id = matrix_execution_id(index);
    let leaf = format!("{scenario}.bin");
    let mut stream = matrix_connection(destination, &scenario, mode);
    let mut reader = BufReader::new(stream.try_clone().expect("clone matrix endpoint"));
    let (mut attempt, mut writers, registry) = matrix_attempt(
        destination,
        mode,
        spool,
        &execution_id,
        &leaf,
        PromotionDisposition::Replace,
    );
    writers[0]
        .file_mut()
        .write_all(b"mounted interruption")
        .expect("write interrupted artifact");
    let artifact_id = writers[0].artifact_id().to_owned();
    drop(writers);
    attempt
        .install_qualification_stage_control(
            stream.try_clone().expect("clone qualification endpoint"),
            Duration::from_secs(30),
        )
        .expect("install mounted interruption control");
    let non_success = if matches!(stage, "copy" | "file_synchronization") {
        attempt.mark_ready(&artifact_id).is_err()
    } else {
        match attempt
            .mark_ready(&artifact_id)
            .and_then(|()| attempt.publish(&registry, &ShutdownToken::detached()))
        {
            Err(_) | Ok(None) => true,
            Ok(Some(outcome)) => !outcome.is_complete(),
        }
    };
    assert!(
        non_success,
        "mounted disruption must retain non-success truth"
    );
    drop(attempt);
    matrix_send(
        &mut stream,
        &serde_json::json!({
            "action": "interruption_observed",
            "scenario": scenario,
            "schema": "clinker.filesystem-publication-control/1",
        }),
    );
    matrix_wait(&mut reader, "recover", &scenario);
    let state = matrix_operator_proof(destination, mode, spool, &execution_id, &scenario);
    matrix_send(
        &mut stream,
        &serde_json::json!({
            "action": "recovery_ready",
            "artifact_id": artifact_id,
            "execution_id": execution_id,
            "manifest_state": format!("{state:?}").to_ascii_lowercase(),
            "scenario": scenario,
            "schema": "clinker.filesystem-publication-control/1",
        }),
    );
    matrix_wait(&mut reader, "purge", &scenario);
    matrix_execute_purge(destination, mode, spool, &execution_id, &scenario);
    matrix_send(
        &mut stream,
        &serde_json::json!({
            "action": "purge_complete",
            "scenario": scenario,
            "schema": "clinker.filesystem-publication-control/1",
        }),
    );
    matrix_wait(&mut reader, "finish", &scenario);
    if destination.join(&leaf).exists() {
        std::fs::remove_file(destination.join(leaf)).expect("remove interrupted visible final");
    }
}

#[cfg(target_os = "linux")]
fn matrix_enospc(destination: &Path, index: u64) {
    let scenario = "capacity-enospc";
    let execution_id = matrix_execution_id(index);
    let leaf = "capacity-enospc.bin";
    let mut stream = matrix_connection(destination, scenario, PublicationMode::Direct);
    let mut reader = BufReader::new(stream.try_clone().expect("clone matrix endpoint"));
    let (attempt, mut writers, _registry) = matrix_attempt(
        destination,
        PublicationMode::Direct,
        None,
        &execution_id,
        leaf,
        PromotionDisposition::Replace,
    );
    let block = vec![0x5a; 1024 * 1024];
    let mut observed = None;
    for _ in 0..256 {
        if let Err(error) = writers[0].file_mut().write_all(&block) {
            observed = error.raw_os_error();
            break;
        }
    }
    if observed.is_none()
        && let Err(error) = writers[0].file_mut().sync_all()
    {
        observed = error.raw_os_error();
    }
    assert_eq!(observed, Some(28), "mounted backing must return ENOSPC");
    drop(writers);
    drop(attempt);
    assert!(!destination.join(leaf).exists());
    let state = matrix_operator_proof(
        destination,
        PublicationMode::Direct,
        None,
        &execution_id,
        scenario,
    );
    assert_eq!(state, AttemptState::Staging);
    matrix_send(
        &mut stream,
        &serde_json::json!({
            "action": "capacity_ready",
            "artifact_id": "artifact-00000001",
            "enospc_raw_os_error": 28,
            "execution_id": execution_id,
            "manifest_state": "staging",
            "scenario": scenario,
            "schema": "clinker.filesystem-publication-control/1",
        }),
    );
    matrix_wait(&mut reader, "purge", scenario);
    matrix_execute_purge(
        destination,
        PublicationMode::Direct,
        None,
        &execution_id,
        scenario,
    );
    matrix_send(
        &mut stream,
        &serde_json::json!({
            "action": "purge_complete",
            "scenario": scenario,
            "schema": "clinker.filesystem-publication-control/1",
        }),
    );
    matrix_wait(&mut reader, "finish", scenario);
}

#[cfg(target_os = "linux")]
#[test]
fn remote_filesystem_publication_matrix() {
    let (Ok(profile), Ok(mount_root)) = (
        std::env::var("CLINKER_FILESYSTEM_PROFILE"),
        std::env::var("CLINKER_FILESYSTEM_ROOT"),
    ) else {
        return;
    };
    assert!(matches!(
        profile.as_str(),
        "linux-nfsv4.1-loopback-ci" | "linux-smb3.1.1-loopback-ci"
    ));
    let mount_root = PathBuf::from(mount_root);
    let destination = mount_root.join(".clinker-publication-matrix");
    std::fs::create_dir(&destination).expect("create mounted publication sandbox");
    let spool = tempfile::tempdir().expect("local publication spool");

    matrix_success(&destination, PublicationMode::Direct, None, 1);
    matrix_success(
        &destination,
        PublicationMode::LocalThenPublish,
        Some(spool.path()),
        2,
    );
    matrix_ordinary_failure(&destination, PublicationMode::Direct, None, 3);
    matrix_ordinary_failure(
        &destination,
        PublicationMode::LocalThenPublish,
        Some(spool.path()),
        4,
    );

    let mut index = 10;
    for (mode, stages) in [
        (
            PublicationMode::Direct,
            &[
                "file_synchronization",
                "rename",
                "parent_directory_synchronization",
            ][..],
        ),
        (
            PublicationMode::LocalThenPublish,
            &[
                "copy",
                "file_synchronization",
                "rename",
                "parent_directory_synchronization",
            ][..],
        ),
    ] {
        for stage in stages {
            matrix_interruption(
                &destination,
                mode,
                (mode == PublicationMode::LocalThenPublish).then_some(spool.path()),
                stage,
                index,
            );
            index += 1;
        }
    }
    matrix_enospc(&destination, 30);
    std::fs::remove_dir(&destination).expect("remove mounted publication sandbox");
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

#[test]
fn fresh_query_reconciles_durable_promotion_intent_from_handles() {
    for final_visible in [false, true] {
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
        file.write_all(b"promotion evidence")
            .expect("write fixture");
        drop(file);
        attempt.mark_ready(&artifact_id).expect("ready artifact");
        attempt.set_fault_for_testing(AttemptFault::PromotionInterrupted);
        attempt
            .publish(&registry, &ShutdownToken::detached())
            .expect_err("promotion interruption must not fabricate an outcome");
        let attempt_root = attempt.attempt_root().to_path_buf();
        let retained = AttemptManifest::read(attempt.manifest_path(), 400_000)
            .expect("durable promotion intent");
        assert_eq!(retained.artifacts()[0].state(), ArtifactState::Promoting);
        drop(attempt);
        drop(registry);

        if final_visible {
            std::fs::rename(
                attempt_root.join(&artifact_id),
                root.path().join("result.bin"),
            )
            .expect("simulate rename before process loss");
        }
        let policy = bounded_policy(root.path(), 0, 1_000, 8_000_000_000, 2_000);
        let query = query(root.path(), &policy, "promotion_reconciliation");
        let root_id = query.owned_root_ids()[0].to_owned();
        let inspection = query
            .inspect(&root_id, EXECUTION_ID, 400_000)
            .expect("fresh handle-relative inspection");
        let expected = if final_visible {
            ArtifactState::VisibleUnsynchronized
        } else {
            ArtifactState::Unpublished
        };
        assert_eq!(
            inspection.artifact_states(),
            &[(artifact_id.clone(), expected)]
        );
    }
}

#[cfg(unix)]
#[test]
fn quota_fault_is_an_explicit_seam_and_never_a_mounted_observation() {
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
    file.write_all(b"quota boundary").expect("write fixture");
    drop(file);
    attempt.set_fault_for_testing(AttemptFault::Quota);

    let error = attempt
        .mark_ready(&artifact_id)
        .expect_err("quota seam must fail closed");
    match error {
        clinker_exec::output::attempt::AttemptError::Io { source, .. } => {
            assert_eq!(source.raw_os_error(), Some(122));
        }
        other => panic!("quota seam returned the wrong error: {other}"),
    }
    let retained = AttemptManifest::read(attempt.manifest_path(), 1_000)
        .expect("quota seam retains owner metadata");
    assert_eq!(retained.state(), AttemptState::Staging);
    assert_eq!(retained.artifacts()[0].state(), ArtifactState::Staging);
    assert!(!root.path().join("result.bin").exists());
}

#[test]
fn staging_intent_must_persist_before_artifact_or_registry_side_effects() {
    let root = tempfile::tempdir().expect("temporary destination");
    let registry = OutputStagingRegistry::default();
    let mut attempt = begin(root.path());
    let attempt_root = attempt.attempt_root().to_path_buf();
    attempt.set_fault_for_testing(AttemptFault::ManifestReplace);

    attempt
        .stage_direct(
            &registry,
            validated(root.path(), "result.bin"),
            "primary-output",
            "result.bin",
            PromotionDisposition::Replace,
        )
        .expect_err("manifest intent failure must stop staging first");

    let retained = AttemptManifest::read(attempt.manifest_path(), 1_000)
        .expect("previous canonical manifest remains durable");
    assert!(retained.artifacts().is_empty());
    assert!(!attempt_root.join("artifact-00000001").exists());
    assert!(registry.pending_paths("primary-output").is_empty());
}

#[test]
fn qualification_control_has_no_pipeline_configuration_route() {
    let error =
        ClinkerToml::parse("[storage.publication]\nqualification_control = \"control.sock\"\n")
            .expect_err("qualification control must not be configurable");
    assert!(error.to_string().contains("qualification_control"));
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

#[cfg(unix)]
#[test]
fn readiness_reopen_rejects_artifact_leaf_substitution() {
    let root = tempfile::tempdir().expect("temporary destination");
    let outside = tempfile::tempdir().expect("outside directory");
    let outside_canary = outside.path().join("canary.bin");
    std::fs::write(&outside_canary, b"outside").expect("outside canary");
    let registry = OutputStagingRegistry::default();
    let mut attempt = begin(root.path());
    let (artifact_id, mut file) = attempt
        .stage_direct(
            &registry,
            validated(root.path(), "result.bin"),
            "out",
            "result.bin",
            PromotionDisposition::Replace,
        )
        .expect("stage artifact");
    file.write_all(b"owned").expect("write artifact");
    drop(file);

    let artifact_path = attempt.attempt_root().join(&artifact_id);
    std::fs::remove_file(&artifact_path).expect("remove admitted artifact");
    std::os::unix::fs::symlink(&outside_canary, &artifact_path)
        .expect("substitute artifact symlink");

    let error = attempt
        .mark_ready(&artifact_id)
        .expect_err("handle-relative no-follow reopen must reject substitution");
    assert!(
        error.to_string().contains("security_policy") || error.to_string().contains("containment"),
        "{error}"
    );
    assert_eq!(std::fs::read(&outside_canary).unwrap(), b"outside");
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
    assert!(entry_page.continuation().is_none());
    assert!(entry_page.cleanup_debt().is_empty());

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
fn bounded_attempt_listing_advances_across_more_attempts_than_one_page() {
    let root = tempfile::tempdir().expect("temporary destination");
    let execution_ids = [
        "018f47a2-9a41-7a27-b4d6-4f7137e3c201",
        "018f47a2-9a41-7a27-b4d6-4f7137e3c202",
        "018f47a2-9a41-7a27-b4d6-4f7137e3c203",
        "018f47a2-9a41-7a27-b4d6-4f7137e3c204",
        "018f47a2-9a41-7a27-b4d6-4f7137e3c205",
    ];
    for execution_id in execution_ids {
        let attempt =
            AttemptPublication::create(validated(root.path(), "."), execution_id, 1_000, 301_000)
                .expect("attempt fixture");
        drop(attempt);
    }
    let policy = bounded_policy(root.path(), 86_400, 2, 8_000_000_000, 2_000);
    let query = query(root.path(), &policy, "multi_page_listing");
    let root_id = query.owned_root_ids()[0].to_owned();
    let mut continuation = None;
    let mut observed = Vec::new();
    let mut encoded_tokens = std::collections::BTreeSet::new();

    loop {
        let page = query
            .list(&root_id, 400_000, continuation.as_ref())
            .expect("bounded list page");
        observed.extend(
            page.entries()
                .iter()
                .map(|entry| entry.inspection().execution_id().to_owned()),
        );
        continuation = page.continuation().cloned();
        let Some(token) = continuation.as_ref() else {
            break;
        };
        assert!(
            encoded_tokens.insert(token.to_bytes().expect("canonical token")),
            "a continuation token must never repeat the same page"
        );
    }

    assert_eq!(observed, execution_ids);
}

#[test]
fn live_lock_only_crash_state_is_discoverable_and_removable() {
    let root = tempfile::tempdir().expect("temporary destination");
    let attempt = begin(root.path());
    let attempt_root = attempt.attempt_root().to_path_buf();
    let manifest_path = attempt.manifest_path().to_path_buf();
    drop(attempt);
    std::fs::remove_file(manifest_path).expect("simulate crash after manifest unlink");

    let policy = bounded_policy(root.path(), 0, 1_000, 8_000_000_000, 2_000);
    let query = query(root.path(), &policy, "terminal_owner_metadata");
    let root_id = query.owned_root_ids()[0].to_owned();
    let inspection = query
        .inspect(&root_id, EXECUTION_ID, 400_000)
        .expect("terminal metadata remains inspectable");
    assert!(inspection.is_owner_metadata_cleanup());
    assert!(inspection.is_eligible());

    let request = query
        .purge_execution(&root_id, EXECUTION_ID)
        .expect("typed purge request");
    let report = query
        .execute(&request, 400_000, None, &ShutdownToken::detached())
        .expect("fresh process removes terminal metadata");
    assert_eq!(report.disposition(), PurgeDisposition::Removed);
    assert!(!attempt_root.exists());
}

#[test]
fn purge_byte_budget_charges_manifest_evidence_not_artifact_payloads() {
    let root = tempfile::tempdir().expect("temporary destination");
    let registry = OutputStagingRegistry::default();
    let mut attempt = begin(root.path());
    let artifact_id = stage_ready(&mut attempt, &registry, root.path(), &[b'x'; 8_192]);
    let attempt_root = attempt.attempt_root().to_path_buf();
    drop(attempt);
    let manifest_bytes = std::fs::metadata(attempt_root.join("manifest.json"))
        .expect("manifest metadata")
        .len();
    assert!(manifest_bytes < 2_048);

    let policy = bounded_policy(root.path(), 0, 1_000, 2_048, 2_000);
    let query = query(root.path(), &policy, "payload_independent_budget");
    let root_id = query.owned_root_ids()[0].to_owned();
    let request = query
        .purge_execution(&root_id, EXECUTION_ID)
        .expect("typed purge request");
    let report = query
        .execute(&request, 400_000, None, &ShutdownToken::detached())
        .expect("payload size does not consume metadata budget");

    assert_eq!(report.disposition(), PurgeDisposition::Removed);
    assert_eq!(report.removed_artifact_count(), 1);
    assert!(report.bounds().considered_bytes() < 2_048);
    assert!(!attempt_root.join(artifact_id).exists());
}

#[test]
fn maximum_manifest_cardinality_purges_across_smaller_cleanup_pages() {
    let root = tempfile::tempdir().expect("temporary destination");
    let attempt = begin(root.path());
    let attempt_root = attempt.attempt_root().to_path_buf();
    let manifest_path = attempt.manifest_path().to_path_buf();
    drop(attempt);

    let mut artifacts = Vec::with_capacity(MANIFEST_MAX_ARTIFACTS);
    for index in 0..MANIFEST_MAX_ARTIFACTS {
        let artifact_id = format!("artifact-{index:08x}");
        std::fs::write(attempt_root.join(&artifact_id), b"").expect("artifact fixture");
        artifacts.push(artifact(
            &artifact_id,
            "bounded-cleanup",
            &format!("artifact-{index:08x}.bin"),
            0,
        ));
    }
    let manifest =
        AttemptManifest::new(EXECUTION_ID, 1_000, 301_000, AttemptState::Ready, artifacts)
            .expect("maximum-cardinality manifest");
    std::fs::write(
        &manifest_path,
        manifest.to_bytes().expect("canonical manifest"),
    )
    .expect("install maximum-cardinality manifest");

    let policy = bounded_policy(root.path(), 0, 2_048, 8_000_000_000, 2_000);
    let query = query(root.path(), &policy, "maximum_cardinality_cleanup");
    let root_id = query.owned_root_ids()[0].to_owned();
    let request = query
        .purge_execution(&root_id, EXECUTION_ID)
        .expect("typed purge request");
    let mut continuation = None;
    let mut removed = 0_usize;
    let mut pages = 0_usize;

    loop {
        let report = query
            .execute(
                &request,
                400_000,
                continuation.as_ref(),
                &ShutdownToken::detached(),
            )
            .expect("bounded cleanup page");
        pages += 1;
        removed += report.removed_artifact_count();
        continuation = report.continuation().cloned();
        if continuation.is_none() {
            assert_eq!(report.disposition(), PurgeDisposition::Removed);
            break;
        }
        assert_eq!(report.disposition(), PurgeDisposition::Partial);
    }

    assert!(pages > 1);
    assert_eq!(removed, MANIFEST_MAX_ARTIFACTS);
    assert!(!attempt_root.exists());
}

#[test]
fn continuation_is_versioned_plan_root_selector_bound_and_single_use() {
    let first = tempfile::tempdir().expect("first destination");
    let second = tempfile::tempdir().expect("second destination");
    let first_attempt = begin(first.path());
    drop(first_attempt);
    let second_attempt_id = "018f47a2-9a41-7a27-b4d6-4f7137e3c160";
    let second_attempt = AttemptPublication::create(
        validated(first.path(), "."),
        second_attempt_id,
        1_000,
        301_000,
    )
    .expect("second bounded attempt");
    drop(second_attempt);
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

    let policy = bounded_policy(root.path(), 86_400, 2, 8_000_000_000, 2_000);
    let query = query(root.path(), &policy, "partial_purge");
    let root_id = query.owned_root_ids()[0].to_owned();
    let request = query
        .purge_execution(&root_id, EXECUTION_ID)
        .expect("typed purge request");
    let shutdown = ShutdownToken::detached();
    let partial = query
        .execute(&request, 400_000, None, &shutdown)
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
