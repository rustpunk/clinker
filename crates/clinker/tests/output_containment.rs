use std::io::Write;
use std::path::Path;

use clinker_exec::output::containment::{
    ContainmentError, OpenDisposition, OutputContainment, PromotionDisposition,
};
use clinker_exec::output::open::open_output;
use clinker_plan::config::IfExistsPolicy;
use clinker_plan::security::{ValidatedPath, validate_path};
use tempfile::tempdir;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

fn relative_output(root: &Path, relative: &str) -> ValidatedPath {
    validate_path(Path::new(relative), root, false).expect("test path should validate")
}

#[cfg(unix)]
fn symlink_dir(target: &Path, link: &Path) {
    std::os::unix::fs::symlink(target, link).expect("directory symlink should be created");
}

#[cfg(windows)]
fn symlink_dir(target: &Path, link: &Path) {
    let status = std::process::Command::new("cmd")
        .args(["/C", "mklink", "/J"])
        .arg(link)
        .arg(target)
        .status()
        .expect("directory junction command should start");
    assert!(status.success(), "directory junction should be created");
}

#[test]
fn unsupported_profile_is_policy_required_before_output_side_effects() {
    let root = tempdir().expect("temporary destination root");
    let destination = relative_output(root.path(), "result.bin");

    let error = OutputContainment::for_profile(destination, "vendor-nas-defaults")
        .expect_err("unlisted storage profiles must fail closed");

    assert!(matches!(error, ContainmentError::PolicyRequired { .. }));
    assert!(!root.path().join("result.bin").exists());
}

#[test]
fn approved_remote_profile_must_match_observed_filesystem_before_open() {
    let root = tempdir().expect("temporary destination root");
    for profile in ["linux-nfsv4.1-loopback-ci", "linux-smb3.1.1-loopback-ci"] {
        let destination = relative_output(root.path(), &format!("{profile}.bin"));
        let error = OutputContainment::for_profile(destination, profile)
            .expect_err("a local filesystem cannot stand in for remote evidence");
        assert!(matches!(error, ContainmentError::PolicyRequired { .. }));
        assert!(!root.path().join(format!("{profile}.bin")).exists());
    }
}

#[test]
fn replaced_ancestor_before_open_is_rejected_without_external_file() {
    let root = tempdir().expect("temporary destination root");
    let destination_dir = root.path().join("destination");
    let original_dir = root.path().join("destination-original");
    let outside_dir = root.path().join("outside");
    std::fs::create_dir(&destination_dir).expect("destination directory");
    std::fs::create_dir(&outside_dir).expect("outside directory");
    let destination = relative_output(root.path(), "destination/result.bin");

    std::fs::rename(&destination_dir, &original_dir).expect("move validated ancestor");
    symlink_dir(&outside_dir, &destination_dir);

    let error = OutputContainment::for_profile(destination, "local-filesystem")
        .expect_err("replaced ancestor must be rejected");

    assert!(matches!(error, ContainmentError::SecurityPolicy { .. }));
    assert!(!outside_dir.join("result.bin").exists());
    assert!(!original_dir.join("result.bin").exists());
}

#[test]
fn open_output_uses_the_same_replaced_ancestor_guard() {
    let root = tempdir().expect("temporary destination root");
    let destination_dir = root.path().join("destination");
    let outside_dir = root.path().join("outside");
    std::fs::create_dir(&destination_dir).expect("destination directory");
    std::fs::create_dir(&outside_dir).expect("outside directory");
    let target = destination_dir.join("result.bin");

    std::fs::remove_dir(&destination_dir).expect("remove validated ancestor");
    symlink_dir(&outside_dir, &destination_dir);

    let error = open_output(IfExistsPolicy::Error, false, |_| Ok(target.clone()))
        .expect_err("open_output must retain use-time containment");

    assert!(error.to_string().contains("security_policy"));
    assert!(!outside_dir.join("result.bin").exists());
}

#[cfg(unix)]
#[test]
fn output_leaf_replacement_is_rejected_before_bytes_are_written() {
    let root = tempdir().expect("temporary destination root");
    let outside = root.path().join("outside.bin");
    std::fs::write(&outside, b"external").expect("outside file should be written");
    let destination = relative_output(root.path(), "result.bin");
    let boundary = OutputContainment::for_profile(destination, "local-filesystem")
        .expect("local destination should be supported");
    std::os::unix::fs::symlink(&outside, root.path().join("result.bin"))
        .expect("leaf symlink should be created");

    let error = boundary
        .open(OpenDisposition::Truncate)
        .expect_err("linked leaf must be rejected");

    assert!(matches!(error, ContainmentError::SecurityPolicy { .. }));
    assert_eq!(
        std::fs::read(outside).expect("outside file remains"),
        b"external"
    );
}

#[test]
fn replaced_source_ancestor_before_promotion_is_rejected() {
    let root = tempdir().expect("temporary destination root");
    let source_dir = root.path().join("source");
    let moved_source_dir = root.path().join("source-original");
    let outside_dir = root.path().join("outside");
    std::fs::create_dir(&source_dir).expect("source directory");
    std::fs::create_dir(&outside_dir).expect("outside directory");
    std::fs::write(source_dir.join("partial.bin"), b"trusted")
        .expect("source artifact should be written");
    std::fs::write(outside_dir.join("partial.bin"), b"external")
        .expect("external artifact should be written");
    let source = relative_output(root.path(), "source/partial.bin");
    let destination = relative_output(root.path(), "result.bin");
    let boundary = OutputContainment::for_profile(destination, "local-filesystem")
        .expect("local destination should be supported");

    std::fs::rename(&source_dir, &moved_source_dir).expect("move validated source ancestor");
    symlink_dir(&outside_dir, &source_dir);

    let error = boundary
        .promote_from(source, PromotionDisposition::Replace)
        .expect_err("source ancestor replacement must be rejected");

    assert!(matches!(error, ContainmentError::SecurityPolicy { .. }));
    assert!(!root.path().join("result.bin").exists());
    assert_eq!(
        std::fs::read(outside_dir.join("partial.bin")).expect("external artifact remains"),
        b"external"
    );
}

#[cfg(unix)]
#[test]
fn local_output_creation_uses_owner_only_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempdir().expect("temporary destination root");
    let destination = relative_output(root.path(), "result.bin");
    let boundary = OutputContainment::for_profile(destination, "local-filesystem")
        .expect("local destination should be supported");

    let mut file = boundary
        .open(OpenDisposition::CreateNew)
        .expect("contained output should open");
    file.write_all(b"contained").expect("contained write");
    file.sync_all().expect("contained sync");

    let mode = std::fs::metadata(root.path().join("result.bin"))
        .expect("output metadata")
        .permissions()
        .mode();
    assert_eq!(mode & 0o077, 0, "group/other permissions must be absent");
}

#[test]
fn same_filesystem_promotion_renames_complete_artifact() {
    let root = tempdir().expect("temporary destination root");
    // The leaf lengths deliberately cover every alignment remainder. This is
    // a regression matrix for Windows' variable-length FILE_RENAME_INFO
    // buffer, where the terminating UTF-16 NUL is outside FileNameLength but
    // still must fit in the supplied storage.
    for (index, destination_name) in ["a", "ab", "abc", "abcd", "result.bin"]
        .into_iter()
        .enumerate()
    {
        let source_name = format!("partial-{index}.bin");
        let source_path = root.path().join(&source_name);
        std::fs::write(&source_path, b"complete artifact").expect("source artifact");
        let source = relative_output(root.path(), &source_name);
        let destination = relative_output(root.path(), destination_name);
        let boundary = OutputContainment::for_profile(destination, "local-filesystem")
            .expect("local destination should be supported");

        boundary
            .promote_from(source, PromotionDisposition::Replace)
            .expect("same-filesystem promotion should succeed");

        assert!(!source_path.exists());
        assert_eq!(
            std::fs::read(root.path().join(destination_name)).expect("promoted artifact"),
            b"complete artifact"
        );
    }
}

#[cfg(any(target_os = "linux", target_os = "windows"))]
#[test]
fn no_replace_promotion_preserves_existing_final_and_source() {
    let root = tempdir().expect("temporary destination root");
    let source_path = root.path().join("partial.bin");
    let final_path = root.path().join("result.bin");
    std::fs::write(&source_path, b"new").expect("source artifact");
    std::fs::write(&final_path, b"existing").expect("existing final");
    let source = relative_output(root.path(), "partial.bin");
    let destination = relative_output(root.path(), "result.bin");
    let boundary = OutputContainment::for_profile(destination, "local-filesystem")
        .expect("local destination should be supported");

    let error = boundary
        .promote_from(source, PromotionDisposition::NoReplace)
        .expect_err("no-replace promotion must reject an existing final");

    assert!(matches!(error, ContainmentError::Io { .. }));
    assert_eq!(
        std::fs::read(final_path).expect("existing final"),
        b"existing"
    );
    assert_eq!(std::fs::read(source_path).expect("source remains"), b"new");
}

#[cfg(target_os = "linux")]
#[test]
fn cross_filesystem_promotion_is_rejected_without_visible_copy() {
    let destination_root = tempdir().expect("temporary destination root");
    let source_root = tempfile::tempdir_in("/dev/shm")
        .expect("Linux containment tests require the standard tmpfs /dev/shm");
    let source_path = source_root.path().join("partial.bin");
    std::fs::write(&source_path, b"source bytes").expect("source artifact should be written");
    let source = validate_path(&source_path, Path::new("/"), true)
        .expect("absolute source fixture should validate");
    let destination = relative_output(destination_root.path(), "result.bin");
    let boundary = OutputContainment::for_profile(destination, "local-filesystem")
        .expect("local destination should be supported");

    let error = boundary
        .promote_from(source, PromotionDisposition::Replace)
        .expect_err("cross-filesystem promotion must fail closed");

    assert!(matches!(error, ContainmentError::SecurityPolicy { .. }));
    assert!(source_path.exists(), "rename must not consume the source");
    assert!(
        !destination_root.path().join("result.bin").exists(),
        "no visible copy fallback may be created"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn remote_filesystem_matrix_semantics() {
    let (Ok(profile), Ok(mount_root)) = (
        std::env::var("CLINKER_FILESYSTEM_PROFILE"),
        std::env::var("CLINKER_FILESYSTEM_ROOT"),
    ) else {
        // The privileged loopback environments are owned by the dedicated CI
        // harness. Ordinary local test runs cover the local policy above.
        return;
    };
    assert!(matches!(
        profile.as_str(),
        "linux-nfsv4.1-loopback-ci" | "linux-smb3.1.1-loopback-ci"
    ));
    let mount_root = Path::new(&mount_root);
    assert!(mount_root.is_dir(), "matrix mount root must exist");
    let sandbox = tempfile::Builder::new()
        .prefix(".clinker-matrix-")
        .tempdir_in(mount_root)
        .expect("destination-local matrix sandbox");
    let sandbox_path = sandbox.path().to_path_buf();

    let destination_profile = match profile.as_str() {
        "linux-nfsv4.1-loopback-ci" => "nfs_v4_1",
        "linux-smb3.1.1-loopback-ci" => "smb_3_1_1",
        _ => unreachable!("profile was validated above"),
    };
    std::fs::write(
        sandbox.path().join("clinker.toml"),
        format!(
            "[storage.publication]\ndestination_profile = \"{destination_profile}\"\nmax_attempt_bytes = \"1MB\"\nmin_free_bytes = \"1B\"\n"
        ),
    )
    .expect("matrix publication profile");

    // Exercise the actual CLI admission, writer, ledger, and promotion path on
    // the mounted share. Direct boundary checks below remain focused fault
    // coverage; this run is the qualification evidence for production wiring.
    std::fs::write(sandbox.path().join("input.csv"), "id\n1\n").expect("matrix input");
    std::fs::write(
        sandbox.path().join("pipeline.yaml"),
        r#"pipeline:
  name: remote_output_commit
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: int }
- type: sink
  name: out
  input: src
  config:
    name: out
    path: production-output.csv
    type: csv
"#,
    )
    .expect("matrix pipeline");
    let output = std::process::Command::new(clinker_bin())
        .current_dir(sandbox.path())
        .args(["run", "pipeline.yaml"])
        .output()
        .expect("run production CLI on mounted share");
    assert!(
        output.status.success(),
        "production CLI remote commit failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        std::fs::read_to_string(sandbox.path().join("production-output.csv"))
            .expect("read production output")
            .contains('1')
    );

    std::fs::write(sandbox.path().join("protected.csv"), "previous\n")
        .expect("previous remote output");
    std::fs::write(
        sandbox.path().join("failure.yaml"),
        r#"pipeline:
  name: remote_output_failure
error_handling:
  strategy: fail_fast
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: int }
- type: transform
  name: fail
  input: src
  config:
    cxl: |
      emit boom = id / 0
- type: sink
  name: out
  input: fail
  config:
    name: out
    path: protected.csv
    type: csv
    if_exists: overwrite
"#,
    )
    .expect("matrix failure pipeline");
    let failed = std::process::Command::new(clinker_bin())
        .current_dir(sandbox.path())
        .args(["run", "failure.yaml"])
        .output()
        .expect("run failing production CLI on mounted share");
    assert!(!failed.status.success(), "failure fixture must fail");
    assert_eq!(
        std::fs::read_to_string(sandbox.path().join("protected.csv"))
            .expect("read protected remote output"),
        "previous\n"
    );

    let destination_dir = sandbox.path().join("destination");
    let outside_dir = sandbox.path().join("outside");
    std::fs::create_dir(&destination_dir).expect("destination directory");
    std::fs::create_dir(&outside_dir).expect("outside directory");
    let escaped = relative_output(sandbox.path(), "destination/escaped.bin");
    std::fs::rename(
        &destination_dir,
        sandbox.path().join("destination-original"),
    )
    .expect("replace destination ancestor");
    symlink_dir(&outside_dir, &destination_dir);
    let error = OutputContainment::for_profile(escaped, &profile)
        .expect_err("remote confinement must reject the replaced ancestor");
    assert!(matches!(error, ContainmentError::SecurityPolicy { .. }));
    assert!(!outside_dir.join("escaped.bin").exists());

    let source_path = sandbox.path().join("quarantine.bin");
    std::fs::write(&source_path, b"remote complete artifact").expect("quarantine artifact");
    let source = relative_output(sandbox.path(), "quarantine.bin");
    let destination = relative_output(sandbox.path(), "published.bin");
    let boundary = OutputContainment::for_profile(destination, &profile)
        .expect("selected remote profile must match the mounted filesystem");
    boundary
        .promote_from(source, PromotionDisposition::Replace)
        .expect("same-share promotion must succeed");
    assert!(!source_path.exists());
    assert_eq!(
        std::fs::read(sandbox.path().join("published.bin"))
            .expect("promoted artifact is immediately visible"),
        b"remote complete artifact"
    );

    let cancelled_partial = sandbox.path().join("cancelled.partial");
    let mut cancelled_file = std::fs::File::create(&cancelled_partial)
        .expect("cancelled partial should remain in quarantine");
    cancelled_file
        .write_all(b"cancel before publication")
        .expect("partial write");
    cancelled_file.sync_all().expect("partial sync");
    drop(cancelled_file);
    assert!(!sandbox.path().join("cancelled.final").exists());
    std::fs::remove_file(cancelled_partial).expect("cancelled partial cleanup");

    let cross_root =
        tempfile::tempdir_in("/dev/shm").expect("remote matrix requires the standard local tmpfs");
    let cross_source_path = cross_root.path().join("local-spool.bin");
    std::fs::write(&cross_source_path, b"local spool").expect("local spool artifact");
    let cross_source = validate_path(&cross_source_path, Path::new("/"), true)
        .expect("local spool fixture should validate");
    let cross_destination = relative_output(sandbox.path(), "cross-device.bin");
    let cross_boundary = OutputContainment::for_profile(cross_destination, &profile)
        .expect("selected remote profile must match before promotion");
    let error = cross_boundary
        .promote_from(cross_source, PromotionDisposition::Replace)
        .expect_err("local spool cannot rename directly into a remote final");
    assert!(matches!(error, ContainmentError::SecurityPolicy { .. }));
    assert!(cross_source_path.exists());
    assert!(!sandbox.path().join("cross-device.bin").exists());

    sandbox
        .close()
        .expect("matrix-owned workspace should be removable after handles close");
    assert!(!sandbox_path.exists());
}
