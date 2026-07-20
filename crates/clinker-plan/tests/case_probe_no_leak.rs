//! The case-sensitivity probe must never leave a stray file in the directory it
//! classifies. It creates a real probe file there (case-sensitivity can only be
//! measured on the target filesystem itself), so cleanup on every exit path is
//! the guarantee that keeps the working tree clean.

use clinker_plan::config::{case_sensitive_dir, collision_key};

/// Count `clinker-case-probe-*` entries left in `dir`.
fn probe_residue(dir: &std::path::Path) -> Vec<std::ffi::OsString> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name())
        .filter(|name| name.to_string_lossy().starts_with("clinker-case-probe-"))
        .collect()
}

#[test]
fn case_sensitive_dir_leaves_no_probe_file_behind() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("errors.csv");

    // The probe runs (and must clean up) even though the target file itself is
    // never created.
    let _ = case_sensitive_dir(&target).unwrap();

    assert!(
        !target.exists(),
        "probe must not create the real output path"
    );
    let residue = probe_residue(dir.path());
    assert!(
        residue.is_empty(),
        "case_sensitive_dir left probe residue behind: {residue:?}"
    );
}

#[test]
fn repeated_probes_leave_no_residue() {
    // Even under many back-to-back probes in one directory, nothing accumulates.
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("out.csv");
    for _ in 0..16 {
        let _ = case_sensitive_dir(&target).unwrap();
    }
    let residue = probe_residue(dir.path());
    assert!(
        residue.is_empty(),
        "repeated probes accumulated residue: {residue:?}"
    );
}

#[test]
fn collision_key_probes_target_dir_without_residue() {
    // `collision_key` classifies via the same probe; a stray temp file must not
    // survive computing a key against a real directory.
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("dlq.csv");
    let key = collision_key(target.to_str().unwrap());
    assert!(!key.is_empty());
    let residue = probe_residue(dir.path());
    assert!(
        residue.is_empty(),
        "collision_key left probe residue behind: {residue:?}"
    );
}
