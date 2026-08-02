//! Independent dependency and Rust-only executable-surface audits.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::child::{self, ChildSpec, Termination};
use crate::error::GateError;
use crate::limits::DEFAULT_CHILD_OUTPUT_BYTES;

const THIN_LAUNCHERS: [&str; 5] = [
    "scripts/ci/test-filesystem-matrix.sh",
    "scripts/release/build-bundle.sh",
    "scripts/release/check-inventory.sh",
    "scripts/release/check-workflow-trust.sh",
    "scripts/release/verify-release.sh",
];
const FORBIDDEN_IMPLEMENTATIONS: [&str; 8] = [
    "validate-release-decisions.py",
    "release-evidence.py",
    "publish-approved-release.py",
    "test_release_decision_records.py",
    "test_publish_approved_release.py",
    "test-release-decisions.sh",
    "test-release-bundle.sh",
    "test-workflow-trust.sh",
];

/// Explicit boundary family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Dependency,
    RustOnly,
}

/// Run one exact boundary audit.
pub fn audit(scope: Scope, root: &Path) -> Result<(), GateError> {
    let root = root
        .canonicalize()
        .map_err(|error| GateError::io("resolve boundary root", &error))?;
    if !root.is_dir() {
        return Err(policy("boundary root must be a directory"));
    }
    match scope {
        Scope::Dependency => audit_dependency(&root),
        Scope::RustOnly => audit_rust_only(&root),
    }
}

fn audit_dependency(root: &Path) -> Result<(), GateError> {
    let mut environment = BTreeMap::new();
    for name in ["PATH", "CARGO_BUILD_JOBS", "CARGO_INCREMENTAL", "NO_COLOR"] {
        if let Some(value) = std::env::var_os(name) {
            environment.insert(OsString::from(name), value);
        }
    }
    let arguments = vec![
        OsString::from("run"),
        OsString::from("--quiet"),
        OsString::from("--manifest-path"),
        root.join("tools/dependency-policy/Cargo.toml")
            .into_os_string(),
        OsString::from("--target-dir"),
        root.join("target/clinker-release-policy-dependency-audit")
            .into_os_string(),
        OsString::from("--locked"),
        OsString::from("--offline"),
        OsString::from("--"),
        OsString::from("--scope"),
        OsString::from("final"),
        OsString::from("--root"),
        root.as_os_str().to_owned(),
    ];
    let result = child::run(ChildSpec {
        program: PathBuf::from("cargo"),
        arguments,
        environment,
        timeout: Duration::from_secs(3600),
        output_limit: DEFAULT_CHILD_OUTPUT_BYTES,
    })?;
    if !matches!(result.termination, Termination::Exited(Some(0)))
        || result.stdout_truncated
        || result.stderr_truncated
    {
        return Err(policy(
            "independent dependency checker failed, timed out, or truncated output",
        ));
    }
    Ok(())
}

fn audit_rust_only(root: &Path) -> Result<(), GateError> {
    let mut findings = BTreeSet::new();
    audit_workflows(root, &mut findings)?;
    audit_script_surfaces(root, &mut findings)?;
    audit_python_files(root, &mut findings)?;
    if let Some(first) = findings.into_iter().next() {
        return Err(policy(first));
    }
    Ok(())
}

fn audit_workflows(root: &Path, findings: &mut BTreeSet<String>) -> Result<(), GateError> {
    let directory = root.join(".github/workflows");
    for path in sorted_files(&directory)? {
        if !matches!(
            path.extension().and_then(|value| value.to_str()),
            Some("yml" | "yaml")
        ) {
            continue;
        }
        let contents = read_text(&path, "read workflow for Rust-only audit")?;
        let relative = relative(root, &path);
        if contains_interpreter_or_legacy(&contents) {
            findings.insert(format!(
                "{relative}: workflow invokes a non-Rust interpreter"
            ));
        }
        for line in contents.lines().map(str::trim) {
            if let Some(command) = line.strip_prefix("run:") {
                let command = command.trim();
                if command.contains("scripts/release/") || command.contains("scripts/ci/") {
                    findings.insert(format!(
                        "{relative}: workflow delegates governed semantics to a script"
                    ));
                }
                if command.contains("Command::new")
                    || command.contains("timeout 0")
                    || command.contains("while true")
                {
                    findings.insert(format!(
                        "{relative}: workflow contains an unbounded command"
                    ));
                }
            }
        }
        if matches!(
            path.file_name().and_then(|value| value.to_str()),
            Some(
                "ci.yml"
                    | "ci.yaml"
                    | "release.yml"
                    | "release.yaml"
                    | "publish-release.yml"
                    | "publish-release.yaml"
            )
        ) && !(contents.contains("tools/release-policy/Cargo.toml")
            && contents.contains("--locked")
            && contents.contains("--offline"))
        {
            findings.insert(format!(
                "{relative}: governed workflow lacks a direct locked/offline Rust gate invocation"
            ));
        }
    }
    Ok(())
}

fn audit_script_surfaces(root: &Path, findings: &mut BTreeSet<String>) -> Result<(), GateError> {
    let allowed: BTreeSet<&str> = THIN_LAUNCHERS.into_iter().collect();
    for directory in [root.join("scripts/ci"), root.join("scripts/release")] {
        for path in sorted_files(&directory)? {
            if path.starts_with(root.join("scripts/release/fixtures")) {
                continue;
            }
            let relative = relative(root, &path);
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("");
            if FORBIDDEN_IMPLEMENTATIONS.contains(&name) {
                findings.insert(format!(
                    "{relative}: non-Rust implementation remains active"
                ));
                continue;
            }
            let metadata = fs::metadata(&path)
                .map_err(|error| GateError::io("read script metadata", &error))?;
            let executable = metadata.permissions().mode() & 0o111 != 0;
            let interpreted = matches!(
                path.extension().and_then(|value| value.to_str()),
                Some("sh" | "py")
            );
            if !executable && !interpreted {
                continue;
            }
            if !allowed.contains(relative.as_str()) {
                findings.insert(format!(
                    "{relative}: executable CI/release surface is not an approved thin launcher"
                ));
                continue;
            }
            validate_thin_launcher(&path, &relative, findings)?;
        }
    }
    Ok(())
}

fn validate_thin_launcher(
    path: &Path,
    relative: &str,
    findings: &mut BTreeSet<String>,
) -> Result<(), GateError> {
    let contents = read_text(path, "read thin launcher")?;
    let required = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "SCRIPT_DIR=",
        "REPO_ROOT=",
        "exec cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline --",
        "\"$@\"",
    ];
    if required.iter().any(|fragment| !contents.contains(fragment))
        || contains_interpreter_or_legacy(&contents)
        || contents.lines().any(|line| {
            let line = line.trim_start();
            line.starts_with("if ")
                || line.starts_with("for ")
                || line.starts_with("while ")
                || line.starts_with("case ")
        })
    {
        findings.insert(format!(
            "{relative}: approved launcher contains semantics beyond strict Rust delegation"
        ));
    }
    Ok(())
}

fn audit_python_files(root: &Path, findings: &mut BTreeSet<String>) -> Result<(), GateError> {
    for directory in [
        ".github", "benches", "crates", "docs", "examples", "release", "scripts", "tools",
    ] {
        for path in sorted_files(&root.join(directory))? {
            if path.extension().and_then(|value| value.to_str()) == Some("py") {
                findings.insert(format!(
                    "{}: Python source is forbidden in the Rust-only repository",
                    relative(root, &path)
                ));
            }
        }
    }
    Ok(())
}

fn contains_interpreter_or_legacy(contents: &str) -> bool {
    let lower = contents.to_ascii_lowercase();
    lower.contains("python")
        || lower.contains(".py")
        || FORBIDDEN_IMPLEMENTATIONS
            .iter()
            .any(|name| lower.contains(&name.to_ascii_lowercase()))
}

fn sorted_files(directory: &Path) -> Result<Vec<PathBuf>, GateError> {
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let mut pending = vec![directory.to_path_buf()];
    let mut files = Vec::new();
    while let Some(current) = pending.pop() {
        let entries = fs::read_dir(&current)
            .map_err(|error| GateError::io("enumerate Rust-only surface", &error))?;
        let mut paths = entries
            .map(|entry| {
                entry
                    .map(|entry| entry.path())
                    .map_err(|error| GateError::io("enumerate Rust-only surface", &error))
            })
            .collect::<Result<Vec<_>, _>>()?;
        paths.sort();
        for path in paths.into_iter().rev() {
            let metadata = fs::symlink_metadata(&path)
                .map_err(|error| GateError::io("inspect Rust-only surface", &error))?;
            if metadata.file_type().is_symlink() {
                return Err(policy("Rust-only audit refuses symbolic links"));
            }
            if metadata.is_dir() {
                pending.push(path);
            } else if metadata.is_file() {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn read_text(path: &Path, operation: &'static str) -> Result<String, GateError> {
    let bytes = fs::read(path).map_err(|error| GateError::io(operation, &error))?;
    if bytes.len() > 2 * 1024 * 1024 {
        return Err(policy("Rust-only surface exceeds the 2 MiB scan limit"));
    }
    String::from_utf8(bytes).map_err(|_| policy("Rust-only surface must be UTF-8"))
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn policy(detail: impl Into<String>) -> GateError {
    GateError::policy("boundary.audit", detail)
}
