//! Rust-only policy for dependencies on the shared failure vocabulary.

mod manifest;
#[cfg(test)]
mod manifest_tests;
mod sha256;
mod source;
#[cfg(test)]
mod source_tests;
#[cfg(test)]
mod test_support;

use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;

use serde_json::Value as JsonValue;

pub use manifest::LOCK_PACKAGE_DIGEST;

/// One independently checkable dependency policy boundary scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scope {
    Core,
    ClinkerNet,
    ClinkerLineage,
    Final,
}

impl Scope {
    fn includes_net(self) -> bool {
        matches!(self, Self::ClinkerNet | Self::Final)
    }

    fn includes_lineage(self) -> bool {
        matches!(self, Self::ClinkerLineage | Self::Final)
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Core => "core",
            Self::ClinkerNet => "clinker-net",
            Self::ClinkerLineage => "clinker-lineage",
            Self::Final => "final",
        })
    }
}

impl FromStr for Scope {
    type Err = BoundaryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "core" => Ok(Self::Core),
            "clinker-net" => Ok(Self::ClinkerNet),
            "clinker-lineage" => Ok(Self::ClinkerLineage),
            "final" => Ok(Self::Final),
            _ => Err(BoundaryError::new(format!(
                "scope must be one of core, clinker-net, clinker-lineage, final; found {value:?}"
            ))),
        }
    }
}

/// A deterministic dependency policy contract violation or checker failure.
#[derive(Debug)]
pub struct BoundaryError {
    message: String,
}

impl BoundaryError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub(crate) fn context(self, context: impl AsRef<str>) -> Self {
        Self::new(format!("{}: {}", context.as_ref(), self.message))
    }
}

impl fmt::Display for BoundaryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for BoundaryError {}

pub type BoundaryResult<T> = Result<T, BoundaryError>;

/// Check one dependency policy scope against the repository and live Cargo metadata.
pub fn check_repository(root: &Path, scope: Scope) -> BoundaryResult<()> {
    let root = root
        .canonicalize()
        .map_err(|error| BoundaryError::new(format!("cannot resolve repository root: {error}")))?;
    let metadata = load_metadata(&root)?;
    check_repository_with_metadata(&root, scope, &metadata)
}

/// Check one dependency policy scope with supplied Cargo metadata.
///
/// This seam keeps fixture tests deterministic without weakening the live CLI,
/// which always obtains metadata from Cargo itself.
pub fn check_repository_with_metadata(
    root: &Path,
    scope: Scope,
    metadata: &JsonValue,
) -> BoundaryResult<()> {
    manifest::check_core(root)?;
    manifest::check_lock_membership(root)?;
    source::check_core_source(root)?;

    if scope.includes_net() {
        manifest::check_consumer(root, "clinker-net")?;
        source::check_consumer_source(root, "clinker-net")?;
    }
    if scope.includes_lineage() {
        manifest::check_consumer(root, "clinker-lineage")?;
        source::check_consumer_source(root, "clinker-lineage")?;
    }
    if scope == Scope::Final {
        manifest::check_final_crate_map(root)?;
    }
    manifest::check_metadata(root, metadata, scope)?;
    Ok(())
}

fn load_metadata(root: &Path) -> BoundaryResult<JsonValue> {
    let output = Command::new("cargo")
        .args([
            "metadata",
            "--locked",
            "--offline",
            "--no-deps",
            "--format-version",
            "1",
        ])
        .current_dir(root)
        .output()
        .map_err(|error| BoundaryError::new(format!("cannot execute cargo metadata: {error}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let detail = stderr
            .lines()
            .last()
            .unwrap_or("cargo metadata failed without diagnostics");
        return Err(BoundaryError::new(format!(
            "cargo metadata --locked --offline --no-deps failed: {detail}"
        )));
    }
    serde_json::from_slice(&output.stdout).map_err(|error| {
        BoundaryError::new(format!("cargo metadata returned invalid JSON: {error}"))
    })
}

pub(crate) fn crate_source_root(root: &Path, crate_name: &str) -> PathBuf {
    root.join("crates").join(crate_name).join("src")
}
