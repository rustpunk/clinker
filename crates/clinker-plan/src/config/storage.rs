//! Workspace-level `[storage]` configuration parsed from `clinker.toml`.
//!
//! Unlike pipeline-level knobs (which live in the per-pipeline YAML under
//! `pipeline:`), storage settings are a property of the *workspace* — the
//! same physical spill volume and staging policy apply to every pipeline run
//! anchored at a given `clinker.toml`. They are deserialized here from the
//! workspace-root `clinker.toml`, validated once at executor startup, and
//! threaded into the run as runtime parameters rather than as part of the
//! compiled plan.
//!
//! The spill root directory and the staging block are both honored at
//! runtime. This module owns the *decision and validation* surface: it parses
//! the `[storage.staging]` block, validates it at startup
//! ([`StagingPolicy::validate`]), and matches source paths against the
//! configured patterns ([`StagingPolicy::compile_matcher`]). The copy itself —
//! single-pass streamed copy with inline BLAKE3 verification and atomic
//! publish — lives in `clinker-channel`'s staging-copy engine, which consumes
//! the decision this module makes.

use crate::config::utils::ByteSize;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Top-level `clinker.toml` document.
///
/// The `[storage]`, `[observability]`, `[channel]`, and `[group]` tables are
/// modeled. Any other top-level table is tolerated rather than rejected — this
/// type is consulted for workspace deployment policy and layout roots, so
/// unknown top-level tables (future workspace-discovery keys) pass through.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClinkerToml {
    /// Typed workspace resource catalog. Logical names are scoped by kind,
    /// while every physical target remains anchored to this workspace.
    #[serde(default)]
    pub catalog: crate::resources::CatalogConfig,
    /// The `[storage]` table. Absent → every field defaults (spill to the
    /// OS temp dir, staging off), matching pre-config behavior exactly.
    #[serde(default)]
    pub storage: StorageConfig,
    /// Complete deployment observability policy. Absence means disabled;
    /// presence is resolved atomically before any execution effect.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observability: Option<super::observability::ObservabilityConfig>,
    /// The `[channel]` table: the workspace root under which per-channel
    /// folders live and the directory-sharding scheme used to enumerate them.
    /// Absent → `root = "channel"`, `shard = none` (see [`ChannelLayout`]).
    #[serde(default)]
    pub channel: ChannelLayout,
    /// The `[group]` table: the workspace root under which group definition
    /// files live. Absent → `root = "group"` (see [`GroupLayout`]).
    #[serde(default)]
    pub group: GroupLayout,
}

impl ClinkerToml {
    /// Parse a `clinker.toml` document from its raw text.
    ///
    /// # Errors
    ///
    /// Returns [`StorageConfigError::Parse`] when the text is not valid TOML
    /// or contains a key whose type does not match the schema.
    pub fn parse(text: &str) -> Result<Self, StorageConfigError> {
        toml::from_str(text).map_err(|e| StorageConfigError::Parse(e.to_string()))
    }

    /// Resolve an absent policy as disabled or one present policy atomically.
    ///
    /// A complete replacement is accepted only when the workspace table is
    /// absent. Field-by-field merging is deliberately unsupported.
    pub fn resolve_observability(
        &self,
        complete_replacement: Option<super::observability::ResolvedObservabilityPolicy>,
    ) -> Result<
        super::observability::ResolvedObservabilityPolicy,
        super::observability::ObservabilityConfigError,
    > {
        match (&self.observability, complete_replacement) {
            (Some(_), Some(_)) => Err(super::observability::ObservabilityConfigError::invalid(
                "observability",
                "conflicts with a complete resolved replacement",
                "remove the `[observability]` table to use the complete replacement, or omit the replacement to use workspace policy",
            )),
            (None, Some(replacement)) => Ok(replacement),
            (Some(config), None) => config.resolve(),
            (None, None) => Ok(super::observability::ResolvedObservabilityPolicy::disabled()),
        }
    }

    /// Read and parse the `clinker.toml` at `workspace_root`, returning the
    /// default (empty) document when no such file exists.
    ///
    /// A missing `clinker.toml` is not an error: a workspace with no storage
    /// opinions runs with the inherited defaults. An unreadable or malformed
    /// file *is* an error so a typo in the storage block surfaces at startup
    /// rather than being silently ignored.
    ///
    /// # Errors
    ///
    /// Returns [`StorageConfigError::Read`] when the file exists but cannot
    /// be read, or [`StorageConfigError::Parse`] when its contents are not
    /// valid TOML.
    pub fn load_from_workspace(workspace_root: &Path) -> Result<Self, StorageConfigError> {
        let path = workspace_root.join("clinker.toml");
        match std::fs::read_to_string(&path) {
            Ok(text) => Self::parse(&text),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(StorageConfigError::Read {
                path: path.clone(),
                source: e.to_string(),
            }),
        }
    }
}

/// The `[storage]` block: spill and staging policy for a workspace.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// `[storage.spill]` — where blocking operators (Aggregate, sort,
    /// grace-hash Combine, node-buffer overflow) write their spill files.
    #[serde(default)]
    pub spill: SpillConfig,
    /// `[storage.staging]` — opt-in copy of source files to local disk
    /// before execution. Off by default; activated per-pipeline by pattern
    /// match. Validated at startup, then driven by `clinker-channel`'s
    /// staging-copy engine per matched source.
    #[serde(default)]
    pub staging: StagingPolicy,
    /// `[storage.publication]` — bounded output publication and retained
    /// attempt policy.
    #[serde(default)]
    pub publication: PublicationPolicy,
}

const GIGABYTE: u64 = 1_000_000_000;
const DEFAULT_FAILED_RETENTION_SECONDS: u64 = 86_400;
const MAX_FAILED_RETENTION_SECONDS: u64 = 604_800;
const DEFAULT_CREATION_GRACE_SECONDS: u64 = 300;
const MAX_CREATION_GRACE_SECONDS: u64 = 3_600;
const DEFAULT_MAX_ATTEMPT_BYTES: u64 = 4 * GIGABYTE;
const MAX_MAX_ATTEMPT_BYTES: u64 = 16 * GIGABYTE;
const DEFAULT_RETAINED_BYTE_LIMIT: u64 = 8 * GIGABYTE;
const MAX_RETAINED_BYTE_LIMIT: u64 = 64 * GIGABYTE;
const DEFAULT_RETAINED_ATTEMPT_LIMIT: u64 = 8;
pub const PUBLICATION_MAX_RETAINED_ATTEMPTS: u64 = 128;
const DEFAULT_MIN_FREE_BYTES: u64 = 2 * GIGABYTE;
const MAX_MIN_FREE_BYTES: u64 = 64 * GIGABYTE;
const DEFAULT_SWEEP_ENTRY_LIMIT: u64 = 1_000;
const MAX_SWEEP_ENTRY_LIMIT: u64 = 10_000;
const DEFAULT_SWEEP_BYTE_LIMIT: u64 = 8 * GIGABYTE;
const MAX_SWEEP_BYTE_LIMIT: u64 = 64 * GIGABYTE;
const DEFAULT_SWEEP_TIME_LIMIT_MS: u64 = 2_000;
const MAX_SWEEP_TIME_LIMIT_MS: u64 = 30_000;
/// Maximum durable attempt-manifest bytes that one cleanup page must inspect
/// in addition to the largest admitted artifact set.
pub const PUBLICATION_MANIFEST_MAX_BYTES: u64 = 4 * 1024 * 1024;

fn default_failed_retention_seconds() -> u64 {
    DEFAULT_FAILED_RETENTION_SECONDS
}

fn default_creation_grace_seconds() -> u64 {
    DEFAULT_CREATION_GRACE_SECONDS
}

fn default_max_attempt_bytes() -> ByteSize {
    ByteSize(DEFAULT_MAX_ATTEMPT_BYTES)
}

fn default_retained_byte_limit() -> ByteSize {
    ByteSize(DEFAULT_RETAINED_BYTE_LIMIT)
}

fn default_retained_attempt_limit() -> u64 {
    DEFAULT_RETAINED_ATTEMPT_LIMIT
}

fn default_min_free_bytes() -> ByteSize {
    ByteSize(DEFAULT_MIN_FREE_BYTES)
}

fn default_sweep_entry_limit() -> u64 {
    DEFAULT_SWEEP_ENTRY_LIMIT
}

fn default_sweep_byte_limit() -> ByteSize {
    ByteSize(DEFAULT_SWEEP_BYTE_LIMIT)
}

fn default_sweep_time_limit_ms() -> u64 {
    DEFAULT_SWEEP_TIME_LIMIT_MS
}

/// How output bytes reach destination-local publication quarantine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationMode {
    /// Write directly into quarantine on the destination filesystem.
    #[default]
    Direct,
    /// Write into a restrictive local spool, then copy and verify into
    /// destination-local quarantine before promotion.
    LocalThenPublish,
}

/// Qualified filesystem profile selected for the publication destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationProfile {
    /// A durable local filesystem.
    #[default]
    Local,
    /// A qualified NFSv4.1 destination.
    NfsV4_1,
    /// A qualified SMB3.1.1 destination.
    #[serde(rename = "smb_3_1_1")]
    Smb3_1_1,
}

impl DestinationProfile {
    /// Author-facing profile spelling used by config and diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::NfsV4_1 => "nfs_v4_1",
            Self::Smb3_1_1 => "smb_3_1_1",
        }
    }
}

/// Meaning of the capacity value exposed in publication explanation data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationCapacity {
    /// A one-time free-space observation used only as an admission check.
    AdvisoryObservation,
}

/// Support verdict exposed in publication explanation data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationSupportStatus {
    /// The selected profile passed local admission checks.
    Supported,
}

/// Stable typed fields for text and structured publication explanations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PublicationExplain {
    /// Resolved `publication.mode`.
    pub mode: PublicationMode,
    /// Resolved `publication.destination_profile`.
    pub destination_profile: DestinationProfile,
    /// Resolved `publication.failed_retention_seconds`.
    pub failed_retention_seconds: u64,
    /// Capacity admission semantics.
    pub capacity: PublicationCapacity,
    /// Checked upper-bound estimate supplied at admission.
    pub estimated_attempt_bytes: u64,
    /// Free bytes observed once at admission.
    pub observed_free_bytes: u64,
    /// Support verdict for the selected profile.
    pub support_status: PublicationSupportStatus,
}

/// Strict `[storage.publication]` author configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationPolicy {
    /// Publication routing mode.
    #[serde(default)]
    pub mode: PublicationMode,
    /// Qualified destination filesystem profile.
    #[serde(default)]
    pub destination_profile: DestinationProfile,
    /// Required local spool for local-then-publish mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_spool_dir: Option<PathBuf>,
    /// Retention for failed attempts in seconds. Zero is allowed.
    #[serde(default = "default_failed_retention_seconds")]
    pub failed_retention_seconds: u64,
    /// Grace period for attempts whose creation may still be in progress.
    #[serde(default = "default_creation_grace_seconds")]
    pub creation_grace_seconds: u64,
    /// Maximum admitted byte estimate for one attempt.
    #[serde(default = "default_max_attempt_bytes")]
    pub max_attempt_bytes: ByteSize,
    /// Maximum aggregate bytes retained for attempts.
    #[serde(default = "default_retained_byte_limit")]
    pub retained_byte_limit: ByteSize,
    /// Maximum number of retained attempts.
    #[serde(default = "default_retained_attempt_limit")]
    pub retained_attempt_limit: u64,
    /// Additional free-space headroom required at admission.
    #[serde(default = "default_min_free_bytes")]
    pub min_free_bytes: ByteSize,
    /// Maximum directory entries considered by one cleanup sweep.
    #[serde(default = "default_sweep_entry_limit")]
    pub sweep_entry_limit: u64,
    /// Maximum bytes considered by one cleanup sweep.
    #[serde(default = "default_sweep_byte_limit")]
    pub sweep_byte_limit: ByteSize,
    /// Maximum elapsed milliseconds spent by one cleanup sweep.
    #[serde(default = "default_sweep_time_limit_ms")]
    pub sweep_time_limit_ms: u64,
}

impl Default for PublicationPolicy {
    fn default() -> Self {
        Self {
            mode: PublicationMode::Direct,
            destination_profile: DestinationProfile::Local,
            local_spool_dir: None,
            failed_retention_seconds: DEFAULT_FAILED_RETENTION_SECONDS,
            creation_grace_seconds: DEFAULT_CREATION_GRACE_SECONDS,
            max_attempt_bytes: default_max_attempt_bytes(),
            retained_byte_limit: default_retained_byte_limit(),
            retained_attempt_limit: DEFAULT_RETAINED_ATTEMPT_LIMIT,
            min_free_bytes: default_min_free_bytes(),
            sweep_entry_limit: DEFAULT_SWEEP_ENTRY_LIMIT,
            sweep_byte_limit: default_sweep_byte_limit(),
            sweep_time_limit_ms: DEFAULT_SWEEP_TIME_LIMIT_MS,
        }
    }
}

/// Publication policy after strict validation and advisory admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPublicationPolicy {
    mode: PublicationMode,
    destination_profile: DestinationProfile,
    local_spool_dir: Option<PathBuf>,
    failed_retention_seconds: u64,
    creation_grace_seconds: u64,
    max_attempt_bytes: u64,
    retained_byte_limit: u64,
    retained_attempt_limit: u64,
    min_free_bytes: u64,
    sweep_entry_limit: u64,
    sweep_byte_limit: u64,
    sweep_time_limit_ms: u64,
    estimated_attempt_bytes: u64,
    observed_free_bytes: u64,
}

impl PublicationPolicy {
    /// Resolve this policy before any attempt or output is created.
    ///
    /// `observed_free_bytes` is a single admission-time observation supplied
    /// by the caller. It reserves no capacity and cannot guarantee that later
    /// writes or syncs avoid `ENOSPC` or `EDQUOT`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageConfigError`] for invalid bounds, profile mismatch,
    /// an unsuitable local spool, checked-arithmetic overflow, an oversized
    /// estimate, or insufficient observed free space.
    pub fn resolve(
        &self,
        destination_root: &Path,
        estimated_attempt_bytes: u64,
        observed_free_bytes: u64,
    ) -> Result<ResolvedPublicationPolicy, StorageConfigError> {
        let filesystem_family =
            crate::config::fs_type::classify_family(destination_root).map_err(|error| {
                StorageConfigError::PublicationDestinationUnprobeable {
                    path: destination_root.to_path_buf(),
                    source: error.to_string(),
                }
            })?;
        self.resolve_for_filesystem_family(
            destination_root,
            filesystem_family,
            estimated_attempt_bytes,
            observed_free_bytes,
        )
    }

    fn resolve_for_filesystem_family(
        &self,
        _destination_root: &Path,
        filesystem_family: crate::config::FilesystemFamily,
        estimated_attempt_bytes: u64,
        observed_free_bytes: u64,
    ) -> Result<ResolvedPublicationPolicy, StorageConfigError> {
        self.validate_bounds()?;
        let required_free_bytes = estimated_attempt_bytes
            .checked_add(self.min_free_bytes.0)
            .ok_or(StorageConfigError::PublicationCapacityOverflow {
                estimated_attempt_bytes,
                min_free_bytes: self.min_free_bytes.0,
            })?;

        match (self.destination_profile, filesystem_family) {
            (DestinationProfile::Local, crate::config::FilesystemFamily::Local)
            | (DestinationProfile::NfsV4_1, crate::config::FilesystemFamily::Nfs)
            | (DestinationProfile::Smb3_1_1, crate::config::FilesystemFamily::Smb) => {}
            (profile, detected) => {
                return Err(StorageConfigError::PublicationProfileMismatch { profile, detected });
            }
        }

        let local_spool_dir = self.validate_local_spool()?;
        if estimated_attempt_bytes > self.max_attempt_bytes.0 {
            return Err(StorageConfigError::PublicationEstimateExceedsLimit {
                key: "max_attempt_bytes",
                estimated_attempt_bytes,
                limit_bytes: self.max_attempt_bytes.0,
            });
        }
        if estimated_attempt_bytes > self.retained_byte_limit.0 {
            return Err(StorageConfigError::PublicationEstimateExceedsLimit {
                key: "retained_byte_limit",
                estimated_attempt_bytes,
                limit_bytes: self.retained_byte_limit.0,
            });
        }
        if observed_free_bytes < required_free_bytes {
            return Err(StorageConfigError::PublicationCapacityInsufficient {
                estimated_attempt_bytes,
                min_free_bytes: self.min_free_bytes.0,
                observed_free_bytes,
                required_free_bytes,
            });
        }

        Ok(ResolvedPublicationPolicy {
            mode: self.mode,
            destination_profile: self.destination_profile,
            local_spool_dir,
            failed_retention_seconds: self.failed_retention_seconds,
            creation_grace_seconds: self.creation_grace_seconds,
            max_attempt_bytes: self.max_attempt_bytes.0,
            retained_byte_limit: self.retained_byte_limit.0,
            retained_attempt_limit: self.retained_attempt_limit,
            min_free_bytes: self.min_free_bytes.0,
            sweep_entry_limit: self.sweep_entry_limit,
            sweep_byte_limit: self.sweep_byte_limit.0,
            sweep_time_limit_ms: self.sweep_time_limit_ms,
            estimated_attempt_bytes,
            observed_free_bytes,
        })
    }

    fn validate_bounds(&self) -> Result<(), StorageConfigError> {
        validate_publication_bound(
            "failed_retention_seconds",
            self.failed_retention_seconds,
            MAX_FAILED_RETENTION_SECONDS,
            true,
            "failed_retention_seconds = 604800",
        )?;
        validate_publication_bound(
            "creation_grace_seconds",
            self.creation_grace_seconds,
            MAX_CREATION_GRACE_SECONDS,
            false,
            "creation_grace_seconds = 3600",
        )?;
        validate_publication_bound(
            "max_attempt_bytes",
            self.max_attempt_bytes.0,
            MAX_MAX_ATTEMPT_BYTES,
            false,
            "max_attempt_bytes = \"16GB\"",
        )?;
        validate_publication_bound(
            "retained_byte_limit",
            self.retained_byte_limit.0,
            MAX_RETAINED_BYTE_LIMIT,
            false,
            "retained_byte_limit = \"64GB\"",
        )?;
        validate_publication_bound(
            "retained_attempt_limit",
            self.retained_attempt_limit,
            PUBLICATION_MAX_RETAINED_ATTEMPTS,
            false,
            "retained_attempt_limit = 128",
        )?;
        validate_publication_bound(
            "min_free_bytes",
            self.min_free_bytes.0,
            MAX_MIN_FREE_BYTES,
            false,
            "min_free_bytes = \"64GB\"",
        )?;
        validate_publication_bound(
            "sweep_entry_limit",
            self.sweep_entry_limit,
            MAX_SWEEP_ENTRY_LIMIT,
            false,
            "sweep_entry_limit = 10000",
        )?;
        validate_publication_bound(
            "sweep_byte_limit",
            self.sweep_byte_limit.0,
            MAX_SWEEP_BYTE_LIMIT,
            false,
            "sweep_byte_limit = \"64GB\"",
        )?;
        let minimum_sweep_bytes = self
            .max_attempt_bytes
            .0
            .checked_add(PUBLICATION_MANIFEST_MAX_BYTES)
            .ok_or(StorageConfigError::PublicationSweepCapacityOverflow {
                max_attempt_bytes: self.max_attempt_bytes.0,
                manifest_overhead_bytes: PUBLICATION_MANIFEST_MAX_BYTES,
            })?;
        if self.sweep_byte_limit.0 < minimum_sweep_bytes {
            return Err(StorageConfigError::PublicationSweepCapacityTooSmall {
                sweep_byte_limit: self.sweep_byte_limit.0,
                max_attempt_bytes: self.max_attempt_bytes.0,
                manifest_overhead_bytes: PUBLICATION_MANIFEST_MAX_BYTES,
                minimum_sweep_bytes,
            });
        }
        validate_publication_bound(
            "sweep_time_limit_ms",
            self.sweep_time_limit_ms,
            MAX_SWEEP_TIME_LIMIT_MS,
            false,
            "sweep_time_limit_ms = 30000",
        )
    }

    fn validate_local_spool(&self) -> Result<Option<PathBuf>, StorageConfigError> {
        if self.mode == PublicationMode::Direct {
            return Ok(self.local_spool_dir.clone());
        }

        let path = self
            .local_spool_dir
            .as_ref()
            .ok_or(StorageConfigError::PublicationLocalSpoolRequired)?;
        let metadata = std::fs::metadata(path).map_err(|error| {
            StorageConfigError::PublicationLocalSpoolInvalid {
                path: path.clone(),
                source: error.to_string(),
            }
        })?;
        if !metadata.is_dir() {
            return Err(StorageConfigError::PublicationLocalSpoolInvalid {
                path: path.clone(),
                source: "path is not a directory".to_string(),
            });
        }
        let kind = crate::config::fs_type::classify(path).map_err(|error| {
            StorageConfigError::PublicationLocalSpoolInvalid {
                path: path.clone(),
                source: error.to_string(),
            }
        })?;
        if kind != crate::config::FsKind::Local {
            return Err(StorageConfigError::PublicationLocalSpoolInvalid {
                path: path.clone(),
                source: format!("detected {kind:?} filesystem; a local filesystem is required"),
            });
        }

        let probe = tempfile::Builder::new()
            .prefix(".clinker-publication-probe-")
            .tempfile_in(path)
            .map_err(|error| StorageConfigError::PublicationLocalSpoolInvalid {
                path: path.clone(),
                source: error.to_string(),
            })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = probe
                .as_file()
                .metadata()
                .map_err(|error| StorageConfigError::PublicationLocalSpoolInvalid {
                    path: path.clone(),
                    source: error.to_string(),
                })?
                .permissions()
                .mode();
            if mode & 0o077 != 0 {
                return Err(StorageConfigError::PublicationLocalSpoolInvalid {
                    path: path.clone(),
                    source: format!("probe file mode {:o} is not owner-only", mode & 0o777),
                });
            }
        }
        drop(probe);
        Ok(Some(path.clone()))
    }
}

fn validate_publication_bound(
    key: &'static str,
    value: u64,
    maximum: u64,
    zero_allowed: bool,
    correction: &'static str,
) -> Result<(), StorageConfigError> {
    if (!zero_allowed && value == 0) || value > maximum {
        return Err(StorageConfigError::PublicationValueOutOfRange {
            key,
            value,
            maximum,
            zero_allowed,
            correction,
        });
    }
    Ok(())
}

impl ResolvedPublicationPolicy {
    /// Resolved publication routing mode.
    pub fn mode(&self) -> PublicationMode {
        self.mode
    }

    /// Qualified destination profile admitted for this run.
    pub fn destination_profile(&self) -> DestinationProfile {
        self.destination_profile
    }

    /// Local spool used by local-then-publish mode, if configured.
    pub fn local_spool_dir(&self) -> Option<&Path> {
        self.local_spool_dir.as_deref()
    }

    /// Failed-attempt retention duration in seconds.
    pub fn failed_retention_seconds(&self) -> u64 {
        self.failed_retention_seconds
    }

    /// Creation grace duration in seconds.
    pub fn creation_grace_seconds(&self) -> u64 {
        self.creation_grace_seconds
    }

    /// Maximum estimated bytes for one attempt.
    pub fn max_attempt_bytes(&self) -> u64 {
        self.max_attempt_bytes
    }

    /// Aggregate retained-attempt byte limit.
    pub fn retained_byte_limit(&self) -> u64 {
        self.retained_byte_limit
    }

    /// Retained-attempt count limit.
    pub fn retained_attempt_limit(&self) -> u64 {
        self.retained_attempt_limit
    }

    /// Admission headroom in bytes.
    pub fn min_free_bytes(&self) -> u64 {
        self.min_free_bytes
    }

    /// Maximum entries considered by one cleanup sweep.
    pub fn sweep_entry_limit(&self) -> u64 {
        self.sweep_entry_limit
    }

    /// Maximum bytes considered by one cleanup sweep.
    pub fn sweep_byte_limit(&self) -> u64 {
        self.sweep_byte_limit
    }

    /// Maximum elapsed milliseconds for one cleanup sweep.
    pub fn sweep_time_limit_ms(&self) -> u64 {
        self.sweep_time_limit_ms
    }

    /// Typed fields for text and structured publication explanations.
    pub fn explain(&self) -> PublicationExplain {
        PublicationExplain {
            mode: self.mode,
            destination_profile: self.destination_profile,
            failed_retention_seconds: self.failed_retention_seconds,
            capacity: PublicationCapacity::AdvisoryObservation,
            estimated_attempt_bytes: self.estimated_attempt_bytes,
            observed_free_bytes: self.observed_free_bytes,
            support_status: PublicationSupportStatus::Supported,
        }
    }

    /// Capacity admission never reserves blocks or quota.
    pub fn reserves_capacity(&self) -> bool {
        false
    }

    /// A successful admission observation cannot guarantee completion.
    pub fn guarantees_completion(&self) -> bool {
        false
    }

    /// Later writes and syncs can truthfully fail with `ENOSPC` or `EDQUOT`.
    pub fn late_enospc_or_edquot_possible(&self) -> bool {
        true
    }
}

/// Default `[channel]` root when the table or its `root` key is omitted.
fn default_channel_root() -> PathBuf {
    PathBuf::from("channel")
}

/// Default `[group]` root when the table or its `root` key is omitted.
fn default_group_root() -> PathBuf {
    PathBuf::from("group")
}

/// The `[channel]` block: where per-channel folders live and how they shard.
///
/// This is pure layout declaration — parsing only, no behavior is wired here.
/// `root` anchors the per-channel folder tree (`<root>/<channel-id>/…`) so a
/// `--channel <id>` invocation resolves by a computed path rather than an
/// O(N) workspace glob-scan. `shard` records the directory-sharding scheme
/// used when materializing and enumerating that tree; it is enumeration
/// ergonomics only (keeping `ls`/editor/git listings small), not a lookup
/// requirement, so it defaults to [`ShardScheme::None`].
///
/// Absent table → both fields default (`root = "channel"`, `shard = none`),
/// matching a workspace that has not opted into an explicit layout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelLayout {
    /// Workspace-relative (or absolute) root under which per-channel folders
    /// live. Omitted → `"channel"`. A relative path is resolved against the
    /// workspace root by the discovery layer, not normalized here.
    #[serde(default = "default_channel_root")]
    pub root: PathBuf,
    /// Directory-sharding scheme for the channel tree. Omitted →
    /// [`ShardScheme::None`] (flat: one folder per channel directly under
    /// `root`).
    #[serde(default)]
    pub shard: ShardScheme,
}

impl Default for ChannelLayout {
    fn default() -> Self {
        Self {
            root: default_channel_root(),
            shard: ShardScheme::default(),
        }
    }
}

/// The `[group]` block: where group definition files live.
///
/// Layout declaration only — no behavior is wired here. `root` anchors the
/// directory holding `*.group.yaml` definitions. Absent table → `root =
/// "group"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupLayout {
    /// Workspace-relative (or absolute) root under which group definition
    /// files live. Omitted → `"group"`. A relative path is resolved against
    /// the workspace root by the discovery layer, not normalized here.
    #[serde(default = "default_group_root")]
    pub root: PathBuf,
}

impl Default for GroupLayout {
    fn default() -> Self {
        Self {
            root: default_group_root(),
        }
    }
}

/// How the per-channel folder tree is sharded on disk for enumeration.
///
/// Sharding is an enumeration-ergonomics choice, not a lookup requirement: a
/// `--channel <id>` invocation always resolves by a computed path, so on ext4
/// htree single-name lookup stays ~O(log n) regardless. `readdir` over one
/// giant flat directory, however, stays O(N); sharding splits the tree into
/// smaller directories so `ls`, editors, and git enumerate a bounded fan-out.
/// It is therefore opt-in, defaulting to [`ShardScheme::None`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ShardScheme {
    /// Flat: every channel folder sits directly under the channel root. The
    /// default — the smallest layout, correct for workspaces below the
    /// directory-fan-out sizes where enumeration cost bites.
    #[default]
    None,
    /// One intermediate directory per leading channel-id character
    /// (`<root>/<first-char>/<channel-id>/`), bounding fan-out by the id
    /// alphabet.
    FirstChar,
    /// Hash the channel id into a fixed set of shard buckets
    /// (`<root>/<hash-bucket>/<channel-id>/`), spreading channels evenly
    /// regardless of id-prefix skew.
    Hash,
}

/// How spill files are compressed: `auto` (the default), `off`, or `on`.
///
/// Spill bodies are postcard-encoded record streams. LZ4 frame compression
/// shrinks large spilled runs, but on small spills the per-frame fixed cost
/// — clearing the compressor's internal state on every frame reset — can
/// dominate the byte savings. The LZ4 v1.8.2 release notes call this out
/// directly, and Pentaho Kettle ships explicit guidance to disable spill
/// compression for small rows. `Auto` encodes that guidance as a heuristic
/// so the common case needs no tuning; `Off` / `On` force the choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressMode {
    /// Compress only when the projected spill is large enough that LZ4's
    /// per-frame fixed cost is amortized (see [`CompressMode::resolve`]).
    #[default]
    Auto,
    /// Never compress: postcard records are written straight to disk with no
    /// LZ4 frame wrapping. Cheapest for small spills; largest on-disk size.
    Off,
    /// Always compress with an LZ4 frame, regardless of projected size. The
    /// pre-knob behavior, kept for spills of large, compressible rows.
    On,
}

/// Minimum projected bytes-per-batch at which `auto` enables compression.
///
/// Below ~4 KiB a spilled batch fits inside a single LZ4 block, so the frame
/// reset cost the v1.8.2 release notes flag is paid with little compressible
/// volume to offset it. 4 KiB matches the small-row threshold Pentaho Kettle
/// documents.
const AUTO_COMPRESS_MIN_BYTES_PER_BATCH: u64 = 4 * 1024;

/// Minimum projected rows-per-batch at which `auto` enables compression.
///
/// Pairs with [`AUTO_COMPRESS_MIN_BYTES_PER_BATCH`]: a batch must be both
/// wide (≥ 4 KiB) and tall (≥ 1024 rows) before compression pays for itself.
/// 1024 is the small-row row-count threshold from the same Pentaho guidance.
const AUTO_COMPRESS_MIN_ROWS_PER_BATCH: u64 = 1024;

/// Per-column byte estimate used to project a spilled batch's size from a
/// schema's column count alone.
///
/// A `Value` slot is 32 bytes (its widest variant is the 24-byte inline
/// string plus the enum discriminant); short strings and fixed-width scalars
/// carry no extra heap, so the per-column slot width alone is the estimate.
/// The projection only has to land on the correct side of the 4 KiB
/// threshold, so a coarse per-column constant is sufficient and keeps the
/// heuristic a pure function of the schema width and batch size.
const ESTIMATED_BYTES_PER_COLUMN: u64 = 32;

impl CompressMode {
    /// Resolve this mode against a projected spill batch's size into a
    /// concrete "compress this file?" decision.
    ///
    /// `On` and `Off` ignore the projection. `Auto` compresses only when the
    /// batch is projected to be both ≥ 4 KiB and ≥ 1024 rows — the point at
    /// which LZ4's per-frame fixed cost is amortized by enough compressible
    /// volume (see [`CompressMode`]).
    pub fn resolve(self, projected_bytes_per_batch: u64, projected_rows_per_batch: u64) -> bool {
        match self {
            CompressMode::On => true,
            CompressMode::Off => false,
            CompressMode::Auto => {
                projected_bytes_per_batch >= AUTO_COMPRESS_MIN_BYTES_PER_BATCH
                    && projected_rows_per_batch >= AUTO_COMPRESS_MIN_ROWS_PER_BATCH
            }
        }
    }

    /// Project a spilled batch's byte size from its schema width and the
    /// configured rows-per-batch, then resolve to a compression decision.
    ///
    /// Convenience over [`CompressMode::resolve`] for callers that hold a
    /// column count and batch size rather than a pre-computed byte figure:
    /// the projection is `column_count × 32 bytes × rows_per_batch`. The
    /// `--explain` plan and the runtime spill writer call this so the
    /// reported mode matches the mode the run actually applies.
    pub fn resolve_for_schema(self, column_count: usize, rows_per_batch: u64) -> bool {
        let bytes_per_row = column_count as u64 * ESTIMATED_BYTES_PER_COLUMN;
        let bytes_per_batch = bytes_per_row.saturating_mul(rows_per_batch);
        self.resolve(bytes_per_batch, rows_per_batch)
    }

    /// Lowercase mode label for JSON output, matching the YAML surface
    /// grammar (`auto` / `off` / `on`). The text `--explain` path renders
    /// the `Debug` form (`Auto` / `Off` / `On`); JSON consumers expect the
    /// wire grammar they would write back, so the two surfaces differ in
    /// case by design.
    pub fn json_label(self) -> &'static str {
        match self {
            CompressMode::Auto => "auto",
            CompressMode::Off => "off",
            CompressMode::On => "on",
        }
    }
}

/// `[storage.spill]` — spill-file root directory.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpillConfig {
    /// Root directory under which the per-run `clinker-spill-*` directory is
    /// created. `None` (key omitted) → the OS temp dir
    /// ([`std::env::temp_dir`]), preserving the historical default.
    ///
    /// A relative path is resolved against the process working directory by
    /// the filesystem layer, not normalized here — operators redirecting
    /// spill to a mounted volume are expected to give an absolute path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dir: Option<PathBuf>,
    /// Cumulative disk-spill quota for the run, in bytes. `None` (key
    /// omitted) → unlimited, preserving the historical default. Accepts a
    /// bare integer (bytes) or a human-readable string (`"500MB"`, `"2GB"`)
    /// through the same [`ByteSize`] parser the source-filter size knobs use,
    /// so a `clinker.toml` author writes one unit grammar across the file.
    ///
    /// When the summed on-disk size of every spill file a run writes crosses
    /// this cap, the run aborts with a dedicated cap-exceeded diagnostic
    /// rather than continuing to fill the volume. The cap is deliberately
    /// distinct from the RSS `memory.limit`: a run can sit comfortably inside
    /// its memory envelope yet still exhaust local disk through an unbounded
    /// stream of spill files, and the operator needs to see "you hit the disk
    /// cap" — not an out-of-memory message — when that happens (the confusing
    /// surface DuckDB hit in duckdb/duckdb#14142, where a temp-dir cap
    /// rendered as "OOM with 187 GiB available").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_cap_bytes: Option<ByteSize>,
    /// Whether spill files are LZ4-compressed. Defaults to [`CompressMode::Auto`],
    /// which compresses only when a spilled batch is projected large enough
    /// to amortize LZ4's per-frame fixed cost. `off` and `on` force the
    /// choice. See [`CompressMode`] for the rationale and threshold.
    #[serde(default)]
    pub compress: CompressMode,
}

impl SpillConfig {
    /// Cumulative spill-quota cap in bytes, or `None` when no
    /// `disk_cap_bytes` was configured (unlimited spill).
    ///
    /// The executor folds `Some(cap)` into the run's memory arbitrator as
    /// the disk-spill quota; `None` leaves the quota at its unlimited
    /// default. Returns a plain `u64` so the executor stays free of the
    /// `ByteSize` newtype.
    pub fn disk_cap(&self) -> Option<u64> {
        self.disk_cap_bytes.map(|ByteSize(n)| n)
    }
}

impl SpillConfig {
    /// Validate that the configured spill `dir` exists and is a writable
    /// directory, returning the resolved root or `None` when no directory
    /// was configured (the OS-temp-dir default).
    ///
    /// Validation runs once at executor startup so a misconfigured spill
    /// volume fails the run before any work begins, rather than at the first
    /// spill — the failure mode DuckDB hit when its `temp_directory` setting
    /// was honored only lazily (duckdb/duckdb#9401).
    ///
    /// # Errors
    ///
    /// Returns [`StorageConfigError::SpillDirMissing`] when the path does not
    /// exist, [`StorageConfigError::SpillDirNotADirectory`] when it exists
    /// but is a file, or [`StorageConfigError::SpillDirNotWritable`] when a
    /// probe write into it fails (permissions, read-only mount).
    pub fn resolve(&self) -> Result<Option<PathBuf>, StorageConfigError> {
        let Some(dir) = self.dir.as_ref() else {
            return Ok(None);
        };
        let meta = std::fs::metadata(dir).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageConfigError::SpillDirMissing { path: dir.clone() }
            } else {
                StorageConfigError::SpillDirNotWritable {
                    path: dir.clone(),
                    source: e.to_string(),
                }
            }
        })?;
        if !meta.is_dir() {
            return Err(StorageConfigError::SpillDirNotADirectory { path: dir.clone() });
        }
        // Probe writability with a real create-and-delete: directory
        // permission bits alone do not guarantee a write succeeds (read-only
        // mount, SELinux, ACLs), and a probe that actually writes catches
        // every case a `mode` inspection would miss.
        let probe = tempfile::Builder::new()
            .prefix(".clinker-spill-probe-")
            .tempfile_in(dir)
            .map_err(|e| StorageConfigError::SpillDirNotWritable {
                path: dir.clone(),
                source: e.to_string(),
            })?;
        // `probe` drops here, unlinking the probe file.
        drop(probe);
        Ok(Some(dir.clone()))
    }
}

/// `[storage.staging]` — source-file staging policy.
///
/// When `enabled`, source files whose path matches one of `patterns` are
/// copied to local disk under `dir` before the pipeline reads them, so a
/// run over a flaky network share (NFS soft-mount, SMB) reads from a stable
/// local copy instead. This mirrors the NiFi `FetchFile` / Airbyte
/// `smart_open` posture: decide-what-to-stage is separated from do-the-copy.
///
/// This type owns the *decision* and *validation* halves:
/// [`StagingPolicy::compile_matcher`] builds the matcher that reports whether a
/// path is selected, and
/// [`StagingPolicy::validate`] runs the startup checks. The copy half — the
/// streamed copy + BLAKE3 verify + atomic publish — lives in `clinker-channel`.
/// Off by default: an empty or absent block leaves every source reading in
/// place exactly as before.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagingPolicy {
    /// Whether source-file staging is enabled. Defaults to `false`, in which
    /// case `patterns` is ignored and every source reads in place.
    #[serde(default)]
    pub enabled: bool,
    /// Local directory the staged copies are written under. Required when
    /// `enabled` — validated at startup to exist, be writable, and sit on a
    /// different volume than every matched source (see
    /// [`StagingPolicy::validate`]). `None` with `enabled = true` is a
    /// startup error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dir: Option<PathBuf>,
    /// Cumulative cap on bytes copied into the staging dir for one run, in
    /// bytes. `None` (omitted) → unlimited. Accepts a bare integer or a
    /// human-readable size (`"50GB"`) through the same [`ByteSize`] grammar
    /// the spill cap uses. Charged by the copy engine before each file is
    /// copied, so a file that would push the run over the cap is refused
    /// rather than copied and rolled back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_cap_bytes: Option<ByteSize>,
    /// Whether a staged copy is integrity-checked against its source after
    /// the copy. Defaults to [`StagingVerify::Blake3`] — a BLAKE3 digest of
    /// source and copy must match, catching the silent truncation an NFS
    /// soft-mount can produce. Consumed by the copy engine.
    #[serde(default)]
    pub verify: StagingVerify,
    /// What to do when a staging-dir copy with the target name already
    /// exists (e.g. a prior crashed run). Defaults to
    /// [`OnExisting::Overwrite`]. Consumed by the copy engine.
    #[serde(default)]
    pub on_existing: OnExisting,
    /// Whether staged copies are removed when the run finishes. Defaults to
    /// [`Cleanup::OnSuccess`] — keep copies after a failure so a re-run can
    /// inspect them, delete them after a clean run. Consumed by the copy
    /// engine.
    #[serde(default)]
    pub cleanup: Cleanup,
    /// Glob patterns selecting which source paths are staged. A source is
    /// staged only when `enabled` and its path matches at least one pattern.
    /// Matched with POSIX/gitignore semantics via the `glob` crate — the
    /// same matcher the source-discovery `exclude:` list uses — against both
    /// the full path and its basename. Empty (the default) ⇒ no source
    /// matches, so `enabled = true` with no patterns stages nothing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub patterns: Vec<String>,
}

/// The staging patterns compiled to [`glob::Pattern`] once, for repeated
/// source-path matching.
///
/// A pipeline run probes every discovered source against the staging patterns —
/// in [`StagingPolicy::validate`], in the per-source stage decision, and in the
/// `--explain` plan. Building this matcher once via
/// [`StagingPolicy::compile_matcher`] and reusing it amortizes the
/// `glob::Pattern::new` parse across all those checks, instead of recompiling
/// every pattern on every path. It is a derived, non-config value, so it lives
/// here rather than as a field on the serde-deserialized [`StagingPolicy`].
#[derive(Debug, Clone)]
pub struct StagingMatcher {
    /// `false` when staging is disabled, so [`StagingMatcher::matches`] short-
    /// circuits to "never matches" without consulting the compiled set.
    enabled: bool,
    /// The parseable patterns, compiled once. Unparseable patterns are dropped
    /// (validity is reported by [`StagingPolicy::validate`] at startup), so a
    /// dropped pattern simply never matches.
    patterns: Vec<glob::Pattern>,
}

impl StagingMatcher {
    /// Whether `path` matches at least one compiled staging pattern.
    ///
    /// Returns `false` when staging is disabled. Each pattern is tested against
    /// both the full path string and the basename (gitignore-style) — `*.csv`
    /// matches a deep path by basename while `/mnt/nfs/**` matches by full path,
    /// mirroring the source-discovery `exclude:` matcher.
    pub fn matches(&self, path: &Path) -> bool {
        if !self.enabled {
            return false;
        }
        let path_str = path.to_string_lossy();
        let basename = path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        self.patterns
            .iter()
            .any(|pat| pat.matches(&path_str) || pat.matches(&basename))
    }
}

/// Post-copy integrity check applied to a staged file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StagingVerify {
    /// Hash source and copy with BLAKE3 and require the digests to match.
    /// The default: an NFS soft-mount can silently truncate a read, and a
    /// content digest is the only check that catches it (a size match does
    /// not). `blake3` reuses the workspace's existing BLAKE3 dependency.
    #[default]
    Blake3,
    /// Skip the post-copy check. Faster, but a truncated or corrupted copy
    /// passes unnoticed — only sensible on a transport already trusted to
    /// deliver complete bytes.
    None,
}

/// What to do when the staging destination already holds a file with the
/// target name (typically left by an earlier crashed run).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnExisting {
    /// Replace the existing file with a fresh copy. The default: a partial
    /// copy from a crashed run must not be trusted, so it is overwritten.
    #[default]
    Overwrite,
    /// Reuse the existing file without re-copying. Trades a re-copy for the
    /// risk of reusing a partial file; only safe when paired with a
    /// post-copy `verify`.
    Reuse,
    /// Fail the run rather than touch an existing destination.
    Error,
}

/// When staged copies are deleted relative to run outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cleanup {
    /// Delete staged copies after a successful run; keep them after a
    /// failure so the inputs a failed run saw can be inspected and re-run
    /// without re-fetching. The default.
    #[default]
    OnSuccess,
    /// Always delete staged copies when the run ends, success or failure.
    Always,
    /// Never delete staged copies; the operator reclaims the staging dir.
    Never,
}

/// A source path after staging resolution.
///
/// `original` is the path the pipeline author wrote; `staged` is the local
/// copy the reader should open instead, or `None` when the source reads in
/// place (staging disabled or no pattern match). Threading this through the
/// reader keeps the read side agnostic to whether staging happened: it opens
/// [`StagedPath::read_path`] regardless.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedPath {
    /// The path declared in the pipeline (the network-share path when
    /// staging is in play).
    pub original: PathBuf,
    /// The local staged copy to read instead, or `None` to read `original`
    /// in place.
    pub staged: Option<PathBuf>,
}

impl StagedPath {
    /// A path that reads in place — `staged` is `None`, so the reader opens
    /// `original` directly.
    pub fn in_place(original: PathBuf) -> Self {
        Self {
            original,
            staged: None,
        }
    }

    /// The path the reader should actually open: the staged copy when one
    /// exists, otherwise the original.
    pub fn read_path(&self) -> &Path {
        self.staged.as_deref().unwrap_or(&self.original)
    }
}

impl StagingPolicy {
    /// Compile the staging patterns once into a reusable [`StagingMatcher`].
    ///
    /// Call this once and reuse the returned matcher for every source-path
    /// check in a run — `validate`'s same-volume loop, the per-source stage
    /// decision, and the `--explain` plan all probe every discovered source, so
    /// compiling the globs once here amortizes the `glob::Pattern::new` parse
    /// across all of them instead of recompiling per path.
    ///
    /// Unparseable patterns are dropped rather than erroring: pattern validity
    /// is reported separately by [`StagingPolicy::validate`] at startup, so by
    /// the time matching runs an invalid pattern has already failed the run —
    /// and a dropped pattern simply never matches, the same behavior the prior
    /// per-call `Err(_) => false` arm gave.
    pub fn compile_matcher(&self) -> StagingMatcher {
        StagingMatcher {
            enabled: self.enabled,
            patterns: self
                .patterns
                .iter()
                .filter_map(|p| glob::Pattern::new(p).ok())
                .collect(),
        }
    }

    /// Validate staging configuration at startup against the set of source
    /// paths a run will read.
    ///
    /// Validation runs once before any input is opened so a misconfigured
    /// staging dir fails the run immediately rather than at the first copy.
    /// When staging is disabled this is a no-op. When enabled it requires
    /// `dir` to be set, to exist as a writable directory, and to sit on a
    /// different volume than every *matched* source — staging onto the same
    /// volume copies bytes without moving I/O off the slow share, a
    /// well-documented anti-pattern. It also rejects an unparseable pattern
    /// so a glob typo surfaces here rather than silently matching nothing.
    ///
    /// # Errors
    ///
    /// Returns [`StorageConfigError::StagingDirUnset`] when enabled without a
    /// `dir`; [`StorageConfigError::StagingDirMissing`] /
    /// [`StorageConfigError::StagingDirNotADirectory`] /
    /// [`StorageConfigError::StagingDirNotWritable`] for a bad dir;
    /// [`StorageConfigError::StagingPatternInvalid`] for an unparseable
    /// pattern; and [`StorageConfigError::StagingSameVolume`] when the
    /// staging dir shares a volume with a matched source.
    pub fn validate(&self, source_paths: &[PathBuf]) -> Result<(), StorageConfigError> {
        if !self.enabled {
            return Ok(());
        }

        for p in &self.patterns {
            glob::Pattern::new(p).map_err(|e| StorageConfigError::StagingPatternInvalid {
                pattern: p.clone(),
                source: e.to_string(),
            })?;
        }

        let dir = self
            .dir
            .as_ref()
            .ok_or(StorageConfigError::StagingDirUnset)?;

        let meta = std::fs::metadata(dir).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageConfigError::StagingDirMissing { path: dir.clone() }
            } else {
                StorageConfigError::StagingDirNotWritable {
                    path: dir.clone(),
                    source: e.to_string(),
                }
            }
        })?;
        if !meta.is_dir() {
            return Err(StorageConfigError::StagingDirNotADirectory { path: dir.clone() });
        }
        // Probe writability with a real create-and-delete: directory mode
        // bits alone do not prove a write succeeds (read-only mount, ACLs).
        let probe = tempfile::Builder::new()
            .prefix(".clinker-staging-probe-")
            .tempfile_in(dir)
            .map_err(|e| StorageConfigError::StagingDirNotWritable {
                path: dir.clone(),
                source: e.to_string(),
            })?;
        drop(probe);

        // Same-volume refusal applies only to matched sources: a source the
        // patterns do not select is read in place regardless of where it
        // lives, so its volume is irrelevant. The same-device probe is the
        // shared filesystem-detection facade — the config layer and the
        // executor-startup checks resolve "same volume?" through one
        // implementation rather than each carrying its own. Compile the glob
        // matcher once for the whole loop rather than per source.
        let matcher = self.compile_matcher();
        for src in source_paths {
            if !matcher.matches(src) {
                continue;
            }
            let same = crate::config::fs_type::same_device(dir, src).map_err(|e| {
                StorageConfigError::StagingDirNotWritable {
                    path: src.clone(),
                    source: e.to_string(),
                }
            })?;
            if same {
                return Err(StorageConfigError::StagingSameVolume {
                    staging_dir: dir.clone(),
                    source: src.clone(),
                });
            }
        }

        Ok(())
    }
}

/// Failure modes when loading or validating `[storage]` configuration.
///
/// Carries enough context (the offending path, the underlying OS message)
/// for the CLI to render a `miette` diagnostic that names the exact
/// `clinker.toml` setting the operator must fix.
#[derive(Debug)]
pub enum StorageConfigError {
    /// `clinker.toml` exists but could not be read.
    Read { path: PathBuf, source: String },
    /// `clinker.toml` is not valid TOML, or a storage key has the wrong type.
    Parse(String),
    /// `storage.spill.dir` points at a path that does not exist.
    SpillDirMissing { path: PathBuf },
    /// `storage.spill.dir` exists but is a file, not a directory.
    SpillDirNotADirectory { path: PathBuf },
    /// `storage.spill.dir` exists and is a directory but cannot be written
    /// (permissions, read-only mount).
    SpillDirNotWritable { path: PathBuf, source: String },
    /// `storage.staging.enabled = true` but no `storage.staging.dir` was set.
    StagingDirUnset,
    /// `storage.staging.dir` points at a path that does not exist.
    StagingDirMissing { path: PathBuf },
    /// `storage.staging.dir` exists but is a file, not a directory.
    StagingDirNotADirectory { path: PathBuf },
    /// `storage.staging.dir` exists and is a directory but cannot be written,
    /// or a path's storage volume could not be determined.
    StagingDirNotWritable { path: PathBuf, source: String },
    /// A `storage.staging.patterns` entry is not a valid glob.
    StagingPatternInvalid { pattern: String, source: String },
    /// `storage.staging.dir` sits on the same volume as a matched source, so
    /// staging would copy bytes without moving I/O off the slow volume.
    StagingSameVolume {
        staging_dir: PathBuf,
        source: PathBuf,
    },
    /// A publication bound is zero when disallowed or exceeds its ceiling.
    PublicationValueOutOfRange {
        key: &'static str,
        value: u64,
        maximum: u64,
        zero_allowed: bool,
        correction: &'static str,
    },
    /// The destination filesystem could not be classified before effects.
    PublicationDestinationUnprobeable { path: PathBuf, source: String },
    /// The selected destination profile disagrees with detection.
    PublicationProfileMismatch {
        profile: DestinationProfile,
        detected: crate::config::FilesystemFamily,
    },
    /// Local-then-publish was selected without a local spool.
    PublicationLocalSpoolRequired,
    /// The local spool is missing, unsuitable, or cannot create an owner-only
    /// probe file.
    PublicationLocalSpoolInvalid { path: PathBuf, source: String },
    /// Estimate plus configured headroom overflowed `u64`.
    PublicationCapacityOverflow {
        estimated_attempt_bytes: u64,
        min_free_bytes: u64,
    },
    /// The configured maximum plus bounded manifest overhead overflowed `u64`.
    PublicationSweepCapacityOverflow {
        max_attempt_bytes: u64,
        manifest_overhead_bytes: u64,
    },
    /// One maximum-sized attempt could not fit in a cleanup page.
    PublicationSweepCapacityTooSmall {
        sweep_byte_limit: u64,
        max_attempt_bytes: u64,
        manifest_overhead_bytes: u64,
        minimum_sweep_bytes: u64,
    },
    /// The attempt estimate exceeds a configured byte limit.
    PublicationEstimateExceedsLimit {
        key: &'static str,
        estimated_attempt_bytes: u64,
        limit_bytes: u64,
    },
    /// The advisory free-space observation is below estimate plus headroom.
    PublicationCapacityInsufficient {
        estimated_attempt_bytes: u64,
        min_free_bytes: u64,
        observed_free_bytes: u64,
        required_free_bytes: u64,
    },
}

impl std::fmt::Display for StorageConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read { path, source } => {
                write!(f, "failed to read {}: {source}", path.display())
            }
            Self::Parse(msg) => write!(f, "invalid clinker.toml: {msg}"),
            Self::SpillDirMissing { path } => write!(
                f,
                "storage.spill.dir {} does not exist; create it or point at an existing volume",
                path.display()
            ),
            Self::SpillDirNotADirectory { path } => write!(
                f,
                "storage.spill.dir {} is a file, not a directory",
                path.display()
            ),
            Self::SpillDirNotWritable { path, source } => write!(
                f,
                "storage.spill.dir {} is not writable: {source}",
                path.display()
            ),
            Self::StagingDirUnset => write!(
                f,
                "storage.staging.enabled is true but storage.staging.dir is not set; \
                 set it to a local directory on a different volume than the staged sources"
            ),
            Self::StagingDirMissing { path } => write!(
                f,
                "storage.staging.dir {} does not exist; create it or point at an existing volume",
                path.display()
            ),
            Self::StagingDirNotADirectory { path } => write!(
                f,
                "storage.staging.dir {} is a file, not a directory",
                path.display()
            ),
            Self::StagingDirNotWritable { path, source } => write!(
                f,
                "storage.staging.dir {} is not writable: {source}",
                path.display()
            ),
            Self::StagingPatternInvalid { pattern, source } => write!(
                f,
                "storage.staging.patterns entry {pattern:?} is not a valid glob: {source}"
            ),
            Self::StagingSameVolume {
                staging_dir,
                source,
            } => write!(
                f,
                "storage.staging.dir {} is on the same volume as source {}; \
                 staging onto the same volume copies bytes without moving I/O off the \
                 source volume — point storage.staging.dir at a local disk on a different volume",
                staging_dir.display(),
                source.display()
            ),
            Self::PublicationValueOutOfRange {
                key,
                value,
                maximum,
                zero_allowed,
                correction,
            } => write!(
                f,
                "storage.publication.{key} value {value} is outside the supported range ({}..={maximum}); use `[storage.publication]\n{correction}`",
                u8::from(!zero_allowed)
            ),
            Self::PublicationDestinationUnprobeable { .. } => write!(
                f,
                "storage.publication.destination_profile could not classify the destination before publication; verify that the destination root exists and is inspectable"
            ),
            Self::PublicationProfileMismatch { profile, detected } => write!(
                f,
                "storage.publication.destination_profile = \"{}\" does not match the detected {} filesystem; {}",
                profile.as_str(),
                detected.as_str(),
                detected.publication_correction()
            ),
            Self::PublicationLocalSpoolRequired => write!(
                f,
                "storage.publication.mode = \"local_then_publish\" requires a restrictively creatable local spool; set `[storage.publication]\nmode = \"local_then_publish\"\nlocal_spool_dir = \"/path/to/local/spool\"`"
            ),
            Self::PublicationLocalSpoolInvalid { .. } => write!(
                f,
                "storage.publication.local_spool_dir is not a restrictively creatable local directory; set `[storage.publication]\nmode = \"local_then_publish\"\nlocal_spool_dir = \"/path/to/local/spool\"`"
            ),
            Self::PublicationCapacityOverflow {
                estimated_attempt_bytes,
                min_free_bytes,
            } => write!(
                f,
                "publication capacity estimate overflow: estimated_attempt_bytes {estimated_attempt_bytes} + storage.publication.min_free_bytes {min_free_bytes}; reduce the estimate or set `[storage.publication]\nmin_free_bytes = \"2GB\"`"
            ),
            Self::PublicationSweepCapacityOverflow {
                max_attempt_bytes,
                manifest_overhead_bytes,
            } => write!(
                f,
                "publication cleanup capacity overflow: storage.publication.max_attempt_bytes {max_attempt_bytes} + bounded manifest overhead {manifest_overhead_bytes}; reduce max_attempt_bytes"
            ),
            Self::PublicationSweepCapacityTooSmall {
                sweep_byte_limit,
                max_attempt_bytes,
                manifest_overhead_bytes,
                minimum_sweep_bytes,
            } => write!(
                f,
                "storage.publication.sweep_byte_limit {sweep_byte_limit} cannot inspect one maximum attempt: max_attempt_bytes {max_attempt_bytes} + bounded manifest overhead {manifest_overhead_bytes} = {minimum_sweep_bytes}; use `[storage.publication]\nsweep_byte_limit = \"{minimum_sweep_bytes}B\"`"
            ),
            Self::PublicationEstimateExceedsLimit {
                key,
                estimated_attempt_bytes,
                limit_bytes,
            } => write!(
                f,
                "estimated_attempt_bytes {estimated_attempt_bytes} exceeds storage.publication.{key} {limit_bytes}; reduce the attempt or set `[storage.publication]\n{key} = \"{estimated_attempt_bytes}B\"` within the documented hard ceiling"
            ),
            Self::PublicationCapacityInsufficient {
                estimated_attempt_bytes,
                min_free_bytes,
                observed_free_bytes,
                required_free_bytes,
            } => write!(
                f,
                "publication observed_free_bytes {observed_free_bytes} is below estimated_attempt_bytes {estimated_attempt_bytes} + storage.publication.min_free_bytes {min_free_bytes} = {required_free_bytes}; this admission check is advisory, does not reserve capacity, and later ENOSPC/EDQUOT remains possible"
            ),
        }
    }
}

impl std::error::Error for StorageConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_document_defaults_to_temp_dir_spill() {
        let doc = ClinkerToml::parse("").unwrap();
        assert!(doc.storage.spill.dir.is_none());
        assert!(!doc.storage.staging.enabled);
    }

    #[test]
    fn local_publication_profile_rejects_detected_share_before_attempt_creation() {
        let destination = tempfile::tempdir().unwrap();
        let attempt_root = destination.path().join(".clinker-attempts");
        let policy = PublicationPolicy::default();

        let error = policy
            .resolve_for_filesystem_family(
                destination.path(),
                crate::config::FilesystemFamily::Nfs,
                1,
                u64::MAX,
            )
            .expect_err("local profile must reject a detected share");

        let rendered = error.to_string();
        assert!(rendered.contains("destination_profile"), "{rendered}");
        assert!(rendered.contains("detected nfs filesystem"), "{rendered}");
        assert!(!attempt_root.exists());
    }

    #[test]
    fn qualified_publication_profiles_reject_other_network_families() {
        let destination = tempfile::tempdir().unwrap();
        for (configured, accepted, rejected) in [
            (
                DestinationProfile::NfsV4_1,
                crate::config::FilesystemFamily::Nfs,
                [
                    crate::config::FilesystemFamily::Smb,
                    crate::config::FilesystemFamily::OtherNetwork,
                ],
            ),
            (
                DestinationProfile::Smb3_1_1,
                crate::config::FilesystemFamily::Smb,
                [
                    crate::config::FilesystemFamily::Nfs,
                    crate::config::FilesystemFamily::OtherNetwork,
                ],
            ),
        ] {
            let policy = PublicationPolicy {
                destination_profile: configured,
                ..PublicationPolicy::default()
            };
            policy
                .resolve_for_filesystem_family(destination.path(), accepted, 1, u64::MAX)
                .expect("matching filesystem family should be admitted");
            for detected in rejected {
                let error = policy
                    .resolve_for_filesystem_family(destination.path(), detected, 1, u64::MAX)
                    .expect_err("a different network family must fail closed");
                assert!(matches!(
                    error,
                    StorageConfigError::PublicationProfileMismatch { .. }
                ));
            }
        }
    }

    #[test]
    fn documented_smb_profile_spelling_parses() {
        let document =
            ClinkerToml::parse("[storage.publication]\ndestination_profile = \"smb_3_1_1\"\n")
                .expect("documented SMB profile should parse");
        assert_eq!(
            document.storage.publication.destination_profile,
            DestinationProfile::Smb3_1_1
        );
    }

    #[test]
    fn missing_clinker_toml_yields_defaults() {
        let empty = tempfile::tempdir().unwrap();
        let doc = ClinkerToml::load_from_workspace(empty.path()).unwrap();
        assert!(doc.storage.spill.dir.is_none());
    }

    #[test]
    fn storage_block_parses_spill_dir_and_staging() {
        let doc = ClinkerToml::parse(
            r#"
            [storage.spill]
            dir = "/var/clinker/spill"

            [storage.staging]
            enabled = true
            "#,
        )
        .unwrap();
        assert_eq!(
            doc.storage.spill.dir.as_deref(),
            Some(Path::new("/var/clinker/spill"))
        );
        assert!(doc.storage.staging.enabled);
    }

    #[test]
    fn unknown_storage_key_is_rejected() {
        let err = ClinkerToml::parse(
            r#"
            [storage.spill]
            directory = "/typo/key"
            "#,
        )
        .unwrap_err();
        assert!(matches!(err, StorageConfigError::Parse(_)));
    }

    #[test]
    fn resolve_none_when_dir_absent() {
        let cfg = SpillConfig::default();
        assert!(cfg.resolve().unwrap().is_none());
    }

    #[test]
    fn resolve_ok_for_existing_writable_dir() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = SpillConfig {
            dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        };
        let resolved = cfg.resolve().unwrap();
        assert_eq!(resolved.as_deref(), Some(dir.path()));
    }

    #[test]
    fn resolve_errors_for_missing_dir() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        let cfg = SpillConfig {
            dir: Some(missing),
            ..Default::default()
        };
        assert!(matches!(
            cfg.resolve().unwrap_err(),
            StorageConfigError::SpillDirMissing { .. }
        ));
    }

    #[test]
    fn resolve_errors_when_path_is_a_file() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let cfg = SpillConfig {
            dir: Some(file.path().to_path_buf()),
            ..Default::default()
        };
        assert!(matches!(
            cfg.resolve().unwrap_err(),
            StorageConfigError::SpillDirNotADirectory { .. }
        ));
    }

    #[test]
    fn disk_cap_absent_yields_none() {
        let doc = ClinkerToml::parse(
            r#"
            [storage.spill]
            dir = "/var/clinker/spill"
            "#,
        )
        .unwrap();
        assert_eq!(doc.storage.spill.disk_cap(), None);
    }

    #[test]
    fn disk_cap_parses_human_readable_string() {
        let doc = ClinkerToml::parse(
            r#"
            [storage.spill]
            disk_cap_bytes = "10GB"
            "#,
        )
        .unwrap();
        // Decimal units, matching the ByteSize grammar used elsewhere:
        // 10 GB = 10_000_000_000 bytes.
        assert_eq!(doc.storage.spill.disk_cap(), Some(10_000_000_000));
    }

    #[test]
    fn disk_cap_parses_bare_integer_as_bytes() {
        let doc = ClinkerToml::parse(
            r#"
            [storage.spill]
            disk_cap_bytes = 1048576
            "#,
        )
        .unwrap();
        assert_eq!(doc.storage.spill.disk_cap(), Some(1_048_576));
    }

    #[test]
    fn disk_cap_rejects_unparseable_size() {
        let err = ClinkerToml::parse(
            r#"
            [storage.spill]
            disk_cap_bytes = "ten gigabytes"
            "#,
        )
        .unwrap_err();
        assert!(matches!(err, StorageConfigError::Parse(_)));
    }

    #[test]
    fn compress_defaults_to_auto() {
        let doc = ClinkerToml::parse("").unwrap();
        assert_eq!(doc.storage.spill.compress, CompressMode::Auto);
    }

    #[test]
    fn compress_parses_each_mode() {
        for (text, expected) in [
            ("auto", CompressMode::Auto),
            ("off", CompressMode::Off),
            ("on", CompressMode::On),
        ] {
            let doc =
                ClinkerToml::parse(&format!("[storage.spill]\ncompress = \"{text}\"\n")).unwrap();
            assert_eq!(doc.storage.spill.compress, expected, "mode {text}");
        }
    }

    #[test]
    fn compress_rejects_unknown_mode() {
        let err = ClinkerToml::parse(
            r#"
            [storage.spill]
            compress = "gzip"
            "#,
        )
        .unwrap_err();
        assert!(matches!(err, StorageConfigError::Parse(_)));
    }

    #[test]
    fn resolve_on_off_ignore_projection() {
        // `on` / `off` are forced regardless of the projected batch size.
        assert!(CompressMode::On.resolve(0, 0));
        assert!(!CompressMode::Off.resolve(u64::MAX, u64::MAX));
    }

    #[test]
    fn resolve_auto_needs_both_thresholds() {
        // Both the byte and the row threshold must be met.
        assert!(CompressMode::Auto.resolve(4096, 1024));
        assert!(CompressMode::Auto.resolve(64 * 1024, 4096));
        // Wide but too few rows → no compression.
        assert!(!CompressMode::Auto.resolve(64 * 1024, 1023));
        // Many rows but too few bytes → no compression.
        assert!(!CompressMode::Auto.resolve(4095, 8192));
    }

    #[test]
    fn resolve_for_schema_projects_from_width_and_rows() {
        // 1 column × 32 B/col × 1024 rows = 32 KiB ≥ 4 KiB and rows ≥ 1024 →
        // compress.
        assert!(CompressMode::Auto.resolve_for_schema(1, 1024));
        // 8 columns × 32 B/col × 16 rows = 4 KiB of bytes but only 16 rows →
        // below the row threshold, so no compression.
        assert!(!CompressMode::Auto.resolve_for_schema(8, 16));
    }

    #[test]
    fn staging_defaults_are_off_and_safe() {
        let p = StagingPolicy::default();
        assert!(!p.enabled);
        assert!(p.dir.is_none());
        assert!(p.patterns.is_empty());
        assert_eq!(p.verify, StagingVerify::Blake3);
        assert_eq!(p.on_existing, OnExisting::Overwrite);
        assert_eq!(p.cleanup, Cleanup::OnSuccess);
    }

    #[test]
    fn staging_block_parses_all_knobs() {
        let doc = ClinkerToml::parse(
            r#"
            [storage.staging]
            enabled = true
            dir = "/var/clinker/staging"
            disk_cap_bytes = "50GB"
            verify = "none"
            on_existing = "reuse"
            cleanup = "always"
            patterns = ["/mnt/nfs/data/**", "*.csv"]
            "#,
        )
        .unwrap();
        let s = &doc.storage.staging;
        assert!(s.enabled);
        assert_eq!(s.dir.as_deref(), Some(Path::new("/var/clinker/staging")));
        assert_eq!(s.disk_cap_bytes.map(|ByteSize(n)| n), Some(50_000_000_000));
        assert_eq!(s.verify, StagingVerify::None);
        assert_eq!(s.on_existing, OnExisting::Reuse);
        assert_eq!(s.cleanup, Cleanup::Always);
        assert_eq!(s.patterns, vec!["/mnt/nfs/data/**", "*.csv"]);
    }

    #[test]
    fn unknown_staging_key_is_rejected() {
        let err = ClinkerToml::parse(
            r#"
            [storage.staging]
            enabledx = true
            "#,
        )
        .unwrap_err();
        assert!(matches!(err, StorageConfigError::Parse(_)));
    }

    #[test]
    fn compiled_matcher_matches_full_path_basename_and_disabled() {
        // The compile-once matcher reproduces the full matching contract: a
        // full-path glob, a basename glob, a non-match, a path matching both
        // globs, a path with no basename, an invalid pattern that drops to
        // never-match, and the disabled short-circuit. A trailing `[` is an
        // invalid glob: it must compile-drop and never match, exactly as the
        // prior per-call `Err(_) => false` arm did.
        let p = StagingPolicy {
            enabled: true,
            patterns: vec!["/mnt/nfs/**".into(), "*.csv".into(), "[".into()],
            ..Default::default()
        };
        let cases = [
            ("/mnt/nfs/data/orders.json", true),  // full-path glob
            ("/local/data/orders.csv", true),     // basename glob
            ("/local/data/orders.json", false),   // no pattern matches
            ("/mnt/nfs/deep/nested/x.csv", true), // matches both globs
            ("/", false),                         // no basename, no match
        ];
        // One matcher, built once, must give the right verdict for every path —
        // proving the optimization changed performance, not behavior.
        let matcher = p.compile_matcher();
        for (path, expected) in cases {
            assert_eq!(matcher.matches(Path::new(path)), expected, "{path}");
        }
        // A disabled policy never matches, regardless of patterns.
        let disabled = StagingPolicy {
            enabled: false,
            patterns: vec!["**".into()],
            ..Default::default()
        };
        assert!(!disabled.compile_matcher().matches(Path::new("/anything")));
    }

    #[test]
    fn staged_path_read_path_prefers_staged_copy() {
        let in_place = StagedPath::in_place(PathBuf::from("/a/b.csv"));
        assert_eq!(in_place.read_path(), Path::new("/a/b.csv"));
        let copied = StagedPath {
            original: PathBuf::from("/mnt/nfs/b.csv"),
            staged: Some(PathBuf::from("/local/b.csv")),
        };
        assert_eq!(copied.read_path(), Path::new("/local/b.csv"));
    }

    #[test]
    fn validate_noop_when_disabled() {
        let p = StagingPolicy::default();
        assert!(p.validate(&[PathBuf::from("/mnt/nfs/x.csv")]).is_ok());
    }

    #[test]
    fn validate_requires_dir_when_enabled() {
        let p = StagingPolicy {
            enabled: true,
            patterns: vec!["*.csv".into()],
            ..Default::default()
        };
        assert!(matches!(
            p.validate(&[]).unwrap_err(),
            StorageConfigError::StagingDirUnset
        ));
    }

    #[test]
    fn validate_rejects_missing_dir() {
        let dir = tempfile::tempdir().unwrap();
        let p = StagingPolicy {
            enabled: true,
            dir: Some(dir.path().join("nope")),
            patterns: vec!["*.csv".into()],
            ..Default::default()
        };
        assert!(matches!(
            p.validate(&[]).unwrap_err(),
            StorageConfigError::StagingDirMissing { .. }
        ));
    }

    #[test]
    fn validate_rejects_dir_that_is_a_file() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let p = StagingPolicy {
            enabled: true,
            dir: Some(file.path().to_path_buf()),
            patterns: vec!["*.csv".into()],
            ..Default::default()
        };
        assert!(matches!(
            p.validate(&[]).unwrap_err(),
            StorageConfigError::StagingDirNotADirectory { .. }
        ));
    }

    #[test]
    fn validate_rejects_invalid_pattern() {
        let dir = tempfile::tempdir().unwrap();
        let p = StagingPolicy {
            enabled: true,
            dir: Some(dir.path().to_path_buf()),
            patterns: vec!["[".into()],
            ..Default::default()
        };
        assert!(matches!(
            p.validate(&[]).unwrap_err(),
            StorageConfigError::StagingPatternInvalid { .. }
        ));
    }

    #[test]
    fn validate_rejects_same_volume_matched_source() {
        // A matched source on the staging dir's own volume is refused (both
        // live under the same tempdir, hence the same device).
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("orders.csv");
        std::fs::write(&src, b"a,b\n").unwrap();
        let p = StagingPolicy {
            enabled: true,
            dir: Some(dir.path().to_path_buf()),
            patterns: vec!["*.csv".into()],
            ..Default::default()
        };
        assert!(matches!(
            p.validate(&[src]).unwrap_err(),
            StorageConfigError::StagingSameVolume { .. }
        ));
    }

    #[test]
    fn channel_and_group_absent_use_defaults() {
        // A document with no [channel]/[group] tables falls back to the
        // conventional roots and the flat shard scheme.
        let doc = ClinkerToml::parse("").unwrap();
        assert_eq!(doc.channel.root, Path::new("channel"));
        assert_eq!(doc.channel.shard, ShardScheme::None);
        assert_eq!(doc.group.root, Path::new("group"));
    }

    #[test]
    fn channel_section_present_but_empty_uses_key_defaults() {
        // The table exists but sets no keys: each key defaults independently.
        let doc = ClinkerToml::parse("[channel]\n").unwrap();
        assert_eq!(doc.channel.root, Path::new("channel"));
        assert_eq!(doc.channel.shard, ShardScheme::None);
    }

    #[test]
    fn channel_section_parses_root_and_each_shard_scheme() {
        for (text, expected) in [
            ("none", ShardScheme::None),
            ("first-char", ShardScheme::FirstChar),
            ("hash", ShardScheme::Hash),
        ] {
            let doc = ClinkerToml::parse(&format!(
                "[channel]\nroot = \"tenants\"\nshard = \"{text}\"\n"
            ))
            .unwrap();
            assert_eq!(doc.channel.root, Path::new("tenants"));
            assert_eq!(doc.channel.shard, expected, "shard {text}");
        }
    }

    #[test]
    fn channel_root_defaults_when_only_shard_set() {
        // Setting one key leaves the other at its default.
        let doc = ClinkerToml::parse("[channel]\nshard = \"hash\"\n").unwrap();
        assert_eq!(doc.channel.root, Path::new("channel"));
        assert_eq!(doc.channel.shard, ShardScheme::Hash);
    }

    #[test]
    fn group_section_parses_root() {
        let doc = ClinkerToml::parse("[group]\nroot = \"cohorts\"\n").unwrap();
        assert_eq!(doc.group.root, Path::new("cohorts"));
    }

    #[test]
    fn group_section_present_but_empty_uses_default_root() {
        let doc = ClinkerToml::parse("[group]\n").unwrap();
        assert_eq!(doc.group.root, Path::new("group"));
    }

    #[test]
    fn channel_rejects_unknown_shard_scheme() {
        let err = ClinkerToml::parse("[channel]\nshard = \"round-robin\"\n").unwrap_err();
        assert!(matches!(err, StorageConfigError::Parse(_)));
    }

    #[test]
    fn channel_rejects_unknown_key() {
        let err = ClinkerToml::parse("[channel]\nroots = \"channel\"\n").unwrap_err();
        assert!(matches!(err, StorageConfigError::Parse(_)));
    }

    #[test]
    fn group_rejects_unknown_key() {
        let err = ClinkerToml::parse("[group]\nshard = \"none\"\n").unwrap_err();
        assert!(matches!(err, StorageConfigError::Parse(_)));
    }

    #[test]
    fn channel_root_rejects_wrong_type() {
        // `root` is a path (string); an integer is a type mismatch.
        let err = ClinkerToml::parse("[channel]\nroot = 3\n").unwrap_err();
        assert!(matches!(err, StorageConfigError::Parse(_)));
    }

    #[test]
    fn channel_and_group_coexist_with_storage() {
        // All three top-level tables parse together, and an unrelated unknown
        // top-level table is still tolerated (ClinkerToml is not
        // deny_unknown_fields).
        let doc = ClinkerToml::parse(
            r#"
            [storage.spill]
            dir = "/var/clinker/spill"

            [channel]
            root = "channel"
            shard = "first-char"

            [group]
            root = "group"

            [future_discovery]
            anything = true
            "#,
        )
        .unwrap();
        assert_eq!(
            doc.storage.spill.dir.as_deref(),
            Some(Path::new("/var/clinker/spill"))
        );
        assert_eq!(doc.channel.shard, ShardScheme::FirstChar);
        assert_eq!(doc.group.root, Path::new("group"));
    }

    #[test]
    fn layout_defaults_match_absent_document() {
        // The Default impls agree with parsing an empty document, so callers
        // that construct a ClinkerToml programmatically see the same roots.
        assert_eq!(ChannelLayout::default().root, Path::new("channel"));
        assert_eq!(ChannelLayout::default().shard, ShardScheme::None);
        assert_eq!(GroupLayout::default().root, Path::new("group"));
    }

    #[test]
    fn validate_ignores_same_volume_unmatched_source() {
        // An unmatched source on the same volume is fine: it reads in place,
        // so the same-volume rule does not apply to it.
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("orders.json");
        std::fs::write(&src, b"{}").unwrap();
        let p = StagingPolicy {
            enabled: true,
            dir: Some(dir.path().to_path_buf()),
            patterns: vec!["*.csv".into()],
            ..Default::default()
        };
        assert!(p.validate(&[src]).is_ok());
    }
}
