//! Source-file staging copy: single-pass streamed copy with inline BLAKE3
//! verification, atomic publish, a stable content-addressed cache layout, and
//! durability/permission hardening.
//!
//! This is the body that plugs into the staging resolution seam: when the
//! workspace [`StagingPolicy`] selects a source path, [`SourceStager`] copies
//! the file to a local volume and returns a [`StagedPath`] whose `staged`
//! points at the local copy, so the reader opens the stable local bytes
//! instead of a flaky network share.
//!
//! ## Why staging exists
//!
//! A pipeline reading directly off a network mount is exposed to several
//! silent-corruption modes that a plain `File::open` cannot detect:
//!
//! - `man 5 nfs`: a *soft* mount can time out mid-read and return short data
//!   with no error — the bytes simply stop, and a size check alone does not
//!   catch a truncation that happens to land on a record boundary.
//! - sshfs with `cache=yes` can return stale or truncated reads after the
//!   server-side file changes under it.
//! - UDP-transported NFS over a saturated link can deliver reordered or
//!   dropped fragments that reassemble into wrong bytes.
//!
//! Staging copies the whole file to local disk once, hashes it while copying,
//! and (by default) verifies the digest, so any of these corruptions surfaces
//! as a hard error at stage time rather than as wrong output downstream.
//!
//! ## Cache layout
//!
//! Each source maps to a **stable, content-addressed** pair of paths under the
//! staging root, deterministic across runs of the same source:
//!
//! - `<source_id>.staged` — the local copy the reader opens.
//! - `<source_id>.manifest.json` — a sidecar recording the source identity
//!   (`StagedManifest`: path + mtime + size + content hash + stage time).
//!
//! `source_id` is the hex BLAKE3 of the canonicalized absolute source path, so
//! the same source always resolves to the same staged file. That stability is
//! what makes [`OnExisting::Reuse`] (reuse-if-fresh) functional: a later run
//! finds the prior `<source_id>.staged` + manifest, compares the recorded
//! mtime/size against the live source, and skips the copy when they match.
//!
//! The manifest is the **commit marker**: a `.staged` file is only trustworthy
//! once its manifest exists. The write order — copy `.staged`, then publish the
//! manifest — means a crash before the manifest lands leaves a `.staged` with
//! no manifest, which the startup [`SourceStager::crash_purge`] treats as
//! orphaned and reaps. A clean (staged + manifest) pair is the reuse cache and
//! is preserved by the purge.
//!
//! The per-file in-flight copy lands at `<source_id>.<run_uuid>.partial`. The
//! `run_uuid` segment keeps two concurrent invocations staging the *same*
//! source from writing the same partial — each writes its own — so they race
//! only on the per-source lock, never on the in-flight bytes.
//!
//! ## Concurrency: one copy under a per-source lock
//!
//! Clinker's partition-and-run scaling story has several `clinker` processes
//! over one partitioned input share a single staging volume, so two siblings
//! routinely stage the *same* shared source at the same time. Without
//! coordination both would see "not staged yet", both copy, and one run's
//! startup [`SourceStager::crash_purge`] could reap the other's in-flight
//! `.partial`. Two mechanisms make concurrent staging of one source correct:
//!
//! - **A per-source advisory lock** (`<source_id>.lock` under the staging root,
//!   an `fs4` OS lock — `flock` on Unix, `LockFileEx` on Windows). A stage takes
//!   the exclusive lock, *re-checks* reuse-if-fresh **under** the lock (so a
//!   sibling that just published wins and the loser reuses without copying),
//!   else copies, publishes `.staged`, writes the manifest, and only then
//!   releases the lock. A concurrent sibling blocks on the lock and, on
//!   acquiring it, finds the now-fresh `.staged` and reuses it — exactly one
//!   copy of a given source across any number of overlapping invocations. The
//!   kernel drops the lock when the process exits (clean, panicked, or killed),
//!   so a crash never strands it.
//! - **A liveness-aware crash-purge.** A `.partial` is reaped only when its
//!   owning run is genuinely dead — its `<source_id>.lock` is *acquirable* under
//!   a try-lock — and it is older than a short creation grace window. A live
//!   sibling's in-flight `.partial` (lock held, or too young to have taken the
//!   lock yet) is never reaped. This mirrors the executor's spill crash-purge.
//!
//! ## Copy invariants
//!
//! - **Single pass.** Each ~1 MiB chunk is read once and fed to both the
//!   BLAKE3 hasher and the destination file. No second read of the source, so
//!   the copy is a memory-budget no-op (one fixed buffer, no per-file
//!   accumulation).
//! - **Atomic publish.** Bytes land in `<source_id>.<run_uuid>.partial`, are
//!   `sync_all`'d, then `rename`d to `<source_id>.staged`. `std::fs::rename`
//!   is an atomic-replace on Linux/macOS/Windows (Win10 1607+), so a concurrent
//!   reader sees either no `.staged` file or the complete one — never a torn
//!   write.
//! - **Durable rename (POSIX).** After the rename the parent directory is
//!   `sync_all`'d so the rename survives a crash. NTFS does not need this.
//! - **Restrictive permissions (Unix).** The staging root entries are owner-only
//!   (`0o600` for staged files and manifests), because staged copies hold
//!   verbatim source records — potentially PII or credentials — on what may be
//!   a shared volume.

use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use clinker_plan::config::{Cleanup, OnExisting, StagedPath, StagingPolicy, StagingVerify};
// `fs4::FileExt` adds `lock` (blocking exclusive) / `try_lock` (non-blocking
// exclusive) / `unlock` to `std::fs::File` under the crate's `sync` feature (the
// only fs4 feature this workspace enables), giving one cross-platform OS
// advisory lock (`flock` on Unix, `LockFileEx` on Windows). Same primitive the
// executor's spill crash-purge uses.
use fs4::FileExt;
use serde::{Deserialize, Serialize};

/// Chunk size for the streamed copy. 1 MiB amortizes per-`read`/`write`
/// syscall overhead against a fixed, bounded buffer — large enough to keep the
/// pipe full on a fast local disk, small enough that the copy never scales its
/// memory with file size.
const COPY_CHUNK_BYTES: usize = 1024 * 1024;

/// Filename extension for a published staged copy.
const STAGED_EXT: &str = "staged";
/// Filename suffix for a staged copy's identity manifest.
const MANIFEST_SUFFIX: &str = ".manifest.json";
/// Filename extension for the per-source advisory lock that serializes
/// concurrent stages of one source.
const LOCK_EXT: &str = "lock";
/// Filename suffix for a per-file in-flight copy, namespaced by run uuid.
const PARTIAL_SUFFIX: &str = ".partial";
/// Filename suffix for the in-flight manifest write (`<source_id>.manifest.json`
/// staged as `<source_id>.manifest.json.partial` before its atomic rename).
const MANIFEST_PARTIAL_SUFFIX: &str = ".manifest.json.partial";

/// How recently a `<source_id>.<run_uuid>.partial` must have been created before
/// the crash-purge will consider reaping it, independent of its owning run's
/// lock state.
///
/// A stage creates its `.partial` and then writes into it; the per-source lock
/// is taken before either. But a sibling that holds the lock and is still
/// `stat`-ing the source has not yet created its `.partial`, and conversely a
/// run that crashed the instant after `create` but before the kernel registered
/// the lock could leave a lockless newborn partial. The grace window means a
/// freshly created `.partial` is never reaped even if its lock probe says
/// "acquirable", closing that narrow window. Five seconds dwarfs the
/// sub-millisecond create→write latency yet stays far below the gap between
/// runs, so a true crash corpse still ages past the window and is reaped on a
/// later startup. Mirrors the executor spill crash-purge's grace window.
const REAP_GRACE: Duration = Duration::from_secs(5);

/// Failure modes of a staging copy.
///
/// Each variant carries the offending path so the CLI can render a diagnostic
/// that names the exact file the operator must inspect. [`StagingError::Verify`]
/// is deliberately distinct from [`StagingError::Io`]: a digest mismatch is the
/// silent-corruption signal staging exists to catch, and the operator needs to
/// see "the copy did not match the source" — not a generic I/O error.
#[derive(Debug, thiserror::Error)]
pub enum StagingError {
    /// An OS-level I/O error while reading the source, writing the temp file,
    /// fsyncing, renaming, or setting permissions.
    #[error("staging I/O error for {path}: {source}")]
    Io {
        /// The path being read or written when the error occurred.
        path: PathBuf,
        /// The underlying OS error.
        source: io::Error,
    },
    /// The staged copy's BLAKE3 digest did not match the source's. The bytes on
    /// the network share and the bytes that landed locally differ — exactly the
    /// soft-mount silent-truncation mode staging guards against.
    #[error(
        "staged copy of {source_path} is corrupt: BLAKE3 of the local copy \
         ({copy_hash}) does not match the source ({source_hash}); \
         the transport delivered different bytes than the source holds"
    )]
    Verify {
        /// The original source path.
        source_path: PathBuf,
        /// Hex BLAKE3 digest computed while copying.
        copy_hash: String,
        /// Hex BLAKE3 digest of a fresh independent read of the source.
        source_hash: String,
    },
    /// The cumulative bytes copied this run would exceed
    /// `storage.staging.disk_cap_bytes`. Distinct from a full-disk
    /// [`StagingError::Io`]: the operator hit a configured budget, not the
    /// volume's physical limit.
    #[error(
        "staging would exceed the configured cap of {cap} bytes \
         (already staged {staged}, {source_path} adds {incoming})"
    )]
    DiskCapExceeded {
        /// The configured `storage.staging.disk_cap_bytes`.
        cap: u64,
        /// Bytes already copied this run before this file.
        staged: u64,
        /// Size of the file that would push the run over the cap.
        incoming: u64,
        /// The source path that triggered the overflow.
        source_path: PathBuf,
    },
    /// `on_existing = error` and a staged copy of this source already exists.
    /// The operator asked to be told rather than silently reuse or overwrite a
    /// prior artifact, so the run fails fast naming the existing copy.
    #[error(
        "staging on_existing = error: a staged copy of {source_path} already \
         exists at {staged_path}; remove it or set on_existing to overwrite or reuse"
    )]
    AlreadyExists {
        /// The original source path.
        source_path: PathBuf,
        /// The pre-existing staged copy that blocked the run.
        staged_path: PathBuf,
    },
}

/// A staged copy's recorded identity, persisted as the `<source_id>.manifest.json`
/// sidecar next to the staged file.
///
/// Two roles: (1) it is the **commit marker** — a `.staged` file is only
/// trustworthy once this manifest exists, so an interrupted copy is detectable
/// as a `.staged` with no manifest; (2) it carries the source's mtime and size
/// at copy time so [`OnExisting::Reuse`] can decide whether a prior copy is
/// still fresh enough to reuse without re-copying.
///
/// `source_mtime` is stored as whole seconds plus a nanosecond remainder since
/// the Unix epoch so the JSON is human-legible and the staleness comparison is
/// exact (it never round-trips through a lossy float).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StagedManifest {
    /// The original source path the copy was made from.
    pub(crate) source_path: PathBuf,
    /// Whole seconds of the source's modification time at copy time, measured
    /// since the Unix epoch. Paired with [`Self::source_mtime_nanos`].
    pub(crate) source_mtime_secs: i64,
    /// Sub-second nanosecond remainder of the source's modification time.
    pub(crate) source_mtime_nanos: u32,
    /// Source size in bytes at copy time.
    pub(crate) source_size: u64,
    /// Hex BLAKE3 digest of the copied bytes.
    pub(crate) content_hash: String,
    /// When the staged copy was published, as an RFC 3339 timestamp. Recorded
    /// for operator audit only; the freshness check uses mtime + size, not this.
    pub(crate) staged_at: String,
}

impl StagedManifest {
    /// Whether this manifest still describes the live source — its recorded
    /// mtime and size both match the source's current `stat`.
    ///
    /// This is the freshness test [`OnExisting::Reuse`] runs: a match means the
    /// source has not changed since it was staged, so the existing copy is safe
    /// to reuse without re-copying. A changed mtime or size means the source was
    /// rewritten and the copy is stale.
    fn matches_source(&self, meta: &std::fs::Metadata) -> bool {
        let Some((secs, nanos)) = mtime_parts(meta) else {
            return false;
        };
        self.source_size == meta.len()
            && self.source_mtime_secs == secs
            && self.source_mtime_nanos == nanos
    }
}

/// A staged copy's resolved on-disk paths and recorded identity.
///
/// Internal to the copy engine — the public [`SourceStager::resolve`] hands the
/// reader a [`StagedPath`], and this is folded into the staged-file log line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StagedFile {
    /// The local `.staged` copy the reader should open.
    staged_path: PathBuf,
    /// The copy's persisted identity manifest.
    manifest: StagedManifest,
}

/// The reuse decision `clinker run --explain` reports for a staged source under
/// `on_existing = reuse`.
///
/// `--explain` does no copy, but it can `stat` the source and read a committed
/// manifest read-only to predict whether the real run would reuse a fresh prior
/// copy (a cache *hit*) or re-stage (a *miss*). The decision is exactly the one
/// [`SourceStager::stage_locked`] makes at run time, computed without touching
/// the copy path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReuseDecision {
    /// `on_existing = reuse` and a committed staged copy matches the live source
    /// (mtime + size) — the real run would reuse it and copy no bytes.
    Hit,
    /// `on_existing = reuse` but no fresh committed copy exists (absent, stale,
    /// or orphaned) — the real run would re-stage.
    Miss,
    /// `on_existing` is not `reuse` (`overwrite` re-copies every run; `error`
    /// fails on an existing copy), so the reuse-if-fresh cache does not apply.
    NotApplicable,
}

impl ReuseDecision {
    /// Lowercase label for the `--explain` staging-plan line.
    pub fn label(self) -> &'static str {
        match self {
            ReuseDecision::Hit => "hit",
            ReuseDecision::Miss => "miss",
            ReuseDecision::NotApplicable => "n/a",
        }
    }
}

/// A read-only prediction of what staging would do to one source, for
/// `clinker run --explain`.
///
/// Computed by [`SourceStager::plan_entry`] without copying a byte: it reports
/// whether the source matches a staging pattern (`staged`), the stable
/// content-addressed path the copy would land at (`staged_path`, present only
/// when staged), and — under `on_existing = reuse` — whether a fresh prior copy
/// would be reused (`reuse`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagingPlanEntry {
    /// The source path the pipeline author declared.
    pub source: PathBuf,
    /// Whether staging is enabled and this source matches a staging pattern.
    /// `false` means the run reads the source in place.
    pub staged: bool,
    /// The stable `<staging_root>/<source_id>.staged` path the copy would land
    /// at. `Some` only when `staged`; `None` for an in-place source.
    pub staged_path: Option<PathBuf>,
    /// The reuse-if-fresh cache decision the real run would make. Always
    /// [`ReuseDecision::NotApplicable`] for an in-place source.
    pub reuse: ReuseDecision,
}

/// Per-run staging engine: owns the staging-policy decision, the running byte
/// total, and the set of staged files this run produced for cleanup.
///
/// One `SourceStager` is built per pipeline run. Unlike a per-run temp
/// directory, staged files land at **stable content-addressed paths** directly
/// under the staging root so a later run can find and reuse them — cleanup is
/// therefore explicit ([`SourceStager::cleanup`]) rather than a `TempDir` drop,
/// and an idempotent startup [`SourceStager::crash_purge`] reaps the artifacts a
/// crashed prior run left behind.
///
/// When the policy is disabled or a path does not match a staging pattern,
/// [`SourceStager::resolve`] returns an in-place [`StagedPath`] and never
/// touches the filesystem.
pub struct SourceStager {
    policy: StagingPolicy,
    /// Cumulative bytes copied this run, checked against the disk cap. A reused
    /// (skipped) copy charges nothing — only bytes actually written count.
    bytes_staged: u64,
    /// Stable paths of every staged copy this run produced or reused, used by
    /// [`SourceStager::cleanup`] to remove them per the cleanup policy.
    produced: Vec<PathBuf>,
}

impl SourceStager {
    /// Build a stager for one run from the workspace staging policy.
    ///
    /// No filesystem work happens here. The staging root must already exist and
    /// be writable (validated at startup); files land directly under it at
    /// content-addressed paths on the first source that actually stages.
    pub fn new(policy: StagingPolicy) -> Self {
        Self {
            policy,
            bytes_staged: 0,
            produced: Vec::new(),
        }
    }

    /// Resolve one source path, staging it if the policy selects it.
    ///
    /// Returns an in-place [`StagedPath`] (no copy, no filesystem touch) when
    /// staging is disabled or `original` matches no pattern. When the path is
    /// selected, applies the `on_existing` policy against the stable
    /// content-addressed staged path and either reuses a fresh prior copy or
    /// re-stages with inline BLAKE3 verification and atomic publish, then
    /// returns a [`StagedPath`] whose `staged` points at the local copy.
    ///
    /// # Errors
    ///
    /// Returns [`StagingError`] when the copy fails, the digest does not match
    /// (`verify = blake3`), the disk cap would be exceeded, or
    /// `on_existing = error` and a staged copy already exists.
    pub fn resolve(&mut self, original: PathBuf) -> Result<StagedPath, StagingError> {
        if !self.policy.pattern_matches(&original) {
            return Ok(StagedPath::in_place(original));
        }
        let staged = self.stage_or_reuse(&original)?;
        self.produced.push(staged.staged_path.clone());
        // Record the copy's provenance: the operator (and any post-run audit)
        // wants the source, its size, the local copy, and the verified digest
        // so a staged run is traceable back to exact bytes.
        tracing::info!(
            source = %staged.manifest.source_path.display(),
            staged = %staged.staged_path.display(),
            bytes = staged.manifest.source_size,
            blake3 = %staged.manifest.content_hash,
            "staged source file to local disk"
        );
        Ok(StagedPath {
            original,
            staged: Some(staged.staged_path),
        })
    }

    /// Predict, read-only, what staging would do to `source` — for
    /// `clinker run --explain`.
    ///
    /// Copies nothing and acquires no lock. Reports whether the source matches a
    /// staging pattern, the stable content-addressed staged path it would land
    /// at, and — under `on_existing = reuse` — whether a fresh prior copy would
    /// be reused. The reuse check `stat`s the source and reads the committed
    /// manifest exactly as the real run's [`Self::stage_locked`] does, so the
    /// `--explain` prediction matches the run's decision without doing I/O on the
    /// copy path. An in-place source (staging disabled or no pattern match)
    /// returns `staged = false` and a [`ReuseDecision::NotApplicable`].
    pub fn plan_entry(&self, source: &Path) -> StagingPlanEntry {
        if !self.policy.pattern_matches(source) {
            return StagingPlanEntry {
                source: source.to_path_buf(),
                staged: false,
                staged_path: None,
                reuse: ReuseDecision::NotApplicable,
            };
        }
        let root = self.staging_root();
        let source_id = source_id(source);
        let staged_path = root.join(format!("{source_id}.{STAGED_EXT}"));
        let manifest_path = root.join(format!("{source_id}{MANIFEST_SUFFIX}"));

        let reuse = match self.policy.on_existing {
            OnExisting::Reuse => {
                // Mirror the run-time freshness check: a committed copy whose
                // recorded mtime + size still match the live source is a hit.
                match (
                    load_clean_manifest(&staged_path, &manifest_path),
                    std::fs::metadata(source),
                ) {
                    (Some(manifest), Ok(meta)) if manifest.matches_source(&meta) => {
                        ReuseDecision::Hit
                    }
                    _ => ReuseDecision::Miss,
                }
            }
            // `overwrite` re-copies every run; `error` aborts on an existing
            // copy. Neither consults the reuse-if-fresh cache.
            OnExisting::Overwrite | OnExisting::Error => ReuseDecision::NotApplicable,
        };

        StagingPlanEntry {
            source: source.to_path_buf(),
            staged: true,
            staged_path: Some(staged_path),
            reuse,
        }
    }

    /// The staging root the policy targets. Present and validated whenever
    /// staging is enabled.
    fn staging_root(&self) -> &Path {
        self.policy
            .dir
            .as_deref()
            .expect("staging dir is validated present at startup when enabled")
    }

    /// Apply `on_existing` against the stable staged path for `source`, either
    /// reusing a fresh prior copy or (re-)staging — serialized against concurrent
    /// siblings staging the same source by a per-source advisory lock.
    ///
    /// The lock is what makes one copy across overlapping invocations: a sibling
    /// that loses the race blocks here, then acquires the lock to find the
    /// winner's freshly published `.staged` and reuses it. The reuse-if-fresh
    /// check therefore runs **under** the lock (the [`stage_locked`] body), not
    /// before it — a check before the lock would see "not staged" and copy
    /// redundantly.
    ///
    /// [`stage_locked`]: Self::stage_locked
    fn stage_or_reuse(&mut self, source: &Path) -> Result<StagedFile, StagingError> {
        let source_id = source_id(source);
        let root = self.staging_root().to_path_buf();
        let lock_path = root.join(format!("{source_id}.{LOCK_EXT}"));

        // Hold the per-source lock for the whole reuse-or-stage decision so a
        // concurrent sibling cannot interleave between the freshness check and
        // the publish. The lock file is created if absent and kept open until
        // the guard drops at the end of this function; the kernel releases the
        // OS lock on drop (and on any process exit), so a crash never strands it.
        let _guard = SourceLock::acquire(&lock_path)?;
        self.stage_locked(source, &source_id, &root)
    }

    /// The locked critical section: re-check reuse-if-fresh, then (re-)stage if
    /// needed. Always called with the per-source lock held.
    fn stage_locked(
        &mut self,
        source: &Path,
        source_id: &str,
        root: &Path,
    ) -> Result<StagedFile, StagingError> {
        let staged_path = root.join(format!("{source_id}.{STAGED_EXT}"));
        let manifest_path = root.join(format!("{source_id}{MANIFEST_SUFFIX}"));

        let existing = load_clean_manifest(&staged_path, &manifest_path);

        match self.policy.on_existing {
            OnExisting::Error => {
                if existing.is_some() {
                    return Err(StagingError::AlreadyExists {
                        source_path: source.to_path_buf(),
                        staged_path,
                    });
                }
            }
            OnExisting::Reuse => {
                if let Some(manifest) = existing {
                    let meta = stat(source)?;
                    if manifest.matches_source(&meta) {
                        // Fresh match under the lock: either a prior run staged
                        // it, or a concurrent sibling just published it while we
                        // blocked on the lock. Either way reuse it and copy no
                        // bytes — this is the cache hit that makes overlapping
                        // invocations copy a source exactly once.
                        tracing::info!(
                            source = %source.display(),
                            staged = %staged_path.display(),
                            "reusing fresh staged copy (mtime + size match)"
                        );
                        return Ok(StagedFile {
                            staged_path,
                            manifest,
                        });
                    }
                    // Stale: the source was rewritten. Fall through to re-stage.
                }
            }
            OnExisting::Overwrite => {}
        }

        // (Re-)stage. Remove any prior staged copy + manifest first so a stale
        // or partial artifact never coexists with the fresh one; the manifest
        // is unlinked before the copy so a crash mid-copy cannot leave a fresh
        // `.staged` paired with a stale manifest.
        let _ = std::fs::remove_file(&manifest_path);
        let _ = std::fs::remove_file(&staged_path);
        self.stage_file(source, source_id, &staged_path, &manifest_path)
    }

    /// Copy one selected source to its stable staged path with inline hashing,
    /// atomic publish, and a committed identity manifest.
    ///
    /// The full lifecycle for a file:
    ///
    /// 1. `stat` the source for its size + mtime; charge the size against the
    ///    run's disk cap before copying a byte.
    /// 2. Open `<source_id>.<run_uuid>.partial` (mode `0o600` on Unix).
    /// 3. Stream 1 MiB chunks: each chunk is read once, fed to the BLAKE3
    ///    hasher, then written to the temp file — one pass over the bytes.
    /// 4. `sync_all` the temp file. On macOS `File::sync_all` issues
    ///    `F_FULLFSYNC`, so the data is on stable storage before the rename.
    /// 5. `rename` `.partial` → `.staged`. `std::fs::rename` is atomic-replace
    ///    on all three target OSes, so a reader never observes a torn file.
    /// 6. (POSIX) `sync_all` the parent directory so the rename itself is
    ///    crash-durable.
    /// 7. When `verify = blake3`, independently re-hash the source and compare;
    ///    a mismatch is [`StagingError::Verify`].
    /// 8. Atomically publish the manifest (`.manifest.json.partial` → rename),
    ///    which commits the copy: only now is the `.staged` file trustworthy.
    ///
    /// On any error after the temp file is created, the `.partial` is removed
    /// before the error propagates, so a failed stage never leaves a stray temp
    /// file behind.
    fn stage_file(
        &mut self,
        source: &Path,
        source_id: &str,
        staged_path: &Path,
        manifest_path: &Path,
    ) -> Result<StagedFile, StagingError> {
        let meta = stat(source)?;
        let source_size = meta.len();

        // Charge the disk cap before writing: a file that would push the run
        // over its configured budget is refused outright rather than copied and
        // then rolled back.
        if let Some(cap) = self.policy.disk_cap_bytes.map(|b| b.0) {
            let projected = self.bytes_staged.saturating_add(source_size);
            if projected > cap {
                return Err(StagingError::DiskCapExceeded {
                    cap,
                    staged: self.bytes_staged,
                    incoming: source_size,
                    source_path: source.to_path_buf(),
                });
            }
        }

        // The partial carries a fresh per-run uuid so two concurrent
        // invocations staging the same source write distinct partials, and the
        // crash-purge can probe the partial's owning run for liveness via the
        // per-source lock. (Concurrent same-source stages are serialized by that
        // lock in `stage_or_reuse`, so in practice only one sibling reaches here
        // per source; the uuid keeps the rare overwrite/error-mode interleavings
        // collision-free too.)
        let run_seg = uuid::Uuid::new_v4().to_string();
        let root = staged_path.parent().unwrap_or(Path::new("."));
        let partial = root.join(format!("{source_id}.{run_seg}{PARTIAL_SUFFIX}"));

        let copy_result = self.copy_into(source, &partial, staged_path);
        let content_hash = match copy_result {
            Ok(content_hash) => {
                if self.policy.verify == StagingVerify::Blake3 {
                    verify_staged(source, staged_path, content_hash)?
                } else {
                    content_hash
                }
            }
            Err(e) => {
                // Best-effort cleanup of the partial before propagating.
                let _ = std::fs::remove_file(&partial);
                return Err(e);
            }
        };

        let (secs, nanos) = mtime_parts(&meta).unwrap_or((0, 0));
        let manifest = StagedManifest {
            source_path: source.to_path_buf(),
            source_mtime_secs: secs,
            source_mtime_nanos: nanos,
            source_size,
            content_hash,
            staged_at: now_rfc3339(),
        };
        // Publishing the manifest is the commit point. If it fails, unlink the
        // staged copy so a `.staged` without a manifest never survives as an
        // orphan a later run might half-trust.
        if let Err(e) = write_manifest_atomic(manifest_path, &manifest) {
            let _ = std::fs::remove_file(staged_path);
            return Err(e);
        }

        self.bytes_staged = self.bytes_staged.saturating_add(source_size);
        Ok(StagedFile {
            staged_path: staged_path.to_path_buf(),
            manifest,
        })
    }

    /// Stream `source` into `partial`, hashing as it copies, then atomically
    /// publish to `staged` and make the rename durable. Returns the hex BLAKE3
    /// digest of the copied bytes.
    fn copy_into(
        &self,
        source: &Path,
        partial: &Path,
        staged: &Path,
    ) -> Result<String, StagingError> {
        let mut reader = File::open(source).map_err(|e| StagingError::Io {
            path: source.to_path_buf(),
            source: e,
        })?;
        advise_sequential(&reader);

        let mut writer = open_partial(partial)?;
        let mut hasher = blake3::Hasher::new();
        let mut buf = vec![0u8; COPY_CHUNK_BYTES];

        loop {
            let n = reader.read(&mut buf).map_err(|e| StagingError::Io {
                path: source.to_path_buf(),
                source: e,
            })?;
            if n == 0 {
                break;
            }
            let chunk = &buf[..n];
            hasher.update(chunk);
            writer.write_all(chunk).map_err(|e| StagingError::Io {
                path: partial.to_path_buf(),
                source: e,
            })?;
        }

        // Flush and fsync the data before the rename. `sync_all` issues
        // F_FULLFSYNC on macOS, so the bytes are on stable storage — a rename
        // that publishes data still sitting in the drive cache could survive a
        // crash with an empty file otherwise.
        writer.flush().map_err(|e| StagingError::Io {
            path: partial.to_path_buf(),
            source: e,
        })?;
        writer.sync_all().map_err(|e| StagingError::Io {
            path: partial.to_path_buf(),
            source: e,
        })?;
        drop(writer);

        std::fs::rename(partial, staged).map_err(|e| StagingError::Io {
            path: staged.to_path_buf(),
            source: e,
        })?;

        fsync_parent_dir(staged)?;

        Ok(hasher.finalize().to_hex().to_string())
    }

    /// Remove the staged copies this run produced, per the cleanup policy.
    ///
    /// Called once after the run finishes with `success` reflecting the run's
    /// exit status. [`Cleanup::OnSuccess`] removes the copies only on a clean
    /// run (a failure keeps them so the operator can inspect the exact inputs);
    /// [`Cleanup::Always`] removes them regardless; [`Cleanup::Never`] keeps
    /// them as a persistent reuse cache. Each staged file's manifest is removed
    /// alongside it so a half-removed (staged-gone, manifest-left) pair never
    /// confuses a later reuse-if-fresh check.
    ///
    /// Best-effort: a removal failure is logged, not propagated — cleanup must
    /// not turn a successful run into a failure, and the next run's
    /// [`SourceStager::crash_purge`] reaps anything left behind.
    pub fn cleanup(&self, success: bool) {
        let remove = match self.policy.cleanup {
            Cleanup::Always => true,
            Cleanup::OnSuccess => success,
            Cleanup::Never => false,
        };
        if !remove {
            return;
        }
        for staged in &self.produced {
            remove_staged_pair(staged);
        }
    }

    /// Idempotently reap the staged artifacts a crashed prior run left behind,
    /// run once at startup before any source is staged.
    ///
    /// A crash leaves three orphan shapes under the staging root:
    ///
    /// - `<source_id>.<run_uuid>.partial` — an interrupted copy. Reaped **only
    ///   when its owning run is dead**: liveness-aware, so a concurrent sibling's
    ///   in-flight partial is never reaped. See [`partial_is_orphaned`].
    /// - `*.staged` with no matching `*.manifest.json` — a copy that crashed
    ///   after the rename but before the manifest was published. The manifest is
    ///   the committed-copy marker, so a manifestless `.staged` is always an
    ///   orphan regardless of liveness. Reaped.
    /// - `*.manifest.json` with no matching `*.staged` — a manifest whose staged
    ///   file was removed. Reaped.
    ///
    /// A clean pair (`.staged` + matching `.manifest.json`) is the reuse cache
    /// and is **kept** — reuse-if-fresh depends on it surviving across runs. A
    /// `.lock` file is the persistent per-source coordination point and is
    /// **never** reaped: it carries no payload, a concurrent sibling may be about
    /// to lock it, and leaving it costs nothing.
    ///
    /// The partial liveness check uses the same `fs4` try-lock + creation-grace
    /// discipline as the executor's spill crash-purge, so concurrent
    /// invocations sharing one staging root stay isolated by construction: a
    /// live run's partial is protected either by its held `<source_id>.lock` or,
    /// in the brief create→lock window, by the grace gate.
    ///
    /// A no-op when staging is disabled, the root is unset, or the root does not
    /// yet exist. Errors are logged, not propagated: a purge failure must not
    /// abort an otherwise-valid run.
    pub fn crash_purge(policy: &StagingPolicy) {
        Self::crash_purge_with_grace(policy, REAP_GRACE);
    }

    /// [`SourceStager::crash_purge`] with the partial creation-grace window
    /// injected, so tests can drive both sides of the window without sleeping: a
    /// zero grace exercises the dead-owner reap path immediately, the real grace
    /// exercises a freshly created partial's protection.
    fn crash_purge_with_grace(policy: &StagingPolicy, grace: Duration) {
        if !policy.enabled {
            return;
        }
        let Some(root) = policy.dir.as_deref() else {
            return;
        };
        let entries = match std::fs::read_dir(root) {
            Ok(entries) => entries,
            // A missing root is not an error here: the startup validator owns
            // dir validity, and the first stage creates entries under it.
            Err(e) if e.kind() == io::ErrorKind::NotFound => return,
            Err(e) => {
                tracing::warn!(
                    root = %root.display(),
                    error = %e,
                    "staging crash-purge could not read the staging root"
                );
                return;
            }
        };

        let mut reaped = 0usize;
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let is_orphan = if name.ends_with(MANIFEST_PARTIAL_SUFFIX) {
                // An in-flight manifest write. It is created and atomically
                // renamed in one tight step under the per-source lock, so a
                // leftover is unambiguously a crash corpse — reaped outright.
                // (Checked before the copy-partial arm, which would otherwise
                // misparse this longer suffix.)
                true
            } else if let Some(stem) = name.strip_suffix(PARTIAL_SUFFIX) {
                // Liveness-aware: only reap a copy partial whose owning run is
                // dead (its per-source lock is acquirable) and which has aged
                // past the creation grace window. A live sibling's in-flight
                // partial is kept. `stem` is `<source_id>.<run_uuid>` (the uuid
                // is hyphenated, never dotted), so the source id is the segment
                // before the final dot.
                let source_id = stem.rsplit_once('.').map_or(stem, |(id, _uuid)| id);
                partial_is_orphaned(&path, root, source_id, grace)
            } else if let Some(id) = name.strip_suffix(&format!(".{STAGED_EXT}")) {
                // Orphan when the matching manifest is absent.
                !root.join(format!("{id}{MANIFEST_SUFFIX}")).exists()
            } else if let Some(id) = name.strip_suffix(MANIFEST_SUFFIX) {
                // Orphan when the matching staged copy is absent.
                !root.join(format!("{id}.{STAGED_EXT}")).exists()
            } else {
                // `.staged` reuse cache, `.lock` coordination files, and
                // anything else are not orphans.
                false
            };
            if is_orphan {
                match std::fs::remove_file(&path) {
                    Ok(()) => reaped += 1,
                    Err(e) => tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "staging crash-purge failed to remove an orphaned artifact"
                    ),
                }
            }
        }
        if reaped > 0 {
            tracing::info!(
                root = %root.display(),
                reaped,
                "staging crash-purge reaped orphaned artifacts from a prior run"
            );
        }
    }
}

/// An RAII guard over a held per-source advisory lock. The exclusive `fs4` lock
/// is released — and the lock file handle closed — when this drops, which also
/// happens automatically if the process exits while holding it.
struct SourceLock {
    file: File,
}

impl SourceLock {
    /// Create (if absent) and exclusively lock `<source_id>.lock`, blocking
    /// until the lock is acquired.
    ///
    /// The lock file is owner-only on Unix like every other staging-root entry,
    /// since it sits on the same potentially-shared volume. The blocking
    /// `lock_exclusive` is what serializes concurrent stages of one source: a
    /// sibling waits here rather than racing into a redundant copy.
    ///
    /// # Errors
    ///
    /// Returns [`StagingError::Io`] when the lock file cannot be created or the
    /// OS lock call fails outright.
    fn acquire(lock_path: &Path) -> Result<Self, StagingError> {
        let file = open_owner_only(lock_path)?;
        // fs4's `lock` is the blocking exclusive lock (`flock(LOCK_EX)` on Unix,
        // `LockFileEx` with the exclusive flag on Windows).
        FileExt::lock(&file).map_err(|e| StagingError::Io {
            path: lock_path.to_path_buf(),
            source: e,
        })?;
        Ok(Self { file })
    }
}

impl Drop for SourceLock {
    fn drop(&mut self) {
        // Best-effort: an unlock failure cannot be propagated from Drop, and the
        // kernel releases the lock on the file handle's close (which Drop of
        // `self.file` triggers next) and unconditionally on process exit.
        let _ = FileExt::unlock(&self.file);
    }
}

/// Whether a `<source_id>.<run_uuid>.partial` is an orphan — its owning run is
/// dead — and therefore safe to reap.
///
/// Two conditions must both hold, mirroring the executor's spill crash-purge:
///
/// 1. The partial is older than `grace` (in production [`REAP_GRACE`]). A
///    younger partial may belong to a live sibling that created it microseconds
///    ago, so it is never an orphan regardless of lock state.
/// 2. The owning run's `<source_id>.lock` is *acquirable* under a try-lock. A
///    live run staging this source holds that lock exclusively, so a successful
///    try-lock means no live owner — the partial is a crash corpse. A
///    `WouldBlock` means a sibling is staging the same source right now, so the
///    partial is in-flight and kept. A missing lock file on an aged-out partial
///    means the owner died before (or without) the lock surviving, which under
///    the grace gate is a corpse.
fn partial_is_orphaned(partial: &Path, root: &Path, source_id: &str, grace: Duration) -> bool {
    // Age gate first: a freshly created partial is presumed live no matter what
    // the lock probe says, closing the create→lock window.
    if partial_age(partial).is_none_or(|age| age < grace) {
        return false;
    }
    let lock_path = root.join(format!("{source_id}.{LOCK_EXT}"));
    let Ok(file) = File::open(&lock_path) else {
        // No probeable lock on a partial already past the grace window: the
        // owning run is gone (or never registered a lock), an orphan.
        return true;
    };
    match FileExt::try_lock(&file) {
        Ok(()) => {
            // Acquired: no live owner. Release at once so the remove can proceed
            // (on Windows an open lock handle would block it) and a concurrent
            // purge sees a consistent state.
            let _ = FileExt::unlock(&file);
            drop(file);
            true
        }
        // A live sibling holds the lock — its partial is in-flight, kept.
        Err(_) => false,
    }
}

/// Age of a `.partial`, used to decide whether its owner has had time to take
/// the per-source lock, or `None` when no timestamp is readable (treated by the
/// caller as "too young to reap", erring toward keeping a file it cannot date).
///
/// A clock skew that puts the mtime in the future yields a zero-ish age, which
/// the grace gate treats as "too young" — the conservative direction (keep
/// rather than reap).
fn partial_age(partial: &Path) -> Option<Duration> {
    let modified = std::fs::metadata(partial).and_then(|m| m.modified()).ok()?;
    SystemTime::now().duration_since(modified).ok()
}

/// The stable content-addressed id for a source path: the hex BLAKE3 of its
/// canonicalized absolute form, truncated to 32 hex chars.
///
/// Canonicalizing first means two spellings of one source (relative vs
/// absolute, symlinked) map to the same staged file, so reuse-if-fresh keys on
/// the real file rather than the string the author happened to write. When the
/// path cannot be canonicalized (it may not exist yet at id-computation time on
/// some flows), the raw path bytes are hashed instead — still deterministic for
/// a given spelling, which is all the layout requires.
fn source_id(source: &Path) -> String {
    let canonical = std::fs::canonicalize(source).unwrap_or_else(|_| source.to_path_buf());
    let hash = blake3::hash(canonical.to_string_lossy().as_bytes());
    hash.to_hex()[..32].to_string()
}

/// Load the committed manifest for a staged path, or `None` when the pair is
/// not a clean, reusable artifact.
///
/// Returns `Some` only when both the `.staged` file and a parseable
/// `.manifest.json` are present — the manifest is the commit marker, so a
/// `.staged` without one is an orphan, not a cache hit.
fn load_clean_manifest(staged_path: &Path, manifest_path: &Path) -> Option<StagedManifest> {
    if !staged_path.exists() {
        return None;
    }
    let bytes = std::fs::read(manifest_path).ok()?;
    serde_json::from_slice::<StagedManifest>(&bytes).ok()
}

/// `stat` a source path, mapping the I/O error to [`StagingError::Io`].
fn stat(source: &Path) -> Result<std::fs::Metadata, StagingError> {
    std::fs::metadata(source).map_err(|e| StagingError::Io {
        path: source.to_path_buf(),
        source: e,
    })
}

/// Decompose a file's modification time into (whole seconds, nanosecond
/// remainder) since the Unix epoch, or `None` when the platform cannot report
/// an mtime. Pre-epoch mtimes (negative durations) are represented with a
/// negative seconds component so the comparison stays exact.
fn mtime_parts(meta: &std::fs::Metadata) -> Option<(i64, u32)> {
    let mtime = meta.modified().ok()?;
    match mtime.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => Some((d.as_secs() as i64, d.subsec_nanos())),
        Err(e) => {
            // Pre-epoch: the error carries the magnitude of the negative offset.
            let d = e.duration();
            Some((-(d.as_secs() as i64), d.subsec_nanos()))
        }
    }
}

/// Current wall-clock time as an RFC 3339 string for the manifest audit field.
fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// Atomically write `manifest` to `path` via a sibling `.partial` + rename so a
/// reader never sees a half-written manifest, then fsync the parent dir so the
/// commit survives a crash.
///
/// This rename is what makes the manifest the durable commit marker: a crash
/// before it leaves no manifest (the staged copy reads as an orphan), and a
/// crash after it leaves a fully-committed pair.
fn write_manifest_atomic(path: &Path, manifest: &StagedManifest) -> Result<(), StagingError> {
    let bytes = serde_json::to_vec_pretty(manifest).map_err(|e| StagingError::Io {
        path: path.to_path_buf(),
        source: io::Error::other(e),
    })?;
    let partial = path.with_extension("json.partial");
    {
        let mut f = open_owner_only(&partial)?;
        f.write_all(&bytes).map_err(|e| StagingError::Io {
            path: partial.clone(),
            source: e,
        })?;
        f.sync_all().map_err(|e| StagingError::Io {
            path: partial.clone(),
            source: e,
        })?;
    }
    std::fs::rename(&partial, path).map_err(|e| StagingError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    fsync_parent_dir(path)
}

/// Remove a staged copy and its sidecar manifest. Best-effort: a missing file
/// or a removal error is ignored, since cleanup and crash-purge both call this
/// where a partial prior removal is expected.
fn remove_staged_pair(staged: &Path) {
    let _ = std::fs::remove_file(staged);
    if let Some(id) = staged
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.strip_suffix(&format!(".{STAGED_EXT}")))
        && let Some(root) = staged.parent()
    {
        let _ = std::fs::remove_file(root.join(format!("{id}{MANIFEST_SUFFIX}")));
    }
}

/// Open the `.partial` temp file for writing, truncating any leftover, with
/// mode `0o600` on Unix.
fn open_partial(partial: &Path) -> Result<File, StagingError> {
    open_owner_only(partial)
}

/// Open `path` for writing (create + truncate), owner-only (`0o600`) on Unix.
///
/// Staged files and manifests hold (or describe) verbatim source records; on a
/// shared staging volume they must not be group/other-readable. Unix sets the
/// mode at create time via `OpenOptionsExt`. Windows has no portable mode bit;
/// the file inherits the directory ACL, NTFS's standard behavior.
fn open_owner_only(path: &Path) -> Result<File, StagingError> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path).map_err(|e| StagingError::Io {
        path: path.to_path_buf(),
        source: e,
    })
}

/// Confirm the staged copy matches the source by re-hashing the source and
/// comparing against the digest computed while copying.
///
/// Returns `copy_hash` unchanged on a match. On a mismatch the published
/// `staged` file is removed — a corrupt copy must never be left readable — and
/// a distinct [`StagingError::Verify`] is returned so the operator sees a
/// corruption signal, not a generic I/O error.
///
/// The independent re-read is what catches the soft-mount silent-truncation
/// mode: a size check cannot, but two content digests can.
///
/// # Errors
///
/// Returns [`StagingError::Verify`] on a digest mismatch, or [`StagingError::Io`]
/// when the source cannot be re-read.
fn verify_staged(source: &Path, staged: &Path, copy_hash: String) -> Result<String, StagingError> {
    let source_hash = hash_path(source)?;
    if source_hash != copy_hash {
        let _ = std::fs::remove_file(staged);
        return Err(StagingError::Verify {
            source_path: source.to_path_buf(),
            copy_hash,
            source_hash,
        });
    }
    Ok(copy_hash)
}

/// Independently hash an entire file with streamed reads, for verify-on-copy.
///
/// Reads in the same 1 MiB chunks as the copy so the verify pass has the same
/// bounded memory footprint. Returns the hex BLAKE3 digest.
fn hash_path(path: &Path) -> Result<String, StagingError> {
    let mut reader = File::open(path).map_err(|e| StagingError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    advise_sequential(&reader);
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; COPY_CHUNK_BYTES];
    loop {
        let n = reader.read(&mut buf).map_err(|e| StagingError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

/// Hint the kernel that `file` will be read front-to-back, so it can ramp up
/// readahead for the streamed copy. Linux-only via `posix_fadvise`; a no-op
/// elsewhere. Best-effort — a failed advise never fails the copy.
#[cfg(target_os = "linux")]
fn advise_sequential(file: &File) {
    // `posix_fadvise` borrows the fd via `AsFd` (File implements it), so no
    // unsafe and no lifetime hazard. A 0/0 range means "the whole file".
    let _ = nix::fcntl::posix_fadvise(
        file,
        0,
        0,
        nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL,
    );
}

/// Non-Linux platforms have no portable equivalent; readahead defaults apply.
#[cfg(not(target_os = "linux"))]
fn advise_sequential(_file: &File) {}

/// Fsync the directory containing `published` so a rename that published it is
/// crash-durable. POSIX-only.
///
/// On ext4/xfs a `rename` is only guaranteed durable once the containing
/// directory's metadata is also fsync'd; without it a crash immediately after
/// the rename can leave the published file missing even though the operation
/// reported success. Opening the directory as a `File` and calling `sync_all`
/// is the portable POSIX way to flush that directory entry.
///
/// Windows is intentionally skipped: NTFS makes a `rename` durable through its
/// metadata journal (the semantics `MOVEFILE_WRITE_THROUGH` requests), so there
/// is no separate directory handle to flush, and Windows offers no
/// `fsync(dir)` equivalent anyway.
#[cfg(unix)]
fn fsync_parent_dir(published: &Path) -> Result<(), StagingError> {
    let Some(parent) = published.parent() else {
        return Ok(());
    };
    let dir = File::open(parent).map_err(|e| StagingError::Io {
        path: parent.to_path_buf(),
        source: e,
    })?;
    dir.sync_all().map_err(|e| StagingError::Io {
        path: parent.to_path_buf(),
        source: e,
    })
}

/// Windows: the rename's durability is handled by the NTFS journal, so there is
/// no parent-directory handle to fsync. See the POSIX variant for the contrast.
#[cfg(not(unix))]
fn fsync_parent_dir(_published: &Path) -> Result<(), StagingError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_plan::config::utils::ByteSize;

    /// A zero grace window so a freshly created fixture partial is immediately
    /// eligible by age, exercising the dead-owner lock probe without sleeping.
    const NO_GRACE: Duration = Duration::ZERO;

    fn policy_with_dir(dir: &Path, patterns: &[&str]) -> StagingPolicy {
        StagingPolicy {
            enabled: true,
            dir: Some(dir.to_path_buf()),
            patterns: patterns.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    /// Count the entries under a directory matching a suffix predicate.
    fn count_with_suffix(dir: &Path, suffix: &str) -> usize {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(suffix))
            .count()
    }

    #[test]
    fn disabled_policy_reads_in_place() {
        let mut stager = SourceStager::new(StagingPolicy::default());
        let resolved = stager
            .resolve(PathBuf::from("/mnt/nfs/data/orders.csv"))
            .unwrap();
        assert_eq!(resolved.staged, None);
    }

    #[test]
    fn unmatched_path_reads_in_place_without_touching_fs() {
        let stage_dir = tempfile::tempdir().unwrap();
        let policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        let mut stager = SourceStager::new(policy);
        let resolved = stager
            .resolve(PathBuf::from("/somewhere/orders.json"))
            .unwrap();
        assert_eq!(resolved.staged, None);
        // No staged entry created for a non-staging run.
        let entries: Vec<_> = std::fs::read_dir(stage_dir.path()).unwrap().collect();
        assert!(entries.is_empty());
    }

    #[test]
    fn matched_source_is_copied_byte_identical_with_manifest() {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        let body = b"id,name\n1,alice\n2,bob\n".repeat(5000);
        std::fs::write(&src, &body).unwrap();

        let policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        let mut stager = SourceStager::new(policy);
        let resolved = stager.resolve(src.clone()).unwrap();

        assert_eq!(resolved.read_path(), resolved.staged.as_deref().unwrap());
        let staged = resolved.staged.expect("matched source should be staged");
        let staged_bytes = std::fs::read(&staged).unwrap();
        assert_eq!(staged_bytes, body);
        assert_eq!(staged.extension().unwrap(), "staged");
        // A committed manifest sits next to the staged copy, and no `.partial`
        // survives.
        assert_eq!(count_with_suffix(stage_dir.path(), MANIFEST_SUFFIX), 1);
        assert_eq!(count_with_suffix(stage_dir.path(), ".partial"), 0);
    }

    #[test]
    fn reuse_if_fresh_skips_copy_on_unchanged_source() {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        std::fs::write(&src, b"id,name\n1,alice\n").unwrap();

        let mut policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        policy.on_existing = OnExisting::Reuse;

        // First run stages the file.
        let mut first = SourceStager::new(policy.clone());
        let staged_first = first.resolve(src.clone()).unwrap().staged.unwrap();
        let inode_marker = std::fs::read(&staged_first).unwrap();

        // Overwrite the staged copy with a sentinel so a re-copy would clobber
        // it; reuse must leave it untouched (proving no copy happened).
        std::fs::write(&staged_first, b"REUSED-SENTINEL").unwrap();

        // Second run, source unchanged: reuse-if-fresh must skip the copy, so
        // the sentinel survives and bytes_staged stays zero.
        let mut second = SourceStager::new(policy);
        let staged_second = second.resolve(src.clone()).unwrap().staged.unwrap();
        assert_eq!(staged_second, staged_first, "stable path across runs");
        assert_eq!(
            std::fs::read(&staged_second).unwrap(),
            b"REUSED-SENTINEL",
            "reuse must not re-copy a fresh source"
        );
        assert_eq!(second.bytes_staged, 0, "a reused copy charges no bytes");
        // Sanity: the original copy did write the real bytes.
        assert_eq!(inode_marker, b"id,name\n1,alice\n");
    }

    #[test]
    fn reuse_if_fresh_restages_when_source_changes() {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        std::fs::write(&src, b"v1").unwrap();

        let mut policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        policy.on_existing = OnExisting::Reuse;

        let mut first = SourceStager::new(policy.clone());
        let staged = first.resolve(src.clone()).unwrap().staged.unwrap();
        assert_eq!(std::fs::read(&staged).unwrap(), b"v1");

        // Rewrite the source with a different *size* so the freshness check
        // (mtime + size) sees a stale manifest. The size change alone is the
        // robust staleness signal — it does not depend on the filesystem's
        // mtime resolution, which can be as coarse as one second.
        std::fs::write(&src, b"v2-longer").unwrap();

        let mut second = SourceStager::new(policy);
        let staged2 = second.resolve(src.clone()).unwrap().staged.unwrap();
        assert_eq!(staged2, staged, "stable path");
        assert_eq!(
            std::fs::read(&staged2).unwrap(),
            b"v2-longer",
            "a changed source must be re-staged"
        );
        assert!(second.bytes_staged > 0, "a re-stage copies bytes");
    }

    #[test]
    fn on_existing_error_fails_when_a_staged_copy_exists() {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        std::fs::write(&src, b"data").unwrap();

        // Pre-stage with the default (overwrite) policy.
        let mut seed = SourceStager::new(policy_with_dir(stage_dir.path(), &["*.csv"]));
        seed.resolve(src.clone()).unwrap();

        // A second run under on_existing = error must fail fast.
        let mut policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        policy.on_existing = OnExisting::Error;
        let mut stager = SourceStager::new(policy);
        let err = stager.resolve(src).unwrap_err();
        assert!(matches!(err, StagingError::AlreadyExists { .. }));
    }

    #[test]
    fn on_existing_error_stages_when_no_copy_exists() {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        std::fs::write(&src, b"data").unwrap();

        let mut policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        policy.on_existing = OnExisting::Error;
        let mut stager = SourceStager::new(policy);
        // No prior copy: error mode stages normally.
        assert!(stager.resolve(src).unwrap().staged.is_some());
    }

    #[test]
    fn on_existing_overwrite_restages_a_fresh_source() {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        std::fs::write(&src, b"original").unwrap();

        let mut seed = SourceStager::new(policy_with_dir(stage_dir.path(), &["*.csv"]));
        let staged = seed.resolve(src.clone()).unwrap().staged.unwrap();
        // Mutate the staged copy; overwrite must re-copy over it even though the
        // source is unchanged (overwrite ignores freshness, unlike reuse).
        std::fs::write(&staged, b"SENTINEL").unwrap();

        let mut policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        policy.on_existing = OnExisting::Overwrite;
        let mut stager = SourceStager::new(policy);
        let staged2 = stager.resolve(src).unwrap().staged.unwrap();
        assert_eq!(
            std::fs::read(&staged2).unwrap(),
            b"original",
            "overwrite must re-copy the source bytes"
        );
    }

    #[test]
    fn cleanup_on_success_removes_after_clean_run() {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        std::fs::write(&src, b"data").unwrap();

        let mut policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        policy.cleanup = Cleanup::OnSuccess;
        let mut stager = SourceStager::new(policy);
        let staged = stager.resolve(src).unwrap().staged.unwrap();
        assert!(staged.exists());

        stager.cleanup(true);
        assert!(!staged.exists(), "on-success cleanup removes after success");
        assert_eq!(
            count_with_suffix(stage_dir.path(), MANIFEST_SUFFIX),
            0,
            "the manifest is removed alongside the staged copy"
        );
    }

    #[test]
    fn cleanup_on_success_keeps_after_failure() {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        std::fs::write(&src, b"data").unwrap();

        let mut policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        policy.cleanup = Cleanup::OnSuccess;
        let mut stager = SourceStager::new(policy);
        let staged = stager.resolve(src).unwrap().staged.unwrap();

        // A failed run keeps the inputs for inspection.
        stager.cleanup(false);
        assert!(
            staged.exists(),
            "on-success cleanup keeps inputs after a failure"
        );
    }

    #[test]
    fn cleanup_always_removes_after_failure() {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        std::fs::write(&src, b"data").unwrap();

        let mut policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        policy.cleanup = Cleanup::Always;
        let mut stager = SourceStager::new(policy);
        let staged = stager.resolve(src).unwrap().staged.unwrap();

        stager.cleanup(false);
        assert!(
            !staged.exists(),
            "always cleanup removes even after a failure"
        );
    }

    #[test]
    fn cleanup_never_keeps_a_reusable_cache() {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        std::fs::write(&src, b"data").unwrap();

        let mut policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        policy.cleanup = Cleanup::Never;
        let mut stager = SourceStager::new(policy);
        let staged = stager.resolve(src).unwrap().staged.unwrap();

        stager.cleanup(true);
        assert!(staged.exists(), "never cleanup keeps the copy as a cache");
        assert_eq!(count_with_suffix(stage_dir.path(), MANIFEST_SUFFIX), 1);
    }

    #[test]
    fn blake3_mismatch_is_detected_as_distinct_error() {
        // The verify path drives the production decision: stage a file
        // (copy_into hashes the copied bytes), then mutate the source before the
        // independent verify read so the fresh source hash diverges — modeling
        // an NFS soft-mount that delivered different bytes during the copy than
        // the source now holds.
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        std::fs::write(&src, b"original bytes that get copied").unwrap();

        let policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        let stager = SourceStager::new(policy);

        let partial = stage_dir.path().join("x.partial");
        let staged = stage_dir.path().join("x.staged");
        let copy_hash = stager.copy_into(&src, &partial, &staged).unwrap();
        assert!(staged.exists());

        std::fs::write(&src, b"different bytes entirely").unwrap();
        let err = verify_staged(&src, &staged, copy_hash).unwrap_err();
        match err {
            StagingError::Verify {
                source_path,
                copy_hash,
                source_hash,
            } => {
                assert_eq!(source_path, src);
                assert_ne!(copy_hash, source_hash);
            }
            other => panic!("expected Verify, got {other:?}"),
        }
        assert!(
            !staged.exists(),
            "verify failure must unlink the staged copy"
        );
    }

    #[test]
    fn verify_passes_for_clean_copy() {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("data.csv");
        std::fs::write(&src, b"a,b,c\n1,2,3\n").unwrap();
        let policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        let mut stager = SourceStager::new(policy);
        let resolved = stager.resolve(src.clone()).unwrap();
        assert!(resolved.staged.is_some());
    }

    #[test]
    fn no_torn_staged_file_when_copy_fails_midway() {
        // A read error mid-copy must leave no `.staged` file and remove the
        // `.partial`. A directory matched as a "source" makes metadata() work
        // but read() fail.
        let stage_dir = tempfile::tempdir().unwrap();
        let src_dir = tempfile::tempdir().unwrap();
        let dir_as_src = src_dir.path().join("subdir.csv");
        std::fs::create_dir(&dir_as_src).unwrap();

        let policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        let mut stager = SourceStager::new(policy);
        let err = stager.resolve(dir_as_src).unwrap_err();
        assert!(matches!(err, StagingError::Io { .. }));

        // No `.partial` or `.staged` artifact may survive a failed copy. The
        // per-source `.lock` is the persistent coordination file and is expected
        // to remain — it carries no payload and is reused by the next attempt.
        let survivors: Vec<_> = std::fs::read_dir(stage_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) != Some(LOCK_EXT))
            .collect();
        assert!(
            survivors.is_empty(),
            "no partial/staged file should survive a failed copy, found {survivors:?}"
        );
    }

    #[test]
    fn crash_purge_reaps_orphans_but_keeps_clean_pairs() {
        let stage_dir = tempfile::tempdir().unwrap();
        let root = stage_dir.path();

        // Orphan 1: an interrupted partial with no live owner. A zero grace makes
        // it eligible by age, and with no `.lock` present its owner reads as dead.
        std::fs::write(root.join("aaaa.deadbeef.partial"), b"half").unwrap();
        // Orphan 2: a staged copy with no manifest (crashed before commit).
        std::fs::write(root.join("bbbb.staged"), b"orphan-staged").unwrap();
        // Orphan 3: a manifest with no staged copy.
        std::fs::write(root.join("cccc.manifest.json"), b"{}").unwrap();
        // Clean pair: staged + matching manifest — the reuse cache, must survive.
        std::fs::write(root.join("dddd.staged"), b"clean").unwrap();
        std::fs::write(root.join("dddd.manifest.json"), b"{}").unwrap();
        // A lingering per-source lock file: never an orphan, must survive.
        std::fs::write(root.join("dddd.lock"), b"").unwrap();

        let policy = policy_with_dir(root, &["*.csv"]);
        SourceStager::crash_purge_with_grace(&policy, NO_GRACE);

        assert!(
            !root.join("aaaa.deadbeef.partial").exists(),
            "a dead-owner partial reaped"
        );
        assert!(
            !root.join("bbbb.staged").exists(),
            "manifestless staged reaped"
        );
        assert!(
            !root.join("cccc.manifest.json").exists(),
            "stagedless manifest reaped"
        );
        assert!(root.join("dddd.staged").exists(), "clean staged kept");
        assert!(
            root.join("dddd.manifest.json").exists(),
            "clean manifest kept"
        );
        assert!(root.join("dddd.lock").exists(), "lock file kept");
    }

    #[test]
    fn crash_purge_keeps_a_live_siblings_in_flight_partial() {
        // A concurrent sibling is staging this source: it holds the per-source
        // lock and has an in-flight partial. The purge must keep that partial
        // even with the grace removed (zero grace), proving the keep decision is
        // the held lock, not the file's age.
        let stage_dir = tempfile::tempdir().unwrap();
        let root = stage_dir.path();
        let source_id = "feedface";
        std::fs::write(
            root.join(format!("{source_id}.abc123.partial")),
            b"in-flight",
        )
        .unwrap();
        // Simulate the live sibling holding its per-source lock.
        let held = SourceLock::acquire(&root.join(format!("{source_id}.{LOCK_EXT}"))).unwrap();

        let policy = policy_with_dir(root, &["*.csv"]);
        SourceStager::crash_purge_with_grace(&policy, NO_GRACE);
        assert!(
            root.join(format!("{source_id}.abc123.partial")).exists(),
            "a live sibling's in-flight partial must never be reaped"
        );

        // Once the sibling releases the lock (its run ended), the same partial is
        // a corpse and a later purge reaps it — proving the lock, not the name,
        // gated the keep.
        drop(held);
        SourceStager::crash_purge_with_grace(&policy, NO_GRACE);
        assert!(
            !root.join(format!("{source_id}.abc123.partial")).exists(),
            "once the owner's lock is released the partial is reapable"
        );
    }

    #[test]
    fn crash_purge_keeps_a_newborn_partial_within_the_grace_window() {
        // The create→lock window: a freshly created partial whose owner has not
        // yet been observed holding the lock. With the real grace in force it
        // must be left alone, never reaped out from under its owner. Regression
        // guard for reaping a live sibling's just-created partial.
        let stage_dir = tempfile::tempdir().unwrap();
        let root = stage_dir.path();
        std::fs::write(root.join("cafef00d.xyz.partial"), b"newborn").unwrap();

        let policy = policy_with_dir(root, &["*.csv"]);
        // The default (real) grace: a just-created partial is inside the window.
        SourceStager::crash_purge(&policy);
        assert!(
            root.join("cafef00d.xyz.partial").exists(),
            "a just-created partial must be kept during the grace window"
        );
    }

    #[test]
    fn crash_purge_reaps_a_leftover_manifest_partial_unconditionally() {
        // An in-flight manifest write is created and renamed atomically under the
        // per-source lock, so a leftover `<id>.manifest.json.partial` is always a
        // crash corpse — reaped without a liveness probe and even within the
        // grace window (it never belongs to a still-live writer past its rename).
        let stage_dir = tempfile::tempdir().unwrap();
        let root = stage_dir.path();
        let leftover = root.join("beadfeed.manifest.json.partial");
        std::fs::write(&leftover, b"{}").unwrap();

        let policy = policy_with_dir(root, &["*.csv"]);
        // Even the real (non-zero) grace must not protect a manifest partial.
        SourceStager::crash_purge(&policy);
        assert!(
            !leftover.exists(),
            "a leftover manifest partial is an unconditional orphan"
        );
    }

    #[test]
    fn partial_is_orphaned_respects_the_grace_window() {
        // Unit-level proof that age alone gates a lockless partial: identical
        // fixture, opposite verdict purely from the grace duration.
        let stage_dir = tempfile::tempdir().unwrap();
        let root = stage_dir.path();
        let partial = root.join("abcd.run1.partial");
        std::fs::write(&partial, b"x").unwrap();

        assert!(
            !partial_is_orphaned(&partial, root, "abcd", REAP_GRACE),
            "a fresh lockless partial is protected by the grace window"
        );
        assert!(
            partial_is_orphaned(&partial, root, "abcd", NO_GRACE),
            "the same partial, aged out, is an orphan"
        );
    }

    #[test]
    fn crash_purge_is_noop_when_disabled_or_root_missing() {
        // Disabled policy: no-op even with a dir set.
        let stage_dir = tempfile::tempdir().unwrap();
        std::fs::write(stage_dir.path().join("x.partial"), b"x").unwrap();
        let mut policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        policy.enabled = false;
        SourceStager::crash_purge_with_grace(&policy, NO_GRACE);
        assert!(stage_dir.path().join("x.partial").exists());

        // Missing root: no panic, no error.
        let mut missing = policy_with_dir(stage_dir.path(), &["*.csv"]);
        missing.dir = Some(stage_dir.path().join("does-not-exist"));
        SourceStager::crash_purge_with_grace(&missing, NO_GRACE);
    }

    #[cfg(unix)]
    #[test]
    fn staged_file_and_manifest_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("secret.csv");
        std::fs::write(&src, b"ssn,balance\n123,9999\n").unwrap();

        let policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        let mut stager = SourceStager::new(policy);
        let staged = stager.resolve(src).unwrap().staged.unwrap();

        let file_mode = std::fs::metadata(&staged).unwrap().permissions().mode() & 0o777;
        assert_eq!(file_mode, 0o600, "staged file must be owner-only");

        let id = staged
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .strip_suffix(".staged")
            .unwrap();
        let manifest = stage_dir.path().join(format!("{id}.manifest.json"));
        let manifest_mode = std::fs::metadata(&manifest).unwrap().permissions().mode() & 0o777;
        assert_eq!(manifest_mode, 0o600, "manifest must be owner-only");
    }

    #[test]
    fn disk_cap_refuses_oversized_file() {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("big.csv");
        std::fs::write(&src, vec![b'x'; 4096]).unwrap();

        let policy = StagingPolicy {
            enabled: true,
            dir: Some(stage_dir.path().to_path_buf()),
            patterns: vec!["*.csv".into()],
            disk_cap_bytes: Some(ByteSize(1024)),
            ..Default::default()
        };
        let mut stager = SourceStager::new(policy);
        let err = stager.resolve(src).unwrap_err();
        assert!(matches!(err, StagingError::DiskCapExceeded { .. }));
    }

    #[test]
    fn disk_cap_accumulates_across_files() {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let a = src_dir.path().join("a.csv");
        let b = src_dir.path().join("b.csv");
        std::fs::write(&a, vec![b'a'; 600]).unwrap();
        std::fs::write(&b, vec![b'b'; 600]).unwrap();

        let policy = StagingPolicy {
            enabled: true,
            dir: Some(stage_dir.path().to_path_buf()),
            patterns: vec!["*.csv".into()],
            disk_cap_bytes: Some(ByteSize(1000)),
            ..Default::default()
        };
        let mut stager = SourceStager::new(policy);
        assert!(stager.resolve(a).unwrap().staged.is_some());
        let err = stager.resolve(b).unwrap_err();
        assert!(matches!(err, StagingError::DiskCapExceeded { .. }));
    }

    #[test]
    fn manifest_round_trips_and_freshness_matches() {
        let src_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        std::fs::write(&src, b"abc").unwrap();
        let meta = std::fs::metadata(&src).unwrap();
        let (secs, nanos) = mtime_parts(&meta).unwrap();
        let manifest = StagedManifest {
            source_path: src.clone(),
            source_mtime_secs: secs,
            source_mtime_nanos: nanos,
            source_size: meta.len(),
            content_hash: "deadbeef".into(),
            staged_at: now_rfc3339(),
        };
        let json = serde_json::to_vec(&manifest).unwrap();
        let back: StagedManifest = serde_json::from_slice(&json).unwrap();
        assert_eq!(back, manifest);
        assert!(
            back.matches_source(&meta),
            "round-tripped manifest is fresh"
        );

        // A size change makes it stale.
        std::fs::write(&src, b"abcd").unwrap();
        let meta2 = std::fs::metadata(&src).unwrap();
        assert!(!back.matches_source(&meta2), "a size change is stale");
    }

    #[test]
    fn plan_entry_in_place_when_disabled_or_unmatched() {
        // Staging disabled → in place.
        let stager = SourceStager::new(StagingPolicy::default());
        let entry = stager.plan_entry(Path::new("/data/orders.csv"));
        assert!(!entry.staged);
        assert_eq!(entry.staged_path, None);
        assert_eq!(entry.reuse, ReuseDecision::NotApplicable);

        // Enabled but the pattern does not match → in place.
        let stage_dir = tempfile::tempdir().unwrap();
        let stager = SourceStager::new(policy_with_dir(stage_dir.path(), &["*.csv"]));
        let entry = stager.plan_entry(Path::new("/data/orders.json"));
        assert!(!entry.staged);
        assert_eq!(entry.staged_path, None);
    }

    #[test]
    fn plan_entry_reports_staged_path_and_reuse_miss_when_absent() {
        // A matched source under reuse with no prior copy → staged, reuse miss,
        // and the staged path is the stable content-addressed path under root.
        let stage_dir = tempfile::tempdir().unwrap();
        let src_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        std::fs::write(&src, b"a,b\n1,2\n").unwrap();
        let policy = StagingPolicy {
            on_existing: OnExisting::Reuse,
            ..policy_with_dir(stage_dir.path(), &["*.csv"])
        };
        let stager = SourceStager::new(policy);
        let entry = stager.plan_entry(&src);
        assert!(entry.staged);
        let expected = stage_dir
            .path()
            .join(format!("{}.{STAGED_EXT}", source_id(&src)));
        assert_eq!(entry.staged_path.as_deref(), Some(expected.as_path()));
        assert_eq!(
            entry.reuse,
            ReuseDecision::Miss,
            "no prior copy exists, so the real run would re-stage"
        );
    }

    #[test]
    fn plan_entry_reports_reuse_hit_after_a_fresh_copy() {
        // Stage once, then a read-only plan_entry under reuse predicts a hit —
        // matching the run-time freshness check without copying again.
        let stage_dir = tempfile::tempdir().unwrap();
        let src_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        std::fs::write(&src, b"a,b\n1,2\n").unwrap();
        let policy = StagingPolicy {
            on_existing: OnExisting::Reuse,
            ..policy_with_dir(stage_dir.path(), &["*.csv"])
        };
        let mut stager = SourceStager::new(policy.clone());
        stager.resolve(src.clone()).expect("first stage copies");

        let planner = SourceStager::new(policy);
        let entry = planner.plan_entry(&src);
        assert!(entry.staged);
        assert_eq!(
            entry.reuse,
            ReuseDecision::Hit,
            "a committed fresh copy must predict a reuse hit"
        );

        // Rewriting the source makes the copy stale → the prediction flips to miss.
        std::fs::write(&src, b"a,b\n1,2\n3,4\n").unwrap();
        assert_eq!(planner.plan_entry(&src).reuse, ReuseDecision::Miss);
    }

    #[test]
    fn plan_entry_reuse_not_applicable_under_overwrite() {
        // Under the default overwrite policy the reuse cache does not apply even
        // when a matching copy exists.
        let stage_dir = tempfile::tempdir().unwrap();
        let src_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        std::fs::write(&src, b"a,b\n1,2\n").unwrap();
        let stager = SourceStager::new(policy_with_dir(stage_dir.path(), &["*.csv"]));
        let entry = stager.plan_entry(&src);
        assert!(entry.staged);
        assert_eq!(entry.reuse, ReuseDecision::NotApplicable);
    }
}
