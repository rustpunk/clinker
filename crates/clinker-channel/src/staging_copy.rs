//! Source-file staging copy: single-pass streamed copy with inline BLAKE3
//! verification, atomic publish, and durability/permission hardening.
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
//! ## Copy invariants
//!
//! - **Single pass.** Each ~1 MiB chunk is read once and fed to both the
//!   BLAKE3 hasher and the destination file. No second read of the source, so
//!   the copy is a memory-budget no-op (one fixed buffer, no per-file
//!   accumulation).
//! - **Atomic publish.** Bytes land in `<file_uuid>.partial`, are `sync_all`'d,
//!   then `rename`d to `<file_uuid>.staged`. `std::fs::rename` is an
//!   atomic-replace on Linux/macOS/Windows (Win10 1607+), so a concurrent
//!   reader sees either no `.staged` file or the complete one — never a torn
//!   write. A crash before the rename leaves only a `.partial`, which the
//!   per-run [`tempfile::TempDir`] removes on drop.
//! - **Durable rename (POSIX).** After the rename the parent directory is
//!   `sync_all`'d so the rename survives a crash. NTFS does not need this (see
//!   [`SourceStager::stage_file`]).
//! - **Restrictive permissions (Unix).** The per-run dir is `0o700` and each
//!   staged file `0o600`, because staged copies hold verbatim source records —
//!   potentially PII or credentials — on what may be a shared volume.

use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use clinker_plan::config::{StagedPath, StagingPolicy, StagingVerify};

/// Chunk size for the streamed copy. 1 MiB amortizes per-`read`/`write`
/// syscall overhead against a fixed, bounded buffer — large enough to keep the
/// pipe full on a fast local disk, small enough that the copy never scales its
/// memory with file size.
const COPY_CHUNK_BYTES: usize = 1024 * 1024;

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
}

/// A staged copy's recorded identity, produced by the per-file copy.
///
/// Captures the integrity facts the stager logs as provenance: the local path,
/// the source it came from, the content digest computed during the copy, and
/// the source's size at copy time. Internal to the copy engine — the public
/// [`SourceStager::resolve`] hands the reader a [`StagedPath`], and the rest is
/// folded into the staged-file log line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StagedFile {
    /// The local `.staged` copy the reader should open.
    staged_path: PathBuf,
    /// The original source path the copy was made from.
    source_path: PathBuf,
    /// Hex BLAKE3 digest of the copied bytes.
    content_hash: String,
    /// Source size in bytes at copy time.
    source_size: u64,
}

/// Per-run staging engine: owns the run's staging subdirectory and the running
/// byte total, and performs the copy for each selected source.
///
/// One `SourceStager` is built per pipeline run. The first staged file
/// lazily creates the per-run `<dir>/<run_uuid>/` subdirectory (mode `0o700`
/// on Unix); subsequent files reuse it. The subdirectory is held as a
/// [`tempfile::TempDir`], so a panic or early exit removes the whole run's
/// staged copies on drop — the same panic-safety story the executor's spill
/// root uses. Disk-cap accounting accumulates across every file the run
/// stages.
///
/// When the policy is disabled or a path does not match a staging pattern,
/// [`SourceStager::resolve`] returns an in-place [`StagedPath`] and never
/// touches the filesystem.
pub struct SourceStager {
    policy: StagingPolicy,
    /// The per-run subdirectory, created on first use. Held as a `TempDir` so
    /// staged copies are torn down on drop or on a panic — the same
    /// panic-safety story the executor's spill root uses.
    run_dir: Option<tempfile::TempDir>,
    /// Cumulative bytes copied this run, checked against the disk cap.
    bytes_staged: u64,
}

impl SourceStager {
    /// Build a stager for one run from the workspace staging policy.
    ///
    /// No filesystem work happens here — the per-run subdirectory is created
    /// lazily on the first file that actually stages, so a run whose sources
    /// never match a pattern leaves the staging dir untouched.
    pub fn new(policy: StagingPolicy) -> Self {
        Self {
            policy,
            run_dir: None,
            bytes_staged: 0,
        }
    }

    /// Resolve one source path, staging it if the policy selects it.
    ///
    /// Returns an in-place [`StagedPath`] (no copy, no filesystem touch) when
    /// staging is disabled or `original` matches no pattern. When the path is
    /// selected, copies it into the per-run subdirectory with inline BLAKE3
    /// verification and atomic publish, then returns a [`StagedPath`] whose
    /// `staged` points at the local copy.
    ///
    /// # Errors
    ///
    /// Returns [`StagingError`] when the per-run dir cannot be created, the
    /// copy fails, the digest does not match (`verify = blake3`), or the disk
    /// cap would be exceeded.
    pub fn resolve(&mut self, original: PathBuf) -> Result<StagedPath, StagingError> {
        if !self.policy.pattern_matches(&original) {
            return Ok(StagedPath::in_place(original));
        }
        let staged = self.stage_file(&original)?;
        // Record the copy's provenance: the operator (and any post-run audit)
        // wants the source, its size, the local copy, and the verified digest
        // so a staged run is traceable back to exact bytes.
        tracing::info!(
            source = %staged.source_path.display(),
            staged = %staged.staged_path.display(),
            bytes = staged.source_size,
            blake3 = %staged.content_hash,
            "staged source file to local disk"
        );
        Ok(StagedPath {
            original,
            staged: Some(staged.staged_path),
        })
    }

    /// Lazily create (once) and return the per-run staging subdirectory.
    ///
    /// On Unix the directory is created with mode `0o700` — staged copies hold
    /// verbatim source records (potentially PII / credentials) and must not be
    /// group/other-readable on a shared volume. On Windows the directory
    /// inherits the parent's ACL, the platform's standard behavior; there is no
    /// portable mode bit to set, so this is left to NTFS inheritance.
    fn run_dir(&mut self) -> Result<&Path, StagingError> {
        if self.run_dir.is_none() {
            let parent = self
                .policy
                .dir
                .as_ref()
                .expect("staging dir is validated present at startup when enabled");
            let mut builder = tempfile::Builder::new();
            builder.prefix("clinker-stage-");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                // 0o700: the per-run dir holds verbatim source records and must
                // be owner-only on a shared staging volume.
                builder.permissions(std::fs::Permissions::from_mode(0o700));
            }
            let dir = builder.tempdir_in(parent).map_err(|e| StagingError::Io {
                path: parent.clone(),
                source: e,
            })?;
            self.run_dir = Some(dir);
        }
        Ok(self.run_dir.as_ref().unwrap().path())
    }

    /// Copy one selected source into the per-run dir with inline hashing and
    /// atomic publish.
    ///
    /// The full lifecycle for a file:
    ///
    /// 1. `stat` the source for its size; charge the size against the run's
    ///    disk cap before copying a byte.
    /// 2. Open `<run_dir>/<file_uuid>.partial` (mode `0o600` on Unix).
    /// 3. Stream 1 MiB chunks: each chunk is read once, fed to the BLAKE3
    ///    hasher, then written to the temp file — one pass over the bytes.
    /// 4. `sync_all` the temp file. On macOS `File::sync_all` issues
    ///    `F_FULLFSYNC`, so the data is on stable storage before the rename.
    /// 5. `rename` `.partial` → `.staged`. `std::fs::rename` is atomic-replace
    ///    on all three target OSes, so a reader never observes a torn file.
    /// 6. (POSIX) `sync_all` the parent directory so the rename itself is
    ///    crash-durable — on ext4/xfs a rename is only durable once the
    ///    directory entry is fsync'd. NTFS makes the rename durable through the
    ///    journal, so this step is a Unix-only no-op on Windows.
    /// 7. When `verify = blake3`, independently re-hash the source and compare;
    ///    a mismatch is [`StagingError::Verify`].
    ///
    /// On any error after the temp file is created, the `.partial` is removed
    /// before the error propagates, so a failed stage never leaves a stray temp
    /// file behind (and the run-dir `TempDir` cleans up the rest on drop).
    fn stage_file(&mut self, source: &Path) -> Result<StagedFile, StagingError> {
        let source_size = std::fs::metadata(source)
            .map_err(|e| StagingError::Io {
                path: source.to_path_buf(),
                source: e,
            })?
            .len();

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

        // Each file is named by a fresh per-file uuid inside the per-run uuid
        // subdir, so a destination collision cannot occur — neither within a
        // run nor across runs sharing the staging root. That is exactly what
        // makes `on_existing` (overwrite/reuse/error) a no-op under this naming
        // scheme: there is never a pre-existing target to overwrite, reuse, or
        // refuse. The config knob is retained for a future content-addressed or
        // stable-name layout; today the uuid scheme subsumes it.
        let file_uuid = uuid::Uuid::new_v4().to_string();
        let run_dir = self.run_dir()?.to_path_buf();
        let partial = run_dir.join(format!("{file_uuid}.partial"));
        let staged = run_dir.join(format!("{file_uuid}.staged"));

        let copy_result = self.copy_into(source, &partial, &staged);
        match copy_result {
            Ok(content_hash) => {
                let content_hash = if self.policy.verify == StagingVerify::Blake3 {
                    verify_staged(source, &staged, content_hash)?
                } else {
                    content_hash
                };
                self.bytes_staged = self.bytes_staged.saturating_add(source_size);
                Ok(StagedFile {
                    staged_path: staged,
                    source_path: source.to_path_buf(),
                    content_hash,
                    source_size,
                })
            }
            Err(e) => {
                // Best-effort cleanup of the partial; the run-dir TempDir will
                // sweep anything left behind on drop regardless.
                let _ = std::fs::remove_file(&partial);
                Err(e)
            }
        }
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
}

/// Open the `.partial` temp file for writing, truncating any leftover, with
/// mode `0o600` on Unix.
///
/// Staged files hold verbatim source records; on a shared staging volume they
/// must not be group/other-readable. Unix sets the mode at create time via
/// `OpenOptionsExt`. Windows has no portable mode bit; the file inherits the
/// directory ACL, NTFS's standard behavior.
fn open_partial(partial: &Path) -> Result<File, StagingError> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(partial).map_err(|e| StagingError::Io {
        path: partial.to_path_buf(),
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

/// Fsync the directory containing `staged` so the rename that published it is
/// crash-durable. POSIX-only.
///
/// On ext4/xfs a `rename` is only guaranteed durable once the containing
/// directory's metadata is also fsync'd; without it a crash immediately after
/// the rename can leave the staged file missing even though the stage reported
/// success. Opening the directory as a `File` and calling `sync_all` is the
/// portable POSIX way to flush that directory entry.
///
/// Windows is intentionally skipped: NTFS makes a `rename` durable through its
/// metadata journal (the semantics `MOVEFILE_WRITE_THROUGH` requests), so there
/// is no separate directory handle to flush, and Windows offers no
/// `fsync(dir)` equivalent anyway.
#[cfg(unix)]
fn fsync_parent_dir(staged: &Path) -> Result<(), StagingError> {
    let Some(parent) = staged.parent() else {
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
fn fsync_parent_dir(_staged: &Path) -> Result<(), StagingError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_plan::config::utils::ByteSize;

    fn policy_with_dir(dir: &Path, patterns: &[&str]) -> StagingPolicy {
        StagingPolicy {
            enabled: true,
            dir: Some(dir.to_path_buf()),
            patterns: patterns.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
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
        // No run subdir created for a non-staging run.
        let entries: Vec<_> = std::fs::read_dir(stage_dir.path()).unwrap().collect();
        assert!(entries.is_empty());
    }

    #[test]
    fn matched_source_is_copied_byte_identical() {
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
        // The published file is `.staged`, and no `.partial` survives.
        assert_eq!(staged.extension().unwrap(), "staged");
        let leftovers: Vec<_> = std::fs::read_dir(staged.parent().unwrap())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "partial"))
            .collect();
        assert!(leftovers.is_empty(), "no .partial should remain");
    }

    #[test]
    fn blake3_mismatch_is_detected_as_distinct_error() {
        // The full verify path: stage a file (copy_into hashes the copied
        // bytes), then mutate the source before the independent verify read so
        // the fresh source hash diverges — modeling an NFS soft-mount that
        // delivered different bytes during the copy than the source now holds.
        // verify_staged is the exact branch stage_file runs, so this drives the
        // production decision, not a hand-built error.
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        std::fs::write(&src, b"original bytes that get copied").unwrap();

        let policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        let mut stager = SourceStager::new(policy);

        let run_dir = stager.run_dir().unwrap().to_path_buf();
        let partial = run_dir.join("x.partial");
        let staged = run_dir.join("x.staged");
        let copy_hash = stager.copy_into(&src, &partial, &staged).unwrap();
        assert!(staged.exists());

        // Source changes after the copy: the verify re-read no longer matches.
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
        // A failed verify removes the published staged copy.
        assert!(!staged.exists(), "verify failure must unlink the staged copy");
    }

    #[test]
    fn verify_passes_for_clean_copy() {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("data.csv");
        std::fs::write(&src, b"a,b,c\n1,2,3\n").unwrap();
        // Default policy verifies with BLAKE3; a clean copy must pass.
        let policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        let mut stager = SourceStager::new(policy);
        let resolved = stager.resolve(src.clone()).unwrap();
        assert!(resolved.staged.is_some());
    }

    #[test]
    fn no_torn_staged_file_when_copy_fails_midway() {
        // A read error mid-copy must leave no `.staged` file and remove the
        // `.partial`. Simulate by staging a path that exists for the size stat
        // but cannot be opened for read (a directory): the size stat succeeds,
        // the open-for-read or read fails, and the partial is cleaned up.
        let stage_dir = tempfile::tempdir().unwrap();
        let src_dir = tempfile::tempdir().unwrap();
        // A directory matched as a "source": metadata() works, File::open then
        // read() fails (reading a directory yields an error on Linux).
        let dir_as_src = src_dir.path().join("subdir.csv");
        std::fs::create_dir(&dir_as_src).unwrap();

        let policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        let mut stager = SourceStager::new(policy);
        let err = stager.resolve(dir_as_src).unwrap_err();
        assert!(matches!(err, StagingError::Io { .. }));

        // No `.staged` and no `.partial` survive in the run dir.
        let run_dir = stager.run_dir.as_ref().unwrap().path().to_path_buf();
        let survivors: Vec<_> = std::fs::read_dir(&run_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();
        assert!(
            survivors.is_empty(),
            "no partial/staged file should survive a failed copy, found {survivors:?}"
        );
    }

    #[test]
    fn crash_before_rename_leaves_no_torn_staged_file() {
        // The atomic-publish guarantee: a crash between writing `.partial` and
        // the rename must leave no `.staged` file. Model the crash by writing a
        // `.partial` and stopping before the rename — exactly the on-disk state
        // a process that died mid-stage would leave. A reader scanning for
        // `.staged` files sees nothing, and the run-dir TempDir removes the
        // orphaned `.partial` on drop.
        let stage_dir = tempfile::tempdir().unwrap();
        let policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        let mut stager = SourceStager::new(policy);
        let run_dir = stager.run_dir().unwrap().to_path_buf();

        // Simulate the pre-rename crash state: a partial exists, no staged yet.
        let partial = run_dir.join("interrupted.partial");
        std::fs::write(&partial, b"half-written bytes").unwrap();

        let staged_files: Vec<_> = std::fs::read_dir(&run_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "staged"))
            .collect();
        assert!(
            staged_files.is_empty(),
            "a crash before rename must leave zero .staged files"
        );

        // Dropping the stager removes the whole run dir, orphaned partial and
        // all — the panic-safety story #173 relies on.
        let run_dir_path = run_dir.clone();
        drop(stager);
        assert!(
            !run_dir_path.exists(),
            "run dir (and its orphaned .partial) must be removed on drop"
        );
    }

    #[cfg(unix)]
    #[test]
    fn staged_file_is_0600_and_run_dir_is_0700() {
        use std::os::unix::fs::PermissionsExt;
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("secret.csv");
        std::fs::write(&src, b"ssn,balance\n123,9999\n").unwrap();

        let policy = policy_with_dir(stage_dir.path(), &["*.csv"]);
        let mut stager = SourceStager::new(policy);
        let resolved = stager.resolve(src).unwrap();
        let staged = resolved.staged.unwrap();

        let file_mode = std::fs::metadata(&staged).unwrap().permissions().mode() & 0o777;
        assert_eq!(file_mode, 0o600, "staged file must be owner-only");

        let dir_mode = std::fs::metadata(staged.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700, "per-run dir must be owner-only");
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
            // Room for the first file (600) but not both (1200).
            disk_cap_bytes: Some(ByteSize(1000)),
            ..Default::default()
        };
        let mut stager = SourceStager::new(policy);
        assert!(stager.resolve(a).unwrap().staged.is_some());
        let err = stager.resolve(b).unwrap_err();
        assert!(matches!(err, StagingError::DiskCapExceeded { .. }));
    }

}
