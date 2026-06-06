//! Idempotent startup crash-purge of orphaned per-run spill directories, and
//! the per-run spill-directory lock that distinguishes a live run's spill dir
//! from a crashed run's leftovers.
//!
//! ## The leak this closes
//!
//! A run's spill directory is a [`tempfile::TempDir`] reaped on `Drop`. A clean
//! exit (or a panic that unwinds) runs `Drop` and removes it. But a `SIGKILL`,
//! the Linux OOM-killer, or a hard power loss skips `Drop` entirely, so the
//! crashed run's `clinker-spill-*` directory — and every spill file inside it —
//! leaks under the spill root indefinitely. Over many crashed runs that fills
//! the spill volume, the exact class of report this purge prevents.
//!
//! ## How a live dir is told apart from a corpse
//!
//! Each run, at spill-root creation, opens `<spill_dir>/.lock` and takes an
//! **OS advisory exclusive lock** on it ([`lock_spill_dir`]), holding the lock
//! file open for the whole run. The kernel releases the lock automatically when
//! the process exits — clean, panicked, or killed — so a crashed run's `.lock`
//! is free even though its directory survives.
//!
//! At the next run's startup, [`spill_crash_purge`] walks the spill root and,
//! for each `clinker-spill-*` directory, *tries* to take the same lock. Success
//! means no live process holds it (the owner died) → the directory is an orphan
//! and is reaped. Failure (`WouldBlock`) means a concurrent run owns it → it is
//! left alone. This is robust against PID reuse — it asks the kernel "is anyone
//! still holding this?" rather than guessing from a recorded PID — and it is the
//! same primitive a concurrency-safety layer can build a publish/lease protocol
//! on top of.

use std::fs::File;
use std::io;
use std::path::Path;

// `fs4::FileExt` implements `try_lock` / `unlock` for `std::fs::File` under the
// crate's `sync` feature (the only fs4 feature this workspace enables), giving a
// cross-platform OS advisory lock (`flock` on Unix, `LockFileEx` on Windows).
use fs4::FileExt;

/// Filename of the per-run spill-directory lock the live run holds open.
const SPILL_LOCK_NAME: &str = ".lock";
/// Prefix every per-run spill directory carries under the spill root.
const SPILL_DIR_PREFIX: &str = "clinker-spill-";

/// Create and exclusively lock `<spill_dir>/.lock`, returning the held lock
/// file. The caller keeps the returned [`File`] alive for the run; dropping it
/// (or the process exiting) releases the lock so the next run's
/// [`spill_crash_purge`] can tell this directory's owner is gone.
///
/// The lock is advisory and best-effort: on the rare platform or filesystem
/// where the lock cannot be acquired, the lock file is still created (so the
/// purge has a `.lock` to probe) and the run proceeds — at worst the purge
/// errs toward keeping a directory it could have reaped, never toward reaping a
/// live one.
///
/// # Errors
///
/// Returns the underlying [`io::Error`] only when the lock file cannot be
/// created at all (a genuinely unwritable spill directory), which the caller
/// surfaces as an internal fault since the directory was pre-validated writable.
pub fn lock_spill_dir(spill_dir: &Path) -> io::Result<File> {
    let lock_path = spill_dir.join(SPILL_LOCK_NAME);
    let file = File::create(&lock_path)?;
    // Best-effort: a failed lock (unsupported FS, etc.) still leaves the run
    // correct — the directory's own TempDir Drop is the primary cleanup, and a
    // missing lock only makes the purge conservative.
    if let Err(e) = FileExt::try_lock(&file) {
        tracing::debug!(
            lock = %lock_path.display(),
            error = %e,
            "could not take the spill-dir lock; crash-purge will be conservative for this dir"
        );
    }
    Ok(file)
}

/// Idempotently reap orphaned `clinker-spill-*` directories left by crashed
/// prior runs, run once at startup before the run creates its own spill dir.
///
/// `spill_root` is the resolved spill root — the configured
/// `storage.spill.dir`, or the OS temp dir when none is configured. For each
/// `clinker-spill-*` child directory, the purge tries to take the directory's
/// `.lock`; a successful acquire means the owning process is gone, so the whole
/// directory is removed. A directory whose lock is still held (a concurrent
/// live run) is skipped. A directory with no `.lock` file at all is treated as
/// an orphan from a run that crashed before it could create the lock.
///
/// Best-effort and non-fatal: every error is logged, never propagated — a purge
/// failure must not abort an otherwise-valid run, and the next startup retries.
pub fn spill_crash_purge(spill_root: &Path) {
    let entries = match std::fs::read_dir(spill_root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return,
        Err(e) => {
            tracing::warn!(
                root = %spill_root.display(),
                error = %e,
                "spill crash-purge could not read the spill root"
            );
            return;
        }
    };

    let mut reaped = 0usize;
    for entry in entries.flatten() {
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with(SPILL_DIR_PREFIX) {
            continue;
        }
        let dir = entry.path();
        if !dir.is_dir() {
            continue;
        }
        if dir_is_orphaned(&dir) {
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => reaped += 1,
                Err(e) => tracing::warn!(
                    dir = %dir.display(),
                    error = %e,
                    "spill crash-purge failed to remove an orphaned spill directory"
                ),
            }
        }
    }
    if reaped > 0 {
        tracing::info!(
            root = %spill_root.display(),
            reaped,
            "spill crash-purge reaped orphaned spill directories from prior runs"
        );
    }
}

/// Whether a `clinker-spill-*` directory is an orphan — its owning run is no
/// longer alive — decided by whether its `.lock` can be exclusively acquired.
///
/// A missing lock file means a run crashed before it could create the lock, so
/// the directory is an orphan. A lock that acquires means the owner is gone. A
/// lock that returns `WouldBlock` means a live run holds it, so the directory is
/// not an orphan.
fn dir_is_orphaned(dir: &Path) -> bool {
    let lock_path = dir.join(SPILL_LOCK_NAME);
    let Ok(file) = File::open(&lock_path) else {
        // No `.lock` (or it cannot be opened): a run that never reached the lock
        // step, hence an orphan.
        return true;
    };
    match FileExt::try_lock(&file) {
        Ok(()) => {
            // Acquired: the previous owner is gone. Release immediately so the
            // remove can proceed (on Windows an open handle would block it) and
            // a concurrent purge sees a consistent state.
            let _ = FileExt::unlock(&file);
            drop(file);
            true
        }
        Err(_) => {
            // A live run holds the lock; not an orphan.
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn purge_reaps_an_orphaned_spill_dir() {
        // An orphaned spill dir from a crashed run: it has a `.lock` no process
        // holds. The purge must take the lock (owner gone) and remove the dir.
        let root = tempfile::tempdir().unwrap();
        let orphan = root.path().join("clinker-spill-deadbeef");
        std::fs::create_dir(&orphan).unwrap();
        // A crashed run leaves a `.lock` (unlocked, since the process died) plus
        // a leaked spill file inside.
        File::create(orphan.join(SPILL_LOCK_NAME)).unwrap();
        std::fs::write(orphan.join("partition-0.spill"), b"leaked").unwrap();

        spill_crash_purge(root.path());
        assert!(!orphan.exists(), "an orphaned spill dir must be reaped");
    }

    #[test]
    fn purge_reaps_a_dir_with_no_lock_file() {
        // A run that crashed before creating its `.lock` leaves a lockless dir;
        // it is still an orphan and must be reaped.
        let root = tempfile::tempdir().unwrap();
        let orphan = root.path().join("clinker-spill-nolock");
        std::fs::create_dir(&orphan).unwrap();
        std::fs::write(orphan.join("partition-0.spill"), b"leaked").unwrap();

        spill_crash_purge(root.path());
        assert!(!orphan.exists(), "a lockless spill dir is an orphan");
    }

    #[test]
    fn purge_keeps_a_live_locked_spill_dir() {
        // A concurrent live run holds its dir's lock. The purge must NOT reap a
        // directory whose lock is still held.
        let root = tempfile::tempdir().unwrap();
        let live = root.path().join("clinker-spill-alive");
        std::fs::create_dir(&live).unwrap();
        // Simulate the live run by holding the lock for the duration of the
        // purge call.
        let held = lock_spill_dir(&live).unwrap();

        spill_crash_purge(root.path());
        assert!(live.exists(), "a live, locked spill dir must be kept");
        // Releasing the lock lets a subsequent purge reap it (the owner is now
        // gone), proving the keep decision was the lock, not the name.
        drop(held);
        spill_crash_purge(root.path());
        assert!(
            !live.exists(),
            "once the lock is released the dir is reapable"
        );
    }

    #[test]
    fn purge_ignores_non_spill_entries() {
        let root = tempfile::tempdir().unwrap();
        let unrelated = root.path().join("not-a-spill-dir");
        std::fs::create_dir(&unrelated).unwrap();
        let file = root.path().join("clinker-spill-but-a-file");
        std::fs::write(&file, b"x").unwrap();

        spill_crash_purge(root.path());
        assert!(unrelated.exists(), "non-prefixed dirs are left untouched");
        assert!(file.exists(), "a prefixed plain file is not a spill dir");
    }

    #[test]
    fn purge_is_noop_on_missing_root() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("does-not-exist");
        // Must not panic or error.
        spill_crash_purge(&missing);
    }

    #[test]
    fn lock_spill_dir_creates_a_lock_file() {
        let dir = tempfile::tempdir().unwrap();
        let _held = lock_spill_dir(dir.path()).unwrap();
        assert!(dir.path().join(SPILL_LOCK_NAME).exists());
    }
}
