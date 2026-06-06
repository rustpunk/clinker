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
//!
//! ## The create-then-lock window, and why a grace period closes it
//!
//! A run cannot create its spill directory and take the lock in one atomic
//! syscall: [`tempfile::Builder`] makes the directory, then [`lock_spill_dir`]
//! creates and locks `.lock` inside it. For a few milliseconds the directory
//! exists with no lock — and, sharing the OS temp dir as the default spill root,
//! a concurrent `clinker run` doing its own startup [`spill_crash_purge`] could
//! observe that lockless dir. A naïve "no `.lock` file ⇒ orphan" rule would then
//! delete a live sibling's in-flight spill dir out from under it, killing the
//! victim with an ENOENT the moment it tried to write its own lock or a spill
//! file. (Concurrent invocations sharing a spill root are a supported scaling
//! story, so this is a real correctness bug, not a theoretical one.)
//!
//! The purge closes that window with a **creation grace period**
//! ([`REAP_GRACE`]): a `clinker-spill-*` directory younger than the grace period
//! is never reaped, regardless of its lock state. Liveness for an *established*
//! directory is still decided by the lock — a held lock is never reaped — but a
//! freshly created, not-yet-locked directory is protected by age alone until its
//! owner has had time to take the lock. The grace period is generous relative to
//! the sub-millisecond create→lock latency yet far shorter than the interval
//! between runs, so a genuine pre-lock crash corpse is still reaped on the next
//! startup once it has aged past the window.

use std::fs::File;
use std::io;
use std::path::Path;
use std::time::{Duration, SystemTime};

// `fs4::FileExt` implements `try_lock` / `unlock` for `std::fs::File` under the
// crate's `sync` feature (the only fs4 feature this workspace enables), giving a
// cross-platform OS advisory lock (`flock` on Unix, `LockFileEx` on Windows).
use fs4::FileExt;

/// Filename of the per-run spill-directory lock the live run holds open.
const SPILL_LOCK_NAME: &str = ".lock";
/// Prefix every per-run spill directory carries under the spill root.
const SPILL_DIR_PREFIX: &str = "clinker-spill-";

/// How recently a `clinker-spill-*` directory must have been created before the
/// crash-purge will leave it alone unconditionally, independent of its lock
/// state.
///
/// This closes the unavoidable create-then-lock window: a run makes its spill
/// directory and only then creates and locks `.lock` inside it, so for a brief
/// instant a live run's directory has no lock. A concurrent purge must not
/// mistake that lockless newborn for a crash corpse and delete it. Five seconds
/// dwarfs the sub-millisecond create→lock latency while staying far below the
/// gap between runs, so a true pre-lock crash corpse still ages past the window
/// and is reaped on a later startup.
const REAP_GRACE: Duration = Duration::from_secs(5);

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
/// `clinker-spill-*` child directory, the purge first checks the directory's
/// age: a directory younger than [`REAP_GRACE`] is left alone unconditionally,
/// since a live sibling may have just created it and not yet taken its lock.
/// For an older directory the purge tries to take the directory's `.lock`; a
/// successful acquire means the owning process is gone, so the whole directory
/// is removed. A directory whose lock is still held (a concurrent live run) is
/// skipped. An aged-out directory with no `.lock` file at all is an orphan from
/// a run that crashed before it could create the lock.
///
/// This is concurrency-safe by construction: it can never reap a live sibling's
/// spill directory. A held lock is never reaped, and a not-yet-locked newborn is
/// protected by the grace window until its owner takes the lock — so concurrent
/// `clinker run` invocations sharing one spill root (the default OS temp dir)
/// stay isolated.
///
/// Best-effort and non-fatal: every error is logged, never propagated — a purge
/// failure must not abort an otherwise-valid run, and the next startup retries.
pub fn spill_crash_purge(spill_root: &Path) {
    spill_crash_purge_with_grace(spill_root, REAP_GRACE);
}

/// [`spill_crash_purge`] with the creation grace window injected, so tests can
/// drive both sides of the window without sleeping: a zero grace exercises the
/// aged-out reap path immediately, the real grace exercises newborn protection.
fn spill_crash_purge_with_grace(spill_root: &Path, grace: Duration) {
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
        if dir_is_orphaned(&dir, grace) {
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
/// longer alive — and therefore safe to reap.
///
/// Two conditions must both hold:
///
/// 1. The directory is older than `grace` (in production [`REAP_GRACE`]). A
///    younger directory may be a live sibling that just created it and has not
///    yet taken its lock, so it is never an orphan regardless of lock state —
///    this is what makes a concurrent purge unable to delete a newborn out from
///    under its owner.
/// 2. No live process holds the directory's `.lock`. A lock that acquires under
///    try-lock means the owner is gone; a missing `.lock` on an aged-out dir
///    means the owner crashed before it could create the lock; a lock that
///    returns `WouldBlock` means a live run holds it, so the dir is kept.
fn dir_is_orphaned(dir: &Path, grace: Duration) -> bool {
    // Age gate first: a freshly created dir is presumed live no matter what its
    // lock looks like, closing the create-then-lock window.
    if dir_age(dir).is_none_or(|age| age < grace) {
        return false;
    }
    let lock_path = dir.join(SPILL_LOCK_NAME);
    let Ok(file) = File::open(&lock_path) else {
        // No `.lock` (or it cannot be opened) on a dir already past the grace
        // window: a run that crashed before it reached the lock step, an orphan.
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

/// Age of a `clinker-spill-*` directory used to decide whether its owner has had
/// time to take the lock, or `None` when no timestamp is readable (treated by
/// the caller as "too young to reap", erring toward keeping a directory it
/// cannot date).
///
/// The `.lock` file's mtime is the primary signal: the lock is created a hair
/// after the directory and is the canonical "this run started" marker, so its
/// age tracks the create-then-lock window directly. A directory with no `.lock`
/// yet falls back to the directory's own mtime — a lockless newborn is one whose
/// owner has not reached the lock step, so its directory mtime is recent and the
/// grace gate keeps it. Whichever timestamp is used, a still-mid-write run never
/// looks older than its true start, so the age estimate stays conservative (more
/// likely to keep than to reap).
fn dir_age(dir: &Path) -> Option<Duration> {
    let started = std::fs::metadata(dir.join(SPILL_LOCK_NAME))
        .and_then(|m| m.modified())
        .or_else(|_| std::fs::metadata(dir).and_then(|m| m.modified()))
        .ok()?;
    // A clock skew that puts the timestamp in the future yields a zero-ish age,
    // which the grace gate treats as "too young" — the conservative direction.
    SystemTime::now().duration_since(started).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Reap-path tests pass a zero grace so a freshly created fixture dir is
    // immediately eligible by age — exercising the lock/no-lock orphan logic
    // without sleeping. Newborn-protection tests use the real grace
    // (`spill_crash_purge`) so the just-created dir is inside the window.
    const NO_GRACE: Duration = Duration::ZERO;

    #[test]
    fn purge_reaps_an_orphaned_spill_dir() {
        // An orphaned spill dir from a crashed run: it has a `.lock` no process
        // holds. Past the grace window, the purge must take the lock (owner
        // gone) and remove the dir.
        let root = tempfile::tempdir().unwrap();
        let orphan = root.path().join("clinker-spill-deadbeef");
        std::fs::create_dir(&orphan).unwrap();
        // A crashed run leaves a `.lock` (unlocked, since the process died) plus
        // a leaked spill file inside.
        File::create(orphan.join(SPILL_LOCK_NAME)).unwrap();
        std::fs::write(orphan.join("partition-0.spill"), b"leaked").unwrap();

        spill_crash_purge_with_grace(root.path(), NO_GRACE);
        assert!(!orphan.exists(), "an orphaned spill dir must be reaped");
    }

    #[test]
    fn purge_reaps_an_aged_dir_with_no_lock_file() {
        // A run that crashed before creating its `.lock` leaves a lockless dir.
        // Once it ages past the grace window it is an orphan and must be reaped.
        let root = tempfile::tempdir().unwrap();
        let orphan = root.path().join("clinker-spill-nolock");
        std::fs::create_dir(&orphan).unwrap();
        std::fs::write(orphan.join("partition-0.spill"), b"leaked").unwrap();

        spill_crash_purge_with_grace(root.path(), NO_GRACE);
        assert!(!orphan.exists(), "an aged lockless spill dir is an orphan");
    }

    #[test]
    fn purge_keeps_a_live_locked_spill_dir() {
        // A concurrent live run holds its dir's lock. The purge must NOT reap a
        // directory whose lock is still held, even once it is past the grace
        // window (zero grace here removes age from the equation).
        let root = tempfile::tempdir().unwrap();
        let live = root.path().join("clinker-spill-alive");
        std::fs::create_dir(&live).unwrap();
        // Simulate the live run by holding the lock for the duration of the
        // purge call.
        let held = lock_spill_dir(&live).unwrap();

        spill_crash_purge_with_grace(root.path(), NO_GRACE);
        assert!(live.exists(), "a live, locked spill dir must be kept");
        // Releasing the lock lets a subsequent purge reap it (the owner is now
        // gone), proving the keep decision was the lock, not the name.
        drop(held);
        spill_crash_purge_with_grace(root.path(), NO_GRACE);
        assert!(
            !live.exists(),
            "once the lock is released the dir is reapable"
        );
    }

    #[test]
    fn purge_keeps_a_newborn_dir_with_no_lock_yet() {
        // The create-then-lock window: a live sibling has just created its spill
        // dir but has not yet taken the lock. With the real grace window in
        // force the freshly created dir must be left alone, never reaped out from
        // under its owner. This is the regression guard for the TOCTOU race that
        // let a concurrent purge delete a live run's spill dir.
        let root = tempfile::tempdir().unwrap();
        let newborn = root.path().join("clinker-spill-newborn");
        std::fs::create_dir(&newborn).unwrap();
        std::fs::write(newborn.join("partition-0.spill"), b"in-flight").unwrap();

        spill_crash_purge(root.path());
        assert!(
            newborn.exists(),
            "a just-created, not-yet-locked spill dir must be kept"
        );
    }

    #[test]
    fn purge_keeps_a_newborn_locked_dir() {
        // A live sibling that already created its dir and took the lock, all
        // within the grace window. Both gates (age and held lock) say keep.
        let root = tempfile::tempdir().unwrap();
        let newborn = root.path().join("clinker-spill-newborn-locked");
        std::fs::create_dir(&newborn).unwrap();
        let _held = lock_spill_dir(&newborn).unwrap();

        spill_crash_purge(root.path());
        assert!(newborn.exists(), "a young, locked spill dir must be kept");
    }

    #[test]
    fn dir_is_orphaned_respects_the_grace_window() {
        // Unit-level proof that age alone gates a not-yet-locked dir: identical
        // fixture, opposite verdict purely from the grace duration.
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("clinker-spill-gradient");
        std::fs::create_dir(&dir).unwrap();

        assert!(
            !dir_is_orphaned(&dir, REAP_GRACE),
            "a fresh lockless dir is protected by the grace window"
        );
        assert!(
            dir_is_orphaned(&dir, NO_GRACE),
            "the same dir, aged out, is an orphan"
        );
    }

    #[test]
    fn purge_ignores_non_spill_entries() {
        let root = tempfile::tempdir().unwrap();
        let unrelated = root.path().join("not-a-spill-dir");
        std::fs::create_dir(&unrelated).unwrap();
        let file = root.path().join("clinker-spill-but-a-file");
        std::fs::write(&file, b"x").unwrap();

        spill_crash_purge_with_grace(root.path(), NO_GRACE);
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
