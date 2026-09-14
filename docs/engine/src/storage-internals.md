# Staging, Crash Durability & Locks

*User-facing view: the User Guide's "Storage & Spill Location" page.*

This page is the engine-internals reference for the durability and concurrency mechanics behind Clinker's storage subsystem: how a matched source file is copied to a local staging volume without ever leaving a corrupt or half-trusted artifact behind, how concurrent `clinker` invocations sharing one staging or spill volume coordinate through advisory locks, and how a startup crash purge reclaims the artifacts a `SIGKILL`-ed run could not clean up. The depth here is the staging copy protocol (single-pass copy + hash, atomic publish via rename, parent-directory `fsync`, verify, manifest commit), the per-source reader-writer lock semantics, the orphan-detection liveness gates, the file-permission model, and the filesystem-journal reasoning behind the directory `fsync`. The user-facing page documents the `[storage]` config block, the spill dir, disk cap, compression, and observability surfaces; those are out of scope here.

## Prepared output storage

The preparation library can seal complete operation bytes in finite memory or
executor-backed raw temporary storage. This is an additive API; current codec
and CLI constructors have not been migrated to it. It is distinct from source
file staging and output publication described below.

An executor provider with no resolved spill root is memory-only. It never
silently uses the operating system temporary directory. With a resolved root,
each operation reserves its future descriptor slot and file/path metadata
before accepting bytes, plus a 16 KiB progress buffer retained through
readback. Memory is held in independently admitted 16 KiB chunks. A spill
request, memory-budget refusal or the 64 KiB residency threshold moves those
bytes to a raw file. The threshold is not an operation-size ceiling: disk
staging can continue within the supplied quota. Standalone memory-only staging
is limited by its explicit budget, not by that spill threshold.

Every temporary write admits at most 8 KiB of prospective disk growth first.
Short writes retain only the bytes actually written and release unused quota;
memory remains charged until its copy has finished spilling. Sealing rewinds
storage and establishes its exact byte length. The existing stage box becomes
readback storage, and the retained progress buffer streams bytes to the
destination without a second operation-sized allocation. A truncated readback
fails instead of delivering an apparently complete operation.

Admission, allocation, quota and cancellation failures retain a typed inline
`ResourceError`. `StageStorage::failure` and `resource_error` recover that
evidence from the nonallocating standard-I/O sentinel at write, flush and
readback boundaries. Direct storage callers must inspect this channel instead
of interpreting the sentinel as an unclassified data error. Failed memory
storage cannot accept more writes or seal its prefix. Metadata/progress/box
failure notifications run while the original owners and grants are live.
No error needs a heap-allocated diagnostic after resource denial.

Before delivery, errors leave the destination and committed encoder state
untouched. Generic destination I/O may accept a prefix before failing; after
delivery begins, `PreparedWriter` poisons continuation on any error and never
retries or commits pending state. Even zero-byte destination acceptance is a
delivery failure. Storage is closed and removed before encoder state commits;
cleanup failure after byte delivery also prevents commit and poisons the writer.
Drop performs cleanup but does not finalize or retry destination writes.

Unlink failure transfers the exact disk charge, path grant and descriptor slot
to an already-admitted cleanup-debt slot. Retry runs outside admission locks;
failure never reports reclaimed bytes. An uncertain native close retains
conservative debt and is never retried by raw handle, since the handle could
have been reused. Debt can outlive the provider in the run arbitrator, keeping
its memory consumer live. Confirmed removal releases charges exactly once;
unresolved debt remains visible at run teardown. Cleanup still runs when the
run is cancelled. The [memory contract](memory-arbitration.md#prepared-output-telemetry)
documents the fixed lifecycle counters and their limits.

## How a file is staged

When `storage.staging` is enabled and a source path matches a configured pattern, the source is copied to a local volume before the pipeline reads it. Each matched source maps to a **stable, content-addressed** set of files directly under the staging `dir`, deterministic across runs of the same source:

- `<source-id>.staged` — the local copy the reader opens.
- `<source-id>.manifest.json` — a sidecar recording the source's identity: its path, modification time, size, the BLAKE3 content hash, and the stage time.
- `<source-id>.lock` — a small advisory-lock file that serializes concurrent invocations staging the same source. It carries no data and persists between runs as the per-source coordination point, alongside the cached copy it guards.

`<source-id>` is derived from the source's canonical path, so the same source always resolves to the same staged file. That stability is what makes the `reuse` cache work — a later run can find the prior copy — and it is why the layout is stable rather than per-run UUIDs.

The copy is built to survive a crash at any point without leaving a corrupt or partial file a later run might trust. The five steps are ordered so that the *only* trustworthy state is one a crash cannot fabricate:

1. **Single-pass copy + hash.** The source is read once in ~1 MiB chunks; each chunk is fed to both the BLAKE3 hasher and the destination file in the same pass. The copy never holds the whole file in memory, so it stays a memory-budget no-op regardless of file size.

2. **Atomic publish.** Bytes are written to a `<source-id>.<run>.partial` temp file, flushed and `fsync`'d, then **renamed** to `<source-id>.staged`. A rename is an atomic replace on Linux, macOS, and Windows (Windows 10 1607+), so a reader scanning for `.staged` files sees either nothing or the complete file — never a half-written one. The `<run>` segment in the partial name keeps any two in-flight copies of one source on distinct temp files, and the per-source lock (see [the staging cache](#the-staging-cache)) ensures only one of them ever runs at a time.

3. **Durable rename.** On Linux/macOS the parent directory is `fsync`'d after the rename, because on ext4/xfs a rename is only crash-durable once the directory entry itself is flushed. On Windows the NTFS journal makes the rename durable, so there is no separate directory flush to do. (See [Crash durability and the parent-directory fsync](#crash-durability-and-the-parent-directory-fsync) below for the full filesystem-journal reasoning.)

4. **Verify.** With `verify = blake3` (the default) the source is independently re-read and hashed, and the two digests must match. A size check cannot catch a soft-mount that silently truncated the read; two content digests can. A mismatch removes the published copy and fails the run with a distinct "staged copy is corrupt" diagnostic (E335) — not a generic I/O error.

5. **Commit the manifest.** The identity manifest is written with the same atomic temp-file + rename discipline. **The manifest is the commit marker:** a `.staged` file is only trustworthy once its manifest exists. A crash between the copy and the manifest leaves a `.staged` with no manifest, which the next run's [crash purge](#crash-purge-of-orphaned-artifacts) reaps as an orphan rather than half-trusting.

If the copy fails partway, the `.partial` is removed before the error propagates. The invariant that closes the protocol: a complete `.staged` paired with a committed `.manifest.json` is the *only* shape any later run will trust, and that pairing cannot exist unless every step above completed.

## The staging cache

Because staged copies live at stable paths, a copy from a prior run is still on disk when the next run starts (unless `cleanup` removed it). The `on_existing` policy decides what happens when that prior copy is found — `overwrite` always re-stages, `error` refuses, and `reuse` reuses the prior copy only when it is still fresh (the source's current modification time and size both match what the manifest recorded). A fresh `reuse` match skips the copy entirely: no bytes read off the share, nothing charged against the disk cap. The freshness check is mtime + size, not a re-hash, so it is a cheap `stat` rather than a full read of the source.

The internals that matter here are how this stays correct under **concurrent** invocations. Under the partition-and-run model — several `clinker` processes over a partitioned input sharing one staging volume — independent runs may stage, reuse, or clean up the *same* shared source at the same time. The per-source `<source-id>.lock` file is a **reader-writer (shared/exclusive) advisory lock** that keeps every such overlap safe on Linux, macOS, and Windows:

- **Exactly one copy.** A run that needs to copy takes the lock *exclusively* for its copy-and-publish. The first run to take it copies and publishes; every other run blocks, then acquires the lock, finds the now-fresh `.staged`, and reuses it. So a source is copied exactly once no matter how many invocations race for it.

- **A reader is never yanked.** A run reading a staged copy holds the lock in *shared* mode for as long as it has the file open, and keeps it held across the moment it decides to reuse a copy and the moment it opens that copy — so the file it chose cannot be deleted or replaced in between. Any number of readers share the lock at once, so concurrent runs all read the same copy in parallel.

- **Cleanup and overwrite wait for readers.** Removing or re-copying a staged pair takes the lock *exclusively*, which a live reader's shared lock blocks. Cleanup probes the lock without waiting (a try-lock): if a concurrent run is still reading the copy, cleanup leaves it in place — the last run to release it, or a later [crash purge](#crash-purge-of-orphaned-artifacts), reaps it. An overwrite re-stage instead *waits* for in-flight readers to finish, then publishes atomically.

The reader-vs-writer distinction in lock-acquisition discipline is the whole safety argument: a copy/cleanup/overwrite mutates the staged pair and must be exclusive; a reuse-and-read only observes it and can be shared; and because the shared lock is held across the choose-then-open gap, no exclusive holder can slip a delete or replace into that window.

### Windows share-mode interoperation

On POSIX an unlinked-but-open file stays readable, so a concurrent delete or atomic-rename replace coexists naturally with an open reader. Windows has no such default. To match the POSIX behavior, the staged copy is opened on Windows with a share mode that permits a concurrent delete or atomic-rename replace (`FILE_SHARE_DELETE`), so an open reader and a concurrent publish/cleanup interoperate there exactly as they do on POSIX. The net guarantee across any mix of concurrent runs sharing a staged source: a reader always sees a complete, coherent `.staged` file and no run fails spuriously.

## Crash purge of orphaned artifacts

A clean (or panicking) run runs its configured `cleanup`. But a `SIGKILL`, the Linux OOM-killer, or a power loss kills the process before any cleanup runs, leaking its staged artifacts under the staging root. To stop that from accumulating across crashes, **every run performs an idempotent crash purge at startup**, before it stages anything. It reaps four orphan shapes left under the staging root:

- a `*.partial` — an interrupted copy. Reaped **only when its owning run is dead** (see the liveness gate below), so a concurrent sibling's in-flight copy is never reaped;
- a `*.staged` with no matching manifest — a copy that crashed before it could commit its manifest;
- a `*.manifest.json` with no matching staged file;
- a `*.lock` whose source has no surviving cache entry — a coordination lock left by a source that is no longer staged (not necessarily from a crash), reclaimed under the liveness and age gates below.

A clean pair (a `.staged` with its committed `.manifest.json`) is the reuse cache and is **kept** — the purge never removes a complete, trustworthy copy — and the source's `.lock` is kept alongside it so a later reuse run has a lock to take.

### Liveness gate: try-lock vs exclusive, and the creation grace window

The purge must distinguish a crash corpse from a live sibling's in-flight work, because several invocations can share one staging volume. It tells them apart the same way the spill-directory purge does — it asks the operating system "is anyone still staging this?" rather than guessing — and a reap proceeds only when **both** of two gates pass:

1. **Acquirable under a try-lock.** A `.partial` is reaped only when the source's `.lock` is acquirable under a non-blocking try-lock. If the try-lock succeeds, no live process holds the lock, so the owning run is gone. If the lock is still held, a concurrent live run owns the work and the artifact is kept.

2. **Aged past a short creation grace window.** Even with an acquirable lock, the artifact must have aged past a short creation grace window. This covers the race where a sibling has *just* started a copy — created the `.partial` — but has not yet taken the lock. A partial too young to have been locked yet is kept regardless of the try-lock result.

The actual removal is then performed while the purge holds the lock **exclusively**, so the reap itself cannot race a sibling mid-acquire.

A `.lock` whose source has **no surviving cache entry** (no `.staged` and no `.manifest.json`) is itself reclaimed under the same two-gate discipline: removed only when it is acquirable under a try-lock and has aged past the creation grace window, with the removal performed under the exclusive lock. A held lock, a lock still guarding a cached copy, and a freshly created lock are all kept. This bounds what would otherwise be unbounded growth of one zero-byte lock file per distinct source ever staged — relevant for a long-lived persistent cache (`on_existing = reuse`, `cleanup = never`) — while never removing a coordination point a live or cached source still needs. The net effect: a concurrent purge can never delete a running sibling's work, and a persistent staging root does not accumulate one orphan lock per source that has ever passed through it.

## File permissions

Staged copies hold verbatim source records — potentially PII, credentials, or financial data — and on a shared staging volume they must not be readable by other users. On Unix each staged file and its manifest are created with mode `0o600` (owner-only). On Windows there is no portable mode bit; staged files inherit the staging directory's ACL, so the directory's ACL must be restricted if the volume is shared. The asymmetry is deliberate: the Unix mode bit is enforced per-file at creation, whereas the Windows ACL model pushes the responsibility up to the directory the operator provisions.

## Crash durability and the parent-directory fsync

The atomic-rename guarantee that underpins both staging publish (step 2) and the manifest commit (step 5) only holds *across a crash* if the rename is durable. On POSIX filesystems (ext4, xfs) a rename's directory entry can still be in the page cache after `rename` returns: the inode's data is durable, but the directory entry that names it may not be, so a power loss between the rename and the next implicit flush could lose the entry and leave the file unreachable. To close that gap, Clinker `fsync`s the **parent directory** after the rename, forcing the directory entry to stable storage before the protocol proceeds.

Windows is intentionally exempt. The NTFS metadata journal already makes the rename crash-durable — the same semantics a `MOVEFILE_WRITE_THROUGH` rename requests — so the directory entry is journaled atomically with the rename and survives a crash without a separate flush. Windows also offers no directory-`fsync` equivalent to call, so there is nothing to do there. The single cross-platform rule is therefore: durable rename means `rename` + parent-dir `fsync` on ext4/xfs, and `rename` alone on NTFS, with the journal standing in for the directory flush.
