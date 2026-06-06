# Storage & Spill Location

Blocking operators — `Aggregate`, sort, and grace-hash `Combine` — accumulate
state in memory up to the configured budget, then **spill to disk** when a
soft or hard memory threshold trips, rather than running the process out of
memory. By default those spill files land in the operating system's temporary
directory. The `[storage]` block in `clinker.toml` lets you redirect them.

## The `[storage]` block

Storage settings are a property of the **workspace**, not of an individual
pipeline, so they live in `clinker.toml` at the workspace root rather than in
the per-pipeline YAML:

```toml
[storage.spill]
dir = "/var/clinker/spill"   # optional; default = OS temp dir
disk_cap_bytes = "10GB"      # optional; default = unlimited
compress = "auto"            # optional; auto | off | on   (default = auto)

[storage.staging]
enabled  = false             # opt-in; default off
dir      = "/var/clinker/staging"   # required when enabled
patterns = ["/mnt/nfs/data/**"]     # which sources to stage
```

The whole block is optional. With no `clinker.toml`, or a `clinker.toml` that
omits `[storage]`, Clinker spills to the OS temp directory exactly as it
always has.

## `storage.spill.dir` — where spill files go

When `dir` is set, the per-run spill directory (`clinker-spill-<random>/`) is
created under that path, and every blocking operator writes its spill files
there. When `dir` is omitted, the per-run directory is created under the OS
temp directory ([`std::env::temp_dir`], typically `$TMPDIR` or `/tmp`).

The directory is **validated once at startup**, before any input is read. If
the path does not exist, is a file, or is not writable, the run fails
immediately with a diagnostic naming the setting:

```
storage.spill.dir /var/clinker/spill does not exist; create it or point at an existing volume
```

Validating up front — rather than at the first spill — means a misconfigured
spill volume fails fast, while the run is cheap to abandon, instead of after
minutes of work. (This is the trap DuckDB fell into when its temp-directory
setting was honored only lazily, [duckdb/duckdb#9401].)

### Why redirect spill off `/tmp`

On many Linux hosts — especially systemd-managed ones — `/tmp` is mounted as
**tmpfs**, which is backed by RAM (and swap), not disk. Spilling there does
not actually free physical memory: the spill bytes stay resident, defeating
the whole point of the memory budget. If `df -T /tmp` reports a `tmpfs`
filesystem, point `storage.spill.dir` at a path on a real block device so
spilling moves pressure off RAM and onto disk.

### Inspecting the resolved spill root

`clinker run --explain` prints the resolved spill root and where it came from,
so you can confirm the setting took effect before committing to a run:

```text
Spill root: /var/clinker/spill [storage.spill.dir]
```

…or, with no configuration:

```text
Spill root: /tmp [OS temp dir (default)]
```

The same `--explain` output reports the resolved disk cap on the next line:

```text
Spill disk cap: 10737418240 bytes [storage.spill.disk_cap_bytes]
```

…or, with no cap configured:

```text
Spill disk cap: unlimited (default)
```

Finally, `--explain` reports the resolved compression decision per blocking
operator, so you can see which spills will be LZ4-framed (`lz4`) and which will
be written raw (`off`) before the run starts. Under `auto` the choice varies by
operator width:

```text
Spill compression: Auto [storage.spill.compress]
  Aggregate 'totals' → lz4
  Sort 'by_amount' → off
```

## `storage.spill.disk_cap_bytes` — cap cumulative spill

By default a run will spill as much as it needs, limited only by the physical
space on the spill volume. `disk_cap_bytes` sets a **cumulative budget**: the
total on-disk size of every spill file a run writes. When that running total
would cross the cap, the run aborts with a dedicated diagnostic instead of
continuing to fill the volume.

```toml
[storage.spill]
dir = "/mnt/fast-ssd/clinker-spill"
disk_cap_bytes = "50GB"
```

The value accepts the same human-readable byte-size grammar as the source
size filters — a bare integer is bytes, and `KB`/`MB`/`GB` suffixes use
decimal units (`1GB` = 1,000,000,000 bytes), matching `du`, `df`, and the AWS
CLI. Omitting the key leaves spill unlimited, exactly as before.

The cap is a **policy ceiling**, deliberately independent of both the memory
budget and the physical volume size. A run can sit well inside its
`memory.limit` and still exhaust local disk through an unbounded stream of
spill files; the cap lets an operator bound that on a shared volume. It is the
guard DataFusion shipped without ([apache/datafusion#15358]) until production
runs filled volumes.

## `storage.spill.compress` — LZ4 compression policy

Spill files are postcard-encoded record streams. By default each stream is
wrapped in an LZ4 frame, which shrinks large spilled runs. But LZ4 carries a
**per-frame fixed cost** — clearing the compressor's internal state on every
frame reset — and on small spills that cost can outweigh the byte savings. The
LZ4 v1.8.2 release notes call this out directly, and Pentaho Kettle ships
explicit guidance to turn spill compression **off** for small rows.

`compress` controls the policy:

```toml
[storage.spill]
compress = "auto"   # auto | off | on   (default = auto)
```

| Mode | Behavior |
| --- | --- |
| `auto` (default) | Compress only when a spilled batch is projected large enough to amortize LZ4's per-frame cost — both **≥ 4 KiB** and **≥ 1024 rows**. Below either threshold the batch is written raw. The projection comes from the operator's schema width and the run's `batch_size`, so the decision is made per blocking operator. |
| `off` | Never compress. Postcard records are written straight to disk with no LZ4 frame. Cheapest for small spills; largest on-disk size. |
| `on` | Always compress with an LZ4 frame. The pre-knob behavior, best for spills of large, compressible rows. |

Each spill file records its compression choice in a one-byte header tag, so the
read path always dispatches to the right decoder regardless of the mode the
file was written with — changing the knob between runs never breaks re-reading
an earlier run's files.

The 4 KiB / 1024-row thresholds mark the empirical crossover: below them the
LZ4 frame's fixed cost dominates the small amount of compressible payload, and
writing raw is faster end-to-end (the `spill_compression` benchmark sweeps
batch sizes from 256 B to 64 KiB and confirms `auto` tracks the faster of
`on` / `off` across the range). Most pipelines should leave `compress` at
`auto`; set `on` when spilling wide, highly compressible rows to a
space-constrained volume, and `off` when spills are dominated by many small
batches.

## Distinguishing the four spill-related failure conditions

Spill-related aborts are split into four distinct diagnostics so a single
glance at the error tells you exactly what to fix — instead of every disk and
memory problem rendering as one ambiguous "out of memory" message (the trap
DuckDB hit in [duckdb/duckdb#14142], where a temp-dir cap was reported as
"Out of Memory Error … 187.3 GiB/187.3 GiB used" and users inspected `df`
only to find free space).

| Condition | Code | What happened | What to do |
| --- | --- | --- | --- |
| **Out of memory** | E310 | An operator's in-RAM state crossed the hard `memory.limit` (a true RSS overrun). | Raise `memory.limit`, reduce input, or let the operator spill. |
| **Spill cap exceeded** | E320 | Cumulative spill bytes crossed `storage.spill.disk_cap_bytes`. The volume may still have free space — you hit the configured budget. | Raise `disk_cap_bytes`, point `storage.spill.dir` at a larger volume, or reduce the spill footprint. |
| **Spill volume full** | E321 | The OS reported the spill volume out of space (`ENOSPC`). The physical disk filled. | Free space on the volume, or move `storage.spill.dir` to a larger mount. |
| **Spill directory unavailable** | (Spill) | The spill directory went bad mid-run — unmounted, remounted read-only, deleted by a cleaner, or permissions revoked. | Remount/restore the volume; stop the over-eager cleaner. |

The key separations:

- **E310 vs E320** — an OOM is an in-RAM overrun; a cap-exceeded is a
  disk-budget stop. A run can hit E320 while comfortably inside its memory
  envelope, so conflating the two would point you at the wrong knob.
- **E320 vs E321** — E320 is the budget *you* set; E321 is the disk itself
  running dry. If you removed `disk_cap_bytes`, an over-large run would no
  longer trip E320 and would instead spill until the volume filled (E321).

(A future per-operator memory-reservation surface will add a fifth,
reservation-exhausted condition; it is not part of the engine yet.)

## Startup storage validation

Before a run spawns its first source-ingest thread — after the plan compiles
but before any input is read or any byte is spilled or staged — Clinker runs a
single comprehensive validation pass over the resolved `[storage]`
configuration. It rejects configurations that are physically wrong for the job,
each with a stable diagnostic code, the offending `clinker.toml` field, and a
`clinker explain --code <CODE>` pointer. Validating up front fails a
misconfigured volume while the run is still cheap to abandon, rather than after
minutes of work when the first spill or staged copy hits the bad volume.

| Code | Rejected configuration | Why |
| --- | --- | --- |
| **E330** | `storage.spill.dir` on an in-memory filesystem (Linux tmpfs / ramfs, Windows RAM disk). | Spilling there keeps the bytes in RAM, so it frees no physical memory and defeats the memory budget. |
| **E331** | `storage.spill.dir` on a network filesystem (NFS / SMB / CIFS / FUSE). | A spill target on a soft-mounted share risks silent truncation and mmap data loss — the failure modes spill exists to avoid. |
| **E332** | `storage.staging.dir` on a network filesystem. | Staging copies inputs *off* a flaky share; a staging dir that is itself on a share reintroduces the fragility staging exists to escape. |
| **E333** | `storage.staging.dir` on the same physical device as a matched (staged) source. | The copy moves no I/O off the source volume, so it buys nothing while still spending time and space. Applies only to **matched** sources. |
| **E334** | `storage.spill.dir` equal to `storage.staging.dir`. | Spill files and staged source copies are sized and cleaned up differently; sharing one directory makes accounting and cleanup ambiguous. |

The filesystem-class checks (E330–E332) read the volume type through one
cross-platform detection layer, so they behave identically on Linux, macOS,
and Windows: Linux matches the `statfs` `f_type` magic, macOS matches the
`f_fstypename` string, and Windows maps `GetDriveTypeW`. (macOS has no native
tmpfs, so E330 only ever fires on Linux and Windows.) The same-device check
(E333) compares the device id on Linux/macOS and the volume serial number on
Windows — the very same probe the staging same-volume rule uses, so there is
one consistent notion of "same device" across the whole run.

### Free-space preflight

Separately from the runtime disk cap (E320) and the full-volume surface
(E321), the startup pass runs a **free-space preflight**: it queries the bytes
available on the spill volume and compares them to the run's estimated spill
footprint (the sum of every blocking operator's predicted peak state, the same
estimate `--explain` surfaces). When the spill volume looks too small, the run
prints a **warning** and continues:

```
W330: spill volume /var/clinker/spill has 2000000000 bytes free but the run is
estimated to spill up to 8000000000 bytes; the run may abort with a full-volume
error (E321) at the final spill — point storage.spill.dir at a larger volume or
reduce the spill footprint (raise memory.limit, partition the input)
```

This is **advisory, not fatal**: the estimate is a coarse upper bound (it
ignores spill compression and the streaming drain), so the run may well finish
within the available space. The warning exists so a long pipeline that *would*
die at its final spill surfaces that risk before it runs for an hour, rather
than after. The free-space query uses a cross-platform probe (statvfs on
Unix, `GetDiskFreeSpaceExW` on Windows) that returns a 64-bit byte count, so
the historical 32-bit `f_bavail` truncation never affects the comparison.

## Mid-run spill failures

The startup check guarantees the spill directory is writable when the run
begins, but it can still go bad **mid-run** — an NFS share remounts read-only,
a volume unmounts, an over-eager temp-file cleaner deletes the directory, or
permissions are revoked. When a spill write fails because the directory has
vanished or become read-only, the run aborts cleanly with a distinct
diagnostic rather than a generic I/O error or a panic:

```
spill directory /var/clinker/spill became unavailable mid-run: No such file or directory
(the directory may have been unmounted, remounted read-only, deleted by an
external cleaner, or had its permissions revoked)
```

This surfaces the directory-level cause directly, so the fix (remount the
volume, stop the cleaner, restore permissions) is obvious from the message.

## Crash purge of orphaned spill directories

A run's spill directory (`clinker-spill-<random>/`) is normally removed when the
run ends — a clean exit, or even a panic that unwinds, deletes it. But a
`SIGKILL`, the Linux OOM-killer, or a power loss kills the process before that
cleanup runs, leaking the directory and every spill file inside it under the
spill root. Over many crashed runs that fills the spill volume.

To prevent that, **a run reaps orphaned spill directories at startup — but
only when a spill directory is explicitly configured** (`storage.spill.dir`),
before it creates its own. Each live run holds an operating-system advisory lock
on a `.lock` file inside its spill directory for the run's whole lifetime; the
OS releases that lock automatically when the process exits, however it exits. At
startup a run scans the configured spill root and, for each `clinker-spill-*`
directory, tries to take that lock: if it succeeds the owning process is gone, so
the directory is an orphan and is removed; if the lock is still held a concurrent
live run owns it, so it is left alone. Asking the kernel "is anyone still
holding this?" is robust against PID reuse and never reaps a directory a
concurrent run is still using. The purge is best-effort: a failure to reap one
directory is logged and the run proceeds.

When `storage.spill.dir` is **not** set, the spill root defaults to the OS temp
directory ([`std::env::temp_dir`], typically `$TMPDIR` or `/tmp`), and **no
startup purge runs** there. In the default case a run cleans up after itself
directly: the per-run spill directory is a `tempfile::TempDir` that is removed on
every normal exit — a clean exit or a panic that unwinds. Only a `SIGKILL`, the
OOM-killer, or a power loss leaks one, and a directory leaked into the OS temp
directory is the operating system's temp-reaper's responsibility to clean up, not
Clinker's.

The purge is deliberately confined to a configured spill root because it must
never police the shared OS temp directory. That directory is used by every
process on the host, so a startup sweep there would race not only concurrent
Clinker runs (the lock-based check narrows that window but cannot eliminate it
against a peer whose just-created spill directory is not yet locked) but also
unrelated programs that happen to use a colliding name prefix. A run owns its
configured spill volume and can safely sweep it; it does not own `/tmp` and so
leaves it alone.

## `storage.staging` — opt-in source staging

Reading source files directly from a network share (NFS, SMB) couples every
run to the share's availability and quirks: a soft-mount can silently truncate
a read, and latency multiplies across many small files. **Source staging**
copies matched source files to a local volume before the pipeline reads them,
so the run works from stable local copies. It is **off by default** and
activated per workspace by pattern match — pipelines that don't opt in behave
exactly as before.

```toml
[storage.staging]
enabled        = true
dir            = "/var/clinker/staging"   # required when enabled
patterns       = [
    "/mnt/nfs/data/**",
    "//fileserver/share/**",
]
disk_cap_bytes = "50GB"   # optional; cap on bytes copied per run (default unlimited)
verify         = "blake3" # optional; blake3 | none   (default blake3)
on_existing    = "overwrite" # optional; overwrite | reuse | error (default overwrite)
cleanup        = "on_success" # optional; on_success | always | never (default on_success)
```

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Master switch. When `false`, `patterns` is ignored and every source reads in place. |
| `dir` | — | Local directory the copies are written under. **Required** when `enabled`. |
| `patterns` | `[]` | Glob patterns selecting which source paths to stage. A source is staged only when `enabled` and its path matches at least one pattern. Empty ⇒ nothing is staged. |
| `disk_cap_bytes` | unlimited | Cumulative cap on bytes copied per run. Same byte-size grammar as the spill cap (`"50GB"`, bare integers are bytes). |
| `verify` | `blake3` | Post-copy integrity check. `blake3` hashes source and copy and requires a match — the only check that catches a soft-mount's silent truncation. `none` skips the check. |
| `on_existing` | `overwrite` | What to do when a staged copy of this source already exists from a prior run: `overwrite` re-copies unconditionally; `reuse` reuses the existing copy **only when it is still fresh** (the source's modification time and size match what was recorded when it was staged), otherwise re-copies; `error` fails the run rather than touch the existing copy. See [The staging cache](#the-staging-cache-on_existing) below. |
| `cleanup` | `on_success` | When staged copies are deleted relative to the run's outcome: `on_success` removes them after a clean exit but **keeps** them after a failure so the operator can inspect the exact inputs the failed run saw; `always` removes them regardless; `never` keeps them as a persistent reuse cache for a later `reuse` run. See [Cleanup](#cleanup-on_success--always--never). |

### Pattern matching

`patterns` uses the same glob grammar as a source's `exclude:` list. Each
pattern is tested against both the **full path** and the **basename**, so
`/mnt/nfs/**` matches a deep path by its full path while `*.csv` matches any
CSV by basename. `**` crosses directory boundaries; `*` does not.

### Startup validation

When `enabled`, staging is **validated once at startup**, before any input is
opened, so a misconfiguration fails the run immediately rather than at the
first copy. The run is refused when:

- `dir` is unset.
- `dir` does not exist, is a file, or is not writable (probed with a real
  create-and-delete, so a read-only mount or restrictive ACL is caught).
- a `patterns` entry is not a valid glob.
- `dir` sits on the **same volume** as a matched source. Staging within one
  volume copies bytes without moving I/O off the slow share — a
  well-documented anti-pattern — so it is refused up front rather than left to
  surface as a confusingly slow pipeline. The check compares the source's and
  the staging dir's storage volume (the device id on Linux/macOS, the volume
  mount root on Windows); point `dir` at a local disk on a different volume.

The same-volume rule applies only to **matched** sources: a source the
patterns don't select reads in place, so its volume is irrelevant.

### How a file is staged

Each matched source maps to a **stable, content-addressed** pair of files
directly under `dir`, deterministic across runs of the same source:

- `<source-id>.staged` — the local copy the reader opens.
- `<source-id>.manifest.json` — a sidecar recording the source's identity:
  its path, modification time, size, the BLAKE3 content hash, and the stage
  time.

`<source-id>` is derived from the source's canonical path, so the same source
always resolves to the same staged file. That stability is what makes the
`reuse` cache work (a later run can find the prior copy) and it is why the layout
is stable rather than per-run UUIDs.

The copy is built to survive a crash at any point without leaving a corrupt or
partial file a later run might trust:

1. **Single-pass copy + hash.** The source is read once in ~1 MiB chunks; each
   chunk is fed to both the BLAKE3 hasher and the destination file in the same
   pass. The copy never holds the whole file in memory, so it stays a
   memory-budget no-op regardless of file size.
2. **Atomic publish.** Bytes are written to a `<source-id>.<run>.partial` temp
   file, flushed and `fsync`'d, then **renamed** to `<source-id>.staged`. A
   rename is an atomic replace on Linux, macOS, and Windows (Windows 10 1607+),
   so a reader scanning for `.staged` files sees either nothing or the complete
   file — never a half-written one. The `<run>` segment in the partial name lets
   two concurrent invocations stage the same source without colliding on the
   in-flight bytes.
3. **Durable rename.** On Linux/macOS the parent directory is `fsync`'d after
   the rename, because on ext4/xfs a rename is only crash-durable once the
   directory entry itself is flushed. On Windows the NTFS journal makes the
   rename durable, so there is no separate directory flush to do.
4. **Verify.** With `verify = blake3` (the default) the source is independently
   re-read and hashed, and the two digests must match. A size check cannot
   catch a soft-mount that silently truncated the read; two content digests
   can. A mismatch removes the published copy and fails the run with a distinct
   "staged copy is corrupt" diagnostic — not a generic I/O error.
5. **Commit the manifest.** The identity manifest is written with the same
   atomic temp-file + rename discipline. **The manifest is the commit marker:**
   a `.staged` file is only trustworthy once its manifest exists. A crash
   between the copy and the manifest leaves a `.staged` with no manifest, which
   the next run's [crash purge](#crash-purge-of-orphaned-artifacts) reaps as an
   orphan rather than half-trusting.

If the copy fails partway, the `.partial` is removed before the error
propagates.

### The staging cache (`on_existing`)

Because staged copies live at stable paths, a copy from a prior run is still on
disk when the next run starts (unless `cleanup` removed it). `on_existing`
decides what happens when that prior copy is found:

| Mode | Behavior |
| --- | --- |
| `overwrite` (default) | Always re-stage. The prior copy and its manifest are removed and the source is copied fresh. The safe default: a copy from a crashed run must not be trusted. |
| `reuse` | Reuse the prior copy **only when it is still fresh** — the source's current modification time and size both match what the manifest recorded. A fresh match **skips the copy entirely** (no bytes read off the share, nothing charged against the disk cap). A changed mtime or size means the source was rewritten, so the copy is stale and is re-staged. |
| `error` | Fail the run with a clear diagnostic if a staged copy already exists, rather than overwrite or reuse it. For workflows that want an explicit "the cache is already populated" stop. |

`reuse` is the mode that turns staging into a cache: re-running the same
pipeline over an unchanged network share copies nothing on the second run. The
freshness check is mtime + size, not a re-hash, so it is a cheap `stat` rather
than a full read of the source.

> Within a single run, `reuse` is correct for one invocation at a time. Running
> several `clinker` invocations that stage the *same* shared source
> concurrently is a separate concern addressed by a follow-up
> concurrency-safety layer; the stable layout and manifest here are designed so
> that layer can add an atomic-publish lock on top without changing the cache
> shape.

### Cleanup (`on_success` | `always` | `never`)

`cleanup` decides when a run's staged copies are removed, keyed on the run's
outcome:

| Mode | Behavior |
| --- | --- |
| `on_success` (default) | Remove the copies after a **clean** exit; **keep** them after a failure (or an interrupted / DLQ-producing run) so the operator can inspect the exact inputs the run saw and re-run without re-fetching. |
| `always` | Remove the copies when the run ends, success or failure. |
| `never` | Keep the copies indefinitely as a persistent reuse cache. Combine with `on_existing = reuse` to make repeated runs over a stable source copy-free. The operator reclaims the staging dir manually (or lets the next run's crash purge eventually reap stale entries). |

Each staged file's manifest is removed alongside it, so cleanup never leaves a
manifest pointing at a staged file that is gone.

### Crash purge of orphaned artifacts

A clean (or panicking) run runs its `cleanup`. But a `SIGKILL`, the Linux
OOM-killer, or a power loss kills the process before any cleanup runs, leaking
its staged artifacts under the staging root. To stop that from accumulating
across crashes, **every run performs an idempotent crash purge at startup**,
before it stages anything. It reaps the artifact shapes a crash leaves behind:

- a `*.partial` — an interrupted copy;
- a `*.staged` with no matching manifest — a copy that crashed before it could
  commit its manifest;
- a `*.manifest.json` with no matching staged file.

A clean pair (a `.staged` with its committed `.manifest.json`) is the reuse
cache and is **kept** — the purge never removes a complete, trustworthy copy.
The classification is by artifact shape alone, so it is safe to run while
another invocation is staging (an in-flight copy is a `.partial`, which belongs
to the writing run and is reclaimed by it, not by a concurrent purge of
committed artifacts).

### File permissions

Staged copies hold verbatim source records — potentially PII, credentials, or
financial data — and on a shared staging volume they must not be readable by
other users. On Unix each staged file and its manifest are created with mode
`0o600` (owner-only). On Windows there is no portable mode bit; staged files
inherit the staging directory's ACL, so restrict the directory's ACL if the
volume is shared.

### Crash durability and the parent-directory fsync

The atomic-rename guarantee only holds across a crash if the rename is durable.
On POSIX filesystems (ext4, xfs) a rename's directory entry can still be in the
page cache after `rename` returns, so Clinker `fsync`s the parent directory
after the rename. Windows is intentionally exempt: the NTFS metadata journal
already makes the rename crash-durable (the semantics `MOVEFILE_WRITE_THROUGH`
requests), and Windows offers no directory-fsync equivalent.

[`std::env::temp_dir`]: https://doc.rust-lang.org/std/env/fn.temp_dir.html
[duckdb/duckdb#9401]: https://github.com/duckdb/duckdb/issues/9401
[duckdb/duckdb#14142]: https://github.com/duckdb/duckdb/issues/14142
[apache/datafusion#15358]: https://github.com/apache/datafusion/issues/15358
