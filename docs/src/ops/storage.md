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

[storage.staging]
enabled = false              # parsed but inactive — staging is not yet implemented
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

## `storage.staging` — not yet active

The `[storage.staging]` block parses and round-trips today but performs no
work: source-file staging (copying inputs from a network share to local disk
before execution) is not yet implemented. The block exists now so a
`clinker.toml` that pre-declares staging intent is accepted rather than
rejected as an unknown key.

[`std::env::temp_dir`]: https://doc.rust-lang.org/std/env/fn.temp_dir.html
[duckdb/duckdb#9401]: https://github.com/duckdb/duckdb/issues/9401
[duckdb/duckdb#14142]: https://github.com/duckdb/duckdb/issues/14142
[apache/datafusion#15358]: https://github.com/apache/datafusion/issues/15358
