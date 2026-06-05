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
