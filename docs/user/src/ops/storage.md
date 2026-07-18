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

Finally, `--explain` reports the resolved compression decision per
spill-writing operator, so you can see which spills will be LZ4-framed (`lz4`)
and which will be written raw (`off`) before the run starts. Under `auto` the
choice varies by operator width:

```text
Spill compression: Auto [storage.spill.compress]
  Aggregate 'totals' → lz4
  Sort 'by_amount' → off
```

Only operators that actually write spill files appear here: the external sort,
the hash Aggregate, the grace-hash / sort-merge Combine, and the pure-range
(block-band) IEJoin Combine, which external-sorts each side and writes its
min/max-tagged blocks to disk and spills its matched-output sort runs the same
way. The remaining in-memory join strategies — the
inline hash build/probe and the equi+range IEJoin (hash-partitioned range
join) — run their kernel entirely in RAM and never open a spill file, so spill
compression does not apply to them and they are omitted from this list, even
though they carry a spill priority for memory arbitration.

## `storage.spill.disk_cap_bytes` — cap concurrent spill

By default a run will spill as much as it needs, limited only by the physical
space on the spill volume. `disk_cap_bytes` sets a **budget on the spill the run
holds at once**: the on-disk size of the spill files live at any moment. When
that footprint would cross the cap, the run aborts with a dedicated diagnostic
instead of continuing to fill the volume. Because the cap tracks what is
concurrently on disk, an operator that deletes intermediate spill files as it
consumes them (such as the merge that folds a heavily fragmented external sort
back together) does not count those transient files twice — only the disk a run
actually occupies at once is charged against the cap.

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

## Observability — what the planner will do before you run

`clinker run --explain` is plan-only (it reads no input and spills nothing), so
it is the safe place to see what a run *would* do to the spill volume and to the
staging dir before committing to it. On top of the resolved spill root, disk
cap, and compression decision documented above, `--explain` surfaces three
storage-observability sections, and a real `clinker run` reports the matching
*actuals* at end-of-run so you can calibrate the estimate.

**A note on byte units.** Three different unit conventions appear across the
storage surface, and it helps to know which is which before comparing figures:

- **Config values you write** (`disk_cap_bytes = "10GB"`) use **decimal** units
  — `1GB` = 1,000,000,000 bytes — matching `du`, `df`, and the AWS CLI (see
  [the disk-cap grammar](#storagespilldisk_cap_bytes--cap-cumulative-spill)).
- **The `=== Estimated Spill Volume ===` section** humanizes with **binary**
  suffixes — `K`/`M`/`G` = KiB/MiB/GiB — so it lines up with the
  `predicted_peak` figure on each stage's Physical Properties line, which uses
  the same humanizer.
- **The cap-headroom line and the post-run actuals** print **raw bytes** with no
  suffix, so the cap-minus-estimate subtraction and the estimate-vs-actual
  comparison are exact rather than rounded.

When you calibrate the estimate against the post-run actual, convert the binary
estimate suffix to bytes first (`1K` = 1024 bytes, `1M` = 1,048,576 bytes) so you
are comparing the same unit the actuals report.

### Estimated spill volume per stage

The `=== Estimated Spill Volume ===` section lists one line per spill-writing
stage (hash Aggregate, external sort, grace-hash / sort-merge Combine, and the
pure-range block-band IEJoin Combine) with its plan-time spill-volume estimate,
followed by a total. The remaining in-memory join strategies (inline hash
build/probe, equi+range IEJoin) never write spill files, so they do not appear
here and do not inflate the total:

```text
=== Estimated Spill Volume ===

Estimated spill volume (per blocking stage):
  [aggregation:hash] dept_totals → 1K
  [sort] by_amount → 4K
  Total: 5K
```

Each figure is the operator's coarse predicted peak live state — the same
`predicted_peak` the Physical Properties arbitration line shows — and bytes
render in binary units (`K`/`M`/`G` = KiB/MiB/GiB). Summing rather than maxing is
the conservative choice for a preflight: two blocking operators can be live and
spilled at the same time, so their footprints add.

A streaming-only pipeline (no blocking operator) has nothing that spills, so the
section is omitted entirely.

**Unknown stages.** The estimate is seeded from input file sizes resolved at
plan time. A stage whose volume cannot be known before the run renders
`unknown` instead of a misleading `0B`, and the total notes that unknown stages
are excluded:

```text
  [aggregation:hash] dept_totals → unknown
  Total (known stages): 0B (excludes stages whose volume is unknown at plan time
  — a network source, a missing or unreadable input, or a glob/regex matcher
  whose discovery fails)
```

The seed is known for every file-backed matcher whose files can be sized at
plan time: a single-file `path:` source, an explicit `paths:` list, and a
`glob:` or `regex:` matcher. A glob/regex seed runs the same discovery resolver
the run uses — applying its `exclude`, `min_size`/`max_size`,
`modified_after`/`before`, `take`, and sort filters — and sums the matched
files' sizes, so the estimate names exactly the bytes the run will read with no
second implementation to drift. A glob/regex that matches nothing seeds zero
(rendered as `unknown`, since there is no spill volume to preview). The seed is
genuinely **unknown** for a network source, for a missing or unreadable input
file, and for a glob/regex matcher whose discovery itself fails (an invalid
pattern, or no match under `on_no_match: error`) — the run surfaces the same
error at startup. Check the post-run actuals below to calibrate any estimate.

### Staging plan per source

When `storage.staging` is enabled, the `=== Staging Plan ===` section reports,
for each source (and each discovered file under a multi-file matcher): whether
it would be staged, the resolved content-addressed staged path, and — under
`on_existing = reuse` — the reuse-if-fresh cache decision (`hit` if a committed
prior copy still matches the live source, `miss` if it would be re-staged):

```text
=== Staging Plan ===

Source 'orders':
  /data/in/orders-2024.csv → staged: yes, path: /mnt/local/staging/3f2a…b1.staged, reuse: hit
  /data/in/orders-2025.csv → staged: yes, path: /mnt/local/staging/9c4e…07.staged, reuse: miss
```

The reuse prediction runs the exact freshness check (mtime + size against the
committed manifest) the real run makes, read-only — `--explain` copies nothing.
A source that matches no staging pattern reports
`staged: no (no pattern match, reads in place)`; a network source reports
`not stagable (network source reads in place)`. When staging is disabled the
section states that every source reads in place.

### Cap headroom

When a spill cap is configured, `--explain` reports the headroom (cap minus
estimate) with the same per-invocation disclaimer the startup
[cap-headroom preflight](#cap-headroom-preflight) carries, and the same 80%
warning:

```text
Cap headroom: 5000000000 bytes free (5000000000 estimated of 10000000000 cap, 50%)
  [per invocation — does NOT account for sibling invocations sharing the spill
  volume under partition-and-run]
```

### Machine-readable form — `--explain json`

`clinker run --explain json` emits the whole plan as JSON for tooling (the
canvas, dashboards, CI gates). The same storage observability the text form
prints lives under a structured `storage_summary` object, so a consumer reads
per-stage spill estimates and the cap / staging summary without re-parsing
prose:

```json
{
  "schema_version": "1",
  "nodes": [ ... ],
  "node_properties": { ... },
  "storage_summary": {
    "spill_root": { "path": "/mnt/fast-ssd/clinker-spill", "source": "storage.spill.dir" },
    "spill_disk_cap_bytes": 1000000000,
    "estimated_spill": {
      "per_stage": [
        { "node_name": "dept_totals", "display_name": "[aggregation:hash] dept_totals", "estimate_bytes": 1024 },
        { "node_name": "by_amount", "display_name": "[sort] by_amount", "estimate_bytes": 4096 }
      ],
      "total_known_bytes": 5120,
      "any_unknown": false
    },
    "spill_compression": {
      "mode": "auto",
      "per_operator": [
        { "node_name": "dept_totals", "display_name": "[aggregation:hash] dept_totals", "compression": "lz4" },
        { "node_name": "by_amount", "display_name": "[sort] by_amount", "compression": "off" }
      ]
    },
    "cap_headroom": {
      "headroom_bytes": 999994880,
      "estimated_bytes": 5120,
      "cap_bytes": 1000000000,
      "pct_of_cap": 0.000512,
      "over_threshold": false
    },
    "staging": { "enabled": false, "sources": [] }
  }
}
```

The fields mirror the text sections one-for-one: `estimated_spill` is the
`=== Estimated Spill Volume ===` section (a stage whose volume is unknown at
plan time carries `estimate_bytes: null` and sets `any_unknown: true`),
`spill_compression` is the `Spill compression:` projection, `cap_headroom` is
the cap-headroom line (omitted when no cap is configured or the estimate is
zero), and `staging` is the `=== Staging Plan ===` section. The JSON and DOT
formats emit only their machine payload — the human-readable
`=== Resolved Outputs ===` preamble the text form prints is suppressed so the
output parses cleanly.

### Post-run actuals — calibrating the estimate

A real `clinker run` that spills prints a per-stage **actual** spill-volume
section at end-of-run, so you can compare it against the `--explain` estimate
for the same stage — the calibration loop that turns a coarse pre-run estimate
into a trustworthy one over repeated runs:

```text
=== Spill Volume (actual, per stage) ===
  dept_totals → 1048576 bytes
  by_amount → 4194304 bytes
  Total: 5242880 bytes (compare against the --explain estimate)
```

The per-stage breakdown sums to the pipeline-wide cumulative spill total. A run
that stayed within memory spilled nothing and prints no section. A large
estimate-vs-actual delta is the single highest-leverage signal when a pipeline
starts spilling unexpectedly (the failure mode behind Polars' documented 13.5×
spill amplification, where an optimizer interaction turned 30 GB of input into
400 GB of spill with no per-stage visibility).

> **Note on the `--explain` compression projection.** The per-operator
> spill-compression decision shown under `Spill compression:` is projected from
> the same column count the operator's runtime spill writer sees, so the
> projected `auto` verdict matches the file the run actually writes. A hash
> Aggregate and a grace-hash / sort-merge Combine project against their output
> schema (engine-stamped identity columns included), exactly the width their
> dispatch arms resolve compression against; an enforcer sort projects against
> the width of the records flowing into it — its upstream's emitted schema —
> which is the width its sort buffer reads at runtime. The read path also
> dispatches on each spill file's own one-byte header tag, so re-reading is
> robust regardless.

## Distinguishing the runtime storage-abort conditions

A run that fails while spilling or staging emits one of several distinct
diagnostics so a single glance at the error tells you exactly what to fix —
instead of every disk and memory problem rendering as one ambiguous "out of
memory" message (the trap DuckDB hit in [duckdb/duckdb#14142], where a temp-dir
cap was reported as "Out of Memory Error … 187.3 GiB/187.3 GiB used" and users
inspected `df` only to find free space). The aborts split along two axes: the
**spill** side (in-memory operator state landing on disk) and the **staging**
side (matched source files copied to local disk before they are read).

### Spill aborts

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

### Staging-copy aborts

When `storage.staging` is enabled, copying a matched source to local disk can
fail in three distinct ways. Like the spill split, each has its own code so a
content-corruption problem never renders as a budget problem and vice versa.
Staging runs before any record flows, so these surface as startup-style
validation failures.

| Condition | Code | What happened | What to do |
| --- | --- | --- | --- |
| **Staged copy corrupt** | E335 | The local copy's BLAKE3 digest did not match the source — the transport (e.g. a soft-mount NFS share) delivered different bytes than the source holds. | Re-run over a healthy transport, harden the mount, or stage from a stable snapshot. Do not set `verify = "none"` to silence it — that hides corruption, not fixes it. |
| **Staging cap exceeded** | E336 | The cumulative bytes staged this run would cross `storage.staging.disk_cap_bytes`. The volume may still have free space — you hit the configured budget, **not** a full disk. | Raise `disk_cap_bytes`, point `storage.staging.dir` at a larger volume, narrow `storage.staging.patterns`, or remove the cap. |
| **Staged copy already exists** | E337 | A staged copy of this source already exists and `on_existing = error` refuses to touch it. | Remove the existing copy, or switch `on_existing` to `overwrite` (re-stage) or `reuse` (reuse a fresh copy). |

The same cap-vs-full-disk separation applies here as on the spill side: **E336**
is the budget you set (mirroring E320), so it must not render as an
out-of-space message — a physically full staging volume instead surfaces as a
staging I/O error (mirroring E321). **E335** is distinct from a generic staging
I/O error: an I/O error means the OS reported a fault, whereas E335 means the
copy completed cleanly yet still does not match the source.

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

### Cap-headroom preflight

When `storage.spill.disk_cap_bytes` is configured, the same startup pass also
runs a **cap-headroom preflight**: it compares the run's estimated spill volume
to the configured cap and warns when the estimate reaches **80% of the cap**.
Unlike the free-space preflight (which probes the physical volume), this checks
the run against the *policy* ceiling you set, so it fires even on a volume with
plenty of free space:

```
W331: this run is estimated to spill up to 9000000000 bytes, which is 90% of the
configured spill cap storage.spill.disk_cap_bytes (10000000000 bytes); the run
may abort with a spill-cap error (E320) before it finishes — raise disk_cap_bytes
or reduce the spill footprint (raise memory.limit, partition the input). This
headroom is per invocation: if you partition the input and run several clinker
invocations against the same spill volume and cap, they share the cap, so the
real headroom is smaller than this figure
```

Like W330, this is **advisory, not fatal** — the estimate is a coarse upper
bound, so a run that compresses well or never trips its memory budget may finish
comfortably under the cap. It fires on a normal `clinker run` (before ingestion,
at startup), not only under `--explain`, so an operator sees the signal on the
real run even when they did not explicitly inspect the plan first.

**Per-invocation accounting.** The cap and the headroom figure are scoped to a
single `clinker` invocation. Under the [partition-and-run](#) model — where you
split a large input by file or key and launch several `clinker` processes that
share one spill volume and one `disk_cap_bytes` — the physical spill volume is
shared by every sibling, so the real headroom is smaller than any one
invocation's figure. The warning text states this explicitly rather than
silently presenting a per-invocation number as a whole-volume guarantee. Clinker
is single-process by design (one invocation = one OS process), so the engine
cannot see its siblings; the disclaimer is the honest stance.

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
run ends — a clean exit, a run that aborts with a fatal error, or even a panic
all delete it. But a `SIGKILL`, the Linux OOM-killer, or a power loss kills the
process before that cleanup runs, leaking the directory and every spill file
inside it. Over many crashed runs that fills the spill volume.

To prevent that, **a run cleans up orphaned spill directories at startup — but
only when a spill directory is explicitly configured** (`storage.spill.dir`),
before it creates its own. It removes only directories left by dead runs and
never touches one a concurrent run is still using.

When `storage.spill.dir` is **not** set, the spill root defaults to the OS temp
directory ([`std::env::temp_dir`], typically `$TMPDIR` or `/tmp`), and **no
startup purge runs** there. In the default case a run still cleans up its own
spill directory on every exit short of a hard kill; a directory leaked into the
OS temp directory by a hard kill is the operating system's temp-reaper's
responsibility, not Clinker's. The purge is confined to a configured spill root
because Clinker owns that volume but does not own the shared OS temp directory.

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

Staging copies the matched source to your local staging directory once, then
verifies the copy against the source (with `verify = blake3`, the default, a
content mismatch fails the run with [E335](#staging-copy-aborts)). From then on
the pipeline reads from the local copy. The same source always resolves to the
same staged file, so a later run can find and `reuse` a prior copy.

### The staging cache (`on_existing`)

Because staged copies live at stable paths, a copy from a prior run is still on
disk when the next run starts (unless `cleanup` removed it). `on_existing`
decides what happens when that prior copy is found:

| Mode | Behavior |
| --- | --- |
| `overwrite` (default) | Always re-stage. The prior copy is removed and the source is copied fresh. The safe default: a copy from a crashed run must not be trusted. |
| `reuse` | Reuse the prior copy **only when it is still fresh** — the source's current modification time and size both match what was recorded when it was staged. A fresh match **skips the copy entirely** (no bytes read off the share, nothing charged against the disk cap). A changed mtime or size means the source was rewritten, so the copy is stale and is re-staged. |
| `error` | Fail the run with a clear diagnostic if a staged copy already exists, rather than overwrite or reuse it. For workflows that want an explicit "the cache is already populated" stop. |

`reuse` is the mode that turns staging into a cache: re-running the same
pipeline over an unchanged network share copies nothing on the second run. The
freshness check is mtime + size, not a re-hash, so it is cheap.

Staging is safe to run from several `clinker` invocations at once over a shared
staging volume: a source is copied exactly once no matter how many runs race for
it, a run always reads a complete copy, and no run fails because a sibling was
reading, cleaning up, or re-staging the same source.

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

A `SIGKILL`, the Linux OOM-killer, or a power loss can kill a run before its
`cleanup` runs, leaving half-finished staging artifacts behind. To stop those
from accumulating, every run cleans up leftover artifacts from dead runs at
startup, before it stages anything. A complete staged copy is the reuse cache
and is always kept; only incomplete leftovers are reclaimed.

### File permissions

Staged copies hold verbatim source records — potentially PII, credentials, or
financial data — so on Unix they are created with owner-only permissions. On
Windows staged files inherit the staging directory's permissions, so restrict
the directory if the volume is shared with other users.

### Crash durability and the parent-directory fsync

Staged copies survive a crash: a later run finds a complete file or nothing at
all, never a half-written one.

[`std::env::temp_dir`]: https://doc.rust-lang.org/std/env/fn.temp_dir.html
[duckdb/duckdb#9401]: https://github.com/duckdb/duckdb/issues/9401
[duckdb/duckdb#14142]: https://github.com/duckdb/duckdb/issues/14142
[apache/datafusion#15358]: https://github.com/apache/datafusion/issues/15358
