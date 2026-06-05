# Research — How do single-host batch data engines and ETL/data-movement tools implement: (1) a configurable spill/temp directory for out-of-core blocking operators (external sort, hash-aggregate, grace-hash join) — directory selection, multi-dir round-robin, and validation that rejects unsafe targets (tmpfs/ramdisk, NFS/network filesystems, spill dir == staging dir, staging on the same physical device as its source); (2) a disk-space cap on spill volume with a clean, distinct error for 'cap exceeded' vs. underlying physical 'disk full (ENOSPC)'; (3) spill compression as auto|on|off with a small-row/row-width heuristic for when compression hurts more than it helps; (4) opt-in local STAGING of remote/network source files — copy-to-fast-local-disk with integrity verification (BLAKE3 or similar), atomic write-then-rename, per-run UUID isolation directories, an on_existing 'reuse-if-fresh' cache policy, and crash-safe idempotent cleanup/purge of orphaned staging dirs. Compare BOTH columnar SQL engines (DuckDB temp_directory/max_temp_directory_size, Polars streaming/out-of-core, Apache Spark spill dirs + spark.local.dir, ClickHouse) AND non-SQL ETL / data-movement tools (Pentaho Kettle/PDI, Vector, Benthos/Redpanda Connect, Embulk, Airbyte, rsync/rclone staging patterns). For each, what are the concrete config surfaces, the failure-mode taxonomy, the atomic-rename + fsync ordering for crash safety, and the integrity-verification approach? Clinker is single-process, single-host, bounded-memory (default 512MB RSS budget, postcard+LZ4 spill format) over finite inputs — prior art must respect that substrate (no daemon, no cluster, no distributed shuffle).

## Summary
Across both columnar SQL engines and non-SQL ETL tools, the prior art for spill/staging substrate is strikingly thin on the exact safeguards Clinker is contemplating — meaning Clinker would be doing genuinely better than most, not reinventing. On the disk-cap axis, ClickHouse is the standout: it exposes per-user and per-query compressed-byte caps via a four-level scope tree (global → user → query → purpose) and crucially throws a *distinct* error code for cap-exceeded (TOO_MANY_ROWS_OR_BYTES) versus physical disk-full (NOT_ENOUGH_SPACE) — exactly the failure-mode separation the question asks for. Spark, by contrast, models no spill cap at all and only handles directory-creation failure (via a hard System.exit), while Kettle/PDI and Vector treat compression as a plain boolean with no auto/row-width heuristic and validate temp dirs only for existence, never rejecting tmpfs, NFS, or same-device targets. The Rust std library now natively supports the cap-vs-ENOSPC distinction at the type level (ErrorKind::QuotaExceeded since 1.85 vs StorageFull since 1.83), and filesystem-type detection (tmpfs/NFS/FUSE rejection) is available but non-portable via nix::sys::statfs, requiring callers to enumerate the unsafe magic-constant set explicitly. The Claude Code statfs truncation bug (issue #63877) is a concrete cautionary tale that f_bavail must be read as 64-bit (bigint) with guards for both negative-wrap and bsize=0 cases.

## Findings
- **[high/substrate]** Spark exposes spill compression only as a boolean toggle (spark.shuffle.spill.compress, default true), with the codec governed separately by spark.io.compression.codec — there is no small-row or row-width auto-suppression heuristic at this surface.
  - sources: https://spark.apache.org/docs/latest/configuration.html
- **[high/substrate]** Kettle/PDI SortRows also treats spill compression as a binary boolean (compressFiles), runtime-overridable by a variable (compressFilesVariable), serialized as 'compress' in XML — no auto mode and no row-width/row-count heuristic for suppressing compression on small rows.
  - sources: https://github.com/pentaho/pentaho-kettle/blob/master/engine/src/main/java/org/pentaho/di/trans/steps/sort/SortRowsMeta.java
- **[high/substrate]** No surveyed engine (Spark, Kettle) exposes an auto|on|off spill-compression mode keyed to row width — Clinker's proposed small-row heuristic has no direct prior-art template and would be novel substrate behavior.
  - sources: https://spark.apache.org/docs/latest/configuration.html, https://github.com/pentaho/pentaho-kettle/blob/master/engine/src/main/java/org/pentaho/di/trans/steps/sort/SortRowsMeta.java
- **[high/substrate]** ClickHouse caps spill volume via two independent settings — max_temp_data_on_disk_size_for_user and max_temp_data_on_disk_size_for_query (both default 0 = unlimited) — that limit the compressed byte volume of temporary files at per-user and per-query scope respectively.
  - sources: https://github.com/ClickHouse/ClickHouse/pull/40893
- **[high/substrate]** ClickHouse throws a distinct error code for cap-exceeded (TOO_MANY_ROWS_OR_BYTES, 'Limit for temporary files size exceeded') versus physical disk-full (NOT_ENOUGH_SPACE, 'Not enough space on temporary disk' from a failed volume->reserve) — directly modeling the cap-vs-ENOSPC failure separation Clinker wants.
  - sources: https://github.com/ClickHouse/ClickHouse/pull/40893
- **[high/substrate]** ClickHouse's temp-data accounting is a four-level scope tree (global → per-user → per-query → per-purpose) where each child propagates its byte delta upward via deltaAllocAndCheck, so a parent limit constrains the aggregate of its children.
  - sources: https://github.com/ClickHouse/ClickHouse/pull/40893
- **[high/substrate]** Kettle SortRows temp directory defaults to the JVM temp token '%%java.io.tmpdir%%' and is validated at design-time only for existence and is-directory — it never rejects tmpfs/ramdisk, NFS, or same-device-as-source targets.
  - sources: https://github.com/pentaho/pentaho-kettle/blob/master/engine/src/main/java/org/pentaho/di/trans/steps/sort/SortRowsMeta.java
- **[high/substrate]** Spark's DiskBlockManager has no disk-space cap and no cap-vs-ENOSPC distinction; the only disk failure it models is directory-creation failure at startup (caught as IOException in createLocalDirs), with runtime ENOSPC during block writes left unclassified.
  - sources: https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/storage/DiskBlockManager.scala
- **[high/substrate]** When all configured Spark local dirs fail to create, Spark performs a hard process exit — System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR) — rather than falling back to a default temp path or returning a recoverable error.
  - sources: https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/storage/DiskBlockManager.scala
- **[high/substrate]** Vector's disk_v2 buffer enforces max_size by blocking the writer until the reader frees space (backpressure), not by returning a distinct cap-exceeded error — at the API level it does not differentiate a soft cap violation from a physical disk-full condition.
  - sources: https://vector.dev/docs/architecture/buffering-model/
- **[high/substrate]** Rust std::io::ErrorKind::StorageFull (stabilized 1.83.0) represents physical storage exhaustion and explicitly excludes quota errors — the natural mapped kind for an underlying ENOSPC during spill writes.
  - sources: https://doc.rust-lang.org/std/io/enum.ErrorKind.html
- **[high/substrate]** Rust std::io::ErrorKind::QuotaExceeded (stabilized 1.85.0, two minor releases after StorageFull) represents a filesystem-or-other quota violation as a distinct kind — together with StorageFull it lets Clinker distinguish a software-imposed spill cap from physical ENOSPC at the type level without inventing custom sentinels.
  - sources: https://doc.rust-lang.org/std/io/enum.ErrorKind.html
- **[high/substrate]** Filesystem-type detection for rejecting unsafe spill targets is available in Rust via nix::sys::statfs (TMPFS_MAGIC, NFS_SUPER_MAGIC, FUSE_SUPER_MAGIC, ~57 magic constants), but it is Linux/Android-only and non-portable; the module itself directs portability-needing callers to statvfs instead.
  - sources: https://docs.rs/nix/latest/nix/sys/statfs/index.html
- **[high/substrate]** There is no single OS-provided 'safe filesystem' flag — callers must enumerate the unsafe set (tmpfs, NFS, FUSE, etc.) explicitly by comparing statfs filesystem_type() against the named magic constants, and detection is platform-dependent across the FsType variants.
  - sources: https://docs.rs/nix/latest/nix/sys/statfs/index.html
- **[high/substrate]** A free-space preflight must read f_bavail as a 64-bit value: Claude Code's own v2.1.153 preflight truncated the kernel's 64-bit f_bavail to signed 32-bit, so any filesystem with >2^32 free 4KB blocks (~17.6 TB free) wrapped negative and triggered a false ENOSPC abort.
  - sources: https://github.com/anthropics/claude-code/issues/63877
- **[high/substrate]** A negative-value guard alone is insufficient for a statfs free-space preflight: Claude Code's v2.1.163 fix (added {bigint:true} + a q<0n guard) resolved the >17.6TB truncation but missed the bsize=0 case (result exactly 0n, not negative), leaving the Bun/macOS APFS variant live — Clinker must guard both negative-wrap and zero-blocksize.
  - sources: https://github.com/anthropics/claude-code/issues/63877

## Caveats
All 16 findings are tagged substrate — none touch Clinker's fixed identity (single-process, finite-input, finite-job pillars), so adopting any of these techniques is compatible with the locked architecture. Evidence is strong for the disk-cap, error-taxonomy, and statfs axes (multiple primary sources: ClickHouse PR source, Rust std docs, nix docs, a live GitHub issue with code-level diagnosis). It is weaker on two of the four sub-questions the original prompt asked about: (a) spill-dir validation that rejects tmpfs/NFS/same-device/spill==staging — the surveyed tools simply do NOT do this (Kettle and Spark validate only existence/creatability), so the finding is an absence-of-prior-art rather than a borrowable pattern; and (b) the opt-in local STAGING workflow (copy-to-fast-disk, BLAKE3 integrity verify, atomic write-then-rename, per-run UUID isolation, on_existing reuse-if-fresh cache, crash-safe orphan purge) — NO confirmed claim in this corpus covers the staging axis at all, nor does any claim cover DuckDB's temp_directory/max_temp_directory_size, Polars streaming out-of-core, Embulk, Airbyte, Benthos/Redpanda Connect, or rsync/rclone atomic-rename+fsync ordering. Those tools were named in the question but produced no surviving verified claims here, so this synthesis is silent on them — treat the staging-workflow and atomic-rename/fsync-ordering portions of the question as UNANSWERED by this corpus rather than answered-negatively. Vector's backpressure model (claim) is the only on-disk-buffer datapoint and comes from a streaming daemon whose blocking semantics do not transfer to Clinker's drain-and-exit model. No degraded-single-source flag is warranted: claims rest on independent primary sources, but coverage is uneven across the four sub-questions.

## Open questions
- What are DuckDB's concrete config surfaces (temp_directory, max_temp_directory_size) and its cap-exceeded vs ENOSPC error taxonomy, and does Polars' streaming/out-of-core engine expose any spill-dir or spill-cap configuration at all? Neither produced a verified claim despite being the closest single-host columnar prior art to Clinker.
- What is the canonical atomic write-then-rename + fsync ordering for crash-safe staging (fsync file → rename → fsync parent dir), and which tools (rclone, rsync, Embulk, Airbyte) implement integrity verification (checksum/BLAKE3) plus per-run isolation and orphan-purge — the entire local-STAGING half of the question is uncovered by the present corpus.
- Should Clinker model its application-imposed spill cap as a dedicated error variant (mirroring ClickHouse's TOO_MANY_ROWS_OR_BYTES) rather than reusing std::io::ErrorKind::QuotaExceeded, given that std reserves QuotaExceeded for filesystem-level quotas and StorageFull for ENOSPC — i.e., a three-way taxonomy (app-cap / fs-quota / physical-full) rather than two-way?
- For the spill-dir validation Clinker wants but no surveyed tool implements (reject tmpfs/NFS/same-device-as-source, reject spill==staging), what is the portable detection strategy across Linux (statfs magic), macOS, and Windows, and is same-physical-device detection feasible via st_dev comparison given the milestone-12 cross-platform commitment?

## Enhancement / gap candidates
- [nice] Idempotent crash-purge of orphaned spill subdirs at startup, mirroring the staging crash-purge in #174 — #174 gives staging an idempotent crash-purge that reaps stale per-run subdirs from crashed prior runs at next startup. Spill has no equivalent: the spill root is a tempfile::TempDir reaped only by Drop, which SIGKILL, the Linux OOM-killer, and power loss all bypass — leaving clinker-spill-* dirs accumulating forever under a configured storage.spill.dir. Once spill.dir is a stable operator-chosen path (#169) rather than an ephemeral $TMPDIR the OS cleans, this is the Airbyte 'staging directory fills up after crashes' report class that #174's rationale cites — but applied to spill, which #174 leaves uncovered. The two subsystems should share one crash-purge primitive.
- [nice] fsync the parent directory after the staging atomic rename, not just the file before rename — #173 specifies 'fsync, close, atomic rename .partial -> .staged' — it fsyncs the file contents but not the containing directory. On ext4/xfs a rename is only crash-durable after the parent directory's own metadata is fsync'd; without it, a crash between rename and the next directory-metadata writeback can leave the .staged name absent or pointing at a zero-length inode despite the data being on disk. #174's reuse-if-fresh then reads a manifest for a .staged file that doesn't durably exist. The existing metrics.rs write_spool atomic-write does file.sync_all() but also omits the dir fsync — so the codebase already has this latent gap in its one shipped atomic-rename site, and #173 is about to copy the incomplete pattern.
- [nice] Restrict spill and staged file permissions (0o600 / 0o700 dir) so source PII isn't world-readable in a shared spill/staging volume — Spill files and staged copies contain verbatim source records — PII, credentials, financial data. tempfile::NamedTempFile creates files mode 0o600 by default, but #173's staged files use a hand-rolled <file_uuid>.partial open + rename rather than tempfile, and the per-run subdir mkdir in #173/#174 has no explicit mode, so it inherits the process umask (commonly 0o022 -> world-readable dir). The codebase has zero set_permissions/PermissionsExt/mode() calls outside tests. On the multi-tenant host the partition-and-run model targets, a 0o755 staging subdir under a shared /var/clinker/staging exposes another tenant's source bytes. Set restrictive modes explicitly at create time on unix; the cross-platform milestone #244 work means the #[cfg] split is already on the table.
- [nice] Define hard-process-exit vs recoverable-error policy when the configured spill.dir becomes unwritable mid-run — #169 validates spill.dir is writable at startup; #170 handles cap and ENOSPC at write time. Neither covers the dir vanishing or going read-only mid-run (NFS remount, volume unmount, permission change, the dir getting rm'd by an over-eager cleanup cron). Spark's documented behavior is a hard System.exit(DISK_STORE_FAILED_TO_CREATE_DIR) when local dirs fail — a deliberate, surfaced choice, not an unclassified IOException. Clinker should make the same call explicitly: a spill site that finds its root gone should produce a distinct diagnostic (not a generic SpillError::Io that reads like a transient hiccup) and the run should fail deterministically rather than silently degrade. The research notes Spark leaves runtime ENOSPC-during-write 'unclassified' — clinker's #170 fixes ENOSPC but leaves dir-disappearance in the same unclassified bucket Spark got dinged for.
- [nice] Reuse the existing ByteSize parser (clinker-plan config/utils.rs) for disk_cap_bytes / staging caps instead of hand-rolling, and unify cap-unit semantics with the memory budget — #170 says 'Parse via a workspace-blessed byte-size parser (or hand-roll if none exists).' One already exists: clinker-plan/src/config/utils.rs defines ByteSize with a parse() accepting human strings and a serde Deserialize, used today for the 512MB memory budget default. Hand-rolling a second parser would create two divergent byte-string grammars in one config file (memory budget understands one syntax, spill cap another) — a documented operability footgun. Equally important: the memory budget and spill cap should agree on KiB-vs-KB binary/decimal semantics so '512MB' means the same thing in both blocks. This is a 'don't reinvent + keep config grammar uniform' note that touches #170 and #171's cap/threshold parsing.
- [critical] Per-invocation namespacing or collision-safe reuse for staging on_existing=reuse-if-fresh under concurrent invocations — #174's reuse-if-fresh reads a sidecar manifest and skips the copy when source mtime+size match. But #173 stages into <staging_root>/<run_uuid>/ — a fresh uuid per run — so two concurrent partition-and-run invocations staging the SAME shared source each write their own copy (no reuse across invocations, defeating the cache the operator configured) OR, if reuse is keyed by source-path manifest at staging_root level, both invocations race to create/read the manifest and the .partial->.staged rename of one can be observed mid-flight by the other's freshness check. #174's acceptance only tests single-run reuse. The cross-invocation cache-coherence question — where does the manifest live, and how is the read-decide-copy sequence made atomic across processes without a daemon — is unaddressed and is squarely in the partition-and-run deployment the pillars endorse.
- [nice] Cap-headroom estimate in --explain must account for other concurrent invocations sharing the spill volume, or state that it cannot — #176 computes cap-headroom as disk_cap_bytes - sum(estimated_spill_volume_per_stage) and warns at >80%. In the partition-and-run model the headroom shown is per-this-invocation, but the physical volume is shared by every sibling invocation — so an --explain showing 40% headroom is misleading when four siblings each show 40%. The honest fix is small: --explain should report free space on the spill volume (statvfs, which the #175 fs-type detection already needs to statfs the same mount) alongside the per-run estimate, and explicitly note the cap is per-invocation. This closes the same Polars #9847 visibility gap #176 cites but for the multi-invocation reality the architecture recommends, rather than implying single-tenant headroom.
- [nice] Spill-volume free-space preflight at startup (statvfs) when an estimate is available, separate from the runtime cap — #170 catches ENOSPC at write time and #176 estimates per-stage spill volume, but nothing checks at startup whether the spill volume plausibly has room before the pipeline runs for an hour and then dies at the last spill. The codebase has zero statvfs/free-space calls. #175 already adds Linux/macOS statfs for fs-type detection (TMPFS_MAGIC/NFS_SUPER_MAGIC) — the same syscall returns free blocks, so a free-space preflight is nearly free to add at the rejection-pass site. DataFusion (#15358) shipped without any spill volume awareness and added the cap reactively after production runs filled volumes; a cheap startup preflight that warns 'estimated spill 80GB, spill volume has 40GB free' converts a one-hour-then-crash into a one-second startup warning. Pair the estimate (#176) with the free-space read (#175 syscall) — neither issue connects them.

---

## Raw research (un-synthesized agent output)

### Search angles
- **Columnar SQL engine spill-dir config surfaces (substrate lineage)** — `How do single-host columnar/analytical SQL engines expose and implement a configurable temp/spill directory for out-of-core blocking operators (external sort, hash-aggregate, grace-hash join)? Detail DuckDB temp_directory + max_temp_directory_size (default sizing, per-thread temp files, eviction), Polars streaming/out-of-core sink spill path selection and POLARS_TEMP_DIR/POLARS_MAX_THREADS interaction, Apache Spark spark.local.dir multi-directory round-robin across local disks (and how a non-distributed local run uses it), and ClickHouse tmp_path / max_temp_data_on_disk_size_for_query/for_user. For each: the exact config key, default behavior, multi-directory round-robin/striping policy, per-operator vs per-query temp file layout, and the disk-space cap knob. Constrain findings to the single-process single-host model — ignore distributed shuffle-service and cluster-shuffle-manager machinery.`
  - Directly sources the (1) configurable spill dir + multi-dir round-robin and (2) disk-space cap requirements from the engines the lineage explicitly blesses as Clinker's model (DuckDB/Polars). Naming the exact config keys (temp_directory, max_temp_directory_size, spark.local.dir, max_temp_data_on_disk_size) makes the borrow concrete and keeps it inside the single-host pillar.
- **Non-SQL ETL / data-movement on-disk buffer + source staging idioms (identity lineage)** — `How do non-SQL ETL and data-movement tools implement on-disk spill/staging for bounded local runs? Cover Pentaho Kettle/PDI sort-rows temp directory + 'Copy files'/'Get a file with FTP' staging-to-local patterns, Vector disk_v2 buffer (max_size, data_dir, per-sink buffer dirs), Benthos/Redpanda Connect disk-backed buffers and cache/processor staging, Embulk local temp + resumable staging, Airbyte normalization/local temp staging, and rsync/rclone copy-to-fast-local-disk staging (--temp-dir, partial+rename, checksum verification). For each: the concrete config surface for temp/buffer directory, whether they stage remote source files to local disk before processing and how, and any cap/quota on on-disk buffer size. Exclude their streaming-daemon/unbounded-source features — focus only on finite-batch and copy-then-process behavior.`
  - Covers the identity-lineage half the project mandates (CXL is NOT SQL; every research pass must include dedicated non-SQL ETL agents per feedback_etl_research_balance). Surfaces the opt-in local STAGING patterns (Kettle copy-to-local, rclone --temp-dir, Vector disk buffers) the question's part (4) needs, scoped away from the daemon/streaming features the three pillars forbid.
- **Failure-mode taxonomy: unsafe spill targets + cap-exceeded vs ENOSPC (failure-modes)** — `How do batch data engines and storage tools VALIDATE a spill/temp directory target and TAXONOMIZE its failures? (a) Detecting and rejecting unsafe targets: tmpfs/ramfs/ramdisk (parsing /proc/mounts or statfs f_type magic like TMPFS_MAGIC/RAMFS_MAGIC), NFS/network filesystems (statfs magic NFS_SUPER_MAGIC, CIFS, fuse), spill-dir equal to staging-dir, and detecting whether a path sits on the same physical block device as another path (st_dev comparison, /proc/self/mountinfo, major:minor device numbers). (b) Distinguishing a configured spill-volume CAP-EXCEEDED error from the underlying physical ENOSPC 'disk full' error in Rust std::io and how DuckDB/ClickHouse/Spark surface each as distinct user-facing diagnostics. Provide the concrete syscalls/crates (statvfs, nix, sysinfo, rustix) and the precise error discrimination.`
  - Isolates the validation + error-taxonomy core of the question's (1) reject-unsafe-targets and (2) cap-vs-ENOSPC distinction — the highest-novelty, highest-risk substrate work. Clinker already parses /proc/mounts for tmpfs in sysstats.rs, so this angle extends a proven in-repo technique to NFS, same-device, and dir-collision detection, plus the Rust-specific ENOSPC discrimination the diagnostics layer (miette) will need.
- **Crash-safe atomic staging: write-then-rename, fsync ordering, integrity verification, idempotent purge (primary specs)** — `What is the canonical crash-safe protocol for staging a remote/network source file to local disk with integrity guarantees, on a single host? Detail: atomic write-then-rename semantics (POSIX rename atomicity within a filesystem, the write-temp + fsync(file) + rename + fsync(parent-dir) ordering, why fsync of the containing directory is required, EXDEV when crossing filesystems), per-run UUID isolation directories, BLAKE3 (vs SHA-256/xxHash) streaming content verification of staged copies, an 'on_existing reuse-if-fresh' cache policy (mtime/size/content-hash freshness check), and crash-safe idempotent cleanup/purge of orphaned staging directories (marker files, lockfiles via flock, age-based GC, how rclone/rsync/Git/SQLite handle orphan temp cleanup). Cite the durability/rename ordering as documented by SQLite atomic-commit, the LWN/ext4 rename-and-fsync guidance, and rclone/rsync partial-file handling.`
  - Targets the question's (4) staging block in full — atomic-rename + fsync ordering, BLAKE3 integrity, per-run UUID dirs, reuse-if-fresh, and crash-safe orphan purge — which is the most spec-precise and correctness-critical piece. Grounding in SQLite/ext4/rclone durability specs gives the load-bearing fsync-ordering detail that practitioner blog posts routinely get wrong, matching the project's 'resolve to code-read/spec evidence, never probably' bar.
- **Spill compression auto/on/off heuristic and the small-row break-even (substrate lineage + practitioner)** — `When does compressing spilled/temp data on a single host help vs hurt, and how do engines decide auto/on/off? Detail the row-width / small-row break-even where LZ4/Zstd CPU cost exceeds the I/O saved (compression ratio vs disk bandwidth vs CPU, why narrow fixed-width rows compress poorly and waste CPU), how DuckDB compresses temp files, Spark spark.shuffle.spill.compress + spark.io.compression.codec (lz4/zstd/snappy) and spark.shuffle.spill.compress vs spark.shuffle.compress separation, ClickHouse temp data compression, and Arrow IPC/Parquet compression-level guidance. Then: what a concrete auto heuristic looks like (sample first N rows, measure achieved ratio, disable below a threshold; or static row-width / estimated-bytes-per-row cutoff), given Clinker already spills LZ4-frame + postcard-encoded rows. Include benchmark-backed thresholds where published.`
  - Owns the question's (3) compression auto/on/off + small-row heuristic, which none of the other angles cover. It's substrate-lineage (Spark/DuckDB compression codecs) plus practitioner-implementation (the actual break-even threshold and sampling heuristic), and it lands precisely on Clinker's existing LZ4-frame+postcard spill format so the recommendation is directly actionable rather than abstract.

### Extracted claims per source (pre-verification)

#### https://github.com/pola-rs/polars/issues/21120 (primary)
- [central/substrate] Polars defaults its spill/temp directory to /tmp/polars (a fixed, non-user-unique path), which means the first user to run Polars on a shared system owns the directory and all subsequent users receive a permission-denied error when the engine tries to create the global file-cache lockfile at /tmp/polars/file-cache/.process-lock.
  - quote: "In practice, this results in the following path on Linux, owned by whichever user first ran Polars on that system: `/tmp/polars` ... This prevents other users on the system from using Polars for large cloud queries which require access to `/tmp/polars/file-cache/.process-lock`, as the parent directory (and the file itself) are writeable only by the first user."
- [central/substrate] Polars exposes the temp-dir location as the POLARS_TEMP_DIR (also referred to as POLARS_TEMP_DIR_BASE_PATH) environment variable, allowing users to override the default /tmp/polars path; a per-user workaround is to set POLARS_TEMP_DIR=/tmp/polars-$(whoami).
  - quote: "Set the following environment variable to specify a unique TEMP directory as a workaround: `POLARS_TEMP_DIR=/tmp/polars-$(whoami)`"
- [supporting/substrate] The Polars file cache was originally designed to use a single shared directory for all users on the system (not per-user isolation), with the intent that cacheable data (e.g. remote file downloads) could be shared across users; per-process isolation via process-id was considered only for spill data, not the shared cache.
  - quote: "I am not sure it has to be unique per user. There is data that can be shared between users. It has to be unique for data that is bound to the process. For spilling we can use the process-id."
- [supporting/substrate] The failure mode from the shared fixed-path design is a hard panic (not a graceful error) surfaced as a PanicException with message 'failed to open/create global file cache lockfile: Permission denied (os error 13)', occurring inside a Rayon worker thread during lazy-frame sink operations.
  - quote: "thread 'polars-0' panicked at crates/polars-io/src/file_cache/cache_lock.rs:20:13:
failed to open/create global file cache lockfile: Permission denied (os error 13)"
- [tangential/either] Rust's standard library documentation explicitly warns that temp_dir() may be shared among users or processes with different privileges, and that creating files or directories with fixed or predictable names in the temp directory introduces 'insecure temporary file' security vulnerabilities; per-user unique naming (e.g. /tmp/tmux-1234 per uid) is the idiomatic mitigation.
  - quote: "The temporary directory may be shared among users, or between processes with different privileges; thus, the creation of any files or directories in the temporary directory must use a secure method to create a uniquely named file. Creating a file or directory with a fixed or predictable name may result in 'insecure temporary file' security vulnerabilities."

#### https://spark.apache.org/docs/latest/configuration.html (primary)
- [central/substrate] spark.local.dir accepts a comma-separated list of directories on different disks, enabling round-robin distribution of spill/scratch space across multiple devices; the default is /tmp
  - quote: "Directory to use for "scratch" space in Spark, including map output files and RDDs that get stored on disk. This should be on a fast, local disk in your system. It can also be a comma-separated list of multiple directories on different disks."
- [supporting/substrate] spark.local.dir is overridden by environment variables (SPARK_LOCAL_DIRS on Standalone, LOCAL_DIRS on YARN) set by the cluster manager, meaning the config-file value may be silently superseded at runtime
  - quote: "Note: This will be overridden by SPARK_LOCAL_DIRS (Standalone) or LOCAL_DIRS (YARN) environment variables set by the cluster manager."
- [central/substrate] Spill compression for shuffles is a boolean toggle (spark.shuffle.spill.compress, default true); the codec is governed separately by spark.io.compression.codec — there is no small-row/row-width auto heuristic exposed at this config surface
  - quote: "Whether to compress data spilled during shuffles. Compression will use spark.io.compression.codec."
- [supporting/substrate] Spark exposes no user-facing disk-space cap on total spill volume and no separate temp-directory settings dedicated to external sort, hash-aggregate, or grace-hash join; all out-of-core operators share spark.local.dir
  - quote: "The documentation does not explicitly specify: Disk-space caps on spill volume; Dedicated staging directory configuration; Separate temp directory settings for external sort, hash-aggregate, or grace-hash join operations"
- [tangential/substrate] The write buffer size for sorted spill records is configurable via spark.shuffle.spill.diskWriteBufferSize (default 1 MiB)
  - quote: "The buffer size, in bytes, to use when writing the sorted records to an on-disk file."

#### https://github.com/ClickHouse/ClickHouse/pull/40893 (primary)
- [central/substrate] ClickHouse PR #40893 introduces two settings — `max_temp_data_on_disk_size_for_user` (default 0 = unlimited) and `max_temp_data_on_disk_size_for_query` (default 0 = unlimited) — that independently cap the compressed byte volume of temporary spill files, enforced at the per-user and per-query scope levels respectively.
  - quote: "M(UInt64, max_temp_data_on_disk_size_for_user, 0, "The maximum amount of data consumed by temporary files on disk in bytes for all concurrently running user queries. Zero means unlimited.", 0)
M(UInt64, max_temp_data_on_disk_size_for_query, 0, "The maximum amount of data consumed by temporary files on disk in bytes for all concurrently running queries. Zero means unlimited.", 0)"
- [central/substrate] When a disk-cap limit is exceeded, ClickHouse throws `ErrorCodes::TOO_MANY_ROWS_OR_BYTES` (not `NOT_ENOUGH_SPACE`), producing a distinct error code from the physical disk-full path; `NOT_ENOUGH_SPACE` is reserved for the `volume->reserve(max_file_size)` failure when the disk itself lacks room for a single new spill file.
  - quote: "if (compressed_delta > 0 && limit && new_consumprion > limit)
    throw Exception(ErrorCodes::TOO_MANY_ROWS_OR_BYTES, "Limit for temporary files size exceeded");
...
if (!reservation)
    throw Exception("Not enough space on temporary disk", ErrorCodes::NOT_ENOUGH_SPACE);"
- [central/substrate] ClickHouse's temporary-data accounting is structured as a four-level scope tree — global → per-user → per-query → per-purpose (sorting, aggregation, etc.) — where each child scope propagates its byte delta upward via `deltaAllocAndCheck`, so a per-user limit constrains the aggregate of all concurrent queries for that user.
  - quote: "/* ... Data can be nested, so parent scope accounts all data written by children.
 * Scopes are: global -> per-user -> per-query -> per-purpose (sorting, aggregation, etc). */
...
void TemporaryDataOnDiskScope::deltaAllocAndCheck(int compressed_delta, int uncompressed_delta)
{
    if (parent)
        parent->deltaAllocAndCheck(compressed_delta, uncompressed_delta);"
- [supporting/substrate] Spill files written by this PR are always compressed: the `OutputWriter` wraps a `WriteBufferFromFile` in a `CompressedWriteBuffer` unconditionally, with no auto/on/off knob for compression in the PR-40893 implementation.
  - quote: "struct TemporaryFileStream::OutputWriter
{
    OutputWriter(const String & path, const Block & header_)
        : out_file_buf(path)
        , out_compressed_buf(out_file_buf)
        , out_writer(out_compressed_buf, DBMS_TCP_PROTOCOL_VERSION, header_)
    {}"
- [supporting/substrate] The per-user `TemporaryDataOnDiskScope` is created in `ProcessListForUser` constructor when a global context is available; the per-query scope is nested under it with the query-level limit, and is injected into the query context via `query_context->setTempDataOnDisk(...)` at query insertion time — not at plan time.
  - quote: "size_t size_limit = global_context->getSettingsRef().max_temp_data_on_disk_size_for_user;
user_temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(global_context->getTempDataOnDisk(), size_limit);
...
query_context->setTempDataOnDisk(std::make_shared<TemporaryDataOnDiskScope>(
    user_process_list.user_temp_data_on_disk, settings.max_temp_data_on_disk_size_for_query));"

#### https://github.com/pentaho/pentaho-kettle/blob/master/engine/src/main/java/org/pentaho/di/trans/steps/sort/SortRowsMeta.java (primary)
- [central/substrate] Kettle SortRows temp directory defaults to the JVM temp dir token '%%java.io.tmpdir%%' and is validated at design-time only for existence and is-directory, with no rejection of tmpfs/ramdisk, NFS, or same-device-as-source targets.
  - quote: "directory = "%%java.io.tmpdir%%";
...
File f = new File( realDirectory );
if ( f.exists() ) {
  if ( f.isDirectory() ) {
    cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, ... "DirectoryExists" ... );
  } else {
    cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, ... "ExistsButNoDirectory" ... );
  }
} else {
  cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, ... "DirectoryNotExists" ... );
}"
- [supporting/substrate] Spill is triggered either by a row-count threshold (sortSize, default 1,000,000) or a free-JVM-heap-percentage threshold (freeMemoryLimit); if neither is configured, the runtime defaults freeMemoryPctLimit to 25.
  - quote: "data.sortSize = Const.toInt( environmentSubstitute( meta.getSortSize() ), -1 );
data.freeMemoryPctLimit = Const.toInt( meta.getFreeMemoryLimit(), -1 );
if ( data.sortSize <= 0 && data.freeMemoryPctLimit <= 0 ) {
  // Prefer the memory limit as it should never fail
  data.freeMemoryPctLimit = 25;
}"
- [central/substrate] Compression is a binary boolean (compressFiles), overridable by a variable (compressFilesVariable) at runtime, serialized as 'compress' in XML — there is no auto mode and no row-width or row-count heuristic for suppressing compression on small rows.
  - quote: "compressFiles = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "compress" ) );
compressFilesVariable = XMLHandler.getTagValue( stepnode, "compress_variable" );
...
data.compressFiles = getBooleanValueOfVariable( meta.getCompressFilesVariable(), meta.getCompressFiles() );"
- [central/substrate] When compression is enabled, spill files are written with GZIPOutputStream over a BufferedOutputStream; when disabled, a 500,000-byte BufferedOutputStream is used directly — no integrity checksum (BLAKE3 or otherwise) is computed on either path.
  - quote: "if ( data.compressFiles ) {
  gzos = new GZIPOutputStream( new BufferedOutputStream( outputStream ) );
  dos = new DataOutputStream( gzos );
} else {
  dos = new DataOutputStream( new BufferedOutputStream( outputStream, 500000 ) );
  gzos = null;
}"
- [supporting/substrate] There is no disk-space cap on spill volume and no distinction between a user-configured cap-exceeded error and a physical ENOSPC failure — IOException from the spill write is caught and rethrown uniformly as KettleException("Error processing temp-file!").
  - quote: "} catch ( IOException e ) {
  ...
  throw new KettleException( "Error processing temp-file!", e );
}"

#### https://vector.dev/docs/architecture/buffering-model/ (primary)
- [central/substrate] Vector disk_v2 constructs per-component isolation directories using the path pattern `{base_dir}/buffer/v2/{buffer_id}`, where buffer_id is the component's identifier, giving each sink its own subdirectory under the global data_dir.
  - quote: "pub(crate) fn get_disk_v2_data_dir_path(base_dir: &Path, buffer_id: &str) -> PathBuf { base_dir.join("buffer").join("v2").join(buffer_id) }"
- [central/substrate] Vector disk_v2 checksums every record using CRC32C, computed over the concatenation of record_id and payload bytes, storing the result as a uint32 field in the record format.
  - quote: "all records are checksummed (CRC32C) ... checksum: uint32(CRC32C of record_id + payload)"
- [central/substrate] Vector disk_v2 enforces max_size by blocking the writer until the reader frees space (backpressure), rather than returning a distinct cap-exceeded error type separate from ENOSPC — the buffer does not differentiate between a soft cap violation and a physical disk-full condition at the API level.
  - quote: "If a write would cause a data file to grow past the maximum file size, it must be written to the next data file ... the writer will wait until enough space has been freed"
- [supporting/substrate] Vector disk_v2 fsyncs data to disk at data-file rotation boundaries (when a file reaches its 128 MiB maximum and a new file is opened), not on every individual record write, giving a 500ms sync interval as the durability window within a single data file.
  - quote: "Data is synchronized to disk every 500ms ... fsync occurring when the current data file is flushed and synchronized to disk before opening the next file"
- [tangential/substrate] Vector disk_v2 has no compression — records are stored as-is with only rkyv serialization; the absence of compression is a deliberate design choice in the current implementation, not a deferred feature with a config toggle.
  - quote: "The documentation contains no mention of compression. Records are stored as-is with only serialization padding from the rkyv library."

#### https://duckdb.org/2024/07/09/memory-management (secondary)
- [central/substrate] DuckDB's temp_directory defaults to the connected database path with a '.tmp' suffix (e.g., 'database.db.tmp'), or just '.tmp' for in-memory databases, and can be overridden with SET temp_directory = '/path'.
  - quote: "The location of the temporary directory can be chosen using the `temp_directory` setting, and is by default the connected database with a `.tmp` suffix (e.g., `database.db.tmp`), or only `.tmp` if connecting to an in-memory database."
- [central/substrate] DuckDB's max_temp_directory_size defaults to 90% of remaining disk space on the drive where temp files are stored, not a fixed absolute cap.
  - quote: "The maximum size of the temporary directory can be limited using the `max_temp_directory_size` setting, which defaults to `90%` of the remaining disk space on the drive where the temporary files are stored."
- [central/substrate] When max_temp_directory_size is exceeded (or spilling cannot be used), DuckDB reports an out-of-memory error and cancels the query — it does NOT distinguish between a configured cap being hit versus a physical ENOSPC condition.
  - quote: "If exceeded (or if disk spilling cannot be used), the outcome is an 'out-of-memory error is reported and the query is canceled.'"
- [supporting/substrate] DuckDB does not validate temp_directory targets for unsafe storage types such as tmpfs, ramdisk, or NFS — the article makes no mention of any such detection or warning.
  - quote: "No claims made regarding validation against unsafe targets (tmpfs, ramdisk, NFS, etc.). The article does not address detection or warnings for problematic storage types."
- [tangential/substrate] DuckDB provides no documented spill compression setting (auto/on/off) and no per-row-width heuristic for compression — the article contains no information on compression or staging/integrity-verification patterns.
  - quote: "No claims found. The article contains no information on compression settings or staging/integrity-verification patterns."

#### https://doc.rust-lang.org/std/io/enum.ErrorKind.html (primary)
- [central/substrate] std::io::ErrorKind::StorageFull was stabilized in Rust 1.83.0 and covers physical storage exhaustion, explicitly excluding quota errors.
  - quote: "The underlying storage (typically, a filesystem) is full. This does not include out of quota errors."
- [central/substrate] std::io::ErrorKind::QuotaExceeded was stabilized in Rust 1.85.0, two minor releases after StorageFull, and covers filesystem or other quota violations as a distinct error kind.
  - quote: "Filesystem quota or some other kind of quota was exceeded."
- [central/substrate] The two variants cleanly separate cap-exceeded (QuotaExceeded) from physical disk-full (StorageFull), enabling Rust programs to distinguish a software-imposed spill cap from an underlying ENOSPC condition at the type level.
  - quote: "StorageFull: This does not include out of quota errors. QuotaExceeded: Filesystem quota or some other kind of quota was exceeded."

#### https://docs.rs/nix/latest/nix/sys/statfs/index.html (primary)
- [central/substrate] nix::sys::statfs defines TMPFS_MAGIC, NFS_SUPER_MAGIC, and FUSE_SUPER_MAGIC as Linux/Android-only constants, making filesystem-type detection via statfs() non-portable — the module itself notes callers needing portability should use statvfs instead.
  - quote: "Get filesystem statistics, non-portably ... Requires the `fs` crate feature. All constants are marked linux_android platform-specific."
- [central/substrate] The Statfs struct returned by statfs() exposes a filesystem_type() method (returning FsType) that callers can compare against the ~57 named magic constants to identify whether a path resides on tmpfs, NFS, FUSE, or other filesystem classes; there is no single 'safe' flag — callers must enumerate the unsafe set explicitly.
  - quote: "FsType struct: 'Describes the file system type as known by the operating system' (platform-dependent, available on FreeBSD, Android, Linux variants, and Cygwin)"
- [supporting/substrate] FUSE_SUPER_MAGIC is listed as a distinct constant from NFS_SUPER_MAGIC, meaning a spill-directory validator must check both independently to reject userspace filesystems (e.g., SSHFS, rclone mount) in addition to kernel NFS mounts.
  - quote: "FUSE_SUPER_MAGIC - userspace filesystem interface ... NFS_SUPER_MAGIC - network filesystem"
- [supporting/substrate] The nix statfs module defines 57 filesystem magic constants for Linux/Android (the fetched list enumerates them), so a deny-list approach to 'unsafe spill targets' requires the implementor to decide which subset to block — the crate provides no pre-grouped 'volatile' or 'network' category.
  - quote: "The module defines 57 filesystem magic constants for Linux/Android, including: ADFS_SUPER_MAGIC, AFFS_SUPER_MAGIC ... XFS_SUPER_MAGIC"
- [tangential/substrate] fstatfs() accepts a file descriptor rather than a path, enabling post-open filesystem-type verification on an already-opened spill file — useful for atomic-open patterns where the directory path may be a symlink to a different filesystem than it appears.
  - quote: "fstatfs() - 'Describes a mounted file system' given a file descriptor"

#### https://github.com/anthropics/claude-code/issues/63877 (primary)
- [central/substrate] The Claude Code temp-filesystem preflight (introduced in v2.1.153) reads f_bavail via the JS runtime's fs.statfs, which truncates the kernel's 64-bit f_bavail to a signed 32-bit integer. Any filesystem with more than 2^32 free 4 KB-blocks (approximately 17.6 TB free) causes f_bavail to wrap negative, making the computed free-MB value negative and triggering a false ENOSPC abort.
  - quote: "q.bavail comes from the runtime's fs.statfs, which truncates the kernel's 64-bit f_bavail to a signed 32-bit int. … any filesystem with > 2^32 free 4 KB-blocks (≈ 17.6 TB free) trips K < 10 and aborts."
- [central/substrate] On macOS with Bun as the runtime (the Claude Code native binary embeds Bun), fs.statfsSync returns bsize = 0 for every APFS path. The preflight computes bavail × 0 / 1048576 = 0 MB free and aborts regardless of actual disk space, and the CLAUDE_CODE_TMPDIR workaround does not help because Bun returns bsize=0 for all paths.
  - quote: "Bun's fs.statfsSync returns bsize = 0 for every path on APFS. That makes the preflight compute free MB = bavail × bsize ÷ 1048576 = N × 0 = 0 … setting CLAUDE_CODE_TMPDIR does not help, since Bun returns bsize=0 for all paths."
- [central/substrate] The fix shipped in Claude Code v2.1.163 adds a {bigint: true} flag and a negative-value guard (q < 0n) to address the 32-bit truncation case, but this guard does not catch the bsize=0 case where the result is exactly 0n rather than negative, leaving the Bun/macOS APFS variant unresolved through at least v2.1.163.
  - quote: "The {bigint: true} + negative-value guard in 2.1.163 fixes the original >17.6 TB truncation, but @AmirL's bsize=0 variant is still live. … The existing q < 0n guard doesn't catch this since 0n is not negative."
- [supporting/substrate] The upstream Bun struct alignment bug (oven-sh/bun#31133) that causes bsize=0 on macOS x86_64 APFS was fixed in oven-sh/bun#31139, which merged 8 days after Bun 1.3.14 shipped (May 13, 2026), so no released Bun version as of June 5, 2026 includes the fix.
  - quote: "Bun's statfs struct alignment bug (oven-sh/bun#31133) returns bsize=0 on macOS x86_64 APFS. The fix (oven-sh/bun#31139) merged May 21 — 8 days after 1.3.14 shipped. No Bun release includes it yet."
- [supporting/substrate] The false-ENOSPC error is observable only when the captured stdout is empty and the exit code is non-zero (e.g. grep with no matches); the diagnostic misattributes the empty output to disk fullness and kills the process, preventing any chained commands from executing.
  - quote: "The diagnostic runs when stdoutToFile && stdout === '' && exitCode !== 0, misattributing the empty output to ENOSPC. The harness kills the process — commands chained after the failure (;, ||, etc.) never execute."

#### https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/storage/DiskBlockManager.scala (primary)
- [central/substrate] Spark DiskBlockManager distributes block files across multiple local directories via a two-level hash: dirId = hash % localDirs.length, then subDirId = (hash / localDirs.length) % subDirsPerLocalDir, where subDirsPerLocalDir is read from spark.local.dir (config.DISKSTORE_SUB_DIRECTORIES). This is deterministic name-hash routing, not round-robin.
  - quote: "val hash = Utils.nonNegativeHash(filename)
val dirId = hash % localDirs.length
val subDirId = (hash / localDirs.length) % subDirsPerLocalDir"
- [central/substrate] If all configured local directories fail to be created, Spark calls System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR) — a hard process exit — rather than falling back to a default temp path or emitting a recoverable error.
  - quote: "if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }"
- [supporting/substrate] When a single local directory in the configured list fails to be created (IOException), DiskBlockManager logs the error and skips it ('Ignoring this directory') rather than failing the process, so partial directory availability is tolerated as long as at least one dir succeeds.
  - quote: "case e: IOException =>
          logError(
            log"Failed to create local dir in ${MDC(PATH, rootDir)}. Ignoring this directory.", e)
          None"
- [central/substrate] DiskBlockManager has no disk-space cap mechanism and no distinction between cap-exceeded and ENOSPC conditions. The only disk failure mode modeled is directory-creation failure at startup; runtime ENOSPC during block writes is not caught or classified by DiskBlockManager itself.
  - quote: "private def createLocalDirs(conf: SparkConf): Array[File] = {
    Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
      try {
        val localDir = Utils.createDirectory(rootDir, "blockmgr")
        logInfo(log"Created local directory at ${MDC(PATH, localDir)}")
        Some(localDir)
      } catch {
        case e: IOException => ..."
- [supporting/substrate] Temporary block files are created via Utils.tempFileWith(file) and intended to be renamed to their final name, implementing a write-then-rename atomic pattern; however crash-safety via fsync is not visible in DiskBlockManager.scala itself — it is delegated to callers.
  - quote: "def createTempFileWith(file: File): File = {
    val tmpFile = Utils.tempFileWith(file)
    if (permissionChangingRequired) {
      createWorldReadableFile(tmpFile)
    }
    tmpFile
  }"

#### https://github.com/ClickHouse/ClickHouse/issues/80631 (primary)
- [central/substrate] ClickHouse's spill reservation error has two distinct layers: an engine-level cap ('Not enough unreserved space' on DiskLocal) that fires before any write attempt, and an OS-level NOT_ENOUGH_SPACE (code 243) that fires when the engine cap is exceeded and the temporary file cannot be created — both surface through TemporaryFileOnLocalDisk but are distinguishable by log level (Trace vs Error) and error code presence.
  - quote: "DiskLocal: Could not reserve 56.43 GiB on local disk `_tmp_default`. Not enough unreserved space ... Code: 243. DB::Exception: Not enough space on temporary disk, cannot reserve 60622675247 bytes on [_tmp_default: available: 51.31 GiB unreserved: 51.31 GiB, total: 62.71 GiB, keeping: 0.00 B]: While executing AggregatingTransform. (NOT_ENOUGH_SPACE)"
- [central/substrate] ClickHouse's spill cap is tracked as an engine-internal 'unreserved space' counter on the DiskLocal abstraction, separate from the OS-reported available bytes — the error message reports both fields ('available: 51.31 GiB unreserved: 51.31 GiB') and they can diverge, meaning the cap can be exhausted while physical disk space remains.
  - quote: "cannot reserve 60622675247 bytes on [_tmp_default: available: 51.31 GiB unreserved: 51.31 GiB, total: 62.71 GiB, keeping: 0.00 B]"
- [central/substrate] ClickHouse's multi-threaded aggregation spill has a confirmed over-reservation bug: each aggregator thread reserves spill space equal to the entire query's current_memory_usage rather than its per-thread result size, causing N_threads × full_query_memory bytes to be reserved against the cap — exhausting it even when actual spill data is far smaller.
  - quote: "it does it by assuming all data in memory for the query needs to be reserved, in each thread ... Should this be `result_size_bytes` instead of `current_memory_usage`? https://github.com/ClickHouse/ClickHouse/blob/afc8d186727a2530748f447c4bf4af81d837abb3/src/Interpreters/Aggregator.cpp#L1622"
- [supporting/substrate] ClickHouse temporary spill files are named with a per-file UUID prefix (e.g. 'tmp206b3044-40ad-4730-a2ab-0329e55e4fe0') under the named temporary disk '_tmp_default', providing per-file isolation but not per-query or per-run directory isolation.
  - quote: "TemporaryFileOnLocalDisk: Creating temporary file 'tmp206b3044-40ad-4730-a2ab-0329e55e4fe0'"
- [supporting/substrate] The over-reservation behavior in ClickHouse aggregation spill has been present since the original spill-to-disk implementation (PR #6678) and was not introduced by recent changes to Aggregator.cpp, making it a longstanding architectural property of the cap-reservation layer rather than a regression.
  - quote: "Yep, this behavior has been there from the very beginning - https://github.com/ClickHouse/ClickHouse/pull/6678"

#### https://sqlite.org/atomiccommit.html (primary)
- [central/substrate] SQLite flushes the rollback journal to disk before writing any changes to the database file itself, enforcing a strict journal-before-data fsync ordering to guarantee crash recoverability.
  - quote: "The reason for the flush operation in step 3.7 is to make absolutely sure that all of the rollback journal is safely on nonvolatile storage prior to making any changes to the database file itself."
- [central/substrate] On Unix, after writing a super-journal file, SQLite also syncs the containing directory to guarantee the file appears in the directory listing after a power failure — directory sync is a required step in the atomic commit protocol.
  - quote: "After the super-journal is constructed, its content is flushed to disk before any further actions are taken. On Unix, the directory that contains the super-journal is also synced in order to make sure the super-journal file will appear in the directory following a power failure."
- [central/substrate] SQLite commits a transaction by deleting (or zeroing) the rollback journal; the journal's absence is the commit marker, making the commit instant a single filesystem operation rather than a write-then-rename.
  - quote: "This is the instant where the transaction commits. If a power failure or system crash occurs prior to this point, then recovery processes...make it appear as if no changes were ever made to the database file. If a power failure or system crash occurs after the rollback journal is deleted, then it appears as if all changes have been written to disk."
- [supporting/substrate] SQLite protects rollback journal pages with a 32-bit checksum evaluated during rollback; a checksum mismatch causes the rollback to be abandoned rather than applied, providing integrity verification for spilled pages.
  - quote: "SQLite also uses a 32-bit checksum on every page of data in the rollback journal. This checksum is evaluated for each page during rollback while rolling back a journal. If an incorrect checksum is seen, the rollback is abandoned."
- [supporting/substrate] In full-synchronous mode, SQLite flushes the rollback journal twice — once for page content and a second time after writing the page count into the header — doubling the fsync cost to close a TORN-WRITE window in the journal header.
  - quote: "The rollback journal is flushed to disk twice: once to write the page content and a second time to write the page count in the header."

#### https://lwn.net/Articles/799807/ (secondary)
- [central/substrate] The crash-safe atomic file replacement sequence documented from PostgreSQL practice requires: fsync of the existing file and its containing directory, then rename, then fsync of the new file and its containing directory — four fsync calls in that order.
  - quote: "fsync() of the existing file and the directory containing it, followed by the rename(), and then an fsync() of the new file and of its containing directory"
- [central/substrate] POSIX mandates a directory fsync for persistence of newly created or renamed files, but at least ext4 performs this implicitly, making the explicit call unnecessary in practice on that filesystem.
  - quote: "POSIX mandates the directory fsync() for persistence... ext4 effectively does the directory fsync() under the covers, so it is not truly necessary, at least for now"
- [supporting/substrate] SQLite's write-ahead log mode can only be safely used when it is known the database file does not reside on a network filesystem, and no reliable programmatic detection mechanism for network filesystems is documented.
  - quote: "a write-ahead log...can be used if it is known that the database file does not reside on a network filesystem"
- [supporting/either] As of the article's writing (September 2019), kernel documentation provided no formal guidance on safe atomic file creation/replacement for database developers, leaving application developers without an authoritative reference.
  - quote: "whatever application developers do, though, some kernel developer will complain about it"

#### https://arxiv.org/pdf/2603.01384 (primary)
- [central/substrate] The four-step crash-safe staging protocol (write temp file → fsync(temp) → rename(temp, target) → fsync(parent directory)) is necessary but still insufficient to guarantee durability under all Linux filesystem behaviors, because without the parent-directory fsync the rename itself may be lost on crash.
  - quote: "write temp file → fsync(temp) → rename(temp, target) → fsync(parent directory) … Without directory fsync, the rename itself may be lost"
- [central/substrate] After an fsync failure, multiple Linux filesystems mark the affected pages clean even when data has not been written to durable storage, which invalidates retry-based recovery logic built on the assumption that a failed fsync leaves the error state detectable.
  - quote: "After fsync failure, multiple Linux filesystems mark pages clean even when they have not been written to durable media"
- [central/substrate] rename provides namespace atomicity (the file either appears at the target path or it does not) but does not guarantee durability or ordering relative to prior data writes, so a crash between fsync(temp) and fsync(parent dir) can silently lose the rename.
  - quote: "rename does not guarantee durability … rename does not guarantee ordering relative to data writes"
- [supporting/substrate] NVMe volatile write-cache Power-Loss Protection (PLP) status creates epistemic ambiguity about whether a flush is a no-op or a true durability boundary, meaning a 'flush succeeded' return code does not uniquely identify that data reached durable media.
  - quote: "Flush succeeded does not uniquely identify a durability boundary; it may be a no-op depending on device configuration"

#### https://jolynch.github.io/posts/use_fast_data_algorithms/ (blog)
- [central/substrate] xxHash (XXH64) achieves ~13,232 MiBps on a 6.6 GiB JSON file, making it approximately 55x faster than SHA-256 (240 MiBps) and about 18x faster than BLAKE3 (3,675 MiBps) on the same hardware.
  - quote: "Yes that's right, ~0.5 seconds user CPU time for `xxh64` versus ~27 for `sha256` ... XXH64: 0.5s / 13,232 MiBps ... BLAKE3: 1.8s / 3,675 MiBps ... SHA256: 27.5s / 240 MiBps"
- [central/substrate] BLAKE3 is cryptographically secure with 256 bits of entropy and runs at 3,675 MiBps — roughly 15x faster than SHA-256 — making it the recommended algorithm when the data source is untrusted and a cryptographic guarantee is required.
  - quote: "if you're reaching for md5 over xxHash because you need a cryptographic alternative, consider blake3 instead"
- [supporting/substrate] XXH64 is sufficient for most data integrity checks on trusted data (e.g., staging a file copied from a local or known-good source), and delivers approximately 10x throughput improvement over MD5 (727 MiBps) on the same workload.
  - quote: "the `XXH64` variant is usually sufficient for most data integrity checks ... xxHash to net about a ~10x improvement on MD5"
- [supporting/either] MD5 is both cryptographically weak and slower than non-cryptographic alternatives (9.1s / 727 MiBps vs 0.5s / 13,232 MiBps for XXH64 on a 6.6 GiB file), making it an inferior choice for staging-pipeline integrity verification in any scenario.
  - quote: "MD5 is both weak and slow"
- [tangential/either] The article contains no concrete claims about crash-safe atomic staging patterns (write-then-rename, fsync ordering), spill/temp directory configuration, disk-space caps or error taxonomy, compression heuristics, or idempotent purge of orphaned staging directories — its scope is limited to hash algorithm throughput comparisons.
  - quote: "Expect `BLAKE3` to be slightly slower than `xxHash` with a single thread"

#### https://rclone.org/docs/ (primary)
- [central/substrate] Rclone uploads to a temporary file named with the pattern `original-file-name.XXXXXX.partial` (where XXXXXX is a hash of the source file fingerprint and `.partial` is the default --partial-suffix value), then atomically renames to the final name only on successful completion; if the upload fails, the `.partial` file is deleted rather than left in place.
  - quote: "Files upload to temporary names with the suffix. On completion, rclone renames the temp file to the final name, overwriting any existing file only then. If upload fails, the `.partial` file is deleted, preventing incomplete files from appearing in directory listings."
- [central/substrate] The `--inplace` flag disables partial-file staging entirely, causing rclone to write directly to the final filename; this exposes incomplete files during transfer and risks overwriting existing destination files if the transfer fails.
  - quote: "With --inplace: Uploads directly to final filenames without temporary staging. This exposes incomplete files during transfer and risks overwriting existing files if the transfer fails, making it less safe but faster on some systems."
- [supporting/substrate] Rclone achieves write atomicity exclusively through filesystem rename operations, not through explicit fsync calls; the documentation provides no crash-safety guarantees at the filesystem-durability level and does not describe fsync ordering.
  - quote: "Rclone achieves atomicity through rename operations rather than explicit fsync calls. The documentation emphasizes that the rename-based approach prevents other users from seeing partially uploaded files until the transfer completes successfully. The documentation provides no explicit crash safety guarantees or fsync implementation details."
- [supporting/substrate] Rclone's configurable temp directory (`--temp-dir`) defaults to OS cache paths (e.g., `$XDG_CACHE_HOME/rclone` on Unix, `%LOCALAPPDATA%\rclone` on Windows) with `$TMPDIR/rclone` as a final fallback; no disk-space cap on the temp directory is documented.
  - quote: "Default Locations: Windows: %LOCALAPPDATA%\rclone; macOS: $HOME/Library/Caches/rclone; Unix: $XDG_CACHE_HOME/rclone or $HOME/.cache/rclone; Fallback: $TMPDIR/rclone. Note: This controls memory allocation, not disk space. No disk space cap flag is documented in the provided content."
- [tangential/substrate] Rclone's configuration file itself is written crash-safely via a write-then-rename sequence: write to a new temp file, mirror permissions, rename the existing file to a backup temp name, rename the new file to the correct name, then delete the backup — but this pattern applies to config file writes, not to data-file transfer staging.
  - quote: "Configuration file writes follow a crash-safe pattern: Write to new temporary file first; Mirror permissions from existing file (Unix systems); Rename existing file to backup temporary name; Rename new file to correct name; Delete backup file."

#### https://avi.im/blag/2025/sqlite-fsync/ (secondary)
- [central/substrate] SQLite's default PRAGMA synchronous = FULL in rollback journal mode prevents database corruption on OS crash or power failure, but does NOT guarantee transaction durability (committed transactions can still be lost).
  - quote: "in journal mode, `FULL` isn't enough to make transactions durable … ensures that an operating system crash or power failure will not corrupt the database"
- [central/substrate] PRAGMA synchronous = EXTRA achieves durability in journal mode by fsyncing the parent directory after the journal file is unlinked, closing the gap where a directory entry update is lost after power failure.
  - quote: "EXTRA provides additional durability if the commit is followed closely by a power loss"
- [central/substrate] WAL mode with synchronous = NORMAL sacrifices durability: a committed transaction can be lost on power failure even though the write succeeded from the application's perspective.
  - quote: "WAL mode does lose durability. A transaction committed in WAL mode with `synchronous=NORMAL`..."
- [supporting/substrate] On macOS, the system-shipped SQLite library defaults to synchronous = NORMAL (value 1) rather than the upstream default of FULL (value 2).
  - quote: "the default is `NORMAL`"
- [supporting/substrate] On macOS, F_FULLFSYNC must be explicitly enabled (PRAGMA fullfsync=ON); the setting is a no-op on non-macOS platforms and is disabled by default even on macOS.
  - quote: "you always want to use `fullfsync`. This setting has no effect on non-macOS machines"

#### https://facebookincubator.github.io/velox/develop/spilling.html (secondary)
- [central/substrate] Velox's spill_compression_codec has a default of 'none' (compression disabled), and accepts discrete codec names (zlib, snappy, lzo, zstd, lz4, gzip) — there is no 'auto' mode or on/off boolean toggle.
  - quote: "Configuration property spill_compression_codec sets the compression codec to use. [...] specifies the compression algorithm type to compress the spilled data before write to disk to trade CPU for IO efficiency."
- [central/substrate] Velox documents no small-row or row-width heuristic for when compression hurts more than it helps; the trade-off is described purely as 'CPU for IO efficiency' with no break-even analysis.
  - quote: "To reduce the spill file size when the size might exceed the disk space, we can enable spill compression."
- [supporting/substrate] Velox spill serialization uses VectorStreamGroup (a columnar, row-vector-based byte stream), not a row-oriented format like postcard or msgpack.
  - quote: "the SpillFileList object takes a row vector as input, creates a VectorStreamGroup to serialize the row vector and writes out the serialized byte stream"
- [supporting/substrate] Velox's spill compression is opt-in and operator-configured; there is no runtime auto-detection that switches compression on or off based on observed data characteristics.
  - quote: "To reduce the spill file size when the size might exceed the disk space, we can enable spill compression."

#### https://facebookincubator.github.io/velox/configs.html (primary)
- [central/substrate] Velox's spill_compression_codec has a default value of 'none', meaning spilling writes uncompressed data to disk by default. There is no 'auto' mode — compression must be explicitly enabled by naming a codec.
  - quote: "Specifies the compression algorithm type to compress the spilled data before write to disk to trade CPU for IO efficiency. The supported compression codecs are: zlib, snappy, lzo, zstd, lz4 and gzip. none means no compression."
- [central/substrate] Velox supports exactly six named compression codecs for spill: zlib, snappy, lzo, zstd, lz4, and gzip. The configuration surface is a string enum with no auto/heuristic mode.
  - quote: "The supported compression codecs are: zlib, snappy, lzo, zstd, lz4 and gzip."
- [supporting/substrate] Velox enforces a per-query disk-space cap on spill volume via max_spill_bytes, defaulting to 100 GB (107,374,182,400 bytes). This is a soft query-level cap distinct from underlying OS ENOSPC.
  - quote: "The max spill bytes limit set for each query. This is used to cap the storage used for spilling... The default value is set to 100 GB."
- [supporting/substrate] Velox spilling is globally disabled by default (spill_enabled defaults to false), but per-operator spilling (aggregation, join, order-by, window) defaults to true — meaning per-operator flags are gated by the master switch.
  - quote: "Spill memory to disk to avoid exceeding memory limits for the query. (default: false)"
- [tangential/either] The Velox configuration reference contains no small-row or row-width break-even heuristic for when compression hurts more than it helps — the decision is left entirely to the operator configuring the codec string.
  - quote: "Specifies the compression algorithm type to compress the spilled data before write to disk to trade CPU for IO efficiency."

#### https://arrow.apache.org/docs/cpp/api/ipc.html (primary)
- [central/substrate] Arrow IPC IpcWriteOptions exposes a min_space_savings field (type std::optional<double>) that gates per-buffer compression: if the expected space savings falls below this fraction, the buffer is written uncompressed instead.
  - quote: "Minimum space savings percentage required for compression to be applied. Space savings is calculated as (1.0 - compressed_size / uncompressed_size). if min_space_savings = 0.1, a 100-byte body buffer won't undergo compression if its expected compressed size exceeds 90 bytes."
- [supporting/substrate] Arrow IPC compression is limited to three codecs: UNCOMPRESSED, LZ4_FRAME, and ZSTD — no other codecs are permitted on the record batch body buffers.
  - quote: "Compression codec to use for record batch body buffers. May only be UNCOMPRESSED, LZ4_FRAME and ZSTD."
- [supporting/substrate] When min_space_savings is unset, compression is applied universally to every buffer with no per-buffer break-even check; the threshold mechanism is opt-in, not the default.
  - quote: "When min_space_savings is unset, compression applies universally"
- [tangential/substrate] Enabling min_space_savings introduces a forward-compatibility break: Arrow C++ versions prior to 12.0.0 cannot read files written with this option enabled.
  - quote: "enabling this option may result in unreadable data for Arrow C++ versions prior to 12.0.0"
- [tangential/substrate] Arrow IPC provides a use_threads boolean (default true) that routes compression work through the global CPU thread pool, enabling parallel compression across buffers.
  - quote: "Use global CPU thread pool to parallelize any computational tasks like compression."

#### https://github.com/openzfs/zfs/pull/13244 (primary)
- [central/substrate] The ZSTD early-abort heuristic applies a two-pass LZ4-then-ZSTD-1 probe before running the target ZSTD level, but only for blocks >= 128 KB and ZSTD levels > 3; smaller blocks or lower levels bypass the heuristic entirely and compress directly.
  - quote: "Zeroth, if this is <= zstd-3, or <= zstd_abort_size (currently 128k), don't try any of this, just go. (because experimentally that was a reasonable cutoff for a perf win with tiny ratio change)"
- [central/substrate] Using LZ4 alone as the single incompressibility probe loses up to 8.5% of compressed savings on highly compressible data versus no early abort; the two-pass design (LZ4 then ZSTD-1) recovers that regression while preserving the speed gain on incompressible data.
  - quote: "LZ4 alone gets you a lot of the way, but on highly compressible data, it was losing up to 8.5% of the compressed savings versus no early abort, and all the zstd-fast levels are worse indications on their own than LZ4, and don't improve the LZ4 pass noticably if stacked like this."
- [supporting/substrate] The 128 KB block-size threshold was chosen empirically: below it, the overhead of the probe passes outweighs the time saved, making the heuristic a net loss; above it, the throughput win on incompressible data dominates.
  - quote: "because experimentally that was a reasonable cutoff for a perf win with tiny ratio change"
- [supporting/substrate] On a Raspberry Pi 4 at ZSTD-15, the early-abort heuristic reduced write time from 2321 seconds to 835 seconds on a low-compressibility dataset, a >2.7x throughput improvement, at the cost of less than 0.3% more space used compared to baseline ZSTD-3.
  - quote: "< 0.3% more space used to go from 2 hours to 15 minutes? Sold."
- [tangential/either] The heuristic exposes a boolean on/off tunable (module parameter), not a per-dataset property; dataset-level granularity was considered and rejected as unreasonable in scope for this change.
  - quote: "whether they'd like, say, a property on datasets or a module parameter to turn it on and off like I have here... I'll probably remove the rest shortly, except the 'on/off', since I was told that making it a dataset property was unreasonable."

#### https://www.mail-archive.com/linux-btrfs@vger.kernel.org/msg65676.html (primary)
- [central/substrate] The Btrfs compression heuristic uses sample sizes that scale with input size: 128K input → 4096-byte sample, 64K → 3072 bytes, 32K → 2048 bytes, 4K → 1024 bytes.
  - quote: "In data: 128K 64K 32K 4K / Sample: 4096b 3072b 2048b 1024b"
- [central/substrate] Entropy above 7.2/8 (>90%) combined with a large enough difference between pair-frequency and character-frequency is the threshold at which the heuristic allows compression to proceed.
  - quote: "If entropy are High 7.2/8 - 8/8 (> 90%), and if we find BIG enough difference between frequency of a pairs and characters / Give compression code a try"
- [central/substrate] The heuristic rejects compression when the ratio is worse than roughly 131072 bytes → ~110000 bytes (approximately 16% reduction); only ratios better than that threshold pass.
  - quote: "That code, as i see, forbidden compression like: - 131072b -> ~110000b / If compression ratio are better, it's allow that."
- [supporting/substrate] The byte-frequency analysis uses a 'core set' of distinct byte values: a core set of 1–50 distinct byte types signals easily compressible data, while 200–256 distinct types signals data that probably cannot be compressed.
  - quote: "If core set small (1-50 different types) Data easy compressible / If big (200-256) - data probably can't be compressed"
- [supporting/substrate] Shannon entropy alone is insufficient as a compression gate because it cannot detect repeated byte strings, requiring the supplemental byte-pair frequency comparison step.
  - quote: "Entropy can't detect repeated strings of bytes"

#### https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md (primary)
- [central/substrate] Non-compressible input blocks are expanded by 0.4% when encoded in LZ4 Block Format — meaning compression of already-incompressible data produces output larger than the input.
  - quote: "this format explains why a non-compressible input block is expanded by 0.4%."
- [central/substrate] Blocks smaller than 12 bytes cannot be compressed by LZ4; independent blocks smaller than 13 bytes also cannot be compressed because the format requires at least one literal sequence before any match reference.
  - quote: "blocks < 12 bytes cannot be compressed. And as an extension, _independent_ blocks < 13 bytes cannot be compressed, because they must start by at least one literal, that the match can then copy afterwards."
- [supporting/substrate] The LZ4 Block Format has a maximum achievable compression ratio of approximately 250x, setting the upper bound on compression benefit for highly repetitive data.
  - quote: "this format has a maximum achievable compression ratio of about ~250"
- [tangential/substrate] The LZ4 Block Format specification defines no hard block size limit; the format document explicitly states this, though real implementations may impose practical limits and ecosystem compatibility recommends supporting up to 4 MB blocks.
  - quote: "The Block Format does not define any 'size limit', though real implementations may feature some practical limits."

### Claims that survived adversarial verification
- [substrate] Spill compression for shuffles is a boolean toggle (spark.shuffle.spill.compress, default true); the codec is governed separately by spark.io.compression.codec — there is no small-row/row-width auto heuristic exposed at this config surface — https://spark.apache.org/docs/latest/configuration.html
- [substrate] ClickHouse PR #40893 introduces two settings — `max_temp_data_on_disk_size_for_user` (default 0 = unlimited) and `max_temp_data_on_disk_size_for_query` (default 0 = unlimited) — that independently cap the compressed byte volume of temporary spill files, enforced at the per-user and per-query scope levels respectively. — https://github.com/ClickHouse/ClickHouse/pull/40893
- [substrate] When a disk-cap limit is exceeded, ClickHouse throws `ErrorCodes::TOO_MANY_ROWS_OR_BYTES` (not `NOT_ENOUGH_SPACE`), producing a distinct error code from the physical disk-full path; `NOT_ENOUGH_SPACE` is reserved for the `volume->reserve(max_file_size)` failure when the disk itself lacks room for a single new spill file. — https://github.com/ClickHouse/ClickHouse/pull/40893
- [substrate] ClickHouse's temporary-data accounting is structured as a four-level scope tree — global → per-user → per-query → per-purpose (sorting, aggregation, etc.) — where each child scope propagates its byte delta upward via `deltaAllocAndCheck`, so a per-user limit constrains the aggregate of all concurrent queries for that user. — https://github.com/ClickHouse/ClickHouse/pull/40893
- [substrate] Kettle SortRows temp directory defaults to the JVM temp dir token '%%java.io.tmpdir%%' and is validated at design-time only for existence and is-directory, with no rejection of tmpfs/ramdisk, NFS, or same-device-as-source targets. — https://github.com/pentaho/pentaho-kettle/blob/master/engine/src/main/java/org/pentaho/di/trans/steps/sort/SortRowsMeta.java
- [substrate] Compression is a binary boolean (compressFiles), overridable by a variable (compressFilesVariable) at runtime, serialized as 'compress' in XML — there is no auto mode and no row-width or row-count heuristic for suppressing compression on small rows. — https://github.com/pentaho/pentaho-kettle/blob/master/engine/src/main/java/org/pentaho/di/trans/steps/sort/SortRowsMeta.java
- [substrate] Vector disk_v2 enforces max_size by blocking the writer until the reader frees space (backpressure), rather than returning a distinct cap-exceeded error type separate from ENOSPC — the buffer does not differentiate between a soft cap violation and a physical disk-full condition at the API level. — https://vector.dev/docs/architecture/buffering-model/
- [substrate] std::io::ErrorKind::StorageFull was stabilized in Rust 1.83.0 and covers physical storage exhaustion, explicitly excluding quota errors. — https://doc.rust-lang.org/std/io/enum.ErrorKind.html
- [substrate] std::io::ErrorKind::QuotaExceeded was stabilized in Rust 1.85.0, two minor releases after StorageFull, and covers filesystem or other quota violations as a distinct error kind. — https://doc.rust-lang.org/std/io/enum.ErrorKind.html
- [substrate] The two variants cleanly separate cap-exceeded (QuotaExceeded) from physical disk-full (StorageFull), enabling Rust programs to distinguish a software-imposed spill cap from an underlying ENOSPC condition at the type level. — https://doc.rust-lang.org/std/io/enum.ErrorKind.html
- [substrate] nix::sys::statfs defines TMPFS_MAGIC, NFS_SUPER_MAGIC, and FUSE_SUPER_MAGIC as Linux/Android-only constants, making filesystem-type detection via statfs() non-portable — the module itself notes callers needing portability should use statvfs instead. — https://docs.rs/nix/latest/nix/sys/statfs/index.html
- [substrate] The Statfs struct returned by statfs() exposes a filesystem_type() method (returning FsType) that callers can compare against the ~57 named magic constants to identify whether a path resides on tmpfs, NFS, FUSE, or other filesystem classes; there is no single 'safe' flag — callers must enumerate the unsafe set explicitly. — https://docs.rs/nix/latest/nix/sys/statfs/index.html
- [substrate] The Claude Code temp-filesystem preflight (introduced in v2.1.153) reads f_bavail via the JS runtime's fs.statfs, which truncates the kernel's 64-bit f_bavail to a signed 32-bit integer. Any filesystem with more than 2^32 free 4 KB-blocks (approximately 17.6 TB free) causes f_bavail to wrap negative, making the computed free-MB value negative and triggering a false ENOSPC abort. — https://github.com/anthropics/claude-code/issues/63877
- [substrate] The fix shipped in Claude Code v2.1.163 adds a {bigint: true} flag and a negative-value guard (q < 0n) to address the 32-bit truncation case, but this guard does not catch the bsize=0 case where the result is exactly 0n rather than negative, leaving the Bun/macOS APFS variant unresolved through at least v2.1.163. — https://github.com/anthropics/claude-code/issues/63877
- [substrate] If all configured local directories fail to be created, Spark calls System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR) — a hard process exit — rather than falling back to a default temp path or emitting a recoverable error. — https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/storage/DiskBlockManager.scala
- [substrate] DiskBlockManager has no disk-space cap mechanism and no distinction between cap-exceeded and ENOSPC conditions. The only disk failure mode modeled is directory-creation failure at startup; runtime ENOSPC during block writes is not caught or classified by DiskBlockManager itself. — https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/storage/DiskBlockManager.scala

---

# Supplemental research — cross-platform (Linux/Windows/macOS) filesystem operations for the storage subsystem

Pure-Rust (no cmake/C deps), rustc 1.91, edition 2024. Verified via web sources during research.

## 1. Filesystem-type detection (reject tmpfs/ramdisk spill; reject NFS/SMB/FUSE for spill+staging)
| | Linux | macOS | Windows |
|---|---|---|---|
| API | `statfs.f_type` magic constants | `statfs.f_fstypename` string | `GetDriveTypeW` + `GetVolumeInformationW` (after `GetVolumePathNameW`) |
| Crate | `nix::sys::statfs` (+ local consts) or `rustix` | `nix` raw `Statfs.f_fstypename` / `libc::statfs` | `windows-sys` (`Win32_Storage_FileSystem`) |
| Notes | TMPFS_MAGIC 0x01021994, RAMFS_MAGIC 0x858458f6, NFS 0x6969, CIFS 0xff534d42, SMB2 0xfe534d42, FUSE 0x65735546. nix doesn't export SMB2/RAMFS → define locally; keep an explicit rejection allowlist. | NEVER use numeric `f_type` (undocumented, version-unstable, collides). Use `f_fstypename` string: reject "nfs","smbfs","afpfs","webdav"; "autofs" => network automount, reject. macOS has NO native tmpfs (RAM disks show as apfs/hfs) → tmpfs check is a documented no-op. | DRIVE_REMOTE=network, DRIVE_RAMDISK (best-effort; some RAM-disk drivers report DRIVE_FIXED). Get fs name via GetVolumeInformationW. |

**RECOMMENDATION:** per-OS `#[cfg(target_os)]` module — **no unified crate covers all three.** Linux `nix` + local magic consts; macOS raw `f_fstypename`; Windows `windows-sys`.

## 2. Device-identity (detect staging dir on the same physical device as its source)
| | Linux/macOS | Windows |
|---|---|---|
| API | `std::os::unix::fs::MetadataExt::dev()` (st_dev) — std, portable, no dep | `GetVolumeInformationByHandleW` volume serial via `windows-sys` |
| Note | identical on both unixes | std `MetadataExt::volume_serial_number()` is **nightly-only** (issue #63010) — do NOT use on 1.91; call the Win32 API directly. |

## 3. Atomic write-then-rename + crash durability
- POSIX: `std::fs::rename` atomic on same FS (EXDEV across FS → reject/handle). `File::sync_all` before rename. **fsync parent dir** after rename (open dir as File + `sync_all`). macOS `sync_all` already issues `F_FULLFSYNC` (since Rust 1.37).
- Windows: `std::fs::rename` does atomic-replace on **Rust 1.85+ / Win10 1607+** (FileRenameInfoEx POSIX semantics; falls back below that). For older Windows, `MoveFileExW` with `MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH`. No directory fsync needed on NTFS.
- **RECOMMENDATION:** std-only with `#[cfg]`; caller calls `sync_all()` before rename and parent-dir `sync_all()` after on POSIX only (document why it's skipped on Windows). `tempfile::persist()` replaces atomically but does NOT sync — caller must sync first. Avoid `atomicwrites` (documented non-atomic Windows path).

## 4. Free-space query + cap-vs-ENOSPC distinction
- Available bytes: Linux/macOS `statvfs` (`f_bavail*f_frsize`), Windows `GetDiskFreeSpaceExW` (`lpFreeBytesAvailableToCaller`). Use **`fs4` 1.1.0** (`available_space(path)->u64`, rustix-backed, cross-platform). **`fs2` is dead (8yr unmaintained) — do not use.** Guard 32-bit `f_bavail` truncation (`f_bavail<=f_blocks`).
- Error distinction: `std::io::ErrorKind::StorageFull` (stable 1.83 → ENOSPC / Win ERROR_DISK_FULL) and `QuotaExceeded` (stable 1.85 → EDQUOT / Win ERROR_DISK_QUOTA_EXCEEDED). The **engine's self-imposed spill cap is NOT an OS error** — it's the engine's own byte counter; raise a distinct app error (e.g. `SpillCapExceeded`) and never conflate with `StorageFull` (which only appears when the physical device is truly full).

## Crate shortlist (vetted)
| crate | purpose | last release | archived | RustSec | verdict |
|---|---|---|---|---|---|
| nix 0.31.x | Linux/macOS statfs, magic consts, FsType | ~2026 | no | RUSTSEC-2021-0119 fixed ≥0.23 | use |
| rustix 1.1.x | safe POSIX syscalls (statfs/statvfs/fsync) | ~2026 | no | none | use (also pulled by fs4) |
| windows-sys 0.61.x | Win32 FFI (drive type, volume info, MoveFileEx) | active | no | none | use |
| fs4 1.1.0 | cross-platform available_space | 2025 | no | none | use |
| tempfile | named temp + persist() | active | no | none | use (if temp-file abstraction wanted) |
| fs2 | free space / locks | 2018 | dormant | none | AVOID (superseded by fs4) |
| atomicwrites | atomic writes | old | no | none | AVOID (non-atomic Windows) |
| same-file | same-file identity | ~2020 | dormant | none | AVOID (wrong abstraction) |

## Planning implications
- fs-type detection (#175) and same-device detection (#175) need a per-OS `#[cfg(target_os)]` module — no single crate; budget Linux+macOS+Windows arms + a portable façade.
- Windows device-identity needs a direct `GetVolumeInformationByHandleW` call (std API is nightly-only).
- Atomic rename (#173) is std-safe on 1.91 across all three; still need explicit file sync + POSIX parent-dir sync.
- Free-space preflight / cap (#170, #176) → `fs4`; error split (#170) maps cleanly onto std `ErrorKind::StorageFull`/`QuotaExceeded` + a distinct engine-cap error (ClickHouse-style).
- CI must compile+test storage code on x86_64-pc-windows-msvc and aarch64-apple-darwin, not just Linux.
- New deps to vet at Cargo.toml edit time: `nix`, `windows-sys`, `fs4` (and switch workspace `blake3` off `features=["pure"]` to get SIMD).
