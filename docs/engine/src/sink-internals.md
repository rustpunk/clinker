# Sink Internals

Sink nodes are the terminal destinations of a pipeline. Authored `type: sink`
deserializes to `PipelineNode::Sink` with `SinkConfig`, lowers to
`PlanNode::Sink`, and executes through `executor/sink_dispatch.rs`. When the
planner certifies a single linear producer feeding one Sink, the executor can
take a **streaming handoff** that wires the producer arm to a dedicated writer
thread through a bounded crossbeam channel and fires `Writer::write_record` per
record, concurrent with producer emission. Other producer shapes materialize
their output before the writer fires. This page covers the topology that
selects the streaming handoff, its relationship to Source-to-Merge fusion, the
back-pressure chain, the counter semantics that must match the buffered arm,
and the per-format nested-value contract.

*User-facing view: the User Guide's [Sink Nodes](https://github.com/rustpunk/clinker/blob/main/docs/user/src/nodes/sink.md) page.*

## Use-time filesystem containment

Plan-time path validation produces a `ValidatedPath`, but that proof alone is
not durable: an ancestor can be replaced after validation and before the
operating system opens the output. Output creation therefore passes through a
second, use-time boundary in `clinker-exec`:

1. Resolve the runtime policy before opening the leaf. Normal output opens use
   the filesystem observed from the retained parent handle; explicit profile
   names exist only for the qualification harness and must match that
   observation.
2. Walk the destination ancestors without following symbolic links or reparse
   points and retain the destination-parent handle.
3. Claim a hidden sibling reservation, then create a uniquely named hidden
   quarantine leaf relative to that handle with owner-only Unix mode and
   no-follow semantics. The final leaf is not opened or truncated during
   staging. Every disposition, including replacement, holds the reservation so
   concurrent runs cannot both mutate one destination. The reservation carries
   the owner PID and an exclusive non-blocking `fs4` lock. An existing
   reservation is reclaimed only after a creation-grace interval and a
   successful lock, which distinguishes a dead owner from a live or newly
   starting publisher. Lock or initialization failure immediately removes any
   reservation created by that attempt.
4. Retain the boundary in a run-scoped publication ledger while single,
   per-source-file fan-out, and split writers produce their bytes. After the
   executor succeeds, preflight every quarantine and destination before the
   first mutation, then promote each quarantine directly through the retained
   handles. Replacement is one atomic rename; the old final is never moved to a
   backup first.
5. If any promotion or post-rename directory synchronization fails, stop and
   return a typed outcome containing the exact synchronized-visible,
   visible-unsynchronized, and unpublished sets. Already-visible finals are not
   rolled back, and unvisited finals remain untouched. Deterministic fault tests
   pin this partial-set accounting.
6. After each successful promotion, remove its reservation and record the exact
   committed final path. Cleanup failure is typed post-publication debt naming
   the visible final and stale reservation. Metadata sidecars are serialized
   before this point and join the same ledger and collision namespace as data
   outputs. Cross-filesystem
   promotion is refused; it never degrades to copying through a visible final
   path.

Linux uses the locked `nix` filesystem bindings for `openat`, `renameat` /
`renameat2`, `fstatfs`, and directory synchronization. macOS uses the matching
`libc` `openat`, `fstat`, `fstatfs`, `renameat`, and `renameatx_np(RENAME_EXCL)`
primitives. Windows opens
the drive root with reparse-point-aware `CreateFileW`, then walks every
descendant and opens every leaf relative to the retained handle with
`NtCreateFile`. It inspects each handle, compares volume identity, and promotes
relative to the retained destination handle with `NtSetInformationFile`.
The logical `ValidatedPath` remains required at the public containment boundary
on every platform. A promotion that made the destination visible but could not
synchronize its parent enters the explicit visible-but-unsynchronized state;
the ledger reports it as an operational failure and never reduces it to a
warning or claims rollback.

The set protocol is recoverable, not globally atomic: individual renames are
atomic, while a multi-file commit has an observation window in which old and new
entries can coexist. An uncatchable process or machine failure may leave hidden
`.partial` and `.reservation` entries alongside a subset of newly visible
finals. The reported/path-observed set is the recovery record; reservation
liveness is established by the lock plus grace rule rather than by silently
deleting a file that might belong to a live run. Output publication does not use
`.backup` entries.

### Attempt-owned publication modes

Output publication is resolved from the strict `[storage.publication]` block
before an attempt directory or output leaf is created. The run-owned attempt
uses the invocation's existing execution ID and registers primary, per-source
fan-out, split, dead-letter, and metadata-sidecar artifacts in one bounded
ledger. A bounded map of compiled destination parents lets one run target more
than one directory without inventing a common filesystem root. Duplicate
destinations are rejected before attempt creation.

`mode = "direct"` is the default. Each writer receives an owner-only file in
the attempt directory on its destination filesystem. Publication synchronizes
that file and promotes it by same-filesystem rename; it never copies and never
falls back to another mode.

`mode = "local_then_publish"` requires `local_spool_dir` on a local filesystem.
The writer first produces and synchronizes an owner-only local file. The
publisher then copies it in bounded 1 MiB chunks into the destination attempt
directory, synchronizes the destination file, and verifies both the checked
byte count and BLAKE3 digest before marking the artifact ready. The local copy
is unlinked only after the destination-owned manifest state is durable. The final leaf
is still reached solely by destination-local promotion; no copy writes directly
to a visible final. A copy, synchronization, digest, manifest, rename, or
directory-sync failure retains truthful incomplete state and never changes the
selected mode.

`destination_profile` is explicit: `local` (the default), `nfs_v4_1`, or
`smb_3_1_1`. A detected share under `local`, or a detected protocol that does
not match the qualified share profile, fails before publication effects. The
probe distinguishes NFS, SMB/CIFS, and other network or userspace mounts;
platforms that report only an undifferentiated remote drive fail closed for
the qualified NFS and SMB profiles. The
remaining strict keys are `failed_retention_seconds`,
`creation_grace_seconds`, `max_attempt_bytes`, `retained_byte_limit`,
`retained_attempt_limit`, `min_free_bytes`, `sweep_entry_limit`,
`sweep_byte_limit`, and `sweep_time_limit_ms`. Their fixed defaults and hard
ceilings are enforced during policy resolution; only failed-attempt retention
permits zero. Resolution also requires `sweep_byte_limit` to cover
`max_attempt_bytes` plus the bounded 4 MiB manifest, so a valid maximum-sized
attempt cannot permanently stall cleanup paging.

Free space is observed once at admission and compared with the checked attempt
estimate plus `min_free_bytes`. That observation is advisory. It reserves no
blocks or quota, proves no completion guarantee, and does not suppress a later
`ENOSPC` or `EDQUOT` from a write or synchronization call. Default attempt
results carry logical execution/artifact IDs, logical leaves, and exact
published, visible-unsynchronized, or unpublished states. Physical paths are
available only through an explicit opt-in intended for sanitized diagnostics.

Aggregate retained-attempt admission acquires handle-relative lock files under
each root's internal `.clinker-attempts` namespace in canonical root order and
holds them through inventory, eligible cleanup, limit checks, and creation of
every execution root. The lock never occupies an author-addressable final
leaf, and namespace paging recognizes it as internal metadata. Each new root
manifest durably carries the admitted byte estimate before those locks are
released. Inventory sums manifest-owned regular files across roots and charges
at least one per-execution reservation until every artifact size is exact;
simultaneous local-spool and destination quarantine copies still count
physically. Missing or uninspectable ownership evidence is conservative debt,
never a zero-byte assumption.
Namespace enumeration is bounded by the publication policy's fixed maximum,
not the current desired retained count. A configuration downgrade therefore
still returns physical attempts through advancing continuation tokens while
also reporting that the aggregate count exceeds current policy.

The operator query recompiles with the same default anchor as `run` (the
pipeline file's directory when no base is explicit) and replays bounded
file-source discovery before rendering per-source output paths. Execution ID
used in a path template is a separate typed input from an exact purge selector,
which lets expired cleanup reconstruct an execution-scoped root. Continuations
cross the CLI as their canonical raw bytes; structured argument arrays are the
authoritative automation surface and text commands apply platform quoting.

Run-owned manifests also replicate a bounded historical-root receipt into the
stable pipeline root. The receipt is bound to the compiled-plan hash, stores
only typed logical source name/path pairs needed by `{source_file}` and
`{source_path}`, and records sorted path-free identifiers for the execution's
output and spool roots. It contains no direct deletion path. When live source
discovery no longer finds a retained failure's inputs, operator compilation
re-renders the authored templates from the receipt, validates the resulting
paths, and requires their identifiers to match exactly. The stable replica is
itself ordinary manifest ownership: successful publication removes it, while
bounded purge removes it only after the same ownership checks as other roots.

### Remote filesystem qualification

Normal CLI output is admitted from the filesystem type observed through the
retained parent handle, including NFS and SMB shares. The profile strings below
are qualification labels, not pipeline settings and not runtime admission
tokens:

- `linux-nfsv4.1-loopback-ci`: a disposable GitHub-hosted `ubuntu-24.04` VM,
  Linux kernel NFS client/server, NFSv4.1 over TCP, a hard mount, and remote
  locking without a local-only lock mode.
- `linux-smb3.1.1-loopback-ci`: the same runner class, Linux kernel CIFS client,
  Samba server, SMB3.1.1, `cache=strict`, remote byte-range locking without
  `nobrl`, and strict synchronization without `nostrictsync`. The loopback
  client disables only client-side permission checks with `noperm`; Samba still
  authorizes I/O as the configured guest identity.

The dedicated CI matrix provisions each server and mount inside its runner,
places the exported root on a mounted 64 MiB temporary filesystem, and executes
both publication modes. Success pauses after the exact `Complete` manifest and
final are synchronized but before normal attempt cleanup. The harness reopens
both through the mounted client, releases cleanup, and then proves the final is
still present while the successful attempt root and manifest are absent. This
barrier is installed only through a programmatic Linux qualification API; YAML,
the ordinary CLI, and environment configuration cannot enable it, and success
still creates no receipt or sidecar.

The same local, identity-bound control pauses before copy, file sync, rename,
and parent-directory sync. For every applicable mode/stage pair, the harness
withdraws the exact NFS export or stops the exact Samba PID, force-lazy-detaches
the client mount, releases the operation, observes a bounded non-success,
restores and remounts the exact profile, and reopens its retained manifest. SMB
remount uses a bounded retry while detached kernel client state is released.
The harness then exercises
bounded list, inspect, purge preview, and purge execution. A separate attempt fills the
mounted bounded backing until the operating system returns `ENOSPC`; the final
must remain absent and operator cleanup must remove the staging attempt.
Deterministic `EDQUOT` coverage is recorded only as `seam_covered` unless a real
quota is separately provisioned and observed.

Evidence uses `clinker.filesystem-matrix-evidence/3`. It records the runner and
kernel, exact package and protocol observations, mount and lock behavior, the
six unchanged edge outcomes, six lifecycle classes, ordered success and
interruption readbacks, real capacity behavior, recovery, persistence,
operator cleanup, and environment teardown. Teardown and bounded evidence/log
upload run unconditionally for passing, failing, timed-out, and interrupted
matrix cells. Legacy schema 1 or any missing, unknown, or truncated proof is
ineligible.

The byte-range/OFD lock observation remains separate from publication
admission. A dedicated production admission-lock section records independent
test-binary processes calling `RunAttemptPublication::create` on the mounted
profile with opposite multi-root order. Both the retained-count and
retained-byte scenarios require bounded completion, exactly one admission, one
rejection, and readback of the same retained execution from every mounted root.

A profile is support-eligible only when that exact cell writes `status: passed`,
`support_eligible: true`, and successful client-mount, bounded-backing, service,
and workspace teardown evidence. Missing packages or
administrative capability, mount/provision failure, incomplete observations, a
semantic failure, or cleanup failure leaves `status: incomplete` and cannot be
interpreted as support. These loopback results do not certify a corporate
share, vendor NAS device, Windows/macOS client, clustered server, or different
server/mount configuration. They prove the implementation against controlled
representatives. Operators of other shares must validate their server and mount
semantics; runtime detection does not turn CI evidence into a vendor support
claim.

Executor spill should remain on local storage for predictable bounded-memory
performance. Local working data is distinct from destination quarantine:
completed bytes still need to be streamed into a destination-local hidden file,
synchronized there, and promoted on that same share. A local working copy
reduces random network I/O; it cannot make a cross-filesystem rename atomic.

## Streaming vs. buffered

When a single Sink sits directly downstream of an eligible linear producer, a bounded crossbeam channel connects the producer arm to the writer thread, and `Writer::write_record` fires **per record** as the producer emits. For a `Merge.interleave` whose direct predecessors are exclusively owned Sources, this combines with Source-to-Merge receiver fusion to form an end-to-end live path. Each Source must have exactly one outgoing edge, targeting that Merge; sharing any predecessor rejects receiver fusion for the whole Merge.

A shared Source does not necessarily materialize the Merge's *output*. After the Source inputs materialize and the non-fused Merge reads those slots, an otherwise-eligible Merge with one downstream Sink can still hand its result to the writer without admitting a `node_buffers[merge]` slot. Explain therefore reports the shared Sources as `materialized` while the Merge may remain `streaming`; that label does not claim live back-pressure across the Source-to-Merge boundary.

When the producer-to-Sink edge is not certified for streaming, the producer's output materializes before the Sink arm invokes the writer. With a fused `Merge.interleave`, that extra slot would break the live back-pressure chain at the Merge output. The streaming handoff avoids that slot. For a non-fused Merge, it still avoids materializing the Merge's own output, but the already-materialized Merge inputs mean back-pressure cannot extend through to the Source readers.

The streaming path is selected **automatically** — there is no opt-in setting. Pipelines that don't match the topology keep the buffered path.

### Topology

```yaml
- type: source
  name: src_a
  config: { type: csv, path: a.csv, schema: ... }
- type: source
  name: src_b
  config: { type: csv, path: b.csv, schema: ... }
- type: merge
  name: merged
  inputs: [src_a, src_b]
  config:
    mode: interleave        # required
- type: sink
  name: out
  input: merged
  config:
    name: out
    type: csv
    path: out.csv
```

### Eligibility

Every condition must hold for the producer-to-Sink streaming handoff to engage. Source exclusivity is a separate condition for the Source-to-Merge boundary: if it fails, the Merge inputs materialize even though an eligible Merge-to-Sink handoff may still stream.

- The Sink has exactly **one incoming edge**.
- Its producer is a supported linear producer: a Merge, fused Source-to-Transform, single-branch Route, streaming Aggregate, or an eligible streaming-output Combine strategy.
- The producer has **no other downstream consumer** besides this Sink, roots no node-anchored window arena, and satisfies its producer-specific streaming requirements.
- The Sink is **not in the init-phase ancestor closure**.
- The `SinkConfig` has **no `split:` block** — splitting writers manage their own file rotation lifecycle.
- The writer is registered in the **single-file writer registry** (not `fan_out_per_source_file`).
- **No `Source` in the pipeline declares a correlation key or document-level DLQ**, and no Sink reconstructs envelopes. Those paths own deferred or document-scoped writer lifecycles that are incompatible with the per-record writer thread.

For a Merge to receive directly from live Source channels as well, it must be an unseeded `interleave` and **every direct predecessor must be a Source exclusively owned by that Merge**. Eligibility is atomic, so one shared predecessor rejects receiver fusion for all of that Merge's Sources (see [Merge & Back-pressure](merge-internals.md)).

### Back-pressure flow

Across a certified producer-to-Sink handoff, back-pressure flows toward the producer. When the upstream boundaries are also streaming or fused, the chain continues to the Source reader:

```
writer slow → bounded crossbeam Sender::send blocks
             → producer arm blocks
             → Source channel fills (when the upstream boundary is fused)
             → Source ingest thread blocks on send
```

The bounded handoff channel between the producer and Sink (**256 events**) limits that edge's in-flight data. With a fused Source-to-Merge boundary, it joins the existing bounded Source channels into a single pace-bound chain from the underlying `Write` sink back to the source reader. A slow file system, a saturated network sink, or a deliberately paced writer then slows the upstream readers rather than accumulating the producer's whole output in a pipeline-internal `Vec`. When an earlier boundary is materialized, back-pressure stops at that boundary.

### Counter semantics

Counter behavior under the streaming path matches the buffered Sink arm **exactly**:

- `records_written` increments once per `Writer::write_record` call.
- `ok_count` counts distinct source `row_num`s reaching the Sink.
- `dlq_count` is unaffected — DLQ entries originate upstream.

Stage metrics (`SchemaScan`, `Write`, `Projection`) accumulate into the same fields the buffered path uses. The dispatcher folds the streaming task's per-task accounting back into the run-wide totals at end of DAG, so a streaming run and a buffered run over the same input produce identical counter output.

## Memory, telemetry, and lineage

A Sink does not retain an unbounded private collection. Incremental paths hold
at most the bounded handoff channel described above. Materialized producers
charge their node buffer to the run-scoped memory authority, and an authored
Sink `sort_order` uses the shared stable resident/spill sorter through the
planning-owned `PhysicalWriterBoundary`. Document-DLQ, envelope, per-source,
and correlation-deferred modes apply that boundary at their actual population
grain; they do not create a second memory budget.

An XML source feeding a Sink still follows the XML reader's two-pass contract.
The envelope pre-scan and body stream each open the finite source independently;
the pre-scan retains only planner-attributed `$doc.*` subtrees and charges them
incrementally against `max_index_bytes` (64 MB by default). Unreferenced XML is
event-walked and discarded, body records stream one at a time, and a path-backed
file that changes between the two opens fails instead of combining metadata and
body from different bytes.

Telemetry is attached to the runtime owner that establishes each real Sink work
unit: the synchronous dispatcher, streaming writer thread, or deferred
correlation commit. The closed metric set is `sink_started`, `sink_completed`,
`sink_records`, and `sink_errors`; each finished or failed work unit emits one
complete `sink` span after the outcome is known. Admission loss is
behavior-neutral: a full telemetry arena may drop the optional span but cannot
change writer bytes or run status.

Lineage keeps the terminal role as an OpenLineage **output dataset**.
`PlanNode::Sink` resolves the physical or catalog dataset identity, Sink
`mapping` contributes direct column edges, and upstream filters and authored
terminal ordering contribute indirect influence. Composition-scoped Sinks keep
distinct external identities. The Rust/YAML node name changed to Sink; the
lineage role and its output-dataset vocabulary did not.

## Writer handling of `Value::Map` payloads

CSV, fixed-width, EDIFACT, X12, and HL7 writers **refuse** records carrying a `Value::Map` payload at any column slot, raising:

```
FormatError::UnserializableMapValue { format, column }
```

JSON serializes `Value::Map` natively as a nested object. XML also accepts a
map at an element field and recursively maps ordinary keys to child elements,
unescaped `@...` keys to attributes, `#text` to text, and arrays to repeated
children. Both recursive writers validate the shared key grammar, decoded-key
collisions, and the 64-container depth cap before any record bytes reach the
sink.

The engine-stamped `$widened` sidecar is handled at projection: it is expanded
or stripped rather than exposed as author XML. The contract is the same on the
streaming and buffered paths. See
[Schema Drift & the `$widened` Sidecar](auto-widen-internals.md) for that
lifecycle.
