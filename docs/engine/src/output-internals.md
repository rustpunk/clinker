# Streaming Output Writes

Output nodes are the terminal sinks of a pipeline. When the planner certifies a single linear producer feeding one Output, the executor can take a **streaming handoff** that wires the producer arm to a dedicated writer thread through a bounded crossbeam channel and fires `Writer::write_record` per record, concurrent with producer emission. Other producer shapes materialize their output before the writer fires. This page covers the topology that selects the streaming handoff, its relationship to Source-to-Merge fusion, the back-pressure chain, the counter semantics that must match the buffered arm, and the writer contract that rejects `Value::Map` payloads.

*User-facing view: the User Guide's "Output Nodes" page.*

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
3. Create the final leaf relative to that handle with owner-only Unix mode and
   no-follow semantics. A replaced ancestor or linked leaf returns
   `security_policy`.
4. Open a promotion source through an independently anchored parent, compare
   filesystem/volume identity, synchronize the complete source, and rename it
   relative to the two handles.
5. Synchronize the destination directory after rename. Cross-filesystem
   promotion is refused; it never degrades to copying through a visible final
   path.

Linux uses the locked `nix` filesystem bindings for `openat`, `renameat` /
`renameat2`, `fstatfs`, and directory synchronization. macOS uses the matching
`libc` `openat`, `fstat`, `fstatfs`, and `renameat` primitives. Windows opens
the drive root with reparse-point-aware `CreateFileW`, then walks every
descendant and opens every leaf relative to the retained handle with
`NtCreateFile`. It inspects each handle, compares volume identity, and promotes
relative to the retained destination handle with `SetFileInformationByHandle`.
The logical `ValidatedPath` remains required at the public containment boundary
on every platform.

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
executes confinement, lock exclusion, synchronized promotion/visibility,
cancellation, cross-filesystem refusal, and cleanup-liveness checks, and tears
the environment down on every exit. Its per-profile artifact records the
runner image and kernel, exact client/server package versions, effective mount
options, negotiated protocol observations, lock behavior, synchronization and
failure-injection results, and teardown status.

A profile is support-eligible only when that exact cell writes `status: passed`,
`support_eligible: true`, and successful teardown evidence. Missing packages or
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

When a single Output sits directly downstream of an eligible linear producer, a bounded crossbeam channel connects the producer arm to the writer thread, and `Writer::write_record` fires **per record** as the producer emits. For a `Merge.interleave` whose direct predecessors are exclusively owned Sources, this combines with Source-to-Merge receiver fusion to form an end-to-end live path. Each Source must have exactly one outgoing edge, targeting that Merge; sharing any predecessor rejects receiver fusion for the whole Merge.

A shared Source does not necessarily materialize the Merge's *output*. After the Source inputs materialize and the non-fused Merge reads those slots, an otherwise-eligible Merge with one downstream Output can still hand its result to the writer without admitting a `node_buffers[merge]` slot. Explain therefore reports the shared Sources as `materialized` while the Merge may remain `streaming`; that label does not claim live back-pressure across the Source-to-Merge boundary.

When the producer-to-Output edge is not certified for streaming, the producer's output materializes before the Output arm invokes the writer. With a fused `Merge.interleave`, that extra slot would break the live back-pressure chain at the Merge output. The streaming handoff avoids that slot. For a non-fused Merge, it still avoids materializing the Merge's own output, but the already-materialized Merge inputs mean back-pressure cannot extend through to the Source readers.

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
- type: output
  name: out
  input: merged
  config:
    name: out
    type: csv
    path: out.csv
```

### Eligibility

Every condition must hold for the producer-to-Output streaming handoff to engage. Source exclusivity is a separate condition for the Source-to-Merge boundary: if it fails, the Merge inputs materialize even though an eligible Merge-to-Output handoff may still stream.

- The Output has exactly **one incoming edge**.
- Its producer is a supported linear producer: a Merge, fused Source-to-Transform, single-branch Route, streaming Aggregate, or an eligible streaming-output Combine strategy.
- The producer has **no other downstream consumer** besides this Output, roots no node-anchored window arena, and satisfies its producer-specific streaming requirements.
- The Output is **not in the init-phase ancestor closure**.
- The `OutputConfig` has **no `split:` block** — splitting writers manage their own file rotation lifecycle.
- The writer is registered in the **single-file writer registry** (not `fan_out_per_source_file`).
- **No `Source` in the pipeline declares a correlation key or document-level DLQ**, and no Output reconstructs envelopes. Those paths own deferred or document-scoped writer lifecycles that are incompatible with the per-record writer thread.

For a Merge to receive directly from live Source channels as well, it must be an unseeded `interleave` and **every direct predecessor must be a Source exclusively owned by that Merge**. Eligibility is atomic, so one shared predecessor rejects receiver fusion for all of that Merge's Sources (see [Merge & Back-pressure](merge-internals.md)).

### Back-pressure flow

Across a certified producer-to-Output handoff, back-pressure flows toward the producer. When the upstream boundaries are also streaming or fused, the chain continues to the Source reader:

```
writer slow → bounded crossbeam Sender::send blocks
             → producer arm blocks
             → Source channel fills (when the upstream boundary is fused)
             → Source ingest thread blocks on send
```

The bounded handoff channel between the producer and Output (**256 events**) limits that edge's in-flight data. With a fused Source-to-Merge boundary, it joins the existing bounded Source channels into a single pace-bound chain from the underlying `Write` sink back to the source reader. A slow file system, a saturated network sink, or a deliberately paced writer then slows the upstream readers rather than accumulating the producer's whole output in a pipeline-internal `Vec`. When an earlier boundary is materialized, back-pressure stops at that boundary.

### Counter semantics

Counter behavior under the streaming path matches the buffered Output arm **exactly**:

- `records_written` increments once per `Writer::write_record` call.
- `ok_count` counts distinct source `row_num`s reaching the Output.
- `dlq_count` is unaffected — DLQ entries originate upstream.

Stage metrics (`SchemaScan`, `Write`, `Projection`) accumulate into the same fields the buffered path uses. The dispatcher folds the streaming task's per-task accounting back into the run-wide totals at end of DAG, so a streaming run and a buffered run over the same input produce identical counter output.

## Writer rejection of `Value::Map` payloads

CSV, XML, fixed-width, EDIFACT, X12, and HL7 writers **refuse** records carrying a `Value::Map` payload at any column slot, raising:

```
FormatError::UnserializableMapValue { format, column }
```

JSON is the exception — it serializes `Value::Map` natively as a nested object.

The typical cause is a `$widened` sidecar reaching a non-JSON writer because the Output node set `include_unmapped: false`, which strips the sidecar's expansion and leaves the raw `Value::Map` slot to hit the writer. The contract is the same on the streaming and buffered paths: the writer rejects the map-valued record rather than emitting a malformed row. See [Schema Drift & the `$widened` Sidecar](auto-widen-internals.md) for the sidecar lifecycle, the `include_unmapped` interaction, and the remediation routes for this rejection.
