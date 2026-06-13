# Streaming Output Writes

Output nodes are the terminal sinks of a pipeline. For most topologies the Output arm runs *buffered*: the upstream stage accumulates every record before the writer fires. But when a single Output sits directly downstream of a fused `Merge.interleave` of Sources, the executor takes a **streaming path** that wires the Merge arm to the writer task through a bounded `tokio::sync::mpsc` channel and fires `Writer::write_record` per record, concurrent with Merge production. This page covers the topology that selects that path, the exact eligibility predicate, the end-to-end back-pressure chain, the counter semantics that must match the buffered arm, and the writer contract that rejects `Value::Map` payloads.

*User-facing view: the User Guide's "Output Nodes" page.*

## Streaming vs. buffered

When a single Output sits directly downstream of a `Merge` whose mode is `interleave` and whose every direct predecessor is a `Source`, the executor takes the streaming path: a bounded `tokio::sync::mpsc::channel` connects the Merge arm to the writer task, and `Writer::write_record` fires **per record** as Merge emits, concurrent with Merge production.

The buffered alternative — which still runs for every other Output topology — waits until the Merge arm has accumulated *every* record before invoking the writer. With a slow upstream Source that defeats the live back-pressure the `Merge.interleave` fusion provides at the Source-channel layer: each record sits in `node_buffers[merge]` until the slow Source finishes. The streaming path exists precisely to preserve that fused back-pressure all the way to the sink.

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

Every condition must hold for the streaming path to engage; if any fails, the buffered path runs:

- The Output has exactly **one incoming edge**, and that predecessor is a `Merge` with `mode: interleave`.
- **Every direct predecessor of that Merge is a `Source`** — the same predicate the fused `Merge.interleave` arm uses for its live `tokio::select!` (see [Merge & Back-pressure](merge-internals.md)).
- The Merge has **no other downstream consumer** besides this one Output (no fan-out).
- The Output is **not in the init-phase ancestor closure**.
- The `OutputConfig` has **no `split:` block** — splitting writers manage their own file rotation lifecycle.
- The writer is registered in the **single-file writer registry** (not `fan_out_per_source_file`).
- **No `Source` in the pipeline declares a correlation key** — the correlation-buffered output path defers writes to `CorrelationCommit` and is incompatible with per-record write.

### Back-pressure flow

Under the streaming path, back-pressure flows end-to-end:

```
writer slow → mpsc::Sender::send().await yields
             → Merge arm yields
             → Source mpsc::Receiver fills
             → Source ingest task blocks on send
```

The bounded handoff channel between Merge and Output (**256 slots**) and the existing per-Source ingest channels form a single pace-bound chain from the underlying `Write` sink back to the source reader. A slow file system, a saturated network sink, or a deliberately-paced writer no longer accumulates records in pipeline-internal `Vec`s; the upstream readers slow down to match.

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
