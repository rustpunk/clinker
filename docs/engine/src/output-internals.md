# Streaming Output Writes

Output nodes are the terminal sinks of a pipeline. When the planner certifies a single linear producer feeding one Output, the executor can take a **streaming handoff** that wires the producer arm to a dedicated writer thread through a bounded crossbeam channel and fires `Writer::write_record` per record, concurrent with producer emission. Other producer shapes materialize their output before the writer fires. This page covers the topology that selects the streaming handoff, its relationship to Source-to-Merge fusion, the back-pressure chain, the counter semantics that must match the buffered arm, and the writer contract that rejects `Value::Map` payloads.

*User-facing view: the User Guide's "Output Nodes" page.*

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
