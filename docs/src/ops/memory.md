# Memory Tuning

Clinker is designed to be a good neighbor on shared servers. Rather than consuming all available memory, it works within a configurable budget and spills to disk when necessary.

## Setting the memory limit

**CLI flag (highest priority):**
```bash
clinker run pipeline.yaml --memory-limit 512M
```

**YAML config:**
```yaml
pipeline:
  memory_limit: "512M"
```

The CLI flag overrides the YAML value. Accepted suffixes: `K` (kilobytes), `M` (megabytes), `G` (gigabytes).

**Default:** 256M

## How it works

Clinker uses a custom accounting allocator to track memory usage across all pipeline nodes. When memory pressure rises toward the configured limit, aggregation operations spill intermediate state to temporary files on disk. The pipeline continues with degraded throughput rather than crashing with an out-of-memory error.

This means:

- Pipelines always complete if disk space is available, regardless of input size.
- Performance degrades gracefully under memory pressure -- you will see slower execution, not failures.
- The memory limit is a soft ceiling, not a hard wall. Momentary spikes may briefly exceed the limit before spill kicks in.

## Sizing guidelines

| Workload | Recommended limit | Notes |
|----------|-------------------|-------|
| Small files (<10 MB) | 128M | Minimal memory pressure |
| Medium files (10-50 MB) | 256M (default) | Covers most ETL jobs |
| Large files or complex aggregations | 512M-1G | Multiple group-by keys, large cardinality |
| Multiple large group-by keys | 1G+ | High-cardinality distinct values |

**Target workload:** Clinker is optimized for 1-5 input files of up to 100 MB each, processing 10K-2M records per run.

## Aggregation strategy interaction

Memory consumption depends heavily on the aggregation strategy the optimizer selects:

- **Hash aggregation** accumulates state in a hash map. Memory usage is proportional to the number of distinct group-by values. With high-cardinality keys, this can consume significant memory before spill triggers.

- **Streaming aggregation** processes groups in order and emits results as each group completes. Memory usage is minimal (proportional to a single group's state) but requires the input to be sorted by the group-by keys.

- **`strategy: auto`** (the default) lets the optimizer choose based on the declared sort order of the input. If the data arrives sorted by the group-by keys, streaming aggregation is selected automatically.

To influence strategy selection:

```yaml
  - type: aggregate
    name: rollup
    input: sorted_data
    config:
      group_by: [department]
      strategy: streaming    # force streaming (input MUST be sorted)
      cxl: |
        emit total = sum(amount)
```

Only force `streaming` when you are certain the input is sorted by the group-by keys. If the data is not sorted, results will be incorrect. Use `auto` when in doubt.

## Monitoring memory usage

Use the [metrics system](metrics.md) to track `peak_rss_bytes` across runs:

```bash
clinker run pipeline.yaml --metrics-spool-dir ./metrics/
```

The metrics file includes `peak_rss_bytes`, which shows the maximum resident memory during execution. If this consistently approaches your memory limit, consider increasing the budget or restructuring the pipeline to reduce intermediate state.

## Shared server considerations

On servers running JVM applications, memory is often at a premium. Recommendations:

- Set `--memory-limit` explicitly rather than relying on the default. Know your budget.
- Use `--threads` to limit CPU contention alongside memory limits.
- Monitor `peak_rss_bytes` in production metrics to right-size the limit over time.
- Schedule large pipelines during off-peak hours when JVM heap pressure is lower.
