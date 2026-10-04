# Interactive Guides

Some parts of Clinker are easier to understand by trying them than by reading
about them. Each guide below is a single page that runs in your browser, on a
desktop or a phone. You change a setting or tap a record, and the page shows
what the engine does with it. The guides don't run a pipeline; each one
reproduces the rules described on the reference page it links back to.

## [Where does a null go?](../cxl/nulls-explainer.html)

How CXL works out an expression when a field is empty: `and`, `or` and `not`
with null, why `null == null` is true, and why a `filter` drops a record whose
condition comes out null. Reference: [Null Handling](../cxl/nulls.md).

## [Correlation keys](../pipelines/correlation-keys-explainer.html)

How one failing line takes the rest of its order to the DLQ, and how an
Aggregate's `group_by` decides between rejecting a whole group and recomputing
totals without the failed line. Reference: [Correlation Keys](../pipelines/correlation-keys.md).

## [Document context](../pipelines/envelope-and-doc-context-explainer.html)

Where `$doc.*` values come from, how each file becomes its own document with
its own Aggregate roll-up, and what `dlq_granularity: document` rejects.
Reference: [Document Envelope Context](../pipelines/envelope-and-doc-context.md).

## [Route and Merge](../nodes/route-merge-explainer.html)

Where each record goes when a Route's conditions are true, not true, or fail,
in exclusive and inclusive mode, and how a Merge rejoins the branches. Reference:
[Route Nodes](../nodes/route.md) and [Merge Nodes](../nodes/merge.md).

## [Combine playground](../nodes/combine-explainer.html)

Which build rows `where:` matches for each driver row, and what `match:`,
`on_miss:` and `drive:` do with them, including range joins. Reference:
[Combine Nodes](../nodes/combine.md).

## [Window functions](../cxl/windows-explainer.html)

Which rows of a partition each `$window.*` function reads, and why
`$window.sum` is a partition total while `$window.cumulative_sum` is the
running total. Reference: [Window Functions](../cxl/windows.md).

## [Streaming vs. blocking](../ops/streaming-vs-blocking-explainer.html)

Which stages stream and which hold their output in a buffer, for several
pipeline shapes, with the `--explain` lines each one produces. Reference:
[Streaming vs. Blocking Stages](../ops/streaming-vs-blocking.md).

## For engine developers

The Clinker Engine Internals book has one more: a memory system explainer,
linked from its *Memory Arbitration & Scheduling* chapter. It covers the memory
budget, back-pressure, spilling to disk and the scheduler, with a simulator.
