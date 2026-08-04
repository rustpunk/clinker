# Running Under a Workflow Orchestrator

Clinker is a finite batch job wrapped in a single CLI binary. That makes it a
natural activity body (or task, or step) for an external workflow orchestrator.
The orchestrator owns scheduling, retries, timeouts, and cross-job state;
Clinker owns one bounded-memory pass over finite input.

Clinker embeds no orchestrator SDK or worker runtime. An external worker
launches `clinker run …` as a child process and observes the exit code, stderr,
and optional metrics spool.

## Exit codes and retry policy

| Exit | Meaning | Recommended handling |
|---|---|---|
| `0` | Success; every output committed. | Activity success. |
| `2` | Completed with records routed to the DLQ. | Activity success; inspect DLQ counts under your data-quality policy. |
| `1` | Configuration, schema, or compile error. | Non-retryable for unchanged configuration and input. |
| `3` | Fatal data-quality error or configured threshold exceeded. | Non-retryable by default; fix the data or pipeline. |
| `4` | I/O, format, spill, or system error. | Retry with bounded backoff after checking whether the cause is transient. |
| `130` | Interrupted by a graceful SIGINT or SIGTERM drain. | Cancellation, not failure. |

Exit `4` includes both transient faults and deterministic failures such as a
malformed input or undersized spill cap, so keep retry attempts finite. Exit
`2` is completion with warnings, not a crashed attempt.

## Cancellation and SIGTERM

Clinker installs a process-wide handler for SIGINT and SIGTERM. Either signal
requests a graceful drain: in-flight work finishes and worker threads join. If
cancellation wins the atomic publication gate, no staged output or sidecar is
promoted and the process exits `130`.

Cancellation latency is bounded by the executor's polling points:

- operator chunk boundaries during streaming dispatch; and
- every 4096 records while a blocking operator builds its in-memory arena.

Set the orchestrator's cancellation grace period above the worst-case time for
one such slice. Translate cancellation to SIGTERM and reserve SIGKILL for the
end of that grace period.

If interruption wins before publication, completed artifacts remain in hidden
destination-local files for inspection and no new final is produced. If
publication wins first, later signals do not interrupt the finite promotion
loop. The run finishes publication and reports its ordinary terminal status.
When cancellation wins, exit `130` takes precedence over DLQ-partial exit `2`.

## Output atomicity and retry safety

Each output is atomic per artifact. It streams into a sibling hidden file and
is renamed into its final path only after successful execution, followed by
synchronization of the retained parent directory. Main outputs and metadata
sidecars share one run-scoped publication ledger and collision namespace.

The consequences an orchestrator can rely on:

- A failure before publication leaves no half-written final file. Previous
  finals remain untouched and hidden partials are retained and logged.
- A failure or hard kill during multi-artifact publication can leave an exact
  subset of newly promoted, complete finals visible. The filesystem cannot
  make several destination renames globally atomic.
- A retry can replace that subset with the default `if_exists: overwrite`.
  With `if_exists: error`, pass `--force` deliberately or change the policy.
  `unique_suffix` gives each attempt a distinct race-safe destination.
- Fan-out and `split:` outputs use the same ledger and remain hidden until the
  complete pipeline reaches publication.

Consumers that need set-level consistency must wait for a successful process
exit and verify the expected artifact set.

## Idempotency

Every attempt is a fresh finite pass. Clinker has no checkpoint, resume cursor,
or incremental state shared between attempts; a retry reprocesses all input.
Retries are idempotent when both conditions hold:

- Inputs remain stable between attempts, ideally through immutable staging or
  a fixed snapshot for each logical batch.
- The caller supplies a stable `--batch-id` for all attempts of that logical
  batch. Without it, each invocation generates a new UUID v7.

`--batch-id` provides correlation, not deduplication or exactly-once delivery.

## Heartbeats

Clinker does not embed an orchestrator heartbeat client. A supervising worker
may heartbeat independently while the child is alive. Size the heartbeat
timeout above one cancellation polling slice, and treat heartbeat details as
advisory progress rather than a resume cursor.

Current liveness surfaces are human log output and the metrics spool written at
completion. Do not parse ordinary stdout or stderr as a versioned control
protocol.

## See also

- [Exit Codes & Error Diagnosis](exit-codes.md)
- [Production Deployment](deployment.md)
- [Metrics & Monitoring](metrics.md)
- [Memory Tuning](memory.md)
