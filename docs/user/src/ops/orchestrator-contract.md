# Running Under a Workflow Orchestrator

Clinker is a finite batch job wrapped in a single CLI binary. That
makes it a natural **activity body** (or task, or step) for an external
workflow orchestrator — Temporal, Airflow, Dagster, Prefect, or a plain
systemd timer. The orchestrator owns scheduling, retries, timeouts, and
cross-job state; Clinker owns one bounded-memory pass over finite input.

This page is the **behavioral contract** that boundary depends on: how a
`clinker run` reports its outcome, how it responds to cancellation, and
what it guarantees about its output files. It is written so an
orchestration author can wire a retry policy and a cancellation timeout
against stable, intended behavior rather than reverse-engineering it.

Temporal is used as the worked example throughout because its
`RetryPolicy`, `heartbeat_timeout`, and cancellation model map cleanly
onto Clinker's exit codes and signal handling. The same contract applies
to any orchestrator that runs Clinker as a child process.

## The shell-out model

Clinker embeds **no** Temporal client, no orchestrator SDK, and no
worker runtime. It does not connect to a Temporal service, poll a task
queue, or report activity results over any wire protocol. Running
Clinker under an orchestrator is deliberately a **shell-out**: the
orchestrator's own worker spawns `clinker run …` as a child process and
reads back three things —

- the **process exit code** (the primary signal; see below),
- **log output** on stderr (`--log-level`), and
- optionally the **metrics spool** file written on completion
  (`--metrics-spool-dir`; see [Metrics & Monitoring](metrics.md)).

Keeping the orchestrator client out of Clinker is a decided
[non-goal](../non-goals.md#not-a-long-running-service): the coupling
lives in a thin worker you write, not in the engine.

## Exit codes → retry policy

Clinker's exit codes are stable and structured for exactly this purpose.
The full diagnostic reference is [Exit Codes & Error
Diagnosis](exit-codes.md); the table below adds the **retry decision**
an orchestrator should make for each.

<!--
Source of truth for these codes: crates/clinker-exec/src/exit_codes.rs.
The constants are pinned by the clinker-exec unit test
`test_exit_codes_documented`; the CLI's error-to-code mapping lives in
crates/clinker/src/main.rs (top-level `run` error match, and the
success-path `report.interrupted` / DLQ branch). Keep this table and the
one in exit-codes.md in sync when any of those change.
-->

| Exit | Meaning | What happened | Orchestrator handling (Temporal) |
|---|---|---|---|
| `0` | Success | All records processed; every output committed. | Activity **success**. |
| `2` | Partial success | Ran to completion, but some records were routed to the dead-letter queue. | Activity **success** — the run finished. Inspect the DLQ counts; escalate only under your own data-quality policy. |
| `1` | Configuration / compile error | Rejected deterministically before any data was written: invalid YAML, CXL type or syntax error, schema-binding failure, DAG-wiring problem, or an unsatisfiable memory budget. | **Non-retryable** `ApplicationError`. The same config and input will fail identically — retrying wastes attempts. |
| `3` | Fatal data error | `fail_fast` tripped, the `--error-threshold` or DLQ-rate ceiling was exceeded, a NaN appeared in a `group_by` key, or a CXL runtime / accumulator error halted the run. | **Non-retryable by default** — deterministic on the same input. Fix the data or the pipeline, then re-run. |
| `4` | I/O / system error | File not found, permission denied, disk full, spill cap exceeded, thread-pool failure, or a malformed input file. | **Retryable** with bounded backoff — many causes are transient. Cap `maximum_attempts`, because some (a genuinely malformed input, a spill cap smaller than the working set) are deterministic and will exhaust the budget. |
| `130` | Interrupted | A SIGINT or SIGTERM drained the run and it exited. | Treat as **cancellation**, not failure (see below). |

### Reading the table into a `RetryPolicy`

- **Fail fast (do not retry): `1` and `3`.** These are deterministic on
  the same input. Surface them as non-retryable so the workflow fails
  immediately instead of burning the retry budget. In Temporal, raise a
  non-retryable `ApplicationError` (or add the code to
  `non_retryable_error_types`).
- **Retry with backoff: `4`.** Transient infrastructure faults (a
  not-yet-arrived input file, a full or contended volume, a locked
  resource) usually clear on their own. Use bounded exponential backoff,
  but first inspect publication diagnostics: every individual final is
  atomic, while a failed multi-file commit may leave a named subset of
  complete finals visible. A stable-input retry with replacement enabled
  converges that set; an external consumer must not treat the set as complete
  until the retry succeeds. Keep `maximum_attempts` finite so a permanently-bad
  input eventually fails the workflow rather than retrying forever.
- **Success, with a caveat: `2`.** Exit `2` means the pipeline *finished*
  — it is success-with-warnings, not a crash. The rejected records are in
  the configured DLQ file; the counts (`records_dlq`, `dlq_path`,
  `per_source_dlq_counts`) are in the metrics spool
  ([Metrics & Monitoring](metrics.md)). Do **not** treat DLQ presence as
  a hard failure unless you choose to. To make DLQ volume a hard failure
  *inside Clinker*, set `--error-threshold N`, which converts an overflow
  into exit `3`. To make it a workflow decision, let the activity succeed
  and branch on the DLQ count in your workflow code.

## Cancellation and SIGTERM contract

Clinker installs a process-wide handler for **both SIGINT and SIGTERM**
(the `ctrlc` crate's "termination" feature). Either signal requests a
**graceful drain**: in-flight work finishes and worker threads join. If
cancellation wins the atomic publication gate, no staged output or sidecar is
promoted and the process exits **130** with the run marked interrupted.

**Cancellation is bounded-latency, not instantaneous.** The shutdown
request is a flag that the executor polls at well-defined points:

- at **operator chunk boundaries** during streaming dispatch, and
- **every 4096 records** while a blocking operator (sort, aggregate)
  builds its in-memory arena.

Between two poll points the run keeps working, so the worst-case delay
from signal to exit is roughly one chunk (or one 4096-record build slice)
of processing time. Size your timeouts accordingly:

- Set Temporal's **`heartbeat_timeout`** and the cancellation **grace
  period** comfortably **above** the worst-case time to process one chunk
  on your largest input. A grace period shorter than that will escalate a
  healthy, draining run to a hard kill.
- The worker must translate a Temporal cancellation into a child
  **SIGTERM** (which Clinker drains cleanly), and use **SIGKILL only as a
  last resort** after the grace period expires. SIGKILL cannot be caught. If
  it lands during the bounded multi-file publication loop, it can leave a
  subset of complete finals visible; reconcile the set as described below.

### What a cancelled run leaves behind

- If the interrupt wins before the **publication point of no return**, no
  final is produced. Every completed output and sidecar remains in its hidden
  destination-local file for inspection, regardless of whether interruption
  was observed during streaming dispatch or a blocking build.
- If publication wins that single atomic race first, later signals do not
  interrupt the finite promotion loop. The run completes publication and
  reports its ordinary success, DLQ-partial, or publication-failure status.
  Orchestrators must allow that bounded commit to finish instead of assuming a
  signal sent after the gate will force exit `130`.

When cancellation wins, exit `130` takes precedence over DLQ-partial code `2`.
No individual final is ever partially written: a final path contains either
its previous complete bytes or newly promoted complete bytes.

## Output atomicity and retry-safety

Each output is written **atomically per artifact**: it streams into a sibling
hidden file in the same directory, and only after the pipeline completes
successfully is that file **renamed** into the final path (followed by
synchronization of the retained parent directory for crash durability). Main
outputs and metadata sidecars share one run-scoped ledger and collision
namespace.

The consequences an orchestrator can rely on:

- A failure before publication leaves **no half-written final file**. Hidden
  partials are kept and logged for operator inspection, and previous finals are
  untouched.
- A failure or hard kill during a multi-artifact publication may leave an exact
  subset of newly promoted complete finals visible. Clinker never moves old
  finals to backups and never rolls visible replacements back. A caught
  failure logs synchronized-visible, visible-unsynchronized, unpublished, and
  cleanup-debt paths separately and exits `4`.
- A **retry can overwrite cleanly.** The default `if_exists: overwrite`
  replaces a previous attempt's output only during successful promotion. If a
  pipeline explicitly uses `if_exists: error`, pass **`--force`** for the retry
  or change that policy deliberately. Alternatively, `if_exists:
  unique_suffix` hands each attempt a distinct,
  race-safe path through a hidden sibling reservation. All collision policies,
  including overwrite, reserve the destination so only one live publisher can
  mutate it.

The same lifecycle covers fan-out outputs (one file per source file) and
`split:` outputs. Their files join the run-scoped publication ledger as they
open and are promoted only after the complete pipeline succeeds. Readers that
need set-level consistency must wait for the process's successful exit; the
filesystem cannot make several destination renames globally atomic.

## Idempotency: what re-running a batch means today

Clinker treats every run as a **fresh, finite pass** over its input.
There is no checkpoint, no resume cursor, and no incremental state
carried between attempts: a retry **reprocesses all input from the
start**. "Cancel + retry" therefore means *discard the hidden partials and
recompute the whole batch*. A retry after exit `4` may also need to replace the
complete subset named by the prior publication diagnostics.

Two conditions make retries idempotent:

- **Stable input.** Clinker reads whatever is on disk at run time. Retries
  are idempotent only if the input files do not change between attempts —
  stage inputs immutably (or point each logical batch at a fixed snapshot)
  so attempt *n+1* sees exactly what attempt *n* saw.
- **A stable correlation key.** Pass **`--batch-id <ID>`** to carry a
  meaningful identifier — e.g. the Temporal workflow or run id — across
  every attempt of one logical batch. It appears in the metrics spool and
  log lines, so all attempts correlate under one key. (Without it, each
  invocation generates its own UUID v7.)

## Heartbeating a long run

For an activity that outlives its `start_to_close_timeout`, Temporal
relies on heartbeats to distinguish a slow-but-healthy run from a wedged
one. Two rules apply to Clinker:

- **Size `heartbeat_timeout` above the cancellation latency** described
  above. Because cancellation is bounded-latency (chunk / 4096-record
  granularity), a heartbeat timeout tighter than one processing slice can
  reap a run that is making progress.
- **Heartbeat details are advisory progress, not a resume cursor.**
  Clinker cannot resume from a heartbeated offset — every attempt is a
  full pass. Use heartbeat details for liveness and observability only;
  never feed them back as a "start from record N" input.

What is observable for liveness **today** is log output (`--log-level`)
and the metrics spool written at completion (`--metrics-spool-dir`). A
structured, machine-readable run report and a live progress stream
suitable for richer heartbeat details are tracked upstream in
[issue #622](https://github.com/rustpunk/clinker/issues/622) and are not
available yet; do not build a contract on them until they land.

## See also

- [Exit Codes & Error Diagnosis](exit-codes.md) — the full per-code
  diagnostic guide and the plan-time `E###` codes.
- [Production Deployment](deployment.md) — binary layout, dedicated
  user, and a systemd example.
- [Metrics & Monitoring](metrics.md) — the fields (`records_dlq`,
  `dlq_path`, timing) an orchestrator can read after a run.
- [Memory Tuning](memory.md) — sizing `--memory-limit` and the spill cap
  that governs exit `4`'s disk-exhaustion path.
