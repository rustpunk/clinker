# Running Clinker Directly or Under a Supervisor

Clinker is a finite synchronous CLI. The ordinary command is the primary path
and needs no worker, service, scheduler, or orchestration component:

```bash
clinker run pipeline.yaml
```

An external scheduler or workflow runner can opt into a versioned child-process
protocol when it needs machine-readable lifecycle evidence:

```bash
clinker run pipeline.yaml --machine ndjson-v1 --batch-id logical-batch
```

Machine mode does not change who owns the run. Clinker still validates,
executes, cancels, and publishes one finite attempt. The parent process owns
scheduling, retry and backoff, deadlines, heartbeats, process lifetime, and
direct-child reaping. Clinker embeds no orchestrator SDK, worker, daemon, or
service runtime.

## Stream ownership and compatibility

In machine mode, stdout contains only compact UTF-8 NDJSON and is flushed after
each event. Human diagnostics and tracing use non-ANSI stderr. The parent must
drain stdout and stderr concurrently; reading one pipe to completion before the
other can deadlock when an OS pipe fills.

Every event carries these fields:

| Field | Meaning |
|---|---|
| `protocol` | Always `clinker.run`. |
| `schema` | Protocol major version. `ndjson-v1` emits `1`. |
| `event` | Lifecycle kind such as `started`, `plan_resolved`, `progress`, `publication_artifacts`, `completed`, `failed`, or `cancelled`. |
| `seq` | Zero-based sequence, increasing by exactly one within this process. |
| `batch_id` | Caller-supplied logical-batch correlation retained across retries. |
| `execution_id` | Fresh non-overridable UUIDv7 generated for this process. |
| `plan_identity` | `pending`, `resolved` with the semantic plan fingerprint, or `unavailable` after admission failure. |

The resolved fingerprint covers the compiled topology, schemas, composition
and CXL dependencies, winning channel/group config values, and all four
runtime-variable scopes. It excludes deployment-only file locations and layer
source formatting, so relocating equivalent inputs does not invalidate a
pinned plan while a value that can change execution does.

Progress events add a bounded logical phase, kind, elapsed time, optional
checkpoint count, and truncation flags. They never contain records, secrets,
source URLs, or physical paths. Failed terminals add a stable failure code,
broad category, sanitized message, and `retry_with_backoff`, `do_not_retry`, or
`policy_required` advice. Before a publication-aware terminal, bounded
`publication_artifacts` records carry the path-free inventory in ordered
chunks. Artifact entries contain only `artifact_id`, `kind`, and `state`.
The terminal carries the attempt's completeness, cleanup-debt count, total
artifact count, and counts by artifact state. Every NDJSON record, including a
maximum-cardinality inventory chunk and its terminal summary, is at most 16
KiB.

A consumer must reject an unsupported `schema` major. Within schema 1 it may
ignore additive fields and unknown nonterminal event kinds, but those additions
carry no completion or failure meaning. Missing required fields, malformed
UTF-8 or JSON, non-monotonic sequence, identity changes, duplicate terminals,
or EOF without a terminal make the attempt incomplete.

## Terminal, exit, and artifact reconciliation

Neither a terminal event nor a process status is sufficient alone. Accept a
controlled outcome only when the supported terminal family, its embedded exit
where present, the actual child status, and current-attempt artifact evidence
agree:

| Terminal evidence | Required process status | Artifact interpretation | Adapter result |
|---|---:|---|---|
| `completed`, result `success`, exit `0` | `0` | Publication is complete; every reported artifact is individually complete. | Success. |
| `completed`, result `completed_with_dlq`, exit `2` | `2` | Publication is complete and includes the reported complete DLQ artifact. | Completed under the caller's data-quality policy. |
| `failed`, embedded exit `1`, `3`, or `4` | The same exit | Use the exact reported publication state. A visible subset, if any, consists only of individually complete artifacts. | Failure; apply the typed retry advice and caller policy. |
| `cancelled` | `130` | Graceful cancellation won before publication; final paths for this attempt remain unchanged. | Cancellation. |
| No terminal, malformed stream, unsupported major, duplicate terminal, forced termination, or mismatched exit | Any | Do not infer current-attempt success from a pre-existing final or a visible complete subset. | Incomplete attempt. |

Exit `4` is intentionally broad; the typed failed terminal distinguishes retry
advice without requiring the parent to parse rendered diagnostics. EOF alone
never proves success. A control-pipe failure can prevent terminal delivery, so
even an otherwise plausible exit remains incomplete without matching terminal
evidence.

Publication is atomic per artifact, not for the artifact set. A failure or
uncontrolled stop during multi-artifact publication can leave an exact subset
of newly promoted, individually complete finals visible. Consumers that need a
complete set must wait for reconciled success and verify the expected artifact
inventory. Reassemble artifact chunks only when every chunk index from zero to
`chunk_count - 1` is present in sequence before the terminal and the assembled
count matches the terminal's `artifact_count` and `state_counts`. Never treat a
partial terminal stream as set-wide success.

## Language-neutral adapter loop

1. Launch one child with a stable caller-owned `batch_id`, piped stdout and
   stderr, and an overall attempt deadline. Retain only bounded sanitized tails
   for diagnostics.
2. Drain stdout and stderr concurrently. Parse stdout incrementally as UTF-8
   NDJSON and validate the first record's protocol major, identities, and
   sequence.
3. Heartbeat the external scheduler on an independent cadence. Report only the
   latest sanitized identity, sequence, logical phase/counts, and snapshot age;
   do not wait for or translate a Clinker progress event into a heartbeat.
4. Enforce a total Start-to-Close-style deadline for the whole process. A
   no-progress timeout is an additional explicit deployment policy, not a
   substitute for the overall deadline.
5. On cancellation or deadline, deliver the platform's real graceful signal to
   the direct child, keep both pipes draining, and start a separately bounded
   cancellation grace period. If grace expires, force termination exactly once.
   A forced stop is incomplete, not cancelled or successful.
6. Always wait for and reap the direct child before joining both drain tasks.
   Accept an outcome only through the terminal, process-status, and artifact
   reconciliation table above.
7. On retry, launch a completely fresh process from the beginning of the input
   with the same `batch_id`. Require a new `execution_id`; retain no Clinker
   progress event as checkpoint or resume state.

The heartbeat interval must be below the scheduler's heartbeat timeout. The
overall attempt deadline must cover ordinary execution and publication; the
grace period is a separate bounded interval for cooperative cancellation before
forced termination.

## Cancellation and process trees

Clinker installs handlers for SIGINT and SIGTERM. If graceful cancellation wins
the atomic gate before publication, it exits `130` and leaves finals unchanged.
If publication wins first, later signals do not relabel or erase already
complete visible artifacts; the bounded promotion finishes with `completed` or
`failed` truth.

Signal-handler installation is admission-critical. If installation fails,
Clinker exits with infrastructure status `4` before opening the machine
protocol stream or touching sources, staging, attempts, outputs, or lineage.

The direct-child contract is exercised on Linux with a real SIGTERM rather
than a closed control pipe or an in-process cancellation shortcut. The
cooperative case verifies exit `130`, a matching `cancelled` terminal,
unchanged final paths, and child reaping while both bounded drains remain live.
The uncooperative case verifies that the grace interval is independent of the
overall attempt deadline, force happens once only after that interval, the
direct child is reaped before drain joining, and the attempt remains
incomplete. Process groups and descendant ownership are deliberately outside
that direct-child proof.

The adapter owns the platform termination domain. On POSIX, an adapter that
creates descendants may place them in a process group and signal that group. On
Windows, such an adapter may use a Job Object, including kill-on-close policy.
These are adapter responsibilities: Clinker does not ship a process-group
manager or Job Object owner. The parent must still reap the direct Clinker
child after graceful or forced termination.

## Retry and identity boundaries

`batch_id` is correlation only. Each invocation generates a fresh
`execution_id` and starts input from the beginning. Clinker has no cross-attempt
checkpoint, resume cursor, deduplication state, distributed transaction, or
exactly-once guarantee. Safe application retry depends on stable input and the
destination's chosen `if_exists` policy.

Cancellation and forced termination preserve failed-attempt evidence under
the configured [storage retention policy](storage.md#output-publication-and-retained-attempts).
An incomplete attempt keeps its staging directory and manifest without changing
pre-existing finals; the manifest's `eligible_after` timestamp, 24 hours by
default, controls when ordinary cleanup may reclaim it. An immediate retry does
not resume or mutate that directory: it starts a new attempt with a fresh
`execution_id` and its own staging state.

Progress is advisory liveness evidence, not a durable checkpoint or external
heartbeat. Machine events are control evidence, not a secret-bearing event bus
or compliance log. Ordinary users remain free to run the standalone command
without `--machine` or any supervisor.

## See also

- [CLI Reference](cli-reference.md)
- [Exit Codes & Error Diagnosis](exit-codes.md)
- [Production Deployment](deployment.md)
- [Metrics & Monitoring](metrics.md)
- [Memory Tuning](memory.md)
