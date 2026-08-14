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
runtime-variable scopes. A composition contributes the body each call site
actually binds, so two channels that patch one shared body differently are
two plans. It excludes deployment-only file locations and layer source
formatting, so relocating equivalent inputs does not invalidate a pinned plan
while a value that can change execution does. Where rejected records are
written is a location and is excluded; *whether* they are written is not — a
pipeline with no `error_handling.dlq.path` and no per-source override
discards them, and reads as a different plan from one that keeps them.

The `version` field carries the fingerprint schema, currently `2`. A digest
is comparable only with another digest of the same version: when the schema
changes, the same pipeline yields a different digest, and the version is how
a consumer holding a pinned value tells that apart from a changed plan.

Progress events add a bounded logical phase, kind, elapsed time, counts, and
truncation flags. They never contain records, secrets, source URLs, or
physical paths. Periodic records follow Clinker's own clock rather than
internal engine activity, so a run inside one long operation keeps producing
them; they stay advisory and bounded and never replace the parent's own
heartbeat. [Progress records](#progress-records) below specifies the counts
and what a consumer may conclude from them. Failed terminals add a stable failure code,
broad category, sanitized message, and `retry_with_backoff`, `do_not_retry`, or
`policy_required` advice. Before a publication-aware terminal, bounded
`publication_artifacts` records carry the path-free inventory in ordered
chunks. Artifact entries contain only `artifact_id`, `kind`, and `state`.
The terminal carries the attempt's completeness, cleanup-debt count, total
artifact count, and counts by artifact state. Every NDJSON record, including a
maximum-cardinality inventory chunk and its terminal summary, is at most 16
KiB.

One invocation that reaches a terminal without running an attempt is
supported: the plan-only `--lineage <FILE>` export, which preflights the
identity policy, writes its document, and returns before any data is read. It
shares this stream's `execution_id` and `batch_id`, so the exported document is
correlatable with the invocation that produced it, and it closes with
`completed` / `success` / exit `0` carrying an explicit *empty* publication —
zero artifacts, every state count zero, no cleanup debt. An absent inventory
would be read on that row as publication complete for a run that published
nothing; an empty one says the same thing the reconciliation table already has
vocabulary for. Its document is written and flushed before that terminal is
attempted, so a terminal the pipe refuses there is reconciled exactly as a
published run's is: exit `4` with
`infrastructure.delivery.unreportable_outcome` and `policy_required`, never
`retry_with_backoff`, because the export already exists on disk. Modes that
write their own document to standard output —
`--explain`, `--dry-run`, `-n`, `--lineage -`, `--lineage-events -` — are
refused at admission with exit `1` before any record is written.

A required lifecycle record that cannot be delivered while nothing has yet been
read, written, or staged ends the run at exit `130` with a `cancelled`
terminal, and the attempt's final paths are unchanged. That applies to
`started`, to the `planning` transition, and to `plan_resolved`, on a run and
on the plan-only `--lineage <FILE>` export alike: a supervisor that stops
reading during plan compile learns the same thing about the same condition
whichever it asked for.

After plan resolution, Clinker starts the required machine-progress worker
before source discovery, staging, attempt creation, sink writes, or lifecycle
START. If that worker cannot be created, the stream ends with exactly one
`infrastructure.runtime.transient` failed terminal and exit `4`; no run effect
has started, and a supervisor may retry with backoff.

A consumer must reject an unsupported `schema` major. Within schema 1 it may
ignore additive fields and unknown nonterminal event kinds, but those additions
carry no completion or failure meaning. Missing required fields, malformed
UTF-8 or JSON, non-monotonic sequence, identity changes, duplicate terminals,
or EOF without a terminal make the attempt incomplete.

Records reach the stream whole or not at all, and a parent that reads slowly
does not change what the stream says. A record the pipe refuses is not written,
takes no `seq` with it, and is not what the next record is numbered after, so a
lost advisory observation leaves the numbering dense rather than shifting every
record after it. A record the pipe accepts only part of is completed by the
next write of this stream — never restarted — so a momentarily full pipe cannot
produce two copies of one record or two terminals. What was already delivered
is likewise not repeated: a terminal retried after a refusal sends only the
inventory chunks the pipe has not taken, so each chunk index appears exactly
once ahead of the terminal that counts them.

## Terminal, exit, and artifact reconciliation

Neither a terminal event nor a process status is sufficient alone. Accept a
controlled outcome only when the supported terminal family, its embedded exit
where present, the actual child status, and current-attempt artifact evidence
agree:

| Terminal evidence | Required process status | Artifact interpretation | Adapter result |
|---|---:|---|---|
| `completed`, result `success`, exit `0` | `0` | Publication is complete; every reported artifact is individually complete. | Success. |
| `completed`, result `completed_with_dlq`, exit `2` | `2` | Publication is complete and includes the reported complete DLQ artifact. | Completed under the caller's data-quality policy. |
| `failed`, embedded exit `1`, `3`, or `4` | The same exit | Use the exact reported publication state. When `publication` is absent the state could not be reported; infer nothing about the visible set from its absence. A reported visible subset, if any, consists only of individually complete artifacts. | Failure; apply the typed retry advice and caller policy. |
| `cancelled` | `130` | Graceful cancellation won before publication; final paths for this attempt remain unchanged. | Cancellation. |
| No terminal, malformed stream, unsupported major, duplicate terminal, forced termination, or mismatched exit | Any | Do not infer current-attempt success from a pre-existing final or a visible complete subset. | Incomplete attempt. |

Exit `4` is intentionally broad; the typed failed terminal distinguishes retry
advice without requiring the parent to parse rendered diagnostics. EOF alone
never proves success. A control-pipe failure can prevent terminal delivery, so
even an otherwise plausible exit remains incomplete without matching terminal
evidence.

That is also why a failed terminal delivery is never reported as a transient
runtime fault. When a run has published and only the terminal saying so cannot
be written, a retried terminal that does get through reports exit `4` with
`infrastructure.delivery.unreportable_outcome` and `policy_required` — never
`retry_with_backoff` — and carries the publication the refused terminal
carried. The failure is on the reporting channel, not in execution: the finals
are visible, the lineage and OTLP terminals for the same run recorded its
completion, and re-running the batch would duplicate published data. A
supervisor reconciles it as a failure whose artifact evidence is complete and
whose repetition is a policy decision, not an automatic one. When neither
terminal reaches the stream the attempt is incomplete by the table above, which
is the same reconciliation it always was.

The terminal family follows the exit code, and the `completed` family covers
only exit `0` and exit `2`. Any other non-cancellation exit is written as a
`failed` terminal carrying the run's own failure classification, including
after an earlier `failed` emission that could not be encoded and so left the
single terminal slot free. A non-zero exit is never restated as result
`success`; if no terminal can be encoded at all, the stream ends without one
and the attempt is incomplete by the table above.

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

## Progress records

A `progress` event carries a `progress` object and a `truncation` object:

```json
{"event":"progress","seq":7,
 "progress":{"phase":"executing","kind":"periodic","elapsed_ms":1031,
             "records_read":200000,"files_done":5,"files_total":5},
 "truncation":{"detail":false,"events":false}}
```

`kind` is `transition` for a lifecycle edge (`planning`, `executing`,
`finalizing`, `publishing`) and `periodic` for an advisory observation inside
a phase.

### The counts

`records_read` is the number of source records read so far, across every
source. It never decreases within a run, and the last one a run emits is the
number of records that run read. **It has no companion total, and no total
will be added.** The terminal carries no record count of its own: a progress
record is the only place this stream reports one, so there is nothing to
reconcile it against and no disagreement to arbitrate. A source is read as a stream, so its
record count is not established until its last record has been read: any
"records remaining" Clinker could publish mid-run would be a guess presented
as a measurement. A supervisor answers *is this run moving* by comparing
`records_read` between two events, which is the question the record is here
to answer. *When will it finish* is not a question this stream answers.

`files_done` and `files_total` are the one denominator Clinker does
establish before it reads anything: a source's file set is enumerated at
startup, so the count is known rather than estimated. `files_total` is
`null` when **any** source of the run reads from something other than an
enumerated file set — a network source, for instance. A denominator covering
only part of a run's work is withdrawn rather than published, because nothing
on the wire would say which part it covered.

`files_total` is also `null` on the earliest records of a run, before source
discovery has completed. It is written once and never changes afterwards, so
a consumer may cache it on first sight; it becoming non-`null` mid-stream is
normal and is not an identity change.

Two things a consumer must not do with these counts:

- **Do not treat `files_done / files_total` as a completion percentage.**
  It measures input consumed, not work finished. It reaches 100% while sort
  merges, aggregate finalization, and output publication are still running,
  which is exactly the shape of a progress bar that sits at 100% and appears
  to hang.
- **Do not divide by a `null` total.** Absence means unknown for this run,
  not zero and not an error. Clinker publishes no percentage of its own for
  this reason: a ratio against an absent denominator is the one value that
  turns a missing number into a wrong one.

### The record cap and the cadence floor

Periodic records are bounded twice. At most one is emitted per second, and at
most **128** per run. After the 128th, Clinker emits exactly one further
record with `truncation.events` set to `true`, and then stops emitting
periodic records for the rest of the run. Transitions are never capped, so
`finalizing` and `publishing` still arrive, as does the terminal.

Because the two bounds compose, **any run longer than roughly two minutes
stops producing periodic progress well before it ends.** This is deliberate:
it is what keeps the stream bounded regardless of how long a run takes.

`truncation.events` is the in-band signal for it. A consumer that sees it
should conclude that *the periodic stream has ended and the run is
continuing normally*. It must **not** conclude that the run has stalled,
and must not treat the silence that follows as evidence of a hung process.
A supervisor that kills or retries a run on progress silence will kill
healthy long runs, and for a pipeline that writes output, a retry means the
work is done twice.

Liveness is the parent's own responsibility, on the parent's own clock — see
step 3 of the [adapter loop](#language-neutral-adapter-loop). The run's real
outcome is the terminal record, which always arrives.

## Cancellation and process trees

Clinker installs handlers for SIGINT and SIGTERM. If graceful cancellation wins
the atomic gate before publication, it exits `130` and leaves finals unchanged.
If publication wins first, later signals do not relabel or erase already
complete visible artifacts; the bounded promotion finishes with `completed` or
`failed` truth.

A cancellation is reported as a cancellation on every surface that reports it,
and which source noticed the signal does not change that. A file source drains
to a chunk boundary and stops; a source that observes cancellation while a
request is already in flight — a REST page read, for instance — unwinds from
inside that read. Both produce exit `130`, a `cancelled` machine terminal, and
an OpenLineage `ABORT`. Cancellation is never reported as an engine failure
class, so alerting keyed on the lineage or OTLP terminal does not page for an
operator-initiated stop.

Exit `130` also covers one non-signal case: a **required** machine lifecycle
record that could not be written before publication. Clinker refuses to
publish an outcome it cannot report, so a broken control pipe stops the attempt
with finals unchanged. Discardable records are excluded from that rule — a lost
periodic `progress` observation is reported on stderr and the run continues to
its real outcome, and never converts a completed run into a cancellation.

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
