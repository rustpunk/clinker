# CLI Reference

Clinker ships two command-line tools: `clinker` (the pipeline runner) and `cxl` (the expression checker/evaluator/formatter, covered in the [CXL CLI chapter](../cxl/cxl-cli.md)). This page is the complete reference for `clinker`.

## clinker run

Execute a pipeline.

```
clinker run [OPTIONS] <CONFIG>
```

### Positional arguments

| Argument | Description |
|----------|-------------|
| `<CONFIG>` | Path to the pipeline YAML configuration file (required) |

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--memory-limit <SIZE>` | YAML `memory.limit`, else `512M` | Memory budget for the execution. Uses the same grammar as the YAML `memory.limit`: a byte count with an optional binary (1024-based) `K`/`M`/`G` suffix (`K` = 1024 bytes, `M` = 1024², `G` = 1024³), where a bare integer is bytes. Other forms — a decimal `GB`, an explicit `GiB`, or a fractional value such as `1.5G` — are rejected. When the limit is approached, aggregation operators spill to disk rather than crashing. When passed, this value overrides any `memory.limit` set in the pipeline YAML; when omitted, the YAML value applies (or the `512M` default when the YAML is also silent). An empty or whitespace-only value — as an ops wrapper produces when it forwards an unset variable, e.g. `--memory-limit "$CLINKER_MEM"` with `CLINKER_MEM` unset — is treated the same as omitting the flag. A non-empty malformed value (for example the decimal `4GB` rather than the binary `4G`) is rejected at the CLI boundary with an error naming `--memory-limit` and echoing the value, so a typo fails loudly instead of silently falling back to the default and shrinking a larger YAML budget. Because the flag simply populates `pipeline.memory.limit`, a startup budget error (`E312`) for a value you passed via `--memory-limit` refers to that same limit. |
| `--threads <N>` | YAML `pipeline.concurrency.threads`, else number of CPUs | Positive capacity applied independently to the Rayon CPU-kernel pool and to concurrent Source schema/read work across top-level and composition-body Sources. It is not a total operating-system thread limit: Source workers and the Rayon pool remain distinct. The selected value is recorded in execution metrics. Zero is rejected before the config is opened. |
| `--batch-id <ID>` | UUID v7 | Logical-batch correlation available as `pipeline.batch_id`, in `{batch_id}` output-path templates, machine events, and opt-in output provenance sidecars. Supplying it does not override the fresh UUIDv7 execution ID and does not provide deduplication, resume, or exactly-once behavior. It is not currently a field in the metrics-spool payload. |
| `--machine ndjson-v1` | -- | Opt into the `clinker.run` schema-1 lifecycle on stdout. Requires a non-empty `--batch-id`; conflicts with plan/dry-run output and with `--lineage -` or `--lineage-events -`. File-based lineage remains compatible: a plan-only `--lineage <FILE>` export shares this stream's identity and closes it with an explicit empty publication inventory, since it runs no attempt. Every line is one compact JSON object; human diagnostics move to stderr. Consumers must concurrently drain both pipes, reject unsupported schema majors, accept only additive schema-1 fields, and reconcile exactly one supported terminal with the actual process status and current-attempt artifact evidence. EOF, malformed output, forced termination, or a missing/duplicate terminal is incomplete, never success. See [Running Clinker Directly or Under a Supervisor](orchestrator-contract.md). |
| `--explain [FORMAT]` | `text` | Print the execution plan and exit without processing data. Accepted formats: `text`, `json`, `dot`. With `json` or `dot`, standard output carries only the document and human diagnostics move to stderr, so a consumer can redirect stdout straight into a parser; with `text` they stay together on stdout. See [Explain Plans](explain.md). |
| `--lineage <PATH>` | -- | Preflight the workspace lineage identity policy, build column lineage, and write it as OpenLineage NDJSON, then exit without processing data. Give a file path, or `-` for stdout. The export is the whole invocation, so one that cannot be delivered exits non-zero rather than reporting success: a destination the exporter cannot write exits `4`, and an event the `[observability.lineage]` byte caps reject exits `1`. Each diagnostic names the destination, states which of the two failed, and prints the configuration change where one applies. A failed export leaves no partial file behind, so a following upload step cannot pick up a stale one. Both this flag and `--lineage-events` need the `lineage` capability, which the released binary has; a build compiled without it refuses the flag at validation rather than exiting zero having emitted nothing (see [Optional capabilities](../getting-started/installation.md#optional-capabilities)). See [Column Lineage](lineage.md). |
| `--lineage-events <PATH>` | -- | Preflight the workspace lineage identity policy, run the pipeline, and emit live OpenLineage run events (a `START` at run begin, then a terminal `COMPLETE` / `FAIL` / `ABORT` with real timing and row counts) as NDJSON to a file path, or `-` for stdout. Cannot be combined with `--lineage`, `--explain`, `--dry-run`, or `-n`. With `-`, normal run output can interleave with the event stream; use a file for clean NDJSON. See [Live run events](lineage.md#live-run-events). |
| `--dry-run` | -- | With no `-n`, performs complete config, overlay, CXL, schema, DAG, resource, and publication-configuration validation, prints resolved outputs, and exits without opening or reading a Source and without opening or publishing a Sink. |
| `-n, --dry-run-n <N>` | -- | Bounded preview. Requires `--dry-run` and a positive `N`. Clinker checks the limit before every read and reads at most `N` records from each declared Source, including Sources inside composition bodies. Records drain in stable plan order to the explicit preview stream; configured Sink paths are never opened or published. Preview emits no live run telemetry or lineage lifecycle. |
| `--dry-run-output <FILE>` | stdout | Destination for bounded-preview bytes. Requires `--dry-run-n`; without it, the option is rejected before config access. All preview Sinks write through this one explicit destination using their configured formats. |
| `--rules-path <DIR>` | selected workspace's `rules/` | Select the CXL module rules root for this run. Precedence is explicit CLI value, then `pipeline.rules_path`, then `[catalog].rules_root`, then the workspace-relative `rules/` default. A relative value is anchored to the workspace selected by `--base-dir` or workspace discovery, not the process working directory. One root is selected; Clinker does not search multiple roots. See [Modules and `use`](../cxl/modules.md#rules-root-selection) and the [typed workspace catalog](../pipelines/channels.md#typed-workspace-catalog). |
| `--base-dir <DIR>` | -- | Base directory for resolving relative paths in the YAML config. Defaults to the directory containing the config file. |
| `--allow-absolute-paths` | -- | Permit absolute file paths in the pipeline YAML. By default, absolute paths are rejected to encourage portable configs. |
| `--env <NAME>` | -- | Sets `CLINKER_ENV` in the current process before the pipeline loads. The current run path does not otherwise consume that value for channel selection; select a channel explicitly with `--channel`. |
| `--quiet` | -- | Suppresses the “applied overlay” summary. Other stdout, tracing, warnings, and errors are not uniformly silenced. |
| `--force` | -- | Overrides an output's `if_exists: error` policy and permits overwrite. Outputs using the default `if_exists: overwrite` already overwrite without this flag; `unique_suffix` keeps its own collision behavior. |
| `--log-level <LEVEL>` | `info` | Closed logging level: `error`, `warn`, `info`, `debug`, or `trace`. Any other spelling is rejected. |
| `--metrics-spool-dir <DIR>` | -- | Directory for per-execution metrics files. See [Metrics & Monitoring](metrics.md). |
| `--channel <ID>` | -- | Apply a logical id from `[catalog.channels]`. The selected file must also have a `[catalog.pipelines]` id listed in the channel manifest. Matching groups are target-bounded before labels narrow them. |
| `--group <NAME>` | -- | Force-include a group overlay by name (repeatable). The selected pipeline or one of its admitted compositions must appear in the group's explicit `targets:` set. Use `clinker channels resolve` to preview the effective plan. |
| `--no-auto-groups` | -- | Suppress selector-derived group membership; only groups named with `--group` apply. |

`--error-threshold` is retired and rejected. Configure the typed pipeline
policy instead; the CLI diagnostic prints this paste-ready replacement:

```yaml
error_handling:
  type_error_threshold: 0.05
```

### Credential profile foundation

The current binary does **not** yet accept a credential-profile option or a
credential-profile configuration table. Referenced credentials therefore do
not activate a source, destination, or observability exporter through this
surface. Do not pass an environment, channel, or group as a substitute: those
selectors never choose credentials, and there is no default or sentinel
profile.

The run-local foundation that later preflight wiring will call is already
bounded. Its default ceilings are 64 named profiles, 256 provider
registrations across those profiles, 1 MiB of decoded profile/provider
definition state, and 256 simultaneously retained handles. Admission checks
all definition counts and bytes before a profile can resolve a requirement.
Each live lease reports its exact retained bytes to the run memory arbitrator
before provider allocation. The registry has no inbound producer and is not a
backpressure target. An arbitrator spill callback queues a request and reports
zero bytes freed synchronously; the next registry-owned checkpoint revokes and
releases the partial set in reverse acquisition order and unregisters the
registry. Cap, memory, and provider failures follow the same fail-closed
cleanup path; an explicit run coordinator may pause acquisition until resume.

These are foundation limits, not newly available command-line behavior. A
later complete preflight surface must add the one explicit profile selector,
credential-required omission checks, and consumer activation together before
the option can appear in the options table above.

### Examples

```bash
# Basic execution
clinker run pipeline.yaml

# Production run with memory budget and forced overwrite
clinker run pipeline.yaml --memory-limit 512M --force --log-level warn

# Validate without processing
clinker run pipeline.yaml --dry-run

# Preview at most 25 records from each declared Source without publishing Sinks
clinker run pipeline.yaml --dry-run -n 25 --dry-run-output preview.csv

# Compile and explain without reading data
clinker run pipeline.yaml --explain text

# Show execution plan as Graphviz
clinker run pipeline.yaml --explain dot | dot -Tpng -o plan.png

# Run with a batch ID available to templates and provenance sidecars
clinker run pipeline.yaml --batch-id "daily-2026-04-11"

# Emit the bounded schema-1 lifecycle for a supervisor
clinker run pipeline.yaml --machine ndjson-v1 --batch-id "daily-2026-04-11"
```

The standalone command remains the common case and requires no supervisor.
Machine mode adds a child-process control stream; it does not add scheduling,
retry, heartbeat, or process-tree management to Clinker. A supervising parent
must heartbeat independently of advisory progress and start a fresh process
with a new execution ID for every retry.

---

## clinker guess

Preview concrete `int` or `float` replacements for inference-only `numeric`
columns without editing the pipeline.

```text
clinker guess [OPTIONS] <CONFIG>
```

With no selector, `guess` reads the base pipeline. `--channel <ID>` selects one
cataloged channel plus its target-admitted derived groups; `--group <NAME>`
selects one explicit, target-admitted group without a channel. The two
selectors conflict. A missing or ambiguous selector is an error rather than a
fallback to the base pipeline, so the report always describes exactly one
effective configuration.

| Flag | Description |
|------|-------------|
| `<CONFIG>` | Pipeline YAML containing the source-schema `numeric` leaves to inspect. |
| `--channel <ID>` | Select one cataloged channel and its derived, target-admitted groups. |
| `--group <NAME>` | Select one explicit, target-admitted group without a channel. |
| `--field <NODE.COLUMN>` | Narrow the preview to one `numeric` source column. Repeatable; repeated selectors are deduplicated in request order. Unknown, malformed, or already-concrete fields are rejected. |
| `--base-dir <DIR>` | Workspace root holding `clinker.toml` and the channel/group roots. Defaults to the pipeline file's directory. |

The command constructs readers through the same CSV, JSON, and XML option and
schema-coercion path as runtime ingest. It emits one deterministic JSON document
containing the selected configuration, bounded coverage, parser-owned numeric
evidence, unresolved reasons, proposed types, and an exact semantic YAML patch.
The patch is a preview string inside the report; this command never changes the
pipeline or an overlay.

Sampling is bounded to four discovered files per selected source, 1,024 records
per sampled file, 8 MiB per file, and eight retained evidence items per field.
Files beyond either file bound are reported as uncovered, and hitting the
record bound is reported as truncated. The limits and every sampled or
uncovered file are included in the report so a proposal is never presented as
having broader evidence than Clinker actually read.

Exit `0` means a complete preview document was written, including when one or
more fields remain unresolved. Selection, configuration, and field errors exit
`1`; source discovery, input I/O, reader, and stdout failures exit `4`.

```bash
# Preview every inference-only numeric leaf in the base pipeline
clinker guess pipeline.yaml

# Preview one field under a cataloged channel
clinker guess pipeline.yaml --channel acme --field orders.amount

# Preview one explicit group without a channel
clinker guess pipeline.yaml --group enterprise
```

---

## clinker explain

Inspect one compiled field's provenance or discover registry-owned diagnostic
descriptors.

```text
clinker explain <CONFIG> --field <PATH> [OPTIONS]
clinker explain --list [--status <STATUS>] [--category <CATEGORY>]
clinker explain --code <CODE>
```

Exactly one of `--field`, `--list`, or `--code` is required. A pipeline path is
required only for `--field` and is rejected for the two static discovery modes.

| Flag | Description |
|------|-------------|
| `--field <PATH>` | Explain one exact or unambiguous shorthand field address in the compiled pipeline. |
| `--list` | Print every registered descriptor in stable code order. |
| `--status <STATUS>` | With `--list`, require `active` or `retired-reserved`. |
| `--category <CATEGORY>` | With `--list`, require one of `configuration`, `composition`, `source-and-expression`, `execution-and-format`, `terminal-authoring`, `security`, or `advisory`. |
| `--code <CODE>` | Print one registered descriptor and its optional longer detail page. |
| `--channel <ID>` | With `--field`, apply the selected channel before compiling provenance. |
| `--group <NAME>` | With `--field`, force-include a group overlay (repeatable). |
| `--no-auto-groups` | With `--field`, suppress selector-derived groups. |
| `--base-dir <DIR>` | Workspace root used by the field-provenance compile path; defaults to `.`. |

All seven descriptor fields—code, severity, status, category, retryability,
meaning, and correction—come from the same leaf registry in both discovery
views. Closed enum values in the descriptor use lowercase kebab-case; filter
spellings come from the same enum tables used to parse those filters. A detail
page can add examples but cannot define whether a code exists.
Unknown or empty filters, no-match combinations, unknown codes, and conflicting
modes exit nonzero. See [Explain Plans](explain.md) for examples and for the
separate `clinker run --explain` plan display.

---

## clinker metrics collect

Sweep per-execution metrics files from a spool directory into a single NDJSON archive.

```
clinker metrics collect [OPTIONS]
```

### Options

| Flag | Description |
|------|-------------|
| `--spool-dir <DIR>` | Spool directory to sweep (required). |
| `--output-file <FILE>` | NDJSON archive destination (required). If the file exists, new entries are appended. |
| `--delete-after-collect` | Remove spool files after they have been successfully written to the archive. |
| `--dry-run` | Preview which files would be collected without writing anything. |

### Examples

```bash
# Collect and archive, then clean up spool
clinker metrics collect \
  --spool-dir /var/spool/clinker/ \
  --output-file /var/log/clinker/metrics.ndjson \
  --delete-after-collect

# Preview what would be collected
clinker metrics collect \
  --spool-dir ./metrics/ \
  --output-file ./archive.ndjson \
  --dry-run
```

---

## clinker channels

Inspect and validate the channel/group multi-tenant overlay system.

```bash
clinker channels resolve <TARGET> [OPTIONS]
clinker channels lint [OPTIONS]
clinker channels group members <GROUP> [OPTIONS]
clinker channels label set <KEY>=<VALUE> <CHANNEL_ID>... [OPTIONS]
```

### clinker channels resolve

Renders the effective post-overlay plan for one target — the DAG plus per-value
provenance (which layer supplied each value, and which group injected which
node). This answers "what does tenant X actually run?".

| Flag | Default | Description |
|------|---------|-------------|
| `<TARGET>` | -- | Path to the base pipeline (or composition) YAML to resolve (required). |
| `--channel <ID>` | -- | Logical channel id from `[catalog.channels]`. Matching groups are derived only after explicit target admission. |
| `--group <NAME>` | -- | Force-include a group overlay by name (repeatable), subject to the same target set as automatic selection. |
| `--no-auto-groups` | -- | Suppress selector-derived group membership. |
| `--base-dir <DIR>` | `.` | Workspace root holding `clinker.toml` and the channel/group roots. |

Exits non-zero when the overlay raises an error (e.g. a config key matching no
parameter), so `resolve` doubles as a targeted check for one tenant.

### clinker channels lint

Compiles every target declared by every `[catalog.channels]` entry and reports
failures — the CI safety net for base-change blast radius. It uses the same
logical identity and target-scope checks as `run` and `explain`; a basename or
current working directory never supplies target identity.

| Flag | Default | Description |
|------|---------|-------------|
| `--base-dir <DIR>` | `.` | Workspace root to lint. |

Exits non-zero if any combination fails to compile or apply. Dangling splice
anchors (an op referencing a missing node) and config keys matching no parameter
are reported per combination.

### clinker channels group members

Lists the channels whose labels currently satisfy a group's selector — "who is
in this group right now?". Because membership is *derived* from labels, this
evaluates the group's `match:` selector against each channel's manifest labels
through the same derivation the overlay resolver uses.

| Flag | Default | Description |
|------|---------|-------------|
| `<GROUP>` | -- | Group name (the `group.name` of a `*.group.yaml`). |
| `--base-dir <DIR>` | `.` | Workspace root holding `clinker.toml` and the channel/group roots. |

A group with no `match:` selector is explicit-only and reports no derived
members. A channel whose labels make the selector ill-typed or reference an
undeclared label is reported as a selector error (never a silent non-match), and
the command exits non-zero when any such error occurs.

### clinker channels label set

Stamps (or overwrites) one label across the named channels by editing each
channel's `channel.cfg.yaml` manifest in place. Idempotent: re-running with the
same value writes nothing. Only the manifest's `labels:` block is rewritten;
other keys and comments are preserved. The channel manifest must already exist
with a non-empty `channel.targets` list; `label set` will not create a
targetless manifest.

| Flag | Default | Description |
|------|---------|-------------|
| `<KEY>=<VALUE>` | -- | Label assignment. `KEY` must be an identifier (letters, digits, `_`) so a selector can reference it. `VALUE` is typed by YAML scalar inference (`true`/`false` → bool, integers → int, decimals → float, otherwise string). |
| `<CHANNEL_ID>...` | -- | One or more channel ids (tenant folder names) to stamp. |
| `--base-dir <DIR>` | `.` | Workspace root holding `clinker.toml` and the channel root. |

Because group membership is attribute-derived, `label set` is the maintenance
operation for group membership: set a label once and every group whose selector
matches gains the channel — no membership list to hand-edit.

### Examples

```bash
# What does tenant `globex` actually run for this pipeline?
clinker channels resolve pipeline/order_fulfillment.yaml --channel globex

# Preview a group overlay standalone (no channel)
clinker channels resolve pipeline/order_fulfillment.yaml --group enterprise

# Compile every channel/group overlay in the workspace and report failures
clinker channels lint

# Which channels are currently in the `enterprise` group?
clinker channels group members enterprise

# Onboard two tenants into the enterprise tier in one shot
clinker channels label set tier=enterprise globex acme-corp
```

---

## clinker refactor

Structural refactors that span a base pipeline and every channel/group overlay
that references it.

```bash
clinker refactor rename-node <TARGET> <OLD> <NEW> [OPTIONS]
```

### clinker refactor rename-node

Renames a base node and propagates the rename to every overlay reference. The
overlay op model addresses base nodes *by name*, so renaming a node otherwise
breaks every overlay that referenced it. This command rewrites, in one
operation:

- the base node's `name` and every consumer's `input:` / `inputs:` /
  `body:`/`header:`/`trailer:` reference;
- a Combine's named-input map (qualifier key and/or upstream value) and — when
  the Combine draws from the renamed node under a same-named qualifier — its
  `where:` / `cxl:` bodies, rewritten via the CXL parser so only true source
  qualifiers are touched (a method receiver like `region.contains(...)` is left
  alone);
- across every target-admitted group / channel-manifest / per-target overlay
  file: op `target`,
  `after`, `before`, injected `alias`, explicit `input`, `rewire` keys and
  values, an inline `node`, a `set config.cxl` value's CXL, and top-level
  `config` dotted-path prefixes (`old.param` → `new.param`).

| Flag | Default | Description |
|------|---------|-------------|
| `<TARGET>` | -- | Path to the base pipeline (or composition) YAML that declares the node. |
| `<OLD>` | -- | Current node name (must exist in the target). |
| `<NEW>` | -- | New node name — identifier only (letters, digits, `_`); must not already exist in the target. |
| `--dry-run` | -- | Print the diff of every file that would change without writing anything. |
| `--base-dir <DIR>` | `.` | Workspace root holding `clinker.toml` and the channel/group roots. |

Ambiguity is guarded: renaming to a name that already exists in the target, or
renaming a node that does not exist, is a hard error. A Combine `where:`/`cxl:`
body that must be rewritten but does not parse aborts the whole operation before
anything is written. After a real (non-dry-run) run the command re-runs
`channels lint` so an incomplete rename fails loudly.

Scope is catalog- and target-bounded. Per-target files are matched by their
logical `channel.target`; channel manifests are admitted only when
`channel.targets` contains the selected pipeline; and groups are admitted only
when `group.targets` names that pipeline or a composition in its resolved
closure. Filenames and basenames never establish identity, and a selector match
cannot widen the refactor beyond the group's declared target set.

Files are rewritten by re-serializing their YAML: key order is preserved, but
comments and incidental scalar styling are normalized. Use `--dry-run` to review
the exact on-disk diff first.

### Examples

```bash
# Preview a rename across the base pipeline and every overlay that references it
clinker refactor rename-node pipeline/order_fulfillment.yaml orders purchases --dry-run

# Apply it, then re-lint the workspace
clinker refactor rename-node pipeline/order_fulfillment.yaml orders purchases
```

---

## clinker config

Inspect a pipeline configuration file.

```bash
clinker config --resolved <CONFIG>
```

### clinker config --resolved

Prints the config with the **multi-value shorthand expanded to canonical form**.
The bare-field forms of `split_to_rows:`, `split_values:`, and `join_values:`
are rewritten to full mappings with every default spelled out — so you can see
exactly what the engine runs:

- a bare `- line_items` under `split_to_rows:` becomes
  `- { field: line_items, keep_empty: true, mode: extract }`;
- a bare `- tags` under `split_values:` becomes
  `- { field: tags, delimiter: ";" }`;
- a bare `- tags` under `join_values:` becomes
  `- { field: tags, delimiter: ";", on_conflict: error, escape: "\\" }`.

The rewrite is **surgical**: only those shorthand blocks change. Comments, key
order, indentation, and every other surface are preserved byte-for-byte, so the
output parses to a plan semantically identical to the input, and running
`config --resolved` on the result is a no-op. Schema columns are already
canonical (`multiple: true` is always written explicitly), so the schema block is
left untouched.

This is config canonicalization for the pipeline file itself. It is distinct
from [`clinker channels resolve`](#clinker-channels-resolve), which renders the
effective *post-overlay* plan for a specific tenant.

A few surfaces are deliberately left as written rather than expanded, since
regenerating them would lose information: a shorthand block that carries an
**interior comment or blank line** between its items is passed through unchanged
(so the comment is never dropped), and a value written as a **YAML alias**
(`*anchor`) is left in place — the anchor it points to is expanded at its
definition, so the alias still resolves to the expanded value. The output uses
the input file's line endings (LF or CRLF).

| Flag | Default | Description |
|------|---------|-------------|
| `<CONFIG>` | -- | Path to the pipeline YAML config file (required). The file is validated before it is rewritten, so a malformed config fails with a config error rather than emitting a half-expanded document. |
| `--resolved` | -- | Print the fully-expanded canonical form to stdout. Currently the only mode; required. |

### Examples

```bash
# Show the fully-expanded canonical form
clinker config --resolved pipeline.yaml

# Materialize the shorthand into a new file
clinker config --resolved pipeline.yaml > pipeline.canonical.yaml
```

See [Source Nodes → Multi-value fields](../nodes/source.md#multi-value-fields)
for the shorthand these forms expand from.

---

## clinker attempts

Inspect and clean up retained publication attempts owned by a pipeline:

```bash
clinker attempts list <PIPELINE> [--path-execution-id <ID>] [--continuation <TOKEN>] [--show-paths] [--format text|json]
clinker attempts inspect <PIPELINE> --execution-id <ID> [--path-execution-id <ID>] [--show-paths] [--format text|json]
clinker attempts purge <PIPELINE> (--execution-id <ID> | --expired) [--path-execution-id <ID>] [--execute] [--continuation <TOKEN>] [--show-paths] [--format text|json]
```

`<PIPELINE>` is required and must be a traversal-free, workspace-relative
`.yaml` or `.yml` path. Every invocation reloads and compiles that pipeline,
then derives its finite destination-parent roots from the compiled config.
There is no option for supplying a storage root, deletion path, or safety
override.

When the original run used path- or overlay-affecting options, repeat them on
the attempt command: `--base-dir`, `--allow-absolute-paths`, `--rules-path`,
`--channel`, repeatable `--group`, and `--no-auto-groups`. Output templates that
use run identity also require the matching `--path-execution-id`, `--batch-id`,
or `--timestamp`. The path identity is deliberately distinct from inspect and
purge's `--execution-id` selector, so `purge --expired` can reconstruct an
execution-scoped destination without changing its selector. Attempt operations
replay file-source discovery for `{source_file}` and `{source_path}` fan-out and
anchor a pipeline without `--base-dir` at the pipeline's own directory, matching
`run`. These values recompile typed `ValidatedPath` roots and never grant
authority to a caller-supplied deletion path.

`list` and `inspect` never mutate retained state. `purge` is also non-mutating
by default: it reports the attempts that the current retention policy admits.
Only `--execute` performs bounded cleanup. Live locks, invalid ownership,
unsupported filesystem entries, ambiguous clocks, and unreadable manifests
remain keep decisions even with `--execute`.

| Command or flag | Behavior |
| --- | --- |
| `list` | Lists retained attempts across all existing roots owned by the freshly compiled pipeline. |
| `inspect --execution-id <ID>` | Reports one canonical execution ID across those roots. |
| `purge --execution-id <ID>` | Previews one logical execution; add `--execute` to remove only positively owned, eligible files. |
| `purge --expired` | Previews all policy-expired attempts admitted by the bounded page; add `--execute` to clean them. |
| `--continuation <TOKEN>` | Resumes the exact plan-, root-, and selector-bound page emitted by a partial result. JSON `resume_argv` is authoritative; the text command applies platform quoting to the raw opaque token. |
| `--show-paths` | Adds sanitized workspace-relative attempt paths. Machine-local prefixes and sensitive-looking components remain redacted. |
| `--format json` | Emits one compact JSON object with stable field order and logical identifiers. The default is deterministic human-readable text. |

Default output is path-free. It contains the logical root ID, execution ID,
lifecycle state, eligibility, artifact IDs, cleanup debt, and exact bounds.
The compact JSON form carries the same fields plus shell-independent
`recovery_argv` and `resume_argv` arrays. Neither form includes record values,
credentials, secrets, or raw debug data.

Safety refusals and incomplete cleanup exit with status 4 and use the stable
E371 or E372 data. The report includes its logical failure code, registry-owned
retry advice, and a pasteable workspace-relative recovery command, for example:

```text
diagnostic: E371
failure: attempt.retention.manifest_invalid
retry: policy_required
recover: clinker attempts inspect pipelines/orders.yaml --execution-id 018f47a2-9a41-7a27-b4d6-4f7137e3c159
```

Examples:

```bash
# Path-free, non-mutating inventory
clinker attempts list pipelines/orders.yaml

# Inspect one retained execution as compact JSON
clinker attempts inspect pipelines/orders.yaml \
  --execution-id 018f47a2-9a41-7a27-b4d6-4f7137e3c159 \
  --format json

# Preview expired cleanup, then perform the same bounded selection
clinker attempts purge pipelines/orders.yaml --expired
clinker attempts purge pipelines/orders.yaml --expired --execute
```

See [Storage & Spill Location](storage.md#output-publication-and-retained-attempts)
for retention, bounds, destination qualification, and cleanup ordering.

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `CLINKER_ENV` | Active environment name. Equivalent to `--env`. Used by `when:` conditions in channel overrides to select environment-specific configuration. |
| `CLINKER_METRICS_SPOOL_DIR` | Default metrics spool directory. Overridden by `--metrics-spool-dir`. |

**Precedence** (highest to lowest): CLI flag, environment variable, YAML config value.
