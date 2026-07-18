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
| `--memory-limit <SIZE>` | `512M` | Memory budget for the execution. Accepts binary (1024-based) `K`/`M`/`G` suffixes (`K` = 1024 bytes, `M` = 1024², `G` = 1024³); a bare integer is bytes. When the limit is approached, aggregation operators spill to disk rather than crashing. When passed, this value overrides any `memory.limit` set in the pipeline YAML; when omitted, the YAML value applies (or the `512M` default when the YAML is also silent). A malformed value (for example the decimal `4GB` rather than the binary `4G`) is rejected with an error naming `--memory-limit` and echoing the value, so a typo fails loudly instead of silently falling back to the default and shrinking a larger YAML budget. |
| `--threads <N>` | number of CPUs | Size of the thread pool used for parallel node execution. |
| `--error-threshold <N>` | `0` (unlimited) | Maximum number of records routed to the dead-letter queue before the pipeline aborts. `0` means no limit -- the pipeline will run to completion regardless of DLQ volume. |
| `--batch-id <ID>` | UUID v7 | Custom execution identifier. Appears in metrics output and log lines. Use a meaningful value (e.g. `daily-2026-04-11`) for correlation across retries. |
| `--explain [FORMAT]` | `text` | Print the execution plan and exit without processing data. Accepted formats: `text`, `json`, `dot`. See [Explain Plans](explain.md). |
| `--lineage <PATH>` | -- | Build column lineage and write it as OpenLineage NDJSON, then exit without processing data. Give a file path, or `-` for stdout. See [Column Lineage](lineage.md). |
| `--lineage-events <PATH>` | -- | Run the pipeline and emit live OpenLineage run events (a `START` at run begin, then a terminal `COMPLETE` / `FAIL` / `ABORT` with real timing and row counts) as NDJSON to a file path, or `-` for stdout. Cannot be combined with `--lineage`, `--explain`, `--dry-run`, or `-n`. See [Live run events](lineage.md#live-run-events). |
| `--dry-run` | -- | Validate the configuration (YAML structure, CXL syntax, type checking, DAG wiring) without reading any data. |
| `-n, --dry-run-n <N>` | -- | Process only the first `N` records through the full pipeline. Implies `--dry-run`. |
| `--dry-run-output <FILE>` | stdout | Redirect dry-run output to a file instead of stdout. Only meaningful with `-n`. |
| `--rules-path <DIR>` | `./rules/` | Search path for CXL module files referenced by `use` statements. |
| `--base-dir <DIR>` | -- | Base directory for resolving relative paths in the YAML config. Defaults to the directory containing the config file. |
| `--allow-absolute-paths` | -- | Permit absolute file paths in the pipeline YAML. By default, absolute paths are rejected to encourage portable configs. |
| `--env <NAME>` | -- | Set the active environment. Equivalent to setting `CLINKER_ENV`. Used by `when:` conditions in channel overrides. |
| `--quiet` | -- | Suppress progress output. Errors are still printed to stderr. |
| `--force` | -- | Allow output files to be overwritten if they already exist. Without this flag, the pipeline aborts rather than clobbering existing output. |
| `--log-level <LEVEL>` | `info` | Logging verbosity. One of: `error`, `warn`, `info`, `debug`, `trace`. |
| `--metrics-spool-dir <DIR>` | -- | Directory for per-execution metrics files. See [Metrics & Monitoring](metrics.md). |
| `--group <NAME>` | -- | Force-include a group overlay by name (repeatable). Applies the group's `overrides` op stream and `config`/`vars` clobber before the pipeline compiles, regardless of the group's selector. Use `clinker channels resolve` to preview the effective plan. |
| `--no-auto-groups` | -- | Suppress selector-derived group membership; only groups named with `--group` apply. |

### Examples

```bash
# Basic execution
clinker run pipeline.yaml

# Production run with memory budget and forced overwrite
clinker run pipeline.yaml --memory-limit 512M --force --log-level warn

# Validate without processing
clinker run pipeline.yaml --dry-run

# Preview first 10 records
clinker run pipeline.yaml --dry-run -n 10

# Show execution plan as Graphviz
clinker run pipeline.yaml --explain dot | dot -Tpng -o plan.png

# Run with a custom batch ID for tracing
clinker run pipeline.yaml --batch-id "daily-2026-04-11" --metrics-spool-dir ./metrics/
```

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
| `--channel <ID>` | -- | Channel id to resolve for (a folder under the channel root). Matching groups are derived from the channel's labels. |
| `--group <NAME>` | -- | Force-include a group overlay by name (repeatable), with or without a channel. |
| `--no-auto-groups` | -- | Suppress selector-derived group membership. |
| `--base-dir <DIR>` | `.` | Workspace root holding `clinker.toml` and the channel/group roots. |

Exits non-zero when the overlay raises an error (e.g. a config key matching no
parameter), so `resolve` doubles as a targeted check for one tenant.

### clinker channels lint

Compiles every (target × overlay) combination across the workspace and reports
failures — the CI safety net for base-change blast radius. This is where the
full-tree scan lives; the run path resolves a single channel by computed lookup.

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
other keys and comments are preserved. A channel with no manifest yet gets one
created (with its folder name as the channel name).

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
- across every group / channel-manifest / per-target overlay file: op `target`,
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

Scope: a *per-target* overlay (`<target>.channel.yaml` / `.comp.yaml`) is
rewritten only when it overlays the renamed pipeline, so a different pipeline
that happens to share the node name is left alone. *Target-agnostic* layers —
channel-wide manifest overrides and group files — are rewritten wherever they
reference the old name; if two pipelines share a node name and the same group or
channel-wide override targets it, review the `--dry-run` diff and rely on
`channels lint` to catch any pipeline the rename should not have touched.

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

## Environment Variables

| Variable | Description |
|----------|-------------|
| `CLINKER_ENV` | Active environment name. Equivalent to `--env`. Used by `when:` conditions in channel overrides to select environment-specific configuration. |
| `CLINKER_METRICS_SPOOL_DIR` | Default metrics spool directory. Overridden by `--metrics-spool-dir`. |

**Precedence** (highest to lowest): CLI flag, environment variable, YAML config value.
